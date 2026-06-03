"""
Compare EOF decomposition methods as predictors of ERCOT grid-damage outcomes.

Loads the forecast-error channel fields and the system hourly outcomes, fits each
method in analysis/eof_methods.py on a training split, and scores every method on
three families of metric:

  1. Damage-function skill — out-of-sample (test) R² when the method's scores are
     used as predictors in a common regression of each outcome. This is the
     headline: which decomposition gives the most predictive "damage function".
  2. Variance / covariance explained — how much of the error field (unsupervised)
     or error–outcome covariance (MCA) the leading modes capture.
  3. Score stability — split-half reproducibility: fit the basis on two disjoint
     halves of the training period, project both onto the test period, and
     measure how well the two score sets agree (matched mean |correlation|).
     A predictor set that is unstable out-of-sample makes a poor damage function.

Outputs:
  tables/eof_method_comparison.csv          — full per-method × per-outcome metrics
  tables/eof_method_diagnostics.csv         — variance/covariance + stability per method
  figures/eof_method_comparison/skill_bar.png        — test R² per method (primary outcome)
  figures/eof_method_comparison/skill_by_outcome.png — test R² grid, methods × outcomes

Usage:
    uv run python -m analysis.eof_method_comparison
    uv run python -m analysis.eof_method_comparison --months 1 2 3 --n_modes 5
    uv run python -m analysis.eof_method_comparison --primary economic_congestion_cost
"""

import argparse
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories

from analysis.pca_decomposition import (
    load_channel_fields, make_chunk_splits, ERROR_FIELDS, ALL_MONTHS,
    RANDOM_STATE, _r2, FIELD_LABELS, _draw_texas, _get_cartopy_crs, _grid_marker_size,
)
from analysis.pca_mode_analysis import load_outcomes, DEPVAR_CONFIGS, DEPVARS
from analysis import eof_methods
from analysis.eof_methods import MethodResult, DEFAULT_K

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

PRIMARY_DEPVAR = "economic_congestion_cost"

# Methods to compare, in display order. Each entry: (key, builder, extra-kwargs).
# MCA is supervised and needs the outcomes table threaded in at call time.
METHOD_ORDER = [
    "eof_perchannel", "eof_joint", "varimax_joint",
    "sparse_joint", "eeof_perchannel", "mca",
]


def _build_specs(common, outcomes_df=None):
    """Map each method key to its (builder, kwargs) pair.

    Parameters
    ----------
    common      : dict — shared kwargs (K, error_fields, seed)
    outcomes_df : pd.DataFrame or None — required only for MCA

    Returns
    -------
    dict {method_key: (builder_fn, kwargs_dict)}
    """
    mca_kw = dict(common, outcomes_df=outcomes_df,
                  mca_outcomes=eof_methods.MCA_OUTCOMES,
                  depvar_configs=DEPVAR_CONFIGS)
    return {
        "eof_perchannel":  (eof_methods.fit_eof_perchannel,  common),
        "eof_joint":       (eof_methods.fit_eof_joint,       common),
        "varimax_joint":   (eof_methods.fit_varimax_joint,   common),
        "sparse_joint":    (eof_methods.fit_sparse_joint,    common),
        "eeof_perchannel": (eof_methods.fit_eeof_perchannel, common),
        "mca":             (eof_methods.fit_mca,             mca_kw),
    }


# ── Damage-function skill ────────────────────────────────────────────────────


def _time_controls(hours):
    """Cyclic hour-of-day / month controls plus a weekend dummy, indexed by hours."""
    h, m = hours.hour, hours.month
    return pd.DataFrame({
        "sin_hour":   np.sin(2 * np.pi * h / 24),
        "cos_hour":   np.cos(2 * np.pi * h / 24),
        "sin_month":  np.sin(2 * np.pi * m / 12),
        "cos_month":  np.cos(2 * np.pi * m / 12),
        "is_weekend": (hours.dayofweek >= 5).astype(float),
    }, index=hours)


def damage_skill(scores_df, dep_series, transform, hours, train_mask, test_mask):
    """Fit a common damage regression on train hours and report train/test R².

    Predictors are the method's scores (standardized on train) plus cyclic time
    controls. R² is always reported on the natural outcome scale (un-doing the
    log1p transform) so methods and outcomes are comparable.

    Parameters
    ----------
    scores_df  : pd.DataFrame (hours × predictors) — may contain NaN rows
    dep_series : pd.Series indexed by valid_time — the outcome
    transform  : "log1p" or "raw"
    hours      : pd.DatetimeIndex
    train_mask : ndarray bool (len hours,)
    test_mask  : ndarray bool (len hours,)

    Returns
    -------
    dict with train_r2, test_r2, n_predictors, n_train, n_test
    """
    import statsmodels.api as sm

    score_cols = list(scores_df.columns)
    y_raw = dep_series.reindex(hours)
    y = np.log1p(y_raw.clip(lower=0)) if transform == "log1p" else y_raw.astype(float)

    base  = pd.concat([_time_controls(hours), scores_df], axis=1)
    valid = base.notna().all(axis=1).values & y.notna().values
    tr    = valid & train_mask
    te    = valid & test_mask
    out   = {"n_predictors": len(score_cols), "n_train": int(tr.sum()), "n_test": int(te.sum())}
    if tr.sum() < 50 or te.sum() < 50:
        return {**out, "train_r2": np.nan, "test_r2": np.nan}

    mu = base.loc[tr, score_cols].mean()
    sd = base.loc[tr, score_cols].std().clip(lower=1e-10)
    X = base.copy()
    X[score_cols] = (base[score_cols] - mu) / sd

    Xmat = sm.add_constant(X.values, has_constant="add")
    res  = sm.OLS(y.values[tr], Xmat[tr]).fit()
    yhat = res.predict(Xmat)

    if transform == "log1p":
        ytrue, ypred = np.expm1(y.values), np.expm1(yhat)
    else:
        ytrue, ypred = y.values, yhat
    return {**out,
            "train_r2": _r2(ytrue[tr], ypred[tr]),
            "test_r2":  _r2(ytrue[te], ypred[te])}


# ── Stability ────────────────────────────────────────────────────────────────


def _train_halves(hours, train_mask, chunk_days=5):
    """Split training hours into two disjoint sets by alternating multi-day chunks.

    Returns (idx_a, idx_b): integer positions into `hours` for the two halves.
    Chunking by day blocks (not random rows) keeps autocorrelated hours together,
    so the two halves are genuinely independent draws of weather regimes.
    """
    dates    = hours.normalize()
    tr_dates = pd.DatetimeIndex(sorted(dates[train_mask].unique()))
    chunk_id = pd.Series(np.arange(len(tr_dates)) // chunk_days, index=tr_dates)
    half     = chunk_id % 2
    date_half = pd.Series(dates).map(half).values        # NaN for non-train dates
    idx_a = np.where(train_mask & (date_half == 0))[0]
    idx_b = np.where(train_mask & (date_half == 1))[0]
    return idx_a, idx_b


def _matched_abs_corr(A, B):
    """Mean over A-columns of the best |Pearson r| to any B-column (greedy match).

    Parameters
    ----------
    A, B : ndarray (n_rows, k) — score sets on the same rows

    Returns
    -------
    float in [0, 1]; 1 = every A mode is perfectly reproduced by some B mode.
    """
    if A.shape[0] < 10 or A.shape[1] == 0 or B.shape[1] == 0:
        return np.nan
    Az = (A - A.mean(0)) / (A.std(0) + 1e-12)
    Bz = (B - B.mean(0)) / (B.std(0) + 1e-12)
    corr = np.abs(Az.T @ Bz) / A.shape[0]                # (kA, kB)
    return float(corr.max(axis=1).mean())


def score_stability(builder, kwargs, bundle, hours, train_mask, test_mask):
    """Split-half stability of a method's test-period scores.

    Fits the basis on each training half, projects both onto the test hours, and
    returns the matched mean |correlation| between the two test-score sets.

    Parameters
    ----------
    builder    : a fit_* function from eof_methods
    kwargs     : dict of extra keyword args for the builder
    bundle     : channel-field bundle
    hours      : pd.DatetimeIndex
    train_mask : ndarray bool
    test_mask  : ndarray bool

    Returns
    -------
    float stability score in [0, 1] (NaN if a half is too small).
    """
    idx_a, idx_b = _train_halves(hours, train_mask)
    if len(idx_a) < 100 or len(idx_b) < 100:
        return np.nan
    ra = builder(bundle, idx_a, hours, **kwargs)
    rb = builder(bundle, idx_b, hours, **kwargs)

    A = ra.scores.loc[test_mask]
    B = rb.scores.loc[test_mask]
    both = A.notna().all(axis=1) & B.notna().all(axis=1)
    return _matched_abs_corr(A.loc[both].values, B.loc[both].values)


# ── Diagnostics summary ──────────────────────────────────────────────────────


def _diag_summary(result: MethodResult):
    """Reduce a method's diagnostics to scalar variance/covariance summaries.

    Returns
    -------
    dict with var_cov_explained (cumulative leading-mode fraction) and, for the
    supervised MCA, frac_var_Y_explained (sum over modes).
    """
    diag = result.diagnostics
    out  = {"frac_var_Y_explained": np.nan}

    if result.name == "mca":
        scf = diag.get("squared_covariance_fraction")
        out["var_cov_explained"]    = float(np.nansum(scf)) if scf is not None else np.nan
        fvy = diag.get("frac_var_Y_by_X")
        out["frac_var_Y_explained"] = float(np.nansum(fvy)) if fvy is not None else np.nan
        return out

    ve = diag.get("var_explained", {})
    # per-channel: average cumulative fraction across channels; joint: single block
    cum = [float(np.nansum(v)) for v in ve.values() if v is not None and len(v)]
    out["var_cov_explained"] = float(np.mean(cum)) if cum else np.nan
    return out


# ── Orchestration ────────────────────────────────────────────────────────────


def build_methods(bundle, train_idx, hours, K, outcomes_df, seed=RANDOM_STATE):
    """Fit every registered method once on the full training split.

    Returns
    -------
    (results, builder_specs)
      results       : list[MethodResult] in METHOD_ORDER
      builder_specs : list of (builder_fn, kwargs) parallel to results, reused by
                      the stability pass so it refits with identical settings.
    """
    common = dict(K=K, error_fields=ERROR_FIELDS, seed=seed)
    specs  = _build_specs(common, outcomes_df)

    results, builder_specs = [], []
    for key in METHOD_ORDER:
        builder, kwargs = specs[key]
        print(f"  Fitting {key} ...", flush=True)
        res = builder(bundle, train_idx, hours, **kwargs)
        results.append(res)
        builder_specs.append((builder, kwargs))
        print(f"    {res.label}: {res.scores.shape[1]} predictors")
    return results, builder_specs


def run_comparison(months=None, K=DEFAULT_K, depvars=None,
                   primary=PRIMARY_DEPVAR, compute_stability=True):
    """Run the full method comparison and write tables + figures.

    Parameters
    ----------
    months            : list of (year, month); defaults to all 12 months of 2025
    K                 : modes per block
    depvars           : outcomes to score; defaults to all DEPVARS
    primary           : the headline outcome for the bar figure
    compute_stability : if False, skip the split-half refits (faster)

    Returns
    -------
    (skill_df, diag_df) DataFrames also written to tables/.
    """
    months  = months or ALL_MONTHS
    depvars = depvars or DEPVARS

    dirs       = setup_directories()
    tables_dir = Path(dirs["tables"])
    fig_dir    = Path(dirs["figures"]) / "eof_method_comparison"
    tables_dir.mkdir(parents=True, exist_ok=True)
    fig_dir.mkdir(parents=True, exist_ok=True)

    print("\n=== Step 1: Loading error channels and outcomes ===")
    bundle      = load_channel_fields(months, dirs)
    outcomes_df = load_outcomes(dirs)
    hours       = bundle["hours"]
    train_mask, test_mask = make_chunk_splits(hours, seed=RANDOM_STATE)
    train_idx   = np.where(train_mask)[0]
    print(f"  hours={len(hours)}  train={train_mask.sum()}  test={test_mask.sum()}")

    print("\n=== Step 2: Fitting decomposition methods ===")
    results, builder_specs = build_methods(bundle, train_idx, hours, K, outcomes_df)

    print("\n=== Step 3: Damage-function skill + diagnostics ===")
    skill_rows, diag_rows = [], []
    for res, (builder, kwargs) in zip(results, builder_specs):
        summary = _diag_summary(res)

        stability = np.nan
        if compute_stability:
            stability = score_stability(builder, kwargs, bundle, hours, train_mask, test_mask)
        diag_rows.append({
            "method": res.name, "label": res.label, "supervised": res.supervised,
            "n_predictors": res.scores.shape[1],
            "var_cov_explained": summary["var_cov_explained"],
            "frac_var_Y_explained": summary["frac_var_Y_explained"],
            "stability": stability,
        })

        for dv in depvars:
            if dv not in outcomes_df.columns:
                continue
            transform = DEPVAR_CONFIGS.get(dv, {}).get("transform", "log1p")
            sk = damage_skill(res.scores, outcomes_df[dv], transform, hours, train_mask, test_mask)
            skill_rows.append({
                "method": res.name, "label": res.label, "supervised": res.supervised,
                "outcome": dv, "outcome_label": DEPVAR_CONFIGS.get(dv, {}).get("label", dv),
                **sk,
            })
        prim = next((r for r in skill_rows if r["method"] == res.name and r["outcome"] == primary), None)
        prim_te = prim["test_r2"] if prim else float("nan")
        print(f"  {res.label:28s} | {primary} test R²={prim_te:+.4f} | "
              f"var/cov={summary['var_cov_explained']:.3f} | stab={stability:.3f}")

    skill_df = pd.DataFrame(skill_rows)
    diag_df  = pd.DataFrame(diag_rows)
    skill_df.to_csv(tables_dir / "eof_method_comparison.csv", index=False)
    diag_df.to_csv(tables_dir / "eof_method_diagnostics.csv", index=False)
    print(f"\n  Saved: {tables_dir / 'eof_method_comparison.csv'}")
    print(f"  Saved: {tables_dir / 'eof_method_diagnostics.csv'}")

    print("\n=== Step 4: Figures ===")
    plot_skill_bar(skill_df, diag_df, primary, fig_dir)
    plot_skill_by_outcome(skill_df, depvars, fig_dir)

    return skill_df, diag_df


# ── Figures ──────────────────────────────────────────────────────────────────


def plot_skill_bar(skill_df, diag_df, primary, fig_dir):
    """Bar chart of test R² per method for the primary outcome.

    Bars are annotated with predictor count and split-half stability; supervised
    (MCA) bars are hatched to flag that they used outcome information when fitting.
    """
    sub = skill_df[skill_df["outcome"] == primary].set_index("method").reindex(METHOD_ORDER)
    diag = diag_df.set_index("method").reindex(METHOD_ORDER)
    if sub["test_r2"].isna().all():
        print("  (no skill to plot for primary outcome)")
        return

    labels = sub["label"].tolist()
    test   = sub["test_r2"].values
    train  = sub["train_r2"].values
    x = np.arange(len(labels))

    fig, ax = plt.subplots(figsize=(max(7, len(labels) * 1.3), 4.8))
    ax.bar(x - 0.2, train, width=0.4, color="#bdc3c7", label="train R²")
    bars = ax.bar(x + 0.2, test, width=0.4, color="#2980b9", label="test R²")
    for i, key in enumerate(METHOD_ORDER):
        if bool(diag.loc[key, "supervised"]):
            bars[i].set_hatch("//")
            bars[i].set_edgecolor("white")
        npred = int(sub["n_predictors"].iloc[i]) if not np.isnan(test[i]) else 0
        stab  = diag.loc[key, "stability"]
        ax.annotate(f"p={npred}\nstab={stab:.2f}" if not np.isnan(stab) else f"p={npred}",
                    (x[i] + 0.2, np.nanmax([test[i], 0])),
                    textcoords="offset points", xytext=(0, 3),
                    ha="center", fontsize=6.5, color="#333")

    ax.axhline(0, color="k", lw=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=20, ha="right", fontsize=8)
    ax.set_ylabel("R² (natural scale)", fontsize=9)
    label = DEPVAR_CONFIGS.get(primary, {}).get("label", primary)
    ax.set_title(f"Damage-function skill by decomposition method — {label}\n"
                 "hatched = supervised (MCA, used outcomes to fit basis)", fontsize=9)
    ax.legend(fontsize=8)
    ax.grid(axis="y", linestyle=":", linewidth=0.5, alpha=0.6)
    plt.tight_layout()
    out = fig_dir / "skill_bar.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out}")


def plot_skill_by_outcome(skill_df, depvars, fig_dir):
    """Heatmap of test R² with methods on rows and outcomes on columns."""
    present = [d for d in depvars if d in set(skill_df["outcome"])]
    if not present:
        return
    mat = (skill_df.pivot_table(index="method", columns="outcome", values="test_r2")
           .reindex(index=METHOD_ORDER, columns=present))
    labels   = [skill_df[skill_df["method"] == m]["label"].iloc[0] for m in METHOD_ORDER]
    col_lbls = [DEPVAR_CONFIGS.get(d, {}).get("label", d) for d in present]

    vals = mat.values.astype(float)
    vmax = np.nanmax(np.abs(vals)) if not np.all(np.isnan(vals)) else 1.0
    fig, ax = plt.subplots(figsize=(max(6, len(present) * 1.5), max(4, len(METHOD_ORDER) * 0.6)))
    im = ax.imshow(vals, aspect="auto", cmap="RdBu_r", vmin=-vmax, vmax=vmax)
    ax.set_xticks(np.arange(len(present)))
    ax.set_xticklabels(col_lbls, rotation=30, ha="right", fontsize=8)
    ax.set_yticks(np.arange(len(METHOD_ORDER)))
    ax.set_yticklabels(labels, fontsize=8)
    for ri in range(vals.shape[0]):
        for ci in range(vals.shape[1]):
            v = vals[ri, ci]
            if not np.isnan(v):
                ax.text(ci, ri, f"{v:+.3f}", ha="center", va="center",
                        fontsize=7, color="white" if abs(v) > vmax * 0.55 else "black")
    fig.colorbar(im, ax=ax, shrink=0.8, pad=0.02).set_label("test R²", fontsize=8)
    ax.set_title("Out-of-sample damage-function R²: method × outcome", fontsize=9)
    plt.tight_layout()
    out = fig_dir / "skill_by_outcome.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out}")

def visualize_modes(model, months=None, K=DEFAULT_K):
    """Visualize spatial loading patterns for a fitted decomposition method.

    Creates a grid of Texas scatter maps with K columns (one per mode) and one
    row per error channel. For joint methods all channels share the same modes
    (so cross-channel co-loading is visible column-by-column); for per-channel
    methods each row is decomposed independently. Red = positive loading,
    Blue = negative.

    Parameters
    ----------
    model  : str — method key: "eof_perchannel", "eof_joint", "varimax_joint",
             "sparse_joint", "eeof_perchannel", or "mca"
    months : list of (year, month) — defaults to all 12 months of 2025
    K      : int — number of modes to fit and display

    Returns
    -------
    Path to the saved PNG, or None if no loadings are available.
    """
    from matplotlib.colors import TwoSlopeNorm

    months  = months or ALL_MONTHS
    dirs    = setup_directories()
    fig_dir = Path(dirs["figures"]) / "eof_method_comparison"
    fig_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n=== Fitting {model} for mode visualization ===")
    bundle      = load_channel_fields(months, dirs)
    outcomes_df = load_outcomes(dirs) if model == "mca" else None
    hours       = bundle["hours"]
    train_mask, _ = make_chunk_splits(hours, seed=RANDOM_STATE)
    train_idx   = np.where(train_mask)[0]

    common = dict(K=K, error_fields=ERROR_FIELDS, seed=RANDOM_STATE)
    specs  = _build_specs(common, outcomes_df)
    if model not in specs:
        raise ValueError(f"Unknown model '{model}'. Choose from: {list(specs)}")

    builder, kwargs = specs[model]
    result = builder(bundle, train_idx, hours, **kwargs)
    diag   = result.diagnostics

    # ── Assemble (label, loading_da, var_fracs, is_joint) per channel row ────
    # loading_da has dims (mode, latitude, longitude).
    # var_fracs is a 1-D array of explained-variance fractions per mode.
    # is_joint=True means all rows share the same modes and var_fracs.
    ve       = diag.get("var_explained", {})
    panels   = []
    is_joint = False
    joint_ve = []

    if "loadings_perfield" in diag:
        for f in ERROR_FIELDS:
            da = diag["loadings_perfield"].get(f)
            if da is not None:
                panels.append((FIELD_LABELS.get(f, f), da, ve.get(f, [])))
    elif "loadings_list" in diag:
        is_joint  = True
        joint_key = next(iter(ve), None)
        joint_ve  = list(ve[joint_key]) if joint_key else list(
            diag.get("squared_covariance_fraction", []))
        for f, da in zip(diag.get("fields", ERROR_FIELDS), diag["loadings_list"]):
            panels.append((FIELD_LABELS.get(f, f), da, joint_ve))
    else:
        print(f"  '{model}' does not store loadings — cannot visualize modes.")
        return None

    if not panels:
        return None

    n_modes = min(K, panels[0][1].sizes.get("mode", K))

    ccrs       = _get_cartopy_crs()
    crs_pc     = ccrs.PlateCarree() if ccrs is not None else None
    subplot_kw = {"projection": crs_pc} if crs_pc is not None else {}
    norm       = TwoSlopeNorm(vmin=-1.0, vcenter=0.0, vmax=1.0)

    fig, axes = plt.subplots(
        len(panels), n_modes,
        figsize=(n_modes * 2.6, len(panels) * 2.1),
        subplot_kw=subplot_kw,
        squeeze=False,
    )

    # For joint methods, characterize each mode by its dominant channels
    # (mean |loading| per field, ranked high→low) so column titles are interpretable.
    _MODE_SHORT = {
        "temp_error_1h":    "T·1h",
        "wspd100_error_1h": "W·1h",
        "temp_error_0h":    "T·0h",
        "wspd100_error_0h": "W·0h",
    }
    if is_joint:
        fields_diag = diag.get("fields", ERROR_FIELDS)
        mode_chars  = []
        for ci in range(n_modes):
            mags   = [(_MODE_SHORT.get(f, f), np.nanmean(np.abs(da.isel(mode=ci).values)))
                      for f, da in zip(fields_diag, diag["loadings_list"])]
            ranked = sorted(mags, key=lambda x: x[1], reverse=True)
            mode_chars.append(" ▸ ".join(lbl for lbl, _ in ranked))
    else:
        mode_chars = [""] * n_modes

    # Hoist the static parts of scatter kwargs (same every cell).
    base_sc_kw = {"cmap": "RdBu_r", "norm": norm, "rasterized": True}
    if crs_pc is not None:
        base_sc_kw["transform"] = crs_pc

    # ri (field) is the outer loop so lat/lon and marker size are computed once per field.
    for ri, (field_label, da, field_ve) in enumerate(panels):
        # Resolve lat/lon once per field — handles 1-D regular and 2-D curvilinear grids.
        ref_da  = da.isel(mode=0)
        lat_c   = ref_da.coords.get("latitude", ref_da.coords.get("lat"))
        lon_c   = ref_da.coords.get("longitude", ref_da.coords.get("lon"))
        if lat_c is None:
            lat_c = ref_da.coords[ref_da.dims[-2]]
            lon_c = ref_da.coords[ref_da.dims[-1]]
        lats_raw, lons_raw = lat_c.values, lon_c.values
        if lats_raw.ndim == 1:
            lons_2d, lats_2d = np.meshgrid(lons_raw, lats_raw)
            lats, lons = lats_2d.ravel(), lons_2d.ravel()
        else:
            lats, lons = lats_raw.ravel(), lons_raw.ravel()
        marker_s = _grid_marker_size(lons)

        for ci in range(n_modes):
            ax   = axes[ri, ci]
            vals = da.isel(mode=ci).values.ravel()

            # Sign convention: positive net loading (Red = above-average error).
            if np.nansum(vals) < 0:
                vals = -vals

            # Normalise to [-1, 1] for a consistent colour scale across panels.
            vals_norm = vals / (np.nanmax(np.abs(vals)) + 1e-12)

            ax.scatter(lons, lats, **{**base_sc_kw, "c": vals_norm, "s": marker_s})
            _draw_texas(ax)

            if ri == 0:
                var_pct   = float(joint_ve[ci]) * 100 if ci < len(joint_ve) else float("nan")
                ve_str    = f"  ({var_pct:.1f}%)" if not np.isnan(var_pct) else ""
                title     = f"Mode {ci + 1}{ve_str}"
                if mode_chars[ci]:
                    title += f"\n{mode_chars[ci]}"
                ax.set_title(title, fontsize=6.5, pad=3)
            if ci == 0:
                ax.set_ylabel(field_label, fontsize=6.5, labelpad=2)
            if not is_joint and ci < len(field_ve):
                ax.text(0.98, 0.02, f"{float(field_ve[ci]) * 100:.1f}%",
                        transform=ax.transAxes, fontsize=5.5,
                        ha="right", va="bottom", color="#555")

    _VE_NOTE = {
        "eeof_perchannel": "per-channel var, lag-0 loadings",
        "mca":             "squared covariance fraction",
    }
    ve_note = _VE_NOTE.get(model, "joint var" if is_joint else "per-channel var")
    fig.suptitle(
        f"{result.label} — Spatial loading patterns  (K = {n_modes})\n"
        f"Red = positive, Blue = negative  |  % = {ve_note}",
        fontsize=9, y=1.02,
    )
    sm = plt.cm.ScalarMappable(cmap="RdBu_r", norm=norm)
    sm.set_array([])
    fig.colorbar(sm, ax=axes[:, -1], shrink=0.7, pad=0.03,
                 label="Normalised loading")

    plt.tight_layout()
    out = fig_dir / f"mode_maps_{model}.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out}")
    return out


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Compare EOF decomposition methods")
    parser.add_argument("--months", type=int, nargs="*", default=None,
                        help="Months of 2025 to use (default: all 12)")
    parser.add_argument("--n_modes", type=int, default=DEFAULT_K,
                        help=f"Modes per block (default: {DEFAULT_K})")
    parser.add_argument("--primary", type=str, default=PRIMARY_DEPVAR,
                        help="Headline outcome for the bar figure")
    parser.add_argument("--no_stability", action="store_true",
                        help="Skip split-half stability refits (faster)")
    parser.add_argument("--model", type=str, default="varimax_joint",
                        choices=list(eof_methods.METHOD_REGISTRY.keys()),
                        help="Method to visualize modes for (default: varimax_joint)")
    parser.add_argument("--visualize_only", action="store_true",
                        help="Skip the full comparison and only plot mode maps")
    args = parser.parse_args()

    months = [(2025, m) for m in args.months] if args.months else None
    if not args.visualize_only:
        run_comparison(months=months, K=args.n_modes, primary=args.primary,
                       compute_stability=not args.no_stability)
    visualize_modes(model=args.model, months=months, K=args.n_modes)


if __name__ == "__main__":
    main()
