"""
EOF/PCA mode regression analysis linking ERCOT forecast errors to congestion costs.

Loads pre-computed PCA decomposition from pca_decomposition.py (scores and
spatial loadings saved as parquet/npz), then links EOF/PC scores to the
economic_congestion_cost outcome via:
  - Primary OLS with HAC (Newey-West) standard errors
  - AR-controlled robustness (lag-1h / lag-24h congestion)
  - Regime-conditional OLS (high-wind, extreme-heat, stressed-grid)

Run pca_decomposition.py first to generate the required input files.

Usage:
    uv run python -m analysis.pca_mode_analysis
    uv run python -m analysis.pca_mode_analysis --n_components 10
"""

import argparse
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import statsmodels.api as sm

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories

from analysis.pca_decomposition import (
    ERROR_FIELDS, REALIZED_FIELDS, ALL_FIELDS, FIELD_LABELS,
    ALL_MONTHS, N_COMPONENTS, RANDOM_STATE, HAC_MAXLAGS,
    COLOR_P001, COLOR_P005, COLOR_NSIG,
    _r2, _normalize_comp, _sig_stars, _draw_texas, _get_cartopy_crs,
    _grid_marker_size, make_chunk_splits, load_pca_results,
)

# ── Outcome configuration ──────────────────────────────────────────────────────

OUTCOMES_CSV = "system_hourly_outcomes_2025.csv"

DEPVAR_CONFIGS = {
    "economic_congestion_cost":       {"label": "Congestion Cost", "transform": "log1p"},
    "total_renewable_curtailment_mw": {"label": "Renewable Curtailment", "transform": "log1p"},
    "avg_intensity_kg_per_mwh":       {"label": "Avg Carbon Intensity",  "transform": "log1p"},
    "ruc_deployment_mw":              {"label": "RUC Deployment",         "transform": "log1p"},
    "rt_scgt_p85_markup":             {"label": "SC Gas Markup",     "transform": "raw"},
    "rt_ccgt_p85_markup":             {"label": "CC Gas Markup",     "transform": "raw"},
    "rt_cllig_p85_markup":            {"label": "Coal Markup",     "transform": "raw"},
}
DEPVARS = list(DEPVAR_CONFIGS.keys())

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)


# ── Outcome loading ────────────────────────────────────────────────────────────


def load_outcomes(dirs):
    """Load the system hourly outcomes CSV and compute derived markup columns.

    Parameters
    ----------
    dirs : dict from setup_directories()

    Returns
    -------
    pd.DataFrame indexed by valid_time with all outcome columns plus:
        rt_cllig_p85_markup, rt_scgt_p85_markup, rt_ccgt_p85_markup
    """
    path = Path(dirs["processed"]) / OUTCOMES_CSV
    df = pd.read_csv(path, parse_dates=["valid_time"]).set_index("valid_time")
    df["rt_cllig_p85_markup"] = df["rt_cllig_p85"] - df["cllig_mc"]
    df["rt_scgt_p85_markup"]  = df["rt_scgt_p85"]  - df["scgt_mc"]
    df["rt_ccgt_p85_markup"]  = df["rt_ccgt_p85"]  - df["ccgt_mc"]
    print(f"  Outcomes: {len(df)} hours, {len(df.columns)} columns from {path.name}")
    return df


# ── Regression matrix ──────────────────────────────────────────────────────────


def build_regression_matrix(scores_dict, error_fields, realized_fields,
                             hours, dep_series, K=N_COMPONENTS, transform="log1p"):
    """Assemble OLS design matrix from PCA scores, interactions, and time controls.

    Design matrix columns:
    - Cyclic time controls: sin/cos hour-of-day, sin/cos month, is_weekend (5)
    - PCA scores per error channel: K × len(error_fields)
    - PCA scores per realized channel: K × len(realized_fields)
    - Pairwise wind×temp interactions per horizon (1h, 0h, realized): K² each

    Parameters
    ----------
    scores_dict    : dict {field: ndarray (T, K)}
    error_fields   : list of str
    realized_fields: list of str
    hours          : pd.DatetimeIndex (T,)
    dep_series     : pd.Series indexed by valid_time
    K              : int
    transform      : "log1p" for log1p(clip(y,0)), "percent_relative_to_mean" for
                     (y / mean(y)) * 100, or "raw" for untransformed y

    Returns
    -------
    X_df           : pd.DataFrame (T_clean, n_features)
    y              : ndarray (T_clean,)
    hours_clean    : pd.DatetimeIndex
    feature_groups : dict — group name → list of column names
    """
    h, m = hours.hour, hours.month
    time_df = pd.DataFrame({
        "sin_hour":   np.sin(2 * np.pi * h / 24),
        "cos_hour":   np.cos(2 * np.pi * h / 24),
        "sin_month":  np.sin(2 * np.pi * m / 12),
        "cos_month":  np.cos(2 * np.pi * m / 12),
        "is_weekend": (hours.dayofweek >= 5).astype(float),
    }, index=hours)

    eof_cols = {}
    for field in error_fields + realized_fields:
        if field not in scores_dict:
            continue
        sc = scores_dict[field]
        for i in range(sc.shape[1]):
            eof_cols[f"PC{i+1}_{field}"] = sc[:, i]
    eof_df = pd.DataFrame(eof_cols, index=hours)

    def _eff_K(f1, f2):
        return min(K,
                   scores_dict[f1].shape[1] if f1 in scores_dict else 0,
                   scores_dict[f2].shape[1] if f2 in scores_dict else 0)

    K_1h   = _eff_K("wspd100_error_1h", "temp_error_1h")
    K_0h   = _eff_K("wspd100_error_0h", "temp_error_0h")
    K_real = _eff_K("era5_wspd100",     "era5_temp")

    int_cols = {}
    for prefix, f1, f2, K_val in [
        ("INT1h",   "wspd100_error_1h", "temp_error_1h", K_1h),
        ("INT0h",   "wspd100_error_0h", "temp_error_0h", K_0h),
        ("INTreal", "era5_wspd100",     "era5_temp",      K_real),
    ]:
        for i in range(K_val):
            for j in range(K_val):
                int_cols[f"{prefix}_PC{i+1}xPC{j+1}"] = scores_dict[f1][:, i] * scores_dict[f2][:, j]
    int_df = pd.DataFrame(int_cols, index=hours)

    aligned = dep_series.reindex(hours)
    if transform == "log1p":
        y_log = np.log1p(aligned.clip(lower=0))
    elif transform == "percent_relative_to_mean":
        mean_val = aligned.mean()
        if not pd.notna(mean_val) or mean_val == 0:
            raise ValueError(
                f"percent_relative_to_mean transform requires a non-zero, non-NaN mean; "
                f"got {mean_val!r}"
            )
        y_log = aligned / mean_val * 100.0
    else:
        y_log = aligned
    X_raw = pd.concat([time_df, eof_df, int_df], axis=1)
    valid = X_raw.notna().all(axis=1) & y_log.notna()

    X_clean     = X_raw.loc[valid]
    y_clean     = y_log.loc[valid].values
    hours_clean = pd.DatetimeIndex(X_clean.index)

    feature_groups = {"time_controls": list(time_df.columns)}
    for field in error_fields + realized_fields:
        if field in scores_dict:
            K_f = scores_dict[field].shape[1]
            feature_groups[field] = [f"PC{i+1}_{field}" for i in range(K_f)]
    feature_groups.update({
        "interactions_1h":   [f"INT1h_PC{i+1}xPC{j+1}"   for i in range(K_1h)  for j in range(K_1h)],
        "interactions_0h":   [f"INT0h_PC{i+1}xPC{j+1}"   for i in range(K_0h)  for j in range(K_0h)],
        "interactions_real": [f"INTreal_PC{i+1}xPC{j+1}" for i in range(K_real) for j in range(K_real)],
    })

    print(f"  Design matrix: {X_clean.shape[0]} hours × {X_clean.shape[1]} features "
          f"({(~valid).sum()} dropped for NaN target)")
    return X_clean, y_clean, hours_clean, feature_groups


def standardize_pca_cols(X_df, train_mask):
    """Standardize PC and interaction columns to zero mean, unit variance on training set.

    Time controls (sin/cos, is_weekend) are left on their natural scale.

    Parameters
    ----------
    X_df       : pd.DataFrame
    train_mask : ndarray bool (T,)

    Returns
    -------
    X_std : pd.DataFrame
    stats : pd.DataFrame — mean and std per column
    """
    scale_cols = [c for c in X_df.columns if c.startswith(("PC", "INT"))]
    X_std      = X_df.copy()
    train_sub  = X_df.loc[train_mask, scale_cols]
    mu    = train_sub.mean()
    sigma = train_sub.std().clip(lower=1e-10)
    X_std[scale_cols] = (X_df[scale_cols] - mu) / sigma
    stats = pd.DataFrame({"column": scale_cols, "mean": mu.values, "std": sigma.values})
    return X_std, stats


# ── OLS inference ──────────────────────────────────────────────────────────────


def run_ols_inference(y, X_df, feature_groups, maxlags=HAC_MAXLAGS):
    """Fit OLS with HAC (Newey-West) standard errors and run joint F-tests.

    Parameters
    ----------
    y              : ndarray (T,) — log1p target
    X_df           : pd.DataFrame (T, p)
    feature_groups : dict
    maxlags        : int

    Returns
    -------
    result    : statsmodels RegressionResultsWrapper
    f_tests   : dict {group: (f_stat, p_val)}
    col_names : list of str (includes const)
    """
    X_mat     = sm.add_constant(X_df.values)
    col_names = ["const"] + list(X_df.columns)

    result = sm.OLS(y, X_mat).fit(
        cov_type="HAC",
        cov_kwds={"maxlags": maxlags, "use_correction": True},
    )
    print(f"  OLS HAC: R²={result.rsquared:.4f}  adj-R²={result.rsquared_adj:.4f}  "
          f"N={result.nobs:.0f}  F={result.fvalue:.2f}  p={result.f_pvalue:.2e}")

    coef_idx = {name: i for i, name in enumerate(col_names)}
    f_tests  = {}
    for group, gcols in feature_groups.items():
        present = [c for c in gcols if c in coef_idx]
        if not present:
            continue
        R_mat = np.zeros((len(present), len(col_names)))
        for row_i, col in enumerate(present):
            R_mat[row_i, coef_idx[col]] = 1.0
        try:
            ft = result.f_test(R_mat)
            f_tests[group] = (float(np.squeeze(ft.statistic)), float(ft.pvalue))
        except Exception:
            f_tests[group] = (float("nan"), float("nan"))

    return result, f_tests, col_names


def save_coef_table(result, col_names, path, dep_series=None):
    """Write an OLS coefficient table (coef, HAC s.e., t, p, CI, stars) to CSV.

    Parameters
    ----------
    result    : statsmodels OLS result
    col_names : list of str — coefficient names (includes 'const')
    path      : Path — output CSV path
    dep_series: pd.Series or None — when given, adds a percent_of_mean column
                (coef as a percent of the mean outcome)
    """
    ci = result.conf_int()
    df = pd.DataFrame({
        "feature": col_names,
        "coef":    result.params,
        "se_hac":  result.bse,
        "t_stat":  result.tvalues,
        "p_value": result.pvalues,
        "ci_low":  ci[:, 0],
        "ci_high": ci[:, 1],
        "signif":  pd.cut(result.pvalues,
                          bins=[-np.inf, 0.001, 0.01, 0.05, 0.1, np.inf],
                          labels=["***", "**", "*", ".", ""]),
    })
    if dep_series is not None:
        mean_y = dep_series.mean()
        if mean_y != 0 and not np.isnan(mean_y):
            df["percent_of_mean"] = df["coef"] / mean_y * 100.0
        else:
            df["percent_of_mean"] = np.nan
    df.to_csv(path, index=False)
    print(f"  Saved: {path}")


# ── Regime regressions ─────────────────────────────────────────────────────────


def run_regime_regressions(regime_mean, X_std, y, hours_clean, dep_series,
                           feature_groups, col_names, tables_dir):
    """OLS regressions conditioned on three weather/grid regimes.

    Regimes: high_wind (ERA5 wind > p75), extreme_heat (ERA5 temp > p90),
    stressed_grid (congestion cost > p75).

    Parameters
    ----------
    regime_mean    : pd.DataFrame — era5_wspd100 / era5_temp per hour
    X_std          : pd.DataFrame (T_clean, p)
    y              : ndarray (T_clean,)
    hours_clean    : pd.DatetimeIndex (T_clean,)
    dep_series     : pd.Series
    feature_groups : dict
    col_names      : list of str
    tables_dir     : Path

    Returns
    -------
    dict {regime_name: (ols_result, n_obs)}
    """
    y_series   = dep_series.reindex(hours_clean)
    thresholds = {
        "wspd_q75": regime_mean["era5_wspd100"].quantile(0.75),
        "temp_q90": regime_mean["era5_temp"].quantile(0.90),
        "cong_q75": y_series.quantile(0.75),
    }
    regime_masks = {
        "high_wind":    (regime_mean["era5_wspd100"].reindex(hours_clean)
                         > thresholds["wspd_q75"]).fillna(False).values,
        "extreme_heat": (regime_mean["era5_temp"].reindex(hours_clean)
                         > thresholds["temp_q90"]).fillna(False).values,
        "stressed_grid":(y_series > thresholds["cong_q75"]).fillna(False).values,
    }

    results = {}
    rows    = []
    for name, mask in regime_masks.items():
        n = mask.sum()
        if n < 200:
            print(f"  {name}: skipped (N={n} < 200)")
            continue
        ols = sm.OLS(y[mask], sm.add_constant(X_std.values[mask])).fit(
            cov_type="HAC",
            cov_kwds={"maxlags": HAC_MAXLAGS, "use_correction": True},
        )
        results[name] = (ols, int(n))
        print(f"  {name}: N={n}  R²={ols.rsquared:.4f}")
        for cname, coef, pval in zip(col_names[1:], ols.params[1:], ols.pvalues[1:]):
            rows.append({"regime": name, "feature": cname, "coef": coef, "p_value": pval})

    pd.DataFrame(rows).to_csv(Path(tables_dir) / "pca_regime_coefficients.csv", index=False)
    print(f"  Saved: {Path(tables_dir) / 'pca_regime_coefficients.csv'}")
    return results


# ── AR robustness ──────────────────────────────────────────────────────────────


def build_ar_features(dep_series, hours):
    """Build lag-1h and lag-24h of log1p(congestion_cost).

    Parameters
    ----------
    dep_series : pd.Series indexed by valid_time
    hours      : pd.DatetimeIndex

    Returns
    -------
    pd.DataFrame with lag_1h, lag_24h
    """
    y_log = np.log1p(dep_series.reindex(hours).clip(lower=0))
    return pd.DataFrame({"lag_1h": y_log.shift(1), "lag_24h": y_log.shift(24)},
                        index=hours)


# ── Visualization ──────────────────────────────────────────────────────────────


def plot_coefficient_forest(result, col_names, feature_groups, fig_dir,
                            depvar="", depvar_label="", title_suffix="",
                            fname_prefix="pca", method_label="PCA"):
    """Coefficient forest plot with 95% HAC confidence intervals.

    Parameters
    ----------
    result       : statsmodels OLS result
    col_names    : list of str
    feature_groups : dict
    fig_dir      : Path
    depvar       : str — outcome key (used in filename)
    depvar_label : str — human-readable outcome label (used in title)
    title_suffix : str — appended to title and filename
    fname_prefix : str — output-filename prefix (e.g. "pca" or "eof")
    method_label : str — decomposition name shown in the title (e.g. "PCA" or "EOF")
    """
    plot_order = [
        ("HRRR 1h 100m Wind Error",          "wspd100_error_1h"),
        ("HRRR 1h Temp Error",               "temp_error_1h"),
        ("GFS Day-Ahead 100m Wind Error",    "wspd100_error_0h"),
        ("GFS Day-Ahead Temp Error",         "temp_error_0h"),
        ("Realized 100m Wind (ERA5)",        "era5_wspd100"),
        ("Realized Temp (ERA5)",             "era5_temp"),
        ("Interaction 1h (wind×temp)",       "interactions_1h"),
        ("Interaction Day-Ahead (wind×temp)", "interactions_0h"),
        ("Interaction Real (wind×temp)",     "interactions_real"),
    ]

    coef  = pd.Series(result.params,  index=col_names)
    ci    = result.conf_int()
    ci_lo = pd.Series(ci[:, 0], index=col_names)
    ci_hi = pd.Series(ci[:, 1], index=col_names)
    pval  = pd.Series(result.pvalues, index=col_names)

    ordered_cols, ordered_labels, group_spans = [], [], []
    pos = 0
    for g_label, g_key in plot_order:
        present = [c for c in feature_groups.get(g_key, []) if c in coef.index]
        if not present:
            continue
        group_spans.append((pos, pos + len(present), g_label))
        for c in present:
            ordered_cols.append(c)
            if c.startswith("INT") and "xPC" in c:
                suffix = c.split("_", 1)[1]
                ordered_labels.append(suffix.replace("PC", "").replace("x", "×"))
            else:
                ordered_labels.append(c.split("_")[0])
            pos += 1

    n = len(ordered_cols)
    if n == 0:
        return

    y_pos  = np.arange(n)
    colors = [COLOR_P001 if (p := pval.get(c, 1.0)) < 0.01 else COLOR_P005 if p < 0.05 else COLOR_NSIG
              for c in ordered_cols]

    bg = ["#f4f6f7", "#ffffff"]
    fig, ax = plt.subplots(figsize=(9, max(5, n * 0.38)))
    for i, (start, end, _) in enumerate(group_spans):
        ax.axhspan(start - 0.5, end - 0.5, color=bg[i % 2], alpha=0.5, zorder=0)
    for i, c in enumerate(ordered_cols):
        ax.errorbar(coef[c], y_pos[i],
                    xerr=[[max(coef[c] - ci_lo[c], 0)], [max(ci_hi[c] - coef[c], 0)]],
                    fmt="o", color=colors[i], markersize=5, capsize=3, linewidth=1.2)

    ax.axvline(0, color="k", linewidth=0.8, linestyle="--")
    ax.set_yticks(y_pos)
    ax.set_yticklabels(ordered_labels, fontsize=8)

    xlim    = ax.get_xlim()
    x_right = xlim[1] + (xlim[1] - xlim[0]) * 0.01
    for start, end, g_label in group_spans:
        ax.text(x_right, (start + end - 1) / 2, g_label,
                fontsize=7, va="center", ha="left", style="italic", color="#444")

    dep_title = depvar_label or depvar or "outcome"
    tfm = DEPVAR_CONFIGS.get(depvar, {}).get("transform") if depvar else None
    if tfm == "log1p":
        scale_note = "log₁p scale, "
    elif tfm == "percent_relative_to_mean":
        scale_note = "percent of mean, "
    else:
        scale_note = ""
    ax.set_xlabel(f"OLS coefficient ({scale_note}standardized inputs)", fontsize=9)
    ax.set_title(
        f"{method_label} Feature Coefficients — {dep_title}{title_suffix}\n"
        "HAC s.e.  ●  dark blue p < 0.01  ●  light blue p < 0.05  ●  gray n.s.",
        fontsize=9,
    )
    ax.invert_yaxis()
    ax.grid(axis="x", linestyle=":", linewidth=0.5, alpha=0.6)
    plt.tight_layout()
    dep_slug = f"_{depvar}" if depvar else ""
    fname = f"{fname_prefix}_coefficient_forest{dep_slug}{title_suffix.replace(' ', '_')}.png"
    out   = fig_dir / fname
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out}")


FTEST_LABELS = {
    "wspd100_error_1h":  "HRRR 1h 100m wind error (PCs)",
    "temp_error_1h":     "HRRR 1h temp error (PCs)",
    "wspd100_error_0h":  "GFS Day-Ahead 100m wind error (PCs)",
    "temp_error_0h":     "GFS Day-Ahead temp error (PCs)",
    "interactions_0h":   "Interactions Day-Ahead (100m wind×temp)",
    "interactions_real": "Interactions realized (100m wind×temp)",
    "era5_wspd100":      "Realized 100m wind (ERA5 PCs)",
    "era5_temp":         "Realized temp (ERA5 PCs)",
}


def plot_ftest_heatmap(all_f_tests, fig_dir, fname_prefix="pca", method_label="PCA"):
    """Heatmap of joint F-test results across outcome variables and feature groups.

    Cell color encodes the F-statistic; significance stars reflect the p-value.

    Parameters
    ----------
    all_f_tests  : dict {depvar: {group: (f_stat, p_val)}}
    fig_dir      : Path
    fname_prefix : str — output-filename prefix (e.g. "pca" or "eof")
    method_label : str — decomposition name shown in the title (e.g. "PCA" or "EOF")
    """
    group_order = list(FTEST_LABELS.keys())
    depvars     = list(all_f_tests.keys())

    present_groups = [g for g in group_order
                      if any(g in ft for ft in all_f_tests.values())]
    if not present_groups or not depvars:
        return

    n_rows = len(present_groups)
    n_cols = len(depvars)
    f_matrix = np.full((n_rows, n_cols), np.nan)
    p_matrix = np.full((n_rows, n_cols), np.nan)
    for ci, dv in enumerate(depvars):
        for ri, g in enumerate(present_groups):
            if g in all_f_tests[dv]:
                fv, pv = all_f_tests[dv][g]
                f_matrix[ri, ci] = fv
                p_matrix[ri, ci] = pv

    col_labels = [DEPVAR_CONFIGS[dv]["label"] if dv in DEPVAR_CONFIGS else dv
                  for dv in depvars]
    row_labels = [FTEST_LABELS.get(g, g) for g in present_groups]

    vmax = np.nanpercentile(f_matrix, 95) if not np.all(np.isnan(f_matrix)) else 1.0

    fig, ax = plt.subplots(figsize=(max(6, n_cols * 1.4), max(4, n_rows * 0.55)))
    im = ax.imshow(f_matrix, aspect="auto", cmap="viridis", vmin=0, vmax=vmax)

    ax.set_xticks(np.arange(n_cols))
    ax.set_xticklabels(col_labels, rotation=30, ha="right", fontsize=8)
    ax.set_yticks(np.arange(n_rows))
    ax.set_yticklabels(row_labels, fontsize=8)

    for ri in range(n_rows):
        for ci in range(n_cols):
            fv = f_matrix[ri, ci]
            pv = p_matrix[ri, ci]
            if np.isnan(fv):
                continue
            stars = "***" if pv < 0.001 else "**" if pv < 0.01 else "*" if pv < 0.05 else ""
            text_color = "white" if fv > vmax * 0.55 else "black"
            ax.text(ci, ri, stars, ha="center", va="center",
                    fontsize=9, color=text_color, fontweight="bold")

    cbar = fig.colorbar(im, ax=ax, shrink=0.7, pad=0.02)
    cbar.set_label("F-statistic", fontsize=8)

    ax.set_title(f"Joint F-tests: {method_label} Feature Groups × Outcome Variables\n"
                 "*** p<0.001  ** p<0.01  * p<0.05",
                 fontsize=9)
    plt.tight_layout()
    out = fig_dir / f"{fname_prefix}_ftest_heatmap.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out}")


def plot_regime_comparison(regime_results, col_names, feature_groups, fig_dir):
    """Coefficient forest comparing three regime subsamples.

    Parameters
    ----------
    regime_results : dict {name: (ols_result, n_obs)}
    col_names      : list of str
    feature_groups : dict
    fig_dir        : Path
    """
    error_groups = ["wspd100_error_1h", "temp_error_1h", "wspd100_error_0h", "temp_error_0h",
                    "interactions_1h", "interactions_0h"]
    error_cols = [c for g in error_groups for c in feature_groups.get(g, [])
                  if c in col_names]
    if not error_cols or not regime_results:
        return

    n_feat   = len(error_cols)
    labels   = [c.replace("_error", "") for c in error_cols]
    palette  = ["#2c3e50", "#e74c3c", "#27ae60", "#8e44ad"]
    offsets  = np.linspace(-0.3, 0.3, len(regime_results) + 1)

    fig, ax = plt.subplots(figsize=(9, max(6, n_feat * 0.38)))
    for ri, (rname, (ols_r, n_obs)) in enumerate(regime_results.items()):
        rcoef  = pd.Series(ols_r.params[1:], index=col_names[1:])
        rci    = ols_r.conf_int()
        rci_lo = pd.Series(rci[1:, 0], index=col_names[1:])
        rci_hi = pd.Series(rci[1:, 1], index=col_names[1:])
        for i, c in enumerate(error_cols):
            if c not in rcoef.index:
                continue
            y_pos = i + offsets[ri]
            ax.errorbar(rcoef[c], y_pos,
                        xerr=[[max(rcoef[c] - rci_lo[c], 0)], [max(rci_hi[c] - rcoef[c], 0)]],
                        fmt="o", color=palette[ri % len(palette)],
                        markersize=4, capsize=2, linewidth=1,
                        label=f"{rname} (N={n_obs})" if i == 0 else "")

    ax.axvline(0, color="k", lw=0.8, ls="--")
    ax.set_yticks(np.arange(n_feat))
    ax.set_yticklabels(labels, fontsize=8)
    ax.set_xlabel("OLS coefficient (log₁p scale, standardized)", fontsize=9)
    ax.set_title("Regime-Conditional Regressions — Error PCA Coefficients", fontsize=9)
    ax.legend(loc="lower right", fontsize=7.5)
    ax.invert_yaxis()
    ax.grid(axis="x", linestyle=":", linewidth=0.5, alpha=0.6)
    plt.tight_layout()
    out = fig_dir / "pca_regime_comparison.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out}")


def _scatter_component(ax, lons, lats, component, norm, crs_pc, s=None):
    """Scatter-plot one normalized PCA component onto a cartopy axis."""
    if s is None:
        s = _grid_marker_size(lons)
    scatter_kwargs = {
        "c": component,
        "cmap": "RdBu_r",
        "norm": norm,
        "s": s,
        "rasterized": True,
    }
    if crs_pc is not None:
        scatter_kwargs["transform"] = crs_pc
    ax.scatter(lons, lats, **scatter_kwargs)
    _draw_texas(ax)


def plot_pca_maps(pca_dict, lat_dict, lon_dict, fig_dir, K_show=None,
                  fname_prefix="pca", method_label="PCA"):
    """Spatial heatmaps of PCA mode loadings for each field.

    Shows variance explained per panel header. No significance borders or OLS
    coefficients. Called once before any depvar regressions.

    Parameters
    ----------
    pca_dict     : dict {field: fitted PCA}
    lat_dict     : dict {field: ndarray}
    lon_dict     : dict {field: ndarray}
    fig_dir      : Path
    K_show       : int or None — modes to display; defaults to all fitted components
    fname_prefix : str — output-filename prefix (e.g. "pca" or "eof")
    method_label : str — decomposition name shown in the title (e.g. "PCA" or "EOF")
    """
    from matplotlib.colors import TwoSlopeNorm

    ccrs = _get_cartopy_crs()
    norm   = TwoSlopeNorm(vmin=-1.0, vcenter=0, vmax=1.0)
    fields = list(pca_dict.keys())
    n_rows = len(fields)
    if K_show is None:
        K_show = max(len(pca.explained_variance_ratio_) for pca in pca_dict.values())

    crs_pc = ccrs.PlateCarree() if ccrs is not None else None
    subplot_kw = {"projection": crs_pc} if crs_pc is not None else None
    fig, axes = plt.subplots(
        n_rows, K_show,
        figsize=(K_show * 2.6, n_rows * 2.2),
        subplot_kw=subplot_kw,
        squeeze=False,
    )

    for r, field in enumerate(fields):
        pca   = pca_dict[field]
        vr    = pca.explained_variance_ratio_
        label = FIELD_LABELS.get(field, field)
        lons_f, lats_f = lon_dict[field], lat_dict[field]
        ms = _grid_marker_size(lons_f)

        for k in range(min(K_show, len(vr))):
            ax = axes[r, k]
            _scatter_component(ax, lons_f, lats_f,
                               _normalize_comp(pca.components_[k]), norm, crs_pc, s=ms)
            title = f"PC{k+1} ({vr[k]*100:.1f}% var)"
            if k == 0:
                ax.set_title(f"{label}\n{title}", fontsize=6.5, pad=3)
            else:
                ax.set_title(title, fontsize=7)

        for k in range(len(vr), K_show):
            axes[r, k].set_visible(False)

    fig.suptitle(f"{method_label} Mode Loadings — variance explained shown per panel",
                 fontsize=8.5, y=1.01)
    plt.tight_layout()
    sm = plt.cm.ScalarMappable(cmap="RdBu_r", norm=norm)
    sm.set_array([])
    fig.colorbar(sm, ax=axes.ravel().tolist(), shrink=0.6, pad=0.02,
                 fraction=0.02, label="Normalised loading")
    out = fig_dir / f"{fname_prefix}_component_maps.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out}")


def plot_pca_coefs_across_outcomes(
    pca_dict, lat_dict, lon_dict, all_results, fig_dir, K_show=N_COMPONENTS,
    fname_prefix="pca",
):
    """One figure per weather field: K_show×3 grid — map | log-outcome coefs | markup coefs.

    For each weather variable, creates a figure where each row is a PCA mode.
    Left column: spatial loading map with % variance explained.
    Middle column: forest plot for log1p-transformed outcomes (coefs ≈ % change in outcome).
    Right column: forest plot for raw markup outcomes (coefs in $/MWh change in markup).

    Color coding: dark blue p<0.01, light blue p<0.05, grey not significant.

    Parameters
    ----------
    pca_dict    : dict {field: SimpleNamespace} with .components_ and .explained_variance_ratio_
    lat_dict    : dict {field: ndarray}
    lon_dict    : dict {field: ndarray}
    all_results : dict {depvar: result_dict} — only depvar keys (with "ols_result") are used
    fig_dir     : Path
    K_show      : int — number of modes (rows) per figure
    """
    from matplotlib.colors import TwoSlopeNorm

    ccrs   = _get_cartopy_crs()
    crs_pc = ccrs.PlateCarree() if ccrs is not None else None
    norm   = TwoSlopeNorm(vmin=-1.0, vcenter=0.0, vmax=1.0)

    plot_fields = [
        "wspd100_error_1h",
        "wspd100_error_0h",
        "temp_error_1h",
        "temp_error_0h",
        "era5_wspd100",
        "era5_temp",
    ]

    depvar_results = {dv: r for dv, r in all_results.items()
                      if dv in DEPVARS and "ols_result" in r}
    if not depvar_results:
        print("  No depvar results found — skipping plot_pca_coefs_across_outcomes")
        return

    log_dvs = [dv for dv in DEPVARS
               if dv in depvar_results and DEPVAR_CONFIGS.get(dv, {}).get("transform") == "log1p"]
    raw_dvs = [dv for dv in DEPVARS
               if dv in depvar_results and DEPVAR_CONFIGS.get(dv, {}).get("transform") == "raw"]
    log_labels = [DEPVAR_CONFIGS[dv]["label"] for dv in log_dvs]
    raw_labels = [DEPVAR_CONFIGS[dv]["label"] for dv in raw_dvs]

    dv_cache = {}
    for dv in log_dvs + raw_dvs:
        res = depvar_results[dv]
        ols = res["ols_result"]
        dv_cache[dv] = {
            "ols":         ols,
            "ci":          ols.conf_int(),
            "name_to_idx": {name: i for i, name in enumerate(res["col_names"])},
        }

    def _collect_coefs(col_name, dv_list):
        """Return coefs, lo_errs, hi_errs, colors for a list of depvars at one PC column."""
        coefs, lo_errs, hi_errs, colors = [], [], [], []
        for dv in dv_list:
            cache = dv_cache[dv]
            idx   = cache["name_to_idx"].get(col_name)
            if idx is None:
                coefs.append(np.nan)
                lo_errs.append(0)
                hi_errs.append(0)
                colors.append(COLOR_NSIG)
                continue
            ols  = cache["ols"]
            ci   = cache["ci"]
            coef = float(ols.params[idx])
            pval = float(ols.pvalues[idx])
            coefs.append(coef)
            lo_errs.append(max(coef - float(ci[idx, 0]), 0))
            hi_errs.append(max(float(ci[idx, 1]) - coef, 0))
            colors.append(COLOR_P001 if pval < 0.01 else COLOR_P005 if pval < 0.05 else COLOR_NSIG)
        return coefs, lo_errs, hi_errs, colors

    def _draw_coef_panel(ax, labels, coefs, lo_errs, hi_errs, colors, xlabel):
        """Render a horizontal forest plot onto ax."""
        y_pos = np.arange(len(labels))
        for i, coef in enumerate(coefs):
            if np.isnan(coef):
                continue
            ax.errorbar(coef, y_pos[i],
                        xerr=[[lo_errs[i]], [hi_errs[i]]],
                        fmt="o", color=colors[i],
                        markersize=5, capsize=3, linewidth=1.2)
        ax.axvline(0, color="k", linewidth=0.8, linestyle="--")
        ax.set_yticks(y_pos)
        ax.set_yticklabels(labels, fontsize=7)
        ax.invert_yaxis()
        ax.grid(axis="x", linestyle=":", linewidth=0.5, alpha=0.6)
        ax.set_xlabel(xlabel, fontsize=7)
        ax.tick_params(axis="x", labelsize=7)

    for field in plot_fields:
        if field not in pca_dict:
            continue
        pca   = pca_dict[field]
        vr    = pca.explained_variance_ratio_
        K_eff = min(K_show, len(vr))
        label = FIELD_LABELS.get(field, field)
        lons_f, lats_f = lon_dict[field], lat_dict[field]
        ms = _grid_marker_size(lons_f)

        fig = plt.figure(figsize=(14, K_eff * 2.4))
        gs  = fig.add_gridspec(K_eff, 3, width_ratios=[1, 1.3, 1.3], hspace=0.35, wspace=0.35)

        map_axes = []
        for k in range(K_eff):
            col_name = f"PC{k+1}_{field}"

            ax_map = (fig.add_subplot(gs[k, 0], projection=crs_pc)
                      if crs_pc is not None else fig.add_subplot(gs[k, 0]))
            _scatter_component(ax_map, lons_f, lats_f,
                               _normalize_comp(pca.components_[k]), norm, crs_pc, s=ms)
            ax_map.set_title(f"PC{k+1}  ({vr[k]*100:.1f}% var)", fontsize=7.5, pad=3)
            ax_map.set_xticks([])
            ax_map.set_yticks([])
            map_axes.append(ax_map)

            ax_log = fig.add_subplot(gs[k, 1])
            _draw_coef_panel(ax_log, log_labels,
                             *_collect_coefs(col_name, log_dvs),
                             xlabel="OLS coef (approx. % change in outcome)")

            ax_raw = fig.add_subplot(gs[k, 2])
            _draw_coef_panel(ax_raw, raw_labels,
                             *_collect_coefs(col_name, raw_dvs),
                             xlabel="OLS coef ($/MWh change in markup)")

        sm_map = plt.cm.ScalarMappable(cmap="RdBu_r", norm=norm)
        sm_map.set_array([])
        fig.colorbar(sm_map, ax=map_axes, shrink=0.6, pad=0.02, fraction=0.05,
                     label="Normalised loading")

        fig.suptitle(
            f"{label}\n"
            "dark blue p<0.01  ●  light blue p<0.05  ●  grey n.s.  |  HAC s.e., 95% CI",
            fontsize=8.5, fontweight="bold",
        )
        out = fig_dir / f"{fname_prefix}_coefs_across_outcomes_{field}.png"
        fig.savefig(out, dpi=150, bbox_inches="tight")
        plt.close(fig)
        print(f"  Saved: {out}")


# ── Main ───────────────────────────────────────────────────────────────────────


def run_pca_mode_analysis(K=N_COMPONENTS, depvars=None):
    """Regress EOF/PC scores against one or more outcome variables.

    Loads pre-computed PCA decomposition from pca_decomposition.py, loads the
    outcomes CSV, then for each depvar runs:
    1. Build regression matrix (EOF scores + interactions + cyclic controls).
    2. Primary OLS with HAC standard errors.
    3. Save per-depvar tables and figures.
    Produces a combined cross-depvar F-test heatmap at the end.

    Parameters
    ----------
    K       : int — number of EOF modes (must match the decomposition run)
    depvars : list of str — outcome variable keys; defaults to all DEPVARS

    Returns
    -------
    dict {depvar: result_dict} for each depvar run
    """
    if depvars is None:
        depvars = DEPVARS

    dirs       = setup_directories()
    fig_dir    = Path(dirs["figures"]) / "pca_analysis"
    tables_dir = Path(dirs["tables"])
    pca_dir    = Path(dirs["processed"]) / "pca"
    fig_dir.mkdir(parents=True, exist_ok=True)
    tables_dir.mkdir(parents=True, exist_ok=True)
    pca_dir.mkdir(parents=True, exist_ok=True)

    # 1. Load decomposition (shared across all depvars)
    print("\n=== Step 1: Loading PCA decomposition results ===")
    pca_data    = load_pca_results(K, pca_dir)
    scores_dict = pca_data["scores_dict"]
    pca_dict    = pca_data["pca_dict"]
    lat_dict    = pca_data["lat_dict"]
    lon_dict    = pca_data["lon_dict"]
    hours       = pca_data["hours"]
    var_df      = pca_data["var_df"]

    # 2. PCA maps (once, independent of depvar)
    print("\n=== Step 2: Generating spatial PCA mode maps ===")
    plot_pca_maps(pca_dict, lat_dict, lon_dict, fig_dir)

    # 3. Load all outcomes once
    print("\n=== Step 3: Loading outcome variables ===")
    outcomes_df = load_outcomes(dirs)

    all_results = {}
    all_f_tests = {}

    for depvar in depvars:
        cfg = DEPVAR_CONFIGS.get(depvar, {"label": depvar, "transform": "log1p"})
        label     = cfg["label"]
        transform = cfg["transform"]

        print(f"\n{'='*60}")
        print(f"=== Depvar: {label} ({depvar}) ===")
        print(f"{'='*60}")

        if depvar not in outcomes_df.columns:
            print(f"  WARNING: {depvar} not in outcomes CSV — skipping")
            continue
        dep_series = outcomes_df[depvar]

        X_df, y, hours_clean, feature_groups = build_regression_matrix(
            scores_dict, ERROR_FIELDS, REALIZED_FIELDS, hours, dep_series,
            K=K, transform=transform,
        )
        train_mask, test_mask = make_chunk_splits(hours_clean, seed=RANDOM_STATE)
        X_std, scale_stats = standardize_pca_cols(X_df, train_mask)
        scale_stats.to_csv(tables_dir / f"pca_scale_stats_{depvar}.csv", index=False)

        ols_result, f_tests, col_names = run_ols_inference(y, X_std, feature_groups)
        all_f_tests[depvar] = f_tests

        if transform == "log1p":
            y_pred_log   = ols_result.fittedvalues
            y_nat        = np.expm1(y)
            y_pred_nat   = np.expm1(y_pred_log)
            train_r2_nat = _r2(y_nat[train_mask], y_pred_nat[train_mask])
            test_r2_nat  = _r2(y_nat[test_mask],  y_pred_nat[test_mask])
            print(f"  Native scale: train R²={train_r2_nat:.4f}  test R²={test_r2_nat:.4f}")
        else:
            train_r2_nat = _r2(y[train_mask], ols_result.fittedvalues[train_mask])
            test_r2_nat  = _r2(y[test_mask],  ols_result.fittedvalues[test_mask])
            print(f"  Test R²={test_r2_nat:.4f}  (raw scale)")

        # Tables
        save_coef_table(
            ols_result,
            col_names,
            tables_dir / f"pca_ols_coefficients_{depvar}.csv",
            dep_series if transform == "percent_relative_to_mean" else None,
        )
        pd.DataFrame(
            [{"group": g, "f_stat": v[0], "p_value": v[1]} for g, v in f_tests.items()]
        ).to_csv(tables_dir / f"pca_joint_ftests_{depvar}.csv", index=False)
        print(f"  Saved: pca_joint_ftests_{depvar}.csv")

        n01 = (ols_result.pvalues[1:] < 0.01).sum()
        n05 = (ols_result.pvalues[1:] < 0.05).sum()
        print(f"  {n01} features p<0.01, {n05} features p<0.05")

        all_results[depvar] = {
            "ols_result":     ols_result,
            "f_tests":        f_tests,
            "col_names":      col_names,
            "feature_groups": feature_groups,
            "train_r2_nat":   train_r2_nat,
            "test_r2_nat":    test_r2_nat,
            "n_hours":        len(hours_clean),
        }

    # PCA coefficient maps across all outcomes (one figure per weather field)
    print("\n=== PCA coefficient maps across outcomes ===")
    plot_pca_coefs_across_outcomes(
        pca_dict, lat_dict, lon_dict,
        {k: v for k, v in all_results.items() if k in DEPVARS},
        fig_dir,
    )

    # Combined heatmap
    print("\n=== Combined F-test heatmap ===")
    plot_ftest_heatmap(all_f_tests, fig_dir)

    # Shared spatial maps (only once, from first successful depvar)
    all_results["pca_dict"] = pca_dict
    all_results["var_df"]   = var_df
    all_results["K"]        = K
    return all_results


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="PCA mode regression analysis for ERCOT outcome variables"
    )
    parser.add_argument("--n_components", type=int, default=N_COMPONENTS,
                        help=f"PCA modes per channel (default: {N_COMPONENTS})")
    parser.add_argument("--depvars", nargs="*", default=None,
                        help="Outcome variables to run (default: all)")
    args = parser.parse_args()
    run_pca_mode_analysis(K=args.n_components, depvars=args.depvars)


if __name__ == "__main__":
    main()
