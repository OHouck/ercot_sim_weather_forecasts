"""
Functional PLS (FPLS) analysis of ERCOT weather-error fields — three-phase workflow.

This module is the *functional* counterpart of `analysis.pls_analysis_v2`.  It
reuses that module's data pipeline verbatim (the same channel bundle, the same
horizon blocks, the same sample cuts and outcomes) but replaces ordinary
multivariate PLS with **Functional PLS** as implemented in the authors' `fpls`
package — Babii, Carrasco & Tsafack (2024), "Functional Partial Least-Squares:
Adaptive Estimation and Inference" (Section 11 of
`reports/literature_method_outline.typ`).

The estimator (package `fpls.FunctionalPLS`)
---------------------------------------------
Each block's stacked weather-error field is treated as a single functional
observation x_i(s) sampled on the (flattened) ERCOT grid s, and the model is the
scalar-on-function regression

        y_i = ∫ x_i(s) β(s) ds + ε_i .

The package solves this ill-posed inverse problem with a conjugate-gradient PLS
iteration on K = Xᵀ X · ds / n and r = Xᵀ y / n; the number of iterations (PLS
components) is the spectral regulariser, chosen adaptively by
`fpls.select_components` (the paper's data-driven stopping rule).  The fitted
`coef_[:, k]` is the coefficient function β(s) using k components — reshaped onto
the grid it is the headline FPLS deliverable, a coefficient *map*.

We follow the package's flattened 1-D treatment of the 2-D field (Section 11:
"FPLS's smoothness theory is stated for a 1-D domain; numerically the flattened
field runs fine") and use a uniform Riemann step `ds = 1`.  Per the request, no
sparsity / group penalty and no 2-D roughness penalty are imposed.

NumPy ≥ 2.0 compatibility
-------------------------
The released `fpls` package calls `float()` on (1, 1) arrays inside its
conjugate-gradient loops, which raises under NumPy ≥ 2.0 (the project runs NumPy
2.4).  `_patch_fpls_numpy2` injects a NumPy-safe `float` into the package's
module namespaces so its own functions resolve to it — no algorithm code is
copied or altered.

Pre-processing (Section 11 recipe)
----------------------------------
Both the outcome and each block's field are residualised on cyclic time controls
via Frisch–Waugh (`fpls.frisch_waugh_residualize`) so β attaches to the forecast
*error* net of diurnal/seasonal structure; each channel is then divided by its
pooled training standard deviation (block scaling) so wind and temperature are
comparable.

Inference: block (moving-block bootstrap) standard errors
---------------------------------------------------------
Because the package returns a coefficient function rather than latent scores,
each block is reduced to its scalar *functional index* t_i = ∫ x_i β ds (the
block's FPLS prediction).  The residualised outcome is regressed on the three
block indices, and standard errors come from a *moving-block bootstrap* over
contiguous multi-day blocks of hours (Section 11 "Inference under temporal
dependence").  A block's index coefficient and its bootstrap band measure whether
that forecast horizon adds predictive power.

Blocks (one FPLS per forecast horizon; the two channels are stacked):
  1. dayahead  : GFS day-ahead 100m wind-speed error + temperature error
  2. hourahead : HRRR 1h 100m wind-speed error + temperature error
  3. realized  : ERA5 realized 100m wind speed + temperature

━━━ Phase 1 — run_fpls_mode_selection ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Per (outcome × block): in-sample and grouped-CV R² vs. the number of FPLS
components, annotated with the package-selected component count.
Output: figures/fpls_decomposition/fpls_selection.png

━━━ Phase 2 — run_fpls_decomposition ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
For each cut × outcome, fit one FPLS per block on the training chunks (component
count from `fpls.select_components`, capped at `default_modes`), evaluate held-out
skill, and save β(s) and the per-hour functional index.
Output: processed/fpls/fpls_modes_{cut}_{outcome}.npz (+ .json meta)

━━━ Phase 3 — run_fpls_analysis ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
For each cut × outcome, regress the (residualised) outcome on the block functional
indices with block standard errors, and produce: β(s) coefficient-function maps,
a block coefficient forest, and a block × outcome significance heatmap.
Output: figures/fpls_analysis/{cut}/

Usage:
    uv run python -m analysis.fpls_analysis --task select      # Phase 1
    uv run python -m analysis.fpls_analysis --task decompose   # Phase 2
    uv run python -m analysis.fpls_analysis --task analyze     # Phase 3
    uv run python -m analysis.fpls_analysis                    # all three
"""

import argparse
import json
import sys
import warnings
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy import stats as scipy_stats
from sklearn.model_selection import GroupKFold

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories

import fpls
import fpls.core
import fpls.selection
from fpls import FunctionalPLS, select_components, frisch_waugh_residualize

from analysis.pca_decomposition import (
    load_channel_fields, ALL_MONTHS, RANDOM_STATE, _r2,
    FIELD_LABELS, _sig_stars, _draw_texas, _get_cartopy_crs,
    _grid_marker_size, make_chunk_splits,
    COLOR_P001, COLOR_P005, COLOR_NSIG,
)
from analysis.pca_mode_analysis import load_outcomes, save_coef_table
from analysis.eof_analysis import (
    DECOMPOSITIONS, SAMPLE_CUT, DEPVAR_CONFIGS, DEPVARS,
    _subset_bundle, _save_fig,
)
# Reuse the exact same input-data construction as the PLS pipeline.
from analysis.pls_analysis_v2 import build_block_matrix, _chunk_labels, _transform_y

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)


# ── NumPy ≥ 2.0 compatibility shim for the fpls package ─────────────────────────

def _patch_fpls_numpy2():
    """Make the `fpls` package run under NumPy ≥ 2.0.

    The package calls ``float(x)`` on (1, 1) arrays inside its conjugate-gradient
    loops (`fpls.core`, `fpls.selection`), which raises a TypeError under NumPy
    ≥ 2.0.  Injecting a NumPy-safe ``float`` into those module namespaces makes the
    package's own functions resolve to it (module globals precede builtins), so the
    upstream algorithm runs unchanged.
    """
    def numpy_safe_float(value):
        return float(np.asarray(value).reshape(-1)[0])
    fpls.core.float      = numpy_safe_float
    fpls.selection.float = numpy_safe_float


_patch_fpls_numpy2()


# ── Constants ─────────────────────────────────────────────────────────────────

# One FPLS block per forecast horizon; reuse eof_analysis's channel pairings.
BLOCKS = DECOMPOSITIONS  # {block_key: ([wind_field, temp_field], label)}

# Maximum FPLS components per block (cap on the package's adaptive selection).
default_modes = {
    "dayahead":  6,
    "hourahead": 6,
    "realized":  6,
}

# Uniform Riemann grid spacing for the flattened functional domain (Section 11).
DS = 1.0

N_CV_FOLDS = 5

# Moving-block bootstrap settings for Phase-3 "block" standard errors.
N_BLOCK_BOOT = 400   # bootstrap resamples
BLOCK_DAYS   = 7     # contiguous days per resampled block

# Array keys persisted per block in the .npz archive — keep save/load in sync.
FPLS_MODES_KEYS = ("beta", "index", "lat", "lon", "offsets", "r2_cum")


# ═══════════════════════════════════════════════════════════════════════════════
# Section 0: Pre-processing (Frisch–Waugh residualization + channel scaling)
# ═══════════════════════════════════════════════════════════════════════════════

def build_time_controls(hours):
    """Cyclic time-control matrix (with intercept) for Frisch–Waugh residualization.

    Parameters
    ----------
    hours : pd.DatetimeIndex (T,)

    Returns
    -------
    ndarray (T, 6) — [const, sin/cos hour, sin/cos month, is_weekend]
    """
    hour_of_day   = hours.hour.values
    month_of_year = hours.month.values
    return np.column_stack([
        np.ones(len(hours)),
        np.sin(2 * np.pi * hour_of_day   / 24), np.cos(2 * np.pi * hour_of_day   / 24),
        np.sin(2 * np.pi * month_of_year / 12), np.cos(2 * np.pi * month_of_year / 12),
        (hours.dayofweek.values >= 5).astype(float),
    ])


def residualize_and_scale_block(X, time_controls, train_idx, offsets):
    """Residualize each pixel on time controls, then scale each channel by its std.

    Frisch–Waugh removes the diurnal/seasonal cycle from every pixel so the FPLS
    coefficient attaches to the forecast error net of calendar structure; per-channel
    scaling (training pooled std) makes the stacked wind and temperature channels
    comparable without flattening within-channel spatial variance.

    Parameters
    ----------
    X             : ndarray (T, p) — raw stacked block field
    time_controls : ndarray (T, q) — control matrix (with intercept)
    train_idx     : ndarray — training row positions (for the scaling stats)
    offsets       : list[(start, end)] — column slice per channel

    Returns
    -------
    ndarray (T, p) — residualized, channel-scaled field
    """
    X_residual = frisch_waugh_residualize(X, time_controls)
    for start, end in offsets:
        channel_std = X_residual[train_idx, start:end].std()
        if channel_std < 1e-12:
            channel_std = 1.0
        X_residual[:, start:end] /= channel_std
    return X_residual


def _choose_k_from_cv(cv_r2):
    """Pick the component count from a grouped-CV R² curve.

    Returns the count that maximizes out-of-sample CV R², but falls back to a single
    component when no count achieves positive CV R² (extra components only overfit) —
    so a flat, decreasing, or negative CV curve is never rewarded with more components.

    Parameters
    ----------
    cv_r2 : ndarray (m_max,) — grouped-CV R² for component counts 1..m_max

    Returns
    -------
    int — selected component count in [1, m_max]
    """
    if not np.any(np.isfinite(cv_r2)) or np.nanmax(cv_r2) <= 0:
        return 1
    return int(np.nanargmax(cv_r2) + 1)


def _grouped_cv_r2(X_processed, y, groups, m_max, n_splits):
    """Grouped K-fold CV R² of FPLS for each component count 1..m_max.

    Parameters
    ----------
    X_processed : ndarray (T, p) — residualized, channel-scaled predictors
    y           : ndarray (T,)   — residualized outcome
    groups      : ndarray (T,)   — chunk ids for grouped CV
    m_max       : int
    n_splits    : int

    Returns
    -------
    ndarray (m_max,) — mean CV R² per component count
    """
    n_splits = min(n_splits, len(np.unique(groups)))
    grouped_kfold = GroupKFold(n_splits=n_splits)
    cv_fold_r2 = np.full((n_splits, m_max), np.nan)
    for fold_idx, (train_rows, test_rows) in enumerate(
            grouped_kfold.split(X_processed, y, groups)):
        fold_model = FunctionalPLS(m_max=m_max).fit(
            X_processed[train_rows], y[train_rows], ds=DS)
        for k in range(1, m_max + 1):
            cv_fold_r2[fold_idx, k - 1] = _r2(
                y[test_rows], fold_model.predict(X_processed[test_rows], n_components=k))
    return np.nanmean(cv_fold_r2, axis=0)


def select_n_components(X_train, y_train, m_max, groups_train, n_splits=N_CV_FOLDS):
    """Adaptive component count, robust to the high-dimensional (p ≥ n) regime.

    For p < n the package's data-driven rule (`fpls.select_components`, the paper's
    early-stopping criterion) is used as intended.  For p ≥ n that rule degenerates
    — its noise estimate comes from an under-determined OLS fit whose residual
    collapses to ~0, so the stopping threshold vanishes and it always returns
    `m_max`, regardless of whether extra components actually generalize.  In that
    regime we instead choose the count by grouped-CV evidence (`_choose_k_from_cv`),
    so a flat/decreasing/negative CV curve yields a parsimonious choice rather than
    the cap.

    Parameters
    ----------
    X_train      : ndarray (n, p) — residualized/scaled training field
    y_train      : ndarray (n,)   — residualized training outcome
    m_max        : int            — component cap
    groups_train : ndarray (n,)   — chunk ids for grouped CV (used when p ≥ n)
    n_splits     : int

    Returns
    -------
    int — selected component count in [1, m_max]
    """
    n_train, n_features = X_train.shape
    if n_features < n_train:
        return int(min(select_components(X_train, y_train, m_max=m_max, ds=DS), m_max))
    cv_r2 = _grouped_cv_r2(X_train, y_train, groups_train, m_max, n_splits)
    return _choose_k_from_cv(cv_r2)


# ═══════════════════════════════════════════════════════════════════════════════
# Section 1: FPLS mode selection (Phase 1)
# ═══════════════════════════════════════════════════════════════════════════════

def _fpls_cv_curve(X_processed, y, groups, m_max, n_splits):
    """In-sample and grouped-CV R² of FPLS vs. number of components.

    Parameters
    ----------
    X_processed : ndarray (T, p) — residualized, channel-scaled predictors
    y           : ndarray (T,)   — residualized outcome
    groups      : ndarray (T,)   — chunk ids for grouped CV
    m_max       : int            — maximum components
    n_splits    : int

    Returns
    -------
    dict with arrays n, insample_r2, cv_r2; scalar adaptive_n.
    """
    component_grid = np.arange(1, m_max + 1)

    insample_model = FunctionalPLS(m_max=m_max).fit(X_processed, y, ds=DS)
    insample_r2 = np.array([
        _r2(y, insample_model.predict(X_processed, n_components=k)) for k in component_grid])

    cv_r2 = _grouped_cv_r2(X_processed, y, groups, m_max, n_splits)

    # Annotate with the count Phase 2 will actually use: package rule when p < n,
    # else the CV-evidence choice (reusing the curve just computed).
    if X_processed.shape[1] < X_processed.shape[0]:
        adaptive_n = int(min(select_components(X_processed, y, m_max=m_max, ds=DS), m_max))
    else:
        adaptive_n = _choose_k_from_cv(cv_r2)
    return {"n": component_grid, "insample_r2": insample_r2,
            "cv_r2": cv_r2, "adaptive_n": adaptive_n}


def _plot_selection_panel(ax, curves, title):
    """Draw one FPLS mode-selection panel: in-sample and CV R² vs n components."""
    component_grid = curves["n"]
    ax.plot(component_grid, curves["insample_r2"] * 100, "o-", color="#2980b9", ms=4, lw=1.0,
            label="in-sample R² (%)")
    ax.plot(component_grid, np.clip(curves["cv_r2"], -0.5, None) * 100, "^--", color="#e67e22",
            ms=4, lw=1.0, label="grouped-CV R² (%)")
    ax.axvline(curves["adaptive_n"], color="#8e44ad", ls="--", lw=1.2,
               label=f"adaptive n={curves['adaptive_n']}")
    ax.axhline(0, color="grey", lw=0.6, ls=":")
    ax.set_xlabel("FPLS components", fontsize=8)
    ax.set_ylabel("R² (%)", fontsize=8)
    ax.set_title(title, fontsize=8)
    ax.set_xticks(component_grid)
    ax.tick_params(labelsize=7)
    ax.grid(alpha=0.3, ls=":")
    ax.legend(fontsize=6)


def run_fpls_mode_selection(depvars=None, months=None, n_splits=N_CV_FOLDS):
    """Phase 1: in-sample/CV R² vs. components + adaptive count, per outcome × block.

    Parameters
    ----------
    depvars  : list[str] — outcomes; defaults to all DEPVARS
    months   : list of (year, month); defaults to all 12 months of 2025
    n_splits : int — CV folds

    Returns
    -------
    Path to the saved selection figure.
    """
    depvars = depvars or DEPVARS
    months  = months  or ALL_MONTHS

    project_dirs      = setup_directories()
    figure_output_dir = Path(project_dirs["figures"]) / "fpls_decomposition"
    figure_output_dir.mkdir(parents=True, exist_ok=True)

    print("\n=== Phase 1: Loading channel fields and outcomes ===")
    channel_bundle     = load_channel_fields(months, project_dirs)
    outcomes_dataframe = load_outcomes(project_dirs)
    all_valid_hours    = channel_bundle["hours"]
    temporal_chunk_labels = _chunk_labels(all_valid_hours)
    time_controls = build_time_controls(all_valid_hours)

    block_matrices = {
        block: build_block_matrix(channel_bundle, fields)
        for block, (fields, _) in BLOCKS.items()
    }

    n_outcome_rows, n_block_cols = len(depvars), len(BLOCKS)
    fig, axes = plt.subplots(
        n_outcome_rows, n_block_cols,
        figsize=(n_block_cols * 4.0, n_outcome_rows * 3.0), squeeze=False)

    for outcome_row_idx, depvar in enumerate(depvars):
        depvar_config = DEPVAR_CONFIGS.get(depvar, {"label": depvar, "transform": "log1p"})
        outcome_values = _transform_y(
            outcomes_dataframe[depvar].reindex(all_valid_hours), depvar_config["transform"]
        ).values
        finite_mask = np.isfinite(outcome_values)
        finite_idx  = np.where(finite_mask)[0]
        print(f"  {depvar}: {int(finite_mask.sum())} valid hours")
        outcome_residual = frisch_waugh_residualize(
            outcome_values[finite_mask], time_controls[finite_mask])
        for block_col_idx, (block, (_, block_label)) in enumerate(BLOCKS.items()):
            X, offsets, _, _ = block_matrices[block]
            X_processed = residualize_and_scale_block(
                X, time_controls, finite_idx, offsets)[finite_mask]
            curves = _fpls_cv_curve(
                X_processed, outcome_residual, temporal_chunk_labels[finite_mask],
                default_modes[block], n_splits)
            panel_title = f"{depvar_config['label']}\n[{block_label.split()[0]} {block}]"
            _plot_selection_panel(axes[outcome_row_idx][block_col_idx], curves, panel_title)
            print(f"  {depvar} / {block}: adaptive n={curves['adaptive_n']}  "
                  f"max CV R²={np.nanmax(curves['cv_r2']):.3f}")

    fig.suptitle("Functional PLS Mode Selection — in-sample vs. grouped-CV R²",
                 fontsize=11, y=0.998)
    fig.tight_layout(rect=[0, 0, 1, 0.97])
    figure_path = _save_fig(fig, figure_output_dir / "fpls_selection.png")
    print(f"  Figure: {figure_path}")
    print(f"  Component caps (default_modes) = {default_modes}")
    print("\nDone — Phase 1 complete.")
    return figure_path


# ═══════════════════════════════════════════════════════════════════════════════
# Section 2: FPLS decomposition — fit functional coefficient and save (Phase 2)
# ═══════════════════════════════════════════════════════════════════════════════

def fit_fpls_block(block_data, y_residual, time_controls, train_idx, test_idx,
                   groups, m_max, outcome_transform):
    """Fit one functional PLS block on training hours; project all hours.

    Residualizes/scales the block field, selects the component count (the package's
    adaptive rule when p < n, else grouped-CV evidence), fits FunctionalPLS, orients
    β so a positive functional index raises the outcome, and returns β(s), the
    per-hour functional index, cumulative in-sample R², and held-out skill.

    Parameters
    ----------
    block_data        : tuple (X, offsets, lat, lon, fields) from build_block_matrix
    y_residual        : ndarray (T,) — residualized outcome aligned to cut hours
    time_controls     : ndarray (T, q) — control matrix (with intercept)
    train_idx         : ndarray — training row positions (outcome finite)
    test_idx          : ndarray — held-out positions for predictive R² (may be empty)
    groups            : ndarray (T,) — chunk ids for grouped CV component selection
    m_max             : int — component cap for this block
    outcome_transform : str — "log1p" or "raw" (only for labelling)

    Returns
    -------
    dict with: beta (p,), index (T,), lat (p,), lon (p,), offsets (C, 2),
               r2_cum (K,), fields (list[str]), n_components (int), test_r2 (float).
    """
    X, offsets, lat, lon, fields = block_data
    X_processed = residualize_and_scale_block(X, time_controls, train_idx, offsets)
    X_train = X_processed[train_idx]
    y_train = y_residual[train_idx]

    n_components = select_n_components(X_train, y_train, m_max, groups[train_idx])
    model = FunctionalPLS(m_max=m_max).fit(X_train, y_train, ds=DS)
    beta_function = model.coef_[:, n_components].copy()

    # Orient β so a higher functional index corresponds to a higher outcome.
    functional_index_all = (X_processed @ beta_function * DS).astype(float)
    if np.corrcoef(functional_index_all[train_idx], y_train)[0, 1] < 0:
        beta_function = -beta_function
        functional_index_all = -functional_index_all

    # Cumulative in-sample R² of the FPLS prediction by component count.
    cumulative_r2 = np.array([
        _r2(y_train, model.predict(X_train, n_components=k))
        for k in range(1, n_components + 1)])

    # Held-out predictive R² (FPLS's own prediction at the selected component count).
    if len(test_idx) >= 20:
        test_r2 = _r2(y_residual[test_idx],
                      model.predict(X_processed[test_idx], n_components=n_components))
    else:
        test_r2 = float("nan")

    return {
        "beta":         beta_function.astype(np.float32),
        "index":        functional_index_all.astype(np.float32),
        "lat":          lat.astype(float),
        "lon":          lon.astype(float),
        "offsets":      np.array(offsets, dtype=int),
        "r2_cum":       cumulative_r2.astype(float),
        "fields":       list(fields),
        "n_components": int(n_components),
        "test_r2":      float(test_r2),
    }


def _modes_path(fpls_dir, cut_key, depvar):
    """Return the .npz path for one (cut, outcome) set of FPLS modes."""
    return Path(fpls_dir) / f"fpls_modes_{cut_key}_{depvar}.npz"


def save_fpls_modes(modes_by_block, hours, y_residual, meta, fpls_dir, cut_key, depvar):
    """Persist one (cut, outcome) set of FPLS block modes and indices to disk.

    Parameters
    ----------
    modes_by_block : dict {block: modes dict from fit_fpls_block}
    hours          : pd.DatetimeIndex — the cut's hours
    y_residual     : ndarray — residualized outcome aligned to hours
    meta           : dict — JSON-serializable metadata
    fpls_dir       : Path
    cut_key        : str
    depvar         : str
    """
    fpls_dir = Path(fpls_dir)
    fpls_dir.mkdir(parents=True, exist_ok=True)
    archive_data = {"hours": hours.values.astype("int64"), "y": y_residual}
    for block_key, block_modes in modes_by_block.items():
        for array_key in FPLS_MODES_KEYS:
            archive_data[f"{block_key}__{array_key}"] = block_modes[array_key]
    npz_output_path = _modes_path(fpls_dir, cut_key, depvar)
    np.savez_compressed(npz_output_path, **archive_data)
    npz_output_path.with_suffix(".json").write_text(json.dumps(meta))
    print(f"  Saved: {npz_output_path.name}")


def load_fpls_modes(fpls_dir, cut_key, depvar):
    """Load one (cut, outcome) set of FPLS modes saved by save_fpls_modes.

    Returns
    -------
    block_modes : dict {block: {beta, index, lat, lon, offsets, r2_cum, fields}}
    hours       : pd.DatetimeIndex
    y_residual  : ndarray
    meta        : dict
    """
    npz_file_path = _modes_path(fpls_dir, cut_key, depvar)
    npz_archive   = np.load(npz_file_path)
    run_metadata  = json.loads(npz_file_path.with_suffix(".json").read_text())
    saved_hours   = pd.DatetimeIndex(npz_archive["hours"].astype("datetime64[ns]"))
    saved_outcome = npz_archive["y"]
    block_modes = {}
    for block_key, channel_field_names in run_metadata["fields"].items():
        block_modes[block_key] = {
            array_key: npz_archive[f"{block_key}__{array_key}"]
            for array_key in FPLS_MODES_KEYS
        }
        block_modes[block_key]["fields"] = channel_field_names
    return block_modes, saved_hours, saved_outcome, run_metadata


def run_fpls_decomposition(depvars=None, months=None, n_components_per_block=None):
    """Phase 2: fit per-block functional PLS for every cut × outcome and save.

    Parameters
    ----------
    depvars                : list[str] — outcomes; defaults to all DEPVARS
    months                 : list of (year, month); defaults to all 12 months of 2025
    n_components_per_block  : dict {block: int} — component caps; defaults to default_modes

    Returns
    -------
    Path to the directory holding the saved modes.
    """
    depvars = depvars or DEPVARS
    months  = months  or ALL_MONTHS
    m_max_by_block = n_components_per_block or default_modes

    project_dirs   = setup_directories()
    fpls_modes_dir = Path(project_dirs["processed"]) / "fpls"
    fpls_modes_dir.mkdir(parents=True, exist_ok=True)

    print("\n=== Phase 2: Loading channel fields and outcomes ===")
    channel_bundle     = load_channel_fields(months, project_dirs)
    outcomes_dataframe = load_outcomes(project_dirs)
    all_valid_hours    = channel_bundle["hours"]

    for cut_key, cut in SAMPLE_CUT.items():
        sample_cut_mask = cut.mask(all_valid_hours, outcomes_dataframe)
        n_cut_hours = int(sample_cut_mask.sum())
        if n_cut_hours < 200:
            print(f"\n=== Phase 2 [{cut.label}]: skipping ({n_cut_hours} hours) ===")
            continue
        print(f"\n=== Phase 2 [{cut.label}]: {n_cut_hours} hours ===")

        cut_bundle      = _subset_bundle(channel_bundle, sample_cut_mask)
        cut_valid_hours = cut_bundle["hours"]
        time_controls   = build_time_controls(cut_valid_hours)
        chunk_labels    = _chunk_labels(cut_valid_hours)
        train_mask, test_mask = make_chunk_splits(cut_valid_hours, seed=RANDOM_STATE)

        block_data = {
            block: (*build_block_matrix(cut_bundle, fields), fields)
            for block, (fields, _) in BLOCKS.items()
        }

        for depvar in depvars:
            if depvar not in outcomes_dataframe.columns:
                print(f"  {depvar}: not in outcomes — skipping")
                continue
            depvar_config     = DEPVAR_CONFIGS.get(depvar, {"label": depvar, "transform": "log1p"})
            outcome_transform = depvar_config["transform"]
            transformed_outcome = _transform_y(
                outcomes_dataframe[depvar].reindex(cut_valid_hours), outcome_transform).values
            finite_mask = np.isfinite(transformed_outcome)
            train_idx = np.where(train_mask & finite_mask)[0]
            test_idx  = np.where(test_mask  & finite_mask)[0]
            if len(train_idx) < 50:
                print(f"  {depvar}: only {len(train_idx)} train hours — skipping")
                continue

            # Residualize the outcome on time controls (NaN-safe: fill then residualize).
            outcome_filled = np.where(finite_mask, transformed_outcome, np.nanmean(transformed_outcome))
            y_residual = frisch_waugh_residualize(outcome_filled, time_controls)

            block_modes = {}
            for block in BLOCKS:
                block_modes[block] = fit_fpls_block(
                    block_data[block], y_residual, time_controls, train_idx, test_idx,
                    chunk_labels, m_max_by_block[block], outcome_transform)
            test_r2_by_block = {b: m["test_r2"] for b, m in block_modes.items()}
            n_comp_by_block  = {b: m["n_components"] for b, m in block_modes.items()}
            print(f"  {depvar}: n_comp={n_comp_by_block}  "
                  f"test R²=" + ", ".join(f"{b}={r:.3f}" for b, r in test_r2_by_block.items()))

            # Store the finite mask via NaN so Phase 3 can drop missing hours.
            y_residual_masked = np.where(finite_mask, y_residual, np.nan)
            run_metadata = {
                "cut_key":      cut_key,
                "cut_label":    cut.label,
                "depvar":       depvar,
                "transform":    outcome_transform,
                "fields":       {b: m["fields"] for b, m in block_modes.items()},
                "n_components": n_comp_by_block,
                "test_r2":      test_r2_by_block,
            }
            save_fpls_modes(block_modes, cut_valid_hours, y_residual_masked,
                            run_metadata, fpls_modes_dir, cut_key, depvar)

    print(f"\nDone — modes saved to {fpls_modes_dir}")
    return fpls_modes_dir


# ═══════════════════════════════════════════════════════════════════════════════
# Section 3: FPLS regression analysis with block standard errors (Phase 3)
# ═══════════════════════════════════════════════════════════════════════════════

def build_index_design(modes_by_block, hours, y_residual):
    """Assemble the regression design from per-block functional indices.

    Columns: one functional index per block, named "idx_{block}".  Rows with any
    missing index or a missing outcome are dropped.

    Parameters
    ----------
    modes_by_block : dict {block: {index: (T,), ...}}
    hours          : pd.DatetimeIndex (T,)
    y_residual     : ndarray (T,) — residualized outcome (may contain NaN)

    Returns
    -------
    design_clean  : pd.DataFrame (T_clean, n_blocks) — one column per block
    outcome_array : ndarray (T_clean,)
    clean_hours   : pd.DatetimeIndex
    block_columns : list[str]
    """
    index_columns = {f"idx_{block}": modes["index"] for block, modes in modes_by_block.items()}
    index_dataframe = pd.DataFrame(index_columns, index=hours)
    outcome_series  = pd.Series(y_residual, index=hours)
    complete_mask   = index_dataframe.notna().all(axis=1) & outcome_series.notna()
    design_clean    = index_dataframe.loc[complete_mask]
    print(f"  Design matrix: {design_clean.shape[0]} hours × {design_clean.shape[1]} block indices")
    return (design_clean, outcome_series.loc[complete_mask].values,
            pd.DatetimeIndex(design_clean.index), list(design_clean.columns))


def run_block_bootstrap_inference(y, X_df, hours, n_boot=N_BLOCK_BOOT,
                                  block_days=BLOCK_DAYS, seed=RANDOM_STATE):
    """OLS on the block functional indices with moving-block bootstrap standard errors.

    Refits OLS on resamples of contiguous `block_days`-day blocks of hours, so the
    sampling distribution respects strong intraday/synoptic dependence (Section 11
    "Inference under temporal dependence").  Each block's index coefficient and its
    bootstrap band measure whether that forecast horizon adds predictive power.

    Parameters
    ----------
    y          : ndarray (T,) — residualized outcome
    X_df       : pd.DataFrame (T, n_blocks) — standardized block indices (no const)
    hours      : pd.DatetimeIndex (T,)
    n_boot     : int — bootstrap resamples
    block_days : int — contiguous days per resampled block
    seed       : int

    Returns
    -------
    result    : SimpleNamespace mimicking a statsmodels result (params, bse, tvalues,
                pvalues, fittedvalues, rsquared, conf_int())
    col_names : list[str] — coefficient names (includes 'const')
    """
    col_names    = ["const"] + list(X_df.columns)
    design_const = np.column_stack([np.ones(len(X_df)), X_df.values])
    point_params = np.linalg.lstsq(design_const, y, rcond=None)[0]
    fitted_values = design_const @ point_params
    rsquared = _r2(y, fitted_values)

    day_index   = hours.normalize()
    unique_days = pd.DatetimeIndex(sorted(day_index.unique()))
    rows_by_day = {day: np.where(day_index == day)[0] for day in unique_days}
    n_blocks    = int(np.ceil(len(unique_days) / block_days))
    max_start   = max(1, len(unique_days) - block_days + 1)
    rng         = np.random.default_rng(seed)

    bootstrap_params = np.full((n_boot, len(col_names)), np.nan)
    for boot_idx in range(n_boot):
        block_starts = rng.integers(0, max_start, size=n_blocks)
        sampled_rows = np.concatenate([
            np.concatenate([rows_by_day[d] for d in unique_days[start:start + block_days]])
            for start in block_starts])
        try:
            bootstrap_params[boot_idx] = np.linalg.lstsq(
                design_const[sampled_rows], y[sampled_rows], rcond=None)[0]
        except np.linalg.LinAlgError:
            continue

    valid_draws = bootstrap_params[~np.isnan(bootstrap_params).any(axis=1)]
    bootstrap_se = valid_draws.std(axis=0, ddof=1)
    conf_intervals = np.column_stack([
        np.percentile(valid_draws, 2.5,  axis=0),
        np.percentile(valid_draws, 97.5, axis=0)])
    with np.errstate(divide="ignore", invalid="ignore"):
        t_values = np.where(bootstrap_se > 0, point_params / bootstrap_se, 0.0)
    p_values = 2.0 * scipy_stats.norm.sf(np.abs(t_values))

    print(f"  Block bootstrap ({len(valid_draws)} draws, {block_days}d blocks): R²={rsquared:.3f}, "
          f"{(p_values[1:] < 0.05).sum()} of {len(col_names)-1} block indices p<0.05")
    return SimpleNamespace(
        params=point_params, bse=bootstrap_se, tvalues=t_values, pvalues=p_values,
        fittedvalues=fitted_values, rsquared=rsquared,
        conf_int=lambda: conf_intervals,
    ), col_names


def plot_fpls_beta_maps(modes_by_block, depvar_label, cut_label, output_path):
    """Coefficient-function maps β(s) per channel — the headline FPLS deliverable.

    One row per block, two columns (wind, temp); each panel maps β(s) over ERCOT,
    signed so red = a locally positive field value raises the outcome.

    Parameters
    ----------
    modes_by_block : dict {block: modes dict with beta, lat, lon, offsets, fields}
    depvar_label   : str
    cut_label      : str
    output_path    : Path
    """
    from matplotlib.colors import TwoSlopeNorm

    cartopy_crs    = _get_cartopy_crs()
    map_projection = cartopy_crs.PlateCarree() if cartopy_crs is not None else None
    subplot_kwargs = {"projection": map_projection} if map_projection is not None else {}

    n_block_rows = len(modes_by_block)
    fig = plt.figure(figsize=(7.5, max(2.4, n_block_rows * 2.4)))
    gridspec = fig.add_gridspec(n_block_rows, 2, hspace=0.4, wspace=0.2)

    for row_idx, (block, modes) in enumerate(modes_by_block.items()):
        beta_function = modes["beta"]
        block_scale = np.percentile(np.abs(beta_function), 99) or 1.0
        beta_norm   = TwoSlopeNorm(vmin=-block_scale, vcenter=0.0, vmax=block_scale)
        for col_idx, (field, (start, end)) in enumerate(zip(modes["fields"], modes["offsets"])):
            ax = fig.add_subplot(gridspec[row_idx, col_idx], **subplot_kwargs)
            scatter_kwargs = {
                "c": beta_function[start:end], "cmap": "RdBu_r", "norm": beta_norm,
                "s": _grid_marker_size(modes["lon"][start:end]), "rasterized": True,
            }
            if map_projection is not None:
                scatter_kwargs["transform"] = map_projection
            scatter = ax.scatter(modes["lon"][start:end], modes["lat"][start:end], **scatter_kwargs)
            _draw_texas(ax)
            ax.set_title(f"{FIELD_LABELS.get(field, field)}\nβ(s)  [{block}]", fontsize=7, pad=2)
            fig.colorbar(scatter, ax=ax, shrink=0.7, pad=0.02)

    fig.suptitle(f"FPLS Coefficient Function β(s) — {depvar_label}  [{cut_label}]",
                 fontsize=10, y=0.998)
    return _save_fig(fig, output_path)


def plot_fpls_block_forest(result, col_names, modes_by_block, depvar_label,
                           cut_label, output_path):
    """Block functional-index coefficients with 95% block-bootstrap CIs.

    Parameters
    ----------
    result         : block-bootstrap result (params, conf_int(), pvalues)
    col_names      : list[str] — coefficient names (includes 'const')
    modes_by_block : dict — used for the per-block selected component count label
    depvar_label   : str
    cut_label      : str
    output_path    : Path
    """
    coef_series = pd.Series(result.params, index=col_names)
    conf_int    = result.conf_int()
    ci_lower    = pd.Series(conf_int[:, 0], index=col_names)
    ci_upper    = pd.Series(conf_int[:, 1], index=col_names)
    pval_series = pd.Series(result.pvalues, index=col_names)

    block_cols = [c for c in col_names if c.startswith("idx_")]
    fig, ax = plt.subplots(figsize=(7, max(2.2, len(block_cols) * 0.9)))
    for row_idx, col in enumerate(block_cols):
        coef = coef_series[col]
        pval = pval_series[col]
        color = COLOR_P001 if pval < 0.01 else COLOR_P005 if pval < 0.05 else COLOR_NSIG
        ax.errorbar(coef, row_idx,
                    xerr=[[max(coef - ci_lower[col], 0)], [max(ci_upper[col] - coef, 0)]],
                    fmt="o", color=color, markersize=7, capsize=4, linewidth=1.5)
    ax.axvline(0, color="k", lw=0.8, ls="--")
    ax.set_yticks(range(len(block_cols)))
    block_keys = [c.replace("idx_", "") for c in block_cols]
    ax.set_yticklabels(
        [f"{b}\n({modes_by_block[b]['beta'].shape[0]} px, "
         f"{int(np.size(modes_by_block[b]['r2_cum']))} comp)" for b in block_keys], fontsize=8)
    ax.invert_yaxis()
    ax.grid(axis="x", ls=":", lw=0.5, alpha=0.6)
    ax.set_xlabel("OLS coefficient on block functional index (standardized)", fontsize=9)
    ax.set_title(f"FPLS Block Indices — {depvar_label}  [{cut_label}]\n"
                 "block-bootstrap 95% CI · dark blue p<0.01 ● light blue p<0.05 ● grey n.s.",
                 fontsize=8.5)
    fig.tight_layout()
    return _save_fig(fig, output_path)


def plot_fpls_block_heatmap(all_block_stats, output_path, cut_label):
    """Heatmap of block significance (|t| from block s.e.) across outcomes.

    Parameters
    ----------
    all_block_stats : dict {depvar: {block: (abs_t, p_val)}}
    output_path     : Path
    cut_label       : str
    """
    available_blocks  = [b for b in BLOCKS if any(b in s for s in all_block_stats.values())]
    available_depvars = list(all_block_stats.keys())
    if not available_blocks or not available_depvars:
        return None

    t_matrix = np.full((len(available_blocks), len(available_depvars)), np.nan)
    p_matrix = np.full((len(available_blocks), len(available_depvars)), np.nan)
    for col_idx, depvar in enumerate(available_depvars):
        for row_idx, block in enumerate(available_blocks):
            if block in all_block_stats[depvar]:
                t_matrix[row_idx, col_idx], p_matrix[row_idx, col_idx] = all_block_stats[depvar][block]

    depvar_labels = [DEPVAR_CONFIGS.get(dv, {}).get("label", dv) for dv in available_depvars]
    block_labels  = [BLOCKS[b][1] for b in available_blocks]
    colormap_max  = np.nanpercentile(t_matrix, 95) if not np.all(np.isnan(t_matrix)) else 1.0

    fig, ax = plt.subplots(figsize=(max(6, len(available_depvars) * 1.4),
                                    max(3, len(available_blocks) * 0.7)))
    heatmap = ax.imshow(t_matrix, aspect="auto", cmap="viridis", vmin=0, vmax=colormap_max)
    ax.set_xticks(range(len(available_depvars)))
    ax.set_xticklabels(depvar_labels, rotation=30, ha="right", fontsize=8)
    ax.set_yticks(range(len(available_blocks)))
    ax.set_yticklabels(block_labels, fontsize=8)
    for row_idx in range(len(available_blocks)):
        for col_idx in range(len(available_depvars)):
            t_value = t_matrix[row_idx, col_idx]
            if np.isnan(t_value):
                continue
            ax.text(col_idx, row_idx, _sig_stars(p_matrix[row_idx, col_idx]),
                    ha="center", va="center", fontsize=9,
                    color="white" if t_value > colormap_max * 0.55 else "black",
                    fontweight="bold")
    fig.colorbar(heatmap, ax=ax, shrink=0.7, pad=0.02, label="|t| (block s.e.)")
    ax.set_title(f"FPLS block-index significance: Blocks × Outcomes  [{cut_label}]\n"
                 "*** p<0.001  ** p<0.01  * p<0.05", fontsize=9)
    fig.tight_layout()
    return _save_fig(fig, output_path)


def _standardize_block_indices(design_matrix):
    """Standardize each block-index column to zero mean / unit variance."""
    standardized = design_matrix.copy()
    column_std = standardized.std().clip(lower=1e-12)
    return (standardized - standardized.mean()) / column_std


def run_fpls_analysis(depvars=None, months=None):
    """Phase 3: regress outcomes on block functional indices (block s.e.) and plot.

    For each cut × outcome, loads the Phase-2 modes, regresses the residualized
    outcome on the per-block functional indices with moving-block bootstrap standard
    errors, saves a coefficient table, and produces a β(s) coefficient-function map,
    a block coefficient forest, and a block × outcome significance heatmap per cut.

    Parameters
    ----------
    depvars : list[str] — outcomes; defaults to all DEPVARS
    months  : list of (year, month) — used only if Phase 2 must be run

    Returns
    -------
    dict {cut_key: {depvar: {result, col_names, r2, n_hours}}}
    """
    depvars = depvars or DEPVARS

    project_dirs      = setup_directories()
    fpls_modes_dir    = Path(project_dirs["processed"]) / "fpls"
    base_figure_dir   = Path(project_dirs["figures"]) / "fpls_analysis"
    tables_output_dir = Path(project_dirs["tables"])
    tables_output_dir.mkdir(parents=True, exist_ok=True)

    results_by_cut = {}
    for cut_key, cut in SAMPLE_CUT.items():
        if not any(_modes_path(fpls_modes_dir, cut_key, dv).exists() for dv in depvars):
            print(f"\n=== Phase 3 [{cut.label}]: no saved modes — running Phase 2 ===")
            run_fpls_decomposition(depvars=depvars, months=months)

        print(f"\n{'='*60}\n=== Phase 3 [{cut.label}] ===\n{'='*60}")
        cut_figure_dir = base_figure_dir / cut_key
        cut_table_dir  = tables_output_dir / "fpls" / cut_key
        cut_figure_dir.mkdir(parents=True, exist_ok=True)
        cut_table_dir.mkdir(parents=True, exist_ok=True)

        cut_results = {}
        cut_block_stats = {}
        for depvar in depvars:
            if not _modes_path(fpls_modes_dir, cut_key, depvar).exists():
                continue
            block_modes, cut_hours, y_residual, run_metadata = load_fpls_modes(
                fpls_modes_dir, cut_key, depvar)

            design_matrix, outcome_array, clean_hours, block_columns = build_index_design(
                block_modes, cut_hours, y_residual)
            if len(design_matrix) < 200:
                print(f"  {depvar}: only {len(design_matrix)} clean hours — skipping")
                continue

            design_standardized = _standardize_block_indices(design_matrix)
            result, col_names = run_block_bootstrap_inference(
                outcome_array, design_standardized, clean_hours)
            cut_block_stats[depvar] = {
                col.replace("idx_", ""): (abs(result.tvalues[i]), result.pvalues[i])
                for i, col in enumerate(col_names) if col.startswith("idx_")
            }
            print(f"  {depvar}: R²(residualized)={result.rsquared:.4f}  N={len(design_matrix)}")

            save_coef_table(result, col_names,
                            cut_table_dir / f"fpls_coefficients_{depvar}.csv")
            cut_results[depvar] = {
                "result":    result,
                "col_names": col_names,
                "r2":        result.rsquared,
                "n_hours":   len(design_matrix),
            }

            depvar_label = DEPVAR_CONFIGS.get(depvar, {}).get("label", depvar)
            plot_fpls_beta_maps(block_modes, depvar_label, cut.label,
                                cut_figure_dir / f"fpls_beta_{depvar}.png")
            plot_fpls_block_forest(result, col_names, block_modes, depvar_label,
                                   cut.label, cut_figure_dir / f"fpls_blocks_{depvar}.png")

        if cut_block_stats:
            plot_fpls_block_heatmap(cut_block_stats,
                                    cut_figure_dir / "fpls_block_heatmap.png", cut.label)
        results_by_cut[cut_key] = cut_results

    print(f"\nDone — figures under {base_figure_dir}")
    return results_by_cut


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    """CLI entry point for the three-phase functional-PLS workflow."""
    arg_parser = argparse.ArgumentParser(
        description="Functional PLS analysis of ERCOT weather-error fields (three phases)",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    arg_parser.add_argument(
        "--task", choices=["select", "decompose", "analyze", "all"], default="all",
        help=("Phase to run: select (Phase 1), decompose (Phase 2), "
              "analyze (Phase 3), all (default)"))
    arg_parser.add_argument("--depvars", nargs="*", default=None,
                            help="Outcome variables (default: all)")
    cli_args = arg_parser.parse_args()

    if cli_args.task in ("select", "all"):
        run_fpls_mode_selection(depvars=cli_args.depvars)
    if cli_args.task in ("decompose", "all"):
        run_fpls_decomposition(depvars=cli_args.depvars)
    if cli_args.task in ("analyze", "all"):
        run_fpls_analysis(depvars=cli_args.depvars)


if __name__ == "__main__":
    main()
