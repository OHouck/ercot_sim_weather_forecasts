"""
PLS analysis of ERCOT weather-error fields — three-phase workflow.

This module mirrors the data-management pipeline of `analysis.eof_analysis`
(shared channel bundle, sample cuts, HAC-OLS inference, F-test heatmaps) but
replaces the unsupervised EOF decomposition with supervised Partial Least
Squares (PLS).  Because PLS finds the modes of *covariability between the
forecast-error fields and a given outcome*, the latent modes are
outcome-specific: they are refit for every (sample cut × outcome).

Blocks (one PLS per forecast horizon; the two channels are stacked so PLS sees
their joint covariance with the outcome):
  1. dayahead  : GFS day-ahead 100m wind-speed error + temperature error
  2. hourahead : HRRR 1h 100m wind-speed error + temperature error
  3. realized  : ERA5 realized 100m wind speed + temperature

Sample cuts (SAMPLE_CUT, imported from eof_analysis) subset the hours used for
each fit: full year, four meteorological seasons, and RUC-deployed hours.

━━━ Phase 1 — run_pls_mode_selection ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
For every (outcome × block) on the full dataset, run grouped (weekly-chunk)
K-fold cross-validation over a grid of component counts.  Produces one figure
per run showing, vs. the number of components: cumulative variance explained in
the predictors (X) and in the outcome (Y), and the out-of-sample CV R².  Review
these to choose `default_modes`.
Output: figures/pls_decomposition/pls_selection.png

━━━ Phase 2 — run_pls_decomposition ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
For each cut × outcome, fit a PLS per block on the training chunks using
`default_modes`, evaluate predictive skill on the held-out test chunks, and save
the supervised loadings + projected per-hour scores to disk (consumed by Phase 3
without re-fitting).
Output: processed/pls/pls_modes_{cut}_{outcome}.npz (+ .json meta)

━━━ Phase 3 — run_pls_analysis ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
For each cut × outcome, load the saved scores, regress the outcome on the PLS
component scores (HAC standard errors), and produce:
  • one figure per outcome: component loading maps (with % X-variance) beside a
    coefficient forest plot with 95% CIs,
  • a joint-F-test heatmap (block × outcome) per cut.
Output: figures/pls_analysis/{cut}/

Usage:
    uv run python -m analysis.pls_analysis_v2 --task select      # Phase 1
    uv run python -m analysis.pls_analysis_v2 --task decompose   # Phase 2
    uv run python -m analysis.pls_analysis_v2 --task analyze     # Phase 3
    uv run python -m analysis.pls_analysis_v2                    # all three
"""

import argparse
import json
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.base import BaseEstimator, RegressorMixin
from sklearn.cross_decomposition import PLSRegression
from sklearn.model_selection import GroupKFold, cross_val_score

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories

from analysis.pca_decomposition import (
    load_channel_fields, ALL_MONTHS, RANDOM_STATE, CHUNK_DAYS, _r2,
    FIELD_LABELS, _normalize_comp, _sig_stars, _draw_texas, _get_cartopy_crs,
    _grid_marker_size, make_chunk_splits,
    COLOR_P001, COLOR_P005, COLOR_NSIG,
)
from analysis.pca_mode_analysis import (
    load_outcomes, standardize_pca_cols, run_ols_inference, save_coef_table,
)
from analysis.eof_analysis import (
    DECOMPOSITIONS, SAMPLE_CUT, DEPVAR_CONFIGS, DEPVARS,
    _subset_bundle, _save_fig,
)

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# ── Constants ─────────────────────────────────────────────────────────────────

# One PLS block per forecast horizon; reuse the channel pairings/labels that
# eof_analysis already defines (each block stacks its [wind, temp] channels).
BLOCKS = DECOMPOSITIONS  # {block_key: ([wind_field, temp_field], label)}

# Number of PLS components retained per block.  Review the Phase-1 selection
# figure, then update these before running Phases 2 and 3.
default_modes = {
    "dayahead":  3,
    "hourahead": 3,
    "realized":  3,
}

# Sparse PLS: nonzero pixels per component per block.
# Chosen from Phase-1 2-D CV sweep (see pls_sparse_selection.png):
#   dayahead  p=1,634  → keep_x=1000 (61%) optimal for congestion/curtailment/carbon
#   hourahead p=10,372 → keep_x=2000 (19%) balances congestion (best 2000) and curtailment (best 1000)
#   realized  p=10,372 → keep_x=7000 (67%) consistently best for all main outcomes
default_keep_x = {
    "dayahead":  1000,
    "hourahead": 2000,
    "realized":  7000,
}

N_CV_FOLDS        = 5
N_COMPONENTS_GRID = [1, 2, 3, 4, 5, 6]
# Candidate keep_x values for the Phase-1 sparse CV sweep.
# dayahead block has p=1,634 columns; hourahead/realized have p=10,372.
# Grid spans 12%–100% of dayahead and 2%–67% of hourahead/realized.
KEEP_X_GRID       = [200, 500, 1000, 2000, 4000, 7000]
# Array keys persisted per block in the .npz archive — must stay in sync across save/load.
PLS_MODES_KEYS    = ("loadings", "scores", "lat", "lon", "offsets", "evr_x", "r2_y_cum")


# ═══════════════════════════════════════════════════════════════════════════════
# Sparse PLS
# ═══════════════════════════════════════════════════════════════════════════════

def _soft_threshold_keep(weight_vector, keep_x):
    """Soft-threshold a weight vector to keep at most `keep_x` nonzeros.

    Sets λ to the (keep_x+1)-th largest |wᵢ|, then shrinks surviving weights
    toward zero by λ (mixOmics-style L1 penalty on the NIPALS weight vector).
    Falls back to hard top-keep_x selection only when ties cause all weights to
    vanish after soft-thresholding.

    Parameters
    ----------
    weight_vector : ndarray (p,) — raw covariance direction X^T y_resid
    keep_x        : int          — maximum nonzero weights to retain

    Returns
    -------
    thresholded_weights : ndarray (p,) — soft-thresholded weight vector (not normalised)
    """
    n_features = weight_vector.size
    if keep_x >= n_features:
        return weight_vector
    abs_weights = np.abs(weight_vector)
    soft_threshold_lambda = np.partition(abs_weights, n_features - keep_x - 1)[n_features - keep_x - 1]
    thresholded_weights = np.sign(weight_vector) * np.maximum(abs_weights - soft_threshold_lambda, 0.0)
    if not np.any(thresholded_weights):
        # Degenerate (ties at λ): hard top-keep_x selection as fallback
        top_weight_indices = np.argpartition(abs_weights, n_features - keep_x)[n_features - keep_x:]
        thresholded_weights = np.zeros_like(weight_vector)
        thresholded_weights[top_weight_indices] = weight_vector[top_weight_indices]
    return thresholded_weights


class SparsePLS(BaseEstimator, RegressorMixin):
    """Sparse PLS for a univariate response via NIPALS with L1-penalised weights.

    Each component's X-weight vector is soft-thresholded so that at most
    `keep_x` pixels are nonzero (Chun & Keleş 2010; Lê Cao et al. 2008).
    Exposes `x_weights_` (W, sparse), `x_loadings_` (P, dense), and
    `x_rotations_` (W* = W(P^T W)^{-1}, for computing scores without deflation)
    so the interface is parallel to sklearn's `PLSRegression`.

    Parameters
    ----------
    n_components : int — latent components to extract
    keep_x       : int — nonzero pixel weights per component

    Notes
    -----
    With a univariate response, the NIPALS weight direction X^T y is computed
    in one step (no inner iteration), so there is no convergence loop.
    """

    def __init__(self, n_components=2, keep_x=100):
        self.n_components = n_components
        self.keep_x       = keep_x

    def fit(self, X, y):
        """Fit sparse NIPALS PLS on training data.

        Parameters
        ----------
        X : ndarray (n, p) — column-standardised predictors
        y : ndarray (n,)   — response (centred internally)

        Returns
        -------
        self
        """
        X = np.asarray(X, dtype=float)
        y = np.asarray(y, dtype=float).ravel()

        self.x_mean_ = X.mean(axis=0)
        self.y_mean_ = y.mean()
        X_centered = X - self.x_mean_
        y_centered = y - self.y_mean_

        n_samples, n_features = X_centered.shape
        n_effective_components = min(self.n_components, n_samples - 1, n_features)

        sparse_weight_matrix = np.zeros((n_features, n_effective_components))
        loading_matrix       = np.zeros((n_features, n_effective_components))
        y_loading_vector     = np.zeros(n_effective_components)

        for component_idx in range(n_effective_components):
            raw_weight_vector = X_centered.T @ y_centered
            raw_weight_vector = _soft_threshold_keep(raw_weight_vector, self.keep_x)
            weight_vector_norm = np.linalg.norm(raw_weight_vector)
            if weight_vector_norm == 0:
                n_effective_components = component_idx
                sparse_weight_matrix = sparse_weight_matrix[:, :n_effective_components]
                loading_matrix       = loading_matrix[:, :n_effective_components]
                y_loading_vector     = y_loading_vector[:n_effective_components]
                break
            raw_weight_vector /= weight_vector_norm
            latent_score_vector = X_centered @ raw_weight_vector
            score_squared_norm  = latent_score_vector @ latent_score_vector
            loading_matrix[:, component_idx]   = (X_centered.T @ latent_score_vector) / score_squared_norm
            y_loading_vector[component_idx]    = (y_centered    @ latent_score_vector) / score_squared_norm
            X_centered -= np.outer(latent_score_vector, loading_matrix[:, component_idx])
            y_centered -= y_loading_vector[component_idx] * latent_score_vector
            sparse_weight_matrix[:, component_idx] = raw_weight_vector

        self.x_weights_   = sparse_weight_matrix                             # (p, K) sparse
        self.x_loadings_  = loading_matrix                                   # (p, K) dense
        # Rotation W* = W(P^T W)^{-1}: maps X_centered → scores without deflation.
        # rcond=1e-3 caps the condition number to prevent blow-up when the outcome
        # signal is weak (e.g. near-zero congestion cost in summer).
        self.x_rotations_ = (sparse_weight_matrix @ np.linalg.pinv(
                                 loading_matrix.T @ sparse_weight_matrix, rcond=1e-3)
                             if n_effective_components > 0 else sparse_weight_matrix)
        # β = W* C: maps X_centered → predicted y
        self._beta = (self.x_rotations_ @ y_loading_vector
                     if n_effective_components > 0 else np.zeros(n_features))
        self.coef_ = self._beta.reshape(1, -1)
        return self

    def predict(self, X):
        """Predict response for new data.

        Parameters
        ----------
        X : ndarray (m, p) — column-standardised predictors

        Returns
        -------
        ndarray (m,)
        """
        X = np.asarray(X, dtype=float)
        return (X - self.x_mean_) @ self._beta + self.y_mean_


# ═══════════════════════════════════════════════════════════════════════════════
# Section 0: Shared data helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _chunk_labels(hours, chunk_days=CHUNK_DAYS):
    """Assign each hour to a contiguous multi-day chunk for grouped CV.

    Grouping CV folds by temporal chunks (rather than shuffling individual
    hours) keeps autocorrelated neighbours together, giving an honest
    out-of-sample estimate.

    Parameters
    ----------
    hours      : pd.DatetimeIndex
    chunk_days : int — days per chunk

    Returns
    -------
    ndarray int (len(hours),) — chunk id per hour
    """
    date_of_each_hour = hours.normalize()
    unique_dates      = pd.DatetimeIndex(sorted(date_of_each_hour.unique()))
    chunk_id_per_date = pd.Series(np.arange(len(unique_dates)) // chunk_days, index=unique_dates)
    return date_of_each_hour.map(chunk_id_per_date).values


def build_block_matrix(bundle, fields):
    """Stack one horizon block's channel fields into a single (T, n_cells) matrix.

    Each channel's in-ERCOT land cells are flattened and concatenated
    horizontally so PLS sees the joint wind+temp field.

    Parameters
    ----------
    bundle : dict from load_channel_fields
    fields : list[str] — channel names for this block (e.g. [wind, temp])

    Returns
    -------
    X       : ndarray (T, n_cells_total)
    offsets : list[(start, end)] — column slice per field
    lats    : ndarray (n_cells_total,) — cell latitude, aligned with X columns
    lons    : ndarray (n_cells_total,) — cell longitude
    """
    channel_data_list = []
    offsets           = []
    lats              = []
    lons              = []
    column_start_offset = 0
    for field in fields:
        field_data_array  = bundle["channel_da"][field]
        valid_land_mask   = ~bundle["nan_all"][field]
        flattened_channel_data = field_data_array.values[:, valid_land_mask]   # (T, n_land)
        lon_grid, lat_grid = np.meshgrid(
            field_data_array["longitude"].values,
            field_data_array["latitude"].values,
        )
        channel_data_list.append(flattened_channel_data)
        offsets.append((column_start_offset, column_start_offset + flattened_channel_data.shape[1]))
        lats.append(lat_grid[valid_land_mask])
        lons.append(lon_grid[valid_land_mask])
        column_start_offset += flattened_channel_data.shape[1]
    return (np.concatenate(channel_data_list, axis=1), offsets,
            np.concatenate(lats), np.concatenate(lons))


def _transform_y(series, transform):
    """Apply the configured outcome transform (log1p or raw) to a Series."""
    if transform == "log1p":
        return np.log1p(series.clip(lower=0))
    return series


def _standardize_columns(X, train_idx):
    """Column-standardize X using training-row statistics.

    Parameters
    ----------
    X         : ndarray (T, p)
    train_idx : ndarray of training row positions

    Returns
    -------
    X_std : ndarray (T, p) — (X - mu) / sigma using train mu, sigma
    """
    train_column_means = X[train_idx].mean(axis=0)
    train_column_stds  = X[train_idx].std(axis=0)
    train_column_stds[train_column_stds < 1e-9] = 1.0
    return (X - train_column_means) / train_column_stds


# ═══════════════════════════════════════════════════════════════════════════════
# Section 1: PLS mode selection (Phase 1)
# ═══════════════════════════════════════════════════════════════════════════════

def _pls_variance_curves(X_std, y, n_grid, groups, n_splits):
    """Cross-validate PLS and compute cumulative X / Y variance vs. n_components.

    Parameters
    ----------
    X_std    : ndarray (T, p) — column-standardized predictors (NaN-free)
    y        : ndarray (T,)   — transformed outcome (NaN-free)
    n_grid   : list[int]      — component counts to evaluate
    groups   : ndarray (T,)   — chunk ids for grouped CV
    n_splits : int

    Returns
    -------
    dict with arrays: n (modes), cv_r2, cumvar_x_pct, cumvar_y_pct; scalar best_n.
    """
    max_feasible_components = min(X_std.shape) - 1
    n_grid   = [n_comp for n_comp in n_grid if n_comp <= max_feasible_components]
    n_splits = min(n_splits, len(np.unique(groups)))

    # Out-of-sample CV R² for each component count.
    cv_r2_scores  = []
    grouped_kfold = GroupKFold(n_splits=n_splits)
    for n_comp in n_grid:
        cv_fold_scores = cross_val_score(
            PLSRegression(n_components=n_comp, scale=False),
            X_std, y, cv=grouped_kfold, groups=groups, scoring="r2", n_jobs=-1,
        )
        cv_r2_scores.append(float(cv_fold_scores.mean()))

    # In-sample cumulative variance explained in X and Y from a single K_max fit.
    max_n_components = max(n_grid)
    pls_model        = PLSRegression(n_components=max_n_components, scale=False).fit(X_std, y)
    x_scores_matrix, x_loadings_matrix = pls_model.x_scores_, pls_model.x_loadings_
    X_centered = X_std - X_std.mean(axis=0)
    per_component_x_variance = (x_scores_matrix ** 2).sum(axis=0) * (x_loadings_matrix ** 2).sum(axis=0)
    cumulative_x_variance_pct = np.cumsum(per_component_x_variance) / (X_centered ** 2).sum() * 100.0

    y_centered              = y - y.mean()
    y_total_sum_of_squares  = float(y_centered @ y_centered)
    n_components_array      = np.array(n_grid)
    cumulative_y_variance_list = []
    for n_comp in n_grid:
        projection_coefficients = np.linalg.lstsq(x_scores_matrix[:, :n_comp], y_centered, rcond=None)[0]
        y_residuals = y_centered - x_scores_matrix[:, :n_comp] @ projection_coefficients
        cumulative_y_variance_list.append(
            (1 - float(y_residuals @ y_residuals) / max(y_total_sum_of_squares, 1e-12)) * 100.0
        )

    optimal_n_components = int(n_components_array[int(np.argmax(cv_r2_scores))])
    return {
        "n":            n_components_array,
        "cv_r2":        np.array(cv_r2_scores),
        "cumvar_x_pct": cumulative_x_variance_pct[n_components_array - 1],
        "cumvar_y_pct": np.array(cumulative_y_variance_list),
        "best_n":       optimal_n_components,
    }


def _plot_selection_panel(ax, curves, title):
    """Draw one mode-selection panel: cumulative X/Y variance + CV R² vs n."""
    n_components_array = curves["n"]
    ax.plot(n_components_array, curves["cumvar_x_pct"], "o-", color="#2980b9", ms=4, lw=1.0,
            label="cum. X variance (%)")
    ax.plot(n_components_array, curves["cumvar_y_pct"], "s-", color="#27ae60", ms=4, lw=1.0,
            label="cum. Y variance (%)")
    ax.plot(n_components_array, np.clip(curves["cv_r2"], 0, None) * 100, "^--", color="#e67e22",
            ms=4, lw=1.0, label="CV R² (%)")
    ax.axvline(curves["best_n"], color="#c0392b", ls=":", lw=1.2,
               label=f"best CV n={curves['best_n']}")
    ax.set_xlabel("PLS components", fontsize=8)
    ax.set_ylabel("Variance / R² (%)", fontsize=8)
    ax.set_title(title, fontsize=8)
    ax.set_xticks(n_components_array)
    ax.tick_params(labelsize=7)
    ax.grid(alpha=0.3, ls=":")
    ax.legend(fontsize=6)


def _sparse_cv_sweep(X_std, y, n_grid, keep_x_grid, groups, n_splits):
    """2-D grouped CV sweep over (n_components × keep_x) for Sparse PLS.

    Parameters
    ----------
    X_std       : ndarray (T, p) — column-standardized predictors (NaN-free)
    y           : ndarray (T,)   — transformed outcome (NaN-free)
    n_grid      : list[int]      — component counts to evaluate
    keep_x_grid : list[int]      — nonzero-pixel counts to evaluate
    groups      : ndarray (T,)   — chunk ids for grouped CV
    n_splits    : int

    Returns
    -------
    cv_results_by_params : dict {(n_components, nonzero_pixel_count): {"r2_mean": float, "r2_std": float}}
    optimal_n_components : int
    optimal_keep_x       : int
    """
    max_feasible_components = min(X_std.shape) - 1
    n_grid      = [n_comp          for n_comp          in n_grid      if n_comp          <= max_feasible_components]
    keep_x_grid = [nonzero_pixels  for nonzero_pixels  in keep_x_grid if nonzero_pixels  <  X_std.shape[1]]
    n_splits    = min(n_splits, len(np.unique(groups)))

    grouped_kfold        = GroupKFold(n_splits=n_splits)
    cv_results_by_params = {}
    for nonzero_pixel_count in keep_x_grid:
        for n_components in n_grid:
            cv_fold_scores = cross_val_score(
                SparsePLS(n_components=n_components, keep_x=nonzero_pixel_count),
                X_std, y, cv=grouped_kfold, groups=groups, scoring="r2", n_jobs=-1,
            )
            cv_results_by_params[(n_components, nonzero_pixel_count)] = {
                "r2_mean": float(cv_fold_scores.mean()),
                "r2_std":  float(cv_fold_scores.std()),
            }

    optimal_n_components, optimal_keep_x = max(
        cv_results_by_params, key=lambda params: cv_results_by_params[params]["r2_mean"]
    )
    return cv_results_by_params, optimal_n_components, optimal_keep_x


def _plot_sparse_panel(ax, results_2d, title, best_n, best_kx):
    """Heatmap of CV R² over (keep_x rows × n_components cols) for one panel.

    Parameters
    ----------
    ax         : matplotlib Axes
    results_2d : dict {(n_components, nonzero_pixel_count): {"r2_mean": float}}
    title      : str
    best_n     : int — optimal n_components (highlighted in red)
    best_kx    : int — optimal keep_x (highlighted in red)
    """
    unique_n_components  = sorted({n_comp     for n_comp,     _              in results_2d})
    unique_keep_x_values = sorted({keep_x_val for _,          keep_x_val     in results_2d})
    cv_r2_grid = np.full((len(unique_keep_x_values), len(unique_n_components)), np.nan)
    for keep_x_row_idx, keep_x_value in enumerate(unique_keep_x_values):
        for n_comp_col_idx, n_comp_value in enumerate(unique_n_components):
            if (n_comp_value, keep_x_value) in results_2d:
                cv_r2_grid[keep_x_row_idx, n_comp_col_idx] = results_2d[(n_comp_value, keep_x_value)]["r2_mean"]

    valid_grid_values = cv_r2_grid[~np.isnan(cv_r2_grid)]
    colormap_vmin, colormap_vmax = (
        (valid_grid_values.min(), valid_grid_values.max()) if valid_grid_values.size > 0 else (0, 1)
    )
    colormap_midpoint = (colormap_vmin + colormap_vmax) / 2

    heatmap_image = ax.imshow(cv_r2_grid, aspect="auto", origin="lower", cmap="viridis",
                              vmin=colormap_vmin, vmax=colormap_vmax)
    ax.set_xticks(range(len(unique_n_components)));  ax.set_xticklabels(unique_n_components,  fontsize=7)
    ax.set_yticks(range(len(unique_keep_x_values))); ax.set_yticklabels(unique_keep_x_values, fontsize=7)
    ax.set_xlabel("n_components", fontsize=8)
    ax.set_ylabel("keep_x (nonzero pixels)", fontsize=8)
    ax.set_title(title, fontsize=8)
    ax.tick_params(labelsize=7)

    for keep_x_row_idx, keep_x_value in enumerate(unique_keep_x_values):
        for n_comp_col_idx, n_comp_value in enumerate(unique_n_components):
            grid_cell_r2 = cv_r2_grid[keep_x_row_idx, n_comp_col_idx]
            if np.isnan(grid_cell_r2):
                continue
            ax.text(n_comp_col_idx, keep_x_row_idx, f"{grid_cell_r2:.2f}",
                    ha="center", va="center", fontsize=6,
                    color="white" if grid_cell_r2 > colormap_midpoint else "black")

    if best_n in unique_n_components and best_kx in unique_keep_x_values:
        best_n_col_idx  = unique_n_components.index(best_n)
        best_kx_row_idx = unique_keep_x_values.index(best_kx)
        ax.add_patch(plt.Rectangle((best_n_col_idx - 0.5, best_kx_row_idx - 0.5), 1, 1,
                                   fill=False, edgecolor="#c0392b", linewidth=2.0))

    plt.colorbar(heatmap_image, ax=ax, shrink=0.85, pad=0.02, label="CV R²")


def run_pls_mode_selection(depvars=None, months=None, n_grid=None,
                           n_splits=N_CV_FOLDS, keep_x_grid=None):
    """Phase 1: CV scree plots (dense) and optional sparse heatmaps.

    Always produces a dense line-chart figure (`pls_selection.png`) showing
    cumulative X/Y variance and out-of-sample CV R² vs n_components for every
    (outcome × block).

    When `keep_x_grid` is supplied, also runs a 2-D grouped CV sweep over
    (n_components × keep_x) and saves a second figure
    (`pls_sparse_selection.png`) with heatmap panels.  Review the heatmap to
    pick values for `default_keep_x`, then run Phase 2.

    Parameters
    ----------
    depvars      : list[str] — outcomes to evaluate; defaults to all DEPVARS
    months       : list of (year, month); defaults to all 12 months of 2025
    n_grid       : list[int] — component counts; defaults to N_COMPONENTS_GRID
    n_splits     : int — CV folds
    keep_x_grid  : list[int] or None — if given, run the sparse 2-D CV sweep
                   over these keep_x values and produce the heatmap figure

    Returns
    -------
    dict {"dense": Path, "sparse": Path or None}
    """
    depvars = depvars or DEPVARS
    months  = months  or ALL_MONTHS
    n_grid  = n_grid  or N_COMPONENTS_GRID

    project_dirs    = setup_directories()
    figure_output_dir = Path(project_dirs["figures"]) / "pls_decomposition"
    figure_output_dir.mkdir(parents=True, exist_ok=True)

    print("\n=== Phase 1: Loading channel fields and outcomes ===")
    channel_bundle  = load_channel_fields(months, project_dirs)
    outcomes_dataframe = load_outcomes(project_dirs)
    all_valid_hours = channel_bundle["hours"]
    temporal_chunk_labels = _chunk_labels(all_valid_hours)

    # Build block matrices once (shared across outcomes).
    block_predictor_matrices = {
        block: build_block_matrix(channel_bundle, fields)[0]
        for block, (fields, _) in BLOCKS.items()
    }

    n_outcome_rows, n_block_cols = len(depvars), len(BLOCKS)

    dense_fig, dense_axes = plt.subplots(
        n_outcome_rows, n_block_cols,
        figsize=(n_block_cols * 4.0, n_outcome_rows * 3.0), squeeze=False,
    )
    if keep_x_grid:
        print(f"\n=== Phase 1 (sparse): CV sweep over keep_x = {keep_x_grid} ===")
        sparse_fig, sparse_axes = plt.subplots(
            n_outcome_rows, n_block_cols,
            figsize=(n_block_cols * 4.5, n_outcome_rows * 3.5), squeeze=False,
        )
    else:
        sparse_fig = sparse_axes = None

    # Single pass over outcomes × blocks — computes dense curves always,
    # sparse sweep only when keep_x_grid is set (avoids loading outcome data twice).
    for outcome_row_idx, depvar in enumerate(depvars):
        depvar_config = DEPVAR_CONFIGS.get(depvar, {"label": depvar, "transform": "log1p"})
        outcome_values = _transform_y(
            outcomes_dataframe[depvar].reindex(all_valid_hours), depvar_config["transform"]
        ).values
        finite_outcome_mask = np.isfinite(outcome_values)
        print(f"  {depvar}: {int(finite_outcome_mask.sum())} valid hours")
        for block_col_idx, (block, (_, block_label)) in enumerate(BLOCKS.items()):
            X_standardized = _standardize_columns(
                block_predictor_matrices[block], np.where(finite_outcome_mask)[0]
            )
            panel_title  = f"{depvar_config['label']}\n[{block_label.split()[0]} {block}]"
            masked_X     = X_standardized[finite_outcome_mask]
            masked_y     = outcome_values[finite_outcome_mask]
            masked_groups = temporal_chunk_labels[finite_outcome_mask]

            variance_curves = _pls_variance_curves(masked_X, masked_y, n_grid, masked_groups, n_splits)
            _plot_selection_panel(dense_axes[outcome_row_idx][block_col_idx], variance_curves, panel_title)

            if keep_x_grid:
                sparse_cv_results, best_n_comp, best_kx = _sparse_cv_sweep(
                    masked_X, masked_y, n_grid, keep_x_grid, masked_groups, n_splits,
                )
                _plot_sparse_panel(
                    sparse_axes[outcome_row_idx][block_col_idx], sparse_cv_results,
                    panel_title, best_n_comp, best_kx,
                )
                print(f"  {depvar} / {block}: best (n={best_n_comp}, keep_x={best_kx})  "
                      f"CV R²={sparse_cv_results[(best_n_comp, best_kx)]['r2_mean']:.3f}")

    dense_fig.suptitle("PLS Mode Selection — cumulative X/Y variance and CV R²",
                       fontsize=11, y=0.998)
    dense_fig.tight_layout(rect=[0, 0, 1, 0.97])
    dense_figure_path = _save_fig(dense_fig, figure_output_dir / "pls_selection.png")
    print(f"  Dense figure: {dense_figure_path}")
    print(f"  Current default_modes = {default_modes}")

    sparse_figure_path = None
    if keep_x_grid:
        sparse_fig.suptitle("Sparse PLS Mode Selection — CV R² over (n_components × keep_x)",
                            fontsize=11, y=0.998)
        sparse_fig.tight_layout(rect=[0, 0, 1, 0.97])
        sparse_figure_path = _save_fig(sparse_fig, figure_output_dir / "pls_sparse_selection.png")
        print(f"  Sparse figure: {sparse_figure_path}")
        print(f"  Current default_keep_x = {default_keep_x}  (update before Phase 2)")

    print("\nDone — Phase 1 complete.")
    return {"dense": dense_figure_path, "sparse": sparse_figure_path}


# ═══════════════════════════════════════════════════════════════════════════════
# Section 2: PLS decomposition — fit supervised modes and save (Phase 2)
# ═══════════════════════════════════════════════════════════════════════════════

def fit_pls_block(block_data, y, train_idx, test_idx, n_components_requested,
                  outcome_transform, keep_x=None):
    """Fit one supervised PLS block on training hours; project all hours.

    When `keep_x` is None (default) a standard dense PLSRegression is fitted.
    When `keep_x` is an int, SparsePLS is used: each component's X-weight vector
    is soft-thresholded so at most `keep_x` pixels are nonzero.

    The block's stacked channel field is column-standardized (train statistics),
    then PLS extracts latent components that covary with the (transformed)
    outcome.  Each component is sign-oriented so a positive score corresponds to
    a higher outcome.  Per-hour scores for every hour are returned by projecting
    onto the fitted rotations (W* = W(P^T W)^{-1}).

    The "loadings" key in the return dict stores x_weights_ (W) — the predictive
    spatial direction.  For dense PLS, W and P differ numerically; for sparse PLS,
    W has at most `keep_x` nonzeros per component (the interpretable sparse map).

    Parameters
    ----------
    block_data            : tuple (X, offsets, lat, lon, fields) from build_block_matrix;
                            X is reused across outcomes within a cut
    y                     : ndarray (T,) — transformed outcome aligned to the cut hours
    train_idx             : ndarray — positions used to fit (outcome finite)
    test_idx              : ndarray — held-out positions for predictive R² (may be empty)
    n_components_requested : int — requested components
    outcome_transform     : str — "log1p" or "raw" (for natural-scale test R²)
    keep_x                : int or None — if int, use SparsePLS keeping at most this many
                            nonzero pixels per component

    Returns
    -------
    dict with: loadings (p, K_eff), scores (T, K_eff), lat (p,), lon (p,),
               offsets (K_fields, 2), evr_x (K_eff,), r2_y_cum (K_eff,),
               fields (list[str]), keep_x (int or None), test_r2 (float).
    """
    X, offsets, lat, lon, fields = block_data
    X_standardized       = _standardize_columns(X, train_idx)
    X_train_standardized = X_standardized[train_idx]

    n_effective_components = int(min(n_components_requested, len(train_idx) - 1, X.shape[1]))
    if keep_x is None:
        pls_model = PLSRegression(n_components=n_effective_components, scale=False)
    else:
        pls_model = SparsePLS(n_components=n_effective_components, keep_x=keep_x)
    pls_model.fit(X_train_standardized, y[train_idx])

    x_rotations = pls_model.x_rotations_.copy()   # W* = W(P^T W)^{-1}, for scoring
    x_weights   = pls_model.x_weights_.copy()     # W, displayed on loading maps
    x_loadings  = pls_model.x_loadings_.copy()    # P, used for explained-variance formula

    # Orient each component so its score correlates positively with the outcome.
    training_scores_matrix    = X_train_standardized @ x_rotations
    y_train_centered          = y[train_idx] - y[train_idx].mean()
    component_sign_corrections = np.where(training_scores_matrix.T @ y_train_centered < 0, -1.0, 1.0)
    x_rotations            *= component_sign_corrections
    x_weights              *= component_sign_corrections
    x_loadings             *= component_sign_corrections
    training_scores_matrix *= component_sign_corrections

    all_hour_pls_scores = (X_standardized @ x_rotations).astype(np.float32)   # all cut hours

    # Variance explained in X per component: ||T_k||^2 ||P_k||^2 / ||X||^2_F
    total_x_variance         = float((X_train_standardized ** 2).sum())
    explained_variance_ratio_x = (
        (training_scores_matrix ** 2).sum(axis=0) * (x_loadings ** 2).sum(axis=0)
    ) / max(total_x_variance, 1e-12)

    # Cumulative outcome variance explained by the first k components.
    y_total_sum_of_squares = float(y_train_centered @ y_train_centered)
    cumulative_outcome_r2  = []
    for component_count in range(1, n_effective_components + 1):
        projection_coefficients = np.linalg.lstsq(
            training_scores_matrix[:, :component_count], y_train_centered, rcond=None
        )[0]
        y_residuals = y_train_centered - training_scores_matrix[:, :component_count] @ projection_coefficients
        cumulative_outcome_r2.append(
            1 - float(y_residuals @ y_residuals) / max(y_total_sum_of_squares, 1e-12)
        )

    # Honest predictive skill on held-out chunks (PLS's own prediction).
    if len(test_idx) >= 20:
        test_set_predictions = pls_model.predict(X_standardized[test_idx]).ravel()
        if outcome_transform == "log1p":
            test_r2 = _r2(np.expm1(y[test_idx]), np.expm1(test_set_predictions))
        else:
            test_r2 = _r2(y[test_idx], test_set_predictions)
    else:
        test_r2 = float("nan")

    return {
        "loadings": x_weights.astype(np.float32),   # x_weights_ (W): spatial patterns
        "scores":   all_hour_pls_scores,
        "lat":      lat.astype(float),
        "lon":      lon.astype(float),
        "offsets":  np.array(offsets, dtype=int),
        "evr_x":    explained_variance_ratio_x.astype(float),
        "r2_y_cum": np.array(cumulative_outcome_r2, dtype=float),
        "fields":   list(fields),
        "keep_x":   keep_x,
        "test_r2":  float(test_r2),
    }


def _modes_path(pls_dir, cut_key, depvar):
    """Return the .npz path for one (cut, outcome) set of PLS modes."""
    return Path(pls_dir) / f"pls_modes_{cut_key}_{depvar}.npz"


def save_pls_modes(modes_by_block, hours, y_trans, meta, pls_dir, cut_key, depvar):
    """Persist one (cut, outcome) set of PLS block modes and scores to disk.

    Parameters
    ----------
    modes_by_block : dict {block: modes dict from fit_pls_block}
    hours          : pd.DatetimeIndex — the cut's hours
    y_trans        : ndarray — transformed outcome aligned to hours
    meta           : dict — JSON-serializable metadata (transform, labels, ...)
    pls_dir        : Path
    cut_key        : str
    depvar         : str
    """
    pls_dir = Path(pls_dir)
    pls_dir.mkdir(parents=True, exist_ok=True)

    archive_data = {"hours": hours.values.astype("int64"), "y": y_trans}
    for block_key, block_modes in modes_by_block.items():
        for array_key in PLS_MODES_KEYS:
            archive_data[f"{block_key}__{array_key}"] = block_modes[array_key]

    npz_output_path = _modes_path(pls_dir, cut_key, depvar)
    np.savez_compressed(npz_output_path, **archive_data)
    npz_output_path.with_suffix(".json").write_text(json.dumps(meta))
    print(f"  Saved: {npz_output_path.name}")


def load_pls_modes(pls_dir, cut_key, depvar):
    """Load one (cut, outcome) set of PLS modes saved by save_pls_modes.

    Returns
    -------
    block_pls_modes      : dict {block: {loadings, scores, lat, lon, offsets, evr_x,
                                         r2_y_cum, fields}}
    saved_hours          : pd.DatetimeIndex
    saved_outcome_values : ndarray
    run_metadata         : dict
    """
    npz_file_path = _modes_path(pls_dir, cut_key, depvar)
    npz_archive   = np.load(npz_file_path)
    run_metadata  = json.loads(npz_file_path.with_suffix(".json").read_text())

    saved_hours          = pd.DatetimeIndex(npz_archive["hours"].astype("datetime64[ns]"))
    saved_outcome_values = npz_archive["y"]

    block_pls_modes = {}
    for block_key, channel_field_names in run_metadata["fields"].items():
        block_pls_modes[block_key] = {
            array_key: npz_archive[f"{block_key}__{array_key}"]
            for array_key in PLS_MODES_KEYS
        }
        block_pls_modes[block_key]["fields"] = channel_field_names
    return block_pls_modes, saved_hours, saved_outcome_values, run_metadata


def run_pls_decomposition(depvars=None, months=None, n_components_per_block=None, keep_x=None):
    """Phase 2: fit per-block supervised PLS for every cut × outcome and save.

    Loads the channel bundle once, then for each cut and outcome splits hours
    into train/test chunks, fits a PLS per block on the training chunks, records
    held-out predictive R², and saves the loadings + projected scores to disk
    (consumed by Phase 3).

    Parameters
    ----------
    depvars               : list[str] — outcomes; defaults to all DEPVARS
    months                : list of (year, month); defaults to all 12 months of 2025
    n_components_per_block : dict {block: int} — components per block; defaults to default_modes
    keep_x                : dict {block: int or None} — nonzero pixels per component; defaults
                            to default_keep_x.  None for a block means dense PLS for that block.

    Returns
    -------
    Path to the directory holding the saved modes.
    """
    depvars              = depvars or DEPVARS
    months               = months  or ALL_MONTHS
    n_components_by_block = n_components_per_block or default_modes
    keep_x_by_block       = keep_x                or default_keep_x

    project_dirs  = setup_directories()
    pls_modes_dir = Path(project_dirs["processed"]) / "pls"
    pls_modes_dir.mkdir(parents=True, exist_ok=True)

    print("\n=== Phase 2: Loading channel fields and outcomes ===")
    channel_bundle     = load_channel_fields(months, project_dirs)
    outcomes_dataframe = load_outcomes(project_dirs)
    all_valid_hours    = channel_bundle["hours"]

    for cut_key, cut in SAMPLE_CUT.items():
        sample_cut_mask = cut.mask(all_valid_hours, outcomes_dataframe)
        n_cut_hours     = int(sample_cut_mask.sum())
        if n_cut_hours < 200:
            print(f"\n=== Phase 2 [{cut.label}]: skipping ({n_cut_hours} hours) ===")
            continue
        print(f"\n=== Phase 2 [{cut.label}]: {n_cut_hours} hours ===")

        cut_channel_bundle = _subset_bundle(channel_bundle, sample_cut_mask)
        cut_valid_hours    = cut_channel_bundle["hours"]
        train_hour_mask, test_hour_mask = make_chunk_splits(cut_valid_hours, seed=RANDOM_STATE)

        # Block matrices are identical across outcomes — build once per cut.
        block_predictor_matrices = {
            block: (*build_block_matrix(cut_channel_bundle, fields), fields)
            for block, (fields, _) in BLOCKS.items()
        }

        for depvar in depvars:
            if depvar not in outcomes_dataframe.columns:
                print(f"  {depvar}: not in outcomes — skipping")
                continue
            depvar_config      = DEPVAR_CONFIGS.get(depvar, {"label": depvar, "transform": "log1p"})
            outcome_transform  = depvar_config["transform"]
            transformed_outcome = _transform_y(
                outcomes_dataframe[depvar].reindex(cut_valid_hours), outcome_transform
            ).values
            finite_outcome_mask = np.isfinite(transformed_outcome)
            train_hour_indices  = np.where(train_hour_mask & finite_outcome_mask)[0]
            test_hour_indices   = np.where(test_hour_mask  & finite_outcome_mask)[0]
            if len(train_hour_indices) < 50:
                print(f"  {depvar}: only {len(train_hour_indices)} train hours — skipping")
                continue

            block_pls_modes = {}
            for block in BLOCKS:
                block_pls_modes[block] = fit_pls_block(
                    block_predictor_matrices[block], transformed_outcome,
                    train_hour_indices, test_hour_indices,
                    n_components_by_block[block], outcome_transform,
                    keep_x=keep_x_by_block.get(block),
                )
            test_r2_by_block = {block: modes["test_r2"] for block, modes in block_pls_modes.items()}
            print(f"  {depvar}: test R² per block = "
                  + ", ".join(f"{b}={r:.3f}" for b, r in test_r2_by_block.items()))

            run_metadata = {
                "cut_key":   cut_key,
                "cut_label": cut.label,
                "depvar":    depvar,
                "transform": outcome_transform,
                "fields":    {b: modes["fields"]  for b, modes in block_pls_modes.items()},
                "keep_x":    {b: modes["keep_x"]  for b, modes in block_pls_modes.items()},
                "test_r2":   test_r2_by_block,
            }
            save_pls_modes(block_pls_modes, cut_valid_hours, transformed_outcome,
                           run_metadata, pls_modes_dir, cut_key, depvar)

    print(f"\nDone — modes saved to {pls_modes_dir}")
    return pls_modes_dir


# ═══════════════════════════════════════════════════════════════════════════════
# Section 3: PLS regression analysis and figures (Phase 3)
# ═══════════════════════════════════════════════════════════════════════════════

def build_pls_design(modes_by_block, hours, y_trans):
    """Assemble the OLS design matrix from PLS block scores and time controls.

    Columns: cyclic time controls (sin/cos hour, sin/cos month, is_weekend) plus
    one column per (block, component): "PC{k+1}_{block}".

    Parameters
    ----------
    modes_by_block : dict {block: {scores: (T, K), ...}}
    hours          : pd.DatetimeIndex (T,)
    y_trans        : ndarray (T,) — transformed outcome (may contain NaN)

    Returns
    -------
    feature_matrix_clean : pd.DataFrame (T_clean, n_features)
    outcome_array        : ndarray (T_clean,)
    clean_hours          : pd.DatetimeIndex
    feature_group_names  : dict {group: [column names]}
    """
    hour_of_day   = hours.hour
    month_of_year = hours.month
    time_control_features = pd.DataFrame({
        "sin_hour":   np.sin(2 * np.pi * hour_of_day   / 24),
        "cos_hour":   np.cos(2 * np.pi * hour_of_day   / 24),
        "sin_month":  np.sin(2 * np.pi * month_of_year / 12),
        "cos_month":  np.cos(2 * np.pi * month_of_year / 12),
        "is_weekend": (hours.dayofweek >= 5).astype(float),
    }, index=hours)

    pls_score_columns  = {}
    feature_group_names = {"time_controls": list(time_control_features.columns)}
    for block, modes in modes_by_block.items():
        component_scores       = modes["scores"]
        component_column_names = [f"PC{k+1}_{block}" for k in range(component_scores.shape[1])]
        for col_name, score_col in zip(component_column_names, component_scores.T):
            pls_score_columns[col_name] = score_col
        feature_group_names[block] = component_column_names
    pls_score_dataframe = pd.DataFrame(pls_score_columns, index=hours)

    outcome_series      = pd.Series(y_trans, index=hours)
    feature_matrix_raw  = pd.concat([time_control_features, pls_score_dataframe], axis=1)
    complete_row_mask   = feature_matrix_raw.notna().all(axis=1) & outcome_series.notna()

    feature_matrix_clean = feature_matrix_raw.loc[complete_row_mask]
    print(f"  Design matrix: {feature_matrix_clean.shape[0]} hours × {feature_matrix_clean.shape[1]} features")
    return (feature_matrix_clean, outcome_series.loc[complete_row_mask].values,
            pd.DatetimeIndex(feature_matrix_clean.index), feature_group_names)


def plot_pls_outcome_figure(modes_by_block, ols_result, col_names, depvar_label,
                            cut_label, output_path):
    """One figure per outcome: component loading maps + a coefficient forest.

    Rows = PLS components (grouped by block); for each row the two left columns
    show that component's wind and temperature loading maps (with % X-variance),
    and a shared right-hand panel shows the OLS coefficient (±95% CI) on each
    component's score, coloured by significance.

    Parameters
    ----------
    modes_by_block : dict {block: modes dict (loadings, lat, lon, offsets,
                            evr_x, fields)}
    ols_result     : statsmodels OLS result (HAC s.e.)
    col_names      : list[str] — coefficient names (includes 'const')
    depvar_label   : str
    cut_label      : str
    output_path    : Path
    """
    from matplotlib.colors import TwoSlopeNorm

    cartopy_crs      = _get_cartopy_crs()
    map_projection   = cartopy_crs.PlateCarree() if cartopy_crs is not None else None
    diverging_color_norm = TwoSlopeNorm(vmin=-1.0, vcenter=0.0, vmax=1.0)

    # One row per (block, component), in block order.
    block_component_pairs = [
        (block, component_idx)
        for block, modes in modes_by_block.items()
        for component_idx in range(modes["loadings"].shape[1])
    ]
    n_component_rows = len(block_component_pairs)
    if n_component_rows == 0:
        return None

    ols_coefficient_series = pd.Series(ols_result.params,  index=col_names)
    ols_conf_intervals     = ols_result.conf_int()
    conf_int_lower_bounds  = pd.Series(ols_conf_intervals[:, 0], index=col_names)
    conf_int_upper_bounds  = pd.Series(ols_conf_intervals[:, 1], index=col_names)
    p_value_series         = pd.Series(ols_result.pvalues, index=col_names)

    fig      = plt.figure(figsize=(11, max(2.4, n_component_rows * 1.7)))
    gridspec = fig.add_gridspec(n_component_rows, 3, width_ratios=[1, 1, 1.4],
                                hspace=0.45, wspace=0.25)

    forest_plot_ax = fig.add_subplot(gridspec[:, 2])
    subplot_projection_kwargs = {"projection": map_projection} if map_projection is not None else {}

    for row_idx, (block, component_idx) in enumerate(block_component_pairs):
        block_modes         = modes_by_block[block]
        field_column_offsets = block_modes["offsets"]
        channel_field_names  = block_modes["fields"]

        for field_col_idx, (field, (start, end)) in enumerate(zip(channel_field_names, field_column_offsets)):
            map_ax = fig.add_subplot(gridspec[row_idx, field_col_idx], **subplot_projection_kwargs)
            normalized_component_loadings = _normalize_comp(block_modes["loadings"][start:end, component_idx])
            scatter_kwargs = {
                "c": normalized_component_loadings,
                "cmap": "RdBu_r",
                "norm": diverging_color_norm,
                "s": _grid_marker_size(block_modes["lon"][start:end]),
                "rasterized": True,
            }
            if map_projection is not None:
                scatter_kwargs["transform"] = map_projection
            map_ax.scatter(block_modes["lon"][start:end], block_modes["lat"][start:end], **scatter_kwargs)
            _draw_texas(map_ax)
            x_variance_pct = block_modes["evr_x"][component_idx] * 100
            map_ax.set_title(f"{FIELD_LABELS.get(field, field)}\n"
                             f"{block} PC{component_idx+1} ({x_variance_pct:.1f}% X-var)",
                             fontsize=6.5, pad=2)

        component_score_col_name = f"PC{component_idx+1}_{block}"
        if component_score_col_name in ols_coefficient_series.index:
            ols_coef   = ols_coefficient_series[component_score_col_name]
            p_val      = p_value_series[component_score_col_name]
            marker_color = COLOR_P001 if p_val < 0.01 else COLOR_P005 if p_val < 0.05 else COLOR_NSIG
            forest_plot_ax.errorbar(
                ols_coef, row_idx,
                xerr=[[max(ols_coef - conf_int_lower_bounds[component_score_col_name], 0)],
                      [max(conf_int_upper_bounds[component_score_col_name] - ols_coef, 0)]],
                fmt="o", color=marker_color, markersize=5, capsize=3, linewidth=1.2,
            )

    forest_plot_ax.axvline(0, color="k", lw=0.8, ls="--")
    forest_plot_ax.set_yticks(range(n_component_rows))
    forest_plot_ax.set_yticklabels(
        [f"{block} PC{comp_idx+1}" for block, comp_idx in block_component_pairs], fontsize=7
    )
    forest_plot_ax.invert_yaxis()
    forest_plot_ax.grid(axis="x", ls=":", lw=0.5, alpha=0.6)
    forest_plot_ax.set_xlabel("OLS coef on PLS score (HAC s.e.)", fontsize=7.5)
    forest_plot_ax.set_title("Coefficients (95% CI)\n"
                             "dark blue p<0.01 ● light blue p<0.05 ● grey n.s.",
                             fontsize=7)

    fig.suptitle(f"PLS Components & Coefficients — {depvar_label}  [{cut_label}]",
                 fontsize=10, y=0.997)
    return _save_fig(fig, output_path)


def plot_pls_ftest_heatmap(all_f_tests, output_path, cut_label):
    """Heatmap of joint F-tests (block × outcome) for one sample cut.

    Cell colour encodes the F-statistic; stars reflect the p-value.

    Parameters
    ----------
    all_f_tests : dict {depvar: {block: (f_stat, p_val)}}
    output_path : Path
    cut_label   : str
    """
    available_blocks  = [b for b in BLOCKS if any(b in ft for ft in all_f_tests.values())]
    available_depvars = list(all_f_tests.keys())
    if not available_blocks or not available_depvars:
        return None

    f_statistic_matrix = np.full((len(available_blocks), len(available_depvars)), np.nan)
    p_value_matrix     = np.full((len(available_blocks), len(available_depvars)), np.nan)
    for depvar_col_idx, depvar in enumerate(available_depvars):
        for block_row_idx, block in enumerate(available_blocks):
            if block in all_f_tests[depvar]:
                f_statistic_matrix[block_row_idx, depvar_col_idx], \
                p_value_matrix[block_row_idx, depvar_col_idx] = all_f_tests[depvar][block]

    depvar_axis_labels = [DEPVAR_CONFIGS.get(dv, {}).get("label", dv) for dv in available_depvars]
    block_axis_labels  = [BLOCKS[b][1] for b in available_blocks]
    colormap_max = (
        np.nanpercentile(f_statistic_matrix, 95)
        if not np.all(np.isnan(f_statistic_matrix)) else 1.0
    )

    fig, ax = plt.subplots(figsize=(max(6, len(available_depvars) * 1.4),
                                    max(3, len(available_blocks) * 0.7)))
    heatmap_image = ax.imshow(f_statistic_matrix, aspect="auto", cmap="viridis",
                              vmin=0, vmax=colormap_max)
    ax.set_xticks(range(len(available_depvars)))
    ax.set_xticklabels(depvar_axis_labels, rotation=30, ha="right", fontsize=8)
    ax.set_yticks(range(len(available_blocks)))
    ax.set_yticklabels(block_axis_labels, fontsize=8)

    for block_row_idx in range(len(available_blocks)):
        for depvar_col_idx in range(len(available_depvars)):
            f_stat_value = f_statistic_matrix[block_row_idx, depvar_col_idx]
            p_val_value  = p_value_matrix[block_row_idx, depvar_col_idx]
            if np.isnan(f_stat_value):
                continue
            ax.text(depvar_col_idx, block_row_idx, _sig_stars(p_val_value),
                    ha="center", va="center", fontsize=9,
                    color="white" if f_stat_value > colormap_max * 0.55 else "black",
                    fontweight="bold")

    fig.colorbar(heatmap_image, ax=ax, shrink=0.7, pad=0.02, label="F-statistic")
    ax.set_title(f"Joint F-tests: PLS Blocks × Outcomes  [{cut_label}]\n"
                 "*** p<0.001  ** p<0.01  * p<0.05", fontsize=9)
    fig.tight_layout()
    return _save_fig(fig, output_path)


def run_pls_analysis(depvars=None, months=None):
    """Phase 3: regress outcomes on saved PLS scores and produce figures.

    For each cut × outcome, loads the Phase-2 modes, regresses the outcome on
    the PLS component scores with HAC standard errors, saves coefficient tables,
    and produces a per-outcome component-map + coefficient figure plus a joint
    F-test heatmap per cut.  If a cut's modes are missing, Phase 2 is run first.

    Parameters
    ----------
    depvars : list[str] — outcomes; defaults to all DEPVARS
    months  : list of (year, month) — used only if Phase 2 must be run

    Returns
    -------
    dict {cut_key: {depvar: {ols_result, col_names, feature_groups, r2_nat,
                             n_hours}}}
    """
    depvars = depvars or DEPVARS

    project_dirs      = setup_directories()
    pls_modes_dir     = Path(project_dirs["processed"]) / "pls"
    base_figure_dir   = Path(project_dirs["figures"]) / "pls_analysis"
    tables_output_dir = Path(project_dirs["tables"])
    tables_output_dir.mkdir(parents=True, exist_ok=True)

    results_by_cut = {}
    for cut_key, cut in SAMPLE_CUT.items():
        # Ensure modes exist for at least one outcome in this cut.
        if not any(_modes_path(pls_modes_dir, cut_key, dv).exists() for dv in depvars):
            print(f"\n=== Phase 3 [{cut.label}]: no saved modes — running Phase 2 ===")
            run_pls_decomposition(depvars=depvars, months=months)

        print(f"\n{'='*60}\n=== Phase 3 [{cut.label}] ===\n{'='*60}")
        cut_figure_dir = base_figure_dir / cut_key
        cut_table_dir  = tables_output_dir / "pls" / cut_key
        cut_figure_dir.mkdir(parents=True, exist_ok=True)
        cut_table_dir.mkdir(parents=True, exist_ok=True)

        cut_depvar_results  = {}
        cut_f_test_results  = {}
        for depvar in depvars:
            if not _modes_path(pls_modes_dir, cut_key, depvar).exists():
                continue
            block_pls_modes, cut_hours, transformed_outcome, run_metadata = load_pls_modes(
                pls_modes_dir, cut_key, depvar
            )

            design_matrix, outcome_array, _, feature_group_names = build_pls_design(
                block_pls_modes, cut_hours, transformed_outcome
            )
            if len(design_matrix) < 200:
                print(f"  {depvar}: only {len(design_matrix)} clean hours — skipping")
                continue

            X_standardized, _ = standardize_pca_cols(
                design_matrix, np.ones(len(design_matrix), dtype=bool)
            )
            ols_regression_result, block_f_tests, feature_column_names = run_ols_inference(
                outcome_array, X_standardized, feature_group_names
            )
            cut_f_test_results[depvar] = {b: block_f_tests[b] for b in BLOCKS if b in block_f_tests}

            r2_natural_scale = (
                _r2(np.expm1(outcome_array), np.expm1(ols_regression_result.fittedvalues))
                if run_metadata["transform"] == "log1p"
                else _r2(outcome_array, ols_regression_result.fittedvalues)
            )
            print(f"  {depvar}: OLS R²(nat)={r2_natural_scale:.4f}  N={len(design_matrix)}")

            save_coef_table(ols_regression_result, feature_column_names,
                            cut_table_dir / f"pls_ols_coefficients_{depvar}.csv")
            cut_depvar_results[depvar] = {
                "ols_result":     ols_regression_result,
                "col_names":      feature_column_names,
                "feature_groups": feature_group_names,
                "r2_nat":         r2_natural_scale,
                "n_hours":        len(design_matrix),
            }

            depvar_label = DEPVAR_CONFIGS.get(depvar, {}).get("label", depvar)
            plot_pls_outcome_figure(
                block_pls_modes, ols_regression_result, feature_column_names,
                depvar_label, cut.label,
                cut_figure_dir / f"pls_components_{depvar}.png",
            )

        if cut_f_test_results:
            plot_pls_ftest_heatmap(
                cut_f_test_results,
                cut_figure_dir / "pls_ftest_heatmap.png",
                cut.label,
            )
        results_by_cut[cut_key] = cut_depvar_results

    print(f"\nDone — figures under {base_figure_dir}")
    return results_by_cut


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    """CLI entry point for the three-phase PLS analysis workflow."""
    arg_parser = argparse.ArgumentParser(
        description="PLS analysis of ERCOT weather-error fields (three phases)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Sparse PLS workflow:\n"
            "  1. Run Phase 1 with --keep-x-grid to see which (n, keep_x) maximises CV R².\n"
            "  2. Update default_keep_x in the module with the chosen values.\n"
            "  3. Run Phase 2 (decompose) — it reads default_keep_x automatically.\n"
            "  4. Run Phase 3 (analyze) as usual.\n"
        ),
    )
    arg_parser.add_argument(
        "--task", choices=["select", "decompose", "analyze", "all"], default="all",
        help=("Phase to run: select (Phase 1 — CV scree plots), "
              "decompose (Phase 2 — fit/save modes), "
              "analyze (Phase 3 — regressions + figures), "
              "all (default, runs all three)"),
    )
    arg_parser.add_argument("--depvars", nargs="*", default=None,
                            help="Outcome variables (default: all)")
    # arg_parser.add_argument(
    #     "--keep-x-grid", nargs="+", type=int, default=None, metavar="K",
    #     dest="keep_x_grid",
    #     help=(
    #         "Phase 1 only: run sparse CV sweep over these keep_x values "
    #         f"(nonzero pixels per component). Default: {KEEP_X_GRID}. "
    #         "Pass 0 to skip sparse selection entirely."
    #     ),
    # )
    cli_args = arg_parser.parse_args()

    # keep_x_grid=None → skip sparse; [0] → also skip; else run sparse sweep
    keep_x_grid = KEEP_X_GRID
    keep_x_grid_filtered = None
    if keep_x_grid is not None:
        keep_x_grid_filtered = [
            keep_x_candidate for keep_x_candidate in keep_x_grid if keep_x_candidate > 0
        ] or None

    if cli_args.task in ("select", "all"):
        run_pls_mode_selection(depvars=cli_args.depvars, keep_x_grid=keep_x_grid_filtered)
    if cli_args.task in ("decompose", "all"):
        run_pls_decomposition(depvars=cli_args.depvars)
    if cli_args.task in ("analyze", "all"):
        run_pls_analysis(depvars=cli_args.depvars)


if __name__ == "__main__":
    main()
