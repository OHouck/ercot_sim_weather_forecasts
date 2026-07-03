"""
Unified asymmetric-error analysis of ERCOT weather fields — four basis methods.

This module runs one shared estimation pipeline in which the ONLY difference
between methods is how the spatial basis functions are constructed. Every
method ends up as the same object — a list of "features", each defined by a
pair of spatial weight vectors (one applied to the positive part of the field,
one to the negative part) — so scoring, regression, inference, coefficient-
surface reconstruction, and figures are literally shared code.

Asymmetric-error specification
------------------------------
Each weather field X is split at zero into its positive and negative parts,

    X+(s) = max(X(s), 0),   X-(s) = min(X(s), 0),

and the scalar-on-function regression estimates a separate coefficient surface
for each part:

    y = alpha + integral X+(s) beta+(s) ds + integral X-(s) beta-(s) ds + eps.

Forecast-error blocks are split at their PHYSICAL zero (over- vs under-
forecast) before residualizing on time controls; the realized-weather block
has no natural zero, so it is residualized first and split at its anomaly zero
(above- vs below-normal conditions). The symmetric linear model is the nested
restriction beta+ = beta-, tested per feature from the bootstrap draws.

The four basis methods
----------------------
1. infrastructure : fixed masks — mean positive/negative part over wind-
                    generation pixels, thermal-generation pixels, and major-
                    metro load-center pixels (process_data.build_basis_masks).
2. weather_zone   : fixed masks — mean positive/negative part over each of the
                    eight ERCOT weather zones.
3. rotated_eof    : Varimax-rotated EOFs fit per channel on the SIGNED field
                    (unsupervised — fitting on rectified parts would let the
                    mechanical dependence between the parts dominate the
                    leading modes); the positive and negative parts are then
                    each projected onto the same eigenfunctions, giving two
                    scores per mode. beta+ and beta- share spatial patterns
                    but differ in amplitude.
4. fpls           : Functional PLS (Babii, Carrasco & Tsafack 2024 `fpls`
                    package) fit on the STACKED [X+, X-] matrix, so the
                    supervised weight functions are estimated jointly and the
                    positive/negative coefficient surfaces are NOT constrained
                    to share spatial patterns.

Blocks (one per forecast horizon; wind + temperature channels stacked):
  dayahead  : GFS day-ahead 100m wind-speed error + temperature error
  hourahead : HRRR 1h 100m wind-speed error + temperature error
  realized  : ERA5 realized 100m wind speed + temperature

Sample cuts: full year plus the four meteorological seasons, so beta+ and
beta- are season-specific (e.g. positive temperature anomalies in summer vs
negative anomalies in winter).

Shared downstream pipeline (per cut x outcome x method):
  a. residualize the outcome on cyclic time controls (Frisch–Waugh)
  b. score every feature: t = X+ @ w+  +  X- @ w-
  c. OLS of the residualized outcome on all feature scores with MOVING-BLOCK
     BOOTSTRAP standard errors (7-day blocks) for temporal dependence
  d. out-of-sample R^2 from a train-chunk fit evaluated on held-out chunks
  e. reconstruct beta+(s), beta-(s) per block-channel with bootstrap bands
  f. per-feature symmetry test (H0: b+ = b-) from the bootstrap draws

Outputs
-------
  processed/main_analysis/betas_{method}_{cut}_{depvar}.npz  (+ .json meta)
  tables/main_analysis/{cut}/{method}_coefficients_{depvar}.csv
  tables/main_analysis/r2_summary.csv
  figures via analysis.figures (run automatically unless --skip-figures)

Usage:
    uv run python -m analysis.main_analysis                        # everything
    uv run python -m analysis.main_analysis --cuts all --depvars economic_congestion_cost
    uv run python -m analysis.main_analysis --methods infrastructure weather_zone
"""

import argparse
import json
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats as scipy_stats
from sklearn.model_selection import GroupKFold

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories

import fpls
import fpls.core
import fpls.selection
from fpls import FunctionalPLS, frisch_waugh_residualize

from analysis.pca_decomposition import (
    load_channel_fields, ALL_MONTHS, RANDOM_STATE, _r2, FIELD_LABELS,
    make_chunk_splits,
)
from analysis.pca_mode_analysis import load_outcomes
from analysis.eof_analysis import (
    DEPVAR_CONFIGS, DEPVARS, _subset_bundle,
    default_modes as EOF_DEFAULT_MODES, _FIELD_TO_BLOCK_KEY,
)
from analysis.eof_methods import fit_varimax
from analysis.pls_analysis_v2 import build_block_matrix, _chunk_labels, _transform_y
from process_data.build_basis_masks import (
    build_infrastructure_pixel_masks, build_weather_zone_pixel_masks,
)

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)


# ── NumPy >= 2.0 compatibility shim for the fpls package ───────────────────────
# The released fpls package calls float() on (1,1) arrays inside its conjugate-
# gradient loops, which raises under NumPy >= 2.0. Injecting a NumPy-safe float
# into its module namespaces fixes this without altering any algorithm code
# (same shim as analysis.fpls_analysis).

def _numpy_safe_float(value):
    """float() that accepts single-element arrays (NumPy >= 2.0 compatibility)."""
    return float(np.asarray(value).reshape(-1)[0])


fpls.core.float      = _numpy_safe_float
fpls.selection.float = _numpy_safe_float


# ── Constants ──────────────────────────────────────────────────────────────────

# One block per forecast horizon. "split_at" controls where the positive/
# negative split happens: forecast errors split at their physical zero (over-
# vs under-forecast); realized weather has no natural zero so it is split at
# its residualized anomaly zero.
BLOCKS = {
    "dayahead":  {"fields": ["wspd100_error_0h", "temp_error_0h"],
                  "label": "GFS Day-Ahead Forecast Errors", "split_at": "physical_zero"},
    "hourahead": {"fields": ["wspd100_error_1h", "temp_error_1h"],
                  "label": "HRRR Hour-Ahead Forecast Errors", "split_at": "physical_zero"},
    "realized":  {"fields": ["era5_wspd100", "era5_temp"],
                  "label": "ERA5 Realized Weather", "split_at": "residual_zero"},
}

METHODS = ("infrastructure", "weather_zone", "rotated_eof", "fpls")

# Sample cuts: full year + meteorological seasons (None = every month).
SAMPLE_CUTS = {
    "all":    {"label": "Full Year",     "months": None},
    "winter": {"label": "Winter (DJF)",  "months": (12, 1, 2)},
    "spring": {"label": "Spring (MAM)",  "months": (3, 4, 5)},
    "summer": {"label": "Summer (JJA)",  "months": (6, 7, 8)},
    "fall":   {"label": "Fall (SON)",    "months": (9, 10, 11)},
}

# Varimax-rotated EOF modes per channel — sourced from analysis.eof_analysis
# default_modes (chosen there from the Phase-1 scree diagnostics) so the two
# pipelines can never fall out of sync; remapped from that module's
# {block_channel: K} keying to plain field names.
EOF_MODES_PER_FIELD = {
    field_name: EOF_DEFAULT_MODES[block_channel_key]
    for field_name, block_channel_key in _FIELD_TO_BLOCK_KEY.items()
}

# Functional PLS settings: component cap, grouped-CV folds for choosing the
# component count, and the uniform Riemann step for the flattened domain.
FPLS_MAX_COMPONENTS = 6
FPLS_CV_FOLDS       = 3
FPLS_RIEMANN_STEP   = 1.0

# Moving-block bootstrap settings for the shared inference stage.
N_BOOTSTRAP_DRAWS    = 400
BOOTSTRAP_BLOCK_DAYS = 7

# A mask must contain at least this many pixels of a channel to become a
# feature (protects against degenerate masks on the coarse GFS grid).
MINIMUM_MASK_PIXELS = 3

# Short channel tags used in feature names and output keys.
CHANNEL_TAGS = {
    "wspd100_error_0h": "wind", "temp_error_0h": "temp",
    "wspd100_error_1h": "wind", "temp_error_1h": "temp",
    "era5_wspd100":     "wind", "era5_temp":     "temp",
}


# ═══════════════════════════════════════════════════════════════════════════════
# Section 1: Pre-processing — time controls and the positive/negative split
# ═══════════════════════════════════════════════════════════════════════════════

def build_time_controls(hours):
    """Cyclic time-control matrix (with intercept) for Frisch–Waugh residualization.

    Parameters
    ----------
    hours : pd.DatetimeIndex (T,)

    Returns
    -------
    ndarray (T, 6) — [const, sin/cos hour-of-day, sin/cos month, is_weekend]
    """
    hour_of_day   = hours.hour.values
    month_of_year = hours.month.values
    return np.column_stack([
        np.ones(len(hours)),
        np.sin(2 * np.pi * hour_of_day   / 24), np.cos(2 * np.pi * hour_of_day   / 24),
        np.sin(2 * np.pi * month_of_year / 12), np.cos(2 * np.pi * month_of_year / 12),
        (hours.dayofweek.values >= 5).astype(float),
    ])


def prepare_positive_negative_parts(block_key, X_signed, offsets, time_controls,
                                    train_row_positions):
    """Split a block's field into positive/negative parts, residualize, and scale.

    Forecast-error blocks (split_at == "physical_zero") are split at the raw
    zero first — the parts then carry the over-/under-forecast interpretation —
    and each part is residualized on the time controls afterwards (a nonlinear
    transform of the field is not orthogonal to the controls, so both parts
    must be residualized for Frisch–Waugh to apply). The realized-weather
    block (split_at == "residual_zero") is residualized first, split at its
    anomaly zero, and each part re-residualized for the same reason.

    Both parts of a channel are divided by the SAME scale — the training-rows
    standard deviation of that channel's signed residualized field — so the
    positive- and negative-part coefficients are directly comparable and each
    channel enters in "per one pooled-SD anomaly" units.

    Parameters
    ----------
    block_key           : str — key of BLOCKS
    X_signed            : ndarray (T, p) — raw stacked block field
    offsets             : list[(start, end)] — column slice per channel
    time_controls       : ndarray (T, q) — control matrix (with intercept)
    train_row_positions : ndarray — training row positions (for the scale)

    Returns
    -------
    X_positive_part : ndarray float32 (T, p)
    X_negative_part : ndarray float32 (T, p)
    channel_scales  : list[float] — the scale used for each channel
    """
    # Step 1: signed residualized field — used for the per-channel scale and,
    # for the realized block, as the object that gets split.
    X_signed_residual = frisch_waugh_residualize(X_signed, time_controls)

    # Step 2: split at the appropriate zero.
    if BLOCKS[block_key]["split_at"] == "physical_zero":
        X_positive_raw = np.clip(X_signed, 0.0, None)
        X_negative_raw = np.clip(X_signed, None, 0.0)
    else:
        X_positive_raw = np.clip(X_signed_residual, 0.0, None)
        X_negative_raw = np.clip(X_signed_residual, None, 0.0)

    # Step 3: residualize each part on the time controls (Frisch–Waugh).
    X_positive_part = frisch_waugh_residualize(X_positive_raw, time_controls)
    X_negative_part = frisch_waugh_residualize(X_negative_raw, time_controls)

    # Step 4: divide both parts of each channel by the channel's pooled signed
    # standard deviation (training rows only) so wind and temperature are
    # comparable and beta+ / beta- share units.
    channel_scales = []
    for column_start, column_end in offsets:
        channel_scale = float(
            X_signed_residual[train_row_positions, column_start:column_end].std())
        if channel_scale < 1e-12:
            channel_scale = 1.0
        X_positive_part[:, column_start:column_end] /= channel_scale
        X_negative_part[:, column_start:column_end] /= channel_scale
        channel_scales.append(channel_scale)

    return (X_positive_part.astype(np.float32),
            X_negative_part.astype(np.float32),
            channel_scales)


# ═══════════════════════════════════════════════════════════════════════════════
# Section 2: Basis construction — the ONLY stage that differs across methods.
#
# Every method returns a list of "features". A feature is a dict:
#   {"name": str, "block": str,
#    "pair_stem": str,   — shared identifier of the underlying basis function,
#    "sign": "pos"/"neg" — which part of the field this feature scores,
#    "weight_positive_part": ndarray (p_block,) or None,
#    "weight_negative_part": ndarray (p_block,) or None}
# and its per-hour score is  X+ @ weight_positive_part + X- @ weight_negative_part
# The (pair_stem, sign) pair is the STRUCTURAL link between the positive- and
# negative-part features of one basis function; symmetry tests and forest
# plots pair rows through it rather than by parsing feature-name suffixes.
# (a None weight contributes nothing). Fixed-mask and EOF bases produce one
# positive-part and one negative-part feature per basis function (shared
# spatial pattern, sign-specific amplitude); FPLS produces one functional
# index per sign whose spatial patterns were estimated jointly on the stacked
# [X+, X-] domain (fully sign-specific patterns).
# ═══════════════════════════════════════════════════════════════════════════════

def build_mask_features(block_key, fields, offsets, masks_by_name):
    """Fixed-mask features: mean positive/negative part over each pixel mask.

    For every (channel, mask) pair, two features are created — the spatial mean
    of the positive part over the mask's pixels, and the spatial mean of the
    negative part — implemented as weight vectors equal to 1/n_mask_pixels on
    the mask's columns of that channel and zero elsewhere. Masks with fewer
    than MINIMUM_MASK_PIXELS pixels in a channel are skipped.

    Parameters
    ----------
    block_key     : str — key of BLOCKS
    fields        : list[str] — the block's channel names
    offsets       : list[(start, end)] — column slice per channel
    masks_by_name : dict {mask_name: ndarray bool (p_block,)} — masks defined
                    over the block's concatenated pixel columns

    Returns
    -------
    list of feature dicts (2 per channel x mask).
    """
    feature_list = []
    for field, (column_start, column_end) in zip(fields, offsets):
        channel_tag = CHANNEL_TAGS[field]
        for mask_name, mask_over_block_columns in masks_by_name.items():
            mask_in_channel = mask_over_block_columns[column_start:column_end]
            n_mask_pixels = int(mask_in_channel.sum())
            if n_mask_pixels < MINIMUM_MASK_PIXELS:
                continue
            # Weight vector: spatial mean over the mask's pixels of this channel.
            mask_mean_weights = np.zeros(mask_over_block_columns.shape[0], dtype=np.float32)
            mask_mean_weights[column_start:column_end][mask_in_channel] = 1.0 / n_mask_pixels
            basis_stem = f"{block_key}_{channel_tag}_{mask_name}"
            feature_list.append({
                "name": f"{basis_stem}_pos",
                "block": block_key, "pair_stem": basis_stem, "sign": "pos",
                "shared_pattern": True,
                "weight_positive_part": mask_mean_weights,
                "weight_negative_part": None,
            })
            feature_list.append({
                "name": f"{basis_stem}_neg",
                "block": block_key, "pair_stem": basis_stem, "sign": "neg",
                "shared_pattern": True,
                "weight_positive_part": None,
                "weight_negative_part": mask_mean_weights,
            })
    return feature_list


def build_rotated_eof_features(cut_bundle, block_key, fields, offsets,
                               train_row_positions):
    """Varimax-rotated EOF features: signed-field modes projected on each part.

    For each channel, a Varimax-rotated EOF is fit on the SIGNED field over the
    cut's training hours (fitting on the rectified parts would let the
    mechanical dependence between X+ and X- dominate the leading modes). The
    linear projection-weight map W per mode is recovered by least squares from
    the training anomalies to the fitted scores, sign-oriented so a positive
    score means above-average field values, and then used as the spatial
    weight for BOTH the positive-part and negative-part features of that mode.

    Parameters
    ----------
    cut_bundle          : dict from load_channel_fields, subset to the cut
    block_key           : str — key of BLOCKS
    fields              : list[str] — the block's channel names
    offsets             : list[(start, end)] — column slice per channel
    train_row_positions : ndarray — training row positions within the cut

    Returns
    -------
    list of feature dicts (2 per retained mode).
    """
    cut_hours = cut_bundle["hours"]
    total_block_columns = offsets[-1][1]

    feature_list = []
    for field, (column_start, column_end) in zip(fields, offsets):
        channel_tag = CHANNEL_TAGS[field]
        n_modes = EOF_MODES_PER_FIELD[field]

        # Step 1: fit the Varimax-rotated EOF on the signed channel (train hours).
        varimax_result = fit_varimax(
            cut_bundle, train_row_positions, cut_hours,
            K=n_modes, error_fields=[field], seed=RANDOM_STATE)
        mode_scores_all_hours = np.asarray(varimax_result.scores.values, dtype=float)

        # Step 2: recover the linear projection-weight map W per mode by least
        # squares from training anomalies to the fitted training scores (exact,
        # because the EOF scores are linear in the centered field).
        channel_data_array = cut_bundle["channel_da"][field]
        valid_land_mask    = ~cut_bundle["nan_all"][field]
        channel_matrix     = channel_data_array.values[:, valid_land_mask]
        training_anomalies = (channel_matrix[train_row_positions]
                              - channel_matrix[train_row_positions].mean(axis=0))
        projection_weights = np.linalg.lstsq(
            training_anomalies, mode_scores_all_hours[train_row_positions],
            rcond=None)[0]                               # (p_channel, n_modes)

        # Step 3: orient each mode so its summed loading is positive (positive
        # score = widespread above-average field values).
        mode_orientation = np.where(projection_weights.sum(axis=0) < 0, -1.0, 1.0)
        projection_weights = projection_weights * mode_orientation[np.newaxis, :]

        # Step 4: embed each mode's weight map into the block's columns and
        # emit a positive-part and a negative-part feature with the SAME map.
        for mode_index in range(projection_weights.shape[1]):
            mode_weights_in_block = np.zeros(total_block_columns, dtype=np.float32)
            mode_weights_in_block[column_start:column_end] = projection_weights[:, mode_index]
            basis_stem = f"{block_key}_{channel_tag}_mode{mode_index + 1}"
            feature_list.append({
                "name": f"{basis_stem}_pos",
                "block": block_key, "pair_stem": basis_stem, "sign": "pos",
                "shared_pattern": True,
                "weight_positive_part": mode_weights_in_block,
                "weight_negative_part": None,
            })
            feature_list.append({
                "name": f"{basis_stem}_neg",
                "block": block_key, "pair_stem": basis_stem, "sign": "neg",
                "shared_pattern": True,
                "weight_positive_part": None,
                "weight_negative_part": mode_weights_in_block,
            })
    return feature_list


def build_fpls_features(block_key, X_positive_part, X_negative_part,
                        outcome_residual, train_row_positions,
                        temporal_chunk_labels):
    """Functional-PLS features: supervised weights fit on the stacked [X+, X-].

    The positive and negative parts are stacked column-wise into a single
    functional covariate on two copies of the spatial domain and the FPLS
    coefficient function is estimated on the training rows. Because the fit is
    supervised and joint, the recovered beta+ and beta- halves are NOT
    constrained to share spatial patterns. The component count is chosen by
    grouped CV on the training chunks (the package's own selection rule
    degenerates when p >= n, always returning the cap — see
    analysis.fpls_analysis.select_n_components), falling back to one component
    when no count achieves positive CV R^2.

    Parameters
    ----------
    block_key             : str — key of BLOCKS
    X_positive_part       : ndarray (T, p) — processed positive part
    X_negative_part       : ndarray (T, p) — processed negative part
    outcome_residual      : ndarray (T,) — residualized outcome (NaN for
                            missing hours; only training rows are used)
    train_row_positions   : ndarray — training rows (finite outcome only)
    temporal_chunk_labels : ndarray (T,) — chunk ids for grouped CV

    Returns
    -------
    feature_list  : list with two feature dicts (pos index, neg index)
    n_components  : int — the selected component count
    """
    # Step 1: stack the parts into the joint functional domain (train rows only
    # for fitting; float64 for the package's conjugate-gradient iterations).
    X_stacked_train = np.hstack([
        X_positive_part[train_row_positions],
        X_negative_part[train_row_positions],
    ]).astype(np.float64)
    y_train = outcome_residual[train_row_positions]
    n_block_columns = X_positive_part.shape[1]

    # Step 2: choose the component count by grouped CV over training chunks.
    train_chunk_labels = temporal_chunk_labels[train_row_positions]
    n_cv_splits = min(FPLS_CV_FOLDS, len(np.unique(train_chunk_labels)))
    cv_r2_by_component_count = np.full((n_cv_splits, FPLS_MAX_COMPONENTS), np.nan)
    grouped_kfold = GroupKFold(n_splits=n_cv_splits)
    for fold_index, (fold_train_rows, fold_test_rows) in enumerate(
            grouped_kfold.split(X_stacked_train, y_train, train_chunk_labels)):
        fold_model = FunctionalPLS(m_max=FPLS_MAX_COMPONENTS).fit(
            X_stacked_train[fold_train_rows], y_train[fold_train_rows],
            ds=FPLS_RIEMANN_STEP)
        for component_count in range(1, FPLS_MAX_COMPONENTS + 1):
            cv_r2_by_component_count[fold_index, component_count - 1] = _r2(
                y_train[fold_test_rows],
                fold_model.predict(X_stacked_train[fold_test_rows],
                                   n_components=component_count))
    mean_cv_r2 = np.nanmean(cv_r2_by_component_count, axis=0)
    if not np.any(np.isfinite(mean_cv_r2)) or np.nanmax(mean_cv_r2) <= 0:
        n_components = 1     # nothing generalizes — stay maximally parsimonious
    else:
        n_components = int(np.nanargmax(mean_cv_r2) + 1)

    # Step 3: final fit on all training rows; extract the coefficient function
    # at the selected component count and split it back into the two halves.
    final_model = FunctionalPLS(m_max=FPLS_MAX_COMPONENTS).fit(
        X_stacked_train, y_train, ds=FPLS_RIEMANN_STEP)
    stacked_beta = final_model.coef_[:, n_components].copy()

    # Step 4: orient beta so a higher functional index raises the outcome.
    training_functional_index = X_stacked_train @ stacked_beta
    if np.corrcoef(training_functional_index, y_train)[0, 1] < 0:
        stacked_beta = -stacked_beta

    # Note: the FPLS pair shares a stem for plotting purposes, but because the
    # two halves of beta were estimated jointly (not as a shared spatial
    # pattern), the pair-level symmetry test is not computed for FPLS —
    # asymmetry is assessed from the beta+/beta- surfaces instead.
    basis_stem = f"{block_key}_fpls_index"
    feature_list = [
        {"name": f"{block_key}_fpls_pos_index",
         "block": block_key, "pair_stem": basis_stem, "sign": "pos",
         "shared_pattern": False,
         "weight_positive_part": stacked_beta[:n_block_columns].astype(np.float32),
         "weight_negative_part": None},
        {"name": f"{block_key}_fpls_neg_index",
         "block": block_key, "pair_stem": basis_stem, "sign": "neg",
         "shared_pattern": False,
         "weight_positive_part": None,
         "weight_negative_part": stacked_beta[n_block_columns:].astype(np.float32)},
    ]
    return feature_list, n_components


# ═══════════════════════════════════════════════════════════════════════════════
# Section 3: Shared downstream — scoring, bootstrap regression, reconstruction
# ═══════════════════════════════════════════════════════════════════════════════

def compute_feature_scores(feature_list, X_positive_by_block, X_negative_by_block):
    """Score every feature: t = X+ @ w+  +  X- @ w-.

    Parameters
    ----------
    feature_list         : list of feature dicts (possibly spanning blocks)
    X_positive_by_block  : dict {block: ndarray (T, p_block)}
    X_negative_by_block  : dict {block: ndarray (T, p_block)}

    Returns
    -------
    pd.DataFrame (T, n_features) — one column per feature, in list order.
    """
    score_columns = {}
    for feature in feature_list:
        block_key = feature["block"]
        feature_score = np.zeros(X_positive_by_block[block_key].shape[0])
        if feature["weight_positive_part"] is not None:
            feature_score += X_positive_by_block[block_key] @ feature["weight_positive_part"]
        if feature["weight_negative_part"] is not None:
            feature_score += X_negative_by_block[block_key] @ feature["weight_negative_part"]
        score_columns[feature["name"]] = feature_score
    return pd.DataFrame(score_columns)


def run_moving_block_bootstrap_regression(outcome_values, scores_dataframe, hours,
                                          n_bootstrap_draws=N_BOOTSTRAP_DRAWS,
                                          block_days=BOOTSTRAP_BLOCK_DAYS,
                                          seed=RANDOM_STATE):
    """OLS of the outcome on the feature scores with moving-block bootstrap SEs.

    The point estimate is plain OLS (with intercept). Standard errors, 95%
    confidence intervals, and p-values come from refitting on resamples of
    contiguous `block_days`-day blocks of hours, so inference respects the
    strong intraday/synoptic dependence of both weather and market outcomes.
    The full matrix of bootstrap parameter draws is returned so downstream
    steps (coefficient-surface bands, symmetry tests) reuse the same draws.

    Parameters
    ----------
    outcome_values    : ndarray (T,) — residualized outcome (no NaN)
    scores_dataframe  : pd.DataFrame (T, n_features) — feature scores (no NaN)
    hours             : pd.DatetimeIndex (T,)
    n_bootstrap_draws : int
    block_days        : int
    seed              : int

    Returns
    -------
    dict with: coefficient_names (list, 'const' first), params (k+1,),
    standard_errors, t_values, p_values, conf_int (k+1, 2),
    bootstrap_draws (n_valid_draws, k+1), fitted_values, r_squared.
    """
    coefficient_names = ["const"] + list(scores_dataframe.columns)
    design_matrix = np.column_stack([np.ones(len(scores_dataframe)),
                                     scores_dataframe.values])
    point_params  = np.linalg.lstsq(design_matrix, outcome_values, rcond=None)[0]
    fitted_values = design_matrix @ point_params
    r_squared     = _r2(outcome_values, fitted_values)

    # Resample contiguous multi-day blocks of whole days.
    day_of_each_hour = hours.normalize()
    unique_days      = pd.DatetimeIndex(sorted(day_of_each_hour.unique()))
    rows_by_day      = {day: np.where(day_of_each_hour == day)[0] for day in unique_days}
    n_blocks_needed  = int(np.ceil(len(unique_days) / block_days))
    max_block_start  = max(1, len(unique_days) - block_days + 1)
    random_generator = np.random.default_rng(seed)

    bootstrap_draws = np.full((n_bootstrap_draws, len(coefficient_names)), np.nan)
    for draw_index in range(n_bootstrap_draws):
        block_start_days = random_generator.integers(0, max_block_start,
                                                     size=n_blocks_needed)
        resampled_rows = np.concatenate([
            np.concatenate([rows_by_day[day]
                            for day in unique_days[start:start + block_days]])
            for start in block_start_days])
        try:
            bootstrap_draws[draw_index] = np.linalg.lstsq(
                design_matrix[resampled_rows], outcome_values[resampled_rows],
                rcond=None)[0]
        except np.linalg.LinAlgError:
            continue

    valid_draws     = bootstrap_draws[~np.isnan(bootstrap_draws).any(axis=1)]
    standard_errors = valid_draws.std(axis=0, ddof=1)
    conf_int = np.percentile(valid_draws, [2.5, 97.5], axis=0).T
    with np.errstate(divide="ignore", invalid="ignore"):
        t_values = np.where(standard_errors > 0, point_params / standard_errors, 0.0)
    p_values = 2.0 * scipy_stats.norm.sf(np.abs(t_values))

    return {
        "coefficient_names": coefficient_names,
        "params": point_params, "standard_errors": standard_errors,
        "t_values": t_values, "p_values": p_values, "conf_int": conf_int,
        "bootstrap_draws": valid_draws,
        "fitted_values": fitted_values, "r_squared": r_squared,
    }


def reconstruct_beta_surfaces(feature_list, regression_result, offsets_by_block,
                              fields_by_block):
    """Rebuild the beta+(s) / beta-(s) coefficient surfaces with bootstrap bands.

    Every method's fitted model implies pixel-level coefficient surfaces
    beta_sign(s) = sum_f b_f * w_sign_f(s). Because the same reconstruction is
    applied to all four methods, the resulting maps are directly comparable.
    Pointwise 95% bands come from applying the reconstruction to every
    bootstrap parameter draw. Units: outcome (residualized/transformed scale)
    per one pooled-SD channel anomaly at pixel s.

    Parameters
    ----------
    feature_list      : list of feature dicts used in the regression
    regression_result : dict from run_moving_block_bootstrap_regression
    offsets_by_block  : dict {block: list[(start, end)]}
    fields_by_block   : dict {block: list[str]}

    Returns
    -------
    dict {"{block}__{channel_tag}__{sign}": {"beta", "beta_lo", "beta_hi"}}
    with one entry per block x channel x sign; arrays are (p_channel,).
    """
    coefficient_names = regression_result["coefficient_names"]
    point_params      = regression_result["params"]
    bootstrap_draws   = regression_result["bootstrap_draws"]

    beta_surfaces = {}
    for block_key, channel_offsets in offsets_by_block.items():
        for field, (column_start, column_end) in zip(fields_by_block[block_key],
                                                     channel_offsets):
            channel_tag = CHANNEL_TAGS[field]
            for sign_tag, weight_key in [("pos", "weight_positive_part"),
                                         ("neg", "weight_negative_part")]:
                # Collect this slice's weight vectors and their coefficients.
                weight_rows, coefficient_positions = [], []
                for feature in feature_list:
                    if feature["block"] != block_key or feature[weight_key] is None:
                        continue
                    weight_rows.append(
                        feature[weight_key][column_start:column_end].astype(float))
                    coefficient_positions.append(
                        coefficient_names.index(feature["name"]))
                if not weight_rows:
                    continue
                weight_matrix = np.array(weight_rows)              # (n_f, p_channel)
                point_surface = point_params[coefficient_positions] @ weight_matrix
                draw_surfaces = bootstrap_draws[:, coefficient_positions] @ weight_matrix
                surface_band  = np.percentile(draw_surfaces, [2.5, 97.5], axis=0)
                beta_surfaces[f"{block_key}__{channel_tag}__{sign_tag}"] = {
                    "beta":    point_surface.astype(np.float32),
                    "beta_lo": surface_band[0].astype(np.float32),
                    "beta_hi": surface_band[1].astype(np.float32),
                }
    return beta_surfaces


def build_coefficient_table(regression_result, scores_dataframe, feature_list):
    """Coefficient table with bootstrap inference and per-pair symmetry tests.

    Positive/negative feature pairs are linked STRUCTURALLY through each
    feature's ("pair_stem", "sign") metadata (never by parsing name suffixes).
    For pairs whose two features apply the SAME spatial pattern to the two
    parts (shared_pattern=True — the mask and EOF bases), the symmetric-model
    restriction H0: b+ = b- is tested from the bootstrap draws (two-sided
    percentile p-value of the coefficient difference) and reported on both
    rows. FPLS pairs (shared_pattern=False) are skipped: their two halves
    multiply different weight functions, so equality of the index coefficients
    is not the symmetry restriction — FPLS asymmetry is read off the
    beta+/beta- surfaces instead. The 'response_shape' column labels pairs:
    'V' when both error directions raise the outcome (b+ > 0, b- < 0),
    'inverted_V' for the opposite, 'monotonic' otherwise.

    Parameters
    ----------
    regression_result : dict from run_moving_block_bootstrap_regression
    scores_dataframe  : pd.DataFrame — used for the per-feature score SD column
    feature_list      : list of feature dicts — supplies pair_stem/sign/
                        shared_pattern metadata per coefficient row

    Returns
    -------
    pd.DataFrame — one row per coefficient (including the intercept).
    """
    coefficient_names = regression_result["coefficient_names"]
    feature_by_name   = {feature["name"]: feature for feature in feature_list}
    table = pd.DataFrame({
        "feature":   coefficient_names,
        "pair_stem": [feature_by_name.get(name, {}).get("pair_stem", "")
                      for name in coefficient_names],
        "sign":      [feature_by_name.get(name, {}).get("sign", "")
                      for name in coefficient_names],
        "coef":      regression_result["params"],
        "boot_se":   regression_result["standard_errors"],
        "t_value":   regression_result["t_values"],
        "p_value":   regression_result["p_values"],
        "ci_lower":  regression_result["conf_int"][:, 0],
        "ci_upper":  regression_result["conf_int"][:, 1],
    })
    score_standard_deviations = scores_dataframe.std()
    table["score_sd"] = [np.nan] + [score_standard_deviations[name]
                                    for name in coefficient_names[1:]]

    # Symmetry test per pair, linked through the structured pair metadata.
    bootstrap_draws = regression_result["bootstrap_draws"]
    table["symmetry_p"]     = np.nan
    table["response_shape"] = ""
    positions_by_pair = {}
    for coefficient_position, name in enumerate(coefficient_names):
        feature = feature_by_name.get(name)
        if feature is not None:
            positions_by_pair.setdefault(feature["pair_stem"], {})[
                feature["sign"]] = coefficient_position
    for pair_stem, positions_by_sign in positions_by_pair.items():
        if "pos" not in positions_by_sign or "neg" not in positions_by_sign:
            continue
        positive_position = positions_by_sign["pos"]
        negative_position = positions_by_sign["neg"]

        positive_coef = regression_result["params"][positive_position]
        negative_coef = regression_result["params"][negative_position]
        if positive_coef > 0 and negative_coef < 0:
            response_shape = "V"
        elif positive_coef < 0 and negative_coef > 0:
            response_shape = "inverted_V"
        else:
            response_shape = "monotonic"

        # The b+ = b- test is only meaningful when both features apply the same
        # spatial pattern (see docstring); FPLS pairs keep symmetry_p = NaN.
        pair_shares_pattern = feature_by_name[
            coefficient_names[positive_position]].get("shared_pattern", True)
        if pair_shares_pattern:
            coefficient_difference_draws = (bootstrap_draws[:, positive_position]
                                            - bootstrap_draws[:, negative_position])
            share_below_zero = float(np.mean(coefficient_difference_draws < 0))
            symmetry_p_value = 2.0 * min(share_below_zero, 1.0 - share_below_zero)
        else:
            symmetry_p_value = np.nan
        for row_position in (positive_position, negative_position):
            table.loc[row_position, "symmetry_p"]     = symmetry_p_value
            table.loc[row_position, "response_shape"] = response_shape
    return table


# ═══════════════════════════════════════════════════════════════════════════════
# Section 4: Driver — the shared pipeline over cuts x outcomes x methods
# ═══════════════════════════════════════════════════════════════════════════════

def run_main_analysis(methods=None, depvars=None, cuts=None, months=None):
    """Run the unified asymmetric-error pipeline for every cut x outcome x method.

    Per cut: subsets the hours, builds each block's positive/negative parts,
    and constructs the outcome-independent bases (masks, rotated EOF). Per
    outcome: residualizes the outcome, fits the outcome-dependent basis (FPLS),
    and for every method runs the shared scoring -> bootstrap regression ->
    out-of-sample R^2 -> beta-surface reconstruction stages, saving coefficient
    tables and beta-surface archives.

    Parameters
    ----------
    methods : list[str] — subset of METHODS; defaults to all four
    depvars : list[str] — outcomes; defaults to all DEPVARS
    cuts    : list[str] — subset of SAMPLE_CUTS keys; defaults to all five
    months  : list of (year, month); defaults to all 12 months of 2025

    Returns
    -------
    pd.DataFrame — the R^2 summary (one row per cut x outcome x method).
    """
    methods = list(methods or METHODS)
    depvars = list(depvars or DEPVARS)
    cuts    = list(cuts or SAMPLE_CUTS)
    months  = months or ALL_MONTHS

    project_directories = setup_directories()
    beta_output_dir  = Path(project_directories["processed"]) / "main_analysis"
    table_output_dir = Path(project_directories["tables"])    / "main_analysis"
    beta_output_dir.mkdir(parents=True, exist_ok=True)
    table_output_dir.mkdir(parents=True, exist_ok=True)

    print("\n=== Loading channel fields and outcomes ===")
    channel_bundle     = load_channel_fields(months, project_directories)
    outcomes_dataframe = load_outcomes(project_directories, months=months)
    all_valid_hours    = channel_bundle["hours"]

    # The pixel masks depend only on each block's grid, which is identical
    # across cuts — build them once here from the full-sample block matrices.
    print("\n=== Building manual-basis pixel masks (per block grid) ===")
    infrastructure_masks_by_block = {}
    weather_zone_masks_by_block   = {}
    for block_key, block_config in BLOCKS.items():
        _, _, block_pixel_lats, block_pixel_lons = build_block_matrix(
            channel_bundle, block_config["fields"])
        infrastructure_masks_by_block[block_key] = build_infrastructure_pixel_masks(
            block_pixel_lats, block_pixel_lons)
        weather_zone_masks_by_block[block_key] = build_weather_zone_pixel_masks(
            block_pixel_lats, block_pixel_lons)
        print(f"  {block_key}: {len(block_pixel_lats)} pixel columns")

    r2_summary_rows = []
    for cut_key in cuts:
        cut_config = SAMPLE_CUTS[cut_key]
        # Step 1: subset the hours to this cut.
        if cut_config["months"] is None:
            cut_selection_mask = np.ones(len(all_valid_hours), dtype=bool)
        else:
            cut_selection_mask = np.isin(all_valid_hours.month, cut_config["months"])
        n_cut_hours = int(cut_selection_mask.sum())
        print(f"\n{'=' * 70}\n=== Cut [{cut_config['label']}]: {n_cut_hours} hours ===\n{'=' * 70}")
        if n_cut_hours < 200:
            print("  Skipping — too few hours")
            continue

        cut_bundle      = _subset_bundle(channel_bundle, cut_selection_mask)
        cut_valid_hours = cut_bundle["hours"]
        time_controls   = build_time_controls(cut_valid_hours)
        temporal_chunk_labels = _chunk_labels(cut_valid_hours)
        train_mask, test_mask = make_chunk_splits(cut_valid_hours, seed=RANDOM_STATE)
        train_row_positions   = np.where(train_mask)[0]

        # Step 2: per block — flatten the field, split into parts, residualize.
        X_positive_by_block, X_negative_by_block = {}, {}
        offsets_by_block, fields_by_block = {}, {}
        pixel_coordinates_by_block = {}
        for block_key, block_config in BLOCKS.items():
            X_signed, offsets, pixel_lats, pixel_lons = build_block_matrix(
                cut_bundle, block_config["fields"])
            X_positive_part, X_negative_part, channel_scales = (
                prepare_positive_negative_parts(
                    block_key, X_signed, offsets, time_controls, train_row_positions))
            X_positive_by_block[block_key] = X_positive_part
            X_negative_by_block[block_key] = X_negative_part
            offsets_by_block[block_key]    = offsets
            fields_by_block[block_key]     = block_config["fields"]
            pixel_coordinates_by_block[block_key] = (pixel_lats, pixel_lons)
            del X_signed
            print(f"  Prepared {block_key}: p={X_positive_part.shape[1]}  "
                  f"channel scales={[f'{s:.3f}' for s in channel_scales]}")

        # Step 3: outcome-independent bases (masks and rotated EOF) — built once
        # per cut and reused across every outcome.
        outcome_independent_features = {}
        if "infrastructure" in methods:
            outcome_independent_features["infrastructure"] = [
                feature
                for block_key, block_config in BLOCKS.items()
                for feature in build_mask_features(
                    block_key, block_config["fields"], offsets_by_block[block_key],
                    infrastructure_masks_by_block[block_key])
            ]
        if "weather_zone" in methods:
            outcome_independent_features["weather_zone"] = [
                feature
                for block_key, block_config in BLOCKS.items()
                for feature in build_mask_features(
                    block_key, block_config["fields"], offsets_by_block[block_key],
                    weather_zone_masks_by_block[block_key])
            ]
        if "rotated_eof" in methods:
            print("  Fitting Varimax EOFs per channel (train hours) ...")
            outcome_independent_features["rotated_eof"] = [
                feature
                for block_key, block_config in BLOCKS.items()
                for feature in build_rotated_eof_features(
                    cut_bundle, block_key, block_config["fields"],
                    offsets_by_block[block_key], train_row_positions)
            ]

        # Step 4: per outcome — residualize y, fit FPLS, run every method.
        for depvar in depvars:
            if depvar not in outcomes_dataframe.columns:
                print(f"  {depvar}: not in outcomes — skipping")
                continue
            depvar_config = DEPVAR_CONFIGS.get(depvar, {"label": depvar, "transform": "log1p"})
            transformed_outcome = _transform_y(
                outcomes_dataframe[depvar].reindex(cut_valid_hours),
                depvar_config["transform"]).values
            finite_outcome_mask = np.isfinite(transformed_outcome)
            if int((train_mask & finite_outcome_mask).sum()) < 100:
                print(f"  {depvar}: too few finite training hours — skipping")
                continue

            # Residualize the outcome on the time controls (NaN-safe: fill with
            # the mean, residualize, then track missing hours via the mask).
            outcome_filled   = np.where(finite_outcome_mask, transformed_outcome,
                                        np.nanmean(transformed_outcome))
            outcome_residual = frisch_waugh_residualize(outcome_filled, time_controls)
            outcome_residual_masked = np.where(finite_outcome_mask, outcome_residual, np.nan)
            train_finite_positions = np.where(train_mask & finite_outcome_mask)[0]
            test_finite_positions  = np.where(test_mask  & finite_outcome_mask)[0]
            print(f"\n  ── {depvar_config['label']} "
                  f"({int(finite_outcome_mask.sum())} finite hours) ──")

            for method in methods:
                # Basis construction — the only method-specific stage.
                if method == "fpls":
                    feature_list = []
                    fpls_components_by_block = {}
                    for block_key in BLOCKS:
                        block_features, n_components = build_fpls_features(
                            block_key,
                            X_positive_by_block[block_key],
                            X_negative_by_block[block_key],
                            outcome_residual_masked, train_finite_positions,
                            temporal_chunk_labels)
                        feature_list.extend(block_features)
                        fpls_components_by_block[block_key] = n_components
                else:
                    feature_list = outcome_independent_features[method]
                    fpls_components_by_block = None

                # Shared stage a: score every feature for every hour.
                scores_dataframe = compute_feature_scores(
                    feature_list, X_positive_by_block, X_negative_by_block)

                # Shared stage b: inference regression on all finite hours with
                # moving-block bootstrap standard errors.
                finite_positions = np.where(finite_outcome_mask)[0]
                regression_result = run_moving_block_bootstrap_regression(
                    outcome_residual[finite_positions],
                    scores_dataframe.iloc[finite_positions],
                    cut_valid_hours[finite_positions])

                # Shared stage c: out-of-sample R^2 — OLS fit on training chunks
                # only, evaluated on the held-out chunks.
                train_design = np.column_stack([
                    np.ones(len(train_finite_positions)),
                    scores_dataframe.values[train_finite_positions]])
                train_only_params = np.linalg.lstsq(
                    train_design, outcome_residual[train_finite_positions], rcond=None)[0]
                test_design = np.column_stack([
                    np.ones(len(test_finite_positions)),
                    scores_dataframe.values[test_finite_positions]])
                out_of_sample_r2 = _r2(outcome_residual[test_finite_positions],
                                       test_design @ train_only_params)

                # Shared stage d: coefficient table with symmetry tests.
                coefficient_table = build_coefficient_table(
                    regression_result, scores_dataframe.iloc[finite_positions],
                    feature_list)
                cut_table_dir = table_output_dir / cut_key
                cut_table_dir.mkdir(parents=True, exist_ok=True)
                coefficient_table.to_csv(
                    cut_table_dir / f"{method}_coefficients_{depvar}.csv", index=False)

                # Shared stage e: reconstruct and save the beta surfaces.
                beta_surfaces = reconstruct_beta_surfaces(
                    feature_list, regression_result, offsets_by_block, fields_by_block)
                beta_archive = {}
                for surface_key, surface in beta_surfaces.items():
                    for array_name, array in surface.items():
                        beta_archive[f"{surface_key}__{array_name}"] = array
                for block_key in BLOCKS:
                    pixel_lats, pixel_lons = pixel_coordinates_by_block[block_key]
                    beta_archive[f"{block_key}__lat"] = pixel_lats
                    beta_archive[f"{block_key}__lon"] = pixel_lons
                    beta_archive[f"{block_key}__offsets"] = np.array(
                        offsets_by_block[block_key], dtype=int)
                beta_archive_path = (beta_output_dir
                                     / f"betas_{method}_{cut_key}_{depvar}.npz")
                np.savez_compressed(beta_archive_path, **beta_archive)
                run_metadata = {
                    "method": method, "cut_key": cut_key, "cut_label": cut_config["label"],
                    "depvar": depvar, "depvar_label": depvar_config["label"],
                    "transform": depvar_config["transform"],
                    "fields_by_block": fields_by_block,
                    # Field-name -> channel-tag mapping saved so figure code can
                    # resolve channels from the archive alone (no string sniffing).
                    "channel_tags": CHANNEL_TAGS,
                    "n_features": len(feature_list),
                    "full_sample_r2": regression_result["r_squared"],
                    "out_of_sample_r2": out_of_sample_r2,
                    "fpls_components_by_block": fpls_components_by_block,
                    "n_hours": int(finite_outcome_mask.sum()),
                }
                beta_archive_path.with_suffix(".json").write_text(json.dumps(run_metadata))

                n_significant = int((regression_result["p_values"][1:] < 0.05).sum())
                print(f"    {method:15s}: features={len(feature_list):3d}  "
                      f"R2(full)={regression_result['r_squared']:.3f}  "
                      f"R2(oos)={out_of_sample_r2:.3f}  sig@5%={n_significant}"
                      + (f"  fpls_k={fpls_components_by_block}"
                         if fpls_components_by_block else ""))
                r2_summary_rows.append({
                    "cut": cut_key, "depvar": depvar, "method": method,
                    "n_features": len(feature_list),
                    "full_sample_r2": regression_result["r_squared"],
                    "out_of_sample_r2": out_of_sample_r2,
                    "n_hours": int(finite_outcome_mask.sum()),
                })

    # Persist the R^2 summary, merging with any rows from previous partial runs
    # so repeated invocations with different subsets accumulate into one table.
    r2_summary = pd.DataFrame(r2_summary_rows)
    r2_summary_path = table_output_dir / "r2_summary.csv"
    if r2_summary_path.exists() and len(r2_summary):
        previous_summary = pd.read_csv(r2_summary_path)
        r2_summary = (pd.concat([previous_summary, r2_summary], ignore_index=True)
                      .drop_duplicates(subset=["cut", "depvar", "method"], keep="last"))
    r2_summary.to_csv(r2_summary_path, index=False)
    print(f"\nDone — R^2 summary at {r2_summary_path}")
    return r2_summary


# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    """CLI entry point: run the unified analysis, then (optionally) all figures."""
    argument_parser = argparse.ArgumentParser(
        description="Unified asymmetric-error analysis (four basis methods)")
    argument_parser.add_argument("--methods", nargs="*", default=None,
                                 choices=list(METHODS),
                                 help="Basis methods to run (default: all four)")
    argument_parser.add_argument("--depvars", nargs="*", default=None,
                                 help="Outcome variables (default: all)")
    argument_parser.add_argument("--cuts", nargs="*", default=None,
                                 choices=list(SAMPLE_CUTS),
                                 help="Sample cuts (default: all five)")
    argument_parser.add_argument("--skip-figures", action="store_true",
                                 help="Skip the figure-generation step")
    cli_arguments = argument_parser.parse_args()

    run_main_analysis(methods=cli_arguments.methods, depvars=cli_arguments.depvars,
                      cuts=cli_arguments.cuts)

    if not cli_arguments.skip_figures:
        from analysis.figures import make_all_figures
        make_all_figures(methods=cli_arguments.methods, depvars=cli_arguments.depvars,
                         cuts=cli_arguments.cuts)


if __name__ == "__main__":
    main()
