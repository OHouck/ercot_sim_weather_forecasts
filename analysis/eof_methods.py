"""
A zoo of comparable EOF/PCA decomposition methods for ERCOT forecast errors.

Each method takes the channel-field bundle from pca_decomposition.load_channel_fields,
fits its spatial basis on a training-hour subset only, then projects *all* hours
onto that basis to produce per-hour scores. Scores are returned in a common
container (MethodResult) so the comparison harness (eof_method_comparison.py) can
feed any method's scores into the same damage-function regression.

Methods implemented (all operate on the four forecast-ERROR channels):
  eof_perchannel  — standard EOF fitted separately per channel (the baseline,
                    mirrors pca_decomposition.fit_pca_channels)
  eof_joint       — multivariate EOF: the four channels stacked into one state
                    vector and decomposed jointly (captures cross-channel
                    co-occurrence, e.g. warm bias + wind under-forecast)
  varimax_joint   — Varimax-rotated multivariate EOF (localised, interpretable
                    joint modes; orthogonal rotation of the joint basis)
  sparse_joint    — Sparse (L1) multivariate EOF: loadings driven to exact zero
                    so each mode is a compact region (SCoTLASS-style)
  eeof_perchannel — Extended EOF / MSSA per channel: lag-embedded basis capturing
                    the temporal evolution of errors (ramps, timing offsets)
  mca             — Maximum Covariance Analysis between the joint error field (X)
                    and the outcome vector (Y): the supervised method whose modes
                    are, by construction, the error patterns that covary with
                    grid outcomes

All bases are fit on TRAIN hours only and projected out-of-sample, so downstream
predictive skill is an honest test estimate. xeofs lacks an out-of-sample
transform for Extended EOF, so eeof_perchannel projects test hours manually onto
the fitted loadings (validated to reproduce xeofs in-sample scores exactly).
"""

import warnings
from dataclasses import dataclass, field
from typing import Callable

import numpy as np
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parent))
from analysis.pca_decomposition import ERROR_FIELDS

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# ── Defaults ─────────────────────────────────────────────────────────────────

DEFAULT_K        = 5
EEOF_EMBEDDING   = 6      # number of lagged copies (delay-embedding dimension)
EEOF_TAU         = 1      # lag step in hours between embedded copies
SPARSE_ALPHA     = 1e-3   # L1 strength for SparsePCA (larger → sparser loadings)
VARIMAX_POWER    = 1      # 1 = Varimax (orthogonal); >1 = Promax-like obliqueness

# Outcomes used to build the MCA "Y" field. Kept small and high-coverage so the
# supervised basis is not dominated by sparse/NaN columns.
MCA_OUTCOMES = [
    "economic_congestion_cost",
    "total_renewable_curtailment_mw",
    "avg_intensity_kg_per_mwh",
]


@dataclass
class MethodResult:
    """Container for one fitted decomposition method.

    Attributes
    ----------
    name        : short machine key (e.g. "eof_joint")
    label       : human-readable label for tables/figures
    scores      : pd.DataFrame (hours × predictors) — per-hour scores, the
                  features handed to the damage regression. May contain NaN
                  rows (e.g. EEOF tail) which the regression drops.
    supervised  : True if the basis was fit using outcome information (MCA)
    n_modes     : modes requested per block
    diagnostics : method-specific dict (variance explained, covariance fraction,
                  fraction of outcome variance explained, loadings for plotting)
    """
    name: str
    label: str
    scores: pd.DataFrame
    supervised: bool
    n_modes: int
    diagnostics: dict = field(default_factory=dict)


# ── Score / data helpers ─────────────────────────────────────────────────────


def _scores_to_df(scores_da, prefix):
    """Convert an xeofs scores DataArray (mode, valid_time) to a tidy DataFrame.

    Parameters
    ----------
    scores_da : xr.DataArray with dims including 'mode' and 'valid_time'
    prefix    : str — column-name prefix (columns become f"{prefix}_PC{k}")

    Returns
    -------
    pd.DataFrame indexed by valid_time, one column per mode.
    """
    arr  = scores_da.transpose("valid_time", "mode").values
    idx  = pd.DatetimeIndex(scores_da["valid_time"].values)
    cols = [f"{prefix}_PC{k + 1}" for k in range(arr.shape[1])]
    return pd.DataFrame(np.asarray(arr, dtype=float), index=idx, columns=cols)


def _error_da_list(bundle, error_fields):
    """Return the list of all-hour error DataArrays for the joint methods."""
    return [bundle["channel_da"][f] for f in error_fields]


# ── Tier-1 / Tier-2 unsupervised methods ─────────────────────────────────────


def fit_eof_perchannel(bundle, train_idx, hours, K=DEFAULT_K,
                       error_fields=ERROR_FIELDS, seed=42):
    """Baseline: standard EOF fitted separately on each error channel.

    Parameters
    ----------
    bundle       : dict from pca_decomposition.load_channel_fields
    train_idx    : ndarray of integer positions into `hours` used for fitting
    hours        : pd.DatetimeIndex — all hours (scores reindexed to this)
    K            : int — modes per channel
    error_fields : list of channel names to decompose
    seed         : int — solver random_state

    Returns
    -------
    MethodResult with 4×K predictor columns (K per channel).
    """
    from xeofs.single import EOF

    frames, var_expl, loadings = [], {}, {}
    for f in error_fields:
        da = bundle["channel_da"][f]
        m  = EOF(n_modes=K, center=True, random_state=seed)
        m.fit(da.isel(valid_time=train_idx), dim="valid_time")
        frames.append(_scores_to_df(m.transform(da), f))
        var_expl[f]  = np.asarray(m.explained_variance_ratio().values, dtype=float)
        loadings[f]  = m.components()
    scores = pd.concat(frames, axis=1).reindex(hours)
    return MethodResult(
        "eof_perchannel", "EOF (per-channel)", scores, False, K,
        {"var_explained": var_expl, "loadings_perfield": loadings},
    )


def _fit_joint_model(model, bundle, train_idx, hours, prefix, error_fields):
    """Shared fit/transform for the multivariate (list-input) single-field models.

    Parameters
    ----------
    model        : an unfitted xeofs single-field model accepting list input
    prefix       : column-name prefix for the resulting scores

    Returns
    -------
    (scores_df reindexed to hours, explained_variance_ratio ndarray)
    """
    da_list = _error_da_list(bundle, error_fields)
    model.fit([d.isel(valid_time=train_idx) for d in da_list], dim="valid_time")
    scores = _scores_to_df(model.transform(da_list), prefix).reindex(hours)
    evr    = np.asarray(model.explained_variance_ratio().values, dtype=float)
    return scores, evr


def fit_eof_joint(bundle, train_idx, hours, K=DEFAULT_K,
                  error_fields=ERROR_FIELDS, seed=42):
    """Multivariate EOF: all error channels stacked and decomposed jointly."""
    from xeofs.single import EOF

    model = EOF(n_modes=K, center=True, random_state=seed)
    scores, evr = _fit_joint_model(model, bundle, train_idx, hours, "JOINT", error_fields)
    return MethodResult(
        "eof_joint", "EOF (joint)", scores, False, K,
        {"var_explained": {"JOINT": evr},
         "loadings_list": model.components(), "fields": error_fields},
    )


def fit_varimax_joint(bundle, train_idx, hours, K=DEFAULT_K,
                      error_fields=ERROR_FIELDS, seed=42, power=VARIMAX_POWER):
    """Varimax-rotated multivariate EOF (orthogonal rotation of the joint basis)."""
    from xeofs.single import EOF, EOFRotator

    da_list = _error_da_list(bundle, error_fields)
    base = EOF(n_modes=K, center=True, random_state=seed)
    base.fit([d.isel(valid_time=train_idx) for d in da_list], dim="valid_time")
    rot = EOFRotator(n_modes=K, power=power)
    rot.fit(base)
    scores = _scores_to_df(rot.transform(da_list), "VMAX").reindex(hours)
    evr    = np.asarray(rot.explained_variance_ratio().values, dtype=float)
    return MethodResult(
        "varimax_joint", "Varimax EOF (joint)", scores, False, K,
        {"var_explained": {"VMAX": evr},
         "loadings_list": rot.components(), "fields": error_fields},
    )


def fit_sparse_joint(bundle, train_idx, hours, K=DEFAULT_K,
                     error_fields=ERROR_FIELDS, seed=42, alpha=SPARSE_ALPHA):
    """Sparse (L1) multivariate EOF — loadings shrunk to exact zeros (SCoTLASS-style)."""
    from xeofs.single import SparsePCA

    model = SparsePCA(n_modes=K, alpha=alpha, center=True, random_state=seed)
    scores, evr = _fit_joint_model(model, bundle, train_idx, hours, "SPARSE", error_fields)
    return MethodResult(
        "sparse_joint", "Sparse EOF (joint)", scores, False, K,
        {"var_explained": {"SPARSE": evr},
         "loadings_list": model.components(), "fields": error_fields, "alpha": alpha},
    )


def _project_eeof(da, components, train_idx, embedding, tau, prefix):
    """Project all hours onto a fitted Extended-EOF basis (manual OOS transform).

    xeofs does not implement transform() for ExtendedEOF, so we reconstruct the
    delay-embedded design and project it onto the fitted loadings. The score at
    start-time index i uses the window [anom[i], anom[i+tau], ..., anom[i+(E-1)tau]]
    dotted with the (mode, embedding, feature) loadings — the same convention
    xeofs uses internally (validated to reproduce its in-sample scores at corr=1).

    Parameters
    ----------
    da         : xr.DataArray (valid_time, latitude, longitude) — all hours
    components : xr.DataArray (mode, embedding, latitude, longitude) — fitted loadings
    train_idx  : ndarray — training positions, used only to compute the centring mean
    embedding  : int — delay-embedding dimension E
    tau        : int — lag step
    prefix     : str — column-name prefix

    Returns
    -------
    pd.DataFrame (n_windows × n_modes); the final (E-1)*tau hours have no full
    window and are omitted (callers reindex to the full hour range → NaN tail).
    """
    mean = da.isel(valid_time=train_idx).mean("valid_time")
    anom = np.nan_to_num((da - mean).values, nan=0.0)         # (T, lat, lon)
    T    = anom.shape[0]
    A    = anom.reshape(T, -1)                                # (T, feat)
    K    = components.sizes["mode"]
    L    = np.nan_to_num(components.values, nan=0.0).reshape(K, embedding, -1)  # (K, E, feat)

    n_win = T - (embedding - 1) * tau
    S = np.zeros((n_win, K), dtype=float)
    for e in range(embedding):
        S += A[e * tau: e * tau + n_win] @ L[:, e, :].T       # (n_win, feat) @ (feat, K)

    idx  = pd.DatetimeIndex(da["valid_time"].values[:n_win])
    cols = [f"{prefix}_EE{k + 1}" for k in range(K)]
    return pd.DataFrame(S, index=idx, columns=cols)


def fit_eeof_perchannel(bundle, train_idx, hours, K=DEFAULT_K,
                        error_fields=ERROR_FIELDS, seed=42,
                        embedding=EEOF_EMBEDDING, tau=EEOF_TAU):
    """Extended EOF (MSSA) per channel — lag-embedded modes capturing error evolution."""
    from xeofs.single import ExtendedEOF

    frames, var_expl, loadings_perfield = [], {}, {}
    for f in error_fields:
        da = bundle["channel_da"][f]
        m  = ExtendedEOF(n_modes=K, tau=tau, embedding=embedding,
                         center=True, random_state=seed)
        m.fit(da.isel(valid_time=train_idx), dim="valid_time")
        comps = m.components()                              # (mode, embedding, lat, lon)
        frames.append(_project_eeof(da, comps, train_idx, embedding, tau, f))
        var_expl[f]          = np.asarray(m.explained_variance_ratio().values, dtype=float)
        loadings_perfield[f] = comps.isel(embedding=0)     # (mode, lat, lon) — lag-0 pattern
    scores = pd.concat(frames, axis=1).reindex(hours)
    return MethodResult(
        "eeof_perchannel", "Extended EOF (per-channel)", scores, False, K,
        {"var_explained": var_expl, "embedding": embedding, "tau": tau,
         "loadings_perfield": loadings_perfield},
    )


# ── Tier-1 supervised method: MCA ────────────────────────────────────────────


def _build_outcome_field(outcomes_df, hours, train_idx, mca_outcomes, depvar_configs):
    """Assemble a clean, train-standardized outcome DataArray for MCA's Y side.

    Each outcome is transformed per its config (log1p / raw), standardized using
    training-row statistics, and remaining NaNs filled with 0 (the standardized
    mean) so xeofs sees no missing values on the Y side.

    Parameters
    ----------
    outcomes_df    : pd.DataFrame indexed by valid_time
    hours          : pd.DatetimeIndex
    train_idx      : ndarray — training positions (for standardization stats)
    mca_outcomes   : list of outcome column names
    depvar_configs : dict {outcome: {"transform": ...}}

    Returns
    -------
    xr.DataArray (valid_time, outcome)
    """
    import xarray as xr

    cols = {}
    for dv in mca_outcomes:
        s = outcomes_df[dv].reindex(hours)
        if depvar_configs.get(dv, {}).get("transform") == "log1p":
            s = np.log1p(s.clip(lower=0))
        cols[dv] = s
    mat = pd.DataFrame(cols, index=hours)

    train_rows = mat.iloc[train_idx]
    mu    = train_rows.mean()
    sigma = train_rows.std().clip(lower=1e-10)
    mat   = ((mat - mu) / sigma).fillna(0.0)

    return xr.DataArray(
        mat.values, dims=["valid_time", "outcome"],
        coords={"valid_time": hours, "outcome": list(mat.columns)},
    )


def fit_mca(bundle, train_idx, hours, K=DEFAULT_K, error_fields=ERROR_FIELDS,
            seed=42, outcomes_df=None, mca_outcomes=None, depvar_configs=None):
    """Maximum Covariance Analysis: joint error field (X) vs outcome vector (Y).

    The leading modes are, by construction, the error patterns that covary most
    with grid outcomes — the supervised counterpart to the unsupervised methods.
    The basis is fit on TRAIN errors and outcomes only; X-scores for all hours
    come from transform(X=...), so no test-outcome information leaks.

    Parameters
    ----------
    outcomes_df    : pd.DataFrame indexed by valid_time (required)
    mca_outcomes   : list of outcome columns for Y (defaults to MCA_OUTCOMES)
    depvar_configs : dict of per-outcome transforms (required for log1p handling)

    Returns
    -------
    MethodResult with K X-score columns; diagnostics carry the squared-covariance
    fraction and the fraction of outcome variance explained per mode.
    """
    from xeofs.cross import MCA

    if outcomes_df is None or depvar_configs is None:
        raise ValueError("fit_mca requires outcomes_df and depvar_configs")
    mca_outcomes = mca_outcomes or MCA_OUTCOMES

    da_list = _error_da_list(bundle, error_fields)
    Y = _build_outcome_field(outcomes_df, hours, train_idx, mca_outcomes, depvar_configs)

    # K cannot exceed the number of outcome columns (the rank of the coupling).
    # use_pca=False: the cross-covariance SVD is (n_X_features × n_outcomes), cheap
    # even for the full error grid, and avoids pre-reducing the tiny Y side to rank 1.
    K_eff = min(K, len(mca_outcomes))
    model = MCA(n_modes=K_eff, use_pca=False, standardize=False, random_state=seed)
    model.fit([d.isel(valid_time=train_idx) for d in da_list],
              Y.isel(valid_time=train_idx), dim="valid_time")

    x_scores = _scores_to_df(model.transform(X=da_list), "MCA").reindex(hours)
    x_comps, _ = model.components()   # tuple(X_list, Y_da); take X side
    diagnostics = {
        "squared_covariance_fraction": np.asarray(
            model.squared_covariance_fraction().values, dtype=float),
        "frac_var_Y_by_X": np.asarray(
            model.fraction_variance_Y_explained_by_X().values, dtype=float),
        "outcomes":     mca_outcomes,
        "loadings_list": x_comps,      # list of (mode, lat, lon) DataArrays, one per field
        "fields":        error_fields,
    }
    return MethodResult("mca", "MCA (errors↔outcomes)", x_scores, True, K_eff, diagnostics)


# ── Registry ─────────────────────────────────────────────────────────────────

# Maps method key → callable(bundle, train_idx, hours, **kwargs) → MethodResult.
# The supervised MCA additionally needs outcomes_df / depvar_configs in kwargs.
METHOD_REGISTRY: dict[str, Callable] = {
    "eof_perchannel":  fit_eof_perchannel,
    "eof_joint":       fit_eof_joint,
    "varimax_joint":   fit_varimax_joint,
    "sparse_joint":    fit_sparse_joint,
    "eeof_perchannel": fit_eeof_perchannel,
    "mca":             fit_mca,
}
