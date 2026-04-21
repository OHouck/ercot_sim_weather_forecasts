"""
Spatial Functional Regression for Forecast Error Attribution (ERCOT).

Estimates how spatially distributed weather forecast errors map into
aggregate renewable curtailment using three core approaches plus extensions:

  Step 1: Naive Ridge baseline (no spatial structure; raw pixels)
  Step 2: Functional PCA (FPCA) — unsupervised spatial basis
  Step 3: Partial Least Squares (PLS) — supervised spatial basis
  Step 4: Spatial regularization on recovered coefficient surface
  Step 5: Model comparison, counterfactual analysis, and visualization

Extensions:
  A: Multi-field comparison (FPCA + PLS across all 4 error fields)
  B: PLS vs FPCA divergence investigation (loading correlations)
  C: Regime-stratified analysis (extreme cold, extreme heat)
  D: Constrained PLS (spatially smooth loading vectors)
  E: Neural operator (FNO-to-scalar for nonlinear mapping)
  F: Quantile regression on FPCA scores (tail behaviour)
  G: Pixel stability analysis (cross-fold coefficient consistency)

Usage:
    uv run python -m analysis.pixel_level_functional_analysis
    uv run python -m analysis.pixel_level_functional_analysis --extensions-only
    uv run python -m analysis.pixel_level_functional_analysis --run3
    uv run python -m analysis.pixel_level_functional_analysis --run4
"""

import copy
import os
import sys
import json
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import cartopy.crs as ccrs
import cartopy.io.shapereader as shpreader
from sklearn.decomposition import PCA
from sklearn.linear_model import RidgeCV
from sklearn.model_selection import KFold, cross_val_score
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import r2_score, mean_squared_error

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", message=".*ill-conditioned.*")
warnings.filterwarnings("ignore", category=UserWarning)

# ── Configuration ────────────────────────────────────────────────────────────

DEPVAR = "total_curtailment_mw"
# Focus on wind speed errors (most relevant for curtailment) from both models
ERROR_FIELDS = ["wspd_error_1h", "wspd_error_0h", "temp_error_1h", "temp_error_0h"]
# Use every other month for seasonal coverage while keeping memory manageable
DEFAULT_MONTHS = [(2025, m) for m in [1, 3, 5, 7, 9, 11]]
N_CV_FOLDS = 5
RANDOM_STATE = 42


# ── Step 0: Data Loading & Preparation ───────────────────────────────────────

def load_pixel_data(months):
    """Load pixel-hourly parquets, keeping only the columns needed for
    functional analysis to reduce memory usage."""
    dirs = setup_directories()
    lmp_dir = Path(dirs["processed"]) / "combined_hourly_gridded_data"

    # Only load columns we actually need
    keep_cols = [
        "pixel_id", "valid_time", "latitude", "longitude",
        DEPVAR,
    ] + ERROR_FIELDS

    dfs = []
    for year, month in months:
        path = lmp_dir / f"pixel_hourly_gfs+hrrr_{year}_{month:02d}.parquet"
        if not path.exists():
            print(f"  [WARNING] Missing: {path}")
            continue
        # Read only needed columns
        import pyarrow.parquet as pq
        parquet_cols = pq.read_schema(path).names
        cols_to_read = [c for c in keep_cols if c in parquet_cols]
        df = pd.read_parquet(path, columns=cols_to_read)
        df["valid_time"] = pd.to_datetime(df["valid_time"])
        if df["valid_time"].dt.tz is not None:
            df["valid_time"] = df["valid_time"].dt.tz_localize(None)
        print(f"  Loaded {year}-{month:02d}: {len(df):,} rows")
        dfs.append(df)
    if not dfs:
        raise FileNotFoundError("No pixel_hourly parquet files found.")
    return pd.concat(dfs, ignore_index=True)


def prepare_functional_data(df, error_field="wspd_error_1h"):
    """Reshape pixel-level data into functional format.

    Returns
    -------
    X : ndarray, shape (T, N_pixels)
        Standardized forecast error field. Each row is one hour, each column
        is one spatial pixel.
    Y : ndarray, shape (T,)
        System-level outcome (total curtailment MW).
    pixel_coords : ndarray, shape (N_pixels, 2)
        (lat, lon) for each pixel column.
    pixel_ids : ndarray of str
        pixel_id labels matching columns of X.
    hour_index : pd.DatetimeIndex
        Timestamps for each row of X.
    """
    # Get unique hours and pixels
    required_cols = ["pixel_id", "valid_time", "latitude", "longitude",
                     error_field, DEPVAR]
    sub = df[required_cols].dropna(subset=[error_field, DEPVAR]).copy()

    # Pivot: rows = hours, columns = pixels
    pivot = sub.pivot_table(
        index="valid_time", columns="pixel_id", values=error_field,
        aggfunc="first"
    )
    # Drop pixels with >10% missing values
    frac_valid = pivot.notna().mean()
    good_pixels = frac_valid[frac_valid > 0.90].index
    pivot = pivot[good_pixels].copy()

    # Drop hours with any missing pixel
    pivot = pivot.dropna(axis=0)

    # Get matching Y values
    hourly_y = sub.groupby("valid_time")[DEPVAR].first()
    common_times = pivot.index.intersection(hourly_y.index)
    pivot = pivot.loc[common_times]
    Y = hourly_y.loc[common_times].values

    # Pixel coordinates — drop any with NaN lat/lon
    coord_map = (
        sub[["pixel_id", "latitude", "longitude"]]
        .dropna(subset=["latitude", "longitude"])
        .drop_duplicates("pixel_id")
        .set_index("pixel_id")
    )
    # Only keep pixels that have valid coordinates
    valid_pixel_ids = pivot.columns.intersection(coord_map.index)
    pivot = pivot[valid_pixel_ids]
    pixel_ids = valid_pixel_ids.values
    pixel_coords = coord_map.loc[pixel_ids, ["latitude", "longitude"]].values

    X = pivot.values  # (T, N_pixels)

    # Standardize X per pixel (across time)
    scaler = StandardScaler()
    X = scaler.fit_transform(X)

    print(f"  Functional data prepared: X {X.shape}, Y {Y.shape}")
    print(f"  Y stats: mean={Y.mean():.1f}, std={Y.std():.1f}, "
          f"min={Y.min():.1f}, max={Y.max():.1f}")
    n_nan = np.isnan(pixel_coords).sum()
    if n_nan > 0:
        print(f"  WARNING: {n_nan} NaN values in pixel_coords!")

    return X, Y, pixel_coords, pixel_ids, pivot.index


def prepare_multi_field_data(df, error_fields=None):
    """Prepare functional data for multiple error fields simultaneously.

    Returns
    -------
    X_dict : dict of {field: ndarray (T, N_pixels)}
    Y : ndarray (T,)
    pixel_coords : ndarray (N_pixels, 2)
    pixel_ids : ndarray
    hour_index : DatetimeIndex
    """
    if error_fields is None:
        error_fields = ERROR_FIELDS

    # Find common hours and pixels across all fields
    required = ["pixel_id", "valid_time", "latitude", "longitude", DEPVAR] + error_fields
    sub = df[required].dropna(subset=[DEPVAR]).copy()

    # Pivot each field
    pivots = {}
    for field in error_fields:
        piv = sub.pivot_table(
            index="valid_time", columns="pixel_id", values=field,
            aggfunc="first"
        )
        # Drop pixels with >10% missing
        good = piv.columns[piv.notna().mean() > 0.90]
        pivots[field] = piv[good]

    # Common pixels and hours across all fields
    common_pixels = pivots[error_fields[0]].columns
    for field in error_fields[1:]:
        common_pixels = common_pixels.intersection(pivots[field].columns)

    for field in error_fields:
        pivots[field] = pivots[field][common_pixels].dropna(axis=0)

    common_times = pivots[error_fields[0]].index
    for field in error_fields[1:]:
        common_times = common_times.intersection(pivots[field].index)

    hourly_y = sub.groupby("valid_time")[DEPVAR].first()
    common_times = common_times.intersection(hourly_y.index)

    X_dict = {}
    for field in error_fields:
        piv = pivots[field].loc[common_times, common_pixels]
        scaler = StandardScaler()
        X_dict[field] = scaler.fit_transform(piv.values)

    Y = hourly_y.loc[common_times].values

    coord_map = (
        sub[["pixel_id", "latitude", "longitude"]]
        .drop_duplicates("pixel_id")
        .set_index("pixel_id")
    )
    pixel_ids = common_pixels.values
    pixel_coords = coord_map.loc[pixel_ids, ["latitude", "longitude"]].values

    print(f"  Multi-field data: {len(error_fields)} fields, "
          f"{len(common_times)} hours, {len(common_pixels)} pixels")

    return X_dict, Y, pixel_coords, pixel_ids, common_times


# ── Step 1: Naive Baseline (Ridge) ───────────────────────────────────────────

def run_naive_baseline(X, Y, n_folds=N_CV_FOLDS):
    """Ridge on the raw pixel-level error field (naive baseline).

    With ~6,500 pixel features, only Ridge (closed-form) is tractable.
    Sparse methods (Lasso/ElasticNet) are deferred to basis-reduced models.
    """
    kf = KFold(n_splits=n_folds, shuffle=True, random_state=RANDOM_STATE)

    print("\n=== Step 1: Naive Baseline (Ridge on raw pixels) ===")
    print("  Fitting RidgeCV...")
    ridge = RidgeCV(alphas=np.logspace(1, 6, 30), cv=kf, scoring="r2")
    ridge.fit(X, Y)
    ridge_r2_cv = cross_val_score(
        RidgeCV(alphas=[ridge.alpha_]), X, Y, cv=kf, scoring="r2"
    )
    print(f"  Ridge: alpha={ridge.alpha_:.2f}, "
          f"CV R²={ridge_r2_cv.mean():.4f} ± {ridge_r2_cv.std():.4f}")

    return {
        "ridge_r2": ridge_r2_cv.mean(),
        "ridge_r2_std": ridge_r2_cv.std(),
        "ridge_coefs": ridge.coef_,
        "ridge_alpha": ridge.alpha_,
        "ridge_r2_folds": ridge_r2_cv,
    }


# ── Step 2: Functional PCA (FPCA) ───────────────────────────────────────────

def run_fpca_analysis(X, Y, pixel_coords, K_values=None, n_folds=N_CV_FOLDS):
    """FPCA-based dimensionality reduction + regression.

    Computes PCA on the spatial covariance of X, projects onto top-K modes,
    then regresses Y on the scores.

    Returns
    -------
    dict with results for each K
    """
    if K_values is None:
        K_values = [5, 10, 20, 50, 100, 200]
    # Cap K at number of features
    K_values = [k for k in K_values if k < min(X.shape)]

    print("\n=== Step 2: Functional PCA (FPCA) ===")

    # Full PCA
    max_k = max(K_values)
    pca = PCA(n_components=max_k, random_state=RANDOM_STATE)
    scores_full = pca.fit_transform(X)  # (T, max_k)
    components = pca.components_         # (max_k, N_pixels)
    explained_var = pca.explained_variance_ratio_

    print(f"  Top-{max_k} components explain "
          f"{explained_var[:max_k].sum()*100:.1f}% variance")

    kf = KFold(n_splits=n_folds, shuffle=True, random_state=RANDOM_STATE)
    results = {}

    for K in K_values:
        theta = scores_full[:, :K]

        ridge = RidgeCV(alphas=np.logspace(-2, 6, 30), cv=kf, scoring="r2")
        ridge.fit(theta, Y)
        r2_cv = cross_val_score(
            RidgeCV(alphas=[ridge.alpha_]), theta, Y, cv=kf, scoring="r2"
        )

        # Coefficient stability
        coef_folds = []
        for train_idx, _ in kf.split(theta):
            r = RidgeCV(alphas=[ridge.alpha_])
            r.fit(theta[train_idx], Y[train_idx])
            coef_folds.append(r.coef_)
        coef_std = np.std(coef_folds, axis=0).mean()

        # Recover spatial coefficient: beta(s) = sum_k beta_k * V_k(s)
        beta_spatial = components[:K].T @ ridge.coef_  # (N_pixels,)

        results[K] = {
            "r2": r2_cv.mean(),
            "r2_std": r2_cv.std(),
            "r2_folds": r2_cv,
            "coef_std": coef_std,
            "explained_var": explained_var[:K].sum(),
            "beta_spatial": beta_spatial,
            "ridge_coefs": ridge.coef_,
            "alpha": ridge.alpha_,
        }
        print(f"  K={K:3d}: expl_var={explained_var[:K].sum():.3f}, "
              f"CV R²={r2_cv.mean():.4f} ± {r2_cv.std():.4f}, "
              f"coef_std={coef_std:.4f}")

    return {
        "components": components,
        "explained_var": explained_var,
        "results_by_K": results,
        "best_K": max(results, key=lambda k: results[k]["r2"]),
    }




# ── Step 4: Spatial Regularization (Graph Total Variation) ───────────────


def _build_neighbor_graph(pixel_coords, threshold=0.15):
    """Build a spatial neighbor adjacency list from pixel coordinates.

    Two pixels are neighbors if their Euclidean distance in (lat, lon) is
    below `threshold` degrees (~0.1° grid spacing).

    Returns
    -------
    neighbors : list of lists — neighbors[i] = [j, k, ...]
    """
    from scipy.spatial import cKDTree
    tree = cKDTree(pixel_coords)
    pairs = tree.query_pairs(r=threshold)
    n = len(pixel_coords)
    neighbors = [[] for _ in range(n)]
    for i, j in pairs:
        neighbors[i].append(j)
        neighbors[j].append(i)
    return neighbors


def run_spatial_regularization(beta_raw, pixel_coords, lambda_smooth=1.0,
                                n_iter=500, lr=0.01):
    """Smooth a raw coefficient surface using graph-based total variation.

    Minimizes: ||beta - beta_raw||^2 + lambda * sum_{i~j} |beta_i - beta_j|

    Uses proximal gradient descent with soft-thresholding on the graph
    differences.

    Parameters
    ----------
    beta_raw : ndarray (N_pixels,)
    pixel_coords : ndarray (N_pixels, 2)
    lambda_smooth : float
    n_iter : int
    lr : float — learning rate

    Returns
    -------
    beta_smooth : ndarray (N_pixels,)
    """
    print("\n=== Step 6: Spatial Regularization ===")

    neighbors = _build_neighbor_graph(pixel_coords)
    beta = beta_raw.copy()
    n = len(beta)

    for iteration in range(n_iter):
        # Gradient of ||beta - beta_raw||^2
        grad = 2 * (beta - beta_raw)

        # Subgradient of TV penalty: sum of sign(beta_i - beta_j) for neighbors
        tv_grad = np.zeros(n)
        for i in range(n):
            for j in neighbors[i]:
                diff = beta[i] - beta[j]
                if abs(diff) > 1e-10:
                    tv_grad[i] += np.sign(diff)

        grad += lambda_smooth * tv_grad
        beta -= lr * grad

    # How much smoothing occurred
    change = np.sqrt(np.mean((beta - beta_raw)**2))
    print(f"  lambda={lambda_smooth}, iter={n_iter}: "
          f"RMSE(smooth - raw)={change:.4f}")

    return beta


# ── Fused Lasso Spatial Regression ───────────────────────────────────────────


def _build_edge_incidence_matrix(pixel_coords, threshold=0.15):
    """Build a sparse signed edge-incidence matrix for the spatial pixel graph.

    Each row represents one edge (i, j) with D[e, i] = +1, D[e, j] = -1.
    Used by the fused lasso solver to encode the spatial fusion penalty
    λ₂ ||D β||₁ = λ₂ Σ_{(i,j)∈E} |βᵢ - βⱼ|.

    Parameters
    ----------
    pixel_coords : ndarray (N, 2) — (lat, lon) per pixel
    threshold : float — max distance (degrees) to call two pixels neighbors

    Returns
    -------
    D : scipy.sparse.csr_matrix, shape (n_edges, N)
    edges : list of (i, j) tuples
    """
    from scipy.spatial import cKDTree
    from scipy import sparse

    tree = cKDTree(pixel_coords)
    pairs = list(tree.query_pairs(r=threshold))
    n = len(pixel_coords)
    m = len(pairs)

    rows = np.repeat(np.arange(m), 2)
    cols = np.array([[i, j] for i, j in pairs]).ravel()
    data = np.tile([1.0, -1.0], m)

    D = sparse.csr_matrix((data, (rows, cols)), shape=(m, n))
    return D, pairs


def _soft_threshold(x, threshold):
    """Proximal operator for the L1 norm (element-wise soft thresholding).

    Parameters
    ----------
    x : ndarray
    threshold : float — must be >= 0

    Returns
    -------
    ndarray, same shape as x
    """
    return np.sign(x) * np.maximum(np.abs(x) - threshold, 0.0)


def _flsa_admm(beta_raw, D, lambda1, lambda2,
               rho=1.0, max_iter=600, tol=1e-5):
    """ADMM solver for the Fused Lasso Signal Approximator (FLSA).

    Solves the convex problem:
        min_{γ}  ½ ||β_raw - γ||²  +  λ₁ ||γ||₁  +  λ₂ ||D γ||₁

    where D is the edge-incidence matrix of the spatial neighbor graph.
    The L1 penalty on γ drives pixel coefficients toward zero (sparsity)
    and the L1 penalty on differences across edges creates piecewise-constant
    spatial regions (fusion).

    ADMM splitting: introduce z₁ = γ (for L1 on γ) and z₂ = D γ (for TV).

    Parameters
    ----------
    beta_raw : ndarray (N,) — noisy spatial coefficient map to denoise
    D : sparse matrix (M, N) — edge-incidence matrix
    lambda1 : float — L1 sparsity weight
    lambda2 : float — spatial fusion (total-variation) weight
    rho : float — ADMM penalty parameter
    max_iter : int
    tol : float — primal residual convergence threshold

    Returns
    -------
    gamma : ndarray (N,) — denoised piecewise-constant coefficient map
    n_iter : int — iterations until convergence
    """
    from scipy import sparse
    from scipy.sparse.linalg import factorized

    n = beta_raw.shape[0]
    I_n = sparse.eye(n, format="csc")
    DTD = (D.T @ D).tocsc()

    # A is constant — factorize once and reuse across all iterations
    A = I_n * (1.0 + rho) + rho * DTD
    A_solve = factorized(A)

    z1 = np.zeros(n)
    z2 = np.zeros(D.shape[0])
    u1 = np.zeros(n)
    u2 = np.zeros(D.shape[0])

    for it in range(max_iter):
        rhs = beta_raw + rho * (z1 - u1) + rho * (D.T @ (z2 - u2))
        gamma = A_solve(rhs)

        d_gamma = D @ gamma  # compute once; used three times below
        z1_new = _soft_threshold(gamma + u1, lambda1 / rho)
        z2_new = _soft_threshold(d_gamma + u2, lambda2 / rho)

        u1 = u1 + gamma - z1_new
        u2 = u2 + d_gamma - z2_new

        r_primal = (np.linalg.norm(gamma - z1_new)
                    + np.linalg.norm(d_gamma - z2_new))
        z1, z2 = z1_new, z2_new

        # Skip early stopping in the first 10 iterations (warm-up)
        if r_primal < tol and it > 10:
            break

    return gamma, it + 1


def run_fused_lasso(X, Y, pixel_coords, pixel_ids,
                    K_fpca=100,
                    lambda1_values=None,
                    lambda2_values=None,
                    n_folds=N_CV_FOLDS,
                    rho=1.0,
                    save_dir=None):
    """Two-stage spatial fused lasso for identifying important forecast-error regions.

    **Stage 1 — FPCA ridge regression**: Reduces the T × N pixel design matrix
    to T × K FPCA scores, fits a Ridge regressor, and recovers the spatial
    coefficient map β̂(s) ∈ R^N.

    **Stage 2 — Fused Lasso Signal Approximator (FLSA)**: Applies the fused
    lasso to β̂(s), producing γ̂(s) that is simultaneously sparse (many pixels
    exactly zero) and piecewise-constant over spatially contiguous regions.
    This directly identifies *which geographic areas* drive forecast-error
    sensitivity in curtailment outcomes.

    Cross-validation picks the best (λ₁, λ₂) pair by splitting on time: for
    each fold the full two-stage pipeline (FPCA → Ridge → FLSA) is refit on
    the training split and evaluated on the held-out split.

    Parameters
    ----------
    X : ndarray (T, N) — standardized pixel error field
    Y : ndarray (T,) — outcome (e.g. total curtailment MW)
    pixel_coords : ndarray (N, 2) — (lat, lon) per pixel
    pixel_ids : ndarray (N,) — pixel_id strings
    K_fpca : int — number of FPCA components for Stage 1
    lambda1_values : list of float — L1 sparsity grid (default: 5 log-spaced)
    lambda2_values : list of float — fusion penalty grid (default: 5 log-spaced)
    n_folds : int — time-blocked CV folds
    rho : float — ADMM step size for FLSA
    save_dir : Path or None — directory for saved figures

    Returns
    -------
    dict with keys:
        beta_raw : ndarray (N,) — FPCA ridge coefficient surface (no fused lasso)
        beta_fused : ndarray (N,) — best fused-lasso-regularized surface
        lambda1_best, lambda2_best : floats — selected penalty values
        cv_r2_grid : ndarray — R² on grid of (lambda1, lambda2)
        cv_r2_best : float — best held-out R²
        r2_raw : float — held-out R² from raw Ridge (no fused lasso)
        zero_fraction : float — fraction of pixels exactly zero in beta_fused
        n_regions : int — connected components of nonzero pixels
        results_grid : dict — full grid results
    """
    from sklearn.decomposition import PCA
    from sklearn.linear_model import RidgeCV

    if lambda1_values is None:
        lambda1_values = [0.0, 0.01, 0.05, 0.2, 1.0]
    if lambda2_values is None:
        lambda2_values = [0.0, 0.01, 0.05, 0.2, 1.0]

    print("\n=== Fused Lasso Spatial Regression ===")
    K_fpca = min(K_fpca, min(X.shape) - 1)

    # Build spatial graph once (shared across all CV folds and λ grid)
    print("  Building spatial neighbor graph...")
    D, edges = _build_edge_incidence_matrix(pixel_coords, threshold=0.15)
    print(f"  Graph: {len(pixel_coords)} pixels, {len(edges)} edges")

    # ── Stage 1 helper: FPCA ridge → beta_spatial ─────────────────────────
    def _fit_stage1(X_tr, Y_tr):
        pca = PCA(n_components=K_fpca, random_state=RANDOM_STATE)
        Theta_tr = pca.fit_transform(X_tr)
        ridge = RidgeCV(alphas=np.logspace(0, 6, 20), cv=3, scoring="r2")
        ridge.fit(Theta_tr, Y_tr)
        beta = pca.components_.T @ ridge.coef_
        return pca, ridge, beta

    # ── Fit on full data for the final coefficient surface ─────────────────
    pca_full, ridge_full, beta_raw = _fit_stage1(X, Y)

    # Variance explained
    print(f"  FPCA K={K_fpca}: explains "
          f"{pca_full.explained_variance_ratio_[:K_fpca].sum()*100:.1f}% variance")
    print(f"  Ridge α={ridge_full.alpha_:.2f}")
    print(f"  beta_raw: min={beta_raw.min():.4f}, max={beta_raw.max():.4f}, "
          f"std={beta_raw.std():.4f}")

    # ── Cross-validation over (λ₁, λ₂) grid ──────────────────────────────
    T = X.shape[0]
    fold_size = T // n_folds
    # Time-blocked folds (avoid leakage between adjacent hours)
    fold_indices = [
        (
            np.concatenate([np.arange(0, f * fold_size),
                            np.arange((f + 1) * fold_size, T)]),
            np.arange(f * fold_size, (f + 1) * fold_size),
        )
        for f in range(n_folds)
    ]

    l1_grid = lambda1_values
    l2_grid = lambda2_values
    cv_r2_grid = np.zeros((len(l1_grid), len(l2_grid)))
    cv_r2_raw = 0.0

    print(f"\n  Cross-validating {len(l1_grid)}×{len(l2_grid)} λ grid "
          f"with {n_folds} time-blocked folds...")

    for fi, (train_idx, test_idx) in enumerate(fold_indices):
        X_tr, Y_tr = X[train_idx], Y[train_idx]
        X_te, Y_te = X[test_idx], Y[test_idx]

        _, _, beta_fold = _fit_stage1(X_tr, Y_tr)
        Y_pred_raw = X_te @ beta_fold
        cv_r2_raw += r2_score(Y_te, Y_pred_raw) / n_folds

        for li, l1 in enumerate(l1_grid):
            for lj, l2 in enumerate(l2_grid):
                if l1 == 0.0 and l2 == 0.0:
                    beta_fused_fold = beta_fold
                else:
                    beta_fused_fold, _ = _flsa_admm(
                        beta_fold, D, l1, l2, rho=rho
                    )
                Y_pred = X_te @ beta_fused_fold
                cv_r2_grid[li, lj] += r2_score(Y_te, Y_pred) / n_folds

        print(f"    Fold {fi+1}/{n_folds} complete")

    best_idx = np.unravel_index(np.argmax(cv_r2_grid), cv_r2_grid.shape)
    lambda1_best = l1_grid[best_idx[0]]
    lambda2_best = l2_grid[best_idx[1]]
    cv_r2_best = cv_r2_grid[best_idx]

    print(f"\n  Best λ₁={lambda1_best}, λ₂={lambda2_best} → "
          f"CV R²={cv_r2_best:.4f}  (raw Ridge CV R²={cv_r2_raw:.4f})")

    # ── Apply best FLSA to full-data beta_raw ─────────────────────────────
    if lambda1_best == 0.0 and lambda2_best == 0.0:
        beta_fused = beta_raw.copy()
        n_iters = 0
    else:
        beta_fused, n_iters = _flsa_admm(
            beta_raw, D, lambda1_best, lambda2_best, rho=rho
        )
    print(f"  FLSA converged in {n_iters} iterations")

    # Sparsity statistics
    zero_fraction = np.mean(np.abs(beta_fused) < 1e-8)
    nonzero_mask = np.abs(beta_fused) >= 1e-8
    n_nonzero = nonzero_mask.sum()

    # Count connected components of nonzero pixels using the already-built graph
    from scipy import sparse as _sp
    from scipy.sparse.csgraph import connected_components
    n_pix = len(beta_fused)
    if edges:
        row_idx = [i for i, j in edges] + [j for i, j in edges]
        col_idx = [j for i, j in edges] + [i for i, j in edges]
        adj = _sp.csr_matrix(
            (np.ones(2 * len(edges)), (row_idx, col_idx)), shape=(n_pix, n_pix)
        )
    else:
        adj = _sp.csr_matrix((n_pix, n_pix))
    nz_idx = np.where(nonzero_mask)[0]
    subadj = adj[nz_idx][:, nz_idx]
    n_regions = int(connected_components(subadj, directed=False)[0]) if n_nonzero else 0

    print(f"  beta_fused: zero_fraction={zero_fraction:.3f} "
          f"({n_nonzero} nonzero pixels), {n_regions} contiguous region(s)")

    results = {
        "beta_raw": beta_raw,
        "beta_fused": beta_fused,
        "lambda1_best": lambda1_best,
        "lambda2_best": lambda2_best,
        "cv_r2_grid": cv_r2_grid,
        "cv_r2_best": cv_r2_best,
        "r2_raw": cv_r2_raw,
        "zero_fraction": zero_fraction,
        "n_regions": n_regions,
        "lambda1_values": l1_grid,
        "lambda2_values": l2_grid,
        "pca": pca_full,
        "ridge": ridge_full,
    }

    if save_dir is not None:
        plot_fused_lasso_results(results, pixel_coords, save_dir)

    return results


def plot_fused_lasso_results(results, pixel_coords, save_dir):
    """Save a 3-panel figure summarizing the fused lasso spatial analysis.

    Panel 1 — Raw FPCA-ridge β map (before fused lasso).
    Panel 2 — Fused lasso γ map (piecewise-constant regions).
    Panel 3 — CV R² heatmap over the (λ₁, λ₂) grid.

    Parameters
    ----------
    results : dict — output of run_fused_lasso()
    pixel_coords : ndarray (N, 2)
    save_dir : Path
    """
    save_dir = Path(save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)

    beta_raw = results["beta_raw"]
    beta_fused = results["beta_fused"]
    cv_grid = results["cv_r2_grid"]
    l1_vals = results["lambda1_values"]
    l2_vals = results["lambda2_values"]
    l1_best = results["lambda1_best"]
    l2_best = results["lambda2_best"]

    vmax = max(np.abs(beta_raw).max(), np.abs(beta_fused).max())
    vmin = -vmax

    # Create 2 cartopy map axes and 1 plain matplotlib axis directly —
    # avoids the remove/re-add dance that mixed subplot_kw would require.
    fig = plt.figure(figsize=(18, 5))
    ax1 = fig.add_subplot(1, 3, 1, projection=ccrs.PlateCarree())
    ax2 = fig.add_subplot(1, 3, 2, projection=ccrs.PlateCarree())
    ax_heat = fig.add_subplot(1, 3, 3)

    for ax, beta, title in [
        (ax1, beta_raw,
         f"FPCA Ridge β (before fused lasso)\nCV R²={results['r2_raw']:.3f}"),
        (ax2, beta_fused,
         f"Fused Lasso γ  (λ₁={l1_best}, λ₂={l2_best})\n"
         f"CV R²={results['cv_r2_best']:.3f}  |  "
         f"zeros={results['zero_fraction']*100:.0f}%  |  "
         f"{results['n_regions']} region(s)"),
    ]:
        _draw_texas_base(ax)
        sc = ax.scatter(
            pixel_coords[:, 1], pixel_coords[:, 0],
            c=beta, cmap="RdBu_r", vmin=vmin, vmax=vmax,
            s=18, transform=ccrs.PlateCarree(), linewidths=0,
        )
        plt.colorbar(sc, ax=ax, orientation="horizontal", pad=0.04,
                     label="β coefficient")
        ax.set_title(title, fontsize=9)

    im = ax_heat.imshow(
        cv_grid, aspect="auto", origin="lower",
        cmap="viridis",
        extent=[-0.5, len(l2_vals) - 0.5, -0.5, len(l1_vals) - 0.5],
    )
    ax_heat.set_xticks(range(len(l2_vals)))
    ax_heat.set_xticklabels([str(v) for v in l2_vals], fontsize=8)
    ax_heat.set_yticks(range(len(l1_vals)))
    ax_heat.set_yticklabels([str(v) for v in l1_vals], fontsize=8)
    ax_heat.set_xlabel("λ₂ (fusion / TV)")
    ax_heat.set_ylabel("λ₁ (L1 sparsity)")
    ax_heat.set_title(f"CV R² grid\nbest: λ₁={l1_best}, λ₂={l2_best}")
    l1_arr, l2_arr = np.array(l1_vals), np.array(l2_vals)
    best_i = int(np.argmin(np.abs(l1_arr - l1_best)))
    best_j = int(np.argmin(np.abs(l2_arr - l2_best)))
    ax_heat.add_patch(plt.Rectangle(
        (best_j - 0.5, best_i - 0.5), 1, 1,
        fill=False, edgecolor="red", linewidth=2,
    ))
    plt.colorbar(im, ax=ax_heat, label="CV R²")

    fig.suptitle(
        "Spatial Fused Lasso — ERCOT Forecast Error → Curtailment",
        fontsize=11, y=1.01,
    )
    plt.tight_layout()
    out = save_dir / "fused_lasso_results.png"
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Fused lasso figure saved → {out}")


# ── Bootstrap Significance Testing ───────────────────────────────────────────

def bootstrap_pixel_pvalues(X, Y, fit_fn, B=100, random_state=RANDOM_STATE):
    """Compute per-pixel two-sided p-values via bootstrap.

    Standard OLS inference doesn't apply to Ridge, PCA-Ridge, or PLS because
    these estimators are biased / use data-adaptive bases.  Bootstrap is the
    principled alternative: resample (hour, outcome) rows with replacement,
    refit the full model pipeline, record beta_spatial for each replicate, and
    use the bootstrap SE to form a z-statistic.

    Parameters
    ----------
    X : ndarray (T, N_pixels)
    Y : ndarray (T,)
    fit_fn : callable(X_boot, Y_boot) -> ndarray (N_pixels,)
        Should fit the complete model on the bootstrap sample and return the
        recovered spatial coefficient vector.
    B : int
        Number of bootstrap replicates (100 gives stable SEs at 5% threshold).
    random_state : int

    Returns
    -------
    p_values : ndarray (N_pixels,)
        Two-sided p-values; pixels where p < 0.05 are significant.
    """
    from scipy import stats as scipy_stats

    rng = np.random.default_rng(random_state)
    T = X.shape[0]
    boot_betas = []

    for b in range(B):
        idx = rng.integers(0, T, size=T)
        try:
            beta_b = fit_fn(X[idx], Y[idx])
            boot_betas.append(beta_b)
        except Exception:
            continue  # skip failed replicates (e.g. rank-deficient bootstrap)

    if len(boot_betas) < 10:
        # Not enough replicates — return all non-significant
        return np.ones(X.shape[1])

    boot_arr = np.stack(boot_betas, axis=0)   # (B_eff, N_pixels)
    boot_se = boot_arr.std(axis=0)             # (N_pixels,)

    # Observed beta from full-data fit
    beta_obs = fit_fn(X, Y)

    # z-statistic: observed / bootstrap SE
    z = np.where(boot_se > 1e-10, beta_obs / boot_se, 0.0)

    # Two-sided p-value from standard normal approximation
    p_values = 2.0 * scipy_stats.norm.sf(np.abs(z))
    return p_values


# ── Step 7: Model Comparison & Visualization ─────────────────────────────────

def _draw_texas_base(ax):
    """Draw Texas state outline on a cartopy axes."""
    proj = ccrs.PlateCarree()
    ax.set_extent([-107.5, -93.0, 25.5, 37.0], crs=proj)
    ax.set_facecolor("#cce5f0")

    shp = shpreader.natural_earth(
        resolution="10m", category="cultural", name="admin_1_states_provinces"
    )
    for rec in shpreader.Reader(shp).records():
        name = rec.attributes.get("name")
        admin = rec.attributes.get("admin")
        if name == "Texas":
            ax.add_geometries([rec.geometry], crs=proj,
                              facecolor="white", edgecolor="black",
                              linewidth=0.8, zorder=2)
        elif admin == "United States of America":
            ax.add_geometries([rec.geometry], crs=proj,
                              facecolor="#f0f0f0", edgecolor="#aaa",
                              linewidth=0.3, zorder=1)


def plot_beta_surface(beta, pixel_coords, title, ax, vmin=None, vmax=None,
                      sig_mask=None):
    """Plot a coefficient surface beta(s) on a Texas map.

    Parameters
    ----------
    sig_mask : ndarray of bool, optional
        If provided, only pixels where sig_mask is True are plotted
        (significant at the chosen threshold).

    Returns the scatter artist for colorbar attachment.
    """
    _draw_texas_base(ax)

    if vmin is None or vmax is None:
        clim = np.nanpercentile(np.abs(beta), 98)
        vmin, vmax = -clim, clim

    if sig_mask is not None:
        plot_beta = beta[sig_mask]
        plot_coords = pixel_coords[sig_mask]
        n_sig = sig_mask.sum()
        n_total = len(sig_mask)
        ax.set_title(f"{title}\n(n={n_sig}/{n_total} sig. pixels)", fontsize=9)
    else:
        plot_beta = beta
        plot_coords = pixel_coords
        ax.set_title(title, fontsize=10)

    if len(plot_beta) == 0:
        return ax.scatter([], [], c=[], cmap="RdBu_r", vmin=vmin, vmax=vmax,
                          transform=ccrs.PlateCarree())

    sc = ax.scatter(
        plot_coords[:, 1], plot_coords[:, 0],
        c=plot_beta, cmap="RdBu_r", vmin=vmin, vmax=vmax,
        s=4, marker="s", transform=ccrs.PlateCarree(), zorder=3, alpha=0.9,
    )
    return sc


def plot_model_comparison(results_summary, save_path):
    """Bar chart comparing CV R² across all methods."""
    fig, ax = plt.subplots(figsize=(12, 6))

    names = list(results_summary.keys())
    r2s = [results_summary[n]["r2"] for n in names]
    stds = [results_summary[n]["r2_std"] for n in names]

    colors = plt.cm.Set2(np.linspace(0, 1, len(names)))
    bars = ax.bar(range(len(names)), r2s, yerr=stds, capsize=4,
                  color=colors, edgecolor="black", linewidth=0.5)

    ax.set_xticks(range(len(names)))
    ax.set_xticklabels(names, rotation=35, ha="right", fontsize=9)
    ax.set_ylabel("Out-of-Sample R² (5-fold CV)", fontsize=11)
    ax.set_title("Model Comparison: Spatial Functional Regression Methods",
                 fontsize=13)
    ax.axhline(0, color="black", linewidth=0.5)

    for bar, r2, std in zip(bars, r2s, stds):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + std + 0.002,
                f"{r2:.4f}", ha="center", va="bottom", fontsize=8)

    ax.set_ylim(min(0, min(r2s) - 0.05), max(r2s) + max(stds) + 0.03)
    fig.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Comparison chart saved: {save_path}")


def plot_beta_comparison(betas_dict, pixel_coords, save_path, error_label="",
                          r2_dict=None, sig_masks_dict=None):
    """Multi-panel beta surface comparison.

    Parameters
    ----------
    r2_dict : dict, optional
        {method_name: r2_value} — R² is appended to each panel title.
    sig_masks_dict : dict, optional
        {method_name: bool_array} — only significant pixels are plotted.
    """
    methods = list(betas_dict.keys())
    n = len(methods)
    ncols = min(3, n)
    nrows = (n + ncols - 1) // ncols

    fig, axes = plt.subplots(
        nrows, ncols, figsize=(6 * ncols, 5 * nrows),
        subplot_kw={"projection": ccrs.PlateCarree()},
    )
    if nrows == 1 and ncols == 1:
        axes = np.array([[axes]])
    elif nrows == 1:
        axes = axes[np.newaxis, :]
    elif ncols == 1:
        axes = axes[:, np.newaxis]

    # Shared color limits
    all_betas = np.concatenate([betas_dict[m] for m in methods])
    clim = np.nanpercentile(np.abs(all_betas), 98)

    sc_last = None
    for idx, method in enumerate(methods):
        r, c = divmod(idx, ncols)
        ax = axes[r, c]
        title = method
        if r2_dict and method in r2_dict:
            title += f"  (R²={r2_dict[method]:.3f})"
        sig_mask = sig_masks_dict.get(method) if sig_masks_dict else None
        sc = plot_beta_surface(
            betas_dict[method], pixel_coords, title, ax,
            vmin=-clim, vmax=clim, sig_mask=sig_mask,
        )
        sc_last = sc

    # Hide unused axes
    for idx in range(n, nrows * ncols):
        r, c = divmod(idx, ncols)
        axes[r, c].set_visible(False)

    if sc_last is not None:
        fig.colorbar(
            sc_last, ax=axes.ravel().tolist(), shrink=0.6,
            label=f"β(s): Effect on {DEPVAR}",
            pad=0.02,
        )

    title = "Recovered Spatial Coefficient Surfaces β(s)"
    if error_label:
        title += f"\n({error_label})"
    if sig_masks_dict:
        title += "  [p < 0.05 only]"
    fig.suptitle(title, fontsize=13, y=1.02)
    fig.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Beta comparison saved: {save_path}")


def plot_explained_variance(explained_var, save_path):
    """Plot cumulative explained variance from FPCA."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))

    n = len(explained_var)
    ax1.bar(range(1, n+1), explained_var, color="steelblue", alpha=0.7)
    ax1.set_xlabel("Component")
    ax1.set_ylabel("Explained Variance Ratio")
    ax1.set_title("Individual Component Variance")

    cum_var = np.cumsum(explained_var)
    ax2.plot(range(1, n+1), cum_var, "o-", color="steelblue", markersize=3)
    ax2.axhline(0.90, color="red", linestyle="--", alpha=0.5, label="90%")
    ax2.axhline(0.95, color="orange", linestyle="--", alpha=0.5, label="95%")
    ax2.set_xlabel("Number of Components")
    ax2.set_ylabel("Cumulative Explained Variance")
    ax2.set_title("Cumulative Explained Variance")
    ax2.legend()

    fig.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Explained variance plot saved: {save_path}")


def plot_fpca_modes(components, pixel_coords, save_path, n_modes=6):
    """Plot the top spatial PCA modes."""
    n_modes = min(n_modes, components.shape[0])
    ncols = 3
    nrows = (n_modes + ncols - 1) // ncols

    fig, axes = plt.subplots(
        nrows, ncols, figsize=(6 * ncols, 5 * nrows),
        subplot_kw={"projection": ccrs.PlateCarree()},
    )
    if nrows == 1:
        axes = axes[np.newaxis, :]

    for i in range(n_modes):
        r, c = divmod(i, ncols)
        ax = axes[r, c]
        mode = components[i]
        clim = np.nanpercentile(np.abs(mode), 98)
        plot_beta_surface(mode, pixel_coords, f"Mode {i+1}", ax,
                          vmin=-clim, vmax=clim)

    for i in range(n_modes, nrows * ncols):
        r, c = divmod(i, ncols)
        axes[r, c].set_visible(False)

    fig.suptitle("Top Spatial PCA Modes (Eigenfunctions)", fontsize=13, y=1.02)
    fig.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  PCA modes saved: {save_path}")


def run_counterfactual_analysis(X, Y, pixel_coords, pixel_ids, model_coefs,
                                 basis_transform_fn, region_name="West Texas Wind"):
    """Zero out forecast errors in a region, measure predicted Y change.

    Parameters
    ----------
    model_coefs : dict with model coefficient info
    basis_transform_fn : callable — X -> theta
    region_name : str

    Returns
    -------
    dict with counterfactual results
    """
    print(f"\n  Counterfactual: zeroing {region_name}")

    # Define West Texas wind region (roughly Permian Basin / Panhandle)
    lat_mask = (pixel_coords[:, 0] > 31.0) & (pixel_coords[:, 0] < 35.5)
    lon_mask = (pixel_coords[:, 1] > -104.0) & (pixel_coords[:, 1] < -99.0)
    region_mask = lat_mask & lon_mask
    n_region = region_mask.sum()
    print(f"    {n_region} pixels in region")

    if n_region == 0:
        return {"delta_Y_mean": 0, "delta_Y_std": 0, "n_region": 0}

    # Zero out region
    X_cf = X.copy()
    X_cf[:, region_mask] = 0

    theta_orig = basis_transform_fn(X)
    theta_cf = basis_transform_fn(X_cf)

    Y_pred_orig = theta_orig @ model_coefs
    Y_pred_cf = theta_cf @ model_coefs

    delta_Y = Y_pred_cf - Y_pred_orig
    print(f"    Mean ΔY: {delta_Y.mean():.2f} MW, "
          f"Std ΔY: {delta_Y.std():.2f} MW")

    return {
        "delta_Y_mean": delta_Y.mean(),
        "delta_Y_std": delta_Y.std(),
        "n_region": n_region,
        "region_mask": region_mask,
    }


def compile_results_table(all_results):
    """Build a summary DataFrame of all model results.

    Returns
    -------
    pd.DataFrame with columns: method, r2, r2_std, coef_std, n_features
    """
    rows = []
    for name, res in all_results.items():
        rows.append({
            "method": name,
            "r2": res["r2"],
            "r2_std": res["r2_std"],
            "coef_std": res.get("coef_std", np.nan),
            "n_features": res.get("n_features", np.nan),
        })
    return pd.DataFrame(rows).sort_values("r2", ascending=False)


# ── Main Orchestrator ────────────────────────────────────────────────────────

def run_full_analysis(months=None, primary_field="wspd_error_1h"):
    """Run the complete spatial functional regression pipeline.

    Parameters
    ----------
    months : list of (year, month) tuples
    primary_field : str — which error field to use for single-field analyses

    Returns
    -------
    dict with all results, figures, and tables
    """
    if months is None:
        months = DEFAULT_MONTHS

    dirs = setup_directories()
    fig_dir = Path(dirs["figures"]) / "functional_analysis"
    fig_dir.mkdir(parents=True, exist_ok=True)
    tables_dir = Path(dirs["tables"])
    tables_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("SPATIAL FUNCTIONAL REGRESSION ANALYSIS")
    print("=" * 70)

    # ── Step 0: Load and prepare data ──
    print("\n=== Step 0: Data Loading ===")
    df = load_pixel_data(months)

    # Single-field data for primary analyses
    X, Y, pixel_coords, pixel_ids, hour_index = prepare_functional_data(
        df, error_field=primary_field
    )

    all_results = {}
    all_betas = {}

    # ── Step 1: Naive Baseline ──
    baseline = run_naive_baseline(X, Y)
    all_results["Ridge (raw)"] = {
        "r2": baseline["ridge_r2"],
        "r2_std": baseline["ridge_r2_std"],
        "coef_std": np.nan,
        "n_features": X.shape[1],
    }
    all_betas["Ridge (raw)"] = baseline["ridge_coefs"]

    # ── Step 2: FPCA ──
    fpca_res = run_fpca_analysis(X, Y, pixel_coords)
    best_K = fpca_res["best_K"]
    all_results[f"FPCA K={best_K}"] = {
        "r2": fpca_res["results_by_K"][best_K]["r2"],
        "r2_std": fpca_res["results_by_K"][best_K]["r2_std"],
        "coef_std": fpca_res["results_by_K"][best_K]["coef_std"],
        "n_features": best_K,
    }
    all_betas[f"FPCA K={best_K}"] = fpca_res["results_by_K"][best_K]["beta_spatial"]

    # Also add other K values for comparison
    for K, kres in fpca_res["results_by_K"].items():
        if K != best_K:
            all_results[f"FPCA K={K}"] = {
                "r2": kres["r2"],
                "r2_std": kres["r2_std"],
                "coef_std": kres["coef_std"],
                "n_features": K,
            }

    # Visualizations
    plot_explained_variance(fpca_res["explained_var"],
                            fig_dir / "fpca_explained_variance.png")
    plot_fpca_modes(fpca_res["components"], pixel_coords,
                    fig_dir / "fpca_spatial_modes.png")

    # ── Step 4: Spatial Regularization ──
    # Apply to the FPCA model's beta surface
    best_method = max(
        {k: v for k, v in all_results.items()
         if k in all_betas},
        key=lambda k: all_results[k]["r2"]
    )
    beta_raw = all_betas[best_method]
    print(f"\nApplying spatial regularization to best method: {best_method}")

    for lam in [0.1, 1.0, 5.0]:
        beta_smooth = run_spatial_regularization(
            beta_raw, pixel_coords, lambda_smooth=lam
        )
        all_betas[f"Smoothed (λ={lam})"] = beta_smooth

    # ── Step 5: Compile and visualize ──
    print("\n=== Step 7: Model Comparison ===")

    # Summary table
    summary_df = compile_results_table(all_results)
    table_path = tables_dir / "functional_analysis_comparison.csv"
    summary_df.to_csv(table_path, index=False)
    print(f"\n  Summary table saved: {table_path}")
    print(summary_df.to_string(index=False))

    # Comparison bar chart — top models only
    top_models = {}
    # Pick best from each category
    categories = {
        "Ridge (raw)": ["Ridge (raw)"],
        f"FPCA K={best_K}": [f"FPCA K={best_K}"],
        "PLS (best)": [f"PLS n={n}" for n in [5, 10, 20, 50, 100, 200]],
    }
    for label, keys in categories.items():
        for k in keys:
            if k in all_results:
                top_models[label] = all_results[k]
                break

    plot_model_comparison(top_models, fig_dir / "model_comparison.png")

    # ── Bootstrap significance for Ridge and FPCA ──
    print("\n=== Bootstrap Significance (B=100) ===")
    ridge_alpha = baseline["ridge_alpha"]
    def _ridge_fit(Xb, Yb):
        r = RidgeCV(alphas=[ridge_alpha], cv=3)
        r.fit(Xb, Yb)
        return r.coef_

    K_sig = best_K
    def _fpca_fit(Xb, Yb):
        K_use = min(K_sig, Xb.shape[0] - 1, Xb.shape[1] - 1)
        pca = PCA(n_components=K_use, random_state=RANDOM_STATE)
        scores = pca.fit_transform(Xb)
        ridge = RidgeCV(alphas=np.logspace(1, 6, 10), cv=3)
        ridge.fit(scores, Yb)
        return pca.components_[:K_use].T @ ridge.coef_

    print("  Ridge bootstrap...")
    ridge_pvals = bootstrap_pixel_pvalues(X, Y, _ridge_fit, B=100)
    print(f"  Ridge: {(ridge_pvals < 0.05).sum()} / {len(ridge_pvals)} pixels significant")

    print("  FPCA bootstrap...")
    fpca_pvals = bootstrap_pixel_pvalues(X, Y, _fpca_fit, B=100)
    print(f"  FPCA: {(fpca_pvals < 0.05).sum()} / {len(fpca_pvals)} pixels significant")

    sig_masks = {
        "Ridge (raw)": ridge_pvals < 0.05,
        f"FPCA K={best_K}": fpca_pvals < 0.05,
    }
    r2_dict_main = {
        "Ridge (raw)": baseline["ridge_r2"],
        f"FPCA K={best_K}": fpca_res["results_by_K"][best_K]["r2"],
    }

    # Beta surface comparison — Ridge vs FPCA
    beta_comparison = {}
    for key in ["Ridge (raw)", f"FPCA K={best_K}"]:
        if key in all_betas:
            beta_comparison[key] = all_betas[key]

    if beta_comparison:
        plot_beta_comparison(
            beta_comparison, pixel_coords,
            fig_dir / f"beta_comparison_{primary_field}.png",
            error_label=primary_field,
            r2_dict=r2_dict_main,
            sig_masks_dict=sig_masks,
        )

    # Smoothed vs raw comparison
    smooth_betas = {k: v for k, v in all_betas.items()
                    if "Smoothed" in k or k == best_method}
    if smooth_betas:
        plot_beta_comparison(
            smooth_betas, pixel_coords,
            fig_dir / f"beta_smoothed_{primary_field}.png",
            error_label=f"{primary_field} (spatial regularization)",
        )

    # ── Counterfactual: West Texas wind region ──
    print("\n=== Counterfactual Analysis ===")
    # Use FPCA model for counterfactual
    fpca_best = fpca_res["results_by_K"][best_K]
    pca_for_cf = PCA(n_components=best_K, random_state=RANDOM_STATE)
    pca_for_cf.fit(X)
    cf_results = run_counterfactual_analysis(
        X, Y, pixel_coords, pixel_ids,
        model_coefs=fpca_best["ridge_coefs"],
        basis_transform_fn=lambda Xin: pca_for_cf.transform(Xin),
        region_name="West Texas Wind Belt",
    )

    # Also try South Texas
    lat_mask_south = (pixel_coords[:, 0] > 26.0) & (pixel_coords[:, 0] < 29.0)
    lon_mask_south = (pixel_coords[:, 1] > -100.0) & (pixel_coords[:, 1] < -96.5)
    south_mask = lat_mask_south & lon_mask_south

    X_cf_south = X.copy()
    X_cf_south[:, south_mask] = 0
    theta_orig = pca_for_cf.transform(X)
    theta_cf_south = pca_for_cf.transform(X_cf_south)
    delta_south = (theta_cf_south - theta_orig) @ fpca_best["ridge_coefs"]
    print(f"  South TX counterfactual: ΔY mean={delta_south.mean():.2f} MW")

    # ── FPCA K selection plot ──
    fpca_k_values = sorted(fpca_res["results_by_K"].keys())
    fpca_r2s = [fpca_res["results_by_K"][k]["r2"] for k in fpca_k_values]
    fpca_vars = [fpca_res["results_by_K"][k]["explained_var"] for k in fpca_k_values]

    fig, ax1 = plt.subplots(figsize=(8, 5))
    color1 = "steelblue"
    ax1.plot(fpca_k_values, fpca_r2s, "o-", color=color1, label="CV R²")
    ax1.set_xlabel("Number of PCA Components (K)")
    ax1.set_ylabel("CV R²", color=color1)
    ax1.tick_params(axis="y", labelcolor=color1)

    ax2 = ax1.twinx()
    color2 = "coral"
    ax2.plot(fpca_k_values, fpca_vars, "s--", color=color2, label="Explained Var")
    ax2.set_ylabel("Cumulative Explained Variance", color=color2)
    ax2.tick_params(axis="y", labelcolor=color2)

    ax1.set_title("FPCA: Predictive Power vs Dimensionality")
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="center right")

    fig.tight_layout()
    fig.savefig(fig_dir / "fpca_k_selection.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

    # ── Save all numerical results as JSON ──
    json_results = {}
    for name, res in all_results.items():
        json_results[name] = {
            "r2": float(res["r2"]),
            "r2_std": float(res["r2_std"]),
            "coef_std": float(res["coef_std"]) if not np.isnan(res.get("coef_std", np.nan)) else None,
            "n_features": int(res["n_features"]) if not np.isnan(res.get("n_features", np.nan)) else None,
        }
    json_results["counterfactual_west_tx"] = {
        "delta_Y_mean": float(cf_results["delta_Y_mean"]),
        "delta_Y_std": float(cf_results["delta_Y_std"]),
        "n_region_pixels": int(cf_results["n_region"]),
    }
    json_results["counterfactual_south_tx"] = {
        "delta_Y_mean": float(delta_south.mean()),
        "delta_Y_std": float(delta_south.std()),
    }
    json_results["data_summary"] = {
        "n_hours": int(X.shape[0]),
        "n_pixels": int(X.shape[1]),
        "Y_mean": float(Y.mean()),
        "Y_std": float(Y.std()),
        "primary_field": primary_field,
    }

    json_path = tables_dir / "functional_analysis_results.json"
    with open(json_path, "w") as f:
        json.dump(json_results, f, indent=2)
    print(f"\n  JSON results saved: {json_path}")

    print("\n" + "=" * 70)
    print("ANALYSIS COMPLETE")
    print("=" * 70)

    return {
        "all_results": all_results,
        "all_betas": all_betas,
        "summary_df": summary_df,
        "fpca_res": fpca_res,
        "pixel_coords": pixel_coords,
        "pixel_ids": pixel_ids,
        "cf_west_tx": cf_results,
        "fig_dir": fig_dir,
    }


# ── Extension A: Partial Least Squares (supervised basis) ────────────────────

def run_pls_analysis(X, Y, pixel_coords, n_components_list=None,
                     n_folds=N_CV_FOLDS):
    """Partial Least Squares regression as a supervised alternative to FPCA.

    PLS finds directions that *jointly* maximise covariance between X and Y,
    whereas FPCA maximises variance in X alone.  When Y-relevant structure
    lives in low-variance X directions, PLS should outperform FPCA.

    Returns
    -------
    dict with results per n_components
    """
    from sklearn.cross_decomposition import PLSRegression

    if n_components_list is None:
        n_components_list = [5, 10, 20, 50, 100, 200]
    n_components_list = [k for k in n_components_list if k < min(X.shape)]

    print("\n=== Extension A: Partial Least Squares (PLS) ===")

    kf = KFold(n_splits=n_folds, shuffle=True, random_state=RANDOM_STATE)
    results = {}

    for n_comp in n_components_list:
        pls = PLSRegression(n_components=n_comp, max_iter=1000)
        r2_folds = cross_val_score(pls, X, Y, cv=kf, scoring="r2")

        # Fit on full data to get spatial coefficients
        pls.fit(X, Y)
        # PLS coefs: the direct coefficient vector in input space
        # pls.coef_ has shape (n_features, n_targets)
        beta_spatial = pls.coef_.ravel()

        # Coefficient stability: std of coefs across folds
        coef_folds = []
        for train_idx, _ in kf.split(X):
            p = PLSRegression(n_components=n_comp, max_iter=500)
            p.fit(X[train_idx], Y[train_idx])
            coef_folds.append(p.coef_.ravel())
        coef_std = np.std(coef_folds, axis=0).mean()

        results[n_comp] = {
            "r2": r2_folds.mean(),
            "r2_std": r2_folds.std(),
            "r2_folds": r2_folds,
            "coef_std": coef_std,
            "beta_spatial": beta_spatial,
        }
        print(f"  n_comp={n_comp:3d}: CV R²={r2_folds.mean():.4f} ± "
              f"{r2_folds.std():.4f}, coef_std={coef_std:.4f}")

    best = max(results, key=lambda k: results[k]["r2"])
    return {"results_by_n": results, "best_n": best}


# ── Extension B: Quantile FPCA (tail behaviour) ───────────────────────────────

def run_quantile_fpca(X, Y, pixel_coords, K=100, quantiles=None,
                      n_folds=N_CV_FOLDS):
    """Estimate how spatial error effects differ across the curtailment
    distribution using quantile regression on FPCA scores.

    For each quantile τ, fits:
        Q_τ(Y | Θ) = Θ γ_τ     (linear quantile regression)

    and recovers the spatial coefficient surface β_τ(s).  Comparing β_0.5
    (median) to β_0.9 (upper tail) reveals whether large-curtailment hours
    are driven by different geographic error patterns.

    Returns
    -------
    dict: per-quantile beta_spatial and pseudo-R²
    """
    from sklearn.linear_model import QuantileRegressor

    if quantiles is None:
        quantiles = [0.25, 0.50, 0.75, 0.90]

    print("\n=== Extension C: Quantile Regression on FPCA Scores ===")

    K_use = min(K, X.shape[0] - 1, X.shape[1] - 1)
    pca = PCA(n_components=K_use, random_state=RANDOM_STATE)
    scores = pca.fit_transform(X)
    components = pca.components_

    kf = KFold(n_splits=n_folds, shuffle=True, random_state=RANDOM_STATE)
    results = {}

    # Baseline: median of Y for Koenker-Bassett pseudo-R²
    Y_med = np.median(Y)

    for tau in quantiles:
        qr = QuantileRegressor(quantile=tau, alpha=0.01, solver="highs")
        qr.fit(scores, Y)
        beta_spatial = components[:K_use].T @ qr.coef_

        # Pinball (check) loss pseudo-R²
        Y_pred = qr.predict(scores)
        resid = Y - Y_pred
        loss_model = np.mean(np.where(resid >= 0, tau * resid, (tau - 1) * resid))
        resid_null = Y - Y_med
        loss_null = np.mean(np.where(resid_null >= 0,
                                     tau * resid_null,
                                     (tau - 1) * resid_null))
        pseudo_r2 = 1.0 - loss_model / (loss_null + 1e-10)

        results[tau] = {
            "beta_spatial": beta_spatial,
            "pseudo_r2": pseudo_r2,
            "coef": qr.coef_,
        }
        print(f"  τ={tau:.2f}: pseudo-R²={pseudo_r2:.4f}")

    return results


# ── Extension D: Pixel stability analysis ────────────────────────────────────

def run_pixel_stability(X, Y, pixel_coords, K=100, n_folds=N_CV_FOLDS):
    """Quantify how reliably each pixel is identified as high-impact.

    For each cross-validation fold, fits FPCA K=100 and recovers beta(s).
    Reports per-pixel mean and standard deviation of beta across folds,
    and a "stability score" = |mean| / std.  High stability score means
    the pixel is consistently identified as positive or negative.

    Returns
    -------
    dict: per-pixel mean_beta, std_beta, stability
    """
    print("\n=== Extension D: Per-Pixel Coefficient Stability ===")

    K_use = min(K, X.shape[0] // n_folds - 1, X.shape[1] - 1)
    kf = KFold(n_splits=n_folds, shuffle=True, random_state=RANDOM_STATE)

    betas_folds = []
    for fold_i, (train_idx, _) in enumerate(kf.split(X)):
        X_tr, Y_tr = X[train_idx], Y[train_idx]
        pca = PCA(n_components=K_use, random_state=RANDOM_STATE)
        scores = pca.fit_transform(X_tr)
        comps = pca.components_

        ridge = RidgeCV(alphas=np.logspace(1, 6, 20))
        ridge.fit(scores, Y_tr)
        beta = comps[:K_use].T @ ridge.coef_
        betas_folds.append(beta)
        print(f"  Fold {fold_i+1}/{n_folds}: beta range "
              f"[{beta.min():.3f}, {beta.max():.3f}]")

    betas_arr = np.stack(betas_folds, axis=0)   # (n_folds, N_pixels)
    mean_beta = betas_arr.mean(axis=0)
    std_beta  = betas_arr.std(axis=0)
    stability = np.abs(mean_beta) / (std_beta + 1e-8)

    print(f"  Top-10% most stable pixels: "
          f"{(stability > np.percentile(stability, 90)).sum()}")
    print(f"  Mean stability score: {stability.mean():.3f}")

    return {
        "mean_beta": mean_beta,
        "std_beta": std_beta,
        "stability": stability,
        "betas_folds": betas_arr,
    }


# ── Extension E: PLS vs FPCA direct beta comparison map ──────────────────────

def plot_pls_vs_fpca(pls_beta, fpca_beta, pixel_coords, save_path,
                     pls_r2=None, fpca_r2=None,
                     pls_sig_mask=None, fpca_sig_mask=None):
    """Side-by-side comparison of PLS and FPCA coefficient surfaces.

    Parameters
    ----------
    pls_r2, fpca_r2 : float, optional
        CV R² values appended to panel titles.
    pls_sig_mask, fpca_sig_mask : ndarray of bool, optional
        Only significant pixels are plotted in each panel.
    """
    fig, axes = plt.subplots(
        1, 3, figsize=(18, 5),
        subplot_kw={"projection": ccrs.PlateCarree()},
    )
    all_betas = np.concatenate([pls_beta, fpca_beta])
    clim = np.nanpercentile(np.abs(all_betas), 98)

    corr = np.corrcoef(pls_beta, fpca_beta)[0, 1]

    pls_title = "PLS (supervised)"
    if pls_r2 is not None:
        pls_title += f"  R²={pls_r2:.3f}"
    fpca_title = "FPCA K=100 (unsupervised)"
    if fpca_r2 is not None:
        fpca_title += f"  R²={fpca_r2:.3f}"

    sc = plot_beta_surface(pls_beta, pixel_coords, pls_title, axes[0],
                           vmin=-clim, vmax=clim, sig_mask=pls_sig_mask)
    plot_beta_surface(fpca_beta, pixel_coords, fpca_title, axes[1],
                      vmin=-clim, vmax=clim, sig_mask=fpca_sig_mask)

    # Difference panel — no significance masking (shown for both)
    diff = pls_beta - fpca_beta
    diff_clim = np.nanpercentile(np.abs(diff), 98)
    plot_beta_surface(diff, pixel_coords, "Difference (PLS − FPCA)",
                      axes[2], vmin=-diff_clim, vmax=diff_clim)

    sig_note = "  [p < 0.05 only]" if (pls_sig_mask is not None or fpca_sig_mask is not None) else ""
    fig.suptitle(
        f"PLS vs FPCA Coefficient Surfaces  (Pearson r = {corr:.3f}){sig_note}",
        fontsize=13,
    )
    fig.colorbar(sc, ax=axes.ravel().tolist(), shrink=0.6,
                 label="β(s): effect on curtailment MW per σ error")
    fig.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  PLS vs FPCA saved: {save_path}")
    return corr


def plot_quantile_betas(quantile_res, pixel_coords, save_path):
    """Multi-panel quantile beta surfaces."""
    taus = sorted(quantile_res.keys())
    n = len(taus)
    fig, axes = plt.subplots(
        1, n, figsize=(6 * n, 5),
        subplot_kw={"projection": ccrs.PlateCarree()},
    )
    if n == 1:
        axes = [axes]
    all_betas = np.concatenate([quantile_res[t]["beta_spatial"] for t in taus])
    clim = np.nanpercentile(np.abs(all_betas), 98)

    for ax, tau in zip(axes, taus):
        pR2 = quantile_res[tau]["pseudo_r2"]
        sc = plot_beta_surface(
            quantile_res[tau]["beta_spatial"], pixel_coords,
            f"τ={tau:.2f}  (pseudo-R²={pR2:.3f})", ax,
            vmin=-clim, vmax=clim,
        )

    fig.suptitle(
        "Quantile Regression: Spatial Effects Across Curtailment Distribution",
        fontsize=13,
    )
    fig.colorbar(sc, ax=axes, shrink=0.6,
                 label="β_τ(s): quantile-specific spatial effect")
    fig.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Quantile betas saved: {save_path}")


def plot_stability_map(stability_res, pixel_coords, save_path):
    """Plot pixel stability (|mean_beta| / std_beta) and sign-consistency."""
    fig, axes = plt.subplots(
        1, 3, figsize=(18, 5),
        subplot_kw={"projection": ccrs.PlateCarree()},
    )
    mean_b = stability_res["mean_beta"]
    stab = stability_res["stability"]

    # Panel 1: mean beta
    clim = np.nanpercentile(np.abs(mean_b), 98)
    sc1 = plot_beta_surface(mean_b, pixel_coords,
                            "Mean β(s) across folds", axes[0],
                            vmin=-clim, vmax=clim)

    # Panel 2: std beta (uncertainty)
    std_b = stability_res["std_beta"]
    sc2 = plot_beta_surface(std_b, pixel_coords,
                            "Std β(s) across folds (uncertainty)", axes[1],
                            vmin=0, vmax=np.percentile(std_b, 98))

    # Panel 3: stability = |mean| / std
    sc3 = plot_beta_surface(stab, pixel_coords,
                            "Stability: |mean β| / std β", axes[2],
                            vmin=0, vmax=np.percentile(stab, 98))

    for sc, ax in zip([sc1, sc2, sc3], axes):
        fig.colorbar(sc, ax=ax, shrink=0.8, pad=0.02)

    fig.suptitle(
        "FPCA Coefficient Stability Across Cross-Validation Folds (K=100)",
        fontsize=13,
    )
    fig.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Stability map saved: {save_path}")


# ════════════════════════════════════════════════════════════════════════════
# Step 3A — Multi-field FPCA / PLS comparison
# ════════════════════════════════════════════════════════════════════════════

def run_multi_field_comparison(months=None, K=100, n_pls=20,
                                n_folds=N_CV_FOLDS):
    """Run FPCA K=100 and PLS n=20 on each of the 4 error fields.

    Generates:
      - 4×2 beta surface grid (rows=fields, cols=FPCA/PLS)
      - Grouped R² bar chart
      - tables/multi_field_comparison.csv

    Returns
    -------
    dict: field → {'fpca': {...}, 'pls': {...}}
    """
    from sklearn.cross_decomposition import PLSRegression

    if months is None:
        months = DEFAULT_MONTHS

    dirs = setup_directories()
    fig_dir = Path(dirs["figures"]) / "functional_analysis"
    fig_dir.mkdir(parents=True, exist_ok=True)
    tables_dir = Path(dirs["tables"])
    tables_dir.mkdir(parents=True, exist_ok=True)

    print("\n" + "=" * 70)
    print("STEP 3A: MULTI-FIELD FPCA / PLS COMPARISON")
    print("=" * 70)

    # Load data once with all error fields
    df = load_pixel_data(months)
    kf = KFold(n_splits=n_folds, shuffle=True, random_state=RANDOM_STATE)

    field_results = {}
    summary_rows = []

    for field in ERROR_FIELDS:
        print(f"\n--- Field: {field} ---")
        try:
            X, Y, pixel_coords, pixel_ids, _ = prepare_functional_data(
                df, error_field=field
            )
        except Exception as e:
            print(f"  Skipping {field}: {e}")
            continue

        K_use = min(K, X.shape[0] - 1, X.shape[1] - 1)
        n_pls_use = min(n_pls, X.shape[0] - 1, X.shape[1] - 1)

        # FPCA K=100
        pca = PCA(n_components=K_use, random_state=RANDOM_STATE)
        scores = pca.fit_transform(X)
        ridge = RidgeCV(alphas=np.logspace(1, 6, 20), cv=kf, scoring="r2")
        ridge.fit(scores, Y)
        fpca_r2 = cross_val_score(
            RidgeCV(alphas=[ridge.alpha_]), scores, Y, cv=kf, scoring="r2"
        )
        fpca_beta = pca.components_[:K_use].T @ ridge.coef_

        # PLS n=20
        pls = PLSRegression(n_components=n_pls_use, max_iter=1000)
        pls_r2 = cross_val_score(pls, X, Y, cv=kf, scoring="r2")
        pls.fit(X, Y)
        pls_beta = pls.coef_.ravel()

        # Bootstrap significance (B=100)
        print(f"  Bootstrap significance (B=100)...")
        _K = K_use
        def _fpca_fit_fn(Xb, Yb):
            K_b = min(_K, Xb.shape[0] - 1, Xb.shape[1] - 1)
            p = PCA(n_components=K_b, random_state=RANDOM_STATE)
            sc = p.fit_transform(Xb)
            r = RidgeCV(alphas=np.logspace(1, 6, 10), cv=3)
            r.fit(sc, Yb)
            return p.components_[:K_b].T @ r.coef_

        _n_pls = n_pls_use
        def _pls_fit_fn(Xb, Yb):
            n_b = min(_n_pls, Xb.shape[0] - 1, Xb.shape[1] - 1)
            p = PLSRegression(n_components=n_b, max_iter=500)
            p.fit(Xb, Yb)
            return p.coef_.ravel()

        fpca_pvals = bootstrap_pixel_pvalues(X, Y, _fpca_fit_fn, B=100)
        pls_pvals = bootstrap_pixel_pvalues(X, Y, _pls_fit_fn, B=100)
        fpca_sig = fpca_pvals < 0.05
        pls_sig = pls_pvals < 0.05
        print(f"  FPCA sig pixels: {fpca_sig.sum()}/{len(fpca_sig)}, "
              f"PLS sig pixels: {pls_sig.sum()}/{len(pls_sig)}")

        field_results[field] = {
            "fpca": {
                "r2": fpca_r2.mean(), "r2_std": fpca_r2.std(),
                "beta": fpca_beta, "K": K_use,
                "sig_mask": fpca_sig,
            },
            "pls": {
                "r2": pls_r2.mean(), "r2_std": pls_r2.std(),
                "beta": pls_beta, "n": n_pls_use,
                "sig_mask": pls_sig,
            },
            "pixel_coords": pixel_coords,
        }

        # PLS-FPCA correlation
        corr = np.corrcoef(pls_beta, fpca_beta)[0, 1]
        print(f"  FPCA K={K_use}: R²={fpca_r2.mean():.4f} ± {fpca_r2.std():.4f}")
        print(f"  PLS  n={n_pls_use}: R²={pls_r2.mean():.4f} ± {pls_r2.std():.4f}")
        print(f"  PLS-FPCA β correlation: {corr:.3f}")

        summary_rows.append({
            "field": field, "method": "FPCA", "K_n": K_use,
            "r2": fpca_r2.mean(), "r2_std": fpca_r2.std(),
            "n_sig_pixels": int(fpca_sig.sum()),
        })
        summary_rows.append({
            "field": field, "method": "PLS", "K_n": n_pls_use,
            "r2": pls_r2.mean(), "r2_std": pls_r2.std(),
            "n_sig_pixels": int(pls_sig.sum()),
        })

    if not field_results:
        print("  No results — aborting.")
        return {}

    # ── 4×2 beta surface grid ──
    fields_ok = [f for f in ERROR_FIELDS if f in field_results]
    n_rows = len(fields_ok)
    n_cols = 2

    fig, axes = plt.subplots(
        n_rows, n_cols, figsize=(14, 5 * n_rows),
        subplot_kw={"projection": ccrs.PlateCarree()},
        gridspec_kw={"hspace": 0.3, "wspace": 0.05},
    )
    if n_rows == 1:
        axes = axes[np.newaxis, :]

    field_labels = {
        "wspd_error_1h": "HRRR 1h Wind Speed Error",
        "wspd_error_0h": "GFS Day-Ahead Wind Speed Error",
        "temp_error_1h": "HRRR 1h Temperature Error",
        "temp_error_0h": "GFS Day-Ahead Temperature Error",
    }
    method_labels = ["FPCA K=100", "PLS n=20"]

    sc_last = None
    for row_i, field in enumerate(fields_ok):
        res = field_results[field]
        pc = res["pixel_coords"]
        betas = [res["fpca"]["beta"], res["pls"]["beta"]]
        sig_masks_plot = [res["fpca"]["sig_mask"], res["pls"]["sig_mask"]]
        all_b = np.concatenate(betas)
        clim = np.nanpercentile(np.abs(all_b), 98)

        for col_i, (method_label, beta, sig_m) in enumerate(
            zip(method_labels, betas, sig_masks_plot)
        ):
            ax = axes[row_i, col_i]
            r2 = res["fpca"]["r2"] if col_i == 0 else res["pls"]["r2"]
            title = (f"{field_labels.get(field, field)}\n"
                     f"{method_label}  (R²={r2:.3f})")
            sc = plot_beta_surface(beta, pc, title, ax,
                                   vmin=-clim, vmax=clim, sig_mask=sig_m)
            sc_last = sc

    if sc_last is not None:
        fig.colorbar(sc_last, ax=axes.ravel().tolist(), shrink=0.5,
                     label="β(s): effect on curtailment MW per σ error",
                     pad=0.02)

    fig.suptitle(
        "Multi-field FPCA vs PLS: Spatial Coefficient Surfaces β(s)\n"
        f"({len(months)} months, DEPVAR={DEPVAR})  [p < 0.05 only]",
        fontsize=13, y=1.01,
    )
    save_path = fig_dir / "multi_field_fpca_pls_comparison.png"
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"\n  Beta grid saved: {save_path}")

    # ── R² bar chart ──
    fig2, ax2 = plt.subplots(figsize=(12, 5))
    x = np.arange(len(fields_ok))
    width = 0.35
    fpca_r2s = [field_results[f]["fpca"]["r2"] for f in fields_ok]
    fpca_stds = [field_results[f]["fpca"]["r2_std"] for f in fields_ok]
    pls_r2s = [field_results[f]["pls"]["r2"] for f in fields_ok]
    pls_stds = [field_results[f]["pls"]["r2_std"] for f in fields_ok]
    xlabels = [field_labels.get(f, f) for f in fields_ok]

    ax2.bar(x - width/2, fpca_r2s, width, yerr=fpca_stds, capsize=4,
            label="FPCA K=100", color="steelblue", alpha=0.8)
    ax2.bar(x + width/2, pls_r2s, width, yerr=pls_stds, capsize=4,
            label="PLS n=20", color="coral", alpha=0.8)
    ax2.set_xticks(x)
    ax2.set_xticklabels(xlabels, rotation=20, ha="right", fontsize=9)
    ax2.set_ylabel("CV R² (5-fold)")
    ax2.set_title("FPCA vs PLS: Predictive Performance by Error Field")
    ax2.legend()
    ax2.set_ylim(0, max(fpca_r2s + pls_r2s) * 1.15)
    fig2.tight_layout()
    save_path2 = fig_dir / "multi_field_r2_comparison.png"
    fig2.savefig(save_path2, dpi=150, bbox_inches="tight")
    plt.close(fig2)
    print(f"  R² comparison chart saved: {save_path2}")

    # ── Summary table ──
    pd.DataFrame(summary_rows).to_csv(
        tables_dir / "multi_field_comparison.csv", index=False
    )
    print(f"  Summary table saved.")

    return field_results


# ════════════════════════════════════════════════════════════════════════════
# Step 3B — PLS vs FPCA divergence investigation
# ════════════════════════════════════════════════════════════════════════════

def investigate_pls_fpca_divergence(X, Y, pixel_coords, field_label="wspd_error_1h",
                                     K=100, n_pls=20, fig_dir=None,
                                     n_folds=N_CV_FOLDS):
    """Diagnose why PLS and FPCA find different spatial patterns.

    Three analyses:
      1. Variance vs covariance decomposition: for each FPCA component,
         plot Var(θ_k) vs |Cov(θ_k, Y)| to see whether high-variance
         directions are also high-Y-covariance directions.
      2. Loading cross-correlation heatmap: correlate each PLS loading
         vector (x_weights_) against each FPCA component.
      3. Geographic interpretation: correlate β(s) from each method with
         pixel-level wind capacity.

    Returns
    -------
    dict with diagnostic statistics
    """
    from sklearn.cross_decomposition import PLSRegression
    from scipy.spatial import cKDTree
    import xarray as xr

    if fig_dir is None:
        dirs = setup_directories()
        fig_dir = Path(dirs["figures"]) / "functional_analysis"
    fig_dir = Path(fig_dir)

    print("\n" + "=" * 70)
    print("STEP 3B: PLS vs FPCA DIVERGENCE INVESTIGATION")
    print(f"  Field: {field_label}")
    print("=" * 70)

    K_use = min(K, X.shape[0] - 1, X.shape[1] - 1)
    n_pls_use = min(n_pls, X.shape[0] - 1, X.shape[1] - 1)

    kf = KFold(n_splits=n_folds, shuffle=True, random_state=RANDOM_STATE)

    # ── Fit FPCA ──
    pca = PCA(n_components=K_use, random_state=RANDOM_STATE)
    scores_fpca = pca.fit_transform(X)   # (T, K)
    components = pca.components_         # (K, N_pixels)
    eigenvalues = pca.explained_variance_  # (K,)

    ridge = RidgeCV(alphas=np.logspace(1, 6, 20), cv=kf, scoring="r2")
    ridge.fit(scores_fpca, Y)
    fpca_beta = components[:K_use].T @ ridge.coef_

    # ── Fit PLS ──
    pls = PLSRegression(n_components=n_pls_use, max_iter=1000)
    pls.fit(X, Y)
    pls_beta = pls.coef_.ravel()
    pls_loadings = pls.x_weights_    # (N_pixels, n_pls) — PLS loading vectors

    corr_overall = np.corrcoef(pls_beta, fpca_beta)[0, 1]
    print(f"  Overall β(s) Pearson r (PLS vs FPCA): {corr_overall:.4f}")

    # ── Analysis 1: Variance vs Covariance decomposition ──
    print("\n  Analysis 1: Variance vs |Cov(θ_k, Y)| per FPCA component")
    cov_with_Y = np.array([abs(np.cov(scores_fpca[:, k], Y)[0, 1])
                            for k in range(K_use)])
    top20_var_mask = np.zeros(K_use, dtype=bool)
    top20_var_mask[:20] = True  # top-20 by variance (PCA sorted order)
    top20_cov_idx = np.argsort(cov_with_Y)[-20:]

    overlap = top20_var_mask.nonzero()[0]
    overlap_count = len(set(overlap) & set(top20_cov_idx))
    print(f"  Top-20 variance components: components 1–20 (by construction)")
    print(f"  Top-20 covariance components: {sorted(top20_cov_idx + 1)}")
    print(f"  Overlap: {overlap_count}/20 components")
    print(f"  → {100 - overlap_count*5:.0f}% of Y-covariance is in low-variance directions")

    # Fraction of total Y-covariance captured by top-20 variance components
    total_cov = cov_with_Y.sum()
    frac_top20_var = cov_with_Y[:20].sum() / (total_cov + 1e-10)
    print(f"  Top-20 variance components capture {frac_top20_var*100:.1f}% of total |Cov(θ, Y)|")

    fig1, axes1 = plt.subplots(1, 2, figsize=(14, 5))

    # Left: eigenvalue (variance) vs |Cov with Y|
    comp_idx = np.arange(1, K_use + 1)
    ax = axes1[0]
    ax2_twin = ax.twinx()
    ln1 = ax.semilogy(comp_idx, eigenvalues, "o-", color="steelblue",
                      markersize=3, label="Eigenvalue (Var)")
    ln2 = ax2_twin.semilogy(comp_idx, cov_with_Y + 1e-10, "s--", color="coral",
                             markersize=3, label="|Cov(θ_k, Y)|")
    ax.set_xlabel("FPCA Component (rank order)")
    ax.set_ylabel("Eigenvalue (log scale)", color="steelblue")
    ax2_twin.set_ylabel("|Cov(θ_k, Y)| (log scale)", color="coral")
    ax.set_title("Variance vs Y-Covariance per FPCA Component")
    lns = ln1 + ln2
    labs = [l.get_label() for l in lns]
    ax.legend(lns, labs, loc="upper right")

    # Right: scatter of eigenvalue vs |Cov| — highlight top-20 cov
    ax_r = axes1[1]
    ax_r.scatter(eigenvalues, cov_with_Y, s=20, alpha=0.5, color="steelblue",
                 label="All components")
    ax_r.scatter(eigenvalues[top20_cov_idx], cov_with_Y[top20_cov_idx],
                 s=60, color="crimson", zorder=5, label="Top-20 by |Cov|")
    for idx in top20_cov_idx:
        ax_r.annotate(str(idx + 1),
                      (eigenvalues[idx], cov_with_Y[idx]),
                      fontsize=7, color="crimson")
    ax_r.set_xlabel("Eigenvalue (component variance)")
    ax_r.set_ylabel("|Cov(θ_k, Y)|")
    ax_r.set_title("Which components are Y-relevant?\n(crimson = top-20 by Y-covariance)")
    ax_r.legend()

    fig1.suptitle(
        f"PLS vs FPCA Divergence: Variance-Covariance Misalignment\n"
        f"Field: {field_label} | {overlap_count}/20 top-variance comps are top-covariance",
        fontsize=12,
    )
    fig1.tight_layout()
    path1 = fig_dir / f"variance_vs_covariance_{field_label}.png"
    fig1.savefig(path1, dpi=150, bbox_inches="tight")
    plt.close(fig1)
    print(f"  Plot saved: {path1}")

    # ── Analysis 2: PLS loading vectors vs FPCA components ──
    n_pls_show = min(n_pls_use, 10)
    n_fpca_show = min(K_use, 20)
    corr_matrix = np.zeros((n_pls_show, n_fpca_show))
    for i in range(n_pls_show):
        for j in range(n_fpca_show):
            corr_matrix[i, j] = abs(
                np.corrcoef(pls_loadings[:, i], components[j])[0, 1]
            )

    fig2, ax = plt.subplots(figsize=(12, 5))
    im = ax.imshow(corr_matrix, aspect="auto", cmap="YlOrRd", vmin=0, vmax=1)
    ax.set_xlabel("FPCA Component (1–20)")
    ax.set_ylabel("PLS Loading Vector (1–10)")
    ax.set_xticks(range(n_fpca_show))
    ax.set_xticklabels([f"PC{j+1}" for j in range(n_fpca_show)], rotation=45, ha="right")
    ax.set_yticks(range(n_pls_show))
    ax.set_yticklabels([f"PLS{i+1}" for i in range(n_pls_show)])
    plt.colorbar(im, ax=ax, label="|Pearson r|")
    ax.set_title(
        f"Absolute Correlation: PLS Loading Vectors vs FPCA Components\n"
        f"Field: {field_label}"
    )
    fig2.tight_layout()
    path2 = fig_dir / f"loading_correlation_{field_label}.png"
    fig2.savefig(path2, dpi=150, bbox_inches="tight")
    plt.close(fig2)
    print(f"  Loading correlation heatmap saved: {path2}")

    # ── Analysis 3: β(s) correlation with wind capacity ──
    dirs = setup_directories()
    gen_path = Path(dirs["processed"]) / "gridded_generation_map.nc"
    wind_corr_fpca = np.nan
    wind_corr_pls = np.nan
    if gen_path.exists():
        import xarray as xr
        ds = xr.open_dataset(gen_path)
        wind_cap_col = "nameplate_mw_tech_onshore_wind_turbine"
        if wind_cap_col in ds:
            # Build pixel-level wind capacity matching pixel_coords
            gen_df = ds.to_dataframe().reset_index()
            ds.close()
            gen_df["pixel_id"] = (
                gen_df["latitude"].round(1).astype(str) + "_" +
                gen_df["longitude"].round(1).astype(str)
            )
            gen_df = gen_df.set_index("pixel_id")[wind_cap_col].fillna(0)

            coord_ids = [f"{lat:.1f}_{lon:.1f}"
                         for lat, lon in pixel_coords]
            # Deduplicate index to ensure scalar lookups
            gen_df = gen_df.groupby(level=0).first()
            wind_vals = np.array([
                float(gen_df[pid]) if pid in gen_df.index else 0.0
                for pid in coord_ids
            ], dtype=float)

            # Only non-zero wind pixels for correlation
            mask = wind_vals > 0
            if mask.sum() > 20:
                wind_corr_fpca = np.corrcoef(fpca_beta[mask], wind_vals[mask])[0, 1]
                wind_corr_pls = np.corrcoef(pls_beta[mask], wind_vals[mask])[0, 1]
                print(f"\n  β(s) ↔ wind capacity (MW) correlation:")
                print(f"    FPCA: r={wind_corr_fpca:.4f}")
                print(f"    PLS:  r={wind_corr_pls:.4f}")

    return {
        "corr_overall": corr_overall,
        "cov_with_Y": cov_with_Y,
        "eigenvalues": eigenvalues,
        "overlap_count": overlap_count,
        "frac_top20_var": frac_top20_var,
        "corr_matrix": corr_matrix,
        "wind_corr_fpca": wind_corr_fpca,
        "wind_corr_pls": wind_corr_pls,
        "fpca_beta": fpca_beta,
        "pls_beta": pls_beta,
        "pixel_coords": pixel_coords,
    }


# ════════════════════════════════════════════════════════════════════════════
# Step 3C — Regime-stratified FPCA / PLS
# ════════════════════════════════════════════════════════════════════════════

# Regime definitions (matching pixel_regression_maps.py)
REGIMES = {
    "extreme_cold": {
        "filter_col": "regime_temp", "filter_val": "extreme_cold",
        "label": "Extreme Cold (Bottom 5% Temp)",
    },
    "extreme_heat": {
        "filter_col": "regime_temp", "filter_val": "extreme_heat",
        "label": "Extreme Heat (Top 5% Temp)",
    },
    "high_wind": {
        "filter_col": "regime_wind", "filter_val": "high_wind",
        "label": "High Wind (Top 10% Wind Speed)",
    },
    "stressed_grid": {
        "filter_col": "regime_grid", "filter_val": "stressed",
        "label": "Stressed Grid (Top 5% LMP Max)",
    },
}

# Extra columns needed for regime classification
REGIME_EXTRA_COLS = ["era5_temp", "era5_wspd", "system_lmp_max"]


def load_pixel_data_with_regimes(months):
    """Load pixel-hourly data with extra columns needed for regime classification."""
    dirs = setup_directories()
    lmp_dir = Path(dirs["processed"]) / "combined_hourly_gridded_data"

    keep_cols = (
        ["pixel_id", "valid_time", "latitude", "longitude", DEPVAR]
        + ERROR_FIELDS
        + REGIME_EXTRA_COLS
    )

    dfs = []
    for year, month in months:
        path = lmp_dir / f"pixel_hourly_gfs+hrrr_{year}_{month:02d}.parquet"
        if not path.exists():
            print(f"  [WARNING] Missing: {path}")
            continue
        import pyarrow.parquet as pq
        parquet_cols = pq.read_schema(path).names
        cols_to_read = [c for c in keep_cols if c in parquet_cols]
        df = pd.read_parquet(path, columns=cols_to_read)
        df["valid_time"] = pd.to_datetime(df["valid_time"])
        if df["valid_time"].dt.tz is not None:
            df["valid_time"] = df["valid_time"].dt.tz_localize(None)
        print(f"  Loaded {year}-{month:02d}: {len(df):,} rows")
        dfs.append(df)
    if not dfs:
        raise FileNotFoundError("No pixel_hourly parquet files found.")
    return pd.concat(dfs, ignore_index=True)


def run_regime_stratified_analysis(months=None,
                                    regimes=("extreme_cold", "extreme_heat"),
                                    error_fields=None,
                                    K=50, n_pls=10,
                                    n_folds=N_CV_FOLDS):
    """Run FPCA and PLS for each extreme weather regime × each error field.

    Parameters
    ----------
    regimes : tuple of str
        Regime keys from REGIMES dict. Default: extreme_cold, extreme_heat.
    error_fields : list of str, optional
        Defaults to ERROR_FIELDS.
    K : int
        FPCA components (reduced from 100 due to fewer hours in regime).
    n_pls : int
        PLS components.

    Returns
    -------
    dict: regime → field → {'fpca': {...}, 'pls': {...}}
    """
    from sklearn.cross_decomposition import PLSRegression
    from process_data.classify_weather_regimes import classify_regimes

    if months is None:
        months = DEFAULT_MONTHS
    if error_fields is None:
        error_fields = ERROR_FIELDS

    dirs = setup_directories()
    fig_dir = Path(dirs["figures"]) / "functional_analysis"
    fig_dir.mkdir(parents=True, exist_ok=True)
    tables_dir = Path(dirs["tables"])
    tables_dir.mkdir(parents=True, exist_ok=True)

    print("\n" + "=" * 70)
    print("STEP 3C: REGIME-STRATIFIED FPCA / PLS ANALYSIS")
    print(f"  Regimes: {list(regimes)}")
    print(f"  Fields: {error_fields}")
    print("=" * 70)

    # Load data with regime columns
    df_full = load_pixel_data_with_regimes(months)

    # Classify regimes (computes system-wide percentile thresholds)
    print("\nClassifying regimes across full dataset...")
    df_full = classify_regimes(df_full)

    # Get full-sample betas for comparison (wspd_error_1h, FPCA K=50)
    X_all, Y_all, pc_all, _, _ = prepare_functional_data(
        df_full, error_field="wspd_error_1h"
    )
    K_all = min(K, X_all.shape[0] - 1, X_all.shape[1] - 1)
    pca_all = PCA(n_components=K_all, random_state=RANDOM_STATE)
    scores_all = pca_all.fit_transform(X_all)
    kf = KFold(n_splits=n_folds, shuffle=True, random_state=RANDOM_STATE)
    ridge_all = RidgeCV(alphas=np.logspace(1, 6, 20), cv=kf, scoring="r2")
    ridge_all.fit(scores_all, Y_all)
    full_sample_beta = pca_all.components_[:K_all].T @ ridge_all.coef_
    full_sample_coords = pc_all
    print(f"  Full-sample FPCA K={K_all}: R²="
          f"{cross_val_score(RidgeCV(alphas=[ridge_all.alpha_]), scores_all, Y_all, cv=kf, scoring='r2').mean():.4f}")

    regime_results = {}
    summary_rows = []

    for regime_name in regimes:
        regime_spec = REGIMES[regime_name]
        regime_label = regime_spec["label"]
        filter_col = regime_spec["filter_col"]
        filter_val = regime_spec["filter_val"]

        # Filter to regime hours
        mask = df_full[filter_col] == filter_val
        df_regime = df_full[mask].copy()
        n_regime_hours = df_regime["valid_time"].nunique()
        print(f"\n--- Regime: {regime_label} ({n_regime_hours} hours) ---")

        if n_regime_hours < 50:
            print(f"  Too few hours ({n_regime_hours}), skipping.")
            continue

        regime_results[regime_name] = {}

        for field in error_fields:
            print(f"  Field: {field}")
            try:
                X_r, Y_r, pc_r, _, _ = prepare_functional_data(
                    df_regime, error_field=field
                )
            except Exception as e:
                print(f"    Error: {e}, skipping.")
                continue

            n_hours_r = X_r.shape[0]
            if n_hours_r < 30:
                print(f"    Too few hours after prep ({n_hours_r}), skipping.")
                continue

            # Reduce K/n_pls based on available data
            K_r = min(K, n_hours_r - 1, X_r.shape[1] - 1)
            n_pls_r = min(n_pls, n_hours_r - 1, X_r.shape[1] - 1)

            # Use leave-one-out-style CV if very few hours
            n_cv = min(n_folds, n_hours_r // 5)
            if n_cv < 2:
                n_cv = 2
            kf_r = KFold(n_splits=n_cv, shuffle=True, random_state=RANDOM_STATE)

            # FPCA
            pca_r = PCA(n_components=K_r, random_state=RANDOM_STATE)
            scores_r = pca_r.fit_transform(X_r)
            ridge_r = RidgeCV(alphas=np.logspace(1, 6, 20), cv=kf_r, scoring="r2")
            ridge_r.fit(scores_r, Y_r)
            fpca_r2 = cross_val_score(
                RidgeCV(alphas=[ridge_r.alpha_]), scores_r, Y_r, cv=kf_r, scoring="r2"
            )
            fpca_beta_r = pca_r.components_[:K_r].T @ ridge_r.coef_

            # PLS
            pls_r = PLSRegression(n_components=n_pls_r, max_iter=1000)
            pls_r2 = cross_val_score(pls_r, X_r, Y_r, cv=kf_r, scoring="r2")
            pls_r.fit(X_r, Y_r)
            pls_beta_r = pls_r.coef_.ravel()

            # Bootstrap significance (B=100; reduce if very few hours)
            B_regime = min(100, max(30, n_hours_r // 2))
            _K_r = K_r
            _n_pls_r = n_pls_r
            def _fpca_r_fit(Xb, Yb):
                K_b = min(_K_r, Xb.shape[0] - 1, Xb.shape[1] - 1)
                p = PCA(n_components=K_b, random_state=RANDOM_STATE)
                sc = p.fit_transform(Xb)
                r = RidgeCV(alphas=np.logspace(1, 6, 10), cv=3)
                r.fit(sc, Yb)
                return p.components_[:K_b].T @ r.coef_
            def _pls_r_fit(Xb, Yb):
                n_b = min(_n_pls_r, Xb.shape[0] - 1, Xb.shape[1] - 1)
                p = PLSRegression(n_components=n_b, max_iter=500)
                p.fit(Xb, Yb)
                return p.coef_.ravel()
            fpca_r_pvals = bootstrap_pixel_pvalues(X_r, Y_r, _fpca_r_fit, B=B_regime)
            pls_r_pvals = bootstrap_pixel_pvalues(X_r, Y_r, _pls_r_fit, B=B_regime)

            # Correlation with full-sample beta (only if same field)
            corr_vs_full = np.nan
            if field == "wspd_error_1h" and len(pc_r) == len(pc_all):
                try:
                    corr_vs_full = np.corrcoef(fpca_beta_r, full_sample_beta)[0, 1]
                except Exception:
                    pass

            regime_results[regime_name][field] = {
                "fpca": {
                    "r2": fpca_r2.mean(), "r2_std": fpca_r2.std(),
                    "beta": fpca_beta_r, "K": K_r,
                    "sig_mask": fpca_r_pvals < 0.05,
                },
                "pls": {
                    "r2": pls_r2.mean(), "r2_std": pls_r2.std(),
                    "beta": pls_beta_r, "n": n_pls_r,
                    "sig_mask": pls_r_pvals < 0.05,
                },
                "pixel_coords": pc_r,
                "n_hours": n_hours_r,
                "corr_vs_full_sample": corr_vs_full,
            }

            print(f"    FPCA K={K_r}: R²={fpca_r2.mean():.4f} ± {fpca_r2.std():.4f}")
            print(f"    PLS  n={n_pls_r}: R²={pls_r2.mean():.4f} ± {pls_r2.std():.4f}")
            if not np.isnan(corr_vs_full):
                print(f"    β(s) corr vs full-sample: {corr_vs_full:.4f}")

            summary_rows.append({
                "regime": regime_name, "field": field, "method": "FPCA",
                "K_n": K_r, "n_hours": n_hours_r,
                "r2": fpca_r2.mean(), "r2_std": fpca_r2.std(),
                "corr_vs_full": corr_vs_full,
            })
            summary_rows.append({
                "regime": regime_name, "field": field, "method": "PLS",
                "K_n": n_pls_r, "n_hours": n_hours_r,
                "r2": pls_r2.mean(), "r2_std": pls_r2.std(),
                "corr_vs_full": np.nan,
            })

    if not regime_results:
        print("  No regime results — aborting.")
        return {}

    # ── Figures: one per regime ──
    field_labels = {
        "wspd_error_1h": "HRRR 1h\nWind Speed Error",
        "wspd_error_0h": "GFS DA\nWind Speed Error",
        "temp_error_1h": "HRRR 1h\nTemp Error",
        "temp_error_0h": "GFS DA\nTemp Error",
    }

    for regime_name, field_dict in regime_results.items():
        fields_ok = [f for f in error_fields if f in field_dict]
        if not fields_ok:
            continue

        n_rows = len(fields_ok)
        n_cols = 2  # FPCA | PLS
        fig, axes = plt.subplots(
            n_rows, n_cols, figsize=(14, 4.5 * n_rows),
            subplot_kw={"projection": ccrs.PlateCarree()},
            gridspec_kw={"hspace": 0.35, "wspace": 0.05},
        )
        if n_rows == 1:
            axes = axes[np.newaxis, :]

        sc_last = None
        for row_i, field in enumerate(fields_ok):
            res = field_dict[field]
            pc = res["pixel_coords"]
            betas = [res["fpca"]["beta"], res["pls"]["beta"]]
            sig_masks_r = [res["fpca"]["sig_mask"], res["pls"]["sig_mask"]]
            all_b = np.concatenate(betas)
            clim = np.nanpercentile(np.abs(all_b), 98) if all_b.size > 0 else 1.0

            for col_i, (m_label, beta, sig_m) in enumerate(
                zip(["FPCA", "PLS"], betas, sig_masks_r)
            ):
                ax = axes[row_i, col_i]
                r2 = res["fpca"]["r2"] if col_i == 0 else res["pls"]["r2"]
                K_n = res["fpca"]["K"] if col_i == 0 else res["pls"]["n"]
                title = (f"{field_labels.get(field, field)}\n"
                         f"{m_label} ({K_n} comps)  R²={r2:.3f}")
                sc = plot_beta_surface(beta, pc, title, ax,
                                       vmin=-clim, vmax=clim, sig_mask=sig_m)
                sc_last = sc

        if sc_last is not None:
            fig.colorbar(sc_last, ax=axes.ravel().tolist(), shrink=0.5,
                         label="β(s): effect on curtailment MW per σ error",
                         pad=0.02)

        regime_info = regime_results[regime_name]
        n_hrs_example = list(regime_info.values())[0]["n_hours"] if regime_info else 0
        fig.suptitle(
            f"Regime: {REGIMES[regime_name]['label']}\n"
            f"FPCA vs PLS Spatial Coefficients β(s)  "
            f"(≈{n_hrs_example} regime hours, DEPVAR={DEPVAR})  [p < 0.05 only]",
            fontsize=12, y=1.01,
        )
        save_path = fig_dir / f"regime_beta_{regime_name}.png"
        fig.savefig(save_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        print(f"\n  Regime figure saved: {save_path}")

    # ── R² summary figure ──
    if summary_rows:
        sum_df = pd.DataFrame(summary_rows)
        sum_df.to_csv(tables_dir / "regime_stratified_results.csv", index=False)
        print(f"  Summary table saved.")

        fig3, ax3 = plt.subplots(figsize=(14, 5))
        width = 0.2
        regimes_ok = [r for r in regimes if r in regime_results]
        x = np.arange(len(error_fields))
        colors = {"extreme_cold": "#4393c3", "extreme_heat": "#d6604d",
                  "high_wind": "#74c476", "stressed_grid": "#9e9ac8",
                  "full_sample": "#bdbdbd"}
        offsets = np.linspace(-width * (len(regimes_ok)), width * len(regimes_ok),
                              len(regimes_ok) * 2 + 1)

        for i, (regime_name, m_label) in enumerate(
            [(r, m) for r in regimes_ok for m in ["FPCA", "PLS"]]
        ):
            mask = (sum_df["regime"] == regime_name) & (sum_df["method"] == m_label)
            sub = sum_df[mask].set_index("field")
            r2s = [sub.loc[f, "r2"] if f in sub.index else 0 for f in error_fields]
            stds = [sub.loc[f, "r2_std"] if f in sub.index else 0 for f in error_fields]
            label = f"{regime_name[:4]} {m_label}"
            color = colors.get(regime_name, "gray")
            alpha = 0.9 if m_label == "FPCA" else 0.55
            ax3.bar(x + offsets[i], r2s, width, yerr=stds, capsize=3,
                    label=label, color=color, alpha=alpha)

        ax3.set_xticks(x)
        ax3.set_xticklabels([field_labels.get(f, f) for f in error_fields],
                            rotation=20, ha="right", fontsize=9)
        ax3.set_ylabel("CV R²")
        ax3.set_title("R² by Regime × Error Field × Method")
        ax3.legend(ncol=4, fontsize=8)
        fig3.tight_layout()
        fig3.savefig(fig_dir / "regime_r2_comparison.png", dpi=150, bbox_inches="tight")
        plt.close(fig3)
        print(f"  R² comparison figure saved.")

    return regime_results


# ════════════════════════════════════════════════════════════════════════════
# Step 4A — Constrained PLS (spatially smooth loading vectors)
# ════════════════════════════════════════════════════════════════════════════

def _build_spatial_laplacian(pixel_coords, threshold=0.15):
    """Build sparse spatial graph Laplacian from pixel coordinates.

    L = D - A  where A is the adjacency matrix of the nearest-neighbor
    graph (threshold = ~0.15° ≈ 1.5× the ERA5 0.1° grid spacing).

    Returns
    -------
    L : scipy.sparse.csr_matrix, shape (N, N)
    """
    from scipy.sparse import diags, csr_matrix
    from scipy.spatial import cKDTree

    tree = cKDTree(pixel_coords)
    pairs = list(tree.query_pairs(r=threshold))
    N = len(pixel_coords)

    rows, cols, data = [], [], []
    degree = np.zeros(N)
    for i, j in pairs:
        rows += [i, j]
        cols += [j, i]
        data += [1.0, 1.0]
        degree[i] += 1
        degree[j] += 1

    A = csr_matrix((data, (rows, cols)), shape=(N, N))
    D = diags(degree, 0, format="csr")
    L = D - A
    return L


def _constrained_pls_nipals(X, Y, n_components, L_inv, tol=1e-6, max_iter=200):
    """Penalized NIPALS PLS with spatial smoothness on loading vectors.

    Each weight vector w_k is projected through P_inv = (I + λL)^{-1}
    which encourages spatially smooth weights.

    Parameters
    ----------
    X : ndarray (T, N_pixels)
    Y : ndarray (T,)
    n_components : int
    L_inv : ndarray or sparse matrix (N_pixels, N_pixels) — precomputed P^{-1}
    tol : float — convergence tolerance
    max_iter : int

    Returns
    -------
    T_scores : ndarray (T, n_components)
    W_weights : ndarray (N_pixels, n_components)
    P_loadings : ndarray (N_pixels, n_components)
    Q_y : ndarray (n_components,)
    coef : ndarray (N_pixels,) — final regression coefficient in input space
    """
    X_work = X.copy()
    Y_work = Y.copy().reshape(-1, 1)

    T_scores = np.zeros((X.shape[0], n_components))
    W_weights = np.zeros((X.shape[1], n_components))
    P_loadings = np.zeros((X.shape[1], n_components))
    Q_y = np.zeros(n_components)

    for k in range(n_components):
        # Initial weight: X^T y / ||X^T y||
        w = X_work.T @ Y_work.ravel()

        # Apply smoothness projection: w = P_inv @ w
        if hasattr(L_inv, "dot"):
            w = L_inv.dot(w)
        else:
            w = L_inv @ w

        norm_w = np.linalg.norm(w)
        if norm_w < 1e-12:
            break
        w = w / norm_w

        for _ in range(max_iter):
            w_old = w.copy()

            # Score
            t = X_work @ w  # (T,)
            t_norm = t @ t

            if t_norm < 1e-12:
                break

            # Y loading
            q = (Y_work.ravel() @ t) / t_norm

            # X loading
            p = (X_work.T @ t) / t_norm

            # New weight: X^T Y_work t / ||...||
            w_new = X_work.T @ (Y_work.ravel() * (q / t_norm))
            # Actually use the simpler NIPALS update: w ∝ X^T (Y_work @ q) / (q^2)
            # Standard: w_new = X_work.T @ Y_work / (q * t_norm) -- simplified below
            w_new = X_work.T @ Y_work.ravel()
            if hasattr(L_inv, "dot"):
                w_new = L_inv.dot(w_new)
            else:
                w_new = L_inv @ w_new
            norm_new = np.linalg.norm(w_new)
            if norm_new < 1e-12:
                break
            w = w_new / norm_new

            if np.linalg.norm(w - w_old) < tol:
                break

        t = X_work @ w
        t_norm2 = t @ t
        if t_norm2 < 1e-12:
            break
        p = X_work.T @ t / t_norm2
        q = Y_work.ravel() @ t / t_norm2

        T_scores[:, k] = t
        W_weights[:, k] = w
        P_loadings[:, k] = p
        Q_y[k] = q

        # Deflate
        X_work -= np.outer(t, p)
        Y_work -= (t * q).reshape(-1, 1)

    # Regression coefficient in original X space
    # coef = W (P^T W)^{-1} Q^T
    PtW = P_loadings.T @ W_weights  # (n_components, n_components)
    try:
        coef = W_weights @ np.linalg.solve(PtW, Q_y)
    except np.linalg.LinAlgError:
        coef = W_weights @ np.linalg.lstsq(PtW, Q_y, rcond=None)[0]

    return T_scores, W_weights, P_loadings, Q_y, coef


def run_constrained_pls(X, Y, pixel_coords,
                         lambda_values=None,
                         n_comp_list=None,
                         n_folds=N_CV_FOLDS):
    """Constrained PLS with spatial Laplacian smoothness penalty.

    For each (lambda, n_components), fits penalized PLS where loading
    vectors are projected through (I + λL)^{-1} to encourage smoothness.
    λ=0 recovers standard PLS.

    Returns
    -------
    dict: {(lambda, n_comp): {'r2', 'r2_std', 'beta', 'tv'}}
    """
    from scipy.sparse.linalg import spsolve
    from scipy.sparse import eye as speye

    if lambda_values is None:
        lambda_values = [0.0, 0.01, 0.1, 1.0, 10.0]
    if n_comp_list is None:
        n_comp_list = [5, 10, 20, 50]

    n_comp_list = [k for k in n_comp_list if k < min(X.shape)]

    print("\n" + "=" * 70)
    print("STEP 4A: CONSTRAINED PLS (Spatially Smooth Loading Vectors)")
    print("=" * 70)

    # Build Laplacian
    L = _build_spatial_laplacian(pixel_coords)
    N = X.shape[1]
    print(f"  Laplacian: {N}×{N}, {L.nnz} nonzeros")

    kf = KFold(n_splits=n_folds, shuffle=True, random_state=RANDOM_STATE)
    results = {}

    for lam in lambda_values:
        # Precompute P_inv = (I + lam*L)^{-1} for this lambda
        if lam == 0.0:
            # Standard PLS: P_inv = identity (use sklearn for speed)
            from sklearn.cross_decomposition import PLSRegression as _PLS
            for n_comp in n_comp_list:
                n_c = min(n_comp, min(X.shape) - 1)
                pls = _PLS(n_components=n_c, max_iter=500)
                r2_folds = cross_val_score(pls, X, Y, cv=kf, scoring="r2")
                pls.fit(X, Y)
                beta = pls.coef_.ravel()
                tv = float(np.abs(np.diff(beta)).mean())
                results[(lam, n_comp)] = {
                    "r2": r2_folds.mean(), "r2_std": r2_folds.std(),
                    "beta": beta, "tv": tv,
                }
                print(f"  λ={lam:.2f}, n={n_comp:3d}: "
                      f"R²={r2_folds.mean():.4f} ± {r2_folds.std():.4f}  TV={tv:.4f}")
            continue

        # P = I + lam*L  (sparse)
        P = speye(N, format="csr") + lam * L

        # P_inv is dense N×N — too large to store directly.
        # Instead we represent the action: v ↦ spsolve(P, v)
        # We'll use a simple wrapper for _constrained_pls_nipals
        class SparseLinOp:
            def __init__(self, P_sparse):
                self.P = P_sparse
            def dot(self, v):
                return spsolve(self.P, v)

        L_inv_op = SparseLinOp(P)

        for n_comp in n_comp_list:
            n_c = min(n_comp, min(X.shape) - 1)
            # Cross-validate manually
            r2_folds = []
            for train_idx, test_idx in kf.split(X):
                X_tr, Y_tr = X[train_idx], Y[train_idx]
                X_te, Y_te = X[test_idx], Y[test_idx]
                try:
                    _, _, _, _, coef = _constrained_pls_nipals(
                        X_tr, Y_tr, n_c, L_inv_op
                    )
                    Y_pred = X_te @ coef
                    ss_res = ((Y_te - Y_pred) ** 2).sum()
                    ss_tot = ((Y_te - Y_te.mean()) ** 2).sum()
                    r2 = 1 - ss_res / (ss_tot + 1e-10)
                    r2_folds.append(r2)
                except Exception:
                    r2_folds.append(np.nan)

            r2_folds = np.array(r2_folds)
            r2_mean = np.nanmean(r2_folds)
            r2_std = np.nanstd(r2_folds)

            # Fit on full data for beta
            _, _, _, _, beta = _constrained_pls_nipals(X, Y, n_c, L_inv_op)
            tv = float(np.abs(np.diff(beta)).mean())

            results[(lam, n_comp)] = {
                "r2": r2_mean, "r2_std": r2_std,
                "beta": beta, "tv": tv,
            }
            print(f"  λ={lam:.2f}, n={n_comp:3d}: "
                  f"R²={r2_mean:.4f} ± {r2_std:.4f}  TV={tv:.4f}")

    return results


def plot_constrained_pls_results(results, pixel_coords, fig_dir):
    """Plot lambda sweep and selected beta surfaces for constrained PLS."""
    fig_dir = Path(fig_dir)

    lambdas = sorted(set(lam for lam, _ in results.keys()))
    n_comps = sorted(set(n for _, n in results.keys()))

    # ── R² vs n_components for each lambda ──
    fig1, ax1 = plt.subplots(figsize=(9, 5))
    colors = plt.cm.viridis(np.linspace(0, 1, len(lambdas)))
    for color, lam in zip(colors, lambdas):
        r2s = [results.get((lam, n), {}).get("r2", np.nan) for n in n_comps]
        stds = [results.get((lam, n), {}).get("r2_std", 0) for n in n_comps]
        ax1.errorbar(n_comps, r2s, yerr=stds, marker="o", color=color,
                     label=f"λ={lam}", capsize=3)
    ax1.set_xlabel("Number of PLS Components")
    ax1.set_ylabel("CV R²")
    ax1.set_title("Constrained PLS: R² vs Components by Smoothness λ")
    ax1.legend(title="Smoothness λ")
    fig1.tight_layout()
    path1 = fig_dir / "constrained_pls_lambda_sweep.png"
    fig1.savefig(path1, dpi=150, bbox_inches="tight")
    plt.close(fig1)
    print(f"  Lambda sweep saved: {path1}")

    # ── Best beta surface for each lambda (at best n_components) ──
    best_betas = {}
    for lam in lambdas:
        best_key = max(
            [(lam, n) for n in n_comps if (lam, n) in results],
            key=lambda k: results[k].get("r2", -np.inf),
            default=None,
        )
        if best_key is not None:
            best_betas[f"λ={best_key[0]} n={best_key[1]}"] = results[best_key]["beta"]

    if len(best_betas) > 1:
        ncols = min(3, len(best_betas))
        nrows = (len(best_betas) + ncols - 1) // ncols
        fig2, axes2 = plt.subplots(
            nrows, ncols, figsize=(6 * ncols, 5 * nrows),
            subplot_kw={"projection": ccrs.PlateCarree()},
        )
        if nrows == 1 and ncols == 1:
            axes2 = np.array([[axes2]])
        elif nrows == 1:
            axes2 = axes2[np.newaxis, :]

        all_betas = np.concatenate(list(best_betas.values()))
        clim = np.nanpercentile(np.abs(all_betas), 98)
        sc_last = None
        for idx, (label, beta) in enumerate(best_betas.items()):
            r, c = divmod(idx, ncols)
            sc = plot_beta_surface(beta, pixel_coords, label, axes2[r, c],
                                   vmin=-clim, vmax=clim)
            sc_last = sc
        for idx in range(len(best_betas), nrows * ncols):
            r, c = divmod(idx, ncols)
            axes2[r, c].set_visible(False)
        if sc_last is not None:
            fig2.colorbar(sc_last, ax=axes2.ravel().tolist(), shrink=0.5,
                          label="β(s)")
        fig2.suptitle("Constrained PLS: Best β(s) per Smoothness Level", fontsize=12)
        fig2.tight_layout()
        path2 = fig_dir / "constrained_pls_beta.png"
        fig2.savefig(path2, dpi=150, bbox_inches="tight")
        plt.close(fig2)
        print(f"  Beta surfaces saved: {path2}")


# ════════════════════════════════════════════════════════════════════════════
# Step 4B — Neural Operator (FNO-to-scalar)
# ════════════════════════════════════════════════════════════════════════════

def _build_era5_grid_index(pixel_coords, lat_resolution=0.1, lon_resolution=0.1):
    """Map each pixel (lat, lon) to a (row, col) index on the ERA5 grid.

    Returns
    -------
    lat_grid : ndarray of unique sorted latitudes
    lon_grid : ndarray of unique sorted longitudes
    lat_idx : ndarray (N_pixels,) int
    lon_idx : ndarray (N_pixels,) int
    H, W : grid height and width
    """
    lats = pixel_coords[:, 0]
    lons = pixel_coords[:, 1]

    lat_grid = np.array(sorted(set(np.round(lats, 1))))
    lon_grid = np.array(sorted(set(np.round(lons, 1))))
    H, W = len(lat_grid), len(lon_grid)

    lat_to_idx = {lat: i for i, lat in enumerate(lat_grid)}
    lon_to_idx = {lon: j for j, lon in enumerate(lon_grid)}

    lat_idx = np.array([lat_to_idx[round(lat, 1)] for lat in lats])
    lon_idx = np.array([lon_to_idx[round(lon, 1)] for lon in lons])

    return lat_grid, lon_grid, lat_idx, lon_idx, H, W


def prepare_grid_data(X_dict, pixel_coords):
    """Reshape multiple error field arrays to (T, C, H, W) tensor.

    Parameters
    ----------
    X_dict : dict {field_name: ndarray (T, N_pixels)} — already standardized
    pixel_coords : ndarray (N_pixels, 2)

    Returns
    -------
    grid_tensor : ndarray (T, C, H, W)
    """
    _, _, lat_idx, lon_idx, H, W = _build_era5_grid_index(pixel_coords)
    T = next(iter(X_dict.values())).shape[0]
    C = len(X_dict)
    grid_tensor = np.zeros((T, C, H, W), dtype=np.float32)

    for c, (field, X) in enumerate(X_dict.items()):
        grid_tensor[:, c, lat_idx, lon_idx] = X.astype(np.float32)

    return grid_tensor


def run_neural_operator(df, months_label="6mo",
                         n_folds=N_CV_FOLDS,
                         n_epochs=100,
                         patience=15,
                         batch_size=64,
                         lr=1e-3,
                         n_modes=16,
                         hidden_dim=64,
                         n_fno_layers=4,
                         downsample_stride=4):
    """Train FNO-to-scalar, CNN-to-scalar, and MLP-to-scalar on the
    multi-channel error grid and compare CV R² with linear methods.

    Uses the NeuralOperator 2.0.0 library (neuralop) for the FNO
    architecture, following the guide by Duruisseaux, Kossaifi & Anandkumar
    (arXiv:2512.01421v2). Key design choices:
      - neuralop.models.FNO with domain_padding for non-periodic spatial data
      - GELU activation (default), linear skip connections, ChannelMLP
      - n_modes respecting Nyquist limit (≤ N/2 per spatial dim)
      - AdamW optimizer with cosine annealing

    Parameters
    ----------
    df : pd.DataFrame from load_pixel_data_with_regimes (or load_pixel_data)
    months_label : str — label for saved files
    n_folds : int
    n_epochs : int
    patience : int — early-stopping patience
    batch_size : int
    lr : float — AdamW learning rate
    n_modes : int — FNO Fourier modes in each dimension (clamped to Nyquist)
    hidden_dim : int — channel width in FNO / CNN
    n_fno_layers : int — number of Fourier layers (recommended 3–6)

    Returns
    -------
    dict: architecture → {'r2_folds', 'r2', 'r2_std'}
    """
    try:
        import torch
        import torch.nn as nn
        import torch.nn.functional as F
        from torch.utils.data import TensorDataset, DataLoader
        from neuralop.models import FNO
    except ImportError:
        print("  torch/neuralop not available — skipping neural operator.")
        return {}

    print("\n" + "=" * 70)
    print("STEP 4B: NEURAL OPERATOR (neuralop FNO / CNN / MLP)")
    print("=" * 70)

    dirs = setup_directories()
    fig_dir = Path(dirs["figures"]) / "functional_analysis"
    fig_dir.mkdir(parents=True, exist_ok=True)
    tables_dir = Path(dirs["tables"])

    # ── Prepare multi-field grid data ──
    print("\n  Preparing multi-field functional data...")
    X_dict = {}
    Y_ref = None
    pixel_coords_ref = None
    for field in ERROR_FIELDS:
        try:
            X, Y, pc, _, _ = prepare_functional_data(df, error_field=field)
            X_dict[field] = X
            if Y_ref is None:
                Y_ref = Y
                pixel_coords_ref = pc
        except Exception as e:
            print(f"  Skipping {field}: {e}")

    if len(X_dict) == 0 or Y_ref is None:
        print("  No valid fields — aborting.")
        return {}

    ref_X = X_dict.get("wspd_error_1h")
    if ref_X is None:
        ref_X = next(iter(X_dict.values()))
    T = ref_X.shape[0]

    X_dict_aligned = {f: X for f, X in X_dict.items() if X.shape[0] == T}
    if len(X_dict_aligned) == 0:
        X_dict_aligned = {"wspd_error_1h": ref_X}

    print(f"  Fields: {list(X_dict_aligned.keys())}")
    grid_np = prepare_grid_data(X_dict_aligned, pixel_coords_ref)
    if downsample_stride > 1:
        grid_np = grid_np[:, :, ::downsample_stride, ::downsample_stride]
        print(f"  Downsampled grid: {grid_np.shape} (stride={downsample_stride})")
    T_actual, C, H, W = grid_np.shape
    Y_np = Y_ref[:T_actual].astype(np.float32)
    Y_mean, Y_std = Y_np.mean(), Y_np.std() + 1e-8
    Y_norm = (Y_np - Y_mean) / Y_std

    # Clamp n_modes to respect Nyquist limit (Section 4.2 of the guide)
    n_modes_h = min(n_modes, H // 2)
    n_modes_w = min(n_modes, W // 2)
    print(f"  Grid shape: {grid_np.shape}  (T={T_actual}, C={C}, H={H}, W={W})")
    print(f"  Fourier modes: ({n_modes_h}, {n_modes_w})  "
          f"[Nyquist: H/2={H//2}, W/2={W//2}]")

    # ── Define models ──

    class FNOScalar(nn.Module):
        """FNO-to-scalar using neuralop.models.FNO.

        Uses the FNO trunk for spectral processing (function → function),
        then global average pooling over spatial dims to reduce to a
        per-sample feature vector, followed by an MLP projection head.

        Architecture follows NeuralOperator 2.0.0 best practices:
          - domain_padding=0.1 for non-periodic ERCOT spatial domain
          - GELU nonlinearity (default)
          - linear skip connections in spectral path
          - soft-gating skip in ChannelMLP path
          - instance_norm for training stability
        """
        def __init__(self):
            super().__init__()
            self.fno = FNO(
                n_modes=(n_modes_h, n_modes_w),
                in_channels=C,
                out_channels=hidden_dim,
                hidden_channels=hidden_dim,
                n_layers=n_fno_layers,
                domain_padding=0.1,
                norm="instance_norm",
                use_channel_mlp=True,
                channel_mlp_expansion=0.5,
                fno_skip="linear",
                channel_mlp_skip="soft-gating",
            )
            self.head = nn.Sequential(
                nn.Linear(hidden_dim, 64),
                nn.GELU(),
                nn.Linear(64, 1),
            )

        def forward(self, x):
            # FNO: (B, C_in, H, W) → (B, hidden_dim, H, W)
            x = self.fno(x)
            # Global average pool → (B, hidden_dim)
            x = x.mean(dim=(-2, -1))
            return self.head(x).squeeze(-1)

    class CNNScalar(nn.Module):
        def __init__(self):
            super().__init__()
            self.net = nn.Sequential(
                nn.Conv2d(C, hidden_dim, 3, padding=1), nn.GELU(),
                nn.Conv2d(hidden_dim, hidden_dim, 3, padding=1), nn.GELU(),
                nn.Conv2d(hidden_dim, hidden_dim // 2, 3, padding=1), nn.GELU(),
            )
            self.head = nn.Sequential(
                nn.Linear(hidden_dim // 2, 32), nn.GELU(), nn.Linear(32, 1)
            )

        def forward(self, x):
            x = self.net(x).mean(dim=(-2, -1))
            return self.head(x).squeeze(-1)

    class MLPScalar(nn.Module):
        def __init__(self):
            super().__init__()
            self.net = nn.Sequential(
                nn.Linear(C * H * W, 256), nn.GELU(),
                nn.Linear(256, 256), nn.GELU(),
                nn.Linear(256, 64), nn.GELU(),
                nn.Linear(64, 1),
            )

        def forward(self, x):
            return self.net(x.view(x.shape[0], -1)).squeeze(-1)

    architectures = {
        "FNO": FNOScalar,
        "CNN": CNNScalar,
        "MLP": MLPScalar,
    }

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"  Device: {device}")

    grid_t = torch.tensor(grid_np, dtype=torch.float32)
    Y_t = torch.tensor(Y_norm, dtype=torch.float32)

    kf = KFold(n_splits=n_folds, shuffle=True, random_state=RANDOM_STATE)
    arch_results = {}

    def _count_params(model):
        return sum(p.numel() for p in model.parameters() if p.requires_grad)

    def train_eval(ModelClass, train_idx, test_idx, fold_i):
        model = ModelClass().to(device)
        if fold_i == 0:
            print(f"    Parameters: {_count_params(model):,}")
        optimizer = torch.optim.AdamW(model.parameters(), lr=lr,
                                      weight_decay=1e-4)
        scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(
            optimizer, T_max=n_epochs
        )
        X_tr = grid_t[train_idx].to(device)
        Y_tr = Y_t[train_idx].to(device)
        X_te = grid_t[test_idx].to(device)
        Y_te = Y_t[test_idx].to(device)

        ds_tr = TensorDataset(X_tr, Y_tr)
        loader_tr = DataLoader(ds_tr, batch_size=batch_size, shuffle=True)

        best_val_loss = float("inf")
        best_state = None
        patience_count = 0

        for epoch in range(n_epochs):
            model.train()
            for xb, yb in loader_tr:
                optimizer.zero_grad()
                pred = model(xb)
                loss = F.mse_loss(pred, yb)
                loss.backward()
                nn.utils.clip_grad_norm_(model.parameters(), 1.0)
                optimizer.step()
            scheduler.step()

            model.eval()
            with torch.no_grad():
                val_loss = F.mse_loss(model(X_te), Y_te).item()

            if val_loss < best_val_loss - 1e-5:
                best_val_loss = val_loss
                best_state = {k: v.clone() if hasattr(v, 'clone') else copy.deepcopy(v)
                              for k, v in model.state_dict().items()
                              if k != '_metadata'}
                patience_count = 0
            else:
                patience_count += 1
                if patience_count >= patience:
                    break

        if best_state is not None:
            model.load_state_dict(best_state, strict=False)

        model.eval()
        with torch.no_grad():
            Y_pred = model(X_te).cpu().numpy()
            Y_true = Y_te.cpu().numpy()

        ss_res = ((Y_true - Y_pred) ** 2).sum()
        ss_tot = ((Y_true - Y_true.mean()) ** 2).sum()
        r2 = 1 - ss_res / (ss_tot + 1e-10)
        return float(r2)

    for arch_name, ModelClass in architectures.items():
        print(f"\n  Architecture: {arch_name}")
        r2_folds = []
        for fold_i, (train_idx, test_idx) in enumerate(kf.split(grid_t)):
            print(f"    Fold {fold_i + 1}/{n_folds}...", end=" ", flush=True)
            try:
                r2 = train_eval(ModelClass, train_idx, test_idx, fold_i)
                print(f"R²={r2:.4f}")
            except Exception as e:
                print(f"Error: {e}")
                r2 = np.nan
            r2_folds.append(r2)

        r2_arr = np.array(r2_folds)
        arch_results[arch_name] = {
            "r2_folds": r2_arr,
            "r2": float(np.nanmean(r2_arr)),
            "r2_std": float(np.nanstd(r2_arr)),
        }
        print(f"  {arch_name}: CV R²={arch_results[arch_name]['r2']:.4f} "
              f"± {arch_results[arch_name]['r2_std']:.4f}")

    # ── Summary figure ──
    arch_names = list(arch_results.keys())
    r2s = [arch_results[n]["r2"] for n in arch_names]
    stds = [arch_results[n]["r2_std"] for n in arch_names]

    fig, ax = plt.subplots(figsize=(9, 5))
    colors = ["steelblue", "mediumseagreen", "darkorange"]
    ax.bar(arch_names, r2s, yerr=stds, capsize=5, color=colors[:len(arch_names)],
           alpha=0.85, edgecolor="black", linewidth=0.5)
    for i, (r2, std) in enumerate(zip(r2s, stds)):
        ax.text(i, r2 + std + 0.005, f"{r2:.3f}", ha="center", fontsize=10)
    ax.set_ylabel("CV R² (5-fold)")
    ax.set_title(
        f"Neural Operator vs Linear Baselines\n"
        f"(C={C} channels, H={H}×W={W} grid, T={T_actual} hours)"
    )
    ax.axhline(0, color="black", linewidth=0.5)
    fig.tight_layout()
    save_path = fig_dir / f"neural_operator_comparison_{months_label}.png"
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"\n  Neural operator comparison saved: {save_path}")

    # Save results table
    rows = [{"architecture": k, "r2": v["r2"], "r2_std": v["r2_std"]}
            for k, v in arch_results.items()]
    pd.DataFrame(rows).to_csv(
        tables_dir / f"neural_operator_results_{months_label}.csv", index=False
    )

    return arch_results


# ── Main extensions orchestrator ─────────────────────────────────────────────

def run_extensions(months=None, primary_field="wspd_error_1h"):
    """Run all analysis extensions and save results + figures.

    Extensions:
      A. PLS (supervised basis — comparison to FPCA)
      B. Seasonal FPCA (winter/spring vs summer/fall)
      C. Quantile regression on FPCA scores (tail behaviour)
      D. Pixel stability analysis (which pixels are consistently identified)
    """
    if months is None:
        months = DEFAULT_MONTHS

    dirs = setup_directories()
    fig_dir = Path(dirs["figures"]) / "functional_analysis"
    fig_dir.mkdir(parents=True, exist_ok=True)
    tables_dir = Path(dirs["tables"])

    print("=" * 70)
    print("ANALYSIS EXTENSIONS")
    print("=" * 70)

    # Load data (column-filtered)
    print("\n=== Loading Data ===")
    df = load_pixel_data(months)
    X, Y, pixel_coords, pixel_ids, hour_index = prepare_functional_data(
        df, error_field=primary_field
    )

    ext_results = {}

    # ── A: PLS ──
    pls_res = run_pls_analysis(X, Y, pixel_coords)
    best_pls_n = pls_res["best_n"]
    ext_results["PLS"] = {
        "r2": pls_res["results_by_n"][best_pls_n]["r2"],
        "r2_std": pls_res["results_by_n"][best_pls_n]["r2_std"],
        "best_n": best_pls_n,
    }

    # Compare PLS vs FPCA K=100 betas
    pca_ref = PCA(n_components=100, random_state=RANDOM_STATE)
    scores_ref = pca_ref.fit_transform(X)
    ridge_ref = RidgeCV(alphas=np.logspace(1, 6, 20))
    ridge_ref.fit(scores_ref, Y)
    fpca100_beta = pca_ref.components_[:100].T @ ridge_ref.coef_
    pls_best_beta = pls_res["results_by_n"][best_pls_n]["beta_spatial"]

    # Bootstrap significance for PLS vs FPCA comparison
    from sklearn.cross_decomposition import PLSRegression as _PLSReg
    print("\n  Bootstrap significance for PLS vs FPCA (B=100)...")
    _n_pls_ext = best_pls_n
    def _pls_ext_fit(Xb, Yb):
        n_b = min(_n_pls_ext, Xb.shape[0] - 1, Xb.shape[1] - 1)
        p = _PLSReg(n_components=n_b, max_iter=500)
        p.fit(Xb, Yb)
        return p.coef_.ravel()

    def _fpca100_ext_fit(Xb, Yb):
        K_b = min(100, Xb.shape[0] - 1, Xb.shape[1] - 1)
        p = PCA(n_components=K_b, random_state=RANDOM_STATE)
        sc = p.fit_transform(Xb)
        r = RidgeCV(alphas=np.logspace(1, 6, 10), cv=3)
        r.fit(sc, Yb)
        return p.components_[:K_b].T @ r.coef_

    pls_ext_pvals = bootstrap_pixel_pvalues(X, Y, _pls_ext_fit, B=100)
    fpca100_pvals = bootstrap_pixel_pvalues(X, Y, _fpca100_ext_fit, B=100)

    corr_pls_fpca = plot_pls_vs_fpca(
        pls_best_beta, fpca100_beta, pixel_coords,
        fig_dir / "pls_vs_fpca.png",
        pls_r2=pls_res["results_by_n"][best_pls_n]["r2"],
        fpca_r2=ext_results.get("fpca100_r2"),
        pls_sig_mask=pls_ext_pvals < 0.05,
        fpca_sig_mask=fpca100_pvals < 0.05,
    )
    ext_results["PLS_FPCA_correlation"] = float(corr_pls_fpca)

    # PLS vs FPCA comparison table for all K
    pls_rows = []
    for n_c, res in pls_res["results_by_n"].items():
        pls_rows.append({
            "n_components": n_c, "method": "PLS",
            "r2": res["r2"], "r2_std": res["r2_std"],
            "coef_std": res["coef_std"],
        })
    pd.DataFrame(pls_rows).to_csv(
        tables_dir / "pls_results.csv", index=False
    )
    print(f"  PLS results saved.")

    # ── B: Quantile regression ──
    quant_res = run_quantile_fpca(X, Y, pixel_coords, K=100)
    ext_results["quantile"] = {
        str(tau): {"pseudo_r2": res["pseudo_r2"]}
        for tau, res in quant_res.items()
    }
    plot_quantile_betas(quant_res, pixel_coords,
                        fig_dir / "quantile_betas.png")

    # ── C: Stability ──
    stab_res = run_pixel_stability(X, Y, pixel_coords, K=100)
    ext_results["stability"] = {
        "mean_stability": float(stab_res["stability"].mean()),
        "pct_highly_stable": float(
            (stab_res["stability"] > np.percentile(stab_res["stability"], 90)).mean()
        ),
    }
    plot_stability_map(stab_res, pixel_coords,
                       fig_dir / "pixel_stability.png")

    # Save top stable pixels table
    stable_rows = [
        {"lat": pixel_coords[i, 0], "lon": pixel_coords[i, 1],
         "mean_beta": stab_res["mean_beta"][i],
         "std_beta": stab_res["std_beta"][i],
         "stability": stab_res["stability"][i]}
        for i in range(len(pixel_coords))
    ]
    stable_df = pd.DataFrame(stable_rows).sort_values("stability", ascending=False)
    stable_df.to_csv(tables_dir / "pixel_stability.csv", index=False)
    print(f"  Pixel stability table saved.")

    # ── PLS K-selection plot ──
    pls_ns = sorted(pls_res["results_by_n"].keys())
    pls_r2s = [pls_res["results_by_n"][k]["r2"] for k in pls_ns]
    fpca_r2s_match = []
    pca_full = PCA(n_components=max(pls_ns), random_state=RANDOM_STATE)
    scores_full = pca_full.fit_transform(X)
    kf_plot = KFold(n_splits=N_CV_FOLDS, shuffle=True, random_state=RANDOM_STATE)
    for k in pls_ns:
        r2 = cross_val_score(
            RidgeCV(alphas=np.logspace(1, 6, 20)),
            scores_full[:, :k], Y, cv=kf_plot, scoring="r2"
        ).mean()
        fpca_r2s_match.append(r2)

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.plot(pls_ns, pls_r2s, "o-", color="steelblue", label="PLS")
    ax.plot(pls_ns, fpca_r2s_match, "s--", color="coral", label="FPCA")
    ax.set_xlabel("Number of Components")
    ax.set_ylabel("CV R²")
    ax.set_title("PLS vs FPCA: Predictive Power vs Dimensionality")
    ax.legend()
    fig.tight_layout()
    fig.savefig(fig_dir / "pls_vs_fpca_k_selection.png",
                dpi=150, bbox_inches="tight")
    plt.close(fig)

    # Append to JSON
    json_path = tables_dir / "functional_analysis_results.json"
    if json_path.exists():
        with open(json_path) as f:
            existing = json.load(f)
    else:
        existing = {}
    existing["extensions"] = ext_results
    with open(json_path, "w") as f:
        json.dump(existing, f, indent=2)
    print(f"\n  Extensions saved to JSON.")

    print("\n" + "=" * 70)
    print("EXTENSIONS COMPLETE")
    print("=" * 70)

    return ext_results, stab_res, quant_res, pls_res


# ════════════════════════════════════════════════════════════════════════════
# Step 5 — High-resolution Neural Operator (MLP + FNO, MPS-accelerated)
# ════════════════════════════════════════════════════════════════════════════

import os as _os
_os.environ.setdefault("PYTORCH_ENABLE_MPS_FALLBACK", "1")


def _select_device():
    """Select best available device: CUDA > MPS (Apple Silicon) > CPU."""
    import torch
    if torch.cuda.is_available():
        return torch.device("cuda")
    if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        try:
            _x = torch.randn(4, 4, device="mps")
            _ = _x[torch.tensor([0, 1], device="mps")]
            return torch.device("mps")
        except Exception as _e:
            print(f"  MPS smoke-test failed ({_e}), falling back to CPU.")
    return torch.device("cpu")


def prepare_grid_data_coarse(X_dict, pixel_coords, target_res=0.25):
    """Regrid infrastructure-pixel error fields to a coarser regular grid.

    Instead of placing pixels on the native 0.1° ERA5 grid, aggregates all
    infrastructure pixels into target_res° target cells using the mean.
    Empty cells (no infrastructure) are left as 0.

    Parameters
    ----------
    X_dict : dict {field: ndarray (T, N_pixels)}
    pixel_coords : ndarray (N_pixels, 2) — (lat, lon)
    target_res : float — output grid spacing in degrees (default 0.25)

    Returns
    -------
    grid : ndarray (T, C, H_out, W_out)
    lat_grid : 1D array of output latitudes
    lon_grid : 1D array of output longitudes
    """
    lats, lons = pixel_coords[:, 0], pixel_coords[:, 1]

    # Build target grid
    lat_min = np.floor(lats.min() / target_res) * target_res
    lat_max = np.ceil(lats.max() / target_res) * target_res
    lon_min = np.floor(lons.min() / target_res) * target_res
    lon_max = np.ceil(lons.max() / target_res) * target_res

    lat_grid = np.arange(lat_min, lat_max + target_res * 0.5, target_res)
    lon_grid = np.arange(lon_min, lon_max + target_res * 0.5, target_res)
    H, W = len(lat_grid), len(lon_grid)

    # Assign each pixel to nearest target cell
    lat_idx = np.round((lats - lat_min) / target_res).astype(int).clip(0, H - 1)
    lon_idx = np.round((lons - lon_min) / target_res).astype(int).clip(0, W - 1)

    T = next(iter(X_dict.values())).shape[0]
    C = len(X_dict)
    grid = np.zeros((T, C, H, W), dtype=np.float32)
    count = np.zeros((C, H, W), dtype=np.float32)

    for c, X in enumerate(X_dict.values()):
        np.add.at(grid[:, c], (slice(None), lat_idx, lon_idx), X.astype(np.float32))
        for i, (li, lj) in enumerate(zip(lat_idx, lon_idx)):
            count[c, li, lj] += 1

    # Divide by count (mean over pixels in each cell)
    nonzero = count > 0
    for c in range(C):
        grid[:, c, nonzero[c]] /= count[c, nonzero[c]]

    return grid, lat_grid, lon_grid


def run_neural_operator_hires(
    df,
    months_label="6mo",
    target_res=0.25,
    n_folds=5,
    n_epochs=150,
    patience=20,
    batch_size=32,
    lr=5e-4,
    weight_decay=1e-4,
    # FNO hyperparams
    fno_modes=16,
    fno_hidden=64,
    fno_layers=4,
    # MLP hyperparams
    mlp_hidden=512,
    mlp_dropout=0.3,
):
    """High-resolution MLP + FNO on a 0.25° regridded error field.

    Uses neuralop.models.FNO (NeuralOperator 2.0.0) for the FNO architecture.
    MPS (Apple Silicon) or CUDA if available; FNO stays on CPU (rfft2).

    Parameters
    ----------
    df : DataFrame from load_pixel_data (all 4 fields available)
    target_res : float — coarse grid resolution in degrees
    n_folds : int — number of CV folds
    n_epochs : int — maximum training epochs
    patience : int — early-stopping patience (epochs)
    batch_size : int
    lr : float — Adam learning rate
    weight_decay : float — L2 regularization
    fno_modes : int — Fourier modes per dimension in FNO
    fno_hidden : int — channel width in FNO layers
    fno_layers : int — number of FNO spectral layers
    mlp_hidden : int — hidden dim in MLP (first layer)
    mlp_dropout : float — dropout probability in MLP
    """
    try:
        import torch
        import torch.nn as nn
        import torch.nn.functional as F
        from torch.utils.data import TensorDataset, DataLoader
    except ImportError:
        print("  torch not available — skipping.")
        return {}

    print("\n" + "=" * 70)
    print("STEP 5: HIGH-RESOLUTION NEURAL OPERATOR (MLP + FNO)")
    print(f"  Target resolution: {target_res}°  |  Folds: {n_folds}")
    print("=" * 70)

    device = _select_device()
    print(f"  Device: {device}")

    dirs = setup_directories()
    fig_dir = Path(dirs["figures"]) / "functional_analysis"
    fig_dir.mkdir(parents=True, exist_ok=True)
    tables_dir = Path(dirs["tables"])

    # ── Prepare multi-field data ─────────────────────────────────────────
    print("\n  Preparing 2-channel error grid (wind + temp, HRRR 1h)...")
    X_dict_raw = {}
    Y_ref, pc_ref = None, None
    for field in ["wspd_error_1h", "temp_error_1h"]:
        try:
            X, Y, pc, _, _ = prepare_functional_data(df, error_field=field)
            X_dict_raw[field] = X
            if Y_ref is None:
                Y_ref, pc_ref = Y, pc
        except Exception as e:
            print(f"  Skipping {field}: {e}")

    if len(X_dict_raw) < 2:
        print("  Need both wind and temp fields — aborting.")
        return {}

    # Align T
    T_min = min(v.shape[0] for v in X_dict_raw.values())
    X_dict_aligned = {k: v[:T_min] for k, v in X_dict_raw.items()}
    Y_np = Y_ref[:T_min].astype(np.float32)

    # ── Coarse-grid regridding ────────────────────────────────────────────
    grid_np, lat_g, lon_g = prepare_grid_data_coarse(
        X_dict_aligned, pc_ref, target_res=target_res
    )
    T, C, H, W = grid_np.shape
    print(f"  Grid: ({T}, {C}, {H}, {W})  "
          f"lat=[{lat_g[0]:.2f}..{lat_g[-1]:.2f}]  "
          f"lon=[{lon_g[0]:.2f}..{lon_g[-1]:.2f}]")
    print(f"  Cells: {H}×{W}={H*W}  (native 0.1° had {pc_ref.shape[0]} infra pixels)")

    Y_mean, Y_std = Y_np.mean(), Y_np.std() + 1e-8
    Y_norm = (Y_np - Y_mean) / Y_std

    # ── Define architectures ─────────────────────────────────────────────
    # Uses neuralop.models.FNO (NeuralOperator 2.0.0) following the guide
    # by Duruisseaux, Kossaifi & Anandkumar (arXiv:2512.01421v2).

    from neuralop.models import FNO as NeuralOpFNO

    # Clamp modes to Nyquist limit (Section 4.2)
    fno_modes_h = min(fno_modes, H // 2)
    fno_modes_w = min(fno_modes, W // 2)
    print(f"  Fourier modes: ({fno_modes_h}, {fno_modes_w})  "
          f"[Nyquist: H/2={H//2}, W/2={W//2}]")

    class FNOScalarHires(nn.Module):
        """FNO-to-scalar using neuralop.models.FNO.

        Architecture: FNO trunk (spatial → spatial) + global avg pool + MLP head.
        Key settings per the FNO guide:
          - domain_padding=0.1 for non-periodic ERCOT domain (Section 3.6)
          - instance_norm for training stability (Section 4.5)
          - linear skip for spectral path, soft-gating for ChannelMLP (Section 3.4.4)
          - GELU nonlinearity (Section 4.5, default)
        """
        def __init__(self):
            super().__init__()
            self.fno = NeuralOpFNO(
                n_modes=(fno_modes_h, fno_modes_w),
                in_channels=C,
                out_channels=fno_hidden,
                hidden_channels=fno_hidden,
                n_layers=fno_layers,
                domain_padding=0.1,
                norm="instance_norm",
                use_channel_mlp=True,
                channel_mlp_expansion=0.5,
                fno_skip="linear",
                channel_mlp_skip="soft-gating",
            )
            self.head = nn.Sequential(
                nn.Linear(fno_hidden, 64),
                nn.GELU(),
                nn.Linear(64, 1),
            )

        def forward(self, x):
            x = self.fno(x)
            x = x.mean(dim=(-2, -1))
            return self.head(x).squeeze(-1)

    in_dim = C * H * W
    class MLPScalarHires(nn.Module):
        def __init__(self):
            super().__init__()
            self.net = nn.Sequential(
                nn.Linear(in_dim, mlp_hidden),
                nn.LayerNorm(mlp_hidden),
                nn.GELU(),
                nn.Dropout(mlp_dropout),
                nn.Linear(mlp_hidden, mlp_hidden // 2),
                nn.LayerNorm(mlp_hidden // 2),
                nn.GELU(),
                nn.Dropout(mlp_dropout),
                nn.Linear(mlp_hidden // 2, 128),
                nn.GELU(),
                nn.Linear(128, 1),
            )

        def forward(self, x):
            return self.net(x.reshape(x.shape[0], -1)).squeeze(-1)

    # neuralop FNO handles complex FFT internally (no MPS cfloat issue)
    # but rfft2 still not on MPS, so FNO stays on CPU
    fno_device = torch.device("cpu")
    architectures = {
        "MLP": (MLPScalarHires, device),
        "FNO": (FNOScalarHires, fno_device),
    }

    # ── Cross-validation ─────────────────────────────────────────────────
    grid_t = torch.tensor(grid_np, dtype=torch.float32)
    Y_t = torch.tensor(Y_norm, dtype=torch.float32)

    kf = KFold(n_splits=n_folds, shuffle=True, random_state=RANDOM_STATE)

    def _count_params(model):
        return sum(p.numel() for p in model.parameters() if p.requires_grad)

    def _clip_grads(model, max_norm=1.0):
        """Gradient clipping (all params float32)."""
        params = [p for p in model.parameters()
                  if p.requires_grad and p.grad is not None]
        if params:
            nn.utils.clip_grad_norm_(params, max_norm)

    def train_eval(ModelClass, train_idx, test_idx, fold_i, arch_device):
        model = ModelClass().to(arch_device)
        if fold_i == 0:
            n_params = _count_params(model)
            print(f"    Parameters: {n_params:,}  |  device: {arch_device}")

        opt = torch.optim.AdamW(model.parameters(), lr=lr,
                                weight_decay=weight_decay)
        scheduler = torch.optim.lr_scheduler.OneCycleLR(
            opt, max_lr=lr, epochs=n_epochs,
            steps_per_epoch=max(1, len(train_idx) // batch_size),
            pct_start=0.15, div_factor=10, final_div_factor=100,
        )

        X_tr = grid_t[train_idx].to(arch_device)
        Y_tr = Y_t[train_idx].to(arch_device)
        X_te = grid_t[test_idx].to(arch_device)
        Y_te = Y_t[test_idx].to(arch_device)

        ds_tr = TensorDataset(X_tr, Y_tr)
        loader_tr = DataLoader(ds_tr, batch_size=batch_size, shuffle=True,
                               drop_last=len(train_idx) > batch_size)

        best_val = float("inf")
        best_state = None
        patience_count = 0

        for epoch in range(n_epochs):
            model.train()
            epoch_loss = 0.0
            n_batches = 0
            for xb, yb in loader_tr:
                opt.zero_grad()
                pred = model(xb)
                loss = F.mse_loss(pred, yb)
                loss.backward()
                _clip_grads(model, max_norm=1.0)
                opt.step()
                scheduler.step()
                epoch_loss += loss.item()
                n_batches += 1

            model.eval()
            with torch.no_grad():
                val_loss = F.mse_loss(model(X_te), Y_te).item()

            if val_loss < best_val - 1e-5:
                best_val = val_loss
                best_state = {k: v.clone() if hasattr(v, 'clone') else copy.deepcopy(v)
                              for k, v in model.state_dict().items()
                              if k != '_metadata'}
                patience_count = 0
            else:
                patience_count += 1
                if patience_count >= patience:
                    print(f"      Early stop @ epoch {epoch+1}")
                    break

            if (epoch + 1) % 25 == 0 or epoch == 0:
                train_r2_approx = 1 - epoch_loss / n_batches
                print(f"      ep{epoch+1:3d}  val_loss={val_loss:.4f}  "
                      f"patience={patience_count}/{patience}")

        if best_state:
            model.load_state_dict(best_state, strict=False)

        model.eval()
        with torch.no_grad():
            Y_pred = model(X_te).cpu().numpy()
            Y_true = Y_te.cpu().numpy()

        ss_res = ((Y_true - Y_pred) ** 2).sum()
        ss_tot = ((Y_true - Y_true.mean()) ** 2).sum()
        return float(1 - ss_res / (ss_tot + 1e-10))

    arch_results = {}
    for arch_name, (ModelClass, arch_device) in architectures.items():
        print(f"\n  ── Architecture: {arch_name} ──")
        r2_folds = []
        for fold_i, (tr_idx, te_idx) in enumerate(kf.split(grid_t)):
            print(f"    Fold {fold_i+1}/{n_folds}...", flush=True)
            try:
                r2 = train_eval(ModelClass, tr_idx, te_idx, fold_i, arch_device)
                print(f"      → R²={r2:.4f}")
            except Exception as e:
                import traceback
                print(f"      Error: {e}")
                traceback.print_exc()
                r2 = float("nan")
            r2_folds.append(r2)

        r2_arr = np.array(r2_folds)
        arch_results[arch_name] = {
            "r2_folds": r2_folds,
            "r2": float(np.nanmean(r2_arr)),
            "r2_std": float(np.nanstd(r2_arr)),
        }
        print(f"  {arch_name}: CV R²={arch_results[arch_name]['r2']:.4f} "
              f"± {arch_results[arch_name]['r2_std']:.4f}")

    # ── Summary plot ────────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(7, 4))
    names = list(arch_results.keys()) + ["PLS n=20\n(baseline)", "Ridge\n(baseline)"]
    r2s   = [arch_results[n]["r2"] for n in arch_results] + [0.554, 0.604]
    stds  = [arch_results[n]["r2_std"] for n in arch_results] + [0.042, 0.028]
    colors = ["#2196F3", "#FF9800", "#9E9E9E", "#9E9E9E"]
    bars = ax.bar(range(len(names)), r2s, yerr=stds, capsize=5,
                  color=colors[:len(names)], edgecolor="black", linewidth=0.6)
    ax.set_xticks(range(len(names)))
    ax.set_xticklabels(names, fontsize=10)
    ax.set_ylabel("CV R²", fontsize=11)
    ax.set_title(
        f"Neural Operator vs Baselines  [{target_res}° grid, {n_folds}-fold CV]\n"
        f"2-channel input: HRRR 1h wind + temp errors",
        fontsize=11,
    )
    ax.set_ylim(0, 1.0)
    ax.axhline(0.604, color="gray", linestyle="--", linewidth=0.8, label="Ridge")
    for bar, r2, std in zip(bars, r2s, stds):
        ax.text(bar.get_x() + bar.get_width() / 2,
                min(r2 + std + 0.01, 0.97),
                f"{r2:.3f}", ha="center", va="bottom", fontsize=9)
    fig.tight_layout()
    save_path = fig_dir / f"neural_hires_{target_res:.2f}deg_{months_label}.png"
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"\n  Chart saved: {save_path.name}")

    # ── Save table ────────────────────────────────────────────────────
    rows = [{"architecture": k, "resolution_deg": target_res,
              "cv_r2": v["r2"], "cv_r2_std": v["r2_std"],
              **{f"fold_{i+1}": f for i, f in enumerate(v["r2_folds"])}}
             for k, v in arch_results.items()]
    pd.DataFrame(rows).to_csv(
        tables_dir / f"neural_hires_{target_res:.2f}deg_results.csv", index=False
    )

    print("\n" + "=" * 70)
    print("STEP 5 COMPLETE")
    print("=" * 70)
    return arch_results


# ════════════════════════════════════════════════════════════════════════════
# Report Maps — clean individual β(s) and neural sensitivity maps
# ════════════════════════════════════════════════════════════════════════════

def generate_report_beta_maps(months=DEFAULT_MONTHS):
    """Generate clean individual spatial maps for each model for report inclusion.

    Produces:
      report_maps/beta_ridge.png          — Ridge β(s), wspd_error_1h
      report_maps/beta_fpca.png           — FPCA K=100 β(s)
      report_maps/beta_pls.png            — PLS n=20 β(s)
      report_maps/beta_linear_trio.png    — 3-panel: Ridge / FPCA / PLS
      report_maps/beta_extreme_cold.png   — PLS n=10, extreme cold subset
      report_maps/beta_extreme_heat.png   — PLS n=10, extreme heat subset
      report_maps/beta_regimes.png        — 2-panel: cold / heat
      report_maps/multi_field_betas.png   — 4×2: all fields × (FPCA, PLS)
      report_maps/neural_sensitivity_wind.png  — MLP |∂Ŷ/∂wind| averaged
      report_maps/neural_sensitivity_temp.png  — MLP |∂Ŷ/∂temp| averaged
      report_maps/neural_sensitivity.png       — 1×2: wind + temp sensitivity
    """
    from sklearn.cross_decomposition import PLSRegression
    from sklearn.decomposition import TruncatedSVD

    dirs = setup_directories()
    fig_dir = Path(dirs["figures"]) / "functional_analysis" / "report_maps"
    fig_dir.mkdir(parents=True, exist_ok=True)

    print("\n" + "=" * 70)
    print("GENERATING REPORT BETA MAPS")
    print("=" * 70)

    # ── Load data ──────────────────────────────────────────────────────────
    from process_data.classify_weather_regimes import classify_regimes
    df = load_pixel_data_with_regimes(months)
    df = classify_regimes(df)
    X_wind, Y, pc, _, times_wind = prepare_functional_data(df, error_field="wspd_error_1h")
    X_temp, _, pc_temp, _, times_temp = prepare_functional_data(df, error_field="temp_error_1h")
    lats, lons = pc[:, 0], pc[:, 1]

    # ── Helper: single-panel β map ─────────────────────────────────────────
    def _one_panel(beta, title, save_path, cmap="RdBu_r", label="β(s) [MW per σ]",
                   vmax=None, positive_only=False):
        fig, ax = plt.subplots(
            1, 1, figsize=(7, 5.5),
            subplot_kw={"projection": ccrs.PlateCarree()},
        )
        _draw_texas_base(ax)
        if vmax is None:
            vmax = np.nanpercentile(np.abs(beta), 98)
        vmin = 0 if positive_only else -vmax
        sc = ax.scatter(
            lons, lats, c=beta, cmap=cmap,
            vmin=vmin, vmax=vmax,
            s=6, marker="s", transform=ccrs.PlateCarree(), zorder=3, alpha=0.9,
        )
        plt.colorbar(sc, ax=ax, label=label, shrink=0.8, pad=0.03)
        ax.set_title(title, fontsize=12, fontweight="bold", pad=6)
        fig.tight_layout()
        fig.savefig(save_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        print(f"  Saved: {save_path.name}")

    # ── Helper: multi-panel β map ──────────────────────────────────────────
    def _multi_panel(betas_titles, save_path, suptitle="", ncols=3,
                     cmap="RdBu_r", label="β(s) [MW per σ]", shared_clim=True,
                     positive_only=False):
        n = len(betas_titles)
        nrows = (n + ncols - 1) // ncols
        fig, axes = plt.subplots(
            nrows, ncols, figsize=(6 * ncols, 5 * nrows),
            subplot_kw={"projection": ccrs.PlateCarree()},
        )
        axes = np.array(axes).reshape(nrows, ncols)

        all_b = np.concatenate([b for b, _ in betas_titles])
        clim = np.nanpercentile(np.abs(all_b), 98) if shared_clim else None

        sc_last = None
        for idx, (beta, title) in enumerate(betas_titles):
            r, c = divmod(idx, ncols)
            ax = axes[r, c]
            _draw_texas_base(ax)
            vmax = clim if shared_clim else np.nanpercentile(np.abs(beta), 98)
            vmin = 0 if positive_only else -vmax
            sc = ax.scatter(
                lons, lats, c=beta, cmap=cmap,
                vmin=vmin, vmax=vmax,
                s=5, marker="s", transform=ccrs.PlateCarree(), zorder=3, alpha=0.9,
            )
            ax.set_title(title, fontsize=10, fontweight="bold")
            sc_last = sc

        for idx in range(n, nrows * ncols):
            r, c = divmod(idx, ncols)
            axes[r, c].set_visible(False)

        if sc_last is not None:
            fig.colorbar(sc_last, ax=axes.ravel().tolist(),
                         label=label, shrink=0.5, pad=0.02)
        if suptitle:
            fig.suptitle(suptitle, fontsize=13, y=1.01)
        fig.tight_layout()
        fig.savefig(save_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        print(f"  Saved: {save_path.name}")

    # ── 1. Ridge β(s) ─────────────────────────────────────────────────────
    print("\n  Fitting Ridge...")
    ridge = RidgeCV(alphas=np.logspace(-2, 4, 20)).fit(X_wind, Y)
    beta_ridge = ridge.coef_
    _one_panel(beta_ridge, "Ridge β(s)  [wind error, HRRR 1h]",
               fig_dir / "beta_ridge.png")

    # ── 2. FPCA K=100 β(s) ────────────────────────────────────────────────
    print("  Fitting FPCA K=100...")
    svd = TruncatedSVD(n_components=100, random_state=RANDOM_STATE).fit(X_wind)
    scores_fpca = svd.transform(X_wind)
    rc = RidgeCV(alphas=np.logspace(-2, 4, 20)).fit(scores_fpca, Y)
    beta_fpca = svd.components_.T @ rc.coef_   # (N_pixels,)
    _one_panel(beta_fpca, "FPCA K=100 β(s)  [wind error, HRRR 1h]",
               fig_dir / "beta_fpca.png")

    # ── 3. PLS n=20 β(s) ──────────────────────────────────────────────────
    print("  Fitting PLS n=20...")
    pls20 = PLSRegression(n_components=20, scale=False).fit(X_wind, Y)
    beta_pls = pls20.coef_.ravel()
    _one_panel(beta_pls, "PLS n=20 β(s)  [wind error, HRRR 1h]",
               fig_dir / "beta_pls.png")

    # ── 4. Linear trio panel ───────────────────────────────────────────────
    _multi_panel(
        [
            (beta_ridge, "Ridge\n(R²=0.604)"),
            (beta_fpca,  "FPCA K=100\n(R²=0.438)"),
            (beta_pls,   "PLS n=20\n(R²=0.554)"),
        ],
        fig_dir / "beta_linear_trio.png",
        suptitle="Linear β(s) Surfaces — HRRR 1h Wind Speed Error → Curtailment",
        ncols=3,
        label="β(s) [MW per σ wind error]",
    )

    # ── 5. Regime: extreme cold & heat ────────────────────────────────────
    print("  Fitting PLS for extreme cold regime...")
    # Build hour-level regime labels aligned to functional data time index
    regime_hourly = df.groupby("valid_time")["regime_temp"].first()
    cold_times = set(regime_hourly.index[regime_hourly == "extreme_cold"])
    heat_times = set(regime_hourly.index[regime_hourly == "extreme_heat"])

    cold_idx = np.array([i for i, t in enumerate(times_wind) if t in cold_times])
    heat_idx = np.array([i for i, t in enumerate(times_wind) if t in heat_times])

    if len(cold_idx) >= 20:
        pls_cold = PLSRegression(n_components=10, scale=False).fit(
            X_wind[cold_idx], Y[cold_idx])
        beta_cold = pls_cold.coef_.ravel()
        _one_panel(beta_cold, "PLS n=10 β(s)  [Extreme Cold — wind error]",
                   fig_dir / "beta_extreme_cold.png")
    else:
        print(f"  Skipping cold regime: only {len(cold_idx)} hours")
        beta_cold = None

    print("  Fitting PLS for extreme heat regime...")
    if len(heat_idx) >= 20:
        pls_heat = PLSRegression(n_components=10, scale=False).fit(
            X_wind[heat_idx], Y[heat_idx])
        beta_heat = pls_heat.coef_.ravel()
        _one_panel(beta_heat, "PLS n=10 β(s)  [Extreme Heat — wind error]",
                   fig_dir / "beta_extreme_heat.png")
    else:
        print(f"  Skipping heat regime: only {len(heat_idx)} hours")
        beta_heat = None

    # ── 6. Regime pair panel ──────────────────────────────────────────────
    if beta_cold is not None and beta_heat is not None:
        n_cold, n_heat = len(cold_idx), len(heat_idx)
        _multi_panel(
            [
                (beta_cold, f"Extreme Cold (n={n_cold} hrs)\nPLS n=10 β(s)"),
                (beta_heat, f"Extreme Heat (n={n_heat} hrs)\nPLS n=10 β(s)"),
            ],
            fig_dir / "beta_regimes.png",
            suptitle="Regime-Stratified β(s) — Wind Error → Curtailment",
            ncols=2,
            label="β(s) [MW per σ wind error]",
        )

    # ── 7. Multi-field 4×2 grid ───────────────────────────────────────────
    print("  Building multi-field 4×2 beta panel...")
    field_betas = []
    field_labels = {
        "wspd_error_1h": "Wind Error\nHRRR 1h",
        "wspd_error_0h": "Wind Error\nGFS Day-Ahead",
        "temp_error_1h": "Temp Error\nHRRR 1h",
        "temp_error_0h": "Temp Error\nGFS Day-Ahead",
    }
    for field, flabel in field_labels.items():
        try:
            Xf, Yf, pcf, _, _ = prepare_functional_data(df, error_field=field)
            # FPCA
            svdf = TruncatedSVD(n_components=100, random_state=RANDOM_STATE).fit(Xf)
            rc_f = RidgeCV(alphas=np.logspace(-2, 4, 10)).fit(svdf.transform(Xf), Yf)
            beta_f_fpca = svdf.components_.T @ rc_f.coef_
            # PLS
            pls_f = PLSRegression(n_components=20, scale=False).fit(Xf, Yf)
            beta_f_pls = pls_f.coef_.ravel()
            field_betas.append((beta_f_fpca, f"FPCA\n{flabel}"))
            field_betas.append((beta_f_pls,  f"PLS\n{flabel}"))
        except Exception as e:
            print(f"    Skipped {field}: {e}")

    if field_betas:
        _multi_panel(
            field_betas,
            fig_dir / "multi_field_betas.png",
            suptitle="β(s) Surfaces Across All Four Error Fields",
            ncols=2,
            label="β(s) [MW per σ error]",
            shared_clim=False,
        )

    # ── 8. Neural operator gradient sensitivity ────────────────────────────
    print("\n  Training MLP for gradient sensitivity maps (30 epochs)...")
    try:
        import torch
        import torch.nn as nn
        import torch.nn.functional as F_torch
        from torch.utils.data import TensorDataset, DataLoader

        # Prepare 2-channel grid (wind + temp), downsampled 4×
        T_min = min(X_wind.shape[0], X_temp.shape[0])
        X_dict_nn = {
            "wspd_error_1h": X_wind[:T_min],
            "temp_error_1h": X_temp[:T_min],
        }
        Y_nn = Y[:T_min]

        grid_np = prepare_grid_data(X_dict_nn, pc)   # (T, 2, H, W)
        if 4 > 1:
            grid_np_ds = grid_np[:, :, ::4, ::4]
        T_nn, C_nn, H_nn, W_nn = grid_np_ds.shape

        Y_mean_nn, Y_std_nn = Y_nn.mean(), Y_nn.std() + 1e-8
        Y_norm_nn = (Y_nn - Y_mean_nn) / Y_std_nn

        grid_t = torch.tensor(grid_np_ds, dtype=torch.float32)
        Y_t_nn = torch.tensor(Y_norm_nn.astype(np.float32))

        class MLPSens(nn.Module):
            def __init__(self, in_dim):
                super().__init__()
                self.net = nn.Sequential(
                    nn.Linear(in_dim, 256), nn.GELU(),
                    nn.Linear(256, 256), nn.GELU(),
                    nn.Linear(256, 64), nn.GELU(),
                    nn.Linear(64, 1),
                )
            def forward(self, x):
                return self.net(x.view(x.shape[0], -1)).squeeze(-1)

        model_sens = MLPSens(C_nn * H_nn * W_nn)
        opt = torch.optim.Adam(model_sens.parameters(), lr=1e-3)
        ds = TensorDataset(grid_t, Y_t_nn)
        loader = DataLoader(ds, batch_size=64, shuffle=True)

        for epoch in range(30):
            model_sens.train()
            for xb, yb in loader:
                opt.zero_grad()
                loss = F_torch.mse_loss(model_sens(xb), yb)
                loss.backward()
                nn.utils.clip_grad_norm_(model_sens.parameters(), 1.0)
                opt.step()
            if (epoch + 1) % 10 == 0:
                model_sens.eval()
                with torch.no_grad():
                    yp = model_sens(grid_t)
                ss_res = ((Y_t_nn - yp)**2).sum().item()
                ss_tot = ((Y_t_nn - Y_t_nn.mean())**2).sum().item()
                print(f"    Epoch {epoch+1}: R²={1 - ss_res/ss_tot:.4f}")

        # Compute gradient sensitivity: mean |∂Ŷ/∂X| over all hours
        model_sens.eval()
        grid_t_grad = grid_t.clone().requires_grad_(True)
        yhat = model_sens(grid_t_grad)
        yhat.sum().backward()
        # grad shape: (T, C, H_ds, W_ds)
        sens = grid_t_grad.grad.abs().mean(dim=0).detach().numpy()  # (C, H_ds, W_ds)

        # Upsample back to full ERA5 grid coords for scatter plot
        # Map each original pixel to its downsampled cell center
        _, _, lat_idx, lon_idx, H_full, W_full = _build_era5_grid_index(pc)
        lat_idx_ds = lat_idx // 4
        lon_idx_ds = lon_idx // 4
        lat_idx_ds = np.clip(lat_idx_ds, 0, H_nn - 1)
        lon_idx_ds = np.clip(lon_idx_ds, 0, W_nn - 1)

        sens_wind_px = sens[0][lat_idx_ds, lon_idx_ds]   # (N_pixels,)
        sens_temp_px = sens[1][lat_idx_ds, lon_idx_ds]

        # Normalize to [0, 1] for interpretability
        vmax_w = np.percentile(sens_wind_px, 98)
        vmax_t = np.percentile(sens_temp_px, 98)

        _one_panel(sens_wind_px,
                   "MLP Gradient Sensitivity — Wind Error (HRRR 1h)\n|∂Ŷ/∂wind| averaged over all hours",
                   fig_dir / "neural_sensitivity_wind.png",
                   cmap="YlOrRd", label="|∂Curtailment/∂wind error| [a.u.]",
                   vmax=vmax_w, positive_only=True)
        _one_panel(sens_temp_px,
                   "MLP Gradient Sensitivity — Temp Error (HRRR 1h)\n|∂Ŷ/∂temp| averaged over all hours",
                   fig_dir / "neural_sensitivity_temp.png",
                   cmap="YlOrRd", label="|∂Curtailment/∂temp error| [a.u.]",
                   vmax=vmax_t, positive_only=True)

        # Combined 1×2 panel
        fig_sens, axes_sens = plt.subplots(
            1, 2, figsize=(13, 5.5),
            subplot_kw={"projection": ccrs.PlateCarree()},
        )
        for ax_s, sens_px, ch_label, vmax_ch in [
            (axes_sens[0], sens_wind_px, "Wind Error (HRRR 1h)", vmax_w),
            (axes_sens[1], sens_temp_px, "Temp Error (HRRR 1h)", vmax_t),
        ]:
            _draw_texas_base(ax_s)
            sc_s = ax_s.scatter(
                lons, lats, c=sens_px, cmap="YlOrRd",
                vmin=0, vmax=vmax_ch,
                s=5, marker="s", transform=ccrs.PlateCarree(), zorder=3, alpha=0.9,
            )
            plt.colorbar(sc_s, ax=ax_s, label="|∂Ŷ/∂error| [a.u.]",
                         shrink=0.8, pad=0.03)
            ax_s.set_title(f"MLP Sensitivity — {ch_label}", fontsize=11,
                           fontweight="bold")
        fig_sens.suptitle(
            "Neural Operator Gradient Sensitivity Maps\n"
            "(mean |∂curtailment/∂error| over 4,364 hours; MLP R²=0.901)",
            fontsize=12,
        )
        fig_sens.tight_layout()
        fig_sens.savefig(fig_dir / "neural_sensitivity.png",
                         dpi=150, bbox_inches="tight")
        plt.close(fig_sens)
        print(f"  Saved: neural_sensitivity.png")

    except ImportError:
        print("  torch not available — skipping neural sensitivity maps.")
    except Exception as e:
        import traceback
        print(f"  Neural sensitivity failed: {e}")
        traceback.print_exc()

    print("\n" + "=" * 70)
    print("REPORT MAPS COMPLETE")
    print(f"  Output dir: {fig_dir}")
    print("=" * 70)
    return fig_dir


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--extensions-only", action="store_true",
                        help="Run PLS / quantile / stability extensions (Run 2)")
    parser.add_argument("--run3", action="store_true",
                        help="Run Step 3: multi-field comparison, PLS-FPCA "
                             "divergence, and regime stratification")
    parser.add_argument("--run4", action="store_true",
                        help="Run Step 4: constrained PLS and neural operator")
    parser.add_argument("--run4b", action="store_true",
                        help="Run Step 4B only: neural operator (skip constrained PLS)")
    parser.add_argument("--report-maps", action="store_true",
                        help="Generate clean individual beta maps for report inclusion")
    parser.add_argument("--run5", action="store_true",
                        help="Run Step 5: high-res (0.25°) MLP+FNO with MPS acceleration")
    parser.add_argument("--fused-lasso", action="store_true",
                        help="Run spatial fused lasso to identify important forecast-error regions")
    parser.add_argument("--fused-lasso-field", default="wspd_error_1h",
                        help="Error field for fused lasso (default: wspd_error_1h)")
    args = parser.parse_args()

    if args.extensions_only:
        run_extensions(months=DEFAULT_MONTHS, primary_field="wspd_error_1h")

    elif args.run3:
        print("\n" + "=" * 70)
        print("RUN 3: MULTI-FIELD + PLS-FPCA DIVERGENCE + REGIME ANALYSIS")
        print("=" * 70)

        # 3A: Multi-field comparison
        field_results_3a = run_multi_field_comparison(months=DEFAULT_MONTHS)

        # 3B: PLS vs FPCA divergence on wspd_error_1h (primary field)
        dirs_3b = setup_directories()
        fig_dir_3b = Path(dirs_3b["figures"]) / "functional_analysis"
        df_3b = load_pixel_data(DEFAULT_MONTHS)
        X_3b, Y_3b, pc_3b, _, _ = prepare_functional_data(
            df_3b, error_field="wspd_error_1h"
        )
        div_results = investigate_pls_fpca_divergence(
            X_3b, Y_3b, pc_3b,
            field_label="wspd_error_1h",
            fig_dir=fig_dir_3b,
        )
        # Also run divergence on wspd_error_0h for GFS comparison
        X_3b_gfs, Y_3b_gfs, pc_3b_gfs, _, _ = prepare_functional_data(
            df_3b, error_field="wspd_error_0h"
        )
        div_results_gfs = investigate_pls_fpca_divergence(
            X_3b_gfs, Y_3b_gfs, pc_3b_gfs,
            field_label="wspd_error_0h",
            fig_dir=fig_dir_3b,
        )

        # 3C: Regime stratification (extreme cold + extreme heat)
        regime_results = run_regime_stratified_analysis(
            months=DEFAULT_MONTHS,
            regimes=("extreme_cold", "extreme_heat"),
            error_fields=ERROR_FIELDS,
            K=50, n_pls=10,
        )

        print("\n" + "=" * 70)
        print("RUN 3 COMPLETE")
        print("=" * 70)

    elif args.run4:
        print("\n" + "=" * 70)
        print("RUN 4: CONSTRAINED PLS + NEURAL OPERATOR")
        print("=" * 70)

        dirs_4 = setup_directories()
        fig_dir_4 = Path(dirs_4["figures"]) / "functional_analysis"

        # Load primary field data
        df_4 = load_pixel_data(DEFAULT_MONTHS)
        X_4, Y_4, pc_4, _, _ = prepare_functional_data(
            df_4, error_field="wspd_error_1h"
        )

        # 4A: Constrained PLS
        cpls_results = run_constrained_pls(
            X_4, Y_4, pc_4,
            lambda_values=[0.0, 0.01, 0.1, 1.0, 10.0],
            n_comp_list=[5, 10, 20, 50],
        )
        plot_constrained_pls_results(cpls_results, pc_4, fig_dir_4)

        # Save constrained PLS table
        cpls_rows = [
            {"lambda": lam, "n_components": n, "r2": v["r2"],
             "r2_std": v["r2_std"], "tv": v["tv"]}
            for (lam, n), v in cpls_results.items()
        ]
        tables_dir_4 = Path(dirs_4["tables"])
        pd.DataFrame(cpls_rows).to_csv(
            tables_dir_4 / "constrained_pls_results.csv", index=False
        )
        print("  Constrained PLS table saved.")

        # 4B: Neural operator
        # Load with regime-compatible data (all 4 fields)
        df_4b = load_pixel_data(DEFAULT_MONTHS)
        neural_results = run_neural_operator(
            df_4b,
            months_label="6mo",
            n_epochs=50,
            patience=10,
            n_folds=3,
            hidden_dim=32,
            n_modes=8,
            n_fno_layers=2,
            downsample_stride=4,
        )

        print("\n" + "=" * 70)
        print("RUN 4 COMPLETE")
        print("=" * 70)

    elif args.run4b:
        print("\n" + "=" * 70)
        print("RUN 4B: NEURAL OPERATOR ONLY")
        print("=" * 70)
        df_4b = load_pixel_data(DEFAULT_MONTHS)
        neural_results = run_neural_operator(
            df_4b,
            months_label="6mo",
            n_epochs=50,
            patience=10,
            n_folds=3,
            hidden_dim=32,
            n_modes=8,
            n_fno_layers=2,
            downsample_stride=4,
        )
        print("\n" + "=" * 70)
        print("RUN 4B COMPLETE")
        print("=" * 70)

    elif args.fused_lasso:
        print("\n" + "=" * 70)
        print("FUSED LASSO: SPATIAL REGION IDENTIFICATION")
        print("=" * 70)
        dirs_fl = setup_directories()
        fig_dir_fl = Path(dirs_fl["figures"]) / "functional_analysis"
        df_fl = load_pixel_data(DEFAULT_MONTHS)
        X_fl, Y_fl, pc_fl, pid_fl, _ = prepare_functional_data(
            df_fl, error_field=args.fused_lasso_field
        )
        fl_results = run_fused_lasso(
            X_fl, Y_fl, pc_fl, pid_fl,
            K_fpca=100,
            lambda1_values=[0.0, 0.01, 0.05, 0.2, 1.0],
            lambda2_values=[0.0, 0.01, 0.05, 0.2, 1.0],
            save_dir=fig_dir_fl,
        )
        # Save summary table
        tables_dir_fl = Path(dirs_fl["tables"])
        pd.DataFrame([{
            "field": args.fused_lasso_field,
            "lambda1_best": fl_results["lambda1_best"],
            "lambda2_best": fl_results["lambda2_best"],
            "cv_r2_best": fl_results["cv_r2_best"],
            "r2_raw": fl_results["r2_raw"],
            "zero_fraction": fl_results["zero_fraction"],
            "n_regions": fl_results["n_regions"],
        }]).to_csv(tables_dir_fl / "fused_lasso_summary.csv", index=False)
        print("  Fused lasso summary saved.")
        print("\n" + "=" * 70)
        print("FUSED LASSO COMPLETE")
        print("=" * 70)

    elif args.report_maps:
        generate_report_beta_maps(months=DEFAULT_MONTHS)

    elif args.run5:
        print("\n" + "=" * 70)
        print("RUN 5: HIGH-RESOLUTION NEURAL OPERATOR (MPS-ACCELERATED)")
        print("=" * 70)
        df_5 = load_pixel_data(DEFAULT_MONTHS)
        run_neural_operator_hires(
            df_5,
            months_label="6mo",
            target_res=0.25,
            n_folds=5,
            n_epochs=150,
            patience=20,
            batch_size=32,
            lr=5e-4,
            weight_decay=1e-4,
            fno_modes=16,
            fno_hidden=64,
            fno_layers=4,
            mlp_hidden=512,
            mlp_dropout=0.3,
        )

    else:
        results = run_full_analysis(
            months=DEFAULT_MONTHS,
            primary_field="wspd_error_1h",
        )
