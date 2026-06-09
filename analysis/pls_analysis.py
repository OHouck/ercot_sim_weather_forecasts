"""
Partial Least Squares (PLS) Spatial Analysis for ERCOT Forecast Error Attribution.

Supports single-field and multi-field (stacked) X matrices.  When more than
one error field is passed, the columns are concatenated horizontally so PLS
sees the full joint covariance structure.  Loading and coefficient surfaces
are then split back by field for per-field visualisation.

Sparsity
--------
Dense PLS components load on essentially every pixel, which makes the maps
hard to read.  Passing ``--sparse`` switches to Sparse PLS (Chun & Keleş 2010;
Lê Cao et al. 2008): each NIPALS weight vector is L1-penalised by
soft-thresholding so only ``keep_x`` pixels load on each component.  The CV
sweep then searches jointly over (n_components, keep_x), and a heatmap reports
out-of-sample R² across that grid.  Sparse runs write figures with a
``_sparse`` suffix so they never overwrite the dense ones.

Outputs
-------
  pls_cv_curve_*.png         — CV R² vs n_components (dense runs)
  pls_cv_heatmap_*.png       — CV R² over (n_components × keep_x) (sparse runs)
  pls_component_maps_*.png   — top-4 component loadings, one panel per field
  pls_coefficient_map_*.png  — β(s) coefficient surface, one panel per field
  pls_comparison_cv.png      — overlay of single-field vs joint CV curves
  pls_comparison_coefs.png   — side-by-side β(s) for each variant

Usage:
    # Single field (default)
    uv run python -m analysis.pls_analysis

    # Two-field joint model
    uv run python -m analysis.pls_analysis --fields wspd_error_1h temp_error_1h

    # Joint model + comparison against each single field
    uv run python -m analysis.pls_analysis --fields wspd_error_1h temp_error_1h --compare

    # Sparse PLS — each component loads on at most keep_x pixels
    uv run python -m analysis.pls_analysis --fields wspd_error_1h temp_error_1h \\
        --sparse --keep-x-grid 50 100 200 400

    # Different outcome variable
    uv run python -m analysis.pls_analysis --fields wspd_error_1h temp_error_1h \\
        --depvar economic_congestion_cost --compare
"""

import argparse
import sys
import warnings
from pathlib import Path

import functools

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import cartopy.crs as ccrs
import cartopy.io.shapereader as shpreader
from sklearn.base import BaseEstimator, RegressorMixin
from sklearn.cross_decomposition import PLSRegression
from sklearn.model_selection import KFold, cross_val_score
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# ── Defaults ──────────────────────────────────────────────────────────────────

DEPVAR = "total_curtailment_mw" # options are total_curtailment_mw or economic_congestion_cost
DEFAULT_FIELDS = ["wspd_error_1h", "temp_error_1h"]
ALL_MONTHS = [(2025, m) for m in range(1, 13)]
N_CV_FOLDS = 5
RANDOM_STATE = 42
N_COMPONENTS_GRID = [1, 2, 3, 5, 7, 10, 15, 20, 30, 50, 75, 100]
N_BOOTSTRAP = 500

# Sparse-PLS sparsity grid: number of NON-ZERO pixel weights kept PER component
# (the mixOmics "keepX" parameter).  Spans the joint stacked X, so a value of
# 100 means 100 nonzero columns total across all stacked error fields.
KEEP_X_GRID = [50, 100, 200, 400, 800, 1600]

# Concise labels for figure titles
FIELD_LABELS = {
    "wspd_error_1h":  "Wind speed error (HRRR 1h)",
    "wspd_error_0h":  "Wind speed error (GFS day-ahead)",
    "temp_error_1h":  "Temp error (HRRR 1h)",
    "temp_error_0h":  "Temp error (GFS day-ahead)",
}


def _label(field):
    return FIELD_LABELS.get(field, field)


# ── Data loading ──────────────────────────────────────────────────────────────

def load_and_prepare(months, error_fields, depvar):
    """Load parquets and build a functional (T × N_pixels*n_fields) matrix.

    Each error field is pivoted independently, then the resulting pixel sets
    and hour sets are intersected to form a common (T, N_pixels) block per
    field.  All blocks are standardised column-wise and then stacked
    horizontally:

        X = [X_field1 | X_field2 | ...]   shape (T, N_pixels * n_fields)

    Returns
    -------
    X           : ndarray (T, N_pixels * n_fields)
    Y           : ndarray (T,)
    pixel_coords: ndarray (N_pixels, 2)   — (lat, lon) for a single field's pixels
    pixel_ids   : ndarray of str
    hour_index  : pd.DatetimeIndex
    offsets     : list of (start, end) int pairs — column slices per field in X
    """
    if isinstance(error_fields, str):
        error_fields = [error_fields]

    dirs = setup_directories()
    lmp_dir = Path(dirs["processed"]) / "combined_hourly_gridded_data"

    keep_cols = (["pixel_id", "valid_time", "latitude", "longitude", depvar]
                 + error_fields)

    dfs = []
    for year, month in months:
        path = lmp_dir / f"pixel_hourly_gfs+hrrr_{year}_{month:02d}.parquet"
        if not path.exists():
            print(f"  [skip] Missing: {path.name}")
            continue
        import pyarrow.parquet as pq
        available = pq.read_schema(path).names
        cols = [c for c in keep_cols if c in available]
        df = pd.read_parquet(path, columns=cols)
        df["valid_time"] = pd.to_datetime(df["valid_time"])
        if df["valid_time"].dt.tz is not None:
            df["valid_time"] = df["valid_time"].dt.tz_localize(None)
        dfs.append(df)
        print(f"  Loaded {year}-{month:02d}: {len(df):,} rows")

    if not dfs:
        raise FileNotFoundError("No pixel_hourly parquet files found.")

    df = pd.concat(dfs, ignore_index=True)
    sub = df.dropna(subset=[depvar]).copy()

    # Build a pivot per field; find common pixels + hours across all fields
    pivots = {}
    for field in error_fields:
        field_sub = sub.dropna(subset=[field])
        piv = field_sub.pivot_table(
            index="valid_time", columns="pixel_id", values=field, aggfunc="first"
        )
        good = piv.columns[piv.notna().mean() > 0.90]
        pivots[field] = piv[good]

    # Intersect pixels
    common_pixels = pivots[error_fields[0]].columns
    for field in error_fields[1:]:
        common_pixels = common_pixels.intersection(pivots[field].columns)

    # Drop hours with any missing value, then intersect across fields
    for field in error_fields:
        pivots[field] = pivots[field][common_pixels].dropna(axis=0)

    common_times = pivots[error_fields[0]].index
    for field in error_fields[1:]:
        common_times = common_times.intersection(pivots[field].index)

    hourly_y = sub.groupby("valid_time")[depvar].first()
    common_times = common_times.intersection(hourly_y.index)

    Y = hourly_y.loc[common_times].values

    # Pixel coordinates
    coord_map = (
        sub[["pixel_id", "latitude", "longitude"]]
        .dropna(subset=["latitude", "longitude"])
        .drop_duplicates("pixel_id")
        .set_index("pixel_id")
    )
    valid_pids = common_pixels.intersection(coord_map.index)
    pixel_ids = valid_pids.values
    pixel_coords = coord_map.loc[pixel_ids, ["latitude", "longitude"]].values
    n_pixels = len(pixel_ids)

    # Stack fields horizontally; track column offsets for later slicing
    blocks = []
    offsets = []
    col_start = 0
    for field in error_fields:
        piv = pivots[field].loc[common_times, valid_pids]
        X_block = StandardScaler().fit_transform(piv.values)
        blocks.append(X_block)
        offsets.append((col_start, col_start + n_pixels))
        col_start += n_pixels

    X = np.concatenate(blocks, axis=1)

    print(f"\nFunctional data: X {X.shape}  "
          f"({len(error_fields)} field(s) × {n_pixels} pixels × "
          f"{len(common_times)} hours)")
    print(f"Y — mean={Y.mean():.1f}  std={Y.std():.1f}  "
          f"min={Y.min():.1f}  max={Y.max():.1f}")

    return X, Y, pixel_coords, pixel_ids, common_times, offsets


# ── Sparse PLS ──────────────────────────────────────────────────────────────

def _soft_threshold_keep(w, keep_x):
    """Soft-threshold a weight vector to keep at most ``keep_x`` nonzeros.

    Implements the mixOmics-style sparsity step: the penalty λ is set to the
    largest *discarded* magnitude (the (keep_x+1)-th largest |wᵢ|), then
    surviving weights are shrunk toward zero by λ.  This is the L1 / lasso
    penalty applied to the PLS weight vector at each NIPALS iteration.

    Parameters
    ----------
    w      : ndarray (p,) — raw covariance direction Xᵀy_resid
    keep_x : int          — number of nonzero weights to retain

    Returns
    -------
    w_thr : ndarray (p,) — soft-thresholded weight vector (not normalised)
    """
    p = w.size
    if keep_x >= p:
        return w
    abs_w = np.abs(w)
    # λ = (keep_x+1)-th largest magnitude → everything ≤ λ is zeroed
    lam = np.partition(abs_w, p - keep_x - 1)[p - keep_x - 1]
    w_thr = np.sign(w) * np.maximum(abs_w - lam, 0.0)
    if not np.any(w_thr):
        # Degenerate (e.g. ties at λ): fall back to hard top-keep_x selection
        keep_idx = np.argpartition(abs_w, p - keep_x)[p - keep_x:]
        w_thr = np.zeros_like(w)
        w_thr[keep_idx] = w[keep_idx]
    return w_thr


class SparsePLS(BaseEstimator, RegressorMixin):
    """Sparse Partial Least Squares for a univariate response.

    NIPALS PLS where each component's weight vector is L1-penalised via
    soft-thresholding so that only ``keep_x`` pixels load on it (Chun & Keleş
    2010; Lê Cao et al. 2008).  Drop-in compatible with the parts of
    ``sklearn.cross_decomposition.PLSRegression`` used in this module:
    exposes ``.fit``, ``.predict``, ``.coef_`` (shape (1, p)),
    ``.x_weights_`` (shape (p, n_components)), and ``.n_components``.

    Parameters
    ----------
    n_components : int   — number of latent components
    keep_x       : int   — nonzero pixel weights retained per component
    max_iter     : int   — accepted for API parity (single-response NIPALS is
                           non-iterative, so it is unused)

    Notes
    -----
    X is assumed already column-standardised by the caller; only centring is
    applied internally (matching how PLSRegression treats pre-scaled input).
    """

    def __init__(self, n_components=2, keep_x=100, max_iter=1000):
        self.n_components = n_components
        self.keep_x = keep_x
        self.max_iter = max_iter

    def fit(self, X, Y):
        X = np.asarray(X, dtype=float)
        y = np.asarray(Y, dtype=float).ravel()

        self.x_mean_ = X.mean(axis=0)
        self.y_mean_ = y.mean()
        X_res = X - self.x_mean_
        y_res = y - self.y_mean_

        n, p = X_res.shape
        K = min(self.n_components, n - 1, p)

        W = np.zeros((p, K))   # sparse weights
        P = np.zeros((p, K))   # x-loadings (for deflation)
        C = np.zeros(K)        # y-loadings

        for k in range(K):
            w = X_res.T @ y_res
            w = _soft_threshold_keep(w, self.keep_x)
            nw = np.linalg.norm(w)
            if nw == 0:
                K = k
                W, P, C = W[:, :K], P[:, :K], C[:K]
                break
            w /= nw
            t = X_res @ w
            tt = t @ t
            P[:, k] = (X_res.T @ t) / tt
            C[k] = (y_res @ t) / tt
            X_res = X_res - np.outer(t, P[:, k])
            y_res = y_res - C[k] * t
            W[:, k] = w

        self.x_weights_ = W
        # B = W (Pᵀ W)⁻¹ C   — standard PLS coefficient reconstruction
        if K > 0:
            beta = W @ np.linalg.pinv(P.T @ W) @ C
        else:
            beta = np.zeros(p)
        self.coef_ = beta.reshape(1, -1)
        self._beta = beta
        return self

    def predict(self, X):
        X = np.asarray(X, dtype=float)
        return (X - self.x_mean_) @ self._beta + self.y_mean_


def _make_pls(n_components, keep_x=None, max_iter=1000):
    """Factory: dense PLSRegression when keep_x is None, else SparsePLS."""
    if keep_x is None:
        return PLSRegression(n_components=n_components, max_iter=max_iter)
    return SparsePLS(n_components=n_components, keep_x=keep_x, max_iter=max_iter)


# ── PLS CV sweep ──────────────────────────────────────────────────────────────

def cv_sweep(X, Y, n_components_grid=None, n_folds=N_CV_FOLDS):
    """Cross-validate PLS over a grid of component counts.

    Returns
    -------
    results : dict  {n_comp: {"r2_mean": float, "r2_std": float, "r2_folds": array}}
    best_n  : int
    """
    if n_components_grid is None:
        n_components_grid = N_COMPONENTS_GRID
    max_feasible = min(X.shape) - 1
    grid = [n for n in n_components_grid if n <= max_feasible]

    kf = KFold(n_splits=n_folds, shuffle=True, random_state=RANDOM_STATE)
    results = {}

    print(f"  CV sweep over {len(grid)} component counts (k={n_folds} folds):")
    for n in grid:
        pls = PLSRegression(n_components=n, max_iter=1000)
        r2_folds = cross_val_score(pls, X, Y, cv=kf, scoring="r2")
        results[n] = {"r2_mean": r2_folds.mean(), "r2_std": r2_folds.std(),
                      "r2_folds": r2_folds}
        print(f"    n={n:4d}  CV R² = {r2_folds.mean():.4f} ± {r2_folds.std():.4f}")

    best_n = max(results, key=lambda k: results[k]["r2_mean"])
    print(f"  → Best n_components = {best_n}  "
          f"(CV R² = {results[best_n]['r2_mean']:.4f})")
    return results, best_n


def cv_sweep_sparse(X, Y, n_components_grid=None, keep_x_grid=None,
                    n_folds=N_CV_FOLDS):
    """Cross-validate Sparse PLS over a 2-D grid of (n_components, keep_x).

    Returns
    -------
    results : dict  {(n_comp, keep_x): {"r2_mean", "r2_std", "r2_folds"}}
    best_n  : int   — n_components at the CV-optimal cell
    best_kx : int   — keep_x at the CV-optimal cell
    """
    if n_components_grid is None:
        n_components_grid = N_COMPONENTS_GRID
    if keep_x_grid is None:
        keep_x_grid = KEEP_X_GRID

    n_pixels = X.shape[1]
    max_feasible = min(X.shape) - 1
    n_grid = [n for n in n_components_grid if n <= max_feasible]
    # keep_x cannot exceed the number of available columns
    kx_grid = sorted({min(kx, n_pixels) for kx in keep_x_grid})

    kf = KFold(n_splits=n_folds, shuffle=True, random_state=RANDOM_STATE)
    results = {}

    print(f"  Sparse CV sweep: {len(n_grid)} component counts × "
          f"{len(kx_grid)} keep_x values (k={n_folds} folds):")
    for kx in kx_grid:
        for n in n_grid:
            model = SparsePLS(n_components=n, keep_x=kx, max_iter=1000)
            r2_folds = cross_val_score(model, X, Y, cv=kf, scoring="r2")
            results[(n, kx)] = {"r2_mean": r2_folds.mean(),
                                "r2_std": r2_folds.std(),
                                "r2_folds": r2_folds}
        best_n_for_kx = max(n_grid, key=lambda n: results[(n, kx)]["r2_mean"])
        print(f"    keep_x={kx:5d}  best n={best_n_for_kx:4d}  "
              f"CV R² = {results[(best_n_for_kx, kx)]['r2_mean']:.4f}")

    best_n, best_kx = max(results, key=lambda k: results[k]["r2_mean"])
    print(f"  → Best (n_components, keep_x) = ({best_n}, {best_kx})  "
          f"(CV R² = {results[(best_n, best_kx)]['r2_mean']:.4f})")
    return results, best_n, best_kx


def _collapse_sparse_cv(results_2d, keep_x):
    """Slice a 2-D sparse CV result dict to a 1-D {n: stats} dict at fixed keep_x.

    Lets the existing ``plot_cv_curve`` reuse the sparse sweep output by
    plotting the R²-vs-n_components curve at the CV-optimal keep_x.
    """
    return {n: stats for (n, kx), stats in results_2d.items() if kx == keep_x}


def fit_pls(X, Y, n_components, keep_x=None):
    pls = _make_pls(n_components, keep_x=keep_x)
    pls.fit(X, Y)
    return pls


def bootstrap_pls_coefs(X, Y, n_components, keep_x=None, n_bootstrap=N_BOOTSTRAP,
                        random_state=RANDOM_STATE):
    """Bootstrap PLS coefficients by resampling hours with replacement.

    n_components (and keep_x, for sparse models) are fixed to the CV-optimal
    values so each bootstrap draw fits the same model complexity as the
    original.  Sign alignment is not needed because coef_ is pinned to the Y
    direction and stays sign-stable.

    Returns
    -------
    coef_samples : ndarray (n_bootstrap, n_total_cols)
    """
    rng = np.random.default_rng(random_state)
    n_obs = X.shape[0]
    coef_samples = np.empty((n_bootstrap, X.shape[1]))

    kx_note = "" if keep_x is None else f", keep_x={keep_x}"
    print(f"  Bootstrapping PLS (n_components={n_components}{kx_note}, "
          f"B={n_bootstrap} draws) …", flush=True)
    for i in range(n_bootstrap):
        if (i + 1) % 100 == 0:
            print(f"    {i + 1}/{n_bootstrap}", flush=True)
        idx = rng.integers(0, n_obs, size=n_obs)
        pls_b = _make_pls(n_components, keep_x=keep_x)
        pls_b.fit(X[idx], Y[idx])
        coef_samples[i] = pls_b.coef_.ravel()

    return coef_samples


def compute_significance_mask(coef_samples, alpha=0.05):
    """Percentile bootstrap 95 % CI; True where CI excludes zero.

    Parameters
    ----------
    coef_samples : ndarray (n_bootstrap, n_cols)
    alpha        : float — two-sided level (default 0.05)

    Returns
    -------
    sig_mask : ndarray bool (n_cols,)
    ci_lo    : ndarray float (n_cols,)
    ci_hi    : ndarray float (n_cols,)
    """
    lo = np.percentile(coef_samples, 100 * alpha / 2,     axis=0)
    hi = np.percentile(coef_samples, 100 * (1 - alpha / 2), axis=0)
    sig_mask = (lo > 0) | (hi < 0)
    return sig_mask, lo, hi


# ── Map helpers ───────────────────────────────────────────────────────────────

def _draw_texas(ax):
    proj = ccrs.PlateCarree()
    ax.set_extent([-107.5, -93.0, 25.5, 37.0], crs=proj)
    ax.set_facecolor("#cce5f0")
    shp = shpreader.natural_earth(
        resolution="10m", category="cultural", name="admin_1_states_provinces"
    )
    for rec in shpreader.Reader(shp).records():
        name  = rec.attributes.get("name")
        admin = rec.attributes.get("admin")
        if name == "Texas":
            ax.add_geometries([rec.geometry], crs=proj,
                              facecolor="white", edgecolor="black",
                              linewidth=0.8, zorder=2)
        elif admin == "United States of America":
            ax.add_geometries([rec.geometry], crs=proj,
                              facecolor="#f0f0f0", edgecolor="#aaa",
                              linewidth=0.3, zorder=1)


def _scatter_map(ax, values, pixel_coords, title,
                 cmap="RdBu_r", vmin=None, vmax=None):
    _draw_texas(ax)
    if vmin is None or vmax is None:
        clim = np.nanpercentile(np.abs(values), 98)
        vmin, vmax = -clim, clim
    sc = ax.scatter(
        pixel_coords[:, 1], pixel_coords[:, 0],
        c=values, cmap=cmap, vmin=vmin, vmax=vmax,
        s=4, marker="s", transform=ccrs.PlateCarree(), zorder=3, alpha=0.9,
    )
    ax.set_title(title, fontsize=9, pad=4)
    return sc


# ── Infrastructure overlay ────────────────────────────────────────────────────

_TEXAS_CITIES = ["Houston", "Dallas", "Fort Worth", "San Antonio", "Austin"]


@functools.lru_cache(maxsize=1)
def _load_infrastructure_overlay():
    """Load static wind, solar, and transmission layers.

    Cached after first call.  City boundaries are handled separately by
    _draw_city_boundaries() which mirrors the approach in pixel_regression_maps.py.

    Keys
    ----
    wind_coords   : (N, 2) float  lat/lon of pixels with wind turbine capacity
    wind_mw       : (N,)   float  nameplate MW
    solar_coords  : (N, 2) float  lat/lon of pixels with solar PV capacity
    solar_mw      : (N,)   float  nameplate MW
    tx_lines      : GeoDataFrame of 345 kV lines, or None
    """
    import xarray as xr
    import geopandas as gpd

    dirs = setup_directories()
    gen_map_path = Path(dirs["processed"]) / "gridded_generation_map.nc"

    wind_coords = solar_coords = np.empty((0, 2))
    wind_mw = solar_mw = np.empty(0)

    if gen_map_path.exists():
        ds = xr.open_dataset(gen_map_path)
        lats = ds.latitude.values
        lons = ds.longitude.values
        lat_g, lon_g = np.meshgrid(lats, lons, indexing="ij")

        wind_cap  = ds["nameplate_mw_tech_onshore_wind_turbine"].values
        solar_cap = ds["nameplate_mw_tech_solar_photovoltaic"].values
        ds.close()

        wm = wind_cap  > 0
        sm = solar_cap > 0

        wind_coords  = np.column_stack([lat_g[wm], lon_g[wm]])
        wind_mw      = wind_cap[wm]
        solar_coords = np.column_stack([lat_g[sm], lon_g[sm]])
        solar_mw     = solar_cap[sm]
    else:
        print("  [infra] gridded_generation_map.nc not found — skipping overlay")

    # Transmission lines shapefile (checked into git under data/)
    data_dir = Path(__file__).resolve().parent.parent / "data"
    tx_shp   = data_dir / "Line_Output.shp"
    tx_lines = None
    if tx_shp.exists():
        tx_lines = gpd.read_file(tx_shp)
    else:
        print(f"  [infra] {tx_shp} not found — skipping transmission overlay")

    return {
        "wind_coords":  wind_coords,
        "wind_mw":      wind_mw,
        "solar_coords": solar_coords,
        "solar_mw":     solar_mw,
        "tx_lines":     tx_lines,
    }


@functools.lru_cache(maxsize=1)
def _load_city_data():
    """Load Natural Earth urban area polygons and city point locations for Texas.

    Mirrors the approach in pixel_regression_maps._draw_city_boundaries().
    Cached after first call.

    Returns
    -------
    dict with:
      urban_geoms : list of shapely geometries for major Texas urban areas
      cities      : list of (name, lon, lat) tuples for label placement
    """
    import geopandas as gpd
    from shapely.geometry import box as shapely_box

    tx_bbox = shapely_box(-107.5, 25.5, -93.0, 37.0)

    # Urban area polygons
    urban_shp = shpreader.natural_earth(
        resolution="10m", category="cultural", name="urban_areas"
    )
    urban_gdf = gpd.read_file(urban_shp)
    if urban_gdf.crs is None:
        urban_gdf = urban_gdf.set_crs(epsg=4326)
    urban_tx = urban_gdf[urban_gdf.geometry.intersects(tx_bbox)].copy()

    # City point locations
    places_shp = shpreader.natural_earth(
        resolution="10m", category="cultural", name="populated_places"
    )
    places_gdf = gpd.read_file(places_shp)
    if places_gdf.crs is None:
        places_gdf = places_gdf.set_crs(epsg=4326)

    cities_gdf = places_gdf[
        places_gdf["NAME"].isin(_TEXAS_CITIES) & (places_gdf["ADM0_A3"] == "USA")
    ].copy()

    # Match each named city to its urban polygon via spatial join
    urban_geoms = []
    if len(urban_tx) > 0 and len(cities_gdf) > 0:
        joined = gpd.sjoin(
            cities_gdf[["NAME", "geometry"]],
            urban_tx.reset_index()[["geometry"]],
            how="left",
            predicate="within",
        )
        urban_indices = joined["index_right"].dropna().astype(int).unique()
        urban_geoms = [urban_tx.iloc[i].geometry for i in urban_indices]

    cities = [
        (row["NAME"], row.geometry.x, row.geometry.y)
        for _, row in cities_gdf.iterrows()
    ]

    return {"urban_geoms": urban_geoms, "cities": cities}


def _draw_city_boundaries(ax, proj):
    """Draw major Texas city outlines and labels onto a Cartopy axis.

    Mirrors pixel_regression_maps._draw_city_boundaries().
    Urban area polygon outlines (grey) + city name labels at point locations.
    """
    city_data = _load_city_data()

    for geom in city_data["urban_geoms"]:
        ax.add_geometries(
            [geom], crs=proj,
            facecolor="none", edgecolor="#555555",
            linewidth=0.9, zorder=6,
        )

    for name, lon, lat in city_data["cities"]:
        ax.text(
            lon, lat, name,
            transform=proj, fontsize=6.5,
            ha="center", va="bottom",
            color="#333333", zorder=7, fontweight="bold",
        )


def _add_infrastructure_overlay(ax):
    """Draw wind, solar, transmission, and city layers onto *ax*.

    Styling matches pixel_regression_maps.py:
      - Wind capacity   : hollow dodgerblue circles  (zorder 6)
      - Solar capacity  : hollow gold squares         (zorder 6)
      - Transmission    : thin dimgray lines          (zorder 5)
      - Major cities    : grey urban outlines + labels (zorder 6–7)
    """
    infra = _load_infrastructure_overlay()
    proj  = ccrs.PlateCarree()

    # Transmission lines
    if infra["tx_lines"] is not None:
        for geom in infra["tx_lines"].geometry:
            if geom is None:
                continue
            parts = [geom] if geom.geom_type == "LineString" else list(geom.geoms)
            for part in parts:
                xs, ys = part.xy
                ax.plot(xs, ys, color="dimgray", linewidth=0.5,
                        transform=proj, zorder=5, alpha=0.55)

    # Wind capacity pixels — hollow dodgerblue circles (matches pixel_regression_maps)
    if len(infra["wind_coords"]) > 0:
        ax.scatter(
            infra["wind_coords"][:, 1], infra["wind_coords"][:, 0],
            s=7, marker="o", facecolors="none", edgecolors="dodgerblue",
            linewidths=0.6, transform=proj, zorder=6, alpha=0.75,
        )

    # Solar capacity pixels — hollow gold squares (matches pixel_regression_maps)
    if len(infra["solar_coords"]) > 0:
        ax.scatter(
            infra["solar_coords"][:, 1], infra["solar_coords"][:, 0],
            s=7, marker="s", facecolors="none", edgecolors="gold",
            linewidths=0.6, transform=proj, zorder=6, alpha=0.75,
        )

    # Major city outlines and labels
    _draw_city_boundaries(ax, proj)


def _infra_legend_handles():
    """Return legend handles matching the infrastructure overlay styling."""
    return [
        mlines.Line2D([], [], color="dimgray", linewidth=1.2,
                      label="Transmission line"),
        mlines.Line2D([], [], marker="o", color="w", markerfacecolor="none",
                      markeredgecolor="dodgerblue", markeredgewidth=0.8,
                      markersize=6, linestyle="none", label="Wind capacity"),
        mlines.Line2D([], [], marker="s", color="w", markerfacecolor="none",
                      markeredgecolor="gold", markeredgewidth=0.8,
                      markersize=6, linestyle="none", label="Solar capacity"),
        mlines.Line2D([], [], color="#555555", linewidth=0.9,
                      label="Major cities"),
    ]


# ── Field ordering ────────────────────────────────────────────────────────────

def _field_display_order(field):
    """Sort key: temp fields first (0), wind speed fields second (1), rest last."""
    if "temp" in field:
        return 0
    if "wspd" in field:
        return 1
    return 2


def _sort_fields_for_display(fields, offsets):
    """Return (fields, offsets) reordered so temp is left, wind speed is right."""
    pairs = sorted(zip(fields, offsets), key=lambda p: _field_display_order(p[0]))
    return [p[0] for p in pairs], [p[1] for p in pairs]


# ── Plots ─────────────────────────────────────────────────────────────────────

def plot_cv_curve(cv_results, best_n, save_path, title_suffix=""):
    """Line chart of CV R² vs number of PLS components."""
    ns    = sorted(cv_results.keys())
    means = [cv_results[n]["r2_mean"] for n in ns]
    stds  = [cv_results[n]["r2_std"]  for n in ns]

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(ns, means, "o-", color="steelblue", linewidth=1.5, markersize=5)
    ax.fill_between(ns,
                    [m - s for m, s in zip(means, stds)],
                    [m + s for m, s in zip(means, stds)],
                    alpha=0.2, color="steelblue")
    ax.axvline(best_n, color="crimson", linestyle="--", linewidth=1.2,
               label=f"Optimal n={best_n}")
    ax.set_xlabel("Number of PLS Components", fontsize=11)
    ax.set_ylabel("Out-of-Sample R² (5-fold CV)", fontsize=11)
    ax.set_title(f"PLS Component Selection{title_suffix}", fontsize=13)
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {save_path.name}")


def plot_cv_heatmap(results_2d, best_n, best_kx, save_path, title_suffix=""):
    """Heatmap of CV R² over the Sparse-PLS (n_components × keep_x) grid."""
    ns  = sorted({n for n, _ in results_2d})
    kxs = sorted({kx for _, kx in results_2d})

    grid = np.full((len(kxs), len(ns)), np.nan)
    for i, kx in enumerate(kxs):
        for j, n in enumerate(ns):
            if (n, kx) in results_2d:
                grid[i, j] = results_2d[(n, kx)]["r2_mean"]

    fig, ax = plt.subplots(figsize=(1.1 * len(ns) + 2, 0.7 * len(kxs) + 2))
    im = ax.imshow(grid, aspect="auto", origin="lower", cmap="viridis")
    ax.set_xticks(range(len(ns)),  labels=ns)
    ax.set_yticks(range(len(kxs)), labels=kxs)
    ax.set_xlabel("Number of PLS Components", fontsize=11)
    ax.set_ylabel("keep_x  (nonzero pixels / component)", fontsize=11)

    # Annotate each cell and ring the CV-optimal one
    for i, kx in enumerate(kxs):
        for j, n in enumerate(ns):
            if np.isnan(grid[i, j]):
                continue
            ax.text(j, i, f"{grid[i, j]:.3f}", ha="center", va="center",
                    fontsize=7, color="white")
    bj, bi = ns.index(best_n), kxs.index(best_kx)
    ax.add_patch(plt.Rectangle((bj - 0.5, bi - 0.5), 1, 1, fill=False,
                               edgecolor="crimson", linewidth=2.5))

    fig.colorbar(im, ax=ax, label="Out-of-Sample R² (5-fold CV)")
    ax.set_title(f"Sparse PLS Selection{title_suffix}\n"
                 f"optimal: n={best_n}, keep_x={best_kx}", fontsize=12)
    fig.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {save_path.name}")


def plot_component_maps(pls, pixel_coords, offsets, error_fields, save_path,
                        n_show=4, depvar=DEPVAR):
    """Heatmap grid: rows = error fields (temp top, wind bottom), cols = top-N components.

    Each cell shows the PLS x-weight for that field × component.
    pls.x_weights_ has shape (n_total_cols, n_components).
    Infrastructure overlay (wind, solar, transmission, load centres) is added
    to every panel; a shared legend appears on the bottom-right panel.
    """
    n_show   = min(n_show, pls.n_components)
    weights  = pls.x_weights_    # (n_total_cols, n_components)

    # Sort rows: temp on top, wind speed on bottom
    disp_fields, disp_offsets = _sort_fields_for_display(error_fields, offsets)
    n_fields = len(disp_fields)

    nrows, ncols = n_fields, n_show
    fig, axes = plt.subplots(
        nrows, ncols,
        figsize=(6 * ncols, 5 * nrows),
        subplot_kw={"projection": ccrs.PlateCarree()},
        squeeze=False,
    )

    for comp_i in range(n_show):
        # Shared colour limits across fields for this component
        all_w = np.concatenate([
            weights[start:end, comp_i] for start, end in disp_offsets
        ])
        clim = np.nanpercentile(np.abs(all_w), 98)

        for field_j, (field, (start, end)) in enumerate(
            zip(disp_fields, disp_offsets)
        ):
            ax = axes[field_j, comp_i]
            w  = weights[start:end, comp_i]
            sc = _scatter_map(
                ax, w, pixel_coords,
                title=f"{_label(field)}  —  Component {comp_i + 1}",
                vmin=-clim, vmax=clim,
            )
            plt.colorbar(sc, ax=ax, shrink=0.7, pad=0.02, label="Weight")
            _add_infrastructure_overlay(ax)

    # Infrastructure legend on bottom-right panel
    axes[-1, -1].legend(
        handles=_infra_legend_handles(),
        loc="lower left", fontsize=7, framealpha=0.85,
        markerscale=1.1, handlelength=1.4,
    )

    field_str = " + ".join(disp_fields)
    fig.suptitle(
        f"Top {n_show} PLS Component Spatial Loadings\n"
        f"Fields: {field_str}  |  Outcome: {depvar}",
        fontsize=12, y=1.01,
    )
    fig.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {save_path.name}")


def plot_coefficient_map(pls, pixel_coords, offsets, error_fields, save_path,
                         cv_r2, best_n, depvar, sig_mask=None):
    """One panel per field showing the recovered β(s) coefficient surface.

    β(s) = pls.coef_.ravel() sliced back to the pixels belonging to each field.
    Panels are sorted temp-left / wind-right.  Non-significant pixels (5% level
    via bootstrap CI) are shown in light grey.  Infrastructure layers (wind
    capacity, solar capacity, transmission lines, load centres) are overlaid on
    every panel.
    """
    beta = pls.coef_.ravel()   # (n_total_cols,)

    # Sort fields for consistent display: temp left, wspd right
    disp_fields, disp_offsets = _sort_fields_for_display(error_fields, offsets)
    n_fields = len(disp_fields)

    # Colour limits from significant pixels only (or all pixels if no mask)
    if sig_mask is not None and sig_mask.any():
        clim = np.nanpercentile(np.abs(beta[sig_mask]), 98)
    else:
        clim = np.nanpercentile(np.abs(beta), 98)

    fig, axes = plt.subplots(
        1, n_fields,
        figsize=(8 * n_fields, 6),
        subplot_kw={"projection": ccrs.PlateCarree()},
        squeeze=False,
    )

    for j, (field, (start, end)) in enumerate(zip(disp_fields, disp_offsets)):
        ax = axes[0, j]
        b  = beta[start:end]

        # Per-field significance slice
        field_sig = sig_mask[start:end] if sig_mask is not None else np.ones(len(b), dtype=bool)

        _draw_texas(ax)

        # Non-significant pixels — light grey background dots (zorder=3)
        if (~field_sig).any():
            ax.scatter(
                pixel_coords[~field_sig, 1], pixel_coords[~field_sig, 0],
                c="lightgrey", s=4, marker="s",
                transform=ccrs.PlateCarree(), zorder=3, alpha=0.6,
            )

        # Significant pixels — full colormap (zorder=4, beneath infra overlay)
        if field_sig.any():
            sc = ax.scatter(
                pixel_coords[field_sig, 1], pixel_coords[field_sig, 0],
                c=b[field_sig], cmap="RdBu_r", vmin=-clim, vmax=clim,
                s=4, marker="s", transform=ccrs.PlateCarree(),
                zorder=4, alpha=0.9,
            )
            cbar = plt.colorbar(sc, ax=ax, shrink=0.65, pad=0.03)
            cbar.set_label(f"β → {depvar}", fontsize=8)
        else:
            sm = plt.cm.ScalarMappable(
                cmap="RdBu_r", norm=plt.Normalize(vmin=-clim, vmax=clim),
            )
            sm.set_array([])
            cbar = plt.colorbar(sm, ax=ax, shrink=0.65, pad=0.03)
            cbar.set_label(f"β → {depvar}", fontsize=8)

        # Infrastructure overlay (zorder 5-7, on top of coefficient pixels)
        _add_infrastructure_overlay(ax)

        n_sig = int(field_sig.sum())
        n_tot = len(b)
        pct   = 100 * n_sig / n_tot if n_tot > 0 else 0
        suffix = (f"\n{n_sig}/{n_tot} pixels significant ({pct:.0f}%)"
                  if sig_mask is not None else "")
        ax.set_title(f"{_label(field)}{suffix}", fontsize=9, pad=4)

    # Infrastructure legend on the rightmost panel
    axes[0, -1].legend(
        handles=_infra_legend_handles(),
        loc="lower left", fontsize=7, framealpha=0.85,
        markerscale=1.1, handlelength=1.4,
    )

    field_str = " + ".join(disp_fields)
    sig_note  = "  [grey = not significant at 5%]" if sig_mask is not None else ""
    fig.suptitle(
        f"PLS Coefficient Surface  β(s){sig_note}\n"
        f"n={best_n} components  |  CV R²={cv_r2:.3f}  |  "
        f"{field_str} → {depvar}",
        fontsize=11, y=1.02,
    )
    fig.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {save_path.name}")


def plot_comparison_cv(all_cv_results, all_best_ns, labels, save_path, depvar):
    """Overlay CV R² curves for multiple PLS runs (single-field + joint)."""
    colors = plt.cm.tab10(np.linspace(0, 0.7, len(labels)))

    fig, ax = plt.subplots(figsize=(9, 5))
    for (label, cv_res, best_n), color in zip(
        zip(labels, all_cv_results, all_best_ns), colors
    ):
        ns    = sorted(cv_res.keys())
        means = [cv_res[n]["r2_mean"] for n in ns]
        stds  = [cv_res[n]["r2_std"]  for n in ns]
        ax.plot(ns, means, "o-", color=color, linewidth=1.6, markersize=5,
                label=f"{label}  (opt n={best_n}, R²={cv_res[best_n]['r2_mean']:.3f})")
        ax.fill_between(ns,
                        [m - s for m, s in zip(means, stds)],
                        [m + s for m, s in zip(means, stds)],
                        alpha=0.12, color=color)
        ax.axvline(best_n, color=color, linestyle=":", linewidth=0.9)

    ax.set_xlabel("Number of PLS Components", fontsize=11)
    ax.set_ylabel("Out-of-Sample R² (5-fold CV)", fontsize=11)
    ax.set_title(f"PLS: Single-field vs Joint Model  |  Outcome: {depvar}",
                 fontsize=13)
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {save_path.name}")


def plot_comparison_coefs(pls_runs, labels, pixel_coords, save_path, depvar,
                          sig_masks=None):
    """Grid of β(s) panels: one column per model run, one row per field.

    For single-field runs the field column is 1 wide; for the joint run each
    field gets its own column.  All panels share the same colour axis.

    sig_masks : list of ndarray bool or None, parallel to pls_runs.
        Each element is the full-length significance mask for that run, or None
        to show all pixels for that run.
    """
    # Flatten to a list of (panel_title, beta_vector, sig_vector) tuples.
    # Within each run sort fields so temp is left, wind speed is right.
    panels = []
    for run_idx, (label, pls, offsets, fields) in enumerate(pls_runs):
        beta = pls.coef_.ravel()
        run_mask = sig_masks[run_idx] if sig_masks is not None else None
        sorted_fields, sorted_offsets = _sort_fields_for_display(fields, offsets)
        for field, (start, end) in zip(sorted_fields, sorted_offsets):
            field_sig = run_mask[start:end] if run_mask is not None else None
            panels.append((f"{label}\n{_label(field)}", beta[start:end], field_sig))

    n = len(panels)
    ncols = min(n, 3)
    nrows = (n + ncols - 1) // ncols

    # Colour limits from significant pixels across all panels
    sig_betas = []
    for _, b, m in panels:
        sig_betas.append(b[m] if m is not None else b)
    clim = np.nanpercentile(np.abs(np.concatenate(sig_betas)), 98)

    fig, axes = plt.subplots(
        nrows, ncols,
        figsize=(7 * ncols, 5.5 * nrows),
        subplot_kw={"projection": ccrs.PlateCarree()},
        squeeze=False,
    )

    last_visible_ax = None
    for idx, (title, beta, field_sig) in enumerate(panels):
        row, col = divmod(idx, ncols)
        ax = axes[row, col]
        last_visible_ax = ax

        _draw_texas(ax)

        if field_sig is not None and (~field_sig).any():
            ax.scatter(
                pixel_coords[~field_sig, 1], pixel_coords[~field_sig, 0],
                c="lightgrey", s=4, marker="s",
                transform=ccrs.PlateCarree(), zorder=3, alpha=0.6,
            )

        plot_mask = field_sig if field_sig is not None else np.ones(len(beta), dtype=bool)
        if plot_mask.any():
            sc = ax.scatter(
                pixel_coords[plot_mask, 1], pixel_coords[plot_mask, 0],
                c=beta[plot_mask], cmap="RdBu_r", vmin=-clim, vmax=clim,
                s=4, marker="s", transform=ccrs.PlateCarree(),
                zorder=4, alpha=0.9,
            )
        else:
            sc = plt.cm.ScalarMappable(
                cmap="RdBu_r", norm=plt.Normalize(vmin=-clim, vmax=clim)
            )
            sc.set_array([])

        # Infrastructure overlay on every panel
        _add_infrastructure_overlay(ax)

        if field_sig is not None:
            n_sig = int(field_sig.sum())
            full_title = f"{title}\n({n_sig}/{len(beta)} sig.)"
        else:
            full_title = title
        ax.set_title(full_title, fontsize=8, pad=4)
        plt.colorbar(sc, ax=ax, shrink=0.65, pad=0.02, label=f"β → {depvar}")

    # Infrastructure legend on the last visible panel
    if last_visible_ax is not None:
        last_visible_ax.legend(
            handles=_infra_legend_handles(),
            loc="lower left", fontsize=7, framealpha=0.85,
            markerscale=1.1, handlelength=1.4,
        )

    for idx in range(n, nrows * ncols):
        row, col = divmod(idx, ncols)
        axes[row, col].set_visible(False)

    sig_note = "  [grey = not significant at 5%]" if sig_masks is not None else ""
    fig.suptitle(
        f"PLS Coefficient Surfaces β(s)  —  Single-field vs Joint{sig_note}\n"
        f"Outcome: {depvar}",
        fontsize=12, y=1.01,
    )
    fig.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {save_path.name}")


# ── Main ──────────────────────────────────────────────────────────────────────

def run(months=None, error_fields=None, depvar=DEPVAR,
        n_components_grid=None, compare=False, n_bootstrap=N_BOOTSTRAP,
        sparse=False, keep_x_grid=None):
    """Fit PLS for the given (possibly multi-field) error spec.

    Parameters
    ----------
    error_fields : list of str
        One or more forecast error column names.  If more than one, they are
        stacked horizontally into a single X matrix.
    compare : bool
        If True and len(error_fields) > 1, also fit each field individually
        and produce comparison figures.
    n_bootstrap : int
        Number of bootstrap draws for coefficient significance testing.
        Set to 0 to skip bootstrapping (all pixels shown).
    sparse : bool
        If True, fit Sparse PLS (L1-penalised weights) and cross-validate
        jointly over (n_components, keep_x).  Each component then loads on at
        most keep_x pixels, giving sparse, more interpretable maps.
    keep_x_grid : list of int
        Candidate keep_x values (nonzero pixels per component) for the sparse
        CV sweep.  Defaults to KEEP_X_GRID.  Ignored when sparse=False.
    """
    if months is None:
        months = ALL_MONTHS
    if error_fields is None:
        error_fields = DEFAULT_FIELDS
    if isinstance(error_fields, str):
        error_fields = [error_fields]
    if n_components_grid is None:
        n_components_grid = N_COMPONENTS_GRID
    if keep_x_grid is None:
        keep_x_grid = KEEP_X_GRID

    dirs = setup_directories()
    fig_dir = Path(dirs["figures"]) / "pls_analysis"
    fig_dir.mkdir(parents=True, exist_ok=True)

    # File-name stems for this run; sparse runs get their own suffix so dense
    # and sparse figures never overwrite each other.
    fields_slug = "_".join(error_fields)
    mode_slug = "_sparse" if sparse else ""

    print("=" * 65)
    print("PLS SPATIAL ANALYSIS" + ("  (SPARSE)" if sparse else ""))
    print(f"  Error fields : {error_fields}")
    print(f"  Outcome      : {depvar}")
    print(f"  Months       : {months[0]} – {months[-1]}")
    print(f"  Compare mode : {compare}")
    print(f"  Bootstrap B  : {n_bootstrap}")
    if sparse:
        print(f"  keep_x grid  : {keep_x_grid}")
    print("=" * 65)

    # ── Joint (or single-field) model ──────────────────────────────────────
    print(f"\n--- Loading data for: {error_fields} ---")
    X, Y, pixel_coords, pixel_ids, hour_index, offsets = load_and_prepare(
        months, error_fields, depvar
    )

    print(f"\n--- CV sweep: joint [{', '.join(error_fields)}] ---")
    if sparse:
        cv_joint_2d, best_n_joint, best_kx_joint = cv_sweep_sparse(
            X, Y, n_components_grid, keep_x_grid
        )
        cv_joint = _collapse_sparse_cv(cv_joint_2d, best_kx_joint)
    else:
        cv_joint, best_n_joint = cv_sweep(X, Y, n_components_grid)
        best_kx_joint = None

    print(f"\n--- Fitting final PLS (n={best_n_joint}"
          f"{'' if best_kx_joint is None else f', keep_x={best_kx_joint}'}) ---")
    pls_joint = fit_pls(X, Y, best_n_joint, keep_x=best_kx_joint)

    # Bootstrap significance for joint model
    if n_bootstrap > 0:
        print(f"\n--- Bootstrapping joint model ---")
        coef_samples_joint = bootstrap_pls_coefs(
            X, Y, best_n_joint, keep_x=best_kx_joint, n_bootstrap=n_bootstrap
        )
        sig_mask_joint, _, _ = compute_significance_mask(coef_samples_joint)
        n_sig = sig_mask_joint.sum()
        print(f"  Significant pixels: {n_sig}/{len(sig_mask_joint)} "
              f"({100*n_sig/len(sig_mask_joint):.1f}%)")
    else:
        sig_mask_joint = None

    # Per-run figures
    if sparse:
        plot_cv_heatmap(
            cv_joint_2d, best_n_joint, best_kx_joint,
            fig_dir / f"pls_cv_heatmap_{fields_slug}_{depvar}.png",
            title_suffix=f"  |  {'+'.join(error_fields)} → {depvar}",
        )
    else:
        plot_cv_curve(
            cv_joint, best_n_joint,
            fig_dir / f"pls_cv_curve_{fields_slug}_{depvar}.png",
            title_suffix=f"\n{'+'.join(error_fields)}  →  {depvar}",
        )
    plot_component_maps(
        pls_joint, pixel_coords, offsets, error_fields,
        fig_dir / f"pls_component_maps_{fields_slug}{mode_slug}_{depvar}.png",
        n_show=4, depvar=depvar,
    )
    plot_coefficient_map(
        pls_joint, pixel_coords, offsets, error_fields,
        fig_dir / f"pls_coefficient_map_{fields_slug}{mode_slug}_{depvar}.png",
        cv_r2=cv_joint[best_n_joint]["r2_mean"],
        best_n=best_n_joint,
        depvar=depvar,
        sig_mask=sig_mask_joint,
    )

    # ── Optional comparison against each single field ──────────────────────
    if compare and len(error_fields) > 1:
        print("\n--- Comparison mode: fitting each field individually ---")

        all_cv_results = [cv_joint]
        all_best_ns    = [best_n_joint]
        all_labels     = ["Joint: " + " + ".join(error_fields)]
        pls_runs       = [("Joint", pls_joint, offsets, error_fields)]
        all_sig_masks  = [sig_mask_joint]

        for field in error_fields:
            print(f"\n  Field: {field}")
            X_s, Y_s, pc_s, pi_s, hi_s, off_s = load_and_prepare(
                months, [field], depvar
            )
            if sparse:
                cv_s_2d, best_n_s, best_kx_s = cv_sweep_sparse(
                    X_s, Y_s, n_components_grid, keep_x_grid
                )
                cv_s = _collapse_sparse_cv(cv_s_2d, best_kx_s)
            else:
                cv_s, best_n_s = cv_sweep(X_s, Y_s, n_components_grid)
                best_kx_s = None
            pls_s = fit_pls(X_s, Y_s, best_n_s, keep_x=best_kx_s)

            # Bootstrap significance for this single-field model
            if n_bootstrap > 0:
                print(f"\n--- Bootstrapping single-field model: {field} ---")
                coef_samples_s = bootstrap_pls_coefs(
                    X_s, Y_s, best_n_s, keep_x=best_kx_s, n_bootstrap=n_bootstrap
                )
                sig_mask_s, _, _ = compute_significance_mask(coef_samples_s)
                n_sig_s = sig_mask_s.sum()
                print(f"  Significant pixels: {n_sig_s}/{len(sig_mask_s)} "
                      f"({100*n_sig_s/len(sig_mask_s):.1f}%)")
            else:
                sig_mask_s = None

            # Per-field individual figures
            if sparse:
                plot_cv_heatmap(
                    cv_s_2d, best_n_s, best_kx_s,
                    fig_dir / f"pls_cv_heatmap_{field}_{depvar}.png",
                    title_suffix=f"  |  {_label(field)} → {depvar}",
                )
            else:
                plot_cv_curve(
                    cv_s, best_n_s,
                    fig_dir / f"pls_cv_curve_{field}_{depvar}.png",
                    title_suffix=f"\n{_label(field)}  →  {depvar}",
                )
            plot_coefficient_map(
                pls_s, pc_s, off_s, [field],
                fig_dir / f"pls_coefficient_map_{field}{mode_slug}_{depvar}.png",
                cv_r2=cv_s[best_n_s]["r2_mean"],
                best_n=best_n_s,
                depvar=depvar,
                sig_mask=sig_mask_s,
            )

            all_cv_results.append(cv_s)
            all_best_ns.append(best_n_s)
            all_labels.append(_label(field))
            pls_runs.append((_label(field), pls_s, off_s, [field]))
            all_sig_masks.append(sig_mask_s)

        # Comparison figures use the joint model's pixel_coords as reference
        plot_comparison_cv(
            all_cv_results, all_best_ns, all_labels,
            fig_dir / f"pls_comparison_cv_{fields_slug}{mode_slug}_{depvar}.png",
            depvar=depvar,
        )
        plot_comparison_coefs(
            pls_runs, all_labels, pixel_coords,
            fig_dir / f"pls_comparison_coefs_{fields_slug}{mode_slug}_{depvar}.png",
            depvar=depvar,
            sig_masks=all_sig_masks,
        )

    print(f"\nDone. Figures written to: {fig_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="PLS spatial analysis for ERCOT forecast errors."
    )
    parser.add_argument(
        "--fields", nargs="+", default=DEFAULT_FIELDS,
        metavar="FIELD",
        help=(
            "One or more forecast error fields to use.  Multiple fields are "
            "stacked horizontally.  "
            f"Default: {DEFAULT_FIELDS}"
        ),
    )
    parser.add_argument(
        "--depvar", default=DEPVAR,
        help=f"Outcome variable (default: {DEPVAR})",
    )
    parser.add_argument(
        "--months", nargs="+", type=int, metavar="M",
        help="Month numbers 1–12 to include (default: all 12).",
    )
    parser.add_argument(
        "--compare", action="store_true",
        help=(
            "When multiple fields are given, also fit each field individually "
            "and produce comparison plots."
        ),
    )
    parser.add_argument(
        "--bootstrap", type=int, default=N_BOOTSTRAP, metavar="B",
        help=(
            f"Number of bootstrap draws for coefficient significance testing "
            f"(default: {N_BOOTSTRAP}). Set to 0 to skip."
        ),
    )
    parser.add_argument(
        "--sparse", action="store_true",
        help=(
            "Fit Sparse PLS (L1-penalised weights) instead of dense PLS, "
            "cross-validating jointly over (n_components, keep_x).  Each "
            "component loads on at most keep_x pixels."
        ),
    )
    parser.add_argument(
        "--keep-x-grid", nargs="+", type=int, default=KEEP_X_GRID, metavar="K",
        help=(
            "Candidate keep_x values (nonzero pixels per component) for the "
            f"sparse CV sweep.  Default: {KEEP_X_GRID}.  Ignored without --sparse."
        ),
    )
    args = parser.parse_args()

    months = [(2025, m) for m in args.months] if args.months else ALL_MONTHS
    run(
        months=months,
        error_fields=args.fields,
        depvar=args.depvar,
        compare=args.compare,
        n_bootstrap=args.bootstrap,
        sparse=args.sparse,
        keep_x_grid=args.keep_x_grid,
    )
