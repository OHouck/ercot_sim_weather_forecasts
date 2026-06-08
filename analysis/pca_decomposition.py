"""
EOF/PCA spatial decomposition of ERCOT forecast error fields.

Loads ERA5 forecast error NetCDFs (processed_data/forecast_errors_era5/),
fits EOFs per spatial channel on a training split using xeofs, projects
scores to all hours, and saves results to disk for downstream analysis in
pca_mode_analysis.py.

Outputs written to processed/pca/ and figures/pca_analysis/:
  pca_scores_K{K}.parquet       — PC scores per channel (valid_time × PC cols)
  pca_loadings_K{K}.npz         — spatial loadings, variance fractions, lat/lon
  pca_variance_explained.csv    — explained variance per field × mode
  pca_regime_mean.parquet       — hourly system-mean ERA5 wind / temperature
  pca_outcomes.parquet          — all outcome columns aligned to common hours
  pca_variance_explained.png    — bar chart of variance per channel
  pca_component_maps.png        — spatial heatmaps of leading EOF loadings

Usage:
    uv run python -m analysis.pca_decomposition
    uv run python -m analysis.pca_decomposition --n_components 10
"""

import argparse
import importlib
import importlib.util
import sys
import warnings
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import xarray as xr

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories, DEFAULT_WEATHER_ZONE_SHP

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# ── Configuration ──────────────────────────────────────────────────────────────

DEPVAR = "economic_congestion_cost"

ERROR_FIELDS    = ["temp_error_1h", "temp_error_0h",
                   "wspd100_error_1h", "wspd100_error_0h"]
REALIZED_FIELDS = ["era5_temp", "era5_wspd100"]
ALL_FIELDS      = ERROR_FIELDS + REALIZED_FIELDS

FIELD_LABELS = {
    "wspd100_error_1h": "HRRR 1h 100m Wind Error",
    "temp_error_1h":    "HRRR 1h Temp Error",
    "wspd100_error_0h": "GFS Day-Ahead 100m Wind Error",
    "temp_error_0h":    "GFS Day-Ahead Temp Error",
    "era5_wspd100":     "Realized 100m Wind Speed (ERA5)",
    "era5_temp":        "Realized Temperature (ERA5)",
}

ALL_MONTHS    = [(2025, m) for m in range(1, 13)]
N_COMPONENTS  = 5
CHUNK_DAYS    = 5
TRAIN_FRAC    = 0.70
RANDOM_STATE  = 42
HAC_MAXLAGS   = 24
K_SWEEP_VALUES = [5, 10, 20]
LASSO_ALPHAS  = np.logspace(-4, 2, 60)

CHANNEL_SPEC = {
    "temp_error_1h":    ("hrrr", "temp_error",     1),
    "wspd100_error_1h": ("hrrr", "wspd100_error",  1),
    "temp_error_0h":    ("gfs",  "temp_error",     0),
    "wspd100_error_0h": ("gfs",  "wspd100_error",  0),
    "era5_temp":        ("hrrr", "era5_temp",      1),
    "era5_wspd100":     ("hrrr", "era5_wspd100",   1),
}
OUTCOMES_CSV = "system_hourly_outcomes_2025.csv"

COLOR_P001 = "#00008B"
COLOR_P005 = "#6699CC"
COLOR_NSIG = "#AAAAAA"


# ── Utilities ──────────────────────────────────────────────────────────────────


def _r2(y_true, y_pred):
    """Coefficient of determination."""
    ss_res = np.sum((y_true - y_pred) ** 2)
    ss_tot = np.sum((y_true - y_true.mean()) ** 2)
    return float(1 - ss_res / max(ss_tot, 1e-12))


def _normalize_comp(comp):
    """Scale a PCA loading vector to max absolute value = 1."""
    m = np.abs(comp).max()
    return comp / m if m > 1e-10 else comp


def _sig_stars(p):
    """Return significance stars string for a p-value."""
    if p < 0.001:
        return "***"
    if p < 0.01:
        return "**"
    if p < 0.05:
        return "*"
    return ""


def _draw_texas(ax):
    """Add Texas coastline and state borders when Cartopy is available."""
    import importlib
    import importlib.util

    if importlib.util.find_spec("cartopy.crs") is None or importlib.util.find_spec("cartopy.feature") is None:
        ax.set_xlim(-106.7, -93.4)
        ax.set_ylim(25.7, 36.6)
        return

    cfeature = importlib.import_module("cartopy.feature")
    ccrs = importlib.import_module("cartopy.crs")
    ax.set_extent([-106.7, -93.4, 25.7, 36.6], crs=ccrs.PlateCarree())
    ax.coastlines(resolution="10m", linewidth=0.5, color="k")
    ax.add_feature(cfeature.STATES, linewidth=0.4, edgecolor="0.4")


def _get_cartopy_crs():
    """Return cartopy.crs if installed, otherwise None."""
    if importlib.util.find_spec("cartopy.crs") is None:
        return None
    return importlib.import_module("cartopy.crs")


def _grid_marker_size(lons, base_s=3, base_step=0.1):
    """Scatter marker area scaled quadratically to grid spacing; ERA5 0.1° → s=3."""
    unique_lons = np.unique(lons)
    if len(unique_lons) < 2:
        return base_s
    step = float(np.median(np.diff(unique_lons)))
    return max(1.0, base_s * (step / base_step) ** 2)

def _adjust_sign(eofs_da, scores=None):
    """Orient each EOF so its net loading is positive (sum of loadings > 0).

    This guarantees that cells with positive loadings are associated with
    higher-than-average values of the underlying field: for a monopole all
    loadings are positive so positive score = widespread high field; for a
    dipole the side covering more area (or higher total magnitude) gets
    positive loadings, so positive score = that side is above average.

    The criterion is purely spatial — it depends only on the EOF pattern, not
    on the time dimension — so it holds equally for all modes including
    higher-order dipoles and tripoles.

    Parameters
    ----------
    eofs_da : xr.DataArray (mode, latitude, longitude)
    scores  : ndarray (T, n_modes) or None — PC scores; flipped in place
              to stay consistent with the (possibly flipped) EOFs

    Returns
    -------
    eofs_da : new DataArray with signs applied (input is not modified)
    signs   : ndarray (n_modes,) of ±1 — multiplier applied to each mode
    """
    eofs_da = eofs_da.copy()
    modes = list(eofs_da["mode"].values)
    signs = np.ones(len(modes), dtype=np.float32)

    for i, m in enumerate(modes):
        eof_flat = np.nan_to_num(eofs_da.sel(mode=m).values.ravel(), nan=0.0)
        if eof_flat.sum() < 0:
            signs[i] = -1.0
            eofs_da.loc[{"mode": m}] *= -1
            if scores is not None:
                scores[:, i] *= -1

    return eofs_da, signs


def build_ercot_mask(latitudes, longitudes, shp_path=DEFAULT_WEATHER_ZONE_SHP):
    """Create a 2D boolean mask (True = inside ERCOT) for a regular lat/lon grid.

    Parameters
    ----------
    latitudes  : ndarray (n_lat,)
    longitudes : ndarray (n_lon,)
    shp_path   : str — path to ERCOT weather-zone shapefile

    Returns
    -------
    ndarray bool (n_lat, n_lon)
    """
    import geopandas as gpd
    zones = gpd.read_file(shp_path).to_crs("EPSG:4326")
    ercot_poly = zones.geometry.union_all()
    lon_2d, lat_2d = np.meshgrid(longitudes, latitudes)
    points = gpd.GeoDataFrame(
        geometry=gpd.points_from_xy(lon_2d.ravel(), lat_2d.ravel()),
        crs="EPSG:4326",
    )
    within = points.geometry.within(ercot_poly)
    return within.values.reshape(len(latitudes), len(longitudes))


def _make_pca_adapter(eofs_da, evr, K_eff, land_mask):
    """Build a sklearn-PCA-compatible namespace from EOF loadings and variance fractions.

    Parameters
    ----------
    eofs_da   : xr.DataArray of EOF loadings (mode, latitude, longitude)
    evr       : array-like — explained variance ratios, one per mode
    K_eff     : int
    land_mask : ndarray bool (n_lat, n_lon) — True where cells are inside ERCOT and non-NaN

    Returns
    -------
    SimpleNamespace with components_ (K_eff, n_cells) and explained_variance_ratio_
    """
    eofs_vals  = np.asarray(eofs_da.values, dtype=float)
    components = eofs_vals.reshape(K_eff, -1)[:, land_mask.ravel()]
    return SimpleNamespace(
        components_=components,
        explained_variance_ratio_=np.asarray(evr, dtype=float),
    )


# ── Data loading ───────────────────────────────────────────────────────────────


def _open_error_nc(model, year, month, dirs):
    """Open one ERA5 error NetCDF, returning xr.Dataset or None if missing.

    Converts valid_time from UTC to US/Central tz-naive to match outcomes CSV.

    Parameters
    ----------
    model : str — "hrrr" or "gfs"
    year  : int
    month : int
    dirs  : dict from setup_directories()

    Returns
    -------
    xr.Dataset or None
    """
    path = (
        Path(dirs["processed"]) / "forecast_errors_era5" / model
        / str(year) / f"{month:02d}" / f"era5_errors_{year}{month:02d}.nc"
    )
    if not path.exists():
        print(f"  [WARNING] Missing: {path}")
        return None
    ds = xr.open_dataset(path)
    utc_idx = pd.DatetimeIndex(ds["valid_time"].values).tz_localize("UTC")
    central  = utc_idx.tz_convert("US/Central").tz_localize(None).floor("h")
    _, keep  = np.unique(central, return_index=True)
    ds       = ds.isel(valid_time=keep)
    ds["valid_time"] = central[keep]
    return ds


def load_channel_fields(months, dirs):
    """Load all EOF channels from ERA5 error NetCDFs and the outcomes CSV.

    Parameters
    ----------
    months : list of (year, month)
    dirs   : dict from setup_directories()

    Returns
    -------
    dict with keys:
        channel_da  : dict {channel: xr.DataArray (valid_time, latitude, longitude)}
        nan_all     : dict {channel: ndarray bool (n_lat, n_lon)}
        hours       : pd.DatetimeIndex
        regime_mean : pd.DataFrame — era5_temp / era5_wspd100 per hour
        outcomes    : pd.DataFrame — all CSV columns reindexed to hours
        dep_series  : pd.Series — outcomes[DEPVAR]
    """
    model_ds = {}
    for model in ("hrrr", "gfs"):
        parts = [_open_error_nc(model, y, m, dirs) for y, m in months]
        parts = [p for p in parts if p is not None]
        if not parts:
            raise RuntimeError(f"No ERA5 error files found for model={model}")
        ds = xr.concat(parts, dim="valid_time")
        _, keep_idx = np.unique(ds["valid_time"].values, return_index=True)
        model_ds[model] = ds.isel(valid_time=keep_idx)
        print(f"  {model.upper()}: {model_ds[model].sizes['valid_time']} hours loaded")

    outcomes_path = Path(dirs["processed"]) / OUTCOMES_CSV
    outcomes_raw  = pd.read_csv(outcomes_path)
    outcomes_raw["valid_time"] = pd.to_datetime(outcomes_raw["valid_time"])
    outcomes_raw  = outcomes_raw.set_index("valid_time")
    print(f"  CSV: {len(outcomes_raw)} hours loaded")

    raw_das     = {}
    time_arrays = []
    for channel, (model, var, lead) in CHANNEL_SPEC.items():
        da = model_ds[model][var].sel(lead_hours=lead)
        raw_das[channel] = da
        time_arrays.append(da["valid_time"].values.astype("datetime64[ns]"))

    csv_times    = outcomes_raw.index.values.astype("datetime64[ns]")
    common_times = time_arrays[0]
    for arr in time_arrays[1:]:
        common_times = np.intersect1d(common_times, arr)
    common_times = np.intersect1d(common_times, csv_times)
    hours = pd.DatetimeIndex(sorted(common_times))

    hrrr_n, gfs_n = model_ds["hrrr"].sizes["valid_time"], model_ds["gfs"].sizes["valid_time"]
    print(f"  HRRR={hrrr_n}  GFS={gfs_n}  CSV={len(outcomes_raw)}  common={len(hours)}")
    if len(hours) < int(0.5 * min(hrrr_n, gfs_n)):
        raise RuntimeError(
            f"Hour overlap {len(hours)} < 50% of min source — possible timezone mismatch."
        )

    print(f"  Building ERCOT service-area mask from: {DEFAULT_WEATHER_ZONE_SHP}")
    ercot_grid_mask = {}
    for model in ("hrrr", "gfs"):
        ref_ch = next(ch for ch, (m, _, _) in CHANNEL_SPEC.items() if m == model)
        da_ref = raw_das[ref_ch]
        mask   = build_ercot_mask(
            da_ref["latitude"].values.astype(float),
            da_ref["longitude"].values.astype(float),
        )
        print(f"  ERCOT mask ({model}): {int(mask.sum())}/{mask.size} cells inside")
        ercot_grid_mask[model] = mask

    nan_all_dict = {}
    channel_da   = {}
    for channel, (model, _, _) in CHANNEL_SPEC.items():
        da  = raw_das[channel].sel(valid_time=hours)
        outside = xr.DataArray(
            ~ercot_grid_mask[model],
            dims=["latitude", "longitude"],
            coords={"latitude": da["latitude"], "longitude": da["longitude"]},
        )
        nan_combined      = da.isnull().all("valid_time") | outside
        nan_all_dict[channel] = nan_combined.values
        channel_da[channel]   = da.fillna(0.0).where(~nan_combined)
        print(f"  {FIELD_LABELS.get(channel, channel)}: "
              f"{int((~nan_combined).values.sum())} cells, {len(hours)} hours")

    regime_mean = pd.DataFrame({
        "era5_wspd100": channel_da["era5_wspd100"].mean(("latitude", "longitude")).to_series(),
        "era5_temp":    channel_da["era5_temp"].mean(("latitude", "longitude")).to_series(),
    }, index=hours)

    outcomes   = outcomes_raw.reindex(hours)
    dep_series = outcomes[DEPVAR]

    return {
        "channel_da":  channel_da,
        "nan_all":     nan_all_dict,
        "hours":       hours,
        "regime_mean": regime_mean,
        "outcomes":    outcomes,
        "dep_series":  dep_series,
    }


def make_chunk_splits(hour_idx, chunk_days=CHUNK_DAYS, train_frac=TRAIN_FRAC,
                      seed=RANDOM_STATE):
    """Split a time index into train/test via shuffled temporal blocks.

    Parameters
    ----------
    hour_idx   : pd.DatetimeIndex
    chunk_days : int
    train_frac : float
    seed       : int

    Returns
    -------
    train_mask, test_mask : ndarray bool (T,)
    """
    dates        = hour_idx.normalize()
    unique_dates = pd.DatetimeIndex(sorted(dates.unique()))
    chunk_ids    = np.arange(len(unique_dates)) // chunk_days
    n_chunks     = int(chunk_ids.max()) + 1

    rng          = np.random.default_rng(seed)
    shuffled     = rng.permutation(n_chunks)
    n_train      = max(1, int(np.floor(n_chunks * train_frac)))
    train_chunks = set(shuffled[:n_train].tolist())

    date_to_chunk = pd.Series(chunk_ids, index=unique_dates)
    row_chunks    = dates.map(date_to_chunk).values
    train_mask    = np.isin(row_chunks, list(train_chunks))
    return train_mask, ~train_mask


# ── PCA fitting ────────────────────────────────────────────────────────────────


def fit_pca_channels(bundle, fields, K=N_COMPONENTS, seed=RANDOM_STATE):
    """Fit EOFs per spatial channel on training hours; project all hours.

    EOFs are fitted on the training split; full-sample scores are genuine
    projections onto in-sample basis vectors.

    Parameters
    ----------
    bundle : dict from load_channel_fields
    fields : list of str — channel names (must be in CHANNEL_SPEC)
    K      : int — EOF modes per channel
    seed   : int

    Returns
    -------
    scores_dict : dict {field: ndarray (T, K)}
    pca_dict    : dict {field: SimpleNamespace} with .components_ and .explained_variance_ratio_
    hours       : pd.DatetimeIndex
    lat_dict    : dict {field: ndarray} — per-cell latitudes aligned with components_
    lon_dict    : dict {field: ndarray} — per-cell longitudes
    var_df      : pd.DataFrame — explained variance per field × mode
    """
    hours      = bundle["hours"]
    train_mask, _ = make_chunk_splits(hours, seed=seed)
    train_idx  = np.where(train_mask)[0]
    n_train    = int(train_mask.sum())
    print(f"  Common hours: {len(hours)}  train={n_train}  test={len(hours) - n_train}")

    # Precompute meshgrids once per model grid (HRRR and GFS have different resolutions)
    model_grid = {}
    for field in fields:
        model = CHANNEL_SPEC[field][0]
        if model not in model_grid:
            da_ref = bundle["channel_da"][field]
            model_grid[model] = np.meshgrid(
                da_ref["longitude"].values, da_ref["latitude"].values
            )

    scores_dict, pca_dict, lat_dict, lon_dict = {}, {}, {}, {}
    var_rows = []
    try:
        from xeofs.single import EOF
    except ImportError:
        raise ImportError(
            "xeofs is required to fit PCA channels. Install project dependencies with `uv sync`."
        )

    for field in fields:
        da        = bundle["channel_da"][field]
        land_mask = ~bundle["nan_all"][field]
        n_cells   = int(land_mask.sum())
        lon_2d, lat_2d = model_grid[CHANNEL_SPEC[field][0]]
        lat_dict[field] = lat_2d[land_mask].astype(float)
        lon_dict[field] = lon_2d[land_mask].astype(float)

        K_eff    = min(K, n_train - 1, n_cells)
        da_train = da.isel(valid_time=train_idx)

        eof_model = EOF(n_modes=K_eff, center=True, random_state=seed)
        eof_model.fit(da_train, dim="valid_time")

        # components() returns (mode, latitude, longitude)
        eofs_da = eof_model.components()
        # transform() centers using training mean internally and projects all hours onto the fitted
        # basis; xeofs returns (mode, valid_time) so transpose to the (T, K) layout downstream expects
        scores = np.asarray(
            eof_model.transform(da).transpose("valid_time", "mode").values, dtype=np.float32)

        # enforce that positive PC scores correspond to above-average values in the original variable;
        # must be applied to EOFs and scores together since scores were computed against the un-flipped basis
        eofs_da, _ = _adjust_sign(eofs_da, scores=scores)

        pca_dict[field]    = _make_pca_adapter(eofs_da, eof_model.explained_variance_ratio().values, K_eff, land_mask)
        scores_dict[field] = scores

        vr     = pca_dict[field].explained_variance_ratio_
        cumvar = np.cumsum(vr) * 100
        for k in range(K_eff):
            var_rows.append({
                "field": field, "mode": k + 1,
                "var_pct": float(vr[k] * 100), "cumvar_pct": float(cumvar[k]),
            })
        print(f"  {FIELD_LABELS.get(field, field)}: {K_eff} modes, "
              f"cumvar={cumvar[-1]:.1f}% ({K_eff} modes / {n_cells} cells)")

    return scores_dict, pca_dict, hours, lat_dict, lon_dict, pd.DataFrame(var_rows)


# ── Disk I/O ───────────────────────────────────────────────────────────────────


def save_pca_results(scores_dict, pca_dict, lat_dict, lon_dict,
                     var_df, bundle, hours, K, pca_dir):
    """Save PCA decomposition artifacts to disk.

    Parameters
    ----------
    scores_dict : dict {field: ndarray (T, K)}
    pca_dict    : dict {field: SimpleNamespace}
    lat_dict    : dict {field: ndarray}
    lon_dict    : dict {field: ndarray}
    var_df      : pd.DataFrame
    bundle      : dict from load_channel_fields
    hours       : pd.DatetimeIndex
    K           : int
    pca_dir     : Path
    """
    pca_dir = Path(pca_dir)
    pca_dir.mkdir(parents=True, exist_ok=True)

    scores_df = pd.DataFrame(
        {f"{field}_PC{k+1}": scores_dict[field][:, k]
         for field in scores_dict
         for k in range(scores_dict[field].shape[1])},
        index=hours,
    )
    scores_df.index.name = "valid_time"
    scores_df.to_parquet(pca_dir / f"pca_scores_K{K}.parquet")
    print(f"  Saved: {pca_dir / f'pca_scores_K{K}.parquet'}")

    npz_data = {}
    for field, pca in pca_dict.items():
        npz_data[f"{field}__components"] = pca.components_
        npz_data[f"{field}__evr"]        = pca.explained_variance_ratio_
        npz_data[f"{field}__lat"]        = lat_dict[field]
        npz_data[f"{field}__lon"]        = lon_dict[field]
    np.savez_compressed(pca_dir / f"pca_loadings_K{K}.npz", **npz_data)
    print(f"  Saved: {pca_dir / f'pca_loadings_K{K}.npz'}")

    var_df.to_csv(pca_dir / "pca_variance_explained.csv", index=False)
    print(f"  Saved: {pca_dir / 'pca_variance_explained.csv'}")

    bundle["regime_mean"].to_parquet(pca_dir / "pca_regime_mean.parquet")
    bundle["outcomes"].to_parquet(pca_dir / "pca_outcomes.parquet")
    print(f"  Saved: {pca_dir / 'pca_regime_mean.parquet'}, {pca_dir / 'pca_outcomes.parquet'}")


def load_pca_results(K, pca_dir):
    """Load PCA decomposition results saved by save_pca_results.

    Parameters
    ----------
    K       : int — number of modes used in decomposition
    pca_dir : Path

    Returns
    -------
    dict with keys: scores_dict, pca_dict, lat_dict, lon_dict, hours,
                    regime_mean, outcomes, dep_series, var_df
    """
    pca_dir = Path(pca_dir)

    scores_df = pd.read_parquet(pca_dir / f"pca_scores_K{K}.parquet")
    hours     = pd.DatetimeIndex(scores_df.index)

    field_k_pairs = []
    for col in scores_df.columns:
        f, k_str = col.rsplit("_PC", 1)
        field_k_pairs.append((f, int(k_str) - 1))
    fields_seen = list(dict.fromkeys(f for f, _ in field_k_pairs))

    scores_dict = {}
    for field in fields_seen:
        cols_for_field = [(col, k) for col, (f, k) in zip(scores_df.columns, field_k_pairs)
                          if f == field]
        arr = np.zeros((len(scores_df), len(cols_for_field)), dtype=np.float32)
        for col, k in cols_for_field:
            arr[:, k] = scores_df[col].values
        scores_dict[field] = arr

    npz         = np.load(pca_dir / f"pca_loadings_K{K}.npz")
    npz_fields  = list(dict.fromkeys(key.rsplit("__", 1)[0] for key in npz.files))
    pca_dict, lat_dict, lon_dict = {}, {}, {}
    for field in npz_fields:
        pca_dict[field] = SimpleNamespace(
            components_=npz[f"{field}__components"],
            explained_variance_ratio_=npz[f"{field}__evr"],
        )
        lat_dict[field] = npz[f"{field}__lat"]
        lon_dict[field] = npz[f"{field}__lon"]

    regime_mean = pd.read_parquet(pca_dir / "pca_regime_mean.parquet")
    outcomes    = pd.read_parquet(pca_dir / "pca_outcomes.parquet")
    dep_series  = outcomes[DEPVAR]
    var_df      = pd.read_csv(pca_dir / "pca_variance_explained.csv")

    return {
        "scores_dict": scores_dict,
        "pca_dict":    pca_dict,
        "lat_dict":    lat_dict,
        "lon_dict":    lon_dict,
        "hours":       hours,
        "regime_mean": regime_mean,
        "outcomes":    outcomes,
        "dep_series":  dep_series,
        "var_df":      var_df,
    }


# ── Visualization ──────────────────────────────────────────────────────────────


def plot_variance_explained(var_df, fig_dir):
    """Bar chart of individual and cumulative variance per channel.

    Parameters
    ----------
    var_df  : pd.DataFrame — [field, mode, var_pct, cumvar_pct]
    fig_dir : Path
    """
    fields = list(var_df["field"].unique())
    fig, axes = plt.subplots(1, len(fields), figsize=(len(fields) * 3.2, 3.5))
    if len(fields) == 1:
        axes = [axes]
    for ax, field in zip(axes, fields):
        sub = var_df[var_df["field"] == field]
        ax.bar(sub["mode"], sub["var_pct"], color="#3498db", alpha=0.7)
        ax2 = ax.twinx()
        ax2.plot(sub["mode"], sub["cumvar_pct"], "ro-", markersize=4)
        ax2.set_ylim(0, 105)
        ax2.set_ylabel("Cumul. var (%)", fontsize=7)
        ax.set_xlabel("PC mode", fontsize=8)
        ax.set_ylabel("Var explained (%)", fontsize=8)
        ax.set_title(FIELD_LABELS.get(field, field), fontsize=7.5)
        ax.set_xticks(sub["mode"])
    fig.suptitle("Variance Explained per PCA Channel", fontsize=9)
    plt.tight_layout()
    out = fig_dir / "pca_variance_explained.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out}")


def plot_pca_maps(pca_dict, lat_dict, lon_dict, fig_dir, K_show=5,
                  sig_levels=None, coef_levels=None):
    """Spatial heatmaps of the leading PCA mode loadings for each field.

    Panels for modes significant in OLS are outlined (dark blue: p < 0.01,
    light blue: p < 0.05); title includes OLS coefficient and stars when provided.

    Parameters
    ----------
    pca_dict    : dict {field: fitted PCA}
    lat_dict    : dict {field: ndarray}
    lon_dict    : dict {field: ndarray}
    fig_dir     : Path
    K_show      : int
    sig_levels  : dict {(field, mode_1based): p_value} or None
    coef_levels : dict {(field, mode_1based): coef} or None
    """
    from matplotlib.colors import TwoSlopeNorm

    ccrs = _get_cartopy_crs()
    norm   = TwoSlopeNorm(vmin=-1.0, vcenter=0, vmax=1.0)
    fields = list(pca_dict.keys())
    n_rows = len(fields)
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

        lons_f   = lon_dict[field]
        marker_s = _grid_marker_size(lons_f)

        for k in range(min(K_show, len(vr))):
            ax     = axes[r, k]
            comp_n = _normalize_comp(pca.components_[k])
            scatter_kwargs = {
                "c": comp_n,
                "cmap": "RdBu_r",
                "norm": norm,
                "s": marker_s,
                "rasterized": True,
            }
            if crs_pc is not None:
                scatter_kwargs["transform"] = crs_pc
            ax.scatter(lons_f, lat_dict[field], **scatter_kwargs)
            _draw_texas(ax)

            p_val = (sig_levels or {}).get((field, k + 1), 1.0)
            coef  = (coef_levels or {}).get((field, k + 1))
            if p_val < 0.01:
                bc, blw = COLOR_P001, 3.0
            elif p_val < 0.05:
                bc, blw = COLOR_P005, 2.0
            else:
                bc, blw = "lightgray", 0.5
            for spine in ax.spines.values():
                spine.set_edgecolor(bc)
                spine.set_linewidth(blw)
                spine.set_visible(True)

            coef_line  = (f"\nβ={coef:+.2f}{_sig_stars(p_val)}"
                          if (p_val < 0.05 and coef is not None) else "")
            mode_title = f"PC{k+1} (var={vr[k]*100:.1f}%){coef_line}"
            if k == 0:
                ax.set_title(f"{label}\n{mode_title}", fontsize=6.5, pad=3)
            else:
                ax.set_title(mode_title, fontsize=7)

    fig.suptitle(
        "PCA Mode Loadings per Field  —  dark blue border: p < 0.01, "
        "light blue: p < 0.05 (primary OLS)",
        fontsize=8.5, y=1.01,
    )
    plt.tight_layout()
    out = fig_dir / "pca_component_maps.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out}")


# ── Main ───────────────────────────────────────────────────────────────────────


def run_pca_decomposition(months=None, K=N_COMPONENTS):
    """Load ERA5 errors, fit EOFs per channel, and save decomposition to disk.

    Steps:
    1. Load ERA5 error NetCDFs and outcomes CSV; build common hour index.
    2. Fit EOFs per channel on training hours only; project all hours.
    3. Save scores, loadings, and supporting data to tables_dir.
    4. Plot variance explained and spatial loading maps.

    Parameters
    ----------
    months : list of (year, month) — defaults to all 12 months of 2025
    K      : int — EOF modes per channel

    Returns
    -------
    dict with scores_dict, pca_dict, lat_dict, lon_dict, hours, var_df, bundle
    """
    if months is None:
        months = ALL_MONTHS

    dirs       = setup_directories()
    fig_dir    = Path(dirs["figures"]) / "pca_analysis"
    tables_dir = Path(dirs["tables"])
    pca_dir    = Path(dirs["processed"]) / "pca"
    fig_dir.mkdir(parents=True, exist_ok=True)
    tables_dir.mkdir(parents=True, exist_ok=True)
    pca_dir.mkdir(parents=True, exist_ok=True)

    print("\n=== Step 1: Loading ERA5 error fields and outcomes CSV ===")
    bundle = load_channel_fields(months, dirs)

    print("\n=== Step 2: Fitting EOFs per channel ===")
    scores_dict, pca_dict, hours, lat_dict, lon_dict, var_df = fit_pca_channels(
        bundle, ALL_FIELDS, K=K
    )

    print("\n=== Step 3: Saving decomposition to disk ===")
    save_pca_results(scores_dict, pca_dict, lat_dict, lon_dict,
                     var_df, bundle, hours, K, pca_dir)

    print("\n=== Step 4: Generating figures ===")
    plot_variance_explained(var_df, fig_dir)
    plot_pca_maps(pca_dict, lat_dict, lon_dict, fig_dir, K_show=min(K, 5))

    return {
        "scores_dict": scores_dict,
        "pca_dict":    pca_dict,
        "lat_dict":    lat_dict,
        "lon_dict":    lon_dict,
        "hours":       hours,
        "var_df":      var_df,
        "bundle":      bundle,
    }


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="EOF/PCA decomposition for ERCOT forecast error fields"
    )
    parser.add_argument("--n_components", type=int, default=N_COMPONENTS,
                        help=f"EOF modes per channel (default: {N_COMPONENTS})")
    args = parser.parse_args()
    run_pca_decomposition(K=args.n_components)


if __name__ == "__main__":
    main()
