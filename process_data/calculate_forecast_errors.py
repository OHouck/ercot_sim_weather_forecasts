"""calculate_forecast_errors.py — Compute forecast errors at weather station locations
or against ERA5-Land gridded reanalysis.

Merges gridded weather forecasts (HRRR or GFS) with either ISD weather station
observations or ERA5-Land reanalysis. For each forecast file, loads the gridded
xarray dataset and uses a spatial join to extract forecast values at observation
coordinates. Computes forecast error = forecast - observed.

Supports two forecast models:
  - HRRR (3km):   calculate_station_errors_for_month(model='hrrr')
                   Default: 1h lead only. Override with lead_hours=[1, 18].
  - GFS  (0.25°): calculate_station_errors_for_month(model='gfs')
                   Day-ahead: all GFS leads (f018-f041 from 12z cycle) are
                   collapsed to lead_hours=0. Each file predicts a unique hour
                   of the following day; "day-ahead" is the conceptual label.

And two ground-truth sources:
  - ISD weather stations: calculate_station_errors_for_month()
  - ERA5-Land reanalysis: calculate_era5_errors_for_month()

Lead time convention:
  - lead_hours=0 means "day-ahead" — forecast issued at 12z the previous day.
    The actual model lead varies (18–41h depending on hour-of-day) but is
    collapsed to a single pseudo-lead for clean, dense output.
  - lead_hours=1 means HRRR 1-hour-ahead (short-range forecast).

Regridding methods for the ERA5 path:
  - Bin-center averaging (HRRR): forecast resolution is finer than ERA5
    (~3 km vs ~11 km), so ~12 forecast cell centers fall within each ERA5 bin
    and are averaged.
  - Bilinear interpolation (GFS): forecast resolution is coarser than ERA5
    (~28 km vs ~11 km), so bin-averaging would leave ~60 % of ERA5 cells empty.
    xarray .interp(method='linear') smoothly fills the finer grid instead.

Timezone convention:
  All output valid_time columns are stored in **US/Central (tz-naive)**.
  Raw NetCDF files (HRRR, GFS, ERA5) remain in UTC; conversion happens at load
  time in load_forecasts() and load_all_observations(). July 2025 CDT = UTC-5,
  so 2025-07-01 12:00 UTC → 2025-07-01 07:00 (stored without tz suffix).
  ⚠ Existing CSVs written before this change stored UTC times and are stale;
    regenerate them by re-running the relevant calculate_*_errors_for_month().

Station-level output: One CSV per station in
  {processed}/forecast_errors/{model}/{year}/{month:02d}/
  Columns: station_id, valid_time [Central], lead_hours,
           forecast_temp, observed_temp, temp_error, temp_pct_error,
           forecast_wspd, observed_wspd, wspd_error, wspd_pct_error,
           forecast_wdir, observed_wdir, wdir_degree_error, lat, lon

ERA5 gridded output:
  {processed}/forecast_errors_era5/{model}/{year}/{month:02d}/
    era5_errors_{YYYYMM}.nc  — full gridded error surface (NetCDF)
    error_summary.csv         — per-cell, per-lead MAE/bias summary
"""

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr
import geopandas as gpd
from shapely.geometry import Point

sys.path.insert(0, str(Path(__file__).parent.parent))
from helper_funcs import setup_directories
from analysis.create_plots import parse_tmp, parse_wnd_speed


# ── Model configuration ──────────────────────────────────────────────────────

_MODEL_CONFIG = {
    'hrrr': {
        'data_dir': 'hrrr_data',
        'display_name': 'HRRR',
        'regrid_method': 'bin',
        'default_lead_hours': [1],    # only 1h lead by default
        'collapse_leads': False,
    },
    'gfs': {
        'data_dir': 'gfs_data',
        'display_name': 'GFS',
        'regrid_method': 'interp',
        'default_lead_hours': None,   # keep all available leads
        'collapse_leads': True,       # remap all lead_hours → 0 ("day-ahead")
    },
}


# ── Timezone utilities ────────────────────────────────────────────────────────

_CENTRAL = 'US/Central'


def _to_central(timestamps):
    """Convert UTC timestamps (tz-naive or tz-aware) to US/Central tz-naive datetimes.

    Handles DST automatically: Jul 2025 → CDT (UTC-5), Jan 2025 → CST (UTC-6).

    Args:
        timestamps: pd.Series or single value that pd.to_datetime() can parse.
                    If tz-naive, assumed to be UTC.
    Returns:
        pd.Series of tz-naive datetime64[ns] values in US/Central wall-clock time.
    """
    s = pd.to_datetime(timestamps)
    scalar = s.ndim == 0
    s = pd.Series([s]) if scalar else s
    if s.dt.tz is None:
        s = s.dt.tz_localize('UTC')
    result = s.dt.tz_convert(_CENTRAL).dt.tz_localize(None)
    return result.iloc[0] if scalar else result.reset_index(drop=True)


def _era5_central_times(ds):
    """Convert an ERA5 xarray Dataset's valid_time to US/Central, deduplicating DST fold.

    ERA5 datasets use UTC valid_time. This converts to tz-naive US/Central and
    removes the duplicate hour produced during the DST fall-back (e.g. Nov clocks-back
    maps two consecutive UTC hours to the same local time; keep the first occurrence).

    Args:
        ds: xarray Dataset with a valid_time coordinate (UTC, tz-naive).

    Returns:
        Tuple (central_dt64, keep_idx):
            central_dt64: 1D numpy datetime64[ns] array of deduplicated Central times.
            keep_idx:     1D int array of indices into ds.valid_time that were kept.
    """
    utc_times = pd.to_datetime(ds.valid_time.values)
    central = _to_central(pd.Series(utc_times))
    dt64 = pd.DatetimeIndex(central).values
    _, keep_idx = np.unique(dt64, return_index=True)
    keep_idx = np.sort(keep_idx)
    if len(keep_idx) < len(dt64):
        n_dup = len(dt64) - len(keep_idx)
        print(f"  WARNING: {n_dup} duplicate Central timestamp(s) from DST fall-back; "
              f"keeping first occurrence")
    return dt64[keep_idx], keep_idx


# ── Forecast lead-time filtering ──────────────────────────────────────────────

def _filter_forecasts(forecasts, lead_hours=None, collapse_leads=False):
    """Filter forecast records by lead time and optionally collapse to day-ahead.

    Args:
        forecasts: List of forecast dicts from load_forecasts().
        lead_hours: If not None, keep only records with lead_hours in this list.
        collapse_leads: If True, remap all lead_hours to 0 (day-ahead labeling).
    Returns:
        Filtered (and possibly remapped) list of forecast dicts.
    """
    if lead_hours is not None:
        forecasts = [r for r in forecasts if r['lead_hours'] in lead_hours]
    if collapse_leads:
        for r in forecasts:
            r['lead_hours'] = 0
    return forecasts


# ── Field parsers ─────────────────────────────────────────────────────────────

def parse_wnd_direction(wnd_str):
    """Parse ISD WND field to wind direction in degrees."""
    if pd.isna(wnd_str):
        return None
    parts = str(wnd_str).split(',')
    if len(parts) < 1 or parts[0] == '999':
        return None
    return int(parts[0])


def circular_angular_error(fc_wdir, obs_wdir):
    """Compute the shortest angular distance between two wind directions (0-360°).

    Returns the error in degrees, always in range [0, 180].
    E.g., difference between 5° and 350° is 15°, not 345°.
    """
    if np.isnan(fc_wdir) or obs_wdir is None:
        return np.nan
    diff = abs(fc_wdir - obs_wdir)
    # Take the shorter arc around the circle
    if diff > 180:
        diff = 360 - diff
    return diff


# ── Station data loading ──────────────────────────────────────────────────────

def load_stations_gdf(raw_dir):
    """Load station metadata as a GeoDataFrame with Point geometry.

    Returns GeoDataFrame with columns: station_id, lat, lon, geometry
    """
    stations = pd.read_csv(
        os.path.join(raw_dir, 'weather_stations', 'stations.csv'),
        dtype={'usaf': str, 'wban': str, 'station_id': str})
    geometry = [Point(lon, lat) for lon, lat in zip(stations['lon'], stations['lat'])]
    gdf = gpd.GeoDataFrame(stations, geometry=geometry, crs='EPSG:4326')
    return gdf


def load_all_observations(stations_gdf, year, month, raw_dir):
    """Load and resample all station observations to hourly (US/Central times).

    Returns a dict mapping station_id -> DataFrame with columns:
        valid_time [US/Central tz-naive], obs_temp, obs_wspd, obs_wdir
    """
    obs_dict = {}
    data_dir = os.path.join(raw_dir, 'weather_stations', str(year), f"{month:02d}")

    for _, row in stations_gdf.iterrows():
        station_id = row['station_id']
        fpath = os.path.join(data_dir, f"{station_id}.csv")
        if not os.path.exists(fpath):
            continue

        df = pd.read_csv(fpath, dtype={'STATION': str})
        # ISD DATE column is UTC
        df['datetime'] = pd.to_datetime(df['DATE'], utc=True)

        # Parse fields
        df['obs_temp'] = df['TMP'].apply(parse_tmp)
        df['obs_wspd'] = df['WND'].apply(parse_wnd_speed)
        df['obs_wdir'] = df['WND'].apply(parse_wnd_direction)

        # Round each observation to its nearest hour (still in UTC at this point)
        df['valid_time_utc'] = df['datetime'].dt.round('h')

        # For each hour, keep the observation closest to the top of that hour
        df['time_diff'] = (df['datetime'] - df['valid_time_utc']).abs()
        df = df.sort_values('time_diff').drop_duplicates(subset='valid_time_utc', keep='first')
        df = df.sort_values('valid_time_utc')

        # Convert to US/Central (tz-naive) — do this after dedup so we convert once
        df['valid_time'] = _to_central(df['valid_time_utc'])

        obs_dict[station_id] = df[['valid_time', 'obs_temp', 'obs_wspd', 'obs_wdir']].reset_index(drop=True)

    return obs_dict


# ── Forecast grid utilities ───────────────────────────────────────────────────

def build_forecast_grid_gdf(sample_nc_path):
    """Build a GeoDataFrame of forecast grid points from a sample NetCDF file.

    Works for any forecast grid: 1D regular lat/lon (GFS) or 2D projected
    lat/lon (HRRR). Each row is one grid cell with its (y, x) index
    and lat/lon as a Point geometry. Used for spatial join against station points.

    Returns GeoDataFrame with columns: y_idx, x_idx, grid_lat, grid_lon, geometry
    """
    ds = xr.open_dataset(sample_nc_path)
    lat = ds.latitude.values
    lon = ds.longitude.values
    ds.close()

    # Handle both 1D regular grids (GFS) and 2D projected grids (HRRR)
    if lat.ndim == 1:
        lon, lat = np.meshgrid(lon, lat)  # (n_lat, n_lon)

    ny, nx = lat.shape
    y_indices, x_indices = np.meshgrid(np.arange(ny), np.arange(nx), indexing='ij')

    grid_df = pd.DataFrame({
        'y_idx': y_indices.ravel(),
        'x_idx': x_indices.ravel(),
        'grid_lat': lat.ravel(),
        'grid_lon': lon.ravel(),
    })
    geometry = gpd.points_from_xy(grid_df['grid_lon'], grid_df['grid_lat'])
    grid_gdf = gpd.GeoDataFrame(grid_df, geometry=geometry, crs='EPSG:4326')
    return grid_gdf


def spatial_join_stations_to_grid(stations_gdf, grid_gdf):
    """Spatially join each station to its nearest forecast grid point.

    Works for any forecast grid (HRRR, GFS, ERA5, etc.).
    Projects to EPSG:3857 (meters) for accurate nearest-neighbor distance,
    then returns results in the original CRS.

    Returns a DataFrame mapping station_id -> (y_idx, x_idx) of the nearest grid cell.
    """
    # Project to metric CRS for accurate distance calculation
    proj_crs = 'EPSG:3857'
    stations_proj = stations_gdf[['station_id', 'geometry']].to_crs(proj_crs)
    grid_proj = grid_gdf[['y_idx', 'x_idx', 'geometry']].to_crs(proj_crs)

    joined = gpd.sjoin_nearest(
        stations_proj,
        grid_proj,
        how='left',
        distance_col='dist_m',
    )
    return joined[['station_id', 'y_idx', 'x_idx', 'dist_m']].reset_index(drop=True)


# ── Bin-averaged regridding (forecast → ERA5 grid) ──────────────────────────

def _build_hrrr_to_era5_bin_map(hrrr_lat2d, hrrr_lon2d, era5_lats, era5_lons,
                                  cache_path=None, force_rebuild=False):
    """Pre-compute which ERA5 bin each HRRR cell center falls into.

    ERA5 has a regular 0.1-degree grid. We construct bin edges at cell
    boundaries (midpoints between adjacent centers) and use np.digitize to
    assign each HRRR cell to its containing ERA5 bin. This assignment is
    computed once and reused for every forecast field.

    Handles ERA5 latitude in either ascending or descending order.

    The result can be cached to a .npz file and reloaded on subsequent calls,
    since the HRRR and ERA5 grids are fixed for a given model configuration.
    The cache stores the forecast grid shape alongside the index arrays; if
    the cached shape doesn't match the current grid, the map is recomputed.

    Args:
        hrrr_lat2d: 2D array of HRRR latitudes, shape (ny, nx).
        hrrr_lon2d: 2D array of HRRR longitudes, shape (ny, nx).
        era5_lats: 1D array of ERA5 latitude centers, shape (n_lat,).
                   May be ascending or descending.
        era5_lons: 1D array of ERA5 longitude centers (ascending), shape (n_lon,).
        cache_path: Optional path to a .npz file for caching the bin map.
                    If the file exists and force_rebuild is False, the cached
                    map is loaded instead of recomputed. Pass None to skip
                    caching entirely.
        force_rebuild: If True, recompute and overwrite any existing cache file.
                       Defaults to False.

    Returns:
        Tuple (bin_lat_idx, bin_lon_idx, valid_mask):
            bin_lat_idx: 1D array, ERA5 lat index for each valid HRRR cell
                         (indices into the original era5_lats ordering).
            bin_lon_idx: 1D array, ERA5 lon index for each valid HRRR cell.
            valid_mask: 1D boolean array (len = ny*nx), True for HRRR cells
                        that fall within the ERA5 domain.
    """
    # ── Try loading from cache ────────────────────────────────────────────
    if cache_path is not None and os.path.exists(cache_path) and not force_rebuild:
        cached = np.load(cache_path)
        cached_shape = tuple(cached['hrrr_shape'])
        if cached_shape == hrrr_lat2d.shape:
            print(f"  Loaded bin map from cache: {cache_path}")
            return cached['bin_lat_idx'], cached['bin_lon_idx'], cached['valid_mask']
        else:
            print(f"  Cache grid shape {cached_shape} ≠ current {hrrr_lat2d.shape}; "
                  f"rebuilding...")

    # Work in ascending latitude for np.digitize
    lat_ascending = era5_lats[0] < era5_lats[-1] if len(era5_lats) > 1 else True
    lats_asc = era5_lats if lat_ascending else era5_lats[::-1]

    # Build bin edges as midpoints between adjacent centers, plus outer edges
    # For a regular 0.1° grid, half-spacing is 0.05°
    half_dlat = np.diff(lats_asc).mean() / 2.0
    half_dlon = np.diff(era5_lons).mean() / 2.0

    lat_edges = np.concatenate([
        [lats_asc[0] - half_dlat],
        (lats_asc[:-1] + lats_asc[1:]) / 2,
        [lats_asc[-1] + half_dlat],
    ])
    lon_edges = np.concatenate([
        [era5_lons[0] - half_dlon],
        (era5_lons[:-1] + era5_lons[1:]) / 2,
        [era5_lons[-1] + half_dlon],
    ])

    flat_lat = hrrr_lat2d.ravel()
    flat_lon = hrrr_lon2d.ravel()

    # np.digitize with ascending edges returns 1-based indices; subtract 1
    lat_bin = np.digitize(flat_lat, lat_edges) - 1
    lon_bin = np.digitize(flat_lon, lon_edges) - 1

    n_lat_asc = len(lats_asc)
    n_lon = len(era5_lons)

    # Valid = within ERA5 domain (bin index in [0, n-1])
    valid = ((lat_bin >= 0) & (lat_bin < n_lat_asc) &
             (lon_bin >= 0) & (lon_bin < n_lon))

    # If ERA5 lats were descending, flip the ascending bin indices back
    if not lat_ascending:
        lat_bin_out = (n_lat_asc - 1) - lat_bin[valid]
    else:
        lat_bin_out = lat_bin[valid]

    # ── Save to cache ─────────────────────────────────────────────────────
    if cache_path is not None:
        os.makedirs(os.path.dirname(os.path.abspath(cache_path)), exist_ok=True)
        np.savez(
            cache_path,
            bin_lat_idx=lat_bin_out,
            bin_lon_idx=lon_bin[valid],
            valid_mask=valid,
            hrrr_shape=np.array(hrrr_lat2d.shape, dtype=np.int32),
        )
        print(f"  Saved bin map to cache: {cache_path}")

    return lat_bin_out, lon_bin[valid], valid


def _regrid_field_to_era5(field_2d, bin_lat_idx, bin_lon_idx, valid_mask, n_lat, n_lon):
    """Regrid a single HRRR 2D field to the ERA5 grid by bin-averaging.

    For each ERA5 cell, computes the mean of all HRRR cell values whose
    centers fall within that ERA5 cell.

    Args:
        field_2d: 2D numpy array, shape (ny, nx), the HRRR field to regrid.
        bin_lat_idx: 1D array of ERA5 lat indices for valid HRRR cells.
        bin_lon_idx: 1D array of ERA5 lon indices for valid HRRR cells.
        valid_mask: 1D boolean mask (len = ny*nx) for HRRR cells within ERA5 domain.
        n_lat: Number of ERA5 latitude cells.
        n_lon: Number of ERA5 longitude cells.

    Returns:
        2D numpy array, shape (n_lat, n_lon), the regridded field.
        Cells with no contributing HRRR points are NaN.
    """
    flat_vals = field_2d.ravel()[valid_mask].astype(np.float64)

    sum_arr = np.zeros((n_lat, n_lon), dtype=np.float64)
    count_arr = np.zeros((n_lat, n_lon), dtype=np.int32)

    np.add.at(sum_arr, (bin_lat_idx, bin_lon_idx), flat_vals)
    np.add.at(count_arr, (bin_lat_idx, bin_lon_idx), 1)

    with np.errstate(invalid='ignore'):
        result = np.where(count_arr > 0, sum_arr / count_arr, np.nan)

    return result.astype(np.float32)


def _regrid_wind_to_era5(wspd_2d, wdir_2d, bin_lat_idx, bin_lon_idx,
                          valid_mask, n_lat, n_lon):
    """Regrid HRRR wind speed and direction to ERA5 grid via u/v decomposition.

    Wind direction is a circular variable and cannot be directly averaged.
    Instead:
      1. Decompose (speed, direction) → (u, v) wind components
      2. Regrid u and v independently by bin-averaging
      3. Recompute speed and direction from regridded u and v

    Uses the meteorological convention shared by HRRR and ERA5:
      u = -speed * sin(wdir_rad)   (eastward component)
      v = -speed * cos(wdir_rad)   (northward component)

    Args:
        wspd_2d: 2D HRRR wind speed array (m/s), shape (ny, nx).
        wdir_2d: 2D HRRR wind direction array (degrees, 0-360), shape (ny, nx).
        bin_lat_idx, bin_lon_idx, valid_mask: Pre-computed bin assignments.
        n_lat, n_lon: ERA5 grid dimensions.

    Returns:
        Tuple (regridded_wspd, regridded_wdir), both shape (n_lat, n_lon).
    """
    wdir_rad = np.radians(wdir_2d)
    u_wind = -wspd_2d * np.sin(wdir_rad)
    v_wind = -wspd_2d * np.cos(wdir_rad)

    u_regridded = _regrid_field_to_era5(u_wind, bin_lat_idx, bin_lon_idx,
                                         valid_mask, n_lat, n_lon)
    v_regridded = _regrid_field_to_era5(v_wind, bin_lat_idx, bin_lon_idx,
                                         valid_mask, n_lat, n_lon)

    regridded_wspd = np.sqrt(u_regridded**2 + v_regridded**2).astype(np.float32)
    regridded_wdir = (np.degrees(np.arctan2(-u_regridded, -v_regridded)) % 360).astype(np.float32)

    return regridded_wspd, regridded_wdir


# ── Bilinear interpolation regridding (GFS regular grid → ERA5) ──────────────

def _regrid_regular_field_to_era5(field_2d, fc_lats_1d, fc_lons_1d,
                                   era5_lats, era5_lons):
    """Bilinear interpolation from a regular forecast grid to the ERA5 grid.

    Appropriate when the forecast grid is coarser than ERA5 (e.g. GFS 0.25°
    → ERA5 0.1°). Bin-center averaging would leave most ERA5 cells empty in
    this regime; bilinear interpolation smoothly fills the finer target grid.

    Uses xarray's built-in .interp() which delegates to scipy's
    RegularGridInterpolator.

    Args:
        field_2d: 2D numpy array, shape (n_fc_lat, n_fc_lon).
        fc_lats_1d: 1D array of forecast latitudes (may be ascending or descending).
        fc_lons_1d: 1D array of forecast longitudes (ascending).
        era5_lats: 1D array of ERA5 latitude centers.
        era5_lons: 1D array of ERA5 longitude centers.

    Returns:
        2D numpy float32 array, shape (n_era5_lat, n_era5_lon).
        Cells outside the forecast domain are NaN.
    """
    da = xr.DataArray(
        field_2d,
        dims=['latitude', 'longitude'],
        coords={'latitude': fc_lats_1d, 'longitude': fc_lons_1d},
    )
    interpolated = da.interp(
        latitude=era5_lats,
        longitude=era5_lons,
        method='linear',
    )
    return interpolated.values.astype(np.float32)


def _regrid_regular_wind_to_era5(wspd_2d, wdir_2d, fc_lats_1d, fc_lons_1d,
                                  era5_lats, era5_lons):
    """Bilinear interpolation for wind from a regular forecast grid to ERA5.

    Wind direction is circular and cannot be interpolated directly. Decomposes
    to u/v components, interpolates each independently, then recomputes speed
    and direction. Uses the same meteorological convention as the bin-averaging
    counterpart (_regrid_wind_to_era5).

    Args:
        wspd_2d: 2D forecast wind speed array (m/s), shape (n_fc_lat, n_fc_lon).
        wdir_2d: 2D forecast wind direction array (degrees, 0-360), same shape.
        fc_lats_1d: 1D array of forecast latitudes.
        fc_lons_1d: 1D array of forecast longitudes.
        era5_lats: 1D array of ERA5 latitude centers.
        era5_lons: 1D array of ERA5 longitude centers.

    Returns:
        Tuple (regridded_wspd, regridded_wdir), both shape (n_era5_lat, n_era5_lon).
    """
    wdir_rad = np.radians(wdir_2d)
    u_wind = -wspd_2d * np.sin(wdir_rad)
    v_wind = -wspd_2d * np.cos(wdir_rad)

    u_regridded = _regrid_regular_field_to_era5(
        u_wind, fc_lats_1d, fc_lons_1d, era5_lats, era5_lons
    )
    v_regridded = _regrid_regular_field_to_era5(
        v_wind, fc_lats_1d, fc_lons_1d, era5_lats, era5_lons
    )

    regridded_wspd = np.sqrt(u_regridded**2 + v_regridded**2).astype(np.float32)
    regridded_wdir = (np.degrees(np.arctan2(-u_regridded, -v_regridded)) % 360).astype(np.float32)

    return regridded_wspd, regridded_wdir


# ── Forecast data loading ─────────────────────────────────────────────────────

def load_hrrr_forecasts(hrrr_base_dir, variable_name, year, month):
    """Load HRRR forecasts from the combined per-(day, cycle) NetCDF format.

    Reads files matching hrrr_{HH}z_{YYYYMMDD}.nc in
    {hrrr_base_dir}/{year}/{month:02d}/. Each file has dims (lead_hour, y, x)
    and stores t2m, si10, wdir10 for all lead times in one file.

    This is the new space-efficient format produced by pull_hrrr.py.
    The old per-element format (temp/wspd/wdir subdirs) is not supported here;
    use load_forecasts() for GFS which still uses that layout.

    Returns a list of dicts with keys:
        issuance_time [UTC], valid_time [US/Central tz-naive], lead_hours,
        data (2D array, shape (ny, nx))
    One entry per (file, lead_hour) combination.
    """
    nc_dir = os.path.join(hrrr_base_dir, str(year), f"{month:02d}")
    nc_files = sorted(Path(nc_dir).glob('hrrr_*.nc'))

    records = []
    n_skipped = 0
    for fpath in nc_files:
        try:
            ds = xr.open_dataset(str(fpath))
        except Exception as e:
            print(f"  WARNING: skipping {fpath.name} — {e}")
            n_skipped += 1
            continue

        try:
            issuance_time = pd.Timestamp(ds.time.values)

            for i, lead_h in enumerate(ds.lead_hour.values):
                data = ds[variable_name].isel(lead_hour=i).values
                vt = ds.valid_time.isel(lead_hour=i).values
                vt_central = _to_central(pd.Series([pd.Timestamp(vt)])).iloc[0]
                records.append({
                    'issuance_time': issuance_time,
                    'valid_time': vt_central,
                    'lead_hours': int(lead_h),
                    'data': data,
                })
        except Exception as e:
            print(f"  WARNING: error reading contents of {fpath.name} — {e}")
            n_skipped += 1
        finally:
            ds.close()

    if n_skipped:
        print(f"  WARNING: skipped {n_skipped}/{len(nc_files)} files due to errors")

    return records


def load_forecasts(element_dir, variable_name, year, month):
    """Load all forecast NetCDF files for one element and extract metadata.

    Works for both multi-step files (HRRR) and single-step files (GFS).
    Handles step as either a dimension or a scalar coordinate.

    valid_time values are converted to US/Central (tz-naive) so they align with
    observation timestamps from load_all_observations() and load_era5_as_obs_dict().

    Returns a list of dicts with keys:
        issuance_time [UTC], valid_time [US/Central tz-naive], lead_hours, data (2D array)
    One entry per (file, step) combination.
    """
    nc_dir = os.path.join(element_dir, str(year), f"{month:02d}")
    nc_files = sorted(Path(nc_dir).glob('*.nc'))

    records = []
    n_skipped = 0
    for fpath in nc_files:
        try:
            ds = xr.open_dataset(str(fpath))
        except Exception as e:
            print(f"  WARNING: skipping {fpath.name} — {e}")
            n_skipped += 1
            continue

        try:
            issuance_time = pd.Timestamp(ds.time.values)

            steps = ds.step.values if 'step' in ds.dims else [ds.step.values]
            for step in steps:
                if 'step' in ds.dims:
                    data = ds[variable_name].sel(step=step).values
                    vt = ds.valid_time.sel(step=step).values
                else:
                    data = ds[variable_name].values
                    vt = ds.valid_time.values

                lead_hours = int(pd.Timedelta(step).total_seconds() / 3600)
                # Convert valid_time (UTC) to US/Central so it matches observations
                vt_central = _to_central(pd.Series([pd.Timestamp(vt)])).iloc[0]
                records.append({
                    'issuance_time': issuance_time,
                    'valid_time': vt_central,
                    'lead_hours': lead_hours,
                    'data': data,
                })
        except Exception as e:
            print(f"  WARNING: error reading contents of {fpath.name} — {e}")
            n_skipped += 1
        finally:
            ds.close()

    if n_skipped:
        print(f"  WARNING: skipped {n_skipped}/{len(nc_files)} files due to errors")

    return records


# ── Station-level error computation ───────────────────────────────────────────

def _compute_and_save_errors(
    temp_forecasts, wspd_forecasts, wdir_forecasts,
    station_grid_map, obs_dict, stations_gdf,
    out_dir, model_name,
):
    """Compute forecast errors at station locations and save CSVs.

    This is the shared core logic used by calculate_station_errors_for_month().
    It receives already-loaded forecast
    records and the station-to-grid mapping, then:
      1. Indexes forecasts by (valid_time [Central], lead_hours)
      2. For each station, matches forecast grid values to observations
      3. Computes error = forecast - observed
      4. Saves per-station CSVs and a summary CSV

    All valid_time values in the output are US/Central (tz-naive).

    Args:
        temp_forecasts: List of forecast record dicts from load_forecasts() for temperature.
        wspd_forecasts: Same for wind speed.
        wdir_forecasts: Same for wind direction.
        station_grid_map: DataFrame with station_id, y_idx, x_idx from spatial join.
        obs_dict: Dict mapping station_id -> observation DataFrame (Central times).
        stations_gdf: GeoDataFrame of station metadata.
        out_dir: Output directory for CSV files.
        model_name: String label for log messages (e.g. 'HRRR', 'GFS').

    Returns:
        Summary DataFrame with per-station, per-lead-time error statistics.
    """
    # Index forecasts by (valid_time [Central], lead_hours) for fast lookup
    temp_index = {(r['valid_time'], r['lead_hours']): r['data'] for r in temp_forecasts}
    wspd_index = {(r['valid_time'], r['lead_hours']): r['data'] for r in wspd_forecasts}
    wdir_index = {(r['valid_time'], r['lead_hours']): r['data'] for r in wdir_forecasts}

    # Get sorted list of unique (valid_time, lead_hours) keys present in all three
    all_keys = sorted(set(temp_index.keys()) & set(wspd_index.keys()) & set(wdir_index.keys()))
    print(f"  {len(all_keys)} forecast (valid_time [Central], lead_hours) entries matched across all elements")

    # Build a lookup from station_id -> (y_idx, x_idx)
    grid_lookup = {
        row['station_id']: (int(row['y_idx']), int(row['x_idx']))
        for _, row in station_grid_map.iterrows()
    }

    station_summaries = []
    n_processed = 0

    for station_id, (y_idx, x_idx) in grid_lookup.items():
        obs = obs_dict.get(station_id)
        if obs is None or len(obs) == 0:
            continue

        station_row = stations_gdf[stations_gdf['station_id'] == station_id].iloc[0]

        # Build error records
        records = []
        obs_times = set(obs['valid_time'].values)

        for valid_time, lead_hours in all_keys:
            vt_ts = pd.Timestamp(valid_time)
            if vt_ts not in obs_times:
                continue

            obs_row = obs[obs['valid_time'] == vt_ts].iloc[0]

            # Extract forecast values at station's nearest grid point
            # Forecast temp is in Kelvin, convert to Celsius
            fc_temp_k = temp_index[(valid_time, lead_hours)][y_idx, x_idx]
            fc_temp = float(fc_temp_k) - 273.15 if not np.isnan(fc_temp_k) else np.nan

            fc_wspd = float(wspd_index[(valid_time, lead_hours)][y_idx, x_idx])
            fc_wdir = float(wdir_index[(valid_time, lead_hours)][y_idx, x_idx])

            obs_temp = obs_row['obs_temp']
            obs_wspd = obs_row['obs_wspd']
            obs_wdir = obs_row['obs_wdir']

            records.append({
                'station_id': station_id,
                'valid_time': vt_ts,   # US/Central tz-naive
                'lead_hours': lead_hours,
                'forecast_temp': round(fc_temp, 2) if not np.isnan(fc_temp) else np.nan,
                'observed_temp': obs_temp,
                'temp_error': round(fc_temp - obs_temp, 2) if (not np.isnan(fc_temp) and obs_temp is not None) else np.nan,
                'temp_pct_error': round((fc_temp - obs_temp) / obs_temp * 100, 1) if (not np.isnan(fc_temp) and obs_temp not in (None, 0)) else np.nan,
                'forecast_wspd': round(fc_wspd, 2) if not np.isnan(fc_wspd) else np.nan,
                'observed_wspd': obs_wspd,
                'wspd_error': round(fc_wspd - obs_wspd, 2) if (not np.isnan(fc_wspd) and obs_wspd is not None) else np.nan,
                'wspd_pct_error': round((fc_wspd - obs_wspd) / obs_wspd * 100, 1) if (not np.isnan(fc_wspd) and obs_wspd not in (None, 0)) else np.nan,
                'forecast_wdir': round(fc_wdir, 1) if not np.isnan(fc_wdir) else np.nan,
                'observed_wdir': obs_wdir,
                'wdir_degree_error': round(circular_angular_error(fc_wdir, obs_wdir), 1),
                'lat': station_row['lat'],
                'lon': station_row['lon'],
            })

        if not records:
            continue

        err_df = pd.DataFrame(records)
        err_df.to_csv(os.path.join(out_dir, f"{station_id}.csv"), index=False)

        # Compute summary stats
        for lead in err_df['lead_hours'].unique():
            subset = err_df[err_df['lead_hours'] == lead]
            station_summaries.append({
                'station_id': station_id,
                'lat': station_row['lat'],
                'lon': station_row['lon'],
                'lead_hours': lead,
                'n_obs': len(subset),
                'temp_mae': subset['temp_error'].abs().mean(),
                'temp_bias': subset['temp_error'].mean(),
                'wspd_mae': subset['wspd_error'].abs().mean(),
                'wspd_bias': subset['wspd_error'].mean(),
                'wdir_mae': subset['wdir_degree_error'].abs().mean(),
            })

        n_processed += 1
        if n_processed % 50 == 0:
            print(f"  Processed {n_processed} stations")

    print(f"  Processed {n_processed} stations total")

    summary_df = pd.DataFrame(station_summaries)
    summary_path = os.path.join(out_dir, 'error_summary.csv')
    summary_df.to_csv(summary_path, index=False)
    print(f"\nSaved {len(summary_df)} {model_name} summary rows to {summary_path}")

    # Print aggregate stats
    for lead in sorted(summary_df['lead_hours'].unique()):
        s = summary_df[summary_df['lead_hours'] == lead]
        print(f"\n  {model_name} Lead {lead}h — {len(s)} stations:")
        print(f"    Temp MAE:  {s['temp_mae'].mean():.2f} °C  (bias: {s['temp_bias'].mean():+.2f})")
        print(f"    Wspd MAE:  {s['wspd_mae'].mean():.2f} m/s (bias: {s['wspd_bias'].mean():+.2f})")
        print(f"    Wdir MAE:  {s['wdir_mae'].mean():.2f}°")

    return summary_df


# ── Station-level entry points ─────────────────────────────────────────────────

def calculate_station_errors_for_month(year, month, model='hrrr', lead_hours=None):
    """Calculate forecast errors at all weather stations for a given month.

    Unified station-level error function for all forecast models (HRRR, GFS).

    1. Load station locations as a GeoDataFrame
    2. Build a GeoDataFrame of the forecast grid and spatially join stations
       to their nearest grid cell
    3. Load all forecasts and ISD observations
    4. Filter by lead time (and optionally collapse leads for day-ahead models)
    5. Compute errors and save per-station CSVs and summary

    All valid_time values in output are US/Central (tz-naive).
    Output: {processed}/forecast_errors/{model}/{year}/{month:02d}/

    Args:
        year: Four-digit year.
        month: Month 1–12.
        model: 'hrrr' or 'gfs'.
        lead_hours: List of lead hours to keep. None uses the model's
                    default_lead_hours from _MODEL_CONFIG. Pass an explicit
                    list to override (e.g. [1, 18] to get both HRRR leads).

    Returns:
        Summary DataFrame with per-station error statistics.
    """
    model_lower = model.lower()
    if model_lower not in _MODEL_CONFIG:
        raise ValueError(
            f"model must be one of {list(_MODEL_CONFIG)}, got '{model}'"
        )
    cfg = _MODEL_CONFIG[model_lower]

    dirs = setup_directories()
    raw_dir = dirs['raw']
    processed_dir = dirs['processed']
    fc_base = os.path.join(raw_dir, cfg['data_dir'])

    # Output directory
    out_dir = os.path.join(
        processed_dir, 'forecast_errors', model_lower,
        str(year), f"{month:02d}"
    )
    os.makedirs(out_dir, exist_ok=True)

    # Load station metadata as GeoDataFrame
    stations_gdf = load_stations_gdf(raw_dir)
    print(f"Loaded {len(stations_gdf)} stations as GeoDataFrame")

    # Build forecast grid GeoDataFrame and spatially join stations
    if model_lower == 'hrrr':
        fc_month_dir = os.path.join(fc_base, str(year), f"{month:02d}")
        sample_nc = sorted(Path(fc_month_dir).glob('hrrr_*.nc'))[0]
    else:
        temp_dir = os.path.join(fc_base, 'temp', str(year), f"{month:02d}")
        sample_nc = sorted(Path(temp_dir).glob('*.nc'))[0]
    print(f"Building {cfg['display_name']} grid GeoDataFrame and joining stations...")
    grid_gdf = build_forecast_grid_gdf(str(sample_nc))
    station_grid_map = spatial_join_stations_to_grid(stations_gdf, grid_gdf)
    print(f"  Joined {len(station_grid_map)} stations to grid (mean dist: "
          f"{station_grid_map['dist_m'].mean():.0f} m)")

    # Load all observations (returns Central-time valid_time)
    print("Loading station observations...")
    obs_dict = load_all_observations(stations_gdf, year, month, raw_dir)
    print(f"  Loaded observations for {len(obs_dict)} stations")

    # Load forecasts (valid_time converted to Central inside loader)
    print(f"Loading {cfg['display_name']} forecasts for {year}-{month:02d}...")
    if model_lower == 'hrrr':
        temp_forecasts = load_hrrr_forecasts(fc_base, 't2m', year, month)
        wspd_forecasts = load_hrrr_forecasts(fc_base, 'si10', year, month)
        wdir_forecasts = load_hrrr_forecasts(fc_base, 'wdir10', year, month)
    else:
        temp_forecasts = load_forecasts(
            os.path.join(fc_base, 'temp'), 't2m', year, month)
        wspd_forecasts = load_forecasts(
            os.path.join(fc_base, 'wspd'), 'si10', year, month)
        wdir_forecasts = load_forecasts(
            os.path.join(fc_base, 'wdir'), 'wdir10', year, month)
    print(f"  Loaded {len(temp_forecasts)} temp, {len(wspd_forecasts)} wspd, "
          f"{len(wdir_forecasts)} wdir forecast fields")

    # Apply lead-time filtering and optional collapse
    effective_leads = lead_hours if lead_hours is not None else cfg.get('default_lead_hours')
    collapse = cfg.get('collapse_leads', False)

    temp_forecasts = _filter_forecasts(temp_forecasts, effective_leads, collapse)
    wspd_forecasts = _filter_forecasts(wspd_forecasts, effective_leads, collapse)
    wdir_forecasts = _filter_forecasts(wdir_forecasts, effective_leads, collapse)

    if effective_leads is not None or collapse:
        print(f"  After filtering: {len(temp_forecasts)} temp, {len(wspd_forecasts)} wspd, "
              f"{len(wdir_forecasts)} wdir fields (leads={effective_leads}, collapse={collapse})")

    return _compute_and_save_errors(
        temp_forecasts, wspd_forecasts, wdir_forecasts,
        station_grid_map, obs_dict, stations_gdf,
        out_dir, cfg['display_name'],
    )


def calculate_hrrr_errors_for_month(year, month):
    """Calculate HRRR forecast errors at weather stations.

    Backward-compatible wrapper — delegates to calculate_station_errors_for_month().
    """
    return calculate_station_errors_for_month(year, month, model='hrrr')


def _compute_era5_gridded_errors(
    temp_forecasts, wspd_forecasts, wdir_forecasts,
    era5_ds, out_dir, year, month, model_name,
    fc_lats, fc_lons,
    regrid_method='bin',
    bin_map_cache_path=None, force_rebuild_bin_map=False,
    wspd100_forecasts=None, wdir100_forecasts=None,
    era5_wind100m_ds=None,
):
    """Compute forecast errors vs ERA5 using regridding and xarray merge.

    Supports two regridding methods:
      - 'bin' (default): Bin-center averaging — appropriate when the forecast grid
        is finer than ERA5 (HRRR 3km → ERA5 0.1°). Each ERA5 cell gets
        the mean of all forecast cell centers that fall within it.
      - 'interp': Bilinear interpolation — appropriate when the forecast grid is
        coarser than ERA5 (GFS 0.25° → ERA5 0.1°). xarray .interp(method='linear')
        smoothly fills the finer target grid.

    After regridding, forecast and ERA5 datasets are merged using xr.merge() on
    shared coordinates, and errors are computed via xarray subtraction.

    100m wind errors are computed when wspd100_forecasts, wdir100_forecasts, and
    era5_wind100m_ds are all provided. ERA5 100m wind (0.25° single-levels) is
    interpolated to the ERA5-Land 0.1° grid via U/V decomposition + bilinear
    interpolation. If any 100m source is missing, 100m errors are silently skipped.

    Args:
        temp_forecasts: List of forecast record dicts from load_forecasts() for temperature.
        wspd_forecasts: Same for 10m wind speed.
        wdir_forecasts: Same for 10m wind direction.
        era5_ds: xarray Dataset opened from ERA5-Land NetCDF (t2m in K, wspd, wdir).
        out_dir: Output directory.
        year, month: For output filename.
        model_name: Display name for log messages (e.g. 'HRRR', 'GFS').
        fc_lats: Forecast latitudes — 2D array (ny, nx) for bin method, 1D for interp.
        fc_lons: Forecast longitudes — 2D array (ny, nx) for bin method, 1D for interp.
        regrid_method: 'bin' for bin-center averaging (HRRR) or 'interp' for
                       bilinear interpolation (GFS). Defaults to 'bin'.
        bin_map_cache_path: Passed through to _build_hrrr_to_era5_bin_map (only used
                            when regrid_method='bin'). None disables caching.
        force_rebuild_bin_map: Passed through to _build_hrrr_to_era5_bin_map (only used
                               when regrid_method='bin'). Defaults to False.
        wspd100_forecasts: Optional list of forecast record dicts for 100m wind speed
                           (HRRR: si100 variable; GFS: si100 from wspd100/ dir).
        wdir100_forecasts: Optional list of forecast record dicts for 100m wind direction.
        era5_wind100m_ds: Optional ERA5 100m wind xarray Dataset from era5_wind100m_*.nc
                          (reanalysis-era5-single-levels, 0.25°, with u100/v100 vars).

    Returns:
        Summary DataFrame with per-cell, per-lead-time error statistics. Includes
        wspd100_mae, wspd100_bias, wdir100_mae columns when 100m data is available.
    """
    # ── Step 1: Index forecasts by (valid_time, lead_hours) ──────────────
    temp_index = {(r['valid_time'], r['lead_hours']): r['data'] for r in temp_forecasts}
    wspd_index = {(r['valid_time'], r['lead_hours']): r['data'] for r in wspd_forecasts}
    wdir_index = {(r['valid_time'], r['lead_hours']): r['data'] for r in wdir_forecasts}

    all_keys = sorted(set(temp_index.keys()) & set(wspd_index.keys()) & set(wdir_index.keys()))
    valid_times = sorted({k[0] for k in all_keys})
    lead_hours_list = sorted({k[1] for k in all_keys})

    print(f"  {len(all_keys)} forecast entries: {len(valid_times)} times × {len(lead_hours_list)} leads")
    print(f"  Lead hours: {lead_hours_list}")

    # ── Step 2: Set up regridding ────────────────────────────────────────
    era5_lats = era5_ds.latitude.values   # 1D
    era5_lons = era5_ds.longitude.values  # 1D
    n_lat = len(era5_lats)
    n_lon = len(era5_lons)

    use_interp = (regrid_method == 'interp')

    if use_interp:
        # Bilinear interpolation: GFS coarse regular grid → ERA5
        print(f"  Using bilinear interpolation for {model_name} → ERA5 regridding")
        print(f"  Forecast grid: {len(fc_lats)} lat × {len(fc_lons)} lon "
              f"→ ERA5: {n_lat} × {n_lon}")
    else:
        # Bin-center averaging: HRRR high-res 2D grid → ERA5
        print(f"  Building {model_name} → ERA5 bin map for regridding...")
        bin_lat_idx, bin_lon_idx, valid_mask = _build_hrrr_to_era5_bin_map(
            fc_lats, fc_lons, era5_lats, era5_lons,
            cache_path=bin_map_cache_path,
            force_rebuild=force_rebuild_bin_map,
        )
        n_valid = valid_mask.sum()
        print(f"  {n_valid}/{fc_lats.size} forecast cells fall within ERA5 domain")

    # ── Step 3: Regrid all forecast fields ───────────────────────────────
    n_times = len(valid_times)
    n_leads = len(lead_hours_list)
    time_to_idx = {t: i for i, t in enumerate(valid_times)}
    lead_to_idx = {lh: i for i, lh in enumerate(lead_hours_list)}

    shape = (n_times, n_leads, n_lat, n_lon)
    fc_temp_arr = np.full(shape, np.nan, dtype=np.float32)
    fc_wspd_arr = np.full(shape, np.nan, dtype=np.float32)
    fc_wdir_arr = np.full(shape, np.nan, dtype=np.float32)

    n_regridded = 0
    for vt, lh in all_keys:
        ti = time_to_idx[vt]
        li = lead_to_idx[lh]

        if use_interp:
            # Bilinear interpolation (GFS)
            fc_temp_arr[ti, li] = _regrid_regular_field_to_era5(
                temp_index[(vt, lh)], fc_lats, fc_lons, era5_lats, era5_lons,
            )
            fc_wspd_arr[ti, li], fc_wdir_arr[ti, li] = _regrid_regular_wind_to_era5(
                wspd_index[(vt, lh)], wdir_index[(vt, lh)],
                fc_lats, fc_lons, era5_lats, era5_lons,
            )
        else:
            # Bin-center averaging (HRRR)
            fc_temp_arr[ti, li] = _regrid_field_to_era5(
                temp_index[(vt, lh)], bin_lat_idx, bin_lon_idx,
                valid_mask, n_lat, n_lon,
            )
            fc_wspd_arr[ti, li], fc_wdir_arr[ti, li] = _regrid_wind_to_era5(
                wspd_index[(vt, lh)], wdir_index[(vt, lh)],
                bin_lat_idx, bin_lon_idx, valid_mask, n_lat, n_lon,
            )

        n_regridded += 1
        if n_regridded % 200 == 0:
            print(f"    Regridded {n_regridded}/{len(all_keys)} forecast fields")

    print(f"    Regridded {n_regridded} forecast fields total")

    # ── Step 3.5: Regrid 100m wind forecast fields ────────────────────────────
    has_100m = (
        wspd100_forecasts is not None and len(wspd100_forecasts) > 0
        and wdir100_forecasts is not None and len(wdir100_forecasts) > 0
        and era5_wind100m_ds is not None
    )
    fc_wspd100_arr = fc_wdir100_arr = None

    if has_100m:
        wspd100_index = {(r['valid_time'], r['lead_hours']): r['data'] for r in wspd100_forecasts}
        wdir100_index = {(r['valid_time'], r['lead_hours']): r['data'] for r in wdir100_forecasts}
        keys_100m = sorted(set(wspd100_index.keys()) & set(wdir100_index.keys()))
        print(f"  Regridding {len(keys_100m)} 100m wind forecast fields...")

        fc_wspd100_arr = np.full(shape, np.nan, dtype=np.float32)
        fc_wdir100_arr = np.full(shape, np.nan, dtype=np.float32)

        for vt, lh in keys_100m:
            if vt not in time_to_idx or lh not in lead_to_idx:
                continue
            ti, li = time_to_idx[vt], lead_to_idx[lh]
            if use_interp:
                fc_wspd100_arr[ti, li], fc_wdir100_arr[ti, li] = _regrid_regular_wind_to_era5(
                    wspd100_index[(vt, lh)], wdir100_index[(vt, lh)],
                    fc_lats, fc_lons, era5_lats, era5_lons,
                )
            else:
                fc_wspd100_arr[ti, li], fc_wdir100_arr[ti, li] = _regrid_wind_to_era5(
                    wspd100_index[(vt, lh)], wdir100_index[(vt, lh)],
                    bin_lat_idx, bin_lon_idx, valid_mask, n_lat, n_lon,
                )
        print(f"    Regridded {len(keys_100m)} 100m wind fields")

    # Convert forecast temp from K to °C
    fc_temp_arr -= 273.15

    # ── Step 4: Build regridded forecast xarray Dataset ──────────────────
    vt_dt64 = pd.DatetimeIndex(valid_times).values  # numpy datetime64[ns], tz-naive

    fc_ds = xr.Dataset(
        {
            'forecast_temp': (['valid_time', 'lead_hours', 'latitude', 'longitude'], fc_temp_arr),
            'forecast_wspd': (['valid_time', 'lead_hours', 'latitude', 'longitude'], fc_wspd_arr),
            'forecast_wdir': (['valid_time', 'lead_hours', 'latitude', 'longitude'], fc_wdir_arr),
        },
        coords={
            'valid_time': ('valid_time', vt_dt64),
            'lead_hours': ('lead_hours', lead_hours_list),
            'latitude':   ('latitude', era5_lats),
            'longitude':  ('longitude', era5_lons),
        },
    )
    if has_100m:
        fc_ds['forecast_wspd100'] = (['valid_time', 'lead_hours', 'latitude', 'longitude'],
                                      fc_wspd100_arr)
        fc_ds['forecast_wdir100'] = (['valid_time', 'lead_hours', 'latitude', 'longitude'],
                                      fc_wdir100_arr)

    # ── Step 5: Build ERA5 observations xarray Dataset ───────────────────
    # Convert ERA5-Land times to US/Central; deduplicate DST fall-back fold hour.
    era5_central_dt64, keep_idx = _era5_central_times(era5_ds)

    # Land mask: cells where ERA5 t2m is all NaN across time are ocean
    era5_t2m_vals = era5_ds['t2m'].values[keep_idx]  # (n_era5_times, n_lat, n_lon)
    land_mask = ~np.all(np.isnan(era5_t2m_vals), axis=0)  # (n_lat, n_lon)
    print(f"  ERA5 grid: {n_lat} lat × {n_lon} lon, {land_mask.sum()} land cells")

    era5_obs = xr.Dataset(
        {
            'era5_temp': (['valid_time', 'latitude', 'longitude'],
                          (era5_t2m_vals - 273.15).astype(np.float32)),
            'era5_wspd': (['valid_time', 'latitude', 'longitude'],
                          era5_ds['wspd'].values[keep_idx].astype(np.float32)),
            'era5_wdir': (['valid_time', 'latitude', 'longitude'],
                          era5_ds['wdir'].values[keep_idx].astype(np.float32)),
        },
        coords={
            'valid_time': ('valid_time', era5_central_dt64),
            'latitude':   ('latitude', era5_lats),
            'longitude':  ('longitude', era5_lons),
        },
    )

    # ── Step 5.5: Build ERA5 100m wind observations ───────────────────────────
    # ERA5 100m wind (reanalysis-era5-single-levels, 0.25°) is interpolated to
    # the ERA5-Land 0.1° grid via U/V decomposition + bilinear interpolation.
    era5_100m_obs = None
    if has_100m:
        era5_100m_dt64, keep_100m = _era5_central_times(era5_wind100m_ds)

        # Interpolate U/V from ERA5 0.25° to ERA5-Land 0.1° (all times at once)
        u100_rg = era5_wind100m_ds['u100'].isel(valid_time=keep_100m).interp(
            latitude=era5_lats, longitude=era5_lons, method='linear',
        ).values.astype(np.float32)
        v100_rg = era5_wind100m_ds['v100'].isel(valid_time=keep_100m).interp(
            latitude=era5_lats, longitude=era5_lons, method='linear',
        ).values.astype(np.float32)

        era5_wspd100 = np.sqrt(u100_rg**2 + v100_rg**2)
        era5_wdir100 = (np.degrees(np.arctan2(-u100_rg, -v100_rg)) % 360).astype(np.float32)

        era5_100m_obs = xr.Dataset(
            {
                'era5_wspd100': (['valid_time', 'latitude', 'longitude'], era5_wspd100),
                'era5_wdir100': (['valid_time', 'latitude', 'longitude'], era5_wdir100),
            },
            coords={
                'valid_time': ('valid_time', era5_100m_dt64),
                'latitude':   ('latitude', era5_lats),
                'longitude':  ('longitude', era5_lons),
            },
        )
        print(f"  ERA5 100m wind interpolated to ERA5-Land grid: {len(era5_100m_dt64)} steps")

    # ── Step 6: Merge forecast and ERA5 on shared coordinates ────────────
    # ERA5 has dims (valid_time, lat, lon); forecast has (valid_time, lead_hours, lat, lon).
    # xr.merge with join='inner' keeps only shared valid_times and auto-broadcasts
    # ERA5 across the lead_hours dimension.
    print(f"  Merging forecast and ERA5 datasets...")
    datasets_to_merge = [fc_ds, era5_obs]
    if era5_100m_obs is not None:
        datasets_to_merge.append(era5_100m_obs)
    merged = xr.merge(datasets_to_merge, join='inner')
    print(f"  Merged dataset: {dict(merged.sizes)}")

    # ── Step 7: Compute errors via xarray operations ─────────────────────
    # xarray handles broadcasting automatically: ERA5 vars (3D without lead_hours)
    # are broadcast across the lead_hours dim when combined with 4D forecast vars.
    merged['temp_error'] = merged['forecast_temp'] - merged['era5_temp']
    merged['wspd_error'] = merged['forecast_wspd'] - merged['era5_wspd']

    # Wind direction error is circular — compute via xarray (not raw numpy)
    # so that 3D era5_wdir auto-broadcasts across the lead_hours dimension.
    wdir_diff = abs(merged['forecast_wdir'] - merged['era5_wdir'])
    merged['wdir_error'] = xr.where(wdir_diff > 180, 360 - wdir_diff, wdir_diff)

    # Expand ERA5 obs to 4D for backward-compatible output format
    # (downstream consumers expect all variables to have the lead_hours dim)
    for era5_var in ['era5_temp', 'era5_wspd', 'era5_wdir']:
        if 'lead_hours' not in merged[era5_var].dims:
            merged[era5_var] = merged[era5_var].broadcast_like(merged['forecast_temp'])

    # 100m wind errors (when 100m data was available and survived the inner merge)
    if 'forecast_wspd100' in merged and 'era5_wspd100' in merged:
        merged['wspd100_error'] = merged['forecast_wspd100'] - merged['era5_wspd100']
        wdir100_diff = abs(merged['forecast_wdir100'] - merged['era5_wdir100'])
        merged['wdir100_error'] = xr.where(wdir100_diff > 180, 360 - wdir100_diff, wdir100_diff)
        for era5_var in ['era5_wspd100', 'era5_wdir100']:
            if 'lead_hours' not in merged[era5_var].dims:
                merged[era5_var] = merged[era5_var].broadcast_like(merged['forecast_wspd100'])

    # Apply land mask: set ocean cells to NaN
    # Use xr.where (not direct numpy assignment) because broadcast_like
    # may produce read-only views.
    land_da = xr.DataArray(land_mask, dims=['latitude', 'longitude'])
    for var in list(merged.data_vars):
        merged[var] = merged[var].where(land_da)

    # ── Step 8: Save output NetCDF ───────────────────────────────────────
    out_vars = ['temp_error', 'wspd_error', 'wdir_error',
                'forecast_temp', 'era5_temp',
                'forecast_wspd', 'era5_wspd',
                'forecast_wdir', 'era5_wdir']
    if 'wspd100_error' in merged:
        out_vars += ['wspd100_error', 'wdir100_error',
                     'forecast_wspd100', 'era5_wspd100',
                     'forecast_wdir100', 'era5_wdir100']
    ds_out = merged[out_vars]

    # Metadata
    ds_out['valid_time'].attrs.update({
        'long_name': 'Valid time (wall-clock)',
        'timezone': 'US/Central (tz-naive, DST applied)',
        'note': 'Timestamps are US/Central wall-clock time stored as tz-naive datetime64. '
                'Use pd.to_datetime(ds.valid_time.values) to recover timestamps.',
    })
    for v in ['temp_error', 'forecast_temp', 'era5_temp']:
        ds_out[v].attrs['units'] = 'degrees C'
    for v in ['wspd_error', 'forecast_wspd', 'era5_wspd']:
        ds_out[v].attrs['units'] = 'm/s'
    for v in ['wdir_error', 'forecast_wdir', 'era5_wdir']:
        ds_out[v].attrs['units'] = 'degrees (0-360)'
    if 'wspd100_error' in ds_out:
        for v in ['wspd100_error', 'forecast_wspd100', 'era5_wspd100']:
            ds_out[v].attrs['units'] = 'm/s'
        for v in ['wdir100_error', 'forecast_wdir100', 'era5_wdir100']:
            ds_out[v].attrs['units'] = 'degrees (0-360)'
    ds_out.attrs['forecast_model'] = model_name
    if use_interp:
        ds_out.attrs['description'] = (
            f'{model_name} forecast errors vs ERA5-Land reanalysis. '
            f'Forecast regridded to ERA5 grid via bilinear interpolation. '
            f'Wind direction regridded via u/v decomposition. '
            f'Error = forecast - ERA5. valid_time in US/Central (tz-naive).'
        )
        ds_out.attrs['regridding_method'] = 'bilinear_interpolation'
    else:
        ds_out.attrs['description'] = (
            f'{model_name} forecast errors vs ERA5-Land reanalysis. '
            f'Forecast regridded to ERA5 grid via bin-center averaging. '
            f'Wind direction regridded via u/v decomposition. '
            f'Error = forecast - ERA5. valid_time in US/Central (tz-naive).'
        )
        ds_out.attrs['regridding_method'] = 'bin_center_averaging'

    compress = {'zlib': True, 'complevel': 5}
    encoding = {v: compress for v in ds_out.data_vars}
    nc_path = os.path.join(out_dir, f'era5_errors_{year}{month:02d}.nc')
    ds_out.to_netcdf(nc_path, encoding=encoding)
    print(f"  Saved ERA5 error NetCDF ({os.path.getsize(nc_path)/1e6:.1f} MB): {nc_path}")

    # ── Step 9: Compute summary statistics ───────────────────────────────
    # Extract error arrays from the merged dataset for summary computation
    temp_err  = merged['temp_error'].values   # (n_times_merged, n_leads, n_lat, n_lon)
    wspd_err  = merged['wspd_error'].values
    wdir_err  = merged['wdir_error'].values
    has_100m_err = 'wspd100_error' in merged
    if has_100m_err:
        wspd100_err = merged['wspd100_error'].values
        wdir100_err = merged['wdir100_error'].values

    import warnings
    station_summaries = []
    for li, lead in enumerate(lead_hours_list):
        te = temp_err[:, li]   # (n_times, n_lat, n_lon)
        we = wspd_err[:, li]
        de = wdir_err[:, li]

        # Suppress "Mean of empty slice" — expected for ocean/boundary cells
        # that have all-NaN time series; those cells are skipped below.
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', RuntimeWarning)
            temp_mae  = np.nanmean(np.abs(te), axis=0)   # (n_lat, n_lon)
            temp_bias = np.nanmean(te, axis=0)
            wspd_mae  = np.nanmean(np.abs(we), axis=0)
            wspd_bias = np.nanmean(we, axis=0)
            wdir_mae  = np.nanmean(np.abs(de), axis=0)
            if has_100m_err:
                we100 = wspd100_err[:, li]
                de100 = wdir100_err[:, li]
                wspd100_mae  = np.nanmean(np.abs(we100), axis=0)
                wspd100_bias = np.nanmean(we100, axis=0)
                wdir100_mae  = np.nanmean(np.abs(de100), axis=0)
        n_obs = np.sum(~np.isnan(te), axis=0)

        for yi in range(n_lat):
            for xi in range(n_lon):
                if not land_mask[yi, xi] or n_obs[yi, xi] == 0:
                    continue
                row = {
                    'cell_id': f'era5_{yi}_{xi}',
                    'lat': float(era5_lats[yi]),
                    'lon': float(era5_lons[xi]),
                    'lead_hours': lead,
                    'n_obs': int(n_obs[yi, xi]),
                    'temp_mae': float(temp_mae[yi, xi]),
                    'temp_bias': float(temp_bias[yi, xi]),
                    'wspd_mae': float(wspd_mae[yi, xi]),
                    'wspd_bias': float(wspd_bias[yi, xi]),
                    'wdir_mae': float(wdir_mae[yi, xi]),
                }
                if has_100m_err:
                    row.update({
                        'wspd100_mae':  float(wspd100_mae[yi, xi]),
                        'wspd100_bias': float(wspd100_bias[yi, xi]),
                        'wdir100_mae':  float(wdir100_mae[yi, xi]),
                    })
                station_summaries.append(row)

    summary_df = pd.DataFrame(station_summaries)
    summary_path = os.path.join(out_dir, 'error_summary.csv')
    summary_df.to_csv(summary_path, index=False)
    print(f"  Saved summary ({len(summary_df)} rows): {summary_path}")

    # Print aggregate stats by lead
    for lead in sorted(summary_df['lead_hours'].unique()):
        s = summary_df[summary_df['lead_hours'] == lead]
        print(f"\n  {model_name} vs ERA5 — Lead {lead}h ({len(s)} cells):")
        print(f"    Temp MAE:   {s['temp_mae'].mean():.2f} °C  (bias: {s['temp_bias'].mean():+.2f})")
        print(f"    Wspd MAE:   {s['wspd_mae'].mean():.2f} m/s (bias: {s['wspd_bias'].mean():+.2f})")
        print(f"    Wdir MAE:   {s['wdir_mae'].mean():.2f}°")
        if 'wspd100_mae' in s.columns:
            print(f"    Wspd100 MAE:{s['wspd100_mae'].mean():.2f} m/s (bias: {s['wspd100_bias'].mean():+.2f})")
            print(f"    Wdir100 MAE:{s['wdir100_mae'].mean():.2f}°")

    return summary_df


def calculate_era5_errors_for_month(year, month, model='hrrr',
                                     lead_hours=None, force_rebuild_bin_map=False):
    """Calculate forecast errors vs ERA5-Land at every ERA5 grid cell for a month.

    Uses ERA5-Land reanalysis as the ground truth (instead of ISD weather stations),
    providing dense spatial coverage of forecast errors over Texas.

    For HRRR (finer than ERA5), forecast fields are regridded via bin-center
    averaging (~12 forecast cells per ERA5 cell). The bin map is cached to
    {processed}/{model}_to_era5_bin_map.npz and reused across months.

    For GFS (coarser than ERA5), bilinear interpolation is used instead, since
    bin-averaging would leave most ERA5 cells empty at 0.25° → 0.1° resolution.
    GFS leads are collapsed to lead_hours=0 ("day-ahead") by default: each GFS
    file (f018–f041 from the 12z cycle) predicts a unique hour of the next day,
    so collapsing produces a dense output rather than a 96%-NaN sparse array.

    Output (US/Central times throughout):
      {processed}/forecast_errors_era5/{model}/{year}/{month:02d}/
        era5_errors_{YYYYMM}.nc   — full gridded errors (NetCDF, dims: time×lead×lat×lon)
        error_summary.csv          — per-cell, per-lead MAE/bias

    Args:
        year: Four-digit year.
        month: Month 1–12.
        model: 'hrrr' or 'gfs'.
        lead_hours: List of lead hours to keep. None uses the model's
                    default_lead_hours from _MODEL_CONFIG. Pass an explicit
                    list to override (e.g. [1, 18] for both HRRR leads).
        force_rebuild_bin_map: If True, recompute the forecast-to-ERA5 bin map even
                               if a cached copy already exists. Only relevant for
                               bin-averaging models (HRRR). Defaults to False.

    Returns:
        Summary DataFrame with per-cell, per-lead-time statistics.
    """
    dirs = setup_directories()
    raw_dir = dirs['raw']
    processed_dir = dirs['processed']

    model_lower = model.lower()
    if model_lower not in _MODEL_CONFIG:
        raise ValueError(
            f"model must be one of {list(_MODEL_CONFIG)}, got '{model}'"
        )
    cfg = _MODEL_CONFIG[model_lower]
    fc_base = os.path.join(raw_dir, cfg['data_dir'])
    model_name = cfg['display_name']

    # Output directory
    out_dir = os.path.join(
        processed_dir, 'forecast_errors_era5', model_lower, str(year), f"{month:02d}"
    )
    os.makedirs(out_dir, exist_ok=True)

    # ── Load ERA5 as xarray Dataset ──
    era5_nc = os.path.join(raw_dir, 'era5_land', str(year), f'{month:02d}',
                           f'era5_land_{year}{month:02d}.nc')
    if not os.path.exists(era5_nc):
        raise FileNotFoundError(
            f"ERA5-Land file not found: {era5_nc}\n"
            f"Run: uv run python -m download_data.pull_era5 --year {year} --month {month}"
        )

    print(f"Loading ERA5-Land data from {era5_nc}...")
    era5_ds = xr.open_dataset(era5_nc)

    # ── Load ERA5 100m wind (optional, same directory as ERA5-Land) ──────────
    era5_wind100m_nc = os.path.join(
        raw_dir, 'era5_land', str(year), f'{month:02d}',
        f'era5_wind100m_{year}{month:02d}.nc',
    )
    if os.path.exists(era5_wind100m_nc):
        print(f"Loading ERA5 100m wind from {era5_wind100m_nc}...")
        era5_wind100m_ds = xr.open_dataset(era5_wind100m_nc)
    else:
        print(f"  ERA5 100m wind not found; skipping 100m errors")
        era5_wind100m_ds = None

    # ── Get forecast grid lat/lon from a sample file ──
    if model_lower == 'hrrr':
        fc_month_dir = os.path.join(fc_base, str(year), f'{month:02d}')
        nc_files = sorted(Path(fc_month_dir).glob('hrrr_*.nc'))
    else:
        temp_dir = os.path.join(fc_base, 'temp', str(year), f'{month:02d}')
        nc_files = sorted(Path(temp_dir).glob('*.nc'))
    if not nc_files:
        raise FileNotFoundError(f"No {model_name} NetCDF files found in "
                                f"{fc_month_dir if model_lower == 'hrrr' else temp_dir}")

    sample_ds = xr.open_dataset(str(nc_files[0]))
    fc_lats = sample_ds.latitude.values
    fc_lons = sample_ds.longitude.values
    sample_ds.close()

    if fc_lats.ndim == 1:
        print(f"  {model_name} grid: {len(fc_lats)}×{len(fc_lons)} (1D regular) "
              f"→ ERA5 grid: {len(era5_ds.latitude)}×{len(era5_ds.longitude)}")
    else:
        print(f"  {model_name} grid: {fc_lats.shape[0]}×{fc_lats.shape[1]} (2D projected) "
              f"→ ERA5 grid: {len(era5_ds.latitude)}×{len(era5_ds.longitude)}")

    # ── Load forecasts (valid_time → Central inside loader) ─────────────────
    print(f"Loading {model_name} forecasts for {year}-{month:02d}...")
    if model_lower == 'hrrr':
        temp_forecasts = load_hrrr_forecasts(fc_base, 't2m', year, month)
        wspd_forecasts = load_hrrr_forecasts(fc_base, 'si10', year, month)
        wdir_forecasts = load_hrrr_forecasts(fc_base, 'wdir10', year, month)
    else:
        temp_forecasts = load_forecasts(os.path.join(fc_base, 'temp'), 't2m', year, month)
        wspd_forecasts = load_forecasts(os.path.join(fc_base, 'wspd'), 'si10', year, month)
        wdir_forecasts = load_forecasts(os.path.join(fc_base, 'wdir'), 'wdir10', year, month)
    print(f"  Loaded {len(temp_forecasts)} temp, {len(wspd_forecasts)} wspd, "
          f"{len(wdir_forecasts)} wdir forecast fields")

    # ── Load 100m forecast wind ───────────────────────────────────────────────
    wspd100_forecasts = wdir100_forecasts = None
    try:
        if model_lower == 'hrrr':
            # 100m wind is in the same combined files as 10m; variable names si100/wdir100
            wspd100_forecasts = load_hrrr_forecasts(fc_base, 'si100', year, month)
            wdir100_forecasts = load_hrrr_forecasts(fc_base, 'wdir100', year, month)
        else:
            # GFS stores 100m wind in separate wspd100/ and wdir100/ element directories
            wspd100_dir = os.path.join(fc_base, 'wspd100')
            wdir100_dir = os.path.join(fc_base, 'wdir100')
            wspd100_forecasts = load_forecasts(wspd100_dir, 'si100', year, month)
            wdir100_forecasts = load_forecasts(wdir100_dir, 'wdir100', year, month)
    except Exception as e:
        print(f"  WARNING: Could not load 100m wind forecasts ({e}); skipping 100m errors")
        wspd100_forecasts = wdir100_forecasts = None

    if wspd100_forecasts:
        print(f"  Loaded {len(wspd100_forecasts)} wspd100, "
              f"{len(wdir100_forecasts)} wdir100 forecast fields")
    else:
        wspd100_forecasts = wdir100_forecasts = None  # ensure None not []

    # Apply lead-time filtering and optional collapse
    effective_leads = lead_hours if lead_hours is not None else cfg.get('default_lead_hours')
    collapse = cfg.get('collapse_leads', False)

    temp_forecasts = _filter_forecasts(temp_forecasts, effective_leads, collapse)
    wspd_forecasts = _filter_forecasts(wspd_forecasts, effective_leads, collapse)
    wdir_forecasts = _filter_forecasts(wdir_forecasts, effective_leads, collapse)
    if wspd100_forecasts is not None:
        wspd100_forecasts = _filter_forecasts(wspd100_forecasts, effective_leads, collapse)
        wdir100_forecasts = _filter_forecasts(wdir100_forecasts, effective_leads, collapse)

    if effective_leads is not None or collapse:
        print(f"  After filtering: {len(temp_forecasts)} temp, {len(wspd_forecasts)} wspd, "
              f"{len(wdir_forecasts)} wdir fields (leads={effective_leads}, collapse={collapse})")
        if wspd100_forecasts is not None:
            print(f"  After filtering: {len(wspd100_forecasts)} wspd100, "
                  f"{len(wdir100_forecasts)} wdir100 fields")

    # ── Regrid forecasts, merge with ERA5, compute errors ──────────────────
    regrid_method = cfg['regrid_method']

    # Bin map caching is only relevant for the 'bin' regridding method
    bin_map_cache_path = None
    if regrid_method == 'bin':
        bin_map_cache_path = os.path.join(
            processed_dir, f'{model_lower}_to_era5_bin_map.npz'
        )

    print(f"Computing ERA5-vs-{model_name} errors for {year}-{month:02d}...")
    summary = _compute_era5_gridded_errors(
        temp_forecasts, wspd_forecasts, wdir_forecasts,
        era5_ds, out_dir, year, month, model_name,
        fc_lats, fc_lons,
        regrid_method=regrid_method,
        bin_map_cache_path=bin_map_cache_path,
        force_rebuild_bin_map=force_rebuild_bin_map,
        wspd100_forecasts=wspd100_forecasts,
        wdir100_forecasts=wdir100_forecasts,
        era5_wind100m_ds=era5_wind100m_ds,
    )
    era5_ds.close()
    if era5_wind100m_ds is not None:
        era5_wind100m_ds.close()
    return summary


if __name__ == '__main__':
    calculate_era5_errors_for_month(2025, 1, model='hrrr', force_rebuild_bin_map=False)
