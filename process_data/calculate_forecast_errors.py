"""calculate_forecast_errors.py — Compute forecast errors at weather station locations.

Merges NDFD gridded weather forecasts with ISD weather station observations.
For each forecast file, loads the gridded xarray dataset and uses xarray's
nearest-neighbor selection to extract forecast values at station coordinates
(loaded as a GeoDataFrame). Computes forecast error = forecast - observed.

Only keeps station observations at the top of each hour (rounding to nearest hour)
to match the hourly forecast valid times.

Output: One CSV per station in {processed}/forecast_errors/{year}/{month:02d}/
with columns: station_id, valid_time, lead_hours, forecast_temp, observed_temp,
              temp_error, forecast_wspd, observed_wspd, wspd_error,
              forecast_wdir, observed_wdir, wdir_error
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
from create_plots import parse_tmp, parse_wnd_speed


def parse_wnd_direction(wnd_str):
    """Parse ISD WND field to wind direction in degrees."""
    if pd.isna(wnd_str):
        return None
    parts = str(wnd_str).split(',')
    if len(parts) < 1 or parts[0] == '999':
        return None
    return int(parts[0])


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
    """Load and resample all station observations to hourly.

    Returns a dict mapping station_id -> DataFrame with columns:
        valid_time, obs_temp, obs_wspd, obs_wdir
    """
    obs_dict = {}
    data_dir = os.path.join(raw_dir, 'weather_stations', str(year), f"{month:02d}")

    for _, row in stations_gdf.iterrows():
        station_id = row['station_id']
        fpath = os.path.join(data_dir, f"{station_id}.csv")
        if not os.path.exists(fpath):
            continue

        df = pd.read_csv(fpath, dtype={'STATION': str})
        df['datetime'] = pd.to_datetime(df['DATE'])

        # Parse fields
        df['obs_temp'] = df['TMP'].apply(parse_tmp)
        df['obs_wspd'] = df['WND'].apply(parse_wnd_speed)
        df['obs_wdir'] = df['WND'].apply(parse_wnd_direction)

        # Round each observation to its nearest hour
        df['valid_time'] = df['datetime'].dt.round('h')

        # For each hour, keep the observation closest to the top of that hour
        df['time_diff'] = (df['datetime'] - df['valid_time']).abs()
        df = df.sort_values('time_diff').drop_duplicates(subset='valid_time', keep='first')
        df = df.sort_values('valid_time')

        obs_dict[station_id] = df[['valid_time', 'obs_temp', 'obs_wspd', 'obs_wdir']].reset_index(drop=True)

    return obs_dict


def build_ndfd_grid_gdf(sample_nc_path):
    """Build a GeoDataFrame of NDFD grid points from a sample NetCDF file.

    Each row is one grid cell with its (y, x) index and lat/lon as a Point geometry.
    Used for spatial join against station points.

    Returns GeoDataFrame with columns: y_idx, x_idx, grid_lat, grid_lon, geometry
    """
    ds = xr.open_dataset(sample_nc_path)
    lat = ds.latitude.values
    lon = ds.longitude.values
    ds.close()

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
    """Spatially join each station to its nearest NDFD grid point.

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


def load_ndfd_forecasts(element_dir, variable_name, year, month):
    """Load all NDFD forecast files for one element and extract metadata.

    Returns a list of dicts with keys:
        issuance_time, valid_time, lead_hours, data (2D array)
    One entry per (file, step) combination.
    """
    nc_dir = os.path.join(element_dir, str(year), f"{month:02d}")
    nc_files = sorted(Path(nc_dir).glob('*.nc'))

    records = []
    for fpath in nc_files:
        ds = xr.open_dataset(str(fpath))
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
            records.append({
                'issuance_time': issuance_time,
                'valid_time': pd.Timestamp(vt),
                'lead_hours': lead_hours,
                'data': data,
            })
        ds.close()

    return records


def calculate_errors_for_month(year, month):
    """Calculate forecast errors at all weather stations for a given month.

    1. Load station locations as a GeoDataFrame
    2. Build a GeoDataFrame of the NDFD grid and spatially join stations to
       their nearest grid cell
    3. Load all NDFD forecasts and ISD observations
    4. For each station and matching forecast valid time, compute error
    5. Save per-station CSVs and a summary CSV

    Returns summary DataFrame with per-station error statistics.
    """
    dirs = setup_directories()
    raw_dir = dirs['raw']
    processed_dir = dirs['processed']
    ndfd_base = os.path.join(raw_dir, 'ndfd_data')

    # Output directory
    out_dir = os.path.join(processed_dir, 'forecast_errors', str(year), f"{month:02d}")
    os.makedirs(out_dir, exist_ok=True)

    # Load station metadata as GeoDataFrame
    stations_gdf = load_stations_gdf(raw_dir)
    print(f"Loaded {len(stations_gdf)} stations as GeoDataFrame")

    # Build NDFD grid GeoDataFrame and spatially join stations to nearest grid cell
    temp_dir = os.path.join(ndfd_base, 'temp', str(year), f"{month:02d}")
    sample_nc = sorted(Path(temp_dir).glob('*.nc'))[0]
    print("Building NDFD grid GeoDataFrame and joining stations...")
    grid_gdf = build_ndfd_grid_gdf(str(sample_nc))
    station_grid_map = spatial_join_stations_to_grid(stations_gdf, grid_gdf)
    print(f"  Joined {len(station_grid_map)} stations to grid (mean dist: "
          f"{station_grid_map['dist_m'].mean():.0f} m)")

    # Load all observations
    print("Loading station observations...")
    obs_dict = load_all_observations(stations_gdf, year, month, raw_dir)
    print(f"  Loaded observations for {len(obs_dict)} stations")

    # Load NDFD forecasts
    print(f"Loading NDFD forecasts for {year}-{month:02d}...")
    temp_forecasts = load_ndfd_forecasts(
        os.path.join(ndfd_base, 'temp'), 't2m', year, month)
    wspd_forecasts = load_ndfd_forecasts(
        os.path.join(ndfd_base, 'wspd'), 'si10', year, month)
    wdir_forecasts = load_ndfd_forecasts(
        os.path.join(ndfd_base, 'wdir'), 'wdir10', year, month)
    print(f"  Loaded {len(temp_forecasts)} temp, {len(wspd_forecasts)} wspd, "
          f"{len(wdir_forecasts)} wdir forecast fields")

    # Index forecasts by (valid_time, lead_hours) for fast lookup
    temp_index = {(r['valid_time'], r['lead_hours']): r['data'] for r in temp_forecasts}
    wspd_index = {(r['valid_time'], r['lead_hours']): r['data'] for r in wspd_forecasts}
    wdir_index = {(r['valid_time'], r['lead_hours']): r['data'] for r in wdir_forecasts}

    # Get sorted list of unique (valid_time, lead_hours) keys present in all three
    all_keys = sorted(set(temp_index.keys()) & set(wspd_index.keys()) & set(wdir_index.keys()))
    print(f"  {len(all_keys)} forecast (valid_time, lead_hours) entries matched across all elements")

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
            # NDFD temp is in Kelvin, convert to Celsius
            fc_temp_k = temp_index[(valid_time, lead_hours)][y_idx, x_idx]
            fc_temp = float(fc_temp_k) - 273.15 if not np.isnan(fc_temp_k) else np.nan

            fc_wspd = float(wspd_index[(valid_time, lead_hours)][y_idx, x_idx])
            fc_wdir = float(wdir_index[(valid_time, lead_hours)][y_idx, x_idx])

            obs_temp = obs_row['obs_temp']
            obs_wspd = obs_row['obs_wspd']
            obs_wdir = obs_row['obs_wdir']

            records.append({
                'station_id': station_id,
                'valid_time': vt_ts,
                'lead_hours': lead_hours,
                'forecast_temp': round(fc_temp, 2) if not np.isnan(fc_temp) else np.nan,
                'observed_temp': obs_temp,
                'temp_error': round(fc_temp - obs_temp, 2) if (not np.isnan(fc_temp) and obs_temp is not None) else np.nan,
                'forecast_wspd': round(fc_wspd, 2) if not np.isnan(fc_wspd) else np.nan,
                'observed_wspd': obs_wspd,
                'wspd_error': round(fc_wspd - obs_wspd, 2) if (not np.isnan(fc_wspd) and obs_wspd is not None) else np.nan,
                'forecast_wdir': round(fc_wdir, 1) if not np.isnan(fc_wdir) else np.nan,
                'observed_wdir': obs_wdir,
                'wdir_error': round(fc_wdir - obs_wdir, 1) if (not np.isnan(fc_wdir) and obs_wdir is not None) else np.nan,
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
            })

        n_processed += 1
        if n_processed % 50 == 0:
            print(f"  Processed {n_processed} stations")

    print(f"  Processed {n_processed} stations total")

    summary_df = pd.DataFrame(station_summaries)
    summary_path = os.path.join(out_dir, 'error_summary.csv')
    summary_df.to_csv(summary_path, index=False)
    print(f"\nSaved {len(summary_df)} summary rows to {summary_path}")

    # Print aggregate stats
    for lead in sorted(summary_df['lead_hours'].unique()):
        s = summary_df[summary_df['lead_hours'] == lead]
        print(f"\n  Lead {lead}h — {len(s)} stations:")
        print(f"    Temp MAE:  {s['temp_mae'].mean():.2f} °C  (bias: {s['temp_bias'].mean():+.2f})")
        print(f"    Wspd MAE:  {s['wspd_mae'].mean():.2f} m/s (bias: {s['wspd_bias'].mean():+.2f})")

    return summary_df


if __name__ == '__main__':
    calculate_errors_for_month(2025, 7)
