import os
import glob
import pandas as pd
import numpy as np
import geopandas as gpd
import xarray as xr

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from helper_funcs import setup_directories
from process_data.process_ercot import (
    load_rt_spp_month,
    load_actual_load_month,
    extract_demand_forecast_lead_times,
)


DEFAULT_WEATHER_ZONE_SHP = (
    '/Users/ohouck/Library/CloudStorage/OneDrive-TheUniversityofChicago/'
    'ercot_sim_weather_forecasts/Texas_GIS_Data/Weather Zone/Weather_Zone.shp'
)


def _normalize_weather_zone_name(name):
    """Normalize weather-zone naming across shapefile and ERCOT datasets."""
    base = str(name).strip().lower().replace('-', '_').replace(' ', '_')
    zone_map = {
        'coast': 'coast',
        'east': 'east',
        'farwest': 'far_west',
        'far_west': 'far_west',
        'north': 'north',
        'northc': 'north_central',
        'northcentral': 'north_central',
        'north_central': 'north_central',
        'south': 'south',
        'southern': 'south',
        'southc': 'south_central',
        'southcentral': 'south_central',
        'south_central': 'south_central',
        'west': 'west',
    }
    return zone_map.get(base)


def _hour_ending_to_int(hour_ending):
    """Parse ERCOT hourEnding strings like '1:00'/'01:00'/'24:00'."""
    return pd.to_numeric(
        hour_ending.astype(str).str.split(':').str[0],
        errors='coerce',
    )


def _load_weather_zone_load_data(months, dirs, cache_tag):
    """Build and save weather-zone load actuals, 1h/18h forecasts, and errors."""
    actual_zone_cols = {
        'coast': 'coast',
        'east': 'east',
        'farWest': 'far_west',
        'north': 'north',
        'northC': 'north_central',
        'southC': 'south_central',
        'southern': 'south',
        'west': 'west',
    }
    forecast_zone_cols = {
        'coast': 'coast',
        'east': 'east',
        'farWest': 'far_west',
        'north': 'north',
        'northCentral': 'north_central',
        'southCentral': 'south_central',
        'southern': 'south',
        'west': 'west',
    }

    actual_parts = []
    forecast_parts = []

    for year, month in months:
        actual_df = load_actual_load_month(year, month).copy()
        actual_df['operatingDay'] = pd.to_datetime(actual_df['operatingDay'])
        actual_df['hourEndingNum'] = _hour_ending_to_int(actual_df['hourEnding'])
        actual_df = actual_df.dropna(subset=['hourEndingNum'])
        actual_df['hour'] = actual_df['operatingDay'] + pd.to_timedelta(
            actual_df['hourEndingNum'] - 1,
            unit='h',
        )

        for col, zone in actual_zone_cols.items():
            if col not in actual_df.columns:
                continue
            part = actual_df[['hour', col]].copy()
            part['weather_zone'] = zone
            part = part.rename(columns={col: 'actual_load'})
            actual_parts.append(part)

        forecast_df = extract_demand_forecast_lead_times(year, month, lead_hours=[1, 18]).copy()
        forecast_df['hour'] = pd.to_datetime(forecast_df['delivery_dt']) - pd.Timedelta(hours=1)

        for lead in [1, 18]:
            lead_df = forecast_df[forecast_df['lead_target'] == lead].copy()
            for col, zone in forecast_zone_cols.items():
                if col not in lead_df.columns:
                    continue
                part = lead_df[['hour', col]].copy()
                part['weather_zone'] = zone
                part['lead_hours'] = lead
                part = part.rename(columns={col: 'forecast_load'})
                forecast_parts.append(part)

    actual_long = pd.concat(actual_parts, ignore_index=True)
    actual_long['actual_load'] = pd.to_numeric(actual_long['actual_load'], errors='coerce')
    actual_long = (
        actual_long
        .dropna(subset=['weather_zone', 'hour'])
        .groupby(['weather_zone', 'hour'], as_index=False)['actual_load']
        .mean()
    )

    forecast_long = pd.concat(forecast_parts, ignore_index=True)
    forecast_long['forecast_load'] = pd.to_numeric(forecast_long['forecast_load'], errors='coerce')
    forecast_long = forecast_long.dropna(subset=['weather_zone', 'hour'])

    forecast_wide = (
        forecast_long
        .pivot_table(
            index=['weather_zone', 'hour'],
            columns='lead_hours',
            values='forecast_load',
            aggfunc='mean',
        )
        .rename(columns={1: 'forecast_load_1h', 18: 'forecast_load_18h'})
        .reset_index()
    )

    load_errors = actual_long.merge(forecast_wide, on=['weather_zone', 'hour'], how='left')

    load_errors['load_error_1h'] = load_errors['forecast_load_1h'] - load_errors['actual_load']
    load_errors['load_error_18h'] = load_errors['forecast_load_18h'] - load_errors['actual_load']

    out_dir = os.path.join(dirs['processed'], 'load_errors_by_weather_zone')
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, f'load_errors_wz_{cache_tag}.csv')
    load_errors.sort_values(['weather_zone', 'hour']).to_csv(out_file, index=False)
    print(f"Saved weather-zone load forecasts/errors to {out_file}")

    return load_errors


def _map_nodes_to_weather_zones(node_station, weather_zone_shapefile):
    """Spatially assign each node to an ERCOT weather zone polygon."""
    if not os.path.exists(weather_zone_shapefile):
        raise FileNotFoundError(
            f"Weather zone shapefile not found: {weather_zone_shapefile}"
        )

    zones = gpd.read_file(weather_zone_shapefile)
    zone_col = None
    for candidate in ['Zone_name', 'zone_name', 'ZONE_NAME']:
        if candidate in zones.columns:
            zone_col = candidate
            break
    if zone_col is None:
        raise ValueError(
            "Could not find a zone name column in weather-zone shapefile. "
            "Expected one of: Zone_name, zone_name, ZONE_NAME."
        )

    zones = zones[[zone_col, 'geometry']].rename(columns={zone_col: 'weather_zone_raw'})
    zones['weather_zone'] = zones['weather_zone_raw'].apply(_normalize_weather_zone_name)
    zones = zones.dropna(subset=['weather_zone']).to_crs('EPSG:4326')

    node_points = gpd.GeoDataFrame(
        node_station[['settlement_point', 'lon', 'lat']].copy(),
        geometry=gpd.points_from_xy(node_station['lon'], node_station['lat']),
        crs='EPSG:4326',
    )

    node_zone = gpd.sjoin(
        node_points,
        zones[['weather_zone_raw', 'weather_zone', 'geometry']],
        how='left',
        predicate='intersects',
    )

    missing_mask = node_zone['weather_zone'].isna()
    if missing_mask.any():
        # Fallback to nearest zone for nodes near polygon boundaries.
        missing_points = node_zone.loc[missing_mask, ['settlement_point', 'geometry']].copy()
        missing_points = gpd.GeoDataFrame(missing_points, geometry='geometry', crs='EPSG:4326')
        nearest = gpd.sjoin_nearest(
            missing_points.to_crs('EPSG:3857'),
            zones[['weather_zone_raw', 'weather_zone', 'geometry']].to_crs('EPSG:3857'),
            how='left',
            distance_col='zone_dist_m',
        )
        nearest = nearest[['settlement_point', 'weather_zone_raw', 'weather_zone']].drop_duplicates('settlement_point')
        nearest_idx = nearest.set_index('settlement_point')
        node_zone.loc[missing_mask, 'weather_zone_raw'] = (
            node_zone.loc[missing_mask, 'settlement_point'].map(nearest_idx['weather_zone_raw'])
        )
        node_zone.loc[missing_mask, 'weather_zone'] = (
            node_zone.loc[missing_mask, 'settlement_point'].map(nearest_idx['weather_zone'])
        )

    node_zone = (
        node_zone
        .drop_duplicates('settlement_point')
        [['settlement_point', 'weather_zone_raw', 'weather_zone']]
        .copy()
    )
    return node_zone


def _load_era5_errors_for_nodes(months, model, dirs, node_coords):
    """Load ERA5 gridded forecast errors and extract values at node coordinates.

    Uses xr.sel(method='nearest') to find the nearest ERA5 grid cell for each
    ERCOT node — no GeoDataFrame sjoin needed since ERA5 has a regular grid.

    Args:
        months: List of (year, month) tuples.
        model: 'hrrr' or 'ndfd'.
        dirs: dict from setup_directories().
        node_coords: DataFrame with settlement_point, lat, lon.

    Returns:
        DataFrame in wide format with one row per (settlement_point, hour),
        columns matching the station-based pivot format.
    """
    MODEL_LEAD_TIMES = {'ndfd': (1, 25), 'hrrr': (1, 18)}
    lead_short, lead_long = MODEL_LEAD_TIMES[model]

    # Load and concat ERA5 error NetCDFs for all months
    parts = []
    for year, month in sorted(months):
        nc_path = os.path.join(
            dirs['processed'], 'forecast_errors_era5', model,
            str(year), f'{month:02d}', f'era5_errors_{year}{month:02d}.nc'
        )
        if not os.path.exists(nc_path):
            raise FileNotFoundError(
                f"ERA5 error file not found: {nc_path}\n"
                f"Run calculate_era5_errors_for_month({year}, {month}, model='{model}') first."
            )
        ds = xr.open_dataset(nc_path)
        # Recover valid_time as datetime (stored as int64 nanoseconds)
        ds['valid_time'] = pd.to_datetime(ds['valid_time'].values)
        parts.append(ds)
        print(f"  Loaded ERA5 errors: {nc_path}")

    era5_ds = xr.concat(parts, dim='valid_time') if len(parts) > 1 else parts[0]

    # Select nearest ERA5 cell for all nodes at once (vectorized)
    node_lats = xr.DataArray(node_coords['lat'].values, dims='node')
    node_lons = xr.DataArray(node_coords['lon'].values, dims='node')
    node_errors = era5_ds.sel(
        latitude=node_lats, longitude=node_lons, method='nearest'
    )
    # Result: Dataset with dims (valid_time, lead_hours, node)

    valid_times = pd.to_datetime(era5_ds.valid_time.values)
    lead_hours_vals = era5_ds.lead_hours.values
    n_nodes = len(node_coords)
    n_times = len(valid_times)
    settlement_points = node_coords['settlement_point'].values
    node_lat_vals = node_coords['lat'].values
    node_lon_vals = node_coords['lon'].values

    # Build DataFrames for each lead time using vectorized array operations
    dfs_by_lead = {}
    for li, lead in enumerate(lead_hours_vals):
        lead_int = int(lead)
        sub = node_errors.isel(lead_hours=li)

        # Each variable is shape (n_times, n_nodes) — flatten to (n_times * n_nodes,)
        hours = np.repeat(valid_times, n_nodes)
        sps = np.tile(settlement_points, n_times)
        lats = np.tile(node_lat_vals, n_times)
        lons = np.tile(node_lon_vals, n_times)

        df = pd.DataFrame({
            'settlement_point': sps,
            'hour': pd.to_datetime(hours).floor('h'),
            'lat': lats,
            'lon': lons,
            f'temp_error_{lead_int}h': sub['temp_error'].values.ravel(),
            f'wspd_error_{lead_int}h': sub['wspd_error'].values.ravel(),
            f'wdir_degree_error_{lead_int}h': sub['wdir_error'].values.ravel(),
            f'forecast_temp_{lead_int}h': sub['forecast_temp'].values.ravel(),
            f'forecast_wspd_{lead_int}h': sub['forecast_wspd'].values.ravel(),
        })

        # Add observed columns only from the short lead (identical for both)
        if lead_int == lead_short:
            df['observed_temp'] = sub['era5_temp'].values.ravel()
            df['observed_wspd'] = sub['era5_wspd'].values.ravel()
            df['observed_wdir'] = sub['era5_wdir'].values.ravel()

        dfs_by_lead[lead_int] = df

    # Merge leads on (settlement_point, hour)
    errors_wide = dfs_by_lead[lead_short].merge(
        dfs_by_lead[lead_long].drop(columns=['lat', 'lon']),
        on=['settlement_point', 'hour'],
        how='outer',
    )

    # Close datasets
    for ds in parts:
        ds.close()

    print(f"  ERA5 errors: {node_coords['settlement_point'].nunique()} nodes, "
          f"{len(errors_wide):,} node-hour rows")

    return errors_wide


def prepare_node_level_data(
    months,
    model='ndfd',
    force_rebuild=False,
    weather_zone_shapefile=DEFAULT_WEATHER_ZONE_SHP,
    error_source='station',
):
    """
    Build a node × hour dataset linking ERCOT LMP to weather forecast errors.

    Supports two error sources:
      - 'station': Each node is matched to its nearest ISD weather station via
        sjoin_nearest. Station-level forecast errors from per-station CSVs.
      - 'era5': Each node is matched to its nearest ERA5 grid cell via
        xr.sel(method='nearest'). No GeoDataFrame construction needed.

    Supports forecast models with different lead times:
      - ndfd: 1h (short) and 25h (long). forecasts available every 3 hours
      - hrrr: 1h (short) and 18h (long)

    Output columns use the actual lead hour as suffix (e.g. temp_error_1h,
    temp_error_25h for NDFD; temp_error_1h, temp_error_18h for HRRR).

    Args:
        months: List of (year, month) tuples to include, e.g. [(2025, 1), (2025, 7)].
                Can also be a single tuple (year, month) for backwards compatibility.
        model: Forecast model — 'ndfd' or 'hrrr' (default 'ndfd')
        force_rebuild: If True, rebuild even if cached file exists
        weather_zone_shapefile: Path to ERCOT weather-zone shapefile
        error_source: 'station' (default) or 'era5'. Determines how forecast
            errors are loaded and spatially matched to nodes.

    Returns:
        DataFrame with one row per (settlement_point, hour) and columns for
        LMP, short/long-lead forecast errors, observed weather, and station
        distance.
    """
    # Accept a single (year, month) tuple for convenience
    if isinstance(months, tuple) and len(months) == 2 and isinstance(months[0], int):
        months = [months]

    # Model-specific lead times
    MODEL_LEAD_TIMES = {
        'ndfd': (1, 25),
        'hrrr': (1, 18),
    }
    if model not in MODEL_LEAD_TIMES:
        raise ValueError(f"Unknown model '{model}'. Choose from: {list(MODEL_LEAD_TIMES)}")

    lead_short, lead_long = MODEL_LEAD_TIMES[model]

    dirs = setup_directories()

    # Build a cache key from the sorted list of months
    months = sorted(months)
    if len(months) == 1:
        year, month = months[0]
        cache_tag = f"{year}_{month:02d}"
    else:
        first_y, first_m = months[0]
        last_y, last_m = months[-1]
        cache_tag = f"{first_y}{first_m:02d}_{last_y}{last_m:02d}"

    source_tag = '' if error_source == 'station' else f'_{error_source}'
    cache_file = os.path.join(
        dirs['processed'],
        f'node_hourly_{model}{source_tag}_{cache_tag}.csv'
    )

    if os.path.exists(cache_file) and not force_rebuild:
        print(f"Loading cached node-level data from {cache_file}")
        return pd.read_csv(cache_file, parse_dates=['hour'])

    period_str = ", ".join(f"{y}-{m:02d}" for y, m in months)
    print(f"Building node-level dataset from scratch (model={model}, months={period_str})...")

    # ── Load node coordinates ──
    print("Loading node coordinates...")
    node_coords = pd.read_csv(os.path.join(dirs['processed'], 'node_coordinates.csv'))
    node_coords = node_coords.dropna(subset=['lat', 'lon'])
    print(f"  {len(node_coords)} nodes with coordinates")

    if error_source == 'era5':
        # ── ERA5 path: load gridded errors directly, no station indirection ──
        print("Loading ERA5 gridded forecast errors...")
        errors_wide = _load_era5_errors_for_nodes(months, model, dirs, node_coords)

        # Weather zone assignment (uses node lat/lon, same for both paths)
        node_meta = node_coords[['settlement_point', 'lat', 'lon']].copy()
        print("Joining each node to a weather zone polygon...")
        node_zone = _map_nodes_to_weather_zones(node_meta, weather_zone_shapefile)
        n_zoned = node_zone['weather_zone'].notna().sum()
        print(f"  Assigned weather zones for {n_zoned} nodes")

        # Load and aggregate RT SPP prices to hourly
        print("Loading RT SPP prices...")
        rt_spp_dfs = []
        for year, month in months:
            rt_spp_dfs.append(load_rt_spp_month(year, month))
        rt_spp = pd.concat(rt_spp_dfs, ignore_index=True)
        rt_spp = rt_spp[rt_spp['settlementPointType'] == 'RN'].copy()
        rt_spp['deliveryDate'] = pd.to_datetime(rt_spp['deliveryDate'])
        rt_spp['hour'] = rt_spp['deliveryDate'] + pd.to_timedelta(
            rt_spp['deliveryHour'] - 1, unit='h'
        )
        price_hourly = (
            rt_spp
            .groupby(['settlementPoint', 'hour'])['settlementPointPrice']
            .agg(lmp='first', lmp_mean='mean', lmp_max='max', lmp_std='std')
            .reset_index()
        )
        price_hourly['lmp_std'] = price_hourly['lmp_std'].fillna(0)
        price_hourly = price_hourly.rename(columns={'settlementPoint': 'settlement_point'})
        print(f"  {len(price_hourly):,} node-hour price observations")

        # Merge prices with node coordinates
        print("Merging prices with ERA5 errors...")
        node_hourly = price_hourly.merge(
            node_coords[['settlement_point', 'lat', 'lon']],
            on='settlement_point',
            how='inner',
        )
        node_hourly = node_hourly.merge(
            node_zone[['settlement_point', 'weather_zone_raw', 'weather_zone']],
            on='settlement_point',
            how='left',
        )
        print(f"  {node_hourly['settlement_point'].nunique()} nodes with prices + coords")

        # Load errors
        print("Loading actual load and extracting 1h/18h load forecasts...")
        load_errors = _load_weather_zone_load_data(months, dirs, cache_tag)

        # Attach ERA5 forecast errors (keyed by settlement_point, not station_id)
        print("Attaching ERA5 forecast errors...")
        node_hourly = node_hourly.merge(
            errors_wide.drop(columns=['lat', 'lon']),
            on=['settlement_point', 'hour'],
            how='left',
        )
        node_hourly = node_hourly.merge(
            load_errors,
            on=['weather_zone', 'hour'],
            how='left',
        )

    else:
        # ── Station path: original approach with station CSVs + sjoin ──
        print("Loading forecast errors from station CSVs...")
        all_error_files = []
        for year, month in months:
            forecast_error_dir = os.path.join(
                dirs['processed'], 'forecast_errors', model, str(year), f"{month:02d}"
            )
            month_files = glob.glob(os.path.join(forecast_error_dir, '*.csv'))
            month_files = [f for f in month_files if not f.endswith('error_summary.csv')]
            all_error_files.extend(month_files)
            print(f"  {year}-{month:02d}: {len(month_files)} station files")

        error_dfs = [pd.read_csv(f) for f in all_error_files]
        all_errors = pd.concat(error_dfs, ignore_index=True)
        all_errors['valid_time'] = pd.to_datetime(all_errors['valid_time'])
        all_errors['hour'] = all_errors['valid_time'].dt.floor('h')

        print(f"  Total: {len(all_errors):,} station-hour-lead observations "
              f"({model.upper()}, {len(months)} months)")

        # Pivot lead_hours into separate columns
        error_cols = [c for c in all_errors.columns
                      if c not in ('station_id', 'valid_time', 'lead_hours',
                                   'hour', 'lat', 'lon')]

        suffix_short = f'_{lead_short}h'
        suffix_long = f'_{lead_long}h'

        lead_short_df = all_errors[all_errors['lead_hours'] == lead_short].copy()
        lead_long_df = all_errors[all_errors['lead_hours'] == lead_long].copy()

        rename_short = {c: f'{c}{suffix_short}' for c in error_cols}
        rename_long = {c: f'{c}{suffix_long}' for c in error_cols}

        lead_short_df = lead_short_df.rename(columns=rename_short)
        lead_long_df = lead_long_df.rename(columns=rename_long)

        keep_short = ['station_id', 'hour', 'lat', 'lon'] + list(rename_short.values())
        keep_long = ['station_id', 'hour'] + list(rename_long.values())

        errors_wide = lead_short_df[keep_short].merge(
            lead_long_df[keep_long],
            on=['station_id', 'hour'],
            how='outer'
        )
        print(f"  After pivot: {len(errors_wide):,} station-hour rows")

        # Deduplicate observed columns
        obs_base_cols = [c for c in error_cols if c.startswith('observed_')]
        for base in obs_base_cols:
            col_short = f'{base}{suffix_short}'
            col_long = f'{base}{suffix_long}'
            if col_short not in errors_wide.columns or col_long not in errors_wide.columns:
                continue
            both_present = errors_wide[col_short].notna() & errors_wide[col_long].notna()
            mismatches = (errors_wide.loc[both_present, col_short] !=
                          errors_wide.loc[both_present, col_long]).sum()
            if mismatches > 0:
                raise ValueError(
                    f"Observed column mismatch: {col_short} vs {col_long} differ "
                    f"in {mismatches} rows where both are non-NA."
                )
            errors_wide[col_short] = errors_wide[col_short].fillna(errors_wide[col_long])
            errors_wide = errors_wide.drop(columns=[col_long])
            errors_wide = errors_wide.rename(columns={col_short: base})

        # Build station GeoDataFrame
        print("Building station GeoDataFrame...")
        stations_meta = (
            errors_wide[['station_id', 'lat', 'lon']]
            .dropna(subset=['lat', 'lon'])
            .drop_duplicates('station_id')
            .copy()
        )
        stations_gdf = gpd.GeoDataFrame(
            stations_meta,
            geometry=gpd.points_from_xy(stations_meta['lon'], stations_meta['lat']),
            crs='EPSG:4326'
        ).to_crs('EPSG:3857')
        print(f"  {len(stations_gdf)} unique stations")

        # Build node GeoDataFrame
        nodes_gdf = gpd.GeoDataFrame(
            node_coords,
            geometry=gpd.points_from_xy(node_coords['lon'], node_coords['lat']),
            crs='EPSG:4326'
        ).to_crs('EPSG:3857')

        # Spatial join — each node to its nearest station
        print("Joining each node to nearest weather station...")
        node_station = gpd.sjoin_nearest(
            nodes_gdf[['settlement_point', 'lat', 'lon', 'geometry']],
            stations_gdf[['station_id', 'geometry']],
            how='left',
            distance_col='dist_m'
        )
        node_station = (
            node_station
            .drop_duplicates('settlement_point')
            [['settlement_point', 'lat', 'lon', 'station_id', 'dist_m']]
            .copy()
        )
        node_station['dist_km'] = node_station['dist_m'] / 1000.0
        print(f"  Matched {node_station['station_id'].notna().sum()} nodes to stations")
        print(f"  Distance: mean {node_station['dist_km'].mean():.1f} km, "
              f"max {node_station['dist_km'].max():.1f} km")

        # Spatial join nodes to weather-zone polygons
        print("Joining each node to a weather zone polygon...")
        node_zone = _map_nodes_to_weather_zones(node_station, weather_zone_shapefile)
        n_zoned = node_zone['weather_zone'].notna().sum()
        print(f"  Assigned weather zones for {n_zoned} nodes")

        # Load and aggregate RT SPP prices to hourly
        print("Loading RT SPP prices...")
        rt_spp_dfs = []
        for year, month in months:
            rt_spp_dfs.append(load_rt_spp_month(year, month))
        rt_spp = pd.concat(rt_spp_dfs, ignore_index=True)
        rt_spp = rt_spp[rt_spp['settlementPointType'] == 'RN'].copy()
        rt_spp['deliveryDate'] = pd.to_datetime(rt_spp['deliveryDate'])
        rt_spp['hour'] = rt_spp['deliveryDate'] + pd.to_timedelta(
            rt_spp['deliveryHour'] - 1, unit='h'
        )
        price_hourly = (
            rt_spp
            .groupby(['settlementPoint', 'hour'])['settlementPointPrice']
            .agg(lmp='first', lmp_mean='mean', lmp_max='max', lmp_std='std')
            .reset_index()
        )
        price_hourly['lmp_std'] = price_hourly['lmp_std'].fillna(0)
        price_hourly = price_hourly.rename(columns={'settlementPoint': 'settlement_point'})
        print(f"  {len(price_hourly):,} node-hour price observations")

        # Merge prices with node→station mapping
        print("Merging prices with node-station mapping...")
        price_with_station = price_hourly.merge(
            node_station[['settlement_point', 'lat', 'lon', 'station_id', 'dist_km']],
            on='settlement_point',
            how='inner'
        )
        price_with_station = price_with_station.merge(
            node_zone[['settlement_point', 'weather_zone_raw', 'weather_zone']],
            on='settlement_point',
            how='left',
        )
        print(f"  {price_with_station['settlement_point'].nunique()} nodes with prices + coords")

        # Load errors
        print("Loading actual load and extracting 1h/18h load forecasts...")
        load_errors = _load_weather_zone_load_data(months, dirs, cache_tag)

        # Attach weather forecast errors and load errors
        print("Attaching forecast errors...")
        node_hourly = price_with_station.merge(
            errors_wide.drop(columns=['lat', 'lon']),
            on=['station_id', 'hour'],
            how='left'
        )
        node_hourly = node_hourly.merge(
            load_errors,
            on=['weather_zone', 'hour'],
            how='left',
        )

    # Time features (common to both paths)
    node_hourly['hour_dt'] = pd.to_datetime(node_hourly['hour'])
    node_hourly['day_of_month'] = node_hourly['hour_dt'].dt.day
    node_hourly['hour_of_day'] = node_hourly['hour_dt'].dt.hour
    node_hourly['weekday'] = node_hourly['hour_dt'].dt.weekday
    node_hourly['month'] = node_hourly['hour_dt'].dt.month

    print(f"\nFinal dataset: {len(node_hourly):,} node-hour observations")
    print(f"  Nodes: {node_hourly['settlement_point'].nunique()}")
    print(f"  Hours: {node_hourly['hour_dt'].min()} to {node_hourly['hour_dt'].max()}")

    # Report error coverage
    for lead in [lead_short, lead_long]:
        col = f'temp_error_{lead}h'
        if col in node_hourly.columns:
            n = node_hourly[col].notna().sum()
            pct = 100 * n / len(node_hourly)
            print(f"  {col} non-missing: {n:,} ({pct:.1f}%)")

    for col in ['forecast_load_1h', 'forecast_load_18h', 'load_error_1h', 'load_error_18h']:
        if col in node_hourly.columns:
            n = node_hourly[col].notna().sum()
            pct = 100 * n / len(node_hourly)
            print(f"  {col} non-missing: {n:,} ({pct:.1f}%)")

    print(f"Saving to {cache_file}")
    node_hourly.to_csv(cache_file, index=False)

    return node_hourly
