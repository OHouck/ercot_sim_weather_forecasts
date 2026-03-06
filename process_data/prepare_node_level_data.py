import os
import glob
import pandas as pd
import numpy as np
import geopandas as gpd

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from helper_funcs import setup_directories
from process_data.process_ercot import load_rt_spp_month


def prepare_node_level_data(months, model='ndfd', force_rebuild=False):
    """
    Build a node × hour dataset linking ERCOT LMP to weather forecast errors.

    Each ERCOT resource node is spatially matched to its nearest ISD weather
    station using geopandas sjoin_nearest (projected to EPSG:3857 for accurate
    distance calculation). The station's short-lead and long-lead forecast
    errors are attached as separate columns, along with the distance to that
    station.

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

    cache_file = os.path.join(
        dirs['processed'],
        f'node_hourly_{model}_{cache_tag}.csv'
    )

    if os.path.exists(cache_file) and not force_rebuild:
        print(f"Loading cached node-level data from {cache_file}")
        return pd.read_csv(cache_file, parse_dates=['hour'])

    period_str = ", ".join(f"{y}-{m:02d}" for y, m in months)
    print(f"Building node-level dataset from scratch (model={model}, months={period_str})...")

    # ── Step 1: Load all forecast errors and pivot by lead time ──
    print("Loading forecast errors...")
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
    # Keep one row per (station_id, hour) with columns for each lead time
    # Dynamically picks up all data columns from calculate_forecast_errors.py
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

    # ── Step 2: Build station GeoDataFrame (unique lat/lon per station) ──
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

    # ── Step 3: Load node coordinates and build node GeoDataFrame ──
    print("Loading node coordinates...")
    node_coords = pd.read_csv(os.path.join(dirs['processed'], 'node_coordinates.csv'))
    node_coords = node_coords.dropna(subset=['lat', 'lon'])

    nodes_gdf = gpd.GeoDataFrame(
        node_coords,
        geometry=gpd.points_from_xy(node_coords['lon'], node_coords['lat']),
        crs='EPSG:4326'
    ).to_crs('EPSG:3857')

    print(f"  {len(nodes_gdf)} nodes with coordinates")

    # ── Step 4: Spatial join — each node to its nearest station ──
    print("Joining each node to nearest weather station...")
    node_station = gpd.sjoin_nearest(
        nodes_gdf[['settlement_point', 'lat', 'lon', 'geometry']],
        stations_gdf[['station_id', 'geometry']],
        how='left',
        distance_col='dist_m'
    )
    # sjoin_nearest can return duplicates when multiple stations are equidistant;
    # keep the first match per node
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

    # ── Step 5: Load and aggregate RT SPP prices to hourly ──
    print("Loading RT SPP prices...")
    rt_spp_dfs = []
    for year, month in months:
        rt_spp_dfs.append(load_rt_spp_month(year, month))
    rt_spp = pd.concat(rt_spp_dfs, ignore_index=True)
    # Filter to Resource Node (RN) settlement points only.
    rt_spp = rt_spp[rt_spp['settlementPointType'] == 'RN'].copy()

    rt_spp['deliveryDate'] = pd.to_datetime(rt_spp['deliveryDate'])
    # deliveryHour is 1-24; deliveryInterval is 1-4 (15-min within hour)
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

    # ── Step 6: Merge prices with node→station mapping ──
    print("Merging prices with node-station mapping...")
    price_with_station = price_hourly.merge(
        node_station[['settlement_point', 'lat', 'lon', 'station_id', 'dist_km']],
        on='settlement_point',
        how='inner'   # only nodes that have coordinates
    )
    print(f"  {price_with_station['settlement_point'].nunique()} nodes with prices + coords")

    # ── Step 7: Attach forecast errors ──
    print("Attaching forecast errors...")
    node_hourly = price_with_station.merge(
        errors_wide.drop(columns=['lat', 'lon']),
        on=['station_id', 'hour'],
        how='left'
    )

    # Time features
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
        n = node_hourly[col].notna().sum()
        pct = 100 * n / len(node_hourly)
        print(f"  {col} non-missing: {n:,} ({pct:.1f}%)")

    print(f"Saving to {cache_file}")
    node_hourly.to_csv(cache_file, index=False)

    return node_hourly
