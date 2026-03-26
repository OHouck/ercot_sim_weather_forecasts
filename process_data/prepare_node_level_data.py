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
from process_data.process_ercot import load_rt_spp_month


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
    """Build weather-zone load actuals, forecasts, and errors.

    Delegates to process_data.calculate_load_error.build_load_snapshot() for
    the actual computation. Returns a DataFrame with columns:
        weather_zone, hour, actual_load,
        forecast_load_1h, load_error_1h,
        forecast_load_dam, load_error_dam
    """
    from process_data.calculate_load_error import build_load_snapshot
    return build_load_snapshot(months)


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


def _load_era5_errors_for_nodes(months, model, dirs, node_coords, leads):
    """Load ERA5 gridded forecast errors and extract values at node coordinates.

    Uses xr.sel(method='nearest') to find the nearest ERA5 grid cell for each
    ERCOT node — no GeoDataFrame sjoin needed since ERA5 has a regular grid.

    Args:
        months: List of (year, month) tuples.
        model: 'hrrr' or 'gfs'.
        dirs: dict from setup_directories().
        node_coords: DataFrame with settlement_point, lat, lon.
        leads: Tuple of lead hours to process (e.g. (1,) or (0,)).

    Returns:
        DataFrame in wide format with one row per (settlement_point, hour),
        columns matching the station-based pivot format.
    """
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
        if lead_int not in leads:
            continue  # skip leads not in our target set
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

        # Add observed columns only from the first lead (identical across leads)
        if lead_int == leads[0]:
            df['observed_temp'] = sub['era5_temp'].values.ravel()
            df['observed_wspd'] = sub['era5_wspd'].values.ravel()
            df['observed_wdir'] = sub['era5_wdir'].values.ravel()

        dfs_by_lead[lead_int] = df

    # Merge leads on (settlement_point, hour)
    lead_list = [l for l in leads if l in dfs_by_lead]
    errors_wide = dfs_by_lead[lead_list[0]]
    for lead_int in lead_list[1:]:
        errors_wide = errors_wide.merge(
            dfs_by_lead[lead_int].drop(columns=['lat', 'lon']),
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
    models=None,
    force_rebuild=False,
    weather_zone_shapefile=DEFAULT_WEATHER_ZONE_SHP,
    error_source='station',
    model=None,  # backward compat: single model name (str) → converted to models dict
):
    """
    Build a node × hour dataset linking ERCOT LMP to weather forecast errors.

    Supports two error sources:
      - 'station': Each node is matched to its nearest ISD weather station via
        sjoin_nearest. Station-level forecast errors from per-station CSVs.
      - 'era5': Each node is matched to its nearest ERA5 grid cell via
        xr.sel(method='nearest'). No GeoDataFrame construction needed.

    Supports loading forecast errors from multiple models simultaneously
    (e.g. HRRR 1h + GFS day-ahead).  Each model contributes columns with
    its own lead-hour suffix (``temp_error_1h`` from HRRR, ``temp_error_0h``
    from GFS), so columns never collide.

    Args:
        months: List of (year, month) tuples to include, e.g. [(2025, 1), (2025, 7)].
                Can also be a single tuple (year, month) for backwards compatibility.
        models: Dict mapping model name → tuple of lead hours, e.g.
                ``{'hrrr': (1,), 'gfs': (0,)}``.  Defaults to combined
                HRRR 1h + GFS day-ahead when ``None``.
        force_rebuild: If True, rebuild even if cached file exists
        weather_zone_shapefile: Path to ERCOT weather-zone shapefile
        error_source: 'station' (default) or 'era5'. Determines how forecast
            errors are loaded and spatially matched to nodes.

    Returns:
        DataFrame with one row per (settlement_point, hour) and columns for
        LMP, forecast errors, observed weather, and station distance.
    """
    # Accept a single (year, month) tuple for convenience
    if isinstance(months, tuple) and len(months) == 2 and isinstance(months[0], int):
        months = [months]

    # Model-specific lead times
    MODEL_LEAD_TIMES = {'hrrr': (1,), 'gfs': (0,)}

    # Backward compat: accept model='hrrr' → models={'hrrr': (1,)}
    if model is not None and models is None:
        models = {model: MODEL_LEAD_TIMES[model]}
    elif models is None:
        models = dict(MODEL_LEAD_TIMES)
    for m in models:
        if m not in MODEL_LEAD_TIMES:
            raise ValueError(f"Unknown model '{m}'. Choose from: {list(MODEL_LEAD_TIMES)}")

    # Combined leads across all models (e.g. (1, 0) for HRRR+GFS)
    all_leads = tuple(lead for model_leads in models.values() for lead in model_leads)
    models_key = '+'.join(sorted(models.keys()))

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
        f'node_hourly_{models_key}{source_tag}_{cache_tag}.csv'
    )

    if os.path.exists(cache_file) and not force_rebuild:
        print(f"Loading cached node-level data from {cache_file}")
        return pd.read_csv(cache_file, parse_dates=['hour'])

    period_str = ", ".join(f"{y}-{m:02d}" for y, m in months)
    print(f"Building node-level dataset from scratch (models={models_key}, months={period_str})...")

    # ── Load node coordinates ──
    print("Loading node coordinates...")
    node_coords = pd.read_csv(os.path.join(dirs['processed'], 'node_coordinates.csv'))
    node_coords = node_coords.dropna(subset=['lat', 'lon'])
    print(f"  {len(node_coords)} nodes with coordinates")

    if error_source == 'era5':
        # ── ERA5 path: load gridded errors directly, no station indirection ──
        print("Loading ERA5 gridded forecast errors...")
        errors_wide = None
        for model_name in sorted(models.keys()):
            model_leads = models[model_name]
            print(f"  Loading {model_name.upper()} errors (leads={model_leads})...")
            model_errors = _load_era5_errors_for_nodes(
                months, model_name, dirs, node_coords, model_leads,
            )
            if errors_wide is None:
                errors_wide = model_errors
            else:
                # Drop columns already present in the base (observed_*, lat, lon)
                dup_cols = [c for c in model_errors.columns
                            if c in errors_wide.columns
                            and c not in ('settlement_point', 'hour')]
                model_errors = model_errors.drop(columns=dup_cols)
                errors_wide = errors_wide.merge(
                    model_errors,
                    on=['settlement_point', 'hour'],
                    how='outer',
                )

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
        errors_wide = None
        for model_name in sorted(models.keys()):
            model_leads = models[model_name]
            all_error_files = []
            for year, month in months:
                forecast_error_dir = os.path.join(
                    dirs['processed'], 'forecast_errors', model_name, str(year), f"{month:02d}"
                )
                month_files = glob.glob(os.path.join(forecast_error_dir, '*.csv'))
                month_files = [f for f in month_files if not f.endswith('error_summary.csv')]
                all_error_files.extend(month_files)
                print(f"  {model_name.upper()} {year}-{month:02d}: {len(month_files)} station files")

            error_dfs = [pd.read_csv(f) for f in all_error_files]
            all_errors = pd.concat(error_dfs, ignore_index=True)
            all_errors['valid_time'] = pd.to_datetime(all_errors['valid_time'], format='mixed')
            all_errors['hour'] = all_errors['valid_time'].dt.floor('h')

            print(f"  Total: {len(all_errors):,} station-hour-lead observations "
                  f"({model_name.upper()}, {len(months)} months)")

            # Pivot lead_hours into separate columns
            error_cols = [c for c in all_errors.columns
                          if c not in ('station_id', 'valid_time', 'lead_hours',
                                       'hour', 'lat', 'lon')]

            # Build a wide DataFrame for each lead, then merge
            lead_dfs = {}
            for lead in model_leads:
                suffix = f'_{lead}h'
                lead_df = all_errors[all_errors['lead_hours'] == lead].copy()
                rename_map = {c: f'{c}{suffix}' for c in error_cols}
                lead_df = lead_df.rename(columns=rename_map)
                keep = ['station_id', 'hour', 'lat', 'lon'] + list(rename_map.values())
                lead_dfs[lead] = lead_df[keep]

            lead_list = list(model_leads)
            model_wide = lead_dfs[lead_list[0]]
            for lead in lead_list[1:]:
                model_wide = model_wide.merge(
                    lead_dfs[lead].drop(columns=['lat', 'lon']),
                    on=['station_id', 'hour'],
                    how='outer',
                )

            # Deduplicate observed columns within this model (identical across leads)
            if len(model_leads) > 1:
                obs_base_cols = [c for c in error_cols if c.startswith('observed_')]
                first_suffix = f'_{lead_list[0]}h'
                for base in obs_base_cols:
                    col_first = f'{base}{first_suffix}'
                    if col_first not in model_wide.columns:
                        continue
                    for lead in lead_list[1:]:
                        col_other = f'{base}_{lead}h'
                        if col_other not in model_wide.columns:
                            continue
                        model_wide[col_first] = model_wide[col_first].fillna(model_wide[col_other])
                        model_wide = model_wide.drop(columns=[col_other])
                    model_wide = model_wide.rename(columns={col_first: base})

            # Merge this model's errors into the combined DataFrame
            if errors_wide is None:
                errors_wide = model_wide
            else:
                # Drop columns already present in the base (observed_*, lat, lon)
                dup_cols = [c for c in model_wide.columns
                            if c in errors_wide.columns
                            and c not in ('station_id', 'hour')]
                model_wide = model_wide.drop(columns=dup_cols)
                errors_wide = errors_wide.merge(
                    model_wide,
                    on=['station_id', 'hour'],
                    how='outer',
                )

        # Consolidate observed columns across models: observed_temp_1h, observed_temp_0h → observed_temp
        # (Observed weather is identical regardless of forecast model/lead.)
        for base in ['observed_temp', 'observed_wspd', 'observed_wdir']:
            suffixed = [c for c in errors_wide.columns if c.startswith(base + '_')]
            if suffixed and base not in errors_wide.columns:
                errors_wide[base] = errors_wide[suffixed[0]]
                for col in suffixed[1:]:
                    errors_wide[base] = errors_wide[base].fillna(errors_wide[col])
                errors_wide = errors_wide.drop(columns=suffixed)

        print(f"  After pivot: {len(errors_wide):,} station-hour rows")

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
    for lead in all_leads:
        col = f'temp_error_{lead}h'
        if col in node_hourly.columns:
            n = node_hourly[col].notna().sum()
            pct = 100 * n / len(node_hourly)
            print(f"  {col} non-missing: {n:,} ({pct:.1f}%)")

    for col in ['forecast_load_1h', 'forecast_load_dam', 'load_error_1h', 'load_error_dam']:
        if col in node_hourly.columns:
            n = node_hourly[col].notna().sum()
            pct = 100 * n / len(node_hourly)
            print(f"  {col} non-missing: {n:,} ({pct:.1f}%)")

    print(f"Saving to {cache_file}")
    node_hourly.to_csv(cache_file, index=False)

    return node_hourly
