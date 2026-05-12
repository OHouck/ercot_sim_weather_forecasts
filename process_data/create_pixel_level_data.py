"""Build the ERA5 pixel × hour analysis dataset for all Texas grid cells.

Merges, for every ERA5 0.1° pixel whose centre lies inside the Texas state
boundary:
1. ERA5 gridded forecast errors (NetCDF) — time-varying, 4D
   Supports loading from multiple models (e.g., HRRR 1h + GFS day-ahead).
2. Gridded generation map (NetCDF) — static, 2D (0 for non-infrastructure pixels)
3. RT SPP system-wide hourly LMP statistics — time-varying, 1D
4. Weather-zone load actuals and forecasts — time-varying, zone-level

This intentionally includes all Texas pixels, not just those with generation
or transmission infrastructure, so that spatial regression analyses can map
forecast-error effects across the full state.

Output: {processed}/combined_hourly_gridded_data/pixel_hourly_{models_key}_{year}_{month:02d}.parquet
"""

import argparse
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

sys.path.insert(0, str(Path(__file__).parent.parent))
from helper_funcs import setup_directories
from process_data.process_ercot import (
    load_rt_spp_month,
    process_generation_mix,
    compute_generation_emissions,
)
from process_data.gridded_generation_mapping import build_gridded_generation_map

# Default model → lead-hours mapping.  Used when callers pass models=None.
MODEL_LEAD_TIMES = {'hrrr': (1,), 'gfs': (0,)}


def _first_per_hour(df):
    """Aggregate a 15-minute DataFrame to hourly by taking the first reading per hour.

    Args:
        df: DataFrame with a 'time' column at sub-hourly resolution.

    Returns:
        DataFrame with a 'valid_time' column (floored to the hour) replacing 'time',
        one row per hour (first interval of each hour retained).
    """
    return (
        df.assign(valid_time=df['time'].dt.floor('h'))
        .groupby('valid_time', as_index=False)
        .first()
        .drop(columns='time')
    )


def _round_coord_id(lat, lon):
    """Build a pixel ID from rounded lat/lon for grid-safe merging."""
    return f"{lat:.1f}_{lon:.1f}"


def _get_texas_pixel_ids(nc_path):
    """Return the set of pixel IDs whose ERA5 grid centres lie inside Texas.

    Reads the lat/lon grid directly from the ERA5 NetCDF (guaranteed clean,
    non-NaN coordinates) rather than relying on columns in a merged DataFrame
    that may contain NaN from an outer join.  Tests every grid cell — not just
    land pixels — so that non-land-mask cells near the coast are not excluded.

    Args:
        nc_path: Path to any ERA5 error NetCDF (used only for its grid).

    Returns:
        set of pixel_id strings inside the Texas state boundary.
    """
    import shapely
    import cartopy.io.shapereader as shpreader

    states_shp = shpreader.natural_earth(
        resolution='10m', category='cultural', name='admin_1_states_provinces'
    )
    texas_geom = None
    for record in shpreader.Reader(states_shp).records():
        if record.attributes.get('name') == 'Texas':
            texas_geom = record.geometry
            break
    if texas_geom is None:
        raise ValueError("Texas geometry not found in Natural Earth data.")

    ds = xr.open_dataset(nc_path)
    lats = ds.latitude.values
    lons = ds.longitude.values
    ds.close()

    lat_grid, lon_grid = np.meshgrid(lats, lons, indexing='ij')
    points = shapely.points(lon_grid.ravel(), lat_grid.ravel())
    in_texas = shapely.within(points, texas_geom).reshape(len(lats), len(lons))

    tx_lat_idx, tx_lon_idx = np.where(in_texas)
    return set(
        _round_coord_id(lats[li], lons[lo])
        for li, lo in zip(tx_lat_idx, tx_lon_idx)
    )


def flatten_era5_errors(year, month, model='hrrr'):
    """Convert ERA5 gridded forecast errors NetCDF to a wide-format DataFrame.

    One row per (pixel_id, valid_time). Lead hours become column suffixes
    (e.g., temp_error_1h, temp_error_18h). 100m wind columns are included
    when present in the NetCDF (wspd100_error_{lead}h, wdir100_error_{lead}h,
    forecast_wspd100_{lead}h, forecast_wdir100_{lead}h, era5_wspd100, era5_wdir100).

    Args:
        year: Integer year.
        month: Integer month.
        model: Forecast model — 'hrrr' or 'gfs'.

    Returns:
        DataFrame with columns: pixel_id, latitude, longitude, valid_time,
        {var}_{lead}h columns, era5_temp, era5_wspd, era5_wdir,
        and optionally era5_wspd100, era5_wdir100.
    """
    dirs = setup_directories()
    nc_path = os.path.join(
        dirs['processed'], 'forecast_errors_era5', model,
        str(year), f'{month:02d}', f'era5_errors_{year}{month:02d}.nc'
    )
    if not os.path.exists(nc_path):
        raise FileNotFoundError(
            f"ERA5 error file not found: {nc_path}\n"
            f"Run calculate_era5_errors_for_month({year}, {month}, "
            f"model='{model}') first."
        )

    ds = xr.open_dataset(nc_path)
    ds['valid_time'] = pd.to_datetime(ds['valid_time'].values)

    lats = ds.latitude.values
    lons = ds.longitude.values
    valid_times = pd.to_datetime(ds.valid_time.values)
    lead_hours_vals = ds.lead_hours.values
    n_times = len(valid_times)

    # Land mask: pixel has at least one non-NaN temp_error across all times/leads
    has_data = ~np.all(np.isnan(ds['temp_error'].values), axis=(0, 1))
    land_lat_idx, land_lon_idx = np.where(has_data)
    n_land = len(land_lat_idx)

    pixel_ids = np.array([
        _round_coord_id(lats[li], lons[lo])
        for li, lo in zip(land_lat_idx, land_lon_idx)
    ])
    pixel_lats = lats[land_lat_idx]
    pixel_lons = lons[land_lon_idx]

    print(f"  ERA5 errors: {n_land} land pixels, {n_times} hours, "
          f"{len(lead_hours_vals)} leads")

    lead_short = int(lead_hours_vals[0])
    has_100m = 'wspd100_error' in ds.data_vars

    # Build a DataFrame per lead, then merge
    dfs_by_lead = {}
    for li, lead in enumerate(lead_hours_vals):
        lead_int = int(lead)

        # Extract (n_times, n_land) slices at land cells
        temp_err = ds['temp_error'].values[:, li, land_lat_idx, land_lon_idx]
        wspd_err = ds['wspd_error'].values[:, li, land_lat_idx, land_lon_idx]
        wdir_err = ds['wdir_error'].values[:, li, land_lat_idx, land_lon_idx]
        fc_temp = ds['forecast_temp'].values[:, li, land_lat_idx, land_lon_idx]
        fc_wspd = ds['forecast_wspd'].values[:, li, land_lat_idx, land_lon_idx]
        fc_wdir = ds['forecast_wdir'].values[:, li, land_lat_idx, land_lon_idx]

        hours = np.repeat(valid_times, n_land)
        pids = np.tile(pixel_ids, n_times)

        df = pd.DataFrame({
            'pixel_id': pids,
            'valid_time': pd.to_datetime(hours).floor('h'),
            f'temp_error_{lead_int}h': temp_err.ravel(),
            f'wspd_error_{lead_int}h': wspd_err.ravel(),
            f'wdir_error_{lead_int}h': wdir_err.ravel(),
            f'forecast_temp_{lead_int}h': fc_temp.ravel(),
            f'forecast_wspd_{lead_int}h': fc_wspd.ravel(),
            f'forecast_wdir_{lead_int}h': fc_wdir.ravel(),
        })

        if has_100m:
            df[f'wspd100_error_{lead_int}h'] = ds['wspd100_error'].values[:, li, land_lat_idx, land_lon_idx].ravel()
            df[f'wdir100_error_{lead_int}h'] = ds['wdir100_error'].values[:, li, land_lat_idx, land_lon_idx].ravel()
            df[f'forecast_wspd100_{lead_int}h'] = ds['forecast_wspd100'].values[:, li, land_lat_idx, land_lon_idx].ravel()
            df[f'forecast_wdir100_{lead_int}h'] = ds['forecast_wdir100'].values[:, li, land_lat_idx, land_lon_idx].ravel()

        # Observed ERA5 values are identical across leads; add once
        if lead_int == lead_short:
            era5_temp = ds['era5_temp'].values[:, li, land_lat_idx, land_lon_idx]
            era5_wspd = ds['era5_wspd'].values[:, li, land_lat_idx, land_lon_idx]
            era5_wdir = ds['era5_wdir'].values[:, li, land_lat_idx, land_lon_idx]
            df['era5_temp'] = era5_temp.ravel()
            df['era5_wspd'] = era5_wspd.ravel()
            df['era5_wdir'] = era5_wdir.ravel()
            df['latitude'] = np.tile(pixel_lats, n_times)
            df['longitude'] = np.tile(pixel_lons, n_times)
            if has_100m:
                df['era5_wspd100'] = ds['era5_wspd100'].values[:, li, land_lat_idx, land_lon_idx].ravel()
                df['era5_wdir100'] = ds['era5_wdir100'].values[:, li, land_lat_idx, land_lon_idx].ravel()

        dfs_by_lead[lead_int] = df

    ds.close()

    # Merge leads on (pixel_id, valid_time)
    lead_list = [int(l) for l in lead_hours_vals]
    errors_wide = dfs_by_lead[lead_list[0]]
    for lead_int in lead_list[1:]:
        errors_wide = errors_wide.merge(
            dfs_by_lead[lead_int],
            on=['pixel_id', 'valid_time'],
            how='outer',
        )

    print(f"  Flattened ERA5 errors: {len(errors_wide):,} rows")
    return errors_wide


def flatten_generation_map():
    """Convert gridded generation map NetCDF to a pixel-level DataFrame.

    Returns ALL ERA5 grid cells (not filtered to infrastructure pixels).
    Cells without generation or infrastructure will have 0s for capacity
    columns, and 0 for has_transmission_line and load_center.

    Returns:
        DataFrame with columns: pixel_id, latitude, longitude,
        total_capacity_mw, n_generators, has_transmission_line, load_center,
        nameplate_mw_tech_* columns.
    """
    gen_ds = build_gridded_generation_map()
    lats = gen_ds.latitude.values
    lons = gen_ds.longitude.values

    # Build DataFrame manually from 2D arrays (to_dataframe can misbehave)
    lat_grid, lon_grid = np.meshgrid(lats, lons, indexing='ij')
    gen_df = pd.DataFrame({
        'latitude': lat_grid.ravel(),
        'longitude': lon_grid.ravel(),
    })
    for var_name in gen_ds.data_vars:
        gen_df[var_name] = gen_ds[var_name].values.ravel()

    gen_df['pixel_id'] = [
        _round_coord_id(lat, lon)
        for lat, lon in zip(gen_df['latitude'], gen_df['longitude'])
    ]

    n_infra = (
        (gen_df['total_capacity_mw'] > 0) |
        (gen_df['has_transmission_line'] == 1) |
        (gen_df['load_center'] == 1)
    ).sum()
    print(f"  Generation map: {len(gen_df)} total ERA5 grid cells "
          f"({n_infra} with infrastructure)")
    return gen_df


def compute_system_lmp_hourly(year, month):
    """Compute system-wide hourly LMP statistics from RT SPP data.

    First averages intervals within each (node, hour), then computes
    mean/max/std across all RN settlement points per hour.

    Args:
        year: Integer year.
        month: Integer month.

    Returns:
        DataFrame with columns: valid_time, system_lmp_mean, system_lmp_max,
        system_lmp_std.
    """
    rt_spp = load_rt_spp_month(year, month)
    rt_spp = rt_spp[rt_spp['settlementPointType'] == 'RN'].copy()

    rt_spp['deliveryDate'] = pd.to_datetime(rt_spp['deliveryDate'])
    rt_spp['valid_time'] = rt_spp['deliveryDate'] + pd.to_timedelta(
        rt_spp['deliveryHour'] - 1, unit='h'
    )

    # Average intervals within each (node, hour)
    node_hourly = (
        rt_spp
        .groupby(['settlementPoint', 'valid_time'])['settlementPointPrice']
        .mean()
        .reset_index()
        .rename(columns={'settlementPointPrice': 'lmp'})
    )

    # System-wide stats across all nodes per hour
    system_lmp = (
        node_hourly
        .groupby('valid_time')['lmp']
        .agg(
            system_lmp_mean='mean',
            system_lmp_max='max',
            system_lmp_std='std',
        )
        .reset_index()
    )
    system_lmp['system_lmp_std'] = system_lmp['system_lmp_std'].fillna(0)

    print(f"  System LMP: {len(system_lmp)} hours, "
          f"mean={system_lmp['system_lmp_mean'].mean():.2f} $/MWh")
    return system_lmp


def build_pixel_hourly_dataset(year, month, models=None, force_rebuild=False):
    """Build the Texas pixel × hour analysis DataFrame.

    For every ERA5 0.1° pixel inside the Texas state boundary, merges:
    - ERA5 gridded forecast errors from one or more forecast models
    - Gridded generation map (0s for pixels without infrastructure)
    - System-wide hourly LMP statistics
    - Congestion metrics (if shadow data exists)
    - Curtailment metrics (if SCED disclosure data exists)
    - Weather-zone load actuals and forecasts

    Args:
        year: Integer year.
        month: Integer month.
        models: Dict mapping model name → tuple of lead hours, e.g.
                ``{'hrrr': (1,), 'gfs': (0,)}``.  Defaults to combined
                HRRR 1h + GFS day-ahead when ``None``.
        force_rebuild: If True, rebuild even if cached parquet exists.

    Returns:
        DataFrame with one row per (pixel_id, valid_time).
    """
    if models is None:
        models = dict(MODEL_LEAD_TIMES)

    models_key = '+'.join(sorted(models.keys()))

    dirs = setup_directories()
    cache_file = os.path.join(
        dirs['processed'], "combined_hourly_gridded_data",
        f'pixel_hourly_{models_key}_{year}_{month:02d}.parquet',
    )

    if os.path.exists(cache_file) and not force_rebuild:
        print(f"Loading cached dataset: {cache_file}")
        return pd.read_parquet(cache_file)

    # ── Step 1: Flatten ERA5 forecast errors for each model and merge ──
    print("Step 1: Flattening ERA5 forecast errors...")

    # Build Texas pixel ID set from the raw ERA5 grid before flattening.
    # We do this once using the first available NC file's clean coordinate arrays
    # rather than relying on lat/lon columns in the merged DataFrame, which can
    # contain NaN for rows that only appear in one model after the outer join.
    dirs = setup_directories()
    first_model = sorted(models.keys())[0]
    first_nc = os.path.join(
        dirs['processed'], 'forecast_errors_era5', first_model,
        str(year), f'{month:02d}', f'era5_errors_{year}{month:02d}.nc'
    )
    print("  Building Texas pixel mask from ERA5 grid...")
    texas_pixel_ids = _get_texas_pixel_ids(first_nc)
    print(f"  {len(texas_pixel_ids):,} ERA5 grid cells inside Texas boundary")

    errors_df = None
    for model_name in sorted(models.keys()):
        print(f"  Loading {model_name.upper()} errors...")
        model_errors = flatten_era5_errors(year, month, model_name)
        if errors_df is None:
            errors_df = model_errors
        else:
            # Drop columns already present in the base (era5_*, lat, lon)
            dup_cols = [c for c in model_errors.columns
                        if c in errors_df.columns
                        and c not in ('pixel_id', 'valid_time')]
            model_errors = model_errors.drop(columns=dup_cols)
            errors_df = errors_df.merge(
                model_errors,
                on=['pixel_id', 'valid_time'],
                how='outer',
            )
    print(f"  Combined ERA5 errors: {len(errors_df):,} rows")

    # Filter to Texas pixels
    errors_df = errors_df[errors_df['pixel_id'].isin(texas_pixel_ids)].copy()
    print(f"  Texas pixels: {errors_df['pixel_id'].nunique():,} pixels, "
          f"{len(errors_df):,} rows")

    print("Step 2: Loading generation map...")
    gen_df = flatten_generation_map()

    print("Step 3: Computing system-wide hourly LMP...")
    system_lmp = compute_system_lmp_hourly(year, month)

    # Drop lat/lon from gen_df before merge (errors_df already has them)
    gen_merge_cols = [c for c in gen_df.columns
                      if c not in ('latitude', 'longitude')]
    print("Step 4: Merging datasets...")
    pixel_hourly = errors_df.merge(
        gen_df[gen_merge_cols],
        on='pixel_id',
        how='left',
    )
    # Non-infrastructure pixels get NaN from the left join; fill gen columns with 0
    gen_fill_cols = [c for c in gen_merge_cols
                     if c not in ('pixel_id',) and c in pixel_hourly.columns]
    pixel_hourly[gen_fill_cols] = pixel_hourly[gen_fill_cols].fillna(0)

    print(f"  After generation merge: {len(pixel_hourly):,} rows "
          f"({pixel_hourly['pixel_id'].nunique()} pixels)")

    pixel_hourly = pixel_hourly.merge(
        system_lmp,
        on='valid_time',
        how='left',
    )

    # ── Step 5: Merge congestion metrics ──
    try:
        from process_data.process_congestion import merge_congestion_system
        print("Step 5: Merging congestion metrics...")
        pixel_hourly = merge_congestion_system(pixel_hourly, year, month)
        print(f"  Added congestion columns: "
              f"economic_congestion_cost, zone_lmp_spread_mw, system_lambda")
    except FileNotFoundError:
        print("Step 5: Congestion data not found — skipping congestion merge.")

    # ── Step 6: Merge curtailment metrics (if SCED disclosure data exists) ──
    try:
        from process_data.process_curtailment import merge_curtailment_system
        print("Step 6: Merging curtailment metrics...")
        pixel_hourly = merge_curtailment_system(pixel_hourly, year, month)
        print(f"  Added curtailment columns: "
              f"wind_curtailment_mw, solar_curtailment_mw, etc.")
    except FileNotFoundError:
        print("Step 6: SCED disclosure data not found — skipping curtailment merge.")

    # ── Step 6b: Merge generation mix + emissions intensity ──
    try:
        print("Step 6b: Merging generation mix and emissions data...")
        gen_mix = process_generation_mix(year, month)
        emissions = compute_generation_emissions(gen_mix)

        gen_hourly = _first_per_hour(gen_mix)
        em_hourly = _first_per_hour(emissions)

        pixel_hourly = (
            pixel_hourly
            .merge(gen_hourly, on='valid_time', how='left')
            .merge(em_hourly, on='valid_time', how='left')
        )
        fuel_cols = [c for c in gen_hourly.columns if c != 'valid_time']
        print(f"  Added generation mix columns: {fuel_cols}")
        print(f"  Added emissions columns: total_generation_mw, total_co2_rate_kg_per_h, avg_intensity_kg_per_mwh")
    except FileNotFoundError:
        print("Step 6b: Generation mix file not found — skipping generation mix merge.")

    # ── Step 7: Merge weather-zone load data ──
    try:
        from process_data.calculate_load_error import merge_load_by_weather_zone
        print("Step 7: Merging weather-zone load data...")
        pixel_hourly = merge_load_by_weather_zone(
            pixel_hourly,
            months=[(year, month)],
            time_col='valid_time',
            lat_col='latitude',
            lon_col='longitude',
        )
        load_cols = [c for c in pixel_hourly.columns
                     if c.startswith('actual_load') or c.startswith('forecast_load_')
                     or c.startswith('load_error_')]
        print(f"  Added load columns: {load_cols}")
    except FileNotFoundError:
        print("Step 7: Load data not found — skipping load merge.")

    # Add time features
    pixel_hourly['hour_of_day'] = pixel_hourly['valid_time'].dt.hour
    pixel_hourly['day_of_month'] = pixel_hourly['valid_time'].dt.day
    pixel_hourly['weekday'] = pixel_hourly['valid_time'].dt.weekday
    pixel_hourly['month'] = pixel_hourly['valid_time'].dt.month
    pixel_hourly['is_weekend'] = (pixel_hourly['weekday'] >= 5).astype(int)

    pixel_hourly.to_parquet(cache_file, index=False)
    print(f"\nSaved: {cache_file}")
    print(f"  Shape: {pixel_hourly.shape}")
    print(f"  Pixels: {pixel_hourly['pixel_id'].nunique()}")
    print(f"  Hours: {pixel_hourly['valid_time'].nunique()}")

    return pixel_hourly


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Build Texas pixel × hour analysis dataset'
    )
    parser.add_argument('--year', type=int, default=2025)
    parser.add_argument('--month', type=int, default=7)
    parser.add_argument(
        '--models', type=str, default=None,
        help='Comma-separated model names (e.g. "hrrr,gfs"). '
             'Defaults to all models in MODEL_LEAD_TIMES.',
    )
    parser.add_argument('--force-rebuild', action='store_true')
    args = parser.parse_args()

    if args.models is not None:
        models = {m.strip(): MODEL_LEAD_TIMES[m.strip()]
                  for m in args.models.split(',')}
    else:
        models = None  # use default (all models)

    build_pixel_hourly_dataset(
        args.year, args.month, models, args.force_rebuild
    )
