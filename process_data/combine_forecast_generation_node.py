"""Combine ERA5 gridded forecast errors, generation map, and system LMP.

Builds a single analysis-ready DataFrame at the (pixel, hour) level by merging:
1. ERA5 gridded forecast errors (NetCDF) — time-varying, 4D
   Supports loading from multiple models (e.g., HRRR 1h + GFS day-ahead).
2. Gridded generation map (NetCDF) — static, 2D
3. RT SPP system-wide hourly LMP statistics — time-varying, 1D

Output: {processed}/pixel_hourly_{models_key}_{year}_{month:02d}.parquet
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
from process_data.process_ercot import load_rt_spp_month
from process_data.gridded_generation_mapping import build_gridded_generation_map

# Default model → lead-hours mapping.  Used when callers pass models=None.
MODEL_LEAD_TIMES = {'hrrr': (1,), 'gfs': (0,)}


def _round_coord_id(lat, lon):
    """Build a pixel ID from rounded lat/lon for grid-safe merging."""
    return f"{lat:.1f}_{lon:.1f}"


def flatten_era5_errors(year, month, model='hrrr'):
    """Convert ERA5 gridded forecast errors NetCDF to a wide-format DataFrame.

    One row per (pixel_id, valid_time). Lead hours become column suffixes
    (e.g., temp_error_1h, temp_error_18h).

    Args:
        year: Integer year.
        month: Integer month.
        model: Forecast model — 'hrrr' or 'gfs'.

    Returns:
        DataFrame with columns: pixel_id, latitude, longitude, valid_time,
        {var}_{lead}h columns, era5_temp, era5_wspd, era5_wdir.
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

    Keeps only pixels with any infrastructure (generation, transmission,
    or load center).

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

    n_total = len(gen_df)

    # Keep only pixels with any infrastructure
    infra_mask = (
        (gen_df['total_capacity_mw'] > 0) |
        (gen_df['has_transmission_line'] == 1) |
        (gen_df['load_center'] == 1)
    )
    gen_df = gen_df[infra_mask].copy()

    print(f"  Generation map: {len(gen_df)} infrastructure pixels "
          f"(out of {n_total} total)")
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
    """Build the combined pixel x hour analysis DataFrame.

    Merges ERA5 gridded forecast errors from one or more forecast models,
    the gridded generation map, and system-wide hourly LMP statistics.
    Only pixels with infrastructure (generation, transmission, or load
    centers) are kept.

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

    print("Step 2: Flattening generation map...")
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
        how='inner',
    )
    print(f"  After generation merge: {len(pixel_hourly):,} rows "
          f"({pixel_hourly['pixel_id'].nunique()} pixels)")

    pixel_hourly = pixel_hourly.merge(
        system_lmp,
        on='valid_time',
        how='left',
    )

    # ── Step 5: Merge congestion metrics (if shadow data exists) ──
    try:
        from process_data.process_congestion import merge_congestion_system
        print("Step 5: Merging congestion metrics...")
        pixel_hourly = merge_congestion_system(pixel_hourly, year, month)
        print(f"  Added congestion columns: "
              f"n_binding_constraints, total_shadow_cost, etc.")
    except FileNotFoundError:
        print("Step 5: Shadow price data not found — skipping congestion merge.")

    # Add time features
    pixel_hourly['hour_of_day'] = pixel_hourly['valid_time'].dt.hour
    pixel_hourly['day_of_month'] = pixel_hourly['valid_time'].dt.day
    pixel_hourly['weekday'] = pixel_hourly['valid_time'].dt.weekday
    pixel_hourly['month'] = pixel_hourly['valid_time'].dt.month

    pixel_hourly.to_parquet(cache_file, index=False)
    print(f"\nSaved: {cache_file}")
    print(f"  Shape: {pixel_hourly.shape}")
    print(f"  Pixels: {pixel_hourly['pixel_id'].nunique()}")
    print(f"  Hours: {pixel_hourly['valid_time'].nunique()}")

    return pixel_hourly


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Build pixel x hour analysis dataset'
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
