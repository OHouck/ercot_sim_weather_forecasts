"""Build the system-wide hourly outcome dataset for PCA analysis.

Creates one row per operating hour with all system-level outcome variables:

Prices / congestion
  economic_congestion_cost [$/h]   — Σ_z (LMP_z - λ) × Q_z
  rt_system_lambda [$/MWh]         — RT SCED system lambda 

Renewables
  wind_curtailment_mw              — system-wide wind curtailment
  solar_curtailment_mw             — system-wide solar curtailment
  total_renewable_curtailment_mw   — wind + solar curtailment

Time features
  hour_of_day, month, is_weekend

Generation / emissions
  total_generation_mw, total_co2_rate_kg_per_h, avg_intensity_kg_per_mwh

Thermal offer prices (HSL-weighted mean over in-market units, p85 of HSL)
  rt_cllig_p85, dam_cllig_p85, cllig_mc   — coal (CLLIG)
  rt_scgt_p85,  dam_scgt_p85,  scgt_mc    — SCGT (SCGT90 + SCLE90)
  rt_ccgt_p85,  dam_ccgt_p85,  ccgt_mc    — CCGT (CCGT90 + CCLE90)

RUC (from 60-Day SCED Disclosure Telemetered Resource Status)
  ruc_deployment_mw         — total output of ONRUC units (committed by RUC)
  ruc_optout_deployment_mw  — total output of ONOPTOUT units (opted out of RUC)

Output: {processed}/system_hourly_outcomes_{year}.csv
"""

import argparse
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from helper_funcs import setup_directories


# ---------------------------------------------------------------------------
# Private loaders
# ---------------------------------------------------------------------------

def _first_per_hour(df):
    """Aggregate a sub-hourly DataFrame to hourly by keeping the first record per hour.

    Args:
        df: DataFrame with a 'time' column at sub-hourly resolution.

    Returns:
        DataFrame with a 'valid_time' column (hour-floored) replacing 'time',
        one row per hour.
    """
    return (
        df.assign(valid_time=df['time'].dt.floor('h'))
        .groupby('valid_time', as_index=False)
        .first()
        .drop(columns='time')
    )


def _load_congestion(year, month):
    """Load hourly economic congestion cost for one month.

    Args:
        year: Integer year.
        month: Integer month.

    Returns:
        DataFrame with columns: valid_time, economic_congestion_cost.
    """
    from process_data.process_congestion import compute_economic_congestion_cost
    df = compute_economic_congestion_cost(year, month)
    return df[['valid_time', 'economic_congestion_cost']]


def _load_system_lambda_first(year, month):
    """Load SCED real-time system lambda, keeping the first observation per hour.

    Uses first-per-hour rather than mean to align with forecast error timing
    (forecast errors are measured at the start of each valid hour).

    Args:
        year: Integer year.
        month: Integer month.

    Returns:
        DataFrame with columns: valid_time, rt_system_lambda.
    """
    dirs = setup_directories()
    path = os.path.join(
        dirs['raw'], 'ercot', 'sced_lambda',
        str(year), f'{month:02d}',
        f'sced_lambda_{year}-{month:02d}.parquet',
    )
    if not os.path.exists(path):
        print(f"  SCED lambda file not found (skipping): {path}")
        return pd.DataFrame(columns=['valid_time', 'rt_system_lambda'])

    df = pd.read_parquet(path, columns=['sced_timestamp', 'system_lambda'])
    df['valid_time'] = (
        pd.to_datetime(df['sced_timestamp'], format='%m/%d/%Y %H:%M:%S')
        .dt.floor('h')
    )
    first = (
        df.sort_values('sced_timestamp')
        .groupby('valid_time', as_index=False)
        .first()[['valid_time', 'system_lambda']]
        .rename(columns={'system_lambda': 'rt_system_lambda'})
    )
    print(f"  SCED lambda: {len(first)} hours (first per hour), "
          f"mean={first['rt_system_lambda'].mean():.2f} $/MWh")
    return first


def _load_curtailment(year, month):
    """Load hourly renewable curtailment metrics for one month.

    Args:
        year: Integer year.
        month: Integer month.

    Returns:
        DataFrame with columns: valid_time, wind_curtailment_mw,
        solar_curtailment_mw, total_renewable_curtailment_mw.
    """
    from process_data.process_curtailment import compute_hourly_curtailment
    df = compute_hourly_curtailment(year, month)
    return df[['valid_time', 'wind_curtailment_mw', 'solar_curtailment_mw',
               'total_curtailment_mw']].rename(
        columns={'total_curtailment_mw': 'total_renewable_curtailment_mw'}
    )


def _load_generation_hourly(year, month):
    """Load generation mix and emissions aggregated to hourly for one month.

    Reads 15-minute generation mix, computes emissions, then keeps the first
    interval per hour (identical to the pattern in create_pixel_level_data).

    Args:
        year: Integer year.
        month: Integer month.

    Returns:
        DataFrame with columns: valid_time, total_generation_mw,
        total_co2_rate_kg_per_h, avg_intensity_kg_per_mwh.
    """
    from process_data.process_ercot import process_generation_mix, compute_generation_emissions
    gen_mix = process_generation_mix(year, month)
    emissions = compute_generation_emissions(gen_mix)
    em_hourly = _first_per_hour(emissions)
    return em_hourly[['valid_time', 'total_generation_mw',
                       'total_co2_rate_kg_per_h', 'avg_intensity_kg_per_mwh']]


def _load_ruc(year, month):
    """Load RUC and opt-out committed generation for one month.

    Reads the yearly per-SCED-timestep CSV produced by
    calculate_ruc_commitments.compute_ruc_commitments, filters to the requested
    month, and collapses each hour's ~12 SCED intervals to the first observation
    per hour (matching the rt_system_lambda convention). Returns an empty
    DataFrame with the expected schema if the file is missing.

    Args:
        year: Integer year.
        month: Integer month.

    Returns:
        DataFrame with columns: valid_time, ruc_deployment_mw,
        ruc_optout_deployment_mw.
    """
    cols = ['valid_time', 'ruc_deployment_mw', 'ruc_optout_deployment_mw']
    dirs = setup_directories()
    path = os.path.join(
        dirs['processed'], 'ruc_commitments', f'ruc_commitments_{year}.csv'
    )
    if not os.path.exists(path):
        print(f"  RUC commitments file not found (skipping): {path}")
        return pd.DataFrame(columns=cols)

    df = pd.read_csv(path, parse_dates=['sced_time_step'])
    df = df[df['sced_time_step'].dt.month == month].copy()
    if df.empty:
        return pd.DataFrame(columns=cols)

    df['valid_time'] = df['sced_time_step'].dt.floor('h')
    first = (
        df.sort_values('sced_time_step')
        .groupby('valid_time', as_index=False)
        .first()[['valid_time', 'ruc_deployment_mw', 'ruc_optout_deployment_mw']]
    )
    return first


def _hsl_weighted_mean(df, value_cols, weight_col='HSL'):
    """Compute HSL-weighted mean for one or more columns in a single grouped pass.

    Args:
        df: DataFrame with valid_time, weight_col, and all value_cols.
        value_cols: Column name or list of column names to average.
        weight_col: Weight column (default 'HSL').

    Returns:
        DataFrame (or Series if value_cols is a string) indexed by valid_time
        with the weighted mean of each requested column.
    """
    if isinstance(value_cols, str):
        value_cols = [value_cols]
        scalar = True
    else:
        scalar = False

    df_clean = df.dropna(subset=value_cols + [weight_col])
    grouped = df_clean.groupby('valid_time')
    denominator = grouped[weight_col].sum().replace(0, np.nan)

    result = {}
    for col in value_cols:
        numerator = (df_clean[col] * df_clean[weight_col]).groupby(df_clean['valid_time']).sum()
        result[col] = numerator / denominator

    out = pd.DataFrame(result)
    return out[value_cols[0]] if scalar else out


def _aggregate_rt_markups(rt, type_set, prefix):
    """Aggregate RT markups for one resource type group to hourly system-wide values.

    Filters to in-market resources of the given type(s), then computes
    HSL-weighted mean of offer_price_p85 and marginal_cost.

    Args:
        rt: Full RT markups DataFrame (output of compute_rt_markups).
        type_set: Set of Resource Type strings (e.g. {'CLLIG'}).
        prefix: Column name prefix for outputs (e.g. 'cllig').

    Returns:
        DataFrame with columns: valid_time, rt_{prefix}_p85, {prefix}_mc.
    """
    sub = rt[rt['Resource Type'].isin(type_set) & rt['in_market']]
    if sub.empty:
        return pd.DataFrame(columns=['valid_time', f'rt_{prefix}_p85', f'{prefix}_mc'])

    agg = _hsl_weighted_mean(sub, ['offer_price_p85', 'marginal_cost'])
    return agg.rename(columns={'offer_price_p85': f'rt_{prefix}_p85',
                                'marginal_cost': f'{prefix}_mc'}).reset_index()


def _aggregate_dam_markups(dam, type_set, prefix):
    """Aggregate DAM markups for one resource type group to hourly system-wide values.

    Converts Delivery Date + Hour Ending (1-based ERCOT convention) to valid_time,
    filters to in-market resources, then computes HSL-weighted mean of offer_price_p85.

    Args:
        dam: Full DAM markups DataFrame (output of compute_dam_markups).
        type_set: Set of Resource Type strings.
        prefix: Column name prefix (e.g. 'cllig').

    Returns:
        DataFrame with columns: valid_time, dam_{prefix}_p85.
    """
    sub = dam[dam['Resource Type'].isin(type_set) & dam['in_market']]
    if sub.empty:
        return pd.DataFrame(columns=['valid_time', f'dam_{prefix}_p85'])

    sub = sub.assign(
        valid_time=pd.to_datetime(sub['Delivery Date'])
        + pd.to_timedelta(sub['Hour Ending'].astype(int) - 1, unit='h')
    )
    price = _hsl_weighted_mean(sub, 'offer_price_p85').rename(f'dam_{prefix}_p85')
    return price.reset_index()


def _load_markups_hourly(year):
    """Load and aggregate thermal markups to system-wide hourly values for a full year.

    Loads the cached year-level RT and DAM markup parquets once, then aggregates
    by resource type group (CLLIG, SCGT, CCGT).

    Args:
        year: Integer year.

    Returns:
        DataFrame with columns: valid_time, rt_cllig_p85, dam_cllig_p85, cllig_mc,
        rt_scgt_p85, dam_scgt_p85, scgt_mc, rt_ccgt_p85, dam_ccgt_p85, ccgt_mc.
    """
    from process_data.compute_markups import compute_rt_markups, compute_dam_markups

    print(f"  Loading RT markups for {year}...")
    rt = compute_rt_markups(year)
    print(f"  Loading DAM markups for {year}...")
    dam = compute_dam_markups(year)

    groups = [
        ({'CLLIG'},           'cllig'),
        ({'SCGT90', 'SCLE90'}, 'scgt'),
        ({'CCGT90', 'CCLE90'}, 'ccgt'),
    ]

    rt_frames = []
    dam_frames = []
    for type_set, prefix in groups:
        rt_frames.append(_aggregate_rt_markups(rt, type_set, prefix))
        dam_frames.append(_aggregate_dam_markups(dam, type_set, prefix))

    # Merge all RT frames on valid_time
    rt_merged = rt_frames[0]
    for df in rt_frames[1:]:
        rt_merged = rt_merged.merge(df, on='valid_time', how='outer')

    # Merge all DAM frames on valid_time
    dam_merged = dam_frames[0]
    for df in dam_frames[1:]:
        dam_merged = dam_merged.merge(df, on='valid_time', how='outer')

    return rt_merged.merge(dam_merged, on='valid_time', how='outer')


# ---------------------------------------------------------------------------
# Main build function
# ---------------------------------------------------------------------------

def build_system_hourly_outcomes(year, months=None, force_rebuild=True):
    """Build and save the system-wide hourly outcome dataset.

    Merges economic congestion, curtailment, generation/emissions, thermal
    markups, and RUC deployments into a single hourly CSV panel.

    Args:
        year: Integer year (e.g. 2025).
        months: List of months to include (default: all 12).
        force_rebuild: If True, overwrite the cached output.

    Returns:
        DataFrame with one row per operating hour.
    """
    if months is None:
        months = list(range(1, 13))

    dirs = setup_directories()
    out_path = os.path.join(dirs['processed'], f'system_hourly_outcomes_{year}.csv')

    if os.path.exists(out_path) and not force_rebuild:
        print(f"Loading cached outcome data: {out_path}")
        return pd.read_csv(out_path, parse_dates=['valid_time'])

    print(f"Loading year-level markup data for {year}...")
    try:
        markups = _load_markups_hourly(year)
    except FileNotFoundError as e:
        print(f"  WARNING: markup data not found — skipping markup columns. ({e})")
        markups = None

    monthly_frames = []
    for month in months:
        print(f"\n=== {year}-{month:02d} ===")

        # Economic congestion cost
        try:
            cong = _load_congestion(year, month)
        except FileNotFoundError as e:
            print(f"  WARNING: congestion data missing — {e}")
            cong = pd.DataFrame(columns=['valid_time', 'economic_congestion_cost'])

        # RT system lambda — first SCED observation per hour
        sced_lam = _load_system_lambda_first(year, month)

        # Curtailment
        try:
            curt = _load_curtailment(year, month)
        except FileNotFoundError as e:
            print(f"  WARNING: curtailment data missing — {e}")
            curt = pd.DataFrame(columns=['valid_time', 'wind_curtailment_mw',
                                          'solar_curtailment_mw',
                                          'total_renewable_curtailment_mw'])

        # Generation mix + emissions
        try:
            gen = _load_generation_hourly(year, month)
        except FileNotFoundError as e:
            print(f"  WARNING: generation mix data missing — {e}")
            gen = pd.DataFrame(columns=['valid_time', 'total_generation_mw',
                                         'total_co2_rate_kg_per_h',
                                         'avg_intensity_kg_per_mwh'])

        # RUC deployment
        ruc = _load_ruc(year, month)

        # Build month panel anchored on congestion valid_times
        panel = cong
        for df in (sced_lam, curt, gen, ruc):
            if df.empty:
                continue
            panel = panel.merge(df, on='valid_time', how='left')
            # replace missing ruc values with 0 (assumes missingness is due to no RUC deployment, not missing file)
            if 'ruc_deployment_mw' in panel.columns:
                panel['ruc_deployment_mw'] = panel['ruc_deployment_mw'].fillna(0)
            if 'ruc_optout_deployment_mw' in panel.columns:
                panel['ruc_optout_deployment_mw'] = panel['ruc_optout_deployment_mw'].fillna(0)

        monthly_frames.append(panel)

    result = pd.concat(monthly_frames, ignore_index=True)
    result = (
        result.sort_values('valid_time')
        .drop_duplicates(subset='valid_time', keep='first')
        .reset_index(drop=True)
    )

    # Merge year-level markups (filtered to included months after concat)
    if markups is not None and not markups.empty:
        result = result.merge(markups, on='valid_time', how='left')

    # Time features
    result['hour_of_day'] = result['valid_time'].dt.hour
    result['month'] = result['valid_time'].dt.month
    result['is_weekend'] = (result['valid_time'].dt.weekday >= 5).astype(int)

    # Reorder: time index first, then outcomes, then controls
    time_feat_cols = ['hour_of_day', 'month', 'is_weekend']
    outcome_cols = [
        'economic_congestion_cost', 'rt_system_lambda',
        'wind_curtailment_mw', 'solar_curtailment_mw',
        'total_renewable_curtailment_mw',
        'total_generation_mw', 'total_co2_rate_kg_per_h', 'avg_intensity_kg_per_mwh',
        'rt_cllig_p85', 'dam_cllig_p85', 'cllig_mc',
        'rt_scgt_p85', 'dam_scgt_p85', 'scgt_mc',
        'rt_ccgt_p85', 'dam_ccgt_p85', 'ccgt_mc',
        'ruc_deployment_mw', 'ruc_optout_deployment_mw',
    ]
    ordered = ['valid_time'] + time_feat_cols + [
        c for c in outcome_cols if c in result.columns
    ]
    result = result[ordered]

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    result.to_csv(out_path, index=False)

    print(f"\nSaved: {out_path}")
    print(f"  Shape: {result.shape}")
    print(f"  Hours: {result['valid_time'].nunique()}")
    print(f"  Columns: {list(result.columns)}")

    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Build system-wide hourly outcome dataset for PCA analysis'
    )
    parser.add_argument('--year', type=int, default=2025)
    parser.add_argument('--months', type=int, nargs='+', default=None,
                        help='Months to include (default: all 12)')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--force', dest='force', action='store_true',
                       help='Overwrite cached output')
    group.add_argument('--no-force', dest='force', action='store_false',
                       help="Don't overwrite cached output")
    parser.set_defaults(force=True)
    args = parser.parse_args()

    build_system_hourly_outcomes(args.year, months=args.months, force_rebuild=args.force)
