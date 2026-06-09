"""Compute hourly economic congestion cost from zone LMPs and SCED system lambda.

Produces one output:
  economic_congestion_cost [$/h] = Σ_z (LMP_z - λ_sys)^2 × Q_z
  zone_lmp_spread_mw [$/MWh]     = load-weighted std dev of zone LMPs
  system_lambda [$/MWh]          = actual RT system lambda from SCED data

System lambda is read directly from the SCED real-time lambda parquet files
(5-minute intervals averaged to hourly). Zone LMPs and load quantities come
from the ERCOT API data in process_ercot.

Usage:
    from process_data.process_congestion import compute_economic_congestion_cost
    econ = compute_economic_congestion_cost(2025, 7)
"""

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from helper_funcs import setup_directories


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def _load_sced_lambda_month(year, month):
    """Load SCED real-time system lambda for one month and average to hourly.

    Reads 5-minute SCED lambda records, floors timestamps to the hour, and
    returns the per-hour mean system lambda.

    Args:
        year: Integer year.
        month: Integer month.

    Returns:
        DataFrame with columns: valid_time, system_lambda.
    """
    dirs = setup_directories()
    path = os.path.join(
        dirs["raw"], "ercot", "sced_lambda",
        str(year), f"{month:02d}",
        f"sced_lambda_{year}-{month:02d}.parquet",
    )
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"SCED lambda parquet not found: {path}\n"
            f"Download with: uv run python -m download_data.pull_sced_lambda "
            f"--year {year} --month {month}"
        )

    df = pd.read_parquet(path, columns=["sced_timestamp", "system_lambda"])
    df["valid_time"] = (
        pd.to_datetime(df["sced_timestamp"], format="%m/%d/%Y %H:%M:%S")
        .dt.floor("h")
    )
    hourly = (
        df.groupby("valid_time")["system_lambda"]
        .mean()
        .reset_index()
    )
    print(f"  SCED lambda: {len(hourly)} hours, "
          f"mean λ={hourly['system_lambda'].mean():.2f} $/MWh")
    return hourly


# ---------------------------------------------------------------------------
# Economic congestion cost
# ---------------------------------------------------------------------------

def compute_economic_congestion_cost(year, month, force_rebuild=False):
    """Compute hourly economic congestion rent = Σ_z (LMP_z - λ_sys)^2 × Q_z.

    System lambda is the actual real-time system lambda from SCED data,
    averaged from 5-minute intervals to hourly. Zone LMPs and load come
    from ERCOT API data.

    Args:
        year: Integer year.
        month: Integer month.
        force_rebuild: If True, recompute even if cached.

    Returns:
        DataFrame with columns:
            valid_time, system_lambda [$/MWh],
            economic_congestion_cost [$/h],
            zone_lmp_spread_mw [$/MWh load-weighted std dev across zones].
    """
    dirs = setup_directories()
    cache_path = os.path.join(
        dirs["processed"], "congestion_metrics",
        f"economic_congestion_{year}{month:02d}.csv",
    )

    if os.path.exists(cache_path) and not force_rebuild:
        print(f"  Loading cached economic congestion: {cache_path}")
        return pd.read_csv(cache_path, parse_dates=["valid_time"])

    from process_data.process_ercot import compute_hourly_zone_lmp, compute_hourly_load_by_lz

    zone_lmp = compute_hourly_zone_lmp(year, month)
    zone_load = compute_hourly_load_by_lz(year, month)
    sced_lambda = _load_sced_lambda_month(year, month)

    zones = [
        "LZ_AEN", "LZ_CPS", "LZ_HOUSTON", "LZ_LCRA",
        "LZ_NORTH", "LZ_RAYBN", "LZ_SOUTH", "LZ_WEST",
    ]
    lmp_zones = [z for z in zones if z in zone_lmp.columns]
    load_zones = [z for z in zones if z in zone_load.columns]
    common_zones = [z for z in lmp_zones if z in load_zones]

    if not common_zones:
        raise ValueError("No load zones matched between LMP and load data")

    merged = zone_lmp[["valid_time"] + lmp_zones].merge(
        zone_load[["valid_time"] + load_zones],
        on="valid_time",
        how="inner",
        suffixes=("_lmp", "_mw"),
    )
    merged = merged.merge(sced_lambda, on="valid_time", how="left")

    lmp_cols = [f"{z}_lmp" for z in common_zones]
    mw_cols = [f"{z}_mw" for z in common_zones]

    # Congestion rent = Σ_z (LMP_z - λ)^2 × Q_z  [$/h]
    merged["economic_congestion_cost"] = sum(
        (merged[f"{z}_lmp"] - merged["system_lambda"])**2 * merged[f"{z}_mw"]
        for z in common_zones
    )

    # Load-weighted LMP std dev across zones [$/MWh]
    total_load = merged[mw_cols].sum(axis=1).replace(0, np.nan)
    load_wtd_mean = sum(
        merged[f"{z}_lmp"] * merged[f"{z}_mw"] for z in common_zones
    ) / total_load
    load_wtd_var = sum(
        merged[f"{z}_mw"] * (merged[f"{z}_lmp"] - load_wtd_mean) ** 2
        for z in common_zones
    ) / total_load
    merged["zone_lmp_spread_mw"] = np.sqrt(load_wtd_var.clip(lower=0))

    result = merged[
        ["valid_time", "system_lambda", "economic_congestion_cost", "zone_lmp_spread_mw"]
    ].copy()

    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    result.to_csv(cache_path, index=False)
    print(f"  Saved economic congestion: {cache_path}")
    print(
        f"    {len(result)} hours, "
        f"mean cost={result['economic_congestion_cost'].mean():.1f} $/h, "
        f"mean λ={result['system_lambda'].mean():.2f} $/MWh"
    )
    return result


# ---------------------------------------------------------------------------
# Merge helper (for create_pixel_level_data.py)
# ---------------------------------------------------------------------------

def merge_congestion_system(pixel_df, year, month, time_col="valid_time"):
    """Merge economic congestion metrics into a pixel-hourly DataFrame.

    Adds: economic_congestion_cost, zone_lmp_spread_mw, system_lambda.

    Args:
        pixel_df: DataFrame with a time column.
        year: Integer year.
        month: Integer month.
        time_col: Name of the time column for merging.

    Returns:
        DataFrame with congestion columns added (left join).
    """
    econ = compute_economic_congestion_cost(year, month)
    econ_cols = ["valid_time", "economic_congestion_cost",
                 "zone_lmp_spread_mw", "system_lambda"]
    econ = econ[econ_cols]

    if time_col != "valid_time":
        econ = econ.rename(columns={"valid_time": time_col})

    return pixel_df.merge(econ, on=time_col, how="left")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Compute hourly economic congestion cost from zone LMPs and SCED lambda"
    )
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--month", type=int, default=7)
    parser.add_argument("--force-rebuild", action="store_true")
    args = parser.parse_args()

    print("=== Computing economic congestion cost ===")
    result = compute_economic_congestion_cost(
        args.year, args.month, args.force_rebuild
    )
    print(f"\n{result.describe()}\n")
