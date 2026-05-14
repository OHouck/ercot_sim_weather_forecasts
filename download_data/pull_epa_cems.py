"""pull_epa_cems.py — Download EPA CEMS hourly emissions data for Texas (2025).

Pulls hourly fuel input, gross load, SO2 and NOx mass from the EPA CAMD
streaming API for all CEMS-reporting units in Texas. Data is fetched one month
at a time and saved as parquet files. Unit-average heat rates and emissions rates
are computed from these records and saved separately.

Requires ~/keys/epa_camd_key.txt (free registration at
https://www.epa.gov/power-sector/cam-api-portal).

Output:
  {raw}/cems/{year}/{mm}/cems_tx_{YYYYMM}.parquet
  {processed}/unit_heat_rates_{year}.parquet
"""

import sys
import time
import calendar
import argparse
import requests
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from helper_funcs import setup_directories

CEMS_HOURLY_URL = "https://api.epa.gov/easey/streaming-services/emissions/apportioned/hourly"

KEEP_COLS = [
    "facilityId", "facilityName", "unitId", "unit_id",
    "date", "hour", "opTime",
    "grossLoad", "heatInput",
    "so2Mass", "noxMass",
    "primaryFuelInfo", "secondaryFuelInfo", "unitType", "programCodeInfo",
]


def _load_epa_key():
    """Load EPA CAMD API key from ~/keys/epa_camd_key.txt."""
    path = Path.home() / "keys" / "epa_camd_key.txt"
    return path.read_text().strip()


def download_cems_month(year, month, force_rebuild=False):
    """Download all TX CEMS hourly records for one month and save as parquet.

    Fetches from the EPA CAMD streaming API with monthly date range. No
    pagination is needed — the API returns all matching records as a JSON array.

    Args:
        year: operating year (int)
        month: operating month 1..12 (int)
        force_rebuild: overwrite cached parquet if True (bool)

    Returns:
        DataFrame with CEMS hourly records for the month
    """
    dirs = setup_directories()
    out_dir = Path(dirs["raw"]) / "cems" / str(year) / f"{month:02d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"cems_tx_{year}{month:02d}.parquet"

    if out_path.exists() and not force_rebuild:
        print(f"  Cached: {out_path}")
        return pd.read_parquet(out_path)

    _, last_day = calendar.monthrange(year, month)
    begin = f"{year}-{month:02d}-01"
    end = f"{year}-{month:02d}-{last_day:02d}"

    api_key = _load_epa_key()
    print(f"  Fetching CEMS for TX {year}-{month:02d} ({begin} to {end})...", flush=True)
    t0 = time.time()

    r = requests.get(
        CEMS_HOURLY_URL,
        headers={"x-api-key": api_key},
        params={"beginDate": begin, "endDate": end, "stateCode": "TX"},
        timeout=300,
    )
    r.raise_for_status()
    data = r.json()
    elapsed = time.time() - t0
    print(f"  Retrieved {len(data):,} records in {elapsed:.1f}s")

    df = pd.DataFrame(data)[KEEP_COLS]
    df["date"] = pd.to_datetime(df["date"])
    df["unit_id"] = df["unit_id"].astype("Int64")

    df.to_parquet(out_path, index=False)
    print(f"  Saved to {out_path}")
    return df


def compute_unit_heat_rates(year, force_rebuild=False):
    """Compute unit-average heat rate and emissions rates from CEMS + net generation.

    Follows Woerman (2023) §C.1: aggregate heat input (fuel burned) over the
    full year, divide by aggregate net generation from the SCED disclosure to
    get a constant heat rate per unit. Emissions rates use the same denominator.

    For units >25 MW: uses CEMS heat input / SCED net generation.
    For units <25 MW or not in CEMS: falls back to EIA Form 923 plant-level data
    (handled in cems_ercot_crosswalk.py after merging).

    Args:
        year: operating year (int)
        force_rebuild: overwrite cached output if True (bool)

    Returns:
        DataFrame with one row per (facilityId, unitId) and columns:
        heat_rate_mmbtu_mwh, so2_rate_lb_mwh, nox_rate_lb_mwh,
        primary_fuel, unit_type, cems_gross_load_mwh, cems_heat_input_mmbtu,
        heat_rate_source='cems'
    """
    dirs = setup_directories()
    out_path = Path(dirs["processed"]) / f"unit_heat_rates_{year}.parquet"

    if out_path.exists() and not force_rebuild:
        print(f"  Cached: {out_path}")
        return pd.read_parquet(out_path)

    # Load all monthly CEMS parquets for the year
    cems_dir = Path(dirs["raw"]) / "cems" / str(year)
    frames = []
    for month in range(1, 13):
        p = cems_dir / f"{month:02d}" / f"cems_tx_{year}{month:02d}.parquet"
        if p.exists():
            frames.append(pd.read_parquet(p))
    if not frames:
        raise FileNotFoundError(f"No CEMS parquets found for {year}. Run download_cems_month first.")
    cems = pd.concat(frames, ignore_index=True)

    operating = cems[cems["opTime"] > 0].copy()

    # calculate total heat input, SO2 and NOX mass and operating hours per unit for full year
    agg = (
        operating.groupby(["facilityId", "facilityName", "unitId", "primaryFuelInfo", "unitType"])
        .agg(
            gross_load_mwh=("grossLoad", "sum"),
            heat_input_mmbtu=("heatInput", "sum"),
            so2_mass_lbs=("so2Mass", "sum"),
            nox_mass_lbs=("noxMass", "sum"),
            operating_hours=("opTime", "sum"),
        )
        .reset_index()
    )

    # net gen (denominator) is added during crosswalk step; gross load used here
    valid = agg["gross_load_mwh"] > 0
    agg["heat_rate_mmbtu_mwh"] = agg["heat_input_mmbtu"] / agg["gross_load_mwh"]
    agg["so2_rate_lb_mwh"] = agg["so2_mass_lbs"] / agg["gross_load_mwh"]
    agg["nox_rate_lb_mwh"] = agg["nox_mass_lbs"] / agg["gross_load_mwh"]
    agg.loc[~valid, ["heat_rate_mmbtu_mwh", "so2_rate_lb_mwh", "nox_rate_lb_mwh"]] = None

    BOUNDS = {
        "Combustion turbine": (9.0, 16.0),
        "Combined cycle": (6.0, 10.0),
        "Boiler": (8.5, 14.0),
        "Steam turbine": (8.5, 14.0),
    }
    for unit_type, (lo, hi) in BOUNDS.items():
        mask = (agg["unitType"] == unit_type) & valid
        out_of_range = mask & ((agg["heat_rate_mmbtu_mwh"] < lo) | (agg["heat_rate_mmbtu_mwh"] > hi))
        n_flagged = out_of_range.sum()
        if n_flagged > 0:
            print(f"  WARNING: {n_flagged} {unit_type} units outside expected heat-rate range [{lo}, {hi}]")

    agg["heat_rate_source"] = "cems"
    agg.to_parquet(out_path, index=False)
    print(f"  Saved {len(agg)} unit heat rates to {out_path}")

    print(f"\n  Summary by fuel type:")
    summary = (
        agg[valid]
        .groupby("primaryFuelInfo")["heat_rate_mmbtu_mwh"]
        .agg(["count", "mean", "min", "max"])
        .round(2)
    )
    print(summary.to_string())
    return agg


def main():
    """Download TX CEMS hourly data for all months of a year."""
    parser = argparse.ArgumentParser(description="Download EPA CEMS for Texas")
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--months", type=int, nargs="+", default=list(range(12, 13)))
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--heat-rates-only", action="store_true",
                        help="Skip downloads, just recompute heat rates from cached data")
    args = parser.parse_args()

    if not args.heat_rates_only:
        for month in args.months:
            print(f"\n=== CEMS {args.year}-{month:02d} ===")
            download_cems_month(args.year, month, force_rebuild=args.force)
    exit()

    print(f"\n=== Computing unit heat rates for {args.year} ===")
    df = compute_unit_heat_rates(args.year, force_rebuild=args.force)
    print(f"Total CEMS units: {len(df):,}")


if __name__ == "__main__":
    main()
