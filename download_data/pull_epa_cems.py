"""pull_epa_cems.py — Download EPA CEMS hourly emissions data for Texas.

Pulls hourly fuel input, gross load, SO2 and NOx mass from the EPA CAMD
streaming API for all CEMS-reporting units in Texas. Data is fetched one month
at a time and saved as parquet files. Per-unit annual totals of heat input
and emissions mass are aggregated and saved separately.

Heat rates are deliberately **not** computed here — CEMS reports gross load
(MW at the generator terminals) which is the wrong denominator. Heat rates
are computed in ``process_data.cems_ercot_crosswalk`` after joining each
CEMS unit to its ERCOT Resource Name(s) and dividing CEMS heat input by SCED
net generation, following Woerman (2023) §C.1.

Requires ~/keys/epa_camd_key.txt (free registration at
https://www.epa.gov/power-sector/cam-api-portal).

Output:
  {raw}/cems/{year}/{mm}/cems_tx_{YYYYMM}.parquet      — hourly per-unit
  {processed}/unit_heat_rates_{year}.parquet           — annual per-unit aggregates
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


def aggregate_unit_annual(year, force_rebuild=False):
    """Aggregate CEMS hourly records to per-unit annual totals.

    Sums heat input (MMBtu), SO2 mass (lb), NOx mass (lb), gross load (MWh)
    and operating hours over the full year for each (facilityId, unitId). Heat
    rates are intentionally not computed here — they require SCED net
    generation as the denominator and are produced by
    ``process_data.cems_ercot_crosswalk.build_crosswalk``.

    Gross load is retained for diagnostic purposes (e.g. cross-check against
    SCED, identify periods where CEMS reported fuel input but no output).

    Args:
        year: Operating year (int).
        force_rebuild: Overwrite cached output if True.

    Returns:
        DataFrame with columns:
            facilityId, facilityName, unitId, primaryFuelInfo, unitType,
            heat_input_mmbtu, so2_mass_lbs, nox_mass_lbs,
            cems_gross_load_mwh, operating_hours.
        One row per (facilityId, unitId) that operated at least one hour.
    """
    dirs = setup_directories()
    out_path = Path(dirs["processed"]) / f"unit_heat_rates_{year}.parquet"

    if out_path.exists() and not force_rebuild:
        print(f"  Cached: {out_path}")
        return pd.read_parquet(out_path)

    cems_dir = Path(dirs["raw"]) / "cems" / str(year)
    frames = []
    for month in range(1, 13):
        p = cems_dir / f"{month:02d}" / f"cems_tx_{year}{month:02d}.parquet"
        if p.exists():
            frames.append(pd.read_parquet(p))
    if not frames:
        raise FileNotFoundError(
            f"No CEMS parquets found for {year}. Run download_cems_month first."
        )
    cems = pd.concat(frames, ignore_index=True)
    operating = cems[cems["opTime"] > 0]

    agg = (
        operating.groupby(["facilityId", "facilityName", "unitId", "primaryFuelInfo", "unitType"])
        .agg(
            heat_input_mmbtu=("heatInput", "sum"),
            so2_mass_lbs=("so2Mass", "sum"),
            nox_mass_lbs=("noxMass", "sum"),
            cems_gross_load_mwh=("grossLoad", "sum"),
            operating_hours=("opTime", "sum"),
        )
        .reset_index()
    )
    agg.to_parquet(out_path, index=False)
    print(f"  Saved {len(agg)} unit annual aggregates to {out_path}")
    return agg


def main():
    """Download TX CEMS hourly data and aggregate to per-unit annual totals."""
    parser = argparse.ArgumentParser(description="Download EPA CEMS for Texas")
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--months", type=int, nargs="+", default=list(range(1, 13)))
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--aggregate-only", action="store_true",
                        help="Skip downloads, just rebuild annual aggregates from cached data")
    args = parser.parse_args()

    if not args.aggregate_only:
        for month in args.months:
            print(f"\n=== CEMS {args.year}-{month:02d} ===")
            download_cems_month(args.year, month, force_rebuild=args.force)

    print(f"\n=== Aggregating annual CEMS totals for {args.year} ===")
    df = aggregate_unit_annual(args.year, force_rebuild=args.force)
    print(f"Total CEMS units: {len(df):,}")


if __name__ == "__main__":
    main()
