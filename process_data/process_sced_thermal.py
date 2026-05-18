"""process_sced_thermal.py — Load thermal-unit net generation from 60-Day SCED Disclosure.

Reads SCED Gen Resource Data CSVs (nested ZIPs in
``{raw}/ercot/sced/{monthname}{year}/``) and extracts per-resource net output
for thermal generation technologies. Converts MW telemetered output to MWh
using SCED's ~5-minute interval cadence and caches the result.

Two outputs are produced:
  - ``{processed}/sced_thermal_hourly_{year}.parquet``  one row per (resource, hour)
  - ``{processed}/sced_thermal_annual_{year}.parquet``  one row per resource

The annual file is the denominator in Woerman (2023) §C.1 heat-rate
calculations: heat_rate = sum(CEMS heat input) / sum(SCED net generation).
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from helper_funcs import setup_directories, THERMAL_RESOURCE_TYPES
from process_data.process_curtailment import _load_sced_disclosure_month


def build_sced_thermal_hourly(year, force_rebuild=False):
    """Build a (resource, hour) parquet of net generation in MWh for thermal units.

    For each SCED 5-minute interval, telemetered net output is recorded in MW.
    MWh per interval = MW × (1/12 h). Per-hour MWh = sum(MW)/12 across the ~12
    intervals in the hour; using the actual interval count handles DST hours
    and gaps correctly.

    Args:
        year: Operating year (int).
        force_rebuild: Overwrite cached parquet if True.

    Returns:
        DataFrame with columns:
            resource_name, resource_type, valid_time,
            net_gen_mwh, hsl_mw_mean, n_sced_intervals.
        One row per (resource, hour) for the full year (thermal resources only).
    """
    dirs = setup_directories()
    cache_path = Path(dirs["processed"]) / f"sced_thermal_hourly_{year}.parquet"
    if cache_path.exists() and not force_rebuild:
        print(f"  Cached: {cache_path.name}")
        return pd.read_parquet(cache_path)

    monthly_frames = []
    for month in range(1, 13):
        try:
            raw = _load_sced_disclosure_month(year, month, resource_types=THERMAL_RESOURCE_TYPES)
        except FileNotFoundError as e:
            print(f"    SKIP {year}-{month:02d}: {e}")
            continue

        hourly = (
            raw.groupby(["resource_name", "resource_type", "valid_time"], as_index=False)
            .agg(
                sum_mw=("telemetered_output", "sum"),
                hsl_mw_mean=("hsl", "mean"),
                n_sced_intervals=("sced_time", "nunique"),
            )
        )
        hourly["net_gen_mwh"] = hourly["sum_mw"] / hourly["n_sced_intervals"]
        hourly = hourly.drop(columns=["sum_mw"])
        monthly_frames.append(hourly)

    if not monthly_frames:
        raise FileNotFoundError(f"No SCED thermal data found for any month of {year}")

    annual = pd.concat(monthly_frames, ignore_index=True)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    annual.to_parquet(cache_path, index=False)
    print(f"  Saved {len(annual):,} (resource, hour) rows to {cache_path}")
    print(f"    Resources: {annual['resource_name'].nunique()}, "
          f"hours: {annual['valid_time'].nunique()}")
    return annual


def build_sced_thermal_annual(year, force_rebuild=False):
    """Aggregate the hourly thermal file to annual net generation per resource.

    Args:
        year: Operating year (int).
        force_rebuild: Overwrite cached parquet if True.

    Returns:
        DataFrame with columns:
            resource_name, resource_type,
            net_gen_mwh_annual, operating_hours, mean_hsl_mw.
        One row per resource that had any thermal SCED output that year.
    """
    dirs = setup_directories()
    cache_path = Path(dirs["processed"]) / f"sced_thermal_annual_{year}.parquet"
    if cache_path.exists() and not force_rebuild:
        print(f"  Cached: {cache_path.name}")
        return pd.read_parquet(cache_path)

    hourly = build_sced_thermal_hourly(year, force_rebuild=force_rebuild)

    annual = (
        hourly.groupby(["resource_name", "resource_type"], as_index=False)
        .agg(
            net_gen_mwh_annual=("net_gen_mwh", "sum"),
            operating_hours=("net_gen_mwh", lambda s: (s > 0).sum()),
            mean_hsl_mw=("hsl_mw_mean", "mean"),
        )
    )
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    annual.to_parquet(cache_path, index=False)
    print(f"  Saved annual aggregates for {len(annual)} thermal resources to {cache_path}")
    return annual


def main():
    """CLI entry point — build hourly and annual SCED thermal aggregates."""
    import argparse
    parser = argparse.ArgumentParser(description="Build SCED thermal net-generation caches")
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    print(f"\n=== SCED thermal net generation for {args.year} ===")
    build_sced_thermal_hourly(args.year, force_rebuild=args.force)
    annual = build_sced_thermal_annual(args.year, force_rebuild=args.force)
    print(f"\n  Top 10 thermal resources by annual MWh:")
    print(annual.nlargest(10, "net_gen_mwh_annual").to_string(index=False))


if __name__ == "__main__":
    main()
