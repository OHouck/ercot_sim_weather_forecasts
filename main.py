"""Build the EOF analysis inputs and run the EOF workflow.

This entrypoint rebuilds the raw weather and grid inputs, then the ERA5
forecast-error inputs, the yearly system outcome file, and finally runs the
three EOF phases from analysis/eof_analysis.py.
"""

from __future__ import annotations

import argparse
from typing import Iterable

from helper_funcs import setup_directories


DEFAULT_YEAR = 2024
DEFAULT_MONTHS = tuple(range(1, 13))


def _validate_months(months: Iterable[int]) -> list[int]:
    """Validate and normalize month numbers.

    Parameters
    ----------
    months : iterable[int]

    Returns
    -------
    list[int]
    """
    normalized = []
    for month in months:
        if month < 1 or month > 12:
            raise ValueError(f"Month must be between 1 and 12, got {month}")
        normalized.append(int(month))
    return normalized


def _download_weather_inputs(year: int, months: list[int], dirs: dict) -> None:
    """Download the weather inputs needed for the EOF pipeline."""
    from download_data.pull_hrrr import download_hrrr_month
    from download_data.pull_gfs import download_gfs_month
    from download_data.pull_era5 import download_era5_month, download_era5_wind100m_month

    print("\n=== Step 1: Download weather inputs ===")
    for month in months:
        download_hrrr_month(year, month, dirs["raw"])
        download_gfs_month(year, month, dirs["raw"])
        download_era5_month(year, month, base_dir=dirs["raw"])
        download_era5_wind100m_month(year, month, base_dir=dirs["raw"])


def _download_grid_inputs(dirs: dict) -> None:
    """Download the grid and coordinate inputs needed downstream."""
    from download_data.pull_np4160 import download_np4_160
    from download_data.pull_eia860 import download_eia860_plants
    from process_data.process_ercot import build_node_coordinates

    print("\n=== Step 2: Download grid inputs ===")
    download_np4_160()
    download_eia860_plants()
    build_node_coordinates(force_rebuild=True)


def _download_market_inputs(year: int, months: list[int]) -> None:
    """Download the ERCOT market inputs used by the outcome builder."""
    from download_data.pull_ercot import download_month as download_ercot_month

    print("\n=== Step 3: Download ERCOT market inputs ===")
    for month in months:
        download_ercot_month(year, month)


def _build_thermal_inputs(year: int, months: list[int], force_rebuild: bool) -> None:
    """Download thermal source data and build the markup tables."""
    from download_data.pull_epa_cems import download_cems_month, aggregate_unit_annual
    from download_data.pull_epa_eia_crosswalk import download_crosswalk
    from download_data.pull_eia923 import build_heat_rates
    from download_data.pull_dam_disclosure import download_dam_disclosure_month
    from download_data.pull_fuel_prices import build_fuel_price_tables
    from download_data.pull_ercot import load_credentials, get_bearer_token
    from process_data.compute_markups import compute_dam_markups, compute_rt_markups

    creds = load_credentials()
    bearer_token = get_bearer_token(creds["username"], creds["password"])
    if not bearer_token:
        raise RuntimeError("Failed to obtain ERCOT bearer token for DAM disclosure downloads")

    print("\n=== Step 4: Download thermal inputs ===")
    for month in months:
        download_cems_month(year, month, force_rebuild=force_rebuild)
        download_dam_disclosure_month(
            year, month, bearer_token, creds["api_key"], force_rebuild=force_rebuild
        )

    download_crosswalk(force_rebuild=force_rebuild)
    build_heat_rates(year, force_rebuild=force_rebuild)
    aggregate_unit_annual(year, force_rebuild=force_rebuild)
    build_fuel_price_tables(year, force_rebuild=force_rebuild)

    print("\n=== Step 5: Build markup amounts ===")
    compute_dam_markups(year, months=months, force_rebuild=force_rebuild)
    compute_rt_markups(year, months=months, force_rebuild=force_rebuild)


def build_pipeline(year=DEFAULT_YEAR, months=None, force_rebuild=True,
                   run_analysis=True, n_modes=None, depvars=None,
                   run_significance=True):
    """Build the EOF analysis inputs and run the EOF workflow.

    Parameters
    ----------
    year : int
    months : iterable[int] or None
    force_rebuild : bool
    run_analysis : bool
    n_modes : int or None
    depvars : list[str] or None
    run_significance : bool

    Returns
    -------
    dict
    """
    from process_data.calculate_forecast_errors import calculate_era5_errors_for_month
    from process_data.calculate_ruc_commitments import compute_ruc_commitments
    from process_data.create_outcome_data import build_system_hourly_outcomes
    from analysis.eof_analysis import (
        run_eof_significance,
        run_eof_decomposition,
        run_eof_analysis,
    )

    dirs = setup_directories()
    month_numbers = _validate_months(months or DEFAULT_MONTHS)
    month_pairs = [(year, month) for month in month_numbers]

    print("Building EOF pipeline for:")
    print("  " + ", ".join(f"{year}-{month:02d}" for year, month in month_pairs))

    _download_weather_inputs(year, month_numbers, dirs)
    _download_grid_inputs(dirs)
    _download_market_inputs(year, month_numbers)
    _build_thermal_inputs(year, month_numbers, force_rebuild)

    print("\n=== Step 6: Build ERA5 forecast errors ===")
    for model in ("hrrr", "gfs"):
        print(f"  Model: {model.upper()}")
        for run_year, month in month_pairs:
            calculate_era5_errors_for_month(run_year, month, model=model)

    print("\n=== Step 7: Build RUC commitments ===")
    compute_ruc_commitments(year, months=month_numbers, force_rebuild=force_rebuild)

    print("\n=== Step 8: Build system hourly outcomes ===")
    build_system_hourly_outcomes(year, months=month_numbers, force_rebuild=force_rebuild)

    exit()

    if run_significance:
        print("\n=== Step 9: EOF significance ===")
        run_eof_significance()

    print("\n=== Step 10: EOF decomposition ===")
    decomp_result = run_eof_decomposition(K=n_modes)

    if run_analysis:
        print("\n=== Step 11: EOF analysis ===")
        run_eof_analysis(K=n_modes, depvars=depvars, bundle=decomp_result.get("bundle"))

    return {
        "dirs": dirs,
        "year": year,
        "months": month_pairs,
        "force_rebuild": force_rebuild,
        "run_analysis": run_analysis,
        "run_significance": run_significance,
    }


def main():
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        description="Build the EOF analysis inputs and run the EOF workflow"
    )
    parser.add_argument("--year", type=int, default=DEFAULT_YEAR,
                        help="Analysis year (default: 2025)")
    parser.add_argument("--months", type=int, nargs="*", default=None,
                        help="Months to process (default: all 12)")
    parser.add_argument("--n-modes", type=int, default=None,
                        help="Override EOF modes per channel uniformly")
    parser.add_argument("--depvars", nargs="*", default=None,
                        help="Subset of EOF outcomes to analyze")
    parser.add_argument("--no-force", action="store_true",
                        help="Reuse cached outputs when available")
    parser.add_argument("--skip-analysis", action="store_true",
                        help="Stop after the EOF decomposition stage")
    parser.add_argument("--skip-significance", action="store_true",
                        help="Skip the EOF significance step")
    args = parser.parse_args()

    months = args.months if args.months else list(DEFAULT_MONTHS)
    build_pipeline(
        year=args.year,
        months=months,
        force_rebuild=not args.no_force,
        run_analysis=not args.skip_analysis,
        n_modes=args.n_modes,
        depvars=args.depvars,
        run_significance=not args.skip_significance,
    )


if __name__ == "__main__":
    main()


