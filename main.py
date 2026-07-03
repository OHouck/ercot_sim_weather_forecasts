"""Build the analysis inputs and run the Functional PLS (FPLS) workflow.

This entrypoint is a self-contained, year/month-configurable pipeline. For the
requested year and months it downloads the raw weather, grid, market, SCED, and
thermal inputs, builds the ERA5 forecast-error fields and the system hourly
outcome panel, then runs the three FPLS phases from analysis/fpls_analysis.py.

Two inputs have no automated downloader and must be placed by hand before an
end-to-end run can complete: the ERCOT generation-mix workbook and the 60-Day
SCED Disclosure ZIPs. The pipeline checks for these and stops with clear
instructions (fail-loud) rather than silently producing a hollow outcome file.

Usage:
    uv run python main.py --year 2024
    uv run python main.py --year 2024 --months 1 2 3
    uv run python main.py --year 2025 --skip-analysis
"""

from __future__ import annotations

import argparse
import calendar
import os
from typing import Iterable

from helper_funcs import setup_directories


DEFAULT_YEAR = 2025
DEFAULT_MONTHS = tuple(range(1, 13))

# Raw outcome columns that the on-demand loaders in create_outcome_data swallow
# silently (via caught FileNotFoundError) when their source data is absent.
# Verified after the outcome build so a missing source fails loud rather than
# yielding a hollow panel. Markup depvars are derived downstream, not checked here.
REQUIRED_OUTCOME_COLUMNS = (
    "economic_congestion_cost",        # needs SCED system lambda
    "total_renewable_curtailment_mw",  # needs 60-Day SCED Disclosure
    "avg_intensity_kg_per_mwh",        # needs generation-mix workbook
    "ruc_deployment_mw",               # needs 60-Day SCED Disclosure
)


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
    """Download the weather inputs needed for the pipeline."""
    from download_data.pull_hrrr import download_hrrr_month
    from download_data.pull_gfs import download_gfs_month
    from download_data.pull_era5 import download_era5_month, download_era5_wind100m_month

    print("\n=== Step 1: Download weather inputs ===")
    for month in months:
        download_hrrr_month(year, month)
        download_gfs_month(year, month)
        download_era5_month(year, month, base_dir=dirs["raw"])
        download_era5_wind100m_month(year, month, base_dir=dirs["raw"])


def _download_grid_inputs(dirs: dict, force_rebuild: bool) -> None:
    """Download the grid and coordinate inputs needed downstream."""
    from download_data.pull_np4160 import download_np4_160
    from download_data.pull_eia860 import download_eia860_plants
    from process_data.process_ercot import build_node_coordinates

    print("\n=== Step 2: Download grid inputs ===")
    download_np4_160()
    download_eia860_plants()
    build_node_coordinates(force_rebuild=force_rebuild)


def _download_market_inputs(year: int, months: list[int]) -> None:
    """Download the ERCOT market inputs used by the outcome builder."""
    from download_data.pull_ercot import download_month as download_ercot_month

    print("\n=== Step 3: Download ERCOT market inputs ===")
    for month in months:
        download_ercot_month(year, month)


def _download_sced_lambda_inputs(year: int, months: list[int], dirs: dict) -> None:
    """Download SCED System Lambda, one file per month.

    System lambda drives ``economic_congestion_cost`` (the primary FPLS outcome)
    and ``rt_system_lambda``. Files land in the layout the outcome builder reads:
    {raw}/ercot/sced_lambda/{year}/{mm}/sced_lambda_{year}-{mm}.parquet.
    """
    from download_data.pull_ercot import (
        load_credentials, get_bearer_token, download_sced_lambda,
    )

    creds = load_credentials()
    bearer_token = get_bearer_token(creds["username"], creds["password"])
    if not bearer_token:
        raise RuntimeError("Failed to obtain ERCOT bearer token for SCED lambda downloads")

    print("\n=== Step 3b: Download SCED system lambda ===")
    for month in months:
        last_day = calendar.monthrange(year, month)[1]
        start_date = f"{year}-{month:02d}-01"
        end_date = f"{year}-{month:02d}-{last_day:02d}"
        month_dir = os.path.join(
            dirs["raw"], "ercot", "sced_lambda", str(year), f"{month:02d}"
        )
        download_sced_lambda(start_date, end_date, month_dir,
                             creds["api_key"], bearer_token)


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


def _check_manual_inputs(year: int, months: list[int], dirs: dict) -> None:
    """Fail loud if a manually-downloaded input is missing for the requested period.

    Two sources have no automated downloader and must be placed by hand:
      * the ERCOT generation-mix workbook (drives carbon intensity), and
      * the 60-Day SCED Disclosure ZIPs (drive RUC deployment + curtailment).

    Raises SystemExit with actionable instructions and expected paths when any
    required file is missing, so an end-to-end run never silently produces an
    outcome panel that is hollow for those variables.
    """
    from process_data.process_curtailment import _find_sced_folders

    problems = []

    gen_mix_path = os.path.join(
        dirs["raw"], "ercot", "generation_mix", f"IntGenbyFuel{year}.xlsx"
    )
    if not os.path.exists(gen_mix_path):
        problems.append(
            f"  * Generation-mix workbook missing:\n"
            f"      expected: {gen_mix_path}\n"
            f"      download the {year} Fuel Mix Report (IntGenbyFuel{year}.xlsx) from\n"
            f"      https://www.ercot.com/gridinfo/generation"
        )

    missing_sced_months = []
    for month in months:
        try:
            _find_sced_folders(year, month)
        except FileNotFoundError:
            missing_sced_months.append(month)
    if missing_sced_months:
        months_label = ", ".join(f"{year}-{m:02d}" for m in missing_sced_months)
        sced_root = os.path.join(dirs["raw"], "ercot", "sced")
        problems.append(
            f"  * 60-Day SCED Disclosure ZIPs missing for operating months: {months_label}\n"
            f"      ERCOT releases operating month N in release folder N+2\n"
            f"      (e.g. 2024-01 data lives in a 'march2024' folder).\n"
            f"      Download the NP3-965-ER archives from the ERCOT MIS portal and place the\n"
            f"      ZIPs under: {sced_root}/<releasemonth><year>/"
        )

    if problems:
        raise SystemExit(
            "\n=== Manual inputs required before an end-to-end run can complete ===\n"
            + "\n".join(problems)
            + "\n\nThese two sources have no automated downloader. Add them, then re-run.\n"
            "(Everything else — weather, market, SCED lambda, thermal, errors — is already\n"
            " downloaded/built above and will be skipped on the next run.)"
        )


def _verify_outcomes(outcomes_df) -> None:
    """Warn if any required outcome column is absent or entirely missing.

    The on-demand loaders in create_outcome_data catch FileNotFoundError and
    drop the affected column, so a build can succeed while silently omitting a
    key outcome. This surfaces that case after the fact.
    """
    hollow = [
        column for column in REQUIRED_OUTCOME_COLUMNS
        if column not in outcomes_df.columns
        or outcomes_df[column].notna().sum() == 0
    ]
    if hollow:
        print("\n  WARNING: the outcome panel is empty for: " + ", ".join(hollow))
        print("  These FPLS depvars will be unusable. Check the source downloads above.")
    else:
        print("  All required outcome columns are populated.")


def build_pipeline(year=DEFAULT_YEAR, months=None, force_rebuild=False,
                   run_analysis=True, depvars=None, run_significance=True):
    """Build the analysis inputs and run the FPLS workflow.

    Parameters
    ----------
    year : int
    months : iterable[int] or None
    force_rebuild : bool
    run_analysis : bool — run FPLS Phase 3 (regression + figures)
    depvars : list[str] or None
    run_significance : bool — run FPLS Phase 1 (mode-selection diagnostics)

    Returns
    -------
    dict
    """
    from process_data.calculate_forecast_errors import calculate_era5_errors_for_month
    from process_data.calculate_ruc_commitments import compute_ruc_commitments
    from process_data.create_outcome_data import build_system_hourly_outcomes
    from analysis.fpls_analysis import (
        run_fpls_mode_selection,
        run_fpls_decomposition,
        run_fpls_analysis,
    )

    dirs = setup_directories()
    month_numbers = _validate_months(months or DEFAULT_MONTHS)
    month_pairs = [(year, month) for month in month_numbers]

    print("Building FPLS pipeline for:")
    print("  " + ", ".join(f"{year}-{month:02d}" for year, month in month_pairs))

    _download_weather_inputs(year, month_numbers, dirs)
    _download_grid_inputs(dirs, force_rebuild)
    _download_market_inputs(year, month_numbers)
    _download_sced_lambda_inputs(year, month_numbers, dirs)
    _build_thermal_inputs(year, month_numbers, force_rebuild)

    print("\n=== Step 6: Build ERA5 forecast errors ===")
    for model in ("hrrr", "gfs"):
        print(f"  Model: {model.upper()}")
        for run_year, month in month_pairs:
            calculate_era5_errors_for_month(run_year, month, model=model,
                                             force_rebuild=force_rebuild)

    # Fail loud on the two manual inputs before the steps that consume them, so
    # nothing partial is written and the user is told exactly what to add.
    _check_manual_inputs(year, month_numbers, dirs)

    print("\n=== Step 7: Build RUC commitments ===")
    compute_ruc_commitments(year, months=month_numbers, force_rebuild=force_rebuild)

    print("\n=== Step 8: Build system hourly outcomes ===")
    outcomes_df = build_system_hourly_outcomes(
        year, months=month_numbers, force_rebuild=force_rebuild)
    _verify_outcomes(outcomes_df)

    if run_significance:
        print("\n=== Step 9: FPLS mode selection (Phase 1) ===")
        run_fpls_mode_selection(depvars=depvars, months=month_pairs)

    print("\n=== Step 10: FPLS decomposition (Phase 2) ===")
    run_fpls_decomposition(depvars=depvars, months=month_pairs)

    if run_analysis:
        print("\n=== Step 11: FPLS analysis (Phase 3) ===")
        run_fpls_analysis(depvars=depvars, months=month_pairs)

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
        description="Build the analysis inputs and run the FPLS workflow"
    )
    parser.add_argument("--year", type=int, default=DEFAULT_YEAR,
                        help="Analysis year (default: 2025)")
    parser.add_argument("--months", type=int, nargs="*", default=None,
                        help="Months to process (default: all 12)")
    parser.add_argument("--depvars", nargs="*", default=None,
                        help="Subset of FPLS outcomes to analyze")
    parser.add_argument("--force", action="store_true",
                        help="Rebuild outputs even if cached versions already exist")
    parser.add_argument("--skip-analysis", action="store_true",
                        help="Stop after the FPLS decomposition stage")
    parser.add_argument("--skip-significance", action="store_true",
                        help="Skip the FPLS mode-selection (Phase 1) step")
    args = parser.parse_args()

    months = args.months if args.months else list(DEFAULT_MONTHS)
    build_pipeline(
        year=args.year,
        months=months,
        force_rebuild=args.force,
        run_analysis=not args.skip_analysis,
        depvars=args.depvars,
        run_significance=not args.skip_significance,
    )


if __name__ == "__main__":
    main()
