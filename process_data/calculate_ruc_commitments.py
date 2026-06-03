"""calculate_ruc_commitments.py — Derive RUC-committed generation from SCED disclosure.

ERCOT's Reliability Unit Commitment (RUC) process forces units online that the
market did not commit on its own. In the 60-Day SCED Disclosure "Gen Resource
Data" CSV, the ``Telemetered Resource Status`` column distinguishes these units:

  - ``ONRUC``     — the unit is online because ERCOT committed it via RUC.
  - ``ONOPTOUT``  — the unit was initially committed by ERCOT but is no longer
                    needed (the QSE opted out of the RUC instruction).

For each SCED timestep (5-minute interval) we sum ``Telemetered Net Output``
across all units in each status group, giving the total MW actually produced by
RUC-committed units and by opted-out units. This replaces the older NP3-764-CD
measure (which reported available HSL headroom, not committed quantity).

Output: {processed}/ruc_commitments/ruc_commitments_{year}.csv
  Columns: sced_time_step, ruc_deployment_mw, ruc_optout_deployment_mw

Usage:
    from process_data.calculate_ruc_commitments import compute_ruc_commitments
    df = compute_ruc_commitments(2025)
"""

import argparse
import io
import os
import sys
import zipfile
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from helper_funcs import setup_directories
from process_data.process_curtailment import _find_sced_folders

# RUC-related telemetered resource statuses to retain.
RUC_STATUS = "ONRUC"
OPTOUT_STATUS = "ONOPTOUT"

# Columns to extract from the Gen Resource Data CSV (saves memory).
USECOLS = [
    "SCED Time Stamp",
    "Telemetered Resource Status",
    "Telemetered Net Output ",  # Note trailing space in ERCOT's header
]


def _load_sced_status_month(year, month):
    """Load SCED Gen Resource Data status + output for one operating month.

    Navigates the nested ZIP structure (outer.zip -> inner.zip ->
    60d_SCED_Gen_Resource_Data-*.csv), reading only the timestamp, resource
    status, and telemetered output columns. No resource-type filter is applied
    (RUC commitments may be any unit type). Rows from adjacent operating months
    (release folders can span month boundaries) are dropped.

    Args:
        year: Operating year (int).
        month: Operating month (int, 1-12).

    Returns:
        DataFrame with columns: sced_time (datetime), status (str),
        telemetered_output (float MW).
    """
    sced_dirs = _find_sced_folders(year, month)
    frames = []

    for sced_dir in sced_dirs:
        for outer_name in sorted(os.listdir(sced_dir)):
            if not outer_name.endswith(".zip"):
                continue

            with zipfile.ZipFile(os.path.join(sced_dir, outer_name)) as outer_zip:
                for inner_name in sorted(outer_zip.namelist()):
                    if not inner_name.endswith(".zip"):
                        continue

                    with zipfile.ZipFile(io.BytesIO(outer_zip.read(inner_name))) as inner_zip:
                        gen_csvs = [
                            n for n in inner_zip.namelist()
                            if "Gen_Resource" in n and n.endswith(".csv")
                        ]
                        if not gen_csvs:
                            continue

                        csv_bytes = inner_zip.read(gen_csvs[0])
                        try:
                            df = pd.read_csv(io.BytesIO(csv_bytes), usecols=USECOLS)
                        except (ValueError, KeyError):
                            alt_cols = [c.rstrip() for c in USECOLS]
                            try:
                                df = pd.read_csv(io.BytesIO(csv_bytes), usecols=alt_cols)
                            except Exception as e:
                                print(f"    WARNING: could not read {gen_csvs[0]}: {e}")
                                continue

                        df.columns = df.columns.str.strip()
                        if len(df) > 0:
                            frames.append(df)

    if not frames:
        raise FileNotFoundError(
            f"No SCED Gen Resource Data found for {year}-{month:02d}"
        )

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.rename(columns={
        "SCED Time Stamp": "sced_time",
        "Telemetered Resource Status": "status",
        "Telemetered Net Output": "telemetered_output",
    })
    combined["sced_time"] = pd.to_datetime(
        combined["sced_time"], format="mixed", dayfirst=False
    )
    combined["telemetered_output"] = pd.to_numeric(
        combined["telemetered_output"], errors="coerce"
    )

    # Filter to the target operating month (folders may span adjacent months).
    n_before = len(combined)
    combined = combined[
        (combined["sced_time"].dt.year == year)
        & (combined["sced_time"].dt.month == month)
    ].copy()
    print(
        f"  Loaded {len(combined):,} SCED records for {year}-{month:02d} "
        f"({n_before - len(combined)} rows from adjacent months dropped)"
    )
    return combined[["sced_time", "status", "telemetered_output"]]


def compute_ruc_commitments(year, months=None, force_rebuild=False):
    """Compute per-SCED-timestep RUC and opt-out committed generation for a year.

    For each 5-minute SCED interval, sums ``Telemetered Net Output`` over units
    with status ONRUC (ruc_deployment_mw) and ONOPTOUT (ruc_optout_deployment_mw).

    Args:
        year: Integer year (e.g. 2025).
        months: List of operating months to include (default: all 12).
        force_rebuild: If True, recompute even if the cached CSV exists.

    Returns:
        DataFrame with columns: sced_time_step (datetime), ruc_deployment_mw
        (float), ruc_optout_deployment_mw (float); one row per SCED timestep.
    """
    if months is None:
        months = list(range(1, 13))

    dirs = setup_directories()
    cache_dir = os.path.join(dirs["processed"], "ruc_commitments")
    cache_path = os.path.join(cache_dir, f"ruc_commitments_{year}.csv")

    if os.path.exists(cache_path) and not force_rebuild:
        print(f"  Loading cached RUC commitments: {cache_path}")
        return pd.read_csv(cache_path, parse_dates=["sced_time_step"])

    monthly = []
    for month in months:
        print(f"\n=== RUC commitments {year}-{month:02d} ===")
        raw = _load_sced_status_month(year, month)
        ruc = raw[raw["status"].isin([RUC_STATUS, OPTOUT_STATUS])]

        # Sum output per timestep within each status group.
        wide = (
            ruc.pivot_table(
                index="sced_time",
                columns="status",
                values="telemetered_output",
                aggfunc="sum",
            )
            .rename(columns={
                RUC_STATUS: "ruc_deployment_mw",
                OPTOUT_STATUS: "ruc_optout_deployment_mw",
            })
        )
        # reindex adds any status column absent in the month; fillna covers both
        # the added columns and timesteps where only one status was present.
        wide = wide.reindex(
            columns=["ruc_deployment_mw", "ruc_optout_deployment_mw"]
        ).fillna(0.0)
        monthly.append(wide.reset_index())

    result = (
        pd.concat(monthly, ignore_index=True)
        .rename(columns={"sced_time": "sced_time_step"})
        .sort_values("sced_time_step")
        .reset_index(drop=True)
    )

    os.makedirs(cache_dir, exist_ok=True)
    result.to_csv(cache_path, index=False)

    print(f"\nSaved: {cache_path}")
    print(f"  Timesteps: {len(result):,}")
    print(
        f"  ruc_deployment_mw:        mean={result['ruc_deployment_mw'].mean():.1f}, "
        f"max={result['ruc_deployment_mw'].max():.1f}"
    )
    print(
        f"  ruc_optout_deployment_mw: mean={result['ruc_optout_deployment_mw'].mean():.1f}, "
        f"max={result['ruc_optout_deployment_mw'].max():.1f}"
    )
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Compute per-SCED-timestep RUC and opt-out committed generation"
    )
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--months", type=int, nargs="+", default=None,
                        help="Operating months to include (default: all 12)")
    parser.add_argument("--force", action="store_true",
                        help="Overwrite cached output")
    args = parser.parse_args()

    compute_ruc_commitments(args.year, months=args.months, force_rebuild=args.force)
