"""compute_markups.py — Compute DAM and real-time markup panels for ERCOT thermal generators.

Follows Woerman (2023) §4.2: markup = offer_price(Q) - marginal_cost, where Q is
evaluated at quantiles {10%, 30%, 50%, 65%, 75%, 85%} of HSL.

DAM: reads 10-step QSE submitted offer curves from DAM disclosure parquets.
RT:  reads 35-step SCED1 Curve from 60-Day SCED Disclosure; 5-minute intervals
     are aggregated to hourly before saving.

Both markets use the same MC: heat_rate × fuel_price + distribution_cost (gas only).
  Gas: TX daily adjusted price = HH daily + TX/HH basis spread
  Coal/Lignite: TX monthly delivered coal cost

XX TODO: NOx/SO2 permit prices; load-zone gas price differentiation
    - Woerman uses $0.0006/lb SO2 and $0.18/lb NOx per Mcf of gas from EPA eGRID
      technical support documentation (EPA, 2018).

Outputs:
  {processed}/dam_markups_{year}.parquet — one row per (Resource Name, Delivery Date, Hour Ending)
  {processed}/rt_markups_{year}.parquet  — one row per (Resource Name, valid_time [hourly])
"""

import io
import os
import sys
import zipfile
import argparse
import numpy as np
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from helper_funcs import setup_directories, THERMAL_RESOURCE_TYPES
from process_data.cems_ercot_crosswalk import build_crosswalk
from process_data.process_curtailment import _find_sced_folders


COAL_TYPES = {"CLLIG"}
SCGT_TYPES = {"SCGT90", "SCLE90"}
CCGT_TYPES = {"CCGT90", "CCLE90"}
GS_TYPES = {"GSREH", "GSNONR", "GSSUP"}

DISTRIBUTION_COST = 0.10  # $/MMBtu pipeline/transport margin for gas (Woerman)
QUANTILE_LEVELS = [0.1, 0.3, 0.5, 0.65, 0.75, 0.85]

# DAM offer curve (10 steps)
DAM_MW_COLS = [f"QSE submitted Curve-MW{i}" for i in range(1, 11)]
DAM_PRICE_COLS = [f"QSE submitted Curve-Price{i}" for i in range(1, 11)]

# SCED1 offer curve (35 steps)
SCED_MW_COLS = [f"SCED1 Curve-MW{i}" for i in range(1, 36)]
SCED_PRICE_COLS = [f"SCED1 Curve-Price{i}" for i in range(1, 36)]

_SCED_USECOLS = [
    "SCED Time Stamp", "Resource Name", "Resource Type",
    "HSL", "LSL", "Telemetered Net Output ", "Telemetered Resource Status",
    *SCED_MW_COLS, *SCED_PRICE_COLS,
]

_MARKUP_COLS = [
    col
    for q in QUANTILE_LEVELS
    for col in (f"offer_price_p{int(q * 100)}", f"markup_p{int(q * 100)}")
]

DAM_KEEP_COLS = [
    "Resource Name", "Resource Type", "Delivery Date", "Hour Ending",
    "Settlement Point Name", "HSL", "LSL", "Awarded Quantity",
    "Energy Settlement Point Price", "Resource Status",
    "heat_rate_mmbtu_mwh", "heat_rate_source",
    "fuel_price_mmbtu", "marginal_cost",
    *_MARKUP_COLS,
    "in_market",
]

RT_KEEP_COLS = [
    "Resource Name", "Resource Type", "valid_time",
    "HSL", "LSL", "telemetered_output_mw",
    "Telemetered Resource Status",
    "heat_rate_mmbtu_mwh", "heat_rate_source",
    "fuel_price_mmbtu", "marginal_cost",
    *_MARKUP_COLS,
    "in_market", "n_sced_intervals",
]


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _interpolate_step_curve(
    mw_arr: np.ndarray, price_arr: np.ndarray, q: np.ndarray
) -> np.ndarray:
    """Evaluate a right-step offer curve at query quantities.

    Price_i applies when Q <= MW_i (first step that covers Q). NaN steps are
    treated as infinity. When Q exceeds all steps, the last valid price is returned.

    Args:
        mw_arr: shape (n, k) — MW breakpoints per step; NaN for unused steps
        price_arr: shape (n, k) — price per step; NaN for unused steps
        q: shape (n,) — query quantity per row

    Returns:
        shape (n,) — offer price; NaN when the curve is entirely empty
    """
    mw_filled = np.where(np.isnan(mw_arr), np.inf, mw_arr)
    covers = mw_filled >= q[:, None]
    any_covers = np.any(covers, axis=1)
    idx = np.argmax(covers, axis=1)
    last_valid = np.maximum(np.sum(~np.isnan(mw_arr), axis=1) - 1, 0)
    idx = np.where(any_covers, idx, last_valid)
    prices = price_arr[np.arange(len(q)), idx]
    prices[np.all(np.isnan(price_arr), axis=1)] = np.nan
    return prices


def _build_complete_daily_prices(fuel_prices: pd.DataFrame, year: int) -> pd.DataFrame:
    """Expand fuel prices to all calendar days, forward-filling non-trading days.

    Henry Hub is only quoted on trading days; weekends/holidays get the prior
    trading day's price. Coal is monthly so it is already fully populated.

    Args:
        fuel_prices: daily table from build_fuel_price_tables (trading days only)
        year: calendar year

    Returns:
        DataFrame with one row per calendar day and no NaN gas prices
    """
    all_dates = pd.DataFrame(
        {"date": pd.date_range(f"{year}-01-01", f"{year}-12-31", freq="D").date}
    )
    complete = all_dates.merge(fuel_prices, on="date", how="left")
    for col in ["tx_gas_daily_mmbtu", "hh_price_mmbtu", "tx_coal_mmbtu"]:
        complete[col] = complete[col].ffill().bfill()
    return complete


def _assign_fuel_price(df: pd.DataFrame, daily_prices: pd.DataFrame) -> pd.DataFrame:
    """Merge daily fuel prices and select the appropriate price per resource type.

    Expects a `_date` column (Python date object) on `df` for the merge key.
    Adds `fuel_price_mmbtu` and drops the intermediate price columns.

    Args:
        df: thermal rows with `_date` and `Resource Type`
        daily_prices: complete daily price table (all calendar days, no NaN gas)

    Returns:
        df with `fuel_price_mmbtu` added; `_date`, `tx_gas_daily_mmbtu`,
        `tx_coal_mmbtu` dropped
    """
    prices_sub = daily_prices[["date", "tx_gas_daily_mmbtu", "tx_coal_mmbtu"]].rename(
        columns={"date": "_date"}
    )
    df = df.merge(prices_sub, on="_date", how="left").drop(columns=["_date"])
    is_coal = df["Resource Type"].isin(COAL_TYPES)
    df["fuel_price_mmbtu"] = df["tx_gas_daily_mmbtu"]
    df.loc[is_coal, "fuel_price_mmbtu"] = df.loc[is_coal, "tx_coal_mmbtu"]
    return df.drop(columns=["tx_gas_daily_mmbtu", "tx_coal_mmbtu"])


# ---------------------------------------------------------------------------
# DAM pipeline
# ---------------------------------------------------------------------------

def compute_dam_markups_month(
    year: int,
    month: int,
    heat_rates: pd.DataFrame,
    daily_prices: pd.DataFrame,
    dirs: dict,
) -> pd.DataFrame:
    """Compute DAM markups for all thermal resources in one operating month.

    For each (Resource Name, Delivery Date, Hour Ending), evaluates the 10-step
    QSE submitted offer curve at Q = alpha × HSL for each quantile level and
    computes markup = offer_price - MC.

    Args:
        year: operating year
        month: operating month 1..12
        heat_rates: resource heat rates from build_crosswalk
        daily_prices: complete daily fuel prices (all calendar days)
        dirs: directory dict from setup_directories()

    Returns:
        DataFrame with markup panel for the month; empty if DAM data is missing
    """
    path = (
        Path(dirs["raw"])
        / "ercot" / "dam_disclosure" / str(year) / f"{month:02d}"
        / f"dam_gen_resource_{year}{month:02d}.parquet"
    )
    if not path.exists():
        print(f"  Missing: {path.name}")
        return pd.DataFrame()

    dam = pd.read_parquet(path)

    # summary table of % of rows and unique resources by Resource Type
    summary = dam.groupby("Resource Type").agg(
        n_rows=("Resource Name", "size"),
        total_capacity_mw=("HSL", "sum"),
        n_resources=("Resource Name", "nunique"),
    )
    summary["% rows"] = 100 * summary["n_rows"] / len(dam)
    summary["% resources"] = 100 * summary["n_resources"] / dam["Resource Name"].nunique()
    summary["% capacity"] = 100 * summary["total_capacity_mw"] / dam["HSL"].sum()
    print("  Resource Type summary:")
    # sort by % capacity, then % resources, then % rows
    summary = summary.sort_values(["% capacity", "% resources", "% rows"], ascending=False)
    print(summary[["% rows", "% resources", "% capacity"]].to_string(float_format="%.1f"))



    dam = dam[dam["Resource Type"].isin(THERMAL_RESOURCE_TYPES)].copy()

    # summary table of % of rows and unique resources by Resource Type
    summary = dam.groupby("Resource Type").agg(
        n_rows=("Resource Name", "size"),
        total_capacity_mw=("HSL", "sum"),
        n_resources=("Resource Name", "nunique"),
    )
    summary["% rows"] = 100 * summary["n_rows"] / len(dam)
    summary["% resources"] = 100 * summary["n_resources"] / dam["Resource Name"].nunique()
    summary["% capacity"] = 100 * summary["total_capacity_mw"] / dam["HSL"].sum()
    print("  Resource Type summary:")
    summary = summary.sort_values(["% capacity", "% resources", "% rows"], ascending=False)
    print(summary[["% rows", "% resources", "% capacity"]].to_string(float_format="%.1f"))

    hr_sub = heat_rates[["resource_name", "heat_rate_mmbtu_mwh", "heat_rate_source"]].rename(
        columns={"resource_name": "Resource Name"}
    )
    dam = dam.merge(hr_sub, on="Resource Name", how="left")

    dam_resources = dam.drop_duplicates("Resource Name")
    print("  Heat-rate source coverage by unique Resource Name:")
    print(dam_resources["heat_rate_source"].value_counts(dropna=False).to_string())

    n_missing = dam["heat_rate_mmbtu_mwh"].isna().sum()
    if n_missing:
        n_res = dam.loc[dam["heat_rate_mmbtu_mwh"].isna(), "Resource Name"].nunique()
        print(f"  WARNING: {n_missing:,} rows ({n_res} resources) missing heat rates")

    dam = dam.assign(_date=dam["Delivery Date"].dt.date)
    dam = _assign_fuel_price(dam, daily_prices)
    dam["marginal_cost"] = dam["heat_rate_mmbtu_mwh"] * dam["fuel_price_mmbtu"]
    dam.loc[~dam["Resource Type"].isin(COAL_TYPES), "marginal_cost"] += DISTRIBUTION_COST

    mw_arr = dam[DAM_MW_COLS].values.astype(float)
    price_arr = dam[DAM_PRICE_COLS].values.astype(float)
    for alpha in QUANTILE_LEVELS:
        label = f"p{int(alpha * 100)}"
        q = (dam["HSL"].values * alpha).astype(float)
        dam[f"offer_price_{label}"] = _interpolate_step_curve(mw_arr, price_arr, q)
        dam[f"markup_{label}"] = dam[f"offer_price_{label}"] - dam["marginal_cost"]

    dam["in_market"] = dam["Awarded Quantity"] > 0
    return dam[[c for c in DAM_KEEP_COLS if c in dam.columns]]


def compute_dam_markups(
    year: int,
    months: list[int] | None = None,
    force_rebuild: bool = False,
) -> pd.DataFrame:
    """Compute the full DAM markup panel for all thermal generators.

    Loads resource heat rates and fuel prices once, then processes each month.
    Caches output to {processed}/dam_markups_{year}.parquet.

    Args:
        year: operating year
        months: months to process (default: all 12)
        force_rebuild: overwrite cached output if True

    Returns:
        DataFrame with one row per (Resource Name, Delivery Date, Hour Ending)
    """
    dirs = setup_directories()
    out_path = Path(dirs["processed"]) / f"dam_markups_{year}.parquet"
    if out_path.exists() and not force_rebuild:
        print(f"  Cached: {out_path}")
        return pd.read_parquet(out_path)

    if months is None:
        months = list(range(1, 13))

    print(f"  Loading resource heat rates for {year}...")
    _, heat_rates = build_crosswalk(year)

    print(f"  Loading and expanding fuel prices for {year}...")
    raw_prices = pd.read_parquet(
        Path(dirs["processed"]) / f"fuel_prices_daily_{year}.parquet"
    )
    daily_prices = _build_complete_daily_prices(raw_prices, year)

    frames = []
    for month in months:
        print(f"\n=== DAM {year}-{month:02d} ===")
        df = compute_dam_markups_month(year, month, heat_rates, daily_prices, dirs)
        if not df.empty:
            frames.append(df)
            p75 = df["markup_p75"].dropna()
            print(f"  p75 markup: mean={p75.mean():.1f}, median={p75.median():.1f} $/MWh")

    if not frames:
        print("  No DAM data found.")
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)
    result.to_parquet(out_path, index=False)
    print(f"\n  Saved {len(result):,} rows → {out_path}")
    return result


# ---------------------------------------------------------------------------
# RT pipeline
# ---------------------------------------------------------------------------

def _load_sced_for_markups(year: int, month: int) -> pd.DataFrame:
    """Load SCED Gen Resource Data for thermal units with SCED1 Curve columns.

    Navigates the nested ZIP structure (outer.zip → inner.zip → CSV) and
    reads only the columns needed for markup computation. Filters to thermal
    resource types and the target operating month.

    Args:
        year: operating year
        month: operating month 1..12

    Returns:
        DataFrame with columns: sced_time, valid_time, Resource Name,
        Resource Type, HSL, LSL, telemetered_output_mw,
        Telemetered Resource Status, SCED1 Curve-MW1..35, SCED1 Curve-Price1..35
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
                            df = pd.read_csv(
                                io.BytesIO(csv_bytes),
                                usecols=_SCED_USECOLS,
                                dtype={"Resource Type": str},
                            )
                        except (ValueError, KeyError):
                            alt_cols = [c.rstrip() for c in _SCED_USECOLS]
                            try:
                                df = pd.read_csv(
                                    io.BytesIO(csv_bytes),
                                    usecols=alt_cols,
                                    dtype={"Resource Type": str},
                                )
                            except Exception as e:
                                print(f"    WARNING: could not read {gen_csvs[0]}: {e}")
                                continue

                        df.columns = df.columns.str.strip()
                        df = df[df["Resource Type"].isin(THERMAL_RESOURCE_TYPES)].copy()
                        if len(df) > 0:
                            frames.append(df)

    if not frames:
        raise FileNotFoundError(
            f"No SCED Gen Resource Data found for {year}-{month:02d}"
        )

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.rename(columns={
        "SCED Time Stamp": "sced_time",
        "Telemetered Net Output": "telemetered_output_mw",
    })
    combined["sced_time"] = pd.to_datetime(combined["sced_time"], format="mixed", dayfirst=False)
    numeric_cols = [c for c in ["HSL", "LSL", "telemetered_output_mw", *SCED_MW_COLS, *SCED_PRICE_COLS] if c in combined.columns]
    combined[numeric_cols] = combined[numeric_cols].apply(pd.to_numeric, errors="coerce")

    combined["valid_time"] = combined["sced_time"].dt.floor("h")

    # Filter to target operating month (release folders can span adjacent months)
    n_before = len(combined)
    combined = combined[
        (combined["sced_time"].dt.year == year)
        & (combined["sced_time"].dt.month == month)
    ].copy()
    print(
        f"  Loaded {len(combined):,} thermal SCED intervals for {year}-{month:02d} "
        f"({n_before - len(combined)} rows from adjacent months dropped)"
    )
    return combined


def compute_rt_markups_month(
    year: int,
    month: int,
    heat_rates: pd.DataFrame,
    daily_prices: pd.DataFrame,
) -> pd.DataFrame:
    """Compute RT markups for all thermal resources in one operating month.

    For each 5-minute SCED interval, evaluates the 35-step SCED1 Curve at
    Q = alpha × HSL and computes markup = offer_price - MC. Intervals are then
    collapsed to hourly by keeping the first interval observed within each
    (Resource Name, hour).

    Args:
        year: operating year
        month: operating month 1..12
        heat_rates: resource heat rates from build_crosswalk
        daily_prices: complete daily fuel prices (all calendar days)

    Returns:
        DataFrame with hourly markup panel for the month; empty if SCED data missing
    """
    try:
        sced = _load_sced_for_markups(year, month)
    except FileNotFoundError as e:
        print(f"  Missing SCED data: {e}")
        return pd.DataFrame()

    print(f"  {len(sced):,} thermal intervals, {sced['Resource Name'].nunique()} resources")

    hr_sub = heat_rates[["resource_name", "heat_rate_mmbtu_mwh", "heat_rate_source"]].rename(
        columns={"resource_name": "Resource Name"}
    )
    sced = sced.merge(hr_sub, on="Resource Name", how="left")

    n_missing = sced["heat_rate_mmbtu_mwh"].isna().sum()
    if n_missing:
        n_res = sced.loc[sced["heat_rate_mmbtu_mwh"].isna(), "Resource Name"].nunique()
        print(f"  WARNING: {n_missing:,} intervals ({n_res} resources) missing heat rates")

    sced = sced.assign(_date=sced["valid_time"].dt.date)
    sced = _assign_fuel_price(sced, daily_prices)
    sced["marginal_cost"] = sced["heat_rate_mmbtu_mwh"] * sced["fuel_price_mmbtu"]
    sced.loc[~sced["Resource Type"].isin(COAL_TYPES), "marginal_cost"] += DISTRIBUTION_COST

    mw_arr = sced[SCED_MW_COLS].values.astype(float)
    price_arr = sced[SCED_PRICE_COLS].values.astype(float)
    for alpha in QUANTILE_LEVELS:
        label = f"p{int(alpha * 100)}"
        q = (sced["HSL"].values * alpha).astype(float)
        sced[f"offer_price_{label}"] = _interpolate_step_curve(mw_arr, price_arr, q)
        sced[f"markup_{label}"] = sced[f"offer_price_{label}"] - sced["marginal_cost"]

    sced["in_market"] = sced["telemetered_output_mw"] > 0

    # Keep the first SCED interval per (resource, hour), and retain interval counts.
    interval_counts = (
        sced.groupby(["Resource Name", "valid_time"])["sced_time"]
        .nunique()
        .rename("n_sced_intervals")
        .reset_index()
    )
    hourly = (
        sced.sort_values(["Resource Name", "valid_time", "sced_time"])
        .drop_duplicates(subset=["Resource Name", "valid_time"], keep="first")
        .merge(interval_counts, on=["Resource Name", "valid_time"], how="left")
    )

    return hourly[[c for c in RT_KEEP_COLS if c in hourly.columns]]


def compute_rt_markups(
    year: int,
    months: list[int] | None = None,
    force_rebuild: bool = False,
) -> pd.DataFrame:
    """Compute the full RT markup panel for all thermal generators.

    Loads resource heat rates and fuel prices once, then processes each month.
    Caches output to {processed}/rt_markups_{year}.parquet.

    Args:
        year: operating year
        months: months to process (default: all 12)
        force_rebuild: overwrite cached output if True

    Returns:
        DataFrame with one row per (Resource Name, valid_time [hourly])
    """
    dirs = setup_directories()
    out_path = Path(dirs["processed"]) / f"rt_markups_{year}.parquet"
    if out_path.exists() and not force_rebuild:
        print(f"  Cached: {out_path}")
        return pd.read_parquet(out_path)

    if months is None:
        months = list(range(1, 13))

    print(f"  Loading resource heat rates for {year}...")
    _, heat_rates = build_crosswalk(year)

    print(f"  Loading and expanding fuel prices for {year}...")
    raw_prices = pd.read_parquet(
        Path(dirs["processed"]) / f"fuel_prices_daily_{year}.parquet"
    )
    daily_prices = _build_complete_daily_prices(raw_prices, year)

    frames = []
    for month in months:
        print(f"\n=== RT {year}-{month:02d} ===")
        df = compute_rt_markups_month(year, month, heat_rates, daily_prices)
        if not df.empty:
            frames.append(df)
            p75 = df["markup_p75"].dropna()
            print(f"  p75 markup: mean={p75.mean():.1f}, median={p75.median():.1f} $/MWh")

    if not frames:
        print("  No RT data found.")
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)
    result.to_parquet(out_path, index=False)
    print(f"\n  Saved {len(result):,} rows → {out_path}")
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    """Compute ERCOT markup panels for one year (DAM, RT, or both)."""
    parser = argparse.ArgumentParser(description="Compute ERCOT markup panels")
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--months", type=int, nargs="+", default=list(range(1, 13)))
    parser.add_argument("--market", choices=["dam", "rt", "both"], default="both")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    def _print_summary(label: str, df: pd.DataFrame) -> None:
        if df.empty:
            return
        print(f"\n  {label} total rows:   {len(df):,}")
        print(f"  {label} resources:    {df['Resource Name'].nunique()}")
        print(f"\n  {label} markup at p75 ($/MWh) by Resource Type:")
        summary = (
            df.groupby("Resource Type")["markup_p75"]
            .agg(["count", "mean", "median", "std"])
            .round(1)
        )
        print(summary.to_string())
        print(f"\n  {label} heat-rate coverage:")
        print(
            df.drop_duplicates("Resource Name")["heat_rate_source"]
            .value_counts()
            .to_string()
        )

    if args.market in ("dam", "both"):
        print(f"\n=== DAM Markups {args.year} ===")
        dam_df = compute_dam_markups(args.year, months=args.months, force_rebuild=True)
        _print_summary("DAM", dam_df)

    if args.market in ("rt", "both"):
        print(f"\n=== RT Markups {args.year} ===")
        rt_df = compute_rt_markups(args.year, months=args.months, force_rebuild=True)
        _print_summary("RT", rt_df)


if __name__ == "__main__":
    main()
