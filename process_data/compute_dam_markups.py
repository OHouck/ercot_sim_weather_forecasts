"""compute_dam_markups.py — Compute DAM markup panel for ERCOT thermal generators.

Follows Woerman (2023) §4.2: markup = offer_price(Q) - marginal_cost, where Q is
evaluated at 65%, 75%, and 85% of HSL. Marginal cost uses unit-level heat rates from
the CEMS-ERCOT crosswalk (or tech defaults) and daily fuel prices from EIA.

Fuel price by resource type:
  Gas (CCGT90, CCLE90, CCGT00, CCLE00, SCGT90, SCLE90, GSREH, GSNONR, GSSUP):
      TX daily adjusted gas price = HH daily + additive TX/HH basis spread
  Coal/Lignite (CLLIG, COAL, STEAM): TX monthly delivered coal cost
  Nuclear (NUC): fixed $0.50/MMBtu uranium cost

Offer price is read from the 10-step ERCOT step-function offer curve.
For query quantity Q, the price is Price_i at the first step where MW_i >= Q.
When Q exceeds all submitted steps, the last valid price is used.

Output:
  {processed}/dam_markups_{year}.parquet — one row per (Resource Name, Delivery Date, Hour Ending)
"""

import sys
import argparse
import numpy as np
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from helper_funcs import setup_directories
from process_data.cems_ercot_crosswalk import build_crosswalk, THERMAL_RESOURCE_TYPES

COAL_TYPES = {"CLLIG", "COAL", "STEAM"}
NUCLEAR_TYPES = {"NUC"}

DISTRIBUTION_COST = 0.10  # $/MMBtu pipeline/transport margin (Woerman)
NUCLEAR_FUEL_PRICE = 0.50  # $/MMBtu (uranium cost approximation)
QUANTILE_LEVELS = [0.65, 0.75, 0.85]

MW_COLS = [f"QSE submitted Curve-MW{i}" for i in range(1, 11)]
PRICE_COLS = [f"QSE submitted Curve-Price{i}" for i in range(1, 11)]

KEEP_COLS = [
    "Resource Name", "Resource Type", "Delivery Date", "Hour Ending",
    "Settlement Point Name", "HSL", "LSL", "Awarded Quantity",
    "Energy Settlement Point Price", "Resource Status",
    "heat_rate_mmbtu_mwh", "heat_rate_source",
    "fuel_price_mmbtu", "marginal_cost",
    "offer_price_p65", "markup_p65",
    "offer_price_p75", "markup_p75",
    "offer_price_p85", "markup_p85",
    "in_market",
]


def _interpolate_step_curve(
    mw_arr: np.ndarray, price_arr: np.ndarray, q: np.ndarray
) -> np.ndarray:
    """Evaluate the 10-step ERCOT offer curve at query quantities.

    The curve is a right-step function: Price_i applies when Q <= MW_i (first step
    that covers Q). NaN steps are treated as infinity. When Q exceeds all submitted
    steps, the last valid price is returned.

    Args:
        mw_arr: shape (n, 10) — MW breakpoints per step, NaN for unused steps
        price_arr: shape (n, 10) — price per step, NaN for unused steps
        q: shape (n,) — query quantity for each row (e.g., alpha × HSL)

    Returns:
        shape (n,) — offer price at each query quantity; NaN when curve is empty
    """
    mw_filled = np.where(np.isnan(mw_arr), np.inf, mw_arr)
    covers = mw_filled >= q[:, None]
    any_covers = np.any(covers, axis=1)
    idx = np.argmax(covers, axis=1)
    # When q exceeds all steps, fall back to last non-NaN step
    last_valid = np.sum(~np.isnan(mw_arr), axis=1) - 1
    last_valid = np.maximum(last_valid, 0)
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


def _assign_fuel_price(dam: pd.DataFrame, daily_prices: pd.DataFrame) -> pd.DataFrame:
    """Merge daily fuel prices and select the appropriate price per fuel type.

    Adds `fuel_price_mmbtu` column based on resource type (gas/coal/nuclear).

    Args:
        dam: thermal DAM rows with `Delivery Date` (datetime) and `Resource Type`
        daily_prices: complete daily price table (all calendar days, no NaN gas)

    Returns:
        DataFrame with `fuel_price_mmbtu` column added
    """
    prices_sub = daily_prices[["date", "tx_gas_daily_mmbtu", "tx_coal_mmbtu"]].rename(
        columns={"date": "_date"}
    )
    dam = dam.assign(_date=dam["Delivery Date"].dt.date).merge(
        prices_sub, on="_date", how="left"
    ).drop(columns=["_date"])

    is_coal = dam["Resource Type"].isin(COAL_TYPES)
    is_nuc = dam["Resource Type"].isin(NUCLEAR_TYPES)
    dam["fuel_price_mmbtu"] = dam["tx_gas_daily_mmbtu"]
    dam.loc[is_coal, "fuel_price_mmbtu"] = dam.loc[is_coal, "tx_coal_mmbtu"]
    dam.loc[is_nuc, "fuel_price_mmbtu"] = NUCLEAR_FUEL_PRICE
    return dam.drop(columns=["tx_gas_daily_mmbtu", "tx_coal_mmbtu"])


def compute_dam_markups_month(
    year: int,
    month: int,
    heat_rates: pd.DataFrame,
    daily_prices: pd.DataFrame,
    dirs: dict,
) -> pd.DataFrame:
    """Compute DAM markups for all thermal resources in one operating month.

    For each (Resource Name, Delivery Date, Hour Ending), evaluates the 10-step
    offer curve at Q = {65%, 75%, 85%} × HSL and computes markup = offer_price - MC.

    Args:
        year: operating year
        month: operating month 1..12
        heat_rates: resource_heat_rates parquet (from build_crosswalk)
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
    dam = dam[dam["Resource Type"].isin(THERMAL_RESOURCE_TYPES)].copy()
    print(f"  {len(dam):,} thermal rows, {dam['Resource Name'].nunique()} resources")

    # Merge heat rates (resource_name → Resource Name)
    hr_sub = heat_rates[
        ["resource_name", "heat_rate_mmbtu_mwh", "heat_rate_source"]
    ].rename(columns={"resource_name": "Resource Name"})
    dam = dam.merge(hr_sub, on="Resource Name", how="left") 

    n_missing = dam["heat_rate_mmbtu_mwh"].isna().sum()
    if n_missing:
        n_res = dam.loc[dam["heat_rate_mmbtu_mwh"].isna(), "Resource Name"].nunique()
        print(f"  WARNING: {n_missing:,} rows ({n_res} resources) missing heat rates")

    dam = _assign_fuel_price(dam, daily_prices)
    dam["marginal_cost"] = dam["heat_rate_mmbtu_mwh"] * dam["fuel_price_mmbtu"] + DISTRIBUTION_COST

    mw_arr = dam[MW_COLS].values.astype(float)
    price_arr = dam[PRICE_COLS].values.astype(float)

    for alpha in QUANTILE_LEVELS:
        label = f"p{int(alpha * 100)}"
        q = (dam["HSL"].values * alpha).astype(float)
        dam[f"offer_price_{label}"] = _interpolate_step_curve(mw_arr, price_arr, q)
        dam[f"markup_{label}"] = dam[f"offer_price_{label}"] - dam["marginal_cost"]

    dam["in_market"] = dam["Awarded Quantity"] > 0
    return dam[[c for c in KEEP_COLS if c in dam.columns]]


def compute_dam_markups(
    year: int,
    months: list[int] | None = None,
    force_rebuild: bool = False,
) -> pd.DataFrame:
    """Compute the full DAM markup panel for all thermal generators.

    Loads resource heat rates and fuel prices once, then processes each month.
    Caches output; use force_rebuild=True to regenerate.

    Args:
        year: operating year
        months: months to process (default: all 12)
        force_rebuild: overwrite cached output if True

    Returns:
        DataFrame with one row per (Resource Name, Delivery Date, Hour Ending)
    """
    dirs = setup_directories()
    out_path = Path(dirs["processed"]) / f"dam_markups_{year}.parquet"
    
    # uncomment to not force rebuild
    # if out_path.exists() and not force_rebuild: 
    #     print(f"  Cached: {out_path}")
    #     return pd.read_parquet(out_path)

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
        print(f"\n=== {year}-{month:02d} ===")
        df = compute_dam_markups_month(year, month, heat_rates, daily_prices, dirs)
        if not df.empty:
            frames.append(df)
            p75 = df["markup_p75"].dropna()
            print(f"  p75 markup: mean={p75.mean():.1f}, median={p75.median():.1f} $/MWh")

    if not frames:
        print("  No data found.")
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)
    result.to_parquet(out_path, index=False)
    print(f"\n  Saved {len(result):,} rows → {out_path}")
    return result


def main():
    """Compute ERCOT DAM markup panel for one year."""
    parser = argparse.ArgumentParser(description="Compute ERCOT DAM markup panel")
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--months", type=int, nargs="+", default=list(range(1, 13)))
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    print(f"\n=== DAM Markups {args.year} ===")
    df = compute_dam_markups(args.year, months=args.months, force_rebuild=args.force)

    if df.empty:
        return

    print(f"\n  Total rows:      {len(df):,}")
    print(f"  Resources:       {df['Resource Name'].nunique()}")
    print(f"  Days covered:    {df['Delivery Date'].nunique()}")
    print(f"  In-market rows:  {df['in_market'].sum():,}")

    print(f"\n  Markup at p75 ($/MWh) by Resource Type:")
    summary = (
        df.groupby("Resource Type")["markup_p75"]
        .agg(["count", "mean", "median", "std"])
        .round(1)
    )
    print(summary.to_string())

    print(f"\n  Heat rate source coverage:")
    print(df.drop_duplicates("Resource Name")["heat_rate_source"].value_counts().to_string())


if __name__ == "__main__":
    main()
