"""pull_fuel_prices.py — Download daily natural gas and monthly coal prices for Texas (2025).

Pulls fuel prices used to estimate marginal costs of ERCOT thermal generators:

  1. Henry Hub daily spot price ($/MMBtu) from EIA API series NG.RNGWHHD.D.
  2. Texas natural gas price sold to electric power consumers ($/MMBtu, monthly)
     from EIA series N3045TX3. Used to compute a monthly basis spread so that
     HH daily prices are adjusted to reflect actual Texas hub prices.
  3. Texas coal price to electric power ($/MMBtu, monthly) from EIA electricity
     operational data (sector 1, fuel COW).

Texas-adjusted daily natural gas price = HH_daily × (TX_monthly / HH_monthly).
Coal prices are monthly averages applied to all operating days in that month.

Requires ~/keys/eia_api_key.txt (free registration at https://www.eia.gov/opendata/).

Output:
  {processed}/fuel_prices_daily_{year}.parquet   — one row per calendar day
  {processed}/fuel_prices_monthly_{year}.parquet — one row per month
"""

import sys
import argparse
import requests
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from helper_funcs import setup_directories

EIA_BASE = "https://api.eia.gov/v2/seriesid"
EIA_ELEC_BASE = "https://api.eia.gov/v2/electricity/electric-power-operational-data/data/"

MCF_TO_MMBTU = 1.02  # 1 MCF natural gas ≈ 1.02 MMBtu (nominal conversion)


def _load_eia_key():
    """Load EIA API key from ~/keys/eia_api_key.txt."""
    return (Path.home() / "keys" / "eia_api_key.txt").read_text().strip()


def _fetch_series(series_id, start, end, api_key, length=600):
    """Fetch an EIA v2 time series and return a list of (period, value) dicts.

    The /v2/seriesid/ endpoint sorts by most-recent-first and ignores start/end
    filters, so we request `length` records and filter client-side to [start, end].
    Use length=20 for monthly series to avoid fetching years of history.

    Args:
        series_id: EIA series ID string (e.g. 'NG.RNGWHHD.D')
        start: start date/month string (e.g. '2025-01-01' or '2025-01')
        end: end date/month string
        api_key: EIA API key
        length: max records to fetch (default 600, use 20 for monthly series)

    Returns:
        list of dicts with 'period' and 'value' keys, sorted ascending by period
    """
    r = requests.get(
        f"{EIA_BASE}/{series_id}",
        params={"api_key": api_key, "length": length},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()["response"]["data"]
    return sorted(
        [d for d in data if start <= d["period"] <= end],
        key=lambda x: x["period"],
    )


def download_henry_hub_daily(year, api_key):
    """Fetch Henry Hub daily spot price for the full year.

    Args:
        year: calendar year (int)
        api_key: EIA API key

    Returns:
        DataFrame with columns: date (date), hh_price_mmbtu (float)
    """
    rows = _fetch_series("NG.RNGWHHD.D", f"{year}-01-01", f"{year}-12-31", api_key)
    df = pd.DataFrame(rows)[["period", "value"]]
    df["date"] = pd.to_datetime(df["period"]).dt.date
    df["hh_price_mmbtu"] = pd.to_numeric(df["value"], errors="coerce")
    return df[["date", "hh_price_mmbtu"]].dropna()


def download_tx_gas_monthly(year, api_key):
    """Fetch Texas natural gas price sold to electric power consumers (monthly).

    Series N3045TX3 is reported in $/MCF; converted to $/MMBtu.

    Args:
        year: calendar year (int)
        api_key: EIA API key

    Returns:
        DataFrame with columns: month (Period[M]), tx_gas_mmbtu (float)
    """
    rows = _fetch_series("NG.N3045TX3.M", f"{year}-01", f"{year}-12", api_key, length=20)
    df = pd.DataFrame(rows)[["period", "value"]]
    df["month"] = pd.to_datetime(df["period"]).dt.to_period("M")
    df["tx_gas_mmbtu"] = pd.to_numeric(df["value"], errors="coerce") / MCF_TO_MMBTU
    return df[["month", "tx_gas_mmbtu"]].dropna()


def download_tx_coal_monthly(year, api_key):
    """Fetch Texas coal cost delivered to electric power (monthly, $/MMBtu).

    Uses EIA electricity operational data (sector 1 = Electric Utility, fuel COW).

    Args:
        year: calendar year (int)
        api_key: EIA API key

    Returns:
        DataFrame with columns: month (Period[M]), tx_coal_mmbtu (float)
    """
    r = requests.get(
        EIA_ELEC_BASE,
        params={
            "api_key": api_key,
            "frequency": "monthly",
            "data[0]": "cost-per-btu",
            "facets[location][]": "TX",
            "facets[sectorid][]": "1",
            "facets[fueltypeid][]": "COW",
            "start": f"{year}-01",
            "end": f"{year}-12",
            "length": 20,
        },
        timeout=30,
    )
    r.raise_for_status()
    rows = r.json()["response"]["data"]
    df = pd.DataFrame(rows)[["period", "cost-per-btu"]]
    df["month"] = pd.to_datetime(df["period"]).dt.to_period("M")
    df["tx_coal_mmbtu"] = pd.to_numeric(df["cost-per-btu"], errors="coerce")
    return df[["month", "tx_coal_mmbtu"]].dropna().sort_values("month")


def build_fuel_price_tables(year, force_rebuild=False):
    """Download and merge all fuel price series into daily and monthly tables.

    Natural gas daily price = HH daily × (TX monthly / HH monthly), so that
    day-to-day variation tracks Henry Hub while the level reflects actual
    Texas electric power prices.

    Args:
        year: calendar year (int)
        force_rebuild: overwrite cached files if True (bool)

    Returns:
        tuple of (daily_df, monthly_df)
          daily_df: date, hh_price_mmbtu, tx_gas_mmbtu, tx_coal_mmbtu
          monthly_df: month, hh_avg_mmbtu, tx_gas_mmbtu, tx_coal_mmbtu, basis_spread
    """
    dirs = setup_directories()
    daily_path = Path(dirs["processed"]) / f"fuel_prices_daily_{year}.parquet"
    monthly_path = Path(dirs["processed"]) / f"fuel_prices_monthly_{year}.parquet"

    if daily_path.exists() and monthly_path.exists() and not force_rebuild:
        print(f"  Cached: {daily_path.name}, {monthly_path.name}")
        return pd.read_parquet(daily_path), pd.read_parquet(monthly_path)

    api_key = _load_eia_key()
    print(f"  Fetching Henry Hub daily {year}...")
    hh = download_henry_hub_daily(year, api_key)
    print(f"    {len(hh)} trading days")

    print(f"  Fetching TX gas monthly {year}...")
    tx_gas = download_tx_gas_monthly(year, api_key)
    print(f"    {len(tx_gas)} months: {tx_gas['tx_gas_mmbtu'].describe().round(3).to_dict()}")

    print(f"  Fetching TX coal monthly {year}...")
    tx_coal = download_tx_coal_monthly(year, api_key)
    print(f"    {len(tx_coal)} months: {tx_coal['tx_coal_mmbtu'].describe().round(3).to_dict()}")

    hh_with_month = hh.assign(month=pd.to_datetime(hh["date"]).dt.to_period("M"))
    hh_monthly_avg = hh_with_month.groupby("month")["hh_price_mmbtu"].mean().rename("hh_avg_mmbtu").reset_index()

    monthly = (
        hh_monthly_avg
        .merge(tx_gas, on="month", how="left")
        .merge(tx_coal, on="month", how="left")
    )
    monthly["basis_spread"] = monthly["tx_gas_mmbtu"] - monthly["hh_avg_mmbtu"]
    monthly.to_parquet(monthly_path, index=False)
    print(f"  Saved monthly table: {monthly_path}")

    daily = hh_with_month.merge(monthly[["month", "tx_gas_mmbtu", "tx_coal_mmbtu", "basis_spread"]], on="month", how="left")
    # additive basis is more stable than multiplicative when HH is near zero
    daily["tx_gas_daily_mmbtu"] = daily["hh_price_mmbtu"] + daily["basis_spread"].fillna(0)
    daily = daily[["date", "hh_price_mmbtu", "tx_gas_daily_mmbtu", "tx_coal_mmbtu"]]
    daily.to_parquet(daily_path, index=False)
    print(f"  Saved daily table: {daily_path}")

    return daily, monthly


def main():
    """Download fuel price data for the specified year."""
    parser = argparse.ArgumentParser(description="Download EIA fuel prices for Texas")
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    print(f"\n=== Fuel prices for {args.year} ===")
    daily, monthly = build_fuel_price_tables(args.year, force_rebuild=args.force)

    print(f"\n  Monthly natural gas prices (TX, $/MMBtu):")
    print(monthly[["month", "hh_avg_mmbtu", "tx_gas_mmbtu", "basis_spread"]].to_string(index=False))
    print(f"\n  Monthly coal prices (TX, $/MMBtu):")
    print(monthly[["month", "tx_coal_mmbtu"]].to_string(index=False))
    print(f"\n  Daily gas: {len(daily)} days, mean HH={daily['hh_price_mmbtu'].mean():.2f} $/MMBtu")


if __name__ == "__main__":
    main()
