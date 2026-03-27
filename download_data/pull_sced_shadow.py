"""pull_sced_shadow.py — Download SCED shadow prices and binding constraints.

Downloads hourly binding transmission constraint data from the ERCOT Public API
(Report NP6-86-CD). Shadow prices represent the marginal cost of congestion on
each binding transmission element.

Output: {raw}/ercot/sced_shadow/{year}/{mm}/shadow_{YYYYMMDD}.csv

Usage:
    uv run python -m download_data.pull_sced_shadow --year 2025 --month 7
    uv run python -m download_data.pull_sced_shadow --year 2025  # all months
"""

import argparse
import calendar
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from download_data.pull_ercot import (
    ercot_request,
    get_bearer_token,
    load_credentials,
)
from helper_funcs import setup_directories

ENDPOINT = "/np6-86-cd/shdw_prices_bnd_trns_const"


def download_shadow_day(date_str, output_dir, api_key, bearer_token):
    """Download SCED shadow prices for a single day.

    Args:
        date_str: 'YYYY-MM-DD'
        output_dir: Directory to save CSV
        api_key: ERCOT subscription key
        bearer_token: OAuth2 bearer token

    Returns:
        Number of records downloaded, or -1 if skipped (already exists)
    """
    output_file = os.path.join(output_dir, f"shadow_{date_str.replace('-', '')}.csv")

    if os.path.exists(output_file):
        print(f"  Skipping {date_str} (already exists)")
        return -1

    print(f"  Downloading shadow prices for {date_str}...")
    params = {
        "SCEDTimestampFrom": f"{date_str}T00:00:00",
        "SCEDTimestampTo": f"{date_str}T23:59:59",
    }

    records = ercot_request(ENDPOINT, params, api_key, bearer_token)

    if records:
        df = pd.DataFrame(records)
        os.makedirs(output_dir, exist_ok=True)
        df.to_csv(output_file, index=False)
        print(f"    Saved {len(df)} records to {output_file}")
        return len(df)
    else:
        print(f"    No data for {date_str}")
        return 0


def download_shadow_month(year, month, api_key=None, bearer_token=None):
    """Download SCED shadow prices for an entire month.

    Args:
        year: Integer year
        month: Integer month
        api_key: ERCOT subscription key (loaded from keys if None)
        bearer_token: OAuth2 bearer token (obtained if None)

    Returns:
        Total records downloaded
    """
    dirs = setup_directories()
    output_dir = os.path.join(
        dirs["raw"], "ercot", "sced_shadow", str(year), f"{month:02d}"
    )
    os.makedirs(output_dir, exist_ok=True)

    # Auth if not provided
    if api_key is None or bearer_token is None:
        creds = load_credentials()
        api_key = api_key or creds["api_key"]
        if bearer_token is None:
            print("Authenticating with ERCOT API...")
            bearer_token = get_bearer_token(creds["username"], creds["password"])
            if bearer_token:
                print("Bearer token obtained.\n")
            else:
                print("WARNING: No bearer token. Requests may fail.\n")

    num_days = calendar.monthrange(year, month)[1]
    total = 0

    print(f"=== SCED Shadow Prices: {year}-{month:02d} ({num_days} days) ===\n")

    for day in range(1, num_days + 1):
        date_str = f"{year}-{month:02d}-{day:02d}"
        n = download_shadow_day(date_str, output_dir, api_key, bearer_token)
        if n > 0:
            total += n

    print(f"\n=== Done: {total:,} total records for {year}-{month:02d} ===")
    return total


def download_shadow_year(year):
    """Download SCED shadow prices for all 12 months."""
    creds = load_credentials()
    api_key = creds["api_key"]
    print("Authenticating with ERCOT API...")
    bearer_token = get_bearer_token(creds["username"], creds["password"])
    if bearer_token:
        print("Bearer token obtained.\n")
    else:
        print("WARNING: No bearer token.\n")

    for month in range(1, 13):
        download_shadow_month(year, month, api_key, bearer_token)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download ERCOT SCED shadow prices")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, default=None,
                        help="Month (1-12). If omitted, downloads all 12 months.")
    args = parser.parse_args()

    if args.month:
        download_shadow_month(args.year, args.month)
    else:
        download_shadow_year(args.year)
