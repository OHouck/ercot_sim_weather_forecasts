"""pull_dam_disclosure.py — Download ERCOT 60-Day DAM Disclosure Reports (NP3-966-ER).

Downloads day-ahead market offer curves for all generation resources from ERCOT's
60-Day DAM Disclosure Reports. Each report is released ~60 days after the operating
date. The Gen Resource Data CSV contains the full offer curve (up to 10 MW/Price
pairs), HSL, LSL, resource type, and awarded quantities needed to compute
day-ahead market markups following Woerman (2023).

Output: {raw}/ercot/dam_disclosure/{year}/{mm}/dam_gen_resource_{YYYYMM}.parquet
"""

import io
import sys
import time
import zipfile
import argparse
import calendar
import requests
import pandas as pd
from pathlib import Path
from datetime import date, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))
from download_data.pull_ercot import (
    ERCOT_API_BASE,
    load_credentials,
    get_bearer_token,
)
from helper_funcs import setup_directories

ARCHIVE_ENDPOINT = f"{ERCOT_API_BASE}/archive/NP3-966-ER"
GEN_RESOURCE_FILE = "60d_DAM_Gen_Resource_Data"


def _list_dam_archives(month_start, month_end, headers):
    """Return all DAM disclosure archive records whose release window covers the operating month.

    Archives are released ~60 days after the operating date, so we search
    [month_start + 58d, month_end + 65d] to capture all relevant releases.

    Args:
        month_start: first day of operating month (date)
        month_end: last day of operating month (date)
        headers: HTTP headers dict with Authorization and subscription key

    Returns:
        list of archive dicts with 'docId' and download link under '_links'
    """
    post_from = (month_start + timedelta(days=58)).strftime("%Y-%m-%dT00:00:00")
    post_to = (month_end + timedelta(days=65)).strftime("%Y-%m-%dT23:59:59")

    all_archives, page = [], 1
    while True:
        r = requests.get(
            ARCHIVE_ENDPOINT,
            headers=headers,
            params={"postDatetimeFrom": post_from, "postDatetimeTo": post_to, "size": 200, "page": page},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        all_archives.extend(data.get("archives", []))
        meta = data.get("_meta", {})
        if page >= meta.get("totalPages", 1):
            break
        page += 1
        time.sleep(0.3)

    return all_archives


def _parse_gen_resource_csv(zip_bytes, target_year, target_month):
    """Extract Gen Resource Data from a DAM disclosure ZIP, filtered to the target month.

    Args:
        zip_bytes: raw bytes of the ZIP archive
        target_year: operating year (int)
        target_month: operating month 1..12 (int)

    Returns:
        DataFrame or None if no matching rows
    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        gen_files = [n for n in z.namelist() if GEN_RESOURCE_FILE in n]
        if not gen_files:
            return None
        with z.open(gen_files[0]) as f:
            df = pd.read_csv(f, low_memory=False)

    df["Delivery Date"] = pd.to_datetime(df["Delivery Date"], format="%m/%d/%Y")
    mask = (df["Delivery Date"].dt.year == target_year) & (df["Delivery Date"].dt.month == target_month)
    result = df[mask]
    return result if len(result) > 0 else None


def download_dam_disclosure_month(year, month, token, api_key, force_rebuild=False):
    """Download and parse all DAM Gen Resource Data for one operating month.

    Searches for archives posted ~60 days after the operating month, downloads
    each ZIP, filters rows to the target operating month, then deduplicates
    across releases (keeps row from most recently posted archive).

    Args:
        year: operating year (int)
        month: operating month 1..12 (int)
        token: valid OAuth2 bearer token (str)
        api_key: ERCOT API subscription key (str)
        force_rebuild: overwrite cached parquet if True (bool)

    Returns:
        DataFrame with all Gen Resource rows for (year, month)
    """
    dirs = setup_directories()
    out_dir = Path(dirs["raw"]) / "ercot" / "dam_disclosure" / str(year) / f"{month:02d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"dam_gen_resource_{year}{month:02d}.parquet"

    if out_path.exists() and not force_rebuild:
        print(f"  Cached: {out_path}")
        return pd.read_parquet(out_path)

    _, last_day = calendar.monthrange(year, month)
    month_start = date(year, month, 1)
    month_end = date(year, month, last_day)

    headers = {"Authorization": f"Bearer {token}", "Ocp-Apim-Subscription-Key": api_key}
    print(f"  Listing archives for {year}-{month:02d}...")
    archives = _list_dam_archives(month_start, month_end, headers)
    print(f"  Found {len(archives)} archives in release window")

    frames = []
    for i, archive in enumerate(archives):
        doc_id = archive["docId"]
        dl_url = archive["_links"]["endpoint"]["href"]
        print(f"    [{i+1}/{len(archives)}] docId={doc_id}...", end=" ", flush=True)
        t0 = time.time()
        try:
            for attempt in range(4):
                r = requests.get(dl_url, headers=headers, timeout=120)
                if r.status_code == 429:
                    wait = 30 * (attempt + 1)
                    print(f"rate limited, waiting {wait}s...", end=" ", flush=True)
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                break
            df = _parse_gen_resource_csv(r.content, year, month)
            if df is not None:
                df["_doc_id"] = doc_id
                frames.append(df)
                print(f"{len(df)} rows ({time.time()-t0:.1f}s)")
            else:
                print(f"no matching rows ({time.time()-t0:.1f}s)")
        except Exception as e:
            print(f"ERROR: {e}")
        time.sleep(2)

    if not frames:
        print(f"  WARNING: No data found for {year}-{month:02d}")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)

    # Keep the row from the most recently posted archive for each (resource, date, hour).
    key_cols = ["Resource Name", "Delivery Date", "Hour Ending"]
    combined = (
        combined.sort_values("_doc_id")
        .drop_duplicates(subset=key_cols, keep="last")
        .drop(columns=["_doc_id"])
    )

    print(f"  Saving {len(combined)} rows to {out_path}")
    combined.to_parquet(out_path, index=False)
    return combined


def main():
    """Download DAM 60-day disclosure for the specified year and months."""
    parser = argparse.ArgumentParser(description="Download ERCOT 60-Day DAM Disclosure")
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--months", type=int, nargs="+", default=list(range(1, 13)))
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    creds = load_credentials()
    token = get_bearer_token(creds["username"], creds["password"])

    for month in args.months:
        print(f"\n=== {args.year}-{month:02d} ===")
        df = download_dam_disclosure_month(args.year, month, token, creds["api_key"], force_rebuild=args.force)
        if len(df):
            print(f"  Resource types: {df['Resource Type'].value_counts().to_dict()}")


if __name__ == "__main__":
    main()
