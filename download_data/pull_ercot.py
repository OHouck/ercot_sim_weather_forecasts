"""pull_ercot.py — Download ERCOT market and demand data.

Downloads day-ahead and real-time settlement point prices, actual system load,
demand forecasts, and SCED system lambda from the ERCOT Public API.
Requires API credentials in ~/keys/.

Authentication flow:
1. Get OAuth2 Bearer token via Azure B2C ROPC flow using username/password
2. Use Bearer token + subscription key for API requests
"""

import io
import os
import sys
import time
import json
import calendar
import zipfile
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))
from helper_funcs import setup_directories

ERCOT_API_BASE = "https://api.ercot.com/api/public-reports"

# Azure B2C OAuth2 endpoint for ERCOT
TOKEN_URL = (
    "https://ercotb2c.b2clogin.com/ercotb2c.onmicrosoft.com/"
    "B2C_1_PUBAPI-ROPC-FLOW/oauth2/v2.0/token"
)
CLIENT_ID = "fec253ea-0d06-4272-a5e6-b478baeecd70"


def load_credentials():
    """Load ERCOT API credentials from ~/keys/.

    Returns:
        dict with keys: api_key, secondary_key, username, password
    """
    keys_dir = os.path.expanduser("~/keys")
    creds = {}
    for name, filename in [
        ('api_key', 'ercot_api_key.txt'),
        ('secondary_key', 'ercot_api_secondary_key.txt'),
        ('username', 'ercot_user.txt'),
        ('password', 'ercot_pwd.txt'),
    ]:
        filepath = os.path.join(keys_dir, filename)
        with open(filepath) as f:
            creds[name] = f.read().strip()
    return creds


def get_bearer_token(username, password):
    """Get OAuth2 Bearer token from ERCOT's Azure B2C endpoint.

    Uses Resource Owner Password Credentials (ROPC) flow.

    Args:
        username: ERCOT account email/username
        password: ERCOT account password

    Returns:
        Bearer token string, or None if authentication fails
    """
    data = {
        'grant_type': 'password',
        'username': username,
        'password': password,
        'response_type': 'token',
        'scope': f'openid {CLIENT_ID} offline_access',
        'client_id': CLIENT_ID,
    }

    resp = requests.post(TOKEN_URL, data=data, timeout=30)

    if resp.status_code == 200:
        token_data = resp.json()
        return token_data.get('access_token')
    else:
        print(f"OAuth error: {resp.status_code}")
        print(f"Response: {resp.text[:500]}")
        return None


def ercot_request(endpoint, params, api_key, bearer_token=None, max_pages=100):
    """Make a paginated request to the ERCOT API.

    The ERCOT API returns data as lists-of-lists with a separate 'fields' array.
    This function combines them into a list of dicts for easy DataFrame creation.

    Args:
        endpoint: API endpoint path (e.g. '/np4-190-cd/dam_stlmnt_pnt_prices')
        params: Query parameters dict
        api_key: ERCOT API subscription key
        bearer_token: OAuth2 bearer token (if None, tries subscription key only)
        max_pages: Maximum number of pages to fetch

    Returns:
        List of dicts (one per record) with field names as keys
    """
    headers = {
        'Ocp-Apim-Subscription-Key': api_key,
    }
    if bearer_token:
        headers['Authorization'] = f'Bearer {bearer_token}'

    all_records = []
    column_names = None
    page = 1
    params = dict(params)  # copy
    params['size'] = 100000

    while page <= max_pages:
        params['page'] = page
        url = f"{ERCOT_API_BASE}{endpoint}"

        resp = requests.get(url, headers=headers, params=params, timeout=60,
                            allow_redirects=True)

        if resp.status_code == 429:
            print(f"  Rate limited, waiting 60s...")
            time.sleep(60)
            continue

        if resp.status_code != 200:
            print(f"  HTTP {resp.status_code}: {resp.text[:300]}")
            break

        data = resp.json()

        # Extract column names from 'fields' on first page
        if column_names is None and 'fields' in data:
            column_names = [f['name'] for f in data['fields']]

        rows = data.get('data', [])
        if not rows:
            break

        # Convert list-of-lists to list-of-dicts
        if column_names:
            for row in rows:
                all_records.append(dict(zip(column_names, row)))
        else:
            all_records.extend(rows)

        # Check pagination
        meta = data.get('_meta', {})
        total_pages = meta.get('totalPages', 1)
        total_records = meta.get('totalRecords', 0)

        if page == 1:
            print(f"    Total records: {total_records}, pages: {total_pages}")

        if page >= total_pages:
            break

        page += 1
        time.sleep(2)  # Rate limit: 30 req/min

    return all_records


def _download_daily_endpoint(
    start_date, end_date, output_dir, api_key, bearer_token,
    endpoint, file_prefix, label, extra_params=None,
):
    """Download a day-iterated ERCOT API endpoint, saving one CSV per day.

    Args:
        start_date: 'YYYY-MM-DD' start
        end_date: 'YYYY-MM-DD' end
        output_dir: Directory to save CSV files
        api_key: ERCOT API key
        bearer_token: OAuth2 bearer token
        endpoint: API endpoint path (e.g. '/np4-190-cd/dam_stlmnt_pnt_prices')
        file_prefix: Output filename prefix (e.g. 'dam_spp' → 'dam_spp_{date}.csv')
        label: Human-readable label for log messages
        extra_params: Additional query parameters merged into the date range params
    """
    os.makedirs(output_dir, exist_ok=True)

    current = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')

    while current <= end:
        date_str = current.strftime('%Y-%m-%d')
        output_file = os.path.join(output_dir, f"{file_prefix}_{date_str}.csv")

        if os.path.exists(output_file):
            print(f"  Skipping {date_str} (already exists)")
            current += timedelta(days=1)
            continue

        print(f"  Downloading {label} for {date_str}...")
        params = {'deliveryDateFrom': date_str, 'deliveryDateTo': date_str}
        if extra_params:
            params.update(extra_params)

        records = ercot_request(endpoint, params, api_key, bearer_token)

        if records:
            pd.DataFrame(records).to_csv(output_file, index=False)
            print(f"    Saved {len(records)} records")
        else:
            print(f"    No data for {date_str}")

        current += timedelta(days=1)
        time.sleep(2)


def download_dam_spp(start_date, end_date, output_dir, api_key, bearer_token=None):
    """Download day-ahead settlement point prices (NP4-190-CD).

    Uses NP4-190-CD endpoint which provides prices at the settlement point
    level (resource nodes, load zones, hubs) rather than bus level.

    Args:
        start_date: 'YYYY-MM-DD' start
        end_date: 'YYYY-MM-DD' end
        output_dir: Directory to save CSV files
        api_key: ERCOT API key
        bearer_token: OAuth2 bearer token

    Saves one CSV per day to output_dir.
    """
    _download_daily_endpoint(
        start_date, end_date, output_dir, api_key, bearer_token,
        endpoint='/np4-190-cd/dam_stlmnt_pnt_prices',
        file_prefix='dam_spp',
        label='DAM SPP',
    )


def download_rt_spp(start_date, end_date, output_dir, api_key, bearer_token=None):
    """Download real-time settlement point prices (NP6-905-CD, 15-min intervals).

    Args:
        start_date: 'YYYY-MM-DD' start
        end_date: 'YYYY-MM-DD' end
        output_dir: Directory to save CSV files
        api_key: ERCOT API key
        bearer_token: OAuth2 bearer token

    Saves one CSV per day to output_dir.
    """
    _download_daily_endpoint(
        start_date, end_date, output_dir, api_key, bearer_token,
        endpoint='/np6-905-cd/spp_node_zone_hub',
        file_prefix='rt_spp',
        label='RT SPP',
    )


def download_actual_load(start_date, end_date, output_dir, api_key, bearer_token=None):
    """Download actual hourly system load by weather zone (NP6-345-CD).

    Downloads one month at a time. Each month produces ~744 rows (24 hours x ~31 days).

    Args:
        start_date: 'YYYY-MM-DD' start
        end_date: 'YYYY-MM-DD' end
        output_dir: Directory to save monthly CSV files
        api_key: ERCOT API key
        bearer_token: OAuth2 bearer token
    """
    os.makedirs(output_dir, exist_ok=True)

    # Parse into months and download each
    current = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')

    while current <= end:
        month_end_day = calendar.monthrange(current.year, current.month)[1]
        month_start = current.replace(day=1)
        month_end = min(current.replace(day=month_end_day), end)

        month_str = month_start.strftime('%Y-%m')
        output_file = os.path.join(output_dir, f"actual_load_{month_str}.csv")

        if os.path.exists(output_file):
            print(f"  Skipping {month_str} (already exists)")
            current = month_end + timedelta(days=1)
            continue

        print(f"  Downloading actual load for {month_str}...")
        params = {
            'operatingDayFrom': month_start.strftime('%Y-%m-%d'),
            'operatingDayTo': month_end.strftime('%Y-%m-%d'),
        }

        records = ercot_request(
            '/np6-345-cd/act_sys_load_by_wzn', params, api_key, bearer_token
        )

        if records:
            df = pd.DataFrame(records)
            df.to_csv(output_file, index=False)
            print(f"    Saved {len(df)} records")
        else:
            print(f"    No data for {month_str}")

        current = month_end + timedelta(days=1)
        time.sleep(2)


def download_demand_forecasts(start_date, end_date, output_dir, api_key, bearer_token=None):
    """Download demand forecasts by model and weather zone (NP3-565-CD).

    Downloads forecasts with inUseFlag=true for each day. Saves raw data
    including all posted times; lead times are computed in post-processing.

    Each day produces ~4,608 rows (192 posted times x 24 delivery hours,
    filtered to the active model at each issuance).

    Args:
        start_date: 'YYYY-MM-DD' start
        end_date: 'YYYY-MM-DD' end
        output_dir: Directory to save daily CSV files
        api_key: ERCOT API key
        bearer_token: OAuth2 bearer token
    """
    _download_daily_endpoint(
        start_date, end_date, output_dir, api_key, bearer_token,
        endpoint='/np3-565-cd/lf_by_model_weather_zone',
        file_prefix='demand_forecast',
        label='demand forecasts',
        extra_params={'inUseFlag': 'true'},
    )


def download_wind_power_forecast(start_date, end_date, output_dir, api_key, bearer_token=None):
    """Download hourly wind power forecasts by geographic region (NP4-742-CD).

    Uses the STWPF (Short-Term Wind Power Forecast), the primary operational
    wind forecast used by Qualified Scheduling Entities (QSEs) to submit Current
    Operating Plans. WGRPP (Wind Generation Resource Production Potential), used
    for Reliability Unit Commitment charge responsibility, is also included since
    both come from the same endpoint.

    Both are 50% probability-of-exceedance hourly forecasts. Regions: Panhandle,
    Coastal, South, West, North, and system-wide.

    Args:
        start_date: 'YYYY-MM-DD' start
        end_date: 'YYYY-MM-DD' end
        output_dir: Directory to save daily CSV files
        api_key: ERCOT API key
        bearer_token: OAuth2 bearer token

    Saves one CSV per day. Expected columns (API-dependent): deliveryDate,
    hourEnding, region, genMw, copHsl, stwpf, wgrpp, hsl.
    """
    _download_daily_endpoint(
        start_date, end_date, output_dir, api_key, bearer_token,
        endpoint='/np4-742-cd/wpp_hrly_actual_fcast_geo',
        file_prefix='wind_forecast',
        label='wind power forecast',
    )


def _fetch_sced_lambda_bundle_index(api_key, bearer_token=None):
    """Fetch the NP6-322-CD monthly bundle index.

    ERCOT pre-packages all SCED lambda runs for each calendar month into a
    single ZIP (a ZIP of ZIPs). This returns the full index so callers can
    look up a month by name.

    Args:
        api_key: ERCOT API subscription key
        bearer_token: OAuth2 bearer token

    Returns:
        Dict mapping 'YYYY-MM' → download URL string
    """
    headers = {'Ocp-Apim-Subscription-Key': api_key}
    if bearer_token:
        headers['Authorization'] = f'Bearer {bearer_token}'

    resp = requests.get(
        f"{ERCOT_API_BASE}/bundle/np6-322-cd",
        headers=headers,
        timeout=30,
    )
    resp.raise_for_status()

    bundles = {}
    for b in resp.json().get('bundles', []):
        # friendlyName pattern: SCEDSYSLAMBDANP6322_2025-07
        parts = b['friendlyName'].split('_')
        if len(parts) == 2:
            bundles[parts[1]] = b['_links']['endpoint']['href']
    return bundles


def _parse_sced_lambda_zip(content, friendly_name):
    """Parse a zipped SCED lambda CSV into a DataFrame.

    Handles historical column name variations (e.g. 'SCEDTimestamp' vs
    'SCED Time Stamp'). Timestamp is stored as a naive string exactly as
    ERCOT provides it (US/Central prevailing time); no timezone conversion
    is done here.

    Args:
        content: Raw bytes of the zip file
        friendly_name: Filename string used in error messages

    Returns:
        DataFrame with columns sced_timestamp (str), system_lambda (float),
        and optionally repeated_hour_flag; or None if parsing fails
    """
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            csv_names = [n for n in zf.namelist() if n.endswith('.csv')]
            if not csv_names:
                return None
            with zf.open(csv_names[0]) as f:
                df = pd.read_csv(f)
    except Exception as e:
        print(f"    Zip error in {friendly_name}: {e}")
        return None

    df.columns = [c.strip() for c in df.columns]
    col_lower = {c: c.lower().replace(' ', '').replace('_', '') for c in df.columns}

    ts_col = next((c for c, l in col_lower.items() if 'timestamp' in l), None)
    lam_col = next((c for c, l in col_lower.items() if 'lambda' in l), None)
    rep_col = next((c for c, l in col_lower.items() if 'repeated' in l), None)

    if ts_col is None or lam_col is None:
        print(f"    Unexpected columns in {friendly_name}: {list(df.columns)}")
        return None

    result = pd.DataFrame({
        'sced_timestamp': df[ts_col].astype(str),
        'system_lambda': pd.to_numeric(df[lam_col], errors='coerce'),
    })
    if rep_col is not None:
        result['repeated_hour_flag'] = df[rep_col].values

    return result.dropna(subset=['system_lambda'])


def download_sced_lambda(start_date, end_date, output_dir, api_key, bearer_token=None):
    """Download SCED System Lambda data (NP6-322-CD) for a date range.

    System lambda is the system-wide energy component of LMP — the shadow price
    on the power balance constraint before nodal congestion adjustments. Published
    per SCED run (~every 5 minutes, ~288 runs/day, ~8,900 runs/month).

    Uses the ERCOT bundle endpoint (one ZIP per calendar month containing all
    SCED runs as nested ZIPs), which avoids per-file rate limits and reduces
    total requests from ~8,900/month to 1/month.

    Args:
        start_date: 'YYYY-MM-DD' start
        end_date: 'YYYY-MM-DD' end
        output_dir: Directory to save monthly parquet files
        api_key: ERCOT API subscription key
        bearer_token: OAuth2 bearer token

    Saves sced_lambda_{YYYY-MM}.parquet per month. Columns: sced_timestamp
    (str, US/Central prevailing time), system_lambda (float, $/MWh), and
    repeated_hour_flag (str, present when ERCOT includes it).
    """
    os.makedirs(output_dir, exist_ok=True)

    headers = {'Ocp-Apim-Subscription-Key': api_key}
    if bearer_token:
        headers['Authorization'] = f'Bearer {bearer_token}'

    print("  Fetching bundle index...")
    bundle_index = _fetch_sced_lambda_bundle_index(api_key, bearer_token)

    current = datetime.strptime(start_date, '%Y-%m-%d').replace(day=1)
    end = datetime.strptime(end_date, '%Y-%m-%d')

    while current <= end:
        month_end_day = calendar.monthrange(current.year, current.month)[1]
        month_end = min(current.replace(day=month_end_day), end)
        month_str = current.strftime('%Y-%m')
        output_file = os.path.join(output_dir, f"sced_lambda_{month_str}.parquet")

        if os.path.exists(output_file):
            print(f"  Skipping {month_str} (already exists)")
            current = month_end + timedelta(days=1)
            continue

        bundle_url = bundle_index.get(month_str)
        if not bundle_url:
            print(f"  No bundle found for {month_str} (not yet published?)")
            current = month_end + timedelta(days=1)
            continue

        print(f"  Downloading bundle for {month_str}...")
        resp = requests.get(bundle_url, headers=headers, timeout=300)
        if resp.status_code != 200:
            print(f"    HTTP {resp.status_code}, skipping {month_str}")
            current = month_end + timedelta(days=1)
            continue

        frames = []
        with zipfile.ZipFile(io.BytesIO(resp.content)) as outer:
            inner_names = outer.namelist()
            print(f"    Parsing {len(inner_names)} SCED run files...")
            for name in inner_names:
                df = _parse_sced_lambda_zip(outer.read(name), name)
                if df is not None:
                    frames.append(df)

        if not frames:
            print(f"    No data parsed for {month_str}")
            current = month_end + timedelta(days=1)
            continue

        month_df = (
            pd.concat(frames, ignore_index=True)
            .drop_duplicates(subset=['sced_timestamp'])
            .sort_values('sced_timestamp')
            .reset_index(drop=True)
        )

        month_df.to_parquet(output_file, index=False)
        lam = month_df['system_lambda']
        print(f"    Saved {len(month_df)} SCED intervals for {month_str}")
        print(f"    Lambda range: [{lam.min():.2f}, {lam.max():.2f}] $/MWh")

        current = month_end + timedelta(days=1)


def _ercot_month_complete(base_dir, year, month):
    """Return True if all ERCOT daily files for the month already exist.

    Checks only the daily-file datasets (DAM SPP, RT SPP, demand forecasts,
    wind forecasts) and the monthly actual-load file. Used to skip
    authentication when the full month was already downloaded.

    Args:
        base_dir: Root ERCOT raw data directory.
        year: Calendar year.
        month: Calendar month (1–12).

    Returns:
        bool
    """
    num_days = calendar.monthrange(year, month)[1]
    month_str = f"{year}-{month:02d}"
    month_dir = f"{year}/{month:02d}"

    daily_prefixes = [
        (os.path.join(base_dir, 'dam_spp', month_dir), 'dam_spp'),
        (os.path.join(base_dir, 'rt_spp', month_dir), 'rt_spp'),
        (os.path.join(base_dir, 'demand_forecast', month_dir), 'demand_forecast'),
        (os.path.join(base_dir, 'wind_forecast', month_dir), 'wind_forecast'),
    ]
    for dir_path, prefix in daily_prefixes:
        for day in range(1, num_days + 1):
            date_str = f"{year}-{month:02d}-{day:02d}"
            if not os.path.exists(os.path.join(dir_path, f"{prefix}_{date_str}.csv")):
                return False

    load_path = os.path.join(
        base_dir, 'actual_load', month_dir, f"actual_load_{month_str}.csv"
    )
    return os.path.exists(load_path)


def download_month(year, month):
    """Download all ERCOT data for a given month.

    Args:
        year: Integer year (e.g. 2025)
        month: Integer month (e.g. 7)
    """
    dirs = setup_directories()
    base_dir = os.path.join(dirs['raw'], 'ercot')

    num_days = calendar.monthrange(year, month)[1]
    start_date = f"{year}-{month:02d}-01"
    end_date = f"{year}-{month:02d}-{num_days:02d}"

    print(f"=== ERCOT Data Download: {start_date} to {end_date} ===\n")

    if _ercot_month_complete(base_dir, year, month):
        print(f"  All ERCOT files for {year}-{month:02d} already exist, skipping.")
        return

    creds = load_credentials()

    # Try to get bearer token
    print("Authenticating with ERCOT API...")
    bearer_token = get_bearer_token(creds['username'], creds['password'])
    if bearer_token:
        print("Bearer token obtained successfully.\n")
    else:
        print("WARNING: Could not get bearer token. Trying with subscription key only.\n")
        print("If requests fail with 401, you may need to:")
        print("  1. Update ~/keys/ercot_user.txt with your ERCOT email address")
        print("  2. Update ~/keys/ercot_pwd.txt with your current password")
        print("  3. Verify your account at https://apiexplorer.ercot.com/\n")

    api_key = creds['api_key']

    # Day-ahead settlement point prices
    print("--- Day-Ahead Settlement Point Prices ---")
    dam_dir = os.path.join(base_dir, 'dam_spp', str(year), f"{month:02d}")
    download_dam_spp(start_date, end_date, dam_dir, api_key, bearer_token)

    # Real-time SPP
    print("\n--- Real-Time Settlement Point Prices ---")
    rt_dir = os.path.join(base_dir, 'rt_spp', str(year), f"{month:02d}")
    download_rt_spp(start_date, end_date, rt_dir, api_key, bearer_token)

    # Actual system load
    print("\n--- Actual System Load ---")
    load_dir = os.path.join(base_dir, 'actual_load', str(year), f"{month:02d}")
    download_actual_load(start_date, end_date, load_dir, api_key, bearer_token)

    # Demand forecasts
    print("\n--- Demand Forecasts ---")
    forecast_dir = os.path.join(base_dir, 'demand_forecast', str(year), f"{month:02d}")
    download_demand_forecasts(start_date, end_date, forecast_dir, api_key, bearer_token)

    print("\n--- Wind Power Forecasts ---")
    wind_dir = os.path.join(base_dir, 'wind_forecast', str(year), f"{month:02d}")
    download_wind_power_forecast(start_date, end_date, wind_dir, api_key, bearer_token)

    print("\n=== ERCOT Download Complete ===")


def download_year(year):
    """Download all ERCOT data for a full year.

    Args:
        year: Integer year (e.g. 2025)
    """
    for month in range(1, 13):
        download_month(year, month)


def download_sced_lambda_year(year):
    """Download SCED System Lambda for all months of a full year.

    Authenticates once and calls download_sced_lambda month-by-month.
    Each month takes 30-60 minutes due to ~8,900 archive file downloads.

    Args:
        year: Integer year (e.g. 2025)
    """
    dirs = setup_directories()
    output_dir = os.path.join(dirs['raw'], 'ercot', 'sced_lambda', str(year))
    creds = load_credentials()

    print(f"=== SCED System Lambda Download: {year} ===\n")

    print("Authenticating with ERCOT API...")
    bearer_token = get_bearer_token(creds['username'], creds['password'])
    if bearer_token:
        print("Bearer token obtained successfully.\n")
    else:
        print("WARNING: Could not get bearer token, proceeding with subscription key only.\n")

    api_key = creds['api_key']

    for month in range(1, 13):
        num_days = calendar.monthrange(year, month)[1]
        start_date = f"{year}-{month:02d}-01"
        end_date = f"{year}-{month:02d}-{num_days:02d}"
        month_dir = os.path.join(output_dir, f"{month:02d}")
        print(f"\n--- SCED Lambda {year}-{month:02d} ---")
        download_sced_lambda(start_date, end_date, month_dir, api_key, bearer_token)

    print(f"\n=== SCED Lambda Download Complete: {year} ===")


if __name__ == "__main__":


    # download_year(2025)
    download_sced_lambda_year(2025)
