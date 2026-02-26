"""pull_ercot.py — Download ERCOT settlement point price data.

Downloads day-ahead and real-time settlement point prices
from the ERCOT Public API. Requires API credentials in ~/keys/.

Authentication flow:
1. Get OAuth2 Bearer token via Azure B2C ROPC flow using username/password
2. Use Bearer token + subscription key for API requests
"""

import os
import sys
import time
import json
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


def download_dam_spp(start_date, end_date, output_dir, api_key, bearer_token=None):
    """Download day-ahead settlement point prices.

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
    os.makedirs(output_dir, exist_ok=True)

    current = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')

    while current <= end:
        date_str = current.strftime('%Y-%m-%d')
        output_file = os.path.join(output_dir, f"dam_spp_{date_str}.csv")

        if os.path.exists(output_file):
            print(f"  Skipping {date_str} (already exists)")
            current += timedelta(days=1)
            continue

        print(f"  Downloading DAM SPP for {date_str}...")
        params = {
            'deliveryDateFrom': date_str,
            'deliveryDateTo': date_str,
        }

        records = ercot_request(
            '/np4-190-cd/dam_stlmnt_pnt_prices', params, api_key, bearer_token
        )

        if records:
            df = pd.DataFrame(records)
            df.to_csv(output_file, index=False)
            print(f"    Saved {len(df)} records")
        else:
            print(f"    No data for {date_str}")

        current += timedelta(days=1)
        time.sleep(2)


def download_rt_spp(start_date, end_date, output_dir, api_key, bearer_token=None):
    """Download real-time settlement point prices (15-min intervals).

    Args:
        start_date: 'YYYY-MM-DD' start
        end_date: 'YYYY-MM-DD' end
        output_dir: Directory to save CSV files
        api_key: ERCOT API key
        bearer_token: OAuth2 bearer token

    Saves one CSV per day to output_dir.
    """
    os.makedirs(output_dir, exist_ok=True)

    current = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')

    while current <= end:
        date_str = current.strftime('%Y-%m-%d')
        output_file = os.path.join(output_dir, f"rt_spp_{date_str}.csv")

        if os.path.exists(output_file):
            print(f"  Skipping {date_str} (already exists)")
            current += timedelta(days=1)
            continue

        print(f"  Downloading RT SPP for {date_str}...")
        params = {
            'deliveryDateFrom': date_str,
            'deliveryDateTo': date_str,
        }

        records = ercot_request(
            '/np6-905-cd/spp_node_zone_hub', params, api_key, bearer_token
        )

        if records:
            df = pd.DataFrame(records)
            df.to_csv(output_file, index=False)
            print(f"    Saved {len(df)} records")
        else:
            print(f"    No data for {date_str}")

        current += timedelta(days=1)
        time.sleep(2)


def download_month(year, month):
    """Download all ERCOT data for a given month.

    Args:
        year: Integer year (e.g. 2025)
        month: Integer month (e.g. 7)
    """
    import calendar

    dirs = setup_directories()
    base_dir = os.path.join(dirs['raw'], 'ercot')
    creds = load_credentials()

    num_days = calendar.monthrange(year, month)[1]
    start_date = f"{year}-{month:02d}-01"
    end_date = f"{year}-{month:02d}-{num_days:02d}"

    print(f"=== ERCOT Data Download: {start_date} to {end_date} ===\n")

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

    print("\n=== ERCOT Download Complete ===")


if __name__ == "__main__":
    download_month(2025, 7)
