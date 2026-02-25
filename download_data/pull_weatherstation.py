"""pull_weatherstation.py — Download hourly ISD weather station data for Texas.

Downloads realized weather observations (temperature, wind speed, wind direction)
from NOAA's Integrated Surface Database (ISD) via the NCEI Data Service API.
These observations serve as ground truth to compare against NDFD forecasts.
"""

import os
import sys
import time
import calendar
import requests
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from helper_funcs import setup_directories

ISD_HISTORY_URL = "https://www1.ncdc.noaa.gov/pub/data/noaa/isd-history.txt"
NCEI_API_URL = "https://www.ncei.noaa.gov/access/services/data/v1"

# Texas bounding box (same as pull_ndfd.py)
TX_LAT_MIN, TX_LAT_MAX = 25.8, 36.5
TX_LON_MIN, TX_LON_MAX = -106.6, -93.5


def download_texas_stations(output_path):
    """Download ISD station history and filter to active Texas stations.

    Parses the fixed-width ISD history file and filters for:
    - CTRY == 'US' and STATE == 'TX'
    - Station has data ending on or after 2025-07-01
    - Station has valid lat/lon within Texas bounds

    Args:
        output_path: Path to save filtered station list CSV

    Returns:
        DataFrame with columns: usaf, wban, station_name, lat, lon, elev, begin, end, station_id
    """
    print("Downloading ISD station history...")
    resp = requests.get(ISD_HISTORY_URL)
    resp.raise_for_status()

    lines = resp.text.strip().split('\n')

    records = []
    for line in lines[22:]:  # Skip header/description lines
        if len(line) < 95:
            continue
        try:
            usaf = line[0:6].strip()
            wban = line[7:12].strip()
            name = line[13:42].strip()
            ctry = line[43:47].strip()
            state = line[48:50].strip()
            lat_str = line[57:64].strip()
            lon_str = line[65:73].strip()
            elev_str = line[74:81].strip()
            begin_str = line[82:90].strip()
            end_str = line[91:99].strip()

            if ctry != 'US' or state != 'TX':
                continue

            lat = float(lat_str)
            lon = float(lon_str)

            if lat < TX_LAT_MIN or lat > TX_LAT_MAX:
                continue
            if lon < TX_LON_MIN or lon > TX_LON_MAX:
                continue

            # Skip stations that ended before our target period
            if end_str < '20250701':
                continue

            records.append({
                'usaf': usaf,
                'wban': wban,
                'station_name': name,
                'lat': lat,
                'lon': lon,
                'elev': float(elev_str) if elev_str else None,
                'begin': begin_str,
                'end': end_str,
            })
        except (ValueError, IndexError):
            continue

    df = pd.DataFrame(records)
    df['station_id'] = df['usaf'] + df['wban']

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Found {len(df)} active Texas stations. Saved to {output_path}")
    return df


def download_station_data(station_id, start_date, end_date, output_path):
    """Download hourly observations for a single station from NCEI API.

    Args:
        station_id: Station ID in USAF+WBAN format (e.g. '72245003927')
        start_date: Start date string 'YYYY-MM-DD'
        end_date: End date string 'YYYY-MM-DD'
        output_path: Path to save CSV output

    Returns:
        True if successful, False otherwise
    """
    params = {
        'dataset': 'global-hourly',
        'stations': station_id,
        'startDate': f'{start_date}T00:00:00',
        'endDate': f'{end_date}T23:59:59',
        'dataTypes': 'TMP,WND',
        'format': 'csv',
        'units': 'metric',
    }

    try:
        resp = requests.get(NCEI_API_URL, params=params, timeout=120)
        if resp.status_code == 200 and len(resp.text.strip()) > 0:
            lines = resp.text.strip().split('\n')
            if len(lines) > 1:
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                with open(output_path, 'w') as f:
                    f.write(resp.text)
                return True
            else:
                return False
        else:
            print(f"  HTTP {resp.status_code} for station {station_id}")
            return False
    except requests.RequestException as e:
        print(f"  Error for station {station_id}: {e}")
        return False


def download_month(year, month):
    """Download all Texas weather station data for a given month.

    Args:
        year: Integer year (e.g. 2025)
        month: Integer month (e.g. 7)
    """
    dirs = setup_directories()
    base_dir = os.path.join(dirs['raw'], 'weather_stations')

    # Step 1: Get station list
    stations_file = os.path.join(base_dir, 'stations.csv')
    if os.path.exists(stations_file):
        stations = pd.read_csv(stations_file, dtype={'usaf': str, 'wban': str, 'station_id': str})
        print(f"Loaded {len(stations)} stations from {stations_file}")
    else:
        stations = download_texas_stations(stations_file)

    # Step 2: Download each station
    num_days = calendar.monthrange(year, month)[1]
    start_date = f"{year}-{month:02d}-01"
    end_date = f"{year}-{month:02d}-{num_days:02d}"
    output_dir = os.path.join(base_dir, str(year), f"{month:02d}")

    success_count = 0
    skip_count = 0
    fail_count = 0

    for idx, row in stations.iterrows():
        station_id = row['station_id']
        output_path = os.path.join(output_dir, f"{station_id}.csv")

        # Skip if already downloaded
        if os.path.exists(output_path):
            skip_count += 1
            continue

        print(f"  [{idx+1}/{len(stations)}] {station_id} ({row['station_name']})...", end="")
        ok = download_station_data(station_id, start_date, end_date, output_path)
        if ok:
            success_count += 1
            print(" ok")
        else:
            fail_count += 1
            print(" no data")

        # Rate limit: 5 req/sec max
        time.sleep(0.25)

    print(f"\nDone: {success_count} downloaded, {skip_count} skipped, {fail_count} no data")
    print(f"Output: {output_dir}")


if __name__ == "__main__":
    download_month(2025, 7)
