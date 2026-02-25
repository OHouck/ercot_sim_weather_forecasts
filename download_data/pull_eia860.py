"""pull_eia860.py — Download EIA Form 860 plant data for Texas generators.

Downloads the EIA-860 annual plant file which contains geographic coordinates
(lat/lon) for every power plant in the US. Filters to Texas/ERCOT plants
for matching against ERCOT settlement point names.
"""

import os
import sys
import io
import zipfile
import requests
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from helper_funcs import setup_directories

EIA860_URL = "https://www.eia.gov/electricity/data/eia860/xls/eia8602024.zip"


def download_eia860_plants():
    """Download EIA Form 860 and extract Texas plant data with coordinates.

    Downloads the full EIA-860 ZIP, reads the Plant file (2___Plant2024.xlsx),
    and filters to Texas plants (State=TX or Balancing Authority=ERCO).

    Returns:
        DataFrame with Texas plant data including lat/lon
    """
    dirs = setup_directories()
    output_dir = os.path.join(dirs['raw'], 'eia860')
    output_file = os.path.join(output_dir, 'texas_plants.csv')

    if os.path.exists(output_file):
        print(f"Already downloaded: {output_file}")
        return pd.read_csv(output_file)

    print(f"Downloading EIA Form 860 from {EIA860_URL}...")
    resp = requests.get(EIA860_URL, timeout=120)
    resp.raise_for_status()
    print(f"  Downloaded {len(resp.content) / 1e6:.1f} MB")

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        # Find the plant file (named like 2___Plant2024.xlsx)
        plant_files = [f for f in zf.namelist() if '2___Plant' in f and f.endswith('.xlsx')]
        if not plant_files:
            raise FileNotFoundError(f"No plant file found in ZIP. Contents: {zf.namelist()}")

        plant_file = plant_files[0]
        print(f"  Reading {plant_file}...")

        with zf.open(plant_file) as f:
            df = pd.read_excel(f, skiprows=1)

    print(f"  Total US plants: {len(df)}")

    # Standardize column names for filtering
    col_map = {}
    for col in df.columns:
        low = col.lower().strip()
        if 'state' == low:
            col_map['state'] = col
        elif 'balancing authority code' in low:
            col_map['ba_code'] = col
        elif 'latitude' in low:
            col_map['lat'] = col
        elif 'longitude' in low:
            col_map['lon'] = col
        elif 'plant code' in low or 'plant id' in low:
            col_map['plant_code'] = col
        elif 'plant name' in low:
            col_map['plant_name'] = col
        elif 'nerc region' in low:
            col_map['nerc_region'] = col
        elif 'county' in low:
            col_map['county'] = col

    # Filter to Texas / ERCOT plants
    state_col = col_map.get('state', 'State')
    ba_col = col_map.get('ba_code')

    mask = df[state_col] == 'TX'
    if ba_col:
        mask = mask | (df[ba_col] == 'ERCO')

    tx_plants = df[mask].copy()
    print(f"  Texas/ERCOT plants: {len(tx_plants)}")

    # Rename to standard columns
    rename = {}
    for std_name, orig_col in col_map.items():
        rename[orig_col] = std_name
    tx_plants = tx_plants.rename(columns=rename)

    # Keep essential columns
    keep_cols = [c for c in ['plant_code', 'plant_name', 'state', 'county',
                              'lat', 'lon', 'ba_code', 'nerc_region']
                 if c in tx_plants.columns]
    tx_plants = tx_plants[keep_cols].copy()

    # Drop plants without coordinates
    before = len(tx_plants)
    tx_plants = tx_plants.dropna(subset=['lat', 'lon'])
    if before - len(tx_plants) > 0:
        print(f"  Dropped {before - len(tx_plants)} plants without coordinates")

    os.makedirs(output_dir, exist_ok=True)
    tx_plants.to_csv(output_file, index=False)
    print(f"  Saved {len(tx_plants)} plants to {output_file}")

    return tx_plants


if __name__ == "__main__":
    df = download_eia860_plants()
    print(f"\nSample:\n{df.head()}")
    print(f"\nColumns: {list(df.columns)}")
