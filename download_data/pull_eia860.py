"""pull_eia860.py — Download EIA Form 860 plant and generator data for Texas/ERCOT.

Downloads the EIA-860 annual ZIP which contains:
- Plant file (2___Plant*.xlsx): geographic coordinates for every US power plant
- Generator file (3_1_Generator*.xlsx): generator-level details including technology,
  capacity, and LMP node designations

Filters to Texas/ERCOT and saves:
- texas_plants.csv: plant-level data with lat/lon
- texas_generators.csv: generator-level data joined with plant lat/lon
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


def _download_zip():
    """Download the EIA 860 ZIP and return the bytes."""
    print(f"Downloading EIA Form 860 from {EIA860_URL}...")
    resp = requests.get(EIA860_URL, timeout=120)
    resp.raise_for_status()
    print(f"  Downloaded {len(resp.content) / 1e6:.1f} MB")
    return resp.content


def _extract_plants(zf):
    """Extract and filter the plant file from the ZIP. Returns DataFrame."""
    plant_files = [f for f in zf.namelist() if '2___Plant' in f and f.endswith('.xlsx')]
    if not plant_files:
        raise FileNotFoundError(f"No plant file found in ZIP. Contents: {zf.namelist()}")

    plant_file = plant_files[0]
    print(f"  Reading {plant_file}...")

    with zf.open(plant_file) as f:
        df = pd.read_excel(f, skiprows=1)

    print(f"  Total US plants: {len(df)}")

    # Standardize column names
    col_map = {}
    for col in df.columns:
        low = col.lower().strip()
        if low == 'state':
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

    # Filter to ERCOT
    ba_col = col_map.get('ba_code')
    tx_plants = df[df[ba_col] == 'ERCO'].copy()
    print(f"  Texas/ERCOT plants: {len(tx_plants)}")

    # Rename columns
    rename = {orig: std for std, orig in col_map.items()}
    tx_plants = tx_plants.rename(columns=rename)

    keep_cols = [c for c in ['plant_code', 'plant_name', 'state', 'county',
                              'lat', 'lon', 'ba_code', 'nerc_region']
                 if c in tx_plants.columns]
    tx_plants = tx_plants[keep_cols].copy()

    # Drop plants without coordinates
    before = len(tx_plants)
    tx_plants = tx_plants.dropna(subset=['lat', 'lon'])
    if before - len(tx_plants) > 0:
        print(f"  Dropped {before - len(tx_plants)} plants without coordinates")

    return tx_plants


def _extract_generators(zf, tx_plants):
    """Extract generator data from the ZIP, filter to ERCOT, join with plant coords."""
    gen_files = [f for f in zf.namelist() if '3_1_Generator' in f and f.endswith('.xlsx')]
    if not gen_files:
        raise FileNotFoundError(f"No generator file found in ZIP. Contents: {zf.namelist()}")

    gen_file = gen_files[0]
    print(f"  Reading {gen_file} (Operable sheet)...")

    with zf.open(gen_file) as f:
        # Read the first sheet (Operable) only
        xl = pd.ExcelFile(f)
        # Find sheet with "Operable" in the name, fall back to first sheet
        operable_sheet = None
        for sheet in xl.sheet_names:
            if 'operable' in sheet.lower():
                operable_sheet = sheet
                break
        if operable_sheet is None:
            operable_sheet = xl.sheet_names[0]
        print(f"  Using sheet: {operable_sheet}")
        df = xl.parse(operable_sheet, skiprows=1)

    print(f"  Total US generators (operable): {len(df)}")

    # Dynamic column mapping for generator fields
    col_map = {}
    for col in df.columns:
        low = col.lower().strip()
        if 'plant code' in low or 'plant id' in low:
            col_map['plant_code'] = col
        elif 'generator id' in low:
            col_map['generator_id'] = col
        elif 'technology' == low or low == 'technology':
            col_map['technology'] = col
        elif 'nameplate capacity' in low and 'mw' in low and 'planned' not in low:
            col_map['nameplate_capacity_mw'] = col
        elif 'nameplate power factor' in low:
            col_map['nameplate_power_factor'] = col
        elif ('lmp' in low and 'node' in low) or ('rto' in low and 'lmp' in low):
            col_map['lmp_node_designation'] = col
        elif low in ('prime mover code', 'prime mover'):
            col_map['prime_mover_code'] = col
        elif low == 'energy source 1':
            col_map['energy_source_1'] = col

    print(f"  Mapped generator columns: {col_map}")

    # Rename columns
    rename = {orig: std for std, orig in col_map.items()}
    df = df.rename(columns=rename)

    # Filter to ERCOT generators by joining on plant_code
    ercot_plant_codes = set(tx_plants['plant_code'].unique())
    df['plant_code'] = pd.to_numeric(df['plant_code'], errors='coerce')
    tx_gens = df[df['plant_code'].isin(ercot_plant_codes)].copy()
    print(f"  Texas/ERCOT generators: {len(tx_gens)}")

    # Keep only the columns we want
    keep_cols = [c for c in ['plant_code', 'generator_id', 'technology',
                              'nameplate_capacity_mw', 'nameplate_power_factor',
                              'lmp_node_designation',
                              'prime_mover_code', 'energy_source_1']
                 if c in tx_gens.columns]
    tx_gens = tx_gens[keep_cols].copy()

    # Merge with plant data for lat/lon and plant_name
    plant_coords = tx_plants[['plant_code', 'plant_name', 'lat', 'lon']].copy()
    tx_gens = tx_gens.merge(plant_coords, on='plant_code', how='left')

    # Reorder columns
    desired_order = ['plant_code', 'generator_id', 'plant_name', 'technology',
                     'prime_mover_code', 'energy_source_1',
                     'nameplate_capacity_mw', 'nameplate_power_factor',
                     'lmp_node_designation', 'lat', 'lon']
    tx_gens = tx_gens[[c for c in desired_order if c in tx_gens.columns]]

    return tx_gens


def download_eia860():
    """Download EIA Form 860 and extract Texas plant and generator data.

    Returns:
        tuple: (plants_df, generators_df)
    """
    dirs = setup_directories()
    output_dir = os.path.join(dirs['raw'], 'eia860')
    plants_file = os.path.join(output_dir, 'texas_plants.csv')
    gens_file = os.path.join(output_dir, 'texas_generators.csv')

    # Check if both files already exist
    if os.path.exists(plants_file) and os.path.exists(gens_file):
        print(f"Already downloaded: {plants_file}")
        print(f"Already downloaded: {gens_file}")
        return pd.read_csv(plants_file), pd.read_csv(gens_file)

    zip_bytes = _download_zip()

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        tx_plants = _extract_plants(zf)
        tx_gens = _extract_generators(zf, tx_plants)

    os.makedirs(output_dir, exist_ok=True)

    tx_plants.to_csv(plants_file, index=False)
    print(f"  Saved {len(tx_plants)} plants to {plants_file}")

    tx_gens.to_csv(gens_file, index=False)
    print(f"  Saved {len(tx_gens)} generators to {gens_file}")

    return tx_plants, tx_gens


def download_eia860_plants():
    """Backward-compatible wrapper. Downloads both files, returns plants DataFrame."""
    result = download_eia860()
    return result[0]


if __name__ == "__main__":
    plants, generators = download_eia860()
    print(f"\nPlants ({len(plants)} rows):")
    print(plants.head())
    print(f"\nGenerators ({len(generators)} rows):")
    print(generators.head())
    print(f"\nGenerator columns: {list(generators.columns)}")
    lmp_filled = generators['lmp_node_designation'].notna().sum() if 'lmp_node_designation' in generators.columns else 0
    print(f"Generators with LMP node designation: {lmp_filled}/{len(generators)}")
