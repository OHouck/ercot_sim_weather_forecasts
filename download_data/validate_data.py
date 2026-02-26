"""validate_data.py — Check completeness of downloaded MVP data for July 2025."""

import os
import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from helper_funcs import setup_directories


def validate_july_2025():
    dirs = setup_directories()
    raw = dirs['raw']
    all_ok = True

    print("=" * 60)
    print("MVP Data Validation: July 2025")
    print("=" * 60)

    # 1. NDFD Forecasts
    print("\n--- NDFD Forecasts ---")
    for elem in ['temp', 'wspd', 'wdir']:
        nc_dir = Path(raw) / 'ndfd_data' / elem / '2025' / '07'
        if nc_dir.exists():
            nc_files = list(nc_dir.glob("*.nc"))
            status = "ok" if len(nc_files) >= 200 else "LOW"
            print(f"  {elem}: {len(nc_files)} files [{status}]")
            if len(nc_files) < 200:
                all_ok = False
        else:
            print(f"  {elem}: MISSING")
            all_ok = False

    # 2. Weather Stations
    print("\n--- Weather Station Data ---")
    stations_file = Path(raw) / 'weather_stations' / 'stations.csv'
    ws_dir = Path(raw) / 'weather_stations' / '2025' / '07'
    if stations_file.exists():
        stations = pd.read_csv(stations_file)
        print(f"  Station list: {len(stations)} stations")
    else:
        print(f"  Station list: MISSING")
        all_ok = False

    if ws_dir.exists():
        ws_files = list(ws_dir.glob("*.csv"))
        status = "ok" if len(ws_files) >= 50 else "LOW"
        print(f"  Data files: {len(ws_files)} station files [{status}]")
        if len(ws_files) < 50:
            all_ok = False

        # Spot check one file
        if ws_files:
            df = pd.read_csv(ws_files[0])
            print(f"  Sample file ({ws_files[0].name}): {len(df)} rows")
    else:
        print(f"  Data directory: MISSING")
        all_ok = False

    # 3. ERCOT DAM SPP
    print("\n--- ERCOT Day-Ahead SPP ---")
    dam_dir = Path(raw) / 'ercot' / 'dam_spp' / '2025' / '07'
    if dam_dir.exists():
        dam_files = sorted(dam_dir.glob("*.csv"))
        status = "ok" if len(dam_files) >= 31 else f"{len(dam_files)}/31"
        print(f"  Files: {len(dam_files)} [{status}]")
        if dam_files:
            df = pd.read_csv(dam_files[0])
            print(f"  Sample ({dam_files[0].name}): {len(df)} records, columns: {list(df.columns)}")
        if len(dam_files) < 31:
            all_ok = False
    else:
        print(f"  MISSING")
        all_ok = False

    # 4. ERCOT RT SPP
    print("\n--- ERCOT Real-Time SPP ---")
    rt_dir = Path(raw) / 'ercot' / 'rt_spp' / '2025' / '07'
    if rt_dir.exists():
        rt_files = sorted(rt_dir.glob("*.csv"))
        status = "ok" if len(rt_files) >= 31 else f"{len(rt_files)}/31"
        print(f"  Files: {len(rt_files)} [{status}]")
        if rt_files:
            df = pd.read_csv(rt_files[0])
            print(f"  Sample ({rt_files[0].name}): {len(df)} records")
        if len(rt_files) < 31:
            all_ok = False
    else:
        print(f"  MISSING")
        all_ok = False

    # Summary
    print("\n" + "=" * 60)
    if all_ok:
        print("All MVP data present and looks complete.")
    else:
        print("Some data missing or incomplete — see above.")
    print("=" * 60)


if __name__ == "__main__":
    validate_july_2025()
