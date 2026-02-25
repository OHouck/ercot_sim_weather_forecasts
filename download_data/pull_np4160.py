"""pull_np4160.py — Download ERCOT NP4-160-SG settlement point mapping.

Downloads the Settlement Points List and Electrical Buses Mapping from
ERCOT's public MIS download. This maps settlement point names (used in
LMP/SPP data) to substations and resource node units.
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

# Most recent NP4-160-SG doclookupId (Feb 2026 ML4)
NP4_160_URL = (
    "https://www.ercot.com/misdownload/servlets/mirDownload"
    "?mimic_duns=000000000&doclookupId=1197364253"
)


def download_np4_160():
    """Download and extract ERCOT NP4-160-SG settlement point mapping.

    Downloads the ZIP containing CSVs that map settlement points to
    electrical buses, resource nodes, and unit names.

    Returns:
        dict mapping filename stems to DataFrames
    """
    dirs = setup_directories()
    output_dir = os.path.join(dirs['raw'], 'ercot', 'np4_160')

    # Check if already downloaded
    if os.path.exists(output_dir) and len(os.listdir(output_dir)) > 0:
        print(f"Already downloaded to {output_dir}")
        result = {}
        for f in os.listdir(output_dir):
            if f.endswith('.csv'):
                result[f] = pd.read_csv(os.path.join(output_dir, f))
        return result

    print(f"Downloading NP4-160-SG...")
    resp = requests.get(NP4_160_URL, timeout=120)
    resp.raise_for_status()
    print(f"  Downloaded {len(resp.content) / 1e6:.1f} MB")

    os.makedirs(output_dir, exist_ok=True)
    result = {}

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        print(f"  ZIP contents: {zf.namelist()}")
        for name in zf.namelist():
            if name.endswith('.csv'):
                with zf.open(name) as f:
                    df = pd.read_csv(f)
                # Save with original filename
                basename = os.path.basename(name)
                df.to_csv(os.path.join(output_dir, basename), index=False)
                result[basename] = df
                print(f"  Extracted {basename}: {len(df)} rows, columns: {list(df.columns)}")

    print(f"\nSaved to {output_dir}")
    return result


if __name__ == "__main__":
    dfs = download_np4_160()
    for name, df in dfs.items():
        print(f"\n=== {name} ===")
        print(df.head())
