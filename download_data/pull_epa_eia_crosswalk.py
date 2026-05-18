"""pull_epa_eia_crosswalk.py — Download the EPA Power Sector Data Crosswalk.

The EPA's Power Sector Data Crosswalk maps EPA CAMD (CEMS) units to EIA Form
860 generators and boilers. Maintained by Catalyst Cooperative since EPA's
original 2021 release. Used as the authoritative Tier-A match for the
CEMS ↔ ERCOT crosswalk (``process_data.cems_ercot_crosswalk``); replaces the
prior name-matching heuristics for the ~95% of in-scope fossil units >25 MW
where EPA has done the manual curation.

Source: https://github.com/catalyst-cooperative/camd-eia-crosswalk-latest
Direct CSV: https://raw.githubusercontent.com/catalyst-cooperative/camd-eia-crosswalk-latest/main/epa_eia_crosswalk.csv

Output:
  {raw}/epa_eia_crosswalk/epa_eia_crosswalk.csv   — raw download (~7,200 rows)
"""

import sys
import argparse
import requests
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from helper_funcs import setup_directories

CROSSWALK_URL = (
    "https://raw.githubusercontent.com/catalyst-cooperative/"
    "camd-eia-crosswalk-latest/main/epa_eia_crosswalk.csv"
)


def download_crosswalk(force_rebuild=False):
    """Download the EPA CAMD-EIA crosswalk CSV to ``{raw}/epa_eia_crosswalk/``.

    Args:
        force_rebuild: Re-download even if cached file exists.

    Returns:
        Absolute Path to the cached CSV.
    """
    dirs = setup_directories()
    out_dir = Path(dirs["raw"]) / "epa_eia_crosswalk"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "epa_eia_crosswalk.csv"

    if out_path.exists() and not force_rebuild:
        print(f"  Cached: {out_path}")
        return out_path

    print(f"  Downloading EPA CAMD-EIA crosswalk from {CROSSWALK_URL}...")
    r = requests.get(CROSSWALK_URL, timeout=60)
    r.raise_for_status()
    out_path.write_bytes(r.content)
    print(f"  Saved {len(r.content) / 1024:.0f} KB to {out_path}")
    return out_path


def load_crosswalk(state="TX", force_rebuild=False):
    """Load the EPA crosswalk filtered to one state (or all).

    Args:
        state: Two-letter state code to filter ``CAMD_STATE`` on, or None
            for all rows.
        force_rebuild: Re-download the file if True.

    Returns:
        DataFrame with the following columns (renamed for downstream use):
            camd_plant_id   — EPA ORIS code (== EIA plant_code, == CEMS facilityId)
            camd_unit_id    — CEMS unitId (boiler/turbine label, e.g. 'CTG-1')
            eia_plant_id    — EIA plant_code (usually equals camd_plant_id)
            eia_generator_id — EIA generator_id
            eia_boiler_id   — EIA boiler_id
            eia_unit_type   — ST/CT/CA/CS/IC/GT (combined-cycle steam vs CT vs
                              combined, internal combustion, gas turbine, etc.)
            match_type_gen  — EPA's match strategy label (e.g. 'Step 1a: Exact match')
        Rows without an EIA generator match (``EIA_GENERATOR_ID`` blank) are
        dropped — we only need rows that bridge CEMS unit → EIA generator.
    """
    path = download_crosswalk(force_rebuild=force_rebuild)
    df = pd.read_csv(path, dtype={"CAMD_PLANT_ID": "Int64", "EIA_PLANT_ID": "Int64"})

    if state is not None:
        df = df[df["CAMD_STATE"] == state].copy()

    df = df.dropna(subset=["EIA_GENERATOR_ID"]).copy()
    out = pd.DataFrame({
        "camd_plant_id":    df["CAMD_PLANT_ID"].astype("Int64"),
        "camd_unit_id":     df["CAMD_UNIT_ID"].astype(str),
        "eia_plant_id":     df["EIA_PLANT_ID"].astype("Int64"),
        "eia_generator_id": df["EIA_GENERATOR_ID"].astype(str),
        "eia_boiler_id":    df["EIA_BOILER_ID"].astype(str),
        "eia_unit_type":    df["EIA_UNIT_TYPE"].astype(str),
        "match_type_gen":   df["MATCH_TYPE_GEN"].astype(str),
    })
    return out.reset_index(drop=True)


def main():
    """CLI: download the crosswalk and print a TX coverage summary."""
    parser = argparse.ArgumentParser(description="Download EPA CAMD-EIA crosswalk")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    download_crosswalk(force_rebuild=args.force)
    tx = load_crosswalk(state="TX")
    print(f"\n  TX rows: {len(tx):,}")
    print(f"  Distinct CAMD plants: {tx['camd_plant_id'].nunique()}")
    print(f"  Distinct (CAMD plant, unit) pairs: "
          f"{tx[['camd_plant_id', 'camd_unit_id']].drop_duplicates().shape[0]}")
    print(f"  EIA unit types:\n{tx['eia_unit_type'].value_counts().to_string()}")


if __name__ == "__main__":
    main()
