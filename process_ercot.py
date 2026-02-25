"""process_ercot.py — Functions to read and process ERCOT market data."""

import os
import glob
import difflib
import pandas as pd
from helper_funcs import setup_directories


def load_dam_lmp_month(year, month):
    """Load all daily DAM LMP CSVs for a month into a single DataFrame.

    Args:
        year: Integer year
        month: Integer month

    Returns:
        DataFrame with columns: deliveryDate, hourEnding, busName, LMP, DSTFlag
    """
    dirs = setup_directories()
    data_dir = os.path.join(dirs['raw'], 'ercot', 'dam_lmp', str(year), f"{month:02d}")
    csv_files = sorted(glob.glob(os.path.join(data_dir, 'dam_lmp_*.csv')))

    if not csv_files:
        raise FileNotFoundError(f"No DAM LMP files found in {data_dir}")

    dfs = [pd.read_csv(f) for f in csv_files]
    df = pd.concat(dfs, ignore_index=True)
    df['LMP'] = pd.to_numeric(df['LMP'], errors='coerce')
    print(f"Loaded {len(df):,} DAM LMP records from {len(csv_files)} files")
    return df


def load_rt_spp_month(year, month):
    """Load all daily RT SPP CSVs for a month into a single DataFrame.

    Args:
        year: Integer year
        month: Integer month

    Returns:
        DataFrame with columns: deliveryDate, deliveryHour, deliveryInterval,
        settlementPoint, settlementPointType, settlementPointPrice, DSTFlag
    """
    dirs = setup_directories()
    data_dir = os.path.join(dirs['raw'], 'ercot', 'rt_spp', str(year), f"{month:02d}")
    csv_files = sorted(glob.glob(os.path.join(data_dir, 'rt_spp_*.csv')))

    if not csv_files:
        raise FileNotFoundError(f"No RT SPP files found in {data_dir}")

    dfs = [pd.read_csv(f) for f in csv_files]
    df = pd.concat(dfs, ignore_index=True)
    df['settlementPointPrice'] = pd.to_numeric(df['settlementPointPrice'], errors='coerce')
    print(f"Loaded {len(df):,} RT SPP records from {len(csv_files)} files")
    return df


def compute_max_lmp_by_node(year, month, point_type='RN'):
    """Compute maximum LMP per settlement point for a month.

    Uses RT SPP data (settlement points) rather than DAM LMP (buses) since
    settlement points have a mapping to units via NP4-160.

    Args:
        year: Integer year
        month: Integer month
        point_type: Settlement point type filter (default 'RN' for resource nodes)

    Returns:
        DataFrame with columns: settlementPoint, max_lmp
    """
    df = load_rt_spp_month(year, month)

    if point_type:
        df = df[df['settlementPointType'] == point_type]

    max_lmp = (df.groupby('settlementPoint')['settlementPointPrice']
               .max()
               .reset_index()
               .rename(columns={'settlementPointPrice': 'max_lmp'}))

    print(f"Computed max LMP for {len(max_lmp)} {point_type} settlement points")
    return max_lmp


def _clean_substation_name(sub):
    """Strip common ERCOT substation suffixes for name matching."""
    s = sub.replace('_', '')
    for suffix in ['ESS', 'BESS', 'SLR', 'SOLAR', 'WND', 'WIND']:
        if s.endswith(suffix) and len(s) > len(suffix) + 2:
            s = s[:-len(suffix)]
    return s


def build_node_coordinates(force_rebuild=False):
    """Build a settlement point to lat/lon coordinate mapping.

    Matches ERCOT resource node names (from NP4-160) to EIA Form 860 plant
    names to get geographic coordinates. Uses multi-strategy name matching:
    1. Exact prefix match (ERCOT abbreviation is prefix of EIA name)
    2. Substring containment (ERCOT abbreviation appears in EIA name)
    3. Fuzzy matching via difflib (cutoff=0.7)

    Args:
        force_rebuild: If True, rebuild even if cached file exists

    Returns:
        DataFrame with columns: settlement_point, lat, lon, plant_name, match_method
    """
    dirs = setup_directories()
    cache_file = os.path.join(dirs['processed'], 'node_coordinates.csv')

    if os.path.exists(cache_file) and not force_rebuild:
        df = pd.read_csv(cache_file)
        print(f"Loaded {len(df)} node coordinates from cache")
        return df

    # Load NP4-160 resource node to unit mapping
    np4_dir = os.path.join(dirs['raw'], 'ercot', 'np4_160')
    rn_files = glob.glob(os.path.join(np4_dir, 'Resource_Node_to_Unit_*.csv'))
    if not rn_files:
        raise FileNotFoundError(
            f"No NP4-160 Resource_Node_to_Unit file found in {np4_dir}. "
            "Run: uv run python -m download_data.pull_np4160")
    rn_df = pd.read_csv(rn_files[0])

    # Get unique resource_node -> substation mapping (take first substation per node)
    nodes = rn_df[['RESOURCE_NODE', 'UNIT_SUBSTATION']].drop_duplicates('RESOURCE_NODE')

    # Load EIA 860 Texas plants
    eia_file = os.path.join(dirs['raw'], 'eia860', 'texas_plants.csv')
    if not os.path.exists(eia_file):
        raise FileNotFoundError(
            f"EIA 860 data not found at {eia_file}. "
            "Run: uv run python -m download_data.pull_eia860")
    eia = pd.read_csv(eia_file)
    eia['norm_name'] = eia['plant_name'].str.upper().str.replace(r'[^A-Z0-9]', '', regex=True)
    eia_norms = eia['norm_name'].tolist()

    # Match each resource node
    results = []
    for _, row in nodes.iterrows():
        rn_name = row['RESOURCE_NODE']
        sub = row['UNIT_SUBSTATION']
        sub_clean = _clean_substation_name(sub)

        if len(sub_clean) < 3:
            continue

        match_row = None
        method = None

        # Strategy 1: Exact prefix match
        hits = eia[eia['norm_name'].str.startswith(sub_clean)]
        if len(hits) > 0:
            match_row = hits.iloc[0]
            method = 'prefix'
        else:
            # Strategy 2: Substring containment
            hits = eia[eia['norm_name'].str.contains(sub_clean, case=False, na=False)]
            if len(hits) > 0:
                match_row = hits.iloc[0]
                method = 'contains'
            else:
                # Strategy 3: Fuzzy matching
                matches = difflib.get_close_matches(sub_clean, eia_norms, n=1, cutoff=0.7)
                if matches:
                    match_row = eia[eia['norm_name'] == matches[0]].iloc[0]
                    method = 'fuzzy'

        if match_row is not None:
            results.append({
                'settlement_point': rn_name,
                'lat': float(match_row['lat']),
                'lon': float(match_row['lon']),
                'plant_name': match_row['plant_name'],
                'match_method': method,
            })

    result_df = pd.DataFrame(results)

    # Save cache
    os.makedirs(os.path.dirname(cache_file), exist_ok=True)
    result_df.to_csv(cache_file, index=False)

    total = len(nodes)
    matched = len(result_df)
    print(f"Matched {matched}/{total} resource nodes ({100*matched/total:.0f}%)")
    by_method = result_df['match_method'].value_counts()
    for method, count in by_method.items():
        print(f"  {method}: {count}")
    print(f"Saved to {cache_file}")

    return result_df


if __name__ == '__main__':
    coords = build_node_coordinates(force_rebuild=True)
    print(f"\nSample matched nodes:")
    print(coords.head(10).to_string(index=False))
