"""process_ercot.py — Functions to read and process ERCOT market data."""

import os
import re
import glob
import difflib
import xml.etree.ElementTree as ET
import pandas as pd
from helper_funcs import setup_directories


def load_dam_spp_month(year, month):
    """Load all daily DAM settlement point price CSVs for a month.

    Args:
        year: Integer year
        month: Integer month

    Returns:
        DataFrame with DAM settlement point price columns
    """
    dirs = setup_directories()
    data_dir = os.path.join(dirs['raw'], 'ercot', 'dam_spp', str(year), f"{month:02d}")
    csv_files = sorted(glob.glob(os.path.join(data_dir, 'dam_spp_*.csv')))

    if not csv_files:
        raise FileNotFoundError(f"No DAM SPP files found in {data_dir}")

    dfs = [pd.read_csv(f) for f in csv_files]
    df = pd.concat(dfs, ignore_index=True)
    # Coerce price column — exact name will depend on API response fields
    for col in ['SettlementPointPrice', 'settlementPointPrice', 'LMP']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            break
    print(f"Loaded {len(df):,} DAM SPP records from {len(csv_files)} files")
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


def _parse_kml_coordinates(kml_path):
    """Parse ERCOT contour map KML file to extract node coordinates.

    These KML files come from ERCOT's real-time LMP contour map and contain
    authoritative lat/lon for settlement points. A cached 2019 snapshot with
    254 nodes is stored in data/rtmLmpPoints.kml.

    Args:
        kml_path: Path to the KML file

    Returns:
        DataFrame with columns: settlement_point, lat, lon, plant_name, match_method
    """
    tree = ET.parse(kml_path)
    root = tree.getroot()
    ns = {'kml': 'http://www.opengis.net/kml/2.2'}

    rows = []
    for pm in root.findall('.//kml:Placemark', ns):
        name_el = pm.find('kml:name', ns)
        coords_el = pm.find('.//kml:coordinates', ns)
        if name_el is None or coords_el is None:
            continue

        name = name_el.text.strip()
        lon, lat, _ = coords_el.text.strip().split(',')

        desc = (pm.find('kml:description', ns).text or '').replace('\n', ' ')
        plant_match = re.search(r'Plant Name:</strong><br\s*/?>\s*(.+?)\s*<', desc)
        plant = plant_match.group(1).strip() if plant_match else ''

        rows.append({
            'settlement_point': name,
            'lat': float(lat),
            'lon': float(lon),
            'plant_name': plant,
            'match_method': 'kml',
        })

    return pd.DataFrame(rows)


def build_node_coordinates(force_rebuild=False):
    """Build a settlement point to lat/lon coordinate mapping.

    Combines two coordinate sources, preferring the more authoritative one:
    1. ERCOT KML contour map (data/rtmLmpPoints.kml) — 254 nodes with
       authoritative coordinates directly from ERCOT. Used first.
    2. EIA Form 860 name matching via NP4-160 — fills in remaining nodes
       using prefix, substring, and fuzzy matching of substation names
       to EIA plant names.

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
    all_rn_names = set(nodes['RESOURCE_NODE'])

    # --- Source 1: KML coordinates (authoritative, from ERCOT contour map) ---
    kml_path = os.path.join(os.path.dirname(__file__), 'data', 'rtmLmpPoints.kml')
    kml_results = pd.DataFrame()
    if os.path.exists(kml_path):
        kml_all = _parse_kml_coordinates(kml_path)
        # Keep only nodes that appear in the current NP4-160 resource node list
        kml_results = kml_all[kml_all['settlement_point'].isin(all_rn_names)].copy()
        print(f"KML: {len(kml_all)} nodes parsed, {len(kml_results)} match current resource nodes")
    else:
        print(f"KML file not found at {kml_path}, skipping KML source")

    matched_by_kml = set(kml_results['settlement_point']) if len(kml_results) > 0 else set()

    # --- Source 2: EIA 860 name matching (for nodes not covered by KML) ---
    eia_file = os.path.join(dirs['raw'], 'eia860', 'texas_plants.csv')
    if not os.path.exists(eia_file):
        raise FileNotFoundError(
            f"EIA 860 data not found at {eia_file}. "
            "Run: uv run python -m download_data.pull_eia860")
    eia = pd.read_csv(eia_file)
    eia['norm_name'] = eia['plant_name'].str.upper().str.replace(r'[^A-Z0-9]', '', regex=True)
    eia_norms = eia['norm_name'].tolist()

    eia_results = []
    for _, row in nodes.iterrows():
        rn_name = row['RESOURCE_NODE']
        if rn_name in matched_by_kml:
            continue  # already have authoritative KML coordinates

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
            eia_results.append({
                'settlement_point': rn_name,
                'lat': float(match_row['lat']),
                'lon': float(match_row['lon']),
                'plant_name': match_row['plant_name'],
                'match_method': method,
            })

    eia_df = pd.DataFrame(eia_results)

    # Combine: KML first (authoritative), then EIA matches
    result_df = pd.concat([kml_results, eia_df], ignore_index=True)

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

    # Save unmatched ERCOT settlement points
    matched_rns = set(result_df['settlement_point'])
    unmatched_ercot = nodes[~nodes['RESOURCE_NODE'].isin(matched_rns)].copy()
    unmatched_ercot_file = os.path.join(dirs['processed'], 'unmatched_ercot_settlement_points.csv')
    unmatched_ercot.to_csv(unmatched_ercot_file, index=False)
    print(f"Saved {len(unmatched_ercot)} unmatched ERCOT settlement points to {unmatched_ercot_file}")

    # Save unmatched EIA 860 plants
    matched_plants = set(result_df['plant_name'])
    unmatched_eia = eia[~eia['plant_name'].isin(matched_plants)].copy()
    unmatched_eia_file = os.path.join(dirs['processed'], 'unmatched_eia860_plants.csv')
    unmatched_eia.to_csv(unmatched_eia_file, index=False)
    print(f"Saved {len(unmatched_eia)} unmatched EIA 860 plants to {unmatched_eia_file}")

    return result_df


if __name__ == '__main__':
    coords = build_node_coordinates(force_rebuild=True)
    print(f"\nSample matched nodes:")
    print(coords.head(10).to_string(index=False))
