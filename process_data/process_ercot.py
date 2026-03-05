"""process_ercot.py — Functions to read and process ERCOT market data."""

import os
import re
import glob
import difflib
import xml.etree.ElementTree as ET
import numpy as np
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
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


def compute_max_lmp_by_node(year, month, point_types='RN'):
    """Compute maximum LMP per settlement point for a month.

    Uses RT SPP data (settlement points) rather than DAM LMP (buses) since
    settlement points have a mapping to units via NP4-160.

    Args:
        year: Integer year
        month: Integer month
        point_types: Settlement point type(s) to include. Can be a string
            (e.g. 'RN') or a list (e.g. ['RN', 'PCCRN']). If None, includes
            all types. Default 'RN' for resource nodes only.

    Returns:
        DataFrame with columns: settlementPoint, max_lmp
    """
    df = load_rt_spp_month(year, month)

    if point_types is not None:
        if isinstance(point_types, str):
            point_types = [point_types]
        df = df[df['settlementPointType'].isin(point_types)]

    max_lmp = (df.groupby('settlementPoint')['settlementPointPrice']
               .max()
               .reset_index()
               .rename(columns={'settlementPointPrice': 'max_lmp'}))

    type_label = ','.join(point_types) if point_types else 'ALL'
    print(f"Computed max LMP for {len(max_lmp)} {type_label} settlement points")
    return max_lmp


def _clean_substation_name(sub):
    """Strip common ERCOT substation suffixes for name matching."""
    s = sub.replace('_', '')
    for suffix in ['ESS', 'BESS', 'SLR', 'SOLAR', 'WND', 'WIND']:
        if s.endswith(suffix) and len(s) > len(suffix) + 2:
            s = s[:-len(suffix)]
    return s


def _normalize_name(name):
    """Normalize names for matching by dropping non-alphanumeric chars."""
    return re.sub(r'[^A-Z0-9]+', '', str(name).upper())


def _strip_html_suffix(name):
    """Strip common HTML contour suffixes (e.g., _CC1, _PUN2, _RN)."""
    s = str(name).upper()
    s = re.sub(r'_(CCS?\d+|CC\d+|PUN\d+|RN\d*)$', '', s)
    return s


def _match_html_nodes_to_resource_nodes(html_df, rn_df):
    """Map HTML contour node names to current NP4-160 resource nodes.

    This resolves cases where HTML names use a different naming convention than
    NP4-160 RESOURCE_NODE names (e.g. TEN_CC1 -> TEN_CT1_STG) by leveraging
    UNIT_SUBSTATION and UNIT_NAME fields with conservative similarity checks.

    Args:
        html_df: DataFrame from _parse_html_contour_maps()
        rn_df: NP4-160 DataFrame with RESOURCE_NODE, UNIT_SUBSTATION, UNIT_NAME

    Returns:
        Tuple of:
        1) DataFrame shaped like html_df but with RESOURCE_NODE names in
           settlement_point and enriched match_method labels (one row per RN)
        2) DataFrame with one row per matched HTML node for manual QA
    """
    if html_df.empty or rn_df.empty:
        empty_main = pd.DataFrame(columns=['settlement_point', 'lat', 'lon', 'plant_name', 'match_method'])
        empty_detail = pd.DataFrame(columns=[
            'html_settlement_point', 'resource_node', 'unit_substation',
            'match_method', 'match_score', 'lat', 'lon'
        ])
        return empty_main, empty_detail

    # Build per-resource-node matching metadata.
    grouped = rn_df.groupby('RESOURCE_NODE', as_index=False).agg({
        'UNIT_SUBSTATION': 'first',
        'UNIT_NAME': lambda x: sorted(set(str(v) for v in x if pd.notna(v))),
    })

    rn_meta = {}
    exact_index = {}
    substation_index = {}
    for _, row in grouped.iterrows():
        rn = row['RESOURCE_NODE']
        sub = str(row['UNIT_SUBSTATION'])
        unit_names = row['UNIT_NAME']

        rn_norm = _normalize_name(rn)
        sub_norm = _normalize_name(_clean_substation_name(sub))
        unit_norms = [_normalize_name(u) for u in unit_names]

        rn_meta[rn] = {
            'rn_norm': rn_norm,
            'sub_norm': sub_norm,
            'unit_norms': unit_norms,
        }
        exact_index.setdefault(rn_norm, []).append(rn)
        substation_index.setdefault(sub_norm, []).append(rn)

    method_rank = {
        'html_contour_exact': 4,
        'html_contour_substation_unique': 3,
        'html_contour_substation_tiebreak': 3,
        'html_contour_substation_scored': 2,
        'html_contour_heuristic': 1,
    }

    chosen_by_rn = {}
    html_match_rows = []

    for _, row in html_df.iterrows():
        html_name = row['settlement_point']
        html_norm = _normalize_name(html_name)
        html_base_norm = _normalize_name(_strip_html_suffix(html_name))

        best_rn = None
        best_method = None
        best_score = 0.0

        # 1) Exact normalized RESOURCE_NODE match.
        exact_hits = exact_index.get(html_norm, [])
        if len(exact_hits) == 1:
            best_rn = exact_hits[0]
            best_method = 'html_contour_exact'
            best_score = 1.0
        elif len(exact_hits) > 1:
            best_rn = sorted(exact_hits)[0]
            best_method = 'html_contour_exact'
            best_score = 1.0
        else:
            # 2) Candidate set driven by substation aliasing around stripped HTML name.
            candidates = set()
            for sub_norm, rns in substation_index.items():
                if (sub_norm == html_base_norm
                        or sub_norm.startswith(html_base_norm)
                        or html_base_norm.startswith(sub_norm)):
                    candidates.update(rns)

            if len(candidates) == 1:
                best_rn = next(iter(candidates))
                best_method = 'html_contour_substation_unique'
                best_score = 0.95
            else:
                # If HTML base maps to a substation with multiple units, use a
                # deterministic tie-break based on unit number proximity.
                candidate_substations = {rn_meta[rn]['sub_norm'] for rn in candidates}
                has_substation_family = any(
                    sub == html_base_norm
                    or sub.startswith(html_base_norm)
                    or html_base_norm.startswith(sub)
                    for sub in candidate_substations
                )

                if has_substation_family and len(candidates) > 1:
                    suffix_num_match = re.search(r'_(?:CCS?|CC|PUN|RN)?(\d+)$', str(html_name).upper())
                    if suffix_num_match:
                        target_num = int(suffix_num_match.group(1))
                        ranked = []
                        for rn in sorted(candidates):
                            unit_nums = []
                            for u in rn_meta[rn]['unit_norms']:
                                num_match = re.search(r'(\d+)$', u)
                                if num_match:
                                    unit_nums.append(int(num_match.group(1)))
                            rn_num_match = re.search(r'(\d+)$', rn_meta[rn]['rn_norm'])
                            if rn_num_match:
                                unit_nums.append(int(rn_num_match.group(1)))
                            if unit_nums:
                                nearest_delta = min(abs(n - target_num) for n in unit_nums)
                            else:
                                nearest_delta = 999
                            ranked.append((nearest_delta, rn))

                        ranked.sort()
                        if ranked and ranked[0][0] < 999:
                            best_rn = ranked[0][1]
                            best_method = 'html_contour_substation_tiebreak'
                            # Map small deltas to slightly higher confidence.
                            best_score = max(0.90, 0.96 - 0.01 * ranked[0][0])

                # 3) Conservative scoring over likely candidates (or all nodes if none).
                if best_rn is None and not candidates:
                    candidates = set(rn_meta.keys())

                if best_rn is None:
                    scored = []
                    for rn in candidates:
                        meta = rn_meta[rn]
                        sub_score = difflib.SequenceMatcher(None, html_base_norm, meta['sub_norm']).ratio()
                        rn_score = difflib.SequenceMatcher(None, html_norm, meta['rn_norm']).ratio()
                        unit_score = max(
                            [difflib.SequenceMatcher(None, html_norm, u).ratio() for u in meta['unit_norms']] or [0.0]
                        )

                        score = max(sub_score, rn_score, unit_score)
                        if meta['sub_norm'].startswith(html_base_norm) or html_base_norm.startswith(meta['sub_norm']):
                            score += 0.08
                        if meta['rn_norm'].startswith(html_base_norm) or html_base_norm in meta['rn_norm']:
                            score += 0.05

                        scored.append((score, rn))

                    scored.sort(reverse=True)
                    if scored:
                        top_score, top_rn = scored[0]
                        second_score = scored[1][0] if len(scored) > 1 else 0.0

                        if top_score >= 0.90 and (top_score - second_score) >= 0.03:
                            best_rn = top_rn
                            best_method = (
                                'html_contour_substation_scored' if len(candidates) > 1
                                else 'html_contour_heuristic'
                            )
                            best_score = float(top_score)

        if best_rn is None:
            continue

        candidate_row = {
            'settlement_point': best_rn,
            'lat': row['lat'],
            'lon': row['lon'],
            'plant_name': row.get('plant_name', ''),
            'match_method': best_method,
            '_match_score': best_score,
        }

        html_match_rows.append({
            'html_settlement_point': html_name,
            'resource_node': best_rn,
            'unit_substation': rn_df.loc[rn_df['RESOURCE_NODE'] == best_rn, 'UNIT_SUBSTATION'].iloc[0],
            'match_method': best_method,
            'match_score': round(float(best_score), 4),
            'lat': row['lat'],
            'lon': row['lon'],
        })

        # If multiple HTML names map to the same RN, keep the strongest match.
        existing = chosen_by_rn.get(best_rn)
        if existing is None:
            chosen_by_rn[best_rn] = candidate_row
        else:
            existing_rank = method_rank.get(existing['match_method'], 0)
            candidate_rank = method_rank.get(candidate_row['match_method'], 0)
            if (candidate_rank > existing_rank or
                    (candidate_rank == existing_rank and candidate_row['_match_score'] > existing['_match_score'])):
                chosen_by_rn[best_rn] = candidate_row

    if not chosen_by_rn:
        empty_main = pd.DataFrame(columns=['settlement_point', 'lat', 'lon', 'plant_name', 'match_method'])
        html_match_df = pd.DataFrame(html_match_rows)
        return empty_main, html_match_df

    matched_df = pd.DataFrame(chosen_by_rn.values())
    html_match_df = pd.DataFrame(html_match_rows)
    return matched_df[['settlement_point', 'lat', 'lon', 'plant_name', 'match_method']], html_match_df


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


def _extract_html_image_map_nodes(html_text):
    """Extract node names and pixel coordinates from an ERCOT contour map HTML.

    Args:
        html_text: Raw HTML string containing <area> tags

    Returns:
        dict mapping node name -> (x, y) pixel coordinates
    """
    nodes = {}
    for m in re.finditer(
        r'<area\s+shape="circle"\s+coords="(\d+),(\d+),\d+"\s+title="([^:]+):', html_text
    ):
        x, y, name = int(m.group(1)), int(m.group(2)), m.group(3).strip()
        nodes[name] = (x, y)
    return nodes


def _parse_html_contour_maps(html_paths, kml_path=None):
    """Parse ERCOT contour map HTML files to extract node coordinates.

    ERCOT serves four live contour map pages, each with ~253 nodes on a
    600x600 PNG image map. Different pages show different subsets of nodes,
    yielding ~295 unique nodes when combined. Pixel coordinates are converted
    to lat/lon via an affine transformation calibrated against the 2019 KML.

    Args:
        html_paths: List of paths to HTML source files (or single path string)
        kml_path: Optional path to KML file for calibrating the transformation

    Returns:
        DataFrame with columns: settlement_point, lat, lon, plant_name, match_method
    """
    if isinstance(html_paths, str):
        html_paths = [html_paths]

    # Combine nodes from all HTML files (first occurrence wins)
    html_nodes = {}
    for path in html_paths:
        if not os.path.exists(path):
            continue
        with open(path) as f:
            page_nodes = _extract_html_image_map_nodes(f.read())
        for name, coords in page_nodes.items():
            if name not in html_nodes:
                html_nodes[name] = coords

    if not html_nodes:
        return pd.DataFrame()

    print(f"HTML contour maps: {len(html_nodes)} unique nodes from "
          f"{len(html_paths)} pages")

    # Fit affine transformation using KML ground control points
    if kml_path and os.path.exists(kml_path):
        kml_df = _parse_kml_coordinates(kml_path)
        kml_coords = {
            row['settlement_point']: (row['lat'], row['lon'])
            for _, row in kml_df.iterrows()
        }
        common = sorted(set(html_nodes) & set(kml_coords))

        if len(common) >= 10:
            A = np.array([[1, html_nodes[n][0], html_nodes[n][1]] for n in common])
            lat_vec = np.array([kml_coords[n][0] for n in common])
            lon_vec = np.array([kml_coords[n][1] for n in common])

            lat_coeffs, _, _, _ = np.linalg.lstsq(A, lat_vec, rcond=None)
            lon_coeffs, _, _, _ = np.linalg.lstsq(A, lon_vec, rcond=None)
            print(f"  Affine transform fitted from {len(common)} ground control points")
        else:
            print(f"  WARNING: Only {len(common)} common nodes, using hardcoded coefficients")
            lat_coeffs = np.array([36.796687, 0.000005, -0.018760])
            lon_coeffs = np.array([-107.009848, 0.023113, -0.000004])
    else:
        # Hardcoded coefficients from Feb 2026 HTML + 2019 KML calibration
        lat_coeffs = np.array([36.796687, 0.000005, -0.018760])
        lon_coeffs = np.array([-107.009848, 0.023113, -0.000004])

    # Convert all HTML nodes to lat/lon
    rows = []
    for name, (px, py) in html_nodes.items():
        lat = lat_coeffs[0] + lat_coeffs[1] * px + lat_coeffs[2] * py
        lon = lon_coeffs[0] + lon_coeffs[1] * px + lon_coeffs[2] * py
        rows.append({
            'settlement_point': name,
            'lat': round(float(lat), 4),
            'lon': round(float(lon), 4),
            'plant_name': '',
            'match_method': 'html_contour',
        })

    return pd.DataFrame(rows)


def build_node_coordinates(force_rebuild=False):
    """Build a settlement point to lat/lon coordinate mapping.

    Combines three coordinate sources in priority order:
    1. ERCOT HTML contour map (data/rtmLmp_html_source.txt) — current (2026)
       node positions converted from pixel coords via affine transformation
       calibrated against KML ground control points. ~253 nodes.
    2. ERCOT KML contour map (data/rtmLmpPoints.kml) — 2019 snapshot with
       254 nodes. Fills in any nodes not in the HTML source.
    3. EIA Form 860 name matching via NP4-160 — fills in remaining nodes
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

    data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
    kml_path = os.path.join(data_dir, 'rtmLmpPoints.kml')

    # --- Source 1: HTML contour maps (current, preferred) ---
    # ERCOT serves 4 contour map pages with overlapping but different node sets
    html_files = [
        os.path.join(data_dir, f)
        for f in ['rtmLmp_html_source.txt', 'rtmSpp_html_source.txt',
                   'damSpp2_html_source.txt', 'damSpp7_html_source.txt']
        if os.path.exists(os.path.join(data_dir, f))
    ]
    html_results = pd.DataFrame()
    html_match_detail = pd.DataFrame()
    if html_files:
        html_all = _parse_html_contour_maps(html_files, kml_path)
        html_results, html_match_detail = _match_html_nodes_to_resource_nodes(html_all, rn_df)
        print(f"HTML: {len(html_all)} nodes parsed, {len(html_results)} mapped to current resource nodes")
    else:
        print("No HTML contour map files found, skipping HTML source")

    # Debug output: HTML names still unmatched after current HTML->RN mapping logic.
    unmatched_html_results = html_all[~html_all['settlement_point'].isin(
        set(html_match_detail['html_settlement_point'])
    )]
    unmatched_html_results.to_csv(os.path.join(dirs['processed'], 'unmatched_html_nodes.csv'), index=False)

    # Debug/manual QA output: inspect HTML->resource mapping quality.
    html_match_detail.to_csv(
        os.path.join(dirs['processed'], 'html_resource_node_match_details.csv'),
        index=False,
    )

    print(f"Saved {len(html_match_detail)} HTML match detail rows to "
          f"{os.path.join(dirs['processed'], 'html_resource_node_match_details.csv')}")
    print(f"Saved {len(unmatched_html_results)} unmatched HTML nodes to "
          f"{os.path.join(dirs['processed'], 'unmatched_html_nodes.csv')}")


    matched_so_far = set(html_results['settlement_point']) if len(html_results) > 0 else set()

    # --- Source 2: KML coordinates (2019 snapshot, fills gaps) ---
    kml_results = pd.DataFrame()
    if os.path.exists(kml_path):
        kml_all = _parse_kml_coordinates(kml_path)
        # Only keep nodes NOT already matched by HTML
        kml_new = kml_all[
            kml_all['settlement_point'].isin(all_rn_names)
            & ~kml_all['settlement_point'].isin(matched_so_far)
        ].copy()
        kml_results = kml_new
        print(f"KML: {len(kml_all)} nodes parsed, {len(kml_results)} new matches "
              f"(not in HTML)")
    else:
        print(f"KML file not found at {kml_path}, skipping KML source")

    matched_so_far |= set(kml_results['settlement_point']) if len(kml_results) > 0 else set()

    # --- Source 3: EIA 860 name matching (for remaining nodes) ---
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
        if rn_name in matched_so_far:
            continue  # already have coordinates from HTML or KML

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

    # Combine: HTML first (current), then KML (2019), then EIA name matches
    result_df = pd.concat([html_results, kml_results, eia_df], ignore_index=True)

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
