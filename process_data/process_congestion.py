"""Process SCED shadow prices into hourly congestion metrics.

Reads daily SCED shadow price CSVs (from pull_sced_shadow.py) and produces:
1. System-level hourly congestion metrics (n_binding, total_shadow_cost, etc.)
2. Per-constraint hourly shadow prices with geolocation via Bus_Output.shp

These metrics can replace or supplement system_lmp_std as the dependent
variable in regression analysis.

Usage:
    from process_data.process_congestion import compute_hourly_congestion_metrics
    cong = compute_hourly_congestion_metrics(2025, 7)
"""

import calendar
import os
import re
import sys
import urllib.request
from difflib import get_close_matches
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from helper_funcs import setup_directories

ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def _load_shadow_month(year, month):
    """Load all daily shadow price CSVs for one month into a single DataFrame.

    Returns:
        DataFrame with all raw shadow price records for the month, or None
        if no files exist.
    """
    dirs = setup_directories()
    shadow_dir = os.path.join(
        dirs["raw"], "ercot", "sced_shadow", str(year), f"{month:02d}"
    )

    if not os.path.isdir(shadow_dir):
        raise FileNotFoundError(
            f"Shadow price directory not found: {shadow_dir}\n"
            f"Run: uv run python -m download_data.pull_sced_shadow "
            f"--year {year} --month {month}"
        )

    num_days = calendar.monthrange(year, month)[1]
    frames = []
    for day in range(1, num_days + 1):
        fname = f"shadow_{year}{month:02d}{day:02d}.csv"
        fpath = os.path.join(shadow_dir, fname)
        if os.path.exists(fpath):
            df = pd.read_csv(fpath)
            frames.append(df)

    if not frames:
        raise FileNotFoundError(
            f"No shadow price CSVs found in {shadow_dir}"
        )

    combined = pd.concat(frames, ignore_index=True)
    print(f"  Loaded {len(combined):,} shadow records for {year}-{month:02d} "
          f"({len(frames)} days)")
    return combined


def _parse_sced_timestamps(df):
    """Parse SCEDTimestamp → valid_time (hourly, US/Central tz-naive).

    SCED timestamps are in US/Central (matching ERCOT's operational time).
    We floor them to the hour for merging with other hourly datasets.
    """
    df = df.copy()
    # SCEDTimestamp is ISO-format string: '2025-07-15T23:55:10'
    df["sced_time"] = pd.to_datetime(df["SCEDTimestamp"])
    df["valid_time"] = df["sced_time"].dt.floor("h")
    return df


# ---------------------------------------------------------------------------
# System-level hourly congestion metrics
# ---------------------------------------------------------------------------

def compute_hourly_congestion_metrics(year, month, force_rebuild=False):
    """Compute hourly system-level congestion metrics from SCED shadow prices.

    For each hour, aggregates across all binding constraints to produce:
    - n_binding_constraints: distinct constraint names
    - total_shadow_cost: sum of |shadowPrice| across all SCED intervals
    - max_shadow_price: peak shadow price in the hour
    - mean_shadow_price: mean shadow price across binding intervals
    - n_violations: constraints with violatedMW > 0
    - total_violated_mw: sum of violated MW (positive only)
    - shadow_cost_weighted: sum of shadowPrice × max(violatedMW, 0) for each
      SCED interval (proxy for congestion rent per interval)

    Args:
        year: Integer year.
        month: Integer month.
        force_rebuild: If True, recompute even if cached.

    Returns:
        DataFrame with one row per hour (valid_time column).
    """
    dirs = setup_directories()
    cache_path = os.path.join(
        dirs["processed"], "congestion_metrics",
        f"congestion_hourly_{year}{month:02d}.csv",
    )

    if os.path.exists(cache_path) and not force_rebuild:
        print(f"  Loading cached congestion metrics: {cache_path}")
        df = pd.read_csv(cache_path, parse_dates=["valid_time"])
        return df

    # Load raw data
    raw = _load_shadow_month(year, month)
    raw = _parse_sced_timestamps(raw)

    # Ensure numeric types
    raw["shadowPrice"] = pd.to_numeric(raw["shadowPrice"], errors="coerce")
    raw["violatedMW"] = pd.to_numeric(raw["violatedMW"], errors="coerce")

    # Per-SCED-interval metrics (before hourly aggregation)
    # violatedMW can be negative (constraint relieved)
    if (raw["shadowPrice"] < 0).any():
        print("  WARNING: Found negative shadow prices in raw data")
        print(raw[raw["shadowPrice"] < 0][["constraintName", "shadowPrice"]].head())    
    # only positive violations count for shadow cost. Negative values mean underutilization
    raw["violated_pos"] = raw["violatedMW"].clip(lower=0) 
    raw["shadow_x_violated"] = raw["shadowPrice"] * raw["violated_pos"]

    # Aggregate to hourly
    hourly = (
        raw.groupby("valid_time")
        .agg(
            n_binding_constraints=("constraintName", "nunique"),
            n_sced_intervals=("sced_time", "nunique"),
            total_shadow_cost=("shadowPrice", "sum"),
            max_shadow_price=("shadowPrice", "max"),
            mean_shadow_price=("shadowPrice", "mean"),
            n_violations=("violated_pos", lambda x: (x > 0).sum()),
            total_violated_mw=("violated_pos", "sum"),
            shadow_cost_weighted=("shadow_x_violated", "sum"),
        )
        .reset_index()
    )

    # Normalize total_shadow_cost by number of SCED intervals to get
    # a per-interval average (since hours have ~12 SCED runs)
    hourly["mean_shadow_cost_per_interval"] = (
        hourly["total_shadow_cost"] / hourly["n_sced_intervals"]
    )

    # First-interval shadow cost: sum of shadow prices across all binding
    # constraints in only the earliest SCED run within each hour.
    # This avoids accumulation across ~12 intervals and gives a clean snapshot.
    first_sced_time = raw.groupby("valid_time")["sced_time"].transform("min")
    first_interval_raw = raw[raw["sced_time"] == first_sced_time]
    first_interval_cost = (
        first_interval_raw.groupby("valid_time")["shadowPrice"]
        .sum()
        .rename("first_interval_shadow_cost")
    )
    hourly = hourly.merge(first_interval_cost, on="valid_time", how="left")

    # Cache
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    hourly.to_csv(cache_path, index=False)
    print(f"  Saved congestion metrics: {cache_path}")
    print(f"    {len(hourly)} hours, "
          f"mean shadow cost={hourly['total_shadow_cost'].mean():.1f}, "
          f"mean n_binding={hourly['n_binding_constraints'].mean():.1f}")

    return hourly


# ---------------------------------------------------------------------------
# Per-constraint hourly shadow prices (with geolocation)
# ---------------------------------------------------------------------------

def _load_node_coordinates():
    """Load ERCOT node coordinates from node_coordinates.csv created in process_ercot.build_node_coordinates().

    Returns:
        DataFrame with columns: settlement_point, lat, lon, prefix
        where prefix is the first token of the settlement point name (uppercase).
    """
    dirs = setup_directories()
    node_path = os.path.join(dirs["processed"], "node_coordinates.csv")
    if not os.path.exists(node_path):
        raise FileNotFoundError(
            f"node_coordinates.csv not found: {node_path}\n"
            f"Run: process_ercot.build_node_coordinates()"
        )
    nodes = pd.read_csv(node_path)
    nodes["prefix"] = nodes["settlement_point"].str.split("_").str[0].str.upper()
    return nodes


def _load_np4160_substation_coords():
    """Build SUBSTATION → (lat, lon) mapping via NP4-160 resource node linkage.

    NP4-160 Settlement_Points file maps SUBSTATION names (which match shadow
    price fromStation/toStation) to RESOURCE_NODEs. We join those resource
    nodes to node_coordinates.csv to get lat/lon.

    Returns:
        dict mapping SUBSTATION (uppercase) → (lat, lon), or empty dict.
    """
    dirs = setup_directories()
    np4_dir = os.path.join(dirs["raw"], "ercot", "np4_160")
    sp_files = [f for f in os.listdir(np4_dir) if f.startswith("Settlement_Points_")]
    if not sp_files:
        return {}

    sp = pd.read_csv(os.path.join(np4_dir, sp_files[0]))
    sp_with_rn = sp[sp["RESOURCE_NODE"].notna() & (sp["RESOURCE_NODE"] != "")].copy()

    try:
        nodes = _load_node_coordinates()
    except FileNotFoundError:
        return {}

    nc_lookup = {
        row["settlement_point"]: (row["lat"], row["lon"])
        for _, row in nodes.iterrows()
    }

    # For each SUBSTATION, average coords of all resource nodes that have coords
    sub_coords: dict[str, list] = {}
    for _, row in sp_with_rn.iterrows():
        rn = row["RESOURCE_NODE"]
        if rn in nc_lookup:
            sub = str(row["SUBSTATION"]).upper()
            if sub not in sub_coords:
                sub_coords[sub] = []
            sub_coords[sub].append(nc_lookup[rn])

    return {
        sub: (
            float(np.mean([c[0] for c in coords])),
            float(np.mean([c[1] for c in coords])),
        )
        for sub, coords in sub_coords.items()
    }


def _load_census_tx_places():
    """Load US Census Texas places and county subdivisions for geocoding.

    Downloads the 2025 Census Gazetteer for Texas (FIPS 48) on first call
    and caches to raw_data/census_places_tx_2025.txt.

    Returns:
        dict mapping cleaned place name (uppercase alpha only) to metadata:
        {
            "coords": (lat, lon),
            "source_name": original place name from Census file,
        }
    """
    dirs = setup_directories()
    cache_places = os.path.join(dirs["raw"], "census_places_tx_2025.txt")
    cache_cousubs = os.path.join(dirs["raw"], "census_cousubs_tx_2025.txt")

    base_url = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2025_Gazetteer"
    for url, local in [
        (f"{base_url}/2025_gaz_place_48.txt", cache_places),
        (f"{base_url}/2025_gaz_cousubs_48.txt", cache_cousubs),
    ]:
        if not os.path.exists(local):
            try:
                urllib.request.urlretrieve(url, local)
            except Exception as e:
                print(f"  WARNING: Could not download Census gazetteer: {e}")

    lookup: dict[str, dict] = {}
    for local in [cache_places, cache_cousubs]:
        if not os.path.exists(local):
            continue
        df = pd.read_csv(local, sep="|")
        name_col = "NAME" if "NAME" in df.columns else df.columns[4]
        for _, row in df.iterrows():
            original_name = str(row[name_col])
            # Strip legal suffixes (city, town, village, CDP)
            name = re.sub(
                r"\s+(city|town|village|CDP|borough)$",
                "",
                original_name,
                flags=re.IGNORECASE,
            )
            clean = re.sub(r"[^A-Z]", "", name.upper())
            if clean and clean not in lookup:
                lookup[clean] = {
                    "coords": (
                        float(row["INTPTLAT"]),
                        float(str(row["INTPTLONG"]).strip()),
                    ),
                    "source_name": original_name,
                }

    return lookup


def _psse_name_candidates(psse_name: str) -> list[str]:
    """Extract candidate place-name tokens from a PSSE bus name.

    PSSE bus names encode substation names with suffixes like:
    - ALPINE2A  → ALPINE
    - ARGYLE1_8 → ARGYLE
    - L_BERGHE8_1Y → BERGHE
    - CHCOGHLL1_8  → CHCOGHLL (China Grove Hills)

    Returns list of cleaned alpha-only tokens, longest first.
    """
    name = str(psse_name).upper()
    candidates = []
    # Remove common line prefixes
    base = re.sub(r"^[LP]_", "", name)
    # Strip from first digit (removes voltage/ID suffixes)
    stripped = re.sub(r"\d.*", "", base)
    clean = re.sub(r"[^A-Z]", "", stripped)
    if clean:
        candidates.append(clean)
    # Also split on underscores and collect parts
    for part in name.split("_"):
        p = re.sub(r"[^A-Z]", "", part)
        if len(p) >= 4:
            candidates.append(p)
    return list(dict.fromkeys(candidates))  # deduplicate, preserve order


def _load_eia_generators():
    """Load EIA Form 860 Texas generators with lat/lon and LMP node designation.

    Returns:
        dict mapping cleaned key (uppercase alpha only) to metadata:
        {
            "coords": (lat, lon),
            "source_name": original EIA plant or node designation name,
        }
    """
    dirs = setup_directories()
    eia_path = os.path.join(dirs["raw"], "eia860", "texas_generators.csv")
    if not os.path.exists(eia_path):
        raise FileNotFoundError(
            f"texas_generators.csv not found: {eia_path}\n"
            f"Run: download_data.pull_eia860"
        )
    eia = pd.read_csv(eia_path)
    eia_valid = eia.dropna(subset=["lat", "lon"])
    lookup: dict[str, dict] = {}
    for _, row in eia_valid.iterrows():
        coords = (float(row["lat"]), float(row["lon"]))

        pname_raw = str(row["plant_name"])
        pname = re.sub(r"[^A-Z]", "", pname_raw.upper())
        if pname and pname not in lookup:
            lookup[pname] = {
                "coords": coords,
                "source_name": pname_raw,
            }

        if pd.notna(row.get("lmp_node_designation")):
            node_raw = str(row["lmp_node_designation"])
            prefix = node_raw.split("_")[0].upper()
            if prefix and prefix not in lookup:
                lookup[prefix] = {
                    "coords": coords,
                    "source_name": node_raw,
                }
    return lookup


def _build_station_coords(shadow_stations, return_details=False):
    """Build station → (lat, lon) mapping using multiple data sources.

    Matching strategies (applied in priority order):

    1. NP4-160 SUBSTATION path: shadow station name matches a SUBSTATION in
       the NP4-160 Settlement Points file; that substation's resource node(s)
       are looked up in node_coordinates.csv.  (~71 matches, most reliable)

    2. Census place geocoding: shadow station name or PSSE bus name (cleaned)
       matches a US Census Texas place or county subdivision name.
       (~129 matches — covers transmission-only substations named after towns)

    3. EIA generator exact match: station name matches an EIA plant name or
       LMP node prefix exactly. (~1 additional match)

    4. Node coordinates prefix match (legacy): station name matches the first
       token of a settlement point name. Superseded by strategy 1 but kept
       as a fallback.

    5. Bus_Output.shp fallback: 123-bus simulation model shapefile.

    Args:
        shadow_stations: set of station names from shadow price data.
        return_details: if True, include station-level matching metadata.

    Returns:
        if return_details is False:
            (station_coords, match_methods)
        if return_details is True:
            (station_coords, match_methods, match_details)
            where match_details[station] has cleaned_name, matched_source_name,
            and source metadata.
    """
    station_coords: dict[str, tuple] = {}
    match_methods: dict[str, str] = {}
    match_details: dict[str, dict] = {}

    def _set_match(station, coords, method, cleaned_name=None, matched_name=""):
        station_coords[station] = coords
        match_methods[station] = method
        match_details[station] = {
            "cleaned_name": cleaned_name or re.sub(r"[^A-Z]", "", str(station).upper()),
            "matched_source_name": matched_name,
            "source": method,
        }

    # --- Strategy 1: NP4-160 SUBSTATION → resource node → coordinates ---
    try:
        sub_lookup = _load_np4160_substation_coords()
        for station in shadow_stations:
            if station in station_coords:
                continue
            key = str(station).upper()
            if key in sub_lookup:
                _set_match(
                    station,
                    sub_lookup[key],
                    "np4160_substation",
                    cleaned_name=key,
                    matched_name=str(station),
                )
    except Exception as e:
        print(f"  WARNING: NP4-160 substation matching failed: {e}")

    # --- Strategy 2: Census Texas place geocoding ---
    try:
        # Also need PSSE bus names for unmatched stations
        dirs = setup_directories()
        np4_dir = os.path.join(dirs["raw"], "ercot", "np4_160")
        sp_files = [f for f in os.listdir(np4_dir) if f.startswith("Settlement_Points_")]
        psse_map: dict[str, str] = {}
        if sp_files:
            sp_df = pd.read_csv(os.path.join(np4_dir, sp_files[0]))
            for _, row in sp_df.iterrows():
                sub = str(row["SUBSTATION"]).upper()
                psse = str(row.get("PSSE_BUS_NAME", ""))
                if sub not in psse_map and psse and psse != "nan":
                    psse_map[sub] = psse

        census = _load_census_tx_places()

        def _census_match(station):
            sub_clean = re.sub(r"[^A-Z]", "", str(station).upper())
            psse = psse_map.get(str(station).upper(), "")
            candidates = [sub_clean] + _psse_name_candidates(psse)

            # Exact match
            for cand in candidates:
                if cand in census:
                    return census[cand]["coords"], "census_exact", cand, census[cand]["source_name"]
            # Prefix match (station is prefix of census name, min 5 chars)
            for cname, info in census.items():
                for cand in candidates:
                    if len(cand) >= 5 and cname.startswith(cand):
                        return info["coords"], "census_prefix", cand, info["source_name"]
            # Fuzzy match (cutoff 0.8)
            for cand in candidates:
                if len(cand) >= 5:
                    hits = get_close_matches(cand, list(census.keys()), n=1,
                                             cutoff=0.8)
                    if hits:
                        hit = hits[0]
                        return census[hit]["coords"], "census_fuzzy", cand, census[hit]["source_name"]
            return None, None, None, None

        for station in shadow_stations:
            if station in station_coords:
                continue
            coords, method, cleaned_name, matched_name = _census_match(station)
            if coords:
                _set_match(
                    station,
                    coords,
                    method,
                    cleaned_name=cleaned_name,
                    matched_name=matched_name,
                )
    except Exception as e:
        print(f"  WARNING: Census geocoding failed: {e}")

    # --- Strategy 3: EIA generator exact match ---
    try:
        eia_lookup = _load_eia_generators()
        for station in shadow_stations:
            if station in station_coords:
                continue
            station_upper = re.sub(r"[^A-Z]", "", str(station).upper())
            if station_upper in eia_lookup:
                _set_match(
                    station,
                    eia_lookup[station_upper]["coords"],
                    "eia_exact",
                    cleaned_name=station_upper,
                    matched_name=eia_lookup[station_upper]["source_name"],
                )
    except FileNotFoundError:
        print("  WARNING: texas_generators.csv not found, skipping EIA matching")

    # --- Strategy 4: Legacy node prefix match ---
    try:
        nodes = _load_node_coordinates()
        prefix_coords = (
            nodes.groupby("prefix")
            .agg(lat=("lat", "mean"), lon=("lon", "mean"))
            .reset_index()
        )
        prefix_lookup = {
            row["prefix"].upper(): (row["lat"], row["lon"])
            for _, row in prefix_coords.iterrows()
        }
        for station in shadow_stations:
            if station in station_coords:
                continue
            key = str(station).upper()
            if key in prefix_lookup:
                _set_match(
                    station,
                    prefix_lookup[key],
                    "node_prefix_legacy",
                    cleaned_name=key,
                    matched_name=key,
                )
    except FileNotFoundError:
        pass

    # --- Strategy 5: Bus_Output.shp fallback ---
    try:
        dirs = setup_directories()
        bus_shp = Path(dirs["root"]) / "Texas_GIS_Data" / "Bus" / "Bus_Output.shp"
        if not bus_shp.exists():
            bus_shp = ROOT / "data" / "Bus_Output.shp"
        if bus_shp.exists():
            bus_gdf = gpd.read_file(bus_shp).to_crs(epsg=4326)
            bus_gdf["centroid"] = bus_gdf.geometry.centroid
            bus_gdf["lat"] = bus_gdf["centroid"].y
            bus_gdf["lon"] = bus_gdf["centroid"].x
            name_col = next(
                (c for c in ["Bus_Name", "NAME", "Name", "BUS_NAME"]
                 if c in bus_gdf.columns), None
            )
            if name_col:
                bus_gdf["bus_prefix"] = (
                    bus_gdf[name_col].str.upper().str.split().str[0]
                )
                bus_lookup = {
                    row["bus_prefix"]: (row["lat"], row["lon"])
                    for _, row in bus_gdf.iterrows()
                }
                for station in shadow_stations:
                    if station in station_coords:
                        continue
                    key = str(station).upper()
                    if key in bus_lookup:
                        _set_match(
                            station,
                            bus_lookup[key],
                            "bus_shp",
                            cleaned_name=key,
                            matched_name=key,
                        )
    except Exception as e:
        print(f"  WARNING: Bus shapefile matching failed: {e}")

    if return_details:
        return station_coords, match_methods, match_details
    return station_coords, match_methods


def geolocate_constraints(shadow_df):
    """Map constraint fromStation/toStation to lat/lon using multiple sources.

    Uses node_coordinates.csv (prefix match), EIA generators (name match),
    and Bus_Output.shp (fallback) to geolocate transmission constraint
    endpoints. For each constraint, assigns the midpoint of (fromStation,
    toStation) as the constraint's geographic location, then bins into ERA5
    0.1° grid cells.

    Args:
        shadow_df: Raw shadow price DataFrame with fromStation/toStation.

    Returns:
        DataFrame with constraintName, pixel_id, latitude, longitude.
    """
    # Get unique station names from shadow data
    from_stations = shadow_df["fromStation"].dropna().unique()
    to_stations = shadow_df["toStation"].dropna().unique()
    all_stations = set(from_stations) | set(to_stations)

    # Build station → (lat, lon) mapping from multiple sources
    station_coords, match_methods = _build_station_coords(all_stations)

    # Report matching results by method
    method_counts = {}
    for method in match_methods.values():
        method_counts[method] = method_counts.get(method, 0) + 1
    matched = len(station_coords)
    total = len(all_stations)
    print(f"  Geolocated {matched}/{total} stations ({matched/total*100:.0f}%)")
    for method, count in sorted(method_counts.items()):
        print(f"    {method}: {count}")

    # For each unique constraint, get midpoint of from/to stations
    constraints = (
        shadow_df[["constraintName", "fromStation", "toStation"]]
        .drop_duplicates(subset="constraintName")
    )

    rows = []
    for _, row in constraints.iterrows():
        from_coord = station_coords.get(row["fromStation"])
        to_coord = station_coords.get(row["toStation"])
        if from_coord and to_coord:
            lat = (from_coord[0] + to_coord[0]) / 2
            lon = (from_coord[1] + to_coord[1]) / 2
        elif from_coord:
            lat, lon = from_coord
        elif to_coord:
            lat, lon = to_coord
        else:
            continue

        pixel_id = f"{lat:.1f}_{lon:.1f}"
        rows.append({
            "constraintName": row["constraintName"],
            "latitude": lat,
            "longitude": lon,
            "pixel_id": pixel_id,
        })

    result = pd.DataFrame(rows)
    print(f"  Geolocated {len(result)}/{len(constraints)} constraints")
    return result


def build_shadow_station_match_tables(year, month):
    """Build matched/unmatched substation tables for shadow station geolocation.

    Args:
        year: Integer year.
        month: Integer month.

    Returns:
        (matched_df, unmatched_df)
        matched_df columns:
            station_name, cleaned_name, matched_source_name, source, match_method,
            latitude, longitude
        unmatched_df columns:
            station_name, cleaned_name
    """
    raw = _load_shadow_month(year, month)
    from_stations = raw["fromStation"].dropna().unique()
    to_stations = raw["toStation"].dropna().unique()
    all_stations = sorted(set(from_stations) | set(to_stations))

    station_coords, match_methods, match_details = _build_station_coords(
        all_stations, return_details=True
    )

    def _coarse_source(method):
        if method.startswith("census"):
            return "census"
        if method.startswith("eia"):
            return "eia"
        if method.startswith("np4160"):
            return "np4160"
        if method.startswith("node_prefix"):
            return "node_prefix_legacy"
        if method == "bus_shp":
            return "bus_shp"
        return method

    matched_rows = []
    for station in all_stations:
        if station not in station_coords:
            continue
        method = match_methods.get(station, "")
        details = match_details.get(station, {})
        lat, lon = station_coords[station]
        matched_rows.append(
            {
                "station_name": station,
                "cleaned_name": details.get(
                    "cleaned_name",
                    re.sub(r"[^A-Z]", "", str(station).upper()),
                ),
                "matched_source_name": details.get("matched_source_name", ""),
                "source": _coarse_source(method),
                "match_method": method,
                "latitude": lat,
                "longitude": lon,
            }
        )

    unmatched_rows = []
    for station in all_stations:
        if station in station_coords:
            continue
        unmatched_rows.append(
            {
                "station_name": station,
                "cleaned_name": re.sub(r"[^A-Z]", "", str(station).upper()),
            }
        )

    matched_df = pd.DataFrame(matched_rows)
    unmatched_df = pd.DataFrame(unmatched_rows)
    if not matched_df.empty:
        matched_df = matched_df.sort_values("station_name").reset_index(drop=True)
    if not unmatched_df.empty:
        unmatched_df = unmatched_df.sort_values("station_name").reset_index(drop=True)
    return matched_df, unmatched_df


def compute_constraint_hourly_by_pixel(year, month, force_rebuild=False):
    """Compute per-pixel hourly shadow cost from geolocated constraints.

    For each ERA5 pixel, sums the shadow costs of all constraints whose
    transmission elements are located at (or near) that pixel.

    Args:
        year: Integer year.
        month: Integer month.
        force_rebuild: If True, recompute even if cached.

    Returns:
        DataFrame with columns: valid_time, pixel_id, local_shadow_cost,
        local_n_binding, local_max_shadow.
    """
    dirs = setup_directories()
    cache_path = os.path.join(
        dirs["processed"], "congestion_metrics",
        f"constraint_by_pixel_{year}{month:02d}.csv",
    )

    if os.path.exists(cache_path) and not force_rebuild:
        print(f"  Loading cached pixel-level congestion: {cache_path}")
        df = pd.read_csv(cache_path, parse_dates=["valid_time"])
        return df

    # Load and parse raw shadow data
    raw = _load_shadow_month(year, month)
    raw = _parse_sced_timestamps(raw)
    raw["shadowPrice"] = pd.to_numeric(raw["shadowPrice"], errors="coerce")

    # Geolocate constraints
    constraint_locs = geolocate_constraints(raw)
    if constraint_locs.empty:
        print("  WARNING: No constraints could be geolocated")
        return pd.DataFrame(columns=["valid_time", "pixel_id",
                                     "local_shadow_cost", "local_n_binding",
                                     "local_max_shadow"])

    # Merge location onto raw shadow data
    raw_located = raw.merge(
        constraint_locs[["constraintName", "pixel_id"]],
        on="constraintName",
        how="inner",
    )

    # Aggregate to (pixel, hour)
    pixel_hourly = (
        raw_located.groupby(["valid_time", "pixel_id"])
        .agg(
            local_shadow_cost=("shadowPrice", "sum"),
            local_n_binding=("constraintName", "nunique"),
            local_max_shadow=("shadowPrice", "max"),
        )
        .reset_index()
    )

    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    pixel_hourly.to_csv(cache_path, index=False)
    print(f"  Saved pixel-level congestion: {cache_path}")
    print(f"    {len(pixel_hourly)} (pixel, hour) rows")

    return pixel_hourly


# ---------------------------------------------------------------------------
# Merge helpers (for combine_forecast_generation_node.py)
# ---------------------------------------------------------------------------

def merge_congestion_system(pixel_df, year, month, time_col="valid_time"):
    """Merge system-level congestion metrics into a pixel-hourly DataFrame.

    Adds: n_binding_constraints, total_shadow_cost, max_shadow_price,
          shadow_cost_weighted, n_violations, total_violated_mw.

    Args:
        pixel_df: DataFrame with a time column.
        year: Integer year.
        month: Integer month.
        time_col: Name of the time column for merging.

    Returns:
        DataFrame with congestion columns added (left join).
    """
    congestion = compute_hourly_congestion_metrics(year, month)
    merge_cols = [
        "valid_time", "n_binding_constraints", "total_shadow_cost",
        "max_shadow_price", "shadow_cost_weighted", "n_violations",
        "total_violated_mw", "mean_shadow_cost_per_interval",
        "first_interval_shadow_cost",
    ]
    congestion = congestion[[c for c in merge_cols if c in congestion.columns]]

    if time_col != "valid_time":
        congestion = congestion.rename(columns={"valid_time": time_col})

    return pixel_df.merge(congestion, on=time_col, how="left")


def merge_congestion_local(pixel_df, year, month, time_col="valid_time"):
    """Merge pixel-level (local) congestion metrics into a pixel-hourly DataFrame.

    Adds: local_shadow_cost, local_n_binding, local_max_shadow.

    Args:
        pixel_df: DataFrame with pixel_id and a time column.
        year: Integer year.
        month: Integer month.
        time_col: Name of the time column for merging.

    Returns:
        DataFrame with local congestion columns added (left join, NaN-filled).
    """
    local = compute_constraint_hourly_by_pixel(year, month)
    if local.empty:
        pixel_df["local_shadow_cost"] = np.nan
        pixel_df["local_n_binding"] = 0
        pixel_df["local_max_shadow"] = np.nan
        return pixel_df

    if time_col != "valid_time":
        local = local.rename(columns={"valid_time": time_col})

    pixel_df = pixel_df.merge(local, on=[time_col, "pixel_id"], how="left")
    pixel_df["local_shadow_cost"] = pixel_df["local_shadow_cost"].fillna(0)
    pixel_df["local_n_binding"] = pixel_df["local_n_binding"].fillna(0).astype(int)
    pixel_df["local_max_shadow"] = pixel_df["local_max_shadow"].fillna(0)
    return pixel_df


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Process SCED shadow prices into congestion metrics"
    )
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--month", type=int, default=7)
    parser.add_argument("--force-rebuild", action="store_true")
    args = parser.parse_args()

    print("=== Computing system-level congestion metrics ===")
    sys_metrics = compute_hourly_congestion_metrics(
        args.year, args.month, args.force_rebuild
    )
    print(f"\n{sys_metrics.describe()}\n")

    print("=== Computing pixel-level congestion metrics ===")
    pix_metrics = compute_constraint_hourly_by_pixel(
        args.year, args.month, args.force_rebuild
    )
    print(f"\n{pix_metrics.describe()}")
