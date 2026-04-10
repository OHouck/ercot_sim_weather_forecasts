"""Process 60-Day SCED Disclosure data into hourly curtailment metrics.

Reads nested ZIP archives of SCED disclosure CSVs (from ERCOT MIS portal),
extracts wind and solar unit data, and computes curtailment = HSL - output.

The SCED disclosure data is stored as nested ZIPs:
  {raw}/ercot/sced/{monthname}{year}/
    ├── cdr.np3-965-er.*.zip           (outer ZIP, contains inner ZIPs)
    │   ├── *.60_Day_SCED_Disclosure.zip  (inner ZIP per operating day)
    │   │   └── 60d_SCED_Gen_Resource_Data-{DD}-{MON}-{YY}.csv

Key columns from Gen Resource Data:
  - SCED Time Stamp: SCED interval timestamp (MM/DD/YYYY HH:MM:SS)
  - Resource Name: unit identifier (maps to settlement point)
  - Resource Type: WIND, PVGR (solar), PWRSTR (battery), etc.
  - HSL: High Sustained Limit [MW] (max available output)
  - Base Point: SCED dispatch instruction [MW]
  - Telemetered Net Output: actual measured output [MW]

Curtailment for wind/solar = max(0, HSL - Telemetered Net Output)

Usage:
    from process_data.process_curtailment import compute_hourly_curtailment
    curt = compute_hourly_curtailment(2025, 7)
"""

import calendar
import io
import os
import sys
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from helper_funcs import setup_directories

# Resource types that are renewable (eligible for curtailment calculation)
RENEWABLE_TYPES = {"WIND", "PVGR"}

# Columns to extract from the Gen Resource Data CSV (saves memory)
USECOLS = [
    "SCED Time Stamp",
    "Resource Name",
    "Resource Type",
    "HSL",
    "Base Point",
    "Telemetered Net Output ",  # Note trailing space in ERCOT's header
]

# Month name mapping for folder names
MONTH_FOLDERS = {
    1: "jan", 2: "feb", 3: "march", 4: "april",
    5: "may", 6: "june", 7: "july", 8: "august",
    9: "sep", 10: "oct", 11: "nov", 12: "dec",
}


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def _find_sced_folders(year, month):
    """Find SCED disclosure folder(s) for a given operating month.

    ERCOT releases SCED disclosure with a ~60-day lag, so folders are named
    by release month, not operating month. The primary mapping is:
        Operating month N → released in month N+2

    Because the lag is approximate (~58-62 days), boundary days may appear
    in the adjacent release folder (N+1). We return both if available.

    Returns list of existing folder paths (primary first, then secondary).
    """
    dirs = setup_directories()
    folders = []

    for offset in [2, 1]:  # Primary folder first, then fallback
        release_month = month + offset
        release_year = year
        if release_month > 12:
            release_month -= 12
            release_year += 1

        folder_name = f"{MONTH_FOLDERS[release_month]}{release_year}"
        sced_dir = os.path.join(dirs["raw"], "ercot", "sced", folder_name)

        if os.path.isdir(sced_dir):
            folders.append(sced_dir)

    if not folders:
        primary_name = f"{MONTH_FOLDERS[(month + 2 - 1) % 12 + 1]}{year + (month + 2 > 12)}"
        raise FileNotFoundError(
            f"SCED disclosure folder not found for operating {year}-{month:02d}\n"
            f"Expected release folder: '{primary_name}'\n"
            f"Download from ERCOT MIS portal (NP3-966-ER)"
        )
    return folders


def _load_sced_disclosure_month(year, month):
    """Load all SCED Gen Resource Data CSVs for one operating month.

    Navigates the nested ZIP structure:
      outer.zip → inner.zip → 60d_SCED_Gen_Resource_Data-{DD}-{MON}-{YY}.csv

    Only loads renewable resource types (WIND, PVGR) to save memory.

    Returns:
        DataFrame with columns: sced_time, resource_name, resource_type,
        hsl, base_point, telemetered_output
    """
    sced_dirs = _find_sced_folders(year, month)

    frames = []
    n_days = 0

    for sced_dir in sced_dirs:
      for outer_name in sorted(os.listdir(sced_dir)):
        if not outer_name.endswith(".zip"):
            continue

        outer_path = os.path.join(sced_dir, outer_name)
        outer_zip = zipfile.ZipFile(outer_path)

        for inner_name in sorted(outer_zip.namelist()):
            if not inner_name.endswith(".zip"):
                continue

            inner_data = outer_zip.read(inner_name)
            inner_zip = zipfile.ZipFile(io.BytesIO(inner_data))

            # Find the Gen Resource Data CSV
            gen_csvs = [
                n for n in inner_zip.namelist()
                if "Gen_Resource" in n and n.endswith(".csv")
            ]
            if not gen_csvs:
                continue

            csv_name = gen_csvs[0]
            csv_bytes = inner_zip.read(csv_name)

            try:
                df = pd.read_csv(
                    io.BytesIO(csv_bytes),
                    usecols=USECOLS,
                    dtype={"Resource Type": str},
                )
            except (ValueError, KeyError):
                # Try without trailing space in column name
                try:
                    alt_cols = [c.rstrip() if c.endswith(" ") else c for c in USECOLS]
                    df = pd.read_csv(
                        io.BytesIO(csv_bytes),
                        usecols=alt_cols,
                        dtype={"Resource Type": str},
                    )
                except Exception as e:
                    print(f"    WARNING: Could not read {csv_name}: {e}")
                    continue

            # Normalize column names (strip whitespace)
            df.columns = df.columns.str.strip()

            # Filter to renewable types only
            df = df[df["Resource Type"].isin(RENEWABLE_TYPES)].copy()

            if len(df) > 0:
                frames.append(df)
                n_days += 1

    if not frames:
        raise FileNotFoundError(
            f"No SCED Gen Resource Data found for {year}-{month:02d} "
            f"in {sced_dir}"
        )

    combined = pd.concat(frames, ignore_index=True)

    # Standardize column names
    combined = combined.rename(columns={
        "SCED Time Stamp": "sced_time_raw",
        "Resource Name": "resource_name",
        "Resource Type": "resource_type",
        "HSL": "hsl",
        "Base Point": "base_point",
        "Telemetered Net Output": "telemetered_output",
    })

    # Parse timestamps and convert to numeric
    combined["sced_time"] = pd.to_datetime(
        combined["sced_time_raw"], format="mixed", dayfirst=False
    )
    combined["hsl"] = pd.to_numeric(combined["hsl"], errors="coerce")
    combined["base_point"] = pd.to_numeric(combined["base_point"], errors="coerce")
    combined["telemetered_output"] = pd.to_numeric(
        combined["telemetered_output"], errors="coerce"
    )

    # Floor to hour for merging with other hourly datasets
    combined["valid_time"] = combined["sced_time"].dt.floor("h")

    # Filter to the target operating month (folders may span adjacent months)
    n_before = len(combined)
    combined = combined[
        (combined["sced_time"].dt.year == year)
        & (combined["sced_time"].dt.month == month)
    ].copy()

    print(f"  Loaded {len(combined):,} renewable SCED records for {year}-{month:02d} "
          f"({n_days} operating days, {n_before - len(combined)} rows from adjacent months dropped)")
    print(f"    WIND: {(combined.resource_type == 'WIND').sum():,}, "
          f"PVGR (solar): {(combined.resource_type == 'PVGR').sum():,}")

    return combined.drop(columns=["sced_time_raw"])


# ---------------------------------------------------------------------------
# Curtailment computation
# ---------------------------------------------------------------------------

def compute_hourly_curtailment(year, month, force_rebuild=False):
    """Compute hourly system-level wind and solar curtailment from SCED disclosure.

    For each SCED interval, curtailment = max(0, HSL - Telemetered Net Output)
    for wind and solar units. Then aggregated to hourly.

    Args:
        year: Integer year.
        month: Integer month.
        force_rebuild: If True, recompute even if cached.

    Returns:
        DataFrame with one row per hour (valid_time column) and columns:
        - wind_curtailment_mw: total wind curtailment [MW]
        - solar_curtailment_mw: total solar curtailment [MW]
        - total_curtailment_mw: wind + solar [MW]
        - wind_hsl_mw: total wind HSL (available capacity) [MW]
        - solar_hsl_mw: total solar HSL [MW]
        - wind_curtailment_pct: curtailment / HSL for wind [%]
        - solar_curtailment_pct: curtailment / HSL for solar [%]
        - n_curtailed_units: number of units curtailed (> 5 MW)
        - n_wind_units: total wind units reporting
        - n_solar_units: total solar units reporting
    """
    dirs = setup_directories()
    cache_dir = os.path.join(dirs["processed"], "curtailment_metrics")
    cache_path = os.path.join(cache_dir, f"curtailment_hourly_{year}{month:02d}.csv")

    if os.path.exists(cache_path) and not force_rebuild:
        print(f"  Loading cached curtailment metrics: {cache_path}")
        df = pd.read_csv(cache_path, parse_dates=["valid_time"])
        return df

    # Load raw SCED data
    raw = _load_sced_disclosure_month(year, month)

    # Compute per-unit curtailment
    raw["curtailment_mw"] = (raw["hsl"] - raw["telemetered_output"]).clip(lower=0)
    raw["is_curtailed"] = raw["curtailment_mw"] > 5.0  # threshold for meaningful curtailment

    # Separate wind and solar
    wind = raw[raw["resource_type"] == "WIND"]
    solar = raw[raw["resource_type"] == "PVGR"]

    # Aggregate wind by hour
    wind_hourly = (
        wind.groupby("valid_time")
        .agg(
            wind_curtailment_mw=("curtailment_mw", "sum"),
            wind_hsl_mw=("hsl", "sum"),
            wind_output_mw=("telemetered_output", "sum"),
            n_wind_curtailed=("is_curtailed", "sum"),
            n_wind_units=("resource_name", "nunique"),
        )
        .reset_index()
    )

    # Aggregate solar by hour
    solar_hourly = (
        solar.groupby("valid_time")
        .agg(
            solar_curtailment_mw=("curtailment_mw", "sum"),
            solar_hsl_mw=("hsl", "sum"),
            solar_output_mw=("telemetered_output", "sum"),
            n_solar_curtailed=("is_curtailed", "sum"),
            n_solar_units=("resource_name", "nunique"),
        )
        .reset_index()
    )

    # Combine
    hourly = wind_hourly.merge(solar_hourly, on="valid_time", how="outer")
    hourly = hourly.fillna(0)

    # Total curtailment
    hourly["total_curtailment_mw"] = (
        hourly["wind_curtailment_mw"] + hourly["solar_curtailment_mw"]
    )
    hourly["n_curtailed_units"] = (
        hourly["n_wind_curtailed"] + hourly["n_solar_curtailed"]
    ).astype(int)

    # Curtailment as % of available capacity
    hourly["wind_curtailment_pct"] = np.where(
        hourly["wind_hsl_mw"] > 0,
        hourly["wind_curtailment_mw"] / hourly["wind_hsl_mw"] * 100,
        0,
    )
    hourly["solar_curtailment_pct"] = np.where(
        hourly["solar_hsl_mw"] > 0,
        hourly["solar_curtailment_mw"] / hourly["solar_hsl_mw"] * 100,
        0,
    )

    # Average across SCED intervals within each hour
    # The raw data has ~12 SCED intervals per hour; we want hourly averages
    n_intervals = raw.groupby("valid_time")["sced_time"].nunique().reset_index()
    n_intervals.columns = ["valid_time", "n_sced_intervals"]
    hourly = hourly.merge(n_intervals, on="valid_time", how="left")

    # Divide sums by n_intervals to get per-interval average (representative hourly value)
    for col in ["wind_curtailment_mw", "solar_curtailment_mw", "total_curtailment_mw",
                "wind_hsl_mw", "solar_hsl_mw", "wind_output_mw", "solar_output_mw"]:
        hourly[col] = hourly[col] / hourly["n_sced_intervals"]

    # Recalculate percentages after averaging
    hourly["wind_curtailment_pct"] = np.where(
        hourly["wind_hsl_mw"] > 0,
        hourly["wind_curtailment_mw"] / hourly["wind_hsl_mw"] * 100,
        0,
    )
    hourly["solar_curtailment_pct"] = np.where(
        hourly["solar_hsl_mw"] > 0,
        hourly["solar_curtailment_mw"] / hourly["solar_hsl_mw"] * 100,
        0,
    )

    # Select output columns
    output_cols = [
        "valid_time",
        "wind_curtailment_mw", "solar_curtailment_mw", "total_curtailment_mw",
        "wind_hsl_mw", "solar_hsl_mw",
        "wind_output_mw", "solar_output_mw",
        "wind_curtailment_pct", "solar_curtailment_pct",
        "n_curtailed_units", "n_wind_units", "n_solar_units",
    ]
    hourly = hourly[[c for c in output_cols if c in hourly.columns]]

    # Cache
    os.makedirs(cache_dir, exist_ok=True)
    hourly.to_csv(cache_path, index=False)
    print(f"  Saved curtailment metrics: {cache_path}")
    print(f"    {len(hourly)} hours")
    print(f"    Mean wind curtailment: {hourly['wind_curtailment_mw'].mean():.1f} MW")
    print(f"    Mean solar curtailment: {hourly['solar_curtailment_mw'].mean():.1f} MW")
    print(f"    Max total curtailment: {hourly['total_curtailment_mw'].max():.1f} MW")

    return hourly


# ---------------------------------------------------------------------------
# Geolocation of curtailed resources
# ---------------------------------------------------------------------------

def _sced_name_to_prefix(name):
    """Extract plant prefix from SCED resource name.

    Examples:
        AJAXWIND_UNIT1 -> AJAXWIND
        ANCHOR_WIND3   -> ANCHOR
        CBY_WT_GEN_1   -> CBY
        ANACACHO_ANA   -> ANACACHO
        7RNCHSLR_UNIT2 -> 7RNCHSLR
    """
    import re
    name = str(name).upper()
    name = re.sub(r"_WT_GEN_\d+$", "", name)
    name = re.sub(r"_(UNIT|GEN|BES|ESS|BESS)\d*$", "", name)
    name = re.sub(r"_WIND\d+$", "", name)
    name = re.sub(r"_SOLAR\d+$", "", name)
    name = re.sub(r"_ANA$", "", name)
    return name


def geolocate_curtailment_resources(resource_names):
    """Map SCED resource names to lat/lon using node_coordinates and EIA data.

    Matching strategies (priority order):
    1. Node coordinates: extract prefix from settlement point names, match
       to SCED resource name prefix
    2. EIA Form 860: match resource name prefix to plant names or LMP node
       designations

    Args:
        resource_names: iterable of SCED resource name strings.

    Returns:
        DataFrame with columns: resource_name, lat, lon, match_method, pixel_id
    """
    import re
    dirs = setup_directories()

    unique_resources = set(resource_names)
    resource_coords = {}
    match_methods = {}

    # --- Build node_coordinates lookup ---
    node_lookup = {}
    node_path = os.path.join(dirs["processed"], "node_coordinates.csv")
    if os.path.exists(node_path):
        nodes = pd.read_csv(node_path)
        for _, row in nodes.iterrows():
            sp = row["settlement_point"].upper()
            # Multiple prefix strategies for settlement points:
            # AJAXWIND_RN -> AJAXWIND
            # TYLRWIND_RN -> TYLRWIND
            # CBY_CBY_G1  -> CBY
            prefix_full = re.sub(r"_(RN|ALL|UNIT\d*|G\d+|G\d+_\d+)$", "", sp)
            prefix_short = sp.split("_")[0]
            node_lookup[prefix_full] = (row["lat"], row["lon"])
            node_lookup[prefix_short] = (row["lat"], row["lon"])

    # --- Build EIA generator lookup ---
    eia_lookup = {}
    eia_path = os.path.join(dirs["raw"], "eia860", "texas_generators.csv")
    if os.path.exists(eia_path):
        eia = pd.read_csv(eia_path)
        for _, row in eia.dropna(subset=["lat", "lon"]).iterrows():
            pname = str(row["plant_name"]).upper().replace(" ", "").replace("-", "")
            eia_lookup[pname] = (row["lat"], row["lon"])
            pname2 = str(row["plant_name"]).upper().replace(" ", "_")
            eia_lookup[pname2] = (row["lat"], row["lon"])
            if pd.notna(row.get("lmp_node_designation")):
                lmp_prefix = re.sub(
                    r"_(RN|ALL|UNIT\d*|G\d+)$", "",
                    str(row["lmp_node_designation"]).upper(),
                )
                eia_lookup[lmp_prefix] = (row["lat"], row["lon"])

    # --- Match each resource ---
    for resource in unique_resources:
        prefix = _sced_name_to_prefix(resource)

        # Strategy 1: node coordinates exact prefix match
        if prefix in node_lookup:
            resource_coords[resource] = node_lookup[prefix]
            match_methods[resource] = "node_prefix"
            continue

        # Strategy 1b: node coordinates contains prefix
        found = False
        for node_prefix, coords in node_lookup.items():
            if len(prefix) >= 4 and (
                prefix in node_prefix or node_prefix.startswith(prefix)
            ):
                resource_coords[resource] = coords
                match_methods[resource] = "node_substring"
                found = True
                break
        if found:
            continue

        # Strategy 2: EIA exact
        if prefix in eia_lookup:
            resource_coords[resource] = eia_lookup[prefix]
            match_methods[resource] = "eia_exact"
            continue

        # Strategy 2b: EIA substring
        for eia_name, coords in eia_lookup.items():
            if len(prefix) >= 4 and (
                prefix in eia_name or eia_name.startswith(prefix)
            ):
                resource_coords[resource] = coords
                match_methods[resource] = "eia_substring"
                found = True
                break

    # Report
    method_counts = {}
    for m in match_methods.values():
        method_counts[m] = method_counts.get(m, 0) + 1
    matched = len(resource_coords)
    total = len(unique_resources)
    print(f"  Geolocated {matched}/{total} renewable resources ({matched/total*100:.0f}%)")
    for method, count in sorted(method_counts.items()):
        print(f"    {method}: {count}")

    # Build output DataFrame
    rows = []
    for resource, (lat, lon) in resource_coords.items():
        pixel_id = f"{lat:.1f}_{lon:.1f}"
        rows.append({
            "resource_name": resource,
            "lat": lat,
            "lon": lon,
            "match_method": match_methods[resource],
            "pixel_id": pixel_id,
        })

    return pd.DataFrame(rows)


def compute_hourly_curtailment_by_pixel(year, month, force_rebuild=False):
    """Compute per-pixel hourly curtailment from geolocated renewable resources.

    For each ERA5 pixel, sums wind and solar curtailment of all resources
    located at that pixel.

    Args:
        year: Integer year.
        month: Integer month.
        force_rebuild: If True, recompute even if cached.

    Returns:
        DataFrame with columns: valid_time, pixel_id, local_wind_curtailment_mw,
        local_solar_curtailment_mw, local_total_curtailment_mw.
    """
    dirs = setup_directories()
    cache_dir = os.path.join(dirs["processed"], "curtailment_metrics")
    cache_path = os.path.join(cache_dir, f"curtailment_by_pixel_{year}{month:02d}.csv")

    if os.path.exists(cache_path) and not force_rebuild:
        print(f"  Loading cached pixel-level curtailment: {cache_path}")
        return pd.read_csv(cache_path, parse_dates=["valid_time"])

    # Load raw SCED data (renewable only)
    raw = _load_sced_disclosure_month(year, month)

    # Compute per-unit curtailment
    raw["curtailment_mw"] = (raw["hsl"] - raw["telemetered_output"]).clip(lower=0)

    # Geolocate resources
    resource_locs = geolocate_curtailment_resources(raw["resource_name"].unique())
    if resource_locs.empty:
        print("  WARNING: No resources could be geolocated")
        return pd.DataFrame(columns=[
            "valid_time", "pixel_id", "local_wind_curtailment_mw",
            "local_solar_curtailment_mw", "local_total_curtailment_mw",
        ])

    # Merge location onto raw data
    raw_located = raw.merge(
        resource_locs[["resource_name", "pixel_id"]],
        on="resource_name",
        how="inner",
    )

    # Separate wind and solar
    wind_located = raw_located[raw_located["resource_type"] == "WIND"]
    solar_located = raw_located[raw_located["resource_type"] == "PVGR"]

    # Aggregate wind by (pixel, hour)
    wind_pix = (
        wind_located.groupby(["valid_time", "pixel_id"])
        .agg(local_wind_curtailment_mw=("curtailment_mw", "sum"),
             n_sced_intervals=("sced_time", "nunique"))
        .reset_index()
    )
    wind_pix["local_wind_curtailment_mw"] /= wind_pix["n_sced_intervals"]

    # Aggregate solar by (pixel, hour)
    solar_pix = (
        solar_located.groupby(["valid_time", "pixel_id"])
        .agg(local_solar_curtailment_mw=("curtailment_mw", "sum"),
             n_sced_intervals=("sced_time", "nunique"))
        .reset_index()
    )
    solar_pix["local_solar_curtailment_mw"] /= solar_pix["n_sced_intervals"]

    # Combine
    pixel_hourly = wind_pix[["valid_time", "pixel_id", "local_wind_curtailment_mw"]].merge(
        solar_pix[["valid_time", "pixel_id", "local_solar_curtailment_mw"]],
        on=["valid_time", "pixel_id"],
        how="outer",
    ).fillna(0)

    pixel_hourly["local_total_curtailment_mw"] = (
        pixel_hourly["local_wind_curtailment_mw"]
        + pixel_hourly["local_solar_curtailment_mw"]
    )

    # Cache
    os.makedirs(cache_dir, exist_ok=True)
    pixel_hourly.to_csv(cache_path, index=False)
    print(f"  Saved pixel-level curtailment: {cache_path}")
    print(f"    {len(pixel_hourly)} (pixel, hour) rows")

    return pixel_hourly


# ---------------------------------------------------------------------------
# Merge helper (for create_pixel_level_data.py)
# ---------------------------------------------------------------------------

def merge_curtailment_system(pixel_df, year, month, time_col="valid_time"):
    """Merge system-level curtailment metrics into a pixel-hourly DataFrame.

    Adds: wind_curtailment_mw, solar_curtailment_mw, total_curtailment_mw,
          wind_curtailment_pct, solar_curtailment_pct, n_curtailed_units.

    Args:
        pixel_df: DataFrame with a time column.
        year: Integer year.
        month: Integer month.
        time_col: Name of the time column for merging.

    Returns:
        DataFrame with curtailment columns added (left join).
    """
    curtailment = compute_hourly_curtailment(year, month)
    merge_cols = [
        "valid_time",
        "wind_curtailment_mw", "solar_curtailment_mw", "total_curtailment_mw",
        "wind_curtailment_pct", "solar_curtailment_pct",
        "n_curtailed_units",
    ]
    curtailment = curtailment[[c for c in merge_cols if c in curtailment.columns]]

    if time_col != "valid_time":
        curtailment = curtailment.rename(columns={"valid_time": time_col})

    return pixel_df.merge(curtailment, on=time_col, how="left")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Process SCED disclosure data into curtailment metrics"
    )
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--month", type=int, default=None,
                        help="Single month to process (default: all 12)")
    parser.add_argument("--force-rebuild", action="store_true")
    args = parser.parse_args()

    months = [args.month] if args.month else range(1, 13)

    for month in months:
        print(f"\n{'='*60}")
        print(f"Processing curtailment for {args.year}-{month:02d}")
        print(f"{'='*60}")
        try:
            metrics = compute_hourly_curtailment(args.year, month, args.force_rebuild)
            print(f"\n{metrics.describe()}\n")
        except FileNotFoundError as e:
            print(f"  SKIPPED: {e}")
