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
import sys
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
    # violatedMW can be negative (constraint relieved); we use abs for cost
    raw["abs_shadow"] = raw["shadowPrice"].abs()
    raw["violated_pos"] = raw["violatedMW"].clip(lower=0)
    raw["shadow_x_violated"] = raw["abs_shadow"] * raw["violated_pos"]

    # Aggregate to hourly
    hourly = (
        raw.groupby("valid_time")
        .agg(
            n_binding_constraints=("constraintName", "nunique"),
            n_sced_intervals=("sced_time", "nunique"),
            total_shadow_cost=("abs_shadow", "sum"),
            max_shadow_price=("abs_shadow", "max"),
            mean_shadow_price=("abs_shadow", "mean"),
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

def _load_bus_coordinates():
    """Load ERCOT bus coordinates from Bus_Output.shp.

    Returns:
        GeoDataFrame with bus names and geometries.
    """
    dirs = setup_directories()
    # Bus shapefile lives in the OneDrive data area
    bus_shp = Path(dirs["root"]) / "Texas_GIS_Data" / "Bus" / "Bus_Output.shp"
    if not bus_shp.exists():
        # Fallback to repo data/ directory
        bus_shp = ROOT / "data" / "Bus_Output.shp"
    if not bus_shp.exists():
        raise FileNotFoundError(f"Bus shapefile not found: {bus_shp}")
    buses = gpd.read_file(bus_shp)
    return buses


def geolocate_constraints(shadow_df, bus_gdf=None):
    """Map constraint fromStation/toStation to lat/lon via Bus_Output.shp.

    For each constraint, assigns the midpoint of (fromStation, toStation) as
    the constraint's geographic location, then bins into ERA5 0.1° grid cells.

    Args:
        shadow_df: Raw shadow price DataFrame with fromStation/toStation.
        bus_gdf: GeoDataFrame of bus locations. Loaded from shapefile if None.

    Returns:
        DataFrame with constraintName, pixel_id, latitude, longitude.
    """
    if bus_gdf is None:
        bus_gdf = _load_bus_coordinates()

    # Get unique station names from shadow data
    from_stations = shadow_df["fromStation"].dropna().unique()
    to_stations = shadow_df["toStation"].dropna().unique()
    all_stations = set(from_stations) | set(to_stations)

    # Build station → (lat, lon) mapping from bus shapefile
    # Bus names in shapefile may differ from SCED station names;
    # try exact match first, then fuzzy substring match
    bus_gdf = bus_gdf.to_crs(epsg=4326)  # ensure WGS84
    bus_gdf["centroid"] = bus_gdf.geometry.centroid
    bus_gdf["lat"] = bus_gdf["centroid"].y
    bus_gdf["lon"] = bus_gdf["centroid"].x

    # Try to identify name column in bus shapefile
    name_col = None
    for candidate in ["Bus_Name", "NAME", "Name", "name", "STATION", "Station",
                       "BUS_NAME"]:
        if candidate in bus_gdf.columns:
            name_col = candidate
            break

    if name_col is None:
        print(f"  WARNING: Could not identify name column in Bus_Output.shp")
        print(f"  Available columns: {bus_gdf.columns.tolist()}")
        return pd.DataFrame(columns=["constraintName", "pixel_id", "latitude",
                                     "longitude"])

    # Build name → coords mapping
    station_coords = {}
    bus_names = bus_gdf[name_col].str.upper().values
    bus_lats = bus_gdf["lat"].values
    bus_lons = bus_gdf["lon"].values

    for station in all_stations:
        station_upper = str(station).upper()
        # Exact match
        idx = np.where(bus_names == station_upper)[0]
        if len(idx) > 0:
            station_coords[station] = (bus_lats[idx[0]], bus_lons[idx[0]])
            continue
        # Substring match: bus name contains station name or vice versa
        for i, bn in enumerate(bus_names):
            if station_upper in str(bn) or str(bn) in station_upper:
                station_coords[station] = (bus_lats[i], bus_lons[i])
                break

    matched = len(station_coords)
    total = len(all_stations)
    print(f"  Geolocated {matched}/{total} stations "
          f"({matched/total*100:.0f}%) via Bus_Output.shp")

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
    raw["abs_shadow"] = raw["shadowPrice"].abs()

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
            local_shadow_cost=("abs_shadow", "sum"),
            local_n_binding=("constraintName", "nunique"),
            local_max_shadow=("abs_shadow", "max"),
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
