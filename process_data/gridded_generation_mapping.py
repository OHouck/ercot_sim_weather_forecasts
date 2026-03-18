"""Map generation capacity, transmission lines, and load centers to ERA5-Land grid.

Builds a static NetCDF with dimensions (latitude, longitude) matching the ERA5-Land
grid that contains variables for: generation capacity by fuel type, transmission line
presence, and load center presence.

Output: {processed}/gridded_generation_map.nc
"""

import os
import sys
import glob
from pathlib import Path

import numpy as np
import pandas as pd
import geopandas as gpd
import xarray as xr
from shapely.geometry import box

sys.path.insert(0, str(Path(__file__).parent.parent))
from helper_funcs import setup_directories
from process_data.prepare_cluster_level_data import (
    _tech_slug,
)

# ERA5-Land grid parameters (fallback if no ERA5 file exists)
ERA5_LAT_MIN, ERA5_LAT_MAX = 25.8, 36.5
ERA5_LON_MIN, ERA5_LON_MAX = -106.6, -93.5
ERA5_RESOLUTION = 0.1

# Default shapefile paths
_GIS_ROOT = (
    '/Users/ohouck/Library/CloudStorage/OneDrive-TheUniversityofChicago/'
    'ercot_sim_weather_forecasts/Texas_GIS_Data'
)
DEFAULT_LINE_SHP = os.path.join(_GIS_ROOT, 'Line', 'Line_Output.shp')
DEFAULT_BUS_SHP = os.path.join(_GIS_ROOT, 'Bus', 'Bus_Output.shp')


def build_era5_template_dataset():
    """Load an ERA5 NetCDF and return a template Dataset with only lat/lon coords.

    Drops all data variables and the time dimension, leaving a Dataset
    with dims (latitude, longitude) that exactly matches the ERA5-Land grid.

    Returns:
        xr.Dataset with no data variables and dims (latitude, longitude),
        sorted in ascending order.

    Raises:
        FileNotFoundError: If no ERA5 NetCDF files are found.
    """
    dirs = setup_directories()
    pattern = os.path.join(dirs['raw'], 'era5_land', '*', '*', 'era5_land_*.nc')
    nc_files = sorted(glob.glob(pattern))

    if not nc_files:
        raise FileNotFoundError(
            "No ERA5 NetCDF files found. Run Step 1c (pull_era5.py) first."
        )

    ds = xr.open_dataset(nc_files[0])

    # Drop all data variables, keeping only coordinates
    ds = ds.drop_vars(list(ds.data_vars))

    # Drop time dimension — take first slice to collapse it
    if 'time' in ds.dims:
        ds = ds.isel(time=0).drop_vars('time', errors='ignore')

    # Ensure ascending lat/lon order
    ds = ds.sortby('latitude').sortby('longitude')

    print(
        f"ERA5 grid loaded from {nc_files[0]}: "
        f"{len(ds.latitude)} lats × {len(ds.longitude)} lons"
    )
    return ds


def _digitize_points(point_lats, point_lons, grid_lats, grid_lons):
    """Bin lat/lon points into ERA5 grid cells using np.digitize.

    Args:
        point_lats, point_lons: 1D arrays of point coordinates.
        grid_lats, grid_lons: 1D arrays of grid cell centers (ascending).

    Returns:
        Tuple (lat_idx, lon_idx): integer arrays of grid cell indices.
        Points outside the grid get index -1 or len(grid).
    """
    half = ERA5_RESOLUTION / 2.0
    lat_edges = np.concatenate([
        [grid_lats[0] - half],
        (grid_lats[:-1] + grid_lats[1:]) / 2,
        [grid_lats[-1] + half],
    ])
    lon_edges = np.concatenate([
        [grid_lons[0] - half],
        (grid_lons[:-1] + grid_lons[1:]) / 2,
        [grid_lons[-1] + half],
    ])
    lat_idx = np.digitize(point_lats, lat_edges) - 1
    lon_idx = np.digitize(point_lons, lon_edges) - 1
    return lat_idx, lon_idx


def _build_grid_polygons(lats, lons):
    """Build a GeoDataFrame of 0.1-degree grid cell polygons.

    Args:
        lats: 1D array of latitude centers (ascending).
        lons: 1D array of longitude centers (ascending).

    Returns:
        GeoDataFrame with columns: lat_idx, lon_idx, geometry. CRS: EPSG:4326.
    """
    half = ERA5_RESOLUTION / 2.0
    records = []
    for i, lat in enumerate(lats):
        for j, lon in enumerate(lons):
            records.append({
                'lat_idx': i,
                'lon_idx': j,
                'geometry': box(lon - half, lat - half, lon + half, lat + half),
            })
    return gpd.GeoDataFrame(records, crs='EPSG:4326')


def _bin_generators(generators_path, lats, lons):
    """Assign EIA 860 generators to ERA5 grid cells, aggregate by fuel type.

    Args:
        generators_path: Path to texas_generators.csv.
        lats, lons: 1D arrays of ERA5 grid cell centers.

    Returns:
        Dict mapping variable name → 2D numpy array of shape (n_lats, n_lons).
        Includes total_capacity_mw, n_generators, nameplate_mw_{broad_cat},
        and nameplate_mw_tech_{slug} for each fine-grained technology.
    """
    gen = pd.read_csv(generators_path)
    gen['nameplate_capacity_mw'] = pd.to_numeric(
        gen['nameplate_capacity_mw'], errors='coerce'
    )
    gen = gen.dropna(subset=['lat', 'lon', 'nameplate_capacity_mw', 'technology'])

    lat_idx, lon_idx = _digitize_points(
        gen['lat'].values, gen['lon'].values, lats, lons
    )
    gen['lat_idx'] = lat_idx
    gen['lon_idx'] = lon_idx

    valid = (
        (gen['lat_idx'] >= 0) & (gen['lat_idx'] < len(lats)) &
        (gen['lon_idx'] >= 0) & (gen['lon_idx'] < len(lons))
    )
    n_dropped = (~valid).sum()
    if n_dropped > 0:
        print(f"  Warning: {n_dropped} generators outside ERA5 domain, skipped")
    gen = gen[valid].copy()

    n_lats, n_lons = len(lats), len(lons)
    arrays = {}

    # Total capacity and generator count
    totals = gen.groupby(['lat_idx', 'lon_idx']).agg(
        total_capacity_mw=('nameplate_capacity_mw', 'sum'),
        n_generators=('nameplate_capacity_mw', 'count'),
    ).reset_index()

    total_cap = np.zeros((n_lats, n_lons), dtype=np.float32)
    n_gen = np.zeros((n_lats, n_lons), dtype=np.int32)
    total_cap[totals['lat_idx'].values, totals['lon_idx'].values] = (
        totals['total_capacity_mw'].values.astype(np.float32)
    )
    n_gen[totals['lat_idx'].values, totals['lon_idx'].values] = (
        totals['n_generators'].values
    )
    arrays['total_capacity_mw'] = total_cap
    arrays['n_generators'] = n_gen

    for tech in gen['technology'].unique():
        tech_gen = gen[gen['technology'] == tech]
        arr = np.zeros((n_lats, n_lons), dtype=np.float32)
        grp = (
            tech_gen.groupby(['lat_idx', 'lon_idx'])['nameplate_capacity_mw']
            .sum()
            .reset_index()
        )
        arr[grp['lat_idx'].values, grp['lon_idx'].values] = (
            grp['nameplate_capacity_mw'].values.astype(np.float32)
        )
        arrays[f'nameplate_mw_tech_{_tech_slug(tech)}'] = arr

    n_cells_with_gen = int((arrays['total_capacity_mw'] > 0).sum())
    print(f"  {len(gen)} generators binned into {n_cells_with_gen} grid cells")
    return arrays


def _mark_transmission(line_shp_path, lats, lons):
    """Mark grid cells that intersect transmission lines.

    Args:
        line_shp_path: Path to Line_Output.shp.
        lats, lons: 1D arrays of ERA5 grid cell centers.

    Returns:
        2D numpy int8 array of shape (n_lats, n_lons); 1 = has transmission line.
    """
    grid_polys = _build_grid_polygons(lats, lons)
    lines = gpd.read_file(line_shp_path)
    if lines.crs is not None and lines.crs != grid_polys.crs:
        lines = lines.to_crs(grid_polys.crs)

    joined = gpd.sjoin(
        grid_polys[['lat_idx', 'lon_idx', 'geometry']],
        lines[['geometry']],
        how='inner',
        predicate='intersects',
    )
    tx_cells = joined[['lat_idx', 'lon_idx']].drop_duplicates()

    arr = np.zeros((len(lats), len(lons)), dtype=np.int8)
    arr[tx_cells['lat_idx'].values, tx_cells['lon_idx'].values] = 1
    print(f"  {len(tx_cells)} grid cells with transmission lines")
    return arr


def _mark_load_centers(bus_shp_path, lats, lons):
    """Mark grid cells containing ERCOT load buses (Gen_bus__N == 0).

    Args:
        bus_shp_path: Path to Bus_Output.shp.
        lats, lons: 1D arrays of ERA5 grid cell centers.

    Returns:
        2D numpy int8 array of shape (n_lats, n_lons); 1 = has load center.
    """
    buses = gpd.read_file(bus_shp_path)

    gen_col = None
    for candidate in ['Gen_bus__N', 'Gen_bus_N', 'GEN_BUS__N']:
        if candidate in buses.columns:
            gen_col = candidate
            break
    if gen_col is None:
        raise ValueError(
            f"Could not find generation bus flag column in {bus_shp_path}. "
            f"Available columns: {list(buses.columns)}"
        )

    load_buses = buses[buses[gen_col] == 0].copy()
    if load_buses.empty:
        print("  Warning: no load buses found (Gen_bus__N == 0)")
        return np.zeros((len(lats), len(lons)), dtype=np.int8)

    lat_idx, lon_idx = _digitize_points(
        load_buses.geometry.y.values, load_buses.geometry.x.values, lats, lons
    )
    valid = (
        (lat_idx >= 0) & (lat_idx < len(lats)) &
        (lon_idx >= 0) & (lon_idx < len(lons))
    )
    arr = np.zeros((len(lats), len(lons)), dtype=np.int8)
    arr[lat_idx[valid], lon_idx[valid]] = 1
    print(f"  {arr.sum()} grid cells with load centers")
    return arr


def build_gridded_generation_map(
    generators_path=None,
    line_shp_path=DEFAULT_LINE_SHP,
    bus_shp_path=DEFAULT_BUS_SHP,
    force_rebuild=False,
):
    """Build the gridded generation/transmission/load map as a NetCDF Dataset.

    Reads the ERA5-Land grid as the spatial template, assigns generation
    capacity, transmission line presence, and load center presence as 2D
    variables with dims (latitude, longitude), and caches to NetCDF.

    Args:
        generators_path: Path to texas_generators.csv (auto-detected if None).
        line_shp_path: Path to Line_Output.shp.
        bus_shp_path: Path to Bus_Output.shp.
        force_rebuild: If True, rebuild even if cached file exists.

    Returns:
        xr.Dataset with dims (latitude, longitude) and variables:
            total_capacity_mw, n_generators,
            nameplate_mw_{broad_cat} for each broad category,
            nameplate_mw_tech_{slug} for each fine-grained technology,
            has_transmission_line, load_center.
    """
    dirs = setup_directories()
    cache_file = os.path.join(dirs['processed'], 'gridded_generation_map.nc')

    if os.path.exists(cache_file) and not force_rebuild:
        ds = xr.open_dataset(cache_file)
        print(f"Loaded gridded generation map from cache: {cache_file}")
        return ds

    if generators_path is None:
        generators_path = os.path.join(dirs['raw'], 'eia860', 'texas_generators.csv')

    # Step 1: Load ERA5 template Dataset (lat/lon coords only, no time)
    ds = build_era5_template_dataset()
    lats = ds.latitude.values
    lons = ds.longitude.values

    # Step 2: Bin generators to grid
    print("Binning generators to ERA5 grid...")
    gen_arrays = _bin_generators(generators_path, lats, lons)

    # Step 3: Mark transmission line cells
    print("Marking transmission line cells...")
    tx_arr = _mark_transmission(line_shp_path, lats, lons)

    # Step 4: Mark load center cells
    print("Marking load center cells...")
    load_arr = _mark_load_centers(bus_shp_path, lats, lons)

    # Step 5: Assign all 2D arrays as Dataset variables with (latitude, longitude) dims
    dims = ('latitude', 'longitude')
    for name, arr in gen_arrays.items():
        ds[name] = xr.Variable(dims, arr)
    ds['has_transmission_line'] = xr.Variable(dims, tx_arr)
    ds['load_center'] = xr.Variable(dims, load_arr)

    # Step 6: Save as NetCDF with compression
    encoding = {v: {'zlib': True, 'complevel': 4} for v in ds.data_vars}
    ds.to_netcdf(cache_file, encoding=encoding)

    print(f"\nSaved gridded generation map to {cache_file}")
    print(f"  Cells with generation: {(gen_arrays['total_capacity_mw'] > 0).sum()}")
    print(f"  Cells with transmission: {tx_arr.sum()}")
    print(f"  Cells with load centers: {load_arr.sum()}")
    print(f"  Total nameplate capacity: {gen_arrays['total_capacity_mw'].sum():,.0f} MW")

    return ds


if __name__ == '__main__':
    build_gridded_generation_map(force_rebuild=True)
