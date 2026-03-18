"""create_plots.py — Visualization functions for ERCOT weather forecast analysis."""

import os
import glob
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import cartopy.io.shapereader as shpreader
import geopandas as gpd
import xarray as xr
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from helper_funcs import setup_directories


def parse_tmp(tmp_str):
    """Parse ISD TMP field to degrees Celsius."""
    if pd.isna(tmp_str) or '+9999' in str(tmp_str):
        return None
    return int(str(tmp_str).split(',')[0]) / 10.0


def parse_wnd_speed(wnd_str):
    """Parse ISD WND field to wind speed in m/s."""
    if pd.isna(wnd_str):
        return None
    parts = str(wnd_str).split(',')
    if len(parts) < 5 or parts[3] == '9999':
        return None
    return int(parts[3]) / 10.0


def load_station_metadata():
    """Load station metadata (lat, lon, name, etc.)."""
    dirs = setup_directories()
    stations_file = os.path.join(dirs['raw'], 'weather_stations', 'stations.csv')
    return pd.read_csv(stations_file, dtype={'usaf': str, 'wban': str, 'station_id': str})


def compute_station_stat(year, month, stat_func, col='TMP', parser=None):
    """Compute a per-station statistic from raw ISD CSVs.

    Args:
        year: Integer year
        month: Integer month
        stat_func: Function to apply to a Series of parsed values (e.g. 'max', 'mean')
        col: Column name to parse (default 'TMP')
        parser: Function to parse raw field strings. Defaults to parse_tmp.

    Returns:
        DataFrame with station_id and the computed statistic
    """
    if parser is None:
        parser = parse_tmp

    dirs = setup_directories()
    data_dir = os.path.join(dirs['raw'], 'weather_stations', str(year), f"{month:02d}")
    csv_files = glob.glob(os.path.join(data_dir, '*.csv'))

    results = []
    for fpath in csv_files:
        station_id = os.path.basename(fpath).replace('.csv', '')
        df = pd.read_csv(fpath, dtype={'STATION': str})
        values = df[col].apply(parser).dropna()
        if len(values) == 0:
            continue
        results.append({'station_id': station_id, 'value': stat_func(values)})

    return pd.DataFrame(results)


def map_station_values(values_df, stations_df, title, label, cmap='RdYlBu_r',
                       figsize=(10, 8), output_path=None):
    """Plot a scatter map of station-level values over Texas.

    Args:
        values_df: DataFrame with 'station_id' and 'value' columns
        stations_df: DataFrame with 'station_id', 'lat', 'lon' columns
        title: Plot title
        label: Colorbar label
        cmap: Matplotlib colormap name
        figsize: Figure size tuple
        output_path: If provided, save figure to this path
    """
    merged = stations_df.merge(values_df, on='station_id', how='inner')

    proj = ccrs.PlateCarree()
    fig, ax = plt.subplots(figsize=figsize, subplot_kw={'projection': proj})

    # Draw Texas state outline
    states_shp = shpreader.natural_earth(
        resolution='10m', category='cultural', name='admin_1_states_provinces')
    for record in shpreader.Reader(states_shp).records():
        if record.attributes.get('name') == 'Texas':
            ax.add_geometries(
                [record.geometry], proj,
                facecolor='#f0f0f0', edgecolor='black', linewidth=1.2)
            break

    scatter = ax.scatter(
        merged['lon'], merged['lat'],
        c=merged['value'],
        cmap=cmap,
        s=40,
        edgecolors='k',
        linewidths=0.3,
        alpha=0.85,
        transform=proj,
        zorder=5,
    )

    cbar = plt.colorbar(scatter, ax=ax, shrink=0.8, pad=0.02)
    cbar.set_label(label, fontsize=12)

    ax.set_title(title, fontsize=14)
    ax.set_extent([-107.5, -93.0, 25.5, 37.0], crs=proj)

    ax.gridlines(draw_labels=True, linewidth=0.3, alpha=0.5)

    fig.tight_layout()

    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        # Not saving the figure when testing
        # fig.savefig(output_path, dpi=150, bbox_inches='tight')
        # print(f"Saved to {output_path}")

    plt.show()
    return fig, ax


def plot_max_temperature_map(year=2025, month=7, output_path=None):
    """Map the maximum temperature reached at each Texas weather station.

    Args:
        year: Integer year
        month: Integer month
        output_path: If provided, save figure to this path
    """
    stations = load_station_metadata()
    stats = compute_station_stat(year, month, stat_func=lambda s: s.max())

    month_name = pd.Timestamp(year=year, month=month, day=1).strftime('%B')
    title = f'Maximum Temperature by Station — {month_name} {year}'
    label = 'Max Temperature (°C)'

    return map_station_values(stats, stations, title, label, output_path=output_path)


def plot_max_wind_speed_map(year=2025, month=7, output_path=None):
    """Map the maximum wind speed reached at each Texas weather station.

    Args:
        year: Integer year
        month: Integer month
        output_path: If provided, save figure to this path
    """
    stations = load_station_metadata()
    stats = compute_station_stat(
        year, month, stat_func=lambda s: s.max(), col='WND', parser=parse_wnd_speed)

    month_name = pd.Timestamp(year=year, month=month, day=1).strftime('%B')
    title = f'Maximum Wind Speed by Station — {month_name} {year}'
    label = 'Max Wind Speed (m/s)'

    return map_station_values(stats, stations, title, label, cmap='YlGnBu',
                              output_path=output_path)


def _draw_texas(ax, proj):
    """Draw the Texas state outline on a cartopy axis."""
    states_shp = shpreader.natural_earth(
        resolution='10m', category='cultural', name='admin_1_states_provinces')
    for record in shpreader.Reader(states_shp).records():
        if record.attributes.get('name') == 'Texas':
            ax.add_geometries(
                [record.geometry], proj,
                facecolor='#f0f0f0', edgecolor='black', linewidth=1.0)
            break
    ax.set_extent([-107.5, -93.0, 25.5, 37.0], crs=proj)


def plot_combined_map(year=2025, month=7, output_path=None):
    """3-panel Texas map: max temperature, max wind speed, and max LMP.

    Left panel: Weather stations colored by max temperature (°C)
    Center panel: Weather stations colored by max wind speed (m/s)
    Right panel: ERCOT resource nodes colored by max LMP ($/MWh)

    Args:
        year: Integer year
        month: Integer month
        output_path: If provided, save figure to this path
    """
    from process_ercot import compute_max_lmp_by_node, build_node_coordinates

    stations = load_station_metadata()
    max_temp = compute_station_stat(year, month, stat_func=lambda s: s.max())
    max_wind = compute_station_stat(
        year, month, stat_func=lambda s: s.max(), col='WND', parser=parse_wnd_speed)
    max_lmp = compute_max_lmp_by_node(year, month)
    node_coords = build_node_coordinates()

    # Merge data
    temp_merged = stations.merge(max_temp, on='station_id', how='inner')
    wind_merged = stations.merge(max_wind, on='station_id', how='inner')
    max_lmp = max_lmp.rename(columns={'settlementPoint': 'settlement_point'})
    lmp_merged = node_coords.merge(max_lmp, on='settlement_point', how='inner')

    month_name = pd.Timestamp(year=year, month=month, day=1).strftime('%B')
    proj = ccrs.PlateCarree()

    fig, axes = plt.subplots(1, 3, figsize=(22, 8),
                             subplot_kw={'projection': proj})

    # Panel 1: Max Temperature
    ax = axes[0]
    _draw_texas(ax, proj)
    sc1 = ax.scatter(
        temp_merged['lon'], temp_merged['lat'], c=temp_merged['value'],
        cmap='RdYlBu_r', s=35, edgecolors='k', linewidths=0.3,
        alpha=0.85, transform=proj, zorder=5)
    plt.colorbar(sc1, ax=ax, shrink=0.7, pad=0.02, label='°C')
    ax.set_title(f'Max Temperature', fontsize=13)
    ax.gridlines(draw_labels=True, linewidth=0.3, alpha=0.5)

    # Panel 2: Max Wind Speed
    ax = axes[1]
    _draw_texas(ax, proj)
    sc2 = ax.scatter(
        wind_merged['lon'], wind_merged['lat'], c=wind_merged['value'],
        cmap='YlGnBu', s=35, edgecolors='k', linewidths=0.3,
        alpha=0.85, transform=proj, zorder=5)
    plt.colorbar(sc2, ax=ax, shrink=0.7, pad=0.02, label='m/s')
    ax.set_title(f'Max Wind Speed', fontsize=13)
    ax.gridlines(draw_labels=True, linewidth=0.3, alpha=0.5)

    # Panel 3: Max LMP
    ax = axes[2]
    _draw_texas(ax, proj)
    sc3 = ax.scatter(
        lmp_merged['lon'], lmp_merged['lat'], c=lmp_merged['max_lmp'],
        cmap='hot_r', s=35, edgecolors='k', linewidths=0.3,
        alpha=0.85, transform=proj, zorder=5, marker='D')
    plt.colorbar(sc3, ax=ax, shrink=0.7, pad=0.02, label='$/MWh')
    ax.set_title(f'Max LMP (Resource Nodes)', fontsize=13)
    ax.gridlines(draw_labels=True, linewidth=0.3, alpha=0.5)

    fig.suptitle(f'Weather Stations & ERCOT Nodes — {month_name} {year}',
                 fontsize=16, y=1.02)
    fig.tight_layout()

    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        fig.savefig(output_path, dpi=150, bbox_inches='tight')
        print(f"Saved to {output_path}")

    plt.show()
    return fig, axes


def plot_ercot_map(output_path=None):
    """Map the simulated ERCOT grid (TX-123BT buses and lines) alongside matched settlement points.

    Plots transmission lines in gray, simulation bus nodes in blue, matched
    ERCOT settlement points (from node_coordinates.csv) in red, and weather
    stations in green.

    Args:
        output_path: If provided, save figure to this path
    """
    from process_ercot import build_node_coordinates

    dirs = setup_directories()
    gis_dir = os.path.join(dirs['root'], 'Texas_GIS_Data')

    buses = gpd.read_file(os.path.join(gis_dir, 'Bus', 'Bus_Output.shp'))
    lines = gpd.read_file(os.path.join(gis_dir, 'Line', 'Line_Output.shp'))
    node_coords = build_node_coordinates()
    stations = load_station_metadata()

    proj = ccrs.PlateCarree()
    fig, ax = plt.subplots(figsize=(12, 10), subplot_kw={'projection': proj})

    _draw_texas(ax, proj)

    # Transmission lines
    for _, row in lines.iterrows():
        coords = list(row.geometry.coords)
        lons, lats = zip(*coords)
        ax.plot(lons, lats, color='#888888', linewidth=0.6, alpha=0.5,
                transform=proj, zorder=2)

    # Simulation buses
    ax.scatter(
        buses['Bus_longit'], buses['Bus_latitu'],
        c='#1f77b4', s=30, edgecolors='k', linewidths=0.4,
        alpha=0.9, transform=proj, zorder=4, label=f'Sim buses ({len(buses)})')

    # Matched settlement points
    ax.scatter(
        node_coords['lon'], node_coords['lat'],
        c='#d62728', s=12, edgecolors='none',
        alpha=0.6, transform=proj, zorder=3, marker='.',
        label=f'Matched settlement pts ({len(node_coords)})')

    # Weather stations
    ax.scatter(
        stations['lon'], stations['lat'],
        c='#2ca02c', s=40, edgecolors='k', linewidths=0.4,
        alpha=0.8, transform=proj, zorder=5, marker='^',
        label=f'Weather stations ({len(stations)})')

    ax.legend(loc='lower left', fontsize=10, framealpha=0.9)
    ax.set_title('Simulated ERCOT Grid (TX-123BT), Settlement Points & Weather Stations', fontsize=14)
    ax.gridlines(draw_labels=True, linewidth=0.3, alpha=0.5)

    fig.tight_layout()

    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        fig.savefig(output_path, dpi=150, bbox_inches='tight')
        print(f"Saved to {output_path}")

    plt.show()
    return fig, ax


def map_power_grid(ds, variables, title=None, cmap='viridis', figsize=(10, 8),
                   output_path=None):
    """Map gridded power variables over Texas.

    Args:
        ds: xarray Dataset loaded from gridded_generation_map.nc.
        variables: Variable name string or list of variable names to combine.
            If a list is provided, values are summed cell-wise.
        title: Optional plot title. If None, generated from variables.
        cmap: Matplotlib colormap name.
        figsize: Figure size tuple.
        output_path: If provided, save figure to this path.

    Returns:
        (fig, ax, combined_da): Matplotlib figure/axis and combined DataArray.
    """
    try:
        import xarray as xr
    except ImportError as exc:
        raise ImportError("xarray is required for map_power_grid") from exc

    if not isinstance(ds, xr.Dataset):
        raise TypeError("ds must be an xarray.Dataset")

    if isinstance(variables, str):
        variables = [variables]
    elif not isinstance(variables, (list, tuple)) or len(variables) == 0:
        raise ValueError("variables must be a non-empty string or list of strings")

    missing = [v for v in variables if v not in ds.data_vars]
    if missing:
        raise KeyError(f"Variables not found in dataset: {missing}")

    lat_name = 'latitude' if 'latitude' in ds.coords else 'lat'
    lon_name = 'longitude' if 'longitude' in ds.coords else 'lon'
    if lat_name not in ds.coords or lon_name not in ds.coords:
        raise KeyError("Dataset must contain latitude/longitude or lat/lon coordinates")

    # Combine the requested layers with a cell-wise sum.
    arrays = [ds[v].astype(float) for v in variables]
    combined_da = arrays[0].copy()
    for arr in arrays[1:]:
        combined_da = combined_da + arr

    # Ensure we are plotting a 2D raster only.
    expected_dims = {lat_name, lon_name}
    if set(combined_da.dims) != expected_dims:
        raise ValueError(
            f"Combined variable must have dims {expected_dims}, got {combined_da.dims}"
        )

    lats = ds[lat_name].values
    lons = ds[lon_name].values
    values = combined_da.values
    masked_values = np.ma.masked_where(~np.isfinite(values) | (values == 0), values)

    plot_cmap = plt.get_cmap(cmap)
    if hasattr(plot_cmap, 'copy'):
        plot_cmap = plot_cmap.copy()
    plot_cmap.set_bad((0, 0, 0, 0))

    if title is None:
        joined = ' + '.join(variables)
        title = f'Power Grid Map: {joined}'

    proj = ccrs.PlateCarree()
    fig, ax = plt.subplots(figsize=figsize, subplot_kw={'projection': proj})

    _draw_texas(ax, proj)

    mesh = ax.pcolormesh(
        lons,
        lats,
        masked_values,
        cmap=plot_cmap,
        shading='auto',
        transform=proj,
        zorder=3,
    )

    cbar = plt.colorbar(mesh, ax=ax, shrink=0.8, pad=0.02)
    cbar.set_label('Combined Value', fontsize=11)

    ax.set_title(title, fontsize=14)
    ax.gridlines(draw_labels=True, linewidth=0.3, alpha=0.5)
    fig.tight_layout()

    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        fig.savefig(output_path, dpi=150, bbox_inches='tight')
        print(f"Saved to {output_path}")

    plt.show()
    return fig, ax, combined_da


if __name__ == '__main__':
    dirs = setup_directories()

    # out_temp = os.path.join(dirs['root'], 'plots', 'max_temp_july_2025.png')
    # plot_max_temperature_map(output_path=out_temp)

    # out_wind = os.path.join(dirs['root'], 'plots', 'max_wind_speed_july_2025.png')
    # plot_max_wind_speed_map(output_path=out_wind)

    # out_combined = os.path.join(dirs['root'], 'plots', 'combined_map_july_2025.png')
    # plot_combined_map(output_path=out_combined)
    
    gridded_generation_path = os.path.join(dirs['processed'], 'gridded_generation_map.nc')
    ds = xr.open_dataset(gridded_generation_path)
    data_vars = list(ds.data_vars)
    solar = ["nameplate_mw_tech_solar_photovoltaic"]
    wind = ["nameplate_mw_tech_onshore_wind_turbine"]
    transmission = ["has_transmission_line"]
    load = ["load_center"]
    gas = ["nameplate_mw_tech_natural_gas_fired_combustion_turbine", "nameplate_mw_tech_natural_gas_fired_combined_cycle", "nameplate_mw_tech_natural_gas_steam_turbine", "nameplate_mw_tech_natural_gas_internal_combustion_engine"]
    coal = ["nameplate_mw_tech_conventional_steam_coal"]
    oil = ["nameplate_mw_tech_petroleum_liquids", "nameplate_mw_tech_petroleum_coke"]
    nuclear = ["nameplate_mw_tech_nuclear"]
    batteries = ["nameplate_mw_tech_batteries"]
    total_generation = ["total_capacity_mw"]
    number_of_generators = ["n_generators"]
    other = set(data_vars) - set(solar + wind + transmission + load + gas + coal + oil + total_generation + batteries + number_of_generators + nuclear)
    print(other)
    # map_power_grid(ds, variables = solar , title='Solar', cmap='inferno')
    # map_power_grid(ds, variables = wind, title='Wind', cmap='inferno')
    # map_power_grid(ds, variables = gas, title='Natural Gas', cmap='inferno')
    # map_power_grid(ds, variables = coal, title='Coal', cmap='inferno')
    # map_power_grid(ds, variables = oil, title='Oil', cmap='inferno')
    # map_power_grid(ds, variables = nuclear, title='Nuclear', cmap='inferno')
    # map_power_grid(ds, variables = batteries, title='Batteries', cmap='inferno')
    # map_power_grid(ds, variables = transmission, title='Transmission Lines', cmap='gray')
    # map_power_grid(ds, variables = load, title='Load Centers', cmap='Reds')
    # map_power_grid(ds, variables = total_generation, title='Total Generation Capacity', cmap='inferno')


