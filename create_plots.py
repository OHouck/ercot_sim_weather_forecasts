"""create_plots.py — Visualization functions for ERCOT weather forecast analysis."""

import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import cartopy.io.shapereader as shpreader
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


if __name__ == '__main__':
    dirs = setup_directories()

    out_temp = os.path.join(dirs['root'], 'plots', 'max_temp_july_2025.png')
    plot_max_temperature_map(output_path=out_temp)

    out_wind = os.path.join(dirs['root'], 'plots', 'max_wind_speed_july_2025.png')
    plot_max_wind_speed_map(output_path=out_wind)

    out_combined = os.path.join(dirs['root'], 'plots', 'combined_map_july_2025.png')
    plot_combined_map(output_path=out_combined)
