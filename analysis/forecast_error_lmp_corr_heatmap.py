"""
Spatial heatmap of per-pixel Pearson correlation between weather forecast errors
and system-level LMP across all ERA5 land pixels in Texas.

Unlike the inline cell in gridded_lr.qmd, this script:
  - Covers ALL ERA5 pixels (not just infrastructure pixels)
  - Accepts configurable error variable, LMP variable, and overlay objects
  - Uses vectorized streaming accumulation for memory efficiency

Usage
-----
Run directly:
    uv run python -m analysis.forecast_error_lmp_corr_heatmap

Or import and call:
    from analysis.forecast_error_lmp_corr_heatmap import plot_forecast_error_lmp_correlation
    fig, ax = plot_forecast_error_lmp_correlation(
        error_col='wspd_error_1h',
        lmp_var='system_lmp_std',
        months=[(2025, m) for m in range(1, 13)],
        overlay=['wind', 'solar', 'gas', 'load_center', 'transmission'],
        save_dir='/path/to/figures',
        save_file='corr_map.png',
    )
"""

import os
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import cartopy.crs as ccrs
import cartopy.io.shapereader as shpreader
import geopandas as gpd

sys.path.insert(0, str(Path(__file__).parent.parent))
from helper_funcs import setup_directories


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_error_col(error_col: str) -> tuple:
    """Parse 'wspd_error_18h' -> ('wspd_error', 18)."""
    m = re.match(r'^([a-z_]+)_(\d+)h$', error_col)
    if m is None:
        raise ValueError(
            f"Cannot parse error_col '{error_col}'. "
            "Expected format like 'wspd_error_1h' or 'temp_error_18h'."
        )
    return m.group(1), int(m.group(2))


def _strip_tz(series: pd.Series) -> pd.Series:
    """Return a tz-naive DatetimeIndex/Series (UTC or already naive)."""
    if hasattr(series, 'dt'):
        if series.dt.tz is not None:
            return series.dt.tz_localize(None)
        return series
    # pd.DatetimeIndex
    if series.tz is not None:
        return series.tz_localize(None)
    return series


# ---------------------------------------------------------------------------
# Core: streaming correlation computation over all ERA5 pixels
# ---------------------------------------------------------------------------

def _build_texas_mask(lats: np.ndarray, lons: np.ndarray) -> np.ndarray:
    """
    Return a (n_lat, n_lon) boolean array — True for pixels whose centre
    falls inside the Texas state boundary.  Uses shapely vectorised
    point-in-polygon for speed (~14 k points in <1 s).
    """
    import shapely

    states_shp = shpreader.natural_earth(
        resolution='10m', category='cultural', name='admin_1_states_provinces'
    )
    texas_geom = None
    for record in shpreader.Reader(states_shp).records():
        if record.attributes.get('name') == 'Texas':
            texas_geom = record.geometry
            break
    if texas_geom is None:
        raise ValueError("Texas geometry not found in Natural Earth shapefile.")

    lat_grid, lon_grid = np.meshgrid(lats, lons, indexing='ij')
    points = shapely.points(lon_grid.ravel(), lat_grid.ravel())
    mask = shapely.within(points, texas_geom).reshape(len(lats), len(lons))
    return mask


def _compute_pixel_correlations(
    months: list,
    model: str,
    var_name: str,
    lead_h: int,
    lmp_var: str,
    dirs: dict,
) -> tuple:
    """
    Compute per-pixel Pearson r between forecast error and system LMP.

    Processes one month at a time (streaming), accumulating Pearson sufficient
    statistics (n, Σx, Σy, Σx², Σy², Σxy) into fixed-size numpy arrays.

    Returns
    -------
    lats : ndarray (n_lat,)
    lons : ndarray (n_lon,)
    r    : ndarray (n_lat, n_lon)  — NaN where n < 3 or zero variance
    """
    lats = lons = None
    accum_n = accum_sx = accum_sy = accum_sx2 = accum_sy2 = accum_sxy = None

    for year, month in months:
        nc_path = os.path.join(
            dirs['processed'], 'forecast_errors_era5', model,
            str(year), f'{month:02d}', f'era5_errors_{year}{month:02d}.nc',
        )
        lmp_path = os.path.join(
            dirs['processed'], 'combined_hourly_gridded_data',
            f'pixel_hourly_gfs+hrrr_{year}_{month:02d}.parquet',
        )

        if not os.path.exists(nc_path):
            print(f"  Missing ERA5 errors: {nc_path}, skipping")
            continue
        if not os.path.exists(lmp_path):
            print(f"  Missing pixel_hourly: {lmp_path}, skipping")
            continue

        ds = xr.open_dataset(nc_path)

        # Initialise accumulators once we know the grid
        if lats is None:
            lats = ds['latitude'].values
            lons = ds['longitude'].values
            shape = (len(lats), len(lons))
            accum_n   = np.zeros(shape, dtype=np.int64)
            accum_sx  = np.zeros(shape)
            accum_sy  = np.zeros(shape)
            accum_sx2 = np.zeros(shape)
            accum_sy2 = np.zeros(shape)
            accum_sxy = np.zeros(shape)

        # Select variable and lead time
        if 'lead_hours' in ds.dims or 'lead_hours' in ds.coords:
            arr = ds[var_name].sel(lead_hours=lead_h)
        else:
            arr = ds[var_name]

        # Load system LMP, de-duplicate by valid_time
        lmp_df = pd.read_parquet(lmp_path, columns=['valid_time', lmp_var])
        lmp_df['valid_time'] = _strip_tz(lmp_df['valid_time'])
        lmp_df = lmp_df.drop_duplicates('valid_time').set_index('valid_time')[lmp_var]

        # Align timestamps
        error_times = pd.DatetimeIndex(arr['valid_time'].values)
        common_times = lmp_df.index.intersection(error_times)
        if len(common_times) == 0:
            print(f"  No matching timestamps for {year}-{month:02d}, skipping")
            ds.close()
            continue

        lmp_vals = lmp_df.loc[common_times].values          # shape (T,)
        err_vals = arr.sel(valid_time=common_times.values).values  # shape (T, lat, lon)

        # Vectorised accumulation
        lmp_valid = ~np.isnan(lmp_vals)                        # (T,)  mask NaN LMP hours
        valid = ~np.isnan(err_vals) & lmp_valid[:, np.newaxis, np.newaxis]  # (T, lat, lon)
        x = np.where(valid, err_vals, 0.0)
        y = np.where(valid, lmp_vals[:, np.newaxis, np.newaxis], 0.0)

        accum_n   += valid.sum(axis=0)
        accum_sx  += x.sum(axis=0)
        accum_sy  += y.sum(axis=0)
        accum_sx2 += (x * x).sum(axis=0)
        accum_sy2 += (y * y).sum(axis=0)
        accum_sxy += (x * y).sum(axis=0)

        ds.close()
        print(f"  {year}-{month:02d}: {len(common_times)} timesteps, "
              f"{valid.any(axis=0).sum()} active pixels")

    if lats is None:
        raise FileNotFoundError("No valid ERA5 error or pixel_hourly files found.")

    # Pearson r from sufficient statistics
    n = accum_n.astype(float)
    with np.errstate(invalid='ignore', divide='ignore'):
        denom = np.sqrt(
            np.maximum(0.0, n * accum_sx2 - accum_sx ** 2) *
            np.maximum(0.0, n * accum_sy2 - accum_sy ** 2)
        )
        r = np.where(denom > 0, (n * accum_sxy - accum_sx * accum_sy) / denom, np.nan)
    r[accum_n < 3] = np.nan

    return lats, lons, r


# ---------------------------------------------------------------------------
# Overlay helpers
# ---------------------------------------------------------------------------

_OVERLAY_CONFIG = {
    'wind': {
        'col':    'nameplate_mw_tech_onshore_wind_turbine',
        'mode':   'gt0',
        'marker': 's',
        'color':  'dodgerblue',
        'label':  'Wind generation',
        'size':   22,
    },
    'solar': {
        'col':    'nameplate_mw_tech_solar_photovoltaic',
        'mode':   'gt0',
        'marker': '^',
        'color':  'gold',
        'label':  'Solar generation',
        'size':   22,
    },
    'gas': {
        'col':    '_gas_total',
        'mode':   'gt0',
        'marker': 'D',
        'color':  'darkorange',
        'label':  'Gas generation',
        'size':   20,
    },
    'batteries': {
        'col':    '_bat_total',
        'mode':   'gt0',
        'marker': 'P',
        'color':  'limegreen',
        'label':  'Battery storage',
        'size':   20,
    },
    'coal': {
        'col':    '_coal_total',
        'mode':   'gt0',
        'marker': 'X',
        'color':  'saddlebrown',
        'label':  'Coal generation',
        'size':   20,
    },
    'load_center': {
        'col':    'load_center',
        'mode':   'eq1',
        'marker': 'v',
        'color':  'red',
        'label':  'Load center',
        'size':   24,
    },
}


def _load_generation_map_df(dirs: dict) -> pd.DataFrame:
    """Load gridded_generation_map.nc and return as a flat DataFrame."""
    map_path = os.path.join(dirs['processed'], 'gridded_generation_map.nc')
    if not os.path.exists(map_path):
        raise FileNotFoundError(
            f"gridded_generation_map.nc not found at {map_path}. "
            "Run process_data/gridded_generation_mapping.py first."
        )
    ds = xr.open_dataset(map_path)
    df = ds.to_dataframe().reset_index()
    ds.close()

    # Derived totals for multi-column tech categories
    gas_cols  = [c for c in df.columns if 'natural_gas' in c and c.startswith('nameplate_mw_tech_')]
    bat_cols  = [c for c in df.columns if 'batteries'   in c and c.startswith('nameplate_mw_tech_')]
    coal_cols = [c for c in df.columns if 'coal'        in c and c.startswith('nameplate_mw_tech_')]

    df['_gas_total']  = df[gas_cols].fillna(0).sum(axis=1)  if gas_cols  else 0.0
    df['_bat_total']  = df[bat_cols].fillna(0).sum(axis=1)  if bat_cols  else 0.0
    df['_coal_total'] = df[coal_cols].fillna(0).sum(axis=1) if coal_cols else 0.0

    return df


# ---------------------------------------------------------------------------
# Private single-panel plotting helper
# ---------------------------------------------------------------------------

def _plot_single_correlation_panel(
    ax,
    lats: np.ndarray,
    lons: np.ndarray,
    r_2d: np.ndarray,
    error_col: str,
    lmp_var: str,
    dirs: dict,
    overlay: list,
    clim_shared: float = None,
) -> tuple:
    """
    Draw a single correlation heatmap panel onto an existing GeoAxes.

    Parameters
    ----------
    ax : cartopy GeoAxes
        The axes to draw onto.
    lats, lons : ndarray
        1-D coordinate arrays (already from ERA5 grid).
    r_2d : ndarray (n_lat, n_lon)
        Pre-computed Pearson r, already masked to Texas (NaN outside).
    error_col : str
        Error column name used only for colorbar / title labels.
    lmp_var : str
        LMP variable name used only for labels.
    dirs : dict
        Output of setup_directories().
    overlay : list of str
        Infrastructure overlay keys (same as in plot_forecast_error_lmp_correlation).
    clim_shared : float, optional
        Shared symmetric colormap limit.  If None, computes from r_2d.

    Returns
    -------
    mesh : QuadMesh
        The pcolormesh artist (used by caller for a shared colorbar).
    legend_handles : list
        Matplotlib legend handle objects for the overlays drawn.
    """
    proj = ccrs.PlateCarree()

    # Texas state outline
    states_shp = shpreader.natural_earth(
        resolution='10m', category='cultural', name='admin_1_states_provinces'
    )
    for record in shpreader.Reader(states_shp).records():
        if record.attributes.get('name') == 'Texas':
            ax.add_geometries(
                [record.geometry], proj,
                facecolor='white', edgecolor='black', linewidth=1.0, zorder=1,
            )
            break

    ax.set_extent([-107.5, -93.0, 25.5, 37.0], crs=proj)
    ax.set_facecolor('#cce5f0')  # ocean background

    # Colormap limits
    valid_corrs = r_2d[~np.isnan(r_2d)]
    if clim_shared is not None:
        clim = clim_shared
    else:
        clim = float(np.nanpercentile(np.abs(valid_corrs), 95))
        clim = max(clim, 0.05)

    # Correlation filled grid
    mesh = ax.pcolormesh(
        lons, lats, r_2d,
        cmap='RdBu_r', vmin=-clim, vmax=clim,
        shading='nearest',
        transform=proj, zorder=3,
        rasterized=True,
    )

    # ── Overlays ──
    legend_handles = []

    # Generation / load center overlays
    gen_overlay = [o for o in overlay if o != 'transmission']
    if gen_overlay:
        gen_df = _load_generation_map_df(dirs)

        for item in gen_overlay:
            cfg = _OVERLAY_CONFIG.get(item)
            if cfg is None:
                print(f"  Unknown overlay '{item}', skipping. "
                      "Valid: wind, solar, gas, batteries, coal, load_center, transmission")
                continue

            col = cfg['col']
            if col not in gen_df.columns:
                print(f"  Column '{col}' not in generation map, skipping '{item}'")
                continue

            if cfg['mode'] == 'gt0':
                subset = gen_df[gen_df[col].fillna(0) > 0]
            else:  # 'eq1'
                subset = gen_df[gen_df[col] == 1]

            if len(subset) == 0:
                print(f"  No pixels for overlay '{item}', skipping")
                continue

            ax.scatter(
                subset['longitude'], subset['latitude'],
                marker=cfg['marker'],
                s=cfg['size'],
                facecolors='none',
                edgecolors=cfg['color'],
                linewidths=0.9,
                transform=proj, zorder=6,
            )
            legend_handles.append(
                mlines.Line2D(
                    [], [],
                    color=cfg['color'],
                    marker=cfg['marker'],
                    linestyle='None',
                    markersize=6,
                    markerfacecolor='none',
                    markeredgewidth=0.9,
                    label=cfg['label'],
                )
            )
            print(f"  Overlay '{item}': {len(subset):,} pixels")

    # Transmission line overlay
    if 'transmission' in overlay:
        tx_shp = os.path.join(Path(__file__).parent.parent, 'data', 'Line_Output.shp')
        if os.path.exists(tx_shp):
            tx_lines = gpd.read_file(tx_shp).to_crs(epsg=4326)
            for geom in tx_lines.geometry:
                if geom is None:
                    continue
                if geom.geom_type == 'LineString':
                    xs, ys = geom.xy
                    ax.plot(list(xs), list(ys), color='dimgray', linewidth=0.5,
                            alpha=0.55, transform=proj, zorder=5)
                elif geom.geom_type == 'MultiLineString':
                    for line in geom.geoms:
                        xs, ys = line.xy
                        ax.plot(list(xs), list(ys), color='dimgray', linewidth=0.5,
                                alpha=0.55, transform=proj, zorder=5)
            legend_handles.append(
                mlines.Line2D([], [], color='dimgray', linewidth=1.2,
                              label='Transmission lines')
            )
            print(f"  Overlay 'transmission': {len(tx_lines)} line features")
        else:
            print(f"  Transmission shapefile not found at {tx_shp}, skipping")

    ax.gridlines(draw_labels=False, linewidth=0.3, alpha=0.5)

    return mesh, legend_handles


# ---------------------------------------------------------------------------
# Main plotting function (single-panel, unchanged)
# ---------------------------------------------------------------------------

def plot_forecast_error_lmp_correlation(
    error_col: str,
    lmp_var: str,
    months: list,
    model: str = 'hrrr',
    overlay: list = None,
    save_dir: str = None,
    save_file: str = None,
) -> tuple:
    """
    Plot a Texas map of per-pixel Pearson r between a forecast error and system LMP.

    Parameters
    ----------
    error_col : str
        Forecast error column in pixel_hourly notation, e.g. 'wspd_error_1h',
        'temp_error_18h'.  The variable name and lead time are parsed automatically.
    lmp_var : str
        System LMP outcome variable: 'system_lmp_std', 'system_lmp_max', or
        'system_lmp_mean'.
    months : list of (int, int)
        List of (year, month) tuples to include, e.g. [(2025, m) for m in range(1, 13)].
    model : str
        Forecast model used for the ERA5 error NetCDF (ERA5 errors are stored
        per model).  Default 'hrrr'.  Pixel-hourly parquet always uses the
        combined gfs+hrrr filename.
    overlay : list of str, optional
        Infrastructure objects to draw on top of the correlation map.
        Supported values: 'wind', 'solar', 'gas', 'batteries', 'coal',
        'load_center', 'transmission'.
    save_dir : str, optional
        Directory in which to save the figure.  Created if it does not exist.
    save_file : str, optional
        Filename for the saved figure (e.g. 'my_map.png').  If save_dir is
        provided but save_file is not, a name is generated automatically.

    Returns
    -------
    fig, ax : matplotlib Figure and GeoAxes
    """
    dirs = setup_directories()
    overlay = list(overlay) if overlay else []

    var_name, lead_h = _parse_error_col(error_col)
    print(f"\nComputing corr({error_col}, {lmp_var}) over {len(months)} months ...")

    lats, lons, r_2d = _compute_pixel_correlations(
        months, model, var_name, lead_h, lmp_var, dirs
    )

    # Mask to Texas boundary — NaN outside Texas speeds up rendering
    # and keeps surrounding states/ocean from showing correlation colours.
    print("Building Texas pixel mask ...")
    texas_mask = _build_texas_mask(lats, lons)
    r_2d = np.where(texas_mask, r_2d, np.nan)

    valid_corrs = r_2d[~np.isnan(r_2d)]
    print(f"\nCorrelation summary ({len(valid_corrs):,} pixels inside Texas):")
    print(pd.Series(valid_corrs).describe().round(3).to_string())

    clim = float(np.nanpercentile(np.abs(valid_corrs), 95))
    clim = max(clim, 0.05)
    print(f"Colormap limits: ±{clim:.3f}")

    # ── Build map ──
    proj = ccrs.PlateCarree()
    fig, ax = plt.subplots(figsize=(13, 11), subplot_kw={'projection': proj})

    # Texas state outline
    states_shp = shpreader.natural_earth(
        resolution='10m', category='cultural', name='admin_1_states_provinces'
    )
    for record in shpreader.Reader(states_shp).records():
        if record.attributes.get('name') == 'Texas':
            ax.add_geometries(
                [record.geometry], proj,
                facecolor='white', edgecolor='black', linewidth=1.0, zorder=1,
            )
            break

    ax.set_extent([-107.5, -93.0, 25.5, 37.0], crs=proj)
    ax.set_facecolor('#cce5f0')  # ocean background

    # Correlation filled grid — each 0.1° ERA5 cell fully coloured.
    # pcolormesh with shading='nearest' centres each cell on its lat/lon
    # coordinate.  NaN cells (outside Texas or missing data) are transparent.
    mesh = ax.pcolormesh(
        lons, lats, r_2d,
        cmap='RdBu_r', vmin=-clim, vmax=clim,
        shading='nearest',
        transform=proj, zorder=3,
        rasterized=True,
    )
    cbar = plt.colorbar(mesh, ax=ax, shrink=0.60, pad=0.02)
    cbar.set_label(f'Pearson r  ({error_col}, {lmp_var})', fontsize=10)

    # ── Overlays ──
    legend_handles = []

    # Generation / load center overlays
    gen_overlay = [o for o in overlay if o != 'transmission']
    if gen_overlay:
        gen_df = _load_generation_map_df(dirs)

        for item in gen_overlay:
            cfg = _OVERLAY_CONFIG.get(item)
            if cfg is None:
                print(f"  Unknown overlay '{item}', skipping. "
                      "Valid: wind, solar, gas, batteries, coal, load_center, transmission")
                continue

            col = cfg['col']
            if col not in gen_df.columns:
                print(f"  Column '{col}' not in generation map, skipping '{item}'")
                continue

            if cfg['mode'] == 'gt0':
                subset = gen_df[gen_df[col].fillna(0) > 0]
            else:  # 'eq1'
                subset = gen_df[gen_df[col] == 1]

            if len(subset) == 0:
                print(f"  No pixels for overlay '{item}', skipping")
                continue

            ax.scatter(
                subset['longitude'], subset['latitude'],
                marker=cfg['marker'],
                s=cfg['size'],
                facecolors='none',
                edgecolors=cfg['color'],
                linewidths=0.9,
                transform=proj, zorder=6,
            )
            legend_handles.append(
                mlines.Line2D(
                    [], [],
                    color=cfg['color'],
                    marker=cfg['marker'],
                    linestyle='None',
                    markersize=6,
                    markerfacecolor='none',
                    markeredgewidth=0.9,
                    label=cfg['label'],
                )
            )
            print(f"  Overlay '{item}': {len(subset):,} pixels")

    # Transmission line overlay
    if 'transmission' in overlay:
        tx_shp = os.path.join(Path(__file__).parent.parent, 'data', 'Line_Output.shp')
        if os.path.exists(tx_shp):
            tx_lines = gpd.read_file(tx_shp).to_crs(epsg=4326)
            for geom in tx_lines.geometry:
                if geom is None:
                    continue
                if geom.geom_type == 'LineString':
                    xs, ys = geom.xy
                    ax.plot(list(xs), list(ys), color='dimgray', linewidth=0.5,
                            alpha=0.55, transform=proj, zorder=5)
                elif geom.geom_type == 'MultiLineString':
                    for line in geom.geoms:
                        xs, ys = line.xy
                        ax.plot(list(xs), list(ys), color='dimgray', linewidth=0.5,
                                alpha=0.55, transform=proj, zorder=5)
            legend_handles.append(
                mlines.Line2D([], [], color='dimgray', linewidth=1.2,
                              label='Transmission lines')
            )
            print(f"  Overlay 'transmission': {len(tx_lines)} line features")
        else:
            print(f"  Transmission shapefile not found at {tx_shp}, skipping")

    # Title & legend
    year_lo = min(y for y, _ in months)
    year_hi = max(y for y, _ in months)
    month_lo = min(mo for _, mo in months)
    month_hi = max(mo for _, mo in months)
    yr_str = str(year_lo) if year_lo == year_hi else f"{year_lo}–{year_hi}"
    ax.set_title(
        f"Per-pixel  corr({error_col},  {lmp_var})\n"
        f"{yr_str}  months {month_lo}–{month_hi}  |  {model.upper()}  |  "
        f"{len(valid_corrs):,} Texas pixels",
        fontsize=11,
    )

    if legend_handles:
        ax.legend(handles=legend_handles, loc='lower left', fontsize=8,
                  framealpha=0.85)

    ax.gridlines(draw_labels=True, linewidth=0.3, alpha=0.5)
    plt.tight_layout()

    # ── Save ──
    if save_dir is not None or save_file is not None:
        fname = save_file or f'corr_{error_col}_{lmp_var}_{model}.png'
        if save_dir is not None:
            os.makedirs(save_dir, exist_ok=True)
            fname = os.path.join(save_dir, fname)
        fig.savefig(fname, dpi=150, bbox_inches='tight')
        print(f"Saved figure to {fname}")

    return fig, ax


# ---------------------------------------------------------------------------
# 2×2 multi-panel correlation heatmap
# ---------------------------------------------------------------------------

def run_correlation_heatmaps(
    months: list,
    lmp_var: str = 'system_lmp_std',
    overlay: list = None,
    save_dir: str = None,
) -> str:
    """
    Produce a 2×2 figure showing per-pixel Pearson r between forecast errors
    (HRRR 1h and GFS day-ahead, for temperature and wind speed) and a system
    LMP variable.

    Parameters
    ----------
    months : list of (int, int)
        List of (year, month) tuples, e.g. [(2025, m) for m in range(1, 13)].
    lmp_var : str
        System LMP column: 'system_lmp_std', 'system_lmp_max', 'system_lmp_mean'.
    overlay : list of str, optional
        Infrastructure overlays for each panel.  Defaults to ['wind', 'solar', 'gas'].
    save_dir : str, optional
        Directory for the output PNG.  Defaults to
        {dirs['figures']}/correlation_heatmaps/.

    Returns
    -------
    save_path : str
        Absolute path of the saved PNG file.
    """
    dirs = setup_directories()
    overlay = list(overlay) if overlay is not None else ['wind', 'solar', 'gas']

    if save_dir is None:
        save_dir = os.path.join(dirs['figures'], 'correlation_heatmaps')
    os.makedirs(save_dir, exist_ok=True)

    panels = [
        ('temp_error_1h', 'hrrr', 'HRRR 1h — Temperature Error'),
        ('wspd_error_1h', 'hrrr', 'HRRR 1h — Wind Speed Error'),
        ('temp_error_0h', 'gfs',  'GFS Day-Ahead — Temperature Error'),
        ('wspd_error_0h', 'gfs',  'GFS Day-Ahead — Wind Speed Error'),
    ]

    # ── Compute correlations and masks for all 4 panels ──
    panel_data = []
    abs_p95_values = []

    for error_col, model, title in panels:
        var_name, lead_h = _parse_error_col(error_col)
        print(f"\nComputing corr({error_col}, {lmp_var}) [{model.upper()}] ...")
        lats, lons, r_raw = _compute_pixel_correlations(
            months, model, var_name, lead_h, lmp_var, dirs
        )
        print("Building Texas pixel mask ...")
        texas_mask = _build_texas_mask(lats, lons)
        r_2d = np.where(texas_mask, r_raw, np.nan)

        valid_corrs = r_2d[~np.isnan(r_2d)]
        p95 = float(np.nanpercentile(np.abs(valid_corrs), 95)) if len(valid_corrs) else 0.0
        abs_p95_values.append(p95)
        panel_data.append((lats, lons, r_2d, error_col, title))
        print(f"  {len(valid_corrs):,} Texas pixels, 95th pct |r| = {p95:.3f}")

    # Shared colormap limit: max of per-panel 95th percentiles
    clim_shared = max(abs_p95_values)
    clim_shared = max(clim_shared, 0.05)
    print(f"\nShared colormap limits: ±{clim_shared:.3f}")

    # ── Build 2×2 figure ──
    proj = ccrs.PlateCarree()
    fig, axes = plt.subplots(
        2, 2, figsize=(18, 14),
        subplot_kw={'projection': proj},
        gridspec_kw={'hspace': 0.15, 'wspace': 0.05},
    )

    last_mesh = None
    all_legend_handles = []

    for idx, (ax, (lats, lons, r_2d, error_col, title)) in enumerate(
        zip(axes.ravel(), panel_data)
    ):
        print(f"\nRendering panel: {title}")
        mesh, legend_handles = _plot_single_correlation_panel(
            ax=ax,
            lats=lats,
            lons=lons,
            r_2d=r_2d,
            error_col=error_col,
            lmp_var=lmp_var,
            dirs=dirs,
            overlay=overlay,
            clim_shared=clim_shared,
        )
        last_mesh = mesh
        # Collect unique legend handles (by label) from the first panel only
        # to avoid duplication in the shared legend
        if idx == 0:
            all_legend_handles = legend_handles

        # Column titles (top row only)
        if idx == 0:
            ax.set_title('Temperature Error', fontsize=12, pad=6)
        elif idx == 1:
            ax.set_title('Wind Speed Error', fontsize=12, pad=6)
        else:
            ax.set_title('')

    # Row labels via fig.text (left side, vertically centred per row)
    fig.text(
        0.01, 0.74, 'HRRR 1h Short-Range',
        va='center', ha='left', fontsize=13, fontweight='bold', rotation=90,
    )
    fig.text(
        0.01, 0.30, 'GFS Day-Ahead',
        va='center', ha='left', fontsize=13, fontweight='bold', rotation=90,
    )

    # Shared figure title
    fig.suptitle(
        f'Per-pixel correlation: Forecast Error vs. LMP Spread ({lmp_var})',
        fontsize=15, y=0.98,
    )

    # Shared colorbar spanning all 4 axes
    cbar = fig.colorbar(
        last_mesh,
        ax=axes.ravel().tolist(),
        orientation='vertical',
        shrink=0.55,
        pad=0.02,
        aspect=30,
    )
    cbar.set_label(f'Pearson r  (forecast error, {lmp_var})', fontsize=11)

    # Shared legend (from first panel's overlay handles)
    if all_legend_handles:
        fig.legend(
            handles=all_legend_handles,
            loc='lower center',
            ncol=len(all_legend_handles),
            fontsize=9,
            framealpha=0.85,
            bbox_to_anchor=(0.45, 0.01),
        )

    # ── Save ──
    save_path = os.path.join(save_dir, 'corr_heatmap_2x2.png')
    fig.savefig(save_path, dpi=150, bbox_inches='tight')
    print(f"\nSaved 2×2 correlation heatmap to {save_path}")

    return save_path


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    _months = [(2025, m) for m in range(1, 13)]

    _save_path = run_correlation_heatmaps(
        months=_months,
        lmp_var='system_lmp_std',
        overlay=['wind', 'solar', 'gas'],
    )
    plt.show()
