"""
Pixel-level regression maps for ERCOT forecast error analysis.

For each ERA5 pixel, runs a regression of system_lmp_std on forecast error
variables (controlling for weather conditions and time fixed effects), then
plots a 2×2 map of coefficient estimates for pixels with significant effects.

Usage:
    uv run python -m analysis.pixel_regression_maps
"""

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import cartopy.crs as ccrs
import cartopy.io.shapereader as shpreader
import geopandas as gpd
from shapely.geometry import box as shapely_box
import pyfixest as pf
import xarray as xr

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories

ROOT = Path(__file__).resolve().parent.parent

DEPVAR = "economic_congestion_cost"
# All 4 error variables estimated jointly in one regression per pixel
ERROR_VARS = [
    "temp_error_1h", "wspd_error_1h", "temp_error_0h", "wspd_error_0h",
]
CONTROLS = ["era5_temp", "era5_wspd", "actual_load", "is_weekend"]
FE = ["hour_of_day", "month"]
SIG_LEVEL = 0.05

REGIMES = {
    "extreme_cold": {
        "filter_col": "regime_temp",
        "filter_val": "extreme_cold",
        "label": "Extreme Cold (Bottom 5% Temp)",
    },
    "extreme_heat": {
        "filter_col": "regime_temp",
        "filter_val": "extreme_heat",
        "label": "Extreme Heat (Top 5% Temp)",
    },
    "high_wind": {
        "filter_col": "regime_wind",
        "filter_val": "high_wind",
        "label": "High Wind (Top 10% Wind Speed)",
    },
    "stressed_grid": {
        "filter_col": "regime_grid",
        "filter_val": "stressed",
        "label": "Stressed Grid (Top 5% LMP Max)",
    },
}

SEASONS = {
    "summer":   {"months": [6, 7, 8],               "label": "Summer (Jun–Aug)"},
    "winter":   {"months": [12, 1, 2],              "label": "Winter (Dec–Feb)"},
    "spring_fall": {"months": [3, 4, 5, 9, 10, 11],    "label": "Spring/Fall (Mar–May, Sep–Nov)"},
}

DEFAULT_MONTHS = [(2025, m) for m in range(1, 13)]

TEXAS_CITIES = ['Houston', 'Dallas', 'Fort Worth', 'San Antonio', 'Austin']

_OVERLAY_CONFIG = {
    'wind': {
        'col':    'nameplate_mw_tech_onshore_wind_turbine',
        'mode':   'gt0',
        'marker': 's',
        'color':  'dodgerblue',
        'label':  'Wind generation',
        'size':   9,
    },
    'solar': {
        'col':    'nameplate_mw_tech_solar_photovoltaic',
        'mode':   'gt0',
        'marker': '^',
        'color':  'gold',
        'label':  'Solar generation',
        'size':   9,
    },
}


def _period_label_from_months(months):
    """Return a human-readable period label for title text.

    Returns None for full-year selections.
    """
    month_set = {month for _, month in months}
    if month_set == set(range(1, 13)):
        return None

    for season_meta in SEASONS.values():
        if month_set == set(season_meta["months"]):
            return season_meta["label"]

    return "Custom Period"


def _draw_city_boundaries(ax, proj):
    """Draw major Texas city outlines and labels using Natural Earth data.

    Uses the 10m urban_areas polygons matched to populated_places points for
    Houston, Dallas, Fort Worth, San Antonio, and Austin.

    Returns
    -------
    list of matplotlib legend handles
    """
    urban_shp = shpreader.natural_earth(
        resolution='10m', category='cultural', name='urban_areas'
    )
    urban_gdf = gpd.read_file(urban_shp)
    if urban_gdf.crs is None:
        urban_gdf = urban_gdf.set_crs(epsg=4326)

    tx_bbox = shapely_box(-107.5, 25.5, -93.0, 37.0)
    urban_tx = urban_gdf[urban_gdf.geometry.intersects(tx_bbox)].copy()

    places_shp = shpreader.natural_earth(
        resolution='10m', category='cultural', name='populated_places'
    )
    places_gdf = gpd.read_file(places_shp)
    if places_gdf.crs is None:
        places_gdf = places_gdf.set_crs(epsg=4326)

    cities = places_gdf[
        places_gdf['NAME'].isin(TEXAS_CITIES) & (places_gdf['ADM0_A3'] == 'USA')
    ].copy()

    if len(urban_tx) > 0 and len(cities) > 0:
        joined = gpd.sjoin(
            cities[['NAME', 'geometry']],
            urban_tx.reset_index()[['geometry']].rename(columns={'index': 'urban_idx'}),
            how='left',
            predicate='within',
        )
        urban_indices = joined['index_right'].dropna().astype(int).unique()
        urban_to_draw = urban_tx.iloc[urban_indices]

        for geom in urban_to_draw.geometry:
            ax.add_geometries(
                [geom], crs=proj,
                facecolor='none', edgecolor='#555555',
                linewidth=0.9, zorder=4,
            )

    for _, city_row in cities.iterrows():
        ax.text(
            city_row.geometry.x, city_row.geometry.y,
            city_row['NAME'],
            transform=proj, fontsize=6.5,
            ha='center', va='bottom',
            color='#333333', zorder=7, fontweight='bold',
        )

    return [mlines.Line2D([], [], color='#555555', linewidth=0.9, label='Major cities')]


def _load_generation_map_df(dirs):
    """Load gridded_generation_map.nc and return as a flat DataFrame."""
    map_path = os.path.join(dirs['processed'], 'gridded_generation_map.nc')
    ds = xr.open_dataset(map_path)
    df = ds.to_dataframe().reset_index()
    ds.close()
    gas_cols = [c for c in df.columns if 'natural_gas' in c and c.startswith('nameplate_mw_tech_')]
    df['_gas_total'] = df[gas_cols].fillna(0).sum(axis=1) if gas_cols else 0.0
    return df


def _draw_overlays(ax, dirs, overlay, proj):
    """Draw generation markers, city boundaries, and transmission lines onto ax.

    Parameters
    ----------
    ax : cartopy GeoAxes
    dirs : dict
    overlay : list of str
        Subset of 'wind', 'solar', 'transmission', 'cities'.
    proj : cartopy CRS

    Returns
    -------
    list of matplotlib legend handles
    """
    legend_handles = []

    if 'cities' in overlay:
        legend_handles.extend(_draw_city_boundaries(ax, proj))

    gen_overlay = [o for o in overlay if o not in ('transmission', 'cities')]
    if gen_overlay:
        gen_df = _load_generation_map_df(dirs)
        for item in gen_overlay:
            cfg = _OVERLAY_CONFIG.get(item)
            if cfg is None:
                continue
            col = cfg['col']
            if col not in gen_df.columns:
                continue
            subset = gen_df[gen_df[col].fillna(0) > 0] if cfg['mode'] == 'gt0' else gen_df[gen_df[col] == 1]
            if len(subset) == 0:
                continue
            ax.scatter(
                subset['longitude'], subset['latitude'],
                marker=cfg['marker'], s=cfg['size'],
                facecolors='none', edgecolors=cfg['color'],
                linewidths=0.7, transform=proj, zorder=6,
            )
            legend_handles.append(mlines.Line2D(
                [], [], color=cfg['color'], marker=cfg['marker'],
                linestyle='None', markersize=5,
                markerfacecolor='none', markeredgewidth=0.7,
                label=cfg['label'],
            ))

    if 'transmission' in overlay:
        tx_shp = os.path.join(dirs['root'], 'Texas_GIS_Data', 'Line', 'Line_Output.shp')
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
            legend_handles.append(mlines.Line2D(
                [], [], color='dimgray', linewidth=1.2, label='Transmission lines',
            ))

    return legend_handles




def add_regime_columns(df):
    """Add weather/grid regime columns using system-wide hourly aggregates."""
    from process_data.classify_weather_regimes import classify_regimes
    return classify_regimes(df)


def filter_to_regime(df, regime_name):
    """Filter DataFrame to hours matching a specific regime.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with regime columns added by add_regime_columns().
    regime_name : str
        Key in REGIMES dict.

    Returns
    -------
    pd.DataFrame
    """
    spec = REGIMES[regime_name]
    mask = df[spec["filter_col"]] == spec["filter_val"]
    filtered = df[mask].copy()
    n_hours = filtered["valid_time"].nunique()
    print(f"  Regime '{regime_name}': {n_hours} hours, {len(filtered):,} rows")
    return filtered


def load_pixel_data(months):
    """Load the pixel × hour analysis dataset for the given months.

    Reads directly from the pre-built pixel_hourly parquets, which already
    contain ERA5 forecast errors (HRRR 1h/18h + GFS 0h), observed weather,
    generation map, system LMP, congestion metrics, curtailment metrics, and
    weather-zone load data.  No raw NetCDF reads are needed here.

    Parameters
    ----------
    months : list of (year, month) tuples

    Returns
    -------
    pd.DataFrame
    """
    dirs = setup_directories()
    lmp_dir = Path(dirs["processed"]) / "combined_hourly_gridded_data"

    dfs = []
    for year, month in months:
        lmp_path = lmp_dir / f"pixel_hourly_gfs+hrrr_{year}_{month:02d}.parquet"
        if not lmp_path.exists():
            print(f"  [WARNING] Missing pixel_hourly parquet: {lmp_path}")
            continue

        df_month = pd.read_parquet(lmp_path)
        df_month["valid_time"] = pd.to_datetime(df_month["valid_time"])
        if df_month["valid_time"].dt.tz is not None:
            df_month["valid_time"] = df_month["valid_time"].dt.tz_localize(None)

        if "is_weekend" not in df_month.columns:
            df_month["is_weekend"] = (df_month["weekday"] >= 5).astype(int)

        n_pixels = df_month["pixel_id"].nunique()
        n_hours = df_month["valid_time"].nunique()
        print(f"  Loaded {year}-{month:02d}: {len(df_month):,} rows "
              f"({n_pixels:,} Texas pixels × {n_hours} hours)")
        dfs.append(df_month)

    if not dfs:
        raise FileNotFoundError("No pixel_hourly parquet files found.")

    df = pd.concat(dfs, ignore_index=True)
    n_pixels = df["pixel_id"].nunique()
    print(f"\nLoaded {len(df):,} total rows across {n_pixels:,} Texas pixels.")
    return df


def run_pixel_regressions(df, depvar=DEPVAR, min_obs=100,
                          error_vars=None, controls=None, fe=None):
    """Run a per-pixel OLS regression and collect coefficient estimates.

    For each pixel, fits:
        {depvar} ~ error_vars + controls | fe

    Parameters
    ----------
    df : pd.DataFrame
        Full pixel × hour dataset with all required columns.
    depvar : str
        Dependent variable column name (default: DEPVAR).
    min_obs : int
        Minimum observations per pixel to run regression.
    error_vars : list of str, optional
        Treatment variables. Defaults to ERROR_VARS.
    controls : list of str, optional
        Control variables. Defaults to CONTROLS.
    fe : list of str, optional
        Fixed effects. Defaults to FE, but drops 'month' if only 1 month.

    Returns
    -------
    pd.DataFrame
        Columns: pixel_id, lat, lon, error_var, coef, std_err, pvalue, n_obs
    """
    if error_vars is None:
        error_vars = ERROR_VARS
    if controls is None:
        controls = CONTROLS
    if fe is None:
        fe = list(FE)

    # Filter error_vars and controls to only those present in data
    error_vars = [v for v in error_vars if v in df.columns]
    controls = [v for v in controls if v in df.columns]
    # throw a warning if there are any 
    fe = [v for v in fe if v in df.columns]

    # Drop 'month' FE if only 1 month of data (no variation)
    if "month" in fe and df["month"].nunique() <= 1:
        fe.remove("month")
        print("  (Dropped 'month' FE — single month of data)")

    rhs = " + ".join(error_vars + controls)
    if fe:
        fml = f"{depvar} ~ {rhs} | {' + '.join(fe)}"
    else:
        fml = f"{depvar} ~ {rhs}"

    # Build a lookup from pixel_id -> (lat, lon).
    coords = (
        df[["pixel_id", "latitude", "longitude"]]
        .dropna(subset=["latitude", "longitude"])
        .drop_duplicates("pixel_id")
        .set_index("pixel_id")
    )

    pixel_ids = df["pixel_id"].unique()
    n_pixels = len(pixel_ids)
    print(f"\nRunning regressions for {n_pixels:,} pixels...")
    print(f"  Formula: {fml}\n")

    required_cols = [depvar] + error_vars + controls + fe
    records = []
    for i, pid in enumerate(pixel_ids):
        if i % 500 == 0:
            print(f"  Progress: {i:,} / {n_pixels:,} pixels")

        pixel_df = df[df["pixel_id"] == pid].copy()

        # Drop rows missing any required variable
        pixel_df = pixel_df.dropna(subset=required_cols)

        if len(pixel_df) < min_obs:
            continue

        # Pre-cast numeric columns to float64 so pyfixest doesn't do it on a slice
        float_cols = [c for c in required_cols if c not in fe]
        pixel_df[float_cols] = pixel_df[float_cols].astype("float64")

        try:
            fit = pf.feols(fml, data=pixel_df)
            tidy_df = fit.tidy()
        except Exception:
            continue

        n_obs = int(fit._N)
        lat = coords.loc[pid, "latitude"]
        lon = coords.loc[pid, "longitude"]

        for err_var in error_vars:
            if err_var not in tidy_df.index:
                continue
            row = tidy_df.loc[err_var]
            records.append(
                {
                    "pixel_id": pid,
                    "lat": lat,
                    "lon": lon,
                    "error_var": err_var,
                    "coef": row["Estimate"],
                    "std_err": row["Std. Error"],
                    "pvalue": row["Pr(>|t|)"],
                    "n_obs": n_obs,
                }
            )

    print(f"  Done. {len(records):,} coefficient records collected.")
    return pd.DataFrame(records)


def _draw_texas_base(ax, proj):
    """Draw state fills (background only) — call before scatter.

    Parameters
    ----------
    ax : matplotlib Axes (cartopy GeoAxes)
    proj : cartopy CRS projection
    """
    ax.set_extent([-107.5, -93.0, 25.5, 37.0], crs=ccrs.PlateCarree())
    ax.set_facecolor("#cce5f0")

    shpfilename = shpreader.natural_earth(
        resolution="10m", category="cultural", name="admin_1_states_provinces"
    )
    reader = shpreader.Reader(shpfilename)
    states = list(reader.records())

    for state in states:
        if state.attributes.get("name") == "Texas":
            # Fill only — border drawn later via _draw_texas_borders so scatter
            # is never occluded by a filled polygon patch.
            ax.add_geometries(
                [state.geometry],
                crs=ccrs.PlateCarree(),
                facecolor="white",
                edgecolor="none",
                zorder=1,
            )
        elif state.attributes.get("admin") == "United States of America":
            ax.add_geometries(
                [state.geometry],
                crs=ccrs.PlateCarree(),
                facecolor="#f0f0f0",
                edgecolor="none",
                zorder=1,
            )


def _draw_texas_borders(ax):
    """Draw state borders on top of scatter — call after scatter.

    Parameters
    ----------
    ax : matplotlib GeoAxes
    """
    shpfilename = shpreader.natural_earth(
        resolution="10m", category="cultural", name="admin_1_states_provinces"
    )
    reader = shpreader.Reader(shpfilename)
    states = list(reader.records())

    for state in states:
        if state.attributes.get("name") == "Texas":
            ax.add_geometries(
                [state.geometry],
                crs=ccrs.PlateCarree(),
                facecolor="none",
                edgecolor="black",
                linewidth=0.8,
                zorder=5,
            )
        elif state.attributes.get("admin") == "United States of America":
            ax.add_geometries(
                [state.geometry],
                crs=ccrs.PlateCarree(),
                facecolor="none",
                edgecolor="#aaaaaa",
                linewidth=0.4,
                zorder=5,
            )


def plot_pixel_coefficient_map(
    results_df, error_var, title, ax, vmin, vmax, dirs=None, overlay=None, sig_level=0.05
):
    """Plot significant pixel coefficients for one error variable on a map.

    Parameters
    ----------
    results_df : pd.DataFrame
        Output from run_pixel_regressions().
    error_var : str
        Which error variable to plot.
    title : str
        Panel title.
    ax : matplotlib GeoAxes
    vmin, vmax : float
        Color scale limits.
    dirs : dict, optional
        Output of setup_directories(). Required when overlay is non-empty.
    overlay : list of str, optional
        Infrastructure overlays to draw: 'wind', 'solar', 'gas', 'transmission'.
    sig_level : float
        p-value threshold for significance.

    Returns
    -------
    (sc, legend_handles) : scatter artist and list of legend handles
    """
    proj = ccrs.PlateCarree()
    _draw_texas_base(ax, proj)

    all_var = results_df[results_df["error_var"] == error_var]
    sub = all_var[all_var["pvalue"] < sig_level].copy()

    print(
        f"  [{error_var}] significant: {len(sub):,} / {len(all_var):,}  "
        f"lat NaN: {sub['lat'].isna().sum()}, lon NaN: {sub['lon'].isna().sum()}"
    )

    sc = None
    if len(sub) > 0:
        sc = ax.scatter(
            sub["lon"],
            sub["lat"],
            c=sub["coef"],
            cmap="RdBu_r",
            vmin=vmin,
            vmax=vmax,
            s=18,
            marker="s",
            transform=proj,
            zorder=3,
            alpha=0.9,
        )

    # Overlays drawn on top of coefficient scatter, before borders
    legend_handles = []
    if overlay and dirs is not None:
        legend_handles = _draw_overlays(ax, dirs, overlay, proj)

    _draw_texas_borders(ax)

    n_sig = len(sub)
    n_total = len(all_var)
    ax.set_title(
        f"{title}\n({n_sig:,} / {n_total:,} pixels significant)",
        fontsize=11,
    )

    return sc, legend_handles


def run_pixel_regression_maps(months=None, save_dir=None, overlay=None,
                               depvar=None, tag=None, regime=None,
                               no_controls=False):
    """Main entry point: run pixel regressions and produce 2×2 map.

    Parameters
    ----------
    months : list of (year, month) tuples, optional
        Defaults to MONTHS (all of 2025).
    save_dir : str or Path, optional
        Directory for output files. Defaults to {figures}/pixel_regressions/.
    overlay : list of str, optional
        Infrastructure overlays drawn on each panel.
        Defaults to ['wind', 'solar', 'transmission', 'cities'].
    depvar : str, optional
        Dependent variable. Defaults to DEPVAR ('total_shadow_cost').
    tag : str, optional
        Suffix for output filenames (e.g., 'summer', 'system_lmp_max').
        Defaults to depvar (or depvar_regime when regime is set).
    regime : str, optional
        Restrict analysis to an extreme weather regime. One of:
        'extreme_cold', 'extreme_heat', 'high_wind', 'stressed_grid'.
        When set, uses min_obs=50 instead of 100 and appends regime label
        to the figure title.
    no_controls : bool, optional
        If True, drop all control variables and fixed effects and regress the
        outcome only on the forecast error variables.

    Returns
    -------
    dict with keys 'map' (Path) and 'table' (Path)
    """
    if months is None:
        months = DEFAULT_MONTHS
    if overlay is None:
        overlay = ['wind', 'solar', 'transmission', 'cities']
    if depvar is None:
        depvar = DEPVAR
    if regime is not None and regime not in REGIMES:
        raise ValueError(
            f"Unknown regime '{regime}'. Choose from: {list(REGIMES.keys())}"
        )
    if tag is None:
        tag = depvar if regime is None else f"{depvar}_{regime}"

    dirs = setup_directories()

    if save_dir is None:
        save_dir = Path(dirs["figures"]) / "pixel_regressions"
    else:
        save_dir = Path(save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)

    tables_dir = Path(dirs["tables"])
    tables_dir.mkdir(parents=True, exist_ok=True)

    # --- Load data ---
    df = load_pixel_data(months)

    # --- Apply regime filter ---
    if regime is not None:
        print(f"\nApplying regime filter: {REGIMES[regime]['label']}")
        df = add_regime_columns(df)
        df = filter_to_regime(df, regime)
        if df["valid_time"].nunique() < 20:
            raise ValueError(
                f"Too few hours ({df['valid_time'].nunique()}) for regime '{regime}'. "
                "Try a longer time window."
            )

    min_obs = 50 if regime is not None else 100
    fe = [] if no_controls else None # setting to none uses defaults

    # --- Run regressions ---
    # Use only error vars and controls that are actually present and have
    # sufficient non-NaN coverage. If a variable drops >80% of obs, skip it.
    avail_errors = []
    for v in ERROR_VARS:
        if v not in df.columns:
            continue
        frac_valid = df[v].notna().mean()
        if frac_valid < 0.2:
            print(f"  Dropping '{v}' from regression (only {frac_valid*100:.0f}% non-NaN)")
        else:
            avail_errors.append(v)
    if no_controls:
        avail_controls = []
        # update tag to reflect no controls
        tag += "_no_controls"
    else:
        avail_controls = [v for v in CONTROLS if v in df.columns and df[v].notna().mean() > 0.2]

    if not avail_errors:
        print("  WARNING: No error variables available for regression.")
        results_df = pd.DataFrame()
    else:
        results_df = run_pixel_regressions(
            df, depvar=depvar,
            error_vars=avail_errors,
            controls=avail_controls,
            fe=fe,
            min_obs=min_obs,
        )

    # --- Save regression results table ---
    table_path = tables_dir / f"pixel_regression_summary_{tag}.csv"
    results_df.to_csv(table_path, index=False)
    print(f"\nRegression results saved to: {table_path}")

    # --- Compute shared color limits ---
    sig_mask = results_df["pvalue"] < SIG_LEVEL
    print(f"\nTotal significant pixels across all error vars: {sig_mask.sum():,}")

    if sig_mask.sum() > 0:
        clim = np.nanpercentile(results_df.loc[sig_mask, "coef"].abs(), 99)
    else:
        clim = 1.0
    vmin, vmax = -clim, clim
    print(f"\nShared color limits: [{vmin:.4f}, {vmax:.4f}]")

    # --- Panel layout ---
    _all_panels = [
        ("temp_error_1h", "HRRR 1h — Temperature Error"),
        ("wspd_error_1h", "HRRR 1h — Wind Speed Error"),
        ("temp_error_0h", "GFS Day-Ahead — Temperature Error"),
        ("wspd_error_0h", "GFS Day-Ahead — Wind Speed Error"),
    ]
    # Only plot panels for error vars that were estimated
    estimated_vars = set(results_df["error_var"].unique()) if len(results_df) > 0 else set()
    panel_config = [(v, t) for v, t in _all_panels if v in estimated_vars]
    if not panel_config:
        # Fallback to weather-only panels
        panel_config = _all_panels[:4]

    n_panels = len(panel_config)
    n_cols = 2
    n_rows = (n_panels + n_cols - 1) // n_cols

    fig, axes = plt.subplots(
        n_rows,
        n_cols,
        figsize=(18, 7 * n_rows),
        subplot_kw={"projection": ccrs.PlateCarree()},
        gridspec_kw={"hspace": 0.15, "wspace": 0.05},
    )

    sc_last = None
    legend_handles = []
    for idx, (err_var, panel_title) in enumerate(panel_config):
        row, col = divmod(idx, 2)
        ax = axes[row, col]
        sc, handles = plot_pixel_coefficient_map(
            results_df,
            error_var=err_var,
            title=panel_title,
            ax=ax,
            vmin=vmin,
            vmax=vmax,
            dirs=dirs,
            overlay=overlay,
            sig_level=SIG_LEVEL,
        )
        if sc is not None:
            sc_last = sc
        if idx == 0:
            legend_handles = handles  # collect once to avoid duplication

    # --- Shared colorbar ---
    if sc_last is not None:
        fig.colorbar(
            sc_last,
            ax=axes.ravel().tolist(),
            shrink=0.6,
            label="Coefficient estimate ($/MWh per unit error)",
            pad=0.02,
        )

    # --- Shared legend ---
    if legend_handles:
        fig.legend(
            handles=legend_handles,
            loc='lower center',
            ncol=len(legend_handles),
            fontsize=9,
            framealpha=0.85,
            bbox_to_anchor=(0.45, 0.01),
        )

    # --- Figure title ---
    depvar_label = depvar.replace("_", " ")
    period_label = _period_label_from_months(months)
    title_main = f"Pixel-Level Regression Coefficients: Forecast Error \u2192 {depvar_label}"
    subtitle_parts = []
    if regime is not None:
        subtitle_parts.append(REGIMES[regime]["label"])
    if period_label is not None:
        subtitle_parts.append(period_label)
    if subtitle_parts:
        title_main = f"{title_main} [{', '.join(subtitle_parts)}]"

    fig.suptitle(
        f"{title_main}\n"
        f"(only significant pixels shown, p < 0.05; controls: "
        f"{'none' if no_controls else 'observed weather, weekend'}; "
        f"{'FE: hour-of-day, month' if not no_controls else 'FE: none'})",
        fontsize=13,
        y=0.98,
    )

    # --- Save figure ---
    save_path = save_dir / f"pixel_regression_2x2_{tag}.png"
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Map saved to: {save_path}")

    return {"map": save_path, "table": table_path}


if __name__ == "__main__":
    # ── Configure run here ────────────────────────────────────────────────────
    vars = ["economic_congestion_cost"]

    summer = [(2025, m) for m in [6, 7, 8]]
    winter = [(2025, m) for m in [12, 1, 2]]
    spring_fall = [(2025, m) for m in [3, 4, 5, 9, 10, 11]]
    full_year = [(2025, m) for m in range(1, 13)]

    month_sets = [
        ("full_year", full_year),
        ("summer", summer),
        ("winter", winter),
        ("spring_fall", spring_fall),
    ]

    for var in vars:
        for season_name, month_set in month_sets:
            tag = var if season_name == "full_year" else f"{var}_{season_name}"
            print(f"\n=== Running pixel regression maps for {tag} ===")
            run_pixel_regression_maps(
                months=month_set,
                depvar=var,
                tag=tag,
            )

