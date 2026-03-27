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
import xarray as xr
import pyfixest as pf

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories

ROOT = Path(__file__).resolve().parent.parent

DEPVAR = "system_lmp_std"
# All 4 error variables estimated jointly in one regression per pixel
ERROR_VARS = [
    "temp_error_1h", "wspd_error_1h", "temp_error_0h", "wspd_error_0h",
    "load_error_1h", "load_error_dam",
]
CONTROLS = ["era5_temp", "era5_wspd", "actual_load", "is_weekend"]
FE = ["hour_of_day", "month"]
SIG_LEVEL = 0.05

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


def _build_texas_mask(lats, lons):
    """Return (n_lat, n_lon) boolean mask — True for pixels inside Texas."""
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
    return shapely.within(points, texas_geom).reshape(len(lats), len(lons))


def load_pixel_data(months):
    """Load all ERA5 pixels inside Texas for the given months.

    Reads ERA5 error NetCDFs (HRRR lead=1h, GFS lead=0h) covering every
    ERA5 pixel in Texas, then merges system-level LMP and time features
    from the combined pixel_hourly parquets.

    Parameters
    ----------
    months : list of (year, month) tuples

    Returns
    -------
    pd.DataFrame
    """
    dirs = setup_directories()
    errors_dir = Path(dirs["processed"]) / "forecast_errors_era5"
    lmp_dir = Path(dirs["processed"]) / "combined_hourly_gridded_data"

    LMP_COLS = ["system_lmp_mean", "system_lmp_max", "system_lmp_std"]
    CONGESTION_COLS = [
        "n_binding_constraints", "total_shadow_cost", "max_shadow_price",
        "shadow_cost_weighted", "n_violations", "total_violated_mw",
        "mean_shadow_cost_per_interval",
    ]
    ALL_SYSTEM_COLS = LMP_COLS + CONGESTION_COLS

    texas_mask = None
    lats = lons = None
    tx_lat_idx = tx_lon_idx = None
    pixel_ids = None

    dfs = []
    for year, month in months:
        hrrr_path = (errors_dir / "hrrr" / str(year) / f"{month:02d}"
                     / f"era5_errors_{year}{month:02d}.nc")
        gfs_path  = (errors_dir / "gfs"  / str(year) / f"{month:02d}"
                     / f"era5_errors_{year}{month:02d}.nc")
        lmp_path  = lmp_dir / f"pixel_hourly_gfs+hrrr_{year}_{month:02d}.parquet"

        for label, p in [("HRRR errors", hrrr_path), ("GFS errors", gfs_path),
                         ("pixel_hourly", lmp_path)]:
            if not p.exists():
                print(f"  [WARNING] Missing {label}: {p}")

        if not hrrr_path.exists() or not gfs_path.exists() or not lmp_path.exists():
            continue

        ds_hrrr = xr.open_dataset(hrrr_path)
        ds_gfs  = xr.open_dataset(gfs_path)

        # Build Texas mask once from the first month's grid
        if lats is None:
            lats = ds_hrrr["latitude"].values
            lons = ds_hrrr["longitude"].values
            print("Building Texas pixel mask...")
            texas_mask = _build_texas_mask(lats, lons)
            tx_lat_idx, tx_lon_idx = np.where(texas_mask)
            lat_grid, lon_grid = np.meshgrid(lats, lons, indexing='ij')
            tx_lat = lat_grid[texas_mask]
            tx_lon = lon_grid[texas_mask]
            pixel_ids = [f"{la:.1f}_{lo:.1f}" for la, lo in zip(tx_lat, tx_lon)]

        hrrr_sel = ds_hrrr.sel(lead_hours=1)
        gfs_sel  = ds_gfs.sel(lead_hours=0)

        # Align on common valid_times (HRRR and GFS may differ)
        hrrr_times = pd.DatetimeIndex(hrrr_sel["valid_time"].values)
        gfs_times  = pd.DatetimeIndex(gfs_sel["valid_time"].values)
        common_times = hrrr_times.intersection(gfs_times)
        if len(common_times) == 0:
            print(f"  [WARNING] No overlapping times for {year}-{month:02d}, skipping")
            ds_hrrr.close()
            ds_gfs.close()
            continue

        hrrr_aligned = hrrr_sel.sel(valid_time=common_times.values)
        gfs_aligned  = gfs_sel.sel(valid_time=common_times.values)

        valid_times = common_times
        n_time    = len(valid_times)
        n_pixels  = len(tx_lat_idx)

        # Extract error/obs arrays: (T, lat, lon) → (T, n_pixels)
        def _tx(arr):
            return arr[:, tx_lat_idx, tx_lon_idx]

        temp_err_1h = _tx(hrrr_aligned["temp_error"].values)
        wspd_err_1h = _tx(hrrr_aligned["wspd_error"].values)
        era5_temp   = _tx(hrrr_aligned["era5_temp"].values)
        era5_wspd   = _tx(hrrr_aligned["era5_wspd"].values)
        temp_err_0h = _tx(gfs_aligned["temp_error"].values)
        wspd_err_0h = _tx(gfs_aligned["wspd_error"].values)

        ds_hrrr.close()
        ds_gfs.close()

        # Build long-form DataFrame: (T × n_pixels) rows
        df_month = pd.DataFrame({
            "valid_time":    np.repeat(valid_times, n_pixels),
            "latitude":      np.tile(lat_grid[texas_mask], n_time),
            "longitude":     np.tile(lon_grid[texas_mask], n_time),
            "pixel_id":      np.tile(pixel_ids, n_time),
            "temp_error_1h": temp_err_1h.ravel(),
            "wspd_error_1h": wspd_err_1h.ravel(),
            "era5_temp":     era5_temp.ravel(),
            "era5_wspd":     era5_wspd.ravel(),
            "temp_error_0h": temp_err_0h.ravel(),
            "wspd_error_0h": wspd_err_0h.ravel(),
        })

        # Load system-level LMP + congestion from parquet (one value per hour)
        # Read schema to discover available columns (congestion may not exist)
        import pyarrow.parquet as pq_schema
        pq_all_cols = pq_schema.read_schema(lmp_path).names
        cols_to_load = ["valid_time"] + [
            c for c in ALL_SYSTEM_COLS if c in pq_all_cols
        ]
        lmp_df = pd.read_parquet(lmp_path, columns=cols_to_load)
        lmp_df["valid_time"] = pd.to_datetime(lmp_df["valid_time"])
        if lmp_df["valid_time"].dt.tz is not None:
            lmp_df["valid_time"] = lmp_df["valid_time"].dt.tz_localize(None)
        lmp_df = lmp_df.drop_duplicates("valid_time").set_index("valid_time")

        df_month["valid_time"] = pd.to_datetime(df_month["valid_time"])
        if df_month["valid_time"].dt.tz is not None:
            df_month["valid_time"] = df_month["valid_time"].dt.tz_localize(None)

        for col in lmp_df.columns:
            df_month[col] = df_month["valid_time"].map(lmp_df[col])

        # Derive time features from valid_time
        df_month["hour_of_day"] = df_month["valid_time"].dt.hour
        df_month["month"] = df_month["valid_time"].dt.month
        df_month["weekday"] = df_month["valid_time"].dt.weekday
        df_month["is_weekend"] = (df_month["weekday"] >= 5).astype(int)

        print(f"  Loaded {year}-{month:02d}: {len(df_month):,} rows "
              f"({n_pixels:,} Texas pixels × {n_time} hours)")
        dfs.append(df_month)

    if not dfs:
        raise FileNotFoundError("No ERA5 error files found.")

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
                               depvar=None, tag=None):
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
        Dependent variable. Defaults to DEPVAR ('system_lmp_std').
    tag : str, optional
        Suffix for output filenames (e.g., 'summer', 'system_lmp_max').
        Defaults to depvar if not provided.

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
    if tag is None:
        tag = depvar

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

    # --- Merge load forecast errors ---
    from process_data.calculate_load_error import merge_load_by_weather_zone
    print("\nMerging weather-zone load forecasts/errors...")
    df = merge_load_by_weather_zone(df, months)

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
    avail_controls = [v for v in CONTROLS if v in df.columns and df[v].notna().mean() > 0.2]

    if not avail_errors:
        print("  WARNING: No error variables available for regression.")
        results_df = pd.DataFrame()
    else:
        results_df = run_pixel_regressions(
            df, depvar=depvar,
            error_vars=avail_errors,
            controls=avail_controls,
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
        ("load_error_1h", "1h-Ahead — Load Forecast Error"),
        ("load_error_dam", "DAM (10am CT) — Load Forecast Error"),
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
    if period_label is not None:
        title_main = f"{title_main} [{period_label}]"

    fig.suptitle(
        f"{title_main}\n"
        "(only significant pixels shown, p < 0.05; "
        "controls: observed weather, weekend FE; FE: hour-of-day, month)",
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
    # Dependent variable: "system_lmp_std" or "system_lmp_max"

    # vars = ["system_lmp_std", "system_lmp_max"]
    vars = ["system_lmp_max"]

    # Months to include — change to a seasonal subset or any custom list, e.g.:
    summer = [(2025, m) for m in [6, 7, 8]]
    winter = [(2025, m) for m in [12, 1, 2]]
    spring_fall = [(2025, m) for m in [3, 4, 5, 9, 10, 11]]
    full_year = [(2025, m) for m in range(1, 13)]

    month_sets = [
        ("full_year", full_year),
        ("summer", summer),
        ("winter", winter),
        ("spring_fall", spring_fall)
    ]

    # RUN_DEPVAR = "system_lmp_std"
    # var = RUN_DEPVAR
    # month_set = full_year
    # tag = var
    # print(f"\n=== Running pixel regression maps for {tag} ===")
    # run_pixel_regression_maps(
    #     months=month_set,
    #     depvar=var,
    #     tag=tag,
    # )

    for var in vars:
        for season_name, month_set in month_sets:
            tag = var if season_name == "full_year" else f"{var}_{season_name}"
            print(f"\n=== Running pixel regression maps for {tag} ===")
            run_pixel_regression_maps(
                months=month_set,
                depvar=var,
                tag=tag,
            )

