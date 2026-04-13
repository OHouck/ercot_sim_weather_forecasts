"""
EDA Plots for ERCOT Forecast Error Analysis
============================================

Functions for exploratory data analysis plots used in check-in notes and reports.

Current plots
-------------
plot_shadow_cost_distribution        — histogram of hourly SCED shadow costs
plot_forecast_error_distributions    — overlaid PDFs of HRRR 1h and GFS day-ahead
                                       temperature and wind-speed errors
plot_eda_combined                    — single 3-panel figure combining both plots
plot_forecast_error_correlation_grid — 4×4 pairplot of cross-model error correlations
plot_mean_forecast_error_maps        — 2×2 cartopy maps of per-pixel mean error

Usage
-----
    uv run python -m analysis.eda_plots
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import gaussian_kde
import xarray as xr
import cartopy.crs as ccrs
import cartopy.io.shapereader as shpreader

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories
from process_data.process_congestion import build_shadow_station_match_tables

DEFAULT_MONTHS = [(2025, m) for m in range(1, 13)]

C_HRRR = "#1f77b4"   # blue  — HRRR 1 h
C_GFS  = "#d62728"   # red   — GFS day-ahead
C_HIST = "#4878d0"   # blue  — shadow cost histogram

# Four error variables used across correlation and map plots
_ERROR_VARS = [
    ("temp_error_1h", "HRRR 1h\nTemp Error (°C)"),
    ("wspd_error_1h", "HRRR 1h\nWind Error (m/s)"),
    ("temp_error_0h", "GFS Day-Ahead\nTemp Error (°C)"),
    ("wspd_error_0h", "GFS Day-Ahead\nWind Error (m/s)"),
]


# =============================================================================
# ── Private helpers ───────────────────────────────────────────────────────────
# =============================================================================

def _build_texas_mask(lats, lons):
    """Return (n_lat, n_lon) boolean array — True for pixels inside Texas."""
    import shapely.geometry as sg
    states_shp = shpreader.natural_earth(
        resolution="10m", category="cultural", name="admin_1_states_provinces"
    )
    texas_geom = None
    for record in shpreader.Reader(states_shp).records():
        if record.attributes.get("name") == "Texas":
            texas_geom = record.geometry
            break
    if texas_geom is None:
        raise RuntimeError("Texas geometry not found in Natural Earth shapefile.")
    mask = np.zeros((len(lats), len(lons)), dtype=bool)
    for i, lat in enumerate(lats):
        for j, lon in enumerate(lons):
            mask[i, j] = texas_geom.contains(sg.Point(lon, lat))
    return mask


def _load_hourly_shadow_costs(months, dirs):
    """Load system-level economic_congestion_cost (one value per hour) for given months.

    Returns a 1-D numpy array of non-NaN congestion cost values [$/h].
    """
    lmp_dir = Path(dirs["processed"]) / "combined_hourly_gridded_data"
    values = []
    for year, month in months:
        path = lmp_dir / f"pixel_hourly_gfs+hrrr_{year}_{month:02d}.parquet"
        if not path.exists():
            print(f"  [WARNING] Missing parquet: {path.name}")
            continue
        df = pd.read_parquet(path, columns=["valid_time", "economic_congestion_cost"])
        df["valid_time"] = pd.to_datetime(df["valid_time"])
        df = df.drop_duplicates("valid_time")
        arr = df["economic_congestion_cost"].dropna().values
        values.append(arr)
        print(f"  Loaded congestion costs {year}-{month:02d}: {len(arr):,} hours")
    if not values:
        raise RuntimeError("No congestion cost data loaded.")
    return np.concatenate(values)


def _load_era5_errors(months, dirs, model, lead):
    """Load temp_error and wspd_error arrays from ERA5 error NetCDFs.

    Returns dict with keys 'temp' and 'wspd', each a 1-D numpy array of
    values for all Texas pixels × all hours in the given months.
    """
    errors_dir = Path(dirs["processed"]) / "forecast_errors_era5" / model
    texas_mask = None
    tx_lat_idx = tx_lon_idx = None
    temp_chunks, wspd_chunks = [], []

    for year, month in months:
        path = errors_dir / str(year) / f"{month:02d}" / f"era5_errors_{year}{month:02d}.nc"
        if not path.exists():
            print(f"  [WARNING] Missing ERA5 errors: {path.name}")
            continue
        ds = xr.open_dataset(path)
        if texas_mask is None:
            lats = ds["latitude"].values
            lons = ds["longitude"].values
            print("  Building Texas pixel mask (first month)...")
            texas_mask = _build_texas_mask(lats, lons)
            tx_lat_idx, tx_lon_idx = np.where(texas_mask)
        ds_lead = ds.sel(lead_hours=lead)
        temp_arr = ds_lead["temp_error"].values   # (T, lat, lon)
        wspd_arr = ds_lead["wspd_error"].values
        temp_tx = temp_arr[:, tx_lat_idx, tx_lon_idx].ravel()
        wspd_tx = wspd_arr[:, tx_lat_idx, tx_lon_idx].ravel()
        temp_chunks.append(temp_tx[~np.isnan(temp_tx)])
        wspd_chunks.append(wspd_tx[~np.isnan(wspd_tx)])
        ds.close()
        print(f"  Loaded {model} lead={lead}h errors {year}-{month:02d}: "
              f"{len(temp_chunks[-1]):,} values")

    if not temp_chunks:
        raise RuntimeError(f"No ERA5 error data loaded for {model} lead={lead}.")
    return {"temp": np.concatenate(temp_chunks), "wspd": np.concatenate(wspd_chunks)}


def _load_pixel_hourly_errors(months, dirs):
    """Load error columns from pixel_hourly parquet files for given months.

    Returns a DataFrame with columns: latitude, longitude, and the four error
    variables defined in _ERROR_VARS.
    """
    lmp_dir = Path(dirs["processed"]) / "combined_hourly_gridded_data"
    error_cols = ["latitude", "longitude"] + [col for col, _ in _ERROR_VARS]
    chunks = []
    for year, month in months:
        path = lmp_dir / f"pixel_hourly_gfs+hrrr_{year}_{month:02d}.parquet"
        if not path.exists():
            print(f"  [WARNING] Missing parquet: {path.name}")
            continue
        df = pd.read_parquet(path, columns=error_cols)
        chunks.append(df)
        print(f"  Loaded {year}-{month:02d}: {len(df):,} rows")
    if not chunks:
        raise RuntimeError("No pixel hourly data loaded.")
    return pd.concat(chunks, ignore_index=True)


# =============================================================================
# ── Panel-drawing helpers ─────────────────────────────────────────────────────
# =============================================================================

def _draw_shadow_cost_panel(ax, costs):
    """Draw the economic congestion cost histogram onto ax (log-scale x-axis)."""
    log_costs = np.log10(costs + 1)
    bins = np.linspace(0, np.percentile(log_costs, 99.9), 80)

    ax.hist(
        log_costs, bins=bins, density=True,
        color=C_HIST, alpha=0.70, edgecolor="white", linewidth=0.3,
    )

    tick_vals = [0, 1_000, 10_000, 100_000, 500_000, 1_000_000, 2_500_000]
    ax.set_xticks([np.log10(v + 1) for v in tick_vals])
    ax.set_xticklabels(
        [f"\${v:,}" if v > 0 else "\$0" for v in tick_vals],
        fontsize=7.5, rotation=30, ha="right",
    )
    ax.set_xlabel("Economic congestion cost ($/h, log scale)", fontsize=9)
    ax.set_ylabel("Density", fontsize=9)
    ax.set_title("Economic Congestion Cost Distribution", fontsize=9, fontweight="bold")
    ax.grid(axis="y", alpha=0.25, linewidth=0.5)


def _load_hourly_curtailment(months, dirs):
    """Load system-level wind and solar curtailment (one value per hour) for given months.

    Returns dict with keys 'wind' and 'solar', each a 1-D numpy array [MW].
    """
    lmp_dir = Path(dirs["processed"]) / "combined_hourly_gridded_data"
    wind_chunks, solar_chunks = [], []
    for year, month in months:
        path = lmp_dir / f"pixel_hourly_gfs+hrrr_{year}_{month:02d}.parquet"
        if not path.exists():
            print(f"  [WARNING] Missing parquet: {path.name}")
            continue
        df = pd.read_parquet(
            path, columns=["valid_time", "wind_curtailment_mw", "solar_curtailment_mw"]
        )
        df["valid_time"] = pd.to_datetime(df["valid_time"])
        df = df.drop_duplicates("valid_time")
        wind_chunks.append(df["wind_curtailment_mw"].dropna().values)
        solar_chunks.append(df["solar_curtailment_mw"].dropna().values)
        print(f"  Loaded curtailment {year}-{month:02d}: {len(df):,} hours")
    if not wind_chunks:
        raise RuntimeError("No curtailment data loaded.")
    return {
        "wind": np.concatenate(wind_chunks),
        "solar": np.concatenate(solar_chunks),
    }


def _draw_curtailment_panel(ax, wind_vals, solar_vals):
    """Draw overlaid wind / solar curtailment histograms + KDE onto ax."""
    C_WIND  = "#1f77b4"   # blue
    C_SOLAR = "#ff7f0e"   # orange

    xlim = (0, max(np.percentile(wind_vals, 99.5), np.percentile(solar_vals, 99.5)))
    bin_width = xlim[1] / 60
    bins = np.arange(0, xlim[1] + bin_width, bin_width)

    for vals, color, label in [
        (wind_vals,  C_WIND,  "Wind curtailment"),
        (solar_vals, C_SOLAR, "Solar curtailment"),
    ]:
        ax.hist(vals, bins=bins, density=True,
                color=color, alpha=0.25, edgecolor="none")
        kde = gaussian_kde(vals, bw_method=0.12)
        x_grid = np.linspace(0, xlim[1], 600)
        ax.plot(x_grid, kde(x_grid), color=color, linewidth=2.0, label=label)
        mean_val = np.mean(vals)
        ax.axvline(mean_val, color=color, linestyle="--", linewidth=1.2, alpha=0.85)

    ax.set_xlim(0, xlim[1])
    ax.set_xlabel("Curtailment (MW)", fontsize=9)
    ax.set_ylabel("Density", fontsize=9)
    ax.set_title("Renewable Curtailment — Wind vs Solar", fontsize=9, fontweight="bold")
    ax.legend(fontsize=8, loc="upper right")
    ax.grid(axis="y", alpha=0.25, linewidth=0.5)


def _draw_error_panel(ax, hrrr_vals, gfs_vals, xlabel, xlim, bin_width):
    """Draw overlaid error histograms + KDE with mean annotations onto ax."""
    rng = np.random.default_rng(42)
    def _sub(arr, n=500_000):
        return rng.choice(arr, size=n, replace=False) if len(arr) > n else arr

    bins = np.arange(xlim[0], xlim[1] + bin_width, bin_width)

    for vals_full, color, label in [
        (hrrr_vals, C_HRRR, "HRRR 1h"),
        (gfs_vals,  C_GFS,  "GFS day-ahead"),
    ]:
        vals = _sub(vals_full)
        ax.hist(vals, bins=bins, density=True,
                color=color, alpha=0.25, edgecolor="none")
        kde = gaussian_kde(vals, bw_method=0.15)
        x_grid = np.linspace(xlim[0], xlim[1], 800)
        ax.plot(x_grid, kde(x_grid), color=color, linewidth=2.0, label=label)

        # Mean vertical line + label
        mean_val = np.mean(vals_full)
        ymax = ax.get_ylim()[1]
        ax.axvline(mean_val, color=color, linestyle="--", linewidth=1.2, alpha=0.85)

    ax.axvline(0, color="black", linewidth=0.8, linestyle="-", alpha=0.4)
    ax.set_xlim(xlim)
    ax.set_xlabel(xlabel, fontsize=9)
    ax.set_ylabel("Density", fontsize=9)
    ax.legend(fontsize=8, loc="upper right")
    ax.grid(axis="y", alpha=0.25, linewidth=0.5)


# =============================================================================
# ── Public plot functions ─────────────────────────────────────────────────────
# =============================================================================

def plot_shadow_cost_distribution(months=None, save_dir=None):
    """Single-panel histogram of hourly SCED shadow costs (log-scale x-axis)."""
    if months is None:
        months = DEFAULT_MONTHS
    dirs = setup_directories()
    if save_dir is None:
        save_dir = Path(dirs["figures"]) / "eda"
    Path(save_dir).mkdir(parents=True, exist_ok=True)

    print("Loading hourly shadow costs...")
    costs = _load_hourly_shadow_costs(months, dirs)

    fig, ax = plt.subplots(figsize=(6, 4))
    _draw_shadow_cost_panel(ax, costs)
    fig.tight_layout()
    out_path = Path(save_dir) / "shadow_cost_distribution.png"
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {out_path}")
    return out_path


def plot_forecast_error_distributions(months=None, save_dir=None):
    """1×2 figure of overlaid HRRR 1h / GFS day-ahead error PDFs."""
    if months is None:
        months = DEFAULT_MONTHS
    dirs = setup_directories()
    if save_dir is None:
        save_dir = Path(dirs["figures"]) / "eda"
    Path(save_dir).mkdir(parents=True, exist_ok=True)

    print("Loading HRRR 1h errors...")
    hrrr = _load_era5_errors(months, dirs, model="hrrr", lead=1)
    print("Loading GFS day-ahead errors...")
    gfs  = _load_era5_errors(months, dirs, model="gfs",  lead=0)

    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    fig.suptitle("Forecast Error Distributions — ERA5 Grid, Texas (2025)",
                 fontsize=11, fontweight="bold")

    _draw_error_panel(axes[0], hrrr["temp"], gfs["temp"],
                      xlabel="Temperature error (°C)  [forecast − ERA5]",
                      xlim=(-12, 12), bin_width=0.25)
    axes[0].set_title("Temperature Errors — HRRR 1h vs GFS Day-Ahead",
                      fontsize=9, fontweight="bold")

    _draw_error_panel(axes[1], hrrr["wspd"], gfs["wspd"],
                      xlabel="Wind speed error (m/s)  [forecast − ERA5]",
                      xlim=(-10, 10), bin_width=0.20)
    axes[1].set_title("Wind Speed Errors — HRRR 1h vs GFS Day-Ahead",
                      fontsize=9, fontweight="bold")

    fig.tight_layout()
    out_path = Path(save_dir) / "forecast_error_distributions.png"
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {out_path}")
    return out_path


def plot_eda_combined(months=None, save_dir=None):
    """2×2 figure: congestion cost | curtailment | temp errors | wind errors.

    Intended for use in check-in notes where all key outcome and error
    distributions share one figure block.
    """
    if months is None:
        months = DEFAULT_MONTHS
    dirs = setup_directories()
    if save_dir is None:
        save_dir = Path(dirs["figures"]) / "eda"
    Path(save_dir).mkdir(parents=True, exist_ok=True)

    print("Loading economic congestion costs...")
    costs = _load_hourly_shadow_costs(months, dirs)
    print("Loading renewable curtailment...")
    curtailment = _load_hourly_curtailment(months, dirs)
    print("Loading HRRR 1h errors...")
    hrrr = _load_era5_errors(months, dirs, model="hrrr", lead=1)
    print("Loading GFS day-ahead errors...")
    gfs  = _load_era5_errors(months, dirs, model="gfs",  lead=0)

    fig, axes = plt.subplots(2, 2, figsize=(14, 9))
    fig.suptitle(
        "Summary Statistics — Congestion, Curtailment, and Forecast Errors, ERCOT 2025",
        fontsize=11, fontweight="bold",
    )

    _draw_shadow_cost_panel(axes[0, 0], costs)

    _draw_curtailment_panel(axes[0, 1], curtailment["wind"], curtailment["solar"])

    _draw_error_panel(axes[1, 0], hrrr["temp"], gfs["temp"],
                      xlabel="Temperature error (°C)  [forecast − ERA5]",
                      xlim=(-12, 12), bin_width=0.25)
    axes[1, 0].set_title("Temperature Errors — HRRR 1h vs GFS Day-Ahead",
                          fontsize=9, fontweight="bold")

    _draw_error_panel(axes[1, 1], hrrr["wspd"], gfs["wspd"],
                      xlabel="Wind speed error (m/s)  [forecast − ERA5]",
                      xlim=(-10, 10), bin_width=0.20)
    axes[1, 1].set_title("Wind Speed Errors — HRRR 1h vs GFS Day-Ahead",
                          fontsize=9, fontweight="bold")

    fig.tight_layout()
    out_path = Path(save_dir) / "eda_summary.png"
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {out_path}")
    return out_path


def plot_shadow_station_geolocation(months=None, save_dir=None):
    """Map geolocated shadow substations and export yearly aggregate match CSVs.

    Processes each month to generate maps, but consolidates matched/unmatched
    substations into single yearly CSVs. The yearly CSVs represent the UNION of
    all unique stations seen across all months in that year (deduplicated by
    station_name). Note: the set of active transmission substations varies
    significantly by month (range: 227–372 substations in 2025), so the yearly
    CSV contains all stations ever geolocated in that year.

    Outputs:
    - shadow_station_matches_{YYYY}.csv (yearly union, deduplicated by station_name)
    - shadow_station_unmatched_{YYYY}.csv (yearly union, deduplicated by station_name)
    - shadow_substation_geolocation_{YYYYMM}.png (monthly individual maps)
    - shadow_substation_geolocation_{YYYY}_all_months.png (yearly consolidated map)

    Args:
        months: List of (year, month) tuples. Defaults to DEFAULT_MONTHS.
        save_dir: Optional figure output directory.

    Returns:
        Dict with yearly CSV paths and list of monthly figure paths.
    """
    if months is None:
        months = DEFAULT_MONTHS
    dirs = setup_directories()
    if save_dir is None:
        save_dir = Path(dirs["figures"]) / "eda"
    save_dir = Path(save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)

    # Group months by year and collect all matched/unmatched per year
    yearly_matched = {}  # year -> list of DataFrames
    yearly_unmatched = {}  # year -> list of DataFrames
    monthly_figures = []

    matched_cols = [
        "station_name",
        "cleaned_name",
        "matched_source_name",
        "source",
        "match_method",
        "latitude",
        "longitude",
    ]
    unmatched_cols = ["station_name", "cleaned_name"]

    for year, month in months:
        if year not in yearly_matched:
            yearly_matched[year] = []
            yearly_unmatched[year] = []

        matched, unmatched = build_shadow_station_match_tables(year=year, month=month)

        matched = matched[[c for c in matched_cols if c in matched.columns]]
        unmatched = unmatched[[c for c in unmatched_cols if c in unmatched.columns]]

        yearly_matched[year].append(matched)
        yearly_unmatched[year].append(unmatched)

        # Generate monthly map figure
        fig = plt.figure(figsize=(9, 7))
        ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())

        states_shp = shpreader.natural_earth(
            resolution="10m",
            category="cultural",
            name="admin_1_states_provinces",
        )
        texas_geom = None
        for record in shpreader.Reader(states_shp).records():
            if record.attributes.get("name") == "Texas":
                texas_geom = record.geometry
                break
        if texas_geom is None:
            raise RuntimeError("Texas geometry not found in Natural Earth shapefile.")

        ax.add_geometries(
            [texas_geom],
            crs=ccrs.PlateCarree(),
            facecolor="#f8f8f8",
            edgecolor="black",
            linewidth=1.0,
            zorder=1,
        )

        if not matched.empty:
            ax.scatter(
                matched["longitude"],
                matched["latitude"],
                s=16,
                color="#1f77b4",
                alpha=0.8,
                transform=ccrs.PlateCarree(),
                zorder=3,
                label="Matched substations",
            )

        ax.set_extent([-106.8, -93.0, 25.5, 36.8], crs=ccrs.PlateCarree())
        ax.gridlines(draw_labels=True, linewidth=0.4, color="gray", alpha=0.4)

        n_matched = int(len(matched))
        n_unmatched = int(len(unmatched))
        ax.set_title(
            f"Shadow Substation Geolocation ({year}-{month:02d})\n"
            f"Matched: {n_matched} | Unmatched: {n_unmatched}",
            fontsize=11,
            fontweight="bold",
        )
        ax.legend(loc="lower left", frameon=True)

        out_path = save_dir / f"shadow_substation_geolocation_{year}{month:02d}.png"
        fig.savefig(out_path, dpi=150, bbox_inches="tight")
        plt.close(fig)

        print(f"Saved: {out_path}")
        monthly_figures.append(out_path)

    # Consolidate yearly CSVs and generate consolidated figures
    # NOTE: The set of active substations varies by month (227–372 in 2025).
    # The yearly CSV contains the UNION of all unique stations ever geolocated
    # that year, deduplicated by station_name.
    yearly_csv_paths = {}
    yearly_figures = {}
    for year in yearly_matched:
        # Combine all months for the year and deduplicate by station_name.
        # This represents all transmission substations that appeared in shadow
        # pricing data at any point during the year.
        matched_combined = pd.concat(yearly_matched[year], ignore_index=True)
        matched_combined = matched_combined.drop_duplicates(subset=["station_name"])

        unmatched_combined = pd.concat(yearly_unmatched[year], ignore_index=True)
        unmatched_combined = unmatched_combined.drop_duplicates(subset=["station_name"])

        matched_out = Path(dirs["processed"]) / f"shadow_station_matches_{year}.csv"
        unmatched_out = Path(dirs["processed"]) / f"shadow_station_unmatched_{year}.csv"

        matched_combined.to_csv(matched_out, index=False)
        unmatched_combined.to_csv(unmatched_out, index=False)

        yearly_csv_paths[year] = {
            "matched_csv": matched_out,
            "unmatched_csv": unmatched_out,
        }

        print(f"Saved: {matched_out}")
        print(f"Saved: {unmatched_out}")

        # Generate consolidated yearly map
        fig = plt.figure(figsize=(9, 7))
        ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())

        states_shp = shpreader.natural_earth(
            resolution="10m",
            category="cultural",
            name="admin_1_states_provinces",
        )
        texas_geom = None
        for record in shpreader.Reader(states_shp).records():
            if record.attributes.get("name") == "Texas":
                texas_geom = record.geometry
                break
        if texas_geom is None:
            raise RuntimeError("Texas geometry not found in Natural Earth shapefile.")

        ax.add_geometries(
            [texas_geom],
            crs=ccrs.PlateCarree(),
            facecolor="#f8f8f8",
            edgecolor="black",
            linewidth=1.0,
            zorder=1,
        )

        if not matched_combined.empty:
            ax.scatter(
                matched_combined["longitude"],
                matched_combined["latitude"],
                s=16,
                color="#1f77b4",
                alpha=0.8,
                transform=ccrs.PlateCarree(),
                zorder=3,
                label="Matched substations",
            )

        ax.set_extent([-106.8, -93.0, 25.5, 36.8], crs=ccrs.PlateCarree())
        ax.gridlines(draw_labels=True, linewidth=0.4, color="gray", alpha=0.4)

        n_matched = int(len(matched_combined))
        n_unmatched = int(len(unmatched_combined))
        ax.set_title(
            f"Shadow Substation Geolocation ({year}) — All Months Combined\n"
            f"Matched: {n_matched} | Unmatched: {n_unmatched}",
            fontsize=11,
            fontweight="bold",
        )
        ax.legend(loc="lower left", frameon=True)

        out_path = save_dir / f"shadow_substation_geolocation_{year}.png"
        fig.savefig(out_path, dpi=150, bbox_inches="tight")
        plt.close(fig)

        print(f"Saved: {out_path}")
        yearly_figures[year] = out_path

    return {
        "yearly_csvs": yearly_csv_paths,
        "yearly_figures": yearly_figures,
        "monthly_figures": monthly_figures,
    }


def plot_forecast_error_correlation_grid(months=None, save_dir=None, n_sample=200_000):
    """4×4 pairplot of cross-model forecast error correlations.

    Layout:
      Diagonal     — histogram + KDE of each error variable
      Upper triangle — hexbin scatter with Pearson r annotated
      Lower triangle — large Pearson r text on a colour-coded background

    Variables: HRRR 1h temp, HRRR 1h wind, GFS day-ahead temp, GFS day-ahead wind.
    Correlations are computed on the full dataset; hexbins use a subsample of
    up to n_sample rows.
    """
    if months is None:
        months = DEFAULT_MONTHS
    dirs = setup_directories()
    if save_dir is None:
        save_dir = Path(dirs["figures"]) / "eda"
    Path(save_dir).mkdir(parents=True, exist_ok=True)

    print("Loading pixel hourly error data for correlation grid...")
    df_raw = _load_pixel_hourly_errors(months, dirs)

    var_cols   = [col for col, _ in _ERROR_VARS]
    var_labels = [lbl for _, lbl in _ERROR_VARS]
    n_vars = len(var_cols)

    df_clean = df_raw[var_cols].dropna()
    print(f"  Rows after dropna: {len(df_clean):,}")

    # Pearson correlations on full data
    corr = df_clean.corr()

    # Subsample for scatter/hexbin
    rng = np.random.default_rng(42)
    n_plot = min(n_sample, len(df_clean))
    idx = rng.choice(len(df_clean), size=n_plot, replace=False)
    df_plot = df_clean.iloc[idx].reset_index(drop=True)

    fig, axes = plt.subplots(n_vars, n_vars, figsize=(11, 11))
    fig.suptitle(
        "Forecast Error Pairwise Correlations — ERA5 Grid, Texas (2025)",
        fontsize=12, fontweight="bold", y=0.99,
    )

    for i in range(n_vars):
        for j in range(n_vars):
            ax = axes[i, j]
            col_i, col_j = var_cols[i], var_cols[j]

            if i == j:
                # Diagonal: histogram + KDE
                vals = df_plot[col_i].values
                ax.hist(vals, bins=70, density=True,
                        color=C_HIST, alpha=0.45, edgecolor="none")
                kde = gaussian_kde(vals, bw_method=0.12)
                xg = np.linspace(vals.min(), vals.max(), 400)
                ax.plot(xg, kde(xg), color=C_HIST, linewidth=1.8)
                ax.set_yticks([])

            elif i < j:
                # Upper triangle: hexbin with r annotated
                ax.hexbin(
                    df_plot[col_j].values,
                    df_plot[col_i].values,
                    gridsize=35,
                    cmap="Blues",
                    mincnt=1,
                    linewidths=0.0,
                )
                r = corr.loc[col_i, col_j]
                color = "darkred" if abs(r) > 0.3 else "#333333"
                ax.text(0.05, 0.93, f"r = {r:.3f}",
                        transform=ax.transAxes, fontsize=9,
                        va="top", fontweight="bold", color=color)

            else:
                # Lower triangle: large r text on tinted background
                r = corr.loc[col_i, col_j]
                # Map r ∈ [−1, 1] to a diverging colour: negative=blue, positive=red
                bg_rgba = plt.cm.RdBu_r(0.5 + r * 0.45)
                ax.set_facecolor((*bg_rgba[:3], 0.30))
                ax.text(0.5, 0.5, f"{r:.3f}",
                        transform=ax.transAxes, fontsize=17,
                        ha="center", va="center", fontweight="bold",
                        color="darkred" if r > 0.3 else ("#1a4a8a" if r < -0.3 else "#333333"))
                ax.set_xticks([])
                ax.set_yticks([])

            # Edge labels
            if j == 0:
                ax.set_ylabel(var_labels[i], fontsize=8, labelpad=3)
            if i == n_vars - 1:
                ax.set_xlabel(var_labels[j], fontsize=8, labelpad=3)

            # Suppress inner tick labels
            if i < n_vars - 1 and i != j:
                ax.tick_params(labelbottom=False)
            if j > 0 and i != j:
                ax.tick_params(labelleft=False)
            if i == j:
                ax.tick_params(labelleft=False)

    fig.tight_layout(rect=[0, 0, 1, 0.98])
    out_path = Path(save_dir) / "forecast_error_correlation_grid.png"
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {out_path}")
    return out_path


def plot_mean_forecast_error_maps(months=None, save_dir=None):
    """2×2 cartopy maps showing per-pixel mean forecast error across all hours.

    Each panel covers one error variable (HRRR 1h temp, HRRR 1h wind,
    GFS day-ahead temp, GFS day-ahead wind).  Diverging RdBu_r colormap
    centred at zero; colour limits set symmetrically at the 98th percentile
    of |error|.
    """
    if months is None:
        months = DEFAULT_MONTHS
    dirs = setup_directories()
    if save_dir is None:
        save_dir = Path(dirs["figures"]) / "eda"
    Path(save_dir).mkdir(parents=True, exist_ok=True)

    print("Loading pixel hourly error data for maps...")
    df = _load_pixel_hourly_errors(months, dirs)

    var_cols = [col for col, _ in _ERROR_VARS]
    pixel_mean = (
        df.groupby(["latitude", "longitude"])[var_cols]
        .mean()
        .reset_index()
    )
    print(f"  Unique pixels: {len(pixel_mean):,}")

    # Load Texas geometry once
    states_shp = shpreader.natural_earth(
        resolution="10m", category="cultural", name="admin_1_states_provinces"
    )
    texas_geom = None
    for record in shpreader.Reader(states_shp).records():
        if record.attributes.get("name") == "Texas":
            texas_geom = record.geometry
            break
    if texas_geom is None:
        raise RuntimeError("Texas geometry not found in Natural Earth shapefile.")

    map_titles = [
        "HRRR 1h Mean Temp Error (°C)\n[forecast − ERA5]",
        "HRRR 1h Mean Wind Speed Error (m/s)\n[forecast − ERA5]",
        "GFS Day-Ahead Mean Temp Error (°C)\n[forecast − ERA5]",
        "GFS Day-Ahead Mean Wind Speed Error (m/s)\n[forecast − ERA5]",
    ]
    cbar_labels = ["°C", "m/s", "°C", "m/s"]

    fig, axes = plt.subplots(
        2, 2, figsize=(14, 10),
        subplot_kw={"projection": ccrs.PlateCarree()},
    )
    fig.suptitle(
        "Spatial Mean Forecast Error — ERA5 Grid, Texas (2025)",
        fontsize=12, fontweight="bold",
    )

    for ax, col, title, cbar_lbl in zip(
        axes.flat, var_cols, map_titles, cbar_labels
    ):
        vals = pixel_mean[col].values
        vmax = np.nanpercentile(np.abs(vals), 98)
        vmin = -vmax

        ax.add_geometries(
            [texas_geom],
            crs=ccrs.PlateCarree(),
            facecolor="#f0f0f0",
            edgecolor="black",
            linewidth=0.8,
            zorder=1,
        )
        sc = ax.scatter(
            pixel_mean["longitude"].values,
            pixel_mean["latitude"].values,
            c=vals,
            s=9,
            cmap="RdBu_r",
            vmin=vmin,
            vmax=vmax,
            transform=ccrs.PlateCarree(),
            zorder=2,
            linewidths=0,
        )
        ax.set_extent([-106.8, -93.0, 25.5, 36.8], crs=ccrs.PlateCarree())
        ax.gridlines(draw_labels=True, linewidth=0.3, color="gray", alpha=0.4)
        ax.set_title(title, fontsize=9, fontweight="bold")

        cb = plt.colorbar(sc, ax=ax, orientation="horizontal",
                          pad=0.06, fraction=0.046)
        cb.set_label(cbar_lbl, fontsize=8)
        cb.ax.tick_params(labelsize=7)

    fig.tight_layout()
    out_path = Path(save_dir) / "mean_forecast_error_maps.png"
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {out_path}")
    return out_path


def plot_std_forecast_error_maps(months=None, save_dir=None):
    """2×2 cartopy maps showing per-pixel forecast error standard deviation.

    Each panel covers one error variable (HRRR 1h temp, HRRR 1h wind,
    GFS day-ahead temp, GFS day-ahead wind).  High std = low forecast skill /
    high spatial variability.  Sequential colormap (YlOrRd) from zero upward;
    upper limit at the 98th percentile of per-pixel std values.
    """
    if months is None:
        months = DEFAULT_MONTHS
    dirs = setup_directories()
    if save_dir is None:
        save_dir = Path(dirs["figures"]) / "eda"
    Path(save_dir).mkdir(parents=True, exist_ok=True)

    print("Loading pixel hourly error data for std maps...")
    df = _load_pixel_hourly_errors(months, dirs)

    var_cols = [col for col, _ in _ERROR_VARS]
    pixel_std = (
        df.groupby(["latitude", "longitude"])[var_cols]
        .std()
        .reset_index()
    )
    print(f"  Unique pixels: {len(pixel_std):,}")

    # Load Texas geometry once
    states_shp = shpreader.natural_earth(
        resolution="10m", category="cultural", name="admin_1_states_provinces"
    )
    texas_geom = None
    for record in shpreader.Reader(states_shp).records():
        if record.attributes.get("name") == "Texas":
            texas_geom = record.geometry
            break
    if texas_geom is None:
        raise RuntimeError("Texas geometry not found in Natural Earth shapefile.")

    map_titles = [
        "HRRR 1h Temp Error Std Dev (°C)",
        "HRRR 1h Wind Speed Error Std Dev (m/s)",
        "GFS Day-Ahead Temp Error Std Dev (°C)",
        "GFS Day-Ahead Wind Speed Error Std Dev (m/s)",
    ]
    cbar_labels = ["°C", "m/s", "°C", "m/s"]

    fig, axes = plt.subplots(
        2, 2, figsize=(14, 10),
        subplot_kw={"projection": ccrs.PlateCarree()},
    )
    fig.suptitle(
        "Spatial Forecast Error Std Dev — ERA5 Grid, Texas (2025)",
        fontsize=12, fontweight="bold",
    )

    for ax, col, title, cbar_lbl in zip(
        axes.flat, var_cols, map_titles, cbar_labels
    ):
        vals = pixel_std[col].values
        vmin = 0.0
        vmax = np.nanpercentile(vals, 98)

        ax.add_geometries(
            [texas_geom],
            crs=ccrs.PlateCarree(),
            facecolor="#f0f0f0",
            edgecolor="black",
            linewidth=0.8,
            zorder=1,
        )
        sc = ax.scatter(
            pixel_std["longitude"].values,
            pixel_std["latitude"].values,
            c=vals,
            s=9,
            cmap="YlOrRd",
            vmin=vmin,
            vmax=vmax,
            transform=ccrs.PlateCarree(),
            zorder=2,
            linewidths=0,
        )
        ax.set_extent([-106.8, -93.0, 25.5, 36.8], crs=ccrs.PlateCarree())
        ax.gridlines(draw_labels=True, linewidth=0.3, color="gray", alpha=0.4)
        ax.set_title(title, fontsize=9, fontweight="bold")

        cb = plt.colorbar(sc, ax=ax, orientation="horizontal",
                          pad=0.06, fraction=0.046)
        cb.set_label(cbar_lbl, fontsize=8)
        cb.ax.tick_params(labelsize=7)

    fig.tight_layout()
    out_path = Path(save_dir) / "std_forecast_error_maps.png"
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {out_path}")
    return out_path


# =============================================================================
# ── Entry point ───────────────────────────────────────────────────────────────
# =============================================================================

def run_eda_plots(months=None):
    """Generate all EDA plots and return a dict of output paths."""
    if months is None:
        months = DEFAULT_MONTHS
    return {
        # "shadow_cost":     plot_shadow_cost_distribution(months=months),
        # "forecast_errors": plot_forecast_error_distributions(months=months),
        "combined":        plot_eda_combined(months=months),
        # "shadow_geolocation": plot_shadow_station_geolocation(months=months),
        "error_correlation_grid": plot_forecast_error_correlation_grid(months=months),
        "mean_error_maps":        plot_mean_forecast_error_maps(months=months),
        "std_error_maps":         plot_std_forecast_error_maps(months=months),
    }


if __name__ == "__main__":
    paths = run_eda_plots()
    print("\nAll EDA plots generated:")
    for key, path in paths.items():
        print(f"  {key}: {path}")
