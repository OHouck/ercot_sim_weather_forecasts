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
plot_forecast_error_vs_realized      — 2×2 panels of error vs ERA5 realized value for
                                       temp and wind, HRRR 1h and GFS day-ahead;
                                       shows whether errors grow at observed extremes

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


def _load_hourly_wide(months, dirs, columns, label=""):
    """Load one-row-per-hour data from pixel_hourly parquets.

    Reads the given columns (plus valid_time), deduplicates to one row per hour,
    and returns a sorted DataFrame.

    Args:
        months: List of (year, month) tuples.
        dirs: Directory dict from setup_directories().
        columns: List of column names to load (valid_time is always included).
        label: Optional description for progress messages.

    Returns:
        DataFrame with valid_time and requested columns, one row per hour, sorted.
    """
    lmp_dir = Path(dirs["processed"]) / "combined_hourly_gridded_data"
    chunks = []
    for year, month in months:
        path = lmp_dir / f"pixel_hourly_gfs+hrrr_{year}_{month:02d}.parquet"
        if not path.exists():
            print(f"  [WARNING] Missing parquet: {path.name}")
            continue
        df = pd.read_parquet(path, columns=["valid_time"] + columns)
        df["valid_time"] = pd.to_datetime(df["valid_time"])
        df = df.drop_duplicates("valid_time")
        chunks.append(df)
        suffix = f" {label}" if label else ""
        print(f"  Loaded{suffix} {year}-{month:02d}: {len(df):,} hours")
    if not chunks:
        raise RuntimeError(f"No{' ' + label if label else ''} data loaded.")
    return pd.concat(chunks, ignore_index=True).sort_values("valid_time").reset_index(drop=True)


def _load_hourly_shadow_costs(months, dirs):
    """Load system-level economic_congestion_cost (one value per hour) for given months.

    Returns a 1-D numpy array of non-NaN congestion cost values [$/h].
    """
    df = _load_hourly_wide(months, dirs, ["economic_congestion_cost"], label="congestion costs")
    return df["economic_congestion_cost"].dropna().values


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


def _load_pixel_hourly_errors(months, dirs, columns=None):
    """Load columns from pixel_hourly parquet files for given months.

    Returns a DataFrame with the requested columns (defaults to latitude,
    longitude, and the four error variables defined in _ERROR_VARS).
    """
    lmp_dir = Path(dirs["processed"]) / "combined_hourly_gridded_data"
    if columns is None:
        columns = ["latitude", "longitude"] + [col for col, _ in _ERROR_VARS]
    chunks = []
    for year, month in months:
        path = lmp_dir / f"pixel_hourly_gfs+hrrr_{year}_{month:02d}.parquet"
        if not path.exists():
            print(f"  [WARNING] Missing parquet: {path.name}")
            continue
        df = pd.read_parquet(path, columns=columns)
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
    df = _load_hourly_wide(
        months, dirs, ["wind_curtailment_mw", "solar_curtailment_mw"], label="curtailment"
    )
    return {
        "wind": df["wind_curtailment_mw"].dropna().values,
        "solar": df["solar_curtailment_mw"].dropna().values,
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
    bins = np.arange(xlim[0], xlim[1] + bin_width, bin_width)

    for vals_full, color, label in [
        (hrrr_vals, C_HRRR, "HRRR 1h"),
        (gfs_vals,  C_GFS,  "GFS day-ahead"),
    ]:
        vals = rng.choice(vals_full, size=500_000, replace=False) if len(vals_full) > 500_000 else vals_full
        ax.hist(vals, bins=bins, density=True,
                color=color, alpha=0.25, edgecolor="none")
        kde = gaussian_kde(vals, bw_method=0.15)
        x_grid = np.linspace(xlim[0], xlim[1], 800)
        ax.plot(x_grid, kde(x_grid), color=color, linewidth=2.0, label=label)

        mean_val = np.mean(vals_full)
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


def plot_cems_ercot_merge(year=2025, save_dir=None):
    """Plot fraction of ERCOT thermal resources matched to heat-rate data.

    Reads `{processed}/resource_heat_rates_{year}.parquet` and the DAM
    disclosure parquet files under `{raw}/ercot/dam_disclosure/{year}` to
    obtain the full list of ERCOT thermal resources. Saves a CSV of resource
    names and a bar plot showing the fraction matched by `heat_rate_source`.

    Returns:
        (csv_path, fig_path)
    """
    dirs = setup_directories()
    if save_dir is None:
        save_dir = Path(dirs["figures"]) / "eda"
    Path(save_dir).mkdir(parents=True, exist_ok=True)

    hr_path = Path(dirs["processed"]) / f"resource_heat_rates_{year}.parquet"
    if not hr_path.exists():
        raise FileNotFoundError(f"Missing heat-rate parquet: {hr_path}")
    hr = pd.read_parquet(hr_path)

    # Ensure expected columns
    if "resource_name" not in hr.columns:
        raise RuntimeError("resource_heat_rates parquet missing 'resource_name' column")
    if "heat_rate_source" not in hr.columns:
        # if older pipeline used different column name, try to detect
        if "heat_rate_source" not in hr.columns:
            hr["heat_rate_source"] = pd.NA

    # Load DAM disclosure resource names (thermal resources list)
    dam_dir = Path(dirs["raw"]) / "ercot" / "dam_disclosure" / str(year)
    resource_names = []
    if dam_dir.exists():
        for month_dir in sorted(dam_dir.glob("[0-9][0-9]")):
            for p in month_dir.glob(f"dam_gen_resource_{year}*.parquet"):
                try:
                    df = pd.read_parquet(p)
                except Exception:
                    continue
                if "Resource Name" in df.columns:
                    vals = df["Resource Name"].dropna().astype(str).unique().tolist()
                elif "resource_name" in df.columns:
                    vals = df["resource_name"].dropna().astype(str).unique().tolist()
                else:
                    vals = []
                resource_names.extend(vals)
    else:
        print(f"  [WARNING] DAM disclosure folder not found: {dam_dir}")

    resource_names = sorted(set(resource_names))
    if not resource_names:
        print("  [WARNING] No DAM resource names found — falling back to heat-rate table list")
        resource_names = sorted(hr["resource_name"].dropna().astype(str).unique().tolist())

    # Save resource names CSV
    csv_path = Path(save_dir) / f"ercot_thermal_resources_{year}.csv"
    pd.DataFrame({"resource_name": resource_names}).to_csv(csv_path, index=False)
    print(f"Saved resource name list: {csv_path}")

    # Merge heat-rate source info onto the resource list
    hr_subset = hr[["resource_name", "heat_rate_source"]].drop_duplicates("resource_name")
    resources_df = pd.DataFrame({"resource_name": resource_names})
    merged = resources_df.merge(hr_subset, on="resource_name", how="left")
    merged["heat_rate_source"] = merged["heat_rate_source"].fillna("unmatched")

    counts = merged["heat_rate_source"].value_counts().sort_values(ascending=False)
    frac = counts / counts.sum()

    # Make a readable label mapping
    label_map = {
        "cems_unit": "CEMS (unit)",
        "cems_plant": "CEMS (plant)",
        "eia923_pm_fuel": "EIA-923 (plant×PM×fuel)",
        "eia923_pm": "EIA-923 (plant×PM)",
        "eia923_plant": "EIA-923 (plant)",
        "tech_default": "Tech default",
        "unmatched": "Unmatched",
    }
    labels = [label_map.get(k, k) for k in frac.index]

    # Plot fractions
    fig, ax = plt.subplots(figsize=(8, 4))
    bars = ax.bar(range(len(frac)), frac.values, color="#4c72b0", edgecolor="none")
    ax.set_xticks(range(len(frac)))
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_ylabel("Fraction of thermal resources")
    ax.set_ylim(0, 1.02)
    ax.set_title(f"ERCOT thermal resources: heat-rate source breakdown ({year})")

    # Annotate bars with percentages
    for i, v in enumerate(frac.values):
        ax.text(i, v + 0.02, f"{v*100:.1f}%", ha="center", va="bottom", fontsize=9)

    fig.tight_layout()
    fig_path = Path(save_dir) / f"cems_ercot_merge_match_fraction_{year}.png"
    fig.savefig(fig_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved match-fraction plot: {fig_path}")

    return csv_path, fig_path


def plot_cems_ercot_merge_enriched(year=2025, save_dir=None, top_n_types=8):
    """Enriched stacked-bar plot of match fractions.

    Produces a single figure with stacked bars for:
      - Overall sample
      - Each resource type (up to `top_n_types` most common)
      - Quartiles of annual SCED net generation (Q1..Q4)

    Also saves a CSV with the fractional breakdown per group and per source.
    Returns: (breakdown_csv_path, fig_path)
    """
    dirs = setup_directories()
    if save_dir is None:
        save_dir = Path(dirs["figures"]) / "eda"
    Path(save_dir).mkdir(parents=True, exist_ok=True)

    hr_path = Path(dirs["processed"]) / f"resource_heat_rates_{year}.parquet"
    if not hr_path.exists():
        raise FileNotFoundError(f"Missing heat-rate parquet: {hr_path}")
    hr = pd.read_parquet(hr_path)

    # Load SCED annual net generation to form quartiles (fallback to zero)
    try:
        from process_data.process_sced_thermal import build_sced_thermal_annual
        sced = build_sced_thermal_annual(year, force_rebuild=False)
        sced = sced[["resource_name", "net_gen_mwh_annual"]]
    except Exception:
        sced = pd.DataFrame(columns=["resource_name", "net_gen_mwh_annual"])
        print(f"  [WARNING] could not load SCED annual; quartile groups will be empty")

    # Prepare master resource table
    resources = hr[["resource_name", "resource_type", "heat_rate_source"]].drop_duplicates("resource_name")
    resources["resource_name"] = resources["resource_name"].astype(str)
    resources = resources.merge(sced, on="resource_name", how="left")
    resources["net_gen_mwh_annual"] = pd.to_numeric(resources.get("net_gen_mwh_annual"), errors="coerce").fillna(0.0)
    resources["heat_rate_source"] = resources["heat_rate_source"].fillna("unmatched")

    # Define groups: Overall, top resource types, quartiles
    overall_label = "Overall"
    type_counts = resources["resource_type"].fillna("UNKNOWN").value_counts()
    top_types = type_counts.nlargest(top_n_types).index.tolist()
    type_labels = list(top_types)

    # Quartiles among resources with positive generation; if none, use empty
    pos = resources[resources["net_gen_mwh_annual"] > 0]
    if len(pos) >= 4:
        quart_edges = pos["net_gen_mwh_annual"].quantile([0.25, 0.5, 0.75]).values
        def qlabel(val):
            if val <= quart_edges[0]:
                return "Q1"
            if val <= quart_edges[1]:
                return "Q2"
            if val <= quart_edges[2]:
                return "Q3"
            return "Q4"
        resources["gen_quartile"] = resources["net_gen_mwh_annual"].map(qlabel)
    else:
        resources["gen_quartile"] = pd.NA

    quart_labels = ["Q1", "Q2", "Q3", "Q4"]

    groups = [overall_label] + type_labels + quart_labels

    # Identify all heat_rate_source categories present
    sources = resources["heat_rate_source"].fillna("unmatched").unique().tolist()
    # Put tech_default and unmatched last for readability
    def sort_key(s):
        if s == "tech_default":
            return (2, s)
        if s == "unmatched":
            return (3, s)
        return (1, s)
    sources = sorted(sources, key=sort_key)

    # Compute fractional breakdown per group
    rows = []
    for g in groups:
        if g == overall_label:
            subset = resources
        elif g in quart_labels:
            subset = resources[resources["gen_quartile"] == g]
        else:
            subset = resources[resources["resource_type"] == g]

        total = len(subset)
        counts = subset["heat_rate_source"].value_counts()
        for src in sources:
            cnt = int(counts.get(src, 0))
            frac = cnt / total if total > 0 else 0.0
            rows.append({"group": g, "source": src, "count": cnt, "fraction": frac, "n_total": total})

    breakdown = pd.DataFrame(rows)
    csv_out = Path(save_dir) / f"cems_ercot_match_breakdown_{year}.csv"
    breakdown.to_csv(csv_out, index=False)
    print(f"Saved breakdown CSV: {csv_out}")

    # Pivot for plotting stacked bars
    pivot = breakdown.pivot_table(index="group", columns="source", values="fraction", fill_value=0.0)
    pivot = pivot.reindex(groups)  # ensure order

    # Colors for sources
    cmap = plt.get_cmap("tab20")
    n_src = len(pivot.columns)
    colors = [cmap(i % 20) for i in range(n_src)]

    fig, ax = plt.subplots(figsize=(12, 5))
    left = np.zeros(len(pivot))
    x = np.arange(len(pivot))
    for i, src in enumerate(pivot.columns):
        vals = pivot[src].values
        ax.bar(x, vals, bottom=left, color=colors[i], label=src)
        left += vals

    ax.set_xticks(x)
    ax.set_xticklabels(groups, rotation=45, ha="right")
    ax.set_ylabel("Fraction of resources")
    ax.set_title(f"Heat-rate match fractions — overall / by type / by gen quartile ({year})")
    ax.set_ylim(0, 1.02)
    ax.legend(title="heat_rate_source", bbox_to_anchor=(1.02, 1), loc="upper left")
    fig.tight_layout()
    fig_out = Path(save_dir) / f"cems_ercot_merge_stacked_{year}.png"
    fig.savefig(fig_out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved stacked-bar plot: {fig_out}")

    # mark todo items complete
    return csv_out, fig_out


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


def _draw_error_vs_realized_panel(ax, realized, error, xlabel, color, n_bins=50):
    """Draw scatter + binned mean error ± 1σ band vs ERA5 realized value onto ax.

    Args:
        ax: Matplotlib Axes.
        realized: 1-D numpy array of ERA5 observed values.
        error: 1-D numpy array of forecast errors aligned with realized.
        xlabel: X-axis label string.
        color: Line/band colour for the binned summary.
        n_bins: Number of equal-width bins across the [0.5, 99.5] percentile range.
    """
    mask = ~(np.isnan(realized) | np.isnan(error))
    realized_c = realized[mask]
    error_c = error[mask]

    rng = np.random.default_rng(42)
    idx = rng.choice(len(realized_c), size=200_000, replace=False) if len(realized_c) > 200_000 else np.arange(len(realized_c))
    ax.scatter(
        realized_c[idx], error_c[idx],
        alpha=0.04, s=1, color="gray", rasterized=True,
    )

    lo, hi = np.percentile(realized_c, [0.5, 99.5])
    edges = np.linspace(lo, hi, n_bins + 1)
    mids = (edges[:-1] + edges[1:]) / 2
    means = np.full(n_bins, np.nan)
    stds = np.full(n_bins, np.nan)
    bin_idx = np.clip(np.digitize(realized_c, edges) - 1, 0, n_bins - 1)
    agg = pd.DataFrame({"b": bin_idx, "e": error_c}).groupby("b")["e"].agg(["mean", "std", "count"])
    enough = agg[agg["count"] >= 20]
    means[enough.index] = enough["mean"].values
    stds[enough.index] = enough["std"].values

    valid = ~np.isnan(means)
    ax.plot(mids[valid], means[valid], color=color, linewidth=2.0, label="Mean error")
    ax.fill_between(
        mids[valid], means[valid] - stds[valid], means[valid] + stds[valid],
        alpha=0.20, color=color, label="±1σ",
    )
    ax.axhline(0, color="black", linewidth=0.8, linestyle="-", alpha=0.4)
    ax.set_xlabel(xlabel, fontsize=9)
    ax.set_ylabel("Forecast error [forecast − ERA5]", fontsize=9)
    ax.legend(fontsize=8, loc="upper right")
    ax.grid(axis="both", alpha=0.25, linewidth=0.5)


def plot_forecast_error_vs_realized(months=None, save_dir=None):
    """2×2 figure: forecast error vs ERA5 realized value for temp/wind × 1h/day-ahead.

    Shows whether forecast skill degrades at extreme observed values
    (heteroscedasticity or non-linearity). X-axis is the ERA5 realized value;
    Y-axis is forecast − ERA5. Each panel shows a subsampled scatter overlaid
    with the binned mean error and a ±1σ band.

    Layout:
        Row 0: temperature — HRRR 1h (left), GFS day-ahead (right)
        Row 1: wind speed  — HRRR 1h (left), GFS day-ahead (right)

    Args:
        months: List of (year, month) tuples. Defaults to DEFAULT_MONTHS.
        save_dir: Optional figure output directory.

    Returns:
        Path to saved figure.
    """
    if months is None:
        months = DEFAULT_MONTHS
    dirs = setup_directories()
    if save_dir is None:
        save_dir = Path(dirs["figures"]) / "eda"
    Path(save_dir).mkdir(parents=True, exist_ok=True)

    print("Loading error-vs-realized data...")
    _cols = ["era5_temp", "era5_wspd", "temp_error_1h", "wspd_error_1h", "temp_error_0h", "wspd_error_0h"]
    df = _load_pixel_hourly_errors(months, dirs, columns=_cols)

    panels = [
        ("era5_temp", "temp_error_1h",  C_HRRR, "ERA5 realized temperature (°C)",  "HRRR 1h — Temperature Error"),
        ("era5_temp", "temp_error_0h",  C_GFS,  "ERA5 realized temperature (°C)",  "GFS Day-Ahead — Temperature Error"),
        ("era5_wspd", "wspd_error_1h",  C_HRRR, "ERA5 realized wind speed (m/s)",  "HRRR 1h — Wind Speed Error"),
        ("era5_wspd", "wspd_error_0h",  C_GFS,  "ERA5 realized wind speed (m/s)",  "GFS Day-Ahead — Wind Speed Error"),
    ]

    fig, axes = plt.subplots(2, 2, figsize=(13, 10))
    fig.suptitle(
        "Forecast Error vs Realized Value — ERA5 Grid, Texas (2025)",
        fontsize=11, fontweight="bold",
    )

    for ax, (realized_col, error_col, color, xlabel, title) in zip(axes.flat, panels):
        _draw_error_vs_realized_panel(
            ax, df[realized_col].values, df[error_col].values, xlabel, color
        )
        ax.set_title(title, fontsize=9, fontweight="bold")

    fig.tight_layout()
    out_path = Path(save_dir) / "forecast_error_vs_realized.png"
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

def plot_co2_intensity(months=None, save_dir=None):
    """Generate weekly-average time-series and diurnal-average plots of ERCOT grid CO₂ intensity.

    Creates a 2-panel figure:
    - Top: weekly-average CO₂ intensity time series (one point per calendar week).
    - Bottom: mean CO₂ intensity by hour of day (0–23), averaged across all weeks.

    Args:
        months: List of (year, month) tuples. Defaults to DEFAULT_MONTHS.
        save_dir: Optional figure output directory.

    Returns:
        Path to the saved figure.
    """
    if months is None:
        months = DEFAULT_MONTHS
    dirs = setup_directories()
    if save_dir is None:
        save_dir = Path(dirs["figures"]) / "eda"
    Path(save_dir).mkdir(parents=True, exist_ok=True)

    ts = _load_hourly_wide(months, dirs, ["avg_intensity_kg_per_mwh"], label="CO2 intensity")
    ts = ts.dropna(subset=["avg_intensity_kg_per_mwh"])

    week_start = ts["valid_time"].dt.to_period("W").dt.start_time
    weekly_ts = (
        ts.groupby(week_start)["avg_intensity_kg_per_mwh"]
        .mean()
        .rename_axis("week")
        .reset_index(name="intensity")
    )

    diurnal_avg = (
        ts.groupby(ts["valid_time"].dt.hour)["avg_intensity_kg_per_mwh"]
        .mean()
        .reindex(range(24))
    )

    C_CO2 = "#2e7d32"

    fig, axes = plt.subplots(2, 1, figsize=(14, 8))
    fig.suptitle("ERCOT Grid CO₂ Intensity — 2025", fontsize=12, fontweight="bold")

    axes[0].plot(weekly_ts["week"], weekly_ts["intensity"], color=C_CO2, linewidth=1.8, marker="o", markersize=4)
    axes[0].fill_between(weekly_ts["week"], weekly_ts["intensity"], alpha=0.15, color=C_CO2)
    axes[0].set_xlabel("Date", fontsize=9)
    axes[0].set_ylabel("CO₂ intensity (kg CO₂/MWh)", fontsize=9)
    axes[0].set_title("Weekly Average CO₂ Intensity", fontsize=9, fontweight="bold")
    axes[0].grid(axis="both", alpha=0.25, linewidth=0.5)

    x = diurnal_avg.index.values
    y = diurnal_avg.values
    axes[1].plot(x, y, color=C_CO2, linewidth=2.0, marker="o", markersize=4)
    axes[1].fill_between(x, y, alpha=0.15, color=C_CO2)
    axes[1].set_xlim(0, 23)
    axes[1].set_xticks(range(0, 24, 3))
    axes[1].set_xticklabels([f"{h:02d}:00" for h in range(0, 24, 3)], fontsize=9)
    axes[1].set_xlabel("Hour of day (Central)", fontsize=9)
    axes[1].set_ylabel("CO₂ intensity (kg CO₂/MWh)", fontsize=9)
    axes[1].set_title("Average CO₂ Intensity by Hour of Day", fontsize=9, fontweight="bold")
    axes[1].grid(axis="y", alpha=0.25, linewidth=0.5)

    fig.tight_layout()
    out_path = Path(save_dir) / "co2_intensity.png"
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {out_path}")
    return out_path


def plot_offer_curves(year=2025, save_dir=None):
    """2×5 offer-curve figures split by season, plus a full-year figure.

    Rows: DAM (top) | RT (bottom).
    Columns: CC≥90 MW | CC<90 MW | CT≥90 MW | CT<90 MW | Coal/Lignite.

    Four figures are produced: full year, summer (Jun–Aug), winter (Dec–Feb),
    and fall+spring (Mar–May, Sep–Nov). Offer prices above the 99th percentile
    per fuel group are winsorized to suppress extreme scarcity hours. Caps are
    computed once from the full-year data so seasonal plots share the same scale.

    Args:
        year: operating year (default 2025)
        save_dir: optional output directory; defaults to {figures}/eda

    Returns:
        dict mapping season label to saved figure Path.
        Keys: "full", "summer", "winter", "spring_fall".
    """
    from process_data.compute_markups import QUANTILE_LEVELS, COAL_TYPES

    dirs = setup_directories()
    save_dir = Path(save_dir) if save_dir is not None else Path(dirs["figures"]) / "eda"
    save_dir.mkdir(parents=True, exist_ok=True)

    dam_path = Path(dirs["processed"]) / f"dam_markups_{year}.parquet"
    rt_path  = Path(dirs["processed"]) / f"rt_markups_{year}.parquet"
    if not dam_path.exists():
        raise FileNotFoundError(f"Missing DAM markups parquet: {dam_path}")

    x_vals   = [int(q * 100) for q in QUANTILE_LEVELS]
    col_vals = [f"offer_price_p{x}" for x in x_vals]

    dam_df = pd.read_parquet(dam_path, columns=["Resource Type", "Delivery Date", "marginal_cost"] + col_vals)
    dam_df["month"] = pd.to_datetime(dam_df["Delivery Date"]).dt.month

    if rt_path.exists():
        rt_df = pd.read_parquet(rt_path, columns=["Resource Type", "valid_time", "marginal_cost"] + col_vals)
        rt_df["month"] = pd.to_datetime(rt_df["valid_time"]).dt.month
    else:
        rt_df = None

    panels = [
        ("CC ≥90 MW (CCGT90)", {"CCGT90"}),
        ("CC <90 MW (CCLE90)", {"CCLE90"}),
        ("CT ≥90 MW (SCGT90)", {"SCGT90"}),
        ("CT <90 MW (SCLE90)", {"SCLE90"}),
        ("Coal / Lignite",     COAL_TYPES),
    ]

    seasons = {
        "full":        (list(range(1, 13)),   f"Full Year {year}"),
        "summer":      ([6, 7, 8],            f"Summer (Jun–Aug) {year}"),
        "winter":      ([12, 1, 2],           f"Winter (Dec–Feb) {year}"),
        "spring_fall": ([3, 4, 5, 9, 10, 11], f"Spring/Fall (Mar–May, Sep–Nov) {year}"),
    }

    C_OFFER = "#6baed6"
    C_MC    = "#08306b"

    def _winsorize_caps(df):
        caps = {}
        for label, types in panels:
            sub = df[df["Resource Type"].isin(types)]
            caps[label] = {c: np.nanpercentile(sub[c], 99) if len(sub) else np.inf
                           for c in col_vals}
        return caps

    dam_caps = _winsorize_caps(dam_df)
    rt_caps  = _winsorize_caps(rt_df) if rt_df is not None else None

    def _render(dam_sub, rt_sub, title, save_path):
        fig, axes = plt.subplots(2, 5, figsize=(22, 9))
        fig.suptitle(
            f"Average Supply Offer Curves and Marginal Costs — ERCOT Thermal Generators\n{title}",
            fontsize=11, fontweight="bold",
        )
        for row_axes, (row_df, market_label, caps) in zip(
            axes, [(dam_sub, "DAM", dam_caps), (rt_sub, "RT", rt_caps)]
        ):
            if row_df is None:
                for ax in row_axes:
                    ax.text(0.5, 0.5, "No RT data", transform=ax.transAxes,
                            ha="center", va="center", fontsize=8, color="gray", style="italic")
                    ax.set_xticks([])
                    ax.set_yticks([])
                continue

            for ax, (label, types) in zip(row_axes, panels):
                ax.set_title(f"{market_label}: {label}", fontsize=8, fontweight="bold")

                sub = row_df[row_df["Resource Type"].isin(types)]
                if sub.empty:
                    ax.text(0.5, 0.5, "No data", transform=ax.transAxes, ha="center", va="center")
                    continue

                clipped    = sub[col_vals].clip(upper=pd.Series(caps[label]), axis=1)
                mean_offer = clipped.mean().tolist()
                mean_mc    = sub["marginal_cost"].mean()

                ax.plot(x_vals, mean_offer, color=C_OFFER, linewidth=2.0,
                        marker="o", markersize=4, label="Offer price")
                ax.axhline(mean_mc, color=C_MC, linewidth=2.0, linestyle="-", label="Marginal cost")

                y_min = min(mean_offer + [mean_mc])
                y_max = max(mean_offer + [mean_mc])
                pad = max((y_max - y_min) * 0.20, 10)
                ax.set_ylim(y_min - pad, y_max + pad)
                ax.set_xlim(0, 100)
                ax.set_xlabel("Quantity (% of capacity)", fontsize=8)
                ax.set_ylabel("Price ($/MWh)", fontsize=8)
                ax.legend(fontsize=7)
                ax.grid(axis="both", alpha=0.25, linewidth=0.5)

        fig.tight_layout()
        fig.savefig(save_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        print(f"Saved: {save_path}")
        return save_path

    out_paths = {}
    for season_key, (months, season_title) in seasons.items():
        dam_sub = dam_df[dam_df["month"].isin(months)]
        rt_sub  = rt_df[rt_df["month"].isin(months)] if rt_df is not None else None
        out_paths[season_key] = _render(dam_sub, rt_sub, season_title,
                                        save_dir / f"offer_curves_{year}_{season_key}.png")

    return out_paths


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
        # "combined":        plot_eda_combined(months=months),
        # "error_correlation_grid":  plot_forecast_error_correlation_grid(months=months),
        # "mean_error_maps":         plot_mean_forecast_error_maps(months=months),
        # "std_error_maps":          plot_std_forecast_error_maps(months=months),
        # "error_vs_realized":       plot_forecast_error_vs_realized(months=months),
        # "co2_intensity":           plot_co2_intensity(months=months),
        "offer_curves":            plot_offer_curves(year=2025),
    }


if __name__ == "__main__":
    paths = run_eda_plots()
    print("\nAll EDA plots generated:")
    for key, path in paths.items():
        print(f"  {key}: {path}")
