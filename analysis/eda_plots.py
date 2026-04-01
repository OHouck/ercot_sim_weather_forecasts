"""
EDA Plots for ERCOT Forecast Error Analysis
============================================

Functions for exploratory data analysis plots used in check-in notes and reports.

Current plots
-------------
plot_shadow_cost_distribution      — histogram of hourly SCED shadow costs
plot_forecast_error_distributions  — overlaid PDFs of HRRR 1h and GFS day-ahead
                                     temperature and wind-speed errors
plot_eda_combined                  — single 3-panel figure combining both plots

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
import cartopy.io.shapereader as shpreader

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories

DEFAULT_MONTHS = [(2025, m) for m in range(1, 13)]

C_HRRR = "#1f77b4"   # blue  — HRRR 1 h
C_GFS  = "#d62728"   # red   — GFS day-ahead
C_HIST = "#4878d0"   # blue  — shadow cost histogram


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
    """Load system-level total_shadow_cost (one value per hour) for given months.

    Returns a 1-D numpy array of non-NaN shadow-cost values.
    """
    lmp_dir = Path(dirs["processed"]) / "combined_hourly_gridded_data"
    values = []
    for year, month in months:
        path = lmp_dir / f"pixel_hourly_gfs+hrrr_{year}_{month:02d}.parquet"
        if not path.exists():
            print(f"  [WARNING] Missing parquet: {path.name}")
            continue
        df = pd.read_parquet(path, columns=["valid_time", "total_shadow_cost"])
        df["valid_time"] = pd.to_datetime(df["valid_time"])
        df = df.drop_duplicates("valid_time")
        arr = df["total_shadow_cost"].dropna().values
        values.append(arr)
        print(f"  Loaded shadow costs {year}-{month:02d}: {len(arr):,} hours")
    if not values:
        raise RuntimeError("No shadow cost data loaded.")
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


# =============================================================================
# ── Panel-drawing helpers ─────────────────────────────────────────────────────
# =============================================================================

def _draw_shadow_cost_panel(ax, costs):
    """Draw the shadow-cost histogram onto ax (log-scale x-axis)."""
    log_costs = np.log10(costs + 1)
    bins = np.linspace(0, np.percentile(log_costs, 99.9), 80)

    ax.hist(
        log_costs, bins=bins, density=True,
        color=C_HIST, alpha=0.70, edgecolor="white", linewidth=0.3,
    )

    tick_vals = [0, 1, 10, 100, 1_000, 10_000, 100_000]
    ax.set_xticks([np.log10(v + 1) for v in tick_vals])
    ax.set_xticklabels(
        [f"\${v:,}" if v > 0 else "\$0" for v in tick_vals],
        fontsize=7.5, rotation=30, ha="right",
    )
    ax.set_xlabel("Total shadow cost / hour (log scale)", fontsize=9)
    ax.set_ylabel("Density", fontsize=9)
    ax.set_title("SCED Shadow Cost Distribution", fontsize=9, fontweight="bold")
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
    """Single 3-panel figure: shadow cost histogram | temp errors | wind errors.

    Intended for use in check-in notes where both sets of plots should share
    one figure block without taking up excessive vertical space.
    """
    if months is None:
        months = DEFAULT_MONTHS
    dirs = setup_directories()
    if save_dir is None:
        save_dir = Path(dirs["figures"]) / "eda"
    Path(save_dir).mkdir(parents=True, exist_ok=True)

    print("Loading data for combined EDA figure...")
    costs = _load_hourly_shadow_costs(months, dirs)
    print("Loading HRRR 1h errors...")
    hrrr = _load_era5_errors(months, dirs, model="hrrr", lead=1)
    print("Loading GFS day-ahead errors...")
    gfs  = _load_era5_errors(months, dirs, model="gfs",  lead=0)

    fig, axes = plt.subplots(1, 3, figsize=(16, 4))
    fig.suptitle(
        "Summary Statistics — Shadow Costs and Forecast Errors, ERCOT 2025",
        fontsize=11, fontweight="bold",
    )

    _draw_shadow_cost_panel(axes[0], costs)

    _draw_error_panel(axes[1], hrrr["temp"], gfs["temp"],
                      xlabel="Temperature error (°C)  [forecast − ERA5]",
                      xlim=(-12, 12), bin_width=0.25)
    axes[1].set_title("Temperature Errors — HRRR 1h vs GFS Day-Ahead",
                      fontsize=9, fontweight="bold")

    _draw_error_panel(axes[2], hrrr["wspd"], gfs["wspd"],
                      xlabel="Wind speed error (m/s)  [forecast − ERA5]",
                      xlim=(-10, 10), bin_width=0.20)
    axes[2].set_title("Wind Speed Errors — HRRR 1h vs GFS Day-Ahead",
                      fontsize=9, fontweight="bold")

    fig.tight_layout()
    out_path = Path(save_dir) / "eda_summary.png"
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
        "shadow_cost":     plot_shadow_cost_distribution(months=months),
        "forecast_errors": plot_forecast_error_distributions(months=months),
        "combined":        plot_eda_combined(months=months),
    }


if __name__ == "__main__":
    paths = run_eda_plots()
    print("\nAll EDA plots generated:")
    for key, path in paths.items():
        print(f"  {key}: {path}")
