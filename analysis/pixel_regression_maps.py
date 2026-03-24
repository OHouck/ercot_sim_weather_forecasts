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
import pyfixest as pf

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories

ROOT = Path(__file__).resolve().parent.parent

MONTHS = [(2025, m) for m in range(1, 13)]
DEPVAR = "system_lmp_std"
# All 4 error variables estimated jointly in one regression per pixel
ERROR_VARS = ["temp_error_1h", "wspd_error_1h", "temp_error_0h", "wspd_error_0h"]
CONTROLS = ["era5_temp", "era5_wspd", "is_weekend"] # need to add actual load
FE = ["hour_of_day", "month"]
SIG_LEVEL = 0.05 


def load_pixel_data(months):
    """Load and concatenate all monthly parquets for the given months.

    Parameters
    ----------
    months : list of (year, month) tuples

    Returns
    -------
    pd.DataFrame
    """
    dirs = setup_directories()
    data_dir = Path(dirs["processed"]) / "combined_hourly_gridded_data"

    dfs = []
    for year, month in months:
        fname = data_dir / f"pixel_hourly_gfs+hrrr_{year}_{month:02d}.parquet"
        if not fname.exists():
            print(f"  [WARNING] Missing: {fname}")
            continue
        df_month = pd.read_parquet(fname)
        dfs.append(df_month)
        print(f"  Loaded {year}-{month:02d}: {len(df_month):,} rows")

    if not dfs:
        raise FileNotFoundError("No monthly parquet files found.")

    df = pd.concat(dfs, ignore_index=True)
    df["is_weekend"] = (df["weekday"] >= 5).astype(int)

    n_pixels = df["pixel_id"].nunique()
    print(f"\nLoaded {len(df):,} total rows across {n_pixels:,} pixels.")
    return df


def run_pixel_regressions(df):
    """Run a per-pixel OLS regression and collect coefficient estimates.

    For each pixel, fits:
        system_lmp_std ~ temp_error_1h + wspd_error_1h + temp_error_0h +
                         wspd_error_0h + era5_temp + era5_wspd + is_weekend
                       | hour_of_day + month

    Parameters
    ----------
    df : pd.DataFrame
        Full pixel × hour dataset with all required columns.

    Returns
    -------
    pd.DataFrame
        Columns: pixel_id, lat, lon, error_var, coef, std_err, pvalue, n_obs
    """
    fml = (
        f"{DEPVAR} ~ "
        + " + ".join(ERROR_VARS + CONTROLS)
        + " | "
        + " + ".join(FE)
    )

    # Build a lookup from pixel_id -> (lat, lon).
    # Some rows have NaN lat/lon (missing ERA5 hours), so drop those first.
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

    records = []
    for i, pid in enumerate(pixel_ids):
        if i % 500 == 0:
            print(f"  Progress: {i:,} / {n_pixels:,} pixels")

        pixel_df = df[df["pixel_id"] == pid].copy()

        # Drop rows missing any required variable
        required_cols = [DEPVAR] + ERROR_VARS + CONTROLS + FE
        pixel_df = pixel_df.dropna(subset=required_cols)

        if len(pixel_df) < 100:
            continue

        # Pre-cast numeric columns to float64 so pyfixest doesn't do it on a slice
        float_cols = [c for c in required_cols if c not in FE]
        pixel_df[float_cols] = pixel_df[float_cols].astype("float64")

        try:
            fit = pf.feols(fml, data=pixel_df)
            tidy_df = fit.tidy()
        except Exception:
            continue

        n_obs = int(fit._N)
        lat = coords.loc[pid, "latitude"]
        lon = coords.loc[pid, "longitude"]

        for err_var in ERROR_VARS:
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
    results_df, error_var, title, ax, vmin, vmax, sig_level=0.05
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
    sig_level : float
        p-value threshold for significance.

    Returns
    -------
    sc : matplotlib PathCollection (scatter) for colorbar attachment
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

    _draw_texas_borders(ax)

    n_sig = len(sub)
    n_total = len(all_var)
    ax.set_title(
        f"{title}\n({n_sig:,} / {n_total:,} pixels significant)",
        fontsize=11,
    )

    return sc


def run_pixel_regression_maps(months=None, save_dir=None):
    """Main entry point: run pixel regressions and produce 2×2 map.

    Parameters
    ----------
    months : list of (year, month) tuples, optional
        Defaults to MONTHS (all of 2025).
    save_dir : str or Path, optional
        Directory for output files. Defaults to {figures}/pixel_regressions/.

    Returns
    -------
    dict with keys 'map' (Path) and 'table' (Path)
    """
    if months is None:
        months = MONTHS

    dirs = setup_directories()

    if save_dir is None:
        save_dir = Path(dirs["figures"]) / "pixel_regressions"
    else:
        save_dir = Path(save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)

    tables_dir = ROOT / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)

    # --- Load data ---
    df = load_pixel_data(months)

    # --- Run regressions ---
    results_df = run_pixel_regressions(df)

    # --- Save regression results table ---
    table_path = tables_dir / "pixel_regression_summary.csv"
    results_df.to_csv(table_path, index=False)
    print(f"\nRegression results saved to: {table_path}")

    # --- Compute shared color limits ---
    # Use 99th percentile of |coef| among significant pixels across all error vars
    sig_mask = results_df["pvalue"] < SIG_LEVEL
    print(f"\nTotal significant pixels across all error vars: {sig_mask.sum():,}")

    if sig_mask.sum() > 0:
        clim = np.nanpercentile(results_df.loc[sig_mask, "coef"].abs(), 99)
    else:
        clim = 1.0
    vmin, vmax = -clim, clim
    print(f"\nShared color limits: [{vmin:.4f}, {vmax:.4f}]")

    # --- Panel layout ---
    panel_config = [
        ("temp_error_1h", "HRRR 1h — Temperature Error"),
        ("wspd_error_1h", "HRRR 1h — Wind Speed Error"),
        ("temp_error_0h", "GFS Day-Ahead — Temperature Error"),
        ("wspd_error_0h", "GFS Day-Ahead — Wind Speed Error"),
    ]

    fig, axes = plt.subplots(
        2,
        2,
        figsize=(18, 14),
        subplot_kw={"projection": ccrs.PlateCarree()},
        gridspec_kw={"hspace": 0.15, "wspace": 0.05},
    )

    sc_last = None
    for idx, (err_var, panel_title) in enumerate(panel_config):
        row, col = divmod(idx, 2)
        ax = axes[row, col]
        sc = plot_pixel_coefficient_map(
            results_df,
            error_var=err_var,
            title=panel_title,
            ax=ax,
            vmin=vmin,
            vmax=vmax,
            sig_level=SIG_LEVEL,
        )
        if sc is not None:
            sc_last = sc

    # --- Shared colorbar ---
    if sc_last is not None:
        fig.colorbar(
            sc_last,
            ax=axes.ravel().tolist(),
            shrink=0.6,
            label="Coefficient estimate ($/MWh per unit error)",
            pad=0.02,
        )

    # --- Figure title ---
    fig.suptitle(
        "Pixel-Level Regression Coefficients: Forecast Error \u2192 LMP Spread\n"
        "(only significant pixels shown, p < 0.05; "
        "controls: observed weather, weekend FE; FE: hour-of-day, month)",
        fontsize=13,
        y=0.98,
    )

    # --- Save figure ---
    save_path = save_dir / "pixel_regression_2x2.png"
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Map saved to: {save_path}")

    return {"map": save_path, "table": table_path}


if __name__ == "__main__":
    run_pixel_regression_maps()
