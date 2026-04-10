"""Forecast Value Map — dollar value of forecast improvement at each pixel.

Combines per-pixel regression coefficients with observed forecast error
variance to estimate the economic value of reducing forecast errors.

    value_i,e = |β_i,e| × σ(error_i,e)

This gives the congestion cost reduction (in $/MWh or shadow-cost units)
from a 1-standard-deviation forecast improvement at pixel i for error type e.

Usage:
    uv run python -m analysis.forecast_value_map
    uv run python -m analysis.forecast_value_map --months 7 --depvar total_shadow_cost
"""

import argparse
import os
import sys
from pathlib import Path

import cartopy.crs as ccrs
import cartopy.io.shapereader as shpreader
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np
import pandas as pd
from shapely.geometry import box as shapely_box

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories

from analysis.pixel_regression_maps import (
    load_pixel_data,
    run_pixel_regressions,
    SIG_LEVEL,
)

# ── Constants ────────────────────────────────────────────────────────────────

WEATHER_ERROR_VARS = [
    "temp_error_1h", "wspd_error_1h", "temp_error_0h", "wspd_error_0h",
]
WEATHER_CONTROLS = ["era5_temp", "era5_wspd", "is_weekend"]
DEFAULT_MONTHS = [(2025, m) for m in range(1, 13)]

ERROR_LABELS = {
    "temp_error_1h": "HRRR 1h — Temperature",
    "wspd_error_1h": "HRRR 1h — Wind Speed",
    "temp_error_0h": "GFS Day-Ahead — Temperature",
    "wspd_error_0h": "GFS Day-Ahead — Wind Speed",
}

TEXAS_CITIES = {
    "Houston": (-95.37, 29.76),
    "Dallas": (-96.80, 32.78),
    "San Antonio": (-98.49, 29.42),
    "Austin": (-97.74, 30.27),
    "El Paso": (-106.45, 31.76),
}


# ── Core computation ─────────────────────────────────────────────────────────

def compute_pixel_error_stats(df, error_vars=None):
    """Compute per-pixel standard deviation of each error variable.

    Args:
        df: Pixel × hour DataFrame.
        error_vars: List of error column names.

    Returns:
        DataFrame with pixel_id, lat, lon, and {var}_std columns.
    """
    if error_vars is None:
        error_vars = WEATHER_ERROR_VARS

    agg_dict = {v: "std" for v in error_vars}
    agg_dict["latitude"] = "first"
    agg_dict["longitude"] = "first"

    stats = df.groupby("pixel_id").agg(agg_dict).reset_index()
    stats = stats.rename(columns={v: f"{v}_std" for v in error_vars})
    return stats


def compute_forecast_value(regression_results, pixel_error_stats,
                            error_vars=None, sig_level=SIG_LEVEL):
    """Compute the forecast value at each pixel.

    value_i,e = |β_i,e| × σ(error_i,e) for significant pixels
    Insignificant pixels get value = 0.

    Args:
        regression_results: Output of run_pixel_regressions().
        pixel_error_stats: Output of compute_pixel_error_stats().
        error_vars: List of error variable names.
        sig_level: Significance threshold.

    Returns:
        DataFrame with: pixel_id, lat, lon, value_{var}, total_value.
    """
    if error_vars is None:
        error_vars = WEATHER_ERROR_VARS

    # Start with pixel locations from error stats
    value_df = pixel_error_stats[["pixel_id", "latitude", "longitude"]].copy()

    for var in error_vars:
        # Get significant regression coefficients for this error variable
        var_results = regression_results[
            (regression_results["error_var"] == var) &
            (regression_results["pvalue"] < sig_level)
        ][["pixel_id", "coef"]].copy()

        # Get error std for each pixel
        std_col = f"{var}_std"
        var_stats = pixel_error_stats[["pixel_id", std_col]].copy()

        # Merge and compute value
        merged = value_df[["pixel_id"]].merge(
            var_results, on="pixel_id", how="left"
        ).merge(
            var_stats, on="pixel_id", how="left"
        )

        value_col = f"value_{var}"
        merged[value_col] = merged["coef"].abs() * merged[std_col]
        merged[value_col] = merged[value_col].fillna(0)

        value_df[value_col] = merged[value_col].values

    # Total value across all error variables
    value_cols = [f"value_{v}" for v in error_vars]
    value_df["total_value"] = value_df[value_cols].sum(axis=1)

    return value_df


# ── Plotting ─────────────────────────────────────────────────────────────────

def _get_texas_boundary():
    """Load Texas boundary for map overlay."""
    shp_path = shpreader.natural_earth(
        resolution="10m", category="cultural", name="admin_1_states_provinces"
    )
    reader = shpreader.Reader(shp_path)
    for rec in reader.records():
        if rec.attributes.get("name") == "Texas":
            return rec.geometry
    return None


def plot_forecast_value_map(value_df, error_vars=None, save_path=None,
                             title_prefix="Forecast Value"):
    """Plot a 2×2 map of forecast value by error type + total.

    Args:
        value_df: Output of compute_forecast_value().
        error_vars: List of error variables to plot.
        save_path: Path to save figure.
        title_prefix: Prefix for subplot titles.
    """
    if error_vars is None:
        error_vars = WEATHER_ERROR_VARS

    texas_geom = _get_texas_boundary()
    projection = ccrs.LambertConformal(
        central_longitude=-99, central_latitude=31.5
    )

    fig, axes = plt.subplots(
        2, 2, figsize=(16, 14),
        subplot_kw={"projection": projection},
    )

    # Plot layout: HRRR temp, HRRR wind, GFS temp, GFS wind
    plot_vars = error_vars[:4]

    # Compute shared vmax across all panels for comparability
    all_values = []
    for var in plot_vars:
        vals = value_df[f"value_{var}"]
        all_values.extend(vals[vals > 0].values)

    if all_values:
        vmax = np.percentile(all_values, 95)
    else:
        vmax = 1.0

    for idx, (ax, var) in enumerate(zip(axes.flat, plot_vars)):
        ax.set_extent([-107, -93, 25.5, 37], crs=ccrs.PlateCarree())

        if texas_geom is not None:
            from cartopy.feature import ShapelyFeature
            ax.add_feature(
                ShapelyFeature([texas_geom], ccrs.PlateCarree(),
                               facecolor="whitesmoke", edgecolor="black",
                               linewidth=0.8),
                zorder=0,
            )

        value_col = f"value_{var}"
        nonzero = value_df[value_df[value_col] > 0]

        if len(nonzero) > 0:
            sc = ax.scatter(
                nonzero["longitude"], nonzero["latitude"],
                c=nonzero[value_col],
                s=3, alpha=0.8,
                cmap="YlOrRd",
                norm=mcolors.Normalize(vmin=0, vmax=vmax),
                transform=ccrs.PlateCarree(),
                zorder=2,
            )

        label = ERROR_LABELS.get(var, var)
        n_nonzero = len(nonzero)
        ax.set_title(f"{label}\n({n_nonzero:,} valued pixels)",
                     fontsize=11)

        # City markers
        for city, (clon, clat) in TEXAS_CITIES.items():
            ax.plot(clon, clat, "k.", markersize=3,
                    transform=ccrs.PlateCarree(), zorder=3)
            ax.text(clon + 0.15, clat + 0.15, city, fontsize=7,
                    transform=ccrs.PlateCarree(), zorder=3)

    # Shared colorbar
    fig.subplots_adjust(right=0.88, hspace=0.15, wspace=0.08)
    cbar_ax = fig.add_axes([0.90, 0.15, 0.02, 0.7])
    sm = plt.cm.ScalarMappable(
        cmap="YlOrRd", norm=mcolors.Normalize(vmin=0, vmax=vmax)
    )
    sm.set_array([])
    cbar = fig.colorbar(sm, cax=cbar_ax)
    cbar.set_label("Forecast Value (|β| × σ)", fontsize=12)

    fig.suptitle(f"{title_prefix}\n"
                 "Value of 1-σ forecast improvement per pixel",
                 fontsize=14, fontweight="bold", y=0.98)

    if save_path:
        fig.savefig(save_path, dpi=200, bbox_inches="tight")
        print(f"Saved: {save_path}")
        plt.close(fig)
    else:
        plt.show()


def plot_total_value_map(value_df, save_path=None, title="Total Forecast Value"):
    """Plot a single map of total forecast value across all error types."""
    texas_geom = _get_texas_boundary()
    projection = ccrs.LambertConformal(
        central_longitude=-99, central_latitude=31.5
    )

    fig, ax = plt.subplots(
        1, 1, figsize=(10, 8),
        subplot_kw={"projection": projection},
    )
    ax.set_extent([-107, -93, 25.5, 37], crs=ccrs.PlateCarree())

    if texas_geom is not None:
        from cartopy.feature import ShapelyFeature
        ax.add_feature(
            ShapelyFeature([texas_geom], ccrs.PlateCarree(),
                           facecolor="whitesmoke", edgecolor="black",
                           linewidth=0.8),
            zorder=0,
        )

    nonzero = value_df[value_df["total_value"] > 0]
    vmax = np.percentile(nonzero["total_value"], 95) if len(nonzero) > 0 else 1

    if len(nonzero) > 0:
        sc = ax.scatter(
            nonzero["longitude"], nonzero["latitude"],
            c=nonzero["total_value"],
            s=5, alpha=0.8,
            cmap="YlOrRd",
            norm=mcolors.Normalize(vmin=0, vmax=vmax),
            transform=ccrs.PlateCarree(),
            zorder=2,
        )
        cbar = fig.colorbar(sc, ax=ax, shrink=0.7, pad=0.02)
        cbar.set_label("Total Forecast Value (|β| × σ)", fontsize=11)

    for city, (clon, clat) in TEXAS_CITIES.items():
        ax.plot(clon, clat, "k.", markersize=4,
                transform=ccrs.PlateCarree(), zorder=3)
        ax.text(clon + 0.15, clat + 0.15, city, fontsize=8,
                transform=ccrs.PlateCarree(), zorder=3)

    ax.set_title(title, fontsize=14, fontweight="bold")

    if save_path:
        fig.savefig(save_path, dpi=200, bbox_inches="tight")
        print(f"Saved: {save_path}")
        plt.close(fig)
    else:
        plt.show()


# ── Main entry point ─────────────────────────────────────────────────────────

def run_forecast_value_analysis(months=None, depvar="first_interval_shadow_cost",
                                 save_dir=None):
    """Run the full forecast value map analysis.

    Args:
        months: List of (year, month) tuples. Default: full 2025.
        depvar: Congestion dependent variable.
        save_dir: Directory for figures.

    Returns:
        Dict with 'value_df', 'regression_results', 'error_stats'.
    """
    if months is None:
        months = DEFAULT_MONTHS

    dirs = setup_directories()
    if save_dir is None:
        save_dir = os.path.join(dirs["figures"], "forecast_value")
    tables_dir = os.path.join(dirs["root"], "tables")
    os.makedirs(save_dir, exist_ok=True)
    os.makedirs(tables_dir, exist_ok=True)

    # Step 1: Load data
    print("Step 1: Loading pixel data...")
    df = load_pixel_data(months)

    # Step 2: Compute pixel error statistics
    print("\nStep 2: Computing per-pixel error statistics...")
    error_stats = compute_pixel_error_stats(df)
    print(f"  {len(error_stats)} pixels with error statistics")

    # Step 3: Run per-pixel regressions
    print("\nStep 3: Running per-pixel regressions...")
    reg_results = run_pixel_regressions(
        df,
        depvar=depvar,
        error_vars=WEATHER_ERROR_VARS,
        controls=WEATHER_CONTROLS,
        min_obs=100,
    )

    if reg_results.empty:
        print("No regression results — cannot compute forecast value.")
        return {"value_df": pd.DataFrame(), "regression_results": reg_results,
                "error_stats": error_stats}

    # Step 4: Compute forecast value
    print("\nStep 4: Computing forecast value per pixel...")
    value_df = compute_forecast_value(reg_results, error_stats)

    # Summary
    nonzero = value_df[value_df["total_value"] > 0]
    print(f"  {len(nonzero)} pixels with nonzero forecast value")
    print(f"  Total value distribution (nonzero pixels):")
    print(f"    Mean:   {nonzero['total_value'].mean():.1f}")
    print(f"    Median: {nonzero['total_value'].median():.1f}")
    print(f"    P90:    {nonzero['total_value'].quantile(0.9):.1f}")
    print(f"    Max:    {nonzero['total_value'].max():.1f}")

    # By error type
    for var in WEATHER_ERROR_VARS:
        vcol = f"value_{var}"
        nz = value_df[value_df[vcol] > 0]
        print(f"    {var}: {len(nz)} pixels, "
              f"mean value = {nz[vcol].mean():.1f}")

    # Step 5: Save results
    tag = depvar
    if len(months) == 1:
        tag += f"_{months[0][0]}_{months[0][1]:02d}"

    # Tables
    table_path = os.path.join(tables_dir, f"forecast_value_{tag}.csv")
    value_df.to_csv(table_path, index=False)
    print(f"\nSaved value table: {table_path}")

    reg_table_path = os.path.join(tables_dir, f"forecast_value_regression_{tag}.csv")
    reg_results.to_csv(reg_table_path, index=False)

    # Step 6: Plots
    print("\nStep 6: Generating maps...")
    plot_forecast_value_map(
        value_df,
        save_path=os.path.join(save_dir, f"forecast_value_by_error_{tag}.png"),
        title_prefix=f"Forecast Value ({depvar})",
    )
    plot_total_value_map(
        value_df,
        save_path=os.path.join(save_dir, f"forecast_value_total_{tag}.png"),
        title=f"Total Forecast Value ({depvar})",
    )

    return {
        "value_df": value_df,
        "regression_results": reg_results,
        "error_stats": error_stats,
    }


def run_regime_value_comparison(months=None, depvar="first_interval_shadow_cost",
                                 save_dir=None):
    """Compare forecast value across weather regimes.

    Runs the full value analysis for each regime and for the full sample,
    then creates a comparison figure.

    Args:
        months: List of (year, month) tuples.
        depvar: Congestion dependent variable.
        save_dir: Directory for figures.

    Returns:
        Dict mapping regime_name → value_df.
    """
    if months is None:
        months = DEFAULT_MONTHS

    dirs = setup_directories()
    if save_dir is None:
        save_dir = os.path.join(dirs["figures"], "forecast_value")
    os.makedirs(save_dir, exist_ok=True)

    from process_data.classify_weather_regimes import classify_regimes
    from analysis.pixel_regression_maps import REGIMES, filter_to_regime

    print("Loading pixel data...")
    df = load_pixel_data(months)
    df = classify_regimes(df)

    error_stats = compute_pixel_error_stats(df)
    regime_values = {}

    for regime_name in ["extreme_cold", "extreme_heat", "stressed_grid"]:
        spec = REGIMES[regime_name]
        print(f"\n{'='*60}")
        print(f"Regime: {spec['label']}")
        print(f"{'='*60}")

        regime_df = filter_to_regime(df, regime_name)
        if regime_df["valid_time"].nunique() < 30:
            print(f"  Too few hours, skipping")
            continue

        reg_results = run_pixel_regressions(
            regime_df, depvar=depvar,
            error_vars=WEATHER_ERROR_VARS,
            controls=WEATHER_CONTROLS,
            min_obs=30,
        )

        if reg_results.empty:
            continue

        # Compute regime-specific error stats
        regime_error_stats = compute_pixel_error_stats(regime_df)
        value_df = compute_forecast_value(reg_results, regime_error_stats)
        regime_values[regime_name] = value_df

        plot_total_value_map(
            value_df,
            save_path=os.path.join(
                save_dir, f"forecast_value_{regime_name}_{depvar}.png"
            ),
            title=f"Forecast Value — {spec['label']}",
        )

    return regime_values


# ── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Forecast value map analysis"
    )
    parser.add_argument(
        "--months", type=int, nargs="+", default=None,
        help="Months (e.g. --months 1 2 7). Default: all 12.",
    )
    parser.add_argument(
        "--depvar", type=str, default="total_shadow_cost",
        help="Congestion dependent variable.",
    )
    parser.add_argument(
        "--regimes", action="store_true",
        help="Also run regime comparison.",
    )
    args = parser.parse_args()

    if args.months:
        months = [(2025, m) for m in args.months]
    else:
        months = DEFAULT_MONTHS

    results = run_forecast_value_analysis(months, args.depvar)

    if args.regimes:
        run_regime_value_comparison(months, args.depvar)
