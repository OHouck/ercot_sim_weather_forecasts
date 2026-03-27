"""
Binscatter comparison of HRRR vs GFS forecast errors.

Compares 1-hour HRRR errors to day-ahead GFS errors for both
2m temperature and 10m wind speed, showing joint error distributions.

Usage:
    uv run python -m analysis.hrrr_gfs_error_correlation
"""

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories

ROOT = Path(__file__).resolve().parent.parent

DEFAULT_MONTHS = [(2025, m) for m in range(1, 13)]
PLOT_MIN = -5
PLOT_MAX = 5


def load_pixel_data_simple(months):
    """Load pixel hourly data and return relevant error columns.

    Parameters
    ----------
    months : list of (year, month) tuples

    Returns
    -------
    pd.DataFrame with columns:
        - temp_error_1h, temp_error_0h
        - wspd_error_1h, wspd_error_0h
    """
    dirs = setup_directories()
    data_dir = Path(dirs["processed"]) / "combined_hourly_gridded_data"

    dfs = []
    for year, month in months:
        fname = data_dir / f"pixel_hourly_gfs+hrrr_{year}_{month:02d}.parquet"
        if not fname.exists():
            print(f"  [WARNING] Missing: {fname}")
            continue

        df_month = pd.read_parquet(
            fname,
            columns=[
                "temp_error_1h", "temp_error_0h",
                "wspd_error_1h", "wspd_error_0h",
            ]
        )
        dfs.append(df_month)
        print(f"  Loaded {year}-{month:02d}: {len(df_month):,} rows")

    if not dfs:
        raise FileNotFoundError("No monthly parquet files found.")

    df = pd.concat(dfs, ignore_index=True)
    print(f"\nLoaded {len(df):,} total rows")
    return df


def create_error_hexbinscatter(
    x, y, xlabel, ylabel, title,
    ax=None, cmap="viridis", gridsize=30,
):
    """Create a hexbin hexbinscatter plot of forecast errors.

    Parameters
    ----------
    x, y : np.ndarray
        Error values for x and y axes.
    xlabel, ylabel : str
        Axis labels.
    title : str
        Plot title.
    ax : matplotlib Axes, optional
        If None, creates a new figure.
    cmap : str
        Colormap for hexbin.
    gridsize : int
        Hexbin gridsize (number of bins along x-axis).

    Returns
    -------
    ax, collection
    """
    if ax is None:
        fig, ax = plt.subplots(figsize=(8, 8))

    # Remove NaNs
    mask = ~(np.isnan(x) | np.isnan(y))
    x_clean = x[mask]
    y_clean = y[mask]

    # Focus density coloring on the central error mass only.
    in_bounds = (
        (x_clean >= PLOT_MIN) & (x_clean <= PLOT_MAX)
        & (y_clean >= PLOT_MIN) & (y_clean <= PLOT_MAX)
    )
    x_plot = x_clean[in_bounds]
    y_plot = y_clean[in_bounds]

    if len(x_plot) == 0:
        raise ValueError("No observations inside plotting bounds [-5, 5].")

    # Create hexbin
    hbm = ax.hexbin(
        x_plot, y_plot,
        gridsize=gridsize,
        cmap=cmap,
        extent=(PLOT_MIN, PLOT_MAX, PLOT_MIN, PLOT_MAX),
        mincnt=1,
        edgecolors='face',
        linewidths=0.2,
    )

    # Add diagonal reference line
    ax.plot([PLOT_MIN, PLOT_MAX], [PLOT_MIN, PLOT_MAX], 'k--', linewidth=1, alpha=0.4, label='1:1 line')

    # Labels and title
    ax.set_xlabel(xlabel, fontsize=11)
    ax.set_ylabel(ylabel, fontsize=11)
    ax.set_xlim(PLOT_MIN, PLOT_MAX)
    ax.set_ylim(PLOT_MIN, PLOT_MAX)
    ax.set_title(title, fontsize=12, fontweight='bold')
    ax.grid(True, alpha=0.3)
    ax.legend(loc='upper left', fontsize=9)

    # Colorbar
    cbar = plt.colorbar(hbm, ax=ax)
    cbar.set_label('Count', fontsize=10)

    # Add statistics text
    corr = np.corrcoef(x_plot, y_plot)[0, 1]
    mae_x = np.mean(np.abs(x_plot))
    mae_y = np.mean(np.abs(y_plot))
    rmse_x = np.sqrt(np.mean(x_plot ** 2))
    rmse_y = np.sqrt(np.mean(y_plot ** 2))
    stats_text = (
        f"Correlation: {corr:.3f}\n"
        f"MAE (x): {mae_x:.3f}  |  MAE (y): {mae_y:.3f}\n"
        f"RMSE (x): {rmse_x:.3f}  |  RMSE (y): {rmse_y:.3f}\n"
        f"n (in-range) = {len(x_plot):,}"
    )
    ax.text(
        0.98, 0.05,
        stats_text,
        transform=ax.transAxes,
        fontsize=9,
        verticalalignment='bottom',
        horizontalalignment='right',
        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8),
        family='monospace',
    )

    return ax, hbm


def run_error_comparison_binscatter(
    months=None, save_dir=None,
):
    """Generate 1×2 binscatter figure comparing HRRR vs GFS errors.

    Parameters
    ----------
    months : list of (year, month) tuples, optional
        Defaults to full 2025.
    save_dir : str or Path, optional
        Output directory. Defaults to {figures}/error_comparison/.

    Returns
    -------
    dict with key 'figure' (Path to saved PNG)
    """
    if months is None:
        months = DEFAULT_MONTHS

    dirs = setup_directories()

    if save_dir is None:
        save_dir = Path(dirs["figures"]) / "error_comparison"
    else:
        save_dir = Path(save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)

    # --- Load data ---
    print("Loading pixel hourly data...")
    df = load_pixel_data_simple(months)

    # --- Create 1×2 figure ---
    fig, axes = plt.subplots(1, 2, figsize=(16, 7))

    # Temperature error: HRRR 1h vs GFS 0h
    ax1, _ = create_error_hexbinscatter(
        df["temp_error_1h"].values,
        df["temp_error_0h"].values,
        xlabel="HRRR 1h Temperature Error (°C)",
        ylabel="GFS Day-Ahead Temperature Error (°C)",
        title="Temperature Error Comparison\n(HRRR 1h vs GFS Day-Ahead)",
        ax=axes[0],
        gridsize=35,
    )

    # Wind speed error: HRRR 1h vs GFS 0h
    ax2, _ = create_error_hexbinscatter(
        df["wspd_error_1h"].values,
        df["wspd_error_0h"].values,
        xlabel="HRRR 1h Wind Speed Error (m/s)",
        ylabel="GFS Day-Ahead Wind Speed Error (m/s)",
        title="Wind Speed Error Comparison\n(HRRR 1h vs GFS Day-Ahead)",
        ax=axes[1],
        gridsize=35,
    )

    fig.suptitle(
        "HRRR vs GFS Forecast Error Comparison (2025, all Texas pixels)",
        fontsize=14,
        fontweight='bold',
        y=0.98,
    )
    plt.tight_layout(rect=[0, 0, 1, 0.96])

    # --- Save figure ---
    save_path = save_dir / "error_binscatter_1x2.png"
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"\nBinscatter figure saved to: {save_path}")

    return {"figure": save_path}


if __name__ == "__main__":
    print("=== HRRR vs GFS Error Binscatter Comparison ===\n")
    run_error_comparison_binscatter()
    pass