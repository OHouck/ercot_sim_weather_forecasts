"""Regime-conditional pixel regressions for extreme weather events.

For each weather regime (extreme cold, extreme heat, high wind, stressed grid),
runs per-pixel regressions of congestion measures on forecast errors.

Key question: During extreme weather events, where on the grid do forecast
errors have the largest effect on congestion?

Usage:
    uv run python -m analysis.extreme_weather_regressions
    uv run python -m analysis.extreme_weather_regressions --months 7 --depvar total_shadow_cost
"""

import argparse
import os
import sys
from pathlib import Path

import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories

# Reuse existing pixel regression infrastructure
from analysis.pixel_regression_maps import (
    load_pixel_data,
    run_pixel_regressions,
    plot_pixel_coefficient_map,
    _draw_texas_base,
    _draw_texas_borders,
    DEPVAR,
    SIG_LEVEL,
)

# ── Constants ────────────────────────────────────────────────────────────────

WEATHER_ERROR_VARS = [
    "temp_error_1h", "wspd_error_1h", "temp_error_0h", "wspd_error_0h",
]
WEATHER_CONTROLS = ["era5_temp", "era5_wspd", "is_weekend"]

CONGESTION_DEPVARS = [
    "first_interval_shadow_cost",
    "total_shadow_cost",
    "max_shadow_price",
    "n_binding_constraints",
    "system_lmp_std",
    "wind_curtailment_mw",
    "solar_curtailment_mw",
    "total_curtailment_mw",
]

DEFAULT_MONTHS = [(2025, m) for m in range(1, 13)]

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


# ── Helpers ──────────────────────────────────────────────────────────────────

def add_regime_columns(df):
    """Add weather/grid regime columns using system-wide hourly aggregates.

    Computes thresholds from the full dataset and classifies every hour.
    """
    from process_data.classify_weather_regimes import classify_regimes
    return classify_regimes(df)


def filter_to_regime(df, regime_name):
    """Filter DataFrame to hours matching a specific regime.

    Args:
        df: DataFrame with regime columns.
        regime_name: Key in REGIMES dict.

    Returns:
        Filtered DataFrame.
    """
    spec = REGIMES[regime_name]
    mask = df[spec["filter_col"]] == spec["filter_val"]
    filtered = df[mask].copy()
    n_hours = filtered["valid_time"].nunique()
    print(f"  Regime '{regime_name}': {n_hours} hours, "
          f"{len(filtered):,} rows")
    return filtered


# ── Asymmetry helpers ────────────────────────────────────────────────────────

def add_asymmetric_vars(df):
    """Split each weather error into positive (over-forecast) and negative
    (under-forecast) components.

    Positive error = forecast > observed (over-predicted).
    Negative error = forecast < observed (under-predicted).
    """
    df = df.copy()
    for var in WEATHER_ERROR_VARS:
        df[f"{var}_pos"] = df[var].clip(lower=0)
        df[f"{var}_neg"] = df[var].clip(upper=0)
    return df


ASYMMETRIC_ERROR_VARS = [
    "temp_error_1h_pos", "temp_error_1h_neg",
    "wspd_error_1h_pos", "wspd_error_1h_neg",
    "temp_error_0h_pos", "temp_error_0h_neg",
    "wspd_error_0h_pos", "wspd_error_0h_neg",
]


# ── Visualisation helpers ────────────────────────────────────────────────────

_PANEL_CONFIG = [
    ("temp_error_1h", "HRRR 1h — Temperature Error"),
    ("wspd_error_1h", "HRRR 1h — Wind Speed Error"),
    ("temp_error_0h", "GFS Day-Ahead — Temperature Error"),
    ("wspd_error_0h", "GFS Day-Ahead — Wind Speed Error"),
]


def plot_regime_coefficient_maps(all_results, save_dir, dirs, depvar,
                                  sig_level=SIG_LEVEL):
    """Create a 2×2 coefficient map for each regime.

    Parameters
    ----------
    all_results : dict
        Mapping regime_name → results DataFrame (output of run_regime_regressions).
    save_dir : str
        Directory for output PNG files.
    dirs : dict
        Output of setup_directories().
    depvar : str
        Dependent variable name (used in filename and title).
    sig_level : float
        p-value threshold.
    """
    os.makedirs(save_dir, exist_ok=True)
    depvar_label = depvar.replace("_", " ")

    for regime_name, results_df in all_results.items():
        spec = REGIMES[regime_name]
        regime_label = spec["label"]

        if results_df.empty:
            continue

        # Shared color limits across all four panels for this regime
        sig_mask = results_df["pvalue"] < sig_level
        if sig_mask.sum() > 0:
            clim = np.nanpercentile(results_df.loc[sig_mask, "coef"].abs(), 99)
        else:
            clim = 1.0
        vmin, vmax = -clim, clim

        fig, axes = plt.subplots(
            2, 2,
            figsize=(18, 14),
            subplot_kw={"projection": ccrs.PlateCarree()},
            gridspec_kw={"hspace": 0.15, "wspace": 0.05},
        )

        sc_last = None
        legend_handles = []
        for idx, (err_var, panel_title) in enumerate(_PANEL_CONFIG):
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
                overlay=["wind", "solar", "transmission", "cities"],
                sig_level=sig_level,
            )
            if sc is not None:
                sc_last = sc
            if idx == 0:
                legend_handles = handles

        if sc_last is not None:
            fig.colorbar(
                sc_last,
                ax=axes.ravel().tolist(),
                shrink=0.6,
                label=f"Coefficient ({depvar_label} per unit error)",
                pad=0.02,
            )

        if legend_handles:
            fig.legend(
                handles=legend_handles,
                loc="lower center",
                ncol=len(legend_handles),
                fontsize=9,
                framealpha=0.85,
                bbox_to_anchor=(0.45, 0.01),
            )

        fig.suptitle(
            f"Regime: {regime_label}\nDependent variable: {depvar_label}\n"
            "(only significant pixels shown, p < 0.05)",
            fontsize=13,
            y=0.98,
        )

        save_path = os.path.join(
            save_dir, f"regime_coef_maps_{regime_name}_{depvar}.png"
        )
        fig.savefig(save_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        print(f"  Regime map saved: {save_path}")


def plot_asymmetry_histograms(results_df, save_path, depvar_label="total shadow cost",
                               sig_level=SIG_LEVEL):
    """4-panel histogram comparing over- vs under-forecast significant coefficients.

    Parameters
    ----------
    results_df : pd.DataFrame
        Output of run_asymmetry_regressions() — contains _pos and _neg error vars.
    save_path : str
        Path to save the figure.
    depvar_label : str
        Human-readable label for the dependent variable.
    sig_level : float
        p-value threshold for significance.
    """
    base_vars = ["temp_error_1h", "wspd_error_1h", "temp_error_0h", "wspd_error_0h"]
    var_labels = {
        "temp_error_1h": "HRRR 1h Temperature Error",
        "wspd_error_1h": "HRRR 1h Wind Speed Error",
        "temp_error_0h": "GFS Day-Ahead Temperature Error",
        "wspd_error_0h": "GFS Day-Ahead Wind Speed Error",
    }

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle(
        f"Forecast Error Asymmetry — {depvar_label}\n"
        "Distribution of significant pixel coefficients by direction",
        fontsize=13, fontweight="bold",
    )

    for ax, base_var in zip(axes.flat, base_vars):
        pos_var = f"{base_var}_pos"
        neg_var = f"{base_var}_neg"

        pos_sig = results_df[
            (results_df["error_var"] == pos_var) & (results_df["pvalue"] < sig_level)
        ]["coef"]
        neg_sig = results_df[
            (results_df["error_var"] == neg_var) & (results_df["pvalue"] < sig_level)
        ]["coef"]

        # Determine shared bin range
        all_vals = pd.concat([pos_sig, neg_sig])
        if len(all_vals) == 0:
            ax.set_title(var_labels.get(base_var, base_var))
            ax.text(0.5, 0.5, "No significant pixels", ha="center", va="center",
                    transform=ax.transAxes, color="gray")
            continue

        lo = np.percentile(all_vals, 1)
        hi = np.percentile(all_vals, 99)
        bins = np.linspace(lo, hi, 30)

        if len(pos_sig) > 0:
            ax.hist(pos_sig, bins=bins, alpha=0.55, color="steelblue",
                    label=f"Over-forecast (+), n={len(pos_sig):,}")
        if len(neg_sig) > 0:
            ax.hist(neg_sig, bins=bins, alpha=0.55, color="firebrick",
                    label=f"Under-forecast (−), n={len(neg_sig):,}")

        ax.axvline(0, color="black", linewidth=0.8, linestyle="--")
        ax.set_xlabel(f"Coefficient (per unit error)", fontsize=9)
        ax.set_ylabel("Pixel count", fontsize=9)
        ax.set_title(var_labels.get(base_var, base_var), fontsize=11)
        ax.legend(fontsize=8)

    fig.tight_layout(rect=[0, 0, 1, 0.94])
    os.makedirs(os.path.dirname(save_path) if os.path.dirname(save_path) else ".", exist_ok=True)
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Asymmetry histograms saved: {save_path}")


# ── Main analysis functions ──────────────────────────────────────────────────

def run_regime_regressions(months=None, depvar="first_interval_shadow_cost",
                            save_dir=None):
    """Run per-pixel regressions for each weather regime.

    Args:
        months: List of (year, month) tuples.
        depvar: Dependent variable (congestion measure).
        save_dir: Directory for figures/tables. Defaults to OneDrive figures.

    Returns:
        Dict mapping regime_name → results DataFrame.
    """
    if months is None:
        months = DEFAULT_MONTHS

    dirs = setup_directories()
    if save_dir is None:
        save_dir = os.path.join(dirs["figures"], "extreme_weather")
    tables_dir = os.path.join(dirs["root"], "tables")
    os.makedirs(save_dir, exist_ok=True)
    os.makedirs(tables_dir, exist_ok=True)

    # Load data and add regime columns
    print("Loading pixel data...")
    df = load_pixel_data(months)
    print("Adding regime classifications...")
    df = add_regime_columns(df)

    all_results = {}
    for regime_name, spec in REGIMES.items():
        print(f"\n{'='*60}")
        print(f"Regime: {spec['label']}")
        print(f"{'='*60}")

        regime_df = filter_to_regime(df, regime_name)

        if regime_df["valid_time"].nunique() < 20:
            print(f"  Too few hours ({regime_df['valid_time'].nunique()}), "
                  f"skipping")
            continue

        # Run per-pixel regressions (use lower min_obs for extreme regimes)
        results = run_pixel_regressions(
            regime_df,
            depvar=depvar,
            error_vars=WEATHER_ERROR_VARS,
            controls=WEATHER_CONTROLS,
            min_obs=50,
        )

        if results.empty:
            print(f"  No significant results for {regime_name}")
            continue

        all_results[regime_name] = results

        # Save results table
        tag = f"{depvar}_{regime_name}"
        table_path = os.path.join(
            tables_dir, f"extreme_weather_regression_{tag}.csv"
        )
        results.to_csv(table_path, index=False)
        print(f"  Saved: {table_path}")

        # Print summary
        for ev in results.error_var.unique():
            sub = results[results.error_var == ev]
            sig = sub[sub.pvalue < SIG_LEVEL]
            pct = len(sig) / len(sub) * 100 if len(sub) > 0 else 0
            mean_c = sig.coef.mean() if len(sig) > 0 else float("nan")
            print(f"    {ev}: {len(sig)}/{len(sub)} sig ({pct:.0f}%), "
                  f"mean coef={mean_c:.2f}")

    # Generate coefficient maps for each regime
    if all_results:
        print("\nGenerating regime coefficient maps...")
        plot_regime_coefficient_maps(all_results, save_dir, dirs, depvar)

    return all_results


def run_asymmetry_regressions(months=None, depvar="first_interval_shadow_cost",
                               regime_name=None, save_dir=None):
    """Test whether over- and under-forecasts have symmetric effects.

    Splits each error variable into positive and negative components,
    then runs per-pixel regressions.

    Args:
        months: List of (year, month) tuples.
        depvar: Dependent variable.
        regime_name: Optional regime filter (None = full sample).
        save_dir: Output directory.

    Returns:
        DataFrame of asymmetric regression results.
    """
    if months is None:
        months = DEFAULT_MONTHS

    dirs = setup_directories()
    if save_dir is None:
        save_dir = os.path.join(dirs["figures"], "asymmetry")
    tables_dir = os.path.join(dirs["root"], "tables")
    os.makedirs(save_dir, exist_ok=True)
    os.makedirs(tables_dir, exist_ok=True)

    print("Loading pixel data...")
    df = load_pixel_data(months)

    if regime_name:
        df = add_regime_columns(df)
        df = filter_to_regime(df, regime_name)

    # Add asymmetric variables
    df = add_asymmetric_vars(df)

    min_obs = 50 if regime_name else 100
    results = run_pixel_regressions(
        df,
        depvar=depvar,
        error_vars=ASYMMETRIC_ERROR_VARS,
        controls=WEATHER_CONTROLS,
        min_obs=min_obs,
    )

    if results.empty:
        print("No significant results for asymmetry analysis.")
        return results

    # Summary
    tag = f"asymmetry_{depvar}"
    if regime_name:
        tag += f"_{regime_name}"

    table_path = os.path.join(tables_dir, f"{tag}.csv")
    results.to_csv(table_path, index=False)
    print(f"\nSaved: {table_path}")

    # Generate asymmetry histogram
    hist_path = os.path.join(save_dir, f"asymmetry_histograms_{depvar}.png")
    if regime_name:
        hist_path = os.path.join(save_dir, f"asymmetry_histograms_{depvar}_{regime_name}.png")
    depvar_label = depvar.replace("_", " ")
    plot_asymmetry_histograms(results, hist_path, depvar_label=depvar_label)

    # Compare positive vs negative effects
    print("\n=== Asymmetry Summary ===")
    for base_var in WEATHER_ERROR_VARS:
        pos_var = f"{base_var}_pos"
        neg_var = f"{base_var}_neg"
        pos_results = results[(results.error_var == pos_var) & (results.pvalue < SIG_LEVEL)]
        neg_results = results[(results.error_var == neg_var) & (results.pvalue < SIG_LEVEL)]
        print(f"\n  {base_var}:")
        print(f"    Over-forecast (pos): {len(pos_results)} sig pixels, "
              f"mean coef = {pos_results.coef.mean():.2f}" if len(pos_results) > 0 else
              f"    Over-forecast (pos): 0 sig pixels")
        print(f"    Under-forecast (neg): {len(neg_results)} sig pixels, "
              f"mean coef = {neg_results.coef.mean():.2f}" if len(neg_results) > 0 else
              f"    Under-forecast (neg): 0 sig pixels")

    return results


# ── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Regime-conditional pixel regressions"
    )
    parser.add_argument(
        "--months", type=int, nargs="+", default=None,
        help="Months to include (e.g. --months 1 2 12). Default: all 12.",
    )
    parser.add_argument(
        "--depvar", type=str, default="first_interval_shadow_cost",
        choices=CONGESTION_DEPVARS,
        help="Dependent variable.",
    )
    parser.add_argument(
        "--asymmetry", action="store_true",
        help="Run asymmetry analysis instead of regime regressions.",
    )
    parser.add_argument(
        "--regime", type=str, default=None,
        choices=list(REGIMES.keys()),
        help="Regime filter for asymmetry analysis.",
    )
    args = parser.parse_args()

    if args.months:
        months = [(2025, m) for m in args.months]
    else:
        months = DEFAULT_MONTHS

    if args.asymmetry:
        run_asymmetry_regressions(months, args.depvar, args.regime)
    else:
        run_regime_regressions(months, args.depvar)
