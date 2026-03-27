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
    DEPVAR,
    SIG_LEVEL,
)

# ── Constants ────────────────────────────────────────────────────────────────

WEATHER_ERROR_VARS = [
    "temp_error_1h", "wspd_error_1h", "temp_error_0h", "wspd_error_0h",
]
WEATHER_CONTROLS = ["era5_temp", "era5_wspd", "is_weekend"]

CONGESTION_DEPVARS = [
    "total_shadow_cost",
    "max_shadow_price",
    "n_binding_constraints",
    "system_lmp_std",
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


# ── Main analysis functions ──────────────────────────────────────────────────

def run_regime_regressions(months=None, depvar="total_shadow_cost",
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

    return all_results


def run_asymmetry_regressions(months=None, depvar="total_shadow_cost",
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
        "--depvar", type=str, default="total_shadow_cost",
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
