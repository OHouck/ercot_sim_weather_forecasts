"""Asymmetric forecast error analysis for ERCOT congestion.

Tests whether over-forecasts and under-forecasts have symmetric effects on
congestion and curtailment by splitting each error variable into positive
(over-forecast) and negative (under-forecast) components, then running
per-pixel regressions.

Optionally restricts to an extreme weather regime for conditional analysis.

Usage:
    uv run python -m analysis.asymmetric_forecast_error_analysis
    uv run python -m analysis.asymmetric_forecast_error_analysis --depvar total_shadow_cost
    uv run python -m analysis.asymmetric_forecast_error_analysis --regime extreme_heat
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

from analysis.pixel_regression_maps import (
    load_pixel_data,
    run_pixel_regressions,
    add_regime_columns,
    filter_to_regime,
    REGIMES,
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

ASYMMETRIC_ERROR_VARS = [
    "temp_error_1h_pos", "temp_error_1h_neg",
    "wspd_error_1h_pos", "wspd_error_1h_neg",
    "temp_error_0h_pos", "temp_error_0h_neg",
    "wspd_error_0h_pos", "wspd_error_0h_neg",
]


# ── Helpers ──────────────────────────────────────────────────────────────────

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


# ── Visualisation ────────────────────────────────────────────────────────────

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
        ax.set_xlabel("Coefficient (per unit error)", fontsize=9)
        ax.set_ylabel("Pixel count", fontsize=9)
        ax.set_title(var_labels.get(base_var, base_var), fontsize=11)
        ax.legend(fontsize=8)

    fig.tight_layout(rect=[0, 0, 1, 0.94])
    os.makedirs(os.path.dirname(save_path) if os.path.dirname(save_path) else ".", exist_ok=True)
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Asymmetry histograms saved: {save_path}")


# ── Main analysis ────────────────────────────────────────────────────────────

def run_asymmetry_regressions(months=None, depvar="first_interval_shadow_cost",
                               regime_name=None, save_dir=None):
    """Test whether over- and under-forecasts have symmetric effects on congestion.

    Splits each error variable into positive (over-forecast) and negative
    (under-forecast) components, then runs per-pixel regressions.

    Parameters
    ----------
    months : list of (year, month) tuples, optional
        Defaults to all of 2025.
    depvar : str
        Dependent variable (congestion measure).
    regime_name : str, optional
        Restrict to an extreme weather regime (None = full sample).
        One of: 'extreme_cold', 'extreme_heat', 'high_wind', 'stressed_grid'.
    save_dir : str, optional
        Output directory for figures and tables.

    Returns
    -------
    pd.DataFrame
        Per-pixel asymmetric regression results.
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

    if regime_name is not None:
        if regime_name not in REGIMES:
            raise ValueError(
                f"Unknown regime '{regime_name}'. Choose from: {list(REGIMES.keys())}"
            )
        print(f"\nApplying regime filter: {REGIMES[regime_name]['label']}")
        df = add_regime_columns(df)
        df = filter_to_regime(df, regime_name)

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

    tag = f"asymmetry_{depvar}"
    if regime_name:
        tag += f"_{regime_name}"

    table_path = os.path.join(tables_dir, f"{tag}.csv")
    results.to_csv(table_path, index=False)
    print(f"\nSaved: {table_path}")

    hist_path = os.path.join(save_dir, f"asymmetry_histograms_{depvar}.png")
    if regime_name:
        hist_path = os.path.join(save_dir, f"asymmetry_histograms_{depvar}_{regime_name}.png")
    depvar_label = depvar.replace("_", " ")
    if regime_name:
        depvar_label += f" — {REGIMES[regime_name]['label']}"
    plot_asymmetry_histograms(results, hist_path, depvar_label=depvar_label)

    print("\n=== Asymmetry Summary ===")
    for base_var in WEATHER_ERROR_VARS:
        pos_var = f"{base_var}_pos"
        neg_var = f"{base_var}_neg"
        pos_results = results[(results.error_var == pos_var) & (results.pvalue < SIG_LEVEL)]
        neg_results = results[(results.error_var == neg_var) & (results.pvalue < SIG_LEVEL)]
        print(f"\n  {base_var}:")
        print(
            f"    Over-forecast (pos): {len(pos_results)} sig pixels, "
            f"mean coef = {pos_results.coef.mean():.2f}" if len(pos_results) > 0 else
            f"    Over-forecast (pos): 0 sig pixels"
        )
        print(
            f"    Under-forecast (neg): {len(neg_results)} sig pixels, "
            f"mean coef = {neg_results.coef.mean():.2f}" if len(neg_results) > 0 else
            f"    Under-forecast (neg): 0 sig pixels"
        )

    return results


# ── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Asymmetric forecast error analysis"
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
        "--regime", type=str, default=None,
        choices=list(REGIMES.keys()),
        help="Optional regime filter.",
    )
    args = parser.parse_args()

    months = [(2025, m) for m in args.months] if args.months else DEFAULT_MONTHS
    run_asymmetry_regressions(months, args.depvar, args.regime)
