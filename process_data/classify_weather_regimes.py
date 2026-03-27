"""Classify each hour into weather/grid regimes for extreme-event analysis.

Regimes are defined by system-wide percentile thresholds computed from the
full 2025 dataset. Each hour receives labels in multiple dimensions:

- regime_temp:  'extreme_cold' | 'extreme_heat' | 'normal'
- regime_wind:  'high_wind' | 'low_wind' | 'normal'
- regime_grid:  'stressed' | 'normal'  (based on LMP max)
- is_extreme:   1 if any non-normal regime, 0 otherwise

Usage:
    from process_data.classify_weather_regimes import classify_regimes
    df = classify_regimes(pixel_df)  # adds regime columns in-place
"""

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from helper_funcs import setup_directories

# Default percentile thresholds
TEMP_COLD_PCT = 0.05      # bottom 5% of system-avg temp → extreme cold
TEMP_HEAT_PCT = 0.95      # top 5% → extreme heat
WIND_HIGH_PCT = 0.90      # top 10% → high wind
WIND_LOW_PCT = 0.10       # bottom 10% → low wind
LMP_STRESS_PCT = 0.95     # top 5% of system_lmp_max → stressed grid


def compute_hourly_system_weather(pixel_df, time_col="valid_time"):
    """Compute system-wide hourly average weather from pixel data.

    Args:
        pixel_df: Pixel × hour DataFrame with era5_temp, era5_wspd columns.
        time_col: Name of the time column.

    Returns:
        DataFrame with one row per hour: time_col, sys_temp, sys_wspd,
        sys_lmp_max.
    """
    hourly = (
        pixel_df.groupby(time_col)
        .agg(
            sys_temp=("era5_temp", "mean"),
            sys_wspd=("era5_wspd", "mean"),
            sys_lmp_max=("system_lmp_max", "first"),
        )
        .reset_index()
    )
    return hourly


def compute_thresholds(hourly_weather, thresholds=None):
    """Compute percentile thresholds from hourly system weather.

    Args:
        hourly_weather: DataFrame with sys_temp, sys_wspd, sys_lmp_max.
        thresholds: Optional dict overriding default percentiles.

    Returns:
        Dict of threshold values.
    """
    if thresholds is None:
        thresholds = {}

    cold_pct = thresholds.get("temp_cold_pct", TEMP_COLD_PCT)
    heat_pct = thresholds.get("temp_heat_pct", TEMP_HEAT_PCT)
    wind_high_pct = thresholds.get("wind_high_pct", WIND_HIGH_PCT)
    wind_low_pct = thresholds.get("wind_low_pct", WIND_LOW_PCT)
    lmp_stress_pct = thresholds.get("lmp_stress_pct", LMP_STRESS_PCT)

    result = {
        "temp_cold": hourly_weather["sys_temp"].quantile(cold_pct),
        "temp_heat": hourly_weather["sys_temp"].quantile(heat_pct),
        "wind_high": hourly_weather["sys_wspd"].quantile(wind_high_pct),
        "wind_low": hourly_weather["sys_wspd"].quantile(wind_low_pct),
        "lmp_stress": hourly_weather["sys_lmp_max"].quantile(lmp_stress_pct),
    }

    print(f"  Regime thresholds (from {len(hourly_weather)} hours):")
    print(f"    Extreme cold: sys_temp < {result['temp_cold']:.1f} °C "
          f"({cold_pct*100:.0f}th pctile)")
    print(f"    Extreme heat: sys_temp > {result['temp_heat']:.1f} °C "
          f"({heat_pct*100:.0f}th pctile)")
    print(f"    High wind:    sys_wspd > {result['wind_high']:.2f} m/s "
          f"({wind_high_pct*100:.0f}th pctile)")
    print(f"    Low wind:     sys_wspd < {result['wind_low']:.2f} m/s "
          f"({wind_low_pct*100:.0f}th pctile)")
    print(f"    Stressed grid: sys_lmp_max > ${result['lmp_stress']:.0f}/MWh "
          f"({lmp_stress_pct*100:.0f}th pctile)")

    return result


def classify_hourly(hourly_weather, thresholds_dict):
    """Assign regime labels to hourly system weather.

    Args:
        hourly_weather: DataFrame with sys_temp, sys_wspd, sys_lmp_max.
        thresholds_dict: Output of compute_thresholds().

    Returns:
        DataFrame with regime columns added.
    """
    hw = hourly_weather.copy()

    # Temperature regime
    hw["regime_temp"] = "normal"
    hw.loc[hw["sys_temp"] < thresholds_dict["temp_cold"], "regime_temp"] = "extreme_cold"
    hw.loc[hw["sys_temp"] > thresholds_dict["temp_heat"], "regime_temp"] = "extreme_heat"

    # Wind regime
    hw["regime_wind"] = "normal"
    hw.loc[hw["sys_wspd"] > thresholds_dict["wind_high"], "regime_wind"] = "high_wind"
    hw.loc[hw["sys_wspd"] < thresholds_dict["wind_low"], "regime_wind"] = "low_wind"

    # Grid stress regime
    hw["regime_grid"] = "normal"
    hw.loc[hw["sys_lmp_max"] > thresholds_dict["lmp_stress"], "regime_grid"] = "stressed"

    # Combined flag
    hw["is_extreme"] = (
        (hw["regime_temp"] != "normal") |
        (hw["regime_wind"] != "normal") |
        (hw["regime_grid"] != "normal")
    ).astype(int)

    # Print summary
    for col in ["regime_temp", "regime_wind", "regime_grid"]:
        counts = hw[col].value_counts()
        print(f"  {col}: {counts.to_dict()}")
    print(f"  is_extreme=1: {hw['is_extreme'].sum()} hours "
          f"({hw['is_extreme'].mean()*100:.1f}%)")

    return hw


def classify_regimes(pixel_df, time_col="valid_time", thresholds=None):
    """Add weather/grid regime columns to a pixel × hour DataFrame.

    Computes system-wide hourly weather, determines percentile thresholds,
    assigns regime labels, and merges back to the pixel-level data.

    Args:
        pixel_df: Pixel × hour DataFrame.
        time_col: Name of the time column.
        thresholds: Optional dict overriding default percentiles.

    Returns:
        DataFrame with regime columns added (modified in place and returned).
    """
    print("Classifying weather regimes...")
    hourly_weather = compute_hourly_system_weather(pixel_df, time_col)
    thresh = compute_thresholds(hourly_weather, thresholds)
    hourly_regimes = classify_hourly(hourly_weather, thresh)

    # Merge regime labels back to pixel data
    regime_cols = [time_col, "regime_temp", "regime_wind", "regime_grid",
                   "is_extreme", "sys_temp", "sys_wspd"]
    pixel_df = pixel_df.merge(
        hourly_regimes[regime_cols],
        on=time_col,
        how="left",
    )

    return pixel_df


def classify_regimes_from_thresholds(pixel_df, thresholds_dict,
                                      time_col="valid_time"):
    """Apply pre-computed thresholds to classify regimes.

    Use this when thresholds were computed on the full year but you are
    applying them to a single-month subset.

    Args:
        pixel_df: Pixel × hour DataFrame.
        thresholds_dict: Output of compute_thresholds() (from full year).
        time_col: Name of the time column.

    Returns:
        DataFrame with regime columns added.
    """
    hourly_weather = compute_hourly_system_weather(pixel_df, time_col)
    hourly_regimes = classify_hourly(hourly_weather, thresholds_dict)

    regime_cols = [time_col, "regime_temp", "regime_wind", "regime_grid",
                   "is_extreme", "sys_temp", "sys_wspd"]
    pixel_df = pixel_df.merge(
        hourly_regimes[regime_cols],
        on=time_col,
        how="left",
    )
    return pixel_df
