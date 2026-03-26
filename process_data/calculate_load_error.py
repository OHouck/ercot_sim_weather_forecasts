"""
Load forecast error calculation for ERCOT weather-zone demand data.

Builds a weather-zone × hour DataFrame with realized load, forecast loads at
configurable lead times, and forecast errors.  Supports both lead-time-based
forecasts (e.g. "1 hour ahead") and time-of-day-based forecasts (e.g. "10 AM CT
on the previous day", matching ERCOT DAM close).

Usage:
    from process_data.calculate_load_error import build_load_snapshot
    load_df = build_load_snapshot(
        months=[(2025, m) for m in range(1, 13)],
    )
"""

import os
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories
from process_data.process_ercot import (
    load_actual_load_month,
    load_demand_forecasts_month,
    _parse_delivery_datetime,
)

# ── Default lead specifications ───────────────────────────────────────────────
# Each dict must have:
#   "label"  – suffix for output columns (e.g. "1h" → forecast_load_1h, load_error_1h)
#   "type"   – "lead_hours" or "time_of_day"
#   "value"  – target lead in hours (for lead_hours)
#   "hour_ct"– target hour in US/Central on previous day (for time_of_day)

DEFAULT_LEAD_SPECS = [
    {"label": "1h", "type": "lead_hours", "value": 1},
    {"label": "dam", "type": "time_of_day", "hour_ct": 10},
]

# Zone column mappings (ERCOT API names → normalized names)
_ACTUAL_ZONE_COLS = {
    "coast": "coast",
    "east": "east",
    "farWest": "far_west",
    "north": "north",
    "northC": "north_central",
    "southC": "south_central",
    "southern": "south",
    "west": "west",
}

_FORECAST_ZONE_COLS = {
    "coast": "coast",
    "east": "east",
    "farWest": "far_west",
    "north": "north",
    "northCentral": "north_central",
    "southCentral": "south_central",
    "southern": "south",
    "west": "west",
}


def _hour_ending_to_int(hour_ending):
    """Parse ERCOT hourEnding strings like '1:00'/'01:00'/'24:00'."""
    return pd.to_numeric(
        hour_ending.astype(str).str.split(":").str[0],
        errors="coerce",
    )


def _extract_forecast_lead_hours(fc_df, target_lead):
    """Select the forecast issuance closest to *target_lead* hours ahead.

    Parameters
    ----------
    fc_df : DataFrame
        Raw demand forecasts with ``posted_dt``, ``delivery_dt``, ``lead_h``
        columns already computed.
    target_lead : int or float
        Target lead time in hours.

    Returns
    -------
    DataFrame with one row per delivery_dt, containing weather-zone columns.
    """
    target_actual = target_lead + 0.5  # forecasts are issued at :30
    df = fc_df[fc_df["lead_h"] > 0].copy()
    df["lead_diff"] = (df["lead_h"] - target_actual).abs()
    idx = df.groupby("delivery_dt")["lead_diff"].idxmin()
    return df.loc[idx]


def _extract_forecast_time_of_day(fc_df, hour_ct):
    """Select the forecast posted closest to *hour_ct* CT on the previous day.

    Parameters
    ----------
    fc_df : DataFrame
        Raw demand forecasts with ``posted_dt``, ``delivery_dt`` columns.
    hour_ct : int
        Target hour in US/Central (e.g. 10 for 10:00 AM CT).

    Returns
    -------
    DataFrame with one row per delivery_dt.
    """
    import datetime as dt
    from zoneinfo import ZoneInfo

    central_tz = ZoneInfo("US/Central")
    utc_tz = dt.timezone.utc

    df = fc_df[fc_df["lead_h"] > 0].copy()

    # For each delivery date, compute the target posting time:
    # hour_ct on the previous calendar day in Central time, converted to
    # tz-naive Central (matching the postedDatetime convention).
    delivery_dates = df["delivery_dt"].dt.normalize().unique()
    target_map = {}
    for d in delivery_dates:
        prev_day = d - pd.Timedelta(days=1)
        target_utc = dt.datetime(
            prev_day.year, prev_day.month, prev_day.day,
            hour_ct, 0, tzinfo=central_tz,
        ).astimezone(utc_tz)
        # postedDatetime in the raw data is UTC but stored as naive
        target_map[d] = pd.Timestamp(target_utc.replace(tzinfo=None))

    df["target_posted"] = df["delivery_dt"].dt.normalize().map(target_map)
    df["time_diff"] = (df["posted_dt"] - df["target_posted"]).dt.total_seconds().abs()
    idx = df.groupby("delivery_dt")["time_diff"].idxmin()
    return df.loc[idx]


def _load_actual_load(months):
    """Load and normalize actual load across months.

    Returns
    -------
    DataFrame with columns: weather_zone, hour, actual_load
    """
    parts = []
    for year, month in months:
        actual_df = load_actual_load_month(year, month).copy()
        actual_df["operatingDay"] = pd.to_datetime(actual_df["operatingDay"])
        actual_df["hourEndingNum"] = _hour_ending_to_int(actual_df["hourEnding"])
        actual_df = actual_df.dropna(subset=["hourEndingNum"])
        actual_df["hour"] = actual_df["operatingDay"] + pd.to_timedelta(
            actual_df["hourEndingNum"] - 1, unit="h",
        )

        for col, zone in _ACTUAL_ZONE_COLS.items():
            if col not in actual_df.columns:
                continue
            part = actual_df[["hour", col]].copy()
            part["weather_zone"] = zone
            part = part.rename(columns={col: "actual_load"})
            parts.append(part)

    actual_long = pd.concat(parts, ignore_index=True)
    actual_long["actual_load"] = pd.to_numeric(actual_long["actual_load"], errors="coerce")
    actual_long = (
        actual_long
        .dropna(subset=["weather_zone", "hour"])
        .groupby(["weather_zone", "hour"], as_index=False)["actual_load"]
        .mean()
    )
    return actual_long


def _load_raw_forecasts(months):
    """Load all raw demand forecast CSVs and add parsed timestamps.

    Returns
    -------
    DataFrame with added columns: posted_dt, delivery_dt, lead_h
    """
    parts = []
    for year, month in months:
        df = load_demand_forecasts_month(year, month)
        parts.append(df)

    fc = pd.concat(parts, ignore_index=True)
    fc["posted_dt"] = pd.to_datetime(fc["postedDatetime"])
    fc["delivery_dt"] = pd.to_datetime(fc.apply(_parse_delivery_datetime, axis=1))
    fc["lead_h"] = (fc["delivery_dt"] - fc["posted_dt"]).dt.total_seconds() / 3600
    return fc


def build_load_snapshot(months, lead_specs=None, force_rebuild=False):
    """Build a weather-zone × hour load snapshot with forecast errors.

    Parameters
    ----------
    months : list of (year, month) tuples
    lead_specs : list of dict, optional
        Each dict describes one forecast lead. Keys:
          - "label": str suffix for columns (e.g. "1h", "dam")
          - "type": "lead_hours" or "time_of_day"
          - "value": target lead hours (required when type="lead_hours")
          - "hour_ct": target CT hour on previous day (required when type="time_of_day")
        Defaults to DEFAULT_LEAD_SPECS (1h-ahead + 10am CT DAM).
    force_rebuild : bool
        If False, return cached CSV when available.

    Returns
    -------
    pd.DataFrame
        Columns: weather_zone, hour, actual_load,
        forecast_load_{label}, load_error_{label} for each lead spec.
    """
    if lead_specs is None:
        lead_specs = DEFAULT_LEAD_SPECS

    dirs = setup_directories()

    # ── Cache key ─────────────────────────────────────────────────────────────
    month_tag = "_".join(f"{y}{m:02d}" for y, m in sorted(months))
    lead_tag = "+".join(spec["label"] for spec in lead_specs)
    cache_name = f"load_errors_wz_{month_tag}_{lead_tag}.csv"
    cache_path = os.path.join(dirs["processed"], "load_errors_by_weather_zone", cache_name)

    if not force_rebuild and os.path.exists(cache_path):
        print(f"Loading cached load snapshot: {cache_path}")
        return pd.read_csv(cache_path, parse_dates=["hour"])

    # ── Actual load ───────────────────────────────────────────────────────────
    print("Loading actual load data...")
    actual = _load_actual_load(months)

    # ── Raw forecasts (load once, extract multiple leads) ─────────────────────
    print("Loading raw demand forecasts...")
    fc_raw = _load_raw_forecasts(months)

    # ── Extract each lead spec ────────────────────────────────────────────────
    forecast_parts = []
    for spec in lead_specs:
        label = spec["label"]
        if spec["type"] == "lead_hours":
            selected = _extract_forecast_lead_hours(fc_raw, spec["value"])
            actual_leads = selected["lead_h"]
            print(
                f"  Lead '{label}' (lead_hours={spec['value']}): "
                f"{len(selected)} hours, actual lead mean={actual_leads.mean():.1f}h"
            )
        elif spec["type"] == "time_of_day":
            selected = _extract_forecast_time_of_day(fc_raw, spec["hour_ct"])
            actual_leads = selected["lead_h"]
            print(
                f"  Lead '{label}' (time_of_day={spec['hour_ct']}:00 CT): "
                f"{len(selected)} hours, actual lead mean={actual_leads.mean():.1f}h"
            )
        else:
            raise ValueError(f"Unknown lead spec type: {spec['type']}")

        # Convert delivery_dt → hour (start of interval, like actual load)
        selected = selected.copy()
        selected["hour"] = pd.to_datetime(selected["delivery_dt"]) - pd.Timedelta(hours=1)

        for col, zone in _FORECAST_ZONE_COLS.items():
            if col not in selected.columns:
                continue
            part = selected[["hour", col]].copy()
            part["weather_zone"] = zone
            part["lead_label"] = label
            part = part.rename(columns={col: "forecast_load"})
            forecast_parts.append(part)

    forecast_long = pd.concat(forecast_parts, ignore_index=True)
    forecast_long["forecast_load"] = pd.to_numeric(
        forecast_long["forecast_load"], errors="coerce"
    )
    forecast_long = forecast_long.dropna(subset=["weather_zone", "hour"])

    # Pivot lead labels to wide columns
    forecast_wide = (
        forecast_long
        .pivot_table(
            index=["weather_zone", "hour"],
            columns="lead_label",
            values="forecast_load",
            aggfunc="mean",
        )
        .rename(columns={spec["label"]: f"forecast_load_{spec['label']}" for spec in lead_specs})
        .reset_index()
    )

    # ── Merge and compute errors ──────────────────────────────────────────────
    result = actual.merge(forecast_wide, on=["weather_zone", "hour"], how="left")

    for spec in lead_specs:
        label = spec["label"]
        fc_col = f"forecast_load_{label}"
        err_col = f"load_error_{label}"
        if fc_col in result.columns:
            result[err_col] = result[fc_col] - result["actual_load"]

    # ── Cache ─────────────────────────────────────────────────────────────────
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    result.sort_values(["weather_zone", "hour"]).to_csv(cache_path, index=False)
    print(f"Saved load snapshot → {cache_path}")
    print(f"  Shape: {result.shape}")
    print(f"  Columns: {list(result.columns)}")

    return result


def merge_load_into_hourly(hourly_df, months, lead_specs=None, time_col="valid_time"):
    """Merge system-total load data into an hourly system-level DataFrame.

    Sums actual load and forecasts across all weather zones to produce
    system-level totals, then merges on the time column.

    Parameters
    ----------
    hourly_df : DataFrame
        Hourly system-level DataFrame (one row per hour).
    months : list of (year, month) tuples
    lead_specs : list of dict, optional
    time_col : str
        Name of the datetime column in hourly_df to merge on.

    Returns
    -------
    DataFrame with load columns added.
    """
    load_df = build_load_snapshot(months, lead_specs=lead_specs)

    # Aggregate to system total
    load_cols = ["actual_load"] + [
        c for c in load_df.columns if c.startswith("forecast_load_") or c.startswith("load_error_")
    ]
    system_load = (
        load_df
        .groupby("hour")[load_cols]
        .sum()
        .reset_index()
        .rename(columns={"hour": time_col})
    )
    system_load[time_col] = pd.to_datetime(system_load[time_col])

    n_before = len(hourly_df)
    hourly_df = hourly_df.merge(system_load, on=time_col, how="left")
    n_matched = hourly_df[load_cols[0]].notna().sum()
    print(f"  Merged system-total load: {n_matched:,}/{n_before:,} hours matched")
    return hourly_df


def merge_load_by_weather_zone(pixel_df, months, lead_specs=None,
                                time_col="valid_time",
                                lat_col="latitude", lon_col="longitude"):
    """Merge weather-zone load data into a pixel-level DataFrame.

    Maps each pixel to a weather zone via spatial join, then merges load
    data on (weather_zone, hour).

    Parameters
    ----------
    pixel_df : DataFrame
        Pixel-level DataFrame with lat/lon and time columns.
    months : list of (year, month) tuples
    lead_specs : list of dict, optional
    time_col : str
    lat_col, lon_col : str
        Column names for pixel coordinates.

    Returns
    -------
    DataFrame with weather_zone and load columns added.
    """
    from helper_funcs import map_pixels_to_weather_zones

    load_df = build_load_snapshot(months, lead_specs=lead_specs)

    # Build pixel → weather zone mapping (one-time, using unique pixel coords)
    pixel_coords = (
        pixel_df[[lat_col, lon_col]]
        .drop_duplicates()
        .dropna()
    )
    wz_map = map_pixels_to_weather_zones(
        pixel_coords[lat_col].values,
        pixel_coords[lon_col].values,
    )
    pixel_coords["weather_zone"] = wz_map
    pixel_df = pixel_df.merge(
        pixel_coords[[lat_col, lon_col, "weather_zone"]],
        on=[lat_col, lon_col],
        how="left",
    )

    # Merge load data on (weather_zone, time)
    load_df = load_df.rename(columns={"hour": time_col})
    load_df[time_col] = pd.to_datetime(load_df[time_col])

    n_before = len(pixel_df)
    pixel_df = pixel_df.merge(
        load_df, on=["weather_zone", time_col], how="left",
    )
    n_matched = pixel_df["actual_load"].notna().sum()
    print(f"  Merged weather-zone load: {n_matched:,}/{n_before:,} rows matched")
    return pixel_df


if __name__ == "__main__":
    months = [(2025, m) for m in range(1, 9)]
    df = build_load_snapshot(months, force_rebuild=True)
    print(df.head(10))
    print(f"\nLoad error stats:")
    for col in [c for c in df.columns if c.startswith("load_error_")]:
        print(f"  {col}: mean={df[col].mean():.1f}, std={df[col].std():.1f}")
