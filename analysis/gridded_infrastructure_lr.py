"""
Infrastructure-level gridded regression: forecast errors → system LMP spread.

Aggregates ERA5 forecast errors by infrastructure type (capacity-weighted),
then regresses system LMP std on these aggregated errors.

Usage:
    uv run python -m analysis.gridded_infrastructure_lr
"""

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pyfixest as pf

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories

ROOT = Path(__file__).resolve().parent.parent

# ── Constants ──────────────────────────────────────────────────────────────────

LEAD_SHORT, LEAD_DAH = 1, 0

ERROR_VARS = [
    f"temp_error_{LEAD_SHORT}h",
    f"wspd_error_{LEAD_SHORT}h",
    f"temp_error_{LEAD_DAH}h",
    f"wspd_error_{LEAD_DAH}h",
]
OBS_VARS = ["era5_temp", "era5_wspd"]
AGG_VARS = ERROR_VARS + OBS_VARS

DEPVAR = "system_lmp_std"
FE = ["hour_of_day", "month"]
CLUSTER_SE = "date"

TREATMENTS = [
    "temp_error_1h_wind",
    "wspd_error_1h_wind",
    "temp_error_1h_solar",
    "wspd_error_1h_solar",
    "temp_error_1h_gas",
    "wspd_error_1h_gas",
    "temp_error_1h_load_center",
    "wspd_error_1h_transmission",
    "temp_error_1h_transmission",
    "temp_error_0h_wind",
    "wspd_error_0h_wind",
    "temp_error_0h_solar",
    "wspd_error_0h_solar",
    "temp_error_0h_gas",
    "wspd_error_0h_gas",
    "temp_error_0h_load_center",
    "wspd_error_0h_transmission",
    "temp_error_0h_transmission",
    "load_error_1h",
    "load_error_dam",
]

CONTROLS = ["era5_temp_load_center", "era5_wspd_wind", "actual_load", "weekday"]

INTERACTIONS = [
    ("temp_error_1h_load_center", "wspd_error_1h_wind"),
    ("temp_error_1h_transmission", "wspd_error_1h_transmission"),
    ("wspd_error_1h_wind", "era5_wspd_wind"),
    ("temp_error_1h_gas", "era5_temp_load_center"),
]

SEASONS = {
    "summer":   {"months": [6, 7, 8],          "label": "Summer (Jun–Aug)"},
    "winter":   {"months": [12, 1, 2],          "label": "Winter (Dec–Feb)"},
    "shoulder": {"months": [3, 4, 5, 9, 10, 11], "label": "Shoulder (Mar–May, Sep–Nov)"},
}


# ── Data loading ───────────────────────────────────────────────────────────────


def load_pixel_data(months, dirs):
    """Load monthly pixel-hourly parquets and concatenate.

    Args:
        months: list of (year, month) tuples
        dirs: directory dict from setup_directories()

    Returns:
        Concatenated DataFrame of all available months.
    """
    data_dir = os.path.join(dirs["processed"], "combined_hourly_gridded_data")
    dfs = []
    for year, month in months:
        path = os.path.join(data_dir, f"pixel_hourly_gfs+hrrr_{year}_{month:02d}.parquet")
        if os.path.exists(path):
            dfs.append(pd.read_parquet(path))
            print(f"  Loaded {year}-{month:02d}: {len(dfs[-1]):,} rows")
        else:
            print(f"  Missing {year}-{month:02d}, skipping")

    if not dfs:
        raise FileNotFoundError("No pixel-hourly parquet files found.")

    pixel_hourly = pd.concat(dfs, ignore_index=True)
    print(f"\nPixel-Hourly Dataset (HRRR 1h + GFS day-ahead)")
    print(f"  Rows: {len(pixel_hourly):,}")
    print(f"  Pixels: {pixel_hourly['pixel_id'].nunique()}")
    print(f"  Hours: {pixel_hourly['valid_time'].nunique()}")
    print(f"  Date range: {pixel_hourly['valid_time'].min()} to {pixel_hourly['valid_time'].max()}")
    return pixel_hourly


# ── Infrastructure aggregation ─────────────────────────────────────────────────


def aggregate_errors_by_infrastructure(pixel_hourly):
    """Aggregate ERA5 forecast errors by infrastructure type per hour.

    Capacity-weighted means for wind/solar/gas/battery/coal pixels;
    unweighted means for transmission/load_center pixels.

    Args:
        pixel_hourly: pixel × hour DataFrame from load_pixel_data()

    Returns:
        hourly DataFrame with one row per valid_time, containing
        aggregated error columns named {var}_{category}.
    """
    # ── Derived capacity columns ────────────────────────────────────────────────
    gas_tech_cols = [
        c for c in pixel_hourly.columns
        if "natural_gas" in c and c.startswith("nameplate_mw_tech_")
    ]
    battery_tech_cols = [
        c for c in pixel_hourly.columns
        if "batteries" in c and c.startswith("nameplate_mw_tech_")
    ]
    coal_tech_cols = [
        c for c in pixel_hourly.columns
        if "coal" in c and c.startswith("nameplate_mw_tech_")
    ]

    pixel_hourly = pixel_hourly.copy()
    pixel_hourly["_gas_total_mw"]     = pixel_hourly[gas_tech_cols].sum(axis=1) if gas_tech_cols else 0.0
    pixel_hourly["_battery_total_mw"] = pixel_hourly[battery_tech_cols].sum(axis=1) if battery_tech_cols else 0.0
    pixel_hourly["_coal_total_mw"]    = pixel_hourly[coal_tech_cols].sum(axis=1) if coal_tech_cols else 0.0

    # ── Build static pixel metadata ─────────────────────────────────────────────
    meta_cols = [
        "latitude", "longitude", "total_capacity_mw", "n_generators",
        "has_transmission_line", "load_center",
        "nameplate_mw_tech_onshore_wind_turbine",
        "nameplate_mw_tech_solar_photovoltaic",
        "_gas_total_mw", "_battery_total_mw", "_coal_total_mw",
    ]
    pix_meta = (
        pixel_hourly.groupby("pixel_id")
        .first()[meta_cols]
        .copy()
    )
    for col in pix_meta.columns:
        if "mw" in col or "capacity" in col:
            pix_meta[col] = pix_meta[col].fillna(0)

    # ── Category definitions ─────────────────────────────────────────────────────
    categories = {
        "wind": {
            "mask":       pix_meta["nameplate_mw_tech_onshore_wind_turbine"] > 0,
            "weight_col": "nameplate_mw_tech_onshore_wind_turbine",
        },
        "solar": {
            "mask":       pix_meta["nameplate_mw_tech_solar_photovoltaic"] > 0,
            "weight_col": "nameplate_mw_tech_solar_photovoltaic",
        },
        "gas": {
            "mask":       pix_meta["_gas_total_mw"] > 0,
            "weight_col": "_gas_total_mw",
        },
        "battery": {
            "mask":       pix_meta["_battery_total_mw"] > 0,
            "weight_col": "_battery_total_mw",
        },
        "coal": {
            "mask":       pix_meta["_coal_total_mw"] > 0,
            "weight_col": "_coal_total_mw",
        },
        "transmission": {
            "mask":       pix_meta["has_transmission_line"] == 1,
            "weight_col": None,
        },
        "load_center": {
            "mask":       pix_meta["load_center"] == 1,
            "weight_col": None,
        },
    }

    print("\nInfrastructure category pixel counts:")
    for cat_name, cat_info in categories.items():
        n = cat_info["mask"].sum()
        print(f"  {cat_name:15s}: {n:,} pixels")

    # ── Aggregate each category per valid_time ───────────────────────────────────
    hourly_parts = {}
    for cat_name, cat_info in categories.items():
        cat_pix_ids = pix_meta.index[cat_info["mask"]]
        subset = pixel_hourly[pixel_hourly["pixel_id"].isin(cat_pix_ids)].copy()
        if subset.empty:
            print(f"  {cat_name}: no pixels, skipping")
            continue

        weight_col = cat_info["weight_col"]

        if weight_col is not None:
            # Capacity-weighted mean: sum(var * weight) / sum(weight)
            weights = subset[weight_col].values
            weighted_df = subset[["valid_time"]].copy()
            weighted_df["_w"] = weights
            for var in AGG_VARS:
                if var in subset.columns:
                    weighted_df[f"_w_{var}"] = subset[var].values * weights

            agg_kwargs = {f"_w_sum_{cat_name}": ("_w", "sum")}
            for var in AGG_VARS:
                if f"_w_{var}" in weighted_df.columns:
                    agg_kwargs[f"{var}_{cat_name}"] = (f"_w_{var}", "sum")

            agg = weighted_df.groupby("valid_time").agg(**agg_kwargs)

            for var in AGG_VARS:
                col = f"{var}_{cat_name}"
                if col in agg.columns:
                    agg[col] = agg[col] / agg[f"_w_sum_{cat_name}"]
            agg = agg.drop(columns=[f"_w_sum_{cat_name}"])
        else:
            # Unweighted mean
            present_vars = [v for v in AGG_VARS if v in subset.columns]
            agg = subset.groupby("valid_time")[present_vars].mean()
            agg.columns = [f"{var}_{cat_name}" for var in present_vars]

        hourly_parts[cat_name] = agg
        print(f"  Aggregated {cat_name}: {len(agg):,} hours, {len(agg.columns)} columns")

    # ── Build hourly skeleton with system LMP + time features ────────────────────
    lmp_cols = ["system_lmp_mean", "system_lmp_max", "system_lmp_std"]
    time_cols = ["hour_of_day", "day_of_month", "weekday", "month"]
    skeleton_cols = ["valid_time"] + [c for c in lmp_cols + time_cols if c in pixel_hourly.columns]

    hourly_skeleton = (
        pixel_hourly
        .drop_duplicates("valid_time")[skeleton_cols]
        .set_index("valid_time")
        .sort_index()
    )

    # ── Join all categories ──────────────────────────────────────────────────────
    hourly = hourly_skeleton.copy()
    for cat_name, cat_df in hourly_parts.items():
        hourly = hourly.join(cat_df, how="left")
    hourly = hourly.reset_index()

    # ── Add date column for clustering SEs ──────────────────────────────────────
    hourly["date"] = hourly["valid_time"].dt.date.astype(str)

    print(f"\nHourly Aggregated Dataset")
    print(f"  Shape: {hourly.shape}")
    print(f"  Hours: {len(hourly):,}")
    print(f"  Date range: {hourly['valid_time'].min()} to {hourly['valid_time'].max()}")
    return hourly


# ── Regression helpers ─────────────────────────────────────────────────────────


def _prepare_data(df, depvar, treatments, controls, fe):
    """Drop rows with NaN in any analysis column and return clean DataFrame."""
    fe_list = fe if isinstance(fe, list) else ([fe] if fe else [])
    all_cols = [depvar] + treatments + controls + fe_list
    existing_cols = [c for c in all_cols if c in df.columns]
    df_clean = df.dropna(subset=existing_cols).copy()
    dropped = len(df) - len(df_clean)
    print(f"  Analysis sample: {len(df_clean):,} obs (dropped {dropped:,} with missing values)")
    return df_clean


def _build_formula(depvar, treatments, controls, fe=None, interactions=None):
    """Build a pyfixest formula string.

    Args:
        depvar: dependent variable name
        treatments: list of treatment variable names
        controls: list of control variable names
        fe: list of fixed effect variable names (or None)
        interactions: list of (var_a, var_b) tuples for interaction terms

    Returns:
        Formula string suitable for pf.feols().
    """
    rhs_parts = list(treatments) + list(controls)
    if interactions:
        for var_a, var_b in interactions:
            rhs_parts.append(f"{var_a}:{var_b}")
    rhs = " + ".join(rhs_parts)
    fml = f"{depvar} ~ {rhs}"
    if fe:
        fe_list = fe if isinstance(fe, list) else [fe]
        fml += " | " + " + ".join(fe_list)
    return fml


def _filter_existing(cols, df):
    """Return only columns that exist in df."""
    present = [c for c in cols if c in df.columns]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        print(f"  Dropping {len(missing)} absent columns: {missing}")
    return present


# ── Plotting ───────────────────────────────────────────────────────────────────


def plot_coefs(model, depvar, treatments, save_path):
    """Plot coefficient estimates with 95% CI.

    HRRR 1h coefficients are plotted in steelblue; GFS day-ahead in darkorange.
    Saves the figure to save_path.

    Args:
        model: fitted pyfixest model
        depvar: dependent variable name (used in title)
        treatments: list of treatment variable names to highlight
        save_path: absolute path to save the PNG
    """
    tidy = model.tidy()

    # Only plot treatment/interaction coefficients (exclude intercept if present)
    plot_vars = [v for v in tidy.index if v != "Intercept"]
    if not plot_vars:
        print("  No coefficients to plot.")
        return

    tidy_plot = tidy.loc[plot_vars]
    estimates = tidy_plot["Estimate"]
    se = tidy_plot["Std. Error"]
    ci_lo = estimates - 1.96 * se
    ci_hi = estimates + 1.96 * se
    labels = tidy_plot.index.tolist()
    n = len(labels)

    # Color by lead time
    colors = []
    for lbl in labels:
        if "_1h_" in lbl or lbl.endswith("_1h"):
            colors.append("steelblue")
        elif "_0h_" in lbl or lbl.endswith("_0h"):
            colors.append("darkorange")
        else:
            colors.append("gray")

    fig, ax = plt.subplots(figsize=(8, max(3, 0.45 * n + 1)))
    y_pos = list(range(n))

    for i, (lo, hi, est, col) in enumerate(zip(ci_lo, ci_hi, estimates, colors)):
        cap = 0.18
        ax.plot([lo, hi], [i, i], color=col, linewidth=1.5, solid_capstyle="butt")
        ax.plot([lo, lo], [i - cap, i + cap], color=col, linewidth=1.5)
        ax.plot([hi, hi], [i - cap, i + cap], color=col, linewidth=1.5)

    ax.scatter(estimates, y_pos, color=colors, zorder=3, s=40)
    ax.axvline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.6)

    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, fontsize=8)
    ax.set_xlabel("Coefficient estimate")
    ax.set_title(f"{depvar} — Infrastructure Regression Coefficients (95% CI)")
    ax.grid(axis="x", linestyle=":", alpha=0.4)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # Legend
    from matplotlib.lines import Line2D
    legend_elements = [
        Line2D([0], [0], color="steelblue", linewidth=2, label="HRRR 1h"),
        Line2D([0], [0], color="darkorange", linewidth=2, label="GFS day-ahead"),
        Line2D([0], [0], color="gray", linewidth=2, label="Other"),
    ]
    ax.legend(handles=legend_elements, fontsize=8, loc="lower right")

    plt.tight_layout()
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    plt.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved coefficient plot → {save_path}")


def plot_seasonal_coefs(seasonal_results, depvar, treatments, save_path):
    """Plot seasonal coefficient estimates in a 1×3 subplot grid.

    Args:
        seasonal_results: dict mapping season_key -> {'model': ..., 'label': ..., 'n_obs': ...}
        depvar: dependent variable name
        treatments: list of treatment names (for color coding)
        save_path: absolute path to save the PNG
    """
    season_keys = list(seasonal_results.keys())
    n_seasons = len(season_keys)
    if n_seasons == 0:
        print("  No seasonal results to plot.")
        return

    # Determine max number of coefficients for height
    max_coefs = max(
        len([v for v in res["model"].tidy().index if v != "Intercept"])
        for res in seasonal_results.values()
        if res["model"] is not None
    )

    fig, axes = plt.subplots(
        1, n_seasons,
        figsize=(7 * n_seasons, max(4, 0.4 * max_coefs + 2)),
        sharey=True,
    )
    if n_seasons == 1:
        axes = [axes]

    for ax, season_key in zip(axes, season_keys):
        res = seasonal_results[season_key]
        m = res["model"]
        if m is None:
            ax.set_title(f"{res['label']}\n(no data)")
            ax.set_visible(False)
            continue

        tidy = m.tidy()
        plot_vars = [v for v in tidy.index if v != "Intercept"]
        if not plot_vars:
            ax.set_title(res["label"])
            continue

        tidy_plot = tidy.loc[plot_vars]
        estimates = tidy_plot["Estimate"]
        se = tidy_plot["Std. Error"]
        ci_lo = estimates - 1.96 * se
        ci_hi = estimates + 1.96 * se
        labels = tidy_plot.index.tolist()
        n = len(labels)
        y_pos = list(range(n))

        colors = []
        for lbl in labels:
            if "_1h_" in lbl or lbl.endswith("_1h"):
                colors.append("steelblue")
            elif "_0h_" in lbl or lbl.endswith("_0h"):
                colors.append("darkorange")
            else:
                colors.append("gray")

        for i, (lo, hi, col) in enumerate(zip(ci_lo, ci_hi, colors)):
            cap = 0.18
            ax.plot([lo, hi], [i, i], color=col, linewidth=1.5, solid_capstyle="butt")
            ax.plot([lo, lo], [i - cap, i + cap], color=col, linewidth=1.5)
            ax.plot([hi, hi], [i - cap, i + cap], color=col, linewidth=1.5)

        ax.scatter(estimates, y_pos, color=colors, zorder=3, s=35)
        ax.axvline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.6)
        ax.set_yticks(y_pos)
        ax.set_yticklabels(labels, fontsize=7)
        ax.set_xlabel("Coefficient estimate", fontsize=9)
        ax.set_title(f"{res['label']}\n(n={res['n_obs']:,})", fontsize=10)
        ax.grid(axis="x", linestyle=":", alpha=0.4)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    # Shared legend on first axis
    from matplotlib.lines import Line2D
    legend_elements = [
        Line2D([0], [0], color="steelblue", linewidth=2, label="HRRR 1h"),
        Line2D([0], [0], color="darkorange", linewidth=2, label="GFS day-ahead"),
    ]
    axes[0].legend(handles=legend_elements, fontsize=8, loc="lower right")

    fig.suptitle(f"{depvar} — Seasonal Infrastructure Regression Coefficients (95% CI)",
                 fontsize=12, y=1.01)
    plt.tight_layout()
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    plt.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved seasonal coefficient plot → {save_path}")


# ── Output helpers ─────────────────────────────────────────────────────────────


def tidy_to_csv(model, n_obs, output_path, season=None):
    """Extract tidy() DataFrame, augment with metadata, and append to CSV.

    Args:
        model: fitted pyfixest model
        n_obs: number of observations used in the regression
        output_path: absolute path to CSV (appended if exists)
        season: season label string or None for main (full-year) regression
    """
    tidy_raw = model.tidy().reset_index()
    # pyfixest tidy() columns: Coefficient, Estimate, Std. Error, t value, Pr(>|t|)[, 2.5%, 97.5%]
    tidy = tidy_raw.iloc[:, :5].copy()
    tidy.columns = ["variable", "estimate", "std_error", "t_value", "pvalue"]

    # Significance stars
    def _stars(p):
        if p < 0.01:
            return "***"
        elif p < 0.05:
            return "**"
        elif p < 0.10:
            return "*"
        return ""

    tidy["stars"]    = tidy["pvalue"].apply(_stars)
    tidy["ci_lower"] = tidy["estimate"] - 1.96 * tidy["std_error"]
    tidy["ci_upper"] = tidy["estimate"] + 1.96 * tidy["std_error"]
    tidy["season"]   = season if season is not None else "full_year"
    tidy["n_obs"]    = n_obs

    write_header = not os.path.exists(output_path)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    tidy.to_csv(output_path, mode="a", header=write_header, index=False)
    print(f"  Appended {len(tidy)} rows to {output_path}")


# ── Main analysis ──────────────────────────────────────────────────────────────


def run_infrastructure_analysis(months=None, save_dir=None):
    """Run the full infrastructure-level regression analysis.

    1. Loads all monthly pixel-hourly parquets.
    2. Aggregates forecast errors by infrastructure type.
    3. Runs main (full-year) regression with interactions.
    4. Runs seasonal subsample regressions.
    5. Saves coefficient plots and tidy CSV tables.

    Args:
        months: list of (year, month) tuples; defaults to all 12 months of 2025
        save_dir: directory for figure outputs; defaults to
                  {dirs['figures']}/infrastructure_regressions/

    Returns:
        dict with keys:
            'coef_main'     : path to main coefficient plot PNG
            'coef_seasonal' : path to seasonal coefficient plot PNG
            'table_main'    : path to main regression CSV
            'table_seasonal': path to seasonal regression CSV
    """
    dirs = setup_directories()

    if months is None:
        months = [(2025, m) for m in range(1, 13)]

    if save_dir is None:
        save_dir = os.path.join(dirs["figures"], "infrastructure_regressions")

    os.makedirs(save_dir, exist_ok=True)

    tables_dir = Path(dirs["tables"])
    os.makedirs(tables_dir, exist_ok=True)

    # ── Load data ───────────────────────────────────────────────────────────────
    print("=" * 60)
    print("Loading pixel-hourly data...")
    print("=" * 60)
    pixel_hourly = load_pixel_data(months, dirs)

    # ── Aggregate ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("Aggregating forecast errors by infrastructure type...")
    print("=" * 60)
    hourly = aggregate_errors_by_infrastructure(pixel_hourly)

    # ── Merge load forecast errors ────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("Merging system-total load forecasts/errors...")
    print("=" * 60)
    from process_data.calculate_load_error import merge_load_into_hourly
    hourly = merge_load_into_hourly(hourly, months)

    # ── Main regression ─────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print(f"Main Regression: {DEPVAR} ~ HRRR 1h + GFS day-ahead errors | hour_of_day + month")
    print("=" * 60)

    treatments_present  = _filter_existing(TREATMENTS, hourly)
    controls_present    = _filter_existing(CONTROLS, hourly)
    interactions_present = [
        (a, b) for a, b in INTERACTIONS
        if a in hourly.columns and b in hourly.columns
    ]

    df_main = _prepare_data(hourly, DEPVAR, treatments_present, controls_present, FE)

    fml_main = _build_formula(
        DEPVAR, treatments_present, controls_present,
        fe=FE, interactions=interactions_present,
    )
    print(f"  Formula: {fml_main}")

    model_main = pf.feols(fml=fml_main, data=df_main, vcov={"CRV1": CLUSTER_SE})
    print(model_main.summary())

    # Save coefficient plot
    path_coef_main = os.path.join(save_dir, "coef_plot_main.png")
    plot_coefs(model_main, DEPVAR, treatments_present, save_path=path_coef_main)

    # Save tidy table
    path_table_main = str(tables_dir / "infrastructure_regression_main.csv")
    # Remove old file so we write fresh (not append)
    if os.path.exists(path_table_main):
        os.remove(path_table_main)
    tidy_to_csv(model_main, n_obs=len(df_main), output_path=path_table_main, season=None)

    # ── Seasonal regressions ────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("Seasonal regressions (hour_of_day FE only, no interactions)...")
    print("=" * 60)

    seasonal_fe = ["hour_of_day"]
    seasonal_treatments = treatments_present
    seasonal_controls   = controls_present

    seasonal_results = {}
    path_table_seasonal = str(tables_dir / "infrastructure_regression_seasonal.csv")
    if os.path.exists(path_table_seasonal):
        os.remove(path_table_seasonal)

    for season_key, season_info in SEASONS.items():
        season_df = hourly[hourly["month"].isin(season_info["months"])].copy()
        label = season_info["label"]

        if season_df.empty:
            print(f"\n  {label}: no data, skipping")
            seasonal_results[season_key] = {"model": None, "label": label, "n_obs": 0}
            continue

        print(f"\n  Season: {label}  ({len(season_df):,} hours before NaN drop)")

        df_season = _prepare_data(
            season_df, DEPVAR, seasonal_treatments, seasonal_controls, seasonal_fe
        )

        if len(df_season) < 50:
            print(f"    Too few observations ({len(df_season)}), skipping")
            seasonal_results[season_key] = {"model": None, "label": label, "n_obs": len(df_season)}
            continue

        fml_season = _build_formula(
            DEPVAR, seasonal_treatments, seasonal_controls, fe=seasonal_fe
        )
        print(f"    Formula: {fml_season}")

        m_season = pf.feols(fml=fml_season, data=df_season, vcov={"CRV1": CLUSTER_SE})
        print(m_season.summary())

        seasonal_results[season_key] = {
            "model": m_season,
            "label": label,
            "n_obs": len(df_season),
        }

        tidy_to_csv(
            m_season,
            n_obs=len(df_season),
            output_path=path_table_seasonal,
            season=season_key,
        )

    # Save seasonal coefficient plot
    path_coef_seasonal = os.path.join(save_dir, "coef_plot_seasonal.png")
    plot_seasonal_coefs(seasonal_results, DEPVAR, seasonal_treatments,
                        save_path=path_coef_seasonal)

    # ── Summary ─────────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("Done.")
    print(f"  Main coef plot    : {path_coef_main}")
    print(f"  Seasonal coef plot: {path_coef_seasonal}")
    print(f"  Main table        : {path_table_main}")
    print(f"  Seasonal table    : {path_table_seasonal}")
    print("=" * 60)

    return {
        "coef_main":      path_coef_main,
        "coef_seasonal":  path_coef_seasonal,
        "table_main":     path_table_main,
        "table_seasonal": path_table_seasonal,
    }


if __name__ == "__main__":
    run_infrastructure_analysis()
