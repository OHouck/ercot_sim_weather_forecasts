"""
Cluster Heterogeneity Visualizations for ERCOT LMP Analysis.

Generates figures (one per lead time) each containing:
  1. Labeled cluster map with distinctive colors
  2. Coefficient plot for temp_error and load_error across 9 clusters
  3. 3x3 grid of coefficient-scaled histograms (temp, wspd, load error)

Usage:
    uv run python -m analysis.cluster_heterogeneity_lr
"""

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import cartopy.crs as ccrs
import cartopy.io.shapereader as shpreader
import pyfixest as pf

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories
from process_data.prepare_cluster_level_data import build_cluster_hourly_data

ROOT = Path(__file__).resolve().parent.parent

# ── Configuration (matches cluster_node_lr.qmd) ──────────────────────────────
# HRRR 1h short-range + GFS day-ahead (lead=0)
LEAD_SHORT, LEAD_DAH = 1, 0
MONTHS = [(2025, m) for m in range(1, 9)]

N_CLUSTERS = 7
GEO_WEIGHT = 2.0
N_NEIGHBORS = 8

DEPVAR = "system_lmp_std"
FE = ["hour_of_day", "month"]
CONTROLS = ["weekday", "actual_load"]

# Load error column to use per lead time
# HRRR 1h → 1h-ahead load forecast; GFS day-ahead → DAM load forecast (10am CT)
LOAD_ERROR_COL = {LEAD_SHORT: f"load_error_{LEAD_SHORT}h", LEAD_DAH: "load_error_dam"}

# 9-color qualitative palette (colorblind-friendly, distinctive)
CLUSTER_COLORS = [
    "#e6194b",  # red
    "#3cb44b",  # green
    "#4363d8",  # blue
    "#f58231",  # orange
    "#911eb4",  # purple
    "#42d4f4",  # cyan
    "#f032e6",  # magenta
    "#bfef45",  # lime
    "#fabed4",  # pink
]


def get_cluster_color(cluster_id):
    return CLUSTER_COLORS[cluster_id % len(CLUSTER_COLORS)]


# ── Data loading ──────────────────────────────────────────────────────────────


def load_all_data(force_rebuild=False, n_clusters=N_CLUSTERS, geo_weight=GEO_WEIGHT, n_neighbors=N_NEIGHBORS, months=None):
    """Load cluster-hour dataset via the canonical builder in prepare_cluster_level_data."""
    if months is None:
        months = MONTHS
    dirs = setup_directories()
    generators_path = os.path.join(dirs["raw"], "eia860", "texas_generators.csv")
    cluster_hourly, node_clusters, cluster_polygons, sil_score = build_cluster_hourly_data(
        months=months,
        n_clusters=n_clusters,
        geo_weight=geo_weight,
        n_neighbors=n_neighbors,
        generators_path=generators_path,
        force_rebuild=force_rebuild,
    )
    return dirs, node_clusters, sil_score, cluster_polygons, cluster_hourly


# ── Per-cluster regressions ───────────────────────────────────────────────────


def run_cluster_regressions(cluster_hourly):
    """Run one joint regression per cluster including both HRRR 1h and GFS day-ahead."""
    treatments = [
        f"temp_error_{LEAD_SHORT}h",
        f"wspd_error_{LEAD_SHORT}h",
        LOAD_ERROR_COL[LEAD_SHORT],
        f"temp_error_{LEAD_DAH}h",
        f"wspd_error_{LEAD_DAH}h",
        LOAD_ERROR_COL[LEAD_DAH],
    ]

    rhs = " + ".join(treatments + CONTROLS)
    fe_str = " + ".join(FE)
    fml = f"{DEPVAR} ~ {rhs} | {fe_str}"

    all_cols = [DEPVAR] + treatments + CONTROLS + FE
    results = {}

    for cluster_id in sorted(cluster_hourly["cluster"].unique()):
        subset = cluster_hourly[cluster_hourly["cluster"] == cluster_id].copy()
        existing_cols = [c for c in all_cols if c in subset.columns]
        subset_clean = subset.dropna(subset=existing_cols)

        if len(subset_clean) < 50:
            print(f"  Cluster {cluster_id}: skipped (only {len(subset_clean)} obs)")
            continue

        try:
            m = pf.feols(fml=fml, data=subset_clean)
            tidy = m.tidy()
            results[cluster_id] = {
                "model": m,
                "tidy": tidy,
                "data": subset_clean,
                "n_obs": len(subset_clean),
            }
            print(f"  Cluster {cluster_id}: {len(subset_clean):,} obs, R2={m._r2:.3f}")
        except Exception as e:
            print(f"  Cluster {cluster_id}: FAILED — {e}")

    return results


# ── Figure 1: Cluster map ────────────────────────────────────────────────────


def plot_cluster_map(node_clusters, cluster_polygons, save_path):
    """Plot Texas map with nodes colored by cluster using our custom palette."""
    proj = ccrs.PlateCarree()
    fig, ax = plt.subplots(figsize=(7, 6), subplot_kw={"projection": proj})

    states_shp = shpreader.natural_earth(
        resolution="10m", category="cultural", name="admin_1_states_provinces"
    )
    for record in shpreader.Reader(states_shp).records():
        if record.attributes.get("name") == "Texas":
            ax.add_geometries(
                [record.geometry], proj,
                facecolor="#f0f0f0", edgecolor="black", linewidth=1.0,
            )
            break
    ax.set_extent([-107.5, -93.0, 25.5, 37.0], crs=proj)

    if cluster_polygons is not None:
        for _, row in cluster_polygons.iterrows():
            c = get_cluster_color(row["cluster"])
            ax.add_geometries(
                [row["geometry"]], proj,
                facecolor=mcolors.to_rgba(c, alpha=0.10),
                edgecolor=mcolors.to_rgba(c, alpha=0.7),
                linewidth=1.2, zorder=3,
            )

    for cid in sorted(node_clusters["cluster"].unique()):
        mask = node_clusters["cluster"] == cid
        ax.scatter(
            node_clusters.loc[mask, "lon"],
            node_clusters.loc[mask, "lat"],
            c=get_cluster_color(cid),
            s=30, edgecolors="k", linewidths=0.3, alpha=0.85,
            transform=proj, zorder=5, label=f"Cluster {cid}",
        )

    centroids = node_clusters.groupby("cluster")[["lat", "lon"]].mean()
    for cid, row in centroids.iterrows():
        ax.text(
            row["lon"], row["lat"], str(cid),
            fontsize=9, fontweight="bold", ha="center", va="center",
            transform=proj, zorder=10,
            bbox=dict(boxstyle="round,pad=0.2", facecolor="white", edgecolor="gray", alpha=0.8),
        )

    ax.set_title(f"ERCOT Node Clusters (k={N_CLUSTERS})", fontsize=12)
    ax.gridlines(draw_labels=True, linewidth=0.3, alpha=0.5)
    plt.tight_layout()
    fig.savefig(save_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved cluster map → {save_path}")


# ── Figure 2: Coefficient plot ────────────────────────────────────────────────


def plot_coef_comparison(results, save_path):
    """Combined coefficient plot: 2 rows × 3 columns."""
    row_specs = [
        (LEAD_SHORT, [(f"temp_error_{LEAD_SHORT}h", "Temp Error"), (f"wspd_error_{LEAD_SHORT}h", "Wind Speed Error"), (LOAD_ERROR_COL[LEAD_SHORT], "Load Error")], f"HRRR {LEAD_SHORT}h Short-Range"),
        (LEAD_DAH, [(f"temp_error_{LEAD_DAH}h", "Temp Error"), (f"wspd_error_{LEAD_DAH}h", "Wind Speed Error"), (LOAD_ERROR_COL[LEAD_DAH], "Load Error")], f"GFS Day-Ahead"),
    ]

    rows = []
    for leadtime, specs, _ in row_specs:
        for cid in sorted(results.keys()):
            tidy = results[cid]["tidy"]
            for coef, label in specs:
                if coef in tidy.index:
                    rows.append({
                        "lead_row": leadtime,
                        "cluster": cid,
                        "coef_name": label,
                        "estimate": tidy.loc[coef, "Estimate"],
                        "se": tidy.loc[coef, "Std. Error"],
                    })

    plot_df = pd.DataFrame(rows)
    if plot_df.empty:
        print("  No coefficients to plot — skipping coef plot.")
        return

    n_clusters = len(plot_df["cluster"].unique())
    panel_h = 0.45 * n_clusters + 1.2

    fig, axes = plt.subplots(2, 3, figsize=(15, 2 * panel_h), sharey="row")

    for row_i, (leadtime, specs, row_label) in enumerate(row_specs):
        for col_i, (coef, label) in enumerate(specs):
            ax = axes[row_i, col_i]
            sub = (plot_df[(plot_df["lead_row"] == leadtime) & (plot_df["coef_name"] == label)].sort_values("cluster"))
            if sub.empty:
                ax.set_visible(False)
                continue

            y_pos = np.arange(len(sub))
            colors = [get_cluster_color(c) for c in sub["cluster"]]
            ci_lo = sub["estimate"] - 1.96 * sub["se"]
            ci_hi = sub["estimate"] + 1.96 * sub["se"]

            for i, (lo, hi) in enumerate(zip(ci_lo, ci_hi)):
                ax.plot([lo, hi], [y_pos[i], y_pos[i]], color=colors[i], linewidth=2, solid_capstyle="butt")
            ax.scatter(sub["estimate"], y_pos, c=colors, s=60, edgecolors="k", linewidths=0.5, zorder=5)
            ax.axvline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)
            ax.set_yticks(y_pos)
            ax.set_yticklabels([f"Cluster {c}" for c in sub["cluster"]])
            ax.set_xlabel("Coefficient (95% CI)", fontsize=9)
            ax.grid(axis="x", linestyle=":", alpha=0.4)
            ax.spines["top"].set_visible(False)
            ax.spines["right"].set_visible(False)

            if row_i == 0:
                ax.set_title(label, fontsize=11, fontweight="bold")
            if col_i == 0:
                ax.set_ylabel(row_label, fontsize=10, labelpad=8)

    fig.suptitle("Per-Cluster Coefficients — HRRR 1h (top) vs GFS Day-Ahead (bottom)", fontsize=13, y=1.01)
    plt.tight_layout()
    fig.savefig(save_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved coef plot → {save_path}")


# ── Figure 3: 3×3 coefficient-scaled histograms ──────────────────────────────


def plot_scaled_histogram_grid(results, cluster_hourly, leadtime, save_path):
    """3x3 grid: one cell per cluster. Each cell has 3 stacked histograms."""
    var_specs = [
        (f"temp_error_{leadtime}h", "Temp Error"),
        (f"wspd_error_{leadtime}h", "Wind Speed Error"),
        (LOAD_ERROR_COL[leadtime], "Load Error"),
    ]

    cluster_ids = sorted(results.keys())
    n = len(cluster_ids)
    ncols = 3
    nrows = (n + ncols - 1) // ncols

    fig, axes = plt.subplots(nrows, ncols, figsize=(14, 4 * nrows))
    axes = np.atleast_2d(axes)

    for idx, cid in enumerate(cluster_ids):
        row_i, col_i = divmod(idx, ncols)
        ax_cell = axes[row_i, col_i]

        tidy = results[cid]["tidy"]
        data = results[cid]["data"]
        color = get_cluster_color(cid)

        scaled_data = []
        labels = []
        for var, label in var_specs:
            if var in tidy.index and var in data.columns:
                coef = tidy.loc[var, "Estimate"]
                pval = tidy.loc[var, "Pr(>|t|)"]
                stars = ("***" if pval < 0.01 else "**" if pval < 0.05 else "*" if pval < 0.10 else "")
                vals = (data[var].dropna() * coef).values
                scaled_data.append(vals)
                labels.append(f"{label}{stars}\n(β={coef:.4f})")
            else:
                scaled_data.append(np.array([]))
                labels.append(f"{label}\n(N/A)")

        linestyles = ["-", "--", ":"]

        all_vals = np.concatenate([d for d in scaled_data if len(d) > 0])
        if len(all_vals) == 0:
            ax_cell.text(0.5, 0.5, "No data", transform=ax_cell.transAxes, ha="center", va="center")
            ax_cell.set_title(f"Cluster {cid}", fontsize=10, fontweight="bold", color=color)
            continue

        lo, hi = np.percentile(all_vals, [1, 99])
        bins = np.linspace(lo, hi, 40)

        for vals, label, ls in zip(scaled_data, labels, linestyles):
            if len(vals) == 0:
                continue
            ax_cell.hist(vals, bins=bins, histtype="stepfilled", density=True, color=color, alpha=0.35, edgecolor="none")
            ax_cell.hist(vals, bins=bins, histtype="step", density=True, color=color, linestyle=ls, linewidth=1.5, label=label)

        ax_cell.axvline(0, color="black", linewidth=0.7, linestyle="--", alpha=0.6)
        ax_cell.set_title(f"Cluster {cid}", fontsize=10, fontweight="bold", color=color)
        ax_cell.legend(fontsize=6, loc="upper right", framealpha=0.8)
        ax_cell.set_xlabel("Effect on LMP Std Dev ($/MWh)", fontsize=7)
        ax_cell.set_ylabel("Density", fontsize=7)
        ax_cell.tick_params(labelsize=7)
        ax_cell.spines["top"].set_visible(False)
        ax_cell.spines["right"].set_visible(False)

    for idx in range(n, nrows * ncols):
        row_i, col_i = divmod(idx, ncols)
        axes[row_i, col_i].set_visible(False)

    fig.suptitle(
        f"Distribution of Marginal Effects by Cluster — {leadtime}h Lead\n(variable × estimated coefficient)",
        fontsize=13, y=1.01,
    )
    plt.tight_layout()
    fig.savefig(save_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved histogram grid → {save_path}")


# ── Main ──────────────────────────────────────────────────────────────────────


def run_cluster_analysis(
    months=None,
    n_clusters=N_CLUSTERS,
    geo_weight=GEO_WEIGHT,
    n_neighbors=N_NEIGHBORS,
    force_rebuild=False,
):
    if months is None:
        months = MONTHS

    dirs, node_clusters, sil_score, cluster_polygons, cluster_hourly = load_all_data(
        force_rebuild=force_rebuild,
        n_clusters=n_clusters,
        geo_weight=geo_weight,
        n_neighbors=n_neighbors,
        months=months,
    )

    out_dir = os.path.join(dirs["figures"], "cluster_heterogeneity")
    os.makedirs(out_dir, exist_ok=True)

    map_path = os.path.join(out_dir, "cluster_map.png")
    print("\nGenerating cluster map...")
    plot_cluster_map(node_clusters, cluster_polygons, map_path)

    print("\nRunning per-cluster regressions (joint HRRR 1h + GFS day-ahead)...")
    results = run_cluster_regressions(cluster_hourly)

    if not results:
        print("No successful regressions — exiting.")
        return {}

    coef_path = os.path.join(out_dir, "coef_plot_combined.png")
    print("\nGenerating combined coefficient plot...")
    plot_coef_comparison(results, coef_path)

    hist_short_path = os.path.join(out_dir, "hist_grid_1h.png")
    hist_dah_path   = os.path.join(out_dir, "hist_grid_dah.png")
    print("\nGenerating HRRR 1h histogram grid...")
    plot_scaled_histogram_grid(results, cluster_hourly, LEAD_SHORT, hist_short_path)
    print("\nGenerating GFS day-ahead histogram grid...")
    plot_scaled_histogram_grid(results, cluster_hourly, LEAD_DAH, hist_dah_path)

    # Export regression results to CSV
    tables_dir = Path(dirs["tables"])
    os.makedirs(tables_dir, exist_ok=True)
    table_path = tables_dir / "cluster_regression_results.csv"

    csv_rows = []
    for cluster_id, res in results.items():
        tidy = res["tidy"]
        n_obs = res["n_obs"]
        for variable in tidy.index:
            estimate = tidy.loc[variable, "Estimate"]
            se = tidy.loc[variable, "Std. Error"]
            t_stat = tidy.loc[variable, "t value"]
            p_value = tidy.loc[variable, "Pr(>|t|)"]
            ci_lower = estimate - 1.96 * se
            ci_upper = estimate + 1.96 * se
            csv_rows.append({
                "cluster": cluster_id,
                "variable": variable,
                "coefficient": estimate,
                "std_error": se,
                "t_stat": t_stat,
                "p_value": p_value,
                "ci_lower": ci_lower,
                "ci_upper": ci_upper,
                "n_obs": n_obs,
            })

    csv_df = pd.DataFrame(csv_rows, columns=[
        "cluster", "variable", "coefficient", "std_error",
        "t_stat", "p_value", "ci_lower", "ci_upper", "n_obs",
    ])
    csv_df.to_csv(table_path, index=False)
    print(f"\n  Saved regression results CSV → {table_path}")

    print(f"\nDone! Output in {out_dir}/")

    return {
        "cluster_map": map_path,
        "coef_plot": coef_path,
        "hist_1h": hist_short_path,
        "hist_dah": hist_dah_path,
        "cluster_table": str(table_path),
    }


if __name__ == "__main__":
    run_cluster_analysis()
