"""
Cluster Heterogeneity Visualizations for ERCOT LMP Analysis.

Generates two PDF notes (one per lead time) each containing:
  1. Labeled cluster map with distinctive colors
  2. Coefficient plot for temp_error and load_error across 9 clusters
  3. 3x3 grid of coefficient-scaled histograms (temp, wspd, load error)

Usage:
    uv run python -m analysis.cluster_heterogeneity_lr
"""

import os
import sys
import subprocess
import tempfile
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

# ── Configuration (matches cluster_node_lr.qmd) ──────────────────────────────
MODEL = "hrrr"
LEAD_TIMES = {"ndfd": (1, 25), "hrrr": (1, 18)}
LEAD_SHORT, LEAD_LONG = LEAD_TIMES[MODEL]
MONTHS = [(2025, m) for m in range(1, 9)]

N_CLUSTERS = 9
GEO_WEIGHT = 10.0
N_NEIGHBORS = 8

DEPVAR = "system_lmp_std"
FE = ["hour_of_day", "month"]
CONTROLS = ["observed_temp", "observed_wspd", "weekday", "actual_load"]

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


def load_all_data(force_rebuild=False):
    """Load cluster-hour dataset via the canonical builder in prepare_cluster_level_data.

    Delegates all pipeline logic (node features, clustering, station errors,
    generation mix) to build_cluster_hourly_data(), which caches results to
    processed_data/ so repeated runs are fast.
    """
    dirs = setup_directories()
    generators_path = os.path.join(dirs["raw"], "eia860", "texas_generators.csv")
    cluster_hourly, node_clusters, cluster_polygons, sil_score = build_cluster_hourly_data(
        months=MONTHS,
        model=MODEL,
        n_clusters=N_CLUSTERS,
        geo_weight=GEO_WEIGHT,
        n_neighbors=N_NEIGHBORS,
        generators_path=generators_path,
        force_rebuild=force_rebuild,
    )
    return dirs, node_clusters, sil_score, cluster_polygons, cluster_hourly


# ── Per-cluster regressions ───────────────────────────────────────────────────


def run_cluster_regressions(cluster_hourly, leadtime):
    """
    Run per-cluster regressions and return a dict of results.

    Returns:
        dict mapping cluster_id -> {
            'model': pyfixest model,
            'tidy': tidy DataFrame,
            'data': cleaned subset DataFrame,
            'n_obs': int,
        }
    """
    treatments = [
        f"temp_error_{leadtime}h",
        f"wspd_error_{leadtime}h",
        f"load_error_{leadtime}h",
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
            print(f"  Cluster {cluster_id}: {len(subset_clean):,} obs, "
                  f"R2={m._r2:.3f}")
        except Exception as e:
            print(f"  Cluster {cluster_id}: FAILED — {e}")

    return results


# ── Figure 1: Cluster map ────────────────────────────────────────────────────


def plot_cluster_map(node_clusters, cluster_polygons, save_path):
    """Plot Texas map with nodes colored by cluster using our custom palette."""
    proj = ccrs.PlateCarree()
    fig, ax = plt.subplots(figsize=(7, 6), subplot_kw={"projection": proj})

    # Texas outline
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

    # Cluster polygons
    if cluster_polygons is not None:
        for _, row in cluster_polygons.iterrows():
            c = get_cluster_color(row["cluster"])
            ax.add_geometries(
                [row["geometry"]], proj,
                facecolor=mcolors.to_rgba(c, alpha=0.10),
                edgecolor=mcolors.to_rgba(c, alpha=0.7),
                linewidth=1.2, zorder=3,
            )

    # Scatter nodes
    for cid in sorted(node_clusters["cluster"].unique()):
        mask = node_clusters["cluster"] == cid
        ax.scatter(
            node_clusters.loc[mask, "lon"],
            node_clusters.loc[mask, "lat"],
            c=get_cluster_color(cid),
            s=30, edgecolors="k", linewidths=0.3, alpha=0.85,
            transform=proj, zorder=5, label=f"Cluster {cid}",
        )

    # Centroid labels
    centroids = node_clusters.groupby("cluster")[["lat", "lon"]].mean()
    for cid, row in centroids.iterrows():
        ax.text(
            row["lon"], row["lat"], str(cid),
            fontsize=9, fontweight="bold", ha="center", va="center",
            transform=proj, zorder=10,
            bbox=dict(
                boxstyle="round,pad=0.2", facecolor="white",
                edgecolor="gray", alpha=0.8,
            ),
        )

    ax.set_title(f"ERCOT Node Clusters (k={N_CLUSTERS})", fontsize=12)
    ax.gridlines(draw_labels=True, linewidth=0.3, alpha=0.5)
    plt.tight_layout()
    fig.savefig(save_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved cluster map → {save_path}")


# ── Figure 2: Coefficient plot ────────────────────────────────────────────────


def plot_coef_comparison(results, leadtime, save_path):
    """
    Horizontal coefficient plot: temp_error, wspd_error, and load_error for each cluster.
    Bars colored by cluster. All three panels on one row.
    """
    coef_names = [
        f"temp_error_{leadtime}h",
        f"wspd_error_{leadtime}h",
        f"load_error_{leadtime}h",
    ]
    coef_labels = ["Temp Error", "Wind Speed Error", "Load Error"]

    rows = []
    for cid in sorted(results.keys()):
        tidy = results[cid]["tidy"]
        for coef, label in zip(coef_names, coef_labels):
            if coef in tidy.index:
                rows.append({
                    "cluster": cid,
                    "coef_name": label,
                    "estimate": tidy.loc[coef, "Estimate"],
                    "se": tidy.loc[coef, "Std. Error"],
                })

    plot_df = pd.DataFrame(rows)
    if plot_df.empty:
        print("  No coefficients to plot — skipping coef plot.")
        return

    n_coefs = len(coef_labels)
    n_clusters = len(plot_df["cluster"].unique())

    fig, axes = plt.subplots(1, n_coefs, figsize=(5 * n_coefs, 0.5 * n_clusters + 1.5),
                             sharey=True)
    if n_coefs == 1:
        axes = [axes]

    for ax, label in zip(axes, coef_labels):
        sub = plot_df[plot_df["coef_name"] == label].sort_values("cluster")
        y_pos = np.arange(len(sub))
        colors = [get_cluster_color(c) for c in sub["cluster"]]

        ci_lo = sub["estimate"] - 1.96 * sub["se"]
        ci_hi = sub["estimate"] + 1.96 * sub["se"]

        # CI whiskers
        for i, (lo, hi) in enumerate(zip(ci_lo, ci_hi)):
            ax.plot([lo, hi], [y_pos[i], y_pos[i]], color=colors[i],
                    linewidth=2, solid_capstyle="butt")

        # Point estimates
        ax.scatter(sub["estimate"], y_pos, c=colors, s=60,
                   edgecolors="k", linewidths=0.5, zorder=5)
        ax.axvline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)
        ax.set_yticks(y_pos)
        ax.set_yticklabels([f"Cluster {c}" for c in sub["cluster"]])
        ax.set_xlabel("Coefficient (95% CI)")
        ax.set_title(label, fontsize=11)
        ax.grid(axis="x", linestyle=":", alpha=0.4)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    fig.suptitle(f"Per-Cluster Coefficients — {leadtime}h Lead", fontsize=13, y=1.02)
    plt.tight_layout()
    fig.savefig(save_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved coef plot → {save_path}")


# ── Figure 3: 3×3 coefficient-scaled histograms ──────────────────────────────


def plot_scaled_histogram_grid(results, cluster_hourly, leadtime, save_path):
    """
    3x3 grid: one cell per cluster. Each cell has 3 stacked histograms
    showing (variable × coefficient), i.e., the distribution of marginal effects.
    """
    var_specs = [
        (f"temp_error_{leadtime}h", "Temp Error"),
        (f"wspd_error_{leadtime}h", "Wind Speed Error"),
        (f"load_error_{leadtime}h", "Load Error"),
    ]

    cluster_ids = sorted(results.keys())
    n = len(cluster_ids)
    ncols = 3
    nrows = (n + ncols - 1) // ncols  # ceil division

    fig, axes = plt.subplots(nrows, ncols, figsize=(14, 4 * nrows))
    axes = np.atleast_2d(axes)

    for idx, cid in enumerate(cluster_ids):
        row_i, col_i = divmod(idx, ncols)
        ax_cell = axes[row_i, col_i]

        tidy = results[cid]["tidy"]
        data = results[cid]["data"]
        color = get_cluster_color(cid)

        # We'll create 3 sub-histograms within one axes using offset y-ticks
        # Actually, stack them vertically using inset axes or just overlay
        # Using a single axes with three vertically offset histograms

        # Collect scaled values and labels
        scaled_data = []
        labels = []
        for var, label in var_specs:
            if var in tidy.index and var in data.columns:
                coef = tidy.loc[var, "Estimate"]
                pval = tidy.loc[var, "Pr(>|t|)"]
                stars = ("***" if pval < 0.01 else
                         "**"  if pval < 0.05 else
                         "*"   if pval < 0.10 else "")
                vals = (data[var].dropna() * coef).values
                scaled_data.append(vals)
                labels.append(f"{label}{stars}\n(β={coef:.4f})")
            else:
                scaled_data.append(np.array([]))
                labels.append(f"{label}\n(N/A)")

        # Line styles per variable: solid=temp, dashed=wspd, dotted=load
        linestyles = ["-", "--", ":"]

        all_vals = np.concatenate([d for d in scaled_data if len(d) > 0])
        if len(all_vals) == 0:
            ax_cell.text(0.5, 0.5, "No data", transform=ax_cell.transAxes,
                         ha="center", va="center")
            ax_cell.set_title(f"Cluster {cid}", fontsize=10, fontweight="bold",
                              color=color)
            continue

        # Determine common bin edges from the pooled range
        lo, hi = np.percentile(all_vals, [1, 99])
        bins = np.linspace(lo, hi, 40)

        for vals, label, ls in zip(scaled_data, labels, linestyles):
            if len(vals) == 0:
                continue
            # Semi-transparent fill
            ax_cell.hist(
                vals, bins=bins, histtype="stepfilled", density=True,
                color=color, alpha=0.35, edgecolor="none",
            )
            # Outline with linestyle encoding variable type
            ax_cell.hist(
                vals, bins=bins, histtype="step", density=True,
                color=color, linestyle=ls, linewidth=1.5, label=label,
            )

        ax_cell.axvline(0, color="black", linewidth=0.7, linestyle="--", alpha=0.6)
        ax_cell.set_title(f"Cluster {cid}", fontsize=10, fontweight="bold",
                          color=color)
        ax_cell.legend(fontsize=6, loc="upper right", framealpha=0.8)
        ax_cell.set_xlabel("Effect on LMP Std Dev ($/MWh)", fontsize=7)
        ax_cell.set_ylabel("Density", fontsize=7)
        ax_cell.tick_params(labelsize=7)
        ax_cell.spines["top"].set_visible(False)
        ax_cell.spines["right"].set_visible(False)

    # Hide unused axes
    for idx in range(n, nrows * ncols):
        row_i, col_i = divmod(idx, ncols)
        axes[row_i, col_i].set_visible(False)

    fig.suptitle(
        f"Distribution of Marginal Effects by Cluster — {leadtime}h Lead\n"
        f"(variable × estimated coefficient)",
        fontsize=13, y=1.01,
    )
    plt.tight_layout()
    fig.savefig(save_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved histogram grid → {save_path}")


# ── Typst document assembly ───────────────────────────────────────────────────


TYPST_TEMPLATE = r"""
#set page(paper: "us-letter", margin: 0.75in)
#set text(font: "New Computer Modern", size: 10pt)

#align(center)[
  #text(size: 16pt, weight: "bold")[Cluster Heterogeneity — {lead}h Lead Time]

  #text(size: 10pt, fill: gray)[ERCOT LMP Analysis \ Model: {model} | Clusters: {n_clusters} | Dep. Var: {depvar}]
]

#v(0.3in)

== Cluster Map

#figure(
  image("{map_path}", width: 85%),
  caption: [Geographic distribution of {n_clusters} ERCOT resource node clusters.
  Clusters formed via agglomerative clustering with geographic connectivity constraint.],
)

#pagebreak()

== Coefficient Estimates by Cluster

#figure(
  image("{coef_path}", width: 95%),
  caption: [Per-cluster OLS estimates of temperature forecast error and load forecast error
  on system LMP standard deviation. Error bars show 95% confidence intervals.
  FE: hour-of-day, month. Controls: observed temp, observed wind speed, weekday, actual load.],
)

#pagebreak()

== Distribution of Marginal Effects

#figure(
  image("{hist_path}", width: 100%),
  caption: [Each panel shows one cluster. Within each panel, three density histograms
  display the variable value multiplied by its estimated coefficient, yielding
  the distribution of marginal effects on system LMP std dev. Variables: temperature
  forecast error, wind speed forecast error, load forecast error.],
)
"""


def compile_typst_pdf(lead, model, n_clusters, depvar,
                      map_path, coef_path, hist_path, output_pdf):
    """Write a typst source file and compile it to PDF."""
    # Typst resolves image paths relative to the .typ file location,
    # so convert absolute paths to relative paths from the output directory.
    out_dir = os.path.dirname(output_pdf)
    content = TYPST_TEMPLATE.format(
        lead=lead,
        model=model.upper(),
        n_clusters=n_clusters,
        depvar=depvar,
        map_path=os.path.relpath(map_path, out_dir),
        coef_path=os.path.relpath(coef_path, out_dir),
        hist_path=os.path.relpath(hist_path, out_dir),
    )

    typst_src = output_pdf.replace(".pdf", ".typ")
    with open(typst_src, "w") as f:
        f.write(content)

    result = subprocess.run(
        ["typst", "compile", typst_src, output_pdf],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"  typst compile FAILED:\n{result.stderr}")
        return False
    else:
        print(f"  Compiled PDF → {output_pdf}")
        # Clean up .typ source
        os.remove(typst_src)
        return True


# ── Main ──────────────────────────────────────────────────────────────────────


def main():
    dirs, node_clusters, sil_score, cluster_polygons, cluster_hourly = load_all_data()

    out_dir = os.path.join(dirs["figures"], "cluster_heterogeneity")
    os.makedirs(out_dir, exist_ok=True)

    # Cluster map (shared across both lead times)
    map_path = os.path.join(out_dir, "cluster_map.png")
    print("\nGenerating cluster map...")
    plot_cluster_map(node_clusters, cluster_polygons, map_path)

    for leadtime, lead_label in [(LEAD_SHORT, "short"), (LEAD_LONG, "long")]:
        print(f"\n{'='*60}")
        print(f"Processing {lead_label} lead ({leadtime}h)")
        print(f"{'='*60}")

        # Run per-cluster regressions
        print("\nRunning per-cluster regressions...")
        results = run_cluster_regressions(cluster_hourly, leadtime)

        if not results:
            print(f"  No successful regressions for {leadtime}h — skipping.")
            continue

        # Coefficient plot
        coef_path = os.path.join(out_dir, f"coef_plot_{leadtime}h.png")
        print("\nGenerating coefficient plot...")
        plot_coef_comparison(results, leadtime, coef_path)

        # 3x3 histogram grid
        hist_path = os.path.join(out_dir, f"hist_grid_{leadtime}h.png")
        print("\nGenerating histogram grid...")
        plot_scaled_histogram_grid(results, cluster_hourly, leadtime, hist_path)

        # Compile typst PDF
        pdf_path = os.path.join(out_dir, f"cluster_heterogeneity_{leadtime}h.pdf")
        print("\nCompiling typst PDF...")
        compile_typst_pdf(
            lead=leadtime,
            model=MODEL,
            n_clusters=N_CLUSTERS,
            depvar=DEPVAR,
            map_path=map_path,
            coef_path=coef_path,
            hist_path=hist_path,
            output_pdf=pdf_path,
        )

    print(f"\nDone! Output in {out_dir}/")


if __name__ == "__main__":
    main()
