"""
Figure functions for the unified asymmetric-error analysis (analysis.main_analysis).

Every function in this module reads only the saved outputs of
`analysis.main_analysis` — the beta-surface archives
(processed/main_analysis/betas_{method}_{cut}_{depvar}.npz + .json) and the
coefficient / R^2 tables (tables/main_analysis/) — so figures can be regenerated
without re-running any estimation.

Figures produced (all under figures/main_analysis/{cut}/):
  1. beta_surfaces_{method}_{depvar}.png
       Per-method coefficient-surface maps: one row per block, four columns
       (wind beta+, wind beta-, temp beta+, temp beta-). Pixels whose 95%
       bootstrap band excludes zero are drawn opaque; the rest are faded.
  2. method_comparison_{block}_{depvar}.png
       The headline comparison: one column per method, one row per
       channel x sign, all on a shared color scale — do the methods place
       forecast-error weight in the same places?
  3. r2_comparison.png
       Out-of-sample R^2 grouped bars, method x outcome, one figure per cut.
  4. asymmetry_forest_{method}_{depvar}.png
       Paired positive/negative coefficients (per one SD of feature score)
       with 95% bootstrap CIs; a star marks pairs where the symmetric-model
       restriction b+ = b- is rejected at 5%.

Usage:
    uv run python -m analysis.figures                 # everything found on disk
    uv run python -m analysis.figures --cuts all --depvars economic_congestion_cost
"""

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.colors import TwoSlopeNorm

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories

from analysis.pca_decomposition import (
    _draw_texas, _get_cartopy_crs, _grid_marker_size,
)

# Human-readable labels for the methods, channels, and signs used in filenames.
METHOD_LABELS = {
    "infrastructure": "Infrastructure Masks",
    "weather_zone":   "Weather-Zone Means",
    "rotated_eof":    "Rotated EOF (FPCA)",
    "fpls":           "Functional PLS",
}
CHANNEL_LABELS = {"wind": "100m Wind", "temp": "2m Temp"}
SIGN_LABELS    = {"pos": "β⁺ (positive part)", "neg": "β⁻ (negative part)"}
BLOCK_LABELS   = {
    "dayahead":  "GFS Day-Ahead Errors",
    "hourahead": "HRRR Hour-Ahead Errors",
    "realized":  "ERA5 Realized Weather",
}
# Alpha used for pixels whose 95% band includes zero (visually de-emphasized).
NON_SIGNIFICANT_ALPHA = 0.12

# Channel x sign panel order shared by the surface-map and comparison figures.
PANEL_CHANNEL_SIGNS = [("wind", "pos"), ("wind", "neg"),
                       ("temp", "pos"), ("temp", "neg")]


def _load_beta_archive(beta_dir, method, cut_key, depvar):
    """Load one saved beta-surface archive and its metadata.

    Parameters
    ----------
    beta_dir : Path — processed/main_analysis
    method   : str
    cut_key  : str
    depvar   : str

    Returns
    -------
    (npz archive, metadata dict) or (None, None) if the file does not exist.
    """
    archive_path = Path(beta_dir) / f"betas_{method}_{cut_key}_{depvar}.npz"
    if not archive_path.exists():
        return None, None
    return np.load(archive_path), json.loads(archive_path.with_suffix(".json").read_text())


def _channel_slice(archive, block_key, channel_tag, metadata):
    """Column slice and pixel coordinates of one channel within a block.

    Parameters
    ----------
    archive     : npz archive from _load_beta_archive
    block_key   : str
    channel_tag : "wind" or "temp"
    metadata    : dict — the archive's .json metadata

    Returns
    -------
    (pixel_lats, pixel_lons) for that channel's columns, or (None, None) if
    the channel is not present in the block.
    """
    block_fields  = metadata["fields_by_block"][block_key]
    block_offsets = archive[f"{block_key}__offsets"]
    # Channel tags come from the archive's own metadata (saved by
    # main_analysis) so the figure code never infers them from field names.
    channel_tags = metadata["channel_tags"]
    for field, (column_start, column_end) in zip(block_fields, block_offsets):
        if channel_tags[field] == channel_tag:
            return (archive[f"{block_key}__lat"][column_start:column_end],
                    archive[f"{block_key}__lon"][column_start:column_end])
    return None, None


def _draw_beta_panel(ax, pixel_lats, pixel_lons, beta, beta_lo, beta_hi,
                     color_norm, map_projection):
    """Draw one beta-surface map panel with significance-faded pixels.

    Pixels whose 95% bootstrap band excludes zero are drawn opaque; the rest
    are drawn at NON_SIGNIFICANT_ALPHA so the significant structure stands out
    without hiding the full surface.

    Parameters
    ----------
    ax             : matplotlib Axes
    pixel_lats     : ndarray (p,)
    pixel_lons     : ndarray (p,)
    beta           : ndarray (p,) — point surface
    beta_lo        : ndarray (p,) — 2.5th percentile surface
    beta_hi        : ndarray (p,) — 97.5th percentile surface
    color_norm     : matplotlib norm shared across panels
    map_projection : cartopy projection or None

    Returns
    -------
    The scatter artist of the significant layer (for colorbars).
    """
    significant = (beta_lo > 0) | (beta_hi < 0)
    marker_size = _grid_marker_size(pixel_lons)
    scatter_kwargs = {"cmap": "RdBu_r", "norm": color_norm,
                      "s": marker_size, "rasterized": True}
    if map_projection is not None:
        scatter_kwargs["transform"] = map_projection
    # Faded layer: every pixel, so the full surface is visible.
    ax.scatter(pixel_lons, pixel_lats, c=beta,
               alpha=NON_SIGNIFICANT_ALPHA, **scatter_kwargs)
    # Opaque layer: only pixels whose band excludes zero.
    scatter_artist = ax.scatter(pixel_lons[significant], pixel_lats[significant],
                                c=beta[significant], **scatter_kwargs)
    _draw_texas(ax)
    return scatter_artist


def plot_beta_surface_maps(method, cut_key, depvar, beta_dir, figure_dir):
    """Per-method beta-surface figure: blocks x (channel x sign) map grid.

    One row per block, four columns (wind beta+, wind beta-, temp beta+,
    temp beta-); each row shares a symmetric color scale set by the 99th
    percentile of |beta| across the row so the two signs are comparable.

    Parameters
    ----------
    method     : str
    cut_key    : str
    depvar     : str
    beta_dir   : Path — processed/main_analysis
    figure_dir : Path — output directory for this cut

    Returns
    -------
    Path to the saved figure, or None if the archive is missing.
    """
    archive, metadata = _load_beta_archive(beta_dir, method, cut_key, depvar)
    if archive is None:
        return None

    cartopy_crs    = _get_cartopy_crs()
    map_projection = cartopy_crs.PlateCarree() if cartopy_crs is not None else None
    subplot_kwargs = {"projection": map_projection} if map_projection is not None else {}

    block_keys = list(metadata["fields_by_block"].keys())
    panel_columns = PANEL_CHANNEL_SIGNS

    fig = plt.figure(figsize=(13, len(block_keys) * 2.9))
    grid_spec = fig.add_gridspec(len(block_keys), len(panel_columns),
                                 hspace=0.25, wspace=0.15)
    for row_index, block_key in enumerate(block_keys):
        # Row-shared symmetric color scale from the 99th percentile of |beta|.
        row_beta_values = np.concatenate([
            archive[f"{block_key}__{channel_tag}__{sign_tag}__beta"]
            for channel_tag, sign_tag in panel_columns
            if f"{block_key}__{channel_tag}__{sign_tag}__beta" in archive.files])
        row_scale  = float(np.percentile(np.abs(row_beta_values), 99)) or 1.0
        color_norm = TwoSlopeNorm(vmin=-row_scale, vcenter=0.0, vmax=row_scale)

        scatter_artist = None
        for column_index, (channel_tag, sign_tag) in enumerate(panel_columns):
            ax = fig.add_subplot(grid_spec[row_index, column_index], **subplot_kwargs)
            surface_key = f"{block_key}__{channel_tag}__{sign_tag}"
            if f"{surface_key}__beta" not in archive.files:
                ax.axis("off")
                continue
            pixel_lats, pixel_lons = _channel_slice(archive, block_key, channel_tag, metadata)
            scatter_artist = _draw_beta_panel(
                ax, pixel_lats, pixel_lons,
                archive[f"{surface_key}__beta"],
                archive[f"{surface_key}__beta_lo"],
                archive[f"{surface_key}__beta_hi"],
                color_norm, map_projection)
            ax.set_title(f"{CHANNEL_LABELS[channel_tag]}  {SIGN_LABELS[sign_tag]}",
                         fontsize=7.5, pad=2)
            if column_index == 0:
                ax.text(-0.14, 0.5, BLOCK_LABELS.get(block_key, block_key),
                        transform=ax.transAxes, rotation=90,
                        va="center", ha="right", fontsize=8)
        if scatter_artist is not None:
            fig.colorbar(scatter_artist, ax=fig.axes[-len(panel_columns):],
                         shrink=0.75, pad=0.01)

    fig.suptitle(
        f"{METHOD_LABELS.get(method, method)} — coefficient surfaces β⁺(s), β⁻(s)\n"
        f"{metadata['depvar_label']}  [{metadata['cut_label']}]   "
        f"(opaque = 95% bootstrap band excludes zero)",
        fontsize=10, y=1.00)
    figure_path = Path(figure_dir) / f"beta_surfaces_{method}_{depvar}.png"
    fig.savefig(figure_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return figure_path


def plot_method_comparison_maps(cut_key, depvar, block_key, beta_dir, figure_dir,
                                methods=None):
    """Headline comparison figure: methods side-by-side on a shared color scale.

    For one block (forecast horizon), draws a grid with one column per method
    and one row per channel x sign (wind beta+, wind beta-, temp beta+,
    temp beta-). A single symmetric color scale across ALL panels makes the
    methods' weight placement directly comparable.

    Parameters
    ----------
    cut_key    : str
    depvar     : str
    block_key  : str — which forecast-horizon block to compare
    beta_dir   : Path — processed/main_analysis
    figure_dir : Path — output directory for this cut
    methods    : list[str] — methods to include; defaults to all with archives

    Returns
    -------
    Path to the saved figure, or None if fewer than two methods have archives.
    """
    methods = methods or list(METHOD_LABELS)
    loaded_archives = {}
    depvar_label = cut_label = ""
    for method in methods:
        archive, metadata = _load_beta_archive(beta_dir, method, cut_key, depvar)
        if archive is not None:
            loaded_archives[method] = (archive, metadata)
            depvar_label, cut_label = metadata["depvar_label"], metadata["cut_label"]
    if len(loaded_archives) < 2:
        return None

    panel_rows = PANEL_CHANNEL_SIGNS

    # One shared symmetric color scale across every panel of the figure.
    all_beta_values = np.concatenate([
        archive[f"{block_key}__{channel_tag}__{sign_tag}__beta"]
        for archive, _ in loaded_archives.values()
        for channel_tag, sign_tag in panel_rows
        if f"{block_key}__{channel_tag}__{sign_tag}__beta" in archive.files])
    shared_scale = float(np.percentile(np.abs(all_beta_values), 99)) or 1.0
    color_norm   = TwoSlopeNorm(vmin=-shared_scale, vcenter=0.0, vmax=shared_scale)

    cartopy_crs    = _get_cartopy_crs()
    map_projection = cartopy_crs.PlateCarree() if cartopy_crs is not None else None
    subplot_kwargs = {"projection": map_projection} if map_projection is not None else {}

    n_method_columns = len(loaded_archives)
    fig = plt.figure(figsize=(n_method_columns * 3.1, len(panel_rows) * 2.6))
    grid_spec = fig.add_gridspec(len(panel_rows), n_method_columns,
                                 hspace=0.22, wspace=0.10)
    scatter_artist = None
    for column_index, (method, (archive, metadata)) in enumerate(loaded_archives.items()):
        for row_index, (channel_tag, sign_tag) in enumerate(panel_rows):
            ax = fig.add_subplot(grid_spec[row_index, column_index], **subplot_kwargs)
            surface_key = f"{block_key}__{channel_tag}__{sign_tag}"
            if f"{surface_key}__beta" not in archive.files:
                ax.axis("off")
                continue
            pixel_lats, pixel_lons = _channel_slice(archive, block_key, channel_tag, metadata)
            scatter_artist = _draw_beta_panel(
                ax, pixel_lats, pixel_lons,
                archive[f"{surface_key}__beta"],
                archive[f"{surface_key}__beta_lo"],
                archive[f"{surface_key}__beta_hi"],
                color_norm, map_projection)
            if row_index == 0:
                ax.set_title(METHOD_LABELS.get(method, method), fontsize=9, pad=4)
            if column_index == 0:
                ax.text(-0.14, 0.5,
                        f"{CHANNEL_LABELS[channel_tag]}\n{SIGN_LABELS[sign_tag]}",
                        transform=ax.transAxes, rotation=90,
                        va="center", ha="right", fontsize=7.5)
    if scatter_artist is not None:
        fig.colorbar(scatter_artist, ax=fig.axes, shrink=0.6, pad=0.015,
                     label="β (outcome per 1-SD anomaly)")

    fig.suptitle(
        f"Method comparison — {BLOCK_LABELS.get(block_key, block_key)}\n"
        f"{depvar_label}  [{cut_label}]   (opaque = 95% band excludes zero)",
        fontsize=10, y=1.00)
    figure_path = Path(figure_dir) / f"method_comparison_{block_key}_{depvar}.png"
    fig.savefig(figure_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return figure_path


def plot_r2_comparison(cut_key, table_dir, figure_dir):
    """Grouped-bar figure of out-of-sample R^2 by method x outcome for one cut.

    Parameters
    ----------
    cut_key    : str
    table_dir  : Path — tables/main_analysis
    figure_dir : Path — output directory for this cut

    Returns
    -------
    Path to the saved figure, or None if the summary table is missing/empty.
    """
    r2_summary_path = Path(table_dir) / "r2_summary.csv"
    if not r2_summary_path.exists():
        return None
    r2_summary = pd.read_csv(r2_summary_path)
    cut_rows = r2_summary[r2_summary["cut"] == cut_key]
    if cut_rows.empty:
        return None

    outcome_names = list(cut_rows["depvar"].unique())
    method_names  = [m for m in METHOD_LABELS if m in set(cut_rows["method"])]
    bar_width     = 0.8 / max(len(method_names), 1)
    method_colors = plt.get_cmap("tab10")

    fig, ax = plt.subplots(figsize=(max(7, len(outcome_names) * 1.5), 4))
    for method_index, method in enumerate(method_names):
        method_r2 = [
            cut_rows[(cut_rows["depvar"] == outcome) & (cut_rows["method"] == method)]
            ["out_of_sample_r2"].mean()
            for outcome in outcome_names]
        bar_positions = (np.arange(len(outcome_names))
                         + (method_index - (len(method_names) - 1) / 2) * bar_width)
        ax.bar(bar_positions, method_r2, width=bar_width,
               color=method_colors(method_index), label=METHOD_LABELS.get(method, method))
    ax.axhline(0, color="k", lw=0.8)
    ax.set_xticks(np.arange(len(outcome_names)))
    ax.set_xticklabels(outcome_names, rotation=25, ha="right", fontsize=7.5)
    ax.set_ylabel("Out-of-sample R² (residualized)", fontsize=9)
    ax.set_title(f"Method comparison — out-of-sample fit  [{cut_key}]", fontsize=10)
    ax.legend(fontsize=8)
    ax.grid(axis="y", ls=":", lw=0.5, alpha=0.6)
    fig.tight_layout()
    figure_path = Path(figure_dir) / "r2_comparison.png"
    fig.savefig(figure_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return figure_path


def plot_asymmetry_forest(method, cut_key, depvar, table_dir, figure_dir):
    """Paired beta+/beta- forest plot with symmetry-test annotations.

    Each row is one basis function (mask x channel, EOF mode, or FPLS index);
    the positive-part and negative-part coefficients are drawn side by side,
    scaled to a one-SD change in the feature score so magnitudes are
    comparable across features. A star next to the row label marks pairs
    where the symmetric-model restriction H0: b+ = b- is rejected at 5%.

    Parameters
    ----------
    method     : str
    cut_key    : str
    depvar     : str
    table_dir  : Path — tables/main_analysis
    figure_dir : Path — output directory for this cut

    Returns
    -------
    Path to the saved figure, or None if the coefficient table is missing.
    """
    coefficient_path = Path(table_dir) / cut_key / f"{method}_coefficients_{depvar}.csv"
    if not coefficient_path.exists():
        return None
    coefficient_table = pd.read_csv(coefficient_path).set_index("feature")

    # Pair up the positive/negative rows through the structured pair metadata
    # (pair_stem + sign columns written by main_analysis) — no name parsing.
    pair_stems = []
    feature_rows = coefficient_table[coefficient_table["pair_stem"].notna()
                                     & (coefficient_table["pair_stem"] != "")]
    for stem, pair_rows in feature_rows.groupby("pair_stem", sort=False):
        names_by_sign = dict(zip(pair_rows["sign"], pair_rows.index))
        if "pos" in names_by_sign and "neg" in names_by_sign:
            pair_stems.append((stem, names_by_sign["pos"], names_by_sign["neg"]))
    if not pair_stems:
        return None

    fig, ax = plt.subplots(figsize=(7.5, max(2.5, len(pair_stems) * 0.34 + 1.2)))
    for row_index, (stem, positive_name, negative_name) in enumerate(pair_stems):
        for sign_offset, feature_name, sign_color in [(-0.16, positive_name, "#c0392b"),
                                                      (+0.16, negative_name, "#2980b9")]:
            row = coefficient_table.loc[feature_name]
            # Scale to a one-SD change in the feature score for comparability.
            score_sd = row["score_sd"] if np.isfinite(row["score_sd"]) else 1.0
            ax.errorbar(row["coef"] * score_sd, row_index + sign_offset,
                        xerr=[[max((row["coef"] - row["ci_lower"]) * score_sd, 0)],
                              [max((row["ci_upper"] - row["coef"]) * score_sd, 0)]],
                        fmt="o", color=sign_color, markersize=4, capsize=2.5, lw=1.1)
    ax.axvline(0, color="k", lw=0.8, ls="--")
    ax.set_yticks(range(len(pair_stems)))
    # Mark pairs where the symmetric-model restriction is rejected with a star
    # appended to the row label (kept inside the label to avoid overlaps).
    row_labels = [
        stem + ("  ★" if coefficient_table.loc[positive_name, "symmetry_p"] < 0.05 else "")
        for stem, positive_name, _ in pair_stems]
    ax.set_yticklabels(row_labels, fontsize=6.5)
    ax.invert_yaxis()
    ax.grid(axis="x", ls=":", lw=0.5, alpha=0.6)
    ax.set_xlabel("Effect of a 1-SD feature score (residualized outcome units)",
                  fontsize=8.5)
    ax.set_title(
        f"{METHOD_LABELS.get(method, method)} — β⁺ (red) vs β⁻ (blue)\n"
        f"{depvar}  [{cut_key}]   ★ = symmetry (b⁺ = b⁻) rejected at 5%",
        fontsize=9)
    fig.tight_layout()
    figure_path = Path(figure_dir) / f"asymmetry_forest_{method}_{depvar}.png"
    fig.savefig(figure_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return figure_path


def make_all_figures(methods=None, depvars=None, cuts=None):
    """Generate every figure for whatever saved results exist on disk.

    Scans processed/main_analysis for beta archives matching the requested
    methods/cuts/outcomes and produces the per-method surface maps, the
    per-block method-comparison maps, the asymmetry forests, and one R^2
    comparison per cut.

    Parameters
    ----------
    methods : list[str] or None — defaults to every known method
    depvars : list[str] or None — defaults to every outcome found on disk
    cuts    : list[str] or None — defaults to every cut found on disk

    Returns
    -------
    list[Path] — every figure written.
    """
    project_directories = setup_directories()
    beta_dir   = Path(project_directories["processed"]) / "main_analysis"
    table_dir  = Path(project_directories["tables"])    / "main_analysis"
    figure_root = Path(project_directories["figures"])  / "main_analysis"

    methods = list(methods or METHOD_LABELS)

    # Discover which (method, cut, depvar) combinations have saved archives.
    available_combinations = []
    for archive_path in sorted(beta_dir.glob("betas_*.npz")):
        metadata = json.loads(archive_path.with_suffix(".json").read_text())
        available_combinations.append(
            (metadata["method"], metadata["cut_key"], metadata["depvar"]))
    if depvars:
        available_combinations = [c for c in available_combinations if c[2] in depvars]
    if cuts:
        available_combinations = [c for c in available_combinations if c[1] in cuts]
    available_combinations = [c for c in available_combinations if c[0] in methods]

    written_figures = []
    for method, cut_key, depvar in available_combinations:
        cut_figure_dir = figure_root / cut_key
        cut_figure_dir.mkdir(parents=True, exist_ok=True)
        for figure_path in (
                plot_beta_surface_maps(method, cut_key, depvar, beta_dir, cut_figure_dir),
                plot_asymmetry_forest(method, cut_key, depvar, table_dir, cut_figure_dir)):
            if figure_path:
                written_figures.append(figure_path)
                print(f"  Figure: {figure_path}")

    # Comparison figures once per (cut, depvar) and R^2 bars once per cut.
    for cut_key in sorted({c[1] for c in available_combinations}):
        cut_figure_dir = figure_root / cut_key
        cut_figure_dir.mkdir(parents=True, exist_ok=True)
        for depvar in sorted({c[2] for c in available_combinations if c[1] == cut_key}):
            for block_key in ("dayahead", "hourahead", "realized"):
                figure_path = plot_method_comparison_maps(
                    cut_key, depvar, block_key, beta_dir, cut_figure_dir, methods=methods)
                if figure_path:
                    written_figures.append(figure_path)
                    print(f"  Figure: {figure_path}")
        figure_path = plot_r2_comparison(cut_key, table_dir, cut_figure_dir)
        if figure_path:
            written_figures.append(figure_path)
            print(f"  Figure: {figure_path}")
    return written_figures


# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    """CLI entry point: regenerate figures from saved results."""
    argument_parser = argparse.ArgumentParser(
        description="Figures for the unified asymmetric-error analysis")
    argument_parser.add_argument("--methods", nargs="*", default=None)
    argument_parser.add_argument("--depvars", nargs="*", default=None)
    argument_parser.add_argument("--cuts", nargs="*", default=None)
    cli_arguments = argument_parser.parse_args()
    make_all_figures(methods=cli_arguments.methods, depvars=cli_arguments.depvars,
                     cuts=cli_arguments.cuts)


if __name__ == "__main__":
    main()
