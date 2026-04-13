"""
Partial Least Squares (PLS) Spatial Analysis for ERCOT Forecast Error Attribution.

Supports single-field and multi-field (stacked) X matrices.  When more than
one error field is passed, the columns are concatenated horizontally so PLS
sees the full joint covariance structure.  Loading and coefficient surfaces
are then split back by field for per-field visualisation.

Outputs
-------
  pls_cv_curve_*.png         — CV R² vs n_components for each run
  pls_component_maps_*.png   — top-4 component loadings, one panel per field
  pls_coefficient_map_*.png  — β(s) coefficient surface, one panel per field
  pls_comparison_cv.png      — overlay of single-field vs joint CV curves
  pls_comparison_coefs.png   — side-by-side β(s) for each variant

Usage:
    # Single field (default)
    uv run python -m analysis.pls_analysis

    # Two-field joint model
    uv run python -m analysis.pls_analysis --fields wspd_error_1h temp_error_1h

    # Joint model + comparison against each single field
    uv run python -m analysis.pls_analysis --fields wspd_error_1h temp_error_1h --compare

    # Different outcome variable
    uv run python -m analysis.pls_analysis --fields wspd_error_1h temp_error_1h \\
        --depvar total_shadow_cost --compare
"""

import argparse
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.io.shapereader as shpreader
from sklearn.cross_decomposition import PLSRegression
from sklearn.model_selection import KFold, cross_val_score
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# ── Defaults ──────────────────────────────────────────────────────────────────

DEPVAR = "total_curtailment_mw"
DEFAULT_FIELDS = ["wspd_error_0h", "temp_error_0h"]
ALL_MONTHS = [(2025, m) for m in range(1, 13)]
N_CV_FOLDS = 5
RANDOM_STATE = 42
N_COMPONENTS_GRID = [1, 2, 3, 5, 7, 10, 15, 20, 30, 50, 75, 100]

# Concise labels for figure titles
FIELD_LABELS = {
    "wspd_error_1h":  "Wind speed error (HRRR 1h)",
    "wspd_error_18h": "Wind speed error (HRRR 18h)",
    "wspd_error_0h":  "Wind speed error (GFS day-ahead)",
    "temp_error_1h":  "Temp error (HRRR 1h)",
    "temp_error_18h": "Temp error (HRRR 18h)",
    "temp_error_0h":  "Temp error (GFS day-ahead)",
}


def _label(field):
    return FIELD_LABELS.get(field, field)


# ── Data loading ──────────────────────────────────────────────────────────────

def load_and_prepare(months, error_fields, depvar):
    """Load parquets and build a functional (T × N_pixels*n_fields) matrix.

    Each error field is pivoted independently, then the resulting pixel sets
    and hour sets are intersected to form a common (T, N_pixels) block per
    field.  All blocks are standardised column-wise and then stacked
    horizontally:

        X = [X_field1 | X_field2 | ...]   shape (T, N_pixels * n_fields)

    Returns
    -------
    X           : ndarray (T, N_pixels * n_fields)
    Y           : ndarray (T,)
    pixel_coords: ndarray (N_pixels, 2)   — (lat, lon) for a single field's pixels
    pixel_ids   : ndarray of str
    hour_index  : pd.DatetimeIndex
    offsets     : list of (start, end) int pairs — column slices per field in X
    """
    if isinstance(error_fields, str):
        error_fields = [error_fields]

    dirs = setup_directories()
    lmp_dir = Path(dirs["processed"]) / "combined_hourly_gridded_data"

    keep_cols = (["pixel_id", "valid_time", "latitude", "longitude", depvar]
                 + error_fields)

    dfs = []
    for year, month in months:
        path = lmp_dir / f"pixel_hourly_gfs+hrrr_{year}_{month:02d}.parquet"
        if not path.exists():
            print(f"  [skip] Missing: {path.name}")
            continue
        import pyarrow.parquet as pq
        available = pq.read_schema(path).names
        cols = [c for c in keep_cols if c in available]
        df = pd.read_parquet(path, columns=cols)
        df["valid_time"] = pd.to_datetime(df["valid_time"])
        if df["valid_time"].dt.tz is not None:
            df["valid_time"] = df["valid_time"].dt.tz_localize(None)
        dfs.append(df)
        print(f"  Loaded {year}-{month:02d}: {len(df):,} rows")

    if not dfs:
        raise FileNotFoundError("No pixel_hourly parquet files found.")

    df = pd.concat(dfs, ignore_index=True)
    sub = df.dropna(subset=[depvar]).copy()

    # Build a pivot per field; find common pixels + hours across all fields
    pivots = {}
    for field in error_fields:
        field_sub = sub.dropna(subset=[field])
        piv = field_sub.pivot_table(
            index="valid_time", columns="pixel_id", values=field, aggfunc="first"
        )
        good = piv.columns[piv.notna().mean() > 0.90]
        pivots[field] = piv[good]

    # Intersect pixels
    common_pixels = pivots[error_fields[0]].columns
    for field in error_fields[1:]:
        common_pixels = common_pixels.intersection(pivots[field].columns)

    # Drop hours with any missing value, then intersect across fields
    for field in error_fields:
        pivots[field] = pivots[field][common_pixels].dropna(axis=0)

    common_times = pivots[error_fields[0]].index
    for field in error_fields[1:]:
        common_times = common_times.intersection(pivots[field].index)

    hourly_y = sub.groupby("valid_time")[depvar].first()
    common_times = common_times.intersection(hourly_y.index)

    Y = hourly_y.loc[common_times].values

    # Pixel coordinates
    coord_map = (
        sub[["pixel_id", "latitude", "longitude"]]
        .dropna(subset=["latitude", "longitude"])
        .drop_duplicates("pixel_id")
        .set_index("pixel_id")
    )
    valid_pids = common_pixels.intersection(coord_map.index)
    pixel_ids = valid_pids.values
    pixel_coords = coord_map.loc[pixel_ids, ["latitude", "longitude"]].values
    n_pixels = len(pixel_ids)

    # Stack fields horizontally; track column offsets for later slicing
    blocks = []
    offsets = []
    col_start = 0
    for field in error_fields:
        piv = pivots[field].loc[common_times, valid_pids]
        X_block = StandardScaler().fit_transform(piv.values)
        blocks.append(X_block)
        offsets.append((col_start, col_start + n_pixels))
        col_start += n_pixels

    X = np.concatenate(blocks, axis=1)

    print(f"\nFunctional data: X {X.shape}  "
          f"({len(error_fields)} field(s) × {n_pixels} pixels × "
          f"{len(common_times)} hours)")
    print(f"Y — mean={Y.mean():.1f}  std={Y.std():.1f}  "
          f"min={Y.min():.1f}  max={Y.max():.1f}")

    return X, Y, pixel_coords, pixel_ids, common_times, offsets


# ── PLS CV sweep ──────────────────────────────────────────────────────────────

def cv_sweep(X, Y, n_components_grid=None, n_folds=N_CV_FOLDS):
    """Cross-validate PLS over a grid of component counts.

    Returns
    -------
    results : dict  {n_comp: {"r2_mean": float, "r2_std": float, "r2_folds": array}}
    best_n  : int
    """
    if n_components_grid is None:
        n_components_grid = N_COMPONENTS_GRID
    max_feasible = min(X.shape) - 1
    grid = [n for n in n_components_grid if n <= max_feasible]

    kf = KFold(n_splits=n_folds, shuffle=True, random_state=RANDOM_STATE)
    results = {}

    print(f"  CV sweep over {len(grid)} component counts (k={n_folds} folds):")
    for n in grid:
        pls = PLSRegression(n_components=n, max_iter=1000)
        r2_folds = cross_val_score(pls, X, Y, cv=kf, scoring="r2")
        results[n] = {"r2_mean": r2_folds.mean(), "r2_std": r2_folds.std(),
                      "r2_folds": r2_folds}
        print(f"    n={n:4d}  CV R² = {r2_folds.mean():.4f} ± {r2_folds.std():.4f}")

    best_n = max(results, key=lambda k: results[k]["r2_mean"])
    print(f"  → Best n_components = {best_n}  "
          f"(CV R² = {results[best_n]['r2_mean']:.4f})")
    return results, best_n


def fit_pls(X, Y, n_components):
    pls = PLSRegression(n_components=n_components, max_iter=1000)
    pls.fit(X, Y)
    return pls


# ── Map helpers ───────────────────────────────────────────────────────────────

def _draw_texas(ax):
    proj = ccrs.PlateCarree()
    ax.set_extent([-107.5, -93.0, 25.5, 37.0], crs=proj)
    ax.set_facecolor("#cce5f0")
    shp = shpreader.natural_earth(
        resolution="10m", category="cultural", name="admin_1_states_provinces"
    )
    for rec in shpreader.Reader(shp).records():
        name  = rec.attributes.get("name")
        admin = rec.attributes.get("admin")
        if name == "Texas":
            ax.add_geometries([rec.geometry], crs=proj,
                              facecolor="white", edgecolor="black",
                              linewidth=0.8, zorder=2)
        elif admin == "United States of America":
            ax.add_geometries([rec.geometry], crs=proj,
                              facecolor="#f0f0f0", edgecolor="#aaa",
                              linewidth=0.3, zorder=1)


def _scatter_map(ax, values, pixel_coords, title,
                 cmap="RdBu_r", vmin=None, vmax=None):
    _draw_texas(ax)
    if vmin is None or vmax is None:
        clim = np.nanpercentile(np.abs(values), 98)
        vmin, vmax = -clim, clim
    sc = ax.scatter(
        pixel_coords[:, 1], pixel_coords[:, 0],
        c=values, cmap=cmap, vmin=vmin, vmax=vmax,
        s=4, marker="s", transform=ccrs.PlateCarree(), zorder=3, alpha=0.9,
    )
    ax.set_title(title, fontsize=9, pad=4)
    return sc


# ── Plots ─────────────────────────────────────────────────────────────────────

def plot_cv_curve(cv_results, best_n, save_path, title_suffix=""):
    """Line chart of CV R² vs number of PLS components."""
    ns    = sorted(cv_results.keys())
    means = [cv_results[n]["r2_mean"] for n in ns]
    stds  = [cv_results[n]["r2_std"]  for n in ns]

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(ns, means, "o-", color="steelblue", linewidth=1.5, markersize=5)
    ax.fill_between(ns,
                    [m - s for m, s in zip(means, stds)],
                    [m + s for m, s in zip(means, stds)],
                    alpha=0.2, color="steelblue")
    ax.axvline(best_n, color="crimson", linestyle="--", linewidth=1.2,
               label=f"Optimal n={best_n}")
    ax.set_xlabel("Number of PLS Components", fontsize=11)
    ax.set_ylabel("Out-of-Sample R² (5-fold CV)", fontsize=11)
    ax.set_title(f"PLS Component Selection{title_suffix}", fontsize=13)
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {save_path.name}")


def plot_component_maps(pls, pixel_coords, offsets, error_fields, save_path,
                        n_show=4, depvar=DEPVAR):
    """Heatmap grid: rows = error fields, cols = top-N components.

    Each cell shows the PLS x-weight for that field × component.
    pls.x_weights_ has shape (n_total_cols, n_components).
    """
    n_show   = min(n_show, pls.n_components)
    n_fields = len(error_fields)
    weights  = pls.x_weights_    # (n_total_cols, n_components)

    nrows, ncols = n_fields, n_show
    fig, axes = plt.subplots(
        nrows, ncols,
        figsize=(6 * ncols, 5 * nrows),
        subplot_kw={"projection": ccrs.PlateCarree()},
        squeeze=False,
    )

    for comp_i in range(n_show):
        # Shared colour limits across fields for this component
        all_w = np.concatenate([
            weights[start:end, comp_i] for start, end in offsets
        ])
        clim = np.nanpercentile(np.abs(all_w), 98)

        for field_j, (field, (start, end)) in enumerate(
            zip(error_fields, offsets)
        ):
            ax = axes[field_j, comp_i]
            w  = weights[start:end, comp_i]
            sc = _scatter_map(
                ax, w, pixel_coords,
                title=f"{_label(field)}  —  Component {comp_i + 1}",
                vmin=-clim, vmax=clim,
            )
            plt.colorbar(sc, ax=ax, shrink=0.7, pad=0.02, label="Weight")

    field_str = " + ".join(error_fields)
    fig.suptitle(
        f"Top {n_show} PLS Component Spatial Loadings\n"
        f"Fields: {field_str}  |  Outcome: {depvar}",
        fontsize=12, y=1.01,
    )
    fig.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {save_path.name}")


def plot_coefficient_map(pls, pixel_coords, offsets, error_fields, save_path,
                         cv_r2, best_n, depvar):
    """One panel per field showing the recovered β(s) coefficient surface.

    β(s) = pls.coef_.ravel() sliced back to the pixels belonging to each field.
    """
    beta     = pls.coef_.ravel()   # (n_total_cols,)
    n_fields = len(error_fields)

    # Shared colour limits
    clim = np.nanpercentile(np.abs(beta), 98)

    fig, axes = plt.subplots(
        1, n_fields,
        figsize=(8 * n_fields, 6),
        subplot_kw={"projection": ccrs.PlateCarree()},
        squeeze=False,
    )

    for j, (field, (start, end)) in enumerate(zip(error_fields, offsets)):
        ax   = axes[0, j]
        b    = beta[start:end]
        sc   = _scatter_map(
            ax, b, pixel_coords,
            title=f"{_label(field)}",
            vmin=-clim, vmax=clim,
        )
        cbar = plt.colorbar(sc, ax=ax, shrink=0.65, pad=0.03)
        cbar.set_label(f"β → {depvar}", fontsize=8)

    field_str = " + ".join(error_fields)
    fig.suptitle(
        f"PLS Coefficient Surface  β(s)\n"
        f"n={best_n} components  |  CV R²={cv_r2:.3f}  |  "
        f"{field_str} → {depvar}",
        fontsize=11, y=1.02,
    )
    fig.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {save_path.name}")


def plot_comparison_cv(all_cv_results, all_best_ns, labels, save_path, depvar):
    """Overlay CV R² curves for multiple PLS runs (single-field + joint)."""
    colors = plt.cm.tab10(np.linspace(0, 0.7, len(labels)))

    fig, ax = plt.subplots(figsize=(9, 5))
    for (label, cv_res, best_n), color in zip(
        zip(labels, all_cv_results, all_best_ns), colors
    ):
        ns    = sorted(cv_res.keys())
        means = [cv_res[n]["r2_mean"] for n in ns]
        stds  = [cv_res[n]["r2_std"]  for n in ns]
        ax.plot(ns, means, "o-", color=color, linewidth=1.6, markersize=5,
                label=f"{label}  (opt n={best_n}, R²={cv_res[best_n]['r2_mean']:.3f})")
        ax.fill_between(ns,
                        [m - s for m, s in zip(means, stds)],
                        [m + s for m, s in zip(means, stds)],
                        alpha=0.12, color=color)
        ax.axvline(best_n, color=color, linestyle=":", linewidth=0.9)

    ax.set_xlabel("Number of PLS Components", fontsize=11)
    ax.set_ylabel("Out-of-Sample R² (5-fold CV)", fontsize=11)
    ax.set_title(f"PLS: Single-field vs Joint Model  |  Outcome: {depvar}",
                 fontsize=13)
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {save_path.name}")


def plot_comparison_coefs(pls_runs, labels, pixel_coords, save_path, depvar):
    """Grid of β(s) panels: one column per model run, one row per field.

    For single-field runs the field column is 1 wide; for the joint run each
    field gets its own column.  All panels share the same colour axis.
    """
    # Flatten to a list of (panel_title, beta_vector) tuples
    panels = []
    for label, pls, offsets, fields in pls_runs:
        beta = pls.coef_.ravel()
        for field, (start, end) in zip(fields, offsets):
            panels.append((f"{label}\n{_label(field)}", beta[start:end]))

    n = len(panels)
    ncols = min(n, 3)
    nrows = (n + ncols - 1) // ncols

    all_betas = np.concatenate([b for _, b in panels])
    clim = np.nanpercentile(np.abs(all_betas), 98)

    fig, axes = plt.subplots(
        nrows, ncols,
        figsize=(7 * ncols, 5.5 * nrows),
        subplot_kw={"projection": ccrs.PlateCarree()},
        squeeze=False,
    )

    for idx, (title, beta) in enumerate(panels):
        row, col = divmod(idx, ncols)
        ax = axes[row, col]
        sc = _scatter_map(ax, beta, pixel_coords, title,
                          vmin=-clim, vmax=clim)
        plt.colorbar(sc, ax=ax, shrink=0.65, pad=0.02,
                     label=f"β → {depvar}", )

    for idx in range(n, nrows * ncols):
        row, col = divmod(idx, ncols)
        axes[row, col].set_visible(False)

    fig.suptitle(
        f"PLS Coefficient Surfaces β(s)  —  Single-field vs Joint\n"
        f"Outcome: {depvar}",
        fontsize=12, y=1.01,
    )
    fig.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {save_path.name}")


# ── Main ──────────────────────────────────────────────────────────────────────

def run(months=None, error_fields=None, depvar=DEPVAR,
        n_components_grid=None, compare=False):
    """Fit PLS for the given (possibly multi-field) error spec.

    Parameters
    ----------
    error_fields : list of str
        One or more forecast error column names.  If more than one, they are
        stacked horizontally into a single X matrix.
    compare : bool
        If True and len(error_fields) > 1, also fit each field individually
        and produce comparison figures.
    """
    if months is None:
        months = ALL_MONTHS
    if error_fields is None:
        error_fields = DEFAULT_FIELDS
    if isinstance(error_fields, str):
        error_fields = [error_fields]
    if n_components_grid is None:
        n_components_grid = N_COMPONENTS_GRID

    dirs = setup_directories()
    fig_dir = Path(dirs["figures"]) / "pls_analysis"
    fig_dir.mkdir(parents=True, exist_ok=True)

    # File-name stem for this run
    fields_slug = "_".join(error_fields)

    print("=" * 65)
    print("PLS SPATIAL ANALYSIS")
    print(f"  Error fields : {error_fields}")
    print(f"  Outcome      : {depvar}")
    print(f"  Months       : {months[0]} – {months[-1]}")
    print(f"  Compare mode : {compare}")
    print("=" * 65)

    # ── Joint (or single-field) model ──────────────────────────────────────
    print(f"\n--- Loading data for: {error_fields} ---")
    X, Y, pixel_coords, pixel_ids, hour_index, offsets = load_and_prepare(
        months, error_fields, depvar
    )

    print(f"\n--- CV sweep: joint [{', '.join(error_fields)}] ---")
    cv_joint, best_n_joint = cv_sweep(X, Y, n_components_grid)

    print(f"\n--- Fitting final PLS (n={best_n_joint}) ---")
    pls_joint = fit_pls(X, Y, best_n_joint)

    # Per-run figures
    plot_cv_curve(
        cv_joint, best_n_joint,
        fig_dir / f"pls_cv_curve_{fields_slug}_{depvar}.png",
        title_suffix=f"\n{'+'.join(error_fields)}  →  {depvar}",
    )
    plot_component_maps(
        pls_joint, pixel_coords, offsets, error_fields,
        fig_dir / f"pls_component_maps_{fields_slug}_{depvar}.png",
        n_show=4, depvar=depvar,
    )
    plot_coefficient_map(
        pls_joint, pixel_coords, offsets, error_fields,
        fig_dir / f"pls_coefficient_map_{fields_slug}_{depvar}.png",
        cv_r2=cv_joint[best_n_joint]["r2_mean"],
        best_n=best_n_joint,
        depvar=depvar,
    )

    # ── Optional comparison against each single field ──────────────────────
    if compare and len(error_fields) > 1:
        print("\n--- Comparison mode: fitting each field individually ---")

        all_cv_results = [cv_joint]
        all_best_ns    = [best_n_joint]
        all_labels     = ["Joint: " + " + ".join(error_fields)]
        pls_runs       = [("Joint", pls_joint, offsets, error_fields)]

        for field in error_fields:
            print(f"\n  Field: {field}")
            X_s, Y_s, pc_s, pi_s, hi_s, off_s = load_and_prepare(
                months, [field], depvar
            )
            cv_s, best_n_s = cv_sweep(X_s, Y_s, n_components_grid)
            pls_s = fit_pls(X_s, Y_s, best_n_s)

            # Per-field individual figures
            plot_cv_curve(
                cv_s, best_n_s,
                fig_dir / f"pls_cv_curve_{field}_{depvar}.png",
                title_suffix=f"\n{_label(field)}  →  {depvar}",
            )
            plot_coefficient_map(
                pls_s, pc_s, off_s, [field],
                fig_dir / f"pls_coefficient_map_{field}_{depvar}.png",
                cv_r2=cv_s[best_n_s]["r2_mean"],
                best_n=best_n_s,
                depvar=depvar,
            )

            all_cv_results.append(cv_s)
            all_best_ns.append(best_n_s)
            all_labels.append(_label(field))
            pls_runs.append((_label(field), pls_s, off_s, [field]))

        # Comparison figures use the joint model's pixel_coords as reference
        plot_comparison_cv(
            all_cv_results, all_best_ns, all_labels,
            fig_dir / f"pls_comparison_cv_{fields_slug}_{depvar}.png",
            depvar=depvar,
        )
        plot_comparison_coefs(
            pls_runs, all_labels, pixel_coords,
            fig_dir / f"pls_comparison_coefs_{fields_slug}_{depvar}.png",
            depvar=depvar,
        )

    print(f"\nDone. Figures written to: {fig_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="PLS spatial analysis for ERCOT forecast errors."
    )
    parser.add_argument(
        "--fields", nargs="+", default=DEFAULT_FIELDS,
        metavar="FIELD",
        help=(
            "One or more forecast error fields to use.  Multiple fields are "
            "stacked horizontally.  "
            f"Default: {DEFAULT_FIELDS}"
        ),
    )
    parser.add_argument(
        "--depvar", default=DEPVAR,
        help=f"Outcome variable (default: {DEPVAR})",
    )
    parser.add_argument(
        "--months", nargs="+", type=int, metavar="M",
        help="Month numbers 1–12 to include (default: all 12).",
    )
    parser.add_argument(
        "--compare", action="store_true",
        help=(
            "When multiple fields are given, also fit each field individually "
            "and produce comparison plots."
        ),
    )
    args = parser.parse_args()

    months = [(2025, m) for m in args.months] if args.months else ALL_MONTHS
    run(
        months=months,
        error_fields=args.fields,
        depvar=args.depvar,
        compare=args.compare,
    )
