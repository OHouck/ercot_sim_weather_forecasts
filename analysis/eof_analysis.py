"""
EOF analysis of ERCOT weather-error fields — three-phase workflow.

Channels (six per decomposition × cut):
  1. day-ahead  : GFS day-ahead wind-speed error + temperature error
  2. hour-ahead : HRRR 1h wind-speed error + temperature error
  3. realized   : ERA5 realized wind speed + temperature

Sample cuts (SAMPLE_CUT) determine which hours are used for each decomposition:
full year, four meteorological seasons (DJF/MAM/JJA/SON), and RUC-deployed
hours. Add further heterogeneity cuts as new SAMPLE_CUT entries.

━━━ Phase 1 — run_eof_significance ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Fit an unrotated EOF on the FULL DATASET for every channel block and plot the
eigenvalue scree with CI bands (used only to diagnose default_modes).
  * North et al. (1982) rule-of-thumb band
  * xeofs bootstrap 95% CI  (set N_BOOTSTRAPS=0 to skip — much faster)
Output: figures/eof_decomposition/eof_significance.png

━━━ Phase 2 — run_eof_decomposition ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Update default_modes after reviewing the Phase-1 scree, then run this phase.
For EACH cut in SAMPLE_CUT:
  • Fits Varimax-rotated EOFs on that cut's hours using default_modes.
  • Saves projection-weight modes to disk (consumed by Phase 3 without re-fitting).
  • Produces a combined loading-map / daily-score figure.
Output: figures/eof_decomposition/eof_decomposition_{cut}.png
        processed/eof/eof_modes_K{tag}_{cut}.npz

━━━ Phase 3 — run_eof_analysis ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
For EACH cut in SAMPLE_CUT:
  • Loads that cut's saved modes from Phase 2 (no re-fitting).
  • Projects the cut's hours onto those modes to get per-hour scores.
  • Runs OLS (HAC s.e.) against every outcome variable in depvars.
  • Saves coefficient tables and produces:
      – eof_coefs_across_outcomes_{field}.png per weather channel
      – eof_ftest_heatmap.png
Output: figures/eof_analysis/{cut}/

Both Phase 2 and Phase 3 use default_modes (a dict keyed by block, e.g.
"dayahead_wind", "hourahead_temp"). Update it after reviewing Phase 1 scree
plots, then re-run Phases 2 and 3.

Block keys are `{decomp}_{channel}`, e.g. `dayahead_wind`, `realized_temp`.

Usage:
    # Phase 1 — inspect scree to choose default_modes:
    uv run python -m analysis.eof_analysis --task significance

    # Phase 2 — fit and save per-cut modes + figures:
    uv run python -m analysis.eof_analysis --task decompose

    # Phase 3 — regressions using saved per-cut modes:
    uv run python -m analysis.eof_analysis --task analyze

    # All three phases in sequence:
    uv run python -m analysis.eof_analysis
"""

import argparse
import json
import sys
import warnings
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import xarray as xr

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories

from analysis.pca_decomposition import (
    load_channel_fields, ALL_MONTHS, ALL_FIELDS, RANDOM_STATE, _r2,
    CHANNEL_SPEC, FIELD_LABELS,
    _draw_texas, _get_cartopy_crs, _grid_marker_size,
    ERROR_FIELDS, REALIZED_FIELDS,
)
from analysis.pca_mode_analysis import (
    load_outcomes, build_regression_matrix, standardize_pca_cols, run_ols_inference,
    save_coef_table, plot_ftest_heatmap,
    plot_pca_coefs_across_outcomes,
)
from analysis.eof_methods import fit_varimax, DEFAULT_K, fit_sparse_joint, fit_mca

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# ── Constants ─────────────────────────────────────────────────────────────────

DEPVAR_CONFIGS = {
    "economic_congestion_cost":       {"label": "Congestion Cost", "transform": "log1p"},
    "total_renewable_curtailment_mw": {"label": "Renewable Curtailment", "transform": "log1p"},
    "avg_intensity_kg_per_mwh":       {"label": "Avg Carbon Intensity",  "transform": "log1p"},
    "ruc_deployment_mw":              {"label": "RUC Deployment",         "transform": "log1p"},
    "ruc_deployment_binary":          {"label": "Binary RUC Deployment",    "transform": "raw"},
    "rt_scgt_p85_markup":             {"label": "SC Gas Markup",     "transform": "raw"},
    "rt_ccgt_p85_markup":             {"label": "CC Gas Markup",     "transform": "raw"},
    "rt_cllig_p85_markup":            {"label": "Coal Markup",     "transform": "raw"},
}
DEPVARS = list(DEPVAR_CONFIGS.keys())

# decomp key -> (channel fields [wind, temp], human-readable label)
DECOMPOSITIONS = {
    "dayahead":  (["wspd100_error_0h", "temp_error_0h"],
                  "GFS Day-Ahead Forecast Errors"),
    "hourahead": (["wspd100_error_1h", "temp_error_1h"],
                  "HRRR Hour-Ahead Forecast Errors"),
    "realized":  (["era5_wspd100", "era5_temp"],
                  "ERA5 Realized Weather"),
}

# Sample cuts used to subset hours before fitting EOFs and for condition-specific
# regressions. Each cut exposes .mask(hours, outcomes_df) -> bool ndarray. Season
# cuts depend only on the calendar month; the RUC cut selects hours with positive
# RUC deployment. Add further heterogeneity cuts here as new SimpleNamespace entries.
SAMPLE_CUT = {
    "all":    SimpleNamespace(label="Full Year",
                              mask=lambda hours, out: np.ones(len(hours), dtype=bool)),
    "winter": SimpleNamespace(label="Winter (DJF)",
                              mask=lambda hours, out: np.isin(hours.month, [12, 1, 2])),
    "spring": SimpleNamespace(label="Spring (MAM)",
                              mask=lambda hours, out: np.isin(hours.month, [3, 4, 5])),
    "summer": SimpleNamespace(label="Summer (JJA)",
                              mask=lambda hours, out: np.isin(hours.month, [6, 7, 8])),
    "fall":   SimpleNamespace(label="Fall (SON)",
                              mask=lambda hours, out: np.isin(hours.month, [9, 10, 11])),
    "ruc":    SimpleNamespace(label="RUC Deployed (>0)",
                              mask=lambda hours, out:
                                  out["ruc_deployment_mw"].reindex(hours).fillna(0.0).values > 0),
}

SIG_N_MODES  = 10
# Set to 0 to skip Monte-Carlo bootstrap and use only North's rule of thumb —
# much faster for testing; restore to 50 (or higher) for publication runs.
N_BOOTSTRAPS = 50

# Number of rotated EOF modes per block (decomp × channel).
# Review Phase 1 (run_eof_significance) scree plots, then update these values
# before running Phase 2 (run_eof_decomposition) and Phase 3 (run_eof_analysis).
default_modes = {
    "dayahead_wind":  3,
    "dayahead_temp":  3,
    "hourahead_wind": 3,
    "hourahead_temp": 2,
    "realized_wind":  2,
    "realized_temp":  2,
}


# ═══════════════════════════════════════════════════════════════════════════════
# Section 1: EOF Significance Analysis
# ═══════════════════════════════════════════════════════════════════════════════

def _channel_tag(field_name):
    """Short channel tag ('wind' or 'temp') derived from a channel field name.

    Parameters
    ----------
    field_name : str

    Returns
    -------
    str — 'wind', 'temp', or field_name unchanged.
    """
    if "wspd" in field_name or "wind" in field_name:
        return "wind"
    if "temp" in field_name:
        return "temp"
    return field_name


# Field-name → block-key reverse lookup built from DECOMPOSITIONS.
_FIELD_TO_BLOCK_KEY = {
    f: f"{dk}_{_channel_tag(f)}"
    for dk, (fields, _) in DECOMPOSITIONS.items()
    for f in fields
}


def _modes_tag(K):
    """Stable filename tag from a per-block modes dict (sorted values joined by '-').

    Parameters
    ----------
    K : dict {block_key: int}

    Returns
    -------
    str — e.g. '2-3-3-3-2-3'
    """
    return "-".join(str(v) for _, v in sorted(K.items()))


def _decomposition_blocks(decomp_key, channel_fields):
    """Enumerate per-channel EOF blocks for one decomposition.

    Returns one block per channel field; each block decomposes a single channel
    independently. Block keys follow the pattern `{decomp_key}_{channel_tag}`.

    Parameters
    ----------
    decomp_key     : str
    channel_fields : list[str]

    Returns
    -------
    list[(block_key, channel_tag, [field_name])]
    """
    return [
        (f"{decomp_key}_{_channel_tag(f)}", _channel_tag(f), [f])
        for f in channel_fields
    ]


def all_block_keys():
    """Return every `{decomp}_{channel}` key — handy for CLI help."""
    return [
        block_key
        for decomp_key, (channel_fields, _) in DECOMPOSITIONS.items()
        for block_key, _, _ in _decomposition_blocks(decomp_key, channel_fields)
    ]


def _parse_n_modes(tokens):
    """Parse a list of 'block_key=N' tokens into a {block_key: int} dict.

    Parameters
    ----------
    tokens : list[str] or None — e.g. ['dayahead_wind=4', 'realized_temp=2']

    Returns
    -------
    dict {block_key: int}

    Raises
    ------
    argparse.ArgumentTypeError for malformed tokens.
    """
    if not tokens:
        return {}
    result = {}
    for token in tokens:
        if "=" not in token:
            raise argparse.ArgumentTypeError(f"Expected KEY=INT, got '{token}'")
        key, _, val = token.partition("=")
        try:
            result[key] = int(val)
        except ValueError:
            raise argparse.ArgumentTypeError(
                f"Expected integer value for '{key}', got '{val}'"
            )
    return result


def _subset_bundle(full_bundle, selection_mask):
    """Shallow copy of `full_bundle` with channels/hours sliced to `selection_mask`.

    Parameters
    ----------
    full_bundle    : dict from load_channel_fields
    selection_mask : ndarray bool over full_bundle['hours']

    Returns
    -------
    season_bundle dict
    """
    season_bundle = dict(full_bundle)
    season_bundle["hours"] = full_bundle["hours"][selection_mask]
    season_bundle["channel_da"] = {
        name: da.isel(valid_time=selection_mask)
        for name, da in full_bundle["channel_da"].items()
    }
    return season_bundle


def _save_fig(figure, output_path):
    """Save a figure at dpi=150, close it, and return the Path."""
    output_path = Path(output_path)
    figure.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(figure)
    return output_path


def _effective_sample_size(pc_series):
    """Autocorrelation-adjusted effective sample size n* of a PC time series.

    Implements n* = n / (1 + 2 * sum_k (1 - k/n) rho(k)) (Thiebaux & Zwiers 1984),
    summing the autocorrelation up to the first non-positive lag.

    Parameters
    ----------
    pc_series : array-like

    Returns
    -------
    float in [1, n]
    """
    ts = np.asarray(pc_series, dtype=float)
    ts = ts[np.isfinite(ts)]
    n = len(ts)
    if n < 10:
        return float(max(n, 1))
    ts = ts - ts.mean()
    ss = float(np.dot(ts, ts))
    if ss <= 0:
        return float(n)

    f_ts    = np.fft.rfft(ts, n=2 * n)
    full_ac = np.fft.irfft(f_ts * np.conj(f_ts))[:n]
    norm_ac = full_ac[1:] / ss
    nonpos  = norm_ac <= 0
    n_pos   = int(nonpos.argmax()) if nonpos.any() else len(norm_ac)
    if n_pos > 0:
        k   = np.arange(1, n_pos + 1)
        wac = float(np.dot(1 - k / n, norm_ac[:n_pos]))
    else:
        wac = 0.0
    return float(np.clip(n / (1 + 2 * wac), 1.0, n))


def bootstrap_eof_significance(channel_bundle, channel_fields,
                                n_scree_modes=SIG_N_MODES,
                                n_boot=N_BOOTSTRAPS, seed=RANDOM_STATE):
    """Fit an unrotated EOF and quantify its variance-spectrum CI.

    Bootstrap CI is only computed when n_boot > 0.  Pass n_boot=0 (or set
    N_BOOTSTRAPS = 0) to skip it and return only North's rule-of-thumb band.

    Parameters
    ----------
    channel_bundle  : dict — season-subset channel bundle from _subset_bundle
    channel_fields  : list[str] — channel names (single-element for per-channel blocks)
    n_scree_modes   : int
    n_boot          : int — bootstrap resamples; 0 disables bootstrap
    seed            : int

    Returns
    -------
    dict with arrays (all percentages, one entry per mode): modes, evr, bs_lo, bs_hi,
    north_lo, north_hi; plus scalars n, n_eff.
    """
    from xeofs.single import EOF

    channel_das = [channel_bundle["channel_da"][f] for f in channel_fields]
    n_hours = int(channel_das[0].sizes["valid_time"])
    n_modes_capped = min(n_scree_modes, n_hours - 1)

    eof_model = EOF(n_modes=n_modes_capped, center=True, random_state=seed)
    eof_model.fit(channel_das, dim="valid_time")
    evr = eof_model.explained_variance_ratio().values * 100.0

    if n_boot > 0:
        from xeofs.validation import EOFBootstrapper
        bs_model = EOFBootstrapper(n_bootstraps=n_boot, seed=seed)
        bs_model.fit(eof_model)
        q = bs_model.explained_variance_ratio().quantile(
            [0.025, 0.975], "n"
        ).values * 100.0
        bs_lo, bs_hi = q[0], q[1]
    else:
        bs_lo = np.full(n_modes_capped, np.nan)
        bs_hi = np.full(n_modes_capped, np.nan)

    eff_n    = _effective_sample_size(eof_model.scores().isel(mode=0).values)
    north_hw = np.sqrt(2.0 / eff_n)

    return {
        "modes":    np.arange(1, n_modes_capped + 1),
        "evr":      evr,
        "bs_lo":    bs_lo,
        "bs_hi":    bs_hi,
        "north_lo": evr * (1 - north_hw),
        "north_hi": evr * (1 + north_hw),
        "n":        n_hours,
        "n_eff":    eff_n,
    }


def _plot_one_significance_panel(panel_axis, panel_label, sig_result):
    """Draw one scree/CI panel onto `panel_axis`.

    North's rule-of-thumb band is always shown. Bootstrap CI is shown only when
    sig_result['bs_lo'] is not all-NaN (i.e. n_boot > 0 when fitting).

    Parameters
    ----------
    panel_axis  : matplotlib Axes
    panel_label : str
    sig_result  : dict from bootstrap_eof_significance
    """
    from matplotlib.lines import Line2D
    from matplotlib.patches import Patch

    modes = sig_result["modes"]
    panel_axis.fill_between(modes,
                             sig_result["north_lo"], sig_result["north_hi"],
                             color="#e67e22", alpha=0.18)

    has_bs = not np.all(np.isnan(sig_result["bs_lo"]))
    if has_bs:
        panel_axis.vlines(modes,
                          sig_result["bs_lo"], sig_result["bs_hi"],
                          color="#2980b9", lw=1.4, zorder=3)
        panel_axis.scatter(
            np.r_[modes, modes],
            np.r_[sig_result["bs_lo"], sig_result["bs_hi"]],
            marker="_", s=55, color="#2980b9", zorder=3,
        )
    panel_axis.plot(modes, sig_result["evr"], "o-", color="#1b4f72", ms=4, lw=1.0, zorder=4)

    panel_axis.set_xlabel("EOF mode (eigenvalue number)", fontsize=8)
    panel_axis.set_ylabel("Variance explained (%)", fontsize=8)
    panel_axis.set_title(f"{panel_label}\n(n={sig_result['n']} hrs)", fontsize=8)
    panel_axis.set_xticks(modes)
    panel_axis.set_ylim(bottom=0)
    panel_axis.tick_params(labelsize=7)
    panel_axis.grid(alpha=0.3, ls=":")

    handles = [
        Line2D([0], [0], color="#1b4f72", marker="o", ms=4, label="explained variance"),
        Patch(facecolor="#e67e22", alpha=0.18,
              label=f"North '82 rule (n*={sig_result['n_eff']:.0f})"),
    ]
    if has_bs:
        handles.insert(1, Line2D([0], [0], color="#2980b9", lw=1.4,
                                  label="xeofs bootstrap 95% CI"))
    panel_axis.legend(handles=handles, fontsize=6.5)


def plot_significance_panels(panel_grid, title, output_path):
    """Plot one significance figure as a grid of scree/CI panels.

    One row per decomposition, two columns (wind + temp channels).

    Parameters
    ----------
    panel_grid  : list[list[(label, sig_dict)]]
    title       : str
    output_path : Path

    Returns
    -------
    Path to the saved figure.
    """
    n_rows = len(panel_grid)
    n_cols = max(len(row) for row in panel_grid)
    fig, axes = plt.subplots(n_rows, n_cols,
                              figsize=(n_cols * 4.2, n_rows * 3.7),
                              squeeze=False)
    for ri, row in enumerate(panel_grid):
        for ci in range(n_cols):
            ax = axes[ri][ci]
            if ci >= len(row):
                ax.axis("off")
                continue
            label, sig_result = row[ci]
            _plot_one_significance_panel(ax, label, sig_result)

    fig.tight_layout(rect=[0, 0, 1, 0.94])
    fig.suptitle(title, fontsize=11, y=0.99)
    return _save_fig(fig, output_path)


def run_eof_significance(n_scree_modes=SIG_N_MODES,
                          n_boot=N_BOOTSTRAPS, seed=RANDOM_STATE):
    """Phase 1: scree plots on the full dataset to diagnose default_modes.

    Fits an unrotated EOF on all available hours for each channel block and plots
    the eigenvalue scree with North's rule-of-thumb CI (and optional bootstrap CI).
    A single figure is produced (one row per decomposition, two columns: wind + temp).

    After reviewing the scree plot, update the module-level default_modes dict then
    run Phase 2 (run_eof_decomposition) and Phase 3 (run_eof_analysis).

    Parameters
    ----------
    n_scree_modes : int — number of eigenvalues to show on the scree
    n_boot        : int — bootstrap resamples; 0 skips bootstrap (North rule only)
    seed          : int

    Returns
    -------
    Path to the saved figure.
    """
    dirs = setup_directories()
    figures_dir = Path(dirs["figures"]) / "eof_decomposition"
    figures_dir.mkdir(parents=True, exist_ok=True)

    print("\n=== Phase 1: Loading full-year channel fields ===")
    bundle = load_channel_fields(ALL_MONTHS, dirs)
    n_hours = len(bundle["hours"])

    bs_note = "" if n_boot else " (bootstrap OFF — North rule only)"
    print(f"\n=== Phase 1: EOF significance — full dataset ({n_hours} hrs){bs_note} ===")

    panel_grid = []
    for decomp_key, (channel_fields, decomp_label) in DECOMPOSITIONS.items():
        decomp_row = []
        for block_key, block_label, block_fields in _decomposition_blocks(decomp_key, channel_fields):
            print(f"  {block_key:20s} ({n_hours} hrs) ...")
            sig_result = bootstrap_eof_significance(
                bundle, block_fields,
                n_scree_modes=n_scree_modes, n_boot=n_boot, seed=seed,
            )
            decomp_row.append((f"{decomp_label}\n[{block_label}: {block_key}]", sig_result))
        panel_grid.append(decomp_row)

    out = figures_dir / "eof_significance.png"
    figure_path = plot_significance_panels(panel_grid, "EOF Significance — Full Dataset", out)
    print(f"\nDone — {figure_path}")
    print(f"Current default_modes = {default_modes}  (update before running Phase 2)")
    return figure_path


# ═══════════════════════════════════════════════════════════════════════════════
# Section 2: EOF Decomposition — Fit, Save, and Visualize Modes
# ═══════════════════════════════════════════════════════════════════════════════

def _orient_modes(loadings_list, scores_df):
    """Flip each mode so its summed loading (over all channels) is positive.

    Parameters
    ----------
    loadings_list : list[xr.DataArray] — one (mode, lat, lon) array per channel
    scores_df     : pd.DataFrame (hours x mode)

    Returns
    -------
    (oriented_loadings_list, oriented_scores_df)
    """
    mode_coords = loadings_list[0]["mode"].values
    ch_arrays = [ch.values for ch in loadings_list]
    sign_per_mode = np.array([
        -1.0 if sum(float(np.nansum(arr[mi])) for arr in ch_arrays) < 0 else 1.0
        for mi in range(len(mode_coords))
    ])
    sign_da = xr.DataArray(sign_per_mode, dims=["mode"], coords={"mode": mode_coords})
    oriented_loadings = [ch * sign_da for ch in loadings_list]
    oriented_scores = scores_df.copy()
    oriented_scores.iloc[:, :] = scores_df.values * sign_per_mode[np.newaxis, :]
    return oriented_loadings, oriented_scores


def _extract_grid_coords(loading_da):
    """Flattened lat/lon arrays and scatter marker size from a loading DataArray.

    Handles both 1-D regular grids (ERA5) and 2-D curvilinear grids (HRRR).

    Parameters
    ----------
    loading_da : xr.DataArray with 'mode' dim and lat/lon coords

    Returns
    -------
    (flat_lats ndarray, flat_lons ndarray, marker_size float)
    """
    first = loading_da.isel(mode=0)
    lat_c = first.coords.get("latitude")
    if lat_c is None:
        lat_c = first.coords.get("lat")
    lon_c = first.coords.get("longitude")
    if lon_c is None:
        lon_c = first.coords.get("lon")
    if lat_c is None:
        lat_c = first.coords[first.dims[-2]]
        lon_c = first.coords[first.dims[-1]]
    lats_raw, lons_raw = lat_c.values, lon_c.values
    if lats_raw.ndim == 1:
        lons_2d, lats_2d = np.meshgrid(lons_raw, lats_raw)
        flat_lats, flat_lons = lats_2d.ravel(), lons_2d.ravel()
    else:
        flat_lats, flat_lons = lats_raw.ravel(), lons_raw.ravel()
    return flat_lats, flat_lons, _grid_marker_size(flat_lons)


def _plot_daily_scores(ax, scores_series, label):
    """Plot daily-averaged scores with ±1 SEM bands onto ax.

    Parameters
    ----------
    ax            : matplotlib Axes
    scores_series : pd.Series with DatetimeIndex
    label         : str — axis title
    """
    gb         = scores_series.resample("D")
    daily_mean = gb.mean().dropna()
    daily_sem  = gb.sem().reindex(daily_mean.index).fillna(0)
    dates = daily_mean.index
    ax.fill_between(dates,
                    daily_mean - daily_sem,
                    daily_mean + daily_sem,
                    alpha=0.30, color="#2c3e50")
    ax.plot(dates, daily_mean, lw=0.8, color="#2c3e50")
    ax.axhline(0, color="#c0392b", lw=0.5, ls="--")
    ax.tick_params(labelsize=5)
    ax.set_title(label, fontsize=6, pad=1)
    plt.setp(ax.get_xticklabels(), rotation=35, ha="right")


def plot_season_figure(season_results, season_label, figures_dir, season_key):
    """Plot all per-channel block results for one season in a single figure.

    Layout: six block rows (3 decomps × 2 channels), each with a loading-map row
    and a compact daily-score row beneath it. All blocks share a single colorbar.

    Parameters
    ----------
    season_results : list[(block_key, decomp_key, channel_fields, fit_result)]
    season_label   : str — e.g. 'Winter (DJF)'
    figures_dir    : Path
    season_key     : str — used for the output filename

    Returns
    -------
    Path to the saved figure, or None if season_results is empty.
    """
    from matplotlib.colors import TwoSlopeNorm

    if not season_results:
        return None

    n_blocks  = len(season_results)
    max_modes = max(res.scores.shape[1] for _, _, _, res in season_results)

    cartopy_crs = _get_cartopy_crs()
    plate_carree = cartopy_crs.PlateCarree() if cartopy_crs is not None else None
    diverging_norm = TwoSlopeNorm(vmin=-1.0, vcenter=0.0, vmax=1.0)

    map_h, score_h = 2.0, 0.85
    height_ratios = [map_h, score_h] * n_blocks

    fig_width  = max(8.0, max_modes * 2.8)
    fig_height = n_blocks * (map_h + score_h) * 0.90 + 1.2

    fig = plt.figure(figsize=(fig_width, fig_height))
    gs  = fig.add_gridspec(
        n_blocks * 2, max_modes,
        height_ratios=height_ratios,
        hspace=0.65, wspace=0.15,
    )

    subplot_kw = {"projection": plate_carree} if plate_carree is not None else {}
    scatter_axes = []

    for bi, (block_key, decomp_key, channel_fields, fit_result) in enumerate(season_results):
        map_row   = bi * 2
        score_row = bi * 2 + 1

        loadings_list = fit_result.diagnostics["loadings_list"]
        var_exp       = fit_result.diagnostics["var_explained"]["VMAX"]
        oriented_loadings, oriented_scores = _orient_modes(loadings_list, fit_result.scores)
        n_modes = oriented_scores.shape[1]

        channel_field   = channel_fields[0]
        channel_loading = oriented_loadings[0]
        flat_lats, flat_lons, msz = _extract_grid_coords(channel_loading)
        loading_vals = channel_loading.values  # (n_modes, ...)

        decomp_full  = DECOMPOSITIONS[decomp_key][1]
        decomp_short = " ".join(decomp_full.split()[:2])
        row_label    = f"{decomp_short}\n{FIELD_LABELS.get(channel_field, channel_field)}"

        for mi in range(max_modes):
            map_ax = fig.add_subplot(gs[map_row, mi], **subplot_kw)

            if mi < n_modes:
                mode_v = loading_vals[mi].ravel()
                norm_v = mode_v / (np.nanmax(np.abs(mode_v)) + 1e-12)
                sc_kw  = dict(c=norm_v, cmap="RdBu_r", norm=diverging_norm,
                              s=msz, rasterized=True)
                if plate_carree is not None:
                    sc_kw["transform"] = plate_carree
                map_ax.scatter(flat_lons, flat_lats, **sc_kw)
                scatter_axes.append(map_ax)
                _draw_texas(map_ax)
                map_ax.set_title(
                    f"Mode {mi + 1}  ({var_exp[mi] * 100:.1f}%)",
                    fontsize=7, pad=2,
                )
            else:
                map_ax.axis("off")

            if mi == 0:
                map_ax.text(-0.12, 0.5, row_label,
                            transform=map_ax.transAxes, rotation=90,
                            va="center", ha="right", fontsize=6)

            score_ax = fig.add_subplot(gs[score_row, mi])
            if mi < n_modes:
                _plot_daily_scores(
                    score_ax,
                    oriented_scores.iloc[:, mi],
                    f"Mode {mi + 1} daily scores",
                )
                if mi == 0:
                    score_ax.set_ylabel("Score", fontsize=6)
            else:
                score_ax.axis("off")

    if scatter_axes:
        sm = plt.cm.ScalarMappable(cmap="RdBu_r", norm=diverging_norm)
        sm.set_array([])

    fig.suptitle(f"Varimax EOF Decomposition — {season_label}", fontsize=10, y=0.998)
    output_path = figures_dir / f"eof_decomposition_{season_key}.png"
    return _save_fig(fig, output_path)


def fit_eof_modes(bundle, fields=ALL_FIELDS, K=None, seed=RANDOM_STATE):
    """Fit a Varimax-rotated EOF independently on each channel and return its modes.

    EOFs are fit on all hours (no train/test split). For each channel we persist the
    linear projection-weight operator W (one spatial weight map per mode) that
    reproduces the fitted scores: score = (data − field_mean) · W. W is recovered by
    least squares against the fitted scores. Each mode is sign-oriented so a positive
    score corresponds to above-average values of the underlying field.

    Parameters
    ----------
    bundle : dict from load_channel_fields
    fields : list of channel names (must be keys of CHANNEL_SPEC)
    K      : int or dict {block_key: int} — rotated modes per channel; defaults
             to default_modes (the per-block dict)
    seed   : int — solver random_state

    Returns
    -------
    modes  : dict {field: {components, evr, lat, lon, field_mean}}
             components is (K_field, n_cells) — the projection-weight maps per mode.
    var_df : pd.DataFrame — explained variance per field × mode
    """
    if K is None:
        K = default_modes
    if isinstance(K, int):
        K = {bk: K for bk in default_modes}

    hours = bundle["hours"]
    all_idx = np.arange(len(hours))
    print(f"  Common hours: {len(hours)} (EOFs fit on all hours)")

    model_grid = {}
    for field in fields:
        model = CHANNEL_SPEC[field][0]
        if model not in model_grid:
            da_ref = bundle["channel_da"][field]
            model_grid[model] = np.meshgrid(
                da_ref["longitude"].values, da_ref["latitude"].values
            )

    modes, var_rows = {}, []
    for field in fields:
        land_mask = ~bundle["nan_all"][field]
        lon_2d, lat_2d = model_grid[CHANNEL_SPEC[field][0]]
        k_field = K.get(_FIELD_TO_BLOCK_KEY.get(field, ""), DEFAULT_K)

        result = fit_varimax(
            bundle, all_idx, hours, K=k_field, error_fields=[field], seed=seed,
        )
        evr    = np.asarray(result.diagnostics["var_explained"]["VMAX"], dtype=float)
        scores = np.asarray(result.scores.values, dtype=float)
        K_eff  = scores.shape[1]

        da         = bundle["channel_da"][field]
        data       = da.values[:, land_mask]
        field_mean = da.mean("valid_time").values[land_mask]
        anomaly    = data - field_mean[np.newaxis, :]
        W          = np.linalg.lstsq(anomaly, scores, rcond=None)[0]

        signs = np.where(W.sum(axis=0) < 0, -1.0, 1.0)
        W    *= signs

        modes[field] = {
            "components": W.T.astype(np.float32),
            "evr":        evr,
            "lat":        lat_2d[land_mask].astype(float),
            "lon":        lon_2d[land_mask].astype(float),
            "field_mean": field_mean.astype(np.float32),
        }

        cumvar = np.cumsum(evr) * 100
        for k in range(K_eff):
            var_rows.append({
                "field": field, "mode": k + 1,
                "var_pct": float(evr[k] * 100), "cumvar_pct": float(cumvar[k]),
            })
        print(f"  {FIELD_LABELS.get(field, field)}: {K_eff} varimax modes, "
              f"cumvar={cumvar[-1]:.1f}% ({int(land_mask.sum())} cells)")

    return modes, pd.DataFrame(var_rows)


def save_eof_modes(modes, var_df, months, K, eof_dir, cut_key):
    """Persist per-channel EOF modes for one sample cut to disk.

    Files are named with both the mode-count tag and the cut key so that modes
    for every cut can coexist in the same directory.

    Parameters
    ----------
    modes   : dict {field: {components, evr, lat, lon, field_mean}}
    var_df  : pd.DataFrame — explained variance per field × mode
    months  : list of (year, month)
    K       : dict {block_key: int}
    eof_dir : Path
    cut_key : str — key from SAMPLE_CUT (e.g. "all", "winter", "ruc")
    """
    eof_dir = Path(eof_dir)
    eof_dir.mkdir(parents=True, exist_ok=True)
    tag = _modes_tag(K)

    npz_data = {
        f"{field}__{key}": m[key]
        for field, m in modes.items()
        for key in ("components", "evr", "lat", "lon", "field_mean")
    }
    stem = f"eof_modes_K{tag}_{cut_key}"
    np.savez_compressed(eof_dir / f"{stem}.npz", **npz_data)
    var_df.to_csv(eof_dir / f"eof_variance_explained_K{tag}_{cut_key}.csv", index=False)
    meta_path = eof_dir / f"{stem}_meta.json"
    meta_path.write_text(json.dumps({"K": K, "cut_key": cut_key,
                                     "months": [list(m) for m in months]}))
    print(f"  Saved: {eof_dir / f'{stem}.npz'}")


def load_eof_modes(K, eof_dir, cut_key):
    """Load per-channel EOF modes for one sample cut saved by save_eof_modes.

    Parameters
    ----------
    K       : dict {block_key: int}
    eof_dir : Path
    cut_key : str — key from SAMPLE_CUT (e.g. "all", "winter", "ruc")

    Returns
    -------
    modes  : dict {field: {components, evr, lat, lon, field_mean}}
    var_df : pd.DataFrame
    months : list of (year, month)
    """
    eof_dir = Path(eof_dir)
    tag     = _modes_tag(K)
    stem    = f"eof_modes_K{tag}_{cut_key}"
    npz     = np.load(eof_dir / f"{stem}.npz")
    fields  = list(dict.fromkeys(key.rsplit("__", 1)[0] for key in npz.files))
    modes   = {
        field: {key: npz[f"{field}__{key}"]
                for key in ("components", "evr", "lat", "lon", "field_mean")}
        for field in fields
    }
    var_df = pd.read_csv(eof_dir / f"eof_variance_explained_K{tag}_{cut_key}.csv")
    meta   = json.loads((eof_dir / f"{stem}_meta.json").read_text())
    months = [tuple(m) for m in meta["months"]]
    return modes, var_df, months


def project_onto_modes(bundle, modes):
    """Project channel data onto saved EOF modes to create per-hour scores.

    For each field the channel anomaly (data minus the modes' field mean) is
    dotted with the rotated loading patterns, reproducing the EOF transform.

    Parameters
    ----------
    bundle : dict from load_channel_fields — must cover the same months as the modes
    modes  : dict {field: {components, lat, lon, field_mean, ...}}

    Returns
    -------
    scores_dict : dict {field: ndarray (T, K)}
    """
    scores_dict = {}
    for field, m in modes.items():
        da        = bundle["channel_da"][field]
        land_mask = ~bundle["nan_all"][field]
        data      = da.values[:, land_mask]
        anomaly   = data - m["field_mean"][np.newaxis, :]
        scores_dict[field] = (anomaly @ m["components"].T).astype(np.float32)
    return scores_dict


def modes_to_pca_dict(modes):
    """Adapt saved modes to the (pca_dict, lat_dict, lon_dict) shape plotters expect.

    Parameters
    ----------
    modes : dict {field: {components, evr, lat, lon, ...}}

    Returns
    -------
    pca_dict : dict {field: SimpleNamespace(.components_, .explained_variance_ratio_)}
    lat_dict : dict {field: ndarray}
    lon_dict : dict {field: ndarray}
    """
    pca_dict, lat_dict, lon_dict = {}, {}, {}
    for field, m in modes.items():
        pca_dict[field] = SimpleNamespace(
            components_=m["components"],
            explained_variance_ratio_=m["evr"],
        )
        lat_dict[field] = m["lat"]
        lon_dict[field] = m["lon"]
    return pca_dict, lat_dict, lon_dict


def run_eof_decomposition(K=None, months=None):
    """Phase 2: for each sample cut, fit Varimax EOFs, save modes, and produce figures.

    Loads channel fields once, then for every cut in SAMPLE_CUT:
      1. Subsets the bundle to that cut's hours.
      2. Fits per-channel Varimax EOFs and saves the projection-weight modes to
         disk as `eof_modes_K{tag}_{cut_key}.npz` (consumed by Phase 3).
      3. Fits per-block Varimax EOFs on the cut's hours and saves a combined
         loading-map / daily-score figure for that cut.

    Parameters
    ----------
    K      : int or dict {block_key: int} — rotated modes per channel; defaults
             to default_modes
    months : list of (year, month); defaults to all 12 months of 2025

    Returns
    -------
    dict with keys 'bundle' and 'months' (for passing to run_eof_analysis).
    """
    if K is None:
        K = default_modes
    if isinstance(K, int):
        K = {bk: K for bk in default_modes}
    months = months or ALL_MONTHS

    dirs        = setup_directories()
    eof_dir     = Path(dirs["processed"]) / "eof"
    figures_dir = Path(dirs["figures"]) / "eof_decomposition"
    eof_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)

    print("\n=== Phase 2: Loading channel fields ===")
    bundle      = load_channel_fields(months, dirs)
    outcomes_df = load_outcomes(dirs)
    all_hours   = bundle["hours"]

    for cut_key, cut in SAMPLE_CUT.items():
        cut_mask = cut.mask(all_hours, outcomes_df)
        n_hours  = int(cut_mask.sum())
        print(f"\n=== Phase 2 [{cut.label}]: {n_hours} hours ===")
        if n_hours < max(K.values()) + 10:
            print(f"  Skipping — too few hours ({n_hours})")
            continue

        cut_bundle = _subset_bundle(bundle, cut_mask)

        # Step 1: fit the per-channel projection modes and save to disk.
        modes, var_df = fit_eof_modes(cut_bundle, ALL_FIELDS, K=K)
        save_eof_modes(modes, var_df, months, K, eof_dir, cut_key)

        # Step 2: fit per-block varimax for the loading-map + daily-score figure.
        block_results = []
        for decomp_key, (channel_fields, _) in DECOMPOSITIONS.items():
            for block_key, _, block_fields in _decomposition_blocks(decomp_key, channel_fields):
                n_block = K.get(_FIELD_TO_BLOCK_KEY.get(block_fields[0], ""), DEFAULT_K)
                if n_hours < n_block + 10:
                    continue
                print(f"  {block_key:20s}: n_modes={n_block}")

                fit_result = fit_varimax(
                    cut_bundle, np.arange(n_hours), cut_bundle["hours"],
                    K=n_block, error_fields=block_fields, seed=RANDOM_STATE,
                )

                fit_result.scores.columns = [
                    f"{block_key}_PC{i + 1}" for i in range(fit_result.scores.shape[1])
                ]
                block_results.append((block_key, decomp_key, block_fields, fit_result))

        figure_path = plot_season_figure(block_results, cut.label, figures_dir, cut_key)
        if figure_path:
            print(f"  Figure: {figure_path}")

    print(f"\nDone — modes saved to {eof_dir}")
    return {"bundle": bundle, "months": months}


# ═══════════════════════════════════════════════════════════════════════════════
# Section 3: EOF Regression Analysis
# ═══════════════════════════════════════════════════════════════════════════════

def run_eof_analysis(K=None, depvars=None, months=None, bundle=None):
    """Phase 3: for each sample cut, load its saved modes and regress against outcomes.

    For every cut in SAMPLE_CUT:
      1. Loads the cut-specific EOF modes saved by Phase 2.
      2. Subsets the bundle to that cut's hours and projects onto the cut's modes.
      3. For each outcome variable runs OLS with HAC standard errors and saves
         coefficient tables.
      4. Produces eof_coefs_across_outcomes figures and an F-test heatmap for that
         cut under figures/eof_analysis/{cut_key}/.

    If modes for a cut are missing, Phase 2 is run automatically to produce them.

    Parameters
    ----------
    K       : int or dict {block_key: int} — EOF modes per channel; defaults to
              default_modes
    depvars : list of str — outcome variable keys; defaults to all DEPVARS
    months  : list of (year, month) — used when bundle must be loaded from disk
    bundle  : dict or None — pre-loaded channel bundle (from run_eof_decomposition);
              if None the bundle is loaded from disk using `months`

    Returns
    -------
    dict {cut_key: {depvar: result_dict}}
    """
    if K is None:
        K = default_modes
    if isinstance(K, int):
        K = {bk: K for bk in default_modes}
    if depvars is None:
        depvars = DEPVARS

    dirs        = setup_directories()
    base_fig_dir = Path(dirs["figures"]) / "eof_analysis"
    tables_dir  = Path(dirs["tables"])
    eof_dir     = Path(dirs["processed"]) / "eof"
    tables_dir.mkdir(parents=True, exist_ok=True)
    eof_dir.mkdir(parents=True, exist_ok=True)

    print("\n=== Phase 3: Loading outcomes ===")
    outcomes_df = load_outcomes(dirs)

    if bundle is None:
        bundle = load_channel_fields(months or ALL_MONTHS, dirs)
    all_hours = bundle["hours"]
    K_scalar  = max(K.values())

    all_cut_results = {}

    for cut_key, cut in SAMPLE_CUT.items():
        print(f"\n{'='*60}")
        print(f"=== Phase 3 [{cut.label}] ===")
        print(f"{'='*60}")

        cut_mask = cut.mask(all_hours, outcomes_df)
        n_cut    = int(cut_mask.sum())
        if n_cut < 200:
            print(f"  Skipping — only {n_cut} hours")
            continue

        # Load saved modes for this cut, running Phase 2 if they're missing.
        modes_file = eof_dir / f"eof_modes_K{_modes_tag(K)}_{cut_key}.npz"
        if not modes_file.exists():
            print(f"  No saved modes for '{cut_key}' — running Phase 2 ...")
            run_eof_decomposition(K=K, months=months)
        modes, var_df, _ = load_eof_modes(K, eof_dir, cut_key)

        # Subset bundle, project onto cut-specific modes to get per-hour scores.
        cut_bundle  = _subset_bundle(bundle, cut_mask)
        scores_dict = project_onto_modes(cut_bundle, modes)
        cut_hours   = cut_bundle["hours"]
        pca_dict, lat_dict, lon_dict = modes_to_pca_dict(modes)
        print(f"  Projected {n_cut} hours onto {len(modes)} channel mode sets")

        cut_fig_dir = base_fig_dir / cut_key
        cut_fig_dir.mkdir(parents=True, exist_ok=True)
        cut_table_dir = tables_dir / "eof" / cut_key
        cut_table_dir.mkdir(parents=True, exist_ok=True)

        cut_results = {}
        cut_f_tests = {}

        for depvar in depvars:
            if depvar not in outcomes_df.columns:
                print(f"  WARNING: {depvar} not in outcomes — skipping")
                continue
            cfg       = DEPVAR_CONFIGS.get(depvar, {"label": depvar, "transform": "log1p"})
            transform = cfg["transform"]

            X_df, y, _, feature_groups = build_regression_matrix(
                scores_dict, ERROR_FIELDS, REALIZED_FIELDS, cut_hours,
                outcomes_df[depvar], K=K_scalar, transform=transform,
            )
            if len(X_df) < 200:
                print(f"  {depvar}: only {len(X_df)} clean hours — skipping")
                continue

            X_std, _ = standardize_pca_cols(X_df, np.ones(len(X_df), dtype=bool))
            ols_result, f_tests, col_names = run_ols_inference(y, X_std, feature_groups)
            cut_f_tests[depvar] = f_tests

            r2_nat = (_r2(np.expm1(y), np.expm1(ols_result.fittedvalues))
                      if transform == "log1p"
                      else _r2(y, ols_result.fittedvalues))
            print(f"  {depvar}: R²={r2_nat:.4f}  N={len(X_df)}")

            save_coef_table(ols_result, col_names,
                            cut_table_dir / f"eof_ols_coefficients_{depvar}.csv")
            cut_results[depvar] = {"ols_result": ols_result, "col_names": col_names,
                                   "feature_groups": feature_groups, "r2_nat": r2_nat,
                                   "n_hours": len(X_df)}

        if not cut_results:
            print(f"  No depvars completed for {cut.label} — skipping plots")
            continue

        plot_pca_coefs_across_outcomes(
            pca_dict, lat_dict, lon_dict,
            {k: v for k, v in cut_results.items() if k in DEPVARS},
            cut_fig_dir, fname_prefix="eof",
        )
        plot_ftest_heatmap(
            cut_f_tests, cut_fig_dir,
            fname_prefix="eof", method_label=f"EOF [{cut.label}]",
        )
        all_cut_results[cut_key] = cut_results

    return all_cut_results


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    """CLI entry point for the three-phase EOF analysis workflow."""
    parser = argparse.ArgumentParser(
        description="EOF analysis of ERCOT weather-error fields (three phases)"
    )
    parser.add_argument(
        "--task", choices=["significance", "decompose", "analyze", "all"],
        default="all",
        help=(
            "Phase to run: "
            "significance (Phase 1 — scree plots), "
            "decompose (Phase 2 — fit/save modes + figures), "
            "analyze (Phase 3 — regressions), "
            "all (default, runs all three in order)"
        ),
    )
    parser.add_argument(
        "--n_modes", type=int, default=None,
        help="EOF modes for all channels uniformly (overrides default_modes dict)",
    )
    parser.add_argument(
        "--depvars", nargs="*", default=None,
        help="Outcome variables for Phase 3 (default: all)",
    )
    args = parser.parse_args()

    K = args.n_modes  # None → functions use default_modes

    if args.task in ("significance", "all"):
        run_eof_significance(n_scree_modes=SIG_N_MODES, n_boot=N_BOOTSTRAPS)
    decomp_bundle = None
    if args.task in ("decompose", "all"):
        result = run_eof_decomposition(K=K)
        decomp_bundle = result.get("bundle")
    if args.task in ("analyze", "all"):
        run_eof_analysis(K=K, depvars=args.depvars, bundle=decomp_bundle)


if __name__ == "__main__":
    main()
