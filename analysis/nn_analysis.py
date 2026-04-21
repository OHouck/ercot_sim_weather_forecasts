"""
MLP analysis for ERCOT curtailment prediction from spatial forecast error fields.

Implements a 3-block MLP trained on a chunk-based train/val/test split that
prevents temporal leakage from high hourly autocorrelation. Data is divided
into 5-day blocks, shuffled, and split 70/15/15 across train/val/test.

Spatial weather fields are kept as (T, C, H, W) tensors so that future CNN
or FNO architectures can be swapped in without changing the data pipeline.
The MLP flattens the spatial grid and appends scalar temporal controls
(hour_of_day, is_weekend, month) before the linear layers.

Experiments:
  channels    — ablation over which spatial channels to include
  regime      — regime-stratified evaluation and regime-specific training
  saliency    — per-channel gradient saliency maps
  overfitting — diagnostics comparing leaky vs clean splits

Usage:
    uv run python -m analysis.nn_analysis --exp all
    uv run python -m analysis.nn_analysis --exp channels
    uv run python -m analysis.nn_analysis --exp regime
    uv run python -m analysis.nn_analysis --exp saliency
    uv run python -m analysis.nn_analysis --exp overfitting
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

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories
from analysis.pixel_level_functional_analysis import (
    DEPVAR as PFA_DEPVAR,
    REGIME_EXTRA_COLS,
    prepare_functional_data,
    prepare_multi_field_data,
    prepare_grid_data_coarse,
    _build_era5_grid_index,
    _select_device,
    REGIMES,
)
from analysis.pixel_regression_maps import (
    _draw_texas_base as _draw_prm_texas_base,
    _draw_texas_borders,
    _draw_overlays,
)

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# ── Configuration ─────────────────────────────────────────────────────────────

DEPVAR = "economic_congestion_cost"
ERROR_FIELDS = ["wspd_error_1h", "temp_error_1h", "wspd_error_0h", "temp_error_0h"]
# Spatial fields: each has a value per pixel per hour → forms (C, H, W) grid channels
SPATIAL_CONTROL_FIELDS = ["era5_temp", "era5_wspd", "actual_load"]
SPATIAL_FIELDS = ERROR_FIELDS + SPATIAL_CONTROL_FIELDS
# Scalar controls (hour_of_day, is_weekend, month) are derived from timestamps

ERROR_FIELD_LABELS = {
    "wspd_error_1h": "HRRR 1h — Wind Speed Error",
    "temp_error_1h": "HRRR 1h — Temperature Error",
    "wspd_error_0h": "GFS Day-Ahead — Wind Speed Error",
    "temp_error_0h": "GFS Day-Ahead — Temperature Error",
}

DEFAULT_MONTHS = [(2025, m) for m in [1, 3, 5, 7, 9, 11]]
CHUNK_DAYS = 5       # size of each temporal block
TRAIN_FRAC = 0.70
VAL_FRAC = 0.15
# TEST_FRAC is implicitly 1 - TRAIN_FRAC - VAL_FRAC = 0.15
RANDOM_STATE = 42
N_EPOCHS = 150
PATIENCE = 20
BATCH_SIZE = 32
LR = 5e-4
WEIGHT_DECAY = 1e-4
MIN_REGIME_SAMPLES = 50


# ── Data loading ───────────────────────────────────────────────────────────────

def load_pixel_data_for_nn(months):
    """Load pixel-hourly data with all columns needed for NN analysis.

    Extends the regime data columns with actual_load (per zone, used as a
    spatial proxy field). Silently drops columns not present in the parquet.

    Parameters
    ----------
    months : list of (year, month) tuples

    Returns
    -------
    pd.DataFrame
    """
    import pyarrow.parquet as pq

    dirs = setup_directories()
    lmp_dir = Path(dirs["processed"]) / "combined_hourly_gridded_data"

    keep_cols = (
        ["pixel_id", "valid_time", "latitude", "longitude", PFA_DEPVAR]
        + ERROR_FIELDS
        + REGIME_EXTRA_COLS
        + ["actual_load"]
    )

    dfs = []
    for year, month in months:
        path = lmp_dir / f"pixel_hourly_gfs+hrrr_{year}_{month:02d}.parquet"
        if not path.exists():
            print(f"  [WARNING] Missing: {path}")
            continue
        parquet_cols = pq.read_schema(path).names
        cols_to_read = [c for c in keep_cols if c in parquet_cols]
        df = pd.read_parquet(path, columns=cols_to_read)
        df["valid_time"] = pd.to_datetime(df["valid_time"])
        if df["valid_time"].dt.tz is not None:
            df["valid_time"] = df["valid_time"].dt.tz_localize(None)
        print(f"  Loaded {year}-{month:02d}: {len(df):,} rows")
        dfs.append(df)
    if not dfs:
        raise FileNotFoundError("No pixel_hourly parquet files found.")
    return pd.concat(dfs, ignore_index=True)


def make_chunk_splits(hour_idx, chunk_days=CHUNK_DAYS, train_frac=TRAIN_FRAC,
                      val_frac=VAL_FRAC, seed=RANDOM_STATE):
    """Split a time index into train/val/test using shuffled temporal chunks.

    Groups consecutive hours into chunk_days-day blocks, shuffles the blocks,
    then assigns 70/15/15 to train/val/test. This prevents temporal leakage
    from autocorrelation while ensuring all three sets cover the full date range.

    Parameters
    ----------
    hour_idx   : pd.DatetimeIndex — timestamps for each observation (T,)
    chunk_days : int — number of consecutive days per block
    train_frac, val_frac : float — fraction of chunks; test gets the remainder
    seed       : int

    Returns
    -------
    train_idx, val_idx, test_idx : ndarray of int — row positions into T
    """
    dates = hour_idx.normalize()
    unique_dates = pd.DatetimeIndex(sorted(dates.unique()))

    chunk_ids = np.arange(len(unique_dates)) // chunk_days
    n_chunks = int(chunk_ids.max()) + 1

    rng = np.random.default_rng(seed)
    shuffled = rng.permutation(n_chunks)

    n_train = int(np.floor(n_chunks * train_frac))
    n_val = int(np.floor(n_chunks * val_frac))
    train_chunks = set(shuffled[:n_train].tolist())
    val_chunks = set(shuffled[n_train:n_train + n_val].tolist())

    date_to_chunk = pd.Series(chunk_ids, index=unique_dates)
    row_chunks = hour_idx.normalize().map(date_to_chunk).values

    train_rows = np.where(np.isin(row_chunks, list(train_chunks)))[0]
    val_rows = np.where(np.isin(row_chunks, list(val_chunks)))[0]
    test_rows = np.where(
        ~np.isin(row_chunks, list(train_chunks | val_chunks))
    )[0]

    return train_rows, val_rows, test_rows


def load_multi_field_data(months, spatial_fields=None, df=None):
    """Load pixel-hourly data and build spatial grid tensor plus scalar controls.

    Spatial fields are kept as (T, C, H, W) — suitable for MLP (flattened),
    CNN, or FNO. Scalar temporal controls are derived from timestamps and
    returned separately so architectures can handle them as needed.

    Fields absent from the dataframe are silently skipped; `loaded_fields`
    reflects what was actually loaded, so callers must use it (not SPATIAL_FIELDS)
    when mapping channels to names.

    Parameters
    ----------
    months         : list of (year, month) tuples
    spatial_fields : list of field names for spatial channels;
                     defaults to SPATIAL_FIELDS
    df             : pre-loaded DataFrame (optional); must contain all fields

    Returns
    -------
    grid_spatial    : ndarray (T, C, H, W) float32
    scalar_controls : ndarray (T, 3) float32 — [hour_of_day, is_weekend, month]
    Y               : ndarray (T,)
    pixel_coords    : ndarray (N_pixels, 2)
    hour_idx        : pd.DatetimeIndex
    loaded_fields   : list of str — channel names in grid channel order
    """
    if spatial_fields is None:
        spatial_fields = SPATIAL_FIELDS
    if df is None:
        df = load_pixel_data_for_nn(months)

    # Filter to fields present in the dataframe before pivoting
    available = [f for f in spatial_fields if f in df.columns]
    if not available:
        raise RuntimeError("No requested fields found in dataframe.")
    missing = set(spatial_fields) - set(available)
    if missing:
        print(f"  Skipping fields not in dataframe: {sorted(missing)}")

    X_dict, Y_ref, _, pixel_ids_ref, hour_idx_ref = prepare_multi_field_data(
        df, error_fields=available
    )

    # Rebuild pixel_coords directly from df: prepare_multi_field_data's internal
    # coord_map.loc lookup silently returns NaN when the pivot column Index type
    # doesn't align with the coord_map index. Rebuilding here is reliable.
    coord_df = (
        df[["pixel_id", "latitude", "longitude"]]
        .dropna(subset=["latitude", "longitude"])
        .drop_duplicates("pixel_id")
        .set_index("pixel_id")
    )
    pixel_coords_ref = coord_df.reindex(pixel_ids_ref)[["latitude", "longitude"]].values

    # Drop any pixels still missing coords (genuinely absent from df)
    valid_px = ~np.isnan(pixel_coords_ref).any(axis=1)
    if not valid_px.all():
        print(f"  Dropping {(~valid_px).sum()} pixels with NaN coordinates")
        pixel_coords_ref = pixel_coords_ref[valid_px]
        X_dict = {k: v[:, valid_px] for k, v in X_dict.items()}

    loaded_fields = list(X_dict.keys())
    grid_spatial = build_grid_tensor(X_dict, pixel_coords_ref, resolution=0.25)

    scalar_controls = np.column_stack([
        hour_idx_ref.hour.values.astype(np.float32),
        (hour_idx_ref.dayofweek >= 5).astype(np.float32),
        hour_idx_ref.month.values.astype(np.float32),
    ])

    return grid_spatial, scalar_controls, Y_ref, pixel_coords_ref, hour_idx_ref, loaded_fields


def _coarse_grid_indices(pixel_coords, resolution):
    """Map pixel coordinates onto a coarse regular grid.

    Parameters
    ----------
    pixel_coords : ndarray (N_pixels, 2) — (lat, lon)
    resolution   : float — grid spacing in degrees

    Returns
    -------
    lat_idx, lon_idx : ndarray (N_pixels,) int
    H, W             : int — grid dimensions
    """
    lats, lons = pixel_coords[:, 0], pixel_coords[:, 1]
    lat_min = np.floor(lats.min() / resolution) * resolution
    lon_min = np.floor(lons.min() / resolution) * resolution
    lat_max = np.ceil(lats.max() / resolution) * resolution
    lon_max = np.ceil(lons.max() / resolution) * resolution
    H = len(np.arange(lat_min, lat_max + resolution * 0.5, resolution))
    W = len(np.arange(lon_min, lon_max + resolution * 0.5, resolution))
    lat_idx = np.round((lats - lat_min) / resolution).astype(int).clip(0, H - 1)
    lon_idx = np.round((lons - lon_min) / resolution).astype(int).clip(0, W - 1)
    return lat_idx, lon_idx, H, W


def build_grid_tensor(X_dict, pixel_coords, resolution=0.25):
    """Regrid pixel arrays to a regular lat/lon grid tensor.

    Parameters
    ----------
    X_dict       : dict {field: ndarray (T, N_pixels)}
    pixel_coords : ndarray (N_pixels, 2)
    resolution   : float — grid spacing in degrees; ≤0.1 uses native ERA5 grid

    Returns
    -------
    grid : ndarray (T, C, H, W) float32
    """
    if resolution <= 0.1:
        _, _, lat_idx, lon_idx, H, W = _build_era5_grid_index(pixel_coords)
        T = next(iter(X_dict.values())).shape[0]
        C = len(X_dict)
        grid = np.zeros((T, C, H, W), dtype=np.float32)
        X_stack = np.stack(
            [v.astype(np.float32) for v in X_dict.values()], axis=1
        )
        grid[:, :, lat_idx, lon_idx] = X_stack
        return grid
    grid, _, _ = prepare_grid_data_coarse(X_dict, pixel_coords, target_res=resolution)
    return grid


# ── MLP architecture ───────────────────────────────────────────────────────────

def _build_mlp(in_dim, hidden=512, dropout=0.3):
    """3-block MLP: Linear → LayerNorm → GELU → Dropout, then scalar head.

    Parameters
    ----------
    in_dim  : int — input dimension (spatial_dim + n_scalar)
    hidden  : int — first hidden layer width
    dropout : float — dropout probability

    Returns
    -------
    nn.Sequential
    """
    import torch.nn as nn
    return nn.Sequential(
        nn.Linear(in_dim, hidden),
        nn.LayerNorm(hidden),
        nn.GELU(),
        nn.Dropout(dropout),
        nn.Linear(hidden, hidden // 2),
        nn.LayerNorm(hidden // 2),
        nn.GELU(),
        nn.Dropout(dropout),
        nn.Linear(hidden // 2, 128),
        nn.GELU(),
        nn.Linear(128, 1),
    )


def _make_mlp_cls(spatial_dim, n_scalar):
    """Return an MLP class that accepts separate spatial and scalar inputs.

    The forward signature (x_spatial, x_scalar) is intentional: future
    architectures (CNN, FNO) can replace the spatial branch without changing
    the training loop or scalar handling.

    Parameters
    ----------
    spatial_dim : int — C * H * W (flattened spatial grid)
    n_scalar    : int — number of scalar temporal controls

    Returns
    -------
    class — instantiates to nn.Module
    """
    import torch
    import torch.nn as nn

    class MLPScalar(nn.Module):
        def __init__(self):
            super().__init__()
            self.net = _build_mlp(spatial_dim + n_scalar)

        def forward(self, x_spatial, x_scalar):
            """Forward pass: flatten spatial grid, concat scalars, pass through MLP.

            Parameters
            ----------
            x_spatial : Tensor (B, C, H, W) or (B, spatial_dim)
            x_scalar  : Tensor (B, n_scalar)

            Returns
            -------
            Tensor (B,)
            """
            flat = x_spatial.reshape(x_spatial.shape[0], -1)
            x = torch.cat([flat, x_scalar], dim=1)
            return self.net(x).squeeze(-1)

    return MLPScalar


# ── Training helpers ──────────────────────────────────────────────────────────

def _r2(y_true, y_pred):
    """Compute R² (coefficient of determination).

    Parameters
    ----------
    y_true, y_pred : ndarray (N,)

    Returns
    -------
    float
    """
    ss_res = ((y_true - y_pred) ** 2).sum()
    ss_tot = ((y_true - y_true.mean()) ** 2).sum()
    return float(1 - ss_res / (ss_tot + 1e-10))


def _count_params(model):
    """Count trainable parameters in a PyTorch model."""
    return sum(p.numel() for p in model.parameters() if p.requires_grad)


def train_with_splits(ModelClass, grid_np, scalar_np, Y_np,
                      train_idx, val_idx, test_idx, device,
                      n_epochs=N_EPOCHS, patience=PATIENCE,
                      batch_size=BATCH_SIZE, lr=LR, weight_decay=WEIGHT_DECAY):
    """Train on train split, early-stop on val, evaluate on test.

    Y and scalar controls are normalized using train-set statistics only
    to prevent any information leakage through normalization.

    Parameters
    ----------
    ModelClass                   : callable → nn.Module with forward(x_spatial, x_scalar)
    grid_np                      : ndarray (T, C, H, W)
    scalar_np                    : ndarray (T, n_scalar) — un-normalized
    Y_np                         : ndarray (T,) — un-normalized target
    train_idx, val_idx, test_idx : ndarray of int
    device                       : torch.device
    n_epochs, patience, batch_size, lr, weight_decay : training hyperparams

    Returns
    -------
    dict with train_r2, val_r2, test_r2, model (on CPU, best checkpoint)
    """
    import torch
    import torch.nn as nn
    import torch.nn.functional as F
    from torch.utils.data import TensorDataset, DataLoader

    # Normalize using train statistics only
    Y_mean = float(Y_np[train_idx].mean())
    Y_std = float(Y_np[train_idx].std()) + 1e-8
    Y_norm = ((Y_np - Y_mean) / Y_std).astype(np.float32)

    sc_mean = scalar_np[train_idx].mean(axis=0)
    sc_std = scalar_np[train_idx].std(axis=0) + 1e-8
    scalar_norm = ((scalar_np - sc_mean) / sc_std).astype(np.float32)

    grid_t = torch.tensor(grid_np, dtype=torch.float32)
    scalar_t = torch.tensor(scalar_norm, dtype=torch.float32)
    Y_t = torch.tensor(Y_norm, dtype=torch.float32)

    X_tr = grid_t[train_idx].to(device)
    S_tr = scalar_t[train_idx].to(device)
    Y_tr = Y_t[train_idx].to(device)
    X_val = grid_t[val_idx].to(device)
    S_val = scalar_t[val_idx].to(device)
    Y_val = Y_t[val_idx].to(device)
    X_te = grid_t[test_idx].to(device)
    S_te = scalar_t[test_idx].to(device)
    Y_te = Y_t[test_idx].to(device)

    model = ModelClass().to(device)
    print(f"    Parameters: {_count_params(model):,}")

    steps_per_epoch = max(1, (len(train_idx) + batch_size - 1) // batch_size)
    opt = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=weight_decay)
    scheduler = torch.optim.lr_scheduler.OneCycleLR(
        opt, max_lr=lr, epochs=n_epochs, steps_per_epoch=steps_per_epoch,
        pct_start=0.1, anneal_strategy="cos",
    )

    loader = DataLoader(
        TensorDataset(X_tr, S_tr, Y_tr), batch_size=batch_size, shuffle=True
    )

    best_val = float("inf")
    best_state = None
    patience_count = 0

    for _ in range(n_epochs):
        model.train()
        for xb, sb, yb in loader:
            opt.zero_grad()
            loss = F.mse_loss(model(xb, sb), yb)
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            opt.step()
            scheduler.step()

        model.eval()
        with torch.no_grad():
            val_loss = F.mse_loss(model(X_val, S_val), Y_val).item()

        if val_loss < best_val - 1e-5:
            best_val = val_loss
            best_state = {k: v.clone() for k, v in model.state_dict().items()}
            patience_count = 0
        else:
            patience_count += 1
            if patience_count >= patience:
                break

    if best_state is not None:
        model.load_state_dict(best_state)

    model.eval()
    with torch.no_grad():
        y_pred_tr = model(X_tr, S_tr).cpu().numpy()
        y_pred_val = model(X_val, S_val).cpu().numpy()
        y_pred_te = model(X_te, S_te).cpu().numpy()

    test_r2 = _r2(Y_t[test_idx].numpy(), y_pred_te)
    return {
        "train_r2": _r2(Y_t[train_idx].numpy(), y_pred_tr),
        "val_r2": _r2(Y_t[val_idx].numpy(), y_pred_val),
        "test_r2": test_r2,
        "r2": test_r2,       # alias kept for plot_bar_comparison compatibility
        "r2_std": 0.0,
        "model": model.cpu(),
    }


# ── Plotting helpers ───────────────────────────────────────────────────────────

def plot_bar_comparison(results_dict, title, save_path, baselines=None):
    """Bar chart comparing test R² across conditions.

    Parameters
    ----------
    results_dict : dict {label: {'r2': float, 'r2_std': float}}
    title        : str
    save_path    : Path
    baselines    : dict {label: r2} — horizontal reference lines
    """
    labels = list(results_dict.keys())
    r2s = [results_dict[k]["r2"] for k in labels]
    stds = [results_dict[k]["r2_std"] for k in labels]

    fig, ax = plt.subplots(figsize=(max(8, len(labels) * 1.4), 5))
    colors = plt.cm.tab10(np.linspace(0, 0.9, len(labels)))
    ax.bar(labels, r2s, yerr=stds, capsize=5, color=colors,
           alpha=0.85, edgecolor="black", linewidth=0.5)
    for i, (r2, std) in enumerate(zip(r2s, stds)):
        ax.text(i, r2 + std + 0.008, f"{r2:.3f}", ha="center", fontsize=9)

    if baselines:
        for blabel, br2 in baselines.items():
            ax.axhline(br2, linestyle="--", linewidth=0.9, alpha=0.7,
                       label=f"{blabel} = {br2:.3f}")
        ax.legend(fontsize=8)

    ax.set_ylabel("Test R²")
    ax.set_ylim(0, min(1.05, max(r2s) + max(stds) + 0.12))
    ax.set_title(title)
    plt.xticks(rotation=20, ha="right")
    fig.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {save_path}")


def compute_channel_saliency(model, grid_t, scalar_t, hour_mask=None, batch_size=256):
    """Compute per-channel mean |∂output/∂input| over selected hours.

    Parameters
    ----------
    model      : trained nn.Module
    grid_t     : torch.Tensor (T, C, H, W)
    scalar_t   : torch.Tensor (T, n_scalar)
    hour_mask  : ndarray bool (T,) or None — restrict to these hours
    batch_size : int — hours per backward pass

    Returns
    -------
    ndarray (C, H, W) — mean absolute gradient per channel and spatial cell
    """
    import torch

    model.eval().to("cpu")
    indices = np.where(hour_mask)[0] if hour_mask is not None else np.arange(len(grid_t))
    n = len(indices)
    if n == 0:
        C, H, W = grid_t.shape[1], grid_t.shape[2], grid_t.shape[3]
        return np.zeros((C, H, W))

    C, H, W = grid_t.shape[1], grid_t.shape[2], grid_t.shape[3]
    sal_accum = torch.zeros(C, H, W)

    for start in range(0, n, batch_size):
        batch_idx = indices[start: start + batch_size].tolist()
        xb = grid_t[batch_idx].clone().requires_grad_(True)
        sb = scalar_t[batch_idx]
        model(xb, sb).sum().backward()
        sal_accum += xb.grad.abs().sum(dim=0).detach()
        xb.grad = None

    return (sal_accum / n).numpy()


def plot_saliency_2x2(sal_per_channel, channel_names, pixel_coords,
                      lat_idx, lon_idx, title, save_path, dirs):
    """Plot 2×2 gradient saliency maps, one panel per error variable.

    Only the 4 error-field channels are plotted; spatial control channels are
    excluded. Infrastructure overlays (wind turbines, solar, cities) are drawn
    on each panel.

    Parameters
    ----------
    sal_per_channel  : ndarray (C, H, W)
    channel_names    : list of str — field name per channel (length C)
    pixel_coords     : ndarray (N_pixels, 2)
    lat_idx, lon_idx : ndarray (N_pixels,) — pixel-to-grid index mapping
    title            : str
    save_path        : Path
    dirs             : dict from setup_directories()
    """
    proj = ccrs.PlateCarree()

    error_panels = [
        (i, name) for i, name in enumerate(channel_names)
        if name in ERROR_FIELD_LABELS
    ]
    if not error_panels:
        print(f"  No error-field channels in {channel_names} — skipping saliency map")
        return

    vmax = max(
        np.nanpercentile(sal_per_channel[i][lat_idx, lon_idx], 99)
        for i, _ in error_panels
    )
    if vmax == 0:
        vmax = 1.0

    fig, axes = plt.subplots(
        2, 2, figsize=(18, 14),
        subplot_kw={"projection": proj},
        gridspec_kw={"hspace": 0.12, "wspace": 0.05},
    )

    sc_last = None
    legend_handles = []

    for panel_idx, (ch_idx, name) in enumerate(error_panels[:4]):
        row, col = divmod(panel_idx, 2)
        ax = axes[row, col]
        sal_pixels = sal_per_channel[ch_idx][lat_idx, lon_idx]

        _draw_prm_texas_base(ax, proj)
        sc = ax.scatter(
            pixel_coords[:, 1], pixel_coords[:, 0],
            c=sal_pixels, cmap="YlOrRd", s=12,
            transform=proj, vmin=0, vmax=vmax, zorder=3,
        )
        handles = _draw_overlays(ax, dirs, ["wind", "solar", "cities"], proj)
        _draw_texas_borders(ax)
        ax.set_title(ERROR_FIELD_LABELS.get(name, name), fontsize=11)
        sc_last = sc
        if panel_idx == 0:
            legend_handles = handles

    if sc_last is not None:
        fig.colorbar(sc_last, ax=axes.ravel().tolist(), shrink=0.6,
                     label="|∂output/∂input|", pad=0.02)

    if legend_handles:
        fig.legend(handles=legend_handles, loc="lower center",
                   ncol=len(legend_handles), fontsize=9, framealpha=0.85,
                   bbox_to_anchor=(0.45, 0.01))

    fig.suptitle(title, fontsize=13, y=0.98)
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {save_path}")


# ── Experiment implementations ─────────────────────────────────────────────────

def run_exp_channels(months, fig_dir, tables_dir, device, df=None):
    """Experiment C: Channel ablation over spatial field combinations.

    Tests seven channel configurations at 0.25° resolution using the chunk-based
    train/val/test split. Configurations range from single error channels up to
    the full spatial fields set (errors + ERA5 actuals + actual_load).

    Parameters
    ----------
    months     : list of (year, month)
    fig_dir    : Path
    tables_dir : Path
    device     : torch.device
    df         : pre-loaded DataFrame (optional)

    Returns
    -------
    dict {channel_label: {train_r2, val_r2, test_r2, r2, r2_std}}
    """
    print("\n" + "=" * 70)
    print("EXPERIMENT C: CHANNEL ABLATION (errors vs actuals vs combined)")
    print("=" * 70)

    if df is None:
        df = load_pixel_data_for_nn(months)

    grid_all, scalar_controls, Y, pc, hour_idx, loaded_fields = load_multi_field_data(
        months, spatial_fields=SPATIAL_FIELDS, df=df
    )
    T, C_all, H, W = grid_all.shape
    field_to_ch = {f: i for i, f in enumerate(loaded_fields)}

    train_idx, val_idx, test_idx = make_chunk_splits(hour_idx)
    print(f"  Split: train={len(train_idx)}, val={len(val_idx)}, test={len(test_idx)} hours")

    n_scalar = scalar_controls.shape[1]

    channel_configs = {
        "1-ch: wind-1h":        ["wspd_error_1h"],
        "1-ch: temp-1h":        ["temp_error_1h"],
        "2-ch: HRRR-1h":        ["wspd_error_1h", "temp_error_1h"],
        "2-ch: GFS-0h":         ["wspd_error_0h", "temp_error_0h"],
        "4-ch: errors only":    ERROR_FIELDS,
        "spatial controls":     SPATIAL_CONTROL_FIELDS,
        "all spatial fields":   SPATIAL_FIELDS,
    }

    results = {}
    for label, fields in channel_configs.items():
        ch_idx = [field_to_ch[f] for f in fields if f in field_to_ch]
        if not ch_idx:
            print(f"\n  [{label}] — no available fields, skipping")
            continue
        print(f"\n  [{label}]")
        grid_sub = grid_all[:, ch_idx, :, :]
        spatial_dim = len(ch_idx) * H * W
        res = train_with_splits(
            _make_mlp_cls(spatial_dim, n_scalar),
            grid_sub, scalar_controls, Y,
            train_idx, val_idx, test_idx, device,
        )
        results[label] = res
        print(f"    train R²={res['train_r2']:.4f}  val R²={res['val_r2']:.4f}"
              f"  test R²={res['test_r2']:.4f}")

    plot_bar_comparison(
        results,
        title="Channel Ablation — MLP at 0.25° (chunk-based 70/15/15 split)",
        save_path=fig_dir / "nn_channel_ablation.png",
    )
    rows = [
        {"channels": k, "train_r2": v["train_r2"], "val_r2": v["val_r2"],
         "test_r2": v["test_r2"]}
        for k, v in results.items()
    ]
    pd.DataFrame(rows).to_csv(tables_dir / "nn_channel_ablation.csv", index=False)
    return results


def run_exp_regime(months, fig_dir, tables_dir, device):
    """Experiment D: Regime-stratified evaluation and regime-specific training.

    Trains one pooled model on all hours, then evaluates its test-set R² within
    each weather regime. Also trains regime-specific models restricted to hours
    of that regime (with regime-filtered train/val/test sets).

    Parameters
    ----------
    months     : list of (year, month)
    fig_dir    : Path
    tables_dir : Path
    device     : torch.device

    Returns
    -------
    dict with pooled, regime_eval, and regime_specific results
    """
    print("\n" + "=" * 70)
    print("EXPERIMENT D: REGIME-STRATIFIED EVALUATION")
    print("=" * 70)

    import torch
    from process_data.classify_weather_regimes import classify_regimes

    df = load_pixel_data_for_nn(months)
    df = classify_regimes(df)

    grid_np, scalar_controls, Y_ref, pc_ref, hour_idx, _ = load_multi_field_data(
        months, df=df
    )
    T, C, H, W = grid_np.shape
    spatial_dim = C * H * W
    n_scalar = scalar_controls.shape[1]
    ModelClass = _make_mlp_cls(spatial_dim, n_scalar)
    print(f"  Grid: ({T}, {C}, {H}, {W})")

    train_idx, val_idx, test_idx = make_chunk_splits(hour_idx)
    print(f"  Split: train={len(train_idx)}, val={len(val_idx)}, test={len(test_idx)}")

    # ── Pooled model ─────────────────────────────────────────────────────────
    print("\n  Training pooled model...")
    pooled_res = train_with_splits(
        ModelClass, grid_np, scalar_controls, Y_ref,
        train_idx, val_idx, test_idx, device,
    )
    print(f"  Pooled — train R²={pooled_res['train_r2']:.4f}  "
          f"val R²={pooled_res['val_r2']:.4f}  test R²={pooled_res['test_r2']:.4f}")

    # ── Regime masks from per-hour regime columns ─────────────────────────────
    hour_df = (
        df.drop_duplicates("valid_time")[["valid_time", "is_extreme", "regime_temp"]]
        .sort_values("valid_time").reset_index(drop=True)
    )
    regime_temp = (
        hour_df.set_index("valid_time").reindex(hour_idx)["regime_temp"].values
    )
    cold_mask = regime_temp == "extreme_cold"
    heat_mask = regime_temp == "extreme_heat"
    normal_mask = ~(cold_mask | heat_mask)
    print(f"  Regime breakdown: cold={cold_mask.sum()}, "
          f"heat={heat_mask.sum()}, normal={normal_mask.sum()}")

    # ── Pooled model evaluated on regime subsets of the test set ─────────────
    pooled_model = pooled_res["model"]
    regime_eval = {}
    if pooled_model is not None:
        # Normalize Y using train statistics (same as in train_with_splits)
        Y_mean = float(Y_ref[train_idx].mean())
        Y_std = float(Y_ref[train_idx].std()) + 1e-8
        Y_norm = ((Y_ref - Y_mean) / Y_std).astype(np.float32)

        sc_mean = scalar_controls[train_idx].mean(axis=0)
        sc_std = scalar_controls[train_idx].std(axis=0) + 1e-8
        scalar_norm = ((scalar_controls - sc_mean) / sc_std).astype(np.float32)

        grid_t = torch.tensor(grid_np, dtype=torch.float32)
        scalar_t = torch.tensor(scalar_norm, dtype=torch.float32)
        pooled_model.eval()
        with torch.no_grad():
            y_pred_all = pooled_model(grid_t, scalar_t).numpy()

        for rname, mask in [("extreme_cold", cold_mask),
                             ("extreme_heat", heat_mask),
                             ("normal", normal_mask)]:
            # Evaluate only on test-set hours within this regime
            regime_test = test_idx[mask[test_idx]]
            if len(regime_test) < 20:
                print(f"  {rname}: too few test hours ({len(regime_test)}) — skipping")
                continue
            r2 = _r2(Y_norm[regime_test], y_pred_all[regime_test])
            regime_eval[rname] = {"r2": r2, "r2_std": 0.0, "n": int(len(regime_test))}
            print(f"  Pooled on {rname} (test set): R²={r2:.4f}  (n={len(regime_test)})")

    # ── Regime-specific models ────────────────────────────────────────────────
    regime_specific = {}
    for rname, mask in [("extreme_cold", cold_mask), ("extreme_heat", heat_mask)]:
        tr_r = train_idx[mask[train_idx]]
        va_r = val_idx[mask[val_idx]]
        te_r = test_idx[mask[test_idx]]
        if len(te_r) < MIN_REGIME_SAMPLES:
            print(f"\n  {rname}: too few test hours ({len(te_r)}) — skipping specific model")
            continue
        print(f"\n  Regime-specific model for {rname} "
              f"(train={len(tr_r)}, val={len(va_r)}, test={len(te_r)})...")
        res_r = train_with_splits(
            ModelClass, grid_np, scalar_controls, Y_ref,
            tr_r, va_r, te_r, device,
        )
        regime_specific[rname] = res_r
        print(f"  {rname}-specific — test R²={res_r['test_r2']:.4f}")

    plot_data = {"Pooled (all)": pooled_res}
    plot_data.update({f"Pooled→{k}": v for k, v in regime_eval.items()})
    plot_data.update({f"Specific:{k}": v for k, v in regime_specific.items()})

    plot_bar_comparison(
        plot_data,
        title="Regime-Stratified MLP — 0.25° grid (chunk-based 70/15/15 split)",
        save_path=fig_dir / "nn_regime_analysis.png",
    )
    rows = [{"model": k, "train_r2": v.get("train_r2", float("nan")),
             "val_r2": v.get("val_r2", float("nan")), "test_r2": v["r2"]}
            for k, v in plot_data.items()]
    pd.DataFrame(rows).to_csv(tables_dir / "nn_regime_analysis.csv", index=False)

    return {"pooled": pooled_res, "regime_eval": regime_eval,
            "regime_specific": regime_specific}


def run_exp_saliency(months, fig_dir, tables_dir, device):
    """Experiment E: Per-channel gradient saliency maps, full and regime-stratified.

    Trains one MLP on the training split, then computes mean |∂output/∂input|
    per channel over all hours and over each weather regime subset. Produces a
    2×2 figure for all hours and one per regime. Infrastructure overlays are
    drawn on every panel.

    Parameters
    ----------
    months     : list of (year, month)
    fig_dir    : Path
    tables_dir : Path
    device     : torch.device

    Returns
    -------
    dict with saliency arrays keyed by condition ('all', regime names)
    """
    import torch
    from process_data.classify_weather_regimes import classify_regimes

    print("\n" + "=" * 70)
    print("EXPERIMENT E: GRADIENT SALIENCY MAPS (per-channel 2×2 + regimes)")
    print("=" * 70)

    dirs = setup_directories()

    df = load_pixel_data_for_nn(months)
    df = classify_regimes(df)

    grid_np, scalar_controls, Y_ref, pc_ref, hour_idx, loaded_fields = load_multi_field_data(
        months, df=df
    )
    T, C, H, W = grid_np.shape
    channel_names = loaded_fields
    lat_idx, lon_idx, _, _ = _coarse_grid_indices(pc_ref, resolution=0.25)
    print(f"  Grid: ({T}, {C}, {H}, {W}), channels: {channel_names}")

    train_idx, val_idx, test_idx = make_chunk_splits(hour_idx)
    print(f"  Split: train={len(train_idx)}, val={len(val_idx)}, test={len(test_idx)}")

    # Train on training split, early-stop on validation
    print("\n  Training MLP for saliency...")
    spatial_dim = C * H * W
    n_scalar = scalar_controls.shape[1]
    res = train_with_splits(
        _make_mlp_cls(spatial_dim, n_scalar),
        grid_np, scalar_controls, Y_ref,
        train_idx, val_idx, test_idx, device,
    )
    model = res["model"]
    print(f"  Train R²={res['train_r2']:.4f}  val R²={res['val_r2']:.4f}"
          f"  test R²={res['test_r2']:.4f}")

    # Normalize scalars using train statistics for saliency computation
    sc_mean = scalar_controls[train_idx].mean(axis=0)
    sc_std = scalar_controls[train_idx].std(axis=0) + 1e-8
    scalar_norm = ((scalar_controls - sc_mean) / sc_std).astype(np.float32)

    grid_t = torch.tensor(grid_np, dtype=torch.float32)
    scalar_t = torch.tensor(scalar_norm, dtype=torch.float32)

    results = {}

    # ── All-hours saliency ────────────────────────────────────────────────────
    print("\n  Computing saliency for all hours...")
    sal_all = compute_channel_saliency(model, grid_t, scalar_t)
    plot_saliency_2x2(
        sal_all, channel_names, pc_ref, lat_idx, lon_idx,
        title=(
            "MLP Gradient Saliency — All Hours\n"
            "(mean |∂output/∂input| per error variable; "
            "wind turbines, solar, cities overlaid)"
        ),
        save_path=fig_dir / "nn_saliency_2x2_all.png",
        dirs=dirs,
    )
    results["all"] = sal_all

    # ── Regime-stratified saliency ────────────────────────────────────────────
    needed_cols = {s["filter_col"] for s in REGIMES.values()}
    avail_cols = [c for c in needed_cols if c in df.columns]
    hour_regime = df.drop_duplicates("valid_time").set_index("valid_time")[avail_cols]
    regime_aligned = hour_regime.reindex(hour_idx) if len(hour_idx) > 0 else pd.DataFrame()

    for regime_name, spec in REGIMES.items():
        col = spec["filter_col"]
        val = spec["filter_val"]
        if regime_aligned.empty or col not in regime_aligned.columns:
            print(f"  Skipping regime '{regime_name}' — column '{col}' not available")
            continue

        mask = (regime_aligned[col] == val).fillna(False).values
        n_hrs = int(mask.sum())
        print(f"\n  Regime '{regime_name}': {n_hrs} hours")
        if n_hrs < 20:
            print("    Too few hours — skipping")
            continue

        sal_regime = compute_channel_saliency(model, grid_t, scalar_t, hour_mask=mask)
        plot_saliency_2x2(
            sal_regime, channel_names, pc_ref, lat_idx, lon_idx,
            title=(
                f"MLP Gradient Saliency — {spec['label']}\n"
                "(mean |∂output/∂input| per error variable; "
                "wind turbines, solar, cities overlaid)"
            ),
            save_path=fig_dir / f"nn_saliency_2x2_{regime_name}.png",
            dirs=dirs,
        )
        results[regime_name] = sal_regime

    # Save per-pixel summary (mean over error channels)
    error_ch_idx = [i for i, n in enumerate(channel_names) if n in ERROR_FIELD_LABELS]
    if error_ch_idx:
        sal_mean = sal_all[error_ch_idx].mean(axis=0)[lat_idx, lon_idx]
        rows = [
            {"lat": pc_ref[i, 0], "lon": pc_ref[i, 1], "saliency": float(sal_mean[i])}
            for i in range(len(pc_ref))
        ]
        pd.DataFrame(rows).to_csv(tables_dir / "nn_saliency_pixels.csv", index=False)

    return results


def run_exp_overfitting_checks(months, fig_dir, tables_dir, device):
    """Experiment F: Temporal leakage and overfitting diagnostics.

    Runs four checks on the 4-channel error-only MLP at 0.25°:
      1. Lag-1/lag-24 autocorrelation of the target.
      2. Lag-1 persistence R² — trivially-achievable baseline.
      3. Shuffled-Y sanity check — permuted target should collapse to ~0.
      4. Leakage comparison: naive shuffled-row 80/20 split (leaky) vs
         chunk-based 70/15/15 split (clean). Quantifies the leakage gap.

    Parameters
    ----------
    months     : list of (year, month)
    fig_dir    : Path
    tables_dir : Path
    device     : torch.device

    Returns
    -------
    dict keyed by check name
    """
    import torch
    import torch.nn.functional as F

    print("\n" + "=" * 70)
    print("EXPERIMENT F: OVERFITTING / TEMPORAL LEAKAGE DIAGNOSTICS")
    print("=" * 70)

    df = load_pixel_data_for_nn(months)
    grid_np, scalar_controls, Y_ref, pc_ref, hour_idx, _ = load_multi_field_data(
        months, spatial_fields=ERROR_FIELDS, df=df
    )
    T, C, H, W = grid_np.shape
    spatial_dim = C * H * W
    n_scalar = scalar_controls.shape[1]
    ModelClass = _make_mlp_cls(spatial_dim, n_scalar)
    print(f"  Grid: ({T}, {C}, {H}, {W})")

    results = {}

    # ── 1. Autocorrelation ────────────────────────────────────────────────────
    s = pd.Series(Y_ref)
    ac1 = float(s.autocorr(lag=1))
    ac24 = float(s.autocorr(lag=24))
    print(f"\n  [1] Target autocorrelation:")
    print(f"      lag-1  : {ac1:.4f}")
    print(f"      lag-24 : {ac24:.4f}")
    results["autocorr_lag1"] = ac1
    results["autocorr_lag24"] = ac24

    # ── 2. Persistence baseline ───────────────────────────────────────────────
    Y_lag = np.roll(Y_ref, 1)
    Y_lag[0] = Y_ref.mean()
    persistence_r2 = _r2(Y_ref[1:], Y_lag[1:])
    print(f"\n  [2] Lag-1 persistence R²: {persistence_r2:.4f}")
    results["persistence_r2"] = persistence_r2

    # ── 3. Shuffled-Y sanity check ────────────────────────────────────────────
    print("\n  [3] Shuffled-Y sanity check (chunk split, permuted target)...")
    chunk_tr, chunk_val, chunk_te = make_chunk_splits(hour_idx)
    rng = np.random.default_rng(RANDOM_STATE)
    Y_shuffled = rng.permutation(Y_ref)
    null_res = train_with_splits(
        ModelClass, grid_np, scalar_controls, Y_shuffled,
        chunk_tr, chunk_val, chunk_te, device,
    )
    print(f"      Null test R²={null_res['test_r2']:.4f}")
    results["null_r2"] = null_res["test_r2"]

    # ── 4. Leakage comparison ─────────────────────────────────────────────────
    print("\n  [4] Leakage comparison: naive shuffled-row vs chunk-based split...")

    # Naive: shuffle all rows randomly → leaky because adjacent hours end up in
    # both train and test, and autocorrelation does most of the "prediction"
    rng2 = np.random.default_rng(RANDOM_STATE)
    shuffled_rows = rng2.permutation(T)
    naive_tr = shuffled_rows[:int(0.8 * T)]
    naive_te = shuffled_rows[int(0.8 * T):]
    naive_val = naive_tr[:int(0.1 * len(naive_tr))]
    naive_tr = naive_tr[int(0.1 * len(naive_tr)):]

    naive_res = train_with_splits(
        ModelClass, grid_np, scalar_controls, Y_ref,
        naive_tr, naive_val, naive_te, device,
    )
    print(f"      Naive shuffled-row test R²={naive_res['test_r2']:.4f}  (leaky)")

    # Clean chunk-based split
    clean_res = train_with_splits(
        ModelClass, grid_np, scalar_controls, Y_ref,
        chunk_tr, chunk_val, chunk_te, device,
    )
    print(f"      Chunk-based test R²={clean_res['test_r2']:.4f}  (clean)")
    leakage_gap = naive_res["test_r2"] - clean_res["test_r2"]
    print(f"      Leakage gap: {leakage_gap:.4f}")

    results.update({
        "naive_split_r2": naive_res["test_r2"],
        "chunk_split_r2": clean_res["test_r2"],
        "leakage_gap": leakage_gap,
    })

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "-" * 60)
    print("  SUMMARY — 4-channel error MLP at 0.25°:")
    print(f"    Lag-1 autocorr (Y):             {results['autocorr_lag1']:.4f}")
    print(f"    Lag-24 autocorr (Y):            {results['autocorr_lag24']:.4f}")
    print(f"    Persistence R²:                 {results['persistence_r2']:.4f}")
    print(f"    Null (shuffled Y) R²:           {results['null_r2']:.4f}")
    print(f"    Naive shuffled-row R²:          {results['naive_split_r2']:.4f}")
    print(f"    Chunk-based test R²:            {results['chunk_split_r2']:.4f}")
    print(f"    Leakage gap:                    {results['leakage_gap']:.4f}")
    print("-" * 60)

    pd.DataFrame([{"metric": k, "value": v}
                  for k, v in results.items()]).to_csv(
        tables_dir / "nn_overfitting_checks.csv", index=False
    )

    comparison = {
        "Persistence\n(lag-1)":    {"r2": results["persistence_r2"], "r2_std": 0.0},
        "Null\n(shuffled Y)":      {"r2": results["null_r2"],         "r2_std": 0.0},
        "Naive split\n(leaky)":    {"r2": results["naive_split_r2"],  "r2_std": 0.0},
        "Chunk split\n(clean)":    {"r2": results["chunk_split_r2"],  "r2_std": 0.0},
    }
    plot_bar_comparison(
        {k: v for k, v in comparison.items() if not np.isnan(v["r2"])},
        title=(
            "Overfitting Diagnostics — 4-ch Error MLP at 0.25°\n"
            "Naive shuffled-row vs chunk-based 70/15/15 split"
        ),
        save_path=fig_dir / "nn_overfitting_checks.png",
    )

    return results


# ── Main orchestrator ──────────────────────────────────────────────────────────

def run_all(months=None, experiments=None):
    """Run selected experiments and save all results.

    Pre-loads pixel data once and shares it across channel and saliency
    experiments to avoid redundant parquet reads.

    Parameters
    ----------
    months      : list of (year, month); defaults to DEFAULT_MONTHS
    experiments : list of experiment names or None (runs all)
    """
    if months is None:
        months = DEFAULT_MONTHS
    if experiments is None:
        experiments = ["channels", "regime", "saliency", "overfitting"]

    try:
        import torch  # noqa: F401
    except ImportError:
        raise RuntimeError("torch is required. Install with: uv add torch")

    device = _select_device()
    print(f"  Device: {device}")

    dirs = setup_directories()
    fig_dir = Path(dirs["figures"]) / "functional_analysis"
    tables_dir = Path(dirs["tables"])
    fig_dir.mkdir(parents=True, exist_ok=True)
    tables_dir.mkdir(parents=True, exist_ok=True)

    df_shared = None
    if "channels" in experiments or "saliency" in experiments:
        print("\n  Pre-loading pixel data...")
        df_shared = load_pixel_data_for_nn(months)

    all_results = {}

    if "channels" in experiments:
        all_results["channels"] = run_exp_channels(
            months, fig_dir, tables_dir, device, df=df_shared
        )
    if "regime" in experiments:
        all_results["regime"] = run_exp_regime(months, fig_dir, tables_dir, device)
    if "saliency" in experiments:
        all_results["saliency"] = run_exp_saliency(
            months, fig_dir, tables_dir, device
        )
    if "overfitting" in experiments:
        all_results["overfitting"] = run_exp_overfitting_checks(
            months, fig_dir, tables_dir, device
        )

    print("\n" + "=" * 70)
    print("ANALYSIS COMPLETE")
    print("=" * 70)
    print("\nSummary:")
    for exp, res in all_results.items():
        if isinstance(res, dict):
            for k, v in res.items():
                if isinstance(v, dict) and "r2" in v:
                    print(f"  {exp}/{k}: test R²={v['r2']:.4f}")

    return all_results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="MLP analysis for ERCOT curtailment prediction"
    )
    parser.add_argument(
        "--exp",
        nargs="+",
        default=["all"],
        choices=["all", "channels", "regime", "saliency", "overfitting"],
        help="Which experiment(s) to run (space-separated)",
    )
    parser.add_argument(
        "--months",
        nargs="+",
        type=int,
        default=[1, 3, 5, 7, 9, 11],
        help="Month numbers within 2025 (e.g. 1 3 5 7 9 11)",
    )
    args = parser.parse_args()

    months = [(2025, m) for m in args.months]
    exps = None if "all" in args.exp else args.exp
    run_all(months=months, experiments=exps)
