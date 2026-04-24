"""
MLP/CNN/FNO analysis for ERCOT congestion cost prediction from spatial forecast error fields.

Predicts economic_congestion_cost using spatial weather forecast error grids
(HRRR 1h and GFS day-ahead wind/temp errors) plus scalar temporal controls.

Data is divided into 5-day temporal blocks, shuffled, and split 70/15/15
across train/val/test to prevent leakage from high hourly autocorrelation.
Spatial fields are kept as (T, C, H, W) tensors — compatible with MLP
(flattened), CNN (conv + global pool), and FNO (spectral conv + global pool).
All architectures share the same (x_spatial, x_scalar) forward interface and
training loop so results are directly comparable.

Experiments:
  arch        — MLP vs CNN vs FNO architecture comparison (run first)
  channels    — ablation over which spatial channels to include
  regime      — regime-stratified evaluation and regime-specific training
  saliency    — per-channel gradient saliency maps
  overfitting — diagnostics comparing leaky vs clean splits
  baseline    — Step 0: log1p transform and cyclic scalar controls
  infra       — Step 1: infrastructure capacity channels
  cluster     — Step 2: cluster-level aggregated error features
  nodal       — Step 3: two-stage nodal LMP auxiliary supervision
  ar          — Round 2: leakage fix + autoregressive lag features
  gbm         — Round 2: LightGBM gradient boosting on cluster+AR features
  gru         — Round 2: GRU recurrent model with 24h sliding window

Usage:
    uv run python -m analysis.nn_analysis --exp all
    uv run python -m analysis.nn_analysis --exp arch
    uv run python -m analysis.nn_analysis --exp channels
    uv run python -m analysis.nn_analysis --exp regime
    uv run python -m analysis.nn_analysis --exp saliency
    uv run python -m analysis.nn_analysis --exp overfitting
    uv run python -m analysis.nn_analysis --exp baseline
    uv run python -m analysis.nn_analysis --exp infra
    uv run python -m analysis.nn_analysis --exp cluster
    uv run python -m analysis.nn_analysis --exp nodal
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

# Infrastructure channels available in pixel_hourly_*.parquet. Used when
# include_infra=True in load_multi_field_data. Values are time-invariant per
# pixel, so they are broadcast across T.
INFRA_FIELDS = [
    "total_capacity_mw",
    "nameplate_mw_tech_onshore_wind_turbine",
    "nameplate_mw_tech_solar_photovoltaic",
    "nameplate_mw_tech_natural_gas_fired_combined_cycle",
    "nameplate_mw_tech_natural_gas_fired_combustion_turbine",
    "nameplate_mw_tech_conventional_steam_coal",
    "nameplate_mw_tech_nuclear",
    "has_transmission_line",
    "load_center",
]


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

    keep_cols = list(dict.fromkeys(
        ["pixel_id", "valid_time", "latitude", "longitude", PFA_DEPVAR, DEPVAR]
        + ERROR_FIELDS
        + REGIME_EXTRA_COLS
        + ["actual_load"]
        + ["era5_temp", "era5_wspd"]
        + INFRA_FIELDS
    ))

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

    n_val = max(1, int(np.floor(n_chunks * val_frac))) if n_chunks >= 3 else 0
    n_train = max(1, min(n_chunks - n_val - 1, int(np.floor(n_chunks * train_frac))))
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


def load_multi_field_data(months, spatial_fields=None, df=None,
                          scalar_mode="raw", include_infra=False):
    """Load pixel-hourly data and build spatial grid tensor plus scalar controls.

    Spatial fields are kept as (T, C, H, W) — suitable for MLP (flattened),
    CNN, or FNO. Scalar temporal controls are derived from timestamps and
    returned separately so architectures can handle them as needed.

    Fields absent from the dataframe are silently skipped; `loaded_fields`
    reflects what was actually loaded, so callers must use it (not SPATIAL_FIELDS)
    when mapping channels to names.

    When include_infra=True, static infrastructure channels are appended after
    the time-varying channels and broadcast across T. The returned grid_spatial
    will have C_error + n_infra channels. loaded_fields will include infra names.

    Parameters
    ----------
    months         : list of (year, month) tuples
    spatial_fields : list of field names for spatial channels;
                     defaults to SPATIAL_FIELDS
    df             : pre-loaded DataFrame (optional); must contain all fields
    scalar_mode    : 'raw' (default) or 'cyclic'. 'raw' returns
                     [hour_of_day, is_weekend, month] (3 cols). 'cyclic'
                     returns [sin(2π·h/24), cos(2π·h/24), sin(2π·m/12),
                     cos(2π·m/12), is_weekend] (5 cols).
    include_infra  : bool — whether to concatenate static INFRA_FIELDS channels

    Returns
    -------
    grid_spatial    : ndarray (T, C, H, W) float32
    scalar_controls : ndarray (T, 3 or 5) float32
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

    X_dict, _, _, pixel_ids_ref, hour_idx_ref = prepare_multi_field_data(
        df, error_fields=available
    )

    # Compute Y from the local DEPVAR (economic_congestion_cost) rather than
    # PFA_DEPVAR (total_curtailment_mw) used internally by prepare_multi_field_data.
    hourly_y = df.groupby("valid_time")[DEPVAR].first()
    Y_ref = hourly_y.reindex(hour_idx_ref).values.astype(np.float32)

    # Drop hours with NaN in the target
    valid_t = ~np.isnan(Y_ref)
    if not valid_t.all():
        print(f"  Dropping {(~valid_t).sum()} hours with NaN {DEPVAR}")
        hour_idx_ref = hour_idx_ref[valid_t]
        Y_ref = Y_ref[valid_t]
        X_dict = {k: v[valid_t] for k, v in X_dict.items()}

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

    scalar_controls = build_scalar_controls(hour_idx_ref, cyclic=(scalar_mode == "cyclic"))

    # Optionally append static infrastructure channels
    if include_infra:
        infra_grid, infra_names = build_infra_channels(
            df, pixel_coords_ref, resolution=0.25
        )
        if infra_grid is not None and infra_grid.shape[0] > 0:
            T = grid_spatial.shape[0]
            # Broadcast (n_infra, H, W) → (T, n_infra, H, W)
            infra_broadcast = np.broadcast_to(
                infra_grid[np.newaxis], (T,) + infra_grid.shape
            ).copy().astype(np.float32)
            grid_spatial = np.concatenate([grid_spatial, infra_broadcast], axis=1)
            loaded_fields = loaded_fields + infra_names
            print(f"  Appended {len(infra_names)} infra channels: {infra_names}")

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


def _make_cnn_cls(C, n_scalar, hidden=64):
    """Return a CNN class with the same (x_spatial, x_scalar) forward interface as MLP.

    Three conv layers with global average pooling extract spatial features,
    which are concatenated with scalar controls before the MLP head.

    Parameters
    ----------
    C        : int — input channels (C in grid tensor)
    n_scalar : int — number of scalar temporal controls
    hidden   : int — conv layer channel width

    Returns
    -------
    class — instantiates to nn.Module
    """
    import torch
    import torch.nn as nn

    class CNNScalar(nn.Module):
        def __init__(self):
            super().__init__()
            self.cnn = nn.Sequential(
                nn.Conv2d(C, hidden, 3, padding=1), nn.GELU(),
                nn.Conv2d(hidden, hidden, 3, padding=1), nn.GELU(),
                nn.Conv2d(hidden, hidden // 2, 3, padding=1), nn.GELU(),
            )
            self.head = nn.Sequential(
                nn.Linear(hidden // 2 + n_scalar, 128),
                nn.LayerNorm(128), nn.GELU(), nn.Dropout(0.2),
                nn.Linear(128, 64), nn.GELU(),
                nn.Linear(64, 1),
            )

        def forward(self, x_spatial, x_scalar):
            """Forward: conv over spatial grid, pool, concat scalars, MLP head.

            Parameters
            ----------
            x_spatial : Tensor (B, C, H, W)
            x_scalar  : Tensor (B, n_scalar)

            Returns
            -------
            Tensor (B,)
            """
            feat = self.cnn(x_spatial).mean(dim=(-2, -1))
            return self.head(torch.cat([feat, x_scalar], dim=1)).squeeze(-1)

    return CNNScalar


def _make_fno_cls(C, H, W, n_scalar, modes=8, hidden=32, n_layers=2):
    """Return an FNO class with the same (x_spatial, x_scalar) forward interface.

    Uses neuralop.models.FNO with domain_padding for non-periodic ERCOT domain.
    FNO must run on CPU (rfft2 unsupported on MPS); callers should pass
    device=torch.device('cpu') to train_with_splits for this architecture.

    Parameters
    ----------
    C, H, W  : int — spatial grid dimensions
    n_scalar : int — number of scalar temporal controls
    modes    : int — Fourier modes per dimension (auto-clamped to Nyquist)
    hidden   : int — FNO channel width
    n_layers : int — number of spectral conv layers

    Returns
    -------
    class — instantiates to nn.Module, or None if neuralop not installed
    """
    try:
        from neuralop.models import FNO
    except ImportError:
        return None

    import torch
    import torch.nn as nn

    modes_h = min(modes, H // 2)
    modes_w = min(modes, W // 2)

    class FNOScalar(nn.Module):
        def __init__(self):
            super().__init__()
            self.fno = FNO(
                n_modes=(modes_h, modes_w),
                in_channels=C,
                out_channels=hidden,
                hidden_channels=hidden,
                n_layers=n_layers,
                domain_padding=0.1,
                norm="instance_norm",
                use_channel_mlp=True,
                channel_mlp_expansion=0.5,
                fno_skip="linear",
                channel_mlp_skip="soft-gating",
            )
            self.head = nn.Sequential(
                nn.Linear(hidden + n_scalar, 64), nn.GELU(), nn.Linear(64, 1)
            )

        def forward(self, x_spatial, x_scalar):
            """Forward: FNO over spatial grid, pool, concat scalars, MLP head.

            Parameters
            ----------
            x_spatial : Tensor (B, C, H, W) — must be on CPU
            x_scalar  : Tensor (B, n_scalar)

            Returns
            -------
            Tensor (B,)
            """
            feat = self.fno(x_spatial).mean(dim=(-2, -1))
            return self.head(torch.cat([feat, x_scalar], dim=1)).squeeze(-1)

    return FNOScalar


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
                      batch_size=BATCH_SIZE, lr=LR, weight_decay=WEIGHT_DECAY,
                      y_native=None, y_inverse=None):
    """Train on train split, early-stop on val, evaluate on test.

    Y and scalar controls are normalized using train-set statistics only
    to prevent any information leakage through normalization.

    When y_native and y_inverse are both provided, also computes native-scale
    metrics by inverting the transform: native_pred = y_inverse(pred * Y_std + Y_mean).
    Reports native_test_r2, native_test_rmse, and tail_test_r2 (R² on top-10%
    of test hours by true native cost).

    Parameters
    ----------
    ModelClass                   : callable → nn.Module with forward(x_spatial, x_scalar)
    grid_np                      : ndarray (T, C, H, W)
    scalar_np                    : ndarray (T, n_scalar) — un-normalized
    Y_np                         : ndarray (T,) — un-normalized target (possibly transformed)
    train_idx, val_idx, test_idx : ndarray of int
    device                       : torch.device
    n_epochs, patience, batch_size, lr, weight_decay : training hyperparams
    y_native                     : ndarray (T,) or None — original native-scale target values
    y_inverse                    : callable or None — invert transform (e.g. np.expm1)

    Returns
    -------
    dict with train_r2, val_r2, test_r2, model (on CPU, best checkpoint).
    If y_native and y_inverse provided, also includes native_test_r2,
    native_test_rmse, tail_test_r2.
    """
    import io
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
            # BytesIO roundtrip avoids neuralop's non-standard state_dict entries
            buf = io.BytesIO()
            torch.save(model.state_dict(), buf)
            best_state = buf.getvalue()
            patience_count = 0
        else:
            patience_count += 1
            if patience_count >= patience:
                break

    if best_state is not None:
        # strict=False: neuralop FNO stores a non-parameter "_metadata" key
        model.load_state_dict(
            torch.load(io.BytesIO(best_state), weights_only=False), strict=False
        )

    model.eval()
    with torch.no_grad():
        y_pred_tr = model(X_tr, S_tr).cpu().numpy()
        y_pred_val = model(X_val, S_val).cpu().numpy()
        y_pred_te = model(X_te, S_te).cpu().numpy()

    test_r2 = _r2(Y_t[test_idx].numpy(), y_pred_te)
    result = {
        "train_r2": _r2(Y_t[train_idx].numpy(), y_pred_tr),
        "val_r2": _r2(Y_t[val_idx].numpy(), y_pred_val),
        "test_r2": test_r2,
        "r2": test_r2,       # alias kept for plot_bar_comparison compatibility
        "r2_std": 0.0,
        "model": model.cpu(),
    }

    # Native-scale metrics when an inverse transform is supplied
    if y_native is not None and y_inverse is not None:
        # Recover transformed predictions → native scale
        y_pred_te_tfm = y_pred_te * Y_std + Y_mean
        y_pred_te_native = y_inverse(y_pred_te_tfm)
        y_true_te_native = y_native[test_idx]

        native_r2 = _r2(y_true_te_native, y_pred_te_native)
        native_rmse = float(np.sqrt(np.mean((y_true_te_native - y_pred_te_native) ** 2)))

        # Tail R²: top-10% of test hours by true native cost
        tail_thresh = np.percentile(y_true_te_native, 90)
        tail_mask = y_true_te_native >= tail_thresh
        if tail_mask.sum() >= 5:
            tail_r2 = _r2(y_true_te_native[tail_mask], y_pred_te_native[tail_mask])
        else:
            tail_r2 = float("nan")

        result.update({
            "native_test_r2": native_r2,
            "native_test_rmse": native_rmse,
            "tail_test_r2": tail_r2,
            "r2": native_r2,   # override bar-chart alias with native R²
        })

    return result


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
    # Clip displayed R² to [-1, 1] to avoid axis-size explosions from extreme negatives
    r2s_clipped = [max(-1.0, r) for r in r2s]
    ax.bar(labels, r2s_clipped, yerr=stds, capsize=5, color=colors,
           alpha=0.85, edgecolor="black", linewidth=0.5)
    for i, (r2, std, r2c) in enumerate(zip(r2s, stds, r2s_clipped)):
        y_text = min(max(r2c + std + 0.02, -0.95), 0.98)
        ax.text(i, y_text, f"{r2:.3f}", ha="center", fontsize=9)

    if baselines:
        for blabel, br2 in baselines.items():
            ax.axhline(br2, linestyle="--", linewidth=0.9, alpha=0.7,
                       label=f"{blabel} = {br2:.3f}")
        ax.legend(fontsize=8)

    ax.set_ylabel("Test R²")
    y_min = min(-0.05, min(r2s_clipped) - 0.05)
    y_max = min(1.05, max(r2s_clipped) + max(stds) + 0.15)
    ax.set_ylim(y_min, max(y_max, y_min + 0.1))
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


# ── New helper functions (Steps 0–3) ──────────────────────────────────────────

def build_scalar_controls(hour_idx, cyclic=False):
    """Build scalar temporal control features from a DatetimeIndex.

    Parameters
    ----------
    hour_idx : pd.DatetimeIndex — timestamps (T,)
    cyclic   : bool — if True, encode hour and month as sin/cos pairs.
               Returns shape (T, 5): [sin_h, cos_h, sin_m, cos_m, is_weekend].
               If False, returns shape (T, 3): [hour_of_day, is_weekend, month].

    Returns
    -------
    ndarray (T, 3) or (T, 5) float32
    """
    if cyclic:
        h = hour_idx.hour.values.astype(np.float32)
        m = hour_idx.month.values.astype(np.float32)
        return np.column_stack([
            np.sin(2 * np.pi * h / 24).astype(np.float32),
            np.cos(2 * np.pi * h / 24).astype(np.float32),
            np.sin(2 * np.pi * (m - 1) / 12).astype(np.float32),
            np.cos(2 * np.pi * (m - 1) / 12).astype(np.float32),
            (hour_idx.dayofweek >= 5).astype(np.float32),
        ])
    return np.column_stack([
        hour_idx.hour.values.astype(np.float32),
        (hour_idx.dayofweek >= 5).astype(np.float32),
        hour_idx.month.values.astype(np.float32),
    ])


def y_transform_log1p():
    """Return forward and inverse functions for log1p target transform.

    The forward transform stabilizes the heavy-tailed economic_congestion_cost
    distribution. The inverse (expm1) is used to recover native-scale predictions
    for reporting.

    Returns
    -------
    tuple (forward, inverse) — both are callables ndarray → ndarray
    """
    return np.log1p, np.expm1


def build_infra_channels(df, pixel_coords_ref, resolution=0.25):
    """Build static infrastructure spatial channels from pixel dataframe.

    Reads INFRA_FIELDS from df, deduplicates by pixel_id, applies log1p to
    capacity columns (MW), keeps binary columns as-is, and regrids to the
    coarse H×W grid used by the spatial error channels.

    Parameters
    ----------
    df               : pd.DataFrame — pixel-hourly data; must have pixel_id,
                       latitude, longitude, and any subset of INFRA_FIELDS
    pixel_coords_ref : ndarray (N_pixels, 2) — (lat, lon) reference grid used
                       by the error channels (to align infra onto same grid)
    resolution       : float — coarse grid spacing in degrees (default 0.25)

    Returns
    -------
    infra_grid : ndarray (n_infra, H, W) float32 or None if no infra columns found
    infra_names : list of str — channel names corresponding to axis 0
    """
    binary_cols = {"has_transmission_line", "load_center"}
    available = [f for f in INFRA_FIELDS if f in df.columns]
    if not available:
        return None, []

    # One row per pixel_id. Drop NaN coords FIRST — the first row of a pixel
    # sometimes has NaN lat/lon (missing forecast), so drop_duplicates before
    # dropna would lose most pixels.
    pixel_static = (
        df[["pixel_id", "latitude", "longitude"] + available]
        .dropna(subset=["latitude", "longitude"])
        .drop_duplicates("pixel_id")
        .reset_index(drop=True)
    )
    if len(pixel_static) == 0:
        return None, []

    pc = pixel_static[["latitude", "longitude"]].values
    lat_idx, lon_idx, H, W = _coarse_grid_indices(pc, resolution)

    # Ensure grid covers pixel_coords_ref extent
    _, _, H_ref, W_ref = _coarse_grid_indices(pixel_coords_ref, resolution)
    H = max(H, H_ref)
    W = max(W, W_ref)

    n_infra = len(available)
    infra_grid = np.zeros((n_infra, H, W), dtype=np.float32)

    for ch, col in enumerate(available):
        vals = pixel_static[col].fillna(0).values.astype(np.float32)
        if col not in binary_cols:
            vals = np.log1p(vals)
        infra_grid[ch, lat_idx, lon_idx] = vals

    return infra_grid, available


def load_cluster_error_features(months, hour_idx):
    """Load cluster-level aggregated error features including lmp_std.

    Thin wrapper around load_cluster_features_v2 that preserves the original
    behaviour (includes lmp_std). Use load_cluster_features_v2 directly when
    you need to control which fields are included.

    Parameters
    ----------
    months   : list of (year, month) tuples
    hour_idx : pd.DatetimeIndex (T,) — hours to align features to

    Returns
    -------
    ndarray (T, n_features) float32 — cluster features aligned to hour_idx
    """
    arr, _ = load_cluster_features_v2(months, hour_idx, include_lmp_std=True)
    return arr


# ── Experiment implementations ─────────────────────────────────────────────────

def run_exp_arch(months, fig_dir, tables_dir, device, df=None):
    """Experiment A: Architecture comparison — MLP vs CNN vs FNO.

    All three use the full SPATIAL_FIELDS (errors + ERA5 actuals + actual_load)
    at 0.25° and the chunk-based 70/15/15 split. FNO runs on CPU (rfft2
    unsupported on MPS) with N_EPOCHS // 3 epochs — a wall-time constraint,
    not a capacity limit, so its R² is a lower bound vs MLP/CNN.

    Parameters
    ----------
    months     : list of (year, month)
    fig_dir    : Path
    tables_dir : Path
    device     : torch.device — for MLP and CNN; FNO always uses CPU
    df         : pre-loaded DataFrame (optional)

    Returns
    -------
    dict {arch_name: {train_r2, val_r2, test_r2, r2, r2_std}}
    """
    import torch

    print("\n" + "=" * 70)
    print("EXPERIMENT A: ARCHITECTURE COMPARISON (MLP vs CNN vs FNO, 0.25°)")
    print("=" * 70)

    if df is None:
        df = load_pixel_data_for_nn(months)

    grid_np, scalar_controls, Y, pc, hour_idx, loaded_fields = load_multi_field_data(
        months, spatial_fields=SPATIAL_FIELDS, df=df
    )
    T, C, H, W = grid_np.shape
    n_scalar = scalar_controls.shape[1]
    print(f"  Grid: ({T}, {C}, {H}, {W}), channels: {loaded_fields}")

    train_idx, val_idx, test_idx = make_chunk_splits(hour_idx)
    print(f"  Split: train={len(train_idx)}, val={len(val_idx)}, test={len(test_idx)}")

    cpu_device = torch.device("cpu")
    fno_epochs = max(20, N_EPOCHS // 3)

    FNOClass = _make_fno_cls(C, H, W, n_scalar)
    arch_configs = [
        ("MLP", _make_mlp_cls(C * H * W, n_scalar), device, N_EPOCHS),
        ("CNN", _make_cnn_cls(C, n_scalar), device, N_EPOCHS),
    ]
    if FNOClass is None:
        print("  [FNO] — skipped (neuralop not installed; run: uv add neuralop)")
    else:
        arch_configs.append(("FNO", FNOClass, cpu_device, fno_epochs))

    results = {}
    for name, ModelClass, dev, epochs in arch_configs:
        label = f"{name} (CPU, {epochs} ep)" if dev == cpu_device else name
        print(f"\n  [{label}]")
        res = train_with_splits(
            ModelClass, grid_np, scalar_controls, Y,
            train_idx, val_idx, test_idx, device=dev, n_epochs=epochs,
        )
        results[name] = res
        print(f"    train R²={res['train_r2']:.4f}  val R²={res['val_r2']:.4f}"
              f"  test R²={res['test_r2']:.4f}")

    plot_bar_comparison(
        results,
        title=(
            "Architecture Comparison — MLP vs CNN vs FNO\n"
            f"All spatial fields at 0.25°, chunk-based 70/15/15 split"
            f"  (FNO: {fno_epochs} epochs on CPU)"
        ),
        save_path=fig_dir / "nn_arch_comparison.png",
    )
    rows = [
        {"architecture": k, "train_r2": v["train_r2"], "val_r2": v["val_r2"],
         "test_r2": v["test_r2"]}
        for k, v in results.items()
    ]
    pd.DataFrame(rows).to_csv(tables_dir / "nn_arch_comparison.csv", index=False)
    return results


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


# ── New experiments (Steps 0–3) ───────────────────────────────────────────────

def run_exp_baseline(months, fig_dir, tables_dir, device, df=None):
    """Experiment: Baseline diagnostics — log1p transform and cyclic scalar controls.

    Evaluates three configurations of the 4-channel error-only MLP to identify
    quick wins from target distribution and encoding improvements:
      (a) raw target, raw scalars (matches current error-only baseline)
      (b) log1p target, raw scalars
      (c) log1p target, cyclic scalars (sin/cos hour and month)

    Also saves a diagnostic figure showing the target distribution histogram
    and scatter vs system_lmp_std.

    All configs use the same chunk-based 70/15/15 split and report both
    transformed R² and native-scale (inverted) R².

    Parameters
    ----------
    months     : list of (year, month)
    fig_dir    : Path
    tables_dir : Path
    device     : torch.device
    df         : pre-loaded DataFrame (optional)

    Returns
    -------
    dict {config_name: {train_r2, val_r2, test_r2, native_test_r2, ...}}
    """
    print("\n" + "=" * 70)
    print("EXPERIMENT: BASELINE DIAGNOSTICS (target transform + scalar encoding)")
    print("=" * 70)

    if df is None:
        df = load_pixel_data_for_nn(months)

    # ── Target distribution diagnostic figure ─────────────────────────────────
    hourly_y_raw = df.groupby("valid_time")[DEPVAR].first().dropna()
    Y_all_raw = hourly_y_raw.values

    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    axes[0].hist(Y_all_raw[Y_all_raw > 0], bins=80, log=True, color="steelblue",
                 edgecolor="white", linewidth=0.3)
    axes[0].set_xlabel("economic_congestion_cost ($)")
    axes[0].set_ylabel("Count (log scale)")
    axes[0].set_title("Target Distribution (positive values, log-y)")

    # Scatter vs system_lmp_std (system-wide)
    if "system_lmp_std" in df.columns:
        hourly_lmp = df.groupby("valid_time")["system_lmp_std"].first()
        lmp_aligned = hourly_lmp.reindex(hourly_y_raw.index)
        valid = ~lmp_aligned.isna()
        axes[1].scatter(lmp_aligned[valid].values, hourly_y_raw[valid].values,
                        alpha=0.15, s=4, color="darkorange")
        axes[1].set_xlabel("system_lmp_std ($/MWh)")
        axes[1].set_ylabel("economic_congestion_cost ($)")
        axes[1].set_title("Congestion Cost vs LMP Spread")
    fig.tight_layout()
    dist_path = fig_dir / "nn_target_distribution.png"
    fig.savefig(dist_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved distribution figure: {dist_path}")

    # ── Load data (4-ch errors only, raw scalars as baseline) ─────────────────
    grid_np, scalar_raw, Y_raw, pc, hour_idx, _ = load_multi_field_data(
        months, spatial_fields=ERROR_FIELDS, df=df, scalar_mode="raw"
    )
    T, C, H, W = grid_np.shape
    spatial_dim = C * H * W
    train_idx, val_idx, test_idx = make_chunk_splits(hour_idx)
    print(f"  Grid: ({T}, {C}, {H}, {W})  Split: {len(train_idx)}/{len(val_idx)}/{len(test_idx)}")

    scalar_cyc = build_scalar_controls(hour_idx, cyclic=True)
    log1p_fwd, log1p_inv = y_transform_log1p()
    Y_log = log1p_fwd(np.clip(Y_raw, 0, None)).astype(np.float32)

    configs = [
        ("(a) raw_target_raw_scalars",   Y_raw,   scalar_raw, None,        None),
        ("(b) log1p_target_raw_scalars", Y_log,   scalar_raw, Y_raw, log1p_inv),
        ("(c) log1p_target_cyc_scalars", Y_log,   scalar_cyc, Y_raw, log1p_inv),
    ]

    results = {}
    for name, Y_use, sc_use, y_nat, y_inv in configs:
        n_sc = sc_use.shape[1]
        print(f"\n  [{name}]")
        res = train_with_splits(
            _make_mlp_cls(spatial_dim, n_sc),
            grid_np, sc_use, Y_use,
            train_idx, val_idx, test_idx, device,
            y_native=y_nat, y_inverse=y_inv,
        )
        results[name] = res
        native_r2 = res.get("native_test_r2", res["test_r2"])
        print(f"    train R²={res['train_r2']:.4f}  val R²={res['val_r2']:.4f}"
              f"  test R²(tfm)={res['test_r2']:.4f}  native R²={native_r2:.4f}")

    # Bar chart using native R² for log1p configs, test_r2 for raw config
    bar_data = {}
    for name, res in results.items():
        r2_plot = res.get("native_test_r2", res["test_r2"])
        bar_data[name] = {"r2": r2_plot, "r2_std": 0.0}

    plot_bar_comparison(
        bar_data,
        title="Baseline Diagnostics — 4-ch Error MLP\nNative-scale test R² (log1p inverted)",
        save_path=fig_dir / "nn_baseline_comparison.png",
    )

    rows = []
    for name, res in results.items():
        rows.append({
            "config": name,
            "train_r2": res["train_r2"],
            "val_r2": res["val_r2"],
            "test_r2": res["test_r2"],
            "native_test_r2": res.get("native_test_r2", float("nan")),
            "native_test_rmse": res.get("native_test_rmse", float("nan")),
            "tail_test_r2": res.get("tail_test_r2", float("nan")),
        })
    pd.DataFrame(rows).to_csv(tables_dir / "nn_baseline_comparison.csv", index=False)
    return results


def run_exp_infra(months, fig_dir, tables_dir, device, df=None):
    """Experiment: Infrastructure channel ablation.

    Builds on the best config from run_exp_baseline (log1p target, cyclic
    scalars) and tests three channel configurations:
      (a) errors only (4 ch) — matches Step 0 best baseline
      (b) errors + infra channels (4 + n_infra ch)
      (c) errors + interaction channels (err × wind_cap + err × load_mask)

    Reports native-scale R² for all configs.

    Parameters
    ----------
    months     : list of (year, month)
    fig_dir    : Path
    tables_dir : Path
    device     : torch.device
    df         : pre-loaded DataFrame (optional)

    Returns
    -------
    dict {config_name: {train_r2, val_r2, test_r2, native_test_r2, ...}}
    """
    print("\n" + "=" * 70)
    print("EXPERIMENT: INFRASTRUCTURE CHANNELS (infra spatial features)")
    print("=" * 70)

    if df is None:
        df = load_pixel_data_for_nn(months)

    log1p_fwd, log1p_inv = y_transform_log1p()

    # Load with infra channels
    grid_with_infra, scalar_cyc, Y_raw, pc, hour_idx, loaded_fields_infra = load_multi_field_data(
        months, spatial_fields=ERROR_FIELDS, df=df,
        scalar_mode="cyclic", include_infra=True,
    )
    Y_log = log1p_fwd(np.clip(Y_raw, 0, None)).astype(np.float32)
    T, C_all, H, W = grid_with_infra.shape
    n_sc = scalar_cyc.shape[1]

    train_idx, val_idx, test_idx = make_chunk_splits(hour_idx)
    print(f"  Grid: ({T}, {C_all}, {H}, {W})  n_infra={C_all - len(ERROR_FIELDS)}")

    # Identify error channel indices vs infra channel indices
    n_err = len([f for f in loaded_fields_infra if f in ERROR_FIELDS])
    grid_errors_only = grid_with_infra[:, :n_err, :, :]

    # Build interaction channels: err × wind_cap_normalized, err × load_center
    wind_col = "nameplate_mw_tech_onshore_wind_turbine"
    load_col = "load_center"

    def _make_interaction_grid(grid_err, infra_full, fields_all):
        """Multiply error channels by normalized wind capacity and load masks.

        Parameters
        ----------
        grid_err   : ndarray (T, n_err, H, W)
        infra_full : ndarray (T, n_infra, H, W) — the infra portion (already broadcast)
        fields_all : list of str — all loaded field names (error + infra)

        Returns
        -------
        ndarray (T, n_err + 2*n_err, H, W)
        """
        n_err_ch = grid_err.shape[1]
        n_infra_start = n_err_ch  # infra starts after error channels in grid_with_infra

        # Find wind cap and load center indices within the full grid
        wind_idx = None
        load_idx = None
        for i, f in enumerate(fields_all):
            if f == wind_col:
                wind_idx = i
            if f == load_col:
                load_idx = i

        interact_parts = [grid_err]

        if wind_idx is not None:
            wind_mask = infra_full[0, wind_idx - n_infra_start]  # (H, W)
            wind_norm = wind_mask / (wind_mask.max() + 1e-8)
            # Broadcast (H, W) → (1, 1, H, W) via newaxis; numpy auto-broadcasts against (T, C, H, W)
            interact_parts.append(grid_err * wind_norm[np.newaxis, np.newaxis])

        if load_idx is not None:
            load_mask = infra_full[0, load_idx - n_infra_start]  # (H, W)
            interact_parts.append(grid_err * load_mask[np.newaxis, np.newaxis])

        return np.concatenate(interact_parts, axis=1).astype(np.float32)

    grid_infra_portion = grid_with_infra[:, n_err:, :, :]

    configs = [
        ("(a) errors_only_4ch",         grid_errors_only),
        ("(b) errors_plus_infra",        grid_with_infra),
        ("(c) errors_plus_interactions", _make_interaction_grid(
            grid_errors_only, grid_infra_portion, loaded_fields_infra)),
    ]

    results = {}
    for name, grid_use in configs:
        _, C_use, H_use, W_use = grid_use.shape
        spatial_dim = C_use * H_use * W_use
        print(f"\n  [{name}] C={C_use}")
        res = train_with_splits(
            _make_mlp_cls(spatial_dim, n_sc),
            grid_use, scalar_cyc, Y_log,
            train_idx, val_idx, test_idx, device,
            y_native=Y_raw, y_inverse=log1p_inv,
        )
        results[name] = res
        native_r2 = res.get("native_test_r2", float("nan"))
        print(f"    train R²={res['train_r2']:.4f}  val R²={res['val_r2']:.4f}"
              f"  test R²(tfm)={res['test_r2']:.4f}  native R²={native_r2:.4f}")

    bar_data = {n: {"r2": r.get("native_test_r2", r["test_r2"]), "r2_std": 0.0}
                for n, r in results.items()}
    plot_bar_comparison(
        bar_data,
        title="Infrastructure Channel Ablation — MLP (log1p, cyclic scalars)\nNative-scale test R²",
        save_path=fig_dir / "nn_infra_comparison.png",
    )

    rows = [{"config": n, "train_r2": r["train_r2"], "val_r2": r["val_r2"],
             "test_r2": r["test_r2"],
             "native_test_r2": r.get("native_test_r2", float("nan")),
             "native_test_rmse": r.get("native_test_rmse", float("nan")),
             "tail_test_r2": r.get("tail_test_r2", float("nan"))}
            for n, r in results.items()]
    pd.DataFrame(rows).to_csv(tables_dir / "nn_infra_comparison.csv", index=False)
    return results


def run_exp_cluster(months, fig_dir, tables_dir, device, df=None):
    """Experiment: Cluster-level aggregated error features.

    Exploits the strong spatial correlation in forecast errors by summarizing
    them at the k=7 cluster level. Tests three configurations:
      (a) cluster features only (no spatial grid) — scalar MLP
      (b) spatial (error grid) + cluster features concatenated onto scalars
      (c) spatial only — matches Step 0c best baseline

    All configs use log1p target and cyclic scalar controls.

    Parameters
    ----------
    months     : list of (year, month)
    fig_dir    : Path
    tables_dir : Path
    device     : torch.device
    df         : pre-loaded DataFrame (optional)

    Returns
    -------
    dict {config_name: {train_r2, val_r2, test_r2, native_test_r2, ...}}
    """
    import torch
    import torch.nn as nn

    print("\n" + "=" * 70)
    print("EXPERIMENT: CLUSTER FEATURES (regional aggregates)")
    print("=" * 70)

    if df is None:
        df = load_pixel_data_for_nn(months)

    log1p_fwd, log1p_inv = y_transform_log1p()

    grid_np, scalar_cyc, Y_raw, pc, hour_idx, _ = load_multi_field_data(
        months, spatial_fields=ERROR_FIELDS, df=df, scalar_mode="cyclic",
    )
    Y_log = log1p_fwd(np.clip(Y_raw, 0, None)).astype(np.float32)
    T, C, H, W = grid_np.shape
    n_sc = scalar_cyc.shape[1]
    train_idx, val_idx, test_idx = make_chunk_splits(hour_idx)
    print(f"  Grid: ({T}, {C}, {H}, {W})  Split: {len(train_idx)}/{len(val_idx)}/{len(test_idx)}")

    cluster_feats = load_cluster_error_features(months, hour_idx)
    n_cluster = cluster_feats.shape[1]
    print(f"  Cluster features: {n_cluster} columns")

    results = {}

    # ── (c) spatial only — same as Step 0c, gives the reference R² ───────────
    print(f"\n  [(c) spatial_only]")
    res_c = train_with_splits(
        _make_mlp_cls(C * H * W, n_sc),
        grid_np, scalar_cyc, Y_log,
        train_idx, val_idx, test_idx, device,
        y_native=Y_raw, y_inverse=log1p_inv,
    )
    results["(c) spatial_only"] = res_c
    print(f"    native R²={res_c.get('native_test_r2', float('nan')):.4f}")

    # ── (a) cluster features only — scalar MLP ────────────────────────────────
    # Feed cluster features as the "spatial" grid (flatten to 1D, no grid needed)
    # Build a 1×1×n_cluster pseudo-grid so train_with_splits works unchanged
    cluster_grid = cluster_feats[:, np.newaxis, np.newaxis, :]  # (T, 1, 1, n_cluster)
    print(f"\n  [(a) cluster_only]")
    res_a = train_with_splits(
        _make_mlp_cls(n_cluster, n_sc),
        cluster_grid, scalar_cyc, Y_log,
        train_idx, val_idx, test_idx, device,
        y_native=Y_raw, y_inverse=log1p_inv,
    )
    results["(a) cluster_only"] = res_a
    print(f"    native R²={res_a.get('native_test_r2', float('nan')):.4f}")

    # ── (b) spatial + cluster — concat cluster features onto scalar controls ──
    scalar_with_cluster = np.concatenate([scalar_cyc, cluster_feats], axis=1)
    n_sc_ext = scalar_with_cluster.shape[1]
    print(f"\n  [(b) spatial_plus_cluster]")
    res_b = train_with_splits(
        _make_mlp_cls(C * H * W, n_sc_ext),
        grid_np, scalar_with_cluster, Y_log,
        train_idx, val_idx, test_idx, device,
        y_native=Y_raw, y_inverse=log1p_inv,
    )
    results["(b) spatial_plus_cluster"] = res_b
    print(f"    native R²={res_b.get('native_test_r2', float('nan')):.4f}")

    bar_data = {n: {"r2": r.get("native_test_r2", r["test_r2"]), "r2_std": 0.0}
                for n, r in results.items()}
    plot_bar_comparison(
        bar_data,
        title="Cluster Feature Ablation — MLP (log1p, cyclic scalars)\nNative-scale test R²",
        save_path=fig_dir / "nn_cluster_comparison.png",
    )

    rows = [{"config": n, "train_r2": r["train_r2"], "val_r2": r["val_r2"],
             "test_r2": r["test_r2"],
             "native_test_r2": r.get("native_test_r2", float("nan")),
             "native_test_rmse": r.get("native_test_rmse", float("nan")),
             "tail_test_r2": r.get("tail_test_r2", float("nan"))}
            for n, r in results.items()]
    pd.DataFrame(rows).to_csv(tables_dir / "nn_cluster_comparison.csv", index=False)
    return results


def run_exp_nodal(months, fig_dir, tables_dir, device, df=None):
    """Experiment: Two-stage nodal LMP auxiliary supervision.

    Stage 1: Train an MLP that predicts per-node LMP from the spatial error
    grid + cyclic scalars. This provides auxiliary supervision from the ~N_nodes
    nodal LMP signals. Output dim = N_nodes (capped at 200 most-covered nodes).

    Stage 2: Take Stage 1 model predictions on all hours (standardized), feed as
    scalar features to a small MLP that predicts log1p congestion cost. This
    tests whether the nodal signal contains information about congestion beyond
    what the spatial error grid captures directly.

    Reports stage1_nodal_r2, stage2_congestion_native_r2, and a baseline
    congestion native R² from the 4-ch spatial MLP.

    Parameters
    ----------
    months     : list of (year, month)
    fig_dir    : Path
    tables_dir : Path
    device     : torch.device
    df         : pre-loaded DataFrame (optional)

    Returns
    -------
    dict with stage1 and stage2 results plus baseline comparison
    """
    import torch
    import torch.nn as nn

    print("\n" + "=" * 70)
    print("EXPERIMENT: NODAL TWO-STAGE AUXILIARY SUPERVISION")
    print("=" * 70)

    dirs = setup_directories()
    processed = Path(dirs["processed"])

    if df is None:
        df = load_pixel_data_for_nn(months)

    log1p_fwd, log1p_inv = y_transform_log1p()

    # ── Load spatial grid and congestion target ────────────────────────────────
    grid_np, scalar_cyc, Y_raw, pc, hour_idx, _ = load_multi_field_data(
        months, spatial_fields=ERROR_FIELDS, df=df, scalar_mode="cyclic",
    )
    Y_log = log1p_fwd(np.clip(Y_raw, 0, None)).astype(np.float32)
    T, C, H, W = grid_np.shape
    n_sc = scalar_cyc.shape[1]
    train_idx, val_idx, test_idx = make_chunk_splits(hour_idx)
    print(f"  Grid: ({T}, {C}, {H}, {W})")

    # ── Load nodal LMP data ────────────────────────────────────────────────────
    years = sorted({y for y, _ in months})
    month_set = {m for _, m in months}
    node_dfs = []
    for year in years:
        tag = f"{year}01_{year}12"
        node_path = processed / f"node_hourly_gfs+hrrr_era5_{tag}.csv"
        if not node_path.exists():
            print(f"  [WARNING] Node CSV not found: {node_path}")
            continue
        ndf = pd.read_csv(node_path, parse_dates=["hour"])
        if ndf["hour"].dt.tz is not None:
            ndf["hour"] = ndf["hour"].dt.tz_localize(None)
        ndf = ndf[ndf["hour"].dt.month.isin(month_set)]
        if "lmp" in ndf.columns:
            node_dfs.append(ndf[["hour", "settlement_point", "lmp"]])

    if not node_dfs:
        print("  [nodal] No node CSV found — skipping nodal experiment")
        return {}

    node_all = pd.concat(node_dfs, ignore_index=True)

    # Pivot to (hour, settlement_point) matrix
    node_pivot = node_all.pivot_table(
        index="hour", columns="settlement_point", values="lmp", aggfunc="first"
    )

    # Align to hour_idx; keep nodes with ≥90% coverage
    node_aligned = node_pivot.reindex(hour_idx)
    coverage = node_aligned.notna().mean(axis=0)
    good_nodes = coverage[coverage >= 0.90].index.tolist()
    print(f"  Nodes with ≥90% coverage: {len(good_nodes)}")

    # Cap at 200 most-covered nodes
    if len(good_nodes) > 200:
        top_nodes = coverage[good_nodes].nlargest(200).index.tolist()
        good_nodes = top_nodes
        print(f"  Capped to 200 nodes")

    if len(good_nodes) < 5:
        print("  Too few nodes with good coverage — skipping nodal experiment")
        return {}

    Y_nodal = node_aligned[good_nodes].copy()
    # Fill residual missing with per-node means computed from train hours
    train_means = Y_nodal.iloc[train_idx].mean(axis=0)
    Y_nodal = Y_nodal.fillna(train_means)
    Y_nodal_np = Y_nodal.values.astype(np.float32)  # (T, N_nodes)
    N_nodes = Y_nodal_np.shape[1]
    print(f"  Y_nodal shape: {Y_nodal_np.shape}")

    # ── Stage 1: predict nodal LMP ────────────────────────────────────────────
    print("\n  [Stage 1] Training nodal LMP predictor...")

    # Per-node standardization using train statistics
    nodal_train_mean = Y_nodal_np[train_idx].mean(axis=0)
    nodal_train_std = Y_nodal_np[train_idx].std(axis=0) + 1e-8
    Y_nodal_norm = ((Y_nodal_np - nodal_train_mean) / nodal_train_std).astype(np.float32)

    import io
    import torch.nn.functional as F
    from torch.utils.data import TensorDataset, DataLoader

    spatial_dim = C * H * W

    # Normalize scalars for Stage 1 using train statistics
    sc_mean_s1 = scalar_cyc[train_idx].mean(axis=0)
    sc_std_s1 = scalar_cyc[train_idx].std(axis=0) + 1e-8
    scalar_norm_s1 = ((scalar_cyc - sc_mean_s1) / sc_std_s1).astype(np.float32)

    grid_t = torch.tensor(grid_np, dtype=torch.float32)
    scalar_t_s1 = torch.tensor(scalar_norm_s1, dtype=torch.float32)
    Y_nod_t = torch.tensor(Y_nodal_norm, dtype=torch.float32)

    # Multi-output MLP: flattened spatial + scalars → N_nodes
    hidden = 512
    nodal_net = nn.Sequential(
        nn.Linear(spatial_dim + n_sc, hidden),
        nn.LayerNorm(hidden), nn.GELU(), nn.Dropout(0.3),
        nn.Linear(hidden, hidden // 2),
        nn.LayerNorm(hidden // 2), nn.GELU(), nn.Dropout(0.3),
        nn.Linear(hidden // 2, N_nodes),
    ).to(device)
    print(f"    Nodal MLP parameters: {sum(p.numel() for p in nodal_net.parameters() if p.requires_grad):,}")

    S1_EPOCHS = 60
    steps_per_epoch = max(1, (len(train_idx) + BATCH_SIZE - 1) // BATCH_SIZE)
    opt_s1 = torch.optim.AdamW(nodal_net.parameters(), lr=LR, weight_decay=WEIGHT_DECAY)
    scheduler_s1 = torch.optim.lr_scheduler.OneCycleLR(
        opt_s1, max_lr=LR, epochs=S1_EPOCHS, steps_per_epoch=steps_per_epoch,
        pct_start=0.1, anneal_strategy="cos",
    )
    loader_s1 = DataLoader(
        TensorDataset(
            grid_t[train_idx].to(device),
            scalar_t_s1[train_idx].to(device),
            Y_nod_t[train_idx].to(device),
        ),
        batch_size=BATCH_SIZE, shuffle=True,
    )

    best_val_s1 = float("inf")
    best_state_s1 = None
    patience_s1 = 0

    for ep in range(S1_EPOCHS):
        nodal_net.train()
        for xb, sb, yb in loader_s1:
            flat = xb.reshape(xb.shape[0], -1)
            out = nodal_net(torch.cat([flat, sb], dim=1))
            loss = F.mse_loss(out, yb)
            opt_s1.zero_grad()
            loss.backward()
            nn.utils.clip_grad_norm_(nodal_net.parameters(), 1.0)
            opt_s1.step()
            scheduler_s1.step()

        nodal_net.eval()
        with torch.no_grad():
            flat_val = grid_t[val_idx].to(device).reshape(len(val_idx), -1)
            val_pred = nodal_net(torch.cat([flat_val, scalar_t_s1[val_idx].to(device)], dim=1))
            val_loss = F.mse_loss(val_pred, Y_nod_t[val_idx].to(device)).item()

        if val_loss < best_val_s1 - 1e-5:
            best_val_s1 = val_loss
            buf = io.BytesIO()
            torch.save(nodal_net.state_dict(), buf)
            best_state_s1 = buf.getvalue()
            patience_s1 = 0
        else:
            patience_s1 += 1
            if patience_s1 >= PATIENCE:
                print(f"    Early stop at epoch {ep+1}")
                break

    if best_state_s1 is not None:
        nodal_net.load_state_dict(torch.load(io.BytesIO(best_state_s1), weights_only=False))

    # Evaluate Stage 1 R² (mean across nodes on test set)
    nodal_net.eval()
    with torch.no_grad():
        flat_te = grid_t[test_idx].to(device).reshape(len(test_idx), -1)
        pred_nod_te = nodal_net(
            torch.cat([flat_te, scalar_t_s1[test_idx].to(device)], dim=1)
        ).cpu().numpy()  # (n_test, N_nodes)

    node_r2s = [
        _r2(Y_nodal_norm[test_idx, j], pred_nod_te[:, j])
        for j in range(N_nodes)
    ]
    stage1_r2 = float(np.nanmean(node_r2s))
    print(f"    Stage 1 nodal test R² (mean across {N_nodes} nodes): {stage1_r2:.4f}")

    # ── Stage 2: use nodal predictions to predict congestion cost ─────────────
    print("\n  [Stage 2] Training congestion cost predictor from nodal features...")

    # Get Stage 1 predictions on all hours
    nodal_net.eval()
    with torch.no_grad():
        flat_all = grid_t.to(device).reshape(T, -1)
        pred_nod_all = nodal_net(
            torch.cat([flat_all, scalar_t_s1.to(device)], dim=1)
        ).cpu().numpy()  # (T, N_nodes)

    # Standardize nodal predictions (using train statistics)
    nod_pred_mean = pred_nod_all[train_idx].mean(axis=0)
    nod_pred_std = pred_nod_all[train_idx].std(axis=0) + 1e-8
    nod_pred_norm = ((pred_nod_all - nod_pred_mean) / nod_pred_std).astype(np.float32)

    # Stage 2 uses nodal predictions as the "spatial" input (pseudo-grid)
    nod_grid = nod_pred_norm[:, np.newaxis, np.newaxis, :]  # (T, 1, 1, N_nodes)

    res_s2 = train_with_splits(
        _make_mlp_cls(N_nodes, n_sc),
        nod_grid, scalar_cyc, Y_log,
        train_idx, val_idx, test_idx, device,
        y_native=Y_raw, y_inverse=log1p_inv,
    )
    stage2_native_r2 = res_s2.get("native_test_r2", float("nan"))
    print(f"    Stage 2 congestion native R²: {stage2_native_r2:.4f}")

    # ── Baseline congestion (4-ch errors, log1p, cyclic) ─────────────────────
    print("\n  [Baseline] 4-ch spatial MLP for comparison...")
    res_base = train_with_splits(
        _make_mlp_cls(C * H * W, n_sc),
        grid_np, scalar_cyc, Y_log,
        train_idx, val_idx, test_idx, device,
        y_native=Y_raw, y_inverse=log1p_inv,
    )
    baseline_native_r2 = res_base.get("native_test_r2", float("nan"))
    print(f"    Baseline congestion native R²: {baseline_native_r2:.4f}")

    bar_data = {
        "Stage 2 (nodal→congestion)": {"r2": stage2_native_r2, "r2_std": 0.0},
        "Baseline (spatial errors)":  {"r2": baseline_native_r2, "r2_std": 0.0},
    }
    plot_bar_comparison(
        bar_data,
        title=f"Nodal Two-Stage vs Baseline\nNative-scale test R² (Stage 1 nodal R²={stage1_r2:.3f})",
        save_path=fig_dir / "nn_nodal_comparison.png",
    )

    rows = [
        {"metric": "stage1_nodal_r2",             "value": stage1_r2},
        {"metric": "stage2_congestion_native_r2", "value": stage2_native_r2},
        {"metric": "baseline_congestion_native_r2", "value": baseline_native_r2},
    ]
    pd.DataFrame(rows).to_csv(tables_dir / "nn_nodal_comparison.csv", index=False)

    return {
        "stage1_r2": stage1_r2,
        "stage2": res_s2,
        "baseline": res_base,
    }


# ── Round-2 helpers ─────────────────────────────────────────────────────────────

def load_cluster_features_v2(months, hour_idx, include_lmp_std=False, extra_fields=None):
    """Expanded cluster feature loader with optional leakage control.

    Loads cluster_hourly CSV and pivots to (T, n_features). Unlike the original
    load_cluster_error_features, this version lets the caller explicitly
    include or exclude lmp_std (which is contemporaneous LMP dispersion —
    correlated with the congestion cost target and should be excluded for
    clean evaluation).

    Parameters
    ----------
    months          : list of (year, month)
    hour_idx        : pd.DatetimeIndex (T,)
    include_lmp_std : bool — whether to include cluster lmp_std (default False)
    extra_fields    : list of additional cluster-level fields to include, e.g.
                      ['max_abs_wspd_error_1h', 'observed_wspd', 'nameplate_mw_wind']

    Returns
    -------
    ndarray (T, n_features) float32
    list of str — column names
    """
    dirs = setup_directories()
    processed = Path(dirs["processed"])
    years = sorted({y for y, _ in months})
    month_set = {m for _, m in months}

    base_fields = ["temp_error_1h", "wspd_error_1h", "temp_error_0h", "wspd_error_0h",
                   "load_error_1h"]
    if include_lmp_std:
        base_fields.append("lmp_std")
    if extra_fields:
        base_fields += [f for f in extra_fields if f not in base_fields]

    dfs = []
    for year in years:
        path = processed / f"cluster_hourly_gfs+hrrr_k7_era5_{year}01_{year}12.csv"
        if not path.exists():
            continue
        df_c = pd.read_csv(path, parse_dates=["hour"])
        if df_c["hour"].dt.tz is not None:
            df_c["hour"] = df_c["hour"].dt.tz_localize(None)
        df_c = df_c[df_c["hour"].dt.month.isin(month_set)]
        dfs.append(df_c)

    if not dfs:
        print("  [cluster_v2] No cluster CSV found — returning zeros")
        return np.zeros((len(hour_idx), 1), dtype=np.float32), ["zero"]

    df_all = pd.concat(dfs, ignore_index=True)
    avail_fields = [f for f in base_fields if f in df_all.columns]

    pivot_parts = []
    col_names = []
    for field in avail_fields:
        piv = df_all.pivot_table(index="hour", columns="cluster", values=field, aggfunc="first")
        piv.columns = [f"c{int(c)}_{field}" for c in piv.columns]
        col_names += list(piv.columns)
        pivot_parts.append(piv)

    # Capacity-weighted error interaction: wspd_error_1h * nameplate_mw_wind per cluster
    if "wspd_error_1h" in avail_fields and "nameplate_mw_wind" in df_all.columns:
        df_all["wspd_err_x_wind"] = df_all["wspd_error_1h"] * np.log1p(
            df_all["nameplate_mw_wind"].fillna(0)
        )
        piv = df_all.pivot_table(index="hour", columns="cluster",
                                 values="wspd_err_x_wind", aggfunc="first")
        piv.columns = [f"c{int(c)}_wspd_err_x_wind" for c in piv.columns]
        col_names += list(piv.columns)
        pivot_parts.append(piv)

    wide = pd.concat(pivot_parts, axis=1).sort_index()
    col_means = wide.mean()
    wide = wide.reindex(hour_idx).fillna(col_means).fillna(0)

    return wide.values.astype(np.float32), list(wide.columns)


def build_ar_features(Y, hour_idx, lags=(1, 24, 168)):
    """Build autoregressive lag features from the target time series.

    Uses calendar-aware shifting so that month boundaries naturally produce
    NaN (forecast errors do not propagate across the inter-month gaps).
    NaN values are filled with the mean of the non-NaN entries for each lag.

    Parameters
    ----------
    Y        : ndarray (T,) — target values (native scale)
    hour_idx : pd.DatetimeIndex (T,)
    lags     : tuple of int — lag lengths in hours

    Returns
    -------
    ndarray (T, n_lags) float32
    list of str — column names ['lag_1h', 'lag_24h', ...]
    """
    ser = pd.Series(Y.astype(float), index=hour_idx)
    cols = {}
    for L in lags:
        shifted = ser.shift(freq=pd.Timedelta(hours=L))
        aligned = shifted.reindex(hour_idx)
        fill = aligned.mean()
        cols[f"lag_{L}h"] = aligned.fillna(fill).fillna(0).values
    df = pd.DataFrame(cols, index=hour_idx)
    return df.values.astype(np.float32), list(df.columns)


def _make_flat_cluster_mlp(in_dim):
    """Return an MLP class that accepts (x_spatial, x_scalar) and ignores spatial input.

    Used in cluster/AR experiments where all features are packed into x_scalar.
    The (x_spatial, x_scalar) signature keeps it compatible with train_with_splits.

    Parameters
    ----------
    in_dim : int — full feature dimension (cluster features + scalars + optional AR)

    Returns
    -------
    class — instantiates to nn.Module
    """
    import torch.nn as nn

    class FlatClusterMLP(nn.Module):
        """Flat MLP on cluster + scalar features; spatial input is ignored."""
        def __init__(self):
            super().__init__()
            self.net = nn.Sequential(
                nn.Linear(in_dim, 256), nn.LayerNorm(256), nn.GELU(), nn.Dropout(0.3),
                nn.Linear(256, 128), nn.GELU(), nn.Dropout(0.2),
                nn.Linear(128, 1),
            )

        def forward(self, x_spatial, x_scalar):
            return self.net(x_scalar).squeeze(-1)

    return FlatClusterMLP


def _make_gru_cls(n_cluster_feat, n_scalar, seq_len=24, hidden=128, n_layers=2, dropout=0.25):
    """Return a GRU class for temporal sequence modeling of cluster features.

    Processes a sliding window of cluster features (seq_len hours) plus current
    scalar controls, outputs a single congestion cost prediction.

    Parameters
    ----------
    n_cluster_feat : int — number of cluster features per timestep
    n_scalar       : int — scalar temporal controls (appended to GRU output)
    seq_len        : int — length of input sequence (default 24 hours)
    hidden         : int — GRU hidden units
    n_layers       : int — GRU stacked layers
    dropout        : float

    Returns
    -------
    class — instantiates to nn.Module with forward(x_seq, x_scalar) → (batch, 1)
    """
    import torch
    import torch.nn as nn

    class GRUModel(nn.Module):
        """GRU over hourly cluster features with scalar control head."""

        def __init__(self):
            super().__init__()
            self.gru = nn.GRU(
                n_cluster_feat, hidden, num_layers=n_layers,
                batch_first=True, dropout=dropout if n_layers > 1 else 0.0,
            )
            self.head = nn.Sequential(
                nn.Linear(hidden + n_scalar, 128),
                nn.GELU(),
                nn.Dropout(dropout),
                nn.Linear(128, 1),
            )

        def forward(self, x_seq, x_scalar):
            _, h_n = self.gru(x_seq)
            h_last = h_n[-1]  # (batch, hidden)
            cat = torch.cat([h_last, x_scalar], dim=1)
            return self.head(cat).squeeze(-1)

    return GRUModel


def _build_gru_dataset(X_cluster, scalar_controls, Y, seq_len=24, hour_idx=None):
    """Build (X_seq, x_scalar, y) tuples for GRU training.

    For each time step t, the input is a (seq_len, n_features) window ending
    at t. Hours before the first sample are zero-padded.

    Parameters
    ----------
    X_cluster      : ndarray (T, n_feat) — cluster features per hour
    scalar_controls: ndarray (T, n_sc) — scalar controls
    Y              : ndarray (T,) — targets
    seq_len        : int
    hour_idx       : pd.DatetimeIndex or None — if provided, zero-pads across
                     month boundaries (gaps > 2h in the index)

    Returns
    -------
    tuple of ndarray: X_seq (T, seq_len, n_feat), x_scalar (T, n_sc), Y (T,)
    """
    T, n_feat = X_cluster.shape
    X_seq = np.zeros((T, seq_len, n_feat), dtype=np.float32)

    # Gap positions: indices where the time series restarts (month boundaries).
    # searchsorted finds the nearest gap >= each t in O(log G) per step.
    if hour_idx is not None:
        gap_pos = np.where(
            np.concatenate([[True], np.diff(hour_idx).astype("timedelta64[h]") > 2])
        )[0]
    else:
        gap_pos = np.array([0])

    for t in range(T):
        # Latest gap that is <= t gives the start of the current contiguous segment
        gi = np.searchsorted(gap_pos, t, side="right") - 1
        segment_start = gap_pos[gi] if gi >= 0 else 0
        lo = max(segment_start, t - seq_len + 1)
        window = X_cluster[lo : t + 1]
        pad_len = seq_len - len(window)
        if pad_len > 0:
            window = np.concatenate([np.zeros((pad_len, n_feat), dtype=np.float32), window])
        X_seq[t] = window

    return X_seq, scalar_controls, Y


def train_gru_with_splits(model_cls, X_seq, x_scalar, Y_log,
                          train_idx, val_idx, test_idx, device,
                          y_native=None, y_inverse=None):
    """Train a GRU model using pre-built sequence tensors.

    Mirrors train_with_splits but handles the extra sequence dimension.

    Parameters
    ----------
    model_cls  : class — instantiates to nn.Module with forward(x_seq, x_scalar)
    X_seq      : ndarray (T, seq_len, n_feat)
    x_scalar   : ndarray (T, n_sc)
    Y_log      : ndarray (T,) — transformed target
    train_idx, val_idx, test_idx : ndarray of int
    device     : torch.device
    y_native   : ndarray (T,) — native-scale target for R² inversion
    y_inverse  : callable or None — inverse transform

    Returns
    -------
    dict with train_r2, val_r2, test_r2, native_test_r2, native_test_rmse, tail_test_r2
    """
    import torch
    import torch.nn as nn
    from torch.utils.data import TensorDataset, DataLoader

    X_seq_t   = torch.tensor(X_seq,      dtype=torch.float32)
    x_sc_t    = torch.tensor(x_scalar,   dtype=torch.float32)
    Y_t       = torch.tensor(Y_log,      dtype=torch.float32)

    train_ds = TensorDataset(X_seq_t[train_idx], x_sc_t[train_idx], Y_t[train_idx])
    val_ds   = TensorDataset(X_seq_t[val_idx],   x_sc_t[val_idx],   Y_t[val_idx])

    train_dl = DataLoader(train_ds, batch_size=BATCH_SIZE, shuffle=True)
    val_dl   = DataLoader(val_ds,   batch_size=BATCH_SIZE * 4)

    model = model_cls().to(device)
    opt   = torch.optim.AdamW(model.parameters(), lr=LR, weight_decay=WEIGHT_DECAY)
    sched = torch.optim.lr_scheduler.ReduceLROnPlateau(opt, patience=7, factor=0.5)
    loss_fn = nn.MSELoss()

    best_val = float("inf")
    best_state = None
    patience_count = 0

    for epoch in range(N_EPOCHS):
        model.train()
        for xs, xc, yb in train_dl:
            xs, xc, yb = xs.to(device), xc.to(device), yb.to(device)
            opt.zero_grad()
            loss = loss_fn(model(xs, xc), yb)
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            opt.step()
        model.eval()
        val_losses = []
        with torch.no_grad():
            for xs, xc, yb in val_dl:
                xs, xc, yb = xs.to(device), xc.to(device), yb.to(device)
                val_losses.append(loss_fn(model(xs, xc), yb).item())
        val_loss = float(np.mean(val_losses))
        sched.step(val_loss)
        if val_loss < best_val - 1e-6:
            best_val = val_loss
            best_state = {k: v.cpu().clone() for k, v in model.state_dict().items()}
            patience_count = 0
        else:
            patience_count += 1
            if patience_count >= PATIENCE:
                print(f"    Early stop at epoch {epoch + 1}")
                break

    if best_state is not None:
        model.load_state_dict(best_state)

    def _predict(idx_arr):
        model.eval()
        with torch.no_grad():
            preds = model(
                X_seq_t[idx_arr].to(device),
                x_sc_t[idx_arr].to(device),
            ).cpu().numpy()
        return preds

    train_r2 = _r2(Y_log[train_idx], _predict(train_idx))
    val_r2   = _r2(Y_log[val_idx],   _predict(val_idx))
    test_r2  = _r2(Y_log[test_idx],  _predict(test_idx))

    result = {"train_r2": float(train_r2), "val_r2": float(val_r2), "test_r2": float(test_r2)}

    if y_native is not None and y_inverse is not None:
        pred_log  = _predict(test_idx)
        pred_nat  = y_inverse(pred_log)
        y_nat_tst = y_native[test_idx]
        result["native_test_r2"]   = float(_r2(pred_nat, y_nat_tst))
        result["native_test_rmse"] = float(np.sqrt(np.mean((pred_nat - y_nat_tst) ** 2)))
        q90 = np.quantile(y_nat_tst, 0.9)
        mask = y_nat_tst >= q90
        result["tail_test_r2"] = float(_r2(pred_nat[mask], y_nat_tst[mask])) if mask.sum() > 5 else float("nan")

    return result


def run_exp_ar(months, fig_dir, tables_dir, device, df=None):
    """Experiment: Leakage fix and autoregressive features.

    Inspired by Lago et al. (2021) and Ziel & Weron (2018) who show lag features
    of the target price are the strongest predictors in tabular energy forecasting.
    Binding grid constraints persist over hours, so lag-1h and lag-24h of
    economic_congestion_cost should carry real signal.

    Also fixes the lmp_std leakage present in the original cluster experiment:
    cluster lmp_std is contemporaneous LMP dispersion, directly correlated with
    the congestion cost target.

    Configs:
      (a) cluster features, NO lmp_std (clean baseline)
      (b) clean cluster + lag-1h congestion
      (c) clean cluster + lag-1h + lag-24h
      (d) clean cluster + lag-1h + lag-24h + lag-168h (weekly)

    Parameters
    ----------
    months     : list of (year, month)
    fig_dir    : Path
    tables_dir : Path
    device     : torch.device
    df         : pre-loaded DataFrame (optional)

    Returns
    -------
    dict {config_name: result_dict}
    """
    print("\n" + "=" * 70)
    print("EXPERIMENT: LEAKAGE FIX + AUTOREGRESSIVE FEATURES")
    print("=" * 70)

    import torch
    import torch.nn as nn

    if df is None:
        df = load_pixel_data_for_nn(months)

    log1p_fwd, log1p_inv = y_transform_log1p()

    grid_spatial, scalar_cyc, Y_raw, pc, hour_idx, _ = load_multi_field_data(
        months, spatial_fields=ERROR_FIELDS, df=df, scalar_mode="cyclic",
    )

    Y_log = log1p_fwd(np.clip(Y_raw, 0, None)).astype(np.float32)
    train_idx, val_idx, test_idx = make_chunk_splits(hour_idx)
    n_sc = scalar_cyc.shape[1]

    # Clean cluster features (no lmp_std)
    X_cluster, cluster_cols = load_cluster_features_v2(months, hour_idx, include_lmp_std=False)
    n_cluster = X_cluster.shape[1]
    print(f"  Clean cluster features: {n_cluster} cols")

    # AR features (computed on Y_raw, filled at boundaries)
    ar_feats, ar_names = build_ar_features(Y_raw, hour_idx, lags=(1, 24, 168))
    # Standardise AR features on training set to avoid scale issues
    ar_mean = ar_feats[train_idx].mean(axis=0, keepdims=True)
    ar_std  = ar_feats[train_idx].std(axis=0, keepdims=True) + 1e-8
    ar_feats = (ar_feats - ar_mean) / ar_std

    def _run(extra_feats=None, label=""):
        feats = np.concatenate([X_cluster, scalar_cyc]
                               + ([extra_feats] if extra_feats is not None else []), axis=1)
        dummy_spatial = np.zeros((len(feats), 1), dtype=np.float32)
        res = train_with_splits(
            _make_flat_cluster_mlp(feats.shape[1]),
            dummy_spatial[:, np.newaxis, np.newaxis, :],  # (T, 1, 1, 1) dummy
            feats, Y_log,
            train_idx, val_idx, test_idx, device,
            y_native=Y_raw, y_inverse=log1p_inv,
        )
        print(f"  [{label}] train R²={res['train_r2']:.4f}  val R²={res['val_r2']:.4f}"
              f"  native R²={res.get('native_test_r2', float('nan')):.4f}")
        return res

    import torch

    results = {}
    results["(a) cluster_no_lmp"] = _run(None, "(a) cluster_no_lmp")
    results["(b) +lag_1h"]        = _run(ar_feats[:, :1], "(b) +lag_1h")
    results["(c) +lag_1h_24h"]    = _run(ar_feats[:, :2], "(c) +lag_1h_24h")
    results["(d) +all_ar_lags"]   = _run(ar_feats,        "(d) +all_ar_lags")

    bar_data = {n: {"r2": r.get("native_test_r2", r["test_r2"]), "r2_std": 0.0}
                for n, r in results.items()}
    plot_bar_comparison(
        bar_data,
        title="AR Features + Leakage Fix — MLP on Clean Cluster Features\nNative-scale test R²",
        save_path=fig_dir / "nn_ar_comparison.png",
    )

    rows = [{"config": n, "train_r2": r["train_r2"], "val_r2": r["val_r2"],
             "test_r2": r["test_r2"],
             "native_test_r2": r.get("native_test_r2", float("nan")),
             "native_test_rmse": r.get("native_test_rmse", float("nan")),
             "tail_test_r2": r.get("tail_test_r2", float("nan"))}
            for n, r in results.items()]
    pd.DataFrame(rows).to_csv(tables_dir / "nn_ar_comparison.csv", index=False)
    return results


def run_exp_gbm(months, fig_dir, tables_dir, device, df=None):
    """Experiment: LightGBM gradient boosting on cluster + AR features.

    Inspired by Lago et al. (2021) who demonstrate gradient boosting consistently
    outperforms neural networks on tabular electricity price forecasting. With
    T=3,550 training points, tree ensembles avoid the MLP's overfitting pathology
    while naturally handling non-linear interactions.

    Configs tested:
      (a) LightGBM, clean cluster features only
      (b) LightGBM, cluster + AR lags
      (c) LightGBM, cluster + AR lags + richer cluster features (max abs error,
          observed wind, capacity-weighted error interaction)
      (d) MLP, cluster + AR lags (same features as b, for architecture comparison)

    Parameters
    ----------
    months     : list of (year, month)
    fig_dir    : Path
    tables_dir : Path
    device     : torch.device
    df         : pre-loaded DataFrame (optional)

    Returns
    -------
    dict {config_name: result_dict}
    """
    print("\n" + "=" * 70)
    print("EXPERIMENT: LIGHTGBM GRADIENT BOOSTING")
    print("=" * 70)

    try:
        import lightgbm as lgb
    except ImportError:
        print("  [skip] lightgbm not installed")
        return {}

    if df is None:
        df = load_pixel_data_for_nn(months)

    log1p_fwd, log1p_inv = y_transform_log1p()

    _, scalar_cyc, Y_raw, _, hour_idx, _ = load_multi_field_data(
        months, spatial_fields=ERROR_FIELDS, df=df, scalar_mode="cyclic",
    )
    Y_log = log1p_fwd(np.clip(Y_raw, 0, None)).astype(np.float32)
    train_idx, val_idx, test_idx = make_chunk_splits(hour_idx)

    X_cluster_base, _ = load_cluster_features_v2(months, hour_idx, include_lmp_std=False)
    X_cluster_rich, _ = load_cluster_features_v2(
        months, hour_idx, include_lmp_std=False,
        extra_fields=["max_abs_wspd_error_1h", "observed_wspd", "nameplate_mw_wind"],
    )
    ar_feats, _ = build_ar_features(Y_raw, hour_idx, lags=(1, 24, 168))
    ar_mean = ar_feats[train_idx].mean(axis=0, keepdims=True)
    ar_std  = ar_feats[train_idx].std(axis=0, keepdims=True) + 1e-8
    ar_feats = (ar_feats - ar_mean) / ar_std

    print(f"  Base cluster features: {X_cluster_base.shape[1]}")
    print(f"  Rich cluster features: {X_cluster_rich.shape[1]}")

    def _eval_gbm(X_train, X_val, X_test, y_train, label):
        model = lgb.LGBMRegressor(
            n_estimators=1000, learning_rate=0.02, num_leaves=31,
            min_child_samples=20, subsample=0.8, colsample_bytree=0.8,
            reg_alpha=0.1, reg_lambda=0.1, random_state=RANDOM_STATE,
            verbose=-1,
        )
        model.fit(
            X_train, y_train,
            eval_set=[(X_val, Y_log[val_idx])],
            callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(-1)],
        )
        train_r2 = _r2(y_train,            model.predict(X_train))
        val_r2   = _r2(Y_log[val_idx],     model.predict(X_val))
        test_r2  = _r2(Y_log[test_idx],    model.predict(X_test))
        pred_nat = log1p_inv(model.predict(X_test))
        y_nat    = Y_raw[test_idx]
        nat_r2   = _r2(y_nat, pred_nat)
        nat_rmse = float(np.sqrt(np.mean((pred_nat - y_nat) ** 2)))
        q90 = np.quantile(y_nat, 0.9)
        mask = y_nat >= q90
        tail_r2 = float(_r2(pred_nat[mask], y_nat[mask])) if mask.sum() > 5 else float("nan")
        print(f"  [{label}] train R²={train_r2:.4f}  val R²={val_r2:.4f}"
              f"  native R²={nat_r2:.4f}  tail R²={tail_r2:.4f}")
        return {"train_r2": float(train_r2), "val_r2": float(val_r2),
                "test_r2": float(test_r2), "native_test_r2": nat_r2,
                "native_test_rmse": nat_rmse, "tail_test_r2": tail_r2,
                "model": model}

    def _feats(X, include_ar=False, ar_lags=None):
        parts = [X, scalar_cyc]
        if include_ar:
            parts.append(ar_feats if ar_lags is None else ar_feats[:, :ar_lags])
        return np.concatenate(parts, axis=1)

    configs = [
        ("(a) LGB_cluster_only",   _feats(X_cluster_base),             False),
        ("(b) LGB_cluster_AR",     _feats(X_cluster_base, True),        False),
        ("(c) LGB_rich_AR",        _feats(X_cluster_rich, True),        False),
    ]
    results = {}
    for name, X_all, _ in configs:
        res = _eval_gbm(
            X_all[train_idx], X_all[val_idx], X_all[test_idx],
            Y_log[train_idx], name,
        )
        results[name] = {k: v for k, v in res.items() if k != "model"}

    # Feature importance for the best config
    best_name = max(results, key=lambda k: results[k].get("native_test_r2", -9))
    best_idx = [i for i, (n, _, _) in enumerate(configs) if n == best_name][0]
    best_X = configs[best_idx][1]
    best_model = _eval_gbm(
        best_X[train_idx], best_X[val_idx], best_X[test_idx],
        Y_log[train_idx], f"refitting {best_name}",
    )["model"]
    # Feature importance for best config — save as CSV to avoid any figure-size issues
    imp = pd.Series(best_model.feature_importances_,
                    index=[f"f{i}" for i in range(best_X.shape[1])]).nlargest(20)
    imp.to_csv(tables_dir / "nn_gbm_feature_importance.csv", header=["importance"])

    bar_data = {n: {"r2": r.get("native_test_r2", r["test_r2"]), "r2_std": 0.0}
                for n, r in results.items()}
    plot_bar_comparison(
        bar_data,
        title="LightGBM — Gradient Boosting on Cluster Features\nNative-scale test R²",
        save_path=fig_dir / "nn_gbm_comparison.png",
    )
    rows = [{"config": n, **{k: v for k, v in r.items()}}
            for n, r in results.items()]
    pd.DataFrame(rows).to_csv(tables_dir / "nn_gbm_comparison.csv", index=False)
    return results


def run_exp_gru(months, fig_dir, tables_dir, device, df=None):
    """Experiment: GRU recurrent model on cluster feature time series.

    Inspired by Khodayar et al. (2020) and Wei et al. (2019) showing LSTM/GRU
    with weather covariates captures temporal persistence in grid constraints
    that MLP misses. Each prediction uses a 24-hour sliding window of cluster
    features as input, allowing the model to learn that binding constraints
    persist across hours.

    Configs:
      (a) MLP on current-hour cluster features (same-hour baseline, no sequence)
      (b) GRU with 24h window, clean cluster features
      (c) GRU with 24h window, cluster + AR-1h (GRU + explicit lag)

    Parameters
    ----------
    months     : list of (year, month)
    fig_dir    : Path
    tables_dir : Path
    device     : torch.device
    df         : pre-loaded DataFrame (optional)

    Returns
    -------
    dict {config_name: result_dict}
    """
    print("\n" + "=" * 70)
    print("EXPERIMENT: GRU RECURRENT MODEL (24h sliding window)")
    print("=" * 70)

    if df is None:
        df = load_pixel_data_for_nn(months)

    log1p_fwd, log1p_inv = y_transform_log1p()

    _, scalar_cyc, Y_raw, _, hour_idx, _ = load_multi_field_data(
        months, spatial_fields=ERROR_FIELDS, df=df, scalar_mode="cyclic",
    )
    Y_log = log1p_fwd(np.clip(Y_raw, 0, None)).astype(np.float32)
    train_idx, val_idx, test_idx = make_chunk_splits(hour_idx)
    n_sc = scalar_cyc.shape[1]

    X_cluster, _ = load_cluster_features_v2(months, hour_idx, include_lmp_std=False)
    n_cluster = X_cluster.shape[1]
    print(f"  Cluster features: {n_cluster}, scalar: {n_sc}")

    import torch
    import torch.nn as nn

    ar_feats, _ = build_ar_features(Y_raw, hour_idx, lags=(1,))
    ar_mean = ar_feats[train_idx].mean(axis=0, keepdims=True)
    ar_std  = ar_feats[train_idx].std(axis=0, keepdims=True) + 1e-8
    ar_feats = (ar_feats - ar_mean) / ar_std

    SEQ_LEN = 24

    # Config (a): MLP baseline (no sequence)
    print("\n  [(a) MLP_cluster_baseline]")
    feats_base = np.concatenate([X_cluster, scalar_cyc], axis=1)
    dummy = np.zeros((len(feats_base), 1, 1, 1), dtype=np.float32)
    res_mlp = train_with_splits(
        _make_flat_cluster_mlp(feats_base.shape[1]), dummy, feats_base, Y_log,
        train_idx, val_idx, test_idx, device,
        y_native=Y_raw, y_inverse=log1p_inv,
    )
    print(f"    train R²={res_mlp['train_r2']:.4f}  val R²={res_mlp['val_r2']:.4f}"
          f"  native R²={res_mlp.get('native_test_r2', float('nan')):.4f}")

    # Build sliding window sequences
    print("\n  Building 24h sliding windows...")
    X_seq_base, _, _ = _build_gru_dataset(X_cluster, scalar_cyc, Y_log,
                                           seq_len=SEQ_LEN, hour_idx=hour_idx)
    X_seq_ar, _, _ = _build_gru_dataset(
        np.concatenate([X_cluster, ar_feats], axis=1), scalar_cyc, Y_log,
        seq_len=SEQ_LEN, hour_idx=hour_idx,
    )

    # Config (b): GRU, clean cluster
    print("\n  [(b) GRU_cluster_24h]")
    GRUCls_base = _make_gru_cls(n_cluster, n_sc, seq_len=SEQ_LEN)
    res_gru_base = train_gru_with_splits(
        GRUCls_base, X_seq_base, scalar_cyc, Y_log,
        train_idx, val_idx, test_idx, device,
        y_native=Y_raw, y_inverse=log1p_inv,
    )
    print(f"    train R²={res_gru_base['train_r2']:.4f}  val R²={res_gru_base['val_r2']:.4f}"
          f"  native R²={res_gru_base.get('native_test_r2', float('nan')):.4f}")

    # Config (c): GRU, cluster + AR-1h
    print("\n  [(c) GRU_cluster_AR_24h]")
    GRUCls_ar = _make_gru_cls(n_cluster + 1, n_sc, seq_len=SEQ_LEN)
    res_gru_ar = train_gru_with_splits(
        GRUCls_ar, X_seq_ar, scalar_cyc, Y_log,
        train_idx, val_idx, test_idx, device,
        y_native=Y_raw, y_inverse=log1p_inv,
    )
    print(f"    train R²={res_gru_ar['train_r2']:.4f}  val R²={res_gru_ar['val_r2']:.4f}"
          f"  native R²={res_gru_ar.get('native_test_r2', float('nan')):.4f}")

    results = {
        "(a) MLP_cluster_baseline": res_mlp,
        "(b) GRU_cluster_24h":      res_gru_base,
        "(c) GRU_cluster_AR_24h":   res_gru_ar,
    }
    bar_data = {n: {"r2": r.get("native_test_r2", r["test_r2"]), "r2_std": 0.0}
                for n, r in results.items()}
    plot_bar_comparison(
        bar_data,
        title="GRU vs MLP — Temporal Sequence Modeling\nNative-scale test R²",
        save_path=fig_dir / "nn_gru_comparison.png",
    )
    rows = [{"config": n, "train_r2": r["train_r2"], "val_r2": r["val_r2"],
             "test_r2": r["test_r2"],
             "native_test_r2": r.get("native_test_r2", float("nan")),
             "native_test_rmse": r.get("native_test_rmse", float("nan")),
             "tail_test_r2": r.get("tail_test_r2", float("nan"))}
            for n, r in results.items()]
    pd.DataFrame(rows).to_csv(tables_dir / "nn_gru_comparison.csv", index=False)
    return results


# ── Round-3 helpers ─────────────────────────────────────────────────────────────


def compute_block_bootstrap_ci(
    y_true, pred_base, pred_full, n_boot=300, block_len=None, alpha=0.05, seed=42
):
    """Circular block bootstrap CI for ΔR² = R²(full) − R²(base) (Künsch 1989).

    Resamples the test set in contiguous temporal blocks to preserve hourly
    autocorrelation, then computes ΔR² on each bootstrap resample.

    Parameters
    ----------
    y_true, pred_base, pred_full : ndarray (n_test,) — native-scale values
    n_boot    : int — bootstrap replicates (300 balances precision and speed)
    block_len : int or None — block length; defaults to ⌈n^{1/3}⌉
    alpha     : float — two-sided CI level (0.05 → 95% CI)
    seed      : int

    Returns
    -------
    dict : delta_r2, ci_lo, ci_hi, boots (ndarray n_boot), p_value (frac ≤ 0)
    """
    n = len(y_true)
    if block_len is None:
        block_len = max(5, int(np.ceil(n ** (1 / 3))))
    rng = np.random.default_rng(seed)
    delta_actual = _r2(y_true, pred_full) - _r2(y_true, pred_base)

    n_blocks_needed = int(np.ceil(n / block_len))
    deltas = []
    for _ in range(n_boot):
        starts = rng.integers(0, n, size=n_blocks_needed)
        idx = np.concatenate([
            np.arange(s, s + block_len) % n for s in starts
        ])[:n]
        d = _r2(y_true[idx], pred_full[idx]) - _r2(y_true[idx], pred_base[idx])
        deltas.append(d)

    deltas = np.array(deltas)
    return {
        "delta_r2": float(delta_actual),
        "ci_lo": float(np.percentile(deltas, 100 * alpha / 2)),
        "ci_hi": float(np.percentile(deltas, 100 * (1 - alpha / 2))),
        "boots": deltas,
        "p_value": float((deltas <= 0).mean()),
    }


def build_fpc_features(grid_np, train_idx, n_components_per_channel=20):
    """Project each spatial channel onto its leading FPC/EOF modes.

    Fits PCA on training hours only; projects all hours. This gives a
    principled basis reduction that avoids the overfitting of raw 9k-dim
    pixel regression (Ramsay & Silverman 2005; Reiss et al. 2017).

    Parameters
    ----------
    grid_np                  : ndarray (T, C, H, W)
    train_idx                : ndarray int — training hour indices
    n_components_per_channel : int — EOFs to retain per channel

    Returns
    -------
    X_fpc      : ndarray (T, C * K) float32 — per-hour mode scores
    components : list of ndarray (K, H, W) — one element per channel
    var_ratios : list of ndarray (K,) — cumulative explained variance
    """
    from sklearn.decomposition import PCA

    T, C, H, W = grid_np.shape
    scores_list, comps_list, var_list = [], [], []
    for c in range(C):
        X_c = grid_np[:, c, :, :].reshape(T, H * W)
        n_comp = min(n_components_per_channel, len(train_idx) - 1, H * W)
        pca = PCA(n_components=n_comp, random_state=RANDOM_STATE)
        pca.fit(X_c[train_idx])
        scores_list.append(pca.transform(X_c))
        comps_list.append(pca.components_.reshape(n_comp, H, W))
        var_list.append(pca.explained_variance_ratio_)

    X_fpc = np.concatenate(scores_list, axis=1).astype(np.float32)
    return X_fpc, comps_list, var_list


def build_capacity_scalar_features(df, hour_idx):
    """Build physics-motivated capacity-weighted error scalars per hour.

    Each scalar is Σ_s weight(s) · error_t(s) over ERA5 pixels, computed both
    system-wide and per geographic quadrant of Texas (lat≥31° × lon≥−100°).
    Wind-error scalars weight by log1p(wind_capacity); temp-error scalars weight
    by load_center.

    Parameters
    ----------
    df       : pd.DataFrame — pixel-hourly data with capacity + error columns
    hour_idx : pd.DatetimeIndex (T,)

    Returns
    -------
    features  : ndarray (T, n_scalars) float32
    col_names : list of str
    """
    needed = [
        "nameplate_mw_tech_onshore_wind_turbine", "load_center",
        "wspd_error_1h", "wspd_error_0h", "temp_error_1h", "temp_error_0h",
        "latitude", "longitude", "valid_time",
    ]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        print(f"  [capacity_scalars] Missing cols: {missing}; returning zeros")
        return np.zeros((len(hour_idx), 1), dtype=np.float32), ["zero"]

    work = df[needed].copy()
    work["wind_cap"] = np.log1p(work["nameplate_mw_tech_onshore_wind_turbine"].fillna(0))
    work["load_c"] = work["load_center"].fillna(0)
    # Quadrant 0–3: bits = (lat ≥ 31) × 2 + (lon ≥ −100)
    work["quad"] = (
        (work["latitude"] >= 31.0).astype(int) * 2
        + (work["longitude"] >= -100.0).astype(int)
    )

    out: dict = {}
    for err_col in ["wspd_error_1h", "wspd_error_0h", "temp_error_1h", "temp_error_0h"]:
        e = work[err_col].fillna(0)
        weight = work["wind_cap"] if "wspd" in err_col else work["load_c"]
        weighted = weight * e

        # System-wide
        sys_key = f"sys_{err_col}"
        out[sys_key] = work.assign(_w=weighted).groupby("valid_time")["_w"].sum()

        # Per-quadrant
        for q in range(4):
            q_weight = weighted * (work["quad"] == q).astype(float)
            q_key = f"q{q}_{err_col}"
            out[q_key] = work.assign(_w=q_weight).groupby("valid_time")["_w"].sum()

    agg = pd.DataFrame(out).reindex(hour_idx).fillna(0)
    return agg.values.astype(np.float32), list(agg.columns)


def _lgb_fit_eval(
    X_train, X_val, X_test, y_train_log, y_val_log, y_true_test, log1p_inv,
    label="", n_estimators=1000, learning_rate=0.02,
):
    """Shared LightGBM fit/eval helper used across Round-3 experiments.

    Parameters
    ----------
    X_train, X_val, X_test : ndarray (n, p) — already standardized
    y_train_log, y_val_log  : ndarray — log1p-transformed training/val targets
    y_true_test             : ndarray — native-scale test targets
    log1p_inv               : callable — inverse transform (np.expm1)
    label                   : str — printed after fitting
    n_estimators, learning_rate : LGB hyperparams

    Returns
    -------
    dict with train_r2, val_r2, native_test_r2, tail_test_r2, model
    """
    try:
        import lightgbm as lgb
    except ImportError:
        print("  [lgb_fit] lightgbm not installed; skipping")
        return {}

    model = lgb.LGBMRegressor(
        n_estimators=n_estimators, learning_rate=learning_rate, num_leaves=31,
        min_child_samples=20, subsample=0.8, colsample_bytree=0.8,
        reg_alpha=0.1, reg_lambda=0.1, random_state=RANDOM_STATE, verbose=-1,
    )
    model.fit(
        X_train, y_train_log,
        eval_set=[(X_val, y_val_log)],
        callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(-1)],
    )
    train_r2 = _r2(y_train_log, model.predict(X_train))
    val_r2 = _r2(y_val_log, model.predict(X_val))

    pred_log = model.predict(X_test)
    pred_nat = log1p_inv(pred_log)
    nat_r2 = _r2(y_true_test, pred_nat)
    q90 = np.quantile(y_true_test, 0.9)
    tail_mask = y_true_test >= q90
    tail_r2 = float(_r2(y_true_test[tail_mask], pred_nat[tail_mask])) if tail_mask.sum() >= 5 else float("nan")
    print(f"  [{label}] train R²={train_r2:.4f}  val R²={val_r2:.4f}  native R²={nat_r2:.4f}  tail R²={tail_r2:.4f}")
    return {
        "train_r2": float(train_r2), "val_r2": float(val_r2),
        "native_test_r2": float(nat_r2), "tail_test_r2": float(tail_r2),
        "pred_native": pred_nat, "model": model,
    }


# ── Round-3 experiments ────────────────────────────────────────────────────────


def run_exp_marginal_weather(months, fig_dir, tables_dir, device, df=None):
    """Step 1: Nested AR models isolating the marginal contribution of weather.

    Fits four nested LightGBM models (M0–M3) to quantify how much each feature
    set adds over the AR baseline, with block-bootstrap 95% CIs (Künsch 1989)
    for ΔR² and a permutation test under the null hypothesis that weather features
    are uninformative.

    Configs
    -------
    M0 : cyclic time only (hour sin/cos, month sin/cos, is_weekend)
    M1 : M0 + lag-1h + lag-24h of congestion cost (AR baseline)
    M2 : M1 + k=7 cluster weather features (no lmp_std)
    M3 : M2 + capacity-weighted cluster interactions (rich features)

    Parameters
    ----------
    months     : list of (year, month)
    fig_dir    : Path
    tables_dir : Path
    device     : torch.device (unused; LGB runs on CPU)
    df         : pre-loaded DataFrame (optional)

    Returns
    -------
    dict with results per config plus bootstrap CI and permutation p-value
    """
    print("\n" + "=" * 70)
    print("EXPERIMENT: MARGINAL WEATHER CONTRIBUTION (Steps 1 + 5)")
    print("=" * 70)

    try:
        import lightgbm as lgb  # noqa: F401
    except ImportError:
        print("  [skip] lightgbm not installed")
        return {}

    if df is None:
        df = load_pixel_data_for_nn(months)

    log1p_fwd, log1p_inv = y_transform_log1p()

    _, scalar_cyc, Y_raw, _, hour_idx, _ = load_multi_field_data(
        months, spatial_fields=ERROR_FIELDS, df=df, scalar_mode="cyclic",
    )
    Y_log = log1p_fwd(np.clip(Y_raw, 0, None)).astype(np.float32)
    train_idx, val_idx, test_idx = make_chunk_splits(hour_idx)

    # AR features (lag-1h + lag-24h), standardised on train
    ar_feats, _ = build_ar_features(Y_raw, hour_idx, lags=(1, 24))
    ar_mean = ar_feats[train_idx].mean(0, keepdims=True)
    ar_std = ar_feats[train_idx].std(0, keepdims=True) + 1e-8
    ar_feats = (ar_feats - ar_mean) / ar_std

    # Cluster features (no lmp_std) and rich cluster features
    X_cl_base, _ = load_cluster_features_v2(months, hour_idx, include_lmp_std=False)
    X_cl_rich, _ = load_cluster_features_v2(
        months, hour_idx, include_lmp_std=False,
        extra_fields=["max_abs_wspd_error_1h", "observed_wspd", "nameplate_mw_wind"],
    )

    # Feature matrices for each model
    def _make_X(include_ar, include_cluster, rich=False):
        parts = [scalar_cyc]
        if include_ar:
            parts.append(ar_feats)
        if include_cluster:
            parts.append(X_cl_rich if rich else X_cl_base)
        return np.concatenate(parts, axis=1)

    configs = {
        "M0_cyclic_only":        _make_X(False, False),
        "M1_AR_baseline":        _make_X(True,  False),
        "M2_cluster_AR":         _make_X(True,  True,  rich=False),
        "M3_rich_cluster_AR":    _make_X(True,  True,  rich=True),
    }

    results = {}
    model_preds = {}
    for name, X in configs.items():
        res = _lgb_fit_eval(
            X[train_idx], X[val_idx], X[test_idx],
            Y_log[train_idx], Y_log[val_idx], Y_raw[test_idx], log1p_inv,
            label=name,
        )
        results[name] = res
        model_preds[name] = res.get("pred_native", np.zeros(len(test_idx)))

    y_te = Y_raw[test_idx]

    # Bootstrap CIs for ΔR² (M2 − M1, M3 − M1)
    boot_m2 = compute_block_bootstrap_ci(
        y_te, model_preds["M1_AR_baseline"], model_preds["M2_cluster_AR"], n_boot=300
    )
    boot_m3 = compute_block_bootstrap_ci(
        y_te, model_preds["M1_AR_baseline"], model_preds["M3_rich_cluster_AR"], n_boot=300
    )
    print(f"\n  ΔR²(M2−M1): {boot_m2['delta_r2']:.4f}  95%CI [{boot_m2['ci_lo']:.4f}, {boot_m2['ci_hi']:.4f}]  p={boot_m2['p_value']:.3f}")
    print(f"  ΔR²(M3−M1): {boot_m3['delta_r2']:.4f}  95%CI [{boot_m3['ci_lo']:.4f}, {boot_m3['ci_hi']:.4f}]  p={boot_m3['p_value']:.3f}")

    # Permutation test for M2: shuffle cluster features row-wise across hours
    print("\n  Running permutation test (50 shuffles)...")
    rng = np.random.default_rng(RANDOM_STATE)
    null_deltas = []
    for _ in range(50):
        perm = rng.permutation(len(hour_idx))
        X_perm = np.concatenate([scalar_cyc, ar_feats, X_cl_base[perm]], axis=1)
        res_p = _lgb_fit_eval(
            X_perm[train_idx], X_perm[val_idx], X_perm[test_idx],
            Y_log[train_idx], Y_log[val_idx], y_te, log1p_inv, label="",
        )
        if "pred_native" in res_p:
            null_deltas.append(_r2(y_te, res_p["pred_native"]) - _r2(y_te, model_preds["M1_AR_baseline"]))
    null_deltas = np.array(null_deltas)
    perm_p_val = float((null_deltas >= boot_m2["delta_r2"]).mean()) if len(null_deltas) else float("nan")
    print(f"  Permutation p-value (M2 vs M1): {perm_p_val:.3f}  null ΔR² mean={null_deltas.mean():.4f}")

    # Bar chart
    bar_data = {
        n: {"r2": r.get("native_test_r2", 0.0), "r2_std": 0.0}
        for n, r in results.items()
    }
    plot_bar_comparison(
        bar_data,
        title="Nested AR Models — Marginal Weather Contribution\nNative-scale test R² (LightGBM)",
        save_path=fig_dir / "nn_marginal_weather.png",
    )

    # Delta R² plot with bootstrap CIs
    fig, ax = plt.subplots(figsize=(6, 4))
    for boot, lab, col in [
        (boot_m2, "M2−M1 (cluster AR)", "#2196F3"),
        (boot_m3, "M3−M1 (rich cluster AR)", "#FF5722"),
    ]:
        d, lo, hi = boot["delta_r2"], boot["ci_lo"], boot["ci_hi"]
        ax.barh(lab, d, xerr=[[d - lo], [hi - d]], capsize=6,
                color=col, alpha=0.8, edgecolor="black", linewidth=0.5)
        ax.text(d + 0.005, 0.2 if "M2" in lab else -0.2, f"{d:.4f}", va="center", fontsize=9)
    ax.axvline(0, color="black", linewidth=0.8, linestyle="--")
    ax.set_xlabel("ΔR² (native scale)")
    ax.set_title("Weather Features vs AR Baseline\n95% Block Bootstrap CI")
    fig.tight_layout()
    fig.savefig(fig_dir / "nn_delta_r2_bootstrap.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {fig_dir / 'nn_delta_r2_bootstrap.png'}")

    # Save tables
    rows = []
    for n, r in results.items():
        rows.append({
            "config": n,
            "train_r2": r.get("train_r2", float("nan")),
            "val_r2": r.get("val_r2", float("nan")),
            "native_test_r2": r.get("native_test_r2", float("nan")),
            "tail_test_r2": r.get("tail_test_r2", float("nan")),
        })
    rows.append({
        "config": "delta_M2_M1_ci_lo", "train_r2": float("nan"),
        "val_r2": float("nan"),
        "native_test_r2": boot_m2["ci_lo"], "tail_test_r2": float("nan"),
    })
    rows.append({
        "config": "delta_M2_M1_ci_hi", "train_r2": float("nan"),
        "val_r2": float("nan"),
        "native_test_r2": boot_m2["ci_hi"], "tail_test_r2": float("nan"),
    })
    pd.DataFrame(rows).to_csv(tables_dir / "nn_marginal_weather.csv", index=False)
    pd.DataFrame({
        "metric": ["delta_M2_M1", "ci_lo_M2", "ci_hi_M2", "p_boot_M2",
                   "delta_M3_M1", "ci_lo_M3", "ci_hi_M3", "p_boot_M3",
                   "perm_p_M2", "null_delta_mean"],
        "value": [boot_m2["delta_r2"], boot_m2["ci_lo"], boot_m2["ci_hi"], boot_m2["p_value"],
                  boot_m3["delta_r2"], boot_m3["ci_lo"], boot_m3["ci_hi"], boot_m3["p_value"],
                  perm_p_val, float(null_deltas.mean()) if len(null_deltas) else float("nan")],
    }).to_csv(tables_dir / "nn_bootstrap_ci.csv", index=False)

    results["boot_m2"] = boot_m2
    results["boot_m3"] = boot_m3
    results["perm_p"] = perm_p_val
    return results


def run_exp_fpc(months, fig_dir, tables_dir, device, df=None):
    """Step 2: FPC/EOF basis regression for principled spatial dimensionality reduction.

    Projects each forecast error channel onto K leading EOF modes computed from
    training hours (Ramsay & Silverman 2005; Hannachi et al. 2007). Compares
    Ridge and LGB across K ∈ {5, 10, 20, 40}, and visualizes the spatial mode
    patterns as Texas maps.

    Parameters
    ----------
    months     : list of (year, month)
    fig_dir    : Path
    tables_dir : Path
    device     : torch.device (unused; Ridge and LGB run on CPU)
    df         : pre-loaded DataFrame (optional)

    Returns
    -------
    dict with results per (model, K) configuration
    """
    print("\n" + "=" * 70)
    print("EXPERIMENT: FPC/EOF BASIS REGRESSION (Step 2)")
    print("=" * 70)

    try:
        import lightgbm as lgb  # noqa: F401
        from sklearn.linear_model import Ridge, RidgeCV
    except ImportError:
        print("  [skip] lightgbm or sklearn not available")
        return {}

    if df is None:
        df = load_pixel_data_for_nn(months)

    log1p_fwd, log1p_inv = y_transform_log1p()

    grid_np, scalar_cyc, Y_raw, pixel_coords, hour_idx, loaded_fields = load_multi_field_data(
        months, spatial_fields=ERROR_FIELDS, df=df, scalar_mode="cyclic",
    )
    Y_log = log1p_fwd(np.clip(Y_raw, 0, None)).astype(np.float32)
    train_idx, val_idx, test_idx = make_chunk_splits(hour_idx)

    ar_feats, _ = build_ar_features(Y_raw, hour_idx, lags=(1, 24))
    ar_mean = ar_feats[train_idx].mean(0, keepdims=True)
    ar_std = ar_feats[train_idx].std(0, keepdims=True) + 1e-8
    ar_feats = (ar_feats - ar_mean) / ar_std

    T, C, H, W = grid_np.shape
    y_te = Y_raw[test_idx]

    # Reference: cluster + AR (best from Round 2)
    X_cl, _ = load_cluster_features_v2(months, hour_idx, include_lmp_std=False)
    X_ref = np.concatenate([X_cl, ar_feats, scalar_cyc], axis=1)
    ref_res = _lgb_fit_eval(
        X_ref[train_idx], X_ref[val_idx], X_ref[test_idx],
        Y_log[train_idx], Y_log[val_idx], y_te, log1p_inv, label="cluster_AR_ref",
    )

    K_values = [5, 10, 20, 40]
    rows = []
    best_eofs = None  # Save EOFs for K=20 visualization

    for K in K_values:
        print(f"\n  K={K} modes per channel ({K * C} total FPC features)...")
        X_fpc, comps, var_ratios = build_fpc_features(grid_np, train_idx, n_components_per_channel=K)
        if K == 20:
            best_eofs = (comps, var_ratios)

        cum_var = [float(np.cumsum(v)[-1]) for v in var_ratios]
        print(f"    Cumulative variance explained per channel: {[f'{v:.2f}' for v in cum_var]}")

        X_full = np.concatenate([X_fpc, ar_feats, scalar_cyc], axis=1)

        # Ridge regression (interpretable, fast)
        sc_mean_r = X_full[train_idx].mean(0)
        sc_std_r = X_full[train_idx].std(0) + 1e-8
        X_n = (X_full - sc_mean_r) / sc_std_r
        ridge = RidgeCV(alphas=[0.01, 0.1, 1, 10, 100]).fit(X_n[train_idx], Y_log[train_idx])
        pred_ridge_te = log1p_inv(ridge.predict(X_n[test_idx]))
        ridge_r2 = float(_r2(y_te, pred_ridge_te))
        print(f"    Ridge native R² = {ridge_r2:.4f}")

        # LightGBM
        lgb_res = _lgb_fit_eval(
            X_full[train_idx], X_full[val_idx], X_full[test_idx],
            Y_log[train_idx], Y_log[val_idx], y_te, log1p_inv, label=f"LGB_K{K}",
        )
        rows.append({
            "K": K, "model": "ridge",
            "native_r2": ridge_r2, "train_r2": float("nan"), "val_r2": float("nan"),
            "tail_r2": float("nan"), "cumvar_mean": float(np.mean(cum_var)),
        })
        rows.append({
            "K": K, "model": "lgb",
            "native_r2": lgb_res.get("native_test_r2", float("nan")),
            "train_r2": lgb_res.get("train_r2", float("nan")),
            "val_r2": lgb_res.get("val_r2", float("nan")),
            "tail_r2": lgb_res.get("tail_test_r2", float("nan")),
            "cumvar_mean": float(np.mean(cum_var)),
        })

    rows.append({
        "K": -1, "model": "cluster_AR_reference",
        "native_r2": ref_res.get("native_test_r2", float("nan")),
        "train_r2": ref_res.get("train_r2", float("nan")),
        "val_r2": ref_res.get("val_r2", float("nan")),
        "tail_r2": ref_res.get("tail_test_r2", float("nan")),
        "cumvar_mean": float("nan"),
    })
    pd.DataFrame(rows).to_csv(tables_dir / "nn_fpc_comparison.csv", index=False)

    # Bar chart: LGB native R² vs K
    lgb_rows = [r for r in rows if r["model"] == "lgb"]
    bar_data = {
        (f"FPC K={r['K']}" if r["K"] > 0 else "Cluster AR ref"):
        {"r2": r["native_r2"], "r2_std": 0.0}
        for r in lgb_rows
    }
    bar_data["Cluster AR ref"] = {"r2": ref_res.get("native_test_r2", 0.0), "r2_std": 0.0}
    plot_bar_comparison(
        bar_data,
        title="FPC/EOF Basis — LGB Native R² vs Number of Modes\n(+ AR lags, cyclic time)",
        save_path=fig_dir / "nn_fpc_comparison.png",
        baselines={"Cluster AR": ref_res.get("native_test_r2", 0.0)},
    )

    # EOF visualization: top 3 modes × 4 channels
    if best_eofs is not None:
        comps, var_ratios = best_eofs
        # Reconstruct grid lat/lon from pixel_coords
        lat_idx, lon_idx, H_g, W_g = _coarse_grid_indices(pixel_coords, resolution=0.25)
        lats = np.unique(np.round(pixel_coords[:, 0] / 0.25) * 0.25)
        lons = np.unique(np.round(pixel_coords[:, 1] / 0.25) * 0.25)

        n_show = min(3, comps[0].shape[0])
        fig, axes = plt.subplots(C, n_show, figsize=(4 * n_show, 3.5 * C))
        if C == 1:
            axes = axes[np.newaxis, :]
        for c, field_name in enumerate(loaded_fields[:C]):
            for k in range(n_show):
                ax = axes[c, k]
                eof_map = comps[c][k]  # (H, W) on the coarse 0.25° grid
                # Use lats/lons if they match H/W, else just use imshow
                if eof_map.shape == (H, W):
                    extent = [lons.min(), lons.max(), lats.min(), lats.max()]
                    img = ax.imshow(
                        eof_map, origin="lower", extent=extent,
                        cmap="RdBu_r", aspect="auto",
                        vmax=np.abs(eof_map).max(), vmin=-np.abs(eof_map).max(),
                    )
                    plt.colorbar(img, ax=ax, fraction=0.046, pad=0.04)
                ax.set_title(
                    f"{ERROR_FIELD_LABELS.get(field_name, field_name)}\n"
                    f"EOF {k + 1}  ({var_ratios[c][k] * 100:.1f}% var)",
                    fontsize=8,
                )
                ax.set_xlabel("Longitude")
                ax.set_ylabel("Latitude")
        fig.suptitle("Leading EOF Modes per Error Channel (K=20, training hours only)", fontsize=10)
        fig.tight_layout()
        fig.savefig(fig_dir / "nn_fpc_eofs.png", dpi=150, bbox_inches="tight")
        plt.close(fig)
        print(f"  Saved: {fig_dir / 'nn_fpc_eofs.png'}")

    return {"rows": rows, "ref": ref_res}


def run_exp_capacity_scalars(months, fig_dir, tables_dir, device, df=None):
    """Step 7: Physics-motivated capacity-weighted scalar features.

    Tests whether a small set of physically grounded scalars — Σ_s cap(s)·error(s)
    summed system-wide and per geographic quadrant — can match or approach
    the cluster-feature baseline (Round 2b best: native R² ≈ 0.42).

    Configs
    -------
    (a) cap_scalars + AR + cyclic — 20 scalars, Ridge + LGB
    (b) cap_scalars + cluster + AR + cyclic — combined (both representations)
    (c) cluster + AR + cyclic — Round-2 reference

    Parameters
    ----------
    months     : list of (year, month)
    fig_dir    : Path
    tables_dir : Path
    device     : torch.device (unused)
    df         : pre-loaded DataFrame (optional)

    Returns
    -------
    dict with results per config
    """
    print("\n" + "=" * 70)
    print("EXPERIMENT: CAPACITY-WEIGHTED SCALAR FEATURES (Step 7)")
    print("=" * 70)

    try:
        from sklearn.linear_model import RidgeCV
        import lightgbm  # noqa: F401
    except ImportError:
        print("  [skip] lightgbm or sklearn not available")
        return {}

    if df is None:
        df = load_pixel_data_for_nn(months)

    log1p_fwd, log1p_inv = y_transform_log1p()

    _, scalar_cyc, Y_raw, _, hour_idx, _ = load_multi_field_data(
        months, spatial_fields=ERROR_FIELDS, df=df, scalar_mode="cyclic",
    )
    Y_log = log1p_fwd(np.clip(Y_raw, 0, None)).astype(np.float32)
    train_idx, val_idx, test_idx = make_chunk_splits(hour_idx)

    ar_feats, _ = build_ar_features(Y_raw, hour_idx, lags=(1, 24))
    ar_mean = ar_feats[train_idx].mean(0, keepdims=True)
    ar_std = ar_feats[train_idx].std(0, keepdims=True) + 1e-8
    ar_feats = (ar_feats - ar_mean) / ar_std

    X_cap, cap_names = build_capacity_scalar_features(df, hour_idx)
    print(f"  Capacity scalar features: {len(cap_names)} columns")

    X_cl, _ = load_cluster_features_v2(months, hour_idx, include_lmp_std=False)

    y_te = Y_raw[test_idx]
    results = {}

    def _feats(*parts):
        return np.concatenate(list(parts), axis=1)

    # (c) cluster + AR (reference)
    X_c = _feats(X_cl, ar_feats, scalar_cyc)
    res_c = _lgb_fit_eval(
        X_c[train_idx], X_c[val_idx], X_c[test_idx],
        Y_log[train_idx], Y_log[val_idx], y_te, log1p_inv,
        label="(c) cluster_AR",
    )
    results["(c) cluster_AR"] = res_c

    # (a) capacity scalars + AR
    X_a = _feats(X_cap, ar_feats, scalar_cyc)
    res_a_lgb = _lgb_fit_eval(
        X_a[train_idx], X_a[val_idx], X_a[test_idx],
        Y_log[train_idx], Y_log[val_idx], y_te, log1p_inv,
        label="(a) cap_AR_LGB",
    )
    results["(a) cap_AR_LGB"] = res_a_lgb

    # Also Ridge for (a) — interpretable
    sc_mean_r = X_a[train_idx].mean(0)
    sc_std_r = X_a[train_idx].std(0) + 1e-8
    X_an = (X_a - sc_mean_r) / sc_std_r
    ridge = RidgeCV(alphas=[0.01, 0.1, 1, 10, 100]).fit(X_an[train_idx], Y_log[train_idx])
    pred_ridge = log1p_inv(ridge.predict(X_an[test_idx]))
    ridge_r2 = float(_r2(y_te, pred_ridge))
    print(f"  [(a) cap_AR_Ridge] native R² = {ridge_r2:.4f}")
    results["(a) cap_AR_Ridge"] = {"native_test_r2": ridge_r2}

    # (b) capacity scalars + cluster + AR
    X_b = _feats(X_cap, X_cl, ar_feats, scalar_cyc)
    res_b = _lgb_fit_eval(
        X_b[train_idx], X_b[val_idx], X_b[test_idx],
        Y_log[train_idx], Y_log[val_idx], y_te, log1p_inv,
        label="(b) cap_cluster_AR",
    )
    results["(b) cap_cluster_AR"] = res_b

    bar_data = {
        n: {"r2": r.get("native_test_r2", 0.0), "r2_std": 0.0}
        for n, r in results.items()
    }
    plot_bar_comparison(
        bar_data,
        title="Capacity-Weighted Scalar Features vs Cluster Features\nNative-scale test R²",
        save_path=fig_dir / "nn_capacity_scalars.png",
        baselines={"Cluster AR (ref)": res_c.get("native_test_r2", 0.0)},
    )

    rows = [
        {"config": n, "native_test_r2": r.get("native_test_r2", float("nan")),
         "train_r2": r.get("train_r2", float("nan")),
         "val_r2": r.get("val_r2", float("nan")),
         "tail_test_r2": r.get("tail_test_r2", float("nan"))}
        for n, r in results.items()
    ]
    pd.DataFrame(rows).to_csv(tables_dir / "nn_capacity_scalars.csv", index=False)
    return results


def run_exp_seed_sweep(months, fig_dir, tables_dir, device, df=None, n_seeds=10):
    """Step 5: Monte-Carlo seed sweep for R² variance quantification.

    Runs the best configuration (cluster features + lag-1h + lag-24h, LGB) on
    n_seeds different chunk-split random seeds. Reports the distribution of
    native test R² to quantify sampling variance from the temporal splitting.

    Parameters
    ----------
    months  : list of (year, month)
    fig_dir : Path
    tables_dir : Path
    device  : torch.device (unused)
    df      : pre-loaded DataFrame (optional)
    n_seeds : int — number of random seeds (10 is fast; plan recommends 20)

    Returns
    -------
    dict with mean_r2, std_r2, per_seed results
    """
    print("\n" + "=" * 70)
    print(f"EXPERIMENT: SEED SWEEP ({n_seeds} splits) — R² VARIANCE (Step 5)")
    print("=" * 70)

    try:
        import lightgbm  # noqa: F401
    except ImportError:
        print("  [skip] lightgbm not installed")
        return {}

    if df is None:
        df = load_pixel_data_for_nn(months)

    log1p_fwd, log1p_inv = y_transform_log1p()

    _, scalar_cyc, Y_raw, _, hour_idx, _ = load_multi_field_data(
        months, spatial_fields=ERROR_FIELDS, df=df, scalar_mode="cyclic",
    )
    Y_log = log1p_fwd(np.clip(Y_raw, 0, None)).astype(np.float32)

    ar_feats, _ = build_ar_features(Y_raw, hour_idx, lags=(1, 24))
    X_cl, _ = load_cluster_features_v2(months, hour_idx, include_lmp_std=False)

    seed_results = []
    for s in range(n_seeds):
        train_idx, val_idx, test_idx = make_chunk_splits(hour_idx, seed=s)

        ar_mean = ar_feats[train_idx].mean(0, keepdims=True)
        ar_std = ar_feats[train_idx].std(0, keepdims=True) + 1e-8
        ar_s = (ar_feats - ar_mean) / ar_std

        X = np.concatenate([X_cl, ar_s, scalar_cyc], axis=1)
        y_te = Y_raw[test_idx]
        res = _lgb_fit_eval(
            X[train_idx], X[val_idx], X[test_idx],
            Y_log[train_idx], Y_log[val_idx], y_te, log1p_inv,
            label=f"seed={s}",
        )
        seed_results.append({"seed": s, **{k: v for k, v in res.items() if k not in ("model", "pred_native")}})

    df_seeds = pd.DataFrame(seed_results)
    df_seeds.to_csv(tables_dir / "nn_seed_sweep.csv", index=False)

    r2s = df_seeds["native_test_r2"].values
    print(f"\n  Native R² across {n_seeds} seeds: mean={r2s.mean():.4f}  std={r2s.std():.4f}  min={r2s.min():.4f}  max={r2s.max():.4f}")

    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    axes[0].hist(r2s, bins=min(10, n_seeds), color="#2196F3", edgecolor="black", alpha=0.8)
    axes[0].axvline(r2s.mean(), color="red", linewidth=1.5, label=f"mean={r2s.mean():.3f}")
    axes[0].axvline(r2s.mean() - r2s.std(), color="orange", linestyle="--", linewidth=1, label=f"±1 sd")
    axes[0].axvline(r2s.mean() + r2s.std(), color="orange", linestyle="--", linewidth=1)
    axes[0].legend(fontsize=8)
    axes[0].set_xlabel("Native Test R²")
    axes[0].set_ylabel("Count")
    axes[0].set_title(f"R² Distribution ({n_seeds} seeds)\nCluster + AR + Cyclic")

    axes[1].plot(range(n_seeds), r2s, "o-", color="#2196F3", markersize=5)
    axes[1].axhline(r2s.mean(), color="red", linewidth=1.5, linestyle="--")
    axes[1].fill_between(range(n_seeds), r2s.mean() - r2s.std(), r2s.mean() + r2s.std(),
                          alpha=0.2, color="red")
    axes[1].set_xlabel("Seed")
    axes[1].set_ylabel("Native Test R²")
    axes[1].set_title("R² per Chunk-Split Seed")

    fig.suptitle("Sampling Variance of Native Test R²\n(LGB: cluster + AR, alt-month 2025)", fontsize=10)
    fig.tight_layout()
    fig.savefig(fig_dir / "nn_seed_sweep.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {fig_dir / 'nn_seed_sweep.png'}")

    return {"mean_r2": float(r2s.mean()), "std_r2": float(r2s.std()), "seed_results": seed_results}


def run_exp_full_year(fig_dir, tables_dir, device, df=None):
    """Step 9: Re-run best model on all 12 months of 2025.

    Compares native R² from the 6-month (alt-month) training sample to the
    full-year sample to quantify the effect of data expansion on model
    performance and confidence-interval width.

    Parameters
    ----------
    fig_dir    : Path
    tables_dir : Path
    device     : torch.device (unused)
    df         : pre-loaded DataFrame for all 12 months (optional)

    Returns
    -------
    dict with 6-month and 12-month results for M1 and M2
    """
    print("\n" + "=" * 70)
    print("EXPERIMENT: FULL-YEAR DATA EXPANSION (Step 9)")
    print("=" * 70)

    try:
        import lightgbm  # noqa: F401
    except ImportError:
        print("  [skip] lightgbm not installed")
        return {}

    all_months = [(2025, m) for m in range(1, 13)]

    if df is None:
        print("  Loading all 12 months...")
        df = load_pixel_data_for_nn(all_months)

    log1p_fwd, log1p_inv = y_transform_log1p()

    _, scalar_cyc, Y_raw, _, hour_idx, _ = load_multi_field_data(
        all_months, spatial_fields=ERROR_FIELDS, df=df, scalar_mode="cyclic",
    )
    Y_log = log1p_fwd(np.clip(Y_raw, 0, None)).astype(np.float32)
    train_idx, val_idx, test_idx = make_chunk_splits(hour_idx)

    print(f"  Full year: T={len(hour_idx)} hours  train={len(train_idx)} val={len(val_idx)} test={len(test_idx)}")

    ar_feats, _ = build_ar_features(Y_raw, hour_idx, lags=(1, 24))
    ar_mean = ar_feats[train_idx].mean(0, keepdims=True)
    ar_std = ar_feats[train_idx].std(0, keepdims=True) + 1e-8
    ar_feats = (ar_feats - ar_mean) / ar_std

    X_cl, _ = load_cluster_features_v2(all_months, hour_idx, include_lmp_std=False)

    y_te = Y_raw[test_idx]
    results = {}

    X_m1 = np.concatenate([scalar_cyc, ar_feats], axis=1)
    results["12mo_M1_AR"] = _lgb_fit_eval(
        X_m1[train_idx], X_m1[val_idx], X_m1[test_idx],
        Y_log[train_idx], Y_log[val_idx], y_te, log1p_inv, label="12mo_M1",
    )

    X_m2 = np.concatenate([X_cl, ar_feats, scalar_cyc], axis=1)
    results["12mo_M2_cluster_AR"] = _lgb_fit_eval(
        X_m2[train_idx], X_m2[val_idx], X_m2[test_idx],
        Y_log[train_idx], Y_log[val_idx], y_te, log1p_inv, label="12mo_M2",
    )

    # Add 6-month reference values from existing tables if available
    try:
        ref = pd.read_csv(tables_dir / "nn_marginal_weather.csv")
        m1_6mo = ref.loc[ref["config"] == "M1_AR_baseline", "native_test_r2"].values
        m2_6mo = ref.loc[ref["config"] == "M2_cluster_AR", "native_test_r2"].values
        if len(m1_6mo): results["6mo_M1_AR"] = {"native_test_r2": float(m1_6mo[0])}
        if len(m2_6mo): results["6mo_M2_cluster_AR"] = {"native_test_r2": float(m2_6mo[0])}
    except Exception:
        pass

    bar_data = {
        n: {"r2": r.get("native_test_r2", 0.0), "r2_std": 0.0}
        for n, r in results.items()
    }
    plot_bar_comparison(
        bar_data,
        title="Full-Year (12 months) vs Alt-Month (6 months)\nNative-scale test R² — LGB Cluster + AR",
        save_path=fig_dir / "nn_full_year.png",
    )
    rows = [
        {"config": n, "native_test_r2": r.get("native_test_r2", float("nan")),
         "train_r2": r.get("train_r2", float("nan")),
         "val_r2": r.get("val_r2", float("nan")),
         "tail_r2": r.get("tail_test_r2", float("nan"))}
        for n, r in results.items()
    ]
    pd.DataFrame(rows).to_csv(tables_dir / "nn_full_year.csv", index=False)
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
    R3_EXPS = ["marginal", "fpc", "cap_scalars", "seed_sweep", "full_year"]
    if experiments is None:
        experiments = ["arch", "channels", "regime", "saliency", "overfitting",
                       "baseline", "infra", "cluster", "nodal",
                       "ar", "gbm", "gru"] + R3_EXPS

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
    if any(e in experiments for e in ["arch", "channels", "saliency",
                                       "baseline", "infra", "cluster", "nodal",
                                       "ar", "gbm", "gru",
                                       "marginal", "fpc", "cap_scalars", "seed_sweep"]):
        print("\n  Pre-loading pixel data...")
        df_shared = load_pixel_data_for_nn(months)

    all_results = {}

    if "arch" in experiments:
        all_results["arch"] = run_exp_arch(
            months, fig_dir, tables_dir, device, df=df_shared
        )
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
    if "baseline" in experiments:
        all_results["baseline"] = run_exp_baseline(
            months, fig_dir, tables_dir, device, df=df_shared
        )
    if "infra" in experiments:
        all_results["infra"] = run_exp_infra(
            months, fig_dir, tables_dir, device, df=df_shared
        )
    if "cluster" in experiments:
        all_results["cluster"] = run_exp_cluster(
            months, fig_dir, tables_dir, device, df=df_shared
        )
    if "nodal" in experiments:
        all_results["nodal"] = run_exp_nodal(
            months, fig_dir, tables_dir, device, df=df_shared
        )
    if "ar" in experiments:
        all_results["ar"] = run_exp_ar(
            months, fig_dir, tables_dir, device, df=df_shared
        )
    if "gbm" in experiments:
        all_results["gbm"] = run_exp_gbm(
            months, fig_dir, tables_dir, device, df=df_shared
        )
    if "gru" in experiments:
        all_results["gru"] = run_exp_gru(
            months, fig_dir, tables_dir, device, df=df_shared
        )
    if "marginal" in experiments:
        all_results["marginal"] = run_exp_marginal_weather(
            months, fig_dir, tables_dir, device, df=df_shared
        )
    if "fpc" in experiments:
        all_results["fpc"] = run_exp_fpc(
            months, fig_dir, tables_dir, device, df=df_shared
        )
    if "cap_scalars" in experiments:
        all_results["cap_scalars"] = run_exp_capacity_scalars(
            months, fig_dir, tables_dir, device, df=df_shared
        )
    if "seed_sweep" in experiments:
        all_results["seed_sweep"] = run_exp_seed_sweep(
            months, fig_dir, tables_dir, device, df=df_shared
        )
    if "full_year" in experiments:
        all_results["full_year"] = run_exp_full_year(fig_dir, tables_dir, device)

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
        description="MLP/CNN/FNO analysis for ERCOT congestion cost prediction"
    )
    parser.add_argument(
        "--exp",
        nargs="+",
        default=["all"],
        choices=["all", "arch", "channels", "regime", "saliency", "overfitting",
                 "baseline", "infra", "cluster", "nodal", "ar", "gbm", "gru",
                 "marginal", "fpc", "cap_scalars", "seed_sweep", "full_year"],
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
