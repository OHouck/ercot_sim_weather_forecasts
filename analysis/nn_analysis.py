"""
Neural Architecture Expansion for ERCOT Curtailment Prediction (Run 5).

Continues from Run 4B (pixel_level_functional_analysis.py --run4b), which
showed MLP R²=0.901, FNO R²=0.736, CNN R²=0.488 on a 4x-downsampled 2-channel
grid. This script tests improved architectures and runs five systematic
experiments:

  A: Architecture comparison — MLP, FNO, U-Net, Conv-FNO, Attention-MLP
  B: Resolution ablation — 0.50°, 0.25°, 0.10° grids
  C: Channel ablation — 2-channel HRRR, 2-channel GFS, 4-channel all
  D: Regime-stratified evaluation and training
  E: Gradient saliency maps vs PLS β(s)

See analysis/functional_analysis_log.md §Run 5 for full design rationale and
literature references.

Usage:
    uv run python -m analysis.nn_analysis --exp all
    uv run python -m analysis.nn_analysis --exp arch
    uv run python -m analysis.nn_analysis --exp resolution
    uv run python -m analysis.nn_analysis --exp channels
    uv run python -m analysis.nn_analysis --exp regime
    uv run python -m analysis.nn_analysis --exp saliency
"""

import argparse
import copy
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.io.shapereader as shpreader
from sklearn.model_selection import KFold

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories
from analysis.pixel_level_functional_analysis import (
    load_pixel_data,
    load_pixel_data_with_regimes,
    prepare_functional_data,
    prepare_grid_data_coarse,
    _build_era5_grid_index,
    _select_device,
)

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# ── Configuration ─────────────────────────────────────────────────────────────

DEPVAR = "total_curtailment_mw"
ALL_FIELDS = ["wspd_error_1h", "temp_error_1h", "wspd_error_0h", "temp_error_0h"]
# Prior baselines for comparison charts
RUN4B_MLP_R2 = 0.901
RUN4B_FNO_R2 = 0.736
RUN1_RIDGE_R2 = 0.604
RUN5_MLP_R2 = 0.9309  # confirmed from run5c: 0.9309 ± 0.0129
DEFAULT_MONTHS = [(2025, m) for m in [1, 3, 5, 7, 9, 11]]
N_CV_FOLDS = 5
RANDOM_STATE = 42
N_EPOCHS = 150
PATIENCE = 20
# FNO/Conv-FNO run on CPU (rfft2 unsupported on MPS). Even at 50 epochs the
# 4-layer model exceeds 4h/fold. Use Run 4B config (2 layers, h=32) + 20 epochs
# so each fold finishes in ~15-20 min.
FNO_N_EPOCHS = 20
FNO_PATIENCE = 5
BATCH_SIZE = 32
LR = 5e-4
WEIGHT_DECAY = 1e-4
# Minimum samples per regime to train a regime-specific model
MIN_REGIME_SAMPLES = 50
# Minimum folds × samples threshold for full N_CV_FOLDS; below this use 3-fold
MIN_SAMPLES_FULL_CV = N_CV_FOLDS * 20


# ── Data preparation ───────────────────────────────────────────────────────────

def load_multi_field_data(months, fields=None, df=None):
    """Load pixel-hourly data and build multi-channel (T, N_pixels) arrays.

    Parameters
    ----------
    months : list of (year, month) tuples
    fields : list of error field names; defaults to ALL_FIELDS
    df     : pre-loaded DataFrame to avoid re-reading parquets (optional)

    Returns
    -------
    X_dict : dict {field: ndarray (T, N_pixels)} — standardized
    Y      : ndarray (T,) — total curtailment MW
    pixel_coords : ndarray (N_pixels, 2) — (lat, lon)
    """
    if fields is None:
        fields = ALL_FIELDS
    if df is None:
        df = load_pixel_data(months)

    X_dict = {}
    Y_ref = None
    pixel_coords_ref = None
    for field in fields:
        try:
            X, Y, pc, _, _ = prepare_functional_data(df, error_field=field)
            X_dict[field] = X
            if Y_ref is None:
                Y_ref, pixel_coords_ref = Y, pc
        except Exception as exc:
            print(f"  Skipping {field}: {exc}")

    if not X_dict:
        raise RuntimeError("No fields loaded — check data availability.")

    T_min = min(v.shape[0] for v in X_dict.values())
    X_dict = {k: v[:T_min] for k, v in X_dict.items()}
    Y_ref = Y_ref[:T_min]
    return X_dict, Y_ref, pixel_coords_ref


def _coarse_grid_indices(pixel_coords, resolution):
    """Compute (lat_idx, lon_idx) mapping pixels onto a coarse regular grid.

    Uses the same rounding logic as prepare_grid_data_coarse so the mapping
    is consistent wherever we need to scatter or gather from the grid.

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
    X_dict : dict {field: ndarray (T, N_pixels)}
    pixel_coords : ndarray (N_pixels, 2)
    resolution : float — grid spacing in degrees; ≤0.1 uses native ERA5 grid

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
        )  # (T, C, N_pixels)
        grid[:, :, lat_idx, lon_idx] = X_stack
        return grid
    grid, _, _ = prepare_grid_data_coarse(X_dict, pixel_coords, target_res=resolution)
    return grid


# ── Architecture definitions ───────────────────────────────────────────────────

def _build_mlp(in_dim, hidden=512, dropout=0.3):
    """MLP: Linear → LayerNorm → GELU → Dropout, repeated twice, then head.

    Parameters
    ----------
    in_dim  : int — flattened input dimension (C * H * W)
    hidden  : int — first hidden layer width
    dropout : float — dropout probability after each hidden block

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


def _make_mlp_cls(in_dim):
    """Return an MLP model class (callable → nn.Module) for the given input dim.

    Wraps _build_mlp in a class so it can be passed to cross_val like any
    other architecture factory.

    Parameters
    ----------
    in_dim : int — flattened C * H * W

    Returns
    -------
    class — instantiates to an nn.Module
    """
    import torch.nn as nn

    class MLPScalar(nn.Module):
        def __init__(self):
            super().__init__()
            self.net = _build_mlp(in_dim)

        def forward(self, x):
            return self.net(x.reshape(x.shape[0], -1)).squeeze(-1)

    return MLPScalar


def _build_fno(C, H, W, modes=16, hidden=64, n_layers=4):
    """FNO-to-scalar: FNO trunk + global avg pool + MLP head.

    Follows NeuralOperator 2.0 guide (arXiv:2512.01421):
      - domain_padding=0.1 for non-periodic ERCOT domain
      - instance_norm, linear skip, soft-gating ChannelMLP

    Parameters
    ----------
    C : int — input channels
    H, W : int — spatial dimensions
    modes : int — Fourier modes per dimension (auto-clamped to Nyquist)
    hidden : int — FNO channel width
    n_layers : int — number of spectral layers

    Returns
    -------
    class — instantiates to an nn.Module
    """
    import torch.nn as nn
    from neuralop.models import FNO

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
                nn.Linear(hidden, 64), nn.GELU(), nn.Linear(64, 1)
            )

        def forward(self, x):
            return self.head(self.fno(x).mean(dim=(-2, -1))).squeeze(-1)

    return FNOScalar


def _build_unet_scalar(C, hidden=32):
    """U-Net encoder-decoder + global avg pool + MLP head.

    Multi-scale spatial features via skip connections between encoder and
    decoder. Adapted for spatial-to-scalar regression (Ronneberger et al. 2015).

    Parameters
    ----------
    C : int — input channels
    hidden : int — base channel count (doubles per encoder level)

    Returns
    -------
    class — instantiates to an nn.Module
    """
    import torch
    import torch.nn as nn

    class UNetScalar(nn.Module):
        def __init__(self):
            super().__init__()
            h = hidden
            self.enc1 = nn.Sequential(
                nn.Conv2d(C, h, 3, padding=1), nn.GELU(),
                nn.Conv2d(h, h, 3, padding=1), nn.GELU(),
            )
            self.down1 = nn.Conv2d(h, h * 2, 3, stride=2, padding=1)
            self.enc2 = nn.Sequential(
                nn.Conv2d(h * 2, h * 2, 3, padding=1), nn.GELU(),
                nn.Conv2d(h * 2, h * 2, 3, padding=1), nn.GELU(),
            )
            self.down2 = nn.Conv2d(h * 2, h * 4, 3, stride=2, padding=1)
            self.bottleneck = nn.Sequential(
                nn.Conv2d(h * 4, h * 4, 3, padding=1), nn.GELU(),
            )
            self.up2 = nn.ConvTranspose2d(h * 4, h * 2, 2, stride=2)
            self.dec2 = nn.Sequential(
                nn.Conv2d(h * 4, h * 2, 3, padding=1), nn.GELU(),
            )
            self.up1 = nn.ConvTranspose2d(h * 2, h, 2, stride=2)
            self.dec1 = nn.Sequential(
                nn.Conv2d(h * 2, h, 3, padding=1), nn.GELU(),
            )
            self.head = nn.Sequential(
                nn.Linear(h, 32), nn.GELU(), nn.Linear(32, 1)
            )

        def forward(self, x):
            e1 = self.enc1(x)
            e2 = self.enc2(self.down1(e1))
            b = self.bottleneck(self.down2(e2))
            up2 = self.up2(b)[..., :e2.shape[-2], :e2.shape[-1]]
            d2 = self.dec2(torch.cat([up2, e2], dim=1))
            up1 = self.up1(d2)[..., :e1.shape[-2], :e1.shape[-1]]
            d1 = self.dec1(torch.cat([up1, e1], dim=1))
            return self.head(d1.mean(dim=(-2, -1))).squeeze(-1)

    return UNetScalar


def _build_conv_fno(C, H, W, cnn_hidden=32, fno_hidden=64, fno_modes=16,
                    fno_layers=3):
    """Conv-FNO hybrid: CNN pre-processor → FNO trunk → global pool → head.

    Shallow CNN captures local features before FNO processes global spectral
    structure, addressing FNO's high-frequency truncation bias (Shi et al. 2025,
    arXiv:2503.17797).

    Parameters
    ----------
    C : int — input channels
    H, W : int — spatial dimensions
    cnn_hidden : int — CNN intermediate channels
    fno_hidden : int — FNO channel width
    fno_modes : int — Fourier modes per dimension (auto-clamped to Nyquist)
    fno_layers : int — number of FNO spectral layers

    Returns
    -------
    class — instantiates to an nn.Module
    """
    import torch.nn as nn
    from neuralop.models import FNO

    modes_h = min(fno_modes, H // 2)
    modes_w = min(fno_modes, W // 2)

    class ConvFNOScalar(nn.Module):
        def __init__(self):
            super().__init__()
            self.cnn = nn.Sequential(
                nn.Conv2d(C, cnn_hidden, 3, padding=1), nn.GELU(),
                nn.Conv2d(cnn_hidden, cnn_hidden, 3, padding=1), nn.GELU(),
            )
            self.fno = FNO(
                n_modes=(modes_h, modes_w),
                in_channels=cnn_hidden,
                out_channels=fno_hidden,
                hidden_channels=fno_hidden,
                n_layers=fno_layers,
                domain_padding=0.1,
                norm="instance_norm",
                use_channel_mlp=True,
                channel_mlp_expansion=0.5,
                fno_skip="linear",
                channel_mlp_skip="soft-gating",
            )
            self.head = nn.Sequential(
                nn.Linear(fno_hidden, 64), nn.GELU(), nn.Linear(64, 1)
            )

        def forward(self, x):
            return self.head(self.fno(self.cnn(x)).mean(dim=(-2, -1))).squeeze(-1)

    return ConvFNOScalar


def _build_attention_mlp(C, H, W, hidden=256, attn_hidden=32):
    """Attention-MLP: learned spatial attention weights → weighted sum → MLP.

    A soft attention map over (H, W) positions lets the model focus on
    high-impact spatial regions (e.g., West TX wind belt) while remaining
    interpretable via the attention weights.

    Parameters
    ----------
    C : int — input channels
    H, W : int — spatial dimensions (unused at build time; captured for docs)
    hidden : int — MLP head width (256 = lightweight, 512 = full-capacity)
    attn_hidden : int — attention CNN intermediate channels (32 = light, 64 = deep)

    Returns
    -------
    class — instantiates to an nn.Module
    """
    import torch
    import torch.nn.functional as F
    import torch.nn as nn

    class AttentionMLP(nn.Module):
        def __init__(self):
            super().__init__()
            self.attn_net = nn.Sequential(
                nn.Conv2d(C, attn_hidden, 3, padding=1), nn.GELU(),
                nn.Conv2d(attn_hidden, attn_hidden // 2, 3, padding=1), nn.GELU(),
                nn.Conv2d(attn_hidden // 2, 1, 1),
            )
            self.head = nn.Sequential(
                nn.Linear(C, hidden),
                nn.LayerNorm(hidden),
                nn.GELU(),
                nn.Dropout(0.2),
                nn.Linear(hidden, hidden // 2),
                nn.LayerNorm(hidden // 2),
                nn.GELU(),
                nn.Dropout(0.2),
                nn.Linear(hidden // 2, 64),
                nn.GELU(),
                nn.Linear(64, 1),
            )

        def _attn_weights(self, x):
            """Soft spatial attention map (B, 1, H, W), normalized over H×W."""
            B, C_, H_, W_ = x.shape
            return F.softmax(
                self.attn_net(x).view(B, -1), dim=-1
            ).view(B, 1, H_, W_)

        def forward(self, x):
            return self.head((x * self._attn_weights(x)).sum(dim=(-2, -1))).squeeze(-1)

        def get_attention(self, x):
            """Return spatial attention map (B, H, W) for interpretability."""
            return self._attn_weights(x).squeeze(1)

    return AttentionMLP


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


def train_fold(ModelClass, grid_t, Y_t, train_idx, test_idx, device,
               n_epochs=N_EPOCHS, patience=PATIENCE,
               batch_size=BATCH_SIZE, lr=LR, weight_decay=WEIGHT_DECAY,
               fold_i=0):
    """Train one model fold and return test R².

    Parameters
    ----------
    ModelClass : callable — returns nn.Module
    grid_t  : torch.Tensor (T, C, H, W)
    Y_t     : torch.Tensor (T,) — normalized target
    train_idx, test_idx : array-like — fold indices
    device  : torch.device
    n_epochs, patience, batch_size, lr, weight_decay : training hyperparams
    fold_i  : int — suppresses parameter count print on folds > 0

    Returns
    -------
    r2 : float
    model : trained nn.Module (on CPU, best checkpoint)
    """
    import torch
    import torch.nn as nn
    import torch.nn.functional as F
    from torch.utils.data import TensorDataset, DataLoader

    model = ModelClass().to(device)
    if fold_i == 0:
        print(f"    Parameters: {_count_params(model):,}")

    steps_per_epoch = max(1, (len(train_idx) + batch_size - 1) // batch_size)
    opt = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=weight_decay)
    scheduler = torch.optim.lr_scheduler.OneCycleLR(
        opt, max_lr=lr, epochs=n_epochs, steps_per_epoch=steps_per_epoch,
        pct_start=0.1, anneal_strategy="cos",
    )

    X_tr = grid_t[train_idx].to(device)
    Y_tr = Y_t[train_idx].to(device)
    X_te = grid_t[test_idx].to(device)
    Y_te = Y_t[test_idx].to(device)

    loader = DataLoader(TensorDataset(X_tr, Y_tr), batch_size=batch_size,
                        shuffle=True)

    best_val = float("inf")
    best_state = None
    patience_count = 0

    for _ in range(n_epochs):
        model.train()
        for xb, yb in loader:
            opt.zero_grad()
            loss = F.mse_loss(model(xb), yb)
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            opt.step()
            scheduler.step()

        model.eval()
        with torch.no_grad():
            val = F.mse_loss(model(X_te), Y_te).item()

        if val < best_val - 1e-5:
            best_val = val
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
        y_pred = model(X_te).cpu().numpy()
        y_true = Y_te.cpu().numpy()

    return _r2(y_true, y_pred), model.cpu()


def cross_val(ModelClass, grid_np, Y_np, device, n_folds=N_CV_FOLDS,
              fno_device=None, store_preds=False, **train_kwargs):
    """K-fold cross-validation returning per-fold R² and summary stats.

    FNO-based architectures must stay on CPU because rfft2 is not available
    on MPS; pass fno_device=torch.device("cpu") for those.

    Parameters
    ----------
    ModelClass : callable — returns nn.Module
    grid_np    : ndarray (T, C, H, W)
    Y_np       : ndarray (T,) — raw (un-normalized) curtailment
    device     : torch.device
    fno_device : torch.device or None — overrides device when set
    n_folds    : int
    store_preds : bool — if True, store held-out normalized predictions per
                  fold in result["fold_preds"] = list of (te_idx, y_pred_norm);
                  used by ensemble to average without retraining
    **train_kwargs : forwarded to train_fold

    Returns
    -------
    dict with r2_folds, r2, r2_std, best_model, and (if store_preds) fold_preds
    """
    import torch

    Y_mean, Y_std = Y_np.mean(), Y_np.std() + 1e-8
    Y_norm = ((Y_np - Y_mean) / Y_std).astype(np.float32)
    grid_t = torch.tensor(grid_np, dtype=torch.float32)
    Y_t = torch.tensor(Y_norm, dtype=torch.float32)

    use_device = fno_device if fno_device is not None else device
    kf = KFold(n_splits=n_folds, shuffle=True, random_state=RANDOM_STATE)

    r2_folds = []
    fold_preds = [] if store_preds else None
    best_r2, best_model = -np.inf, None

    for fold_i, (tr_idx, te_idx) in enumerate(kf.split(grid_t)):
        print(f"    Fold {fold_i + 1}/{n_folds}...", end=" ", flush=True)
        try:
            r2, model = train_fold(
                ModelClass, grid_t, Y_t, tr_idx, te_idx,
                device=use_device, fold_i=fold_i, **train_kwargs
            )
            print(f"R²={r2:.4f}")
            if r2 > best_r2:
                best_r2, best_model = r2, model
            if store_preds:
                with torch.no_grad():
                    y_pred = model(grid_t[te_idx]).numpy()
                fold_preds.append((te_idx, y_pred))
        except Exception as exc:
            print(f"Error: {exc}")
            r2 = np.nan
            if store_preds:
                fold_preds.append((te_idx, None))
        r2_folds.append(r2)

    r2_arr = np.array(r2_folds)
    result = {
        "r2_folds": r2_arr,
        "r2": float(np.nanmean(r2_arr)),
        "r2_std": float(np.nanstd(r2_arr)),
        "best_model": best_model,
    }
    if store_preds:
        result["fold_preds"] = fold_preds
    return result


# ── Plotting helpers ───────────────────────────────────────────────────────────

def _draw_texas_base(ax):
    """Draw state and coastline borders on a Cartopy axes."""
    states_shp = shpreader.natural_earth(resolution="10m", category="cultural",
                                         name="admin_1_states_provinces_lines")
    ax.add_geometries(
        list(shpreader.Reader(states_shp).geometries()),
        ccrs.PlateCarree(), facecolor="none", edgecolor="0.5", linewidth=0.5,
    )
    coastlines = shpreader.natural_earth(resolution="10m", category="physical",
                                          name="coastline")
    ax.add_geometries(
        list(shpreader.Reader(coastlines).geometries()),
        ccrs.PlateCarree(), facecolor="none", edgecolor="0.4", linewidth=0.4,
    )
    ax.set_extent([-107, -93, 25.5, 37], crs=ccrs.PlateCarree())


def plot_bar_comparison(results_dict, title, save_path, baselines=None):
    """Bar chart comparing CV R² across architectures or conditions.

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

    ax.set_ylabel("CV R²")
    ax.set_ylim(0, min(1.05, max(r2s) + max(stds) + 0.12))
    ax.set_title(title)
    plt.xticks(rotation=20, ha="right")
    fig.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {save_path}")


def plot_saliency_map(saliency, pixel_coords, title, save_path, ref_beta=None):
    """Plot gradient saliency on a Texas map with optional PLS β(s) reference.

    Parameters
    ----------
    saliency    : ndarray (N_pixels,) — absolute saliency values
    pixel_coords : ndarray (N_pixels, 2) — (lat, lon)
    title       : str
    save_path   : Path
    ref_beta    : ndarray (N_pixels,) or None — PLS β(s) to plot alongside
    """
    ncols = 2 if ref_beta is not None else 1
    fig, axes = plt.subplots(
        1, ncols, figsize=(8 * ncols, 6),
        subplot_kw={"projection": ccrs.PlateCarree()},
    )
    if ncols == 1:
        axes = [axes]

    _draw_texas_base(axes[0])
    sc = axes[0].scatter(
        pixel_coords[:, 1], pixel_coords[:, 0],
        c=saliency, cmap="YlOrRd", s=12,
        transform=ccrs.PlateCarree(), vmin=0,
    )
    plt.colorbar(sc, ax=axes[0], label="|∂output/∂input|")
    axes[0].set_title(title)

    if ref_beta is not None:
        _draw_texas_base(axes[1])
        vmax = np.percentile(np.abs(ref_beta), 98)
        sc2 = axes[1].scatter(
            pixel_coords[:, 1], pixel_coords[:, 0],
            c=ref_beta, cmap="RdBu_r", s=12,
            transform=ccrs.PlateCarree(), vmin=-vmax, vmax=vmax,
        )
        plt.colorbar(sc2, ax=axes[1], label="PLS β(s)")
        axes[1].set_title("PLS n=20 β(s) (wspd_error_1h)")

    fig.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {save_path}")


# ── Experiment implementations ─────────────────────────────────────────────────

def run_exp_arch(months, fig_dir, tables_dir, device, fno_device, df=None, skip_fno=False):
    """Experiment A: Architecture comparison at 0.25° with all 4 channels.

    Tests MLP, FNO, U-Net, Conv-FNO, and Attention-MLP at 0.25° resolution
    with all 4 error channels.

    Parameters
    ----------
    months     : list of (year, month)
    fig_dir    : Path
    tables_dir : Path
    device     : torch.device
    fno_device : torch.device — for architectures using rfft2
    df         : pre-loaded DataFrame (optional, avoids re-reading parquets)
    skip_fno   : bool — skip FNO/Conv-FNO (CPU-only, very slow)

    Returns
    -------
    dict {arch_name: {r2, r2_std, r2_folds, best_model}}
    """
    print("\n" + "=" * 70)
    print("EXPERIMENT A: ARCHITECTURE COMPARISON (0.25°, 4-channel)")
    print("=" * 70)

    X_dict, Y, pc = load_multi_field_data(months, fields=ALL_FIELDS, df=df)
    grid_np = build_grid_tensor(X_dict, pc, resolution=0.25)
    T, C, H, W = grid_np.shape
    print(f"  Grid: {T}T × {C}C × {H}H × {W}W  (in_dim={C*H*W})")

    try:
        from neuralop.models import FNO as _FNO  # noqa: F401
        has_neuralop = True
    except ImportError:
        has_neuralop = False
        print("  neuralop not available — skipping FNO/Conv-FNO")

    arch_map = {
        "MLP": _make_mlp_cls(C * H * W),
        "U-Net": _build_unet_scalar(C),
        "Attention-MLP": _build_attention_mlp(C, H, W),
    }
    fno_arch_map = {}
    if has_neuralop and not skip_fno:
        # Run 4B config (2L/h32/m8): deep config (4L/h64/m16) takes 4h+/fold on CPU
        fno_arch_map["FNO"] = _build_fno(C, H, W, modes=8, hidden=32, n_layers=2)
        fno_arch_map["Conv-FNO"] = _build_conv_fno(C, H, W, fno_modes=8, fno_hidden=32, fno_layers=2)
    elif skip_fno:
        print("  Skipping FNO/Conv-FNO (--skip-fno flag set)")

    results = {}

    for name, ModelClass in arch_map.items():
        print(f"\n  [{name}]")
        results[name] = cross_val(ModelClass, grid_np, Y, device)
        print(f"  {name}: R²={results[name]['r2']:.4f} "
              f"± {results[name]['r2_std']:.4f}")

    for name, ModelClass in fno_arch_map.items():
        print(f"\n  [{name}] (CPU, {FNO_N_EPOCHS} epochs)")
        results[name] = cross_val(
            ModelClass, grid_np, Y, device, fno_device=fno_device,
            n_epochs=FNO_N_EPOCHS, patience=FNO_PATIENCE,
        )
        print(f"  {name}: R²={results[name]['r2']:.4f} "
              f"± {results[name]['r2_std']:.4f}")

    plot_bar_comparison(
        results,
        title="Architecture Comparison — 0.25° grid, 4-channel, 5-fold CV",
        save_path=fig_dir / "nn_arch_comparison.png",
        baselines={"Run4B MLP": RUN4B_MLP_R2, "Run4B FNO": RUN4B_FNO_R2, "Run1 Ridge": RUN1_RIDGE_R2},
    )
    rows = [{"architecture": k, "r2": v["r2"], "r2_std": v["r2_std"],
             "resolution": "0.25deg", "channels": "4-ch"}
            for k, v in results.items()]
    pd.DataFrame(rows).to_csv(tables_dir / "nn_arch_comparison.csv", index=False)
    return results


def run_exp_resolution(months, fig_dir, tables_dir, device, fno_device, df=None, skip_fno=False):
    """Experiment B: Resolution ablation for MLP and FNO.

    Tests MLP and FNO (if neuralop available) at 0.50°, 0.25°, and 0.10°
    (native ERA5) grid resolution.

    Parameters
    ----------
    months     : list of (year, month)
    fig_dir    : Path
    tables_dir : Path
    device, fno_device : torch.device
    df         : pre-loaded DataFrame (optional)
    skip_fno   : bool — skip FNO (CPU-only, very slow)

    Returns
    -------
    dict {resolution_label: {arch: result}}
    """
    print("\n" + "=" * 70)
    print("EXPERIMENT B: RESOLUTION ABLATION (MLP + FNO)")
    print("=" * 70)

    X_dict, Y, pc = load_multi_field_data(months, fields=ALL_FIELDS, df=df)

    try:
        from neuralop.models import FNO as _FNO  # noqa: F401
        has_neuralop = True
    except ImportError:
        has_neuralop = False

    all_results = {}

    for res in [0.5, 0.25, 0.1]:
        label = f"{res}deg"
        print(f"\n  Resolution: {res}°")
        grid_np = build_grid_tensor(X_dict, pc, resolution=res)
        T, C, H, W = grid_np.shape
        print(f"    Grid: {T}T × {C}C × {H}H × {W}W")
        all_results[label] = {}

        print(f"    MLP...")
        mlp_result = cross_val(_make_mlp_cls(C * H * W), grid_np, Y, device)
        all_results[label]["MLP"] = mlp_result
        print(f"    MLP R²={mlp_result['r2']:.4f} ± {mlp_result['r2_std']:.4f}")

        if has_neuralop and not skip_fno:
            print(f"    FNO (CPU, {FNO_N_EPOCHS} epochs)...")
            fno_result = cross_val(
                _build_fno(C, H, W, modes=8, hidden=32, n_layers=2), grid_np, Y, device, fno_device=fno_device,
                n_epochs=FNO_N_EPOCHS, patience=FNO_PATIENCE,
            )
            all_results[label]["FNO"] = fno_result
            print(f"    FNO R²={fno_result['r2']:.4f} ± {fno_result['r2_std']:.4f}")

    flat = {f"{arch}@{res_label}": res
            for res_label, arch_dict in all_results.items()
            for arch, res in arch_dict.items()}
    plot_bar_comparison(
        flat,
        title="Resolution Ablation — MLP and FNO at 0.50°, 0.25°, 0.10°",
        save_path=fig_dir / "nn_resolution_ablation.png",
    )

    rows = [{"resolution": res_label, "architecture": arch,
             "r2": res["r2"], "r2_std": res["r2_std"]}
            for res_label, arch_dict in all_results.items()
            for arch, res in arch_dict.items()]
    pd.DataFrame(rows).to_csv(tables_dir / "nn_resolution_ablation.csv", index=False)
    return all_results


def run_exp_channels(months, fig_dir, tables_dir, device, df=None):
    """Experiment C: Channel ablation — decompose predictive value of each field.

    Trains an MLP at 0.25° for five channel configurations using a single
    data load to avoid redundant parquet reads.

    Parameters
    ----------
    months     : list of (year, month)
    fig_dir    : Path
    tables_dir : Path
    device     : torch.device
    df         : pre-loaded DataFrame (optional)

    Returns
    -------
    dict {channel_label: {r2, r2_std, r2_folds}}
    """
    print("\n" + "=" * 70)
    print("EXPERIMENT C: CHANNEL ABLATION")
    print("=" * 70)

    # Load all 4 fields once; slice per config below
    if df is None:
        df = load_pixel_data(months)
    X_dict_full, Y, pc = load_multi_field_data(months, fields=ALL_FIELDS, df=df)

    channel_configs = {
        "1-ch: wind-1h":  ["wspd_error_1h"],
        "1-ch: temp-1h":  ["temp_error_1h"],
        "2-ch: HRRR-1h":  ["wspd_error_1h", "temp_error_1h"],
        "2-ch: GFS-0h":   ["wspd_error_0h", "temp_error_0h"],
        "4-ch: all":      ALL_FIELDS,
    }

    results = {}
    for label, fields in channel_configs.items():
        print(f"\n  [{label}]")
        X_sub = {f: X_dict_full[f] for f in fields if f in X_dict_full}
        grid_np = build_grid_tensor(X_sub, pc, resolution=0.25)
        T, C, H, W = grid_np.shape
        print(f"    Grid: ({T}, {C}, {H}, {W})")
        results[label] = cross_val(_make_mlp_cls(C * H * W), grid_np, Y, device)
        print(f"    R²={results[label]['r2']:.4f} ± {results[label]['r2_std']:.4f}")

    plot_bar_comparison(
        results,
        title="Channel Ablation — MLP at 0.25°",
        save_path=fig_dir / "nn_channel_ablation.png",
        baselines={"Run4B MLP (2-ch)": RUN4B_MLP_R2},
    )
    rows = [{"channels": k, "r2": v["r2"], "r2_std": v["r2_std"]}
            for k, v in results.items()]
    pd.DataFrame(rows).to_csv(tables_dir / "nn_channel_ablation.csv", index=False)
    return results


def run_exp_regime(months, fig_dir, tables_dir, device):
    """Experiment D: Regime-stratified evaluation and regime-specific training.

    Evaluates the pooled MLP model's R² separately on extreme cold, extreme
    heat, and normal hours. Also trains regime-specific MLP models and compares
    to the pooled baseline within each regime.

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

    df = load_pixel_data_with_regimes(months)
    df = classify_regimes(df)

    X_dict = {}
    Y_ref, pc_ref, idx_ref = None, None, None
    for field in ALL_FIELDS:
        try:
            X, Y, pc, _, hour_idx = prepare_functional_data(df, error_field=field)
            X_dict[field] = X
            if Y_ref is None:
                Y_ref, pc_ref, idx_ref = Y, pc, hour_idx
        except Exception as exc:
            print(f"  Skipping {field}: {exc}")

    T_min = min(v.shape[0] for v in X_dict.values())
    X_dict = {k: v[:T_min] for k, v in X_dict.items()}
    Y_ref = Y_ref[:T_min]

    grid_np = build_grid_tensor(X_dict, pc_ref, resolution=0.25)
    T, C, H, W = grid_np.shape
    in_dim = C * H * W
    print(f"  Grid: ({T}, {C}, {H}, {W})")

    hour_df = (
        df.drop_duplicates("valid_time")[["valid_time", "is_extreme", "regime_temp"]]
        .sort_values("valid_time")
        .reset_index(drop=True)
    )
    times = pd.Series(idx_ref[:T_min]) if (
        idx_ref is not None and len(idx_ref) == T_min
    ) else hour_df["valid_time"].iloc[:T_min]

    regime_temp = (
        hour_df[hour_df["valid_time"].isin(times)]
        .set_index("valid_time")
        .reindex(times)["regime_temp"]
        .values
    )

    cold_mask = regime_temp == "extreme_cold"
    heat_mask = regime_temp == "extreme_heat"
    normal_mask = ~(cold_mask | heat_mask)

    print(f"  Regime breakdown: cold={cold_mask.sum()}, "
          f"heat={heat_mask.sum()}, normal={normal_mask.sum()}")

    print("\n  Training pooled model...")
    pooled_res = cross_val(_make_mlp_cls(in_dim), grid_np, Y_ref, device)
    print(f"  Pooled CV R²={pooled_res['r2']:.4f} ± {pooled_res['r2_std']:.4f}")

    Y_mean, Y_std = Y_ref.mean(), Y_ref.std() + 1e-8
    Y_norm = ((Y_ref - Y_mean) / Y_std).astype(np.float32)
    grid_t = torch.tensor(grid_np, dtype=torch.float32)
    Y_t = torch.tensor(Y_norm, dtype=torch.float32)

    pooled_model = pooled_res["best_model"]
    regime_eval = {}
    if pooled_model is not None:
        pooled_model.eval()
        with torch.no_grad():
            y_pred_all = pooled_model(grid_t).numpy()
        for rname, mask in [("extreme_cold", cold_mask),
                             ("extreme_heat", heat_mask),
                             ("normal", normal_mask)]:
            if mask.sum() < 20:
                print(f"  {rname}: too few samples ({mask.sum()}) — skipping")
                continue
            y_tr = Y_t[mask].numpy()
            y_pr = y_pred_all[mask]
            r2 = _r2(y_tr, y_pr)
            regime_eval[rname] = {"r2": r2, "r2_std": 0.0, "n": int(mask.sum())}
            print(f"  Pooled on {rname}: R²={r2:.4f}  (n={mask.sum()})")

    regime_specific = {}
    for rname, mask in [("extreme_cold", cold_mask), ("extreme_heat", heat_mask)]:
        if mask.sum() < MIN_REGIME_SAMPLES:
            continue
        print(f"\n  Regime-specific model for {rname} (n={mask.sum()})...")
        n_folds_r = N_CV_FOLDS if mask.sum() >= MIN_SAMPLES_FULL_CV else 3
        if n_folds_r < N_CV_FOLDS:
            print(f"    Using {n_folds_r}-fold CV (insufficient samples for {N_CV_FOLDS})")
        res_r = cross_val(_make_mlp_cls(in_dim), grid_np[mask], Y_ref[mask],
                           device, n_folds=n_folds_r)
        regime_specific[rname] = res_r
        print(f"  {rname}-specific MLP: R²={res_r['r2']:.4f} "
              f"± {res_r['r2_std']:.4f}")

    plot_data = {"Pooled (all)": pooled_res}
    plot_data.update({f"Pooled→{k}": v for k, v in regime_eval.items()})
    plot_data.update({f"Specific:{k}": v for k, v in regime_specific.items()})

    plot_bar_comparison(
        plot_data,
        title="Regime-Stratified MLP — 0.25° grid, 4-channel",
        save_path=fig_dir / "nn_regime_analysis.png",
    )
    rows = [{"model": k, "r2": v["r2"], "r2_std": v["r2_std"]}
            for k, v in plot_data.items()]
    pd.DataFrame(rows).to_csv(tables_dir / "nn_regime_analysis.csv", index=False)

    return {"pooled": pooled_res, "regime_eval": regime_eval,
            "regime_specific": regime_specific}


def run_exp_saliency(months, fig_dir, tables_dir, device, df=None):
    """Experiment E: Gradient saliency maps vs PLS β(s).

    Trains an MLP on the full dataset, then computes per-pixel gradient
    saliency (mean |∂output/∂input| over all samples, batched to control
    memory). Plots alongside PLS n=20 β(s) and reports Pearson r.

    Parameters
    ----------
    months      : list of (year, month)
    fig_dir     : Path
    tables_dir  : Path
    device      : torch.device
    df          : pre-loaded DataFrame (optional)

    Returns
    -------
    dict with saliency array and correlation with PLS
    """
    print("\n" + "=" * 70)
    print("EXPERIMENT E: GRADIENT SALIENCY MAPS")
    print("=" * 70)

    import torch
    from sklearn.cross_decomposition import PLSRegression

    X_dict, Y, pc = load_multi_field_data(months, fields=ALL_FIELDS, df=df)
    grid_np, lat_g, lon_g = prepare_grid_data_coarse(X_dict, pc, target_res=0.25)
    T, C, H, W = grid_np.shape

    # Pixel-to-grid index mapping (consistent with prepare_grid_data_coarse)
    lat_idx, lon_idx, _, _ = _coarse_grid_indices(pc, resolution=0.25)

    pls_beta = None
    X_wind = X_dict.get("wspd_error_1h")
    if X_wind is not None:
        print("  Computing PLS n=20 β(s) reference...")
        pls = PLSRegression(n_components=20, max_iter=500)
        pls.fit(X_wind, Y)
        pls_beta = pls.coef_.ravel()

    print("  Training MLP on full data for saliency...")
    in_dim = C * H * W
    Y_mean, Y_std = Y.mean(), Y.std() + 1e-8
    Y_norm = ((Y - Y_mean) / Y_std).astype(np.float32)
    grid_t = torch.tensor(grid_np, dtype=torch.float32)
    Y_t = torch.tensor(Y_norm, dtype=torch.float32)

    # Train on all data (no held-out set): use indices [0, T) as both train and test
    all_idx = np.arange(T)
    _, model = train_fold(_make_mlp_cls(in_dim), grid_t, Y_t, all_idx, all_idx, device)

    # Batched gradient saliency: accumulate |∂output/∂input| in chunks to
    # avoid materializing the full (T, C, H, W) gradient tensor at once.
    print("  Computing gradient saliency...")
    model.eval().to("cpu")
    sal_accum = torch.zeros(C, H, W)
    saliency_batch = 256

    for start in range(0, T, saliency_batch):
        xb = grid_t[start: start + saliency_batch].requires_grad_(True)
        model(xb).sum().backward()
        sal_accum += xb.grad.abs().sum(dim=0).detach()
        xb.grad = None

    sal_grid = (sal_accum / T).mean(dim=0).numpy()  # (H, W) mean over channels

    # Map (H, W) back to pixel_coords using the same index mapping as the grid
    saliency_pixels = sal_grid[lat_idx, lon_idx]

    pls_corr = None
    if pls_beta is not None:
        pls_corr = float(np.corrcoef(saliency_pixels, np.abs(pls_beta))[0, 1])
        print(f"  Pearson r (|saliency| vs |PLS β|): {pls_corr:.4f}")

    plot_saliency_map(
        saliency_pixels, pc,
        title="MLP Gradient Saliency",
        save_path=fig_dir / "nn_saliency_map.png",
        ref_beta=pls_beta,
    )

    rows = [{"lat": pc[i, 0], "lon": pc[i, 1], "saliency": float(saliency_pixels[i])}
            for i in range(len(pc))]
    pd.DataFrame(rows).to_csv(tables_dir / "nn_saliency_pixels.csv", index=False)

    return {"saliency": saliency_pixels, "pls_corr": pls_corr, "pls_beta": pls_beta}


# ── Run 6: improved architectures ─────────────────────────────────────────────

def _build_residual_mlp(in_dim, hidden=512, dropout=0.3):
    """4-block residual MLP for better gradient flow than the flat Run 5 MLP.

    Each ResBlock is: LayerNorm → Linear → GELU → Dropout → Linear, with a
    skip projection if input and output dims differ.  Four blocks give the
    same effective depth as the Run 5 MLP but with identity shortcuts.

    Parameters
    ----------
    in_dim  : int — flattened C * H * W
    hidden  : int — hidden width for all blocks
    dropout : float — dropout probability inside each block

    Returns
    -------
    class — instantiates to an nn.Module
    """
    import torch
    import torch.nn as nn

    class ResBlock(nn.Module):
        def __init__(self, dim_in, dim_out, p_drop):
            super().__init__()
            self.net = nn.Sequential(
                nn.LayerNorm(dim_in),
                nn.Linear(dim_in, dim_out),
                nn.GELU(),
                nn.Dropout(p_drop),
                nn.Linear(dim_out, dim_out),
            )
            self.proj = nn.Linear(dim_in, dim_out) if dim_in != dim_out else nn.Identity()

        def forward(self, x):
            return self.net(x) + self.proj(x)

    class ResidualMLP(nn.Module):
        def __init__(self):
            super().__init__()
            self.embed = nn.Linear(in_dim, hidden)
            self.blocks = nn.Sequential(
                ResBlock(hidden, hidden, dropout),
                ResBlock(hidden, hidden // 2, dropout),
                ResBlock(hidden // 2, 128, dropout),
                ResBlock(128, 64, dropout),
            )
            self.head = nn.Linear(64, 1)

        def forward(self, x):
            return self.head(self.blocks(self.embed(x.reshape(x.shape[0], -1)))).squeeze(-1)

    return ResidualMLP


def run_exp_followup(months, fig_dir, tables_dir, device, df=None):
    """Run 6: targeted improvements on the best Run 5 architecture (MLP).

    Tests four follow-up architectures at 0.25° with 4 channels:
      F1 — Residual MLP (4 ResBlocks, same hidden=512)
      F2 — Large Attention-MLP (hidden=512 head, deeper attention)
      F3 — U-Net (fixed skip-connection bug from Run 5)
      F4 — Ensemble average of best MLP and best Attention-MLP predictions

    Parameters
    ----------
    months     : list of (year, month)
    fig_dir    : Path
    tables_dir : Path
    device     : torch.device
    df         : pre-loaded DataFrame (optional)

    Returns
    -------
    dict {arch_name: {r2, r2_std, r2_folds, best_model}}
    """
    import torch

    print("\n" + "=" * 70)
    print("RUN 6: FOLLOW-UP IMPROVEMENTS (0.25°, 4-channel)")
    print("=" * 70)

    X_dict, Y, pc = load_multi_field_data(months, fields=ALL_FIELDS, df=df)
    grid_np = build_grid_tensor(X_dict, pc, resolution=0.25)
    T, C, H, W = grid_np.shape
    print(f"  Grid: {T}T × {C}C × {H}H × {W}W")

    results = {}

    # F1: Residual MLP — store per-fold held-out predictions for F4 ensemble
    print("\n  [F1: Residual-MLP]")
    results["Residual-MLP"] = cross_val(
        _build_residual_mlp(C * H * W), grid_np, Y, device, store_preds=True
    )
    print(f"  Residual-MLP: R²={results['Residual-MLP']['r2']:.4f} "
          f"± {results['Residual-MLP']['r2_std']:.4f}")

    # F2: Large Attention-MLP — store per-fold held-out predictions for F4 ensemble
    print("\n  [F2: Large Attention-MLP]")
    results["Attention-MLP (large)"] = cross_val(
        _build_attention_mlp(C, H, W, hidden=512, attn_hidden=64), grid_np, Y, device,
        store_preds=True,
    )
    print(f"  Attention-MLP (large): R²={results['Attention-MLP (large)']['r2']:.4f} "
          f"± {results['Attention-MLP (large)']['r2_std']:.4f}")

    # F3: Fixed U-Net
    print("\n  [F3: U-Net (fixed)]")
    results["U-Net (fixed)"] = cross_val(
        _build_unet_scalar(C), grid_np, Y, device
    )
    print(f"  U-Net (fixed): R²={results['U-Net (fixed)']['r2']:.4f} "
          f"± {results['U-Net (fixed)']['r2_std']:.4f}")

    # F4: Ensemble — average F1/F2 stored held-out predictions (no retraining)
    print("\n  [F4: Ensemble (Residual-MLP + Large Attention-MLP)]")
    Y_norm = ((Y - Y.mean()) / (Y.std() + 1e-8)).astype(np.float32)
    r2_ens = []
    for (te_idx_mlp, pred_mlp), (te_idx_attn, pred_attn) in zip(
        results["Residual-MLP"]["fold_preds"],
        results["Attention-MLP (large)"]["fold_preds"],
    ):
        if pred_mlp is None or pred_attn is None:
            r2_ens.append(np.nan)
            continue
        y_pred_ens = 0.5 * (pred_mlp + pred_attn)
        r2_ens.append(_r2(Y_norm[te_idx_mlp], y_pred_ens))

    r2_ens_arr = np.array(r2_ens)
    results["Ensemble"] = {
        "r2_folds": r2_ens_arr,
        "r2": float(np.nanmean(r2_ens_arr)),
        "r2_std": float(np.nanstd(r2_ens_arr)),
        "best_model": None,
    }
    print(f"  Ensemble: R²={results['Ensemble']['r2']:.4f} "
          f"± {results['Ensemble']['r2_std']:.4f}")

    results["Run5 MLP (baseline)"] = {
        "r2": 0.9358, "r2_std": 0.0142,
        "r2_folds": np.array([0.9331, 0.9267, 0.9155, 0.9516, 0.9519]),
        "best_model": None,
    }

    plot_bar_comparison(
        results,
        title="Run 6: Follow-up Improvements vs Run 5 MLP Baseline",
        save_path=fig_dir / "nn_run6_comparison.png",
        baselines={"Run5 MLP": RUN5_MLP_R2},
    )
    rows = [{"architecture": k, "r2": v["r2"], "r2_std": v["r2_std"]}
            for k, v in results.items()]
    pd.DataFrame(rows).to_csv(tables_dir / "nn_run6_results.csv", index=False)
    return results


# ── Main orchestrator ──────────────────────────────────────────────────────────

def run_all(months=None, experiments=None, skip_fno=False):
    """Run selected experiments and save all results.

    Loads pixel data once and passes the DataFrame to experiments that share
    the same months and fields (arch, resolution, channels, saliency) to
    avoid redundant parquet reads.

    Parameters
    ----------
    months      : list of (year, month); defaults to DEFAULT_MONTHS
    experiments : list of experiment names or None to run all
    skip_fno    : bool — skip FNO/Conv-FNO in arch and resolution experiments
    """
    if months is None:
        months = DEFAULT_MONTHS
    if experiments is None:
        experiments = ["arch", "resolution", "channels", "regime", "saliency", "followup"]

    try:
        import torch  # noqa: F401
    except ImportError:
        raise RuntimeError("torch is required. Install with: uv add torch")

    device = _select_device()
    # rfft2 is unavailable on MPS, so FNO always runs on CPU
    fno_device = torch.device("cpu")
    print(f"  Primary device: {device}  |  FNO device: {fno_device}")

    dirs = setup_directories()
    fig_dir = Path(dirs["figures"]) / "functional_analysis"
    tables_dir = Path(dirs["tables"])
    fig_dir.mkdir(parents=True, exist_ok=True)
    tables_dir.mkdir(parents=True, exist_ok=True)

    # Pre-load once for all non-regime experiments to avoid repeated I/O
    shared_exps = {"arch", "resolution", "channels", "saliency"}
    df_shared = None
    if shared_exps & set(experiments):
        print("\n  Pre-loading pixel data...")
        df_shared = load_pixel_data(months)

    all_results = {}

    if "arch" in experiments:
        all_results["arch"] = run_exp_arch(
            months, fig_dir, tables_dir, device, fno_device, df=df_shared, skip_fno=skip_fno
        )
    if "resolution" in experiments:
        all_results["resolution"] = run_exp_resolution(
            months, fig_dir, tables_dir, device, fno_device, df=df_shared, skip_fno=skip_fno
        )
    if "channels" in experiments:
        all_results["channels"] = run_exp_channels(
            months, fig_dir, tables_dir, device, df=df_shared
        )
    if "regime" in experiments:
        all_results["regime"] = run_exp_regime(
            months, fig_dir, tables_dir, device
        )
    if "saliency" in experiments:
        all_results["saliency"] = run_exp_saliency(
            months, fig_dir, tables_dir, device, df=df_shared
        )
    if "followup" in experiments:
        # Ensure shared df is loaded even if arch/channels/saliency were skipped
        if df_shared is None:
            print("\n  Pre-loading pixel data for follow-up...")
            df_shared = load_pixel_data(months)
        all_results["followup"] = run_exp_followup(
            months, fig_dir, tables_dir, device, df=df_shared
        )

    print("\n" + "=" * 70)
    print("RUN 5 COMPLETE")
    print("=" * 70)
    print("\nSummary:")
    for exp, res in all_results.items():
        if isinstance(res, dict):
            for k, v in res.items():
                if isinstance(v, dict) and "r2" in v:
                    print(f"  {exp}/{k}: R²={v['r2']:.4f} ± {v['r2_std']:.4f}")

    return all_results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run 5: Neural architecture expansion for ERCOT curtailment"
    )
    parser.add_argument(
        "--exp",
        nargs="+",
        default=["all"],
        choices=["all", "arch", "resolution", "channels", "regime", "saliency", "followup"],
        help="Which experiment(s) to run (space-separated)",
    )
    parser.add_argument(
        "--months",
        nargs="+",
        type=int,
        default=[1, 3, 5, 7, 9, 11],
        help="Month numbers within 2025 (e.g. 1 3 5 7 9 11)",
    )
    parser.add_argument(
        "--skip-fno",
        action="store_true",
        help="Skip FNO/Conv-FNO in arch and resolution experiments (very slow on CPU)",
    )
    args = parser.parse_args()

    months = [(2025, m) for m in args.months]
    exps = None if "all" in args.exp else args.exp
    run_all(months=months, experiments=exps, skip_fno=args.skip_fno)
