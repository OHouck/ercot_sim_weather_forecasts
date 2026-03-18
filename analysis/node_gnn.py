"""
Graph Neural Network: Weather Forecast Error Impact on ERCOT LMP
================================================================

v3 — Transmission-guided graph with virtual super node

Graph construction:
  1. Load ERCOT 345 kV transmission lines from Line_Output.shp
  2. Map each settlement-point node to its nearest simulation bus
  3. Connect nodes whose mapped buses share a transmission line
  4. Fix isolated nodes by connecting to k=3 nearest connected nodes
  5. Add a **virtual super node** connected to every real node

Full propagation mechanism:
  The virtual super node acts as a system-wide information aggregator,
  analogous to the ERCOT system operator's global dispatch view.  In the
  first GAT layer every real node sends its embedding to the virtual node,
  which computes an attention-weighted global summary.  In the second layer
  the virtual node broadcasts that summary back, so every real node
  receives information about every other real node — even those many hops
  away on the transmission graph.  This gives full shock propagation in
  just two layers, without O(N^2) global self-attention.  The GAT's
  learned attention weights control how much each node contributes to (and
  draws from) the global state versus its local transmission neighbors.

Usage:
    uv run python -m analysis.node_gnn
"""

import os
import sys
from pathlib import Path

os.environ.setdefault("PYTORCH_ENABLE_MPS_FALLBACK", "1")

import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.io.shapereader as shpreader
from scipy.spatial import KDTree
from sklearn.preprocessing import StandardScaler

import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.data import Data
from torch_geometric.loader import DataLoader
from torch_geometric.nn import GATConv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories
from process_data.prepare_node_level_data import prepare_node_level_data

# ── Configuration ────────────────────────────────────────────────
MODEL = "hrrr"
ERROR_SOURCE = "era5"
MONTHS = [(2025, m) for m in range(1, 13)]

FALLBACK_K = 3            # k-NN for isolated nodes
HIDDEN_DIM = 128
N_HEADS = 4
N_GAT_LAYERS = 3
DROPOUT = 0.2
MLP_HIDDEN = 64

TRAIN_DAYS = (1, 15)
VAL_DAYS = (16, 25)
TEST_DAYS = (26, 31)

BATCH_SIZE = 64
LEARNING_RATE = 1e-3
WEIGHT_DECAY = 1e-4
MAX_EPOCHS = 200
PATIENCE = 20

_GIS_ROOT = (
    '/Users/ohouck/Library/CloudStorage/OneDrive-TheUniversityofChicago/'
    'ercot_sim_weather_forecasts/Texas_GIS_Data'
)
LINE_SHP = os.path.join(_GIS_ROOT, 'Line', 'Line_Output.shp')
BUS_SHP = os.path.join(_GIS_ROOT, 'Bus', 'Bus_Output.shp')


def _select_device() -> torch.device:
    if torch.cuda.is_available():
        return torch.device("cuda")
    if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        try:
            _x = torch.randn(4, 4, device="mps")
            _ = _x[torch.tensor([0, 1], device="mps")]
            return torch.device("mps")
        except Exception as _e:
            print(f"  MPS smoke-test failed ({_e}), falling back to CPU.")
    return torch.device("cpu")


DEVICE = _select_device()
SEED = 42

WEATHER_ZONES = [
    "coast", "east", "far_west", "north",
    "north_central", "south", "south_central", "west",
]

BASE_NUMERIC_COLS = [
    "observed_temp",       # 0
    "observed_wspd",       # 1
    "temp_error_1h",       # 2
    "wspd_error_1h",       # 3
    "temp_error_18h",      # 4
    "wspd_error_18h",      # 5
    "forecast_temp_1h",    # 6
    "forecast_wspd_1h",    # 7
    "forecast_temp_18h",   # 8
    "forecast_wspd_18h",   # 9
    "actual_load",         # 10
    "forecast_load_1h",    # 11
    "forecast_load_18h",   # 12
    "load_error_1h",       # 13
    "load_error_18h",      # 14
    "lat",                 # 15
    "lon",                 # 16
]
TARGET_COL = "lmp"

ERROR_FEATURE_INDICES = [2, 3, 4, 5]
ERROR_FEATURE_NAMES = [
    "temp_error_1h", "wspd_error_1h", "temp_error_18h", "wspd_error_18h",
]

LMP_CLIP_LO = 1
LMP_CLIP_HI = 99

# Edge type constants (stored as edge_attr feature 2)
EDGE_TRANSMISSION = 0
EDGE_FALLBACK_KNN = 1
EDGE_VIRTUAL = 2


# ── Data Loading ─────────────────────────────────────────────────

def load_data():
    dirs = setup_directories()
    df = prepare_node_level_data(
        months=MONTHS, model=MODEL, error_source=ERROR_SOURCE,
    )
    df["hour"] = pd.to_datetime(df["hour"])
    node_coords = pd.read_csv(
        os.path.join(dirs["processed"], "node_coordinates.csv")
    )
    node_coords = node_coords.dropna(subset=["lat", "lon"])
    return df, node_coords, dirs


# ── Graph Construction ───────────────────────────────────────────

def build_transmission_graph(node_coords):
    """Build a graph guided by the ERCOT 345 kV transmission topology.

    Algorithm:
      1. Load 123 simulation buses and 255 transmission lines.
      2. Map each settlement-point node to its nearest simulation bus.
      3. For each transmission line (FBus→TBus), connect all nodes mapped
         to FBus with all nodes mapped to TBus (bipartite edges).
      4. Connect all nodes mapped to the *same* bus to each other.
      5. Fix any isolated nodes: connect each to its 3 nearest already-
         connected nodes.  Repeat until the graph is fully connected.
      6. Add a virtual super node (index = N) connected to all N real
         nodes.

    Returns:
        edge_index: (2, E) LongTensor  — includes virtual-node edges
        edge_attr:  (E, 2) FloatTensor — [normalised distance, edge type]
        node_order: list of settlement_point names (length N, real only)
        positions:  (N, 2) ndarray of (lat, lon) for real nodes
        virtual_node_idx: int — index of the virtual super node (= N)
    """
    # ── Load shapefiles ──
    lines = gpd.read_file(LINE_SHP)
    buses = gpd.read_file(BUS_SHP)

    bus_positions = buses[["Bus_Number", "Bus_latitu", "Bus_longit"]].copy()
    bus_positions = bus_positions.rename(columns={
        "Bus_latitu": "lat", "Bus_longit": "lon",
    })
    bus_pos_arr = bus_positions[["lat", "lon"]].values
    bus_numbers = bus_positions["Bus_Number"].values
    bus_tree = KDTree(bus_pos_arr)

    # ── Map settlement-point nodes to nearest simulation bus ──
    node_order = node_coords["settlement_point"].tolist()
    positions = node_coords[["lat", "lon"]].values
    num_nodes = len(node_order)

    _, nearest_bus_idx = bus_tree.query(positions)
    node_to_bus = bus_numbers[nearest_bus_idx]   # bus number per node

    # Invert: bus_number → list of node indices
    bus_to_nodes = {}
    for i, bus_num in enumerate(node_to_bus):
        bus_to_nodes.setdefault(int(bus_num), []).append(i)

    # ── Build edges from transmission lines ──
    edge_set = {}   # (src, dst) → geographic distance

    for _, line in lines.iterrows():
        fbus = int(line["FBus_Num"])
        tbus = int(line["TBus_Num"])
        fnodes = bus_to_nodes.get(fbus, [])
        tnodes = bus_to_nodes.get(tbus, [])

        # Cross-bus edges
        for fi in fnodes:
            for ti in tnodes:
                if fi == ti:
                    continue
                dist = np.sqrt(
                    (positions[fi, 0] - positions[ti, 0]) ** 2
                    + (positions[fi, 1] - positions[ti, 1]) ** 2
                )
                for s, d in [(fi, ti), (ti, fi)]:
                    if (s, d) not in edge_set:
                        edge_set[(s, d)] = (dist, EDGE_TRANSMISSION)

    # Intra-bus edges (nodes mapped to the same bus)
    for bus_num, node_list in bus_to_nodes.items():
        for i in range(len(node_list)):
            for j in range(i + 1, len(node_list)):
                ni, nj = node_list[i], node_list[j]
                dist = np.sqrt(
                    (positions[ni, 0] - positions[nj, 0]) ** 2
                    + (positions[ni, 1] - positions[nj, 1]) ** 2
                )
                for s, d in [(ni, nj), (nj, ni)]:
                    if (s, d) not in edge_set:
                        edge_set[(s, d)] = (dist, EDGE_TRANSMISSION)

    # ── Fix isolated nodes ──
    n_transmission = len(edge_set) // 2
    connected = set()
    for s, d in edge_set:
        connected.add(s)
        connected.add(d)
    isolated = [i for i in range(num_nodes) if i not in connected]

    node_tree = KDTree(positions)
    iteration = 0
    while isolated:
        iteration += 1
        new_isolated = []
        for iso_node in isolated:
            dists, idxs = node_tree.query(positions[iso_node], k=FALLBACK_K + 1)
            added = False
            for k_idx in range(1, FALLBACK_K + 1):
                neighbor = idxs[k_idx]
                dist = dists[k_idx]
                for s, d in [(iso_node, neighbor), (neighbor, iso_node)]:
                    if (s, d) not in edge_set:
                        edge_set[(s, d)] = (dist, EDGE_FALLBACK_KNN)
                connected.add(iso_node)
                added = True
            if not added:
                new_isolated.append(iso_node)
        isolated = new_isolated
        if iteration > 20:
            break

    n_fallback = (len(edge_set) // 2) - n_transmission

    # ── Add virtual super node ──
    virtual_idx = num_nodes  # index N

    # Use mean position as a sentinel (won't be plotted)
    for i in range(num_nodes):
        edge_set[(virtual_idx, i)] = (0.0, EDGE_VIRTUAL)
        edge_set[(i, virtual_idx)] = (0.0, EDGE_VIRTUAL)

    # ── Assemble tensors ──
    src_list, dst_list, dist_list, type_list = [], [], [], []
    for (s, d), (dist, etype) in edge_set.items():
        src_list.append(s)
        dst_list.append(d)
        dist_list.append(dist)
        type_list.append(etype)

    edge_index = torch.tensor([src_list, dst_list], dtype=torch.long)

    dist_arr = np.array(dist_list, dtype=np.float32)
    real_mask = np.array(type_list) != EDGE_VIRTUAL
    dist_max = dist_arr[real_mask].max() if real_mask.any() else 1.0
    dist_norm = np.where(real_mask, dist_arr / max(dist_max, 1e-8), 0.0)

    type_arr = np.array(type_list, dtype=np.float32)
    edge_attr = torch.tensor(
        np.stack([dist_norm, type_arr], axis=1), dtype=torch.float32,
    )

    print(f"  Transmission edges: {n_transmission}")
    print(f"  Fallback k-NN edges: {n_fallback}")
    print(f"  Virtual-node edges: {num_nodes} (bidirectional)")
    print(f"  Total edges: {edge_index.shape[1]}")

    return edge_index, edge_attr, node_order, positions, virtual_idx


# ── Feature Engineering ──────────────────────────────────────────

def _encode_cyclical(values, period):
    angle = 2 * np.pi * values / period
    return np.sin(angle), np.cos(angle)


def prepare_features(df, node_order, virtual_node_idx):
    """Build per-hour (X, y, mask) arrays.  Appends a virtual super node
    whose features are the system-wide mean of all valid real nodes.

    Returns:
        hours, hour_to_features, num_features
    """
    node_to_idx = {sp: i for i, sp in enumerate(node_order)}
    num_real = len(node_order)
    num_total = num_real + 1   # real nodes + virtual node

    df_f = df[df["settlement_point"].isin(node_to_idx)].copy()
    df_f["node_idx"] = df_f["settlement_point"].map(node_to_idx)

    for zone in WEATHER_ZONES:
        df_f[f"wz_{zone}"] = (df_f["weather_zone"] == zone).astype(np.float32)
    wz_cols = [f"wz_{z}" for z in WEATHER_ZONES]

    df_f["hod_sin"], df_f["hod_cos"] = _encode_cyclical(df_f["hour_of_day"], 24)
    df_f["wday_sin"], df_f["wday_cos"] = _encode_cyclical(df_f["weekday"], 7)
    df_f["mon_sin"], df_f["mon_cos"] = _encode_cyclical(df_f["month"], 12)
    cyclical_cols = [
        "hod_sin", "hod_cos", "wday_sin", "wday_cos", "mon_sin", "mon_cos",
    ]

    feature_cols = BASE_NUMERIC_COLS + wz_cols + cyclical_cols
    num_features = len(feature_cols)

    hours = sorted(df_f["hour"].unique())
    hour_to_features = {}

    grouped = df_f.groupby("hour")
    for hour, group in grouped:
        X = np.full((num_total, num_features), np.nan, dtype=np.float32)
        y = np.full(num_total, np.nan, dtype=np.float32)
        mask = np.zeros(num_total, dtype=bool)

        idxs = group["node_idx"].values
        X[idxs] = group[feature_cols].values.astype(np.float32)
        y[idxs] = group[TARGET_COL].values.astype(np.float32)
        mask[idxs] = True

        # Virtual node: system-wide mean of all valid real-node features
        valid = mask[:num_real]
        if valid.any():
            X[virtual_node_idx] = np.nanmean(X[:num_real][valid], axis=0)
        else:
            X[virtual_node_idx] = 0.0
        # Virtual node never has an LMP target
        mask[virtual_node_idx] = False

        hour_to_features[hour] = (X, y, mask)

    return hours, hour_to_features, num_features


def build_pyg_datasets(hours, hour_to_features, edge_index, edge_attr):
    train_hours, val_hours, test_hours = [], [], []
    for h in hours:
        day = h.day
        if TRAIN_DAYS[0] <= day <= TRAIN_DAYS[1]:
            train_hours.append(h)
        elif VAL_DAYS[0] <= day <= VAL_DAYS[1]:
            val_hours.append(h)
        else:
            test_hours.append(h)

    print(f"  Train hours: {len(train_hours)}, Val: {len(val_hours)}, "
          f"Test: {len(test_hours)}")

    train_y_all = np.concatenate([
        hour_to_features[h][1][hour_to_features[h][2]] for h in train_hours
    ])
    train_y_finite = train_y_all[np.isfinite(train_y_all)]
    lmp_lo = np.percentile(train_y_finite, LMP_CLIP_LO)
    lmp_hi = np.percentile(train_y_finite, LMP_CLIP_HI)
    print(f"  LMP clipping: [{lmp_lo:.1f}, {lmp_hi:.1f}] $/MWh")

    train_X_all = np.vstack([hour_to_features[h][0] for h in train_hours])
    scaler = StandardScaler()
    scaler.fit(np.nan_to_num(train_X_all, nan=0.0))

    def hours_to_data(hour_list):
        out = []
        for h in hour_list:
            X, y, mask = hour_to_features[h]
            X_s = scaler.transform(np.nan_to_num(X, nan=0.0)).astype(np.float32)
            y_c = np.clip(y, lmp_lo, lmp_hi)
            out.append(Data(
                x=torch.tensor(X_s), y=torch.tensor(y_c),
                edge_index=edge_index, edge_attr=edge_attr,
                mask=torch.tensor(mask),
            ))
        return out

    return (
        hours_to_data(train_hours), hours_to_data(val_hours),
        hours_to_data(test_hours), scaler, lmp_lo, lmp_hi,
    )


# ── Model ────────────────────────────────────────────────────────

class ERCOTGraphNet(nn.Module):
    """Residual GAT with edge features for node-level LMP prediction.

    edge_dim=2: [normalised geographic distance, edge-type indicator]
    """

    def __init__(self, in_channels, hidden=HIDDEN_DIM, heads=N_HEADS,
                 dropout=DROPOUT, mlp_hidden=MLP_HIDDEN, edge_dim=2):
        super().__init__()
        self.input_proj = nn.Linear(in_channels, hidden)

        self.gat1 = GATConv(hidden, hidden, heads=heads, dropout=dropout,
                            concat=True, edge_dim=edge_dim)
        self.bn1 = nn.BatchNorm1d(hidden * heads)
        self.proj1 = nn.Linear(hidden * heads, hidden)

        self.gat2 = GATConv(hidden, hidden, heads=heads, dropout=dropout,
                            concat=True, edge_dim=edge_dim)
        self.bn2 = nn.BatchNorm1d(hidden * heads)
        self.proj2 = nn.Linear(hidden * heads, hidden)

        self.gat3 = GATConv(hidden, hidden, heads=heads, dropout=dropout,
                            concat=False, edge_dim=edge_dim)
        self.bn3 = nn.BatchNorm1d(hidden)

        self.mlp = nn.Sequential(
            nn.Linear(hidden, mlp_hidden),
            nn.ELU(),
            nn.Dropout(dropout),
            nn.Linear(mlp_hidden, mlp_hidden // 2),
            nn.ELU(),
            nn.Dropout(dropout),
            nn.Linear(mlp_hidden // 2, 1),
        )
        self.dropout = dropout
        self._attn_weights = [None, None, None]

    def forward(self, data, return_attention=False):
        x, edge_index = data.x, data.edge_index
        edge_attr = data.edge_attr if hasattr(data, "edge_attr") else None

        x = F.elu(self.input_proj(x))
        residual = x

        out, a1 = self.gat1(x, edge_index, edge_attr=edge_attr,
                            return_attention_weights=True)
        self._attn_weights[0] = a1
        out = self.bn1(out)
        out = F.elu(self.proj1(out))
        out = F.dropout(out, p=self.dropout, training=self.training)
        x = out + residual
        residual = x

        out, a2 = self.gat2(x, edge_index, edge_attr=edge_attr,
                            return_attention_weights=True)
        self._attn_weights[1] = a2
        out = self.bn2(out)
        out = F.elu(self.proj2(out))
        out = F.dropout(out, p=self.dropout, training=self.training)
        x = out + residual
        residual = x

        out, a3 = self.gat3(x, edge_index, edge_attr=edge_attr,
                            return_attention_weights=True)
        self._attn_weights[2] = a3
        out = self.bn3(out)
        out = F.elu(out)
        out = F.dropout(out, p=self.dropout, training=self.training)
        x = out + residual

        return self.mlp(x).squeeze(-1)


# ── Training ─────────────────────────────────────────────────────

def train_model(model, train_loader, val_loader):
    optimizer = torch.optim.AdamW(
        model.parameters(), lr=LEARNING_RATE, weight_decay=WEIGHT_DECAY,
    )
    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(
        optimizer, T_max=MAX_EPOCHS, eta_min=1e-6,
    )
    huber = nn.SmoothL1Loss(beta=5.0)

    best_val_mae = float("inf")
    best_state = None
    patience_counter = 0
    history = {"train_mae": [], "val_mae": []}

    for epoch in range(MAX_EPOCHS):
        model.train()
        mae_sum, count = 0.0, 0
        for batch in train_loader:
            batch = batch.to(DEVICE)
            optimizer.zero_grad()
            pred = model(batch)
            mask = batch.mask
            pm, ym = pred[mask], batch.y[mask]
            loss = huber(pm, ym)
            if torch.isnan(loss):
                raise RuntimeError("NaN loss — check MPS fallback.")
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
            with torch.no_grad():
                mae_sum += F.l1_loss(pm, ym, reduction="sum").item()
                count += mask.sum().item()
        scheduler.step()

        model.eval()
        v_sum, v_cnt = 0.0, 0
        with torch.no_grad():
            for batch in val_loader:
                batch = batch.to(DEVICE)
                pred = model(batch)
                m = batch.mask
                v_sum += F.l1_loss(pred[m], batch.y[m], reduction="sum").item()
                v_cnt += m.sum().item()

        t_mae = mae_sum / max(count, 1)
        v_mae = v_sum / max(v_cnt, 1)
        history["train_mae"].append(t_mae)
        history["val_mae"].append(v_mae)

        improved = v_mae < best_val_mae
        if improved:
            best_val_mae = v_mae
            best_state = {k: v.cpu().clone()
                          for k, v in model.state_dict().items()}
            patience_counter = 0
        else:
            patience_counter += 1

        if epoch % 10 == 0 or improved:
            lr = scheduler.get_last_lr()[0]
            print(f"  Epoch {epoch:3d}: train={t_mae:.2f}  val={v_mae:.2f}"
                  f"  lr={lr:.2e}{'  *' if improved else ''}")

        if patience_counter >= PATIENCE:
            print(f"  Early stopping at epoch {epoch} "
                  f"(best val_mae={best_val_mae:.2f})")
            break

    if best_state is not None:
        model.load_state_dict(best_state)
    return model, history


# ── Evaluation ───────────────────────────────────────────────────

def evaluate_model(model, test_loader, node_order):
    """Metrics for real nodes only (virtual node excluded by mask)."""
    model.eval()
    predictions = []
    n = len(node_order)
    node_errors = {i: [] for i in range(n)}

    with torch.no_grad():
        for batch in test_loader:
            batch = batch.to(DEVICE)
            pred = model(batch).cpu().numpy()
            y = batch.y.cpu().numpy()
            mask = batch.mask.cpu().numpy()
            predictions.append((pred, y, mask))
            for i in range(n):
                if mask[i]:
                    node_errors[i].append(pred[i] - y[i])

    all_p = np.concatenate([p[m] for p, _, m in predictions])
    all_t = np.concatenate([t[m] for _, t, m in predictions])

    mae = np.mean(np.abs(all_p - all_t))
    rmse = np.sqrt(np.mean((all_p - all_t) ** 2))
    ss_res = np.sum((all_t - all_p) ** 2)
    ss_tot = np.sum((all_t - np.mean(all_t)) ** 2)
    r2 = 1.0 - ss_res / max(ss_tot, 1e-8)

    rows = []
    for i, sp in enumerate(node_order):
        errs = node_errors[i]
        if not errs:
            continue
        errs = np.array(errs)
        rows.append({
            "settlement_point": sp,
            "node_mae": np.mean(np.abs(errs)),
            "node_rmse": np.sqrt(np.mean(errs ** 2)),
            "node_bias": np.mean(errs),
            "n_obs": len(errs),
        })

    return {"mae": mae, "rmse": rmse, "r2": r2}, pd.DataFrame(rows), predictions


# ── Vulnerability & Attention ────────────────────────────────────

def compute_vulnerability_scores(model, test_data, node_order):
    """Gradient saliency for real nodes only."""
    model.eval()
    n = len(node_order)
    grad_acc = np.zeros((n, len(ERROR_FEATURE_INDICES)), dtype=np.float64)
    cnt = np.zeros(n, dtype=np.int64)

    for d in test_data:
        d = d.to(DEVICE)
        x = d.x.detach().clone().requires_grad_(True)
        dd = Data(x=x, edge_index=d.edge_index,
                  edge_attr=d.edge_attr, mask=d.mask)
        pred = model(dd)
        pred[dd.mask].sum().backward()
        g = x.grad.cpu().numpy()
        for i in range(n):
            if dd.mask[i]:
                grad_acc[i] += np.abs(g[i, ERROR_FEATURE_INDICES])
                cnt[i] += 1

    avg = grad_acc / np.maximum(cnt[:, None], 1)
    vdf = pd.DataFrame({"settlement_point": node_order})
    for j, nm in enumerate(ERROR_FEATURE_NAMES):
        vdf[f"grad_{nm}"] = avg[:, j]
    vdf["vulnerability_score"] = vdf[
        [f"grad_{nm}" for nm in ERROR_FEATURE_NAMES]
    ].sum(axis=1)
    return vdf


def extract_attention_weights(model, test_data, node_order, virtual_idx):
    """Aggregate final-layer attention, excluding virtual-node and self-loop
    edges from the output."""
    model.eval()
    exp_ei = None
    acc = None
    count = 0

    with torch.no_grad():
        for d in test_data:
            d = d.to(DEVICE)
            model(d, return_attention=True)
            ei, av = model._attn_weights[-1]
            if av.dim() > 1:
                av = av.mean(dim=-1)
            if exp_ei is None:
                exp_ei = ei.cpu()
                acc = np.zeros(exp_ei.shape[1], dtype=np.float64)
            acc += av.cpu().numpy()
            count += 1

    mean_a = acc / max(count, 1)
    src = exp_ei[0].numpy()
    dst = exp_ei[1].numpy()

    # Keep only real→real edges (no self-loops, no virtual node)
    keep = (src != dst) & (src != virtual_idx) & (dst != virtual_idx)
    sf, df_, af = src[keep], dst[keep], mean_a[keep]

    edf = pd.DataFrame({
        "src_idx": sf, "dst_idx": df_,
        "src": [node_order[i] for i in sf],
        "dst": [node_order[i] for i in df_],
        "mean_attention": af,
    })

    inc = edf.groupby("dst_idx")["mean_attention"].mean()
    ndf = pd.DataFrame({
        "settlement_point": node_order,
        "incoming_attention_mean": [inc.get(i, 0.0)
                                    for i in range(len(node_order))],
    })
    return edf, ndf


# ── Visualization ────────────────────────────────────────────────

def _draw_texas(ax, proj):
    shp = shpreader.natural_earth(
        resolution="10m", category="cultural",
        name="admin_1_states_provinces",
    )
    for rec in shpreader.Reader(shp).records():
        if rec.attributes.get("name") == "Texas":
            ax.add_geometries([rec.geometry], proj,
                              facecolor="#f0f0f0", edgecolor="black",
                              linewidth=1.0)
            break
    ax.set_extent([-107.5, -93.0, 25.5, 37.0], crs=proj)


def plot_graph_topology(edge_index, edge_attr, positions, node_order,
                        virtual_idx, save_path):
    """Draw the transmission-guided graph on a Texas map.

    Colors: orange = transmission, green = fallback k-NN.  Virtual-node
    edges are omitted (they connect to every node).
    """
    proj = ccrs.PlateCarree()
    fig, ax = plt.subplots(figsize=(14, 11), subplot_kw={"projection": proj})
    _draw_texas(ax, proj)

    src = edge_index[0].numpy()
    dst = edge_index[1].numpy()
    etypes = edge_attr[:, 1].numpy()

    color_map = {EDGE_TRANSMISSION: "#e67e22", EDGE_FALLBACK_KNN: "#27ae60"}
    label_map = {EDGE_TRANSMISSION: "Transmission line",
                 EDGE_FALLBACK_KNN: "Fallback k-NN"}
    drawn = set()
    handles = {}

    for idx in range(len(src)):
        s, d = int(src[idx]), int(dst[idx])
        et = int(etypes[idx])
        if et == EDGE_VIRTUAL:
            continue
        if s >= len(positions) or d >= len(positions):
            continue
        pair = (min(s, d), max(s, d))
        if pair in drawn:
            continue
        drawn.add(pair)

        c = color_map.get(et, "gray")
        lw = 0.6 if et == EDGE_TRANSMISSION else 0.8
        line, = ax.plot(
            [positions[s, 1], positions[d, 1]],
            [positions[s, 0], positions[d, 0]],
            color=c, linewidth=lw, alpha=0.5, transform=proj, zorder=3,
        )
        if et not in handles:
            handles[et] = line

    ax.scatter(
        positions[:, 1], positions[:, 0],
        c="red", s=18, edgecolors="k", linewidths=0.3,
        alpha=0.8, transform=proj, zorder=5,
    )

    legend_handles = [handles[k] for k in sorted(handles)]
    legend_labels = [label_map[k] for k in sorted(handles)]
    ax.legend(legend_handles, legend_labels, loc="lower left", fontsize=9)

    n_tx = sum(1 for e in etypes if e == EDGE_TRANSMISSION) // 2
    n_fb = sum(1 for e in etypes if e == EDGE_FALLBACK_KNN) // 2
    ax.set_title(
        f"Transmission-Guided Graph — {len(node_order)} nodes, "
        f"{n_tx} transmission + {n_fb} fallback edges + virtual super node",
        fontsize=12,
    )
    ax.gridlines(draw_labels=True, linewidth=0.3, alpha=0.5)
    plt.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {save_path}")


def plot_training_curves(history, save_path):
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(history["train_mae"], label="Train MAE", color="steelblue")
    ax.plot(history["val_mae"], label="Val MAE", color="darkorange")
    ax.set_xlabel("Epoch")
    ax.set_ylabel("MAE ($/MWh)")
    ax.set_title("Training Curves — Transmission GAT (v3)")
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {save_path}")


def plot_pred_vs_actual(predictions, save_path):
    ap = np.concatenate([p[m] for p, _, m in predictions])
    at = np.concatenate([t[m] for _, t, m in predictions])
    fig, ax = plt.subplots(figsize=(7, 7))
    ax.scatter(at, ap, s=1, alpha=0.1, color="steelblue")
    lo, hi = min(at.min(), ap.min()), max(at.max(), ap.max())
    ax.plot([lo, hi], [lo, hi], "k--", linewidth=0.8, alpha=0.6)
    ax.set_xlabel("Actual LMP ($/MWh)")
    ax.set_ylabel("Predicted LMP ($/MWh)")
    ax.set_title("Predicted vs Actual LMP — Test Set")
    ax.set_aspect("equal")
    plt.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {save_path}")


def plot_node_map(node_df, node_coords, save_path, color_col, cmap,
                  cbar_label, title):
    merged = node_df.merge(
        node_coords[["settlement_point", "lat", "lon"]],
        on="settlement_point", how="inner",
    )
    proj = ccrs.PlateCarree()
    fig, ax = plt.subplots(figsize=(12, 10), subplot_kw={"projection": proj})
    _draw_texas(ax, proj)
    sc = ax.scatter(
        merged["lon"], merged["lat"], c=merged[color_col],
        cmap=cmap, s=40, edgecolors="k", linewidths=0.3,
        alpha=0.85, transform=proj, zorder=5,
    )
    plt.colorbar(sc, ax=ax, shrink=0.7, pad=0.02).set_label(
        cbar_label, fontsize=10,
    )
    ax.set_title(title, fontsize=13)
    ax.gridlines(draw_labels=True, linewidth=0.3, alpha=0.5)
    plt.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {save_path}")


def plot_attention_network(edf, node_coords, node_order, save_path, top_k=200):
    pos = node_coords.set_index("settlement_point")[["lat", "lon"]]
    top = edf.nlargest(top_k, "mean_attention")
    proj = ccrs.PlateCarree()
    fig, ax = plt.subplots(figsize=(12, 10), subplot_kw={"projection": proj})
    _draw_texas(ax, proj)
    mx = top["mean_attention"].max()
    for _, r in top.iterrows():
        if r["src"] not in pos.index or r["dst"] not in pos.index:
            continue
        sp, dp = pos.loc[r["src"]], pos.loc[r["dst"]]
        a = 0.3 + 0.7 * (r["mean_attention"] / max(mx, 1e-8))
        ax.plot([sp["lon"], dp["lon"]], [sp["lat"], dp["lat"]],
                color="steelblue", linewidth=1.0, alpha=a,
                transform=proj, zorder=3)
    lats = [pos.loc[s, "lat"] if s in pos.index else np.nan for s in node_order]
    lons = [pos.loc[s, "lon"] if s in pos.index else np.nan for s in node_order]
    ax.scatter(lons, lats, c="red", s=15, edgecolors="k", linewidths=0.2,
               alpha=0.7, transform=proj, zorder=5)
    ax.set_title(f"Top {top_k} GAT Attention Edges — Information Flow",
                 fontsize=13)
    ax.gridlines(draw_labels=True, linewidth=0.3, alpha=0.5)
    plt.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {save_path}")


# ── Main ─────────────────────────────────────────────────────────

def main():
    torch.manual_seed(SEED)
    np.random.seed(SEED)

    print("=" * 60)
    print("Loading data...")
    df, node_coords, dirs = load_data()

    out_dir = os.path.join(dirs["figures"], "node_gnn")
    results_dir = os.path.join(out_dir, "results")
    os.makedirs(results_dir, exist_ok=True)

    # ── Build transmission-guided graph ──
    print("\nBuilding transmission-guided graph...")
    nodes_with_data = set(df["settlement_point"].unique())
    coords_with_data = node_coords[
        node_coords["settlement_point"].isin(nodes_with_data)
    ].reset_index(drop=True)

    edge_index, edge_attr, node_order, positions, virtual_idx = \
        build_transmission_graph(coords_with_data)

    plot_graph_topology(
        edge_index, edge_attr, positions, node_order, virtual_idx,
        os.path.join(out_dir, "graph_topology.png"),
    )

    # ── Prepare features ──
    print("\nPreparing features...")
    hours, hour_to_features, num_features = prepare_features(
        df, node_order, virtual_idx,
    )
    print(f"  Hours: {len(hours)}, Features/node: {num_features}")
    print(f"  Real nodes: {len(node_order)}, Virtual node idx: {virtual_idx}")

    # ── Build PyG datasets ──
    print("\nBuilding PyG datasets (intra-month split)...")
    train_data, val_data, test_data, scaler, lmp_lo, lmp_hi = \
        build_pyg_datasets(hours, hour_to_features, edge_index, edge_attr)

    train_loader = DataLoader(train_data, batch_size=BATCH_SIZE, shuffle=True)
    val_loader = DataLoader(val_data, batch_size=BATCH_SIZE, shuffle=False)
    test_loader = DataLoader(test_data, batch_size=1, shuffle=False)

    # ── Model ──
    print(f"\nDevice: {DEVICE}")
    if DEVICE.type == "mps":
        print("  Apple Silicon GPU (MPS) — sparse ops fall back to CPU")
    model = ERCOTGraphNet(in_channels=num_features).to(DEVICE)
    n_params = sum(p.numel() for p in model.parameters())
    print(f"Model: {n_params:,} parameters")
    print(f"  {N_GAT_LAYERS}-layer residual GAT ({N_HEADS} heads, "
          f"hidden={HIDDEN_DIM}), edge_dim=2")

    # ── Train ──
    print("\nTraining...")
    model, history = train_model(model, train_loader, val_loader)

    print("\nGenerating plots...")
    plot_training_curves(history, os.path.join(out_dir, "training_curves.png"))

    # ── Evaluate ──
    print("\nEvaluating on test set (days 26+ of each month)...")
    metrics, node_metrics, predictions = evaluate_model(
        model, test_loader, node_order,
    )
    print(f"  MAE:  {metrics['mae']:.2f} $/MWh")
    print(f"  RMSE: {metrics['rmse']:.2f} $/MWh")
    print(f"  R2:   {metrics['r2']:.4f}")

    plot_pred_vs_actual(predictions, os.path.join(out_dir, "pred_vs_actual.png"))
    plot_node_map(node_metrics, coords_with_data,
                  os.path.join(out_dir, "node_mae_map.png"),
                  "node_mae", "YlOrRd", "MAE ($/MWh)",
                  "Per-Node Prediction MAE — Test Set")

    # ── Vulnerability ──
    print("\nComputing vulnerability scores...")
    vuln_df = compute_vulnerability_scores(model, test_data, node_order)
    plot_node_map(vuln_df, coords_with_data,
                  os.path.join(out_dir, "vulnerability_map.png"),
                  "vulnerability_score", "hot_r",
                  "Vulnerability (sum |dLMP/d(error)|)",
                  "Node Vulnerability to Forecast Errors")

    top20 = vuln_df.nlargest(20, "vulnerability_score")
    print("\nTop 20 Most Vulnerable Nodes:")
    print(top20[["settlement_point", "vulnerability_score",
                  "grad_temp_error_1h", "grad_wspd_error_1h"]].to_string(
        index=False))

    # ── Attention ──
    print("\nExtracting attention weights...")
    edge_attn, node_inf = extract_attention_weights(
        model, test_data, node_order, virtual_idx,
    )
    plot_attention_network(edge_attn, coords_with_data, node_order,
                           os.path.join(out_dir, "attention_network.png"))

    # ── Save ──
    node_metrics.to_csv(os.path.join(results_dir, "node_test_metrics.csv"),
                        index=False)
    vuln_df.to_csv(os.path.join(results_dir, "vulnerability_scores.csv"),
                   index=False)
    edge_attn.to_csv(os.path.join(results_dir, "edge_attention.csv"),
                     index=False)
    node_inf.to_csv(os.path.join(results_dir, "node_influence.csv"),
                    index=False)
    torch.save({
        "model_state_dict": model.state_dict(),
        "scaler_mean": scaler.mean_,
        "scaler_scale": scaler.scale_,
        "node_order": node_order,
        "metrics": metrics,
        "lmp_clip": (lmp_lo, lmp_hi),
        "virtual_node_idx": virtual_idx,
        "config": {
            "HIDDEN_DIM": HIDDEN_DIM, "N_HEADS": N_HEADS,
            "N_GAT_LAYERS": N_GAT_LAYERS, "DROPOUT": DROPOUT,
        },
    }, os.path.join(results_dir, "model_checkpoint.pt"))

    print(f"\nAll outputs saved to {out_dir}")
    print("Done.")


if __name__ == "__main__":
    main()
