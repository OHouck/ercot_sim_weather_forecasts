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

import glob
import os
import re
import sys
import warnings
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
# Uses combined HRRR (1h short-range) + GFS (day-ahead, lead=0) by default
ERROR_SOURCE = "era5"
MONTHS = [(2025, m) for m in range(1, 13)]

FALLBACK_K = 3            # k-NN for isolated nodes
HIDDEN_DIM = 128
N_HEADS = 4
N_GAT_LAYERS = 2          # 2 layers suffice (virtual node = diameter-2)
DROPOUT = 0.3             # v4: increased from 0.2 to reduce overfitting
MLP_HIDDEN = 64

TRAIN_DAYS = (1, 15)
VAL_DAYS = (16, 25)
TEST_DAYS = (26, 31)

BATCH_SIZE = 64
LEARNING_RATE = 1e-3
WEIGHT_DECAY = 1e-4
MAX_EPOCHS = 200
PATIENCE = 40             # v4: increased from 20 (val curve is noisy)
WARMUP_EPOCHS = 5
NODE_EMBED_DIM = 8        # v5: learnable per-node embedding dimension

dirs = setup_directories()
_GIS_ROOT = os.path.join(dirs["root"], "Texas_GIS_Data")
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
    "temp_error_1h",       # 2  HRRR short-range
    "wspd_error_1h",       # 3  HRRR short-range
    "temp_error_0h",       # 4  GFS day-ahead
    "wspd_error_0h",       # 5  GFS day-ahead
    "forecast_temp_1h",    # 6
    "forecast_wspd_1h",    # 7
    "actual_load",         # 8
    "forecast_load_1h",    # 9
    "load_error_1h",       # 10
    "lat",                 # 11
    "lon",                 # 12
]
TARGET_COL = "lmp"

ERROR_FEATURE_INDICES = [2, 3, 4, 5]
ERROR_FEATURE_NAMES = [
    "temp_error_1h", "wspd_error_1h",   # HRRR short-range
    "temp_error_0h", "wspd_error_0h",   # GFS day-ahead
]

LMP_CLIP_LO = 1
LMP_CLIP_HI = 99

# Edge type constants (stored as edge_attr feature 2)
EDGE_TRANSMISSION = 0
EDGE_FALLBACK_KNN = 1
EDGE_VIRTUAL = 2

# Technology types for node classification
TECH_TYPES = ["wind", "solar", "gas", "storage", "other"]

_WIND_RE = re.compile(r"WIND|WND|_W\d|TURBINE|TURBN", re.I)
_SOLAR_RE = re.compile(r"SLR|SOLAR|_PV|SOLR", re.I)
_BESS_RE = re.compile(r"ESR\d?|ESS\d?|BESS|BATTERY", re.I)
_GAS_RE = re.compile(r"_GT\d|_CT\d|_CC\d|_ST\d|CCGT|GAS|STEAM|_STG", re.I)


def _classify_node_tech(node_order):
    """Classify settlement-point nodes by technology from NP4-160 unit names."""
    rn_files = glob.glob(
        os.path.join(dirs["raw"], "ercot", "np4_160", "Resource_Node_to_Unit_*.csv")
    )
    unit_map = {}
    if rn_files:
        rn = pd.read_csv(rn_files[0])
        unit_map = rn.groupby("RESOURCE_NODE")["UNIT_NAME"].apply(" ".join).to_dict()

    tech = {}
    for sp in node_order:
        combined = sp + " " + unit_map.get(sp, "")
        if _WIND_RE.search(combined):
            tech[sp] = "wind"
        elif _SOLAR_RE.search(combined):
            tech[sp] = "solar"
        elif _BESS_RE.search(combined):
            tech[sp] = "storage"
        elif _GAS_RE.search(combined):
            tech[sp] = "gas"
        else:
            tech[sp] = "other"
    return tech


# ── Data Loading ─────────────────────────────────────────────────

def load_data():
    dirs = setup_directories()
    df = prepare_node_level_data(
        months=MONTHS, error_source=ERROR_SOURCE,
    )
    df["hour"] = pd.to_datetime(df["hour"])
    node_coords = pd.read_csv(
        os.path.join(dirs["processed"], "node_coordinates.csv")
    )
    node_coords = node_coords.dropna(subset=["lat", "lon"])
    return df, node_coords, dirs


# ── Graph Construction ───────────────────────────────────────────

def build_transmission_graph(node_coords):
    """Build a settlement-node graph via simulation-bus contraction.

    Algorithm:
      1. Map each settlement-point node to its nearest simulation bus.
      2. Build the full simulation transmission graph (all buses + lines).
      3. Iteratively remove simulation buses that have no mapped nodes,
         rewiring their edges directly between their neighbours (i.e. path
         contraction).  One pass through the initially-empty buses suffices
         because contractions are processed in-place — each bus's neighbour
         list already reflects earlier removals in the same pass.
      4. The result is a backbone graph where every bus has ≥ 1 mapped node.
      5. Connect settlement-point nodes whose backbone buses share an edge,
         plus all nodes that share the same backbone bus (intra-bus edges).
      6. Add a virtual super node connected to every real node.

    Returns:
        edge_index:       (2, E) LongTensor  — includes virtual-node edges
        edge_attr:        (E, 2) FloatTensor — [normalised distance, edge type]
        node_order:       list of settlement_point names (length N)
        positions:        (N, 2) ndarray of (lat, lon) for real nodes
        virtual_node_idx: int — index of the virtual super node (= N)
        backbone_bus_pos: (M, 2) ndarray of (lat, lon) for backbone buses
        backbone_edges:   list of (lat1, lon1, lat2, lon2) for backbone edges
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
    node_to_bus = bus_numbers[nearest_bus_idx]

    bus_to_nodes = {}
    for i, bus_num in enumerate(node_to_bus):
        bus_to_nodes.setdefault(int(bus_num), []).append(i)

    # ── Build full simulation adjacency (bus_num → set of neighbour bus_nums) ──
    all_bus_nums = set(int(b) for b in bus_numbers)
    adj = {b: set() for b in all_bus_nums}
    for _, line in lines.iterrows():
        fbus, tbus = int(line["FBus_Num"]), int(line["TBus_Num"])
        if fbus != tbus and fbus in adj and tbus in adj:
            adj[fbus].add(tbus)
            adj[tbus].add(fbus)

    print(f"  Simulation graph: {len(adj)} buses, "
          f"{sum(len(v) for v in adj.values()) // 2} lines")

    # ── Contract empty buses (single pass is sufficient) ──
    # Process every bus that has no mapped settlement node.  For each one,
    # add direct edges between all its neighbours (so paths through it are
    # preserved), then remove it.  Processing order doesn't matter because
    # adj is modified in place — a later bus sees the already-updated graph.
    empty_buses = [b for b in list(adj) if b not in bus_to_nodes]
    for bus in empty_buses:
        if bus not in adj:
            continue   # already removed as a neighbour in an earlier step
        neighbours = list(adj[bus])
        for nb in neighbours:
            adj[nb].discard(bus)
            adj[nb].update(n for n in neighbours if n != nb)
        del adj[bus]

    backbone_buses = set(adj)
    print(f"  Backbone graph:   {len(backbone_buses)} buses, "
          f"{sum(len(v) for v in adj.values()) // 2} edges "
          f"(contracted {len(all_bus_nums) - len(backbone_buses)} empty buses)")

    # ── Build edge set for GNN graph ──
    edge_set = {}

    # Cross-bus edges: nodes whose backbone buses are adjacent
    for bus, neighbours in adj.items():
        fnodes = bus_to_nodes.get(bus, [])
        for nb in neighbours:
            if nb <= bus:
                continue   # process each undirected pair once
            tnodes = bus_to_nodes.get(nb, [])
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

    # Intra-bus edges: nodes sharing the same backbone bus
    for bus, node_list in bus_to_nodes.items():
        if bus not in backbone_buses:
            continue
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

    # ── Virtual super node ──
    virtual_idx = num_nodes
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

    print(f"  Virtual-node edges: {num_nodes} (bidirectional)")
    print(f"  Total edges: {edge_index.shape[1]}")

    # ── Backbone visualisation data ──
    bus_latlon = {int(row["Bus_Number"]): (row["lat"], row["lon"])
                  for _, row in bus_positions.iterrows()}
    backbone_bus_pos = np.array(
        [bus_latlon[b] for b in sorted(backbone_buses) if b in bus_latlon]
    )
    backbone_edges = [
        (bus_latlon[a][0], bus_latlon[a][1], bus_latlon[b][0], bus_latlon[b][1])
        for a, nbs in adj.items() for b in nbs if a < b
        if a in bus_latlon and b in bus_latlon
    ]

    return (edge_index, edge_attr, node_order, positions, virtual_idx,
            backbone_bus_pos, backbone_edges)


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

    # Weather zone one-hot
    for zone in WEATHER_ZONES:
        df_f[f"wz_{zone}"] = (df_f["weather_zone"] == zone).astype(np.float32)
    wz_cols = [f"wz_{z}" for z in WEATHER_ZONES]

    # Technology type one-hot (v4)
    tech_map = _classify_node_tech(node_order)
    tech_counts = pd.Series(tech_map).value_counts()
    print(f"  Node tech: {tech_counts.to_dict()}")
    for tt in TECH_TYPES:
        df_f[f"tech_{tt}"] = (
            df_f["settlement_point"].map(tech_map) == tt
        ).astype(np.float32)
    tech_cols = [f"tech_{tt}" for tt in TECH_TYPES]

    # Interaction features: forecast error × technology (v4)
    df_f["wspd_err_1h_x_wind"] = df_f["wspd_error_1h"] * df_f["tech_wind"]
    df_f["temp_err_1h_x_solar"] = df_f["temp_error_1h"] * df_f["tech_solar"]
    interaction_cols = [
        "wspd_err_1h_x_wind", 
        "temp_err_1h_x_solar", 
    ]

    # System-wide hourly aggregates (v5) — same value for all nodes in an
    # hour, giving the model context about the overall system state.
    sys_agg = df_f.groupby("hour").agg(
        sys_mean_temp_err_1h=("temp_error_1h", "mean"),
        sys_mean_wspd_err_1h=("wspd_error_1h", "mean"),
        sys_mean_load=("actual_load", "mean"),
    ).reset_index()
    df_f = df_f.merge(sys_agg, on="hour", how="left")
    sys_cols = list(sys_agg.columns.drop("hour"))
    print(f"  System-wide features: {sys_cols}")

    # Cyclical time features
    df_f["hod_sin"], df_f["hod_cos"] = _encode_cyclical(df_f["hour_of_day"], 24)
    df_f["wday_sin"], df_f["wday_cos"] = _encode_cyclical(df_f["weekday"], 7)
    df_f["mon_sin"], df_f["mon_cos"] = _encode_cyclical(df_f["month"], 12)
    cyclical_cols = [
        "hod_sin", "hod_cos", "wday_sin", "wday_cos", "mon_sin", "mon_cos",
    ]

    feature_cols = (BASE_NUMERIC_COLS + wz_cols + tech_cols
                    + interaction_cols + sys_cols + cyclical_cols)
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
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", RuntimeWarning)
                vn_feats = np.nanmean(X[:num_real][valid], axis=0)
            X[virtual_node_idx] = np.nan_to_num(vn_feats, nan=0.0)
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

    # ── Per-node mean LMP from training data (for target demeaning) ──
    num_total = hour_to_features[hours[0]][0].shape[0]
    node_lmp_sum = np.zeros(num_total, dtype=np.float64)
    node_lmp_cnt = np.zeros(num_total, dtype=np.int64)
    for h in train_hours:
        _, y, mask = hour_to_features[h]
        valid = mask & np.isfinite(y)
        node_lmp_sum[valid] += y[valid]
        node_lmp_cnt[valid] += 1
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        node_lmp_mean = np.where(
            node_lmp_cnt > 0, node_lmp_sum / node_lmp_cnt, 0.0,
        ).astype(np.float32)
    n_real = num_total - 1
    print(f"  Node LMP mean range: [{node_lmp_mean[:n_real].min():.1f}, "
          f"{node_lmp_mean[:n_real].max():.1f}] $/MWh")

    # ── Clip bounds on demeaned LMP ──
    train_y_demeaned = np.concatenate([
        (hour_to_features[h][1] - node_lmp_mean)[hour_to_features[h][2]]
        for h in train_hours
    ])
    train_y_finite = train_y_demeaned[np.isfinite(train_y_demeaned)]
    lmp_lo = np.percentile(train_y_finite, LMP_CLIP_LO)
    lmp_hi = np.percentile(train_y_finite, LMP_CLIP_HI)
    print(f"  Demeaned LMP clipping: [{lmp_lo:.1f}, {lmp_hi:.1f}] $/MWh")

    # ── Feature scaler (fit on training X) ──
    train_X_all = np.vstack([hour_to_features[h][0] for h in train_hours])
    scaler = StandardScaler()
    scaler.fit(np.nan_to_num(train_X_all, nan=0.0))

    node_lmp_mean_t = torch.tensor(node_lmp_mean)

    def hours_to_data(hour_list):
        out = []
        for h in hour_list:
            X, y, mask = hour_to_features[h]
            X_s = scaler.transform(np.nan_to_num(X, nan=0.0)).astype(np.float32)
            y_dm = np.clip(y - node_lmp_mean, lmp_lo, lmp_hi)
            out.append(Data(
                x=torch.tensor(X_s), y=torch.tensor(y_dm),
                edge_index=edge_index, edge_attr=edge_attr,
                mask=torch.tensor(mask),
                node_lmp_mean=node_lmp_mean_t,
            ))
        return out

    return (
        hours_to_data(train_hours), hours_to_data(val_hours),
        hours_to_data(test_hours), scaler, lmp_lo, lmp_hi,
        node_lmp_mean,
    )


# ── Model ────────────────────────────────────────────────────────

class ERCOTGraphNet(nn.Module):
    """2-layer residual GAT with learnable node embeddings.

    v5 additions:
      - Learnable node embeddings (nn.Parameter): each node (including the
        virtual super node) gets a trainable vector that captures its unique
        identity — congestion patterns, local generation mix, typical price
        regime, etc.  Concatenated with input features before the input
        projection, so the GAT layers see both time-varying features and
        static node identity.  Dropout is applied to embeddings during
        training to prevent memorisation of node-specific biases.
      - System-wide hourly features are now part of the input feature vector.

    edge_dim=2: [normalised geographic distance, edge-type indicator]
    """

    def __init__(self, in_channels, num_nodes, embed_dim=NODE_EMBED_DIM,
                 hidden=HIDDEN_DIM, heads=N_HEADS, dropout=DROPOUT,
                 mlp_hidden=MLP_HIDDEN, edge_dim=2):
        super().__init__()
        # Learnable per-node embedding (includes virtual node)
        self.node_embed = nn.Parameter(
            torch.randn(num_nodes, embed_dim) * 0.02
        )
        self.embed_drop = nn.Dropout(dropout)
        self.input_proj = nn.Linear(in_channels + embed_dim, hidden)

        self.gat1 = GATConv(hidden, hidden, heads=heads, dropout=dropout,
                            concat=True, edge_dim=edge_dim)
        self.bn1 = nn.BatchNorm1d(hidden * heads)
        self.proj1 = nn.Linear(hidden * heads, hidden)

        self.gat2 = GATConv(hidden, hidden, heads=heads, dropout=dropout,
                            concat=False, edge_dim=edge_dim)
        self.bn2 = nn.BatchNorm1d(hidden)

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
        self._attn_weights = [None, None]

    def forward(self, data, return_attention=False):
        x, edge_index = data.x, data.edge_index
        edge_attr = data.edge_attr if hasattr(data, "edge_attr") else None

        # Concatenate learnable node embeddings with input features
        n = self.node_embed.shape[0]
        B = x.shape[0] // n
        embeds = self.embed_drop(self.node_embed.repeat(B, 1))
        x = torch.cat([x, embeds], dim=1)

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
        out = F.elu(out)
        out = F.dropout(out, p=self.dropout, training=self.training)
        x = out + residual

        return self.mlp(x).squeeze(-1)


# ── Training ─────────────────────────────────────────────────────

def train_model(model, train_loader, val_loader):
    optimizer = torch.optim.AdamW(
        model.parameters(), lr=LEARNING_RATE, weight_decay=WEIGHT_DECAY,
    )
    # v4: ReduceLROnPlateau with warmup — more adaptive than cosine
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
        optimizer, mode="min", factor=0.5, patience=10, min_lr=1e-6,
    )
    huber = nn.SmoothL1Loss(beta=5.0)

    best_val_mae = float("inf")
    best_state = None
    patience_counter = 0
    history = {"train_mae": [], "val_mae": []}

    for epoch in range(MAX_EPOCHS):
        # Warmup: linearly ramp LR for first few epochs
        if epoch < WARMUP_EPOCHS:
            warmup_lr = LEARNING_RATE * (epoch + 1) / WARMUP_EPOCHS
            for pg in optimizer.param_groups:
                pg["lr"] = warmup_lr

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

        # Step scheduler after warmup
        if epoch >= WARMUP_EPOCHS:
            scheduler.step(v_mae)

        improved = v_mae < best_val_mae
        if improved:
            best_val_mae = v_mae
            best_state = {k: v.cpu().clone()
                          for k, v in model.state_dict().items()}
            patience_counter = 0
        else:
            patience_counter += 1

        if epoch % 10 == 0 or improved:
            lr = optimizer.param_groups[0]["lr"]
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
    """Metrics for real nodes only (virtual node excluded by mask).

    v4: un-demeans predictions and targets to report metrics in original
    $/MWh scale.
    """
    model.eval()
    predictions = []
    n = len(node_order)
    node_errors = {i: [] for i in range(n)}

    with torch.no_grad():
        for batch in test_loader:
            batch = batch.to(DEVICE)
            pred_dm = model(batch).cpu().numpy()
            y_dm = batch.y.cpu().numpy()
            mask = batch.mask.cpu().numpy()
            nm = batch.node_lmp_mean.cpu().numpy()
            # Un-demean to original scale
            pred = pred_dm + nm
            y = y_dm + nm
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
                        virtual_idx, save_path,
                        backbone_bus_pos=None, backbone_edges=None):
    """Draw the transmission-guided graph on a Texas map.

    Layer order (back → front):
      1. Light gray  — full simulation lines + buses (all shapefiles)
      2. Steel blue  — backbone buses and edges after empty-bus contraction
      3. Orange      — GNN graph edges (settlement nodes connected via backbone)
      4. Red dots    — settlement-point nodes
    Virtual-node edges are omitted.
    """
    proj = ccrs.PlateCarree()
    fig, ax = plt.subplots(figsize=(14, 11), subplot_kw={"projection": proj})
    _draw_texas(ax, proj)

    # ── Simulation buses and lines (background layer) ──
    sim_lines = gpd.read_file(LINE_SHP)
    sim_buses = gpd.read_file(BUS_SHP)

    # Reproject to WGS84 using the .prj sidecar so PlateCarree renders correctly
    if sim_lines.crs is not None:
        sim_lines = sim_lines.to_crs(epsg=4326)

    sim_line_handle = None
    for geom in sim_lines.geometry:
        if geom is None or geom.is_empty:
            continue
        parts = list(geom.geoms) if geom.geom_type == "MultiLineString" else [geom]
        for part in parts:
            lons, lats = part.xy
            h, = ax.plot(
                list(lons), list(lats),
                color="#666666", linewidth=1.0, alpha=0.6,
                transform=proj, zorder=2,
            )
            if sim_line_handle is None:
                sim_line_handle = h

    # Buses use the explicit lat/lon attribute columns (always WGS84 degrees)
    bus_lats = pd.to_numeric(sim_buses["Bus_latitu"], errors="coerce").values
    bus_lons = pd.to_numeric(sim_buses["Bus_longit"], errors="coerce").values
    valid = np.isfinite(bus_lats) & np.isfinite(bus_lons)
    sim_bus_scatter = ax.scatter(
        bus_lons[valid], bus_lats[valid],
        c="#333333", s=30, marker="D", edgecolors="none",
        alpha=0.9, transform=proj, zorder=3,
    )

    # ── Backbone buses and edges (mid layer) ──
    bb_line_handle = None
    bb_bus_scatter = None
    if backbone_edges is not None:
        for lat1, lon1, lat2, lon2 in backbone_edges:
            h, = ax.plot(
                [lon1, lon2], [lat1, lat2],
                color="steelblue", linewidth=1.4, alpha=0.75,
                transform=proj, zorder=4,
            )
            if bb_line_handle is None:
                bb_line_handle = h
    if backbone_bus_pos is not None and len(backbone_bus_pos):
        bb_bus_scatter = ax.scatter(
            backbone_bus_pos[:, 1], backbone_bus_pos[:, 0],
            c="steelblue", s=50, marker="D", edgecolors="white",
            linewidths=0.5, alpha=1.0, transform=proj, zorder=5,
        )

    # ── Graph edges (foreground layer) ──
    src = edge_index[0].numpy()
    dst = edge_index[1].numpy()
    etypes = edge_attr[:, 1].numpy()

    color_map = {EDGE_TRANSMISSION: "#e67e22"}
    label_map = {EDGE_TRANSMISSION: "Graph edge (transmission)"}
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
            color=c, linewidth=lw, alpha=0.5, transform=proj, zorder=6,
        )
        if et not in handles:
            handles[et] = line

    node_scatter = ax.scatter(
        positions[:, 1], positions[:, 0],
        c="red", s=18, edgecolors="k", linewidths=0.3,
        alpha=0.8, transform=proj, zorder=7,
    )

    # ── Legend ──
    legend_handles = []
    legend_labels = []
    if sim_line_handle is not None:
        legend_handles.append(sim_line_handle)
        legend_labels.append("Simulation line (345 kV)")
    legend_handles.append(sim_bus_scatter)
    legend_labels.append("Simulation bus")
    if bb_line_handle is not None:
        legend_handles.append(bb_line_handle)
        legend_labels.append("Backbone edge (after contraction)")
    if bb_bus_scatter is not None:
        legend_handles.append(bb_bus_scatter)
        legend_labels.append("Backbone bus (has ≥1 node)")
    for k in sorted(handles):
        legend_handles.append(handles[k])
        legend_labels.append(label_map[k])
    legend_handles.append(node_scatter)
    legend_labels.append("Settlement-point node")
    ax.legend(legend_handles, legend_labels, loc="lower left", fontsize=9)

    n_tx = sum(1 for e in etypes if e == EDGE_TRANSMISSION) // 2
    n_bb_buses = len(backbone_bus_pos) if backbone_bus_pos is not None else "?"
    n_bb_edges = len(backbone_edges) if backbone_edges is not None else "?"
    ax.set_title(
        f"Transmission-Guided Graph — {len(node_order)} nodes, {n_tx} graph edges\n"
        f"Sim: {len(sim_buses)} buses, {len(sim_lines)} lines  |  "
        f"Backbone: {n_bb_buses} buses, {n_bb_edges} edges",
        fontsize=11,
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
    ax.set_title("Training Curves — Transmission GAT")
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

    (edge_index, edge_attr, node_order, positions, virtual_idx,
     backbone_bus_pos, backbone_edges) = build_transmission_graph(coords_with_data)

    plot_graph_topology(
        edge_index, edge_attr, positions, node_order, virtual_idx,
        os.path.join(out_dir, "graph_topology.png"),
        backbone_bus_pos=backbone_bus_pos,
        backbone_edges=backbone_edges,
    )

    # ── Prepare features ──
    print("\nPreparing features...")
    hours, hour_to_features, num_features = prepare_features(
        df, node_order, virtual_idx,
    )
    print(f"  Hours: {len(hours)}, Features/node: {num_features}")
    print(f"  Real nodes: {len(node_order)}, Virtual node idx: {virtual_idx}")

    # ── Build PyG datasets ──
    print("\nBuilding PyG datasets (intra-month split, demeaned targets)...")
    (train_data, val_data, test_data, scaler, lmp_lo, lmp_hi,
     node_lmp_mean) = \
        build_pyg_datasets(hours, hour_to_features, edge_index, edge_attr)

    train_loader = DataLoader(train_data, batch_size=BATCH_SIZE, shuffle=True)
    val_loader = DataLoader(val_data, batch_size=BATCH_SIZE, shuffle=False)
    test_loader = DataLoader(test_data, batch_size=1, shuffle=False)

    # ── Model ──
    print(f"\nDevice: {DEVICE}")
    if DEVICE.type == "mps":
        print("  Apple Silicon GPU (MPS) — sparse ops fall back to CPU")
    num_nodes = virtual_idx + 1  # includes virtual super node
    model = ERCOTGraphNet(
        in_channels=num_features, num_nodes=num_nodes,
    ).to(DEVICE)
    n_params = sum(p.numel() for p in model.parameters())
    print(f"Model: {n_params:,} parameters")
    print(f"  {N_GAT_LAYERS}-layer residual GAT ({N_HEADS} heads, "
          f"hidden={HIDDEN_DIM}), edge_dim=2, "
          f"node_embed={NODE_EMBED_DIM}")

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
        "node_lmp_mean": node_lmp_mean,
        "metrics": metrics,
        "lmp_clip": (lmp_lo, lmp_hi),
        "virtual_node_idx": virtual_idx,
        "config": {
            "HIDDEN_DIM": HIDDEN_DIM, "N_HEADS": N_HEADS,
            "N_GAT_LAYERS": N_GAT_LAYERS, "DROPOUT": DROPOUT,
            "NODE_EMBED_DIM": NODE_EMBED_DIM,
        },
    }, os.path.join(results_dir, "model_checkpoint.pt"))

    print(f"\nAll outputs saved to {out_dir}")
    print("Done.")


if __name__ == "__main__":
    main()
