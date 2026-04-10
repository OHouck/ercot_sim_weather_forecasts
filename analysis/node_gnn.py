"""
Graph Neural Network: Weather Forecast Error Impact on ERCOT Node LMP
=====================================================================

v4 — Transmission-guided node-level prediction (no virtual super node)

Usage:
        uv run python -m analysis.node_gnn
"""

import glob
import os
import re
import sys
from pathlib import Path

os.environ.setdefault("PYTORCH_ENABLE_MPS_FALLBACK", "1")

import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.io.shapereader as shpreader
import statsmodels.api as sm
from scipy.spatial import KDTree
from sklearn.preprocessing import StandardScaler

import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.data import Data
from torch_geometric.loader import DataLoader
from torch_geometric.nn import GATConv
from torch_geometric.utils import dropout_edge
from captum.attr import IntegratedGradients

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories
from process_data.process_ercot import load_rt_spp_month

# ── Configuration ────────────────────────────────────────────────
# Uses combined HRRR (1h short-range) + GFS (day-ahead, lead=0) by default
ERROR_SOURCE = "era5"
MONTHS = [(2025, m) for m in range(1, 13)]

HIDDEN_DIM = 64
N_HEADS = 2
N_GAT_LAYERS = 4
DROPOUT = 0.5
INPUT_DROPOUT = 0.2
INPUT_NOISE_STD = 0.02
EMBED_L2 = 5e-4
EDGE_DROPOUT = 0.30

TRAIN_DAYS = (1, 15)
VAL_DAYS = (16, 25)
TEST_DAYS = (26, 31)

BATCH_SIZE = 64
LEARNING_RATE = 7e-4
WEIGHT_DECAY = 1e-2
MAX_EPOCHS = 200
PATIENCE = 25
WARMUP_EPOCHS = 5
MIN_EPOCHS = 25
NODE_EMBED_DIM = 0        # disable node-ID memorization by default
TARGET_TRANSFORM_QUANTILE = 75

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
    "temp_error_1h_resid",
    "wspd_error_1h_resid",
    "temp_error_0h_resid",
    "wspd_error_0h_resid",
    "era5_temp_resid",
    "era5_wspd_resid",
    "lat",
    "lon",
]
TARGET_COL = "lmp_resid"

ERROR_FEATURE_INDICES = [0, 1, 2, 3]
ERROR_FEATURE_NAMES = [
    "temp_error_1h", "wspd_error_1h",   # HRRR short-range
    "temp_error_0h", "wspd_error_0h",   # GFS day-ahead
]

LMP_CLIP_LO = 1
LMP_CLIP_HI = 99

# Edge type constants (stored as edge_attr feature 2)
EDGE_TRANSMISSION = 0

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

def residualize_on_time_fe(series, hour_of_day, weekday, month):
    """Remove cyclical time fixed effects and return residuals."""
    hour_sin, hour_cos = _encode_cyclical(hour_of_day.values, 24)
    wday_sin, wday_cos = _encode_cyclical(weekday.values, 7)
    mon_sin, mon_cos = _encode_cyclical(month.values, 12)
    X = np.column_stack([hour_sin, hour_cos, wday_sin, wday_cos, mon_sin, mon_cos])
    X = sm.add_constant(X, has_constant="add")
    mask = np.isfinite(series.values)
    resid = np.full(len(series), np.nan, dtype=np.float32)
    if mask.sum() < 10:
        return pd.Series(np.nan_to_num(series.values, nan=0.0), index=series.index)
    fit = sm.OLS(series.values[mask], X[mask]).fit()
    resid[mask] = fit.resid.astype(np.float32)
    return pd.Series(np.nan_to_num(resid, nan=0.0), index=series.index)


def _build_rt_hour_timestamp(rt_df):
    """Build robust hourly timestamp from RT SPP schema variants."""
    if "deliveryDate" in rt_df.columns:
        base_date = pd.to_datetime(rt_df["deliveryDate"], errors="coerce")
    else:
        base_date = pd.Series(pd.NaT, index=rt_df.index)

    hour_col = None
    for c in ["deliveryHour", "hourEnding", "hour"]:
        if c in rt_df.columns:
            hour_col = c
            break

    if hour_col is not None:
        hour_vals = pd.to_numeric(rt_df[hour_col], errors="coerce")
        # ERCOT hour-ending fields are typically 1..24; convert to hour-beginning.
        finite_hours = hour_vals[np.isfinite(hour_vals)]
        if len(finite_hours) and finite_hours.min() >= 1 and finite_hours.max() <= 24:
            hour_vals = hour_vals - 1
        ts = base_date + pd.to_timedelta(hour_vals, unit="h")
    elif "SCEDTimestamp" in rt_df.columns:
        ts = pd.to_datetime(rt_df["SCEDTimestamp"], errors="coerce")
    elif "deliveryInterval" in rt_df.columns:
        interval = pd.to_numeric(rt_df["deliveryInterval"], errors="coerce")
        ts = base_date + pd.to_timedelta((interval - 1) * 15, unit="m")
    else:
        raise ValueError("Could not construct hourly timestamp from RT SPP data.")

    return pd.to_datetime(ts, errors="coerce").dt.floor("h")


def load_data():
    """Load pixel-hour features and node-level hourly LMP targets."""
    dirs = setup_directories()

    pixel_dir = Path(dirs["processed"]) / "combined_hourly_gridded_data"
    frames = []
    for year, month in MONTHS:
        p = pixel_dir / f"pixel_hourly_gfs+hrrr_{year}_{month:02d}.parquet"
        if not p.exists():
            continue
        cols = [
            "valid_time", "pixel_id", "latitude", "longitude",
            "temp_error_1h", "wspd_error_1h", "temp_error_0h", "wspd_error_0h",
            "era5_temp", "era5_wspd", "total_shadow_cost",
            "hour_of_day", "weekday", "month",
        ]
        frames.append(pd.read_parquet(p, columns=cols))

    if not frames:
        raise FileNotFoundError("No pixel_hourly parquet files found for configured months.")

    pixel_df = pd.concat(frames, ignore_index=True)
    pixel_df["hour"] = pd.to_datetime(pixel_df["valid_time"]).dt.floor("h")

    rt_frames = []
    for year, month in MONTHS:
        try:
            rt = load_rt_spp_month(year, month)
        except FileNotFoundError:
            continue

        sp_col = "settlementPoint"
        if sp_col not in rt.columns and "settlementPointName" in rt.columns:
            sp_col = "settlementPointName"
        if sp_col not in rt.columns:
            continue

        if "settlementPointType" in rt.columns:
            rt = rt[rt["settlementPointType"] == "RN"].copy()

        rt["hour"] = _build_rt_hour_timestamp(rt)
        rt["lmp"] = pd.to_numeric(rt["settlementPointPrice"], errors="coerce")
        rt = rt.dropna(subset=["hour", "lmp"])
        rt = rt.rename(columns={sp_col: "settlement_point"})
        rt_frames.append(rt[["settlement_point", "hour", "lmp"]])

    if not rt_frames:
        raise FileNotFoundError("No RT SPP data found for configured months.")

    rt_hourly = (
        pd.concat(rt_frames, ignore_index=True)
        .groupby(["settlement_point", "hour"], as_index=False)["lmp"]
        .mean()
    )

    node_coords = pd.read_csv(os.path.join(dirs["processed"], "node_coordinates.csv"))
    node_coords = node_coords.dropna(subset=["lat", "lon"]).copy()

    pixel_coords = (
        pixel_df[["pixel_id", "latitude", "longitude"]]
        .drop_duplicates("pixel_id")
        .dropna(subset=["latitude", "longitude"])
        .reset_index(drop=True)
    )
    if pixel_coords.empty:
        pixel_coords = pixel_df[["pixel_id"]].drop_duplicates("pixel_id").copy()
        latlon = pixel_coords["pixel_id"].str.split("_", n=1, expand=True)
        pixel_coords["latitude"] = pd.to_numeric(latlon[0], errors="coerce")
        pixel_coords["longitude"] = pd.to_numeric(latlon[1], errors="coerce")
        pixel_coords = (
            pixel_coords
            .dropna(subset=["latitude", "longitude"])
            .reset_index(drop=True)
        )
    if pixel_coords.empty:
        raise ValueError("No valid pixel coordinates available for nearest-neighbor mapping.")
    tree = KDTree(pixel_coords[["latitude", "longitude"]].values)
    node_coords["pixel_id"] = pd.NA
    valid_nodes = np.isfinite(node_coords[["lat", "lon"]].to_numpy()).all(axis=1)
    if valid_nodes.any():
        _, nn_idx = tree.query(node_coords.loc[valid_nodes, ["lat", "lon"]].to_numpy(), k=1)
        nn_idx = np.asarray(nn_idx, dtype=int)
        nn_idx = np.clip(nn_idx, 0, len(pixel_coords) - 1)
        mapped_pixels = pixel_coords["pixel_id"].to_numpy()[nn_idx]
        node_coords.loc[valid_nodes, "pixel_id"] = mapped_pixels
    node_coords = node_coords.dropna(subset=["pixel_id"]).copy()

    node_hour = pixel_df.merge(
        node_coords[["settlement_point", "pixel_id", "lat", "lon"]],
        on="pixel_id", how="inner",
    )
    node_hour = node_hour.merge(rt_hourly, on=["settlement_point", "hour"], how="inner")

    lo = np.nanpercentile(node_hour["lmp"], LMP_CLIP_LO)
    hi = np.nanpercentile(node_hour["lmp"], LMP_CLIP_HI)
    node_hour["lmp"] = node_hour["lmp"].clip(lo, hi)

    node_hour[TARGET_COL] = residualize_on_time_fe(
        node_hour["lmp"],
        node_hour["hour_of_day"],
        node_hour["weekday"],
        node_hour["month"],
    )

    for col in ["temp_error_1h", "wspd_error_1h", "temp_error_0h", "wspd_error_0h", "era5_temp", "era5_wspd"]:
        node_hour[f"{col}_resid"] = residualize_on_time_fe(
            node_hour[col], node_hour["hour_of_day"], node_hour["weekday"], node_hour["month"]
        )

    return node_hour, node_coords, dirs


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
        Returns:
                edge_index:       (2, E) LongTensor
                edge_attr:        (E, 2) FloatTensor — [normalised distance, edge type]
                node_order:       list of settlement_point names (length N)
                positions:        (N, 2) ndarray of (lat, lon) for real nodes
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

    # ── Assemble tensors ──
    src_list, dst_list, dist_list, type_list = [], [], [], []
    for (s, d), (dist, etype) in edge_set.items():
        src_list.append(s)
        dst_list.append(d)
        dist_list.append(dist)
        type_list.append(etype)

    edge_index = torch.tensor([src_list, dst_list], dtype=torch.long)

    dist_arr = np.array(dist_list, dtype=np.float32)
    dist_max = dist_arr.max() if len(dist_arr) else 1.0
    dist_norm = np.where(np.isfinite(dist_arr), dist_arr / max(dist_max, 1e-8), 0.0)

    type_arr = np.array(type_list, dtype=np.float32)
    edge_attr = torch.tensor(
        np.stack([dist_norm, type_arr], axis=1), dtype=torch.float32,
    )

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

    return (edge_index, edge_attr, node_order, positions,
            backbone_bus_pos, backbone_edges)


# ── Feature Engineering ──────────────────────────────────────────

def _encode_cyclical(values, period):
    angle = 2 * np.pi * values / period
    return np.sin(angle), np.cos(angle)


def prepare_features(df, node_order):
    """Build per-hour graph features with node-level targets."""
    node_to_idx = {sp: i for i, sp in enumerate(node_order)}
    num_nodes = len(node_order)

    df_f = df[df["settlement_point"].isin(node_to_idx)].copy()
    df_f["node_idx"] = df_f["settlement_point"].map(node_to_idx)

    feature_cols = BASE_NUMERIC_COLS
    num_features = len(feature_cols)

    hours = sorted(df_f["hour"].unique())
    hour_to_features = {}

    grouped = df_f.groupby("hour")
    for hour, group in grouped:
        X = np.full((num_nodes, num_features), np.nan, dtype=np.float32)
        y = np.full(num_nodes, np.nan, dtype=np.float32)
        y_raw = np.full(num_nodes, np.nan, dtype=np.float32)
        mask = np.zeros(num_nodes, dtype=bool)

        idxs = group["node_idx"].values
        X[idxs] = group[feature_cols].values.astype(np.float32)
        y[idxs] = group[TARGET_COL].values.astype(np.float32)
        y_raw[idxs] = group["lmp"].values.astype(np.float32)
        mask[idxs] = True

        hour_to_features[hour] = (X, y, y_raw, mask)

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

    # Robust signed transform for heavy-tailed scalar targets:
    # z = asinh(y / scale), then z is standardized for training.
    train_y = np.concatenate([
        hour_to_features[h][2][hour_to_features[h][3]]
        for h in train_hours
    ]).astype(np.float32)
    y_scale = float(np.nanpercentile(np.abs(train_y), TARGET_TRANSFORM_QUANTILE))
    if not np.isfinite(y_scale) or y_scale < 1e-6:
        y_scale = 1.0

    def _y_to_z(y_raw):
        return np.arcsinh(y_raw / y_scale)

    train_z = _y_to_z(train_y)
    y_mean = float(np.nanmean(train_z))
    y_std = float(np.nanstd(train_z))
    y_std = y_std if y_std > 1e-8 else 1.0
    print(
        f"  Scalar target transform: asinh(y/{y_scale:.1f}), "
        f"mean={y_mean:.3f}, std={y_std:.3f}"
    )

    # ── Feature scaler (fit on training X) ──
    train_X_all = np.vstack([hour_to_features[h][0] for h in train_hours])
    scaler = StandardScaler()
    scaler.fit(np.nan_to_num(train_X_all, nan=0.0))

    def hours_to_data(hour_list):
        out = []
        for h in hour_list:
            X, y, y_raw, mask = hour_to_features[h]
            X_s = scaler.transform(np.nan_to_num(X, nan=0.0)).astype(np.float32)
            y_z = _y_to_z(np.nan_to_num(y_raw, nan=0.0)).astype(np.float32)
            y_norm = ((y_z - y_mean) / y_std).astype(np.float32)
            out.append(Data(
                x=torch.tensor(X_s), y=torch.tensor(y_norm),
                y_raw=torch.tensor(y_raw, dtype=torch.float32),
                edge_index=edge_index, edge_attr=edge_attr,
                mask=torch.tensor(mask),
            ))
        return out

    return (
        hours_to_data(train_hours), hours_to_data(val_hours),
        hours_to_data(test_hours), scaler, y_mean, y_std,
        y_scale,
    )


# ── Model ────────────────────────────────────────────────────────

class ERCOTGraphNet(nn.Module):
    """Residual multi-layer GAT with node-level output."""

    def __init__(self, in_channels, num_nodes, embed_dim=NODE_EMBED_DIM,
                 hidden=HIDDEN_DIM, heads=N_HEADS, dropout=DROPOUT, edge_dim=2):
        super().__init__()
        self.embed_dim = embed_dim
        if embed_dim > 0:
            # Learnable per-node embedding
            self.node_embed = nn.Parameter(torch.randn(num_nodes, embed_dim) * 0.02)
            self.embed_drop = nn.Dropout(dropout)
            input_dim = in_channels + embed_dim
        else:
            self.node_embed = None
            self.embed_drop = None
            input_dim = in_channels

        self.input_proj = nn.Linear(input_dim, hidden)
        self.gat_layers = nn.ModuleList([
            GATConv(hidden, hidden, heads=heads, dropout=dropout,
                    concat=False, edge_dim=edge_dim)
            for _ in range(N_GAT_LAYERS)
        ])
        self.bn_layers = nn.ModuleList([nn.BatchNorm1d(hidden) for _ in range(N_GAT_LAYERS)])
        self.res_layers = nn.ModuleList([nn.Linear(hidden, hidden) for _ in range(N_GAT_LAYERS)])
        self.out_head = nn.Linear(hidden, 1)
        self.dropout = dropout
        self._attn_weights = [None for _ in range(N_GAT_LAYERS)]

    def forward(self, data, return_attention=False):
        x, edge_index = data.x, data.edge_index
        edge_attr = data.edge_attr if hasattr(data, "edge_attr") else None

        if self.training and EDGE_DROPOUT > 0:
            edge_index, keep_mask = dropout_edge(edge_index, p=EDGE_DROPOUT, training=True)
            if edge_attr is not None:
                edge_attr = edge_attr[keep_mask]

        # Regularize raw inputs for better out-of-sample behavior.
        x = F.dropout(x, p=INPUT_DROPOUT, training=self.training)
        if self.training and INPUT_NOISE_STD > 0:
            x = x + INPUT_NOISE_STD * torch.randn_like(x)

        # Concatenate learnable node embeddings when enabled.
        if self.node_embed is not None:
            n = self.node_embed.shape[0]
            B = x.shape[0] // n
            embeds = self.embed_drop(self.node_embed.repeat(B, 1))
            x = torch.cat([x, embeds], dim=1)

        x = F.elu(self.input_proj(x))
        for i, gat in enumerate(self.gat_layers):
            residual = self.res_layers[i](x)
            out, attn = gat(x, edge_index, edge_attr=edge_attr, return_attention_weights=True)
            self._attn_weights[i] = attn
            out = self.bn_layers[i](out)
            out = F.elu(out)
            out = F.dropout(out, p=self.dropout, training=self.training)
            x = out + residual

        return self.out_head(x).squeeze(-1)


# ── Training ─────────────────────────────────────────────────────

def train_model(model, train_loader, val_loader):
    optimizer = torch.optim.AdamW(
        model.parameters(), lr=LEARNING_RATE, weight_decay=WEIGHT_DECAY,
    )
    # v4: ReduceLROnPlateau with warmup — more adaptive than cosine
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
        optimizer, mode="min", factor=0.4, patience=6, min_lr=1e-6,
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
            ym = batch.y.view(-1)
            m = batch.mask.view(-1)
            loss = huber(pred[m], ym[m])
            # Penalize node embeddings to limit node-specific memorization.
            if model.node_embed is not None and EMBED_L2 > 0:
                loss = loss + EMBED_L2 * model.node_embed.pow(2).mean()
            if torch.isnan(loss):
                raise RuntimeError("NaN loss — check MPS fallback.")
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
            with torch.no_grad():
                mae_sum += (pred[m] - ym[m]).abs().sum().item()
                count += int(m.sum().item())

        model.eval()
        v_sum, v_cnt = 0.0, 0
        with torch.no_grad():
            for batch in val_loader:
                batch = batch.to(DEVICE)
                pred = model(batch)
                ym = batch.y.view(-1)
                m = batch.mask.view(-1)
                v_sum += (pred[m] - ym[m]).abs().sum().item()
                v_cnt += int(m.sum().item())

        t_mae = mae_sum / max(count, 1)
        v_mae = v_sum / max(v_cnt, 1)
        history["train_mae"].append(t_mae)
        history["val_mae"].append(v_mae)

        # Step scheduler after warmup
        if epoch >= WARMUP_EPOCHS:
            scheduler.step(v_mae)

        v_mae_smooth = np.mean(history["val_mae"][-3:])
        improved = v_mae_smooth < (best_val_mae - 1e-4)
        if improved:
            best_val_mae = v_mae_smooth
            best_state = {k: v.cpu().clone()
                          for k, v in model.state_dict().items()}
            patience_counter = 0
        else:
            patience_counter += 1

        if epoch % 10 == 0 or improved:
            lr = optimizer.param_groups[0]["lr"]
            print(f"  Epoch {epoch:3d}: train={t_mae:.2f}  val={v_mae:.2f}"
                f"  val_smooth={v_mae_smooth:.2f}"
                f"  lr={lr:.2e}{'  *' if improved else ''}")

        if (epoch + 1) >= MIN_EPOCHS and patience_counter >= PATIENCE:
            print(f"  Early stopping at epoch {epoch} "
                f"(best val_smooth={best_val_mae:.2f})")
            break

    if best_state is not None:
        model.load_state_dict(best_state)
    return model, history


# ── Evaluation ───────────────────────────────────────────────────

def fit_prediction_calibration(model, val_loader, y_mean, y_std, y_scale):
    """Fit affine calibration y* = a * y_hat + b on validation set."""
    model.eval()
    model_device = next(model.parameters()).device
    pred_list = []
    true_list = []
    with torch.no_grad():
        for batch in val_loader:
            batch = batch.to(model_device)
            pred_n = model(batch).cpu().numpy().reshape(-1)
            y_n = batch.y.cpu().numpy().reshape(-1)
            m = batch.mask.cpu().numpy().astype(bool).reshape(-1)

            pred_z = pred_n * y_std + y_mean
            y_z = y_n * y_std + y_mean

            pred = np.sinh(pred_z) * y_scale
            y = np.sinh(y_z) * y_scale

            pred_list.append(pred[m])
            true_list.append(y[m])

    if not pred_list:
        return 1.0, 0.0

    yhat = np.concatenate(pred_list)
    ytrue = np.concatenate(true_list)
    if np.nanstd(yhat) < 1e-8:
        return 1.0, 0.0

    X = np.column_stack([yhat, np.ones_like(yhat)])
    a, b = np.linalg.lstsq(X, ytrue, rcond=None)[0]
    print(f"  Calibration (val): y* = {a:.3f} * y_hat + {b:.3f}")
    return float(a), float(b)


def evaluate_model(model, test_loader, y_mean, y_std, y_scale, calibration=(1.0, 0.0)):
    """Evaluate node-level predictions on masked test nodes."""
    model.eval()
    model_device = next(model.parameters()).device
    pred_list = []
    true_list = []
    cal_a, cal_b = calibration

    with torch.no_grad():
        for batch in test_loader:
            batch = batch.to(model_device)
            pred_n = model(batch).cpu().numpy().reshape(-1)
            y_n = batch.y.cpu().numpy().reshape(-1)
            m = batch.mask.cpu().numpy().astype(bool).reshape(-1)

            pred_z = pred_n * y_std + y_mean
            y_z = y_n * y_std + y_mean

            pred = np.sinh(pred_z) * y_scale
            y = np.sinh(y_z) * y_scale
            pred = cal_a * pred + cal_b
            pred_list.append(pred[m])
            true_list.append(y[m])

    all_p = np.concatenate(pred_list)
    all_t = np.concatenate(true_list)

    mae = np.mean(np.abs(all_p - all_t))
    rmse = np.sqrt(np.mean((all_p - all_t) ** 2))
    ss_res = np.sum((all_t - all_p) ** 2)
    ss_tot = np.sum((all_t - np.mean(all_t)) ** 2)
    r2 = 1.0 - ss_res / max(ss_tot, 1e-8)

    pred_df = pd.DataFrame({"y_pred": all_p, "y_true": all_t})
    return {"mae": mae, "rmse": rmse, "r2": r2}, pred_df


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


def extract_attention_weights(model, test_data, node_order):
    """Aggregate final-layer attention, excluding self-loop edges."""
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

    # Keep only real directed edges (exclude self-loops).
    keep = (src != dst)
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
                        save_path,
                        backbone_bus_pos=None, backbone_edges=None):
    """Draw the transmission-guided graph on a Texas map.

    Layer order (back → front):
      1. Light gray  — full simulation lines + buses (all shapefiles)
      2. Steel blue  — backbone buses and edges after empty-bus contraction
      3. Orange      — GNN graph edges (settlement nodes connected via backbone)
      4. Red dots    — settlement-point nodes
    All graph edges are transmission-guided.
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
    ap = predictions["y_pred"].to_numpy()
    at = predictions["y_true"].to_numpy()
    fig, ax = plt.subplots(figsize=(7, 7))
    ax.scatter(at, ap, s=1, alpha=0.1, color="steelblue")
    lo, hi = min(at.min(), ap.min()), max(at.max(), ap.max())
    ax.plot([lo, hi], [lo, hi], "k--", linewidth=0.8, alpha=0.6)
    ax.set_xlabel("Actual congestion outcome")
    ax.set_ylabel("Predicted congestion outcome")
    ax.set_title("Predicted vs Actual Outcome — Test Set")
    ax.set_aspect("equal")
    plt.tight_layout()
    fig.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {save_path}")


def compute_integrated_gradients(model, test_data, error_feature_indices, error_feature_names):
    """Compute mean absolute IG attribution per error feature."""
    model.eval()
    original_device = next(model.parameters()).device
    # Captum internally constructs float64 integration weights; MPS does not
    # support float64 tensors, so run attribution on CPU for compatibility.
    ig_device = torch.device("cpu") if original_device.type == "mps" else original_device
    model = model.to(ig_device)

    agg = np.zeros(len(error_feature_indices), dtype=np.float64)
    n_examples = 0

    for d in test_data:
        d = d.to(ig_device)

        def forward_with_x(x):
            dd = Data(x=x, edge_index=d.edge_index, edge_attr=d.edge_attr)
            dd.batch = torch.zeros(x.shape[0], dtype=torch.long, device=x.device)
            return model(dd).sum()

        ig = IntegratedGradients(forward_with_x)
        x_in = d.x.detach().clone().to(torch.float32).requires_grad_(True)
        baseline = torch.zeros_like(x_in, dtype=torch.float32)
        attr = ig.attribute(x_in, baselines=baseline)
        attr_abs = attr.detach().cpu().numpy().mean(axis=0)
        agg += np.abs(attr_abs[error_feature_indices])
        n_examples += 1

    if n_examples == 0:
        return pd.DataFrame(columns=["feature", "mean_abs_ig"])

    agg /= n_examples
    out = pd.DataFrame({
        "feature": error_feature_names,
        "mean_abs_ig": agg,
    }).sort_values("mean_abs_ig", ascending=False)

    # Restore model device for any downstream work (e.g., checkpoint save).
    model.to(original_device)
    return out


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

    (edge_index, edge_attr, node_order, positions,
     backbone_bus_pos, backbone_edges) = build_transmission_graph(coords_with_data)

    plot_graph_topology(
        edge_index, edge_attr, positions, node_order,
        os.path.join(out_dir, "graph_topology.png"),
        backbone_bus_pos=backbone_bus_pos,
        backbone_edges=backbone_edges,
    )

    # ── Prepare features ──
    print("\nPreparing features...")
    hours, hour_to_features, num_features = prepare_features(df, node_order)
    print(f"  Hours: {len(hours)}, Features/node: {num_features}")
    print(f"  Nodes: {len(node_order)}")

    # ── Build PyG datasets ──
    print("\nBuilding PyG datasets (intra-month split, demeaned targets)...")
    (train_data, val_data, test_data, scaler, y_mean, y_std,
     y_scale) = \
        build_pyg_datasets(hours, hour_to_features, edge_index, edge_attr)

    train_loader = DataLoader(train_data, batch_size=BATCH_SIZE, shuffle=True)
    val_loader = DataLoader(val_data, batch_size=BATCH_SIZE, shuffle=False)
    test_loader = DataLoader(test_data, batch_size=1, shuffle=False)

    # ── Model ──
    print(f"\nDevice: {DEVICE}")
    if DEVICE.type == "mps":
        print("  Apple Silicon GPU (MPS) — sparse ops fall back to CPU")
    num_nodes = len(node_order)
    model = ERCOTGraphNet(
        in_channels=num_features, num_nodes=num_nodes,
    ).to(DEVICE)
    n_params = sum(p.numel() for p in model.parameters())
    print(f"Model: {n_params:,} parameters")
    print(f"  {N_GAT_LAYERS}-layer residual GAT ({N_HEADS} heads, "
          f"hidden={HIDDEN_DIM}), edge_dim=2, "
                        f"node_embed={NODE_EMBED_DIM}, edge_dropout={EDGE_DROPOUT}, "
                    f"node-level output")

    # ── Train ──
    print("\nTraining...")
    model, history = train_model(model, train_loader, val_loader)

    print("\nGenerating plots...")
    plot_training_curves(history, os.path.join(out_dir, "training_curves.png"))

    print("\nFitting prediction calibration on validation set...")
    calibration = fit_prediction_calibration(
        model, val_loader, y_mean=y_mean, y_std=y_std, y_scale=y_scale,
    )

    # ── Evaluate ──
    print("\nEvaluating on test set (days 26+ of each month)...")
    metrics, predictions = evaluate_model(
        model, test_loader, y_mean=y_mean, y_std=y_std,
        y_scale=y_scale, calibration=calibration,
    )
    print(f"  MAE:  {metrics['mae']:.2f} $/MWh")
    print(f"  RMSE: {metrics['rmse']:.2f} $/MWh")
    print(f"  R2:   {metrics['r2']:.4f}")

    plot_pred_vs_actual(predictions, os.path.join(out_dir, "pred_vs_actual.png"))

    # ── Integrated Gradients attribution ──
    print("\nComputing Integrated Gradients attribution...")
    ig_df = compute_integrated_gradients(
        model,
        test_data,
        error_feature_indices=ERROR_FEATURE_INDICES,
        error_feature_names=ERROR_FEATURE_NAMES,
    )
    if not ig_df.empty:
        print("\nTop feature attributions (IG):")
        print(ig_df.to_string(index=False))

    # ── Save ──
    predictions.to_csv(os.path.join(results_dir, "hourly_test_predictions.csv"),
                       index=False)
    ig_df.to_csv(os.path.join(results_dir, "integrated_gradients_summary.csv"),
                 index=False)
    torch.save({
        "model_state_dict": model.state_dict(),
        "scaler_mean": scaler.mean_,
        "scaler_scale": scaler.scale_,
        "node_order": node_order,
        "metrics": metrics,
        "y_mean": y_mean,
        "y_std": y_std,
        "y_scale": y_scale,
        "calibration": {"a": calibration[0], "b": calibration[1]},
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
