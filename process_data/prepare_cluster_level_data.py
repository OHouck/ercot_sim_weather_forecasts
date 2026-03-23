import os
import sys
import glob
import re
from pathlib import Path
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from scipy.spatial import ConvexHull
from shapely.geometry import MultiPoint, Point
from sklearn.cluster import AgglomerativeClustering
from sklearn.neighbors import kneighbors_graph
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score

# Allow imports from the project root (helper_funcs lives there)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories

# ── EIA generator broad-category mapping ─────────────────────────────────────
# Fine-grained EIA technology → one of: gas, nuclear, coal, solar, wind, other
_BROAD_CATEGORY_MAP = {
    "Natural Gas Internal Combustion Engine": "gas",
    "Natural Gas Fired Combined Cycle":       "gas",
    "Natural Gas Fired Combustion Turbine":   "gas",
    "Natural Gas Steam Turbine":              "gas",
    "Nuclear":                                "nuclear",
    "Conventional Steam Coal":                "coal",
    "Solar Photovoltaic":                     "solar",
    "Onshore Wind Turbine":                   "wind",
    # everything else (petroleum, batteries, hydro, biomass, etc.) → other
}
_BROAD_CATEGORIES = ["gas", "nuclear", "coal", "solar", "wind", "other"]


def _tech_slug(tech: str) -> str:
    """Convert a technology name to a safe column-name slug."""
    return re.sub(r"[^a-z0-9]+", "_", tech.lower()).strip("_")


def compute_node_lmp_features(df):
    """
    Compute per-node LMP summary statistics for clustering.

    Returns DataFrame with one row per settlement_point and columns:
      settlement_point, lat, lon, mean_lmp, std_lmp, peak_offpeak_spread,
      mean_lmp_<month>, std_lmp_<month> for each month present in df.

    The monthly mean/std columns capture seasonal patterns (e.g. a node that
    is expensive in summer but cheap in winter clusters differently from one
    that is consistently expensive year-round).
    """
    # Overall mean and std LMP per node
    node_stats = (
        df.groupby('settlement_point')['lmp']
        .agg(mean_lmp='mean', std_lmp='std')
        .reset_index()
    )
    node_stats['std_lmp'] = node_stats['std_lmp'].fillna(0)

    # Per-month mean and std, pivoted to wide format (one row per node)
    monthly = (
        df.groupby(['settlement_point', 'month'])['lmp']
        .agg(mean_lmp='mean', std_lmp='std')
        .reset_index()
    )
    monthly['std_lmp'] = monthly['std_lmp'].fillna(0)

    monthly_mean = (
        monthly.pivot(index='settlement_point', columns='month', values='mean_lmp')
        .rename(columns=lambda m: f'mean_lmp_m{m:02d}')
        .reset_index()
    )
    monthly_std = (
        monthly.pivot(index='settlement_point', columns='month', values='std_lmp')
        .rename(columns=lambda m: f'std_lmp_m{m:02d}')
        .reset_index()
    )

    # Peak (hours 14-19) vs off-peak (hours 0-5) spread
    df_peak = df[df['hour_of_day'].between(14, 19)]
    df_offpeak = df[df['hour_of_day'].between(0, 5)]

    peak_mean = df_peak.groupby('settlement_point')['lmp'].mean().rename('peak_lmp')
    offpeak_mean = df_offpeak.groupby('settlement_point')['lmp'].mean().rename('offpeak_lmp')

    spread = pd.concat([peak_mean, offpeak_mean], axis=1)
    spread['peak_offpeak_spread'] = spread['peak_lmp'] - spread['offpeak_lmp']
    spread = spread[['peak_offpeak_spread']].reset_index()

    # Get lat/lon per node (one row per node from the main df)
    node_coords = (
        df[['settlement_point', 'lat', 'lon']]
        .drop_duplicates('settlement_point')
    )

    # Merge everything
    node_features = (
        node_coords
        .merge(node_stats, on='settlement_point')
        .merge(spread, on='settlement_point', how='left')
        .merge(monthly_mean, on='settlement_point', how='left')
        .merge(monthly_std, on='settlement_point', how='left')
    )
    node_features['peak_offpeak_spread'] = node_features['peak_offpeak_spread'].fillna(0)

    monthly_mean_cols = [c for c in node_features.columns if c.startswith('mean_lmp_m')]
    monthly_std_cols = [c for c in node_features.columns if c.startswith('std_lmp_m')]
    # Fill missing months (nodes with no data in a given month) with overall mean/std
    for col in monthly_mean_cols:
        node_features[col] = node_features[col].fillna(node_features['mean_lmp'])
    for col in monthly_std_cols:
        node_features[col] = node_features[col].fillna(node_features['std_lmp'])

    months_present = sorted(df['month'].unique())
    print(f"Computed LMP features for {len(node_features)} nodes")
    print(f"  Overall mean_lmp: {node_features['mean_lmp'].mean():.2f} "
          f"(std across nodes: {node_features['mean_lmp'].std():.2f})")
    print(f"  Overall std_lmp: {node_features['std_lmp'].mean():.2f}")
    print(f"  peak_offpeak_spread: {node_features['peak_offpeak_spread'].mean():.2f}")
    print(f"  Monthly features: {len(monthly_mean_cols)} mean + {len(monthly_std_cols)} std "
          f"(months: {months_present})")

    return node_features


def cluster_nodes(node_features, n_clusters=20, geo_weight=2.0, n_neighbors=8,
                  min_cluster_size=10):
    """
    Cluster ERCOT nodes using agglomerative clustering with geographic
    connectivity constraint.

    Features: [lat, lon] (weighted by geo_weight) + all LMP feature columns
    (mean_lmp, std_lmp, peak_offpeak_spread, plus per-month mean_lmp_m* and
    std_lmp_m* if present). All features are standardized before weighting.

    The connectivity constraint uses a k-nearest-neighbors graph on
    geographic coordinates, ensuring that only geographically adjacent
    nodes can be merged into the same cluster.

    After initial clustering, any cluster smaller than min_cluster_size has
    its nodes reassigned to their nearest valid cluster by geographic centroid
    distance. Reassignment repeats until all clusters meet the threshold (or
    only one cluster remains).

    Args:
        node_features: DataFrame from compute_node_lmp_features(), with columns
                       [settlement_point, lat, lon, mean_lmp, std_lmp,
                        peak_offpeak_spread] plus optional mean_lmp_m* / std_lmp_m*
        n_clusters: Number of clusters to form (before small-cluster merging)
        geo_weight: Multiplier for geographic features after standardization.
                    Higher values = clusters are more geographically compact.
        n_neighbors: Number of neighbors for the connectivity graph.
                     Higher = more permissive merging. 8-12 is typical for
                     ~450 points spread across Texas.
        min_cluster_size: Clusters with fewer nodes than this are merged into
                          the nearest valid cluster by centroid distance.

    Returns:
        (result_df, silhouette_score): node_features with added 'cluster' column,
        and the silhouette score computed on the final labels.
    """
    geo_cols = ['lat', 'lon']
    # Dynamically pick up all LMP feature columns (overall + monthly)
    lmp_cols = (
        ['mean_lmp', 'std_lmp', 'peak_offpeak_spread']
        + sorted(c for c in node_features.columns if c.startswith('mean_lmp_m'))
        + sorted(c for c in node_features.columns if c.startswith('std_lmp_m'))
    )

    scaler_geo = StandardScaler()
    scaler_lmp = StandardScaler()

    X_geo = scaler_geo.fit_transform(node_features[geo_cols].values)
    X_lmp = scaler_lmp.fit_transform(node_features[lmp_cols].values)

    # Apply geographic weight
    X_geo_weighted = X_geo * geo_weight

    # Combined feature matrix
    X = np.hstack([X_geo_weighted, X_lmp])

    # Build geographic connectivity graph (using raw lat/lon, not weighted)
    connectivity = kneighbors_graph(
        node_features[geo_cols].values,
        n_neighbors=n_neighbors,
        mode='connectivity',
        include_self=False
    )
    # Make symmetric (required by AgglomerativeClustering)
    connectivity = connectivity + connectivity.T
    connectivity[connectivity > 1] = 1

    # Run constrained agglomerative clustering
    model = AgglomerativeClustering(
        n_clusters=n_clusters,
        connectivity=connectivity,
        linkage='ward',
    )
    labels = model.fit_predict(X)

    # ── Post-process: merge clusters smaller than min_cluster_size ──
    coords = node_features[geo_cols].values  # (n_nodes, 2) lat/lon
    labels = labels.copy()

    while True:
        counts = np.bincount(labels)
        small = np.where(counts < min_cluster_size)[0]
        if len(small) == 0:
            break

        # Compute centroids of all current clusters
        unique_labels = np.unique(labels)
        centroids = {
            c: coords[labels == c].mean(axis=0)
            for c in unique_labels
        }
        valid_labels = set(unique_labels) - set(small)
        if not valid_labels:
            break  # can't merge further

        for sc in small:
            if sc not in np.unique(labels):
                continue  # already merged in a previous iteration
            sc_centroid = centroids[sc]
            # Find nearest valid cluster centroid
            nearest = min(
                valid_labels,
                key=lambda c: np.linalg.norm(sc_centroid - centroids[c])
            )
            labels[labels == sc] = nearest

        # Re-compact labels to 0..k-1
        mapping = {old: new for new, old in enumerate(np.unique(labels))}
        labels = np.array([mapping[l] for l in labels])

    result = node_features.copy()
    result['cluster'] = labels

    counts_final = np.bincount(labels)
    sil_score = silhouette_score(X, labels)

    n_small_before = (np.bincount(model.labels_) < min_cluster_size).sum()
    print(f"Clustering: {n_clusters} clusters requested, geo_weight={geo_weight}, "
          f"n_neighbors={n_neighbors}, min_cluster_size={min_cluster_size}")
    if n_small_before > 0:
        print(f"  Merged {n_small_before} small cluster(s) → {len(counts_final)} final clusters")
    print(f"  Silhouette score: {sil_score:.3f}")
    print(f"  Cluster sizes: min={counts_final.min()}, "
          f"max={counts_final.max()}, "
          f"mean={counts_final.mean():.1f}")

    return result, sil_score


def sweep_n_clusters(node_features, k_range=range(5, 41, 5), geo_weight=10.0, n_neighbors=8):
    """Sweep over different k values and plot silhouette scores."""
    results = []
    for k in k_range:
        _, sil = cluster_nodes(node_features, n_clusters=k,
                               geo_weight=geo_weight, n_neighbors=n_neighbors)
        results.append({'k': k, 'silhouette': sil})

    results_df = pd.DataFrame(results)

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(results_df['k'], results_df['silhouette'], 'o-', color='steelblue')
    ax.set_xlabel('Number of clusters (k)')
    ax.set_ylabel('Silhouette score')
    ax.set_title('Silhouette Score vs Number of Clusters')
    ax.grid(True, alpha=0.3)
    best_idx = results_df['silhouette'].idxmax()
    ax.axvline(x=results_df.loc[best_idx, 'k'], color='red', linestyle='--', alpha=0.5,
               label=f"Best k={int(results_df.loc[best_idx, 'k'])}")
    ax.legend()
    plt.tight_layout()

    best_k = results_df.loc[best_idx, 'k']
    print(f"\nBest k by silhouette: {int(best_k)} "
          f"(score={results_df.loc[best_idx, 'silhouette']:.3f})")

    return results_df, fig


def build_cluster_polygons(node_clusters, buffer_deg=0.1):
    """
    Build a convex-hull polygon for each cluster from its node coordinates.

    Clusters with fewer than 3 nodes (where a convex hull is degenerate) get a
    circular buffer around their centroid instead.

    Args:
        node_clusters: DataFrame with columns [settlement_point, cluster, lat, lon]
        buffer_deg: Degrees to buffer each polygon outward so that stations near
                    the boundary are captured. Default 0.1° (~11 km).

    Returns:
        GeoDataFrame with columns [cluster, geometry] in EPSG:4326.
    """
    records = []
    for cluster_id, grp in node_clusters.groupby('cluster'):
        pts = list(zip(grp['lon'], grp['lat']))
        if len(pts) >= 3:
            poly = MultiPoint(pts).convex_hull
        else:
            # Point or LineString — buffer to a polygon
            poly = MultiPoint(pts).convex_hull

        poly_buffered = poly.buffer(buffer_deg)
        records.append({'cluster': cluster_id, 'geometry': poly_buffered})

    cluster_polys = gpd.GeoDataFrame(records, crs='EPSG:4326')
    print(f"Built {len(cluster_polys)} cluster polygons (buffer={buffer_deg}°)")
    return cluster_polys


def load_station_errors_wide(months, models, dirs):
    """
    Load per-station forecast error CSVs and pivot lead times to wide format.

    Supports loading from multiple models simultaneously (e.g. HRRR 1h + GFS
    day-ahead).  Each model contributes columns with its own lead-hour suffix.

    Returns a DataFrame with one row per (station_id, hour) and columns:
      station_id, station_lat, station_lon, <error_cols>_<lead>h, ...

    Args:
        months: List of (year, month) tuples
        models: Dict mapping model name → tuple of lead hours,
                e.g. ``{'hrrr': (1,), 'gfs': (0,)}``.
        dirs: dict from setup_directories()

    Returns:
        DataFrame with station-hour rows and wide error columns.
    """
    MODEL_LEAD_TIMES = {'hrrr': (1,), 'gfs': (0,)}
    if models is None:
        models = dict(MODEL_LEAD_TIMES)

    combined_wide = None

    for model_name in sorted(models.keys()):
        leads = models[model_name]

        all_files = []
        for year, month in sorted(months):
            error_dir = os.path.join(
                dirs['processed'], 'forecast_errors', model_name, str(year), f"{month:02d}"
            )
            month_files = glob.glob(os.path.join(error_dir, '*.csv'))
            month_files = [f for f in month_files if not f.endswith('error_summary.csv')]
            all_files.extend(month_files)

        dfs = [pd.read_csv(f) for f in all_files]
        all_errors = pd.concat(dfs, ignore_index=True)
        all_errors['valid_time'] = pd.to_datetime(all_errors['valid_time'])
        all_errors['hour'] = all_errors['valid_time'].dt.floor('h')

        error_cols = [c for c in all_errors.columns
                      if c not in ('station_id', 'valid_time', 'lead_hours', 'hour', 'lat', 'lon')]

        # Build a wide DataFrame for each lead, then merge
        lead_dfs = {}
        for lead in leads:
            suffix = f'_{lead}h'
            lead_df = all_errors[all_errors['lead_hours'] == lead].copy()
            rename_map = {c: f'{c}{suffix}' for c in error_cols}
            lead_df = lead_df.rename(columns=rename_map)
            keep = ['station_id', 'hour', 'lat', 'lon'] + list(rename_map.values())
            lead_dfs[lead] = lead_df[keep]

        lead_list = list(leads)
        model_wide = lead_dfs[lead_list[0]]
        for lead in lead_list[1:]:
            model_wide = model_wide.merge(
                lead_dfs[lead].drop(columns=['lat', 'lon']),
                on=['station_id', 'hour'],
                how='outer',
            )
        model_wide = model_wide.rename(columns={'lat': 'station_lat', 'lon': 'station_lon'})

        # ── Deduplicate observed columns within this model ──
        if len(leads) > 1:
            obs_base_cols = [c for c in error_cols if c.startswith('observed_')]
            first_suffix = f'_{lead_list[0]}h'
            for base in obs_base_cols:
                col_first = f'{base}{first_suffix}'
                if col_first not in model_wide.columns:
                    continue
                for lead in lead_list[1:]:
                    col_other = f'{base}_{lead}h'
                    if col_other not in model_wide.columns:
                        continue
                    model_wide[col_first] = model_wide[col_first].fillna(model_wide[col_other])
                    model_wide = model_wide.drop(columns=[col_other])
                model_wide = model_wide.rename(columns={col_first: base})

        print(f"  Loaded {len(model_wide):,} station-hour rows "
              f"({model_wide['station_id'].nunique()} stations, model={model_name})")

        # Merge this model into the combined DataFrame
        if combined_wide is None:
            combined_wide = model_wide
        else:
            # Drop columns already present in the base (observed_*, station_lat, station_lon)
            dup_cols = [c for c in model_wide.columns
                        if c in combined_wide.columns
                        and c not in ('station_id', 'hour')]
            model_wide = model_wide.drop(columns=dup_cols)
            combined_wide = combined_wide.merge(
                model_wide,
                on=['station_id', 'hour'],
                how='outer',
            )

    models_key = '+'.join(sorted(models.keys()))
    print(f"Combined: {len(combined_wide):,} station-hour rows "
          f"({combined_wide['station_id'].nunique()} stations, models={models_key})")
    return combined_wide


def aggregate_to_cluster_hour(df, node_clusters, leads,
                               station_errors=None, cluster_polygons=None):
    """
    Aggregate data to cluster x hour level.

    LMP is aggregated from nodes assigned to each cluster. Weather forecast
    errors and observed conditions are aggregated from weather stations whose
    coordinates fall inside the cluster's convex-hull polygon.

    If station_errors or cluster_polygons are not provided, falls back to the
    node-attached station errors already in df (one station per node).

    For each (cluster, hour):
      - LMP: mean, std, max, min across nodes
      - System LMP STD: system_lmp_std, (total std across all nodes in all clusters for that hour)
      - Forecast errors (per lead time): mean, std, max(|error|)
      - Observed wind speed and temperature: mean, std, max, min

    Args:
        df: Node-level DataFrame from prepare_node_level_data()
        node_clusters: DataFrame with columns [settlement_point, cluster, lat, lon]
        leads: Tuple of lead hours to process (e.g. (1,) or (0,))
        station_errors: Optional wide-format station-hour DataFrame from
                        load_station_errors_wide(). If provided, used for all
                        weather aggregation instead of node-attached errors.
        cluster_polygons: Optional GeoDataFrame from build_cluster_polygons().
                          Required when station_errors is provided.

    Returns:
        DataFrame with one row per (cluster, hour).
    """
    use_polygons = (station_errors is not None) and (cluster_polygons is not None)

    # ── LMP aggregation from nodes ──
    df_with_cluster = df.merge(
        node_clusters[['settlement_point', 'cluster']],
        on='settlement_point',
        how='inner'
    )
    print(f"Nodes matched to clusters: "
          f"{df_with_cluster['settlement_point'].nunique()} / "
          f"{df['settlement_point'].nunique()}")

    # Build the aggregation dict dynamically.  Load-error columns are
    # detected by prefix (demand forecast leads are independent of weather
    # model leads, so we don't filter by the `leads` parameter here).
    lmp_agg = {
        'lmp_mean':       ('lmp',         'mean'),
        'lmp_std':        ('lmp',         'std'),
        'lmp_max':        ('lmp',         'max'),
        'lmp_min':        ('lmp',         'min'),
        'actual_load':    ('actual_load', 'mean'),
        'n_nodes_in_hour':('lmp',         'count'),
    }
    for le_col in [c for c in df_with_cluster.columns if c.startswith('load_error_')]:
        lmp_agg[le_col] = (le_col, 'mean')

    lmp_hourly = (
        df_with_cluster
        .groupby(['cluster', 'hour'])
        .agg(**lmp_agg)
        .reset_index()
    )
    lmp_hourly['lmp_std'] = lmp_hourly['lmp_std'].fillna(0)

    system_lmp_std = (
        df_with_cluster
        .groupby('hour')['lmp']
        .std()
        .rename('system_lmp_std')
        .reset_index()
    )

    # merge m:1 back to lmp_hourly to get system_lmp_std in the same df for modeling
    lmp_hourly = lmp_hourly.merge(system_lmp_std, on='hour', how='left')


    # ── Weather aggregation ──
    if use_polygons:
        weather_hourly = _aggregate_weather_from_polygons(
            station_errors, cluster_polygons, leads
        )
    else:
        # Fallback: use node-attached station errors already in df
        weather_hourly = _aggregate_weather_from_nodes(
            df_with_cluster, leads
        )

    # ── Merge LMP and weather ──
    cluster_hourly = lmp_hourly.merge(weather_hourly, on=['cluster', 'hour'], how='left')

    # Time features
    cluster_hourly['hour_dt'] = pd.to_datetime(cluster_hourly['hour'])
    cluster_hourly['hour_of_day'] = cluster_hourly['hour_dt'].dt.hour
    cluster_hourly['weekday'] = cluster_hourly['hour_dt'].dt.weekday
    cluster_hourly['month'] = cluster_hourly['hour_dt'].dt.month
    cluster_hourly['day_of_month'] = cluster_hourly['hour_dt'].dt.day

    # Cluster centroid
    centroids = node_clusters.groupby('cluster')[['lat', 'lon']].mean()
    centroids.columns = ['cluster_lat', 'cluster_lon']
    cluster_hourly = cluster_hourly.merge(centroids, on='cluster', how='left')

    print(f"\nCluster-hour dataset: {len(cluster_hourly):,} observations")
    print(f"  Clusters: {cluster_hourly['cluster'].nunique()}")
    print(f"  Hours: {cluster_hourly['hour'].nunique()}")
    print(f"  Nodes per cluster-hour: "
          f"mean={cluster_hourly['n_nodes_in_hour'].mean():.1f}, "
          f"min={cluster_hourly['n_nodes_in_hour'].min()}")
    source = "cluster polygons" if use_polygons else "node-attached stations (fallback)"
    print(f"  Weather source: {source}")

    return cluster_hourly


def _aggregate_weather_from_polygons(station_errors, cluster_polygons, leads):
    """
    Spatial join stations to cluster polygons, then aggregate weather per cluster-hour.
    """
    # Build station GeoDataFrame from unique station positions
    station_meta = (
        station_errors[['station_id', 'station_lat', 'station_lon']]
        .dropna(subset=['station_lat', 'station_lon'])
        .drop_duplicates('station_id')
    )
    stations_gdf = gpd.GeoDataFrame(
        station_meta,
        geometry=gpd.points_from_xy(station_meta['station_lon'], station_meta['station_lat']),
        crs='EPSG:4326'
    )

    # Spatial join: each station → cluster polygon it falls in
    # Reset index to ensure 'cluster' appears as a regular column after sjoin
    polys = cluster_polygons[['cluster', 'geometry']].reset_index(drop=True)
    joined_raw = gpd.sjoin(
        stations_gdf[['station_id', 'geometry']],
        polys,
        how='inner',
        predicate='within'
    )
    # geopandas may suffix right-side columns; handle both cases
    print(f"  sjoin columns: {list(joined_raw.columns)}")
    if 'cluster' in joined_raw.columns:
        joined = joined_raw[['station_id', 'cluster']]
    elif 'cluster_right' in joined_raw.columns:
        joined = joined_raw[['station_id', 'cluster_right']].rename(columns={'cluster_right': 'cluster'})
    else:
        # Fall back: cluster is stored in the index (geopandas index_right)
        cluster_col = [c for c in joined_raw.columns if 'cluster' in c.lower()]
        raise KeyError(f"'cluster' not found after sjoin. Available columns: {list(joined_raw.columns)}, cluster-like: {cluster_col}")
    joined = joined.drop_duplicates()

    n_matched = joined['station_id'].nunique()
    n_total = len(station_meta)
    print(f"  Stations inside cluster polygons: {n_matched} / {n_total}")

    # Attach cluster to station-hour errors
    station_cluster = station_errors.merge(joined, on='station_id', how='inner')

    return _compute_weather_aggs(station_cluster, leads)


def _aggregate_weather_from_nodes(df_with_cluster, leads):
    """
    Aggregate weather from node-attached errors (one observation per node).
    Deduplicate so each (node/station, cluster, hour) contributes once.
    """
    error_prefixes = ('temp_error', 'wspd_error', 'wdir_degree_error',
                      'observed_temp', 'observed_wspd', 'observed_wdir',
                      'forecast_temp', 'forecast_wspd')
    weather_cols = [c for c in df_with_cluster.columns
                    if c.startswith(error_prefixes)]
    # Use station_id if available (station path), else settlement_point (ERA5 path)
    id_col = 'station_id' if 'station_id' in df_with_cluster.columns else 'settlement_point'
    keep = [id_col, 'cluster', 'hour'] + weather_cols
    station_cluster = (
        df_with_cluster[keep]
        .drop_duplicates(subset=[id_col, 'cluster', 'hour'])
    )
    return _compute_weather_aggs(station_cluster, leads)


def _compute_weather_aggs(station_cluster, leads):
    """
    Compute per (cluster, hour) aggregations for forecast errors and observed weather.

    For each lead time:
      - temp_error, wspd_error, wdir_degree_error: mean, std, max(|error|)
      - observed_temp, observed_wspd: mean, std, max, min
    """
    agg_dict = {}

    for lead in leads:
        suffix = f'_{lead}h'

        for err_col in [f'temp_error{suffix}', f'wspd_error{suffix}',
                        f'wdir_degree_error{suffix}']:
            if err_col in station_cluster.columns:
                agg_dict[err_col] = ['mean', 'std']

    for obs_col in ['observed_temp', 'observed_wspd', 'observed_wdir']:
        if obs_col in station_cluster.columns:
            agg_dict[obs_col] = ['mean', 'std', 'max', 'min']

    if not agg_dict:
        return pd.DataFrame(columns=['cluster', 'hour'])

    grouped = station_cluster.groupby(['cluster', 'hour']).agg(agg_dict).reset_index()

    # Flatten MultiIndex columns: (col, agg) → col_agg, except 'mean' → col
    new_cols = []
    for col in grouped.columns:
        if not isinstance(col, tuple):
            new_cols.append(col)
            continue
        col_name, agg_func = col
        if agg_func in ('mean', ''):
            new_cols.append(col_name)
        else:
            new_cols.append(f'{col_name}_{agg_func}')
    grouped.columns = new_cols

    # Add max_abs_error columns from the mean-aggregated data
    # (we need to re-aggregate abs values separately)
    for lead in leads:
        suffix = f'_{lead}h'
        for err_col in [f'temp_error{suffix}', f'wspd_error{suffix}',
                        f'wdir_degree_error{suffix}']:
            if err_col not in station_cluster.columns:
                continue
            max_abs_col = f'max_abs_{err_col}'
            max_abs = (
                station_cluster
                .groupby(['cluster', 'hour'])[err_col]
                .agg(lambda x: x.abs().max())
                .rename(max_abs_col)
                .reset_index()
            )
            grouped = grouped.merge(max_abs, on=['cluster', 'hour'], how='left')

    return grouped


# ── EIA generator mix by cluster ─────────────────────────────────────────────


def compute_cluster_generation_mix(node_clusters, cluster_polygons, generators_path):
    """
    Compute the generation technology mix for each cluster from EIA Form 860
    generator data.

    Each generator is spatially assigned to the cluster whose convex-hull
    polygon it falls inside.  Generators that fall outside every polygon are
    assigned to the nearest cluster centroid as a fallback (covers plants near
    polygon boundaries or in lightly-populated areas of Texas).

    For every cluster the function returns two capacity measures per technology:

    * **nameplate_mw** – sum of nameplate_capacity_mw
    * **scaled_mw**    – sum of nameplate_capacity_mw × nameplate_power_factor
                         (available / effective capacity)

    Technology columns are produced at two levels of granularity:

    * **Fine-grained** – one column per EIA technology string, slug-formatted
      (e.g. ``nameplate_mw_natural_gas_fired_combined_cycle``).
    * **Broad category** – gas, nuclear, coal, solar, wind, other
      (e.g. ``nameplate_mw_gas``, ``scaled_mw_wind``).

    Broad-category mapping:
      gas    → Natural Gas * (all four NG technology types)
      nuclear → Nuclear
      coal   → Conventional Steam Coal
      solar  → Solar Photovoltaic
      wind   → Onshore Wind Turbine
      other  → everything else (petroleum, batteries, hydro, biomass, …)

    Args:
        node_clusters:    DataFrame with columns [settlement_point, cluster,
                          lat, lon] – output of cluster_nodes().
        cluster_polygons: GeoDataFrame with columns [cluster, geometry] in
                          EPSG:4326 – output of build_cluster_polygons().
        generators_path:  Path to the EIA 860 Texas generators CSV.  Expected
                          columns: technology, nameplate_capacity_mw,
                          nameplate_power_factor, lat, lon.

    Returns:
        DataFrame with one row per cluster and columns:
          cluster,
          n_generators,
          total_nameplate_mw,
          total_scaled_mw,
          nameplate_mw_<broad_cat>,   scaled_mw_<broad_cat>,   … (6 cats)
          nameplate_mw_<tech_slug>,   scaled_mw_<tech_slug>,   … (per EIA tech)
    """
    # ── 1. Load generators and build GeoDataFrame ──────────────────────────
    gen = pd.read_csv(generators_path)
    gen["nameplate_capacity_mw"] = pd.to_numeric(gen["nameplate_capacity_mw"],
                                                  errors="coerce")
    gen["nameplate_power_factor"] = pd.to_numeric(gen["nameplate_power_factor"],
                                                   errors="coerce")
    gen = gen.dropna(subset=["lat", "lon", "nameplate_capacity_mw",
                              "nameplate_power_factor", "technology"])
    gen["scaled_mw"] = gen["nameplate_capacity_mw"] * gen["nameplate_power_factor"]

    gen_gdf = gpd.GeoDataFrame(
        gen,
        geometry=gpd.points_from_xy(gen["lon"], gen["lat"]),
        crs="EPSG:4326",
    )

    # ── 2. Spatial join: generators → cluster polygons (within) ───────────
    polys = cluster_polygons[["cluster", "geometry"]].reset_index(drop=True)

    joined = gpd.sjoin(
        gen_gdf[["technology", "nameplate_capacity_mw", "scaled_mw", "geometry"]],
        polys,
        how="left",
        predicate="within",
    )
    # Normalise the cluster column name (geopandas may suffix it)
    if "cluster" not in joined.columns and "cluster_right" in joined.columns:
        joined = joined.rename(columns={"cluster_right": "cluster"})

    # ── 3. Fallback: assign unmatched generators to nearest cluster centroid
    unmatched_mask = joined["cluster"].isna()
    n_unmatched = unmatched_mask.sum()
    if n_unmatched > 0:
        centroids = (
            node_clusters.groupby("cluster")[["lat", "lon"]].mean().reset_index()
        )
        centroids_gdf = gpd.GeoDataFrame(
            centroids,
            geometry=gpd.points_from_xy(centroids["lon"], centroids["lat"]),
            crs="EPSG:4326",
        ).to_crs("EPSG:3857")

        unmatched_pts = gen_gdf.loc[unmatched_mask].to_crs("EPSG:3857")
        nearest = gpd.sjoin_nearest(
            unmatched_pts[["technology", "nameplate_capacity_mw",
                           "scaled_mw", "geometry"]],
            centroids_gdf[["cluster", "geometry"]],
            how="left",
        )
        if "cluster_right" in nearest.columns:
            nearest = nearest.rename(columns={"cluster_right": "cluster"})

        joined.loc[unmatched_mask, "cluster"] = nearest["cluster"].values
        print(f"  Generation mix: {n_unmatched} generators assigned via "
              f"nearest-centroid fallback")

    joined["cluster"] = joined["cluster"].astype(int)

    # ── 4. Broad-category column ───────────────────────────────────────────
    joined["broad_cat"] = joined["technology"].map(_BROAD_CATEGORY_MAP).fillna("other")

    # ── 5. Aggregate ───────────────────────────────────────────────────────
    cluster_ids = sorted(joined["cluster"].unique())

    # 5a. Fine-grained technology pivot
    # Fine-grained columns are prefixed with "tech_" to avoid collisions with
    # the broad-category columns (e.g. EIA "Nuclear" → slug "nuclear" would
    # otherwise clash with broad-cat column "nameplate_mw_nuclear").
    fine_nameplate = (
        joined.groupby(["cluster", "technology"])["nameplate_capacity_mw"]
        .sum()
        .unstack(fill_value=0)
    )
    fine_nameplate.columns = [
        f"nameplate_mw_tech_{_tech_slug(c)}" for c in fine_nameplate.columns
    ]

    fine_scaled = (
        joined.groupby(["cluster", "technology"])["scaled_mw"]
        .sum()
        .unstack(fill_value=0)
    )
    fine_scaled.columns = [
        f"scaled_mw_tech_{_tech_slug(c)}" for c in fine_scaled.columns
    ]

    # 5b. Broad-category pivot
    broad_nameplate = (
        joined.groupby(["cluster", "broad_cat"])["nameplate_capacity_mw"]
        .sum()
        .unstack(fill_value=0)
    )
    # Ensure all broad categories are present even if zero
    for cat in _BROAD_CATEGORIES:
        if cat not in broad_nameplate.columns:
            broad_nameplate[cat] = 0.0
    broad_nameplate = broad_nameplate[_BROAD_CATEGORIES]
    broad_nameplate.columns = [f"nameplate_mw_{c}" for c in broad_nameplate.columns]

    broad_scaled = (
        joined.groupby(["cluster", "broad_cat"])["scaled_mw"]
        .sum()
        .unstack(fill_value=0)
    )
    for cat in _BROAD_CATEGORIES:
        if cat not in broad_scaled.columns:
            broad_scaled[cat] = 0.0
    broad_scaled = broad_scaled[_BROAD_CATEGORIES]
    broad_scaled.columns = [f"scaled_mw_{c}" for c in broad_scaled.columns]

    # 5c. Totals and generator count
    totals = (
        joined.groupby("cluster")
        .agg(
            n_generators=("nameplate_capacity_mw", "count"),
            total_nameplate_mw=("nameplate_capacity_mw", "sum"),
            total_scaled_mw=("scaled_mw", "sum"),
        )
    )

    # ── 6. Combine and fill any clusters with zero generators ─────────────
    all_clusters = pd.DataFrame({"cluster": cluster_ids}).set_index("cluster")
    result = (
        all_clusters
        .join(totals, how="left")
        .join(broad_nameplate, how="left")
        .join(broad_scaled, how="left")
        .join(fine_nameplate, how="left")
        .join(fine_scaled, how="left")
        .fillna(0)
        .reset_index()
    )
    result["n_generators"] = result["n_generators"].astype(int)

    # ── 7. Summary print ──────────────────────────────────────────────────
    print(f"Generation mix computed for {len(result)} clusters "
          f"({int(result['n_generators'].sum())} generators, "
          f"{result['total_nameplate_mw'].sum():.0f} MW total nameplate)")
    for cat in _BROAD_CATEGORIES:
        mw = result[f"nameplate_mw_{cat}"].sum()
        if mw > 0:
            print(f"  {cat:8s}: {mw:8,.0f} MW nameplate")

    return result


# ── Canonical cluster-hour dataset builder ────────────────────────────────────


def build_cluster_hourly_data(
    months, models=None, n_clusters=9, geo_weight=10.0, n_neighbors=8,
    generators_path=None, force_rebuild=False,
    error_source='station',
    model=None,  # backward compat: single model name (str) → converted to models dict
):
    """
    Build (or load from cache) the canonical cluster × hour analysis dataset.

    Runs the full pipeline end-to-end:
      prepare_node_level_data → compute_node_lmp_features → cluster_nodes →
      build_cluster_polygons → aggregate_to_cluster_hour →
      compute_cluster_generation_mix

    Supports loading forecast errors from multiple models simultaneously
    (e.g. HRRR 1h + GFS day-ahead).

    Three artefacts are cached in ``dirs["processed"]`` and reloaded on
    subsequent calls (set ``force_rebuild=True`` to bypass):

    * ``cluster_hourly_{models_key}_k{n_clusters}_{months_tag}.csv``
    * ``node_clusters_{models_key}_k{n_clusters}_{months_tag}.csv``
    * ``cluster_polygons_{models_key}_k{n_clusters}_{months_tag}.gpkg``

    Args:
        months:          List of (year, month) tuples, e.g. [(2025, 1), …].
        models:          Dict mapping model name → tuple of lead hours,
                         e.g. ``{'hrrr': (1,), 'gfs': (0,)}``.  Defaults to
                         combined HRRR 1h + GFS day-ahead when ``None``.
        n_clusters:      Number of clusters to form.
        geo_weight:      Geographic weight for agglomerative clustering.
        n_neighbors:     k-NN connectivity graph size for clustering.
        generators_path: Path to EIA 860 Texas generators CSV.
        force_rebuild:   If True, ignore existing cache files and rebuild.
        error_source:    ``'station'`` (default) or ``'era5'``. Passed through
                         to ``prepare_node_level_data()``.

    Returns:
        Tuple ``(cluster_hourly, node_clusters, cluster_polygons, sil_score)``.

        * ``cluster_hourly``  — DataFrame, one row per (cluster, hour),
          with all LMP, weather, load, time, and generation-mix columns.
        * ``node_clusters``   — DataFrame, one row per settlement_point,
          with lat/lon and cluster assignment.
        * ``cluster_polygons``— GeoDataFrame, one row per cluster, with
          convex-hull polygon geometry (EPSG:4326).
        * ``sil_score``       — Silhouette score (float) from clustering,
          or None when loaded from cache.
    """
    from process_data.prepare_node_level_data import prepare_node_level_data

    MODEL_LEAD_TIMES = {"hrrr": (1,), "gfs": (0,)}

    # Backward compat: accept model='hrrr' → models={'hrrr': (1,)}
    if model is not None and models is None:
        models = {model: MODEL_LEAD_TIMES[model]}
    elif models is None:
        models = dict(MODEL_LEAD_TIMES)

    models_key = '+'.join(sorted(models.keys()))
    # Combined leads across all models (e.g. (1, 0) for HRRR+GFS)
    leads = tuple(lead for model_leads in models.values() for lead in model_leads)

    dirs = setup_directories()

    # ── Cache key ─────────────────────────────────────────────────────────
    months = sorted(months)
    if len(months) == 1:
        months_tag = f"{months[0][0]}_{months[0][1]:02d}"
    else:
        first_y, first_m = months[0]
        last_y,  last_m  = months[-1]
        months_tag = f"{first_y}{first_m:02d}_{last_y}{last_m:02d}"

    source_tag = '' if error_source == 'station' else f'_{error_source}'
    key = f"{models_key}_k{n_clusters}{source_tag}_{months_tag}"
    ch_path  = os.path.join(dirs["processed"], f"cluster_hourly_{key}.csv")
    nc_path  = os.path.join(dirs["processed"], f"node_clusters_{key}.csv")
    gpkg_path = os.path.join(dirs["processed"], f"cluster_polygons_{key}.gpkg")

    cache_files = [ch_path, nc_path, gpkg_path]
    cache_exists = all(os.path.exists(p) for p in cache_files)

    if cache_exists and not force_rebuild:
        print(f"Loading cached cluster-hour data ({key})…")
        cluster_hourly  = pd.read_csv(ch_path,  parse_dates=["hour"])
        node_clusters   = pd.read_csv(nc_path)
        cluster_polygons = gpd.read_file(gpkg_path)
        return cluster_hourly, node_clusters, cluster_polygons, None

    print(f"Building cluster-hour dataset ({key}, force_rebuild={force_rebuild})…")

    # ── 1. Node-level data ────────────────────────────────────────────────
    df = prepare_node_level_data(months=months, models=models, force_rebuild=False,
                                error_source=error_source)

    # ── 2. Clustering ─────────────────────────────────────────────────────
    node_features = compute_node_lmp_features(df)
    node_clusters, sil_score = cluster_nodes(
        node_features,
        n_clusters=n_clusters,
        geo_weight=geo_weight,
        n_neighbors=n_neighbors,
    )
    cluster_polygons = build_cluster_polygons(node_clusters)

    # ── 3. Cluster × hour aggregation ─────────────────────────────────────
    if error_source == 'era5':
        # ERA5 errors are already attached to nodes at grid-cell resolution.
        # Skip station CSV loading; aggregate directly from node-attached errors.
        cluster_hourly = aggregate_to_cluster_hour(
            df, node_clusters, leads,
        )
    else:
        # Try polygon-based station aggregation; fall back to node-attached errors
        # if individual station CSV files time out (common with OneDrive sync).
        station_errors = None
        try:
            station_errors = load_station_errors_wide(months, models, dirs)
        except (TimeoutError, OSError) as exc:
            print(f"  Station error loading failed ({exc}); using node-attached fallback.")

        if station_errors is not None:
            cluster_hourly = aggregate_to_cluster_hour(
                df, node_clusters, leads,
                station_errors=station_errors,
                cluster_polygons=cluster_polygons,
            )
        else:
            cluster_hourly = aggregate_to_cluster_hour(
                df, node_clusters, leads,
            )

    # ── 4. Generation mix ─────────────────────────────────────────────────
    cluster_gen_mix = compute_cluster_generation_mix(
        node_clusters, cluster_polygons, generators_path
    )

    # Percentage shares (nameplate MW basis)
    for cat in _BROAD_CATEGORIES:
        cluster_gen_mix[f"pct_{cat}"] = (
            cluster_gen_mix[f"nameplate_mw_{cat}"]
            / cluster_gen_mix["total_nameplate_mw"] * 100
        ).fillna(0)

    # Merge all generation columns into cluster_hourly (1:m on cluster)
    gen_cols = (
        ["cluster", "n_generators", "total_nameplate_mw", "total_scaled_mw"]
        + [f"nameplate_mw_{c}" for c in _BROAD_CATEGORIES]
        + [f"scaled_mw_{c}"    for c in _BROAD_CATEGORIES]
        + [f"pct_{c}"          for c in _BROAD_CATEGORIES]
        + [c for c in cluster_gen_mix.columns if c.startswith("nameplate_mw_tech_")]
        + [c for c in cluster_gen_mix.columns if c.startswith("scaled_mw_tech_")]
    )
    cluster_hourly = cluster_hourly.merge(
        cluster_gen_mix[gen_cols], on="cluster", how="left"
    )

    # ── 5. Save cache ─────────────────────────────────────────────────────
    cluster_hourly.to_csv(ch_path, index=False)
    node_clusters.to_csv(nc_path, index=False)
    cluster_polygons.to_file(gpkg_path, driver="GPKG")

    print(f"  Saved cluster_hourly  → {ch_path}")
    print(f"  Saved node_clusters   → {nc_path}")
    print(f"  Saved cluster_polygons→ {gpkg_path}")
    print(f"  cluster_hourly shape: {cluster_hourly.shape}")

    return cluster_hourly, node_clusters, cluster_polygons, sil_score
