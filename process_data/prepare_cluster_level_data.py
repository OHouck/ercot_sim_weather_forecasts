import os
import glob
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


def load_station_errors_wide(months, model, dirs):
    """
    Load per-station forecast error CSVs and pivot lead times to wide format.

    Returns a DataFrame with one row per (station_id, hour) and columns:
      station_id, station_lat, station_lon, <error_cols>_<lead>h, <obs_cols>_<lead>h, ...

    This is the same pivot logic used inside prepare_node_level_data, but
    exposes it here so aggregate_to_cluster_hour can use all stations in a
    cluster polygon rather than just the single nearest station per node.

    Args:
        months: List of (year, month) tuples
        model: 'ndfd' or 'hrrr'
        dirs: dict from setup_directories()

    Returns:
        DataFrame with station-hour rows and wide error columns.
    """
    MODEL_LEAD_TIMES = {'ndfd': (1, 25), 'hrrr': (1, 18)}
    lead_short, lead_long = MODEL_LEAD_TIMES[model]

    all_files = []
    for year, month in sorted(months):
        error_dir = os.path.join(
            dirs['processed'], 'forecast_errors', model, str(year), f"{month:02d}"
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

    lead_short_df = all_errors[all_errors['lead_hours'] == lead_short].copy()
    lead_long_df = all_errors[all_errors['lead_hours'] == lead_long].copy()

    rename_short = {c: f'{c}_{lead_short}h' for c in error_cols}
    rename_long = {c: f'{c}_{lead_long}h' for c in error_cols}

    lead_short_df = lead_short_df.rename(columns=rename_short)
    lead_long_df = lead_long_df.rename(columns=rename_long)

    keep_short = ['station_id', 'hour', 'lat', 'lon'] + list(rename_short.values())
    keep_long = ['station_id', 'hour'] + list(rename_long.values())

    errors_wide = lead_short_df[keep_short].merge(
        lead_long_df[keep_long], on=['station_id', 'hour'], how='outer'
    )
    errors_wide = errors_wide.rename(columns={'lat': 'station_lat', 'lon': 'station_lon'})

    print(f"Loaded {len(errors_wide):,} station-hour rows "
          f"({errors_wide['station_id'].nunique()} stations, model={model})")
    return errors_wide


def aggregate_to_cluster_hour(df, node_clusters, lead_short, lead_long,
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
      - Forecast errors (per lead time): mean, std, max(|error|)
      - Observed wind speed and temperature: mean, std, max, min

    Args:
        df: Node-level DataFrame from prepare_node_level_data()
        node_clusters: DataFrame with columns [settlement_point, cluster, lat, lon]
        lead_short: Short lead time in hours (e.g. 1)
        lead_long: Long lead time in hours (e.g. 25)
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

    lmp_hourly = (
        df_with_cluster
        .groupby(['cluster', 'hour'])['lmp']
        .agg(lmp_mean='mean', lmp_std='std', lmp_max='max', lmp_min='min',
             n_nodes_in_hour='count')
        .reset_index()
    )
    lmp_hourly['lmp_std'] = lmp_hourly['lmp_std'].fillna(0)

    # ── Weather aggregation ──
    if use_polygons:
        weather_hourly = _aggregate_weather_from_polygons(
            station_errors, cluster_polygons, lead_short, lead_long
        )
    else:
        # Fallback: use node-attached station errors already in df
        weather_hourly = _aggregate_weather_from_nodes(
            df_with_cluster, lead_short, lead_long
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


def _aggregate_weather_from_polygons(station_errors, cluster_polygons, lead_short, lead_long):
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

    return _compute_weather_aggs(station_cluster, lead_short, lead_long)


def _aggregate_weather_from_nodes(df_with_cluster, lead_short, lead_long):
    """
    Aggregate weather from node-attached station errors (one station per node).
    Deduplicate so each (station_id, cluster, hour) contributes once.
    """
    error_prefixes = ('temp_error', 'wspd_error', 'wdir_degree_error',
                      'observed_temp', 'observed_wspd', 'forecast_temp', 'forecast_wspd')
    weather_cols = [c for c in df_with_cluster.columns
                    if c.startswith(error_prefixes)]
    keep = ['station_id', 'cluster', 'hour'] + weather_cols
    station_cluster = (
        df_with_cluster[keep]
        .drop_duplicates(subset=['station_id', 'cluster', 'hour'])
    )
    return _compute_weather_aggs(station_cluster, lead_short, lead_long)


def _compute_weather_aggs(station_cluster, lead_short, lead_long):
    """
    Compute per (cluster, hour) aggregations for forecast errors and observed weather.

    For each lead time:
      - temp_error, wspd_error, wdir_degree_error: mean, std, max(|error|)
      - observed_temp, observed_wspd: mean, std, max, min
    """
    agg_dict = {}

    for lead in [lead_short, lead_long]:
        suffix = f'_{lead}h'

        for err_col in [f'temp_error{suffix}', f'wspd_error{suffix}',
                        f'wdir_degree_error{suffix}']:
            if err_col in station_cluster.columns:
                agg_dict[err_col] = ['mean', 'std']

        for obs_col in [f'observed_temp{suffix}', f'observed_wspd{suffix}']:
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
    for lead in [lead_short, lead_long]:
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
