"""
Forecast Error Map EDA
======================

For each ERCOT node cluster, identifies hours with the largest forecast error
("treatment") and finds a comparable nearby hour with near-average forecast error
("control"). Creates side-by-side LMP maps showing how price gradients differ
between the two cases.

Configuration controls WEATHER_VAR and LEAD_TIME; each run produces 18 maps
(2 per cluster × 9 clusters by default) plus an updated LaTeX document.
"""

import os
import sys
import textwrap
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import cartopy.crs as ccrs
import cartopy.io.shapereader as shpreader

sys.path.insert(0, str(Path(__file__).parent))
from helper_funcs import setup_directories
from process_data.prepare_node_level_data import prepare_node_level_data
from process_data.prepare_cluster_level_data import (
    compute_node_lmp_features,
    cluster_nodes,
    build_cluster_polygons,
    aggregate_to_cluster_hour,
    load_station_errors_wide,
)

# =============================================================================
# ── Configuration ─────────────────────────────────────────────────────────────
# =============================================================================

WEATHER_VAR = 'temp'   # 'wspd' or 'temp'
LEAD_TIME   = 1        # 1 or 18 for HRRR; 1 or 25 for NDFD

# Data
MODEL  = 'hrrr'
MONTHS = [(2025, m) for m in range(1, 9)]   # Jan–Aug 2025

# Clustering (mirrors cluster_node_lr.qmd)
N_CLUSTERS  = 9
GEO_WEIGHT  = 10.0
N_NEIGHBORS = 8

# Control matching
WINDOW_DAYS     = 14   # days around treatment date to search for control
ERROR_TOLERANCE = 0.5  # control error must be within ±(ERROR_TOLERANCE × std) of mean

# =============================================================================
# ── Helpers ───────────────────────────────────────────────────────────────────
# =============================================================================

LEAD_TIMES = {'ndfd': (1, 25), 'hrrr': (1, 18)}
UNITS      = {'wspd': 'm/s', 'temp': '°C'}


def find_treatment_control(cluster_hourly, cluster_id, error_col, window_days, error_tolerance):
    """
    Return (treatment_row, control_row) as pd.Series from cluster_hourly.

    Treatment: the hour with the largest abs(error_col) for this cluster.
    Control:   same hour_of_day, within ±window_days of treatment date, with
               error closest to the cluster mean. Falls back to wider tolerances
               (1.0× then 2.0× std dev, then any hour of that hour_of_day) if
               no candidate is found.
    """
    subset = cluster_hourly[cluster_hourly['cluster'] == cluster_id].copy()
    subset = subset.dropna(subset=[error_col])

    if subset.empty:
        raise ValueError(f"No data for cluster {cluster_id} with column {error_col}")

    subset['hour_dt_ts'] = pd.to_datetime(subset['hour_dt'])

    # Treatment: max absolute error
    treat_idx = subset[error_col].abs().idxmax()
    treatment = subset.loc[treat_idx]
    treat_dt  = treatment['hour_dt_ts']
    treat_hod = int(treatment['hour_of_day'])

    mean_err = subset[error_col].mean()
    std_err  = max(subset[error_col].std(), 1e-6)

    # Exclude treatment hour from control candidates
    candidates = subset[subset.index != treat_idx].copy()
    candidates = candidates[candidates['hour_of_day'] == treat_hod]
    delta_days = (candidates['hour_dt_ts'] - treat_dt).abs()
    candidates = candidates[delta_days <= pd.Timedelta(days=window_days)]
    candidates = candidates.copy()
    candidates['_dist_from_mean'] = (candidates[error_col] - mean_err).abs()

    # Progressive fallback on tolerance
    for tol_factor in [error_tolerance, 1.0, 2.0]:
        mask = candidates['_dist_from_mean'] <= tol_factor * std_err
        if mask.any():
            control = candidates.loc[candidates.loc[mask, '_dist_from_mean'].idxmin()]
            return treatment, control

    # Last resort: any hour_of_day match, ignoring window
    all_hod = subset[(subset.index != treat_idx) & (subset['hour_of_day'] == treat_hod)].copy()
    if not all_hod.empty:
        all_hod['_dist_from_mean'] = (all_hod[error_col] - mean_err).abs()
        control = all_hod.loc[all_hod['_dist_from_mean'].idxmin()]
        print(f"  Warning: cluster {cluster_id} — fell back to unconstrained control window")
        return treatment, control

    # Absolute last resort: any hour
    fallback = subset[subset.index != treat_idx].copy()
    fallback['_dist_from_mean'] = (fallback[error_col] - mean_err).abs()
    control = fallback.loc[fallback['_dist_from_mean'].idxmin()]
    print(f"  Warning: cluster {cluster_id} — no same-hour control found, using closest error")
    return treatment, control


def _get_hour_lmp(df, node_clusters, hour_dt_val):
    """
    Return merged DataFrame of (settlement_point, lat, lon, cluster, lmp_mean)
    for all nodes that have data in the given hour.
    """
    hour_dt_val = pd.to_datetime(hour_dt_val)
    hour_data = (
        df[pd.to_datetime(df['hour']) == hour_dt_val][['settlement_point', 'lmp_mean']]
        .dropna(subset=['lmp_mean'])
    )
    merged = hour_data.merge(
        node_clusters[['settlement_point', 'lat', 'lon', 'cluster']],
        on='settlement_point',
        how='inner',
    )
    return merged


def plot_lmp_map(
    df,
    node_clusters,
    cluster_polygons,
    cluster_id,
    hour_dt_val,
    case_label,
    error_val,
    error_col,
    unit,
    mean_err,
    hour_wspd_err,
    hour_temp_err,
    weather_var,
    actual_load,
    vmin,
    vmax,
    save_path=None,
):
    """
    Plot a Texas map with all nodes colored by LMP. The highlighted cluster is
    outlined with a dashed polygon boundary.

    Annotation box shows wind and temperature forecast errors for the cluster
    at this specific hour, plus the cluster-wide mean and total system load.
    """
    hour_dt_val = pd.to_datetime(hour_dt_val)
    all_hour_lmp = _get_hour_lmp(df, node_clusters, hour_dt_val)

    proj = ccrs.PlateCarree()
    fig, ax = plt.subplots(figsize=(12, 9), subplot_kw={'projection': proj})

    # Texas state outline
    states_shp = shpreader.natural_earth(
        resolution='10m', category='cultural', name='admin_1_states_provinces')
    for record in shpreader.Reader(states_shp).records():
        if record.attributes.get('name') == 'Texas':
            ax.add_geometries(
                [record.geometry], proj,
                facecolor='#f5f5f5', edgecolor='#333333', linewidth=1.2,
                zorder=1,
            )
            break
    ax.set_extent([-107.5, -93.0, 25.5, 37.0], crs=proj)

    if all_hour_lmp.empty:
        ax.set_title(f"Cluster {cluster_id} [{case_label}] — NO DATA", fontsize=12)
        if save_path:
            fig.savefig(save_path, dpi=150, bbox_inches='tight')
        plt.close(fig)
        return

    # All nodes colored by LMP
    # norm = mcolors.Normalize(vmin=vmin, vmax=vmax)
    norm = mcolors.Normalize(vmin=0, vmax=85) # fix scale for all maps to make them comparable
    sc = ax.scatter(
        all_hour_lmp['lon'], all_hour_lmp['lat'],
        c=all_hour_lmp['lmp_mean'],
        cmap='plasma',
        norm=norm,
        s=35, edgecolors='k', linewidths=0.2, alpha=0.88,
        transform=proj, zorder=4,
    )

    # Cluster polygon outline to identify the highlighted cluster
    cluster_row = cluster_polygons[cluster_polygons['cluster'] == cluster_id]
    if not cluster_row.empty:
        ax.add_geometries(
            cluster_row['geometry'].values, proj,
            facecolor='none', edgecolor='steelblue', linewidth=2.0,
            linestyle='--', zorder=5, alpha=0.9,
        )

    # Colorbar
    cbar = fig.colorbar(sc, ax=ax, orientation='vertical', shrink=0.7, pad=0.02)
    cbar.set_label('Real Time Local Marginal Price ($/MWh)', fontsize=10)

    # Annotation box
    hod = int(hour_dt_val.hour)
    date_str = hour_dt_val.strftime('%Y-%m-%d')
    wspd_str = f"{hour_wspd_err:.2f}" if hour_wspd_err is not None and not np.isnan(hour_wspd_err) else 'N/A'
    temp_str = f"{hour_temp_err:.2f}" if hour_temp_err is not None and not np.isnan(hour_temp_err) else 'N/A'
    mean_err_str = f"{mean_err:.2f}"
    err_str = f"{error_val:.2f}" if not np.isnan(error_val) else 'N/A'
    load_str = f"{actual_load:,.0f} MW" if actual_load is not None and not np.isnan(actual_load) else 'N/A'

    # Primary line shows the treatment variable with cluster mean; secondary shows the other
    if weather_var == 'wspd':
        primary = f"wspd error: {err_str} m/s  [cluster mean: {mean_err_str} m/s]"
        secondary = f"temp error: {temp_str} °C"
    else:
        primary = f"temp error: {err_str} °C  [cluster mean: {mean_err_str} °C]"
        secondary = f"wspd error: {wspd_str} m/s"

    annotation = (
        f"Cluster {cluster_id} — {case_label}\n"
        f"Lead {LEAD_TIME}h forecast errors:\n"
        f"  {primary}\n"
        f"  {secondary}\n"
        f"Regional load: {load_str}"
    )
    ax.text(
        0.02, 0.97, annotation,
        transform=ax.transAxes,
        fontsize=8.5,
        verticalalignment='top',
        bbox=dict(boxstyle='round,pad=0.4', facecolor='white',
                  edgecolor='lightgray', alpha=0.85),
        zorder=10,
    )

    ax.set_title(
        f"Cluster {cluster_id} [{case_label}] — {date_str} {hod:02d}:00",
        fontsize=12,
    )
    ax.gridlines(draw_labels=True, linewidth=0.3, alpha=0.4)

    plt.tight_layout()
    if save_path:
        fig.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"  Saved: {os.path.basename(save_path)}")
    plt.close(fig)


def write_latex(tex_path, output_dir, generated_files, weather_var, lead_time,
                model, months, n_clusters):
    """
    Write a standalone LaTeX document showing all generated maps.
    generated_files: list of (cluster_id, case, filename) tuples.
    """
    month_str = f"{months[0][0]}-{months[0][1]:02d} to {months[-1][0]}-{months[-1][1]:02d}"
    unit = UNITS.get(weather_var, '')
    var_long = 'wind speed' if weather_var == 'wspd' else 'temperature'

    # Group by cluster
    by_cluster = {}
    for c, case, fname in generated_files:
        by_cluster.setdefault(c, {})[case] = fname

    lines = [
        r'\documentclass[11pt]{article}',
        r'\usepackage{graphicx}',
        r'\usepackage{geometry}',
        r'\usepackage{booktabs}',
        r'\usepackage{caption}',
        r'\usepackage{subcaption}',
        r'\geometry{margin=0.75in, top=0.6in, bottom=0.6in}',
        r'\setlength{\parskip}{0pt}',
        r'\setlength{\intextsep}{6pt}',
        r'\graphicspath{{' + output_dir.rstrip('/') + r'/}}',
        r'',
        r'\begin{document}',
        r'',
        r'\title{Forecast Error Map EDA \\[4pt]',
        rf'\large {var_long.capitalize()} errors, lead {lead_time}h ({model.upper()})}}',
        r'\author{}',
        r'\date{\today}',
        r'\maketitle',
        r'',
        r'\section*{Configuration}',
        r'\begin{tabular}{ll}',
        r'\toprule',
        rf'Weather variable & \texttt{{{weather_var}}} ({var_long}, {unit}) \\',
        rf'Lead time & {lead_time}h \\',
        rf'Model & {model.upper()} \\',
        rf'Months & {month_str} \\',
        rf'Clusters & {n_clusters} \\',
        r'\bottomrule',
        r'\end{tabular}',
        r'',
        r'\bigskip',
        textwrap.dedent(rf"""
        For each cluster, the \textbf{{treatment}} map shows the hour with the
        largest absolute mean {var_long} forecast error in that cluster.
        The \textbf{{control}} map shows a nearby hour (within {WINDOW_DAYS} days)
        at the same hour of day with {var_long} error close to the cluster mean.
        Node colors show real-time LMP (\$/MWh); the color scale is shared between
        treatment and control within each cluster.
        Annotations report the {var_long} error for that hour alongside both wind
        speed and temperature errors for reference.
        """).strip(),
        r'',
    ]

    for c in sorted(by_cluster.keys()):
        cases = by_cluster[c]
        treat_fname = cases.get('treatment', '')
        ctrl_fname  = cases.get('control', '')

        lines += [
            r'\begin{figure}[htbp]',
            r'  \centering',
            r'  \begin{subfigure}[t]{0.48\textwidth}',
            r'    \centering',
        ]
        if treat_fname:
            lines += [
                rf'    \includegraphics[width=\textwidth]{{{treat_fname}}}',
                r'    \caption{Treatment (large error)}',
            ]
        lines += [
            r'  \end{subfigure}',
            r'  \hfill',
            r'  \begin{subfigure}[t]{0.48\textwidth}',
            r'    \centering',
        ]
        if ctrl_fname:
            lines += [
                rf'    \includegraphics[width=\textwidth]{{{ctrl_fname}}}',
                r'    \caption{Control (near-mean error)}',
            ]
        lines += [
            r'  \end{subfigure}',
            rf'  \caption{{Cluster {c}: LMP maps for treatment vs.\ control hours '
            rf'({var_long} error, lead {lead_time}h).}}',
            r'\end{figure}',
            r'\vspace{-8pt}',
        ]

    lines += [r'\end{document}', r'']

    with open(tex_path, 'w') as f:
        f.write('\n'.join(lines))
    print(f"LaTeX written to {tex_path}")


# =============================================================================
# ── Main ──────────────────────────────────────────────────────────────────────
# =============================================================================

def main():
    dirs = setup_directories()
    LEAD_SHORT, LEAD_LONG = LEAD_TIMES[MODEL]

    # ── Load node-level data ──────────────────────────────────────────────────
    print("Loading node-level data...")
    df = prepare_node_level_data(months=MONTHS, model=MODEL, force_rebuild=False)
    df['hour'] = pd.to_datetime(df['hour'])
    if 'hour_dt' in df.columns:
        df['hour_dt'] = pd.to_datetime(df['hour_dt'])

    print(f"  {len(df):,} node-hour obs, "
          f"{df['settlement_point'].nunique()} nodes, "
          f"{df['hour'].nunique()} hours")

    # ── Cluster nodes ─────────────────────────────────────────────────────────
    print("\nClustering nodes...")
    node_features = compute_node_lmp_features(df)
    node_clusters, sil_score = cluster_nodes(
        node_features,
        n_clusters=N_CLUSTERS,
        geo_weight=GEO_WEIGHT,
        n_neighbors=N_NEIGHBORS,
    )
    cluster_polygons = build_cluster_polygons(node_clusters)
    print(f"  {node_clusters['cluster'].nunique()} clusters, silhouette={sil_score:.3f}")

    # ── Aggregate to cluster × hour ───────────────────────────────────────────
    print("\nAggregating to cluster × hour...")
    station_errors = load_station_errors_wide(MONTHS, MODEL, dirs)
    cluster_hourly = aggregate_to_cluster_hour(
        df, node_clusters, LEAD_SHORT, LEAD_LONG,
        station_errors=station_errors,
        cluster_polygons=cluster_polygons,
    )
    cluster_hourly['hour_dt'] = pd.to_datetime(cluster_hourly['hour_dt'])

    error_col = f'{WEATHER_VAR}_error_{LEAD_TIME}h'
    wspd_col  = f'wspd_error_{LEAD_TIME}h'
    temp_col  = f'temp_error_{LEAD_TIME}h'

    if error_col not in cluster_hourly.columns:
        raise ValueError(
            f"Column '{error_col}' not found in cluster_hourly. "
            f"Available error columns: "
            f"{[c for c in cluster_hourly.columns if 'error' in c]}"
        )

    unit = UNITS[WEATHER_VAR]

    # ── Output directory ──────────────────────────────────────────────────────
    output_dir = os.path.join(dirs['figures'], 'forecast_error_map_eda')
    os.makedirs(output_dir, exist_ok=True)

    # ── Cluster-level means for annotation ───────────────────────────────────
    # De-duplicate columns in case error_col == wspd_col (when WEATHER_VAR='wspd')
    mean_cols = list(dict.fromkeys([error_col, wspd_col, temp_col]))
    cluster_means = cluster_hourly.groupby('cluster')[mean_cols].mean()

    # ── Generate maps ─────────────────────────────────────────────────────────
    print(f"\nGenerating maps (WEATHER_VAR={WEATHER_VAR}, LEAD_TIME={LEAD_TIME}h)...")
    generated_files = []

    for c in sorted(cluster_hourly['cluster'].unique()):
        print(f"\nCluster {c}:")

        try:
            treatment, control = find_treatment_control(
                cluster_hourly, c, error_col, WINDOW_DAYS, ERROR_TOLERANCE,
            )
        except ValueError as e:
            print(f"  Skipping cluster {c}: {e}")
            continue

        treat_dt = pd.to_datetime(treatment['hour_dt'])
        ctrl_dt  = pd.to_datetime(control['hour_dt'])
        print(f"  Treatment: {treat_dt}  {error_col}={treatment[error_col]:.2f}")
        print(f"  Control:   {ctrl_dt}   {error_col}={control[error_col]:.2f}  "
              f"(cluster mean={cluster_means.loc[c, error_col]:.2f})")

        # Shared LMP color scale: based on ALL nodes at both hours (5th–95th pct)
        treat_lmp = _get_hour_lmp(df, node_clusters, treat_dt)
        ctrl_lmp  = _get_hour_lmp(df, node_clusters, ctrl_dt)
        all_vals = pd.concat([treat_lmp['lmp_mean'], ctrl_lmp['lmp_mean']]).dropna()

        if all_vals.empty:
            print(f"  Skipping cluster {c}: no LMP data for treatment/control hours")
            continue

        vmin = float(np.percentile(all_vals, 5))
        vmax = float(np.percentile(all_vals, 95))
        if vmin >= vmax:
            vmin, vmax = float(all_vals.min()), float(all_vals.max())

        mean_err = float(cluster_means.loc[c, error_col])

        for row, lmp_hour_df, case in [
            (treatment, treat_lmp, 'treatment'),
            (control,   ctrl_lmp,  'control'),
        ]:
            fname = f'{WEATHER_VAR}_{LEAD_TIME}h_cluster{c:02d}_{case}.png'
            save_path = os.path.join(output_dir, fname)

            hour_wspd = row.get(wspd_col, np.nan)
            hour_temp = row.get(temp_col, np.nan)
            hour_load = row.get('actual_load', np.nan)

            plot_lmp_map(
                df=df,
                node_clusters=node_clusters,
                cluster_polygons=cluster_polygons,
                cluster_id=c,
                hour_dt_val=pd.to_datetime(row['hour_dt']),
                case_label=case.capitalize(),
                error_val=row[error_col],
                error_col=error_col,
                unit=unit,
                mean_err=mean_err,
                hour_wspd_err=hour_wspd,
                hour_temp_err=hour_temp,
                weather_var=WEATHER_VAR,
                actual_load=hour_load,
                vmin=vmin,
                vmax=vmax,
                save_path=save_path,
            )
            generated_files.append((c, case, fname))

    # ── Write LaTeX ───────────────────────────────────────────────────────────
    tex_path = os.path.join(
        Path(__file__).parent,
        'reports', 'forecast_error_map_eda', 'forecast_error_map_eda.tex',
    )
    print(f"\nWriting LaTeX report...")
    write_latex(
        tex_path=str(tex_path),
        output_dir=output_dir,
        generated_files=generated_files,
        weather_var=WEATHER_VAR,
        lead_time=LEAD_TIME,
        model=MODEL,
        months=MONTHS,
        n_clusters=N_CLUSTERS,
    )

    print(f"\nDone. {len(generated_files)} maps saved to {output_dir}")


if __name__ == '__main__':
    main()
