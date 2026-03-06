"""validate_data.py — Check completeness and coverage of downloaded data."""

import os
import sys
import glob
import calendar
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import geopandas as gpd
import cartopy.crs as ccrs
import cartopy.io.shapereader as shpreader

sys.path.insert(0, str(Path(__file__).parent.parent))
from helper_funcs import setup_directories


def validate_data(year=2025, month=7):
    """Check completeness of downloaded data for a given month.

    Args:
        year: Integer year (e.g. 2025)
        month: Integer month (e.g. 7)
    """
    dirs = setup_directories()
    raw = dirs['raw']
    all_ok = True
    num_days = calendar.monthrange(year, month)[1]
    month_str = f"{month:02d}"
    month_name = calendar.month_name[month]

    print("=" * 60)
    print(f"Data Validation: {month_name} {year}")
    print("=" * 60)

    # 1. NDFD Forecasts
    print("\n--- NDFD Forecasts ---")
    for elem in ['temp', 'wspd', 'wdir']:
        nc_dir = Path(raw) / 'ndfd_data' / elem / str(year) / month_str
        if nc_dir.exists():
            nc_files = list(nc_dir.glob("*.nc"))
            status = "ok" if len(nc_files) >= 200 else "LOW"
            print(f"  {elem}: {len(nc_files)} files [{status}]")
            if len(nc_files) < 200:
                all_ok = False
        else:
            print(f"  {elem}: MISSING")
            all_ok = False

    # 1b. HRRR Forecasts
    print("\n--- HRRR Forecasts ---")
    hrrr_dir = Path(raw) / 'hrrr_data' / str(year) / month_str
    if hrrr_dir.exists():
        hrrr_files = list(hrrr_dir.glob("*.nc"))
        status = "ok" if len(hrrr_files) >= num_days else "LOW"
        print(f"  NetCDF files: {len(hrrr_files)} [{status}]")
        if len(hrrr_files) < num_days:
            all_ok = False
    else:
        print(f"  MISSING")
        all_ok = False

    # 2. Weather Stations
    print("\n--- Weather Station Data ---")
    stations_file = Path(raw) / 'weather_stations' / 'stations.csv'
    ws_dir = Path(raw) / 'weather_stations' / str(year) / month_str
    if stations_file.exists():
        stations = pd.read_csv(stations_file)
        print(f"  Station list: {len(stations)} stations")
    else:
        print(f"  Station list: MISSING")
        all_ok = False

    if ws_dir.exists():
        ws_files = list(ws_dir.glob("*.csv"))
        status = "ok" if len(ws_files) >= 50 else "LOW"
        print(f"  Data files: {len(ws_files)} station files [{status}]")
        if len(ws_files) < 50:
            all_ok = False

        # Spot check one file
        if ws_files:
            df = pd.read_csv(ws_files[0])
            print(f"  Sample file ({ws_files[0].name}): {len(df)} rows")
    else:
        print(f"  Data directory: MISSING")
        all_ok = False

    # 3. ERCOT DAM SPP
    print("\n--- ERCOT Day-Ahead SPP ---")
    dam_dir = Path(raw) / 'ercot' / 'dam_spp' / str(year) / month_str
    if dam_dir.exists():
        dam_files = sorted(dam_dir.glob("*.csv"))
        status = "ok" if len(dam_files) >= num_days else f"{len(dam_files)}/{num_days}"
        print(f"  Files: {len(dam_files)} [{status}]")
        if dam_files:
            df = pd.read_csv(dam_files[0])
            print(f"  Sample ({dam_files[0].name}): {len(df)} records, columns: {list(df.columns)}")
        if len(dam_files) < num_days:
            all_ok = False
    else:
        print(f"  MISSING")
        all_ok = False

    # 4. ERCOT RT SPP
    print("\n--- ERCOT Real-Time SPP ---")
    rt_dir = Path(raw) / 'ercot' / 'rt_spp' / str(year) / month_str
    if rt_dir.exists():
        rt_files = sorted(rt_dir.glob("*.csv"))
        status = "ok" if len(rt_files) >= num_days else f"{len(rt_files)}/{num_days}"
        print(f"  Files: {len(rt_files)} [{status}]")
        if rt_files:
            df = pd.read_csv(rt_files[0])
            print(f"  Sample ({rt_files[0].name}): {len(df)} records")
        if len(rt_files) < num_days:
            all_ok = False
    else:
        print(f"  MISSING")
        all_ok = False

    # Summary
    print("\n" + "=" * 60)
    if all_ok:
        print(f"All data for {month_name} {year} present and looks complete.")
    else:
        print("Some data missing or incomplete — see above.")
    print("=" * 60)


def validate_settlement_point_coverage(year, month):
    """Report settlement point type distribution and coordinate coverage.

    Loads one day of RT SPP data to show the distribution of settlement point
    types, then compares RN nodes against node_coordinates.csv to report
    coordinate coverage.

    Args:
        year: Integer year (e.g. 2025)
        month: Integer month (e.g. 7)
    """
    dirs = setup_directories()
    month_str = f"{month:02d}"
    month_name = calendar.month_name[month]

    print("\n" + "=" * 60)
    print(f"Settlement Point Coverage: {month_name} {year}")
    print("=" * 60)

    # Load one day of RT SPP to get type distribution
    rt_dir = Path(dirs['raw']) / 'ercot' / 'rt_spp' / str(year) / month_str
    if not rt_dir.exists():
        print(f"  RT SPP data not found for {year}-{month_str}")
        return

    rt_files = sorted(rt_dir.glob("rt_spp_*.csv"))
    if not rt_files:
        print(f"  No RT SPP files found in {rt_dir}")
        return

    rt_sample = pd.read_csv(rt_files[0])
    sample_date = rt_files[0].stem.replace('rt_spp_', '')

    print(f"\n--- Settlement Point Type Distribution (sample: {sample_date}) ---")
    type_counts = rt_sample['settlementPointType'].value_counts()
    total_records = len(rt_sample)
    sp_col = 'settlementPoint' if 'settlementPoint' in rt_sample.columns else 'settlementPointName'
    for sp_type in type_counts.index:
        count = type_counts[sp_type]
        n_unique = rt_sample[rt_sample['settlementPointType'] == sp_type][sp_col].nunique()
        print(f"  {sp_type:8s}: {count:6d} records ({100*count/total_records:5.1f}%), "
              f"{n_unique:4d} unique nodes")

    # Load node coordinates and check coverage
    coords_file = Path(dirs['processed']) / 'node_coordinates.csv'
    if not coords_file.exists():
        print(f"\n  node_coordinates.csv not found — run build_node_coordinates() first")
        return

    coords = pd.read_csv(coords_file)
    matched_names = set(coords['settlement_point'])

    rn_names = set(rt_sample[rt_sample['settlementPointType'] == 'RN'][sp_col])
    rn_with_coords = rn_names & matched_names
    rn_without_coords = rn_names - matched_names

    print(f"\n--- RN Coordinate Coverage ---")
    print(f"  RN nodes in RT SPP:           {len(rn_names)}")
    print(f"  RN nodes with coordinates:    {len(rn_with_coords)} "
          f"({100*len(rn_with_coords)/max(len(rn_names),1):.1f}%)")
    print(f"  RN nodes without coordinates: {len(rn_without_coords)}")

    print(f"\n--- Node Coordinate Match Methods ---")
    for method, count in coords['match_method'].value_counts().items():
        print(f"  {method:20s}: {count:4d}")
    print(f"  {'TOTAL':20s}: {len(coords):4d}")

    # Show which non-RN types have names that match node_coordinates
    # (they shouldn't, since coordinates are built from NP4-160 Resource Nodes only)
    print(f"\n--- Non-RN Settlement Points vs node_coordinates.csv ---")
    print(f"  (Node coordinates are built from NP4-160, which only contains")
    print(f"   Resource Nodes. Non-RN types use different naming conventions.)")
    for sp_type in type_counts.index:
        if sp_type == 'RN':
            continue
        type_names = set(rt_sample[rt_sample['settlementPointType'] == sp_type][sp_col])
        overlap = type_names & matched_names
        print(f"  {sp_type:8s}: {len(type_names):4d} unique names, "
              f"{len(overlap):3d} in node_coordinates")


def _draw_texas(ax, proj):
    """Draw the Texas state outline on a cartopy axis."""
    states_shp = shpreader.natural_earth(
        resolution='10m', category='cultural', name='admin_1_states_provinces')
    for record in shpreader.Reader(states_shp).records():
        if record.attributes.get('name') == 'Texas':
            ax.add_geometries(
                [record.geometry], proj,
                facecolor='#f0f0f0', edgecolor='black', linewidth=1.0)
            break
    ax.set_extent([-107.5, -93.0, 25.5, 37.0], crs=proj)


def _draw_ercot_gis_layers(ax, proj, gis_root):
    """Overlay ERCOT line and bus shapefiles on a cartopy axis."""
    line_shp = os.path.join(gis_root, 'Line', 'Line_Output.shp')
    bus_shp = os.path.join(gis_root, 'Bus', 'Bus_Output.shp')

    if os.path.exists(line_shp):
        line_gdf = gpd.read_file(line_shp)
        if line_gdf.crs is not None:
            line_gdf = line_gdf.to_crs('EPSG:4326')
        ax.add_geometries(
            line_gdf.geometry,
            crs=proj,
            facecolor='none',
            edgecolor='#6f6f6f',
            linewidth=0.35,
            alpha=0.45,
            zorder=2,
        )

    if os.path.exists(bus_shp):
        bus_gdf = gpd.read_file(bus_shp)
        if bus_gdf.crs is not None:
            bus_gdf = bus_gdf.to_crs('EPSG:4326')
        bus_gdf = bus_gdf[bus_gdf.geometry.notnull()]
        if len(bus_gdf) > 0:
            bus_gdf.plot(
                ax=ax,
                transform=proj,
                color='#5a5a5a',
                markersize=3,
                alpha=0.45,
                zorder=3,
            )


def validate_node_coordinate_matching():
    """Validate the ERCOT node-to-coordinate matching pipeline.

    Produces:
    1. A map of matched nodes (colored by match method) and unmatched EIA plants
    2. Summary statistics on match rates by method
    """
    dirs = setup_directories()
    processed = dirs['processed']
    raw = dirs['raw']

    # Load data
    matched_file = os.path.join(processed, 'node_coordinates.csv')
    unmatched_ercot_file = os.path.join(processed, 'unmatched_ercot_settlement_points.csv')
    unmatched_eia_file = os.path.join(processed, 'unmatched_eia860_plants.csv')
    eia_file = os.path.join(raw, 'eia860', 'texas_plants.csv')

    for f, label in [(matched_file, 'node_coordinates.csv'),
                     (unmatched_ercot_file, 'unmatched_ercot_settlement_points.csv'),
                     (unmatched_eia_file, 'unmatched_eia860_plants.csv'),
                     (eia_file, 'texas_plants.csv')]:
        if not os.path.exists(f):
            print(f"  MISSING: {label} — run build_node_coordinates() first")
            return

    matched = pd.read_csv(matched_file)
    unmatched_ercot = pd.read_csv(unmatched_ercot_file)
    unmatched_eia = pd.read_csv(unmatched_eia_file)
    eia_all = pd.read_csv(eia_file)

    # Ensure lat/lon are numeric (CSVs may store them as strings)
    for df in [matched, unmatched_eia, eia_all]:
        df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
        df['lon'] = pd.to_numeric(df['lon'], errors='coerce')

    # Also load NP4-160 for total resource node count and unit-per-node info
    np4_dir = os.path.join(raw, 'ercot', 'np4_160')
    rn_files = glob.glob(os.path.join(np4_dir, 'Resource_Node_to_Unit_*.csv'))
    if not rn_files:
        print("  MISSING: NP4-160 data — run pull_np4160 first")
        return
    rn_df = pd.read_csv(rn_files[0])

    # ----------------------------------------------------------------
    # Summary statistics
    # ----------------------------------------------------------------
    total_rn = rn_df['RESOURCE_NODE'].nunique()
    n_matched = len(matched)
    n_unmatched = len(unmatched_ercot)

    print("\n" + "=" * 60)
    print("Node Coordinate Matching Validation")
    print("=" * 60)

    print(f"\n  Total resource nodes (NP4-160): {total_rn}")
    print(f"  Matched:   {n_matched} ({100 * n_matched / total_rn:.1f}%)")
    print(f"  Unmatched: {n_unmatched} ({100 * n_unmatched / total_rn:.1f}%)")

    print("\n  Match rate by method:")
    by_method = matched['match_method'].value_counts()
    for method, count in by_method.items():
        print(f"    {method:20s} {count:4d}  ({100 * count / total_rn:.1f}%)")

    # Units per resource node
    units_per_rn = rn_df.groupby('RESOURCE_NODE').size()
    matched_units = units_per_rn.reindex(matched['settlement_point']).dropna()
    unmatched_units = units_per_rn.reindex(unmatched_ercot['RESOURCE_NODE']).dropna()

    print(f"\n  Units per matched node:   "
          f"mean={matched_units.mean():.1f}, median={matched_units.median():.0f}, "
          f"max={matched_units.max():.0f}")
    print(f"  Units per unmatched node: "
          f"mean={unmatched_units.mean():.1f}, median={unmatched_units.median():.0f}, "
          f"max={unmatched_units.max():.0f}")

    # EIA plant match stats
    n_eia_total = len(eia_all)
    n_eia_matched = n_eia_total - len(unmatched_eia)
    print(f"\n  EIA 860 Texas plants: {n_eia_total}")
    print(f"  EIA plants used in matching: {n_eia_matched}")
    print(f"  EIA plants unmatched: {len(unmatched_eia)}")

    # ----------------------------------------------------------------
    # Map plot
    # ----------------------------------------------------------------
    proj = ccrs.PlateCarree()
    fig, axes = plt.subplots(1, 2, figsize=(20, 8), subplot_kw={'projection': proj})

    # --- Panel 1: Matched nodes by method ---
    ax = axes[0]
    _draw_texas(ax, proj)
    _draw_ercot_gis_layers(ax, proj, os.path.join(dirs['root'], 'Texas_GIS_Data'))

    # Plot all HTML-derived match variants as a single class.
    matched_plot = matched.copy()
    matched_plot['match_method_plot'] = matched_plot['match_method'].where(
        ~matched_plot['match_method'].str.startswith('html_contour', na=False),
        'html_contour'
    )

    method_styles = {
        'html_contour': {'color': '#2196F3', 'marker': 'o', 'label': 'HTML contour', 'size': 46, 'alpha': 0.9},
        'kml':          {'color': '#4CAF50', 'marker': 's', 'label': 'KML (2019)', 'size': 20, 'alpha': 0.8},
        'prefix':       {'color': '#FF9800', 'marker': '^', 'label': 'EIA prefix', 'size': 20, 'alpha': 0.8},
        'contains':     {'color': '#9C27B0', 'marker': 'D', 'label': 'EIA substring', 'size': 20, 'alpha': 0.8},
        'fuzzy':        {'color': '#F44336', 'marker': 'v', 'label': 'EIA fuzzy', 'size': 20, 'alpha': 0.8},
    }

    for method, style in method_styles.items():
        subset = matched_plot[matched_plot['match_method_plot'] == method]
        if len(subset) == 0:
            continue
        ax.scatter(
            subset['lon'], subset['lat'],
            c=style['color'], marker=style['marker'],
            s=style['size'], edgecolors='k', linewidths=0.3, alpha=style['alpha'],
            label=f"{style['label']} ({len(subset)})",
            transform=proj, zorder=5)

    ax.legend(loc='lower left', fontsize=9, framealpha=0.9)
    ax.set_title(f'Matched Resource Nodes ({n_matched}/{total_rn})', fontsize=13)
    ax.gridlines(draw_labels=True, linewidth=0.3, alpha=0.5)

    # --- Panel 2: Unmatched EIA plants ---
    ax = axes[1]
    _draw_texas(ax, proj)
    _draw_ercot_gis_layers(ax, proj, os.path.join(dirs['root'], 'Texas_GIS_Data'))

    # Show matched EIA plants as light background dots
    eia_matched_names = set(matched['plant_name'].dropna())
    eia_matched_pts = eia_all[eia_all['plant_name'].isin(eia_matched_names)]
    if len(eia_matched_pts) > 0:
        ax.scatter(
            eia_matched_pts['lon'], eia_matched_pts['lat'],
            c='#2196F3', marker='o', s=15, alpha=1.0,
            label=f'EIA matched ({len(eia_matched_pts)})',
            transform=proj, zorder=4)

    # Unmatched EIA plants
    ax.scatter(
        unmatched_eia['lon'], unmatched_eia['lat'],
        c='#F44336', marker='x', s=25, alpha=0.6, linewidths=0.8,
        label=f'EIA unmatched ({len(unmatched_eia)})',
        transform=proj, zorder=5)

    ax.legend(loc='lower left', fontsize=9, framealpha=0.9)
    ax.set_title(f'EIA 860 Plants: Matched vs Unmatched', fontsize=13)
    ax.gridlines(draw_labels=True, linewidth=0.3, alpha=0.5)

    fig.suptitle('Node Coordinate Matching Validation', fontsize=15, y=1.01)
    fig.tight_layout()

    output_path = os.path.join(dirs['figures'], 'node_coordinate_matching.png')

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches='tight')
    print(f"\n  Saved plot to {output_path}")

    plt.show()
    return fig, axes


if __name__ == "__main__":
    # validate_data(2025, 1)
    # validate_settlement_point_coverage(2025, 1)
    validate_node_coordinate_matching()
