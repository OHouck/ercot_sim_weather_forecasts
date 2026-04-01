"""main.py — Run all data download, processing, and analysis steps.

Comment/uncomment sections below to control which steps run.
Steps are ordered by dependency — later steps require earlier ones to have
completed at least once (data is cached on disk between runs).

All per-month steps loop over MONTHS, a list of (year, month) tuples defined
at the top. Each download script skips files that already exist on disk, so
re-running is cheap.

Usage:
    uv run python main.py
"""

import os
from helper_funcs import setup_directories

# ── Time period ─────────────────────────────────────────────────────────────
# List of (year, month) tuples to process. Every per-month step loops over
# this list.  Change this single variable to expand or restrict the period.
MONTHS = [(2025, m) for m in range(1, 13)]

dirs = setup_directories()

# #############################################################################
#                        DATA DOWNLOADS (Steps 1–4)
# #############################################################################
# These steps download raw data from external sources. They are generally
# slow (hours) but idempotent — existing files are skipped.  Uncomment
# individual blocks only when you need to (re-)download specific datasets.

# =============================================================================
# STEP 1a: Download NDFD weather forecasts from NOAA S3
# Downloads GRIB2 files, extracts Texas bounding box, saves as NetCDF.
# ~30-60 min per element per month. Skips files that already exist.
# (Not used in the main HRRR+GFS pipeline — kept for reference.)
# =============================================================================
# from download_data.pull_ndfd import download_12z_forecasts_month
# ndfd_base = os.path.join(dirs['raw'], 'ndfd_data')
# for year, month in MONTHS:
#     for element in ['temp', 'wspd', 'wdir']:
#         download_12z_forecasts_month(element, year, month, ndfd_base)

# =============================================================================
# STEP 1b-i: Download HRRR weather forecasts from NOAA S3
# Byte-range downloads of TMP/UGRD/VGRD, extracts Texas, saves as NetCDF.
# ~2-3 hours per month (1,488 files × ~6 MB each). Skips existing files.
# =============================================================================
# from download_data.pull_hrrr import download_hrrr_month
# hrrr_base = os.path.join(dirs['raw'], 'hrrr_data')
# for year, month in MONTHS:
#     download_hrrr_month(year, month, hrrr_base)

# =============================================================================
# STEP 1b-ii: Download GFS weather forecasts from NOAA S3
# Day-ahead forecasts (12z cycle, f018–f041). ~1-2 hours per month.
# =============================================================================
# from download_data.pull_gfs import download_gfs_month
# gfs_base = os.path.join(dirs['raw'], 'gfs_data')
# for year, month in MONTHS:
#     download_gfs_month(year, month, gfs_base)

# =============================================================================
# STEP 1c: Download ERA5-Land hourly reanalysis for Texas
# Downloads 2m_temperature and 10m_u/v_wind from the Copernicus CDS API.
# Derives wind speed and direction and saves as compressed NetCDF.
# Requires ~/.cdsapirc with a valid API key.
# ~10-30 min per month. Skips files that already exist.
# =============================================================================
# from download_data.pull_era5 import download_era5_month
# for year, month in MONTHS:
#     download_era5_month(year, month, base_dir=dirs['raw'])

# =============================================================================
# STEP 2: Download realized weather observations from NOAA ISD
# Pulls hourly temperature and wind data for ~200 Texas weather stations.
# ~1 min per month. Skips stations already downloaded.
# =============================================================================
# from download_data.pull_weatherstation import download_month as download_weather
# for year, month in MONTHS:
#     print(f"\n=== Downloading ISD weather station data for {year}-{month:02d} ===")
#     download_weather(year, month)

# =============================================================================
# STEP 3: Download ERCOT market data (DAM SPP + RT SPP + load + forecasts)
# Day-ahead and real-time settlement point prices. Requires ERCOT API
# credentials in ~/keys/. ~30 min per month. Skips days already downloaded.
# =============================================================================
# from download_data.pull_ercot import download_month as download_ercot
# for year, month in MONTHS:
#     download_ercot(year, month)

# =============================================================================
# STEP 3b: Download SCED shadow prices from ERCOT API
# Transmission congestion data (NP6-86-CD). ~2-4 hours for all 12 months.
# =============================================================================
# from download_data.pull_sced_shadow import download_shadow_year
# download_shadow_year(2025)

# =============================================================================
# STEP 4a: Download NP4-160 settlement point mapping from ERCOT MIS
# Maps resource nodes to unit substations. Public download, no auth.
# (Not month-specific — run once.)
# =============================================================================
# from download_data.pull_np4160 import download_np4_160
# download_np4_160()

# =============================================================================
# STEP 4b: Download EIA Form 860 plant data
# Gets lat/lon coordinates for all Texas power plants. Public download.
# (Not month-specific — run once.)
# =============================================================================
# from download_data.pull_eia860 import download_eia860_plants
# download_eia860_plants()

# =============================================================================
# STEP 4c: Build node coordinate mapping (NP4-160 x EIA 860)
# Matches ERCOT settlement point names to EIA plant names to get lat/lon.
# Saves to processed_data/node_coordinates.csv.
# Requires Steps 4a and 4b.  (Not month-specific — run once.)
# =============================================================================
# from process_data.process_ercot import build_node_coordinates
# build_node_coordinates(force_rebuild=True)


# #############################################################################
#                     DATA PROCESSING (Steps 5–7)
# #############################################################################

# =============================================================================
# STEP 5a: Calculate forecast errors at weather station locations
# Interpolates gridded forecasts to station lat/lon (nearest neighbor),
# compares to ISD hourly observations, and saves per-station error CSVs.
# Requires Steps 1 and 2. ~2 min per month per model.
# =============================================================================
# from process_data.calculate_forecast_errors import calculate_station_errors_for_month
# for year, month in MONTHS:
#     calculate_station_errors_for_month(year, month, model='hrrr')
#     calculate_station_errors_for_month(year, month, model='gfs')

# =============================================================================
# STEP 5b: Calculate ERA5-based gridded forecast errors
# Uses ERA5-Land reanalysis as ground truth instead of ISD weather stations.
# Dense spatial coverage (~14,000 ERA5 cells). PRIMARY error source.
# Requires Step 1b (HRRR/GFS) AND Step 1c (ERA5-Land).
# ~10-20 min per month per model.
# =============================================================================
# from process_data.calculate_forecast_errors import calculate_era5_errors_for_month
# for year, month in MONTHS:
#     calculate_era5_errors_for_month(year, month, model='hrrr')
#     calculate_era5_errors_for_month(year, month, model='gfs')

# =============================================================================
# STEP 5c: Build gridded generation & infrastructure map
# Maps EIA 860 generation capacity, transmission lines, load buses onto
# the ERA5 0.1 grid. One-time static map. Requires Steps 4b, 4c.
# =============================================================================
# from process_data.gridded_generation_mapping import build_gridded_generation_map
# build_gridded_generation_map(force_rebuild=False)

# =============================================================================
# STEP 5d: Build pixel x hour analysis dataset
# Merges ERA5 forecast errors (HRRR + GFS), generation map, system LMP,
# congestion metrics, and curtailment into one parquet per month.
# Requires Steps 5b, 5c, 3, 3b.
# =============================================================================
# from process_data.combine_forecast_generation_node import build_pixel_hourly_dataset
# for year, month in MONTHS:
#     build_pixel_hourly_dataset(year, month, force_rebuild=True)

# =============================================================================
# STEP 5e: Build node x hour dataset
# Links each ERCOT resource node's LMP to weather forecast errors.
# Requires Steps 4c, 5b, 3.
# =============================================================================
# from process_data.prepare_node_level_data import prepare_node_level_data
# prepare_node_level_data(
#     months=MONTHS,
#     error_source='era5',
#     force_rebuild=True,
# )

# =============================================================================
# STEP 6: Build cluster x hour dataset
# Clusters ERCOT nodes geographically + by LMP, aggregates to cluster level.
# Requires Step 5e.
# =============================================================================
from process_data.prepare_cluster_level_data import build_cluster_hourly_data
build_cluster_hourly_data(
    months=MONTHS,
    n_clusters=7,
    geo_weight=2.0,
    n_neighbors=8,
    force_rebuild=True,
)

# =============================================================================
# STEP 7a: Process congestion metrics from shadow prices
# Converts raw SCED shadow price CSVs into hourly system-level congestion.
# Requires Step 3b. (Also called automatically by Step 5d if shadow data exists.)
# =============================================================================
from process_data.process_congestion import compute_hourly_congestion_metrics
for year, month in MONTHS:
    compute_hourly_congestion_metrics(year, month, force_rebuild=True)

# =============================================================================
# STEP 7b: Process curtailment metrics from 60-day SCED disclosure
# Extracts wind/solar curtailment (HSL - output) from nested ZIP archives.
# Also geolocates resources to ERA5 pixels using node_coordinates + EIA 860.
# Requires SCED disclosure ZIPs to be downloaded (manual from ERCOT MIS).
# =============================================================================
from process_data.process_curtailment import (
    compute_hourly_curtailment,
    compute_hourly_curtailment_by_pixel,
)
for year, month in MONTHS:
    compute_hourly_curtailment(year, month, force_rebuild=True)
    compute_hourly_curtailment_by_pixel(year, month, force_rebuild=True)


# #############################################################################
#                        ANALYSIS PIPELINE (Steps A1–A10)
# #############################################################################
# Each step saves figures to {OneDrive}/figures/ and tables to {repo}/tables/.

# ── Analysis configuration ────────────────────────────────────────────────────
ANALYSIS_MONTHS = [(2025, m) for m in range(1, 13)]
N_CLUSTERS = 7
GEO_WEIGHT = 2.0
N_NEIGHBORS = 8

# # ── Step A1: Cluster heterogeneity regressions ─────────────────────────────
# # Per-cluster joint HRRR 1h + GFS day-ahead regression
# from analysis.cluster_heterogeneity_lr import run_cluster_analysis
# cluster_outputs = run_cluster_analysis(
#     months=ANALYSIS_MONTHS,
#     n_clusters=N_CLUSTERS,
#     geo_weight=GEO_WEIGHT,
#     n_neighbors=N_NEIGHBORS,
# )

# # ── Step A2: Raw correlation heatmaps (2x2) ────────────────────────────────
# # Per-pixel Pearson r between forecast error and system LMP spread
# from analysis.forecast_error_lmp_corr_heatmap import run_correlation_heatmaps
# corr_outputs = run_correlation_heatmaps(
#     months=ANALYSIS_MONTHS,
#     lmp_var="system_lmp_std",
# )

# ── Step A3: Pixel-level regression coefficient maps (2x2) ────────────────
# Per-pixel OLS with controls and absorbed FE
from analysis.pixel_regression_maps import run_pixel_regression_maps
pixel_outputs = run_pixel_regression_maps(months=ANALYSIS_MONTHS)

# # ── Step A4: Infrastructure-level regressions ──────────────────────────────
# # Capacity-weighted aggregation by tech category
# from analysis.gridded_infrastructure_lr import run_infrastructure_analysis
# infra_outputs = run_infrastructure_analysis(months=ANALYSIS_MONTHS)

# ── Step A5: Extreme weather regime regressions (shadow cost DV) ───────────
# Per-pixel regressions conditioned on extreme weather regimes
from analysis.extreme_weather_regressions import run_regime_regressions
regime_outputs = run_regime_regressions(
    months=ANALYSIS_MONTHS,
    depvar="first_interval_shadow_cost",
)

# ── Step A6: Forecast value maps (shadow cost DV) ──────────────────────────
# Dollar value of forecast improvement at each pixel
from analysis.forecast_value_map import run_forecast_value_analysis
value_outputs = run_forecast_value_analysis(
    months=ANALYSIS_MONTHS,
    depvar="first_interval_shadow_cost",
)

# ── Step A7: Forecast error asymmetry analysis (shadow cost DV) ────────────
# Over-forecast vs under-forecast effects on congestion
from analysis.extreme_weather_regressions import run_asymmetry_regressions
asymmetry_outputs = run_asymmetry_regressions(
    months=ANALYSIS_MONTHS,
    depvar="first_interval_shadow_cost",
)

# ── Step A8: Curtailment-focused regime regressions ────────────────────────
# How forecast errors affect renewable curtailment during extreme weather
curtailment_regime_outputs = run_regime_regressions(
    months=ANALYSIS_MONTHS,
    depvar="wind_curtailment_mw",
)

# ── Step A9: Forecast value maps for curtailment ──────────────────────────
curtailment_value_outputs = run_forecast_value_analysis(
    months=ANALYSIS_MONTHS,
    depvar="wind_curtailment_mw",
)

