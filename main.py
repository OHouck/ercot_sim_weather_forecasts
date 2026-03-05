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

# =============================================================================
# STEP 1a: Download NDFD weather forecasts from NOAA S3
# Downloads GRIB2 files, extracts Texas bounding box, saves as NetCDF.
# ~30-60 min per element per month. Skips files that already exist.
# =============================================================================
# from download_data.pull_ndfd import download_12z_forecasts_month
# ndfd_base = os.path.join(dirs['raw'], 'ndfd_data')
# for year, month in MONTHS:
#     for element in ['temp', 'wspd', 'wdir']:
#         download_12z_forecasts_month(element, year, month, ndfd_base)

# =============================================================================
# STEP 1b: Download HRRR weather forecasts from NOAA S3
# Byte-range downloads of TMP/UGRD/VGRD, extracts Texas, saves as NetCDF.
# ~2-3 hours per month (1,488 files × ~6 MB each). Skips existing files.
# =============================================================================
# from download_data.pull_hrrr import download_hrrr_month
# hrrr_base = os.path.join(dirs['raw'], 'hrrr_data')
# for year, month in MONTHS:
#     download_hrrr_month(year, month, hrrr_base)

# =============================================================================
# STEP 2: Download realized weather observations from NOAA ISD
# Pulls hourly temperature and wind data for ~200 Texas weather stations.
# ~1 min per month. Skips stations already downloaded.
# =============================================================================
# from download_data.pull_weatherstation import download_month as download_weather
# for year, month in MONTHS:
#     download_weather(year, month)

# =============================================================================
# STEP 3: Download ERCOT market data (DAM SPP + RT SPP)
# Day-ahead and real-time settlement point prices. Requires ERCOT API
# credentials in ~/keys/. ~30 min per month. Skips days already downloaded.
# =============================================================================
# from download_data.pull_ercot import download_month as download_ercot
# for year, month in MONTHS:
#     download_ercot(year, month)

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
# Saves three CSVs to processed_data/:
#   - node_coordinates.csv              (matched nodes with lat/lon)
#   - unmatched_ercot_settlement_points.csv  (ERCOT nodes with no EIA match)
#   - unmatched_eia860_plants.csv            (EIA plants with no ERCOT match)
# Requires Steps 4a and 4b.  (Not month-specific — run once.)
# =============================================================================
# from process_data.process_ercot import build_node_coordinates
# build_node_coordinates(force_rebuild=True)

# =============================================================================
# STEP 5: Calculate forecast errors at weather station locations
# Interpolates gridded forecasts to station lat/lon (nearest neighbor),
# compares to ISD hourly observations, and saves per-station error CSVs.
# Requires Steps 1 and 2. ~2 min per month per model.
# =============================================================================
from process_data.calculate_forecast_errors import (
    calculate_ndfd_errors_for_month,
    calculate_hrrr_errors_for_month,
)
for year, month in MONTHS:
    # calculate_ndfd_errors_for_month(year, month) # not using atm
    calculate_hrrr_errors_for_month(year, month)

# =============================================================================
# STEP 6: Generate plots
# Creates Texas maps of max temperature, max wind speed, and max LMP.
# Requires Steps 2, 3, 4c.
# =============================================================================
# from create_plots import (
#     plot_max_temperature_map,
#     plot_max_wind_speed_map,
#     plot_combined_map,
#     plot_ercot_map,
# )
# plot_dir = os.path.join(dirs['root'], 'plots')
# for year, month in MONTHS:
#     tag = f"{year}_{month:02d}"
#     plot_max_temperature_map(year, month, output_path=os.path.join(plot_dir, f'max_temp_{tag}.png'))
#     plot_max_wind_speed_map(year, month, output_path=os.path.join(plot_dir, f'max_wind_speed_{tag}.png'))
#     plot_combined_map(year, month, output_path=os.path.join(plot_dir, f'combined_map_{tag}.png'))
# plot_ercot_map()
