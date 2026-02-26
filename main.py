"""main.py — Run all data download, processing, and analysis steps.

Comment/uncomment sections below to control which steps run.
Steps are ordered by dependency — later steps require earlier ones to have
completed at least once (data is cached on disk between runs).

Usage:
    uv run python main.py
"""

import os
from helper_funcs import setup_directories

YEAR = 2025
MONTH = 7

dirs = setup_directories()

# =============================================================================
# STEP 1: Download NDFD weather forecasts from NOAA S3
# Downloads GRIB2 files, extracts Texas bounding box, saves as NetCDF.
# ~30-60 min per element. Skips files that already exist.
# =============================================================================
# from download_data.pull_ndfd import download_and_extract_texas_month
# base_dir = os.path.join(dirs['raw'], 'ndfd_data')
# for element in ['temp', 'wspd', 'wdir']:
#     download_and_extract_texas_month(element, year=YEAR, month=MONTH, base_dir=base_dir)

# =============================================================================
# STEP 2: Download realized weather observations from NOAA ISD
# Pulls hourly temperature and wind data for ~200 Texas weather stations.
# ~1 min. Skips stations already downloaded.
# =============================================================================
# from download_data.pull_weatherstation import download_month as download_weather
# download_weather(YEAR, MONTH)

# =============================================================================
# STEP 3: Download ERCOT market data (DAM SPP + RT SPP)
# Day-ahead and real-time settlement point prices. Requires ERCOT API
# credentials in ~/keys/. ~30 min. Skips days already downloaded.
# =============================================================================
# from download_data.pull_ercot import download_month as download_ercot
# download_ercot(YEAR, MONTH)

# =============================================================================
# STEP 4a: Download NP4-160 settlement point mapping from ERCOT MIS
# Maps resource nodes to unit substations. Public download, no auth.
# =============================================================================
# from download_data.pull_np4160 import download_np4_160
# download_np4_160()

# =============================================================================
# STEP 4b: Download EIA Form 860 plant data
# Gets lat/lon coordinates for all Texas power plants. Public download.
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
# Requires Steps 4a and 4b.
# =============================================================================
from process_ercot import build_node_coordinates
build_node_coordinates(force_rebuild=True)

# =============================================================================
# STEP 5: Validate all downloaded data
# Checks file counts and samples each dataset for completeness.
# =============================================================================
from download_data.validate_data import validate_july_2025
validate_july_2025()

# =============================================================================
# STEP 6: Generate plots
# Creates Texas maps of max temperature, max wind speed, and max LMP.
# Requires Steps 2, 3, 4c.
# =============================================================================
# from create_plots import (
#     plot_max_temperature_map,
#     plot_max_wind_speed_map,
#     plot_combined_map,
# )
# plot_dir = os.path.join(dirs['root'], 'plots')
# plot_max_temperature_map(YEAR, MONTH, output_path=os.path.join(plot_dir, f'max_temp_july_{YEAR}.png'))
# plot_max_wind_speed_map(YEAR, MONTH, output_path=os.path.join(plot_dir, f'max_wind_speed_july_{YEAR}.png'))
# plot_combined_map(YEAR, MONTH, output_path=os.path.join(plot_dir, f'combined_map_july_{YEAR}.png'))
