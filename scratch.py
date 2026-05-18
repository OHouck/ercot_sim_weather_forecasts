import os
import sys
import glob
import argparse
import traceback
import csv

import xarray as xr
import pandas as pd


THERMAL_RESOURCE_TYPES = {
    "CCGT90", "CCLE90", "CCGT00", "CCLE00",
    "SCGT90", "SCLE90",
    "GSREH", "GSNONR", "GSSUP",
    "CLLIG", "STEAM", "COAL", "NUC",
}

# path = "/Users/ohouck/Library/CloudStorage/OneDrive-TheUniversityofChicago/ercot_sim_weather_forecasts/processed_data/forecast_errors_era5/gfs/2025/02/era5_errors_202502.nc"
path = "/Users/ohouck/Library/CloudStorage/OneDrive-TheUniversityofChicago/ercot_sim_weather_forecasts/processed_data/combined_hourly_gridded_data/pixel_hourly_gfs+hrrr_2025_04.parquet"

ds = pd.read_parquet(path)


for cols in ds.columns:
    print(cols)