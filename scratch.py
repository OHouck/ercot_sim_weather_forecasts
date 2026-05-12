import os
import sys
import glob
import argparse
import traceback
import csv

import xarray as xr
import pandas as pd


path = "/Users/ohouck/Library/CloudStorage/OneDrive-TheUniversityofChicago/ercot_sim_weather_forecasts/processed_data/combined_hourly_gridded_data/pixel_hourly_gfs+hrrr_2025_11.parquet"


df = pd.read_parquet(path)

for col in df.columns:
    print(col)