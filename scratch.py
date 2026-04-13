import numpy as np
import pandas as pd
import xarray as xr
import geopandas as gpd
import os

path="/Users/ohouck/Library/CloudStorage/OneDrive-TheUniversityofChicago/ercot_sim_weather_forecasts/processed_data/combined_hourly_gridded_data/pixel_hourly_gfs+hrrr_2025_04.parquet"

df = pd.read_parquet(path)

print(df.columns)
