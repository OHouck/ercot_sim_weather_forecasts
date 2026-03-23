import numpy as np
import pandas as pd
import xarray as xr
import geopandas as gpd
import os

path = "/Users/ohouck/Library/CloudStorage/OneDrive-TheUniversityofChicago/ercot_sim_weather_forecasts/raw_data/gfs_data/wdir/2025/01/gfs_12z_20250114_f024.nc"

ds = xr.open_dataset(path)
print(ds.time.values)
print(ds.valid_time.values)
