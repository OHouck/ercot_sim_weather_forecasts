import xarray as xr
import numpy as np
path = "/Users/ohouck/Library/CloudStorage/OneDrive-TheUniversityofChicago/ercot_sim_weather_forecasts/raw_data/hrrr_data/wspd/2025/07/hrrr_09z_20250725_f01.nc"

ds = xr.open_dataset(path)
print(ds)

time = np.unique(ds["time"].values)
step = np.unique(ds["step"].values)
valid_time = np.unique(ds["valid_time"].values) 

print("Time:", time)
print("Step:", step)
print("Valid Time:", valid_time)


