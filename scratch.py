import os
import sys
import glob
import argparse
import traceback
import csv

import xarray as xr
import pandas as pd



# path = "/Users/ohouck/Library/CloudStorage/OneDrive-TheUniversityofChicago/ercot_sim_weather_forecasts/raw_data/cems/2025/08/cems_tx_202508.parquet"
# path = "/Users/ohouck/Library/CloudStorage/OneDrive-TheUniversityofChicago/ercot_sim_weather_forecasts/raw_data/cems/2025/12/cems_tx_202512.parquet"
path = "/Users/ohouck/Library/CloudStorage/OneDrive-TheUniversityofChicago/ercot_sim_weather_forecasts/raw_data/ercot/dam_disclosure/2025/05/dam_gen_resource_202505.parquet"
df = pd.read_parquet(path)

# save to csv
out_path = "~/Downloads/dam_gen_resource_202505.csv"
df.to_csv(out_path, index=False)
print(f"Saved to {out_path}")
