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

path = "/Users/ohouck/Library/CloudStorage/OneDrive-TheUniversityofChicago/ercot_sim_weather_forecasts/processed_data/dam_markups_2025.parquet"

ds = pd.read_parquet(path)

print(ds.head())