import numpy as np
import pandas as pd
# path = "/Users/ohouck/Library/CloudStorage/OneDrive-TheUniversityofChicago/ercot_sim_weather_forecasts/raw_data/ercot/np4_160/Settlement_Points_02122026_130727.csv"
path = "/Users/ohouck/Library/CloudStorage/OneDrive-TheUniversityofChicago/ercot_sim_weather_forecasts/raw_data/ercot/np4_160/Resource_Node_to_Unit_02122026_130727.csv"


df = pd.read_csv(path)


resource_nodes = df["RESOURCE_NODE"].unique()
unit_substations = df["UNIT_SUBSTATION"].unique()
unit_names = df["UNIT_NAME"].unique()



print(f"Total rows: {len(df)}")
print(f"Unique RESOURCE_NODE values: {len(resource_nodes)}")
print(f"Unique UNIT_SUBSTATION values: {len(unit_substations)}")
print(f"Unique UNIT_NAME values: {len(unit_names)}")
