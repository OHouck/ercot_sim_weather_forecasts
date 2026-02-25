The dataset of the synthetic Texas 123-bus backbone transmission (TX-123BT) system. 
The procedures and details to create TX-123BT system are described in the paper below:
Jin Lu, Xingpeng Li et al., “A Synthetic Texas Backbone Power System with Climate-Dependent Spatio-Temporal Correlated Profiles”.
If you use this dataset in your work, please cite the paper above. 

***Introduction:
The TX-123BT system has similar temporal and spatial characteristics as the actual Electric Reliability Council of Texas (ERCOT) system.
TX-123BT system has a backbone network consisting of only high-voltage transmission lines distributed in the Texas territory.
It includes time series profiles of renewable generation, electrical load, and transmission thermal limits for the entire year of 2019.
The North American Land Data Assimilation System (NLDAS) climate data is extracted and used to create the climate-dependent time series profiles mentioned above.  
Two sets of climate-dependent dynamic line rating (DLR) profiles are created: (i) daily DLR and (ii) hourly DLR. 

***Power system configuration data:
'Bus_data.csv': Bus data including bus name and location (longitude & latitude, weather zone).
'Line_data.csv': Line capacity and terminal bus information.
'Generator_data.xlsx': 'Gen_data' sheet: Generator parameters including active/reactive capacity, fuel type, cost and ramping rate.
			     'Solar Plant Number' sheet: Correspondence between the solar plant number and generator number.
			     'Wind Plant Number' sheet: Correspondence between the wind plant number and generator number.

***Time series profiles:
'Climate_2019' folder: Include each day's climate data for solar radiation, air temperature, wind speed near surface at 10 meter height.
			     Each file in the folder includes the hourly temperature, longwave & shortwave solar radiation, zonal & Meridional  wind speed data of a day in 2019.
'dynamic_rating_2019' folder: Include the hourly dynamic line rating for each day in 2019.
					Each file includes the hourly line rating (MW) of a line for all hours in 2019.
					In each file, columns represent hour 1-24 in a day, rows represent day 1-365 in 2019.    
'Daily_rating_2019.csv': The daily dynamic line rating (MW) for all lines and all days in 2019.
'solar_2019' folder: Solar production for all the solar farms in the TX-123BT and for all the days in 2019.
			   Each file includes the hourly solar production (MW) of all the solar plants for a day in 2019.
			   In each file, columns represent hour 1-24 in a day, rows represent solar plant 1-72. 
'wind_2019' folder: Wind production for all the wind farms in the case and for all the days in 2019.
			  Each file includes the hourly wind production (MW) of all the wind plants for a day in 2019.
			  In each file, columns represent hour 1-24 in a day, rows represent wind plant 1-82. 
'load_2019' folder: Include each day's hourly load data on all the buses.
			  Each file includes the hourly nodal loads (MW) of all the buses in a day in 2019.
 			  In each file,columns represent bus 1-123, rows represent hour 1-24 in a day.

***Python Codes to run security-constrainted unit commitment (SCUC) for TX-123BT profiles
Recommand Python Version: Python 3.11
Required packages: Numpy, pyomo, pypower, pickle
Required a solver which can be called by the pyomo to solve the SCUC optimization problem.

*'Sample_Codes_SCUC' folder: A standard SCUC model.
The load, solar generation, wind generation profiles are provided by 'load_annual','solar_annual', 'wind_annual' folders.
The daily line rating profiles are provided by 'Line_annual_Dmin.txt'.
'power_mod.py': define the python class for the power system.
'UC_function.py': define functions to build, solve, and save results for pyomo SCUC model.
'formpyomo_UC': define the function to create the input file for pyomo model.
'Run_SCUC_annual': run this file to perform SCUC simulation on the selected days of the TX-123BT profiles.
Steps to run SCUC simulation:
1) Set up the python environment. 
2) Set the solver location: 'UC_function.py'=>'solve_UC' function=>UC_solver=SolverFactory('solver_name',executable='solver_location')
3) Set the days you want to run SCUC: 'Run_SCUC_annual.py'=>last row: run_annual_UC(case_inst,start_day,end_day)
For example: to run SCUC simulations for 125th-146th days in 2019, the last row of the file is 'run_annual_UC(case_inst,125,146)'
You can also run a single day's SCUC simulation by using: 'run_annual_UC(case_inst,single_day,single_day)'

* 'Sample_Codes_SCUC_HourlyDLR' folder: The SCUC model consider hourly dynamic line rating (DLR) profiles.
The load, solar generation, wind generation profiles are provided by 'load_annual','solar_annual', 'wind_annual' folders.
The hourly line rating profiles in 2019 are provided by 'dynamic_rating_result' folder.
'power_mod.py': define the python class for the power system.
'UC_function_DLR.py': define functions to build, solve, and save results for pyomo SCUC model (with hourly DLR).
'formpyomo_UC': define the function to create the input file for pyomo model.
'RunUC_annual_dlr': run this file to perform SCUC simulation (with hourly DLR) on the selected days of the TX-123BT profiles.
Steps to run SCUC simulation (with hourly DLR):
1) Set up the python environment. 
2) Set the solver location: 'UC_function_DLR.py'=>'solve_UC' function=>UC_solver=SolverFactory('solver_name',executable='solver_location')
3) Set the daily profiles for SCUC simulation: 'RunUC_annual_dlr.py'=>last row: run_annual_UC_dlr(case_inst,start_day,end_day)
For example: to run SCUC simulations (with hourly DLR) for 125th-146th days in 2019, the last row of the file is 'run_annual_UC_dlr(case_inst,125,146)'
You can also run a single day's SCUC simulation (with hourly DLR) by using: 'run_annual_UC_dlr(case_inst,single_day,single_day)'

The SCUC/SCUC with DLR simulation results are saved in the 'UC_results' folders under the corresponding folder.
Under 'UC_results' folder:
'UCcase_Opcost.txt': total operational cost ($)
'UCcase_pf.txt': the power flow results (MW). Rows represent lines, columns represent hours.
'UCcase_pfpct.txt': the percentage of the power flow to the line capacity (%). Rows represent lines, columns represent hours.
'UCcase_pgt.txt': the generators output power (MW). Rows represent conventional generators, columns represent hours.
'UCcase_lmp.txt': the locational marginal price ($/MWh). Rows represent buses, columns represent hours.


***Geographic information system (GIS) data:
'Texas_GIS_Data' folder: includes the geographic information systems (GIS) data of the TX-123BT system configurations and ERCOT weather zones.
The GIS data can be viewed and edited using GIS software: ArcGIS.
The subfolders are:
'Bus' folder: the shapefile of bus data for the TX-123BT system.
'Line' folder: the shapefile of line data for the TX-123BT system.
'Weather Zone' folder: the shapefile of the weather zones in Electric Reliability Council of Texas (ERCOT).

*** Maps(Pictures) of the TX-123BT & ERCOT Weather Zone
'Maps_TX123BT_WeatherZone' folder:
1) 'TX123BT_Noted.jpg': The maps (pictures) of the TX-123BT transmission network. Buses are in blue and lines are in green.
2) 'Area_Houston_Noted.jpg', 'Area_Dallas_Noted.jpg', 'Area_Austin_SanAntonio_Noted.jpg':The maps for different areas including Houston, Dallas, and Austin-SanAntonio are also provided.
3) 'Weather_Zone.jpg': The map of ERCOT weather zones. It's ploted by author, may be slightly different from the actual ERCOT weather zones. 


***Funding
This project is supported by Alfred P. Sloan Foundation.

***License:
This work is licensed under the terms of the Creative Commons Attribution 4.0 (CC BY 4.0) license.

***Disclaimer:
The author doesn’t make any warranty for the accuracy, completeness, or usefulness of any information disclosed and the author assumes no liability or responsibility for any errors or omissions for the information (data/code/results etc) disclosed.

***Contributions:
Jin Lu created this dataset. Xingpeng Li supervised this work. Hongyi Li and Taher Chegini provided the raw historical climate data (extracted from an open-access dataset - NLDAS).

