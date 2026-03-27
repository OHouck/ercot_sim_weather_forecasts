# CLAUDE.md — Data Build Guide

## Research Question
How do joint errors in short-range (HRRR 1h) and day-ahead (GFS) wind and temperature forecasts impact locational marginal prices (LMP) and renewable curtailment in ERCOT?

## Scope
**Full year 2025.** Pipeline is built and validated for all 12 months.

## Multi-Model Pipeline
The pipeline combines two forecast models side-by-side in every analysis dataset:
- **HRRR** (High-Resolution Rapid Refresh): 3 km regional model, leads 1h and 18h
- **GFS** (Global Forecast System): 0.25° global model, lead f018–f041 from the 12z cycle, collapsed to `lead_hours=0` ("day-ahead") in error files

Because HRRR produces columns suffixed `_1h` and `_18h` and GFS produces `_0h`, names never collide. The default `models` dict is `{'hrrr': (1,), 'gfs': (0,)}` and the combined cache key is `gfs+hrrr`.

---

## Directory Structure
All raw data is stored on OneDrive via `helper_funcs.setup_directories()`:
```
root = /Users/ohouck/Library/CloudStorage/OneDrive-TheUniversityofChicago/ercot_sim_weather_forecasts
```

Layout:
```
{root}/
├── raw_data/
│   ├── ndfd_data/              # Step 1a: NDFD weather forecasts (unused in main pipeline)
│   │   ├── temp/2025/07/       # ~248 NetCDF files per month
│   │   ├── wspd/2025/07/
│   │   └── wdir/2025/07/
│   ├── hrrr_data/              # Step 1b: HRRR weather forecasts (3 km, CONUS)
│   │   ├── temp/2025/{mm}/     # 1,488 NetCDF files/month (24 cycles × 2 leads × ~31 days)
│   │   │   └── hrrr_{HH}z_{YYYYMMDD}_f{01,18}.nc
│   │   ├── wspd/2025/{mm}/     # same structure
│   │   └── wdir/2025/{mm}/     # same structure
│   ├── gfs_data/               # Step 1b: GFS day-ahead forecasts (0.25°, global)
│   │   ├── temp/2025/{mm}/     # 744 NetCDF files/month (1 per valid hour, from 12z cycle)
│   │   │   └── gfs_12z_{YYYYMMDD}_f{018..041}.nc
│   │   ├── wspd/2025/{mm}/     # same structure
│   │   └── wdir/2025/{mm}/     # same structure
│   ├── era5_land/              # Step 1c: ERA5-Land reanalysis (0.1°, hourly)
│   │   └── 2025/{mm}/          # era5_land_{YYYYMM}.nc (one file per month)
│   ├── weather_stations/       # Step 2: ISD realized hourly observations
│   │   ├── stations.csv        # 205 Texas station metadata
│   │   └── 2025/{mm}/          # ~202 per-station hourly CSVs
│   └── ercot/                  # Step 3: ERCOT market data
│       ├── dam_spp/2025/{mm}/  # 31 daily CSVs — day-ahead settlement point prices
│       ├── rt_spp/2025/{mm}/   # 31 daily CSVs — real-time settlement point prices
│       ├── actual_load/2025/{mm}/    # hourly actual load by weather zone
│       ├── demand_forecast/2025/{mm}/ # hourly demand forecasts (1h + 18h) by weather zone
│       ├── sced_shadow/2025/{mm}/    # Step 7a: SCED shadow prices (31 daily CSVs)
│       │   └── shadow_{YYYYMMDD}.csv
│       └── np4_160/            # Step 4a: Settlement point mapping (5 CSVs)
│           ├── Resource_Node_to_Unit_*.csv
│           ├── Settlement_Points_*.csv
│           └── ...
├── data/                       # Static GIS / reference data (checked into git)
│   ├── {rtmLmp,rtmSpp,damSpp2,damSpp7}_html_source.txt  # Step 4c: ERCOT HTML contour maps
│   ├── rtmLmpPoints.kml        # Step 4c: 2019 ERCOT KML snapshot
│   ├── Line_Output.shp (+ .dbf/.prj/.shx)  # Step 5c: Transmission line GIS
│   └── Bus_Output.shp (+ .dbf/.prj/.shx)   # Step 5c: ERCOT bus GIS
├── processed_data/
│   ├── node_coordinates.csv            # Step 4c: 544 matched nodes with lat/lon
│   ├── unmatched_ercot_settlement_points.csv
│   ├── unmatched_eia860_plants.csv
│   ├── forecast_errors/{model}/2025/{mm}/     # Step 5a: Per-station error CSVs
│   │   ├── {station_id}.csv
│   │   └── error_summary.csv
│   ├── forecast_errors_era5/{model}/2025/{mm}/ # Step 5b: ERA5 gridded errors
│   │   ├── era5_errors_{YYYYMM}.nc
│   │   └── error_summary.csv
│   ├── gridded_generation_map.nc              # Step 5c: Static generation/infra map
│   ├── congestion_metrics/                    # Step 7a: Hourly congestion from shadow prices
│   │   └── congestion_hourly_{YYYYMM}.csv
│   ├── load_errors_by_weather_zone/           # Step 5e dependency
│   │   └── load_errors_wz_{tag}.csv
│   ├── combined_hourly_gridded_data/          # Step 5d: Pixel × hour dataset
│   │   └── pixel_hourly_gfs+hrrr_{year}_{mm}.parquet  # ~978k rows, 52 cols
│   ├── node_hourly_gfs+hrrr[_{error_source}]_{tag}.csv  # Step 5e: Node × hour
│   ├── node_clusters_{models_key}_k{k}_{tag}.csv         # Step 6: cluster labels
│   ├── cluster_polygons_{models_key}_k{k}_{tag}.gpkg     # Step 6: cluster geometries
│   └── cluster_hourly_{models_key}_k{k}_{tag}.csv        # Step 6: Cluster × hour
└── figures/                    # Generated visualizations
```

---

## Data Sources Summary

| Dataset | Source | Auth | Script |
|---------|--------|------|--------|
| NDFD forecasts | NOAA S3 `s3://noaa-ndfd-pds/wmo/` | No | `download_data/pull_ndfd.py` |
| HRRR forecasts | AWS S3 `noaa-hrrr-bdp-pds` | No | `download_data/pull_hrrr.py` |
| GFS forecasts | AWS S3 `noaa-gfs-bdp-pds` | No | `download_data/pull_gfs.py` |
| ERA5-Land reanalysis | Copernicus CDS API | CDS API key | `download_data/pull_era5.py` |
| Realized weather | NCEI ISD API | No | `download_data/pull_weatherstation.py` |
| Day-ahead SPP | ERCOT API (NP4-190) | OAuth2 + subscription key | `download_data/pull_ercot.py` |
| Real-time SPP | ERCOT API (NP6-905) | OAuth2 + subscription key | `download_data/pull_ercot.py` |
| Actual load | ERCOT API (NP6-905) | OAuth2 + subscription key | `download_data/pull_ercot.py` |
| Demand forecasts | ERCOT API | OAuth2 + subscription key | `download_data/pull_ercot.py` |
| NP4-160 SP mapping | ERCOT MIS public download | No | `download_data/pull_np4160.py` |
| EIA Form 860 plants | EIA website | No | `download_data/pull_eia860.py` |
| Node coords HTML | ERCOT contour map HTML (4 pages) | No | `data/*_html_source.txt` |
| SCED shadow prices | ERCOT API (NP6-86-CD) | OAuth2 + subscription key | `download_data/pull_sced_shadow.py` |
| Node coords KML | GitHub (cached 2019 ERCOT snapshot) | No | `data/rtmLmpPoints.kml` |
| Transmission GIS | `data/Line_Output.shp` | No | `process_data/gridded_generation_mapping.py` |
| Bus GIS | `data/Bus_Output.shp` | No | `process_data/gridded_generation_mapping.py` |

## Credentials
- `~/keys/ercot_api_key.txt` — ERCOT API subscription key (32 chars)
- `~/keys/ercot_api_secondary_key.txt` — backup subscription key
- `~/keys/ercot_user.txt` — ERCOT account username
- `~/keys/ercot_pwd.txt` — ERCOT account password
- `~/.cdsapirc` — Copernicus CDS API key for ERA5-Land downloads (new-style endpoint: `url: https://cds.climate.copernicus.eu/api`)

---

## Step 0: Project Setup (DONE)

Changes made:
- `helper_funcs.py`: Added `raw`, `processed`, and `figures` keys to `setup_directories()`
- `pyproject.toml`: Added `xarray`, `cfgrib`, `netcdf4`, `geopandas`, `cartopy`, `openpyxl`, `cdsapi`, `pyfixest`, `great-tables` dependencies
- Run `uv sync` to install

Prerequisites: `brew install awscli eccodes`

---

## Step 1: NDFD Weather Forecasts (DONE — not used in main pipeline)

**Script**: `download_data/pull_ndfd.py`

Downloads NDFD 2.5km CONUS forecast GRIB2 files from NOAA S3, extracts Texas bounding box (lat 25.8-36.5, lon -106.6 to -93.5), saves as compressed NetCDF. Keeps only 1h and 25h lead times from Group B issuances. NDFD is no longer used in the main HRRR+GFS analysis pipeline but the data and script remain for reference.

### Output files
- `{raw}/ndfd_data/{element}/2025/{mm}/ndfd_{element}_{YYYYMMDD}_{HHH}h.nc`
- ~248 files per element per month; each file has dims `(step, y, x)` over Texas bounding box

---

## Step 1b: HRRR + GFS Weather Forecasts (DONE)

### HRRR — High-Resolution Rapid Refresh (3 km)

**Script**: `download_data/pull_hrrr.py`

Downloads 2m temperature and 10m wind (U/V) components from the NOAA HRRR archive on AWS S3 (`noaa-hrrr-bdp-pds`). Byte-range downloads fetch only the variables needed (TMP, UGRD, VGRD), then wind speed and direction are derived and saved as NetCDF.

**Lead times downloaded**: f01 (1-hour-ahead) and f18 (18-hour-ahead)
**Cycles**: all 24 UTC cycles per day (00z–23z)
**Coverage**: Texas bounding box (lat 25.8–36.5, lon -106.6 to -93.5), 414 × 447 curvilinear grid

**File format**: `hrrr_{HH}z_{YYYYMMDD}_f{01,18}.nc`
- Dims: `(y=414, x=447)` (curvilinear — lat/lon are 2D coordinate arrays)
- Variables: `t2m` [K], `u10`, `v10` [m/s] (for wspd/wdir elements: `wspd10`, `wdir10`)
- One file per (cycle, lead time, element): 1,488 files/month/element

```bash
uv run python -m download_data.pull_hrrr  # interactive prompts for year/month
```

### GFS — Global Forecast System (0.25°)

**Script**: `download_data/pull_gfs.py`

Downloads 2m temperature and 10m wind from the NOAA GFS archive on AWS S3 (`noaa-gfs-bdp-pds`). Uses the 12z cycle exclusively. Extracts lead hours f018–f041 (corresponding to valid times 06z+1day through 05z+2days, i.e., all 24 hours of the following day). One file per valid hour.

**Lead times downloaded**: f018–f041 from the 12z cycle (24 files per day = one per valid hour)
**Resolution**: 0.25° regular lat/lon grid over Texas (43 lats × 53 lons)
**Coverage**: lat 25.8–36.5, lon -106.6 to -93.5

**File format**: `gfs_12z_{YYYYMMDD}_{f018..f041}.nc`
- Dims: `(latitude=43, longitude=53)` (regular 1D grid)
- Variables: `t2m` [K], `u10`, `v10` [m/s]
- One file per valid hour: 744 files/month/element

```bash
uv run python -m download_data.pull_gfs --year 2025 --month 7
```

**How GFS "day-ahead" is constructed**: When calculating ERA5 errors, all f018–f041 files for a given day are loaded and matched to their `valid_time`. The result is a single `lead_hours=0` layer in the error NetCDF (treating every GFS file as "the day-ahead forecast for that valid hour").

---

## Step 1c: ERA5-Land Reanalysis (DONE)

**Script**: `download_data/pull_era5.py`

Downloads ERA5-Land hourly reanalysis for the Texas bounding box from the Copernicus CDS API. ERA5-Land provides gap-free gridded observations at ~9 km (0.1°) resolution and serves as the primary ground truth for forecast error calculation.

```bash
uv run python -m download_data.pull_era5 --year 2025 --month 7
```

### Key implementation details

**CDS API request** (`download_era5_month()`):
- Dataset: `reanalysis-era5-land`
- Variables requested: `2m_temperature`, `10m_u_component_of_wind`, `10m_v_component_of_wind`
- Area: `[north, west, south, east]` = `[36.5, -106.6, 25.8, -93.5]`
- `data_format: netcdf`, `download_format: unarchived` (new CDS API ≥ 0.7 syntax)
- Requires `~/.cdsapirc` with the new-style URL (`https://cds.climate.copernicus.eu/api`)

**Derived variables** added before saving:
- `wspd = sqrt(u10² + v10²)` — wind speed [m/s]
- `wdir = atan2(-u10, -v10) * 180/π (mod 360)` — meteorological wind direction [degrees]

**Output NetCDF variables**: `t2m` [K], `u10`, `v10`, `wspd`, `wdir`; times stored in UTC; zlib compression (complevel=5).

### Output
- `{raw}/era5_land/{year}/{month:02d}/era5_land_{YYYYMM}.nc`
- ~109 lat × ~132 lon = ~14,388 grid cells; 744 hourly steps per month

---

## Step 2: Weather Station Observations (DONE)

**Script**: `download_data/pull_weatherstation.py`

Downloads hourly realized weather (temperature, wind) from NOAA's Integrated Surface Database (ISD). Used as ground truth for station-level error validation (not for main ERA5-based pipeline).

```bash
uv run python -m download_data.pull_weatherstation
```

### Key implementation details

**Station list parsing** (`download_texas_stations()`):
- Source: `https://www1.ncdc.noaa.gov/pub/data/noaa/isd-history.txt` (fixed-width)
- Data starts at line 22. Column positions (0-indexed):
  - USAF: 0-5, WBAN: 7-11, NAME: 13-41, CTRY: 43-46, STATE: 48-49
  - LAT: 57-63, LON: 65-72, ELEV: 74-80, BEGIN: 82-89, END: 91-98
- Filter: CTRY='US', STATE='TX', END >= target date, lat/lon in TX bounds
- Station ID for API = USAF + WBAN concatenated (11 digits, e.g. `72259003927`)

**NCEI API** (`download_station_data()`):
- Endpoint: `https://www.ncei.noaa.gov/access/services/data/v1`
- No auth required. Rate limit: 5 req/sec
- Params: `dataset=global-hourly`, `stations={11-digit-id}`, `dataTypes=TMP,WND`, `format=csv`, `units=metric`
- Timeout: 120s

### ISD CSV data format
Columns: `STATION, DATE, SOURCE, REPORT_TYPE, CALL_SIGN, QUALITY_CONTROL, TMP, WND`

**TMP field**: `+0333,1` = 33.3°C (value in tenths, quality flag). `+9999` = missing.

**WND field**: `170,1,N,0082,1` = direction 170°, speed 8.2 m/s. `999`/`9999` = missing.

### Results for July 2025
- 205 active TX stations found, 202 returned data (3 had no data)
- ~700-1100 rows per station for 31 days

---

## Step 3: ERCOT Market Data (DONE)

**Script**: `download_data/pull_ercot.py`

Downloads day-ahead hourly LMP, real-time settlement point prices, actual load, and demand forecasts.

```bash
uv run python -m download_data.pull_ercot
```

### Key implementation details

**Authentication** (OAuth2 via Azure B2C ROPC flow):
1. POST to `https://ercotb2c.b2clogin.com/ercotb2c.onmicrosoft.com/B2C_1_PUBAPI-ROPC-FLOW/oauth2/v2.0/token`
2. Body: `grant_type=password`, `username`, `password`, `client_id=fec253ea-0d06-4272-a5e6-b478baeecd70`, `scope=openid {client_id} offline_access`
3. API calls need BOTH `Authorization: Bearer {token}` AND `Ocp-Apim-Subscription-Key: {api_key}` headers.

**API response format**:
- JSON: `_meta`, `report`, `fields`, `data`, `_links`
- `data` is list-of-lists; `fields` provides column names — must zip them together
- Max page size: 100,000 records. Rate limit: 30 req/min.

**Endpoints**:
| Report | Endpoint | Key fields |
|--------|----------|------------|
| DAM SPP | `/np4-190-cd/dam_stlmnt_pnt_prices` | deliveryDate, hourEnding, settlementPoint, settlementPointPrice, settlementPointType |
| RT SPP | `/np6-905-cd/spp_node_zone_hub` | deliveryDate, deliveryInterval, settlementPointName, settlementPointPrice, settlementPointType |
| Actual load | `/np6-345-cd/act_sys_load_by_wzn` | deliveryDate, hourEnding, systemLoad by weather zone |
| Demand forecast | `/np3-560-cd/wsl_alr_1h`, `/np3-560-cd/wsl_alr_2d` | 1h-ahead and day-ahead load forecast by weather zone |

### Results for July 2025
- DAM SPP: 31 daily files; RT SPP: 31 daily files

---

## Step 4: ERCOT Node-to-Coordinate Mapping (DONE)

**Scripts**: `download_data/pull_np4160.py`, `download_data/pull_eia860.py`
**Processing**: `process_data/process_ercot.py` → `build_node_coordinates()`

ERCOT settlement points have names (e.g., `AJAXWIND_RN`) but no geographic coordinates. Three sources are combined in priority order:

**Source 1: ERCOT HTML contour maps** (preferred, current)
- Files: `data/{rtmLmp,rtmSpp,damSpp2,damSpp7}_html_source.txt`
- 295 unique nodes across 4 pages; pixel coords on a 600×600 PNG
- Pixel-to-lat/lon via least-squares affine transform (212 KML ground control points)
- Affine accuracy: mean 0.9 km, max 1.4 km; 227 match current NP4-160 nodes

**Source 2: ERCOT KML contour map** (2019 snapshot, fills gaps)
- File: `data/rtmLmpPoints.kml` — 254 nodes with authoritative lat/lon; 18 additional matches

**Source 3: EIA Form 860 name matching** (remaining nodes)
- Three strategies: prefix (~179), substring (~52), fuzzy (~68)

**Result**: 544/937 resource nodes matched (58%). Cached to `{processed}/node_coordinates.csv`.

### Settlement point types in RT SPP data

Only RN (Resource Node) types are used. Other types (PCCRN, LCCRN, PUN, LZ/HU/SH/AH) use different naming conventions and are excluded.

---

## Step 5: Forecast Error Calculation (DONE)

**Script**: `process_data/calculate_forecast_errors.py`

**Timezone convention**: All output `valid_time` columns are **US/Central (tz-naive)**. July CDT = UTC−5; January CST = UTC−6.

### Step 5a: Station-level errors

~202 Texas weather stations. One CSV per station per model per month.

```bash
uv run python -c "
from process_data.calculate_forecast_errors import calculate_era5_errors_for_month
calculate_era5_errors_for_month(2025, 7, model='hrrr')
calculate_era5_errors_for_month(2025, 7, model='gfs')
"
```

**Output**: `{processed}/forecast_errors/{model}/{year}/{month:02d}/{station_id}.csv`

### Step 5b: ERA5 gridded errors (ERA5-Land as ground truth)

~14,000 ERA5 cells. Dense spatial coverage. **This is the primary error source for analysis.**

```bash
uv run python -c "
from process_data.calculate_forecast_errors import calculate_era5_errors_for_month
calculate_era5_errors_for_month(2025, 7, model='hrrr')  # HRRR leads 1h + 18h
calculate_era5_errors_for_month(2025, 7, model='gfs')   # GFS day-ahead (lead=0)
"
```

**Output**: `{processed}/forecast_errors_era5/{model}/{year}/{month:02d}/era5_errors_{YYYYMM}.nc`

NetCDF dimensions: `(valid_time [Central], lead_hours, latitude=108, longitude=132)`

NetCDF variables:
- `temp_error`, `wspd_error`, `wdir_error` — forecast minus ERA5 (errors)
- `forecast_temp`, `forecast_wspd`, `forecast_wdir` — forecast values
- `era5_temp`, `era5_wspd`, `era5_wdir` — observed ERA5 values

For HRRR: `lead_hours = [1, 18]`. For GFS: `lead_hours = [0]` (single day-ahead lead).

---

## Step 5c: Gridded Generation & Infrastructure Mapping (DONE)

**Script**: `process_data/gridded_generation_mapping.py`

Maps EIA Form 860 generation capacity, ERCOT transmission lines, and ERCOT load buses onto the ERA5 0.1° grid. Required before Step 5d.

```bash
uv run python -c "
from process_data.gridded_generation_mapping import build_gridded_generation_map
build_gridded_generation_map()
"
```

**Output**: `{processed}/gridded_generation_map.nc` — static NetCDF, dims `(latitude, longitude)`
- Variables: `total_capacity_mw`, `n_generators`, `nameplate_mw_tech_{slug}` (~16 technologies), `has_transmission_line`, `load_center`

Technology slugs include: `onshore_wind_turbine`, `solar_photovoltaic`, `natural_gas_fired_combined_cycle`, `natural_gas_fired_combustion_turbine`, `natural_gas_steam_turbine`, `conventional_steam_coal`, `nuclear`, `batteries`, etc.

---

## Step 5d: Pixel × Hour Analysis Dataset (DONE)

**Script**: `process_data/combine_forecast_generation_node.py`

Builds the main analysis-ready dataset by merging ERA5 forecast errors from both models (HRRR + GFS), the generation map, and system-level LMP. One row per (pixel, hour) for all infrastructure pixels.

```bash
# Default: builds combined HRRR 1h + GFS day-ahead dataset
uv run python -c "
from process_data.combine_forecast_generation_node import build_pixel_hourly_dataset
for month in range(1, 13):
    build_pixel_hourly_dataset(2025, month)  # defaults to models={'hrrr':(1,),'gfs':(0,)}
"
```

### Key functions
- `build_pixel_hourly_dataset(year, month, models=None, force_rebuild=False)` — main entry point; `models` defaults to `{'hrrr': (1,), 'gfs': (0,)}`
- `flatten_era5_errors(year, month, model)` — ERA5 errors → wide DataFrame (one row per pixel × hour per lead)
- `flatten_generation_map()` — generation map NetCDF → DataFrame (infrastructure pixels only)
- `compute_system_lmp_hourly(year, month)` — RT SPP (RN nodes only) → system-level hourly `system_lmp_mean`, `system_lmp_max`, `system_lmp_std`

### Output
- `{processed}/combined_hourly_gridded_data/pixel_hourly_gfs+hrrr_{year}_{mm}.parquet`
- ~978,000 rows/month (≈5,000 infrastructure pixels × 744 hours/month), 52 columns

**All columns** (52):

| Column | Type | Description |
|--------|------|-------------|
| `pixel_id` | str | `"{lat:.1f}_{lon:.1f}"` — ERA5 0.1° grid cell identifier |
| `valid_time` | datetime | US/Central tz-naive |
| `latitude`, `longitude` | float | ERA5 grid cell center |
| `temp_error_0h` | float | GFS day-ahead temperature error vs ERA5 [°C] |
| `wspd_error_0h` | float | GFS day-ahead wind speed error vs ERA5 [m/s] |
| `wdir_error_0h` | float | GFS day-ahead wind direction error [degrees] |
| `forecast_temp_0h` | float | GFS day-ahead forecast temperature [°C] |
| `forecast_wspd_0h` | float | GFS day-ahead forecast wind speed [m/s] |
| `forecast_wdir_0h` | float | GFS day-ahead forecast wind direction [degrees] |
| `era5_temp` | float | ERA5 observed temperature [°C] (shared across leads) |
| `era5_wspd` | float | ERA5 observed wind speed [m/s] |
| `era5_wdir` | float | ERA5 observed wind direction [degrees] |
| `temp_error_1h` | float | HRRR 1h temperature error vs ERA5 [°C] |
| `wspd_error_1h` | float | HRRR 1h wind speed error [m/s] |
| `wdir_error_1h` | float | HRRR 1h wind direction error [degrees] |
| `forecast_temp_1h` | float | HRRR 1h forecast temperature [°C] |
| `forecast_wspd_1h` | float | HRRR 1h forecast wind speed [m/s] |
| `forecast_wdir_1h` | float | HRRR 1h forecast wind direction [degrees] |
| `temp_error_18h` | float | HRRR 18h temperature error vs ERA5 [°C] |
| `wspd_error_18h` | float | HRRR 18h wind speed error [m/s] |
| `wdir_error_18h` | float | HRRR 18h wind direction error [degrees] |
| `forecast_temp_18h` | float | HRRR 18h forecast temperature [°C] |
| `forecast_wspd_18h` | float | HRRR 18h forecast wind speed [m/s] |
| `forecast_wdir_18h` | float | HRRR 18h forecast wind direction [degrees] |
| `total_capacity_mw` | float | Total generation capacity in pixel [MW] |
| `n_generators` | int | Number of generating units in pixel |
| `nameplate_mw_tech_onshore_wind_turbine` | float | Wind capacity [MW] |
| `nameplate_mw_tech_solar_photovoltaic` | float | Solar PV capacity [MW] |
| `nameplate_mw_tech_natural_gas_fired_combined_cycle` | float | CCGT capacity [MW] |
| `nameplate_mw_tech_natural_gas_fired_combustion_turbine` | float | CT capacity [MW] |
| `nameplate_mw_tech_natural_gas_steam_turbine` | float | Gas steam turbine [MW] |
| `nameplate_mw_tech_conventional_steam_coal` | float | Coal [MW] |
| `nameplate_mw_tech_nuclear` | float | Nuclear [MW] |
| `nameplate_mw_tech_batteries` | float | Battery storage [MW] |
| *(+ ~8 more tech columns)* | float | Petroleum, biomass, landfill gas, etc. |
| `has_transmission_line` | int | 1 if pixel intersects a 345 kV transmission line |
| `load_center` | int | 1 if pixel contains an ERCOT load bus |
| `system_lmp_mean` | float | System-wide mean LMP [$/MWh] — same for all pixels in the hour |
| `system_lmp_max` | float | System-wide max LMP across RN nodes [$/MWh] |
| `system_lmp_std` | float | System-wide std dev of LMP across RN nodes [$/MWh] |
| `hour_of_day` | int | 0–23 [Central] |
| `day_of_month` | int | 1–31 |
| `weekday` | int | 0=Monday … 6=Sunday |
| `month` | int | 1–12 |

---

## Step 5e: Node × Hour Dataset (DONE)

**Script**: `process_data/prepare_node_level_data.py`

Builds a node × hour dataset linking each ERCOT resource node's LMP to weather forecast errors from both models.

```bash
uv run python -c "
from process_data.prepare_node_level_data import prepare_node_level_data
prepare_node_level_data(
    months=[(2025, m) for m in range(1, 13)],
    error_source='era5',   # default; 'station' also available
)
"
```

### Key implementation details

**Error sources** (`error_source` parameter):
- `'era5'` (recommended): each node → nearest ERA5 cell via `xr.sel(method='nearest')`
- `'station'`: each node → nearest ISD station via `gpd.sjoin_nearest`

**Multi-model loading**: loops over `models` dict (`{'hrrr': (1,), 'gfs': (0,)}` by default), loads errors for each, merges on `(settlement_point, hour)`. GFS `_0h` columns and HRRR `_1h` / `_18h` columns are all present in the output.

**Backward compatibility**: accepts `model='hrrr'` (old string API) which is auto-converted to `models={'hrrr': (1,)}`.

**Load data**: ERCOT weather-zone actual load + 1h-ahead and 18h-ahead demand forecasts are merged by spatial join of node coordinates to weather zone polygons.

### Output
- `{processed}/node_hourly_gfs+hrrr[_{error_source}]_{tag}.csv`
- One row per (settlement_point, hour)

**Columns**:

| Column | Description |
|--------|-------------|
| `settlement_point` | ERCOT resource node name (e.g. `AJAXWIND_RN`) |
| `hour` | valid_time as datetime string [US/Central] |
| `lmp` | Real-time LMP [$/MWh] (single interval) |
| `lmp_mean`, `lmp_max`, `lmp_std` | LMP stats across 15-min intervals in the hour |
| `lat`, `lon` | Node coordinates |
| `weather_zone` | ERCOT weather zone (coast/east/far_west/north/north_central/south/south_central/west) |
| `temp_error_0h` | GFS day-ahead temperature error vs ERA5 [°C] |
| `wspd_error_0h` | GFS day-ahead wind speed error [m/s] |
| `wdir_degree_error_0h` | GFS day-ahead wind direction error [degrees] |
| `forecast_temp_0h`, `forecast_wspd_0h` | GFS forecast values |
| `temp_error_1h` | HRRR 1h temperature error vs ERA5 [°C] |
| `wspd_error_1h` | HRRR 1h wind speed error [m/s] |
| `wdir_degree_error_1h` | HRRR 1h wind direction error [degrees] |
| `forecast_temp_1h`, `forecast_wspd_1h` | HRRR 1h forecast values |
| `observed_temp` | ERA5 (or ISD) observed temperature [°C] |
| `observed_wspd` | ERA5 (or ISD) observed wind speed [m/s] |
| `observed_wdir` | ERA5 (or ISD) observed wind direction [degrees] |
| `actual_load` | ERCOT actual system load in weather zone [MW] |
| `forecast_load_1h` | 1h-ahead demand forecast [MW] |
| `forecast_load_18h` | 18h-ahead demand forecast [MW] |
| `load_error_1h` | actual_load − forecast_load_1h [MW] |
| `load_error_18h` | actual_load − forecast_load_18h [MW] |
| `hour_dt` | datetime64 version of `hour` |
| `hour_of_day`, `day_of_month`, `weekday`, `month` | time features |

---

## Step 6: Cluster × Hour Dataset (DONE)

**Script**: `process_data/prepare_cluster_level_data.py`

Clusters ERCOT nodes geographically and by LMP patterns, then aggregates weather and LMP data to a cluster × hour level.

```bash
uv run python -c "
from process_data.prepare_cluster_level_data import build_cluster_hourly_data
cluster_hourly, node_clusters, cluster_polygons, sil = build_cluster_hourly_data(
    months=[(2025, m) for m in range(1, 13)],
    n_clusters=9,
    geo_weight=10.0,
    n_neighbors=8,
)
"
```

### Clustering approach
- Features: standardized `[lat, lon]` (weighted by `geo_weight`) + LMP summary stats
- Algorithm: `AgglomerativeClustering` with k-NN connectivity constraint (geographic contiguity)
- Small clusters (< `min_cluster_size`) reassigned to nearest valid cluster
- Use `sweep_n_clusters()` to choose k via silhouette scores

### Output files
- `{processed}/cluster_hourly_{models_key}_k{k}_{tag}.csv` — one row per (cluster, hour)
- `{processed}/node_clusters_{models_key}_k{k}_{tag}.csv` — cluster label per node
- `{processed}/cluster_polygons_{models_key}_k{k}_{tag}.gpkg` — convex-hull polygons

**Cluster-hourly columns** (key variables):

| Column | Description |
|--------|-------------|
| `cluster` | Cluster ID (0-indexed integer) |
| `hour` | valid_time [US/Central] |
| `lmp_mean`, `lmp_std`, `lmp_max`, `lmp_min` | LMP stats across nodes in cluster [$/MWh] |
| `system_lmp_std` | System-wide LMP std dev (all nodes, all clusters) [$/MWh] |
| `actual_load` | System load [MW] |
| `temp_error_1h`, `temp_error_1h_std`, `max_abs_temp_error_1h` | HRRR 1h temp error stats across cluster |
| `wspd_error_1h`, `wspd_error_1h_std`, `max_abs_wspd_error_1h` | HRRR 1h wind speed error stats |
| `temp_error_0h`, `temp_error_0h_std`, `max_abs_temp_error_0h` | GFS day-ahead temp error stats |
| `wspd_error_0h`, `wspd_error_0h_std`, `max_abs_wspd_error_0h` | GFS day-ahead wind speed error stats |
| `load_error_1h`, `load_error_18h` | Demand forecast errors [MW] |
| `observed_temp`, `observed_wspd` | Cluster mean observed weather (ERA5 or ISD) |
| `nameplate_mw_gas`, `nameplate_mw_wind`, `nameplate_mw_solar`, `nameplate_mw_nuclear`, `nameplate_mw_coal`, `nameplate_mw_other` | Generation capacity by broad tech [MW] |
| `total_nameplate_mw` | Total capacity in cluster [MW] |
| `pct_gas`, `pct_wind`, `pct_solar`, ... | Capacity share by broad tech |
| `nameplate_mw_tech_*` | Capacity per EIA technology slug |
| `n_nodes`, `n_generators` | Node and generator counts |
| `cluster_lat`, `cluster_lon` | Cluster centroid |
| `hour_of_day`, `weekday`, `month` | Time features |

---

## Step 7: Renewable Curtailment Data (TODO)

ERCOT publishes 60-Day SCED Disclosure with individual unit output and HSL. Curtailment = HSL - actual output for renewables.
- Source: https://www.ercot.com/mp/data-products/data-product-details?id=NP3-966-ER

---

## Execution Order

```bash
# Step 0: Setup
uv sync

# Step 1b: HRRR forecasts (~2-3 hours per month)
uv run python -m download_data.pull_hrrr  # interactive prompts for year/month

# Step 1b: GFS day-ahead forecasts (~1-2 hours per month)
uv run python -m download_data.pull_gfs --year 2025 --month 7

# Step 1c: ERA5-Land reanalysis (~10-30 min per month, requires ~/.cdsapirc)
uv run python -m download_data.pull_era5 --year 2025 --month 7

# Step 2: Weather stations (~1 min)
uv run python -m download_data.pull_weatherstation

# Step 3: ERCOT market data (~30 min)
uv run python -m download_data.pull_ercot

# Step 4: Node coordinate mapping (~1 min)
uv run python -m download_data.pull_np4160
uv run python -m download_data.pull_eia860
uv run python -c "from process_data.process_ercot import build_node_coordinates; build_node_coordinates(force_rebuild=True)"

# Step 5b: ERA5 gridded forecast errors (~10-20 min per model per month)
uv run python -c "
from process_data.calculate_forecast_errors import calculate_era5_errors_for_month
for month in range(1, 13):
    calculate_era5_errors_for_month(2025, month, model='hrrr')
    calculate_era5_errors_for_month(2025, month, model='gfs')
"

# Step 5c: Gridded generation/infrastructure map (one-time, ~1 min)
uv run python -c "
from process_data.gridded_generation_mapping import build_gridded_generation_map
build_gridded_generation_map()
"

# Step 5d: Pixel × hour dataset — combined HRRR + GFS (per month, ~5 min)
uv run python -c "
from process_data.combine_forecast_generation_node import build_pixel_hourly_dataset
for month in range(1, 13):
    build_pixel_hourly_dataset(2025, month)  # defaults to {'hrrr':(1,),'gfs':(0,)}
"

# Step 5e: Node × hour dataset
uv run python -c "
from process_data.prepare_node_level_data import prepare_node_level_data
prepare_node_level_data(
    months=[(2025, m) for m in range(1, 13)],
    error_source='era5',
)
"

# Step 6: Cluster × hour dataset
uv run python -c "
from process_data.prepare_cluster_level_data import build_cluster_hourly_data
build_cluster_hourly_data(
    months=[(2025, m) for m in range(1, 13)],
    n_clusters=9,
    geo_weight=10.0,
    n_neighbors=8,
)
"

# Validate
uv run python -m download_data.validate_data

# ── Analysis Pipeline (run after Steps 5d and 6) ──────────────────────────────

# A1: Per-cluster heterogeneity regressions (~5-10 min)
uv run python -m analysis.cluster_heterogeneity_lr
# → figures/cluster_heterogeneity/{cluster_map,coef_plot_combined,hist_grid_1h,hist_grid_dah}.png
# → tables/cluster_regression_results.csv

# A2: Raw correlation heatmaps 2×2 (~5-15 min, streams all months)
uv run python -m analysis.forecast_error_lmp_corr_heatmap
# → figures/correlation_heatmaps/corr_heatmap_2x2.png

# A3: Pixel-level regression maps 2×2 (~10-30 min, ~5k regressions)
uv run python -m analysis.pixel_regression_maps
# → figures/pixel_regressions/pixel_regression_2x2.png
# → tables/pixel_regression_summary.csv

# A4: Infrastructure-level regressions
uv run python -m analysis.gridded_infrastructure_lr
# → figures/infrastructure_regressions/{coef_plot_main,coef_plot_seasonal}.png
# → tables/infrastructure_regression_{main,seasonal}.csv

# Compile unified PDF report (requires typst CLI)
uv run python -m analysis.create_analysis_report
# → output/analysis_report.pdf

# ── Congestion & Extreme Weather Analysis (run after Steps 5d and shadow download) ──

# Step 7a: Download SCED shadow prices (~2-4 hours for all 12 months)
uv run python -m download_data.pull_sced_shadow --year 2025

# Step 7b: Process congestion metrics
uv run python -c "
from process_data.process_congestion import compute_hourly_congestion_metrics
for month in range(1, 13):
    compute_hourly_congestion_metrics(2025, month, force_rebuild=True)
"

# Step 7c: Rebuild pixel datasets with congestion columns
uv run python -c "
from process_data.combine_forecast_generation_node import build_pixel_hourly_dataset
for month in range(1, 13):
    build_pixel_hourly_dataset(2025, month, force_rebuild=True)
"

# A5: Extreme weather regime regressions
uv run python -m analysis.extreme_weather_regressions --depvar total_shadow_cost

# A6: Forecast value maps
uv run python -m analysis.forecast_value_map --depvar total_shadow_cost

# A7: Asymmetry analysis (over- vs under-forecast effects)
uv run python -m analysis.extreme_weather_regressions --asymmetry --depvar total_shadow_cost

# Or run the entire pipeline end-to-end:
uv run python main.py
```

---

## Processing & Visualization Scripts

### `process_data/calculate_forecast_errors.py`
Computes forecast errors (NDFD, HRRR, or GFS) against ISD stations or ERA5-Land reanalysis.

- `_to_central(timestamps)` — UTC → US/Central tz-naive
- `calculate_ndfd_errors_for_month(year, month)` — NDFD vs ISD stations
- `calculate_hrrr_errors_for_month(year, month)` — HRRR vs ISD stations
- `calculate_era5_errors_for_month(year, month, model='hrrr')` — HRRR or GFS vs ERA5
- `build_forecast_grid_gdf(sample_nc_path)` — GeoDataFrame of any 2D lat/lon forecast grid
- `build_era5_grid_gdf(era5_ds)` — GeoDataFrame of ERA5 regular 1D lat/lon grid

### `process_data/gridded_generation_mapping.py`
Maps generation/infrastructure onto the ERA5 grid.

- `build_gridded_generation_map(force_rebuild=False)` — main entry point; caches to `{processed}/gridded_generation_map.nc`
- `build_era5_template_dataset()` — load ERA5 NetCDF to get lat/lon grid template
- `_bin_generators(generators_path, lats, lons)` — bin EIA generators into ERA5 cells
- `_mark_transmission(lats, lons, shp_path)` — mark cells intersecting transmission lines
- `_mark_load_centers(lats, lons, shp_path)` — mark cells containing ERCOT load buses
- `_tech_slug(tech)` — EIA technology name → safe column slug

### `process_data/combine_forecast_generation_node.py`
Builds the pixel × hour analysis dataset (Step 5d).

- `MODEL_LEAD_TIMES = {'hrrr': (1,), 'gfs': (0,)}` — module-level default models dict
- `build_pixel_hourly_dataset(year, month, models=None, force_rebuild=False)` — main entry point; `models` defaults to `MODEL_LEAD_TIMES`
- `flatten_era5_errors(year, month, model)` — ERA5 errors → wide DataFrame (one row per pixel × hour)
- `flatten_generation_map()` — generation map → infrastructure pixels only
- `compute_system_lmp_hourly(year, month)` — RT SPP → system-level hourly stats

### `process_data/prepare_node_level_data.py`
Builds the node × hour dataset (Step 5e).

- `prepare_node_level_data(months, models=None, error_source='era5', force_rebuild=False, model=None)` — main entry point; `models` defaults to `{'hrrr':(1,),'gfs':(0,)}`; `model='hrrr'` (string) is backward-compatible
- `_load_era5_errors_for_nodes(nodes_gdf, year, month, model)` — extract ERA5 errors at node coordinates via `xr.sel(method='nearest')`
- `_map_nodes_to_weather_zones(nodes_gdf, zones_shp_path)` — spatial join
- `_load_weather_zone_load_data(year, month)` — actual load + 1h/18h demand forecasts by weather zone

### `process_data/prepare_cluster_level_data.py`
Clusters nodes and builds the cluster × hour dataset (Step 6).

- `compute_node_lmp_features(months, model)` — per-node LMP summary stats for clustering
- `cluster_nodes(node_features_gdf, n_clusters, geo_weight, n_neighbors, min_cluster_size)` — agglomerative clustering with geographic connectivity
- `sweep_n_clusters(node_features_gdf, k_range, ...)` — silhouette scores vs k
- `build_cluster_polygons(node_clusters_gdf)` — convex-hull polygon per cluster
- `aggregate_to_cluster_hour(df, node_clusters, leads, station_errors=None, cluster_polygons=None)` — aggregate weather/LMP to cluster level; `leads` is a tuple e.g. `(1, 0)` for HRRR+GFS
- `load_station_errors_wide(months, models, dirs)` — load ISD station error CSVs for all models; `models=None` defaults to combined HRRR+GFS
- `build_cluster_hourly_data(months, models=None, n_clusters=..., ...)` — main entry point; `models` defaults to `{'hrrr':(1,),'gfs':(0,)}`; `model='hrrr'` string is backward-compatible
- `_tech_slug(tech)` — technology name → safe column slug

### `process_data/process_ercot.py`
Reads and processes ERCOT market data:

- `load_dam_spp_month(year, month)` — loads all daily DAM SPP CSVs
- `load_rt_spp_month(year, month)` — loads all daily RT SPP CSVs
- `compute_max_lmp_by_node(year, month, point_types='RN')` — max LMP per settlement point
- `build_node_coordinates(force_rebuild=False)` — name-matching pipeline (Step 4c)
- `load_actual_load_month(year, month)` — actual hourly load by weather zone
- `load_demand_forecasts_month(year, month)` — 1h and 18h demand forecasts by weather zone

---

## Analysis Pipeline

All analysis scripts use the combined HRRR+GFS pipeline by default (`LEAD_SHORT=1` for HRRR, `LEAD_DAH=0` for GFS day-ahead). Each script saves figures to `{OneDrive}/figures/` and tables to `{repo}/tables/`. The `create_analysis_report.py` script assembles everything into `output/analysis_report.pdf`.

### `analysis/cluster_heterogeneity_lr.py`
Per-cluster regressions showing heterogeneous treatment effects (Step 6 cluster data).

**Entry point**: `run_cluster_analysis(months, n_clusters, geo_weight, n_neighbors, force_rebuild)`

**Outputs**:
- `figures/cluster_heterogeneity/cluster_map.png`
- `figures/cluster_heterogeneity/coef_plot_combined.png` — 2×3 grid (HRRR 1h / GFS day-ahead × temp / wind / load)
- `figures/cluster_heterogeneity/hist_grid_1h.png` — marginal effect distributions, HRRR 1h
- `figures/cluster_heterogeneity/hist_grid_dah.png` — marginal effect distributions, GFS day-ahead
- `tables/cluster_regression_results.csv` — tidy coefficient table

### `analysis/forecast_error_lmp_corr_heatmap.py`
Spatial heatmap of per-pixel Pearson correlation between forecast errors and system LMP spread. Streams ERA5 error NetCDFs month-by-month for memory efficiency.

**Entry point**: `run_correlation_heatmaps(months, lmp_var, overlay, save_dir)` — produces 2×2 figure

**Outputs**:
- `figures/correlation_heatmaps/corr_heatmap_2x2.png` — 2×2: HRRR/GFS × temp/wind

Also provides `plot_forecast_error_lmp_correlation(error_col, ...)` for single-panel standalone use.

### `analysis/pixel_regression_maps.py` *(new)*
Per-pixel OLS regressions of `system_lmp_std` on all four forecast error variables jointly, with controls (ERA5 observed weather, weekend) and absorbed FE (hour-of-day, month). Maps significant coefficients (p < 0.05) for each error variable.

**Entry point**: `run_pixel_regression_maps(months, save_dir)`

**Regression**: `system_lmp_std ~ temp_error_1h + wspd_error_1h + temp_error_0h + wspd_error_0h + era5_temp + era5_wspd + is_weekend | hour_of_day + month`

**Outputs**:
- `figures/pixel_regressions/pixel_regression_2x2.png` — 2×2: HRRR/GFS × temp/wind (only significant pixels colored)
- `tables/pixel_regression_summary.csv` — `pixel_id, lat, lon, error_var, coef, std_err, pvalue, n_obs`

### `analysis/gridded_infrastructure_lr.py` *(new; replaces `gridded_lr.qmd`)*
Aggregates ERA5 forecast errors by infrastructure type (capacity-weighted per valid hour), then regresses system LMP spread on these category-level errors with cross-category interactions.

**Entry point**: `run_infrastructure_analysis(months, save_dir)`

**Infrastructure categories**: wind, solar, gas, battery, coal, transmission (unweighted), load_center (unweighted)

**Key regression**: `system_lmp_std ~ temp_error_1h_{cat} + wspd_error_1h_{cat} + ... + temp_error_1h_load_center:wspd_error_1h_wind + ... | hour_of_day + month` (clustered SE by date)

**Outputs**:
- `figures/infrastructure_regressions/coef_plot_main.png`
- `figures/infrastructure_regressions/coef_plot_seasonal.png` — summer / winter / shoulder
- `tables/infrastructure_regression_main.csv`
- `tables/infrastructure_regression_seasonal.csv`

### `analysis/create_analysis_report.py` *(new)*
Assembles all figures and tables into a unified Typst PDF report. Reads from the standard output paths; sections with missing files are gracefully skipped.

**Entry point**: `create_analysis_report(output_dir)`

**Output**: `output/analysis_report.pdf`

**Report sections**:
1. Introduction
2. Raw Correlation Heatmaps (Stage 2)
3. Pixel-Level Regression Maps (Stage 3)
4. Infrastructure-Level Results (Stage 4) — coefficient table + seasonal plot
5. Cluster Heterogeneity (Stage 1) — map, coefficient plot, histogram grids
6. GNN Results (placeholder)
7. Appendix — cluster regression table

### `analysis/node_gnn.py`
Graph Neural Network predicting node-level LMP from weather features. Uses transmission graph with virtual super node. `BASE_NUMERIC_COLS` includes both `temp_error_1h`/`wspd_error_1h` (HRRR) and `temp_error_0h`/`wspd_error_0h` (GFS). Not yet integrated into the report pipeline.

### `analysis/extreme_weather_regressions.py` *(new)*
Regime-conditional pixel regressions for extreme weather events. For each regime (extreme cold, extreme heat, high wind, stressed grid), runs per-pixel regressions of congestion measures on forecast errors. Also includes asymmetry analysis (over- vs under-forecast effects).

**Entry point**: `run_regime_regressions(months, depvar)`, `run_asymmetry_regressions(months, depvar, regime_name)`

**Outputs**:
- `tables/extreme_weather_regression_{depvar}_{regime}.csv`
- `tables/asymmetry_{depvar}.csv`

### `analysis/forecast_value_map.py` *(new)*
Computes the dollar value of forecast accuracy at each ERA5 pixel. Combines per-pixel regression coefficients with observed forecast error variance: `value = |β| × σ(error)`.

**Entry point**: `run_forecast_value_analysis(months, depvar)`, `run_regime_value_comparison(months, depvar)`

**Outputs**:
- `figures/forecast_value/forecast_value_by_error_{depvar}.png` — 2×2 map by error type
- `figures/forecast_value/forecast_value_total_{depvar}.png` — single total value map
- `tables/forecast_value_{depvar}.csv`

### `process_data/process_congestion.py` *(new)*
Processes SCED shadow prices into hourly congestion metrics. System-level metrics (n_binding_constraints, total_shadow_cost, max_shadow_price, etc.) are merged into the pixel × hour dataset.

- `compute_hourly_congestion_metrics(year, month)` — hourly system-level congestion
- `merge_congestion_system(pixel_df, year, month)` — merge into pixel data
- `geolocate_constraints(shadow_df)` — map constraints to lat/lon via Bus_Output.shp

### `process_data/classify_weather_regimes.py` *(new)*
Classifies each hour into weather/grid regimes using system-wide percentile thresholds.

- `classify_regimes(pixel_df)` — adds regime_temp, regime_wind, regime_grid, is_extreme columns
- `compute_thresholds(hourly_weather)` — compute percentile thresholds

### `download_data/pull_sced_shadow.py` *(new)*
Downloads SCED shadow prices and binding transmission constraints from ERCOT API (NP6-86-CD).

**Output**: `{raw}/ercot/sced_shadow/{year}/{mm}/shadow_{YYYYMMDD}.csv`

### Interactive / EDA notebooks (not in pipeline)
- `analysis/local_node_lr.qmd` — node-level regression, interactive use
- `analysis/analysis_forecast_error_eda.py` — treatment/control LMP maps per cluster
- `analysis/gridded_lr.qmd` — superseded by `gridded_infrastructure_lr.py`; kept for reference

## Output File Manifest

| Key | Path | Produced by |
|-----|------|-------------|
| `cluster_map` | `figures/cluster_heterogeneity/cluster_map.png` | `cluster_heterogeneity_lr.py` |
| `coef_plot` | `figures/cluster_heterogeneity/coef_plot_combined.png` | `cluster_heterogeneity_lr.py` |
| `hist_1h` | `figures/cluster_heterogeneity/hist_grid_1h.png` | `cluster_heterogeneity_lr.py` |
| `hist_dah` | `figures/cluster_heterogeneity/hist_grid_dah.png` | `cluster_heterogeneity_lr.py` |
| `cluster_table` | `tables/cluster_regression_results.csv` | `cluster_heterogeneity_lr.py` |
| `corr_heatmap` | `figures/correlation_heatmaps/corr_heatmap_2x2.png` | `forecast_error_lmp_corr_heatmap.py` |
| `pixel_reg_map` | `figures/pixel_regressions/pixel_regression_2x2.png` | `pixel_regression_maps.py` |
| `pixel_table` | `tables/pixel_regression_summary.csv` | `pixel_regression_maps.py` |
| `infra_coef` | `figures/infrastructure_regressions/coef_plot_main.png` | `gridded_infrastructure_lr.py` |
| `infra_seasonal` | `figures/infrastructure_regressions/coef_plot_seasonal.png` | `gridded_infrastructure_lr.py` |
| `infra_table` | `tables/infrastructure_regression_main.csv` | `gridded_infrastructure_lr.py` |
| `report` | `output/analysis_report.pdf` | `create_analysis_report.py` |
| `forecast_value_map` | `figures/forecast_value/forecast_value_total_{depvar}.png` | `forecast_value_map.py` |
| `forecast_value_2x2` | `figures/forecast_value/forecast_value_by_error_{depvar}.png` | `forecast_value_map.py` |
| `forecast_value_table` | `tables/forecast_value_{depvar}.csv` | `forecast_value_map.py` |
| `extreme_weather_table` | `tables/extreme_weather_regression_{depvar}_{regime}.csv` | `extreme_weather_regressions.py` |
| `asymmetry_table` | `tables/asymmetry_{depvar}.csv` | `extreme_weather_regressions.py` |

---

## Troubleshooting

### ERCOT API returns 401
Requires BOTH Bearer token (OAuth) AND subscription key. Verify `~/keys/ercot_user.txt` (6 chars) and `~/keys/ercot_pwd.txt` are current.

### NCEI API timeouts
120s timeout with 0.25s delay between requests. Re-run if it times out — already-downloaded files are skipped.

### ERA5-Land: CDS API errors
- **401 / invalid key**: Verify `~/.cdsapirc` uses new-style URL (`https://cds.climate.copernicus.eu/api`) and current API key.
- **`data_format` key not accepted**: Requires `cdsapi>=0.7.0`. Run `uv sync`.
- **Request queued for a long time**: ERA5-Land queue is server-side; a full month takes 5–20 minutes.

### pixel_hourly parquet missing / wrong filename
The combined pipeline writes `pixel_hourly_gfs+hrrr_{year}_{mm}.parquet`. Old single-model files (`pixel_hourly_hrrr_*`) are still on disk from earlier runs but are not used by the current pipeline. Re-run `build_pixel_hourly_dataset(year, month)` to regenerate with both models.

### node_hourly cache from old single-model run
Old files like `node_hourly_hrrr_era5_*.csv` remain on disk. The current pipeline writes `node_hourly_gfs+hrrr_era5_*.csv`. Delete old caches or use `force_rebuild=True` to regenerate.

### `KeyError: 'station_id'` in cluster aggregation
The ERA5 error path does not produce a `station_id` column — nodes are matched directly to ERA5 cells. `aggregate_to_cluster_hour` detects this automatically and uses `settlement_point` instead.

# currentDate
Today's date is 2026-03-23.
