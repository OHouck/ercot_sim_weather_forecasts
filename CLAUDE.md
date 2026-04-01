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
│   ├── ercot/                  # Step 3: ERCOT market data
│   │   ├── dam_spp/2025/{mm}/  # 31 daily CSVs — day-ahead settlement point prices
│   │   ├── rt_spp/2025/{mm}/   # 31 daily CSVs — real-time settlement point prices
│   │   ├── actual_load/2025/{mm}/    # hourly actual load by weather zone
│   │   ├── demand_forecast/2025/{mm}/ # hourly demand forecasts (1h + 18h) by weather zone
│   │   ├── sced_shadow/2025/{mm}/    # Step 3b: SCED shadow prices (daily CSVs)
│   │   │   └── shadow_{YYYYMMDD}.csv
│   │   ├── sced/               # Step 7b: 60-Day SCED Disclosure (nested ZIPs by release month)
│   │   │   ├── jan2025/        # release month folders — each contains ~2 outer ZIPs
│   │   │   ├── feb2025/        # outer ZIP → inner ZIP per day → 7 CSVs per day
│   │   │   ├── ...             # naming: cdr.np3-965-er.*.zip
│   │   │   ├── dec2025/
│   │   │   ├── jan2026/        # contains Nov 2025 operating data
│   │   │   └── feb2026/        # contains Dec 2025 operating data
│   │   └── np4_160/            # Step 4a: Settlement point mapping (5 CSVs)
│   │       ├── Resource_Node_to_Unit_*.csv
│   │       ├── Settlement_Points_*.csv
│   │       └── ...
│   └── eia860/
│       └── texas_generators.csv  # Step 4b: EIA plant data with lat/lon + LMP node names
├── data/                       # Static GIS / reference data (checked into git)
│   ├── {rtmLmp,rtmSpp,damSpp2,damSpp7}_html_source.txt  # Step 4c: ERCOT HTML contour maps
│   ├── rtmLmpPoints.kml        # Step 4c: 2019 ERCOT KML snapshot
│   ├── Line_Output.shp (+ .dbf/.prj/.shx)  # Step 5c: Transmission line GIS
│   └── Bus_Output.shp (+ .dbf/.prj/.shx)   # Step 5c: ERCOT bus GIS (123 simulation buses)
├── processed_data/
│   ├── node_coordinates.csv            # Step 4c: ~596 matched nodes with lat/lon
│   ├── unmatched_ercot_settlement_points.csv
│   ├── unmatched_eia860_plants.csv
│   ├── forecast_errors/{model}/2025/{mm}/     # Step 5a: Per-station error CSVs
│   │   ├── {station_id}.csv
│   │   └── error_summary.csv
│   ├── forecast_errors_era5/{model}/2025/{mm}/ # Step 5b: ERA5 gridded errors
│   │   ├── era5_errors_{YYYYMM}.nc
│   │   └── error_summary.csv
│   ├── gridded_generation_map.nc              # Step 5c: Static generation/infra map
│   ├── congestion_metrics/                    # Step 7a: Hourly congestion + pixel-level
│   │   ├── congestion_hourly_{YYYYMM}.csv
│   │   └── constraint_by_pixel_{YYYYMM}.csv
│   ├── curtailment_metrics/                   # Step 7b: Hourly curtailment + pixel-level
│   │   ├── curtailment_hourly_{YYYYMM}.csv
│   │   └── curtailment_by_pixel_{YYYYMM}.csv
│   ├── load_errors_by_weather_zone/           # Step 5e dependency
│   │   └── load_errors_wz_{tag}.csv
│   ├── combined_hourly_gridded_data/          # Step 5d: Pixel × hour dataset
│   │   └── pixel_hourly_gfs+hrrr_{year}_{mm}.parquet  # ~978k rows, 65 cols
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
| SCED shadow prices | ERCOT API (NP6-86-CD) | OAuth2 + subscription key | `download_data/pull_sced_shadow.py` |
| 60-Day SCED Disclosure | ERCOT MIS portal (NP3-966-ER) | Manual download | `process_data/process_curtailment.py` |
| NP4-160 SP mapping | ERCOT MIS public download | No | `download_data/pull_np4160.py` |
| EIA Form 860 plants | EIA website | No | `download_data/pull_eia860.py` |
| Node coords HTML | ERCOT contour map HTML (4 pages) | No | `data/*_html_source.txt` |
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

---

## Step 3b: SCED Shadow Prices (DONE)

**Script**: `download_data/pull_sced_shadow.py`

Downloads SCED shadow prices and binding transmission constraints from the ERCOT API (NP6-86-CD). One CSV per day stored in `{raw}/ercot/sced_shadow/2025/{mm}/shadow_{YYYYMMDD}.csv`.

```bash
uv run python -m download_data.pull_sced_shadow --year 2025
```

**Shadow price CSV columns**: `SCEDTimestamp, repeatedHourFlag, constraintID, constraintName, contingencyName, shadowPrice, maxShadowPrice, limit, value, violatedMW, fromStation, toStation, fromStationkV, toStationkV, CCTStatus`

**Coverage**: Full year 2025 (345 daily files; 20 days missing across Aug–Nov due to ERCOT API HTTP 500 errors — analysis uses ~94% coverage for those months).

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

**Result**: ~596 resource nodes matched. Cached to `{processed}/node_coordinates.csv`.

### Settlement point types in RT SPP data

Only RN (Resource Node) types are used. Other types (PCCRN, LCCRN, PUN, LZ/HU/SH/AH) use different naming conventions and are excluded.

---

## Step 5: Forecast Error Calculation (DONE)

**Script**: `process_data/calculate_forecast_errors.py`

**Timezone convention**: All output `valid_time` columns are **US/Central (tz-naive)**. July CDT = UTC−5; January CST = UTC−6.

### Step 5a: Station-level errors

~202 Texas weather stations. One CSV per station per model per month.

**Output**: `{processed}/forecast_errors/{model}/{year}/{month:02d}/{station_id}.csv`

### Step 5b: ERA5 gridded errors (ERA5-Land as ground truth)

~14,000 ERA5 cells. Dense spatial coverage. **This is the primary error source for analysis.**

```bash
uv run python -c "
from process_data.calculate_forecast_errors import calculate_era5_errors_for_month
for month in range(1, 13):
    calculate_era5_errors_for_month(2025, month, model='hrrr')  # HRRR leads 1h + 18h
    calculate_era5_errors_for_month(2025, month, model='gfs')   # GFS day-ahead (lead=0)
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

Builds the main analysis-ready dataset by merging ERA5 forecast errors from both models (HRRR + GFS), the generation map, system-level LMP, congestion metrics (Step 7a), and curtailment metrics (Step 7b). One row per (pixel, hour) for all infrastructure pixels.

```bash
uv run python -c "
from process_data.combine_forecast_generation_node import build_pixel_hourly_dataset
for month in range(1, 13):
    build_pixel_hourly_dataset(2025, month, force_rebuild=True)
"
```

### Key functions
- `build_pixel_hourly_dataset(year, month, models=None, force_rebuild=False)` — main entry point; `models` defaults to `{'hrrr': (1,), 'gfs': (0,)}`
- `flatten_era5_errors(year, month, model)` — ERA5 errors → wide DataFrame (one row per pixel × hour per lead)
- `flatten_generation_map()` — generation map NetCDF → DataFrame (infrastructure pixels only)
- `compute_system_lmp_hourly(year, month)` — RT SPP (RN nodes only) → system-level hourly `system_lmp_mean`, `system_lmp_max`, `system_lmp_std`

The build pipeline has 6 steps internally:
1. ERA5 forecast errors (HRRR + GFS)
2. Generation map (infrastructure pixels only)
3. System LMP
4. Merge all three
5. Merge congestion metrics (if shadow data exists)
6. Merge curtailment metrics (if SCED disclosure data exists)

### Output
- `{processed}/combined_hourly_gridded_data/pixel_hourly_gfs+hrrr_{year}_{mm}.parquet`
- ~978,000 rows/month (≈5,000 infrastructure pixels × 744 hours/month), **65 columns**

**Columns** (65 total):

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
| `system_lmp_mean` | float | System-wide mean LMP [$/MWh] |
| `system_lmp_max` | float | System-wide max LMP across RN nodes [$/MWh] |
| `system_lmp_std` | float | System-wide std dev of LMP across RN nodes [$/MWh] |
| `hour_of_day` | int | 0–23 [Central] |
| `day_of_month` | int | 1–31 |
| `weekday` | int | 0=Monday … 6=Sunday |
| `month` | int | 1–12 |
| `n_binding_constraints` | int | Number of distinct binding constraints per hour |
| `total_shadow_cost` | float | Sum of shadow prices across all SCED intervals [$/MW] |
| `max_shadow_price` | float | Max shadow price in the hour [$/MW] |
| `shadow_cost_weighted` | float | Shadow price × violated MW |
| `n_violations` | int | Number of positive violations |
| `total_violated_mw` | float | Total violated MW [MW] |
| `mean_shadow_cost_per_interval` | float | Normalized by ~12 SCED intervals/hour |
| `wind_curtailment_mw` | float | System-wide wind curtailment (avg over SCED intervals) [MW] |
| `solar_curtailment_mw` | float | System-wide solar curtailment [MW] |
| `total_curtailment_mw` | float | Wind + solar curtailment [MW] |
| `wind_curtailment_pct` | float | Wind curtailment / wind HSL [%] |
| `solar_curtailment_pct` | float | Solar curtailment / solar HSL [%] |
| `n_curtailed_units` | int | Number of curtailed renewable units (> 5 MW threshold) |

---

## Step 5e: Node × Hour Dataset (DONE)

**Script**: `process_data/prepare_node_level_data.py`

Builds a node × hour dataset linking each ERCOT resource node's LMP to weather forecast errors from both models.

```bash
uv run python -c "
from process_data.prepare_node_level_data import prepare_node_level_data
prepare_node_level_data(
    months=[(2025, m) for m in range(1, 13)],
    error_source='era5',
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

---

## Step 6: Cluster × Hour Dataset (DONE)

**Script**: `process_data/prepare_cluster_level_data.py`

Clusters ERCOT nodes geographically and by LMP patterns, then aggregates weather and LMP data to a cluster × hour level.

```bash
uv run python -c "
from process_data.prepare_cluster_level_data import build_cluster_hourly_data
build_cluster_hourly_data(
    months=[(2025, m) for m in range(1, 13)],
    n_clusters=7,
    geo_weight=2.0,
    n_neighbors=8,
)
"
```

**Default clustering parameters**: `n_clusters=7`, `geo_weight=2.0`, `n_neighbors=8`

### Clustering approach
- Features: standardized `[lat, lon]` (weighted by `geo_weight`) + LMP summary stats
- Algorithm: `AgglomerativeClustering` with k-NN connectivity constraint (geographic contiguity)
- Small clusters (< `min_cluster_size`) reassigned to nearest valid cluster
- Use `sweep_n_clusters()` to choose k via silhouette scores

### Output files
- `{processed}/cluster_hourly_{models_key}_k{k}_{tag}.csv` — one row per (cluster, hour)
- `{processed}/node_clusters_{models_key}_k{k}_{tag}.csv` — cluster label per node
- `{processed}/cluster_polygons_{models_key}_k{k}_{tag}.gpkg` — convex-hull polygons

---

## Step 7a: Congestion Processing (DONE)

**Script**: `process_data/process_congestion.py`

Processes SCED shadow price CSVs into hourly system-level and pixel-level congestion metrics. Automatically called by `build_pixel_hourly_dataset` when shadow data exists.

```bash
uv run python -c "
from process_data.process_congestion import compute_hourly_congestion_metrics
for month in range(1, 13):
    compute_hourly_congestion_metrics(2025, month, force_rebuild=True)
"
```

### Constraint geolocation

`geolocate_constraints(shadow_df)` maps `fromStation`/`toStation` names to lat/lon using three sources in priority order:

1. **Node coordinates prefix match** (`_load_node_coordinates`): extracts substation prefix from settlement point names (e.g., `AJAXWIND_RN` → `AJAXWIND`), matches to shadow station names — ~15 matches
2. **EIA generator name match** (`_load_eia_generators`): matches against plant names and LMP node designations — ~12 matches
3. **Bus_Output.shp fallback**: 123-bus simulation model shapefile — ~1 match

**Match rate**: ~28/274 stations (10%) → 30/188 constraints geolocated per month. The fundamental limitation is that shadow price `fromStation`/`toStation` are ERCOT SCADA bus abbreviations — a different identifier namespace from settlement points or plant names.

### Outputs
- `{processed}/congestion_metrics/congestion_hourly_{YYYYMM}.csv` — system-level hourly metrics
- `{processed}/congestion_metrics/constraint_by_pixel_{YYYYMM}.csv` — per-pixel hourly shadow costs (~889 rows/month)

---

## Step 7b: Renewable Curtailment (DONE)

**Script**: `process_data/process_curtailment.py`

Processes ERCOT 60-Day SCED Disclosure nested ZIP archives to extract per-unit wind and solar curtailment. Curtailment = `max(0, HSL − Telemetered Net Output)` for WIND and PVGR resource types.

```bash
uv run python -c "
from process_data.process_curtailment import (
    compute_hourly_curtailment,
    compute_hourly_curtailment_by_pixel,
)
for month in range(1, 13):
    compute_hourly_curtailment(2025, month, force_rebuild=True)
    compute_hourly_curtailment_by_pixel(2025, month, force_rebuild=True)
"
```

### SCED Disclosure folder structure (CRITICAL)

**Folders are named by RELEASE month, not operating month.** ERCOT publishes with a ~60-day lag:
- `march2025/` → contains operating data from **January 2025**
- `april2025/` → contains operating data from **February 2025**
- ... and so on (N+2 offset)

`_find_sced_folders(year, month)` correctly resolves operating month → release folders, looking in N+2 (primary) and N+1 (secondary) for boundary-day coverage.

**Inner ZIP structure**: Each outer ZIP contains inner ZIPs named by release date. The CSV filenames inside also use the release date. **Always read actual operating dates from the `SCED Time Stamp` column inside the CSV, not from the filename.**

**2025 coverage**: 363/365 operating days (Jan 31 and Dec 31 missing — `feb2026` folder provides Dec 2025 data).

### Data quality
- No NaN values in `HSL` or `Telemetered Net Output` columns
- ~388–395 wind units (WIND type), ~228–273 solar units (PVGR type) per day
- 96 SCED intervals per day (5-minute resolution)
- Resource types beyond WIND/PVGR: PWRSTR (battery), SCLE90, CCGT90, SCGT90, etc. — only WIND and PVGR are used

### Resource geolocation

`geolocate_curtailment_resources(resource_names)` maps SCED resource names (e.g., `AJAXWIND_UNIT1`) to lat/lon using:
1. **Node coordinates** (`_sced_name_to_prefix` strips unit suffixes → prefix match against settlement point names) — ~230-232 resources
2. **EIA generators** (plant name / LMP node prefix match) — ~33 resources
- **Match rate**: ~269/652 resources (41%), covering ~143 unique ERA5 pixels

### Curtailment statistics (2025)

| Month | Hours | Missing Days | Mean Wind MW | Mean Solar MW | Max Total MW |
|-------|-------|-------------|-------------|--------------|-------------|
| Jan | 720 | Jan 31 | 720 | 432 | 15,256 |
| Feb | 672 | None | 642 | 387 | 12,347 |
| Mar | 743 | 1 hour | 1,618 | 1,197 | 16,554 |
| Apr | 720 | None | 1,601 | 822 | 14,914 |
| May | 744 | None | 708 | 489 | 8,551 |
| Jun | 720 | None | 620 | 551 | 12,253 |
| Jul | 744 | None | 272 | 292 | 4,872 |
| Aug | 744 | None | 150 | 136 | 2,086 |
| Sep | 720 | None | 131 | 329 | 2,868 |
| Oct | 744 | None | 582 | 608 | 16,240 |
| Nov | 720 | None | 824 | 435 | 17,124 |
| Dec | 720 | Dec 31 | 1,030 | 324 | 11,612 |

Spring (Mar–Apr) has peak curtailment; summer (Jul–Aug) lowest. High Oct/Nov curtailment driven by wind.

### Outputs
- `{processed}/curtailment_metrics/curtailment_hourly_{YYYYMM}.csv` — system-level hourly metrics
- `{processed}/curtailment_metrics/curtailment_by_pixel_{YYYYMM}.csv` — per-pixel hourly curtailment (~106k rows/month for located resources)

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

# Step 3b: SCED shadow prices (~2-4 hours)
uv run python -m download_data.pull_sced_shadow --year 2025

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

# Step 7a: Process congestion metrics from shadow prices
uv run python -c "
from process_data.process_congestion import compute_hourly_congestion_metrics
for month in range(1, 13):
    compute_hourly_congestion_metrics(2025, month, force_rebuild=True)
"

# Step 7b: Process curtailment metrics from 60-day SCED disclosure
# (SCED disclosure ZIPs must be manually downloaded from ERCOT MIS NP3-966-ER
#  and placed in {raw}/ercot/sced/{release_month_name}{year}/)
uv run python -c "
from process_data.process_curtailment import compute_hourly_curtailment, compute_hourly_curtailment_by_pixel
for month in range(1, 13):
    compute_hourly_curtailment(2025, month, force_rebuild=True)
    compute_hourly_curtailment_by_pixel(2025, month, force_rebuild=True)
"

# Step 5d: Pixel × hour dataset — combined HRRR + GFS + congestion + curtailment
uv run python -c "
from process_data.combine_forecast_generation_node import build_pixel_hourly_dataset
for month in range(1, 13):
    build_pixel_hourly_dataset(2025, month, force_rebuild=True)
"

# Step 5e: Node × hour dataset
uv run python -c "
from process_data.prepare_node_level_data import prepare_node_level_data
prepare_node_level_data(months=[(2025, m) for m in range(1, 13)], error_source='era5')
"

# Step 6: Cluster × hour dataset
uv run python -c "
from process_data.prepare_cluster_level_data import build_cluster_hourly_data
build_cluster_hourly_data(months=[(2025, m) for m in range(1, 13)], n_clusters=7, geo_weight=2.0, n_neighbors=8)
"

# ── Analysis Pipeline ──────────────────────────────────────────────────────────

# A1: Per-cluster heterogeneity regressions
uv run python -m analysis.cluster_heterogeneity_lr

# A2: Raw correlation heatmaps 2×2
uv run python -m analysis.forecast_error_lmp_corr_heatmap

# A3: Pixel-level regression maps 2×2
uv run python -m analysis.pixel_regression_maps

# A4: Infrastructure-level regressions
uv run python -m analysis.gridded_infrastructure_lr

# A5: Extreme weather regime regressions (shadow cost + curtailment DVs)
uv run python -m analysis.extreme_weather_regressions --depvar total_shadow_cost
uv run python -m analysis.extreme_weather_regressions --depvar wind_curtailment_mw

# A6: Forecast value maps
uv run python -m analysis.forecast_value_map --depvar total_shadow_cost
uv run python -m analysis.forecast_value_map --depvar wind_curtailment_mw

# A7: Asymmetry analysis
uv run python -m analysis.extreme_weather_regressions --asymmetry --depvar total_shadow_cost

# A10: Compile unified PDF report (requires typst CLI)
uv run python -m analysis.create_analysis_report

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

- `build_gridded_generation_map(force_rebuild=False)` — main entry point
- `build_era5_template_dataset()` — load ERA5 NetCDF to get lat/lon grid template
- `_bin_generators(generators_path, lats, lons)` — bin EIA generators into ERA5 cells
- `_mark_transmission(lats, lons, shp_path)` — mark cells intersecting transmission lines
- `_mark_load_centers(lats, lons, shp_path)` — mark cells containing ERCOT load buses
- `_tech_slug(tech)` — EIA technology name → safe column slug

### `process_data/combine_forecast_generation_node.py`
Builds the pixel × hour analysis dataset (Step 5d).

- `MODEL_LEAD_TIMES = {'hrrr': (1,), 'gfs': (0,)}` — module-level default models dict
- `build_pixel_hourly_dataset(year, month, models=None, force_rebuild=False)` — main entry point
- `flatten_era5_errors(year, month, model)` — ERA5 errors → wide DataFrame
- `flatten_generation_map()` — generation map → infrastructure pixels only
- `compute_system_lmp_hourly(year, month)` — RT SPP → system-level hourly stats

### `process_data/prepare_node_level_data.py`
Builds the node × hour dataset (Step 5e).

- `prepare_node_level_data(months, models=None, error_source='era5', force_rebuild=False, model=None)` — main entry point
- `_load_era5_errors_for_nodes(nodes_gdf, year, month, model)` — extract ERA5 errors at node coordinates
- `_map_nodes_to_weather_zones(nodes_gdf, zones_shp_path)` — spatial join
- `_load_weather_zone_load_data(year, month)` — actual load + 1h/18h demand forecasts

### `process_data/prepare_cluster_level_data.py`
Clusters nodes and builds the cluster × hour dataset (Step 6).

- `compute_node_lmp_features(months, model)` — per-node LMP summary stats for clustering
- `cluster_nodes(node_features_gdf, n_clusters, geo_weight, n_neighbors, min_cluster_size)` — agglomerative clustering
- `sweep_n_clusters(node_features_gdf, k_range, ...)` — silhouette scores vs k
- `build_cluster_polygons(node_clusters_gdf)` — convex-hull polygon per cluster
- `aggregate_to_cluster_hour(df, node_clusters, leads, ...)` — aggregate weather/LMP to cluster level
- `build_cluster_hourly_data(months, models=None, n_clusters=7, geo_weight=2.0, n_neighbors=8, ...)` — main entry point

### `process_data/process_ercot.py`
Reads and processes ERCOT market data:

- `load_dam_spp_month(year, month)` — loads all daily DAM SPP CSVs
- `load_rt_spp_month(year, month)` — loads all daily RT SPP CSVs
- `compute_max_lmp_by_node(year, month, point_types='RN')` — max LMP per settlement point
- `build_node_coordinates(force_rebuild=False)` — name-matching pipeline (Step 4c)
- `load_actual_load_month(year, month)` — actual hourly load by weather zone
- `load_demand_forecasts_month(year, month)` — 1h and 18h demand forecasts by weather zone

### `process_data/process_congestion.py`
Processes SCED shadow prices into hourly congestion metrics (Step 7a).

- `compute_hourly_congestion_metrics(year, month, force_rebuild=False)` — system-level hourly congestion
- `compute_constraint_hourly_by_pixel(year, month, force_rebuild=False)` — per-pixel hourly shadow costs
- `geolocate_constraints(shadow_df)` — map constraints to lat/lon using node_coordinates, EIA generators, and Bus_Output.shp (multi-source, ~10% station match rate)
- `_load_node_coordinates()` — loads node_coordinates.csv, extracts substation prefixes
- `_load_eia_generators()` — loads texas_generators.csv for EIA plant coordinates
- `_build_station_coords(shadow_stations)` — multi-source station → (lat, lon) mapping
- `merge_congestion_system(pixel_df, year, month)` — merge system-level metrics into pixel data
- `merge_congestion_local(pixel_df, year, month)` — merge pixel-level congestion metrics

### `process_data/process_curtailment.py`
Processes 60-Day SCED Disclosure into curtailment metrics (Step 7b).

- `compute_hourly_curtailment(year, month, force_rebuild=False)` — system-level hourly wind/solar curtailment
- `compute_hourly_curtailment_by_pixel(year, month, force_rebuild=False)` — per-pixel curtailment for geolocated resources
- `geolocate_curtailment_resources(resource_names)` — maps SCED resource names → lat/lon (~41% match rate)
- `_sced_name_to_prefix(name)` — strips unit suffixes from SCED resource names
- `_load_sced_disclosure_month(year, month)` — extracts renewable records from nested ZIPs
- `_find_sced_folders(year, month)` — resolves operating month → release folder(s) via 60-day lag
- `merge_curtailment_system(pixel_df, year, month)` — merge system-level metrics into pixel data

### `process_data/classify_weather_regimes.py`
Classifies each hour into weather/grid regimes using system-wide percentile thresholds.

- `classify_regimes(pixel_df)` — adds regime_temp, regime_wind, regime_grid, is_extreme columns
- `compute_thresholds(hourly_weather)` — compute percentile thresholds

### `download_data/pull_sced_shadow.py`
Downloads SCED shadow prices from ERCOT API (NP6-86-CD).

- `download_shadow_year(year)` — downloads all 12 months
- `download_shadow_month(year, month)` — downloads one month

**Output**: `{raw}/ercot/sced_shadow/{year}/{mm}/shadow_{YYYYMMDD}.csv`

---

## Analysis Pipeline

All analysis scripts use the combined HRRR+GFS pipeline by default (`LEAD_SHORT=1` for HRRR, `LEAD_DAH=0` for GFS day-ahead). Each script saves figures to `{OneDrive}/figures/` and tables to `{repo}/tables/`. The `create_analysis_report.py` script assembles everything into `output/analysis_report.pdf`.

### `analysis/cluster_heterogeneity_lr.py`
Per-cluster regressions showing heterogeneous treatment effects (Step 6 cluster data).

**Entry point**: `run_cluster_analysis(months, n_clusters, geo_weight, n_neighbors, force_rebuild)`

**Outputs**:
- `figures/cluster_heterogeneity/cluster_map.png`
- `figures/cluster_heterogeneity/coef_plot_combined.png` — 2×3 grid (HRRR 1h / GFS day-ahead × temp / wind / load)
- `figures/cluster_heterogeneity/hist_grid_1h.png`, `hist_grid_dah.png`
- `tables/cluster_regression_results.csv`

### `analysis/forecast_error_lmp_corr_heatmap.py`
Spatial heatmap of per-pixel Pearson correlation between forecast errors and system LMP spread.

**Entry point**: `run_correlation_heatmaps(months, lmp_var, overlay, save_dir)`

**Outputs**: `figures/correlation_heatmaps/corr_heatmap_2x2.png`

### `analysis/pixel_regression_maps.py`
Per-pixel OLS regressions of `system_lmp_std` on forecast errors with controls and absorbed FE.

**Entry point**: `run_pixel_regression_maps(months, save_dir)`

**Outputs**: `figures/pixel_regressions/pixel_regression_2x2.png`, `tables/pixel_regression_summary.csv`

### `analysis/gridded_infrastructure_lr.py`
Aggregates ERA5 forecast errors by infrastructure type, regresses system LMP spread.

**Entry point**: `run_infrastructure_analysis(months, save_dir)`

**Outputs**: `figures/infrastructure_regressions/{coef_plot_main,coef_plot_seasonal}.png`, `tables/infrastructure_regression_{main,seasonal}.csv`

### `analysis/extreme_weather_regressions.py`
Regime-conditional pixel regressions for extreme weather events; also asymmetry analysis.

**Entry point**: `run_regime_regressions(months, depvar)`, `run_asymmetry_regressions(months, depvar)`

**Regimes**: extreme_cold, extreme_heat, high_wind, stressed_grid

**Outputs**: `tables/extreme_weather_regression_{depvar}_{regime}.csv`, `tables/asymmetry_{depvar}.csv`

### `analysis/forecast_value_map.py`
Dollar value of forecast accuracy at each pixel: `value = |β| × σ(error)`.

**Entry point**: `run_forecast_value_analysis(months, depvar)`, `run_regime_value_comparison(months, depvar)`

**Outputs**: `figures/forecast_value/forecast_value_by_error_{depvar}.png`, `figures/forecast_value/forecast_value_total_{depvar}.png`, `tables/forecast_value_{depvar}.csv`

### `analysis/create_analysis_report.py`
Assembles all figures and tables into a unified Typst PDF report.

**Entry point**: `create_analysis_report(output_dir)`

**Output**: `output/analysis_report.pdf`

### `analysis/node_gnn.py`
Graph Neural Network predicting node-level LMP from weather features. Not yet integrated into the report pipeline.

---

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
| `congestion_hourly` | `processed_data/congestion_metrics/congestion_hourly_{YYYYMM}.csv` | `process_congestion.py` |
| `constraint_by_pixel` | `processed_data/congestion_metrics/constraint_by_pixel_{YYYYMM}.csv` | `process_congestion.py` |
| `curtailment_hourly` | `processed_data/curtailment_metrics/curtailment_hourly_{YYYYMM}.csv` | `process_curtailment.py` |
| `curtailment_by_pixel` | `processed_data/curtailment_metrics/curtailment_by_pixel_{YYYYMM}.csv` | `process_curtailment.py` |
| `pixel_hourly` | `processed_data/combined_hourly_gridded_data/pixel_hourly_gfs+hrrr_{year}_{mm}.parquet` | `combine_forecast_generation_node.py` |

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

### SCED disclosure: operating month not found
`_find_sced_folders()` looks for release folders N+2 and N+1. Folder names must match the pattern `{month_name}{year}` (e.g., `march2025`, `jan2026`). November 2025 data is in `jan2026/`; December 2025 is in `feb2026/`. If a folder is missing, download from ERCOT MIS portal (NP3-966-ER) and place it in `{raw}/ercot/sced/`.

### SCED disclosure: date confusion (filename vs content)
CSV filenames inside the nested ZIPs use the **release date**, not the operating date. E.g., `60d_SCED_Gen_Resource_Data-07-JUL-25.csv` inside the `july2025` folder contains operating data from **May 8, 2025**. Always rely on the `SCED Time Stamp` column for the actual operating date. The pipeline handles this correctly by filtering on `sced_time.dt.month == month` after parsing.

### pixel_hourly parquet missing / wrong filename
The combined pipeline writes `pixel_hourly_gfs+hrrr_{year}_{mm}.parquet`. Old single-model files (`pixel_hourly_hrrr_*`) are still on disk from earlier runs but are not used. Re-run `build_pixel_hourly_dataset(year, month, force_rebuild=True)` to regenerate.

### node_hourly cache from old single-model run
Old files like `node_hourly_hrrr_era5_*.csv` remain on disk. The current pipeline writes `node_hourly_gfs+hrrr_era5_*.csv`. Delete old caches or use `force_rebuild=True`.

### `KeyError: 'station_id'` in cluster aggregation
The ERA5 error path does not produce a `station_id` column. `aggregate_to_cluster_hour` detects this automatically and uses `settlement_point` instead.

# currentDate
Today's date is 2026-03-27.
