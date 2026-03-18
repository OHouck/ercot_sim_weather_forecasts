# CLAUDE.md — MVP Data Build Guide

## Research Question
How do joint errors in 24hr wind and temperature forecasts impact locational marginal prices (LMP) and renewable curtailment in ERCOT?

## MVP Scope
**July 2025 only.** Build and validate the full pipeline for one month before scaling.

## Directory Structure
All raw data is stored on OneDrive via `helper_funcs.setup_directories()`:
```
root = /Users/ohouck/Library/CloudStorage/OneDrive-TheUniversityofChicago/ercot_sim_weather_forecasts
```

Layout:
```
{root}/
├── raw_data/
│   ├── ndfd_data/              # Step 1a: NDFD weather forecasts
│   │   ├── temp/2025/07/       # ~248 NetCDF files
│   │   ├── wspd/2025/07/       # ~248 NetCDF files
│   │   └── wdir/2025/07/       # ~248 NetCDF files
│   ├── hrrr_data/              # Step 1b: HRRR weather forecasts
│   │   ├── temp/2025/07/       # NetCDF files (one per cycle × lead time)
│   │   ├── wspd/2025/07/
│   │   └── wdir/2025/07/
│   ├── era5_land/              # Step 1c: ERA5-Land reanalysis
│   │   └── 2025/07/            # era5_land_202507.nc (hourly, all vars)
│   ├── weather_stations/       # Step 2: ISD realized observations
│   │   ├── stations.csv        # 205 Texas station metadata
│   │   └── 2025/07/            # ~202 per-station hourly CSVs
│   ├── ercot/                  # Step 3: ERCOT market data
│   │   ├── dam_spp/2025/07/    # 31 daily CSVs (settlement point level)
│   │   ├── rt_spp/2025/07/     # 31 daily CSVs
│   │   └── np4_160/            # Step 4a: Settlement point mapping (5 CSVs)
│   └── eia860/                 # Step 4b: EIA Form 860 plant data
│       └── texas_plants.csv    # 1,369 TX plants with lat/lon
├── processed_data/
│   ├── node_coordinates.csv    # Step 4c: 544 matched nodes with lat/lon
│   ├── unmatched_ercot_settlement_points.csv  # Unmatched ERCOT nodes for review
│   ├── unmatched_eia860_plants.csv            # Unmatched EIA plants for review
│   ├── forecast_errors/{model}/2025/07/       # Step 5a: Per-station error CSVs + summary
│   └── forecast_errors_era5/{model}/2025/07/  # Step 5b: ERA5 gridded errors (NetCDF + summary)
└── plots/                      # Generated visualizations
    ├── max_temp_july_2025.png
    ├── max_wind_speed_july_2025.png
    └── combined_map_july_2025.png
```

## Data Sources Summary

| Dataset | Source | Auth | Script |
|---------|--------|------|--------|
| NDFD forecasts | NOAA S3 `s3://noaa-ndfd-pds/wmo/` | No | `download_data/pull_ndfd.py` |
| HRRR forecasts | AWS S3 `noaa-hrrr-bdp-pds` | No | `download_data/pull_hrrr.py` |
| ERA5-Land reanalysis | Copernicus CDS API | CDS API key | `download_data/pull_era5.py` |
| Realized weather | NCEI ISD API | No | `download_data/pull_weatherstation.py` |
| Day-ahead SPP | ERCOT API (NP4-190) | OAuth2 + subscription key | `download_data/pull_ercot.py` |
| Real-time SPP | ERCOT API (NP6-905) | OAuth2 + subscription key | `download_data/pull_ercot.py` |
| NP4-160 SP mapping | ERCOT MIS public download | No | `download_data/pull_np4160.py` |
| EIA Form 860 plants | EIA website | No | `download_data/pull_eia860.py` |
| Node coords HTML | ERCOT contour map HTML (4 pages) | No | `data/*_html_source.txt` |
| Node coords KML | GitHub (cached 2019 ERCOT snapshot) | No | `data/rtmLmpPoints.kml` |
| Station forecast errors | NDFD/HRRR + ISD (Steps 1+2) | No | `process_data/calculate_forecast_errors.py` |
| ERA5 gridded errors | NDFD/HRRR + ERA5 (Steps 1+1c) | No | `process_data/calculate_forecast_errors.py` |
| Validation | All above | — | `download_data/validate_data.py` |

## Credentials
- `~/keys/ercot_api_key.txt` — ERCOT API subscription key (32 chars)
- `~/keys/ercot_api_secondary_key.txt` — backup subscription key
- `~/keys/ercot_user.txt` — ERCOT account username
- `~/keys/ercot_pwd.txt` — ERCOT account password
- `~/.cdsapirc` — Copernicus CDS API key for ERA5-Land downloads (new-style endpoint: `url: https://cds.climate.copernicus.eu/api`)

---

## Step 0: Project Setup (DONE)

Changes made:
- `helper_funcs.py`: Added `raw` and `processed` keys to `setup_directories()`
- `pyproject.toml`: Added `xarray`, `cfgrib`, `netcdf4`, `geopandas`, `cartopy`, `openpyxl`, `cdsapi` dependencies
- Run `uv sync` to install

Prerequisites: `brew install awscli eccodes`

---

## Step 1: NDFD Weather Forecasts (DONE)

**Script**: `download_data/pull_ndfd.py` (pre-existing, already working)

Downloads NDFD 2.5km CONUS forecast GRIB2 files from NOAA S3, extracts Texas bounding box (lat 25.8-36.5, lon -106.6 to -93.5), saves as compressed NetCDF. Keeps only 1h and 25h lead times from Group B issuances.

### Run for a single month
```python
from download_data.pull_ndfd import download_and_extract_texas_month
from helper_funcs import setup_directories
import os

dirs = setup_directories()
base_dir = os.path.join(dirs['raw'], 'ndfd_data')
for element in ['temp', 'wspd', 'wdir']:
    download_and_extract_texas_month(element, year=2025, month=7, base_dir=base_dir)
```

### Results for July 2025
- temp: 248 files, wspd: 248 files, wdir: 248 files
- ~496 GRIB files downloaded per element, ~half have matching lead times
- Each NetCDF file has 2 steps (1h, 25h lead time), ~490×516 grid points

---

## Step 2: Weather Station Observations (DONE)

**Script**: `download_data/pull_weatherstation.py`

Downloads hourly realized weather (temperature, wind) from NOAA's Integrated Surface Database (ISD). These are ground truth observations to compare against NDFD forecasts.

### Run
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
- Timeout: 120s (API can be slow)

### ISD CSV data format
Columns: `STATION, DATE, SOURCE, REPORT_TYPE, CALL_SIGN, QUALITY_CONTROL, TMP, WND`

**TMP field**: `+0333,1` = 33.3°C (value in tenths, quality flag). `+9999` = missing.
```python
def parse_tmp(tmp_str):
    if pd.isna(tmp_str) or '+9999' in str(tmp_str):
        return None
    return int(str(tmp_str).split(',')[0]) / 10.0
```

**WND field**: `170,1,N,0082,1` = direction 170°, speed 8.2 m/s. `999`/`9999` = missing.
```python
def parse_wnd(wnd_str):
    if pd.isna(wnd_str):
        return None, None
    parts = str(wnd_str).split(',')
    direction = int(parts[0]) if parts[0] != '999' else None
    speed = int(parts[3]) / 10.0 if parts[3] != '9999' else None
    return direction, speed
```

### Results for July 2025
- 205 active TX stations found, 202 returned data (3 had no data)
- ~700-1100 rows per station for 31 days (varies by reporting frequency)

---

## Step 3: ERCOT Market Data (DONE)

**Script**: `download_data/pull_ercot.py`

Downloads day-ahead hourly LMP and real-time settlement point prices.

### Run
```bash
uv run python -m download_data.pull_ercot
```

### Key implementation details

**Authentication** (OAuth2 via Azure B2C ROPC flow):
1. POST to `https://ercotb2c.b2clogin.com/ercotb2c.onmicrosoft.com/B2C_1_PUBAPI-ROPC-FLOW/oauth2/v2.0/token`
2. Body: `grant_type=password`, `username`, `password`, `client_id=fec253ea-0d06-4272-a5e6-b478baeecd70`, `scope=openid {client_id} offline_access`
3. Returns `access_token` (Bearer token, ~1053 chars)
4. API calls need BOTH:
   - `Authorization: Bearer {token}` header
   - `Ocp-Apim-Subscription-Key: {api_key}` header

**API response format**:
- JSON with keys: `_meta`, `report`, `fields`, `data`, `_links`
- `data` is a list-of-lists (NOT list-of-dicts)
- `fields` provides column names: `[{"name": "deliveryDate"}, {"name": "hourEnding"}, ...]`
- Must zip `fields` with each `data` row to create dicts
- Max page size: 100,000 records (use `size=100000` param)

**Endpoints**:
| Report | Endpoint | Fields | Records/day |
|--------|----------|--------|-------------|
| DAM SPP | `/np4-190-cd/dam_stlmnt_pnt_prices` | deliveryDate, hourEnding, settlementPoint, settlementPointPrice, settlementPointType, DSTFlag | settlement point level |
| RT SPP | `/np6-905-cd/spp_node_zone_hub` | deliveryDate, deliveryInterval, settlementPointName, settlementPointPrice, settlementPointType | varies |

**Rate limit**: 30 req/min. Use `time.sleep(2)` between requests.

**Pagination**: Check `_meta.totalPages`. Loop until `page >= totalPages`.

### Results for July 2025
- DAM SPP: 31 files (settlement point level prices)
- RT SPP: 31 files

---

## Step 4: ERCOT Node-to-Coordinate Mapping (DONE)

**Scripts**: `download_data/pull_np4160.py`, `download_data/pull_eia860.py`
**Processing**: `process_data/process_ercot.py` → `build_node_coordinates()`

ERCOT settlement points have names (e.g., `AJAXWIND_RN`) but no geographic coordinates. Three sources are combined to build a coordinate mapping:

### 4a: NP4-160-SG Settlement Point Mapping
- Source: `https://www.ercot.com/misdownload/servlets/mirDownload?mimic_duns=000000000&doclookupId=1197364253`
- No auth required. Public download.
- ZIP containing 5 CSVs. Key file: `Resource_Node_to_Unit_*.csv` (1,584 rows)
- Maps `RESOURCE_NODE → UNIT_SUBSTATION → UNIT_NAME`
- 937 unique resource nodes, 745 unique substations
- Output: `{raw}/ercot/np4_160/`

```bash
uv run python -m download_data.pull_np4160
```

### 4b: EIA Form 860 Plant Data
- Source: `https://www.eia.gov/electricity/data/eia860/xls/eia8602024.zip`
- No auth required. Public download. 22 MB ZIP.
- Contains `2___Plant_Y2024.xlsx` with lat/lon for every US power plant
- Filter: `State == 'TX'` or `Balancing Authority Code == 'ERCO'` → 1,369 Texas plants
- Read with `pd.read_excel(f, skiprows=1)` (requires `openpyxl`)
- Output: `{raw}/eia860/texas_plants.csv`

```bash
uv run python -m download_data.pull_eia860
```

### 4c: Node Coordinate Matching Pipeline

See [README_build_node_coordinates.md](README_build_node_coordinates.md) for a detailed explanation.

`process_ercot.build_node_coordinates()` combines three coordinate sources in priority order:

**Source 1: ERCOT HTML contour maps** (preferred, current)
- Files: `data/{rtmLmp,rtmSpp,damSpp2,damSpp7}_html_source.txt`
- Source: 4 live ERCOT contour map pages (`/content/cdr/contours/*.html`)
- 295 unique nodes across 4 pages, each with pixel coords on a 600x600 PNG
- Pixel-to-lat/lon via least-squares affine transform (212 ground control points from KML)
- Affine accuracy: mean 0.9 km, max 1.4 km
- 227 match current NP4-160 resource nodes

**Source 2: ERCOT KML contour map** (2019 snapshot, fills gaps)
- File: `data/rtmLmpPoints.kml` — cached 2019 snapshot from GitHub
- 254 nodes with authoritative lat/lon; 18 additional matches not in HTML
- Also serves as ground control points for the HTML affine calibration

**Source 3: EIA Form 860 name matching** (for remaining nodes)
- Matches ERCOT substation names (from NP4-160) to EIA plant names
- Three strategies: prefix (~179), substring (~52), fuzzy (~68)

**Result**: 544/937 resource nodes matched (58%). Cached to `{processed}/node_coordinates.csv`.
Also saves `unmatched_ercot_settlement_points.csv` and `unmatched_eia860_plants.csv` for manual review.

### Settlement point types in RT SPP data

The RT SPP data (NP6-905) contains multiple settlement point types. Only RN (Resource Node)
types are used in the analysis because only RN names appear in NP4-160 and thus in
`node_coordinates.csv`. The other types use different naming conventions:

| Type | Share | Description | In node_coordinates? |
|------|-------|-------------|---------------------|
| RN | ~70% | Resource nodes (generation sites) | Yes — this is what we match |
| PCCRN | ~15% | Privately Contracted Capacity RN (bilateral contracts, unit-level names) | No |
| LCCRN | ~7% | Load Curve Capability RN (capacity cost participation, unit-level names) | No |
| PUN | ~5% | Point of Use Node (demand/load points, different physical locations) | No |
| LZ/HU/SH/AH | ~3% | Load zones, hubs, sub-hubs (aggregates, not point-level) | No |

PCCRN/LCCRN could theoretically be mapped via NP4-160's `Settlement_Points` file (which has
a SUBSTATION column), but this is not implemented. PUN nodes would require coordinates from
the CRR Network Model or similar source.

### Possible improvements
- **CRR Network Model KML**: Requires free IMRE registration at `https://www.ercot.com/services/rq/imre`. Contains bus polygon geometries for all network buses. Parser exists at `github.com/patrickbrown4/pvvm_pvtos`. Would cover ~100% of nodes.
- Cross-reference EIA Form 860 generator-level data (`3_1_Generator*.xlsx`) for unit-level matching instead of plant-level
- Re-save the 4 HTML source files periodically to capture new nodes added to the ERCOT contour maps
- Map PCCRN/LCCRN names to coordinates via NP4-160 `Settlement_Points` → SUBSTATION → EIA plants

---

## Step 1c: ERA5-Land Reanalysis (DONE)

**Script**: `download_data/pull_era5.py`

Downloads ERA5-Land hourly reanalysis for the Texas bounding box (lat 25.8–36.5, lon -106.6 to -93.5) from the Copernicus CDS API. ERA5-Land provides gap-free gridded observations at ~9 km (0.1°) resolution and serves as a dense ground truth alternative to the ~202 ISD weather stations.

### Run
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
- `wdir = atan2(-u10, -v10) * 180/π (mod 360)` — meteorological wind direction [degrees], matching NDFD/HRRR `wdir10` convention

**Output NetCDF variables**: `t2m` [K], `u10`, `v10`, `wspd`, `wdir` [m/s / degrees]; times stored in UTC; saved with zlib compression (complevel=5).

### Output
- `{raw}/era5_land/{year}/{month:02d}/era5_land_{YYYYMM}.nc`
- ~109 lat × ~132 lon = ~14,388 grid cells covering Texas; 744 hourly steps per month

---

## Step 5: Forecast Error Calculation (DONE)

**Script**: `process_data/calculate_forecast_errors.py`

Merges gridded weather forecasts (NDFD or HRRR) with either ISD station observations (Step 5a) or ERA5-Land reanalysis (Step 5b) to compute forecast errors. Uses `gpd.sjoin_nearest` (projected to EPSG:3857) to match each observation location to its nearest forecast grid cell.

**Timezone convention**: All output `valid_time` columns are **US/Central (tz-naive)**. Conversion from UTC happens at data-load time (`load_forecasts()` and `load_all_observations()`), so all timestamps are already in Central before errors are computed. July CDT = UTC−5; January CST = UTC−6. ⚠ Existing CSVs produced before this change stored UTC times and need to be regenerated.

### Step 5a: Station-level errors (ISD observations as ground truth)

~202 Texas weather stations. One CSV per station.

```bash
# HRRR
uv run python -c "
from process_data.calculate_forecast_errors import calculate_hrrr_errors_for_month
calculate_hrrr_errors_for_month(2025, 7)
"
# NDFD
uv run python -c "
from process_data.calculate_forecast_errors import calculate_ndfd_errors_for_month
calculate_ndfd_errors_for_month(2025, 7)
"
```

**Output**: `{processed}/forecast_errors/{model}/{year}/{month:02d}/`
- `{station_id}.csv` — per-station rows with columns: `station_id, valid_time [Central], lead_hours, forecast_temp, observed_temp, temp_error, temp_pct_error, forecast_wspd, observed_wspd, wspd_error, wspd_pct_error, forecast_wdir, observed_wdir, wdir_degree_error, lat, lon`
- `error_summary.csv` — per-station, per-lead MAE and bias

**Results for HRRR July 2025** (202 stations, lead times 1h + 18h):
- Lead 1h: Temp MAE 1.04°C (bias +0.16), Wind MAE 1.19 m/s (bias +0.08)
- Lead 18h: Temp MAE 1.12°C (bias +0.21), Wind MAE 1.24 m/s (bias +0.03)

### Step 5b: ERA5 gridded errors (ERA5-Land as ground truth)

~14,000 ERA5 cells. Dense spatial coverage; requires Step 1c data.

```bash
uv run python -c "
from process_data.calculate_forecast_errors import calculate_era5_errors_for_month
calculate_era5_errors_for_month(2025, 7, model='hrrr')
"
```

**Output**: `{processed}/forecast_errors_era5/{model}/{year}/{month:02d}/`
- `era5_errors_{YYYYMM}.nc` — compressed NetCDF with dims `(valid_time [Central], lead_hours, latitude, longitude)` and variables: `temp_error`, `wspd_error`, `wdir_error`, `forecast_temp`, `era5_temp`, `forecast_wspd`, `era5_wspd`, `forecast_wdir`, `era5_wdir`
- `error_summary.csv` — per-ERA5-cell, per-lead MAE and bias

**Key implementation details**:
- `build_era5_grid_gdf(era5_ds)` — ERA5's regular 1D lat/lon grid → GeoDataFrame (analogous to `build_forecast_grid_gdf` for 2D Lambert grids)
- `load_era5_as_obs_dict(era5_nc_path)` — loads ERA5 arrays into the same `{cell_id: DataFrame}` format used by the station pipeline; converts K→°C; skips ocean-masked cells
- `_compute_era5_gridded_errors()` — accumulates errors into pre-allocated NumPy arrays and saves as NetCDF (CSV would be ~20M rows for a single month)

---

## Step 6: Renewable Curtailment Data (TODO)

ERCOT publishes 60-Day SCED Disclosure with individual unit output and HSL. Curtailment = HSL - actual output for renewables.
- Source: https://www.ercot.com/mp/data-products/data-product-details?id=NP3-966-ER

---

## Execution Order

```bash
# Step 0: Already done
uv sync  # installs all dependencies including cdsapi

# Step 1a: NDFD forecasts (~30-60 min per element)
uv run python -c "
from download_data.pull_ndfd import download_and_extract_texas_month
from helper_funcs import setup_directories
import os
dirs = setup_directories()
base_dir = os.path.join(dirs['raw'], 'ndfd_data')
for element in ['temp', 'wspd', 'wdir']:
    download_and_extract_texas_month(element, year=2025, month=7, base_dir=base_dir)
"

# Step 1b: HRRR forecasts (~2-3 hours for a full month)
uv run python -m download_data.pull_hrrr  # interactive prompts for year/month

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

# Step 5a: Station-level forecast errors (~2 min per model, requires Steps 1+2)
uv run python -c "
from process_data.calculate_forecast_errors import calculate_hrrr_errors_for_month
calculate_hrrr_errors_for_month(2025, 7)
"

# Step 5b: ERA5 gridded forecast errors (~10-20 min per model, requires Steps 1+1c)
uv run python -c "
from process_data.calculate_forecast_errors import calculate_era5_errors_for_month
calculate_era5_errors_for_month(2025, 7, model='hrrr')
"

# Validate
uv run python -m download_data.validate_data

# Generate plots
uv run python create_plots.py
```

---

## Processing & Visualization Scripts

### `process_data/calculate_forecast_errors.py`
Computes forecast errors (NDFD or HRRR) against either ISD weather stations or ERA5-Land reanalysis. All output `valid_time` values are **US/Central (tz-naive)**.

**Timezone utility**:
- `_to_central(timestamps)` — converts UTC timestamps (tz-naive or tz-aware) to US/Central tz-naive; handles DST automatically

**Station-level entry points** (ISD observations as ground truth):
- `calculate_ndfd_errors_for_month(year, month)` — NDFD vs ISD stations; output: `{processed}/forecast_errors/ndfd/{year}/{month:02d}/`
- `calculate_hrrr_errors_for_month(year, month)` — HRRR vs ISD stations; output: `{processed}/forecast_errors/hrrr/{year}/{month:02d}/`

**ERA5 gridded entry point** (ERA5-Land reanalysis as ground truth):
- `calculate_era5_errors_for_month(year, month, model='hrrr')` — NDFD or HRRR vs ERA5; output: `{processed}/forecast_errors_era5/{model}/{year}/{month:02d}/era5_errors_{YYYYMM}.nc`

**Shared helpers**:
- `load_stations_gdf(raw_dir)` — station metadata as GeoDataFrame
- `load_all_observations(stations_gdf, year, month, raw_dir)` — ISD data resampled to hourly; times converted to US/Central
- `build_forecast_grid_gdf(sample_nc_path)` — GeoDataFrame of any 2D lat/lon forecast grid (NDFD, HRRR)
- `build_era5_grid_gdf(era5_ds)` — GeoDataFrame of ERA5's regular 1D lat/lon grid
- `spatial_join_stations_to_grid(stations_gdf, grid_gdf)` — `sjoin_nearest` (EPSG:3857) to match any observation GDF to nearest forecast grid cell
- `load_forecasts(element_dir, variable_name, year, month)` — loads NetCDF files; converts `valid_time` to US/Central
- `load_era5_as_obs_dict(era5_nc_path)` — ERA5 NetCDF → `{cell_id: DataFrame}` format compatible with the station pipeline

### `download_data/pull_era5.py`
Downloads ERA5-Land hourly reanalysis from the Copernicus CDS API:
- `download_era5_month(year, month, base_dir, force_rebuild=False)` — downloads one month, adds derived `wspd`/`wdir`, saves compressed NetCDF
- `download_era5_months(months, base_dir, force_rebuild=False)` — loops over a list of `(year, month)` tuples
- CLI: `uv run python -m download_data.pull_era5 --year 2025 --month 7`

### `process_data/process_ercot.py`
Functions for reading and processing ERCOT market data:
- `load_dam_spp_month(year, month)` — loads all daily DAM SPP CSVs into one DataFrame
- `load_rt_spp_month(year, month)` — loads all daily RT SPP CSVs into one DataFrame
- `compute_max_lmp_by_node(year, month, point_types='RN')` — max LMP per settlement point; accepts a string, list of strings, or None (all types)
- `build_node_coordinates(force_rebuild=False)` — builds the name-matching pipeline (Step 4c), caches to `{processed}/node_coordinates.csv`

### `download_data/validate_data.py`
Functions for validating downloaded data:
- `validate_data(year, month)` — checks file completeness for NDFD, HRRR, weather stations, DAM SPP, RT SPP
- `validate_settlement_point_coverage(year, month)` — reports settlement point type distribution in RT SPP, RN coordinate coverage, and match method breakdown
- `validate_node_coordinate_matching()` — detailed matching pipeline validation with map visualization

### `create_plots.py`
Visualization functions using cartopy for Texas maps:
- `plot_max_temperature_map()` — weather stations colored by max temperature
- `plot_max_wind_speed_map()` — weather stations colored by max wind speed
- `plot_combined_map()` — 3-panel figure: max temp, max wind speed, max LMP
- `map_station_values()` — reusable scatter-map plotter
- `compute_station_stat()` — generic per-station statistic from ISD data (supports any column/parser/stat_func)

Parsers in `create_plots.py`:
- `parse_tmp(tmp_str)` — ISD TMP field → °C
- `parse_wnd_speed(wnd_str)` — ISD WND field → m/s

---

## Troubleshooting

### ERCOT API returns 401
The API requires BOTH a Bearer token (from OAuth) AND a subscription key. If the OAuth token request fails:
1. Verify `~/keys/ercot_user.txt` is your ERCOT username (6 chars)
2. Verify `~/keys/ercot_pwd.txt` is current
3. Check your account at https://apiexplorer.ercot.com/

### NCEI API timeouts
The NCEI API can be slow (30+ seconds per request). The script uses 120s timeout and 0.25s delay between requests. If it times out, just re-run — it skips already-downloaded files.

### NDFD: only ~248 files per element (not ~496)
This is expected. Of ~496 Group B GRIB files downloaded, only ~half contain the target lead times (1h and 25h). The rest are skipped.

### ERA5-Land: CDS API errors
- **401 / invalid key**: Verify `~/.cdsapirc` uses the new-style URL (`url: https://cds.climate.copernicus.eu/api`) and a current API key from your CDS profile page.
- **`data_format` key not accepted**: Requires `cdsapi>=0.7.0`. Run `uv sync` to ensure the right version is installed.
- **Request queued for a long time**: ERA5-Land requests are queued server-side; a full month (~50-100 MB) typically takes 5–20 minutes depending on CDS load. The script will wait until the download completes.
- **ERA5 errors NetCDF missing**: If `calculate_era5_errors_for_month()` raises `FileNotFoundError`, the ERA5 raw file is missing — run Step 1c first.

### Stale UTC forecast error CSVs
After the timezone change to US/Central, any existing per-station CSVs in `processed_data/forecast_errors/` contain UTC `valid_time` values and are stale. Delete the relevant subdirectory and re-run `calculate_hrrr_errors_for_month()` or `calculate_ndfd_errors_for_month()` to regenerate with correct Central times.
