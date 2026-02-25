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
│   ├── ndfd_data/              # Step 1: NDFD weather forecasts
│   │   ├── temp/2025/07/       # ~248 NetCDF files
│   │   ├── wspd/2025/07/       # ~248 NetCDF files
│   │   └── wdir/2025/07/       # ~248 NetCDF files
│   ├── weather_stations/       # Step 2: ISD realized observations
│   │   ├── stations.csv        # 205 Texas station metadata
│   │   └── 2025/07/            # ~202 per-station hourly CSVs
│   └── ercot/                  # Step 3: ERCOT market data
│       ├── dam_lmp/2025/07/    # 31 daily CSVs (~439K records each)
│       └── rt_spp/2025/07/     # 31 daily CSVs
└── processed_data/             # Future: cleaned/merged datasets
```

## Data Sources Summary

| Dataset | Source | Auth | Script |
|---------|--------|------|--------|
| NDFD forecasts | NOAA S3 `s3://noaa-ndfd-pds/wmo/` | No | `download_data/pull_ndfd.py` |
| Realized weather | NCEI ISD API | No | `download_data/pull_weatherstation.py` |
| Day-ahead LMP | ERCOT API | OAuth2 + subscription key | `download_data/pull_ercot.py` |
| Real-time SPP | ERCOT API | OAuth2 + subscription key | `download_data/pull_ercot.py` |
| Validation | All above | — | `download_data/validate_data.py` |

## Credentials
- `~/keys/ercot_api_key.txt` — ERCOT API subscription key (32 chars)
- `~/keys/ercot_api_secondary_key.txt` — backup subscription key
- `~/keys/ercot_user.txt` — ERCOT account username
- `~/keys/ercot_pwd.txt` — ERCOT account password

---

## Step 0: Project Setup (DONE)

Changes made:
- `helper_funcs.py`: Added `raw` and `processed` keys to `setup_directories()`
- `pyproject.toml`: Added `xarray`, `cfgrib`, `netcdf4` dependencies
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
| DAM LMP | `/np4-183-cd/dam_hourly_lmp` | deliveryDate, hourEnding, busName, LMP, DSTFlag | ~439K (18,290 buses × 24h) |
| RT SPP | `/np6-905-cd/spp_node_zone_hub` | deliveryDate, deliveryInterval, settlementPointName, settlementPointPrice, settlementPointType | varies |

**Rate limit**: 30 req/min. Use `time.sleep(2)` between requests.

**Pagination**: Check `_meta.totalPages`. Loop until `page >= totalPages`.

### Results for July 2025
- DAM LMP: 31 files, ~438,960 records/day (18,290 buses × 24 hours)
- Columns: deliveryDate, hourEnding, busName, LMP, DSTFlag
- RT SPP: 31 files

---

## Step 4: ERCOT Node-to-Coordinate Mapping (TODO)

For the synthetic 123-bus system, bus coordinates may be in `scuc/123bus_case_final.pkl`. For real ERCOT nodes, download from:
- ERCOT GIS data: https://www.ercot.com/mp/data-products/data-product-details?id=NP4-160-SG

---

## Step 5: Renewable Curtailment Data (TODO)

ERCOT publishes 60-Day SCED Disclosure with individual unit output and HSL. Curtailment = HSL - actual output for renewables.
- Source: https://www.ercot.com/mp/data-products/data-product-details?id=NP3-966-ER

---

## Execution Order

```bash
# Step 0: Already done
uv sync

# Step 1: NDFD forecasts (~30-60 min per element)
uv run python -c "
from download_data.pull_ndfd import download_and_extract_texas_month
from helper_funcs import setup_directories
import os
dirs = setup_directories()
base_dir = os.path.join(dirs['raw'], 'ndfd_data')
for element in ['temp', 'wspd', 'wdir']:
    download_and_extract_texas_month(element, year=2025, month=7, base_dir=base_dir)
"

# Step 2: Weather stations (~1 min)
uv run python -m download_data.pull_weatherstation

# Step 3: ERCOT market data (~30 min)
uv run python -m download_data.pull_ercot

# Validate
uv run python -m download_data.validate_data
```

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
