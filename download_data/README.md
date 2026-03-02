# NDFD Data Download and Processing

This module downloads National Digital Forecast Database (NDFD) forecast data from NOAA's public S3 bucket and extracts regional subsets (currently Texas).

## Data Source

- **S3 Bucket**: `s3://noaa-ndfd-pds/wmo/`
- **Access**: Public, no authentication required (`--no-sign-request`)
- **Provider**: NOAA/National Weather Service

## Forecast Structure

### How NDFD Forecasts Work

NDFD is **not a traditional NWP model** with fixed initialization cycles. It is a continuously updated mosaic of forecasts from NWS Weather Forecast Offices. Forecasts are issued throughout the day, with each new file replacing the previous one.

### Products and Issuance Frequency

NDFD files on S3 are organized by WMO product codes:

| Product | Grid | Forecast Range | Temporal Resolution | Issuance Frequency |
|---------|------|----------------|---------------------|--------------------|
| Z88 | CONUS 2.5km | Days 1-3 (up to 72h) | 3-hourly | ~24 files/day |
| Z87 | CONUS 5km | Days 4-7 | 6-hourly | ~5 files/day |
| Z98 | CONUS combined | Days 1-3 | ~30 min updates | ~48 files/day |
| Z97 | Extended range | Days 4-7 | 6-hourly | ~5 files/day |

**We download only Z88 (CONUS 2.5km, Days 1-3)** for manageable file sizes and hourly issuance.

### Z88 Step Grid: Group A vs Group B

The Z88 files are all 3-hourly forecasts but are issued on a staggered schedule with **two offset step grids**:

| Group | Issuance Hours (UTC) | Forecast Steps (hours) | Has 1h? | Has 25h? |
|-------|---------------------|------------------------|---------|----------|
| **A** | 00, 03, 06, 09, 12, 15, 18, 21 | 2, 5, 8, 11, 14, 17, 20, 23, 26, 29, ..., 71 | No | No |
| **B** | 01, 02, 04, 05, 07, 08, 10, 11, 13, 14, 16, 17, 19, 20, 22, 23 | 1, 4, 7, 10, 13, 16, 19, 22, 25, 28, ..., 70 | **Yes** | **Yes** |

Both groups produce forecasts valid at the same 3-hourly synoptic times (00, 03, 06, 09, 12, 15, 18, 21 UTC) -- they are just offset by their different initialization times.

**Why no exact 24h step?** The step grids start at 1h or 2h (not 0h) and increment by 3h. Since 24 = 1 + 3*n has no integer solution (23/3 is not an integer), neither grid lands on exactly 24h. The closest are:
- Group A: **23h**
- Group B: **25h**

### Current Download Configuration

We download **one 12Z Group A file per day** and keep all 3-hourly steps up to 48h lead time. This gives ~16 steps per file (2, 5, 8, 11, 14, 17, 20, 23, 26, 29, 32, 35, 38, 41, 44, 47h).

For a given valid_time, two 12Z initializations may cover it:
- **Same-day** (lead < 24h): the 12Z init from the same calendar day
- **Day-ahead** (lead >= 24h): the 12Z init from the previous day

This enables comparing same-day vs day-ahead forecast errors at each valid time.

#### Legacy configuration
The legacy approach downloaded all Group B files (16 per day) keeping only 1h and 25h lead times. Use `download_and_extract_texas_month()` for the legacy approach.

### Elements Downloaded

| Element | Variable | Description | Units (raw) |
|---------|----------|-------------|-------------|
| `temp` | `t2m` | 2m temperature (instantaneous) | Kelvin |
| `wspd` | - | 10m wind speed | m/s |
| `wdir` | - | 10m wind direction | degrees |

Note: `maxt`/`mint` (daily max/min temperature) are not downloaded because they only have 24h step intervals and don't support hourly lead times.

## File Structure

### S3 Source Structure

```
s3://noaa-ndfd-pds/wmo/
├── temp/                          # Temperature
│   ├── 2025/
│   │   ├── 01/
│   │   │   ├── 01/               # Day of month
│   │   │   │   ├── YEUZ88_KWBN_202501010147
│   │   │   │   ├── YEUZ88_KWBN_202501010247
│   │   │   │   └── ...
│   │   │   ├── 02/
│   │   │   └── ...
│   │   ├── 02/
│   │   └── ...
├── wspd/                          # Wind speed
├── wdir/                          # Wind direction
└── ...                            # Other elements (not downloaded)
```

### WMO File Naming Convention

```
YEUZ88_KWBN_YYYYMMDDHHMM
│││ │   │     └── Issuance timestamp (UTC)
│││ │   └── Originating center (KWBN = NWS MDL)
│││ └── Product code (88 = CONUS 2.5km Days 1-3)
││└── Geographic region (U = CONUS)
│└── Element code (E=temp, C=wspd, B=wdir, G=maxt, H=mint)
└── WMO bulletin designator (Y = GRIB2)
```

**Element codes (2nd character):**
| Code | Element |
|------|---------|
| E | temp |
| G | maxt |
| H | mint |
| C | wspd |
| B | wdir |

**Region codes (3rd character):**
| Code | Region |
|------|--------|
| U | CONUS (Continental US) -- **this is what we download** |
| A | Alaska |
| R | Pacific regional |
| S | Hawaii |
| T | Guam / Pacific Islands |
| Y | Oceanic (wind only) |

### Local Output Structure (12Z Texas Extraction)

After running `pull_ndfd.py`, data is saved as one file per day:

```
{data_dir}/ndfd_data/
├── temp/
│   ├── 2025/
│   │   ├── 07/
│   │   │   ├── ndfd_12z_20250701.nc
│   │   │   ├── ndfd_12z_20250702.nc
│   │   │   └── ...  (31 files for July)
│   │   └── ...
├── wspd/
├── wdir/
└── ...
```

## NetCDF File Structure

Each extracted `ndfd_12z_*.nc` file contains:

### Dimensions
| Dimension | Description |
|-----------|-------------|
| `step` | Forecast lead time steps (~16 steps: 2h, 5h, 8h, ..., 47h) |
| `y` | Grid y-coordinate (~490 points for Texas) |
| `x` | Grid x-coordinate (~516 points for Texas) |

### Coordinates
| Coordinate | Type | Description |
|------------|------|-------------|
| `time` | datetime64 | Forecast initialization time (12Z UTC) |
| `step` | timedelta64 | Lead time from initialization (2h to 47h, 3-hourly) |
| `valid_time` | datetime64 | Valid time for each forecast step = time + step |
| `latitude` | float64 (y, x) | 2D latitude array (Lambert Conformal) |
| `longitude` | float64 (y, x) | 2D longitude array (converted to -180 to 180) |
| `heightAboveGround` | float64 | Height of measurement (2m for temperature) |

### Data Variables
| Variable | Units | Description |
|----------|-------|-------------|
| `t2m` | Kelvin | 2m temperature forecast (for temp element) |
| `si10` | m/s | 10m wind speed forecast (for wspd element) |
| `wdir10` | degrees | 10m wind direction forecast (for wdir element) |

### Example: Reading the Data

```python
import xarray as xr

# Open a single 12Z file
ds = xr.open_dataset("ndfd_data/temp/2025/07/ndfd_12z_20250715.nc")

# Access temperature data (in Kelvin)
temp_kelvin = ds.t2m.values  # shape: (~16, ~490, ~516)

# Get lead times (3-hourly: 2, 5, 8, ..., 47h)
print(f"Lead times: {ds.step.values}")

# Compute valid times
init_time = ds.time.values
valid_times = init_time + ds.step.values
print(f"Init: {init_time}, Valid: {valid_times}")

ds.close()
```

## Grid Projection

NDFD CONUS data uses a **Lambert Conformal Conic** projection, which means:

1. Latitude and longitude are 2D arrays (not 1D)
2. Grid cells are not rectangular in lat/lon space
3. Use `pcolormesh` (not `imshow`) for plotting
4. Cannot use simple `.sel(latitude=slice(...))` for subsetting

The extraction code handles this by:
1. Creating a boolean mask for the Texas region
2. Finding the bounding box of the mask
3. Using `.isel()` with index slices to extract the region

## Texas Bounds

The extraction uses these geographic bounds:
- **Latitude**: 25.8N to 36.5N
- **Longitude**: -106.6W to -93.5W

## Usage

### Download 12Z Forecasts for a Single Month

```python
from download_data.pull_ndfd import download_12z_forecasts_month
from helper_funcs import setup_directories
import os

dirs = setup_directories()
base_dir = os.path.join(dirs['raw'], 'ndfd_data')
for element in ['temp', 'wspd', 'wdir']:
    download_12z_forecasts_month(element, 2025, 7, base_dir)
```

### Download Full Year of 12Z Data

```python
from download_data.pull_ndfd import download_year_data

download_year_data(
    year=2025,
    elements=['temp', 'wspd', 'wdir'],
    base_dir='/path/to/output',
    init_12z=True  # default
)
```

### Legacy: Download All Group B Issuances

```python
from download_data.pull_ndfd import download_and_extract_texas_month

download_and_extract_texas_month(
    element='temp',
    year=2025,
    month=1,
    base_dir='/path/to/output'
)
```

## Data Availability

- **Start**: ~2020 (varies by element)
- **End**: Current (updated continuously)
- **Download frequency**: 1 file per day (12Z initialization)
- **Lead times extracted**: All 3-hourly steps from 2h to 47h (~16 steps)
- **Forecast range in files**: Up to 72h (Days 1-3), but only steps up to 48h are kept

## Dependencies

- `xarray`: Data handling
- `cfgrib`: GRIB2 file reading
- `eccodes`: ECMWF GRIB library (install via `brew install eccodes` on macOS)
- `matplotlib`: Plotting
- `numpy`: Array operations
- `awscli`: S3 data download

## Notes

1. **File sizes**: Raw CONUS GRIB files are ~6MB each. Texas NetCDF extracts are much smaller due to spatial subsetting and lead time filtering.

2. **Non-CONUS regions**: Only CONUS files (region code `U`) are downloaded. Alaska, Hawaii, Puerto Rico, etc. are filtered out at the download stage.

3. **Coordinate conversion**: Longitude is converted from 0-360 to -180-180 format during extraction.

4. **Temporary storage**: During download, full CONUS files are downloaded to a temp directory, Texas is extracted, then the temp files are deleted automatically.

5. **12Z file selection**: The download finds the file closest to 12:00 UTC within a 2-hour window (11:00-13:00). Exact issuance minutes vary slightly day to day.

6. **Same-day vs day-ahead**: A valid_time at e.g. 17 UTC can be reached by two 12Z forecasts: same-day (lead=5h) and day-ahead from the previous day (lead=29h). The downstream `calculate_forecast_errors.py` handles both via the `(valid_time, lead_hours)` index.
