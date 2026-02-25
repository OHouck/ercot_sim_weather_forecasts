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

We download **only Group B files** because they contain both target lead times:
- **1-hour lead time**: direct short-range forecast
- **25-hour lead time**: closest available to 24h (~1 day ahead)

This skips Group A issuance hours (00, 03, 06, 09, 12, 15, 18, 21 UTC), reducing downloads by ~1/3.

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

### Local Output Structure (Texas Extraction)

After running `pull_ndfd.py`, data is saved as:

```
{data_dir}/ndfd_data/
├── temp/
│   ├── 2025/
│   │   ├── 01/
│   │   │   ├── YEUZ88_KWBN_202501010147_texas.nc
│   │   │   ├── YEUZ88_KWBN_202501010247_texas.nc
│   │   │   └── ...
│   │   ├── 02/
│   │   └── ...
├── wspd/
├── wdir/
└── ...
```

## NetCDF File Structure

Each extracted `*_texas.nc` file contains:

### Dimensions
| Dimension | Description |
|-----------|-------------|
| `step` | Forecast lead time steps (2 steps: 1h and 25h) |
| `y` | Grid y-coordinate (~490 points for Texas) |
| `x` | Grid x-coordinate (~516 points for Texas) |

### Coordinates
| Coordinate | Type | Description |
|------------|------|-------------|
| `time` | datetime64 | Forecast initialization time (UTC) |
| `step` | timedelta64 | Lead time from initialization (1h, 25h) |
| `valid_time` | datetime64 | Valid time for each forecast step |
| `latitude` | float64 (y, x) | 2D latitude array (Lambert Conformal) |
| `longitude` | float64 (y, x) | 2D longitude array (converted to -180 to 180) |
| `heightAboveGround` | float64 | Height of measurement (2m for temperature) |

### Data Variables
| Variable | Units | Description |
|----------|-------|-------------|
| `t2m` | Kelvin | 2m temperature forecast (for temp element) |

### Example: Reading the Data

```python
import xarray as xr

# Open a single file
ds = xr.open_dataset("ndfd_data/temp/2025/01/YEUZ88_KWBN_202501010147_texas.nc")

# Access temperature data (in Kelvin)
temp_kelvin = ds.t2m.values  # shape: (2, ~490, ~516) for 2 lead times

# Convert to Fahrenheit
temp_fahrenheit = (ds.t2m - 273.15) * 9/5 + 32

# Get lead times
print(f"Lead times: {ds.step.values}")  # [1h, 25h]

# Get coordinates
lat = ds.latitude.values  # 2D array
lon = ds.longitude.values  # 2D array

# Get forecast times
print(f"Forecast issued: {ds.time.values}")
print(f"Valid times: {ds.valid_time.values}")

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

### Download Full Year of Data

```python
from download_ndfd.pull_ndfd import download_year_data

# Download temp, wspd, wdir for 2025, extracting only Texas
download_year_data(
    year=2025,
    elements=['temp', 'wspd', 'wdir'],
    base_dir='/path/to/output',
    texas_only=True
)
```

### Download Single Month

```python
from download_ndfd.pull_ndfd import download_and_extract_texas_month

download_and_extract_texas_month(
    element='temp',
    year=2025,
    month=1,
    base_dir='/path/to/output'
)
```

### Plot Forecast Map

```python
from download_ndfd.pull_ndfd import plot_texas_temp_forecast

# Plot in Fahrenheit
plot_texas_temp_forecast(
    nc_file='path/to/texas_data.nc',
    step_index=0,  # 1h lead time
    output_file='forecast_map.png',
    units='F'
)
```

## Data Availability

- **Start**: ~2020 (varies by element)
- **End**: Current (updated continuously)
- **Download frequency**: ~16 Group B issuances per day
- **Lead times extracted**: 1h, 25h
- **Forecast range in files**: Up to 72h (Days 1-3), but only 1h and 25h steps are kept

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

5. **Lead time tolerance**: When matching target lead times, a 30-minute tolerance is used. Steps within 30 minutes of 1h or 25h are accepted.
