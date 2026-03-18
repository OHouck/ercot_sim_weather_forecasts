"""pull_ifs.py — Download ECMWF IFS HRES 12z forecasts for Texas via MARS WebAPI.

Uses the ECMWF MARS WebAPI (ecmwf-api-client) to download the operational IFS HRES
model at 0.1°×0.1° resolution. The 12z UTC initialization cycle is the latest forecast
available before ERCOT's day-ahead market closes at 10:00 AM Central (~15:00–16:00 UTC).

Lead times 17–41h from the 12z init cover the full next delivery day:
  Step +17h = 05:00 UTC = 00:00 CDT  (HE01 start)
  Step +18h = 06:00 UTC = 01:00 CDT  (HE01 end)
  ...
  Step +40h = 04:00 UTC = 23:00 CDT  (HE24 end)
  Step +41h = 05:00 UTC = 00:00 CDT  (HE24/next day boundary)

Downloaded variables:
  - param 167 (2t)  → t2m  [Kelvin]
  - param 165 (10u) → u10  [m/s, eastward]
  - param 166 (10v) → v10  [m/s, northward]

Derived variables added after download:
  - wspd: wind speed = sqrt(u10² + v10²)  [m/s]
  - wdir: meteorological wind direction (direction FROM which wind blows) [degrees 0–360]
          matches NDFD wdir10 / HRRR wdir10 / ERA5 wdir convention

Output (one file per element per day):
  {raw}/ifs_data/temp/{year}/{month:02d}/ifs_12z_{YYYYMMDD}.nc  → variable: t2m
  {raw}/ifs_data/wspd/{year}/{month:02d}/ifs_12z_{YYYYMMDD}.nc  → variable: wspd
  {raw}/ifs_data/wdir/{year}/{month:02d}/ifs_12z_{YYYYMMDD}.nc  → variable: wdir

Each NetCDF has dims (step=25, latitude, longitude) with coordinates:
  time       — init time (12:00 UTC, scalar)
  step       — forecast step as timedelta64
  valid_time — UTC valid time for each step (1D array of 25 datetimes)
  latitude   — 1D, descending (0.1° spacing)
  longitude  — 1D, ascending, -180 to 180 convention (0.1° spacing)

Times are stored in UTC. Downstream conversion to US/Central is applied in
calculate_forecast_errors.py (same as HRRR, NDFD, ERA5).

Prerequisites (one-time setup):
  1. Register at https://api.ecmwf.int → accept "Operational archive" licence
  2. Get your API key from My Profile → API Key
  3. Create ~/.ecmwfapirc:
       {
           "url"   : "https://api.ecmwf.int/v1",
           "key"   : "YOUR_32_CHAR_KEY_HERE",
           "email" : "your@email.edu"
       }
  4. chmod 600 ~/.ecmwfapirc
  Note: Different from ~/.cdsapirc (that is for Copernicus CDS / ERA5 only)

Usage:
    # Programmatic
    from download_data.pull_ifs import download_ifs_month
    from helper_funcs import setup_directories
    dirs = setup_directories()
    download_ifs_month(2025, 7, base_dir=dirs['raw'])

    # CLI
    uv run python -m download_data.pull_ifs --year 2025 --month 7
"""

import os
import sys
import calendar
import argparse
import tempfile
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories

# ── Texas bounding box (mirrors NDFD / HRRR / ERA5 bounds) ───────────────────
TEXAS_LAT_MIN = 25.8
TEXAS_LAT_MAX = 36.5
TEXAS_LON_MIN = -106.6
TEXAS_LON_MAX = -93.5

# MARS area string: "N/W/S/E"
MARS_AREA = f"{TEXAS_LAT_MAX}/{TEXAS_LON_MIN}/{TEXAS_LAT_MIN}/{TEXAS_LON_MAX}"

# IFS HRES lead times: hourly from +17h to +41h inclusive (25 steps)
# 12z init + 17h = 05:00 UTC = 00:00 CDT (midnight start of delivery day)
# 12z init + 41h = 05:00 UTC D+2 = 00:00 CDT D+2 (end of delivery day)
IFS_STEPS = list(range(17, 42))  # [17, 18, 19, ..., 41]
MARS_STEPS = "/".join(str(s) for s in IFS_STEPS)

# ECMWF GRIB parameter codes for surface variables
# 167 = 2m temperature (t2m), 165 = 10m U-wind (u10), 166 = 10m V-wind (v10)
MARS_PARAMS = "167/165/166"

# NetCDF compression (mirrors other downloaders)
COMPRESS_OPTS = {'zlib': True, 'complevel': 5}

# Element-to-variable mapping for output files
ELEMENTS = {
    'temp': 't2m',
    'wspd': 'wspd',
    'wdir': 'wdir',
}


def _all_element_files_exist(out_dirs: dict, date_str: str) -> bool:
    """Return True if all 3 element NetCDF files already exist on disk."""
    for element, out_dir in out_dirs.items():
        path = os.path.join(out_dir, f'ifs_12z_{date_str}.nc')
        if not os.path.exists(path):
            return False
    return True


def _build_output_dirs(base_dir: str, year: int, month: int) -> dict:
    """Build and create the per-element output directories.

    Returns a dict: {'temp': path, 'wspd': path, 'wdir': path}
    """
    out_dirs = {}
    for element in ELEMENTS:
        d = os.path.join(base_dir, 'ifs_data', element, str(year), f'{month:02d}')
        os.makedirs(d, exist_ok=True)
        out_dirs[element] = d
    return out_dirs


def _process_ifs_nc(nc_path: str, out_dirs: dict, date_str: str) -> None:
    """Process a raw MARS NetCDF file and save per-element output NetCDFs.

    MARS NetCDF output for surface forecast fields with multiple steps typically has:
      - 'time' dimension containing valid times (one per step)
      - Variables: t2m [K], u10 [m/s], v10 [m/s]
      - Latitude (descending), Longitude (may be 0–360)

    This function normalizes the structure to:
      - Dims: (step, latitude, longitude)
      - Coords: time (init, scalar), step (timedelta), valid_time (UTC datetime)
      - Longitude: -180 to 180 convention

    Then derives wspd/wdir and saves 3 per-element compressed NetCDFs.
    """
    ds = xr.open_dataset(nc_path)

    # ── Determine dimension and coordinate structure ──────────────────────────
    # MARS NetCDF for forecast data: 'time' dim contains valid datetimes.
    # The init time is known: date_str at 12:00 UTC.
    init_dt = pd.Timestamp(f"{date_str} 12:00:00", tz="UTC")

    if 'step' in ds.dims:
        # Structure A: step is already a dim (less common for MARS NetCDF)
        valid_times = np.array([
            init_dt + pd.Timedelta(s, unit='h') for s in IFS_STEPS
        ], dtype='datetime64[ns]')
        step_dim = 'step'
    elif 'time' in ds.dims and ds.dims['time'] > 1:
        # Structure B: 'time' dim contains valid times (most common MARS NetCDF)
        valid_times = pd.DatetimeIndex(ds['time'].values)
        # Rename 'time' → 'step' so we can add the right coords
        ds = ds.rename_dims({'time': 'step'})
        ds = ds.drop_vars(['time'], errors='ignore')
        step_dim = 'step'
    else:
        raise ValueError(
            f"Unexpected MARS NetCDF structure. Dims: {dict(ds.dims)}, "
            f"Coords: {list(ds.coords)}"
        )

    # Build step values (integer hours from init)
    step_values = np.array(IFS_STEPS, dtype=np.int32)

    # ── Normalize longitude to -180/180 ───────────────────────────────────────
    lon_var = 'longitude' if 'longitude' in ds.coords else 'lon'
    if float(ds[lon_var].max()) > 180.0:
        ds[lon_var] = ds[lon_var].where(ds[lon_var] <= 180.0,
                                         ds[lon_var] - 360.0)
        ds = ds.sortby(lon_var)

    # ── Extract arrays ────────────────────────────────────────────────────────
    lat_var = 'latitude' if 'latitude' in ds.coords else 'lat'
    lat = ds[lat_var].values
    lon = ds[lon_var].values

    # Variable names from MARS NetCDF (CF convention)
    t2m = ds['t2m'].values   # shape (step, lat, lon), Kelvin
    u10 = ds['u10'].values   # shape (step, lat, lon), m/s
    v10 = ds['v10'].values   # shape (step, lat, lon), m/s
    ds.close()

    # ── Derived wind variables ────────────────────────────────────────────────
    wspd = np.sqrt(u10 ** 2 + v10 ** 2)
    wdir = (np.degrees(np.arctan2(-u10, -v10)) % 360.0)

    # ── Build common coordinates ──────────────────────────────────────────────
    coords = {
        'time':       init_dt.tz_localize(None),   # scalar, UTC (tz-naive for NetCDF)
        'step':       ('step', [pd.Timedelta(hours=s) for s in step_values]),
        'valid_time': ('step', [pd.Timestamp(vt).tz_localize(None).to_numpy()
                                for vt in valid_times]),
        'latitude':   (lat_var, lat),
        'longitude':  (lon_var, lon),
    }

    dims = ['step', lat_var, lon_var]

    # ── Save per-element NetCDFs ──────────────────────────────────────────────
    element_data = {
        'temp': ('t2m', t2m, 'K', '2 metre temperature'),
        'wspd': ('wspd', wspd, 'm s**-1', 'Wind speed at 10m'),
        'wdir': ('wdir', wdir, 'degrees',
                 'Wind direction at 10m (meteorological, FROM which wind blows)'),
    }

    for element, (var_name, data, units, long_name) in element_data.items():
        out_path = os.path.join(out_dirs[element], f'ifs_12z_{date_str}.nc')
        da = xr.DataArray(data, dims=dims, coords=coords, name=var_name)
        da.attrs = {'long_name': long_name, 'units': units}
        out_ds = da.to_dataset()
        out_ds.attrs = {
            'source': 'ECMWF IFS HRES, MARS WebAPI',
            'init_time': f'{date_str} 12:00 UTC',
            'lead_hours': f'{IFS_STEPS[0]}–{IFS_STEPS[-1]}',
            'resolution': '0.1 degree',
        }
        encoding = {var_name: COMPRESS_OPTS.copy()}
        out_ds.to_netcdf(out_path, encoding=encoding)


def download_ifs_day(
    target_date: date,
    base_dir: str,
    force_rebuild: bool = False,
) -> bool:
    """Download IFS HRES 12z forecast for a single date.

    Issues one MARS request for the target date, steps 17–41h, at 0.1° resolution
    over the Texas bounding box. Processes the raw NetCDF into 3 per-element files.

    Args:
        target_date: The forecast initialization date (12z of this date).
        base_dir: Root raw data directory (dirs['raw'] from setup_directories()).
        force_rebuild: If True, re-download even if all output files already exist.

    Returns:
        True if data was downloaded, False if skipped (files already existed).
    """
    date_str = target_date.strftime('%Y%m%d')
    year, month = target_date.year, target_date.month

    out_dirs = _build_output_dirs(base_dir, year, month)

    if not force_rebuild and _all_element_files_exist(out_dirs, date_str):
        print(f'  [{date_str}] All 3 element files already exist — skipping.')
        return False

    # Temp file in same directory as output (avoids cross-device rename issues)
    tmp_dir = out_dirs['temp']
    tmp_path = os.path.join(tmp_dir, f'_tmp_ifs_{date_str}.nc')

    try:
        from ecmwf.api import ECMWFDataServer

        server = ECMWFDataServer()

        request = {
            'class':   'od',          # operational data
            'stream':  'oper',        # high-resolution deterministic
            'expver':  '1',
            'type':    'fc',          # forecast
            'levtype': 'sfc',         # surface
            'date':    date_str,
            'time':    '1200',        # 12z init cycle
            'step':    MARS_STEPS,    # "17/18/19/.../41"
            'param':   MARS_PARAMS,   # 2t / 10u / 10v
            'area':    MARS_AREA,     # "36.5/-106.6/25.8/-93.5" (N/W/S/E)
            'grid':    '0.1/0.1',     # 0.1° resolution
            'format':  'netcdf',
            'target':  tmp_path,
        }

        print(f'  [{date_str}] Submitting MARS request (may queue for several minutes)...')
        server.retrieve(request)

        raw_mb = os.path.getsize(tmp_path) / 1e6
        print(f'  [{date_str}] Download complete ({raw_mb:.1f} MB). Processing...')

        _process_ifs_nc(tmp_path, out_dirs, date_str)
        print(f'  [{date_str}] Saved 3 element NetCDFs.')

    except Exception as exc:
        print(f'  [{date_str}] ERROR: {exc}')
        raise
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

    return True


def download_ifs_month(
    year: int,
    month: int,
    base_dir: str = None,
    force_rebuild: bool = False,
) -> dict:
    """Download IFS HRES 12z forecasts for all days in a month.

    Issues one MARS request per day. MARS requests are server-side queued;
    expect ~2–10 minutes per day, ~1–5 hours for a full month.

    Args:
        year: Four-digit year (e.g. 2025).
        month: Month number 1–12.
        base_dir: Root raw data directory. Uses setup_directories()['raw'] if None.
        force_rebuild: Re-download even if output files already exist.

    Returns:
        Dict mapping date → {'downloaded': bool, 'error': str or None}.
    """
    if base_dir is None:
        dirs = setup_directories()
        base_dir = dirs['raw']

    _, n_days = calendar.monthrange(year, month)
    all_dates = [date(year, month, d) for d in range(1, n_days + 1)]

    print(f'\n{"=" * 60}')
    print(f'IFS HRES download: {year}-{month:02d} ({n_days} days)')
    print(f'  Init cycle:  12z UTC')
    print(f'  Lead times:  {IFS_STEPS[0]}–{IFS_STEPS[-1]}h (hourly, {len(IFS_STEPS)} steps)')
    print(f'  Resolution:  0.1° × 0.1°')
    print(f'  Texas bbox:  lat [{TEXAS_LAT_MIN}, {TEXAS_LAT_MAX}], '
          f'lon [{TEXAS_LON_MIN}, {TEXAS_LON_MAX}]')
    print(f'  Output root: ...ifs_data/{{element}}/{year}/{month:02d}/')
    print('=' * 60)

    results = {}
    n_downloaded = n_skipped = n_failed = 0

    for target_date in all_dates:
        date_str = target_date.strftime('%Y%m%d')
        try:
            downloaded = download_ifs_day(target_date, base_dir,
                                           force_rebuild=force_rebuild)
            results[date_str] = {'downloaded': downloaded, 'error': None}
            if downloaded:
                n_downloaded += 1
            else:
                n_skipped += 1
        except Exception as exc:
            results[date_str] = {'downloaded': False, 'error': str(exc)}
            n_failed += 1

    print(f'\nDone: {n_downloaded} downloaded, {n_skipped} skipped, {n_failed} failed.')
    if n_failed > 0:
        failed_dates = [d for d, r in results.items() if r['error']]
        print(f'Failed dates: {", ".join(failed_dates)}')

    return results


# ── CLI ───────────────────────────────────────────────────────────────────────

def _build_parser():
    p = argparse.ArgumentParser(
        description='Download ECMWF IFS HRES 12z forecasts for Texas via MARS WebAPI.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument('--year', type=int, required=True, help='Four-digit year')
    p.add_argument('--month', type=int, required=True, help='Month number (1–12)')
    p.add_argument(
        '--force-rebuild', action='store_true',
        help='Re-download even if output files already exist',
    )
    return p


def main():
    args = _build_parser().parse_args()
    dirs = setup_directories()
    download_ifs_month(
        year=args.year,
        month=args.month,
        base_dir=dirs['raw'],
        force_rebuild=args.force_rebuild,
    )


if __name__ == '__main__':
    main()
