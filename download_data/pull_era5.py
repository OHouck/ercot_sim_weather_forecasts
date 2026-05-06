"""pull_era5.py — Download ERA5-Land hourly reanalysis and ERA5 100m wind for Texas.

Uses the Copernicus Climate Data Store (CDS) API. Requires ~/.cdsapirc with a
valid API key (https://cds.climate.copernicus.eu/api).

ERA5-Land (reanalysis-era5-land, 0.1° resolution):
  Downloaded variables: 2m_temperature, 10m_u/v_component_of_wind
  Derived: wspd [m/s], wdir [degrees]
  Output: {base_dir}/era5_land/{year}/{month:02d}/era5_land_{YYYYMM}.nc

ERA5 100m wind (reanalysis-era5-single-levels, 0.25° resolution):
  ERA5-Land does not include 100m wind; the full ERA5 dataset does.
  Downloaded variables: 100m_u/v_component_of_wind
  Derived: wspd100 [m/s], wdir100 [degrees]
  Output: {base_dir}/era5_land/{year}/{month:02d}/era5_wind100m_{YYYYMM}.nc

Times in output files are stored in UTC.
Timezone conversion to US/Central is applied downstream in calculate_forecast_errors.py.

Usage:
    # ERA5-Land (temperature + 10m wind)
    from download_data.pull_era5 import download_era5_month
    download_era5_month(2025, 7, base_dir=dirs['raw'])

    # ERA5 100m wind
    from download_data.pull_era5 import download_era5_wind100m_month
    download_era5_wind100m_month(2025, 7, base_dir=dirs['raw'])

    # CLI (downloads both)
    uv run python -m download_data.pull_era5 --year 2025 --month 7
"""

import os
import sys
import shutil
import argparse
import calendar
import tempfile
from pathlib import Path

import numpy as np
import xarray as xr

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories

# ── Texas bounding box (mirrors NDFD / HRRR bounds) ──────────────────────────
TEXAS_LAT_MIN = 25.8
TEXAS_LAT_MAX = 36.5
TEXAS_LON_MIN = -106.6
TEXAS_LON_MAX = -93.5

# CDS short names for ERA5-Land variables
ERA5_VARS = [
    '2m_temperature',
    '10m_u_component_of_wind',
    '10m_v_component_of_wind',
]

# NetCDF compression settings (mirrors NDFD download)
COMPRESS_OPTS = {'zlib': True, 'complevel': 5}


def _add_derived_wind(
    ds: xr.Dataset,
    u_var: str = 'u10',
    v_var: str = 'v10',
    wspd_name: str = 'wspd',
    wdir_name: str = 'wdir',
    height_label: str = '10m',
) -> xr.Dataset:
    """Add derived wind speed and direction variables to an ERA5 xarray Dataset.

    speed = sqrt(u² + v²)  [m/s]
    direction = meteorological convention (FROM which wind blows):
                atan2(-u, -v) * 180/π  (mod 360)

    Args:
        ds: ERA5 dataset containing u_var and v_var.
        u_var: Name of the eastward wind component in ds.
        v_var: Name of the northward wind component in ds.
        wspd_name: Output variable name for wind speed.
        wdir_name: Output variable name for wind direction.
        height_label: Height string used in attrs (e.g. '10m', '100m').

    Returns:
        ds with wspd_name and wdir_name added.
    """
    ds[wspd_name] = np.sqrt(ds[u_var] ** 2 + ds[v_var] ** 2)
    ds[wspd_name].attrs.update({
        'long_name': f'Wind speed at {height_label}',
        'units': 'm s**-1',
    })

    wdir_rad = np.arctan2(-ds[u_var], -ds[v_var])
    ds[wdir_name] = (wdir_rad * 180.0 / np.pi) % 360.0
    ds[wdir_name].attrs.update({
        'long_name': f'Wind direction at {height_label} (meteorological, FROM which wind blows)',
        'units': 'degrees',
    })

    return ds


def download_era5_month(
    year: int,
    month: int,
    base_dir: str,
    force_rebuild: bool = False,
) -> str:
    """Download ERA5-Land hourly data for Texas for a given month.

    Downloads 2m_temperature, 10m_u/v_wind from the CDS API, adds derived
    wind speed and direction variables, and saves as compressed NetCDF.
    Times in the output file are UTC.

    Args:
        year: Four-digit year (e.g. 2025).
        month: Month number 1–12.
        base_dir: Root raw data directory (dirs['raw'] from setup_directories()).
        force_rebuild: If True, re-download even if the output file already exists.

    Returns:
        Absolute path to the output NetCDF file.
    """
    out_dir = os.path.join(base_dir, 'era5_land', str(year), f'{month:02d}')
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f'era5_land_{year}{month:02d}.nc')

    if os.path.exists(out_path) and not force_rebuild:
        print(f'ERA5 file already exists: {out_path}  (use force_rebuild=True to re-download)')
        return out_path

    # Build list of days in the month
    _, n_days = calendar.monthrange(year, month)
    days = [f'{d:02d}' for d in range(1, n_days + 1)]
    times = [f'{h:02d}:00' for h in range(24)]

    # CDS request parameters (new API ≥ 0.7.0)
    request = {
        'product_type': 'reanalysis',
        'variable': ERA5_VARS,
        'year': str(year),
        'month': f'{month:02d}',
        'day': days,
        'time': times,
        # Area: [north, west, south, east] in decimal degrees
        'area': [TEXAS_LAT_MAX, TEXAS_LON_MIN, TEXAS_LAT_MIN, TEXAS_LON_MAX],
        'data_format': 'netcdf',
        'download_format': 'unarchived',
    }

    print(f'Requesting ERA5-Land for {year}-{month:02d} ({n_days} days, 24 hr/day)...')
    print(f'  Area: lat [{TEXAS_LAT_MIN}, {TEXAS_LAT_MAX}], '
          f'lon [{TEXAS_LON_MIN}, {TEXAS_LON_MAX}]')
    print(f'  Variables: {ERA5_VARS}')
    print(f'  Output: {out_path}')

    # Download to a temp file so we can post-process before placing the final file
    with tempfile.NamedTemporaryFile(suffix='.nc', delete=False) as tmp:
        tmp_path = tmp.name

    try:
        import cdsapi
        c = cdsapi.Client()
        # The new CDS API returns a Result object; call .download() to fetch the file
        result = c.retrieve('reanalysis-era5-land', request, tmp_path)
        # For cdsapi >= 0.7: retrieve() may return a Result with a .download() method
        # or directly write to tmp_path. Handle both cases.
        if hasattr(result, 'download'):
            result.download(tmp_path)

        print(f'  Download complete ({os.path.getsize(tmp_path) / 1e6:.1f} MB raw). '
              f'Adding derived variables...')

        # Open, add derived wind variables, re-save with compression
        ds = xr.open_dataset(tmp_path)
        ds = _add_derived_wind(ds)

        encoding = {v: COMPRESS_OPTS.copy() for v in ds.data_vars}
        ds.to_netcdf(out_path, encoding=encoding)
        ds.close()

        final_mb = os.path.getsize(out_path) / 1e6
        print(f'  Saved compressed NetCDF ({final_mb:.1f} MB): {out_path}')

        # Print a quick sanity check
        ds2 = xr.open_dataset(out_path)
        lat = ds2.latitude.values
        lon = ds2.longitude.values
        n_times = len(ds2.valid_time)
        print(f'  Grid: {len(lat)} lat × {len(lon)} lon, {n_times} hourly steps')
        print(f'  t2m range: {float(ds2.t2m.min()):.1f} – {float(ds2.t2m.max()):.1f} K')
        print(f'  wspd range: {float(ds2.wspd.min()):.2f} – {float(ds2.wspd.max()):.2f} m/s')
        ds2.close()

    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

    return out_path


def download_era5_wind100m_month(
    year: int,
    month: int,
    base_dir: str,
    force_rebuild: bool = False,
) -> str:
    """Download ERA5 100m wind for Texas for a given month.

    Uses reanalysis-era5-single-levels (0.25° resolution) because ERA5-Land
    does not include 100m wind components.

    Downloads 100m_u/v_component_of_wind, adds derived wspd100 and wdir100,
    and saves as compressed NetCDF in the same directory as era5_land files.

    Args:
        year: Four-digit year (e.g. 2025).
        month: Month number 1–12.
        base_dir: Root raw data directory (dirs['raw'] from setup_directories()).
        force_rebuild: If True, re-download even if the output file already exists.

    Returns:
        Absolute path to the output NetCDF file.
    """
    out_dir = os.path.join(base_dir, 'era5_land', str(year), f'{month:02d}')
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f'era5_wind100m_{year}{month:02d}.nc')

    if os.path.exists(out_path) and not force_rebuild:
        print(f'ERA5 100m wind file already exists: {out_path}  (use force_rebuild=True to re-download)')
        return out_path

    _, n_days = calendar.monthrange(year, month)
    days = [f'{d:02d}' for d in range(1, n_days + 1)]
    times = [f'{h:02d}:00' for h in range(24)]

    request = {
        'product_type': ['reanalysis'],
        'variable': ['100m_u_component_of_wind', '100m_v_component_of_wind'],
        'year': str(year),
        'month': f'{month:02d}',
        'day': days,
        'time': times,
        'area': [TEXAS_LAT_MAX, TEXAS_LON_MIN, TEXAS_LAT_MIN, TEXAS_LON_MAX],
        'data_format': 'netcdf',
        'download_format': 'unarchived',
    }

    print(f'Requesting ERA5 100m wind for {year}-{month:02d} ({n_days} days, 24 hr/day)...')
    print(f'  Dataset: reanalysis-era5-single-levels (0.25° resolution)')
    print(f'  Area: lat [{TEXAS_LAT_MIN}, {TEXAS_LAT_MAX}], lon [{TEXAS_LON_MIN}, {TEXAS_LON_MAX}]')
    print(f'  Output: {out_path}')

    with tempfile.NamedTemporaryFile(suffix='.nc', delete=False) as tmp:
        tmp_path = tmp.name

    try:
        import cdsapi
        c = cdsapi.Client()
        result = c.retrieve('reanalysis-era5-single-levels', request, tmp_path)
        if hasattr(result, 'download'):
            result.download(tmp_path)

        print(f'  Download complete ({os.path.getsize(tmp_path) / 1e6:.1f} MB raw). '
              f'Adding derived variables...')

        ds = xr.open_dataset(tmp_path)
        ds = _add_derived_wind(ds, u_var='u100', v_var='v100',
                               wspd_name='wspd100', wdir_name='wdir100',
                               height_label='100m')
        encoding = {v: COMPRESS_OPTS.copy() for v in ds.data_vars}
        ds.to_netcdf(out_path, encoding=encoding)
        ds.close()

        ds2 = xr.open_dataset(out_path)
        lat = ds2.latitude.values
        lon = ds2.longitude.values
        n_times = len(ds2.valid_time)
        print(f'  Grid: {len(lat)} lat × {len(lon)} lon, {n_times} hourly steps')
        print(f'  wspd100 range: {float(ds2.wspd100.min()):.2f} – {float(ds2.wspd100.max()):.2f} m/s')
        ds2.close()

    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

    return out_path


def download_era5_months(
    months: list,
    base_dir: str,
    force_rebuild: bool = False,
) -> dict:
    """Download ERA5-Land and ERA5 100m wind for a list of (year, month) tuples.

    Args:
        months: List of (year, month) tuples, e.g. [(2025, 1), (2025, 2), ...].
        base_dir: Root raw data directory (dirs['raw'] from setup_directories()).
        force_rebuild: Re-download even if output file exists.

    Returns:
        Dict mapping (year, month) -> {'era5_land': path, 'wind100m': path}.
    """
    results = {}
    for year, month in months:
        print(f'\n{"="*60}')
        print(f'ERA5: {year}-{month:02d}')
        print('='*60)
        land_path = download_era5_month(year, month, base_dir, force_rebuild=force_rebuild)
        wind100_path = download_era5_wind100m_month(year, month, base_dir, force_rebuild=force_rebuild)
        results[(year, month)] = {'era5_land': land_path, 'wind100m': wind100_path}
    return results


# ── CLI ───────────────────────────────────────────────────────────────────────

def _build_parser():
    p = argparse.ArgumentParser(
        description='Download ERA5-Land and ERA5 100m wind for Texas.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument('--year', type=int, required=True, help='Four-digit year')
    p.add_argument('--month', type=int, required=True, help='Month number (1–12)')
    p.add_argument(
        '--force-rebuild', action='store_true',
        help='Re-download even if output file already exists',
    )
    p.add_argument(
        '--land-only', action='store_true',
        help='Download ERA5-Land (t2m, 10m wind) only, skip 100m wind',
    )
    p.add_argument(
        '--wind100m-only', action='store_true',
        help='Download ERA5 100m wind only, skip ERA5-Land',
    )
    return p


def main():
    args = _build_parser().parse_args()
    dirs = setup_directories()

    if not args.wind100m_only:
        download_era5_month(
            year=args.year,
            month=args.month,
            base_dir=dirs['raw'],
            force_rebuild=args.force_rebuild,
        )

    if not args.land_only:
        download_era5_wind100m_month(
            year=args.year,
            month=args.month,
            base_dir=dirs['raw'],
            force_rebuild=args.force_rebuild,
        )


if __name__ == '__main__':
    main()
