"""pull_ndfd.py — Download NDFD 12Z weather forecasts from NOAA S3 and extract Texas.

Downloads one CONUS Z88 (2.5km, Days 1-3) GRIB2 file per day from the ~12 UTC
issuance, extracts the Texas bounding box, keeps all 3-hourly forecast steps up
to 48h lead time, and saves as compressed NetCDF.

Output: {base_dir}/{element}/{year}/{month:02d}/ndfd_12z_{YYYYMMDD}.nc
Each file contains ~16 steps (2, 5, 8, ..., 47h) over the Texas grid.

Usage:
    # Single month
    from download_data.pull_ndfd import download_12z_forecasts_month
    download_12z_forecasts_month('temp', 2025, 7, base_dir)

    # Full year
    uv run python -m download_data.pull_ndfd
"""

import subprocess
import os
import calendar
from datetime import datetime
import sys
from pathlib import Path
import tempfile

import xarray as xr
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))
from helper_funcs import setup_directories

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# WMO header element codes (2nd character in Y{code}UZ88 filenames)
ELEMENT_WMO_CODES = {
    'temp': 'E',
    'maxt': 'G',
    'mint': 'H',
    'wspd': 'C',
    'wdir': 'B',
}

# Maximum lead time (hours) to retain from each forecast file.
# Group A 12Z files have 3-hourly steps: 2, 5, 8, ..., 47h covers 48h.
MAX_LEAD_HOURS = 48

# Texas geographic bounds for spatial extraction
TEXAS_LAT_MIN, TEXAS_LAT_MAX = 25.8, 36.5
TEXAS_LON_MIN, TEXAS_LON_MAX = -106.6, -93.5

S3_BUCKET = "s3://noaa-ndfd-pds/wmo"

# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

def _list_s3_day(element, year, month, day):
    """List all files on S3 for a given element/date. Returns list of filenames."""
    s3_day_path = f"{S3_BUCKET}/{element}/{year}/{month:02d}/{day:02d}/"
    result = subprocess.run(
        ["aws", "s3", "ls", "--no-sign-request", s3_day_path],
        capture_output=True, text=True,
    )
    if result.returncode != 0 or not result.stdout.strip():
        return [], s3_day_path

    filenames = []
    for line in result.stdout.strip().split('\n'):
        parts = line.split()
        if len(parts) >= 4:
            filenames.append(parts[-1])
    return filenames, s3_day_path


def _download_s3_file(s3_path, local_path):
    """Download a single file from S3. Returns True on success."""
    result = subprocess.run(
        ["aws", "s3", "cp", "--no-sign-request", s3_path, local_path],
        capture_output=True, text=True,
    )
    return result.returncode == 0


def _filter_conus_z88(filenames, element):
    """Keep only CONUS Z88 filenames for the given element.

    CONUS Z88 files start with 'Y{ELEM}UZ88' where ELEM is the WMO element
    code (E=temp, C=wspd, B=wdir, etc.).
    """
    wmo_code = ELEMENT_WMO_CODES[element]
    prefix = f"Y{wmo_code}UZ88"
    return [f for f in filenames if f.startswith(prefix)]


def _find_closest_to_12z(filenames):
    """From CONUS Z88 filenames, find the one issued closest to 12:00 UTC.

    Parses the YYYYMMDDHHMM timestamp from each filename and returns the one
    closest to 12:00 UTC within a 2-hour tolerance window. Returns None if no
    file falls within the window.

    Filename format: Y{ELEM}UZ88_KWBN_YYYYMMDDHHMM
    """
    best_file = None
    best_distance_min = None

    for filename in filenames:
        try:
            timestamp_str = filename.split('_')[-1]  # e.g. "202507011147"
            hh = int(timestamp_str[8:10])
            mm = int(timestamp_str[10:12])
            distance_min = abs((hh * 60 + mm) - 12 * 60)
        except (ValueError, IndexError):
            continue

        if distance_min <= 120 and (best_distance_min is None or distance_min < best_distance_min):
            best_distance_min = distance_min
            best_file = filename

    return best_file


# ---------------------------------------------------------------------------
# GRIB extraction
# ---------------------------------------------------------------------------

def extract_texas_from_grib(grib_file, output_dir, max_lead_hours=MAX_LEAD_HOURS,
                            output_filename=None):
    """Extract Texas bounding box from a CONUS GRIB2 file and save as NetCDF.

    Opens the GRIB2 file with cfgrib, filters to forecast steps within
    *max_lead_hours*, subsets the 2D Lambert Conformal grid to the Texas
    bounding box, converts longitude to [-180, 180], and writes compressed
    NetCDF.

    Args:
        grib_file: Path to the GRIB2 file.
        output_dir: Directory to write the NetCDF file.
        max_lead_hours: Keep all forecast steps with lead time <= this (hours).
        output_filename: Override the output filename. Defaults to
            ``{grib_stem}_texas.nc``.

    Returns:
        Path to the saved NetCDF file, or None if the file was skipped.
    """
    try:
        ds = xr.open_dataset(str(grib_file), engine='cfgrib')

        # --- Filter forecast steps to the retention window ----------------
        if 'step' in ds.dims:
            max_td = np.timedelta64(max_lead_hours, 'h')
            keep_steps = ds.step.values[ds.step.values <= max_td]
            if len(keep_steps) == 0:
                print(f"  - Skipping {grib_file.name}: no steps within {max_lead_hours}h")
                ds.close()
                return None
            ds = ds.sel(step=keep_steps)

        # --- Verify this is a 2D CONUS grid (Lambert Conformal) -----------
        lat = ds.latitude.values
        lon = ds.longitude.values

        if lat.ndim == 1:
            # 1D lat/lon → non-CONUS regional file (Alaska, Hawaii, etc.)
            print(f"  - Skipping {grib_file.name}: 1D grid (non-CONUS)")
            ds.close()
            return None

        # --- Subset to Texas bounding box ---------------------------------
        lon_180 = np.where(lon > 180, lon - 360, lon)  # convert to [-180, 180]

        texas_mask = (
            (lat >= TEXAS_LAT_MIN) & (lat <= TEXAS_LAT_MAX) &
            (lon_180 >= TEXAS_LON_MIN) & (lon_180 <= TEXAS_LON_MAX)
        )
        y_idx, x_idx = np.where(texas_mask)

        if len(y_idx) == 0:
            print(f"  - Skipping {grib_file.name}: no grid points in Texas")
            ds.close()
            return None

        y_slice = slice(y_idx.min(), y_idx.max() + 1)
        x_slice = slice(x_idx.min(), x_idx.max() + 1)

        ds_texas = ds.isel(y=y_slice, x=x_slice)
        ds_texas = ds_texas.assign_coords(
            longitude=(('y', 'x'), lon_180[y_slice, x_slice])
        )

        # --- Write compressed NetCDF --------------------------------------
        if output_filename is None:
            output_filename = grib_file.stem + '_texas.nc'
        output_file = os.path.join(output_dir, output_filename)

        encoding = {var: {'zlib': True, 'complevel': 5} for var in ds_texas.data_vars}
        ds_texas.to_netcdf(output_file, encoding=encoding)

        ds.close()
        ds_texas.close()
        return output_file

    except Exception as e:
        print(f"  - Error processing {grib_file.name}: {e}")
        return None


# ---------------------------------------------------------------------------
# Download entry points
# ---------------------------------------------------------------------------

def download_12z_forecasts_month(element, year, month, base_dir):
    """Download one 12Z NDFD Z88 file per day and extract the Texas subset.

    For each day in the month:
      1. Lists all CONUS Z88 files on S3 for that day
      2. Selects the file closest to 12:00 UTC issuance
      3. Downloads to a temp directory, extracts Texas, saves as NetCDF
      4. Skips days that already have an output file on disk

    Output: {base_dir}/{element}/{year}/{month:02d}/ndfd_12z_{YYYYMMDD}.nc
    """
    if element not in ELEMENT_WMO_CODES:
        print(f"  Unknown element '{element}'")
        return False

    output_dir = os.path.join(base_dir, element, str(year), f"{month:02d}")
    os.makedirs(output_dir, exist_ok=True)

    num_days = calendar.monthrange(year, month)[1]
    total_successful = 0
    total_skipped = 0

    for day in range(1, num_days + 1):
        date_str = f"{year}{month:02d}{day:02d}"
        output_filename = f"ndfd_12z_{date_str}.nc"
        output_path = os.path.join(output_dir, output_filename)

        # Skip days already on disk
        if os.path.exists(output_path):
            total_successful += 1
            continue

        # List available files on S3 and pick the best 12Z candidate
        all_files, s3_day_path = _list_s3_day(element, year, month, day)
        conus_files = _filter_conus_z88(all_files, element)
        best_file = _find_closest_to_12z(conus_files)

        if best_file is None:
            reason = "no files on S3" if not all_files else "no 12Z file found"
            print(f"  {year}-{month:02d}-{day:02d}: {reason}")
            total_skipped += 1
            continue

        # Download the GRIB file to a temp directory, extract Texas, clean up
        with tempfile.TemporaryDirectory() as tmp:
            local_grib = os.path.join(tmp, best_file)

            if not _download_s3_file(f"{s3_day_path}{best_file}", local_grib):
                print(f"  {year}-{month:02d}-{day:02d}: download failed ({best_file})")
                total_skipped += 1
                continue

            nc_path = extract_texas_from_grib(
                Path(local_grib), output_dir,
                max_lead_hours=MAX_LEAD_HOURS,
                output_filename=output_filename,
            )

            if nc_path:
                print(f"  {year}-{month:02d}-{day:02d}: extracted from {best_file}")
                total_successful += 1
            else:
                total_skipped += 1

    # Summary
    print(f"\n  {element} {year}-{month:02d}: "
          f"{total_successful} extracted, {total_skipped} skipped")
    print(f"  Max lead time: {MAX_LEAD_HOURS}h  |  Output: {output_dir}")

    nc_files = list(Path(output_dir).glob("ndfd_12z_*.nc"))
    if nc_files:
        size_mb = sum(f.stat().st_size for f in nc_files) / (1024 * 1024)
        print(f"  {len(nc_files)} files, {size_mb:.1f} MB total")

    return True


def download_year_data(year, elements, base_dir):
    """Download 12Z NDFD forecasts for every month of *year*.

    Args:
        year: Calendar year.
        elements: Element names to download (e.g. ['temp', 'wspd', 'wdir']).
        base_dir: Root output directory for NDFD data.
    """
    for element in elements:
        print(f"\n=== Processing {element} for {year} ===")
        for month in range(1, 13):
            download_12z_forecasts_month(element, year, month, base_dir)


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def check_data_availability(element='temp', start_year=2020, end_year=2025):
    """Print which months have NDFD data on S3 for each year."""
    print(f"\n=== Data Availability for {element} ===\n")

    for year in range(start_year, end_year + 1):
        result = subprocess.run(
            ["aws", "s3", "ls", "--no-sign-request", f"{S3_BUCKET}/{element}/{year}/"],
            capture_output=True, text=True,
        )
        if result.returncode != 0 or not result.stdout.strip():
            print(f"{year}: No data")
            continue

        months = [
            line.split()[1].strip('/')
            for line in result.stdout.strip().split('\n')
            if 'PRE' in line
        ]
        months = [m for m in months if m.isdigit()]
        if months:
            print(f"{year}: Months {min(months)}-{max(months)} ({len(months)} months)")
        else:
            print(f"{year}: No data or irregular structure")


def plot_texas_temp_forecast(nc_file, step_index=0, output_file=None, units='F'):
    """Plot a single temperature forecast step on a Texas map.

    Args:
        nc_file: Path to a Texas-subset NetCDF file.
        step_index: Which forecast step to plot (0-based index).
        output_file: Save path. If None, displays interactively.
        units: 'F', 'C', or 'K'.

    Returns:
        Path to the saved figure, or None if displayed interactively.
    """
    import matplotlib.pyplot as plt

    ds = xr.open_dataset(nc_file)

    # Identify temperature variable
    temp_var = next(
        (name for name in ['t2m', 't', 'tmax', 'tmp'] if name in ds.data_vars),
        list(ds.data_vars)[0],
    )

    if 'step' in ds.dims:
        temp = ds[temp_var].isel(step=step_index).values
        valid_time = ds.valid_time.isel(step=step_index).values
    else:
        temp = ds[temp_var].values
        valid_time = ds.valid_time.values

    lat = ds.latitude.values
    lon = ds.longitude.values
    forecast_time = ds.time.values

    # Unit conversion
    unit_cfg = {
        'F': ('°F', 20, 90, lambda t: (t - 273.15) * 9 / 5 + 32),
        'C': ('°C', -10, 35, lambda t: t - 273.15),
        'K': ('K', 260, 310, lambda t: t),
    }
    unit_label, vmin, vmax, convert = unit_cfg[units]
    temp = convert(temp)

    # Plot
    fig, ax = plt.subplots(figsize=(12, 10))
    mesh = ax.pcolormesh(lon, lat, temp, cmap='RdYlBu_r',
                         vmin=vmin, vmax=vmax, shading='auto')

    cbar = plt.colorbar(mesh, ax=ax, shrink=0.8, pad=0.02)
    cbar.set_label(f'Temperature ({unit_label})', fontsize=12)

    forecast_str = str(np.datetime64(forecast_time, 'h'))[:13].replace('T', ' ')
    valid_str = str(np.datetime64(valid_time, 'h'))[:13].replace('T', ' ')
    ax.set_title(f'NDFD Temperature Forecast for Texas\n'
                 f'Issued: {forecast_str} UTC  |  Valid: {valid_str} UTC',
                 fontsize=14)

    ax.set(xlabel='Longitude', ylabel='Latitude', xlim=(-107, -93), ylim=(25.5, 37))
    ax.grid(True, alpha=0.3, linestyle='--')
    plt.tight_layout()
    ds.close()

    if output_file:
        plt.savefig(output_file, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"Saved figure to: {output_file}")
        return output_file
    else:
        plt.show()
        return None


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    dirs = setup_directories()

    year = 2025
    elements = ['temp', 'wspd', 'wdir']
    output_dir = os.path.join(dirs['raw'], 'ndfd_data')

    print(f"Starting 12Z download for {year}")
    print(f"Output directory: {output_dir}")
    print(f"Elements: {', '.join(elements)}")
    print(f"Product: CONUS 2.5km Z88, 12Z initialization only")
    print(f"Max lead time: {MAX_LEAD_HOURS}h (all 3-hourly steps up to 48h)")
    print(f"Texas extraction: ENABLED")
    print(f"\nExpected data: Jan-{datetime.now().strftime('%b')} {year}")

    response = input("\nProceed with download? (yes/no): ")
    if response.lower() not in ['yes', 'y']:
        print("Download cancelled.")
        return

    download_year_data(year, elements, output_dir)

    print("\n=== Download Complete ===")
    print(f"Data saved to: {output_dir}")

    print("\nSummary:")
    for element in elements:
        element_dir = os.path.join(output_dir, element, str(year))
        if os.path.exists(element_dir):
            total_files = sum(len(files) for _, _, files in os.walk(element_dir))
            total_size_mb = sum(
                os.path.getsize(os.path.join(dirpath, filename))
                for dirpath, _, filenames in os.walk(element_dir)
                for filename in filenames
            ) / (1024 * 1024)
            print(f"  {element}: {total_files} files, {total_size_mb:.1f} MB")


if __name__ == "__main__":
    main()
