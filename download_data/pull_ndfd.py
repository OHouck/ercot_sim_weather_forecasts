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

# WMO header element codes (2nd character in Y{code}UZ88 filenames)
ELEMENT_WMO_CODES = {
    'temp': 'E',
    'maxt': 'G',
    'mint': 'H',
    'wspd': 'C',
    'wdir': 'B',
}

# Target lead times to extract from each forecast file (hours)
# Note: Z88 files are 3-hourly starting at step=1h or step=2h.
# No file has an exact 24h step; 25h is the closest from Group B files.
TARGET_LEAD_HOURS = [1, 25]

# Group A issuance hours (steps start at 2h: 2,5,8,...,23,26,...)
# Group B issuance hours (steps start at 1h: 1,4,7,...,22,25,...)
# We only want Group B since it has both 1h and 25h steps.
GROUP_A_HOURS = {0, 3, 6, 9, 12, 15, 18, 21}

def check_data_availability(element='maxt', start_year=2020, end_year=2025):
    """Check which months have data for each year"""
    print(f"\n=== Data Availability for {element} ===\n")
    
    for year in range(start_year, end_year + 1):
        path = f"s3://noaa-ndfd-pds/wmo/{element}/{year}/"
        cmd = ["aws", "s3", "ls", "--no-sign-request", path]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0 and result.stdout.strip():
            months = [line.split()[1].replace('/', '').replace('PRE', '').strip() 
                     for line in result.stdout.split('\n') if 'PRE' in line]
            months = [m for m in months if m.isdigit()]
            
            if months:
                print(f"{year}: Months {min(months)}-{max(months)} ({len(months)} months)")
            else:
                print(f"{year}: No data or irregular structure")
        else:
            print(f"{year}: No data")

def extract_texas_from_grib(grib_file, output_dir, target_lead_hours=None):
    """Extract Texas bounding box from CONUS GRIB2 file and save as NetCDF.

    Handles Lambert Conformal projection with 2D lat/lon arrays.
    Skips files that don't cover Texas (e.g., Puerto Rico, Alaska, Hawaii).

    Args:
        grib_file: Path to the GRIB2 file
        output_dir: Directory to save extracted NetCDF files
        target_lead_hours: List of lead times (hours) to keep. If None, keeps all steps.
    """
    # Texas bounds
    lat_min, lat_max = 25.8, 36.5
    lon_min, lon_max = -106.6, -93.5

    try:
        # Open with xarray + cfgrib
        ds = xr.open_dataset(str(grib_file), engine='cfgrib')

        # Filter to target lead times if specified
        if target_lead_hours is not None and 'step' in ds.dims:
            target_steps = [np.timedelta64(h, 'h') for h in target_lead_hours]
            available_steps = ds.step.values
            # Keep only steps that match a target (within 30 min tolerance)
            keep_steps = []
            for target in target_steps:
                diffs = np.abs(available_steps - target)
                best_idx = np.argmin(diffs)
                if diffs[best_idx] <= np.timedelta64(30, 'm'):
                    keep_steps.append(available_steps[best_idx])
            if len(keep_steps) == 0:
                print(f"  - Skipping {grib_file.name}: no matching lead times")
                ds.close()
                return None
            ds = ds.sel(step=keep_steps)

        lon = ds.longitude.values
        lat = ds.latitude.values

        # Check if this is a 2D grid (CONUS Lambert Conformal) or 1D (other regions)
        if lat.ndim == 1:
            # 1D coordinates - likely a non-CONUS regional file
            # Convert longitude and check if any points are in Texas
            lon_converted = np.where(lon > 180, lon - 360, lon)
            if not (lat.min() <= lat_max and lat.max() >= lat_min and
                    lon_converted.min() <= lon_max and lon_converted.max() >= lon_min):
                print(f"  - Skipping {grib_file.name}: region doesn't overlap Texas")
                ds.close()
                return None
            # For 1D grids, we'd need different handling - skip for now
            print(f"  - Skipping {grib_file.name}: 1D grid format not supported")
            ds.close()
            return None

        # 2D grid (CONUS Lambert Conformal projection)
        # Convert longitude from 0-360 to -180-180 format
        lon_converted = np.where(lon > 180, lon - 360, lon)

        # Create mask for Texas region
        mask = (
            (lat >= lat_min) & (lat <= lat_max) &
            (lon_converted >= lon_min) & (lon_converted <= lon_max)
        )

        # Find the bounding box indices for the mask
        y_indices, x_indices = np.where(mask)

        if len(y_indices) == 0:
            print(f"  - Skipping {grib_file.name}: no data points in Texas region")
            ds.close()
            return None

        y_min, y_max = y_indices.min(), y_indices.max() + 1
        x_min, x_max = x_indices.min(), x_indices.max() + 1

        # Subset to bounding box using isel
        ds_texas = ds.isel(y=slice(y_min, y_max), x=slice(x_min, x_max))

        # Update longitude to standard format (-180 to 180)
        ds_texas = ds_texas.assign_coords(
            longitude=(('y', 'x'), lon_converted[y_min:y_max, x_min:x_max])
        )

        # Create output filename
        output_file = os.path.join(output_dir, grib_file.stem + '_texas.nc')

        # Save as compressed NetCDF
        encoding = {var: {'zlib': True, 'complevel': 5}
                   for var in ds_texas.data_vars}
        ds_texas.to_netcdf(output_file, encoding=encoding)

        ds.close()
        ds_texas.close()

        return output_file

    except Exception as e:
        print(f"  ✗ Error processing {grib_file.name}: {e}")
        return None

def download_and_extract_texas_month(element, year, month, base_dir):
    """Download month of CONUS-only NDFD Z88 data, extract Texas, and delete GRIB files.

    Only downloads CONUS 2.5km Days 1-3 files (Z88 product, ~hourly issuance).
    Filters each file to keep only the target lead times before saving.
    """
    wmo_code = ELEMENT_WMO_CODES.get(element)
    if wmo_code is None:
        print(f"✗ Unknown element '{element}' - no WMO code mapping")
        return False

    # CONUS Z88 filename prefix: e.g. YEUZ88 for temp, YCUZ88 for wspd
    conus_prefix = f"Y{wmo_code}UZ88"

    output_dir = os.path.join(base_dir, element, str(year), f"{month:02d}")
    os.makedirs(output_dir, exist_ok=True)

    num_days = calendar.monthrange(year, month)[1]
    total_successful = 0
    total_skipped = 0
    total_downloaded = 0

    for day in range(1, num_days + 1):
        s3_day_path = f"s3://noaa-ndfd-pds/wmo/{element}/{year}/{month:02d}/{day:02d}/"

        # List files for this day
        list_cmd = ["aws", "s3", "ls", "--no-sign-request", s3_day_path]
        list_result = subprocess.run(list_cmd, capture_output=True, text=True)

        if list_result.returncode != 0 or not list_result.stdout.strip():
            continue

        # Filter for CONUS Z88 Group B files only (skip Group A issuance hours)
        conus_files = []
        for line in list_result.stdout.strip().split('\n'):
            parts = line.split()
            if len(parts) >= 4:
                filename = parts[-1]
                if not filename.startswith(conus_prefix):
                    continue
                # Extract issuance hour from filename: YEUZ88_KWBN_YYYYMMDDHHMM
                # The hour is at characters 18-20 after "YEUZ88_KWBN_YYYYMMDD"
                try:
                    timestamp_str = filename.split('_')[-1]  # e.g. "202501010047"
                    issuance_hour = int(timestamp_str[8:10])  # HH portion
                    if issuance_hour in GROUP_A_HOURS:
                        continue
                except (ValueError, IndexError):
                    pass
                conus_files.append(filename)

        if not conus_files:
            continue

        print(f"  {year}-{month:02d}-{day:02d}: {len(conus_files)} CONUS Z88 Group B files")

        with tempfile.TemporaryDirectory() as temp_dir:
            for filename in conus_files:
                s3_file = f"{s3_day_path}{filename}"
                local_file = os.path.join(temp_dir, filename)

                cp_cmd = [
                    "aws", "s3", "cp", "--no-sign-request",
                    s3_file, local_file
                ]
                cp_result = subprocess.run(cp_cmd, capture_output=True, text=True)

                if cp_result.returncode != 0:
                    total_skipped += 1
                    continue

                total_downloaded += 1
                result = extract_texas_from_grib(
                    Path(local_file), output_dir,
                    target_lead_hours=TARGET_LEAD_HOURS
                )
                if result:
                    total_successful += 1
                else:
                    total_skipped += 1

                # Delete GRIB file immediately to save disk space
                os.remove(local_file)

    print(f"\n✓ {element} {year}-{month:02d}: "
          f"{total_downloaded} downloaded, {total_successful} extracted, "
          f"{total_skipped} skipped")
    print(f"  Lead times kept: {TARGET_LEAD_HOURS}h")
    print(f"  Saved to: {output_dir}")

    nc_files = list(Path(output_dir).glob("*.nc"))
    if nc_files:
        texas_size_mb = sum(f.stat().st_size for f in nc_files) / (1024 * 1024)
        print(f"  Texas NetCDF files: {len(nc_files)} files, {texas_size_mb:.1f} MB")

    return True

def download_year_data(year, elements, base_dir, texas_only=True):
    """Download data for an entire year, optionally extracting only Texas"""
    
    for element in elements:
        print(f"\n=== Processing {element} for {year} ===")
        for month in range(1, 13):
            if texas_only:
                download_and_extract_texas_month(element, year, month, base_dir)
            else:
                download_ndfd_month(element, year, month, base_dir)

def download_ndfd_month(element, year, month, base_dir):
    """Download entire month of NDFD data (full CONUS, no extraction)"""
    output_dir = os.path.join(base_dir, element, str(year), f"{month:02d}")
    os.makedirs(output_dir, exist_ok=True)
    
    s3_path = f"s3://noaa-ndfd-pds/wmo/{element}/{year}/{month:02d}/"
    
    cmd = [
        "aws", "s3", "sync",
        "--no-sign-request",
        s3_path,
        output_dir
    ]
    
    print(f"Downloading {element} for {year}-{month:02d}...")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode == 0:
        # Count files downloaded
        file_count = sum(len(files) for _, _, files in os.walk(output_dir))
        print(f"✓ Downloaded {element} {year}-{month:02d}: {file_count} files")
        return True
    else:
        print(f"✗ Error: {result.stderr}")
        return False

def plot_texas_temp_forecast(nc_file, step_index=0, output_file=None, units='F'):
    """Create a map of temperature forecasts for Texas.

    Args:
        nc_file: Path to the Texas NetCDF file
        step_index: Which forecast step to plot (0, 1, 2, etc.)
        output_file: Path to save the figure (if None, displays interactively)
        units: Temperature units - 'F' for Fahrenheit, 'C' for Celsius, 'K' for Kelvin

    Returns:
        Path to saved figure or None if displayed interactively
    """
    import matplotlib.pyplot as plt

    ds = xr.open_dataset(nc_file)

    # Get the temperature variable (cfgrib names it 't2m' or 't')
    temp_var = None
    for name in ['t2m', 't', 'tmax', 'tmp']:
        if name in ds.data_vars:
            temp_var = name
            break
    if temp_var is None:
        temp_var = list(ds.data_vars)[0]

    if 'step' in ds.dims:
        temp = ds[temp_var].isel(step=step_index).values
        valid_time = ds.valid_time.isel(step=step_index).values
    else:
        temp = ds[temp_var].values
        valid_time = ds.valid_time.values

    lat = ds.latitude.values
    lon = ds.longitude.values
    forecast_time = ds.time.values

    # Convert temperature units
    if units == 'F':
        temp = (temp - 273.15) * 9/5 + 32
        unit_label = '°F'
        cmap = 'RdYlBu_r'
        vmin, vmax = 20, 90
    elif units == 'C':
        temp = temp - 273.15
        unit_label = '°C'
        cmap = 'RdYlBu_r'
        vmin, vmax = -10, 35
    else:  # Kelvin
        unit_label = 'K'
        cmap = 'RdYlBu_r'
        vmin, vmax = 260, 310

    # Create figure
    fig, ax = plt.subplots(figsize=(12, 10))

    mesh = ax.pcolormesh(lon, lat, temp, cmap=cmap, vmin=vmin, vmax=vmax, shading='auto')

    cbar = plt.colorbar(mesh, ax=ax, shrink=0.8, pad=0.02)
    cbar.set_label(f'Temperature ({unit_label})', fontsize=12)

    # Format times for title
    forecast_dt = np.datetime64(forecast_time, 'h')
    valid_dt = np.datetime64(valid_time, 'h')
    forecast_str = str(forecast_dt)[:13].replace('T', ' ')
    valid_str = str(valid_dt)[:13].replace('T', ' ')

    ax.set_title(f'NDFD Temperature Forecast for Texas\n'
                 f'Forecast issued: {forecast_str} UTC | Valid: {valid_str} UTC',
                 fontsize=14)

    ax.set_xlabel('Longitude', fontsize=12)
    ax.set_ylabel('Latitude', fontsize=12)

    ax.set_xlim(-107, -93)
    ax.set_ylim(25.5, 37)

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


def main():
    dirs = setup_directories()
    
    # Configuration
    year = 2025
    texas_only = True  # Set to False if you want full CONUS files instead
    
    # Elements to download (CONUS Z88 product, ~hourly issuance, lead times 1h & 24h)
    elements = [
        'temp',      # Temperature (instantaneous 2m)
        'wspd',      # Wind speed (10m)
        'wdir',      # Wind direction (10m)
    ]
    
    output_dir = os.path.join(dirs['raw'], 'ndfd_data')
    download = True 

    if download:
        print(f"Starting download for {year}")
        print(f"Output directory: {output_dir}")
        print(f"Elements: {', '.join(elements)}")
        print(f"Product: CONUS 2.5km Z88, Group B only (skip 00/03/06/.../21 UTC)")
        print(f"Lead times: {TARGET_LEAD_HOURS}h (1h and ~24h)")
        print(f"Texas extraction: {'ENABLED' if texas_only else 'DISABLED (full CONUS)'}")
        print(f"\nExpected data: Jan-{datetime.now().strftime('%b')} {year}")
        
        # Confirm before starting
        response = input("\nProceed with download? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("Download cancelled.")
            return
        
        download_year_data(year, elements, output_dir, texas_only=texas_only)
        
        print("\n=== Download Complete ===")
        print(f"Data saved to: {output_dir}")
        
        # Show summary
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

    # plot example forecast
    example_element = 'temp'
    example_month = 1
    example_nc_dir = os.path.join(output_dir, example_element, str(year), f"{example_month:02d}")
    print(example_nc_dir)
    example_nc_files = list(Path(example_nc_dir).glob("*_texas.nc"))
    num_files = len(example_nc_files)
    print(f"\nFound {num_files} example Texas NetCDF files for {example_element}")
    if example_nc_files:
        example_nc_file = example_nc_files[0]
        print(f"\nPlotting example forecast from: {example_nc_file}")
        plot_texas_temp_forecast(example_nc_file, step_index=0, output_file=None, units='F')

    

if __name__ == "__main__":
    main()