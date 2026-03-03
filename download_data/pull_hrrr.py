"""pull_hrrr.py — Download HRRR 3km weather forecasts from AWS S3 and extract Texas.

Downloads HRRR surface (wrfsfcf) GRIB2 files from the NOAA HRRR archive on S3,
using byte-range requests to download only TMP:2m, UGRD:10m, VGRD:10m fields
(~6 MB vs ~150 MB per file). Extracts the Texas bounding box, computes wind
speed/direction from U/V components, and saves as compressed NetCDF.

All times are in UTC. HRRR runs 24 initializations per day (00z–23z).
Standard cycles (all 24) produce forecasts to 18h lead time.
Extended cycles (00z, 06z, 12z, 18z) produce forecasts to 48h lead time.
Currently downloads f01 and f18 from all 24 cycles; extend to f24+ by
changing LEAD_TIMES and adding EXTENDED_CYCLES logic.

Output: {base_dir}/{element}/{year}/{month:02d}/hrrr_{HH}z_{YYYYMMDD}_f{FF}.nc
Each file contains one forecast step over the Texas grid (~470×440 points at 3km).

Usage:
    # Single month
    from download_data.pull_hrrr import download_hrrr_month
    download_hrrr_month(2025, 7)

    # CLI
    uv run python -m download_data.pull_hrrr
"""

import os
import sys
import time
import calendar
import tempfile
from pathlib import Path

import requests
import numpy as np
import xarray as xr

sys.path.insert(0, str(Path(__file__).parent.parent))
from helper_funcs import setup_directories

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

S3_BUCKET = "noaa-hrrr-bdp-pds"
S3_BASE_URL = f"https://{S3_BUCKET}.s3.amazonaws.com"

# Texas geographic bounds (same as pull_ndfd.py)
TEXAS_LAT_MIN, TEXAS_LAT_MAX = 25.8, 36.5
TEXAS_LON_MIN, TEXAS_LON_MAX = -106.6, -93.5

# Initialization cycles (all 24 hourly runs)
ALL_CYCLES = list(range(24))

# Extended cycles go to f48; standard cycles go to f18.
# Currently unused but documented for future expansion to f24+ lead times.
EXTENDED_CYCLES = [0, 6, 12, 18]

# Lead times to download (hours). All cycles support up to f18.
LEAD_TIMES = [1, 18]

# Variables to extract from .idx (key = idx match string, value = internal name)
TARGET_VARIABLES = [
    "TMP:2 m above ground",
    "UGRD:10 m above ground",
    "VGRD:10 m above ground",
]

# Output element names (match NDFD convention for downstream compatibility)
ELEMENTS = ["temp", "wspd", "wdir"]


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------

def _build_s3_url(date_str, cycle_hour, lead_hour, ext=".grib2"):
    """Construct the S3 HTTPS URL for a HRRR surface file or its .idx.

    Args:
        date_str: 'YYYYMMDD'
        cycle_hour: 0–23
        lead_hour: 0–48
        ext: '.grib2' or '.grib2.idx'

    Returns:
        Full HTTPS URL string.
    """
    filename = f"hrrr.t{cycle_hour:02d}z.wrfsfcf{lead_hour:02d}{ext}"
    return f"{S3_BASE_URL}/hrrr.{date_str}/conus/{filename}"


# ---------------------------------------------------------------------------
# .idx parsing and byte-range computation
# ---------------------------------------------------------------------------

def _parse_idx(idx_text):
    """Parse a HRRR .idx file into a list of record dicts.

    Each line has format:
        {record}:{byte_offset}:d={YYYYMMDDHH}:{VAR}:{LEVEL}:{FCST_TYPE}:

    Returns:
        List of dicts with keys: record, byte_start, var_level (e.g. 'TMP:2 m above ground').
        byte_end is filled in from the next record's byte_start - 1.
    """
    records = []
    for line in idx_text.strip().split("\n"):
        parts = line.split(":")
        if len(parts) < 6:
            continue
        record_num = int(parts[0])
        byte_start = int(parts[1])
        var_name = parts[3]
        level = parts[4]
        records.append({
            "record": record_num,
            "byte_start": byte_start,
            "var_level": f"{var_name}:{level}",
        })

    # Compute byte_end from next record's start
    for i in range(len(records) - 1):
        records[i]["byte_end"] = records[i + 1]["byte_start"] - 1
    if records:
        records[-1]["byte_end"] = None  # last record: download to end of file

    return records


def _compute_byte_ranges(idx_records, target_vars):
    """Find byte ranges for target variables in parsed idx records.

    Args:
        idx_records: Output of _parse_idx().
        target_vars: List of strings like 'TMP:2 m above ground'.

    Returns:
        List of (var_level, byte_start, byte_end) tuples for matched variables.

    Raises:
        ValueError: If any target variable is not found.
    """
    ranges = []
    found = set()

    for rec in idx_records:
        if rec["var_level"] in target_vars:
            ranges.append((rec["var_level"], rec["byte_start"], rec["byte_end"]))
            found.add(rec["var_level"])

    missing = set(target_vars) - found
    if missing:
        raise ValueError(f"Variables not found in .idx: {missing}")

    return ranges


# ---------------------------------------------------------------------------
# Byte-range download
# ---------------------------------------------------------------------------

def _download_byte_range(url, byte_start, byte_end, max_retries=3):
    """Download a byte range from an HTTPS URL.

    Args:
        url: Full HTTPS URL to the GRIB2 file.
        byte_start: Starting byte (inclusive).
        byte_end: Ending byte (inclusive), or None for end-of-file.
        max_retries: Number of retry attempts with exponential backoff.

    Returns:
        Bytes content on success, None on failure.
    """
    range_str = f"bytes={byte_start}-"
    if byte_end is not None:
        range_str += str(byte_end)

    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers={"Range": range_str}, timeout=60)
            if resp.status_code in (200, 206):
                return resp.content
            elif resp.status_code == 404:
                return None
            else:
                print(f"    HTTP {resp.status_code} (attempt {attempt + 1}/{max_retries})")
        except requests.RequestException as e:
            print(f"    Request error (attempt {attempt + 1}/{max_retries}): {e}")

        if attempt < max_retries - 1:
            time.sleep(2 ** attempt)

    return None


def _download_variable_gribs(date_str, cycle_hour, lead_hour, tmp_dir):
    """Download the 3 target variable GRIB messages for one forecast file.

    Steps:
        1. Fetch the .idx file
        2. Parse byte ranges for TMP:2m, UGRD:10m, VGRD:10m
        3. Download each range and concatenate into one .grib2 file

    Returns:
        Path to the combined partial GRIB2 file, or None on failure.
    """
    # Fetch .idx
    idx_url = _build_s3_url(date_str, cycle_hour, lead_hour, ext=".grib2.idx")
    try:
        resp = requests.get(idx_url, timeout=30)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"    Failed to fetch .idx: {e}")
        return None

    # Parse and find byte ranges
    idx_records = _parse_idx(resp.text)
    try:
        ranges = _compute_byte_ranges(idx_records, TARGET_VARIABLES)
    except ValueError as e:
        print(f"    {e}")
        return None

    # Download each variable's bytes and concatenate
    grib2_url = _build_s3_url(date_str, cycle_hour, lead_hour)
    combined_path = os.path.join(
        tmp_dir, f"hrrr_{cycle_hour:02d}z_{date_str}_f{lead_hour:02d}.grib2"
    )

    with open(combined_path, "wb") as f:
        for var_level, byte_start, byte_end in ranges:
            data = _download_byte_range(grib2_url, byte_start, byte_end)
            if data is None:
                print(f"    Failed to download {var_level}")
                return None
            f.write(data)

    return combined_path


# ---------------------------------------------------------------------------
# Texas extraction and NetCDF saving
# ---------------------------------------------------------------------------

def _extract_texas_from_hrrr(grib_path, output_dirs, date_str, cycle_hour, lead_hour):
    """Extract Texas bounding box from a partial HRRR GRIB2 and save as NetCDF.

    Opens the GRIB2 file with cfgrib (filtered by variable), subsets the 2D
    Lambert Conformal grid to the Texas bounding box, computes wind speed and
    direction from U/V, constructs valid_time, and writes compressed NetCDF.

    Args:
        grib_path: Path to the combined partial GRIB2 file.
        output_dirs: Dict mapping element name → output directory path.
        date_str: 'YYYYMMDD'.
        cycle_hour: Initialization hour (0–23).
        lead_hour: Forecast lead time in hours.

    Returns:
        Number of files successfully saved (0–3).
    """
    output_filename = f"hrrr_{cycle_hour:02d}z_{date_str}_f{lead_hour:02d}.nc"

    try:
        # Open each variable separately using filter_by_keys
        ds_tmp = xr.open_dataset(
            grib_path, engine="cfgrib",
            backend_kwargs={"filter_by_keys": {"shortName": "2t"}},
        )
        ds_u = xr.open_dataset(
            grib_path, engine="cfgrib",
            backend_kwargs={"filter_by_keys": {"shortName": "10u"}},
        )
        ds_v = xr.open_dataset(
            grib_path, engine="cfgrib",
            backend_kwargs={"filter_by_keys": {"shortName": "10v"}},
        )
    except Exception as e:
        print(f"    Error opening GRIB: {e}")
        return 0

    try:
        # --- Compute Texas bounding box mask (use temperature grid) ----------
        lat = ds_tmp.latitude.values
        lon = ds_tmp.longitude.values

        if lat.ndim != 2:
            print(f"    Skipping: expected 2D grid, got {lat.ndim}D")
            return 0

        # Convert longitude from 0–360 to -180/180
        lon_180 = np.where(lon > 180, lon - 360, lon)

        texas_mask = (
            (lat >= TEXAS_LAT_MIN) & (lat <= TEXAS_LAT_MAX)
            & (lon_180 >= TEXAS_LON_MIN) & (lon_180 <= TEXAS_LON_MAX)
        )
        y_idx, x_idx = np.where(texas_mask)

        if len(y_idx) == 0:
            print(f"    No grid points in Texas bounding box")
            return 0

        y_slice = slice(y_idx.min(), y_idx.max() + 1)
        x_slice = slice(x_idx.min(), x_idx.max() + 1)

        # --- Construct time coordinates (all UTC) ----------------------------
        init_time = np.datetime64(
            f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}T{cycle_hour:02d}:00"
        )
        step = np.timedelta64(lead_hour, "h")
        valid_time = init_time + step

        lon_texas = lon_180[y_slice, x_slice]
        lat_texas = lat[y_slice, x_slice]

        # --- Temperature (t2m) -----------------------------------------------
        # Identify the temperature variable name cfgrib assigned
        temp_var = next(
            (name for name in ["t2m", "t", "tmp"] if name in ds_tmp.data_vars),
            list(ds_tmp.data_vars)[0],
        )
        temp_vals = ds_tmp[temp_var].values
        if temp_vals.ndim == 2:
            temp_texas = temp_vals[y_slice, x_slice]
        else:
            temp_texas = temp_vals[y_slice, x_slice]

        ds_temp_out = xr.Dataset(
            {"t2m": (["y", "x"], temp_texas.astype(np.float32))},
            coords={
                "latitude": (["y", "x"], lat_texas),
                "longitude": (["y", "x"], lon_texas),
                "time": init_time,
                "step": step,
                "valid_time": valid_time,
            },
            attrs={
                "source": "NOAA HRRR (3km)",
                "product": "wrfsfcf",
                "units": "K",
                "description": "2m temperature",
                "time_zone": "UTC",
            },
        )

        # --- Wind components (u10, v10) → speed and direction -----------------
        u_var = next(
            (name for name in ["u10", "10u", "ugrd"] if name in ds_u.data_vars),
            list(ds_u.data_vars)[0],
        )
        v_var = next(
            (name for name in ["v10", "10v", "vgrd"] if name in ds_v.data_vars),
            list(ds_v.data_vars)[0],
        )

        u_vals = ds_u[u_var].values
        v_vals = ds_v[v_var].values

        if u_vals.ndim == 2:
            u_texas = u_vals[y_slice, x_slice]
            v_texas = v_vals[y_slice, x_slice]
        else:
            u_texas = u_vals[y_slice, x_slice]
            v_texas = v_vals[y_slice, x_slice]

        wspd = np.sqrt(u_texas**2 + v_texas**2).astype(np.float32)
        wdir = ((270 - np.degrees(np.arctan2(v_texas, u_texas))) % 360).astype(np.float32)

        wind_coords = {
            "latitude": (["y", "x"], lat_texas),
            "longitude": (["y", "x"], lon_texas),
            "time": init_time,
            "step": step,
            "valid_time": valid_time,
        }
        wind_attrs_base = {
            "source": "NOAA HRRR (3km)",
            "product": "wrfsfcf",
            "time_zone": "UTC",
        }

        ds_wspd_out = xr.Dataset(
            {"si10": (["y", "x"], wspd)},
            coords=wind_coords,
            attrs={**wind_attrs_base, "units": "m/s",
                   "description": "10m wind speed (computed from U/V)"},
        )
        ds_wdir_out = xr.Dataset(
            {"wdir10": (["y", "x"], wdir)},
            coords=wind_coords,
            attrs={**wind_attrs_base, "units": "degrees",
                   "description": "10m wind direction (meteorological convention)"},
        )

        # --- Write compressed NetCDF -----------------------------------------
        encoding_f32 = {"zlib": True, "complevel": 5, "dtype": "float32"}
        saved = 0

        for element, ds_out, var_name in [
            ("temp", ds_temp_out, "t2m"),
            ("wspd", ds_wspd_out, "si10"),
            ("wdir", ds_wdir_out, "wdir10"),
        ]:
            out_path = os.path.join(output_dirs[element], output_filename)
            ds_out.to_netcdf(out_path, encoding={var_name: encoding_f32})
            saved += 1

        return saved

    except Exception as e:
        print(f"    Error extracting Texas: {e}")
        return 0

    finally:
        ds_tmp.close()
        ds_u.close()
        ds_v.close()


# ---------------------------------------------------------------------------
# Download orchestration
# ---------------------------------------------------------------------------

def download_hrrr_month(year, month, base_dir=None):
    """Download HRRR surface forecasts for one month and extract Texas.

    For each day × 24 cycles × 2 lead times:
        1. Check if all 3 output NetCDFs already exist (skip if so)
        2. Download partial GRIB via byte-range (.idx → Range request)
        3. Extract Texas, compute wind speed/direction, save 3 NetCDFs
        4. Clean up temp files

    Args:
        year: Calendar year.
        month: Calendar month (1–12).
        base_dir: Root output directory. Defaults to {raw}/hrrr_data.
    """
    if base_dir is None:
        dirs = setup_directories()
        base_dir = os.path.join(dirs["raw"], "hrrr_data")

    # Create output directories for each element
    output_dirs = {}
    for element in ELEMENTS:
        d = os.path.join(base_dir, element, str(year), f"{month:02d}")
        os.makedirs(d, exist_ok=True)
        output_dirs[element] = d

    num_days = calendar.monthrange(year, month)[1]
    total_success = 0
    total_skipped = 0
    total_failed = 0

    for day in range(1, num_days + 1):
        date_str = f"{year}{month:02d}{day:02d}"

        for cycle in ALL_CYCLES:
            for lead in LEAD_TIMES:
                output_filename = f"hrrr_{cycle:02d}z_{date_str}_f{lead:02d}.nc"

                # Skip if all 3 element files already exist
                all_exist = all(
                    os.path.exists(os.path.join(output_dirs[el], output_filename))
                    for el in ELEMENTS
                )
                if all_exist:
                    total_success += 1
                    continue

                # Download partial GRIB via byte-range
                with tempfile.TemporaryDirectory() as tmp:
                    grib_path = _download_variable_gribs(date_str, cycle, lead, tmp)

                    if grib_path is None:
                        print(f"  {year}-{month:02d}-{day:02d} {cycle:02d}z f{lead:02d}: "
                              f"download failed")
                        total_failed += 1
                        continue

                    saved = _extract_texas_from_hrrr(
                        grib_path, output_dirs, date_str, cycle, lead,
                    )

                    if saved == 3:
                        total_success += 1
                    else:
                        print(f"  {year}-{month:02d}-{day:02d} {cycle:02d}z f{lead:02d}: "
                              f"only {saved}/3 elements saved")
                        total_failed += 1

                # Brief pause to avoid S3 throttling
                time.sleep(0.1)

        # Daily progress
        day_total = len(ALL_CYCLES) * len(LEAD_TIMES)
        print(f"  {year}-{month:02d}-{day:02d}: processed {day_total} forecasts")

    # Summary
    total = num_days * len(ALL_CYCLES) * len(LEAD_TIMES)
    print(f"\n  HRRR {year}-{month:02d}: "
          f"{total_success} successful, {total_skipped} skipped, "
          f"{total_failed} failed (of {total} total)")

    for element in ELEMENTS:
        nc_files = list(Path(output_dirs[element]).glob("hrrr_*.nc"))
        if nc_files:
            size_mb = sum(f.stat().st_size for f in nc_files) / (1024 * 1024)
            print(f"  {element}: {len(nc_files)} files, {size_mb:.1f} MB")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    dirs = setup_directories()

    year = 2025
    month = 7
    base_dir = os.path.join(dirs["raw"], "hrrr_data")

    num_days = calendar.monthrange(year, month)[1]
    total_files = num_days * len(ALL_CYCLES) * len(LEAD_TIMES)

    print(f"HRRR Download Configuration")
    print(f"  Period: {year}-{month:02d} ({num_days} days)")
    print(f"  Cycles: All 24 hourly (00z–23z)")
    print(f"  Lead times: {LEAD_TIMES} hours")
    print(f"  Variables: TMP:2m, UGRD:10m, VGRD:10m → temp, wspd, wdir")
    print(f"  Method: Byte-range download (~6 MB per file vs ~150 MB full)")
    print(f"  Estimated downloads: {total_files} GRIB files")
    print(f"  Estimated download size: ~{total_files * 6 / 1024:.1f} GB")
    print(f"  Output: {base_dir}")
    print(f"  Output files: {total_files} per element × 3 elements = {total_files * 3}")
    print(f"  Texas extraction: ENABLED")
    print(f"  Time zone: UTC")

    response = input("\nProceed with download? (yes/no): ")
    if response.lower() not in ["yes", "y"]:
        print("Download cancelled.")
        return

    download_hrrr_month(year, month, base_dir)

    print("\n=== Download Complete ===")
    print(f"Data saved to: {base_dir}")


if __name__ == "__main__":
    main()
