"""pull_gfs.py — Download GFS 0.25-degree weather forecasts from AWS S3 and extract Texas.

Downloads GFS global forecast GRIB2 files from the NOAA GFS archive on S3
(bucket: noaa-gfs-bdp-pds), using byte-range requests to download only
TMP:2m, UGRD:10m, VGRD:10m fields (~3 MB vs ~300 MB per file). Extracts the
Texas bounding box, computes wind speed/direction from U/V components, and
saves as compressed NetCDF.

GFS runs 4 initializations per day (00z, 06z, 12z, 18z). This script downloads
only the 12z cycle with lead times f018–f041 (24 steps per day). One NetCDF
file per (element, lead time, day).

All times are in UTC.

S3 path pattern:
    gfs.{YYYYMMDD}/12/atmos/gfs.t12z.pgrb2.0p25.f{FFF}[.idx]

Output:
    {base_dir}/{element}/{year}/{month:02d}/gfs_12z_{YYYYMMDD}_f{FFF:03d}.nc
    Elements: temp (t2m [K]), wspd (si10 [m/s]), wdir (wdir10 [degrees])

Usage:
    # Single month
    from download_data.pull_gfs import download_gfs_month
    download_gfs_month(2025, 7)

    # CLI (prompts for confirmation)
    uv run python -m download_data.pull_gfs
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

S3_BUCKET = "noaa-gfs-bdp-pds"
S3_BASE_URL = f"https://{S3_BUCKET}.s3.amazonaws.com"

# Texas geographic bounds (consistent with NDFD/HRRR/ERA5 scripts)
TEXAS_LAT_MIN, TEXAS_LAT_MAX = 25.8, 36.5
TEXAS_LON_MIN, TEXAS_LON_MAX = -106.6, -93.5

# GFS uses 0–360 longitude; convert Texas bounds
TEXAS_LON_MIN_360 = TEXAS_LON_MIN + 360.0  # 253.4
TEXAS_LON_MAX_360 = TEXAS_LON_MAX + 360.0  # 266.5

# Initialization cycle: only 12z UTC
CYCLE = 12

# Lead times to download: hours 18 through 41 (inclusive)
LEAD_TIMES = list(range(18, 42))  # [18, 19, ..., 41]

# Variables to extract — match strings used in GFS .idx files
TARGET_VARIABLES = [
    "TMP:2 m above ground",
    "UGRD:10 m above ground",
    "VGRD:10 m above ground",
]

# Output element names (consistent with NDFD/HRRR convention)
ELEMENTS = ["temp", "wspd", "wdir"]


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------

def _build_s3_url(date_str, lead_hour, ext=""):
    """Construct the S3 HTTPS URL for a GFS pgrb2 0.25-degree file or its .idx.

    GFS files have no file extension (e.g. gfs.t12z.pgrb2.0p25.f018);
    index files append '.idx'.

    Args:
        date_str: 'YYYYMMDD'
        lead_hour: 0–384
        ext: '' for GRIB2 data file, '.idx' for the index file

    Returns:
        Full HTTPS URL string.
    """
    filename = f"gfs.t{CYCLE:02d}z.pgrb2.0p25.f{lead_hour:03d}{ext}"
    return f"{S3_BASE_URL}/gfs.{date_str}/{CYCLE:02d}/atmos/{filename}"


# ---------------------------------------------------------------------------
# .idx parsing and byte-range computation
# ---------------------------------------------------------------------------

def _parse_idx(idx_text):
    """Parse a GFS .idx file into a list of record dicts.

    Each line has format:
        {record}:{byte_offset}:d={YYYYMMDDHH}:{VAR}:{LEVEL}:{FCST_TYPE}:

    Returns:
        List of dicts with keys: record, byte_start, var_level, byte_end.
        byte_end is the byte before the next record's start; None for the last record.
    """
    records = []
    for line in idx_text.strip().split("\n"):
        parts = line.split(":")
        if len(parts) < 6:
            continue
        try:
            record_num = int(parts[0])
            byte_start = int(parts[1])
        except ValueError:
            continue
        var_name = parts[3]
        level = parts[4]
        records.append({
            "record": record_num,
            "byte_start": byte_start,
            "var_level": f"{var_name}:{level}",
        })

    for i in range(len(records) - 1):
        records[i]["byte_end"] = records[i + 1]["byte_start"] - 1
    if records:
        records[-1]["byte_end"] = None  # last record: read to EOF

    return records


def _compute_byte_ranges(idx_records, target_vars):
    """Find byte ranges for target variables in parsed idx records.

    Args:
        idx_records: Output of _parse_idx().
        target_vars: List of strings like 'TMP:2 m above ground'.

    Returns:
        List of (var_level, byte_start, byte_end) tuples for matched variables.

    Raises:
        ValueError: If any target variable is not found in the index.
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
    """Download a byte range from an HTTPS URL with exponential backoff.

    Args:
        url: Full HTTPS URL to the GRIB2 file.
        byte_start: Starting byte (inclusive).
        byte_end: Ending byte (inclusive), or None for end-of-file.
        max_retries: Number of retry attempts.

    Returns:
        Bytes content on success, None on failure (including 404).
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


def _download_variable_gribs(date_str, lead_hour, tmp_dir):
    """Download the 3 target variable GRIB messages for one GFS forecast step.

    Steps:
        1. Fetch the .idx file to find byte offsets
        2. Parse byte ranges for TMP:2m, UGRD:10m, VGRD:10m
        3. Download each byte range and concatenate into one .grib2 file

    Args:
        date_str: 'YYYYMMDD'
        lead_hour: Forecast lead time in hours (18–41).
        tmp_dir: Temporary directory for the intermediate GRIB2 file.

    Returns:
        Path to the combined partial GRIB2 file, or None on any failure.
    """
    idx_url = _build_s3_url(date_str, lead_hour, ext=".idx")
    try:
        resp = requests.get(idx_url, timeout=30)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"    Failed to fetch .idx: {e}")
        return None

    idx_records = _parse_idx(resp.text)
    try:
        ranges = _compute_byte_ranges(idx_records, TARGET_VARIABLES)
    except ValueError as e:
        print(f"    {e}")
        return None

    grib2_url = _build_s3_url(date_str, lead_hour)
    combined_path = os.path.join(
        tmp_dir, f"gfs_{CYCLE:02d}z_{date_str}_f{lead_hour:03d}.grib2"
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

def _extract_texas_from_gfs(grib_path, output_dirs, date_str, lead_hour):
    """Extract Texas bounding box from a partial GFS GRIB2 and save as NetCDF.

    GFS uses a regular 0.25-degree lat/lon grid with:
        - latitude:  90.0 → -90.0 (decreasing, 721 points)
        - longitude: 0.0  → 359.75 (0-360 convention, 1440 points)

    Converts longitude to -180/180 convention on output for consistency
    with NDFD/HRRR/ERA5 data in this project.

    Args:
        grib_path: Path to the combined partial GRIB2 file.
        output_dirs: Dict mapping element name → output directory path.
        date_str: 'YYYYMMDD'.
        lead_hour: Forecast lead time in hours.

    Returns:
        Number of elements successfully saved (0–3).
    """
    output_filename = f"gfs_{CYCLE:02d}z_{date_str}_f{lead_hour:03d}.nc"

    try:
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
        # --- Subset to Texas (GFS lat/lon are 1D regular arrays) --------------
        # latitude:  1D, values ~90.0 → ~-90.0 (decreasing)
        # longitude: 1D, values 0.0 → 359.75  (0-360)
        lat = ds_tmp.latitude.values   # shape: (721,)
        lon = ds_tmp.longitude.values  # shape: (1440,)

        lat_mask = (lat >= TEXAS_LAT_MIN) & (lat <= TEXAS_LAT_MAX)
        lon_mask = (lon >= TEXAS_LON_MIN_360) & (lon <= TEXAS_LON_MAX_360)

        lat_texas = lat[lat_mask]
        lon_texas = lon[lon_mask]

        if len(lat_texas) == 0 or len(lon_texas) == 0:
            print(f"    No grid points found in Texas bounding box")
            return 0

        # Convert output longitude to -180/180 for consistency with other datasets
        lon_texas_180 = lon_texas - 360.0

        # --- Time coordinates -------------------------------------------------
        init_time = np.datetime64(
            f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}T{CYCLE:02d}:00"
        )
        step = np.timedelta64(lead_hour, "h")
        valid_time = init_time + step

        # --- Temperature (t2m) ------------------------------------------------
        temp_var = next(
            (name for name in ["t2m", "t"] if name in ds_tmp.data_vars),
            list(ds_tmp.data_vars)[0],
        )
        temp_vals = ds_tmp[temp_var].values  # shape: (721, 1440)
        temp_texas = temp_vals[lat_mask, :][:, lon_mask]  # (n_lat_TX, n_lon_TX)

        ds_temp_out = xr.Dataset(
            {"t2m": (["latitude", "longitude"], temp_texas.astype(np.float32))},
            coords={
                "latitude": lat_texas,
                "longitude": lon_texas_180,
                "time": init_time,
                "step": step,
                "valid_time": valid_time,
            },
            attrs={
                "source": "NOAA GFS (0.25-degree)",
                "product": "pgrb2.0p25",
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

        u_texas = ds_u[u_var].values[lat_mask, :][:, lon_mask]
        v_texas = ds_v[v_var].values[lat_mask, :][:, lon_mask]

        wspd = np.sqrt(u_texas**2 + v_texas**2).astype(np.float32)
        # Meteorological convention: direction wind is coming FROM
        wdir = ((270 - np.degrees(np.arctan2(v_texas, u_texas))) % 360).astype(np.float32)

        wind_coords = {
            "latitude": lat_texas,
            "longitude": lon_texas_180,
            "time": init_time,
            "step": step,
            "valid_time": valid_time,
        }
        wind_attrs_base = {
            "source": "NOAA GFS (0.25-degree)",
            "product": "pgrb2.0p25",
            "time_zone": "UTC",
        }

        ds_wspd_out = xr.Dataset(
            {"si10": (["latitude", "longitude"], wspd)},
            coords=wind_coords,
            attrs={**wind_attrs_base, "units": "m/s",
                   "description": "10m wind speed (computed from U/V)"},
        )
        ds_wdir_out = xr.Dataset(
            {"wdir10": (["latitude", "longitude"], wdir)},
            coords=wind_coords,
            attrs={**wind_attrs_base, "units": "degrees",
                   "description": "10m wind direction (meteorological convention)"},
        )

        # --- Write compressed NetCDF ------------------------------------------
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

def download_gfs_month(year, month, base_dir=None):
    """Download GFS 12z forecasts for one month and extract Texas.

    For each day × 25 lead times (f018–f041):
        1. Check if all 3 output NetCDFs already exist (skip if so)
        2. Fetch .idx to find byte offsets for TMP:2m, UGRD:10m, VGRD:10m
        3. Download byte ranges and concatenate into a partial GRIB2 file
        4. Extract Texas, compute wind speed/direction, save 3 NetCDFs
        5. Clean up temp files

    Args:
        year: Calendar year.
        month: Calendar month (1–12).
        base_dir: Root output directory. Defaults to {raw}/gfs_data.

    Output structure:
        {base_dir}/temp/{year}/{month:02d}/gfs_12z_{YYYYMMDD}_f{FFF:03d}.nc
        {base_dir}/wspd/{year}/{month:02d}/gfs_12z_{YYYYMMDD}_f{FFF:03d}.nc
        {base_dir}/wdir/{year}/{month:02d}/gfs_12z_{YYYYMMDD}_f{FFF:03d}.nc
    """
    if base_dir is None:
        dirs = setup_directories()
        base_dir = os.path.join(dirs["raw"], "gfs_data")

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
        day_success = 0
        day_skipped = 0
        day_failed = 0

        for lead in LEAD_TIMES:
            output_filename = f"gfs_{CYCLE:02d}z_{date_str}_f{lead:03d}.nc"

            # Skip if all 3 element files already exist
            all_exist = all(
                os.path.exists(os.path.join(output_dirs[el], output_filename))
                for el in ELEMENTS
            )
            if all_exist:
                day_skipped += 1
                total_skipped += 1
                continue

            with tempfile.TemporaryDirectory() as tmp:
                grib_path = _download_variable_gribs(date_str, lead, tmp)

                if grib_path is None:
                    print(f"  {date_str} f{lead:03d}: download failed")
                    day_failed += 1
                    total_failed += 1
                    continue

                saved = _extract_texas_from_gfs(grib_path, output_dirs, date_str, lead)

                if saved == 3:
                    day_success += 1
                    total_success += 1
                else:
                    print(f"  {date_str} f{lead:03d}: only {saved}/3 elements saved")
                    day_failed += 1
                    total_failed += 1

            time.sleep(0.1)  # brief pause to avoid S3 throttling

        status_parts = [f"{day_success} ok"]
        if day_skipped:
            status_parts.append(f"{day_skipped} skipped")
        if day_failed:
            status_parts.append(f"{day_failed} failed")
        print(f"  {date_str}: {', '.join(status_parts)} (of {len(LEAD_TIMES)} lead times)")

    total = num_days * len(LEAD_TIMES)
    print(f"\n  GFS {year}-{month:02d}: "
          f"{total_success} successful, {total_skipped} skipped, "
          f"{total_failed} failed (of {total} total)")

    for element in ELEMENTS:
        nc_files = list(Path(output_dirs[element]).glob("gfs_*.nc"))
        if nc_files:
            size_mb = sum(f.stat().st_size for f in nc_files) / (1024 * 1024)
            print(f"  {element}: {len(nc_files)} files, {size_mb:.1f} MB")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    dirs = setup_directories()

    year = 2025
    month = 2
    base_dir = os.path.join(dirs["raw"], "gfs_data")

    num_days = calendar.monthrange(year, month)[1]
    total_files = num_days * len(LEAD_TIMES)

    print("GFS Download Configuration")
    print(f"  Period:        {year}-{month:02d} ({num_days} days)")
    print(f"  Cycle:         {CYCLE:02d}z (12 UTC only)")
    print(f"  Lead times:    f{LEAD_TIMES[0]:03d}–f{LEAD_TIMES[-1]:03d} "
          f"({len(LEAD_TIMES)} steps per day)")
    print(f"  Variables:     TMP:2m, UGRD:10m, VGRD:10m → temp, wspd, wdir")
    print(f"  Grid:          0.25-degree global lat/lon (GFS pgrb2.0p25)")
    print(f"  Method:        Byte-range download (~3 MB per file vs ~300 MB full)")
    print(f"  S3 bucket:     {S3_BUCKET}")
    print(f"  Downloads:     {total_files} GRIB files × 3 byte-ranges each")
    print(f"  Output files:  {total_files} per element × 3 elements = {total_files * 3}")
    print(f"  Output dir:    {base_dir}")
    print(f"  Texas grid:    ~44 lat × 53 lon points at 0.25°")

    response = input("\nProceed with download? (yes/no): ")
    if response.lower() not in ["yes", "y"]:
        print("Download cancelled.")
        return

    download_gfs_month(year, month, base_dir)

    print("\n=== Download Complete ===")
    print(f"Data saved to: {base_dir}")


if __name__ == "__main__":
    main()
