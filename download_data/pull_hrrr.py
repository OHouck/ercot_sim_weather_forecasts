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

Output format (combined — space-efficient):
  {base_dir}/{year}/{month:02d}/hrrr_{HH}z_{YYYYMMDD}.nc
  Each file covers both lead times for one (cycle, day), storing t2m, si10,
  wdir10 in a single NetCDF with dims (lead_hour, y, x). lat/lon coordinates
  are stored once instead of being duplicated across 6 per-element files.
  This reduces storage from ~4,464 files/month to 744 files/month.

This data is very large, so after processing forecast errors, delete the raw files.

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

# Texas geographic bounds
TEXAS_LAT_MIN, TEXAS_LAT_MAX = 25.8, 36.5
TEXAS_LON_MIN, TEXAS_LON_MAX = -106.6, -93.5

# Initialization cycles (all 24 hourly runs)
ALL_CYCLES = list(range(24))

# Lead times to download (hours). All cycles support up to f18.
LEAD_TIMES = [1, 18]

# Variables to extract from .idx (key = idx match string)
TARGET_VARIABLES = [
    "TMP:2 m above ground",
    "UGRD:10 m above ground",
    "VGRD:10 m above ground",
]


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------

def _build_s3_url(date_str, cycle_hour, lead_hour, ext=".grib2"):
    """Construct the S3 HTTPS URL for a HRRR surface file or its .idx."""
    filename = f"hrrr.t{cycle_hour:02d}z.wrfsfcf{lead_hour:02d}{ext}"
    return f"{S3_BASE_URL}/hrrr.{date_str}/conus/{filename}"


# ---------------------------------------------------------------------------
# .idx parsing and byte-range computation
# ---------------------------------------------------------------------------

def _parse_idx(idx_text):
    """Parse a HRRR .idx file into a list of record dicts.

    Each line has format:
        {record}:{byte_offset}:d={YYYYMMDDHH}:{VAR}:{LEVEL}:{FCST_TYPE}:

    Returns list of dicts with keys: record, byte_start, byte_end, var_level.
    """
    records = []
    for line in idx_text.strip().split("\n"):
        parts = line.split(":")
        if len(parts) < 6:
            continue
        records.append({
            "record": int(parts[0]),
            "byte_start": int(parts[1]),
            "var_level": f"{parts[3]}:{parts[4]}",
        })

    for i in range(len(records) - 1):
        records[i]["byte_end"] = records[i + 1]["byte_start"] - 1
    if records:
        records[-1]["byte_end"] = None

    return records


def _compute_byte_ranges(idx_records, target_vars):
    """Find byte ranges for target variables in parsed idx records.

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
    """Download a byte range from an HTTPS URL. Returns bytes or None on failure."""
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

    Returns path to the combined partial GRIB2 file, or None on failure.
    """
    idx_url = _build_s3_url(date_str, cycle_hour, lead_hour, ext=".grib2.idx")
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
# Texas extraction (returns field arrays, does not save)
# ---------------------------------------------------------------------------

def _extract_texas_fields(grib_path, date_str, cycle_hour, lead_hour):
    """Extract Texas bounding box from a partial HRRR GRIB2.

    Opens the GRIB2 file with cfgrib, subsets the 2D Lambert Conformal grid
    to the Texas bounding box, computes wind speed/direction from U/V.

    Returns:
        Dict with keys: lat2d, lon2d, t2m, si10, wdir10, init_time, valid_time
        All spatial arrays are float32, shape (ny, nx).
        Returns None on any error.
    """
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
        return None

    try:
        lat = ds_tmp.latitude.values
        lon = ds_tmp.longitude.values

        if lat.ndim != 2:
            print(f"    Skipping: expected 2D grid, got {lat.ndim}D")
            return None

        lon_180 = np.where(lon > 180, lon - 360, lon)

        texas_mask = (
            (lat >= TEXAS_LAT_MIN) & (lat <= TEXAS_LAT_MAX)
            & (lon_180 >= TEXAS_LON_MIN) & (lon_180 <= TEXAS_LON_MAX)
        )
        y_idx, x_idx = np.where(texas_mask)

        if len(y_idx) == 0:
            print("    No grid points in Texas bounding box")
            return None

        y_slice = slice(y_idx.min(), y_idx.max() + 1)
        x_slice = slice(x_idx.min(), x_idx.max() + 1)

        init_time = np.datetime64(
            f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}T{cycle_hour:02d}:00"
        )
        valid_time = init_time + np.timedelta64(lead_hour, "h")

        lat2d = lat[y_slice, x_slice]
        lon2d = lon_180[y_slice, x_slice]

        # Temperature
        temp_var = next(
            (n for n in ["t2m", "t", "tmp"] if n in ds_tmp.data_vars),
            list(ds_tmp.data_vars)[0],
        )
        t2m = ds_tmp[temp_var].values[y_slice, x_slice].astype(np.float32)

        # Wind components → speed and direction
        u_var = next(
            (n for n in ["u10", "10u", "ugrd"] if n in ds_u.data_vars),
            list(ds_u.data_vars)[0],
        )
        v_var = next(
            (n for n in ["v10", "10v", "vgrd"] if n in ds_v.data_vars),
            list(ds_v.data_vars)[0],
        )
        u = ds_u[u_var].values[y_slice, x_slice]
        v = ds_v[v_var].values[y_slice, x_slice]

        si10 = np.sqrt(u**2 + v**2).astype(np.float32)
        wdir10 = ((270 - np.degrees(np.arctan2(v, u))) % 360).astype(np.float32)

        return {
            "lat2d": lat2d,
            "lon2d": lon2d,
            "t2m": t2m,
            "si10": si10,
            "wdir10": wdir10,
            "init_time": init_time,
            "valid_time": valid_time,
        }

    except Exception as e:
        print(f"    Error extracting Texas fields: {e}")
        return None

    finally:
        ds_tmp.close()
        ds_u.close()
        ds_v.close()


# ---------------------------------------------------------------------------
# Combined NetCDF saving
# ---------------------------------------------------------------------------

def _save_combined_nc(output_path, all_lead_data):
    """Save a combined NetCDF with all variables and lead times for one (day, cycle).

    All variables (t2m, si10, wdir10) and all lead times are packed into a
    single file with dims (lead_hour, y, x). lat/lon coordinates are stored
    once, eliminating the redundancy of the old per-element file format.

    Args:
        output_path: Full path to the output .nc file.
        all_lead_data: Dict mapping lead_hour (int) → field dict from
                       _extract_texas_fields().
    """
    leads_sorted = sorted(all_lead_data.keys())
    first = all_lead_data[leads_sorted[0]]

    t2m = np.stack([all_lead_data[lh]["t2m"] for lh in leads_sorted], axis=0)
    si10 = np.stack([all_lead_data[lh]["si10"] for lh in leads_sorted], axis=0)
    wdir10 = np.stack([all_lead_data[lh]["wdir10"] for lh in leads_sorted], axis=0)

    valid_times = np.array(
        [all_lead_data[lh]["valid_time"] for lh in leads_sorted],
        dtype="datetime64[ns]",
    )

    ds = xr.Dataset(
        {
            "t2m":    (["lead_hour", "y", "x"], t2m),
            "si10":   (["lead_hour", "y", "x"], si10),
            "wdir10": (["lead_hour", "y", "x"], wdir10),
        },
        coords={
            "latitude":   (["y", "x"], first["lat2d"]),
            "longitude":  (["y", "x"], first["lon2d"]),
            "time":       first["init_time"],
            "lead_hour":  ("lead_hour", np.array(leads_sorted, dtype=np.int32)),
            "valid_time": ("lead_hour", valid_times),
        },
        attrs={
            "source": "NOAA HRRR (3km)",
            "product": "wrfsfcf",
            "time_zone": "UTC",
            "variables": "t2m [K], si10 [m/s], wdir10 [degrees meteorological]",
            "lead_hours": str(leads_sorted),
        },
    )

    encoding = {v: {"zlib": True, "complevel": 5, "dtype": "float32"}
                for v in ["t2m", "si10", "wdir10"]}
    ds.to_netcdf(output_path, encoding=encoding)


# ---------------------------------------------------------------------------
# Download orchestration
# ---------------------------------------------------------------------------

def download_hrrr_month(year, month, base_dir=None):
    """Download HRRR surface forecasts for one month and extract Texas.

    For each day × 24 cycles:
        1. Check if combined output NetCDF already exists (skip if so)
        2. For each lead time: download partial GRIB via byte-range
        3. Extract Texas and compute wind speed/direction from each lead GRIB
        4. Write one combined NetCDF with all variables and lead times
        5. Clean up temp files

    Output files:
        {base_dir}/{year}/{month:02d}/hrrr_{HH}z_{YYYYMMDD}.nc
        ~744 files/month (vs 4,464 in the old per-element format)

    Args:
        year: Calendar year.
        month: Calendar month (1–12).
        base_dir: Root output directory. Defaults to {raw}/hrrr_data.
    """
    if base_dir is None:
        dirs = setup_directories()
        base_dir = os.path.join(dirs["raw"], "hrrr_data")

    output_dir = os.path.join(base_dir, str(year), f"{month:02d}")
    os.makedirs(output_dir, exist_ok=True)

    num_days = calendar.monthrange(year, month)[1]
    total_success = 0
    total_failed = 0

    for day in range(1, num_days + 1):
        date_str = f"{year}{month:02d}{day:02d}"

        for cycle in ALL_CYCLES:
            output_filename = f"hrrr_{cycle:02d}z_{date_str}.nc"
            output_path = os.path.join(output_dir, output_filename)

            if os.path.exists(output_path):
                total_success += 1
                continue

            all_lead_data = {}
            with tempfile.TemporaryDirectory() as tmp:
                failed = False
                for lead in LEAD_TIMES:
                    grib_path = _download_variable_gribs(date_str, cycle, lead, tmp)
                    if grib_path is None:
                        print(f"  {year}-{month:02d}-{day:02d} {cycle:02d}z f{lead:02d}: "
                              f"download failed")
                        failed = True
                        break

                    fields = _extract_texas_fields(grib_path, date_str, cycle, lead)
                    if fields is None:
                        print(f"  {year}-{month:02d}-{day:02d} {cycle:02d}z f{lead:02d}: "
                              f"extraction failed")
                        failed = True
                        break

                    all_lead_data[lead] = fields

                if failed or len(all_lead_data) != len(LEAD_TIMES):
                    total_failed += 1
                    continue

                try:
                    _save_combined_nc(output_path, all_lead_data)
                    total_success += 1
                except Exception as e:
                    print(f"  {year}-{month:02d}-{day:02d} {cycle:02d}z: save failed — {e}")
                    if os.path.exists(output_path):
                        os.remove(output_path)
                    total_failed += 1

            time.sleep(0.1)

        print(f"  {year}-{month:02d}-{day:02d}: processed {len(ALL_CYCLES)} cycles")

    total = num_days * len(ALL_CYCLES)
    print(f"\n  HRRR {year}-{month:02d}: "
          f"{total_success} successful, {total_failed} failed (of {total} total)")

    nc_files = list(Path(output_dir).glob("hrrr_*.nc"))
    if nc_files:
        size_mb = sum(f.stat().st_size for f in nc_files) / (1024 * 1024)
        print(f"  {len(nc_files)} combined files, {size_mb:.1f} MB total")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    dirs = setup_directories()

    year = 2025
    month = 7
    base_dir = os.path.join(dirs["raw"], "hrrr_data")

    num_days = calendar.monthrange(year, month)[1]
    n_cycles = len(ALL_CYCLES)
    total_files = num_days * n_cycles

    print(f"HRRR Download Configuration")
    print(f"  Period: {year}-{month:02d} ({num_days} days)")
    print(f"  Cycles: All 24 hourly (00z–23z)")
    print(f"  Lead times: {LEAD_TIMES} hours")
    print(f"  Variables: TMP:2m, UGRD:10m, VGRD:10m → t2m, si10, wdir10")
    print(f"  Method: Byte-range download (~6 MB per GRIB vs ~150 MB full)")
    print(f"  Output format: 1 combined NetCDF per (day, cycle) — all vars + leads")
    print(f"  Output files: {total_files} (vs {total_files * 6} in old per-element format)")
    print(f"  Output: {base_dir}/{year}/{month:02d}/")

    response = input("\nProceed with download? (yes/no): ")
    if response.lower() not in ["yes", "y"]:
        print("Download cancelled.")
        return

    download_hrrr_month(year, month, base_dir)

    print("\n=== Download Complete ===")
    print(f"Data saved to: {base_dir}/{year}/{month:02d}/")


if __name__ == "__main__":
    main()
