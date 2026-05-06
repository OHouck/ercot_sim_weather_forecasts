"""pull_hrrr.py — Download HRRR 3km weather forecasts from AWS S3 and extract Texas.

Downloads HRRR surface (wrfsfcf) GRIB2 files from the NOAA HRRR archive on S3,
using byte-range requests to download only TMP:2m, UGRD:10m, VGRD:10m,
UGRD:80m, VGRD:80m fields (~8 MB vs ~150 MB per file). Extracts the Texas
bounding box, computes wind speed/direction from U/V components, and saves as
compressed NetCDF.

All times are in UTC. HRRR runs 24 initializations per day (00z–23z).
Standard cycles (all 24) produce forecasts to 18h lead time.
Extended cycles (00z, 06z, 12z, 18z) produce forecasts to 48h lead time.
Currently downloads f01 and f18 from all 24 cycles; extend to f24+ by
changing LEAD_TIMES and adding EXTENDED_CYCLES logic.

100m wind speed is not available in HRRR wrfsfcf files. It is estimated using
the wind profile power law from 10m and 80m wind speeds:
    alpha = ln(si80 / si10) / ln(80 / 10)
    si100 = si80 * (100 / 80) ** alpha
where alpha is the Hellmann exponent. When either speed is < 0.1 m/s,
alpha falls back to 1/7 (neutral-stability open terrain default).
Wind direction at 100m equals direction at 80m (power law preserves direction).

Output format (combined — space-efficient):
  {base_dir}/{year}/{month:02d}/hrrr_{HH}z_{YYYYMMDD}.nc
  Each file covers both lead times for one (cycle, day), storing t2m, si10,
  wdir10, si80, wdir80, si100, wdir100, alpha in a single NetCDF with dims
  (lead_hour, y, x). lat/lon coordinates are stored once instead of being
  duplicated across per-element files.

This data is very large, so after processing forecast errors, delete the raw files.

Usage:
    # Single month
    from download_data.pull_hrrr import download_hrrr_month
    download_hrrr_month(2025, 7)

    # Backfill 100m wind on existing files (adds si80/alpha/si100/wdir100)
    from download_data.pull_hrrr import patch_hrrr_100m_wind_month
    patch_hrrr_100m_wind_month(2025, 7)

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
    "UGRD:80 m above ground",
    "VGRD:80 m above ground",
]

# Wind profile power law heights (metres)
_Z_LOW = 10.0
_Z_HIGH = 80.0
_Z_TARGET = 100.0
_ALPHA_DEFAULT = 1.0 / 7.0  # neutral stability over open terrain
_MIN_SPEED = 0.1  # m/s — use default alpha below this


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

    Args:
        idx_records: Output of _parse_idx().
        target_vars: List of var_level strings to match.

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


def _download_variable_gribs(date_str, cycle_hour, lead_hour, tmp_dir,
                              target_vars=None):
    """Download target variable GRIB messages for one forecast file.

    Args:
        date_str: 'YYYYMMDD'
        cycle_hour: UTC initialization hour (0–23).
        lead_hour: Forecast lead time in hours.
        tmp_dir: Temporary directory for the intermediate GRIB2 file.
        target_vars: Variables to download; defaults to TARGET_VARIABLES.

    Returns:
        Path to the combined partial GRIB2 file, or None on failure.
    """
    if target_vars is None:
        target_vars = TARGET_VARIABLES

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
        ranges = _compute_byte_ranges(idx_records, target_vars)
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
# GRIB / grid helpers
# ---------------------------------------------------------------------------

def _get_var(ds, candidates):
    """Return first name from candidates found in ds.data_vars, else first var."""
    return next((n for n in candidates if n in ds.data_vars), list(ds.data_vars)[0])


def _texas_slices(lat, lon):
    """Return (y_slice, x_slice) for Texas bounding box on a 2D HRRR grid.

    Args:
        lat: 2D latitude array.
        lon: 2D longitude array in -180/180 convention.

    Returns:
        (y_slice, x_slice), or (None, None) if no grid points found.
    """
    mask = (
        (lat >= TEXAS_LAT_MIN) & (lat <= TEXAS_LAT_MAX)
        & (lon >= TEXAS_LON_MIN) & (lon <= TEXAS_LON_MAX)
    )
    y_idx, x_idx = np.where(mask)
    if len(y_idx) == 0:
        return None, None
    return slice(y_idx.min(), y_idx.max() + 1), slice(x_idx.min(), x_idx.max() + 1)


def _open_wind_at_height(grib_path, level):
    """Open cfgrib datasets for U and V wind at a specific height above ground.

    Args:
        grib_path: Path to a (partial) GRIB2 file.
        level: Height above ground in metres (e.g. 10 or 80).

    Returns:
        Tuple (ds_u, ds_v).
    """
    filt = {"typeOfLevel": "heightAboveGround", "level": level}
    ds_u = xr.open_dataset(
        grib_path, engine="cfgrib",
        backend_kwargs={"filter_by_keys": {**filt, "shortName": "u"}},
    )
    ds_v = xr.open_dataset(
        grib_path, engine="cfgrib",
        backend_kwargs={"filter_by_keys": {**filt, "shortName": "v"}},
    )
    return ds_u, ds_v


# ---------------------------------------------------------------------------
# Power law helpers
# ---------------------------------------------------------------------------

def _compute_power_law_alpha(si_low, si_high, z_low=_Z_LOW, z_high=_Z_HIGH):
    """Compute per-pixel Hellmann exponent from two wind speed measurements.

    alpha = ln(si_high / si_low) / ln(z_high / z_low)

    Falls back to _ALPHA_DEFAULT (1/7) where either speed is below _MIN_SPEED.

    Args:
        si_low: Wind speed array at z_low [m/s].
        si_high: Wind speed array at z_high [m/s].
        z_low: Height of lower measurement [m].
        z_high: Height of upper measurement [m].

    Returns:
        alpha array (float32), same shape as inputs.
    """
    valid = (si_low >= _MIN_SPEED) & (si_high >= _MIN_SPEED)
    ratio = np.where(valid, si_high / si_low, 1.0)
    return np.where(
        valid,
        np.log(ratio) / np.log(z_high / z_low),
        _ALPHA_DEFAULT,
    ).astype(np.float32)


def _extrapolate_wind_speed(si_ref, alpha, z_ref=_Z_HIGH, z_target=_Z_TARGET):
    """Extrapolate wind speed to z_target using the power law.

    si_target = si_ref * (z_target / z_ref) ** alpha

    Args:
        si_ref: Wind speed at z_ref [m/s].
        alpha: Hellmann exponent array.
        z_ref: Reference height [m].
        z_target: Target height [m].

    Returns:
        Wind speed at z_target (float32).
    """
    return (si_ref * (z_target / z_ref) ** alpha).astype(np.float32)


# ---------------------------------------------------------------------------
# Texas extraction (returns field arrays, does not save)
# ---------------------------------------------------------------------------

def _extract_texas_fields(grib_path, date_str, cycle_hour, lead_hour):
    """Extract Texas bounding box from a partial HRRR GRIB2.

    Opens the GRIB2 file with cfgrib, subsets the 2D Lambert Conformal grid
    to the Texas bounding box, computes wind speed/direction at 10m and 80m,
    and extrapolates to 100m via the wind profile power law.

    Args:
        grib_path: Path to the combined partial GRIB2 file.
        date_str: 'YYYYMMDD'.
        cycle_hour: UTC initialization hour.
        lead_hour: Forecast lead time in hours.

    Returns:
        Dict with keys: lat2d, lon2d, t2m, si10, wdir10, si80, wdir80,
        si100, wdir100, alpha, init_time, valid_time.
        All spatial arrays are float32, shape (ny, nx).
        Returns None on any error.
    """
    try:
        ds_tmp = xr.open_dataset(
            grib_path, engine="cfgrib",
            backend_kwargs={"filter_by_keys": {"shortName": "2t"}},
        )
        ds_u10 = xr.open_dataset(
            grib_path, engine="cfgrib",
            backend_kwargs={"filter_by_keys": {"shortName": "10u"}},
        )
        ds_v10 = xr.open_dataset(
            grib_path, engine="cfgrib",
            backend_kwargs={"filter_by_keys": {"shortName": "10v"}},
        )
        # cfgrib shortName for 80m wind is 'u'/'v' (no height prefix like '10u')
        ds_u80, ds_v80 = _open_wind_at_height(grib_path, level=80)
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
        y_slice, x_slice = _texas_slices(lat, lon_180)
        if y_slice is None:
            print("    No grid points in Texas bounding box")
            return None

        init_time = np.datetime64(
            f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}T{cycle_hour:02d}:00"
        )
        valid_time = init_time + np.timedelta64(lead_hour, "h")

        lat2d = lat[y_slice, x_slice]
        lon2d = lon_180[y_slice, x_slice]

        t2m = ds_tmp[_get_var(ds_tmp, ["t2m", "t", "tmp"])].values[y_slice, x_slice].astype(np.float32)

        u10 = ds_u10[_get_var(ds_u10, ["u10", "10u", "ugrd"])].values[y_slice, x_slice]
        v10 = ds_v10[_get_var(ds_v10, ["v10", "10v", "vgrd"])].values[y_slice, x_slice]
        si10 = np.sqrt(u10**2 + v10**2).astype(np.float32)
        wdir10 = ((270 - np.degrees(np.arctan2(v10, u10))) % 360).astype(np.float32)

        u80 = ds_u80[_get_var(ds_u80, ["u", "u80", "ugrd"])].values[y_slice, x_slice]
        v80 = ds_v80[_get_var(ds_v80, ["v", "v80", "vgrd"])].values[y_slice, x_slice]
        si80 = np.sqrt(u80**2 + v80**2).astype(np.float32)
        wdir80 = ((270 - np.degrees(np.arctan2(v80, u80))) % 360).astype(np.float32)

        alpha = _compute_power_law_alpha(si10, si80)
        si100 = _extrapolate_wind_speed(si80, alpha)

        return {
            "lat2d": lat2d,
            "lon2d": lon2d,
            "t2m": t2m,
            "si10": si10,
            "wdir10": wdir10,
            "si80": si80,
            "wdir80": wdir80,
            "si100": si100,
            "wdir100": wdir80,  # power law scales magnitude only; direction unchanged
            "alpha": alpha,
            "init_time": init_time,
            "valid_time": valid_time,
        }

    except Exception as e:
        print(f"    Error extracting Texas fields: {e}")
        return None

    finally:
        ds_tmp.close()
        ds_u10.close()
        ds_v10.close()
        ds_u80.close()
        ds_v80.close()


# ---------------------------------------------------------------------------
# Combined NetCDF saving
# ---------------------------------------------------------------------------

def _save_combined_nc(output_path, all_lead_data):
    """Save a combined NetCDF with all variables and lead times for one (day, cycle).

    All variables (t2m, si10, wdir10, si80, wdir80, si100, wdir100, alpha) and
    all lead times are packed into a single file with dims (lead_hour, y, x).
    lat/lon coordinates are stored once.

    Args:
        output_path: Full path to the output .nc file.
        all_lead_data: Dict mapping lead_hour (int) → field dict from
                       _extract_texas_fields().
    """
    leads_sorted = sorted(all_lead_data.keys())
    first = all_lead_data[leads_sorted[0]]

    def _stack(key):
        return np.stack([all_lead_data[lh][key] for lh in leads_sorted], axis=0)

    valid_times = np.array(
        [all_lead_data[lh]["valid_time"] for lh in leads_sorted],
        dtype="datetime64[ns]",
    )

    ds = xr.Dataset(
        {
            "t2m":    (["lead_hour", "y", "x"], _stack("t2m")),
            "si10":   (["lead_hour", "y", "x"], _stack("si10")),
            "wdir10": (["lead_hour", "y", "x"], _stack("wdir10")),
            "si80":   (["lead_hour", "y", "x"], _stack("si80")),
            "wdir80": (["lead_hour", "y", "x"], _stack("wdir80")),
            "si100":  (["lead_hour", "y", "x"], _stack("si100")),
            "wdir100":(["lead_hour", "y", "x"], _stack("wdir100")),
            "alpha":  (["lead_hour", "y", "x"], _stack("alpha")),
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
            "variables": (
                "t2m [K], si10 [m/s], wdir10 [deg], "
                "si80 [m/s], wdir80 [deg], "
                "si100 [m/s] (power-law from 10m+80m), wdir100 [deg], "
                "alpha [dimensionless Hellmann exponent]"
            ),
            "lead_hours": str(leads_sorted),
            "power_law": (
                f"alpha = ln(si80/si10) / ln({_Z_HIGH}/{_Z_LOW}); "
                f"si100 = si80 * ({_Z_TARGET}/{_Z_HIGH})^alpha; "
                f"default alpha = {_ALPHA_DEFAULT:.4f} when si < {_MIN_SPEED} m/s"
            ),
        },
    )

    encoding = {
        v: {"zlib": True, "complevel": 5, "dtype": "float32"}
        for v in ["t2m", "si10", "wdir10", "si80", "wdir80", "si100", "wdir100", "alpha"]
    }
    ds.to_netcdf(output_path, encoding=encoding)


# ---------------------------------------------------------------------------
# Download orchestration
# ---------------------------------------------------------------------------

def download_hrrr_month(year, month, base_dir=None):
    """Download HRRR surface forecasts for one month and extract Texas.

    For each day × 24 cycles:
        1. Check if combined output NetCDF already exists (skip if so)
        2. For each lead time: download partial GRIB via byte-range
           (TMP:2m, UGRD/VGRD:10m, UGRD/VGRD:80m)
        3. Extract Texas; compute wind speed/direction at 10m, 80m, and 100m
           (100m via wind profile power law)
        4. Write one combined NetCDF with all variables and lead times
        5. Clean up temp files

    Output files:
        {base_dir}/{year}/{month:02d}/hrrr_{HH}z_{YYYYMMDD}.nc

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
# Backfill patch for existing files (adds 80m / 100m wind)
# ---------------------------------------------------------------------------

_80M_VARS = ["UGRD:80 m above ground", "VGRD:80 m above ground"]


def patch_hrrr_100m_wind_month(year, month, base_dir=None):
    """Add 80m and 100m wind variables to existing HRRR combined NetCDF files.

    Used to backfill months that were downloaded before 100m wind support was
    added. For each existing hrrr_HHz_YYYYMMDD.nc that lacks si80:
        1. Downloads only UGRD/VGRD:80m byte ranges from S3
        2. Computes si80, wdir80 for Texas; derives alpha and si100 using si10
           already stored in the file
        3. Patches the file in-place with si80, wdir80, si100, wdir100, alpha

    Args:
        year: Calendar year.
        month: Calendar month (1–12).
        base_dir: Root output directory. Defaults to {raw}/hrrr_data.
    """
    if base_dir is None:
        dirs = setup_directories()
        base_dir = os.path.join(dirs["raw"], "hrrr_data")

    output_dir = os.path.join(base_dir, str(year), f"{month:02d}")
    if not os.path.isdir(output_dir):
        print(f"  Directory not found: {output_dir}")
        return

    num_days = calendar.monthrange(year, month)[1]
    n_patched = n_skipped = n_failed = 0

    for day in range(1, num_days + 1):
        date_str = f"{year}{month:02d}{day:02d}"

        for cycle in ALL_CYCLES:
            output_path = os.path.join(output_dir, f"hrrr_{cycle:02d}z_{date_str}.nc")
            if not os.path.exists(output_path):
                continue

            try:
                with xr.open_dataset(output_path) as ds_orig:
                    if "si80" in ds_orig.data_vars:
                        n_skipped += 1
                        continue
                    ds_orig.load()
            except Exception as e:
                print(f"  Cannot open hrrr_{cycle:02d}z_{date_str}.nc: {e}")
                n_failed += 1
                continue

            with tempfile.TemporaryDirectory() as tmp:
                all_80m = {}
                failed = False

                for lead in LEAD_TIMES:
                    grib_path = _download_variable_gribs(
                        date_str, cycle, lead, tmp, target_vars=_80M_VARS
                    )
                    if grib_path is None:
                        print(f"  {date_str} {cycle:02d}z f{lead:02d}: 80m download failed")
                        failed = True
                        break

                    try:
                        ds_u80, ds_v80 = _open_wind_at_height(grib_path, level=80)
                    except Exception as e:
                        print(f"  {date_str} {cycle:02d}z f{lead:02d}: GRIB open failed — {e}")
                        failed = True
                        break

                    try:
                        lat = ds_u80.latitude.values
                        lon_180 = np.where(ds_u80.longitude.values > 180,
                                           ds_u80.longitude.values - 360,
                                           ds_u80.longitude.values)
                        y_slice, x_slice = _texas_slices(lat, lon_180)
                        if y_slice is None:
                            print(f"  {date_str} {cycle:02d}z: no Texas points in 80m GRIB")
                            failed = True
                        else:
                            u80 = ds_u80[_get_var(ds_u80, ["u", "u80"])].values[y_slice, x_slice]
                            v80 = ds_v80[_get_var(ds_v80, ["v", "v80"])].values[y_slice, x_slice]
                            wdir80 = ((270 - np.degrees(np.arctan2(v80, u80))) % 360).astype(np.float32)
                            all_80m[lead] = {
                                "si80": np.sqrt(u80**2 + v80**2).astype(np.float32),
                                "wdir80": wdir80,
                            }
                    except Exception as e:
                        print(f"  {date_str} {cycle:02d}z f{lead:02d}: extract failed — {e}")
                        failed = True
                    finally:
                        ds_u80.close()
                        ds_v80.close()

                    if failed:
                        break

                if failed or len(all_80m) != len(LEAD_TIMES):
                    n_failed += 1
                    continue

                try:
                    leads_sorted = sorted(all_80m.keys())
                    si80 = np.stack([all_80m[lh]["si80"] for lh in leads_sorted], axis=0)
                    wdir80 = np.stack([all_80m[lh]["wdir80"] for lh in leads_sorted], axis=0)
                    alpha = _compute_power_law_alpha(ds_orig["si10"].values, si80)
                    si100 = _extrapolate_wind_speed(si80, alpha)

                    dims = ["lead_hour", "y", "x"]
                    ds_new = ds_orig.assign({
                        "si80":   (dims, si80),
                        "wdir80": (dims, wdir80),
                        "si100":  (dims, si100),
                        "wdir100":(dims, wdir80),
                        "alpha":  (dims, alpha),
                    })
                    ds_new.attrs["power_law"] = (
                        f"alpha = ln(si80/si10) / ln({_Z_HIGH}/{_Z_LOW}); "
                        f"si100 = si80 * ({_Z_TARGET}/{_Z_HIGH})^alpha; "
                        f"default alpha = {_ALPHA_DEFAULT:.4f} when si < {_MIN_SPEED} m/s"
                    )

                    tmp_out = output_path + ".tmp"
                    enc_new = {
                        v: {"zlib": True, "complevel": 5, "dtype": "float32"}
                        for v in ["si80", "wdir80", "si100", "wdir100", "alpha"]
                    }
                    ds_new.to_netcdf(tmp_out, encoding=enc_new)
                    os.replace(tmp_out, output_path)
                    n_patched += 1

                except Exception as e:
                    print(f"  {date_str} {cycle:02d}z: patch save failed — {e}")
                    tmp_out = output_path + ".tmp"
                    if os.path.exists(tmp_out):
                        os.remove(tmp_out)
                    n_failed += 1

            time.sleep(0.05)

        print(f"  {year}-{month:02d}-{day:02d}: patch cycle done")

    print(f"\n  HRRR {year}-{month:02d} patch: "
          f"{n_patched} patched, {n_skipped} already done, {n_failed} failed")


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
    print(f"  Variables: TMP:2m, UGRD/VGRD:10m, UGRD/VGRD:80m → t2m, si10, wdir10, si80, wdir80, si100, wdir100, alpha")
    print(f"  100m wind: power-law extrapolation (alpha from 10m+80m)")
    print(f"  Method: Byte-range download (~8 MB per GRIB vs ~150 MB full)")
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
