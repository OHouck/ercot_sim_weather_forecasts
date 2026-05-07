import os
import sys
import glob
import argparse
import traceback
import csv

import xarray as xr
import pandas as pd


EXPECTED_VARS = [
	"t2m",
	"si10",
	"wdir10",
	"si80",
	"wdir80",
	"si100",
	"wdir100",
	"alpha",
]


def find_time_coord(ds):
	# Return a list of candidate time coordinates (name strings) in order of preference.
	candidates = []
	# prefer coordinate literally named 'time'
	if "time" in ds.coords:
		candidates.append("time")
	# also accept 'valid_time' or any coord with 'time' in its name
	for c in ds.coords:
		if c in ("time",):
			continue
		if "time" in str(c).lower() or "valid" in str(c).lower():
			candidates.append(c)
	# prefer datetime dtype coords that are 1D
	for c in ds.coords:
		if c in candidates:
			continue
		try:
			vals = ds[c].values
			if getattr(vals, "dtype", None) is not None and "datetime" in str(vals.dtype).lower():
				# include if length >= 1
				if getattr(vals, "shape", ())[0] >= 1:
					candidates.append(c)
		except Exception:
			continue
	return candidates


def check_times(time_values):
	# return (ok:bool, msg:str, freq:str or None, n_missing:int)
	try:
		times = pd.to_datetime(time_values)
	except Exception:
		return False, "unable to parse time values", None, -1
	if times.isnull().any():
		return False, "null timestamps present", None, -1
	if len(times) < 2:
		return False, "less than 2 time steps", None, 0
	# infer freq
	try:
		freq = pd.infer_freq(times)
	except Exception:
		freq = None
	if freq is not None:
		expected = pd.date_range(start=times[0], end=times[-1], freq=freq)
		if len(expected) != len(times):
			return False, f"missing steps (expected {len(expected)} got {len(times)})", freq, len(expected) - len(times)
		return True, None, freq, 0
	# fallback: compare diffs to minimum diff
	diffs = times.to_series().diff().dropna()
	min_diff = diffs.min()
	n_gaps = (diffs > min_diff).sum()
	if n_gaps > 0:
		return False, f"irregular intervals, {n_gaps} gaps (min interval {min_diff})", None, int(n_gaps)
	return True, None, None, 0


def check_file(fpath, expected_vars_lower):
	result = {
		"file": fpath,
		"n_times": None,
		"missing_vars": [],
		"time_ok": None,
		"time_msg": None,
		"freq": None,
		"n_missing_steps": None,
	}
	try:
		ds = xr.open_dataset(fpath, decode_times=True)
	except Exception as e:
		result["time_ok"] = False
		result["time_msg"] = f"open error: {e}"
		return result
	try:
		vars_lower = set([str(v).lower() for v in list(ds.data_vars.keys()) + list(ds.coords.keys())])
		missing = [v for v in expected_vars_lower if v not in vars_lower]
		result["missing_vars"] = missing

		# Find the most appropriate time coordinate for the data variables.
		# Many HRRR files index forecasts along a 'lead_hour' dim and expose
		# 'valid_time' (length == lead_hour). Prefer coords sharing a dim
		# with the first data variable (usually the time-like dim).
		time_candidates = find_time_coord(ds)

		# Determine a preferred coord per data variable, falling back to candidates
		chosen_times = None
		for var in ds.data_vars:
			dims = ds[var].dims
			# look for a coord whose dims intersect var dims and is datetime
			chosen = None
			for c in ds.coords:
				if any(d in ds[c].dims for d in dims) and "datetime" in str(ds[c].values.dtype).lower():
					chosen = c
					break
			if chosen is not None:
				chosen_times = chosen
				break

		if chosen_times is None and time_candidates:
			chosen_times = time_candidates[0]

		if chosen_times is None:
			result["time_ok"] = False
			result["time_msg"] = "no time-like coordinate found"
			ds.close()
			return result

		time_vals = ds[chosen_times].values
		# If the chosen time coord is associated with a dimension (e.g., lead_hour),
		# ensure its length matches that dimension size and that entries are not null.
		try:
			if getattr(time_vals, "size", None) is None:
				result["time_ok"] = False
				result["time_msg"] = "time coord has no values"
			else:
				# Basic checks: parseable, no nulls
				ok, msg, freq, n_missing = check_times(time_vals)
				result["time_ok"] = bool(ok)
				result["time_msg"] = msg
				result["freq"] = freq
				result["n_missing_steps"] = n_missing
				result["n_times"] = int(len(time_vals))
				# If time coord has an associated dim, validate length matches dim
				coord_dims = ds[chosen_times].dims
				if coord_dims:
					dim0 = coord_dims[0]
					if dim0 in ds.dims:
						if len(time_vals) != ds.dims[dim0]:
							result["time_ok"] = False
							result["time_msg"] = (
								f"time coord length {len(time_vals)} != dim {dim0} size {ds.dims[dim0]}"
							)
		except Exception as e:
			result["time_ok"] = False
			result["time_msg"] = f"exception checking time coord: {e}"
	except Exception as e:
		result["time_ok"] = False
		result["time_msg"] = f"exception: {e}"
	finally:
		try:
			ds.close()
		except Exception:
			pass
	return result


def main(root, out_csv):
	pattern = os.path.join(root, "**", "*.nc")
	files = sorted(glob.glob(pattern, recursive=True))
	if not files:
		print(f"No .nc files found under {root}")
		return 1

	expected_vars_lower = [v.lower() for v in EXPECTED_VARS]
	results = []
	for f in files:
		try:
			r = check_file(f, expected_vars_lower)
		except Exception:
			r = {"file": f, "n_times": None, "missing_vars": [], "time_ok": False, "time_msg": traceback.format_exc(), "freq": None, "n_missing_steps": None}
		results.append(r)
		# brief progress
		status = "OK" if (not r["missing_vars"] and r["time_ok"]) else "ISSUE"
		print(f"[{status}] {os.path.relpath(f)} missing_vars={len(r['missing_vars'])} time_ok={r['time_ok']} msg={r['time_msg']}")

	# write CSV
	fieldnames = ["file", "n_times", "missing_vars", "time_ok", "time_msg", "freq", "n_missing_steps"]
	with open(out_csv, "w", newline="") as fh:
		w = csv.DictWriter(fh, fieldnames=fieldnames)
		w.writeheader()
		for r in results:
			row = r.copy()
			row["missing_vars"] = ";".join(row["missing_vars"]) if row["missing_vars"] else ""
			w.writerow(row)

	# summary
	n_total = len(results)
	n_bad_vars = sum(1 for r in results if r["missing_vars"])
	n_bad_time = sum(1 for r in results if not r["time_ok"]) 
	print("")
	print(f"Scanned {n_total} files. {n_bad_vars} files missing expected variables. {n_bad_time} files have time issues.")
	print(f"Detailed results written to: {out_csv}")
	return 0


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Check HRRR .nc files for expected vars and contiguous timesteps")
    p.add_argument("root", nargs="?", default=os.path.expanduser("/Users/ohouck/Library/CloudStorage/OneDrive-TheUniversityofChicago/ercot_sim_weather_forecasts/raw_data/hrrr_data"), help="root folder to search for .nc files")
    p.add_argument("--out", default="hrrr_check_results.csv", help="output CSV summary")
    args = p.parse_args()

    # path = "/Users/ohouck/Library/CloudStorage/OneDrive-TheUniversityofChicago/ercot_sim_weather_forecasts/raw_data/hrrr_data/2025/10/hrrr_10z_20251031.nc"
    # ds = xr.open_dataset(path)
    # print(ds)
    # exit(0)

    rc = main(args.root, args.out)
    sys.exit(rc)