"""pull_eia923.py — Download EIA Form 923 and compute annual heat rates.

EIA Form 923 reports monthly net generation and fuel consumption (in MMBtu
attributable to electricity) at the plant × prime-mover × fuel-type level for
all U.S. utility-scale generators. We use it as Woerman's (2023) §C.1 Tier-2
fallback for ERCOT generators that lack CEMS coverage: <25 MW units do not
report to CEMS but still appear in 923, and aggregating 12 months of 923 data
gives a plant×PM×fuel heat rate that beats a market-wide technology default.

Source: https://www.eia.gov/electricity/data/eia923/
Current-year ZIP: ``xls/f923_{YYYY}.zip``; archived years: ``archive/xls/f923_{YYYY}.zip``

Outputs:
  {raw}/eia923/f923_{year}.zip                       — raw ZIP
  {processed}/eia923_heat_rates_{year}.parquet       — annual rates per
      (plant_code, prime_mover, fuel_type) and aggregated to (plant_code)
"""

import io
import sys
import zipfile
import argparse
import requests
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from helper_funcs import setup_directories

CURRENT_URL  = "https://www.eia.gov/electricity/data/eia923/xls/f923_{year}.zip"
ARCHIVE_URL  = "https://www.eia.gov/electricity/data/eia923/archive/xls/f923_{year}.zip"


def _download_zip(year, force_rebuild=False):
    """Fetch the EIA-923 ZIP for ``year``, trying current then archive URL.

    Args:
        year: Operating year (int).
        force_rebuild: Re-download even if cached file exists.

    Returns:
        Absolute Path to the cached ZIP.
    """
    dirs = setup_directories()
    out_dir = Path(dirs["raw"]) / "eia923"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"f923_{year}.zip"

    if out_path.exists() and not force_rebuild:
        print(f"  Cached: {out_path}")
        return out_path

    last_err = None
    for url in (CURRENT_URL.format(year=year), ARCHIVE_URL.format(year=year)):
        print(f"  Trying {url}...")
        try:
            r = requests.get(url, timeout=300)
            r.raise_for_status()
            if not r.content.startswith(b"PK"):
                last_err = f"non-zip response ({len(r.content)} bytes) from {url}"
                continue
            out_path.write_bytes(r.content)
            print(f"  Saved {len(r.content) / 1e6:.1f} MB to {out_path}")
            return out_path
        except requests.HTTPError as e:
            last_err = e
            continue
    raise RuntimeError(f"Could not download EIA-923 for {year}: {last_err}")


def _extract_page1(zip_path):
    """Read 'Page 1 Generation and Fuel Data' from the EIA-923 schedules workbook.

    The relevant workbook inside the ZIP is named with a pattern like
    ``EIA923_Schedules_2_3_4_5_M_12_{YYYY}_Final.xlsx``. Page 1 has 5 header
    rows of metadata before the table proper, so we ``skiprows=5``.

    Args:
        zip_path: Path to the EIA-923 annual ZIP.

    Returns:
        Raw DataFrame from Page 1 with 97 columns; column names retain the
        multi-line headers (``'Reported\\nPrime Mover'``, etc.).
    """
    with zipfile.ZipFile(zip_path) as zf:
        targets = [
            n for n in zf.namelist()
            if "Schedules_2_3_4_5" in n and n.endswith(".xlsx")
        ]
        if not targets:
            raise FileNotFoundError(
                f"No Schedules_2_3_4_5 workbook in {zip_path}: {zf.namelist()}"
            )
        with zf.open(targets[0]) as f:
            data = f.read()
    return pd.read_excel(
        io.BytesIO(data),
        sheet_name="Page 1 Generation and Fuel Data",
        skiprows=5,
    )


def build_heat_rates(year, force_rebuild=False):
    """Build annual heat rates per (plant_code, prime_mover, fuel) from EIA-923.

    Annual heat rate = sum(Elec_Fuel_MMBtu) / sum(Net_Generation_MWh) over the
    12 monthly Page-1 rows for each (plant, prime mover, fuel) combination.
    The 'Elec' (vs 'Tot') fuel column is correct here — it excludes the share
    of fuel attributable to thermal output at CHP plants, matching the
    convention used by EIA's own published heat rates.

    Args:
        year: Operating year (int).
        force_rebuild: Overwrite cached output if True.

    Returns:
        DataFrame with columns:
            plant_code         — int (EIA plant_code == EPA ORIS)
            prime_mover_code   — str (ST, GT, CT, CA, CC, CS, IC, ...) or 'ALL'
                                  for the plant-wide aggregate row
            fuel_type          — str (NG, BIT, SUB, LIG, DFO, RFO, ...) or 'ALL'
            elec_fuel_mmbtu    — sum across 12 months
            net_gen_mwh        — sum across 12 months
            heat_rate_mmbtu_mwh — elec_fuel_mmbtu / net_gen_mwh (NaN if <=0 gen)
            n_records           — number of monthly rows aggregated

        Three granularities are stacked: (plant, PM, fuel), (plant, PM, ALL),
        and (plant, ALL, ALL). Lookup falls back from the finest to the
        coarsest level.
    """
    dirs = setup_directories()
    out_path = Path(dirs["processed"]) / f"eia923_heat_rates_{year}.parquet"
    if out_path.exists() and not force_rebuild:
        print(f"  Cached: {out_path}")
        return pd.read_parquet(out_path)

    zip_path = _download_zip(year, force_rebuild=force_rebuild)
    print(f"  Reading EIA-923 Page 1 for {year}...")
    page1 = _extract_page1(zip_path)
    print(f"    Loaded {len(page1):,} rows")

    def find_col(needle):
        """Locate a Page-1 column by case-insensitive substring (newlines collapsed)."""
        needle_n = needle.lower().replace("\n", " ").replace("  ", " ")
        for col in page1.columns:
            if needle_n in str(col).lower().replace("\n", " ").replace("  ", " "):
                return col
        raise KeyError(
            f"EIA-923 Page 1 missing column matching {needle!r}; "
            f"available columns: {list(page1.columns)}"
        )

    plant_col = find_col("plant id")
    pm_col    = find_col("prime mover")
    fuel_col  = find_col("fuel type code")
    fuel_mmbtu_col = find_col("elec fuel consumption mmbtu")
    netgen_col = find_col("net generation (megawatt")

    df = pd.DataFrame({
        "plant_code":       pd.to_numeric(page1[plant_col], errors="coerce").astype("Int64"),
        "prime_mover_code": page1[pm_col].astype(str).str.upper().str.strip(),
        "fuel_type":        page1[fuel_col].astype(str).str.upper().str.strip(),
        "elec_fuel_mmbtu":  pd.to_numeric(page1[fuel_mmbtu_col], errors="coerce"),
        "net_gen_mwh":      pd.to_numeric(page1[netgen_col], errors="coerce"),
    }).dropna(subset=["plant_code"])

    def aggregate(group_cols):
        out = (
            df.groupby(group_cols, as_index=False)
            .agg(
                elec_fuel_mmbtu=("elec_fuel_mmbtu", "sum"),
                net_gen_mwh=("net_gen_mwh", "sum"),
                n_records=("plant_code", "size"),
            )
        )
        return out

    by_pm_fuel = aggregate(["plant_code", "prime_mover_code", "fuel_type"])

    by_pm = aggregate(["plant_code", "prime_mover_code"])
    by_pm["fuel_type"] = "ALL"

    by_plant = aggregate(["plant_code"])
    by_plant["prime_mover_code"] = "ALL"
    by_plant["fuel_type"] = "ALL"

    out = pd.concat([by_pm_fuel, by_pm, by_plant], ignore_index=True)
    valid = out["net_gen_mwh"] > 0
    out["heat_rate_mmbtu_mwh"] = (out["elec_fuel_mmbtu"] / out["net_gen_mwh"]).where(valid)

    out = out[[
        "plant_code", "prime_mover_code", "fuel_type",
        "elec_fuel_mmbtu", "net_gen_mwh", "heat_rate_mmbtu_mwh", "n_records",
    ]]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path, index=False)
    print(f"  Saved {len(out):,} heat-rate rows to {out_path}")
    return out


def main():
    """CLI: download EIA-923 for one year and build annual heat rates."""
    parser = argparse.ArgumentParser(description="Pull EIA-923 and build heat rates")
    parser.add_argument("--year", type=int, default=2024,
                        help="Operating year (default 2024 — most recent finalized)")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    print(f"\n=== EIA-923 heat rates for {args.year} ===")
    df = build_heat_rates(args.year, force_rebuild=args.force)
    has_rate = df["heat_rate_mmbtu_mwh"].notna()
    print(f"\n  Total rows: {len(df):,}  "
          f"(with valid heat rate: {has_rate.sum():,})")
    print(f"  Distinct plants: {df['plant_code'].nunique():,}")
    print(f"\n  Heat rate distribution (MMBtu/MWh):")
    print(df.loc[has_rate, "heat_rate_mmbtu_mwh"].describe().round(2).to_string())


if __name__ == "__main__":
    main()
