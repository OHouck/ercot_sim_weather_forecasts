"""cems_ercot_crosswalk.py — Link CEMS heat rates to ERCOT DAM Resource Names.

Builds a unit-level mapping from EPA CEMS facilities (identified by ORIS code
`facilityId` and `unitId`) to ERCOT DAM Resource Names. The bridge is EIA
Form 860, which contains both the EPA ORIS code (`plant_code`) and the ERCOT
LMP node designation for each generator.

Follows Woerman (2023) §C.1: for each ERCOT Resource Name, the heat rate is
the ratio of total heat input to total gross load aggregated over the CEMS
units that physically feed that resource over the full study year. Woerman's
data appendix matches CEMS units to EIA generators via the EIA-EPA crosswalk
(which we implement via `facilityId == plant_code`), then matches EIA to ERCOT
"using each generator's plant name."

EIA→ERCOT matching tiers (applied in order, first hit wins):
  1. `lmp_to_resource`         — cleaned lmp equals a DAM `Resource Name`
  2. `lmp_to_settlement_pt`    — cleaned lmp equals a DAM `Settlement Point Name`
  3. `lmp_prefix_to_sp`        — cleaned lmp shares site prefix with a unique
                                  thermal SP (e.g. `FRNYPP_1_CCU` → `FRNYPP_CC1`
                                  via prefix `FRNYPP` + unit-digit `1`)
  4. `name_to_settlement_pt`   — EIA `plant_name` resolves via
                                  `node_coordinates.csv` → SP (with prefix
                                  fall-through for legacy SP names)
  5. `name_override_to_prefix` — hand-curated `plant_name` → SP prefix map
                                  (e.g. "Martin Lake" → MLSES) for major
                                  plants whose `lmp_node_designation` is empty
  6. `unmatched`               — no link found

CEMS-facility-level matching (when no EIA generator matches):
  - `facility_name_override`   — hand-curated map for CEMS facilities not in EIA
  - Plant-level fallback (`facility_fallback`) — every ERCOT Resource linked to
    a facility through any tier above receives the facility-wide aggregate
    heat rate.

Outputs:
  {processed}/cems_ercot_crosswalk_{year}.parquet
  {processed}/resource_heat_rates_{year}.parquet
"""

import re
import sys
import argparse
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from helper_funcs import setup_directories
from process_data.process_ercot import (
    _load_eia860,
    _build_sp_prefix_lookup,
    _load_node_coords_plant_map,
    _resolve_node_coord_sps,
    build_eia_resource_crosswalk,
)


# ERCOT DAM thermal resource type codes:
#   CC   = Combined Cycle         SC   = Simple Cycle gas turbine
#   GS   = Gas Steam              CL   = Coal / Lignite
#   GT   = gas turbine (higher efficiency)  LE = lower efficiency
#   90   = 1990s-era vintage       00   = 2000s-era vintage
#   REH  = reheat steam            NONR = non-reheat steam   SUP = supercritical steam
#   LIG  = lignite (subtype of coal)
#   STEAM / COAL = legacy generic categories      NUC = nuclear
THERMAL_RESOURCE_TYPES = {
    "CCGT90", "CCLE90", "CCGT00", "CCLE00",
    "SCGT90", "SCLE90",
    "GSREH", "GSNONR", "GSSUP",
    "CLLIG", "STEAM", "COAL", "NUC",
}

# EIA-NEMS / Cicala (2022) defaults used as a last-resort fallback for thermal
# resources that have no CEMS coverage (smaller plants <25 MW, ERCOT plants whose
# names couldn't be bridged to EPA ORIS codes, etc.). Woerman (2023) §C.1 follows
# the same tiered approach: CEMS unit → CEMS plant → technology default.
TECH_DEFAULT_HEAT_RATE = {
    "CCGT90": 7.0,
    "CCLE90": 7.5,
    "CCGT00": 7.0,
    "CCLE00": 7.5,
    "SCGT90": 10.5,
    "SCLE90": 11.0,
    "GSREH":  10.5,
    "GSNONR": 11.0,
    "GSSUP":  11.0,
    "CLLIG":  10.5,
    "STEAM":  10.5,
    "COAL":   10.0,
    "NUC":    10.5,
}

# Hand-curated EIA `plant_name` → ERCOT settlement-point prefix overrides for plants
# whose `lmp_node_designation` is empty in EIA 860 (mostly legacy coal/lignite units).
# Each entry maps the upper-cased plant name to the SP prefix used in DAM 2025; all
# thermal resources at any SP starting with that prefix inherit the heat rate.
PLANT_NAME_PREFIX_OVERRIDES = {
    "OAK GROVE":                       "OGSES",
    "OAK GROVE (TX)":                  "OGSES",
    "MARTIN LAKE":                     "MLSES",
    "FAYETTE POWER PROJECT":           "FPPYD",
    "SAM SEYMOUR":                     "FPPYD",
    "COLETO CREEK":                    "COL",
    "MONTGOMERY COUNTY POWER STATION": "MCSES",
    "BRAZOS VALLEY ENERGY, LP":        "BVE",
    "DECKER POWER STATION":            "DECKER",
}

# Keyed on CEMS `facilityName` for facilities absent from EIA Form 860 (often
# peaker / cogen sites under non-ERCOT corporate names). Entries here intentionally
# duplicate some PLANT_NAME_PREFIX_OVERRIDES because CEMS facilityName and EIA
# plant_name are populated independently and may diverge (e.g. "Oak Grove" vs
# "Oak Grove (TX)"); a single dict can't safely cover both lookup paths.
FACILITY_NAME_PREFIX_OVERRIDES = {
    "OAK GROVE":                       "OGSES",
    "MARTIN LAKE":                     "MLSES",
    "SAM SEYMOUR":                     "FPPYD",
    "COLETO CREEK":                    "COL",
}


def _normalize_id(value):
    """Strip non-alphanumerics and uppercase. None -> ''."""
    if pd.isna(value):
        return ""
    return re.sub(r"[^A-Z0-9]", "", str(value).upper())


def _load_thermal_resources(year):
    """Load DAM Gen Resource Data for all available months and return thermal resources.

    Args:
        year: operating year (int)

    Returns:
        DataFrame with columns Resource Name, Settlement Point Name, Resource Type
        (one row per unique resource).
    """
    dirs = setup_directories()
    dam_dir = Path(dirs["raw"]) / "ercot" / "dam_disclosure" / str(year)
    frames = []
    for month_dir in sorted(dam_dir.glob("[0-9][0-9]")):
        for p in month_dir.glob(f"dam_gen_resource_{year}*.parquet"):
            df = pd.read_parquet(p, columns=["Resource Name", "Settlement Point Name", "Resource Type"])
            frames.append(df[df["Resource Type"].isin(THERMAL_RESOURCE_TYPES)])
    if not frames:
        raise FileNotFoundError(f"No DAM disclosure parquets found for {year}.")
    return pd.concat(frames, ignore_index=True).drop_duplicates()


def _load_cems_units(year):
    """Load per-unit annual CEMS aggregates (the output of compute_unit_heat_rates)."""
    dirs = setup_directories()
    path = Path(dirs["processed"]) / f"unit_heat_rates_{year}.parquet"
    df = pd.read_parquet(path)
    df["facilityId"] = df["facilityId"].astype(int)
    return df


def _build_cems_facility_resources(cems, eia, plant_to_settlement_pts, resources_by_sp, sp_prefix_lookup):
    """Match CEMS facilities not in EIA 860 to ERCOT resources via facilityName.

    Tiers tried per facility (first hit wins):
      1. ``facilityName`` resolves via node_coordinates.csv (prefix fall-through)
      2. ``facilityName`` in ``FACILITY_NAME_PREFIX_OVERRIDES`` → SPs at that prefix

    Args:
        cems: CEMS unit-level DataFrame
        eia: EIA 860 DataFrame
        plant_to_settlement_pts: dict from node_coordinates.csv
        resources_by_sp: dict Settlement Point Name → list of Resource Names
        sp_prefix_lookup: dict site prefix → list of thermal SP names

    Returns:
        DataFrame with columns facilityId, resource_name (one row per linkage)
    """
    eia_plants = set(eia["plant_code"].astype(int).unique())
    facilities_no_eia = (
        cems[~cems["facilityId"].astype(int).isin(eia_plants)][["facilityId", "facilityName"]]
        .drop_duplicates()
    )
    rows = []
    for _, row in facilities_no_eia.iterrows():
        name = row["facilityName"]
        if pd.isna(name):
            continue
        sps = _resolve_node_coord_sps(name, plant_to_settlement_pts, resources_by_sp, sp_prefix_lookup)
        if not sps:
            override = FACILITY_NAME_PREFIX_OVERRIDES.get(str(name).upper().strip())
            if override:
                sps = [sp for sp in sp_prefix_lookup.get(override, []) if sp in resources_by_sp]
        for sp in sps:
            for r in resources_by_sp.get(sp, []):
                rows.append((row["facilityId"], r))
    return pd.DataFrame(rows, columns=["facilityId", "resource_name"])


def _match_units_within_facility(cems_units, eia):
    """Try to match each CEMS unit to a specific EIA generator inside the same facility.

    Returns a DataFrame with columns facilityId, unitId, generator_id, unit_match_strategy.
    """
    out = []
    eia_by_plant = eia.groupby("plant_code")
    for facility_id, cems_group in cems_units.groupby("facilityId"):
        if facility_id not in eia_by_plant.groups:
            for _, row in cems_group.iterrows():
                out.append((facility_id, row["unitId"], None, "no_eia_plant"))
            continue

        eia_group = eia_by_plant.get_group(facility_id).copy()
        eia_norm = {_normalize_id(g): g for g in eia_group["generator_id"].astype(str)}

        for _, row in cems_group.iterrows():
            unit_norm = _normalize_id(row["unitId"])
            if not unit_norm:
                out.append((facility_id, row["unitId"], None, "unitid_empty"))
                continue
            if unit_norm in eia_norm:
                out.append((facility_id, row["unitId"], eia_norm[unit_norm], "unit_exact"))
                continue
            # Try trailing-digit match — strip alpha prefix from CEMS unitId
            trailing = re.sub(r"^[A-Z]+", "", unit_norm)
            if trailing and trailing in eia_norm:
                out.append((facility_id, row["unitId"], eia_norm[trailing], "unit_trailing_digits"))
                continue
            # Try the reverse — strip leading digits from EIA gen ids and match
            matched = None
            for eia_n, eia_g in eia_norm.items():
                if eia_n.endswith(unit_norm) or unit_norm.endswith(eia_n):
                    matched = eia_g
                    break
            if matched is not None:
                out.append((facility_id, row["unitId"], matched, "unit_suffix"))
            else:
                out.append((facility_id, row["unitId"], None, "no_unit_match"))

    return pd.DataFrame(out, columns=["facilityId", "unitId", "generator_id", "unit_match_strategy"])


def build_crosswalk(year, force_rebuild=False):
    """Build CEMS → ERCOT crosswalk and per-resource heat rates for the given year.

    Args:
        year: operating year (int)
        force_rebuild: overwrite cached output if True (bool)

    Returns:
        tuple of (crosswalk_df, resource_heat_rates_df)
          crosswalk_df: one row per (facilityId, unitId, resource_name) linkage
          resource_heat_rates_df: one row per ERCOT Resource Name with
            heat_rate_mmbtu_mwh, so2_rate_lb_mwh, nox_rate_lb_mwh,
            cems_gross_load_mwh, cems_heat_input_mmbtu, heat_rate_source
    """
    dirs = setup_directories()
    cw_path = Path(dirs["processed"]) / f"cems_ercot_crosswalk_{year}.parquet"
    hr_path = Path(dirs["processed"]) / f"resource_heat_rates_{year}.parquet"

    if cw_path.exists() and hr_path.exists() and not force_rebuild:
        print(f"  Cached: {cw_path.name}, {hr_path.name}")
        return pd.read_parquet(cw_path), pd.read_parquet(hr_path)

    print(f"  Loading thermal DAM resources for {year}...")
    thermal = _load_thermal_resources(year)
    print(f"    {thermal['Resource Name'].nunique()} unique thermal resources "
          f"across {thermal['Settlement Point Name'].nunique()} settlement points")

    print("  Loading EIA 860 generators...")
    eia = _load_eia860()
    print(f"    {len(eia)} generators across {eia['plant_code'].nunique()} plants "
          f"({eia['lmp_node_designation'].notna().sum()} with LMP node)")

    print("  Loading CEMS unit heat rates...")
    cems = _load_cems_units(year)
    print(f"    {len(cems)} CEMS units across {cems['facilityId'].nunique()} facilities")

    print("  Loading plant_name → settlement_point bridge from node_coordinates.csv...")
    plant_to_sps = _load_node_coords_plant_map()
    print(f"    {len(plant_to_sps)} plant_names mapped to settlement points")

    resources_by_sp = thermal.groupby("Settlement Point Name")["Resource Name"].apply(list).to_dict()
    resource_set = set(thermal["Resource Name"])
    sp_prefix_lookup = _build_sp_prefix_lookup(thermal["Settlement Point Name"].dropna().unique())
    print(f"    {len(sp_prefix_lookup)} site prefixes (≥3 chars) for prefix matching")

    print("  Matching EIA generators → ERCOT Resource Names...")
    eia_to_resource = build_eia_resource_crosswalk(
        eia, resource_set, resources_by_sp, plant_to_sps, sp_prefix_lookup,
        plant_name_overrides=PLANT_NAME_PREFIX_OVERRIDES,
    )
    matched_eia = eia_to_resource[eia_to_resource["match_strategy"] != "unmatched"]
    print(f"    {matched_eia['plant_code'].nunique()} EIA plants matched, "
          f"{matched_eia['resource_name'].nunique()} unique resources covered")
    print(f"    Strategy breakdown: {eia_to_resource['match_strategy'].value_counts().to_dict()}")

    print("  Matching CEMS facilities not in EIA via facilityName...")
    no_eia_links = _build_cems_facility_resources(cems, eia, plant_to_sps, resources_by_sp, sp_prefix_lookup)
    if len(no_eia_links) > 0:
        print(f"    Recovered {no_eia_links['facilityId'].nunique()} facilities, "
              f"{no_eia_links['resource_name'].nunique()} resources")

    print("  Matching CEMS units → EIA generators (within facility)...")
    unit_match = _match_units_within_facility(cems, eia)
    print(f"    {unit_match['unit_match_strategy'].value_counts().to_dict()}")

    # Combine into per-(facility, unit, resource) crosswalk
    cw = (
        cems[["facilityId", "facilityName", "unitId", "unitType", "primaryFuelInfo",
              "gross_load_mwh", "heat_input_mmbtu", "so2_mass_lbs", "nox_mass_lbs",
              "operating_hours", "heat_rate_mmbtu_mwh"]]
        .merge(unit_match, on=["facilityId", "unitId"], how="left")
        .merge(eia_to_resource,
               left_on=["facilityId", "generator_id"],
               right_on=["plant_code", "generator_id"], how="left")
    )

    # Plant-level fallback: if unit matching failed but ANY generator at this plant
    # has a resource (via EIA or via facilityName bridge), fall back to plant-level aggregation.
    facility_resources = (
        eia_to_resource[eia_to_resource["resource_name"].notna()]
        .groupby("plant_code")["resource_name"].apply(lambda s: sorted(set(s)))
        .to_dict()
    )
    for fid, sub in no_eia_links.groupby("facilityId"):
        facility_resources.setdefault(int(fid), [])
        facility_resources[int(fid)] = sorted(set(facility_resources[int(fid)]) | set(sub["resource_name"]))

    needs_fallback = cw["resource_name"].isna() & cw["facilityId"].isin(facility_resources)
    if needs_fallback.any():
        fallback_map = pd.DataFrame(
            [(fid, r) for fid, rs in facility_resources.items() for r in rs],
            columns=["facilityId", "fb_resource_name"],
        )
        fallback = (
            cw[needs_fallback].drop(columns=["resource_name", "match_strategy"])
            .merge(fallback_map, on="facilityId", how="inner")
            .rename(columns={"fb_resource_name": "resource_name"})
            .assign(match_strategy="facility_fallback")
        )
        cw = pd.concat([cw[~needs_fallback], fallback], ignore_index=True)

    cw.to_parquet(cw_path, index=False)
    print(f"  Saved crosswalk to {cw_path}")

    # Aggregate to per-resource heat rate (Woerman §C.1 style)
    matched = cw[cw["resource_name"].notna()].copy()
    matched["heat_rate_source"] = matched["match_strategy"].map({
        "lmp_to_resource":         "cems_unit_direct",
        "lmp_to_settlement_pt":    "cems_unit_via_sp",
        "lmp_prefix_to_sp":        "cems_unit_via_prefix",
        "name_to_settlement_pt":   "cems_unit_via_name",
        "name_override_to_prefix": "cems_unit_via_override",
        "facility_fallback":       "cems_facility_avg",
    })

    # Sum heat input and gross load across CEMS units feeding each resource
    resource_hr = (
        matched.groupby("resource_name", as_index=False)
        .agg(
            cems_gross_load_mwh=("gross_load_mwh", "sum"),
            cems_heat_input_mmbtu=("heat_input_mmbtu", "sum"),
            so2_mass_lbs=("so2_mass_lbs", "sum"),
            nox_mass_lbs=("nox_mass_lbs", "sum"),
            operating_hours=("operating_hours", "sum"),
            n_cems_units=("unitId", "nunique"),
            facility_ids=("facilityId", lambda s: ",".join(str(x) for x in sorted(set(s)))),
            heat_rate_source=("heat_rate_source", lambda s: s.value_counts().index[0]),
        )
    )
    valid = resource_hr["cems_gross_load_mwh"] > 0
    resource_hr["heat_rate_mmbtu_mwh"] = (
        resource_hr["cems_heat_input_mmbtu"] / resource_hr["cems_gross_load_mwh"]
    ).where(valid)
    resource_hr["so2_rate_lb_mwh"] = (
        resource_hr["so2_mass_lbs"] / resource_hr["cems_gross_load_mwh"]
    ).where(valid)
    resource_hr["nox_rate_lb_mwh"] = (
        resource_hr["nox_mass_lbs"] / resource_hr["cems_gross_load_mwh"]
    ).where(valid)

    # Technology-default fallback: thermal resources with no CEMS coverage at all
    # get a tech-default heat rate. Matches Woerman (2023) §C.1's final tier.
    resource_types = thermal.drop_duplicates("Resource Name").set_index("Resource Name")["Resource Type"]
    all_thermal = pd.DataFrame({"resource_name": resource_types.index})
    all_thermal["resource_type"] = all_thermal["resource_name"].map(resource_types)
    resource_hr = all_thermal.merge(resource_hr, on="resource_name", how="left")

    needs_default = resource_hr["heat_rate_mmbtu_mwh"].isna()
    resource_hr.loc[needs_default, "heat_rate_mmbtu_mwh"] = (
        resource_hr.loc[needs_default, "resource_type"].map(TECH_DEFAULT_HEAT_RATE)
    )
    resource_hr.loc[needs_default, "heat_rate_source"] = "tech_default"
    # so2/nox stay NaN for tech_default rows — no CEMS basis for emissions

    resource_hr.to_parquet(hr_path, index=False)
    print(f"  Saved resource heat rates to {hr_path}")
    return cw, resource_hr


def main():
    """Build the CEMS → ERCOT crosswalk and per-resource heat rates."""
    parser = argparse.ArgumentParser(description="Build CEMS → ERCOT DAM crosswalk")
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    print(f"\n=== CEMS → ERCOT Crosswalk for {args.year} ===")
    cw, resource_hr = build_crosswalk(args.year, force_rebuild=args.force)

    print(f"\n  Crosswalk summary:")
    print(f"    Total rows: {len(cw)}")
    print(f"    Match strategy breakdown:")
    print(cw["match_strategy"].value_counts().to_string())

    print(f"\n  Resource heat rates:")
    print(f"    ERCOT thermal resources covered: {len(resource_hr)} (CEMS + tech default)")
    print(f"    With heat rate from CEMS: {(resource_hr['heat_rate_source'] != 'tech_default').sum()}")

    summary = (
        resource_hr.groupby("heat_rate_source")["heat_rate_mmbtu_mwh"]
        .agg(["count", "mean", "min", "max"]).round(2)
    )
    print(f"\n  Heat rate by source (mmbtu/mwh):")
    print(summary.to_string())


if __name__ == "__main__":
    main()
