"""cems_ercot_crosswalk.py — Generator-level CEMS ↔ ERCOT crosswalk and heat rates.

Builds a per-resource heat rate following Woerman (2023) §C.1: aggregate CEMS
hourly heat input (fuel burned) and ERCOT SCED hourly net generation over the
year, then take the ratio. CEMS reports gross load at the generator terminals,
so we deliberately ignore CEMS ``grossLoad`` and use SCED net generation as the
denominator — net of station service / auxiliary consumption, matching the
quantity ERCOT settles against.

Identifier namespaces (none shared directly across CEMS and ERCOT):
  CEMS:   facilityId (= EPA ORIS), unitId (boiler/turbine label, e.g. "CT1")
  EIA:    plant_code (= ORIS), generator_id, lmp_node_designation (= ERCOT
            Resource Name when populated)
  ERCOT:  Resource Name (identical in DAM and SCED disclosure)


Tier A (CEMS unit ↔ EIA generator, within facility):
    - EPA CAMD-EIA crosswalk (download_data.pull_epa_eia_crosswalk) — EPA's
        manually-curated unit→generator file, authoritative when present.
    - exact unitId == generator_id (normalized)
    - trailing-digit match (strip leading alpha from CEMS unitId)
    - suffix match (CEMS unitId ends with EIA generator_id or vice versa)

Tier B (EIA generator ↔ ERCOT Resource Name, direct):
    - lmp_clean == Resource Name                  (1:1)
    - lmp_clean is a prefix of Resource Names     (1:N, e.g. THW_CC1 → THW_CC1_1.._8)
    - Resource Name is a prefix of lmp_clean      (N:1, rare)

Aggregation (per facility):
    - If every CEMS unit at the facility maps to an EIA gen AND every such
      gen has at least one ERCOT resource match → "unit-level" aggregation,
      one heat rate per (facility, generator) sub-group.
    - Otherwise → "plant-level" fallback, one heat rate per facility covering
      all CEMS heat input and all ERCOT resources reachable from any
      generator at that plant.

Tier C (EIA-923 fallback for non-CEMS resources, Woerman 2023 §C.1 Tier 2):
    For ERCOT resources whose CEMS-derived rate is missing or implausible,
    use EIA-923 Page 1 ``Elec_Fuel_MMBtu / Net_Generation_MWh`` aggregated
    over 12 months. Lookup falls back from (plant, prime_mover, fuel) →
    (plant, prime_mover, ALL) → (plant, ALL, ALL) using
    ``download_data.pull_eia923``.

Tier D (last-resort tech default, EIA-NEMS/Cicala 2022):
    Per-technology heat rate from ``TECH_DEFAULT_HEAT_RATE`` below.

Outputs:
  {processed}/cems_ercot_crosswalk_{year}.parquet — per-(facility,unit,resource)
  {processed}/resource_heat_rates_{year}.parquet  — per-resource heat rate
"""

import re
import sys
import argparse
import requests
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from helper_funcs import setup_directories, THERMAL_RESOURCE_TYPES
from download_data.pull_epa_eia_crosswalk import load_crosswalk as load_epa_crosswalk
from download_data.pull_eia923 import build_heat_rates as build_eia923_heat_rates
from process_data.process_ercot import _load_eia860, _normalize_name
from process_data.process_sced_thermal import build_sced_thermal_annual


# EIA-NEMS / Cicala (2022) defaults used as a last-resort fallback for thermal
# resources that have no CEMS coverage (smaller plants <25 MW, ERCOT plants
# whose names couldn't be bridged to EPA ORIS codes, etc.). Woerman (2023) §C.1
# follows the same tiered approach: CEMS unit → CEMS plant → technology default.
TECH_DEFAULT_HEAT_RATE = {
    "CCGT90": 7.0,   "CCLE90": 7.5,   "CCGT00": 7.0,   "CCLE00": 7.5,
    "SCGT90": 10.5,  "SCLE90": 11.0,
    "GSREH":  10.5,  "GSNONR": 11.0,  "GSSUP":  11.0,
    "CLLIG":  10.5,  "STEAM":  10.5,  "COAL":   10.0,  "NUC": 10.5,
}
assert set(TECH_DEFAULT_HEAT_RATE) >= THERMAL_RESOURCE_TYPES, (
    "TECH_DEFAULT_HEAT_RATE must cover every thermal resource type"
)

# Plausible heat-rate envelope by technology (MMBtu/MWh). Sources: EIA
# https://www.eia.gov/todayinenergy/detail.php?id=32572 typical ranges,
# widened to absorb part-load operation. Rates outside the envelope are
# treated as a data-matching artifact (e.g. CEMS heat input divided by SCED
# net generation that covers only a subset of the plant's resources) and the
# resource falls back to the tech default.
PLAUSIBLE_HEAT_RATE_RANGE = {
    "CCGT90": (5.5, 13.0), "CCLE90": (5.5, 13.0),
    "CCGT00": (5.5, 13.0), "CCLE00": (5.5, 13.0),
    "SCGT90": (8.0, 16.0), "SCLE90": (8.0, 16.0),
    "GSREH":  (8.0, 16.0), "GSNONR": (8.0, 16.0), "GSSUP": (8.0, 16.0),
    "CLLIG":  (8.5, 13.5), "STEAM":  (8.5, 13.5), "COAL":  (8.5, 13.5),
    "NUC":    (9.5, 12.0),
}


def _plausibility_bounds(resource_types):
    """Return ``(lo_series, hi_series)`` aligned with ``resource_types`` index."""
    lo = resource_types.map(lambda t: PLAUSIBLE_HEAT_RATE_RANGE.get(t, (0, float("inf")))[0])
    hi = resource_types.map(lambda t: PLAUSIBLE_HEAT_RATE_RANGE.get(t, (0, float("inf")))[1])
    return lo, hi


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _load_thermal_resources(year):
    """Load DAM Gen Resource Data and return the unique thermal resource list.

    Args:
        year: Operating year.

    Returns:
        DataFrame with columns: resource_name, settlement_point, resource_type.
        Deduplicated to one row per resource.
    """
    dirs = setup_directories()
    dam_dir = Path(dirs["raw"]) / "ercot" / "dam_disclosure" / str(year)
    frames = []
    for month_dir in sorted(dam_dir.glob("[0-9][0-9]")):
        for p in month_dir.glob(f"dam_gen_resource_{year}*.parquet"):
            df = pd.read_parquet(
                p, columns=["Resource Name", "Settlement Point Name", "Resource Type"]
            )
            frames.append(df[df["Resource Type"].isin(THERMAL_RESOURCE_TYPES)])
    if not frames:
        raise FileNotFoundError(f"No DAM disclosure parquets found for {year}.")
    return (
        pd.concat(frames, ignore_index=True)
        .rename(columns={
            "Resource Name": "resource_name",
            "Settlement Point Name": "settlement_point",
            "Resource Type": "resource_type",
        })
        .drop_duplicates("resource_name")
        .reset_index(drop=True)
    )


def _load_cems_annual(year):
    """Load the CEMS per-unit annual aggregates produced by pull_epa_cems.py."""
    dirs = setup_directories()
    path = Path(dirs["processed"]) / f"unit_heat_rates_{year}.parquet"
    if not path.exists():
        raise FileNotFoundError(
            f"CEMS annual aggregates not found at {path}. "
            "Run: uv run python -m download_data.pull_epa_cems --aggregate-only"
        )
    df = pd.read_parquet(path)
    df["facilityId"] = df["facilityId"].astype(int)
    return df


def _normalize_id(value):
    """``_normalize_name`` with NA tolerance — '' for missing input."""
    if pd.isna(value):
        return ""
    return _normalize_name(value)


def _build_epa_crosswalk_lookup(eia):
    """Build ``{(plant_code, normalized_unitId): generator_id}`` from EPA crosswalk.

    A single CEMS unit can map to multiple EIA generators (combined-cycle units
    where one shared CT-stack maps to both CT and steam generators). We keep
    only the first generator that also appears in ``eia`` (a sanity check that
    the generator was operable in the EIA-860 vintage we're using), preferring
    rows where the EPA crosswalk reports an exact match.

    Args:
        eia: EIA Form 860 DataFrame from ``_load_eia860`` — used to filter EPA
            rows whose ``eia_generator_id`` is not present in our 860 vintage.

    Returns:
        Dict mapping ``(plant_code, _normalize_id(unitId))`` → ``generator_id``.
        Returns an empty dict if the crosswalk file cannot be loaded.
    """
    try:
        epa = load_epa_crosswalk(state="TX", force_rebuild=False)
    except (FileNotFoundError, requests.RequestException, ValueError) as e:
        print(f"  WARN: could not load EPA CAMD-EIA crosswalk ({e}); skipping Tier 0")
        return {}

    epa = epa.dropna(subset=["camd_plant_id", "camd_unit_id"]).copy()
    epa["camd_plant_id"] = epa["camd_plant_id"].astype(int)
    epa["unit_norm"] = epa["camd_unit_id"].map(_normalize_id)
    epa = epa[epa["unit_norm"] != ""]
    epa["exact"] = epa["match_type_gen"].str.contains("Exact", case=False, na=False)

    eia_keys = set(zip(eia["plant_code"], eia["generator_id"].astype(str)))
    in_eia = [(p, g) in eia_keys for p, g in zip(epa["camd_plant_id"], epa["eia_generator_id"])]
    epa = epa[in_eia]

    epa = (
        epa.sort_values("exact", ascending=False)
        .drop_duplicates(["camd_plant_id", "unit_norm"], keep="first")
    )
    return dict(zip(zip(epa["camd_plant_id"], epa["unit_norm"]), epa["eia_generator_id"]))


def _match_cems_units_to_eia_gens(cems_units, eia):
    """Match each CEMS unit to a specific EIA generator inside the same facility.

    Tier 0 uses the EPA Power Sector Data Crosswalk
    (``download_data.pull_epa_eia_crosswalk``) for the ~95% of in-scope fossil
    units >25 MW where EPA has done the manual curation. Tiers 1–3 below
    fall back to the original normalization-based heuristics for CEMS units
    EPA has not covered (mostly small / new / retired units).

    Tier 0: epa_crosswalk            EPA CAMD-EIA crosswalk row exists
    Tier 1: unit_exact               normalized unitId == normalized generator_id
    Tier 2: unit_trailing_digits     strip leading alpha from CEMS unitId, then exact
    Tier 3: unit_suffix              one ID ends with the other

    Args:
        cems_units: DataFrame with columns facilityId, unitId.
        eia: DataFrame from ``_load_eia860`` with plant_code and generator_id.

    Returns:
        DataFrame with columns facilityId, unitId, generator_id, unit_match_strategy.
        ``generator_id`` is None when no match was found.
    """
    epa_lookup = _build_epa_crosswalk_lookup(eia)

    out = []
    eia_by_plant = eia.groupby("plant_code")
    for facility_id, cems_group in cems_units.groupby("facilityId"):
        if facility_id not in eia_by_plant.groups:
            for _, row in cems_group.iterrows():
                out.append((facility_id, row["unitId"], None, "no_eia_plant"))
            continue

        eia_group = eia_by_plant.get_group(facility_id)
        eia_norm = {_normalize_id(g): g for g in eia_group["generator_id"].astype(str)}

        for _, row in cems_group.iterrows():
            unit_norm = _normalize_id(row["unitId"])
            if not unit_norm:
                out.append((facility_id, row["unitId"], None, "unitid_empty"))
                continue
            epa_gen = epa_lookup.get((int(facility_id), unit_norm))
            if epa_gen is not None:
                out.append((facility_id, row["unitId"], epa_gen, "epa_crosswalk"))
                continue
            if unit_norm in eia_norm:
                out.append((facility_id, row["unitId"], eia_norm[unit_norm], "unit_exact"))
                continue
            trailing = re.sub(r"^[A-Z]+", "", unit_norm)
            if trailing and trailing in eia_norm:
                out.append((facility_id, row["unitId"], eia_norm[trailing], "unit_trailing_digits"))
                continue
            matched = None
            for eia_n, eia_g in eia_norm.items():
                if eia_n.endswith(unit_norm) or unit_norm.endswith(eia_n):
                    matched = eia_g
                    break
            out.append(
                (facility_id, row["unitId"], matched, "unit_suffix" if matched else "no_unit_match")
            )
    return pd.DataFrame(out, columns=["facilityId", "unitId", "generator_id", "unit_match_strategy"])


# ---------------------------------------------------------------------------
# Tier B: EIA generator ↔ ERCOT Resource Name (direct, no settlement-point hop)
# ---------------------------------------------------------------------------

def _match_eia_gens_to_resources(eia, resource_names):
    """Match each EIA generator directly to ERCOT Resource Names via lmp_clean.

    Three strategies, applied in order, first hit wins per generator (a single
    generator can still expand to multiple resources via prefix matching):

      1. ``lmp_to_resource``      lmp_clean equals a Resource Name exactly
      2. ``lmp_prefix_to_resource``  lmp_clean is a prefix of one or more
                                     Resource Names (with at least one resource
                                     name being strictly longer — common CC
                                     split pattern THW_CC1 → THW_CC1_1.._8)
      3. ``resource_prefix_to_lmp``  a Resource Name is a prefix of lmp_clean
                                     (rare; covers cases where EIA carries a
                                     more specific suffix than DAM)

    Args:
        eia: EIA Form 860 DataFrame with plant_code, generator_id, lmp_clean.
        resource_names: iterable of valid ERCOT thermal Resource Names.

    Returns:
        DataFrame with columns plant_code, generator_id, lmp_clean,
        resource_name, gen_match_strategy. One row per (generator, resource)
        pair; generators with no match appear once with resource_name=None.
    """
    resource_set = set(resource_names)
    resource_list = sorted(resource_set)

    rows = []
    for _, gen in eia.iterrows():
        lmp = gen["lmp_clean"]
        plant_code = gen["plant_code"]
        gen_id = gen["generator_id"]

        if pd.isna(lmp) or not lmp:
            rows.append((plant_code, gen_id, lmp, None, "no_lmp"))
            continue

        if lmp in resource_set:
            rows.append((plant_code, gen_id, lmp, lmp, "lmp_to_resource"))
            continue

        prefix_hits = [
            r for r in resource_list
            if len(r) > len(lmp) and r.startswith(lmp) and (r[len(lmp)] in "_0123456789")
        ]
        if prefix_hits:
            for r in prefix_hits:
                rows.append((plant_code, gen_id, lmp, r, "lmp_prefix_to_resource"))
            continue

        reverse_hits = [r for r in resource_list if len(r) < len(lmp) and lmp.startswith(r)]
        if reverse_hits:
            best = max(reverse_hits, key=len)
            rows.append((plant_code, gen_id, lmp, best, "resource_prefix_to_lmp"))
            continue

        rows.append((plant_code, gen_id, lmp, None, "unmatched"))

    return pd.DataFrame(
        rows, columns=["plant_code", "generator_id", "lmp_clean", "resource_name", "gen_match_strategy"]
    )


# ---------------------------------------------------------------------------
# Aggregation: assign each CEMS unit and each resource to a group
# ---------------------------------------------------------------------------

def _assign_groups(cems_with_gens, gen_to_resource):
    """Compute connected-component groups of CEMS units and ERCOT resources per facility.

    Within each facility, build a bipartite graph: CEMS unit ↔ ERCOT Resource
    Name, with an edge whenever the unit's matched EIA generator reaches the
    resource (Tier B). The connected components of that graph are the
    aggregation cells — every CEMS unit in a component contributes heat input
    to the numerator and every resource in the component contributes net
    generation to the denominator.

    If any CEMS unit at the facility has no chain to a resource (missing Tier A
    or missing Tier B for its gen), the whole facility is promoted to a single
    plant-level component covering all CEMS units and all resources reachable
    from any EIA generator at the plant. Within-facility heterogeneity
    (e.g. coal + peaker) is therefore captured only when every unit matches.

    Args:
        cems_with_gens: CEMS unit table joined with ``_match_cems_units_to_eia_gens``.
        gen_to_resource: output of ``_match_eia_gens_to_resources``.

    Returns:
        Tuple of two DataFrames:
          unit_groups:     facilityId, unitId, group_key, group_level
          resource_groups: facilityId, resource_name, group_key, group_level
        Each resource appears in exactly one group_key (no duplicates).
    """
    gen_resources = gen_to_resource.dropna(subset=["resource_name"]).copy()
    gen_resources["plant_code"] = gen_resources["plant_code"].astype(int)
    gens_with_resource = (
        gen_resources.groupby(["plant_code", "generator_id"])["resource_name"].apply(set)
        .to_dict()
    )
    plant_resources = (
        gen_resources.groupby("plant_code")["resource_name"].apply(set).to_dict()
    )

    unit_rows = []
    resource_rows = []
    for facility_id, group in cems_with_gens.groupby("facilityId"):
        fid = int(facility_id)
        unit_chains = []
        for _, r in group.iterrows():
            gen_id = r["generator_id"]
            rset = gens_with_resource.get((fid, gen_id)) if pd.notna(gen_id) else None
            unit_chains.append((r["unitId"], rset))

        any_missing = any(rset is None for _, rset in unit_chains)
        plant_level = any_missing or not unit_chains

        if plant_level:
            key = f"plant::{fid}"
            for unit_id, _ in unit_chains:
                unit_rows.append((fid, unit_id, key, "plant"))
            for res in plant_resources.get(fid, set()):
                resource_rows.append((fid, res, key, "plant"))
            continue

        all_resources = set().union(*(rset for _, rset in unit_chains))
        parent = {f"U:{u}": f"U:{u}" for u, _ in unit_chains}
        parent.update({f"R:{r}": f"R:{r}" for r in all_resources})

        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(a, b):
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[ra] = rb

        for unit_id, rset in unit_chains:
            for res in rset:
                union(f"U:{unit_id}", f"R:{res}")

        for unit_id, _ in unit_chains:
            root = find(f"U:{unit_id}")
            unit_rows.append((fid, unit_id, f"unit::{fid}::{root}", "unit"))
        for res in all_resources:
            root = find(f"R:{res}")
            resource_rows.append((fid, res, f"unit::{fid}::{root}", "unit"))

    unit_groups = pd.DataFrame(
        unit_rows, columns=["facilityId", "unitId", "group_key", "group_level"]
    )
    resource_groups = pd.DataFrame(
        resource_rows,
        columns=["facilityId", "resource_name", "group_key", "group_level"],
    ).drop_duplicates()
    return unit_groups, resource_groups


# ---------------------------------------------------------------------------
# Heat-rate computation
# ---------------------------------------------------------------------------

def _compute_heat_rates_per_group(cems_units, unit_groups, resource_groups, sced_annual):
    """Compute group-level heat rate = Σ CEMS heat input / Σ SCED net generation.

    Args:
        cems_units: CEMS annual table with heat_input_mmbtu, so2_mass_lbs,
            nox_mass_lbs per (facilityId, unitId).
        unit_groups: from ``_assign_groups``.
        resource_groups: from ``_assign_groups``.
        sced_annual: per-resource annual SCED net generation
            (``net_gen_mwh_annual``).

    Returns:
        DataFrame indexed by group_key with columns:
            cems_heat_input_mmbtu, cems_so2_mass_lbs, cems_nox_mass_lbs,
            sced_net_gen_mwh, n_cems_units, n_resources, group_level,
            heat_rate_mmbtu_mwh, so2_rate_lb_mwh, nox_rate_lb_mwh.
    """
    cems_with_group = cems_units.merge(
        unit_groups[["facilityId", "unitId", "group_key", "group_level"]],
        on=["facilityId", "unitId"], how="inner",
    )
    numer = (
        cems_with_group.groupby(["group_key", "group_level"], as_index=False)
        .agg(
            cems_heat_input_mmbtu=("heat_input_mmbtu", "sum"),
            cems_so2_mass_lbs=("so2_mass_lbs", "sum"),
            cems_nox_mass_lbs=("nox_mass_lbs", "sum"),
            n_cems_units=("unitId", "nunique"),
        )
    )

    sced_lookup = sced_annual.set_index("resource_name")["net_gen_mwh_annual"]
    resource_groups = resource_groups.copy()
    resource_groups["net_gen_mwh"] = resource_groups["resource_name"].map(sced_lookup).fillna(0.0)
    denom = (
        resource_groups.groupby("group_key", as_index=False)
        .agg(
            sced_net_gen_mwh=("net_gen_mwh", "sum"),
            n_resources=("resource_name", "nunique"),
        )
    )

    rates = numer.merge(denom, on="group_key", how="outer")
    valid = rates["sced_net_gen_mwh"] > 0
    for num_col, rate_col in [
        ("cems_heat_input_mmbtu", "heat_rate_mmbtu_mwh"),
        ("cems_so2_mass_lbs",     "so2_rate_lb_mwh"),
        ("cems_nox_mass_lbs",     "nox_rate_lb_mwh"),
    ]:
        rates[rate_col] = (rates[num_col] / rates["sced_net_gen_mwh"]).where(valid)
    return rates


# ---------------------------------------------------------------------------
# Tier C: EIA-923 fallback (Woerman 2023 §C.1 Tier 2)
# ---------------------------------------------------------------------------

def _lookup_eia923_heat_rates(gen_to_resource, eia, eia923):
    """Look up an EIA-923 plant×PM×fuel heat rate for each ERCOT resource.

    For each resource that reaches at least one EIA generator via Tier B, we
    aggregate the EIA-923 (plant, prime_mover, fuel) heat rates over all
    matched generators using fuel-weighted means (sum_elec_fuel_mmbtu /
    sum_net_gen_mwh). Falls back from (plant, PM, fuel) → (plant, PM, ALL) →
    (plant, ALL, ALL) when finer granularities are missing. Plausibility
    filtering is the caller's responsibility.

    Args:
        gen_to_resource: output of ``_match_eia_gens_to_resources``.
        eia: EIA Form 860 DataFrame from ``_load_eia860`` — must include
            ``prime_mover_code`` and ``energy_source_1`` columns
            (populated by ``pull_eia860.py``).
        eia923: ``build_eia923_heat_rates`` output (stacked granularities).

    Returns:
        DataFrame with columns:
            resource_name, eia923_heat_rate_mmbtu_mwh, eia923_source
            (one of 'eia923_pm_fuel', 'eia923_pm', 'eia923_plant', or NA).
    """
    cols_needed = {"prime_mover_code", "energy_source_1"}
    missing = cols_needed - set(eia.columns)
    if missing:
        print(f"  WARN: EIA-860 missing columns {missing}; "
              "re-run download_data.pull_eia860 to enable EIA-923 fallback")
        return pd.DataFrame(columns=["resource_name", "eia923_heat_rate_mmbtu_mwh", "eia923_source"])

    eia_keys = eia.copy()
    eia_keys["plant_code"] = eia_keys["plant_code"].astype(int)
    eia_keys["generator_id"] = eia_keys["generator_id"].astype(str)
    eia_keys["prime_mover_code"] = eia_keys["prime_mover_code"].astype(str).str.upper().str.strip()
    eia_keys["energy_source_1"] = eia_keys["energy_source_1"].astype(str).str.upper().str.strip()

    matched = gen_to_resource.dropna(subset=["resource_name"]).copy()
    matched["plant_code"] = matched["plant_code"].astype(int)
    matched["generator_id"] = matched["generator_id"].astype(str)
    matched = matched.merge(
        eia_keys[["plant_code", "generator_id", "prime_mover_code", "energy_source_1"]],
        on=["plant_code", "generator_id"], how="left",
    )

    def to_lookup(df, key_cols):
        out = {}
        for r in df.itertuples(index=False):
            key = tuple(getattr(r, c) for c in key_cols)
            if key in out:
                continue
            mmbtu = float(r.elec_fuel_mmbtu) if pd.notna(r.elec_fuel_mmbtu) else 0.0
            mwh = float(r.net_gen_mwh) if pd.notna(r.net_gen_mwh) else 0.0
            out[key] = (mmbtu, mwh)
        return out

    rate_pm_fuel = to_lookup(
        eia923[eia923["fuel_type"] != "ALL"],
        ["plant_code", "prime_mover_code", "fuel_type"],
    )
    rate_pm = to_lookup(
        eia923[(eia923["fuel_type"] == "ALL") & (eia923["prime_mover_code"] != "ALL")],
        ["plant_code", "prime_mover_code"],
    )
    rate_plant = to_lookup(
        eia923[(eia923["fuel_type"] == "ALL") & (eia923["prime_mover_code"] == "ALL")],
        ["plant_code"],
    )

    def lookup_row(plant, pm, fuel):
        v = rate_pm_fuel.get((plant, pm, fuel))
        if v is not None:
            return v[0], v[1], "eia923_pm_fuel"
        v = rate_pm.get((plant, pm))
        if v is not None:
            return v[0], v[1], "eia923_pm"
        v = rate_plant.get((plant,))
        if v is not None:
            return v[0], v[1], "eia923_plant"
        return None, None, None

    out = []
    for resource_name, group in matched.groupby("resource_name"):
        num = 0.0
        denom = 0.0
        sources = []
        for _, gen in group.iterrows():
            mmbtu, mwh, src = lookup_row(
                int(gen["plant_code"]), gen["prime_mover_code"], gen["energy_source_1"],
            )
            if mmbtu is None or mwh is None or pd.isna(mmbtu) or pd.isna(mwh) or mwh <= 0:
                continue
            num += float(mmbtu)
            denom += float(mwh)
            sources.append(src)
        if denom <= 0 or not sources:
            out.append((resource_name, pd.NA, None))
            continue
        rank = {"eia923_pm_fuel": 0, "eia923_pm": 1, "eia923_plant": 2}
        worst_source = max(sources, key=lambda s: rank.get(s, 99))
        out.append((resource_name, num / denom, worst_source))

    rates_df = pd.DataFrame(
        out, columns=["resource_name", "eia923_heat_rate_mmbtu_mwh", "eia923_source"]
    )
    rates_df["eia923_heat_rate_mmbtu_mwh"] = pd.to_numeric(
        rates_df["eia923_heat_rate_mmbtu_mwh"], errors="coerce"
    )
    return rates_df


# ---------------------------------------------------------------------------
# Top-level pipeline
# ---------------------------------------------------------------------------

def build_crosswalk(year, force_rebuild=False):
    """Build the CEMS↔ERCOT crosswalk and per-resource heat rates for one year.

    Args:
        year: Operating year (int).
        force_rebuild: Overwrite cached outputs if True.

    Returns:
        Tuple (crosswalk_df, resource_heat_rates_df).
        crosswalk_df: one row per (facilityId, unitId, resource_name) linkage.
        resource_heat_rates_df: one row per ERCOT thermal resource with the
            heat rate and source label.
    """
    dirs = setup_directories()
    cw_path = Path(dirs["processed"]) / f"cems_ercot_crosswalk_{year}.parquet"
    hr_path = Path(dirs["processed"]) / f"resource_heat_rates_{year}.parquet"

    # if cw_path.exists() and hr_path.exists() and not force_rebuild:
    #     print(f"  Cached: {cw_path.name}, {hr_path.name}")
    #     return pd.read_parquet(cw_path), pd.read_parquet(hr_path)

    print(f"  Loading thermal DAM resources for {year}...")
    thermal = _load_thermal_resources(year)
    print(f"    {len(thermal)} thermal resources")

    print("  Loading EIA 860 generators...")
    eia = _load_eia860()
    n_with_lmp = eia["lmp_clean"].notna().sum()
    print(f"    {len(eia)} generators across {eia['plant_code'].nunique()} plants "
          f"({n_with_lmp} with lmp_node)")

    print("  Loading CEMS unit annual aggregates...")
    cems = _load_cems_annual(year)
    print(f"    {len(cems)} CEMS units across {cems['facilityId'].nunique()} facilities")

    print("  Loading SCED thermal annual net generation...")
    sced_annual = build_sced_thermal_annual(year, force_rebuild=False)
    print(f"    {len(sced_annual)} thermal resources with SCED net generation, "
          f"total {sced_annual['net_gen_mwh_annual'].sum() / 1e6:.1f} TWh")

    print("  Tier A: matching CEMS units → EIA generators within facility...")
    unit_match = _match_cems_units_to_eia_gens(cems[["facilityId", "unitId"]], eia)
    strat_a = unit_match["unit_match_strategy"].value_counts().to_dict()
    print(f"    {strat_a}")

    print("  Tier B: matching EIA generators → ERCOT Resource Names (direct)...")
    gen_to_resource = _match_eia_gens_to_resources(eia, thermal["resource_name"])
    strat_b = gen_to_resource["gen_match_strategy"].value_counts().to_dict()
    print(f"    {strat_b}")
    matched_gens = gen_to_resource.dropna(subset=["resource_name"])
    print(f"    {matched_gens['generator_id'].nunique()} EIA generators reach "
          f"{matched_gens['resource_name'].nunique()} ERCOT resources")

    print("  Assigning unit/plant aggregation groups...")
    cems_with_gens = cems[["facilityId", "unitId"]].merge(
        unit_match, on=["facilityId", "unitId"], how="left"
    )
    unit_groups, resource_groups = _assign_groups(cems_with_gens, gen_to_resource)
    n_unit_facilities = unit_groups[unit_groups["group_level"] == "unit"]["facilityId"].nunique()
    n_plant_facilities = unit_groups[unit_groups["group_level"] == "plant"]["facilityId"].nunique()
    print(f"    unit-level facilities: {n_unit_facilities}, "
          f"plant-level fallback: {n_plant_facilities}")

    print("  Computing group heat rates from CEMS heat input ÷ SCED net generation...")
    rates = _compute_heat_rates_per_group(cems, unit_groups, resource_groups, sced_annual)

    cw = (
        cems[["facilityId", "facilityName", "unitId", "unitType", "primaryFuelInfo",
              "heat_input_mmbtu", "so2_mass_lbs", "nox_mass_lbs", "operating_hours"]]
        .merge(unit_match, on=["facilityId", "unitId"], how="left")
        .merge(unit_groups[["facilityId", "unitId", "group_key", "group_level"]],
               on=["facilityId", "unitId"], how="left")
        .merge(
            resource_groups[["group_key", "resource_name"]].drop_duplicates(),
            on="group_key", how="left",
        )
    )
    cw.to_parquet(cw_path, index=False)
    print(f"  Saved crosswalk to {cw_path}")

    resource_to_group = resource_groups[["resource_name", "group_key", "group_level"]].drop_duplicates()
    rates_per_resource = resource_to_group.merge(
        rates[["group_key", "heat_rate_mmbtu_mwh", "so2_rate_lb_mwh", "nox_rate_lb_mwh",
               "cems_heat_input_mmbtu", "sced_net_gen_mwh", "n_cems_units", "n_resources"]],
        on="group_key", how="left",
    )
    rates_per_resource["heat_rate_source"] = rates_per_resource["group_level"].map({
        "unit":  "cems_unit",
        "plant": "cems_plant",
    })

    out = thermal[["resource_name", "resource_type"]].merge(
        rates_per_resource, on="resource_name", how="left"
    )
    lo, hi = _plausibility_bounds(out["resource_type"])
    implausible = out["heat_rate_mmbtu_mwh"].notna() & (
        (out["heat_rate_mmbtu_mwh"] < lo) | (out["heat_rate_mmbtu_mwh"] > hi)
    )
    if implausible.any():
        n_unit = (implausible & (out["heat_rate_source"] == "cems_unit")).sum()
        n_plant = (implausible & (out["heat_rate_source"] == "cems_plant")).sum()
        print(f"  Demoting {implausible.sum()} CEMS-derived rates "
              f"({n_unit} cems_unit, {n_plant} cems_plant) — outside plausible range")
    out.loc[implausible, "heat_rate_mmbtu_mwh"] = pd.NA
    out.loc[implausible, "heat_rate_source"] = pd.NA

    print(f"  Tier C: looking up EIA-923 heat rates ({year})...")
    try:
        eia923 = build_eia923_heat_rates(year, force_rebuild=False)
        eia923_rates = _lookup_eia923_heat_rates(gen_to_resource, eia, eia923)
        out = out.merge(eia923_rates, on="resource_name", how="left")
        needs_923 = out["heat_rate_mmbtu_mwh"].isna() & out["eia923_heat_rate_mmbtu_mwh"].notna()
        plausible_923 = (out["eia923_heat_rate_mmbtu_mwh"] >= lo) & (out["eia923_heat_rate_mmbtu_mwh"] <= hi)
        accept_923 = needs_923 & plausible_923
        n_demoted_923 = int((needs_923 & ~plausible_923).sum())
        out.loc[accept_923, "heat_rate_mmbtu_mwh"] = out.loc[accept_923, "eia923_heat_rate_mmbtu_mwh"]
        out.loc[accept_923, "heat_rate_source"] = out.loc[accept_923, "eia923_source"]
        print(f"    Accepted {int(accept_923.sum())} EIA-923 heat rates "
              f"({n_demoted_923} demoted for implausibility); "
              f"sources: {out.loc[accept_923, 'heat_rate_source'].value_counts().to_dict()}")
        out = out.drop(columns=["eia923_heat_rate_mmbtu_mwh", "eia923_source"])
    except (FileNotFoundError, RuntimeError, requests.RequestException) as e:
        print(f"  WARN: EIA-923 fallback unavailable ({type(e).__name__}: {e}); "
              "skipping Tier C and using tech_default for residuals")

    needs_default = out["heat_rate_mmbtu_mwh"].isna()
    out.loc[needs_default, "heat_rate_mmbtu_mwh"] = (
        out.loc[needs_default, "resource_type"].map(TECH_DEFAULT_HEAT_RATE)
    )
    out.loc[needs_default, "heat_rate_source"] = "tech_default"
    out.to_parquet(hr_path, index=False)
    print(f"  Saved resource heat rates to {hr_path}")

    return cw, out


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    """Build the CEMS↔ERCOT crosswalk + per-resource heat rates for the year."""
    parser = argparse.ArgumentParser(description="Build CEMS → ERCOT DAM crosswalk")
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    print(f"\n=== CEMS ↔ ERCOT crosswalk for {args.year} ===")
    cw, resource_hr = build_crosswalk(args.year, force_rebuild=args.force)

    print(f"\n  Crosswalk rows: {len(cw):,}")
    print(f"  Resources covered (thermal): {len(resource_hr)}")
    print(f"  By heat rate source:")
    print(resource_hr["heat_rate_source"].value_counts().to_string())

    print(f"\n  Heat-rate summary by source (MMBtu/MWh):")
    summary = (
        resource_hr.groupby("heat_rate_source")["heat_rate_mmbtu_mwh"]
        .agg(["count", "mean", "min", "max"]).round(2)
    )
    print(summary.to_string())


if __name__ == "__main__":
    main()
