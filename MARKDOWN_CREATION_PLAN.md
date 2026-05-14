# Day-Ahead Market Markup Estimation Plan — ERCOT Thermal Generators (2025)

Methodology follows Woerman (2023) *Market Size and Market Power: Evidence from the Texas Electricity Market*, adapted from real-time to **day-ahead** market and updated to ERCOT's current data products. The core identity is:

```
markup(unit, hour) = offer_price(unit, hour, Q) − marginal_cost(unit, hour)
marginal_cost      = heat_rate × fuel_price + Σ (emissions_rate_p × permit_price_p) + variable_O&M (omitted, absorbed by unit FE in regressions)
```

Offer curves come from ERCOT's **60-Day DAM Disclosure** (NP3-966-ER). Heat rates and emissions rates come from **EPA CEMS** divided by **ERCOT SCED net generation** (per Woerman §C.1). Fuel prices come from EIA / S&P / NGI. Permit prices come from EPA market reports. Markups are evaluated at three fixed capacity-factor points (65%, 75%, 85% of HSL) to match Woerman §4.2.

---

## 0. Pre-flight: API credentials and tested endpoints

| Source | Endpoint | Auth | Tested? | Notes |
|---|---|---|---|---|
| **ERCOT Public Reports API** | `https://api.ercot.com/api/public-reports/archive/NP3-966-ER` | OAuth2 Bearer + `Ocp-Apim-Subscription-Key` | ✅ **Verified working** — returned 4,396 archives, downloaded one 7.4 MB zip and parsed `60d_DAM_Gen_Resource_Data` with all 10 (MW, Price) offer-curve columns plus `Resource Type`, `HSL`, `LSL`, `Awarded Quantity`, `Resource Status`, etc. | Credentials in `~/keys/ercot_*.txt` already used by `download_data/pull_ercot.py` |
| **ERCOT SCED 60-Day Disclosure** (NP3-965-ER) | `https://api.ercot.com/api/public-reports/archive/NP3-965-ER` | Same | ✅ already downloaded for all 2025 in `{raw}/ercot/sced/` (per `process_curtailment.py`) | Reused for hourly net-generation when computing heat rates |
| **EPA CAMD EASEY API** (hourly CEMS) | `https://api.epa.gov/easey/streaming-services/emissions/apportioned/hourly` and `…/camd-services/bulk-files` | `x-api-key` header | ❌ **Requires free API key** — `403 API_KEY_MISSING`. Register at https://www.epa.gov/power-sector/cam-api-portal → save to `~/keys/epa_camd_key.txt`. | Returns hourly heat input (MMBtu), gross load (MWh), SO₂ / NOₓ / CO₂ mass (lb / short tons) per ORISPL × Unit |
| **EIA Open Data API v2** | `https://api.eia.gov/v2/natural-gas/pri/sum/data` and `…/coal/shipments/by-mine-by-plant/data` | `api_key` query param | ❌ **Requires free API key** — register at https://www.eia.gov/opendata/register.php → `~/keys/eia_api_key.txt`. | For Henry Hub daily, monthly Texas city-gate, monthly delivered coal price (Form 923 receipts), and Form 860 generator metadata |
| **EIA Bulk files** (Forms 860, 923) | `https://www.eia.gov/electricity/data/eia860/` and `…/eia923/` | None | ✅ Pull annual ZIPs directly | Already partially used (`pull_eia860.py`) |
| **Waha / Houston Ship Channel daily price** | S&P Global Market Intelligence (gold-standard, paid) | Paid subscription | ❓ Not in user's keys | See §3.4 for free fallbacks |
| **gridstatus.io** | `https://api.gridstatus.io/v1/datasets/` | `x-api-key` (already in `~/keys/grid_status_key.txt`) | Mirrors ERCOT DAM resource data and Henry Hub | Optional secondary source for spot-checks |

**Action items before pipeline can run:**

1. Register and save `~/keys/epa_camd_key.txt` (EPA CAMD API key).
2. Register and save `~/keys/eia_api_key.txt` (EIA API key).
3. Decide fuel-price strategy (§3.4) — recommend Henry Hub daily (EIA) + monthly Waha basis differential from EIA *Natural Gas Monthly* Table 3 as a default, with S&P Global if available later.

---

## 1. Scope and definitions

- **Period:** Jan 1 – Dec 31, 2025 (operating days). Use **delivery date** (the day for which DAM cleared at 10 a.m. on D-1), not posting date.
- **Market:** ERCOT Day-Ahead Market (DAM), 10 a.m. D-1 auction, hourly delivery.
- **Units in scope (thermal):** `Resource Type` in `{CCGT90, CCLE90, CCGT00, CCLE00, SCGT90, SCLE90, GSNONCCGT90, GSREH, STEAM, COAL, NUC}` — i.e., everything that converts fuel→electricity via a thermal cycle. Exclude `PVGR` (solar), `WIND`, `PWRSTR` (battery), `RUNRR` etc. Confirm exact set with `df['Resource Type'].value_counts()` against one DAM Gen Resource Data file.
- **Firm definition:** match `QSE` and DME (Decision Making Entity) columns to ultimate parent via S&P Global ownership; if unavailable, use the public ERCOT Resource Entity registry. Woerman matches Form 860 + S&P; the simpler proxy is `Resource Entity` from the SP NP4-160 mapping file already in `{raw}/ercot/np4_160/`.
- **Markup points** (Woerman §4.2): markup is a function of quantity, so evaluate at q = 0.65·HSL, 0.75·HSL, 0.85·HSL by linear-interpolating the offer curve (`(MW1, Price1), …, (MW10, Price10)`). Drop hours where the unit's `Resource Status` ∉ `{ON, ONTEST, ONEMR}` — i.e., not committed to produce in DAM.

---

## 2. Output schema

Final analysis-ready table: `{processed}/dam_markups/dam_markups_2025_{mm}.parquet` — one row per (unit, delivery_date, hour_ending).

| Column | Type | Description |
|---|---|---|
| `delivery_date` | date | DAM delivery date (Central) |
| `hour_ending` | int | 1–24 (DST handled per ERCOT convention) |
| `resource_name` | str | ERCOT Resource Name (e.g. `WAP_WAPG1`) |
| `resource_type` | str | CCGT90, COAL, NUC, etc. |
| `qse` | str | QSE submitting the offer |
| `firm` | str | Ultimate parent (via ownership crosswalk) |
| `settlement_point` | str | Resource Node name |
| `hsl_mw`, `lsl_mw` | float | Submitted HSL/LSL for the hour |
| `awarded_mw` | float | DAM cleared quantity |
| `dam_lmp` | float | Energy Settlement Point Price ($/MWh) |
| `offer_price_85`, `offer_price_75`, `offer_price_65` | float | Offer price ($/MWh) at 0.85, 0.75, 0.65 × HSL (linearly interpolated) |
| `heat_rate_mmbtu_per_mwh` | float | Unit's CEMS-derived average heat rate (constant by unit, see §3.2) |
| `fuel_price_mmbtu` | float | Daily delivered fuel price for unit's hub ($/MMBtu) |
| `fuel_cost` | float | `heat_rate × fuel_price + 0.10` ($/MWh) |
| `so2_rate_lb_per_mwh`, `nox_rate_lb_per_mwh` | float | CEMS-derived average emissions rates |
| `so2_price`, `nox_price` | float | Permit prices ($/lb) for the day |
| `emissions_cost` | float | Σ rate × price, only over pollutants the unit is regulated for ($/MWh) |
| `marginal_cost` | float | `fuel_cost + emissions_cost` ($/MWh) |
| `markup_85`, `markup_75`, `markup_65` | float | Offer price minus marginal cost at each quantity ($/MWh) |
| `lerner_85`, `lerner_75`, `lerner_65` | float | `markup / offer_price` |

---

## 3. Pipeline steps

### 3.1 — Download DAM 60-Day Disclosure for 2025 (NP3-966-ER)

**New script:** `download_data/pull_dam_disclosure.py`

- Reuses `load_credentials()` and `get_bearer_token()` from `download_data/pull_ercot.py`.
- For each delivery month `m ∈ 1..12`, the 60-Day DAM release window is approximately `[m+2 month start, m+2 month end]` (e.g., January 2025 deliveries are released early March 2025). To be safe, query archives posted in `[m+2 - 5d, m+3 + 5d]`.
- Endpoint: `GET /archive/NP3-966-ER?postDatetimeFrom={iso}&postDatetimeTo={iso}&size=1000`.
- Each archive ZIP contains 13 CSVs. Keep only `60d_DAM_Gen_Resource_Data-*.csv`. The filename date is the **release date**; the operating date is in the `Delivery Date` column inside — filter on `Delivery Date.dt.month == m`. (Same caveat as `process_curtailment.py`.)
- Concatenate per month, save to `{raw}/ercot/dam_disclosure/2025/{mm}/dam_gen_resource_{YYYYMM}.parquet`.
- Verify: row count should be roughly `n_thermal_units × 24 × days_in_month`. For a typical 2025 month this is on the order of 250 thermal units × 720 ≈ 180k rows; total ~5k rows from one file × 60 release days = compressed to ~180k rows per month after dedup.

### 3.2 — Build CEMS-to-ERCOT crosswalk and pull EPA CEMS (heat rate + emissions)

**New script:** `download_data/pull_epa_cems.py`

- After `~/keys/epa_camd_key.txt` is registered, list bulk-files for hourly emissions, state TX, 2025: `GET https://api.epa.gov/easey/camd-services/bulk-files` with header `x-api-key: {key}`. Filter to `dataType=Emissions, dataSubType=Hourly, stateCode=TX, year=2025`.
- Each bulk file is one quarter × state. Download the four 2025 TX quarters. Columns include `State, Facility Name, Facility ID (ORISPL), Unit ID, Date, Hour, Gross Load (MW), Steam Load (1000 lb), SO2 Mass (lbs), CO2 Mass (short tons), NOx Mass (lbs), Heat Input (mmBtu), Primary Fuel Type, Secondary Fuel Type, Operating Time, Unit Type, …`.
- Persist to `{raw}/cems/2025/{q}/cems_tx_{YYYYQ}.parquet`.

**New crosswalk module:** `process_data/cems_ercot_crosswalk.py`

Joining CEMS units (ORISPL, UnitID) to ERCOT Resource Names is the trickiest plumbing step. Recommended approach:

1. **EIA Form 860 → ORISPL:** existing `pull_eia860.py` already gives plant `ORISPL`, `Plant Name`, `Generator ID`, `Technology`, `Lat`, `Lon`. For Texas plants only.
2. **EIA 860 → ERCOT Resource Name:** Form 860 Sheet 1 ("Plant") has a column `BA Code = ERCO` for ERCOT plants and the schedule contains `LMP Node` (the ERCOT settlement point). Use this directly when present. For plants without that field populated, fall back to fuzzy/prefix matching on plant name → resource name (the same approach already used in `process_curtailment.geolocate_curtailment_resources`).
3. **Aggregate to ERCOT-unit granularity:** ERCOT "Resource" typically corresponds to one EIA generator (e.g., `WAP_WAPG1` ≈ `Welsh Power Plant Gen 1`). CEMS "Unit" usually corresponds to a boiler, which may map many-to-one (combined-cycle: 2 GTs + 1 ST → one ERCOT CCGT resource) or one-to-one. Use the EIA 860 *Boiler-Generator* association table to sum CEMS heat input and emissions across boilers feeding a single ERCOT resource.
4. **Persist:** `{processed}/cems_ercot_crosswalk.csv` with `(orispl, cems_unit_id, ercot_resource_name, eia_generator_id, technology)`. Manually inspect unmatched units; target ≥85% match by capacity (small SCGT peakers will be hardest).

**Compute unit-average heat rate and emissions rates (Woerman §C.1):**

```
heat_rate(unit) = Σ_t heat_input_mmbtu(t) / Σ_t net_generation_mwh(t)
SO2_rate(unit)  = Σ_t SO2_lbs(t)          / Σ_t net_generation_mwh(t)
NOx_rate(unit)  = Σ_t NOx_lbs(t)          / Σ_t net_generation_mwh(t)
```

- Net generation: take 5-minute SCED `Telemetered Net Output` from `60d_SCED_Gen_Resource_Data` (already in `{raw}/ercot/sced/`), average to hourly, then sum across all of 2025. CEMS reports *gross* load; per Woerman we use **net** from SCED so that "auxiliary load" inside the plant is accounted for. (CEMS heat input is total fuel burned — including aux — which is what we want.)
- For each CEMS unit, drop hours with operating time < 0.95 to limit transient start/stop noise (consistent with Woerman footnote 31).
- Units **<25 MW** are not in CEMS. Fall back to EIA Form 923: monthly fuel input by plant × prime mover × fuel type ÷ monthly net generation. Aggregate over 2025 to a single average. For units missing both, use the ERCOT-wide average heat rate for that (prime mover, fuel type) combination as the final fallback. Record the method in a `heat_rate_source` column (`cems`, `eia923_plant`, `eia923_typeavg`).

### 3.3 — EIA Form 923 for coal prices and small-unit fallback heat rates

**Script (new):** `download_data/pull_eia923.py`

- Download Form 923 annual ZIP for 2025 (released in stages; final usually Sept 2026). For 2025 markups computed in 2026Q2+, the *preliminary* monthly releases are sufficient — these are posted ~5 months after the operating month at https://www.eia.gov/electricity/data/eia923/.
- Sheets: `EIA923_Schedule_5_…` (generation + fuel by plant × prime mover × fuel) and `EIA923_Schedule_2_…` (fuel receipts and costs). Filter to plants in `BAlanceAuth = ERCO`.
- Coal price: from Schedule 2, filter `Fuel Group = Coal`, restrict to **spot** purchases only (per Woerman §C.1), compute weighted-average $/MMBtu by month across all ERCOT coal plants. Apply uniformly to every coal unit for that month.

### 3.4 — Natural gas hub prices

Woerman uses S&P Global daily spot prices at four hubs and matches by ERCOT load zone:
- West load zone → Waha
- North → Carthage
- South → Agua Dulce
- Houston → Katy

**Recommended free-data strategy (because no S&P access in `~/keys/`):**

1. **Henry Hub daily** from EIA series `NG.RNGWHHD.D` via `/v2/natural-gas/pri/fut/data` — verified to exist.
2. **Monthly Waha basis differential** (Waha − Henry Hub) — EIA's *Natural Gas Monthly* publishes monthly average Waha prices; compute the basis and apply it as a flat daily adjustment within each month. (Acceptable approximation in 2025; the Waha basis can be extreme, e.g., negative in late 2024, but using monthly average is the standard fallback when daily Waha is not free.)
3. **Carthage / Katy / Agua Dulce monthly averages** from EIA's *Natural Gas: Prices* citygate series and *Texas Natural Gas Monthly* survey, or fall back to Henry Hub + zero basis for hubs other than Waha if monthly survey data is sparse.
4. Add Woerman's **$0.10 / MMBtu distribution charge** on top.
5. Map unit → load zone → hub using the existing `np4_160` settlement-point mapping and the ERCOT weather-zone/load-zone shapefile already used in `prepare_node_level_data._map_nodes_to_weather_zones`.

**Optional upgrade path:** if S&P Global access becomes available, replace the daily Waha series with the true spot price. Keep the column name `fuel_price_mmbtu` stable so downstream code does not change.

**Persist:** `{processed}/fuel_prices/fuel_prices_2025.csv` with columns `(date, hub, price_per_mmbtu, source)`.

### 3.5 — Emissions permit prices

- **SO₂:** the Acid Rain Program SO₂ allowance market has essentially collapsed to near-zero prices since 2015. For 2025, use EPA's *SO₂ Allowance Auction* clearing price as a flat daily value (typically <$0.10/ton). Source: EPA CAMD market reports, https://www.epa.gov/airmarkets/allowance-markets. Manual download → `{raw}/permits/so2_prices_2025.csv`.
- **NOₓ:** for 2025, the relevant program is **CSAPR NOₓ Ozone-Season Group 3** (the program that supersedes the Texas/Northeast trading programs Woerman used). EPA CAMD market reports publish quarterly average allowance prices. Manual download → `{raw}/permits/nox_csapr_prices_2025.csv`. For non-ozone-season hours (October–April), the relevant program is CSAPR NOₓ Annual Group 3.
- **Which units are regulated for which pollutant:** use EPA's Air Markets Program Data (AMPD) "facility attributes" service via `https://api.epa.gov/easey/camd-services/facilities` (same EPA key). Pull `(facilityId, unitId, ProgramCodeInfo)` for TX in 2025. A unit with `OS NOX` program enrollment gets NOₓ permit cost during May–Sep; CSAPR Annual NOₓ year-round; ARP for SO₂ year-round.

**Cost calculation:** per Woerman §C.1,
```
emissions_cost($/MWh) = Σ_p [ rate_p (lb/MWh) × price_p ($/lb) ]
```
where the sum is only over pollutants the unit is enrolled in. SO₂ allowances are denominated in tons; divide by 2,000 to get $/lb.

### 3.6 — Combine to compute markup

**New script:** `process_data/compute_dam_markups.py`

Pseudocode:
```python
def compute_dam_markups(year=2025):
    crosswalk = pd.read_csv(processed/'cems_ercot_crosswalk.csv')          # ercot_resource → orispl/unit
    heat_rates = pd.read_parquet(processed/'unit_heat_rates_2025.parquet') # ercot_resource → heat_rate, so2_rate, nox_rate, fuel_type
    fuel_px    = pd.read_csv(processed/'fuel_prices/fuel_prices_2025.csv')  # (date, hub) → $/MMBtu
    permit_px  = pd.read_csv(processed/'permits/permit_prices_2025.csv')   # (date, pollutant) → $/lb
    resource_zone = pd.read_csv(processed/'resource_to_load_zone.csv')     # ercot_resource → load_zone → hub
    programs   = pd.read_csv(processed/'unit_emissions_programs.csv')      # ercot_resource → bool flags

    rows = []
    for m in range(1, 13):
        dam = pd.read_parquet(raw/f'ercot/dam_disclosure/2025/{m:02d}/dam_gen_resource_{2025}{m:02d}.parquet')
        dam = dam[dam['Resource Type'].isin(THERMAL_TYPES)]
        dam = dam[dam['Resource Status'].isin(['ON','ONTEST','ONEMR'])]
        dam = dam.merge(heat_rates, on='resource_name')                    # + heat_rate, rates, fuel_type
        dam = dam.merge(resource_zone, on='resource_name')                 # + hub
        dam['date'] = pd.to_datetime(dam['Delivery Date'])
        dam = dam.merge(fuel_px, on=['date','hub'])                        # + fuel_price_mmbtu
        dam['fuel_cost'] = dam['heat_rate'] * (dam['fuel_price_mmbtu'] + 0.10)
        dam = _add_emissions_cost(dam, permit_px, programs)                # + emissions_cost
        dam['marginal_cost'] = dam['fuel_cost'] + dam['emissions_cost']

        for cf in (0.85, 0.75, 0.65):
            dam[f'offer_price_{int(cf*100)}'] = _interp_offer_curve(dam, cf)
            dam[f'markup_{int(cf*100)}']      = dam[f'offer_price_{int(cf*100)}'] - dam['marginal_cost']
            dam[f'lerner_{int(cf*100)}']      = dam[f'markup_{int(cf*100)}'] / dam[f'offer_price_{int(cf*100)}']

        dam.to_parquet(processed/f'dam_markups/dam_markups_2025_{m:02d}.parquet')
```

`_interp_offer_curve` reshapes the 10 (MW, Price) wide columns into long, sorts by MW, ensures monotone non-decreasing in price (it must be per ERCOT protocol), then linearly interpolates the price at `cf × HSL`. If the requested quantity is above the last MW vertex, take the last price; if below the first vertex (rare since first vertex is usually at LSL), take the first price.

### 3.7 — Validation tests

Each download/process script ends with assertions that fail loudly:

1. **DAM coverage:** `dam_2025.groupby('Delivery Date').size() ≈ n_thermal_units × 24` for ≥97% of days. Investigate missing days (Woerman's process_curtailment notes 2025 has 363/365 SCED days; DAM disclosure has fewer gaps because release timing is more regular).
2. **CEMS coverage:** for each CEMS unit-hour 2025-01-01..2025-12-31, `cems.shape[0] / (n_units × 8760) ≥ 0.95`.
3. **Heat-rate sanity check:** for nat-gas CCGT units, computed heat rate should be in `[6.5, 9.0]` MMBtu/MWh; for SCGT in `[9.0, 14.0]`; for coal in `[9.0, 12.5]`. Flag any unit outside these bounds and check it manually before including in markups.
4. **Marginal cost order-of-magnitude:** at Henry Hub ≈ $3/MMBtu, a 7.5 MMBtu/MWh CCGT gives ~$22.5/MWh fuel cost; with $0.10 distribution that's $23.3. Coal at 10.5 MMBtu/MWh × $2.20/MMBtu ≈ $23. Both should land in the $15–$50 range for 2025.
5. **Offer-curve interpolation:** for at least one unit, plot the raw (MW, Price) pairs and overlay the interpolated values at 65/75/85% HSL; visually confirm monotonicity and that the interp lies on the curve.
6. **Markup distribution:** the median markup at 85% HSL across all thermal units in 2025 should be positive (firms typically markup above MC on the high-quantity side of the curve) and the bottom 10th percentile should be modestly negative (units willing to be price-takers near LSL to avoid startup costs — see Woerman §4.1 on the U-shape of offer curves). If 85% markups are everywhere negative something is wrong with marginal-cost calculation.

---

## 4. Execution order

```bash
# One-time setup
echo $EPA_KEY > ~/keys/epa_camd_key.txt
echo $EIA_KEY > ~/keys/eia_api_key.txt

# Step A: ERCOT DAM 60-day disclosure for all 2025 (~12 monthly archives × ~30 daily files; ~3-4 hours total)
uv run python -m download_data.pull_dam_disclosure --year 2025

# Step B: EPA CEMS hourly for TX 2025 Q1–Q4 (~30 min)
uv run python -m download_data.pull_epa_cems --state TX --year 2025

# Step C: EIA Form 923 for 2025 (annual + monthly preliminary; ~5 min)
uv run python -m download_data.pull_eia923 --year 2025

# Step D: Fuel prices (~5 min)
uv run python -m download_data.pull_fuel_prices --year 2025

# Step E: Permit prices (manual download then load) — see §3.5
# Place CSVs in {raw}/permits/ then validate
uv run python -c "from process_data.compute_dam_markups import validate_permit_inputs; validate_permit_inputs(2025)"

# Step F: Build CEMS ↔ ERCOT crosswalk (~2 min)
uv run python -c "from process_data.cems_ercot_crosswalk import build_crosswalk; build_crosswalk(force_rebuild=True)"

# Step G: Compute unit-average heat rates + emissions rates (~5 min)
uv run python -c "from process_data.compute_dam_markups import compute_unit_heat_rates; compute_unit_heat_rates(2025)"

# Step H: Compute markups (~10 min)
uv run python -c "
from process_data.compute_dam_markups import compute_dam_markups
compute_dam_markups(2025)
"
```

---

## 5. Deliverables

After running the pipeline:

- **Raw caches:**
  - `{raw}/ercot/dam_disclosure/2025/{mm}/dam_gen_resource_{YYYYMM}.parquet`
  - `{raw}/cems/2025/{q}/cems_tx_{YYYYQ}.parquet`
  - `{raw}/permits/{so2,nox_csapr}_prices_2025.csv`

- **Processed intermediates:**
  - `{processed}/cems_ercot_crosswalk.csv`
  - `{processed}/unit_heat_rates_2025.parquet` (heat rate, SO₂ rate, NOₓ rate, source method)
  - `{processed}/fuel_prices/fuel_prices_2025.csv`
  - `{processed}/resource_to_load_zone.csv`

- **Final analysis table:**
  - `{processed}/dam_markups/dam_markups_2025_{mm}.parquet` — one row per (thermal unit, hour) with markups at 65/75/85% HSL, ready to plug into the same regression framework already used in `analysis/cluster_heterogeneity_lr.py` and `analysis/extreme_weather_regressions.py`.

---

## 6. Known limitations / deviations from Woerman (2023)

1. **Day-ahead vs real-time.** Woerman studies real-time; this plan applies the same `offer − MC` definition to DAM offers. The economic interpretation is the same (markup above MC at a chosen quantity) but the DAM problem includes additional three-part-bid components (`Start Up Hot/Inter/Cold`, `Min Gen Cost`) which the energy-curve markup ignores. Acceptable for energy-market market-power analysis; flag in any write-up.
2. **Hourly vs constant heat rate.** Following Woerman §C.1, we use the unit-average heat rate over 2025 rather than hour-by-hour, because hourly CEMS÷SCED ratios are noisy and firms do not realistically forecast hourly heat rates when bidding. The hourly version can be added as a robustness check.
3. **Waha daily price proxy.** Without S&P Global, daily Waha is approximated by Henry Hub + monthly basis. This is the largest single uncertainty in the marginal-cost build; the Waha basis is volatile and occasionally extreme. Document and revisit if S&P access becomes available.
4. **Variable O&M omitted.** Woerman omits it too; in regressions it is absorbed by unit fixed effects. For descriptive markup levels reported in tables, add a $1–3/MWh typical adder if needed for level-comparability with literature, but never for within-unit variation analysis.
5. **Nuclear marginal cost.** Treat as a single constant per Deutch et al. (2009) — $8–10/MWh in 2025 dollars — applied to both STP units. ERCOT nuclear units do not have CEMS coverage so heat-rate path does not apply.

Sources:
- [Clean Air Markets API Portal | US EPA](https://www.epa.gov/power-sector/cam-api-portal)
- [EIA Open Data registration](https://www.eia.gov/opendata/register.php)
- [ERCOT MIS NP3-966-ER 60-Day DAM Disclosure](https://www.ercot.com/mp/data-products/data-product-details?id=NP3-966-ER)
- [EPA Hourly CEMS data | CAMPD](https://campd.epa.gov/data)
- [EIA Form 923 electric power data](https://www.eia.gov/electricity/data/eia923/)
- [EPA Allowance Markets / permit prices](https://www.epa.gov/airmarkets/allowance-markets)
