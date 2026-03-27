# Research Plan: Where Should We Invest in Better Forecasts to Reduce Congestion Risk?

## Reframed Research Question

**"Where on the grid do weather forecast errors create congestion risk, and what is the spatially heterogeneous value of forecast accuracy for ERCOT grid resilience during extreme weather events?"**

The core empirical finding motivating this plan: a 1 m/s HRRR wind speed forecast error near Houston increases system LMP dispersion 2× more than the same error in the Panhandle, and winter amplifies all effects 3–5×. The economic value of improving weather forecasts is *spatially heterogeneous* and *regime-dependent*. This plan extends the existing pipeline to (a) use direct congestion measures instead of LMP proxies, (b) isolate extreme weather events, and (c) quantify the dollar value of targeted forecast improvements at each grid cell.

---

## Overview of New Work

| Phase | Description | New Data? | New Code? | Timeline |
|-------|-------------|-----------|-----------|----------|
| **A** | Download SCED shadow prices & binding constraints | Yes | Yes | ~2 days |
| **B** | Download 60-day SCED disclosure (curtailment) | Yes | Yes | ~2 days |
| **C** | Process congestion variables & merge into pixel data | No new download | Yes | ~1 day |
| **D** | Define extreme weather regimes | No | Yes | ~0.5 day |
| **E** | Run new regressions (direct congestion + extremes) | No | Yes | ~1 day |
| **F** | Build "forecast value map" | No | Yes | ~1 day |
| **G** | Test asymmetry (over- vs under-forecast) | No | Yes | ~0.5 day |
| **H** | Updated report and figures | No | Yes | ~1 day |

---

## Phase A: Download SCED Shadow Prices & Binding Constraints

### What this data provides
Shadow prices are the most direct measure of congestion in ERCOT. When a transmission constraint binds, the shadow price ($/MW) represents the marginal cost of relieving 1 MW of congestion on that element. This replaces the indirect proxy of `system_lmp_std` with a direct measure of *which constraints are binding and how costly they are*.

### Data source
- **ERCOT Report**: NP6-86-CD — "SCED Shadow Prices and Binding Transmission Constraints"
- **ERCOT API endpoint**: `/np6-86-cd/shdw_prices_bnd_trns_const`
- **Frequency**: Every SCED interval (~5 min), published hourly
- **Fields**: `SCEDTimestamp`, `constraintName`, `contingencyName`, `shadowPrice`, `maxShadowPrice`, `fromStation`, `fromStationKV`, `toStation`, `toStationKV`, `CCTStatus`, `violatedMW`, `limit`, `actualFlow`
- **Auth**: Same ERCOT OAuth2 + subscription key already in `download_data/pull_ercot.py`

### New script: `download_data/pull_sced_shadow.py`

```python
"""
Download SCED shadow prices and binding transmission constraints from ERCOT API.

ERCOT Report: NP6-86-CD
Endpoint: /np6-86-cd/shdw_prices_bnd_trns_const

Output: {raw}/ercot/sced_shadow/2025/{mm}/shadow_{YYYYMMDD}.csv
"""
```

**Implementation steps**:

1. **Reuse existing auth** from `pull_ercot.py`:
   - Import `load_credentials()`, `get_bearer_token()`, `ercot_request()` from `download_data.pull_ercot`
   - Or refactor shared auth into `download_data/ercot_auth.py` to avoid circular imports

2. **API request pattern** (follows existing `pull_ercot.py` exactly):
   ```python
   endpoint = "/np6-86-cd/shdw_prices_bnd_trns_const"
   params = {
       "SCEDTimestamp": f">={start_date}T00:00:00",
       "SCEDTimestamp": f"<={end_date}T23:59:59",
   }
   # Use existing ercot_request() which handles pagination, rate limits, 429s
   ```

3. **Download one day at a time** (shadow data is high-volume: ~288 SCED intervals/day × many constraints):
   ```python
   def download_shadow_month(year, month):
       for day in range(1, days_in_month + 1):
           out_path = raw / "ercot" / "sced_shadow" / str(year) / f"{month:02d}" / f"shadow_{year}{month:02d}{day:02d}.csv"
           if out_path.exists():
               continue  # skip already downloaded
           data = ercot_request(endpoint, params, api_key, token)
           pd.DataFrame(data).to_csv(out_path, index=False)
   ```

4. **Output schema**:
   ```
   {raw}/ercot/sced_shadow/2025/{mm}/shadow_{YYYYMMDD}.csv
   ```
   Columns: `SCEDTimestamp, constraintName, contingencyName, shadowPrice, maxShadowPrice, fromStation, fromStationKV, toStation, toStationKV, CCTStatus, violatedMW, limit, actualFlow`

5. **Volume estimate**: ~200–500 MB/month (varies with number of binding constraints)

### Alternative: `gridstatus` Python library
The open-source [`gridstatus`](https://opensource.gridstatus.io/) library has an `ercot.get_sced_shadow_prices()` wrapper that may simplify download. Evaluate whether it handles pagination and auth correctly before committing to a custom implementation.

```python
import gridstatus
ercot = gridstatus.Ercot()
shadow = ercot.get_sced_shadow_prices(date="2025-07-15")
```

**Decision**: If `gridstatus` works reliably, use it. If not, fall back to direct API calls using the existing `ercot_request()` pattern.

---

## Phase B: Download 60-Day SCED Disclosure (Curtailment Data)

### What this data provides
The 60-Day SCED Disclosure contains unit-level output and High Sustained Limits (HSL) for every generating unit in each SCED interval. For wind and solar units, **curtailment = HSL − actual output** when HSL > output. This gives a direct, unit-level measure of renewable curtailment driven by congestion.

### Data source
- **ERCOT Report**: NP3-966-ER — "60-Day SCED Disclosure"
- **Access**: ERCOT MIS portal (ZIP file downloads, not API)
- **URL pattern**: `https://www.ercot.com/misdownload/servlets/mirDownload?dession=...`
- **Frequency**: Released with 60-day lag (so data for Jan 2025 available ~Mar 2025)
- **File format**: Large CSVs inside ZIP archives, one per day

### New script: `download_data/pull_sced_disclosure.py`

```python
"""
Download 60-Day SCED Disclosure from ERCOT MIS portal.

ERCOT Report: NP3-966-ER
Contains per-unit output, HSL, and telemetry for each SCED interval.

Output: {raw}/ercot/sced_disclosure/2025/{mm}/sced_{YYYYMMDD}.csv
"""
```

**Implementation steps**:

1. **Manual vs automated download**: ERCOT MIS downloads require browser-style session handling. Options:
   - **Option A**: Use `requests` with session cookies (may require scraping the MIS login page)
   - **Option B**: Download manually from ERCOT MIS, store in OneDrive, write processing script only
   - **Recommendation**: Start with Option B (manual download for 12 months) while building Option A for automation

2. **Key columns to extract**:
   - `Resource_Name`: unit identifier (maps to settlement point via NP4-160)
   - `Resource_Type`: `WIND`, `SOLAR`, `GAS_CC`, etc.
   - `Interval_Time`: SCED timestamp
   - `Telemetered_Net_Output`: actual output [MW]
   - `HSL`: High Sustained Limit [MW] (max available output)
   - `Base_Point`: SCED dispatch instruction [MW]

3. **Curtailment calculation**:
   ```python
   # For wind/solar units only:
   curtailment_mw = max(0, HSL - Telemetered_Net_Output)
   # Flag: was this unit curtailed?
   is_curtailed = (HSL - Telemetered_Net_Output) > threshold_mw  # e.g., 5 MW
   ```

4. **Output**: Daily CSVs with curtailment computed per unit per interval
   ```
   {raw}/ercot/sced_disclosure/2025/{mm}/sced_{YYYYMMDD}.csv
   ```

### Volume & storage
- Raw SCED disclosure is ~500 MB–1 GB per month (compressed)
- After filtering to wind/solar units and aggregating, much smaller

---

## Phase C: Process Congestion Variables & Merge into Pixel Data

### New script: `process_data/process_congestion.py`

This script creates multiple congestion variables from the shadow price and curtailment data, then merges them into the existing pixel × hour dataset.

### C.1: Constraint-level shadow prices → system hourly congestion metrics

```python
def compute_hourly_congestion_metrics(year, month):
    """
    From SCED shadow prices, compute hourly system-level congestion metrics.

    Returns DataFrame with columns:
        valid_time:           datetime [US/Central]
        n_binding_constraints: number of unique constraints binding in the hour
        total_shadow_cost:    sum of (shadow_price × |violatedMW|) across all constraints
        max_shadow_price:     maximum shadow price in the hour [$/MW]
        mean_shadow_price:    mean shadow price across binding constraints
        n_violations:         number of constraints where CCTStatus = 'Violation'
        total_violated_mw:    sum of violated MW across all violated constraints
    """
```

**Aggregation logic**:
1. Load daily shadow CSVs for the month
2. Parse `SCEDTimestamp` → round to hour
3. Group by hour:
   - Count distinct `constraintName` → `n_binding_constraints`
   - Sum `shadowPrice * abs(violatedMW)` → `total_shadow_cost` (proxy for congestion rent)
   - Max `shadowPrice` → `max_shadow_price`
   - Count rows where `CCTStatus == 'Violation'` → `n_violations`
4. Return hourly DataFrame

### C.2: Constraint-level → spatial congestion (geolocated)
XX This might not work, instead also compare to merging using identified nodes from the build_node_coordinates function in process_ercot.py

```python
def geolocate_constraints(shadow_df, bus_gdf):
    """
    Map fromStation/toStation in shadow data to lat/lon using Bus_Output.shp.

    Steps:
    1. Load Bus_Output.shp (already in data/ directory)
    2. Fuzzy-match station names in shadow data to bus names in shapefile
    3. For each constraint, assign midpoint of (fromStation, toStation) as location
    4. Bin into ERA5 0.1° grid cells (same as pixel_id format)

    Returns DataFrame with pixel_id and constraint-level shadow prices.
    """
```

This enables a **spatial congestion variable** — at each pixel, the total shadow cost of constraints whose transmission elements pass through or near that pixel.

### C.3: Curtailment metrics

```python
def compute_hourly_curtailment(year, month):
    """
    From SCED disclosure, compute hourly wind/solar curtailment.

    Returns DataFrame with columns:
        valid_time:               datetime [US/Central]
        wind_curtailment_mw:     total wind curtailment [MW]
        solar_curtailment_mw:    total solar curtailment [MW]
        total_curtailment_mw:    wind + solar [MW]
        wind_curtailment_pct:    curtailment / HSL for wind [%]
        solar_curtailment_pct:   curtailment / HSL for solar [%]
        n_curtailed_units:       number of units being curtailed
    """
```

### C.4: Merge into pixel × hour dataset

**Modify `process_data/combine_forecast_generation_node.py`**:

Add a new function and update `build_pixel_hourly_dataset()`:

```python
def compute_congestion_hourly(year, month):
    """Load and return hourly congestion metrics from shadow prices."""
    from process_data.process_congestion import compute_hourly_congestion_metrics
    return compute_hourly_congestion_metrics(year, month)

def compute_curtailment_hourly(year, month):
    """Load and return hourly curtailment metrics from SCED disclosure."""
    from process_data.process_congestion import compute_hourly_curtailment
    return compute_hourly_curtailment(year, month)
```

In `build_pixel_hourly_dataset()`, add after the system LMP merge:
```python
# --- Congestion metrics (system-level, same for all pixels in the hour) ---
congestion = compute_congestion_hourly(year, month)
df = df.merge(congestion, on="valid_time", how="left")

# --- Curtailment metrics (system-level) ---
curtailment = compute_curtailment_hourly(year, month)
df = df.merge(curtailment, on="valid_time", how="left")
```

### New columns added to pixel × hour dataset

| Column | Type | Description |
|--------|------|-------------|
| `n_binding_constraints` | int | Binding constraints in the hour |
| `total_shadow_cost` | float | Sum of shadow_price × violated_MW [$/h] |
| `max_shadow_price` | float | Max shadow price across constraints [$/MW] |
| `n_violations` | int | Constraint violations in the hour |
| `wind_curtailment_mw` | float | Total wind curtailment [MW] |
| `solar_curtailment_mw` | float | Total solar curtailment [MW] |
| `total_curtailment_mw` | float | Wind + solar curtailment [MW] |
| `wind_curtailment_pct` | float | Wind curtailment as % of available [%] |
| `local_shadow_cost` | float | Shadow cost from constraints near this pixel [$/h] |

---

## Phase D: Define Extreme Weather Regimes

### Rationale
The existing seasonal splits (summer/winter/shoulder) are coarse. We want to isolate *extreme* weather events — the hours when forecast errors are most consequential for grid reliability. The literature shows winter storms and heat waves produce qualitatively different congestion patterns.

### New script: `process_data/classify_weather_regimes.py`

```python
"""
Classify each hour into weather regimes based on observed conditions.

Regimes:
1. Extreme cold:    system-avg ERA5 temp in bottom 5th percentile
2. Extreme heat:    system-avg ERA5 temp in top 5th percentile
3. High wind:       system-avg ERA5 wind speed in top 10th percentile
4. Normal:          everything else

Sub-regimes (optional):
- "stressed grid":  hours where system_lmp_max > 500 $/MWh (or top 5% of prices)
- "low wind":       wind speed in bottom 10th percentile (thermal stress + no wind)
"""

def classify_regimes(pixel_df, temp_col="era5_temp", wspd_col="era5_wspd"):
    """
    Add regime columns to pixel hourly DataFrame.

    Steps:
    1. Compute system-wide average ERA5 temp and wind per hour
       (average across all pixels for each valid_time)
    2. Compute percentile thresholds from full-year distribution
    3. Assign regime labels

    New columns:
        regime_temp:   'extreme_cold' | 'extreme_heat' | 'normal'
        regime_wind:   'high_wind' | 'low_wind' | 'normal'
        regime_grid:   'stressed' | 'normal'  (based on LMP threshold)
        regime_combined: concatenation, e.g. 'extreme_cold_high_wind_stressed'
    """
```

### Threshold calibration
Run once on the full 2025 dataset to determine thresholds:

```python
# Compute hourly system-average weather
hourly_means = pixel_df.groupby("valid_time").agg(
    sys_temp=("era5_temp", "mean"),
    sys_wspd=("era5_wspd", "mean"),
    sys_lmp_max=("system_lmp_max", "first"),
).reset_index()

# Define thresholds
temp_5  = hourly_means.sys_temp.quantile(0.05)   # extreme cold cutoff
temp_95 = hourly_means.sys_temp.quantile(0.95)   # extreme heat cutoff
wspd_90 = hourly_means.sys_wspd.quantile(0.90)   # high wind cutoff
lmp_95  = hourly_means.sys_lmp_max.quantile(0.95) # stressed grid cutoff
```

### Expected regime sizes (rough estimates for 2025)
- Extreme cold: ~438 hours (5% of 8,760)
- Extreme heat: ~438 hours
- High wind: ~876 hours (10%)
- Stressed grid: ~438 hours (5%)
- Combined extreme + stressed: ~100–200 hours (these are the events that matter most)

---

## Phase E: New Regressions

### E.1: Replace `system_lmp_std` with direct congestion measures

**Modify `analysis/pixel_regression_maps.py`**:

Run the existing per-pixel regression specification but with new dependent variables:

```python
NEW_DEPVARS = [
    "total_shadow_cost",          # Direct congestion cost
    "n_binding_constraints",      # Congestion breadth
    "max_shadow_price",           # Congestion severity
    "wind_curtailment_mw",        # Renewable curtailment
    "total_curtailment_mw",       # Total curtailment
]
```

For each depvar, run:
```
{depvar} ~ temp_error_1h + wspd_error_1h + temp_error_0h + wspd_error_0h
         + load_error_1h + load_error_dam
         + era5_temp + era5_wspd + actual_load + is_weekend
         | hour_of_day + month
```

**Execution**:
```python
for depvar in NEW_DEPVARS:
    run_pixel_regression_maps(
        months=DEFAULT_MONTHS,
        depvar=depvar,
        tag=depvar,
    )
```

**Expected output**: 5 new sets of 2×2 maps + summary CSVs showing which pixels' forecast errors significantly predict each congestion measure.

### E.2: Regime-conditional regressions

**New analysis script: `analysis/extreme_weather_regressions.py`**

```python
"""
Regime-conditional pixel regressions.

For each regime (extreme_cold, extreme_heat, high_wind, stressed_grid),
run per-pixel regressions on direct congestion measures.

Key question: During extreme cold events, where do forecast errors
have the largest effect on congestion?
"""

REGIMES = {
    "extreme_cold":  {"filter": "regime_temp == 'extreme_cold'",
                      "label": "Extreme Cold (Bottom 5% Temp)"},
    "extreme_heat":  {"filter": "regime_temp == 'extreme_heat'",
                      "label": "Extreme Heat (Top 5% Temp)"},
    "high_wind":     {"filter": "regime_wind == 'high_wind'",
                      "label": "High Wind (Top 10% Wind Speed)"},
    "stressed_grid": {"filter": "regime_grid == 'stressed'",
                      "label": "Stressed Grid (Top 5% LMP Max)"},
}

def run_regime_regressions(months, depvar="total_shadow_cost"):
    """
    For each regime:
    1. Filter pixel data to hours in that regime
    2. Run per-pixel regressions (same spec as pixel_regression_maps.py)
    3. Generate coefficient maps
    4. Save results

    Output:
        figures/extreme_weather/{depvar}_{regime}_2x2.png
        tables/extreme_weather_regression_{depvar}_{regime}.csv
    """
```

**Minimum observations**: Since extreme regimes are ~400–900 hours, per-pixel regressions may have low power for rarely-active pixels. Set minimum obs threshold to 50 (down from 100 in the full-year specification) and report effective sample sizes.

**FE adjustment**: With only ~400 hours in a regime, drop `month` FE (insufficient variation). Keep `hour_of_day` FE.

```
{depvar} ~ temp_error_1h + wspd_error_1h + temp_error_0h + wspd_error_0h
         + load_error_1h + load_error_dam
         + era5_temp + era5_wspd + actual_load + is_weekend
         | hour_of_day
```

### E.3: Infrastructure-level regressions with congestion DVs

**Modify `analysis/gridded_infrastructure_lr.py`**:

Run the existing capacity-weighted aggregation pipeline but with congestion dependent variables and regime interactions:

```python
# Main specification (full year)
total_shadow_cost ~ temp_error_1h_wind + wspd_error_1h_wind + ...
                  + interactions
                  | hour_of_day + month
                  , vcov={"CRV1": "date"}

# Regime interaction specification
total_shadow_cost ~ temp_error_1h_wind * regime_extreme
                  + wspd_error_1h_wind * regime_extreme
                  + ...
                  | hour_of_day + month
```

where `regime_extreme = 1` during extreme cold/heat/stressed hours.

### E.4: Forecast error asymmetry tests

**New analysis in `analysis/asymmetry_tests.py`**:

```python
"""
Test whether over-forecasts and under-forecasts have symmetric effects
on congestion. Hypothesis: under-forecasting wind during grid stress
is worse than over-forecasting.

Specification:
    {depvar} ~ wspd_error_pos_1h + wspd_error_neg_1h
             + temp_error_pos_1h + temp_error_neg_1h
             + wspd_error_pos_0h + wspd_error_neg_0h
             + temp_error_pos_0h + temp_error_neg_0h
             + controls | FE

where:
    wspd_error_pos_1h = max(0, wspd_error_1h)   # over-forecast wind
    wspd_error_neg_1h = min(0, wspd_error_1h)   # under-forecast wind
    (etc.)
"""

def create_asymmetric_vars(df):
    """Split each error variable into positive and negative parts."""
    for var in ["temp_error_1h", "wspd_error_1h", "temp_error_0h", "wspd_error_0h"]:
        df[f"{var}_pos"] = df[var].clip(lower=0)
        df[f"{var}_neg"] = df[var].clip(upper=0)
    return df
```

**Expected finding**: Wind under-forecasting (negative `wspd_error_neg`) should have a larger coefficient magnitude than over-forecasting (`wspd_error_pos`), especially during stressed grid conditions. This would imply asymmetric value of forecast improvements.

---

## Phase F: Build the "Forecast Value Map"

### Concept
Combine pixel-level regression coefficients with observed forecast error distributions to compute the **marginal value of a 1-unit forecast improvement** at each grid cell, expressed in $/MWh of reduced congestion cost.

### New analysis script: `analysis/forecast_value_map.py`

```python
"""
Compute the dollar value of forecast accuracy at each ERA5 pixel.

Method:
    For each pixel i and error variable e:
        value_ie = |β_ie| × σ(error_ie) × mean(depvar)

    where:
        β_ie     = regression coefficient from pixel regression
        σ(error_ie) = standard deviation of forecast error at pixel i
        mean(depvar) = mean congestion cost (normalizes to dollars)

    A "1-sigma improvement" at pixel i in variable e would reduce
    system congestion by approximately value_ie $/MWh.

    The map shows sum across error variables:
        total_value_i = Σ_e value_ie
"""

def compute_forecast_value(regression_results, pixel_error_stats, depvar_mean):
    """
    Parameters:
    -----------
    regression_results : DataFrame
        Output of run_pixel_regressions(): pixel_id, error_var, coef, pvalue
    pixel_error_stats : DataFrame
        Per-pixel std of each error variable (from ERA5 error NetCDFs)
    depvar_mean : float
        Mean of the dependent variable (for scaling)

    Returns:
    --------
    DataFrame with columns:
        pixel_id, lat, lon, value_temp_1h, value_wspd_1h,
        value_temp_0h, value_wspd_0h, total_value
    """
```

### Interpretation guide
- **High-value pixels**: Locations where forecast accuracy improvements would most reduce congestion costs. These are pixels with *both* large regression coefficients *and* large error variance.
- A pixel with β=2.0 and σ(error)=0.5 has value 1.0 — a 1-σ improvement in forecast accuracy at that pixel would reduce system LMP dispersion by 1.0 $/MWh.
- Policy implication: NWS/NOAA should prioritize observational infrastructure (weather stations, radar, soundings) at high-value pixels.

### Output
- `figures/forecast_value/forecast_value_map.png` — single map showing total forecast value per pixel
- `figures/forecast_value/forecast_value_by_error.png` — 2×2 map (HRRR/GFS × temp/wind)
- `figures/forecast_value/forecast_value_extreme_cold.png` — value map during extreme cold regime
- `figures/forecast_value/forecast_value_extreme_heat.png` — value map during extreme heat regime
- `tables/forecast_value_summary.csv` — per-pixel values, sortable by total_value

### Additional calculation: aggregate dollar value

```python
def compute_aggregate_value(pixel_values, congestion_cost_total):
    """
    Scale pixel-level values to aggregate dollar terms.

    If ERCOT's 2025 congestion cost was ~$1.9B (per Grid Strategies),
    and a 10% improvement in forecast accuracy across all pixels
    reduces system_lmp_std by X%, then:

        dollar_savings = congestion_cost_total × X%

    This gives a back-of-envelope number comparable to the NREL finding
    of "$100M/year from 10% wind forecast improvement."
    """
```

---

## Phase G: Asymmetry Analysis (incorporated in Phase E.4)

See Phase E.4 above. The key deliverable is a figure showing coefficient magnitudes for positive vs negative errors, split by regime.

---

## Phase H: Updated Report and Figures

### Modify `analysis/create_analysis_report.py`

Add new sections to the Typst report:

```
## Updated Report Outline

1. Introduction (updated framing)
2. Data & Methods
   2.1 Weather forecast data (HRRR + GFS) — existing
   2.2 Congestion data (shadow prices, curtailment) — NEW
   2.3 Extreme weather regime classification — NEW
3. Descriptive Results
   3.1 Raw correlation heatmaps — existing (Stage A2)
   3.2 Congestion patterns by season and regime — NEW
4. Pixel-Level Regression Maps
   4.1 Full-year, system_lmp_std — existing (Stage A3)
   4.2 Full-year, direct congestion measures — NEW
   4.3 Regime-conditional maps (extreme cold, extreme heat) — NEW
5. Infrastructure-Level Results
   5.1 Capacity-weighted regressions — existing (Stage A4)
   5.2 Regime interactions — NEW
6. Forecast Value Maps — NEW
   6.1 Where forecast improvements matter most
   6.2 Dollar value of targeted improvements
   6.3 Policy implications for observational infrastructure
7. Asymmetry Analysis — NEW
   7.1 Over- vs under-forecasting effects
   7.2 Asymmetry during extreme events
8. Cluster Heterogeneity — existing (Stage A1)
9. Appendix
   9.1 Regression tables
   9.2 Robustness checks
```

### New figures

| Figure | Script | Description |
|--------|--------|-------------|
| `congestion_descriptive/shadow_cost_timeseries.png` | `analysis/congestion_descriptives.py` | Time series of total shadow cost, colored by regime |
| `congestion_descriptive/curtailment_vs_wind_error.png` | same | Scatter of curtailment vs wind forecast error |
| `congestion_descriptive/binding_constraints_map.png` | same | Map of most frequently binding constraints |
| `pixel_regressions/pixel_regression_2x2_total_shadow_cost.png` | `pixel_regression_maps.py` | 2×2 map with direct congestion DV |
| `pixel_regressions/pixel_regression_2x2_wind_curtailment_mw.png` | same | 2×2 map with curtailment DV |
| `extreme_weather/extreme_cold_total_shadow_cost_2x2.png` | `extreme_weather_regressions.py` | Regression maps during extreme cold |
| `extreme_weather/extreme_heat_total_shadow_cost_2x2.png` | same | Regression maps during extreme heat |
| `forecast_value/forecast_value_map.png` | `forecast_value_map.py` | Dollar value of forecast improvement per pixel |
| `forecast_value/forecast_value_extreme.png` | same | Value maps under extreme regimes |
| `asymmetry/asymmetry_coef_plot.png` | `asymmetry_tests.py` | Over- vs under-forecast coefficient comparison |

---

## Execution Plan

### Step-by-step commands

```bash
# ── Phase A: Download shadow prices ───────────────────────────────────────────
# First, check if gridstatus works:
uv add gridstatus
uv run python -c "
import gridstatus
ercot = gridstatus.Ercot()
test = ercot.get_sced_shadow_prices(date='2025-07-15')
print(test.shape, test.columns.tolist())
"

# If gridstatus doesn't work, use direct ERCOT API:
uv run python -m download_data.pull_sced_shadow --year 2025

# ── Phase B: Download 60-day SCED disclosure ──────────────────────────────────
# Manually downloaded sced disclosure data for 2025
# data is stored in monthly files of this format: /Users/ohouck/Library/CloudStorage/OneDrive-TheUniversityofChicago/ercot_sim_weather_forecasts/raw_data/ercot/sced/may2025 
# Then process:
uv run python -m download_data.pull_sced_disclosure --year 2025

# ── Phase C: Process congestion variables ─────────────────────────────────────
uv run python -c "
from process_data.process_congestion import compute_hourly_congestion_metrics, compute_hourly_curtailment
for month in range(1, 13):
    compute_hourly_congestion_metrics(2025, month)
    compute_hourly_curtailment(2025, month)
"

# Rebuild pixel × hour datasets with congestion variables
uv run python -c "
from process_data.combine_forecast_generation_node import build_pixel_hourly_dataset
for month in range(1, 13):
    build_pixel_hourly_dataset(2025, month, force_rebuild=True)
"

# ── Phase D: Classify weather regimes ─────────────────────────────────────────
# (Done inline during regression loading — no separate build step needed)

# ── Phase E: Run new regressions ──────────────────────────────────────────────

# E.1: Pixel regressions with direct congestion DVs
uv run python -c "
from analysis.pixel_regression_maps import run_pixel_regression_maps
for depvar in ['total_shadow_cost', 'n_binding_constraints', 'wind_curtailment_mw']:
    run_pixel_regression_maps(depvar=depvar, tag=depvar)
"

# E.2: Regime-conditional regressions
uv run python -m analysis.extreme_weather_regressions

# E.3: Infrastructure-level with new DVs
uv run python -m analysis.gridded_infrastructure_lr  # after adding congestion DVs

# E.4: Asymmetry tests
uv run python -m analysis.asymmetry_tests

# ── Phase F: Forecast value maps ──────────────────────────────────────────────
uv run python -m analysis.forecast_value_map

# ── Phase H: Updated report ───────────────────────────────────────────────────
uv run python -m analysis.create_analysis_report
```

---

## File Manifest (New Files)

| File | Type | Purpose |
|------|------|---------|
| `download_data/pull_sced_shadow.py` | Download | SCED shadow prices from ERCOT API |
| `download_data/pull_sced_disclosure.py` | Download | 60-day SCED disclosure (curtailment) |
| `process_data/process_congestion.py` | Processing | Shadow prices → congestion metrics; SCED → curtailment |
| `process_data/classify_weather_regimes.py` | Processing | Extreme weather regime classification |
| `analysis/extreme_weather_regressions.py` | Analysis | Regime-conditional pixel regressions |
| `analysis/asymmetry_tests.py` | Analysis | Over- vs under-forecast asymmetry |
| `analysis/forecast_value_map.py` | Analysis | Dollar value of forecast improvement per pixel |
| `analysis/congestion_descriptives.py` | Analysis | Descriptive figures for congestion data |

### Modified files

| File | Change |
|------|--------|
| `process_data/combine_forecast_generation_node.py` | Add congestion/curtailment merge in `build_pixel_hourly_dataset()` |
| `analysis/pixel_regression_maps.py` | Support new `depvar` options; add regime filtering |
| `analysis/gridded_infrastructure_lr.py` | Support new DVs; add regime interaction terms |
| `analysis/create_analysis_report.py` | Add sections 2.2, 2.3, 3.2, 4.2, 4.3, 6, 7 |
| `helper_funcs.py` | Add `sced_shadow` and `sced_disclosure` paths to `setup_directories()` |
| `CLAUDE.md` | Document new Steps 7a/7b and analysis phases |
| `main.py` | Add new download/process/analysis steps |

---

## Key Risks and Mitigations

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| SCED shadow price API requires different auth | Low | Same OAuth2 + subscription key; test first |
| 60-day SCED disclosure files are too large | Medium | Filter to wind/solar units during download; use chunked reading |
| Shadow price station names don't match Bus_Output.shp | Medium | Fuzzy matching; fallback to constraint-level (non-spatial) analysis |
| Too few extreme-regime hours for per-pixel regression power | Medium | Lower min_obs threshold to 50; aggregate to coarser grid (0.5° instead of 0.1°) |
| Regime classification is sensitive to threshold choice | Low | Report results for 5th, 10th percentile cutoffs as robustness check |
| CRR data would add expected-vs-realized congestion dimension | Low priority | Defer to future work; shadow prices are sufficient for this paper |

---

## Relation to Literature

This plan positions the paper to make three novel contributions:

1. **First paper to use observed multi-model forecast errors matched to direct congestion measures** (shadow prices, curtailment), rather than simulating forecast improvements or using price proxies. The closest existing work (NREL's Value of Improved Wind Forecasting, 2016) uses production cost simulations rather than realized market data.

2. **Spatial heterogeneity in forecast value**: No existing paper maps where forecast improvements matter most. The "forecast value map" is a novel deliverable with direct policy implications for NOAA observational network planning.

3. **Regime-dependent effects**: The finding that winter amplifies forecast error → congestion effects by 3–5× is new. The asymmetry analysis (over- vs under-forecasting) adds practical value for grid operators managing forecast uncertainty.
