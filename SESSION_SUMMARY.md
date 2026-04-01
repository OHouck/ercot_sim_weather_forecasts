# Session Summary — Congestion Analysis Implementation

**Last Updated**: March 27, 2026
**Scope**: Implementing Phases A–H of `PLAN_congestion_analysis.md`

---

## What Was Accomplished

### Phase A: SCED Shadow Price Downloads ✅

**Script**: `download_data/pull_sced_shadow.py` (created in prior session)

Downloaded SCED shadow prices (NP6-86-CD) from the ERCOT API for all 12 months of 2025. Shadow prices are the most direct measure of transmission congestion — when a constraint binds, the shadow price ($/MW) represents the marginal cost of relieving congestion.

| Month | Files | Days | Status |
|-------|-------|------|--------|
| Jan | 31/31 | 31 | ✅ Complete |
| Feb | 28/28 | 28 | ✅ Complete |
| Mar | 31/31 | 31 | ✅ Complete |
| Apr | 30/30 | 30 | ✅ Complete |
| May | 31/31 | 31 | ✅ Complete |
| Jun | 30/30 | 30 | ✅ Complete |
| Jul | 31/31 | 31 | ✅ Complete |
| Aug | 26/31 | 26 | ⚠️ 5 days missing (22–25, 30) |
| Sep | 24/30 | 24 | ⚠️ 6 days missing (12, 15, 21, 24–25, 29) |
| Oct | 29/31 | 29 | ⚠️ 2 days missing (12, 18) |
| Nov | 23/30 | 23 | ⚠️ 7 days missing (8–9, 12, 16–17, 24, 26) |
| Dec | 31/31 | 31 | ✅ Complete |

**Missing days**: 20 days total across Aug–Nov. Multiple download retries all returned HTTP 500 from ERCOT's API — these dates may not yet be available in the 60-day SCED disclosure. Eight months are fully complete.

### Phase C: Congestion Processing & Pixel Merge ✅

**Scripts**: `process_data/process_congestion.py`, `process_data/combine_forecast_generation_node.py` (modified)

- Processed shadow price CSVs into hourly system-level congestion metrics for all 12 months
- Merged 7 new congestion columns into the pixel × hour parquets (52 → 59 columns)
- New columns: `n_binding_constraints`, `total_shadow_cost`, `max_shadow_price`, `shadow_cost_weighted`, `n_violations`, `total_violated_mw`, `mean_shadow_cost_per_interval`

| Month | Congestion Hours | Mean Shadow Cost ($/hr) | Mean Binding Constraints |
|-------|-----------------|------------------------|------------------------|
| Jan | 744 | $20,765 | 12.3 |
| Feb | 672 | $16,403 | 12.1 |
| Mar | 743 | $13,637 | 12.0 |
| Apr | 720 | $22,018 | 18.8 |
| May | 736 | $15,211 | 12.0 |
| Jun | 717 | $11,300 | 12.3 |
| Jul | 744 | $15,965 | 12.1 |
| Aug | 621 | $13,878 | 9.4 |
| Sep | 571 | $12,486 | 8.8 |
| Oct | 695 | $10,007 | 11.4 |
| Nov | 537 | $11,388 | 10.4 |
| Dec | 742 | $11,959 | 9.4 |

All 12 pixel parquets rebuilt with 59 columns and `congestion=True`.

### Phase D: Weather Regime Classification ✅

**Script**: `process_data/classify_weather_regimes.py` (created in prior session)

Classifies each hour into weather/grid regimes using system-wide percentile thresholds:
- **Extreme cold**: Bottom 5% of ERA5 temperature
- **Extreme heat**: Top 5% of ERA5 temperature
- **High wind**: Top 10% of ERA5 wind speed
- **Stressed grid**: Top 5% of system LMP max

### Phase E: Regime-Conditional Regressions ✅

**Script**: `analysis/extreme_weather_regressions.py` (created in prior session)

Ran per-pixel regressions of `total_shadow_cost` on forecast errors for each weather regime, across all 12 months (6,502 pixels each).

**Key findings**:

| Regime | Most Important Error | Sig Pixels | Mean β |
|--------|---------------------|-----------|--------|
| **Extreme Cold** | HRRR wind speed (1h) | 30% | +$14,202 |
| **Extreme Cold** | GFS temp (day-ahead) | 18% | −$9,528 |
| **Stressed Grid** | HRRR wind speed (1h) | 37% | +$13,713 |
| **Stressed Grid** | GFS temp (day-ahead) | 31% | −$6,437 |
| **High Wind** | GFS wind speed (day-ahead) | 31% | +$4,040 |
| **Extreme Heat** | HRRR wind speed (1h) | 15% | +$2,053 |

**Interpretation**: Wind speed forecast errors during cold snaps and stressed grid periods have the largest congestion impact — a 1 m/s HRRR wind error increases total hourly shadow cost by $13,000–14,000 at significant pixels. Extreme heat has the weakest effects.

### Phase F: Forecast Value Map ✅

**Script**: `analysis/forecast_value_map.py` (created in prior session)

Computes the dollar value of forecast accuracy at each pixel: `value = |β| × σ(error)`.

**Results** (full year, `depvar=total_shadow_cost`):
- 6,242 / 6,534 pixels have nonzero forecast value
- **Mean total value**: $3,647 per pixel
- **Median**: $3,535 | **P90**: $6,164 | **Max**: $12,631
- Value breakdown by error type:
  - `temp_error_1h` (HRRR): 3,801 pixels, mean $1,470
  - `wspd_error_1h` (HRRR): 4,744 pixels, mean $1,423
  - `temp_error_0h` (GFS): 3,812 pixels, mean $1,681
  - `wspd_error_0h` (GFS): 2,950 pixels, mean $1,362

**Figures**: `figures/forecast_value/forecast_value_by_error_total_shadow_cost.png` (2×2), `forecast_value_total_total_shadow_cost.png` (single map)

### Phase G: Asymmetry Analysis ✅

**Script**: `analysis/extreme_weather_regressions.py` → `run_asymmetry_regressions()`

Splits each forecast error into positive (over-forecast) and negative (under-forecast) components.

**Key findings**:
| Error Variable | Direction | Sig Pixels | Mean β |
|---------------|-----------|-----------|--------|
| **temp_error_1h** | Over-forecast (pos) | 547 | −$1,454 |
| **temp_error_1h** | Under-forecast (neg) | 913 | +$124 |
| **wspd_error_1h** | Over-forecast (pos) | 556 | +$1,217 |
| **wspd_error_1h** | Under-forecast (neg) | 902 | +$1,547 |
| **temp_error_0h** | Over-forecast (pos) | 470 | +$1,844 |
| **temp_error_0h** | Under-forecast (neg) | 162 | −$386 |
| **wspd_error_0h** | Over-forecast (pos) | 564 | +$190 |
| **wspd_error_0h** | Under-forecast (neg) | 171 | +$645 |

**Interpretation**: Under-forecasting wind speed (predicting less wind than realized) has more significant pixels and a larger coefficient than over-forecasting — consistent with wind generation being higher than expected, causing unexpected congestion on export corridors.

### Additional: Updated Pixel Regressions ✅

Also re-ran the full-year pixel regressions (`system_lmp_std` as DV) with all 12 months including load error variables:
- `temp_error_1h`: 54% sig, mean β = −0.43
- `wspd_error_1h`: 49% sig, mean β = +1.20
- `load_error_dam` (day-ahead load forecast error): 69% sig, mean β = +0.03
- Updated figure: `pixel_regression_2x2_system_lmp_std.png`

---

## Output File Inventory

### Figures
| File | Description |
|------|-------------|
| `figures/forecast_value/forecast_value_by_error_total_shadow_cost.png` | 2×2 map: value by error type |
| `figures/forecast_value/forecast_value_total_total_shadow_cost.png` | Single map: total forecast value |
| `figures/pixel_regressions/pixel_regression_2x2_system_lmp_std.png` | Updated full-year pixel regression maps |
| `figures/pixel_regressions/pixel_regression_2x2_total_shadow_cost_july.png` | July-only shadow cost regressions |

### Tables
| File | Description |
|------|-------------|
| `tables/extreme_weather_regression_total_shadow_cost_extreme_cold.csv` | Regime regression: extreme cold |
| `tables/extreme_weather_regression_total_shadow_cost_extreme_heat.csv` | Regime regression: extreme heat |
| `tables/extreme_weather_regression_total_shadow_cost_high_wind.csv` | Regime regression: high wind |
| `tables/extreme_weather_regression_total_shadow_cost_stressed_grid.csv` | Regime regression: stressed grid |
| `tables/extreme_weather_regression_wind_curtailment_mw_extreme_cold.csv` | Curtailment regime regression: extreme cold |
| `tables/extreme_weather_regression_wind_curtailment_mw_extreme_heat.csv` | Curtailment regime regression: extreme heat |
| `tables/extreme_weather_regression_total_curtailment_mw_extreme_cold.csv` | Total curtailment regime regression: extreme cold |
| `tables/extreme_weather_regression_total_curtailment_mw_extreme_heat.csv` | Total curtailment regime regression: extreme heat |
| `tables/asymmetry_total_shadow_cost.csv` | Asymmetry analysis: full sample |
| `tables/forecast_value_total_shadow_cost.csv` | Forecast value per pixel |
| `tables/forecast_value_regression_total_shadow_cost.csv` | Regression coefficients for value calc |
| `tables/pixel_regression_summary_system_lmp_std.csv` | Updated full-year pixel regressions |

### Processed Data
| File | Description |
|------|-------------|
| `processed_data/curtailment_metrics/curtailment_hourly_{YYYYMM}.csv` | Hourly curtailment metrics (Jan–Nov 2025; 11 files) |
| `processed_data/combined_hourly_gridded_data/pixel_hourly_gfs+hrrr_2025_{mm}.parquet` | Pixel × hour dataset (65 cols, all 12 months) |

---

## What Is Left To Do

### Missing Data (Low Priority)
- **20 shadow price days** across Aug–Nov (HTTP 500 from ERCOT API — may not yet be in the 60-day disclosure). Analysis already uses available data (80–95% coverage for those months). Can retry later.

### Phase B: 60-Day SCED Disclosure / Curtailment Data ✅

**Scripts**: `process_data/process_curtailment.py` (created), `process_data/combine_forecast_generation_node.py` (modified)

Processed ERCOT's 60-Day SCED Disclosure nested ZIP archives to extract per-unit wind and solar curtailment. Curtailment = `max(0, HSL − Telemetered Net Output)` for WIND and PVGR units. Merged 6 new curtailment columns into the pixel × hour parquets (59 → 65 columns).

**Key implementation details**:
- SCED disclosure is released ~60 days after the operating date; folder naming uses release month (e.g., `sep2025` contains July 2025 operating data)
- Each folder is a nested ZIP (outer → inner → 7 CSVs per operating day)
- `_find_sced_folders()` loads N+2 and N+1 release folders, then filters by operating date to get boundary-day coverage
- Caches to `{processed}/curtailment_metrics/curtailment_hourly_{YYYYMM}.csv`

**Data availability**: Jan–Oct 2025 (full). Nov 2025 has only 24 hours (1 operating day in the available folder). Dec 2025 unavailable (would need `feb2026` folder which doesn't exist yet).

**Curtailment statistics by month**:

| Month | Hours | Mean Wind MW | Mean Total MW | Max Total MW |
|-------|-------|-------------|--------------|-------------|
| Jan | 720 | 720 | 1,152 | 15,256 |
| Feb | 672 | 642 | 1,029 | 12,347 |
| Mar | 743 | 1,618 | 2,815 | 16,554 |
| Apr | 720 | 1,601 | 2,423 | 14,914 |
| May | 744 | 708 | 1,197 | 8,551 |
| Jun | 720 | 620 | 1,171 | 12,253 |
| Jul | 744 | 272 | 564 | 4,872 |
| Aug | 744 | 150 | 286 | 2,086 |
| Sep | 720 | 131 | 460 | 2,868 |
| Oct | 744 | 582 | 1,190 | 16,240 |
| Nov | 24 | 255 | 550 | 1,595 |

Spring months (Mar–Apr) have highest curtailment — consistent with peak wind generation outpacing transmission capacity.

**New pixel parquet columns** (65 total, up from 59):
- `wind_curtailment_mw`, `solar_curtailment_mw`, `total_curtailment_mw`
- `wind_curtailment_pct`, `solar_curtailment_pct`, `n_curtailed_units`

**Curtailment regime regressions** (partially complete):
- Completed: `extreme_cold` and `extreme_heat` for both `wind_curtailment_mw` and `total_curtailment_mw`
- Pending: `high_wind`, `stressed_grid` regimes; asymmetry analysis; forecast value maps for curtailment DV
- These 4 tasks were relaunched as background jobs at the start of this session

### Phase B Curtailment Regressions (In Progress)
- **Regime regressions**: `high_wind` and `stressed_grid` for `wind_curtailment_mw` and `total_curtailment_mw` still pending (only `extreme_cold` and `extreme_heat` completed so far)
- **Asymmetry analysis**: `run_asymmetry_regressions(depvar='wind_curtailment_mw')` not yet run
- **Forecast value maps**: `run_forecast_value_analysis(depvar='wind_curtailment_mw')` not yet run
- All 4 tasks relaunched as background jobs at the start of the most recent session (will need to be rerun on next session if they didn't complete)

### Phase C.2: Constraint Geolocation (Partially Done, Needs Improvement)
- Current approach using `Bus_Output.shp` achieves only ~2% match rate (SCED station names are abbreviated differently than bus names)
- **User note in plan**: "This might not work, instead also compare to merging using identified nodes from the `build_node_coordinates` function in `process_ercot.py`"
- Without geolocation, congestion metrics are **system-level only** (same value for all pixels in each hour)
- Improving this would enable **constraint-level spatial analysis** — mapping which specific transmission corridors are affected by forecast errors at nearby pixels

### Phase H: Updated Report (Not Started)
- Update `reports/analysis_report.typ` with new sections:
  - Regime regression results (tables: extreme_cold, extreme_heat, high_wind, stressed_grid for total_shadow_cost)
  - Forecast value maps (`figures/forecast_value/forecast_value_by_error_total_shadow_cost.png`)
  - Asymmetry analysis (`tables/asymmetry_total_shadow_cost.csv`)
  - Curtailment findings (once remaining regressions complete)
- Compile updated Typst PDF via `uv run python -m analysis.create_analysis_report`

### Potential Extensions
1. **Regime-specific forecast value maps** — `run_regime_value_comparison()` exists but hasn't been run for all regimes
2. **Seasonal regime regressions** — winter vs. summer extreme weather effects
3. **Regime regression maps** — the regime regressions currently save tables only; generating 2×2 spatial maps for each regime would be valuable for the paper
4. **Interaction between weather and load errors** — load forecast errors are significant (69% of pixels); interaction with weather errors could reveal compounding effects
5. **Infrastructure-specific regime analysis** — combine `gridded_infrastructure_lr.py` with regime conditioning
6. **Dec 2025 curtailment** — retry when `feb2026` SCED disclosure folder is available

---

## Code Changes Made (All Sessions Combined)

| Session | File | Change |
|---------|------|--------|
| Session 1 | `download_data/pull_sced_shadow.py` | Created: downloads SCED shadow prices from ERCOT API |
| Session 1 | `process_data/process_congestion.py` | Created: shadow prices → hourly congestion metrics |
| Session 1 | `process_data/classify_weather_regimes.py` | Created: extreme weather regime classification |
| Session 1 | `analysis/extreme_weather_regressions.py` | Created: regime-conditional pixel regressions + asymmetry |
| Session 1 | `analysis/forecast_value_map.py` | Created: dollar value of forecast accuracy per pixel |
| Session 1 | `process_data/combine_forecast_generation_node.py` | Modified: added congestion merge (Step 5), curtailment merge (Step 6) |
| Session 1 | `analysis/pixel_regression_maps.py` | Modified: added congestion/curtailment DVs and system cols |
| Session 1 | `main.py` | Modified: added Steps A5–A10 for congestion + curtailment pipeline |
| Session 2 | `process_data/process_curtailment.py` | Created: SCED disclosure nested ZIPs → hourly curtailment metrics |
| Session 2 | All 12 pixel parquets | Rebuilt: 52 → 59 cols (congestion) → 65 cols (curtailment) |
