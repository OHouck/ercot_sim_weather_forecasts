# Analysis Pipeline Refactoring Plan

## Goal

Restructure the analysis code into a clean pipeline where:
1. Each analysis `.py` file runs independently, producing **figures and tables** (PNG/CSV) saved to disk
2. A single `analysis/create_analysis_report.py` script assembles all outputs into a unified Typst PDF report
3. `main.py` orchestrates the full pipeline end-to-end (data build + analysis + report)

---

## Architecture Overview

```
main.py
 ├── [Data build steps — already done, mostly commented out]
 ├── Analysis Step 1: analysis/cluster_heterogeneity_lr.py    → figures/
 ├── Analysis Step 2: analysis/forecast_error_lmp_corr_heatmap.py → figures/
 ├── Analysis Step 3: analysis/pixel_regression_maps.py [NEW]  → figures/
 ├── Analysis Step 4: analysis/gridded_infrastructure_lr.py [NEW] → figures/ + tables/
 ├── Analysis Step 5: analysis/node_gnn.py                     → figures/ (future)
 └── Report:          analysis/create_analysis_report.py [NEW] → output/report.pdf
```

**Output directories** (all under project root):
- `figures/cluster_heterogeneity/` — cluster map, coefficient plots, histogram grids
- `figures/correlation_heatmaps/` — 4-panel raw correlation heatmap
- `figures/pixel_regressions/` — 4-panel pixel-level regression coefficient maps
- `figures/infrastructure_regressions/` — coefficient table(s) as PNG or CSV
- `tables/` — regression result CSVs for inclusion in the Typst report
- `output/` — final compiled `analysis_report.pdf`

---

## Stage 1: Refactor `cluster_heterogeneity_lr.py` ✅ DONE

### Current state
- Runs per-cluster regressions (joint HRRR 1h + GFS day-ahead)
- Generates 4 PNGs (cluster map, coefficient plot, 2 histogram grids)
- Builds a Typst document inline and compiles to PDF — **all in one script**

### Changes required

**1a. Strip Typst generation out of `cluster_heterogeneity_lr.py`**
- Remove the `compile_typst_pdf()` function entirely
- Remove the Typst compilation call from `main()`
- The script's `main()` should only:
  1. Load cluster data via `build_cluster_hourly_data()`
  2. Run `run_cluster_regressions()` → dict of per-cluster results
  3. Save figures to `figures/cluster_heterogeneity/`:
     - `cluster_map.png`
     - `coef_plot_combined.png`
     - `hist_grid_1h.png`
     - `hist_grid_dah.png`
  4. Save regression summary table to `tables/cluster_regression_results.csv`
     - Columns: `cluster, variable, coefficient, std_error, t_stat, p_value, ci_lower, ci_upper, n_obs`
     - One row per (cluster, variable) — all clusters and both HRRR/GFS variables
  5. Return or print paths to all generated outputs

**1b. Expose a callable entry point**
- Rename `main()` → `run_cluster_analysis(months, n_clusters, geo_weight, n_neighbors, force_rebuild)` returning a dict of output paths
- Keep `if __name__ == "__main__"` calling `run_cluster_analysis()` with defaults

**Estimated effort**: Small. Mostly deletion of `compile_typst_pdf()` and adding CSV export of the tidy regression frames.

---

## Stage 2: Refactor `forecast_error_lmp_corr_heatmap.py` ✅ DONE

### Current state
- `plot_forecast_error_lmp_correlation()` generates a single heatmap (one error variable at a time)
- Called from `__main__` with a single `error_col='temp_error_1h'`
- Each call produces one PNG

### Changes required

**2a. Add a `run_correlation_heatmaps()` function** that produces a **2×2 figure** (4 subplots):

| | Temperature | Wind Speed |
|---|---|---|
| **HRRR 1h** | `temp_error_1h` | `wspd_error_1h` |
| **GFS Day-Ahead** | `temp_error_0h` | `wspd_error_0h` |

- LMP variable: `system_lmp_std` (LMP spread) for all 4 panels
- Each subplot uses the existing `_compute_pixel_correlations()` + `_build_texas_mask()` logic
- Shared colorbar across all 4 panels (symmetric, `RdBu_r`)
- Infrastructure overlay on all panels (wind, solar, gas markers)
- Save as `figures/correlation_heatmaps/corr_heatmap_2x2.png`

**2b. Implementation approach**:
- Factor the existing single-panel plotting code into `_plot_single_correlation_panel(ax, error_col, lmp_var, months, ...)`
- New `run_correlation_heatmaps(months, lmp_var, overlay, save_dir)`:
  1. Create `fig, axes = plt.subplots(2, 2, subplot_kw={'projection': ccrs.PlateCarree()}, figsize=(16, 14))`
  2. Loop over `[('temp_error_1h', 'HRRR 1h Temp'), ('wspd_error_1h', 'HRRR 1h Wind'), ('temp_error_0h', 'GFS DA Temp'), ('wspd_error_0h', 'GFS DA Wind')]`
  3. Call `_plot_single_correlation_panel()` for each
  4. Add shared colorbar, panel titles, figure title
  5. Save PNG
- Keep the old `plot_forecast_error_lmp_correlation()` for standalone use

**2c. Update `__main__`** to call `run_correlation_heatmaps()` by default

**Estimated effort**: Medium. Main work is refactoring single-panel plotting into a reusable function and building the 2×2 layout.

---

## Stage 3: Create `analysis/pixel_regression_maps.py` ✅ DONE

### Purpose
Run a regression **at each pixel independently** and plot a map of significant coefficients. This is distinct from Stage 2 (which shows raw correlations) — here we show **controlled regression coefficients**.

### Specification

**Data source**: `pixel_hourly_gfs+hrrr_{year}_{mm}.parquet` (Step 5d)

**Regression (per pixel)**:
```
system_lmp_std ~ temp_error_1h + wspd_error_1h + temp_error_0h + wspd_error_0h
                 + C(month) + C(hour_of_day) + C(weekday >= 5)
                 + era5_temp + era5_wspd
                 + forecast_load_1h + forecat_load_dah + actual_load
```


**Implementation plan**:

1. **`load_pixel_data(months)`**: Load and concatenate all monthly parquets. Add `is_weekend = weekday >= 5` column.

2. **`run_pixel_regressions(df, error_col, controls, fe_cols)`**:
   - Group by `pixel_id`
   - For each pixel (~5,000 infrastructure pixels), run OLS using pyfixest:
     - Dependent variable: `system_lmp_std`
     - Main variables of interest: `temp_error_1h + temp_error_0h + wspd_error_1h + wspd_error_0h`
     - Controls: `era5_temp`, `era5_wspd`, observed load variables, weekend dummy
     - Fixed effects: absorb fixed effects for hour `hour_of_day + month`
   - Extract coefficient and p-value for `error_col`
   - Return DataFrame: `pixel_id, lat, lon, coef, pvalue`

3. **`plot_pixel_coefficient_map(results_df, title, ax, vmin, vmax, sig_level=0.05)`**:
   - Filter to `pvalue < sig_level`
   - Plot on Cartopy Texas map using `scatter` (colored by coefficient, `RdBu_r`)
   - Insignificant pixels left blank (no fill)
   - Infrastructure overlay optional

4. **`run_pixel_regression_maps(months, save_dir)`** — main entry point:
   - Load data once
   - Run 4 regressions: `temp_error_1h`, `wspd_error_1h`, `temp_error_0h`, `wspd_error_0h`
   - Build 2×2 figure (same layout as Stage 2):

   | | Temperature | Wind Speed |
   |---|---|---|
   | **HRRR 1h** | `temp_error_1h` coef | `wspd_error_1h` coef |
   | **GFS Day-Ahead** | `temp_error_0h` coef | `wspd_error_0h` coef |

   - Save as `figures/pixel_regressions/pixel_regression_2x2.png`
   - Also save regression summary CSV: `tables/pixel_regression_summary.csv`
     - Columns: `pixel_id, lat, lon, error_var, coef, std_err, pvalue, n_obs`

**Performance considerations**:
- ~5,000 pixels × ~8,760 hours/pixel (12 months) = manageable
- Use `pyfixest.feols` with absorbed FEs to keep regressions fast
- Parallelize across pixels with `joblib` or vectorize via `pyfixest` panel regression with `pixel_id` interaction if needed
- Alternative fast approach: use `pyfixest` with `i(pixel_id, error_col)` interaction to run all pixels in one regression — extract pixel-specific coefficients from interaction terms

**Estimated effort**: Medium-Large. New file from scratch, but regression logic can borrow from `cluster_heterogeneity_lr.py` and plotting from `forecast_error_lmp_corr_heatmap.py`.

---

## Stage 4: Convert `gridded_lr.qmd` → `analysis/gridded_infrastructure_lr.py` ✅ DONE

### Current state
`gridded_lr.qmd` is a Quarto notebook that:
1. Loads pixel × hour parquets
2. Aggregates forecast errors by infrastructure category (wind, solar, gas, battery, transmission, load center) using capacity-weighted means
3. Runs regressions of `system_lmp_std` on category-specific errors with interactions
4. Produces coefficient plots and seasonal subsamples
5. Includes a spatial correlation map

### Changes required

**4a. Create `analysis/gridded_infrastructure_lr.py`**

Port the following from `gridded_lr.qmd`:

1. **`aggregate_errors_by_infrastructure(df)`**:
   - Classify pixels by infrastructure type
   - Compute capacity-weighted hourly mean errors per category
   - Return one-row-per-hour DataFrame with columns like `temp_error_1h_wind`, `wspd_error_0h_gas`, etc.

2. **`run_infrastructure_regression(agg_df, depvar, treatments, controls, fe, interactions)`**:
   - Uses `pyfixest.feols()` with cluster-robust SEs by date
   - Returns fitted model + tidy coefficient DataFrame

3. **`run_infrastructure_analysis(months, save_dir)`** — main entry point:
   - Load & aggregate data
   - Run main regression (all months pooled)
   - Run seasonal subsamples (summer/winter/shoulder)
   - Save outputs:
     - `tables/infrastructure_regression_main.csv` — full coefficient table
     - `tables/infrastructure_regression_seasonal.csv` — seasonal coefficients
     - `figures/infrastructure_regressions/coef_plot_main.png` — coefficient plot
     - `figures/infrastructure_regressions/coef_plot_seasonal.png` — seasonal comparison

**4b. Key regression specification** (from `gridded_lr.qmd`):
```
system_lmp_std ~ temp_error_1h_wind + wspd_error_1h_wind + temp_error_1h_solar + ...
                 + temp_error_0h_wind + wspd_error_0h_wind + temp_error_0h_solar + ...
                 + era5_temp_load_center + era5_wspd_wind + weekday
                 + temp_error_1h_load_center:wspd_error_1h_wind
                 + wspd_error_1h_wind:era5_wspd_wind
                 + temp_error_1h_gas:era5_temp_load_center
                 | hour_of_day + month
```
Cluster SEs by date.

**4c. Table output format** for Typst:
- CSV with columns: `variable, coef, std_error, t_stat, p_value, stars, ci_lower, ci_upper`
- Rows grouped by: HRRR 1h errors, GFS day-ahead errors, controls, interactions
- The Typst report will read this CSV and render it as a formatted table

**Estimated effort**: Medium. The regression logic already exists in the `.qmd` — main work is extracting it to pure Python and ensuring the aggregation is correct.

---

## Stage 5: Create `analysis/create_analysis_report.py` ✅ DONE

### Purpose
Single script that assembles all analysis outputs into a unified Typst document and compiles to PDF.

### Typst document structure

```typst
#set page(paper: "us-letter", margin: 0.75in)
#set text(font: "New Computer Modern", size: 10pt)

= Forecast Error and LMP Spread in ERCOT

== 1. Introduction
[Brief description of research question and methodology — hardcoded text]

== 2. Raw Correlation: Forecast Error vs. LMP Spread
[2×2 correlation heatmap figure from Stage 2]
[Caption explaining each panel]

#pagebreak()

== 3. Pixel-Level Regression: Controlled Forecast Error Effects
[2×2 regression coefficient map from Stage 3]
[Caption explaining significance filtering and controls]

#pagebreak()

== 4. Infrastructure-Level Results
[Coefficient table from Stage 4 — main regression]
[Seasonal comparison plot from Stage 4]
[Discussion of how generation mix mediates forecast error → LMP transmission]

#pagebreak()

== 5. Cluster Heterogeneity
=== 5.1 Cluster Map
[cluster_map.png from Stage 1]

=== 5.2 Coefficient Estimates by Cluster
[coef_plot_combined.png from Stage 1]

=== 5.3 Distribution of Marginal Effects — HRRR 1h
[hist_grid_1h.png from Stage 1]

=== 5.4 Distribution of Marginal Effects — GFS Day-Ahead
[hist_grid_dah.png from Stage 1]

#pagebreak()

== 6. Graph Neural Network Results
[Placeholder — to be filled when node_gnn.py results are ready]

== Appendix
[Additional tables, robustness checks]
```

### Implementation

1. **`build_typst_source(figure_dirs, table_dirs)`**:
   - Scans expected output paths (hardcoded list of expected files)
   - Validates all required figures/tables exist
   - Returns Typst source string with `#image()` and `#table()` calls
   - For tables: read CSV → generate Typst `#table()` markup with formatted numbers

2. **`compile_report(typst_source, output_path)`**:
   - Write `.typ` file to temp location
   - Run `typst compile {src} {output_path}`
   - Clean up `.typ` source

3. **`create_analysis_report(output_dir='output')`** — main entry point:
   - Define expected file paths:
     ```python
     EXPECTED_FILES = {
         'cluster_map': 'figures/cluster_heterogeneity/cluster_map.png',
         'coef_plot': 'figures/cluster_heterogeneity/coef_plot_combined.png',
         'hist_1h': 'figures/cluster_heterogeneity/hist_grid_1h.png',
         'hist_dah': 'figures/cluster_heterogeneity/hist_grid_dah.png',
         'corr_heatmap': 'figures/correlation_heatmaps/corr_heatmap_2x2.png',
         'pixel_reg_map': 'figures/pixel_regressions/pixel_regression_2x2.png',
         'infra_coef': 'figures/infrastructure_regressions/coef_plot_main.png',
         'infra_seasonal': 'figures/infrastructure_regressions/coef_plot_seasonal.png',
         'infra_table': 'tables/infrastructure_regression_main.csv',
         'cluster_table': 'tables/cluster_regression_results.csv',
         'pixel_table': 'tables/pixel_regression_summary.csv',
     }
     ```
   - Check which files exist, warn about missing ones
   - Build Typst source (skip sections for missing files)
   - Compile to `output/analysis_report.pdf`

4. **CSV → Typst table helper**: `csv_to_typst_table(csv_path, caption, sig_bold=True)`
   - Reads CSV, formats numbers (3 decimal places, bold if significant)
   - Returns Typst `#figure(table(...), caption: [...])` block

**Estimated effort**: Medium. Mostly string templating. The Typst compilation pattern already exists in `cluster_heterogeneity_lr.py`.

---

## Stage 6: Update `main.py` ✅ DONE

### Changes

Add analysis steps after the existing data build:

```python
# =============================================================================
# ANALYSIS PIPELINE
# =============================================================================

# ── Analysis configuration ───────────────────────────────────────────────────
ANALYSIS_MONTHS = [(2025, m) for m in range(1, 13)]
N_CLUSTERS = 7
GEO_WEIGHT = 2.0
N_NEIGHBORS = 8

# ── Step A1: Cluster heterogeneity regressions ──────────────────────────────
from analysis.cluster_heterogeneity_lr import run_cluster_analysis
cluster_outputs = run_cluster_analysis(
    months=ANALYSIS_MONTHS,
    n_clusters=N_CLUSTERS,
    geo_weight=GEO_WEIGHT,
    n_neighbors=N_NEIGHBORS,
)

# ── Step A2: Raw correlation heatmaps (2×2) ─────────────────────────────────
from analysis.forecast_error_lmp_corr_heatmap import run_correlation_heatmaps
corr_outputs = run_correlation_heatmaps(
    months=ANALYSIS_MONTHS,
    lmp_var='system_lmp_std',
)

# ── Step A3: Pixel-level regression coefficient maps (2×2) ──────────────────
from analysis.pixel_regression_maps import run_pixel_regression_maps
pixel_outputs = run_pixel_regression_maps(months=ANALYSIS_MONTHS)

# ── Step A4: Infrastructure-level regressions ────────────────────────────────
from analysis.gridded_infrastructure_lr import run_infrastructure_analysis
infra_outputs = run_infrastructure_analysis(months=ANALYSIS_MONTHS)

# ── Step A5: Graph neural network (placeholder) ─────────────────────────────
# from analysis.node_gnn import run_gnn_analysis
# gnn_outputs = run_gnn_analysis(months=ANALYSIS_MONTHS)

# ── Compile Typst report ────────────────────────────────────────────────────
from analysis.create_analysis_report import create_analysis_report
create_analysis_report(output_dir='output')
```

---

## Stage 7: Update `CLAUDE.md` ✅ DONE

Add a new section documenting the analysis pipeline:

### New section: "## Analysis Pipeline"

Document:
- Each analysis script, its inputs, outputs, and callable entry point
- The report assembly script and its expected file manifest
- The Typst document structure
- How to run the full pipeline vs individual analyses

### Update "## Execution Order"

Add analysis steps after Step 6:
```bash
# Analysis
uv run python -m analysis.cluster_heterogeneity_lr
uv run python -m analysis.forecast_error_lmp_corr_heatmap
uv run python -m analysis.pixel_regression_maps
uv run python -m analysis.gridded_infrastructure_lr
uv run python -m analysis.create_analysis_report

# Or run everything:
uv run python main.py
```

---

## Implementation Order & Dependencies

```
Stage 1 (refactor cluster_heterogeneity_lr.py)  ─┐
Stage 2 (refactor forecast_error_lmp_corr_heatmap.py) ─┤
Stage 3 (NEW: pixel_regression_maps.py)          ─┤── all independent
Stage 4 (convert gridded_lr.qmd → .py)           ─┘
                                                   │
                                                   ▼
Stage 5 (NEW: create_analysis_report.py)  ← depends on Stages 1-4 outputs
                                                   │
                                                   ▼
Stage 6 (update main.py)                 ← depends on all entry points existing
Stage 7 (update CLAUDE.md)               ← depends on final structure
```

**Stages 1–4 can be done in parallel** since they are independent refactors with no cross-dependencies. Stage 5 must come after since it reads their outputs. Stages 6–7 are final integration.

---

## Key Design Decisions

1. **Typst over LaTeX**: The project already uses Typst (in `cluster_heterogeneity_lr.py`). Stick with it for consistency and simpler syntax.

2. **CSV as the interchange format for tables**: Each analysis script writes regression results to CSV. The report script reads CSVs and converts to Typst table markup. This decouples analysis from presentation.

3. **PNG for all figures**: Already the standard in this project. Typst reads PNGs natively via `#image()`.

4. **`pyfixest` for regressions, `statsmodels` as fallback**: `pyfixest` handles absorbed FEs efficiently. For the per-pixel regressions (Stage 3), if `pyfixest` is too slow for ~5k separate regressions, fall back to vectorized OLS or `statsmodels`.

5. **No Quarto dependency for the analysis pipeline**: Convert `gridded_lr.qmd` to pure Python. The Quarto notebooks can remain for interactive exploration but are not part of the automated pipeline.

6. **GNN results as placeholder**: Section 6 of the report will have placeholder text until `node_gnn.py` produces standardized outputs. The report script should handle missing GNN outputs gracefully.

---

## File Manifest (after all stages complete)

### New files
- `analysis/pixel_regression_maps.py` — Stage 3
- `analysis/gridded_infrastructure_lr.py` — Stage 4
- `analysis/create_analysis_report.py` — Stage 5

### Modified files
- `analysis/cluster_heterogeneity_lr.py` — Stage 1 (remove Typst generation)
- `analysis/forecast_error_lmp_corr_heatmap.py` — Stage 2 (add 2×2 entry point)
- `main.py` — Stage 6 (add analysis pipeline)
- `CLAUDE.md` — Stage 7 (document analysis pipeline)

### Unchanged files
- `analysis/node_gnn.py` — future integration
- `analysis/local_node_lr.qmd` — kept for interactive use, not in pipeline
- `analysis/analysis_forecast_error_eda.py` — kept for EDA, not in main pipeline
- `analysis/create_plots.py` — utility functions, no changes needed
- All `process_data/` and `download_data/` scripts — unchanged
