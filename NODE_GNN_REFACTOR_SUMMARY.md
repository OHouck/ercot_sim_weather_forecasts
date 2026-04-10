# Node GNN Refactor Summary (Scalar Congestion Target)

Date: 2026-04-09
File updated: analysis/node_gnn.py

## Goal
Refactor the node-level LMP GNN into a graph-level scalar model for hourly system congestion outcome prediction, with Double-ML style temporal residualization and Integrated Gradients attribution focused on weather-error features.

## What Changed
- Switched target from per-node `lmp` to hourly scalar `y_scalar` (residualized `total_shadow_cost`).
- Added temporal FE residualization helper:
  - `residualize_on_time_fe(series, hour_of_day, weekday, month)`
  - Uses cyclical sine/cosine terms and OLS residuals.
- Replaced data source path:
  - Old: `prepare_node_level_data(...)` node-hour cache.
  - New: monthly `pixel_hourly_gfs+hrrr_YYYY_MM.parquet` ingestion.
  - Maps each node to nearest pixel via KDTree.
- Added robust coordinate fallback:
  - If `latitude`/`longitude` are missing, parse from `pixel_id` (`lat_lon`).
- Refactored node features to residualized weather features and coordinates:
  - `temp_error_1h_resid`, `wspd_error_1h_resid`, `temp_error_0h_resid`, `wspd_error_0h_resid`, `era5_temp_resid`, `era5_wspd_resid`, `lat`, `lon`.
- Refactored model head to graph-level output:
  - Added `GlobalAttention` pooling over node embeddings.
  - Output now one scalar per graph/hour.
- Refactored training/evaluation:
  - Loss on scalar predictions only.
  - Train-set normalization of scalar target (`y_mean`, `y_std`).
  - Evaluation de-normalizes predictions for MAE/RMSE/R2 in original units.
- Added Integrated Gradients attribution:
  - `compute_integrated_gradients(...)`
  - Saves mean absolute attribution for error features.
- Updated main pipeline outputs:
  - `hourly_test_predictions.csv`
  - `integrated_gradients_summary.csv`
  - checkpoint now stores `y_mean`, `y_std`.

## Validation Performed
1. Syntax check:
   - `python -m py_compile analysis/node_gnn.py` passed.
2. Import smoke test:
   - module import succeeded.
3. Data-path smoke test:
   - `load_data()` succeeded with output shape `(5213212, 25)` and no missing `y_scalar`.
4. End-to-end component smoke test:
   - graph construction, feature prep, dataset build, and model forward all succeeded.
   - split sizes observed: train=4319, val=2880, test=1548 hours.

## Notes
- `GlobalAttention` currently emits a deprecation warning in torch-geometric; behavior is still correct. Can be migrated later to `AttentionalAggregation` if desired.
- Existing visualization/helper functions for node-level outputs remain in file, but the main workflow now uses scalar outcome + IG outputs.
