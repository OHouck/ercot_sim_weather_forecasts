# NN Improvement Plan — ERCOT Congestion Cost Prediction

## 0. Where we stand (summary of prior work)

Prior report: `{OneDrive}/reports/nn_improvements.typ`.

- **Target**: `economic_congestion_cost` (hourly, heavy-tailed; median \$306k/h, max \$63M/h).
- **Data**: 6 months of 2025 (Jan, Mar, May, Jul, Sep, Nov); T = 3,550 hours; 4 error channels (`wspd_error_{1h,0h}`, `temp_error_{1h,0h}`) on a 43×53 / 0.25° grid; 70/15/15 chunk-split.
- **Best clean test R²**: **0.421** from `MLP(cluster_features_k7 + lag-1h + lag-24h + cyclic time)`.
- **Best without AR lags**: ~0 — weather alone carries almost no signal once `lmp_std` leakage is removed.
- **What failed**: two-stage nodal supervision (compresses signal), GRU (insufficient data), raw 4×43×53 pixel MLP (overfits).
- **Known weaknesses**: tail R² is poor (−0.20 best, −19 for LGB); no uncertainty quantification; cluster features assume the k=7 geographic clustering from Step 6 is the right basis.

The rest of this document proposes a sequence of ablations designed around three convergent ideas from the literature review:

1. **Basis reduction before learning** — 2500 hours is too few to fit 9k-feature models, but plenty for models on ~10–50 coefficients of a principled basis (FPC/EOF, varimax-rotated modes, tensor factors, graph-pooled features).
2. **Decouple the AR baseline from the weather signal** — the current headline 0.421 is mostly serial correlation. The scientific question ("do forecast errors predict congestion?") requires the *marginal* R² of weather features over an honest AR baseline.
3. **Inference under dependence** — train/test R² is not enough; we need confidence intervals that account for spatial+temporal autocorrelation (Conley / Driscoll–Kraay / dependent wild bootstrap).

Each step below is self-contained, specifies a clear *go/no-go* criterion, and has contingencies for both success and failure.

---

## Step 1 — Clean AR-only baseline, then measure marginal value of weather

**Motivation.** The reported 0.421 mixes AR persistence with weather. To isolate the weather contribution we need the stripped-down AR baseline.

**What to do.**
1. Fit four nested models on the chunk-split test set, all with log1p(target) + cyclic time:
   - M0: `cyclic_time` only
   - M1: `cyclic_time + lag-1h + lag-24h`
   - M2: `M1 + cluster_features_k7 (no lmp_std)`
   - M3: `M1 + cluster_features + capacity_weighted_error × cluster` (the "rich" config from Round 2b)
2. Report native R², native RMSE, tail R² (top 10% of y), and the **ΔR² of M2 − M1 and M3 − M1** with dependent-wild-bootstrap 95% CIs over chunk blocks (Shao 2010).
3. Add a permutation test: shuffle the weather features across chunks, re-fit M2, report the null distribution of ΔR².

**Go criterion.** ΔR² of weather features is statistically non-zero under the bootstrap, and ≥ 0.02 on native scale.

**If successful** → proceed to Step 2 (improve the basis in which weather features are represented).
**If not successful (weather features add nothing to AR)** → the project's main claim collapses; pivot to Step 5 (functional/FPC basis) *and* Step 8 (value-of-forecast framing under a distributional model); the answer may be "congestion is primarily persistent; weather helps only in specific regimes" — Step 6 then becomes essential.

---

## Step 2 — Scalar-on-function regression with FPC / EOF basis

**Motivation.** Ramsay & Silverman (2005); Reiss et al. (2017). Instead of ad-hoc geographic clusters, project each channel's 43×53 field onto its leading empirical eigenmodes (EOFs / FPCs). This is the principled basis reduction for spatially autocorrelated covariates and makes no assumption about cluster boundaries.

**What to do.**
1. For each of the 4 error channels separately, compute EOFs on the *training hours only*. Retain `K ∈ {5, 10, 20, 40}` modes per channel (cumulative variance diagnostic).
2. Build feature matrix `X_fpc` = mode scores per hour (4 × K features total). Optionally add capacity-weighted projections: `∫ β(s) wind_capacity(s) X_t(s) ds` implemented as a dot product with the gridded wind-capacity mask.
3. Fit Ridge + MLP + LightGBM on `[X_fpc, AR_lags, cyclic_time]` with the same split. Compare native R² against Step 1's M2.
4. Visualize the top EOFs as Texas maps and check whether they resemble the k=7 cluster polygons (sanity check).

**Go criterion.** FPC-based model matches or beats cluster-based model on native R² (≥ 0.42), AND the top 2–3 EOFs are geographically interpretable.

**If successful** → drop ad-hoc clusters, adopt FPC basis going forward, continue to Step 3 (non-linear FPC regression and interactions).
**If not successful** → the k=7 clusters are capturing something EOFs miss (possibly non-linear aggregation via the cluster-LMP pattern). Keep clusters, try Step 3 with varimax-rotated EOFs (Hannachi et al. 2007) which typically localize better, and revisit in Step 4.

---

## Step 3 — Penalized functional regression and interactions

**Motivation.** A scalar-on-function model `y = α + ∫ β(s) X(s) ds + ε` with a roughness penalty on β(s) gives an interpretable spatial coefficient map *and* natural regularization. The MLP in Step 2 has no smoothness prior on β.

**What to do.**
1. Implement `pyfixest` / `pyGAM` scalar-on-function with thin-plate spline or B-spline basis for β(s), one per channel. 10–20 basis functions on 43×53 grid, roughness penalty chosen by GCV.
2. Fit `y_log = α + Σ_c ∫ β_c(s) X_{t,c}(s) ds + γ·AR_lags + cyclic + ε` on training fold.
3. Plot β_c(s) as Texas heatmaps. Compare to the pixel-regression maps from `analysis/pixel_regression_maps.py`.
4. Add a *capacity-weighted* interaction: `∫ β(s) · wind_capacity(s) · wspd_error_{1h}(s) ds`; this encodes the physical hypothesis that error × installed capacity drives the outcome.

**Go criterion.** Native R² ≥ Step 2 best, AND β(s) maps are non-trivial (not just a constant surface) and concentrate on plausible Texas regions (Panhandle wind corridor, Gulf coast).

**If successful** → this becomes the headline model (interpretable spatial coefficient maps are publication-ready); continue to Step 4 for non-parametric extensions.
**If not successful (β is essentially flat, or R² drops below Step 2)** → spatial detail is not useful; the right representation is probably a small number of capacity-weighted scalars. Drop functional regression, go to Step 4b (generator-capacity-weighted aggregates) directly.

---

## Step 4 — Graph neural network on the 596-node graph

**Motivation.** Khodayar & Wang (2019); Wang et al. (2023). With T small and 596 natural nodes (ERCOT resource nodes with coordinates), a GNN that aggregates forecast errors at each node via its local neighborhood is a learned, data-driven basis reduction — conceptually superior to fixed k=7 clusters.

**What to do.**
1. Build a node graph: 596 nodes with edges to k=8 nearest neighbors by great-circle distance. Node features per hour = interpolated ERA5 forecast errors at the node's lat/lon (already available in `node_hourly_*.csv`).
2. Use a 2–3 layer GCN or GraphSAGE with global mean + max pooling to produce a small hour-level embedding, concatenate with AR lags + cyclic time, predict `log1p(congestion)`.
3. Regularize aggressively: dropout 0.5 on node features, node-feature masking, small hidden dim (32–64).
4. Compare to Step 2/3 on native R² and tail R².

**Go criterion.** Native R² ≥ Step 3 best, AND tail R² improves over Step 3 by > 0.1.

**If successful** → GNN is the next-generation replacement for the fixed cluster basis; plan a follow-up where cluster polygons *emerge* from learned node embeddings (spectral clustering on learned embeddings).
**If not successful** → confirms the Round-2c finding that 2500 sequences is too few for learned spatial aggregation. Fall back to Step 3's fixed functional basis, and document the data-size requirement (probably need full year + 2024 backfill, see Step 9).

---

## Step 5 — Non-parametric estimation with proper spatial-temporal inference

**Motivation.** All experiments so far report a single test R² from one chunk split, without uncertainty. Inference under spatial+temporal dependence requires HAC-type corrections (Conley 1999; Driscoll–Kraay 1998) or block bootstrap (Künsch 1989; Shao 2010).

**What to do.**
1. Implement dependent wild bootstrap (Shao 2010) with block length `⌈T^{1/3}⌉ ≈ 15` hours. Report 95% CIs for the ΔR² of each feature set over the AR-only baseline.
2. For coefficient-level inference in the scalar-on-function model (Step 3), compute Driscoll–Kraay standard errors on β(s) — spatial version uses Conley kernel with cutoff = 100 km.
3. Quantile regression with pinball loss for tails: fit Step 3 model with `q=0.90` and `q=0.95` pinball loss, report empirical coverage.
4. Monte-Carlo over 20 random chunk-split seeds; report R² distribution rather than a point estimate.

**Go criterion.** CIs and seed-variance provide an honest picture. Specific: ΔR²(weather | AR) has a 95% CI bounded away from zero in at least one configuration.

**If successful** → the reported gains are defensible; write up.
**If not successful** → the weather signal is within the noise floor for this sample size. Two branches: (i) expand T (Step 9); (ii) reframe as a value-of-forecast / decision-theoretic analysis (Step 8) where even small R² can have economic value.

---

## Step 6 — Regime-stratified models and extreme-event heads

**Motivation.** Current tail R² is poor because one flat model prices both quiet hours and extreme hours. Regimes are already defined in `analysis/extreme_weather_regressions.py` (extreme_cold, extreme_heat, high_wind, stressed_grid). Lago et al. (2021) and Marcjasz et al. (2023) show regime-specific or distributional heads materially improve tail accuracy.

**What to do.**
1. Fit the Step 3 (or best-so-far) model *separately* for each regime using only regime hours; report regime-conditional native R² and tail R².
2. Alternatively: single shared-backbone model with a gated output head per regime (MoE-lite). Backbone = FPC + AR; gate softmax over 4 regimes fed by cyclic time + temperature level + load level.
3. Distributional regression (Marcjasz et al. 2023): JSU or censored-log-normal output head predicting (μ, σ, skew) rather than a point; evaluate with CRPS, pinball loss at q∈{0.5, 0.9, 0.95, 0.99}.

**Go criterion.** Tail R² on extreme regimes exceeds −0.10 (vs current best −0.20), AND CRPS under the distributional model improves over the point forecast's implied Gaussian by ≥ 10%.

**If successful** → the project produces regime-conditional and probabilistic forecasts, both of which are ERCOT-relevant deliverables. Becomes a second publication thread.
**If not successful (tail R² still poor)** → the tail behaviour is driven by unobserved grid state (specific binding constraint identities), not by weather. Document this and pivot the tail story to: "weather forecast error explains variation in *moderate* congestion; tail events require constraint-state features that are not yet in the pipeline."

---

## Step 7 — Capacity-aware interaction features on the full grid

**Motivation.** The Round-2b "rich" config added `wspd_error × log1p(nameplate_mw_wind)` *at the cluster level* and gained ~0.02 R² in LightGBM. This is a crude aggregation of a physically grounded quantity (wind power shortfall ≈ error × capacity). At the pixel level it becomes `∫ β(s) · capacity_wind(s) · wspd_error_{1h,t}(s) ds` — a natural scalar summary.

**What to do.**
1. Build per-hour scalars:
   - `Σ_s wind_capacity(s) · wspd_error_{1h,t}(s)` (total wind-shortfall proxy)
   - `Σ_s solar_capacity(s) · shortwave_error_t(s)` (needs `wind_error` replaced by solar NWP error; may require a new field)
   - `Σ_s load_center(s) · temp_error_t(s)` (cooling-demand error proxy)
   - Same but split into 4 quadrants (N/S × E/W Texas) for geographic partitioning
2. Feed these 4–12 scalars alongside AR lags and cyclic time; fit Ridge + LightGBM + MLP.
3. Compare to Step 2 FPC.

**Go criterion.** A handful of physics-motivated scalars reach ≥ 80% of Step 2's native R².

**If successful** → the dominant weather signal is low-dimensional and physically interpretable; this is the cleanest story and easiest to communicate to energy economists. Adopt as the primary set of weather predictors.
**If not successful** → the physics-motivated aggregation is too crude (possibly because of transmission congestion remapping). Fall back to FPC (Step 2).

---

## Step 8 — Value-of-forecast reframing

**Motivation.** Katz & Murphy (1997); Hong et al. (2020). R² is not the natural ERCOT metric; $/year value of reducing forecast error is. A small R² with correct *direction* can still produce high economic value.

**What to do.**
1. Compute `d(congestion) / d(forecast_error)` per feature via the Step 3 or Step 7 coefficient map (already done for pixel regressions in `analysis/forecast_value_map.py`, but now under a proper AR baseline).
2. Estimate annual $ value of a 10% forecast-error reduction per channel: `value ≈ |β_c| · 0.1 · σ(error_c) · 8760`.
3. Quantify the value of switching HRRR→GFS or using a hypothetical ML-corrected forecast (FourCastNet / GraphCast skill).

**Go criterion.** Ballpark value is on the order of \$1M–\$100M/year; 95% CI for the value from Step 5 bootstrap is bounded away from zero.

**If successful** → the economic story makes the project publishable even if R² is modest.
**If not successful (value is indistinguishable from zero)** → the honest conclusion is that forecast improvements in this 2025 sample would not materially reduce ERCOT congestion cost. That is itself a publishable finding (null results in energy economics are rare and useful).

---

## Step 9 — Data expansion (contingency / stretch)

**Motivation.** Nearly every failure mode above — overfit MLPs, underpowered GRUs, wide bootstrap CIs, unstable tail R² — traces to T ≈ 2500. Going to the full 2025 plus 2024 (if backfillable) roughly triples T, which typically halves variance on R² estimates.

**Trigger.** Invoke this step if Step 5 CIs are too wide to distinguish treatment effects, or Step 4/6 underperforms in ways the literature attributes to sample size.

**What to do.**
1. Extend `DEFAULT_MONTHS` to all 12 months of 2025 (already downloaded per CLAUDE.md).
2. Assess feasibility of 2024 backfill: HRRR/GFS archives exist; ERA5 yes; ERCOT SCED disclosures yes. Main cost is compute (~2–3 hr/month for HRRR download).
3. Re-run the winning configuration from Steps 1–7 on the enlarged dataset.

**Go criterion.** Native R² increases by ≥ 0.05 and CIs tighten by ≥ 30% when T doubles.

**If successful** → all claims are strengthened; write-up uses the full dataset.
**If not successful (R² plateaus)** → confirms the 2025 sample already exhausts the learnable signal from these features; the route to further gains is richer features (Step 6/7) or a different outcome (e.g., nodal LMP variance instead of aggregate congestion cost).

---

## Suggested execution order

1. **Steps 1 and 5 in parallel** (clean AR baseline + bootstrap inference). Together they give the honest headline number. Low implementation cost, high clarifying value.
2. **Step 2** (FPC basis) — replaces the ad-hoc cluster choice with a principled one; enables Step 3.
3. **Step 3** (penalized functional regression) — interpretable coefficient maps.
4. **Step 7** (capacity-weighted scalars) in parallel with Step 6 (regimes + distributional). These address the two remaining weaknesses (interpretability and tails).
5. **Step 4** (GNN) is the most ambitious architectural move — defer until Steps 1–3 have defined the baseline it must beat.
6. **Step 8** (value-of-forecast) always runs last on whichever model wins.
7. **Step 9** (data expansion) is a contingency; trigger on failure at Steps 4/5/6.

## Dependencies and implementation notes

- EOF / FPC basis computation: `sklearn.decomposition.PCA` on the flattened training-hour spatial field, then project val/test; enforce training-only fit.
- Scalar-on-function regression: `pygam` (PenaltyMatrix, TensorTerm) or a custom torch implementation with explicit B-spline basis and quadratic penalty.
- Dependent wild bootstrap: resample by chunk blocks with Rademacher weights; use ~1000 replicates.
- Driscoll–Kraay / Conley SEs: implement via `pyfixest` with `vcov='DK(Bartlett,L=24)'` or `vcov='conley(100km)'`.
- GNN: `torch_geometric` with `GCNConv` / `SAGEConv`; build edge index from `scipy.spatial.cKDTree` on node coordinates.
- Distributional head: `torch.distributions.Normal` + pinball loss for quantile variants; `properscoring` for CRPS.

## Key literature anchors

- **Basis reduction**: Ramsay & Silverman (2005); Reiss et al. (2017); Hannachi, Jolliffe & Stephenson (2007); Cressie & Johannesson (2008).
- **Inference under dependence**: Conley (1999); Driscoll & Kraay (1998); Shao (2010); Lahiri (2003).
- **Spatio-temporal GPs**: Gneiting (2002); Cressie & Huang (1999); Heaton et al. (2019).
- **Energy forecasting**: Lago et al. (2021); Marcjasz et al. (2023); Hong et al. (2020).
- **GNNs for grid/weather**: Khodayar & Wang (2019); Wang et al. (2023); Lam et al. (2023, GraphCast).
- **Value of forecasts**: Katz & Murphy (1997); Mahoney et al. (2012).
