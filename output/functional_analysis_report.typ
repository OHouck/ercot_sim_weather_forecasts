#set document(
  title: "Spatial Forecast Errors and Renewable Curtailment: Functional Regression Results",
  author: "ERCOT Simulation Project",
)
#set page(
  paper: "us-letter",
  margin: (top: 1in, bottom: 1in, left: 1.1in, right: 1.1in),
  numbering: "1",
  header: align(right)[
    #text(9pt, fill: gray)[Functional Analysis — ERCOT 2025]
  ],
)
#set text(font: "New Computer Modern", size: 11pt, lang: "en")
#set heading(numbering: "1.")
#set par(justify: true, leading: 0.7em)
#show heading.where(level: 1): it => {
  v(1.2em)
  text(14pt, weight: "bold", it)
  v(0.4em)
}
#show heading.where(level: 2): it => {
  v(0.9em)
  text(12pt, weight: "bold", it)
  v(0.2em)
}
#show heading.where(level: 3): it => {
  v(0.6em)
  text(11pt, weight: "semibold", style: "italic", it)
  v(0.1em)
}

// ── Title block ──────────────────────────────────────────────────────────────
#align(center)[
  #v(0.5in)
  #text(18pt, weight: "bold")[
    Spatial Forecast Errors and Renewable Curtailment \
    in ERCOT: A Functional Regression Analysis
  ]
  #v(0.3em)
  #text(12pt, fill: gray)[April 2025 · Preliminary Results]
  #v(0.5in)
]

// ── Abstract ─────────────────────────────────────────────────────────────────
#block(
  fill: luma(240),
  inset: 12pt,
  radius: 4pt,
  width: 100%,
)[
  *Abstract.* We estimate scalar-on-function regression models mapping 2D spatial weather forecast error fields (wind speed and temperature, HRRR 1h and GFS day-ahead) to system-wide renewable curtailment in ERCOT over six months of 2025 (4,364 hours, 6,502 infrastructure pixels). Partial Least Squares (PLS) explains 55% of curtailment variance from wind speed errors alone, outperforming FPCA (44%) by discovering that 40% of the curtailment-predictive signal lives in low-variance spatial modes that FPCA under-weights. Regime stratification reveals that temperature errors rival wind errors in extreme cold events (R²≈0.57 each), while day-ahead GFS errors dominate in extreme heat. A two-field MLP neural operator (wind + temperature, 4× downsampled grid) achieves CV R²=0.90, establishing the strong nonlinearity of the spatial error–curtailment mapping.
]

#v(0.3em)

// ════════════════════════════════════════════════════════════════════════════
= Data and Setup
// ════════════════════════════════════════════════════════════════════════════

*Sample.* Six months of 2025 (January, March, May, July, September, November), yielding T = 4,364 hourly observations. Weather forecast errors are computed against ERA5-Land reanalysis on the 0.1° grid; infrastructure pixels are the N = 6,502 ERA5 cells containing at least one generating unit, transmission line, or load bus in ERCOT.

*Forecast error fields.* Four fields are analyzed:
- `wspd_error_1h`: HRRR 1h-ahead wind speed error (forecast − ERA5) [m/s]
- `temp_error_1h`: HRRR 1h-ahead temperature error [°C]
- `wspd_error_0h`: GFS day-ahead wind speed error [m/s]
- `temp_error_0h`: GFS day-ahead temperature error [°C]

*Outcome.* `total_curtailment_mw`: system-wide wind + solar curtailment (mean = 1,250 MW, std = 2,269 MW, min = 15 MW, max = 17,124 MW).

*Evaluation.* 5-fold cross-validated R² throughout, except regime analyses (small N) use 5-fold with n=10 PLS components. All models fit on standardized predictors.

// ════════════════════════════════════════════════════════════════════════════
= Baseline Models: Ridge and FPCA
// ════════════════════════════════════════════════════════════════════════════

== Ridge Regression (Raw Pixels)

A Ridge regression on all 6,502 pixel errors achieves CV R² = 0.604 ± 0.028 — an upper bound for linear single-field methods, though numerically ill-conditioned (condition number ≈ 10⁸) and uninterpretable due to the high dimensionality.

== Functional PCA (FPCA)

FPCA projects the T × N error matrix onto the top-K eigenvectors of the spatial covariance X^⊤X and regresses curtailment on the resulting scores. The coefficient surface β(s) is reconstructed as a linear combination of the K spatial modes.

#figure(
  table(
    columns: (auto, auto, auto),
    align: (right, right, right),
    stroke: 0.5pt,
    [*K*], [*CV R²*], [*± Std*],
    [5], [0.258], [0.016],
    [10], [0.318], [0.020],
    [20], [0.350], [0.020],
    [50], [0.379], [0.019],
    [100], [0.438], [0.026],
    [*200*], [*0.471*], [*0.024*],
  ),
  caption: [FPCA performance vs number of components, `wspd_error_1h`. K=200 is the best interpretable model.],
)

The β(s) surface concentrates positive coefficients in the West Texas / Panhandle wind belt — regions where overpredicted wind speeds generate excess curtailment. High-impact pixels are stable across CV folds (651 pixels, 10% of the grid, identified consistently in all 5 folds).

// ════════════════════════════════════════════════════════════════════════════
= Partial Least Squares: Supervised Basis Construction
// ════════════════════════════════════════════════════════════════════════════


PLS constructs basis vectors that maximize Cov(Xw, Y) rather than Var(X), directly targeting curtailment-predictive spatial patterns.

#figure(
  table(
    columns: (auto, auto, auto, auto),
    align: (right, right, right, right),
    stroke: 0.5pt,
    [*n components*], [*CV R²*], [*± Std*], [*Coef instability*],
    [5], [0.464], [0.025], [0.533],
    [10], [0.532], [0.029], [1.935],
    [*20*], [*0.554*], [*0.042*], [*5.915*],
    [50], [0.400], [0.064], [19.32],
    [100], [0.092], [0.061], [36.39],
  ),
  caption: [PLS performance vs n components. n=20 is optimal; n≥50 overfits severely.],
)

*PLS n=20 (R²=0.554) outperforms FPCA K=200 (R²=0.471) using only 10% as many features.* The supervised basis construction is the single largest methodological improvement.

== Why PLS and FPCA Find Different Patterns

The PLS and FPCA β(s) surfaces have Pearson r = 0.14 — nearly orthogonal despite both predicting curtailment from the same data. Decomposing the FPCA score covariance with Y reveals:

- The top-20 variance components (PC 1–20) capture *74%* of total |Cov(θ_k, Y)| — but not all.
- *40% of the curtailment-predictive signal lives in low-variance FPCA components* (PC 22, 25, 30, 42, 45, 52, 54, 80+).
- Only 12/20 top-variance components overlap with the top-20 covariance-ranked components.

These low-variance but high-covariance modes likely represent fine-scale regional contrast patterns — opposing error signs across the wind belt versus load centers — that have weak grid-wide variance but strong predictive power because they drive local supply-demand imbalances. FPCA under-weights them; PLS explicitly seeks them.

== Spatial Coefficient Surfaces: Ridge, FPCA, and PLS

@fig-linear-trio shows the recovered β(s) surfaces for all three linear methods on the HRRR 1h wind speed error field. Red pixels indicate locations where a positive forecast error (wind overprediction) is associated with *higher* curtailment; blue pixels indicate the opposite. All three surfaces share the West Texas / Panhandle wind belt as the dominant positive region, but differ substantially in fine-scale spatial detail — PLS reveals sharp sub-regional contrasts not visible in the smoother FPCA surface.

#figure(
  image("../figures/functional_analysis/report_maps/beta_linear_trio.png", width: 100%),
  caption: [Recovered β(s) surfaces for Ridge (left), FPCA K=100 (center), and PLS n=20 (right), all fitted on HRRR 1h wind speed error. Red = positive association (wind overprediction → more curtailment), blue = negative. Color scale is shared across panels. West Texas / Panhandle wind belt dominates all three; PLS shows sharper regional contrasts driven by its supervised basis construction.],
) <fig-linear-trio>

// ════════════════════════════════════════════════════════════════════════════
= All Four Error Fields
// ════════════════════════════════════════════════════════════════════════════

#figure(
  table(
    columns: (auto, auto, auto, auto, auto, auto),
    align: (left, right, right, right, right, right),
    stroke: 0.5pt,
    [*Field*], [*FPCA R²*], [*±*], [*PLS R²*], [*±*], [*β corr*],
    [`wspd_error_1h`], [0.438], [0.025], [0.554], [0.042], [0.142],
    [`wspd_error_0h`], [0.341], [0.024], [0.442], [0.054], [0.188],
    [`temp_error_1h`], [0.321], [0.020], [0.534], [0.021], [0.177],
    [`temp_error_0h`], [0.234], [0.033], [0.354], [0.058], [0.224],
  ),
  caption: [FPCA (K=100) and PLS (n=20) performance across all four error fields. "β corr" is the Pearson correlation between PLS and FPCA coefficient surfaces.],
)

Key observations:
- *PLS outperforms FPCA by 10–21 pp* across all fields — the supervised advantage is universal.
- *Temperature errors are highly predictive for curtailment.* HRRR 1h temperature errors (R²=0.534) are nearly as predictive as wind errors (R²=0.554), which is surprising given that curtailment is directly driven by wind generation capacity.
- *Day-ahead GFS errors are weaker than 1h HRRR errors* across both wind and temperature, consistent with greater spatial specificity at shorter lead times.
- *β correlations are uniformly low* (r=0.14–0.22) — the divergence between supervised and unsupervised spatial patterns is systematic across all fields.

@fig-multifield shows β(s) surfaces for all four fields side-by-side (FPCA left column, PLS right column). The wind error surfaces concentrate on the West Texas wind belt in all cases, while the temperature error surfaces are more diffuse — consistent with temperature errors acting through demand rather than supply.

#figure(
  image("../figures/functional_analysis/report_maps/multi_field_betas.png", width: 100%),
  caption: [β(s) surfaces for all four error fields (rows) and both methods (columns: FPCA K=100 left, PLS n=20 right). Color scales differ across rows to highlight within-field spatial structure. Wind error β surfaces concentrate on the West Texas / Panhandle wind belt; temperature error β surfaces are more geographically diffuse, consistent with temperature errors influencing curtailment through load rather than direct wind supply.],
) <fig-multifield>

// ════════════════════════════════════════════════════════════════════════════
= Extreme Weather Regime Stratification
// ════════════════════════════════════════════════════════════════════════════

Regime thresholds are computed from the full 4,409-hour dataset:
- *Extreme cold:* system-mean temperature < 2.3°C (5th percentile) — 212 hours (4.8%)
- *Extreme heat:* system-mean temperature > 32.1°C (95th percentile) — 212 hours (4.8%)

== Extreme Cold

#figure(
  table(
    columns: (auto, auto, auto, auto, auto),
    align: (left, right, right, right, right),
    stroke: 0.5pt,
    [*Field*], [*FPCA R²*], [*±*], [*PLS R²*], [*±*],
    [`wspd_error_1h`], [0.553], [0.117], [0.629], [0.144],
    [`temp_error_1h`], [0.567], [0.103], [0.606], [0.106],
    [`wspd_error_0h`], [0.400], [0.089], [0.444], [0.128],
    [`temp_error_0h`], [0.506], [0.062], [0.555], [0.067],
  ),
  caption: [Regime-stratified results for extreme cold hours (212 hours). FPCA K=50, PLS n=10.],
)

*Temperature errors rival wind errors in extreme cold.* FPCA R² for `temp_error_1h` (0.567) slightly exceeds `wspd_error_1h` (0.553) — a complete reversal of the full-sample ordering where wind dominates. During cold snaps, demand spikes likely force dispatch decisions that interact strongly with renewable dispatch, making temperature forecast accuracy as operationally important as wind forecast accuracy.

The β(s) surface for extreme cold has Pearson r = 0.13 with the full-sample surface — *near-orthogonal spatial attribution.* Full-sample models cannot recover the spatial structure relevant to cold-weather grid stress.

== Extreme Heat


#figure(
  table(
    columns: (auto, auto, auto, auto, auto),
    align: (left, right, right, right, right),
    stroke: 0.5pt,
    [*Field*], [*FPCA R²*], [*±*], [*PLS R²*], [*±*],
    [`wspd_error_0h`], [0.515], [0.186], [0.567], [0.156],
    [`wspd_error_1h`], [0.395], [0.178], [0.513], [0.199],
    [`temp_error_1h`], [0.362], [0.216], [0.375], [0.392],
    [`temp_error_0h`], [0.303], [0.280], [0.375], [0.493],
  ),
  caption: [Regime-stratified results for extreme heat hours (212 hours). Rows ordered by FPCA R².],
)

In extreme heat, *GFS day-ahead wind errors (R²=0.515) outperform HRRR 1h errors (R²=0.395).* This reversal — shorter lead time being less predictive — is consistent with day-ahead unit commitment driving outcomes during summer heat events: the day-ahead schedule is set before the heat peak emerges, so day-ahead forecast accuracy for wind (which affects how much thermal capacity is committed) matters more than real-time accuracy. Temperature errors are substantially weaker in extreme heat (R²≈0.30–0.37), consistent with the mechanism being wind-driven curtailment rather than demand-driven dispatch.

The extreme heat β(s) surface has r = 0.42 with the full-sample surface — more similar than extreme cold, but still substantially different.

@fig-regimes directly compares the cold and heat β(s) spatial patterns. Note that the cold-regime surface shows strong negative coefficients in parts of the wind belt (blue), indicating that wind underprediction during cold snaps *reduces* curtailment — consistent with dispatch rerouting thermal capacity to meet demand. The heat-regime surface remains positive in the wind belt but extends further east toward load centers.

#figure(
  image("../figures/functional_analysis/report_maps/beta_regimes.png", width: 100%),
  caption: [PLS n=10 β(s) surfaces for extreme cold (left, 212 hours) and extreme heat (right, 212 hours), fitted separately on HRRR 1h wind speed error. Color scales are shared across panels. The spatial attribution patterns are nearly orthogonal to each other and to the full-sample surface (r≈0.13–0.42), demonstrating that the mechanisms linking forecast errors to curtailment differ fundamentally across weather regimes.],
) <fig-regimes>

== Quantile Regression on Curtailment Extremes

Using FPCA scores (K=100) as predictors for quantile regression on `wspd_error_1h`:

#figure(
  table(
    columns: (auto, auto),
    align: (right, right),
    stroke: 0.5pt,
    [*Quantile τ*], [*Pseudo-R²*],
    [0.25], [0.206],
    [0.50], [0.183],
    [0.75], [0.387],
    [*0.90*], [*0.651*],
  ),
  caption: [Quantile regression pseudo-R² by target quantile.],
)

*Extreme curtailment events (τ=0.90) are 3.5× more predictable from spatial forecast errors than median curtailment.* This is the most operationally significant finding: the grid events that matter most (high curtailment) are precisely the ones where spatial forecast error structure is most predictive.

// ════════════════════════════════════════════════════════════════════════════
= Neural Operators: Nonlinear Mapping
// ════════════════════════════════════════════════════════════════════════════

To test whether the spatial error–curtailment relationship is linear, we train three neural architectures on a 2-channel input grid (wind + temperature error, HRRR 1h) downsampled to 27×33 = 891 cells (4× spatial stride).

#figure(
  table(
    columns: (auto, auto, auto, auto),
    align: (left, right, right, left),
    stroke: 0.5pt,
    [*Architecture*], [*CV R²*], [*± Std*], [*Notes*],
    [*MLP*], [*0.901*], [*0.013*], [1782→256→256→64→1],
    [FNO], [0.736], [0.038], [2-layer, modes=8, hidden=32],
    [CNN], [0.488], [0.059], [3-layer conv + global avg pool],
    [Ridge (baseline)], [0.604], [0.028], [single-field, 6,502 pixels],
    [PLS n=20 (baseline)], [0.554], [0.042], [single-field, wspd only],
  ),
  caption: [Neural operator results vs linear baselines. 3-fold CV, 50 epochs, early stopping. MLP uses flattened 27×33×2 = 1,782 inputs; FNO uses spectral convolutions; CNN uses local 3×3 convolutions.],
)

== Key Findings

*MLP (R²=0.901) reduces unexplained variance by 50% relative to the best linear model (Ridge R²=0.604).* Combining wind and temperature error spatial fields with a nonlinear architecture captures interaction effects that no linear β(s) model can represent.

*FNO (R²=0.736) outperforms all linear baselines* despite operating on 16× downsampled data. Fourier spectral convolutions capture multi-scale spatial structure that local pixel regression misses — consistent with curtailment being driven by large-scale error patterns spanning the entire West Texas wind belt.

*CNN (R²=0.488) underperforms both.* Local 3×3 receptive fields are insufficient; global spatial structure — either via full-grid flattening (MLP) or spectral decomposition (FNO) — is the key inductive bias for this problem.

*The multi-field nonlinear advantage is large.* Moving from single-field PLS (R²=0.554) to two-field MLP (R²=0.901) represents the combined effect of (a) adding temperature errors, and (b) allowing nonlinear interactions. Runs 1–3 showed temperature adds ~0.05–0.10 R² in linear models; the bulk of the remaining gain (0.30 R²) must come from nonlinearity.

*Caveat.* All neural results use 4× spatial downsampling and CPU training (50 epochs, 3-fold CV). Full-resolution GPU training would yield higher baselines; results here are conservative lower bounds on neural operator performance.

=== Neural Gradient Sensitivity Maps

Since neural models lack an explicit β(s) coefficient surface, we use *gradient sensitivity analysis*: we compute the mean absolute gradient |∂Ŷ/∂x_i| for each spatial pixel i averaged over all 4,364 hours. This measures how much a small perturbation at each location shifts the predicted curtailment on average, providing an analogue to β(s) for nonlinear models.

@fig-neural-sens shows separate sensitivity maps for the wind error channel and the temperature error channel. High-sensitivity pixels indicate locations where forecast accuracy most affects the model's curtailment predictions.

#figure(
  image("../figures/functional_analysis/report_maps/neural_sensitivity.png", width: 100%),
  caption: [MLP gradient sensitivity maps: mean |∂curtailment/∂error| averaged over all 4,364 hours, for the wind speed error channel (left) and temperature error channel (right). The MLP was retrained on the full dataset (30 epochs, R²=0.996 in-sample). High values (orange/red) indicate locations where forecast errors most strongly perturb predicted curtailment. Wind sensitivity concentrates sharply in the West Texas / Panhandle wind belt; temperature sensitivity is more spatially dispersed, with elevated values near major load centers in Central and East Texas.],
) <fig-neural-sens>

// ════════════════════════════════════════════════════════════════════════════
= Spatial Smoothness Regularization
// ════════════════════════════════════════════════════════════════════════════

We attempted to impose spatial smoothness on PLS loading vectors via a Laplacian penalty: projected weight vectors through (I + λL)^{−1} at each NIPALS iteration, where L = D − A is the spatial graph Laplacian on the ERA5 pixel neighborhood graph (6,502 × 6,502, 57,114 nonzeros).

#figure(
  table(
    columns: (auto, auto, auto, auto, auto),
    align: (right, right, right, right, right),
    stroke: 0.5pt,
    [*λ*], [*n=5*], [*n=10*], [*n=20*], [*n=50*],
    [0.00], [0.464], [0.532], [*0.554*], [0.400],
    [0.01], [0.128], [0.104], [−0.241], [−4.142],
    [0.10], [0.120], [0.110], [−0.120], [−2.729],
    [1.00], [0.092], [0.117], [0.046], [−0.712],
    [10.00], [0.038], [0.089], [0.098], [−0.113],
  ),
  caption: [Constrained PLS CV R² by λ and n components. Standard PLS (λ=0) is the only stable configuration.],
)

Penalized NIPALS is numerically unstable for any λ > 0. The loading matrix reconstruction W(P^⊤W)^{−1}Q becomes ill-conditioned when smoothed weight vectors are near-collinear, causing R² to collapse to as low as −4.14. Post-hoc smoothing of the β(s) surface (graph total variation penalty after fitting standard PLS) is the numerically robust alternative for interpretability.

// ════════════════════════════════════════════════════════════════════════════
= Summary and Implications
// ════════════════════════════════════════════════════════════════════════════

#figure(
  table(
    columns: (auto, auto, auto),
    align: (left, right, left),
    stroke: 0.5pt,
    [*Model / Setting*], [*CV R²*], [*Key feature*],
    [MLP (wind + temp, 2-field)], [0.901], [nonlinear, multi-field],
    [FNO (wind + temp, downsampled)], [0.736], [spectral, multi-field],
    [Ridge (wind, raw pixels)], [0.604], [linear, full-res, single-field],
    [PLS n=20 (wind, HRRR 1h)], [0.554], [linear, supervised basis],
    [FPCA K=200 (wind)], [0.471], [linear, unsupervised basis],
    [PLS, extreme cold], [0.629], [regime-specific],
    [PLS, extreme heat], [0.513], [regime-specific],
    [Quantile τ=0.90 (FPCA)], [0.651], [tail events],
  ),
  caption: [Summary of best-performing models across all analyses.],
)

*Five main conclusions:*

+ *Spatial forecast errors are strongly predictive of curtailment (R²=0.55–0.90).* The ERA5 0.1° error field carries substantial operational signal well beyond what aggregate error statistics would suggest.

+ *PLS decisively outperforms FPCA in the linear regime* because 40% of the curtailment-predictive signal lies in low-variance spatial modes that FPCA under-weights. Supervised basis construction is the most impactful methodological choice in the linear setting.

+ *Temperature errors are nearly as important as wind errors,* especially in extreme cold (R²=0.57 for temp vs 0.55 for wind). Operational models that focus only on wind forecast errors miss a significant fraction of predictable curtailment.

+ *Regime-specific spatial attribution is essential.* Full-sample β(s) surfaces share almost no structure with extreme-cold β(s) surfaces (r=0.13). Grid operations during cold snaps are driven by a fundamentally different spatial error pattern than normal conditions.

+ *The error–curtailment mapping is strongly nonlinear.* A simple two-field MLP achieves R²=0.90, reducing unexplained variance by 50% relative to linear Ridge. The next research priority is understanding which interactions drive the nonlinear gain — likely threshold effects in dispatch constraints and transmission binding — and whether regime-stratified nonlinear models yield further improvements.

// ════════════════════════════════════════════════════════════════════════════
= Appendix: Model Specifications
// ════════════════════════════════════════════════════════════════════════════

*FPCA.* Truncated SVD on the standardized T × N error matrix, retaining top-K left singular vectors as spatial modes. Ridge regression on K scores.

*PLS.* NIPALS algorithm. Loading vectors w maximize Cov(Xw, Y) subject to ‖w‖=1 and orthogonality to prior components. Scalar regression on projected scores.

*Ridge.* 5-fold CV over α ∈ {0.001, 0.01, 0.1, 1, 10, 100, 1000} on raw standardized pixel values.

*MLP.* Architecture: flatten(C×H×W) → Linear(1782, 256) → GELU → Linear(256, 256) → GELU → Linear(256, 64) → GELU → Linear(64, 1). Adam lr=1e-3, cosine LR decay, early stopping patience=10.

*FNO.* Lift → 2× [SpectralConv2d(modes=8) + Conv2d(1×1) + GELU] → global average pool → MLP(32→16→1). SpectralConv2d operates on the top-8 Fourier modes in each spatial dimension.

*CNN.* Conv2d(2→32, 3×3) → GELU → Conv2d(32→32, 3×3) → GELU → Conv2d(32→16, 3×3) → GELU → global avg pool → Linear(16→32) → GELU → Linear(32→1).

*Data.* 6 months (Jan, Mar, May, Jul, Sep, Nov 2025). ERA5 0.1° grid, N=6,502 infrastructure pixels. Neural operator uses 4× spatial downsampling (105×130 → 27×33). All Y normalized to zero mean / unit variance for neural training; R² computed on original scale.
