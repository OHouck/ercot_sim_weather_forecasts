"""
PCA dimension reduction for ERCOT spatial forecast error fields.

Fits PCA separately on each of 6 spatial channels — 4 forecast error channels
(wspd_error_1h, temp_error_1h, wspd_error_0h, temp_error_0h) and 2 realized
weather channels (era5_wspd, era5_temp) — using all 12 months of 2025.

Regresses economic_congestion_cost on the leading K PCA scores per channel,
plus all K² pairwise interaction terms between paired channels (1h wind×temp,
day-ahead wind×temp, and realized wind×temp), using OLS with HAC standard
errors and LASSO cross-validation for robustness.

Analysis pipeline:
  - Primary OLS: all K PCA scores + interactions + cyclic time controls
  - AR-controlled robustness: add lag-1h / lag-24h of congestion
  - LASSO CV: alpha selected via 5-fold chunk cross-validation
  - Regime-conditional OLS: high-wind, extreme-heat, stressed-grid subsamples
  - K sweep: K = 5, 10, 20 with LASSO to evaluate mode count

Usage:
    uv run python -m analysis.pca_analysis
    uv run python -m analysis.pca_analysis --n_components 10
"""

import argparse
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pyarrow.parquet as pq
import statsmodels.api as sm
from sklearn.decomposition import PCA
from sklearn.linear_model import LassoCV

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from helper_funcs import setup_directories

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# ── Configuration ──────────────────────────────────────────────────────────────

DEPVAR = "economic_congestion_cost"

# Forecast error fields: difference between model forecast and ERA5 reanalysis
ERROR_FIELDS = ["wspd_error_1h", "temp_error_1h", "wspd_error_0h", "temp_error_0h", "wspd100_error_1h", "wspd100_error_0h"]
# Realized weather fields: ERA5 observed values (captures the weather state)
REALIZED_FIELDS = ["era5_wspd", "era5_temp", "era5_wspd100"]
ALL_FIELDS = ERROR_FIELDS + REALIZED_FIELDS

FIELD_LABELS = {
    "wspd_error_1h": "HRRR 1h 10m Wind Error",
    "wspd100_error_1h": "HRRR 1h 100m Wind Error",
    "temp_error_1h": "HRRR 1h Temp Error",
    "wspd_error_0h": "GFS Day-Ahead 10m Wind Error",
    "wspd100_error_0h": "GFS Day-Ahead 100m Wind Error",
    "temp_error_0h": "GFS Day-Ahead Temp Error",
    "era5_wspd":     "Realized 10m Wind Speed (ERA5)",
    "era5_wspd100":  "Realized 100m Wind Speed (ERA5)",
    "era5_temp":     "Realized Temperature (ERA5)",
}

ALL_MONTHS = [(2025, m) for m in range(1, 13)]
N_COMPONENTS = 5     # default EOF modes per channel (K=5 optimal from nn_analysis)
CHUNK_DAYS = 5
TRAIN_FRAC = 0.70
RANDOM_STATE = 42
HAC_MAXLAGS = 24     # Newey-West truncation (1 day)
K_SWEEP_VALUES = [5, 10, 20]
LASSO_ALPHAS = np.logspace(-4, 2, 60)

# Significance color palette used across all forest plots
COLOR_P001 = "#00008B"   # dark blue — p < 0.01
COLOR_P005 = "#6699CC"   # light blue — p < 0.05
COLOR_NSIG = "#AAAAAA"   # gray — not significant


# ── Utility ────────────────────────────────────────────────────────────────────


def _r2(y_true, y_pred):
    """Coefficient of determination."""
    ss_res = np.sum((y_true - y_pred) ** 2)
    ss_tot = np.sum((y_true - y_true.mean()) ** 2)
    return float(1 - ss_res / max(ss_tot, 1e-12))


def _normalize_comp(comp):
    """Scale a PCA loading vector to max absolute value = 1."""
    m = np.abs(comp).max()
    return comp / m if m > 1e-10 else comp


def _sig_stars(p):
    """Return significance stars string for a p-value."""
    if p < 0.001:
        return "***"
    if p < 0.01:
        return "**"
    if p < 0.05:
        return "*"
    return ""


# ── Data loading ───────────────────────────────────────────────────────────────


def load_pixel_data(months):
    """Load pixel-hourly parquet files for the given months.

    Reads error fields, realized weather fields, and the congestion cost target.

    Parameters
    ----------
    months : list of (year, month)

    Returns
    -------
    pd.DataFrame
    """
    dirs = setup_directories()
    lmp_dir = Path(dirs["processed"]) / "combined_hourly_gridded_data"
    keep_cols = (
        ["pixel_id", "valid_time", "latitude", "longitude", DEPVAR]
        + ALL_FIELDS
    )

    dfs = []
    for year, month in months:
        path = lmp_dir / f"pixel_hourly_gfs+hrrr_{year}_{month:02d}.parquet"
        if not path.exists():
            print(f"  [WARNING] Missing: {path}")
            continue
        schema = pq.read_schema(path).names
        cols = [c for c in keep_cols if c in schema]
        df = pd.read_parquet(path, columns=cols)
        df["valid_time"] = pd.to_datetime(df["valid_time"])
        if df["valid_time"].dt.tz is not None:
            df["valid_time"] = df["valid_time"].dt.tz_localize(None)
        dfs.append(df)
        print(f"  Loaded {year}-{month:02d}: {len(df):,} rows")

    combined = pd.concat(dfs, ignore_index=True)
    print(f"  Total: {len(combined):,} pixel-hour rows, "
          f"{combined['valid_time'].nunique()} unique hours, "
          f"{combined['pixel_id'].nunique()} unique pixels")
    return combined


def pivot_channel_matrix(df, field, fill_value=0.0):
    """Pivot pixel-level values to an (hour × pixel) matrix.

    Parameters
    ----------
    df         : pd.DataFrame with columns [valid_time, pixel_id, field]
    field      : str
    fill_value : float — fill value for missing (pixel, hour) pairs

    Returns
    -------
    mat       : ndarray (T, N_pixels) float32
    hours     : pd.DatetimeIndex
    pixel_ids : ndarray (N_pixels,)
    """
    pivot = (
        df[["valid_time", "pixel_id", field]]
        .dropna(subset=[field])
        .pivot_table(index="valid_time", columns="pixel_id", values=field, aggfunc="first")
        .fillna(fill_value)
    )
    return (
        pivot.values.astype(np.float32),
        pd.DatetimeIndex(pivot.index),
        pivot.columns.values,
    )


def make_chunk_splits(hour_idx, chunk_days=CHUNK_DAYS, train_frac=TRAIN_FRAC,
                      seed=RANDOM_STATE):
    """Split a time index into train/test via shuffled 5-day temporal blocks.

    Parameters
    ----------
    hour_idx   : pd.DatetimeIndex
    chunk_days : int
    train_frac : float
    seed       : int

    Returns
    -------
    train_mask, test_mask : ndarray bool (T,)
    """
    dates = hour_idx.normalize()
    unique_dates = pd.DatetimeIndex(sorted(dates.unique()))
    chunk_ids = np.arange(len(unique_dates)) // chunk_days
    n_chunks = int(chunk_ids.max()) + 1

    rng = np.random.default_rng(seed)
    shuffled = rng.permutation(n_chunks)
    n_train = max(1, int(np.floor(n_chunks * train_frac)))
    train_chunks = set(shuffled[:n_train].tolist())

    date_to_chunk = pd.Series(chunk_ids, index=unique_dates)
    row_chunks = dates.map(date_to_chunk).values
    train_mask = np.isin(row_chunks, list(train_chunks))
    return train_mask, ~train_mask


# ── PCA fitting ────────────────────────────────────────────────────────────────


def fit_pca_channels(df, fields, K=N_COMPONENTS, seed=RANDOM_STATE):
    """Fit PCA per spatial channel on training hours; project all hours.

    PCA is fitted exclusively on the training split so that out-of-sample PC
    scores are genuine projections onto in-sample basis vectors. The pixel ×
    hour matrix for each field is built by pivoting the long-format dataframe.

    Parameters
    ----------
    df     : pd.DataFrame — pixel-hourly data
    fields : list of str — channel names
    K      : int — PC modes per channel
    seed   : int

    Returns
    -------
    scores_dict : dict {field: ndarray (T, K)} — PC scores per hour
    pca_dict    : dict {field: fitted PCA}
    hours       : pd.DatetimeIndex — common time index (intersection)
    lat_dict    : dict {field: ndarray} — pixel latitudes (decoded from pixel_id)
    lon_dict    : dict {field: ndarray} — pixel longitudes
    var_df      : pd.DataFrame — explained variance per field × mode
    """
    mats, hour_sets = {}, {}
    for field in fields:
        mat, hours_f, pids = pivot_channel_matrix(df, field)
        mats[field] = (mat, hours_f, pids)
        hour_sets[field] = set(hours_f.tolist())

    common_hours = sorted(
        hour_sets[fields[0]].intersection(*(hour_sets[f] for f in fields[1:]))
    )
    hours = pd.DatetimeIndex(common_hours)
    train_mask, _ = make_chunk_splits(hours, seed=seed)
    n_train = train_mask.sum()
    print(f"  Common hours: {len(hours)}  train={n_train}  test={len(hours)-n_train}")

    scores_dict, pca_dict, lat_dict, lon_dict = {}, {}, {}, {}
    var_rows = []

    for field in fields:
        mat, hours_f, pids = mats[field]
        h_ser = pd.Series(np.arange(len(hours_f)), index=hours_f)
        idx = h_ser.reindex(hours).values.astype(int)
        mat_c = mat[idx]  # (T, N_pixels)

        n_comp = min(K, n_train - 1, mat_c.shape[1])
        pca = PCA(n_components=n_comp, random_state=RANDOM_STATE)
        pca.fit(mat_c[train_mask])
        scores = pca.transform(mat_c).astype(np.float32)

        scores_dict[field] = scores
        pca_dict[field] = pca

        # Decode lat/lon from pixel_id strings (format: "lat_lon", e.g. "25.9_-97.4").
        # The latitude/longitude columns in the parquet are sparse/NaN for many rows.
        pid_parts = np.array([str(p).split("_") for p in pids])
        lat_dict[field] = pid_parts[:, 0].astype(float)
        lon_dict[field] = pid_parts[:, 1].astype(float)

        cumvar = np.cumsum(pca.explained_variance_ratio_) * 100
        for k in range(n_comp):
            var_rows.append({
                "field": field,
                "mode": k + 1,
                "var_pct": float(pca.explained_variance_ratio_[k] * 100),
                "cumvar_pct": float(cumvar[k]),
            })
        print(f"  {FIELD_LABELS.get(field, field)}: {n_comp} modes, "
              f"cumvar={cumvar[-1]:.1f}% ({n_comp} modes / {mat_c.shape[1]} pixels)")

    return scores_dict, pca_dict, hours, lat_dict, lon_dict, pd.DataFrame(var_rows)


# ── Regression matrix ──────────────────────────────────────────────────────────


def build_regression_matrix(scores_dict, error_fields, realized_fields,
                             hours, dep_series, K=N_COMPONENTS):
    """Assemble OLS design matrix from PCA scores, interactions, and time controls.

    Design matrix columns:
    - Cyclic time controls: sin/cos hour-of-day, sin/cos month, is_weekend (5)
    - PCA scores for each error channel: K × 4 = 4K
    - PCA scores for each realized channel: K × 2 = 2K
    - Interaction terms (all K² pairwise combinations per time horizon):
        PC_i(wspd_1h) × PC_j(temp_1h)  for i,j in 1..K  (1h error wind × temp)
        PC_i(wspd_0h) × PC_j(temp_0h)  for i,j in 1..K  (0h error wind × temp)
        PC_i(era5_wspd) × PC_j(era5_temp) for i,j in 1..K (realized wind × temp)

    AR lags are excluded from the primary specification so coefficients
    measure the total causal effect of forecast errors rather than the
    conditional effect given yesterday's congestion level.

    Parameters
    ----------
    scores_dict    : dict {field: ndarray (T, K)}
    error_fields   : list of str — forecast error channels
    realized_fields: list of str — realized weather channels
    hours          : pd.DatetimeIndex (T,)
    dep_series     : pd.Series — hourly target indexed by valid_time
    K              : int

    Returns
    -------
    X_df           : pd.DataFrame (T_clean, n_features)
    y              : ndarray (T_clean,) — log1p target
    hours_clean    : pd.DatetimeIndex
    feature_groups : dict — group name → list of column names
    """
    h, m = hours.hour, hours.month
    time_df = pd.DataFrame({
        "sin_hour":   np.sin(2 * np.pi * h / 24),
        "cos_hour":   np.cos(2 * np.pi * h / 24),
        "sin_month":  np.sin(2 * np.pi * m / 12),
        "cos_month":  np.cos(2 * np.pi * m / 12),
        "is_weekend": (hours.dayofweek >= 5).astype(float),
    }, index=hours)

    # PCA score columns
    eof_cols = {}
    for field in error_fields + realized_fields:
        if field not in scores_dict:
            continue
        sc = scores_dict[field]
        for i in range(sc.shape[1]):
            eof_cols[f"PC{i+1}_{field}"] = sc[:, i]
    eof_df = pd.DataFrame(eof_cols, index=hours)

    # Interaction terms (matched mode index)
    def _K(f1, f2):
        return min(K,
                   scores_dict[f1].shape[1] if f1 in scores_dict else 0,
                   scores_dict[f2].shape[1] if f2 in scores_dict else 0)

    int_cols = {}
    K_1h   = _K("wspd_error_1h", "temp_error_1h")
    K_0h   = _K("wspd_error_0h", "temp_error_0h")
    K_real = _K("era5_wspd", "era5_temp")
    for i in range(K_1h):
        for j in range(K_1h):
            int_cols[f"INT1h_PC{i+1}xPC{j+1}"] = (
                scores_dict["wspd_error_1h"][:, i] * scores_dict["temp_error_1h"][:, j]
            )
    for i in range(K_0h):
        for j in range(K_0h):
            int_cols[f"INT0h_PC{i+1}xPC{j+1}"] = (
                scores_dict["wspd_error_0h"][:, i] * scores_dict["temp_error_0h"][:, j]
            )
    for i in range(K_real):
        for j in range(K_real):
            int_cols[f"INTreal_PC{i+1}xPC{j+1}"] = (
                scores_dict["era5_wspd"][:, i] * scores_dict["era5_temp"][:, j]
            )
    int_df = pd.DataFrame(int_cols, index=hours)

    y_series = dep_series.reindex(hours).clip(lower=0)
    y_log = np.log1p(y_series)

    X_raw = pd.concat([time_df, eof_df, int_df], axis=1)
    valid = X_raw.notna().all(axis=1) & y_log.notna()
    X_clean = X_raw.loc[valid]
    y_clean = y_log.loc[valid].values
    hours_clean = pd.DatetimeIndex(X_clean.index)

    feature_groups = {
        "time_controls":     list(time_df.columns),
        "wspd_error_1h":     [f"PC{i+1}_wspd_error_1h" for i in range(K_1h)],
        "temp_error_1h":     [f"PC{i+1}_temp_error_1h" for i in range(K_1h)],
        "wspd_error_0h":     [f"PC{i+1}_wspd_error_0h" for i in range(K_0h)],
        "temp_error_0h":     [f"PC{i+1}_temp_error_0h" for i in range(K_0h)],
        "era5_wspd":         [f"PC{i+1}_era5_wspd" for i in range(K_real)],
        "era5_temp":         [f"PC{i+1}_era5_temp" for i in range(K_real)],
        "interactions_1h":   [f"INT1h_PC{i+1}xPC{j+1}" for i in range(K_1h) for j in range(K_1h)],
        "interactions_0h":   [f"INT0h_PC{i+1}xPC{j+1}" for i in range(K_0h) for j in range(K_0h)],
        "interactions_real": [f"INTreal_PC{i+1}xPC{j+1}" for i in range(K_real) for j in range(K_real)],
    }

    n_dropped = (~valid).sum()
    print(f"  Design matrix: {X_clean.shape[0]} hours × {X_clean.shape[1]} features "
          f"({n_dropped} dropped for NaN target)")
    return X_clean, y_clean, hours_clean, feature_groups


def standardize_pca_cols(X_df, train_mask):
    """Standardize PCA score and interaction columns to zero mean, unit variance.

    All columns starting with 'PC' or 'INT' are standardized using training-set
    moments. Time controls (sin/cos, is_weekend) are left on their natural scale.

    Parameters
    ----------
    X_df       : pd.DataFrame
    train_mask : ndarray bool (T,)

    Returns
    -------
    X_std  : pd.DataFrame
    stats  : pd.DataFrame — mean and std per column
    """
    scale_cols = [c for c in X_df.columns if c.startswith(("PC", "INT"))]
    X_std = X_df.copy()
    train_sub = X_df.loc[train_mask, scale_cols]
    mu = train_sub.mean()
    sigma = train_sub.std().clip(lower=1e-10)
    X_std[scale_cols] = (X_df[scale_cols] - mu) / sigma
    stats = pd.DataFrame({"column": scale_cols,
                          "mean": mu.values, "std": sigma.values})
    return X_std, stats


# ── OLS inference ──────────────────────────────────────────────────────────────


def run_ols_inference(y, X_df, feature_groups, maxlags=HAC_MAXLAGS):
    """Fit OLS with HAC (Newey-West) standard errors and run joint F-tests.

    Parameters
    ----------
    y              : ndarray (T,) — log1p target
    X_df           : pd.DataFrame (T, p) — standardized design matrix
    feature_groups : dict — group → list of column names
    maxlags        : int — HAC truncation lag

    Returns
    -------
    result    : statsmodels RegressionResultsWrapper
    f_tests   : dict {group: (f_stat, p_val)}
    col_names : list of str — regressor names including const
    """
    X_mat = sm.add_constant(X_df.values)
    col_names = ["const"] + list(X_df.columns)

    result = sm.OLS(y, X_mat).fit(
        cov_type="HAC",
        cov_kwds={"maxlags": maxlags, "use_correction": True},
    )
    print(f"  OLS HAC: R²={result.rsquared:.4f}  adj-R²={result.rsquared_adj:.4f}  "
          f"N={result.nobs:.0f}  F={result.fvalue:.2f}  p={result.f_pvalue:.2e}")

    coef_idx = {name: i for i, name in enumerate(col_names)}
    f_tests = {}
    for group, gcols in feature_groups.items():
        present = [c for c in gcols if c in coef_idx]
        if not present:
            continue
        R_mat = np.zeros((len(present), len(col_names)))
        for row_i, col in enumerate(present):
            R_mat[row_i, coef_idx[col]] = 1.0
        try:
            ft = result.f_test(R_mat)
            f_tests[group] = (float(np.squeeze(ft.statistic)), float(ft.pvalue))
        except Exception:
            f_tests[group] = (float("nan"), float("nan"))

    return result, f_tests, col_names


# ── LASSO analysis ─────────────────────────────────────────────────────────────


def run_lasso_analysis(y, X_df, feature_groups, train_mask, tables_dir):
    """LASSO cross-validation on PCA features.

    Applies LassoCV with a log-spaced alpha grid using 5 chunk-based CV folds.
    All features (including time controls) enter the regularized regression.
    Selected features (non-zero coefficients) are compared against the OLS
    significant features to assess robustness of the findings.

    Parameters
    ----------
    y              : ndarray (T,) — log1p target
    X_df           : pd.DataFrame (T, p) — standardized design matrix
    feature_groups : dict
    train_mask     : ndarray bool (T,)
    tables_dir     : Path

    Returns
    -------
    lasso    : fitted LassoCV
    coef     : pd.Series — all coefficients (zero for unselected)
    test_r2  : float — out-of-sample R² on log1p scale
    """
    lasso = LassoCV(alphas=LASSO_ALPHAS, cv=5, max_iter=100_000, fit_intercept=True,
                    random_state=RANDOM_STATE)
    lasso.fit(X_df.values[train_mask], y[train_mask])

    coef = pd.Series(lasso.coef_, index=X_df.columns)
    n_sel = int((coef != 0).sum())
    test_r2 = _r2(y[~train_mask], lasso.predict(X_df.values[~train_mask]))

    print(f"  LASSO: alpha={lasso.alpha_:.5f}  selected={n_sel}/{len(coef)}  "
          f"test R²={test_r2:.4f}")

    out = pd.DataFrame({
        "feature": coef.index,
        "coef_lasso": coef.values,
        "selected": (coef != 0).values,
    })
    out.to_csv(tables_dir / "pca_lasso_coefficients.csv", index=False)
    print(f"  Saved: {tables_dir / 'pca_lasso_coefficients.csv'}")

    return lasso, coef, test_r2


# ── Regime regressions ─────────────────────────────────────────────────────────


def run_regime_regressions(df, X_std, y, hours_clean, dep_series,
                           feature_groups, col_names, tables_dir):
    """OLS regressions conditioned on three weather/grid regimes.

    Regimes are defined from system-wide hourly aggregates:
      high_wind    : mean ERA5 wind speed > 75th percentile
      extreme_heat : mean ERA5 temperature > 90th percentile
      stressed_grid: economic_congestion_cost > 75th percentile

    Parameters
    ----------
    df           : pd.DataFrame — pixel-hourly data (for hourly aggregates)
    X_std        : pd.DataFrame (T_clean, p) — standardized design matrix
    y            : ndarray (T_clean,) — log1p target
    hours_clean  : pd.DatetimeIndex (T_clean,)
    dep_series   : pd.Series — full hourly target
    feature_groups : dict
    col_names    : list of str — column names (includes const)
    tables_dir   : Path

    Returns
    -------
    dict {regime_name: (ols_result, n_obs)}
    """
    hourly_agg = df.groupby("valid_time")[["era5_wspd", "era5_temp"]].mean()
    y_series = dep_series.reindex(hours_clean)

    thresholds = {
        "wspd_q75":  hourly_agg["era5_wspd"].quantile(0.75),
        "temp_q90":  hourly_agg["era5_temp"].quantile(0.90),
        "cong_q75":  y_series.quantile(0.75),
    }

    regime_masks = {
        "high_wind":    (hourly_agg["era5_wspd"].reindex(hours_clean)
                         > thresholds["wspd_q75"]).fillna(False).values,
        "extreme_heat": (hourly_agg["era5_temp"].reindex(hours_clean)
                         > thresholds["temp_q90"]).fillna(False).values,
        "stressed_grid":(y_series > thresholds["cong_q75"]).fillna(False).values,
    }

    results = {}
    rows = []
    for name, mask in regime_masks.items():
        n = mask.sum()
        if n < 200:
            print(f"  {name}: skipped (N={n} < 200)")
            continue
        X_sub = X_std.values[mask]
        y_sub = y[mask]
        ols = sm.OLS(y_sub, sm.add_constant(X_sub)).fit(
            cov_type="HAC",
            cov_kwds={"maxlags": HAC_MAXLAGS, "use_correction": True},
        )
        results[name] = (ols, int(n))
        print(f"  {name}: N={n}  R²={ols.rsquared:.4f}")
        for cname, coef, pval in zip(col_names[1:], ols.params[1:], ols.pvalues[1:]):
            rows.append({"regime": name, "feature": cname,
                         "coef": coef, "p_value": pval})

    pd.DataFrame(rows).to_csv(tables_dir / "pca_regime_coefficients.csv", index=False)
    print(f"  Saved: {tables_dir / 'pca_regime_coefficients.csv'}")
    return results


# ── K sweep ────────────────────────────────────────────────────────────────────


def run_k_sweep(df, dep_series, K_values, tables_dir):
    """Compare OLS and LASSO performance across K PCA modes per channel.

    Parameters
    ----------
    df          : pd.DataFrame — pixel-hourly data
    dep_series  : pd.Series — hourly target
    K_values    : list of int
    tables_dir  : Path

    Returns
    -------
    pd.DataFrame with K, n_features, OLS/LASSO train and test R²
    """
    # Fit PCA once at K_max, then slice to smaller K values
    K_max = max(K_values)
    sc_max, _, hrs_max, _, _, _ = fit_pca_channels(df, ALL_FIELDS, K=K_max)

    rows = []
    for K in K_values:
        print(f"\n  K = {K} modes per channel...")
        sc = {f: sc_max[f][:, :K] for f in ALL_FIELDS if f in sc_max}
        X_df, y, hrs_c, fg = build_regression_matrix(
            sc, ERROR_FIELDS, REALIZED_FIELDS, hrs_max, dep_series, K=K
        )
        train_mask, test_mask = make_chunk_splits(hrs_c, seed=RANDOM_STATE)
        X_std, _ = standardize_pca_cols(X_df, train_mask)

        # OLS
        ols, _, col_names = run_ols_inference(y, X_std, fg)
        X_mat = sm.add_constant(X_std.values)
        ols_test_r2 = _r2(y[test_mask], ols.predict(X_mat)[test_mask])

        # LASSO
        lasso = LassoCV(alphas=LASSO_ALPHAS, cv=5, max_iter=100_000,
                        fit_intercept=True, random_state=RANDOM_STATE)
        lasso.fit(X_std.values[train_mask], y[train_mask])
        lasso_test_r2 = _r2(y[test_mask], lasso.predict(X_std.values[test_mask]))
        n_sel = int((lasso.coef_ != 0).sum())

        print(f"    OLS test R²={ols_test_r2:.4f}  "
              f"LASSO test R²={lasso_test_r2:.4f}  "
              f"LASSO selected={n_sel}/{X_std.shape[1]}")

        rows.append({
            "K": K,
            "n_features": X_std.shape[1],
            "ols_insample_r2": float(ols.rsquared),
            "ols_test_r2": float(ols_test_r2),
            "lasso_alpha": float(lasso.alpha_),
            "lasso_n_selected": n_sel,
            "lasso_test_r2": float(lasso_test_r2),
        })

    k_df = pd.DataFrame(rows)
    k_df.to_csv(tables_dir / "pca_k_sweep.csv", index=False)
    print(f"  Saved: {tables_dir / 'pca_k_sweep.csv'}")
    return k_df


# ── AR robustness ──────────────────────────────────────────────────────────────


def build_ar_features(dep_series, hours):
    """Build lag-1h and lag-24h of log1p(congestion_cost).

    Parameters
    ----------
    dep_series : pd.Series indexed by valid_time
    hours      : pd.DatetimeIndex

    Returns
    -------
    ar_df : pd.DataFrame with lag_1h, lag_24h
    """
    y_log = np.log1p(dep_series.reindex(hours).clip(lower=0))
    return pd.DataFrame({"lag_1h": y_log.shift(1), "lag_24h": y_log.shift(24)},
                        index=hours)


# ── Visualization ──────────────────────────────────────────────────────────────


def _draw_texas(ax):
    """Add Texas coastline and state borders to a Cartopy GeoAxes."""
    import cartopy.feature as cfeature
    import cartopy.crs as ccrs
    ax.set_extent([-106.7, -93.4, 25.7, 36.6], crs=ccrs.PlateCarree())
    ax.coastlines(resolution="10m", linewidth=0.5, color="k")
    ax.add_feature(cfeature.STATES, linewidth=0.4, edgecolor="0.4")


def plot_pca_maps(pca_dict, lat_dict, lon_dict, fig_dir, K_show=5,
                  sig_levels=None, coef_levels=None):
    """Spatial heatmaps of the leading PCA mode loadings for each field.

    Each panel shows the spatial loading (eigenvector scaled to unit max),
    coloured red/blue. Panels for modes significant in the main OLS regression
    are outlined (dark blue border: p < 0.01, light blue: p < 0.05) and their
    title includes the OLS coefficient and significance stars.

    Parameters
    ----------
    pca_dict    : dict {field: fitted PCA}
    lat_dict    : dict {field: ndarray} — pixel latitudes
    lon_dict    : dict {field: ndarray} — pixel longitudes
    fig_dir     : Path
    K_show      : int
    sig_levels  : dict {(field, mode_1based): p_value} or None
    coef_levels : dict {(field, mode_1based): coef} or None — OLS coefficients
    """
    import cartopy.crs as ccrs
    from matplotlib.colors import TwoSlopeNorm

    fields = list(pca_dict.keys())
    n_rows = len(fields)

    fig, axes = plt.subplots(
        n_rows, K_show,
        figsize=(K_show * 2.6, n_rows * 2.2),
        subplot_kw={"projection": ccrs.PlateCarree()},
    )
    if n_rows == 1:
        axes = axes[np.newaxis, :]

    for r, field in enumerate(fields):
        pca = pca_dict[field]
        lats, lons = lat_dict[field], lon_dict[field]
        vr = pca.explained_variance_ratio_
        label = FIELD_LABELS.get(field, field)

        for k in range(min(K_show, len(vr))):
            ax = axes[r, k]
            comp_n = _normalize_comp(pca.components_[k])
            norm = TwoSlopeNorm(vmin=-1.0, vcenter=0, vmax=1.0)
            ax.scatter(lons, lats, c=comp_n, cmap="RdBu_r", norm=norm,
                       s=3, transform=ccrs.PlateCarree(), rasterized=True)
            _draw_texas(ax)

            p_val = (sig_levels or {}).get((field, k + 1), 1.0)
            coef  = (coef_levels or {}).get((field, k + 1))

            if p_val < 0.01:
                bc, blw = COLOR_P001, 3.0
            elif p_val < 0.05:
                bc, blw = COLOR_P005, 2.0
            else:
                bc, blw = "lightgray", 0.5
            for spine in ax.spines.values():
                spine.set_edgecolor(bc)
                spine.set_linewidth(blw)
                spine.set_visible(True)

            var_str = f"var={vr[k]*100:.1f}%"
            if p_val < 0.05 and coef is not None:
                coef_line = f"\nβ={coef:+.2f}{_sig_stars(p_val)}"
            else:
                coef_line = ""
            mode_title = f"PC{k+1} ({var_str}){coef_line}"
            if k == 0:
                ax.set_title(f"{label}\n{mode_title}", fontsize=6.5, pad=3)
            else:
                ax.set_title(mode_title, fontsize=7)

    fig.suptitle(
        "PCA Mode Loadings per Field  —  dark blue border: p < 0.01, "
        "light blue: p < 0.05 (primary OLS)",
        fontsize=8.5, y=1.01,
    )
    plt.tight_layout()
    out = fig_dir / "pca_component_maps.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out}")


def plot_variance_explained(var_df, fig_dir):
    """Bar chart of individual and cumulative variance per channel.

    Parameters
    ----------
    var_df  : pd.DataFrame — [field, mode, var_pct, cumvar_pct]
    fig_dir : Path
    """
    fields = list(var_df["field"].unique())
    fig, axes = plt.subplots(1, len(fields), figsize=(len(fields) * 3.2, 3.5))
    if len(fields) == 1:
        axes = [axes]
    for ax, field in zip(axes, fields):
        sub = var_df[var_df["field"] == field]
        ax.bar(sub["mode"], sub["var_pct"], color="#3498db", alpha=0.7)
        ax2 = ax.twinx()
        ax2.plot(sub["mode"], sub["cumvar_pct"], "ro-", markersize=4)
        ax2.set_ylim(0, 105)
        ax2.set_ylabel("Cumul. var (%)", fontsize=7)
        ax.set_xlabel("PC mode", fontsize=8)
        ax.set_ylabel("Var explained (%)", fontsize=8)
        ax.set_title(FIELD_LABELS.get(field, field), fontsize=7.5)
        ax.set_xticks(sub["mode"])
    fig.suptitle("Variance Explained per PCA Channel", fontsize=9)
    plt.tight_layout()
    out = fig_dir / "pca_variance_explained.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out}")


def plot_coefficient_forest(result, col_names, feature_groups, fig_dir, title_suffix=""):
    """Coefficient forest plot with 95% HAC confidence intervals.

    Color code: dark blue = p < 0.01, light blue = p < 0.05, gray = n.s.

    Parameters
    ----------
    result        : statsmodels OLS result with HAC covariance
    col_names     : list of str
    feature_groups: dict
    fig_dir       : Path
    title_suffix  : str — appended to plot title
    """
    plot_order = [
        ("HRRR 1h Wind Error",         "wspd_error_1h"),
        ("HRRR 1h Temp Error",          "temp_error_1h"),
        ("GFS Day-Ahead Wind Error",    "wspd_error_0h"),
        ("GFS Day-Ahead Temp Error",    "temp_error_0h"),
        ("Realized Wind (ERA5)",        "era5_wspd"),
        ("Realized Temp (ERA5)",        "era5_temp"),
        ("Interaction 1h (wind×temp)",  "interactions_1h"),
        ("Interaction 0h (wind×temp)",  "interactions_0h"),
        ("Interaction Real (wind×temp)","interactions_real"),
    ]

    coef  = pd.Series(result.params, index=col_names)
    ci    = result.conf_int()
    ci_lo = pd.Series(ci[:, 0], index=col_names)
    ci_hi = pd.Series(ci[:, 1], index=col_names)
    pval  = pd.Series(result.pvalues, index=col_names)

    ordered_cols, ordered_labels, group_spans = [], [], []
    pos = 0
    for g_label, g_key in plot_order:
        gcols = feature_groups.get(g_key, [])
        present = [c for c in gcols if c in coef.index]
        if not present:
            continue
        group_spans.append((pos, pos + len(present), g_label))
        for c in present:
            ordered_cols.append(c)
            if c.startswith("INT") and "xPC" in c:
                # e.g. INT1h_PC2xPC3 -> "2×3"
                suffix = c.split("_", 1)[1]   # "PC2xPC3"
                ordered_labels.append(suffix.replace("PC", "").replace("x", "×"))
            else:
                ordered_labels.append(c.split("_")[0])   # e.g. "PC1"
            pos += 1

    n = len(ordered_cols)
    if n == 0:
        return
    y_pos = np.arange(n)
    colors = [
        COLOR_P001 if pval.get(c, 1.0) < 0.01
        else COLOR_P005 if pval.get(c, 1.0) < 0.05
        else COLOR_NSIG
        for c in ordered_cols
    ]

    bg = ["#f4f6f7", "#ffffff"]
    fig, ax = plt.subplots(figsize=(9, max(5, n * 0.38)))
    for i, (start, end, _) in enumerate(group_spans):
        ax.axhspan(start - 0.5, end - 0.5, color=bg[i % 2], alpha=0.5, zorder=0)

    for i, c in enumerate(ordered_cols):
        ax.errorbar(coef[c], y_pos[i],
                    xerr=[[max(coef[c] - ci_lo[c], 0)], [max(ci_hi[c] - coef[c], 0)]],
                    fmt="o", color=colors[i], markersize=5, capsize=3, linewidth=1.2)

    ax.axvline(0, color="k", linewidth=0.8, linestyle="--")
    ax.set_yticks(y_pos)
    ax.set_yticklabels(ordered_labels, fontsize=8)

    xlim = ax.get_xlim()
    x_right = xlim[1] + (xlim[1] - xlim[0]) * 0.01
    for start, end, g_label in group_spans:
        ax.text(x_right, (start + end - 1) / 2, g_label,
                fontsize=7, va="center", ha="left", style="italic", color="#444")

    ax.set_xlabel("OLS coefficient (log₁p scale, standardized inputs)", fontsize=9)
    ax.set_title(
        f"PCA Feature Coefficients — economic_congestion_cost{title_suffix}\n"
        "HAC s.e.  ●  dark blue p < 0.01  ●  light blue p < 0.05  ●  gray n.s.",
        fontsize=9,
    )
    ax.invert_yaxis()
    ax.grid(axis="x", linestyle=":", linewidth=0.5, alpha=0.6)
    plt.tight_layout()
    fname = f"pca_coefficient_forest{title_suffix.replace(' ', '_')}.png"
    out = fig_dir / fname
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out}")


def plot_joint_ftest(f_tests, fig_dir):
    """Horizontal bar chart of -log10(p) for joint F-tests per feature group.

    Parameters
    ----------
    f_tests : dict {group: (f_stat, p_val)}
    fig_dir : Path
    """
    labels_map = {
        "time_controls":     "Cyclic time controls",
        "wspd_error_1h":     "HRRR 1h wind error (PCs)",
        "temp_error_1h":     "HRRR 1h temp error (PCs)",
        "wspd_error_0h":     "GFS 0h wind error (PCs)",
        "temp_error_0h":     "GFS 0h temp error (PCs)",
        "era5_wspd":         "Realized wind (ERA5 PCs)",
        "era5_temp":         "Realized temp (ERA5 PCs)",
        "interactions_1h":   "Interactions 1h (wind×temp)",
        "interactions_0h":   "Interactions 0h (wind×temp)",
        "interactions_real": "Interactions realized (wind×temp)",
    }
    groups = [g for g, (f, p) in f_tests.items() if not np.isnan(f)]
    pvals  = [f_tests[g][1] for g in groups]
    fstats = [f_tests[g][0] for g in groups]
    neg_lp = [-np.log10(max(p, 1e-20)) for p in pvals]
    nice   = [labels_map.get(g, g) for g in groups]

    fig, ax = plt.subplots(figsize=(8.5, max(3.5, len(groups) * 0.45)))
    colors = [COLOR_P001 if p < 0.01 else COLOR_P005 if p < 0.05 else COLOR_NSIG
              for p in pvals]
    bars = ax.barh(nice, neg_lp, color=colors)
    ax.axvline(-np.log10(0.05), color="k", linestyle="--", linewidth=1, label="p = 0.05")
    ax.axvline(-np.log10(0.01), color="k", linestyle=":",  linewidth=1, label="p = 0.01")
    for bar, fstat, pval in zip(bars, fstats, pvals):
        ax.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height() / 2,
                f"F = {fstat:.2f},  p = {pval:.3f}", va="center", fontsize=7.5)
    ax.set_xlabel("−log₁₀(p-value) of joint HAC F-test", fontsize=9)
    ax.set_title("Joint Significance of PCA Feature Groups", fontsize=9)
    ax.legend(fontsize=8)
    ax.invert_yaxis()
    plt.tight_layout()
    out = fig_dir / "pca_joint_ftest.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out}")


def plot_lasso_results(lasso_coef, ols_result, col_names, feature_groups, fig_dir):
    """Side-by-side LASSO vs OLS coefficient comparison.

    Shows only weather-related features (time controls excluded).
    LASSO bars are colored only for non-zero selected features.

    Parameters
    ----------
    lasso_coef  : pd.Series — LASSO coefficients
    ols_result  : OLS result
    col_names   : list of str
    feature_groups : dict
    fig_dir     : Path
    """
    weather_groups = [
        "wspd_error_1h", "temp_error_1h", "wspd_error_0h", "temp_error_0h",
        "era5_wspd", "era5_temp",
        "interactions_1h", "interactions_0h", "interactions_real",
    ]
    weather_cols = [c for g in weather_groups for c in feature_groups.get(g, [])
                    if c in lasso_coef.index]
    if not weather_cols:
        return

    ols_coef  = pd.Series(ols_result.params[1:], index=col_names[1:])
    ols_pvals = pd.Series(ols_result.pvalues[1:], index=col_names[1:])

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, max(5, len(weather_cols) * 0.32)),
                                    sharey=True)
    y_pos = np.arange(len(weather_cols))
    labels = [
        c.split("_", 1)[1].replace("PC", "").replace("x", "×") if c.startswith("INT") and "xPC" in c
        else c.split("_")[0]
        for c in weather_cols
    ]

    # OLS
    ols_colors = [COLOR_P001 if ols_pvals.get(c, 1) < 0.01
                  else COLOR_P005 if ols_pvals.get(c, 1) < 0.05
                  else COLOR_NSIG for c in weather_cols]
    ax1.barh(y_pos, [ols_coef.get(c, 0) for c in weather_cols],
             color=ols_colors, alpha=0.8)
    ax1.axvline(0, color="k", lw=0.8, ls="--")
    ax1.set_xlabel("OLS coefficient", fontsize=9)
    ax1.set_title("OLS (HAC s.e.)\ndark blue p<0.01, light blue p<0.05", fontsize=8)
    ax1.set_yticks(y_pos)
    ax1.set_yticklabels(labels, fontsize=8)
    ax1.invert_yaxis()

    # LASSO
    lasso_vals = [lasso_coef.get(c, 0) for c in weather_cols]
    lasso_colors = [COLOR_P001 if v != 0 else "#DDDDDD" for v in lasso_vals]
    ax2.barh(y_pos, lasso_vals, color=lasso_colors, alpha=0.8)
    ax2.axvline(0, color="k", lw=0.8, ls="--")
    ax2.set_xlabel("LASSO coefficient", fontsize=9)
    ax2.set_title("LASSO (selected = dark blue,\nnon-selected = gray)", fontsize=8)
    ax2.invert_yaxis()

    fig.suptitle("OLS vs LASSO Coefficient Comparison", fontsize=10)
    plt.tight_layout()
    out = fig_dir / "pca_lasso_vs_ols.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out}")


def plot_regime_comparison(regime_results, col_names, feature_groups, fig_dir):
    """Coefficient forest comparing primary OLS vs three regime subsamples.

    Shows only PCA error features (not time controls).

    Parameters
    ----------
    regime_results : dict {name: (ols_result, n_obs)}
    col_names      : list of str — from primary OLS (includes const)
    feature_groups : dict
    fig_dir        : Path
    """
    error_groups = ["wspd_error_1h", "temp_error_1h", "wspd_error_0h", "temp_error_0h",
                    "interactions_1h", "interactions_0h"]
    error_cols = [c for g in error_groups for c in feature_groups.get(g, [])
                  if c in col_names]
    if not error_cols or not regime_results:
        return

    regime_names = list(regime_results.keys())
    n_reg = len(regime_names)
    n_feat = len(error_cols)
    labels = [c.replace("_error", "").replace("PC", "PC") for c in error_cols]

    palette = ["#2c3e50", "#e74c3c", "#27ae60", "#8e44ad"]
    offsets = np.linspace(-0.3, 0.3, n_reg + 1)

    fig, ax = plt.subplots(figsize=(9, max(6, n_feat * 0.38)))
    for ri, (rname, (ols_r, n_obs)) in enumerate(regime_results.items()):
        rcoef  = pd.Series(ols_r.params[1:], index=col_names[1:])
        rci    = ols_r.conf_int()
        rci_lo = pd.Series(rci[1:, 0], index=col_names[1:])
        rci_hi = pd.Series(rci[1:, 1], index=col_names[1:])
        for i, c in enumerate(error_cols):
            if c not in rcoef.index:
                continue
            y = i + offsets[ri]
            ax.errorbar(rcoef[c], y,
                        xerr=[[max(rcoef[c]-rci_lo[c], 0)], [max(rci_hi[c]-rcoef[c], 0)]],
                        fmt="o", color=palette[ri % len(palette)],
                        markersize=4, capsize=2, linewidth=1,
                        label=f"{rname} (N={n_obs})" if i == 0 else "")

    ax.axvline(0, color="k", lw=0.8, ls="--")
    ax.set_yticks(np.arange(n_feat))
    ax.set_yticklabels(labels, fontsize=8)
    ax.set_xlabel("OLS coefficient (log₁p scale, standardized)", fontsize=9)
    ax.set_title("Regime-Conditional Regressions — Error PCA Coefficients", fontsize=9)
    ax.legend(loc="lower right", fontsize=7.5)
    ax.invert_yaxis()
    ax.grid(axis="x", linestyle=":", linewidth=0.5, alpha=0.6)
    plt.tight_layout()
    out = fig_dir / "pca_regime_comparison.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out}")


def plot_k_sweep(k_df, fig_dir):
    """Line chart of OLS and LASSO performance vs K modes per channel.

    Parameters
    ----------
    k_df    : pd.DataFrame — output of run_k_sweep
    fig_dir : Path
    """
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(9, 3.5))
    Ks = k_df["K"]
    ax1.plot(Ks, k_df["ols_test_r2"],   "o-", color="#2980b9", label="OLS test R²")
    ax1.plot(Ks, k_df["lasso_test_r2"], "s-", color="#e67e22", label="LASSO test R²")
    ax1.plot(Ks, k_df["ols_insample_r2"], "--", color="#2980b9", alpha=0.4,
             label="OLS in-sample R²")
    ax1.set_xlabel("K (PCA modes per channel)", fontsize=9)
    ax1.set_ylabel("R² (log1p scale)", fontsize=9)
    ax1.set_title("Predictive R² vs K", fontsize=9)
    ax1.legend(fontsize=8)
    ax1.set_xticks(Ks)
    ax1.grid(axis="y", linestyle=":", alpha=0.5)

    ax2.bar(Ks, k_df["n_features"],       label="Total features", alpha=0.6, color="#95a5a6")
    ax2.bar(Ks, k_df["lasso_n_selected"], label="LASSO selected",  alpha=0.8, color="#e67e22")
    ax2.set_xlabel("K (PCA modes per channel)", fontsize=9)
    ax2.set_ylabel("Number of features", fontsize=9)
    ax2.set_title("Total vs LASSO-Selected Features", fontsize=9)
    ax2.legend(fontsize=8)
    ax2.set_xticks(Ks)

    fig.suptitle("K-Sweep: PCA Modes per Channel", fontsize=10)
    plt.tight_layout()
    out = fig_dir / "pca_k_sweep.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out}")


def plot_interaction_maps(pca_dict, lat_dict, lon_dict,
                          ols_result, col_names, feature_groups, fig_dir,
                          sig_threshold=0.05):
    """Paired loading maps for significant HRRR-1h wind×temperature interaction terms.

    For each significant INT1h_PC{i}xPC{j} term shows a narrow header row with
    the interaction label and β coefficient, then wind and temperature loading
    maps side by side.

    Parameters
    ----------
    pca_dict       : dict {field: fitted PCA}
    lat_dict       : dict {field: ndarray} — pixel latitudes
    lon_dict       : dict {field: ndarray} — pixel longitudes
    ols_result     : statsmodels OLS result
    col_names      : list of str (includes const)
    feature_groups : dict
    fig_dir        : Path
    sig_threshold  : float
    """
    import cartopy.crs as ccrs
    from matplotlib.colors import TwoSlopeNorm

    pval_ser = pd.Series(ols_result.pvalues[1:], index=col_names[1:])
    coef_ser = pd.Series(ols_result.params[1:],  index=col_names[1:])
    int_cols = feature_groups.get("interactions_1h", [])
    sig_terms = [
        (c, coef_ser[c], pval_ser.get(c, 1.0))
        for c in int_cols
        if pval_ser.get(c, 1.0) < sig_threshold
    ][:20]
    if not sig_terms:
        print("  No significant INT1h terms to plot.")
        return

    n_sig  = len(sig_terms)
    n_cols = 2   # pairs per row
    n_rows = (n_sig + n_cols - 1) // n_cols

    crs_pc = ccrs.PlateCarree()
    norm   = TwoSlopeNorm(vmin=-1.0, vcenter=0.0, vmax=1.0)

    fig     = plt.figure(figsize=(10, n_rows * 2.55))
    fig.subplots_adjust(top=0.94, left=0.03, right=0.97, bottom=0.02)
    subfigs = np.atleast_2d(fig.subfigures(n_rows, n_cols, hspace=0.08, wspace=0.02))

    for p, (col_name, coef, pval) in enumerate(sig_terms):
        suffix = col_name.split("_", 1)[1]
        i_str, j_str = suffix.split("x")
        i = int(i_str[2:]) - 1
        j = int(j_str[2:]) - 1

        sf = subfigs[p // n_cols, p % n_cols]

        ax_w, ax_t = sf.subplots(1, 2, subplot_kw={"projection": crs_pc})

        ax_w.scatter(lon_dict["wspd_error_1h"], lat_dict["wspd_error_1h"],
                     c=_normalize_comp(pca_dict["wspd_error_1h"].components_[i]),
                     cmap="RdBu_r", norm=norm, s=3, transform=crs_pc, rasterized=True)
        _draw_texas(ax_w)
        ax_w.set_title(f"Wind PC{i+1}", fontsize=8)
        ax_w.text(0.5, -0.22, f"PC{i+1}×PC{j+1}  β={coef:+.3f}{_sig_stars(pval)}",
                  transform=ax_w.transAxes, ha="center", va="top", fontsize=8.5,
                  fontweight="bold", color=COLOR_P001 if pval < 0.01 else COLOR_P005)

        ax_t.scatter(lon_dict["temp_error_1h"], lat_dict["temp_error_1h"],
                     c=_normalize_comp(pca_dict["temp_error_1h"].components_[j]),
                     cmap="RdBu_r", norm=norm, s=3, transform=crs_pc, rasterized=True)
        _draw_texas(ax_t)
        ax_t.set_title(f"Temp PC{j+1}", fontsize=8)

    # Hide unused subfigures when n_sig is not a multiple of n_cols
    for p in range(n_sig, n_rows * n_cols):
        subfigs[p // n_cols, p % n_cols].set_visible(False)

    fig.text(0.5, 0.99, "Significant HRRR-1h Wind × Temperature Interaction Terms",
             ha="center", va="top", fontsize=10, fontweight="bold")
    out = fig_dir / "pca_interaction_maps.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out}")


# ── Main analysis ──────────────────────────────────────────────────────────────


def run_pca_analysis(months=None, K=N_COMPONENTS):
    """Run the full PCA inference analysis pipeline.

    Steps:
    1.  Load all 12 months of pixel-hourly data (error fields + realized weather).
    2.  Fit PCA per channel on training hours only.
    3.  Build regression matrix (PCA scores + interactions + cyclic controls).
    4.  OLS with HAC s.e. — primary specification.
    5.  AR-controlled robustness check.
    6.  LASSO cross-validation.
    7.  Regime-conditional regressions (high-wind, extreme-heat, stressed-grid).
    8.  K-sweep (K = 5, 10, 20).
    9.  Save tables and figures.

    Parameters
    ----------
    months : list of (year, month) — defaults to all 12 months of 2025
    K      : int — PCA modes per channel

    Returns
    -------
    dict with all results
    """
    if months is None:
        months = ALL_MONTHS

    dirs = setup_directories()
    fig_dir    = Path(dirs["figures"]) / "pca_analysis"
    tables_dir = Path(dirs["tables"])
    fig_dir.mkdir(parents=True, exist_ok=True)
    tables_dir.mkdir(parents=True, exist_ok=True)

    # 1. Load data
    print("\n=== Step 1: Loading pixel-hourly data ===")
    df = load_pixel_data(months)
    dep_series = df.groupby("valid_time")[DEPVAR].first()
    n_tot, n_nan = len(dep_series), dep_series.isna().sum()
    print(f"  Target: {n_tot} hours, {n_nan} NaN ({n_nan/n_tot*100:.1f}%)")

    # 2. Fit PCA per channel
    print("\n=== Step 2: Fitting PCA per channel ===")
    scores_dict, pca_dict, hours, lat_dict, lon_dict, var_df = fit_pca_channels(
        df, ALL_FIELDS, K=K
    )
    var_df.to_csv(tables_dir / "pca_variance_explained.csv", index=False)

    # 3. Build regression matrix
    print("\n=== Step 3: Building regression matrix ===")
    X_df, y, hours_clean, feature_groups = build_regression_matrix(
        scores_dict, ERROR_FIELDS, REALIZED_FIELDS, hours, dep_series, K=K
    )
    train_mask, test_mask = make_chunk_splits(hours_clean, seed=RANDOM_STATE)
    X_std, scale_stats = standardize_pca_cols(X_df, train_mask)
    scale_stats.to_csv(tables_dir / "pca_scale_stats.csv", index=False)

    # 4. Primary OLS
    print("\n=== Step 4: Primary OLS (no AR lags) ===")
    ols_result, f_tests, col_names = run_ols_inference(y, X_std, feature_groups)

    X_mat = sm.add_constant(X_std.values)
    y_pred_log = ols_result.predict(X_mat)
    y_nat, y_pred_nat = np.expm1(y), np.expm1(y_pred_log)
    train_r2_nat = _r2(y_nat[train_mask], y_pred_nat[train_mask])
    test_r2_nat  = _r2(y_nat[test_mask],  y_pred_nat[test_mask])
    print(f"  Native scale: train R²={train_r2_nat:.4f}  test R²={test_r2_nat:.4f}")

    # 5. AR robustness
    print("\n=== Step 5: AR-controlled robustness ===")
    ar_df = build_ar_features(dep_series, hours)
    X_ar  = pd.concat([X_df, ar_df.reindex(X_df.index)], axis=1)
    valid_ar = X_ar.notna().all(axis=1)
    X_ar_clean = X_ar.loc[valid_ar]
    hours_ar = pd.DatetimeIndex(X_ar_clean.index)
    y_ar = np.log1p(dep_series.reindex(hours_ar).clip(lower=0).values)
    train_ar, _ = make_chunk_splits(hours_ar, seed=RANDOM_STATE)
    X_ar_std, _ = standardize_pca_cols(X_ar_clean, train_ar)
    fg_ar = {**feature_groups, "ar_lags": ["lag_1h", "lag_24h"]}
    ols_ar, f_tests_ar, col_names_ar = run_ols_inference(y_ar, X_ar_std, fg_ar)

    # 6. LASSO
    print("\n=== Step 6: LASSO cross-validation ===")
    lasso, lasso_coef, lasso_test_r2 = run_lasso_analysis(
        y, X_std, feature_groups, train_mask, tables_dir
    )

    # 7. Regime regressions
    print("\n=== Step 7: Regime-conditional regressions ===")
    regime_results = run_regime_regressions(
        df, X_std, y, hours_clean, dep_series, feature_groups, col_names, tables_dir
    )

    # 8. K sweep
    print("\n=== Step 8: K sweep (K = 5, 10, 20) ===")
    k_df = run_k_sweep(df, dep_series, K_SWEEP_VALUES, tables_dir)

    # 9. Save coefficient tables
    print("\n=== Step 9: Saving tables ===")

    def _save_coef_table(result, cnames, path):
        ci = result.conf_int()
        df_out = pd.DataFrame({
            "feature":  cnames,
            "coef":     result.params,
            "se_hac":   result.bse,
            "t_stat":   result.tvalues,
            "p_value":  result.pvalues,
            "ci_low":   ci[:, 0],
            "ci_high":  ci[:, 1],
            "signif":   pd.cut(result.pvalues,
                               bins=[-np.inf, 0.001, 0.01, 0.05, 0.1, np.inf],
                               labels=["***", "**", "*", ".", ""]),
        })
        df_out.to_csv(path, index=False)
        print(f"  Saved: {path}")

    _save_coef_table(ols_result, col_names,    tables_dir / "pca_ols_coefficients.csv")
    _save_coef_table(ols_ar,     col_names_ar, tables_dir / "pca_ols_ar_coefficients.csv")

    ftest_rows = (
        [{"group": g, "f_stat": v[0], "p_value": v[1], "spec": "no_ar"}
         for g, v in f_tests.items()]
        + [{"group": g, "f_stat": v[0], "p_value": v[1], "spec": "with_ar"}
           for g, v in f_tests_ar.items()]
    )
    pd.DataFrame(ftest_rows).to_csv(tables_dir / "pca_joint_ftests.csv", index=False)
    print(f"  Saved: {tables_dir / 'pca_joint_ftests.csv'}")

    # 10. Figures
    print("\n=== Step 10: Generating figures ===")

    # Build sig_levels and coef_levels dicts from primary OLS pvalues/coefficients
    sig_levels, coef_levels = {}, {}
    for col, pv, coef in zip(col_names[1:], ols_result.pvalues[1:], ols_result.params[1:]):
        if col.startswith("PC") and "_" in col:
            parts = col.split("_", 1)
            try:
                mode = int(parts[0][2:])
            except ValueError:
                continue
            field = parts[1]
            if field in ALL_FIELDS:
                sig_levels[(field, mode)]  = float(pv)
                coef_levels[(field, mode)] = float(coef)

    plot_variance_explained(var_df, fig_dir)
    plot_pca_maps(pca_dict, lat_dict, lon_dict, fig_dir,
                  K_show=min(K, 5), sig_levels=sig_levels, coef_levels=coef_levels)
    plot_coefficient_forest(ols_result, col_names, feature_groups, fig_dir)
    plot_joint_ftest(f_tests, fig_dir)
    plot_lasso_results(lasso_coef, ols_result, col_names, feature_groups, fig_dir)
    plot_regime_comparison(regime_results, col_names, feature_groups, fig_dir)
    plot_k_sweep(k_df, fig_dir)
    plot_interaction_maps(pca_dict, lat_dict, lon_dict,
                          ols_result, col_names, feature_groups, fig_dir)

    # Console summary
    n01 = (ols_result.pvalues[1:] < 0.01).sum()
    n05 = (ols_result.pvalues[1:] < 0.05).sum()
    print(f"\n=== Summary: {n01} features p<0.01, {n05} features p<0.05 ===")

    eof_rows = pd.read_csv(tables_dir / "pca_ols_coefficients.csv")
    eof_rows = eof_rows[~eof_rows["feature"].isin(
        ["const"] + feature_groups["time_controls"]
    )]
    print("\n  --- Weather PCA Coefficients ---")
    print(eof_rows.to_string(index=False))
    print("\n  --- Joint F-tests ---")
    for g, (f, p) in f_tests.items():
        print(f"    {g:<25}  F={f:.3f}  p={p:.4f}  {_sig_stars(p)}")
    print("\n  --- K sweep ---")
    print(k_df.to_string(index=False))

    return {
        "ols_result": ols_result, "ols_ar": ols_ar,
        "f_tests": f_tests,       "f_tests_ar": f_tests_ar,
        "col_names": col_names,   "col_names_ar": col_names_ar,
        "pca_dict": pca_dict,     "feature_groups": feature_groups,
        "lasso": lasso,           "lasso_coef": lasso_coef,
        "regime_results": regime_results,
        "k_df": k_df,             "var_df": var_df,
        "train_r2_nat": train_r2_nat, "test_r2_nat": test_r2_nat,
        "lasso_test_r2": lasso_test_r2,
        "n_hours": len(hours_clean), "K": K,
    }


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="PCA analysis for ERCOT congestion costs"
    )
    parser.add_argument("--n_components", type=int, default=N_COMPONENTS,
                        help=f"PCA modes per channel (default: {N_COMPONENTS})")
    args = parser.parse_args()
    run_pca_analysis(K=args.n_components)


if __name__ == "__main__":
    main()
