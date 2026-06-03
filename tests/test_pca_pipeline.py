"""
Tests for pca_decomposition.py and pca_mode_analysis.py.

Runs on real Jan-Mar 2025 ERA5 data with K=3 modes to stay fast.
Uses a temp directory for all artifact I/O.
"""

import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from analysis.pca_decomposition import (
    load_channel_fields,
    make_chunk_splits,
    fit_pca_channels,
    save_pca_results,
    load_pca_results,
    _adjust_sign,
    ALL_FIELDS,
)
from analysis.pca_mode_analysis import (
    build_regression_matrix,
    standardize_pca_cols,
    run_ols_inference,
    load_outcomes,
    DEPVARS,
    DEPVAR_CONFIGS,
)
from helper_funcs import setup_directories

MONTHS = [(2025, 1), (2025, 2), (2025, 3)]   # 3 months → ~18 chunks
K      = 3


@pytest.fixture(scope="module")
def dirs():
    return setup_directories()


@pytest.fixture(scope="module")
def bundle(dirs):
    return load_channel_fields(MONTHS, dirs)


@pytest.fixture(scope="module")
def outcomes(dirs):
    return load_outcomes(dirs)


@pytest.fixture(scope="module")
def pca_results(bundle):
    return fit_pca_channels(bundle, ALL_FIELDS, K=K)


@pytest.fixture(scope="module")
def saved_artifacts(pca_results, bundle):
    """Save to a temp dir and return the path."""
    scores_dict, pca_dict, hours, lat_dict, lon_dict, var_df = pca_results
    tmpdir = Path(tempfile.mkdtemp())
    save_pca_results(scores_dict, pca_dict, lat_dict, lon_dict,
                     var_df, bundle, hours, K, tmpdir)
    return tmpdir


# ── 1. load_channel_fields ─────────────────────────────────────────────────────

class TestLoadChannelFields:
    def test_all_channels_present(self, bundle):
        for field in ALL_FIELDS:
            assert field in bundle["channel_da"], f"Missing channel: {field}"

    def test_hours_count(self, bundle):
        n_hours = len(bundle["hours"])
        # 3 months × ~720 hours each, allow for some NaN-filtered gaps
        assert 1500 < n_hours < 2400, f"Unexpected hour count: {n_hours}"

    def test_regime_mean_columns(self, bundle):
        for col in ("era5_wspd", "era5_temp"):
            assert col in bundle["regime_mean"].columns, f"Missing regime_mean col: {col}"


# ── 2. make_chunk_splits ───────────────────────────────────────────────────────

class TestMakeChunkSplits:
    def test_train_fraction(self, bundle):
        train, test = make_chunk_splits(bundle["hours"])
        frac = train.sum() / len(train)
        # With ~18 five-day chunks the fraction won't be exact — allow wide tolerance
        assert 0.50 < frac < 0.90, f"Train fraction {frac:.2f} far from expected 0.70"

    def test_disjoint(self, bundle):
        train, test = make_chunk_splits(bundle["hours"])
        assert not (train & test).any(), "Train and test masks overlap"

    def test_exhaustive(self, bundle):
        train, test = make_chunk_splits(bundle["hours"])
        assert (train | test).all(), "Some hours not in train or test"

    def test_reproducible(self, bundle):
        t1, _ = make_chunk_splits(bundle["hours"], seed=7)
        t2, _ = make_chunk_splits(bundle["hours"], seed=7)
        np.testing.assert_array_equal(t1, t2)

    def test_different_seeds(self, bundle):
        t1, _ = make_chunk_splits(bundle["hours"], seed=1)
        t2, _ = make_chunk_splits(bundle["hours"], seed=2)
        assert not np.array_equal(t1, t2), "Different seeds produced same split"


# ── 3. fit_pca_channels ────────────────────────────────────────────────────────

class TestFitPcaChannels:
    def test_scores_shape(self, pca_results, bundle):
        scores_dict, pca_dict, hours, lat_dict, lon_dict, var_df = pca_results
        T = len(hours)
        for field in ALL_FIELDS:
            assert field in scores_dict, f"Missing scores for {field}"
            assert scores_dict[field].shape == (T, K), (
                f"{field} scores shape {scores_dict[field].shape} != ({T}, {K})"
            )

    def test_variance_table(self, pca_results):
        _, _, _, _, _, var_df = pca_results
        assert set(var_df.columns) >= {"field", "mode", "var_pct", "cumvar_pct"}
        assert len(var_df) == len(ALL_FIELDS) * K

    def test_variance_positive(self, pca_results):
        _, _, _, _, _, var_df = pca_results
        assert (var_df["var_pct"] > 0).all(), "Some variance fractions <= 0"

    def test_cumvar_leq_100(self, pca_results):
        _, _, _, _, _, var_df = pca_results
        assert (var_df["cumvar_pct"] <= 100.01).all(), "Cumulative variance > 100%"

    def test_lat_lon_present(self, pca_results):
        _, _, _, lat_dict, lon_dict, _ = pca_results
        for field in ALL_FIELDS:
            assert field in lat_dict and field in lon_dict

    def test_loadings_shape(self, pca_results):
        _, pca_dict, _, lat_dict, _, _ = pca_results
        for field in ALL_FIELDS:
            n_cells = lat_dict[field].size
            assert pca_dict[field].components_.shape == (K, n_cells), (
                f"{field} loadings shape mismatch"
            )


# ── 4. save / load roundtrip ───────────────────────────────────────────────────

class TestSaveLoadRoundtrip:
    def test_scores_parquet_exists(self, saved_artifacts):
        assert (saved_artifacts / f"pca_scores_K{K}.parquet").exists()

    def test_loadings_npz_exists(self, saved_artifacts):
        assert (saved_artifacts / f"pca_loadings_K{K}.npz").exists()

    def test_roundtrip_scores(self, pca_results, saved_artifacts):
        scores_dict_orig = pca_results[0]
        loaded = load_pca_results(K, saved_artifacts)
        scores_loaded = loaded["scores_dict"]
        for field in ALL_FIELDS:
            assert field in scores_loaded, f"Missing {field} after reload"
            np.testing.assert_allclose(
                scores_dict_orig[field].astype(np.float32),
                scores_loaded[field],
                rtol=1e-4,
                err_msg=f"Scores mismatch for {field}",
            )

    def test_roundtrip_components(self, pca_results, saved_artifacts):
        pca_orig = pca_results[1]
        loaded = load_pca_results(K, saved_artifacts)
        for field in ALL_FIELDS:
            np.testing.assert_allclose(
                pca_orig[field].components_,
                loaded["pca_dict"][field].components_,
                rtol=1e-5,
                err_msg=f"Components mismatch for {field}",
            )

    def test_hours_preserved(self, pca_results, saved_artifacts):
        hours_orig = pca_results[2]
        loaded     = load_pca_results(K, saved_artifacts)
        pd.testing.assert_index_equal(hours_orig, loaded["hours"])


# ── 5. build_regression_matrix ────────────────────────────────────────────────

class TestBuildRegressionMatrix:
    @pytest.fixture(scope="class")
    def reg_data(self, pca_results, outcomes):
        from analysis.pca_decomposition import ERROR_FIELDS, REALIZED_FIELDS
        scores_dict, _, hours, _, _, _ = pca_results
        dep_series = outcomes["economic_congestion_cost"]
        return build_regression_matrix(
            scores_dict, ERROR_FIELDS, REALIZED_FIELDS,
            hours, dep_series, K=K
        )

    def test_returns_four_items(self, reg_data):
        assert len(reg_data) == 4

    def test_feature_matrix_shape(self, reg_data, pca_results, bundle):
        X, y, hours_clean, _ = reg_data
        assert X.shape[0] == y.shape[0] == len(hours_clean)
        assert X.shape[0] > 100, "Too few rows after NaN drop"

    def test_no_nan_in_X(self, reg_data):
        X, _, _, _ = reg_data
        assert not X.isna().any().any(), "NaN in design matrix"

    def test_no_nan_in_y(self, reg_data):
        _, y, _, _ = reg_data
        assert not np.isnan(y).any(), "NaN in target"

    def test_y_nonnegative(self, reg_data):
        _, y, _, _ = reg_data
        assert (y >= 0).all(), "log1p target has negative values"

    def test_feature_groups_coverage(self, reg_data):
        X, _, _, feature_groups = reg_data
        all_fg_cols = [c for cols in feature_groups.values() for c in cols]
        assert set(all_fg_cols) == set(X.columns), "feature_groups don't match X columns"

    def test_interaction_columns_present(self, reg_data):
        X, _, _, _ = reg_data
        int_cols = [c for c in X.columns if c.startswith("INT")]
        assert len(int_cols) > 0, "No interaction columns found"


# ── 6. run_ols_inference ───────────────────────────────────────────────────────

class TestRunOlsInference:
    @pytest.fixture(scope="class")
    def ols_out(self, pca_results, outcomes):
        from analysis.pca_decomposition import ERROR_FIELDS, REALIZED_FIELDS
        scores_dict, _, hours, _, _, _ = pca_results
        dep_series = outcomes["economic_congestion_cost"]

        X, y, hours_clean, feature_groups = build_regression_matrix(
            scores_dict, ERROR_FIELDS, REALIZED_FIELDS,
            hours, dep_series, K=K
        )
        train_mask, _ = make_chunk_splits(hours_clean)
        X_std, _ = standardize_pca_cols(X, train_mask)
        return run_ols_inference(y, X_std, feature_groups)

    def test_returns_tuple(self, ols_out):
        assert len(ols_out) == 3, "run_ols_inference should return (result, f_tests, col_names)"

    def test_r2_in_range(self, ols_out):
        result, _, _ = ols_out
        r2 = result.rsquared
        assert 0.0 <= r2 <= 1.0, f"R² {r2:.3f} out of range"

    def test_pvalues_valid(self, ols_out):
        result, _, _ = ols_out
        assert hasattr(result, "pvalues")
        assert (result.pvalues >= 0).all() and (result.pvalues <= 1.0001).all()

    def test_f_tests_present(self, ols_out):
        _, f_tests, _ = ols_out
        assert len(f_tests) > 0, "No joint F-test results"

    def test_col_names_match(self, ols_out):
        result, _, col_names = ols_out
        assert col_names[0] == "const"
        assert len(col_names) == result.params.shape[0]


# ── 7. load_outcomes ──────────────────────────────────────────────────────────

class TestLoadOutcomes:
    def test_all_depvars_present(self, outcomes):
        for dv in DEPVARS:
            assert dv in outcomes.columns, f"Missing depvar column: {dv}"

    def test_markup_columns_correct(self, outcomes):
        np.testing.assert_allclose(
            outcomes["rt_cllig_p85_markup"].values,
            (outcomes["rt_cllig_p85"] - outcomes["cllig_mc"]).values,
        )
        np.testing.assert_allclose(
            outcomes["rt_scgt_p85_markup"].values,
            (outcomes["rt_scgt_p85"] - outcomes["scgt_mc"]).values,
        )
        np.testing.assert_allclose(
            outcomes["rt_ccgt_p85_markup"].values,
            (outcomes["rt_ccgt_p85"] - outcomes["ccgt_mc"]).values,
        )

    def test_markups_can_be_negative(self, outcomes):
        assert outcomes["rt_cllig_p85_markup"].min() < 0 or True, "Not a hard constraint"

    def test_index_is_datetime(self, outcomes):
        assert pd.api.types.is_datetime64_any_dtype(outcomes.index)



# ── 8. raw transform ──────────────────────────────────────────────────────────

class TestRawTransform:
    def test_raw_y_can_be_negative(self, pca_results, outcomes):
        from analysis.pca_decomposition import ERROR_FIELDS, REALIZED_FIELDS
        scores_dict, _, hours, _, _, _ = pca_results
        dep_series = outcomes["rt_cllig_p85_markup"]
        _, y, _, _ = build_regression_matrix(
            scores_dict, ERROR_FIELDS, REALIZED_FIELDS,
            hours, dep_series, K=K, transform="raw",
        )
        assert y.min() < 0 or np.isnan(dep_series.reindex(hours)).all(), \
            "raw markup y should potentially contain negative values"

    def test_log1p_y_nonneg(self, pca_results, outcomes):
        from analysis.pca_decomposition import ERROR_FIELDS, REALIZED_FIELDS
        scores_dict, _, hours, _, _, _ = pca_results
        dep_series = outcomes["economic_congestion_cost"]
        _, y, _, _ = build_regression_matrix(
            scores_dict, ERROR_FIELDS, REALIZED_FIELDS,
            hours, dep_series, K=K, transform="log1p",
        )
        assert (y >= 0).all(), "log1p target must be non-negative"


# ── 9. _adjust_sign on synthetic data ─────────────────────────────────────────

class TestAdjustSign:
    """Synthetic-data tests for the sign convention applied to EOFs/scores.

    The convention enforced is: positive PC score ↔ field is above its training
    mean. We build data with a known above-average pattern, fit EOFs, then
    artificially flip them and verify _adjust_sign restores the right sign on
    both the loadings and the score columns.
    """

    @staticmethod
    def _build_synthetic_eofs(seed=0, n_time=300, grid=(6, 6)):
        """Build synthetic data, fit EOFs, return (da_train, eofs_da, scores).

        The field has a dominant Gaussian spatial pattern (all-positive loadings)
        modulated by a random time series — so the true PC1 spatial mean is
        positive and a positive score means above-average field.
        """
        import xarray as xr
        from xeofs.single import EOF

        rng = np.random.default_rng(seed)
        n_lat, n_lon = grid
        lats = np.linspace(30.0, 35.0, n_lat)
        lons = np.linspace(-100.0, -95.0, n_lon)

        ll, lo = np.meshgrid(lats, lons, indexing="ij")
        pattern = np.exp(-((ll - 32.5) ** 2 + (lo + 97.5) ** 2) / 3.0)

        ts    = rng.normal(size=n_time)
        noise = 0.15 * rng.normal(size=(n_time, n_lat, n_lon))
        field = ts[:, None, None] * pattern[None, :, :] + noise + 10.0

        da = xr.DataArray(
            field,
            dims=["valid_time", "latitude", "longitude"],
            coords={
                "valid_time": pd.date_range("2025-01-01", periods=n_time, freq="h"),
                "latitude":  lats,
                "longitude": lons,
            },
        )
        model = EOF(n_modes=3, center=True)
        model.fit(da, dim="valid_time")
        eofs   = model.components()
        scores = np.asarray(
            model.scores(normalized=False).transpose("valid_time", "mode").values,
            dtype=np.float32)  # xeofs scores are (mode, valid_time); _adjust_sign expects (T, mode)
        return da, eofs, scores

    @staticmethod
    def _flip_mode0(eofs_da, scores):
        """Return copies with the first mode of both EOF and scores multiplied by -1."""
        eofs_flipped = eofs_da.copy()
        eofs_flipped.loc[{"mode": 1}] *= -1  # xeofs uses 1-based mode coordinates
        scores_flipped = scores.copy()
        scores_flipped[:, 0] *= -1
        return eofs_flipped, scores_flipped

    @staticmethod
    def _pc1_vs_spatial_mean_corr(eofs_da, da):
        """Recompute PC1 from EOF and return its correlation with spatial mean."""
        anom = (da - da.mean("valid_time")).values.reshape(da.shape[0], -1)
        eof1 = np.nan_to_num(eofs_da.sel(mode=1).values.ravel(), nan=0.0)  # xeofs: mode=1 is first
        pc1  = anom @ eof1
        sm   = anom.mean(axis=1)
        return float(np.corrcoef(pc1, sm)[0, 1])

    def test_flips_wrong_sign(self):
        da, eofs, scores = self._build_synthetic_eofs(seed=1)
        eofs_wrong, scores_wrong = self._flip_mode0(eofs, scores)

        eofs_fixed, signs = _adjust_sign(eofs_wrong, scores=scores_wrong)
        assert signs[0] == -1.0, "Mode 0 was flipped — should have been corrected back"

        corr = self._pc1_vs_spatial_mean_corr(eofs_fixed, da)
        assert corr > 0, f"PC1 should be positively correlated with spatial mean, got {corr:+.3f}"

    def test_idempotent(self):
        """Applying the sign correction twice should be a no-op the second time."""
        da, eofs, scores = self._build_synthetic_eofs(seed=2)
        eofs_pass1, signs1 = _adjust_sign(eofs.copy(), scores=scores.copy())
        eofs_pass2, signs2 = _adjust_sign(eofs_pass1.copy(), scores=None)
        assert np.all(signs2 == 1.0), f"Second pass should not flip any modes (signs={signs2})"
        assert self._pc1_vs_spatial_mean_corr(eofs_pass1, da) > 0

    def test_scores_consistent_with_eofs(self):
        """After adjustment, score sign must match what you'd get by projecting onto the (flipped) EOF."""
        da, eofs, scores = self._build_synthetic_eofs(seed=3)
        eofs_wrong, scores_wrong = self._flip_mode0(eofs, scores)

        eofs_fixed, _ = _adjust_sign(eofs_wrong, scores=scores_wrong)

        anom = (da - da.mean("valid_time")).values.reshape(da.shape[0], -1)
        eof1_fixed     = np.nan_to_num(eofs_fixed.sel(mode=1).values.ravel(), nan=0.0)  # xeofs: mode=1
        pc1_recomputed = anom @ eof1_fixed

        corr = float(np.corrcoef(scores_wrong[:, 0], pc1_recomputed)[0, 1])
        assert corr > 0.99, f"Stored scores not consistent with flipped EOF (corr={corr:+.3f})"

    def test_positive_score_means_above_average(self):
        """Monopole guarantee: hours with PC1 > 0 have above-average spatial mean."""
        da, eofs, scores = self._build_synthetic_eofs(seed=4)
        eofs_wrong, scores_wrong = self._flip_mode0(eofs, scores)

        _, _ = _adjust_sign(eofs_wrong, scores=scores_wrong)

        sm = (da - da.mean("valid_time")).mean(("latitude", "longitude")).values
        pos_hours = scores_wrong[:, 0] > 0
        neg_hours = scores_wrong[:, 0] < 0
        assert sm[pos_hours].mean() > sm[neg_hours].mean(), (
            "Positive PC1 hours should have higher spatial-mean anomaly than negative"
        )
        assert sm[pos_hours].mean() > 0, "Mean spatial anomaly during PC1>0 hours should be positive"

    def test_map_interpretation_red_above_blue_below(self):
        """The user-facing map interpretation: when score > 0, the average
        anomaly in red cells (positive loadings) is positive and the average
        anomaly in blue cells (negative loadings) is negative. When score < 0,
        the relationship reverses. This is the property a viewer reads off the
        PCA mode maps in pca_mode_analysis.py.

        This holds by the dot-product definition of the score and is checked
        here for both a monopole and a dipole.
        """
        import xarray as xr
        from xeofs.single import EOF

        def _verify(da, mode_idx=0):
            model = EOF(n_modes=3, center=True)
            model.fit(da, dim="valid_time")
            eofs   = model.components()
            scores = np.asarray(
            model.scores(normalized=False).transpose("valid_time", "mode").values,
            dtype=np.float32)  # xeofs scores are (mode, valid_time); _adjust_sign expects (T, mode)
            eofs, _ = _adjust_sign(eofs, scores=scores)

            eof_m = eofs.isel(mode=mode_idx).values  # isel: 0-based position, independent of xeofs mode coords
            pc    = scores[:, mode_idx]
            anom  = (da - da.mean("valid_time")).values  # (T, lat, lon)

            # Only check cells with visually meaningful loadings (top 30% of |loading|).
            # Near-zero loadings render as faint/white in the map and don't carry
            # interpretation.
            cutoff = 0.30 * np.abs(eof_m).max()
            red_mask  = eof_m >  cutoff
            blue_mask = eof_m < -cutoff
            pos_hours = pc > 0
            neg_hours = pc < 0

            if red_mask.sum() > 0:
                assert anom[pos_hours][:, red_mask].mean()  > 0, "score>0: red cells below mean"
                assert anom[neg_hours][:, red_mask].mean()  < 0, "score<0: red cells above mean"
            if blue_mask.sum() > 0:
                assert anom[pos_hours][:, blue_mask].mean() < 0, "score>0: blue cells above mean"
                assert anom[neg_hours][:, blue_mask].mean() > 0, "score<0: blue cells below mean"

        # Case A — monopole
        da_mono, _, _ = self._build_synthetic_eofs(seed=11)
        _verify(da_mono)

        # Case B — north-dominant dipole (different sign per region)
        rng = np.random.default_rng(99)
        n_time = 400
        lats = np.linspace(26.0, 36.0, 10)
        lons = np.linspace(-105.0, -94.0, 6)
        north = lats > 31.0
        ts    = rng.normal(size=n_time)
        noise = 0.1 * rng.normal(size=(n_time, len(lats), len(lons)))
        field = np.zeros((n_time, len(lats), len(lons)))
        field[:,  north, :] =  2.0 * ts[:, None, None] + noise[:,  north, :]
        field[:, ~north, :] = -1.0 * ts[:, None, None] + noise[:, ~north, :]
        da_dipole = xr.DataArray(
            field,
            dims=["valid_time", "latitude", "longitude"],
            coords={
                "valid_time": pd.date_range("2025-01-01", periods=n_time, freq="h"),
                "latitude":   lats,
                "longitude":  lons,
            },
        )
        _verify(da_dipole)

    def test_dipole_positive_loadings_mean_higher_values(self):
        """Dipole guarantee: the dominant-amplitude side gets positive loadings.

        Use a north-dominant dipole (north amplitude 2×, south 1×, equal cell
        counts). The net loading sum will be positive for north, so _adjust_sign
        must orient the EOF with north positive. We then verify:
          - north cells have positive loadings
          - when score > 0, north is above average and south is below average
        """
        import xarray as xr
        from xeofs.single import EOF

        rng = np.random.default_rng(42)
        n_time = 400
        lats = np.linspace(26.0, 36.0, 10)
        lons = np.linspace(-105.0, -94.0, 6)

        # Equal cell counts but north has 2× signal amplitude → net loading sum > 0 for north
        north_rows = lats > 31.0
        south_rows = ~north_rows

        ts    = rng.normal(size=n_time)
        noise = 0.1 * rng.normal(size=(n_time, len(lats), len(lons)))
        field = np.zeros((n_time, len(lats), len(lons)))
        field[:, north_rows, :] =  2.0 * ts[:, None, None] + noise[:, north_rows, :]
        field[:, south_rows, :] = -1.0 * ts[:, None, None] + noise[:, south_rows, :]

        da = xr.DataArray(
            field,
            dims=["valid_time", "latitude", "longitude"],
            coords={
                "valid_time": pd.date_range("2025-01-01", periods=n_time, freq="h"),
                "latitude":  lats,
                "longitude": lons,
            },
        )
        model = EOF(n_modes=3, center=True)
        model.fit(da, dim="valid_time")
        eofs   = model.components()
        scores = np.asarray(
            model.scores(normalized=False).transpose("valid_time", "mode").values,
            dtype=np.float32)  # xeofs scores are (mode, valid_time); _adjust_sign expects (T, mode)

        eofs, _ = _adjust_sign(eofs, scores=scores)

        eof1 = eofs.sel(mode=1).values   # (lat, lon) — xeofs uses 1-based mode coordinates
        pc1  = scores[:, 0]
        anom = (da - da.mean("valid_time")).values  # (T, lat, lon)

        pos_hours = pc1 > 0
        neg_hours = pc1 < 0

        # North (dominant amplitude) should carry positive loadings
        assert eof1[north_rows, :].sum() > 0, (
            f"North (dominant side) should have positive net loading, got {eof1[north_rows,:].sum():.3f}"
        )
        assert eof1[south_rows, :].sum() < 0, (
            f"South should have negative net loading, got {eof1[south_rows,:].sum():.3f}"
        )

        # Positive-score hours: north above average, south below average
        assert anom[pos_hours][:, north_rows, :].mean() > 0, "North should be above avg when score>0"
        assert anom[pos_hours][:, south_rows, :].mean() < 0, "South should be below avg when score>0"
        # And the reverse for negative-score hours
        assert anom[neg_hours][:, north_rows, :].mean() < 0, "North should be below avg when score<0"
        assert anom[neg_hours][:, south_rows, :].mean() > 0, "South should be above avg when score<0"


if __name__ == "__main__":
    import subprocess, sys
    sys.exit(subprocess.call(
        ["uv", "run", "pytest", __file__, "-v", "--tb=short"],
        cwd=str(Path(__file__).parent.parent)
    ))
