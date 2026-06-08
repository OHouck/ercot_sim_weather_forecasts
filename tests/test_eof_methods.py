"""
Tests for analysis/eof_methods.py and analysis/eof_method_comparison.py.

Runs on small synthetic data with a known latent factor that drives both the
error fields and the outcomes, so every method should be able to recover
predictive structure. The synthetic error fields include time-invariant NaN
(masked) cells to mirror the real ERCOT pipeline, where grid cells outside the
ERCOT service area are NaN for all hours.

The headline correctness check is test_eeof_projection_matches_xeofs: our manual
out-of-sample projection for Extended EOF must reproduce xeofs's own in-sample
scores (xeofs provides no transform() for EEOF), since the whole train/test
protocol for that method rests on it.
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from analysis.pca_decomposition import ERROR_FIELDS
from analysis import eof_methods
from analysis.eof_methods import (
    fit_eof_perchannel, fit_eof_joint, fit_varimax, fit_sparse_joint,
    fit_eeof_perchannel, fit_mca, _project_eeof, MethodResult,
)
from analysis.eof_method_comparison import (
    damage_skill, score_stability, _matched_abs_corr, _train_halves, _diag_summary,
    build_methods,
)
from analysis.pca_mode_analysis import DEPVAR_CONFIGS

T       = 400
GRID    = (6, 6)
K       = 3
SEED    = 0
PRIMARY = "economic_congestion_cost"


# ── Synthetic fixtures ───────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def synthetic():
    """Build a synthetic bundle + outcomes with a shared latent factor.

    A single latent series z drives all four error channels (different spatial
    patterns) and the outcomes, so EOF/MCA can recover predictive modes. Three
    masked NaN cells per field exercise the real-pipeline NaN handling.
    """
    rng = np.random.default_rng(SEED)
    n_lat, n_lon = GRID
    hours = pd.date_range("2025-01-01", periods=T, freq="h")
    lats  = np.linspace(30.0, 35.0, n_lat)
    lons  = np.linspace(-100.0, -95.0, n_lon)

    z = rng.normal(size=T)                       # shared latent factor
    z_lag = np.concatenate([[0.0], z[:-1]])      # one-hour lag (for EEOF structure)

    ll, lo = np.meshgrid(lats, lons, indexing="ij")
    base_pattern = np.exp(-((ll - 32.5) ** 2 + (lo + 97.5) ** 2) / 4.0)

    channel_da, nan_all = {}, {}
    for i, fld in enumerate(ERROR_FIELDS):
        pattern = base_pattern * (1.0 + 0.3 * i) + 0.1 * rng.normal(size=GRID)
        field = (z[:, None, None] + 0.4 * z_lag[:, None, None]) * pattern[None] \
            + 0.25 * rng.normal(size=(T, n_lat, n_lon))
        da = xr.DataArray(
            field, dims=["valid_time", "latitude", "longitude"],
            coords={"valid_time": hours, "latitude": lats, "longitude": lons},
        )
        # mask 3 cells as NaN for all hours (mimics outside-ERCOT cells)
        mask = np.zeros(GRID, dtype=bool)
        mask[0, 0] = mask[1, 2] = mask[-1, -1] = True
        da = da.where(~xr.DataArray(mask, dims=["latitude", "longitude"],
                                    coords={"latitude": lats, "longitude": lons}))
        channel_da[fld] = da
        nan_all[fld]    = mask

    bundle = {"channel_da": channel_da, "hours": hours, "nan_all": nan_all}

    # outcomes: monotone-ish functions of z so a damage function exists
    outcomes = pd.DataFrame(index=hours)
    outcomes["economic_congestion_cost"]       = np.expm1(0.8 * z + 0.3 * z ** 2 + 1.0) \
        + rng.normal(scale=0.5, size=T)
    outcomes["total_renewable_curtailment_mw"] = np.clip(50 * z + 100 + 10 * rng.normal(size=T), 0, None)
    outcomes["avg_intensity_kg_per_mwh"]       = 400 + 20 * z + 5 * rng.normal(size=T)
    outcomes = outcomes.clip(lower=0)

    train_mask = np.zeros(T, dtype=bool)
    train_mask[: int(0.7 * T)] = True            # first 70% train, last 30% test
    test_mask = ~train_mask
    train_idx = np.where(train_mask)[0]

    return dict(bundle=bundle, hours=hours, outcomes=outcomes,
                train_mask=train_mask, test_mask=test_mask, train_idx=train_idx, z=z)


def _builders(synthetic):
    """Return {key: (builder, kwargs)} for all six methods on the synthetic data."""
    common = dict(K=K, error_fields=ERROR_FIELDS, seed=SEED)
    mca_kw = dict(common, outcomes_df=synthetic["outcomes"],
                  mca_outcomes=eof_methods.MCA_OUTCOMES, depvar_configs=DEPVAR_CONFIGS)
    return {
        "eof_perchannel":  (fit_eof_perchannel,  common),
        "eof_joint":       (fit_eof_joint,       common),
        "varimax_joint":   (fit_varimax,   common),
        "sparse_joint":    (fit_sparse_joint,    common),
        "eeof_perchannel": (fit_eeof_perchannel, common),
        "mca":             (fit_mca,             mca_kw),
    }


@pytest.fixture(scope="module")
def fitted(synthetic):
    """Fit every method once on the training split."""
    out = {}
    for key, (builder, kwargs) in _builders(synthetic).items():
        out[key] = builder(synthetic["bundle"], synthetic["train_idx"],
                           synthetic["hours"], **kwargs)
    return out


# ── 1. Each method runs and returns sane scores ──────────────────────────────


class TestMethodsRun:
    def test_all_return_method_result(self, fitted):
        for key, res in fitted.items():
            assert isinstance(res, MethodResult), f"{key} did not return MethodResult"

    def test_scores_indexed_by_hours(self, fitted, synthetic):
        for key, res in fitted.items():
            assert list(res.scores.index) == list(synthetic["hours"]), \
                f"{key} scores not aligned to hours"

    def test_predictor_counts(self, fitted):
        # per-channel methods → K per channel; joint/MCA → ~K total
        assert fitted["eof_perchannel"].scores.shape[1] == K * len(ERROR_FIELDS)
        assert fitted["eeof_perchannel"].scores.shape[1] == K * len(ERROR_FIELDS)
        assert fitted["eof_joint"].scores.shape[1] == K
        assert fitted["varimax_joint"].scores.shape[1] == K
        assert fitted["sparse_joint"].scores.shape[1] == K
        assert fitted["mca"].scores.shape[1] == min(K, len(eof_methods.MCA_OUTCOMES))

    def test_scores_have_signal(self, fitted):
        """Each method must produce at least one varying (informative) score column.

        Higher sparse-EOF modes may legitimately collapse to exact zero when the
        underlying signal is low-rank, so we require the leading mode to carry
        signal rather than every mode.
        """
        for key, res in fitted.items():
            sc = res.scores.dropna(how="all")
            assert len(sc) > 0, f"{key} all-NaN scores"
            stds = sc.std().values
            stds = stds[~np.isnan(stds)]
            assert stds.size and stds.max() > 1e-6, f"{key} has no varying score column"

    def test_oos_scores_present_on_test(self, fitted, synthetic):
        """Test-hour scores must be populated (EEOF may drop only its short tail)."""
        test_mask = synthetic["test_mask"]
        for key, res in fitted.items():
            te = res.scores.loc[test_mask]
            frac_valid = te.notna().all(axis=1).mean()
            assert frac_valid > 0.9, f"{key}: only {frac_valid:.2f} of test hours have scores"

    def test_mca_supervised_flag(self, fitted):
        assert fitted["mca"].supervised is True
        for key in ("eof_perchannel", "eof_joint", "varimax_joint", "sparse_joint", "eeof_perchannel"):
            assert fitted[key].supervised is False


# ── 2. EEOF manual projection correctness ────────────────────────────────────


class TestEEOFProjection:
    def test_eeof_projection_matches_xeofs(self, synthetic):
        """Manual OOS projection must reproduce xeofs in-sample EEOF scores."""
        from xeofs.single import ExtendedEOF

        da   = synthetic["bundle"]["channel_da"][ERROR_FIELDS[0]]
        emb, tau, n_modes = 5, 1, 3
        all_idx = np.arange(len(synthetic["hours"]))

        model = ExtendedEOF(n_modes=n_modes, tau=tau, embedding=emb,
                            center=True, random_state=SEED)
        model.fit(da, dim="valid_time")
        xeofs_scores = model.scores().transpose("valid_time", "mode").values

        manual = _project_eeof(da, model.components(), all_idx, emb, tau, prefix="f")
        n_win  = manual.shape[0]

        for k in range(n_modes):
            # xeofs labels scores at window start-time; compare the first n_win
            xs = xeofs_scores[:n_win, k]
            ms = manual.iloc[:, k].values
            valid = ~np.isnan(xs)
            corr = np.corrcoef(ms[valid], xs[valid])[0, 1]
            assert abs(corr) > 0.999, f"EEOF mode {k}: projection corr {corr:.4f} != 1"

    def test_eeof_tail_is_nan(self, fitted, synthetic):
        """The final (embedding-1)*tau hours have no full window → NaN tail."""
        res = fitted["eeof_perchannel"]
        emb, tau = res.diagnostics["embedding"], res.diagnostics["tau"]
        tail = (emb - 1) * tau
        assert res.scores.iloc[-tail:].isna().all().all(), "EEOF tail should be NaN"
        assert res.scores.iloc[: -tail].notna().any().any(), "EEOF body should be populated"


# ── 3. Diagnostics ───────────────────────────────────────────────────────────


class TestDiagnostics:
    def test_var_explained_in_range(self, fitted):
        for key in ("eof_perchannel", "eof_joint", "varimax_joint", "sparse_joint", "eeof_perchannel"):
            s = _diag_summary(fitted[key])
            assert 0.0 < s["var_cov_explained"] <= 1.0001, f"{key} var/cov out of range"

    def test_mca_reports_outcome_variance(self, fitted):
        s = _diag_summary(fitted["mca"])
        assert 0.0 <= s["frac_var_Y_explained"] <= 1.0001
        assert 0.0 <= s["var_cov_explained"] <= 1.0001

    def test_sparse_loadings_are_sparser(self, fitted):
        """Sparse EOF should have more exact-zero loadings than plain joint EOF."""
        def zero_frac(res):
            comps = res.diagnostics["loadings_list"]
            vals = np.concatenate([np.nan_to_num(c.values).ravel() for c in comps])
            return np.mean(np.abs(vals) < 1e-8)
        assert zero_frac(fitted["sparse_joint"]) >= zero_frac(fitted["eof_joint"])


# ── 4. Damage skill + harness utilities ──────────────────────────────────────


class TestDamageSkill:
    def test_skill_finite_and_predictive(self, fitted, synthetic):
        """With a real latent signal, methods should achieve positive test R²."""
        for key, res in fitted.items():
            sk = damage_skill(res.scores, synthetic["outcomes"][PRIMARY],
                              "log1p", synthetic["hours"],
                              synthetic["train_mask"], synthetic["test_mask"])
            assert np.isfinite(sk["test_r2"]), f"{key} test R² not finite"
            assert sk["test_r2"] > 0.05, f"{key} test R²={sk['test_r2']:.3f} unexpectedly low"

    def test_n_predictors_reported(self, fitted, synthetic):
        sk = damage_skill(fitted["eof_joint"].scores, synthetic["outcomes"][PRIMARY],
                          "log1p", synthetic["hours"],
                          synthetic["train_mask"], synthetic["test_mask"])
        assert sk["n_predictors"] == K


class TestHarnessUtils:
    def test_matched_abs_corr_identity(self):
        rng = np.random.default_rng(1)
        A = rng.normal(size=(200, 3))
        assert _matched_abs_corr(A, A) == pytest.approx(1.0, abs=1e-6)

    def test_matched_abs_corr_permutation_invariant(self):
        rng = np.random.default_rng(2)
        A = rng.normal(size=(200, 3))
        B = A[:, ::-1]                      # reversed column order
        assert _matched_abs_corr(A, B) == pytest.approx(1.0, abs=1e-6)

    def test_train_halves_disjoint_and_in_train(self, synthetic):
        idx_a, idx_b = _train_halves(synthetic["hours"], synthetic["train_mask"])
        assert set(idx_a).isdisjoint(set(idx_b)), "halves overlap"
        train_pos = set(np.where(synthetic["train_mask"])[0])
        assert set(idx_a) | set(idx_b) <= train_pos, "halves leak outside train"

    def test_stability_in_unit_range(self, synthetic):
        builder, kwargs = _builders(synthetic)["eof_joint"]
        stab = score_stability(builder, kwargs, synthetic["bundle"], synthetic["hours"],
                               synthetic["train_mask"], synthetic["test_mask"])
        assert 0.0 <= stab <= 1.0001, f"stability {stab} out of range"


# ── 5. Mini end-to-end ───────────────────────────────────────────────────────


class TestEndToEnd:
    def test_build_methods_runs(self, synthetic):
        results, specs = build_methods(
            synthetic["bundle"], synthetic["train_idx"], synthetic["hours"],
            K, synthetic["outcomes"], seed=SEED,
        )
        assert len(results) == 6
        assert len(specs) == 6
        assert all(isinstance(r, MethodResult) for r in results)


if __name__ == "__main__":
    import subprocess
    sys.exit(subprocess.call(
        ["uv", "run", "pytest", __file__, "-v", "--tb=short"],
        cwd=str(Path(__file__).parent.parent),
    ))
