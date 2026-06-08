"""
Tests for the pure utility functions in analysis/eof_analysis.py.

These tests run without loading any real data files, using small synthetic
xarray DataArrays and NumPy arrays. Covered functions:
  - _subset_bundle
  - _orient_modes
  - _extract_grid_coords
  - _effective_sample_size
  - _channel_tag / _decomposition_blocks / all_block_keys / _parse_n_modes
  - bootstrap_eof_significance (lightweight smoke test, joint + per-channel)
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from analysis.eof_analysis import (
    _subset_bundle,
    _orient_modes,
    _extract_grid_coords,
    _effective_sample_size,
    _channel_tag,
    _decomposition_blocks,
    all_block_keys,
    _parse_n_modes,
    bootstrap_eof_significance,
    _FIELD_TO_BLOCK_KEY,
    _modes_tag,
    default_modes,
    DECOMPOSITIONS,
)
# Sanity-check: joint blocks were removed; only per-channel blocks exist.
assert "dayahead_joint" not in all_block_keys(), "joint blocks should have been removed"


# ── Helpers ──────────────────────────────────────────────────────────────────

_LAT_RANGE = (30.0, 35.0)    # Texas latitude extent shared across all synthetic fixtures
_LON_RANGE = (-100.0, -95.0) # Texas longitude extent


def _make_loading_da(n_modes=3, n_lat=6, n_lon=7, seed=0, curvilinear=False):
    """Synthetic loading DataArray with dims (mode, lat, lon) or (mode, y, x)."""
    lats_1d = np.linspace(*_LAT_RANGE, n_lat)
    lons_1d = np.linspace(*_LON_RANGE, n_lon)
    loading_data = np.random.default_rng(seed).normal(size=(n_modes, n_lat, n_lon))
    if not curvilinear:
        return xr.DataArray(
            loading_data, dims=["mode", "latitude", "longitude"],
            coords={"mode": np.arange(n_modes), "latitude": lats_1d, "longitude": lons_1d},
        )
    lons_2d, lats_2d = np.meshgrid(lons_1d, lats_1d)
    return xr.DataArray(
        loading_data, dims=["mode", "y", "x"],
        coords={"mode": np.arange(n_modes),
                "lat": (["y", "x"], lats_2d), "lon": (["y", "x"], lons_2d)},
    )


def _make_bundle(n_timesteps=120, n_lat=4, n_lon=5, seed=0):
    """Minimal synthetic bundle with two channel DataArrays."""
    random_gen = np.random.default_rng(seed)
    hours = pd.date_range("2025-01-01", periods=n_timesteps, freq="h")
    lats  = np.linspace(*_LAT_RANGE, n_lat)
    lons  = np.linspace(*_LON_RANGE, n_lon)

    def _make_channel_array(noise_scale=1.0):
        return xr.DataArray(
            random_gen.normal(scale=noise_scale, size=(n_timesteps, n_lat, n_lon)),
            dims=["valid_time", "latitude", "longitude"],
            coords={"valid_time": hours, "latitude": lats, "longitude": lons},
        )

    return {
        "channel_da": {"wspd100_error_0h": _make_channel_array(),
                       "temp_error_0h": _make_channel_array(2.0)},
        "hours":      hours,
        "extra_key":  "shared_by_reference",
    }


# ── _subset_bundle ───────────────────────────────────────────────────────────


class TestSubsetBundle:
    def test_season_hours_length(self):
        bundle = _make_bundle(n_timesteps=120)
        selection_mask = np.isin(bundle["hours"].month, [1])
        season_bundle = _subset_bundle(bundle, selection_mask)
        assert len(season_bundle["hours"]) == int(selection_mask.sum())

    def test_channel_da_time_length(self):
        bundle = _make_bundle(n_timesteps=120)
        selection_mask = np.isin(bundle["hours"].month, [1])
        season_bundle = _subset_bundle(bundle, selection_mask)
        for channel_array in season_bundle["channel_da"].values():
            assert channel_array.sizes["valid_time"] == len(season_bundle["hours"])

    def test_season_hours_in_bundle(self):
        bundle = _make_bundle(n_timesteps=120)
        selection_mask = np.zeros(len(bundle["hours"]), dtype=bool)
        selection_mask[:30] = True
        season_bundle = _subset_bundle(bundle, selection_mask)
        assert len(season_bundle["hours"]) == 30

    def test_extra_keys_shared_by_reference(self):
        bundle = _make_bundle(n_timesteps=120)
        selection_mask = np.ones(len(bundle["hours"]), dtype=bool)
        season_bundle = _subset_bundle(bundle, selection_mask)
        assert season_bundle["extra_key"] is bundle["extra_key"]

    def test_original_bundle_unchanged(self):
        bundle = _make_bundle(n_timesteps=120)
        original_len = len(bundle["hours"])
        selection_mask = np.zeros(len(bundle["hours"]), dtype=bool)
        selection_mask[:10] = True
        _subset_bundle(bundle, selection_mask)
        assert len(bundle["hours"]) == original_len


# ── _orient_modes ────────────────────────────────────────────────────────────


def _make_loadings_and_scores(n_modes=3, n_lat=5, n_lon=6, n_timesteps=80, seed=0):
    """Create two synthetic loading DataArrays (one per channel) and a scores DataFrame."""
    random_gen = np.random.default_rng(seed)
    hours = pd.date_range("2025-01-01", periods=n_timesteps, freq="h")
    scores_df = pd.DataFrame(
        random_gen.normal(size=(n_timesteps, n_modes)),
        index=hours,
        columns=[f"VMAX_PC{mode_index + 1}" for mode_index in range(n_modes)],
    )
    return [_make_loading_da(n_modes, n_lat, n_lon, seed),
            _make_loading_da(n_modes, n_lat, n_lon, seed + 1)], scores_df


class TestOrientModes:
    def test_does_not_modify_inputs(self):
        loadings, scores = _make_loadings_and_scores()
        original_loading_values = [loading_array.values.copy() for loading_array in loadings]
        original_scores = scores.values.copy()
        _orient_modes(loadings, scores)
        for loading_array, original in zip(loadings, original_loading_values):
            np.testing.assert_array_equal(loading_array.values, original)
        np.testing.assert_array_equal(scores.values, original_scores)

    def test_positive_net_loading_after_orientation(self):
        loadings, scores = _make_loadings_and_scores(seed=42)
        oriented_loadings, _ = _orient_modes(loadings, scores)
        for mode_index in range(oriented_loadings[0].sizes["mode"]):
            total = sum(
                float(np.nansum(loading_array.isel(mode=mode_index).values))
                for loading_array in oriented_loadings
            )
            assert total >= 0, (f"Mode {mode_index + 1} still has negative net loading "
                                 f"after orientation")

    def test_scores_flipped_consistently(self):
        """Modes whose loading is flipped must have their scores negated too."""
        loadings, scores = _make_loadings_and_scores(seed=7)
        oriented_loadings, oriented_scores = _orient_modes(loadings, scores)
        n_modes = oriented_loadings[0].sizes["mode"]
        for mode_index in range(n_modes):
            original_total = sum(
                float(np.nansum(loading_array.isel(mode=mode_index).values))
                for loading_array in loadings
            )
            original_score_col = scores.iloc[:, mode_index].values
            new_score_col = oriented_scores.iloc[:, mode_index].values
            if original_total < 0:
                np.testing.assert_array_almost_equal(new_score_col, -original_score_col)
            else:
                np.testing.assert_array_almost_equal(new_score_col, original_score_col)

    def test_already_positive_mode_unchanged(self):
        """A mode whose all-positive loading array sums positively must not be flipped."""
        loadings, scores = _make_loadings_and_scores(n_modes=2, seed=99)
        # Force all loading values strictly positive so no mode should be flipped.
        positive_loadings = [loading_array.copy(data=np.abs(loading_array.values) + 0.1)
                             for loading_array in loadings]
        _, oriented_scores = _orient_modes(positive_loadings, scores)
        np.testing.assert_array_equal(oriented_scores.iloc[:, 0].values,
                                      scores.iloc[:, 0].values)


# ── _extract_grid_coords ──────────────────────────────────────────────────────


class TestExtractGridCoords:
    def test_1d_grid_shapes(self):
        loading_array = _make_loading_da(n_lat=6, n_lon=7, seed=0)
        flat_lats, flat_lons, marker_size = _extract_grid_coords(loading_array)
        assert flat_lats.shape == (6 * 7,)
        assert flat_lons.shape == (6 * 7,)

    def test_2d_grid_shapes(self):
        loading_array = _make_loading_da(n_lat=6, n_lon=7, seed=1, curvilinear=True)
        flat_lats, flat_lons, marker_size = _extract_grid_coords(loading_array)
        assert flat_lats.shape == (6 * 7,)
        assert flat_lons.shape == (6 * 7,)

    def test_1d_and_2d_agree(self):
        """Meshgridding a 1-D grid should give the same flat coords as its 2-D equivalent."""
        n_lat, n_lon = 4, 5
        loading_1d = _make_loading_da(n_modes=2, n_lat=n_lat, n_lon=n_lon, seed=0)
        loading_2d = _make_loading_da(n_modes=2, n_lat=n_lat, n_lon=n_lon, seed=0,
                                      curvilinear=True)
        lats_regular, lons_regular, _ = _extract_grid_coords(loading_1d)
        lats_curvi, lons_curvi, _ = _extract_grid_coords(loading_2d)
        np.testing.assert_allclose(np.sort(lats_regular), np.sort(lats_curvi))
        np.testing.assert_allclose(np.sort(lons_regular), np.sort(lons_curvi))

    def test_marker_size_positive(self):
        _, _, marker_size = _extract_grid_coords(_make_loading_da())
        assert marker_size > 0


# ── _effective_sample_size ────────────────────────────────────────────────────


class TestEffectiveSampleSize:
    def test_white_noise_close_to_n(self):
        """i.i.d. white noise has n* ≈ n (within 20%)."""
        random_gen = np.random.default_rng(0)
        series_values = random_gen.normal(size=500)
        effective_n = _effective_sample_size(series_values)
        assert 400 <= effective_n <= 500

    def test_ar1_reduces_n_eff(self):
        """High-autocorrelation AR(1) should give n* << n."""
        random_gen = np.random.default_rng(1)
        ar1_coeff = 0.95
        series_values = np.zeros(500)
        series_values[0] = random_gen.normal()
        for time_step in range(1, 500):
            series_values[time_step] = (ar1_coeff * series_values[time_step - 1]
                                        + random_gen.normal(
                                            scale=np.sqrt(1 - ar1_coeff ** 2)))
        effective_n = _effective_sample_size(series_values)
        assert effective_n < 100, (f"Expected n* << 500 for ar1_coeff=0.95, "
                                    f"got {effective_n:.1f}")

    def test_constant_series_returns_n(self):
        """Zero-variance series (sum_squared_anomalies=0) should fall back to full n."""
        series_values = np.ones(50)
        assert _effective_sample_size(series_values) == pytest.approx(50.0)

    def test_short_series_returns_length(self):
        """Series shorter than 10 samples should return max(n, 1)."""
        assert _effective_sample_size(np.array([1.0, 2.0, 3.0])) == pytest.approx(3.0)
        assert _effective_sample_size(np.array([])) == pytest.approx(1.0)

    def test_result_bounded(self):
        """n* must always be in [1, n] for any input."""
        random_gen = np.random.default_rng(2)
        for _ in range(5):
            series_values = random_gen.normal(size=random_gen.integers(15, 200))
            effective_n = _effective_sample_size(series_values)
            assert 1.0 <= effective_n <= len(series_values) + 1e-9

    def test_nans_are_ignored(self):
        """NaN values must be stripped before the calculation."""
        series_values = np.array(
            [np.nan, 1.0, 2.0, 3.0, np.nan]
            + list(np.random.default_rng(3).normal(size=50))
        )
        effective_n = _effective_sample_size(series_values)
        n_finite = np.isfinite(series_values).sum()
        assert 1.0 <= effective_n <= n_finite + 1e-9


# ── Block enumeration + CLI parsing ───────────────────────────────────────────


class TestBlocks:
    def test_channel_tag(self):
        assert _channel_tag("wspd100_error_0h") == "wind"
        assert _channel_tag("era5_wspd100") == "wind"
        assert _channel_tag("temp_error_1h") == "temp"
        assert _channel_tag("era5_temp") == "temp"
        # Unrecognised names pass through unchanged.
        assert _channel_tag("foo") == "foo"

    def test_decomposition_blocks_per_channel_only(self):
        blocks = _decomposition_blocks("dayahead", ["wspd100_error_0h", "temp_error_0h"])
        # Per-channel only — no joint block.
        assert [block[1] for block in blocks] == ["wind", "temp"]
        # Each block carries exactly one field.
        assert blocks[0][2] == ["wspd100_error_0h"]
        assert blocks[1][2] == ["temp_error_0h"]
        # Keys are prefixed by the decomposition key.
        assert [block[0] for block in blocks] == ["dayahead_wind", "dayahead_temp"]

    def test_all_block_keys_unique(self):
        block_keys = all_block_keys()
        assert len(block_keys) == len(set(block_keys))   # globally unique
        assert "dayahead_wind" in block_keys and "realized_temp" in block_keys
        assert "dayahead_joint" not in block_keys

    def test_parse_n_modes(self):
        parsed_modes = _parse_n_modes(["dayahead_joint=4", "realized_temp=2"])
        assert parsed_modes == {"dayahead_joint": 4, "realized_temp": 2}

    def test_parse_n_modes_empty(self):
        assert _parse_n_modes(None) == {}
        assert _parse_n_modes([]) == {}

    def test_parse_n_modes_rejects_malformed(self):
        import argparse
        with pytest.raises(argparse.ArgumentTypeError):
            _parse_n_modes(["dayahead_joint"])


# ── bootstrap_eof_significance ────────────────────────────────────────────────


class TestBootstrapEofSignificance:
    """Smoke tests — verify the function runs and returns a well-formed dict."""

    @pytest.fixture(scope="class")
    def sig_result(self):
        bundle = _make_bundle(n_timesteps=80, n_lat=4, n_lon=5)
        channel_fields = ["wspd100_error_0h", "temp_error_0h"]
        return bootstrap_eof_significance(bundle, channel_fields,
                                          n_scree_modes=4, n_boot=8, seed=0)

    def test_single_channel_runs(self):
        """A one-field (per-channel) block must also yield a well-formed spectrum."""
        bundle = _make_bundle(n_timesteps=80, n_lat=4, n_lon=5)
        sig_result = bootstrap_eof_significance(bundle, ["temp_error_0h"],
                                                n_scree_modes=3, n_boot=8, seed=0)
        assert sig_result["n"] == 80
        for array_key in ("evr", "bs_lo", "bs_hi", "north_lo", "north_hi"):
            assert len(sig_result[array_key]) == len(sig_result["modes"])

    def test_returns_required_keys(self, sig_result):
        required_keys = {"modes", "evr", "bs_lo", "bs_hi", "north_lo", "north_hi",
                         "n", "n_eff"}
        assert required_keys.issubset(sig_result.keys())

    def test_modes_consecutive_from_one(self, sig_result):
        np.testing.assert_array_equal(sig_result["modes"],
                                       np.arange(1, len(sig_result["modes"]) + 1))

    def test_all_arrays_same_length(self, sig_result):
        n_modes = len(sig_result["modes"])
        for array_key in ("evr", "bs_lo", "bs_hi", "north_lo", "north_hi"):
            assert len(sig_result[array_key]) == n_modes, (
                f"len({array_key}) != {n_modes}"
            )

    def test_evr_sums_to_at_most_100(self, sig_result):
        assert sig_result["evr"].sum() <= 100.1

    def test_north_band_brackets_evr(self, sig_result):
        """North's interval must bracket the point estimate by construction."""
        np.testing.assert_array_less(sig_result["north_lo"], sig_result["evr"] + 1e-9)
        np.testing.assert_array_less(sig_result["evr"] - 1e-9, sig_result["north_hi"])

    def test_n_eff_at_most_n(self, sig_result):
        assert sig_result["n_eff"] <= sig_result["n"] + 1e-9

    def test_n_matches_bundle_size(self, sig_result):
        assert sig_result["n"] == 80


# ── Modes: fit / project / save / load ────────────────────────────────────────


def _bundle_with_masks(n_timesteps=480, n_lat=4, n_lon=5, seed=0):
    """Synthetic bundle augmented with the all-land nan_all masks fit_eof_modes needs."""
    bundle = _make_bundle(n_timesteps, n_lat, n_lon, seed)
    bundle["nan_all"] = {field: np.zeros((n_lat, n_lon), dtype=bool)
                         for field in bundle["channel_da"]}
    return bundle


class TestModesProjection:
    """The core guarantee: projecting saved modes reproduces the EOF fit scores."""

    def test_projection_reproduces_fit_scores(self):
        from analysis.eof_analysis import fit_eof_modes, project_onto_modes
        from analysis.eof_methods import fit_varimax

        bundle = _bundle_with_masks()
        fields = list(bundle["channel_da"])
        modes, _ = fit_eof_modes(bundle, fields, K=2, seed=0)
        projected = project_onto_modes(bundle, modes)

        # fit_eof_modes fits on all hours (no train/test split), so reproduce the
        # fit scores by fitting fit_varimax over every hour as well.
        hours = bundle["hours"]
        all_idx = np.arange(len(hours))
        for field in fields:
            fit_scores = fit_varimax(
                bundle, all_idx, hours, K=2, error_fields=[field], seed=0
            ).scores.values
            for k in range(2):
                corr = abs(np.corrcoef(fit_scores[:, k], projected[field][:, k])[0, 1])
                assert corr > 0.999, f"{field} mode {k+1}: |corr|={corr:.4f}"

    def test_components_shape_matches_cells(self):
        from analysis.eof_analysis import fit_eof_modes

        bundle = _bundle_with_masks(n_lat=4, n_lon=5)
        modes, _ = fit_eof_modes(bundle, list(bundle["channel_da"]), K=2, seed=0)
        for field, m in modes.items():
            assert m["components"].shape == (2, 4 * 5)
            assert m["field_mean"].shape == (4 * 5,)
            assert m["lat"].shape == (4 * 5,)


class TestModesRoundTrip:
    def test_save_load_roundtrip(self, tmp_path):
        from analysis.eof_analysis import (
            fit_eof_modes, save_eof_modes, load_eof_modes,
        )

        bundle = _bundle_with_masks()
        fields = list(bundle["channel_da"])
        # K must be a dict (the normalised form); use a minimal two-key dict matching
        # the two fields in the synthetic bundle.
        K_dict = {"wspd100_error_0h": 2, "temp_error_0h": 2}
        modes, var_df = fit_eof_modes(bundle, fields, K=K_dict, seed=0)
        save_eof_modes(modes, var_df, [(2025, 1), (2025, 2)], K_dict, tmp_path)

        loaded, loaded_var_df, months = load_eof_modes(K_dict, tmp_path)
        assert months == [(2025, 1), (2025, 2)]
        assert set(loaded) == set(modes)
        for field in fields:
            for key in ("components", "evr", "lat", "lon", "field_mean"):
                np.testing.assert_allclose(loaded[field][key], modes[field][key])

    def test_int_k_normalised_by_fit_eof_modes(self):
        """Passing a scalar K to fit_eof_modes should normalise to dict internally."""
        from analysis.eof_analysis import fit_eof_modes

        bundle = _bundle_with_masks()
        fields = list(bundle["channel_da"])
        # K=2 (int) — entry-point normalises to {bk: 2 for bk in default_modes}.
        # fit_eof_modes must succeed and produce 2 modes per field.
        modes, _ = fit_eof_modes(bundle, fields, K=2, seed=0)
        for m in modes.values():
            assert m["components"].shape[0] == 2


# ── _extract_grid_coords — coord-resolution regression ───────────────────────


class TestExtractGridCoordsCoordNames:
    """Regression tests for the 'latitude' DataArray truth-value bug (GH fix)."""

    def test_latitude_longitude_coord_names(self):
        """ERA5-style: coords named 'latitude'/'longitude' — must not raise ValueError."""
        n_lat, n_lon = 5, 7
        lats = np.linspace(30.0, 35.0, n_lat)
        lons = np.linspace(-100.0, -95.0, n_lon)
        da = xr.DataArray(
            np.zeros((3, n_lat, n_lon)),
            dims=["mode", "latitude", "longitude"],
            coords={"mode": [0, 1, 2], "latitude": lats, "longitude": lons},
        )
        # This was the failing case: coords.get("latitude") returns a multi-element
        # DataArray, and old code tried `bool(DataArray)` via `or`, raising ValueError.
        flat_lats, flat_lons, msz = _extract_grid_coords(da)
        assert flat_lats.shape == (n_lat * n_lon,)
        assert flat_lons.shape == (n_lat * n_lon,)

    def test_lat_lon_coord_names(self):
        """HRRR-style curvilinear: coords named 'lat'/'lon' — must resolve correctly."""
        da = _make_loading_da(n_lat=4, n_lon=5, curvilinear=True)
        flat_lats, flat_lons, msz = _extract_grid_coords(da)
        assert flat_lats.shape == (4 * 5,)
        assert flat_lons.shape == (4 * 5,)

    def test_fallback_to_dims(self):
        """When no lat/lon named coords exist, falls back to last two dims."""
        n_lat, n_lon = 3, 4
        lats = np.linspace(30.0, 35.0, n_lat)
        lons = np.linspace(-100.0, -95.0, n_lon)
        da = xr.DataArray(
            np.zeros((2, n_lat, n_lon)),
            dims=["mode", "y", "x"],
            coords={"mode": [0, 1], "y": lats, "x": lons},
        )
        flat_lats, flat_lons, msz = _extract_grid_coords(da)
        assert flat_lats.shape == (n_lat * n_lon,)
        assert flat_lons.shape == (n_lat * n_lon,)


# ── default_modes structure ───────────────────────────────────────────────────


class TestDefaultModes:
    def test_is_dict(self):
        assert isinstance(default_modes, dict)

    def test_has_six_blocks(self):
        assert len(default_modes) == 6

    def test_block_keys_match_all_block_keys(self):
        assert set(default_modes.keys()) == set(all_block_keys())

    def test_all_values_positive_ints(self):
        for block_key, n_modes in default_modes.items():
            assert isinstance(n_modes, int) and n_modes > 0, (
                f"{block_key}: expected positive int, got {n_modes!r}"
            )


# ── _FIELD_TO_BLOCK_KEY ───────────────────────────────────────────────────────


class TestFieldToBlockKey:
    def test_covers_all_decomposition_fields(self):
        all_fields = [f for _, (fields, _) in DECOMPOSITIONS.items() for f in fields]
        assert set(_FIELD_TO_BLOCK_KEY.keys()) == set(all_fields)

    def test_known_mappings(self):
        assert _FIELD_TO_BLOCK_KEY["wspd100_error_0h"] == "dayahead_wind"
        assert _FIELD_TO_BLOCK_KEY["temp_error_0h"]    == "dayahead_temp"
        assert _FIELD_TO_BLOCK_KEY["wspd100_error_1h"] == "hourahead_wind"
        assert _FIELD_TO_BLOCK_KEY["temp_error_1h"]    == "hourahead_temp"
        assert _FIELD_TO_BLOCK_KEY["era5_wspd100"]     == "realized_wind"
        assert _FIELD_TO_BLOCK_KEY["era5_temp"]        == "realized_temp"

    def test_all_values_are_block_keys(self):
        valid_block_keys = set(all_block_keys())
        for field, block_key in _FIELD_TO_BLOCK_KEY.items():
            assert block_key in valid_block_keys, (
                f"Field {field!r} maps to unknown block key {block_key!r}"
            )


# ── _modes_tag ────────────────────────────────────────────────────────────────


class TestModesTag:
    def test_uniform_dict_produces_stable_tag(self):
        K = {"dayahead_wind": 3, "dayahead_temp": 3,
             "hourahead_wind": 3, "hourahead_temp": 2,
             "realized_wind": 2, "realized_temp": 2}
        tag = _modes_tag(K)
        assert isinstance(tag, str)
        assert len(tag) > 0

    def test_tag_deterministic_regardless_of_insertion_order(self):
        K1 = {"dayahead_wind": 3, "realized_temp": 2, "dayahead_temp": 3,
              "hourahead_wind": 3, "hourahead_temp": 2, "realized_wind": 2}
        K2 = {"realized_wind": 2, "hourahead_temp": 2, "dayahead_temp": 3,
              "dayahead_wind": 3, "hourahead_wind": 3, "realized_temp": 2}
        assert _modes_tag(K1) == _modes_tag(K2)

    def test_different_values_produce_different_tags(self):
        K_a = {"a": 3, "b": 2}
        K_b = {"a": 2, "b": 3}
        assert _modes_tag(K_a) != _modes_tag(K_b)

    def test_values_appear_in_tag(self):
        K = {"dayahead_wind": 7, "dayahead_temp": 7,
             "hourahead_wind": 7, "hourahead_temp": 7,
             "realized_wind": 7, "realized_temp": 7}
        tag = _modes_tag(K)
        assert "7" in tag


# ── K normalisation at entry points ──────────────────────────────────────────


class TestKNormalisation:
    """fit_eof_modes (and phase functions) must accept a scalar K and normalise it."""

    def test_scalar_k_produces_uniform_modes(self):
        """Scalar K=2 must yield 2 modes per field, same as a full block dict with 2."""
        from analysis.eof_analysis import fit_eof_modes

        bundle = _bundle_with_masks(n_lat=4, n_lon=5)
        fields = list(bundle["channel_da"])
        modes_from_int, _ = fit_eof_modes(bundle, fields, K=2, seed=0)
        # dict must use block keys (not field names) — int normalises to this form.
        modes_from_dict, _ = fit_eof_modes(
            bundle, fields,
            K={bk: 2 for bk in default_modes},
            seed=0,
        )
        for field in fields:
            assert modes_from_int[field]["components"].shape == \
                   modes_from_dict[field]["components"].shape

    def test_per_block_k_honoured(self):
        """A per-block dict with different values must produce different mode counts."""
        from analysis.eof_analysis import fit_eof_modes

        bundle = _bundle_with_masks(n_lat=4, n_lon=5)
        fields = list(bundle["channel_da"])
        # The test bundle has wspd100_error_0h (→ dayahead_wind) and
        # temp_error_0h (→ dayahead_temp); use block keys, not field names.
        K_mixed = {**default_modes, "dayahead_wind": 3, "dayahead_temp": 2}
        modes, _ = fit_eof_modes(bundle, fields, K=K_mixed, seed=0)
        assert modes["wspd100_error_0h"]["components"].shape[0] == 3
        assert modes["temp_error_0h"]["components"].shape[0] == 2


if __name__ == "__main__":
    import subprocess
    sys.exit(subprocess.call(
        ["uv", "run", "pytest", __file__, "-v", "--tb=short"],
        cwd=str(Path(__file__).parent.parent),
    ))
