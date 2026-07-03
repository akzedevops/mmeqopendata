"""Regression tests for the ASK08 GMPE and hazard helpers in dam_risk.

These guard the v2 fixes for the broken large-distance coefficient (a18) and the
un-annualized return period, both of which passed the original test suite because
nothing exercised far-field PGA or the rate conversion. Spec 003 P4/P5 added
hazard-curve, fragility, config-sourced scoring, and report smoke tests.
"""
import argparse
import math
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from mmeq.analysis.dam_risk import (
    _grade,
    compute_hazard_curve,
    compute_return_period,
    estimate_pga_ask08,
)
from mmeq.config import RISK_GRADE_THRESHOLDS, RISK_WEIGHTS

REPO_ROOT = Path(__file__).resolve().parents[1]


class TestGmpeFarField:
    def test_pga_decreases_monotonically_with_distance(self):
        # The large-distance term (rrup >= 100 km) must not introduce a cliff.
        dists = [5, 30, 80, 100, 120, 200, 300, 475]
        pgas = [estimate_pga_ask08(7.7, r, vs30=760) for r in dists]
        for near, far in zip(pgas, pgas[1:]):
            assert near >= far, "PGA must not increase with distance"

    def test_pga_continuous_across_100km_boundary(self):
        # A wrong a18 (~-0.39) collapses PGA from ~0.06 g to ~0.001 g here.
        below = estimate_pga_ask08(7.7, 99.0, vs30=760)
        above = estimate_pga_ask08(7.7, 101.0, vs30=760)
        assert above == pytest.approx(below, rel=0.1)

    def test_far_field_pga_is_physically_reasonable(self):
        # At 200 km from a M7.7, PGA should be ~0.01-0.05 g, not the 0.0001 floor.
        pga_200 = estimate_pga_ask08(7.7, 200.0, vs30=760)
        assert 0.005 < pga_200 < 0.1
        # And at the far end of the 475 km rupture, still well above the floor.
        pga_475 = estimate_pga_ask08(7.7, 475.0, vs30=760)
        assert pga_475 > 0.001

    def test_near_field_unchanged(self):
        # The published validation point (~near-fault) must stay in range.
        pga_near = estimate_pga_ask08(7.7, 5.0, vs30=760)
        assert 0.2 < pga_near < 0.8


class TestReturnPeriod:
    def test_return_period_is_annualized(self):
        # a_value is a catalog-level intercept; the return period must divide by
        # the annual rate, i.e. scale with catalog_years.
        b, a = 1.0, 5.0
        rp_short = compute_return_period(6.0, b, a, catalog_years=10)
        rp_long = compute_return_period(6.0, b, a, catalog_years=70)
        # Longer catalog -> lower annual rate -> longer return period.
        assert rp_long > rp_short
        # Explicit value: a_annual = a - log10(years); n/yr = 10^(a_annual - b*6).
        expected = 1.0 / (10 ** ((a - math.log10(70)) - b * 6.0))
        assert rp_long == pytest.approx(expected, rel=1e-6)

    def test_zero_catalog_years_is_safe(self):
        assert compute_return_period(6.0, 1.0, 5.0, catalog_years=0) == math.inf


class TestHazardCurve:
    def test_exceedance_rate_monotonic_decreasing_in_pga(self):
        hc = compute_hazard_curve(rjb_km=20.0, vs30=760.0, b_val=1.0, a_val=6.0, mc=4.0)
        rates = hc["annual_rate"].to_numpy()
        assert rates[0] > 0
        # Exceedance rate must never increase with the PGA level.
        assert (np.diff(rates) <= 1e-15).all()
        # And it must actually decay over the full range.
        assert rates[-1] < rates[0]

    def test_catalog_years_scaling(self):
        # a_val is a catalog-level intercept; annualizing divides by catalog
        # years, so doubling the catalog length must halve every rate exactly.
        hc1 = compute_hazard_curve(20.0, 760.0, 1.0, 6.0, 4.0, catalog_years=28.0)
        hc2 = compute_hazard_curve(20.0, 760.0, 1.0, 6.0, 4.0, catalog_years=56.0)
        np.testing.assert_allclose(
            hc2["annual_rate"], hc1["annual_rate"] / 2.0, rtol=1e-9
        )

    def test_return_period_is_inverse_of_rate(self):
        hc = compute_hazard_curve(20.0, 760.0, 1.0, 6.0, 4.0)
        pos = hc[hc["annual_rate"] > 0]
        np.testing.assert_allclose(
            pos["return_period_yr"], 1.0 / pos["annual_rate"], rtol=1e-12
        )


class TestFragility:
    def test_exceedance_decreasing_in_damage_state(self):
        from mmeq.analysis.fragility import FRAGILITY_PARAMS, fragility_probability

        for dam_type in FRAGILITY_PARAMS:
            for pga in [0.05, 0.2, 0.5, 1.0, 1.5]:
                p = fragility_probability(pga, dam_type)
                assert p["DS1"] >= p["DS2"] >= p["DS3"] >= p["DS4"], (
                    f"P(DS>=i) must be non-increasing in i ({dam_type}, {pga}g)"
                )

    def test_lognormal_median_crossing_is_half(self):
        # At PGA equal to the fragility median, P(DS>=i) = Phi(0) = 0.5.
        from mmeq.analysis.fragility import FRAGILITY_PARAMS, fragility_probability

        for dam_type, params in FRAGILITY_PARAMS.items():
            for ds, pr in params.items():
                probs = fragility_probability(pr["median"], dam_type)
                assert probs[ds] == pytest.approx(0.5, abs=1e-12)

    def test_discrete_damage_states_sum_to_one(self):
        from mmeq.analysis.fragility import damage_state_probabilities

        for pga in [0.01, 0.2, 0.6, 1.5]:
            p = damage_state_probabilities(pga, "earthfill")
            assert sum(p.values()) == pytest.approx(1.0, abs=1e-9)

    def test_zero_pga_no_damage(self):
        from mmeq.analysis.fragility import fragility_probability

        assert fragility_probability(0.0) == {
            "DS1": 0.0, "DS2": 0.0, "DS3": 0.0, "DS4": 0.0
        }


class TestConfigSourcedScoring:
    """Spec 003 P5: weights/thresholds come from config, not inline literals."""

    def test_weights_sum_to_one(self):
        assert sum(RISK_WEIGHTS.values()) == pytest.approx(1.0)

    def test_published_defaults_unchanged(self):
        assert RISK_WEIGHTS == {
            "seismic": 0.35, "fault": 0.30, "proximity": 0.20, "exposure": 0.15
        }
        assert RISK_GRADE_THRESHOLDS == {"critical": 7.0, "high": 5.0, "moderate": 3.0}

    def test_grade_boundaries(self):
        assert _grade(RISK_GRADE_THRESHOLDS["critical"]) == "Critical"
        assert _grade(RISK_GRADE_THRESHOLDS["critical"] - 0.01) == "High"
        assert _grade(RISK_GRADE_THRESHOLDS["high"]) == "High"
        assert _grade(RISK_GRADE_THRESHOLDS["high"] - 0.01) == "Moderate"
        assert _grade(RISK_GRADE_THRESHOLDS["moderate"]) == "Moderate"
        assert _grade(RISK_GRADE_THRESHOLDS["moderate"] - 0.01) == "Low"


def _synthetic_catalog(n: int = 60) -> pd.DataFrame:
    """Tiny plausible Myanmar catalog with one dominant M7.7 event."""
    rng = np.random.RandomState(0)
    df = pd.DataFrame({
        "time_utc": pd.date_range("2015-01-01", periods=n, freq="30D"),
        "mag": np.round(np.clip(3.0 + rng.exponential(0.8, n), 3.0, 7.0), 1),
        "depth": np.round(rng.uniform(5.0, 40.0, n), 1),
        "latitude": np.round(rng.uniform(16.0, 26.0, n), 3),
        "longitude": np.round(rng.uniform(92.0, 100.0, n), 3),
        "location": "Synthetic, Myanmar",
    })
    # Dominant scenario event (drives dam_risk_scores' major_eq selection).
    df.loc[0, ["mag", "depth", "latitude", "longitude"]] = [7.7, 10.0, 22.0, 96.0]
    return df


def _report_args(catalog: Path, output: Path, **overrides) -> argparse.Namespace:
    """cmd_report args with every stage disabled except dam risk."""
    ns = argparse.Namespace(
        data=str(catalog),
        output=str(output),
        min_mag=0.0,
        mc=None,
        reuse_risk=False,
        no_dams=False,
        no_forecast=True,
        no_dashboard=True,
        no_3d=True,
        no_animated=True,
        no_population=True,
        no_buildings=True,
        no_pdf=True,
        no_clusters=True,
        no_coulomb=True,
        no_fragility=True,
        no_montecarlo=True,
    )
    for k, v in overrides.items():
        setattr(ns, k, v)
    return ns


class TestReportSmoke:
    @pytest.mark.skipif(
        not (REPO_ROOT / "myanmar_dams.geojson").exists(),
        reason="myanmar_dams.geojson not available",
    )
    def test_report_dam_risk_only(self, tmp_path, monkeypatch):
        from mmeq.cli import cmd_report

        # Dam/fault inputs are resolved from the CWD.
        monkeypatch.chdir(REPO_ROOT)
        # Do not pick up a stale report/dam_vs30.csv from a dev checkout.
        monkeypatch.setenv("MMEQ_REPORT_DIR", str(tmp_path / "empty"))

        catalog = tmp_path / "catalog.csv"
        _synthetic_catalog().to_csv(catalog, index=False)
        out = tmp_path / "out"

        cmd_report(_report_args(catalog, out))

        risk_csv = out / "dam_risk_scores.csv"
        assert risk_csv.exists(), "report must produce dam_risk_scores.csv"
        risk = pd.read_csv(risk_csv)
        assert len(risk) > 0
        assert {"name", "pga_g", "composite_risk", "risk_grade"} <= set(risk.columns)
        assert set(risk["risk_grade"]) <= {"Critical", "High", "Moderate", "Low"}
        # Output is sorted by composite risk, descending.
        assert risk["composite_risk"].is_monotonic_decreasing

    def test_report_reuses_precomputed_risk(self, tmp_path, monkeypatch):
        """--reuse-risk loads report/dam_risk_scores.csv instead of recomputing."""
        from mmeq.cli import cmd_report

        monkeypatch.chdir(tmp_path)  # no dam geojson here: recompute would yield nothing

        catalog = tmp_path / "catalog.csv"
        _synthetic_catalog().to_csv(catalog, index=False)

        report_dir = tmp_path / "prebuilt"
        report_dir.mkdir()
        monkeypatch.setenv("MMEQ_REPORT_DIR", str(report_dir))
        precomputed = pd.DataFrame({
            "name": ["Dam A", "Dam B"],
            "latitude": [22.0, 21.0],
            "longitude": [96.0, 95.0],
            "pga_g": [0.42, 0.10],
            "composite_risk": [8.0, 4.0],
            "risk_grade": ["Critical", "Moderate"],
        })
        # Written after the catalog, so it is "fresh".
        precomputed.to_csv(report_dir / "dam_risk_scores.csv", index=False)

        out = tmp_path / "out"
        cmd_report(_report_args(catalog, out, reuse_risk=True))

        risk = pd.read_csv(out / "dam_risk_scores.csv")
        # The exact synthetic rows prove reuse (a recompute could never make these).
        assert list(risk["name"]) == ["Dam A", "Dam B"]
        assert list(risk["risk_grade"]) == ["Critical", "Moderate"]

    def test_stale_precomputed_risk_is_ignored(self, tmp_path, monkeypatch):
        import os

        from mmeq.cli import _load_precomputed_risk

        report_dir = tmp_path / "prebuilt"
        report_dir.mkdir()
        monkeypatch.setenv("MMEQ_REPORT_DIR", str(report_dir))
        risk_path = report_dir / "dam_risk_scores.csv"
        pd.DataFrame({"name": ["X"], "risk_grade": ["Low"]}).to_csv(risk_path, index=False)

        catalog = tmp_path / "catalog.csv"
        _synthetic_catalog().to_csv(catalog, index=False)
        # Make the risk CSV strictly older than the catalog.
        old = os.path.getmtime(catalog) - 100
        os.utime(risk_path, (old, old))

        assert _load_precomputed_risk(str(catalog)) is None
        # And a fresh one is returned.
        os.utime(risk_path, None)
        assert _load_precomputed_risk(str(catalog)) is not None

    def test_malformed_precomputed_risk_falls_back(self, tmp_path, monkeypatch):
        """An unparseable CSV (e.g. truncated by an interrupted build) -> None."""
        from mmeq.cli import _load_precomputed_risk

        report_dir = tmp_path / "prebuilt"
        report_dir.mkdir()
        monkeypatch.setenv("MMEQ_REPORT_DIR", str(report_dir))
        risk_path = report_dir / "dam_risk_scores.csv"
        # Ragged rows: pandas' C parser raises ParserError on the extra fields.
        risk_path.write_text('name,risk_grade\n"Dam A,Low\nDam B,High,0.5,extra,x\n')

        assert _load_precomputed_risk(None) is None

    def test_zero_byte_precomputed_risk_falls_back(self, tmp_path, monkeypatch):
        """A zero-byte CSV (pandas EmptyDataError) -> None."""
        from mmeq.cli import _load_precomputed_risk

        report_dir = tmp_path / "prebuilt"
        report_dir.mkdir()
        monkeypatch.setenv("MMEQ_REPORT_DIR", str(report_dir))
        (report_dir / "dam_risk_scores.csv").touch()

        assert _load_precomputed_risk(None) is None
