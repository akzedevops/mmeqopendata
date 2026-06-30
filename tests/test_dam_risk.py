"""Regression tests for the ASK08 GMPE and hazard helpers in dam_risk.

These guard the v2 fixes for the broken large-distance coefficient (a18) and the
un-annualized return period, both of which passed the original test suite because
nothing exercised far-field PGA or the rate conversion.
"""
import math

import numpy as np
import pytest

from mmeq.analysis.dam_risk import estimate_pga_ask08, compute_return_period


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
