"""Regression test for the Omori fit: early (sub-hour) aftershocks must count."""
import numpy as np
import pandas as pd

from mmeq.analysis.aftershock import omori_params


def _sequence(dt_hours):
    """Catalog: a M7.0 mainshock at t0 plus aftershocks at the given offsets."""
    t0 = pd.Timestamp("2025-03-28 06:00:00")
    times = [t0] + [t0 + pd.Timedelta(hours=float(h)) for h in dt_hours]
    mags = [7.0] + [4.0] * len(dt_hours)
    return pd.DataFrame({"time_utc": times, "mag": mags}), t0


def test_first_hour_aftershocks_are_used():
    # Omori decay: most events in the first hour. The old code started the first
    # histogram bin at t=1 h and dropped all of these, biasing the fit.
    rng = np.random.default_rng(0)
    early = rng.uniform(0.02, 1.0, 60)   # 60 events within the first hour
    late = rng.uniform(1.0, 72.0, 15)
    catalog, t0 = _sequence(np.concatenate([early, late]))

    K, c, p = omori_params(t0, catalog, window_days=30, min_mag=3.0)

    # A real sequence dominated by early events must yield a usable fit, not the
    # (0, 1, 0) "could not fit" sentinel.
    assert K > 0
    assert 0.5 <= p <= 2.0
    assert c > 0


def test_too_few_events_returns_sentinel():
    catalog, t0 = _sequence([2.0, 5.0])  # only 2 aftershocks
    assert omori_params(t0, catalog, min_mag=3.0) == (0.0, 1.0, 0.0)


def test_mle_recovers_synthetic_omori_parameters():
    # Simulate an inhomogeneous Poisson process with known (K, c, p) by
    # inverting the cumulative intensity, then require the MLE to recover p
    # within 0.1 — the binned least-squares fit this replaced was off by
    # 0.2-0.4 (audit finding, Ogata 1983).
    K_true, c_true, p_true = 100.0, 2.0, 1.1
    T = 30 * 24.0
    rng = np.random.default_rng(42)

    def cum(t):
        return K_true * ((t + c_true) ** (1 - p_true) - c_true ** (1 - p_true)) / (1 - p_true)

    def inv(y):
        return (y * (1 - p_true) / K_true + c_true ** (1 - p_true)) ** (1 / (1 - p_true)) - c_true

    n = rng.poisson(cum(T))
    times_h = np.sort(inv(rng.uniform(0, cum(T), n)))
    catalog, t0 = _sequence(times_h[times_h > 0])

    K, c, p = omori_params(t0, catalog, window_days=30, min_mag=3.0)
    assert abs(p - p_true) < 0.1
    assert abs(c - c_true) < 1.5
    assert 0.5 * K_true < K < 2.0 * K_true
