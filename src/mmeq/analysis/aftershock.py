import logging
import math
from typing import Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def omori_params(
    mainshock_time: pd.Timestamp,
    catalog: pd.DataFrame,
    window_days: float = 30,
    min_mag: float = 3.0,
) -> Tuple[float, float, float]:
    aftershocks = catalog[
        (catalog["time_utc"] > mainshock_time)
        & (catalog["time_utc"] <= mainshock_time + pd.Timedelta(days=window_days))
        & (catalog["mag"] >= min_mag)
    ].copy()

    if len(aftershocks) < 5:
        logger.warning("Too few aftershocks for Omori fit")
        return 0.0, 1.0, 0.0

    dt_hours = (aftershocks["time_utc"] - mainshock_time).dt.total_seconds() / 3600
    dt_hours = dt_hours.values

    bins = np.logspace(np.log10(1), np.log10(max(dt_hours.max(), 2)), 20)
    counts, edges = np.histogram(dt_hours, bins=bins)
    centers = np.sqrt(edges[:-1] * edges[1:])

    mask = counts > 0
    if mask.sum() < 3:
        return 0.0, 1.0, 0.0

    log_centers = np.log10(centers[mask])
    log_rates = np.log10(counts[mask] / np.diff(edges)[mask])

    coeffs = np.polyfit(log_centers, log_rates, 1)
    p = max(0.5, min(2.0, -coeffs[0]))

    K = counts[0] / (centers[0] ** (-p + 1) + 0.01)
    c_param = centers[0] * 0.1

    total_aftershocks = len(aftershocks)
    logger.info(f"Omori params: K={K:.1f}, c={c_param:.2f}h, p={p:.2f}, N={total_aftershocks}")
    return K, c_param, p


def forecast_aftershocks(
    K: float,
    c: float,
    p: float,
    t_start_hours: float,
    t_end_hours: float,
) -> pd.DataFrame:
    if K <= 0 or p <= 0:
        return pd.DataFrame()

    t = np.linspace(t_start_hours, t_end_hours, 100)
    rate = K / (t + c) ** p
    cumul = np.zeros_like(t)
    for i in range(1, len(t)):
        dt = t[i] - t[i - 1]
        cumul[i] = cumul[i - 1] + rate[i] * dt

    return pd.DataFrame({
        "hours_since_mainshock": t,
        "rate_per_hour": rate,
        "cumulative_expected": cumul,
    })


def modified_omori_forecast(
    catalog: pd.DataFrame,
    mainshock_time: pd.Timestamp = None,
    forecast_days: int = 90,
    min_mag: float = 3.0,
) -> Tuple[pd.DataFrame, dict]:
    if mainshock_time is None:
        mainshock_time = catalog.loc[catalog["mag"].idxmax(), "time_utc"]
        if isinstance(mainshock_time, str):
            mainshock_time = pd.Timestamp(mainshock_time)

    mainshock = catalog.loc[catalog["mag"].idxmax()]

    K, c, p = omori_params(mainshock_time, catalog, window_days=30, min_mag=min_mag)

    if K <= 0:
        logger.warning("Could not fit Omori parameters")
        return pd.DataFrame(), {}

    elapsed_hours = (pd.Timestamp.now(tz="UTC") - mainshock_time.tz_localize("UTC")).total_seconds() / 3600
    forecast_hours = forecast_days * 24
    start_h = max(0, elapsed_hours)

    forecast_df = forecast_aftershocks(K, c, p, start_h, start_h + forecast_hours)

    params = {
        "mainshock_mag": float(mainshock["mag"]),
        "mainshock_time": str(mainshock_time),
        "mainshock_lat": float(mainshock["latitude"]),
        "mainshock_lon": float(mainshock["longitude"]),
        "K": round(K, 2),
        "c_hours": round(c, 3),
        "p": round(p, 3),
        "min_mag": min_mag,
        "elapsed_hours": round(elapsed_hours, 1),
        "forecast_days": forecast_days,
        "expected_aftershocks_7d": 0,
        "expected_aftershocks_30d": 0,
        "expected_aftershocks_90d": 0,
    }

    for days, key in [(7, "expected_aftershocks_7d"), (30, "expected_aftershocks_30d"), (90, "expected_aftershocks_90d")]:
        end_h = start_h + days * 24
        sub = forecast_df[forecast_df["hours_since_mainshock"] <= end_h]
        if not sub.empty:
            params[key] = round(sub["cumulative_expected"].iloc[-1] - sub["cumulative_expected"].iloc[0], 0)

    logger.info(
        f"Aftershock forecast: {params['expected_aftershocks_30d']:.0f} events (M>={min_mag}) "
        f"expected in next 30 days, p={p:.2f}"
    )
    return forecast_df, params
