import logging
import math
from typing import Tuple

import numpy as np
import pandas as pd

from mmeq.analysis.seismology import select_scenario_event

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
    dt_hours = dt_hours[dt_hours > 0]
    if len(dt_hours) < 5:
        return 0.0, 1.0, 0.0

    # Maximum-likelihood fit of the modified Omori law rate(t) = K/(t+c)^p on
    # the exact event times (Ogata 1983). The previous least-squares fit on
    # log-binned counts is provably biased (underestimates p by 0.2-0.4 and K
    # severalfold on synthetic catalogs) and hardcoded c.
    #
    # For fixed (c, p) the K that maximizes the likelihood over (0, T] is
    # K_hat = N / A(c, p) with A = ((T+c)^(1-p) - c^(1-p)) / (1-p)
    # (A = ln((T+c)/c) at p=1), giving the profile negative log-likelihood
    # -(N ln(N/A) - p * sum(ln(t_i + c)) - N).
    from scipy.optimize import minimize

    t_obs = np.sort(dt_hours)
    T = window_days * 24.0
    n = len(t_obs)

    def _integral(c: float, p: float) -> float:
        if abs(p - 1.0) < 1e-9:
            return math.log((T + c) / c)
        return ((T + c) ** (1.0 - p) - c ** (1.0 - p)) / (1.0 - p)

    def _neg_loglik(x) -> float:
        log_c, p = x
        c = math.exp(log_c)
        a = _integral(c, p)
        if not np.isfinite(a) or a <= 0:
            return 1e12
        return -(n * math.log(n / a) - p * np.log(t_obs + c).sum() - n)

    bounds = [(math.log(1e-3), math.log(1e3)), (0.2, 3.0)]
    best = None
    for c0 in (0.1, 1.0, 10.0):
        for p0 in (0.8, 1.1, 1.5):
            res = minimize(_neg_loglik, x0=[math.log(c0), p0],
                           method="L-BFGS-B", bounds=bounds)
            if res.success and (best is None or res.fun < best.fun):
                best = res
    if best is None:
        logger.warning("Omori MLE did not converge")
        return 0.0, 1.0, 0.0

    c_param = math.exp(best.x[0])
    p = float(best.x[1])
    K = n / _integral(c_param, p)
    if p <= bounds[1][0] + 1e-6 or p >= bounds[1][1] - 1e-6:
        logger.warning("Omori p=%.2f sits at an optimizer bound — fit is suspect", p)

    logger.info(f"Omori MLE params: K={K:.1f}, c={c_param:.2f}h, p={p:.2f}, N={n}")
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
        # Max-magnitude event with ties broken by recency (the catalog holds
        # two M7.7s; a bare idxmax silently fits the 1988 Lancang event
        # instead of the 2025 Sagaing mainshock).
        mainshock = select_scenario_event(catalog)
        mainshock_time = mainshock["time_utc"]
        if isinstance(mainshock_time, str):
            mainshock_time = pd.Timestamp(mainshock_time)
    else:
        # Caller supplied the mainshock time: describe the event nearest that
        # time, not the global-max event, so the reported lat/lon/mag match the
        # window the Omori fit is centred on.
        mainshock_time = pd.Timestamp(mainshock_time)
        idx = (pd.to_datetime(catalog["time_utc"]) - mainshock_time).abs().idxmin()
        mainshock = catalog.loc[idx]

    K, c, p = omori_params(mainshock_time, catalog, window_days=30, min_mag=min_mag)

    if K <= 0:
        logger.warning("Could not fit Omori parameters")
        return pd.DataFrame(), {}

    # mainshock_time may be tz-naive or tz-aware depending on how time_utc was
    # loaded; normalize to UTC before differencing with an aware "now".
    ms_utc = mainshock_time.tz_localize("UTC") if mainshock_time.tzinfo is None else mainshock_time.tz_convert("UTC")
    elapsed_hours = (pd.Timestamp.now(tz="UTC") - ms_utc).total_seconds() / 3600
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
