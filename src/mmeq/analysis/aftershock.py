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
    dt_hours = dt_hours[dt_hours > 0]
    if len(dt_hours) < 5:
        return 0.0, 1.0, 0.0

    # Log-spaced bins spanning the full observed range. The first bin edge must
    # start at the earliest aftershock (often minutes after the mainshock), not
    # a hardcoded t=1 h — Omori decay peaks early, and np.histogram silently
    # drops everything below the lowest edge.
    t_min = max(dt_hours.min(), 0.01)
    t_max = max(dt_hours.max(), t_min * 10)
    bins = np.logspace(np.log10(t_min), np.log10(t_max), 20)
    counts, edges = np.histogram(dt_hours, bins=bins)
    centers = np.sqrt(edges[:-1] * edges[1:])

    mask = counts > 0
    if mask.sum() < 3:
        return 0.0, 1.0, 0.0

    log_centers = np.log10(centers[mask])
    log_rates = np.log10(counts[mask] / np.diff(edges)[mask])

    # rate(t) = K / (t + c)^p  ->  log10(rate) ≈ log10(K) - p*log10(t) for t >> c.
    # Slope gives p; intercept gives K (events/hour at t=1 h), which uses the
    # whole fit rather than a single bin's raw count.
    coeffs = np.polyfit(log_centers, log_rates, 1)
    p = max(0.5, min(2.0, -coeffs[0]))
    K = 10 ** coeffs[1]
    c_param = 0.1  # hours; typical Omori offset, small vs the fit window

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
        mainshock = catalog.loc[catalog["mag"].idxmax()]
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


def aftershock_probability_grid(
    K: float, c: float, p: float,
    mainshock_lat: float = 22.01, mainshock_lon: float = 95.94, mainshock_mag: float = 7.7,
    forecast_days: int = 30, min_mag: float = 5.0,
    grid_spacing: float = 0.1, radius_deg: float = 5.0,
) -> pd.DataFrame:
    """Generate spatial probability grid for aftershocks using Omori + ETAS kernel.

    Uses power-law spatial decay from the rupture zone (Wells & Coppersmith
    scaling for rupture length) combined with temporal Omori forecast.

    Returns DataFrame with lat, lon, expected_count columns. The per-cell values
    are expected event counts (they sum to n_expected over the grid), not
    probabilities, and may exceed 1.
    """
    # Temporal: expected number of M>=min_mag aftershocks in forecast window
    # Assume mainshock happened ~30 days ago for current forecast
    t_start = 30 * 24  # hours since mainshock
    t_end = (30 + forecast_days) * 24
    if p == 1.0:
        n_expected = K * (np.log(t_end + c) - np.log(t_start + c))
    else:
        n_expected = K / (1 - p) * ((t_end + c) ** (1 - p) - (t_start + c) ** (1 - p))

    # Scale for minimum magnitude (Gutenberg-Richter with b=1)
    n_expected *= 10 ** (-(min_mag - 3.0))

    # Spatial kernel: power-law decay from rupture zone
    # Rupture length ~ 475 km for M7.7 (Wells & Coppersmith)
    rupture_half_len = 237.5 / 111.0  # degrees
    d_param = 0.05  # characteristic distance in degrees
    q = 1.5  # power-law exponent

    lats = np.arange(mainshock_lat - radius_deg, mainshock_lat + radius_deg, grid_spacing)
    lons = np.arange(mainshock_lon - radius_deg, mainshock_lon + radius_deg, grid_spacing)
    grid_lat, grid_lon = np.meshgrid(lats, lons, indexing="ij")

    # Distance from nearest point on rupture (simplified as N-S line)
    dist_along = np.clip(grid_lat - mainshock_lat, -rupture_half_len, rupture_half_len)
    dist_perp = (grid_lon - mainshock_lon) * np.cos(np.radians(mainshock_lat))
    dist_from_rupture = np.sqrt((grid_lat - mainshock_lat - dist_along) ** 2 + dist_perp ** 2)

    # Power-law kernel
    kernel = (1 + (dist_from_rupture / d_param) ** 2) ** (-q)
    kernel /= kernel.sum()  # normalize

    expected = n_expected * kernel

    rows = []
    for i in range(len(lats)):
        for j in range(len(lons)):
            if expected[i, j] > 1e-6:
                rows.append({"lat": lats[i], "lon": lons[j], "expected_count": expected[i, j]})
    return pd.DataFrame(rows)
