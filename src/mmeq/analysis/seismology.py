import logging
import numpy as np
import pandas as pd
from typing import Tuple, Optional

logger = logging.getLogger(__name__)


def b_value(
    magnitudes: pd.Series,
    min_mag: Optional[float] = None,
) -> Tuple[float, float, float]:
    """
    Calculate b-value using the maximum likelihood method (Aki, 1965).
    Returns (b_value, a_value, min_mag_used).
    """
    magnitudes = magnitudes.dropna()
    if min_mag is None:
        min_mag = magnitude_of_completeness(magnitudes)
    mags = magnitudes[magnitudes >= min_mag]
    if len(mags) < 10:
        logger.warning("Too few events for reliable b-value estimation")
        return 0.0, 0.0, min_mag

    mean_mag = mags.mean()
    delta_m = 0.1
    b = np.log10(np.e) / (mean_mag - (min_mag - delta_m / 2))
    n = len(mags)
    a = np.log10(n) + b * min_mag

    logger.info(f"b-value: {b:.3f}, a-value: {a:.3f}, Mc: {min_mag:.1f}, N={n}")
    return b, a, min_mag


def magnitude_of_completeness(
    magnitudes: pd.Series,
    bin_width: float = 0.1,
) -> float:
    """
    Estimate magnitude of completeness (Mc) using the maximum curvature method.
    """
    magnitudes = magnitudes.dropna()
    if magnitudes.empty:
        return 0.0

    bins = np.arange(magnitudes.min(), magnitudes.max() + bin_width, bin_width)
    hist, bin_edges = np.histogram(magnitudes, bins=bins)
    max_idx = np.argmax(hist)
    mc = bin_edges[max_idx]
    return round(mc, 1)


def decluster_catalog(
    df: pd.DataFrame,
    window_days: float = 30.0,
    distance_km: float = 50.0,
) -> pd.DataFrame:
    """
    Simple declustering using window method (Gardner-Knopoff style).
    For each main shock, remove smaller events within the time-distance window.
    """
    if "time_utc" not in df.columns:
        raise ValueError("DataFrame must have 'time_utc' column")

    df = df.copy()
    df["time_utc"] = pd.to_datetime(df["time_utc"])
    df = df.sort_values("time_utc", ascending=False).reset_index(drop=True)

    times = df["time_utc"].values
    lats = df["latitude"].values
    lons = df["longitude"].values
    mags = df["mag"].values
    n = len(df)

    is_mainshock = np.ones(n, dtype=bool)

    window_td = np.timedelta64(int(window_days * 86400 * 1e9), "ns")

    for i in range(n):
        if not is_mainshock[i]:
            continue
        mag_i = mags[i]

        time_mask = np.abs(times - times[i]) <= window_td
        mag_mask = mags < mag_i
        candidates = np.where(time_mask & mag_mask & is_mainshock)[0]

        if len(candidates) == 0:
            continue

        dlat = lats[candidates] - lats[i]
        dlon = lons[candidates] - lons[i]
        dists = np.sqrt(dlat**2 + dlon**2) * 111.0
        close = candidates[dists <= distance_km]
        is_mainshock[close] = False

    df["is_mainshock"] = is_mainshock
    mainshocks = df[df["is_mainshock"]].drop(columns=["is_mainshock"])
    logger.info(
        f"Declustered: {len(df)} total -> {len(mainshocks)} main shocks "
        f"({len(df) - len(mainshocks)} removed)"
    )
    return mainshocks
