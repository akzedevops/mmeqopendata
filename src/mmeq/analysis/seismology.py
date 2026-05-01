import logging
import math
import numpy as np
import pandas as pd
from typing import Tuple, Optional, Dict, List

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
    Estimate magnitude of completeness (Mc) using the maximum curvature method
    with a stability correction.
    The raw max-curvature method tends to underestimate Mc by 0.1-0.2 units
    (Woessner & Wiemer 2005), so we apply a correction of +0.2.
    """
    magnitudes = magnitudes.dropna()
    if magnitudes.empty:
        return 0.0

    bins = np.arange(magnitudes.min(), magnitudes.max() + bin_width, bin_width)
    hist, bin_edges = np.histogram(magnitudes, bins=bins)
    max_idx = np.argmax(hist)
    mc = bin_edges[max_idx] + 0.2
    return round(mc, 1)


def b_value_stability(
    magnitudes: pd.Series,
    mc_range: np.ndarray = None,
    bin_width: float = 0.1,
) -> List[Dict]:
    """
    Compute b-value at multiple Mc thresholds to assess stability.
    Returns list of dicts with mc, b, a, n for each threshold.
    """
    magnitudes = magnitudes.dropna()
    if mc_range is None:
        mc_range = np.arange(2.0, 6.1, 0.5)
    results = []
    for mc in mc_range:
        mags = magnitudes[magnitudes >= mc]
        if len(mags) < 20:
            continue
        mean_mag = mags.mean()
        b = np.log10(np.e) / (mean_mag - (mc - bin_width / 2))
        a = np.log10(len(mags)) + b * mc
        results.append({"mc": round(mc, 1), "b": round(b, 3), "a": round(a, 3), "n": len(mags)})
    return results


def multi_period_b_value(
    df: pd.DataFrame,
    periods: List[Tuple[int, int]] = None,
    mc: float = None,
) -> List[Dict]:
    """
    Compute b-value for different time periods to assess catalog homogeneity.
    """
    if periods is None:
        periods = [
            (1970, 1999), (2000, 2009), (2010, 2019), (2020, 2026),
        ]
    results = []
    for start, end in periods:
        sub = df[(df["time_utc"].dt.year >= start) & (df["time_utc"].dt.year <= end)]
        if len(sub) < 50:
            continue
        mc_used = mc if mc is not None else magnitude_of_completeness(sub["mag"])
        b, a, mc_actual = b_value(sub["mag"], min_mag=mc_used)
        results.append({
            "period": f"{start}-{end}",
            "n_total": len(sub),
            "n_above_mc": len(sub[sub["mag"] >= mc_actual]),
            "mc": mc_actual,
            "b": round(b, 3),
            "a": round(a, 3),
        })
    return results


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
        dlon = (lons[candidates] - lons[i]) * math.cos(math.radians(lats[i]))
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


def seismic_gap_analysis(df: pd.DataFrame, lon_range=(94, 98), lat_range=(16, 28), bin_size=0.5) -> pd.DataFrame:
    """Compute cumulative seismic moment release by latitude along the Sagaing Fault.

    Returns DataFrame with lat_bin, moment_nm, equivalent_mw, event_count.
    Low-moment bins indicate potential seismic gaps.
    """
    corridor = df[
        (df["longitude"].between(*lon_range)) & (df["latitude"].between(*lat_range))
    ].copy()
    corridor["moment"] = 10 ** (1.5 * corridor["mag"] + 9.05)
    corridor["lat_bin"] = (corridor["latitude"] / bin_size).round() * bin_size

    result = corridor.groupby("lat_bin").agg(
        moment_nm=("moment", "sum"),
        event_count=("mag", "count"),
        max_mag=("mag", "max"),
    ).reindex(np.arange(lat_range[0], lat_range[1], bin_size), fill_value=0)
    result.index.name = "lat_bin"
    result["equivalent_mw"] = np.where(
        result["moment_nm"] > 0,
        (np.log10(result["moment_nm"]) - 9.05) / 1.5,
        0,
    )
    return result.reset_index()
