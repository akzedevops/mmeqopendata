import logging
import math
import os
from typing import Optional

import numpy as np
import pandas as pd
import json

logger = logging.getLogger(__name__)


def _load_fault_segments() -> list:
    faults_path = os.path.join(os.getcwd(), "fault_lines.json")
    if not os.path.exists(faults_path):
        return []
    with open(faults_path, encoding="utf-8") as f:
        data = json.load(f)
    segments = []
    for feat in data.get("features", []):
        geom = feat.get("geometry", {})
        if geom.get("type") == "LineString":
            for i in range(len(geom["coordinates"]) - 1):
                segments.append((geom["coordinates"][i], geom["coordinates"][i + 1]))
        elif geom.get("type") == "MultiLineString":
            for line in geom["coordinates"]:
                for i in range(len(line) - 1):
                    segments.append((line[i], line[i + 1]))
    return segments


def _load_dams_df() -> Optional[pd.DataFrame]:
    candidates = [
        os.path.join(os.getcwd(), "myanmar_dams.geojson"),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "myanmar_dams.geojson"),
    ]
    for p in candidates:
        p = os.path.normpath(p)
        if os.path.exists(p):
            with open(p, encoding="utf-8") as f:
                data = json.load(f)
            rows = []
            for feat in data.get("features", []):
                coords = feat["geometry"]["coordinates"]
                props = feat.get("properties", {})
                rows.append({
                    "longitude": coords[0],
                    "latitude": coords[1],
                    "name": props.get("name", "Unnamed Dam"),
                    "status": props.get("status", ""),
                    "function": props.get("function", ""),
                    "capacity_mw": props.get("capacity_mw", ""),
                    "height_m": props.get("height_m", ""),
                    "river": props.get("river", ""),
                    "state": props.get("state", ""),
                    "reservoir_area_km2": props.get("reservoir_area_km2", ""),
                    "total_storage_mcm": props.get("total_storage_mcm", ""),
                })
            return pd.DataFrame(rows)
    return None


def _point_to_segment_distance(px, py, ax, ay, bx, by):
    dx, dy = bx - ax, by - ay
    if dx == 0 and dy == 0:
        return math.sqrt((px - ax) ** 2 + (py - ay) ** 2)
    t = max(0, min(1, ((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy)))
    proj_x, proj_y = ax + t * dx, ay + t * dy
    return math.sqrt((px - proj_x) ** 2 + (py - proj_y) ** 2)


def distance_to_nearest_fault(lat: float, lon: float, segments: list) -> float:
    min_dist = float("inf")
    for (lon1, lat1), (lon2, lat2) in segments:
        d = _point_to_segment_distance(lon, lat, lon1, lat1, lon2, lat2)
        if d < min_dist:
            min_dist = d
    return min_dist * 111.0


def estimate_pga_ask08(
    mag: float,
    rjb_km: float,
    vs30: float = 760.0,
    rake: float = 180.0,
    dip: float = 90.0,
) -> float:
    """
    Estimate PGA using the Abrahamson & Silva (2008) NGA-West1 GMPE.
    Designed for shallow crustal earthquakes in active tectonic regions.

    Uses coefficients for PGA (T=0.0s) from Table 3 of Abrahamson & Silva (2008).

    Parameters
    ----------
    mag : float - Moment magnitude
    rjb_km : float - Joyner-Boore distance (km). For extended ruptures, this is
                      the distance to the surface projection of the fault plane.
                      Approximated as distance to nearest fault trace for this study.
    vs30 : float - Average shear-wave velocity in top 30m (m/s). Default 760 = BC rock.
    rake : float - Rake angle (degrees). 180 = pure right-lateral strike-slip.
    dip : float - Fault dip angle (degrees). 90 = vertical (strike-slip).

    Returns
    -------
    float - Median PGA in g (natural log units converted to g)
    """
    rjb = max(rjb_km, 0.0)

    a1 = -0.526
    a2 = -1.60
    a3 = 0.143
    a4 = 0.0
    a10 = -0.135
    a13 = -0.015
    c1 = 6.2
    c4 = 5.6
    a6 = 0.0
    a12 = 0.0

    h = c4
    r = math.sqrt(rjb ** 2 + h ** 2)

    if mag <= c1:
        f_mag = a1 + a4 * (mag - c1) + a13 * (8.5 - mag) ** 2
    else:
        f_mag = a1 + a10 * (mag - c1) + a13 * (8.5 - mag) ** 2

    f_dis = (a2 + a3 * mag) * math.log(r)

    ln_pga = f_mag + f_dis

    if vs30 < 760.0:
        f_site = a6 * math.log(vs30 / 760.0)
        ln_pga += f_site

    pga_g = math.exp(ln_pga)
    return max(pga_g, 0.0001)


def estimate_pga(mag: float, depth_km: float, dist_km: float, vs30: float = 760.0) -> float:
    """
    Estimate PGA using Abrahamson & Silva (2008) for crustal strike-slip earthquakes.
    For extended ruptures (like the 500km 2025 event), dist_km should be interpreted
    as distance to the fault trace (Rjb), not epicentral distance.
    vs30 is the site-specific shear-wave velocity in m/s.
    """
    if mag < 3.0:
        return 0.0001
    try:
        pga = estimate_pga_ask08(
            mag=mag,
            rjb_km=dist_km,
            vs30=vs30,
            rake=180.0,
            dip=90.0,
        )
        return max(pga, 0.0001)
    except Exception:
        r = math.sqrt(dist_km ** 2 + depth_km ** 2)
        if r < 1:
            r = 1
        h = 0.032 * 10 ** (0.41 * mag)
        log_a = 0.41 * mag - math.log10(r + h) - 0.0034 * r + 1.30
        pga_ms2 = 10 ** log_a / 100.0
        return max(pga_ms2 / 9.81, 0.0001)


def _load_vs30() -> dict:
    vs30_path = os.path.join(os.getcwd(), "report", "dam_vs30.csv")
    if not os.path.exists(vs30_path):
        return {}
    df = pd.read_csv(vs30_path)
    vs30_map = {}
    for _, row in df.iterrows():
        key = f"{row['lat']:.4f},{row['lon']:.4f}"
        vs30_map[key] = float(row["vs30"])
    return vs30_map


def compute_hazard_curve(
    rjb_km: float,
    vs30: float,
    b_val: float,
    a_val: float,
    mc: float,
    catalog_years: float = 56.0,
    pga_levels: np.ndarray = None,
    min_mag: float = 5.0,
    max_mag: float = 8.0,
    delta_m: float = 0.1,
) -> pd.DataFrame:
    """
    Compute a seismic hazard curve (PGA vs annual exceedance probability)
    at a given site using the Cornell-McGuire PSHA approach.

    a_val is the catalog-level a-value; it is converted to annual rate internally.
    """
    if pga_levels is None:
        pga_levels = np.logspace(-3, 0, 50)

    a_annual = a_val - math.log10(catalog_years)

    mags = np.arange(min_mag, max_mag + delta_m, delta_m)
    rates = np.zeros(len(pga_levels))

    for m in mags:
        n_per_year = 10 ** (a_annual - b_val * m) - 10 ** (a_annual - b_val * (m + delta_m))
        n_per_year = max(n_per_year, 0)

        pga_median = estimate_pga_ask08(mag=m, rjb_km=rjb_km, vs30=vs30)

        sigma_ln = 0.65
        for i, pga_target in enumerate(pga_levels):
            if pga_median > 0:
                epsilon = (math.log(pga_target) - math.log(pga_median)) / sigma_ln
                from scipy.stats import norm
                p_exceed = 1.0 - norm.cdf(epsilon)
                rates[i] += n_per_year * p_exceed

    prob_50yr = 1.0 - np.exp(-rates * 50)

    return pd.DataFrame({
        "pga_g": pga_levels,
        "annual_rate": rates,
        "annual_prob": rates,
        "prob_50yr": prob_50yr,
        "return_period_yr": np.where(rates > 0, 1.0 / rates, np.inf),
    })


def compute_return_period(mag_threshold: float, b_value: float, a_value: float, years: float = 50) -> float:
    n_per_year = 10 ** (a_value - b_value * mag_threshold)
    if n_per_year <= 0:
        return float("inf")
    return 1.0 / n_per_year


def sensitivity_analysis(
    eq_df: pd.DataFrame,
    b_val: float,
    a_val: float,
    mc: float,
    n_samples: int = 100,
) -> pd.DataFrame:
    """
    Monte Carlo sensitivity analysis on risk scoring weights.
    Returns a DataFrame with weight combinations and resulting risk grade distributions.
    """
    dams_df = _load_dams_df()
    if dams_df is None or dams_df.empty:
        return pd.DataFrame()

    fault_segments = _load_fault_segments()
    major_eq = eq_df.nlargest(1, "mag").iloc[0]
    major_mag = major_eq["mag"]
    major_depth = major_eq["depth"]
    major_lat = major_eq["latitude"]
    major_lon = major_eq["longitude"]

    rng = np.random.RandomState(42)
    results = []
    for _ in range(n_samples):
        w = rng.dirichlet([1, 1, 1, 1])
        w_seismic, w_fault, w_prox, w_exp = w

        critical = high = moderate = low = 0
        for __, dam in dams_df.iterrows():
            dlat = dam["latitude"] - major_lat
            dlon = (dam["longitude"] - major_lon) * math.cos(math.radians(major_lat))
            dist_to_major = math.sqrt(dlat ** 2 + dlon ** 2) * 111.0
            dist_fault = distance_to_nearest_fault(dam["latitude"], dam["longitude"], fault_segments) if fault_segments else 50.0
            pga = estimate_pga(major_mag, major_depth, dist_to_major)

            seismic_score = min(10, pga / 0.05 * 10)
            fault_score = min(10, 10 / max(1, dist_fault / 10)) if not math.isnan(dist_fault) else 5
            prox_score = min(10, 10 / max(1, dist_to_major / 50))

            try:
                h = float(dam.get("height_m") or 0)
            except (ValueError, TypeError):
                h = 0
            try:
                c = float(dam.get("capacity_mw") or 0)
            except (ValueError, TypeError):
                c = 0
            try:
                s = float(dam.get("total_storage_mcm") or 0)
            except (ValueError, TypeError):
                s = 0
            exposure_score = min(10, (h * 0.3 + c * 0.02 + s * 0.005) / 5)

            composite = seismic_score * w_seismic + fault_score * w_fault + prox_score * w_prox + exposure_score * w_exp

            if composite >= 7:
                critical += 1
            elif composite >= 5:
                high += 1
            elif composite >= 3:
                moderate += 1
            else:
                low += 1

        results.append({
            "w_seismic": round(w_seismic, 3),
            "w_fault": round(w_fault, 3),
            "w_proximity": round(w_prox, 3),
            "w_exposure": round(w_exp, 3),
            "critical": critical,
            "high": high,
            "moderate": moderate,
            "low": low,
        })

    return pd.DataFrame(results)


def dam_risk_scores(
    eq_df: pd.DataFrame,
    b_val: float,
    a_val: float,
    mc: float,
) -> pd.DataFrame:
    dams_df = _load_dams_df()
    if dams_df is None or dams_df.empty:
        logger.warning("No dam data available")
        return pd.DataFrame()

    fault_segments = _load_fault_segments()
    vs30_map = _load_vs30()
    major_eq = eq_df.nlargest(1, "mag").iloc[0]
    major_mag = major_eq["mag"]
    major_depth = major_eq["depth"]
    major_lat = major_eq["latitude"]
    major_lon = major_eq["longitude"]

    results = []
    for _, dam in dams_df.iterrows():
        dlat = dam["latitude"] - major_lat
        dlon = (dam["longitude"] - major_lon) * math.cos(math.radians(major_lat))
        dist_to_major = math.sqrt(dlat ** 2 + dlon ** 2) * 111.0

        dist_fault = distance_to_nearest_fault(dam["latitude"], dam["longitude"], fault_segments) if fault_segments else float("nan")

        rjb_km = dist_fault if (not math.isnan(dist_fault) and dist_fault > 0) else dist_to_major

        vs30 = vs30_map.get(f"{dam['latitude']:.4f},{dam['longitude']:.4f}", 760.0)
        pga = estimate_pga(major_mag, major_depth, rjb_km, vs30=vs30)

        nearby = eq_df[
            ((eq_df["latitude"] - dam["latitude"]).abs() < 1.0)
            & ((eq_df["longitude"] - dam["longitude"]).abs() < 1.0)
        ]
        if len(nearby) == 0:
            nearby = eq_df

        seismic_score = min(10, pga / 0.05 * 10)
        fault_score = min(10, 10 / max(1, dist_fault / 10)) if not math.isnan(dist_fault) else 5
        prox_score = min(10, 10 / max(1, dist_to_major / 50))

        try:
            h = float(dam.get("height_m") or 0)
        except (ValueError, TypeError):
            h = 0
        try:
            c = float(dam.get("capacity_mw") or 0)
        except (ValueError, TypeError):
            c = 0
        try:
            s = float(dam.get("total_storage_mcm") or 0)
        except (ValueError, TypeError):
            s = 0

        exposure_score = min(10, (h * 0.3 + c * 0.02 + s * 0.005) / 5)

        composite = seismic_score * 0.35 + fault_score * 0.30 + prox_score * 0.20 + exposure_score * 0.15

        return_50yr = compute_return_period(6.0, b_val, a_val, 50)

        results.append({
            "name": dam["name"],
            "status": dam["status"],
            "function": dam["function"],
            "river": dam["river"],
            "state": dam["state"],
            "latitude": dam["latitude"],
            "longitude": dam["longitude"],
            "height_m": h,
            "capacity_mw": c,
            "storage_mcm": s,
            "dist_to_major_km": round(dist_to_major, 1),
            "dist_to_fault_km": round(dist_fault, 1) if not math.isnan(dist_fault) else None,
            "vs30": round(vs30),
            "pga_g": round(pga, 4),
            "seismic_score": round(seismic_score, 1),
            "fault_score": round(fault_score, 1),
            "proximity_score": round(prox_score, 1),
            "exposure_score": round(exposure_score, 1),
            "composite_risk": round(composite, 1),
            "risk_grade": (
                "Critical" if composite >= 7
                else "High" if composite >= 5
                else "Moderate" if composite >= 3
                else "Low"
            ),
            "return_period_m6_yrs": round(return_50yr, 1),
        })

    risk_df = pd.DataFrame(results).sort_values("composite_risk", ascending=False).reset_index(drop=True)
    logger.info(f"Computed risk scores for {len(risk_df)} dams")
    return risk_df
