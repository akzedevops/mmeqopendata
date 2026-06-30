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
    """Distance (km) from a point to the nearest fault segment.

    Segments are in (lon, lat) degree pairs.  The point-to-segment math is
    done in a local approximate Cartesian frame (degrees scaled by cos(lat))
    so that 1 unit ≈ 111 km in both axes at the site latitude.
    """
    cos_lat = math.cos(math.radians(lat))
    min_dist_deg = float("inf")
    for (lon1, lat1), (lon2, lat2) in segments:
        # scale longitudes to approximate equal-area at site latitude
        d = _point_to_segment_distance(
            lon * cos_lat, lat,
            lon1 * cos_lat, lat1,
            lon2 * cos_lat, lat2,
        )
        if d < min_dist_deg:
            min_dist_deg = d
    return min_dist_deg * 111.0


def estimate_pga_ask08(
    mag: float,
    rrup_km: float,
    vs30: float = 760.0,
    rake: float = 180.0,
    dip: float = 90.0,
    ztor: float = 0.0,
    width_km: float = 15.0,
    rjb_km: float = None,
    rx_km: float = -1.0,
) -> float:
    """Abrahamson & Silva (2008) NGA-West1 GMPE for PGA.

    Coefficients verified against the OpenQuake reference implementation
    (gem/oq-engine abrahamson_silva_2008.py) and Tables 4-5a of
    Abrahamson & Silva, Earthquake Spectra 24(1):67-97.

    Parameters
    ----------
    mag      : Moment magnitude
    rrup_km  : Closest distance to rupture plane (km)
    vs30     : Shear-wave velocity in top 30 m (m/s), default 760
    rake     : Rake angle (degrees). 180 = right-lateral strike-slip
    dip      : Fault dip (degrees). 90 = vertical
    ztor     : Depth to top of rupture (km), default 0
    width_km : Down-dip rupture width (km), default 15
    rjb_km   : Joyner-Boore distance (km); defaults to rrup_km
    rx_km    : Horizontal distance from top edge of rupture (km);
               negative = footwall side (no hanging-wall effect)

    Returns
    -------
    Median PGA in g.
    """
    if rjb_km is None:
        rjb_km = rrup_km

    rrup = max(rrup_km, 0.1)
    rjb = max(rjb_km, 0.0)

    # ------------------------------------------------------------------
    # IMT-independent constants  (Table 4, p. 84)
    # ------------------------------------------------------------------
    c1 = 6.75
    c4 = 4.5
    a3 = 0.265
    a4 = -0.231
    a5 = -0.398
    n = 1.18
    c = 1.88
    c2 = 50.0  # noqa: F841 (ASK08 coefficient kept for reference; unused in this PGA impl)

    # ------------------------------------------------------------------
    # PGA coefficients  (Table 5a, p. 84)
    # ------------------------------------------------------------------
    VLIN = 865.1
    b_site = -1.186
    a1 = 0.804
    a2 = -0.9679
    a8 = -0.0372
    a10 = 0.9445
    a12 = 0.0
    a13 = -0.0600
    a14 = 1.0800
    a15 = -0.3500  # noqa: F841 (HW coefficient; HW term simplified for vertical faults)
    a16 = 0.9000
    a18 = -0.0067

    # ------------------------------------------------------------------
    # 1. Base term  (Eq. 2-4, p. 75)
    # ------------------------------------------------------------------
    R = math.sqrt(rrup ** 2 + c4 ** 2)

    if mag <= c1:
        f1 = a1 + a4 * (mag - c1) + a8 * (8.5 - mag) ** 2 + (a2 + a3 * (mag - c1)) * math.log(R)
    else:
        f1 = a1 + a5 * (mag - c1) + a8 * (8.5 - mag) ** 2 + (a2 + a3 * (mag - c1)) * math.log(R)

    # ------------------------------------------------------------------
    # 2. Faulting-style term  (Table 2, p. 75)
    # ------------------------------------------------------------------
    f_style = 0.0
    if 30 < rake < 150:       # reverse
        f_style = a12
    elif -120 < rake < -60:   # normal
        f_style = a13

    # ------------------------------------------------------------------
    # 3. Geometry terms (hanging-wall, ztor, large-distance).
    #    These are site-independent, so they must be computed BEFORE the
    #    rock-reference PGA1100 — ASK08 defines PGA1100 from the full median
    #    model on Vs30=1100 rock, including these terms (Eq. 5, p. 77).
    # ------------------------------------------------------------------
    f_hw = 0.0
    if rx_km > 0 and dip < 90.0:
        Fhw = 1.0
        # T1: distance taper
        if rjb < 30.0:
            T1 = 1.0 - rjb / 30.0
        else:
            T1 = 0.0
        # T2: magnitude taper
        if mag <= 6.0:
            T2 = 0.0
        elif mag < 7.0:
            T2 = mag - 6.0
        else:
            T2 = 1.0
        # T3: Rrup/Rx taper
        T3 = 1.0  # simplified for vertical faults
        # T4: depth taper
        if ztor <= 10.0:
            T4 = 1.0 - ztor / 10.0 if ztor > 0 else 1.0
        else:
            T4 = 0.0
        # T5: dip taper
        T5 = 1.0 - (dip - 30.0) / 60.0 if dip >= 30.0 else 1.0
        f_hw = Fhw * a14 * T1 * T2 * T3 * T4 * T5

    # ------------------------------------------------------------------
    # 5. Depth-to-top-of-rupture term  (Eq. 13, p. 78)
    # ------------------------------------------------------------------
    f_ztor = a16 * min(ztor, 10.0) / 10.0

    # ------------------------------------------------------------------
    # 6. Large-distance term  (Eq. 14-15, p. 79)
    # ------------------------------------------------------------------
    f_large = 0.0
    if rrup >= 100.0:
        if mag < 5.5:
            T6 = 1.0
        elif mag <= 6.5:
            T6 = 0.5 * (6.5 - mag) + 0.5
        else:
            T6 = 0.5
        f_large = a18 * (rrup - 100.0) * T6

    f_geom = f_hw + f_ztor + f_large

    # ------------------------------------------------------------------
    # 4. Site response term  (Eq. 5-7, p. 77)
    #    Requires PGA on reference rock (Vs30=1100) first, computed from the
    #    full median model (base + style + geometry) with the linear site term
    #    for Vs30=1100 (> VLIN).
    # ------------------------------------------------------------------
    vs30_star_1100 = min(1100.0, 1500.0)  # v1=1500 for PGA
    f_site_1100 = (a10 + b_site * n) * math.log(vs30_star_1100 / VLIN)
    pga1100 = math.exp(f1 + f_style + f_geom + f_site_1100)

    # Now compute actual site term for the target Vs30
    vs30_star = min(vs30, 1500.0)  # v1 = 1500 for PGA
    if vs30 < VLIN:
        f_site = (a10 * math.log(vs30_star / VLIN)
                  - b_site * math.log(pga1100 + c)
                  + b_site * math.log(pga1100 + c * (vs30_star / VLIN) ** n))
    else:
        f_site = (a10 + b_site * n) * math.log(vs30_star / VLIN)

    # ------------------------------------------------------------------
    # Total
    # ------------------------------------------------------------------
    ln_pga = f1 + f_style + f_site + f_geom
    pga_g = math.exp(ln_pga)
    return max(pga_g, 0.0001)


def estimate_pga(mag: float, depth_km: float, dist_km: float, vs30: float = 760.0) -> float:
    """Estimate PGA using Abrahamson & Silva (2008) for crustal strike-slip.

    For extended ruptures (like the 500 km 2025 event), *dist_km* should be
    the distance to the nearest fault trace (≈ Rrup for vertical faults).
    """
    if mag < 3.0:
        return 0.0001
    return max(
        estimate_pga_ask08(mag=mag, rrup_km=dist_km, vs30=vs30, rake=180.0, dip=90.0),
        0.0001,
    )


def _load_vs30() -> dict:
    report_dir = os.environ.get("MMEQ_REPORT_DIR", "report")
    vs30_path = os.path.join(report_dir, "dam_vs30.csv")
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

        pga_median = estimate_pga_ask08(mag=m, rrup_km=rjb_km, vs30=vs30)

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


def compute_return_period(
    mag_threshold: float,
    b_value: float,
    a_value: float,
    catalog_years: float,
) -> float:
    """Return period (years) for events >= mag_threshold.

    a_value is the catalog-level Gutenberg-Richter intercept (cumulative count
    over the whole catalog), so it must be converted to an annual rate by
    subtracting log10(catalog_years) before inverting. Without this the result
    is 1 / (cumulative catalog count), which is too short by a factor of
    catalog_years.
    """
    if catalog_years <= 0:
        return float("inf")
    a_annual = a_value - math.log10(catalog_years)
    n_per_year = 10 ** (a_annual - b_value * mag_threshold)
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
            rjb_km = dist_fault if (not math.isnan(dist_fault) and dist_fault > 0) else dist_to_major
            pga = estimate_pga(major_mag, major_depth, rjb_km)

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

    # Catalog time span (years) for annualizing the Gutenberg-Richter rate.
    _t = pd.to_datetime(eq_df["time_utc"], errors="coerce").dropna()
    catalog_years = max((_t.max() - _t.min()).days / 365.25, 1.0) if len(_t) > 1 else 1.0

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

        return_period_m6 = compute_return_period(6.0, b_val, a_val, catalog_years)

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
            "return_period_m6_yrs": round(return_period_m6, 1),
        })

    risk_df = pd.DataFrame(results).sort_values("composite_risk", ascending=False).reset_index(drop=True)
    logger.info(f"Computed risk scores for {len(risk_df)} dams")
    return risk_df


def monte_carlo_pga(
    eq_df: pd.DataFrame,
    n_iterations: int = 1000,
    sigma_ln: float = 0.65,
) -> pd.DataFrame:
    """
    Monte Carlo simulation of PGA uncertainty at all dam locations.

    Propagates ASK08 aleatory uncertainty (sigma_ln) through the GMPE to produce
    probabilistic PGA distributions at each dam site.

    Returns DataFrame with dam name, lat, lon, and PGA statistics.
    """
    dams_df = _load_dams_df()
    if dams_df is None or dams_df.empty:
        return pd.DataFrame()

    fault_segments = _load_fault_segments()
    vs30_map = _load_vs30()
    major_eq = eq_df.nlargest(1, "mag").iloc[0]
    major_mag = major_eq["mag"]
    major_depth = major_eq["depth"]
    major_lat = major_eq["latitude"]
    major_lon = major_eq["longitude"]

    rng = np.random.RandomState(42)
    results = []

    for _, dam in dams_df.iterrows():
        dlat = dam["latitude"] - major_lat
        dlon = (dam["longitude"] - major_lon) * math.cos(math.radians(major_lat))
        dist_to_major = math.sqrt(dlat ** 2 + dlon ** 2) * 111.0
        dist_fault = distance_to_nearest_fault(
            dam["latitude"], dam["longitude"], fault_segments
        ) if fault_segments else float("nan")
        rjb_km = dist_fault if (not math.isnan(dist_fault) and dist_fault > 0) else dist_to_major
        vs30 = vs30_map.get(f"{dam['latitude']:.4f},{dam['longitude']:.4f}", 760.0)

        ln_pga_median = math.log(estimate_pga(major_mag, major_depth, rjb_km, vs30=vs30))

        ln_pga_samples = rng.normal(ln_pga_median, sigma_ln, n_iterations)
        pga_samples = np.exp(ln_pga_samples)

        results.append({
            "name": dam["name"],
            "latitude": dam["latitude"],
            "longitude": dam["longitude"],
            "pga_median_g": round(float(np.median(pga_samples)), 4),
            "pga_mean_g": round(float(np.mean(pga_samples)), 4),
            "pga_std_g": round(float(np.std(pga_samples)), 4),
            "pga_p05_g": round(float(np.percentile(pga_samples, 5)), 4),
            "pga_p16_g": round(float(np.percentile(pga_samples, 16)), 4),
            "pga_p50_g": round(float(np.percentile(pga_samples, 50)), 4),
            "pga_p84_g": round(float(np.percentile(pga_samples, 84)), 4),
            "pga_p95_g": round(float(np.percentile(pga_samples, 95)), 4),
            "prob_pga_gt_0.1g": round(float(np.mean(pga_samples > 0.1)), 4),
            "prob_pga_gt_0.2g": round(float(np.mean(pga_samples > 0.2)), 4),
            "prob_pga_gt_0.5g": round(float(np.mean(pga_samples > 0.5)), 4),
        })

    mc_df = pd.DataFrame(results)
    logger.info(f"Monte Carlo PGA ({n_iterations} iterations) for {len(mc_df)} dams")
    return mc_df
