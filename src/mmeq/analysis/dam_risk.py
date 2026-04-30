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


def estimate_pga(mag: float, depth_km: float, dist_km: float) -> float:
    r_hypo = math.sqrt(dist_km ** 2 + depth_km ** 2)
    if r_hypo < 1:
        r_hypo = 1
    log_pga = (
        -1.715
        + 0.500 * mag
        - 0.530 * math.log10(r_hypo)
        - 0.00260 * r_hypo
    )
    return 10 ** log_pga * 9.81


def compute_return_period(mag_threshold: float, b_value: float, a_value: float, years: float = 50) -> float:
    n_per_year = 10 ** (a_value - b_value * mag_threshold)
    if n_per_year <= 0:
        return float("inf")
    return 1.0 / n_per_year


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

        pga = estimate_pga(major_mag, major_depth, dist_to_major)

        nearby = eq_df[
            ((eq_df["latitude"] - dam["latitude"]).abs() < 1.0)
            & ((eq_df["longitude"] - dam["longitude"]).abs() < 1.0)
        ]
        if len(nearby) == 0:
            nearby = eq_df

        seismic_score = min(10, pga / 0.15 * 10)
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
