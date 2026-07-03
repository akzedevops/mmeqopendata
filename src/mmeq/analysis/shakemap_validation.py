"""Validate ASK08 GMPE predictions against USGS ShakeMap station recordings."""
import json
import logging
import math
import os

import numpy as np
import pandas as pd

from mmeq.analysis.dam_risk import _load_fault_segments, distance_to_nearest_fault, estimate_pga_ask08
from mmeq.config import DATA_DIR

logger = logging.getLogger(__name__)

# Default ShakeMap station list, resolved against the repo data/ dir (config.DATA_DIR,
# overridable via MMEQ_DATA_DIR) so it works regardless of the current directory.
_SHAKEMAP_PATH = os.path.join(DATA_DIR, "shakemap", "stationlist.json")


def _residual_and_ratio(pga_obs: float, pga_pred: float):
    """Return (residual_ln, ratio), guarding against zero/negative PGA.

    A station can report a 0 amplitude (it passes the ``value is not None``
    filter) and ``estimate_pga_ask08`` is floored at a tiny value, so log/division
    must be guarded or the whole validation crashes on a single bad row.
    """
    if pga_obs <= 0 or pga_pred <= 0:
        return None, None
    return round(math.log(pga_obs / pga_pred), 4), round(pga_obs / pga_pred, 4)


def _load_all_fault_segments() -> list:
    """Load fault segments from rupture trace, GEM faults, and plate boundary faults."""
    segments = []
    # 1. Actual rupture trace (best for this event)
    try:
        from mmeq.analysis.finite_fault import load_rupture_trace
        trace = load_rupture_trace()
        for i in range(len(trace) - 1):
            segments.append((trace[i], trace[i + 1]))
    except Exception:
        pass
    # 2. GEM active faults
    try:
        from mmeq.analysis.gem_faults import load_gem_fault_segments
        segments.extend(load_gem_fault_segments())
    except Exception:
        pass
    # 3. Fallback to plate boundary faults
    if not segments:
        segments = _load_fault_segments()
    return segments


def load_shakemap_stations(shakemap_path: str = None) -> list[dict]:
    """Return stations with actual PGA recordings (pga_g in g)."""
    path = shakemap_path or _SHAKEMAP_PATH
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    stations = []
    for feat in data.get("features", []):
        props = feat["properties"]
        for ch in props.get("channels", []):
            pga_amp = next(
                (a for a in ch.get("amplitudes", [])
                 if a.get("name") == "pga" and a.get("value") is not None),
                None,
            )
            if pga_amp:
                coords = feat["geometry"]["coordinates"]
                stations.append({
                    "code": props["code"],
                    "name": props.get("name", ""),
                    "lat": coords[1],
                    "lon": coords[0],
                    "pga_g": pga_amp["value"] / 100.0,  # %g → g
                })
                break  # one entry per station

    logger.info("Loaded %d stations with PGA recordings", len(stations))
    return stations


def validate_against_shakemap(
    mag: float = 7.7,
    shakemap_path: str = None,
    dam_risk_path: str = None,
) -> pd.DataFrame:
    """Compare ASK08 predicted PGA vs ShakeMap-observed PGA at seismic stations.

    ``shakemap_path`` defaults to the repo station list (``config.DATA_DIR``).
    If ``dam_risk_path`` is given and points to a readable ``dam_risk_scores.csv``
    with the expected columns, dam-site rows are appended.

    Returns DataFrame with columns:
        station_code, name, lat, lon, dist_fault_km,
        pga_observed_g, pga_predicted_g, residual_ln, ratio
    """
    segments = _load_all_fault_segments()

    rows = []

    # --- seismic stations ---
    for st in load_shakemap_stations(shakemap_path):
        dist_km = distance_to_nearest_fault(st["lat"], st["lon"], segments)
        pga_pred = estimate_pga_ask08(mag=mag, rrup_km=dist_km, rjb_km=dist_km)
        pga_obs = st["pga_g"]
        residual_ln, ratio = _residual_and_ratio(pga_obs, pga_pred)
        rows.append({
            "station_code": st["code"],
            "name": st["name"],
            "lat": st["lat"],
            "lon": st["lon"],
            "dist_fault_km": round(dist_km, 2),
            "pga_observed_g": round(pga_obs, 5),
            "pga_predicted_g": round(pga_pred, 5),
            "residual_ln": residual_ln,
            "ratio": ratio,
        })

    # --- dam sites (ShakeMap-interpolated PGA from dam_risk_scores.csv) ---
    if dam_risk_path and os.path.exists(dam_risk_path):
        dams = pd.read_csv(dam_risk_path)
        # expected columns: name, latitude, longitude, pga_g (ShakeMap interpolated),
        # dist_to_fault_km (the rupture distance dam_risk.py writes)
        needed = {"name", "latitude", "longitude", "pga_g", "dist_to_fault_km"}
        if needed.issubset(dams.columns):
            for _, row in dams.iterrows():
                pga_obs = row["pga_g"]
                dist_km = row["dist_to_fault_km"]
                pga_pred = estimate_pga_ask08(mag=mag, rrup_km=dist_km, rjb_km=dist_km)
                residual_ln, ratio = _residual_and_ratio(pga_obs, pga_pred)
                rows.append({
                    "station_code": "DAM",
                    "name": row["name"],
                    "lat": row["latitude"],
                    "lon": row["longitude"],
                    "dist_fault_km": round(dist_km, 2),
                    "pga_observed_g": round(pga_obs, 5),
                    "pga_predicted_g": round(pga_pred, 5),
                    "residual_ln": residual_ln,
                    "ratio": ratio,
                })
            logger.info("Appended %d dam-site rows from %s", len(dams), dam_risk_path)
        else:
            logger.warning("dam_risk_scores.csv missing expected columns; skipping dam rows")
    else:
        logger.debug("No dam risk scores found at %s", dam_risk_path)

    df = pd.DataFrame(rows)
    logger.info("Validation complete: %d rows", len(df))
    return df
