"""Compare instrumental horizontal PGA with the event-specific AS08 scenario."""
import json
import logging
import math
import os

import pandas as pd

from mmeq.analysis.dam_risk import estimate_pga_ask08
from mmeq.config import DATA_DIR

logger = logging.getLogger(__name__)
_SHAKEMAP_PATH = os.path.join(DATA_DIR, "shakemap", "stationlist.json")


def _residual_and_ratio(pga_obs: float, pga_pred: float):
    if not all(math.isfinite(value) and value > 0 for value in (pga_obs, pga_pred)):
        return None, None
    return round(math.log(pga_obs / pga_pred), 4), round(pga_obs / pga_pred, 4)


def _horizontal_pga(channels: list):
    """Geometric mean of a clean orthogonal pair; not rotation-derived GMRotI50."""
    components = {}
    for channel in channels:
        name = channel.get("name", "").upper()
        if not name or name[-1] not in "12NE":
            continue
        for amplitude in channel.get("amplitudes", []):
            if (amplitude.get("name") != "pga" or amplitude.get("units") != "%g"
                    or str(amplitude.get("flag")) != "0"):
                continue
            try:
                value = float(amplitude["value"]) / 100.0
            except (KeyError, TypeError, ValueError):
                continue
            if math.isfinite(value) and value > 0:
                components.setdefault(name[:-1], {})[name[-1]] = value
    for prefix in sorted(components):
        values = components[prefix]
        for first, second in (("1", "2"), ("N", "E")):
            if first in values and second in values:
                return math.sqrt(values[first] * values[second])
    return None


def load_shakemap_stations(shakemap_path: str = None) -> list[dict]:
    """Load unflagged horizontal recordings and USGS event-specific metadata."""
    with open(shakemap_path or _SHAKEMAP_PATH, encoding="utf-8") as source:
        data = json.load(source)
    stations = []
    for feature in data.get("features", []):
        props = feature["properties"]
        if props.get("station_type") != "seismic":
            continue
        pga = _horizontal_pga(props.get("channels", []))
        if pga is None:
            logger.warning("Skipping %s: no clean horizontal PGA pair", props.get("code"))
            continue
        try:
            distances = props["distances"]
            rrup = float(distances["rrup"])
            rjb = float(distances["rjb"])
            vs30 = float(props["vs30"])
            if not all(math.isfinite(value) for value in (rrup, rjb, vs30)):
                raise ValueError("nonfinite metadata")
            if min(rrup, rjb) < 0 or vs30 <= 0:
                raise ValueError("invalid metadata")
        except (KeyError, TypeError, ValueError):
            logger.warning("Skipping %s: missing/invalid rupture distances or Vs30", props.get("code"))
            continue
        longitude, latitude = feature["geometry"]["coordinates"][:2]
        stations.append({
            "code": props["code"], "name": props.get("name", ""),
            "lat": latitude, "lon": longitude, "pga_g": pga,
            "rrup_km": rrup, "rjb_km": rjb, "vs30": vs30,
        })
    logger.info("Loaded %d stations with clean horizontal PGA pairs", len(stations))
    return stations


def validate_against_shakemap(
    mag: float = 7.7,
    shakemap_path: str = None,
    dam_risk_path: str = None,
) -> pd.DataFrame:
    """Station-only diagnostic; dam model outputs are not observations.

    ``dam_risk_path`` is accepted for compatibility but is deliberately not read.
    Horizontal peak geometric means approximate, but do not reproduce, GMRotI50.
    """
    if dam_risk_path:
        logger.warning("Dam predictions are not observations; excluding them from station validation")
    rows = []
    for station in load_shakemap_stations(shakemap_path):
        predicted = estimate_pga_ask08(
            mag=mag, rrup_km=station["rrup_km"], rjb_km=station["rjb_km"], vs30=station["vs30"],
        )
        residual, ratio = _residual_and_ratio(station["pga_g"], predicted)
        rows.append({
            "station_code": station["code"], "name": station["name"],
            "lat": station["lat"], "lon": station["lon"],
            "dist_fault_km": station["rrup_km"], "rrup_km": station["rrup_km"],
            "rjb_km": station["rjb_km"], "vs30": station["vs30"],
            "pga_observed_g": station["pga_g"], "pga_predicted_g": predicted,
            "residual_ln": residual, "ratio": ratio,
            "within_distance_range": station["rrup_km"] <= 200,
        })
    columns = ["station_code", "name", "lat", "lon", "dist_fault_km", "rrup_km", "rjb_km", "vs30",
               "pga_observed_g", "pga_predicted_g", "residual_ln", "ratio", "within_distance_range"]
    return pd.DataFrame(rows, columns=columns)
