"""OSM critical infrastructure exposure analysis.

Loads schools, hospitals, and other critical buildings from OpenStreetMap
(via Overpass API download) and estimates PGA at each site using the
ASK08 GMPE with rupture distance to the 2025 M7.7 Sagaing Fault trace.
"""

import json
import logging
import math
import os

import numpy as np
import pandas as pd

from .dam_risk import distance_to_nearest_fault, estimate_pga_ask08, _load_fault_segments
from .finite_fault import load_rupture_trace

logger = logging.getLogger(__name__)

_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data")
_OSM_PATH = os.path.join(_DATA_DIR, "osm", "critical_infrastructure.json")

# MMI thresholds (Wald et al. 1999 PGA-MMI relation, approximate)
PGA_THRESHOLDS = {
    "Violent (IX+)": 0.65,
    "Severe (VIII)": 0.34,
    "Very Strong (VII)": 0.18,
    "Strong (VI)": 0.092,
    "Moderate (V)": 0.039,
}


def load_osm_infrastructure() -> pd.DataFrame:
    """Load OSM critical infrastructure and extract coordinates."""
    with open(_OSM_PATH) as f:
        data = json.load(f)

    rows = []
    for e in data["elements"]:
        tags = e.get("tags", {})
        # Get coordinates: nodes have lat/lon, ways/relations have center
        if "lat" in e:
            lat, lon = e["lat"], e["lon"]
        elif "center" in e:
            lat, lon = e["center"]["lat"], e["center"]["lon"]
        else:
            continue

        amenity = tags.get("amenity", "")
        building = tags.get("building", "")
        category = amenity or building
        name = tags.get("name", tags.get("name:en", ""))

        rows.append({
            "osm_id": e["id"],
            "osm_type": e["type"],
            "lat": lat,
            "lon": lon,
            "category": category,
            "name": name,
        })
    return pd.DataFrame(rows)


def compute_building_exposure(mag: float = 7.7, vs30_default: float = 760.0) -> pd.DataFrame:
    """Compute PGA at each OSM building using ASK08 + rupture trace distance.

    Returns DataFrame with columns: osm_id, lat, lon, category, name,
    rrup_km, pga_g, mmi_label.
    """
    df = load_osm_infrastructure()
    logger.info("Loaded %d OSM infrastructure elements", len(df))

    # Load rupture trace + fault lines for best distance estimates
    trace = load_rupture_trace()
    trace_segs = [(trace[i], trace[i + 1]) for i in range(len(trace) - 1)] if len(trace) >= 2 else []
    fault_segs = _load_fault_segments()
    # Filter fault segments to Myanmar region for speed
    filtered = []
    for (lon1, lat1), (lon2, lat2) in fault_segs:
        if 15 < lat1 < 29 and 92 < lon1 < 102 and 15 < lat2 < 29 and 92 < lon2 < 102:
            filtered.append(((lon1, lat1), (lon2, lat2)))
    segments = filtered + trace_segs
    if not segments:
        logger.warning("No fault/rupture segments available, cannot compute exposure")
        return df.assign(rrup_km=float("nan"), pga_g=float("nan"), mmi_label="Unknown")

    # Compute rupture distance and PGA for each building (vectorized for speed)
    lats = df["lat"].values
    lons = df["lon"].values
    rrup_arr = np.empty(len(df))
    pga_arr = np.empty(len(df))
    for i in range(len(df)):
        rrup_arr[i] = distance_to_nearest_fault(lats[i], lons[i], segments)
        pga_arr[i] = estimate_pga_ask08(mag=mag, rrup_km=rrup_arr[i], vs30=vs30_default)

    df["rrup_km"] = rrup_arr
    df["pga_g"] = pga_arr

    # Assign MMI label
    df["mmi_label"] = df["pga_g"].apply(_pga_to_mmi_label)

    logger.info(
        "Exposure: %d buildings with PGA > 0.1g, %d > 0.3g",
        (df["pga_g"] > 0.1).sum(),
        (df["pga_g"] > 0.3).sum(),
    )
    return df


def _pga_to_mmi_label(pga: float) -> str:
    for label, threshold in PGA_THRESHOLDS.items():
        if pga >= threshold:
            return label
    return "Light (IV-)"


def exposure_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Summarize exposure by category and shaking intensity."""
    return (
        df.groupby(["category", "mmi_label"])
        .size()
        .reset_index(name="count")
        .pivot_table(index="category", columns="mmi_label", values="count", fill_value=0)
    )
