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

from mmeq.config import DATA_DIR as _DATA_DIR

from .dam_risk import distance_to_nearest_fault, estimate_pga_ask08, _load_fault_segments
from .finite_fault import load_rupture_trace

logger = logging.getLogger(__name__)
_OSM_PATH = os.path.join(_DATA_DIR, "osm", "critical_infrastructure.json")
_VS30_GRID_PATH = os.path.join(_DATA_DIR, "shakemap", "vs30_grid.csv")

# MMI thresholds (Wald et al. 1999 PGA-MMI relation, approximate)
PGA_THRESHOLDS = {
    "Violent (IX+)": 0.65,
    "Severe (VIII)": 0.34,
    "Very Strong (VII)": 0.18,
    "Strong (VI)": 0.092,
    "Moderate (V)": 0.039,
}

# HAZUS casualty rates per indoor occupant by MMI (FEMA 2003, Table 13-3)
HAZUS_CASUALTY = {
    "Violent (IX+)": {"injury": 0.03, "hospital": 0.0012, "fatality": 0.00001},
    "Severe (VIII)": {"injury": 0.003, "hospital": 0.00012, "fatality": 0.000001},
    "Very Strong (VII)": {"injury": 0.0003, "hospital": 0.000012, "fatality": 0.0},
    "Strong (VI)": {"injury": 0.00005, "hospital": 0.000001, "fatality": 0.0},
    "Moderate (V)": {"injury": 0.0, "hospital": 0.0, "fatality": 0.0},
    "Light (IV-)": {"injury": 0.0, "hospital": 0.0, "fatality": 0.0},
}

# HAZUS building damage state probabilities by MMI (averaged across types)
HAZUS_DAMAGE = {
    "Violent (IX+)": {"none": 0.03, "slight": 0.15, "moderate": 0.30, "extensive": 0.30, "complete": 0.22},
    "Severe (VIII)": {"none": 0.15, "slight": 0.35, "moderate": 0.30, "extensive": 0.15, "complete": 0.05},
    "Very Strong (VII)": {"none": 0.50, "slight": 0.30, "moderate": 0.15, "extensive": 0.04, "complete": 0.01},
    "Strong (VI)": {"none": 0.80, "slight": 0.15, "moderate": 0.04, "extensive": 0.01, "complete": 0.0},
    "Moderate (V)": {"none": 0.95, "slight": 0.04, "moderate": 0.01, "extensive": 0.0, "complete": 0.0},
    "Light (IV-)": {"none": 1.0, "slight": 0.0, "moderate": 0.0, "extensive": 0.0, "complete": 0.0},
}

_vs30_tree = None
_vs30_values = None


def _load_vs30_grid():
    """Load USGS ShakeMap Vs30 grid and build KD-tree for nearest lookup."""
    global _vs30_tree, _vs30_values
    if _vs30_tree is not None:
        return
    if not os.path.exists(_VS30_GRID_PATH):
        _vs30_tree = _vs30_values = None
        return
    from scipy.spatial import cKDTree
    import math as m
    data = np.loadtxt(_VS30_GRID_PATH, delimiter=",", skiprows=1)
    lons, lats, vs30s = data[:, 0], data[:, 1], data[:, 2]
    xyz = np.column_stack([
        np.cos(np.radians(lats)) * np.cos(np.radians(lons)),
        np.cos(np.radians(lats)) * np.sin(np.radians(lons)),
        np.sin(np.radians(lats)),
    ])
    _vs30_tree = cKDTree(xyz)
    _vs30_values = vs30s


def _get_vs30(lat, lon, default=760.0):
    _load_vs30_grid()
    if _vs30_tree is None:
        return default
    rlat, rlon = math.radians(lat), math.radians(lon)
    xyz = (math.cos(rlat) * math.cos(rlon), math.cos(rlat) * math.sin(rlon), math.sin(rlat))
    _, idx = _vs30_tree.query(xyz)
    return float(_vs30_values[idx])


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

    # Compute rupture distance and PGA for each building with site-specific Vs30
    lats = df["lat"].values
    lons = df["lon"].values
    rrup_arr = np.empty(len(df))
    pga_arr = np.empty(len(df))
    vs30_arr = np.empty(len(df))
    for i in range(len(df)):
        rrup_arr[i] = distance_to_nearest_fault(lats[i], lons[i], segments)
        vs30_arr[i] = _get_vs30(lats[i], lons[i], vs30_default)
        pga_arr[i] = estimate_pga_ask08(mag=mag, rrup_km=rrup_arr[i], vs30=vs30_arr[i])

    df["rrup_km"] = rrup_arr
    df["vs30"] = vs30_arr
    df["pga_g"] = pga_arr

    # Assign MMI label
    df["mmi_label"] = df["pga_g"].apply(_pga_to_mmi_label)

    # HAZUS loss estimation
    df["damage_state"] = df["mmi_label"].map(
        lambda m: max(HAZUS_DAMAGE.get(m, {}), key=HAZUS_DAMAGE.get(m, {"none": 1}).get, default="none")
    )

    logger.info(
        "Exposure: %d buildings with PGA > 0.1g, %d > 0.3g, Vs30 range: %.0f-%.0f",
        (df["pga_g"] > 0.1).sum(),
        (df["pga_g"] > 0.3).sum(),
        vs30_arr.min(), vs30_arr.max(),
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


def estimate_losses(df: pd.DataFrame, occupants_per_building: int = 50) -> dict:
    """Estimate casualties and damage using HAZUS ratios.

    Args:
        df: Building exposure DataFrame with mmi_label column
        occupants_per_building: Average indoor occupants per building

    Returns:
        dict with injury, hospitalized, fatality counts and damage state counts
    """
    total_injuries = 0
    total_hospital = 0
    total_fatalities = 0
    damage_counts = {"none": 0, "slight": 0, "moderate": 0, "extensive": 0, "complete": 0}

    for mmi, group in df.groupby("mmi_label"):
        n = len(group)
        cas = HAZUS_CASUALTY.get(mmi, {})
        total_injuries += n * occupants_per_building * cas.get("injury", 0)
        total_hospital += n * occupants_per_building * cas.get("hospital", 0)
        total_fatalities += n * occupants_per_building * cas.get("fatality", 0)

        dmg = HAZUS_DAMAGE.get(mmi, {"none": 1})
        for state, prob in dmg.items():
            damage_counts[state] += int(n * prob)

    return {
        "injuries": int(total_injuries),
        "hospitalized": int(total_hospital),
        "fatalities": int(total_fatalities),
        "damage": damage_counts,
    }
