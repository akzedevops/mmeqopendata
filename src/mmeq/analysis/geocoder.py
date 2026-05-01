"""Offline reverse geocoder for Myanmar states/regions.

Uses geoBoundaries ADM1 polygons to map lat/lon to state/region name.
Also finds the nearest populated place name from a built-in list.
"""

import json
import logging
import os

from shapely.geometry import Point, shape

logger = logging.getLogger(__name__)

_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data")
_ADMIN_PATH = os.path.join(_DATA_DIR, "admin", "mmr_admin1.geojson")

# Major cities/towns with approximate coordinates for nearest-place lookup
_PLACES = [
    ("Mandalay", 21.97, 96.08, "Mandalay"),
    ("Naypyidaw", 19.76, 96.07, "Naypyidaw"),
    ("Yangon", 16.87, 96.20, "Yangon"),
    ("Sagaing", 21.88, 95.98, "Saigang"),
    ("Meiktila", 20.88, 95.86, "Mandalay"),
    ("Bagan", 21.17, 94.87, "Mandalay"),
    ("Monywa", 21.91, 95.13, "Saigang"),
    ("Myitkyina", 25.38, 97.40, "Kachin"),
    ("Taunggyi", 20.78, 97.04, "Shan"),
    ("Lashio", 22.93, 97.75, "Shan"),
    ("Mawlamyine", 16.49, 97.63, "Mon"),
    ("Pathein", 16.78, 94.73, "Ayeyarwady"),
    ("Sittwe", 20.15, 92.90, "Rakhine"),
    ("Hakha", 21.97, 93.61, "Chin"),
    ("Loikaw", 19.67, 97.21, "Kayah"),
    ("Hpa-An", 16.89, 97.63, "Kayin"),
    ("Dawei", 14.08, 98.20, "Tanitharyi"),
    ("Bago", 17.34, 96.48, "Bago"),
    ("Magway", 20.15, 94.93, "Magway"),
    ("Pyay", 18.82, 95.22, "Bago"),
    ("Pakokku", 21.33, 95.10, "Magway"),
    ("Myingyan", 21.46, 95.39, "Mandalay"),
    ("Mogok", 22.92, 96.51, "Mandalay"),
    ("Kengtung", 21.29, 99.61, "Shan"),
    ("Kalay", 23.19, 94.07, "Saigang"),
    ("Shwebo", 22.57, 95.70, "Saigang"),
    ("Pyin Oo Lwin", 22.03, 96.47, "Mandalay"),
    ("Nay Pyi Taw", 19.76, 96.13, "Naypyidaw"),
    ("Taungoo", 18.94, 96.43, "Bago"),
    ("Bhamo", 24.25, 97.23, "Kachin"),
    ("Putao", 27.33, 97.42, "Kachin"),
    ("Hsipaw", 22.62, 97.30, "Shan"),
    ("Inle Lake", 20.58, 96.91, "Shan"),
]

_regions = None
_admin_shapes = None


def _load_admin():
    global _admin_shapes
    if _admin_shapes is not None:
        return _admin_shapes
    if not os.path.exists(_ADMIN_PATH):
        logger.warning("Admin boundaries not found: %s", _ADMIN_PATH)
        _admin_shapes = []
        return _admin_shapes
    with open(_ADMIN_PATH) as f:
        data = json.load(f)
    _admin_shapes = [
        (feat["properties"]["shapeName"], shape(feat["geometry"]))
        for feat in data["features"]
    ]
    return _admin_shapes


def get_state(lat: float, lon: float) -> str:
    """Return Myanmar state/region name for a lat/lon point, or empty string."""
    admin = _load_admin()
    pt = Point(lon, lat)
    for name, polygon in admin:
        if polygon.contains(pt):
            return name
    return ""


def get_nearest_place(lat: float, lon: float) -> str:
    """Return the name of the nearest major town/city."""
    import math
    best_dist = float("inf")
    best_name = ""
    cos_lat = math.cos(math.radians(lat))
    for name, plat, plon, _ in _PLACES:
        dx = (lon - plon) * cos_lat
        dy = lat - plat
        d = dx * dx + dy * dy
        if d < best_dist:
            best_dist = d
            best_name = name
    return best_name


def enrich_dataframe(df):
    """Add 'state_region' and 'nearest_city' columns to a quake DataFrame."""
    import math
    admin = _load_admin()
    states = []
    cities = []
    for _, row in df.iterrows():
        lat, lon = row["latitude"], row["longitude"]
        states.append(get_state(lat, lon))
        cities.append(get_nearest_place(lat, lon))
    df["state_region"] = states
    df["nearest_city"] = cities
    return df
