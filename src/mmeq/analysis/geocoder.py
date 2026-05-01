"""Offline reverse geocoder for Myanmar states/regions.

Uses geoBoundaries ADM1 polygons to map lat/lon to state/region name.
Uses OSM place nodes (74K villages/towns/cities) for nearest place lookup.
"""

import csv
import json
import logging
import math
import os

from shapely.geometry import Point, shape

logger = logging.getLogger(__name__)

_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data")
_ADMIN_PATH = os.path.join(_DATA_DIR, "admin", "mmr_admin1.geojson")
_PLACES_PATH = os.path.join(_DATA_DIR, "osm", "myanmar_places.csv")

_admin_shapes = None
_place_tree = None
_place_names = None


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


def _load_places():
    """Load OSM places and build a KD-tree for fast nearest-neighbor lookup."""
    global _place_tree, _place_names
    if _place_tree is not None:
        return

    if not os.path.exists(_PLACES_PATH):
        logger.warning("Places file not found: %s", _PLACES_PATH)
        _place_tree = None
        _place_names = []
        return

    import numpy as np
    from scipy.spatial import cKDTree

    coords = []
    names = []
    with open(_PLACES_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            lat = float(row["lat"])
            lon = float(row["lon"])
            name = row["name_en"] or row["name"]
            place_type = row["place"]
            coords.append((lat, lon))
            names.append((name, place_type))

    # Convert to 3D cartesian for accurate nearest-neighbor on a sphere
    arr = []
    for lat, lon in coords:
        rlat, rlon = math.radians(lat), math.radians(lon)
        arr.append((math.cos(rlat) * math.cos(rlon), math.cos(rlat) * math.sin(rlon), math.sin(rlat)))

    _place_tree = cKDTree(arr)
    _place_names = names
    logger.info("Loaded %d OSM places for geocoding", len(names))


def get_state(lat: float, lon: float) -> str:
    """Return Myanmar state/region name for a lat/lon point, or empty string."""
    admin = _load_admin()
    pt = Point(lon, lat)
    for name, polygon in admin:
        if polygon.contains(pt):
            return name
    return ""


def get_nearest_place(lat: float, lon: float) -> tuple:
    """Return (name, place_type) of the nearest OSM place node."""
    _load_places()
    if _place_tree is None:
        return ("", "")
    rlat = math.radians(lat)
    rlon = math.radians(lon)
    xyz = (math.cos(rlat) * math.cos(rlon), math.cos(rlat) * math.sin(rlon), math.sin(rlat))
    _, idx = _place_tree.query(xyz)
    return _place_names[idx]


def enrich_dataframe(df):
    """Add 'state_region', 'nearest_city', and 'place_type' columns."""
    _load_admin()
    _load_places()

    states = []
    cities = []
    place_types = []
    for _, row in df.iterrows():
        lat, lon = row["latitude"], row["longitude"]
        states.append(get_state(lat, lon))
        name, ptype = get_nearest_place(lat, lon)
        cities.append(name)
        place_types.append(ptype)

    df["state_region"] = states
    df["nearest_city"] = cities
    df["place_type"] = place_types
    return df
