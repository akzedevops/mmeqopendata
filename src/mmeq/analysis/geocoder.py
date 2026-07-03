"""Offline reverse geocoder for Myanmar administrative divisions.

Uses geoBoundaries polygons (ADM1=state, ADM2=district, ADM3=township)
and 74K OSM place nodes for nearest place lookup with distance.
"""

import csv
import json
import logging
import math
import os
import threading

from shapely.geometry import Point, shape
from shapely.strtree import STRtree

from mmeq.config import DATA_DIR as _DATA_DIR

logger = logging.getLogger(__name__)
_ADMIN1_PATH = os.path.join(_DATA_DIR, "admin", "mmr_admin1.geojson")
_ADMIN2_PATH = os.path.join(_DATA_DIR, "admin", "mmr_admin2.geojson")
_ADMIN3_PATH = os.path.join(_DATA_DIR, "admin", "mmr_admin3.geojson")
_PLACES_PATH = os.path.join(_DATA_DIR, "osm", "myanmar_places.csv")

_admin_cache = {}  # level -> (names, tree, geoms)
_place_tree = None
_place_names = None
_place_coords = None

# cmd_export enriches from a thread pool; lazy init must be locked so a thread
# can never observe a partially-initialized loader (e.g. _place_tree set while
# _place_names is still None).
_admin_lock = threading.Lock()
_places_lock = threading.Lock()

EARTH_R = 6371.0  # km


def _load_admin(level, path):
    if level in _admin_cache:
        return _admin_cache[level]
    with _admin_lock:
        if level in _admin_cache:  # another thread loaded it while we waited
            return _admin_cache[level]
        if not os.path.exists(path):
            logger.warning("Admin boundaries not found: %s", path)
            _admin_cache[level] = ([], None, [])
            return _admin_cache[level]
        with open(path) as f:
            data = json.load(f)
        names = []
        geoms = []
        for feat in data["features"]:
            names.append(feat["properties"]["shapeName"])
            geoms.append(shape(feat["geometry"]))
        tree = STRtree(geoms)
        _admin_cache[level] = (names, tree, geoms)
        return _admin_cache[level]


def _query_admin(lat, lon, level, path):
    names, tree, geoms = _load_admin(level, path)
    if tree is None:
        return ""
    pt = Point(lon, lat)
    idx = tree.query(pt)
    for i in idx:
        if geoms[i].contains(pt):
            return names[i]
    return ""


def _load_places():
    global _place_tree, _place_names, _place_coords
    if _place_tree is not None:
        return
    with _places_lock:
        if _place_tree is not None:  # another thread loaded it while we waited
            return
        if not os.path.exists(_PLACES_PATH):
            logger.warning("Places file not found: %s", _PLACES_PATH)
            _place_names = _place_coords = None
            _place_tree = None
            return

        from scipy.spatial import cKDTree

        coords = []
        names = []
        latlons = []
        with open(_PLACES_PATH, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                lat, lon = float(row["lat"]), float(row["lon"])
                name = row["name_en"] or row["name"]
                rlat, rlon = math.radians(lat), math.radians(lon)
                coords.append((math.cos(rlat) * math.cos(rlon),
                               math.cos(rlat) * math.sin(rlon),
                               math.sin(rlat)))
                names.append((name, row["place"]))
                latlons.append((lat, lon))

        tree = cKDTree(coords)
        # Assign the guard variable (_place_tree) LAST: unlocked fast-path
        # readers that pass the guard must see fully-populated companions.
        _place_names = names
        _place_coords = latlons
        _place_tree = tree
        logger.info("Loaded %d OSM places for geocoding", len(names))


def _haversine_km(lat1, lon1, lat2, lon2):
    rlat1, rlon1 = math.radians(lat1), math.radians(lon1)
    rlat2, rlon2 = math.radians(lat2), math.radians(lon2)
    dlat, dlon = rlat2 - rlat1, rlon2 - rlon1
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return EARTH_R * 2 * math.asin(min(1, math.sqrt(a)))


def get_state(lat, lon):
    return _query_admin(lat, lon, 1, _ADMIN1_PATH)


def get_district(lat, lon):
    return _query_admin(lat, lon, 2, _ADMIN2_PATH)


def get_township(lat, lon):
    return _query_admin(lat, lon, 3, _ADMIN3_PATH)


def get_nearest_place(lat, lon):
    """Return (name, place_type, distance_km) of nearest OSM place."""
    _load_places()
    if _place_tree is None:
        return ("", "", 0.0)
    rlat, rlon = math.radians(lat), math.radians(lon)
    xyz = (math.cos(rlat) * math.cos(rlon),
           math.cos(rlat) * math.sin(rlon),
           math.sin(rlat))
    _, idx = _place_tree.query(xyz)
    name, ptype = _place_names[idx]
    plat, plon = _place_coords[idx]
    dist = _haversine_km(lat, lon, plat, plon)
    return (name, ptype, round(dist, 1))


def enrich_dataframe(df):
    """Add state_region, district, township, nearest_city, place_type, distance_km."""
    _load_places()
    states, districts, townships = [], [], []
    cities, ptypes, dists = [], [], []
    for _, row in df.iterrows():
        lat, lon = row["latitude"], row["longitude"]
        states.append(get_state(lat, lon))
        districts.append(get_district(lat, lon))
        townships.append(get_township(lat, lon))
        name, pt, d = get_nearest_place(lat, lon)
        cities.append(name)
        ptypes.append(pt)
        dists.append(d)
    df["state_region"] = states
    df["district"] = districts
    df["township"] = townships
    df["nearest_city"] = cities
    df["place_type"] = ptypes
    df["distance_km"] = dists
    return df
