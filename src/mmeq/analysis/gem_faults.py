import json
import os

_CANDIDATES = [
    os.path.join(os.getcwd(), "data", "gem_faults", "gem_myanmar_faults.geojson"),
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "data", "gem_faults", "gem_myanmar_faults.geojson"),
]


def _geojson_path() -> str:
    for p in _CANDIDATES:
        p = os.path.normpath(p)
        if os.path.exists(p):
            return p
    raise FileNotFoundError("gem_myanmar_faults.geojson not found; checked: " + str(_CANDIDATES))


def load_gem_faults_geojson() -> dict:
    """Load the raw GEM faults GeoJSON dict."""
    with open(_geojson_path(), encoding="utf-8") as f:
        return json.load(f)


def load_gem_fault_segments() -> list:
    """Load GEM fault segments. Returns list of ((lon1,lat1),(lon2,lat2)) tuples."""
    data = load_gem_faults_geojson()
    segments = []
    for feat in data.get("features", []):
        geom = feat.get("geometry", {})
        if geom.get("type") == "LineString":
            coords = geom["coordinates"]
            for i in range(len(coords) - 1):
                segments.append((tuple(coords[i][:2]), tuple(coords[i + 1][:2])))
        elif geom.get("type") == "MultiLineString":
            for line in geom["coordinates"]:
                for i in range(len(line) - 1):
                    segments.append((tuple(line[i][:2]), tuple(line[i + 1][:2])))
    return segments
