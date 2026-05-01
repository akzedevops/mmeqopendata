import json
import os

_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data")


def load_finite_fault_patches() -> list[dict]:
    """Load USGS finite fault patches. Returns list of dicts with:
    center_lat, center_lon, slip_m, rake, moment_nm, depth_km (estimated from polygon z-coords).
    The polygon coordinates have [lon, lat, depth_km] vertices."""
    path = os.path.join(_DATA_DIR, "finite_fault", "FFM.geojson")
    with open(path) as f:
        gj = json.load(f)

    patches = []
    for feat in gj["features"]:
        coords = feat["geometry"]["coordinates"][0]  # outer ring
        lons = [c[0] for c in coords]
        lats = [c[1] for c in coords]
        depths = [c[2] for c in coords if len(c) > 2]
        p = feat["properties"]
        patches.append({
            "center_lon": sum(lons) / len(lons),
            "center_lat": sum(lats) / len(lats),
            "slip_m": p.get("slip"),
            "rake": p.get("rake"),
            "moment_nm": p.get("sf_moment"),
            "depth_km": sum(depths) / len(depths) if depths else None,
        })
    return patches


def load_rupture_trace() -> list[tuple]:
    """Load the actual surface rupture trace from data/shakemap/rupture.json.
    Returns list of (lon, lat) coordinate pairs forming the rupture trace."""
    path = os.path.join(_DATA_DIR, "shakemap", "rupture.json")
    with open(path) as f:
        data = json.load(f)

    trace = []
    features = data.get("features", [data])
    for feat in features:
        geom = feat.get("geometry") or feat
        coords = geom.get("coordinates", [])
        gtype = geom.get("type", "")
        if gtype == "MultiPolygon":
            for polygon in coords:
                for c in polygon[0]:
                    if len(c) >= 2:
                        trace.append((c[0], c[1]))
        elif gtype == "Polygon":
            for c in coords[0]:
                if len(c) >= 2:
                    trace.append((c[0], c[1]))
    return trace
