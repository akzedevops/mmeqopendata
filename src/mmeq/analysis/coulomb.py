"""
Coulomb stress transfer modeling for the 2025 Mw 7.7 Sagaing Fault rupture.

Computes static Coulomb failure stress change (ΔCFS) using the point-source
double-couple summation over fault patches, following Aki & Richards (2002).

Each fault segment is divided into rectangular patches. For each patch, the
static stress at the observation point is computed using the far-field
double-couple radiation pattern, then summed to get the total Coulomb stress.

References:
    Aki, K., & Richards, P. G. (2002). Quantitative Seismology (2nd ed.).
    King, G. C. P., Stein, R. S., & Lin, J. (1994). Static stress changes
    and the triggering of earthquakes. Bull. Seism. Soc. Am., 84(3), 935-953.
    Toda, S., et al. (1998). Stress transferred by the 1995 Kobe earthquake.
    J. Geophys. Res., 103(B10), 24543-24565.
"""

import json
import logging
import math
import os
from typing import Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)

MU_EL = 30e9
FRICTION_COEFF = 0.4

SAGAING_FAULT_TRACE = [
    (96.4803, 14.4000), (96.4537, 14.9093), (96.4061, 15.1355),
    (96.4193, 15.3962), (96.4472, 16.0034), (96.4697, 16.6438),
    (96.4661, 17.4351), (96.4207, 17.9816), (96.3414, 18.7048),
    (96.2254, 19.4119), (96.0158, 20.0399), (95.9427, 20.9001),
    (95.9183, 21.6810), (95.9181, 22.3522), (95.9093, 23.0150),
    (95.9691, 23.7605), (96.1173, 24.4801), (96.3104, 25.1336),
    (96.5813, 25.6469), (96.9056, 26.2874), (97.1304, 26.8202),
    (97.3465, 27.2411), (97.4711, 27.7024), (97.3484, 28.0826),
]

RUPTURE_SOUTH_LAT = 19.0
RUPTURE_NORTH_LAT = 24.5
RUPTURE_DEPTH_KM = 15.0
EPICENTER_LAT = 22.001
EPICENTER_LON = 95.925
EPICENTER_MAG = 7.7


def _compute_strike_angle(lon1, lat1, lon2, lat2):
    """Compute strike angle in radians (clockwise from north)."""
    dl = (lon2 - lon1) * math.cos(math.radians((lat1 + lat2) / 2))
    da = lat2 - lat1
    return math.atan2(dl, da)


def _ll_to_meters(lat, lon, origin_lat, origin_lon):
    x = (lon - origin_lon) * math.cos(math.radians(origin_lat)) * 111000.0
    y = (lat - origin_lat) * 111000.0
    return x, y


def build_rupture_segments(
    fault_trace=None,
    south_lat=None,
    north_lat=None,
    depth_km=15.0,
    mw=7.7,
    mu_pa=30e9,
):
    """Build rectangular fault segments from the Sagaing Fault trace."""
    if fault_trace is None:
        fault_trace = SAGAING_FAULT_TRACE
    if south_lat is None:
        south_lat = RUPTURE_SOUTH_LAT
    if north_lat is None:
        north_lat = RUPTURE_NORTH_LAT

    mo = 10 ** (1.5 * mw + 9.05)

    rupture_points = [(lon, lat) for lon, lat in fault_trace if south_lat <= lat <= north_lat]
    if len(rupture_points) < 2:
        logger.warning("Not enough rupture points in fault trace")
        return []

    total_length_m = 0.0
    segments = []

    for i in range(len(rupture_points) - 1):
        lon1, lat1 = rupture_points[i]
        lon2, lat2 = rupture_points[i + 1]

        seg_len_m = math.sqrt(
            ((lon2 - lon1) * math.cos(math.radians((lat1 + lat2) / 2)) * 111000) ** 2
            + ((lat2 - lat1) * 111000) ** 2
        )
        if seg_len_m < 100:
            continue

        strike = _compute_strike_angle(lon1, lat1, lon2, lat2)
        total_length_m += seg_len_m

        segments.append({
            "center_lat": (lat1 + lat2) / 2,
            "center_lon": (lon1 + lon2) / 2,
            "strike_rad": strike,
            "length_m": seg_len_m,
            "start_lat": lat1,
            "start_lon": lon1,
            "end_lat": lat2,
            "end_lon": lon2,
        })

    slip_m = mo / (mu_pa * total_length_m * depth_km * 1000) if total_length_m > 0 else 0
    for s in segments:
        s["slip_m"] = slip_m
        s["depth_km"] = depth_km

    logger.info(
        f"Built {len(segments)} rupture segments: "
        f"L={total_length_m / 1000:.0f}km, slip={slip_m:.2f}m"
    )
    return segments


def _patch_coulomb_stress(
    obs_x, obs_y, patch_x, patch_y, patch_depth, patch_strike,
    patch_moment, patch_length,
):
    """
    Compute Coulomb stress at a surface observation point from a fault patch.

    Uses the static double-couple Green's function from Aki & Richards (2002).
    For a vertical right-lateral strike-slip source:

        σ_yz = Mo/(4π r³) * [3 sin(2θ) cos(φ)]

    where r is distance, θ is angle from strike, φ is takeoff angle.

    For surface observations (φ=π/2), and resolving onto receiver faults
    parallel to the source (Coulomb stress):

        ΔCFS ≈ Mo/(4π) * 3 * sin(2α) / r³

    where α is the angle between the fault strike and the observation direction,
    and r is the 3D distance from the patch center.

    Parameters
    ----------
    obs_x, obs_y : observation point in local Cartesian meters
    patch_x, patch_y : patch center in local Cartesian meters
    patch_depth : depth to patch center (meters)
    patch_strike : fault strike in radians (CW from north)
    patch_moment : seismic moment of patch (N·m)
    patch_length : along-strike length of patch (meters)

    Returns
    -------
    float - Coulomb stress change in Pa
    """
    dx = obs_x - patch_x
    dy = obs_y - patch_y

    r_sq = dx * dx + dy * dy + patch_depth * patch_depth
    r = math.sqrt(r_sq)
    if r < 1000.0:
        r = 1000.0
        r_sq = r * r

    cos_s = math.sin(patch_strike)
    sin_s = math.cos(patch_strike)

    along = dx * cos_s + dy * sin_s
    perp = dx * sin_s - dy * cos_s

    r_perp_sq = perp * perp + patch_depth * patch_depth
    if r_perp_sq < 1.0:
        r_perp_sq = 1.0

    sin2alpha = 2.0 * along * math.sqrt(r_perp_sq) / r_sq

    cos2alpha = (along * along - r_perp_sq) / r_sq

    tau = patch_moment / (4.0 * math.pi * r_sq * r) * 3.0 * sin2alpha

    sigma_n = -patch_moment / (4.0 * math.pi * r_sq * r) * 3.0 * cos2alpha

    dcfs = tau - FRICTION_COEFF * sigma_n

    return dcfs


def compute_coulomb_at_points(
    obs_lats, obs_lons, segments=None, receiver_strike=None
):
    """
    Compute Coulomb stress change at observation points from the rupture.

    Divides each fault segment into patches and sums the point-source
    double-couple stress contributions from each patch.

    Returns array of ΔCFS values in MPa.
    """
    if segments is None:
        segments = build_rupture_segments()

    obs_lats = np.asarray(obs_lats, dtype=float)
    obs_lons = np.asarray(obs_lons, dtype=float)
    n = len(obs_lats)

    origin_lat = np.mean([s["center_lat"] for s in segments])
    origin_lon = np.mean([s["center_lon"] for s in segments])

    obs_x_m = np.array([
        _ll_to_meters(la, lo, origin_lat, origin_lon)[0] for la, lo in zip(obs_lats, obs_lons)
    ])
    obs_y_m = np.array([
        _ll_to_meters(la, lo, origin_lat, origin_lon)[1] for la, lo in zip(obs_lats, obs_lons)
    ])

    dcfs = np.zeros(n)

    for seg in segments:
        seg_len = seg["length_m"]
        depth = seg["depth_km"] * 1000
        slip = seg["slip_m"]
        strike = seg["strike_rad"]

        n_along = max(2, int(seg_len / 20000))
        n_depth = max(2, int(depth / 5000))

        d_along = seg_len / n_along
        d_depth = depth / n_depth

        patch_area = d_along * d_depth
        patch_moment = MU_EL * slip * patch_area

        cos_s = math.sin(strike)
        sin_s = math.cos(strike)

        seg_ox, seg_oy = _ll_to_meters(
            seg["center_lat"], seg["center_lon"], origin_lat, origin_lon
        )

        for i in range(n_along):
            along_offset = (i + 0.5) * d_along - seg_len / 2
            px = seg_ox + along_offset * cos_s
            py = seg_oy + along_offset * sin_s

            for j in range(n_depth):
                pd = (j + 0.5) * d_depth

                for k in range(n):
                    dcfs[k] += _patch_coulomb_stress(
                        obs_x_m[k], obs_y_m[k],
                        px, py, pd, strike,
                        patch_moment, d_along,
                    )

    dcfs_mpa = dcfs / 1e6

    logger.info(
        f"Coulomb stress: min={dcfs_mpa.min():.4f}, max={dcfs_mpa.max():.4f}, "
        f"mean={dcfs_mpa.mean():.4f} MPa"
    )
    return dcfs_mpa


def compute_coulomb_grid(
    lat_min=16.0, lat_max=27.0, lon_min=93.5, lon_max=100.0,
    resolution=0.5, segments=None,
):
    """
    Compute Coulomb stress change on a regular grid for visualization.

    Returns grid_lats, grid_lons, dcfs_grid (2D array in MPa).
    """
    lats = np.arange(lat_min, lat_max + resolution, resolution)
    lons = np.arange(lon_min, lon_max + resolution, resolution)
    grid_lons, grid_lats = np.meshgrid(lons, lats)

    flat_lats = grid_lats.flatten()
    flat_lons = grid_lons.flatten()

    dcfs = compute_coulomb_at_points(flat_lats, flat_lons, segments=segments)

    dcfs_grid = dcfs.reshape(grid_lats.shape)
    return grid_lats, grid_lons, dcfs_grid


def compute_coulomb_at_dams(segments=None):
    """
    Compute Coulomb stress change at all dam locations.

    Returns list of dicts with dam name, lat, lon, dcfs_mpa.
    """
    dams_path = os.path.join(os.getcwd(), "myanmar_dams.geojson")
    if not os.path.exists(dams_path):
        dams_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "..", "..", "myanmar_dams.geojson"
        )

    if not os.path.exists(dams_path):
        logger.warning("Dam GeoJSON not found")
        return []

    with open(dams_path, encoding="utf-8") as f:
        data = json.load(f)

    dams = []
    for feat in data.get("features", []):
        coords = feat["geometry"]["coordinates"]
        props = feat.get("properties", {})
        dams.append({
            "name": props.get("name", "Unnamed"),
            "lat": coords[1],
            "lon": coords[0],
            "type": props.get("function", "Unknown"),
        })

    if not dams:
        return []

    obs_lats = np.array([d["lat"] for d in dams])
    obs_lons = np.array([d["lon"] for d in dams])

    dcfs = compute_coulomb_at_points(obs_lats, obs_lons, segments=segments)

    results = []
    for i, dam in enumerate(dams):
        dcfs_val = float(dcfs[i])
        if dcfs_val > 0.001:
            regime = "Triggered"
        elif dcfs_val < -0.001:
            regime = "Shadow"
        else:
            regime = "Neutral"

        results.append({
            "name": dam["name"],
            "latitude": dam["lat"],
            "longitude": dam["lon"],
            "type": dam["type"],
            "dcfs_mpa": round(dcfs_val, 6),
            "stress_regime": regime,
        })

    n_triggered = sum(1 for r in results if r["stress_regime"] == "Triggered")
    n_shadow = sum(1 for r in results if r["stress_regime"] == "Shadow")
    n_neutral = sum(1 for r in results if r["stress_regime"] == "Neutral")
    logger.info(
        f"Dam stress: {n_triggered} triggered, {n_shadow} shadow, {n_neutral} neutral"
    )

    return results
