import logging
import math
import os
from typing import Optional

import numpy as np
import pandas as pd
import json

from mmeq.analysis.seismology import select_scenario_event
from mmeq.config import GMPE_SIGMA_LN, RISK_GRADE_THRESHOLDS, RISK_WEIGHTS

logger = logging.getLogger(__name__)


def _log_effective_risk_config() -> None:
    """Report the effective risk configuration at compute time.

    RISK_WEIGHTS / RISK_GRADE_THRESHOLDS / GMPE_SIGMA_LN are overridable via
    MMEQ_* env vars, which silently changes published science — make the values
    actually used loud in the logs, and warn (do not raise; the pipeline must
    degrade gracefully) if the weights no longer sum to 1.
    """
    logger.info(
        "Effective dam-risk config: weights=%s grade_thresholds=%s gmpe_sigma_ln=%s",
        RISK_WEIGHTS, RISK_GRADE_THRESHOLDS, GMPE_SIGMA_LN,
    )
    weight_sum = sum(RISK_WEIGHTS.values())
    if abs(weight_sum - 1.0) > 1e-6:
        logger.warning(
            "RISK_WEIGHTS sum to %.6f, not 1.0 — composite risk scores and "
            "published grades will be skewed. Check MMEQ_RISK_W_* overrides.",
            weight_sum,
        )


def _grade(composite: float) -> str:
    """Map a composite risk score to its published grade (thresholds in config)."""
    if composite >= RISK_GRADE_THRESHOLDS["critical"]:
        return "Critical"
    if composite >= RISK_GRADE_THRESHOLDS["high"]:
        return "High"
    if composite >= RISK_GRADE_THRESHOLDS["moderate"]:
        return "Moderate"
    return "Low"


def _dam_size(dam) -> tuple:
    """Parse (height_m, capacity_mw, total_storage_mcm) from a dam row, 0 if missing."""
    vals = []
    for key in ("height_m", "capacity_mw", "total_storage_mcm"):
        try:
            vals.append(float(dam.get(key) or 0))
        except (ValueError, TypeError):
            vals.append(0)
    return tuple(vals)


def _component_scores(pga: float, dist_fault: float, dist_to_major: float, dam) -> tuple:
    """Shared 0-10 component scores used by dam_risk_scores AND sensitivity_analysis.

    Returns (seismic, fault, proximity, exposure) scores. Keeping this in one
    place guarantees the two analyses cannot silently diverge (spec 003 P5).
    """
    seismic_score = min(10, pga / 0.05 * 10)
    fault_score = min(10, 10 / max(1, dist_fault / 10)) if not math.isnan(dist_fault) else 5
    prox_score = min(10, 10 / max(1, dist_to_major / 50))
    h, c, s = _dam_size(dam)
    exposure_score = min(10, (h * 0.3 + c * 0.02 + s * 0.005) / 5)
    return seismic_score, fault_score, prox_score, exposure_score


def _load_fault_segments() -> list:
    faults_path = os.path.join(os.getcwd(), "fault_lines.json")
    if not os.path.exists(faults_path):
        return []
    with open(faults_path, encoding="utf-8") as f:
        data = json.load(f)
    segments = []
    for feat in data.get("features", []):
        geom = feat.get("geometry", {})
        if geom.get("type") == "LineString":
            for i in range(len(geom["coordinates"]) - 1):
                segments.append((geom["coordinates"][i], geom["coordinates"][i + 1]))
        elif geom.get("type") == "MultiLineString":
            for line in geom["coordinates"]:
                for i in range(len(line) - 1):
                    segments.append((line[i], line[i + 1]))
    return segments


def _load_rupture_trace() -> list:
    """Segments of the 2025 M7.7 rupture outline (USGS ShakeMap rupture.json).

    Returns [((lon1, lat1), (lon2, lat2)), ...] in the same format as
    _load_fault_segments, or [] when the file is missing. Ring coordinates may
    carry a depth third element, which is dropped — the surface projection is
    an adequate Rrup for a near-vertical strike-slip rupture. This is the
    distance basis for the deterministic 2025 scenario; the nearest-mapped-
    fault distance previously used came from a GLOBAL plate-boundary file and
    assigned near-fault M7.7 PGA to dams ~435 km from the actual rupture
    (2026-07-06 audit, finding C2 / spec 006).
    """
    from mmeq import config

    path = os.path.join(config.DATA_DIR, "shakemap", "rupture.json")
    if not os.path.exists(path):
        return []
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logger.warning("Could not load rupture geometry %s: %s", path, e)
        return []

    segments = []

    def add_line(coords):
        pts = [(c[0], c[1]) for c in coords]
        for i in range(len(pts) - 1):
            segments.append((pts[i], pts[i + 1]))

    for feat in data.get("features", []):
        geom = feat.get("geometry", {})
        gtype = geom.get("type")
        coords = geom.get("coordinates", [])
        if gtype == "LineString":
            add_line(coords)
        elif gtype in ("MultiLineString", "Polygon"):
            for line in coords:
                add_line(line)
        elif gtype == "MultiPolygon":
            for poly in coords:
                for ring in poly:
                    add_line(ring)
    return segments


def load_dams_df(path: Optional[str] = None) -> Optional[pd.DataFrame]:
    """Load the Myanmar dams GeoJSON into a DataFrame, or None if not found.

    This is the single dam-loader for the whole package: analysis (clustering,
    coulomb) and visualization (map) all import it so the resolution logic and
    column schema live in exactly one place. When ``path`` is given it is the
    sole candidate; otherwise the file is resolved via config.DAMS_PATH (explicit
    MMEQ_DAMS_PATH override) then the ordered config.DAMS_PATH_CANDIDATES (cwd
    first — the historical lookup — then the repo root). Returns None on a
    missing file so callers can degrade gracefully.
    """
    from mmeq import config

    if path:
        candidates = [path]
    else:
        candidates = [config.DAMS_PATH] if config.DAMS_PATH else []
        candidates += config.DAMS_PATH_CANDIDATES
    for p in candidates:
        p = os.path.normpath(p)
        if os.path.exists(p):
            with open(p, encoding="utf-8") as f:
                data = json.load(f)
            rows = []
            for feat in data.get("features", []):
                coords = feat["geometry"]["coordinates"]
                props = feat.get("properties", {})
                rows.append({
                    "longitude": coords[0],
                    "latitude": coords[1],
                    "name": props.get("name", "Unnamed Dam"),
                    "status": props.get("status", ""),
                    "function": props.get("function", ""),
                    "capacity_mw": props.get("capacity_mw", ""),
                    "height_m": props.get("height_m", ""),
                    "river": props.get("river", ""),
                    "state": props.get("state", ""),
                    "reservoir_area_km2": props.get("reservoir_area_km2", ""),
                    "total_storage_mcm": props.get("total_storage_mcm", ""),
                })
            return pd.DataFrame(rows)
    return None


# Backwards-compatible private alias (this module's internal call sites).
_load_dams_df = load_dams_df


def _point_to_segment_distance(px, py, ax, ay, bx, by):
    dx, dy = bx - ax, by - ay
    if dx == 0 and dy == 0:
        return math.sqrt((px - ax) ** 2 + (py - ay) ** 2)
    t = max(0, min(1, ((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy)))
    proj_x, proj_y = ax + t * dx, ay + t * dy
    return math.sqrt((px - proj_x) ** 2 + (py - proj_y) ** 2)


def distance_to_nearest_fault(lat: float, lon: float, segments: list) -> float:
    """Distance (km) from a point to the nearest fault segment.

    Segments are in (lon, lat) degree pairs.  The point-to-segment math is
    done in a local approximate Cartesian frame (degrees scaled by cos(lat))
    so that 1 unit ≈ 111 km in both axes at the site latitude.
    """
    cos_lat = math.cos(math.radians(lat))
    min_dist_deg = float("inf")
    for (lon1, lat1), (lon2, lat2) in segments:
        # scale longitudes to approximate equal-area at site latitude
        d = _point_to_segment_distance(
            lon * cos_lat, lat,
            lon1 * cos_lat, lat1,
            lon2 * cos_lat, lat2,
        )
        if d < min_dist_deg:
            min_dist_deg = d
    return min_dist_deg * 111.0


def estimate_pga_ask08(
    mag: float,
    rrup_km: float,
    vs30: float = 760.0,
    rake: float = 180.0,
    dip: float = 90.0,
    ztor: float = 0.0,
    width_km: float = 15.0,
    rjb_km: float = None,
    rx_km: float = -1.0,
) -> float:
    """Abrahamson & Silva (2008) NGA-West1 GMPE for PGA.

    Coefficients verified against the OpenQuake reference implementation
    (gem/oq-engine abrahamson_silva_2008.py) and Tables 4-5a of
    Abrahamson & Silva, Earthquake Spectra 24(1):67-97.

    Parameters
    ----------
    mag      : Moment magnitude
    rrup_km  : Closest distance to rupture plane (km)
    vs30     : Shear-wave velocity in top 30 m (m/s), default 760
    rake     : Rake angle (degrees). 180 = right-lateral strike-slip
    dip      : Fault dip (degrees). 90 = vertical
    ztor     : Depth to top of rupture (km), default 0
    width_km : Down-dip rupture width (km), default 15
    rjb_km   : Joyner-Boore distance (km); defaults to rrup_km
    rx_km    : Horizontal distance from top edge of rupture (km);
               negative = footwall side (no hanging-wall effect)

    Returns
    -------
    Median PGA in g.
    """
    if rjb_km is None:
        rjb_km = rrup_km

    rrup = max(rrup_km, 0.1)
    rjb = max(rjb_km, 0.0)

    # ------------------------------------------------------------------
    # IMT-independent constants  (Table 4, p. 84)
    # ------------------------------------------------------------------
    c1 = 6.75
    c4 = 4.5
    a3 = 0.265
    a4 = -0.231
    a5 = -0.398
    n = 1.18
    c = 1.88
    c2 = 50.0  # noqa: F841 (ASK08 coefficient kept for reference; unused in this PGA impl)

    # ------------------------------------------------------------------
    # PGA coefficients  (Table 5a, p. 84)
    # ------------------------------------------------------------------
    VLIN = 865.1
    b_site = -1.186
    a1 = 0.804
    a2 = -0.9679
    a8 = -0.0372
    a10 = 0.9445
    a12 = 0.0
    a13 = -0.0600
    a14 = 1.0800
    a15 = -0.3500  # noqa: F841 (HW coefficient; HW term simplified for vertical faults)
    a16 = 0.9000
    a18 = -0.0067

    # ------------------------------------------------------------------
    # 1. Base term  (Eq. 2-4, p. 75)
    # ------------------------------------------------------------------
    R = math.sqrt(rrup ** 2 + c4 ** 2)

    if mag <= c1:
        f1 = a1 + a4 * (mag - c1) + a8 * (8.5 - mag) ** 2 + (a2 + a3 * (mag - c1)) * math.log(R)
    else:
        f1 = a1 + a5 * (mag - c1) + a8 * (8.5 - mag) ** 2 + (a2 + a3 * (mag - c1)) * math.log(R)

    # ------------------------------------------------------------------
    # 2. Faulting-style term  (Table 2, p. 75)
    # ------------------------------------------------------------------
    f_style = 0.0
    if 30 < rake < 150:       # reverse
        f_style = a12
    elif -120 < rake < -60:   # normal
        f_style = a13

    # ------------------------------------------------------------------
    # 3. Geometry terms (hanging-wall, ztor, large-distance).
    #    These are site-independent, so they must be computed BEFORE the
    #    rock-reference PGA1100 — ASK08 defines PGA1100 from the full median
    #    model on Vs30=1100 rock, including these terms (Eq. 5, p. 77).
    # ------------------------------------------------------------------
    f_hw = 0.0
    if rx_km > 0 and dip < 90.0:
        Fhw = 1.0
        # T1: distance taper
        if rjb < 30.0:
            T1 = 1.0 - rjb / 30.0
        else:
            T1 = 0.0
        # T2: magnitude taper
        if mag <= 6.0:
            T2 = 0.0
        elif mag < 7.0:
            T2 = mag - 6.0
        else:
            T2 = 1.0
        # T3: Rrup/Rx taper
        T3 = 1.0  # simplified for vertical faults
        # T4: depth taper
        if ztor <= 10.0:
            T4 = 1.0 - ztor / 10.0 if ztor > 0 else 1.0
        else:
            T4 = 0.0
        # T5: dip taper
        T5 = 1.0 - (dip - 30.0) / 60.0 if dip >= 30.0 else 1.0
        f_hw = Fhw * a14 * T1 * T2 * T3 * T4 * T5

    # ------------------------------------------------------------------
    # 5. Depth-to-top-of-rupture term  (Eq. 13, p. 78)
    # ------------------------------------------------------------------
    f_ztor = a16 * min(ztor, 10.0) / 10.0

    # ------------------------------------------------------------------
    # 6. Large-distance term  (Eq. 14-15, p. 79)
    # ------------------------------------------------------------------
    f_large = 0.0
    if rrup >= 100.0:
        if mag < 5.5:
            T6 = 1.0
        elif mag <= 6.5:
            T6 = 0.5 * (6.5 - mag) + 0.5
        else:
            T6 = 0.5
        f_large = a18 * (rrup - 100.0) * T6

    f_geom = f_hw + f_ztor + f_large

    # ------------------------------------------------------------------
    # 4. Site response term  (Eq. 5-7, p. 77)
    #    Requires PGA on reference rock (Vs30=1100) first, computed from the
    #    full median model (base + style + geometry) with the linear site term
    #    for Vs30=1100 (> VLIN).
    # ------------------------------------------------------------------
    vs30_star_1100 = min(1100.0, 1500.0)  # v1=1500 for PGA
    f_site_1100 = (a10 + b_site * n) * math.log(vs30_star_1100 / VLIN)
    pga1100 = math.exp(f1 + f_style + f_geom + f_site_1100)

    # Now compute actual site term for the target Vs30
    vs30_star = min(vs30, 1500.0)  # v1 = 1500 for PGA
    if vs30 < VLIN:
        f_site = (a10 * math.log(vs30_star / VLIN)
                  - b_site * math.log(pga1100 + c)
                  + b_site * math.log(pga1100 + c * (vs30_star / VLIN) ** n))
    else:
        f_site = (a10 + b_site * n) * math.log(vs30_star / VLIN)

    # ------------------------------------------------------------------
    # Total
    # ------------------------------------------------------------------
    ln_pga = f1 + f_style + f_site + f_geom
    pga_g = math.exp(ln_pga)
    return max(pga_g, 0.0001)


def estimate_pga(mag: float, depth_km: float, dist_km: float, vs30: float = 760.0) -> float:
    """Estimate PGA using Abrahamson & Silva (2008) for crustal strike-slip.

    For extended ruptures (like the 500 km 2025 event), *dist_km* should be
    the distance to the nearest fault trace (≈ Rrup for vertical faults).
    """
    if mag < 3.0:
        return 0.0001
    return max(
        estimate_pga_ask08(mag=mag, rrup_km=dist_km, vs30=vs30, rake=180.0, dip=90.0),
        0.0001,
    )


def _load_vs30() -> dict:
    report_dir = os.environ.get("MMEQ_REPORT_DIR", "report")
    vs30_path = os.path.join(report_dir, "dam_vs30.csv")
    if not os.path.exists(vs30_path):
        return {}
    df = pd.read_csv(vs30_path)
    vs30_map = {}
    for _, row in df.iterrows():
        key = f"{row['lat']:.4f},{row['lon']:.4f}"
        vs30_map[key] = float(row["vs30"])
    return vs30_map


_SMOOTHED_CACHE = {}


def _smoothed_seismicity(
    declustered_df: pd.DataFrame,
    mc: float,
    catalog_years: float,
    cell_deg: float,
    smooth_sigma_km: float,
):
    """Grid-bin + Gaussian-smooth the declustered catalog into per-cell annual
    rates (events >= Mc/yr). Site-independent, so it is computed once and cached
    on the catalog identity + grid params — cmd_report calls compute_hazard_curve
    once per dam (254x) and this O(cells^2) smoothing must not repeat (Kilo
    review of PR #38). Returns (cell_lat, cell_lon, annual_rate_cell), or None
    when the (filtered) catalog is empty. Keyed on the ORIGINAL catalog object
    so the per-dam calls (which pass the same df_dec) share one computation.
    """
    key = (id(declustered_df), len(declustered_df), float(mc), float(catalog_years),
           cell_deg, smooth_sigma_km)
    cached = _SMOOTHED_CACHE.get(key)
    if cached is not None:
        return cached

    ev = declustered_df.dropna(subset=["latitude", "longitude", "mag"])
    ev = ev[ev["mag"] >= mc]
    if ev.empty or catalog_years <= 0:
        _SMOOTHED_CACHE[key] = None
        return None

    lat0, lat1 = ev["latitude"].min(), ev["latitude"].max()
    lon0, lon1 = ev["longitude"].min(), ev["longitude"].max()
    lat_edges = np.arange(lat0 - cell_deg, lat1 + 2 * cell_deg, cell_deg)
    lon_edges = np.arange(lon0 - cell_deg, lon1 + 2 * cell_deg, cell_deg)
    counts, _, _ = np.histogram2d(
        ev["latitude"], ev["longitude"], bins=[lat_edges, lon_edges]
    )
    clat = (lat_edges[:-1] + lat_edges[1:]) / 2
    clon = (lon_edges[:-1] + lon_edges[1:]) / 2
    grid_lat, grid_lon = np.meshgrid(clat, clon, indexing="ij")
    cell_lat = grid_lat.ravel()
    cell_lon = grid_lon.ravel()
    cell_n = counts.ravel()

    occupied = cell_n > 0
    src_lat, src_lon, src_n = cell_lat[occupied], cell_lon[occupied], cell_n[occupied]
    mean_coslat = math.cos(math.radians(float(np.mean(cell_lat))))
    dlat = (cell_lat[None, :] - src_lat[:, None]) * 111.0
    dlon = (cell_lon[None, :] - src_lon[:, None]) * 111.0 * mean_coslat
    kern = np.exp(-(dlat ** 2 + dlon ** 2) / (2.0 * smooth_sigma_km ** 2))
    kern /= kern.sum(axis=1, keepdims=True)
    smoothed = (src_n[:, None] * kern).sum(axis=0)
    annual_rate_cell = smoothed / catalog_years

    result = (cell_lat, cell_lon, annual_rate_cell)
    if len(_SMOOTHED_CACHE) > 8:
        _SMOOTHED_CACHE.clear()
    _SMOOTHED_CACHE[key] = result
    return result


def compute_hazard_curve(
    site_lat: float,
    site_lon: float,
    vs30: float,
    declustered_df: pd.DataFrame,
    b_val: float,
    mc: float,
    catalog_years: float,
    pga_levels: np.ndarray = None,
    min_mag: float = 5.0,
    max_mag: float = 8.0,
    delta_m: float = 0.1,
    sigma_ln: float = GMPE_SIGMA_LN,
    cell_deg: float = 0.5,
    smooth_sigma_km: float = 50.0,
) -> pd.DataFrame:
    """
    Cornell-McGuire hazard curve (PGA vs annual exceedance rate) at a site,
    with a Frankel (1995)-style smoothed-seismicity source model.

    The previous implementation integrated over magnitude only, applying the
    ENTIRE regional catalog rate at the site's single nearest-fault distance —
    a dam 1 km from any fault trace was assigned the recurrence of every M5-8
    event in the ~2,000x950 km catalog box at 1 km, inflating near-fault
    475-yr PGA to ~2.1-2.5 g vs ~1.0 g on-fault in the dedicated Myanmar PSHA
    literature (Thant et al. 2023, Geoscience Letters). (2026-07-06 audit,
    finding C3 / spec 006.)

    Source model: declustered events >= mc are binned on a cell_deg grid;
    per-cell counts are Gaussian-kernel smoothed (sigma = smooth_sigma_km)
    with the TOTAL rate conserved; each cell's annual rate is extrapolated
    over magnitude with a Gutenberg-Richter slope b anchored at mc, and the
    hazard integral sums rate(cell, m-bin) * P(PGA > x | m, r_cell->site)
    over cells and magnitude bins. Cell-to-site distances are floored at 5 km.
    """
    from scipy.stats import norm

    if pga_levels is None:
        pga_levels = np.logspace(-3, 0, 50)

    # Site-independent gridded, kernel-smoothed annual rates (computed once and
    # cached across the per-dam calls; see _smoothed_seismicity).
    smoothed = _smoothed_seismicity(
        declustered_df, mc, catalog_years, cell_deg, smooth_sigma_km
    )
    if smoothed is None:
        z = np.zeros(len(pga_levels))
        return pd.DataFrame({
            "pga_g": pga_levels, "annual_rate": z, "annual_prob": z,
            "prob_50yr": z, "return_period_yr": np.full(len(pga_levels), np.inf),
        })
    cell_lat, cell_lon, annual_rate_cell = smoothed

    # --- site distances, aggregated into log-spaced distance bins ---
    site_coslat = math.cos(math.radians(site_lat))
    d_km = np.sqrt(
        ((cell_lat - site_lat) * 111.0) ** 2
        + ((cell_lon - site_lon) * 111.0 * site_coslat) ** 2
    )
    d_km = np.maximum(d_km, 5.0)
    r_edges = np.logspace(np.log10(5.0), np.log10(max(d_km.max() * 1.01, 10.0)), 41)
    r_idx = np.clip(np.digitize(d_km, r_edges) - 1, 0, len(r_edges) - 2)
    r_centers = np.sqrt(r_edges[:-1] * r_edges[1:])
    rate_by_r = np.zeros(len(r_centers))
    np.add.at(rate_by_r, r_idx, annual_rate_cell)

    # --- Gutenberg-Richter magnitude bins anchored at mc ---
    mags = np.arange(max(min_mag, mc), max_mag + delta_m, delta_m)
    frac_bin = 10.0 ** (-b_val * (mags - mc)) - 10.0 ** (-b_val * (mags + delta_m - mc))
    frac_bin = np.maximum(frac_bin, 0.0)

    log_pga_targets = np.log(pga_levels)
    rates = np.zeros(len(pga_levels))
    for ri, r in enumerate(r_centers):
        lam_r = rate_by_r[ri]
        if lam_r <= 0:
            continue
        for m, frac in zip(mags, frac_bin):
            pga_median = estimate_pga_ask08(mag=float(m), rrup_km=float(r), vs30=vs30)
            if pga_median <= 0:
                continue
            eps = (log_pga_targets - math.log(pga_median)) / sigma_ln
            rates += lam_r * frac * norm.sf(eps)

    prob_50yr = 1.0 - np.exp(-rates * 50)
    return pd.DataFrame({
        "pga_g": pga_levels,
        "annual_rate": rates,
        "annual_prob": rates,
        "prob_50yr": prob_50yr,
        "return_period_yr": np.where(rates > 0, 1.0 / rates, np.inf),
    })


def compute_return_period(
    mag_threshold: float,
    b_value: float,
    a_value: float,
    catalog_years: float,
) -> float:
    """Return period (years) for events >= mag_threshold.

    a_value is the catalog-level Gutenberg-Richter intercept (cumulative count
    over the whole catalog), so it must be converted to an annual rate by
    subtracting log10(catalog_years) before inverting. Without this the result
    is 1 / (cumulative catalog count), which is too short by a factor of
    catalog_years.
    """
    if catalog_years <= 0:
        return float("inf")
    a_annual = a_value - math.log10(catalog_years)
    n_per_year = 10 ** (a_annual - b_value * mag_threshold)
    if n_per_year <= 0:
        return float("inf")
    return 1.0 / n_per_year


def sensitivity_analysis(
    eq_df: pd.DataFrame,
    b_val: float,
    a_val: float,
    mc: float,
    n_samples: int = 100,
) -> pd.DataFrame:
    """
    Monte Carlo sensitivity analysis on risk scoring weights.
    Returns a DataFrame with weight combinations and resulting risk grade distributions.
    """
    dams_df = _load_dams_df()
    if dams_df is None or dams_df.empty:
        return pd.DataFrame()

    fault_segments = _load_fault_segments()
    rupture_segments = _load_rupture_trace()
    if not rupture_segments:
        logger.warning(
            "2025 rupture geometry unavailable — scenario PGA falls back to "
            "nearest-fault/epicentral distance (see spec 006)"
        )
    major_eq = select_scenario_event(eq_df)
    major_mag = major_eq["mag"]
    major_depth = major_eq["depth"]
    major_lat = major_eq["latitude"]
    major_lon = major_eq["longitude"]

    rng = np.random.RandomState(42)
    results = []
    for _ in range(n_samples):
        w = rng.dirichlet([1, 1, 1, 1])
        w_seismic, w_fault, w_prox, w_exp = w

        counts = {"Critical": 0, "High": 0, "Moderate": 0, "Low": 0}
        for __, dam in dams_df.iterrows():
            dlat = dam["latitude"] - major_lat
            dlon = (dam["longitude"] - major_lon) * math.cos(math.radians(major_lat))
            dist_to_major = math.sqrt(dlat ** 2 + dlon ** 2) * 111.0
            dist_fault = distance_to_nearest_fault(dam["latitude"], dam["longitude"], fault_segments) if fault_segments else 50.0
            if rupture_segments:
                rjb_km = distance_to_nearest_fault(dam["latitude"], dam["longitude"], rupture_segments)
            else:
                rjb_km = dist_fault if (not math.isnan(dist_fault) and dist_fault > 0) else dist_to_major
            pga = estimate_pga(major_mag, major_depth, rjb_km)

            seismic_score, fault_score, prox_score, exposure_score = _component_scores(
                pga, dist_fault, dist_to_major, dam
            )

            composite = seismic_score * w_seismic + fault_score * w_fault + prox_score * w_prox + exposure_score * w_exp
            counts[_grade(composite)] += 1

        results.append({
            "w_seismic": round(w_seismic, 3),
            "w_fault": round(w_fault, 3),
            "w_proximity": round(w_prox, 3),
            "w_exposure": round(w_exp, 3),
            "critical": counts["Critical"],
            "high": counts["High"],
            "moderate": counts["Moderate"],
            "low": counts["Low"],
        })

    return pd.DataFrame(results)


def dam_risk_scores(
    eq_df: pd.DataFrame,
    b_val: float,
    a_val: float,
    mc: float,
) -> pd.DataFrame:
    _log_effective_risk_config()
    dams_df = _load_dams_df()
    if dams_df is None or dams_df.empty:
        logger.warning("No dam data available")
        return pd.DataFrame()

    fault_segments = _load_fault_segments()
    rupture_segments = _load_rupture_trace()
    if not rupture_segments:
        logger.warning(
            "2025 rupture geometry unavailable — scenario PGA falls back to "
            "nearest-fault/epicentral distance (see spec 006)"
        )
    vs30_map = _load_vs30()
    major_eq = select_scenario_event(eq_df)
    major_mag = major_eq["mag"]
    major_depth = major_eq["depth"]
    major_lat = major_eq["latitude"]
    major_lon = major_eq["longitude"]

    # Catalog time span (years) for annualizing the Gutenberg-Richter rate.
    _t = pd.to_datetime(eq_df["time_utc"], errors="coerce").dropna()
    catalog_years = max((_t.max() - _t.min()).days / 365.25, 1.0) if len(_t) > 1 else 1.0

    results = []
    for _, dam in dams_df.iterrows():
        dlat = dam["latitude"] - major_lat
        dlon = (dam["longitude"] - major_lon) * math.cos(math.radians(major_lat))
        dist_to_major = math.sqrt(dlat ** 2 + dlon ** 2) * 111.0

        dist_fault = distance_to_nearest_fault(dam["latitude"], dam["longitude"], fault_segments) if fault_segments else float("nan")

        if rupture_segments:
            rjb_km = distance_to_nearest_fault(dam["latitude"], dam["longitude"], rupture_segments)
        else:
            rjb_km = dist_fault if (not math.isnan(dist_fault) and dist_fault > 0) else dist_to_major

        vs30 = vs30_map.get(f"{dam['latitude']:.4f},{dam['longitude']:.4f}", 760.0)
        pga = estimate_pga(major_mag, major_depth, rjb_km, vs30=vs30)

        nearby = eq_df[
            ((eq_df["latitude"] - dam["latitude"]).abs() < 1.0)
            & ((eq_df["longitude"] - dam["longitude"]).abs() < 1.0)
        ]
        if len(nearby) == 0:
            nearby = eq_df

        seismic_score, fault_score, prox_score, exposure_score = _component_scores(
            pga, dist_fault, dist_to_major, dam
        )
        h, c, s = _dam_size(dam)

        composite = (
            seismic_score * RISK_WEIGHTS["seismic"]
            + fault_score * RISK_WEIGHTS["fault"]
            + prox_score * RISK_WEIGHTS["proximity"]
            + exposure_score * RISK_WEIGHTS["exposure"]
        )

        return_period_m6 = compute_return_period(6.0, b_val, a_val, catalog_years)

        results.append({
            "name": dam["name"],
            "status": dam["status"],
            "function": dam["function"],
            "river": dam["river"],
            "state": dam["state"],
            "latitude": dam["latitude"],
            "longitude": dam["longitude"],
            "height_m": h,
            "capacity_mw": c,
            "storage_mcm": s,
            "dist_to_major_km": round(dist_to_major, 1),
            "dist_to_rupture_km": round(rjb_km, 1),
            "dist_to_fault_km": round(dist_fault, 1) if not math.isnan(dist_fault) else None,
            "vs30": round(vs30),
            "pga_g": round(pga, 4),
            "seismic_score": round(seismic_score, 1),
            "fault_score": round(fault_score, 1),
            "proximity_score": round(prox_score, 1),
            "exposure_score": round(exposure_score, 1),
            "composite_risk": round(composite, 1),
            "risk_grade": _grade(composite),
            "return_period_m6_yrs": round(return_period_m6, 1),
        })

    risk_df = pd.DataFrame(results).sort_values("composite_risk", ascending=False).reset_index(drop=True)
    logger.info(f"Computed risk scores for {len(risk_df)} dams")
    return risk_df


def monte_carlo_pga(
    eq_df: pd.DataFrame,
    n_iterations: int = 1000,
    sigma_ln: float = GMPE_SIGMA_LN,
) -> pd.DataFrame:
    """
    Monte Carlo simulation of PGA uncertainty at all dam locations.

    Propagates ASK08 aleatory uncertainty (sigma_ln) through the GMPE to produce
    probabilistic PGA distributions at each dam site.

    Returns DataFrame with dam name, lat, lon, and PGA statistics.
    """
    dams_df = _load_dams_df()
    if dams_df is None or dams_df.empty:
        return pd.DataFrame()

    fault_segments = _load_fault_segments()
    rupture_segments = _load_rupture_trace()
    if not rupture_segments:
        logger.warning(
            "2025 rupture geometry unavailable — scenario PGA falls back to "
            "nearest-fault/epicentral distance (see spec 006)"
        )
    vs30_map = _load_vs30()
    major_eq = select_scenario_event(eq_df)
    major_mag = major_eq["mag"]
    major_depth = major_eq["depth"]
    major_lat = major_eq["latitude"]
    major_lon = major_eq["longitude"]

    rng = np.random.RandomState(42)
    results = []

    for _, dam in dams_df.iterrows():
        dlat = dam["latitude"] - major_lat
        dlon = (dam["longitude"] - major_lon) * math.cos(math.radians(major_lat))
        dist_to_major = math.sqrt(dlat ** 2 + dlon ** 2) * 111.0
        dist_fault = distance_to_nearest_fault(
            dam["latitude"], dam["longitude"], fault_segments
        ) if fault_segments else float("nan")
        if rupture_segments:
            rjb_km = distance_to_nearest_fault(dam["latitude"], dam["longitude"], rupture_segments)
        else:
            rjb_km = dist_fault if (not math.isnan(dist_fault) and dist_fault > 0) else dist_to_major
        vs30 = vs30_map.get(f"{dam['latitude']:.4f},{dam['longitude']:.4f}", 760.0)

        ln_pga_median = math.log(estimate_pga(major_mag, major_depth, rjb_km, vs30=vs30))

        ln_pga_samples = rng.normal(ln_pga_median, sigma_ln, n_iterations)
        pga_samples = np.exp(ln_pga_samples)

        results.append({
            "name": dam["name"],
            "latitude": dam["latitude"],
            "longitude": dam["longitude"],
            "pga_median_g": round(float(np.median(pga_samples)), 4),
            "pga_mean_g": round(float(np.mean(pga_samples)), 4),
            "pga_std_g": round(float(np.std(pga_samples)), 4),
            "pga_p05_g": round(float(np.percentile(pga_samples, 5)), 4),
            "pga_p16_g": round(float(np.percentile(pga_samples, 16)), 4),
            "pga_p50_g": round(float(np.percentile(pga_samples, 50)), 4),
            "pga_p84_g": round(float(np.percentile(pga_samples, 84)), 4),
            "pga_p95_g": round(float(np.percentile(pga_samples, 95)), 4),
            "prob_pga_gt_0.1g": round(float(np.mean(pga_samples > 0.1)), 4),
            "prob_pga_gt_0.2g": round(float(np.mean(pga_samples > 0.2)), 4),
            "prob_pga_gt_0.5g": round(float(np.mean(pga_samples > 0.5)), 4),
        })

    mc_df = pd.DataFrame(results)
    logger.info(f"Monte Carlo PGA ({n_iterations} iterations) for {len(mc_df)} dams")
    return mc_df
