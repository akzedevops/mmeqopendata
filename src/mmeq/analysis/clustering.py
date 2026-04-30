import logging
import os
import warnings
import pandas as pd
import numpy as np
from typing import Optional, Dict, List

from mmeq.config import DBSCAN_EPS, DBSCAN_MIN_SAMPLES

logger = logging.getLogger(__name__)

try:
    from sklearn.cluster import DBSCAN
    from matplotlib.patches import Patch
    from matplotlib.lines import Line2D
    import geopandas as gpd
    import contextily as ctx
    import matplotlib.pyplot as plt

    HAS_VIZ = True
except ImportError:
    HAS_VIZ = False

_DEPTH_BANDS = [
    (0, 30, "#d62728", "Shallow (0–30 km)"),
    (30, 70, "#ff7f0e", "Intermediate (30–70 km)"),
    (70, 300, "#1f77b4", "Deep (70–300 km)"),
    (300, 1000, "#08306b", "Very Deep (300+ km)"),
]

_CLUSTER_BORDER_COLORS = [
    "#e41a1c", "#4daf4a", "#377eb8", "#ff7f00", "#984ea3",
    "#a65628", "#f781bf", "#999999", "#66c2a5", "#fc8d62",
]


def _depth_color_arr(depths: np.ndarray) -> List[str]:
    colors = []
    for d in depths:
        c = _DEPTH_BANDS[-1][2]
        for lo, hi, col, _ in _DEPTH_BANDS:
            if lo <= d < hi:
                c = col
                break
        colors.append(c)
    return colors


def _depth_label(avg_depth: float) -> str:
    for lo, hi, _, label in _DEPTH_BANDS:
        if lo <= avg_depth < hi:
            return label
    return _DEPTH_BANDS[-1][3]


def _name_cluster(lat: float, lon: float) -> str:
    if 24.5 <= lat <= 28 and 95.5 <= lon <= 98.5:
        return "N Myanmar (Kachin/Sagaing)"
    if 23 <= lat <= 25 and 97 <= lon <= 99:
        return "NE Myanmar (Shan/Lashio)"
    if 21 <= lat <= 24 and 95 <= lon <= 97.5:
        return "Central Myanmar\n(Mandalay/Sagaing Fault)"
    if 19 <= lat <= 21.5 and 95 <= lon <= 97:
        return "E-Central Myanmar\n(Naypyidaw/Shan)"
    if 17 <= lat <= 19.5 and 94 <= lon <= 96.5:
        return "S Myanmar\n(Bago/Yangon/Rakhine)"
    if 15 <= lat <= 17.5 and 96.5 <= lon <= 99:
        return "SE Myanmar (Mon/Kayin)"
    if 21 <= lat <= 22 and 100 <= lon <= 102:
        return "E Shan (China Border)"
    if 19 <= lat <= 21 and 98 <= lon <= 100.5:
        return "E Shan (Thai Border)"
    if 24 <= lat <= 26.5 and 97 <= lon <= 99.5:
        return "NE Myanmar (Bhamo/Myitkyina)"
    if 13 <= lat <= 16 and 97 <= lon <= 100:
        return "Deep South (Tanintharyi)"
    return f"Region ({lat:.1f}N, {lon:.1f}E)"


def dams_in_seismic_zones(
    df: pd.DataFrame,
    eps: float = DBSCAN_EPS,
    min_samples: int = DBSCAN_MIN_SAMPLES,
) -> pd.DataFrame:
    df = df.copy()
    df["cluster"] = run_dbscan(df, eps=eps, min_samples=min_samples)
    stats = _cluster_stats(df)
    dams_df = _load_dams()
    if dams_df is None or dams_df.empty:
        logger.warning("No dam data available for risk analysis")
        return pd.DataFrame()

    from shapely.geometry import Point

    results = []
    real_clusters = sorted(c for c in stats if c != -1)
    for _, dam in dams_df.iterrows():
        dam_pt = Point(dam["longitude"], dam["latitude"])
        for c in real_clusters:
            sub = df[df["cluster"] == c]
            pts = sub[["longitude", "latitude"]].values
            if len(pts) < 3:
                continue
            from shapely.geometry import MultiPoint
            hull = MultiPoint([tuple(p) for p in pts]).convex_hull
            if hull.contains(dam_pt):
                s = stats[c]
                results.append({
                    "dam_name": dam["name"],
                    "dam_lat": dam["latitude"],
                    "dam_lon": dam["longitude"],
                    "cluster_id": c,
                    "cluster_name": s["name"].split("\n")[0].strip(),
                    "cluster_events": s["count"],
                    "cluster_max_mag": s["mag_max"],
                    "cluster_max_mag_date": s["max_mag_date"],
                    "cluster_avg_depth": round(s["avg_depth"], 1),
                })
                break

    risk_df = pd.DataFrame(results)
    if not risk_df.empty:
        risk_df = risk_df.sort_values("cluster_max_mag", ascending=False).reset_index(drop=True)
    logger.info(f"Found {len(risk_df)} dams within seismic cluster zones")
    return risk_df


def run_dbscan(
    df: pd.DataFrame,
    eps: float = DBSCAN_EPS,
    min_samples: int = DBSCAN_MIN_SAMPLES,
) -> pd.Series:
    if "latitude" not in df.columns or "longitude" not in df.columns:
        raise ValueError("DataFrame must have 'latitude' and 'longitude' columns")
    coords = df[["latitude", "longitude"]]
    db = DBSCAN(eps=eps, min_samples=min_samples).fit(coords)
    return pd.Series(db.labels_, index=df.index, name="cluster")


def _cluster_stats(df: pd.DataFrame) -> Dict[int, dict]:
    stats = {}
    for c in sorted(df["cluster"].unique()):
        sub = df[df["cluster"] == c]
        center_lat = sub["latitude"].median()
        center_lon = sub["longitude"].median()

        max_mag_row = sub.loc[sub["mag"].idxmax()]
        max_mag_date = ""
        if "time_utc" in max_mag_row.index:
            try:
                max_mag_date = pd.to_datetime(max_mag_row["time_utc"]).strftime("%Y-%m-%d")
            except Exception:
                pass

        avg_depth = sub["depth"].mean()
        name = _name_cluster(center_lat, center_lon) if c != -1 else "Unclustered"

        stats[int(c)] = {
            "name": name,
            "count": len(sub),
            "mag_max": sub["mag"].max(),
            "mag_min": sub["mag"].min(),
            "max_mag_date": max_mag_date,
            "avg_depth": avg_depth,
            "depth_label": _depth_label(avg_depth),
            "center_lat": center_lat,
            "center_lon": center_lon,
        }
    return stats


def _load_dams() -> Optional[pd.DataFrame]:
    candidates = [
        os.path.join(os.getcwd(), "myanmar_dams.geojson"),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "myanmar_dams.geojson"),
    ]
    dams_path = None
    for p in candidates:
        p = os.path.normpath(p)
        if os.path.exists(p):
            dams_path = p
            break
    if dams_path is None:
        return None
    import json
    with open(dams_path, encoding="utf-8") as f:
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
        })
    return pd.DataFrame(rows)


def plot_clusters_on_map(
    df: pd.DataFrame,
    title: str,
    filename: str,
    faults_gdf: Optional[object] = None,
    eps: float = DBSCAN_EPS,
    min_samples: int = DBSCAN_MIN_SAMPLES,
    show_dams: bool = True,
) -> None:
    if not HAS_VIZ:
        logger.error("Missing optional deps: scikit-learn, geopandas, contextily, matplotlib")
        return

    df = df.copy()
    df["cluster"] = run_dbscan(df, eps=eps, min_samples=min_samples)
    stats = _cluster_stats(df)

    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.longitude, df.latitude),
        crs="EPSG:4326",
    ).to_crs(epsg=3857)

    gdf["x"] = gdf.geometry.x
    gdf["y"] = gdf.geometry.y
    gdf["depth_color"] = _depth_color_arr(gdf["depth"].values)
    gdf["marker_size"] = gdf["mag"].clip(lower=1).values ** 1.8 * 3

    real_clusters = sorted(c for c in stats if c != -1)
    cluster_colors_map = {c: _CLUSTER_BORDER_COLORS[i % len(_CLUSTER_BORDER_COLORS)]
                          for i, c in enumerate(real_clusters)}

    fig, ax = plt.subplots(figsize=(18, 16))

    noise = gdf[gdf["cluster"] == -1]
    if not noise.empty:
        ax.scatter(noise["x"], noise["y"], s=4, c="#cccccc", alpha=0.2, zorder=1, edgecolors="none")

    for c in real_clusters:
        sub = gdf[gdf["cluster"] == c]
        border_color = cluster_colors_map[c]

        ax.scatter(
            sub["x"], sub["y"],
            s=sub["marker_size"],
            c=sub["depth_color"],
            alpha=0.7,
            edgecolors="none",
            zorder=3 + c,
        )

        try:
            from shapely.geometry import MultiPoint
            pts = np.column_stack([sub["x"].values, sub["y"].values])
            if len(pts) >= 3:
                hull_geom = MultiPoint(pts.tolist()).convex_hull
                hull_gdf = gpd.GeoDataFrame(geometry=[hull_geom], crs="EPSG:3857")
                hull_gdf.plot(
                    ax=ax, facecolor="none",
                    edgecolor=border_color, linewidth=2,
                    linestyle="--", alpha=0.6, zorder=2,
                )
        except Exception:
            pass

        center = sub.geometry.unary_union.centroid
        s = stats[c]
        short_name = s["name"].split("\n")[0].strip()
        ax.annotate(
            f"{short_name}\n{s['count']:,} events",
            xy=(center.x, center.y),
            fontsize=9, fontweight="bold",
            color=border_color,
            ha="center", va="center",
            bbox=dict(boxstyle="round,pad=0.4", facecolor="white", edgecolor=border_color, alpha=0.9, linewidth=1.5),
            zorder=20,
        )

    if faults_gdf is not None:
        faults_gdf = faults_gdf.to_crs(epsg=3857) if faults_gdf.crs != "EPSG:3857" else faults_gdf
        faults_gdf.plot(ax=ax, color="purple", linewidth=1.2, alpha=0.6, zorder=2)

    if show_dams:
        dams_df = _load_dams()
        if dams_df is not None and not dams_df.empty:
            dams_gdf = gpd.GeoDataFrame(
                dams_df,
                geometry=gpd.points_from_xy(dams_df.longitude, dams_df.latitude),
                crs="EPSG:4326",
            ).to_crs(epsg=3857)
            ax.scatter(
                dams_gdf.geometry.x, dams_gdf.geometry.y,
                s=40, c="#2196F3", marker="^", alpha=0.8,
                edgecolors="white", linewidths=0.5, zorder=15,
                label=f"Dams ({len(dams_gdf)})",
            )
            logger.info(f"Plotted {len(dams_gdf)} dams on cluster map")

    margin = 400000
    ax.set_xlim(gdf.total_bounds[0] - margin, gdf.total_bounds[2] + margin)
    ax.set_ylim(gdf.total_bounds[1] - margin, gdf.total_bounds[3] + margin)

    try:
        ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik, zoom=7)
    except Exception as e:
        logger.warning(f"Basemap download failed ({e}), saving without basemap")

    ax.set_axis_off()
    ax.set_title(title, fontsize=18, fontweight="bold", pad=20)

    # ---- LEGEND 1: Depth colors (upper-left) ----
    depth_handles = []
    for lo, hi, color, label in _DEPTH_BANDS:
        depth_handles.append(
            Line2D([0], [0], marker="o", color="w", markerfacecolor=color,
                   markersize=12, label=label, markeredgecolor="grey", markeredgewidth=0.5)
        )
    depth_handles.append(
        Line2D([0], [0], marker="o", color="w", markerfacecolor="#cccccc",
               markersize=6, label="Unclustered", alpha=0.5)
    )
    if faults_gdf is not None:
        depth_handles.append(Line2D([0], [0], color="purple", linewidth=2, label="Fault Lines"))
    if show_dams:
        depth_handles.append(
            Line2D([0], [0], marker="^", color="w", markerfacecolor="#2196F3",
                   markersize=10, label="Dams", markeredgecolor="grey", markeredgewidth=0.5)
        )

    legend1 = ax.legend(
        handles=depth_handles,
        loc="upper left",
        fontsize=11,
        title="Earthquake Depth",
        title_fontsize=13,
        framealpha=0.95,
        edgecolor="black",
        fancybox=True,
        shadow=True,
        borderpad=1,
        labelspacing=0.8,
        handletextpad=1,
    )
    legend1.get_title().set_fontweight("bold")

    # ---- LEGEND 2: Magnitude size (lower-left) ----
    mag_sizes = [(2.0, "M 2"), (4.0, "M 4"), (6.0, "M 6"), (8.0, "M 8")]
    mag_handles = []
    for mag, label in mag_sizes:
        s = mag ** 1.8 * 3
        mag_handles.append(
            Line2D([0], [0], marker="o", color="w", markerfacecolor="#888888",
                   markersize=np.sqrt(s) * 0.8, label=label,
                   markeredgecolor="grey", markeredgewidth=0.5)
        )
    legend2 = ax.legend(
        handles=mag_handles,
        loc="lower left",
        fontsize=11,
        title="Magnitude (dot size)",
        title_fontsize=13,
        framealpha=0.95,
        edgecolor="black",
        fancybox=True,
        shadow=True,
        borderpad=1,
        labelspacing=0.8,
        handletextpad=1,
    )
    legend2.get_title().set_fontweight("bold")
    ax.add_artist(legend1)

    # ---- LEGEND 3: Cluster regions (upper-right) ----
    region_handles = []
    for c in real_clusters:
        s = stats[c]
        color = cluster_colors_map[c]
        short = s["name"].split("\n")[0].strip()
        mag_info = f"M{s['mag_max']:.1f}"
        if s["max_mag_date"]:
            mag_info += f" ({s['max_mag_date']})"
        region_handles.append(
            Patch(facecolor=color, edgecolor="black", linewidth=0.8, alpha=0.7,
                  label=f"{short}: {s['count']:,}, {mag_info}")
        )
    legend3 = ax.legend(
        handles=region_handles,
        loc="upper right",
        fontsize=9,
        title="Seismic Zones",
        title_fontsize=12,
        framealpha=0.95,
        edgecolor="black",
        fancybox=True,
        shadow=True,
        borderpad=1,
        labelspacing=0.6,
    )
    legend3.get_title().set_fontweight("bold")
    ax.add_artist(legend2)

    # ---- FOOTER ----
    note = (
        f"DBSCAN density-based clustering (eps={eps}, min_samples={min_samples}) | "
        f"Color = depth | Size = magnitude | "
        f"Total: {len(df):,} earthquakes in {len(real_clusters)} zones | "
        f"Data: mmeq.akze.net"
    )
    fig.text(0.5, 0.01, note, ha="center", fontsize=9, fontstyle="italic", color="#444444")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        plt.savefig(filename, dpi=200, bbox_inches="tight", facecolor="white")
    plt.close()

    num_noise = (df["cluster"] == -1).sum()
    logger.info(f"Clustered: {len(real_clusters)} clusters, {num_noise} noise -> {filename}")
    for c in real_clusters:
        s = stats[c]
        logger.info(
            f"  {s['name'].split(chr(10))[0]}: {s['count']:,} events, "
            f"Max M{s['mag_max']:.1f} ({s['max_mag_date']}), "
            f"Avg depth {s['avg_depth']:.0f}km"
        )
