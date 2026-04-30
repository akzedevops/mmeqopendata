import logging
import os
from typing import Optional

import pandas as pd
import folium
from folium.plugins import HeatMap, MarkerCluster

from mmeq.config import (
    MAP_CENTER,
    MAP_ZOOM,
    MAG_THRESHOLDS,
    MAG_COLORS,
    FAULT_LINES_PATH,
    OUTPUT_DIR,
)

logger = logging.getLogger(__name__)


def _mag_color(mag: float) -> str:
    if mag < MAG_THRESHOLDS["low"]:
        return MAG_COLORS["low"]
    elif mag < MAG_THRESHOLDS["medium"]:
        return MAG_COLORS["medium"]
    return MAG_COLORS["high"]


def _mag_radius(mag: float) -> float:
    return max(2, min(12, mag * 1.5))


def build_earthquake_map(
    df: pd.DataFrame,
    output_path: Optional[str] = None,
    fault_lines_path: Optional[str] = None,
    min_mag: float = 0.0,
    show_heatmap: bool = True,
    show_markers: bool = True,
    cluster_markers: bool = True,
) -> str:
    if "time_utc" not in df.columns or "mag" not in df.columns:
        raise ValueError("DataFrame must have 'time_utc' and 'mag' columns")

    if output_path is None:
        output_path = os.path.join(OUTPUT_DIR, "enhanced_earthquake_map.html")
    if fault_lines_path is None:
        fault_lines_path = FAULT_LINES_PATH

    df = df[df["mag"] >= min_mag].copy()
    df["time_utc"] = pd.to_datetime(df["time_utc"])

    start_date = df["time_utc"].min().strftime("%B %Y")
    end_date = df["time_utc"].max().strftime("%B %Y")

    quake_map = folium.Map(
        location=MAP_CENTER,
        zoom_start=MAP_ZOOM,
        tiles="OpenStreetMap",
    )

    if show_markers:
        marker_layer = folium.FeatureGroup(name="Earthquake Markers")
        if cluster_markers:
            cluster = MarkerCluster(name="Earthquake Clusters").add_to(marker_layer)
            target = cluster
        else:
            target = marker_layer

        for _, row in df.iterrows():
            color = _mag_color(row["mag"])
            radius = _mag_radius(row["mag"])
            folium.CircleMarker(
                location=[row["latitude"], row["longitude"]],
                radius=radius,
                color=color,
                fill=True,
                fill_opacity=0.7,
                popup=(
                    f"<b>Magnitude:</b> {row['mag']}<br>"
                    f"<b>Depth:</b> {row['depth']} km<br>"
                    f"<b>Time (MMT):</b> {row.get('time_mmt', 'N/A')}"
                ),
            ).add_to(target)
        marker_layer.add_to(quake_map)

    if os.path.exists(fault_lines_path):
        fault_lines_layer = folium.FeatureGroup(name="Fault Lines")
        folium.GeoJson(
            fault_lines_path,
            style_function=lambda x: {
                "color": "purple",
                "weight": 2,
                "opacity": 0.7,
            },
        ).add_to(fault_lines_layer)
        fault_lines_layer.add_to(quake_map)
    else:
        logger.warning(f"Fault lines file not found: {fault_lines_path}")

    if show_heatmap:
        heatmap_layer = folium.FeatureGroup(name="Earthquake Heatmap")
        heat_data = df[["latitude", "longitude"]].values.tolist()
        HeatMap(heat_data, radius=10, blur=15, max_zoom=10).add_to(heatmap_layer)
        heatmap_layer.add_to(quake_map)

    folium.LayerControl().add_to(quake_map)

    legend_html = f"""
    <div style="position: fixed; bottom: 50px; left: 50px; width: 210px; z-index:9999;
                font-size:14px; background-color: white; opacity: 0.9; padding: 10px; border-radius: 5px;">
        <b>Earthquake Magnitude Legend</b><br>
        <i style="background: green; width: 10px; height: 10px; border-radius: 50%; display: inline-block;"></i> Magnitude &lt; {MAG_THRESHOLDS['low']}<br>
        <i style="background: orange; width: 10px; height: 10px; border-radius: 50%; display: inline-block;"></i> {MAG_THRESHOLDS['low']} &le; Magnitude &lt; {MAG_THRESHOLDS['medium']}<br>
        <i style="background: red; width: 10px; height: 10px; border-radius: 50%; display: inline-block;"></i> Magnitude &ge; {MAG_THRESHOLDS['medium']}<br>
        <i style="background: purple; width: 15px; height: 3px; display: inline-block;"></i> Fault Lines<br><br>
        <b>Data Range:</b><br>{start_date} – {end_date}
    </div>
    """
    quake_map.get_root().html.add_child(folium.Element(legend_html))

    title_html = f"""
    <h3 align="center" style="font-size:20px">
    <b>Myanmar Earthquake Visualization<br>({start_date} – {end_date})</b></h3>
    """
    quake_map.get_root().html.add_child(folium.Element(title_html))

    quake_map.save(output_path)
    logger.info(f"Saved earthquake map -> {output_path}")
    return output_path
