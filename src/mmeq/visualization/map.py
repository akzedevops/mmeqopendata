import html
import logging
import os
from typing import Optional

import pandas as pd
import folium
from folium.plugins import HeatMap, MarkerCluster

from mmeq.analysis.dam_risk import load_dams_df
from mmeq.visualization.assets import copy_vendor, use_local_assets
from mmeq.config import (
    MAP_CENTER,
    MAP_ZOOM,
    MAG_THRESHOLDS,
    MAG_COLORS,
    FAULT_LINES_PATH,
    OUTPUT_DIR,
)

logger = logging.getLogger(__name__)


def _esc(value) -> str:
    """HTML-escape a data-derived string before popup interpolation.

    folium renders string popups verbatim inside a jQuery template literal in
    the published page, so unescaped OSM names / dam CSV fields / API strings
    are a stored-XSS vector on the public map (2026-07-06 audit, finding C7).
    Backticks are escaped too — a literal one would terminate the template
    literal and blank the map — and so is "$": inside a template literal
    ${expr} is EVALUATED as JavaScript, so an unescaped dollar sign is its own
    execution vector (Kilo review of PR #36). HTML entities are not decoded by
    the JS parser, so "&#36;" can never form "${" in the literal, while jQuery
    renders it back as a visible "$".
    """
    return html.escape(str(value)).replace("`", "&#96;").replace("$", "&#36;")


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
    dams_path: Optional[str] = None,
    min_mag: float = 0.0,
    show_heatmap: bool = True,
    show_markers: bool = True,
    cluster_markers: bool = True,
    show_dams: bool = True,
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
        tiles="CartoDB positron",
    )

    # folium JSCSSMixin instances whose default CDN links we rewrite to local
    # vendored assets at save time (see use_local_assets).
    asset_objects = [quake_map]

    if show_markers:
        marker_layer = folium.FeatureGroup(name="Earthquake Markers")
        if cluster_markers:
            cluster = MarkerCluster(name="Earthquake Clusters").add_to(marker_layer)
            asset_objects.append(cluster)
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
                    f"<b>Time (MMT):</b> {_esc(row.get('time_mmt', 'N/A'))}"
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

    dams_df = load_dams_df(dams_path) if show_dams else None
    if show_dams and dams_df is not None and not dams_df.empty:
        dams_layer = folium.FeatureGroup(name="Dams")
        for _, row in dams_df.iterrows():
            name = row["name"]
            status = row["status"]
            func = row["function"]
            capacity = row["capacity_mw"]
            height = row["height_m"]
            river = row["river"]
            popup_html = f"<b>Dam:</b> {_esc(name)}"
            if status:
                popup_html += f"<br><b>Status:</b> {_esc(status)}"
            if func:
                popup_html += f"<br><b>Function:</b> {_esc(func)}"
            if capacity:
                popup_html += f"<br><b>Capacity:</b> {_esc(capacity)} MW"
            if height:
                popup_html += f"<br><b>Height:</b> {_esc(height)} m"
            if river:
                popup_html += f"<br><b>River:</b> {_esc(river)}"
            icon_color = "blue"
            if status == "Complete":
                icon_color = "blue"
            elif status in ("Proposed", "Planned", "Identified Sites"):
                icon_color = "lightblue"
            elif status == "Under Construction":
                icon_color = "orange"
            elif status == "Suspended":
                icon_color = "red"
            folium.Marker(
                location=[row["latitude"], row["longitude"]],
                icon=folium.Icon(icon="tint", color=icon_color, prefix="fa"),
                popup=folium.Popup(popup_html, max_width=300),
            ).add_to(dams_layer)
        dams_layer.add_to(quake_map)
        logger.info(f"Added {len(dams_df)} dams to map")
    elif show_dams:
        logger.warning("Dams file not found (see config.DAMS_PATH_CANDIDATES)")

    if show_heatmap:
        heatmap_layer = folium.FeatureGroup(name="Earthquake Heatmap")
        heat_data = df[["latitude", "longitude"]].values.tolist()
        hm = HeatMap(heat_data, radius=10, blur=15, max_zoom=10)
        asset_objects.append(hm)
        hm.add_to(heatmap_layer)
        heatmap_layer.add_to(quake_map)

    # OSM critical infrastructure overlay
    try:
        from mmeq.analysis.osm_exposure import load_osm_infrastructure
        osm_df = load_osm_infrastructure()
        osm_colors = {"school": "#1565c0", "hospital": "#d32f2f", "clinic": "#e91e63",
                       "police": "#333", "fire_station": "#ff6f00", "university": "#6a1b9a",
                       "place_of_worship": "#78909c"}
        osm_layer = folium.FeatureGroup(name="OSM Buildings (34K)", show=False)
        for _, row in osm_df.iterrows():
            color = osm_colors.get(row["category"], "#888")
            folium.CircleMarker(
                location=[row["lat"], row["lon"]],
                radius=2,
                color=color,
                fill=True,
                fill_opacity=0.6,
                weight=0,
                popup=f"{_esc(row['category'])}: {_esc(row['name'])}" if row["name"] else _esc(row["category"]),
            ).add_to(osm_layer)
        osm_layer.add_to(quake_map)
        logger.info("Added %d OSM buildings to map", len(osm_df))
    except Exception as e:
        logger.warning("OSM overlay skipped: %s", e)

    folium.LayerControl().add_to(quake_map)

    legend_html = f"""
    <div style="position: fixed; bottom: 50px; left: 50px; width: 210px; z-index:9999;
                font-size:14px; background-color: white; opacity: 0.9; padding: 10px; border-radius: 5px;">
        <b>Earthquake Magnitude Legend</b><br>
        <i style="background: green; width: 10px; height: 10px; border-radius: 50%; display: inline-block;"></i> Magnitude &lt; {MAG_THRESHOLDS['low']}<br>
        <i style="background: orange; width: 10px; height: 10px; border-radius: 50%; display: inline-block;"></i> {MAG_THRESHOLDS['low']} &le; Magnitude &lt; {MAG_THRESHOLDS['medium']}<br>
        <i style="background: red; width: 10px; height: 10px; border-radius: 50%; display: inline-block;"></i> Magnitude &ge; {MAG_THRESHOLDS['medium']}<br>
        <i style="background: purple; width: 15px; height: 3px; display: inline-block;"></i> Fault Lines<br>
        <i style="background: #2196F3; width: 10px; height: 10px; border-radius: 50%; display: inline-block;"></i> Dams<br><br>
        <b>Data Range:</b><br>{start_date} – {end_date}
    </div>
    """
    quake_map.get_root().html.add_child(folium.Element(legend_html))

    title_html = f"""
    <h3 align="center" style="font-size:20px">
    <b>Myanmar Earthquake Visualization<br>({start_date} – {end_date})</b></h3>
    """
    quake_map.get_root().html.add_child(folium.Element(title_html))

    use_local_assets(*asset_objects)
    copy_vendor(os.path.dirname(os.path.abspath(output_path)))
    quake_map.save(output_path)
    logger.info(f"Saved earthquake map -> {output_path}")
    return output_path
