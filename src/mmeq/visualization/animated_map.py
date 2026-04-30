import logging
import os
from typing import Optional

import pandas as pd
import folium
from folium.plugins import TimestampedGeoJson

from mmeq.config import MAP_CENTER, MAP_ZOOM, FAULT_LINES_PATH

logger = logging.getLogger(__name__)


def build_animated_map(
    df: pd.DataFrame,
    output_path: str = "animated_earthquake_map.html",
    fault_lines_path: Optional[str] = None,
    min_mag: float = 0.0,
) -> str:
    df = df[df["mag"] >= min_mag].copy()
    df["time_utc"] = pd.to_datetime(df["time_utc"])

    if fault_lines_path is None:
        fault_lines_path = FAULT_LINES_PATH

    quake_map = folium.Map(
        location=MAP_CENTER,
        zoom_start=MAP_ZOOM,
        tiles="CartoDB positron",
    )

    features = []
    for _, row in df.iterrows():
        t = row["time_utc"]
        if pd.isna(t):
            continue
        mag = row["mag"]
        depth = row["depth"]
        radius = max(3, min(15, mag * 1.5))
        if mag < 3:
            color = "green"
        elif mag < 5:
            color = "orange"
        else:
            color = "red"

        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [float(row["longitude"]), float(row["latitude"])],
            },
            "properties": {
                "time": t.isoformat(),
                "style": {
                    "color": color,
                    "radius": radius,
                    "fillOpacity": 0.7,
                    "weight": 1,
                },
                "icon": "circle",
                "popup": f"M{mag:.1f}, {depth:.0f}km depth",
            },
        })

    timestamps_json = {
        "type": "FeatureCollection",
        "features": features,
    }

    TimestampedGeoJson(
        timestamps_json,
        period="P1M",
        add_last_point=True,
        auto_play=False,
        loop=False,
        max_speed=10,
        loop_button=True,
        date_options="YYYY-MM",
        time_slider_drag_update=True,
    ).add_to(quake_map)

    if os.path.exists(fault_lines_path):
        folium.GeoJson(
            fault_lines_path,
            style_function=lambda x: {"color": "purple", "weight": 2, "opacity": 0.7},
            name="Fault Lines",
        ).add_to(quake_map)

    folium.LayerControl().add_to(quake_map)

    title_html = '<h3 align="center" style="font-size:18px"><b>Myanmar Earthquake Timeline</b></h3>'
    quake_map.get_root().html.add_child(folium.Element(title_html))

    quake_map.save(output_path)
    logger.info(f"Animated map saved -> {output_path} ({len(features)} events)")
    return output_path
