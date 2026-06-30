import logging
import warnings
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

try:
    import plotly.graph_objects as go
    HAS_3D = True
except ImportError:
    HAS_3D = False


def depth_cross_section(
    df: pd.DataFrame,
    output_path: str = "depth_cross_section.html",
    lat_range: tuple = (18, 26),
    lon_range: tuple = (94, 100),
) -> str:
    if not HAS_3D:
        logger.error("plotly required for 3D visualization")
        return ""

    df = df.copy()
    df["time_utc"] = pd.to_datetime(df["time_utc"])
    sub = df[
        (df["latitude"] >= lat_range[0])
        & (df["latitude"] <= lat_range[1])
        & (df["longitude"] >= lon_range[0])
        & (df["longitude"] <= lon_range[1])
    ]

    def depth_color(d):
        if d < 30:
            return "Shallow"
        elif d < 70:
            return "Intermediate"
        else:
            return "Deep"

    sub = sub.copy()
    sub["depth_class"] = sub["depth"].apply(depth_color)

    color_map = {"Shallow": "#d62728", "Intermediate": "#ff7f0e", "Deep": "#1f77b4"}

    fig = go.Figure()

    for cls in ["Shallow", "Intermediate", "Deep"]:
        group = sub[sub["depth_class"] == cls]
        if group.empty:
            continue
        fig.add_trace(go.Scatter3d(
            x=group["longitude"],
            y=group["latitude"],
            z=-group["depth"],
            mode="markers",
            marker=dict(
                size=np.clip(np.maximum(group["mag"].values, 0.0) ** 1.5 * 0.8, 2, 15),
                color=color_map[cls],
                opacity=0.6,
            ),
            name=f"{cls} ({len(group)})",
            text=[
                f"M{m:.1f}, {d:.0f}km, {t}"
                for m, d, t in zip(group["mag"], group["depth"], group["time_utc"].dt.strftime("%Y-%m-%d"))
            ],
            hovertemplate="%{text}<extra></extra>",
        ))

    fig.update_layout(
        title="Myanmar Earthquake 3D Depth Cross-Section",
        scene=dict(
            xaxis_title="Longitude",
            yaxis_title="Latitude",
            zaxis_title="Depth (km)",
            camera=dict(eye=dict(x=1.5, y=1.5, z=0.8)),
        ),
        height=800,
        template="plotly_white",
    )

    fig.write_html(output_path, include_plotlyjs="cdn")
    logger.info(f"3D cross-section saved -> {output_path}")
    return output_path
