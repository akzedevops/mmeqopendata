import logging
import os
from typing import Optional

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

logger = logging.getLogger(__name__)


def build_dashboard(
    df: pd.DataFrame,
    risk_df: Optional[pd.DataFrame] = None,
    forecast_params: Optional[dict] = None,
    forecast_df: Optional[pd.DataFrame] = None,
    output_path: str = "dashboard.html",
) -> str:
    df = df.copy()
    df["time_utc"] = pd.to_datetime(df["time_utc"])

    fig = make_subplots(
        rows=3, cols=2,
        subplot_titles=(
            "Earthquake Timeline (Magnitude vs Time)",
            "Depth Distribution",
            "Frequency-Magnitude Distribution",
            "Magnitude vs Depth",
            "Monthly Earthquake Count",
            "Dam Seismic Risk (Top 20)" if risk_df is not None else "Top Earthquakes",
        ),
        specs=[
            [{"type": "scatter"}, {"type": "histogram"}],
            [{"type": "bar"}, {"type": "scatter"}],
            [{"type": "bar"}, {"type": "bar"}],
        ],
        vertical_spacing=0.08,
        horizontal_spacing=0.12,
    )

    colors = {
        "shallow": "#d62728",
        "intermediate": "#ff7f0e",
        "deep": "#1f77b4",
        "very_deep": "#08306b",
    }

    def depth_color(d):
        if d < 30:
            return colors["shallow"]
        elif d < 70:
            return colors["intermediate"]
        elif d < 300:
            return colors["deep"]
        return colors["very_deep"]

    dc = [depth_color(d) for d in df["depth"]]

    fig.add_trace(
        go.Scatter(
            x=df["time_utc"], y=df["mag"],
            mode="markers",
            marker=dict(size=np.clip(df["mag"] ** 1.5 * 0.5, 3, 20), color=dc, opacity=0.6),
            text=[f"M{m:.1f}, {d:.0f}km, {t}" for m, d, t in zip(df["mag"], df["depth"], df["time_utc"].dt.strftime("%Y-%m-%d"))],
            name="Earthquakes",
            hovertemplate="%{text}<extra></extra>",
        ),
        row=1, col=1,
    )

    fig.add_trace(
        go.Histogram(
            x=df["depth"],
            nbinsx=50,
            marker_color="#1f77b4",
            name="Depth",
        ),
        row=1, col=2,
    )

    mag_bins = np.arange(df["mag"].min(), df["mag"].max() + 0.1, 0.1)
    counts, edges = np.histogram(df["mag"], bins=mag_bins)
    centers = (edges[:-1] + edges[1:]) / 2
    log_counts = np.log10(np.maximum(counts, 0.5))

    fig.add_trace(
        go.Bar(
            x=centers, y=counts,
            marker_color="#2ca02c",
            name="FMD",
        ),
        row=2, col=1,
    )

    fig.add_trace(
        go.Scatter(
            x=df["depth"], y=df["mag"],
            mode="markers",
            marker=dict(size=3, color="#d62728", opacity=0.3),
            name="Mag vs Depth",
        ),
        row=2, col=2,
    )

    monthly = df.set_index("time_utc").resample("ME").size()
    fig.add_trace(
        go.Bar(
            x=monthly.index, y=monthly.values,
            marker_color="#9467bd",
            name="Monthly Count",
        ),
        row=3, col=1,
    )

    if risk_df is not None and not risk_df.empty:
        top = risk_df.head(20)
        bar_colors = {
            "Critical": "#d62728",
            "High": "#ff7f0e",
            "Moderate": "#ffdd57",
            "Low": "#2ca02c",
        }
        fig.add_trace(
            go.Bar(
                x=top["name"],
                y=top["composite_risk"],
                marker_color=[bar_colors.get(g, "#888") for g in top["risk_grade"]],
                text=top["risk_grade"],
                name="Dam Risk",
            ),
            row=3, col=2,
        )

    fig.update_xaxes(title_text="Time", row=1, col=1)
    fig.update_yaxes(title_text="Magnitude", row=1, col=1)
    fig.update_xaxes(title_text="Depth (km)", row=1, col=2)
    fig.update_yaxes(title_text="Count", row=1, col=2)
    fig.update_xaxes(title_text="Magnitude", row=2, col=1)
    fig.update_yaxes(title_text="Count", row=2, col=1)
    fig.update_xaxes(title_text="Depth (km)", row=2, col=2)
    fig.update_yaxes(title_text="Magnitude", row=2, col=2)
    fig.update_xaxes(title_text="Month", row=3, col=1)
    fig.update_yaxes(title_text="Count", row=3, col=1)
    fig.update_xaxes(title_text="Dam", tickangle=45, row=3, col=2)
    fig.update_yaxes(title_text="Risk Score", row=3, col=2)

    fig.update_layout(
        title_text="Myanmar Earthquake & Dam Risk Dashboard",
        title_font_size=20,
        height=1200,
        showlegend=False,
        template="plotly_white",
    )

    fig.write_html(output_path, include_plotlyjs="cdn")
    logger.info(f"Dashboard saved -> {output_path}")
    return output_path
