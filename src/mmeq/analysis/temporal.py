import logging
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Optional

from mmeq.config import OUTPUT_DIR

logger = logging.getLogger(__name__)


def monthly_frequency(
    df: pd.DataFrame,
    output_path: Optional[str] = None,
) -> pd.Series:
    if "time_utc" not in df.columns:
        raise ValueError("DataFrame must have 'time_utc' column")
    df = df.copy()
    df["time_utc"] = pd.to_datetime(df["time_utc"])
    monthly = df.set_index("time_utc").resample("ME")["mag"].count()

    if output_path is None:
        output_path = os.path.join(OUTPUT_DIR, "monthly_frequency_trends.png")

    plt.figure(figsize=(12, 6))
    monthly.plot(color="teal")
    plt.title("Monthly Frequency of Earthquakes")
    plt.xlabel("Year")
    plt.ylabel("Number of Earthquakes")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    logger.info(f"Saved monthly frequency chart -> {output_path}")
    return monthly


def magnitude_distribution(
    df: pd.DataFrame,
    output_path: Optional[str] = None,
) -> None:
    if "mag" not in df.columns:
        raise ValueError("DataFrame must have 'mag' column")

    if output_path is None:
        output_path = os.path.join(OUTPUT_DIR, "magnitude_distribution.png")

    plt.figure(figsize=(10, 6))
    sns.histplot(df["mag"], bins=30, kde=True, color="skyblue")
    plt.title("Magnitude Distribution of Earthquakes (Myanmar)")
    plt.xlabel("Magnitude")
    plt.ylabel("Frequency")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    logger.info(f"Saved magnitude distribution -> {output_path}")


def magnitude_vs_depth(
    df: pd.DataFrame,
    output_path: Optional[str] = None,
) -> None:
    if "mag" not in df.columns or "depth" not in df.columns:
        raise ValueError("DataFrame must have 'mag' and 'depth' columns")

    if output_path is None:
        output_path = os.path.join(OUTPUT_DIR, "mag_vs_depth_relationship.png")

    plt.figure(figsize=(10, 6))
    sns.scatterplot(x="depth", y="mag", data=df, alpha=0.6)
    plt.title("Magnitude vs. Depth of Earthquakes")
    plt.xlabel("Depth (km)")
    plt.ylabel("Magnitude")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    logger.info(f"Saved mag vs depth chart -> {output_path}")
