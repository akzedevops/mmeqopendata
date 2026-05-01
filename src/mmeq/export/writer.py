import os
import json
import logging
import pandas as pd
import pytz
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from mmeq.config import (
    EXPORT_DIR,
    MIN_LAT,
    MAX_LAT,
    MIN_LON,
    MAX_LON,
    MIN_DEPTH,
    MAX_DEPTH,
    MIN_MAG,
    MAX_MAG,
    COMBINED_CSV,
    COMBINED_JSON,
    START_YEAR,
)

logger = logging.getLogger(__name__)

utc_zone = pytz.utc
myanmar_zone = pytz.timezone("Asia/Yangon")


def validate_quake_data(
    df: pd.DataFrame,
    end_date: Optional[datetime] = None,
) -> pd.DataFrame:
    if df.empty:
        return df
    if end_date is None:
        end_date = datetime.now(timezone.utc) - timedelta(days=1)
    df = df.copy()
    df["time"] = pd.to_datetime(df["time"], errors="coerce", utc=True)
    for col in ["latitude", "longitude", "depth", "mag"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df.dropna(subset=["time", "latitude", "longitude", "depth", "mag"], inplace=True)
    end_ts = pd.Timestamp(end_date).tz_localize("UTC") if pd.Timestamp(end_date).tzinfo is None else pd.Timestamp(end_date)
    df = df[
        df["time"].between(
            pd.Timestamp("1950-01-01", tz=utc_zone),
            end_ts,
        )
    ]
    df = df[df["latitude"].between(MIN_LAT, MAX_LAT)]
    df = df[df["longitude"].between(MIN_LON, MAX_LON)]
    df = df[df["depth"].between(MIN_DEPTH, MAX_DEPTH)]
    df = df[df["mag"].between(MIN_MAG, MAX_MAG)]
    df["time_utc"] = df["time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df["time_mmt"] = (
        df["time"].dt.tz_convert(myanmar_zone).dt.strftime("%Y-%m-%d %H:%M:%S")
    )
    df.drop(columns=["time"], inplace=True)

    # Enrich with state/region and nearest city
    try:
        from mmeq.analysis.geocoder import enrich_dataframe
        df = enrich_dataframe(df)
    except Exception as e:
        logger.warning("Geocoding enrichment skipped: %s", e)

    return df


def deduplicate_csv(path: str) -> None:
    if not os.path.exists(path):
        return
    try:
        df = pd.read_csv(path, on_bad_lines="skip")
        before = len(df)
        df.drop_duplicates(inplace=True)
        if len(df) < before:
            df.to_csv(path, index=False)
            logger.info(f"Deduplicated {path}: {before} -> {len(df)} rows")
    except Exception as e:
        logger.warning(f"Failed to deduplicate {path}: {e}")


def save_to_csv(
    df: pd.DataFrame,
    path: str,
    dedup: bool = False,
) -> None:
    if df.empty:
        return
    write_header = not os.path.exists(path)
    df.to_csv(path, mode="a", index=False, header=write_header)
    if dedup:
        deduplicate_csv(path)


def save_to_json(df: pd.DataFrame, path: str) -> None:
    if df.empty:
        return
    with open(path, "w", encoding="utf-8") as f:
        json.dump(
            {"earthquakes": df.to_dict(orient="records")},
            f,
            indent=2,
            ensure_ascii=False,
        )


def load_combined_json(path: Optional[str] = None) -> List[dict]:
    if path is None:
        path = COMBINED_JSON
    if not os.path.exists(path):
        return []
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f).get("earthquakes", [])
    except Exception as e:
        logger.warning(f"Could not load existing combined JSON: {e}")
        return []


def merge_combined_json(
    existing: List[dict],
    new_records: List[dict],
) -> List[dict]:
    seen = set()
    merged = []
    for record in existing + new_records:
        key = (record.get("time_utc"), record.get("latitude"), record.get("longitude"))
        if key not in seen:
            seen.add(key)
            merged.append(record)
    return merged
