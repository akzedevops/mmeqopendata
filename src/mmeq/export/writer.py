import os
import glob
import json
import logging
import tempfile
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


def _atomic_write(path: str, write_fn) -> None:
    """Write via a temp file in the same directory, then os.replace into place.

    Guarantees readers never see a partially-written file: a crash/OOM mid-write
    leaves the previous file intact instead of corrupting the canonical store.
    """
    directory = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=directory, suffix=".tmp")
    os.close(fd)
    try:
        write_fn(tmp)
        # mkstemp creates 0600 temp files and os.replace preserves the mode;
        # match the 0644 the repo's committed data files use.
        os.chmod(tmp, 0o644)
        os.replace(tmp, path)
    except Exception:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise


def _dump_json(payload: dict, path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def _dedup_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Drop duplicate events, keying on the stable event ``id`` when present.

    Keying on ``id`` (and keeping the last occurrence) means a revised event —
    same id but updated mag/depth/geocoding — collapses to one row instead of
    accumulating a near-duplicate, which an all-column drop_duplicates misses.
    """
    if "id" in df.columns:
        return df.drop_duplicates(subset=["id"], keep="last")
    return df.drop_duplicates()


def deduplicate_csv(path: str) -> None:
    if not os.path.exists(path):
        return
    try:
        df = pd.read_csv(path, on_bad_lines="skip")
        before = len(df)
        df = _dedup_frame(df)
        if len(df) < before:
            _atomic_write(path, lambda p: df.to_csv(p, index=False))
            logger.info(f"Deduplicated {path}: {before} -> {len(df)} rows")
    except Exception as e:
        logger.warning(f"Failed to deduplicate {path}: {e}")


def save_to_csv(
    df: pd.DataFrame,
    path: str,
    dedup: bool = False,
    overwrite: bool = False,
) -> None:
    """Write ``df`` to ``path``.

    overwrite=True writes only ``df`` (used for monthly files, which are
    re-fetched in full each run — appending would accumulate duplicates).
    overwrite=False merges with any existing file (used for the accumulating
    yearly/combined files), deduplicating on the event ``id`` when ``dedup``.
    """
    if df.empty:
        return
    if not overwrite and os.path.exists(path):
        existing = pd.read_csv(path, on_bad_lines="skip")
        df = pd.concat([existing, df], ignore_index=True)
    if dedup:
        df = _dedup_frame(df)
    _atomic_write(path, lambda p: df.to_csv(p, index=False))


def save_to_json(df: pd.DataFrame, path: str) -> None:
    if df.empty:
        return
    payload = {"earthquakes": df.to_dict(orient="records")}
    _atomic_write(path, lambda p: _dump_json(payload, p))


def save_combined_json(records: List[dict], path: str) -> None:
    """Atomically write the combined-JSON store ({"earthquakes": records}).

    Must go through _atomic_write: a truncated combined JSON is silently treated
    as empty by load_combined_json, which would erase the accumulated history on
    the next merge.
    """
    payload = {"earthquakes": records}
    _atomic_write(path, lambda p: _dump_json(payload, p))


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
    # Key on the stable event id when present so a revised event replaces its
    # prior version; later records (new_records) win. Fall back to a
    # coordinate/time key for legacy records that predate the id column.
    merged_by_key = {}
    order = []
    for record in existing + new_records:
        key = record.get("id") or (
            record.get("time_utc"), record.get("latitude"), record.get("longitude")
        )
        if key not in merged_by_key:
            order.append(key)
        merged_by_key[key] = record
    return [merged_by_key[k] for k in order]


def rebuild_combined(export_dir: Optional[str] = None) -> int:
    """Rebuild the combined CSV + JSON from the union of all monthly CSVs and the
    existing combined CSV, deduped by event id and sorted by time_utc.

    Reconciles drift between the two combined stores (the JSON had silently fallen
    far behind the CSV) and re-sorts the catalog chronologically (it was written in
    thread-completion order). Both files are written atomically.
    """
    if export_dir is None:
        export_dir = EXPORT_DIR
    sources = sorted(glob.glob(os.path.join(export_dir, "csv", "monthly", "*.csv")))
    combined_csv = os.path.join(export_dir, "csv", "combined", "earthquakes_combined.csv")
    if os.path.exists(combined_csv):
        sources.append(combined_csv)

    frames = []
    for src in sources:
        try:
            frames.append(pd.read_csv(src, on_bad_lines="skip"))
        except Exception as e:
            logger.warning(f"rebuild: skipping unreadable {src}: {e}")
    if not frames:
        logger.warning("rebuild: no source CSVs found under %s", export_dir)
        return 0

    combined = pd.concat(frames, ignore_index=True)
    combined = _dedup_frame(combined)
    if "time_utc" in combined.columns:
        combined = combined.sort_values("time_utc", kind="stable").reset_index(drop=True)

    combined_json = os.path.join(export_dir, "json", "combined", "earthquakes_combined.json")
    _atomic_write(combined_csv, lambda p: combined.to_csv(p, index=False))
    payload = {"earthquakes": combined.to_dict(orient="records")}
    _atomic_write(combined_json, lambda p: _dump_json(payload, p))
    logger.info("Rebuilt combined catalog: %d events (CSV + JSON reconciled)", len(combined))
    return len(combined)
