import os
import json
import time
import math
import logging
from typing import List, Tuple, Dict, Optional
from datetime import datetime, timedelta

import pytz
import requests
import pandas as pd
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CONFIG SECTION ---
API_URL = "https://mmeq.akze.net/api/myanmar-quakes"
# If there's no combined CSV, start from this year (inclusive)
START_YEAR = 1950
# Export up to yesterday (inclusive) to avoid partial "today"
END_DATE = (datetime.utcnow() - timedelta(days=1)).date()

EXPORT_DIR = "quake_exports"
LOG_FILE = "dataexport.log"

EXPORT_SUBDIRS = [
    "json/monthly", "json/yearly", "json/combined",
    "csv/monthly", "csv/yearly", "csv/combined",
]

# Optional bounds to keep data sane; adjust as needed
MIN_LAT, MAX_LAT = -90, 90
MIN_LON, MAX_LON = -180, 180
MIN_DEPTH, MAX_DEPTH = 0, 700
MIN_MAG, MAX_MAG = 0, 10

# Threads for parallel monthly fetching
MAX_WORKERS = min(8, os.cpu_count() or 4)

# Networking
HTTP_TIMEOUT = 30
RETRY_TIMES = 3
RETRY_BACKOFF = 2.0  # seconds

utc_zone = pytz.utc
myanmar_zone = pytz.timezone("Asia/Yangon")

# --- LOGGING SETUP ---
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
logging.getLogger().addHandler(console_handler)


# --- DIRECTORY SETUP ---
def ensure_directories() -> None:
    os.makedirs(EXPORT_DIR, exist_ok=True)
    for sub in EXPORT_SUBDIRS:
        os.makedirs(os.path.join(EXPORT_DIR, sub), exist_ok=True)


# --- UTILITIES ---
def _read_combined_last_date() -> Optional[datetime.date]:
    """
    Return the last updated date from the combined CSV, or None if not present.
    """
    path = os.path.join(EXPORT_DIR, "csv/combined/earthquakes_combined.csv")
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_csv(path, usecols=["time_utc"])
        df["time_utc"] = pd.to_datetime(df["time_utc"], errors="coerce", utc=True)
        if df["time_utc"].notna().any():
            return df["time_utc"].max().date()
        return None
    except Exception as e:
        logging.warning(f"Failed to read combined CSV for last date: {e}")
        return None


def get_last_updated_date() -> datetime.date:
    """
    Return the last updated date (UTC date) from the combined CSV, or START_YEAR if not found.
    """
    last = _read_combined_last_date()
    if last:
        logging.info(f"Last updated date (from combined CSV): {last}")
        return last
    fallback = datetime(START_YEAR, 1, 1).date()
    logging.info(f"No combined CSV found. Falling back to start year: {fallback}")
    return fallback


def generate_date_ranges() -> List[Tuple[int, int, str, str]]:
    """
    Generate (year, month, from_date, to_date) tuples for months needing update,
    starting from the first of the month after the last updated date up to END_DATE.
    """
    last_updated = get_last_updated_date()
    current = (last_updated.replace(day=1) + relativedelta(months=1))
    date_ranges: List[Tuple[int, int, str, str]] = []
    while current <= END_DATE:
        dt_from = current
        dt_to = (dt_from + relativedelta(months=1)) - timedelta(days=1)
        if dt_to > END_DATE:
            dt_to = END_DATE
        from_date = dt_from.strftime("%Y-%m-%d")
        to_date = dt_to.strftime("%Y-%m-%d")
        date_ranges.append((dt_from.year, dt_from.month, from_date, to_date))
        current += relativedelta(months=1)
    return date_ranges


def _http_get(url: str, params: Dict[str, str]) -> requests.Response:
    """
    GET with retries and simple exponential backoff.
    """
    last_exc: Optional[Exception] = None
    for attempt in range(1, RETRY_TIMES + 1):
        try:
            resp = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
            if resp.status_code >= 500:
                raise requests.HTTPError(f"Server error {resp.status_code}")
            return resp
        except Exception as e:
            last_exc = e
            sleep_for = RETRY_BACKOFF * (2 ** (attempt - 1))
            logging.warning(f"GET {url} failed (attempt {attempt}/{RETRY_TIMES}): {e}. Retrying in {sleep_for:.1f}s")
            time.sleep(sleep_for)
    assert last_exc is not None
    raise last_exc


def fetch_quake_data(from_date: str, to_date: str) -> pd.DataFrame:
    """
    Fetch earthquake data from API for the given date range [from_date, to_date].
    The API is expected to return JSON array of events.
    """
    params = {"from": from_date, "to": to_date}
    resp = _http_get(API_URL, params)
    try:
        data = resp.json()
    except Exception as e:
        raise RuntimeError(f"Failed to parse JSON for range {from_date}..{to_date}: {e}")

    if data is None:
        data = []
    if not isinstance(data, list):
        # Some APIs wrap in {"results": [...]}
        data = data.get("results", [])

    df = pd.json_normalize(data) if data else pd.DataFrame()
    df = normalize_quake_df(df)
    return df


def normalize_quake_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize columns and types to a consistent schema.

    Output columns:
      - id (optional if API provides one)
      - time_utc (UTC ISO time string)
      - time_yangon (Yangon ISO time string)
      - lat, lon (float)
      - depth_km (float)
      - mag (float)
      - place (optional)
      - source (optional)
      - raw_* (any unmapped fields preserved as JSON string if desired)
    """
    if df.empty:
        return df

    # Column mapping candidates
    colmap_candidates = {
        "id": ["id", "event_id", "quake_id"],
        "time": ["time_utc", "time", "datetime", "event_time", "timestamp"],
        "lat": ["lat", "latitude"],
        "lon": ["lon", "lng", "longitude"],
        "depth_km": ["depth_km", "depth"],
        "mag": ["mag", "magnitude", "mag_value"],
        "place": ["place", "location", "region"],
        "source": ["source", "network", "catalog"],
    }

    def pick(colnames: List[str]) -> Optional[str]:
        for c in colnames:
            if c in df.columns:
                return c
        return None

    # Prepare output dataframe
    out = pd.DataFrame(index=df.index.copy())

    # id
    id_col = pick(colmap_candidates["id"])
    if id_col and id_col in df.columns:
        out["id"] = df[id_col].astype(str)
    else:
        # will create later from composite key if needed
        out["id"] = pd.NA

    # lat, lon
    lat_col = pick(colmap_candidates["lat"])
    lon_col = pick(colmap_candidates["lon"])
    if lat_col in df.columns and lon_col in df.columns:
        out["lat"] = pd.to_numeric(df[lat_col], errors="coerce")
        out["lon"] = pd.to_numeric(df[lon_col], errors="coerce")
    else:
        out["lat"] = pd.NA
        out["lon"] = pd.NA

    # depth_km
    depth_col = pick(colmap_candidates["depth_km"])
    if depth_col in df.columns:
        out["depth_km"] = pd.to_numeric(df[depth_col], errors="coerce")
    else:
        out["depth_km"] = pd.NA

    # mag
    mag_col = pick(colmap_candidates["mag"])
    if mag_col in df.columns:
        out["mag"] = pd.to_numeric(df[mag_col], errors="coerce")
    else:
        out["mag"] = pd.NA

    # place, source
    place_col = pick(colmap_candidates["place"])
    source_col = pick(colmap_candidates["source"])
    out["place"] = df[place_col].astype(str) if place_col in df.columns else pd.NA
    out["source"] = df[source_col].astype(str) if source_col in df.columns else pd.NA

    # time
    time_col = pick(colmap_candidates["time"])
    time_series = pd.to_datetime(df[time_col], errors="coerce", utc=True) if time_col in df.columns else pd.NaT
    if isinstance(time_series, pd.Series):
        out["time_utc"] = time_series.dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        yangon_times = time_series.dt.tz_convert(myanmar_zone)
        out["time_yangon"] = yangon_times.dt.strftime("%Y-%m-%d %H:%M:%S%z")
    else:
        out["time_utc"] = pd.NA
        out["time_yangon"] = pd.NA

    # Filter out rows with invalid coordinates, mag, depth
    mask = (
        out["lat"].between(MIN_LAT, MAX_LAT, inclusive="both") &
        out["lon"].between(MIN_LON, MAX_LON, inclusive="both")
    )
    if "depth_km" in out.columns:
        mask = mask & out["depth_km"].between(MIN_DEPTH, MAX_DEPTH, inclusive="both")
    if "mag" in out.columns:
        mask = mask & out["mag"].between(MIN_MAG, MAX_MAG, inclusive="both")

    out = out[mask].copy()

    # Create composite key for missing ids
    def mk_key(row) -> str:
        parts = [
            str(row.get("time_utc", "")),
            f"{row.get('lat', ''):.4f}" if pd.notna(row.get("lat")) else "",
            f"{row.get('lon', ''):.4f}" if pd.notna(row.get("lon")) else "",
            f"{row.get('mag', ''):.2f}" if pd.notna(row.get("mag")) else "",
        ]
        return "|".join(parts)

    if out["id"].isna().any():
        out.loc[out["id"].isna(), "id"] = out[out["id"].isna()].apply(mk_key, axis=1)

    # Final canonical column order
    cols = ["id", "time_utc", "time_yangon", "lat", "lon", "depth_km", "mag", "place", "source"]
    # Include only available columns
    cols = [c for c in cols if c in out.columns]
    out = out[cols].dropna(subset=["time_utc"], how="any")

    # Ensure dtypes for CSV
    for num_col in ["lat", "lon", "depth_km", "mag"]:
        if num_col in out.columns:
            out[num_col] = pd.to_numeric(out[num_col], errors="coerce")

    return out.reset_index(drop=True)


def write_month(year: int, month: int, df: pd.DataFrame) -> None:
    """
    Write monthly CSV/JSON files for given year-month.
    """
    # Paths
    csv_path = os.path.join(EXPORT_DIR, "csv", "monthly", f"{year:04d}", f"{month:02d}.csv")
    json_path = os.path.join(EXPORT_DIR, "json", "monthly", f"{year:04d}", f"{month:02d}.json")

    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    os.makedirs(os.path.dirname(json_path), exist_ok=True)

    # Sort by time
    if "time_utc" in df.columns:
        df_sorted = df.sort_values("time_utc")
    else:
        df_sorted = df

    df_sorted.to_csv(csv_path, index=False)
    df_sorted.to_json(json_path, orient="records", force_ascii=False, date_format="iso")


def build_year(year: int) -> None:
    """
    Aggregate all monthly files for a given year into yearly CSV/JSON.
    """
    monthly_dir = os.path.join(EXPORT_DIR, "csv", "monthly", f"{year:04d}")
    if not os.path.isdir(monthly_dir):
        return

    frames = []
    for m in range(1, 13):
        mp = os.path.join(monthly_dir, f"{m:02d}.csv")
        if os.path.exists(mp):
            try:
                dfm = pd.read_csv(mp)
                frames.append(dfm)
            except Exception as e:
                logging.warning(f"Failed reading monthly CSV {mp}: {e}")

    if not frames:
        return

    yr = pd.concat(frames, ignore_index=True)
    yr = dedupe_events(yr)

    # Sort by time
    if "time_utc" in yr.columns:
        yr = yr.sort_values("time_utc")

    csv_path = os.path.join(EXPORT_DIR, "csv", "yearly", f"{year:04d}.csv")
    json_path = os.path.join(EXPORT_DIR, "json", "yearly", f"{year:04d}.json")
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    os.makedirs(os.path.dirname(json_path), exist_ok=True)

    yr.to_csv(csv_path, index=False)
    yr.to_json(json_path, orient="records", force_ascii=False, date_format="iso")


def build_combined() -> None:
    """
    Aggregate all yearly CSV files into a combined CSV/JSON.
    """
    yearly_dir = os.path.join(EXPORT_DIR, "csv", "yearly")
    if not os.path.isdir(yearly_dir):
        return

    frames = []
    for fname in sorted(os.listdir(yearly_dir)):
        if not fname.lower().endswith(".csv"):
            continue
        fp = os.path.join(yearly_dir, fname)
        try:
            dfy = pd.read_csv(fp)
        except Exception as e:
            logging.warning(f"Failed reading yearly CSV {fp}: {e}")
            continue
        frames.append(dfy)

    if not frames:
        return

    combined = pd.concat(frames, ignore_index=True)
    combined = dedupe_events(combined)
    if "time_utc" in combined.columns:
        combined = combined.sort_values("time_utc")

    csv_path = os.path.join(EXPORT_DIR, "csv", "combined", "earthquakes_combined.csv")
    json_path = os.path.join(EXPORT_DIR, "json", "combined", "earthquakes_combined.json")
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    os.makedirs(os.path.dirname(json_path), exist_ok=True)

    combined.to_csv(csv_path, index=False)
    combined.to_json(json_path, orient="records", force_ascii=False, date_format="iso")


def dedupe_events(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplicate events by 'id' if available; otherwise by composite of time_utc+lat+lon+mag.
    """
    if df.empty:
        return df

    work = df.copy()
    if "id" in work.columns and work["id"].notna().any():
        work = work.drop_duplicates(subset=["id"])
    else:
        cols = [c for c in ["time_utc", "lat", "lon", "mag"] if c in work.columns]
        if cols:
            work = work.drop_duplicates(subset=cols)
        else:
            work = work.drop_duplicates()
    return work.reset_index(drop=True)


def process_month(year: int, month: int, from_date: str, to_date: str) -> Tuple[int, int, int]:
    """
    Fetch and write a single month. Returns (year, month, count).
    """
    df = fetch_quake_data(from_date, to_date)
    if not df.empty:
        df = dedupe_events(df)
        write_month(year, month, df)
        return year, month, len(df)
    else:
        # Ensure empty files exist to mark processed month
        write_month(year, month, df)
        return year, month, 0


def main() -> None:
    ensure_directories()

    date_ranges = generate_date_ranges()
    if not date_ranges:
        logging.info("No new months to fetch. Nothing to do.")
        return

    logging.info(f"Fetching {len(date_ranges)} month(s) from API...")

    results: List[Tuple[int, int, int]] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_month, y, m, f, t): (y, m, f, t)
            for (y, m, f, t) in date_ranges
        }
        for fut in as_completed(futures):
            y, m, f, t = futures[fut]
            try:
                yy, mm, count = fut.result()
                logging.info(f"Processed {yy:04d}-{mm:02d}: {count} record(s)")
                results.append((yy, mm, count))
            except Exception as e:
                logging.error(f"Failed processing {y:04d}-{m:02d} ({f}..{t}): {e}")

    # Rebuild yearly aggregates for affected years
    affected_years = sorted({y for (y, _m, _c) in results})
    for y in affected_years:
        build_year(y)

    # Rebuild combined aggregate
    build_combined()
    logging.info("Data export completed.")


if __name__ == "__main__":
    main()
