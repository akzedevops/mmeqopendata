import os
import sys
import requests
import pandas as pd
import logging
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib3.util import Retry
from requests.adapters import HTTPAdapter
import pytz
import json
from typing import List, Tuple, Dict

# --- CONFIG SECTION ---
API_URL = "https://mmeq.akze.net/api/myanmar-quakes"
START_YEAR = 1950
END_DATE = datetime.now(timezone.utc) - timedelta(days=1)
EXPORT_DIR = "quake_exports"
LOG_FILE = "dataexport.log"

EXPORT_SUBDIRS = [
    "json/monthly", "json/yearly", "json/combined",
    "csv/monthly", "csv/yearly", "csv/combined"
]

MIN_LAT, MAX_LAT = -90, 90
MIN_LON, MAX_LON = -180, 180
MIN_DEPTH, MAX_DEPTH = 0, 700
MIN_MAG, MAX_MAG = 0, 10

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
for subdir in EXPORT_SUBDIRS:
    os.makedirs(os.path.join(EXPORT_DIR, subdir), exist_ok=True)

# --- HTTP SESSION with retry + connection pooling ---
def _build_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[502, 503, 504],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session

_session = _build_session()


def get_last_updated_date() -> datetime.date:
    """Return the last updated date from the combined CSV, or START_YEAR if not found."""
    path = os.path.join(EXPORT_DIR, "csv/combined/earthquakes_combined.csv")
    try:
        if not os.path.exists(path):
            raise FileNotFoundError()
        # Read only the time column; use tail to avoid loading all rows into memory
        row_count = sum(1 for _ in open(path)) - 1  # exclude header
        skip = max(0, row_count - 500)
        df = pd.read_csv(path, usecols=["time_utc"], skiprows=range(1, skip + 1) if skip else None)
        df["time_utc"] = pd.to_datetime(df["time_utc"], errors="coerce", utc=True)
        last_date = df["time_utc"].max().date()
        logging.info(f"Last updated date: {last_date}")
        return last_date
    except Exception as e:
        logging.warning(f"Fallback to start year due to: {e}")
        return datetime(START_YEAR, 1, 1).date()


def generate_date_ranges() -> List[Tuple[int, int, str, str]]:
    """
    Generate a list of (year, month, from_date, to_date) tuples for months
    needing update, starting from the last updated date.
    """
    last_updated = get_last_updated_date()
    date_ranges = []
    current = last_updated + timedelta(days=1)
    while current <= END_DATE.date():
        dt_from = current.replace(day=1)
        dt_to = (dt_from + relativedelta(months=1)) - timedelta(days=1)
        from_date = dt_from.strftime("%Y-%m-%d")
        to_date = dt_to.strftime("%Y-%m-%d")
        date_ranges.append((dt_from.year, dt_from.month, from_date, to_date))
        current += relativedelta(months=1)
    return date_ranges


def fetch_quake_data(from_date: str, to_date: str) -> pd.DataFrame:
    """Fetch earthquake data from API for the given date range."""
    url = f"{API_URL}?from={from_date}&to={to_date}"
    try:
        response = _session.get(url, timeout=(5, 30))
        response.raise_for_status()
        data = response.json()
        logging.info(f"✅ Data fetched for {from_date} → {to_date}")
        return pd.DataFrame(data.get("earthquakes", []))
    except Exception as e:
        logging.error(f"❌ Error fetching data ({from_date} → {to_date}): {e}")
        return pd.DataFrame()


def validate_quake_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate the earthquake DataFrame, add formatted columns."""
    if df.empty:
        return df
    df = df.copy()
    df["time"] = pd.to_datetime(df["time"], errors="coerce", utc=True)
    for col in ["latitude", "longitude", "depth", "mag"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df.dropna(subset=["time", "latitude", "longitude", "depth", "mag"], inplace=True)
    end_ts = pd.Timestamp(END_DATE).tz_localize("UTC") if pd.Timestamp(END_DATE).tzinfo is None else pd.Timestamp(END_DATE)
    df = df[df["time"].between(pd.Timestamp("1950-01-01", tz=utc_zone), end_ts)]
    # Apply bounds validation using the defined constants
    df = df[df["latitude"].between(MIN_LAT, MAX_LAT)]
    df = df[df["longitude"].between(MIN_LON, MAX_LON)]
    df = df[df["depth"].between(MIN_DEPTH, MAX_DEPTH)]
    df = df[df["mag"].between(MIN_MAG, MAX_MAG)]
    df["time_utc"] = df["time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df["time_mmt"] = df["time"].dt.tz_convert(myanmar_zone).dt.strftime("%Y-%m-%d %H:%M:%S")
    df.drop(columns=["time"], inplace=True)
    # Enrich with state/region and nearest city/village
    try:
        from mmeq.analysis.geocoder import enrich_dataframe
        df = enrich_dataframe(df)
    except Exception as e:
        logging.warning(f"Geocoding enrichment skipped: {e}")
    return df


def deduplicate_csv(path: str):
    """Remove any duplicate rows in a CSV file based on all columns."""
    if not os.path.exists(path):
        return
    try:
        df = pd.read_csv(path)
        df.drop_duplicates(inplace=True)
        df.to_csv(path, index=False)
    except Exception as e:
        logging.warning(f"Failed to deduplicate {path}: {e}")


def save_to_csv(df: pd.DataFrame, path: str, dedup: bool = False):
    """Append DataFrame to CSV. Pass dedup=True to deduplicate after writing."""
    if df.empty:
        return
    write_header = not os.path.exists(path)
    df.to_csv(path, mode="a", index=False, header=write_header)
    if dedup:
        deduplicate_csv(path)


def save_to_json(df: pd.DataFrame, path: str):
    """Save DataFrame as a JSON file."""
    if df.empty:
        return
    with open(path, "w") as f:
        json.dump({"earthquakes": df.to_dict(orient="records")}, f, indent=2)


def load_combined_json(path: str) -> List[dict]:
    """Load existing combined JSON records, returning an empty list if not found."""
    if not os.path.exists(path):
        return []
    try:
        with open(path) as f:
            return json.load(f).get("earthquakes", [])
    except Exception as e:
        logging.warning(f"Could not load existing combined JSON: {e}")
        return []


def process_month(year: int, month: int, from_date: str, to_date: str) -> Tuple[int, int, pd.DataFrame]:
    """Fetch, validate, and return quake data for a single month."""
    df_raw = fetch_quake_data(from_date, to_date)
    df_valid = validate_quake_data(df_raw)
    return (year, month, df_valid)


def main():
    date_ranges = generate_date_ranges()
    logging.info(f"🚀 Processing {len(date_ranges)} months of earthquake data...")

    all_frames: List[pd.DataFrame] = []
    yearly_frames: Dict[int, List[pd.DataFrame]] = {}
    has_error = False

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_month, y, m, fd, td): (y, m)
                   for y, m, fd, td in date_ranges}
        for future in as_completed(futures):
            try:
                year, month, df_valid = future.result()
            except Exception:
                has_error = True
                y, m = futures[future]
                logging.error(f"❌ Failed to process {y}-{m:02d}")
                continue
            if df_valid.empty:
                continue
            monthly_csv = os.path.join(EXPORT_DIR, "csv/monthly", f"earthquakes_{year}_{month:02d}.csv")
            monthly_json = os.path.join(EXPORT_DIR, "json/monthly", f"earthquakes_{year}_{month:02d}.json")
            save_to_csv(df_valid, monthly_csv)
            save_to_json(df_valid, monthly_json)
            yearly_frames.setdefault(year, []).append(df_valid)
            all_frames.append(df_valid)
            logging.info(f"✅ Updated {year}-{month:02d}: {len(df_valid)} records")

    if not all_frames:
        logging.info("🏁 No new data to process.")
        if has_error:
            sys.exit(1)
        return

    logging.info("\n📅 Saving yearly and combined files...")
    for year, frames in yearly_frames.items():
        ydf = pd.concat(frames, ignore_index=True)
        save_to_csv(ydf, os.path.join(EXPORT_DIR, "csv/yearly", f"earthquakes_{year}.csv"), dedup=True)
        save_to_json(ydf, os.path.join(EXPORT_DIR, "json/yearly", f"earthquakes_{year}.json"))

    combined_df = pd.concat(all_frames, ignore_index=True)
    combined_csv = os.path.join(EXPORT_DIR, "csv/combined/earthquakes_combined.csv")
    combined_json = os.path.join(EXPORT_DIR, "json/combined/earthquakes_combined.json")

    save_to_csv(combined_df, combined_csv, dedup=True)

    existing_records = load_combined_json(combined_json)
    new_records = combined_df.to_dict(orient="records")
    seen = set()
    merged = []
    for record in existing_records + new_records:
        key = (record.get("time_utc"), record.get("latitude"), record.get("longitude"))
        if key not in seen:
            seen.add(key)
            merged.append(record)
    with open(combined_json, "w", encoding="utf-8") as f:
        json.dump({"earthquakes": merged}, f, indent=2, ensure_ascii=False)

    logging.info("🏁 All done! Earthquake data is up to date!")
    if has_error:
        sys.exit(1)


if __name__ == "__main__":
    main()