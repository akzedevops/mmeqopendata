import os
import requests
import pandas as pd
import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz
import json

# --- CONFIG SECTION ---
API_URL = "https://mmeq.akze.me/api/myanmar-quakes"
START_YEAR = 1950
END_DATE = datetime.utcnow() - timedelta(days=1)
EXPORT_DIR = "quake_exports"
LOG_FILE = "dataexport.log"

MIN_LAT, MAX_LAT = -90, 90
MIN_LON, MAX_LON = -180, 180
MIN_DEPTH, MAX_DEPTH = 0, 700
MIN_MAG, MAX_MAG = 0, 10

utc_zone = pytz.utc
myanmar_zone = pytz.timezone("Asia/Yangon")

# Logging setup
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
logging.getLogger().addHandler(console_handler)

# Ensure directories exist
for subdir in ["json/monthly", "json/yearly", "json/combined", "csv/monthly", "csv/yearly", "csv/combined"]:
    os.makedirs(os.path.join(EXPORT_DIR, subdir), exist_ok=True)

def get_last_updated_date():
    try:
        df = pd.read_csv(os.path.join(EXPORT_DIR, "csv/combined/earthquakes_combined.csv"))
        df["time_utc"] = pd.to_datetime(df["time_utc"], errors="coerce", utc=True)
        last_date = df["time_utc"].max().date()
        logging.info(f"Last updated date: {last_date}")
        return last_date
    except Exception:
        return datetime(START_YEAR, 1, 1).date()

def generate_date_ranges():
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

def fetch_quake_data(from_date, to_date):
    url = f"{API_URL}?from={from_date}&to={to_date}"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        logging.info(f"✅ Data fetched for {from_date} → {to_date}")
        return pd.DataFrame(data.get("earthquakes", []))
    except Exception as e:
        logging.error(f"❌ Error fetching data ({from_date} → {to_date}): {e}")
        return pd.DataFrame()

def validate_quake_data(df):
    if df.empty:
        return df
    df = df.copy()
    df["time"] = pd.to_datetime(df["time"], errors="coerce", utc=True)
    df.dropna(subset=["time", "latitude", "longitude", "depth", "mag"], inplace=True)
    df = df[df["time"].between(pd.Timestamp("1950-01-01", tz=utc_zone), pd.Timestamp(END_DATE, tz=utc_zone))]
    df["time_utc"] = df["time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df["time_mmt"] = df["time"].dt.tz_convert(myanmar_zone).dt.strftime("%Y-%m-%d %H:%M:%S")
    df.drop(columns=["time"], inplace=True)
    return df

def save_to_csv(df, path):
    if df.empty:
        return
    write_header = not os.path.exists(path)
    df.to_csv(path, mode="a", index=False, header=write_header)

def save_to_json(df, path):
    if df.empty:
        return
    with open(path, "w") as f:
        json.dump({"earthquakes": df.to_dict(orient="records")}, f, indent=2)

def main():
    date_ranges = generate_date_ranges()
    logging.info(f"🚀 Processing {len(date_ranges)} months of earthquake data...")
    combined_df = pd.DataFrame()
    yearly_dfs = {}
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_quake_data, fd, td): (y, m) for y, m, fd, td in date_ranges}
        for future in as_completed(futures):
            year, month = futures[future]
            df_raw = future.result()
            df_valid = validate_quake_data(df_raw)
            if df_valid.empty:
                continue
            monthly_csv = os.path.join(EXPORT_DIR, "csv/monthly", f"earthquakes_{year}_{month:02d}.csv")
            monthly_json = os.path.join(EXPORT_DIR, "json/monthly", f"earthquakes_{year}_{month:02d}.json")
            save_to_csv(df_valid, monthly_csv)
            save_to_json(df_valid, monthly_json)
            yearly_dfs[year] = pd.concat([yearly_dfs.get(year, pd.DataFrame()), df_valid])
            combined_df = pd.concat([combined_df, df_valid])
            logging.info(f"✅ Updated {year}-{month:02d}: {len(df_valid)} records")

    logging.info("\n📅 Saving yearly and combined files...")
    for year, ydf in yearly_dfs.items():
        save_to_csv(ydf, os.path.join(EXPORT_DIR, "csv/yearly", f"earthquakes_{year}.csv"))
        save_to_json(ydf, os.path.join(EXPORT_DIR, "json/yearly", f"earthquakes_{year}.json"))
    save_to_csv(combined_df, os.path.join(EXPORT_DIR, "csv/combined/earthquakes_combined.csv"))
    save_to_json(combined_df, os.path.join(EXPORT_DIR, "json/combined/earthquakes_combined.json"))
    logging.info("🏁 All done! Earthquake data is up to date!")

if __name__ == "__main__":
    main()
