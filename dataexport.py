#!/usr/bin/env python3

import os
import requests
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz
import json

# --- CONFIG SECTION ---
API_URL = "https://mmeq.akze.me/api/myanmar-quakes"
START_YEAR = 1950
END_YEAR = 2025
END_MONTH = 3
EXPORT_DIR = "quake_exports"

MIN_LAT, MAX_LAT = -90, 90
MIN_LON, MAX_LON = -180, 180
MIN_DEPTH, MAX_DEPTH = 0, 700
MIN_MAG, MAX_MAG = 0, 10

utc_zone = pytz.utc
myanmar_zone = pytz.timezone("Asia/Yangon")

for subdir in [
    "json/monthly", "json/yearly", "json/combined",
    "csv/monthly", "csv/yearly", "csv/combined"
]:
    os.makedirs(os.path.join(EXPORT_DIR, subdir), exist_ok=True)

def generate_date_ranges():
    date_ranges = []
    for year in range(START_YEAR, END_YEAR + 1):
        for month in range(1, 13):
            if (year == END_YEAR) and (month > END_MONTH):
                break
            dt_from = datetime(year, month, 1)
            dt_to = dt_from + relativedelta(months=1, days=-1)
            from_date = dt_from.strftime("%Y-%m-%d")
            to_date = dt_to.strftime("%Y-%m-%d")
            date_ranges.append((year, month, from_date, to_date))
    return date_ranges

def fetch_quake_data(from_date, to_date):
    url = f"{API_URL}?from={from_date}&to={to_date}"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        quakes = data.get("earthquakes", [])
        return pd.DataFrame(quakes)
    except Exception as e:
        print(f"❌ Error fetching data ({from_date} → {to_date}): {e}")
        return pd.DataFrame()

def validate_quake_data(df):
    if df.empty:
        return df

    df = df.copy()
    df["time"] = pd.to_datetime(df["time"], errors="coerce", utc=True)

    start_date = pd.Timestamp("1950-01-01", tz=utc_zone)
    end_date = pd.Timestamp("2025-12-31 23:59:59", tz=utc_zone)
    df = df[(df["time"] >= start_date) & (df["time"] <= end_date)].copy()

    numeric_cols = ["latitude", "longitude", "depth", "mag"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df.dropna(subset=numeric_cols + ["time"], inplace=True)

    mask_lat = df["latitude"].between(MIN_LAT, MAX_LAT)
    mask_lon = df["longitude"].between(MIN_LON, MAX_LON)
    mask_depth = df["depth"].between(MIN_DEPTH, MAX_DEPTH)
    mask_mag = df["mag"].between(MIN_MAG, MAX_MAG)

    df = df[mask_lat & mask_lon & mask_depth & mask_mag].copy()
    df.drop_duplicates(inplace=True)

    df["time_utc"] = df["time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df["time_mmt"] = df["time"].dt.tz_convert(myanmar_zone).dt.strftime("%Y-%m-%d %H:%M:%S")

    df.drop(columns=["time"], inplace=True)

    cols_order = ["time_utc", "time_mmt", "latitude", "longitude", "depth", "mag", "location", "country"]
    df = df[cols_order]

    return df

def save_to_csv(df, path):
    if df.empty:
        return
    write_header = not os.path.exists(path)
    df.to_csv(path, mode="a", index=False, header=write_header)

def save_to_json(df, path):
    if df.empty:
        return
    quake_list = df.to_dict(orient="records")
    with open(path, "w") as f:
        json.dump({"earthquakes": quake_list}, f, indent=2)

def main():
    date_ranges = generate_date_ranges()
    total = len(date_ranges)
    processed = 0

    combined_df = pd.DataFrame()
    yearly_dfs = {}

    print(f"🚀 Fetching and Validating {total} months of earthquake data...")

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_quake_data, fd, td): (y, m) for y, m, fd, td in date_ranges}

        for future in as_completed(futures):
            year, month = futures[future]
            df_raw = future.result()
            fetched = len(df_raw)
            df_valid = validate_quake_data(df_raw)
            valid = len(df_valid)

            processed += 1
            print(f"[{processed}/{total}] ✅ {year}-{month:02d} | Fetched: {fetched} → Valid: {valid}")

            if df_valid.empty:
                continue

            monthly_csv = os.path.join(EXPORT_DIR, "csv/monthly", f"earthquakes_{year}_{month:02d}.csv")
            monthly_json = os.path.join(EXPORT_DIR, "json/monthly", f"earthquakes_{year}_{month:02d}.json")

            save_to_csv(df_valid, monthly_csv)
            save_to_json(df_valid, monthly_json)

            yearly_dfs[year] = pd.concat([yearly_dfs.get(year, pd.DataFrame()), df_valid])
            combined_df = pd.concat([combined_df, df_valid])

    print("\n📅 Saving yearly files...")
    for year, ydf in yearly_dfs.items():
        print(f"   📁 Year {year} ({len(ydf)} records)")
        save_to_csv(ydf, os.path.join(EXPORT_DIR, "csv/yearly", f"earthquakes_{year}.csv"))
        save_to_json(ydf, os.path.join(EXPORT_DIR, "json/yearly", f"earthquakes_{year}.json"))

    print("\n📦 Saving combined files...")
    save_to_csv(combined_df, os.path.join(EXPORT_DIR, "csv/combined/earthquakes_combined.csv"))
    save_to_json(combined_df, os.path.join(EXPORT_DIR, "json/combined/earthquakes_combined.json"))

    print("\n🏁 All done! Data exported in quake_exports/{json,csv}/{monthly,yearly,combined}/")

if __name__ == "__main__":
    main()
