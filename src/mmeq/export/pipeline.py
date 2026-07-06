"""Export pipeline orchestration.

Extracted verbatim from ``cli.cmd_export`` (audit M1). ``run_export`` performs
the parallel month-by-month fetch, then the monthly/yearly/combined aggregation
and atomic writes, preserving the original raising/exit semantics exactly
(``sys.exit(1)`` on any month failure, identical log lines). The ``--rebuild``
path stays in ``cli.cmd_export``.
"""
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from mmeq.config import EXPORT_DIR
from mmeq.export.fetcher import fetch_quake_data_complete, generate_date_ranges
from mmeq.export.writer import (
    validate_quake_data,
    save_to_csv,
    save_to_json,
    save_merged_json,
    load_combined_json,
    merge_combined_json,
    save_combined_json,
)


def run_export(workers: int) -> None:
    date_ranges = generate_date_ranges()
    logging.info(f"Processing {len(date_ranges)} months of earthquake data...")

    if not date_ranges:
        logging.info("No new data to process.")
        return

    all_frames = []
    yearly_frames = {}

    def process_month(year, month, from_date, to_date):
        df_raw = fetch_quake_data_complete(from_date, to_date)
        df_valid = validate_quake_data(df_raw)
        return year, month, df_valid

    has_error = False
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(process_month, y, m, fd, td): (y, m)
            for y, m, fd, td in date_ranges
        }
        for future in as_completed(futures):
            try:
                year, month, df_valid = future.result()
            except Exception:
                has_error = True
                y, m = futures[future]
                logging.error(f"Failed to process {y}-{m:02d}")
                continue

            if df_valid.empty:
                continue

            monthly_csv = os.path.join(
                EXPORT_DIR, "csv/monthly", f"earthquakes_{year}_{month:02d}.csv"
            )
            monthly_json = os.path.join(
                EXPORT_DIR, "json/monthly", f"earthquakes_{year}_{month:02d}.json"
            )
            save_to_csv(df_valid, monthly_csv, overwrite=True)
            save_to_json(df_valid, monthly_json)
            yearly_frames.setdefault(year, []).append(df_valid)
            all_frames.append(df_valid)
            logging.info(f"Updated {year}-{month:02d}: {len(df_valid)} records")

    if not all_frames:
        logging.info("No new data to process.")
        if has_error:
            sys.exit(1)
        return

    logging.info("Saving yearly and combined files...")
    for year, frames in yearly_frames.items():
        ydf = pd.concat(frames, ignore_index=True)
        save_to_csv(
            ydf,
            os.path.join(EXPORT_DIR, "csv/yearly", f"earthquakes_{year}.csv"),
            dedup=True,
        )
        # Merge (not overwrite): a run only fetches recent windows, and the
        # yearly JSON must accumulate the whole year like the yearly CSV does.
        save_merged_json(
            ydf,
            os.path.join(EXPORT_DIR, "json/yearly", f"earthquakes_{year}.json"),
        )

    combined_df = pd.concat(all_frames, ignore_index=True)
    combined_csv = os.path.join(EXPORT_DIR, "csv/combined/earthquakes_combined.csv")
    combined_json = os.path.join(EXPORT_DIR, "json/combined/earthquakes_combined.json")

    save_to_csv(combined_df, combined_csv, dedup=True)

    existing_records = load_combined_json(combined_json)
    new_records = combined_df.to_dict(orient="records")
    merged = merge_combined_json(existing_records, new_records)
    save_combined_json(merged, combined_json)

    logging.info("All done! Earthquake data is up to date!")
    if has_error:
        sys.exit(1)
