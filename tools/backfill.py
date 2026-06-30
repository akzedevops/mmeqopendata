#!/usr/bin/env python3
"""One-time bisecting backfill — spec 001 R11.

Re-fetch a range of months with window bisection (`fetch_quake_data_complete`) and
MERGE the result into the monthly files, deduped by event id. This recovers events the
API silently truncated during past dense windows (the daily fetch had no bisection then).

Strictly additive: it merges fresh data into the existing monthly file and dedups by id
(keep-last), so an event count can only stay the same or rise — it never drops events,
even if a fresh fetch comes back smaller than what accreted on disk.

After running over the needed ranges, run `mmeq export --rebuild` to reconcile the
combined CSV+JSON from the updated monthly files.

Usage:
  python tools/backfill.py --from 2025-01 --to 2026-06
"""
import argparse
import logging
import os
import sys
from datetime import date, timedelta

import pandas as pd
from dateutil.relativedelta import relativedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.mmeq.config import EXPORT_DIR
from src.mmeq.export.fetcher import fetch_quake_data_complete
from src.mmeq.export.writer import save_to_csv, save_to_json, validate_quake_data

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("backfill")


def _row_count(path: str) -> int:
    if not os.path.exists(path):
        return 0
    with open(path) as f:
        return max(0, sum(1 for _ in f) - 1)


def _month_starts(from_ym: str, to_ym: str):
    cur = date.fromisoformat(from_ym + "-01")
    end = date.fromisoformat(to_ym + "-01")
    while cur <= end:
        yield cur
        cur += relativedelta(months=1)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--from", dest="from_ym", required=True, help="First month YYYY-MM")
    ap.add_argument("--to", dest="to_ym", required=True, help="Last month YYYY-MM (inclusive)")
    args = ap.parse_args()

    total_before = total_after = 0
    changed = []
    for first in _month_starts(args.from_ym, args.to_ym):
        last = (first + relativedelta(months=1)) - timedelta(days=1)
        from_date, to_date = first.isoformat(), last.isoformat()
        monthly_csv = os.path.join(EXPORT_DIR, "csv", "monthly", f"earthquakes_{first.year}_{first.month:02d}.csv")
        monthly_json = os.path.join(EXPORT_DIR, "json", "monthly", f"earthquakes_{first.year}_{first.month:02d}.json")

        before = _row_count(monthly_csv)
        try:
            df = validate_quake_data(fetch_quake_data_complete(from_date, to_date))
        except Exception as e:
            log.warning("fetch failed for %s..%s: %s (leaving existing file untouched)", from_date, to_date, e)
            continue

        # Merge with existing (overwrite=False) + dedup by id → strictly additive.
        save_to_csv(df, monthly_csv, dedup=True, overwrite=False)
        after = _row_count(monthly_csv)
        if after != before and os.path.exists(monthly_csv):
            # keep the monthly JSON consistent with the merged CSV
            save_to_json(pd.read_csv(monthly_csv, on_bad_lines="skip"), monthly_json)
            changed.append((f"{first.year}-{first.month:02d}", before, after))
            log.info("%s: %d -> %d (+%d)", from_date[:7], before, after, after - before)

        total_before += before
        total_after += after

    log.info("Backfill %s..%s done: %d -> %d (+%d) across %d changed months",
             args.from_ym, args.to_ym, total_before, total_after,
             total_after - total_before, len(changed))
    for ym, b, a in changed:
        print(f"  {ym}: {b} -> {a} (+{a - b})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
