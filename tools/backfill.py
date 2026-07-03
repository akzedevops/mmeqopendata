#!/usr/bin/env python3
"""Month-range backfill — spec 001 R11.

Re-fetch a range of months and MERGE the result into the monthly files, deduped by event
id (keep-last). This recovers **late-arriving and revised events**: the API has no record
cap (it serves the full catalog in one request — the earlier "500-cap" was a measurement
artifact), so the real gap is events that arrived after their month was last fetched
(month-boundary stragglers) or whose attributes the API later revised.

Behavior:
- Merges fresh data into the existing monthly file and dedups by id, so events are never
  dropped — the count can only stay the same or rise.
- Because dedup is keep-last, a re-fetch of already-known events with *revised* mag/depth/
  geocoding updates the CSV content even when the event count is unchanged; the monthly
  JSON is regenerated from the merged CSV on every fetch so the two never drift.
- (`fetch_quake_data_complete` is a plain fetch unless MMEQ_API_PAGE_CAP is set positive.)

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

# Allow running without installing the package (mmeq lives under src/).
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from mmeq.config import EXPORT_DIR
from mmeq.export.fetcher import fetch_quake_data_complete
from mmeq.export.writer import save_to_csv, save_to_json, validate_quake_data

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("backfill")


def _row_count(path: str) -> int:
    """Parsed row count (not physical lines, so embedded newlines don't skew it)."""
    if not os.path.exists(path):
        return 0
    try:
        return len(pd.read_csv(path, on_bad_lines="skip"))
    except Exception:
        return 0


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

        if df.empty:
            total_before += before
            total_after += before
            continue

        # Merge with existing (overwrite=False) + dedup by id → strictly additive.
        save_to_csv(df, monthly_csv, dedup=True, overwrite=False)
        after = _row_count(monthly_csv)
        # Always regenerate the monthly JSON from the merged CSV — content can change
        # (revised mag/depth/geocoding via keep-last dedup) even when the count does not,
        # so gating JSON on a count delta would let CSV and JSON drift.
        if os.path.exists(monthly_csv):
            save_to_json(pd.read_csv(monthly_csv, on_bad_lines="skip"), monthly_json)
        if after != before:
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
