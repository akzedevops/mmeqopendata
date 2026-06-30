"""Regression tests for CSV export dedup/overwrite behavior.

Guards the monthly-CSV duplicate-accumulation bug: monthly files are re-fetched
in full each run and must be overwritten, not appended.
"""
import pandas as pd

from mmeq.export.writer import save_to_csv, merge_combined_json


def _frame(ids):
    return pd.DataFrame({
        "id": ids,
        "time_utc": ["2025-05-01 00:00:00"] * len(ids),
        "latitude": [22.0] * len(ids),
        "longitude": [96.0] * len(ids),
        "mag": [5.0] * len(ids),
    })


def test_monthly_overwrite_does_not_accumulate(tmp_path):
    path = str(tmp_path / "earthquakes_2025_05.csv")
    df = _frame(["a", "b", "c"])
    # Simulate three CI runs re-fetching the same month.
    for _ in range(3):
        save_to_csv(df, path, overwrite=True)
    out = pd.read_csv(path)
    assert len(out) == 3, "overwrite must not append duplicates across runs"


def test_dedup_keys_on_id_and_keeps_last(tmp_path):
    path = str(tmp_path / "combined.csv")
    save_to_csv(_frame(["a", "b"]), path, dedup=True)
    # Re-fetch with a revised event "b" (same id, updated mag).
    revised = _frame(["b", "c"])
    revised.loc[revised["id"] == "b", "mag"] = 5.9
    save_to_csv(revised, path, dedup=True)
    out = pd.read_csv(path)
    assert sorted(out["id"]) == ["a", "b", "c"], "no duplicate event ids"
    assert out.loc[out["id"] == "b", "mag"].iloc[0] == 5.9, "revised event wins"


def test_merge_combined_json_dedups_on_id():
    existing = [{"id": "a", "mag": 5.0}, {"id": "b", "mag": 5.0}]
    new = [{"id": "b", "mag": 5.9}, {"id": "c", "mag": 4.0}]
    merged = merge_combined_json(existing, new)
    ids = [r["id"] for r in merged]
    assert ids == ["a", "b", "c"]
    assert next(r for r in merged if r["id"] == "b")["mag"] == 5.9
