"""Regression tests for CSV export dedup/overwrite behavior.

Guards the monthly-CSV duplicate-accumulation bug: monthly files are re-fetched
in full each run and must be overwritten, not appended.
"""
import json
import os
import stat

import pandas as pd
import pytest

import mmeq.export.writer as writer
from mmeq.export.writer import (
    load_combined_json,
    merge_combined_json,
    rebuild_combined,
    save_combined_json,
    save_to_csv,
)


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


def test_rebuild_reconciles_json_to_csv_and_sorts(tmp_path):
    # Two monthly files (out of chronological order) + a stale, under-populated combined
    # JSON — the live bug. rebuild_combined must union the monthly data, dedup by id,
    # sort by time_utc, and write CSV and JSON with identical record counts.
    monthly = tmp_path / "csv" / "monthly"
    monthly.mkdir(parents=True)
    (tmp_path / "json" / "combined").mkdir(parents=True)
    (tmp_path / "csv" / "combined").mkdir(parents=True)

    pd.DataFrame({"id": ["b1", "b2"], "time_utc": ["2025-02-02 00:00:00", "2025-02-01 00:00:00"],
                  "mag": [5.0, 4.0]}).to_csv(monthly / "earthquakes_2025_02.csv", index=False)
    pd.DataFrame({"id": ["a1"], "time_utc": ["2025-01-01 00:00:00"], "mag": [3.0]}
                 ).to_csv(monthly / "earthquakes_2025_01.csv", index=False)
    # stale combined JSON with only 1 of the 3 events (mimics the 421-vs-9390 drift)
    (tmp_path / "json" / "combined" / "earthquakes_combined.json").write_text(
        json.dumps({"earthquakes": [{"id": "a1", "time_utc": "2025-01-01 00:00:00", "mag": 3.0}]}))

    n = rebuild_combined(export_dir=str(tmp_path))
    assert n == 3

    csv_df = pd.read_csv(tmp_path / "csv" / "combined" / "earthquakes_combined.csv")
    js = json.loads((tmp_path / "json" / "combined" / "earthquakes_combined.json").read_text())
    assert len(csv_df) == 3 and len(js["earthquakes"]) == 3, "JSON reconciled to CSV count"
    assert list(csv_df["time_utc"]) == sorted(csv_df["time_utc"]), "sorted chronologically"


def test_atomic_write_leaves_no_tmp(tmp_path):
    # A successful write leaves the file and no leftover *.tmp.
    path = str(tmp_path / "out.csv")
    save_to_csv(_frame(["a", "b"]), path, overwrite=True)
    assert os.path.exists(path)
    assert not any(f.endswith(".tmp") for f in os.listdir(tmp_path)), "no temp file left behind"


def test_atomic_write_produces_world_readable_file(tmp_path):
    # mkstemp creates 0600 temps and os.replace preserves the mode; the final
    # file must be 0644 like the repo's committed data files.
    path = str(tmp_path / "out.csv")
    save_to_csv(_frame(["a"]), path, overwrite=True)
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o644


def test_save_combined_json_round_trips(tmp_path):
    # The combined JSON must round-trip through the reader and use the exact
    # _dump_json envelope/formatting shared by every other JSON writer.
    path = str(tmp_path / "earthquakes_combined.json")
    records = [
        {"id": "a", "time_utc": "2025-05-01 00:00:00", "mag": 5.0, "location": "မန္တလေး"},
        {"id": "b", "time_utc": "2025-05-02 00:00:00", "mag": 4.2, "location": "Sagaing"},
    ]
    save_combined_json(records, path)

    assert load_combined_json(path) == records
    with open(path, encoding="utf-8") as f:
        content = f.read()
    expected = json.dumps({"earthquakes": records}, indent=2, ensure_ascii=False)
    assert content == expected, "must match the _dump_json formatting used elsewhere"
    assert "မန္တလေး" in content, "ensure_ascii=False must be preserved"
    assert not any(f.endswith(".tmp") for f in os.listdir(tmp_path))


def test_save_combined_json_is_atomic(tmp_path, monkeypatch):
    # A crash mid-write must leave the previous combined JSON intact: a
    # truncated file parses as [] in load_combined_json and would silently
    # erase the accumulated history on the next merge.
    path = str(tmp_path / "earthquakes_combined.json")
    save_combined_json([{"id": "a", "mag": 5.0}], path)

    def exploding_dump(payload, p):
        with open(p, "w", encoding="utf-8") as f:
            f.write('{"earthquakes": [{"id":')  # truncated garbage
        raise OSError("disk full")

    monkeypatch.setattr(writer, "_dump_json", exploding_dump)
    with pytest.raises(OSError):
        save_combined_json([{"id": "b", "mag": 6.0}], path)

    assert load_combined_json(path) == [{"id": "a", "mag": 5.0}], "history preserved"
    assert not any(f.endswith(".tmp") for f in os.listdir(tmp_path)), "temp cleaned up"
