"""Regression test for get_last_updated_date reading the full (unsorted) catalog."""
import pandas as pd

import mmeq.export.fetcher as fetcher


def test_last_updated_uses_global_max_not_tail(tmp_path, monkeypatch):
    # Combined file where the NEWEST event is the first physical row (the file is
    # built in completion order and never sorted). A tail-only read would miss it.
    rows = [{"time_utc": "2026-06-15 00:00:00"}]  # newest, at the top
    rows += [{"time_utc": f"2020-01-{d:02d} 00:00:00"} for d in range(1, 28)] * 30  # 800+ older rows
    csv = tmp_path / "earthquakes_combined.csv"
    pd.DataFrame(rows).to_csv(csv, index=False)

    monkeypatch.setattr(fetcher, "COMBINED_CSV", str(csv))
    last = fetcher.get_last_updated_date()
    assert str(last) == "2026-06-15", "must return the global max date, not the tail's"


def test_last_updated_falls_back_when_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(fetcher, "COMBINED_CSV", str(tmp_path / "nope.csv"))
    last = fetcher.get_last_updated_date()
    assert last.year == fetcher.START_YEAR
