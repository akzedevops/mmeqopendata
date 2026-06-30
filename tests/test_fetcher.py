"""Regression tests for the fetcher: last-updated read and 500-cap bisection."""
from datetime import date, timedelta

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


def test_bisection_recovers_capped_window(monkeypatch):
    # The API caps at API_PAGE_CAP records per window. Stub it with a small cap and a
    # fake API that returns one record per day in the window: a wide window therefore
    # "hits the cap" and must be bisected until each piece is under it. The union must
    # recover every day exactly once.
    monkeypatch.setattr(fetcher, "API_PAGE_CAP", 5)

    def fake_fetch(from_date, to_date):
        d0 = date.fromisoformat(from_date)
        d1 = date.fromisoformat(to_date)
        days = [(d0 + timedelta(days=i)).isoformat() for i in range((d1 - d0).days + 1)]
        return pd.DataFrame({"id": days, "time": days})

    monkeypatch.setattr(fetcher, "fetch_quake_data", fake_fetch)

    df = fetcher.fetch_quake_data_complete("2025-03-01", "2025-03-31")
    assert df["id"].nunique() == 31, "bisection must recover all 31 days despite the cap"
    assert len(df) == 31, "disjoint windows -> no duplicates"


def test_no_bisection_when_under_cap(monkeypatch):
    calls = []

    def fake_fetch(from_date, to_date):
        calls.append((from_date, to_date))
        return pd.DataFrame({"id": ["a", "b"], "time": ["t", "t"]})  # 2 < cap

    monkeypatch.setattr(fetcher, "API_PAGE_CAP", 500)
    monkeypatch.setattr(fetcher, "fetch_quake_data", fake_fetch)
    df = fetcher.fetch_quake_data_complete("2025-01-01", "2025-01-31")
    assert len(df) == 2
    assert calls == [("2025-01-01", "2025-01-31")], "must not bisect an under-cap window"
