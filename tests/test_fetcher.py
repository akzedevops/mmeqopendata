"""Regression tests for the fetcher: last-updated read and 500-cap bisection."""
from datetime import date, timedelta

import pandas as pd
import pytest

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


def test_bisection_disabled_by_default(monkeypatch):
    # The API has no cap; bisection is off by default (API_PAGE_CAP=0). A large response
    # must NOT trigger bisection — a single fetch returns everything.
    calls = []

    def fake_fetch(from_date, to_date):
        calls.append((from_date, to_date))
        return pd.DataFrame({"id": list(range(2000)), "time": ["t"] * 2000})

    monkeypatch.setattr(fetcher, "API_PAGE_CAP", 0)  # default
    monkeypatch.setattr(fetcher, "fetch_quake_data", fake_fetch)
    df = fetcher.fetch_quake_data_complete("2025-01-01", "2025-12-31")
    assert len(df) == 2000
    assert len(calls) == 1, "with the cap disabled, never bisect even a huge window"


class _FakeResp:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


def _raw_v1_record(rid: str) -> dict:
    """A raw v1-shaped record as /api/v2/export serves it: string-typed values,
    canonical key order, "time" always present."""
    return {
        "time": "2026-06-01T02:21:31.000Z", "latitude": "24.441", "longitude": "94.449",
        "depth": "90", "mag": "3.6", "magType": "ml", "nst": "", "gap": "", "dmin": "",
        "rms": "", "net": "", "id": rid, "updated": "2026-06-01T08:07:42.000Z",
        "location": "Min Thar", "place": "Myanmar", "country": "MM",
        "continent": "Asia", "type": "earthquake", "timeAdded": "", "timestamp": "",
        "locationInferred": "false", "state": "Sagaing", "initialPosition": "",
        "shakemapURL": "", "shakemapLastUpdated": "",
    }


class _FakeV2Session:
    """Serves a raw-v1-record catalog page by page, recording requested params."""

    def __init__(self):
        self.calls = []
        self.events = [_raw_v1_record(f"a{i}") for i in range(1, 4)]

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, dict(params or {})))
        offset, limit = int(params["offset"]), int(params["limit"])
        batch = self.events[offset:offset + limit]
        return _FakeResp({
            "meta": {"count": len(batch), "total": len(self.events),
                     "limit": limit, "offset": offset},
            "earthquakes": batch,
        })


def test_v2_fetch_hits_export_route_and_converts_window(monkeypatch):
    fake = _FakeV2Session()
    monkeypatch.setattr(fetcher, "API_V2_URL", "https://mmeq.example/api/v2")
    monkeypatch.setattr(fetcher, "get_session", lambda: fake)

    df = fetcher.fetch_quake_data("2026-06-01", "2026-06-02")

    # /export route with from + half-open to (inclusive v1 to converts to +1 day)
    assert all(url == "https://mmeq.example/api/v2/export" for url, _ in fake.calls)
    assert all(p["to"] == "2026-06-03" and p["from"] == "2026-06-01" for _, p in fake.calls)
    assert {"limit", "offset"} <= set(fake.calls[0][1])
    assert sorted(df["id"]) == ["a1", "a2", "a3"]


def test_v2_records_pass_through_verbatim(monkeypatch):
    fake = _FakeV2Session()
    monkeypatch.setattr(fetcher, "API_V2_URL", "https://mmeq.example/api/v2")
    monkeypatch.setattr(fetcher, "get_session", lambda: fake)

    df = fetcher.fetch_quake_data("2026-06-01", "2026-06-02")

    # Column order == the record's canonical key order (pandas derives CSV order from it)
    assert list(df.columns) == list(_raw_v1_record("x").keys())
    # String-typed values and legacy fields survive untouched — no mapping, no blanking
    row = df.loc[df["id"] == "a1"].iloc[0]
    assert row["depth"] == "90" and row["mag"] == "3.6"
    assert row["continent"] == "Asia" and row["state"] == "Sagaing"
    assert row["locationInferred"] == "false"
    assert row["time"] == "2026-06-01T02:21:31.000Z"


def test_v2_pagination_uses_multiple_calls(monkeypatch):
    fake = _FakeV2Session()
    monkeypatch.setattr(fetcher, "API_V2_URL", "https://mmeq.example/api/v2")
    monkeypatch.setattr(fetcher, "get_session", lambda: fake)

    # 12,000 distinct events: exceeds the 10k page size, forcing a second page.
    fake.events = [_raw_v1_record(f"id{i}") for i in range(12000)]
    df = fetcher.fetch_quake_data("2026-06-01", "2026-06-02")
    assert len(df) == 12000
    assert df["id"].nunique() == 12000, "pages must union without duplication"
    assert len(fake.calls) == 2, "meta.total must drive exactly two 10k pages"


def test_v2_meta_null_does_not_crash(monkeypatch):
    # The API may serve "meta": null; the fetch must fall back to the batch
    # length instead of raising AttributeError on None.get(...).
    class _MetaNullSession:
        def get(self, url, params=None, timeout=None):
            return _FakeResp({
                "meta": None,
                "earthquakes": [_raw_v1_record("a1"), _raw_v1_record("a2")],
            })

    monkeypatch.setattr(fetcher, "API_V2_URL", "https://mmeq.example/api/v2")
    monkeypatch.setattr(fetcher, "get_session", lambda: _MetaNullSession())

    df = fetcher.fetch_quake_data("2026-06-01", "2026-06-02")
    assert sorted(df["id"]) == ["a1", "a2"]


def test_v2_runaway_server_raises_instead_of_looping(monkeypatch):
    # A pathological server that keeps returning non-empty pages with
    # total > offset must trip the page cap, not loop forever.
    class _RunawaySession:
        def __init__(self):
            self.calls = 0

        def get(self, url, params=None, timeout=None):
            self.calls += 1
            offset = int(params["offset"])
            return _FakeResp({
                "meta": {"total": offset + 10},  # always advertises more
                "earthquakes": [_raw_v1_record(f"r{offset}")],
            })

    fake = _RunawaySession()
    monkeypatch.setattr(fetcher, "API_V2_URL", "https://mmeq.example/api/v2")
    monkeypatch.setattr(fetcher, "get_session", lambda: fake)
    monkeypatch.setattr(fetcher, "MAX_V2_PAGES", 7)

    with pytest.raises(ValueError, match="exceeded 7 pages"):
        fetcher.fetch_quake_data("2026-06-01", "2026-06-02")
    assert fake.calls == 7, "must stop exactly at the page cap"


def test_v1_path_when_v2_disabled(monkeypatch):
    calls = []

    class _V1Session:
        def get(self, url, timeout=None, params=None):
            calls.append(url)
            return _FakeResp({"earthquakes": [{"id": "x", "time": "t"}]})

    monkeypatch.setattr(fetcher, "API_V2_URL", "")
    monkeypatch.setattr(fetcher, "get_session", lambda: _V1Session())
    df = fetcher.fetch_quake_data("2026-06-01", "2026-06-02")
    assert len(df) == 1 and "myanmar-quakes" in calls[0]
