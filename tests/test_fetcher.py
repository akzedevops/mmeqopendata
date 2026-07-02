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


class _FakeV2Session:
    """Serves a 3-event catalog in pages of 2, recording requested params."""

    def __init__(self):
        self.calls = []
        self.events = [
            {"id": "a1", "time_utc": "2026-06-01T02:21:31Z", "latitude": 24.441,
             "longitude": 94.449, "depth_km": 90.0, "mag": 3.6, "mag_type": "ml",
             "location": "Min Thar", "place": "Myanmar", "country": "MM",
             "type": "earthquake", "net": None, "updated_at": "2026-06-01T08:07:42Z"},
            {"id": "a2", "time_utc": "2026-06-01T09:28:38Z", "latitude": 22.269,
             "longitude": 93.815, "depth_km": None, "mag": 3.0, "mag_type": None,
             "location": None, "place": None, "country": "MM", "type": "earthquake",
             "net": None, "updated_at": None},
            {"id": "a3", "time_utc": "2026-06-02T14:52:19.000Z", "latitude": 19.27,
             "longitude": 96.0, "depth_km": 10.0, "mag": 4.1, "mag_type": "mb",
             "location": "X", "place": "Myanmar", "country": "MM",
             "type": "earthquake", "net": "us", "updated_at": None},
        ]

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, dict(params or {})))
        offset, limit = int(params["offset"]), int(params["limit"])
        batch = self.events[offset:offset + limit]
        return _FakeResp({
            "meta": {"count": len(batch), "total": len(self.events),
                     "limit": limit, "offset": offset},
            "earthquakes": batch,
        })


def test_v2_fetch_maps_paginates_and_converts_window(monkeypatch):
    fake = _FakeV2Session()
    monkeypatch.setattr(fetcher, "API_V2_URL", "https://mmeq.example/api/v2")
    monkeypatch.setattr(fetcher, "get_session", lambda: fake)
    # Force pagination: page size 2 over 3 events.
    monkeypatch.setattr(fetcher, "_fetch_v2", fetcher._fetch_v2)

    df = fetcher.fetch_quake_data("2026-06-01", "2026-06-02")

    # inclusive v1 window converts to half-open: to = 2026-06-03
    assert all(c[1]["to"] == "2026-06-03" and c[1]["from"] == "2026-06-01" for c in fake.calls)
    assert sorted(df["id"]) == ["a1", "a2", "a3"]
    # v1 shape: 'time'/'depth'/'magType' columns exist; schema fillers present
    assert {"time", "depth", "magType", "nst", "shakemapURL"} <= set(df.columns)
    assert df.loc[df["id"] == "a1", "depth"].iloc[0] == 90.0
    # nulls become "" for string fields, never None-strings
    assert df.loc[df["id"] == "a2", "magType"].iloc[0] == ""


def test_v2_pagination_uses_multiple_calls(monkeypatch):
    fake = _FakeV2Session()
    monkeypatch.setattr(fetcher, "API_V2_URL", "https://mmeq.example/api/v2")
    monkeypatch.setattr(fetcher, "get_session", lambda: fake)

    # 12,000 distinct events: exceeds the 10k page size, forcing a second page.
    fake.events = [dict(fake.events[i % 3], id=f"id{i}") for i in range(12000)]
    df = fetcher.fetch_quake_data("2026-06-01", "2026-06-02")
    assert len(df) == 12000
    assert df["id"].nunique() == 12000, "pages must union without duplication"
    assert len(fake.calls) >= 2, "must paginate past the 10k page size"


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
