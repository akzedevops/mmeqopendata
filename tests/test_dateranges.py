import os
import json
import pandas as pd
import pytest
from datetime import datetime, timezone, timedelta

from mmeq.export.fetcher import generate_date_ranges, get_last_updated_date
from mmeq.export.writer import (
    save_to_csv,
    save_to_json,
    load_combined_json,
    deduplicate_csv,
    merge_combined_json,
)


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "latitude": [21.0, 22.0],
        "longitude": [96.0, 97.0],
        "depth": [10.0, 15.0],
        "mag": [3.5, 4.0],
        "time_utc": ["2025-01-01 00:00:00", "2025-01-02 00:00:00"],
        "time_mmt": ["2025-01-01 06:30:00", "2025-01-02 06:30:00"],
    })


@pytest.fixture
def tmp_csv(tmp_path):
    return str(tmp_path / "test.csv")


@pytest.fixture
def tmp_json(tmp_path):
    return str(tmp_path / "test.json")


class TestSaveToCSV:
    def test_creates_new_file(self, sample_df, tmp_csv):
        save_to_csv(sample_df, tmp_csv)
        assert os.path.exists(tmp_csv)
        loaded = pd.read_csv(tmp_csv)
        assert len(loaded) == 2

    def test_appends_to_existing(self, sample_df, tmp_csv):
        save_to_csv(sample_df, tmp_csv)
        save_to_csv(sample_df, tmp_csv)
        loaded = pd.read_csv(tmp_csv)
        assert len(loaded) == 4

    def test_empty_df_noop(self, tmp_csv):
        save_to_csv(pd.DataFrame(), tmp_csv)
        assert not os.path.exists(tmp_csv)

    def test_dedup_flag(self, sample_df, tmp_csv):
        save_to_csv(sample_df, tmp_csv)
        save_to_csv(sample_df, tmp_csv, dedup=True)
        loaded = pd.read_csv(tmp_csv)
        assert len(loaded) == 2


class TestSaveToJSON:
    def test_creates_valid_json(self, sample_df, tmp_json):
        save_to_json(sample_df, tmp_json)
        with open(tmp_json, encoding="utf-8") as f:
            data = json.load(f)
        assert "earthquakes" in data
        assert len(data["earthquakes"]) == 2

    def test_preserves_unicode(self, tmp_json):
        df = pd.DataFrame({
            "location": ["Bago Region, Myanmar (Burma)"],
            "mag": [3.5],
        })
        save_to_json(df, tmp_json)
        with open(tmp_json, encoding="utf-8") as f:
            content = f.read()
        assert "Burma" in content

    def test_empty_df_noop(self, tmp_json):
        save_to_json(pd.DataFrame(), tmp_json)
        assert not os.path.exists(tmp_json)


class TestDeduplicateCSV:
    def test_removes_exact_duplicates(self, sample_df, tmp_csv):
        save_to_csv(sample_df, tmp_csv)
        save_to_csv(sample_df, tmp_csv)
        deduplicate_csv(tmp_csv)
        loaded = pd.read_csv(tmp_csv)
        assert len(loaded) == 2

    def test_nonexistent_file(self):
        deduplicate_csv("/nonexistent/path.csv")


class TestLoadCombinedJSON:
    def test_loads_existing(self, tmp_json):
        data = {"earthquakes": [{"mag": 3.5}]}
        with open(tmp_json, "w") as f:
            json.dump(data, f)
        result = load_combined_json(tmp_json)
        assert len(result) == 1

    def test_returns_empty_on_missing(self):
        result = load_combined_json("/nonexistent/path.json")
        assert result == []

    def test_handles_corrupt_file(self, tmp_path):
        bad_file = str(tmp_path / "bad.json")
        with open(bad_file, "w") as f:
            f.write("not valid json{{{")
        result = load_combined_json(bad_file)
        assert result == []


class TestMergeCombinedJSON:
    def test_merges_without_duplicates(self):
        existing = [
            {"time_utc": "2025-01-01 00:00:00", "latitude": "21.0", "longitude": "96.0"},
        ]
        new = [
            {"time_utc": "2025-01-01 00:00:00", "latitude": "21.0", "longitude": "96.0"},
            {"time_utc": "2025-01-02 00:00:00", "latitude": "22.0", "longitude": "97.0"},
        ]
        result = merge_combined_json(existing, new)
        assert len(result) == 2

    def test_empty_inputs(self):
        assert merge_combined_json([], []) == []

    def test_preserves_order(self):
        existing = [
            {"time_utc": "2025-01-01", "latitude": "21.0", "longitude": "96.0"},
        ]
        new = [
            {"time_utc": "2025-01-03", "latitude": "23.0", "longitude": "98.0"},
            {"time_utc": "2025-01-02", "latitude": "22.0", "longitude": "97.0"},
        ]
        result = merge_combined_json(existing, new)
        assert result[0]["time_utc"] == "2025-01-01"
        assert result[1]["time_utc"] == "2025-01-03"
        assert result[2]["time_utc"] == "2025-01-02"


class TestGenerateDateRanges:
    def test_returns_list_of_tuples(self):
        ranges = generate_date_ranges(
            end_date=datetime(2025, 2, 15, tzinfo=timezone.utc).date(),
        )
        if ranges:
            for item in ranges:
                assert len(item) == 4
                assert isinstance(item[0], int)
                assert isinstance(item[1], int)
                assert isinstance(item[2], str)
                assert isinstance(item[3], str)

    def test_date_format(self):
        ranges = generate_date_ranges(
            end_date=datetime(2025, 3, 15, tzinfo=timezone.utc).date(),
        )
        if ranges:
            from_str = ranges[0][2]
            to_str = ranges[0][3]
            datetime.strptime(from_str, "%Y-%m-%d")
            datetime.strptime(to_str, "%Y-%m-%d")
