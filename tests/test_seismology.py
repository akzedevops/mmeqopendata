import pandas as pd
import numpy as np
import pytest

from mmeq.analysis.seismology import b_value, magnitude_of_completeness, decluster_catalog
from mmeq.analysis.clustering import run_dbscan


@pytest.fixture
def mag_series():
    np.random.seed(42)
    return pd.Series(np.random.uniform(3.0, 7.0, 500))


@pytest.fixture
def catalog_df():
    np.random.seed(42)
    n = 50
    return pd.DataFrame({
        "time_utc": pd.date_range("2025-01-01", periods=n, freq="6h"),
        "latitude": np.random.uniform(18.0, 25.0, n),
        "longitude": np.random.uniform(94.0, 100.0, n),
        "depth": np.random.uniform(5.0, 100.0, n),
        "mag": np.random.uniform(3.0, 6.0, n),
    })


class TestBValue:
    def test_returns_tuple(self, mag_series):
        result = b_value(mag_series, min_mag=3.0)
        assert isinstance(result, tuple)
        assert len(result) == 3

    def test_b_value_reasonable_range(self, mag_series):
        b, a, mc = b_value(mag_series, min_mag=3.0)
        assert 0.1 <= b <= 3.0, f"b-value {b} outside expected range"

    def test_auto_mc(self, mag_series):
        b, a, mc = b_value(mag_series)
        assert mc > 0

    def test_too_few_events(self):
        s = pd.Series([4.0, 4.5])
        b, a, mc = b_value(s, min_mag=3.0)
        assert b == 0.0


class TestMagnitudeOfCompleteness:
    def test_returns_float(self, mag_series):
        mc = magnitude_of_completeness(mag_series)
        assert isinstance(mc, float)

    def test_empty_series(self):
        mc = magnitude_of_completeness(pd.Series(dtype=float))
        assert mc == 0.0

    def test_reasonable_value(self, mag_series):
        mc = magnitude_of_completeness(mag_series)
        assert 2.0 <= mc <= 5.0


class TestDecluster:
    def test_returns_dataframe(self, catalog_df):
        result = decluster_catalog(catalog_df)
        assert isinstance(result, pd.DataFrame)
        assert len(result) <= len(catalog_df)

    def test_retains_larger_events(self):
        df = pd.DataFrame({
            "time_utc": pd.to_datetime(["2025-01-01 00:00:00", "2025-01-01 01:00:00"]),
            "latitude": [21.0, 21.01],
            "longitude": [96.0, 96.01],
            "depth": [10.0, 10.0],
            "mag": [6.0, 3.0],
        })
        result = decluster_catalog(df)
        assert 6.0 in result["mag"].values

    def test_no_self_removal(self, catalog_df):
        result = decluster_catalog(catalog_df)
        assert len(result) >= 1


class TestDBSCAN:
    def test_returns_series(self, catalog_df):
        labels = run_dbscan(catalog_df)
        assert isinstance(labels, pd.Series)
        assert len(labels) == len(catalog_df)

    def test_missing_columns(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        with pytest.raises(ValueError, match="latitude"):
            run_dbscan(df)

    def test_cluster_labels(self, catalog_df):
        labels = run_dbscan(catalog_df)
        assert labels.name == "cluster"
        unique = labels.unique()
        assert -1 in unique or len(unique) > 0
