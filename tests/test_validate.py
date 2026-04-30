import pandas as pd
import numpy as np
import pytest
from datetime import datetime, timezone

from mmeq.export.writer import validate_quake_data


@pytest.fixture
def sample_raw_df():
    return pd.DataFrame({
        "time": [
            "2025-03-28T06:20:52.000Z",
            "2025-03-28T06:32:04.000Z",
            "invalid_time",
        ],
        "latitude": [21.996, 21.707, 20.0],
        "longitude": [95.926, 95.969, 96.0],
        "depth": [10.0, 10.0, 5.0],
        "mag": [7.7, 6.7, 3.5],
    })


@pytest.fixture
def sample_raw_with_oob():
    return pd.DataFrame({
        "time": [
            "2025-01-01T00:00:00.000Z",
            "2025-01-01T01:00:00.000Z",
            "2025-01-01T02:00:00.000Z",
            "2025-01-01T03:00:00.000Z",
            "2025-01-01T04:00:00.000Z",
        ],
        "latitude": [21.0, -91.0, 21.0, 21.0, np.nan],
        "longitude": [96.0, 96.0, 181.0, 96.0, 96.0],
        "depth": [10.0, 10.0, 10.0, 800.0, 10.0],
        "mag": [3.5, 3.5, 3.5, 3.5, 3.5],
    })


class TestValidateQuakeData:
    def test_basic_validation(self, sample_raw_df):
        result = validate_quake_data(sample_raw_df)
        assert len(result) == 2
        assert "time_utc" in result.columns
        assert "time_mmt" in result.columns
        assert "time" not in result.columns

    def test_invalid_time_dropped(self, sample_raw_df):
        result = validate_quake_data(sample_raw_df)
        times = result["time_utc"].tolist()
        assert "2025-03-28 06:20:52" in times[0]

    def test_empty_input(self):
        result = validate_quake_data(pd.DataFrame())
        assert result.empty

    def test_out_of_bounds_latitude(self, sample_raw_with_oob):
        result = validate_quake_data(sample_raw_with_oob)
        lats = result["latitude"].tolist()
        assert all(-90 <= lat <= 90 for lat in lats)

    def test_out_of_bounds_longitude(self, sample_raw_with_oob):
        result = validate_quake_data(sample_raw_with_oob)
        lons = result["longitude"].tolist()
        assert all(-180 <= lon <= 180 for lon in lons)

    def test_out_of_bounds_depth(self, sample_raw_with_oob):
        result = validate_quake_data(sample_raw_with_oob)
        depths = result["depth"].tolist()
        assert all(0 <= d <= 700 for d in depths)

    def test_nan_rows_dropped(self, sample_raw_with_oob):
        result = validate_quake_data(sample_raw_with_oob)
        assert result.notna().all(axis=None)

    def test_mmt_timezone_conversion(self, sample_raw_df):
        result = validate_quake_data(sample_raw_df)
        assert len(result) > 0
        assert "time_mmt" in result.columns
        mmt_val = result.iloc[0]["time_mmt"]
        assert isinstance(mmt_val, str)

    def test_all_nan_mag(self):
        df = pd.DataFrame({
            "time": ["2025-01-01T00:00:00.000Z"],
            "latitude": [21.0],
            "longitude": [96.0],
            "depth": [10.0],
            "mag": [np.nan],
        })
        result = validate_quake_data(df)
        assert result.empty
