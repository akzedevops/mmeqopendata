import json
import shutil
import subprocess

import pandas as pd
import pytest

from mmeq.visualization.animated_map import build_animated_map
from mmeq.visualization.map import build_earthquake_map
from mmeq.visualization.map_controls import _YEAR_FILTER_FUNCTION, MapYearFilter, prepare_map_events


def _events():
    return pd.DataFrame({
        "time_utc": ["2023-12-31T23:30:00-01:00", "2025-01-01T00:30:00+01:00", "2026-06-01", "invalid"],
        "mag": [3.0, 4.0, 5.0, 6.0],
        "depth": [10.0] * 4,
        "latitude": [21.0] * 4,
        "longitude": [96.0] * 4,
    })


def test_years_use_utc_and_exclude_invalid_dates_without_mutating_input():
    source = _events()
    original = source.copy(deep=True)
    selected = prepare_map_events(source, 0)
    assert selected["time_utc"].dt.year.tolist() == [2024, 2024, 2026]
    pd.testing.assert_frame_equal(source, original)
    assert len(prepare_map_events(source, 4.5)) == 1


@pytest.mark.parametrize("builder", [build_earthquake_map, build_animated_map])
@pytest.mark.parametrize("case", ["empty", "invalid", "magnitude"])
def test_empty_map_input_has_clear_error(tmp_path, builder, case):
    frame = _events()
    if case == "empty":
        frame = frame.iloc[:0]
    elif case == "invalid":
        frame = frame.iloc[-1:]
    with pytest.raises(ValueError, match="No earthquakes with valid timestamps"):
        builder(frame, output_path=str(tmp_path / "map.html"), min_mag=9 if case == "magnitude" else 0)


def test_year_options_include_gaps_and_single_year():
    frame = prepare_map_events(_events(), 0)
    assert MapYearFilter(frame).years == [2024, 2025, 2026]
    assert MapYearFilter(frame.iloc[:1]).years == [2024]


@pytest.mark.parametrize("markers,heatmap,cluster", [(True, True, True), (True, False, False), (False, True, False), (False, False, False)])
def test_static_filter_handles_optional_layers(tmp_path, monkeypatch, markers, heatmap, cluster):
    monkeypatch.setattr("mmeq.analysis.osm_exposure.load_osm_infrastructure", lambda: pd.DataFrame())
    output = tmp_path / "map.html"
    build_earthquake_map(_events(), output_path=str(output), show_dams=False,
                         show_markers=markers, show_heatmap=heatmap, cluster_markers=cluster)
    rendered = output.read_text()
    assert 'aria-label="Earthquake year filters"' in rendered
    assert 'data-action="reset"' in rendered
    assert 'value="2025"' in rendered
    assert ("const markerTarget =" in rendered) == markers
    assert (".setLatLngs(selected.map" in rendered) == heatmap
    if markers:
        assert '"eventYear": 2024' in rendered
    assert "2023-12-31T23:30:00-01:00" not in rendered


def test_animated_filter_pauses_rebuilds_and_keeps_numeric_time_fix(tmp_path):
    output = tmp_path / "animated.html"
    build_animated_map(_events(), output_path=str(output))
    rendered = output.read_text()
    assert "timeDimensionControl._player.stop()" in rendered
    assert "map.removeLayer(animation)" in rendered
    assert "map.timeDimension.setAvailableTimes([], 'replace')" in rendered
    assert "feature.properties.time).getUTCFullYear()" in rendered
    assert "L.TimeDimension.Util.sort_and_deduplicate" in rendered


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js executes the browser year predicate")
def test_year_filter_is_inclusive_and_handles_gap_years():
    program = _YEAR_FILTER_FUNCTION + """
    const years = [1969, 1970, 2001, 2024, 2024, 2026];
    console.log(JSON.stringify([
        years.filter(year => inYearRange(year, 2024, 2024)),
        years.filter(year => inYearRange(year, 1970, 2024)),
        years.filter(year => inYearRange(year, 2025, 2025))
    ]));
    """
    result = subprocess.run(["node", "-e", program], check=True, capture_output=True, text=True)
    assert json.loads(result.stdout) == [[2024, 2024], [1970, 2001, 2024, 2024], []]
