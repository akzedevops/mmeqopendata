import json
import shutil
import subprocess

import pandas as pd
import pytest

from mmeq.visualization.animated_map import _NUMERIC_TIME_SORT, build_animated_map


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js is needed to execute the timestamp regression")
@pytest.mark.parametrize("timestamps", [
    [1000000000000, 999999999999, -315619200000, -631152000000, 0, 0, 1000000000000],
    [],
    [999239415000, 999239415000],
])
def test_numeric_time_sort(timestamps):
    program = "const L = {TimeDimension: {Util: {}}};\n" + _NUMERIC_TIME_SORT
    program += f"\nconst input = {json.dumps(timestamps)};"
    program += "\nconsole.log(JSON.stringify({sorted: L.TimeDimension.Util.sort_and_deduplicate(input), input}));"
    result = subprocess.run(["node", "-e", program], check=True, capture_output=True, text=True)
    actual = json.loads(result.stdout)
    assert actual["sorted"] == sorted(set(timestamps))
    assert actual["input"] == timestamps


def test_animation_installs_time_sort_before_initialization(tmp_path):
    events = pd.DataFrame({
        "time_utc": pd.to_datetime(["1960-01-01", "2001-08-31", "2001-09-10", "2026-11-11"]),
        "latitude": [21.0] * 4,
        "longitude": [96.0] * 4,
        "mag": [4.0] * 4,
        "depth": [10.0] * 4,
    })
    output = tmp_path / "animated.html"
    build_animated_map(events, output_path=str(output), fault_lines_path=str(tmp_path / "absent.json"))
    rendered = output.read_text()
    assert rendered.index(_NUMERIC_TIME_SORT.strip()) < rendered.index(".timeDimension = L.timeDimension(")
    for timestamp in events["time_utc"]:
        assert timestamp.strftime("%Y-%m-%dT%H:%M:%SZ") in rendered
    assert 'class="mmeq-map-panel"' in rendered
    assert 'duration: "P68Y"' in rendered
