"""Stored-XSS regression: data-derived strings (dam CSV fields, OSM names,
API strings) must be HTML-escaped before popup interpolation — folium renders
string popups verbatim inside a jQuery template literal on the published map
(2026-07-06 audit, finding C7)."""
import json

import pandas as pd

from mmeq.visualization.map import _esc, build_earthquake_map

PAYLOAD = "<img src=x onerror=alert(1)>"
TPL_PAYLOAD = "${alert(1)}"


def test_esc_neutralizes_html_and_backticks():
    out = _esc(f"Evil `{PAYLOAD}` dam")
    assert "<" not in out and ">" not in out and "`" not in out
    assert "&lt;img" in out and "&#96;" in out


def test_esc_neutralizes_template_interpolation():
    # popups land inside a backtick template literal, where ${expr} is
    # EVALUATED — escaping tags and backticks alone is not enough.
    out = _esc(f"Dam {TPL_PAYLOAD} river")
    assert "${" not in out
    assert "&#36;" in out


def test_map_html_contains_no_unescaped_payload(tmp_path):
    dams = {
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [96.0, 21.5]},
            "properties": {
                "name": PAYLOAD,
                "status": f"Complete{PAYLOAD}",
                "function": "Irrigation",
                "river": f"`backtick`{PAYLOAD}{TPL_PAYLOAD}",
            },
        }],
    }
    dams_path = tmp_path / "dams.geojson"
    dams_path.write_text(json.dumps(dams), encoding="utf-8")

    df = pd.DataFrame({
        "time_utc": ["2025-03-28 06:20:52"],
        "time_mmt": [f"2025-03-28 12:50:52{PAYLOAD}"],
        "latitude": [21.996],
        "longitude": [95.926],
        "depth": [10.0],
        "mag": [7.7],
    })
    out = str(tmp_path / "map.html")
    build_earthquake_map(
        df, output_path=out, dams_path=str(dams_path),
        show_heatmap=False, cluster_markers=False,
    )
    html_text = open(out, encoding="utf-8").read()
    assert PAYLOAD not in html_text, "raw payload reached the published HTML"
    assert TPL_PAYLOAD not in html_text, "raw ${...} interpolation reached the published HTML"
    assert "&lt;img src=x" in html_text, "escaped payload should be present"
