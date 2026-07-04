"""Self-hosting guarantee: generated map/dashboard HTML references no CDN origin.

Every JS/CSS/font/image the folium maps and plotly dashboards need must be served
page-relative from the vendored tree copied next to the output — zero third-party
origins on the public site. Runtime map TILE fetches (OpenStreetMap / CartoDB base
layers) are excluded: they are live map-tile requests inside JS ``L.tileLayer``
strings, not static ``<script>`` / ``<link>`` assets.
"""
import re
from pathlib import Path

import pandas as pd
import pytest

from mmeq.visualization.assets import VENDOR_DIR, VENDOR_MANIFEST
from mmeq.visualization.map import build_earthquake_map
from mmeq.visualization.dashboard import build_dashboard

# src="..." on <script> and href="..." on <link ... rel=stylesheet> — the only
# tags that pull static assets. Tile URLs live in JS strings, not these attrs.
_SCRIPT_SRC = re.compile(r"<script[^>]*\bsrc=[\"']([^\"']+)[\"']", re.I)
_LINK_HREF = re.compile(r"<link[^>]*\bhref=[\"']([^\"']+)[\"']", re.I)


def _sample_df():
    return pd.DataFrame({
        "time_utc": pd.date_range("2025-01-01", periods=6, freq="7D"),
        "time_mmt": ["2025-01-01"] * 6,
        "latitude": [21.0, 22.5, 20.1, 23.4, 19.8, 24.0],
        "longitude": [96.0, 95.2, 97.1, 94.8, 96.6, 95.9],
        "depth": [10.0, 25.0, 5.0, 40.0, 15.0, 30.0],
        "mag": [3.1, 4.8, 5.6, 2.9, 6.2, 3.7],
        "location": ["A", "B", "C", "D", "E", "F"],
    })


def _asset_refs(html: str):
    return _SCRIPT_SRC.findall(html) + _LINK_HREF.findall(html)


def _assert_no_remote(refs):
    remote = [r for r in refs if r.startswith(("http://", "https://", "//"))]
    assert not remote, f"remote asset references leaked into HTML: {remote}"


def test_vendor_tree_populated():
    """Sanity: the checked-in vendor tree exists and holds every manifest file."""
    for name, rel in VENDOR_MANIFEST.items():
        assert (VENDOR_DIR / rel).is_file(), f"missing vendored asset {name!r} ({rel})"


def test_map_html_has_no_remote_assets(tmp_path):
    out = tmp_path / "map.html"
    build_earthquake_map(
        _sample_df(),
        output_path=str(out),
        min_mag=0.0,
        show_heatmap=True,
        show_markers=True,
        cluster_markers=True,
        show_dams=False,
    )
    html = out.read_text()
    refs = _asset_refs(html)

    _assert_no_remote(refs)

    # Every page-relative asset reference must resolve to a file next to the HTML.
    local = [r for r in refs if r.startswith("vendor/")]
    assert local, "expected vendored asset references in map HTML"
    for rel in local:
        assert (tmp_path / rel).is_file(), f"referenced vendor file absent: {rel}"

    # The heatmap + cluster plugins were exercised, so their vendored JS must ship.
    assert (tmp_path / "vendor" / VENDOR_MANIFEST["leaflet-heat.js"]).is_file()
    assert (tmp_path / "vendor" / VENDOR_MANIFEST["markerclusterjs"]).is_file()


def test_dashboard_html_has_no_remote_assets(tmp_path):
    pytest.importorskip("plotly")
    out = tmp_path / "dashboard.html"
    build_dashboard(_sample_df(), output_path=str(out))
    html = out.read_text()
    refs = _asset_refs(html)

    _assert_no_remote(refs)

    # include_plotlyjs="directory" writes plotly.min.js beside the HTML.
    plotly_refs = [r for r in refs if "plotly" in r.lower()]
    assert plotly_refs, "expected a local plotly.min.js reference"
    for rel in plotly_refs:
        assert (tmp_path / rel).is_file(), f"local plotly asset absent: {rel}"
    assert (tmp_path / "plotly.min.js").is_file()


def test_animated_map_html_has_no_remote_assets(tmp_path):
    from mmeq.visualization.animated_map import build_animated_map

    out = tmp_path / "animated.html"
    build_animated_map(_sample_df(), output_path=str(out), min_mag=0.0)
    html = out.read_text()
    refs = _asset_refs(html)

    _assert_no_remote(refs)
    local = [r for r in refs if r.startswith("vendor/")]
    assert local, "expected vendored asset references in animated-map HTML"
    for rel in local:
        assert (tmp_path / rel).is_file(), f"referenced vendor file absent: {rel}"
    # The TimestampedGeoJson plugin was exercised — its vendored JS must ship.
    assert (tmp_path / "vendor" / VENDOR_MANIFEST["leaflet.timedimension"]).is_file()


def test_cross_section_html_has_no_remote_assets(tmp_path):
    pytest.importorskip("plotly")
    from mmeq.visualization.cross_section import depth_cross_section

    out = tmp_path / "cross.html"
    depth_cross_section(_sample_df(), output_path=str(out))
    html = out.read_text()
    refs = _asset_refs(html)

    _assert_no_remote(refs)
    plotly_refs = [r for r in refs if "plotly" in r.lower()]
    assert plotly_refs, "expected a local plotly.min.js reference"
    assert (tmp_path / "plotly.min.js").is_file()
