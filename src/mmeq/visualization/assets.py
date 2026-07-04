"""Self-hosted (vendored) assets for folium map output.

Generated folium HTML must reference ZERO third-party origins — no CDN scripts,
stylesheets, fonts, or images — so the public GitHub Pages site carries no
supply-chain surface (SRI-less CDN scripts, folium's moving ``@main`` refs). The
static asset tree lives beside this module under ``vendor/`` (populated by
``tools/fetch_vendor.py``) and is copied next to each saved HTML at render time.

``use_local_assets`` rewrites every default JS/CSS link on the objects passed to
it to a page-relative ``vendor/...`` URL, using folium's supported ``JSCSSMixin``
API (``add_js_link`` / ``add_css_link``), which replaces the entry registered
under the same name. ``copy_vendor`` drops the tree next to the output file.

This supersedes the earlier one-off pins in ``_folium_pins.py``: the awesome-rotate
CSS and the HeatMap ``leaflet-heat.js`` are covered here alongside every other
default asset.
"""
from __future__ import annotations

import logging
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)

# Directory that ships with the package (see pyproject package-data).
VENDOR_DIR = Path(__file__).resolve().parent / "vendor"
# Page-relative directory name the tree is copied into next to each HTML file.
VENDOR_URL_PREFIX = "vendor"

# folium default-asset name -> path relative to the vendor root. Must cover every
# default emitted by folium.Map and the plugins we use (HeatMap, MarkerCluster,
# TimestampedGeoJson); kept in lockstep with tools/fetch_vendor.py's PATHMAP.
VENDOR_MANIFEST: dict[str, str] = {
    # folium.Map JS
    "leaflet": "leaflet/leaflet.js",
    "jquery": "jquery/jquery.min.js",
    "bootstrap": "bootstrap/bootstrap.bundle.min.js",
    "awesome_markers": "awesome-markers/leaflet.awesome-markers.js",
    # folium.Map CSS
    "leaflet_css": "leaflet/leaflet.css",
    "bootstrap_css": "bootstrap/bootstrap.min.css",
    "glyphicons_css": "bootstrap-glyphicons/bootstrap-glyphicons.css",
    "awesome_markers_font_css": "fontawesome/css/all.min.css",
    "awesome_markers_css": "awesome-markers/leaflet.awesome-markers.css",
    "awesome_rotate_css": "folium/leaflet.awesome.rotate.min.css",
    # HeatMap
    "leaflet-heat.js": "folium/leaflet_heat.min.js",
    # MarkerCluster
    "markerclusterjs": "markercluster/leaflet.markercluster.js",
    "markerclustercss": "markercluster/MarkerCluster.css",
    "markerclusterdefaultcss": "markercluster/MarkerCluster.Default.css",
    # TimestampedGeoJson
    "jquery3.7.1": "timedimension/jquery.min.js",
    "jqueryui1.10.2": "timedimension/jquery-ui.min.js",
    "iso8601": "timedimension/iso8601.min.js",
    "leaflet.timedimension": "timedimension/leaflet.timedimension.min.js",
    "moment": "timedimension/moment.min.js",
    "highlight.js_css": "timedimension/highlight-default.min.css",
    "leaflet.timedimension_css": "timedimension/leaflet.timedimension.control.css",
}


def _local_url(name: str) -> str:
    return f"{VENDOR_URL_PREFIX}/{VENDOR_MANIFEST[name]}"


def use_local_assets(*objects) -> None:
    """Rewrite every default JS/CSS link on ``objects`` to a local vendor URL.

    Pass any folium ``JSCSSMixin`` instances — the ``folium.Map`` plus each plugin
    that carries its own defaults (``HeatMap``, ``MarkerCluster``,
    ``TimestampedGeoJson``). Every default whose name is in ``VENDOR_MANIFEST`` is
    replaced by name via ``add_js_link`` / ``add_css_link``; any remaining default
    that still points at an ``http(s)`` origin is a hard error, so a new folium
    default can never silently ship a CDN reference.

    Note: folium's ``_add_link`` mutates the CLASS-level default list, so the
    override applies uniformly to every instance of that class in the process.
    """
    for obj in objects:
        for name, url in list(getattr(obj, "default_js", [])):
            if name in VENDOR_MANIFEST:
                obj.add_js_link(name, _local_url(name))
            elif str(url).startswith(("http://", "https://", "//")):
                raise ValueError(
                    f"un-vendored remote JS default {name!r} -> {url!r}; "
                    "add it to VENDOR_MANIFEST and tools/fetch_vendor.py"
                )
        for name, url in list(getattr(obj, "default_css", [])):
            if name in VENDOR_MANIFEST:
                obj.add_css_link(name, _local_url(name))
            elif str(url).startswith(("http://", "https://", "//")):
                raise ValueError(
                    f"un-vendored remote CSS default {name!r} -> {url!r}; "
                    "add it to VENDOR_MANIFEST and tools/fetch_vendor.py"
                )


def copy_vendor(output_dir) -> Path:
    """Copy the vendor tree to ``<output_dir>/vendor`` (idempotent)."""
    if not VENDOR_DIR.is_dir():
        raise FileNotFoundError(
            f"vendor tree missing at {VENDOR_DIR}; run tools/fetch_vendor.py"
        )
    dest = Path(output_dir) / VENDOR_URL_PREFIX
    shutil.copytree(VENDOR_DIR, dest, dirs_exist_ok=True)
    logger.debug("copied vendor assets -> %s", dest)
    return dest
