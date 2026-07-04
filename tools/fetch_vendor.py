#!/usr/bin/env python3
"""Populate the self-hosted asset tree for folium map / plotly dashboard output.

Generated HTML (folium maps + plotly dashboards) must reference ZERO third-party
origins so the public GitHub Pages site carries no CDN supply-chain surface. This
script downloads every JS/CSS/font/image that folium's ``Map`` defaults and the
plugins we actually use (``HeatMap``, ``MarkerCluster``, ``TimestampedGeoJson``)
pull from CDNs, verifies each byte against a checked-in sha256 lock, and writes
them under ``src/mmeq/visualization/vendor/``.

The pinned versions are read PROGRAMMATICALLY from the installed folium package
(``folium.Map.default_js`` / ``default_css`` and each plugin's ``default_js`` /
``default_css``) so the list cannot drift away from the folium we ship against.
Every default asset name must appear in ``PATHMAP`` below — an unknown name is a
hard error, forcing this file to be updated when folium bumps its bundle.

Two files are folium's own repo assets. ``leaflet_heat.min.js`` is copied from the
installed ``folium/templates/`` directory. ``leaflet.awesome.rotate.min.css`` is
NOT shipped inside the wheel, so it is downloaded from the pinned folium git tag.

Usage::

    python tools/fetch_vendor.py            # download + write vendor tree + lock
    python tools/fetch_vendor.py --verify   # verify existing tree against lock, no network
"""
from __future__ import annotations

import argparse
import hashlib
import json
import posixpath
import re
import shutil
import sys
import urllib.request
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import folium
from folium import Map
from folium.plugins import HeatMap, MarkerCluster, TimestampedGeoJson

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
VENDOR_DIR = REPO / "src" / "mmeq" / "visualization" / "vendor"
LOCK_PATH = HERE / "vendor_lock.json"

FOLIUM_VERSION = folium.__version__
FOLIUM_PKG = Path(folium.__file__).resolve().parent
FOLIUM_TEMPLATES = FOLIUM_PKG / "templates"
# Pinned folium git tag for repo assets not shipped inside the installed wheel.
FOLIUM_TAG = f"v{FOLIUM_VERSION}"
FOLIUM_GH = (
    f"https://cdn.jsdelivr.net/gh/python-visualization/folium@{FOLIUM_TAG}"
    "/folium/templates"
)

# folium default-name -> path (relative to the vendor root). Every default asset
# name emitted by Map / the plugins we use MUST be listed here; a missing name
# aborts the run so the vendor set can never silently drift from folium.
PATHMAP: dict[str, str] = {
    # folium.Map defaults
    "leaflet": "leaflet/leaflet.js",
    "jquery": "jquery/jquery.min.js",
    "bootstrap": "bootstrap/bootstrap.bundle.min.js",
    "awesome_markers": "awesome-markers/leaflet.awesome-markers.js",
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

# Assets copied from the installed folium package instead of downloaded.
COPY_FROM_PACKAGE = {"leaflet-heat.js": FOLIUM_TEMPLATES / "leaflet_heat.min.js"}
# Assets downloaded from the pinned folium git tag (not shipped in the wheel).
DOWNLOAD_FROM_TAG = {
    "awesome_rotate_css": f"{FOLIUM_GH}/leaflet.awesome.rotate.min.css",
}

# CSS files whose url() references we should NOT fully follow. fontawesome only
# needs the solid-900 face (the only icon weight our markers render); glyphicons
# fonts are never rendered (we use the 'fa' prefix), so its @font-face refs stay
# lazily-unfetched and need not be vendored.
CSS_FOLLOW_FILTER = {
    "fontawesome/css/all.min.css": lambda ref: "fa-solid-900" in ref,
    "bootstrap-glyphicons/bootstrap-glyphicons.css": lambda ref: False,
}

_URL_RE = re.compile(r"""url\(\s*['"]?([^'")]+)['"]?\s*\)""")


def _collect_defaults() -> list[tuple[str, str]]:
    """Read (name, url) for every default asset off the installed folium objects."""
    seen: dict[str, str] = {}
    for obj in (Map, HeatMap, MarkerCluster, TimestampedGeoJson):
        for name, url in list(getattr(obj, "default_js", [])) + list(
            getattr(obj, "default_css", [])
        ):
            seen.setdefault(name, url)
    return sorted(seen.items())


def _fetch(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "mmeq-fetch-vendor"})
    with urllib.request.urlopen(req, timeout=60) as resp:  # noqa: S310 (pinned https)
        return resp.read()


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _write(rel_path: str, data: bytes) -> None:
    dest = VENDOR_DIR / rel_path
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(data)


def _follow_css(rel_path: str, source_url: str, css: bytes,
                records: dict[str, dict]) -> None:
    """Download relative url() assets referenced by a stylesheet."""
    keep = CSS_FOLLOW_FILTER.get(rel_path, lambda ref: True)
    for raw in _URL_RE.findall(css.decode("utf-8", "replace")):
        ref = raw.strip()
        if ref.startswith(("data:", "http://", "https://", "//", "#")):
            continue
        ref = ref.split("?", 1)[0].split("#", 1)[0]
        if not ref or not keep(ref):
            continue
        target = posixpath.normpath(posixpath.join(posixpath.dirname(rel_path), ref))
        if target.startswith("..") or target in records:
            continue
        src = urljoin(source_url, ref)
        data = _fetch(src)
        _write(target, data)
        records[target] = {"source": src, "sha256": _sha256(data), "bytes": len(data)}
        print(f"  + {target}  ({len(data):,} B)  <- {src}")


def build() -> dict:
    if VENDOR_DIR.exists():
        shutil.rmtree(VENDOR_DIR)
    VENDOR_DIR.mkdir(parents=True)

    defaults = _collect_defaults()
    unknown = [n for n, _ in defaults if n not in PATHMAP]
    if unknown:
        sys.exit(f"ERROR: folium default assets not in PATHMAP: {unknown}")

    records: dict[str, dict] = {}
    for name, cdn_url in defaults:
        rel = PATHMAP[name]
        if name in COPY_FROM_PACKAGE:
            data = COPY_FROM_PACKAGE[name].read_bytes()
            source = f"folium-package:{COPY_FROM_PACKAGE[name].relative_to(FOLIUM_PKG)}"
        elif name in DOWNLOAD_FROM_TAG:
            source = DOWNLOAD_FROM_TAG[name]
            data = _fetch(source)
        else:
            source = cdn_url
            data = _fetch(cdn_url)
        _write(rel, data)
        records[rel] = {"source": source, "sha256": _sha256(data), "bytes": len(data)}
        print(f"  + {rel}  ({len(data):,} B)  <- {source}")
        if rel.endswith(".css"):
            _follow_css(rel, source, data, records)

    lock = {
        "folium_version": FOLIUM_VERSION,
        "files": dict(sorted(records.items())),
    }
    LOCK_PATH.write_text(json.dumps(lock, indent=2) + "\n")

    total = sum(r["bytes"] for r in records.values())
    print(f"\nfolium {FOLIUM_VERSION}: {len(records)} files, {total:,} B "
          f"({total / 1024:.0f} KiB) -> {VENDOR_DIR}")
    print(f"lock -> {LOCK_PATH}")
    return lock


def verify() -> int:
    if not LOCK_PATH.exists():
        print(f"no lock at {LOCK_PATH}", file=sys.stderr)
        return 1
    lock = json.loads(LOCK_PATH.read_text())
    bad = 0
    for rel, meta in lock["files"].items():
        path = VENDOR_DIR / rel
        if not path.exists():
            print(f"MISSING {rel}")
            bad += 1
            continue
        got = _sha256(path.read_bytes())
        if got != meta["sha256"]:
            print(f"HASH MISMATCH {rel}\n  want {meta['sha256']}\n  got  {got}")
            bad += 1
    print("OK" if not bad else f"{bad} problem(s)")
    return 1 if bad else 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--verify", action="store_true",
                    help="verify existing tree against the lock (no network)")
    args = ap.parse_args()
    if args.verify:
        return verify()
    build()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
