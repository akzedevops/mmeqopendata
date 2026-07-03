"""Pin folium's moving-ref CDN assets to the installed folium version.

Upstream folium (issues #664, #1307) still ships template asset URLs that point
at a MOVING ref: ``HeatMap``'s ``leaflet_heat.min.js`` is pinned to ``@main`` and
every ``folium.Map`` emits a ref-less ``leaflet.awesome.rotate.min.css`` from the
default branch. Either can change under us without warning. We override both via
folium's supported ``JSCSSMixin`` API (``add_js_link`` / ``add_css_link``), which
replaces the link registered under the same name.

The pin lives here in ONE place. It is pinned to the *installed* folium version
(currently 0.20.0) — bump ``FOLIUM_PIN`` together with the folium dependency.
"""

# == installed folium version; bump together with the folium dependency.
FOLIUM_PIN = "v0.20.0"
_TEMPLATES = (
    f"https://cdn.jsdelivr.net/gh/python-visualization/folium@{FOLIUM_PIN}"
    "/folium/templates"
)


def pin_heatmap(hm):
    """Pin a ``HeatMap`` plugin's ``leaflet-heat.js`` to ``FOLIUM_PIN``."""
    hm.add_js_link("leaflet-heat.js", f"{_TEMPLATES}/leaflet_heat.min.js")
    return hm


def pin_map(m):
    """Pin a ``folium.Map``'s awesome-rotate CSS to ``FOLIUM_PIN``."""
    m.add_css_link(
        "awesome_rotate_css", f"{_TEMPLATES}/leaflet.awesome.rotate.min.css"
    )
    return m
