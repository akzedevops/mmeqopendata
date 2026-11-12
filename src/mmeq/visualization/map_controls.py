"""Shared year filters and presentation for the earthquake maps."""

import pandas as pd
from branca.element import MacroElement, Template

from mmeq.config import MAG_COLORS, MAG_THRESHOLDS


_YEAR_FILTER_FUNCTION = """
function inYearRange(year, fromYear, toYear) {
    return year >= fromYear && year <= toYear;
}
"""


def prepare_map_events(frame: pd.DataFrame, min_mag: float) -> pd.DataFrame:
    """Select timestamped events and normalize year boundaries to UTC."""
    events = frame[frame["mag"] >= min_mag].copy()
    events["time_utc"] = pd.to_datetime(events["time_utc"], utc=True, errors="coerce", format="mixed")
    events = events.dropna(subset=["time_utc"])
    if events.empty:
        raise ValueError("No earthquakes with valid timestamps match the magnitude filter")
    return events


class MapYearFilter(MacroElement):
    """Filter earthquake layers without affecting reference overlays."""

    _template = Template("""
    {% macro header(this, kwargs) %}
    <style>
        .mmeq-map-panel {
            position: absolute; top: 16px; left: 16px; z-index: 1000;
            width: min(330px, calc(100vw - 90px)); padding: 18px;
            box-sizing: border-box; border: 1px solid #dbe5e8; border-radius: 16px;
            background: rgba(255,255,255,.97); color: #142e3b;
            box-shadow: 0 8px 32px rgba(20,46,59,.15);
            font: 14px/1.45 system-ui, sans-serif;
        }
        .mmeq-map-panel * { box-sizing: border-box; }
        .mmeq-eyebrow { color: #486773; font-size: 10px; font-weight: 700; letter-spacing: .13em; }
        .mmeq-map-panel h1 { margin: 5px 0 3px; font-size: 22px; font-weight: 750; letter-spacing: -.03em; }
        .mmeq-count { margin: 0 0 14px; color: #087a74; font-weight: 650; }
        .mmeq-year-fields { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
        .mmeq-map-panel label { margin: 0; color: #486773; font-size: 12px; font-weight: 600; }
        .mmeq-map-panel select {
            display: block; width: 100%; min-height: 42px; margin-top: 5px; padding: 8px;
            color: #142e3b; background: #f5f9fa; border: 1px solid #c9d8df; border-radius: 8px;
            font: inherit; font-size: 14px;
        }
        .mmeq-actions { display: flex; gap: 8px; margin: 12px 0; }
        .mmeq-actions button {
            flex: 1; min-height: 40px; padding: 8px; border: 1px solid #c9d8df;
            border-radius: 8px; background: white; color: #234d5e; font: inherit;
            font-size: 12px; font-weight: 650; cursor: pointer;
        }
        .mmeq-actions button:first-child { background: #0a766f; border-color: #0a766f; color: white; }
        .mmeq-map-panel :focus-visible { outline: 3px solid #2196d2; outline-offset: 2px; }
        .mmeq-legend { display: flex; gap: 12px; padding-top: 12px; border-top: 1px solid #e3ebef; font-size: 11px; }
        .mmeq-legend i { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 4px; }
        .mmeq-help, .mmeq-playback { margin: 8px 0 0; color: #486773; font-size: 11px; }
        .mmeq-playback { font-variant-numeric: tabular-nums; }
        .leaflet-bar-timecontrol {
            max-width: calc(100vw - 100px); border-radius: 10px;
            box-shadow: 0 4px 20px rgba(20,46,59,.15);
        }
        .leaflet-bar-timecontrol a.timecontrol-date { min-width: 96px; }
        .leaflet-control-layers-toggle {
            background-image: none !important; width: 46px !important; height: 44px !important;
            text-align: center; line-height: 44px; color: #234d5e;
        }
        .leaflet-control-layers-toggle::after {
            content: 'Layers'; font: 600 11px/44px system-ui, sans-serif;
        }
        @media (max-width: 600px) {
            .mmeq-map-panel { top: 10px; left: 10px; width: calc(100vw - 76px); padding: 12px; }
            .mmeq-map-panel h1 { font-size: 19px; }
            .mmeq-count { margin-bottom: 8px; }
            .mmeq-legend { padding-top: 8px; }
        }
    </style>
    {% endmacro %}
    {% macro html(this, kwargs) %}
    <section id="{{this.get_name()}}" class="mmeq-map-panel" aria-label="Earthquake year filters">
        <div class="mmeq-eyebrow">MYANMAR · OPEN EARTHQUAKE DATA</div>
        <h1>{{this.title}}</h1>
        <p class="mmeq-count" role="status" aria-live="polite"></p>
        <div class="mmeq-year-fields">
            <label for="{{this.get_name()}}-from">From year (UTC)
                <select id="{{this.get_name()}}-from" data-filter="from">
                {% for year in this.years %}<option value="{{year}}">{{year}}</option>{% endfor %}
                </select>
            </label>
            <label for="{{this.get_name()}}-to">To year (UTC)
                <select id="{{this.get_name()}}-to" data-filter="to">
                {% for year in this.years %}<option value="{{year}}" {% if loop.last %}selected{% endif %}>{{year}}</option>{% endfor %}
                </select>
            </label>
        </div>
        <div class="mmeq-actions">
            <button type="button" data-action="latest">Latest year</button>
            <button type="button" data-action="reset">All years</button>
        </div>
        <div class="mmeq-legend" aria-label="Magnitude legend">
            <span><i style="background:{{this.colors.low}}"></i>M &lt; {{this.thresholds.low}}</span>
            <span><i style="background:{{this.colors.medium}}"></i>M {{this.thresholds.low}}–&lt;{{this.thresholds.medium}}</span>
            <span><i style="background:{{this.colors.high}}"></i>M ≥ {{this.thresholds.medium}}</span>
        </div>
        <p class="mmeq-help">{% if this.animation %}Play or drag the timeline. Earlier selected events remain visible.{% else %}Filters earthquake markers and heatmap, not reference overlays.{% endif %}</p>
        {% if this.animation %}<p class="mmeq-playback"></p>{% endif %}
    </section>
    {% endmacro %}
    {% macro script(this, kwargs) %}
    (function () {
        {{this.filter_function}}
        const map = {{this._parent.get_name()}};
        const panel = document.getElementById({{this.get_name()|tojson}});
        const fromSelect = panel.querySelector('[data-filter="from"]');
        const toSelect = panel.querySelector('[data-filter="to"]');
        const count = panel.querySelector('.mmeq-count');
        const years = {{this.years|tojson}};
        const format = new Intl.NumberFormat('en');
        L.DomEvent.disableClickPropagation(panel);
        L.DomEvent.disableScrollPropagation(panel);
        map.zoomControl.setPosition('bottomright');
        {% if this.animation %}
        let animation = {{this.animation.get_name()}};
        const allFeatures = animation._baseLayer.toGeoJSON().features;
        const baseOptions = animation._baseLayer.options;
        const animationOptions = animation.options;
        const control = timeDimensionControl.getContainer();
        const playback = panel.querySelector('.mmeq-playback');
        function updatePlayback() {
            const visible = animation._currentLayer ? animation._currentLayer.getLayers().length : 0;
            playback.textContent = format.format(visible) + ' events visible at this frame';
        }
        map.timeDimension.on('timeload', function () { requestAnimationFrame(updatePlayback); });
        {% else %}
        const rows = {{this.rows|tojson}};
        {% if this.marker_target %}
        const markerTarget = {{this.marker_target.get_name()}};
        const allMarkers = markerTarget.getLayers().slice();
        {% endif %}
        {% endif %}
        function applyFilter() {
            const fromYear = Number(fromSelect.value);
            const toYear = Number(toSelect.value);
            {% if this.animation %}
            timeDimensionControl._player.stop();
            const selected = allFeatures.filter(function (feature) {
                return inYearRange(new Date(feature.properties.time).getUTCFullYear(), fromYear, toYear);
            });
            map.removeLayer(animation);
            map.timeDimension.setAvailableTimes([], 'replace');
            const base = L.geoJson({type: 'FeatureCollection', features: selected}, baseOptions);
            animation = L.timeDimension.layer.geoJson(base, animationOptions).addTo(map);
            const times = map.timeDimension.getAvailableTimes();
            if (times.length) map.timeDimension.setCurrentTime(times[0]);
            control.style.display = selected.length ? '' : 'none';
            updatePlayback();
            {% else %}
            const selected = rows.filter(function (row) { return inYearRange(row[0], fromYear, toYear); });
            {% if this.marker_target %}
            const markers = allMarkers.filter(function (marker) {
                return inYearRange(marker.options.eventYear, fromYear, toYear);
            });
            markerTarget.clearLayers();
            if (markerTarget.addLayers) markerTarget.addLayers(markers);
            else markers.forEach(function (marker) { markerTarget.addLayer(marker); });
            {% endif %}
            {% if this.heat_layer %}
            {{this.heat_layer.get_name()}}.setLatLngs(selected.map(function (row) { return [row[1], row[2]]; }));
            {% endif %}
            {% endif %}
            count.textContent = selected.length ? format.format(selected.length) + ' earthquakes selected' : 'No earthquakes in these years';
        }
        fromSelect.addEventListener('change', function () {
            if (Number(fromSelect.value) > Number(toSelect.value)) toSelect.value = fromSelect.value;
            applyFilter();
        });
        toSelect.addEventListener('change', function () {
            if (Number(toSelect.value) < Number(fromSelect.value)) fromSelect.value = toSelect.value;
            applyFilter();
        });
        panel.querySelector('[data-action="latest"]').addEventListener('click', function () {
            fromSelect.value = toSelect.value = String(years[years.length - 1]);
            applyFilter();
        });
        panel.querySelector('[data-action="reset"]').addEventListener('click', function () {
            fromSelect.value = String(years[0]);
            toSelect.value = String(years[years.length - 1]);
            applyFilter();
        });
        {% if this.animation %}
        count.textContent = format.format(allFeatures.length) + ' earthquakes selected';
        updatePlayback();
        {% else %}
        count.textContent = format.format(rows.length) + ' earthquakes selected';
        {% endif %}
    })();
    {% endmacro %}
    """)

    def __init__(self, events, marker_target=None, heat_layer=None, animation=None):
        super().__init__()
        self._name = "MapYearFilter"
        event_years = events["time_utc"].dt.year
        self.years = list(range(int(event_years.min()), int(event_years.max()) + 1))
        self.rows = [] if animation is not None else [
            [int(year), float(latitude), float(longitude)]
            for year, latitude, longitude in zip(event_years, events["latitude"], events["longitude"])
        ]
        self.marker_target = marker_target
        self.heat_layer = heat_layer
        self.animation = animation
        self.title = "Earthquake timeline" if animation is not None else "Earthquake explorer"
        self.colors = MAG_COLORS
        self.thresholds = MAG_THRESHOLDS
        self.filter_function = _YEAR_FILTER_FUNCTION
