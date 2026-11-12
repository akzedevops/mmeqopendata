import json
import math

import pandas as pd
import pytest

from mmeq.analysis.shakemap_validation import _horizontal_pga, load_shakemap_stations, validate_against_shakemap
from mmeq.analysis import osm_exposure


def channel(name, value, flag="0", units="%g"):
    return {"name": name, "amplitudes": [{"name": "pga", "value": value, "flag": flag, "units": units}]}


def test_horizontal_pair_excludes_vertical_and_uses_geometric_mean():
    assert _horizontal_pga([channel("HNZ", 99), channel("HN1", 4), channel("HN2", 9)]) == pytest.approx(0.06)
    assert _horizontal_pga([channel("h1", 4), channel("h2", 9)]) == pytest.approx(0.06)
    assert _horizontal_pga([channel("HNN", 4), channel("HNE", 9)]) == pytest.approx(0.06)


@pytest.mark.parametrize("bad", [channel("h2", 9, "Outlier"), channel("h2", 9, units="cm/s/s"),
                                 channel("h2", None), channel("h2", math.nan), channel("h2", -9),
                                 channel("z", 9), channel("other2", 9)])
def test_invalid_horizontal_pair_is_excluded(bad):
    assert _horizontal_pga([channel("h1", 4), bad]) is None


def test_committed_stations_use_event_distances_and_site_conditions(tmp_path):
    station = next(station for station in load_shakemap_stations() if station["code"] == "KTN")
    assert station["rrup_km"] == pytest.approx(370.594)
    assert station["vs30"] == pytest.approx(400.41)
    assert station["pga_g"] == pytest.approx(math.sqrt(1.3778 * 1.2626) / 100)
    predictions = tmp_path / "modeled_dams.csv"
    predictions.write_text("not an observation")
    result = validate_against_shakemap(dam_risk_path=str(predictions))
    assert set(result.station_code) == {"NPW", "KTN", "NGU", "CHTO", "YGN"}
    assert result.within_distance_range.sum() == 3
    assert result.loc[result.station_code == "NPW", "rrup_km"].iloc[0] == pytest.approx(2.751)


def test_noninstrumental_and_missing_metadata_are_excluded(tmp_path):
    props = {"code": "TEST", "station_type": "macroseismic", "channels": [channel("h1", 4), channel("h2", 9)]}
    path = tmp_path / "stations.json"
    for station_type in ("macroseismic", "seismic"):
        props["station_type"] = station_type
        path.write_text(json.dumps({"features": [{"properties": props}]}))
        assert load_shakemap_stations(str(path)) == []


def test_buildings_use_only_event_rupture(monkeypatch):
    buildings = pd.DataFrame([{"lat": 20.0, "lon": 96.0, "category": "school"}])
    rupture = [((95.0, 19.0), (95.0, 21.0))]
    monkeypatch.setattr(osm_exposure, "load_osm_infrastructure", lambda: buildings)
    monkeypatch.setattr(osm_exposure, "_load_rupture_trace", lambda: rupture)
    monkeypatch.setattr(osm_exposure, "_get_vs30", lambda *args: 760)
    result = osm_exposure.compute_building_exposure()
    assert result.rrup_km.iloc[0] > 100
    monkeypatch.setattr(osm_exposure, "_load_rupture_trace", lambda: [])
    assert osm_exposure.compute_building_exposure().pga_g.isna().all()
