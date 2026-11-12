from pathlib import Path

import pytest
import json
import pandas as pd

from mmeq.publication import _replace_block, check_site_links, publish


def test_publication_replaces_exactly_one_block():
    source = "before<!-- BEGIN PUBLICATION -->old<!-- END PUBLICATION -->after"
    assert _replace_block(source, "PUBLICATION", "new") == (
        "before<!-- BEGIN PUBLICATION -->\nnew\n<!-- END PUBLICATION -->after"
    )
    for invalid in ("no markers", source + source):
        with pytest.raises(ValueError):
            _replace_block(invalid, "PUBLICATION", "new")


def test_local_download_validation(tmp_path):
    hazard = tmp_path / "report/hazard_curves"
    hazard.mkdir(parents=True)
    (hazard / "index.html").write_text('<a href="dam.csv">dam</a>')
    (hazard / "dam.csv").write_text("pga_g,annual_rate\n")
    homepage = tmp_path / "index.html"
    homepage.write_text('<a href="report/hazard_curves/">curves</a><a href="https://example.com/">offsite</a>')
    check_site_links(tmp_path)
    homepage.write_text('<a href="report/hazard_curves/sensitivity_analysis.csv">missing</a>')
    with pytest.raises(ValueError, match="Missing published file"):
        check_site_links(tmp_path)
    homepage.write_text('<a href="../outside">escape</a>')
    with pytest.raises(ValueError, match="escapes site"):
        check_site_links(tmp_path)


def test_no_published_downloads_are_missing():
    check_site_links(Path(__file__).resolve().parents[1] / "docs")


def test_publication_generation_is_consistent_and_repeatable(tmp_path, monkeypatch):
    from mmeq import publication
    monkeypatch.setattr(publication, "decluster_catalog", lambda frame: frame)
    monkeypatch.setattr(publication, "b_value", lambda *args, **kwargs: (1.0, 2.0, 4.0))
    site = tmp_path / "docs"
    report = site / "report"
    hazard = report / "hazard_curves"
    hazard.mkdir(parents=True)
    inputs = tmp_path / "report"
    inputs.mkdir()
    paper = tmp_path / "paper"
    readme = tmp_path / "README.md"
    readme.write_text("Earthquakes-9%2C679-orange.svg\n<!-- BEGIN PUBLICATION -->old<!-- END PUBLICATION -->")
    (site / "index.html").write_text('<span data-stat="CatalogCount">0</span><script>const YEARS = {};</script>')
    catalog = tmp_path / "catalog.csv"
    catalog.write_text("time_utc,mag,depth\n2026-11-11,5,10\n2026-11-10,4,40\n")
    pd.DataFrame([{"name": 'Dam, <test>', "risk_grade": "Critical", "vs30": 760,
                   "pga_g": 0.2, "dist_to_fault_km": 10, "composite_risk": 8}]).to_csv(
                       report / "dam_risk_scores.csv", index=False)
    pd.DataFrame([{"mmi_label": "Severe (VIII)", "category": "school", "pga_g": 0.2}]).to_csv(
        report / "building_exposure.csv", index=False)
    pd.DataFrame([{"station_code": "TEST", "rrup_km": 10, "vs30": 760, "pga_observed_g": 0.3,
                   "pga_predicted_g": 0.2, "ratio": 1.5, "within_distance_range": True}]).to_csv(
                       report / "shakemap_validation.csv", index=False)
    (hazard / "dam.csv").write_text("pga_g,annual_rate\n0.01,0.1\n1,0.0001\n")
    (inputs / "sensitivity_analysis.csv").write_text("critical,high\n1,0\n")
    first = publish(catalog, site, paper, readme, inputs)
    assert first["values"]["CatalogCount"] == "2"
    assert first["critical_dams"][0]["name"] == 'Dam, <test>'
    assert json.loads((report / "publication.json").read_text()) == first
    assert (report / "sensitivity_analysis.csv").is_file()
    assert 'const YEARS = {"2026": 2};' in (site / "index.html").read_text()
    before = readme.read_text()
    assert "Earthquakes-2-orange.svg" in before
    assert publish(catalog, site, paper, readme, inputs) == first
    assert readme.read_text() == before
