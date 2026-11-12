"""Generate publication values from report artifacts and validate site downloads."""
import hashlib
import json
import re
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlsplit

import numpy as np
import pandas as pd

from mmeq.analysis.seismology import b_value, decluster_catalog


class _Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.paths = []

    def handle_starttag(self, tag, attrs):
        for name, value in attrs:
            if value and ((tag == "a" and name == "href") or (tag == "img" and name == "src")):
                self.paths.append(value)


def check_site_links(site: Path) -> None:
    """Fail on missing local homepage and hazard-index downloads, without network."""
    for page in (site / "index.html", site / "report/hazard_curves/index.html"):
        parser = _Links()
        parser.feed(page.read_text(encoding="utf-8"))
        for link in parser.paths:
            parts = urlsplit(link)
            if parts.scheme or parts.netloc or not parts.path:
                continue
            target = (page.parent / unquote(parts.path)).resolve()
            if site.resolve() not in target.parents and target != site.resolve():
                raise ValueError(f"Link escapes site: {page}: {link}")
            if target.is_dir():
                target /= "index.html"
            if not target.is_file():
                raise ValueError(f"Missing published file: {page}: {link}")


def _replace_block(text: str, name: str, replacement: str) -> str:
    pattern = rf"(<!-- BEGIN {name} -->).*?(<!-- END {name} -->)"
    text, count = re.subn(pattern, lambda match: f"{match[1]}\n{replacement}\n{match[2]}", text, flags=re.S)
    if count != 1:
        raise ValueError(f"Expected exactly one {name} publication block")
    return text


def publish(catalog_path: Path, site: Path, paper: Path, readme: Path, report_input: Path) -> dict:
    catalog = pd.read_csv(catalog_path)
    catalog["time_utc"] = pd.to_datetime(catalog["time_utc"], errors="raise")
    report = site / "report"
    risk = pd.read_csv(report / "dam_risk_scores.csv")
    buildings = pd.read_csv(report / "building_exposure.csv")
    stations = pd.read_csv(report / "shakemap_validation.csv")
    if stations.empty or (stations["station_code"] == "DAM").any():
        raise ValueError("Publication requires independent station-only validation")
    sensitivity = pd.read_csv(report_input / "sensitivity_analysis.csv")
    sensitivity.to_csv(report / "sensitivity_analysis.csv", index=False)
    vs30 = risk["vs30"]
    grades = risk["risk_grade"].value_counts()
    declustered = decluster_catalog(catalog)
    raw_b, _, raw_mc = b_value(catalog["mag"])
    fixed_b, _, _ = b_value(catalog["mag"], min_mag=4.0)
    dec_b, _, dec_mc = b_value(declustered["mag"])
    hazard = []
    for path in sorted((report / "hazard_curves").glob("*.csv")):
        curve = pd.read_csv(path)
        rates = curve["annual_rate"].to_numpy()
        if rates.max() <= 1 / 475 or rates.min() >= 1 / 475:
            raise ValueError(f"475-year PGA outside sampled curve: {path}")
        hazard.append(float(np.interp(1 / 475, rates[::-1], curve["pga_g"].to_numpy()[::-1])))
    if len(hazard) != len(risk):
        raise ValueError("Hazard curve count must match dam inventory")
    high_count = int(grades.get("Critical", 0) + grades.get("High", 0))
    values = {
        "CatalogCount": f"{len(catalog):,}",
        "CatalogLatest": catalog["time_utc"].max().strftime("%Y-%m-%d"),
        "MeanMagnitude": f"{catalog['mag'].mean():.2f}",
        "LargeFive": str(int((catalog["mag"] >= 5).sum())),
        "LargeSeven": str(int((catalog["mag"] >= 7).sum())),
        "ShallowPercent": f"{100 * (catalog['depth'] < 30).mean():.1f}",
        "RawB": f"{raw_b:.2f}", "RawMc": f"{raw_mc:.1f}",
        "FixedB": f"{fixed_b:.2f}", "FixedCount": f"{int((catalog['mag'] >= 4).sum()):,}",
        "DeclusteredB": f"{dec_b:.2f}", "DeclusteredMc": f"{dec_mc:.1f}",
        "DamCount": str(len(risk)), "HighCount": str(high_count),
        "HighPercent": f"{100 * high_count / len(risk):.1f}",
        "VsMin": f"{vs30.min():.0f}", "VsMax": f"{vs30.max():.0f}", "VsMean": f"{vs30.mean():.0f}",
        "HazardMin": f"{min(hazard):.3f}", "HazardMax": f"{max(hazard):.3f}",
        "HazardMean": f"{np.mean(hazard):.3f}",
        "HazardAboveOne": str(sum(value > 0.1 for value in hazard)),
        "HazardAboveTwo": str(sum(value > 0.2 for value in hazard)),
        "BuildingCount": f"{len(buildings):,}",
        "BuildingSevere": f"{int((buildings['mmi_label'] == 'Severe (VIII)').sum()):,}",
        "BuildingVeryStrong": f"{int((buildings['mmi_label'] == 'Very Strong (VII)').sum()):,}",
        "BuildingStrong": f"{int((buildings['mmi_label'] == 'Strong (VI)').sum()):,}",
        "SchoolCount": f"{int(((buildings['category'] == 'school') & (buildings['pga_g'] > 0.1)).sum()):,}",
        "HospitalCount": f"{int(((buildings['category'] == 'hospital') & (buildings['pga_g'] > 0.1)).sum()):,}",
        "StationCount": str(len(stations)),
        "InRangeStations": str(int(stations["within_distance_range"].sum())),
    }
    generated = paper / "generated"
    generated.mkdir(parents=True, exist_ok=True)
    (generated / "stats.tex").write_text(
        "".join(f"\\newcommand{{\\{name}}}{{{value}}}\n" for name, value in values.items()), encoding="utf-8",
    )
    risk_rows = [f"{grade} & {grades.get(grade, 0)} & {100 * grades.get(grade, 0) / len(risk):.1f}\\% \\\\"
                 for grade in ("Critical", "High", "Moderate", "Low")]
    risk_table = ("\\begin{tabular}{@{}lrr@{}}\n\\toprule\n"
                  "Grade & Count & \\% \\\\\n\\midrule\n" + "\n".join(risk_rows)
                  + "\n\\bottomrule\n\\end{tabular}\n")
    (generated / "risk_table.tex").write_text(risk_table, encoding="utf-8")
    station_rows = []
    for row in stations.sort_values("rrup_km").itertuples():
        if not re.fullmatch(r"[A-Za-z0-9.-]+", row.station_code):
            raise ValueError("Unsafe station code in LaTeX output")
        station_rows.append(
            f"{row.station_code} & {row.rrup_km:.1f} & {row.vs30:.0f} & "
            f"{row.pga_observed_g:.3f} & {row.pga_predicted_g:.3f} & {row.ratio:.2f} \\\\"
        )
    station_table = ("\\begin{tabular}{@{}lrrrrr@{}}\n\\toprule\n"
                     "Station & $R_{rup}$ (km) & $V_{S30}$ (m/s) & Obs. ($g$) & Pred. ($g$) & Ratio \\\\\n"
                     "\\midrule\n" + "\n".join(station_rows) + "\n\\bottomrule\n\\end{tabular}\n")
    (generated / "station_table.tex").write_text(station_table, encoding="utf-8")
    critical = risk[risk["risk_grade"] == "Critical"].sort_values("composite_risk", ascending=False).head(10)
    fields = ["name", "pga_g", "dist_to_fault_km", "composite_risk"]
    metadata = {"catalog_sha256": hashlib.sha256(catalog_path.read_bytes()).hexdigest(), "values": values,
                "critical_dams": critical[fields].to_dict(orient="records")}
    (report / "publication.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    homepage = site / "index.html"
    html = homepage.read_text(encoding="utf-8")
    html = re.sub(r'(data-stat="([A-Za-z]+)">)[^<]*',
                  lambda match: match[1] + values[match[2]], html)
    years = {str(year): int(count) for year, count in catalog.groupby(catalog["time_utc"].dt.year).size().items()}
    html, replacements = re.subn(r"const YEARS = .*?;", "const YEARS = " + json.dumps(years) + ";", html)
    if replacements != 1:
        raise ValueError("Expected one yearly download manifest")
    homepage.write_text(html, encoding="utf-8")
    summary = (
        f"Catalog through **{values['CatalogLatest']}**: **{values['CatalogCount']} events**, "
        f"mean M {values['MeanMagnitude']}; {values['LargeFive']} M≥5 and {values['LargeSeven']} M≥7.\n\n"
        f"Dam screening: **{high_count}/{len(risk)} Critical or High ({values['HighPercent']}%)**. "
        + ", ".join(f"{grade}: {grades.get(grade, 0)}" for grade in ("Critical", "High", "Moderate", "Low"))
        + f". These are heuristic screening grades, not failure probabilities.\n\n"
        f"Catalog-only 475-year PGA: **{values['HazardMin']}–{values['HazardMax']}g**, "
        f"mean {values['HazardMean']}g; {values['HazardAboveOne']} dams above 0.1g.\n\n"
        f"Scenario building exposure: {values['BuildingSevere']} Severe (VIII), "
        f"{values['BuildingVeryStrong']} Very Strong (VII), {values['BuildingStrong']} Strong (VI); "
        f"{values['SchoolCount']} schools and {values['HospitalCount']} hospitals above 0.1g. "
        "These are modeled values, not measured damage.\n\n"
        f"Station comparison: {len(stations)} unflagged horizontal pairs; "
        f"{values['InRangeStations']} within 200 km. See the generated paper table and scientific audit."
    )
    text = _replace_block(readme.read_text(encoding="utf-8"), "PUBLICATION", summary)
    text = re.sub(r"Earthquakes-(?:\d|%2[Cc]|,)+-orange",
                  f"Earthquakes-{len(catalog):,}-orange".replace(",", "%2C"), text)
    readme.write_text(text, encoding="utf-8")
    return metadata
