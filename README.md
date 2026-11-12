# Myanmar Earthquake Open Data

[![CI](https://github.com/akzedevops/mmeqopendata/actions/workflows/report_and_pages.yml/badge.svg)](https://github.com/akzedevops/mmeqopendata/actions/workflows/report_and_pages.yml)
[![Daily Fetch](https://github.com/akzedevops/mmeqopendata/actions/workflows/daily_data_fetch.yml/badge.svg)](https://github.com/akzedevops/mmeqopendata/actions/workflows/daily_data_fetch.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.9+](https://img.shields.io/badge/Python-3.9+-green.svg)](https://python.org)
[![Events](https://img.shields.io/badge/Earthquakes-10%2C082-orange.svg)](https://akzedevops.github.io/mmeqopendata/)

A hobby project for collecting, analyzing, and visualizing historical and recent earthquake data in Myanmar. Built on the [Myanmar Earthquake API](https://mmeq.akze.net); coverage depends on the available catalog and network history.

**Live dashboard:** [https://akzedevops.github.io/mmeqopendata/](https://akzedevops.github.io/mmeqopendata/)

**Write-up:** [`paper/main.pdf`](paper/main.pdf) — a casual hobby write-up on Myanmar dam seismic risk with 14 figures and open data.

**Scientific audit:** [`docs/scientific-audit.md`](docs/scientific-audit.md) records corrected methods, independent reference checks, and remaining limitations. These outputs support screening, not engineering safety decisions.

## What It Does

- Fetches earthquake data from the API, validates, deduplicates, exports as CSV + JSON
- Scores 254 dams by seismic risk using PGA, fault proximity, and structural exposure
- Computes site-specific Vs30 from the USGS ShakeMap grid (Wald & Allen 2007 slope proxy)
- Builds probabilistic hazard curves (Cornell-McGuire PSHA) at each dam
- Uses Abrahamson & Silva (2008) GMPE with rupture distance to fault trace
- Runs b-value / completeness analysis, aftershock forecasting (Omori law)
- DBSCAN clustering in UTM coordinates, Monte Carlo sensitivity on risk weights
- Approximate Coulomb stress transfer using an idealized seven-segment, uniform-slip rupture
- Generates interactive maps (Folium), dashboards (Plotly), 3D views, animated timelines
- Auto-deploys to GitHub Pages via CI/CD

## Quick Start

```bash
git clone https://github.com/akzedevops/mmeqopendata.git
cd mmeqopendata
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

mmeq export                        # Fetch earthquake data
mmeq report --output ./report      # Generate everything
mmeq analyze --type all            # Run analyses only
mmeq visualize --min-mag 3.0       # Interactive map
```

## CLI

### `mmeq export`

Fetches earthquake data from the API and exports as CSV/JSON.

```bash
mmeq export                    # Fetch all pending months
mmeq export --workers 5        # Use 5 parallel threads (default: 10)
```

Output structure:
```
quake_exports/
├── csv/
│   ├── monthly/    # earthquakes_YYYY_MM.csv
│   ├── yearly/     # earthquakes_YYYY.csv
│   └── combined/   # earthquakes_combined.csv
└── json/
    ├── monthly/    # earthquakes_YYYY_MM.json
    ├── yearly/     # earthquakes_YYYY.json
    └── combined/   # earthquakes_combined.json
```

### `mmeq analyze`

```bash
mmeq analyze --type all                          # Run everything
mmeq analyze --type temporal                      # Frequency, distribution, depth charts
mmeq analyze --type clustering                    # DBSCAN cluster map with dam overlay
mmeq analyze --type seismology --decluster        # b-value, Mc, declustering
mmeq analyze --dam-risk                           # Dam risk scoring
mmeq analyze --min-mag 4.0 --mc 3.5               # Filter and override Mc
```

### `mmeq visualize`

```bash
mmeq visualize                      # All events, all layers
mmeq visualize --min-mag 4.0        # Only M4.0+
mmeq visualize --no-heatmap         # Disable heatmap
mmeq visualize --no-dams            # Disable dam overlay
```

### `mmeq report`

Full pipeline — all analyses + all outputs in one command.

```bash
mmeq report --output ./report       # Generate everything
mmeq report --no-pdf                # Skip PDF
mmeq report --no-3d                 # Skip 3D cross-section
mmeq report --no-forecast           # Skip aftershock forecast
mmeq report --no-dashboard          # Skip Plotly dashboard
mmeq report --no-animated           # Skip animated map
mmeq report --no-population         # Skip population exposure
mmeq report --no-dams               # Skip dam risk analysis
```

| Output | File |
|---|---|
| Interactive Map | `enhanced_earthquake_map.html` |
| Dashboard | `dashboard.html` |
| 3D View | `depth_cross_section.html` |
| Animation | `animated_earthquake_map.html` |
| PDF Report | `myanmar_earthquake_report.pdf` |
| Dam Risk | `dam_risk_scores.csv` |
| Hazard Curves | `hazard_curves/*.csv` |
| Population | `population_exposure.csv` |
| Sensitivity | `sensitivity_analysis.csv` |

### `mmeq publish`

Synchronizes the README summary, homepage statistics, paper tables, and publication
manifest from the catalog and generated analysis outputs. Run from the repository
root after generating the report; this command does not rerun the scientific analyses.

```bash
mmeq publish                      # Refresh publication data and catalog SHA-256
mmeq publish --check-links        # Check local homepage/hazard-index links only
```

## Publication and Automation

- Daily data fetch runs the Go exporter at **00:17 UTC**; GitHub schedules may be delayed.
- A nightly Python-versus-Go export comparison checks byte parity against the live API.
- The report workflow tests the code, rebuilds the report, synchronizes publication
  data, regenerates figures and the paper, checks links, and deploys GitHub Pages.
- The freshness monitor runs at **12:43 UTC** and opens or updates an issue for
  missing successful fetch/parity runs within 48 hours, a failed latest report, or
  a deployed catalog hash mismatch. Earthquake occurrence time is not a job heartbeat.
- Changes go through a branch and pull request; deployment follows merge to `master`.

## How the Dam Risk Scoring Works

254 dams from [Open Development Mekong](https://data.opendevelopmentmekong.net/en/dataset/myanmar-dams) (IFC/WLE, CC BY-SA 4.0).

Each dam gets a composite risk score from four components:

| Component | Weight | What it measures |
|---|---|---|
| PGA (ground shaking) | 35% | Abrahamson & Silva 2008 GMPE with site-specific Vs30 |
| Fault proximity | 30% | Distance to a mapped fault/plate-boundary trace (screening proxy) |
| Mainshock proximity | 20% | Distance to the 2025 M7.7 epicenter |
| Structural exposure | 15% | Dam height, capacity, storage volume |

The ground motion model uses rupture distance (not epicentral distance) — this matters a lot for the 2025 M7.7 event because the fault rupture extended ~475 km along the Sagaing Fault. A dam might be 300 km from the epicenter but only 5 km from the rupture trace.

Vs30 (shear-wave velocity in the top 30 m) is sampled at each dam from the USGS ShakeMap Vs30 grid (a Wald & Allen 2007 topographic-slope proxy). Range: ~230–875 m/s across the 254 sites (mean ~450 m/s) — dams in the soft sediments of the central basin amplify shaking relative to those founded on rock.

The station diagnostic uses USGS event-specific rupture distances, station Vs30, and the geometric mean of two unflagged horizontal peaks. This approximates AS08's GMRotI50 component; it is not a regional model validation. Flagged PANO observations and modeled dam values are excluded. See [the scientific audit](docs/scientific-audit.md).

**Generated results** — synchronized by `mmeq publish` on every report build:

<!-- BEGIN PUBLICATION -->
Catalog through **2026-11-11**: **10,082 events**, mean M 3.21; 393 M≥5 and 6 M≥7.

Dam screening: **160/254 Critical or High (63.0%)**. Critical: 44, High: 116, Moderate: 45, Low: 49. These are heuristic screening grades, not failure probabilities.

Catalog-only 475-year PGA: **0.002–0.562g**, mean 0.143g; 172 dams above 0.1g.

Scenario building exposure: 3,395 Severe (VIII), 5,864 Very Strong (VII), 7,058 Strong (VI); 1,674 schools and 583 hospitals above 0.1g. These are modeled values, not measured damage.

Station comparison: 5 unflagged horizontal pairs; 3 within 200 km. See the generated paper table and scientific audit.
<!-- END PUBLICATION -->

## How the Seismology Works

Current catalog counts are generated above, rather than frozen in prose.

**USGS completeness backfill:** the upstream API is a multi-network union (62% Thai-network ids, 22% USGS, …) with gaps — most notably an ~18-month hole (2013-09…2015-02, all of 2014 empty). Missing events were backfilled from USGS FDSN (M≥4.0, epicenter inside a Myanmar ADM1 polygon), **spatio-temporally deduplicated** against the existing catalog so no cross-network duplicate is re-injected: 98 events for the 2014 window (spec 007) + 160 more across 1970–2019 (spec 008), pulled 2026-07-07, reproducible via `tools/backfill_usgs.py`. ~10 pre-existing cross-network near-duplicate pairs remain in the union. **Completeness caveat:** because the backfill stops at 2019, the catalog is more uniformly complete at M≥4 for 1970–2019 than for 2020–2025 (still the raw API), so time-resolved rate / temporal-b analysis should treat the 2019/2020 boundary as a coverage change, not a seismicity change. The exact backfilled event ids are frozen in `specs/008-backfill-manifest.csv` (ComCat is mutable).

**b-value and completeness:** The paper generates the full-catalog, fixed-Mc, and Gardner-Knopoff-declustered b-values from the current data. Mixed magnitude scales, changing network coverage, and the 2025 aftershock sequence complicate interpretation; a plausible b-value does not demonstrate completeness or stationarity. PSHA uses the declustered rates.

**Clustering:** DBSCAN in UTM Zone 47N coordinates (so distances are in meters, not degrees) identifies ~6 seismic zones (the exact count is sensitive to the DBSCAN radius; the dominant central cluster is the robust feature). The central Myanmar cluster contains ~96% of all events.

**Aftershock forecasting:** Modified Omori law fitted by maximum likelihood (Ogata 1983) to the 2025 M7.7 Sagaing sequence gives p ≈ 0.77 with c ≈ 16 h — a slow decay, partly reflecting early-sequence catalog incompleteness after the mainshock — forecasting expected M≥3 aftershocks at 7, 30, and 90 day windows. (Earlier releases quoted p = 0.93 from a biased least-squares fit that had also selected the 1988 Lancang M7.7 as the mainshock.)

**Coulomb stress proxy:** The default model uses an idealized seven-segment Sagaing trace with uniform moment-derived slip, **not** the archived USGS 530-patch slip solution. It classifies 16 dam locations positive and 72 negative at ±0.01 MPa. These are approximate crustal-receiver stress indicators, not dam structural loads or failure probabilities; the paper describes the limitations.

## Probabilistic Hazard

Beyond single-event PGA, each dam gets a Cornell-McGuire-style hazard curve integrating the Gutenberg-Richter distribution and GMPE over a sampled M5–8 range.

Current 475-year PGA results appear in the generated summary. These are **catalog-only estimates**, with no fault-source recurrence term and a simplified treatment of completeness, source depth, and magnitude scales. Regional comparisons with Yang et al. (2023, *Geoscience Letters* 10:48) and GEM are context, not independent validation: source models, site conditions, and locations differ. Annual exceedance rate and probability are exported separately (`annual_prob = 1 - exp(-annual_rate)`).

## Building Exposure from OpenStreetMap

34,224 critical infrastructure sites pulled from OSM via Overpass API — schools, hospitals, clinics, police stations, fire stations, universities, and places of worship within the shaking zone.

Each OSM element uses AS08 with distance to the **actual 2025 rupture only**, not unrelated faults. Updated exposure counts are generated above. These are scenario predictions, not measured shaking, structural damage, or casualty forecasts. OSM elements need not correspond one-to-one with physical buildings.

Uses site-specific Vs30 from the USGS ShakeMap grid (mean 320 m/s) — soft soils in the Irrawaddy basin amplify shaking significantly compared to reference rock.

The older offline grid comparison is not reproducible from the tracked data and is not retained as validation evidence. Sparse station recordings cannot establish systematic directivity bias or validate downstream loss assumptions.

## Figures

14 figures in [`paper/figures/`](paper/figures/) (300 DPI, PDF + PNG):

| Figure | Description |
|---|---|
| fig1 | Myanmar seismicity and dam locations |
| fig2 | Frequency-magnitude distribution + b-value stability |
| fig3 | Depth histogram + magnitude vs depth |
| fig4 | Dam risk map (color-coded by grade) |
| fig5 | ASK08 PGA attenuation curves with recorded data |
| fig6 | Annual earthquake counts (temporal evolution) |
| fig7 | Risk score distribution + grade counts + Monte Carlo sensitivity |
| fig8 | Dam status and function breakdowns |
| fig9 | Vs30 site classification map |
| fig10 | PSHA hazard curves at representative dams |
| fig11 | Coulomb stress transfer from the 2025 rupture |
| fig12 | Fragility curves (slight / moderate / extensive damage) |
| fig13 | Monte Carlo PGA sensitivity |
| fig14 | OSM building exposure map |

Regenerate all:
```bash
python generate_figures.py
```

## Project Structure

```
src/mmeq/
├── cli.py                          # CLI entry point
├── config.py                       # Config constants
├── export/
│   ├── fetcher.py                  # API fetching, date range generation
│   ├── writer.py                   # CSV/JSON export, validation, dedup, rebuild
│   └── pipeline.py                 # run_export orchestration
├── analysis/
│   ├── aftershock.py               # Modified Omori law
│   ├── clustering.py               # DBSCAN in UTM coordinates
│   ├── coulomb.py                  # Coulomb stress transfer
│   ├── dam_risk.py                 # ASK08 GMPE, Vs30, PGA, hazard curves, risk scoring
│   ├── finite_fault.py             # USGS finite fault model loader
│   ├── fragility.py                # Dam fragility curves
│   ├── gem_faults.py               # GEM Global Active Faults loader
│   ├── osm_exposure.py             # OSM building exposure analysis
│   ├── population.py               # Population exposure (city-based)
│   ├── geocoder.py                 # admin/place enrichment (point-in-polygon + kd-tree)
│   ├── seismology.py               # b-value, Mc, declustering, scenario selection
│   ├── shakemap_validation.py      # GMPE validation against ShakeMap stations
│   └── temporal.py                 # Temporal analysis charts
└── visualization/
    ├── animated_map.py             # Animated earthquake timeline
    ├── cross_section.py            # 3D depth visualization
    ├── dashboard.py                # Plotly dashboard
    ├── map.py                      # Folium interactive map
    ├── report.py                   # PDF report generation
    └── assets.py + vendor/         # self-hosted JS/CSS (see tools/fetch_vendor.py)

go/                                 # Go rewrite of the export pipeline (specs/002):
├── cmd/mmeq-export/                #   single static binary replacing `mmeq export`
└── internal/{api,catalog,config,   #   in the daily CI; byte-parity with the Python
              export,geocoder}      #   writer is golden-file tested

tools/                              # see tools/README.md
├── backfill_usgs.py                # USGS gap/completeness fill (spatio-temporal dedup)
├── backfill.py                     # mmeq-API monthly re-fetch/merge helper
├── build_figure_data.py            # precomputes dashboard/figure data
├── diff_exports.py                 # Python-vs-Go export tree diff (shadow CI gate)
└── fetch_vendor.py                 # download + sha256-pin the vendored JS/CSS
```

A nightly **shadow workflow** (`.github/workflows/shadow_go_export.yml`) runs both
exporters against the live API and diffs the trees. The Go binary already runs the
daily fetch; Python remains the reference implementation for parity checks and
local use (see `specs/002` and `specs/004`).

## Configuration

### Basemaps

The interactive earthquake map and animated timeline use OpenStreetMap standard
tiles, which require no API key. Attribution stays visible on each map. Tiles
are fetched online by the browser; they are not bundled with the self-hosted
JavaScript/CSS assets. Follow the [OSM tile usage policy](https://operations.osmfoundation.org/policies/tiles/):
no bulk downloading or offline prefetching, and preserve browser caching and
referrer headers. The community tile service is best-effort, not an unlimited
or guaranteed hosting service. Higher-traffic deployments should use a suitable
tile provider or self-hosted tiles.

### Environment Variables

Override defaults with environment variables:

| Variable | Default | Description |
|---|---|---|
| `MMEQ_API_URL` | `https://mmeq.akze.net/api/myanmar-quakes` | API endpoint |
| `MMEQ_START_YEAR` | `1950` | Earliest year to fetch |
| `MMEQ_EXPORT_DIR` | `quake_exports` | Data output directory |
| `MMEQ_MAX_WORKERS` | `10` | Parallel download threads |
| `MMEQ_FAULT_LINES` | `fault_lines.json` | Fault line GeoJSON path |

## Data Sources

| Data | Source | License |
|---|---|---|
| Earthquakes | [Myanmar Earthquake API](https://mmeq.akze.net) (USGS/ISC) | Open |
| Dams | [Open Development Mekong](https://data.opendevelopmentmekong.net/en/dataset/myanmar-dams) (IFC/WLE) | CC BY-SA 4.0 |
| Fault lines | USGS plate boundary data | Public domain |
| ShakeMap / Finite Fault | [USGS event us7000pn9s](https://earthquake.usgs.gov/earthquakes/eventpage/us7000pn9s) | Public domain |
| Active faults | [GEM Global Active Faults](https://github.com/GEMScienceTools/gem-global-active-faults) | CC BY-SA 4.0 |
| Buildings | [OpenStreetMap](https://www.openstreetmap.org/) via Overpass API | ODbL |
| Population | City-based estimates (in-repo); [WorldPop](https://www.worldpop.org/) Myanmar 1km grid used for a one-off offline exposure figure (raster not tracked) | CC BY 4.0 |
| Elevation | [Copernicus DEM 30m](https://copernicus-dem-30m.s3.eu-central-1.amazonaws.com/) | Open |
| PGA model | Abrahamson & Silva (2008) NGA-West1 | Academic |
| Vs30 proxy | Wald & Allen (2007) slope-based | Academic |

## Tests

```bash
ruff check .              # lint (must be clean)
pytest tests/ -v          # Python suite, including independent AS08 reference cases
cd go && go test ./...    # Go exporter suite incl. golden-parity fixtures
```

Python tests cover data validation, date ranges, CSV/JSON I/O, deduplication,
seismology, the hazard model (Coulomb parity, PSHA), map XSS-escaping, vendored
assets, station quality/components, publication links/provenance, and dam-risk scoring.
The Go suite additionally pins byte-level parity
with the Python writer/geocoder via golden files generated by running the actual
Python code.

## Contributing

Contributions welcome — see **[CONTRIBUTING.md](CONTRIBUTING.md)** for setup, the
dev loop, and the domain-correctness rules. Non-trivial changes are developed
spec-first (see [`specs/`](specs/)). Maintenance scripts are documented in
[`tools/README.md`](tools/README.md); agent-oriented guidance is in
[`CLAUDE.md`](CLAUDE.md) / [`AGENTS.md`](AGENTS.md).

## API

**Interactive docs: [mmeq.akze.net/docs](https://mmeq.akze.net/docs)** (Swagger UI) ·
OpenAPI spec: [/openapi.yaml](https://mmeq.akze.net/openapi.yaml)

Three routes: `/api/v2/earthquakes` (typed JSON — use this for programmatic access),
`/api/v2/export` (full-fidelity raw records — what this repo's export pipeline uses),
and the legacy `/api/myanmar-quakes` (kept for compatibility, shown below).

```
GET https://mmeq.akze.net/api/myanmar-quakes?from=YYYY-MM-DD&to=YYYY-MM-DD
```

```bash
curl -s "https://mmeq.akze.net/api/myanmar-quakes?from=2025-03-01&to=2025-03-31" | jq '.earthquakes[:3]'
```

```python
import requests
resp = requests.get(
    "https://mmeq.akze.net/api/myanmar-quakes",
    params={"from": "2025-03-01", "to": "2025-03-31"}
)
for q in resp.json()["earthquakes"]:
    print(f"M{q['mag']} at {q['location']}")
```

No auth required. Free and open.

## License

MIT — see [LICENSE](LICENSE).
