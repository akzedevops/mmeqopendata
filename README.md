# Myanmar Earthquake Open Data

A hobby project for collecting, analyzing, and visualizing earthquake data in Myanmar. Built on the [Myanmar Earthquake API](https://mmeq.akze.net), covering records from 1950 to present.

**Live dashboard:** [https://akzedevops.github.io/mmeqopendata/](https://akzedevops.github.io/mmeqopendata/)

**Write-up:** [`paper/main.pdf`](paper/main.pdf) — a casual hobby write-up on Myanmar dam seismic risk with 13 figures and open data.

## What It Does

- Fetches earthquake data from the API, validates, deduplicates, exports as CSV + JSON
- Scores 254 dams by seismic risk using PGA, fault proximity, and structural exposure
- Computes site-specific Vs30 from Copernicus DEM (Wald & Allen 2007 slope proxy)
- Builds probabilistic hazard curves (Cornell-McGuire PSHA) at each dam
- Uses Abrahamson & Silva (2008) GMPE with rupture distance to fault trace
- Runs b-value / completeness analysis, aftershock forecasting (Omori law)
- DBSCAN clustering in UTM coordinates, Monte Carlo sensitivity on risk weights
- Coulomb stress transfer modeling from USGS finite fault data
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

## How the Dam Risk Scoring Works

254 dams from [Open Development Mekong](https://data.opendevelopmentmekong.net/en/dataset/myanmar-dams) (IFC/WLE, CC BY-SA 4.0).

Each dam gets a composite risk score from four components:

| Component | Weight | What it measures |
|---|---|---|
| PGA (ground shaking) | 35% | Abrahamson & Silva 2008 GMPE with site-specific Vs30 |
| Fault proximity | 30% | Distance to nearest active fault segment |
| Mainshock proximity | 20% | Distance to the 2025 M7.7 epicenter |
| Structural exposure | 15% | Dam height, capacity, storage volume |

The ground motion model uses rupture distance (not epicentral distance) — this matters a lot for the 2025 M7.7 event because the fault rupture extended ~475 km along the Sagaing Fault. A dam might be 300 km from the epicenter but only 5 km from the rupture trace.

Vs30 (shear-wave velocity in the top 30 m) is estimated at each dam from the Copernicus DEM using the Wald & Allen (2007) slope proxy. Range: 600–1,100 m/s across the 254 sites.

I validated the GMPE against the actual Naypyidaw ShakeMap recording — predicted 0.51g vs observed 0.57g (ratio 1.12), which is well within one standard deviation.

**Results:**

| Grade | Count |
|---|---|
| Critical | 25 |
| High | 112 |
| Moderate | 10 |
| Low | 107 |

137 dams (54%) scored Critical or High.

## How the Seismology Works

The catalog has 9,242 events. Most are small (mean M 3.2), but there are 6 events ≥ M7.0 and 358 ≥ M5.0.

**b-value and completeness:** The Gutenberg-Richter b-value at Mc = 4.0 is 0.71. Below M4 the catalog is incomplete (the raw b-value of 0.33 was an artifact of missing small events). The b-value converges to ~1.0 at Mc ≥ 4.5, which is typical for tectonic regions.

**Clustering:** DBSCAN in UTM Zone 47N coordinates (so distances are in meters, not degrees) identifies 7 seismic zones. The central Myanmar cluster contains 95% of all events.

**Aftershock forecasting:** Modified Omori law fitted to the M7.7 sequence gives p = 0.83 (slightly below the global average of ~1.0), forecasting expected M≥3 aftershocks at 7, 30, and 90 day windows.

**Coulomb stress transfer:** Using the USGS finite fault model (530 slip patches, 0–7 m variable slip), I computed which dams are in stress-triggered zones (69 dams, 27%) vs stress shadows (127 dams, 50%).

## Probabilistic Hazard

Beyond single-event PGA, each dam gets a full hazard curve using the Cornell-McGuire PSHA approach — integrating the Gutenberg-Richter magnitude distribution with the GMPE over all possible magnitudes (M5–8).

The 475-year PGA (10% chance of exceedance in 50 years) ranges from 0.09g to 1.31g across the dam portfolio, with a mean of 0.31g. 32 dams exceed 0.5g at this return period.

## Building Exposure from OpenStreetMap

34,224 critical infrastructure sites pulled from OSM via Overpass API — schools, hospitals, clinics, police stations, fire stations, universities, and places of worship within the shaking zone.

For each building, PGA is estimated using the same ASK08 GMPE with rupture distance to the combined fault trace.

| Shaking Intensity | Buildings |
|---|---|
| Severe (VIII) | ~5,700 |
| Very Strong (VII) | ~6,200 |
| Strong (VI) | ~8,100 |

2,166 schools and 901 hospitals had PGA above 0.1g.

Uses site-specific Vs30 from the USGS ShakeMap grid (mean 320 m/s) — soft soils in the Irrawaddy basin amplify shaking significantly compared to reference rock.

Validated against the USGS ShakeMap grid — Naypyidaw prediction is 0.43g vs USGS 0.55g (ratio 0.79). Near-fault underprediction is expected because this was a supershear rupture.

## Figures

13 figures in [`paper/figures/`](paper/figures/) (300 DPI, PDF + PNG):

| Figure | Description |
|---|---|
| fig1 | Myanmar seismicity and dam locations |
| fig2 | Frequency-magnitude distribution + b-value stability |
| fig3 | Depth histogram + magnitude vs depth |
| fig4 | Dam risk map (color-coded by grade) |
| fig5 | ASK08 PGA attenuation curves with recorded data |
| fig6 | Dam risk map with all 254 dams |
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
│   └── writer.py                   # CSV/JSON export, validation, dedup
├── analysis/
│   ├── aftershock.py               # Modified Omori law
│   ├── clustering.py               # DBSCAN in UTM coordinates
│   ├── coulomb.py                  # Coulomb stress transfer
│   ├── dam_risk.py                 # ASK08 GMPE, Vs30, PGA, hazard curves, risk scoring
│   ├── finite_fault.py             # USGS finite fault model loader
│   ├── fragility.py                # Dam fragility curves
│   ├── gem_faults.py               # GEM Global Active Faults loader
│   ├── osm_exposure.py             # OSM building exposure analysis
│   ├── population.py               # Population exposure
│   ├── population_raster.py        # WorldPop raster-based exposure
│   ├── seismology.py               # b-value, Mc, declustering
│   ├── shakemap_validation.py      # GMPE validation against ShakeMap stations
│   └── temporal.py                 # Temporal analysis charts
└── visualization/
    ├── animated_map.py             # Animated earthquake timeline
    ├── cross_section.py            # 3D depth visualization
    ├── dashboard.py                # Plotly dashboard
    ├── map.py                      # Folium interactive map
    └── report.py                   # PDF report generation
```

## Configuration

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
| Population | [WorldPop](https://www.worldpop.org/) Myanmar 1km grid | CC BY 4.0 |
| Elevation | [Copernicus DEM 30m](https://copernicus-dem-30m.s3.eu-central-1.amazonaws.com/) | Open |
| PGA model | Abrahamson & Silva (2008) NGA-West1 | Academic |
| Vs30 proxy | Wald & Allen (2007) slope-based | Academic |

## Tests

```bash
pytest tests/ -v
```

39 tests covering data validation, date ranges, CSV/JSON I/O, deduplication, seismology, and clustering.

## API

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
