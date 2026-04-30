# Myanmar Earthquake Open Data

A Python toolkit for collecting, analyzing, and visualizing earthquake data in Myanmar. Built on the [Myanmar Earthquake API](https://mmeq.akze.net), covering records from 1950 to present. Includes site-specific seismic hazard assessment, dam risk scoring, probabilistic hazard curves, and automated report generation.

**Live dashboard:** [https://akzedevops.github.io/mmeqopendata/](https://akzedevops.github.io/mmeqopendata/)

**Research paper:** [`paper/main.pdf`](paper/main.pdf) — 21-page peer-review-ready manuscript with 10 figures, 3 tables, and 13 references.

## Features

- **Data Pipeline** — Fetch earthquake data from the API month-by-month in parallel, validate, deduplicate, export as CSV + JSON
- **Interactive Map** — Folium HTML map with magnitude-colored markers, heatmap, fault lines, dam overlay (CartoDB tiles)
- **Dam Risk Scoring** — 254 dams scored by PGA, fault proximity, and structural exposure (25 Critical, 119 High risk)
- **Site-Specific Vs30** — Topographic slope proxy (Wald & Allen 2007) from Copernicus DEM for each dam site
- **Probabilistic Hazard Curves** — Cornell-McGuire PSHA with 475-year PGA at each dam (mean: 0.31g, max: 1.31g)
- **Modern GMPE** — Abrahamson & Silva (2008) NGA-West1 for crustal strike-slip with Joyner-Boore distance to fault trace
- **Catalog Completeness Analysis** — b-value stability testing, multi-period analysis, corrected Mc = 4.0
- **Aftershock Forecasting** — Modified Omori law with decay parameter estimation
- **Plotly Dashboard** — 6-panel interactive dashboard (timeline, depth, FMD, mag-vs-depth, monthly counts, dam risk)
- **Monte Carlo Sensitivity** — 100-iteration weight perturbation analysis for risk scoring robustness
- **UTM-Projected Clustering** — DBSCAN in metric coordinates (Zone 47N) for physically correct distances
- **3D Cross-Section** — lat/lon/depth visualization along the Sagaing Fault
- **Animated Timeline** — Month-by-month earthquake progression (TimestampedGeoJson)
- **Population Exposure** — Estimated population within radius of earthquake/dam sites
- **PDF Reports** — Auto-generated comprehensive report with statistics and risk tables
- **GitHub Pages** — Auto-deployed site with all interactive outputs
- **CI/CD** — Daily data fetch + report generation + Pages deployment

## Requirements

- Python 3.9+

## Installation

```bash
git clone https://github.com/akzedevops/mmeqopendata.git
cd mmeqopendata
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Quick Start

```bash
# Fetch latest earthquake data
mmeq export

# Generate full report (all analyses + all outputs)
mmeq report --output ./report

# Individual commands
mmeq analyze --type all
mmeq visualize --min-mag 3.0
mmeq analyze --type seismology --decluster
```

## CLI Reference

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

Run analyses on earthquake data.

```bash
mmeq analyze --type all                          # Run everything
mmeq analyze --type temporal                      # Frequency, distribution, depth charts
mmeq analyze --type clustering                    # DBSCAN cluster map with dam overlay
mmeq analyze --type seismology                    # b-value, Mc, declustering
mmeq analyze --type seismology --decluster        # Include temporal declustering
mmeq analyze --dam-risk                           # Dams within seismic cluster zones
mmeq analyze --min-mag 4.0 --mc 3.5               # Filter and override Mc
mmeq analyze --no-dams                            # Disable dam overlay on cluster map
```

| Type | Output | Description |
|---|---|---|
| `temporal` | 3 PNG charts | Monthly frequency, magnitude distribution, mag-vs-depth |
| `clustering` | `earthquake_clusters.png` | DBSCAN map with depth colors, zone labels, dams |
| `seismology` | Console summary | b-value, a-value, Mc, optional declustering |

### `mmeq visualize`

Interactive Folium HTML map.

```bash
mmeq visualize                      # All events, all layers
mmeq visualize --min-mag 4.0        # Only M4.0+
mmeq visualize --no-heatmap         # Disable heatmap
mmeq visualize --no-dams            # Disable dam overlay
```

Map features: magnitude-colored circle markers, marker clustering, heatmap layer, fault line overlay, dam overlay (254 dams, color-coded by status), CartoDB tiles.

### `mmeq report`

Full pipeline: all analyses + all outputs in one command.

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

| Output | File | Description |
|---|---|---|
| Interactive Map | `enhanced_earthquake_map.html` | Folium map with earthquakes, faults, dams |
| Dashboard | `dashboard.html` | 6-panel Plotly interactive dashboard |
| 3D View | `depth_cross_section.html` | 3D lat/lon/depth scatter |
| Animation | `animated_earthquake_map.html` | Time-series earthquake progression |
| PDF Report | `myanmar_earthquake_report.pdf` | Comprehensive report with tables |
| Dam Risk | `dam_risk_scores.csv` | 254 dams with PGA, Vs30, fault distance, risk grade, 475yr PGA |
| Hazard Curves | `hazard_curves/*.csv` | PGA vs return period at each dam site |
| Population | `population_exposure.csv` | People within 50km of major earthquakes |
| Sensitivity | `sensitivity_analysis.csv` | 100 Monte Carlo weight perturbation results |

## Dam Risk Assessment

254 dams from [Open Development Mekong](https://data.opendevelopmentmekong.net/en/dataset/myanmar-dams) (IFC/WLE, CC BY-SA 4.0).

**Ground Motion Model:** Abrahamson & Silva (2008) NGA-West1 for crustal strike-slip, with Joyner-Boore distance ($R_{jb}$) to the nearest fault trace (not epicentral distance), properly accounting for the 500 km extended rupture of the 2025 M7.7 event.

**Site Effects:** Vs30 estimated at each dam site from the Copernicus DEM (30 m) using the Wald & Allen (2007) topographic slope proxy at 1 km resolution.

| Component | Weight | Source |
|---|---|---|
| Seismic (PGA) | 35% | ASK08 GMPE with site-specific Vs30 |
| Fault proximity | 30% | Distance to nearest fault segment |
| Mainshock proximity | 20% | Distance to M7.7 epicenter |
| Structural exposure | 15% | Dam height, capacity, storage volume |

**Results (254 dams):**

| Grade | Count | Criteria |
|---|---|---|
| Critical | 25 | Composite score >= 7 |
| High | 119 | 5 <= score < 7 |
| Moderate | 100 | 3 <= score < 5 |
| Low | 10 | score < 3 |

**Vs30 distribution:** 600-1,100 m/s (mean: 968 m/s, median: 900 m/s)

**475-year PGA (10% exceedance in 50 years):** 0.09-1.31g (mean: 0.31g), 32 dams exceed 0.5g

**Dam properties:**
- 160 completed, 51 proposed, 14 planned, 9 under construction
- Functions: 117 irrigation, 93 hydropower, 21 multi-purpose, 5 water supply

## Seismicity Analysis

- **b-value:** 0.71 at Mc = 4.0 (corrected for catalog incompleteness; raw value of 0.33 was an artifact)
- **b-value stability:** Converges to ~1.0 at Mc >= 4.5, confirming incompleteness below M4
- **7 seismic zones** identified via UTM-projected DBSCAN (eps = 33 km)
- **Central Myanmar cluster:** 8,775 events (95%), max M7.7 (2025-03-28)

## Aftershock Forecasting

Modified Omori law parameters fitted to the M7.7 aftershock sequence:

```
p (decay exponent): 0.83 (slightly below global average of ~1.0)
```

Forecasts expected number of M>=3 aftershocks at 7, 30, and 90 day windows.

## Publication Figures

10 publication-quality figures in [`paper/figures/`](paper/figures/) (300 DPI, PDF + PNG):

| Figure | Description |
|---|---|
| fig1_study_area | Myanmar seismicity and dam infrastructure |
| fig2_fmd | Frequency-magnitude distribution + b-value stability |
| fig3_depth | Depth histogram + magnitude vs depth |
| fig4_dam_risk_map | Dam risk assessment map |
| fig5_pga_attenuation | ASK08 attenuation curves with recorded PGA |
| fig6_temporal | Temporal evolution of seismicity |
| fig7_risk_distribution | Risk scores + grade counts + Monte Carlo sensitivity |
| fig8_dam_types | Dam status and function distributions |
| fig9_vs30_map | Site-specific Vs30 classification |
| fig10_hazard_curves | PSHA hazard curves at representative dams |

Regenerate all figures:
```bash
python generate_figures.py
```

## Project Structure

```
src/mmeq/
├── __init__.py
├── cli.py                          # CLI entry point (4 subcommands)
├── config.py                       # Config constants + env var overrides
├── export/
│   ├── fetcher.py                  # API fetching, date range generation
│   └── writer.py                   # CSV/JSON save, validation, dedup
├── analysis/
│   ├── aftershock.py               # Modified Omori law forecast
│   ├── clustering.py               # DBSCAN clustering (UTM-projected) + map + dam overlay
│   ├── dam_risk.py                 # ASK08 GMPE, Vs30, PGA, hazard curves, risk scoring
│   ├── population.py               # Population exposure estimation
│   ├── seismology.py               # b-value, Mc, stability analysis, multi-period, declustering
│   └── temporal.py                 # Frequency, distribution, depth charts
└── visualization/
    ├── animated_map.py             # TimestampedGeoJson timeline
    ├── cross_section.py            # 3D depth visualization
    ├── dashboard.py                # Plotly 6-panel dashboard
    ├── map.py                      # Folium interactive map
    └── report.py                   # PDF report generation
```

## Configuration

| Variable | Default | Description |
|---|---|---|
| `MMEQ_API_URL` | `https://mmeq.akze.net/api/myanmar-quakes` | API endpoint |
| `MMEQ_START_YEAR` | `1950` | Earliest year to fetch |
| `MMEQ_EXPORT_DIR` | `quake_exports` | Data output directory |
| `MMEQ_MAX_WORKERS` | `10` | Parallel download threads |
| `MMEQ_FAULT_LINES` | `fault_lines.json` | Fault line GeoJSON path |
| `MMEQ_OUTPUT_DIR` | `.` | Analysis/chart output directory |

## Data Fields

| Field | Type | Description |
|---|---|---|
| `time_utc` | datetime | UTC timestamp |
| `time_mmt` | datetime | Myanmar Standard Time (UTC+6:30) |
| `latitude` | float | Latitude |
| `longitude` | float | Longitude |
| `depth` | float | Depth in km |
| `mag` | float | Magnitude |
| `magType` | string | Magnitude type (ml, mb, mw, mww, etc.) |
| `location` | string | Descriptive location name |
| `id` | string | Unique event identifier |

## Running Tests

```bash
pip install -e ".[dev]"
pytest tests/ -v
pytest tests/ -v --cov=mmeq
```

39 tests covering data validation, date ranges, CSV/JSON I/O, deduplication, seismology, and clustering.

## CI/CD

Two GitHub Actions workflows:

| Workflow | Trigger | Description |
|---|---|---|
| `daily_data_fetch.yml` | Daily at midnight UTC | Fetch new data, auto-commit |
| `report_and_pages.yml` | Push to master | Generate report, deploy to Pages |

The daily workflow triggers the report workflow after data updates, so the live site stays current.

## Data Sources

| Data | Source | License |
|---|---|---|
| Earthquakes | [Myanmar Earthquake API](https://mmeq.akze.net) (USGS/ISC) | Open |
| Dams | [Open Development Mekong](https://data.opendevelopmentmekong.net/en/dataset/myanmar-dams) (IFC/WLE) | CC BY-SA 4.0 |
| Fault lines | USGS plate boundary data | Public domain |
| Elevation | [Copernicus DEM 30m](https://copernicus-dem-30m.s3.eu-central-1.amazonaws.com/) | Open |
| PGA model | Abrahamson & Silva (2008) NGA-West1 | Academic |
| Vs30 proxy | Wald & Allen (2007) slope-based classification | Academic |

## API Reference

### Endpoints

```
GET https://mmeq.akze.net/api/myanmar-quakes?from=YYYY-MM-DD&to=YYYY-MM-DD
GET https://mmeq.akze.net/api/myanmar-quakes?date=YYYY-MM-DD
```

### Example

```python
import requests

resp = requests.get(
    "https://mmeq.akze.net/api/myanmar-quakes",
    params={"from": "2025-03-01", "to": "2025-03-31"}
)
for q in resp.json()["earthquakes"]:
    print(f"M{q['mag']} at {q['location']}")
```

No authentication required. Free and open. Avoid polling more than once per second.

## License

MIT License — see [LICENSE](LICENSE).
