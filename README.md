# Myanmar Earthquake Open Data

A Python toolkit for collecting, analyzing, and visualizing earthquake data in Myanmar. Built on top of the [Myanmar Earthquake API](https://mmeq.akze.net), covering records from 1950 to present. Daily automated updates via GitHub Actions.

## Features

- **Data Pipeline** — Fetch earthquake data from the API month-by-month in parallel, validate, deduplicate, and export as monthly/yearly/combined CSV + JSON
- **CLI Interface** — Single `mmeq` command with subcommands for export, analysis, and visualization
- **Temporal Analysis** — Monthly frequency trends, magnitude distribution histograms, magnitude vs depth scatter plots
- **Spatial Clustering** — DBSCAN density-based clustering with depth-colored markers, magnitude-scaled sizes, annotated seismic zone labels, convex hull boundaries, and interactive basemap
- **Seismology Module** — b-value estimation (Gutenberg-Richter law), magnitude of completeness (Mc), temporal declustering (Gardner-Knopoff)
- **Interactive Map** — Folium-based HTML map with circle markers, marker clustering, heatmap layer, fault line overlay, and date range legends
- **Configurable** — All parameters configurable via `config.py` or environment variables (API URL, file paths, DBSCAN params, map center, etc.)
- **Tested** — 39 unit tests covering data validation, date range generation, CSV/JSON I/O, deduplication, seismology, and clustering
- **CI/CD** — GitHub Actions workflow runs daily at midnight UTC to fetch new data and auto-commit

## Requirements

- Python 3.9+
- See `requirements.txt` for all dependencies

## Installation

```bash
# Clone the repository
git clone https://github.com/your-username/mmeqopendata.git
cd mmeqopendata

# Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate        # Linux/macOS
# .venv\Scripts\activate         # Windows

# Install as an editable package with CLI
pip install -e .

# Or install with dev dependencies (for running tests)
pip install -e ".[dev]"
```

## Quick Start

```bash
# Fetch latest earthquake data from the API
mmeq export

# Run all analyses (temporal + clustering + seismology)
mmeq analyze --type all

# Generate interactive map with magnitude filter
mmeq visualize --min-mag 3.0

# Run seismology analysis with declustering
mmeq analyze --type seismology --decluster

# Show help
mmeq --help
mmeq analyze --help
mmeq visualize --help
```

## CLI Reference

### `mmeq export`

Fetches earthquake data from the Myanmar Earthquake API and exports it as CSV and JSON.

```bash
mmeq export                    # Fetch all pending months
mmeq export --workers 5        # Use 5 parallel threads (default: 10)
```

**Output structure:**
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

Runs analyses on the earthquake data and generates charts and statistics.

```bash
mmeq analyze --type all                          # Run everything
mmeq analyze --type temporal                      # Frequency trends, magnitude distribution, depth scatter
mmeq analyze --type clustering                    # DBSCAN spatial clustering with map
mmeq analyze --type seismology                    # b-value, magnitude of completeness
mmeq analyze --type seismology --decluster        # Also run temporal declustering
mmeq analyze --min-mag 4.0                        # Only analyze M4.0+ events
mmeq analyze --mc 3.5                             # Override magnitude of completeness
mmeq analyze --output ./my_results                # Custom output directory
mmeq analyze --data path/to/custom.csv            # Use a different data file
```

**Analysis types:**

| Type | Outputs | Description |
|---|---|---|
| `temporal` | `monthly_frequency_trends.png`, `magnitude_distribution.png`, `mag_vs_depth_relationship.png` | Time-series and distribution charts |
| `clustering` | `earthquake_clusters.png` | DBSCAN cluster map with depth-colored markers, zone annotations, 3 legends |
| `seismology` | Console summary | b-value (Gutenberg-Richter), magnitude of completeness, optional declustering |

**Seismology output example:**
```
Seismological Summary:
  b-value: 0.331
  a-value: 4.620
  Mc (completeness): 2.2
  Total events: 9,242
  Main shocks: 3,237 (after declustering)
  Removed (aftershocks): 6,005
```

### `mmeq visualize`

Generates an interactive Folium HTML map with earthquake markers, heatmap, and fault lines.

```bash
mmeq visualize                                  # All events, all layers
mmeq visualize --min-mag 4.0                    # Only M4.0+
mmeq visualize --no-heatmap                     # Disable heatmap layer
mmeq visualize --no-cluster                     # Disable marker clustering
mmeq visualize --no-markers                     # Disable circle markers
mmeq visualize --output ./maps                  # Custom output directory
```

**Map features:**
- Circle markers colored by magnitude (green < 3, orange 3–5, red 5+)
- Marker radius scaled by magnitude
- Marker clustering for performance (collapsible groups)
- Heatmap layer showing earthquake density
- Fault line overlay from `fault_lines.json`
- Dynamic legend with date range and magnitude scale
- Layer control to toggle features on/off

## Legacy Scripts

The original standalone scripts are still included and work without installing the package:

```bash
python dataexport.py        # Fetch and export data (same as mmeq export)
python advanalysis.py       # Temporal + clustering analysis
python adv2analysis.py      # Decade-by-decade clustering with fault lines
python visualizer.py        # Interactive Folium map
```

## Project Structure

```
mmeqopendata/
├── src/mmeq/                        # Main package
│   ├── __init__.py                  # Package metadata (version)
│   ├── cli.py                       # CLI entry point (argparse)
│   ├── config.py                    # All configuration constants + env var overrides
│   ├── export/
│   │   ├── __init__.py
│   │   ├── fetcher.py               # API fetching, date range generation
│   │   └── writer.py                # CSV/JSON save, validation, deduplication
│   ├── analysis/
│   │   ├── __init__.py
│   │   ├── clustering.py            # DBSCAN clustering + map plotting
│   │   ├── temporal.py              # Frequency, magnitude distribution, depth charts
│   │   └── seismology.py            # b-value, Mc, declustering
│   └── visualization/
│       ├── __init__.py
│       └── map.py                   # Folium interactive map builder
├── tests/
│   ├── test_validate.py             # Data validation tests (10 tests)
│   ├── test_dateranges.py           # Date ranges, CSV/JSON I/O, dedup (17 tests)
│   └── test_seismology.py           # b-value, Mc, declustering, DBSCAN (12 tests)
├── scripts/
│   └── run_cli.py                   # Thin entry-point script
├── .github/workflows/
│   └── daily_data_fetch.yml         # CI: daily data fetch + auto-commit
├── dataexport.py                    # Legacy standalone export script
├── advanalysis.py                   # Legacy standalone analysis script
├── adv2analysis.py                  # Legacy standalone clustering script
├── visualizer.py                    # Legacy standalone visualization script
├── fault_lines.json                 # Plate boundary data for map overlay
├── pyproject.toml                   # Package configuration
├── requirements.txt                 # Python dependencies
├── LICENSE                          # MIT License
└── README.md
```

## Configuration

All settings can be overridden via environment variables:

| Variable | Default | Description |
|---|---|---|
| `MMEQ_API_URL` | `https://mmeq.akze.net/api/myanmar-quakes` | API endpoint |
| `MMEQ_START_YEAR` | `1950` | Earliest year to fetch |
| `MMEQ_EXPORT_DIR` | `quake_exports` | Data output directory |
| `MMEQ_MAX_WORKERS` | `10` | Parallel download threads |
| `MMEQ_FAULT_LINES` | `fault_lines.json` | Fault line GeoJSON path |
| `MMEQ_OUTPUT_DIR` | `.` | Analysis/chart output directory |

## Data Fields

Each earthquake record contains:

| Field | Type | Description |
|---|---|---|
| `time_utc` | datetime | UTC timestamp |
| `time_mmt` | datetime | Myanmar Standard Time (UTC+6:30) |
| `latitude` | float | Latitude (-90 to 90) |
| `longitude` | float | Longitude (-180 to 180) |
| `depth` | float | Depth in km (0 to 700) |
| `mag` | float | Magnitude (0 to 10) |
| `magType` | string | Magnitude type (ml, mb, mw, mww, etc.) |
| `location` | string | Descriptive location name |
| `place` | string | Region/place |
| `country` | string | Country code (MM) |
| `id` | string | Unique event identifier |

## Cluster Map Legend

The clustering analysis produces a map with three legends:

**Depth (marker color):**
- Red — Shallow (0–30 km), most destructive
- Orange — Intermediate (30–70 km)
- Blue — Deep (70–300 km)
- Dark Blue — Very Deep (300+ km)

**Magnitude (marker size):** Larger dots = stronger earthquakes

**Seismic Zones (upper right):** Each cluster labeled with region name, event count, and maximum earthquake with date

## Running Tests

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ -v --cov=mmeq
```

## Automated Data Updates

A GitHub Actions workflow (`.github/workflows/daily_data_fetch.yml`) runs daily at midnight UTC to:
1. Fetch new earthquake data from the API
2. Update CSV and JSON exports
3. Auto-commit changes to the repository

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

## Data Source

Earthquake data is provided by the [Myanmar Earthquake API](https://mmeq.akze.net). Fault line data from global plate boundary datasets.

## Contact

For questions or issues, contact aungkhantzawd@gmail.com or open a GitHub issue.
