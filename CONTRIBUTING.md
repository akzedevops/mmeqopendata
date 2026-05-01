# Contributing

Thanks for your interest! This is a hobby project, but contributions are welcome.

## How to contribute

1. Fork the repo and create a branch from `master`
2. Make your changes
3. Run the tests: `pytest tests/ -v`
4. Run the full pipeline: `mmeq report --output /tmp/test`
5. Open a pull request

## What's useful

- **More OSM data** — better building classifications, missing schools/hospitals
- **Myanmar-specific fragility curves** — the current ones use generic HAZUS parameters
- **Burmese translations** — place names, UI text
- **Station data** — if you have access to Myanmar seismic station recordings
- **Bug reports** — open an issue with steps to reproduce

## Setup

```bash
git clone https://github.com/akzedevops/mmeqopendata.git
cd mmeqopendata
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest tests/ -v
```

## Code style

- Follow existing patterns — look at nearby code before writing new code
- Keep modules focused — one analysis per file in `src/mmeq/analysis/`
- Add docstrings to public functions
- No external API calls in tests

## Data

Large files (DEM tiles, full GEM faults, WorldPop raster) are gitignored. They download automatically when needed. Don't commit files over 5 MB without checking `.gitignore` first.
