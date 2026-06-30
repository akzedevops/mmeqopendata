# CLAUDE.md

Guidance for Claude Code when working in this repository.

## What this is

**Myanmar Earthquake Open Data** (`mmeq-opendata`, v2.0.0) — a Python toolkit that
fetches, validates, analyzes, and visualizes earthquake data for Myanmar (1950–present)
from the [Myanmar Earthquake API](https://mmeq.akze.net), plus a seismic-risk study of
254 Myanmar dams. Outputs auto-deploy to GitHub Pages.

It is a **scientific data pipeline**, not a web app. Correctness of the seismology and
ground-motion math matters more than anything else — see "Domain correctness" below.

## Layout

```
src/mmeq/                 # ← canonical v2 package (edit code here)
├── cli.py                # argparse entry point: export | analyze | visualize | report
├── config.py             # all constants + env-var overrides (MMEQ_*)
├── export/               # fetcher.py (API + date ranges), writer.py (CSV/JSON, validate, dedup)
├── analysis/             # seismology, dam_risk (GMPE/Vs30/PSHA), clustering, aftershock,
│                         #   coulomb, fragility, finite_fault, gem_faults, osm_exposure,
│                         #   population[_raster], shakemap_validation, temporal, geocoder
└── visualization/        # map (Folium), dashboard (Plotly), cross_section (3D),
                          #   animated_map, report (PDF)

tests/                    # pytest: test_validate, test_dateranges, test_seismology (39 tests)
data/                     # input datasets: admin boundaries, shakemap, osm, faults, finite_fault
quake_exports/            # generated earthquake CSV/JSON (csv|json × monthly|yearly|combined)
docs/                     # GitHub Pages site + generated report/ artifacts
paper/                    # LaTeX write-up (main.tex) + figures/ (13 figs, PDF+PNG)
.github/workflows/        # daily_data_fetch.yml, report_and_pages.yml
```

### Live root scripts

The old monolithic analysis scripts (`advanalysis.py`, `adv2analysis.py`, `visualizer.py`)
were removed — `src/mmeq/` supersedes them (see git history if needed). Two root scripts
remain live:

- `dataexport.py` — invoked by the **daily fetch CI** (`daily_data_fetch.yml`). If you
  change export logic in `src/mmeq/export/`, keep this in sync or migrate the workflow.
  (Slated for retirement once CI runs `mmeq export` — see `specs/003`.)
- `generate_figures.py` — generates the 14 paper figures; it **imports `src.mmeq`** and
  is the canonical figure generator (`python generate_figures.py`).

When adding features, put them in `src/mmeq/` and wire them through `cli.py`.

## Commands

```bash
pip install -e ".[dev]"          # setup (Python 3.9+); includes pytest + ruff
ruff check .                     # lint — must be clean (enforced in CI)
pytest tests/ -v                 # run the test suite — do this before/after any change
mmeq export                      # fetch & export earthquake data (network); the CI cron runs this
mmeq export --rebuild            # reconcile combined CSV+JSON from monthly files, sort by time
mmeq analyze --type all          # analyses only
mmeq visualize --min-mag 3.0     # interactive Folium map
mmeq report --output ./report    # full pipeline: all analyses + all outputs
python generate_figures.py       # regenerate paper figures
```

`mmeq export` fetches month-by-month and **bisects any window that hits the API's
500-record cap** (the API is newest-first with no pagination) so data stays complete;
writes are atomic. `mmeq report` has `--no-*` flags for every stage (`--no-pdf`,
`--no-dams`, `--no-coulomb`, `--no-montecarlo`, …) — use them to run a single stage fast.

**CI gates on tests:** `report_and_pages.yml`'s deploy `needs:` a `test` job (ruff +
pytest), and `tests.yml` runs on every PR/push — keep both green or the dashboard won't
deploy. See `specs/001` (data-fetch) and `specs/003` (improvements) for the active roadmap.

## Conventions

- Config lives in `src/mmeq/config.py`; everything is overridable via `MMEQ_*` env vars.
  Add new tunables there, not as literals scattered in modules.
- Logging via the stdlib `logging` module (`-v` for DEBUG). No `print` for diagnostics;
  `print` is only for user-facing CLI summaries.
- Optional/heavy deps (`geopandas`, `contextily`) and network stages are wrapped in
  `try/except` so the pipeline degrades gracefully — preserve that pattern.
- Data flows as pandas DataFrames with stable column names: `time_utc`, `mag`, `depth`,
  `lat`/`latitude`, `lon`/`longitude`, `location`. Validation/dedup happens in
  `export/writer.py`.

## Domain correctness (read before touching analysis/)

This code makes scientific claims that are published in `paper/` and on the live
dashboard. Bugs here are silent and serious. v2.0.0 fixed a critically broken GMPE
(see `CHANGELOG.md`). Therefore:

- The **ASK08 GMPE** (`analysis/dam_risk.py`) uses Abrahamson & Silva (2008) NGA-West1
  coefficients and **rupture distance** (not epicentral). Do not "simplify" the distance
  metric — the 2025 M7.7 rupture ran ~475 km along the Sagaing Fault, so it matters.
- **Vs30** is a Wald & Allen (2007) slope proxy; PSHA uses Cornell-McGuire integration.
  Cite the source for any new ground-motion/site term.
- Geographic distances must apply `cos(lat)` correction or be done in projected UTM
  (Zone 47N / EPSG:32647); DBSCAN clustering runs in UTM so distances are metric.
- The README tables (event counts, b-value, dam risk grades, PGA ranges) are derived
  numbers — if your change alters them, update README.md, the paper, and figures together.
- When in doubt about a library API (geopandas, scipy, scikit-learn, folium, plotly),
  fetch current docs via Context7 rather than relying on memory.

## Verifying changes

1. `ruff check .` and `pytest tests/ -v` must stay green; add tests for new analysis
   functions. CI now gates deploy on both, but run them locally first.
2. Run the affected stage in isolation (e.g. `mmeq report --no-pdf --no-animated …`)
   and sanity-check the printed summary numbers against expected ranges.
3. CI (`report_and_pages.yml`) runs `test` → `build` (regenerates figures + paper) →
   `deploy` on push to `master` touching `quake_exports/**`, `src/mmeq/**`, `data/**`,
   `generate_figures.py`, `tools/**`, `paper/main.tex`, or `pyproject.toml`. The `test`
   gate must pass before anything ships to the public dashboard — but a logically-wrong
   analysis change that still passes tests will deploy, so be careful on `master`.

## Spec-driven workflow

Non-trivial features are developed spec-first. See `specs/README.md`. Start a feature with
the `/spec` command, which drafts a spec under `specs/`, then implements → tests → reviews
in a loop until acceptance criteria pass.
</content>
</invoke>
