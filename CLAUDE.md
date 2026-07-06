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
├── export/               # fetcher.py (API + date ranges), writer.py (CSV/JSON, validate,
│                         #   dedup), pipeline.py (run_export orchestration)
├── analysis/             # seismology, dam_risk (GMPE/Vs30/PSHA), clustering, aftershock,
│                         #   coulomb, fragility, finite_fault, gem_faults, osm_exposure,
│                         #   population, shakemap_validation, temporal, geocoder
└── visualization/        # map (Folium), dashboard (Plotly), cross_section (3D),
                          #   animated_map, report (PDF), assets.py + vendor/ (self-hosted
                          #   JS/CSS — see tools/fetch_vendor.py + vendor_lock.json)

go/                       # Go rewrite of the export pipeline ONLY (specs/002+004):
                          #   cmd/mmeq-export + internal/{api,catalog,config,export,geocoder};
                          #   golden-file tested for byte-parity with the Python writer —
                          #   run `cd go && go test ./...` when touching it
tests/                    # pytest suite (94 tests): validation, dateranges, seismology,
                          #   writer/fetcher, dam-risk scoring, vendored assets
tools/                    # diff_exports.py (Python-vs-Go tree diff), backfill.py,
                          #   build_figure_data.py, fetch_vendor.py (+ vendor_lock.json)
data/                     # input datasets: admin boundaries, shakemap, osm, faults, finite_fault
quake_exports/            # generated earthquake CSV/JSON (csv|json × monthly|yearly|combined)
docs/                     # GitHub Pages site + generated report/ artifacts
paper/                    # LaTeX write-up (main.tex) + figures/ (14 figs, PDF+PNG)
.github/workflows/        # daily_data_fetch.yml, report_and_pages.yml, tests.yml,
                          #   shadow_go_export.yml (nightly Python-vs-Go parity diff)
```

The upstream API service lives in a separate PRIVATE repo (`akzedevops/mmeq-api`,
checked out at `~/mmeq-api`); its interactive docs are at https://mmeq.akze.net/docs.
The export pipeline fetches `/api/v2/export` (full-fidelity raw records — see
`specs/004`); the typed `/api/v2/earthquakes` route deliberately drops legacy columns
and CANNOT reproduce the published 32-column artifact.

### Live root scripts

The old monolithic analysis scripts (`advanalysis.py`, `adv2analysis.py`, `visualizer.py`)
were removed — `src/mmeq/` supersedes them (see git history if needed). One root script
remains live (`dataexport.py` was retired at the spec-002 cutover — the daily CI
now runs the Go `mmeq-export` binary; `mmeq export` remains for local use):

- `generate_figures.py` — generates the 14 paper figures; it **imports `src.mmeq`** and
  is the canonical figure generator (`python generate_figures.py`).

When adding features, put them in `src/mmeq/` and wire them through `cli.py`.

## Commands

```bash
pip install -e ".[dev]"          # setup (Python 3.9+); includes pytest + ruff
ruff check .                     # lint — must be clean (enforced in CI)
pytest tests/ -v                 # run the test suite — do this before/after any change
(cd go && go test ./...)         # Go exporter suite — mandatory when touching go/
mmeq export                      # fetch & export earthquake data (network); the CI cron runs this
mmeq export --rebuild            # reconcile combined CSV+JSON from monthly files, sort by time
mmeq analyze --type all          # analyses only
mmeq visualize --min-mag 3.0     # interactive Folium map
mmeq report --output ./report    # full pipeline: all analyses + all outputs
python generate_figures.py       # regenerate paper figures
```

The **daily CI runs the Go exporter** (`go/cmd/mmeq-export`, `/api/v2/export` route);
the Python `mmeq export` remains for local use and fetches the same paginated
`/api/v2/export` route when `MMEQ_API_V2_URL` is set (legacy v1 fallback keeps the
500-record-cap bisection). Writes are atomic in both implementations. `mmeq report` has `--no-*` flags for every stage (`--no-pdf`,
`--no-dams`, `--no-coulomb`, `--no-montecarlo`, …) — use them to run a single stage fast.

**CI gates on tests:** `report_and_pages.yml`'s deploy `needs:` a `test` job (ruff +
pytest), and `tests.yml` runs on every PR/push — keep both green or the dashboard won't
deploy. Specs 001–004 are Done; the active roadmap is `specs/005` (artifact schema v2, Draft).

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
   `generate_figures.py`, `tools/build_figure_data.py`, `paper/main.tex`, or `pyproject.toml`. The `test`
   gate must pass before anything ships to the public dashboard — but a logically-wrong
   analysis change that still passes tests will deploy, so be careful on `master`.

## Spec-driven workflow

Non-trivial features are developed spec-first. See `specs/README.md`. Start a feature with
the `/spec` command, which drafts a spec under `specs/`, then implements → tests → reviews
in a loop until acceptance criteria pass.
</content>
</invoke>
