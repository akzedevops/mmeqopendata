# Contributing

Thanks for your interest! This is a hobby project, but it makes **published
scientific claims** (the paper and the live dashboard), so contributions are
very welcome — with a little more care than a typical toy repo. This guide gets
you from clone to merged PR.

## TL;DR

```bash
git clone https://github.com/akzedevops/mmeqopendata.git && cd mmeqopendata
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"          # Python 3.9+
ruff check .                     # lint (must be clean)
pytest tests/ -v                 # 109 tests (must pass)
(cd go && go test ./...)         # only if you touch go/
```

Then branch off `master`, make your change, keep lint + tests green, and open a PR.

## What this project is

A Python toolkit that fetches, validates, analyzes, and visualizes Myanmar
earthquake data (1950–present), plus a seismic-risk study of 254 dams. It is a
**scientific data pipeline, not a web app** — correctness of the seismology and
ground-motion math is the top priority. The published `quake_exports/` catalog
is an **open research dataset**.

- **Canonical code lives in `src/mmeq/`** and is wired through `src/mmeq/cli.py`
  (`export | analyze | visualize | report`). Add features there.
- **`go/`** is a byte-parity Go rewrite of the *export pipeline only* (the daily
  CI cron runs it). Touch it only if you're changing export/fetch/write behavior,
  and keep `go test ./...` green.
- The full layout and the domain-correctness rules are in
  [`CLAUDE.md`](CLAUDE.md) — read that too; it's the source of truth for how the
  code is organized and what must not break.

## Dev setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"          # editable install + pytest + ruff
```

Optional/heavy deps (`geopandas`, `contextily`) and network stages are wrapped
in `try/except` so the pipeline degrades gracefully — **preserve that pattern**;
don't make a heavy import mandatory.

## The dev loop

1. **Branch** off `master` (`git switch -c fix/short-slug`). Direct pushes to
   `master` are not accepted — everything goes through a PR.
2. **Make the change** in `src/mmeq/` (and `go/` if it's export behavior).
3. **Lint:** `ruff check .` — must be clean (CI enforces it).
4. **Test:** `pytest tests/ -v` — must pass; add a test for any new analysis
   function. `(cd go && go vet ./... && go test ./...)` if you touched `go/`.
5. **Run the affected stage in isolation** and sanity-check the printed numbers,
   e.g. `mmeq report --no-pdf --no-animated --output /tmp/t` (there's a `--no-*`
   flag for every stage). Don't trust "tests pass" alone for science changes — a
   logically-wrong analysis can still be green.
6. **Open a PR.** CI runs ruff + pytest + `go test` + CodeQL + an automated review
   on every PR. Keep them green.

## Non-trivial changes are spec-first

Anything beyond a small fix is developed **spec-first**: write a short
`specs/NNN-slug.md` (problem → design → acceptance criteria) *before* the code,
then implement → test → review until every criterion is checked. See
[`specs/README.md`](specs/README.md). Specs 001–009 are the record of every
substantial change — read the relevant one before touching that subsystem.
(Claude Code users: the `/spec` slash command scaffolds this.)

## Domain correctness — read before touching `analysis/`

Bugs here are silent and end up in the paper. The load-bearing rules:

- **ASK08 GMPE** (`analysis/dam_risk.py`) — Abrahamson & Silva (2008) NGA-West1
  coefficients, verified against OpenQuake to 0.04%. It uses **rupture distance**,
  not epicentral. Don't "simplify" the distance metric.
- **The 2025 scenario is the M7.7 Sagaing event** — the catalog has *two* M7.7s
  (1988 Lancang, 2025 Sagaing); select the scenario via
  `seismology.select_scenario_event` (ties broken by recency), never a bare
  `nlargest`/`idxmax`.
- **PSHA** is smoothed-seismicity (Frankel 1995) over the declustered catalog; it
  has no fault-source term (no slip-rate data) and is documented as such. The
  Coulomb kernel is a parity-correct point-source approximation, not full Okada.
- **Geographic distance** must apply `cos(lat)` or run in projected UTM (Zone 47N
  / EPSG:32647). DBSCAN clustering runs in UTM so distances are metric.
- **Derived numbers move together.** If your change alters an event count,
  b-value, dam grade, or PGA range, update `README.md`, `paper/main.tex`, **and**
  the figures in the *same* PR. CI regenerates figures on merge to `master`.
- When unsure about a library API (geopandas, scipy, sklearn, folium, plotly),
  check current docs rather than memory.

## The published catalog is a research dataset

- **Keep all 32 columns.** Don't trim the artifact — spec 005 (a column-trim
  proposal) was **rejected** because the columns have research value.
- CSV headers are **per-file and not all in the same order** (a known quirk).
  Parse by **column name, never by position**.
- The catalog is a **multi-network union** (~62% Thai `th_`, ~22% USGS `us`, plus
  `in_`/`ems`); the same quake can appear under different ids with epicenters
  tens of km apart. If you reconcile against another catalog, **dedup
  spatio-temporally** (same event within ~35 s AND ~250 km), never by id alone.
  `tools/backfill_usgs.py` is the reference implementation.
- Backfilled/derived data must go through the normal `validate_quake_data` path
  and document its provenance (see `specs/007`–`008` and
  `specs/008-backfill-manifest.csv`).

## Conventions

- Config: all tunables in `src/mmeq/config.py`, overridable via `MMEQ_*` env vars.
  Add new knobs there, not as scattered literals.
- Logging: stdlib `logging` (`-v` = DEBUG). `print` is for user-facing CLI
  summaries only, never diagnostics.
- Style: match nearby code; one analysis per file in `analysis/`; docstrings on
  public functions; **no network calls in tests**.
- Don't commit files > 5 MB without checking `.gitignore` (DEM tiles, the full
  GEM fault set, and the WorldPop raster are intentionally gitignored and
  download on demand).

## Good first contributions

- **Myanmar-specific dam fragility curves** — the current ones use generic HAZUS
  parameters, and every dam is currently treated as earthfill (no construction-
  type data). Real curves would be high-impact.
- **A fault-source PSHA term** using Sagaing Fault slip rates — the biggest known
  limitation; would turn the catalog-only lower bound into a full hazard model.
- **Magnitude homogenization** (mb/ml → Mw) before b-value/moment work.
- **More/better OSM exposure data**, Burmese place-name translations, additional
  seismic-station recordings for GMPE validation, bug reports with repro steps.

## Reporting bugs / data issues

Open an issue with steps to reproduce. For a suspected *scientific* error, include
the exact command, the numbers you got, and what you expected with a citation —
that's the fastest path to a fix.
