---
spec: 002
title: Rewrite the fetch/export CLI in Go (single static binary) on a separate branch
status: Done            # Draft | Approved | In progress | Done
author: Claude (research) + Aung Khant Zaw
created: 2026-06-30
---

## Problem / motivation

The daily data-fetch CI installs the **entire scientific Python stack**
(`pip install -e ".[dev]"` + plotly/fpdf2 → pandas, numpy, scipy, sklearn, geopandas,
contextily, shapely, pyproj, matplotlib) just to run an HTTP-and-CSV job
(`python dataexport.py`). That is slow, flaky, and heavy for what is fundamentally an
I/O-bound fetch → validate → dedup → export task. A Go rewrite of *only that pipeline*
yields a single static binary with fast cold start, trivial concurrency, and zero Python
dependency for the daily cron — while the scientific analysis stays in Python where it
belongs.

This is a **separate-branch** effort (`feat/go-export-rewrite`) because it is large and
must not destabilize the live pipeline; it lands only after a shadow-run proves byte-parity.

## Goal

A `mmeq-export` Go binary that reproduces the Python export output **byte-for-byte**
(golden-file tested) and replaces `python dataexport.py` in `daily_data_fetch.yml`, with the
scientific analysis + figures + dashboard + paper untouched (they keep reading the CSV/JSON
the binary produces). The CSV/JSON artifact is the contract boundary: **Go produces it,
Python consumes it.**

## Non-goals

- **Do NOT port any scientific code.** All of `analysis/` (ASK08 GMPE, PSHA, DBSCAN,
  declustering, fragility, Coulomb, Monte-Carlo) and `visualization/` + `generate_figures.py`
  stay in Python. No mature Go equivalent for numpy/scipy/sklearn/geopandas/rasterio/
  matplotlib, and rewriting them risks silent numerical divergence in *published* claims —
  exactly what CLAUDE.md forbids.
- Not a rewrite of the CLI's `analyze/visualize/report` subcommands — only `export`.

## Design

Port classification (from the codebase map):

| Component | Port to Go? | Why |
|---|---|---|
| `export/fetcher.py`, `export/writer.py`, `config.py`, `cli.py:cmd_export`, `dataexport.py` | **PORT** | I/O-bound, concurrency, simple transforms, single-binary value |
| `analysis/geocoder.py` (point-in-polygon ADM1/2/3 + nearest OSM place + haversine; emits 6 published columns inside `validate_quake_data`) | **PORT (with care)** — or hybrid | Pure geometry, but fidelity-sensitive (see risks) |
| everything else in `analysis/` and `visualization/` | **KEEP (Python)** | SciPy-stack bound, peer-review-grade |

Recommended Go layout (separate module under `go/` or repo subdir):
```
cmd/mmeq-export/main.go         # cobra root + `export` subcommand
internal/config/                # MMEQ_* env parsing (mirror config.py)
internal/api/                   # retryablehttp client; fetch(from,to) -> []Quake
internal/catalog/               # model, validate, dedup, daterange, writer
internal/geocoder/              # admin polygons (orb) + nearest place (kd-tree) [optional]
testdata/                       # golden CSV/JSON from current Python output
```

Libraries (verified current, mid-2026): stdlib `net/http` + `hashicorp/go-retryablehttp`
(v0.7.8) for retries/backoff; stdlib `encoding/csv`,`encoding/json`; `spf13/cobra` for the
CLI; `golang.org/x/sync/errgroup` with `SetLimit(workers)` to mirror `ThreadPoolExecutor`;
`time.LoadLocation("Asia/Yangon")` + `time/tzdata` (embedded) for `time_mmt`; `paulmach/orb`
(planar polygon-contains) + a kd-tree (`kyroy/kdtree` or gonum) for the geocoder.

### Increment plan (updated 2026-07-03 — fetch route decided: v1-compat)

The mmeq API v2 (Go/SQLite, `github.com/akzedevops/mmeq-api`, private) is in production.
The exporter has BOTH clients, but **defaults to the v1-compat route**
(`MMEQ_FETCH_ROUTE=v1`): the typed `/api/v2/earthquakes` schema deliberately drops the
raw upstream columns (`dmin`, `gap`, `nst`, `rms`, `shakemapURL`, `continent`, …) that
make up the published 33-column artifact (later corrected: the shipped header is
**32** columns — `time` is dropped; see specs 004/005), and adds `ingested_at` — so a v2-fed export
can never be byte-compatible. Discovered 2026-07-03 by the first full-tree diff; the
same diff also caught a production bug where the v1-compat route alphabetized record
keys (Go map marshal), which would have silently flipped pandas' CSV column order —
fixed server-side (mmeq-api `9532801`, canonical `v1KeyOrder` re-serialization).
The 001 bisection tripwire still applies (`MMEQ_API_PAGE_CAP`, default off).

- [x] **I1** — `internal/config` (MMEQ_* env mirror) + `internal/catalog` model,
      `Validate`, `Dedup` (keep-last by id); unit-tested.
- [x] **I2** — `internal/api`: v2 client (`FetchWindow` paginated, `FetchUpdatedAfter`)
      **plus `FetchWindowV1`** (compat route, verbatim raw records); retries with
      backoff; httptest-tested.
- [x] **I3** — `internal/catalog` writer: monthly (overwrite) / yearly / combined
      (merge+dedup) CSV+JSON with **golden-file parity vs the Python writer** —
      fixtures generated by running the actual Python `save_to_csv`/`save_to_json`
      on the same synthetic input via the repo venv.
- [x] **I4** — full Go geocoder port (no hybrid needed): 44/44 golden-row parity.
- [x] **I5 (code)** — `internal/export.Run` (mirrors `cli.py:cmd_export` incl. the
      yearly-JSON overwrite quirk and relativedelta month stepping; the overwrite quirk was later FIXED on both sides — 2026-07-06 audit C5, yearly JSON now merges like the yearly CSV), `cmd/mmeq-export`
      CLI, `tools/diff_exports.py` (id-keyed, float-tolerant tree diff),
      `.github/workflows/shadow_go_export.yml` (daily 02:30 UTC full-2026 rebuild via
      both exporters + diff). **Local parity proven** against a seeded local API:
      12/16 files byte-identical, the other 4 identical after row sort (Python's
      `as_completed` row order is nondeterministic; order is not contract).
- [x] **I5 (gate)** — DONE 2026-07-03: three clean shadow cycles (the third on
      the export route) → `daily_data_fetch.yml` cut over to the Go binary;
      `dataexport.py` retired. The mmeq-api key-order fix deployed earlier the
      same day.

**Contract behaviors that must be reproduced exactly** (golden-file tested): validation
bounds (lat/lon/depth/mag) + NaN-drop; `time_utc` UTC `%Y-%m-%d %H:%M:%S` and `time_mmt` in
Asia/Yangon (UTC+06:30, via tzdb not a hardcoded offset); `id`-keyed dedup `keep="last"`;
combined-JSON merge keyed on id else `(time_utc,lat,lon)` preserving first-seen order;
monthly = overwrite (with per-window id dedup — added to BOTH implementations 2026-07-06 so the Python `validate_quake_data` matches Go's `Dedup(Validate(...))`), yearly/combined = merge+dedup; date-range generation from
`last_event + 1 day`; the 32-column CSV header/order (spec text originally said 33) ending in the 6 geocoder columns +
`shakemapURL/shakemapLastUpdated`; JSON `{"earthquakes":[...]}` indent=2, UTF-8 literal
(non-ASCII Myanmar names). **The Go port must also implement [[001-data-fetch-upgrade]]'s
500-cap bisection and contract gate** — so 001 should land first as the reference.

## Data & outputs impact

- No change to artifact *shapes* — the Go binary must emit identical CSV/JSON.
- `daily_data_fetch.yml` loses the `pip install` step and calls the Go binary.

## Acceptance criteria

- [x] `go test ./...` passes, including golden-file tests diffing Go output against
      snapshots of the current Python `mmeq export` output for several date ranges.
- [x] Byte-identical combined/monthly/yearly CSV+JSON vs Python on the same input (modulo a
      documented, golden-tested float-formatting rule).
- [x] Geocoder parity (full Go port, 44/44 golden rows): the 6 columns match Python on a fixture set (distance_km within a
      stated rounding tolerance), OR the hybrid Python post-step is wired and tested.
- [x] A **shadow CI job** runs the Go binary alongside Python for ≥3 daily cycles with clean
      diffs before any cutover.
- [x] After cutover, `daily_data_fetch.yml` has no Python dependency and the daily commit is
      unchanged in content.

## Risks / rollback

- **Timezone / time formatting** — parse as UTC, convert via embedded tzdb; verify the
  +06:30 half-hour offset survives. High-fidelity risk.
- **Float formatting** — pandas `to_csv` float repr vs Go `strconv.FormatFloat`; use
  shortest round-trip (`'g',-1,64`) and golden-test; cosmetic drift creates noisy daily git
  diffs and can break exact-match parsers.
- **Geocoder fidelity** — scipy `cKDTree` uses 3D-unit-sphere Euclidean nearest then
  haversine; shapely `STRtree.contains` boundary/multipolygon semantics must match orb.
  Ties and boundary points are where Go can pick a different place. Mitigation: hybrid
  fallback (Go writes pre-geocode CSV; a ~150-LOC Python step adds the 6 columns).
- **Permissive schema** — pandas auto-absorbs new API keys (`shakemapURL` appeared later); a
  fixed Go struct silently drops them. Use a struct + overflow `map[string]any`.
- **Porting the wrong reference** — `dataexport.py` and `src/mmeq/export/*` have drifted;
  port from the canonical package *after* [[003-project-improvements]] #3 unifies them.
- Rollback at any phase: revert `daily_data_fetch.yml` to `python dataexport.py`; the branch
  is isolated until cutover so the live pipeline is never at risk.

## Notes

Migration path: golden-snapshot Python output → build Go binary in parallel (not wired to
CI) → geocoder decision (full Go vs hybrid) → shadow CI diff job → cutover → deprecate
`dataexport.py`. The scientific pipeline never changes, insulating the public dashboard.
Sources: go-retryablehttp, paulmach/orb, spf13/cobra, gonum/kyroy kd-tree (see research).
