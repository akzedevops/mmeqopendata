---
spec: 001
title: Upgrade the earthquake data-fetch/export system
status: In progress      # Draft | Approved | In progress | Done
author: Claude (research) + Aung Khant Zaw
created: 2026-06-30
---

> **Progress (2026-06-30):** R1 (bisection), R2 (CI unified on `mmeq export`),
> R3 (atomic writes + `mmeq export --rebuild`; live JSON reconciled 421→9,390),
> and R5 (hardened retries) shipped in PR #8. **R11 (one-time backfill, below) is
> the active task.** Still open: R4 (manifest), R6 (full contract gate), R7
> (Parquet/DuckDB), R8 (CI rebase safety), R9 (freshness badge), R10 (weekly look-back).

## Problem / motivation

The fetch/export subsystem has correctness and completeness gaps that were uncovered
by research (3-agent audit + live API probes). Two are **verified and serious**:

1. **The API silently caps at 500 records per request, newest-first, with NO pagination.**
   Probed live: `GET /api/myanmar-quakes?from=2025-03-01&to=2025-03-31` returns *exactly*
   500 records, earliest `2025-03-29` — i.e. **the 2025-03-28 M7.7 mainshock and three
   weeks of its aftershocks are unreachable in a single monthly window.** The response is a
   bare `{"earthquakes": [...]}` — no `total`/`limit`/`offset`/cursor/`next`, no
   ETag/If-Modified-Since. The current month-window strategy only produced a complete
   catalog because it accreted over hundreds of daily runs while each day stayed < 500.
   **A full rebuild from the API today would truncate every >500-event window** — the
   project's headline event would be lost. Disaster recovery is effectively broken.

2. **The combined JSON is desynced from the CSV: 421 records vs 9,390** (verified on disk).
   `merge_combined_json` accumulates against the existing JSON using only *this run's*
   fetched months, while the CSV accumulates correctly; the two drifted and nothing
   reconciles. The public JSON artifact is missing ~95% of events.

Additional issues (from code reading): no atomic writes (in-place writes can corrupt the
canonical state store on a crash), the combined CSV *is* the implicit state store (no
manifest/high-water-mark file), thin retry config (no 429 handling, no jitter, no rate
limit across the 10 concurrent workers), no response-schema/contract validation, no
"window returned exactly the cap → suspect truncation" detection, past-month revisions are
never re-fetched, and `dataexport.py` (run by CI) is a drifting duplicate of
`src/mmeq/export/*` (see [[003-project-improvements]]).

## Goal

A fetch/export system that (a) returns *complete* data for any window regardless of the
500-cap, (b) keeps CSV and JSON in sync, (c) writes atomically and tracks state
explicitly, and (d) fails loudly on truncation/contract violations instead of silently
shipping incomplete data to the public dashboard.

## Non-goals

- The Go rewrite (that is [[002-go-export-rewrite]]; this spec is Python-side and must land
  first so the Go port has a correct, tested reference).
- Changing the analysis/figure pipeline.
- Changing the upstream API (we don't control mmeq.akze.net).

## Design

Changes land in `src/mmeq/export/{fetcher,writer}.py`, `src/mmeq/config.py`,
`src/mmeq/cli.py`, and the two workflows. `dataexport.py` is retired in favour of
`mmeq export` (R2) so fixes actually reach production.

- **R1 — Adaptive window bisection (the key fix).** In `fetcher.py`, when a window returns
  `>= MMEQ_API_PAGE_CAP` (default 500) records, recursively split the date window
  (month → days → hours) and union the de-duplicated results until every sub-window is
  under the cap. New config `MMEQ_API_PAGE_CAP=500`. This is the only way to get complete
  data for active sequences and to make full rebuild correct.
- **R3 — Atomic writes + JSON reconcile.** All writers write `*.tmp` then `os.replace(...)`.
  Add `mmeq export --rebuild` that regenerates the combined JSON (and re-sorts the combined
  CSV by `time_utc`) from the full set of monthly files, fixing the 421-vs-9,390 drift.
- **R4 — Explicit state manifest.** Write `quake_exports/_manifest.json`:
  `{last_event_utc, last_run_utc, total_events, per_month_counts, cap_hit_windows[]}`.
  `get_last_updated_date()` reads it (falling back to the CSV scan). Underpins R1 re-slicing
  and the freshness badge.
- **R5 — Harden HTTP.** Add `429` to `status_forcelist`, `respect_retry_after_header=True`,
  separate connect/read retries, backoff jitter, and a small inter-request delay or lower
  default `MAX_WORKERS` so concurrent workers don't trip rate limits.
- **R6 — Contract + data-quality gate.** Validate required fields per record
  (`id, time, latitude, longitude, depth, mag`); after a run, assert no month shrank
  unexpectedly and log dedup/per-window counts; treat `len == cap` as "truncated → bisect"
  (ties to R1) and a contract violation as a non-zero CI exit with a clear message.
- **R2 — Unify CI on the package.** Point `daily_data_fetch.yml` at `mmeq export`; reduce
  `dataexport.py` to a shim or delete it. (Shared with [[003-project-improvements]] #3.)
- **R8 — CI push safety.** Add a `concurrency:` group to `daily_data_fetch.yml` and
  `git pull --rebase origin master` before push in both workflows (two workflows push to
  master today with no rebase → non-fast-forward risk).
- **R10 — Weekly look-back.** Once a week, re-fetch the trailing ~90 days so revised
  magnitudes/locations land via the existing `id`-keyed `keep="last"` dedup.

- **R11 — One-time bisecting backfill (active).** The fixes above only make *future*
  fetches complete; events the API truncated during past dense windows (the daily fetch
  had no bisection then) are still missing from the catalog. Re-fetch the dense historical
  windows — the 2022-06..08 and 2025-03..05 sequences, plus recent months — with
  `fetch_quake_data_complete`, overwrite their monthly CSVs, then `mmeq export --rebuild`.
  If the event count rises, regenerate figures/README/paper (CI does this on push). Sparse
  pre-2020 months (< ~300 events/month) never hit the cap, so the backfill targets the
  instrumented-dense era. Run via a parallel agent fan-out over disjoint month ranges
  (monthly files are per-month, so concurrent writes don't collide).

Deferred / optional: **R7** (Parquet/DuckDB combined catalog as the analytical
source-of-truth — a duckdb MCP already exists here) and **R9** (freshness badge + CI
failure alerting). Capture as follow-ups, not blockers.

## Data & outputs impact

- Input: same API; new `quake_exports/_manifest.json`.
- Artifacts: combined JSON repopulated to ~9,390 records; combined CSV sorted by time;
  monthly/yearly unchanged in shape. Re-sorting the combined CSV is a one-time large diff.
- Derived numbers: a corrected full rebuild may *add* previously-truncated events
  (e.g. early-March-2025 aftershocks) → b-value, counts, and dam/PSHA numbers in README +
  paper must be re-verified and regenerated together (CI already does figures/paper).

## Acceptance criteria

- [x] New `tests/test_fetcher.py` case: a stubbed API returning 500 for a month and < 500
      for sub-windows yields the *union* (bisection recovers all records). *(PR #8)*
- [x] `mmeq export --rebuild` produces a combined JSON whose record count equals the
      combined CSV row count (was 421 vs 9,390 → now 9,390 = 9,390). *(PR #8)*
- [x] Atomic-write test: a successful write leaves no temp file; writes via tmp+replace. *(PR #8)*
- [x] A window returning exactly the cap is bisected (asserted in test). *(PR #8)*
- [x] `daily_data_fetch.yml` runs `mmeq export`; `dataexport.py` reduced to a shim. *(PR #8)*
- [ ] **R11:** after the backfill + `--rebuild`, the catalog event count is ≥ 9,390 and
      any recovered events are reflected in regenerated figures/README/paper.
- [x] No regression: full `pytest tests/ -v` green (56 tests). *(PR #8)*

## Risks / rollback

- R1 bisection could multiply API calls on dense windows — bound recursion depth and add
  rate limiting (R5). Risk: low.
- Re-sorting/rebuilding combined files is a big one-time diff and changes the public JSON;
  do it in one reviewed commit. Rollback = revert the commit (history preserves old files).
- Migrating CI off `dataexport.py` changes the production path — gate behind the new
  pytest CI job ([[003-project-improvements]] #1) and a shadow run before cutover.

## Notes

API probes (2026-06-30): `2025-03-01..03-31` → 500 recs, earliest 2025-03-29;
`2025-03-28..04-30` → 500 recs, 04-19..04-30 only; quiet windows return true counts.
Only top-level key is `earthquakes`. Combined JSON on disk: 421; CSV: 9,390.
