---
spec: 001
title: Upgrade the earthquake data-fetch/export system
status: Done
author: Claude (research) + Aung Khant Zaw
created: 2026-06-30
---

> **Progress (2026-06-30):** R2 (CI unified on `mmeq export`), R3 (atomic writes +
> `mmeq export --rebuild`; live JSON reconciled 421→9,390), and R5 (hardened retries)
> shipped in PR #8. **R1 (bisection) shipped but was based on a false 500-cap premise —
> now disabled by default (it's a dormant tripwire); see the corrected Problem #1.**
> R11 (targeted sync of ~12 late month-boundary events) is the active task. Still open:
> R4 (manifest), R6 (contract gate), R7 (Parquet/DuckDB), R8 (CI rebase), R9 (freshness
> badge), R10 (weekly look-back — the right fix for the late-arriving boundary events).

## Problem / motivation

The fetch/export subsystem has correctness and completeness gaps uncovered by research
(3-agent audit + live API probes).

1. **~~The API silently caps at 500 records per request~~ — CORRECTION: there is no cap.**
   The original audit (and an early draft of this spec) claimed a 500-record cap based on
   a `WebFetch` probe that showed exactly 500 records. **That was a measurement artifact of
   the WebFetch tool's markdown conversion, not the API.** Verified with the real client
   (`requests`, which the pipeline actually uses): `GET ?from=1970-01-01&to=2026-06-30`
   returns the **full 9,402-record catalog in one response**; `?from=2025-01-01&to=2025-12-31`
   returns 2,469; the 2025-03 month returns 338. The API is uncapped and unpaginated.
   Consequence: the **R1 window-bisection is unnecessary** and is now **disabled by default**
   (`MMEQ_API_PAGE_CAP=0`), kept only as a dormant tripwire. The real completeness gap is
   small and is handled by R11 + R10 below.

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

- **R11 — Targeted sync of late/boundary events (active).** With the cap finding
  corrected, the real gap is small: comparing the full API catalog (9,402) to ours (9,390)
  shows **12 missing events**, all late-arriving end-of-month/recent events (e.g.
  `2025-05-31T20:31`, `2025-08-31`, `2026-02-28`, `2026-06-29/30`) — events that arrived
  after their month was last fetched and were never back-filled (the R10 problem), plus the
  current unfetched days. Fix: `tools/backfill.py` re-fetches the affected months and
  **merges (id-dedup, strictly additive)** into the monthly files, then `mmeq export
  --rebuild`. A heavy parallel/bisecting backfill is *not* needed (no cap); a targeted
  merge of the affected months suffices. The durable fix is **R10** (weekly trailing
  re-fetch) so late events land automatically.

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
- [x] **R11:** DONE (catalog 9,405 events ≥ 9,390 as of 2026-07-03; count re-verified at the spec-002 cutover) — original text: after the backfill + `--rebuild`, the catalog event count is ≥ 9,390 and
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
