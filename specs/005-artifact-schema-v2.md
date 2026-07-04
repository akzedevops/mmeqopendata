---
spec: 005
title: A leaner published artifact schema ("artifact v2") for quake_exports CSV/JSON
status: Draft            # Draft | Approved | In progress | Done
author: Claude (3-agent audit fan-out) + Aung Khant Zaw
created: 2026-07-03
---

## Decision required (owner sign-off before any implementation)

This is a **design-only draft**. It ships nothing. Three choices must be made first;
each has a recommendation with its reasoning below.

- **D1 — Column set.** How aggressively to trim the 32-column artifact.
  **RECOMMEND: the 24-column "lean" set** — drop only the 8 provably
  zero-information columns (`country`, `continent`, `type`, `state`, `timestamp`,
  `initialPosition`, `shakemapURL`, `shakemapLastUpdated`); keep the 4
  sparse-but-real historical science metrics (`nst`, `gap`, `dmin`, `rms`) and the
  6 geocoder columns. A 18-column "aggressive" alternative (also drop the 6
  recomputable geocoder columns) is documented — pick lean unless the owner judges
  the human-readable Myanmar admin names to be re-derivable-on-demand noise. **Do
  NOT add `ingested_at`** (recommendation below).
- **D2 — Compatibility mechanism.** How external consumers are protected.
  **RECOMMEND: in-place rewrite on `master` + MAJOR version bump (3.0.0)**, with a
  `git tag artifact-v1-final` on the pre-rewrite commit so any consumer pinned to a
  raw-file URL can re-pin to the tag and keep the 32-column shape indefinitely, plus
  a dated deprecation notice in README/dashboard/CHANGELOG. Rejected: a parallel
  `quake_exports/v2/` tree (permanently doubles a 1,278-file directory for a
  hypothetical consumer). See "Design".
- **D3 — Timing.** When to flip the default. **RECOMMEND: independent of any
  deadline** — unlike spec 004 there is no self-healing fetch-window clock here;
  the migration is a pure column projection over files already on disk, runnable any
  time. Land behind a default-off flag first, flip in the lockstep docs increment.

Status stays **Draft** until D1–D3 are chosen.

## Problem / motivation

The specs 001/002/004 work stabilized the *transport* (both exporters now fetch
`/api/v2/export`, byte-parity proven, golden- and shadow-pinned) but deliberately
left the *artifact shape* untouched: 32 columns, unchanged since the Node-v1 era.
Spec 004's own audit (its "Research findings") flagged the cleanup as a future spec
and explicitly scoped it out of the transport migration (004 §"Phase 2"). This spec
is that Phase 2, drafted to be decision-ready — not to be bundled with anything.

The 32-column header carries a large dead payload. A 3-agent audit of the ~9,410-row (2026-07-03)
combined catalog (`quake_exports/csv/combined/earthquakes_combined.csv`, verified in
this repo) found:

- **7 columns** carry the entire scientific + identity signal actually consumed by
  code: `latitude`, `longitude`, `depth`, `mag`, `time_utc`, `time_mmt`, `id`
  (confirmed: a grep of `src/mmeq/**` column-subscripts reads only these plus the
  `lat`/`lon` aliases — nothing reads `magType`, `net`, `place`, `location`, the
  geocoder columns, or the sparse metrics).
- **8 columns carry zero information**: `country`≡"MM", `continent`≡"Asia",
  `type`≡"earthquake" (constants); `state` (100% empty); `timestamp` (an exact
  epoch copy of `time_utc`); `initialPosition` (rounded `lat,lon`);
  `shakemapURL`/`shakemapLastUpdated` (populated on exactly 1 of ~9,410 events).
- **4 columns are sparse but genuinely real**: `nst`, `gap`, `dmin`, `rms` — 3–21%
  filled, historical USGS-sourced rows only. **The upstream source no longer supplies
  them for new events** (newest rows 0% filled), so they are frozen historical
  science that can never be regenerated once dropped.
- **6 geocoder columns** (`state_region`, `district`, `township`, `nearest_city`,
  `place_type`, `distance_km`) are **produced-but-unread**: written by
  `src/mmeq/analysis/geocoder.py:161-166`, read by no analysis/figure/test/dashboard
  code. They are recomputable (re-run the geocoder over the OSM place dataset) but
  are the only human-readable Myanmar admin context in the artifact.

So 14 of 32 columns are droppable in principle, 8 of them losslessly. The cost of the
status quo is not correctness — it is noise: every daily diff, every golden fixture,
every consumer's parser carries 8 columns that mean nothing and 6 that no code reads.

## Research findings (2026-07-03, three-agent audit; re-verified against the repo)

**Column value / consumer audit** — as summarized above; the read-set was reconfirmed
by grep over `src/mmeq/**`, `tests/**`, `generate_figures.py`.

**The paper does NOT depend on the artifact schema.** `grep` over `paper/main.tex`
finds no reference to `quake_exports`, `.csv`, column names, or column order — figures
are generated from in-memory DataFrames via `generate_figures.py`/`src.mmeq`, not by
parsing the published CSVs. **The migration cannot affect the paper.** (This is the
single biggest de-risker versus a naive assumption.)

**The dashboard links to files, does not parse columns positionally.**
`docs/index.html:167-168` and `:217` build **download** URLs to
`quake_exports/csv|json/combined/earthquakes_combined.*`; no JS indexes CSV columns.
A schema change alters the *contents* those links serve, not the links themselves.

**Header order is already per-file and non-canonical.** The combined CSV header
(`…initialPosition,time_utc,time_mmt,<geocoder×6>,shakemapURL,shakemapLastUpdated`)
places the shakemap pair **last** — a different order than
`catalog.DefaultColumns` (`…initialPosition,shakemapURL,shakemapLastUpdated,time_utc,
time_mmt,<geocoder×6>`). This is expected (spec 004 §Problem documented headers as
per-file artifacts). It matters here only in that the migration must project each
file **onto its own kept-columns-in-existing-order**, not force a global reorder — a
reorder would touch every never-refetched historical file gratuitously.

**Dropping a column requires an explicit projection, not a list edit.**
`go/internal/catalog/writer.go` (comment at :30-33, `ColumnsFor`/`normalizeColumns`):
`DefaultColumns` controls *order only*; **any key not in the list is appended in
sorted order**. Removing a name from `DefaultColumns` therefore does NOT drop the
column — it reappears, sorted, at the end. Both writers need a new
**allow-list / drop-set projection step** applied to the record before
ordering. This is the core new mechanism this spec introduces; everything else is
data rewriting.

**`ingested_at` exists upstream but is deliberately excluded.** The v2 API record
carries `ingested_at` (`go/internal/api/client.go:107`, `client_test.go:52`); it is
absent from today's 32-column artifact (not in `DefaultColumns`, so already dropped).
Adding it would inject a **per-fetch-varying** value into row bytes, defeating golden
determinism and generating daily-diff churn for zero current consumer. **Recommend
leaving it out**; provenance, if ever wanted, belongs in the `/api/v2/export`
`meta` envelope, not the row.

## Design — the chosen scenario

### Schema (D1) — RECOMMENDED "lean" 24-column set

Drop the 8 zero-information columns; keep everything with any information content:

```
KEEP (24):  latitude, longitude, depth, mag, magType, nst, gap, dmin, rms, net,
            id, updated, location, place, timeAdded, locationInferred,
            time_utc, time_mmt,
            state_region, district, township, nearest_city, place_type, distance_km
DROP (8):   country, continent, type, state, timestamp, initialPosition,
            shakemapURL, shakemapLastUpdated
```

Rationale per drop: `country`/`continent`/`type` are single-valued constants
trivially re-added by any consumer; `state` is 100% empty; `timestamp` duplicates
`time_utc` byte-for-byte; `initialPosition` duplicates rounded `lat,lon`;
the shakemap pair is degenerate (1 event in the whole catalog) and its one URL is reconstructible. Nothing
in the drop set is read by any code, and none of it can carry future signal (the
constants can't vary; `state` was never populated; the shakemap fields are not
supplied by the current upstream). **The 4 sparse metrics are KEPT** precisely
because they are the one class of *irreplaceable* data in the table.

**Alternative "aggressive" 18-column set** (owner's call under D1): additionally drop
the 6 geocoder columns. Defensible only if the owner treats Myanmar admin names as
re-derivable-on-demand and unwanted in the flat file. Not recommended: they are the
only geographic context a non-technical open-data user gets, cost little, and are
deterministic (no diff churn). If chosen, the geocoder enrichment step should also be
skipped at export time (not merely projected away) to save compute.

**Column order within the kept set:** each file keeps its *own surviving columns in
their existing relative order* (a pure projection/deletion — see the per-file-header
finding). New fetches emit the kept subset of `DefaultColumns` order. No global
reorder of historical files.

### Compatibility (D2) — RECOMMENDED in-place + MAJOR bump + frozen tag

- One-time migration commit rewrites all 639 CSV + 639 JSON files
  (`find quake_exports -type f` = 1,278) in place to the lean schema.
- **Before** that commit, `git tag artifact-v1-final <pre-rewrite SHA>` and push it.
  raw.githubusercontent.com URLs accept a tag/SHA in the ref slot, so any external
  consumer pinned to the old files re-pins from `…/master/quake_exports/…` to
  `…/artifact-v1-final/quake_exports/…` and keeps the exact 32-column bytes forever —
  at **zero ongoing carrying cost in HEAD** (the old files live in history, not the
  working tree).
- CHANGELOG major bump to **3.0.0** with a dated deprecation notice; a short notice on
  the dashboard and in README pointing pinned consumers at the tag.

Rejected — **parallel `quake_exports/v2/` tree**: permanently doubles a 1,278-file
directory (→ ~2,556), forces dual daily writes, dual golden fixtures, and a dashboard
choice, all to serve a consumer we have no evidence exists. The tag gives the same
"old shape stays retrievable" guarantee without carrying it in HEAD.
Rejected — **history rewrite (filter-repo) to purge old columns**: needlessly
destructive; the point is to preserve the old shape at a ref, not erase it.

### Blast-radius inventory (must change in lockstep)

- `go/internal/catalog/writer.go:49-56` — `DefaultColumns` trimmed to the kept set;
  **new** `ArtifactColumns`/drop-set projection applied in `ColumnsFor`/before
  `normalizeColumns` (removing from `DefaultColumns` alone re-appends the column —
  see finding). Comment `:32` ("production 32-column header") → new count.
- `go/internal/catalog/testdata/gen_golden.py:61` — `DEFAULT_COLUMNS` must stay in
  sync; **regenerate every golden fixture** (`existing_combined.*`, `golden_monthly.*`,
  `golden_merged.*`, `golden_merged_fresh.*`) — its own reviewed increment (I4).
- `go/internal/config/config.go:42`, `go/internal/api/client.go:171,188` — "32-column"
  comments → new count / new schema-version language.
- `src/mmeq/export/writer.py` — same projection/drop-set on the Python path (writers
  currently pass through all raw keys; both need the allow-list). If the aggressive
  set is chosen, also gate the `geocoder.add_place_columns` call.
- `src/mmeq/config.py` — add `MMEQ_ARTIFACT_SCHEMA` tunable (default `v1`; flips to
  `v2` in the docs increment) per the config-in-config.py convention.
- `tools/diff_exports.py` — header-equality logic (`:77-88`) still works, but a **new
  `--project`/projection-equality mode** is needed for the migration gate (I3).
- `.github/workflows/shadow_go_export.yml:67-80` — once both exporters emit v2 the
  existing Python-vs-Go diff still holds; during the transition it must diff at the
  chosen schema (bump the env there in lockstep, or the shadow reds out).
- `docs/index.html:167-168,217` — download links resolve unchanged; the served
  *content* changes shape. Add the deprecation notice near them.
- `README.md` (quake_exports section ~`:56`) + `CLAUDE.md:48` — the latter still says
  "33-column" (stale even today; 004 I5 established 32) — fix to the new count.
- `specs/002` and `specs/004` — their "byte-identical / 32-column" contract language
  gets a forward-reference note that 005 supersedes the schema (not the transport).
- `CHANGELOG.md` — 3.0.0 entry.
- **`paper/main.tex` — no change (verified: zero column/order dependency).**
- **Golden geocoder fixtures (`go/internal/geocoder/testdata/`) — no change** unless
  the aggressive set is chosen and enrichment is disabled.

### Verification spine

The migration is a **pure column projection**: every surviving cell must be
byte-identical to today. The central gate (mirroring spec 004's shadow discipline) is
a **projection-equality check** — for every file in `quake_exports`, the v2 file must
equal the v1 file with the dropped columns deleted, byte for byte. No value in any
kept column may change. If that holds over all 1,278 files, correctness is proven
mechanically rather than argued.

## Increments

- [ ] **I1 (projection mechanism, default-off)** — add the allow-list/drop-set
      projection to BOTH writers (`catalog/writer.go`, `export/writer.py`) plus the
      `MMEQ_ARTIFACT_SCHEMA` tunable in `src/mmeq/config.py` (default `v1`). With the
      default, output is byte-unchanged — **existing golden + shadow tests pass with
      no fixture regeneration** (that's the gate). Unit-test the projection in
      isolation (drop-set removed, surviving order preserved).
- [ ] **I2 (define v2 set)** — encode the chosen D1 set as `ArtifactColumnsV2` in Go
      and Python behind the flag; unit tests assert the exact kept set + that dropped
      keys never reappend (the ColumnsFor-reappend trap). No artifact rewrite yet.
- [ ] **I3 (projection-equality gate)** — extend `tools/diff_exports.py` with a
      `--project <dropped-cols>` mode (or add `tools/project_check.py`): assert v2
      output == v1 output minus the dropped columns, byte-level, over the full
      combined catalog and a monthly/yearly sample. This is the correctness proof and
      must pass before any file is rewritten. Independently runnable in CI.
- [ ] **I4 (golden regeneration — its own reviewed step)** — run
      `gen_golden.py` under `MMEQ_ARTIFACT_SCHEMA=v2`; commit the regenerated catalog
      fixtures; **the review checks the fixture diff shows ONLY dropped columns**,
      nothing else moved. Kept separate so the "byte baseline reset" is a deliberate,
      reviewed act, never a side effect.
- [ ] **I5 (one-time migration commit)** — `git tag artifact-v1-final` on the
      pre-rewrite SHA and push; rewrite all 1,278 files via a dedicated migration
      script (project each file onto its surviving columns-in-existing-order); the I3
      gate must pass on every rewritten file vs its pre-rewrite self. Atomic writes.
- [ ] **I6 (docs/dashboard/contract lockstep + flip default)** — flip
      `MMEQ_ARTIFACT_SCHEMA` default to `v2`; update README, `CLAUDE.md` (fix the
      stale "33-column"), `docs/index.html` deprecation notice, specs 002/004 forward
      note, `shadow_go_export.yml` env, CHANGELOG 3.0.0. Only after this does "the
      published artifact is v2" hold end-to-end.

## Acceptance criteria

- [ ] Projection-equality (I3) holds for **every** file in `quake_exports`: v2 file ==
      v1 file with dropped columns removed, byte for byte — no kept cell changed.
- [ ] `artifact-v1-final` tag pushed; a raw-file URL pinned to it resolves to the exact
      32-column bytes (old shape retrievable, zero HEAD carrying cost).
- [ ] Go golden + parity tests green under v2 defaults; shadow CI ≥3 consecutive clean
      cycles at the new schema; `ruff check .` + `pytest tests/ -v` green.
- [ ] Python and Go emit **byte-identical v2 trees** on the same data
      (`tools/diff_exports.py`, tolerance 1e-9).
- [ ] Golden fixture diff (I4) contains **only** dropped-column deletions.
- [ ] README / `CLAUDE.md` / CHANGELOG / dashboard notice updated; `CLAUDE.md`'s stale
      "33-column" corrected. Paper unchanged (and unaffected).

## Risks / rollback

- **Unknown external consumers pinned to `master` raw URLs break** on the migration
  commit — mitigated by `artifact-v1-final` (re-pin = keep old shape) + the dated
  deprecation notice. This is the one irreducible risk; the tag makes it recoverable
  by any consumer without our involvement.
- **Projection mechanism silently mutates a surviving column's bytes** (e.g. a reorder
  slips in, floats re-marshal) — caught by the I3 byte-level projection-equality gate,
  which is a hard precondition for I5. This is why I3 precedes any rewrite.
- **ColumnsFor re-append trap** — deleting a name from `DefaultColumns` re-adds it,
  sorted; a naive implementation would "drop" nothing. I2's test asserts dropped keys
  never reappear.
- **Sparse-science loss** — dropping `nst/gap/dmin/rms` (NOT recommended) would be
  irreversible for new events; keeping them costs 4 mostly-empty columns. Recommended
  set keeps them.
- **Geocoder loss under the aggressive set** — recoverable (re-run geocoder) but gone
  from HEAD; hence the lean set keeps them and the aggressive set is owner-gated.
- **One large migration commit** (1,278 files) inflates that commit's diff — one-time,
  acceptable; history size is dominated by the daily churn already.
- **Rollback** — because the migration is a pure projection and the pre-rewrite tree is
  preserved (tag + history), rollback is: flip `MMEQ_ARTIFACT_SCHEMA` back to `v1` and
  `git revert` the migration commit → the exact 32-column artifact returns, no data
  reconstruction needed.

## Notes

Scope discipline: this is spec 004's explicitly-deferred "Phase 2" and must not be
bundled with transport work. It touches no seismology/GMPE math — it is a byte-level
column projection over already-fetched files plus a writer allow-list, gated by a
mechanical equality proof. The paper's independence from the artifact schema (verified,
not assumed) is what makes it safe. Decision trail and the full 3-agent audit reports
live in the 2026-07-03 session transcript.
