---
spec: 007
title: Backfill the 2013-09 … 2015-02 catalog gap from USGS FDSN
status: Done           # Draft | Approved | In progress | Done
author: Claude (2026-07-07) + Aung Khant Zaw
created: 2026-07-07
---

## Problem / motivation

The 2026-07-06 integrity audit found the published catalog has **zero events
from 2013-09 through 2015-02** — no 2014 monthly files at all. This is not low
seismicity: USGS ComCat recorded ~86 M≥4.5 events in the Myanmar region in 2014
alone (44 in 2013-H2, 23 in 2015-H1). The upstream `mmeq.akze.net` API (which
proxies USGS/ISC) has an ~18-month ingestion hole, so any rate/temporal
analysis over that window is biased low. The owner confirmed the artifact is a
research dataset and asked to **backfill the gap from USGS** rather than leave a
documented hole.

## Design

### Source & filter (reproducible rule, not a black-box mimic)

The catalog is itself an inconsistent subset of USGS (e.g. 2016 kept only 32 of
~50 in-Myanmar M≥4.0 events), so exactly reproducing the API is impossible and
undesirable. Instead the backfill uses a **transparent, reproducible rule**:

- **Source:** USGS FDSN event service (`earthquake.usgs.gov/fdsnws/event/1`),
  GeoJSON.
- **Window:** `2013-09-01T00:00:00Z … 2015-03-01T00:00:00Z` (half-open) — the
  exact empty span.
- **Bounding box:** lat 9–29, lon 92–102 (the catalog's own spatial extent).
- **Magnitude:** M ≥ 4.0 — matches the catalog's adjacent-year completeness
  (2012 kept M≥4.0; the fill's median is M4.4).
- **Spatial refinement:** keep only events whose epicenter falls inside a
  Myanmar ADM1 polygon, decided by the pipeline's own geocoder
  (`analysis/geocoder.enrich_dataframe` → non-empty `state_region`). This is
  the same characterization every existing catalog row already carries and
  yields **98 events** (~65/yr, in line with 2012's 71 and 2016's 32).

### Schema & provenance

Backfilled rows are mapped to the exact 32-column artifact schema and run
through the normal `validate_quake_data` path (time formatting, geocoding,
per-window dedup), so they are byte-shaped identically to the existing USGS
rows (`net='us'`, `country='MM'`, `continent='Asia'`, `type='earthquake'`,
`magType` from USGS, real `updated`, `locationInferred=1`, `timeAdded=0`,
`timestamp=0`; geocoder columns populated; `shakemap*`/`state`/`initialPosition`
empty). **No schema change** (the owner requires the columns kept stable).

**Provenance is implicit and fully documented:** the window was empty, so every
event in 2013-09 … 2015-02 is a backfill. The exact query (service, box,
window, magnitude, polygon rule, pull date) is recorded in README + CHANGELOG,
making the set reproducible and the backfilled rows identifiable by their time
window. No consumer-visible flag column is added.

### Write path

`tools/backfill_usgs.py` fetches → maps → validates/geocodes → filters to
Myanmar → writes monthly CSV+JSON (`earthquakes_2013_09` … `earthquakes_2015_02`)
via the existing atomic writers, then calls `rebuild_combined()` (which after
spec-002/phase-2 also regenerates the yearly artifacts from the monthlies). The
combined store is read first in the rebuild so its column order is canonical.

## Data & outputs impact

- New monthly files for 2013-09 … 2015-02 (18 files × CSV+JSON).
- `earthquakes_combined.{csv,json}` grows by 98 rows (9,420 → 9,518),
  re-sorted chronologically.
- Yearly 2013/2014/2015 CSV+JSON regenerated.
- README "known catalog gaps" note updated (gap filled, method documented);
  event-count references refreshed. Any derived stat that moves (b-value at
  Mc4.0, declustered counts) checked and updated in lockstep per CLAUDE.md.
- Paper: catalog-size / M≥5 counts checked; the 2014 events are all M4.0–5.7,
  so headline b-value (dominated by 2025) and dam/PSHA numbers are unaffected —
  verified, not assumed.

## Acceptance criteria

- [x] `tools/backfill_usgs.py` is idempotent and reproducible: re-running yields
      the same 98-row set (dedup by id), no duplicates introduced.
- [x] Combined catalog has no zero-event month in 2013-09 … 2015-02; every
      backfilled row is inside a Myanmar ADM1 polygon and M≥4.0.
- [x] All backfilled rows validate (bounds, `time_utc`/`time_mmt` = +06:30,
      32-column schema, `net='us'`); combined CSV↔JSON id-sets agree; no
      duplicate ids anywhere.
- [x] `ruff check .` + `pytest tests/ -v` green; Go golden/parity tests
      untouched (the Go exporter is not involved — this is a one-time tool).
- [x] README gap note + counts updated; paper counts checked; CHANGELOG entry
      with the exact reproducible query.

## Risks / rollback

- **Completeness step vs neighboring years** — 2016's own under-count (API
  fault) means 2014 may look marginally more complete than 2016. Mitigated: the
  fill matches 2012's density, M≥4.0 is documented, and the backfill window is
  explicit so a researcher can re-cut completeness. Rate analysis over
  2010–2019 (the paper's period bin) is unaffected at this scale.
- **Provenance opacity** — mitigated by full query documentation; the empty
  window makes the backfilled set unambiguous without a flag column.
- **Rollback** — delete the 2013-09 … 2015-02 monthly files and re-run
  `rebuild_combined()`; the catalog returns to its prior state (the gap).
