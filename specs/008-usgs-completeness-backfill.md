---
spec: 008
title: Complete the historical catalog (1970–2019) from USGS with spatio-temporal dedup
status: Done           # Draft | Approved | In progress | Done
author: Claude (2026-07-07) + Aung Khant Zaw
created: 2026-07-07
---

## Problem / motivation

Spec 007 filled the empty 2014 window. A follow-up audit of the *whole* catalog
against USGS ComCat (all in-Myanmar M≥4.0, 1970–present) revealed the deficit is
broader — and mostly hidden by cross-network duplication:

- The catalog is a **multi-network union**: 62% Thai-network ids (`th_`), 22%
  USGS (`us`), 9% India (`in_`), 4% EMSC (`ems`). The same physical earthquake
  is often stored under a regional network's id whose epicenter disagrees with
  USGS by tens of km, so **id-only comparison is meaningless**.
- Of 761 USGS events with no id match, **476 are cross-network duplicates**
  already present (a quake within seconds, located 30–160 km off). Only **285**
  are genuinely missing by spatio-temporal test; time-primary dedup (same event
  if any catalog quake is within 35 s, ≤250 km) stabilizes this at ~219.
- The genuinely-missing events cluster in **1970–1999** (~22, sparse era, USGS
  authoritative) and **2013–2019** (~136, the degraded era around the 2014
  hole). The 2020–2025 residue (~60) carries the highest dedup risk (dense
  catalogs) and was **deliberately excluded** (owner scope decision: 1970–2019
  only).

## Design

- Extend `tools/backfill_usgs.py` with a **spatio-temporal dedup** step
  (`_drop_crossnetwork_dups`): before writing, drop any fetched USGS event that
  matches a catalog event by exact id OR is within `--window-s` seconds (35) AND
  `--dup-km` km (250) of one. Time-primary because origin time agrees tightly
  across networks while location scatters. Dedup is **on by default** now, so
  the tool can never inject a duplicate.
- Monthly writes MERGE (id-dedup) into existing files — `save_to_csv(dedup=True)`
  for CSV, `save_merged_json` for JSON (a bare `save_to_json` would overwrite
  the many already-populated 1970–2019 months). Then `rebuild_combined()`
  reconciles combined + yearly.
- Run: `python tools/backfill_usgs.py --start 1970-01-01 --end 2020-01-01`.

## Data & outputs impact

- **+160 genuinely-missing events** added (141 `us`, 16 `iscgem`, 3 `iscgemsup`
  — ISC-GEM is USGS's reviewed historical catalog, authoritative for pre-2000);
  1,473 id-dups + 134 cross-network dups correctly excluded.
- Combined 9,519 → **9,679**; yearly artifacts + monthly files regenerated.
- **0 duplicates introduced** (verified: no added event is within 35 s/250 km of
  a pre-existing one; the 10 pre-existing cross-network near-dup pairs are
  unchanged). Schema intact (32 columns).
- Derived numbers updated in lockstep (CLAUDE.md): M≥5 367→393, b@Mc4.0 N
  2,597→2,757 (b 0.72 unchanged), declustered b 1.06→1.04, DBSCAN central
  cluster 9,045→9,277 (95%→96%) and **count 7→6 (eps-sensitive, now flagged)**,
  hazard 170→172 (>0.1g) / 42→46 (>0.2g). Dam grades unchanged (44/116/45/49 —
  scenario is the 2025 event).

## Acceptance criteria

- [x] `_drop_crossnetwork_dups` removes id-dups AND spatio-temporal
      cross-network dups; dedup on by default; `--window-s`/`--dup-km` tunable.
- [x] Backfill adds only genuinely-missing events: 0 added event within
      35 s/250 km of a pre-existing one; 0 duplicate ids; 32-column schema.
- [x] No near-duplicate pairs introduced (10 pre-existing → 10 after).
- [x] All derived numbers recomputed and updated across README/paper/docs;
      figures regenerate in CI.
- [x] `ruff check .` + `pytest tests/ -v` green; Go untouched (one-time tool).
- [x] CHANGELOG entry with the reproducible query and the dedup rule.

## Risks / rollback

- **Dedup threshold sensitivity** — the genuinely-missing count is stable
  (217–219 across 30–60 s / 200–300 km), and the 250 km bound is generous
  toward *not* adding a duplicate. The excluded 2020–2025 residue avoids the
  densest, riskiest years entirely.
- **DBSCAN count instability** — the "7 clusters" claim was always eps-fragile
  (6–8 across nearby ε); the paper now reports the count with that caveat and
  leans on the stable dominant-central-cluster fact instead.
- **Rollback** — `git revert` the backfill commit (or delete the added ids and
  `rebuild_combined()`); the catalog returns to its 9,519-row state.

## Notes

The exact 160 backfilled USGS event ids are frozen in
`008-backfill-manifest.csv` (id, time_utc, mag, net) with the query header —
ComCat is mutable, so this list, not a live re-query, is the authoritative
record of what was added. Verified 2026-07-07 by a 5-agent adversarial pass:
0 duplicates injected (checked to 120 s / 500 km), all 134 cross-network drops
are genuine same-events (median dt 0 s, median |dM| 0), all derived numbers
recompute exactly. Two non-blocking notes carried forward: (a) the ±0.01-scale
raw b-value is illustrative only; (b) the 250 km dedup bound is deliberately
generous — its error direction is conservative (it can only ever over-drop,
never inject a duplicate), which is the right bias for a research dataset.
