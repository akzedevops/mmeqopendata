---
spec: 004
title: Migrate the artifact pipeline to API v2 via a full-fidelity export endpoint
status: Draft            # Draft | Approved | In progress | Done
author: Claude (3-agent research fan-out) + Aung Khant Zaw
created: 2026-07-03
---

## Problem / motivation

The daily CI was cut over to the typed `/api/v2/earthquakes` route (fetcher's
`_fetch_v2`, gated on `MMEQ_API_V2_URL`). On 2026-07-03 the first monthly file
written through it (`earthquakes_2026_07.csv`) proved the route degrades the
published artifact two ways (verified against the live store):

1. **Real data blanked** — `_v2_record_to_v1` fills the 12 legacy fields with `""`,
   but the store's `raw` column has real values for upstream-ingested rows
   (`continent: "Asia"`, `timeAdded`, `timestamp`, `locationInferred`,
   `initialPosition`, `shakemapLastUpdated`). The committed July rows lost that.
2. **Format drift** — JSON number `10` decodes to Python int → CSV `10` not `10.0`;
   `updated_at` lacks the v1 `.000Z` style.

(A suspected third regression — column-order flip — turned out to be pre-existing:
the July header is byte-identical to June's, which was itself written with a
non-canonical order in the Node-v1 era. Headers are **per-file** artifacts:
older files even place the shakemap pair last. The canonical `DefaultColumns`
order is what fresh fetches through the fixed v1-compat route produce; repairing
July will *change* its header to that order — an improvement, not a restoration —
while never-refetched months keep their historical headers.)

Meanwhile spec 002's Go exporter went the other way (fetches the v1-compat route,
byte-parity proven). Two exporters on two contracts is not a stable end state. The
goal of this spec: **one v2-native transport that both exporters use, with zero
artifact change**, so the v1-compat route stops being load-bearing for CI.

## Research findings (2026-07-03, three-agent audit)

**Column value audit** (9,400-row combined CSV):
- Real, unrecoverable: `id, latitude, longitude, depth, mag, magType, time_utc,
  updated, timeAdded, location, place, locationInferred` + sparse-but-genuine
  seismic-quality metrics `nst, gap, dmin, rms` (10–21% filled, historical
  USGS-sourced rows only; the upstream source **no longer supplies them** for new
  events — newest rows are 0% filled).
- Constants: `country`="MM", `continent`="Asia", `type`="earthquake".
- Dead/redundant: `state` (100% empty), `timestamp` (exact epoch copy of
  `time_utc`), `initialPosition` (rounded lat,lon). `shakemapURL`/
  `shakemapLastUpdated` populated on exactly 1 event.
- Derived (recomputable): `time_mmt` + the 6 geocoder columns.

**Consumer audit**: only `latitude, longitude, depth, mag, time_utc, time_mmt, id`
are read by any analysis/visualization/figure/test/dashboard code. Nothing indexes
CSV columns positionally. The docs dashboard fetches no quake_exports data (links
only). Column order is enforced only by exporter-internal contracts
(`go/internal/catalog.DefaultColumns`, Go golden fixtures, `tools/diff_exports.py`
header equality) — all changeable in lockstep if ever needed.

**Store contents** (`mmeq-api` `events.raw`): CSV-seeded rows carry the CSV columns
(incl. geocoder fields, no upstream-only fields); upstream-ingested rows carry the
full upstream record (incl. the 12 legacy fields, no `time_utc`). All values are
JSON **strings** (byte-stable to pass through; no float re-marshaling hazard).

## Design — the chosen scenario

**Phase 1 (this spec): v2 transport, unchanged artifact.**
Add `GET /api/v2/export` to mmeq-api — a purpose-built artifact-pipeline endpoint:

- v2 conventions: half-open `[from,to)`, `limit`/`offset` (max 10000), strong
  ETag/304, `{meta, earthquakes}` envelope. `updated_after` comes free via the
  existing `Query` machinery but is NOT load-bearing for Phase 1 (neither
  exporter passes it today) — do not build anything on it here.
- Each record is served from the event's `raw`, re-serialized in the canonical
  v1 key order via the (already deployed, tested) `orderedV1Record` serializer —
  values copied as raw bytes, never re-encoded, `"time"` injected for CSV-seeded
  rows. **Records are byte-identical to what the v1-compat route serves today**,
  wrapped in the v2 envelope. No DB migration; read path only.
- **Byte-equality pins** (contract-tested, review findings 2 & 3):
  - Records must go out through the same `writeJSON`/`json.NewEncoder` path as
    v1compat (default HTML-escaping ON) — a hand-rolled buffer or
    `SetEscapeHTML(false)` silently breaks record-byte equality.
  - The raw-emptiness filter (`raw IS NOT NULL AND raw != ''`, as in
    `RawEventsByDay`) must live in the WHERE clause of **both** the COUNT and
    the page query, or `meta.total` over-reports and offset pagination
    tears (duplicate/skipped records). Contract test must include an
    empty-raw row.
- Absent keys (e.g. `nst` on CSV-seeded rows, geocoder fields on upstream rows)
  stay absent; consumers already treat absent-as-NaN (pandas) / absent-as-missing
  (Go `Extra` map).

Both exporters then fetch `/api/v2/export`:
- **Python** `_fetch_v2` → rewritten to consume `/api/v2/export` and pass records
  into the DataFrame **verbatim** (delete `_v2_record_to_v1` and `_V1_ONLY_FIELDS`
  — the blanking mapper was the bug). Pagination loop kept.
- **Go** `internal/api` gains `FetchWindowExport` (paginated like `FetchWindow`,
  records verbatim like `FetchWindowV1`); `MMEQ_FETCH_ROUTE` default becomes
  `export`, with `v1`/`v2` kept as fallbacks.

Because the records are byte-identical to v1-compat records, **the artifact stays
byte-for-byte unchanged** — spec 002's parity guarantee, golden tests, and shadow
CI carry over without regeneration.

**Data repair (one-shot, DEADLINE-BOUND):** degraded rows self-heal only while
their month is still in the fetch window — `generate_date_ranges` starts at
`last_updated + 1 day`, so **once `last_updated` crosses into August, July is
never refetched by any normal run** and the degraded rows become permanent
(healable only by manual surgery; `--rebuild` does not refetch). Therefore:
I2 must be deployed and I3 dispatched **before 2026-08-01**, and every degraded
row accumulated during the transition (~2/day) heals in the same run via
keep-last dedup (verified: both the CSV dedup and the combined-JSON merge
unconditionally replace records for existing ids, Python and Go alike). Verify
`continent`/`timeAdded`/`initialPosition` values return and the July monthly
header equals `DefaultColumns` — yearly/combined headers are governed by their
existing files and are explicitly NOT expected to change.

**End state:** `/api/myanmar-quakes` (v1-compat) stops being load-bearing for CI —
kept serving for external compatibility, but internal consumers are all-v2. Spec
002's final cutover (daily CI → Go binary) proceeds unchanged, now on v2 transport.

**Phase 2 (explicit NON-goal here, future spec if ever wanted): artifact schema
cleanup.** The audits show 25 of 32 columns are read-free and several are
dead/constant — a leaner "artifact v2" is possible (drop `state`, `timestamp`,
`initialPosition`, constants; keep sparse quality metrics). Deliberately out of
scope: it changes the published dataset shape for unknown external consumers and
resets every golden baseline. Do not bundle it with the transport migration.

## Increments

- [ ] **I1 (mmeq-api)** — `/api/v2/export` endpoint: `store.ExportEvents`
      (Query machinery + `raw` column, emptiness filter in BOTH count and page
      WHERE), handler reusing the v1compat serializer and the shared
      `writeJSON` path; contract tests: record-byte equality vs the v1-compat
      route on the same events, ETag/304, pagination incl. an empty-raw row;
      openapi.yaml; deploy to d2 (approval-gated) and verify in prod.
      Decide Cache-Control (recommend keeping the shared 300s — exporters
      don't send If-None-Match, and nginx adds no CDN layer).
- [ ] **I2 (Python)** — HARD PRECONDITION: I1 verified in prod first (master's
      v2 branch raises on error with NO v1 fallback and 404 is non-retried —
      merging I2 early kills the daily CI until someone manually unsets
      `MMEQ_API_V2_URL`). `_fetch_v2` → `/api/v2/export`, verbatim records;
      delete `_v2_record_to_v1` + `_V1_ONLY_FIELDS`; tests: fetched frame ==
      v1-route frame on fixtures; `daily_data_fetch.yml` env unchanged.
- [ ] **I3 (repair + verify)** — dispatch the daily fetch BEFORE 2026-08-01;
      assert July rows regain real values (`continent`, `timeAdded`,
      `initialPosition`, `.000Z` updated) and July monthly header ==
      `DefaultColumns`. If the deadline is missed: one-shot backfill via a
      temporary `--from` override or seed-style surgery (document which).
- [ ] **I4 (Go)** — `FetchWindowExport` (v2 pager + verbatim records);
      `MMEQ_FETCH_ROUTE=export` default. Known breakage to fix in the same
      increment: `TestRunV1Route` exercises the old default (fake server only
      serves the v1 path); `cmd/mmeq-export/main.go` hard-rejects routes other
      than v1/v2 (+ usage text); `export.Run`'s route dispatch silently falls
      back to v1 on unknown values — make it a three-way switch with an
      explicit error. Shadow CI keeps its Python step on the v1 route (env
      unset) as an INDEPENDENT cross-check: v1-served Python vs export-served
      Go proves the two API routes byte-agree end-to-end. Parity acceptance
      unchanged (≥3 clean cycles) → spec 002 cutover to the Go binary.
- [ ] **I5 (retire)** — after cutover: mark v1-compat "external compatibility
      only" in openapi + README; retire the Python fetch path with
      `dataexport.py` per spec 003; only THEN does the "no CI workflow
      references v1-compat" criterion hold (the shadow's Python step uses it
      until it is itself retired). Fix inherited doc drift while touching
      these files: the artifact is 32 columns, not 33 (v1KeyOrder's 33 minus
      the dropped `time`), in `v1compat.go`, `client.go`, `config.go`.

## Acceptance criteria

- [ ] `/api/v2/export` record bytes == v1-compat record bytes for the same events
      (contract-tested), plus meta envelope, ETag/304, pagination correct with
      empty-raw rows present.
- [ ] Python export via `/api/v2/export` produces artifacts byte-identical to a
      v1-route run on the same data (tree-diffed with `tools/diff_exports.py`).
- [ ] The 2026-07 degraded rows repaired in `quake_exports` before 2026-08-01
      (real values back, July monthly header == `DefaultColumns`) via a normal
      export run, no manual file surgery.
- [ ] Go exporter on `export` route passes all existing golden/parity tests
      unchanged; shadow CI (v1-Python vs export-Go — deliberate cross-route
      check) ≥3 consecutive clean cycles.
- [ ] After I5 only: v1-compat route no longer referenced by any CI workflow.

## Risks / rollback

- **Endpoint regression risks prod** — read-path only, no schema change; rollback =
  restart previous binary (`mmeq-api.prev` pattern). CI keeps working during any
  outage via route fallbacks (`MMEQ_FETCH_ROUTE=v1`, Python falls back to v1 when
  `MMEQ_API_V2_URL` is unset).
- **Envelope mismatch** (v1 returns bare `{"earthquakes":[…]}`, export adds meta +
  pagination) — Python pagination loop already exists for v2; Go reuses the v2
  pager. Contract tests pin both.
- **Absent-key asymmetry** (CSV-seeded vs upstream rows) — already production
  behavior on the v1 route today; pandas/Go both tolerate it. Golden tests cover
  mixed frames.
- **Daily commits during the transition** keep writing degraded v2-shaped rows
  (~2 events/day) until I2 lands — healed by keep-last dedup once I3 runs,
  PROVIDED the affected months are still inside the fetch window (see the
  2026-08-01 deadline above). Values always survive in the store's `raw`; what
  expires is the zero-effort repair path.
- **Offset pagination under concurrent ingest** — pages are separate queries with
  no snapshot isolation; an ingest between pages can duplicate (healed by dedup)
  or skip (healed by the next run's window refetch) a record. In practice months
  are far below one 10k page and ingest runs 23:30 UTC vs fetch 00:00, so
  exposure is negligible; noted for completeness.
- **I2 merged before I1 is live = dead daily CI** (404, no fallback, non-retried)
  — hence the hard precondition on I2; recovery is a manual env-var edit in
  `daily_data_fetch.yml`, not automatic.

## Notes

Decision trail: option A (typed columns + backfill) rejected — only option needing
a prod DB migration, reintroduces float-marshaling drift, leaves 12 mostly-null
columns on the clean contract. Option B (`?include=raw`) viable but muddies the
primary endpoint; C keeps contracts separated and reuses proven serialization.
Full agent reports in the session transcript (2026-07-03).
