# mmeq-export (Go)

Go reimplementation of the earthquake **fetch → validate → dedup → export** pipeline —
a single static binary intended to replace `python dataexport.py` in the daily CI cron.

**Scope:** this binary produces the CSV/JSON artifacts; the scientific analysis, figures,
dashboard, and paper stay in Python and consume those artifacts. The CSV/JSON is the
contract boundary. See [`../specs/002-go-export-rewrite.md`](../specs/002-go-export-rewrite.md)
(and [`001-data-fetch-upgrade.md`](../specs/001-data-fetch-upgrade.md), which lands first
as the Python reference).

**Status:** scaffold only — the export logic is unimplemented pending spec 002 approval.

## Layout

```
cmd/mmeq-export/      # CLI entry (stdlib stub now; cobra during implementation)
internal/config/      # MMEQ_* env config (mirrors config.py)
internal/api/         # retrying HTTP client + 500-cap window bisection (spec 001)
internal/catalog/     # validate, dedup, date-range, CSV/JSON writer (byte-parity)
internal/geocoder/    # ADM polygons + nearest OSM place (or hybrid Python post-step)
testdata/             # golden CSV/JSON snapshots from current Python output
```

## Build & run

```bash
cd go
go build ./...
go run ./cmd/mmeq-export export   # prints "not yet implemented"
```

## Migration (keeps CI green throughout)

1. Snapshot current Python `mmeq export` output as golden fixtures in `testdata/`.
2. Implement the binary; `go test ./...` diffs against the golden fixtures.
3. Shadow CI job runs Go alongside Python for ≥3 daily cycles with clean diffs.
4. Cut `daily_data_fetch.yml` over to the Go binary; drop the Python `pip install`.
5. Deprecate `dataexport.py`.
