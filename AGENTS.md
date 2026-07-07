# AGENTS.md

Guidance for AI coding agents working in this repository (any agent, not just
Claude). This is the emerging cross-tool convention; the authoritative,
detailed instructions live in **[`CLAUDE.md`](CLAUDE.md)** — read it first.

## The short version

This is a **scientific data pipeline**, not a web app. It fetches, validates,
and analyzes Myanmar earthquake data and publishes a seismic-risk study of 254
dams to a paper + live dashboard. **Correctness of the seismology/ground-motion
math is the top priority** — bugs here are silent and end up in published claims.

## Before you change anything

1. Read [`CLAUDE.md`](CLAUDE.md) (layout, domain-correctness rules, conventions)
   and [`CONTRIBUTING.md`](CONTRIBUTING.md) (dev loop, PR/CI flow).
2. Non-trivial work is **spec-first**: write `specs/NNN-slug.md` before code and
   read the existing specs (001–009) for the subsystem you're touching.

## Hard rules (most-violated first)

- **Edit `src/mmeq/`**, wire through `cli.py`. The Go code in `go/` is an
  export-only byte-parity mirror — only touch it for export/fetch/write changes,
  and keep `go test ./...` green.
- **Keep `ruff check .` and `pytest tests/ -v` green.** Add a test for new
  analysis functions. But "tests pass" ≠ "science correct" — run the affected
  stage and sanity-check the numbers.
- **Derived numbers move together:** an event-count / b-value / dam-grade / PGA
  change means updating `README.md`, `paper/main.tex`, and the figures in the
  *same* change.
- **The `quake_exports/` catalog is a research dataset:** keep all 32 columns
  (don't trim — spec 005 rejected), parse CSVs by column *name* not position, and
  dedup against other catalogs **spatio-temporally** (multi-network union — id
  alone massively over-counts). See `tools/backfill_usgs.py`.
- **Branch + PR**, never push to `master`. CI (ruff, pytest, `go test`, CodeQL,
  automated review) gates every PR, and merging to `master` auto-deploys the
  public dashboard.
- Config goes in `src/mmeq/config.py` (`MMEQ_*` env overrides). Logging via
  stdlib `logging`; `print` only for user-facing CLI summaries. Wrap heavy/optional
  deps and network calls in `try/except`.

## Tools

See [`tools/README.md`](tools/README.md) for the maintenance scripts (USGS
backfill, vendor pinning, Python-vs-Go export diff, figure data).
