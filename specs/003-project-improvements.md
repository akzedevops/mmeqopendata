---
spec: 003
title: Project improvements — CI test gate, de-duplication, structure, readability
status: Draft            # Draft | Approved | In progress | Done
author: Claude (research) + Aung Khant Zaw
created: 2026-06-30
---

## Problem / motivation

A project-wide audit (research only) found maintainability/structure issues that are not
correctness bugs but raise the risk of *future* silent breakage of the published dashboard
and paper. The biggest: **no test gate before deploy**, **duplicated/drifting code paths**
(`dataexport.py` vs `src/mmeq/export/*`; `report/` vs `docs/report/`), and analysis run
**twice per CI build**. These compound the data-fetch and Go work, so fixing them first
makes 001/002 cleaner.

## Goal

A repo where every push that ships to the public dashboard passes `pytest` first, the
export/report/figure paths route through `mmeq` subcommands (no duplicated scripts, no
double-computation), tunables live in `config.py`, and lint/format is enforced.

## Non-goals

- No scientific/numeric changes (those are correctness specs, already handled).
- Not a full rewrite of `generate_figures.py` internals beyond modularization needed to
  test/route it.

## Design — ranked work items

**P1 — Add a pytest CI gate (S, High).** New `test` job (`pip install -e ".[dev]" && pytest
tests/ -v`); make `report_and_pages.yml` `build`/`deploy` `needs: test`. Today neither
workflow runs tests, yet `report_and_pages` ships analysis straight to Pages on every
master push — the silent-ship risk CLAUDE.md warns about.

**P2 — Unify `report/` vs `docs/report/` and stop double-computing (M, High).** CI runs
`tools/build_figure_data.py` (writes `report/dam_risk_scores.csv`) *and*
`mmeq report --output docs/report` (recomputes the same). Read paths are inconsistent
(`generate_figures.py`→`report/`, `shakemap_validation.py:15` hardcoded, `fragility.py:183`
cwd-relative, `dam_risk.py` `_load_vs30` uses `MMEQ_REPORT_DIR`). Thread one `report_dir`
everywhere and have `mmeq report` consume the already-built CSVs instead of recomputing.

**P3 — Retire `dataexport.py` duplication (M, High).** It re-implements
`validate_quake_data`/`_dedup_frame`/`save_to_csv`/`save_to_json`/`generate_date_ranges`/
`fetch_quake_data` already in `src/mmeq/export/*`, and has already drifted (its
`save_to_json` lacks `ensure_ascii=False`; bounds copy-pasted from `config.py`). Make
`daily_data_fetch.yml` run `mmeq export`; reduce `dataexport.py` to a shim or delete.
(Shared with [[001-data-fetch-upgrade]] R2 and a prerequisite for [[002-go-export-rewrite]].)

**P4 — Test the published-number functions (M, High).** Zero tests for `coulomb`,
`fragility`, `osm_exposure`, `population*`, `shakemap_validation`, `temporal`, `geocoder`,
`finite_fault`, `gem_faults`, and within `dam_risk` the consequential
`dam_risk_scores`/`monte_carlo_pga`/`sensitivity_analysis`/`compute_hazard_curve`. Add unit
tests for grade thresholds, hazard return-period inversion, fragility lognormal CDF, plus an
end-to-end `mmeq report --no-pdf --no-animated …` smoke test over a ~50-row fixture (would
also catch the cluster-image bug below).

**P5 — Hoist magic numbers into `config.py` + one scoring helper (S, Med).** Risk weights
`0.35/0.30/0.20/0.15`, grade thresholds `7/5/3`, `seismic_score = pga/0.05*10`, exposure
`h*0.3+c*0.02+s*0.005`, and `sigma_ln=0.65` are inlined in *both* `dam_risk_scores` and
`sensitivity_analysis` — tune one, the two analyses silently diverge. Centralize and share a
`_score_dam()` helper.

**P6 — Delete dead legacy scripts (S, Med).** `advanalysis.py`, `adv2analysis.py`,
`visualizer.py` are referenced only by CLAUDE.md's "do not edit" note (zero imports/workflow
refs). Delete (git preserves history) or move to `archive/`, and drop the CLAUDE.md note.

**P7 — Add ruff + pre-commit + lint CI step; trim redundant pip installs (S, Med).** No
linter/formatter today. Add `ruff` (lint+format) to the `dev` extra and a CI lint step.
Remove the dead `pip install plotly fpdf2` lines (already in `pyproject` deps) from both
workflows.

**P8 — Promote `tools/build_figure_data.py` to `mmeq build-figure-data` and modularize
`generate_figures.py` (M, Med).** Remove the `sys.path.insert`/`from src.mmeq` hacks and the
mixed `src.mmeq`/`mmeq` import roots; wrap each of the 14 figures in a function with a
`--only N` flag so one figure can regenerate/test in isolation (today an exception in fig 7
aborts 8–13; only fig14 is guarded).

**P9 — Centralize data-file paths off `os.getcwd()` (M, Med).** `dam_risk.py:14,34`,
`coulomb.py:298`, `map.py:18`, `clustering.py:200`, `population.py:18`, `gem_faults.py:5`
locate files via `os.getcwd()` — breaks when `mmeq` runs from any other dir. Add a
package-relative `DATA_DIR`/`PROJECT_ROOT` in `config.py`.

**P10 — Vectorize `sensitivity_analysis` (M, Med).** It recomputes per-dam PGA/fault-distance
inside its 100-sample Monte-Carlo loop (100 × 254 × N_segments) though only the weights
change. Precompute per-dam terms once, vectorize the weight sampling.

**Incidental bug to fix in P4/P8:** `cmd_report` (`cli.py:348`) passes a
`earthquake_clusters.png` to the PDF that only `cmd_analyze` generates, so the report PDF's
cluster image is silently missing in CI. Generate it in `cmd_report` or drop the arg.

## Data & outputs impact

- No artifact-shape changes intended. P2/P8 may change which dir figures read from; verify
  figures + numbers regenerate identically (golden compare before/after).

## Acceptance criteria

- [ ] CI `test` job runs `pytest` and `report_and_pages` deploy `needs: test` (P1).
- [ ] `dam_risk_scores` computed once per CI build (P2) — assert no duplicate compute in the
      workflow logs / refactor removes the second call.
- [ ] `daily_data_fetch.yml` runs `mmeq export`; `dataexport.py` retired (P3).
- [ ] New tests for `dam_risk_scores`, `compute_hazard_curve`, `fragility`, + report smoke
      test; coverage of analysis modules rises measurably (P4).
- [ ] Risk weights/thresholds/`sigma_ln` sourced from `config.py`; both analyses use the
      shared helper (P5).
- [ ] `ruff check` clean in CI; dead scripts gone (P6/P7).
- [ ] No regression: full `pytest tests/ -v` green; figures + paper regenerate identically.

## Risks / rollback

- P2/P3 touch the production CI path — land behind the P1 test gate and verify a full
  `report_and_pages` dry run (workflow_dispatch on branch) regenerates identical figures
  before merge.
- P8 modularization could change figure rendering subtly — golden-compare PNG/PDF (with
  `SOURCE_DATE_EPOCH` pinned) before/after.
- Each item is independently revertable; sequence P1 → P6/P7 (quick wins) → P2/P3/P8
  (structural) → P4/P5/P9/P10.

## Notes

Quick wins first (P1, P6, P7, P5). The structural items (P2, P3, P8) share one root cause —
export/report/figure logic living *outside* the CLI — so routing everything through `mmeq`
subcommands resolves duplication, reproducibility, and double-compute together, and is the
foundation [[002-go-export-rewrite]] ports from.
