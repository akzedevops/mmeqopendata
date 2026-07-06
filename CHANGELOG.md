# Changelog

## Unreleased

### Hazard-model corrections (2026-07-06 integrity audit, phase 4 — spec 006)

- **Scenario "rupture distance" now uses the actual 2025 rupture geometry**
  (`data/shakemap/rupture.json`) instead of a global plate-boundary trace
  file. Dams near non-ruptured fault sections (Kachin, Tanintharyi) had M7.7
  near-fault PGA: Ta Nai Hka, ~435 km from the rupture, was published at
  0.44 g / Critical — now 0.010 g / High. `dam_risk_scores.csv` gains a
  `dist_to_rupture_km` column. Dam grades: 45/134/61/14 → **44/116/45/49**
  (Critical+High 160, 63%).
- **PSHA rebuilt with a Frankel (1995) smoothed-seismicity source model.**
  The old Cornell-McGuire code integrated over magnitude only, collapsing the
  entire ~2,000-km regional rate onto each dam's single nearest-fault
  distance (near-fault 475-yr PGA ~2.1–2.5 g). The new model bins the
  declustered catalog on a 0.5° grid, Gaussian-smooths per-cell rates (σ =
  50 km, total rate conserved) and integrates hazard over the source-to-site
  distance distribution. Portfolio 475-yr PGA now 0.002–0.55 g (mean 0.14).
  **Documented as a catalog-only lower bound** — the committed fault file has
  no slip rates, so there is no fault-source term. A report stage
  (`--no-hazard`) regenerates per-dam `hazard_curves/*.csv` + an index
  manifest, replacing the frozen pre-declustering artifacts (finding M7) and
  the dead directory link.
- **Coulomb kernel parity fixed.** `_patch_coulomb_stress` used an unsigned
  `sin2α` that made the shear term odd in the along-strike coordinate; static
  stress from a point moment source must satisfy σ(−r)=σ(+r) (Aki & Richards
  2002). Signed `perp` restores the four-lobed King-Stein-Lin pattern
  (verified: ΔCFS(−r)=ΔCFS(+r) to machine precision, four alternating lobes).
  Classification threshold raised to the standard ±0.01 MPa (0.1 bar); the old
  ±0.001 MPa sat in tidal-stress noise. Triggered/shadow: 34/163 → **16/72**.
- References added: Frankel 1995, Gardner & Knopoff 1974, Okada 1992,
  Thant et al. 2023, Wald et al. 1999 (some were cited but missing).
### Publishing-seam fixes (2026-07-06 integrity audit, phase 3)

- **Daily data pushes now trigger the site rebuild.** GITHUB_TOKEN pushes
  deliberately create no workflow runs, so the dashboard silently served
  stale data (all of June it showed May 31). The daily job now dispatches
  `report_and_pages.yml` explicitly after a push that committed new data.
- **The live paper PDF is now the built one**: CI copies `paper/main.pdf` to
  `docs/report/paper.pdf`; the site had been serving a hand-committed copy
  frozen on 2026-05-01, missing every subsequent correction.
- **Stored-XSS closed on the public map**: dam CSV fields, OSM names and API
  strings are HTML-escaped (incl. backticks — folium interpolates popups into
  a jQuery template literal) before popup construction; regression test
  asserts no raw payload reaches the saved HTML.
- `.gitignore`'s unanchored `report/` (which also matched `docs/report/` and
  silently blocked committing new site artifacts like `vendor/` and
  `plotly.min.js`) anchored to `/report/`; dead "Copy static map" no-op step
  removed; Pages triggers widened to `tools/**` (matches CLAUDE.md).

### Data-artifact repairs (2026-07-06 integrity audit, phase 2)

- **Yearly JSON now merges instead of overwriting** (Python `save_merged_json`
  + Go `MergeCombinedJSON` on the yearly path). Previously each run replaced
  the yearly JSON with only its own fetch window: the published
  `earthquakes_2025.json` held 98 of 2,469 events — the entire M7.7 sequence
  absent — and froze that way at year rollover.
- **Yearly artifacts joined the rebuild path**: `mmeq export --rebuild` now
  regenerates every yearly CSV+JSON from the monthly files (same
  monthly-wins precedence as the combined store), so backfilled events reach
  the yearly files. One-time repair committed: 2025 JSON 98 → 2,469 records,
  2026 reconciled, the 10 late month-end events (incl. an M4.4) restored to
  the yearly CSVs, and all yearly files are now chronologically sorted.
- **Go `MergeCombinedJSON` NaN-id corruption fixed**: an id-less quake in a
  mixed batch produced a literal `null` record (Go NaN map keys are
  insert-only), silently dropping the event, and NaN-id records loaded from an
  existing file collapsed to one survivor. Both now get unique keys, matching
  Python's never-merge NaN semantics; regression-tested on both sides.

### Scientific corrections (2026-07-06 integrity audit, phase 1)

- **Scenario event fixed: 2025 Sagaing, not 1988 Lancang.** The catalog holds
  two M7.7 events and every scenario consumer (`dam_risk_scores`,
  `monte_carlo_pga`, `sensitivity_analysis`, the Omori aftershock forecast)
  selected the maximum-magnitude event with a bare `nlargest`/`idxmax`, which
  tie-breaks to the *first* row — the 1988-11-06 Lancang-Gengma earthquake on
  the China border, ~400 km from the epicenter the README/paper describe. A
  shared `seismology.select_scenario_event` now breaks magnitude ties by
  recency (override: `MMEQ_SCENARIO_EVENT_ID`). Published dam grades change
  from 25/148/67/14 (Critical/High/Moderate/Low) to **45/134/61/14** —
  179 dams (70%) Critical or High.
- **Omori fit rewritten as Ogata (1983) maximum likelihood** on exact event
  times. The previous least-squares fit on log-binned counts underestimated
  p by 0.2–0.4 on synthetic catalogs (and pinned at its silent p=0.5 clip on
  the real 2025 sequence) while hardcoding c=0.1 h. The 2025 M7.7 sequence now
  fits K≈35, c≈16.4 h, p≈0.77; a synthetic-recovery regression test asserts
  |p_fit − p_true| < 0.1. Previously published p=0.93 was the 1988 event's
  biased fit.
- **Paper sensitivity claim corrected**: "Critical+High stays above 30%
  regardless of weights" was contradicted by the shipped artifact; the paper
  now reports the actual distribution (median ~73%, 22/100 draws below 30%).
- Stale "54% Critical or High" in the paper (a pre-GMPE-fix leftover) fixed
  to 70% alongside the regenerated grade table.

### Go export cutover (specs 001/002/004 complete)

The daily data-fetch CI now runs the Go `mmeq-export` binary (build ~seconds,
no Python stack) after three clean shadow-parity cycles, the third on the
`/api/v2/export` route. `dataexport.py` retired; the Python `mmeq export`
stays for local use and remains parity-pinned by the nightly shadow diff.

### Roadmap cleanup (2026-07-03)

- **ShakeMap validation wired into `mmeq report`** — the ASK08-vs-station GMPE
  validation (paper Table `tab:validation`) now runs as a report stage after dam
  risk, gated by `--no-validation`, writing `shakemap_validation.csv` with a
  one-line station-count / mean observed-vs-predicted ratio summary. Its input
  paths are no longer cwd-relative hardcodes (station list from `config.DATA_DIR`,
  dam CSV from the report dir).
- **`population_raster.py` removed** — born orphaned; its WorldPop `.tif` is
  gitignored and absent, so it only ever returned `0.0`. The WorldPop figure is
  documented as a one-off offline analysis (CHANGELOG/README/paper reconciled).
- **Folium moving-CDN refs pinned** — `HeatMap`'s `@main` `leaflet_heat.min.js`
  and every Map's ref-less `leaflet.awesome.rotate.min.css` are now pinned to the
  installed folium version via `add_js_link`/`add_css_link` (one shared helper,
  `visualization/_folium_pins.py`).
- **Export pipeline extracted** — `cmd_export`'s fetch/aggregate/write body moved
  verbatim into `export/pipeline.py:run_export(workers)`; `cmd_export` is now a
  thin wrapper (mkdirs + `--rebuild` + `run_export`). Pure move, semantics
  preserved.

### v2 export migration (spec 004, 2026-07-03)

The daily export now fetches the API's new full-fidelity `/api/v2/export` route
(v2 envelope + pagination; records byte-identical to the legacy contract). The
previous typed-v2 fetch path silently blanked 12 legacy fields and drifted number
formats — the two affected July 2026 events were re-fetched and repaired in place
(commit `a71da231`). The upstream API is now self-documenting: Swagger UI at
[mmeq.akze.net/docs](https://mmeq.akze.net/docs).

### Go export rewrite lands in-tree (spec 002)

- `go/` module: `mmeq-export` binary reproducing the Python export pipeline
  (fetch → validate → geocode → dedup → write) with golden-file byte-parity,
  down to CPython float repr, the 32-column contract, and geocoder ties.
  Not yet wired into CI.
- `tools/diff_exports.py` — id-keyed, float-tolerant export-tree diff.
- `.github/workflows/shadow_go_export.yml` — nightly Python-vs-Go parity run;
  the Go binary takes over `daily_data_fetch.yml` after ≥3 clean cycles.

### Fixed
- Id-less events no longer collapse in the Go catalog dedup (mirrors pandas'
  measured `drop_duplicates` semantics exactly), and `--reuse-risk` now falls
  back to recomputing on a malformed/truncated `dam_risk_scores.csv` instead of
  aborting (Kilo review findings, PR #21).

### Spec 003 follow-up (P2/P4/P5)

Pure refactor + tests + CI plumbing — all
scientific outputs verified byte-identical (`dam_risk_scores.csv`,
`sensitivity_analysis.csv`; grades still Critical 25 / High 148 / Moderate 67 /
Low 14).

### Added
- `mmeq report --reuse-risk` (env: `MMEQ_REUSE_RISK=1`) — reuse a fresh
  `dam_risk_scores.csv` from `MMEQ_REPORT_DIR` (default `report/`) instead of
  recomputing it. `report_and_pages.yml` now passes the flag so dam risk is
  computed once per CI build (`tools/build_figure_data.py` remains the single
  producer). A CSV older than the catalog, missing, or invalid falls back to
  recomputing, so standalone `mmeq report` behavior is unchanged.
- Config tunables `RISK_WEIGHTS`, `RISK_GRADE_THRESHOLDS`, `GMPE_SIGMA_LN`
  (overridable via `MMEQ_RISK_W_*`, `MMEQ_RISK_GRADE_*`, `MMEQ_GMPE_SIGMA_LN`).
- Tests (64 → 77): hazard-curve monotonicity vs PGA level + exact
  `catalog_years` rate scaling; fragility lognormal median crossing = 0.5,
  monotonic damage-state exceedance, discrete-state normalization; grade
  threshold boundaries from config; a `cmd_report` dam-risk smoke test on a
  synthetic catalog (skips if `myanmar_dams.geojson` is absent); reuse +
  staleness tests for `--reuse-risk`.

### Changed
- `dam_risk.py`: composite weights (0.35/0.30/0.20/0.15), grade thresholds
  (7/5/3) and GMPE `sigma_ln` (0.65) are no longer inlined in
  `dam_risk_scores`, `sensitivity_analysis`, `compute_hazard_curve` and
  `monte_carlo_pga`; they come from `config.py`, and both scoring analyses now
  share `_component_scores()`/`_grade()` helpers so they cannot silently
  diverge (spec 003 P5).

## v2.0.2 — 2026-07-02

Scientific-audit follow-up (verified against Gardner & Knopoff 1974, OpenQuake,
King/Stein/Lin 1994, USBR/Pells & Fell fragility literature, and USGS/GFZ/Science
event data).

### Fixed
- **Declustering is now true Gardner-Knopoff (1974)** — `decluster_catalog`
  previously used a fixed 30-day/50-km window while the README/paper described it
  as "Gardner-Knopoff". It now applies the canonical magnitude-dependent windows
  (M7.7 → ~86 km / ~967 days), processing events largest-first; pass
  `window_days`+`distance_km` for the legacy fixed window. Declustered catalog:
  4,541 of 9,403 events; b ≈ 1.05 (was 1.06), Mc 4.7, M6 return period ~2.1 yr,
  475-yr PGA mean ~0.31g / 36 dams >0.5g — conclusions unchanged, method now
  matches its label. Dam risk grades unaffected (scenario-PGA based).
- **Wells & Coppersmith misattribution** — the ~475 km rupture length used in the
  aftershock spatial kernel is the *observed* 2025 rupture (USGS finite fault);
  W&C (1994) scaling predicts only ~160 km for M7.7 strike-slip. Comment corrected.

### Audited (no change needed)
- Data layer via SQL audit: 0 duplicate ids, monthly ↔ combined exactly in sync,
  chronologically sorted, physically sane bounds.
- ASK08 coefficients (OpenQuake), recorded PGAs (GFZ 1.07g / USGS 0.62g / 0.57g
  horizontal), Coulomb friction μ′=0.4 (King, Stein & Lin 1994), Hanks-Kanamori
  moment constant, Aki-Utsu b-value MLE, HAZUS-style lognormal dam fragility
  medians (within USBR / Pells & Fell empirical ranges).

## v2.0.1 — 2026-06-30

Correctness pass across the analysis pipeline and a data-export fix. Figures and
README numbers regenerated against the corrected code.

### Fixed
- **ASK08 large-distance coefficient** — `a18` was `-0.39`, should be `-0.0067`
  (and `a16` `0.70`→`0.90`). The wrong value collapsed PGA to the floor for any
  dam beyond ~100 km of the rupture, mis-grading distant dams as "Low". Dam risk
  grades changed accordingly (Critical 25 / High 148 / Moderate 67 / Low 14; was
  25 / 112 / 10 / 107). The rock-reference `PGA1100` now also includes the
  hanging-wall / ztor / large-distance geometry terms.
- **`generate_figures.py` used a second, broken GMPE** — fig5 (attenuation) was
  drawn with the pre-v2 model (`a1=-0.526`, no site/geometry terms); now uses the
  canonical `estimate_pga_ask08`.
- **Return period not annualized** — `compute_return_period` now divides the
  catalog a-value by the catalog span.
- **Monthly CSV duplicate accumulation** — monthly files are overwritten (full
  re-fetch each run) instead of appended; dedup keys on the stable event `id`.
  Cleaned 8,387 accumulated duplicate rows from existing exports.
- **CI export swallowed fetch failures** — `dataexport.py` now re-raises so a
  failed nightly fetch fails the job instead of committing missing data.
- **Omori fit dropped the first hour** of aftershocks; `K` now from the full-fit
  intercept (p = 0.93). tz-aware mainshock times handled.
- **Coulomb along-strike vector was mirrored** — corrected (triggered 34 / shadow
  163; was 69 / 127).
- **PSHA now uses a declustered catalog** — hazard rates (return periods, 475-yr
  PGA, hazard curves) were estimated on the raw catalog, where the 2025 aftershock
  sequence drove the auto-Mc to ~2.4 and b to ~0.35, inflating the hazard.
  `cmd_report` and the figures now decluster (Gardner-Knopoff) before fitting
  Gutenberg-Richter (Mc ~4.7, b ~1.06), and `compute_hazard_curve` receives the
  real catalog span. M6 return period ~2 yr; 475-yr PGA mean ~0.30g, 35 dams >0.5g.
- Smaller fixes: report PDF GMPE citation, `forecast_params=None` crash,
  ShakeMap log/div guard, negative-magnitude marker NaN, `--mc 0` honored,
  `MMEQ_MAX_WORKERS` wired up, DBSCAN UTM pinned to 47N.

### Paper / README
- Regenerated all 14 figures against the corrected pipeline (fig5 was being drawn
  by a second, pre-v2 broken GMPE) and updated paper/README/Pages numbers to
  match. Dam Vs30 is now sampled from the USGS ShakeMap grid (~230–875 m/s); the
  earlier "Copernicus DEM" generator was not in the repo.

## v2.0.0 — 2026-05-01

Major overhaul of calculations, data sources, and GitHub Pages.

### Fixed
- **ASK08 GMPE critically broken** — wrong coefficients (a1=-0.526→0.804), missing 7 of 9 formula terms, non-functional site response. Rewritten from OpenQuake reference.
- Declustering distance missing cos(lat) correction
- Sensitivity analysis using epicentral distance instead of fault distance
- Animated timeline not animating (missing Z suffix on timestamps)
- Missing `import pandas` in fragility.py (broke CI)
- Stale numbers in paper and README (earthquake counts, dam risk grades)

### Added
- **OSM building exposure** — 34,224 schools, hospitals, clinics from OpenStreetMap with PGA at each site
- **Site-specific Vs30** for buildings from USGS ShakeMap grid (288K points, 180–900 m/s)
- **HAZUS loss estimation** — casualty and damage ratios applied to building exposure
- **Township geocoding** — 330 townships (ADM3) + 74 districts (ADM2) from geoBoundaries
- **Village-level nearest place** — 74,028 OSM place nodes with KD-tree lookup
- **Seismic gap analysis** — cumulative moment release by latitude along Sagaing Fault
- **Aftershock spatial probability grid** — ETAS power-law kernel with Omori forecast
- **Coulomb stress transfer** from USGS finite fault model (530 patches)
- **Dam fragility curves** — HAZUS log-normal functions for 3 damage states
- **Monte Carlo PGA uncertainty** — 1000-iteration epistemic uncertainty
- **USGS ShakeMap validation** — NPW station predicted 0.51g vs observed 0.57g
- **GEM Global Active Faults** — 395 Myanmar faults, 9,675 segments
- **WorldPop population exposure** — one-off offline analysis (2.89M within 50km of epicenter); the 1 km raster is not tracked in-repo, so this figure is not reproduced by the pipeline
- **USGS finite fault model** — 530 slip patches, 0–7m variable slip
- **USGS rupture trace** — 475km surface rupture geometry
- New columns in earthquake exports: `state_region`, `district`, `township`, `nearest_city`, `place_type`, `distance_km`

### Changed
- Paper restyled as casual hobby write-up (17 pages, 14 figures)
- All map figures now have Myanmar state borders
- GitHub Pages redesigned with figures gallery, data explorer, year filtering
- README rewritten with educational sections
- CI/CD workflows fixed (cache key, mmeq command, trigger paths)

### Data
- 9,242 earthquake events (1970–2026)
- 254 dams scored: 25 Critical, 112 High, 10 Moderate, 107 Low
- 34,224 OSM buildings: 2,166 schools and 901 hospitals above 0.1g PGA

## v1.0.0 — 2025-04

Initial release. Data pipeline, basic dam risk scoring, interactive maps, CI/CD.
