---
spec: 006
title: Hazard-model corrections — scenario rupture distance, PSHA source model, Coulomb kernel
status: In progress    # Draft | Approved | In progress | Done
author: Claude (2026-07-06 integrity audit) + Aung Khant Zaw
created: 2026-07-06
---

## Problem / motivation

The 2026-07-06 full-repo integrity audit confirmed (adversarially, with
independent reproduction) three modeling defects that make published numbers
wrong, all upstream of the (verified-correct) ASK08 GMPE:

1. **C2 — "rupture distance" is not rupture distance.** The deterministic 2025
   M7.7 scenario PGA per dam uses the distance to the nearest segment of
   `fault_lines.json` — a **global plate-boundary** trace file (6,051 segments,
   lat −66…+86). Dams near fault sections that did **not** rupture (northern
   Sagaing continuation in Kachin, Andaman/Sunda boundary near Tanintharyi) get
   near-fault M7.7 PGA: Ta Nai Hka, ~435 km from the actual rupture, is
   published at 0.444 g / Critical — inflated up to 64× for ~46 dams.
2. **C3 — PSHA collapses the whole regional rate onto one distance.**
   `compute_hazard_curve` integrates over magnitude only: the full
   Gutenberg-Richter rate of the ~2,000×950 km catalog box is applied at the
   dam's single nearest-fault distance. A dam 1 km from any fault trace is
   assigned the recurrence of every M5–8 event in the region at 1 km,
   inflating near-fault 475-yr PGA to ~2.1–2.5 g vs ~1.0 g on-fault in the
   dedicated Myanmar PSHA literature (Thant et al. 2023, Geoscience Letters).
3. **C4 — Coulomb kernel violates point-source parity.**
   `_patch_coulomb_stress` uses `sin2α = 2·along·√(perp²+depth²)/r²` — an
   unsigned |sin| that makes the shear term odd in the along-strike
   coordinate, where static stress from a point moment source must satisfy
   σ(−r) = σ(+r) (Aki & Richards 2002, the module's own reference). The summed
   field is systematically positive beyond the northern rupture tip and
   negative beyond the southern one (measured 25–70× N/S asymmetry), erasing
   the positive lobe toward Naypyidaw/Bago and driving the published
   34 triggered / 163 shadow dam counts.

Related: **M7** — the 250 hazard-curve CSVs on the dashboard are frozen
pre-declustering artifacts no pipeline stage regenerates; they contradict the
paper's PSHA numbers.

## Design

### C4 (coulomb.py) — minimal parity-correct kernel

- `sin2α = 2·along·perp / r²` (signed perp) and, for angular consistency,
  `cos2α = (along² − perp²) / r²`. Both terms are then even under
  (along, perp) → (−along, −perp), restoring the four-lobed King-Stein-Lin
  pattern. The depth² in r² remains as near-field damping.
- Classification threshold raised from ±0.001 MPa to **±0.01 MPa (0.1 bar)**,
  the standard triggering threshold (King et al. 1994; Stein 1999) —
  0.001 MPa is well inside tidal-stress noise.

### C2 (dam_risk.py) — scenario Rrup from the actual rupture geometry

- New `_load_rupture_trace()` reads the USGS rupture polyline(s) from
  `data/shakemap/rupture.json` (already in the repo; the Coulomb stage uses
  it) into the same segment format as `_load_fault_segments`.
- Scenario PGA in `dam_risk_scores`, `monte_carlo_pga` and
  `sensitivity_analysis` uses `dist_to_rupture_km` = distance to the nearest
  rupture segment (existing `distance_to_nearest_fault` helper). The
  nearest-mapped-fault distance keeps its screening role in `fault_score`
  only, and the two distances are distinctly named in the output CSV.
- Fallback when the rupture file is missing: previous behavior (nearest fault,
  else epicentral), with a WARNING.

### C3 (dam_risk.py) — Cornell-McGuire with a spatial source model

- Frankel (1995)-style smoothed seismicity: declustered events ≥ Mc binned on
  a 0.5° grid; per-cell annual rates Gaussian-smoothed (σ = 50 km); total
  regional rate conserved. Per site: λ(PGA>x) = Σ_cells Σ_m rate_cell(m-bin) ·
  P(PGA>x | m, r_cell→site) with the truncated-G-R bin rates anchored at Mc
  (b from the declustered catalog) and r floored at 5 km.
- `compute_hazard_curve` signature becomes site-based
  (`site_lat`, `site_lon`, `vs30`, declustered catalog, b, Mc,
  `catalog_years`); `generate_figures.py` fig10 switches to real dam
  coordinates.
- New report stage (`--no-hazard` flag) writes per-dam
  `hazard_curves/<name>.csv` (deduplicated filenames) plus an `index.html`
  manifest — replacing the frozen artifacts and fixing the dead directory
  link (M7 + minor).

## Data & outputs impact

`dam_risk_scores.csv` (new column, changed PGA/grades), `monte_carlo_pga.csv`,
`sensitivity_analysis.csv`, `coulomb JSON` + fig11, fig10, fig4/6/7/13,
`hazard_curves/*.csv` (regenerated at last), README dam-grade/PSHA/Coulomb
numbers, paper sections 'Results', 'Probabilistic Hazard', 'Coulomb'.

## Acceptance criteria

- [ ] Parity unit test: for a single patch, ΔCFS(−r) == ΔCFS(+r) to machine
      precision; a strike-parallel receiver field shows 4 alternating-sign
      lobes around one patch.
- [ ] Regenerated Coulomb dam counts published with the ±0.01 MPa threshold;
      README/paper/fig11 updated together.
- [ ] `dam_risk_scores.csv` carries `dist_to_rupture_km`; Ta Nai Hka's
      scenario PGA drops below 0.05 g and its published grade is recomputed
      accordingly; no dam >300 km from the rupture keeps a near-fault
      scenario PGA.
- [ ] Smoothed-seismicity PSHA conserves the total regional rate to <1%,
      near-fault sites exceed remote sites by >10×, and the collapsed-distance
      inflation (~2.1–2.5 g on-fault) is gone. NOTE (decision recorded during
      implementation): the committed GEM faults file carries no slip rates, so
      a fault-source term is not supportable from repo data — the model is
      catalog-only smoothed seismicity and REPORTS LOWER on-fault values
      (~0.1–0.2 g at 475 yr) than fault-source-informed studies (Thant et al.
      2023, ~1 g on-fault). The paper/README must state this as a known lower
      bound rather than quoting the old inflated range; README/paper PSHA
      numbers and fig10 regenerated from the new model.
- [ ] A report stage regenerates `hazard_curves/*.csv` + manifest; the
      dashboard's Browse link resolves.
- [ ] `ruff check .` clean; full pytest suite green with new regression tests
      for the kernel parity, rupture-distance selection, and rate
      conservation.

## Risks / rollback

- The three fixes lower most published hazard numbers; the paper's headline
  ("more than two-thirds of dams Critical or High") may weaken — the paper
  text must follow the numbers, not vice versa.
- The ad-hoc point-source Coulomb kernel remains an approximation (no Okada
  rectangular dislocation); the spec fixes its symmetry, not its rigor — the
  paper's Limitations section already says this and keeps saying it.
- Rollback: single revert of the implementing PR restores prior behavior;
  artifacts regenerate on the next CI build.
