---
spec: 009
title: Validate the PSHA / ASK08 output against published Myanmar hazard studies
status: Done           # Draft | Approved | In progress | Done
author: Claude (6-agent research workflow) + Aung Khant Zaw
created: 2026-07-07
---

## Problem / motivation

The integrity work fixed the *bugs*; this validates the *geophysics* against the
external literature — is our published hazard consistent with independent
Myanmar PSHA studies, or wrong beyond the documented lower-bound caveat? Run as a
6-agent research + adversarial workflow (4 literature miners, 1 synthesizer, 1
adversarial reviewer).

## Findings

**GMPE (ASK08) — SOUND.** Our scenario PGA reproduces the 2025 M7.7 Naypyidaw
recording (0.51 g predicted vs 0.57 g observed, ratio 1.12 ≈ 0.2σ), matches
OpenQuake to 0.04%, and agrees with independent NGA-West2 checks that bracket
NPW within ±1σ. Caveats (already in the paper): the station-validation is n=6
with a low median obs/pred ratio (0.18) driven by distant-station
over-prediction, and NPW is a rock-prediction-vs-soil-observation comparison.

**PSHA source model — comparable to reference-rock, not a blanket lower bound.**
The synthesizer initially claimed our on-fault rock 475-yr PGA was ~0.08–0.10 g
(≈2–4× below GEM). **The adversarial reviewer refuted this, and re-computation
confirmed the refutation:** the seismicity-density peak is *not* on the Sagaing
fault trace (where the naive sample was taken) but near ~94.8°E, where the
portfolio maxes at **0.437 g rock / 0.559 g soil** (Myaing Chaung). So:

- Our **rock** 475-yr peak ~0.44 g is **comparable to GEM v2018.1 reference-rock**
  for the Sagaing corridor (~0.2–0.55 g), not far below it.
- Our **soil** peak ~0.56 g is ~2× below **Yang et al. (2023)**'s site-amplified
  >1 g near-fault values — a gap consistent with the absent fault-source
  recurrence term (18–20 mm/yr Sagaing slip, ~200–280 yr characteristic M7.7),
  possibly offset locally by the 2025 M7.7 inflating the 56-yr catalog rate.

Published benchmarks gathered: **Yang, H.-B. et al. (2023)**, *Geoscience
Letters* 10:48 — full fault-source PSHA (430 EOS faults, Sagaing central-segment
18 mm/yr, OpenQuake), Sagaing-corridor 475-yr >1 g **soil**; **GEM v2018.1** —
Mandalay/Naypyidaw ~0.2–0.35 g and fault-trace ~0.35–0.55 g at **reference rock**
(475-yr).

## Actions taken (this spec)

- **Citation corrected.** The paper cited "Thant, M. et al. (2023), Geoscience
  Letters 10:56" — wrong lead author AND article number. CrossRef confirms
  **Yang, H.-B. et al. (2023), Geoscience Letters 10:48** (Myo Thant is 6th of 8
  authors). Fixed in README + paper reference + inline.
- **PSHA-vs-literature sentence rewritten** from a vague "catalog-only lower
  bound" to the verified benchmark (rock ~0.44 g ≈ GEM reference-rock; ~2× below
  Yang soil >1 g), which is both more accurate and adds real validation value.
  The adversarial reviewer's warning — do NOT inject the synthesizer's false
  "~0.08–0.10 g / 2–4× below GEM" numbers — was heeded.
- **Stale hazard-curve orphans cleaned.** `docs/report/hazard_curves/` held 311
  CSVs for 254 dams (~57 orphan slugs from prior runs, misstating any
  directory-level aggregate). The `mmeq report` hazard stage now clears the
  directory before regenerating; the committed set is regenerated to 254.

## Acceptance criteria

- [x] GMPE soundness confirmed against the recording + independent GMPE checks.
- [x] Our rock/soil 475-yr peaks benchmarked like-for-like against GEM rock and
      Yang soil; direction/magnitude of any gap explained.
- [x] Adversarial reviewer's refutation reproduced by direct recomputation; no
      false number entered the paper.
- [x] Citation corrected (verified via CrossRef); hazard-curve orphans removed +
      stage fixed; `ruff` + `pytest` green.

## Notes

The workflow's value was the adversarial step: the first-pass synthesis built a
"2–4× below GEM" verdict on an on-fault number that was wrong (sampled the fault
trace, not the density peak); the skeptic caught it and re-computation confirmed
the model is actually *comparable to* reference-rock near the source. Lesson
carried into the paper text: benchmark like-for-like (rock↔rock, soil↔soil) and
at the model's actual hazard peak, not an assumed on-fault location.
