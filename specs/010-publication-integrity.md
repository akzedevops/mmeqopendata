---
spec: 010
title: Publication integrity, station validation, and automation monitoring
status: Done
created: 2026-11-12
---

## Problem

The sensitivity download is not published, headline counts are hard-coded, and
successful builds do not check the site's local links. The station validation
uses distance to unrelated active faults, sometimes selects vertical PGA, ignores
quality flags and station Vs30, and labels modeled dam PGA as observations.
Consequently the paper's station table and interpretation are not defensible.
The building stage also mixes unrelated faults into scenario distance; figure 5
plots dam PGA against fault rather than rupture distance. Hazard exports label
annual rates as probabilities, and figure 11 colors markers at a different
threshold from its legend. The paper incorrectly attributes the default Coulomb
calculation to the archived variable-slip finite-fault model.

## Design

- Validate only instrumental station observations using the event-specific USGS
  rupture distances and station Vs30. Compare the geometric mean of two clean
  horizontal PGA components, explicitly an approximation to AS08's GMRotI50.
  Exclude flagged observations and identify predictions outside the model's
  approximate 200 km distance range. Never count modeled dam values as recordings.
- Generate publication statistics and the paper's station table from the same
  catalog/report artifacts. Publish the sensitivity CSV outside hazard_curves,
  whose contents are regenerated. Check local download links before deployment.
- Add independent scheduled freshness checks and issue-based report-failure
  notification. Avoid peak-of-hour scheduling; do not claim cron punctuality.
- Document the scientific audit, sources, verified results, and remaining model
  limitations without claiming independent peer review or dam-safety certification.

## Acceptance criteria

- [x] Regression tests reject vertical-only, flagged, and synthetic observations.
- [x] KTN uses the supplied 370.594 km rupture distance, not a nearby unrelated fault.
- [x] Publication counts and station table are generated, not manually frozen.
- [x] Sensitivity download exists and local link validation catches missing files.
- [x] Freshness checks distinguish pipeline heartbeat from latest earthquake time.
- [x] Python lint/tests, Go tests, workflow validation, and affected report stages pass.
- [x] README, paper source/PDF, and affected figures reflect the corrected results.

## Scope and scientific impact

Station comparisons, building scenario PGA, the annual probability export, and
figure classifications change. Deterministic dam PGA and PSHA annual rates are
not recalibrated to these sparse recordings. The 80-case OpenQuake parity check
is recorded as an independent reference fixture. Input catalogs and their
32-column schema are preserved. Changes remain local for review.
