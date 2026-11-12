# Scientific and automation audit — 2026-11-12

This is a reproducibility and implementation review, **not independent peer
review, a comprehensive validation of all geophysics, or a dam-safety assessment**.
The baseline is commit `6817408`; the catalog remains unchanged at 10,082 events,
latest origin time 2026-11-11 02:38:05 UTC. Historical conclusions in spec 009 and
the 2.1.0 changelog must be read with the corrections here.

## Confirmed defects and corrections

### Station comparison

The previous comparison measured distance to the nearest of the event rupture,
GEM faults, and other fault traces. This assigned KTN 2.3 km even though the
USGS event-specific rupture distance is **370.594 km**. The code selected the
first PGA channel (sometimes vertical), ignored observation flags and station
Vs30, and appended modeled dam PGA as if it were observed. Consequently the
paper's claimed 50-fold KTN overprediction and speculation that KTN was a grid
estimate were unsupported.

The corrected comparison:

- Uses `distances.rrup`, `distances.rjb`, and `vs30` from the instrumental station list.
- Uses the geometric mean of two orthogonal, positive, finite, unflagged
  horizontal PGA peaks in `%g`, converted to g. It never substitutes a vertical channel.
- Excludes PANO because its component PGAs are flagged `Outlier`.
- Excludes modeled dam outputs from observed-data validation.
- Marks distances greater than 200 km as extrapolations, not in-range validation.

| Station | Rrup (km) | Vs30 (m/s) | Horizontal observed (g) | Predicted (g) | Obs/pred |
|---|---:|---:|---:|---:|---:|
| NPW | 2.751 | 309.64 | 0.59823 | 0.47824 | 1.2509 |
| NGU | 113.953 | 325.24 | 0.04915 | 0.07080 | 0.6942 |
| YGN | 157.136 | 262.52 | 0.02128 | 0.05372 | 0.3962 |
| CHTO | 272.222 | 639.85 | 0.01021 | 0.01731 | 0.5901 |
| KTN | 370.594 | 400.41 | 0.01319 | 0.01231 | 1.0714 |

AS08 predicts **GMRotI50**, not the geometric mean of two independently picked
component maxima. Without time histories, the latter is only an approximation.
Only three retained stations are within 200 km. This small, single-event sample
does not establish regional GMPE validity or calibrate any downstream fragility
or hazard model. Predictions retain the vertical strike-slip scenario assumptions.

### Building exposure and figures

Building distance also combined the actual rupture with unrelated plate
boundaries. It now uses only the 2025 event rupture projection, consistently
with the dam scenario. Surface-projection distance approximates rupture-plane
distance for a near-vertical fault; it is not a general 3D rupture solver.

| Modeled result | Before | Corrected |
|---|---:|---:|
| Severe (VIII) OSM elements | 5,714 | 3,395 |
| Very Strong (VII) elements | 6,153 | 5,864 |
| Strong (VI) elements | 8,061 | 7,058 |
| Schools with PGA > 0.1g | 2,166 | 1,674 |
| Hospitals with PGA > 0.1g | 901 | 583 |

These are **scenario predictions**, not observed damage or shaking. OSM elements
are not necessarily unique buildings, and completeness is not established.
Figure 5 now plots dam PGA against event-rupture distance, removes fabricated
5-km placements of individual component peaks, and uses the corrected station
comparison. Figure 11 colors markers from the same exported stress regime used
in its legend, rather than mixing 0.001 and 0.01 MPa thresholds.

### Hazard probabilities and unsupported claims

The hazard export's `annual_prob` previously duplicated `annual_rate`. It now
uses the Poisson probability `-expm1(-annual_rate)`. Annual rates, return periods,
and the PSHA source model are unchanged. The regenerated 475-year PGA range is
0.002–0.562g, mean 0.143g; 172 dams exceed 0.1g and 46 exceed 0.2g.

The paper incorrectly attributed the default Coulomb computation to the
archived 530-patch variable-slip model. The actual code uses an idealized
seven-segment trace with uniform moment-derived slip, then subdivides it. The
paper now describes this accurately and does not interpret crustal-receiver
stress classes as dam structural loads. This audit does not establish the
physical accuracy of that approximate stress kernel.

Unreproducible WorldPop totals, unsupported disaster totals, and injury/collapse
forecasts from assumed occupancy and loss coefficients are removed as verified
results. The exploratory loss helper remains in the code but is not a verified
HAZUS implementation. Heuristic Critical/High grades are not failure probabilities.

## Independent AS08 implementation check

An isolated OpenQuake 3.26.2 calculation produced 80 reference PGA values:

- Magnitudes: 5.0, 6.5, 7.7, 8.0.
- Rrup = Rjb: 1, 10, 50, 100, 200 km; Rx = -1 km.
- Vs30: 180, 310, 760, 1100 m/s; inferred sites; default basin depth (`z1pt0=-999`).
- Rake 180°, dip 90°, Ztor 0 km, width 15 km.

Maximum relative difference from `estimate_pga_ask08`: **8.9 × 10⁻¹⁶**.
The fixture is `tests/fixtures/as08_openquake.json`; ordinary tests need no
OpenQuake dependency. Regenerate it with `tools/build_as08_reference.py` in a
separate OpenQuake environment. This verifies the tested implementation cases,
**not** regional suitability, distant extrapolations, the fixed sigma, or the
local simplified hanging-wall term for dipping faults.

## Publication and automation

- `mmeq publish` generates the catalog/year counts, screening and exposure
  summary, paper macros/tables, and `docs/report/publication.json` with the
  input catalog SHA-256. The article's key catalog/grade/hazard/exposure numbers
  now come from report artifacts rather than frozen prose.
- Sensitivity output is published as `docs/report/sensitivity_analysis.csv`,
  outside the hazard directory that is cleared on regeneration.
- The homepage uses JSON and DOM `textContent` for dam names instead of naive
  CSV splitting and `innerHTML`; comma-containing names and markup stay literal.
- CI clears generated report outputs first, regenerates publications, builds
  the PDF, and checks local homepage/hazard-index links before deployment.
- Report failures create or update an issue. The daily fetch moves from 00:00
  to 00:17 UTC to avoid the top-of-hour peak; this does not guarantee punctuality.
- A separate daily monitor checks 48-hour scheduled-job heartbeats and the
  deployed catalog hash. It checks pipeline activity, **not** time since the last
  earthquake, and defers hash comparison while deployment is active.
- A GitHub-hosted monitor cannot detect its own disabled schedule or a platform-wide
  outage reliably. An external uptime/heartbeat service remains a recommendation.

## Verification and remaining limits

The review includes Python lint/regressions, Go race tests and vet, actionlint,
the complete report/figure build, a rebuilt paper PDF, local download validation,
and Chromium checks of publication counts, year counts, the dam table and
markup-safe rendering. All 682 existing CSV/JSON pairs had matching event IDs;
that verifies export consistency, **not** physical-event completeness or absence
of cross-network duplicates. The catalog itself was not changed.

Validation results: **205 Python tests passed**, Ruff/actionlint and Go vet/race
tests passed, and the revised PDF compiled without TeX warnings. Python still
reports 578 dependency/test warnings (mostly NumPy timedelta deprecations and
Carto basemap API-key notices); these are not treated as proof of future
compatibility. GitHub returned zero open Dependabot and CodeQL alerts at review
time; that is not a comprehensive security guarantee. The new workflows have
been checked locally, not executed on GitHub or deployed by this audit.

Important remaining modeling limits are mixed magnitude scales, changing
completeness, a single regional recurrence treatment, simplified source depths,
no fault-source recurrence term, fixed ground-motion variability, site proxies,
uncalibrated dam fragility/weights, and sparse station coverage. The paper now
states these limits rather than treating plausible numbers as validation.

## Primary sources consulted

1. [USGS event details](https://earthquake.usgs.gov/earthquakes/feed/v1.0/detail/us7000pn9s.geojson)
   and [instrumental station product](https://earthquake.usgs.gov/product/shakemap/us7000pn9s/us/1749216983926/download/stationlist.json).
   The six event-specific station distances in the live product match the tracked file.
2. [OpenQuake AS08 reference implementation](https://github.com/gem/oq-engine/blob/master/openquake/hazardlib/gsim/abrahamson_silva_2008.py):
   component definition, required distances/site parameters, coefficients, and numerical comparison.
3. Yang et al. (2023), [Probabilistic seismic hazard assessments for Myanmar and its metropolitan areas](https://doi.org/10.1186/s40562-023-00301-x),
   *Geoscience Letters* 10:48. Bibliographic identity was checked with Crossref and
   the article was read for its fault/area/subduction sources and site treatment.
   Unlike this repository, it has multiple source categories; range similarity is
   not like-for-like validation. The old numerical GEM-range claim was not
   independently reproduced and is no longer presented as validation evidence.
4. [GitHub workflow events](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows),
   consulted through Context7 MCP: delayed/dropped schedules, automatic inactivity
   disabling, and workflow trigger behavior.
