# Changelog

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
- **WorldPop population raster** — 2.89M within 50km of epicenter
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
