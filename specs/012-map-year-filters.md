---
spec: 012
title: Year-filtered maps with responsive explorer controls
status: Done
created: 2026-11-12
---

## Problem

The static earthquake map and animated timeline cannot isolate a year or range
of years. Large cumulative catalogs obscure recent events, while the map titles,
legend, and controls have inconsistent layout and styling.

## Design

- Share a compact, accessible explorer panel between both Folium maps, with UTC
  From/To year selectors, Latest year and All years buttons, an event count,
  magnitude legend, and responsive styling. Preserve OSM attribution and local assets.
- Filter static earthquake markers and heatmap together, including clustered,
  unclustered, and optional-layer modes. Do not filter dams, faults, or buildings.
- Pause animation on filter changes, rebuild its event layer and monthly timeline
  from selected events, and restart at the first selected event. No events from
  outside the selected years may remain visible. Restore full data on reset.
- Include gap years, show an explicit empty state, and hide empty playback controls.
  Enforce From <= To and use UTC consistently at year boundaries.
- Keep the numeric timestamp-sort correction. Do not alter the research catalog,
  scientific calculations, or generated scientific statistics.
- Give the cumulative animation a duration longer than the entire catalog span:
  TimeDimension's undefined duration otherwise starts at epoch zero and silently
  drops pre-1970 events. All selected events still remain visible through the end.

## Acceptance criteria

- [x] Both maps filter a single year and inclusive year range; presets/reset work.
- [x] Static markers and heatmap counts agree; non-earthquake layers are unchanged.
- [x] Animation plays/seeks after filtering, excludes outside-year events, and resets.
- [x] Gap years, singleton data, invalid/empty inputs, UTC boundaries, and missing
  optional layers have deterministic behavior and regression coverage.
- [x] Keyboard-accessible controls and attribution fit desktop/mobile viewports.
- [x] No remote script/style dependencies or unsafe data interpolation are added.
- [x] Lint/tests pass; full-catalog browser interactions produce no JavaScript errors.

## Verification

Ruff and all 225 Python tests pass (575 existing NumPy/pandas warnings).
`tools/check_map_filters.cjs` passes against the full 10,082-event catalog and
gap-year/pre-1970 fixtures, including unclustered markers, heatmap-only, and
disabled earthquake-layer modes. The check independently compares rendered
counts with input year counts, blocks network requests, exercises playback and
reset, and checks keyboard focus and 390x844 control/attribution bounds.
Desktop (1400x900) and mobile screenshots were also inspected with real OSM
tiles. Browser checks are optional local verification, not part of Python CI.

## Release

Branch + PR. The report workflow rebuilds both HTML maps after merge. This spec
does not authorize changes to model outputs or catalog records.
