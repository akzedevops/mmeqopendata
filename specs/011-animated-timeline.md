---
spec: 011
title: Restore chronological animation and visible timeline controls
status: Done
created: 2026-11-12
---

## Problem

The deployed animated map has 10,082 features but only one available animation
frame (August 2001). Leaflet.TimeDimension 1.1.1 sorts numeric epoch timestamps
lexicographically in `sort_and_deduplicate`. Historical timestamps on opposite
sides of the September 2001 twelve-to-thirteen-digit millisecond boundary therefore
produce reversed range endpoints. The monthly range collapses to one frame.
The heading also occupies document flow above a full-height map, pushing the
bottom playback controls below the viewport. Prior tile-loading checks did not
exercise playback and missed these failures.

## Design

- Override the affected utility with numeric sorting and exact deduplication
  before TimeDimension initializes; retain the pinned upstream vendor files.
- Keep the title out of document flow and allow timeline controls to wrap on
  narrow screens, without changing earthquake observations or scientific output.
- Add regression tests around the epoch/2001 boundary and verify actual browser
  playback, seeking, and control visibility, not just map tiles or asset URLs.

## Acceptance criteria

- [x] Utility sorts negative, zero, and pre/post-2001 timestamps numerically,
  removes duplicates, and does not mutate its input.
- [x] Generated HTML installs the correction before TimeDimension initializes.
- [x] Full-catalog timeline spans March 1970 to November 2026 with 682 monthly
  frames, rather than one frame in August 2001.
- [x] Play advances the timestamp and seeking changes the displayed events.
- [x] Playback/date controls remain within desktop and mobile viewports.
- [x] Python lint/tests pass and browser playback produces no JavaScript errors.

## Verification

Ruff and all 211 Python tests pass (575 unrelated NumPy/pandas warnings). The
timestamp regression executes JavaScript with Node.js when available and is
explicitly skipped without Node.js; the generated-script order test always runs.
Chromium with the full catalog starts at frame 0 with one event, advances to
frame 3 when Play is clicked, and reaches frame 681 with all 10,082 events when
the slider is dragged to its end. No JavaScript errors occur. The control bar
fits within both 1200x800 and 390x844 viewports. These are local browser checks;
deployment is verified separately after merge.

## Scope

Presentation and animation only. Catalog schema, scientific analyses, paper,
and derived risk/exposure numbers remain unchanged. Generated HTML is rebuilt
by the report workflow after merge.
