---
spec: NNN
title: <short imperative title>
status: Draft            # Draft | Approved | In progress | Done
author: <name>
created: <YYYY-MM-DD>
---

## Problem / motivation

<What's wrong or missing, and why it matters. For scientific changes, state the
correctness concern explicitly. Link the issue, paper section, or CHANGELOG entry.>

## Goal

<One or two sentences: what "done" looks like, in plain terms.>

## Non-goals

<What this deliberately does NOT do, to bound scope.>

## Design

<How it works. Which modules under src/mmeq/ change, new functions/signatures, data
flow, config keys added to config.py, CLI flags added to cli.py. Cite any
seismology/GMPE source (author, year) for new ground-motion or site terms.>

## Data & outputs impact

- Input data needed: <files under data/ or external API>
- Generated artifacts affected: <quake_exports/**, docs/report/**, paper/figures/**>
- Derived numbers that change: <README tables, paper values — list them so they get
  updated together>

## Acceptance criteria

Each must be measurable (a test, a number with a range, or a regenerated artifact).

- [ ] <e.g. `pytest tests/test_seismology.py` passes with a new test for X>
- [ ] <e.g. `mmeq report --no-pdf` prints b-value in 0.6–1.1 at Mc≥4.5>
- [ ] <e.g. README dam-risk grade counts match the new dam_risk_scores.csv>
- [ ] No regression: full `pytest tests/ -v` green.

## Risks / rollback

<What could break (the public dashboard ships on push to master), and how to revert.>

## Notes

<Decisions made during implementation, links to references fetched via Context7, etc.>
</content>
