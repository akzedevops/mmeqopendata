---
description: Spec-driven development loop — draft a spec, then implement → verify → review until acceptance criteria pass
argument-hint: <feature description, or a path to an existing specs/NNN-*.md>
---

You are running the **spec-driven agent loop** for the `mmeq-opendata` project (a Myanmar
earthquake / dam seismic-risk Python data pipeline). Read `CLAUDE.md` and
`specs/README.md` first if you have not already this session — they define the project
conventions and the loop you are executing.

Input: `$ARGUMENTS`

## How to run the loop

### 0. Locate or create the spec
- If `$ARGUMENTS` is a path to an existing spec, open it and resume from its status.
- Otherwise, treat `$ARGUMENTS` as a feature request:
  - Pick the next number `NNN` (look at existing `specs/NNN-*.md`, zero-padded).
  - Copy `specs/TEMPLATE.md` to `specs/NNN-<slug>.md` and fill every section from the
    request and your reading of the code. Use today's date.
  - If the request is ambiguous on scope, data source, or acceptance criteria, ask the
    user **before** writing code (use AskUserQuestion for genuine forks only — pick sane
    defaults otherwise and note them in the spec).
- Set status to `Approved` once the spec is concrete and acceptance criteria are
  measurable. Add a row to the index table in `specs/README.md`.

### 1. Plan
- Break the spec into ordered, verifiable steps. Identify the exact files under
  `src/mmeq/` to touch and whether `cli.py`/`config.py` need wiring.
- Use the TaskCreate/TaskUpdate tools to track steps. Flag domain-correctness risks
  (GMPE/distance/Vs30/projection) from the start.

### 2. Implement
- Edit code in `src/mmeq/` (never the legacy root scripts for new work — see CLAUDE.md).
- Add new constants to `config.py`, new flags to `cli.py`. Match existing style: stdlib
  `logging`, graceful `try/except` around heavy/optional deps and network stages.
- When unsure of a library API (geopandas, scipy, scikit-learn, folium, plotly, pandas),
  fetch current docs via Context7 — do not guess signatures.

### 3. Verify
- Run `pytest tests/ -v`. Add/extend tests for new analysis functions.
- Run the affected stage in isolation (e.g. `mmeq report --no-pdf --no-animated ...` or
  the specific `mmeq analyze --type ...`) and check printed summary numbers land in the
  ranges the spec predicted.
- Tick acceptance-criteria checkboxes in the spec only when actually demonstrated.

### 4. Review
- Diff the change against the spec's design and acceptance criteria. Run `/code-review`
  on the working diff for correctness bugs.
- Confirm derived numbers stay in sync: if results changed, update README tables, the
  paper, and regenerate figures (`python generate_figures.py`) as the spec's "Data &
  outputs impact" requires.
- **Loop:** if any criterion fails or review finds a real bug, return to step 2. Repeat
  until all acceptance criteria are checked and `pytest` is green.

### 5. Close out
- Set spec status to `Done`; update the `specs/README.md` index row.
- Summarize what changed, the verification evidence (test output, key numbers), and any
  follow-ups. Do **not** commit, push, or open a PR unless the user explicitly asks.

## Guardrails
- This pipeline auto-deploys to a public GitHub Pages dashboard on push to `master`
  touching `src/mmeq/**`, `data/**`, or `quake_exports/**`. Be conservative; never push
  on your own initiative.
- Scientific correctness over speed. A change that makes the math wrong but the tests
  green is a failure — verify against the domain notes in CLAUDE.md.
- Keep going through the loop autonomously; only stop to ask the user at genuine decision
  forks or true blockers.
</content>
