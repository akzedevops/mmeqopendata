# Specs — spec-driven development for mmeq-opendata

This project develops non-trivial changes **spec-first**. A spec is a short, reviewable
contract written *before* code. It keeps scientific/data changes honest: you state what
must be true, then make it true, then prove it.

## The loop

```
  ┌─────────────────────────────────────────────────────────────┐
  │  1. SPEC      write specs/NNN-slug.md (problem, design,       │
  │               acceptance criteria, data/figures impact)       │
  │                          │                                    │
  │  2. PLAN      break into steps; identify files & risks        │
  │                          │                                    │
  │  3. IMPLEMENT edit src/mmeq/, wire through cli.py             │
  │                          │                                    │
  │  4. VERIFY    pytest + run the affected stage; check numbers  │
  │                          │                                    │
  │  5. REVIEW    diff vs spec; domain correctness; docs in sync  │
  │              ┌───────────┴───────────┐                        │
  │        criteria fail            criteria pass                 │
  │              └──► back to 3         └──► mark Done, commit     │
  └─────────────────────────────────────────────────────────────┘
```

Run it with the **`/spec`** slash command (`.claude/commands/spec.md`). It will scaffold a
new spec from `TEMPLATE.md`, then drive steps 2–5 until every acceptance criterion is met
(or it surfaces a blocker for you to decide).

## Conventions

- One spec per change: `specs/NNN-short-slug.md` (zero-padded incrementing number).
- Status lives in the spec front-matter: `Draft → Approved → In progress → Done`.
- A spec is **Done** only when every acceptance criterion is checked and `pytest` is green.
- Specs are durable records — keep them after merge; they document *why*, which the diff can't.
- For scientific changes, every acceptance criterion must be **measurable** (a number, a
  test, a figure that regenerates) — not "looks right".

## Index

| Spec | Title | Status |
|------|-------|--------|
| [001](001-data-fetch-upgrade.md) | Upgrade the earthquake data-fetch/export system | Draft |
| [002](002-go-export-rewrite.md) | Rewrite the fetch/export CLI in Go (separate branch) | Draft |
| [003](003-project-improvements.md) | Project improvements — CI test gate, de-duplication, structure | Draft |
</content>
