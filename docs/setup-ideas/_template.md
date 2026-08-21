---
id: IDEA-NNN
title: Short human title
status: Idea            # Idea | Researched | Prototyped | Backtesting-ready | Backtesting | Validated | In Playbook | Rejected
regime: [any]          # see "Regime vocabulary" below
related: []            # e.g. [IDEA-000, IDEA-020] — must match existing IDEA files
companionSpecs: []     # e.g. [docs/idea000-idea012-backtest-runbook.md]
mcpPointers: []        # tool + setupId pointers; NEVER cache stats in prose
hypothesisAnchor: false # true only for IDEA-000 (carries the hypothesis-anchor comment)
---

# IDEA-NNN — {title}

> Per-idea detail file. The hub ([setup-ideas-and-backtesting.md](../setup-ideas-and-backtesting.md)) keeps a short stub (status, source, framing, detail link) pointing here.

> **New-idea starter.** `Thesis` / `Mechanics` / `Status / verdicts` are the recommended outline when creating a file from this template. Migrated IDEA-000–023 keep the hub's original `**Concept:**` / `**Setup:**` headings. Do not retrofit those files to this outline.

## Thesis
What the setup is and why an edge should exist. Auction / order-flow mechanics.

## Mechanics
Location → trigger → confirmation → invalidation → risk state.

## Status / verdicts
Narrative of where this stands. **No bare win rates.** Any numeric performance claim must carry an `mcpPointers` entry, a `jobId`, or an explicit `<!-- stats: point-in-time -->` marker — pull fresh numbers from MCP/SQLite at query time. Literature figures must name the source and must not be quoted as Desk expectancy.

## Unknowns (optional, new ideas only)
What is not yet known, and what would change the design if answered differently. Not a retrofit requirement for IDEA-000–023.

## Invalidation (optional, new ideas only)
What would kill the idea (structure, sample, or logical inconsistency between gate and entry). Not a retrofit requirement for IDEA-000–023.

## See also
- Hub stub: [setup-ideas-and-backtesting.md#idea-nnn](../setup-ideas-and-backtesting.md#idea-nnn)

---

## Frontmatter reference

### Regime vocabulary
`regime:` values should come from the `Regime` enum in [`src/pipelines/regime.rs`](../../src/pipelines/regime.rs) (serde `camelCase`):

- `oneSidedAcceptance`
- `migration`
- `transition`
- `unclear`

Doc-only tags allowed where a pipeline regime doesn't apply (the linter warns, not errors, on these): `any`, `rthOpen`, `globex`.

### mcpPointers shape
```yaml
mcpPointers:
  - tool: query_signal_outcome_distribution
    setupId: IDEA-NNN
    note: Pass jobId from latest verified run; never quote N from this file
```

### hypothesisAnchor
Only `IDEA-000` sets `hypothesisAnchor: true`. That file must contain the
`<!-- hypothesis-anchor: IDEA-000 -->` comment and the fenced `json` hypothesis
example — `src/research/hypothesis.rs` reads
`docs/setup-ideas/IDEA-000-regime-gated-selector.md` via `include_str!` and
`cargo test idea_000_doc_anchor` validates it. Do not move or reformat that block casually.
