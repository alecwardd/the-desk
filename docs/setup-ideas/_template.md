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

> Per-idea detail file. The hub ([setup-ideas-and-backtesting.md](../setup-ideas-and-backtesting.md)) keeps a one-line stub anchor pointing here.

## Thesis
What the setup is and why an edge should exist. Auction / order-flow mechanics.

## Mechanics
Location → trigger → confirmation → invalidation → risk state.

## Status / verdicts
Narrative of where this stands. **No bare win rates.** Any numeric performance claim must carry an `mcpPointers` entry, a `jobId`, or an explicit `<!-- stats: point-in-time -->` marker — pull fresh numbers from MCP/SQLite at query time.

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
`<!-- hypothesis-anchor: IDEA-000 -->` comment immediately followed by the fenced
`json` hypothesis example — `src/research/hypothesis.rs` reads it via `include_str!`
and `cargo test idea_000_doc_anchor` validates it. Do not move or reformat that block casually.
