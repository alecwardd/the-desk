---
id: IDEA-031
title: Session Range Compression and Expansion
status: Idea
regime: [compression, expansion, transition]
related: [IDEA-000, IDEA-005, IDEA-007, IDEA-014]
companionSpecs:
  - docs/data-and-backtesting-guide.md
  - docs/mcp/tool-reference.md
hypothesisAnchor: false
---

# IDEA-031 - Session Range Compression and Expansion

> Research-only promotion of a trader-authored capture from 2026-07-10. No live
> market data, backtest output, account context, or signal logic was created.

## Thesis

NQ volatility may alternate between compression, expansion, and digestion, but
the sequence must be tested at explicit session boundaries. The captured belief
that an expansive RTH session is likely to be followed by a contracted RTH
session is a hypothesis—not a validated statistic or trade rule.

The research should distinguish three views:

1. **Like-to-like:** RTH→RTH, Asia→Asia, London→London.
2. **Consecutive session handoff:** RTH→Asia→London→RTH in trading-day order.
3. **Rolling multi-session state:** several sessions coiling or expanding before
   the next transition.

## Candidate Measurements

- Session range, true range, realized volatility, and range percentile against
  a same-session rolling baseline.
- ATR-normalized range and robust alternatives such as median/MAD percentiles.
- Range contraction/expansion ratios between adjacent sessions.
- Consecutive inside/outside ranges, value-area width/overlap, and IB/OR width.
- RVOL, day type, profile shape, and whether expansion was directional or
  two-sided.
- Gap/event context, contract rollover, holidays, and missing/partial sessions.

RTH, Asia, London, and combined Globex must never share an unlabeled baseline.

## Candidate State Model

Start with transparent predicates rather than a learned classifier:

- `Compressed`: range or realized-volatility percentile below a locked threshold.
- `Normal`: within the middle baseline band.
- `Expanded`: above a locked threshold.
- `Transition`: a material change between adjacent states, labeled with the
  source and destination session types.

Thresholds are research parameters, not constants to hard-code before coverage
and sensitivity checks.

## Backtest Questions

1. Conditional on an expanded RTH session, what is the next RTH range
   distribution versus the unconditional baseline?
2. After two or more compressed like sessions, how does the next-session range
   distribution change? Include `N`, uncertainty, and threshold sensitivity.
3. Does the consecutive RTH→Asia→London sequence add information beyond the
   current session's own range percentile?
4. Do value overlap, RVOL, day type, or profile shape distinguish productive
   compression from ordinary low-participation noise?
5. On expansion transitions, what fraction accepts outside prior value versus
   repairs back into balance before IB completes?
6. Are results stable across contract months, weekdays, macro-event days, and
   the available history window?

## Implementation Boundary

First produce a coverage audit and docs-only study design. Reuse existing
session summaries and research tools where possible. If fields are missing,
propose the smallest deterministic additions after the study design is reviewed.

Do not:

- create an entry/exit signal from compression alone;
- quote an edge before a dated, provenance-complete backtest clears the
  sample-size policy;
- mix session scopes or partial sessions silently;
- change live agents, playbooks, risk, chart settings, or execution behavior.

## See Also

- Hub stub: [setup-ideas-and-backtesting.md#idea-031](../setup-ideas-and-backtesting.md#idea-031)
- Setup index: [index.md](index.md)
