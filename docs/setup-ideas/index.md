# The Desk — Setup Ideas & Backtesting Research

Living tracker for trade setup ideas, backtesting hypotheses, research findings, and
cross-cutting infrastructure work. Each idea lives in its own file in this directory and
is tracked from concept through validation. New ideas start from [_template.md](_template.md).

**Restructured 2026-07-05** from the single-file log (`docs/setup-ideas-and-backtesting.md`,
now a pointer stub). Hypothesis `docReference` values are idea IDs (`IDEA-NNN`), not file
paths, so registered hypotheses are unaffected; the `<!-- hypothesis-anchor -->` comments
moved with their ideas.

### Companion specs

- **Multi-instrument flow architecture (NQ / MNQ / ES / MES)** — [`docs/multi-instrument-flow-architecture.md`](../multi-instrument-flow-architecture.md) (tracked as [IDEA-021](idea-021-multi-instrument-flow.md)).
- **IDEA-000 / IDEA-012 backtest runbook** — [`docs/idea000-idea012-backtest-runbook.md`](../idea000-idea012-backtest-runbook.md): copy-pasteable register → backtest → gate → activate sequence.
- **Social intelligence & continual learning (X/Twitter)** — [`docs/social-intelligence-roadmap.md`](../social-intelligence-roadmap.md) (master feature track), [`docs/social-confluence-design.md`](../social-confluence-design.md) (Phase A v1 spec), [decision-log.md](../decision-log.md) ADR-020 (Pending), [IDEA-023](idea-023-social-intelligence.md).

### Cross-cutting notes

- [research-notes.md](research-notes.md) — March 2026 snapshot, backtest findings, instrumentation caveats, research sources.
- [codebase-audit.md](codebase-audit.md) — external codebase review and opinions.
- [scratchpad.md](scratchpad.md) — chartbook MGI checklist, definition passes, working notes.

---

## How to Use This Tracker

| Status | Meaning |
|--------|---------|
| **Idea** | Concept identified, not yet researched or coded |
| **Researched** | Supporting evidence gathered, mechanics understood |
| **Prototyped** | Pipeline or detection logic implemented |
| **Backtesting-ready** | Instrumentation and setup mechanics are ready for a verified backtest rerun |
| **Backtesting** | Running through historical .scid data |
| **Validated** | Backtest results confirm edge; ready for template |
| **In Playbook** | Added to setup_templates.rs and active |
| **Rejected** | Tested and found no reliable edge |

---

## Idea Catalog

Priorities: **P0** regime overlay · **P1** implementable with existing pipelines ·
**P2** infrastructure upgrades · **P3** requires external data · **P4** new detection logic required.

| ID | Idea | Status | Priority |
|----|------|--------|----------|
| IDEA-000 | [Regime-Gated Setup Selector](idea-000-regime-gated-setup-selector.md) | Rejected → folded into IDEA-020 | P0 |
| IDEA-001 | [Opening Drive Classification](idea-001-opening-drive-classification.md) | Researched | P1 |
| IDEA-011 | [One-Sided IB Extension Acceptance](idea-011-one-sided-ib-extension.md) | Backtesting-ready | P1 |
| IDEA-002 | [Trapped Trader Reversal](idea-002-trapped-trader-reversal.md) | Researched | P1 |
| IDEA-012 | [Absorption Failure / Liquidity Vacuum](idea-012-absorption-failure-vacuum.md) | Rejected → folded into IDEA-020 | P1 |
| IDEA-003 | [Naked VPOC Magnet Trade](idea-003-naked-vpoc-magnet.md) | Researched | P1 |
| IDEA-004 | [Multi-Timeframe CVD Divergence](idea-004-multi-tf-cvd-divergence.md) | Researched | P1 |
| IDEA-005 | [Session Transition Sweep Patterns](idea-005-session-transition-sweeps.md) | Researched | P1 |
| IDEA-020 | [Footprint Rebid/Reoffer Zone Lifecycle](idea-020-rebid-reoffer-zone-lifecycle.md) | Stage 1 landed — primary track | P1 |
| IDEA-022 | [Rally Offer Replenishment / Touch Offer Exhaustion](idea-022-rally-offer-replenishment.md) | Idea | P1 |
| IDEA-021 | [Multi-Instrument Flow Architecture (NQ/MNQ/ES/MES)](idea-021-multi-instrument-flow.md) | Spec drafted — Stage A buildable | P1 |
| IDEA-006 | [Volume Imbalance Bars (Lopez de Prado)](idea-006-volume-imbalance-bars.md) | Researched | P2 |
| IDEA-019 | [Adaptive Session-Pace Volume Bars (ACSIL)](idea-019-adaptive-session-pace-bars.md) | Idea | P2 |
| IDEA-007 | [Microstructure Regime Detection](idea-007-microstructure-regime-detection.md) | Researched | P2 |
| IDEA-016 | [VWAP Pipeline Enhancements (Dual Session + Anchored)](idea-016-vwap-enhancements.md) | Idea | P2 |
| IDEA-017 | [MCP Product Hardening — Playbook as First-Class Data](idea-017-mcp-product-hardening.md) | Idea | P2 |
| IDEA-018 | [Multi-Instrument Concurrent Tracking](idea-018-multi-instrument-tracking.md) | Idea | P2 |
| IDEA-023 | [Social Intelligence & Continual Learning (X)](idea-023-social-intelligence.md) | Idea — blocked on ADR-020 | P2 |
| IDEA-008 | [0DTE Gamma Regime Trading](idea-008-0dte-gamma-regime.md) | Researched | P3 |
| IDEA-013 | [Gamma-Gated Setup Overlay](idea-013-gamma-gated-overlay.md) | Researched | P3 |
| IDEA-009 | [NQ/ES SMT Divergence](idea-009-nq-es-smt-divergence.md) | Researched | P3 |
| IDEA-010 | [Fair Value Gap with Order Flow Confirmation](idea-010-fair-value-gap.md) | Researched | P4 |
| IDEA-014 | [London Inventory Unwind Into RTH](idea-014-london-inventory-unwind.md) | Researched | P4 |
| IDEA-015 | [Post-Macro / Post-Earnings Jump Repair-or-Go](idea-015-post-macro-jump-repair.md) | Researched | P4 |

---

## Backtesting Queue

Ordered by expected information value × implementation ease:

| # | Hypothesis | Setup | Data Needed | Priority |
|---|-----------|-------|-------------|----------|
| 1 | One-sided vs both-sided IB extension: first pullback expectancy | IDEA-011 | session_summaries, IB extension events | High |
| 2 | London trends, RTH opens back in value, DNP/VWAP reclaim → unwind probability | IDEA-014 | multi-session summaries, delta, VWAP | High |
| 3 | Absorption invalidation + pace expansion at key level → 15/30 min follow-through | IDEA-012 | absorption events, pace, key levels | High |
| 4 | Open Drive + RVOL ≥ Elevated → pullback to VWAP win rate | IDEA-001 | session_summaries, events | High |
| 5 | Regime selector improves OR5 / IB / DNVA / VWAP family expectancy | IDEA-000 | session_summaries, events, setup outcomes | High |
| 6 | Naked VPOC fill rate within 1/3/5/10 sessions | IDEA-003 | session_summaries POC + ticks | Medium |
| 7 | CVD divergence at VA boundary → reversal within 30 min | IDEA-004 | delta pipeline, events | Medium |
| 8 | London sweep of Asia range → RTH direction prediction | IDEA-005 | Globex session data | Medium |
| 9 | Volume bars vs time bars: R-distribution comparison for existing setups | IDEA-006 | .scid tick data | Medium |
| 10 | Positive-gamma gating vs negative-gamma gating on existing setup families | IDEA-013 | options / gamma data + setup outcomes | Medium |
| 11 | Stacked imbalances (≥3, ≥4:1) fail → reversal probability | IDEA-002 | footprint data | Medium |
| 12 | Narrow IB (<0.7x avg) → breakout continuation rate | IDEA-001 | session_summaries IB range | Low |
| 13 | Three-session alignment → range extension beyond IB | IDEA-005 | multi-session data | Low |
| 14 | Prior Globex VWAP as S/R in first 60 min of RTH on unwind days | IDEA-016 | session VWAP snapshots, ticks | Low |
| 15 | Anchored VWAP from IB break: band respect vs session VWAP bands | IDEA-016 | IB break events, ticks | Low |
| 16 | Zone establishment age vs clearance velocity → follow-through / regime | IDEA-020 | zone lifecycle events, pace | Medium |

---

## Verified Backtesting Runbook

Use this sequence for any setup study that depends on `signal_outcomes`:

1. **Preflight integrity:** call `validate_signal_outcome_integrity` with the intended `source`, `jobId` if available, and `setupId` if narrowed. `failed` means stop; `warning` means inspect legacy ratios before using the result.
2. **Use fresh job IDs:** never mix old and new outcome engines in the same statistic. Fresh deterministic backtests should produce a new `job_id` and should store their integrity report in `backtest_runs.metrics.signalOutcomeIntegrity`.
3. **Prefer verified rows:** while the transition is active, research tools default `includeUnverified=true` for backwards compatibility. For new studies, pass `includeUnverified=false`.
4. **Pin provenance in notes:** every published result should cite `source`, `job_id`, setup id, date/session scope, outcome engine version, rules schema version, and whether `qualityCounts.verified` covers the full sample.
5. **Flip defaults later:** after verified reruns exist for the immediate research windows, change the research-tool default from `includeUnverified=true` to verified-only and keep legacy inclusion as an explicit opt-in.

Immediate next target: rerun IDEA-011 under this runbook and promote the verified result into [research-notes.md](research-notes.md).

---

*Last updated: 2026-07-05 (restructured from the single-file log).*
