# The Desk — Phase 3 PRD: Maturity

**Version:** 0.5 (Structured Outline)
**Date:** 2026-02-25
**Status:** Future — Structured outline with requirement IDs. Acceptance criteria to be defined after Phase 2 lessons.
**Depends on:** Phase 1 (Live Co-Pilot) and Phase 2 (Intelligence Expansion) complete

---

## 1. Phase Summary

Phase 3 matures The Desk into a deeply personalized trading partner that adapts to the individual trader over time, supports multiple instruments, and provides advanced analytics. This is where the moat fully materializes — every trader's Desk becomes genuinely unique and irreplaceable.

**Target timeline:** Months 8-12 (after Phase 2 is stable)

**Success metric:** Traders report that their Desk is genuinely unique to them — a tool no one else could replicate by downloading the same software.

---

## 2. Phase 3 Entry Criteria

| # | Criterion | Verification |
|---|-----------|-------------|
| 1 | Phase 2 options pipeline stable for 10+ trading sessions | No data gaps, gamma levels verified against provider source |
| 2 | Phase 2 behavioral pattern recognition producing insights | At least 3 statistically significant patterns surfaced to user |
| 3 | All Phase 2 P0 requirements implemented and tested | Requirement traceability matrix complete |
| 4 | Post-session review workflow validated by daily use | User completes review for >80% of sessions |
| 5 | 60+ session days of data accumulated | Sufficient data for adaptive coaching to learn from |

---

## 3. Feature Requirements

### 3.1 Adaptive Coaching

**Goal:** The Desk learns from how the trader responds to its prompts and adjusts coaching behavior over time.

**In scope:** Prompt timing adjustment, verbosity calibration, style adaptation, confidence detection.
**Out of scope:** Autonomous rule changes (the trader always controls their playbook).

#### Adaptation Guardrails

These are non-negotiable constraints on what adaptive coaching can and cannot do:

| Can Adapt | Cannot Adapt |
|-----------|-------------|
| Prompt timing (earlier or later alerts for known setups) | Setup conditions or rules (only the trader changes their playbook) |
| Prompt verbosity (less detail for well-known setups) | Risk parameters (always enforced as configured) |
| Coaching style/personality (direct vs. analytical vs. motivational) | Regulatory language constraints (always "your rules say...") |
| Quiet period inference (learn when trader prefers no interruption) | Whether to fire a rules-engine alert (deterministic, not adaptive) |
| Confidence calibration (flag overridden rules for review) | Trade execution or placement (never, architectural constraint) |

#### Adaptation Signals

| Signal | Interpretation |
|--------|---------------|
| Trader consistently responds "Took it" within 30s | Trader is ready — reduce verbosity for this setup |
| Trader consistently responds "Passed" with no note | Prompt may be unhelpful — flag rule for review |
| Trader ignores prompt (no response for 5+ minutes) | Timing may be wrong OR trader is busy — track and learn |
| Trader adds notes like "too early" / "too late" | Adjust timing for this setup |
| Trader overrides a specific condition repeatedly | Flag the condition for review: "You've overridden X 8 out of 10 times. Should this rule be updated?" |

#### Requirements

| ID | Requirement | Priority |
|----|-------------|----------|
| ADAPT-01 | Track prompt response timing (seconds from prompt display to trader response) per setup | P0 |
| ADAPT-02 | Track prompt override patterns (which setups/conditions are consistently ignored or overridden) | P0 |
| ADAPT-03 | Adjust prompt verbosity based on trader familiarity — reduce detail for setups with 20+ "Took it" responses | P1 |
| ADAPT-04 | Adjust prompt timing based on observed trader behavior — shift alert window earlier/later | P1 |
| ADAPT-05 | Learn quiet periods from trader behavior (beyond explicit config) — detect time windows where prompts are consistently ignored | P2 |
| ADAPT-06 | Coaching style evolves over months of interaction — personality becomes more natural and tailored | P2 |
| ADAPT-07 | Confidence calibration: if trader overrides a condition in >70% of opportunities, surface review prompt: "Your rules include X but you override it frequently. Should this rule be updated?" | P0 |
| ADAPT-08 | All adaptation is transparent — trader can view what the system has learned and reset any adaptation | P0 |
| ADAPT-09 | Adaptation changes are logged in the decision log with before/after comparison | P0 |
| ADAPT-10 | Minimum data threshold: no adaptation occurs until 30+ session days of data are accumulated | P0 |
| ADAPT-11 | Adaptation guardrails are enforced programmatically — the system cannot violate the "Cannot Adapt" column above | P0 |

### 3.2 Sub-Agent Personality System

**Goal:** A configurable, named coaching presence with personality settings that becomes a retention feature. Originally conceptualized in the V1 vision document.

**In scope:** Named persona, configurable personality traits, voice/style preferences.
**Out of scope:** Multiple simultaneous sub-agents, agent-to-agent communication.

| ID | Requirement | Priority |
|----|-------------|----------|
| AGENT-01 | Configurable coaching persona with name and personality description | P1 |
| AGENT-02 | Personality traits as weighted sliders: directness, analytical depth, motivational tone, humor, formality | P1 |
| AGENT-03 | Persona influences prompt language style while preserving all compliance constraints | P1 |
| AGENT-04 | Persona evolves based on adaptive coaching data (ADAPT-06) | P2 |
| AGENT-05 | Trader can reset persona to defaults at any time | P1 |

### 3.3 Advanced Pattern Recognition

**Goal:** Multi-dimensional analysis across months of structured data to surface deep insights.

**In scope:** Cross-setup correlation, market regime classification, drawdown analysis, setup lifecycle.
**Out of scope:** Predictive trading signals (the system identifies patterns, never predicts outcomes).

| ID | Requirement | Priority |
|----|-------------|----------|
| APR-01 | Cross-setup correlation analysis (does taking Setup A affect performance on Setup B later in the session?) | P1 |
| APR-02 | Market regime classification with performance overlay (trend/range/transition, high/low volatility) | P0 |
| APR-03 | Drawdown sequence analysis (what behavioral patterns precede drawdowns?) | P0 |
| APR-04 | Recovery pattern analysis (what does the trader do differently after a losing streak?) | P1 |
| APR-05 | Setup lifecycle tracking (when did a setup start working? When did it peak? Is it declining?) | P0 |
| APR-06 | Risk-adjusted performance metrics over time (Sharpe-like metrics on execution quality, not just P&L) | P1 |
| APR-07 | Seasonal and cyclical pattern detection across months of data | P2 |

### 3.4 Multi-Instrument Support

**Goal:** Extend beyond NQ to other futures instruments.

**In scope:** ES, YM, RTY, CL, GC. Per-instrument configuration. Multi-instrument monitoring.
**Out of scope:** Equities, forex, crypto. Cross-asset arbitrage strategies.

#### Multi-Instrument Generalization Audit

Before implementation, audit all pipeline code to determine what is NQ-specific:

| Component | NQ-Specific Elements | Generalization Needed |
|-----------|---------------------|----------------------|
| DTC Client | Symbol string only | Parameterize symbol; rest is protocol-generic |
| VWAP Pipeline | None (math is instrument-agnostic) | Session boundaries may differ per instrument |
| TPO Pipeline | 30-min brackets (convention, not NQ-specific) | Make bracket size configurable per instrument |
| Delta Pipeline | Trade direction classification (bid/ask) | Same logic applies to all futures |
| Levels Pipeline | Round number intervals (100 NQ points) | Make interval configurable per instrument |
| Risk Tracker | R-value in NQ points/dollars | Add per-instrument tick value ($5 for NQ, $12.50 for ES, etc.) |
| Rules Engine | None (evaluates conditions generically) | Condition fields are pipeline-dependent, already generic |
| Recording Format | None | Header could include instrument ID |
| Config | Default symbol "NQ" | Add per-instrument config sections |

#### Requirements

| ID | Requirement | Priority |
|----|-------------|----------|
| MULTI-01 | ES (S&P 500 E-mini) support — highest priority after NQ | P0 |
| MULTI-02 | YM (Dow E-mini) support | P1 |
| MULTI-03 | RTY (Russell 2000 E-mini) support | P1 |
| MULTI-04 | CL (Crude Oil) support — different market dynamics, tests generalizability | P2 |
| MULTI-05 | GC (Gold) support | P2 |
| MULTI-06 | Per-instrument pipeline configuration (tick size, value area periods, session times, round number intervals) | P0 |
| MULTI-07 | Per-instrument playbook and setups (setups are scoped to a specific instrument) | P0 |
| MULTI-08 | Multi-instrument correlation monitoring (NQ vs. ES divergences) | P2 |
| MULTI-09 | Per-instrument risk tracking (separate R-value definitions per instrument) | P0 |
| MULTI-10 | Simultaneous monitoring of up to 3 instruments (requires multiple DTC subscriptions) | P1 |

### 3.5 Additional Data Feed Support

**Goal:** Support traders who aren't on Sierra Chart or Rithmic.

| ID | Requirement | Priority |
|----|-------------|----------|
| FEED-01 | Denali data feed support via Sierra Chart DTC (should work with minimal changes) | P0 |
| FEED-02 | Direct Rithmic API connection (bypass Sierra Chart DTC) | P1 |
| FEED-03 | NinjaTrader integration (separate DTC server or proprietary connection) | P2 |
| FEED-04 | Generic webhook input for custom data sources | P2 |

### 3.6 Performance Analytics Dashboard

**Goal:** Rich visualization of trading performance across multiple dimensions.

| ID | Requirement | Priority |
|----|-------------|----------|
| PERF-01 | Equity curve with drawdown visualization | P0 |
| PERF-02 | Performance breakdown by setup, by day, by time, by market regime | P0 |
| PERF-03 | Plan adherence trend over time (weekly rolling average) | P0 |
| PERF-04 | Behavioral discipline score — composite metric of prompt adherence, rules adherence, risk management | P1 |
| PERF-05 | Comparison views: this month vs. last month, this setup vs. that setup | P1 |
| PERF-06 | Export capabilities for sharing with mentors or accountability partners | P2 |

### 3.7 Playbook Versioning

**Goal:** Track how setups evolve over time, enabling the trader to see what changes helped or hurt.

**Design direction:** Git-like snapshot model with diffs.

| ID | Requirement | Priority |
|----|-------------|----------|
| VER-01 | Full version history for every setup definition (automatic snapshot on save) | P0 |
| VER-02 | Diff view between any two versions of a setup | P0 |
| VER-03 | Performance metrics tagged to setup versions (did the filter you added actually improve results?) | P0 |
| VER-04 | "Experiment" mode — test a setup modification alongside the original, track results separately | P1 |
| VER-05 | Rollback capability — revert a setup to any previous version | P0 |

---

## 4. Dependencies on Phase 2

| Phase 2 Component | Phase 3 Depends On It For |
|-------------------|--------------------------|
| Options/gamma pipeline | Multi-instrument needs gamma data for ES, CL, etc. |
| Behavioral pattern recognition | Adaptive coaching learns from behavioral data |
| Structured post-session review | Pattern recognition feeds from review data |
| Trade grading and mistake taxonomy | Performance analytics drill-down data |
| Expanded backtest import | Playbook versioning compares performance across versions |

---

## 5. Long-Term Considerations (Phase 4+)

These ideas are captured to avoid losing them. They are not committed to any timeline.

### From V1 Vision: Community Features

The original vision included a Community phase with:
- **Shareable playbook templates** — share setup structures (not parameters) with other traders
- **Trading pods** — small groups of traders with shared accountability
- **Mentor access** — give a coach read-only access to session reviews

**Rationale for deferral:** Community features require user accounts, server infrastructure, and content moderation. These are fundamentally different from the local-first architecture of Phases 1-3. Evaluate after product-market fit is established.

### Other Phase 4+ Ideas

- **Mobile session review** — review sessions on your phone after trading
- **Voice interface** — talk to The Desk during sessions (hands-free coaching)
- **Multi-account tracking** — for traders running multiple prop firm evaluations
- **Automated compliance reporting** — generate prop firm rule compliance reports
- **API for custom integrations** — let power users build their own extensions
- **Market replay library marketplace** — community-contributed notable sessions

### Business Model Evolution
- _[Pricing tiers, enterprise features, team features — to be considered after product-market fit]_

---

## 6. Notes From Earlier Phases

> This section will be updated as we build Phases 1 and 2.

### Architecture Notes
- _[To be filled during development]_

### Scope Adjustments
- _[To be filled as priorities shift]_

### New Ideas
- _[Capture ideas that emerge but belong in Phase 3]_

### Lessons Learned
- _[Accumulated learnings from Phase 1 and 2 development]_

---

*Acceptance criteria for each requirement will be defined after Phase 2 development reveals practical constraints and user priorities.*
