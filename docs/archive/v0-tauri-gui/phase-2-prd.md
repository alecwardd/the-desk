# The Desk — Phase 2 PRD: Intelligence Expansion

**Version:** 0.5 (Structured Outline)
**Date:** 2026-02-25
**Status:** Future — Structured outline with requirement IDs. Acceptance criteria to be defined after Phase 1 lessons.
**Depends on:** Phase 1 (Live Co-Pilot) complete and validated

---

## 1. Phase Summary

Phase 2 expands The Desk's analytical capabilities with options/gamma data integration, deeper order flow analysis, structured post-session review with behavioral pattern recognition, and the first iteration of long-term performance analytics.

**Target timeline:** Months 4-7 (after Phase 1 is stable and in daily use)

**Success metric:** The Desk surfaces a behavioral insight the trader didn't know about themselves — and acting on it improves their results.

---

## 2. Phase 2 Entry Criteria

All of the following must be true before Phase 2 development begins:

| # | Criterion | Verification |
|---|-----------|-------------|
| 1 | Phase 1 DTC client stable for 20+ consecutive trading sessions | Session recording logs show zero disconnection data loss |
| 2 | All Phase 1 P0 requirements implemented and tested | Requirement traceability matrix complete |
| 3 | At least 1 trader using The Desk in daily RTH trading | User feedback collected |
| 4 | Pipeline latency consistently <50ms in production | Performance monitoring data |
| 5 | Phase 1 open questions resolved (PRD Section 9) | Decision log updated |
| 6 | Session recording/replay fully functional | Replay of 5+ real sessions verified |

---

## 3. Feature Requirements

### 3.1 Options / Gamma Data Pipeline

**Goal:** Give the trader visibility into the derivatives landscape affecting NQ price behavior.

**In scope:** GEX by strike, dealer positioning model, charm/vanna flow, gamma level overlay on market structure.
**Out of scope:** Live options trading, options P&L tracking, options strategy builder.

#### Provider Evaluation Framework

| Criterion | Weight | **Databento** (preferred) | Unusual Whales | CBOE Raw | OptionData.io | ConvexValue |
|-----------|--------|---------------------------|---------------|----------|---------------|-------------|
| NQ/NDX coverage depth | 25% | ✅ OPRA + CME (NDX + NQ) | TBD | NDX only | TBD | TBD |
| Data freshness / latency | 20% | ✅ Direct feeds | TBD | TBD | TBD | TBD |
| Greek accuracy (GEX, delta, gamma, charm, vanna) | 20% | ✅ We compute (own model) | Pre-computed | We compute | Pre-computed | Pre-computed |
| API reliability / rate limits | 15% | ✅ Strong | TBD | TBD | TBD | TBD |
| Cost / licensing | 10% | ✅ Usage + subscription | TBD | TBD | TBD | TBD |
| Integration complexity | 10% | ⚠️ Higher (build GEX pipeline) | Lower | Lower | Lower | TBD |

**Preferred provider:** Databento (see `docs/phase-2-options-databento-memo.md`). Rationale: single source for NDX (OPRA) and NQ (CME) options, raw data so we compute all Greeks ourselves for a robust model, Rust client library, excellent docs. Trade-off: we build the GEX/Greeks pipeline in Rust. Alternatives: Unusual Whales (fastest path, pre-computed), ConvexValue (pre-computed GEX/gamma, gxoi, gxvolm — evaluate if Databento build proves too heavy).

**Decision gate:** Provider selection must be made before any Phase 2 implementation begins. Use the `options-api-researcher` subagent to populate the evaluation matrix.

#### Requirements

| ID | Requirement | Priority |
|----|-------------|----------|
| OPT-01 | Integrate selected options data provider API (REST or WebSocket) | P0 |
| OPT-02 | Compute or ingest GEX by strike and expiry for NQ/NDX | P0 |
| OPT-03 | Identify key gamma levels (GEX flip, max gamma, zero gamma) | P0 |
| OPT-04 | Estimate dealer positioning (long gamma / short gamma regions) | P0 |
| OPT-05 | Compute charm and vanna flow estimates for intraday decay effects | P1 |
| OPT-06 | Refresh options data at configurable interval (default: every 5 minutes during RTH) | P0 |
| OPT-07 | Expose gamma levels as structured data to the rules engine (new condition fields: `gex_level`, `gamma_exposure_sign`, `dealer_positioning`) | P0 |
| OPT-08 | Display key gamma levels in the market state sidebar | P0 |
| OPT-09 | Handle SPX gamma data as proxy for NQ when NQ-specific options data is insufficient | P1 |
| OPT-10 | LLM coaching incorporates gamma context into setup prompts when available | P1 |
| OPT-11 | Graceful degradation: all features work without options data (feed unavailable or not subscribed) | P0 |

**Open decisions:**
- [x] Which provider gives the best signal-to-noise for NQ trading? → **Databento** (preferred; see memo)
- [x] Build our own GEX model from raw data or rely on pre-computed from provider? → **Build our own** (raw data from Databento; compute Greeks/GEX in Rust for full control and robustness)
- [ ] What latency tolerance for options data? (30-60 second delay likely acceptable)

### 3.2 Advanced Order Flow Analysis

**Goal:** Move beyond cumulative delta to detect specific order flow patterns the trader uses for confirmation.

**In scope:** Volume imbalances, absorption detection, initiative/responsive classification, delta divergence.
**Out of scope:** Full footprint chart rendering (the trader uses Sierra Chart for that), high-frequency order flow replay.

| ID | Requirement | Priority |
|----|-------------|----------|
| FLOW-01 | Detect volume imbalances (stacked bid/ask imbalances from DOM snapshot data) | P0 |
| FLOW-02 | Detect absorption patterns (large resting orders filled without price movement) | P1 |
| FLOW-03 | Classify market activity as initiative vs. responsive at each price level | P1 |
| FLOW-04 | Alert on delta divergences (price new high/low without delta confirmation) | P0 |
| FLOW-05 | Aggregate volume by price by aggressor (footprint-style data for rules engine) | P1 |
| FLOW-06 | Expose order flow signals as structured data to the rules engine (new condition fields) | P0 |
| FLOW-07 | All order flow analysis operates on the 100ms DOM snapshots recorded during sessions | P0 |
| FLOW-08 | Order flow signals are fully deterministic (no LLM involvement in detection) | P0 |

**Open decisions:**
- [ ] Does 100ms DOM snapshot resolution provide enough data for meaningful absorption/imbalance detection?
- [ ] Which specific patterns does the trader use most frequently? (Inform via Phase 1 usage data)
- [ ] How to handle the gap between "what The Desk sees in aggregated data" vs "what the trader reads live on their DOM"?

### 3.3 Structured Post-Session Review

**Goal:** Move beyond basic session logs to a structured coaching review that identifies patterns across sessions.

**In scope:** Trade grading, mistake taxonomy, emotional state analysis, LLM-generated review narrative, historical comparison.
**Out of scope:** Automated trade detection from market data (Phase 1 relies on CSV import or manual entry).

| ID | Requirement | Priority |
|----|-------------|----------|
| REV-01 | Automated trade grading: planned vs. unplanned, rules followed vs. deviated, setup quality score | P0 |
| REV-02 | Emotional state tracking over time: correlate trader self-reports with performance metrics | P0 |
| REV-03 | Plan adherence score per session (percentage of trades that followed the plan) | P0 |
| REV-04 | Mistake taxonomy: categorize deviations into predefined types (impulse entry, moved stop, skipped setup, oversized, revenge trade, boredom trade, FOMO entry) | P0 |
| REV-05 | LLM-generated session review narrative using Claude Opus for deep analysis | P1 |
| REV-06 | Compare current session to historical sessions with similar market context | P1 |
| REV-07 | Post-session review prompted immediately after session close, with option to defer | P0 |
| REV-08 | Export session review as PDF or Markdown for sharing with mentors | P2 |

### 3.4 Behavioral Pattern Recognition

**Goal:** Identify recurring behavioral patterns across weeks/months that are invisible in daily review.

**In scope:** Temporal patterns, post-win/loss behavior, setup lifecycle, overtrade detection.
**Out of scope:** Predictive modeling (we identify patterns, not predict future behavior).

**Minimum sample size thresholds:** No behavioral insight is surfaced until the minimum data requirement is met. This prevents overfitting to small samples.

| Pattern Type | Minimum Samples |
|-------------|----------------|
| Day-of-week performance | 4 samples per day (20 trading days minimum) |
| Time-of-day performance | 20 trades in each time bucket |
| Post-win/loss behavior | 10 sequences of each type |
| Setup performance trend | 15 instances of the setup |
| Overtrade correlation | 15 sessions with varying trade counts |

| ID | Requirement | Priority |
|----|-------------|----------|
| BPR-01 | Day-of-week performance analysis (win rate, avg R by day) | P0 |
| BPR-02 | Time-of-day performance analysis (performance by 30-minute bucket) | P0 |
| BPR-03 | Post-win and post-loss behavior analysis (does performance change after a big win/loss?) | P0 |
| BPR-04 | Setup decay/improvement detection (is a setup's edge changing over time?) | P1 |
| BPR-05 | Overtrade detection (correlation between trade count and session performance) | P0 |
| BPR-06 | Context-dependent performance (trend days vs. range days, high vs. low volatility) | P1 |
| BPR-07 | Consecutive session momentum (hot streaks, cold streaks, mean reversion patterns) | P2 |
| BPR-08 | Surface behavioral insights in pre-session briefing when statistically significant | P0 |
| BPR-09 | Enforce minimum sample sizes before displaying any pattern (see thresholds above) | P0 |
| BPR-10 | All behavioral analysis is purely descriptive — never predictive or advisory | P0 |

### 3.5 Expanded Backtest Import

**Goal:** Make it easier to bring in backtest results from more tools and formats.

**In scope:** Additional import formats, column mapping, backtest versioning.
**Out of scope:** Building a backtesting engine (Never Do list).

| ID | Requirement | Priority |
|----|-------------|----------|
| BT-01 | NinjaTrader trade log import | P1 |
| BT-02 | TradingView strategy report import | P1 |
| BT-03 | Generic CSV import with user-defined column mapping UI | P0 |
| BT-04 | JSON import format for Python script results | P1 |
| BT-05 | Backtest result versioning — track how metrics change as the trader refines a setup | P1 |

---

## 4. Integration with Existing Architecture

Phase 2 features follow the same three-layer architecture:

| Feature | Layer 1 (Pipelines) | Layer 2 (Rules Engine) | Layer 3 (LLM) |
|---------|:------------------:|:---------------------:|:--------------:|
| Options/gamma pipeline | New `OptionsState` pipeline | New condition fields (GEX, gamma, dealer) | Gamma context in coaching prompts |
| Order flow analysis | New `OrderFlowState` pipeline | New condition fields (imbalance, absorption) | Order flow context in prompts |
| Post-session review | -- | -- | Opus-powered review narrative |
| Behavioral patterns | Computed from stored data (batch, not real-time) | -- | Insights surfaced in pre-session briefing |
| Backtest import | -- | -- | -- (data layer only) |

---

## 5. Dependencies on Phase 1

| Phase 1 Component | Phase 2 Depends On It For |
|-------------------|--------------------------|
| DTC client | Foundation for all real-time data |
| Pipeline architecture | Options and order flow pipelines follow same pattern |
| Rules engine | New condition fields for options/flow |
| SQLite schema | Extended for behavioral data, trade grading, options data |
| LLM integration | Deeper analysis prompts, session review |
| Session recording | Order flow analysis on recorded DOM data |
| Trade import (CSV) | Foundation for trade grading and pattern analysis |
| Prompt response system (LOG-08..13) | Behavioral pattern data (prompt adherence over time) |

---

## 6. Deferred Considerations

### Research Library (from V1 Vision)

The original vision document included a "Discover" stage with a research library — saved posts, links, screenshots, voice notes, tagged and searchable. This solves a real problem (traders have ideas scattered across Twitter bookmarks, Discord messages, and notebooks) but is not core to the intelligence expansion focus of Phase 2.

**Recommendation:** Evaluate for Phase 3 or as a standalone feature. If Phase 2 reveals that traders want to attach external context to their setups (beyond journal notes), this moves up in priority.

### Voice/Audio Coaching

Text-to-speech for high-priority coaching prompts would let the trader keep eyes on Sierra Chart. This is a significant UX improvement for live sessions.

**Recommendation:** Evaluate after Phase 1 user feedback. If traders report that reading prompts on the second monitor is disruptive during entry timing, prioritize for Phase 2 or early Phase 3.

### Broker API Integration

Direct Rithmic API for trade data would enable real-time trade tracking without manual entry or CSV import. This makes in-trade management coaching significantly more powerful.

**Recommendation:** Evaluate technical feasibility during Phase 1 DTC client work. If Rithmic's API supports trade event streaming alongside market data, add as Phase 2 P1.

---

## 7. Notes From Phase 1 Development

> This section will be updated as we build Phase 1.

### Architecture Notes
- _[To be filled during Phase 1 development]_

### Scope Adjustments
- _[To be filled as priorities shift]_

### New Ideas
- _[Capture ideas that emerge during Phase 1 but belong in Phase 2]_

### Lessons Learned
- _[What we learned about the DTC protocol, Claude API latency, user experience, etc.]_

---

*Acceptance criteria for each requirement will be defined after Phase 1 development reveals practical constraints and user priorities.*
