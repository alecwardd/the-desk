# The Desk — Phase 2 PRD: Intelligence Expansion

**Version:** 0.1 (Placeholder)
**Date:** 2026-02-20
**Status:** Future — Not yet scoped in detail
**Depends on:** Phase 1 (Live Co-Pilot) complete and validated

---

## 1. Phase Summary

Phase 2 expands The Desk's analytical capabilities with options/gamma data integration, deeper order flow analysis, structured post-session review with behavioral pattern recognition, and the first iteration of long-term performance analytics.

**Target timeline:** Months 4-7 (after Phase 1 is stable and in daily use)

**Success metric:** The Desk surfaces a behavioral insight the trader didn't know about themselves — and acting on it improves their results.

---

## 2. Planned Feature Areas

### 2.1 Options / Gamma Data Pipeline

**Goal:** Give the trader visibility into the derivatives landscape affecting NQ price behavior.

**Planned scope:**
- Unusual Whales API integration (first priority) — GEX by strike/expiry, delta/gamma/charm/vanna exposure, options flow alerts
- CBOE raw data integration (second priority) — compute GEX and dealer positioning from first principles
- OptionData.io WebSocket integration (optional, $599/mo) — real-time tick-level options flow
- Gamma exposure visualization — key GEX levels overlaid on market structure
- Dealer positioning model — estimate where market makers need to hedge
- Charm/vanna flow estimation — how Greeks decay affects positioning intraday

**Key questions to resolve during Phase 1:**
- [ ] Which options data provider gives the best signal-to-noise for NQ trading?
- [ ] How frequently does GEX data need to refresh? Real-time or periodic (every 5 min)?
- [ ] Should we build our own GEX model from CBOE raw data or rely on pre-computed from providers?
- [ ] What's the latency tolerance for options data? (Less critical than tick data — 30-60 second delay acceptable?)
- [ ] How do we handle SPX gamma data as a proxy for NQ (since NQ options are less liquid than SPX)?

**Integration with existing system:**
- Options pipeline feeds into Layer 1 (deterministic pipelines) alongside TPO, delta, VWAP
- Rules engine gains new condition fields: GEX_level, gamma_exposure_sign, dealer_positioning
- LLM coaching incorporates gamma context into setup prompts

**API references collected:**
- Unusual Whales: REST API with Greek exposure endpoints (GEX by strike/expiry, delta, gamma, charm, vanna), flow alerts, directional flow. JSON responses with paginated data arrays.
- OptionData.io: WebSocket at `wss://ws.optiondata.io`, streams live option trades with full Greeks, supports 17+ filter parameters, $599/mo.
- CBOE: Direct data for building from scratch.
- ConvexValue / OmegaMind: Potential specialized integrations.

### 2.2 Advanced Order Flow Analysis

**Goal:** Move beyond cumulative delta to detect specific order flow patterns that the trader uses for confirmation.

**Planned scope:**
- Volume imbalance detection (stacked bid/ask imbalances on DOM snapshot data)
- Absorption detection (large resting orders being filled without price movement)
- Initiative vs. responsive activity classification
- Delta divergence alerting (price making new highs/lows without delta confirmation)
- Footprint-style data aggregation (volume by price by aggressor)

**Key questions to resolve during Phase 1:**
- [ ] Does the DTC DOM snapshot data (recorded at 100ms intervals) give enough resolution for meaningful order flow analysis?
- [ ] What specific order flow patterns does the trader use most frequently for confirmation?
- [ ] Should order flow signals be fully deterministic, or are some inherently discretionary?
- [ ] How do we handle the distinction between "what The Desk sees in aggregated data" vs "what the trader sees live on their DOM"?

### 2.3 Structured Post-Session Review

**Goal:** Move beyond basic session logs to a structured coaching review that identifies patterns across sessions.

**Planned scope:**
- Automated trade grading (planned vs. unplanned, rules followed vs. deviated, setup quality)
- Emotional state tracking over time (trader self-reports, correlated with performance)
- Plan adherence scoring (percentage of trades that followed the plan per session)
- "Mistake taxonomy" — categorize deviations (impulse entry, moved stop, skipped setup, oversized, etc.)
- LLM-generated session review narrative (Opus-level analysis)
- Comparison to historical sessions with similar market context

**Key questions to resolve during Phase 1:**
- [ ] What journaling fields do traders actually fill out consistently vs. what they skip?
- [ ] How granular should emotional state tracking be? (Simple: green/yellow/red? Or detailed categories?)
- [ ] Should review be prompted immediately after session close, or available on-demand?

### 2.4 Behavioral Pattern Recognition

**Goal:** Identify recurring behavioral patterns across weeks/months that are invisible in daily review.

**Planned scope:**
- Day-of-week performance patterns
- Time-of-day performance patterns
- Post-win and post-loss behavior analysis (does win rate drop after a big green day?)
- Setup decay/improvement detection (is a setup losing its edge over time?)
- Overtrade detection (correlation between trade count and performance)
- Context-dependent performance (trend days vs. range days, high gamma vs. low gamma)
- Consecutive session momentum (hot streaks, cold streaks, mean reversion)

**Key questions to resolve during Phase 1:**
- [ ] How many sessions of data are needed before pattern analysis becomes statistically meaningful?
- [ ] Should insights be surfaced proactively (in pre-session briefing) or on-demand (in a dashboard)?
- [ ] How do we avoid overfitting to small sample sizes? (Minimum samples before showing a pattern?)

### 2.5 Expanded Backtest Import

**Goal:** Make it easier to bring in backtest results from more tools and formats.

**Planned scope:**
- NinjaTrader trade log import
- TradingView strategy report import
- Generic CSV import with column mapping UI
- Python script results import (JSON format)
- Backtest result versioning (track how metrics change as you refine a setup)

---

## 3. Notes From Phase 1 Development

> This section will be updated as we build Phase 1. Capture any insights, scope changes,
> or new requirements that affect Phase 2 planning here.

### Architecture Notes
- _[To be filled during Phase 1 development]_

### Scope Adjustments
- _[To be filled as priorities shift]_

### New Ideas
- _[Capture ideas that emerge during Phase 1 but belong in Phase 2]_

### Lessons Learned
- _[What we learned about the DTC protocol, Claude API latency, user experience, etc.]_

---

## 4. Dependencies on Phase 1

| Phase 1 Component | Phase 2 Depends On It For |
|-------------------|--------------------------|
| DTC client | Foundation for all real-time data |
| Pipeline architecture | Options pipeline follows same pattern |
| Rules engine | New condition fields for options/flow |
| SQLite schema | Extended for behavioral data, trade grading |
| LLM integration | Deeper analysis prompts, session review |
| Session recording | Order flow analysis on recorded DOM data |
| Trade import | Foundation for trade grading and pattern analysis |

---

*This document is a living placeholder. It will be refined as Phase 1 development reveals what matters most for Phase 2.*
