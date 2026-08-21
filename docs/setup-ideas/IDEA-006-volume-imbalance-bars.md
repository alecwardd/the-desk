---
id: IDEA-006
title: Volume Imbalance Bars (Lopez de Prado)
status: Researched
regime: [any]
related: []
companionSpecs: []
mcpPointers: []
hypothesisAnchor: false
---

# IDEA-006 — Volume Imbalance Bars (Lopez de Prado)

> Per-idea detail file. The hub ([setup-ideas-and-backtesting.md](../setup-ideas-and-backtesting.md)) keeps a short stub (status, source, framing, detail link) pointing here.

<!-- stats: point-in-time -->
**Status:** Researched
**Source:** Lopez de Prado, "Advances in Financial Machine Learning" Ch. 2-3
**Complements:** All existing setups (infrastructure improvement)

**Concept:** Replace or supplement time-based bars with volume/tick/dollar bars that normalize information arrival. Lopez de Prado (2018) argues imbalance bars fire when information arrives rather than on the clock. The "3–8 bars earlier than time-bar traders" figure is **that book's claim**, not a Desk sample (`N` not reported here; not a verified NQ expectancy).

**Bar Types:**
- **Volume bars**: New bar every N contracts (calibrate to ~1,000-1,500 bars/RTH)
- **Tick bars**: New bar every N transactions
- **Dollar bars**: New bar every $N notional (most stable across contract rolls)
- **Imbalance bars**: New bar when cumulative signed volume/ticks deviate from expected → earliest regime change detection

**Why It Matters:**
- Time bars over-sample quiet periods and under-sample active ones
- Volume/tick/dollar bars produce near-normal return distributions
- Improves statistical properties of ALL downstream signals
- Source-reported (Lopez de Prado 2018): imbalance bars can surface regime changes earlier than equivalent time bars. Treat as literature motivation until a Desk volume-bar vs time-bar R-distribution exists.

**Implementation Notes:**
- Modify `.scid` processing loop to emit events on volume/tick thresholds in addition to time
- Start with volume bars (simplest): accumulate volume, emit bar when threshold reached
- Calibrate bar size using 20-day rolling session volume ÷ target bar count
- Later: implement imbalance bars per Lopez de Prado formula (E[b_t] exponentially weighted)

**Backtesting Hypothesis:**
> Do existing setups (OR5, rebid/reoffer, DNVA reversion) produce better R-distributions when evaluated on volume bars vs. 1-minute time bars?

---

## See also

- Hub stub: [setup-ideas-and-backtesting.md#idea-006](../setup-ideas-and-backtesting.md#idea-006)
- Setup index: [index.md](index.md)
