# IDEA-006: Volume Imbalance Bars (Lopez de Prado)

> Part of the [setup-ideas tracker](index.md) — statuses, catalog, and the
> backtesting queue live there; new ideas start from [_template.md](_template.md).

**Status:** Researched
**Source:** Lopez de Prado, "Advances in Financial Machine Learning" Ch. 2-3
**Complements:** All existing setups (infrastructure improvement)

**Concept:** Replace or supplement time-based bars with volume/tick/dollar bars that normalize information arrival. Imbalance bars fire at the *moment* information arrives — 3-8 bars earlier than time-bar traders see it.

**Bar Types:**
- **Volume bars**: New bar every N contracts (calibrate to ~1,000-1,500 bars/RTH)
- **Tick bars**: New bar every N transactions
- **Dollar bars**: New bar every $N notional (most stable across contract rolls)
- **Imbalance bars**: New bar when cumulative signed volume/ticks deviate from expected → earliest regime change detection

**Why It Matters:**
- Time bars over-sample quiet periods and under-sample active ones
- Volume/tick/dollar bars produce near-normal return distributions
- Improves statistical properties of ALL downstream signals
- Imbalance bars detect trend changes 3-8 bars earlier than equivalent time bars

**Implementation Notes:**
- Modify `.scid` processing loop to emit events on volume/tick thresholds in addition to time
- Start with volume bars (simplest): accumulate volume, emit bar when threshold reached
- Calibrate bar size using 20-day rolling session volume ÷ target bar count
- Later: implement imbalance bars per Lopez de Prado formula (E[b_t] exponentially weighted)

**Backtesting Hypothesis:**
> Do existing setups (OR5, rebid/reoffer, DNVA reversion) produce better R-distributions when evaluated on volume bars vs. 1-minute time bars?
