# IDEA-002: Trapped Trader Reversal

> Part of the [setup-ideas tracker](index.md) — statuses, catalog, and the
> backtesting queue live there; new ideas start from [_template.md](_template.md).

**Status:** Researched
**Source:** Footprint analysis, microstructure theory
**Complements:** Rebid/Reoffer (tpl_rebid_support, tpl_reoffer_resistance), Absorption pipeline

**Concept:** When traders chase a breakout that fails, their forced liquidation accelerates the reversal. The existing absorption pipeline already detects passive orders absorbing aggressive flow — this wraps it into a failed breakout framework with explicit entry/stop/target logic.

**Setup — Failed Breakout Trap (Primary):**
- Context: Price breaks key level (IB high/low, prior day extreme, VAH/VAL)
- Trap signal: Heavy volume on breakout + absorption confirmed on footprint
  - `confirmed_absorption_event_count > 0` at the breakout level
  - Price fails to hold above/below the broken level within 2-5 minutes
- Entry: Short after price reverses back below the broken level (or long for breakdown)
- Stop: 6-10 NQ points above the failed breakout high
- Target 1: POC (20-40 NQ points)
- Target 2: Opposite VA boundary
- Win rate: 75-80% when absorption confirms (practitioner-reported)

**Setup — Stacked Imbalance Trap:**
- Context: 3+ consecutive price levels show imbalances in one direction
- Trap signal: Despite stacked imbalances, price fails to advance
  - `imbalance_count >= 3` but no range extension within 1-2 minutes
- Entry: Enter opposite direction on first lower close (for failed buy imbalances)
- Stop: Above the imbalance zone
- Target: POC or developing VAL/VAH
- NQ-specific: Use 4:1 or 5:1 imbalance ratio threshold (vs 3:1 on ES)

**Setup — Exhaustion Fade:**
- Context: Aggressive flow pushes price to session extreme
- Signal: Volume dries up + delta flattens (use `confirmed_exhaustion_event_count`)
- Entry: Fade after 2-3 bars of declining volume at extreme
- Stop: Beyond exhaustion extreme
- Target: VWAP or POC

**Implementation Notes:**
- Wire absorption events to level proximity (which key level was being tested when absorption occurred)
- Add `BreakoutState` tracking: level broken → volume check → hold/fail timer → trap classification
- Leverage `has_recent_confirmed_absorption` and distance fields already in MarketState
- Consider adding `failed_breakout_count` to MarketState

**Backtesting Hypotheses:**
> When price breaks IB high with absorption detected within 5 ticks, and price fails to hold for 3+ minutes, what is the R-distribution of fading the breakout targeting POC?

> When stacked imbalances (≥3 levels, ≥4:1 ratio) form but price fails to extend, what is the reversal probability within the next 15 minutes?
