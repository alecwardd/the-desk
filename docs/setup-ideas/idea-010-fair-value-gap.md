# IDEA-010: Fair Value Gap with Order Flow Confirmation

> Part of the [setup-ideas tracker](index.md) — statuses, catalog, and the
> backtesting queue live there; new ideas start from [_template.md](_template.md).

**Status:** Researched
**Source:** ICT/SMC methodology combined with order flow
**Complements:** Rebid/Reoffer zones (similar concept — gaps as zones)

**Concept:** FVGs represent genuine institutional imbalances. Combining with order flow confirmation (footprint, delta, absorption) filters out low-quality gaps. 70-80% of FVGs eventually fill.

**Setup — FVG Retest with Footprint Confirmation:**
- Identify FVG on 5M-1H at a major level (VAH, VAL, prior day high/low)
- Wait for retrace into FVG zone
- Enter on footprint absorption + delta divergence at consequent encroachment (50% of gap)
- Stop: Beyond FVG boundary
- Target 1: Origin of the move that created the FVG
- Target 2: Next liquidity pool (prior swing)

**Three-Layer Confirmation Model:**
1. Liquidity sweep (stop hunt above/below key level)
2. FVG formation (imbalance after sweep)
3. Order block (institutional entry zone)
All three align → highest probability

**Implementation Notes:**
- Need candle-based FVG detection logic (three-candle pattern)
- Build multi-timeframe candle aggregation from tick data
- FVG zone tracking with fill status (like rebid/reoffer zone lifecycle)
- Consequent encroachment = 50% level of gap
