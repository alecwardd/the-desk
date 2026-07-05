# IDEA-004: Multi-Timeframe CVD Divergence

> Part of the [setup-ideas tracker](index.md) — statuses, catalog, and the
> backtesting queue live there; new ideas start from [_template.md](_template.md).

**Status:** Researched
**Source:** Order flow analysis, extends delta pinch concept
**Complements:** Delta Pinch Reversal (tpl_delta_pinch_reversal), DNVA Retest (tpl_dnva_retest)

**Concept:** While delta pinch catches *sudden* inventory shifts, CVD divergence catches *gradual* exhaustion — price making new extremes while cumulative delta weakens. Adding multi-timeframe and level-specific delta divergence creates higher-conviction signals.

**Setup — Multi-TF CVD Divergence:**
- Identify divergence on higher timeframe (15M-1H) at a major level
- Confirm: Lower timeframe (1M-5M) shows the same divergence pattern
- Entry: Break of divergent bar in reversal direction on lower TF
- Stop: 4-6 NQ ticks beyond the divergence extreme
- Target: 1.5-2x risk; often 20-40 NQ points at major levels
- Win rate: 70-75% at major levels with footprint confirmation

**Setup — Delta at POC Divergence:**
- Context: Price returns to POC but delta *at that specific price level* differs from prior visit
- Bullish: Delta at POC more positive than prior visit → accumulation → buy rejection
- Bearish: Delta at POC more negative → distribution → sell rejection
- Stop: Beyond POC + 6-8 NQ ticks
- Target: Opposite VA boundary

**Setup — Exhaustion Delta Spike:**
- CVD makes >2 std dev spike from rolling mean without proportional price move
- Interpretation: Aggressive side exhausted; passive absorption winning
- Fade after reversal candle confirms
- Target: Mean reversion to VWAP or developing POC

**Implementation Notes:**
- Add rolling CVD statistics (mean, std dev) to `DeltaPipeline` for spike detection
- Add delta-at-POC tracking: store delta value at POC price each time POC is visited
- Multi-TF: compute delta on 1m, 5m, 15m, 30m aggregation windows
- Divergence detection: compare price new-high/low vs CVD new-high/low on each timeframe

**Backtesting Hypotheses:**
> When CVD diverges from price at a VWAP band or VA boundary, what is the reversal probability within 30 minutes?

> When delta at POC shifts direction between visits (positive → negative or vice versa), what is the directional outcome over the next hour?
