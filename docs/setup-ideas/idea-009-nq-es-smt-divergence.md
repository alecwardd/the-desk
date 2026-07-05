# IDEA-009: NQ/ES SMT Divergence

> Part of the [setup-ideas tracker](index.md) — statuses, catalog, and the
> backtesting queue live there; new ideas start from [_template.md](_template.md).

**Status:** Researched
**Source:** ICT methodology, cross-asset analysis
**Complements:** All directional setups (confirmation layer)
**Requires:** ES .scid data feed from Sierra Chart

**Concept:** When ES and NQ diverge at structural levels, the lagging market provides a cleaner, higher-probability trade.

**Setup — SMT Divergence Entry:**
- Context: Both pulling back to support zone
- Divergence: ES makes lower low; NQ holds above prior low (bullish NQ)
- Entry: Buy NQ one tick above the divergent bar's high
- Stop: 1 ATR or below the NQ low that held
- Target: Prior NQ swing high
- Example: Oct 2, 2024 — ES made lower low, NQ held → NQ rallied 250+ points

**Setup — NQ/ES Ratio Mean Reversion:**
- Compute NQ/ES ratio intraday with rolling mean + std dev
- Entry: When ratio exceeds 2 std dev, fade the divergence
- Stop: Ratio extends to 3 std dev
- Target: Ratio returns to mean
- Natural hedge: pairs trade removes directional risk

**Implementation Notes:**
- Add ES .scid reader alongside NQ (same reader, different file)
- Compute NQ/ES ratio pipeline
- Add swing high/low detection to both instruments
- SMT divergence detector: compare swing structures across instruments
