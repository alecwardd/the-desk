# IDEA-008: 0DTE Gamma Regime Trading

> Part of the [setup-ideas tracker](index.md) — statuses, catalog, and the
> backtesting queue live there; new ideas start from [_template.md](_template.md).

**Status:** Researched
**Source:** Dim/Eraker/Vilkov 2024, SpotGamma framework, CBOE research
**Complements:** Delta Pinch (regime context), VWAP Bands (positive gamma = mean reversion)
**Requires:** External GEX data feed (SpotGamma, Databento options chain, or manual levels)

**Concept:** 0DTE options create structural dealer hedging flows that shape NQ intraday behavior. Positive gamma = mean reversion (dealers sell rallies, buy dips). Negative gamma = momentum (dealers amplify moves).

**Setup — Positive Gamma Mean Reversion:**
- Context: Price above HVL/gamma flip; nearest high-gamma strikes identified
- Entry: Fade moves toward gamma wall strikes with footprint absorption confirmation
- Stop: 6-10 NQ points beyond gamma wall
- Target: Opposite gamma wall or POC (15-30 NQ points)
- Best in: Rotational days, mid-week (highest 0DTE OI)

**Setup — Negative Gamma Acceleration:**
- Context: Price below HVL; negative GEX confirmed
- Entry: Trade breakdowns through gamma support with momentum confirmation
- Stop: Above broken gamma level (8-12 NQ points)
- Target: Next gamma support or "blind spot" (50-100+ NQ points)
- Best in: Trend days, post-OpEx windows

**Key Statistics:**
- Markets close within SpotGamma 1-day estimated range 78% of the time
- Positive gamma: strengthens intraday reversal (statistically significant at 5-min and 60-min)
- Negative gamma: strengthens intraday momentum (statistically significant)

**Implementation Notes:**
- Phase 2 Databento integration (ADR-013) can provide raw options data
- Compute GEX from NDX/QQQ/SPX chains → map to NQ price levels
- Add `gamma_regime: GammaRegime` to MarketState (Positive, Negative, Neutral)
- Add `gamma_flip_level`, `call_wall`, `put_wall` to key levels
