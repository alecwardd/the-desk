# IDEA-015: Post-Macro / Post-Earnings Jump Repair-or-Go

> Part of the [setup-ideas tracker](index.md) — statuses, catalog, and the
> backtesting queue live there; new ideas start from [_template.md](_template.md).

**Status:** Researched
**Source:** CME around-the-clock liquidity research; jump-risk literature; local style fit
**Complements:** IDEA-000 Regime Selector, Session Inventory Clear, OR5 Mid Retest, DNVA Retest
**Requires:** External event calendar for clean automation; otherwise usable as a discretionary overlay

**Concept:** NQ is unusually exposed to post-earnings and macro jump risk. The useful setup is not "trade the news." It is classify the jump day into:
- **acceptance / continuation**
- **repair / re-entry into value**

**Why This Matters:**
- CME documented a 107% increase in Nasdaq futures volume in the hour after Nvidia earnings on 2025-02-26
- Jump risk clusters around the open and close in recent equity-index research

**Setup — Jump Acceptance Continuation:**
- Context:
  - Overnight or 8:30 ET shock moves price outside prior value
  - First pullback holds outside prior value
  - DNP / VWAP / delta remain aligned with shock direction
- Entry:
  - First pullback that holds outside value
- Stop:
  - Back inside prior value
- Target:
  - Next structural level, then session range expansion

**Setup — Jump Repair:**
- Context:
  - Shock move initially leaves prior value
  - Price then re-enters prior value
  - Delta pinches back through DNP / VWAP
  - Value re-acceptance confirmed
- Entry:
  - First pullback after re-entry / reclaim
- Stop:
  - Back outside value
- Target:
  - POC, prior close, or opposite value edge

**Implementation Notes:**
- External calendar improves automation, but core structure can be detected from price and session references alone
- Add an optional `event_day_context` flag if macro / earnings calendar is integrated later

**Backtesting Hypothesis:**
> On overnight gap or 8:30 ET shock days, does value re-entry plus DNP/VWAP reclaim outperform generic gap-fill or breakout logic?
