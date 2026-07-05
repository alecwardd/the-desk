# IDEA-001: Opening Drive Classification

> Part of the [setup-ideas tracker](index.md) — statuses, catalog, and the
> backtesting queue live there; new ideas start from [_template.md](_template.md).

**Status:** Researched
**Source:** Dalton AMT framework, IB/ORB statistics
**Complements:** OR5 Mid Retest (tpl_or5_mid_retest), IB Extension Play (tpl_ib_extension)

**Concept:** Classify the opening type within the first 15-30 minutes of RTH to predict the day's character *before* IB completes. Use the classification to filter which setups are active for the rest of the session.

**Opening Types (Dalton):**
1. **Open Drive** — No retrace past open price in first 5-15 min. Strongest trend day predictor.
2. **Open Test Drive** — Tests one direction, rejects, then drives. Predicts Normal Variation.
3. **Open Rejection Reverse** — Opens one direction, reverses sharply. Range day or opposite-direction trend.
4. **Open Auction** — Two-sided trade near open. Predicts Normal Variation or Neutral.

**Key Statistics:**
- NQ single-breaks IB 80% of sessions (6-month NY session sample)
- Single break continues in that direction 73% of the time
- Double breaks happen only 27% of the time
- High or low of day set in first 30 min ~50% of the time; first 60 min ~75%
- 30-min ORB continuation rate: 67% on NQ

**Classification Inputs (already available):**
- RTH open price vs. prior day VA (VAH, VAL, POC) — `levels` pipeline
- Overnight range width — `levels` pipeline (overnight_high, overnight_low)
- IB high/low and 20-day rolling average — `tpo` pipeline + `session_summaries`
- OR5 break direction — `or5` pipeline

**Setup — Open Drive Continuation:**
- Entry: First pullback to VWAP or OR5 mid after drive direction established
- Stop: Below the open price
- Target: IB 1.5x–2x extensions
- Filter: RVOL >= Elevated

**Setup — Narrow IB Breakout Anticipation:**
- Context: IB range < 0.7x 20-day average (compute from session_summaries)
- Entry: First break of IB with delta confirmation
- Stop: Back inside IB
- Target: 0.5x, 1.0x, 1.5x IB extensions
- Rationale: Narrow IB = coiled spring; breakout is imminent and directional

**Setup — IB Midpoint Retest After Break:**
- IB midpoint retest occurs 44.9% of the time after IB break
- Bounce confirms 41.3% of the time; reversal to opposite 39.1%
- Filter with delta/footprint to determine which

**Implementation Notes:**
- Add `OpeningType` enum to `day_type.rs` (OpenDrive, OpenTestDrive, OpenRejectionReverse, OpenAuction)
- Classify at minute 15 and minute 30 using open price, retrace depth, and OR range
- Store in MarketState as `opening_type`
- Add 20-day rolling IB range to RVOL pipeline or session comparison

**Backtesting Hypothesis:**
> When opening type = OpenDrive AND RVOL >= Elevated, what is the win rate of trading the first pullback to VWAP in the drive direction?
