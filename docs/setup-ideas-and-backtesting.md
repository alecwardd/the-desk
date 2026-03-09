# The Desk — Setup Ideas & Backtesting Research

Living document for trade setup ideas, backtesting hypotheses, and research findings. Each idea is tracked from concept through validation.

---

## How to Use This Document

| Status | Meaning |
|--------|---------|
| **Idea** | Concept identified, not yet researched or coded |
| **Researched** | Supporting evidence gathered, mechanics understood |
| **Prototyped** | Pipeline or detection logic implemented |
| **Backtesting** | Running through historical .scid data |
| **Validated** | Backtest results confirm edge; ready for template |
| **In Playbook** | Added to setup_templates.rs and active |
| **Rejected** | Tested and found no reliable edge |

---

## Priority 1 — Implementable with Existing Pipelines

### IDEA-001: Opening Drive Classification

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

---

### IDEA-002: Trapped Trader Reversal

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

---

### IDEA-003: Naked VPOC Magnet Trade

**Status:** Researched
**Source:** Auction Market Theory, volume profile analysis
**Complements:** Single Print Continuation (tpl_single_print_continuation), Session Inventory (tpl_session_inventory_clear)

**Concept:** Track POCs from prior sessions that price has not revisited ("naked" VPOCs). These act as price magnets — the market tends to gravitate toward unreconciled fair value.

**Setup — Naked VPOC Fill:**
- Maintain list of naked VPOCs from prior 5-10 sessions
- Entry: When developing profile + delta direction aligns toward a naked VPOC, enter on pullback
- Stop: Below nearest HVN cluster or developing VAL
- Target: The naked VPOC itself
- Statistics: ~6 exact VPOC bounces/month on index futures; 75%+ fill rate over multi-day horizon

**Setup — POC Magnet Mean Reversion:**
- Context: Price moves 60+ NQ points away from developing POC in a session
- Entry: First reversal signal (rejection candle, delta divergence) toward POC
- Stop: Beyond reversal extreme
- Target: POC level
- Win rate: 75%+ in ranging/consolidating markets

**Setup — Triple Confluence:**
- Context: HVN cluster aligns with previous day's POC AND a Fibonacci level (61.8%)
- Entry: Rejection trade at triple confluence
- Stop: Beyond the cluster
- Target: Opposite VA boundary
- Win rate: Claimed 85%+ (practitioner)

**Implementation Notes:**
- Add `naked_vpocs: Vec<NakedVpoc>` to `LevelsPipeline`
  - Struct: `{ session_date: String, price: f64, created_at: f64 }`
  - On each trade, check if price crosses any naked VPOC → mark as filled
  - Persist across sessions via database
- Add `prior_pocs` tracking in `session_summaries` or a dedicated table
- Composite profiles (5-day, 10-day, 20-day) as a future extension

**Backtesting Hypotheses:**
> What percentage of naked VPOCs get filled within 1, 3, 5, and 10 sessions?

> When price approaches a naked VPOC with confirming delta (session delta in approach direction), what is the bounce rate at the VPOC?

> What is the R-distribution when entering at a naked VPOC with a stop 10 NQ points beyond?

---

### IDEA-004: Multi-Timeframe CVD Divergence

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

---

### IDEA-005: Session Transition Sweep Patterns

**Status:** Researched
**Source:** Multi-session analysis, institutional flow patterns
**Complements:** Session Inventory Clear (tpl_session_inventory_clear)

**Concept:** Session transitions (Asia→London, London→RTH) create predictable liquidity sweep patterns. London almost always sweeps one side of the Asian range. The direction of RTH relative to the London sweep is a strong directional signal.

**Setup — Asia Range Sweep → London Continuation:**
- Pre-market: Mark Asia session high/low (6 PM - 2 AM ET)
- London open (3 AM ET): Watch for sweep of Asia high or low
- Entry: After London sweeps Asia range and shows displacement in opposite direction, enter on first pullback
- Stop: Beyond the Asia range extreme that was swept
- Target 1: Opposite side of Asia range
- Target 2: Full London session range extension (50-100+ NQ points)

**Setup — Three-Session Alignment:**
- Context: Asia, London, and RTH all move in same direction
- Entry: Any pullback to intraday VWAP or developing POC
- Stop: Below developing VAL (longs) / above developing VAH (shorts)
- Target: Extended range — highest conviction for trend days

**Setup — London-RTH Gap Direction:**
- Tuesday gaps fill ~70%
- Monday gap-ups fill only 53%
- If gap is in London's direction → continuation; if opposed → gap fill probable

**Implementation Notes:**
- Already have `globex_or30_high/low`, `london_or60_high/low` in MarketState
- Add `asia_session_high/low` tracking to `LevelsPipeline`
- Add sweep detection: price exceeds Asia high/low then reverses → "sweep" event
- Add three-session alignment flag to MarketState

**Backtesting Hypotheses:**
> When London sweeps the Asia high and then reverses, what is the probability RTH continues in the reversal direction?

> On three-session alignment days, what is the average range extension beyond IB?

---

## Priority 2 — Infrastructure Upgrades

### IDEA-006: Volume Imbalance Bars (Lopez de Prado)

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

---

### IDEA-007: Microstructure Regime Detection

**Status:** Researched
**Source:** HMM literature, Park & Kownatzki 2024, Lopez de Prado 2018
**Complements:** All setups (meta-filter)

**Concept:** Classify the current microstructure regime in real-time and use it as a meta-filter for all playbook setups. Run momentum setups in trending regimes, mean-reversion setups in rotational regimes, reduce size in transition regimes.

**3-State Model:**
1. **Trend** — High directional autocorrelation, expanding range, persistent order flow imbalance
2. **Rotation** — Low autocorrelation, contracting range, balanced order flow
3. **Transition/High-Vol** — Elevated realized vol, regime uncertainty

**Simpler Volatility Regime Detector (start here):**
- Compute 5-min realized volatility using log returns
- Compare to 20-day rolling average at same time-of-day
- RV ratio > 1.5: Trending → momentum setups
- RV ratio 0.7-1.3: Normal → full playbook
- RV ratio < 0.7: Compressed → breakout imminent, reduce reversion setups

**Advanced: Hidden Markov Model:**
- 3-state HMM on returns + volatility at 1-min frequency
- Academic Sharpe > 2.0 pre-cost on e-mini S&P500
- Requires: state estimation library in Rust or pre-computed in Python/exported

**Implementation Notes:**
- Start with the volatility ratio approach (simple, no ML dependency)
- Add `regime: MicrostructureRegime` to MarketState
- Rules engine checks regime before evaluating setups
- Later: implement HMM in Rust using `nalgebra` for matrix ops

**Backtesting Hypothesis:**
> What is the win rate improvement when filtering DNVA reversion and VWAP band setups to only fire in Rotation regime (RV ratio 0.7-1.3)?

---

## Priority 3 — Requires External Data

### IDEA-008: 0DTE Gamma Regime Trading

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

---

### IDEA-009: NQ/ES SMT Divergence

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

---

## Priority 4 — New Detection Logic Required

### IDEA-010: Fair Value Gap with Order Flow Confirmation

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

---

## Backtesting Queue

Ordered by expected information value × implementation ease:

| # | Hypothesis | Setup | Data Needed | Priority |
|---|-----------|-------|-------------|----------|
| 1 | Open Drive + RVOL ≥ Elevated → pullback to VWAP win rate | IDEA-001 | session_summaries, events | High |
| 2 | Absorption at IB break + price fails to hold 3 min → fade R-dist | IDEA-002 | absorption events, IB levels | High |
| 3 | Naked VPOC fill rate within 1/3/5/10 sessions | IDEA-003 | session_summaries POC + ticks | High |
| 4 | CVD divergence at VA boundary → reversal within 30 min | IDEA-004 | delta pipeline, events | Medium |
| 5 | London sweep of Asia range → RTH direction prediction | IDEA-005 | Globex session data | Medium |
| 6 | Volume bars vs time bars: R-distribution comparison for existing setups | IDEA-006 | .scid tick data | Medium |
| 7 | Regime filter (RV ratio) improves DNVA/VWAP band win rate | IDEA-007 | session_summaries, 5-min RV | Medium |
| 8 | Stacked imbalances (≥3, ≥4:1) fail → reversal probability | IDEA-002 | footprint data | Medium |
| 9 | Narrow IB (<0.7x avg) → breakout continuation rate | IDEA-001 | session_summaries IB range | Low |
| 10 | Three-session alignment → range extension beyond IB | IDEA-005 | multi-session data | Low |

---

## Research Sources

| Source | Topics | Confidence |
|--------|--------|-----------|
| Lopez de Prado, "Advances in Financial Machine Learning" (2018) | Volume clock, imbalance bars, regime detection | Very High |
| Dalton, "Markets in Profile" | Opening types, day types, AMT | Very High |
| Dim, Eraker, Vilkov (2024) — SSRN 4692190 | 0DTE gamma effects | High |
| Garmash (2025) — SSRN 5329719 | 0DTE gamma hedging | High |
| Park & Kownatzki (2024) — SSRN 4872960 | Microstructure regimes, volatility scaling | High |
| CBOE Research | 0DTE market impact | High |
| Adams, Fontaine, Ornthanalai (2024) — Bank of Canada | 0DTE market dynamics | High |
| Hawkes process forecasting — arxiv 2408.03594 | Order flow clustering | Medium-High |
| ICT/SMC practitioner community | FVG, SMT divergence, session sweeps | Medium |
| SpotGamma | GEX levels, gamma regime | Medium-High |

---

*Last updated: 2026-03-09*
