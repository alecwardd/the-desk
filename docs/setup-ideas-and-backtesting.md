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

## March 2026 Research Snapshot

Grounding for the additions below. This pass combined:
- Local sample from `~/.the-desk/data.db`: 3.53M raw ticks, 191,819 `market_events`, 222 `session_summaries`
- Valid RTH sample: 81 usable RTH sessions from 2025-11-28 through 2026-03-06
- Current-market research as of 2026-03-09 on 0DTE, dealer gamma, CME liquidity, and around-the-clock NQ flow

### Style Inference From Existing Playbook

The current system clearly encodes a discretionary NQ/MNQ style built around:
- Market Profile / auction context first
- Levels as locations, not entries
- Delta, liquidity, and inventory confirmation before execution
- OR5 / IB / DNVA / DNP / VWAP / rebid-reoffer / session inventory / pinch concepts
- London and RTH handoff awareness

### Local Findings That Matter

These are the highest-signal observations from the local history sample:
- **Double Distribution dominates.** 52 of 81 valid RTH sessions were classified `DoubleDistribution`. Only 7 of 81 were `Trend`.
- **London did not carry cleanly into RTH.** London and RTH closed in the same direction only 41.5% of the time; reversal happened 58.5% of the time.
- **One-sided IB extension was cleaner than generic IB extension.**
  - `up_only`: 12 sessions, 75.0% closed up
  - `down_only`: 8 sessions, 62.5% closed down
  - `both_sides`: 43 sessions, noisy / mixed
- **Raw pinch was not compelling as a standalone directional edge.** Higher-severity pinch events did not show strong session-close alignment in the current sample.
- **Absorption failure looked more actionable than absorption itself.**
  - RTH `absorption_confirmed` with `direction=down` aligned with down closes only 38.9%
  - RTH `absorption_invalidated` with `direction=down` flipped to opposite-direction close behavior 58.8%

### Instrumentation Caveats

Do not use these fields for serious strategy selection until they are repaired:
- `signal_outcomes` is currently dominated by one custom setup (`Volume Value Area Traverse`) with clearly broken `time_exit` / excursion behavior
- `single_prints_direction` in `session_summaries` is currently not useful for statistical slicing
- `poor_high` / `poor_low` flags are sparse or incomplete in the current stored sample

### Regime-First Conclusion

The strongest conclusion from this pass is not "add more standalone setups." It is:

> Add regime overlays first, then decide which existing setups are even allowed to fire.

Current local evidence suggests:
- Use **initiative / continuation logic** only when the day is proving one-sided and accepting away from balance
- Use **inventory-clear / mean-reversion / repair logic** when the session is behaving like a double-distribution migration or London-to-RTH unwind
- Treat **pinch**, **OR5**, and **raw absorption** as context-dependent, not standalone edge

---

## Priority 0 — Regime Overlay

### IDEA-000: Regime-Gated Setup Selector

**Status:** Researched
**Source:** Local 2025-11-28 through 2026-03-06 database study; 0DTE / dealer gamma literature; CME liquidity research
**Complements:** All existing setup templates

**Concept:** Stop treating every setup as always-on. Add a top-level regime selector that determines which setup families are valid:
- **Initiative / continuation**
- **Responsive / mean reversion**
- **Transition / stand aside**

The regime layer should drive which existing templates are active, not just how they are narrated.

**Local Rationale:**
- Most valid RTH sessions in the current sample were `DoubleDistribution`, not clean trend days
- London-RTH reversal was more common than London-RTH continuation
- One-sided IB extension had meaning; generic IB extension did not
- Raw pinch did not show enough standalone value to justify unrestricted firing

**Primary Regime Buckets:**
1. **One-Sided Acceptance**
   - High RVOL
   - One-sided IB extension
   - Price accepted above/below VWAP and DNP
   - No meaningful opposite-side extension
   - Allowed setup families:
     - OR5 continuation
     - IB Extension Play
     - Single Print Continuation
     - Rebid / Reoffer hold
2. **Migration / Inventory Clear**
   - Double-distribution behavior
   - Both-side extension or London unwind into RTH
   - Acceptance back into prior value or current value
   - Allowed setup families:
     - DNVA retest
     - VWAP band repair
     - Session inventory clear
     - London inventory unwind
3. **Transition / Liquidity Failure**
   - Mixed direction
   - Failed absorption
   - Liquidity pulling / pace expanding through defended level
   - Allowed setup families:
     - Absorption failure / liquidity vacuum
     - Failed-breakout trap
   - Reduce or disable:
     - Blind continuation entries

**Implementation Notes:**
- Add a top-level `regime_selector` or `setup_family_gate` to `MarketState`
- Inputs can be built from existing pipelines:
  - `day_type`
  - `balance_state`
  - IB extension state
  - London and overnight session direction
  - VWAP / DNP acceptance
  - absorption confirmed vs invalidated
  - pace percentile / RVOL
- Rules engine should check regime before evaluating setup conditions

**Backtesting Hypotheses:**
> Does gating OR5, IB Extension, and Single Print Continuation to one-sided acceptance regimes improve win rate versus ungated firing?

> Does gating DNVA, VWAP band, and session inventory setups to migration / inventory-clear regimes improve expectancy?

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

### IDEA-011: One-Sided IB Extension Acceptance

**Status:** Researched
**Source:** Local 2025-11-28 through 2026-03-06 database study
**Complements:** IB Extension Play (tpl_ib_extension), OR5 Mid Retest (tpl_or5_mid_retest)

**Concept:** The useful signal is not "IB extension happened." It is whether extension stayed one-sided or became two-sided. Two-sided extension usually means migration / auction, not trend acceptance.

**Local Statistics:**
- `up_only`: 12 sessions, 75.0% closed up
- `down_only`: 8 sessions, 62.5% closed down
- `both_sides`: 43 sessions, mixed / noisy
- `none`: 18 sessions

**Setup — One-Sided Acceptance Continuation:**
- Context:
  - First valid IB extension is one-sided
  - Opposite-side extension does not print
  - RVOL >= Elevated
  - Price remains accepted above VWAP + DNP for longs, below for shorts
- Entry:
  - First pullback to the extension origin, VWAP, OR5 mid, or developing value edge
- Stop:
  - Back inside IB or through the acceptance level
- Target:
  - 0.5x / 1.0x / 1.5x IB extensions
  - Late-session trend continuation only if opposite extension still absent

**Setup — Extension Failure Reclassification:**
- If the opposite-side extension prints:
  - Cancel continuation bias
  - Reclassify the day as migration / double-distribution until proven otherwise
  - Switch to responsive setups (DNVA, VWAP-band, inventory-clear, failed-break)

**Implementation Notes:**
- Add a session-level `ib_extension_state` enum:
  - `None`
  - `UpOnly`
  - `DownOnly`
  - `BothSides`
- Store the first extension timestamp and direction
- Use it as a hard filter for IB continuation and OR5 continuation logic

**Backtesting Hypothesis:**
> When the first IB extension remains one-sided for at least 30 minutes and RVOL >= Elevated, what is the R-distribution of trading the first pullback in extension direction?

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

### IDEA-012: Absorption Failure / Liquidity Vacuum

**Status:** Researched
**Source:** Local 2025-11-28 through 2026-03-06 database study; CME liquidity research
**Complements:** IDEA-002 Trapped Trader Reversal, Rebid/Reoffer, Absorption pipeline

**Concept:** The better signal may be the *failure* of a defended level, not the original absorption itself. A failed defense plus liquidity pull creates a vacuum move that can travel faster than the original defense setup.

**Local Statistics:**
- RTH `absorption_confirmed`, `direction=down`: aligned with down closes only 38.9%
- RTH `absorption_invalidated`, `direction=down`: flipped to opposite-direction close behavior 58.8%

This is not enough to call it validated, but it is enough to promote failure-of-defense into a first-class research track.

**Setup — Failed Absorption Reversal / Vacuum:**
- Context:
  - Absorption detected at a key level
  - Price does not reject cleanly
  - Absorption invalidates or times out
  - DOM shows pulling through the defended level
  - Pace expands into the break
- Entry:
  - Through the failed zone, not at the original defense price
- Stop:
  - Back inside the defended absorption zone
- Target 1:
  - Next nearby key level
- Target 2:
  - Opposite value edge if the move becomes inventory-clearing

**Critical Rule:**
- Do not treat visible resting size as sufficient evidence.
- Require:
  - failed defense
  - pace expansion
  - liquidity pull / inability to refill

**Implementation Notes:**
- Extend absorption tracking with:
  - `absorption_state = detected | confirmed | invalidated`
  - `time_to_invalidation_ms`
  - `liquidity_pull_rate`
  - `pace_at_failure`
- Tie invalidation to level context:
  - IB high / low
  - prior day high / low
  - VAH / VAL
  - DNVA boundary

**Backtesting Hypothesis:**
> When absorption at a key level invalidates within X minutes and pace percentile expands above Y, what is the directional follow-through over the next 15 and 30 minutes?

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

### IDEA-016: VWAP Pipeline Enhancements (Dual Session + Anchored)

**Status:** Idea
**Source:** QA review of `vwap.rs` pipeline, March 2026
**Complements:** VWAP Band Zone Entry (tpl_vwap_band_zone), all VWAP-referencing setups

**Concept:** The current VWAP pipeline is mathematically correct and incremental, but it only supports a single session-anchored VWAP at a time. Two enhancements would increase its value as a trading reference:

**Enhancement 1 — Dual VWAP (Globex + Developing RTH):**

Currently VWAP resets fully at each session boundary (6 PM ET for Globex, 9:30 AM ET for RTH). This means:
- During Globex, there is one VWAP covering Asia + London (correct — London does not reset it)
- At RTH open, the Globex VWAP is discarded and a fresh RTH VWAP begins

The problem: Globex VWAP is a meaningful reference level during the first 30-60 minutes of RTH, especially on London-to-RTH handoff and gap days. Losing it at 9:30 removes context the trader needs.

- Add a second `VwapPipeline` accumulator to `PipelineEngine` (e.g., `vwap_prior_session`)
- At RTH open, snapshot the Globex VWAP + bands into `prior_globex_vwap`, `prior_globex_vwap_1sd_upper/lower`
- Expose in MarketState for the first 60-90 minutes of RTH, then let it age out
- Zero additional per-tick cost (just a snapshot at boundary)

**Enhancement 2 — Anchored VWAP:**

Allow VWAP to be anchored from a user-specified event or time, not just the session open. Common anchors:
- Previous day's high/low (naked VPOC equivalent for VWAP)
- Significant absorption event
- IB high/low break
- OR5 break

- Add a small `AnchoredVwap` struct (same `sum_pv / sum_v` math, separate accumulator)
- Allow 1-3 active anchored VWAPs at a time via MCP tool (e.g., `anchor_vwap { from_timestamp_ms }`)
- Each anchored VWAP accumulates independently and can be queried or cleared
- Useful for playbook rules that reference "VWAP from the break" or "VWAP from the session low"

**Implementation Notes:**
- Enhancement 1 is trivial — one extra `VwapPipeline` instance + snapshot at boundary
- Enhancement 2 requires MCP tool integration and a small vec of active anchors
- Both are O(1) per tick, no recalculation
- Add `prior_globex_vwap`, `prior_globex_vwap_1sd_upper`, `prior_globex_vwap_1sd_lower` to MarketState
- Add `anchored_vwaps: Vec<AnchoredVwapState>` (capped at 3) with MCP create/clear tools

**Backtesting Hypotheses:**
> On London-to-RTH unwind days (IDEA-014), does prior Globex VWAP act as support/resistance during the first 60 minutes of RTH?

> When VWAP is anchored from the IB break point, does price respect the anchored VWAP ±1SD bands more reliably than session VWAP bands for continuation entries?

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

### IDEA-013: Gamma-Gated Setup Overlay

**Status:** Researched
**Source:** Local 2025-11-28 through 2026-03-06 database study; Cboe March 2026 volume data; Dim/Eraker/Vilkov; Adams/Fontaine/Ornthanalai
**Complements:** IDEA-000 Regime Selector, IDEA-008 0DTE Gamma Regime Trading
**Requires:** External gamma / wall / flip data

**Concept:** Gamma should not be treated as a standalone setup. It should be used as a selector for which of *your existing setups* are appropriate.

**Current-Market Motivation (as of 2026-03-09):**
- Cboe reported SPX 0DTE volume hit a record 63% of SPX trading in February 2026
- NQ already has Monday-Friday weekly expiries on CME
- Recent literature suggests regime dependence matters more than blanket "0DTE causes volatility" claims:
  - Positive dealer gamma tends to strengthen reversal behavior
  - Negative dealer gamma tends to strengthen momentum behavior
  - Broad market impact can be modest on average, so the useful application is *filtering*, not narrative overreach

**Overlay Rules:**
- **Positive gamma / inside major wall**
  - Favor:
    - DNVA retest
    - VWAP band repair
    - failed-breakout traps
    - session inventory clear
  - De-emphasize:
    - blind breakout continuation
- **Negative gamma / outside major wall**
  - Favor:
    - OR5 continuation
    - one-sided IB extension acceptance
    - single-print continuation
    - acceleration-zone hold
  - De-emphasize:
    - passive mean reversion

**Implementation Notes:**
- Use the same gamma data feed planned in IDEA-008
- Add:
  - `gamma_regime`
  - `inside_major_gamma_wall`
  - `distance_to_call_wall`
  - `distance_to_put_wall`
- Feed those fields into the regime selector first, then the setup templates

**Backtesting Hypothesis:**
> Does positive-gamma gating improve DNVA / VWAP-band expectancy, and does negative-gamma gating improve OR5 / IB-extension expectancy, versus ungated baseline?

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

### IDEA-014: London Inventory Unwind Into RTH

**Status:** Researched
**Source:** Local 2025-11-28 through 2026-03-06 database study
**Complements:** Session Inventory Clear (tpl_session_inventory_clear), DNVA Retest (tpl_dnva_retest), VWAP Band Zone Entry (tpl_vwap_band_zone)

**Concept:** In the current local sample, London direction was more likely to unwind than continue into RTH. This suggests a dedicated handoff setup: trade the unwind only when RTH opens back into value and inventory begins clearing.

**Local Statistics:**
- London and RTH closed same direction only 41.5%
- Reversal happened 58.5%
- Reverse handoff days were mostly high-RVOL and often `DoubleDistribution`

**Setup — London Inventory Unwind:**
- Context:
  - London trended materially
  - RTH opens back inside prior value, overnight value, or current developing value
  - DNP / VWAP reclaim confirms clearing
  - No clean one-sided acceptance away from value
- Entry:
  - First pullback after reclaim of DNP / VWAP / value edge
- Stop:
  - Back through the reclaim level or back outside accepted value
- Target 1:
  - Developing POC or prior close
- Target 2:
  - Opposite side of current value if unwind becomes full migration

**Do Not Use When:**
- London delta and price both remain one-sided through the RTH open
- RTH immediately shows one-sided IB extension acceptance
- Gamma / event regime strongly favors continuation instead of repair

**Implementation Notes:**
- Add a `london_rth_handoff_state`:
  - `Continuation`
  - `Unwind`
  - `Unclear`
- Inputs:
  - London open/close direction
  - RTH open relative to prior / overnight value
  - DNP / VWAP acceptance
  - early RTH delta sign

**Backtesting Hypothesis:**
> When London trends but RTH opens back inside value and reclaims DNP/VWAP, what is the probability of a move to POC or opposite value edge before IB completes?

---

### IDEA-015: Post-Macro / Post-Earnings Jump Repair-or-Go

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

---

## Backtesting Queue

Ordered by expected information value × implementation ease:

| # | Hypothesis | Setup | Data Needed | Priority |
|---|-----------|-------|-------------|----------|
| 1 | One-sided vs both-sided IB extension: first pullback expectancy | IDEA-011 | session_summaries, IB extension events | High |
| 2 | London trends, RTH opens back in value, DNP/VWAP reclaim → unwind probability | IDEA-014 | multi-session summaries, delta, VWAP | High |
| 3 | Absorption invalidation + pace expansion at key level → 15/30 min follow-through | IDEA-012 | absorption events, pace, key levels | High |
| 4 | Open Drive + RVOL ≥ Elevated → pullback to VWAP win rate | IDEA-001 | session_summaries, events | High |
| 5 | Regime selector improves OR5 / IB / DNVA / VWAP family expectancy | IDEA-000 | session_summaries, events, setup outcomes | High |
| 6 | Naked VPOC fill rate within 1/3/5/10 sessions | IDEA-003 | session_summaries POC + ticks | Medium |
| 7 | CVD divergence at VA boundary → reversal within 30 min | IDEA-004 | delta pipeline, events | Medium |
| 8 | London sweep of Asia range → RTH direction prediction | IDEA-005 | Globex session data | Medium |
| 9 | Volume bars vs time bars: R-distribution comparison for existing setups | IDEA-006 | .scid tick data | Medium |
| 10 | Positive-gamma gating vs negative-gamma gating on existing setup families | IDEA-013 | options / gamma data + setup outcomes | Medium |
| 11 | Stacked imbalances (≥3, ≥4:1) fail → reversal probability | IDEA-002 | footprint data | Medium |
| 12 | Narrow IB (<0.7x avg) → breakout continuation rate | IDEA-001 | session_summaries IB range | Low |
| 13 | Three-session alignment → range extension beyond IB | IDEA-005 | multi-session data | Low |
| 14 | Prior Globex VWAP as S/R in first 60 min of RTH on unwind days | IDEA-016 | session VWAP snapshots, ticks | Low |
| 15 | Anchored VWAP from IB break: band respect vs session VWAP bands | IDEA-016 | IB break events, ticks | Low |

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
| Cboe volume report (2026-03-04) | SPX 0DTE share of volume | High |
| CME around-the-clock liquidity note (2025) | NQ after-hours volume and earnings response | High |
| CME liquidity beyond order-book depth (2025) | Liquidity vacuum / fill-rate framing | High |
| Božović (2025) — SSRN 5223127 | Intraday jump clustering around open / close | High |
| Hawkes process forecasting — arxiv 2408.03594 | Order flow clustering | Medium-High |
| ICT/SMC practitioner community | FVG, SMT divergence, session sweeps | Medium |
| SpotGamma | GEX levels, gamma regime | Medium-High |

---

*Last updated: 2026-03-09*
