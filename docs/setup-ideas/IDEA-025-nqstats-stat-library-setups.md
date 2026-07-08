---
id: IDEA-025
title: NQStats Statistical Setup Library
status: Researched
regime: [any, rthOpen, globex]
related: [IDEA-005, IDEA-011, IDEA-014]
companionSpecs: []
mcpPointers:
  - tool: run_backtest
    setupId: IDEA-025
    note: Split these source-derived concepts into child hypotheses before treating any statistic as verified.
hypothesisAnchor: false
---

# IDEA-025 - NQStats Statistical Setup Library

> Source-research capture from NQStats. These are not validated Desk edges. All numeric values below are source-reported point-in-time facts from NQStats, captured on 2026-07-05, and should be treated as hypothesis-generation material only.

<!-- stats: point-in-time -->

## Sources Verified

All source pages returned HTTP 200 during the capture pass:

- https://nqstats.com/
- https://nqstats.com/am_tbr.html
- https://nqstats.com/hour_stats.html
- https://nqstats.com/aln_sessions.html
- https://nqstats.com/ib_breaks.html
- https://nqstats.com/noon_curve.html
- https://nqstats.com/rth_breaks.html
- https://nqstats.com/1h_continuation.html

## Thesis

NQStats is useful as an external idea generator because its concepts are mostly structural, session-scoped, and directly backtestable from NQ time-and-sales/session bars:

- Defined opens and range anchors: 8:00 TBR open, hourly opens, IB, prior RTH range.
- Simple breach/reversion logic with explicit time windows.
- Overnight session structure: Asia, London, and NY handoff.
- Directional confluence from where a range closed and the order in which extremes formed.

The useful work is not to import the published win rates. The useful work is to translate the setup mechanics into Desk hypotheses, run our own replay/backtests, and compare the behavior under our data, contracts, fees, execution assumptions, and market regimes.

## Prioritized Testing Queue

| Rank | Setup candidate | Why start here | First Desk test |
|---:|---|---|---|
| 1 | AM TBR early +/-0.25 SDEV reversion to 8:00 open | Large source sample, clean trigger, explicit target, easy invalidation/timeout, symmetric long/short version | After 8:00 ET, compute rolling 20-session sample stdev of prior session percent net changes; when price first touches +/-0.25 stdev from 8:00 open, test reversion to 8:00 open before 12:00, with subtests for 8:01-8:29 vs 8:30-8:59 first touches. |
| 2 | IB combined confluence break | Already aligns with Desk IB work and IDEA-011; high implementation fit | At 10:30, require next bar open inside IB. If IB closes above midpoint and low was set first/high last, test high break by noon/close. Mirror for below-midpoint/high-first low break. |
| 3 | Noon Curve Q2 break confluence | Simple 8:00-16:00 geometry, useful as a PM bias/target selector rather than an immediate entry | Build Q1 8:00-10:00 and Q2 10:00-12:00 ranges. If Q2 breaks only Q1 high, test probability that AM low and PM high become full-day extremes; mirror for Q1 low. |
| 4 | 9AM hour continuation | Strong source-reported 9AM signal, easy to compute, likely useful as bias overlay | Classify 9:00-10:00 hour direction; test same-direction close for NY session and full Globex session, plus drawdown and entry timing variants. |
| 5 | Hourly first-breach reversion to current hour open | Many events and useful intraday scalp logic, but requires careful execution assumptions | For hours 8:00-15:00 ET, if current hour opens inside prior hour range, test first breach of prior hour high/low by one tick and reversion to current hour open before hour close. Prioritize 9:00 and 15:00 hours first. |
| 6 | ALN overnight pattern gate | Good as a context/regime feature, less direct as a standalone entry | Classify Asia/London relationship at 8:00 close; test NY break of London high/low and conditional behavior after first break. |
| 7 | RTH prior-day range open scenarios | Useful risk/target context, but likely less of a standalone entry | Classify 9:30 open vs prior RTH high/low; test gap-hold, gap-fill, and opposite extreme breach probabilities with regime overlays. |

## Setup 1 - AM TBR +/-0.25 SDEV Reversion

Source page: https://nqstats.com/am_tbr.html

### Mechanics

- TBR open: 8:00 ET.
- Window: 8:00-12:00 ET.
- Level construction: project +/-0.25 stdev from the 8:00 TBR open.
- Stdev lookback: rolling 20-day sample standard deviation of prior session percent net changes.
- Trigger: first touch of either +0.25 or -0.25 stdev level.
- Target/outcome: reversion to the 8:00 TBR open before 12:00.
- Measure MAE as extension beyond the touched level before reversion; measure MFE as continuation beyond the open after reversion.

### Source-Reported Facts

- Sessions studied: 2,572, covering 2016-2026.
- Sessions touching either side: 2,545.
- No-touch sessions: 27.
- +0.25 touches: 1,252, with 927 reverting to open, or 74.0%.
- -0.25 touches: 1,293, with 964 reverting to open, or 74.6%.
- First-touch timing matters:
  - 8:xx first touches: 1,506 touches, 79.0% overall reversion.
  - 9:xx first touches: 980 touches, 69.5% overall reversion.
  - 10:xx first touches: 48 touches, 39.6% overall reversion.
  - 11:xx first touches: 11 touches, 9.1% overall reversion.
- 8:01-8:29 sub-band:
  - +0.25: n=314, 82.8%.
  - -0.25: n=361, 83.1%.
- 8:30-8:59 sub-band:
  - +0.25: n=418, 75.1%.
  - -0.25: n=413, 76.5%.
- Cumulative reversion for all sessions:
  - By 9:00: +16.3% / -17.2%.
  - By 10:00: +60.4% / -60.2%.
  - By 11:00: +71.2% / -70.8%.
  - Final 12:00: +74.0% / -74.6%.
- 8am-touched sessions cumulative reversion:
  - By 9:00: +27.9% / -28.8%.
  - By 10:00: +68.2% / -68.7%.
  - By 11:00: +76.1% / -76.7%.
  - Final 12:00: +78.4% / -79.6%.

### Backtest Notes

- Treat the source's 8:31 minute cluster as a data-quality warning, not a standalone edge. NQStats shows 236 events at 8:31, far above adjacent minutes, which may reflect bar/timestamp construction.
- First implementation should avoid minute-specific optimization. Test broader bands: 8:01-8:29, 8:30-8:59, 9:00-9:59, and after 10:00.
- Need to decide whether "prior session percent net change" means RTH, Globex, or custom 8:00-12:00 TBR-to-TBR change before implementing.
- Candidate trade forms:
  - Fade +0.25 back to 8:00 open.
  - Fade -0.25 back to 8:00 open.
  - Optional continuation follow-through after reversion, using MFE distribution as a separate hypothesis.

## Setup 2 - Hourly First-Breach Reversion

Source page: https://nqstats.com/hour_stats.html

### Mechanics

- Session: 8:00-16:00 ET.
- Qualifier: current hour open must be strictly inside the prior hour high-low range.
- Trigger: current hour first breaches the prior hour high or low by one tick, 0.25 points.
- Outcome: after the breach flag is set, any bar wick touches or crosses the current hour open before the hour closes.
- Only first breach per hour is counted.
- Breach timing is bucketed into 20-minute segments: 00-20, 20-40, 40-60.

### Source-Reported Facts

- Total breach events: 17,701.
- Overall reversion rate: 61.5%.

| Hour ET | Breaches | Reverted | Rev % | 00-20 | 20-40 | 40-60 | Avg MAE % | Avg MFE % |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 8:00 | 2,295 | 1,460 | 63.6 | 74.6 | 45.1 | 17.3 | 0.130 | 0.186 |
| 9:00 | 2,485 | 1,868 | 75.2 | 87.4 | 63.0 | 32.9 | 0.203 | 0.307 |
| 10:00 | 2,299 | 1,364 | 59.3 | 68.0 | 33.3 | 12.5 | 0.241 | 0.294 |
| 11:00 | 2,018 | 1,097 | 54.4 | 67.2 | 28.1 | 9.6 | 0.193 | 0.219 |
| 12:00 | 2,065 | 1,148 | 55.6 | 68.3 | 31.0 | 13.6 | 0.165 | 0.197 |
| 13:00 | 2,115 | 1,237 | 58.5 | 71.1 | 32.8 | 14.7 | 0.154 | 0.199 |
| 14:00 | 2,177 | 1,274 | 58.5 | 68.5 | 35.8 | 20.3 | 0.169 | 0.218 |
| 15:00 | 2,247 | 1,443 | 64.2 | 76.2 | 50.3 | 19.9 | 0.181 | 0.231 |

### Backtest Notes

- Start with 9:00 because it has the strongest source-reported rate and a large sample.
- 00-20 minute breaches dominate. Late-hour breaches look much weaker and should be a separate reject/avoid test.
- Need realistic execution assumptions: after prior-hour breach, can we enter near the breach level with acceptable slippage, or is the measured return to open not tradable?
- Useful variants:
  - Reversion-to-open only.
  - Reversion-to-open then continuation beyond open.
  - Only take when current hour open is near prior-hour midpoint.
  - Exclude major economic release windows.

## Setup 3 - ALN Asia/London/New York Patterns

Source page: https://nqstats.com/aln_sessions.html

### Mechanics

- Asia session: 20:00-02:00 ET.
- London session: 02:00-08:00 ET.
- New York outcome window: 08:00-16:00 ET.
- Pattern is classified at the 8:00 bar close.
- Outcomes track whether NY breaks London high, London low, or both.

### Source-Reported Pattern Facts

| Pattern | Definition | Frequency | NY breaks London high | NY breaks London low | Breaks both |
|---|---|---:|---:|---:|---:|
| P1 London engulfs Asia | London high > Asia high and London low < Asia low | 558, 22.0% | 71.5 | 70.4 | 42.5 |
| P2 Asia engulfs London | Asia high > London high and Asia low < London low | 175, 6.9% | 81.1 | 74.9 | 56.0 |
| P3 Partial engulf up | London high > Asia high and London low remains inside Asia range | 1,042, 41.0% | 80.8 | 65.5 | 47.6 |
| P4 Partial engulf down | London low < Asia low and London high remains inside Asia range | 767, 30.2% | 68.6 | 75.0 | 44.6 |

Additional source facts:

- P2 had zero "breaks neither" sessions in the 10-year sample.
- P3 has the clearest high-break lean.
- P4 has the clearest low-break lean.
- When the opposite side breaks first in P3/P4, the original directional edge is reported to degrade materially.

### Backtest Notes

- This should probably be a regime/context feature before it becomes an entry.
- First tests:
  - P3 -> probability and expectancy of NY London-high break.
  - P4 -> probability and expectancy of NY London-low break.
  - Conditional after first break: does first break direction predict one-sided day, two-sided day, or trap?
- Session boundary correctness is critical. Verify CME/Sierra timestamps across DST.

## Setup 4 - Initial Balance Break Confluence

Source page: https://nqstats.com/ib_breaks.html

### Mechanics

- IB window: 09:30-10:30 ET.
- Qualifier: first post-IB bar opens inside the IB range.
- Breach: any bar wick trades at least one tick, 0.25 points, beyond IB high or low.
- Outcomes measured by noon and by 16:00 close.
- Confluences:
  - IB close above or below its midpoint.
  - Which IB extreme was established last.

### Source-Reported Facts

- Qualifying days: 2,571.
- By noon:
  - Either side breached: 82.5%, 2,120 days.
  - IB high breached: 47.0%, 1,208 days.
  - IB low breached: 39.8%, 1,023 days.
- By close:
  - Either side breached: 96.1%, 2,471 days.
  - IB high breached: 62.9%, 1,617 days.
  - IB low breached: 54.9%, 1,412 days.
- IB close above midpoint:
  - n=1,405.
  - High break by noon: 70.1%.
  - High break by close: 82.3%.
- IB close below midpoint:
  - n=1,156.
  - Low break by noon: 65.8%.
  - Low break by close: 76.5%.
- Low set first, high set last:
  - n=1,298.
  - High break by noon: 68.2%.
  - High break by close: 80.9%.
- High set first, low set last:
  - n=1,269.
  - Low break by noon: 57.8%.
  - Low break by close: 71.2%.
- Combined bullish confluence: close above midpoint plus low first/high last:
  - n=1,114.
  - High break by noon: 74.0%.
  - High break by close: 84.0%.
- Combined bearish confluence: close below midpoint plus high first/low last:
  - n=974.
  - Low break by noon: 67.9%.
  - Low break by close: 78.0%.

### Backtest Notes

- This is the cleanest fit with the existing Desk IB work.
- Backtest should separate "breach probability" from a tradable entry:
  - Entry at 10:30 in direction of confluence.
  - Entry on pullback to IB midpoint.
  - Entry on first failed opposite-side move.
  - Entry only when OR/IB acceptance confirms one-sided behavior.
- Track max adverse excursion before breach target, not just whether the breach occurs.

## Setup 5 - Noon Curve and Q2 Break Confluence

Source page: https://nqstats.com/noon_curve.html

### Mechanics

- Custom TBR: 8:00-16:00 ET.
- AM window: 8:00-12:00.
- PM window: 12:00-16:00.
- Thesis: full-day high and low usually form on opposite sides of noon.
- Quarter split:
  - Q1: 8:00-10:00.
  - Q2: 10:00-12:00.
  - Q3: 12:00-14:00.
  - Q4: 14:00-16:00.
- Q2 break of Q1 high/low is used as confluence for which side forms the full-day opposite extreme.

### Source-Reported Facts

- Sessions studied: 2,479.
- Opposite-side sessions: 1,805, or 72.81%.
- Both high and low in AM: 541, or 21.82%.
- Both high and low in PM: 133, or 5.37%.
- Extreme formation times:
  - AM high/low pooled mean: 10:12, stdev 72 minutes.
  - PM high/low pooled mean: 14:04, stdev 88 minutes.
- Percent change from 8:00 open:
  - AM high mean +0.46%, median +0.31%, n=1,322.
  - AM low mean -0.52%, median -0.36%, n=1,565.
  - PM high mean +1.07%, median +0.88%, n=1,157.
  - PM low mean -1.29%, median -1.05%, n=914.
- AM extreme formation:
  - n=2,305 after excluding 174 sessions where Q1 set both AM high and low.
  - Q1 sets AM high or AM low: 84.95%.
  - Q1 sets AM high: 39.61%.
  - Q1 sets AM low: 45.34%.
- Q2 break confluence in opposite-side sessions:
  - Q2 breaks Q1 high only: n=794, 44.0% of opposite-side pool; AM low/PM high outcome 82.12%.
  - Q2 breaks Q1 low only: n=631, 35.0%; AM high/PM low outcome 72.42%.
  - Q2 breaks both: n=253, 14.0%; near coin flip.
  - Q2 breaks neither: n=127, 7.0%; near coin flip.

### Backtest Notes

- This is probably a target-selection and PM-bias model, not a direct 12:00 market order.
- Candidate hypotheses:
  - If Q2 breaks only Q1 high, buy pullback after noon for PM high extension.
  - If Q2 breaks only Q1 low, sell pullback after noon for PM low extension.
  - Avoid or de-rate when Q2 breaks both or neither.
- Need to test whether the edge survives after requiring executable entry mechanics and stop placement.

## Setup 6 - Prior RTH Range Open Scenarios

Source page: https://nqstats.com/rth_breaks.html

### Mechanics

- Classify 09:30 RTH open relative to prior day's RTH high/low.
- Breach: wick more than one tick, 0.25 points, beyond prior RTH level.
- Close probabilities use 16:00 bar close.
- Outcome sets:
  - Whether session closes beyond the breached/open-side level.
  - Whether price reaches the opposite prior RTH extreme.

### Source-Reported Facts

- RTH sessions studied: 2,488, March 2016-March 2026.
- Opening distribution:
  - Opens above prior RTH high: 654 sessions, 26.3%.
  - Opens below prior RTH low: 363 sessions, 14.6%.
  - Opens inside prior RTH range: 1,471 sessions, 59.1%.
- Gap up:
  - Close above prior RTH high: 69.9%, 457 days.
  - Close below prior RTH high: 30.1%, 197 days.
  - Does not breach prior RTH low: 88.1%, 576 days.
- Gap down:
  - Close below prior RTH low: 59.5%, 216 days.
  - Close above prior RTH low: 40.5%, 147 days.
  - Does not breach prior RTH high: 90.4%, 328 days.
- Opens inside prior RTH range:
  - No breach either side: 17.7%, 261 days.
  - Breaches one side only: 74.0%, 1,088 days.
  - Breaches both sides: 8.3%, 122 days.

### Backtest Notes

- This is useful context for gap-hold/gap-fill framing.
- Gap-up statistics likely include long-term NQ upward drift; do not import as a long bias without regime adjustment.
- Test with filters:
  - Overnight range location.
  - Prior day trend/range type.
  - Opening drive behavior.
  - VWAP/DNP reclaim or rejection.

## Setup 7 - 1H Continuation

Source page: https://nqstats.com/1h_continuation.html

### Mechanics

- Signal hours:
  - 18:00-19:00 ET, measured by 19:00 close vs 18:00 open.
  - 09:00-10:00 ET, measured by 10:00 close vs 09:00 open.
- Direction:
  - Green hour = signal hour closes above its open.
  - Red hour = signal hour closes below its open.
- Outcome windows:
  - Full session: 17:00 close vs 18:00 open.
  - NY equity session: 16:00 close vs 09:30 open.
- Continuation means the outcome window closes in the same direction as the signal hour.

### Source-Reported Facts

- Total sessions: 2,472.

| Signal | Direction | Outcome | n | Wins | Losses | Continuation % | Avg continuation pts | Avg reversal pts |
|---|---|---|---:|---:|---:|---:|---:|---:|
| 18:00 hour | Green | Full session | 1,284 | 773 | 511 | 60.2 | +119.19 | -117.34 |
| 18:00 hour | Red | Full session | 1,163 | 573 | 590 | 49.3 | -133.37 | +108.17 |
| 18:00 hour | Green | NY session | 1,284 | 695 | 589 | 54.1 | +91.48 | -99.17 |
| 18:00 hour | Red | NY session | 1,163 | 523 | 640 | 45.0 | -110.69 | +93.28 |
| 09:00 hour | Green | Full session | 1,293 | 895 | 398 | 69.2 | +125.35 | -109.35 |
| 09:00 hour | Red | Full session | 1,173 | 693 | 480 | 59.1 | -134.31 | +92.61 |
| 09:00 hour | Green | NY session | 1,293 | 913 | 380 | 70.6 | +101.01 | -85.94 |
| 09:00 hour | Red | NY session | 1,173 | 737 | 436 | 62.8 | -113.59 | +72.61 |

### Backtest Notes

- The 09:00 signal is much more interesting than the 18:00 signal in the source data.
- Treat as a directional bias first, not a naked entry:
  - Test whether 09:00 direction improves IB/OR continuation entries.
  - Test drawdown from 10:00 entry to 16:00 close.
  - Test pullback-entry variants after 10:00 instead of immediate chase.
- Compare separately by:
  - Gap up/down/inside prior RTH range.
  - IB close above/below midpoint.
  - ALN pattern.
  - AM TBR touch/reversion state.

## Implementation Checklist

- Add exact session boundary helpers or reuse existing ones:
  - Asia 20:00-02:00 ET.
  - London 02:00-08:00 ET.
  - NY/RTH 09:30-16:00 ET.
  - TBR 08:00-12:00 and 08:00-16:00.
- Normalize contract/session data before comparing to source claims. The source says 2016-2026; our local data availability and contract rolls may differ.
- Separate probability-of-touch from tradable expectancy. Many source stats are event probabilities, not complete trading systems.
- For every child hypothesis, report:
  - Sample size.
  - Win rate.
  - Expectancy in R.
  - MFE/MAE distribution.
  - Signals per active session.
  - Slippage/commission assumptions.
  - Exclusion windows around major macro events.
- Avoid overfitting minute-level AM TBR observations until timestamp construction is verified.

## Suggested Child Hypotheses

- IDEA-025A: AM TBR +/-0.25 SDEV fade to 8:00 open before noon.
- IDEA-025B: IB combined confluence high/low break by noon and close.
- IDEA-025C: Noon Curve Q2-only break as PM extreme-direction bias.
- IDEA-025D: 9AM hour continuation as directional filter for RTH setups.
- IDEA-025E: Hourly first-breach reversion by hour and 20-minute segment.
- IDEA-025F: ALN pattern-gated NY London high/low breaks.
- IDEA-025G: Prior RTH open scenario gap-hold/gap-fill context.

## See also

- Hub stub: [setup-ideas-and-backtesting.md#idea-025](../setup-ideas-and-backtesting.md#idea-025)
- Setup ideas index: [index.md](index.md)
