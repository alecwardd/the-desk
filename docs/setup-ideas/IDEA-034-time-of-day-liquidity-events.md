---
id: IDEA-034
title: Time-of-day liquidity-event calendar — participation anomalies + continuation/reversal
status: Backtesting
regime: [any]
related: [IDEA-007, IDEA-031, IDEA-033]
companionSpecs:
  - docs/data-and-backtesting-guide.md
  - docs/setup-ideas-and-backtesting.md
  - docs/mcp/tool-reference.md
mcpPointers:
  - tool: get_research_summary
    note: Call first before any historical claim; do not cache N in this file
  - tool: query_ticks
    note: Raw tick access for bucket volume / tape-speed derivation; pair with ingest_raw_ticks_from_scid + get_raw_tick_ingest_gaps for coverage checks
  - tool: get_session_history
    note: Per-session OHLC / volume / delta context rows (RTH vs whole-Globex only)
  - tool: query_distribution
    note: Session-summary metrics only (total_volume, session_delta, rvol_ratio, …); no time-of-day bucket columns
  - tool: query_event_frequency
    note: Segment-filterable event stats once a locked event definition exists; not a substitute for Stage 1 derivation
hypothesisAnchor: false
---

# IDEA-034 — Time-of-day liquidity-event calendar

> Point-in-time research/backtest plan captured **2026-07-24**.
> Queue source: second-brain `queue/ready-for-agent.md` (2026-07-13) ← inbox note
> "large moves in the market based on time". The original capture was docs-only;
> current execution status is recorded below. No signal claim is made.

## Current gate status (2026-08-11)

| Gate | Status | Evidence / consequence |
|------|--------|------------------------|
| Coverage check | **CLEARED** | [2026-07-24 coverage record](../backtests/2026-07-24-idea-034-stage1-coverage.md); preliminary counts are superseded by the accepted replay where they differ |
| Stage 1 bucket-stats artifact | **ACCEPTED** | Clean-`b63e83a` provenance verification matched the reviewed artifact exactly on the approved core metrics; see the [2026-08-11 verification record](../backtests/2026-08-11-idea-034-bucket-stats-verification.md) |
| Calendar/event-rate extraction | **NOT RUN / NOT AUTHORIZED** | Bucket coverage N is not event N; no participation calendar or named-window result exists yet |
| Stage 1 overall verdict | **OPEN** | Acceptance here clears only the bucket-stats evidence gate; it is not a Stage 1 pass |
| Stage 2 / live promotion | **LOCKED** | Requires a separately approved Stage 1 calendar result and later gates |

<!-- stats: point-in-time -->

## Origin

Trader observation: large liquidity enters and exits the market at predictable
clock times — plausibly on-the-hour (e.g. 02:00 / 03:00 / 04:00 ET, quoted in the
inbox note as ~2am/3am Central) when different participant classes come in and
out, and plausibly ~10 minutes before hour blocks when desks flatten before
their session closes. Order-flow microstructure should shift around these
windows. Knowing the reliable participation calendar — and recognizing
**irregular** events off-calendar — should improve trade timing, especially
during choppier Globex hours.

Three goals, in dependency order:

1. **Calendar:** identify windows where large participants reliably enter/exit.
2. **Anomalies:** identify off-calendar irregular events — a large player
   changing positioning, being stopped out, or adjusting to a new thesis.
3. **Behavior:** after an identified event, is there any continuation/reversal
   relationship? Events may be market-maker adjustment (direction-agnostic) or
   directional initiation/stop-out — the study does not assume which.

## Verdict framing (pre-backtest)

| Option | Meaning for this track | Pre-backtest stance |
|--------|------------------------|---------------------|
| **Adopt** | Promote a verified participation calendar + event-conditioned behavior stats into research context | **Blocked** until Stage 1–2 clear sample-size, control, and robustness gates |
| **Adapt** | Keep as offline research calendar with labeled confidence tiers | **Default** |
| **Skip** | Discard the hypothesis | **No** — directly targets the trader's stated Globex timing problem and is fully testable on trade-side data |

Success authorizes a **research calendar + gated verdicts only**. Any live
trading integration (agent context, coaching, alerts) is a separate, later
promotion step requiring trader confirmation.

---

## Repo-native definitions (locked for this plan)

These definitions are research contracts. They are **not** live `MarketState`
fields unless noted.

| Term | Repo-native meaning for IDEA-034 |
|------|----------------------------------|
| **Clock bucket** | A fixed 5-min window of the ET day (288 buckets), primary granularity. 1-min refinement used for tape-speed onset timing. Sensitivity: 1-min / 5-min / 15-min. |
| **Liquidity event ("large move")** | A bucket whose **executed volume** and/or **tape speed** (trades or contracts per second) exceeds the trailing same-bucket baseline — **not** a price-move threshold. Primary: bucket volume > 95th percentile of the trailing 20 like-days same-bucket distribution. Secondary definitions (multiple, pre-registered): (a) volume ≥ k × trailing same-bucket median, k ∈ {2, 3}; (b) tape-speed spike on 1-min granularity; (c) |signed delta| anomaly same-bucket. Price displacement (vs IDEA-033 ATR/RV) is reported as an **outcome**, not the detection criterion. Sensitivity: trailing window ∈ {10, 20, 40} days. |
| **Calendar event (regular)** | A bucket whose anomaly rate across the sample is significantly above the all-bucket base rate — a reliably recurring participation window. |
| **Irregular event** | A single-day anomaly in a bucket that is **not** a calendar window — candidate large-player positioning change / stop-out / thesis adjustment. Reported as its own cohort. |
| **Continuation / reversal** | Signed by the event's initial impulse direction (net delta-signed price move during the event bucket + 1 bucket). Continuation = same-direction net move beyond a noise band at horizon; reversal = opposite-direction. Reported as distributions of net move + MFE/MAE, never binary labels alone. |
| **Horizons** | +5 / +15 / +30 / +60 min post-event, pre-registered. |
| **Event dedup** | Anomalous buckets within 15 min merge into one event (onset = first anomalous bucket). Prevents one burst inflating N. |

**Timezone / sessions (code truth):** all times **Eastern Time**. Globex open
`18:00` ET, London segment start `02:00` ET (`LONDON_OPEN_ET` in `src/lib.rs`),
RTH `09:30–16:15` ET. The trader's inbox note quotes Central times; conversions
(CT = ET − 1h): 2am CT = 03:00 ET, 3am CT = 04:00 ET. All spec times are ET;
any CT mention is prose-only.

**Anchors:** candidate windows are **event-anchored + ET clock grid**. Event
anchors: Globex open 18:00, London open 02:00, RTH open 09:30, equity close
16:00, maintenance break 17:00–18:00. Clock-grid candidates: on-the-hour and
:50 (10 min pre-hour, the desk-flatten hypothesis). DST-mismatch weeks
(US/EU transitions) are flagged as a separate cohort or excluded — never
silently pooled.

**Instrument:** Primary **NQ** front month with explicit contract per window
(H6 / M6 / U6 across the sample). MNQ/ES/MES only as separately labeled folds;
no silent pooling.

---

## Coverage matrix (verified 2026-07-24)

| Input / field | Status | Provenance | Granularity | History (documented) | Gap |
|---------------|--------|------------|-------------|----------------------|-----|
| NQ `.scid` tick files | **Available** (external) | Sierra configured data dir; authoritative per data guide | Tick (time, price, volume, bid/ask) | Clean 4-symbol forward record since **2026-06-23**; backfills earlier with integrity caveats | NQH6 window repeatedly documented "gappy, double-distribution" (hub ~lines 778–791) — quality gate required |
| Bucket volume / tape speed / signed delta | **Derivable offline** | `.scid` replay, one-pass pattern of `src/research/nine_am_continuation.rs` (ET-minute clock constants) | 1-min / 5-min buckets | Same as tick coverage | No existing time-of-day move analytics; must be built as an offline study binary |
| `session_volume_curves` (RVOL buckets) | **Available** | `src/db/mod.rs` (session_date, session_type, bucket_index, cumulative_volume) | Bucketed per RTH / Globex | Same as session coverage | Whole-Globex only; closest existing baseline but not event detection |
| `session_summaries` (volume, delta, rvol_ratio, day type) | **Available** | Pipeline finalization / backfill | Per session_type × date | RTH + whole-Globex rows | **Globex not split into Asia/London rows** — segment attribution must come from tick timestamps (`classify_session_segment`, `src/lib.rs`) |
| `raw_ticks` DB cache | **Partial** | `ingest_raw_ticks_from_scid` | Tick | 30-day warm retention, older archived | Not a full-history store; study replays `.scid` directly |
| DOM / book liquidity (`depth_events`, `dom_feature_snapshots`) | **Recent-only** | `.depth` files durable; DB hot retention **7 days** | Depth events | Recent weeks only | **Parked:** true book-side liquidity variant is a later optional stage; this plan uses trade-side proxy only |
| 9AM continuation / IB campaign study pattern | **Available as template** | `src/research/nine_am_continuation.rs`, `src/research/ib_campaign.rs` + bins | Tick-exact offline replay | Campaign window **2025-12-15 → 2026-07-21** across H6/M6 rollovers, holiday exclusions | Copy this pattern; do not mutate `run_backtest` |
| Gap detection | **Available** | Backfill gap logic (30-min threshold, `src/backfill.rs`) | Per session | — | Must be run per window to quantify Globex-hours holes; feeds the quality gate |

**Hard rule:** "liquidity" in this plan means **executed participation**
(volume / tape speed / delta). No claim about order-book liquidity unless a
separately labeled DOM stage is executed. If a day/bucket fails the quality
gate it is **excluded and reported**, never interpolated.

---

## Staged design (Stage 2 gated behind Stage 1)

### Stage 1 — Descriptive participation calendar (authorized by this spec)

1. `get_research_summary` → record coverage by session_type / root (no account
   or private paths in outputs).
2. Offline `.scid` replay (9AM-continuation pattern, isolated campaign DB):
   build per-day 5-min bucket volume, tape speed, signed delta for the full
   Globex+RTH day, segmented by repo constants (Globex 18:00→02:00, London
   02:00→09:30, RTH 09:30→16:15) as separately labeled folds.
3. Quality gate: per-day gap scan (backfill 30-min logic); exclude + report
   degraded days per bucket. Coverage table: actual N per bucket, date span,
   contract, missing days.
4. Baselines: trailing same-bucket distribution (window ∈ {10, 20, 40} like
   days) → anomaly flags per the locked definitions.
5. Calendar extraction: per-bucket anomaly rate vs all-bucket base rate;
   test pre-registered candidate windows (event anchors + on-the-hour + :50)
   as named hypothesis rows.
6. Robustness: re-run headline stats on the clean 2026-06-23+ subset; report
   agreement/disagreement.

**Pass Stage 1:** named buckets show anomaly rates significantly above baseline
with N ≥ 30 per bucket (AGENT.md Research Sample Size Policy: N < 20
insufficient; 20–29 directional; ≥ 30 reportable), stable across folds and the
clean-subset check → produce the **participation calendar** and unlock Stage 2.
**Fail:** no bucket separates, or N < 30 after quality gating → **PARK**;
report null result, do not proceed.

### Stage 2 — Event-conditioned continuation/reversal (gated; not authorized yet)

1. Requires Stage 1 pass. Event cohort = detected liquidity events (calendar +
   irregular as separate cohorts).
2. Controls: matched non-anomalous windows in the **same clock bucket**, plus
   the all-window distribution. No control, no claim.
3. Outcomes: net move + MFE/MAE at +5/+15/+30/+60 min, signed by initial
   impulse direction; distributions, not bare hit rates.
4. Comparison: event cohort vs matched controls at each horizon; costs stated
   as event-study market movement (no fill model unless separately approved).

**Adopt (research-only):** event cohort separates from matched controls at
pre-registered horizons with N ≥ 30 per compared bucket.
**Adapt:** weak/unstable separation → keep as context label only.
**Skip:** no separation after sensitivity.

### Stage 3 — DOM / book-liquidity variant (optional, parked)

Only if explicitly reopened: same event definitions on recent `.depth`
coverage, separately labeled, small-N caveats mandatory.

### Required outputs (when a future agent executes)

- Coverage table (actual N per bucket, date span, contracts, excluded days).
- Participation calendar (bucket, anchor label, anomaly rate, fold stability,
  clean-subset agreement).
- Irregular-event cohort summary (count, timing distribution vs calendar).
- Stage 2 tables (events vs matched controls, per horizon, per cohort) — if reached.
- Adopt / Adapt / Skip per stage with sample-size labels.
- No promotion to live context or playbook without trader confirmation.

---

## Falsifiable hypotheses

Per AGENT.md sample-size policy, every comparison needs N ≥ 30 **per bucket**.

### Stage 1

- **H1 (calendar exists):** Liquidity-event anomalies cluster in specific clock
  buckets: at least one candidate bucket shows an anomaly rate significantly
  above the all-bucket base rate, N ≥ 30 days, stable across folds.
  - *Falsify if* no bucket separates from base rate after sensitivity on
    trailing window, bucket size, and threshold definitions.
- **H2 (pre-registered windows):** The trader's candidate windows — London open
  (02:00 ET anchor), 03:00–04:00 ET (2–3am CT), on-the-hour grid, and :50
  pre-hour desk-flatten windows — carry elevated anomaly rates vs matched
  non-candidate buckets.
  - *Falsify if* candidate windows do not outperform a permutation of random
    bucket sets.
- **H3 (irregular cohort):** Off-calendar irregular events occur at a rate
  distinguishable from baseline noise and cluster near session transitions
  rather than uniformly.
  - *Falsify if* irregular timing is uniform (no structure to exploit).

### Stage 2 (only after Stage 1 passes)

- **H4 (behavioral skew):** Post-event price behavior differs from matched
  same-bucket controls: continuation **or** reversal skew at ≥ 1 pre-registered
  horizon, N ≥ 30 events per cohort (calendar vs irregular reported
  separately).
  - *Falsify if* event-vs-control distributions do not separate after the
    stated cost policy.

No hypothesis authorizes live signals, alerts, sizing, or playbook changes.

---

## Design controls

| Control | Requirement |
|---------|-------------|
| **Instrument** | NQ; explicit contract per window (H6/M6/U6); other roots only labeled separately |
| **Sessions** | Full day, segmented per repo constants; Globex folds primary (trader pain point); never mix unlabeled |
| **Anchors** | Event-anchored + ET clock grid; DST-mismatch weeks flagged or excluded |
| **Detection** | Multiple pre-registered definitions (percentile primary; k×median; tape-speed; delta); sensitivity table mandatory |
| **Controls** | Matched same-bucket non-anomalous windows + all-window baseline |
| **Leakage** | Trailing baselines use only prior like-days; no same-day future buckets in the baseline; walk-forward folds |
| **Quality gate** | Per-day gap scan; degraded days excluded per bucket and reported; clean 2026-06-23+ subset robustness check |
| **Rollover / holidays** | Explicit contract per window; holiday exclusions per campaign precedent; `scid_window_mismatch` warnings surfaced, never silent |
| **Costs** | Event-study market movement only; no fill simulation unless separately approved |
| **Sample** | `get_research_summary` + coverage table before any claim; per-bucket N ≥ 30 for reportable results |

---

## Relationship to existing ideas

| Idea | Relation |
|------|----------|
| [IDEA-033](IDEA-033-expected-range-atr-rv-iv-research-plan.md) | ATR/RV machinery reused for price-displacement **outcomes** and normalization; do not fork vol definitions |
| [IDEA-031](IDEA-031-session-range-compression-expansion.md) | Session transition taxonomy; shares Asia/London segmentation vocabulary |
| Hub IDEA-007 | RV-ratio / microstructure sketch — candidate feature family, not implemented |
| 9AM continuation / IB campaign (`src/research/`) | Implementation template for clock-time tick-exact offline studies |

---

## Explicit non-goals (this pass)

- No backtest execution, no `register_hypothesis` / `run_backtest`.
- No production pipeline, MCP tool, chart study, alert, or live agent context.
- No DOM/book-liquidity claims (trade-side proxy only; DOM parked to Stage 3).
- No signal integration, playbook activation, or "best times to trade" advice.
- No silent pooling across instruments, sessions, or DST-mismatch weeks.
- No copy of `private/`, account details, or raw DB dumps into this file.

## Recommended next action

1. Await explicit approval for **Stage 1 calendar/event-rate extraction** from
   the accepted bucket-stats evidence; preserve the pre-registered candidate
   windows, sensitivity rules, and isolated-research boundary.
2. Record a separate Stage 1 verdict from event rates and fold stability. Do
   not infer a pass from the bucket coverage N=131 result.
3. Stage 2 opens only on a documented Stage 1 pass and separate authorization.

## See also

- Hub stub: [setup-ideas-and-backtesting.md#idea-034](../setup-ideas-and-backtesting.md#idea-034)
- Setup index: [index.md](index.md)
- Data workflow: [docs/data-and-backtesting-guide.md](../data-and-backtesting-guide.md)
- Sample-size policy: `AGENT.md` (Research Sample Size Policy)
