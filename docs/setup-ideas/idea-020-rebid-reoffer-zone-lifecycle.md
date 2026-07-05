# IDEA-020: Footprint Rebid/Reoffer Zone Lifecycle

> Part of the [setup-ideas tracker](index.md) — statuses, catalog, and the
> backtesting queue live there; new ideas start from [_template.md](_template.md).

**Status:** Stage 1 landed (2026-06-23); Stage 2 deferred. **Now the primary track** — the framework
into which the rejected IDEA-000 (regime) and IDEA-012 (absorption-failure) concepts were folded.
**Source:** Trader doctrine session 2026-06-23 (see memory `rebid-reoffer-zone-doctrine`)
**Complements / absorbs:** Rebid/Reoffer templates, Absorption pipeline; **supersedes IDEA-000** (regime
becomes a Stage-2 read derived from zone outcomes, not a standalone entry) and **IDEA-012** (a failed
defense / vacuum is a `Failed` zone in this lifecycle, anchored to a real level — see those entries'
2026-06-23 verdicts).

**Concept:** Redefine acceleration zones around the trader's actual model — **footprint stacked
one-sided delta** (≥5 consecutive levels at ~3:1, loose bands) with an initiative move away — instead
of the original 5-min-bar range proxy. Track a lifecycle whose *outcomes* are themselves signals:

- **Forming** → watchlist. **Retested** (price re-enters the band, ≤5-tick poke tolerated) → **entry**
  with a tight stop (high R:R). **Held** (fires back in zone direction) → continuation. **Failed**
  (extends ≥5 ticks beyond the poke *with acceptance* = dwell + building volume / accelerating tape)
  → trend change / not real initiative. **Abandoned** (never returns to the band) → strong-trend tell.

**Stage 1 (landed 2026-06-23):** directional, proximity- and status-aware zones from footprint stacked
delta (`footprint.stacked_imbalance_zones` + rewritten `rebid_reoffer.rs` lifecycle). Fixed
`active_rebid_zone`/`active_reoffer_zone` (were both `active_zone_count > 0`), implemented the dead
`rebid_zone_held`, and added `reoffer_zone_held` + `rebid_zone_retested`/`reoffer_zone_retested` (the
entry trigger). `RULES_ENGINE_SCHEMA_VERSION` 4→5. Acceptance =
`tape_dwell_at_current_price_ms` + (`pace_percentile` elevated OR `rvol_velocity > 0` OR
`tape_acceleration > 0`). Starting tunables: 5 levels, 3:1 ratio, 5-tick move-away / poke / failure
extension, ~20–30s dwell.

**Backtest finding (2026-06-24) — entry mechanics, not exits, are the limiter.** A full exit sweep on
the retest/held entries (job `1d690dee`, NQH6, isolated DB) confirmed the trader's win-rate intuition:
at a 1R target, win rates are **50–55%** (the original 3R target's 25% was an artifact). But expectancy
stays thin (best = rebid-held-long @ 2R, **+0.22R**, N=64). The smoking gun: **MAE p50 = 1.0R for *every*
variant** — half of all trades touch the full flat 3pt stop, which is *not* how the trader executes. So
the flat-point stop and the zone *definition* — not the exit target — cap the result.

**Stage 1.5 (in progress — make mechanics match execution):**
- **Zone-anchored exits (landed 2026-06-24):** `MarketState` now exposes `rebid_zone_low/high` and
  `reoffer_zone_low/high` (nearest zone band edges), wired as named levels in `resolve_price_expression`.
  A stop can now sit *just past the zone* (`named_level_offset` on `rebid_zone_low`, e.g. `offsetTicks:-4`)
  and a target at a structural level, instead of a flat point — directly attacking the MAE-p50=1R problem.
  Backtest next with zone-anchored stops vs the flat-point baseline.
- **Excursion diagnostic (2026-06-25, job `e76710a7`):** uncapping the target (12pt stop / 40pt target)
  showed trades run **~9–12 pt median MFE** (the earlier ~3 pt was a 9pt-target cap, not reality) — but
  still short of the 24–36 pt a high-R:R style wants. **Zone-anchored stops underperformed the flat 12 pt**
  on every rebid-long metric (so the Stage-1.5 anchored-exit idea was a miss; the named-level capability is
  retained but unused by default). Verdict: the limiter is the **entries**, not the exits.
- **Recent-window zone detection (landed 2026-06-25):** the Stage-1 detector read the *session-cumulative*
  footprint; the trader's zone is a *quick one-sided burst* then a leave. Detection now uses a recent
  window — `footprint.stacked_imbalance_zones_recent(...)` over `ZONE_FOOTPRINT_WINDOW_MS` (300 s, tunable),
  built by an efficient back-scan of recent trades. Re-backtest the zone entries with this detector (flat
  12 pt stop, ~9–12 pt target per the diagnostic) — ideally on the cleaner **live-recorded** data, since
  the gappy NQH6 backfill is a poor judge and we've largely exhausted what it can tell us.

**Stage 1.5 conclusion (2026-06-25) — mechanical cycle exhausted on the NQH6 backfill.** The recent-window
A/B (job `6e70e29e`) barely moved fire rate (N 64→69) and showed no clear MFE improvement; every entry/exit
variation across the full cycle (targets → stops → zone-anchoring → detection window) lands negative-to-
breakeven on this window (median favorable excursion ~6–12 pt with ~80% stop-hit at a 12 pt stop). **This is
not proof the setup has no edge** — it is proof the *gappy, double-distribution NQH6 backfill cannot answer
the question*, and that further mechanical tuning on it is motion without information. **Stop backtesting
this data.** Two paths actually resolve the uncertainty: (1) **live-eye validation** — rebuild the live
server to the recent-window detector (`70bec5c`), restart, and during an active RTH session compare the
code's `get_rebid_reoffer_zones` output against the trader's chart: *does the code find the zones the trader
actually trades?* That tests the one thing backtesting can't. (2) **Clean-data re-backtest** — let the
live-recorded 4-contract `.scid` (started 2026-06-23) accrue a few weeks, then re-run. Until one of those,
no further IDEA-020 backtest variations on NQH6.

**Stage 2 (deferred — revisit in a future build):**
- **Per-session zone aggregates** (formed / retested / held / failed / abandoned) rolled into an
  order-flow-sourced **regime input for IDEA-000**: many forming+held, few failing → trend; many
  failing → transition; many abandoned → strong trend. This is the highest-value follow-up — it gives
  the regime classifier a second, independent signal (the v1 regime gate was too loose).
- **DOM corroboration** (`dom_summary` bid/ask pull-rates): confirm the "original aggressor reloads +
  trapped passive side covers" mechanic on the retest. `dom_summary` is delayed context, not live book.
  **Rally-side variant:** see **IDEA-022** (touch offer replenishment vs exhaustion) for the specific
  DOM signature that marks many initiative rally ends — offers stop refilling after lifts.
- **Tick-adjacency** for "consecutive levels" (Stage 1 uses consecutive entries in the sorted footprint,
  matching existing `stacked_imbalances`); revisit if gappy bands cause false zones.

**Backtesting Hypotheses (after Stage 1 deploys):**
> What is the R-distribution of entering on a retest of a held footprint zone with a tight (≤5-tick) stop?

> Do sessions with a high abandoned-zone ratio close as trend days more often than the base rate?

**Research extension — zone establishment age & clearance velocity (2026-06-30):**

Live Globex observation (London handoff): overhead sell absorption / reoffer bands that had been
defending for minutes were **cleared quickly** once buyers printed held rebid and pace turned — the
push through **30036–30040** took far less time than the zones had been in place. Hypothesis: *how
long a zone has existed* and *how fast opposing flow clears it* may be independent, actionable
signals — not just zone status (`Held` / `Failed`).

| Metric | Definition (derivable today) | Question |
|--------|------------------------------|----------|
| **`zone_age_ms`** | `now − timestamp_ms` at status transition | Do fresher zones hold more often than stale ones? |
| **`time_to_retest_ms`** | `retested_ms − timestamp_ms` | Fast retest vs slow retest — which has better Held→continuation? |
| **`time_to_clear_ms`** | `failure_cross_ms → Failed` (or first trade through band with acceptance) | Does a zone cleared in &lt;X s predict follow-through better than a grind? |
| **`clearance_velocity_ticks_per_sec`** | Extension through band ÷ time from first cross to `Failed` | "Snap" clears vs slow acceptance — vacuum vs contested break |
| **`establishment_vs_clear_ratio`** | `zone_age_ms / time_to_clear_ms` | Long-established shelf cleared instantly → initiative tell? |

**Why this fits IDEA-020 (not a new pipeline):** `AccelerationZone` already stores `timestamp_ms`,
`retested_ms`, and `failure_cross_ms`; acceptance dwell is `FAILURE_DWELL_MS` (~25 s). What's
missing is **event logging + research queries** on those durations, not new detection logic.
Mirror the absorption pipeline's `time_to_invalidation_ms` pattern (IDEA-012).

**Backtesting hypotheses (add to queue once zone events are logged):**

> When a held reoffer zone with `zone_age_ms > Y` is cleared with `time_to_clear_ms < X` and pace
> percentile &gt; Z, what is 15/30-minute follow-through vs slow clears of the same band?

> Do rebid zones that reach `Held` within `time_to_retest_ms < X` outperform slow-forming held zones
> on MFE before MAE (same flat stop baseline as IDEA-020 Stage 1.5)?

> On sessions where many long-established zones are cleared quickly in one direction, does close
> direction align more often than the base rate? (Feeds IDEA-020 Stage 2 regime-from-zone-outcomes.)

**Implementation note:** Log structured `zone_status_transition` events (from → to, durations above) into
`market_events` during backfill/live; expose rolling "nearest zone age" on MCP for coaching context.
Do **not** add playbook alerts until sample size ≥ 30 on live-recorded depth history — same guardrail
as IDEA-022.
