# IDEA-022: Rally Offer Replenishment / Touch Offer Exhaustion

> Part of the [setup-ideas tracker](index.md) — statuses, catalog, and the
> backtesting queue live there; new ideas start from [_template.md](_template.md).

**Status:** Idea (2026-06-29)
**Source:** Live London Globex DOM observation session 2026-06-29; trader doctrine — *price only rises when buyers lift willing sellers at the offer; rallies often end when offers stop replenishing after being consumed ("no one left to sell to the buyers")*
**Complements:** IDEA-020 (DOM corroboration on zone lifecycle), IDEA-012 (liquidity vacuum after failed defense — different trigger, similar air-pocket mechanics), absorption/exhaustion pipelines
**Targets:** Sierra Chart ACSIL study (execution chart) + The Desk `depth` pipeline + MCP DOM tools + optional rules-engine condition fields

**Concept:** During an initiative rally, **sellers on the ask are fuel, not friction**. Each tick higher requires a buyer to lift displayed offer liquidity. A healthy uptrend shows a repeating microstructure loop:

1. Buyer lifts the ask (trade at offer)
2. Offer liquidity is consumed
3. **Fresh offers reload** at the same or next tick up
4. Repeat

A rally often stalls or ends when step (3) fails — the touch goes **hollow**: lifts clear the ask, nothing meaningful reloads, price may still tick up briefly on air, then the auction pauses or reverses. The trader's discretionary read (~50% of local rally endings) is specifically this **offer-replenishment failure at the touch**, distinct from:

- **Ask reload during extension** (healthy — sellers still willing to sell to buyers)
- **Far-book positioning** (e.g. contingency walls several points away — not the immediate touch mechanic)
- **Bid-side absorption** (defense below, not offer depletion above)
- **Generic high churn** (activity without distinguishing fill→refill vs fill→vacuum)

**Two measurable states:**

| State | DOM signature | Rally implication |
|-------|---------------|-----------------|
| **Healthy offer reload** | Ask decreases classified as fills; stacked quantity returns at/near touch within short window; `near_touch_ask_depth` stable or cycling | Buyers still have liquidity to lift — rally mechanism intact |
| **Touch offer exhaustion** | Lifts consume ask; reload latency rises or refill stops; `near_touch_ask_depth` collapses and stays thin; price may print new highs on minimal lift volume | "No one left to sell" — high-probability stall / end-of-leg tell |

**Why this is quantifiable:** The Desk already ingests Sierra `MarketDepthData` `.depth` files and cross-references `.scid` trade volume to separate **likely fills from likely pulls** (`aggregate_trade_volume_by_level` in `src/depth/mod.rs`; exposed via `get_pull_stack_activity`, `get_liquidity_behavior_at_level`, `explain_book_reaction`). `DomSummary` already carries touch-adjacent fields: `near_touch_ask_depth`, `ask_pull_rate`, `refill_rate`, `touch_level_churn_per_minute`, `pull_stack_bias`. What does **not** exist yet is a **directional, rally-scoped** metric that answers: *after buyers lift the offer during an up-leg, does the offer come back?*

**Proposed metrics (v1 — implementable without new data sources):**

1. **`ask_refill_rate`** (ask-only) — same formula as today's combined `refill_rate` (`stacked / removed` on the ask side only), computed over a rolling 30–60s window at the touch band (best ask ± N ticks).
2. **`post_fill_replenish_ratio`** — for each ask decrease classified as a **fill** at price *P*, did displayed ask quantity at *P* or *P + tick* return above threshold within *T* ms (e.g. 500 ms–2 s)? Ratio over the window = replenishment health.
3. **`touch_offer_depletion_score`** — `ask_fills / (ask_fills + ask_post_fill_reloads)` during an up-tape segment (price making higher highs). Rises toward 1.0 as lifts stop being replenished.
4. **`vacuum_lift_count`** — price ticks up ≥ N ticks while `near_touch_ask_depth` ≤ threshold and ask fill volume is below baseline — air-pocket lifts.
5. **`rally_offer_exhaustion_state`** (enum) — `Healthy` | `Thinning` | `Exhausted`, derived from composite: new/high-near-high price + falling ask refill + collapsing near-touch ask depth + optional pace spike on low lift volume.

**Context gating (avoid false signals):**

- Scope to **initiative direction** — measure ask replenishment only when tape/regime indicates an up-leg (positive session or leg delta, price above VWAP, higher-high structure, or explicit "rally leg" detector). Mirror for down-legs on the bid side.
- Distinguish **reload** from **spoof pull** — reload follows a classified fill; pull-without-fill is not replenishment failure.
- Require **minimum touch churn** — exhaustion is meaningful only when the rally had been actively trading two-sided at the touch (avoid declaring exhaustion in a dead market).

**Sierra Chart ACSIL study (trader-facing):**

- Custom study or chart-region indicator on the execution chart, fed by Sierra's native market depth + last trade (no MCP dependency at screen time).
- Display suggestions: offer-replenishment health meter (green/yellow/red), post-fill reload markers at the touch, optional alert when `Exhausted` fires on a new high.
- Thresholds should be session-pace aware (London Globex vs RTH open) — same pattern as IDEA-019's adaptive volume bar logic.

**The Desk / MCP integration (agent-facing):**

- Add computed fields to `DomSummary` / `MarketState`: `ask_refill_rate`, `touch_offer_exhaustion_state`, optional rolling `post_fill_replenish_ratio`.
- New or extended MCP tools: e.g. `get_touch_offer_health` (live + historical window) returning the metrics above with staleness and confidence labels; wire into `get_dom_regime_summary` narrative.
- Optional rules-engine fields for playbook alerts: `touch_offer_exhaustion_state`, `ask_refill_rate_below`, `vacuum_lift_detected` — **coaching only**, framed as "your playbook watches for offer depletion after initiative legs."
- Historical: replay `.depth` + `.scid` through backfill; log structured `touch_offer_exhaustion` events into `market_events` for frequency/conditional research (same pattern as absorption events).

**Relationship to existing ideas:**

- **IDEA-020 Stage 2** already lists "DOM corroboration" on zone retests — this idea is the **specific DOM mechanic** for rally-end detection at the touch, not zone lifecycle per se.
- **IDEA-012** vacuum is **failed defense + break**; offer exhaustion is **successful rally + fuel runs out** — complementary, different entry location.
- Today's `refill_rate` in `dom_summary` is **bid+ask combined** — useful context but **not sufficient** for the rally-offer thesis; ask-only and post-fill scoped variants are the core gap.

**Backtesting hypotheses (when instrumented):**

> During Globex/RTH up-legs that print a session or leg high, what fraction of highs are followed within 5–15 minutes by a ≥ X-tick pullback when `touch_offer_exhaustion_state = Exhausted` vs `Healthy`?

> Does `post_fill_replenish_ratio` below threshold at a new high predict stall better than generic `ask_pull_rate` or combined `refill_rate`?

> On IDEA-020 held buy-zone continuation entries, does ask replenishment staying healthy through the lift improve MFE before MAE vs entries where the touch was already hollow?

**Implementation sequencing (suggested — not started):**

1. **Rust prototype** — ask-only refill + post-fill replenish detector in `src/depth/mod.rs`; unit tests with synthetic `.depth` + trade alignment fixtures.
2. **Live surface** — expose via MCP; add to `get_dom_regime_summary` liquidity narrative.
3. **Sierra study** — parallel ACSIL indicator for discretionary chart (shared threshold constants in config doc, not hardcoded in both places).
4. **Event detector + research** — log exhaustion transitions; run conditional queries once N ≥ 30 on live-recorded depth history.
5. **Playbook** — only after backtest or live-eye validation; avoid repeating IDEA-012's over-firing mistake.

**Open questions to resolve before prototyping:**

- Default replenish window *T* (500 ms vs 2 s) and minimum reload size (contracts at touch for NQ).
- Whether "touch" = best ask only or best ask + 1 tick (NQ often lifts through stacked offers).
- Alert suppression after exhaustion (one-shot per leg vs recurring).
