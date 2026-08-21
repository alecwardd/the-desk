---
id: IDEA-035
title: Leg-to-Leg Volume/Delta Profile Engine (swing-anchored per-leg profiles)
status: Researched
regime: [any]
related: [IDEA-029, IDEA-020, IDEA-004, IDEA-033]
companionSpecs:
  - docs/data-and-backtesting-guide.md
  - docs/setup-ideas/IDEA-029-sierra-execution-chart-study-context.md
  - docs/mcp/tool-reference.md
mcpPointers:
  - tool: get_research_summary
    note: Call first before any historical claim; do not cache N in this file
  - tool: get_delta_profile
    note: Existing session/segment delta profile; leg engine reuses the same primitives over leg windows
  - tool: get_footprint
    note: Existing volume-at-price / bid/ask accumulator; leg profile is this structure over a leg window
  - tool: get_footprint_window
    note: Historical time-windowed footprint; validation reference for reconstructed leg profiles
  - tool: query_signal_outcome_distribution
    setupId: IDEA-035
    note: Future backtest outcome query; do not quote stats from this file
hypothesisAnchor: false
---

# IDEA-035 — Leg-to-Leg Volume/Delta Profile Engine

> Point-in-time **docs-only** research/engineering spec captured **2026-07-24**.
> Origin: second-brain inbox notes "Stacked Leg-To-Leg Volume Profiles"
> (2026-07-16), "delta rotation calculator in the-desk" + "the-desk trading
> IDEA-029" (2026-07-09), and the 2026-07-13 source card (Delta Dynamics
> leg-to-leg, OrderFlow Labs Leg 2 Leg Profiles). Definitions below were locked
> in a 2026-07-24 interview with the trader. This note does **not** run a
> backtest, build a pipeline, or make any signal claim.

<!-- stats: point-in-time -->

## Origin

The trader anchors volume and delta profiles on **swing points** — swing
highs/lows, session highs/lows, usually "excess" (a wick / TPO single-print
style rejection) — and reads each **leg** of the auction as its own
self-contained profile ("fractal auction market theory": the volume nodes of
any leg are value and interest on that timeframe, however small). On screen
today this is approximated by the OrderFlow Labs Delta Rotation Calculator
(1-min bar delta ranked as a percentile against a trailing window of bars) for
spotting the excess/spike that starts a leg, plus vendor leg-to-leg profile
studies (OFL Leg 2 Leg, Delta Dynamics) for the per-leg profiles.

Prior repo analysis (IDEA-029 Track C, and the 2026-07-09 review in the vault)
established two facts:

- The volume/delta-at-price primitive already exists
  (`src/pipelines/footprint.rs` — per-price bid/ask volume accumulator). A leg
  profile is that same structure accumulated over a leg window.
- There is **no** swing/leg/pivot detector anywhere in `src/pipelines/`. The
  missing piece is purely the leg-boundary model — a modeling problem, not an
  engineering one.

This spec locks that model quantitatively so it can be (a) backtested offline
from recorded `.scid` data and (b) translated into a Sierra Chart visual
configuration with the same math.

## Thesis

Auctions move in legs separated by swing points where initiative activity
exhausts (delta extreme + sharp rejection = excess). Treating each leg as its
own volume/delta profile gives a deterministic, multi-scale description of the
auction: where volume was accepted (HVN), rejected or skipped (LVN, shelves),
and whether participation in the current leg is building or tapering. If leg
boundaries are stable and the resulting levels carry information, stacked leg
profiles become the substrate for tradeable setups (IDEA-036 and successors)
and for agent-visible context (`get_leg_profile`).

## Locked definitions (research contracts)

These are the quantitative replacements for "by feel" reads. They are
**not** live `MarketState` fields unless a later promotion says so.

| Term | Locked definition |
|------|-------------------|
| **Leg** | A directional auction segment between two anchor extremes (anchor high → anchor low for a down leg, and vice versa). The current leg is *developing* until its terminating anchor is confirmed. |
| **ATR basis** | ATR(14) of 1-minute bars, computed from tick data. The only volatility measure for leg-boundary thresholds in this spec. |
| **Confirmed anchor** | A price extreme is a confirmed anchor once price retraces **≥ k × ATR(14,1m)** from it. k swept ∈ {0.25, 0.5, 0.75, 1.0}; default candidate 0.5. On confirmation, the *new* leg's profile is accumulated **retroactively from the extreme** (the wick belongs to the new leg), and the confirmation timestamp is recorded. |
| **Provisional anchor (delta-rotation trigger)** | A 1-min bar whose \|delta\| ≥ **p-th percentile** of the trailing **N** 1-min bars (p swept ∈ {80, 90, 95}, N swept ∈ {50, 100, 200}) **and** price then moves **≥ 0.25 × ATR(14,1m)** in the opposite direction within **3 bars**. Marks a candidate excess/swing point *before* the retracement condition confirms it. The displacement condition is what separates excess from a one-sided bar that keeps going. |
| **Two-state anchor model** | Provisional (delta trigger) → confirmed (retracement). A leg opened provisionally is labeled `developing` until confirmed; legs may also open on the retracement rule alone (price-only path). **Causality rule:** backtests may only use anchor/profile states whose trigger/confirmation timestamps precede the decision timestamp. |
| **Session anchors** | RTH session high/low are candidate anchors under the same rules (no special casing beyond candidacy). |
| **Leg profile** | Volume-at-price and delta-at-price accumulated from ticks over the leg window, binned at **4 ticks (1.00 NQ/ES point)** per row. Sensitivity runs at 1 and 8 ticks. Adaptive binning (e.g. ATR-fraction rows) is a **parked later variant**, not part of v1. |
| **POC** | Highest-volume row of the leg profile. |
| **HVN** | Local-maximum rows with volume ≥ **70th percentile** of the leg's row volumes. |
| **LVN** | **Interior** local-minimum rows with volume ≤ **50%** of the mean of the two flanking peaks. The tapering end rows of a bell-shaped leg are excluded — they are not the "low volume nodes" the trader reads as gaps/unfinished business. |
| **Shelf** | Adjacent-row volume ratio ≥ **2.0** (a step edge, signed up/down). Candidate marker for absorption or an unfinished auction. |
| **Value area (leg)** | 70% of leg volume expanded outward from the leg POC (standard VA algorithm, volume-based). |
| **Building / tapering** | **Building** = trailing 3-bar volume rate ≥ 50% of the leg's average per-bar rate **and** a new leg extreme within the last 3 bars. **Tapering** = rate < 50% **and** no new extreme. Net delta alignment of recent bars is reported as a third descriptive field, not part of the label. Thresholds swept in Stage 1. |
| **Second touch** | (Used by IDEA-036.) First entry of price into a zone after the counter-leg begins = first touch; a return to the zone after leaving it (or after a minimum tick displacement) = second touch. Zone entry/exit hysteresis defined in IDEA-036. |

**Instrument:** primary **NQ** front month with explicit contract per window
(per data guide §4.0.3). MNQ/ES/MES only as separately labeled folds; no
silent pooling.

**Session scope:** **RTH only (09:30–16:15 ET)** for v1, matching the trader's
discretionary calibration. Globex is a later, separately labeled scope. Never
mix RTH and Globex in one baseline.

## Coverage matrix (verified 2026-07-24)

| Input / field | Status | Provenance | Granularity | Gap |
|---------------|--------|------------|-------------|-----|
| NQ `.scid` tick files | **Available** (external, authoritative) | Sierra data dir; replay per data guide | Tick (time, price, volume, bid/ask) | Clean 4-symbol forward record since **2026-06-23**; earlier backfills carry integrity caveats — quality gate required |
| Volume/delta-at-price accumulator | **Available** | `src/pipelines/footprint.rs` | Per price level | Accumulates over current window only; leg-window accumulation must be built |
| Swing/leg/pivot detector | **Missing** | — | — | The core deliverable of this spec; no pivot code exists in `src/pipelines/` |
| 1-min bar delta percentile | **Derivable offline** | `.scid` replay → 1-min bars → delta rank over trailing N | 1-min bars | No existing percentile-rank-of-delta pipeline; built inside the study binary |
| ATR(14,1m) | **Derivable offline** | `.scid` replay; consistent with IDEA-033 ATR machinery | 1-min bars | Do not fork vol definitions; reuse IDEA-033 conventions |
| Session delta profile / footprint windows | **Available** | `get_delta_profile`, `get_footprint`, `get_footprint_window` | Session / windowed | Validation reference for reconstructed leg profiles |
| Offline replay pattern | **Available as template** | `src/research/nine_am_continuation.rs`, `src/research/ib_campaign.rs` | Tick-exact replay | Copy this pattern into an isolated study binary; do not mutate `run_backtest` |

## Staged design

### Stage 1 — Leg engine + boundary stability study (authorized by this spec)

1. `get_research_summary` → record coverage by session_type/root before any claim.
2. Offline `.scid` replay (9AM-continuation pattern, isolated research DB):
   implement the leg state machine (tick-level extreme tracking, provisional
   trigger on 1-min delta percentile + displacement, confirmation on k×ATR
   retracement, retroactive profile accumulation from the extreme).
3. Quality gate: per-day gap scan (backfill 30-min logic); degraded days
   excluded and reported. Robustness re-run on the clean 2026-06-23+ subset.
4. Boundary stability output, per k × p × N parameter cell:
   - legs per RTH day (distribution), median leg duration/range/volume,
   - over-segmentation check: leg statistics in documented chop regimes vs
     trend regimes (legs/day should separate, not explode),
   - anchor agreement: price-only anchors vs delta-triggered anchors —
     coincidence rate, lead time of the provisional trigger,
   - profile sanity: leg POC within leg value area; sum of leg volumes ≈
     session volume (within tolerance, modulo unconfirmed tails).
5. Sensitivity table mandatory across k, p, N, bin size (1/4/8 ticks).

**Pass Stage 1:** a parameter cell exists where legs/day is stable across
folds and the clean subset, chop does not over-segment, and leg-volume
accounting closes — with N ≥ 30 RTH days per reported bucket (AGENT.md
sample-size policy). **Fail:** no cell is stable → **PARK**, report null, do
not proceed to tooling.

### Stage 2 — `get_leg_profile` MCP tool (gated; not authorized yet)

Only after a documented Stage 1 pass and trader confirmation:

1. Rust pipeline module (`src/pipelines/leg_profile.rs`) reusing the footprint
   accumulator over leg windows — incremental math, no per-tick rebuilds.
2. MCP surface exposing compact context: leg direction, anchor time/price,
   provisional/confirmed state, age, volume, net delta, POC/HVN/LVN/shelves,
   value area, building/tapering label, confluence with session profile levels.
3. Advisory context only until IDEA-036 (or successors) produce verdicts. Never
   a standalone trigger (per IDEA-029's safe-context matrix).

### Sierra translation track (parallel, trader-facing)

Per the locked decision: **same math, approximate visuals, no custom ACSIL.**

- Swing/ZigZag-style study with ATR-based reversal driving a Volume-by-Price
  reset (Sierra can reset a VbP study off another study's signal).
- The existing OFL Delta Rotation Calculator stays as the provisional-trigger
  visual; its percentile lookback maps to the spec's p/N parameters.
- Small divergences between the Sierra visual and the Rust engine are
  tolerated and documented; the Rust engine is the backtestable source of
  truth. Settings candidates live in tracked docs, never claimed as live
  chart state unless the trader supplies it.

## Falsifiable hypotheses

- **H1 (stable segmentation):** A (k, p, N) cell exists producing a stable
  legs-per-day distribution across RTH folds and the clean subset, without
  over-segmenting chop. *Falsify if* every cell either under-segments (1–2
  legs/day regardless of regime) or explodes in chop.
- **H2 (delta trigger adds information):** Delta-triggered anchors materially
  coincide with price-only anchors while leading them (provisional state is
  useful early warning). *Falsify if* the trigger mostly fires mid-leg (no
  anchor proximity) or adds no lead time over the retracement rule.
- **H3 (profile accounting closes):** Leg profiles partition session volume
  coherently (sum of leg volumes ≈ session volume; leg POC inside leg VA).
  *Falsify if* accounting breaks — the engine is mis-attributing volume and no
  downstream level read is trustworthy.

No hypothesis authorizes live signals, alerts, sizing, or playbook changes.

## Design controls

| Control | Requirement |
|---------|-------------|
| **Causality** | Backtest reads use only anchors/profiles whose trigger/confirmation precedes the decision timestamp; retroactive accumulation is a labeling convenience, never lookahead |
| **Instrument** | NQ; explicit contract per window; other roots labeled separately |
| **Sessions** | RTH only v1; Globex later and separately labeled |
| **Detection** | Multiple pre-registered parameter cells (k, p, N, bin); sensitivity table mandatory |
| **Leakage** | Trailing percentile windows and ATR use only prior bars; walk-forward folds |
| **Quality gate** | Per-day gap scan; degraded days excluded and reported; clean 2026-06-23+ subset robustness check |
| **Sample** | `get_research_summary` + coverage table before any claim; N ≥ 30 per bucket for reportable results |
| **Vendor claims** | OFL / Delta Dynamics material is `trust: hype` inspiration only; all definitions above are repo-native |

## Relationship to existing ideas

| Idea | Relation |
|------|----------|
| [IDEA-029](IDEA-029-sierra-execution-chart-study-context.md) | Track C predicted this work ("deterministic swing-leg boundary model plus a future `get_leg_profile`"); this spec is that model, now quantified. Its safe-context matrix still governs promotion. |
| [IDEA-020](IDEA-020-footprint-rebid-reoffer-lifecycle.md) | Rebid/reoffer zone lifecycle shares footprint primitives and the "second chance entries" behavior the trader describes at leg LVNs |
| [IDEA-004](IDEA-004-mtf-cvd-divergence.md) | Delta divergence context; leg delta profiles are a finer-grained sibling |
| [IDEA-033](IDEA-033-expected-range-atr-rv-iv-research-plan.md) | ATR conventions reused; do not fork volatility definitions |
| IDEA-036 | First tradeable setup built on this engine (pullback-join on stacked leg profiles) |

## Explicit non-goals (this pass)

- No backtest execution, no `register_hypothesis` / `run_backtest`.
- No production pipeline, MCP tool, chart study, alert, or live agent context.
- No custom ACSIL / C++ implementation; no live chart automation.
- No adaptive bin sizing (parked variant).
- No Globex scope in v1.
- No copy of vault inbox files, screenshots, `private/`, or account details into this file.

## Recommended next action

1. Human/agent runs the **Stage 1 coverage check** (gap scan + RTH day count)
   on an isolated research DB.
2. Build the offline leg-engine replay binary (9AM-continuation pattern) with
   the locked parameter grid.
3. Eyeball calibration: overlay reconstructed legs on a few recorded sessions
   against the trader's Sierra chart before trusting aggregate stats.
4. Stage 2 (MCP tool) and IDEA-036 backtests open only on a documented
   Stage 1 pass.

## See also

- Hub stub: [setup-ideas-and-backtesting.md#idea-035](../setup-ideas-and-backtesting.md#idea-035)
- Setup index: [index.md](index.md)
- Data workflow: [docs/data-and-backtesting-guide.md](../data-and-backtesting-guide.md)
- Sample-size policy: `AGENT.md` (Research Sample Size Policy)
