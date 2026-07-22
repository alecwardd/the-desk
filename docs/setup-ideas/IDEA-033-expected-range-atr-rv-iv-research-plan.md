---
id: IDEA-033
title: Expected-range research plan — ATR vs realized vol vs implied vol (session sizing + runners)
status: Researched
regime: [any]
related: [IDEA-007, IDEA-027, IDEA-028, IDEA-031, IDEA-032]
companionSpecs:
  - docs/data-and-backtesting-guide.md
  - docs/setup-ideas-and-backtesting.md
  - docs/mcp/tool-reference.md
  - docs/phase-2-options-databento-memo.md
mcpPointers:
  - tool: get_research_summary
    note: Call first before any historical claim; do not cache N in this file
  - tool: query_distribution
    note: Session-summary metrics only (ib_range, high, low, rvol_ratio, …); no ATR/RV/IV columns yet
  - tool: get_session_history
    note: Per-session OHLC / IB / day-type rows for range derivation
  - tool: get_options_context
    note: Live ConvexValue volatility fields only; not a historical IV store
  - tool: query_signal_outcome_excursions
    note: MFE/MAE for runner-hold evaluation after a locked setup/jobId
  - tool: get_kelly_position_size
    note: Current R-based sizing baseline; not ATR/vol-scaled
hypothesisAnchor: false
---

# IDEA-033 — Expected-range research plan (ATR / RV / IV)

> Point-in-time **docs-only** research/backtest plan captured **2026-07-22**.
> Queue source: second-brain `queue/ready-for-agent.md` (expected-range ATR vs
> implied/realized vol for session sizing and runner decisions). This note does
> **not** run a backtest, change sizing, or invent missing IV history.

<!-- stats: point-in-time -->

## Origin

Trader research question: compare **ATR**, **realized volatility (RV)**, and any
**supported implied-volatility (IV)** measure (or an **explicitly labeled
proxy**) as inputs to (1) **session sizing** and (2) **runner hold/exit**
decisions, framed as an **expected range** problem.

This file is the decision-ready study design for a later offline backtest. It
verifies current repo coverage and refuses silent substitution when a field is
missing.

## Verdict framing (pre-backtest)

| Option | Meaning for this track | Pre-backtest stance |
|--------|------------------------|---------------------|
| **Adopt** | Promote ATR/RV/IV expected-range into live risk/agent sizing or runner rules | **Blocked** until Stage A–C clear sample-size and leak controls |
| **Adapt** | Keep as offline research; stage ATR/RV first; IV only when provenance exists | **Default** |
| **Skip** | Discard the comparison | **No** — complementary to IDEA-031 / IDEA-007 and actionable once coverage gates pass |

---

## Repo-native definitions (locked for this plan)

These definitions are research contracts. They are **not** live `MarketState`
fields unless noted.

| Term | Repo-native meaning for IDEA-033 |
|------|----------------------------------|
| **Session range** | For a completed `session_summaries` row: `high − low` (points). Not a first-class `query_distribution` metric today; derive offline from `high`/`low`. Scope by `session_type` (`RTH` / `Globex`) and never mix unlabeled. |
| **True range (TR)** | Classic \( \max(H_t-L_t,\ \|H_t-C_{t-1}\|,\ \|L_t-C_{t-1}\|) \) on **like-session** bars (RTH→RTH or explicit Globex→Globex). Prior close must come from the same scope and contract-continuity policy. |
| **ATR(n)** | Mean of the last *n* like-session true ranges, computed **offline** from summaries or `.scid`-derived OHLC. **No ATR pipeline, table column, or MCP metric exists in-repo** (verified 2026-07-22). Candidate defaults for Stage A: `n ∈ {5, 10, 14}` with sensitivity table. |
| **Realized volatility (RV)** | Session-scoped RV from returns inside the session window (candidate: 1-min or 5-min log returns of last trade, annualization policy pre-registered). Distinct from: (a) IDEA-007’s sketched same-time-of-day RV **ratio** (not implemented), (b) absorption `localVolatilityTicks` (60s high−low / tick size for adaptive zones only). |
| **Implied volatility (IV)** | Vendor-reported options IV only when the field is source-labeled and time-aligned. Live ConvexValue path exposes `impliedVolatility` (param `volatility`), `frontVolatility`, `backVolatility`, `volTermSpread` via `get_options_context`. **Not persisted historically in SQLite for research.** |
| **IV proxy (explicit)** | A non-IV series used *only* if labeled `PROXY`, never as silent IV. Candidates discussed in-repo: **VIX level/change** from `VIX_CGI.scid` (IDEA-028; not ingested by Desk pipelines) — proxy for *broad equity vol regime*, **not** NQ/NDX options IV. |
| **Expected range** | A **forecast** of upcoming session range (points) formed at a causal decision time \(t_0\) from ATR and/or RV and/or IV(/proxy). Primary target: next **RTH** session range. Secondary (separate folds): Globex segment ranges only if segment summaries exist with adequate \(N\). |
| **Session sizing** | Mapping expected range → **risk unit usage** (contracts or \(R\) fraction) **before** first discretionary entry of the session, holding stop distance and R definition fixed. Baseline is current Desk risk: dynamic \(R\) + `get_kelly_position_size` (signal-performance Kelly), **not** vol-scaled. Evaluation is **event-study / counterfactual sizing on historical outcomes**, not live risk-config mutation. |
| **Runner decision** | After a setup reaches a pre-registered first target (T1), whether to **exit remainder** vs **hold runner** toward T2+/session extremes, conditioned on expected-range residual (how much of forecast range is already consumed). Uses `signal_outcomes` MFE/MAE / time-to-outcome — **not** a live runner engine (templates list multi-target labels; no ATR/runner state machine in production). |

**Timezone / sessions (code truth):** Eastern Time. RTH `09:30–16:15`, Globex open `18:00`, London segment start `02:00` (`LONDON_OPEN_ET` in `src/lib.rs`). Skill prose that cites a different London clock is subordinate to these constants for this study.

**Instrument:** Primary **NQ** front month with explicit contract in backtest windows (`docs/data-and-backtesting-guide.md`). MNQ/ES/MES only if separately labeled; do not pool silently.

---

## Coverage matrix (verified 2026-07-22)

| Input / field | Status | Provenance | Granularity | History (documented) | Gap |
|---------------|--------|------------|-------------|----------------------|-----|
| NQ/MNQ/ES/MES `.scid` ticks | **Available** (external files) | Sierra on configured data dir; authoritative per data guide | Tick | Forward record set documented since **2026-06-23**; older backfills possible with integrity caveats | Must pass contract + `scid_window_mismatch` checks per window |
| `session_summaries` OHLC, `ib_range`, `or_*`, day type, RVOL ratio | **Available** (rebuildable) | Pipeline finalization / `backfill_history` | Per session_type × date | Hub March 2026 snapshot cited 222 summaries / 81 usable RTH (2025-11-28→2026-03-06) — **point-in-time; re-check with `get_research_summary` before any run** | No `session_range` column; derive `high−low` |
| `query_distribution` metrics | **Available** | `RESEARCH_DISTRIBUTION_METRICS` in `src/db/mod.rs` | Session summary | Same as summaries | Includes `high`/`low`/`ib_range`/`rvol_ratio`; **no** ATR, RV, IV, session_range |
| ATR(n) | **Missing as primitive** | — | Would be offline from TR series | — | Must derive; do not claim MCP ATR |
| Session RV / IDEA-007 RV-ratio | **Missing as pipeline** | Sketch only in hub IDEA-007 | Would be 1–5m returns | — | Offline derivation from ticks/bars required |
| `localVolatilityTicks` | **Available but wrong object** | Absorption pipeline | ~60s trade range in ticks | Live + event metadata | **Not** session RV; do not use as ATR/RV substitute without explicit label |
| ConvexValue IV / front/back vol | **Live only** | `src/options/mod.rs` → MCP options tools | Snapshot at fetch | No durable historical IV table found | Stage C blocked until logged series or vendor history |
| Databento / self-computed IV | **Not built** | ADR-013 / phase-2 memo | — | — | External Phase 2; out of scope for Stage A |
| VIX / SPX context `.scid` | **Machine-side files documented; not Desk-ingested** | IDEA-028 (listing-only confirmation 2026-07-09) | Tick files | Intraday history caveats in SPX/VIX memos | Usable only as **labeled PROXY** after ingest design; price-scale validation required |
| VolSignals / other IV dashboards | **Not subscribed / not integrated** | IDEA-026/027 vendor evals | — | — | No Desk history; do not fabricate |
| Kelly / R sizing | **Available** | `get_kelly_position_size`, risk config, Lucid R framing in `AGENT.md` | Live / journal | Outcome-dependent | Baseline for sizing study; not vol-aware |
| Runner engine | **Missing** | Multi-target labels on templates; archive coaching copy | — | Outcomes MFE/MAE exist when setups backtested | Runner study = offline policy on outcomes, not live tool |
| IDEA-031 compression/expansion | **Related queue** | Docs only | Session transitions | Shares range/RV vocabulary | Complementary; do not duplicate as sizing/runner verdict |

**Hard rule:** If IV history is absent for a date, that date is **IV-missing**, not “IV = VIX” or “IV = ATR”. Proxies require the `PROXY:` prefix in every table, chart, and hypothesis ID.

---

## Two evaluation tracks (do not merge metrics)

### Track S — Session sizing

**Decision time \(t_0\):** Immediately before RTH open (or at a locked pre-open timestamp), using only information available at \(t_0\).

**Action space (research counterfactual):** Map expected-range bucket → size multiplier \(m \in \{0.5, 1.0, 1.5\}\) applied to a **fixed** baseline size (1R risk or fixed contract count). Stop distance held constant in points (or fixed structural stop rule).

**Primary metrics (pre-register):**

1. Distribution of session \(R\) / P&L under each multiplier policy vs baseline \(m=1\) (journal or backtest outcomes scoped to the same sessions).
2. Hit rate of daily loss / heat gates (circuit-breaker trips) under each policy.
3. Calibration: predicted range vs realized `high−low` (MAE, bias, coverage of 50%/80% prediction intervals).

**Out of scope for Track S:** Changing Lucid daily loss limits, live `risk_config`, or account-specific contract caps.

### Track R — Runner decisions

**Decision time \(t_1\):** First touch of T1 (or first `outcome` progress milestone defined per setup template), using residual expected range \( \hat{R}_{session} - R_{consumed} \).

**Action space:** Binary hold-runner vs flatten-at-T1.

**Primary metrics:**

1. Incremental MFE after \(t_1\) when residual expected range is high vs low (bucketed).
2. MAE after \(t_1\) (giveback) in the same buckets.
3. Net \(R\) improvement of residual-conditioned hold vs always-exit-T1 and always-hold-to-T2.

**Out of scope for Track R:** New live alerts, playbook activation, or “you should hold” coaching claims.

Tracks share feature engineering (ATR/RV/IV→expected range) but **report separate tables, separate \(N\), and separate adopt/adapt/skip calls**.

---

## Falsifiable hypotheses

Follow `AGENT.md` Research Sample Size Policy: `N < 20` insufficient; `20 <= N < 30` directional; `N >= 30` reportable **per compared bucket**. Prefer `N >= 30` for sizing implications.

### Shared forecast layer

- **H0 (calibration):** Offline ATR(n) and/or RV predictors, fit only on past folds, produce RTH range forecasts whose absolute error is **no worse** than a naive baseline (trailing like-session median range) on a locked holdout with \(N\ge 30\) RTH sessions.
  - *Falsify if* ATR/RV MAE ≥ naive MAE (and intervals are miscalibrated) after sensitivity on \(n\) and bar size.

- **H-IV (conditional):** When a **provenance-complete** IV (or labeled PROXY) series exists at \(t_0\), adding it improves range calibration vs ATR/RV-only on the overlapping dates with \(N\ge 30\).
  - *Falsify if* IV/PROXY adds no skill, or if missingness bias explains the lift.
  - **Until IV history exists, H-IV is parked — not tested by substituting ATR.**

### Track S

- **HS1:** Size multipliers from expected-range **percentiles** (low→\(m=0.5\), mid→\(1.0\), high→\(1.5\)) improve risk-adjusted session outcomes vs fixed \(m=1\) without increasing daily-limit breach rate, \(N\ge 30\) sessions per bucket.
  - *Falsify if* expectancy/drawdown metrics do not beat baseline or breaches rise.

- **HS2:** ATR-based expected range and RV-based expected range disagree on ≥30% of sessions; when they disagree, the **intersection** (both low / both high) is more informative than either alone.
  - *Falsify if* disagreement is noise (no lift for intersection buckets).

### Track R

- **HR1:** Conditional on T1 being reached, **high residual** expected range predicts higher post-T1 MFE than **low residual**, \(N\ge 30\) per residual bucket for at least one locked setup family (start: `tpl_or5_mid_retest` / short mirror **or** another setup with verified outcome coverage).
  - *Falsify if* residual buckets show no MFE separation after costs/slippage policy is stated.

- **HR2:** A residual-gated hold policy beats always-exit-T1 and always-hold on net \(R\) for the same setup/`jobId` universe.
  - *Falsify if* neither gated policy wins on the pre-registered primary metric.

No hypothesis authorizes live sizing or runner autopilot.

---

## Design controls

| Control | Requirement |
|---------|-------------|
| **Instrument** | NQ; contract passed explicitly per window |
| **Sessions** | Primary: RTH. Globex/Asia/London only as separately labeled folds (align with IDEA-031) |
| **Horizons** | Forecast: next full RTH range. Runner residual: from \(t_1\) to session end or T2/time-stop |
| **Baselines** | (1) Trailing median like-session range; (2) IB-range scaled heuristic; (3) fixed size \(m=1\); (4) always-exit-T1 / always-hold |
| **Leakage** | Features at \(t_0\)/\(t_1\) use only past sessions + information ≤ decision time; no same-session future OHLC; no full-sample IV that was unavailable live |
| **Rollover / holidays / gaps** | Exclude or flag via `carry_forward_levels_valid` / rollover warnings; document partial sessions |
| **Costs** | State event-study vs fill-simulation; Desk research reports market movement vs levels — do not invent fill models unless separately approved |
| **Sample** | `get_research_summary` + dry-run `feasibleForN30` before register; walk-forward folds with per-bucket \(N\) |
| **IV missingness** | Separate analysis cohort; never impute IV from ATR/RV without `PROXY` label and a dedicated hypothesis ID |

---

## Smallest staged backtest (specify only — do not execute)

### Stage A — Coverage + ATR/RV calibration (minimal)

1. `get_research_summary` → record session counts by `session_type` / root (do not paste live account or private paths into docs).
2. Export or query RTH `session_summaries` (`high`,`low`,`close`,`ib_range`,`session_date`,`contract_symbol`).
3. Offline: build TR/ATR(5/10/14) and one RV estimator; produce next-session range predictions vs naive median.
4. Report: MAE/bias, interval coverage, \(N\), fold IDs, commit hash — **no sizing yet**.

**Pass Stage A:** ATR or RV beats naive on holdout with \(N\ge 30\) **or** documented adapt with clear failure modes.
**Fail:** Cannot assemble \(N\ge 30\) clean RTH rows → **PARK** sizing/runner stages.

### Stage B — Track S sizing counterfactual

1. Lock Stage A winner (or ensemble rule).
2. Apply \(m\in\{0.5,1.0,1.5\}\) to a fixed historical outcome set (journal trades **or** one verified backtest `jobId` — never mix without labeling).
3. Compare breach rate + expectancy; sample-size gate per bucket.

**Adopt (research-only):** Material improvement without breach increase.
**Adapt:** Weak/unstable → keep as context label only.
**Skip sizing:** No lift after sensitivity.

### Stage C — Track R runner residual

1. Requires Stage A forecast + a setup with verified `signal_outcomes` (`includeUnverified:false`).
2. At T1, compute residual expected range; bucket high/low; compare post-T1 MFE/MAE/net \(R\).

**Adopt/Adapt/Skip** independently of Stage B.

### Stage D — IV / PROXY (optional, gated)

Only if a dated provenance plan lands (ConvexValue historical log, Databento, or IDEA-028 VIX ingest with `PROXY:` labels). Re-run H-IV on overlap dates. **Do not block Stages A–C on IV.**

### Required outputs (when a future agent executes)

- Coverage table (actual \(N\), date span, contracts, missing days).
- Prediction skill table (ATR vs RV vs naive; IV if present).
- Track S results table (separate).
- Track R results table (separate).
- Explicit IV missingness report.
- Adopt / Adapt / Skip per track with sample-size labels.
- No promotion to live risk or playbook without trader confirmation.

---

## Relationship to existing ideas

| Idea | Relation |
|------|----------|
| [IDEA-031](IDEA-031-session-range-compression-expansion.md) | Range/RV **state transitions**; this plan uses expected range for **sizing/runners** |
| Hub IDEA-007 | RV-ratio microstructure sketch — candidate RV feature, not implemented |
| [IDEA-028](IDEA-028-spx-vix-rth-context-feed.md) | VIX as possible **PROXY** context only |
| [IDEA-027](IDEA-027-options-data-vendor-comparison.md) / [IDEA-026](IDEA-026-volsignals-vs3d-vendor-eval.md) | IV vendor landscape; not historical Desk IV |
| [IDEA-032](IDEA-032-hmm-lecture-notes-repo-fit.md) | Latent-regime research; orthogonal; do not couple |

---

## Explicit non-goals (this pass)

- No backtest execution, no `register_hypothesis` / `run_backtest`.
- No production ATR/RV/IV pipeline, MCP tool, chart study, or sizing tool.
- No live risk-limit, Lucid, or account-specific sizing change.
- No signal integration, playbook activation, or strategy verdict.
- No subscription, signup, or external research purchase.
- No Phase 1 multi-contract / storage / MCP code changes.
- No copy of `private/`, broker screenshots, live positions, or raw DB dumps into this file.

## Recommended next action

1. Human/agent runs **Stage A coverage check** via MCP (`get_research_summary` + RTH summary pull) on an isolated research DB.
2. Only then authorize offline ATR/RV calibration notebooks/jobs.
3. Keep IV on Stage D until a provenance-complete series exists.

## See also

- Hub stub: [setup-ideas-and-backtesting.md#idea-033](../setup-ideas-and-backtesting.md#idea-033)
- Setup index: [index.md](index.md)
- Data workflow: [docs/data-and-backtesting-guide.md](../data-and-backtesting-guide.md)
- Sample-size policy: `AGENT.md` (Research Sample Size Policy)
- Options Phase 2: [docs/phase-2-options-databento-memo.md](../phase-2-options-databento-memo.md)
