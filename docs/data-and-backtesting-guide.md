# The Desk — Data & Backtesting Guide

**If you are about to do anything with recorded market data or backtesting, read this
first.** It is the canonical, idea-agnostic reference for: where the data lives, what is
the source of truth, how recording works, and the exact workflow to backtest against it
with high confidence. It links out to the specialized docs rather than duplicating them.

Companion docs:
- [System Data Flow](architecture/data-flow.md) — the moving parts (Sierra ↔ MCP ↔ agents ↔ storage).
- [Setup Ideas & Backtesting tracker](setup-ideas/index.md) — the idea catalog + verdicts (one file per idea).
- [MCP tool reference](mcp/tool-reference.md) (generated catalog) and [tool routing](../skills/mcp-tools/SKILL.md).
- [Ops & Storage runbook](ops/automation-and-storage.md) — retention, archival, scheduled tasks.

---

## 1. Mental model (read this once and it all makes sense)

Three actors, one rule each:

| Actor | Role | Writes | Source of truth? |
| --- | --- | --- | --- |
| **Sierra Chart** | Recorder | `.scid` (ticks) + `.depth` (DOM) on `T:\SierraChart\Data` | **YES** — these files are authoritative |
| **`the-desk-mcp`** | Reader / ingester / brain | `data.db` (derived cache) | No — rebuildable from the files |
| **`the-desk-storage`** | Janitor | prunes/archives/compacts `data.db` | No |

**The single most important fact:** the `.scid`/`.depth` files are the authoritative source of
*market data* — recording depends only on Sierra running, and backtests replay the `.scid` files
directly, not the database.

But "the DB is just a cache" is only half true. Split `data.db` into three categories:

1. **Authoritative external source** — Sierra `.scid` / `.depth`. Raw market data; safe as long as Sierra records.
2. **Rebuildable derived market data** — `raw_ticks`, `depth_events`, `market_events`, `session_summaries`, `prior_day_levels`, snapshots, backtest `signal_outcomes`. Regenerable from the files via re-ingest / `backfill_history` / replay.
3. **Durable local state** — `risk_config`, `setups`, `research_hypotheses`, journal/trade records, memory, account/risk state. **NOT** regenerable from `.scid`/`.depth`; it survives *only* via the `[backup]` snapshots (on `X:`) and seeded reference DBs. Treat it like any database you must back up — a DB wipe loses it.

---

## 2. Where the data lives (data dictionary)

### A. The files (source of truth, on `T:`)

| Path | Contents | Notes |
| --- | --- | --- |
| `T:\SierraChart\Data\<SYMBOL>.scid` | Every trade tick (40-byte records: time, price, volume, bid/ask) | The raw feed. Requires **Intraday Data Storage Time Unit = 1 Tick** in Sierra (see [sierra-chart-settings.md](sierra-chart-settings.md)). |
| `T:\SierraChart\Data\MarketDepthData\<SYMBOL>.depth` | DOM order-book events | ~92 GB and growing; the durable source behind `depth_events`. |

Recorded symbols (since 2026-06-23): **NQ, MNQ, ES, MES** (see [multi-instrument-flow-architecture.md](multi-instrument-flow-architecture.md)).

### B. The database (`~/.the-desk/data.db` → junction to `T:\TheDesk\state\data.db`)

Full schema is in `src/db/mod.rs`. The tables you actually care about for backtesting/research:

| Table | What it holds | Populated by | Retention |
| --- | --- | --- | --- |
| `raw_ticks` | Ticks ingested from `.scid` | MCP live tail + `ingest_raw_ticks_from_scid` (**not** `backfill_history` — that replays pipelines without persisting raw ticks) | `warm_retention_days` (30); older archived to `X:\TheDesk\archive\*.csv.zst` |
| `depth_events` | DOM events ingested from `.depth` | MCP depth loop | `depth_retention_days` (7); `.depth` files are the durable copy |
| `market_events` | Structured events (level tests, extensions, day-type changes) detected during processing | EventDetector | retained |
| `session_summaries` | Per-RTH-session computed summary (delta, DNVA, day type, IB range) | RTH-close finalization | retained (small, valuable) — RVOL curves + research baselines |
| `prior_day_levels` / `_v22` | Prior-day H/L/C, VA, POC, DNVA | session finalization | retained |
| `signal_outcomes` | MFE / MAE / R-result tracked after a signal fires | live + **backtest** (tagged by `source` + `jobId`) | retained — the heart of backtest stats |
| `research_hypotheses` | Registered hypotheses | `register_hypothesis` | retained |
| `setups` | Playbook setup definitions | seeding / drafts | retained |
| `playbook_signals` | Fired alerts | rules engine | retained |
| `dom_snapshots`, `dom_feature_snapshots`, `pipeline_snapshots` | Reconstructed DOM ladders + periodic feature-state snapshots | live processing | larger; prune candidates |
| `historical_job_runs`, `validation_runs` | Backfill / backtest job + integrity tracking | jobs | retained |

> **Why this matters for confidence:** when a backtest reports stats, they come from
> `signal_outcomes` rows tagged with that run's `jobId` and `source="backtest"`. Reference
> tables (`session_summaries`, `session_volume_curves`, `prior_day_levels`, `setups`,
> `research_hypotheses`, `risk_config`) provide the historical *inputs* (RVOL curves, prior levels) the replay needs.

---

## 3. Recording & availability (the practical rules)

- **To record high-fidelity data: only Sierra must run.** The MCP/DB are not required for the
  `.scid`/`.depth` to accrue. (Automated by the Sierra lifecycle scheduled tasks — see the ops runbook.)
- **The MCP server** ingests the files into `data.db` and serves live tools. Start it (via Cursor
  or Claude Code) when you want live tooling or to keep the DB current. On startup it does a
  **warm replay** of ~2 Globex opens and then tails live; larger gaps are filled with
  `backfill_history` (events/sessions) or `ingest_raw_ticks_from_scid` (raw ticks).
- **The DB is available on weekends** for agent work — it is only briefly unavailable while a
  maintenance prune/compact runs (and those abort if the MCP is up). Plan heavy maintenance for
  a low-use window.
- **The single-writer rule:** `data.db` has one writer at a time (the MCP). Anything that writes
  to it — live ingestion, `the-desk-storage`, a backtest against the live DB — contends. **Run
  heavy backtests against an isolated copy** (next section).

---

## 4. The canonical backtest workflow

Backtests replay historical `.scid` through the **same** rules engine the live server uses, then
record `signal_outcomes` you query for stats. Run from any MCP client (a Cursor agent is typical).

**First, don't re-test a settled idea:** run `list_hypotheses` and skim the catalog in
[setup-ideas/index.md](setup-ideas/index.md) before registering anything new.

### 4.0 Before you start — three freshness/safety gates
1. **Rules-engine version:** `RULES_ENGINE_SCHEMA_VERSION` is currently **5**. If you changed any
   `ConditionField`/operator/evaluate semantics, bump it and **rebuild + restart the MCP** —
   otherwise a stale binary rejects new fields and cached backtest stats are invalid. (Cursor runs
   `target_alt\release\the-desk-mcp.exe`; build there.)
2. **Isolated DB:** build a tiny isolated DB so the run never contends with the live writer or
   bloats the live `data.db`:
   ```
   the-desk-mcp --seed-backtest-db --to <dest.db> [--from <live data.db>]
   ```
   It copies only the small **reference** tables (`session_summaries`, `session_volume_curves`,
   `prior_day_levels`, `prior_day_levels_v22`, `risk_config`, `setups`, `research_hypotheses`) —
   **not** `raw_ticks` (the replay reads `.scid`). Point the runner/backtest at `<dest.db>`.
3. **Contract:** pass the contract that was front during the window directly to `run_backtest`
   as `{ "contract": "NQH6.CME" }` (replays that contract's `.scid` without touching
   `active_symbol_override`, so live trading stays on the current front month). A coverage mismatch
   surfaces a `scid_window_mismatch_warning` + `integrity_status:"warning"` instead of a silent zero.

### 4.1 The loop (per hypothesis)
1. **Baseline integrity** — `validate_signal_outcome_integrity({ source: "backtest" })`.
2. **Dry-run feasibility** — `register_hypothesis({ ..., dryRun: true })`; check `feasibleForN30`,
   `projectedSampleSize`, `warnings`. Widen the window if the projected sample is too small.
3. **Register** — `register_hypothesis({ ..., dryRun: false })`. Keep `active: false`.
4. **Run** — `run_backtest({ startDate, endDate, setupIds:["<id>"], contract:"<front>", waitForCompletion: true })`; capture the `jobId`.
5. **Run integrity** — `validate_signal_outcome_integrity({ source:"backtest", jobId, setupId })`; proceed only if `status="ok"`.
6. **Read stats** — `query_signal_outcome_distribution` / `_conditional` / `_excursions` with
   `{ setupId, jobId, source:"backtest", includeUnverified:false }`. Use `summarize_hypothesis_run`
   for the headline (it flags `over_firing` / `chatty` if a state-flag setup re-fires).
7. **Compare** — for a gate/variant question, register both variants, backtest both, and
   `compare_backtests` (or compare distributions). Only promote if it beats the ungated baseline.
8. **Gate → Activate** — `propose_draft_setup({ setupId, jobId })` then, only after you review,
   `activate_draft_setup({ setupId, traderConfirmation:"<note>" })`. Activation is explicit.

A worked, idea-specific example (IDEA-000 / IDEA-012, with full JSON) lives in
[idea000-idea012-backtest-runbook.md](idea000-idea012-backtest-runbook.md). Use this guide for the
*method*; that doc for *concrete templates*.

### 4.2 Reading the results with confidence
- **N (sample size) first.** Small N = directional at most; follow `AGENT.md` "Research Sample Size Policy".
- **Excursions over win rate.** `query_signal_outcome_excursions` gives MFE/MAE distributions in
  points — these tell you whether stops/targets are placed where the move actually goes, independent
  of an arbitrary fixed target. (This is how we found a 3R target was measuring a rare tail.)
- **`includeUnverified:false`** so you only read outcomes that passed integrity.
- **Data quality caveat:** historical backfills can have gaps; check `integrity_status` and any
  `scid_window_mismatch_warning`. Cleaner forward-recorded data (the 4-contract feed since 2026-06-23)
  is the better judge as it accrues.

---

## 5. Research/query tools — what to use when

| Question | Tool |
| --- | --- |
| How often does event X occur per session? | `query_event_frequency` |
| When X is true, how often does Y follow? | `query_conditional` |
| Distribution of a metric (session delta, RVOL, MFE)? | `query_distribution`, `query_signal_outcome_distribution` |
| What was the MFE/MAE after a signal? | `query_signal_outcome_excursions` |
| Compare two backtests / setups | `compare_backtests`, `get_setup_performance_matrix` |
| Sample-size sanity before trusting stats | `get_research_summary` (call first) |
| DOM behavior frequency / at levels | `query_dom_behavior_frequency`, `query_dom_reaction_at_levels` |

Historical tools need the relevant data persisted first — and *which* ingest depends on the query:
- **Event / session research** (`query_event_frequency`, `query_conditional`, distributions): run `backfill_history` for the window.
- **Raw tick queries** (`query_ticks`): run `get_raw_tick_ingest_gaps`, then `ingest_raw_ticks_from_scid` — `backfill_history` does **not** persist raw ticks.
- **DOM research** (`query_dom_*`): depends on persisted `dom_feature_snapshots` coverage, not ordinary `.scid` backfill.

Always check `get_research_summary` first for the baseline sample size.

---

## 6. Gotchas / must-knows (hard-won)

- **Rebuild + restart after any rules change** (`target_alt`), or the live binary rejects new fields. Bump `RULES_ENGINE_SCHEMA_VERSION` so cached stats invalidate.
- **Never backtest against the live `data.db`** for heavy runs — isolate (§4.0). It both contends with the live writer and bloats the DB.
- **`depth_events` grows fastest.** It's the DOM table; bounded by `depth_retention_days`. The `.depth` files are the re-ingestable source. (This is what ballooned the DB to 629 GB; see the [ops & storage runbook](ops/automation-and-storage.md).)
- **Files are truth for *market data*; trader/control state is not rebuildable.** Market-data tables rebuild from `.scid`/`.depth` (re-ingest / backfill / replay), but `setups`, `research_hypotheses`, journal/trades, risk config, memory, and account state survive *only* via the `[backup]` snapshots (on `X:`). Back them up — a DB wipe is not harmless.
- **Schema is in `src/db/mod.rs`**; the generated tool catalog is `docs/mcp/tool-reference.md` (regenerate with `--write-tool-docs` after tool changes).

---

## 7. "Read these, in this order" (pointer map for an agent doing backtesting)

1. **This guide** — the model + workflow.
2. [`skills/trading-domain/SKILL.md`](../skills/trading-domain/SKILL.md) — TPO/delta/PTT meaning (never misuse the terms).
3. [`skills/mcp-tools/SKILL.md`](../skills/mcp-tools/SKILL.md) — which tool for which scenario.
4. [`docs/setup-ideas/index.md`](setup-ideas/index.md) — current ideas + verdicts (don't re-litigate settled ones).
5. [`docs/idea000-idea012-backtest-runbook.md`](idea000-idea012-backtest-runbook.md) — concrete registration/run JSON templates.
6. `AGENT.md` "Research Sample Size Policy" — how much to trust a result.
