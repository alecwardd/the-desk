# The Desk — MCP Tool Reference

> **Generated file — do not edit by hand.**
> Regenerate with `cargo run --bin the-desk-mcp -- --write-tool-docs`.
> The test `tool_reference_doc_is_current` fails when this file is stale.

The Desk MCP server exposes **121 MCP tools** across **9 domains**. Each domain maps to a module under `src/bin/the-desk-mcp/tools/`. For scenario-based routing ("which tool do I call when…"), read `skills/mcp-tools/SKILL.md` first; this file is the exhaustive catalog.

| Domain | Tools | Reach for it when… |
|---|---|---|
| [Market](#market) | 24 | you need current market state or session-relative framing during live conversation |
| [DOM](#dom) | 10 | the trader asks how the order book behaved at a price or wants liquidity context |
| [Options](#options) | 3 | gamma exposure or options-derived levels are relevant to the trade discussion |
| [Playbook](#playbook) | 16 | you are tracking setups, triaging what deserves attention, or moving a trade idea through its lifecycle |
| [Risk](#risk) | 9 | sizing a trade, checking limits, or starting/ending a trading session |
| [Journal](#journal) | 12 | recording or reviewing actual trades and journaling the session |
| [Memory](#memory) | 12 | you want durable context about the trader, or to persist an insight worth remembering |
| [Research](#research) | 23 | answering "how often" / "what happens after" questions or running and comparing backtests |
| [Admin](#admin) | 12 | diagnosing data problems, backfilling history, or verifying feed and database health |

## Market

Live market structure reads: snapshot, TPO, delta, key levels, tape pace, footprint, and per-pipeline state.

Module: `src/bin/the-desk-mcp/tools/market.rs`

### `check_delta_confirmation`

Check delta confirmation at session level and at a specific price level. Returns whether session delta and price-level delta both support the trade direction. Use before trade entry for Stowe's 'execution requires delta confirmation'.

### `get_absorption_events`

Recent absorption-flow lifecycle events (absorption, exhaustion, delta divergence). Each event includes subtype, candidate/confirmed/invalidated status, zone bounds, direction, regime metadata, and severity.

### `get_context_frame`

Context frame for agent interpretation. Call this when you need session-relative framing, stable buckets, historical analogs, forward-path caveats, or setup-linked outcome context; use get_market_snapshot for raw values, get_session_context for session identity, compare_sessions for explicit analog-only research, and get_attention_inbox for what deserves attention now.

### `get_day_type`

Day type classification (Dalton): NonTrend, Normal, NormalVariation, Neutral/NeutralCenter/NeutralExtreme, Trend, or DoubleDistributionTrend. Profile shape: Gaussian, PShape, BShape, DShape, or Elongated. Balance state: Balanced vs Imbalanced. Single prints direction relative to POC.

### `get_delta_at_price`

Delta at a specific price level from the delta profile. Returns signed delta at that price, buy/sell confirmation, and the top N prices by absolute delta magnitude (where conviction is concentrated). Omit price to use current price.

### `get_delta_profile`

Delta profile: segment delta (Asia-only, London-only, or RTH-only), combined Globex delta (Asia+London when in Globex), cumulative delta, DNVA high/low, DNP. Use for inventory and positioning analysis.

### `get_footprint`

Footprint / volume-at-price data for the current session: top price levels by total volume with bid volume, ask volume, delta, and delta-per-volume ratio. Use price_low/price_high to focus on a specific price zone (e.g. near a key level). For a time-windowed footprint showing what happened at a specific time, use get_footprint_window instead.

### `get_footprint_window`

Time-windowed footprint: bid/ask volume at each price level traded between start_time_ms and end_time_ms. Ideal for reconstructing what happened at a specific price during a specific time window — e.g. 'show me the footprint at the overnight low between 20:00 and 20:10'. Results are sorted by price ascending. Use get_market_snapshot to find current timestamp_ms, then subtract milliseconds to target earlier windows. Optionally narrow the price range with price_low/price_high.

### `get_imbalances`

Stacked and diagonal imbalance detection from the footprint. Stacked: 3+ consecutive levels where one side dominates (>2:1 ratio) -- shows directional conviction. Diagonal: aggressive lifting/hitting across adjacent levels -- shows urgency. Returns prices and direction for each type.

### `get_key_levels`

Key reference levels: prior day high/low/close, prior session value area high/low and POC, overnight (Globex) high/low, Globex OR30 and London OR60, and initial balance high/low. Includes sessionType, tradingDay, and contract rollover status so agents can gate carry-forward references.

### `get_market_snapshot`

Current market snapshot: last price, VWAP with 1/2/3 SD bands, TPO value area (high/low/POC), delta neutral value area (DNVA high/low/DNP), session delta, cumulative delta, key levels (prior day H/L/C, prior VA/POC, overnight range, OR, IB), Globex/London opening ranges, and session context (sessionType, sessionSegment, tradingDay), plus tape pace, imbalance count, absorption event count, and average trade size. Prefers live pipeline state; falls back to last persisted snapshot.

### `get_or5_status`

5-minute Opening Range (Leo's A+ setup): OR5 high, low, midpoint (key level), break direction (None/Up/Down), whether mid has been retested after breakout, and extension targets (75%% and 100%% of range from mid).

### `get_pinch_events`

Recent delta momentum reversal ('pinch') events: when heavy one-sided delta is suddenly met by fast opposing flow, causing inventory to shift. Each event has timeframe (1m/5m/15m/30m), severity score (0-5), pre/post delta, price at pinch, and price displacement.

### `get_proximity_report`

Which key levels is price currently near (within specified tick distance). Returns levels sorted by distance ascending. Includes prior day H/L/C, VA/POC, overnight (Globex), Globex OR30, London OR60, IB, OR5 mid, and IB extensions. Response includes sessionType/sessionSegment/tradingDay.

### `get_rebid_reoffer_zones`

Active rebid/reoffer acceleration zones: price ranges of one-sided aggressive activity. Each zone has type (Buy/Sell), status (Fresh/Retested/Held/Failed), price range, volume, and delta. Key concept: 'never fade a held zone.'

### `get_rvol`

Relative Volume: ratio of current session's cumulative volume vs the N-day average at the same time-of-day. Returns classification (Low/Normal/Elevated/High), percentile rank (0-100 vs history at same time), velocity (rate of change per 5-min bucket), acceleration (second derivative), bucket progress, actual vs expected volume, and lookback days. Use to calibrate participation quality and regime context.

### `get_session_context`

Current session context: sessionType (RTH/Globex/Unknown), sessionSegment (Asia/London/None), tradingDay (6 PM ET roll), data freshness, and contract rollover status.

### `get_session_inventory`

Cross-session delta inventory: whether current session is Building (extending prior direction), Clearing (opposing prior direction), or Neutral. Direction: Long/Short/Flat. Includes consecutive sessions with same-direction delta (trend count) and DNP shift (how much the delta neutral pivot has migrated from prior session).

### `get_session_summary`

Session summary: total tick count, latest tick timestamp, and latest pipeline snapshot. Provides a quick health check of data flow.

### `get_snapshot_at`

Historical pipeline snapshot nearest to a given timestamp. Pipeline state (VWAP, POC, VA, delta, day type, etc.) is stored every ~30 seconds. Use this to answer 'what was the market structure at 20:00?' — pass that time as epoch milliseconds. The response includes the actual snapshot timestamp so you can see how close the match is. Use get_market_snapshot to get the current timestamp_ms and work backward.

### `get_tape_pace`

Tape pace analytics with coverage-aware rolling ticks/sec and volume/sec over 5-second, 30-second, and 5-minute windows. Returns both session-relative and rolling-context pace percentiles, smoothed normalized acceleration plus raw acceleration, 30-minute regime baselines, window validity/coverage, dwell at current price, and explicit data quality metadata so agents can distinguish live vs stale tape context.

### `get_tpo_detail`

Per-price TPO letter detail for the current session: shows which 30-minute brackets (A, B, C, …) printed at each price level. Bracket A = first 30 min (Opening Range), B = 30-60 min (completes IB), C onwards = regular session. Single-print levels (is_single_print: true) are tail/excess candidates. Use price_low/price_high to focus on a specific price zone.

### `get_tpo_profile`

TPO (Time-Price-Opportunity) profile data: POC (point of control), value area high/low, opening range high/low (first 30 min), initial balance high/low (first 60 min). Use for auction market theory analysis.

### `get_trade_size_profile`

Trade size distribution: counts of 1-lot, 2-5 lot, 6-20 lot, and 21+ lot trades for the current session. Includes average trade size and prices where institutional (21+) lot trades clustered. Use for identifying institutional participation and footprint locations.

## DOM

Depth-of-market analysis: DOM snapshots, pull/stack activity, liquidity behavior at levels, and book-reaction explanations.

Module: `src/bin/the-desk-mcp/tools/dom.rs`

### `explain_book_reaction`

Explanation-oriented delayed DOM read around a timestamp or level. Grounds the interpretation in persisted DOM summaries, nearby depth events, and executed tape. DOM data has ~1s polling lag from Sierra.

### `get_dom_regime_summary`

Summarize delayed DOM behavior over a window so agents can tell whether liquidity has been persistent, flashing, or flipping. Returns time-in-state, flip counts, persistence, confidence, and a narrative summary.

### `get_dom_snapshot_at`

Delayed DOM snapshot reconstructed from Sierra `.depth` history at or immediately before a timestamp. Returns best bid/ask, spread, touch imbalance, and the top resting levels on each side. Use this when you want the ladder view, not just executed tape. Note: Sierra depth data has ~1 second polling lag, so this is a delayed reconstruction, not real-time.

### `get_dom_tape_context_at`

One-call delayed DOM + tape context at a timestamp. Combines the nearest DOM snapshot, the nearest persisted DOM feature summary, raw-tick footprint over a short window, and derived flow flags. DOM data has ~1s polling lag from Sierra.

### `get_dom_window`

Windowed delayed DOM summary using persisted DOM feature snapshots when available. Returns compact DOM summaries across a time range and optionally narrows the reported pull/stack levels to a price band. DOM data has ~1s polling lag from Sierra.

### `get_liquidity_behavior_at_level`

Liquidity behavior around a target price over a time window. This focuses pull/stack analysis on a narrow band around a level, such as prior VAH, IB high, or an anchored VWAP level.

### `get_pull_stack_activity`

Estimate pull/stack activity from Sierra `.depth` history over a time window, then align DOM decreases with `.scid` trades to separate likely fills from likely pulls. Use price_low/price_high to focus on a specific zone.

### `query_dom_behavior_conditional`

Historical setup outcome context when a DOM behavior was present near signal fire. Answers questions like whether persistent bid support improved setup follow-through, with research metadata for outcome sample reliability.

### `query_dom_behavior_frequency`

Historical frequency of DOM behaviors such as persisted bid support, ask resistance, liquidity flips, pulling acceleration, or stacking acceleration. Uses persisted DOM feature snapshots and returns research metadata including sample reliability and truncation status.

### `query_dom_reaction_at_levels`

Historical DOM behavior around a specific event type or level interaction. Helps answer whether persisted support, flips, or pulling acceleration commonly accompanied a class of market events. Returns research metadata and marks capped market-event scans as non-reportable.

## Options

Options integration: gamma levels and dealer-positioning context for the current session.

Module: `src/bin/the-desk-mcp/tools/options.rs`

### `get_gamma_levels`

Top SPX/options gamma concentration strikes from ConvexValue, with call/put breakdown, open interest, OI change, volume bias, vomma, recent 5m volume, avg spread, expiration coverage, and cache metadata. Use for pre-session context like 'where are the likely gamma walls?' or 'where is new positioning opening today?'

### `get_options_context`

Aggregate ConvexValue options regime context: underlying price/change, aggregate gxoi/dxoi, call/put gxoi/dxoi splits, put-call ratio, flow decomposition (flowratio, call/put value/volume bias), vol surface (front/back IV, term spread), premium flow (value bought/sold), vanna/charm regime, and cache metadata. Use when an agent needs broad options positioning context rather than per-strike detail.

### `refresh_options_snapshot`

Force-refresh the cached ConvexValue snapshot used by get_gamma_levels and get_options_context, then return the fresh options context plus a gamma-level preview.

## Playbook

Playbook evaluation, setup lifecycle state, attention signals, and trade idea cards.

Module: `src/bin/the-desk-mcp/tools/playbook.rs`

### `acknowledge_attention_signal`

Acknowledge an attention signal as reviewed by the trader or an agent. Use acknowledgedBy='trader' or 'agent:<name>'.

### `acknowledge_setup_prompt`

Mark a setup's discretionary prompt as confirmed and persist the lifecycle transition.

### `close_setup_state`

Close a setup lifecycle state and persist the transition.

### `evaluate_playbook`

Evaluate all active playbook setups against current market state. Returns per-setup status (conditionsMet, approaching, notActive) and recent signal count. Always frames results as 'your playbook says...' -- never advisory.

### `get_active_trade_ideas`

Current trade idea cards derived from playbook setup lifecycle and attention signals. These are idea-state overlays, not execution instructions.

### `get_attention_changelog`

Replay attention signal lifecycle deltas such as created, priority_changed, acknowledged, expired, invalidated, or notified. Use for agent catch-up and audit trails.

### `get_attention_inbox`

Ranked proactive attention inbox. Call this first when asking what deserves attention now; returns durable playbook-grounded signals, never raw ticks.

### `get_setup_context`

Full setup context for a named setup. Returns all computed data relevant to that setup type: OR5 levels, delta confirmation, RVOL, day type, nearby zones, risk state. One call = everything needed to discuss a potential trade.

### `get_setup_state_history`

Return recent durable setup state/progress transitions for a setup or session. Use to answer what changed before/after a restart.

### `get_signal_detail`

Full detail for one attention signal: evidence links, setup/risk context references, priority breakdown, and suggested next MCP tools for agent routing.

### `mark_setup_in_trade`

Mark a setup as in-trade and persist the lifecycle transition.

### `mark_trade_idea_confirmed`

Mark a trade idea as confirmed with evidence. Enforces typed lifecycle instead of a free-form state setter.

### `mark_trade_idea_in_trade`

Mark a trade idea as in-trade, optionally linking a signal outcome ID.

### `mark_trade_idea_invalidated`

Mark a trade idea as invalidated with a reason code and optional note.

### `mark_trade_idea_resolved`

Mark a trade idea as resolved with an outcome and optional note.

### `what_changed_since`

Cursor-based catch-up feed for what changed since a prior attention cursor. Use when the trader asks what changed while away.

## Risk

Risk and account state: limits, position sizing, risk config, and trading session open/close.

Module: `src/bin/the-desk-mcp/tools/risk.rs`

### `end_trading_session`

End a trading session in the local journal store. Optionally saves a freeform session note as a journal entry linked to the session.

### `get_account_state`

Account state for risk coach: last known balance, open positions not from chat, Lucid params (daily loss, account size), profit goals. Call at session start to report last balance and ask for confirmation.

### `get_kelly_position_size`

1/4 Kelly position sizing with optional confidence scaling. Returns suggested R to risk, fractional Kelly, and raw f*. Uses get_signal_performance for win rate and avg winner/loser R. Confidence: 0.5=low (1/8 Kelly), 1.0=normal (1/4 Kelly), 1.5=high (up to 1/2 Kelly).

### `get_risk_config`

Trader's risk configuration: R-value in points and dollars, max daily loss in R-units and dollars, max consecutive losses, max trades per session, no-trade zones.

### `get_risk_state`

Current risk state: daily P&L in R-units, trade count, consecutive losses/wins, drawdown, and whether the daily loss limit has been reached. Uses the trader's configured R framework.

### `init_risk_state`

Initialize or reset risk state for a new session. Creates the initial risk state row (0 P&L, 0 trades, no streaks) so get_risk_state returns valid data. Call at session start to enable full risk tracking. Uses max_daily_loss_r from risk_config.

### `save_account_state`

Save account state: balance, open positions, Lucid params. Call after trader confirms. Partial updates: only provided fields are updated.

### `save_risk_config`

Save risk configuration. Partial updates: only provided fields are updated. Call to persist R-value, max daily loss, circuit breaker, and trade limits. Required for full risk tracking when config is not yet in database.

### `start_trading_session`

Start a trading session in the local journal store. Creates a session row that trades and journal entries can attach to. Use this at the beginning of a discretionary review or live session when you want Cursor agents to log journal context consistently.

## Journal

Trade entries, fill imports, journal notes, session reviews, and journal pattern queries.

Module: `src/bin/the-desk-mcp/tools/journal.rs`

### `close_trade_entry`

Close a trade journal entry with exit details. Optionally updates risk state when result_r is supplied and update_risk_state is true.

### `get_recent_journal_notes`

Get a compact slice of recent journal notes. Supports filtering by tag, setup reference, or trade reference.

### `get_session_journal`

Return journal notes for a session. If session_id is omitted, uses the latest open session when available.

### `get_session_review_context`

Return a structured session review bundle: session metadata, trade journal entries, journal notes, and deterministic summary metrics for debrief workflows.

### `get_trade_entry`

Get a single trade journal entry by ID.

### `import_trade_fills`

Import broker-exported fills into the trade journal. Accepts an array of fill rows, skips duplicates idempotently, stores raw import rows, and synthesizes normalized round-trip trade entries.

### `list_trade_entries`

List trade journal entries. Without filters, returns the most recent trade entries across sessions.

### `query_journal_patterns`

Aggregate deterministic journal patterns across sessions: planned-vs-unplanned counts, rules adherence, emotional states, review tags, mistake tags, and gross points.

### `record_trade_result`

Record a completed trade result. Updates risk state (daily P&L, consecutive wins/losses, drawdown, at_limit). Also creates a trade record for performance tracking. Call after a trade is closed to keep risk state current.

### `review_trade_entry`

Update structured trade review fields including thesis, review tags, mistake tags, discipline flags, and notes.

### `save_journal_entry`

Save a journal note. If session_id is omitted, the latest open session is used when available.

### `upsert_trade_entry`

Create or update a trade journal entry. Supports manual chat-first trade logging as well as imported-fill normalization. If session_id is omitted, the latest open session is used when available.

## Memory

Trader memory: agent insights, behavioral patterns, follow-ups, briefings, and trader-context fit.

Module: `src/bin/the-desk-mcp/tools/memory.rs`

### `acknowledge_agent_insight`

Acknowledge an insight after surfacing it. Supported actions: surfaced, helpful, irrelevant, wrong, pin.

### `create_memory_followup`

Create an open follow-up item for later session review or confirmation.

### `detect_behavioral_patterns`

Run deterministic behavioral memory detection over stored sessions, trades, and reviews, then upsert active behavioral patterns.

### `get_behavioral_patterns`

Get active behavioral patterns with optional scope filters and minimum sample size.

### `get_memory_brief`

Return a ranked memory brief for session_start, setup_check, trade_review, or weekly_review. Includes recent sessions, matching patterns, matching insights, and open follow-ups.

### `get_pre_session_briefing`

Build a session-start packet that merges ranked memory, current account/risk context, and contract rollover status. When persisted memory maintenance is dirty (`memoryMaintenance.refreshSuggested`), runs a single bounded `refresh_memory_state` unless `skipMemoryRefreshIfDirty` is true.

### `get_trader_context_fit`

Typed trader memory context fit. Separates executed-trade memory, setup opportunity context, coaching reminders, live post-loss/ordinal state, reliability, and provenance. Memory reports context only and must not drive sizing by itself.

### `recall_agent_insights`

Recall stored agent insights with filters for category, setup, status, and context scope.

### `refresh_memory_state`

Explicitly refresh memory maintenance state without coupling recomputation to read requests. Can refresh behavioral patterns, insight lifecycle status, or both.

### `resolve_memory_followup`

Resolve an open memory follow-up, optionally attaching a resolution note.

### `save_agent_insight`

Save an agent-authored memory insight. New insights start as candidate unless they are reinforced by a matching prior insight or explicitly backed by patternIds in evidence.

### `supersede_agent_insight`

Supersede an older insight with a newer replacement insight ID.

## Research

Historical research: hypotheses, backtests, and frequency/conditional/distribution queries over recorded sessions.

Module: `src/bin/the-desk-mcp/tools/research.rs`

### `activate_draft_setup`

Activate an inactive draft setup after human confirmation. Re-checks cached engine-version freshness before setting active=true.

### `cancel_backfill`

Cancel an in-flight historical backfill or backtest job.

### `compare_backtests`

Compare two or more backtest runs side-by-side. Pass run IDs to compare params, metrics, and signal performance across parameter variations.

### `compare_sessions`

Compare current session structure against similar historical sessions. Uses multi-dimensional similarity: IB range, day type, profile shape, balance state, RVOL ratio, session delta sign, single prints direction. Returns the most similar past sessions, outcomes, and research metadata including rows considered, result cap, and truncation status.

### `get_backfill_status`

Poll progress for a queued/running historical backfill or backtest job.

### `get_backtest_results`

Retrieve stored backtest runs. Returns most recent runs with params, metrics, and signal performance. Use to analyze historical backtest results.

### `get_research_summary`

Research summary: pre-session statistical briefing. Returns session count in database, IB range distribution, recent day types, and key frequencies. One call = baseline context for the trading day.

### `get_session_history`

Query past session summaries with optional filters. Returns structured session data (OHLC, IB range, day type, delta, close vs levels, POC, VA, DNVA per session) for historical analysis and multi-session value migration.

### `get_setup_performance_matrix`

Per-setup performance matrix in one call. Returns aggregated setup metrics: total/resolved/pending counts, target/stop/time-exit breakdown, win rate, avg R, avg winner/loser R. Supports date + session scope filters, minimum resolved threshold, sorting, and limit.

### `get_signal_performance`

Signal/setup performance statistics. Returns win rate, average R, total signals, resolved/pending counts, target hit vs stop hit vs time-exit counts. Filter by setup_id to see performance of a specific setup. Optional source filter: live|backtest.

### `list_hypotheses`

List registered research hypotheses, optionally filtered by lifecycle (hypothesis/draft/failed/rejectedByHuman/retired/active). Use before proposing new hypotheses to avoid repeating rejected ideas.

### `propose_draft_setup`

Evaluate the strict promotion gate for a hypothesis run and transition hypothesis->draft on pass, or hypothesis->failed on fail. Requires explicit setupId and completed jobId.

### `query_conditional`

Conditional probability query: 'When event X happens N+ times in a resolved trading-day/session unit, how often does outcome Y occur in the matching session summary?' Example: 'If IB-mid is tested 3+ times, how often do we close above IB-mid?' Returns probability, sample size, counts, and metadata notes for missing summaries or truncation.

### `query_distribution`

Distribution of a numeric metric from session summaries. Returns mean, median, population stddev, Type-7 linear-interpolation percentiles (10/25/75/90), min, max, and metadata. Metrics: ib_range, session_delta, total_volume, rvol_ratio, tick_count, vwap_close, etc.

### `query_event_frequency`

Query how often a market event occurs. Returns total occurrences, sessions with event, per-session average, percentage of sessions, and research metadata. Session counts use resolved trading-day/session context under the requested scope; exact duplicate market-event rows are ignored by DB identity constraints, but distinct occurrences of the same phenomenon still count separately. Structural event types: *_test (level tests), ib_extension_hit, ib_formed, or_formed, new_session_high/low, day_type_change, poor_high/low_detected, excess_high/low_detected, or5_mid_retest, dnp_cross, rvol_spike. Flow event types: absorption_detected/absorption_confirmed/absorption_invalidated (metadata.eventSubtype: absorption/exhaustion/delta_divergence), pinch_detected (metadata.timeframe: 1m/5m/15m/30m), acceleration_zone_created, acceleration_zone_held, large_trade_cluster.

### `query_signal_outcome_conditional`

Conditional win rate for signal outcomes: when setup X fires and the matching resolved trading-day/session summary has field=value (e.g. day_type=Trend), what is the win rate? Joins signal_outcomes to session_summaries by compound session key and returns research metadata. Requires signal_outcomes to be populated.

### `query_signal_outcome_distribution`

Distribution of R-results from signal_outcomes for a setup. Answers: 'When setup X fires, what is the distribution of R-results?' Returns mean, median, population stddev, Type-7 percentiles, and metadata. Requires signal_outcomes to be populated (run backtest or live tracking).

### `query_signal_outcome_excursions`

Outcome excursion diagnostics for signal outcomes. Returns distributions for max favorable excursion (MFE), max adverse excursion (MAE), time-to-outcome (minutes), and MFE/MAE ratio, plus resolved outcome breakdown and top-level research metadata. Use to evaluate execution quality and target/stop behavior.

### `register_hypothesis`

Register or dry-run validate a research hypothesis as an inactive per-version SetupDefinition. First-slice scope is RTH only; use run_backtest with the returned setupId to execute.

### `run_backtest`

Queue a backtest replay job and return a job id. Replays the rules engine over historical .scid data without blocking the MCP server.

### `set_hypothesis_lifecycle`

Manually transition a hypothesis/draft to rejectedByHuman or retired with a required reason. Does not activate setups.

### `summarize_hypothesis_run`

Summarize one completed hypothesis backtest run by explicit setupId and jobId. Reads signal_outcomes/backtest_runs and returns gate metrics without changing lifecycle.

### `validate_signal_outcome_integrity`

Validate signal_outcomes integrity for research/backtest trust. Returns quality counts, failed invariant counts, legacy ratio, and ok/warning/failed status. Filter by source, jobId, or setupId before running setup studies.

## Admin

Operations: feed health, tick ingestion, contract rollover, archival, and data integrity validation.

Module: `src/bin/the-desk-mcp/tools/admin.rs`

### `archive_status`

Storage tier status: shows hot (current session), warm (SQLite ticks), and cold (archived) tier sizes. Includes session summary count and last archive date. Use to monitor data lifecycle.

### `backfill_history`

Queue a historical backfill job and return a job id. Processes past sessions through all 14 pipelines, detects market events, and persists session summaries without blocking the MCP server.

### `create_database_backup`

Create a verified on-demand snapshot of the SQLite database (trades, journal, signal outcomes, memory, all session data) using VACUUM INTO, then prune old snapshots per the [backup] config. Unlike the automatic startup backup, this ignores the minimum-interval gate and always writes a fresh snapshot — call it before risky operations (large imports, schema changes) or when the trader wants a known-good restore point. Returns the new snapshot's path/size, how many old backups were pruned, and the full list of retained backups. Backups live in ~/.the-desk/backups by default.

### `get_contract_rollover_status`

Validate active futures contract rollover state before trusting prior-session references. Compares freshly resolved contract, live pipeline contract, current-contract prior levels, same-root legacy levels, resolver warnings, and feed freshness. Returns whether prior-day references are authoritative, legacy-context-only, or should be cleared/backfilled.

### `get_feed_health`

Feed health diagnostics: SCID path status, file metadata, latest DB tick timestamp, ingest lag, freshness/source state, and contract rollover status.

### `get_raw_tick_ingest_gaps`

Report raw_ticks DB coverage vs the active .scid file for the configured contract: SCID first/last timestamps, DB min/max tick times, session_summary date span, and missing ranges (prefix/suffix only — internal tape holes are not detected). Optional startDate/endDate (YYYY-MM-DD) clip.

### `get_runtime_events`

Recent MCP runtime diagnostics: structured startup, feed, session-boundary, setup-transition, background-job, and worker events. Use this for post-mortems after get_feed_health/validate_data_integrity flags a problem; not for raw tick data.

### `ingest_raw_ticks_from_scid`

Load trades from the Sierra .scid file into SQLite raw_ticks using INSERT OR IGNORE. Default onlyGaps=true fills prefix/suffix gaps vs existing rows for the current contract; onlyGaps=false scans the full date clip. Separate from backfill_history (which replays pipelines / session summaries without persisting raw ticks). Large ingests: set waitForCompletion=false to avoid MCP timeouts (check dbTickCount via get_session_summary).

### `query_ticks`

Query raw tick data. Without filters, returns the most recent ticks (most-recent first). With start_time_ms/end_time_ms, returns ticks in that time window in chronological order (ASC) — ideal for reconstructing the tape at a specific moment. With price_low/price_high, limits to trades in that price range. With session_date (YYYY-MM-DD), limits to that trading day. All filters can be combined. Use get_market_snapshot to get the current timestamp_ms and work backward from there.

### `scan_scid_timestamp_anomalies`

Scan the active Sierra .scid file in byte order for equal or backward timestamps. Returns anomaly counts, worst backward delta, and capped samples for the requested date clip.

### `validate_contract_rollover`

Validate active futures contract rollover state before trusting prior-session references. Alias of get_contract_rollover_status using validate_* taxonomy for pre-session safety gates.

### `validate_data_integrity`

Validate data integrity: checks tick count, stream freshness, contract rollover status, and pipeline consistency invariants (POC within VA, VA contains ~70%% of TPOs, delta sum consistency). Returns pass/fail status with details.

