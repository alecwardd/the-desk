//! Tool parameter structs (serde + JsonSchema) shared across tool modules.

use schemars::JsonSchema;
use serde::Deserialize;

#[allow(unused_imports)]
use crate::{helpers::*, lifecycle::*, state::*};

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(crate) struct LimitParams {
    /// Maximum number of items to return (default 25).
    pub(crate) limit: Option<u64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OptionsSnapshotParams {
    /// Optional root symbol. Defaults to [options].convexvalue_probe_root.
    pub(crate) root: Option<String>,
    /// Optional expiration selectors accepted by ConvexValue.
    pub(crate) exps: Option<Vec<u32>>,
    /// Optional spot-relative range filter (for example 0.10 for +/-10%).
    pub(crate) range: Option<f64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GammaLevelsParams {
    /// Optional root symbol. Defaults to [options].convexvalue_probe_root.
    pub(crate) root: Option<String>,
    /// Optional expiration selectors accepted by ConvexValue.
    pub(crate) exps: Option<Vec<u32>>,
    /// Optional spot-relative range filter (for example 0.10 for +/-10%).
    pub(crate) range: Option<f64>,
    /// Maximum number of strikes to return (default 12, max 50).
    pub(crate) top: Option<u64>,
    /// Force a network refresh instead of serving a warm cache hit.
    pub(crate) force_refresh: Option<bool>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OptionsContextParams {
    /// Optional root symbol. Defaults to [options].convexvalue_probe_root.
    pub(crate) root: Option<String>,
    /// Optional expiration selectors accepted by ConvexValue.
    pub(crate) exps: Option<Vec<u32>>,
    /// Optional spot-relative range filter (for example 0.10 for +/-10%).
    pub(crate) range: Option<f64>,
    /// Force a network refresh instead of serving a warm cache hit.
    pub(crate) force_refresh: Option<bool>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(crate) struct TickQueryParams {
    /// Maximum number of ticks to return (default 200, max 2000). When a time range is set,
    /// results are returned in ascending chronological order; otherwise most-recent first.
    pub(crate) limit: Option<u64>,
    /// Start of time range as Unix epoch milliseconds (e.g. 1740092400000.0).
    /// Use get_market_snapshot to find the current timestamp, then subtract to target earlier times.
    pub(crate) start_time_ms: Option<f64>,
    /// End of time range as Unix epoch milliseconds.
    pub(crate) end_time_ms: Option<f64>,
    /// Filter to ticks at or above this price.
    pub(crate) price_low: Option<f64>,
    /// Filter to ticks at or below this price.
    pub(crate) price_high: Option<f64>,
    /// Filter to a specific trading session date in YYYY-MM-DD format (e.g. "2026-03-04").
    pub(crate) session_date: Option<String>,
    /// Optional root-symbol filter (e.g. NQ).
    pub(crate) root_symbol: Option<String>,
    /// Optional contract-symbol filter (e.g. NQM26.CME).
    pub(crate) contract_symbol: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(crate) struct FootprintWindowParams {
    /// Start of time window as Unix epoch milliseconds. Required for meaningful output.
    pub(crate) start_time_ms: Option<f64>,
    /// End of time window as Unix epoch milliseconds. Required for meaningful output.
    pub(crate) end_time_ms: Option<f64>,
    /// Optional: only return levels at or above this price.
    pub(crate) price_low: Option<f64>,
    /// Optional: only return levels at or below this price.
    pub(crate) price_high: Option<f64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(crate) struct FootprintParams {
    /// Optional: only return levels at or above this price. Filtering happens before the top-30 volume sort.
    pub(crate) price_low: Option<f64>,
    /// Optional: only return levels at or below this price. Filtering happens before the top-30 volume sort.
    pub(crate) price_high: Option<f64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(crate) struct TpoDetailParams {
    /// Optional: only return levels at or above this price.
    pub(crate) price_low: Option<f64>,
    /// Optional: only return levels at or below this price.
    pub(crate) price_high: Option<f64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(crate) struct SnapshotAtParams {
    /// Target time as Unix epoch milliseconds. Returns the stored pipeline snapshot
    /// closest to this timestamp. Snapshots are stored every ~30 seconds.
    pub(crate) timestamp_ms: Option<f64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ContextFrameParams {
    /// Optional historical timestamp to frame. Omit for the current live market frame.
    #[serde(alias = "timestamp_ms")]
    pub(crate) timestamp_ms: Option<f64>,
    /// Optional setup ID to include setup-specific signal outcome context.
    #[serde(alias = "setup_id")]
    pub(crate) setup_id: Option<String>,
    /// Include historical analogs and forward-path stats. Default true.
    pub(crate) include_historical: Option<bool>,
    /// Historical matching mode: weightedAnalog (default) or strictBucket.
    pub(crate) matching_mode: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DomSnapshotAtParams {
    /// Target time as Unix epoch milliseconds for delayed DOM reconstruction.
    pub(crate) timestamp_ms: f64,
    /// Number of price levels to return on each side (default 10, max 25).
    pub(crate) levels_per_side: Option<u64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PullStackParams {
    /// Inclusive start time as Unix epoch milliseconds.
    pub(crate) start_time_ms: f64,
    /// Exclusive end time as Unix epoch milliseconds.
    pub(crate) end_time_ms: f64,
    /// Optional lower bound to focus on a specific price zone.
    pub(crate) price_low: Option<f64>,
    /// Optional upper bound to focus on a specific price zone.
    pub(crate) price_high: Option<f64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LiquidityBehaviorParams {
    /// Inclusive start time as Unix epoch milliseconds.
    pub(crate) start_time_ms: f64,
    /// Exclusive end time as Unix epoch milliseconds.
    pub(crate) end_time_ms: f64,
    /// Center price to inspect.
    pub(crate) price: f64,
    /// Radius around the target price in ticks (default 4, max 20).
    pub(crate) radius_ticks: Option<u64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DomWindowParams {
    pub(crate) start_time_ms: Option<f64>,
    pub(crate) end_time_ms: Option<f64>,
    pub(crate) price_low: Option<f64>,
    pub(crate) price_high: Option<f64>,
    pub(crate) limit: Option<usize>,
    pub(crate) include_aggregate: Option<bool>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DomTapeContextParams {
    pub(crate) timestamp_ms: f64,
    pub(crate) window_ms: Option<f64>,
    pub(crate) price_low: Option<f64>,
    pub(crate) price_high: Option<f64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExplainBookReactionParams {
    pub(crate) timestamp_ms: Option<f64>,
    pub(crate) price: Option<f64>,
    pub(crate) start_time_ms: Option<f64>,
    pub(crate) end_time_ms: Option<f64>,
    pub(crate) radius_ticks: Option<u64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DomRegimeSummaryParams {
    pub(crate) timestamp_ms: Option<f64>,
    pub(crate) start_time_ms: Option<f64>,
    pub(crate) end_time_ms: Option<f64>,
    pub(crate) window_ms: Option<f64>,
    pub(crate) limit: Option<usize>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DomBehaviorFrequencyParams {
    pub(crate) behavior: String,
    pub(crate) min_duration_ms: Option<f64>,
    pub(crate) start_date: Option<String>,
    pub(crate) end_date: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DomBehaviorConditionalParams {
    pub(crate) behavior: String,
    pub(crate) setup_id: Option<String>,
    pub(crate) min_duration_ms: Option<f64>,
    pub(crate) start_date: Option<String>,
    pub(crate) end_date: Option<String>,
    pub(crate) source: Option<String>,
    #[serde(alias = "job_id", alias = "jobId")]
    pub(crate) job_id: Option<String>,
    #[serde(alias = "includeUnverified")]
    pub(crate) include_unverified: Option<bool>,
    #[schemars(schema_with = "schemars_optional_loose_object")]
    pub(crate) scope: Option<serde_json::Value>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DomReactionAtLevelsParams {
    pub(crate) event_type: String,
    pub(crate) behavior: String,
    pub(crate) min_duration_ms: Option<f64>,
    pub(crate) start_date: Option<String>,
    pub(crate) end_date: Option<String>,
    #[schemars(schema_with = "schemars_optional_loose_object")]
    pub(crate) scope: Option<serde_json::Value>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(crate) struct DeltaConfirmParams {
    /// True for a buy/long setup, false for a sell/short setup.
    pub(crate) is_buy_setup: Option<bool>,
    /// Optional price level to check delta at. Defaults to current price.
    pub(crate) price: Option<f64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(crate) struct DeltaAtPriceParams {
    /// Price level to query delta at. Omit for current price.
    pub(crate) price: Option<f64>,
    /// Number of top prices by absolute delta to return (default 10).
    pub(crate) top_n: Option<usize>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(crate) struct SetupContextParams {
    /// Name of the setup template (e.g. "OR5 Mid Retest", "DNVA Retest").
    pub(crate) setup_name: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(crate) struct ProximityParams {
    /// Maximum distance in ticks to include in the report (default 20).
    pub(crate) max_distance_ticks: Option<f64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(crate) struct SaveAccountStateParams {
    /// Current account balance in dollars.
    pub(crate) last_balance_dollars: Option<f64>,
    /// Open positions not from chat: array of {direction, size, entryPrice, instrument?, setupId?}.
    pub(crate) open_positions: Option<Vec<OpenPositionInput>>,
    /// Lucid daily loss limit in dollars (e.g. 750).
    pub(crate) lucid_daily_loss_dollars: Option<f64>,
    /// Lucid account size in dollars (e.g. 50000).
    pub(crate) lucid_account_size_dollars: Option<f64>,
    /// Profit target per payout cycle (e.g. 2000).
    pub(crate) profit_target_per_cycle: Option<f64>,
    /// Position sizing method (default quarter_kelly).
    pub(crate) position_sizing_method: Option<String>,
    /// Kelly fraction (default 0.25).
    pub(crate) kelly_fraction: Option<f64>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub(crate) struct OpenPositionInput {
    pub(crate) direction: String,
    pub(crate) size: i64,
    pub(crate) entry_price: f64,
    pub(crate) instrument: Option<String>,
    pub(crate) setup_id: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(crate) struct KellyPositionSizeParams {
    /// Setup ID for setup-specific stats. Omit for aggregate.
    pub(crate) setup_id: Option<String>,
    /// Current account balance in dollars (for sizing calc).
    pub(crate) balance_dollars: Option<f64>,
    /// Confidence multiplier: 0.5=low, 1.0=normal, 1.5=high.
    pub(crate) confidence_multiplier: Option<f64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct RecordTradeResultParams {
    /// Trade direction: "long" or "short".
    pub(crate) direction: String,
    /// Number of contracts.
    pub(crate) size: i64,
    /// Entry price.
    pub(crate) entry_price: f64,
    /// Exit price.
    pub(crate) exit_price: f64,
    /// Result in R-units (positive = win, negative = loss).
    pub(crate) result_r: f64,
    /// Optional setup ID for performance tracking.
    pub(crate) setup_id: Option<String>,
    /// Optional stop price used.
    pub(crate) stop_price: Option<f64>,
    /// Optional notes about the trade.
    pub(crate) notes: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SaveRiskConfigParams {
    /// R-value in NQ points (e.g. 50).
    pub(crate) r_value_points: Option<f64>,
    /// R-value in dollars (e.g. 250 for MNQ).
    pub(crate) r_value_dollars: Option<f64>,
    /// Max daily loss in R-units before session stop (e.g. 3).
    pub(crate) max_daily_loss_r: Option<f64>,
    /// Max consecutive losses before circuit breaker (e.g. 3).
    pub(crate) max_consecutive_losses: Option<i64>,
    /// Max trades per session (e.g. 8).
    pub(crate) max_trades_per_session: Option<i64>,
    /// Max daily loss in dollars (e.g. 750). Used with Lucid params.
    pub(crate) max_daily_loss_dollars: Option<f64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BackfillParams {
    /// Start date (YYYY-MM-DD). Omit for "all available".
    #[serde(alias = "start_date")]
    pub(crate) start_date: Option<String>,
    /// End date (YYYY-MM-DD). Omit for "through today".
    #[serde(alias = "end_date")]
    pub(crate) end_date: Option<String>,
    /// Reprocess sessions even if summaries already exist.
    #[serde(alias = "force")]
    pub(crate) force: Option<bool>,
    /// Run rules engine during backfill to populate signal outcomes (backtest replay).
    #[serde(alias = "run_rules")]
    pub(crate) run_rules: Option<bool>,
    /// Setup IDs to evaluate. Omit for all active setups.
    #[serde(alias = "setup_ids")]
    pub(crate) setup_ids: Option<Vec<String>>,
    /// Wait for the background job to complete before responding.
    #[serde(alias = "wait_for_completion")]
    pub(crate) wait_for_completion: Option<bool>,
    /// Optional contract to replay (e.g. "NQH6.CME"). When set, the job reads
    /// that contract's `.scid` and pins its metadata WITHOUT mutating global
    /// feed config — so a historical window can use the contract that was front
    /// then while live trading stays on the current front month. Mainly for
    /// backtests; pass the contract instead of flipping `active_symbol_override`.
    #[serde(alias = "contract_symbol", alias = "contract")]
    pub(crate) contract_symbol: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BackfillStatusParams {
    #[serde(alias = "job_id")]
    pub(crate) job_id: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RawTickIngestGapParams {
    /// Optional start of clip window (YYYY-MM-DD, ET midnight).
    #[serde(alias = "start_date")]
    pub(crate) start_date: Option<String>,
    /// Optional end of clip window (YYYY-MM-DD, exclusive at next midnight).
    #[serde(alias = "end_date")]
    pub(crate) end_date: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct IngestRawTicksParams {
    #[serde(alias = "start_date")]
    pub(crate) start_date: Option<String>,
    #[serde(alias = "end_date")]
    pub(crate) end_date: Option<String>,
    /// When true (default), only SCID windows missing from raw_ticks for this contract.
    #[serde(alias = "only_gaps")]
    pub(crate) only_gaps: Option<bool>,
    #[serde(alias = "wait_for_completion")]
    pub(crate) wait_for_completion: Option<bool>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ScanScidTimestampAnomaliesParams {
    #[serde(alias = "start_date")]
    pub(crate) start_date: Option<String>,
    #[serde(alias = "end_date")]
    pub(crate) end_date: Option<String>,
    pub(crate) max_events_reported: Option<usize>,
    pub(crate) persist_result: Option<bool>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimeEventsParams {
    /// Maximum events to return (default 50, max 500).
    pub(crate) limit: Option<usize>,
    /// Only include events emitted at or after this Unix epoch millisecond timestamp.
    pub(crate) since_ms: Option<f64>,
    /// Exact level filter: trace, debug, info, warn, or error. Do not combine with minLevel.
    pub(crate) level: Option<String>,
    /// Minimum level filter: returns events at this level or higher. Prefer this for post-mortems; mutually exclusive with level.
    pub(crate) min_level: Option<String>,
    /// Exact category filter, e.g. scid, session, setup, depth, historical_job.
    pub(crate) category: Option<String>,
    /// Exact stable event name filter, e.g. scid.tail_reset.
    pub(crate) event_name: Option<String>,
    /// Include persisted SQLite events in addition to the in-memory ring buffer.
    pub(crate) include_persisted: Option<bool>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CancelBackfillParams {
    #[serde(alias = "job_id")]
    pub(crate) job_id: String,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(crate) struct FrequencyParams {
    /// Event type to query (e.g. "ib_mid_test", "new_session_high").
    pub(crate) event_type: String,
    /// Start date filter (YYYY-MM-DD).
    pub(crate) start_date: Option<String>,
    /// End date filter (YYYY-MM-DD).
    pub(crate) end_date: Option<String>,
    /// Optional session/trading-day scope filter.
    #[serde(flatten)]
    pub(crate) session_scope: SessionScopeParams,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(crate) struct ConditionalParams {
    /// Event type for the condition (e.g. "ib_mid_test").
    pub(crate) event_type: String,
    /// Minimum event count per session to satisfy the condition.
    pub(crate) min_count: Option<i64>,
    /// Session summary field to check (e.g. "close_vs_ib_mid", "ib_extension_state", "day_type").
    pub(crate) outcome_field: String,
    /// Value to match (e.g. "above", "below", "Trend").
    pub(crate) outcome_value: String,
    /// Start date filter (YYYY-MM-DD).
    pub(crate) start_date: Option<String>,
    /// End date filter (YYYY-MM-DD).
    pub(crate) end_date: Option<String>,
    /// Optional session/trading-day scope filter.
    #[serde(flatten)]
    pub(crate) session_scope: SessionScopeParams,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(crate) struct DistributionParams {
    /// Metric column from session_summaries (e.g. "ib_range", "session_delta", "total_volume").
    pub(crate) metric: String,
    /// Start date filter (YYYY-MM-DD).
    pub(crate) start_date: Option<String>,
    /// End date filter (YYYY-MM-DD).
    pub(crate) end_date: Option<String>,
    /// Optional session/trading-day scope filter.
    #[serde(flatten)]
    pub(crate) session_scope: SessionScopeParams,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(crate) struct SignalOutcomeDistributionParams {
    /// Setup ID to analyze (e.g. "or5-mid-retest").
    pub(crate) setup_id: String,
    /// Start date filter (YYYY-MM-DD).
    pub(crate) start_date: Option<String>,
    /// End date filter (YYYY-MM-DD).
    pub(crate) end_date: Option<String>,
    /// Optional source filter: live | backtest | backfill.
    pub(crate) source: Option<String>,
    /// Optional backtest/backfill job ID filter.
    #[serde(alias = "job_id", alias = "jobId")]
    pub(crate) job_id: Option<String>,
    /// Include legacyUnverified/notBacktestable rows during transition (default true).
    #[serde(alias = "includeUnverified")]
    pub(crate) include_unverified: Option<bool>,
    /// Optional session/trading-day scope filter.
    #[serde(flatten)]
    pub(crate) session_scope: SessionScopeParams,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(crate) struct SignalOutcomeConditionalParams {
    /// Setup ID to analyze.
    pub(crate) setup_id: String,
    /// Session summary field to filter by (e.g. "day_type", "profile_shape", "balance_state").
    pub(crate) session_field: String,
    /// Value to match (e.g. "Trend", "Normal", "above").
    pub(crate) field_value: String,
    /// Start date filter (YYYY-MM-DD).
    pub(crate) start_date: Option<String>,
    /// End date filter (YYYY-MM-DD).
    pub(crate) end_date: Option<String>,
    /// Optional source filter: live | backtest | backfill.
    pub(crate) source: Option<String>,
    /// Optional backtest/backfill job ID filter.
    #[serde(alias = "job_id", alias = "jobId")]
    pub(crate) job_id: Option<String>,
    /// Include legacyUnverified/notBacktestable rows during transition (default true).
    #[serde(alias = "includeUnverified")]
    pub(crate) include_unverified: Option<bool>,
    /// Optional session/trading-day scope filter.
    #[serde(flatten)]
    pub(crate) session_scope: SessionScopeParams,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SignalOutcomeExcursionsParams {
    /// Setup ID to analyze. Omit for combined outcomes across setups.
    #[serde(alias = "setup_id")]
    pub(crate) setup_id: Option<String>,
    /// Start date filter (YYYY-MM-DD).
    #[serde(alias = "start_date")]
    pub(crate) start_date: Option<String>,
    /// End date filter (YYYY-MM-DD).
    #[serde(alias = "end_date")]
    pub(crate) end_date: Option<String>,
    /// Optional source filter: live | backtest | backfill.
    pub(crate) source: Option<String>,
    /// Optional backtest/backfill job ID filter.
    #[serde(alias = "job_id", alias = "jobId")]
    pub(crate) job_id: Option<String>,
    /// Include legacyUnverified/notBacktestable rows during transition (default true).
    #[serde(alias = "includeUnverified")]
    pub(crate) include_unverified: Option<bool>,
    /// Optional session/trading-day scope filter.
    #[serde(flatten)]
    pub(crate) session_scope: SessionScopeParams,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(crate) struct SessionHistoryParams {
    /// Start date filter (YYYY-MM-DD).
    pub(crate) start_date: Option<String>,
    /// End date filter (YYYY-MM-DD).
    pub(crate) end_date: Option<String>,
    /// Filter by day type (e.g. "Trend", "Normal").
    pub(crate) day_type: Option<String>,
    /// Maximum number of sessions to return (default 20).
    pub(crate) limit: Option<u64>,
    /// Optional session/trading-day scope filter.
    #[serde(flatten)]
    pub(crate) session_scope: SessionScopeParams,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SetupPerformanceMatrixParams {
    /// Start date filter (YYYY-MM-DD).
    #[serde(alias = "start_date")]
    pub(crate) start_date: Option<String>,
    /// End date filter (YYYY-MM-DD).
    #[serde(alias = "end_date")]
    pub(crate) end_date: Option<String>,
    /// Minimum resolved outcomes required for inclusion (default 0).
    #[serde(alias = "min_resolved")]
    pub(crate) min_resolved: Option<i64>,
    /// Sort key: winRate | avgR | resolved | totalSignals (default resolved).
    #[serde(alias = "sort_by")]
    pub(crate) sort_by: Option<String>,
    /// Maximum number of setup rows to return (default 50).
    pub(crate) limit: Option<u64>,
    /// Optional session/trading-day scope filter.
    #[serde(flatten)]
    pub(crate) session_scope: SessionScopeParams,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(crate) struct CompareBacktestsParams {
    /// Backtest run IDs to compare.
    pub(crate) run_ids: Vec<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(crate) struct CompareSessionsParams {
    /// Current IB range for similarity matching.
    pub(crate) current_ib_range: Option<f64>,
    /// Current day type for filtering.
    pub(crate) current_day_type: Option<String>,
    /// Profile shape (e.g. "Normal", "Trend", "DoubleDistribution").
    pub(crate) profile_shape: Option<String>,
    /// Balance state (e.g. "Balanced", "Building", "Clearing").
    pub(crate) balance_state: Option<String>,
    /// Current RVOL ratio for similarity.
    pub(crate) rvol_ratio: Option<f64>,
    /// Session delta sign: "positive", "negative", or "neutral".
    pub(crate) session_delta_sign: Option<String>,
    /// Single prints direction for similarity.
    pub(crate) single_prints_direction: Option<String>,
    /// Max similar sessions to return (default 5).
    pub(crate) max_results: Option<u64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(crate) struct SignalPerformanceParams {
    /// Setup ID to filter by.
    pub(crate) setup_id: Option<String>,
    /// Start date filter (YYYY-MM-DD).
    pub(crate) start_date: Option<String>,
    /// End date filter (YYYY-MM-DD).
    pub(crate) end_date: Option<String>,
    /// Optional source filter: "live" or "backtest".
    pub(crate) source: Option<String>,
    /// Optional backtest job ID filter.
    #[serde(alias = "job_id", alias = "jobId")]
    pub(crate) job_id: Option<String>,
    /// Optional session/trading-day scope filter.
    #[serde(flatten)]
    pub(crate) session_scope: SessionScopeParams,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SignalOutcomeIntegrityParams {
    /// Optional source filter: "live", "backtest", or "backfill".
    pub(crate) source: Option<String>,
    /// Optional backtest job ID filter.
    #[serde(alias = "job_id", alias = "jobId")]
    pub(crate) job_id: Option<String>,
    /// Optional setup ID filter.
    #[serde(alias = "setup_id", alias = "setupId")]
    pub(crate) setup_id: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StartTradingSessionParams {
    pub(crate) session_id: Option<String>,
    pub(crate) session_type: Option<String>,
    pub(crate) start_time_ms: Option<f64>,
    pub(crate) pre_session_note: Option<String>,
    pub(crate) recording_path: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct EndTradingSessionParams {
    pub(crate) session_id: Option<String>,
    pub(crate) end_time_ms: Option<f64>,
    pub(crate) recording_path: Option<String>,
    pub(crate) session_note: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UpsertTradeEntryParams {
    pub(crate) id: Option<String>,
    pub(crate) session_id: Option<String>,
    pub(crate) setup_id: Option<String>,
    pub(crate) instrument: Option<String>,
    pub(crate) trade_account: Option<String>,
    pub(crate) entry_time_ms: Option<f64>,
    pub(crate) entry_price: f64,
    pub(crate) exit_time_ms: Option<f64>,
    pub(crate) exit_price: Option<f64>,
    pub(crate) direction: String,
    pub(crate) size: i64,
    pub(crate) max_open_size: Option<i64>,
    pub(crate) stop_price: Option<f64>,
    pub(crate) target_prices: Option<Vec<f64>>,
    pub(crate) result_r: Option<f64>,
    pub(crate) gross_points: Option<f64>,
    pub(crate) planned: Option<bool>,
    pub(crate) rules_followed: Option<bool>,
    pub(crate) emotional_state: Option<String>,
    pub(crate) thesis: Option<String>,
    pub(crate) review_tags: Option<Vec<String>>,
    pub(crate) mistake_tags: Option<Vec<String>>,
    pub(crate) entry_fill_count: Option<i64>,
    pub(crate) exit_fill_count: Option<i64>,
    pub(crate) import_batch_id: Option<String>,
    pub(crate) notes: Option<String>,
    pub(crate) source: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CloseTradeEntryParams {
    pub(crate) id: String,
    pub(crate) exit_price: f64,
    pub(crate) exit_time_ms: Option<f64>,
    pub(crate) result_r: Option<f64>,
    pub(crate) gross_points: Option<f64>,
    pub(crate) notes: Option<String>,
    pub(crate) update_risk_state: Option<bool>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ReviewTradeEntryParams {
    pub(crate) id: String,
    pub(crate) planned: bool,
    pub(crate) rules_followed: Option<bool>,
    pub(crate) emotional_state: Option<String>,
    pub(crate) thesis: Option<String>,
    pub(crate) review_tags: Option<Vec<String>>,
    pub(crate) mistake_tags: Option<Vec<String>>,
    pub(crate) notes: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SaveJournalEntryParams {
    pub(crate) id: Option<String>,
    pub(crate) session_id: Option<String>,
    pub(crate) date: Option<String>,
    pub(crate) content: String,
    pub(crate) tags: Option<Vec<String>>,
    pub(crate) setup_references: Option<Vec<String>>,
    pub(crate) trade_references: Option<Vec<String>>,
    pub(crate) created_at_ms: Option<f64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TradeListParams {
    pub(crate) session_id: Option<String>,
    pub(crate) limit: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TradeEntryIdParams {
    pub(crate) id: String,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SessionJournalParams {
    pub(crate) session_id: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RecentJournalNotesParams {
    pub(crate) limit: Option<u64>,
    pub(crate) tag: Option<String>,
    pub(crate) setup_reference: Option<String>,
    pub(crate) trade_reference: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SessionReviewContextParams {
    pub(crate) session_id: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct JournalPatternParams {
    pub(crate) start_date: Option<String>,
    pub(crate) end_date: Option<String>,
    pub(crate) session_type: Option<String>,
    pub(crate) limit: Option<u64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SaveAgentInsightParams {
    pub(crate) id: Option<String>,
    pub(crate) session_id: Option<String>,
    pub(crate) trade_id: Option<String>,
    pub(crate) setup_id: Option<String>,
    pub(crate) category: String,
    pub(crate) summary: String,
    #[schemars(schema_with = "schemars_loose_object")]
    pub(crate) evidence: serde_json::Value,
    pub(crate) tags: Option<Vec<String>>,
    #[schemars(schema_with = "schemars_optional_loose_object")]
    pub(crate) scope: Option<serde_json::Value>,
    pub(crate) confidence: Option<f64>,
    pub(crate) salience: Option<f64>,
    pub(crate) source: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RecallAgentInsightsParams {
    pub(crate) category: Option<String>,
    pub(crate) setup_id: Option<String>,
    pub(crate) statuses: Option<Vec<String>>,
    pub(crate) tag: Option<String>,
    pub(crate) session_type: Option<String>,
    pub(crate) session_segment: Option<String>,
    pub(crate) time_bucket: Option<String>,
    pub(crate) day_type: Option<String>,
    pub(crate) start_date: Option<String>,
    pub(crate) end_date: Option<String>,
    pub(crate) limit: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InsightAcknowledgeParams {
    pub(crate) id: String,
    pub(crate) action: String,
    pub(crate) surfaced_at_ms: Option<f64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SupersedeInsightParams {
    pub(crate) previous_id: String,
    pub(crate) replacement_id: String,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BehavioralPatternMemoryParams {
    pub(crate) pattern_type: Option<String>,
    pub(crate) session_type: Option<String>,
    pub(crate) session_segment: Option<String>,
    pub(crate) time_bucket: Option<String>,
    pub(crate) day_type: Option<String>,
    pub(crate) setup_id: Option<String>,
    pub(crate) min_sample_size: Option<i64>,
    pub(crate) active_only: Option<bool>,
    pub(crate) limit: Option<u64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CreateMemoryFollowupParams {
    pub(crate) id: Option<String>,
    pub(crate) session_id: Option<String>,
    pub(crate) trade_id: Option<String>,
    pub(crate) source: Option<String>,
    pub(crate) title: String,
    pub(crate) detail: Option<String>,
    pub(crate) tags: Option<Vec<String>>,
    #[schemars(schema_with = "schemars_optional_loose_object")]
    pub(crate) due_context: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResolveMemoryFollowupParams {
    pub(crate) id: String,
    pub(crate) resolution_note: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SetupStateHistoryParams {
    pub(crate) setup_id: Option<String>,
    pub(crate) session_date: Option<String>,
    pub(crate) minutes: Option<f64>,
    pub(crate) limit: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SetupLifecycleParams {
    pub(crate) setup_id: String,
}

#[derive(Debug, Default, Clone, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AttentionCursorParams {
    #[serde(alias = "last_signal_id")]
    pub(crate) last_signal_id: Option<String>,
    #[serde(alias = "last_event_id")]
    pub(crate) last_event_id: Option<String>,
    #[serde(alias = "since_ms")]
    pub(crate) since_ms: Option<f64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AttentionInboxParams {
    pub(crate) status: Option<String>,
    #[serde(alias = "min_priority")]
    pub(crate) min_priority: Option<String>,
    pub(crate) limit: Option<usize>,
    #[serde(alias = "include_expired")]
    pub(crate) include_expired: Option<bool>,
    pub(crate) cursor: Option<AttentionCursorParams>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AttentionSignalDetailParams {
    #[serde(alias = "signal_id")]
    pub(crate) signal_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AttentionSignalAcknowledgeParams {
    #[serde(alias = "signal_id")]
    pub(crate) signal_id: String,
    #[serde(alias = "acknowledged_by")]
    pub(crate) acknowledged_by: String,
    pub(crate) note: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WhatChangedSinceParams {
    pub(crate) cursor: Option<AttentionCursorParams>,
    pub(crate) limit: Option<usize>,
    #[serde(alias = "include_signals")]
    pub(crate) include_signals: Option<bool>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AttentionChangelogParams {
    pub(crate) cursor: Option<AttentionCursorParams>,
    pub(crate) limit: Option<usize>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ActiveTradeIdeasParams {
    pub(crate) status: Option<String>,
    #[serde(alias = "setup_id")]
    pub(crate) setup_id: Option<String>,
    pub(crate) limit: Option<usize>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TradeIdeaConfirmParams {
    #[serde(alias = "idea_id")]
    pub(crate) idea_id: String,
    #[serde(alias = "evidence_note")]
    pub(crate) evidence_note: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TradeIdeaInvalidateParams {
    #[serde(alias = "idea_id")]
    pub(crate) idea_id: String,
    #[serde(alias = "reason_code")]
    pub(crate) reason_code: String,
    pub(crate) note: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TradeIdeaInTradeParams {
    #[serde(alias = "idea_id")]
    pub(crate) idea_id: String,
    #[serde(alias = "signal_outcome_id")]
    pub(crate) signal_outcome_id: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TradeIdeaResolveParams {
    #[serde(alias = "idea_id")]
    pub(crate) idea_id: String,
    pub(crate) outcome: String,
    pub(crate) note: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MemoryBriefParams {
    pub(crate) intent: Option<String>,
    pub(crate) session_id: Option<String>,
    pub(crate) setup_id: Option<String>,
    pub(crate) session_type: Option<String>,
    pub(crate) session_segment: Option<String>,
    pub(crate) day_type: Option<String>,
    pub(crate) time_bucket: Option<String>,
    pub(crate) pre_session_note: Option<String>,
    pub(crate) limit: Option<u64>,
    pub(crate) include_recent_sessions: Option<bool>,
    pub(crate) include_patterns: Option<bool>,
    pub(crate) include_insights: Option<bool>,
    pub(crate) include_followups: Option<bool>,
    /// When true, `get_pre_session_briefing` skips the bounded automatic
    /// `refresh_memory_state` pass even if memory maintenance is dirty.
    pub(crate) skip_memory_refresh_if_dirty: Option<bool>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RefreshMemoryStateParams {
    pub(crate) refresh_patterns: Option<bool>,
    pub(crate) refresh_insight_lifecycle: Option<bool>,
    pub(crate) include_patterns: Option<bool>,
    pub(crate) reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize, serde::Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ImportedFillRowInput {
    pub(crate) entry_time: String,
    pub(crate) last_activity_time: Option<String>,
    pub(crate) symbol: String,
    pub(crate) status: String,
    pub(crate) internal_order_id: Option<String>,
    pub(crate) order_type: Option<String>,
    pub(crate) buy_sell: String,
    pub(crate) open_close: Option<String>,
    pub(crate) order_quantity: Option<i64>,
    pub(crate) price: Option<f64>,
    pub(crate) filled_quantity: Option<i64>,
    pub(crate) average_fill_price: f64,
    pub(crate) parent_internal_order_id: Option<String>,
    pub(crate) service_order_id: Option<String>,
    pub(crate) trade_account: Option<String>,
    pub(crate) exchange_order_id: Option<String>,
    pub(crate) text_tag: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ImportTradeFillsParams {
    pub(crate) rows: Vec<ImportedFillRowInput>,
    pub(crate) batch_id: Option<String>,
    pub(crate) session_id: Option<String>,
    pub(crate) source: Option<String>,
    pub(crate) timezone: Option<String>,
    pub(crate) notes: Option<String>,
}
