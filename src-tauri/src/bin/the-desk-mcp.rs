use rmcp::{
    handler::server::{tool::ToolRouter, wrapper::Parameters},
    model::*,
    tool, tool_handler, tool_router,
    transport::stdio,
    ErrorData as McpError, ServerHandler, ServiceExt,
};
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use the_desk_backend::backfill;
use the_desk_backend::db::{
    AccountStateRecord, Database, HistoricalJobRun, OpenPositionRecord, RiskConfigRecord,
    SessionScopeFilter, SessionSummary, SetupPerformanceSortBy, SignalOutcome, TradeRecord,
};
use the_desk_backend::depth::{
    aggregate_trade_volume_by_level, build_dom_feature_snapshot, build_dom_summary, DepthBook,
    DepthReader, DomFeatureSnapshot, DomSummary, ScanControl as DepthScanControl,
};
use the_desk_backend::feed::scid_reader::{
    parse_record_scaled, ScanControl as ScidScanControl, ScidReader,
};
use the_desk_backend::feed::{load_feed_config, load_storage_config, TradeSide};
use the_desk_backend::outcome_tracker;
use the_desk_backend::pipelines::{
    EventDetector, FlowEventEmitter, PipelineEngine, PriorSessionData, RvolPipeline,
};
use the_desk_backend::research;
use the_desk_backend::risk::{RiskConfig, RiskState, RiskTracker};
use the_desk_backend::rules::RulesEngine;
use the_desk_backend::{
    classify_delta_segment, classify_session, et_minutes_from_timestamp, globex_open_ms,
    minute_of_session_from_timestamp, session_date_from_timestamp_ms, DeltaSegment, SessionType,
    GLOBEX_OPEN_ET, RTH_CLOSE_ET, RTH_OPEN_ET,
};
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::{sleep, Duration};

const FRESHNESS_THRESHOLD_MS: f64 = 15_000.0;
const JOB_PROGRESS_PERSIST_INTERVAL_MS: f64 = 1_000.0;
const JOB_PROGRESS_RECORD_STEP: usize = 50_000;
const JOB_PROGRESS_RATE_EMA_ALPHA: f64 = 0.25;

#[derive(Clone)]
pub struct TheDeskMcp {
    db: Arc<Mutex<Database>>,
    db_path: Arc<String>,
    pipelines: Arc<Mutex<PipelineEngine>>,
    detector: Arc<Mutex<EventDetector>>,
    flow_emitter: Arc<Mutex<FlowEventEmitter>>,
    rules: Arc<Mutex<RulesEngine>>,
    last_bid: Arc<Mutex<f64>>,
    last_ask: Arc<Mutex<f64>>,
    backfill_manager: Arc<AsyncMutex<BackfillManager>>,
    tool_router: ToolRouter<Self>,
}

#[derive(Debug)]
struct InMemoryJobState {
    run: HistoricalJobRun,
    request_key: String,
    cancel_flag: Arc<AtomicBool>,
}

#[derive(Debug, Default)]
struct BackfillManager {
    active_job_id: Option<String>,
    last_job_id: Option<String>,
    jobs: HashMap<String, InMemoryJobState>,
}

fn db_error(e: impl std::fmt::Display) -> McpError {
    McpError::new(ErrorCode::INTERNAL_ERROR, e.to_string(), None)
}

fn lock_error() -> McpError {
    McpError::new(ErrorCode::INTERNAL_ERROR, "database lock poisoned", None)
}

fn freshness_status(age_ms: f64) -> &'static str {
    if age_ms < 0.0 || !age_ms.is_finite() {
        "unknown"
    } else if age_ms <= FRESHNESS_THRESHOLD_MS {
        "ok"
    } else {
        "warning"
    }
}

fn transition_hint(et_minutes: i32) -> Option<(&'static str, &'static str, &'static str)> {
    if (RTH_CLOSE_ET..GLOBEX_OPEN_ET).contains(&et_minutes) {
        Some(("RTH", "Globex", "rth_close_to_globex_open"))
    } else if (GLOBEX_OPEN_ET..GLOBEX_OPEN_ET + 5).contains(&et_minutes) {
        Some(("RTH", "Globex", "globex_open"))
    } else if (RTH_OPEN_ET..RTH_OPEN_ET + 5).contains(&et_minutes) {
        Some(("Globex", "RTH", "rth_open"))
    } else {
        None
    }
}

fn text_result(mut json: serde_json::Value) -> CallToolResult {
    if let Some(obj) = json.as_object_mut() {
        if let Some(age_ms) = obj.get("dataAgeMs").and_then(|v| v.as_f64()) {
            obj.insert(
                "freshnessStatus".to_string(),
                serde_json::json!(freshness_status(age_ms)),
            );
            obj.insert(
                "freshnessThresholdMs".to_string(),
                serde_json::json!(FRESHNESS_THRESHOLD_MS),
            );
        }
    }
    CallToolResult::success(vec![Content::text(json.to_string())])
}

fn no_data(msg: &str) -> CallToolResult {
    CallToolResult::success(vec![Content::text(msg.to_string())])
}

fn invalid_params_error(msg: impl Into<String>) -> McpError {
    McpError::new(ErrorCode::INVALID_PARAMS, msg.into(), None)
}

fn validate_time_window(start_time_ms: f64, end_time_ms: f64) -> Result<(), McpError> {
    if !start_time_ms.is_finite() || !end_time_ms.is_finite() {
        return Err(invalid_params_error(
            "startTimeMs/endTimeMs must be finite numbers",
        ));
    }
    if end_time_ms <= start_time_ms {
        return Err(invalid_params_error(
            "endTimeMs must be greater than startTimeMs",
        ));
    }
    Ok(())
}

fn depth_reader_for_timestamp(timestamp_ms: f64) -> Result<DepthReader, McpError> {
    let config = load_feed_config();
    let path = DepthReader::find_file_for_timestamp(&config, timestamp_ms)
        .map_err(db_error)?
        .ok_or_else(|| {
            invalid_params_error(format!(
                "No Sierra .depth file found for timestamp {timestamp_ms}"
            ))
        })?;
    Ok(DepthReader::new(path, config.price_scale))
}

fn aggregate_window_trades(
    config: &the_desk_backend::feed::FeedConfig,
    start_time_ms: f64,
    end_time_ms: f64,
) -> Result<HashMap<(the_desk_backend::depth::DepthSide, i64), f64>, McpError> {
    let reader = ScidReader::from_feed_config(config);
    let mut trades = Vec::new();
    reader
        .scan_range(Some(start_time_ms), Some(end_time_ms), |tick| {
            trades.push((tick.price, tick.side, tick.volume));
            Ok(ScidScanControl::Continue)
        })
        .map_err(db_error)?;
    Ok(aggregate_trade_volume_by_level(trades))
}

fn latest_depth_reader() -> Result<Option<DepthReader>, McpError> {
    let config = load_feed_config();
    let mut files = DepthReader::list_symbol_depth_files(&config).map_err(db_error)?;
    files.sort();
    Ok(files
        .pop()
        .map(|path| DepthReader::new(path, config.price_scale)))
}

/// Shared helper: read `.depth` + `.scid` files to produce a DOM snapshot and feature summary
/// for a time window.  Used by `get_dom_window`, `get_dom_tape_context_at`, and
/// `explain_book_reaction` fallback paths.
fn compute_dom_feature_for_window(
    start_ms: f64,
    end_ms: f64,
    snapshot_at_ms: f64,
    levels_per_side: usize,
    price_low: Option<f64>,
    price_high: Option<f64>,
) -> Result<(DomFeatureSnapshot, the_desk_backend::depth::DomSnapshot), McpError> {
    let config = load_feed_config();
    let reader = depth_reader_for_timestamp(snapshot_at_ms)?;
    let trades = aggregate_window_trades(&config, start_ms, end_ms)?;
    let activity = reader
        .summarize_window(start_ms, end_ms, &trades, price_low, price_high)
        .map_err(db_error)?;
    let snapshot = reader
        .snapshot_at(snapshot_at_ms, levels_per_side)
        .map_err(db_error)?;
    let feature = build_dom_feature_snapshot(&snapshot, activity);
    Ok((feature, snapshot))
}

fn merge_dom_summary_into_snapshot(
    snapshot: Option<serde_json::Value>,
    dom_summary: &DomSummary,
) -> serde_json::Value {
    let mut snapshot = snapshot.unwrap_or_else(|| serde_json::json!({}));
    if let Some(obj) = snapshot.as_object_mut() {
        obj.insert(
            "domSummary".to_string(),
            serde_json::to_value(dom_summary).unwrap_or_default(),
        );
    }
    snapshot
}

fn footprint_from_ticks(ticks: &[the_desk_backend::db::RawTickRecord]) -> Vec<serde_json::Value> {
    let mut by_price: HashMap<i64, (f64, f64)> = HashMap::new();
    for tick in ticks {
        let key = (tick.price / 0.25).round() as i64;
        let entry = by_price.entry(key).or_insert((0.0, 0.0));
        if tick.is_buy {
            entry.1 += tick.volume;
        } else {
            entry.0 += tick.volume;
        }
    }
    let mut rows = by_price
        .into_iter()
        .map(|(key, (bid_volume, ask_volume))| {
            let total = bid_volume + ask_volume;
            let delta = ask_volume - bid_volume;
            serde_json::json!({
                "price": key as f64 * 0.25,
                "bidVolume": bid_volume,
                "askVolume": ask_volume,
                "totalVolume": total,
                "delta": delta,
                "deltaPerVolume": if total > 0.0 { delta / total } else { 0.0 },
            })
        })
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| {
        a["price"]
            .as_f64()
            .unwrap_or_default()
            .partial_cmp(&b["price"].as_f64().unwrap_or_default())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    rows
}

#[derive(Debug, Default, Deserialize, JsonSchema, Clone)]
#[serde(rename_all = "camelCase")]
struct SessionScopeParams {
    /// Session type filter: "RTH", "Globex", or "Unknown".
    #[serde(alias = "session_type")]
    session_type: Option<String>,
    /// Globex segment filter: "Asia", "London", or "None".
    #[serde(alias = "session_segment")]
    session_segment: Option<String>,
    /// Exact trading day (YYYY-MM-DD, 6 PM ET roll).
    #[serde(alias = "trading_day")]
    trading_day: Option<String>,
    /// Trading-day range start (YYYY-MM-DD, 6 PM ET roll).
    #[serde(alias = "trading_day_start")]
    trading_day_start: Option<String>,
    /// Trading-day range end (YYYY-MM-DD, 6 PM ET roll).
    #[serde(alias = "trading_day_end")]
    trading_day_end: Option<String>,
}

fn normalize_session_type_param(value: &str) -> Option<&'static str> {
    match value.trim().to_ascii_lowercase().as_str() {
        "rth" => Some("RTH"),
        "globex" => Some("Globex"),
        "unknown" => Some("Unknown"),
        _ => None,
    }
}

fn normalize_session_segment_param(value: &str) -> Option<&'static str> {
    match value.trim().to_ascii_lowercase().as_str() {
        "asia" => Some("Asia"),
        "london" => Some("London"),
        "none" => Some("None"),
        _ => None,
    }
}

fn validate_ymd_opt(label: &str, value: Option<&str>) -> Result<(), McpError> {
    if let Some(date) = value {
        if chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").is_err() {
            return Err(invalid_params_error(format!(
                "{label} must be YYYY-MM-DD, got: {date}"
            )));
        }
    }
    Ok(())
}

fn build_session_scope_filter(
    params: &SessionScopeParams,
) -> Result<Option<SessionScopeFilter>, McpError> {
    let mut session_type = params
        .session_type
        .as_deref()
        .map(|raw| {
            normalize_session_type_param(raw).ok_or_else(|| {
                invalid_params_error(format!(
                    "sessionType must be one of RTH|Globex|Unknown, got: {raw}"
                ))
            })
        })
        .transpose()?
        .map(ToString::to_string);

    let session_segment = params
        .session_segment
        .as_deref()
        .map(|raw| {
            normalize_session_segment_param(raw).ok_or_else(|| {
                invalid_params_error(format!(
                    "sessionSegment must be one of Asia|London|None, got: {raw}"
                ))
            })
        })
        .transpose()?
        .map(ToString::to_string);

    if let (Some(st), Some(ss)) = (&session_type, &session_segment) {
        if st == "RTH" && ss != "None" {
            return Err(invalid_params_error(
                "sessionSegment Asia/London is only valid for Globex",
            ));
        }
    }
    if session_segment
        .as_deref()
        .map(|s| s == "Asia" || s == "London")
        .unwrap_or(false)
        && session_type.is_none()
    {
        session_type = Some("Globex".to_string());
    }

    let trading_day_start = params
        .trading_day_start
        .clone()
        .or_else(|| params.trading_day.clone());
    let trading_day_end = params
        .trading_day_end
        .clone()
        .or_else(|| params.trading_day.clone());
    validate_ymd_opt("tradingDay", params.trading_day.as_deref())?;
    validate_ymd_opt("tradingDayStart", trading_day_start.as_deref())?;
    validate_ymd_opt("tradingDayEnd", trading_day_end.as_deref())?;
    if let (Some(sd), Some(ed)) = (trading_day_start.as_deref(), trading_day_end.as_deref()) {
        if sd > ed {
            return Err(invalid_params_error(
                "tradingDayStart must be on or before tradingDayEnd",
            ));
        }
    }

    let scope = SessionScopeFilter {
        session_type,
        session_segment,
        trading_day_start,
        trading_day_end,
    };
    if scope.session_type.is_none()
        && scope.session_segment.is_none()
        && scope.trading_day_start.is_none()
        && scope.trading_day_end.is_none()
    {
        Ok(None)
    } else {
        Ok(Some(scope))
    }
}

fn parse_setup_perf_sort(sort_by: Option<&str>) -> Result<SetupPerformanceSortBy, McpError> {
    match sort_by.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
        None | Some("resolved") => Ok(SetupPerformanceSortBy::Resolved),
        Some("winrate") => Ok(SetupPerformanceSortBy::WinRate),
        Some("avgr") => Ok(SetupPerformanceSortBy::AvgR),
        Some("totalsignals") => Ok(SetupPerformanceSortBy::TotalSignals),
        Some(other) => Err(invalid_params_error(format!(
            "sortBy must be one of winRate|avgR|resolved|totalSignals, got: {other}"
        ))),
    }
}

fn normalize_signal_source(raw: &str) -> Option<&'static str> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "live" => Some("live"),
        "backtest" => Some("backtest"),
        _ => None,
    }
}

fn load_contextual_prior_dnva(
    db: &Database,
    session_type: Option<&str>,
    session_segment: Option<&str>,
    trading_day: Option<&str>,
) -> (Option<(f64, f64, f64)>, Option<(f64, f64, f64)>) {
    let Some(td) = trading_day else {
        return (None, None);
    };

    if session_type == Some("Globex") {
        match session_segment {
            Some("London") => (
                db.load_prior_session_dnva("London", td).ok().flatten(),
                db.load_session_dnva(td, "Asia").ok().flatten(),
            ),
            _ => (
                db.load_prior_session_dnva("London", td).ok().flatten(),
                db.load_prior_session_dnva("Asia", td).ok().flatten(),
            ),
        }
    } else {
        (
            db.load_session_dnva(td, "London").ok().flatten(),
            db.load_session_dnva(td, "Asia").ok().flatten(),
        )
    }
}

fn historical_job_response(run: &HistoricalJobRun, already_running: bool) -> serde_json::Value {
    let mut progress = run.progress.clone();
    if let Some(progress_obj) = progress.as_object_mut() {
        let estimated_records = progress_obj
            .get("estimatedRecords")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let records_scanned = progress_obj
            .get("recordsScanned")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let remaining_records = estimated_records.saturating_sub(records_scanned);
        progress_obj.insert(
            "remainingRecords".to_string(),
            serde_json::json!(remaining_records),
        );
        let eta_rate = progress_obj
            .get("smoothedRecordsPerSecond")
            .and_then(|v| v.as_f64())
            .filter(|rate| *rate > 0.0)
            .or_else(|| {
                progress_obj
                    .get("recordsPerSecond")
                    .and_then(|v| v.as_f64())
                    .filter(|rate| *rate > 0.0)
            });
        let eta_ms = eta_rate
            .filter(|_| remaining_records > 0)
            .map(|rate| remaining_records as f64 / rate * 1000.0);
        let raw_eta_ms = progress_obj
            .get("recordsPerSecond")
            .and_then(|v| v.as_f64())
            .filter(|rate| *rate > 0.0 && remaining_records > 0)
            .map(|rate| remaining_records as f64 / rate * 1000.0);
        progress_obj.insert(
            "etaMs".to_string(),
            eta_ms
                .map(|value| serde_json::json!(value))
                .unwrap_or(serde_json::Value::Null),
        );
        progress_obj.insert(
            "rawEtaMs".to_string(),
            raw_eta_ms
                .map(|value| serde_json::json!(value))
                .unwrap_or(serde_json::Value::Null),
        );
    }
    serde_json::json!({
        "jobId": run.id,
        "jobType": run.job_type,
        "status": run.status,
        "alreadyRunning": already_running,
        "submittedAtMs": run.submitted_at_ms,
        "startedAtMs": run.started_at_ms,
        "finishedAtMs": run.finished_at_ms,
        "params": run.params,
        "currentPhase": progress.get("currentPhase").cloned().unwrap_or(serde_json::json!(null)),
        "progress": progress,
        "warnings": run.warnings,
        "error": run.error,
        "result": run.result,
    })
}

fn normalized_job_key(
    job_type: backfill::HistoricalJobType,
    params: &BackfillParams,
    force_run_rules: bool,
) -> String {
    let mut setup_ids = params.setup_ids.clone().unwrap_or_default();
    setup_ids.sort();
    serde_json::json!({
        "jobType": job_type.as_str(),
        "startDate": params.start_date,
        "endDate": params.end_date,
        "force": params.force.unwrap_or(false),
        "runRules": force_run_rules || params.run_rules.unwrap_or(false),
        "setupIds": if setup_ids.is_empty() { serde_json::Value::Null } else { serde_json::json!(setup_ids) },
    })
    .to_string()
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct LimitParams {
    /// Maximum number of items to return (default 25).
    limit: Option<u64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct TickQueryParams {
    /// Maximum number of ticks to return (default 200, max 2000). When a time range is set,
    /// results are returned in ascending chronological order; otherwise most-recent first.
    limit: Option<u64>,
    /// Start of time range as Unix epoch milliseconds (e.g. 1740092400000.0).
    /// Use get_market_snapshot to find the current timestamp, then subtract to target earlier times.
    start_time_ms: Option<f64>,
    /// End of time range as Unix epoch milliseconds.
    end_time_ms: Option<f64>,
    /// Filter to ticks at or above this price.
    price_low: Option<f64>,
    /// Filter to ticks at or below this price.
    price_high: Option<f64>,
    /// Filter to a specific trading session date in YYYY-MM-DD format (e.g. "2026-03-04").
    session_date: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct FootprintWindowParams {
    /// Start of time window as Unix epoch milliseconds. Required for meaningful output.
    start_time_ms: Option<f64>,
    /// End of time window as Unix epoch milliseconds. Required for meaningful output.
    end_time_ms: Option<f64>,
    /// Optional: only return levels at or above this price.
    price_low: Option<f64>,
    /// Optional: only return levels at or below this price.
    price_high: Option<f64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct FootprintParams {
    /// Optional: only return levels at or above this price. Filtering happens before the top-30 volume sort.
    price_low: Option<f64>,
    /// Optional: only return levels at or below this price. Filtering happens before the top-30 volume sort.
    price_high: Option<f64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct TpoDetailParams {
    /// Optional: only return levels at or above this price.
    price_low: Option<f64>,
    /// Optional: only return levels at or below this price.
    price_high: Option<f64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct SnapshotAtParams {
    /// Target time as Unix epoch milliseconds. Returns the stored pipeline snapshot
    /// closest to this timestamp. Snapshots are stored every ~30 seconds.
    timestamp_ms: Option<f64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct DomSnapshotAtParams {
    /// Target time as Unix epoch milliseconds for delayed DOM reconstruction.
    timestamp_ms: f64,
    /// Number of price levels to return on each side (default 10, max 25).
    levels_per_side: Option<u64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct PullStackParams {
    /// Inclusive start time as Unix epoch milliseconds.
    start_time_ms: f64,
    /// Exclusive end time as Unix epoch milliseconds.
    end_time_ms: f64,
    /// Optional lower bound to focus on a specific price zone.
    price_low: Option<f64>,
    /// Optional upper bound to focus on a specific price zone.
    price_high: Option<f64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct LiquidityBehaviorParams {
    /// Inclusive start time as Unix epoch milliseconds.
    start_time_ms: f64,
    /// Exclusive end time as Unix epoch milliseconds.
    end_time_ms: f64,
    /// Center price to inspect.
    price: f64,
    /// Radius around the target price in ticks (default 4, max 20).
    radius_ticks: Option<u64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct DomWindowParams {
    start_time_ms: Option<f64>,
    end_time_ms: Option<f64>,
    price_low: Option<f64>,
    price_high: Option<f64>,
    limit: Option<usize>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct DomTapeContextParams {
    timestamp_ms: f64,
    window_ms: Option<f64>,
    price_low: Option<f64>,
    price_high: Option<f64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ExplainBookReactionParams {
    timestamp_ms: Option<f64>,
    price: Option<f64>,
    start_time_ms: Option<f64>,
    end_time_ms: Option<f64>,
    radius_ticks: Option<u64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct DeltaConfirmParams {
    /// True for a buy/long setup, false for a sell/short setup.
    is_buy_setup: Option<bool>,
    /// Optional price level to check delta at. Defaults to current price.
    price: Option<f64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct DeltaAtPriceParams {
    /// Price level to query delta at. Omit for current price.
    price: Option<f64>,
    /// Number of top prices by absolute delta to return (default 10).
    top_n: Option<usize>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct SetupContextParams {
    /// Name of the setup template (e.g. "OR5 Mid Retest", "DNVA Retest").
    setup_name: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct ProximityParams {
    /// Maximum distance in ticks to include in the report (default 20).
    max_distance_ticks: Option<f64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct SaveAccountStateParams {
    /// Current account balance in dollars.
    last_balance_dollars: Option<f64>,
    /// Open positions not from chat: array of {direction, size, entryPrice, instrument?, setupId?}.
    open_positions: Option<Vec<OpenPositionInput>>,
    /// Lucid daily loss limit in dollars (e.g. 750).
    lucid_daily_loss_dollars: Option<f64>,
    /// Lucid account size in dollars (e.g. 50000).
    lucid_account_size_dollars: Option<f64>,
    /// Profit target per payout cycle (e.g. 2000).
    profit_target_per_cycle: Option<f64>,
    /// Position sizing method (default quarter_kelly).
    position_sizing_method: Option<String>,
    /// Kelly fraction (default 0.25).
    kelly_fraction: Option<f64>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
struct OpenPositionInput {
    direction: String,
    size: i64,
    entry_price: f64,
    instrument: Option<String>,
    setup_id: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct KellyPositionSizeParams {
    /// Setup ID for setup-specific stats. Omit for aggregate.
    setup_id: Option<String>,
    /// Current account balance in dollars (for sizing calc).
    balance_dollars: Option<f64>,
    /// Confidence multiplier: 0.5=low, 1.0=normal, 1.5=high.
    confidence_multiplier: Option<f64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct RecordTradeResultParams {
    /// Trade direction: "long" or "short".
    direction: String,
    /// Number of contracts.
    size: i64,
    /// Entry price.
    entry_price: f64,
    /// Exit price.
    exit_price: f64,
    /// Result in R-units (positive = win, negative = loss).
    result_r: f64,
    /// Optional setup ID for performance tracking.
    setup_id: Option<String>,
    /// Optional stop price used.
    stop_price: Option<f64>,
    /// Optional notes about the trade.
    notes: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct SaveRiskConfigParams {
    /// R-value in NQ points (e.g. 50).
    r_value_points: Option<f64>,
    /// R-value in dollars (e.g. 250 for MNQ).
    r_value_dollars: Option<f64>,
    /// Max daily loss in R-units before session stop (e.g. 3).
    max_daily_loss_r: Option<f64>,
    /// Max consecutive losses before circuit breaker (e.g. 3).
    max_consecutive_losses: Option<i64>,
    /// Max trades per session (e.g. 8).
    max_trades_per_session: Option<i64>,
    /// Max daily loss in dollars (e.g. 750). Used with Lucid params.
    max_daily_loss_dollars: Option<f64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct BackfillParams {
    /// Start date (YYYY-MM-DD). Omit for "all available".
    #[serde(alias = "start_date")]
    start_date: Option<String>,
    /// End date (YYYY-MM-DD). Omit for "through today".
    #[serde(alias = "end_date")]
    end_date: Option<String>,
    /// Reprocess sessions even if summaries already exist.
    #[serde(alias = "force")]
    force: Option<bool>,
    /// Run rules engine during backfill to populate signal outcomes (backtest replay).
    #[serde(alias = "run_rules")]
    run_rules: Option<bool>,
    /// Setup IDs to evaluate. Omit for all active setups.
    #[serde(alias = "setup_ids")]
    setup_ids: Option<Vec<String>>,
    /// Wait for the background job to complete before responding.
    #[serde(alias = "wait_for_completion")]
    wait_for_completion: Option<bool>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct BackfillStatusParams {
    #[serde(alias = "job_id")]
    job_id: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct CancelBackfillParams {
    #[serde(alias = "job_id")]
    job_id: String,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct FrequencyParams {
    /// Event type to query (e.g. "ib_mid_test", "new_session_high").
    event_type: String,
    /// Start date filter (YYYY-MM-DD).
    start_date: Option<String>,
    /// End date filter (YYYY-MM-DD).
    end_date: Option<String>,
    /// Optional session/trading-day scope filter.
    #[serde(flatten)]
    session_scope: SessionScopeParams,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct ConditionalParams {
    /// Event type for the condition (e.g. "ib_mid_test").
    event_type: String,
    /// Minimum event count per session to satisfy the condition.
    min_count: Option<i64>,
    /// Session summary field to check (e.g. "close_vs_ib_mid", "close_vs_vwap", "day_type").
    outcome_field: String,
    /// Value to match (e.g. "above", "below", "Trend").
    outcome_value: String,
    /// Start date filter (YYYY-MM-DD).
    start_date: Option<String>,
    /// End date filter (YYYY-MM-DD).
    end_date: Option<String>,
    /// Optional session/trading-day scope filter.
    #[serde(flatten)]
    session_scope: SessionScopeParams,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct DistributionParams {
    /// Metric column from session_summaries (e.g. "ib_range", "session_delta", "total_volume").
    metric: String,
    /// Start date filter (YYYY-MM-DD).
    start_date: Option<String>,
    /// End date filter (YYYY-MM-DD).
    end_date: Option<String>,
    /// Optional session/trading-day scope filter.
    #[serde(flatten)]
    session_scope: SessionScopeParams,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct SignalOutcomeDistributionParams {
    /// Setup ID to analyze (e.g. "or5-mid-retest").
    setup_id: String,
    /// Start date filter (YYYY-MM-DD).
    start_date: Option<String>,
    /// End date filter (YYYY-MM-DD).
    end_date: Option<String>,
    /// Optional session/trading-day scope filter.
    #[serde(flatten)]
    session_scope: SessionScopeParams,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct SignalOutcomeConditionalParams {
    /// Setup ID to analyze.
    setup_id: String,
    /// Session summary field to filter by (e.g. "day_type", "profile_shape", "balance_state").
    session_field: String,
    /// Value to match (e.g. "Trend", "Normal", "above").
    field_value: String,
    /// Start date filter (YYYY-MM-DD).
    start_date: Option<String>,
    /// End date filter (YYYY-MM-DD).
    end_date: Option<String>,
    /// Optional session/trading-day scope filter.
    #[serde(flatten)]
    session_scope: SessionScopeParams,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct SignalOutcomeExcursionsParams {
    /// Setup ID to analyze. Omit for combined outcomes across setups.
    #[serde(alias = "setup_id")]
    setup_id: Option<String>,
    /// Start date filter (YYYY-MM-DD).
    #[serde(alias = "start_date")]
    start_date: Option<String>,
    /// End date filter (YYYY-MM-DD).
    #[serde(alias = "end_date")]
    end_date: Option<String>,
    /// Optional session/trading-day scope filter.
    #[serde(flatten)]
    session_scope: SessionScopeParams,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct SessionHistoryParams {
    /// Start date filter (YYYY-MM-DD).
    start_date: Option<String>,
    /// End date filter (YYYY-MM-DD).
    end_date: Option<String>,
    /// Filter by day type (e.g. "Trend", "Normal").
    day_type: Option<String>,
    /// Maximum number of sessions to return (default 20).
    limit: Option<u64>,
    /// Optional session/trading-day scope filter.
    #[serde(flatten)]
    session_scope: SessionScopeParams,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct SetupPerformanceMatrixParams {
    /// Start date filter (YYYY-MM-DD).
    #[serde(alias = "start_date")]
    start_date: Option<String>,
    /// End date filter (YYYY-MM-DD).
    #[serde(alias = "end_date")]
    end_date: Option<String>,
    /// Minimum resolved outcomes required for inclusion (default 0).
    #[serde(alias = "min_resolved")]
    min_resolved: Option<i64>,
    /// Sort key: winRate | avgR | resolved | totalSignals (default resolved).
    #[serde(alias = "sort_by")]
    sort_by: Option<String>,
    /// Maximum number of setup rows to return (default 50).
    limit: Option<u64>,
    /// Optional session/trading-day scope filter.
    #[serde(flatten)]
    session_scope: SessionScopeParams,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct CompareBacktestsParams {
    /// Backtest run IDs to compare.
    run_ids: Vec<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct CompareSessionsParams {
    /// Current IB range for similarity matching.
    current_ib_range: Option<f64>,
    /// Current day type for filtering.
    current_day_type: Option<String>,
    /// Profile shape (e.g. "Normal", "Trend", "DoubleDistribution").
    profile_shape: Option<String>,
    /// Balance state (e.g. "Balanced", "Building", "Clearing").
    balance_state: Option<String>,
    /// Current RVOL ratio for similarity.
    rvol_ratio: Option<f64>,
    /// Session delta sign: "positive", "negative", or "neutral".
    session_delta_sign: Option<String>,
    /// Single prints direction for similarity.
    single_prints_direction: Option<String>,
    /// Max similar sessions to return (default 5).
    max_results: Option<u64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct SignalPerformanceParams {
    /// Setup ID to filter by.
    setup_id: Option<String>,
    /// Start date filter (YYYY-MM-DD).
    start_date: Option<String>,
    /// End date filter (YYYY-MM-DD).
    end_date: Option<String>,
    /// Optional source filter: "live" or "backtest".
    source: Option<String>,
    /// Optional backtest job ID filter.
    #[serde(alias = "job_id", alias = "jobId")]
    job_id: Option<String>,
    /// Optional session/trading-day scope filter.
    #[serde(flatten)]
    session_scope: SessionScopeParams,
}

#[tool_router]
impl TheDeskMcp {
    fn new(db: Database, pipelines: PipelineEngine, db_path: String) -> Self {
        Self {
            db: Arc::new(Mutex::new(db)),
            db_path: Arc::new(db_path),
            pipelines: Arc::new(Mutex::new(pipelines)),
            detector: Arc::new(Mutex::new(EventDetector::new())),
            flow_emitter: Arc::new(Mutex::new(FlowEventEmitter::new())),
            rules: Arc::new(Mutex::new(RulesEngine::default())),
            last_bid: Arc::new(Mutex::new(0.0)),
            last_ask: Arc::new(Mutex::new(0.0)),
            backfill_manager: Arc::new(AsyncMutex::new(BackfillManager::default())),
            tool_router: Self::tool_router(),
        }
    }

    async fn wait_for_job_terminal(&self, job_id: &str) -> Option<HistoricalJobRun> {
        loop {
            let maybe_run = {
                let manager = self.backfill_manager.lock().await;
                manager.jobs.get(job_id).map(|state| state.run.clone())
            };
            if let Some(run) = maybe_run {
                if matches!(run.status.as_str(), "completed" | "failed" | "cancelled") {
                    return Some(run);
                }
            } else {
                return self
                    .db
                    .lock()
                    .ok()
                    .and_then(|db| db.get_historical_job_run(job_id).ok().flatten());
            }
            sleep(Duration::from_millis(100)).await;
        }
    }

    async fn get_job_run(
        &self,
        job_id: Option<&str>,
    ) -> Result<Option<HistoricalJobRun>, McpError> {
        let manager = self.backfill_manager.lock().await;
        if let Some(job_id) = job_id {
            if let Some(state) = manager.jobs.get(job_id) {
                return Ok(Some(state.run.clone()));
            }
        } else if let Some(active_id) = &manager.active_job_id {
            if let Some(state) = manager.jobs.get(active_id) {
                return Ok(Some(state.run.clone()));
            }
        } else if let Some(last_id) = &manager.last_job_id {
            if let Some(state) = manager.jobs.get(last_id) {
                return Ok(Some(state.run.clone()));
            }
        }
        drop(manager);

        let db = self.db.lock().map_err(|_| lock_error())?;
        if let Some(job_id) = job_id {
            db.get_historical_job_run(job_id).map_err(db_error)
        } else {
            db.latest_historical_job_run().map_err(db_error)
        }
    }

    async fn queue_historical_job(
        &self,
        params: BackfillParams,
        job_type: backfill::HistoricalJobType,
        force_run_rules: bool,
    ) -> Result<(HistoricalJobRun, bool), McpError> {
        let (start_ms, end_ms_exclusive) = backfill::parse_backfill_date_range(
            params.start_date.as_deref(),
            params.end_date.as_deref(),
        )
        .map_err(|e| invalid_params_error(e.to_string()))?;
        let initial_estimated_records = {
            let config = load_feed_config();
            let reader = ScidReader::from_feed_config(&config);
            reader
                .estimate_range_records(start_ms, end_ms_exclusive)
                .unwrap_or(0)
        };

        let request_key = normalized_job_key(job_type, &params, force_run_rules);
        let submitted_at_ms = chrono::Utc::now().timestamp_millis() as f64;
        let run_rules = force_run_rules || params.run_rules.unwrap_or(false);
        let params_json = serde_json::json!({
            "startDate": params.start_date,
            "endDate": params.end_date,
            "force": params.force.unwrap_or(false),
            "runRules": run_rules,
            "setupIds": params.setup_ids,
        });

        let mut manager = self.backfill_manager.lock().await;
        if let Some(active_id) = &manager.active_job_id {
            if let Some(active) = manager.jobs.get(active_id) {
                if active.request_key == request_key {
                    return Ok((active.run.clone(), true));
                }
                return Err(McpError::new(
                    ErrorCode::INTERNAL_ERROR,
                    format!("historical job already running: {}", active.run.id),
                    None,
                ));
            }
        }

        let job_id = uuid::Uuid::new_v4().to_string();
        let run = HistoricalJobRun {
            id: job_id.clone(),
            job_type: job_type.as_str().to_string(),
            status: "queued".to_string(),
            params: params_json.clone(),
            progress: serde_json::json!({
                "estimatedRecords": initial_estimated_records,
                "recordsScanned": 0,
                "sessionsCompleted": 0,
                "sessionsSkipped": 0,
                "currentSessionDate": serde_json::Value::Null,
                "currentPhase": "queued",
                "progressPercent": if initial_estimated_records > 0 { serde_json::json!(0.0) } else { serde_json::Value::Null },
                "elapsedMs": 0.0,
                "recordsPerSecond": 0.0,
                "smoothedRecordsPerSecond": 0.0,
            }),
            result: None,
            warnings: Vec::new(),
            error: None,
            submitted_at_ms,
            started_at_ms: None,
            finished_at_ms: None,
        };
        {
            let db = self.db.lock().map_err(|_| lock_error())?;
            db.insert_historical_job_run(&run).map_err(db_error)?;
        }

        let cancel_flag = Arc::new(AtomicBool::new(false));
        manager.active_job_id = Some(job_id.clone());
        manager.last_job_id = Some(job_id.clone());
        manager.jobs.insert(
            job_id.clone(),
            InMemoryJobState {
                run: run.clone(),
                request_key,
                cancel_flag: Arc::clone(&cancel_flag),
            },
        );
        drop(manager);

        let db_path = Arc::clone(&self.db_path);
        let manager = Arc::clone(&self.backfill_manager);
        let worker_params = backfill::BackfillJobParams {
            job_id: job_id.clone(),
            job_type,
            start_date: params.start_date,
            end_date: params.end_date,
            force: params.force.unwrap_or(false),
            run_rules,
            setup_ids: params.setup_ids,
        };
        tokio::task::spawn_blocking(move || {
            let config = load_feed_config();
            let reader = ScidReader::from_feed_config(&config);
            let db = match Database::open(db_path.as_str()) {
                Ok(db) => db,
                Err(err) => {
                    let mut guard = manager.blocking_lock();
                    if let Some(state) = guard.jobs.get_mut(&job_id) {
                        state.run.status = "failed".to_string();
                        state.run.error = Some(err.to_string());
                        state.run.finished_at_ms =
                            Some(chrono::Utc::now().timestamp_millis() as f64);
                        guard.active_job_id = None;
                    }
                    return;
                }
            };

            let started_at_ms = chrono::Utc::now().timestamp_millis() as f64;
            {
                let mut guard = manager.blocking_lock();
                if let Some(state) = guard.jobs.get_mut(&job_id) {
                    state.run.status = "running".to_string();
                    state.run.started_at_ms = Some(started_at_ms);
                    state.run.progress["currentPhase"] = serde_json::json!("scanning");
                    let _ = db.update_historical_job_run(
                        &job_id,
                        &the_desk_backend::db::HistoricalJobRunUpdate {
                            status: "running",
                            progress: &state.run.progress,
                            result: None,
                            warnings: &state.run.warnings,
                            error: None,
                            started_at_ms: Some(started_at_ms),
                            finished_at_ms: None,
                        },
                    );
                }
            }

            eprintln!(
                "[the-desk-mcp] historical job {} started ({})",
                job_id,
                worker_params.job_type.as_str()
            );
            let mut last_progress_db_write_ms = started_at_ms;
            let mut last_persisted_records = 0_usize;
            let mut last_persisted_sessions_completed = 0_usize;
            let mut last_persisted_sessions_skipped = 0_usize;
            let mut last_persisted_phase = String::from("scanning");
            let mut last_persisted_session_date: Option<String> = None;
            let mut smoothed_records_per_second = 0.0_f64;
            let result = backfill::run_backfill_job(
                &reader,
                &db,
                &worker_params,
                |progress| {
                    let now_ms = chrono::Utc::now().timestamp_millis() as f64;
                    let elapsed_ms = (now_ms - started_at_ms).max(0.0);
                    let records_per_second = if elapsed_ms > 0.0 {
                        progress.records_scanned as f64 / (elapsed_ms / 1000.0)
                    } else {
                        0.0
                    };
                    if records_per_second > 0.0 {
                        smoothed_records_per_second = if smoothed_records_per_second <= 0.0 {
                            records_per_second
                        } else {
                            (JOB_PROGRESS_RATE_EMA_ALPHA * records_per_second)
                                + ((1.0 - JOB_PROGRESS_RATE_EMA_ALPHA)
                                    * smoothed_records_per_second)
                        };
                    }
                    let progress_percent = if progress.estimated_records > 0 {
                        Some(
                            ((progress.records_scanned as f64 / progress.estimated_records as f64)
                                * 100.0)
                                .clamp(0.0, 100.0),
                        )
                    } else {
                        None
                    };
                    let mut guard = manager.blocking_lock();
                    if let Some(state) = guard.jobs.get_mut(&job_id) {
                        state.run.progress = serde_json::json!({
                            "estimatedRecords": progress.estimated_records,
                            "recordsScanned": progress.records_scanned,
                            "sessionsCompleted": progress.sessions_completed,
                            "sessionsSkipped": progress.sessions_skipped,
                            "currentSessionDate": progress.current_session_date,
                            "currentPhase": progress.current_phase,
                            "progressPercent": progress_percent,
                            "elapsedMs": elapsed_ms,
                            "recordsPerSecond": records_per_second,
                            "smoothedRecordsPerSecond": smoothed_records_per_second,
                        });
                        let should_persist = progress.current_phase != last_persisted_phase
                            || progress.current_session_date != last_persisted_session_date
                            || progress.sessions_completed != last_persisted_sessions_completed
                            || progress.sessions_skipped != last_persisted_sessions_skipped
                            || progress
                                .records_scanned
                                .saturating_sub(last_persisted_records)
                                >= JOB_PROGRESS_RECORD_STEP
                            || (now_ms - last_progress_db_write_ms)
                                >= JOB_PROGRESS_PERSIST_INTERVAL_MS;
                        if should_persist {
                            let _ = db.update_historical_job_run(
                                &job_id,
                                &the_desk_backend::db::HistoricalJobRunUpdate {
                                    status: &state.run.status,
                                    progress: &state.run.progress,
                                    result: state.run.result.as_ref(),
                                    warnings: &state.run.warnings,
                                    error: state.run.error.as_deref(),
                                    started_at_ms: state.run.started_at_ms,
                                    finished_at_ms: state.run.finished_at_ms,
                                },
                            );
                            last_progress_db_write_ms = now_ms;
                            last_persisted_records = progress.records_scanned;
                            last_persisted_sessions_completed = progress.sessions_completed;
                            last_persisted_sessions_skipped = progress.sessions_skipped;
                            last_persisted_phase = progress.current_phase.clone();
                            last_persisted_session_date = progress.current_session_date.clone();
                        }
                    }
                },
                cancel_flag.as_ref(),
            );

            let finished_at_ms = chrono::Utc::now().timestamp_millis() as f64;
            let mut guard = manager.blocking_lock();
            if let Some(state) = guard.jobs.get_mut(&job_id) {
                match result {
                    Ok(result) => {
                        state.run.status = "completed".to_string();
                        state.run.result = Some(serde_json::to_value(&result).unwrap_or_default());
                        state.run.warnings = result.warnings.clone();
                        state.run.error = None;
                    }
                    Err(backfill::BackfillJobError::Cancelled) => {
                        state.run.status = "cancelled".to_string();
                        state.run.error = None;
                    }
                    Err(err) => {
                        state.run.status = "failed".to_string();
                        state.run.error = Some(err.to_string());
                    }
                }
                state.run.finished_at_ms = Some(finished_at_ms);
                let _ = db.update_historical_job_run(
                    &job_id,
                    &the_desk_backend::db::HistoricalJobRunUpdate {
                        status: &state.run.status,
                        progress: &state.run.progress,
                        result: state.run.result.as_ref(),
                        warnings: &state.run.warnings,
                        error: state.run.error.as_deref(),
                        started_at_ms: state.run.started_at_ms,
                        finished_at_ms: state.run.finished_at_ms,
                    },
                );
            }
            guard.active_job_id = None;
        });

        Ok((run, false))
    }

    /// Get a live snapshot from the in-memory pipeline engine.
    /// Uses try_lock to avoid blocking when backfill/poll holds the lock.
    fn live_snapshot(&self) -> Option<serde_json::Value> {
        let pipelines = self.pipelines.try_lock().ok()?;
        let bid = *self.last_bid.lock().ok()?;
        let ask = *self.last_ask.lock().ok()?;
        if bid <= 0.0 && ask <= 0.0 {
            return None;
        }
        let snapshot = pipelines.snapshot(bid, ask);
        serde_json::to_value(&snapshot).ok()
    }

    #[tool(
        description = "Current market snapshot: last price, VWAP with 1/2/3 SD bands, TPO value area (high/low/POC), delta neutral value area (DNVA high/low/DNP), session delta, cumulative delta, key levels (prior day H/L/C, prior VA/POC, overnight range, OR, IB), Globex/London opening ranges, and session context (sessionType, sessionSegment, tradingDay), plus tape pace, imbalance count, absorption event count, and average trade size. Prefers live pipeline state; falls back to last persisted snapshot."
    )]
    async fn get_market_snapshot(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let dom_feature = db.latest_dom_feature_state().ok().flatten();
        if let Some(snapshot) = self.live_snapshot() {
            return Ok(text_result(serde_json::json!({
                "snapshot": snapshot,
                "domSummary": dom_feature.as_ref().and_then(|(_, p)| p.get("domSummary")).cloned(),
                "source": "live_pipeline"
            })));
        }
        match db.latest_feature_state() {
            Ok(Some(snapshot)) => Ok(text_result(serde_json::json!({
                "snapshot": snapshot,
                "domSummary": dom_feature.as_ref().and_then(|(_, p)| p.get("domSummary")).cloned(),
                "dataAgeMs": compute_data_age(&db),
                "source": "database"
            }))),
            Ok(None) => Ok(no_data(
                "No market snapshot available yet. Ensure Sierra Chart is running and .scid data is being ingested.",
            )),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Current session context: sessionType (RTH/Globex/Unknown), sessionSegment (Asia/London/None), tradingDay (6 PM ET roll), and data freshness."
    )]
    async fn get_session_context(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let now_ms = chrono::Utc::now().timestamp_millis() as f64;
        let context_ts = db
            .latest_tick_timestamp_ms()
            .ok()
            .flatten()
            .unwrap_or(now_ms);
        let et_minutes = et_minutes_from_timestamp(context_ts).unwrap_or(-1);
        let (is_transition, transition_from, transition_to, transition_phase) =
            if let Some((from, to, phase)) = transition_hint(et_minutes) {
                (
                    true,
                    serde_json::json!(from),
                    serde_json::json!(to),
                    serde_json::json!(phase),
                )
            } else {
                (
                    false,
                    serde_json::Value::Null,
                    serde_json::Value::Null,
                    serde_json::Value::Null,
                )
            };
        match db.latest_feature_state() {
            Ok(Some(s)) => Ok(text_result(serde_json::json!({
                "sessionType": s.get("sessionType"),
                "sessionSegment": s.get("sessionSegment"),
                "tradingDay": s.get("tradingDay"),
                "isTransition": is_transition,
                "transitionFrom": transition_from,
                "transitionTo": transition_to,
                "transitionPhase": transition_phase,
                "etMinutes": et_minutes,
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data("No session context available")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "TPO (Time-Price-Opportunity) profile data: POC (point of control), value area high/low, opening range high/low (first 30 min), initial balance high/low (first 60 min). Use for auction market theory analysis."
    )]
    async fn get_tpo_profile(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        match db.latest_feature_state() {
            Ok(Some(s)) => Ok(text_result(serde_json::json!({
                "poc": s.get("poc"),
                "vaHigh": s.get("vaHigh"),
                "vaLow": s.get("vaLow"),
                "orHigh": s.get("orHigh"),
                "orLow": s.get("orLow"),
                "ibHigh": s.get("ibHigh"),
                "ibLow": s.get("ibLow"),
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data("No TPO data available")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Delta profile: segment delta (Asia-only, London-only, or RTH-only), combined Globex delta (Asia+London when in Globex), cumulative delta, DNVA high/low, DNP. Use for inventory and positioning analysis."
    )]
    async fn get_delta_profile(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        match db.latest_feature_state() {
            Ok(Some(s)) => Ok(text_result(serde_json::json!({
                "sessionDelta": s.get("sessionDelta"),
                "globexDelta": s.get("globexDelta"),
                "cumulativeDelta": s.get("cumulativeDelta"),
                "dnvaHigh": s.get("dnvaHigh"),
                "dnvaLow": s.get("dnvaLow"),
                "dnp": s.get("dnp"),
                "sessionSegment": s.get("sessionSegment"),
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data("No delta data available")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Key reference levels: prior day high/low/close, prior session value area high/low and POC, overnight (Globex) high/low, Globex OR30 and London OR60, and initial balance high/low. Includes sessionType (RTH vs Globex), sessionSegment (Asia/London/None), and tradingDay."
    )]
    async fn get_key_levels(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        match db.latest_feature_state() {
            Ok(Some(s)) => {
                let session_type = s.get("sessionType").and_then(|v| v.as_str());
                let session_segment = s.get("sessionSegment").and_then(|v| v.as_str());
                let trading_day = s.get("tradingDay").and_then(|v| v.as_str());
                let is_globex = session_type == Some("Globex");
                let mut out = serde_json::json!({
                    "sessionType": s.get("sessionType"),
                    "sessionSegment": s.get("sessionSegment"),
                    "tradingDay": s.get("tradingDay"),
                    "priorDayHigh": s.get("priorDayHigh"),
                    "priorDayLow": s.get("priorDayLow"),
                    "priorDayClose": s.get("priorDayClose"),
                    "priorVaHigh": s.get("priorVaHigh"),
                    "priorVaLow": s.get("priorVaLow"),
                    "priorPoc": s.get("priorPoc"),
                    "priorDnvaHigh": s.get("priorDnvaHigh"),
                    "priorDnvaLow": s.get("priorDnvaLow"),
                    "priorDnp": s.get("priorDnp"),
                    "overnightHigh": s.get("overnightHigh"),
                    "overnightLow": s.get("overnightLow"),
                    "globexOr30High": s.get("globexOr30High"),
                    "globexOr30Low": s.get("globexOr30Low"),
                    "londonOr60High": s.get("londonOr60High"),
                    "londonOr60Low": s.get("londonOr60Low"),
                    "sessionHigh": s.get("sessionHigh"),
                    "sessionLow": s.get("sessionLow"),
                    "ibHigh": s.get("ibHigh"),
                    "ibLow": s.get("ibLow"),
                    "priorLondonDnvaHigh": serde_json::Value::Null,
                    "priorLondonDnvaLow": serde_json::Value::Null,
                    "priorLondonDnp": serde_json::Value::Null,
                    "priorAsiaDnvaHigh": serde_json::Value::Null,
                    "priorAsiaDnvaLow": serde_json::Value::Null,
                    "priorAsiaDnp": serde_json::Value::Null,
                    "untestedDnps": serde_json::json!([]),
                    "dataAgeMs": compute_data_age(&db)
                });
                if is_globex {
                    out["sessionScopeNote"] = serde_json::json!("For Globex, use overnightHigh/overnightLow as the session range. sessionHigh, sessionLow, IB, OR, and OR5 are RTH-only and may be zero or from a prior RTH session.");
                }
                let (london_dnva, asia_dnva) =
                    load_contextual_prior_dnva(&db, session_type, session_segment, trading_day);
                if let Some((h, l, p)) = london_dnva {
                    out["priorLondonDnvaHigh"] = serde_json::json!(h);
                    out["priorLondonDnvaLow"] = serde_json::json!(l);
                    out["priorLondonDnp"] = serde_json::json!(p);
                }
                if let Some((h, l, p)) = asia_dnva {
                    out["priorAsiaDnvaHigh"] = serde_json::json!(h);
                    out["priorAsiaDnvaLow"] = serde_json::json!(l);
                    out["priorAsiaDnp"] = serde_json::json!(p);
                }
                // Include untested DNPs from prior sessions (price never revisited DNP).
                if let Ok(untested) = db.load_untested_dnps(10) {
                    let list: Vec<serde_json::Value> = untested
                        .into_iter()
                        .map(|(sd, st, dnp)| {
                            serde_json::json!({
                                "sessionDate": sd,
                                "sessionType": st,
                                "dnp": dnp
                            })
                        })
                        .collect();
                    out["untestedDnps"] = serde_json::json!(list);
                }
                Ok(text_result(out))
            }
            Ok(None) => Ok(no_data("No key levels available")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Tape pace analytics: rolling ticks/sec and volume/sec over 5-second, 30-second, and 5-minute windows. Includes tape acceleration (rate of change), pace percentile (current vs session distribution), and dwell time at current price. Use to gauge market activity intensity and participation quality."
    )]
    async fn get_tape_pace(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        // Try live pipeline first for full snapshot including volume/sec and dwell.
        // Use try_lock to avoid blocking when backfill/poll holds the lock.
        if let Ok(pipelines) = self.pipelines.try_lock() {
            let now_ms = chrono::Utc::now().timestamp_millis() as f64;
            let snap = pipelines.tape_pace.snapshot(now_ms);
            let last_price = pipelines.levels.last_price;
            let dwell = if last_price > 0.0 {
                pipelines.tape_pace.dwell_at_price(last_price)
            } else {
                0.0
            };
            return Ok(text_result(serde_json::json!({
                "ticksPerSec5s": snap.ticks_per_sec_5s,
                "ticksPerSec30s": snap.ticks_per_sec_30s,
                "ticksPerSec5m": snap.ticks_per_sec_5m,
                "volumePerSec5s": snap.volume_per_sec_5s,
                "volumePerSec30s": snap.volume_per_sec_30s,
                "volumePerSec5m": snap.volume_per_sec_5m,
                "acceleration": snap.acceleration,
                "pacePercentile": snap.pace_percentile,
                "dwellAtCurrentPriceMs": dwell,
                "currentPrice": last_price,
                "dataAgeMs": compute_data_age(&db)
            })));
        }
        // Fallback to DB
        match db.latest_feature_state() {
            Ok(Some(s)) => Ok(text_result(serde_json::json!({
                "ticksPerSec5s": s.get("tapePace5s"),
                "ticksPerSec30s": s.get("tapePace30s"),
                "ticksPerSec5m": s.get("tapePace5m"),
                "acceleration": s.get("tapeAcceleration"),
                "pacePercentile": s.get("pacePercentile"),
                "note": "Falling back to DB snapshot. Volume/sec and dwell not available.",
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data("No tape pace data")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Footprint / volume-at-price data for the current session: top price levels by total volume with bid volume, ask volume, delta, and delta-per-volume ratio. Use price_low/price_high to focus on a specific price zone (e.g. near a key level). For a time-windowed footprint showing what happened at a specific time, use get_footprint_window instead."
    )]
    async fn get_footprint(
        &self,
        Parameters(params): Parameters<FootprintParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        if let Ok(pipelines) = self.pipelines.try_lock() {
            let mut all_levels = pipelines.footprint.levels();
            // Apply optional price range filter before sorting/truncating.
            if params.price_low.is_some() || params.price_high.is_some() {
                all_levels.retain(|(price, _)| {
                    if let Some(lo) = params.price_low {
                        if *price < lo {
                            return false;
                        }
                    }
                    if let Some(hi) = params.price_high {
                        if *price > hi {
                            return false;
                        }
                    }
                    true
                });
            }
            // Sort by total volume descending, return top 30.
            all_levels.sort_by(|a, b| {
                b.1.total()
                    .partial_cmp(&a.1.total())
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            let top: Vec<serde_json::Value> = all_levels
                .iter()
                .take(30)
                .map(|(price, lvl)| {
                    serde_json::json!({
                        "price": price,
                        "bidVolume": lvl.bid_volume,
                        "askVolume": lvl.ask_volume,
                        "totalVolume": lvl.total(),
                        "delta": lvl.delta(),
                        "deltaPerVolume": lvl.delta_per_volume(),
                        "imbalanceRatio": lvl.imbalance_ratio(),
                    })
                })
                .collect();
            return Ok(text_result(serde_json::json!({
                "topLevelsByVolume": top,
                "totalPriceLevels": all_levels.len(),
                "priceFilter": { "low": params.price_low, "high": params.price_high },
                "dataAgeMs": compute_data_age(&db)
            })));
        }
        match db.latest_microstructure_snapshot() {
            Ok(Some(s)) => Ok(text_result(serde_json::json!({
                "snapshot": s,
                "note": "Falling back to DB snapshot. Per-level detail not available.",
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data("No footprint data available")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Time-windowed footprint: bid/ask volume at each price level traded between start_time_ms and end_time_ms. Ideal for reconstructing what happened at a specific price during a specific time window — e.g. 'show me the footprint at the overnight low between 20:00 and 20:10'. Results are sorted by price ascending. Use get_market_snapshot to find current timestamp_ms, then subtract milliseconds to target earlier windows. Optionally narrow the price range with price_low/price_high."
    )]
    async fn get_footprint_window(
        &self,
        Parameters(params): Parameters<FootprintWindowParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        if let Ok(pipelines) = self.pipelines.try_lock() {
            let start = params.start_time_ms.unwrap_or(0.0);
            let end = params.end_time_ms.unwrap_or(f64::MAX);
            let mut levels = pipelines.footprint.levels_in_window(start, end);
            // Apply optional price range filter.
            if params.price_low.is_some() || params.price_high.is_some() {
                levels.retain(|(price, _)| {
                    if let Some(lo) = params.price_low {
                        if *price < lo {
                            return false;
                        }
                    }
                    if let Some(hi) = params.price_high {
                        if *price > hi {
                            return false;
                        }
                    }
                    true
                });
            }
            let total_volume: f64 = levels.iter().map(|(_, l)| l.total()).sum();
            let net_delta: f64 = levels.iter().map(|(_, l)| l.delta()).sum();
            let level_count = levels.len();
            let level_data: Vec<serde_json::Value> = levels
                .iter()
                .map(|(price, lvl)| {
                    serde_json::json!({
                        "price": price,
                        "bidVolume": lvl.bid_volume,
                        "askVolume": lvl.ask_volume,
                        "totalVolume": lvl.total(),
                        "delta": lvl.delta(),
                        "deltaPerVolume": lvl.delta_per_volume(),
                        "imbalanceRatio": lvl.imbalance_ratio(),
                    })
                })
                .collect();
            return Ok(text_result(serde_json::json!({
                "levels": level_data,
                "levelCount": level_count,
                "windowStartMs": start,
                "windowEndMs": if end == f64::MAX { serde_json::Value::Null } else { serde_json::json!(end) },
                "priceFilter": { "low": params.price_low, "high": params.price_high },
                "summary": {
                    "totalVolume": total_volume,
                    "netDelta": net_delta,
                },
                "note": "In-memory current session only. For historical sessions, use query_ticks with time and price filters.",
                "dataAgeMs": compute_data_age(&db)
            })));
        }
        Err(McpError::internal_error("Pipeline lock unavailable", None))
    }

    #[tool(
        description = "Per-price TPO letter detail for the current session: shows which 30-minute brackets (A, B, C, …) printed at each price level. Bracket A = first 30 min (Opening Range), B = 30-60 min (completes IB), C onwards = regular session. Single-print levels (is_single_print: true) are tail/excess candidates. Use price_low/price_high to focus on a specific price zone."
    )]
    async fn get_tpo_detail(
        &self,
        Parameters(params): Parameters<TpoDetailParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        if let Ok(pipelines) = self.pipelines.try_lock() {
            let detail = pipelines
                .tpo
                .tpo_letter_detail(params.price_low, params.price_high);
            let single_print_prices: Vec<f64> = detail
                .iter()
                .filter(|d| d.is_single_print)
                .map(|d| d.price)
                .collect();
            let level_count = detail.len();
            let single_count = single_print_prices.len();
            return Ok(text_result(serde_json::json!({
                "levels": detail,
                "levelCount": level_count,
                "singlePrintCount": single_count,
                "singlePrintPrices": single_print_prices,
                "priceFilter": { "low": params.price_low, "high": params.price_high },
                "note": "In-memory current session only. Brackets: A=0 (OR), B=1 (completes IB), C=2, D=3, ...",
                "dataAgeMs": compute_data_age(&db)
            })));
        }
        Err(McpError::internal_error("Pipeline lock unavailable", None))
    }

    #[tool(
        description = "Historical pipeline snapshot nearest to a given timestamp. Pipeline state (VWAP, POC, VA, delta, day type, etc.) is stored every ~30 seconds. Use this to answer 'what was the market structure at 20:00?' — pass that time as epoch milliseconds. The response includes the actual snapshot timestamp so you can see how close the match is. Use get_market_snapshot to get the current timestamp_ms and work backward."
    )]
    async fn get_snapshot_at(
        &self,
        Parameters(params): Parameters<SnapshotAtParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let target_ms = params
            .timestamp_ms
            .unwrap_or_else(|| db.latest_tick_timestamp_ms().ok().flatten().unwrap_or(0.0));
        match db.get_snapshot_near(target_ms) {
            Ok(Some((snapshot_ts, payload))) => Ok(text_result(serde_json::json!({
                "snapshot": payload,
                "snapshotTimestampMs": snapshot_ts,
                "requestedTimestampMs": target_ms,
                "offsetMs": snapshot_ts - target_ms,
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data("No pipeline snapshots found. Snapshots are stored every ~30s once data is flowing.")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Delayed DOM snapshot reconstructed from Sierra `.depth` history at or immediately before a timestamp. Returns best bid/ask, spread, touch imbalance, and the top resting levels on each side. Use this when you want the ladder view, not just executed tape. Note: Sierra depth data has ~1 second polling lag, so this is a delayed reconstruction, not real-time."
    )]
    async fn get_dom_snapshot_at(
        &self,
        Parameters(params): Parameters<DomSnapshotAtParams>,
    ) -> Result<CallToolResult, McpError> {
        let levels_per_side = params.levels_per_side.unwrap_or(10).clamp(1, 25) as usize;
        let timestamp_ms = params.timestamp_ms;
        let snapshot = tokio::task::spawn_blocking(move || {
            let reader = depth_reader_for_timestamp(timestamp_ms)?;
            reader
                .snapshot_at(timestamp_ms, levels_per_side)
                .map_err(db_error)
        })
        .await
        .map_err(|e| db_error(format!("DOM snapshot task failed: {e}")))??;

        Ok(text_result(serde_json::json!({
            "snapshot": snapshot,
            "requestedTimestampMs": timestamp_ms,
            "note": "This is reconstructed from Sierra historical `.depth` data, not inferred from trade prints."
        })))
    }

    #[tool(
        description = "Estimate pull/stack activity from Sierra `.depth` history over a time window, then align DOM decreases with `.scid` trades to separate likely fills from likely pulls. Use price_low/price_high to focus on a specific zone."
    )]
    async fn get_pull_stack_activity(
        &self,
        Parameters(params): Parameters<PullStackParams>,
    ) -> Result<CallToolResult, McpError> {
        validate_time_window(params.start_time_ms, params.end_time_ms)?;
        let start_time_ms = params.start_time_ms;
        let end_time_ms = params.end_time_ms;
        let price_low = params.price_low;
        let price_high = params.price_high;
        let summary = tokio::task::spawn_blocking(move || {
            let config = load_feed_config();
            let path = DepthReader::find_file_for_timestamp(&config, start_time_ms)
                .map_err(db_error)?
                .ok_or_else(|| {
                    invalid_params_error(format!(
                        "No Sierra .depth file found for timestamp {start_time_ms}"
                    ))
                })?;
            let depth_reader = DepthReader::new(path, config.price_scale);
            let trades = aggregate_window_trades(&config, start_time_ms, end_time_ms)?;
            depth_reader
                .summarize_window(start_time_ms, end_time_ms, &trades, price_low, price_high)
                .map_err(db_error)
        })
        .await
        .map_err(|e| db_error(format!("Pull/stack task failed: {e}")))??;

        Ok(text_result(serde_json::json!({
            "activity": summary,
            "priceFilter": { "low": price_low, "high": price_high },
            "note": "Estimated filled vs pulled is heuristic: DOM decreases are aligned to same-price `.scid` trade volume within the requested window."
        })))
    }

    #[tool(
        description = "Liquidity behavior around a target price over a time window. This focuses pull/stack analysis on a narrow band around a level, such as prior VAH, IB high, or an anchored VWAP level."
    )]
    async fn get_liquidity_behavior_at_level(
        &self,
        Parameters(params): Parameters<LiquidityBehaviorParams>,
    ) -> Result<CallToolResult, McpError> {
        validate_time_window(params.start_time_ms, params.end_time_ms)?;
        let radius_ticks = params.radius_ticks.unwrap_or(4).clamp(1, 20) as f64;
        let low = params.price - radius_ticks * 0.25;
        let high = params.price + radius_ticks * 0.25;
        let start_time_ms = params.start_time_ms;
        let end_time_ms = params.end_time_ms;
        let target_price = params.price;
        let summary = tokio::task::spawn_blocking(move || {
            let config = load_feed_config();
            let path = DepthReader::find_file_for_timestamp(&config, start_time_ms)
                .map_err(db_error)?
                .ok_or_else(|| {
                    invalid_params_error(format!(
                        "No Sierra .depth file found for timestamp {start_time_ms}"
                    ))
                })?;
            let depth_reader = DepthReader::new(path, config.price_scale);
            let trades = aggregate_window_trades(&config, start_time_ms, end_time_ms)?;
            depth_reader
                .summarize_window(start_time_ms, end_time_ms, &trades, Some(low), Some(high))
                .map_err(db_error)
        })
        .await
        .map_err(|e| db_error(format!("Liquidity behavior task failed: {e}")))??;

        Ok(text_result(serde_json::json!({
            "targetPrice": target_price,
            "radiusTicks": radius_ticks,
            "window": { "startTimeMs": start_time_ms, "endTimeMs": end_time_ms },
            "activity": summary,
            "note": "Use this to inspect whether liquidity near a specific level was stacking, getting pulled, or likely being consumed by trades."
        })))
    }

    #[tool(
        description = "Windowed delayed DOM summary using persisted DOM feature snapshots when available. Returns compact DOM summaries across a time range and optionally narrows the reported pull/stack levels to a price band. DOM data has ~1s polling lag from Sierra."
    )]
    async fn get_dom_window(
        &self,
        Parameters(params): Parameters<DomWindowParams>,
    ) -> Result<CallToolResult, McpError> {
        if let (Some(start), Some(end)) = (params.start_time_ms, params.end_time_ms) {
            validate_time_window(start, end)?;
        }
        let limit = params.limit.unwrap_or(20).clamp(1, 100);
        let mut snapshots = {
            let db = self.db.lock().map_err(|_| lock_error())?;
            db.query_dom_feature_snapshots(params.start_time_ms, params.end_time_ms, limit)
                .map_err(db_error)?
        };
        if snapshots.is_empty() {
            if let (Some(start), Some(end)) = (params.start_time_ms, params.end_time_ms) {
                let price_low = params.price_low;
                let price_high = params.price_high;
                let direct = tokio::task::spawn_blocking(move || {
                    let (feature, _) =
                        compute_dom_feature_for_window(start, end, end, 10, price_low, price_high)?;
                    Ok::<_, McpError>((
                        feature.timestamp_ms,
                        serde_json::to_value(feature).unwrap_or_default(),
                    ))
                })
                .await
                .map_err(|e| db_error(format!("DOM window task failed: {e}")))??;
                snapshots.push(direct);
            }
        }

        for (_, payload) in &mut snapshots {
            if let Some(activity) = payload.get_mut("activity").and_then(|v| v.as_object_mut()) {
                for key in ["topPullLevels", "topStackLevels"] {
                    if let Some(levels) = activity.get_mut(key).and_then(|v| v.as_array_mut()) {
                        levels.retain(|level| {
                            let price = level.get("price").and_then(|v| v.as_f64()).unwrap_or(0.0);
                            if let Some(low) = params.price_low {
                                if price < low {
                                    return false;
                                }
                            }
                            if let Some(high) = params.price_high {
                                if price > high {
                                    return false;
                                }
                            }
                            true
                        });
                    }
                }
            }
        }

        let latest = snapshots.last().map(|(_, payload)| payload.clone());
        Ok(text_result(serde_json::json!({
            "windowStartMs": params.start_time_ms,
            "windowEndMs": params.end_time_ms,
            "priceFilter": { "low": params.price_low, "high": params.price_high },
            "snapshots": snapshots.into_iter().map(|(ts, payload)| serde_json::json!({
                "timestampMs": ts,
                "payload": payload
            })).collect::<Vec<_>>(),
            "latest": latest,
            "source": if latest.is_some() { "dom_feature_snapshots" } else { "none" }
        })))
    }

    #[tool(
        description = "One-call delayed DOM + tape context at a timestamp. Combines the nearest DOM snapshot, the nearest persisted DOM feature summary, raw-tick footprint over a short window, and derived flow flags. DOM data has ~1s polling lag from Sierra."
    )]
    async fn get_dom_tape_context_at(
        &self,
        Parameters(params): Parameters<DomTapeContextParams>,
    ) -> Result<CallToolResult, McpError> {
        let window_ms = params
            .window_ms
            .unwrap_or(60_000.0)
            .clamp(5_000.0, 300_000.0);
        let start_time_ms = params.timestamp_ms - window_ms;
        let end_time_ms = params.timestamp_ms + 1_000.0;
        validate_time_window(start_time_ms, end_time_ms)?;

        let (mut feature, mut dom_snapshot, ticks) = {
            let db = self.db.lock().map_err(|_| lock_error())?;
            (
                db.get_dom_feature_near(params.timestamp_ms)
                    .map_err(db_error)?,
                db.get_dom_snapshot_near(params.timestamp_ms)
                    .map_err(db_error)?,
                db.query_ticks_filtered(
                    Some(start_time_ms),
                    Some(end_time_ms),
                    params.price_low,
                    params.price_high,
                    None,
                    2_000,
                )
                .map_err(db_error)?,
            )
        };

        if feature.is_none() || dom_snapshot.is_none() {
            let timestamp_ms = params.timestamp_ms;
            let price_low = params.price_low;
            let price_high = params.price_high;
            let fallback = tokio::task::spawn_blocking(move || {
                let (feat, snap) = compute_dom_feature_for_window(
                    start_time_ms,
                    end_time_ms,
                    timestamp_ms,
                    10,
                    price_low,
                    price_high,
                )?;
                Ok::<_, McpError>((
                    (
                        snap.snapshot_timestamp_ms,
                        serde_json::to_value(&snap).unwrap_or_default(),
                    ),
                    (
                        feat.timestamp_ms,
                        serde_json::to_value(feat).unwrap_or_default(),
                    ),
                ))
            })
            .await
            .map_err(|e| db_error(format!("DOM tape context task failed: {e}")))??;
            dom_snapshot.get_or_insert(fallback.0);
            feature.get_or_insert(fallback.1);
        }

        let footprint = footprint_from_ticks(&ticks);
        let total_volume: f64 = ticks.iter().map(|tick| tick.volume).sum();
        let net_delta: f64 = ticks
            .iter()
            .map(|tick| {
                if tick.is_buy {
                    tick.volume
                } else {
                    -tick.volume
                }
            })
            .sum();
        let dom_summary = feature
            .as_ref()
            .and_then(|(_, payload)| payload.get("domSummary"))
            .cloned();
        let activity = feature
            .as_ref()
            .and_then(|(_, payload)| payload.get("activity"))
            .cloned();
        let aggressive_buyers = net_delta > 0.0
            && dom_summary
                .as_ref()
                .and_then(|v| v.get("askPullRate"))
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0)
                < 0.5;
        let aggressive_sellers = net_delta < 0.0
            && dom_summary
                .as_ref()
                .and_then(|v| v.get("bidPullRate"))
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0)
                < 0.5;

        Ok(text_result(serde_json::json!({
            "timestampMs": params.timestamp_ms,
            "windowMs": window_ms,
            "domSnapshot": dom_snapshot.map(|(_, payload)| payload),
            "domFeature": feature.map(|(_, payload)| payload),
            "domSummary": dom_summary,
            "activity": activity,
            "tape": {
                "tickCount": ticks.len(),
                "totalVolume": total_volume,
                "netDelta": net_delta,
                "footprint": footprint,
            },
            "derivedFlags": {
                "aggressiveBuyers": aggressive_buyers,
                "aggressiveSellers": aggressive_sellers,
                "domSupportsHigher": dom_summary.as_ref().and_then(|v| v.get("liquidityBias")).and_then(|v| v.as_str()) == Some("bid_support"),
                "domCapsHigher": dom_summary.as_ref().and_then(|v| v.get("liquidityBias")).and_then(|v| v.as_str()) == Some("ask_resistance"),
            }
        })))
    }

    #[tool(
        description = "Explanation-oriented delayed DOM read around a timestamp or level. Grounds the interpretation in persisted DOM summaries, nearby depth events, and executed tape. DOM data has ~1s polling lag from Sierra."
    )]
    async fn explain_book_reaction(
        &self,
        Parameters(params): Parameters<ExplainBookReactionParams>,
    ) -> Result<CallToolResult, McpError> {
        let target_time_ms = params
            .timestamp_ms
            .or(params.end_time_ms)
            .ok_or_else(|| invalid_params_error("timestampMs or endTimeMs is required"))?;
        let start_time_ms = params.start_time_ms.unwrap_or(target_time_ms - 30_000.0);
        let end_time_ms = params.end_time_ms.unwrap_or(target_time_ms + 1_000.0);
        validate_time_window(start_time_ms, end_time_ms)?;
        let radius_ticks = params.radius_ticks.unwrap_or(6) as f64;
        let price_low = params.price.map(|price| price - radius_ticks * 0.25);
        let price_high = params.price.map(|price| price + radius_ticks * 0.25);

        let (feature, depth_events, ticks) = {
            let db = self.db.lock().map_err(|_| lock_error())?;
            (
                db.get_dom_feature_near(target_time_ms).map_err(db_error)?,
                db.query_depth_events(
                    Some(start_time_ms),
                    Some(end_time_ms),
                    price_low,
                    price_high,
                    200,
                )
                .map_err(db_error)?,
                db.query_ticks_filtered(
                    Some(start_time_ms),
                    Some(end_time_ms),
                    price_low,
                    price_high,
                    None,
                    500,
                )
                .map_err(db_error)?,
            )
        };

        let feature_payload = if let Some((_, payload)) = feature {
            payload
        } else {
            let timestamp_ms = target_time_ms;
            tokio::task::spawn_blocking(move || {
                let (feat, _) = compute_dom_feature_for_window(
                    start_time_ms,
                    end_time_ms,
                    timestamp_ms,
                    10,
                    price_low,
                    price_high,
                )?;
                Ok::<_, McpError>(serde_json::to_value(feat).unwrap_or_default())
            })
            .await
            .map_err(|e| db_error(format!("Explain book reaction task failed: {e}")))??
        };

        let dom_summary = feature_payload
            .get("domSummary")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        let bid_pull_rate = dom_summary
            .get("bidPullRate")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let ask_pull_rate = dom_summary
            .get("askPullRate")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let pull_stack_bias = dom_summary
            .get("pullStackBias")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let net_delta: f64 = ticks
            .iter()
            .map(|tick| {
                if tick.is_buy {
                    tick.volume
                } else {
                    -tick.volume
                }
            })
            .sum();

        let liquidity_bias = dom_summary
            .get("liquidityBias")
            .and_then(|v| v.as_str())
            .unwrap_or("balanced");
        let total_volume: f64 = ticks.iter().map(|t| t.volume).sum();

        // Extract top pull/stack prices from activity for narrative
        let top_pull = feature_payload
            .get("activity")
            .and_then(|a| a.get("topPullLevels"))
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())
            .cloned();
        let top_stack = feature_payload
            .get("activity")
            .and_then(|a| a.get("topStackLevels"))
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())
            .cloned();

        // Build magnitude-aware narrative
        let mut parts = Vec::new();

        // Pull rate comparison with actual numbers
        let bid_pct = (bid_pull_rate * 100.0).round();
        let ask_pct = (ask_pull_rate * 100.0).round();
        if (bid_pull_rate - ask_pull_rate).abs() > 0.1 {
            if bid_pull_rate > ask_pull_rate {
                parts.push(format!(
                    "Bids pulled at {bid_pct:.0}% rate vs asks at {ask_pct:.0}% — bid-side liquidity was being withdrawn faster."
                ));
            } else {
                parts.push(format!(
                    "Asks pulled at {ask_pct:.0}% rate vs bids at {bid_pct:.0}% — offer-side liquidity was being withdrawn faster."
                ));
            }
        } else {
            parts.push(format!(
                "Pull rates roughly balanced (bids {bid_pct:.0}%, asks {ask_pct:.0}%)."
            ));
        }

        // Top pull level with price
        if let Some(ref pull) = top_pull {
            let price = pull.get("price").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let qty = pull
                .get("estimatedPulledQuantity")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let side = pull
                .get("side")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            if qty > 0.0 {
                parts.push(format!(
                    "Top pull level: {price:.2} ({side} side, {qty:.0} contracts pulled)."
                ));
            }
        }

        // Top stack level with price
        if let Some(ref stack) = top_stack {
            let price = stack.get("price").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let qty = stack
                .get("stackedQuantity")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let side = stack
                .get("side")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            if qty > 0.0 {
                parts.push(format!(
                    "Top stack level: {price:.2} ({side} side, {qty:.0} contracts stacked)."
                ));
            }
        }

        // Net delta context
        if net_delta.abs() > 0.0 {
            let direction = if net_delta > 0.0 {
                "buyer-led"
            } else {
                "seller-led"
            };
            parts.push(format!(
                "Net delta {net_delta:+.0} over {total_volume:.0} volume — tape was {direction}."
            ));
        }

        // Depth event density
        if !depth_events.is_empty() {
            parts.push(format!(
                "{} depth events in window — {} book activity.",
                depth_events.len(),
                if depth_events.len() > 100 {
                    "heavy"
                } else if depth_events.len() > 30 {
                    "moderate"
                } else {
                    "light"
                }
            ));
        }

        // Overall read combining book + tape
        let overall = if pull_stack_bias > 0.0 && net_delta >= 0.0 {
            "Book and tape aligned supportive: bid-side liquidity held up while tape stayed neutral-to-positive."
        } else if pull_stack_bias < 0.0 && net_delta <= 0.0 {
            "Book and tape aligned defensive: offers held better than bids while tape skewed seller-led."
        } else if pull_stack_bias > 0.0 && net_delta < 0.0 {
            "Book was supportive but tape disagreed — bids were stacking while sellers dominated the tape. Potential absorption."
        } else if pull_stack_bias < 0.0 && net_delta > 0.0 {
            "Book was fragile but tape was buying — offers were pulling while buyers lifted aggressively. Potential breakout setup."
        } else {
            "Liquidity stayed relatively balanced — the reaction looks more tape-driven than book-driven."
        };
        parts.push(overall.to_string());

        let explanation = parts.join(" ");

        Ok(text_result(serde_json::json!({
            "timestampMs": target_time_ms,
            "window": { "startTimeMs": start_time_ms, "endTimeMs": end_time_ms },
            "priceFocus": { "price": params.price, "radiusTicks": params.radius_ticks },
            "domFeature": feature_payload,
            "depthEventCount": depth_events.len(),
            "tapeTickCount": ticks.len(),
            "totalVolume": total_volume,
            "netDelta": net_delta,
            "pullRates": { "bid": bid_pull_rate, "ask": ask_pull_rate },
            "pullStackBias": pull_stack_bias,
            "liquidityBias": liquidity_bias,
            "topPullLevel": top_pull,
            "topStackLevel": top_stack,
            "explanation": explanation,
        })))
    }

    #[tool(
        description = "Stacked and diagonal imbalance detection from the footprint. Stacked: 3+ consecutive levels where one side dominates (>2:1 ratio) -- shows directional conviction. Diagonal: aggressive lifting/hitting across adjacent levels -- shows urgency. Returns prices and direction for each type."
    )]
    async fn get_imbalances(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        if let Ok(pipelines) = self.pipelines.try_lock() {
            let stacked_prices = pipelines.footprint.stacked_imbalances(2.0, 3);
            let diagonals = pipelines.footprint.diagonal_imbalances(2.0);
            let diagonal_data: Vec<serde_json::Value> = diagonals
                .iter()
                .map(|(p1, p2, ratio, is_buy)| {
                    serde_json::json!({
                        "priceLow": p1,
                        "priceHigh": p2,
                        "ratio": ratio,
                        "direction": if *is_buy { "buy" } else { "sell" },
                    })
                })
                .collect();
            return Ok(text_result(serde_json::json!({
                "stackedImbalancePrices": stacked_prices,
                "stackedCount": stacked_prices.len(),
                "diagonalImbalances": diagonal_data,
                "diagonalCount": diagonals.len(),
                "note": "Stacked: 3+ consecutive levels with >2:1 imbalance ratio. Diagonal: adjacent-level bid/ask imbalances >2:1.",
                "dataAgeMs": compute_data_age(&db)
            })));
        }
        match db.latest_microstructure_snapshot() {
            Ok(Some(s)) => Ok(text_result(serde_json::json!({
                "snapshot": s,
                "note": "Falling back to DB snapshot. Stacked/diagonal detail not available.",
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data("No imbalance data available")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Recent absorption events: high-volume levels where price failed to break through (absorption) or where volume declined into a directional move (exhaustion). Each event includes timestamp, price, severity score."
    )]
    async fn get_absorption_events(
        &self,
        Parameters(params): Parameters<LimitParams>,
    ) -> Result<CallToolResult, McpError> {
        let limit = params.limit.unwrap_or(25) as usize;

        // Try live pipeline first (try_lock to avoid blocking when backfill/poll holds lock)
        if let Ok(pipelines) = self.pipelines.try_lock() {
            let live_events = pipelines.absorption.recent_events();
            if !live_events.is_empty() {
                let events: Vec<serde_json::Value> = live_events
                    .iter()
                    .rev()
                    .take(limit)
                    .map(|evt| {
                        serde_json::json!({
                            "timestampMs": evt.timestamp_ms,
                            "eventType": evt.event_type,
                            "price": evt.price,
                            "severity": evt.severity,
                        })
                    })
                    .collect();
                let db = self.db.lock().map_err(|_| lock_error())?;
                return Ok(text_result(serde_json::json!({
                    "events": events,
                    "count": events.len(),
                    "source": "live_pipeline",
                    "dataAgeMs": compute_data_age(&db)
                })));
            }
        }

        // Fall back to market_events table (where FlowEventEmitter writes absorption_detected events)
        let db = self.db.lock().map_err(|_| lock_error())?;
        match db.list_market_events_by_type("absorption_detected", limit) {
            Ok(events) => Ok(text_result(serde_json::json!({
                "events": events,
                "count": events.len(),
                "source": "market_events_db",
                "dataAgeMs": compute_data_age(&db)
            }))),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Trade size distribution: counts of 1-lot, 2-5 lot, 6-20 lot, and 21+ lot trades for the current session. Includes average trade size and prices where institutional (21+) lot trades clustered. Use for identifying institutional participation and footprint locations."
    )]
    async fn get_trade_size_profile(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        if let Ok(pipelines) = self.pipelines.try_lock() {
            let snap = pipelines.trade_size.snapshot();
            let total_trades = snap.lot_1 + snap.lot_2_5 + snap.lot_6_20 + snap.lot_21_plus;
            let large_prices = pipelines.trade_size.large_trade_prices();
            let large_data: Vec<serde_json::Value> = large_prices
                .iter()
                .take(20)
                .map(|(price, count)| {
                    serde_json::json!({
                        "price": price,
                        "largeLotCount": count,
                    })
                })
                .collect();
            return Ok(text_result(serde_json::json!({
                "lot1": snap.lot_1,
                "lot2to5": snap.lot_2_5,
                "lot6to20": snap.lot_6_20,
                "lot21plus": snap.lot_21_plus,
                "totalTrades": total_trades,
                "avgTradeSize": snap.avg_trade_size,
                "largeTradePrices": large_data,
                "dataAgeMs": compute_data_age(&db)
            })));
        }
        match db.latest_microstructure_snapshot() {
            Ok(Some(s)) => Ok(text_result(serde_json::json!({
                "snapshot": s,
                "note": "Falling back to DB snapshot. Per-price detail not available.",
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data("No trade size data available")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Evaluate all active playbook setups against current market state. Returns per-setup status (conditionsMet, approaching, notActive) and recent signal count. Always frames results as 'your playbook says...' -- never advisory."
    )]
    async fn evaluate_playbook(&self) -> Result<CallToolResult, McpError> {
        let (setups, risk_at_limit, fallback_price, count, data_age_ms) = {
            let db = self.db.lock().map_err(|_| lock_error())?;
            let setups = db.list_setups().unwrap_or_default();
            let risk_at_limit = db
                .load_risk_state()
                .ok()
                .flatten()
                .map(|s| s.at_limit)
                .unwrap_or(false);
            let fallback_price = db
                .latest_feature_state()
                .ok()
                .flatten()
                .and_then(|s| s.get("lastPrice").and_then(|v| v.as_f64()))
                .unwrap_or(0.0);
            let count = db.count_playbook_signals().unwrap_or(0);
            let data_age_ms = compute_data_age(&db);
            (setups, risk_at_limit, fallback_price, count, data_age_ms)
        };

        let bid = self.last_bid.lock().map(|g| *g).unwrap_or(0.0);
        let ask = self.last_ask.lock().map(|g| *g).unwrap_or(0.0);
        let (bid, ask) = if bid > 0.0 || ask > 0.0 {
            (
                if bid > 0.0 { bid } else { ask - 0.25 },
                if ask > 0.0 { ask } else { bid + 0.25 },
            )
        } else {
            (fallback_price - 0.25, fallback_price + 0.25)
        };

        let mut setup_statuses: Vec<serde_json::Value> = Vec::new();
        if let (Ok(pipelines), Ok(mut rules)) = (self.pipelines.try_lock(), self.rules.lock()) {
            let market = pipelines.snapshot(bid, ask);
            for setup in &setups {
                let _ = rules.evaluate(setup, &market, risk_at_limit);
                let state = rules.get_state(&setup.id);
                setup_statuses.push(serde_json::json!({
                    "setupId": setup.id,
                    "setupName": setup.name,
                    "state": format!("{:?}", state),
                }));
            }
        } else {
            for setup in &setups {
                setup_statuses.push(serde_json::json!({
                    "setupId": setup.id,
                    "setupName": setup.name,
                    "state": "unknown",
                }));
            }
        }

        Ok(text_result(serde_json::json!({
            "setupStatuses": setup_statuses,
            "recentSignalCount": count,
            "dataAgeMs": data_age_ms
        })))
    }

    #[tool(
        description = "Current risk state: daily P&L in R-units, trade count, consecutive losses/wins, drawdown, and whether the daily loss limit has been reached. Uses the trader's configured R framework."
    )]
    async fn get_risk_state(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        match db.load_risk_state() {
            Ok(Some(risk)) => Ok(text_result(serde_json::json!({
                "riskState": risk,
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data("Risk state not initialized")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Account state for risk coach: last known balance, open positions not from chat, Lucid params (daily loss, account size), profit goals. Call at session start to report last balance and ask for confirmation."
    )]
    async fn get_account_state(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        match db.load_account_state() {
            Ok(Some(state)) => Ok(text_result(serde_json::json!({
                "accountState": state,
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data("Account state not initialized. Ask trader for current balance and save via save_account_state.")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Save account state: balance, open positions, Lucid params. Call after trader confirms. Partial updates: only provided fields are updated."
    )]
    async fn save_account_state(
        &self,
        Parameters(params): Parameters<SaveAccountStateParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let existing = db.load_account_state().map_err(db_error)?;
        let base = existing.unwrap_or(AccountStateRecord {
            last_balance_dollars: 0.0,
            last_balance_updated_at_ms: 0,
            open_positions: Vec::new(),
            lucid_daily_loss_dollars: None,
            lucid_account_size_dollars: None,
            profit_target_per_cycle: None,
            position_sizing_method: "quarter_kelly".to_string(),
            kelly_fraction: 0.25,
        });
        let now_ms = chrono::Utc::now().timestamp_millis();
        let has_updates = params.last_balance_dollars.is_some() || params.open_positions.is_some();
        let open_positions: Vec<OpenPositionRecord> = match params.open_positions {
            Some(positions) => positions
                .into_iter()
                .map(|p| OpenPositionRecord {
                    direction: p.direction,
                    size: p.size,
                    entry_price: p.entry_price,
                    instrument: p.instrument,
                    setup_id: p.setup_id,
                })
                .collect(),
            None => base.open_positions,
        };
        let state = AccountStateRecord {
            last_balance_dollars: params
                .last_balance_dollars
                .unwrap_or(base.last_balance_dollars),
            last_balance_updated_at_ms: if has_updates {
                now_ms
            } else {
                base.last_balance_updated_at_ms
            },
            open_positions,
            lucid_daily_loss_dollars: params
                .lucid_daily_loss_dollars
                .or(base.lucid_daily_loss_dollars),
            lucid_account_size_dollars: params
                .lucid_account_size_dollars
                .or(base.lucid_account_size_dollars),
            profit_target_per_cycle: params
                .profit_target_per_cycle
                .or(base.profit_target_per_cycle),
            position_sizing_method: params
                .position_sizing_method
                .unwrap_or_else(|| base.position_sizing_method.clone()),
            kelly_fraction: params.kelly_fraction.unwrap_or(base.kelly_fraction),
        };
        db.save_account_state(&state).map_err(db_error)?;
        Ok(text_result(serde_json::json!({
            "saved": true,
            "accountState": state,
        })))
    }

    #[tool(
        description = "1/4 Kelly position sizing with optional confidence scaling. Returns suggested R to risk, fractional Kelly, and raw f*. Uses get_signal_performance for win rate and avg winner/loser R. Confidence: 0.5=low (1/8 Kelly), 1.0=normal (1/4 Kelly), 1.5=high (up to 1/2 Kelly)."
    )]
    async fn get_kelly_position_size(
        &self,
        Parameters(params): Parameters<KellyPositionSizeParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let perf = db
            .signal_performance(params.setup_id.as_deref(), None, None)
            .map_err(db_error)?;
        let win_rate = perf.get("winRate").and_then(|v| v.as_f64()).unwrap_or(0.5);
        let avg_winner = perf.get("avgWinnerR").and_then(|v| v.as_f64());
        let avg_loser = perf.get("avgLoserR").and_then(|v| v.as_f64());
        let (p, q, b) = match (avg_winner, avg_loser) {
            (Some(aw), Some(al)) if al.abs() > 1e-9 => {
                let b = aw / al.abs();
                (win_rate, 1.0 - win_rate, b)
            }
            _ => {
                return Ok(text_result(serde_json::json!({
                    "note": "Insufficient signal data for Kelly. Need avgWinnerR and avgLoserR from signal_outcomes.",
                    "suggestedR": 1.0,
                    "confidenceMultiplier": params.confidence_multiplier.unwrap_or(1.0),
                })))
            }
        };
        let f_full = if b > 1e-9 { (b * p - q) / b } else { 0.0 };
        let f_full = f_full.clamp(0.0, 1.0);
        let conf = params.confidence_multiplier.unwrap_or(1.0);
        let f_quarter = 0.25_f64 * f_full * conf;
        let f_quarter = f_quarter.clamp(0.0, 0.5);
        let balance = params.balance_dollars.unwrap_or(50_000.0);
        let risk_config = db.load_risk_config().map_err(db_error)?;
        let r_dollars = risk_config.r_value_dollars;
        let suggested_r = if r_dollars > 1e-9 {
            (f_quarter * balance) / r_dollars
        } else {
            1.0
        };
        Ok(text_result(serde_json::json!({
            "fullKellyF": f_full,
            "quarterKellyF": f_quarter,
            "suggestedR": suggested_r,
            "confidenceMultiplier": conf,
            "balanceDollars": balance,
            "winRate": p,
            "avgWinnerR": avg_winner,
            "avgLoserR": avg_loser,
        })))
    }

    #[tool(
        description = "Trader's risk configuration: R-value in points and dollars, max daily loss in R-units and dollars, max consecutive losses, max trades per session, no-trade zones."
    )]
    async fn get_risk_config(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let config = db.load_risk_config().map_err(db_error)?;
        Ok(text_result(serde_json::json!({
            "riskConfig": config
        })))
    }

    #[tool(
        description = "Save risk configuration. Partial updates: only provided fields are updated. Call to persist R-value, max daily loss, circuit breaker, and trade limits. Required for full risk tracking when config is not yet in database."
    )]
    async fn save_risk_config(
        &self,
        Parameters(params): Parameters<SaveRiskConfigParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let base = db.load_risk_config().map_err(db_error)?;
        let config = RiskConfigRecord {
            r_value_points: params.r_value_points.unwrap_or(base.r_value_points),
            r_value_dollars: params.r_value_dollars.unwrap_or(base.r_value_dollars),
            max_daily_loss_r: params.max_daily_loss_r.unwrap_or(base.max_daily_loss_r),
            max_consecutive_losses: params
                .max_consecutive_losses
                .unwrap_or(base.max_consecutive_losses),
            max_trades_per_session: params
                .max_trades_per_session
                .or(base.max_trades_per_session),
            no_trade_zones: base.no_trade_zones,
            max_daily_loss_dollars: params
                .max_daily_loss_dollars
                .or(base.max_daily_loss_dollars),
        };
        db.save_risk_config(&config).map_err(db_error)?;
        Ok(text_result(serde_json::json!({
            "saved": true,
            "riskConfig": config
        })))
    }

    #[tool(
        description = "Initialize or reset risk state for a new session. Creates the initial risk state row (0 P&L, 0 trades, no streaks) so get_risk_state returns valid data. Call at session start to enable full risk tracking. Uses max_daily_loss_r from risk_config."
    )]
    async fn init_risk_state(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let config = db.load_risk_config().map_err(db_error)?;
        let state = RiskState {
            daily_pnl_r: 0.0,
            trade_count: 0,
            consecutive_losses: 0,
            consecutive_wins: 0,
            drawdown_r: 0.0,
            max_daily_loss_r: config.max_daily_loss_r,
            at_limit: false,
        };
        db.save_risk_state(&state).map_err(db_error)?;
        Ok(text_result(serde_json::json!({
            "initialized": true,
            "riskState": state
        })))
    }

    #[tool(
        description = "Record a completed trade result. Updates risk state (daily P&L, consecutive wins/losses, drawdown, at_limit). Also creates a trade record for performance tracking. Call after a trade is closed to keep risk state current."
    )]
    async fn record_trade_result(
        &self,
        Parameters(params): Parameters<RecordTradeResultParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;

        // 1. Insert trade record
        let trade_id = uuid::Uuid::new_v4().to_string();
        let now_ms = chrono::Utc::now().timestamp_millis() as f64;
        let trade = TradeRecord {
            id: trade_id.clone(),
            session_id: None,
            setup_id: params.setup_id.clone(),
            entry_time: now_ms,
            entry_price: params.entry_price,
            exit_time: Some(now_ms),
            exit_price: Some(params.exit_price),
            direction: params.direction.clone(),
            size: params.size,
            stop_price: params.stop_price,
            target_prices: Vec::new(),
            result_r: Some(params.result_r),
            planned: true,
            rules_followed: None,
            emotional_state: None,
            notes: params.notes.unwrap_or_default(),
            source: "mcp".to_string(),
        };
        db.insert_trade(&trade).map_err(db_error)?;

        // 1b. Bridge trades -> signal_outcomes: resolve pending signal if setup_id matches
        if let Some(ref setup_id) = params.setup_id {
            let _ = db.resolve_pending_signal_by_setup_id(setup_id, params.result_r, now_ms);
        }

        // 2. Load current risk state, apply result via RiskTracker, save
        let risk_state = db.load_risk_state().map_err(db_error)?.unwrap_or_default();
        let config = db.load_risk_config().map_err(db_error)?;
        let mut tracker = RiskTracker::new(RiskConfig {
            max_daily_loss_r: config.max_daily_loss_r,
            max_trades_per_session: config.max_trades_per_session.unwrap_or(8) as usize,
        });
        tracker.restore_state(risk_state);
        tracker.record_trade_result(params.result_r);
        let new_state = tracker.state();
        db.save_risk_state(&new_state).map_err(db_error)?;

        Ok(text_result(serde_json::json!({
            "recorded": true,
            "tradeId": trade_id,
            "resultR": params.result_r,
            "updatedRiskState": new_state,
            "atLimit": new_state.at_limit,
            "consecutiveLosses": new_state.consecutive_losses,
            "consecutiveWins": new_state.consecutive_wins,
            "dailyPnlR": new_state.daily_pnl_r,
            "drawdownR": new_state.drawdown_r,
            "tradeCount": new_state.trade_count,
        })))
    }

    #[tool(
        description = "Query raw tick data. Without filters, returns the most recent ticks (most-recent first). With start_time_ms/end_time_ms, returns ticks in that time window in chronological order (ASC) — ideal for reconstructing the tape at a specific moment. With price_low/price_high, limits to trades in that price range. With session_date (YYYY-MM-DD), limits to that trading day. All filters can be combined. Use get_market_snapshot to get the current timestamp_ms and work backward from there."
    )]
    async fn query_ticks(
        &self,
        Parameters(params): Parameters<TickQueryParams>,
    ) -> Result<CallToolResult, McpError> {
        let limit = params.limit.unwrap_or(200).min(2000) as usize;
        let db = self.db.lock().map_err(|_| lock_error())?;
        let has_filters = params.start_time_ms.is_some()
            || params.end_time_ms.is_some()
            || params.price_low.is_some()
            || params.price_high.is_some()
            || params.session_date.is_some();
        if has_filters {
            match db.query_ticks_filtered(
                params.start_time_ms,
                params.end_time_ms,
                params.price_low,
                params.price_high,
                params.session_date.as_deref(),
                limit,
            ) {
                Ok(ticks) => Ok(text_result(serde_json::json!({
                    "ticks": ticks,
                    "count": ticks.len(),
                    "order": if params.start_time_ms.is_some() || params.end_time_ms.is_some() { "chronological" } else { "mostRecentFirst" },
                    "filters": {
                        "startTimeMs": params.start_time_ms,
                        "endTimeMs": params.end_time_ms,
                        "priceLow": params.price_low,
                        "priceHigh": params.price_high,
                        "sessionDate": params.session_date,
                    },
                    "dataAgeMs": compute_data_age(&db)
                }))),
                Err(e) => Err(db_error(e)),
            }
        } else {
            match db.list_recent_ticks(limit) {
                Ok(ticks) => Ok(text_result(serde_json::json!({
                    "ticks": ticks,
                    "count": ticks.len(),
                    "order": "mostRecentFirst",
                    "dataAgeMs": compute_data_age(&db)
                }))),
                Err(e) => Err(db_error(e)),
            }
        }
    }

    #[tool(
        description = "Session summary: total tick count, latest tick timestamp, and latest pipeline snapshot. Provides a quick health check of data flow."
    )]
    async fn get_session_summary(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let tick_count = db.raw_tick_count().unwrap_or(0);
        let last_ts = db.latest_tick_timestamp_ms().ok().flatten();
        let snapshot = db.latest_feature_state().ok().flatten();
        Ok(text_result(serde_json::json!({
            "tickCount": tick_count,
            "latestTickTimestampMs": last_ts,
            "latestSnapshot": snapshot,
            "dataAgeMs": compute_data_age(&db)
        })))
    }

    #[tool(
        description = "Feed health diagnostics: SCID path status, file metadata, latest DB tick timestamp, ingest lag, and freshness/source state."
    )]
    async fn get_feed_health(&self) -> Result<CallToolResult, McpError> {
        let config = load_feed_config();
        let reader = ScidReader::from_feed_config(&config);
        let scid_path = reader.path().to_string_lossy().to_string();
        let meta = std::fs::metadata(reader.path()).ok();
        let file_exists = meta.is_some();
        let file_size_bytes = meta.as_ref().map(|m| m.len()).unwrap_or(0);
        let file_modified_ms = meta
            .and_then(|m| m.modified().ok())
            .and_then(|mtime| mtime.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as f64)
            .unwrap_or(-1.0);

        let db = self.db.lock().map_err(|_| lock_error())?;
        let tick_count = db.raw_tick_count().unwrap_or(0);
        let latest_tick_ms = db.latest_tick_timestamp_ms().ok().flatten();
        let data_age_ms = compute_data_age(&db);
        let source_state = if !file_exists {
            "missing"
        } else {
            freshness_status(data_age_ms)
        };

        Ok(text_result(serde_json::json!({
            "scidPath": scid_path,
            "fileExists": file_exists,
            "fileSizeBytes": file_size_bytes,
            "fileModifiedMs": file_modified_ms,
            "latestDbTickTimestampMs": latest_tick_ms,
            "dbTickCount": tick_count,
            "ingestLagMs": data_age_ms,
            "sourceState": source_state,
            "dataAgeMs": data_age_ms
        })))
    }

    #[tool(
        description = "Queue a historical backfill job and return a job id. Processes past sessions through all 14 pipelines, detects market events, and persists session summaries without blocking the MCP server."
    )]
    async fn backfill_history(
        &self,
        Parameters(params): Parameters<BackfillParams>,
    ) -> Result<CallToolResult, McpError> {
        let config = load_feed_config();
        let reader = ScidReader::from_feed_config(&config);
        if !reader.path().exists() {
            return Ok(no_data(
                "SCID file not found. Ensure Sierra Chart data path is configured.",
            ));
        }
        let wait = params.wait_for_completion.unwrap_or(false);
        let (run, already_running) = self
            .queue_historical_job(params, backfill::HistoricalJobType::ResearchBackfill, false)
            .await?;
        if wait {
            if let Some(done) = self.wait_for_job_terminal(&run.id).await {
                return Ok(text_result(historical_job_response(&done, false)));
            }
        }
        Ok(text_result(historical_job_response(&run, already_running)))
    }

    #[tool(
        description = "Queue a backtest replay job and return a job id. Replays the rules engine over historical .scid data without blocking the MCP server."
    )]
    async fn run_backtest(
        &self,
        Parameters(params): Parameters<BackfillParams>,
    ) -> Result<CallToolResult, McpError> {
        let config = load_feed_config();
        let reader = ScidReader::from_feed_config(&config);
        if !reader.path().exists() {
            return Ok(no_data(
                "SCID file not found. Ensure Sierra Chart data path is configured.",
            ));
        }
        let wait = params.wait_for_completion.unwrap_or(false);
        let (run, already_running) = self
            .queue_historical_job(params, backfill::HistoricalJobType::Backtest, true)
            .await?;
        if wait {
            if let Some(done) = self.wait_for_job_terminal(&run.id).await {
                return Ok(text_result(historical_job_response(&done, false)));
            }
        }
        Ok(text_result(historical_job_response(&run, already_running)))
    }

    #[tool(description = "Poll progress for a queued/running historical backfill or backtest job.")]
    async fn get_backfill_status(
        &self,
        Parameters(params): Parameters<BackfillStatusParams>,
    ) -> Result<CallToolResult, McpError> {
        match self.get_job_run(params.job_id.as_deref()).await? {
            Some(run) => Ok(text_result(historical_job_response(&run, false))),
            None => Ok(no_data("No historical job found")),
        }
    }

    #[tool(description = "Cancel an in-flight historical backfill or backtest job.")]
    async fn cancel_backfill(
        &self,
        Parameters(params): Parameters<CancelBackfillParams>,
    ) -> Result<CallToolResult, McpError> {
        let mut manager = self.backfill_manager.lock().await;
        if let Some(state) = manager.jobs.get_mut(&params.job_id) {
            state.cancel_flag.store(true, Ordering::Relaxed);
            state.run.status = "cancelling".to_string();
            state.run.progress["currentPhase"] = serde_json::json!("cancelling");
            if let Ok(db) = self.db.lock() {
                let _ = db.update_historical_job_run(
                    &params.job_id,
                    &the_desk_backend::db::HistoricalJobRunUpdate {
                        status: &state.run.status,
                        progress: &state.run.progress,
                        result: state.run.result.as_ref(),
                        warnings: &state.run.warnings,
                        error: state.run.error.as_deref(),
                        started_at_ms: state.run.started_at_ms,
                        finished_at_ms: state.run.finished_at_ms,
                    },
                );
            }
            return Ok(text_result(serde_json::json!({
                "jobId": params.job_id,
                "status": "cancelling",
            })));
        }
        Ok(no_data("Historical job not found"))
    }

    #[tool(
        description = "Retrieve stored backtest runs. Returns most recent runs with params, metrics, and signal performance. Use to analyze historical backtest results."
    )]
    async fn get_backtest_results(
        &self,
        Parameters(params): Parameters<LimitParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let limit = params.limit.unwrap_or(10) as usize;
        match db.list_backtest_runs(limit) {
            Ok(runs) => Ok(text_result(serde_json::json!({
                "runs": runs,
                "count": runs.len(),
            }))),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Compare two or more backtest runs side-by-side. Pass run IDs to compare params, metrics, and signal performance across parameter variations."
    )]
    async fn compare_backtests(
        &self,
        Parameters(params): Parameters<CompareBacktestsParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let mut runs = Vec::new();
        for id in &params.run_ids {
            if let Ok(Some(run)) = db.get_backtest_run(id) {
                runs.push(run);
            }
        }
        Ok(text_result(serde_json::json!({
            "runs": runs,
            "count": runs.len(),
        })))
    }

    #[tool(
        description = "Compare current session structure against similar historical sessions. Uses multi-dimensional similarity: IB range, day type, profile shape, balance state, RVOL ratio, session delta sign, single prints direction. Returns the most similar past sessions with their outcomes (close vs IB mid, delta, etc.)."
    )]
    async fn compare_sessions(
        &self,
        Parameters(params): Parameters<CompareSessionsParams>,
    ) -> Result<CallToolResult, McpError> {
        let snapshot = self.live_snapshot().or_else(|| {
            self.db
                .lock()
                .ok()
                .and_then(|d| d.latest_feature_state().ok().flatten())
        });
        let ib_range = params.current_ib_range.unwrap_or_else(|| {
            snapshot
                .as_ref()
                .and_then(|s| {
                    let h = s.get("ibHigh")?.as_f64()?;
                    let l = s.get("ibLow")?.as_f64()?;
                    Some(h - l)
                })
                .unwrap_or(0.0)
        });
        let rvol_ratio = params.rvol_ratio.or_else(|| {
            snapshot
                .as_ref()
                .and_then(|s| s.get("rvolRatio").and_then(|v| v.as_f64()))
        });
        let session_delta_sign = params.session_delta_sign.or_else(|| {
            snapshot.as_ref().and_then(|s| {
                s.get("sessionDelta").and_then(|v| v.as_f64()).map(|d| {
                    (if d > 0.5 {
                        "positive"
                    } else if d < -0.5 {
                        "negative"
                    } else {
                        "neutral"
                    })
                    .to_string()
                })
            })
        });
        let profile_shape = params.profile_shape.or_else(|| {
            snapshot.as_ref().and_then(|s| {
                s.get("profileShape")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            })
        });
        let balance_state = params.balance_state.or_else(|| {
            snapshot.as_ref().and_then(|s| {
                s.get("balanceState")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            })
        });
        let single_prints = params.single_prints_direction.or_else(|| {
            snapshot.as_ref().and_then(|s| {
                s.get("singlePrintsDirection")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            })
        });

        let query = research::SessionSimilarityQuery {
            ib_range: Some(ib_range),
            day_type: params.current_day_type.clone(),
            profile_shape,
            balance_state,
            rvol_ratio,
            session_delta_sign,
            single_prints_direction: single_prints,
            weights: research::SimilarityWeights::default(),
        };
        let db = self.db.lock().map_err(|_| lock_error())?;
        let max = params.max_results.unwrap_or(5) as usize;
        match research::compare_sessions_multi(&db, &query, max) {
            Ok(sessions) => Ok(text_result(serde_json::json!({
                "queryDimensions": {
                    "ibRange": ib_range,
                    "dayType": params.current_day_type,
                    "profileShape": query.profile_shape,
                    "balanceState": query.balance_state,
                    "rvolRatio": query.rvol_ratio,
                },
                "similarSessions": sessions,
                "count": sessions.len(),
            }))),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Query how often a market event occurs. Returns total occurrences, sessions with event, per-session average, and percentage of sessions. Structural event types: *_test (level tests), ib_extension_hit, ib_formed, or_formed, new_session_high/low, day_type_change, poor_high/low_detected, excess_high/low_detected, or5_mid_retest, dnp_cross, rvol_spike. Flow event types: absorption_detected (metadata.eventSubtype: absorption/exhaustion/delta_divergence), pinch_detected (metadata.timeframe: 1m/5m/15m/30m), acceleration_zone_created, acceleration_zone_held, large_trade_cluster."
    )]
    async fn query_event_frequency(
        &self,
        Parameters(params): Parameters<FrequencyParams>,
    ) -> Result<CallToolResult, McpError> {
        let scope = build_session_scope_filter(&params.session_scope)?;
        let db = self.db.lock().map_err(|_| lock_error())?;
        match research::event_frequency(
            &db,
            &params.event_type,
            params.start_date.as_deref(),
            params.end_date.as_deref(),
            scope.as_ref(),
        ) {
            Ok(result) => Ok(text_result(
                serde_json::to_value(&result).unwrap_or_default(),
            )),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Conditional probability query: 'When event X happens N+ times in a session, how often does outcome Y occur?' Example: 'If IB-mid is tested 3+ times, how often do we close above IB-mid?' Returns probability, sample size, and counts."
    )]
    async fn query_conditional(
        &self,
        Parameters(params): Parameters<ConditionalParams>,
    ) -> Result<CallToolResult, McpError> {
        let scope = build_session_scope_filter(&params.session_scope)?;
        let db = self.db.lock().map_err(|_| lock_error())?;
        let min_count = params.min_count.unwrap_or(1);
        match research::conditional_probability(
            &db,
            &params.event_type,
            min_count,
            &params.outcome_field,
            &params.outcome_value,
            params.start_date.as_deref(),
            params.end_date.as_deref(),
            scope.as_ref(),
        ) {
            Ok(result) => Ok(text_result(
                serde_json::to_value(&result).unwrap_or_default(),
            )),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Distribution of a numeric metric from session summaries. Returns mean, median, stddev, percentiles (10/25/75/90), min, max. Metrics: ib_range, session_delta, total_volume, rvol_ratio, tick_count, vwap_close, etc."
    )]
    async fn query_distribution(
        &self,
        Parameters(params): Parameters<DistributionParams>,
    ) -> Result<CallToolResult, McpError> {
        let scope = build_session_scope_filter(&params.session_scope)?;
        let db = self.db.lock().map_err(|_| lock_error())?;
        match research::metric_distribution(
            &db,
            &params.metric,
            params.start_date.as_deref(),
            params.end_date.as_deref(),
            scope.as_ref(),
        ) {
            Ok(result) => Ok(text_result(
                serde_json::to_value(&result).unwrap_or_default(),
            )),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Distribution of R-results from signal_outcomes for a setup. Answers: 'When setup X fires, what is the distribution of R-results?' Returns mean, median, stddev, percentiles. Requires signal_outcomes to be populated (run backtest or live tracking)."
    )]
    async fn query_signal_outcome_distribution(
        &self,
        Parameters(params): Parameters<SignalOutcomeDistributionParams>,
    ) -> Result<CallToolResult, McpError> {
        let scope = build_session_scope_filter(&params.session_scope)?;
        let db = self.db.lock().map_err(|_| lock_error())?;
        match research::signal_outcome_distribution(
            &db,
            &params.setup_id,
            params.start_date.as_deref(),
            params.end_date.as_deref(),
            scope.as_ref(),
        ) {
            Ok(result) => Ok(text_result(
                serde_json::to_value(&result).unwrap_or_default(),
            )),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Conditional win rate for signal outcomes: when setup X fires and session has field=value (e.g. day_type=Trend), what is the win rate? Joins signal_outcomes with session_summaries. Requires signal_outcomes to be populated."
    )]
    async fn query_signal_outcome_conditional(
        &self,
        Parameters(params): Parameters<SignalOutcomeConditionalParams>,
    ) -> Result<CallToolResult, McpError> {
        let scope = build_session_scope_filter(&params.session_scope)?;
        let db = self.db.lock().map_err(|_| lock_error())?;
        match research::signal_outcome_conditional(
            &db,
            &params.setup_id,
            &params.session_field,
            &params.field_value,
            params.start_date.as_deref(),
            params.end_date.as_deref(),
            scope.as_ref(),
        ) {
            Ok(result) => Ok(text_result(
                serde_json::to_value(&result).unwrap_or_default(),
            )),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Outcome excursion diagnostics for signal outcomes. Returns distributions for max favorable excursion (MFE), max adverse excursion (MAE), time-to-outcome (minutes), and MFE/MAE ratio, plus resolved outcome breakdown. Use to evaluate execution quality and target/stop behavior."
    )]
    async fn query_signal_outcome_excursions(
        &self,
        Parameters(params): Parameters<SignalOutcomeExcursionsParams>,
    ) -> Result<CallToolResult, McpError> {
        let scope = build_session_scope_filter(&params.session_scope)?;
        let db = self.db.lock().map_err(|_| lock_error())?;
        match research::signal_outcome_excursions(
            &db,
            params.setup_id.as_deref(),
            params.start_date.as_deref(),
            params.end_date.as_deref(),
            scope.as_ref(),
        ) {
            Ok(result) => Ok(text_result(
                serde_json::to_value(&result).unwrap_or_default(),
            )),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Query past session summaries with optional filters. Returns structured session data (OHLC, IB range, day type, delta, close vs levels, POC, VA, DNVA per session) for historical analysis and multi-session value migration."
    )]
    async fn get_session_history(
        &self,
        Parameters(params): Parameters<SessionHistoryParams>,
    ) -> Result<CallToolResult, McpError> {
        let scope = build_session_scope_filter(&params.session_scope)?;
        let start_date = scope
            .as_ref()
            .and_then(|s| s.trading_day_start.as_deref())
            .or(params.start_date.as_deref());
        let end_date = scope
            .as_ref()
            .and_then(|s| s.trading_day_end.as_deref())
            .or(params.end_date.as_deref());
        let db = self.db.lock().map_err(|_| lock_error())?;
        let limit = params.limit.unwrap_or(20) as usize;
        match db.list_session_summaries(
            start_date,
            end_date,
            params.day_type.as_deref(),
            scope.as_ref().and_then(|s| s.session_type.as_deref()),
            limit,
        ) {
            Ok(sessions) => {
                let count = sessions.len();
                let summaries: Vec<serde_json::Value> = sessions
                    .into_iter()
                    .map(|s| {
                        serde_json::json!({
                            "sessionDate": s.session_date,
                            "sessionType": s.session_type,
                            "dayType": s.day_type,
                            "ibRange": s.ib_range,
                            "high": s.high, "low": s.low, "close": s.close,
                            "poc": s.poc,
                            "vaHigh": s.vah,
                            "vaLow": s.val,
                            "dnvaHigh": s.dnva_high,
                            "dnvaLow": s.dnva_low,
                            "dnp": s.dnp,
                            "sessionDelta": s.session_delta,
                            "closeVsIbMid": s.close_vs_ib_mid,
                            "closeVsVwap": s.close_vs_vwap,
                            "closeVsPoc": s.close_vs_poc,
                            "rvolRatio": s.rvol_ratio,
                            "poorHigh": s.poor_high, "poorLow": s.poor_low,
                            "excessHigh": s.excess_high, "excessLow": s.excess_low,
                        })
                    })
                    .collect();
                Ok(text_result(serde_json::json!({
                    "sessions": summaries,
                    "count": count,
                })))
            }
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Signal/setup performance statistics. Returns win rate, average R, total signals, resolved/pending counts, target hit vs stop hit vs time-exit counts. Filter by setup_id to see performance of a specific setup. Optional source filter: live|backtest."
    )]
    async fn get_signal_performance(
        &self,
        Parameters(params): Parameters<SignalPerformanceParams>,
    ) -> Result<CallToolResult, McpError> {
        let scope = build_session_scope_filter(&params.session_scope)?;
        let source = params
            .source
            .as_deref()
            .map(|raw| {
                normalize_signal_source(raw).ok_or_else(|| {
                    invalid_params_error(format!("source must be one of live|backtest, got: {raw}"))
                })
            })
            .transpose()?;
        let db = self.db.lock().map_err(|_| lock_error())?;
        match db.signal_performance_filtered(
            params.setup_id.as_deref(),
            params.start_date.as_deref(),
            params.end_date.as_deref(),
            source,
            params.job_id.as_deref(),
            scope.as_ref(),
        ) {
            Ok(result) => Ok(text_result(result)),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Per-setup performance matrix in one call. Returns aggregated setup metrics: total/resolved/pending counts, target/stop/time-exit breakdown, win rate, avg R, avg winner/loser R. Supports date + session scope filters, minimum resolved threshold, sorting, and limit."
    )]
    async fn get_setup_performance_matrix(
        &self,
        Parameters(params): Parameters<SetupPerformanceMatrixParams>,
    ) -> Result<CallToolResult, McpError> {
        let scope = build_session_scope_filter(&params.session_scope)?;
        let sort_by = parse_setup_perf_sort(params.sort_by.as_deref())?;
        let min_resolved = params.min_resolved.unwrap_or(0).max(0);
        let limit = params.limit.unwrap_or(50).max(1) as usize;
        let db = self.db.lock().map_err(|_| lock_error())?;
        match db.setup_performance_matrix_filtered(
            params.start_date.as_deref(),
            params.end_date.as_deref(),
            None,
            None,
            scope.as_ref(),
            min_resolved,
            sort_by,
            limit,
        ) {
            Ok(rows) => Ok(text_result(serde_json::json!({
                "rows": rows,
                "count": rows.len(),
                "sortBy": params.sort_by.unwrap_or_else(|| "resolved".to_string()),
                "minResolved": min_resolved,
            }))),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Research summary: pre-session statistical briefing. Returns session count in database, IB range distribution, recent day types, and key frequencies. One call = baseline context for the trading day."
    )]
    async fn get_research_summary(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let session_count = db.session_summary_count().unwrap_or(0);
        let ib_dist = research::metric_distribution(&db, "ib_range", None, None, None)
            .ok()
            .map(|d| serde_json::to_value(&d).unwrap_or_default());
        let delta_dist = research::metric_distribution(&db, "session_delta", None, None, None)
            .ok()
            .map(|d| serde_json::to_value(&d).unwrap_or_default());

        Ok(text_result(serde_json::json!({
            "sessionsInDatabase": session_count,
            "ibRangeDistribution": ib_dist,
            "sessionDeltaDistribution": delta_dist,
            "note": if session_count < 20 {
                "Limited sample size. Run backfill_history to process more historical data."
            } else {
                "Statistical baselines established."
            },
        })))
    }

    #[tool(
        description = "Storage tier status: shows hot (current session), warm (SQLite ticks), and cold (archived) tier sizes. Includes session summary count and last archive date. Use to monitor data lifecycle."
    )]
    async fn archive_status(&self) -> Result<CallToolResult, McpError> {
        let storage = load_storage_config();
        let db = self.db.lock().map_err(|_| lock_error())?;
        let tick_count = db.raw_tick_count().unwrap_or(0);
        let session_count = db.session_summary_count().unwrap_or(0);

        let archive_dir = std::path::Path::new(&storage.cold_archive_dir);
        let archive_files: Vec<String> = if archive_dir.exists() {
            std::fs::read_dir(archive_dir)
                .ok()
                .map(|entries| {
                    entries
                        .filter_map(|e| e.ok())
                        .filter(|e| {
                            e.path()
                                .extension()
                                .map(|ext| ext == "zst")
                                .unwrap_or(false)
                        })
                        .map(|e| e.file_name().to_string_lossy().to_string())
                        .collect()
                })
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        Ok(text_result(serde_json::json!({
            "warmTier": {
                "rawTickCount": tick_count,
                "retentionDays": storage.warm_retention_days,
            },
            "coldTier": {
                "archiveDir": storage.cold_archive_dir,
                "archiveFiles": archive_files,
                "archiveFileCount": archive_files.len(),
            },
            "research": {
                "sessionSummaryCount": session_count,
            },
            "autoArchive": storage.auto_archive,
        })))
    }

    #[tool(
        description = "5-minute Opening Range (Leo's A+ setup): OR5 high, low, midpoint (key level), break direction (None/Up/Down), whether mid has been retested after breakout, and extension targets (75%% and 100%% of range from mid)."
    )]
    async fn get_or5_status(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        match db.latest_feature_state() {
            Ok(Some(s)) => Ok(text_result(serde_json::json!({
                "or5High": s.get("or5High"),
                "or5Low": s.get("or5Low"),
                "or5Mid": s.get("or5Mid"),
                "or5Locked": s.get("or5Locked"),
                "or5BreakDirection": s.get("or5BreakDirection"),
                "or5MidRetested": s.get("or5MidRetested"),
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data(
                "No OR5 data available. RTH session may not have started.",
            )),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Relative Volume: ratio of current session's cumulative volume vs the N-day average at the same time-of-day. Returns classification (Low/Normal/Elevated/High), percentile rank (0-100 vs history at same time), velocity (rate of change per 5-min bucket), acceleration (second derivative), bucket progress, actual vs expected volume, and lookback days. Use to calibrate participation quality and regime context."
    )]
    async fn get_rvol(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        // Try live pipeline first for full snapshot.
        if let Ok(pipelines) = self.pipelines.try_lock() {
            let rvol = &pipelines.rvol;
            let actual = rvol.session_volume();
            let expected = rvol.expected_volume_at_bucket();
            let total = rvol.total_buckets();
            let bucket = rvol.bucket_index();
            let session_pct = if total > 0 {
                format!("{:.1}%", bucket as f64 / total as f64 * 100.0)
            } else {
                "0.0%".to_string()
            };
            return Ok(text_result(serde_json::json!({
                "rvolRatio": rvol.rvol_ratio(),
                "rvolClassification": format!("{:?}", rvol.classification()),
                "rvolPercentile": rvol.rvol_percentile(),
                "currentBucket": bucket,
                "totalBuckets": total,
                "sessionProgress": session_pct,
                "actualVolume": actual,
                "expectedVolume": expected,
                "volumeDelta": actual - expected,
                "velocity": rvol.rvol_velocity(),
                "acceleration": rvol.rvol_acceleration(),
                "lookbackDays": rvol.lookback_days(),
                "dataAgeMs": compute_data_age(&db),
            })));
        }
        // Fallback to DB snapshot when pipeline lock is unavailable.
        match db.latest_feature_state() {
            Ok(Some(s)) => Ok(text_result(serde_json::json!({
                "rvolRatio": s.get("rvolRatio"),
                "rvolClassification": s.get("rvolClassification"),
                "note": "Falling back to DB snapshot. Percentile, velocity, and bucket details not available.",
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data("No RVOL data available")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Day type classification (Dalton): Normal, NormalVariation, Neutral, Trend, or DoubleDistribution. Profile shape: Gaussian, PShape, BShape, DShape. Balance state: Balanced vs Imbalanced. Single prints direction relative to POC."
    )]
    async fn get_day_type(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        match db.latest_feature_state() {
            Ok(Some(s)) => Ok(text_result(serde_json::json!({
                "dayType": s.get("dayType"),
                "profileShape": s.get("profileShape"),
                "balanceState": s.get("balanceState"),
                "singlePrintsDirection": s.get("singlePrintsDirection"),
                "poorHigh": s.get("poorHigh"),
                "poorLow": s.get("poorLow"),
                "excessHigh": s.get("excessHigh"),
                "excessLow": s.get("excessLow"),
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data("No day type data available")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Active rebid/reoffer acceleration zones: price ranges of one-sided aggressive activity. Each zone has type (Buy/Sell), status (Fresh/Retested/Held/Failed), price range, volume, and delta. Key concept: 'never fade a held zone.'"
    )]
    async fn get_rebid_reoffer_zones(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        if let Ok(pipelines) = self.pipelines.try_lock() {
            let active: Vec<serde_json::Value> = pipelines
                .rebid_reoffer
                .active_zones()
                .iter()
                .map(|z| {
                    serde_json::json!({
                        "zoneType": z.zone_type,
                        "status": z.status,
                        "high": z.high,
                        "low": z.low,
                        "mid": z.mid(),
                        "volume": z.volume,
                        "delta": z.delta,
                        "timestampMs": z.timestamp_ms,
                    })
                })
                .collect();
            return Ok(text_result(serde_json::json!({
                "activeZones": active,
                "activeZoneCount": active.len(),
                "dataAgeMs": compute_data_age(&db)
            })));
        }
        match db.latest_feature_state() {
            Ok(Some(s)) => Ok(text_result(serde_json::json!({
                "activeZoneCount": s.get("activeZoneCount"),
                "note": "Falling back to DB snapshot. Zone details not available.",
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data("No rebid/reoffer data available")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Recent delta momentum reversal ('pinch') events: when heavy one-sided delta is suddenly met by fast opposing flow, causing inventory to shift. Each event has timeframe (1m/5m/15m/30m), severity score (0-5), pre/post delta, price at pinch, and price displacement."
    )]
    async fn get_pinch_events(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        if let Ok(pipelines) = self.pipelines.try_lock() {
            let events = pipelines.pinch.recent_events();
            let event_data: Vec<serde_json::Value> = events
                .iter()
                .map(|e| serde_json::to_value(e).unwrap_or_default())
                .collect();
            return Ok(text_result(serde_json::json!({
                "events": event_data,
                "count": events.len(),
                "dataAgeMs": compute_data_age(&db)
            })));
        }
        match db.latest_feature_state() {
            Ok(Some(s)) => Ok(text_result(serde_json::json!({
                "pinchEventCount": s.get("pinchEventCount"),
                "note": "Falling back to DB snapshot. Event details not available.",
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data("No pinch data available")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Cross-session delta inventory: whether current session is Building (extending prior direction), Clearing (opposing prior direction), or Neutral. Direction: Long/Short/Flat. Includes consecutive sessions with same-direction delta (trend count) and DNP shift (how much the delta neutral pivot has migrated from prior session)."
    )]
    async fn get_session_inventory(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        if let Ok(pipelines) = self.pipelines.try_lock() {
            let inv = &pipelines.session_inventory;
            return Ok(text_result(serde_json::json!({
                "inventoryState": inv.state(),
                "inventoryDirection": inv.direction(),
                "sessionsInTrend": inv.sessions_in_trend(),
                "dnpShift": inv.dnp_shift(),
                "dataAgeMs": compute_data_age(&db)
            })));
        }
        match db.latest_feature_state() {
            Ok(Some(s)) => Ok(text_result(serde_json::json!({
                "inventoryState": s.get("inventoryState"),
                "inventoryDirection": s.get("inventoryDirection"),
                "sessionsInTrend": s.get("sessionsInTrend"),
                "note": "Falling back to DB snapshot. DNP shift not available.",
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data("No session inventory data available")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Delta at a specific price level from the delta profile. Returns signed delta at that price, buy/sell confirmation, and the top N prices by absolute delta magnitude (where conviction is concentrated). Omit price to use current price."
    )]
    async fn get_delta_at_price(
        &self,
        Parameters(params): Parameters<DeltaAtPriceParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        if let Ok(pipelines) = self.pipelines.try_lock() {
            let price = params.price.unwrap_or(pipelines.levels.last_price);
            let top_n = params.top_n.unwrap_or(10);
            let delta = pipelines.delta.delta_at_price(price);
            let confirms_buy = pipelines.delta.delta_confirmation_at_price(price, true);
            let confirms_sell = pipelines.delta.delta_confirmation_at_price(price, false);

            // Top N prices by absolute delta
            let mut profile = pipelines.delta.profile();
            profile.sort_by(|a, b| {
                b.1.abs()
                    .partial_cmp(&a.1.abs())
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            let top: Vec<serde_json::Value> = profile
                .iter()
                .take(top_n)
                .map(|(p, d)| {
                    serde_json::json!({
                        "price": p,
                        "delta": d,
                    })
                })
                .collect();

            return Ok(text_result(serde_json::json!({
                "price": price,
                "deltaAtPrice": delta,
                "confirmsBuy": confirms_buy,
                "confirmsSell": confirms_sell,
                "sessionDelta": pipelines.delta.session_delta(),
                "topPricesByDelta": top,
                "dataAgeMs": compute_data_age(&db)
            })));
        }
        Ok(no_data(
            "Delta at price requires live pipeline. Pipeline not available.",
        ))
    }

    #[tool(
        description = "Check delta confirmation at session level and at a specific price level. Returns whether session delta and price-level delta both support the trade direction. Use before trade entry for Stowe's 'execution requires delta confirmation'."
    )]
    async fn check_delta_confirmation(
        &self,
        Parameters(params): Parameters<DeltaConfirmParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let is_buy = params.is_buy_setup.unwrap_or(true);

        // Try pipeline for price-level delta (try_lock to avoid blocking)
        if let Ok(pipelines) = self.pipelines.try_lock() {
            let session_delta = pipelines.delta.session_delta();
            let session_confirms = if is_buy {
                session_delta > 0.0
            } else {
                session_delta < 0.0
            };
            let price = params.price.unwrap_or(pipelines.levels.last_price);
            let price_delta = pipelines.delta.delta_at_price(price);
            let price_confirms = pipelines.delta.delta_confirmation_at_price(price, is_buy);
            return Ok(text_result(serde_json::json!({
                "sessionDeltaConfirms": session_confirms,
                "sessionDelta": session_delta,
                "priceLevelDeltaConfirms": price_confirms,
                "deltaAtPrice": price_delta,
                "price": price,
                "bothConfirm": session_confirms && price_confirms,
                "direction": if is_buy { "long" } else { "short" },
                "dataAgeMs": compute_data_age(&db)
            })));
        }

        // Fallback: session-level only
        match db.latest_feature_state() {
            Ok(Some(s)) => {
                let session_delta = s
                    .get("sessionDelta")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0);
                let confirmed = if is_buy {
                    session_delta > 0.0
                } else {
                    session_delta < 0.0
                };
                Ok(text_result(serde_json::json!({
                    "sessionDeltaConfirms": confirmed,
                    "sessionDelta": session_delta,
                    "direction": if is_buy { "long" } else { "short" },
                    "note": "Price-level delta not available (pipeline not live).",
                    "dataAgeMs": compute_data_age(&db)
                })))
            }
            Ok(None) => Ok(no_data("No delta data available")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Full setup context for a named setup. Returns all computed data relevant to that setup type: OR5 levels, delta confirmation, RVOL, day type, nearby zones, risk state. One call = everything needed to discuss a potential trade."
    )]
    async fn get_setup_context(
        &self,
        Parameters(params): Parameters<SetupContextParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let snapshot = self
            .live_snapshot()
            .or_else(|| db.latest_feature_state().ok().flatten());
        let dom_feature = db.latest_dom_feature_state().ok().flatten();
        let risk = db.load_risk_state().ok().flatten();
        let setup_name = params.setup_name.unwrap_or_default();
        let last_price = snapshot
            .as_ref()
            .and_then(|s| s.get("lastPrice"))
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let mut nearby_levels = Vec::new();
        if let Some(snapshot) = snapshot.as_ref() {
            let level_keys = [
                ("priorDayHigh", "PriorDayHigh"),
                ("priorDayLow", "PriorDayLow"),
                ("priorVaHigh", "PriorVaHigh"),
                ("priorVaLow", "PriorVaLow"),
                ("priorPoc", "PriorPoc"),
                ("overnightHigh", "OvernightHigh"),
                ("overnightLow", "OvernightLow"),
                ("ibHigh", "IbHigh"),
                ("ibLow", "IbLow"),
                ("orHigh", "OrHigh"),
                ("orLow", "OrLow"),
                ("or5Mid", "Or5Mid"),
                ("poc", "Poc"),
                ("vaHigh", "VaHigh"),
                ("vaLow", "VaLow"),
                ("dnvaHigh", "DnvaHigh"),
                ("dnvaLow", "DnvaLow"),
            ];
            for (key, label) in level_keys {
                if let Some(price) = snapshot.get(key).and_then(|v| v.as_f64()) {
                    let distance_ticks = ((last_price - price) / 0.25).abs();
                    if distance_ticks <= 20.0 {
                        nearby_levels.push(serde_json::json!({
                            "level": label,
                            "price": price,
                            "distanceTicks": distance_ticks
                        }));
                    }
                }
            }
            nearby_levels.sort_by(|a, b| {
                a["distanceTicks"]
                    .as_f64()
                    .unwrap_or(f64::MAX)
                    .partial_cmp(&b["distanceTicks"].as_f64().unwrap_or(f64::MAX))
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }

        Ok(text_result(serde_json::json!({
            "setupName": setup_name,
            "marketSnapshot": snapshot,
            "domSummary": dom_feature.as_ref().and_then(|(_, payload)| payload.get("domSummary")).cloned(),
            "domFeature": dom_feature.as_ref().map(|(_, payload)| payload.clone()),
            "recentPullStackSummary": dom_feature.as_ref().and_then(|(_, payload)| payload.get("activity")).cloned(),
            "nearbyLevelReactionContext": nearby_levels,
            "riskState": risk,
            "dataAgeMs": compute_data_age(&db),
            "guidance": "Your playbook defines this setup. Evaluate all conditions before entry."
        })))
    }

    #[tool(
        description = "Which key levels is price currently near (within specified tick distance). Returns levels sorted by distance ascending. Includes prior day H/L/C, VA/POC, overnight (Globex), Globex OR30, London OR60, IB, OR5 mid, and IB extensions. Response includes sessionType/sessionSegment/tradingDay."
    )]
    async fn get_proximity_report(
        &self,
        Parameters(params): Parameters<ProximityParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        match db.latest_feature_state() {
            Ok(Some(s)) => {
                let last_price = s.get("lastPrice").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let max_ticks = params.max_distance_ticks.unwrap_or(20.0);

                let mut levels = Vec::new();
                let level_keys = [
                    ("priorDayHigh", "PriorDayHigh"),
                    ("priorDayLow", "PriorDayLow"),
                    ("priorDayClose", "PriorDayClose"),
                    ("priorVaHigh", "PriorVaHigh"),
                    ("priorVaLow", "PriorVaLow"),
                    ("priorPoc", "PriorPoc"),
                    ("overnightHigh", "OvernightHigh"),
                    ("overnightLow", "OvernightLow"),
                    ("globexOr30High", "GlobexOr30High"),
                    ("globexOr30Low", "GlobexOr30Low"),
                    ("londonOr60High", "LondonOr60High"),
                    ("londonOr60Low", "LondonOr60Low"),
                    ("ibHigh", "IbHigh"),
                    ("ibLow", "IbLow"),
                    ("orHigh", "OrHigh"),
                    ("orLow", "OrLow"),
                    ("or5Mid", "Or5Mid"),
                    ("poc", "Poc"),
                    ("vaHigh", "VaHigh"),
                    ("vaLow", "VaLow"),
                    ("dnvaHigh", "DnvaHigh"),
                    ("dnvaLow", "DnvaLow"),
                    ("dnp", "Dnp"),
                ];
                for (key, label) in &level_keys {
                    if let Some(price) = s.get(*key).and_then(|v| v.as_f64()) {
                        if price > 0.0 {
                            let dist = ((last_price - price) / 0.25).abs();
                            if dist <= max_ticks {
                                levels.push(serde_json::json!({
                                    "level": label,
                                    "price": price,
                                    "distanceTicks": dist,
                                }));
                            }
                        }
                    }
                }
                levels.sort_by(|a, b| {
                    let da = a["distanceTicks"].as_f64().unwrap_or(f64::MAX);
                    let db_val = b["distanceTicks"].as_f64().unwrap_or(f64::MAX);
                    da.partial_cmp(&db_val).unwrap_or(std::cmp::Ordering::Equal)
                });
                let session_type = s
                    .get("sessionType")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Unknown");
                let session_segment = s
                    .get("sessionSegment")
                    .and_then(|v| v.as_str())
                    .unwrap_or("None");
                Ok(text_result(serde_json::json!({
                    "sessionType": session_type,
                    "sessionSegment": session_segment,
                    "tradingDay": s.get("tradingDay"),
                    "lastPrice": last_price,
                    "maxDistanceTicks": max_ticks,
                    "nearbyLevels": levels,
                    "dataAgeMs": compute_data_age(&db)
                })))
            }
            Ok(None) => Ok(no_data("No market data available for proximity report")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Validate data integrity: checks tick count, stream freshness, pipeline consistency invariants (POC within VA, VA contains ~70%% of TPOs, delta sum consistency), and session boundary correctness. Returns pass/fail status with details."
    )]
    async fn validate_data_integrity(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let pipelines = self.pipelines.lock().map_err(|_| {
            McpError::new(ErrorCode::INTERNAL_ERROR, "pipeline lock poisoned", None)
        })?;
        let tick_count = db.raw_tick_count().unwrap_or(0);
        let last_ts = db.latest_tick_timestamp_ms().ok().flatten();
        let now_ms = chrono::Utc::now().timestamp_millis() as f64;
        let age_ms = last_ts.map(|v| now_ms - v).unwrap_or(f64::INFINITY);
        let stream_fresh = age_ms.is_finite() && age_ms <= FRESHNESS_THRESHOLD_MS;

        let mut checks = serde_json::json!({
            "rawTicksPresent": tick_count > 0,
            "streamFresh": stream_fresh,
            "freshnessThresholdMs": FRESHNESS_THRESHOLD_MS,
        });
        let mut invariants_ok = true;
        for (name, passed, detail) in pipelines.validate_invariants() {
            checks[name] = serde_json::json!({
                "passed": passed,
                "detail": detail
            });
            invariants_ok &= passed;
        }
        let status = if tick_count == 0 || !stream_fresh || !invariants_ok {
            "warning"
        } else {
            "ok"
        };

        let result = serde_json::json!({
            "status": status,
            "tickCount": tick_count,
            "lastTickAgeMs": if age_ms.is_finite() { age_ms } else { -1.0 },
            "checks": checks
        });

        let _ = db.insert_validation_run(now_ms, status, &result);

        Ok(text_result(result))
    }
}

#[tool_handler]
impl ServerHandler for TheDeskMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            instructions: Some(
                "The Desk - AI trading co-pilot backend for NQ futures. \
                 Provides real-time market structure (VWAP, TPO, Delta), \
                 microstructure analytics (tape pace, footprint, absorption), \
                 and playbook evaluation via Sierra Chart .scid data. \
                 All coaching frames as 'your playbook says...' -- never advisory."
                    .into(),
            ),
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            ..Default::default()
        }
    }
}

fn data_dir() -> std::path::PathBuf {
    let home = std::env::var("USERPROFILE")
        .or_else(|_| std::env::var("HOME"))
        .unwrap_or_else(|_| ".".to_string());
    let dir = std::path::PathBuf::from(home).join(".the-desk");
    std::fs::create_dir_all(&dir).ok();
    dir
}

fn compute_data_age(db: &Database) -> f64 {
    let now_ms = chrono::Utc::now().timestamp_millis() as f64;
    db.latest_tick_timestamp_ms()
        .ok()
        .flatten()
        .map(|ts| now_ms - ts)
        .unwrap_or(-1.0)
}

fn persist_integrity_check(db: &Database, pipelines: &PipelineEngine) {
    let tick_count = db.raw_tick_count().unwrap_or(0);
    let last_ts = db.latest_tick_timestamp_ms().ok().flatten();
    let now_ms = chrono::Utc::now().timestamp_millis() as f64;
    let age_ms = last_ts.map(|v| now_ms - v).unwrap_or(f64::INFINITY);
    let stream_fresh = age_ms.is_finite() && age_ms <= FRESHNESS_THRESHOLD_MS;

    let mut checks = serde_json::Map::new();
    checks.insert(
        "rawTicksPresent".to_string(),
        serde_json::json!(tick_count > 0),
    );
    checks.insert("streamFresh".to_string(), serde_json::json!(stream_fresh));
    checks.insert(
        "freshnessThresholdMs".to_string(),
        serde_json::json!(FRESHNESS_THRESHOLD_MS),
    );
    for (name, passed, detail) in pipelines.validate_invariants() {
        checks.insert(
            name,
            serde_json::json!({
                "passed": passed,
                "detail": detail
            }),
        );
    }

    let invariants_ok = checks
        .iter()
        .filter_map(|(_, v)| v.get("passed").and_then(|p| p.as_bool()))
        .all(|p| p);
    let status = if tick_count == 0 || !stream_fresh || !invariants_ok {
        "warning"
    } else {
        "ok"
    };
    let result = serde_json::json!({
        "status": status,
        "tickCount": tick_count,
        "lastTickAgeMs": if age_ms.is_finite() { age_ms } else { -1.0 },
        "checks": checks
    });
    let _ = db.insert_validation_run(now_ms, status, &result);
}

/// Process a single tick through the pipeline engine, event detector, rules engine, and outcome tracker.
#[allow(clippy::too_many_arguments)]
fn process_tick(
    pipelines: &Arc<Mutex<PipelineEngine>>,
    detector: &Arc<Mutex<EventDetector>>,
    flow_emitter: &Arc<Mutex<FlowEventEmitter>>,
    rules: &Arc<Mutex<RulesEngine>>,
    db: &Arc<Mutex<Database>>,
    last_bid: &Arc<Mutex<f64>>,
    last_ask: &Arc<Mutex<f64>>,
    price: f64,
    volume: f64,
    is_buy: bool,
    timestamp_ms: f64,
    bid: f64,
    ask: f64,
    event_buffer: &mut Vec<the_desk_backend::pipelines::MarketEvent>,
) {
    let session_type = et_minutes_from_timestamp(timestamp_ms)
        .map(classify_session)
        .unwrap_or(if minute_of_session_from_timestamp(timestamp_ms) < 0 {
            SessionType::Globex
        } else {
            SessionType::Rth
        });
    if session_type == SessionType::Unknown {
        return;
    }
    let minute = minute_of_session_from_timestamp(timestamp_ms);
    let (snapshot, _session_date) = {
        if let Ok(mut p) = pipelines.lock() {
            p.on_trade_with_timestamp(price, volume, is_buy, minute, timestamp_ms);

            let cur_bid = if bid > 0.0 { bid } else { price - 0.25 };
            let cur_ask = if ask > 0.0 { ask } else { price + 0.25 };
            let snapshot = p.snapshot(cur_bid, cur_ask);
            let session_date = session_date_from_timestamp_ms(timestamp_ms);

            // Structural events (level tests, IB extensions, day type changes, etc.)
            if let Ok(mut det) = detector.lock() {
                det.detect_into(&snapshot, timestamp_ms, &session_date, minute, event_buffer);
            }

            // Flow events (absorption, pinch, acceleration zones, large trade clusters)
            if let Ok(mut fe) = flow_emitter.lock() {
                fe.detect_into(&p, timestamp_ms, &session_date, price, event_buffer);
            }

            (snapshot, session_date)
        } else {
            return;
        }
    };

    // Rules engine: evaluate setups and fire alerts (outside pipeline lock to avoid deadlock)
    let setups = db
        .lock()
        .ok()
        .and_then(|d| d.list_setups().ok())
        .unwrap_or_default();
    let risk_at_limit = db
        .lock()
        .ok()
        .and_then(|d| d.load_risk_state().ok().flatten())
        .map(|s| s.at_limit)
        .unwrap_or(false);
    if let Ok(mut r) = rules.lock() {
        for setup in &setups {
            if let Some(alert) = r.evaluate(setup, &snapshot, risk_at_limit) {
                if let Ok(d) = db.lock() {
                    let _ = d.insert_playbook_signal(
                        timestamp_ms,
                        &alert.setup_id,
                        &serde_json::to_value(&alert).unwrap_or_else(|_| serde_json::json!({})),
                    );
                    let signal_id = format!("{}_{}", alert.setup_id, timestamp_ms as u64);
                    let outcome = SignalOutcome {
                        signal_id: signal_id.clone(),
                        setup_id: alert.setup_id.clone(),
                        setup_name: Some(alert.setup_name.clone()),
                        session_date: session_date_from_timestamp_ms(timestamp_ms),
                        source: "live".to_string(),
                        job_id: None,
                        fired_at_ms: timestamp_ms,
                        fired_price: alert.current_price,
                        target_price: alert.target_prices.first().copied(),
                        stop_price: alert.stop_price,
                        outcome: "pending".to_string(),
                        outcome_at_ms: None,
                        max_favorable_excursion: None,
                        max_adverse_excursion: None,
                        r_result: None,
                        time_to_outcome_ms: None,
                        rvol_at_fire: Some(snapshot.rvol_ratio),
                        rvol_bucket_at_fire: Some(snapshot.rvol_bucket_index as i32),
                    };
                    let _ = d.insert_signal_outcome(&outcome);
                }
            }
        }
        r.update_prev_market(&snapshot);
    }

    // Outcome tracker: update MFE/MAE and resolve signals
    if let Ok(d) = db.lock() {
        let _ = outcome_tracker::on_tick(&d, price, timestamp_ms, None);
    }

    // Flush event buffer periodically
    if event_buffer.len() >= 50 {
        if let Ok(d) = db.lock() {
            let _ = d.insert_market_events_batch(event_buffer);
        }
        event_buffer.clear();
    }
    if bid > 0.0 {
        if let Ok(mut b) = last_bid.lock() {
            *b = bid;
        }
    }
    if ask > 0.0 {
        if let Ok(mut a) = last_ask.lock() {
            *a = ask;
        }
    }
}

fn persist_dom_summary_into_feature_state(
    db: &Database,
    pipelines: &Arc<Mutex<PipelineEngine>>,
    last_bid: &Arc<Mutex<f64>>,
    last_ask: &Arc<Mutex<f64>>,
    timestamp_ms: f64,
    dom_summary: &DomSummary,
) {
    let payload = {
        let bid = last_bid.lock().ok().map(|v| *v).unwrap_or_default();
        let ask = last_ask.lock().ok().map(|v| *v).unwrap_or_default();
        if bid > 0.0 || ask > 0.0 {
            pipelines.lock().ok().map(|p| {
                serde_json::to_value(p.snapshot(bid.max(0.0), ask.max(0.0))).unwrap_or_default()
            })
        } else {
            Some(merge_dom_summary_into_snapshot(
                db.latest_feature_state().ok().flatten(),
                dom_summary,
            ))
        }
    };
    if let Some(payload) = payload {
        let _ = db.upsert_feature_state(timestamp_ms, &payload);
    }
}

#[allow(clippy::too_many_arguments)]
fn persist_depth_records(
    db: &Arc<Mutex<Database>>,
    pipelines: &Arc<Mutex<PipelineEngine>>,
    last_bid: &Arc<Mutex<f64>>,
    last_ask: &Arc<Mutex<f64>>,
    reader: &DepthReader,
    book: &DepthBook,
    records: &[the_desk_backend::depth::DepthRecord],
    batch_id: &mut i64,
) {
    if records.is_empty() {
        return;
    }
    let Some(last_record) = records.last() else {
        return;
    };
    let source_file = reader.path().to_string_lossy().to_string();
    let trading_day = session_date_from_timestamp_ms(last_record.timestamp_ms);
    if let Ok(mut db) = db.lock() {
        if let Ok(next_batch_id) = db.insert_depth_events_batch(&source_file, records, *batch_id) {
            *batch_id = next_batch_id;
        }
        let snapshot = book.snapshot(&source_file, last_record.timestamp_ms, 10);
        let snapshot_json = serde_json::to_value(&snapshot).unwrap_or_default();
        let _ = db.insert_dom_snapshot(
            &source_file,
            last_record.timestamp_ms,
            &trading_day,
            &snapshot_json,
        );

        let feature_window_start = (last_record.timestamp_ms - 60_000.0).max(0.0);
        let feature = {
            let config = load_feed_config();
            aggregate_window_trades(&config, feature_window_start, last_record.timestamp_ms)
                .ok()
                .and_then(|trades| {
                    reader
                        .summarize_window(
                            feature_window_start,
                            last_record.timestamp_ms,
                            &trades,
                            None,
                            None,
                        )
                        .ok()
                })
                .map(|activity| build_dom_feature_snapshot(&snapshot, activity))
                .unwrap_or_else(|| DomFeatureSnapshot {
                    source_file: source_file.clone(),
                    timestamp_ms: snapshot.snapshot_timestamp_ms,
                    session_date: snapshot.session_date.clone(),
                    dom_summary: build_dom_summary(
                        &snapshot,
                        &the_desk_backend::depth::PullStackActivitySummary {
                            source_file: source_file.clone(),
                            start_time_ms: feature_window_start,
                            end_time_ms: last_record.timestamp_ms,
                            session_date: snapshot.session_date.clone(),
                            record_count: records.len(),
                            batch_count: records.iter().filter(|r| r.end_of_batch).count(),
                            bid: Default::default(),
                            ask: Default::default(),
                            top_pull_levels: Vec::new(),
                            top_stack_levels: Vec::new(),
                        },
                    ),
                    activity: the_desk_backend::depth::PullStackActivitySummary {
                        source_file: source_file.clone(),
                        start_time_ms: feature_window_start,
                        end_time_ms: last_record.timestamp_ms,
                        session_date: snapshot.session_date.clone(),
                        record_count: records.len(),
                        batch_count: records.iter().filter(|r| r.end_of_batch).count(),
                        bid: Default::default(),
                        ask: Default::default(),
                        top_pull_levels: Vec::new(),
                        top_stack_levels: Vec::new(),
                    },
                })
        };
        let feature_json = serde_json::to_value(&feature).unwrap_or_default();
        let _ = db.insert_dom_feature_snapshot(
            &source_file,
            feature.timestamp_ms,
            &trading_day,
            &feature_json,
        );
        if let Ok(mut pipelines) = pipelines.lock() {
            pipelines.set_dom_summary(Some(feature.dom_summary.clone()));
        }
        persist_dom_summary_into_feature_state(
            &db,
            pipelines,
            last_bid,
            last_ask,
            feature.timestamp_ms,
            &feature.dom_summary,
        );
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = data_dir().join("data.db");
    let db = Database::open(&db_path.to_string_lossy())?;

    let mut pipelines = PipelineEngine::new();
    if let Ok(volumes) = db.recent_rth_session_volumes(20) {
        let curves: Vec<Vec<f64>> = volumes
            .into_iter()
            .map(RvolPipeline::curve_from_total_volume)
            .collect();
        pipelines.rvol.load_historical_curve(&curves);
    }

    let config = load_feed_config();
    let reader = ScidReader::from_feed_config(&config);
    let scid_available = reader.path().exists();

    // Create the server immediately so stdio is ready before backfill starts.
    // The startup backfill runs in a background task and populates pipeline
    // state concurrently with tool serving.
    let server = TheDeskMcp::new(db, pipelines, db_path.to_string_lossy().to_string());

    if scid_available {
        // Spawn background startup backfill from 2 Globex opens ago.
        // Clones the shared Arcs from the server so the backfill can update
        // pipeline and DB state without blocking the MCP listener.
        let pipelines_startup = Arc::clone(&server.pipelines);
        let flow_emitter_startup = Arc::clone(&server.flow_emitter);
        let db_startup = Arc::clone(&server.db);
        let reader_startup = reader.clone();

        tokio::spawn(async move {
            let since = globex_open_ms(2);
            eprintln!(
                "[the-desk-mcp] Backfilling from {} ...",
                reader_startup.path().display()
            );
            let ticks = match reader_startup.read_bulk_since(Some(since)) {
                Ok(t) if !t.is_empty() => t,
                Ok(_) => {
                    eprintln!("[the-desk-mcp] No ticks since prior Globex open");
                    return;
                }
                Err(e) => {
                    eprintln!("[the-desk-mcp] Backfill error: {e}");
                    return;
                }
            };

            // Hold pipeline lock only during tick processing. Release pipelines
            // before acquiring db at boundaries to avoid deadlock and let DB-only
            // tools (e.g. get_feed_health) run while backfill proceeds.
            let mut pipelines = match pipelines_startup.lock() {
                Ok(p) => p,
                Err(_) => return,
            };

            let mut current_session = SessionType::Unknown;
            let mut current_delta_segment = DeltaSegment::Unknown;
            let mut boundary_count = 0u32;

            for tick in &ticks {
                if let Some(et_min) = et_minutes_from_timestamp(tick.timestamp_ms) {
                    let new_session = classify_session(et_min);
                    let new_segment = classify_delta_segment(et_min);

                    if new_session != current_session
                        && current_session != SessionType::Unknown
                        && new_session != SessionType::Unknown
                    {
                        let end_state = if current_session == SessionType::Rth {
                            Some(pipelines.session_end_state())
                        } else {
                            None
                        };
                        let date = session_date_from_timestamp_ms(tick.timestamp_ms);
                        let today_str = session_date_from_timestamp_ms(tick.timestamp_ms);
                        drop(pipelines);
                        if let Some(ref es) = end_state {
                            if let Ok(db) = db_startup.lock() {
                                let _ = db.save_prior_day_full_with_dnva(
                                    &date,
                                    es.high,
                                    es.low,
                                    es.close,
                                    es.va_high,
                                    es.va_low,
                                    es.poc,
                                    Some(es.dnva_high),
                                    Some(es.dnva_low),
                                    Some(es.dnp),
                                );
                                eprintln!(
                                    "[the-desk-mcp] Session boundary: RTH\u{2192}Globex, saved prior day H={:.2} L={:.2} C={:.2}",
                                    es.high, es.low, es.close
                                );
                            }
                        }
                        let prior = {
                            if let Ok(db) = db_startup.lock() {
                                db.load_prior_day_full(&today_str)
                            } else {
                                Ok(None)
                            }
                        };
                        pipelines = match pipelines_startup.lock() {
                            Ok(p) => p,
                            Err(_) => return,
                        };
                        pipelines.reset_session_with_type(new_session == SessionType::Globex);
                        if new_session == SessionType::Rth || new_session == SessionType::Globex {
                            if let Ok(Some((h, l, c, va_h, va_l, poc, dnva_h, dnva_l, dnp))) = prior
                            {
                                pipelines.levels.set_prior_day(h, l, c);
                                if let (Some(vh), Some(vl), Some(pc)) = (va_h, va_l, poc) {
                                    pipelines.levels.set_prior_profile(vh, vl, pc);
                                }
                                if let (Some(dh), Some(dl), Some(dp)) = (dnva_h, dnva_l, dnp) {
                                    pipelines.levels.set_prior_dnva(dh, dl, dp);
                                }
                            }
                        }
                        let inv_session_type = if new_session == SessionType::Rth {
                            "RTH"
                        } else if new_segment == DeltaSegment::Asia {
                            "Asia"
                        } else {
                            "London"
                        };
                        drop(pipelines);
                        if let Ok(db) = db_startup.lock() {
                            if let Ok(summaries) = db.list_session_summaries(
                                None,
                                None,
                                None,
                                Some(inv_session_type),
                                5,
                            ) {
                                let prior_inv: Vec<PriorSessionData> = summaries
                                    .into_iter()
                                    .filter(|s| {
                                        s.dnva_high > 0.0 && s.dnva_low > 0.0 && s.dnp > 0.0
                                    })
                                    .map(|s| PriorSessionData {
                                        final_delta: s.session_delta,
                                        dnva_high: s.dnva_high,
                                        dnva_low: s.dnva_low,
                                        dnp: s.dnp,
                                    })
                                    .collect();
                                if let Ok(mut p) = pipelines_startup.lock() {
                                    p.session_inventory.load_prior_sessions(prior_inv);
                                }
                            }
                        }
                        pipelines = match pipelines_startup.lock() {
                            Ok(p) => p,
                            Err(_) => return,
                        };
                        boundary_count += 1;
                    } else if new_segment != current_delta_segment
                        && current_delta_segment != DeltaSegment::Unknown
                        && new_segment != DeltaSegment::Unknown
                    {
                        pipelines.reset_segment(new_segment);
                        boundary_count += 1;
                    }

                    if new_session != SessionType::Unknown {
                        current_session = new_session;
                    }
                    if new_segment != DeltaSegment::Unknown {
                        current_delta_segment = new_segment;
                    }
                }

                let is_buy = matches!(tick.side, TradeSide::Buy);
                let minute = minute_of_session_from_timestamp(tick.timestamp_ms);
                pipelines.on_trade_with_timestamp(
                    tick.price,
                    tick.volume,
                    is_buy,
                    minute,
                    tick.timestamp_ms,
                );
            }

            let last = ticks.last().unwrap();
            let bid = if last.bid > 0.0 {
                last.bid
            } else {
                last.price - 0.25
            };
            let ask = if last.ask > 0.0 {
                last.ask
            } else {
                last.price + 0.25
            };
            let snapshot = pipelines.snapshot(bid, ask);

            // Sync flow emitter counts so live polling doesn't emit stale events
            if let Ok(mut fe) = flow_emitter_startup.lock() {
                fe.sync_counts(&pipelines);
            }
            drop(pipelines);
            if let Ok(db) = db_startup.lock() {
                let _ = db.upsert_feature_state(
                    last.timestamp_ms,
                    &serde_json::to_value(&snapshot).unwrap_or_default(),
                );
            }
            eprintln!(
                "[the-desk-mcp] Backfill complete: {} ticks, {} session boundaries, last price {:.2}",
                ticks.len(),
                boundary_count,
                last.price
            );
        });
    } else {
        eprintln!(
            "[the-desk-mcp] SCID file not found: {}",
            reader.path().display()
        );
    }

    // Background: poll .scid for new ticks and update pipeline engine + DB
    if scid_available {
        let pipelines_bg = Arc::clone(&server.pipelines);
        let detector_bg = Arc::clone(&server.detector);
        let flow_emitter_bg = Arc::clone(&server.flow_emitter);
        let rules_bg = Arc::clone(&server.rules);
        let last_bid_bg = Arc::clone(&server.last_bid);
        let last_ask_bg = Arc::clone(&server.last_ask);
        let db_bg = Arc::clone(&server.db);
        let poll_ms = config.flush_poll_ms;
        let price_scale = config.price_scale;
        let reader_path = reader.path().to_path_buf();

        tokio::spawn(async move {
            use std::io::{Read, Seek, SeekFrom};
            use tokio::time::{sleep, Duration};

            let poll = Duration::from_millis(poll_ms.max(250));
            let mut offset: u64 = 0;
            let mut persist_counter: u64 = 0;
            let mut event_buffer = Vec::new();
            let mut tick_buffer: Vec<(f64, f64, f64, f64, f64, bool, String)> = Vec::new();
            let mut last_integrity_check =
                std::time::Instant::now() - std::time::Duration::from_secs(30);
            // Seed current session and segment from the system clock so we can detect boundaries.
            let now_et = et_minutes_from_timestamp(chrono::Utc::now().timestamp_millis() as f64);
            let mut current_session = now_et.map(classify_session).unwrap_or(SessionType::Unknown);
            let mut current_delta_segment = now_et
                .map(classify_delta_segment)
                .unwrap_or(DeltaSegment::Unknown);

            // Seek to current EOF so we only process NEW ticks
            if let Ok(f) = std::fs::File::open(&reader_path) {
                offset = f.metadata().map(|m| m.len()).unwrap_or(56);
            }

            loop {
                sleep(poll).await;
                if last_integrity_check.elapsed() >= std::time::Duration::from_secs(15) {
                    if let (Ok(db), Ok(p)) = (db_bg.lock(), pipelines_bg.lock()) {
                        persist_integrity_check(&db, &p);
                    }
                    last_integrity_check = std::time::Instant::now();
                }

                let mut file = match std::fs::File::open(&reader_path) {
                    Ok(f) => f,
                    Err(_) => continue,
                };
                let len = file.metadata().map(|m| m.len()).unwrap_or(0);
                if len <= offset {
                    continue;
                }
                if file.seek(SeekFrom::Start(offset)).is_err() {
                    continue;
                }

                let mut record = [0_u8; 40];
                let mut ticks_this_poll = 0u64;
                while file.read_exact(&mut record).is_ok() {
                    offset += 40;
                    if let Some(tick) = parse_record_scaled(&record, price_scale) {
                        // Detect session and segment boundaries during live polling
                        if let Some(et_min) = et_minutes_from_timestamp(tick.timestamp_ms) {
                            let new_session = classify_session(et_min);
                            let new_segment = classify_delta_segment(et_min);

                            if new_session != current_session
                                && current_session != SessionType::Unknown
                                && new_session != SessionType::Unknown
                            {
                                if let Ok(mut p) = pipelines_bg.lock() {
                                    let end_state = if current_session == SessionType::Rth {
                                        Some(p.session_end_state())
                                    } else {
                                        None
                                    };
                                    let date = session_date_from_timestamp_ms(tick.timestamp_ms);
                                    if let Some(ref es) = end_state {
                                        if let Ok(db) = db_bg.lock() {
                                            let _ = db.save_prior_day_full_with_dnva(
                                                &date,
                                                es.high,
                                                es.low,
                                                es.close,
                                                es.va_high,
                                                es.va_low,
                                                es.poc,
                                                Some(es.dnva_high),
                                                Some(es.dnva_low),
                                                Some(es.dnp),
                                            );
                                        }
                                    }
                                    let today_str =
                                        session_date_from_timestamp_ms(tick.timestamp_ms);
                                    let prior = if let Ok(db) = db_bg.lock() {
                                        db.load_prior_day_full(&today_str).ok().flatten()
                                    } else {
                                        None
                                    };
                                    p.reset_session_with_type(new_session == SessionType::Globex);
                                    if let Some((h, l, c, va_h, va_l, poc, dnva_h, dnva_l, dnp)) =
                                        prior
                                    {
                                        p.levels.set_prior_day(h, l, c);
                                        if let (Some(vh), Some(vl), Some(pc)) = (va_h, va_l, poc) {
                                            p.levels.set_prior_profile(vh, vl, pc);
                                        }
                                        if let (Some(dh), Some(dl), Some(dp)) =
                                            (dnva_h, dnva_l, dnp)
                                        {
                                            p.levels.set_prior_dnva(dh, dl, dp);
                                        }
                                    }
                                    let inv_session_type = if new_session == SessionType::Rth {
                                        "RTH"
                                    } else if new_segment == DeltaSegment::Asia {
                                        "Asia"
                                    } else {
                                        "London"
                                    };
                                    drop(p);
                                    if let Ok(db) = db_bg.lock() {
                                        if let Ok(summaries) = db.list_session_summaries(
                                            None,
                                            None,
                                            None,
                                            Some(inv_session_type),
                                            5,
                                        ) {
                                            let prior_inv: Vec<PriorSessionData> = summaries
                                                .into_iter()
                                                .filter(|s| {
                                                    s.dnva_high > 0.0
                                                        && s.dnva_low > 0.0
                                                        && s.dnp > 0.0
                                                })
                                                .map(|s| PriorSessionData {
                                                    final_delta: s.session_delta,
                                                    dnva_high: s.dnva_high,
                                                    dnva_low: s.dnva_low,
                                                    dnp: s.dnp,
                                                })
                                                .collect();
                                            if let Ok(mut p) = pipelines_bg.lock() {
                                                p.session_inventory.load_prior_sessions(prior_inv);
                                            }
                                        }
                                    }
                                    eprintln!(
                                        "[the-desk-mcp] Live boundary: {:?} → {:?}",
                                        current_session, new_session
                                    );
                                }
                                if let Ok(mut det) = detector_bg.lock() {
                                    det.reset();
                                }
                                if let Ok(mut fe) = flow_emitter_bg.lock() {
                                    fe.reset();
                                }
                            } else if new_segment != current_delta_segment
                                && current_delta_segment != DeltaSegment::Unknown
                                && new_segment != DeltaSegment::Unknown
                            {
                                if let Ok(mut p) = pipelines_bg.lock() {
                                    p.reset_segment(new_segment);
                                    eprintln!(
                                        "[the-desk-mcp] Segment boundary: {:?} → {:?}",
                                        current_delta_segment, new_segment
                                    );
                                }
                            }

                            if new_session != SessionType::Unknown {
                                current_session = new_session;
                            }
                            if new_segment != DeltaSegment::Unknown {
                                current_delta_segment = new_segment;
                            }
                        }

                        let is_buy = matches!(tick.side, TradeSide::Buy);
                        process_tick(
                            &pipelines_bg,
                            &detector_bg,
                            &flow_emitter_bg,
                            &rules_bg,
                            &db_bg,
                            &last_bid_bg,
                            &last_ask_bg,
                            tick.price,
                            tick.volume,
                            is_buy,
                            tick.timestamp_ms,
                            tick.bid,
                            tick.ask,
                            &mut event_buffer,
                        );

                        let bid = if tick.bid > 0.0 {
                            tick.bid
                        } else {
                            tick.price - 0.25
                        };
                        let ask = if tick.ask > 0.0 {
                            tick.ask
                        } else {
                            tick.price + 0.25
                        };
                        let session_date = session_date_from_timestamp_ms(tick.timestamp_ms);
                        tick_buffer.push((
                            tick.timestamp_ms,
                            tick.price,
                            tick.volume,
                            bid,
                            ask,
                            is_buy,
                            session_date,
                        ));

                        if tick_buffer.len() >= 100 {
                            if let Ok(db) = db_bg.lock() {
                                let _ = db.insert_raw_ticks_batch(&tick_buffer);
                            }
                            tick_buffer.clear();
                        }

                        ticks_this_poll += 1;
                    }
                }

                // Flush remaining events
                if !event_buffer.is_empty() {
                    if let Ok(db) = db_bg.lock() {
                        let _ = db.insert_market_events_batch(&event_buffer);
                    }
                    event_buffer.clear();
                }

                // Flush remaining raw ticks
                if !tick_buffer.is_empty() {
                    if let Ok(db) = db_bg.lock() {
                        let _ = db.insert_raw_ticks_batch(&tick_buffer);
                    }
                    tick_buffer.clear();
                }

                // Persist snapshot periodically (every ~4 polls)
                if ticks_this_poll > 0 {
                    persist_counter += 1;
                    if persist_counter.is_multiple_of(4) {
                        if let (Ok(p), Ok(b), Ok(a), Ok(db)) = (
                            pipelines_bg.lock(),
                            last_bid_bg.lock(),
                            last_ask_bg.lock(),
                            db_bg.lock(),
                        ) {
                            let bid = if *b > 0.0 { *b } else { 0.0 };
                            let ask = if *a > 0.0 { *a } else { 0.0 };
                            if bid > 0.0 {
                                let snapshot = p.snapshot(bid, ask);
                                let ts = chrono::Utc::now().timestamp_millis() as f64;
                                let _ = db.upsert_feature_state(
                                    ts,
                                    &serde_json::to_value(&snapshot).unwrap_or_default(),
                                );
                            }
                        }
                    }
                }
            }
        });
    }

    if latest_depth_reader()?.is_some() {
        let pipelines_depth = Arc::clone(&server.pipelines);
        let db_depth = Arc::clone(&server.db);
        let last_bid_depth = Arc::clone(&server.last_bid);
        let last_ask_depth = Arc::clone(&server.last_ask);

        tokio::spawn(async move {
            let poll = Duration::from_millis(1_000);
            let mut active_path: Option<std::path::PathBuf> = None;
            let mut offset = 0_u64;
            let mut batch_id = 0_i64;
            let mut book = DepthBook::default();

            loop {
                let Some(reader) = latest_depth_reader().ok().flatten() else {
                    sleep(poll).await;
                    continue;
                };

                if active_path.as_deref() != Some(reader.path()) {
                    active_path = Some(reader.path().to_path_buf());
                    offset = reader.data_start_offset();
                    batch_id = 0;
                    book = DepthBook::default();
                }

                let mut new_records = Vec::<the_desk_backend::depth::DepthRecord>::new();
                match reader.scan_new_records(&mut offset, |record| {
                    book.apply(&record);
                    new_records.push(record);
                    Ok(DepthScanControl::Continue)
                }) {
                    Ok(_) => {
                        if !new_records.is_empty() {
                            persist_depth_records(
                                &db_depth,
                                &pipelines_depth,
                                &last_bid_depth,
                                &last_ask_depth,
                                &reader,
                                &book,
                                &new_records,
                                &mut batch_id,
                            );
                        }
                    }
                    Err(err) => {
                        eprintln!("[the-desk-mcp] Depth worker error: {err}");
                    }
                }

                sleep(poll).await;
            }
        });
    }

    let service = server.serve(stdio()).await?;
    service.waiting().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summary_row(
        session_date: &str,
        session_type: &str,
        dnva_high: f64,
        dnva_low: f64,
        dnp: f64,
    ) -> SessionSummary {
        SessionSummary {
            session_date: session_date.to_string(),
            session_type: session_type.to_string(),
            open_price: dnva_low,
            high: dnva_high,
            low: dnva_low,
            close: dnp,
            poc: dnp,
            vah: dnva_high,
            val: dnva_low,
            ib_high: 0.0,
            ib_low: 0.0,
            ib_range: 0.0,
            ib_mid: 0.0,
            or_high: 0.0,
            or_low: 0.0,
            day_type: String::new(),
            profile_shape: String::new(),
            balance_state: String::new(),
            total_volume: 0.0,
            tick_count: 0,
            session_delta: 0.0,
            cumulative_delta: 0.0,
            dnp,
            dnva_high,
            dnva_low,
            vwap_close: 0.0,
            signal_count: 0,
            single_prints_direction: String::new(),
            excess_high: false,
            excess_low: false,
            poor_high: false,
            poor_low: false,
            rvol_ratio: 0.0,
            close_vs_ib_mid: "n/a".to_string(),
            close_vs_vwap: "n/a".to_string(),
            close_vs_poc: "n/a".to_string(),
            snapshot_json: None,
        }
    }

    fn test_server() -> TheDeskMcp {
        let db = Database::open(":memory:").expect("db");
        TheDeskMcp::new(db, PipelineEngine::new(), ":memory:".into())
    }

    #[test]
    fn parse_setup_perf_sort_validates_values() {
        assert_eq!(
            parse_setup_perf_sort(None).expect("default"),
            SetupPerformanceSortBy::Resolved
        );
        assert_eq!(
            parse_setup_perf_sort(Some("winRate")).expect("winRate"),
            SetupPerformanceSortBy::WinRate
        );
        assert!(parse_setup_perf_sort(Some("bogus")).is_err());
    }

    #[test]
    fn build_session_scope_filter_validates_and_infers_segment() {
        let invalid = SessionScopeParams {
            session_type: Some("RTH".into()),
            session_segment: Some("Asia".into()),
            ..Default::default()
        };
        assert!(build_session_scope_filter(&invalid).is_err());

        let inferred = SessionScopeParams {
            session_segment: Some("London".into()),
            ..Default::default()
        };
        let scope = build_session_scope_filter(&inferred)
            .expect("scope")
            .expect("some");
        assert_eq!(scope.session_type.as_deref(), Some("Globex"));
        assert_eq!(scope.session_segment.as_deref(), Some("London"));
    }

    #[test]
    fn normalize_signal_source_validates_values() {
        assert_eq!(normalize_signal_source("live"), Some("live"));
        assert_eq!(normalize_signal_source("backtest"), Some("backtest"));
        assert_eq!(normalize_signal_source("paper"), None);
    }

    #[tokio::test]
    async fn dom_window_tool_returns_persisted_feature_snapshots() {
        let server = test_server();
        {
            let db = server.db.lock().expect("db lock");
            let payload = serde_json::json!({
                "domSummary": {
                    "liquidityBias": "bid_support",
                    "pullStackBias": 12.0
                },
                "activity": {
                    "topPullLevels": [],
                    "topStackLevels": []
                }
            });
            db.insert_dom_feature_snapshot("NQ.depth", 1_000.0, "2026-03-05", &payload)
                .expect("insert feature");
        }

        let result = server
            .get_dom_window(Parameters(DomWindowParams {
                start_time_ms: Some(900.0),
                end_time_ms: Some(1_100.0),
                price_low: None,
                price_high: None,
                limit: Some(10),
            }))
            .await
            .expect("tool call");

        let rendered = format!("{result:?}");
        assert!(rendered.contains("bid_support"));
    }

    #[tokio::test]
    async fn get_key_levels_rth_uses_same_day_asia_and_london_dnva() {
        let server = test_server();
        {
            let db = server.db.lock().expect("db lock");
            db.upsert_session_summary(&summary_row(
                "2026-03-05",
                "Asia",
                21010.0,
                20990.0,
                21000.0,
            ))
            .expect("insert asia");
            db.upsert_session_summary(&summary_row(
                "2026-03-05",
                "London",
                21025.0,
                21005.0,
                21015.0,
            ))
            .expect("insert london");
            db.upsert_feature_state(
                1_000.0,
                &serde_json::json!({
                    "sessionType": "RTH",
                    "sessionSegment": "None",
                    "tradingDay": "2026-03-05"
                }),
            )
            .expect("seed feature state");
        }

        let result = server.get_key_levels().await.expect("tool call");
        let rendered = format!("{result:?}");
        assert!(rendered.contains("priorAsiaDnvaHigh"));
        assert!(rendered.contains("21010.0"));
        assert!(rendered.contains("priorLondonDnvaHigh"));
        assert!(rendered.contains("21025.0"));
    }

    #[tokio::test]
    async fn get_key_levels_globex_london_uses_same_day_asia_and_prior_london() {
        let server = test_server();
        {
            let db = server.db.lock().expect("db lock");
            db.upsert_session_summary(&summary_row(
                "2026-03-05",
                "Asia",
                21030.0,
                21010.0,
                21020.0,
            ))
            .expect("insert asia same day");
            db.upsert_session_summary(&summary_row(
                "2026-03-04",
                "London",
                21040.0,
                21020.0,
                21030.0,
            ))
            .expect("insert london prior");
            db.upsert_session_summary(&summary_row(
                "2026-03-05",
                "London",
                21999.0,
                21990.0,
                21994.5,
            ))
            .expect("insert london same day");
            db.upsert_feature_state(
                1_000.0,
                &serde_json::json!({
                    "sessionType": "Globex",
                    "sessionSegment": "London",
                    "tradingDay": "2026-03-05"
                }),
            )
            .expect("seed feature state");
        }

        let result = server.get_key_levels().await.expect("tool call");
        let rendered = format!("{result:?}");
        assert!(rendered.contains("priorAsiaDnvaHigh"));
        assert!(rendered.contains("21030.0"));
        assert!(rendered.contains("priorLondonDnvaHigh"));
        assert!(rendered.contains("21040.0"));
    }
}
