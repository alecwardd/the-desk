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
    SessionScopeFilter, SignalOutcome, TradeRecord,
};
use the_desk_backend::feed::scid_reader::{parse_record_scaled, ScidReader};
use the_desk_backend::feed::{load_feed_config, load_storage_config, TradeSide};
use the_desk_backend::outcome_tracker;
use the_desk_backend::pipelines::{EventDetector, FlowEventEmitter, PipelineEngine, RvolPipeline};
use the_desk_backend::research;
use the_desk_backend::risk::{RiskConfig, RiskState, RiskTracker};
use the_desk_backend::rules::RulesEngine;
use the_desk_backend::{
    classify_session, et_minutes_from_timestamp, globex_open_ms, minute_of_session_from_timestamp,
    session_date_from_timestamp_ms, SessionType, GLOBEX_OPEN_ET, RTH_CLOSE_ET, RTH_OPEN_ET,
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
    /// Maximum number of ticks to return, most recent first (default 500).
    limit: Option<u64>,
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
    fn live_snapshot(&self) -> Option<serde_json::Value> {
        let pipelines = self.pipelines.lock().ok()?;
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
        if let Some(snapshot) = self.live_snapshot() {
            return Ok(text_result(serde_json::json!({
                "snapshot": snapshot,
                "source": "live_pipeline"
            })));
        }
        let db = self.db.lock().map_err(|_| lock_error())?;
        match db.latest_feature_state() {
            Ok(Some(snapshot)) => Ok(text_result(serde_json::json!({
                "snapshot": snapshot,
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
        description = "Delta profile: session cumulative delta, delta neutral value area (DNVA high/low), delta neutral pivot (DNP -- where cumulative delta crosses zero). Use for inventory and positioning analysis."
    )]
    async fn get_delta_profile(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        match db.latest_feature_state() {
            Ok(Some(s)) => Ok(text_result(serde_json::json!({
                "sessionDelta": s.get("sessionDelta"),
                "cumulativeDelta": s.get("cumulativeDelta"),
                "dnvaHigh": s.get("dnvaHigh"),
                "dnvaLow": s.get("dnvaLow"),
                "dnp": s.get("dnp"),
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
                let is_globex = s.get("sessionType").and_then(|v| v.as_str()) == Some("Globex");
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
                    "dataAgeMs": compute_data_age(&db)
                });
                if is_globex {
                    out["sessionScopeNote"] = serde_json::json!("For Globex, use overnightHigh/overnightLow as the session range. sessionHigh, sessionLow, IB, OR, and OR5 are RTH-only and may be zero or from a prior RTH session.");
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
        // Try live pipeline first for full snapshot including volume/sec and dwell
        if let Ok(pipelines) = self.pipelines.lock() {
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
        description = "Footprint / volume-at-price data: top price levels by total volume with bid volume, ask volume, delta, and delta-per-volume ratio at each level. Use for identifying where conviction is concentrated."
    )]
    async fn get_footprint(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        if let Ok(pipelines) = self.pipelines.lock() {
            let mut all_levels = pipelines.footprint.levels();
            // Sort by total volume descending, return top 30
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
        description = "Stacked and diagonal imbalance detection from the footprint. Stacked: 3+ consecutive levels where one side dominates (>2:1 ratio) -- shows directional conviction. Diagonal: aggressive lifting/hitting across adjacent levels -- shows urgency. Returns prices and direction for each type."
    )]
    async fn get_imbalances(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        if let Ok(pipelines) = self.pipelines.lock() {
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
        let db = self.db.lock().map_err(|_| lock_error())?;
        match db.list_recent_absorption_events(limit) {
            Ok(events) => Ok(text_result(serde_json::json!({
                "events": events,
                "count": events.len(),
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
        if let Ok(pipelines) = self.pipelines.lock() {
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
        if let (Ok(pipelines), Ok(mut rules)) = (self.pipelines.lock(), self.rules.lock()) {
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
        description = "Query recent raw tick data. Returns individual trades with price, volume, bid, ask, and aggressor side. Ordered most recent first."
    )]
    async fn query_ticks(
        &self,
        Parameters(params): Parameters<TickQueryParams>,
    ) -> Result<CallToolResult, McpError> {
        let limit = params.limit.unwrap_or(500) as usize;
        let db = self.db.lock().map_err(|_| lock_error())?;
        match db.list_recent_ticks(limit) {
            Ok(ticks) => Ok(text_result(serde_json::json!({
                "ticks": ticks,
                "count": ticks.len(),
                "dataAgeMs": compute_data_age(&db)
            }))),
            Err(e) => Err(db_error(e)),
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
        description = "Signal/setup performance statistics. Returns win rate, average R, total signals, target hit vs stop hit counts. Filter by setup_id to see performance of a specific setup."
    )]
    async fn get_signal_performance(
        &self,
        Parameters(params): Parameters<SignalPerformanceParams>,
    ) -> Result<CallToolResult, McpError> {
        let scope = build_session_scope_filter(&params.session_scope)?;
        let db = self.db.lock().map_err(|_| lock_error())?;
        match db.signal_performance_filtered(
            params.setup_id.as_deref(),
            params.start_date.as_deref(),
            params.end_date.as_deref(),
            None,
            None,
            scope.as_ref(),
        ) {
            Ok(result) => Ok(text_result(result)),
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
        description = "Relative Volume: ratio of current session's cumulative volume vs the N-day average at the same time-of-day. Classification: Low (<85%%), Normal (85-100%%), Elevated (100-115%%), High (>115%%). Use to calibrate expectations."
    )]
    async fn get_rvol(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        match db.latest_feature_state() {
            Ok(Some(s)) => Ok(text_result(serde_json::json!({
                "rvolRatio": s.get("rvolRatio"),
                "rvolClassification": s.get("rvolClassification"),
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
        if let Ok(pipelines) = self.pipelines.lock() {
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
        if let Ok(pipelines) = self.pipelines.lock() {
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
        if let Ok(pipelines) = self.pipelines.lock() {
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
        if let Ok(pipelines) = self.pipelines.lock() {
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

        // Try pipeline for price-level delta
        if let Ok(pipelines) = self.pipelines.lock() {
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
        let snapshot = db.latest_feature_state().ok().flatten();
        let risk = db.load_risk_state().ok().flatten();
        let setup_name = params.setup_name.unwrap_or_default();

        Ok(text_result(serde_json::json!({
            "setupName": setup_name,
            "marketSnapshot": snapshot,
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
            let mut boundary_count = 0u32;

            for tick in &ticks {
                if let Some(et_min) = et_minutes_from_timestamp(tick.timestamp_ms) {
                    let new_session = classify_session(et_min);
                    if new_session != current_session
                        && current_session != SessionType::Unknown
                        && new_session != SessionType::Unknown
                    {
                        // Release pipelines before db to avoid deadlock; tools can run now
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
                                let _ = db.save_prior_day_full(
                                    &date, es.high, es.low, es.close, es.va_high, es.va_low, es.poc,
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
                        pipelines.reset_session();
                        if new_session == SessionType::Rth || new_session == SessionType::Globex {
                            if let Ok(Some((h, l, c, va_h, va_l, poc))) = prior {
                                pipelines.levels.set_prior_day(h, l, c);
                                if let (Some(vh), Some(vl), Some(pc)) = (va_h, va_l, poc) {
                                    pipelines.levels.set_prior_profile(vh, vl, pc);
                                }
                            }
                        }
                        boundary_count += 1;
                    }
                    if new_session != SessionType::Unknown {
                        current_session = new_session;
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

    let service = server.serve(stdio()).await?;
    service.waiting().await?;
    Ok(())
}
