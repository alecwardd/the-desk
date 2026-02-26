use rmcp::{
    handler::server::{tool::ToolRouter, wrapper::Parameters},
    model::*,
    tool, tool_handler, tool_router,
    transport::stdio,
    ErrorData as McpError, ServerHandler, ServiceExt,
};
use schemars::JsonSchema;
use serde::Deserialize;
use std::sync::{Arc, Mutex};
use the_desk_backend::backfill;
use the_desk_backend::db::Database;
use the_desk_backend::feed::scid_reader::{parse_record_scaled, ScidReader};
use the_desk_backend::feed::{load_feed_config, load_storage_config, TradeSide};
use the_desk_backend::pipelines::{EventDetector, PipelineEngine};
use the_desk_backend::research;
use the_desk_backend::{
    classify_session, et_minutes_from_timestamp, globex_open_ms, minute_of_session_from_timestamp,
    session_date_from_timestamp_ms, SessionType,
};

#[derive(Clone)]
pub struct TheDeskMcp {
    db: Arc<Mutex<Database>>,
    pipelines: Arc<Mutex<PipelineEngine>>,
    detector: Arc<Mutex<EventDetector>>,
    last_bid: Arc<Mutex<f64>>,
    last_ask: Arc<Mutex<f64>>,
    tool_router: ToolRouter<Self>,
}

fn db_error(e: impl std::fmt::Display) -> McpError {
    McpError::new(ErrorCode::INTERNAL_ERROR, e.to_string(), None)
}

fn lock_error() -> McpError {
    McpError::new(ErrorCode::INTERNAL_ERROR, "database lock poisoned", None)
}

fn text_result(json: serde_json::Value) -> CallToolResult {
    CallToolResult::success(vec![Content::text(json.to_string())])
}

fn no_data(msg: &str) -> CallToolResult {
    CallToolResult::success(vec![Content::text(msg.to_string())])
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
struct BackfillParams {
    /// Start date (YYYY-MM-DD). Omit for "all available".
    start_date: Option<String>,
    /// End date (YYYY-MM-DD). Omit for "through today". Reserved for future use.
    #[allow(dead_code)]
    end_date: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct FrequencyParams {
    /// Event type to query (e.g. "ib_mid_test", "new_session_high").
    event_type: String,
    /// Start date filter (YYYY-MM-DD).
    start_date: Option<String>,
    /// End date filter (YYYY-MM-DD).
    end_date: Option<String>,
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
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct DistributionParams {
    /// Metric column from session_summaries (e.g. "ib_range", "session_delta", "total_volume").
    metric: String,
    /// Start date filter (YYYY-MM-DD).
    start_date: Option<String>,
    /// End date filter (YYYY-MM-DD).
    end_date: Option<String>,
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
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct CompareSessionsParams {
    /// Current IB range for similarity matching.
    current_ib_range: Option<f64>,
    /// Current day type for filtering.
    current_day_type: Option<String>,
    /// Max similar sessions to return (default 5).
    max_results: Option<u64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct SignalPerformanceParams {
    /// Setup ID to filter by.
    setup_id: Option<String>,
}

#[tool_router]
impl TheDeskMcp {
    fn new(db: Database, pipelines: PipelineEngine) -> Self {
        Self {
            db: Arc::new(Mutex::new(db)),
            pipelines: Arc::new(Mutex::new(pipelines)),
            detector: Arc::new(Mutex::new(EventDetector::new())),
            last_bid: Arc::new(Mutex::new(0.0)),
            last_ask: Arc::new(Mutex::new(0.0)),
            tool_router: Self::tool_router(),
        }
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
        description = "Current market snapshot: last price, VWAP with 1/2/3 SD bands, TPO value area (high/low/POC), delta neutral value area (DNVA high/low/DNP), session delta, cumulative delta, key levels (prior day H/L/C, prior VA/POC, overnight range, OR, IB), tape pace, imbalance count, absorption event count, and average trade size. Prefers live pipeline state; falls back to last persisted snapshot."
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
        description = "Key reference levels: prior day high/low/close, prior session value area high/low and POC, overnight (Globex) high/low, initial balance high/low. These are the structural levels that frame current session context."
    )]
    async fn get_key_levels(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        match db.latest_feature_state() {
            Ok(Some(s)) => Ok(text_result(serde_json::json!({
                "priorDayHigh": s.get("priorDayHigh"),
                "priorDayLow": s.get("priorDayLow"),
                "priorDayClose": s.get("priorDayClose"),
                "priorVaHigh": s.get("priorVaHigh"),
                "priorVaLow": s.get("priorVaLow"),
                "priorPoc": s.get("priorPoc"),
                "overnightHigh": s.get("overnightHigh"),
                "overnightLow": s.get("overnightLow"),
                "ibHigh": s.get("ibHigh"),
                "ibLow": s.get("ibLow"),
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data("No key levels available")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Tape pace analytics: rolling ticks/sec and volume/sec over 5-second, 30-second, and 5-minute windows. Includes tape acceleration (rate of change). Use to gauge market activity intensity."
    )]
    async fn get_tape_pace(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        match db.latest_feature_state() {
            Ok(Some(s)) => Ok(text_result(serde_json::json!({
                "tapePace5s": s.get("tapePace5s"),
                "tapePace30s": s.get("tapePace30s"),
                "tapePace5m": s.get("tapePace5m"),
                "tapeAcceleration": s.get("tapeAcceleration"),
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data("No tape pace data")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Footprint / volume-at-price data: latest microstructure snapshot including bid/ask volume distribution, imbalance ratios, and stacked imbalance detection."
    )]
    async fn get_footprint(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        match db.latest_microstructure_snapshot() {
            Ok(Some(s)) => Ok(text_result(serde_json::json!({
                "snapshot": s,
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data("No footprint data available")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Stacked and diagonal imbalance detection from the footprint. Imbalances indicate directional conviction at specific price levels."
    )]
    async fn get_imbalances(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        match db.latest_microstructure_snapshot() {
            Ok(Some(s)) => Ok(text_result(serde_json::json!({
                "snapshot": s,
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
        description = "Trade size distribution: counts of 1-lot, 2-5 lot, 6-20 lot, and 21+ lot trades for the current session. Includes average trade size. Tracks institutional participation."
    )]
    async fn get_trade_size_profile(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        match db.latest_microstructure_snapshot() {
            Ok(Some(s)) => Ok(text_result(serde_json::json!({
                "snapshot": s,
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data("No trade size data available")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Evaluate all active playbook setups against current market state. Returns which setups have conditions met, approaching, or invalidated. Always frames results as 'your playbook says...' -- never advisory."
    )]
    async fn evaluate_playbook(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        match db.count_playbook_signals() {
            Ok(count) => Ok(text_result(serde_json::json!({
                "recentSignalCount": count,
                "note": "Use playbook_signals plus current snapshot for full context",
                "dataAgeMs": compute_data_age(&db)
            }))),
            Err(e) => Err(db_error(e)),
        }
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
        description = "Backfill historical .scid data: process past sessions through all 14 pipelines, detect market events, and persist session summaries. Idempotent -- skips dates already processed. Use to build up the historical research database."
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

        let since_ms = params.start_date.as_deref().map(|d| {
            chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d")
                .map(|nd| {
                    nd.and_hms_opt(0, 0, 0)
                        .unwrap()
                        .and_utc()
                        .timestamp_millis() as f64
                })
                .unwrap_or(0.0)
        });

        let db = self.db.lock().map_err(|_| lock_error())?;
        match backfill::run_backfill(&reader, &db, since_ms) {
            Ok(result) => Ok(text_result(
                serde_json::to_value(&result).unwrap_or_default(),
            )),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Compare current session structure against similar historical sessions. Matches by IB range similarity and optionally day type. Returns the most similar past sessions with their outcomes (close vs IB mid, delta, etc.)."
    )]
    async fn compare_sessions(
        &self,
        Parameters(params): Parameters<CompareSessionsParams>,
    ) -> Result<CallToolResult, McpError> {
        let ib_range = params.current_ib_range.unwrap_or_else(|| {
            self.live_snapshot()
                .and_then(|s| {
                    let h = s.get("ibHigh")?.as_f64()?;
                    let l = s.get("ibLow")?.as_f64()?;
                    Some(h - l)
                })
                .unwrap_or(0.0)
        });
        let db = self.db.lock().map_err(|_| lock_error())?;
        let max = params.max_results.unwrap_or(5) as usize;
        match research::compare_sessions(&db, ib_range, params.current_day_type.as_deref(), max) {
            Ok(sessions) => Ok(text_result(serde_json::json!({
                "currentIbRange": ib_range,
                "similarSessions": sessions,
                "count": sessions.len(),
            }))),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Query how often a market event occurs. Example: 'How often is IB-mid tested per session?' Returns total occurrences, sessions with event, per-session average, and percentage of sessions."
    )]
    async fn query_event_frequency(
        &self,
        Parameters(params): Parameters<FrequencyParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        match research::event_frequency(
            &db,
            &params.event_type,
            params.start_date.as_deref(),
            params.end_date.as_deref(),
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
        let db = self.db.lock().map_err(|_| lock_error())?;
        match research::metric_distribution(
            &db,
            &params.metric,
            params.start_date.as_deref(),
            params.end_date.as_deref(),
        ) {
            Ok(result) => Ok(text_result(
                serde_json::to_value(&result).unwrap_or_default(),
            )),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Query past session summaries with optional filters. Returns structured session data (OHLC, IB range, day type, delta, close vs levels) for historical analysis."
    )]
    async fn get_session_history(
        &self,
        Parameters(params): Parameters<SessionHistoryParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let limit = params.limit.unwrap_or(20) as usize;
        match db.list_session_summaries(
            params.start_date.as_deref(),
            params.end_date.as_deref(),
            params.day_type.as_deref(),
            limit,
        ) {
            Ok(sessions) => {
                let count = sessions.len();
                let summaries: Vec<serde_json::Value> = sessions
                    .into_iter()
                    .map(|s| {
                        serde_json::json!({
                            "sessionDate": s.session_date,
                            "dayType": s.day_type,
                            "ibRange": s.ib_range,
                            "high": s.high, "low": s.low, "close": s.close,
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
        let db = self.db.lock().map_err(|_| lock_error())?;
        match db.signal_performance(params.setup_id.as_deref(), None, None) {
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
        let ib_dist = research::metric_distribution(&db, "ib_range", None, None)
            .ok()
            .map(|d| serde_json::to_value(&d).unwrap_or_default());
        let delta_dist = research::metric_distribution(&db, "session_delta", None, None)
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
        match db.latest_feature_state() {
            Ok(Some(s)) => Ok(text_result(serde_json::json!({
                "activeZoneCount": s.get("activeZoneCount"),
                "note": "Zone details are computed in-memory. Use get_market_snapshot for full state.",
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data("No rebid/reoffer data available")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Recent delta momentum reversal ('pinch') events: when heavy one-sided delta is suddenly met by fast opposing flow, causing inventory to shift. Each event has timeframe, severity score (0-5), pre/post delta, price displacement. Multi-timeframe: 1m, 5m, 15m, 30m."
    )]
    async fn get_pinch_events(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        match db.latest_feature_state() {
            Ok(Some(s)) => Ok(text_result(serde_json::json!({
                "pinchEventCount": s.get("pinchEventCount"),
                "note": "Full pinch event details are computed in-memory by the pipeline engine.",
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data("No pinch data available")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Cross-session delta inventory: whether current session is Building (extending prior direction), Clearing (opposing prior direction), or Neutral. Direction: Long/Short/Flat. Includes consecutive sessions with same-direction delta (trend count)."
    )]
    async fn get_session_inventory(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        match db.latest_feature_state() {
            Ok(Some(s)) => Ok(text_result(serde_json::json!({
                "inventoryState": s.get("inventoryState"),
                "inventoryDirection": s.get("inventoryDirection"),
                "sessionsInTrend": s.get("sessionsInTrend"),
                "dataAgeMs": compute_data_age(&db)
            }))),
            Ok(None) => Ok(no_data("No session inventory data available")),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Check delta confirmation at current price level. Returns whether delta supports the direction at the current price (Stowe's 'execution requires delta confirmation'). Use before trade entry."
    )]
    async fn check_delta_confirmation(
        &self,
        Parameters(params): Parameters<DeltaConfirmParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        match db.latest_feature_state() {
            Ok(Some(s)) => {
                let session_delta = s
                    .get("sessionDelta")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0);
                let is_buy = params.is_buy_setup.unwrap_or(true);
                let confirmed = if is_buy {
                    session_delta > 0.0
                } else {
                    session_delta < 0.0
                };
                Ok(text_result(serde_json::json!({
                    "deltaConfirms": confirmed,
                    "sessionDelta": session_delta,
                    "direction": if is_buy { "long" } else { "short" },
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
        description = "Which key levels is price currently near (within specified tick distance). Returns levels sorted by distance ascending. Includes prior day H/L/C, VA/POC, overnight, IB, OR5 mid, and IB extensions."
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
                Ok(text_result(serde_json::json!({
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
        let tick_count = db.raw_tick_count().unwrap_or(0);
        let last_ts = db.latest_tick_timestamp_ms().ok().flatten();
        let now_ms = chrono::Utc::now().timestamp_millis() as f64;
        let age_ms = last_ts.map(|v| now_ms - v).unwrap_or(f64::INFINITY);

        let mut checks = serde_json::json!({
            "rawTicksPresent": tick_count > 0,
            "streamFresh": age_ms.is_finite() && age_ms <= 15_000.0,
        });

        if let Ok(Some(snapshot)) = db.latest_feature_state() {
            let poc = snapshot.get("poc").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let va_high = snapshot
                .get("vaHigh")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let va_low = snapshot
                .get("vaLow")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let poc_in_va = va_low <= poc && poc <= va_high;
            checks["pocWithinVa"] = serde_json::json!(poc_in_va);
        }

        let status = if tick_count == 0 || age_ms > 15_000.0 {
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

/// Process a single tick through the pipeline engine and event detector.
#[allow(clippy::too_many_arguments)]
fn process_tick(
    pipelines: &Arc<Mutex<PipelineEngine>>,
    detector: &Arc<Mutex<EventDetector>>,
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
    let minute = minute_of_session_from_timestamp(timestamp_ms);
    if let Ok(mut p) = pipelines.lock() {
        p.on_trade_with_timestamp(price, volume, is_buy, minute, timestamp_ms);

        let cur_bid = if bid > 0.0 { bid } else { price - 0.25 };
        let cur_ask = if ask > 0.0 { ask } else { price + 0.25 };
        let snapshot = p.snapshot(cur_bid, cur_ask);
        let session_date = session_date_from_timestamp_ms(timestamp_ms);

        if let Ok(mut det) = detector.lock() {
            let events = det.detect(&snapshot, timestamp_ms, &session_date, minute);
            event_buffer.extend(events);
        }

        // Flush event buffer periodically
        if event_buffer.len() >= 50 {
            if let Ok(d) = db.lock() {
                let _ = d.insert_market_events_batch(event_buffer);
            }
            event_buffer.clear();
        }
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

    let config = load_feed_config();
    let reader = ScidReader::from_feed_config(&config);
    let scid_available = reader.path().exists();

    if scid_available {
        // Backfill from 2 Globex opens ago to capture yesterday's full RTH session
        let since = globex_open_ms(2);
        eprintln!(
            "[the-desk-mcp] Backfilling from {} ...",
            reader.path().display()
        );
        match reader.read_bulk_since(Some(since)) {
            Ok(ticks) if !ticks.is_empty() => {
                let mut current_session = SessionType::Unknown;
                let mut boundary_count = 0u32;

                for tick in &ticks {
                    // Detect session boundary transitions
                    if let Some(et_min) = et_minutes_from_timestamp(tick.timestamp_ms) {
                        let new_session = classify_session(et_min);
                        if new_session != current_session
                            && current_session != SessionType::Unknown
                            && new_session != SessionType::Unknown
                        {
                            // RTH → Globex: save prior day levels
                            if current_session == SessionType::Rth {
                                let end_state = pipelines.session_end_state();
                                let date = session_date_from_timestamp_ms(tick.timestamp_ms);
                                let _ = db.save_prior_day_full(
                                    &date,
                                    end_state.high,
                                    end_state.low,
                                    end_state.close,
                                    end_state.va_high,
                                    end_state.va_low,
                                    end_state.poc,
                                );
                                eprintln!(
                                    "[the-desk-mcp] Session boundary: RTH→Globex, saved prior day H={:.2} L={:.2} C={:.2}",
                                    end_state.high, end_state.low, end_state.close
                                );
                            }
                            pipelines.reset_session();
                            boundary_count += 1;

                            // After reset, load prior day levels for the new session
                            if new_session == SessionType::Rth || new_session == SessionType::Globex
                            {
                                let today_str = session_date_from_timestamp_ms(tick.timestamp_ms);
                                if let Ok(Some((h, l, c, va_h, va_l, poc))) =
                                    db.load_prior_day_full(&today_str)
                                {
                                    pipelines.levels.set_prior_day(h, l, c);
                                    if let (Some(vh), Some(vl), Some(pc)) = (va_h, va_l, poc) {
                                        pipelines.levels.set_prior_profile(vh, vl, pc);
                                    }
                                }
                            }
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
                let _ = db.upsert_feature_state(
                    last.timestamp_ms,
                    &serde_json::to_value(&snapshot).unwrap_or_default(),
                );
                eprintln!(
                    "[the-desk-mcp] Backfill complete: {} ticks, {} session boundaries, last price {:.2}",
                    ticks.len(),
                    boundary_count,
                    last.price
                );
            }
            Ok(_) => eprintln!("[the-desk-mcp] No ticks since prior Globex open"),
            Err(e) => eprintln!("[the-desk-mcp] Backfill error: {e}"),
        }
    } else {
        eprintln!(
            "[the-desk-mcp] SCID file not found: {}",
            reader.path().display()
        );
    }

    let server = TheDeskMcp::new(db, pipelines);

    // Background: poll .scid for new ticks and update pipeline engine + DB
    if scid_available {
        let pipelines_bg = Arc::clone(&server.pipelines);
        let detector_bg = Arc::clone(&server.detector);
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

            // Seek to current EOF so we only process NEW ticks
            if let Ok(f) = std::fs::File::open(&reader_path) {
                offset = f.metadata().map(|m| m.len()).unwrap_or(56);
            }

            loop {
                sleep(poll).await;

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
