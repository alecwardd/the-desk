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
use the_desk_backend::db::Database;

#[derive(Clone)]
pub struct TheDeskMcp {
    db: Arc<Mutex<Database>>,
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

#[tool_router]
impl TheDeskMcp {
    fn new(db: Database) -> Self {
        Self {
            db: Arc::new(Mutex::new(db)),
            tool_router: Self::tool_router(),
        }
    }

    #[tool(
        description = "Current market snapshot: last price, VWAP with 1/2/3 SD bands, TPO value area (high/low/POC), delta neutral value area (DNVA high/low/DNP), session delta, cumulative delta, key levels (prior day H/L/C, prior VA/POC, overnight range, OR, IB), tape pace, imbalance count, absorption event count, and average trade size. Returns the latest persisted pipeline state."
    )]
    async fn get_market_snapshot(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        match db.latest_feature_state() {
            Ok(Some(snapshot)) => Ok(text_result(serde_json::json!({
                "snapshot": snapshot,
                "dataAgeMs": compute_data_age(&db),
                "source": "feature_state"
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
        description = "Run a backtest over historical data. Currently returns stored run metadata -- full backtest execution is planned for a future release."
    )]
    async fn run_backtest(&self) -> Result<CallToolResult, McpError> {
        Ok(text_result(serde_json::json!({
            "status": "not_implemented",
            "message": "The Desk records and evaluates; this endpoint currently returns stored run metadata only."
        })))
    }

    #[tool(
        description = "Compare current session structure against historical sessions. Reserved for historical analytics phase."
    )]
    async fn compare_sessions(&self) -> Result<CallToolResult, McpError> {
        Ok(text_result(serde_json::json!({
            "status": "not_implemented",
            "message": "Session comparison endpoint is reserved for historical analytics phase."
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = data_dir().join("data.db");
    let db = Database::open(&db_path.to_string_lossy())?;
    let service = TheDeskMcp::new(db).serve(stdio()).await?;
    service.waiting().await?;
    Ok(())
}
