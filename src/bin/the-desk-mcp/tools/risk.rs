//! Risk and account state: limits, sizing, session open/close.

use chrono::Utc;
use rmcp::{
    handler::server::wrapper::Parameters, model::*, tool, tool_router, ErrorData as McpError,
};
use the_desk_backend::db::{
    AccountStateRecord, JournalEntry, OpenPositionRecord, RiskConfigRecord, SessionRecord,
};
use the_desk_backend::memory::mark_memory_dirty as memory_mark_dirty;
use the_desk_backend::risk::RiskState;
use the_desk_backend::trading_day_from_timestamp_ms;

#[allow(unused_imports)]
use crate::{helpers::*, lifecycle::*, params::*, state::*};

#[tool_router(router = risk_router, vis = "pub(crate)")]
impl TheDeskMcp {
    #[tool(
        description = "Current risk state: daily P&L in R-units, trade count, consecutive losses/wins, drawdown, and whether the daily loss limit has been reached. Uses the trader's configured R framework."
    )]
    pub(crate) async fn get_risk_state(&self) -> Result<CallToolResult, McpError> {
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
    pub(crate) async fn get_account_state(&self) -> Result<CallToolResult, McpError> {
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
    pub(crate) async fn save_account_state(
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
    pub(crate) async fn get_kelly_position_size(
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
    pub(crate) async fn get_risk_config(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let config = db.load_risk_config().map_err(db_error)?;
        Ok(text_result(serde_json::json!({
            "riskConfig": config
        })))
    }

    #[tool(
        description = "Save risk configuration. Partial updates: only provided fields are updated. Call to persist R-value, max daily loss, circuit breaker, and trade limits. Required for full risk tracking when config is not yet in database."
    )]
    pub(crate) async fn save_risk_config(
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
    pub(crate) async fn init_risk_state(&self) -> Result<CallToolResult, McpError> {
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
        self.playbook_cache.set_risk_at_limit(state.at_limit);
        Ok(text_result(serde_json::json!({
            "initialized": true,
            "riskState": state
        })))
    }

    #[tool(
        description = "Start a trading session in the local journal store. Creates a session row that trades and journal entries can attach to. Use this at the beginning of a discretionary review or live session when you want Cursor agents to log journal context consistently."
    )]
    pub(crate) async fn start_trading_session(
        &self,
        Parameters(params): Parameters<StartTradingSessionParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let start_time_ms = params
            .start_time_ms
            .unwrap_or_else(|| Utc::now().timestamp_millis() as f64);
        let session = SessionRecord {
            id: params
                .session_id
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            date: trading_day_from_timestamp_ms(start_time_ms),
            session_type: params
                .session_type
                .unwrap_or_else(|| infer_session_type_label(start_time_ms)),
            start_time: start_time_ms,
            end_time: None,
            recording_path: params.recording_path,
            pre_session_note: params.pre_session_note,
        };
        db.upsert_session(&session).map_err(db_error)?;
        let memory_maintenance =
            memory_mark_dirty(&db, false, true, "start_trading_session").map_err(db_error)?;
        Ok(text_result(serde_json::json!({
            "started": true,
            "session": session,
            "memoryMaintenance": memory_maintenance
        })))
    }

    #[tool(
        description = "End a trading session in the local journal store. Optionally saves a freeform session note as a journal entry linked to the session."
    )]
    pub(crate) async fn end_trading_session(
        &self,
        Parameters(params): Parameters<EndTradingSessionParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let session_id = resolve_session_id(&db, params.session_id.as_deref())?
            .ok_or_else(|| invalid_params_error("no open session found to close"))?;
        let end_time_ms = params
            .end_time_ms
            .unwrap_or_else(|| Utc::now().timestamp_millis() as f64);
        db.update_session_end(&session_id, end_time_ms, params.recording_path.as_deref())
            .map_err(db_error)?;

        if let Some(content) = params.session_note.filter(|note| !note.trim().is_empty()) {
            let entry = JournalEntry {
                id: uuid::Uuid::new_v4().to_string(),
                session_id: Some(session_id.clone()),
                date: trading_day_from_timestamp_ms(end_time_ms),
                content,
                tags: vec!["session-end".to_string()],
                setup_references: Vec::new(),
                trade_references: Vec::new(),
                created_at: end_time_ms,
            };
            db.upsert_journal_entry(&entry).map_err(db_error)?;
        }

        Ok(text_result(serde_json::json!({
            "ended": true,
            "sessionId": session_id,
            "endTimeMs": end_time_ms
        })))
    }
}
