use crate::depth::DepthRecord;
use crate::memory::{
    AgentInsightQuery, AgentInsightRecord, BehavioralPatternQuery, BehavioralPatternRecord,
    MemoryFollowupQuery, MemoryFollowupRecord, MemoryMaintenanceState, INSIGHT_DISMISSED,
    INSIGHT_PINNED, INSIGHT_SUPERSEDED,
};
use crate::pipelines::event_detector::MarketEvent;
use crate::risk::RiskState;
use crate::rules::SetupDefinition;
use crate::tick_time_context_from_timestamp_ms;
use crate::trading_day_from_timestamp_ms;
use rusqlite::{params, Connection, Row};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DbError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
}

// ---------------------------------------------------------------------------
// Event types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionEventInput {
    pub event_type: String,
    pub setup_id: Option<String>,
    pub data: serde_json::Value,
    #[serde(default)]
    pub session_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionEventRecord {
    pub id: i64,
    pub event_type: String,
    pub setup_id: Option<String>,
    pub data: serde_json::Value,
    pub session_id: Option<String>,
    pub timestamp: Option<f64>,
}

// ---------------------------------------------------------------------------
// Session entity
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionRecord {
    pub id: String,
    pub date: String,
    pub session_type: String,
    pub start_time: f64,
    pub end_time: Option<f64>,
    pub recording_path: Option<String>,
    pub pre_session_note: Option<String>,
}

// ---------------------------------------------------------------------------
// Trade entity
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TradeRecord {
    pub id: String,
    pub session_id: Option<String>,
    pub setup_id: Option<String>,
    pub instrument: Option<String>,
    pub trade_account: Option<String>,
    pub entry_time: f64,
    pub entry_price: f64,
    pub exit_time: Option<f64>,
    pub exit_price: Option<f64>,
    pub direction: String,
    pub size: i64,
    pub max_open_size: Option<i64>,
    pub stop_price: Option<f64>,
    pub target_prices: Vec<f64>,
    pub result_r: Option<f64>,
    pub gross_points: Option<f64>,
    pub planned: bool,
    pub rules_followed: Option<bool>,
    pub emotional_state: Option<String>,
    pub thesis: Option<String>,
    pub review_tags: Vec<String>,
    pub mistake_tags: Vec<String>,
    pub entry_fill_count: i64,
    pub exit_fill_count: i64,
    pub import_batch_id: Option<String>,
    pub notes: String,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TradeReviewUpdate {
    pub planned: bool,
    pub rules_followed: Option<bool>,
    pub emotional_state: Option<String>,
    pub thesis: Option<String>,
    pub review_tags: Vec<String>,
    pub mistake_tags: Vec<String>,
    pub notes: String,
}

// ---------------------------------------------------------------------------
// Journal entity
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JournalEntry {
    pub id: String,
    pub session_id: Option<String>,
    pub date: String,
    pub content: String,
    pub tags: Vec<String>,
    pub setup_references: Vec<String>,
    pub trade_references: Vec<String>,
    pub created_at: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TradeImportBatchRecord {
    pub batch_id: String,
    pub source: String,
    pub imported_at: f64,
    pub notes: String,
    pub fill_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportedFillRecord {
    pub fingerprint: String,
    pub batch_id: String,
    pub trade_id: Option<String>,
    pub symbol: String,
    pub trade_account: Option<String>,
    pub fill_time: f64,
    pub order_side: String,
    pub open_close: Option<String>,
    pub quantity: i64,
    pub price: f64,
    pub status: String,
    pub external_order_id: Option<String>,
    pub service_order_id: Option<String>,
    pub raw_payload: serde_json::Value,
}

// ---------------------------------------------------------------------------
// Risk config entity
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RiskConfigRecord {
    pub r_value_points: f64,
    pub r_value_dollars: f64,
    pub max_daily_loss_r: f64,
    pub max_consecutive_losses: i64,
    pub max_trades_per_session: Option<i64>,
    pub no_trade_zones: Vec<serde_json::Value>,
    #[serde(default)]
    pub max_daily_loss_dollars: Option<f64>,
}

/// Open position not from chat (user-confirmed at session start).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenPositionRecord {
    pub direction: String,
    pub size: i64,
    pub entry_price: f64,
    pub instrument: Option<String>,
    pub setup_id: Option<String>,
}

/// Account state for risk coach: balance, positions, Lucid params, goals.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountStateRecord {
    pub last_balance_dollars: f64,
    pub last_balance_updated_at_ms: i64,
    pub open_positions: Vec<OpenPositionRecord>,
    pub lucid_daily_loss_dollars: Option<f64>,
    pub lucid_account_size_dollars: Option<f64>,
    pub profit_target_per_cycle: Option<f64>,
    pub position_sizing_method: String,
    pub kelly_fraction: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawTickRecord {
    pub id: i64,
    pub timestamp_ms: f64,
    pub price: f64,
    pub volume: f64,
    pub bid: f64,
    pub ask: f64,
    pub is_buy: bool,
    pub session_date: String,
    pub root_symbol: Option<String>,
    pub contract_symbol: Option<String>,
}

pub type RawTickBatchRow = (f64, f64, f64, f64, f64, bool, String, String, String);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DepthEventRecord {
    pub id: i64,
    pub source_file: String,
    pub timestamp_ms: f64,
    pub side: Option<String>,
    pub command: String,
    pub price: f64,
    pub quantity: f64,
    pub num_orders: i64,
    pub end_of_batch: bool,
    pub batch_id: Option<i64>,
    pub trading_day: Option<String>,
}

/// End-of-session summary with key metrics for historical research.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionSummary {
    pub session_date: String,
    pub session_type: String,
    pub root_symbol: String,
    pub contract_symbol: String,
    pub contract_month: Option<String>,
    pub symbol_resolution_mode: String,
    pub carry_forward_levels_valid: bool,
    pub rollover_warning: Option<String>,
    pub open_price: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub poc: f64,
    pub vah: f64,
    pub val: f64,
    pub ib_high: f64,
    pub ib_low: f64,
    pub ib_range: f64,
    pub ib_mid: f64,
    pub or_high: f64,
    pub or_low: f64,
    pub day_type: String,
    pub profile_shape: String,
    pub balance_state: String,
    pub total_volume: f64,
    pub tick_count: i64,
    pub session_delta: f64,
    pub cumulative_delta: f64,
    pub dnp: f64,
    pub dnva_high: f64,
    pub dnva_low: f64,
    pub vwap_close: f64,
    pub signal_count: i64,
    pub single_prints_direction: String,
    pub excess_high: bool,
    pub excess_low: bool,
    pub poor_high: bool,
    pub poor_low: bool,
    pub rvol_ratio: f64,
    pub close_vs_ib_mid: String,
    pub close_vs_vwap: String,
    pub close_vs_poc: String,
    pub snapshot_json: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PriorDayReference {
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub va_high: Option<f64>,
    pub va_low: Option<f64>,
    pub poc: Option<f64>,
    pub dnva_high: Option<f64>,
    pub dnva_low: Option<f64>,
    pub dnp: Option<f64>,
    pub root_symbol: Option<String>,
    pub contract_symbol: Option<String>,
}

/// Signal outcome tracking record.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignalOutcome {
    pub signal_id: String,
    pub setup_id: String,
    pub setup_name: Option<String>,
    pub session_date: String,
    #[serde(default)]
    pub root_symbol: Option<String>,
    #[serde(default)]
    pub contract_symbol: Option<String>,
    #[serde(default = "default_signal_source")]
    pub source: String,
    #[serde(default)]
    pub job_id: Option<String>,
    pub fired_at_ms: f64,
    pub fired_price: f64,
    pub target_price: Option<f64>,
    pub stop_price: Option<f64>,
    pub outcome: String,
    pub outcome_at_ms: Option<f64>,
    pub max_favorable_excursion: Option<f64>,
    pub max_adverse_excursion: Option<f64>,
    pub r_result: Option<f64>,
    pub time_to_outcome_ms: Option<f64>,
    #[serde(default)]
    pub rvol_at_fire: Option<f64>,
    #[serde(default)]
    pub rvol_bucket_at_fire: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignalOutcomeExcursionRow {
    pub setup_id: String,
    pub setup_name: Option<String>,
    pub outcome: String,
    pub max_favorable_excursion: Option<f64>,
    pub max_adverse_excursion: Option<f64>,
    pub time_to_outcome_ms: Option<f64>,
    pub fired_at_ms: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetupPerformanceSortBy {
    WinRate,
    AvgR,
    Resolved,
    TotalSignals,
}

/// Optional session/trading-day scope filter used by research-style queries.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionScopeFilter {
    pub session_type: Option<String>,
    pub session_segment: Option<String>,
    pub trading_day_start: Option<String>,
    pub trading_day_end: Option<String>,
    pub root_symbol: Option<String>,
    pub contract_symbol: Option<String>,
    #[serde(default = "default_include_rollover_sessions")]
    pub include_rollover_sessions: bool,
    #[serde(default)]
    pub continuous_mode: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplaySignalRecord {
    pub signal_id: String,
    pub timestamp_ms: f64,
    pub session_date: String,
    #[serde(default)]
    pub root_symbol: Option<String>,
    #[serde(default)]
    pub contract_symbol: Option<String>,
    pub setup_id: String,
    pub payload: serde_json::Value,
    pub source: String,
    pub job_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoricalJobRun {
    pub id: String,
    pub job_type: String,
    pub status: String,
    pub params: serde_json::Value,
    pub progress: serde_json::Value,
    pub result: Option<serde_json::Value>,
    pub warnings: Vec<String>,
    pub error: Option<String>,
    pub submitted_at_ms: f64,
    pub started_at_ms: Option<f64>,
    pub finished_at_ms: Option<f64>,
}

/// Parameters for updating a historical job run (avoids too many function args).
pub struct HistoricalJobRunUpdate<'a> {
    pub status: &'a str,
    pub progress: &'a serde_json::Value,
    pub result: Option<&'a serde_json::Value>,
    pub warnings: &'a [String],
    pub error: Option<&'a str>,
    pub started_at_ms: Option<f64>,
    pub finished_at_ms: Option<f64>,
}

fn default_signal_source() -> String {
    "live".to_string()
}

fn default_include_rollover_sessions() -> bool {
    true
}

impl SessionScopeFilter {
    fn is_empty(&self) -> bool {
        self.session_type.is_none()
            && self.session_segment.is_none()
            && self.trading_day_start.is_none()
            && self.trading_day_end.is_none()
            && self.root_symbol.is_none()
            && self.contract_symbol.is_none()
            && self.include_rollover_sessions
            && !self.continuous_mode
    }
}

fn normalize_session_type_filter(value: &str) -> Option<&'static str> {
    let norm = value.trim().to_ascii_lowercase();
    match norm.as_str() {
        "rth" => Some("RTH"),
        "globex" => Some("Globex"),
        "unknown" => Some("Unknown"),
        _ => None,
    }
}

fn normalize_session_segment_filter(value: &str) -> Option<&'static str> {
    let norm = value.trim().to_ascii_lowercase();
    match norm.as_str() {
        "asia" => Some("Asia"),
        "london" => Some("London"),
        "none" => Some("None"),
        _ => None,
    }
}

fn normalize_session_type_value(value: &str) -> Option<&'static str> {
    normalize_session_type_filter(value)
}

fn normalize_session_segment_value(value: &str, session_type: &str) -> Option<&'static str> {
    if session_type != "Globex" {
        return Some("None");
    }
    normalize_session_segment_filter(value).or(Some("None"))
}

fn resolved_event_row_context(
    timestamp_ms: f64,
    session_type: Option<&str>,
    session_segment: Option<&str>,
    trading_day: Option<&str>,
    session_date_fallback: Option<&str>,
) -> Option<(String, String, String)> {
    let stored_type = session_type
        .filter(|v| !v.trim().is_empty())
        .and_then(normalize_session_type_value);
    let stored_day = trading_day.filter(|v| !v.trim().is_empty());
    let stored_segment = session_segment.filter(|v| !v.trim().is_empty());

    if let (Some(st), Some(td)) = (stored_type, stored_day) {
        let seg = normalize_session_segment_value(stored_segment.unwrap_or("None"), st)?;
        return Some((st.to_string(), seg.to_string(), td.to_string()));
    }

    if let Some(ctx) = tick_time_context_from_timestamp_ms(timestamp_ms) {
        let st = match ctx.session_type {
            crate::SessionType::Rth => "RTH",
            crate::SessionType::Globex => "Globex",
            crate::SessionType::Unknown => "Unknown",
        };
        let seg = if st == "Globex" {
            match ctx.session_segment {
                crate::SessionSegment::Asia => "Asia",
                crate::SessionSegment::London => "London",
                crate::SessionSegment::None => "None",
            }
        } else {
            "None"
        };
        return Some((st.to_string(), seg.to_string(), ctx.trading_day));
    }

    session_date_fallback.map(|d| ("Unknown".to_string(), "None".to_string(), d.to_string()))
}

fn trading_day_if_scope_match_for_event_row(
    timestamp_ms: f64,
    session_type: Option<&str>,
    session_segment: Option<&str>,
    trading_day: Option<&str>,
    session_date_fallback: Option<&str>,
    scope: Option<&SessionScopeFilter>,
) -> Option<String> {
    let (row_session_type, row_session_segment, row_trading_day) = resolved_event_row_context(
        timestamp_ms,
        session_type,
        session_segment,
        trading_day,
        session_date_fallback,
    )?;

    let Some(scope) = scope else {
        return Some(row_trading_day);
    };
    if scope.is_empty() {
        return Some(row_trading_day);
    }
    if let Some(filter_type) = scope.session_type.as_deref() {
        let normalized = normalize_session_type_filter(filter_type)?;
        if row_session_type != normalized {
            return None;
        }
    }
    if let Some(filter_segment) = scope.session_segment.as_deref() {
        let normalized = normalize_session_segment_filter(filter_segment)?;
        if row_session_segment != normalized {
            return None;
        }
    }
    if let Some(start) = scope.trading_day_start.as_deref() {
        if row_trading_day.as_str() < start {
            return None;
        }
    }
    if let Some(end) = scope.trading_day_end.as_deref() {
        if row_trading_day.as_str() > end {
            return None;
        }
    }

    Some(row_trading_day)
}

fn trading_day_if_scope_match(
    timestamp_ms: f64,
    scope: Option<&SessionScopeFilter>,
) -> Option<String> {
    let ctx = tick_time_context_from_timestamp_ms(timestamp_ms)?;
    let Some(scope) = scope else {
        return Some(ctx.trading_day);
    };
    if scope.is_empty() {
        return Some(ctx.trading_day);
    }

    if let Some(filter_type) = scope.session_type.as_deref() {
        let normalized = normalize_session_type_filter(filter_type)?;
        let current = match ctx.session_type {
            crate::SessionType::Rth => "RTH",
            crate::SessionType::Globex => "Globex",
            crate::SessionType::Unknown => "Unknown",
        };
        if current != normalized {
            return None;
        }
    }

    if let Some(filter_segment) = scope.session_segment.as_deref() {
        let normalized = normalize_session_segment_filter(filter_segment)?;
        let current = match ctx.session_segment {
            crate::SessionSegment::Asia => "Asia",
            crate::SessionSegment::London => "London",
            crate::SessionSegment::None => "None",
        };
        if current != normalized {
            return None;
        }
    }

    if let Some(start) = scope.trading_day_start.as_deref() {
        if ctx.trading_day.as_str() < start {
            return None;
        }
    }
    if let Some(end) = scope.trading_day_end.as_deref() {
        if ctx.trading_day.as_str() > end {
            return None;
        }
    }

    Some(ctx.trading_day)
}

fn analysis_day_for_scope(
    session_date: &str,
    timestamp_ms: f64,
    scope: Option<&SessionScopeFilter>,
) -> Option<String> {
    match trading_day_if_scope_match(timestamp_ms, scope) {
        Some(day) => Some(day),
        None => {
            if scope.map(|s| !s.is_empty()).unwrap_or(false) {
                None
            } else {
                Some(session_date.to_string())
            }
        }
    }
}

fn contract_fields_match_scope(
    root_symbol: Option<&str>,
    contract_symbol: Option<&str>,
    scope: Option<&SessionScopeFilter>,
) -> bool {
    let Some(scope) = scope else {
        return true;
    };
    if let Some(filter_root) = scope.root_symbol.as_deref() {
        if root_symbol.unwrap_or_default() != filter_root {
            return false;
        }
    }
    if let Some(filter_contract) = scope.contract_symbol.as_deref() {
        if contract_symbol.unwrap_or_default() != filter_contract {
            return false;
        }
    }
    true
}

impl Default for RiskConfigRecord {
    fn default() -> Self {
        Self {
            r_value_points: 80.0,
            r_value_dollars: 400.0,
            max_daily_loss_r: 3.0,
            max_consecutive_losses: 3,
            max_trades_per_session: Some(8),
            no_trade_zones: Vec::new(),
            max_daily_loss_dollars: Some(1200.0),
        }
    }
}

// ---------------------------------------------------------------------------
// Database
// ---------------------------------------------------------------------------

pub struct Database {
    conn: Connection,
}

impl Database {
    pub fn open(path: &str) -> Result<Self, DbError> {
        let conn = Connection::open(path)?;
        conn.busy_timeout(Duration::from_secs(5))?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
        let db = Self { conn };
        db.run_migrations()?;
        Ok(db)
    }

    // ------------------------------------------------------------------
    // Schema migration
    // ------------------------------------------------------------------

    fn run_migrations(&self) -> Result<(), DbError> {
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER NOT NULL DEFAULT 0
            )",
        )?;

        let version: i32 = self
            .conn
            .query_row(
                "SELECT COALESCE(MAX(version), 0) FROM schema_version",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);

        if version < 1 {
            self.migrate_v1()?;
        }
        if version < 2 {
            self.migrate_v2()?;
        }
        if version < 3 {
            self.migrate_v3()?;
        }
        if version < 4 {
            self.migrate_v4()?;
        }
        if version < 5 {
            self.migrate_v5()?;
        }
        if version < 6 {
            self.migrate_v6()?;
        }
        if version < 7 {
            self.migrate_v7()?;
        }
        if version < 8 {
            self.migrate_v8()?;
        }
        if version < 9 {
            self.migrate_v9()?;
        }
        if version < 10 {
            self.migrate_v10()?;
        }
        if version < 11 {
            self.migrate_v11()?;
        }
        if version < 12 {
            self.migrate_v12()?;
        }
        if version < 13 {
            self.migrate_v13()?;
        }
        if version < 14 {
            self.migrate_v14()?;
        }
        if version < 15 {
            self.migrate_v15()?;
        }
        if version < 16 {
            self.migrate_v16()?;
        }
        if version < 17 {
            self.migrate_v17()?;
        }
        if version < 18 {
            self.migrate_v18()?;
        }
        if version < 19 {
            self.migrate_v19()?;
        }
        if version < 20 {
            self.migrate_v20()?;
        }

        Ok(())
    }

    /// V1: original schema (creates tables if not exists for backward compat).
    fn migrate_v1(&self) -> Result<(), DbError> {
        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS setups (
              id TEXT PRIMARY KEY,
              name TEXT NOT NULL,
              description TEXT NOT NULL DEFAULT '',
              active INTEGER NOT NULL,
              conditions TEXT NOT NULL DEFAULT '[]',
              min_delta REAL NOT NULL DEFAULT 0.0,
              require_above_vwap INTEGER NOT NULL DEFAULT 0,
              duplicate_suppression_ms INTEGER NOT NULL DEFAULT 2000
            );
            CREATE TABLE IF NOT EXISTS session_events (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              event_type TEXT NOT NULL,
              setup_id TEXT NULL,
              data TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS risk_state (
              singleton INTEGER PRIMARY KEY,
              daily_pnl_r REAL NOT NULL,
              trade_count INTEGER NOT NULL,
              consecutive_losses INTEGER NOT NULL,
              drawdown_r REAL NOT NULL,
              max_daily_loss_r REAL NOT NULL,
              at_limit INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS prior_day_levels (
              date TEXT PRIMARY KEY,
              high REAL NOT NULL,
              low REAL NOT NULL,
              close REAL NOT NULL
            );
            INSERT OR REPLACE INTO schema_version (version) VALUES (1);
            ",
        )?;
        Ok(())
    }

    /// V2: extended schema — new tables, new columns, indexes.
    fn migrate_v2(&self) -> Result<(), DbError> {
        // Extend setups with rich playbook fields
        let setup_columns = [
            ("entry_logic", "TEXT NOT NULL DEFAULT '{}'"),
            ("stop_logic", "TEXT NOT NULL DEFAULT '{}'"),
            ("targets", "TEXT NOT NULL DEFAULT '[]'"),
            ("position_sizing", "TEXT NOT NULL DEFAULT '{}'"),
            ("market_context", "TEXT NOT NULL DEFAULT '{}'"),
            ("invalidation", "TEXT NOT NULL DEFAULT '[]'"),
            ("backtest_results", "TEXT NOT NULL DEFAULT '{}'"),
            ("context_backtest_results", "TEXT NOT NULL DEFAULT '[]'"),
            ("discretionary_conditions", "TEXT NOT NULL DEFAULT '[]'"),
            ("template_source", "TEXT NULL"),
        ];
        for (col, def) in &setup_columns {
            let sql = format!("ALTER TABLE setups ADD COLUMN {col} {def}");
            self.conn.execute_batch(&sql).ok(); // ignore if column already exists
        }

        // Extend session_events
        let event_columns = [("session_id", "TEXT NULL"), ("timestamp", "REAL NULL")];
        for (col, def) in &event_columns {
            let sql = format!("ALTER TABLE session_events ADD COLUMN {col} {def}");
            self.conn.execute_batch(&sql).ok();
        }

        // Extend prior_day_levels with VA/POC
        let level_columns = [
            ("va_high", "REAL NULL"),
            ("va_low", "REAL NULL"),
            ("poc", "REAL NULL"),
        ];
        for (col, def) in &level_columns {
            let sql = format!("ALTER TABLE prior_day_levels ADD COLUMN {col} {def}");
            self.conn.execute_batch(&sql).ok();
        }

        // Extend risk_state with consecutive wins
        self.conn
            .execute_batch(
                "ALTER TABLE risk_state ADD COLUMN consecutive_wins INTEGER NOT NULL DEFAULT 0",
            )
            .ok();

        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS sessions (
              id TEXT PRIMARY KEY,
              date TEXT NOT NULL,
              session_type TEXT NOT NULL DEFAULT 'rth',
              start_time REAL NOT NULL,
              end_time REAL NULL,
              recording_path TEXT NULL,
              pre_session_note TEXT NULL
            );

            CREATE TABLE IF NOT EXISTS trades (
              id TEXT PRIMARY KEY,
              session_id TEXT NULL REFERENCES sessions(id),
              setup_id TEXT NULL REFERENCES setups(id),
              entry_time REAL NOT NULL,
              entry_price REAL NOT NULL,
              exit_time REAL NULL,
              exit_price REAL NULL,
              direction TEXT NOT NULL,
              size INTEGER NOT NULL,
              stop_price REAL NULL,
              target_prices TEXT NOT NULL DEFAULT '[]',
              result_r REAL NULL,
              planned INTEGER NOT NULL DEFAULT 0,
              rules_followed INTEGER NULL,
              emotional_state TEXT NULL,
              notes TEXT NOT NULL DEFAULT '',
              source TEXT NOT NULL DEFAULT 'manual'
            );

            CREATE TABLE IF NOT EXISTS journal_entries (
              id TEXT PRIMARY KEY,
              session_id TEXT NULL REFERENCES sessions(id),
              date TEXT NOT NULL,
              content TEXT NOT NULL DEFAULT '',
              tags TEXT NOT NULL DEFAULT '[]',
              setup_references TEXT NOT NULL DEFAULT '[]',
              trade_references TEXT NOT NULL DEFAULT '[]',
              created_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS risk_config (
              singleton INTEGER PRIMARY KEY DEFAULT 1,
              r_value_points REAL NOT NULL DEFAULT 8.0,
              r_value_dollars REAL NOT NULL DEFAULT 40.0,
              max_daily_loss_r REAL NOT NULL DEFAULT 3.0,
              max_consecutive_losses INTEGER NOT NULL DEFAULT 3,
              max_trades_per_session INTEGER NULL DEFAULT 8,
              no_trade_zones TEXT NOT NULL DEFAULT '[]'
            );

            CREATE INDEX IF NOT EXISTS idx_session_events_session
              ON session_events(session_id, event_type);
            CREATE INDEX IF NOT EXISTS idx_trades_session
              ON trades(session_id);
            CREATE INDEX IF NOT EXISTS idx_journal_session
              ON journal_entries(session_id);

            UPDATE schema_version SET version = 2;
            ",
        )?;
        Ok(())
    }

    /// V3: backend intelligence schema for raw feed and computed snapshots.
    fn migrate_v3(&self) -> Result<(), DbError> {
        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS raw_ticks (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              timestamp_ms REAL NOT NULL,
              price REAL NOT NULL,
              volume REAL NOT NULL,
              bid REAL NOT NULL,
              ask REAL NOT NULL,
              is_buy INTEGER NOT NULL,
              session_date TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_raw_ticks_timestamp ON raw_ticks(timestamp_ms);
            CREATE INDEX IF NOT EXISTS idx_raw_ticks_session ON raw_ticks(session_date, timestamp_ms);

            CREATE TABLE IF NOT EXISTS feature_state (
              singleton INTEGER PRIMARY KEY DEFAULT 1,
              timestamp_ms REAL NOT NULL,
              payload TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS pipeline_snapshots (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              timestamp_ms REAL NOT NULL,
              payload TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_pipeline_snapshots_ts ON pipeline_snapshots(timestamp_ms);

            CREATE TABLE IF NOT EXISTS playbook_signals (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              timestamp_ms REAL NOT NULL,
              setup_id TEXT NOT NULL,
              payload TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_playbook_signals_ts ON playbook_signals(timestamp_ms);
            CREATE INDEX IF NOT EXISTS idx_playbook_signals_setup ON playbook_signals(setup_id);

            CREATE TABLE IF NOT EXISTS microstructure_snapshots (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              timestamp_ms REAL NOT NULL,
              payload TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_microstructure_snapshots_ts ON microstructure_snapshots(timestamp_ms);

            CREATE TABLE IF NOT EXISTS absorption_events (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              timestamp_ms REAL NOT NULL,
              event_type TEXT NOT NULL,
              price REAL NOT NULL,
              severity REAL NOT NULL,
              payload TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_absorption_events_ts ON absorption_events(timestamp_ms);

            CREATE TABLE IF NOT EXISTS validation_runs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              run_at_ms REAL NOT NULL,
              status TEXT NOT NULL,
              details TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS backtest_runs (
              id TEXT PRIMARY KEY,
              created_at_ms REAL NOT NULL,
              params TEXT NOT NULL,
              metrics TEXT NOT NULL,
              trades TEXT NOT NULL
            );

            UPDATE schema_version SET version = 3;
            ",
        )?;
        Ok(())
    }

    /// V4: market structure research tables — events, session summaries, signal outcomes.
    fn migrate_v4(&self) -> Result<(), DbError> {
        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS market_events (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              session_date TEXT NOT NULL,
              timestamp_ms REAL NOT NULL,
              event_type TEXT NOT NULL,
              level_name TEXT,
              price REAL NOT NULL,
              direction TEXT,
              sequence_num INTEGER,
              metadata_json TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_market_events_session
              ON market_events(session_date);
            CREATE INDEX IF NOT EXISTS idx_market_events_type
              ON market_events(event_type, session_date);

            CREATE TABLE IF NOT EXISTS session_summaries (
              session_date TEXT PRIMARY KEY,
              session_type TEXT NOT NULL,
              open_price REAL, high REAL, low REAL, close REAL,
              poc REAL, vah REAL, val REAL,
              ib_high REAL, ib_low REAL, ib_range REAL,
              ib_mid REAL,
              or_high REAL, or_low REAL,
              day_type TEXT,
              total_volume REAL, tick_count INTEGER,
              session_delta REAL, cumulative_delta REAL,
              dnp REAL, dnva_high REAL, dnva_low REAL,
              vwap_close REAL,
              signal_count INTEGER DEFAULT 0,
              single_prints_direction TEXT,
              excess_high INTEGER DEFAULT 0, excess_low INTEGER DEFAULT 0,
              poor_high INTEGER DEFAULT 0, poor_low INTEGER DEFAULT 0,
              rvol_ratio REAL,
              close_vs_ib_mid TEXT,
              close_vs_vwap TEXT,
              close_vs_poc TEXT,
              snapshot_json TEXT
            );

            CREATE TABLE IF NOT EXISTS signal_outcomes (
              signal_id TEXT PRIMARY KEY,
              setup_id TEXT NOT NULL,
              setup_name TEXT,
              fired_at_ms REAL NOT NULL,
              fired_price REAL NOT NULL,
              target_price REAL,
              stop_price REAL,
              outcome TEXT NOT NULL DEFAULT 'pending',
              outcome_at_ms REAL,
              max_favorable_excursion REAL,
              max_adverse_excursion REAL,
              r_result REAL,
              time_to_outcome_ms REAL
            );
            CREATE INDEX IF NOT EXISTS idx_signal_outcomes_setup
              ON signal_outcomes(setup_id);
            CREATE INDEX IF NOT EXISTS idx_signal_outcomes_outcome
              ON signal_outcomes(outcome);

            UPDATE schema_version SET version = 4;
            ",
        )?;
        Ok(())
    }

    /// V5: add profile_shape and balance_state columns to session_summaries.
    fn migrate_v5(&self) -> Result<(), DbError> {
        self.conn.execute_batch(
            "
            ALTER TABLE session_summaries ADD COLUMN profile_shape TEXT NOT NULL DEFAULT '';
            ALTER TABLE session_summaries ADD COLUMN balance_state TEXT NOT NULL DEFAULT '';

            UPDATE schema_version SET version = 5;
            ",
        )?;
        Ok(())
    }

    /// V6: add dedup constraints for raw_ticks and market_events.
    fn migrate_v6(&self) -> Result<(), DbError> {
        self.conn.execute_batch(
            "
            DELETE FROM raw_ticks
            WHERE id NOT IN (
              SELECT MIN(id)
              FROM raw_ticks
              GROUP BY timestamp_ms, price, volume, bid, ask, is_buy, session_date
            );

            DELETE FROM market_events
            WHERE id NOT IN (
              SELECT MIN(id)
              FROM market_events
              GROUP BY
                session_date,
                timestamp_ms,
                event_type,
                COALESCE(level_name, ''),
                price,
                COALESCE(direction, ''),
                COALESCE(sequence_num, -1)
            );

            CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_ticks_identity
              ON raw_ticks(timestamp_ms, price, volume, bid, ask, is_buy, session_date);

            CREATE UNIQUE INDEX IF NOT EXISTS ux_market_events_identity
              ON market_events(
                session_date,
                timestamp_ms,
                event_type,
                COALESCE(level_name, ''),
                price,
                COALESCE(direction, ''),
                COALESCE(sequence_num, -1)
              );

            UPDATE schema_version SET version = 6;
            ",
        )?;
        Ok(())
    }

    /// V7: account_state table and risk_config max_daily_loss_dollars.
    fn migrate_v7(&self) -> Result<(), DbError> {
        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS account_state (
              singleton INTEGER PRIMARY KEY DEFAULT 1,
              last_balance_dollars REAL NOT NULL DEFAULT 0.0,
              last_balance_updated_at_ms INTEGER NOT NULL DEFAULT 0,
              open_positions_json TEXT NOT NULL DEFAULT '[]',
              lucid_daily_loss_dollars REAL NULL,
              lucid_account_size_dollars REAL NULL,
              profit_target_per_cycle REAL NULL,
              position_sizing_method TEXT NOT NULL DEFAULT 'quarter_kelly',
              kelly_fraction REAL NOT NULL DEFAULT 0.25
            );
            INSERT OR IGNORE INTO account_state (singleton) VALUES (1);
            ",
        )?;
        // Add max_daily_loss_dollars to risk_config if not present (SQLite has no IF NOT EXISTS for ADD COLUMN).
        let _ = self
            .conn
            .execute_batch("ALTER TABLE risk_config ADD COLUMN max_daily_loss_dollars REAL NULL");
        self.conn.execute_batch(
            "
            UPDATE schema_version SET version = 7;
            ",
        )?;
        Ok(())
    }

    /// V8: historical job ledger and replay metadata for backfill/backtests.
    fn migrate_v8(&self) -> Result<(), DbError> {
        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS historical_job_runs (
              id TEXT PRIMARY KEY,
              job_type TEXT NOT NULL,
              status TEXT NOT NULL,
              params_json TEXT NOT NULL,
              progress_json TEXT NOT NULL DEFAULT '{}',
              result_json TEXT NULL,
              warning_json TEXT NULL,
              error_text TEXT NULL,
              submitted_at_ms REAL NOT NULL,
              started_at_ms REAL NULL,
              finished_at_ms REAL NULL
            );
            CREATE INDEX IF NOT EXISTS idx_historical_job_runs_status_submitted
              ON historical_job_runs(status, submitted_at_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_historical_job_runs_type_submitted
              ON historical_job_runs(job_type, submitted_at_ms DESC);
            ",
        )?;

        let _ = self
            .conn
            .execute_batch("ALTER TABLE playbook_signals ADD COLUMN signal_id TEXT NULL");
        let _ = self
            .conn
            .execute_batch("ALTER TABLE playbook_signals ADD COLUMN session_date TEXT NULL");
        let _ = self.conn.execute_batch(
            "ALTER TABLE playbook_signals ADD COLUMN source TEXT NOT NULL DEFAULT 'live'",
        );
        let _ = self
            .conn
            .execute_batch("ALTER TABLE playbook_signals ADD COLUMN job_id TEXT NULL");

        let _ = self
            .conn
            .execute_batch("ALTER TABLE signal_outcomes ADD COLUMN session_date TEXT NULL");
        let _ = self.conn.execute_batch(
            "ALTER TABLE signal_outcomes ADD COLUMN source TEXT NOT NULL DEFAULT 'live'",
        );
        let _ = self
            .conn
            .execute_batch("ALTER TABLE signal_outcomes ADD COLUMN job_id TEXT NULL");

        self.conn.execute_batch(
            "
            UPDATE playbook_signals
            SET signal_id = COALESCE(signal_id, setup_id || '_' || CAST(timestamp_ms AS INTEGER))
            WHERE signal_id IS NULL OR signal_id = '';
            UPDATE playbook_signals
            SET session_date = COALESCE(
              session_date,
              date((timestamp_ms / 1000.0), 'unixepoch')
            )
            WHERE session_date IS NULL OR session_date = '';
            UPDATE signal_outcomes
            SET session_date = COALESCE(
              session_date,
              date((fired_at_ms / 1000.0), 'unixepoch')
            )
            WHERE session_date IS NULL OR session_date = '';

            DELETE FROM playbook_signals
            WHERE id NOT IN (
              SELECT MIN(id) FROM playbook_signals GROUP BY signal_id
            );

            CREATE UNIQUE INDEX IF NOT EXISTS ux_playbook_signals_signal_id
              ON playbook_signals(signal_id);
            CREATE INDEX IF NOT EXISTS idx_playbook_signals_session_setup
              ON playbook_signals(session_date, setup_id);
            CREATE INDEX IF NOT EXISTS idx_playbook_signals_job_id
              ON playbook_signals(job_id);
            CREATE INDEX IF NOT EXISTS idx_signal_outcomes_session_setup
              ON signal_outcomes(session_date, setup_id);
            CREATE INDEX IF NOT EXISTS idx_signal_outcomes_job_id
              ON signal_outcomes(job_id);

            UPDATE schema_version SET version = 8;
            ",
        )?;
        Ok(())
    }

    /// V9: persisted market-event session context (type/segment/trading day).
    fn migrate_v9(&self) -> Result<(), DbError> {
        let _ = self
            .conn
            .execute_batch("ALTER TABLE market_events ADD COLUMN session_type TEXT NULL");
        let _ = self
            .conn
            .execute_batch("ALTER TABLE market_events ADD COLUMN session_segment TEXT NULL");
        let _ = self
            .conn
            .execute_batch("ALTER TABLE market_events ADD COLUMN trading_day TEXT NULL");

        self.conn.execute_batch(
            "
            CREATE INDEX IF NOT EXISTS idx_market_events_event_trading_day
              ON market_events(event_type, trading_day);
            CREATE INDEX IF NOT EXISTS idx_market_events_day_session_segment
              ON market_events(trading_day, session_type, session_segment);
            ",
        )?;

        {
            let mut stmt = self.conn.prepare(
                "SELECT id, timestamp_ms, session_date,
                        COALESCE(session_type, ''), COALESCE(session_segment, ''), COALESCE(trading_day, '')
                 FROM market_events",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, f64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                ))
            })?;
            let mut updates: Vec<(i64, String, String, String)> = Vec::new();
            for row in rows.filter_map(|r| r.ok()) {
                let (id, ts, session_date, stored_type, stored_segment, stored_day) = row;
                let has_type = !stored_type.trim().is_empty();
                let has_segment = !stored_segment.trim().is_empty();
                let has_day = !stored_day.trim().is_empty();
                if has_type && has_segment && has_day {
                    continue;
                }
                if let Some(ctx) = tick_time_context_from_timestamp_ms(ts) {
                    let session_type = match ctx.session_type {
                        crate::SessionType::Rth => "RTH".to_string(),
                        crate::SessionType::Globex => "Globex".to_string(),
                        crate::SessionType::Unknown => "Unknown".to_string(),
                    };
                    let session_segment = if session_type == "Globex" {
                        match ctx.session_segment {
                            crate::SessionSegment::Asia => "Asia".to_string(),
                            crate::SessionSegment::London => "London".to_string(),
                            crate::SessionSegment::None => "None".to_string(),
                        }
                    } else {
                        "None".to_string()
                    };
                    updates.push((id, session_type, session_segment, ctx.trading_day));
                } else {
                    updates.push((id, "Unknown".to_string(), "None".to_string(), session_date));
                }
            }
            drop(stmt);

            if !updates.is_empty() {
                let tx = self.conn.unchecked_transaction()?;
                {
                    let mut update_stmt = tx.prepare(
                        "UPDATE market_events
                         SET session_type = ?2, session_segment = ?3, trading_day = ?4
                         WHERE id = ?1",
                    )?;
                    for (id, session_type, session_segment, trading_day) in &updates {
                        update_stmt.execute(params![
                            id,
                            session_type,
                            session_segment,
                            trading_day
                        ])?;
                    }
                }
                tx.commit()?;
            }
        }

        self.conn
            .execute("UPDATE market_events SET session_type='Unknown' WHERE session_type IS NULL OR session_type=''", [])?;
        self.conn
            .execute("UPDATE market_events SET session_segment='None' WHERE session_segment IS NULL OR session_segment=''", [])?;
        self.conn.execute(
            "UPDATE market_events
             SET trading_day = session_date
             WHERE trading_day IS NULL OR trading_day = ''",
            [],
        )?;

        self.conn
            .execute_batch("UPDATE schema_version SET version = 9;")?;
        Ok(())
    }

    /// V10: add depth/DOM storage tables for delayed DOM reconstruction.
    fn migrate_v10(&self) -> Result<(), DbError> {
        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS depth_events (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              source_file TEXT NOT NULL,
              timestamp_ms REAL NOT NULL,
              side TEXT NULL,
              command TEXT NOT NULL,
              price REAL NOT NULL,
              quantity REAL NOT NULL,
              num_orders INTEGER NOT NULL DEFAULT 0,
              end_of_batch INTEGER NOT NULL DEFAULT 0,
              batch_id INTEGER NULL,
              trading_day TEXT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_depth_events_ts
              ON depth_events(timestamp_ms);
            CREATE INDEX IF NOT EXISTS idx_depth_events_day_command
              ON depth_events(trading_day, command);

            CREATE TABLE IF NOT EXISTS dom_snapshots (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              source_file TEXT NOT NULL,
              timestamp_ms REAL NOT NULL,
              trading_day TEXT NOT NULL,
              payload TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_dom_snapshots_ts
              ON dom_snapshots(timestamp_ms);
            CREATE INDEX IF NOT EXISTS idx_dom_snapshots_day
              ON dom_snapshots(trading_day, timestamp_ms);

            CREATE TABLE IF NOT EXISTS dom_feature_snapshots (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              source_file TEXT NOT NULL,
              timestamp_ms REAL NOT NULL,
              trading_day TEXT NOT NULL,
              payload TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_dom_feature_snapshots_ts
              ON dom_feature_snapshots(timestamp_ms);
            CREATE INDEX IF NOT EXISTS idx_dom_feature_snapshots_day
              ON dom_feature_snapshots(trading_day, timestamp_ms);

            UPDATE schema_version SET version = 10;
            ",
        )?;
        Ok(())
    }

    /// V11: RVOL volume curves table + signal outcome RVOL context columns.
    fn migrate_v11(&self) -> Result<(), DbError> {
        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS session_volume_curves (
              session_date TEXT NOT NULL,
              session_type TEXT NOT NULL,
              bucket_index INTEGER NOT NULL,
              cumulative_volume REAL NOT NULL,
              PRIMARY KEY (session_date, session_type, bucket_index)
            );
            CREATE INDEX IF NOT EXISTS idx_svc_type_date
              ON session_volume_curves(session_type, session_date);

            UPDATE schema_version SET version = 11;
            ",
        )?;
        // Add RVOL columns to signal_outcomes (ignore if already present).
        let _ = self
            .conn
            .execute_batch("ALTER TABLE signal_outcomes ADD COLUMN rvol_at_fire REAL;");
        let _ = self
            .conn
            .execute_batch("ALTER TABLE signal_outcomes ADD COLUMN rvol_bucket_at_fire INTEGER;");
        Ok(())
    }

    /// V12: session_summaries composite primary key (session_date, session_type)
    /// to support Asia, London, and RTH sessions per date.
    fn migrate_v12(&self) -> Result<(), DbError> {
        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS session_summaries_new (
              session_date TEXT NOT NULL,
              session_type TEXT NOT NULL,
              open_price REAL, high REAL, low REAL, close REAL,
              poc REAL, vah REAL, val REAL,
              ib_high REAL, ib_low REAL, ib_range REAL,
              ib_mid REAL,
              or_high REAL, or_low REAL,
              day_type TEXT,
              total_volume REAL, tick_count INTEGER,
              session_delta REAL, cumulative_delta REAL,
              dnp REAL, dnva_high REAL, dnva_low REAL,
              vwap_close REAL,
              signal_count INTEGER DEFAULT 0,
              single_prints_direction TEXT,
              excess_high INTEGER DEFAULT 0, excess_low INTEGER DEFAULT 0,
              poor_high INTEGER DEFAULT 0, poor_low INTEGER DEFAULT 0,
              rvol_ratio REAL,
              close_vs_ib_mid TEXT,
              close_vs_vwap TEXT,
              close_vs_poc TEXT,
              snapshot_json TEXT,
              profile_shape TEXT NOT NULL DEFAULT '',
              balance_state TEXT NOT NULL DEFAULT '',
              PRIMARY KEY (session_date, session_type)
            );
            INSERT INTO session_summaries_new SELECT
              session_date,
              COALESCE(NULLIF(TRIM(session_type), ''), 'RTH'),
              open_price, high, low, close,
              poc, vah, val,
              ib_high, ib_low, ib_range, ib_mid,
              or_high, or_low,
              day_type,
              total_volume, tick_count,
              session_delta, cumulative_delta,
              dnp, dnva_high, dnva_low,
              vwap_close,
              signal_count,
              single_prints_direction,
              excess_high, excess_low, poor_high, poor_low,
              rvol_ratio,
              close_vs_ib_mid, close_vs_vwap, close_vs_poc,
              snapshot_json,
              profile_shape,
              balance_state
            FROM session_summaries;
            DROP TABLE session_summaries;
            ALTER TABLE session_summaries_new RENAME TO session_summaries;
            UPDATE schema_version SET version = 12;
            ",
        )?;
        Ok(())
    }

    /// V13: prior_day_levels DNVA columns for RTH.
    fn migrate_v13(&self) -> Result<(), DbError> {
        let columns = [("dnva_high", "REAL"), ("dnva_low", "REAL"), ("dnp", "REAL")];
        for (col, def) in &columns {
            let sql = format!("ALTER TABLE prior_day_levels ADD COLUMN {col} {def}");
            let _ = self.conn.execute(&sql, []);
        }
        self.conn
            .execute_batch("UPDATE schema_version SET version = 13;")?;
        Ok(())
    }

    /// V14: untested_dnps table for DNPs that were not revisited during a session.
    fn migrate_v14(&self) -> Result<(), DbError> {
        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS untested_dnps (
              session_date TEXT NOT NULL,
              session_type TEXT NOT NULL,
              dnp REAL NOT NULL,
              created_at INTEGER NOT NULL,
              PRIMARY KEY (session_date, session_type)
            );
            CREATE INDEX IF NOT EXISTS idx_untested_dnps_created
              ON untested_dnps (created_at DESC);
            UPDATE schema_version SET version = 14;
            ",
        )?;
        Ok(())
    }

    /// V15: index untested DNP values to speed up range-touch cleanup.
    fn migrate_v15(&self) -> Result<(), DbError> {
        self.conn.execute_batch(
            "
            CREATE INDEX IF NOT EXISTS idx_untested_dnps_dnp
              ON untested_dnps (dnp);
            UPDATE schema_version SET version = 15;
            ",
        )?;
        Ok(())
    }

    /// V16: remove legacy `absorption_events` table. `market_events` is canonical.
    fn migrate_v16(&self) -> Result<(), DbError> {
        self.conn.execute_batch(
            "
            DROP TABLE IF EXISTS absorption_events;
            UPDATE schema_version SET version = 16;
            ",
        )?;
        Ok(())
    }

    /// V17: trade journal enrichment fields plus fill-import ledger tables.
    fn migrate_v17(&self) -> Result<(), DbError> {
        let trade_columns = [
            ("instrument", "TEXT NULL"),
            ("trade_account", "TEXT NULL"),
            ("max_open_size", "INTEGER NULL"),
            ("gross_points", "REAL NULL"),
            ("thesis", "TEXT NULL"),
            ("review_tags", "TEXT NOT NULL DEFAULT '[]'"),
            ("mistake_tags", "TEXT NOT NULL DEFAULT '[]'"),
            ("entry_fill_count", "INTEGER NOT NULL DEFAULT 1"),
            ("exit_fill_count", "INTEGER NOT NULL DEFAULT 0"),
            ("import_batch_id", "TEXT NULL"),
        ];
        for (col, def) in &trade_columns {
            let sql = format!("ALTER TABLE trades ADD COLUMN {col} {def}");
            let _ = self.conn.execute(&sql, []);
        }

        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS trade_import_batches (
              batch_id TEXT PRIMARY KEY,
              source TEXT NOT NULL,
              imported_at REAL NOT NULL,
              notes TEXT NOT NULL DEFAULT '',
              fill_count INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS trade_fill_imports (
              fingerprint TEXT PRIMARY KEY,
              batch_id TEXT NOT NULL REFERENCES trade_import_batches(batch_id),
              trade_id TEXT NULL REFERENCES trades(id),
              symbol TEXT NOT NULL,
              trade_account TEXT NULL,
              fill_time REAL NOT NULL,
              order_side TEXT NOT NULL,
              open_close TEXT NULL,
              quantity INTEGER NOT NULL,
              price REAL NOT NULL,
              status TEXT NOT NULL,
              external_order_id TEXT NULL,
              service_order_id TEXT NULL,
              raw_payload TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_trade_import_batches_time
              ON trade_import_batches(imported_at DESC);
            CREATE INDEX IF NOT EXISTS idx_trade_fill_imports_batch
              ON trade_fill_imports(batch_id);
            CREATE INDEX IF NOT EXISTS idx_trade_fill_imports_trade
              ON trade_fill_imports(trade_id);
            CREATE INDEX IF NOT EXISTS idx_trade_fill_imports_time
              ON trade_fill_imports(fill_time);
            UPDATE schema_version SET version = 17;
            ",
        )?;
        Ok(())
    }

    /// V18: memory system tables for agent insights, behavioral patterns, and follow-ups.
    fn migrate_v18(&self) -> Result<(), DbError> {
        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS agent_insights (
              id TEXT PRIMARY KEY,
              created_at_ms REAL NOT NULL,
              updated_at_ms REAL NOT NULL,
              session_id TEXT NULL REFERENCES sessions(id),
              trade_id TEXT NULL REFERENCES trades(id),
              setup_id TEXT NULL REFERENCES setups(id),
              category TEXT NOT NULL,
              status TEXT NOT NULL,
              summary TEXT NOT NULL,
              evidence_json TEXT NOT NULL,
              tags_json TEXT NOT NULL DEFAULT '[]',
              scope_json TEXT NOT NULL DEFAULT '{}',
              confidence REAL NOT NULL DEFAULT 0.5,
              salience REAL NOT NULL DEFAULT 0.5,
              times_surfaced INTEGER NOT NULL DEFAULT 0,
              last_surfaced_ms REAL NULL,
              superseded_by TEXT NULL,
              source TEXT NOT NULL DEFAULT 'agent',
              helpful_count INTEGER NOT NULL DEFAULT 0,
              irrelevant_count INTEGER NOT NULL DEFAULT 0,
              wrong_count INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_agent_insights_status
              ON agent_insights(status, updated_at_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_agent_insights_setup
              ON agent_insights(setup_id, category);
            CREATE INDEX IF NOT EXISTS idx_agent_insights_session
              ON agent_insights(session_id);

            CREATE TABLE IF NOT EXISTS behavioral_patterns (
              id TEXT PRIMARY KEY,
              detected_at_ms REAL NOT NULL,
              pattern_type TEXT NOT NULL,
              description TEXT NOT NULL,
              metric_json TEXT NOT NULL,
              scope_json TEXT NOT NULL DEFAULT '{}',
              sample_size INTEGER NOT NULL,
              confidence REAL NOT NULL DEFAULT 0.5,
              active INTEGER NOT NULL DEFAULT 1,
              superseded_by TEXT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_behavioral_patterns_active
              ON behavioral_patterns(active, pattern_type, sample_size DESC);

            CREATE TABLE IF NOT EXISTS memory_followups (
              id TEXT PRIMARY KEY,
              created_at_ms REAL NOT NULL,
              resolved_at_ms REAL NULL,
              session_id TEXT NULL REFERENCES sessions(id),
              trade_id TEXT NULL REFERENCES trades(id),
              source TEXT NOT NULL DEFAULT 'agent',
              title TEXT NOT NULL,
              detail TEXT NOT NULL DEFAULT '',
              status TEXT NOT NULL DEFAULT 'open',
              tags_json TEXT NOT NULL DEFAULT '[]',
              due_context_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS idx_memory_followups_status
              ON memory_followups(status, created_at_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_memory_followups_session
              ON memory_followups(session_id);
            UPDATE schema_version SET version = 18;
            ",
        )?;
        Ok(())
    }

    /// V19: contract-aware storage and rollover safety metadata.
    fn migrate_v19(&self) -> Result<(), DbError> {
        let alter_statements = [
            "ALTER TABLE raw_ticks ADD COLUMN root_symbol TEXT NULL",
            "ALTER TABLE raw_ticks ADD COLUMN contract_symbol TEXT NULL",
            "ALTER TABLE market_events ADD COLUMN root_symbol TEXT NULL",
            "ALTER TABLE market_events ADD COLUMN contract_symbol TEXT NULL",
            "ALTER TABLE session_summaries ADD COLUMN root_symbol TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE session_summaries ADD COLUMN contract_symbol TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE session_summaries ADD COLUMN contract_month TEXT NULL",
            "ALTER TABLE session_summaries ADD COLUMN symbol_resolution_mode TEXT NOT NULL DEFAULT 'hybrid'",
            "ALTER TABLE session_summaries ADD COLUMN carry_forward_levels_valid INTEGER NOT NULL DEFAULT 1",
            "ALTER TABLE session_summaries ADD COLUMN rollover_warning TEXT NULL",
            "ALTER TABLE prior_day_levels ADD COLUMN root_symbol TEXT NULL",
            "ALTER TABLE prior_day_levels ADD COLUMN contract_symbol TEXT NULL",
            "ALTER TABLE playbook_signals ADD COLUMN root_symbol TEXT NULL",
            "ALTER TABLE playbook_signals ADD COLUMN contract_symbol TEXT NULL",
            "ALTER TABLE signal_outcomes ADD COLUMN root_symbol TEXT NULL",
            "ALTER TABLE signal_outcomes ADD COLUMN contract_symbol TEXT NULL",
            "ALTER TABLE session_volume_curves ADD COLUMN root_symbol TEXT NULL",
            "ALTER TABLE session_volume_curves ADD COLUMN contract_symbol TEXT NULL",
            "ALTER TABLE untested_dnps ADD COLUMN contract_symbol TEXT NULL",
        ];
        for sql in alter_statements {
            let _ = self.conn.execute(sql, []);
        }

        self.conn.execute_batch(
            "
            DROP INDEX IF EXISTS ux_raw_ticks_identity;
            CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_ticks_identity
              ON raw_ticks(timestamp_ms, price, volume, bid, ask, is_buy, session_date, COALESCE(contract_symbol, ''));

            DROP INDEX IF EXISTS ux_market_events_identity;
            CREATE UNIQUE INDEX IF NOT EXISTS ux_market_events_identity
              ON market_events(
                session_date,
                timestamp_ms,
                event_type,
                COALESCE(level_name, ''),
                price,
                COALESCE(direction, ''),
                COALESCE(sequence_num, -1),
                COALESCE(contract_symbol, '')
              );

            CREATE INDEX IF NOT EXISTS idx_raw_ticks_contract_time
              ON raw_ticks(contract_symbol, session_date, timestamp_ms);
            CREATE INDEX IF NOT EXISTS idx_market_events_contract_day
              ON market_events(contract_symbol, trading_day, event_type);
            CREATE INDEX IF NOT EXISTS idx_session_summaries_contract_day
              ON session_summaries(contract_symbol, session_date, session_type);
            CREATE INDEX IF NOT EXISTS idx_session_summaries_root_day
              ON session_summaries(root_symbol, session_date, session_type);
            CREATE INDEX IF NOT EXISTS idx_prior_day_levels_contract
              ON prior_day_levels(contract_symbol, date);
            CREATE INDEX IF NOT EXISTS idx_signal_outcomes_contract_day
              ON signal_outcomes(contract_symbol, session_date, setup_id);
            CREATE INDEX IF NOT EXISTS idx_playbook_signals_contract_day
              ON playbook_signals(contract_symbol, session_date, setup_id);
            CREATE INDEX IF NOT EXISTS idx_session_volume_curves_contract
              ON session_volume_curves(contract_symbol, session_date, session_type);
            CREATE INDEX IF NOT EXISTS idx_untested_dnps_contract
              ON untested_dnps(contract_symbol, created_at DESC);

            UPDATE schema_version SET version = 19;
            ",
        )?;
        Ok(())
    }

    /// V20: explicit memory maintenance freshness state for read-only briefing tools.
    fn migrate_v20(&self) -> Result<(), DbError> {
        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS memory_maintenance_state (
              singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
              patterns_last_refreshed_at_ms REAL NULL,
              insights_lifecycle_last_refreshed_at_ms REAL NULL,
              patterns_dirty INTEGER NOT NULL DEFAULT 1,
              insights_lifecycle_dirty INTEGER NOT NULL DEFAULT 1,
              dirty_since_ms REAL NULL,
              dirty_reasons_json TEXT NOT NULL DEFAULT '[]',
              last_refresh_reason TEXT NULL
            );
            INSERT OR IGNORE INTO memory_maintenance_state (
              singleton,
              patterns_last_refreshed_at_ms,
              insights_lifecycle_last_refreshed_at_ms,
              patterns_dirty,
              insights_lifecycle_dirty,
              dirty_since_ms,
              dirty_reasons_json,
              last_refresh_reason
            ) VALUES (1, NULL, NULL, 1, 1, NULL, '[]', NULL);
            UPDATE schema_version SET version = 20;
            ",
        )?;
        Ok(())
    }

    // ------------------------------------------------------------------
    // Setup CRUD
    // ------------------------------------------------------------------

    fn decode_setup_row(row: &Row<'_>) -> rusqlite::Result<SetupDefinition> {
        let conditions_str: String = row.get(4)?;
        let conditions: Vec<String> = serde_json::from_str(&conditions_str).unwrap_or_default();
        let entry_logic: serde_json::Value = row
            .get::<_, String>(8)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or(serde_json::json!({}));
        let stop_logic: serde_json::Value = row
            .get::<_, String>(9)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or(serde_json::json!({}));
        let targets: Vec<serde_json::Value> = row
            .get::<_, String>(10)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();
        let position_sizing: serde_json::Value = row
            .get::<_, String>(11)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or(serde_json::json!({}));
        let market_context: serde_json::Value = row
            .get::<_, String>(12)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or(serde_json::json!({}));
        let invalidation: Vec<serde_json::Value> = row
            .get::<_, String>(13)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();
        let backtest_results: serde_json::Value = row
            .get::<_, String>(14)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or(serde_json::json!({}));
        let context_backtest: Vec<serde_json::Value> = row
            .get::<_, String>(15)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();
        let discretionary: Vec<String> = row
            .get::<_, String>(16)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();
        let template_source: Option<String> = row.get(17).ok().flatten();

        Ok(SetupDefinition {
            id: row.get(0)?,
            name: row.get(1)?,
            description: row.get(2)?,
            active: row.get::<_, i64>(3)? == 1,
            conditions,
            min_delta: row.get(5)?,
            require_above_vwap: row.get::<_, i64>(6)? == 1,
            duplicate_suppression_ms: row.get::<_, i64>(7)? as u64,
            entry_logic,
            stop_logic,
            targets,
            position_sizing,
            market_context,
            invalidation,
            backtest_results,
            context_backtest_results: context_backtest,
            discretionary_conditions: discretionary,
            template_source,
        })
    }

    pub fn upsert_setup(&self, setup: &SetupDefinition) -> Result<(), DbError> {
        let conditions_json =
            serde_json::to_string(&setup.conditions).unwrap_or_else(|_| "[]".to_string());
        let entry_logic = serde_json::to_string(&setup.entry_logic).unwrap_or_else(|_| "{}".into());
        let stop_logic = serde_json::to_string(&setup.stop_logic).unwrap_or_else(|_| "{}".into());
        let targets = serde_json::to_string(&setup.targets).unwrap_or_else(|_| "[]".into());
        let position_sizing =
            serde_json::to_string(&setup.position_sizing).unwrap_or_else(|_| "{}".into());
        let market_context =
            serde_json::to_string(&setup.market_context).unwrap_or_else(|_| "{}".into());
        let invalidation =
            serde_json::to_string(&setup.invalidation).unwrap_or_else(|_| "[]".into());
        let backtest_results =
            serde_json::to_string(&setup.backtest_results).unwrap_or_else(|_| "{}".into());
        let context_backtest =
            serde_json::to_string(&setup.context_backtest_results).unwrap_or_else(|_| "[]".into());
        let discretionary =
            serde_json::to_string(&setup.discretionary_conditions).unwrap_or_else(|_| "[]".into());

        self.conn.execute(
            "INSERT INTO setups (id, name, description, active, conditions, min_delta,
                require_above_vwap, duplicate_suppression_ms, entry_logic, stop_logic,
                targets, position_sizing, market_context, invalidation,
                backtest_results, context_backtest_results, discretionary_conditions, template_source)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)
            ON CONFLICT(id) DO UPDATE SET
              name=excluded.name, description=excluded.description,
              active=excluded.active, conditions=excluded.conditions,
              min_delta=excluded.min_delta, require_above_vwap=excluded.require_above_vwap,
              duplicate_suppression_ms=excluded.duplicate_suppression_ms,
              entry_logic=excluded.entry_logic, stop_logic=excluded.stop_logic,
              targets=excluded.targets, position_sizing=excluded.position_sizing,
              market_context=excluded.market_context, invalidation=excluded.invalidation,
              backtest_results=excluded.backtest_results,
              context_backtest_results=excluded.context_backtest_results,
              discretionary_conditions=excluded.discretionary_conditions,
              template_source=excluded.template_source",
            params![
                setup.id,
                setup.name,
                setup.description,
                i64::from(setup.active),
                conditions_json,
                setup.min_delta,
                i64::from(setup.require_above_vwap),
                setup.duplicate_suppression_ms as i64,
                entry_logic,
                stop_logic,
                targets,
                position_sizing,
                market_context,
                invalidation,
                backtest_results,
                context_backtest,
                discretionary,
                setup.template_source,
            ],
        )?;
        Ok(())
    }

    pub fn list_setups(&self) -> Result<Vec<SetupDefinition>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, description, active, conditions, min_delta,
                    require_above_vwap, duplicate_suppression_ms,
                    entry_logic, stop_logic, targets, position_sizing,
                    market_context, invalidation, backtest_results,
                    context_backtest_results, discretionary_conditions, template_source
             FROM setups",
        )?;
        let rows = stmt.query_map([], Self::decode_setup_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn list_active_setups(&self) -> Result<Vec<SetupDefinition>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, description, active, conditions, min_delta,
                    require_above_vwap, duplicate_suppression_ms,
                    entry_logic, stop_logic, targets, position_sizing,
                    market_context, invalidation, backtest_results,
                    context_backtest_results, discretionary_conditions, template_source
             FROM setups
             WHERE active = 1",
        )?;
        let rows = stmt.query_map([], Self::decode_setup_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Load active setup definitions and the current risk gate used by live rules evaluation.
    pub fn load_playbook_runtime_seed(&self) -> Result<(Vec<SetupDefinition>, bool), DbError> {
        let active_setups = self.list_active_setups()?;
        let risk_at_limit = self
            .load_risk_state()?
            .map(|state| state.at_limit)
            .unwrap_or(false);
        Ok((active_setups, risk_at_limit))
    }

    pub fn delete_setup(&self, id: &str) -> Result<(), DbError> {
        self.conn
            .execute("DELETE FROM setups WHERE id = ?1", params![id])?;
        Ok(())
    }

    pub fn toggle_setup(&self, id: &str, active: bool) -> Result<(), DbError> {
        self.conn.execute(
            "UPDATE setups SET active = ?1 WHERE id = ?2",
            params![i64::from(active), id],
        )?;
        Ok(())
    }

    pub fn get_setup(&self, id: &str) -> Result<Option<SetupDefinition>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, description, active, conditions, min_delta,
                    require_above_vwap, duplicate_suppression_ms,
                    entry_logic, stop_logic, targets, position_sizing,
                    market_context, invalidation, backtest_results,
                    context_backtest_results, discretionary_conditions, template_source
             FROM setups
             WHERE id = ?1
             LIMIT 1",
        )?;
        let mut rows = stmt.query(params![id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(Self::decode_setup_row(row)?))
        } else {
            Ok(None)
        }
    }

    // ------------------------------------------------------------------
    // Session CRUD
    // ------------------------------------------------------------------

    pub fn create_session(&self, session: &SessionRecord) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO sessions (id, date, session_type, start_time, end_time, recording_path, pre_session_note)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                session.id, session.date, session.session_type,
                session.start_time, session.end_time,
                session.recording_path, session.pre_session_note,
            ],
        )?;
        Ok(())
    }

    pub fn upsert_session(&self, session: &SessionRecord) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO sessions (id, date, session_type, start_time, end_time, recording_path, pre_session_note)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(id) DO UPDATE SET
               date = excluded.date,
               session_type = excluded.session_type,
               start_time = excluded.start_time,
               end_time = excluded.end_time,
               recording_path = excluded.recording_path,
               pre_session_note = excluded.pre_session_note",
            params![
                session.id,
                session.date,
                session.session_type,
                session.start_time,
                session.end_time,
                session.recording_path,
                session.pre_session_note,
            ],
        )?;
        Ok(())
    }

    pub fn update_session_end(
        &self,
        id: &str,
        end_time: f64,
        recording_path: Option<&str>,
    ) -> Result<(), DbError> {
        self.conn.execute(
            "UPDATE sessions SET end_time = ?1, recording_path = ?2 WHERE id = ?3",
            params![end_time, recording_path, id],
        )?;
        Ok(())
    }

    pub fn get_session(&self, id: &str) -> Result<Option<SessionRecord>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, date, session_type, start_time, end_time, recording_path, pre_session_note
             FROM sessions WHERE id = ?1",
        )?;
        let mut rows = stmt.query(params![id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(SessionRecord {
                id: row.get(0)?,
                date: row.get(1)?,
                session_type: row.get(2)?,
                start_time: row.get(3)?,
                end_time: row.get(4)?,
                recording_path: row.get(5)?,
                pre_session_note: row.get(6)?,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn list_sessions(&self, limit: usize) -> Result<Vec<SessionRecord>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, date, session_type, start_time, end_time, recording_path, pre_session_note
             FROM sessions ORDER BY start_time DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map([limit as i64], |row| {
            Ok(SessionRecord {
                id: row.get(0)?,
                date: row.get(1)?,
                session_type: row.get(2)?,
                start_time: row.get(3)?,
                end_time: row.get(4)?,
                recording_path: row.get(5)?,
                pre_session_note: row.get(6)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn get_latest_open_session(&self) -> Result<Option<SessionRecord>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, date, session_type, start_time, end_time, recording_path, pre_session_note
             FROM sessions
             WHERE end_time IS NULL
             ORDER BY start_time DESC
             LIMIT 1",
        )?;
        let mut rows = stmt.query([])?;
        if let Some(row) = rows.next()? {
            Ok(Some(SessionRecord {
                id: row.get(0)?,
                date: row.get(1)?,
                session_type: row.get(2)?,
                start_time: row.get(3)?,
                end_time: row.get(4)?,
                recording_path: row.get(5)?,
                pre_session_note: row.get(6)?,
            }))
        } else {
            Ok(None)
        }
    }

    // ------------------------------------------------------------------
    // Trade CRUD
    // ------------------------------------------------------------------

    fn trade_from_row(row: &rusqlite::Row<'_>) -> Result<TradeRecord, rusqlite::Error> {
        let target_prices: Vec<f64> = row
            .get::<_, String>(13)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();
        let review_tags: Vec<String> = row
            .get::<_, String>(20)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();
        let mistake_tags: Vec<String> = row
            .get::<_, String>(21)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();
        Ok(TradeRecord {
            id: row.get(0)?,
            session_id: row.get(1)?,
            setup_id: row.get(2)?,
            instrument: row.get(3)?,
            trade_account: row.get(4)?,
            entry_time: row.get(5)?,
            entry_price: row.get(6)?,
            exit_time: row.get(7)?,
            exit_price: row.get(8)?,
            direction: row.get(9)?,
            size: row.get(10)?,
            max_open_size: row.get(11)?,
            stop_price: row.get(12)?,
            target_prices,
            result_r: row.get(14)?,
            gross_points: row.get(15)?,
            planned: row.get::<_, i64>(16)? == 1,
            rules_followed: row.get::<_, Option<i64>>(17)?.map(|v| v == 1),
            emotional_state: row.get(18)?,
            thesis: row.get(19)?,
            review_tags,
            mistake_tags,
            entry_fill_count: row.get(22)?,
            exit_fill_count: row.get(23)?,
            import_batch_id: row.get(24)?,
            notes: row.get(25)?,
            source: row.get(26)?,
        })
    }

    pub fn insert_trade(&self, trade: &TradeRecord) -> Result<(), DbError> {
        let targets_json = serde_json::to_string(&trade.target_prices)?;
        let review_tags_json = serde_json::to_string(&trade.review_tags)?;
        let mistake_tags_json = serde_json::to_string(&trade.mistake_tags)?;
        self.conn.execute(
            "INSERT INTO trades (id, session_id, setup_id, instrument, trade_account, entry_time, entry_price,
                exit_time, exit_price, direction, size, max_open_size, stop_price, target_prices,
                result_r, gross_points, planned, rules_followed, emotional_state, thesis, review_tags,
                mistake_tags, entry_fill_count, exit_fill_count, import_batch_id, notes, source)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27)",
            params![
                trade.id,
                trade.session_id,
                trade.setup_id,
                trade.instrument,
                trade.trade_account,
                trade.entry_time,
                trade.entry_price,
                trade.exit_time,
                trade.exit_price,
                trade.direction,
                trade.size,
                trade.max_open_size,
                trade.stop_price,
                targets_json,
                trade.result_r,
                trade.gross_points,
                i64::from(trade.planned),
                trade.rules_followed.map(i64::from),
                trade.emotional_state,
                trade.thesis,
                review_tags_json,
                mistake_tags_json,
                trade.entry_fill_count,
                trade.exit_fill_count,
                trade.import_batch_id,
                trade.notes,
                trade.source,
            ],
        )?;
        Ok(())
    }

    pub fn upsert_trade(&self, trade: &TradeRecord) -> Result<(), DbError> {
        let targets_json = serde_json::to_string(&trade.target_prices)?;
        let review_tags_json = serde_json::to_string(&trade.review_tags)?;
        let mistake_tags_json = serde_json::to_string(&trade.mistake_tags)?;
        self.conn.execute(
            "INSERT INTO trades (id, session_id, setup_id, instrument, trade_account, entry_time, entry_price,
                exit_time, exit_price, direction, size, max_open_size, stop_price, target_prices,
                result_r, gross_points, planned, rules_followed, emotional_state, thesis, review_tags,
                mistake_tags, entry_fill_count, exit_fill_count, import_batch_id, notes, source)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27)
             ON CONFLICT(id) DO UPDATE SET
                session_id = excluded.session_id,
                setup_id = excluded.setup_id,
                instrument = excluded.instrument,
                trade_account = excluded.trade_account,
                entry_time = excluded.entry_time,
                entry_price = excluded.entry_price,
                exit_time = excluded.exit_time,
                exit_price = excluded.exit_price,
                direction = excluded.direction,
                size = excluded.size,
                max_open_size = excluded.max_open_size,
                stop_price = excluded.stop_price,
                target_prices = excluded.target_prices,
                result_r = excluded.result_r,
                gross_points = excluded.gross_points,
                planned = excluded.planned,
                rules_followed = excluded.rules_followed,
                emotional_state = excluded.emotional_state,
                thesis = excluded.thesis,
                review_tags = excluded.review_tags,
                mistake_tags = excluded.mistake_tags,
                entry_fill_count = excluded.entry_fill_count,
                exit_fill_count = excluded.exit_fill_count,
                import_batch_id = excluded.import_batch_id,
                notes = excluded.notes,
                source = excluded.source",
            params![
                trade.id,
                trade.session_id,
                trade.setup_id,
                trade.instrument,
                trade.trade_account,
                trade.entry_time,
                trade.entry_price,
                trade.exit_time,
                trade.exit_price,
                trade.direction,
                trade.size,
                trade.max_open_size,
                trade.stop_price,
                targets_json,
                trade.result_r,
                trade.gross_points,
                i64::from(trade.planned),
                trade.rules_followed.map(i64::from),
                trade.emotional_state,
                trade.thesis,
                review_tags_json,
                mistake_tags_json,
                trade.entry_fill_count,
                trade.exit_fill_count,
                trade.import_batch_id,
                trade.notes,
                trade.source,
            ],
        )?;
        Ok(())
    }

    pub fn update_trade_exit(
        &self,
        id: &str,
        exit_time: f64,
        exit_price: f64,
        result_r: f64,
    ) -> Result<(), DbError> {
        self.conn.execute(
            "UPDATE trades SET exit_time = ?1, exit_price = ?2, result_r = ?3 WHERE id = ?4",
            params![exit_time, exit_price, result_r, id],
        )?;
        Ok(())
    }

    pub fn update_trade_review(&self, id: &str, review: &TradeReviewUpdate) -> Result<(), DbError> {
        let review_tags_json = serde_json::to_string(&review.review_tags)?;
        let mistake_tags_json = serde_json::to_string(&review.mistake_tags)?;
        self.conn.execute(
            "UPDATE trades
             SET planned = ?1, rules_followed = ?2, emotional_state = ?3, thesis = ?4,
                 review_tags = ?5, mistake_tags = ?6, notes = ?7
             WHERE id = ?8",
            params![
                i64::from(review.planned),
                review.rules_followed.map(i64::from),
                review.emotional_state,
                review.thesis,
                review_tags_json,
                mistake_tags_json,
                review.notes,
                id
            ],
        )?;
        Ok(())
    }

    pub fn list_trades_for_session(&self, session_id: &str) -> Result<Vec<TradeRecord>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, session_id, setup_id, instrument, trade_account, entry_time, entry_price,
                    exit_time, exit_price, direction, size, max_open_size, stop_price, target_prices,
                    result_r, gross_points, planned, rules_followed, emotional_state, thesis,
                    review_tags, mistake_tags, entry_fill_count, exit_fill_count, import_batch_id,
                    notes, source
             FROM trades WHERE session_id = ?1 ORDER BY entry_time",
        )?;
        let rows = stmt.query_map([session_id], Self::trade_from_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn get_open_trade(&self, session_id: &str) -> Result<Option<TradeRecord>, DbError> {
        let trades = self.list_trades_for_session(session_id)?;
        Ok(trades.into_iter().find(|t| t.exit_time.is_none()))
    }

    pub fn get_trade(&self, id: &str) -> Result<Option<TradeRecord>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, session_id, setup_id, instrument, trade_account, entry_time, entry_price,
                    exit_time, exit_price, direction, size, max_open_size, stop_price, target_prices,
                    result_r, gross_points, planned, rules_followed, emotional_state, thesis,
                    review_tags, mistake_tags, entry_fill_count, exit_fill_count, import_batch_id,
                    notes, source
             FROM trades WHERE id = ?1",
        )?;
        let mut rows = stmt.query([id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(Self::trade_from_row(row)?))
        } else {
            Ok(None)
        }
    }

    pub fn list_recent_trades(&self, limit: usize) -> Result<Vec<TradeRecord>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, session_id, setup_id, instrument, trade_account, entry_time, entry_price,
                    exit_time, exit_price, direction, size, max_open_size, stop_price, target_prices,
                    result_r, gross_points, planned, rules_followed, emotional_state, thesis,
                    review_tags, mistake_tags, entry_fill_count, exit_fill_count, import_batch_id,
                    notes, source
             FROM trades ORDER BY entry_time DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map([limit as i64], Self::trade_from_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    // ------------------------------------------------------------------
    // Journal CRUD
    // ------------------------------------------------------------------

    pub fn insert_journal_entry(&self, entry: &JournalEntry) -> Result<(), DbError> {
        let tags = serde_json::to_string(&entry.tags)?;
        let setup_refs = serde_json::to_string(&entry.setup_references)?;
        let trade_refs = serde_json::to_string(&entry.trade_references)?;
        self.conn.execute(
            "INSERT INTO journal_entries (id, session_id, date, content, tags, setup_references, trade_references, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                entry.id, entry.session_id, entry.date, entry.content,
                tags, setup_refs, trade_refs, entry.created_at,
            ],
        )?;
        Ok(())
    }

    pub fn upsert_journal_entry(&self, entry: &JournalEntry) -> Result<(), DbError> {
        let tags = serde_json::to_string(&entry.tags)?;
        let setup_refs = serde_json::to_string(&entry.setup_references)?;
        let trade_refs = serde_json::to_string(&entry.trade_references)?;
        self.conn.execute(
            "INSERT INTO journal_entries (id, session_id, date, content, tags, setup_references, trade_references, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(id) DO UPDATE SET
               session_id = excluded.session_id,
               date = excluded.date,
               content = excluded.content,
               tags = excluded.tags,
               setup_references = excluded.setup_references,
               trade_references = excluded.trade_references,
               created_at = excluded.created_at",
            params![
                entry.id,
                entry.session_id,
                entry.date,
                entry.content,
                tags,
                setup_refs,
                trade_refs,
                entry.created_at,
            ],
        )?;
        Ok(())
    }

    pub fn update_journal_entry(
        &self,
        id: &str,
        content: &str,
        tags: &[String],
    ) -> Result<(), DbError> {
        let tags_json = serde_json::to_string(tags)?;
        self.conn.execute(
            "UPDATE journal_entries SET content = ?1, tags = ?2 WHERE id = ?3",
            params![content, tags_json, id],
        )?;
        Ok(())
    }

    pub fn get_journal_for_session(&self, session_id: &str) -> Result<Vec<JournalEntry>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, session_id, date, content, tags, setup_references, trade_references, created_at
             FROM journal_entries WHERE session_id = ?1 ORDER BY created_at",
        )?;
        let rows = stmt.query_map([session_id], |row| {
            let tags: Vec<String> = row
                .get::<_, String>(4)
                .ok()
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default();
            let setup_refs: Vec<String> = row
                .get::<_, String>(5)
                .ok()
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default();
            let trade_refs: Vec<String> = row
                .get::<_, String>(6)
                .ok()
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default();
            Ok(JournalEntry {
                id: row.get(0)?,
                session_id: row.get(1)?,
                date: row.get(2)?,
                content: row.get(3)?,
                tags,
                setup_references: setup_refs,
                trade_references: trade_refs,
                created_at: row.get(7)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn list_recent_journal_entries(&self, limit: usize) -> Result<Vec<JournalEntry>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, session_id, date, content, tags, setup_references, trade_references, created_at
             FROM journal_entries ORDER BY created_at DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map([limit as i64], |row| {
            let tags: Vec<String> = row
                .get::<_, String>(4)
                .ok()
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default();
            let setup_refs: Vec<String> = row
                .get::<_, String>(5)
                .ok()
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default();
            let trade_refs: Vec<String> = row
                .get::<_, String>(6)
                .ok()
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default();
            Ok(JournalEntry {
                id: row.get(0)?,
                session_id: row.get(1)?,
                date: row.get(2)?,
                content: row.get(3)?,
                tags,
                setup_references: setup_refs,
                trade_references: trade_refs,
                created_at: row.get(7)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn upsert_agent_insight(&self, insight: &AgentInsightRecord) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO agent_insights (
                id, created_at_ms, updated_at_ms, session_id, trade_id, setup_id, category, status,
                summary, evidence_json, tags_json, scope_json, confidence, salience,
                times_surfaced, last_surfaced_ms, superseded_by, source,
                helpful_count, irrelevant_count, wrong_count
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8,
                ?9, ?10, ?11, ?12, ?13, ?14,
                ?15, ?16, ?17, ?18,
                ?19, ?20, ?21
            )
            ON CONFLICT(id) DO UPDATE SET
                updated_at_ms = excluded.updated_at_ms,
                session_id = excluded.session_id,
                trade_id = excluded.trade_id,
                setup_id = excluded.setup_id,
                category = excluded.category,
                status = excluded.status,
                summary = excluded.summary,
                evidence_json = excluded.evidence_json,
                tags_json = excluded.tags_json,
                scope_json = excluded.scope_json,
                confidence = excluded.confidence,
                salience = excluded.salience,
                times_surfaced = excluded.times_surfaced,
                last_surfaced_ms = excluded.last_surfaced_ms,
                superseded_by = excluded.superseded_by,
                source = excluded.source,
                helpful_count = excluded.helpful_count,
                irrelevant_count = excluded.irrelevant_count,
                wrong_count = excluded.wrong_count",
            params![
                insight.id,
                insight.created_at_ms,
                insight.updated_at_ms,
                insight.session_id,
                insight.trade_id,
                insight.setup_id,
                insight.category,
                insight.status,
                insight.summary,
                serde_json::to_string(&insight.evidence)?,
                serde_json::to_string(&insight.tags)?,
                serde_json::to_string(&insight.scope)?,
                insight.confidence,
                insight.salience,
                insight.times_surfaced,
                insight.last_surfaced_ms,
                insight.superseded_by,
                insight.source,
                insight.helpful_count,
                insight.irrelevant_count,
                insight.wrong_count,
            ],
        )?;
        Ok(())
    }

    pub fn get_agent_insight(&self, id: &str) -> Result<Option<AgentInsightRecord>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, created_at_ms, updated_at_ms, session_id, trade_id, setup_id, category, status,
                    summary, evidence_json, tags_json, scope_json, confidence, salience,
                    times_surfaced, last_surfaced_ms, superseded_by, source,
                    helpful_count, irrelevant_count, wrong_count
             FROM agent_insights WHERE id = ?1",
        )?;
        let mut rows = stmt.query([id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(Self::agent_insight_from_row(row)?))
        } else {
            Ok(None)
        }
    }

    pub fn list_agent_insights(
        &self,
        query: &AgentInsightQuery,
    ) -> Result<Vec<AgentInsightRecord>, DbError> {
        let mut sql = String::from(
            "SELECT id, created_at_ms, updated_at_ms, session_id, trade_id, setup_id, category, status,
                    summary, evidence_json, tags_json, scope_json, confidence, salience,
                    times_surfaced, last_surfaced_ms, superseded_by, source,
                    helpful_count, irrelevant_count, wrong_count
             FROM agent_insights WHERE 1=1",
        );
        let mut params_vec: Vec<rusqlite::types::Value> = Vec::new();

        if let Some(category) = query.category.as_ref() {
            sql.push_str(&format!(" AND category = ?{}", params_vec.len() + 1));
            params_vec.push(category.clone().into());
        }
        if let Some(setup_id) = query.setup_id.as_ref() {
            sql.push_str(&format!(" AND setup_id = ?{}", params_vec.len() + 1));
            params_vec.push(setup_id.clone().into());
        }
        if let Some(statuses) = query.statuses.as_ref() {
            if !statuses.is_empty() {
                let placeholders = (0..statuses.len())
                    .map(|offset| format!("?{}", params_vec.len() + offset + 1))
                    .collect::<Vec<_>>()
                    .join(", ");
                sql.push_str(&format!(" AND status IN ({placeholders})"));
                for status in statuses {
                    params_vec.push(status.clone().into());
                }
            }
        }
        if let Some(start_date) = query.start_date.as_ref() {
            sql.push_str(&format!(
                " AND date(created_at_ms / 1000, 'unixepoch') >= ?{}",
                params_vec.len() + 1
            ));
            params_vec.push(start_date.clone().into());
        }
        if let Some(end_date) = query.end_date.as_ref() {
            sql.push_str(&format!(
                " AND date(created_at_ms / 1000, 'unixepoch') <= ?{}",
                params_vec.len() + 1
            ));
            params_vec.push(end_date.clone().into());
        }

        sql.push_str(" ORDER BY updated_at_ms DESC");
        if let Some(limit) = query.limit {
            sql.push_str(&format!(" LIMIT ?{}", params_vec.len() + 1));
            params_vec.push((limit as i64).into());
        }

        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(
            rusqlite::params_from_iter(params_vec),
            Self::agent_insight_from_row,
        )?;
        let mut items = rows.collect::<Result<Vec<_>, _>>()?;
        items.retain(|insight| {
            let tag_ok = query
                .tag
                .as_ref()
                .map(|tag| insight.tags.iter().any(|value| value == tag))
                .unwrap_or(true);
            let scope = &insight.scope;
            let session_type_ok = query
                .session_type
                .as_ref()
                .map(|session_type| {
                    scope
                        .get("sessionType")
                        .and_then(|value| value.as_str())
                        .map(|value| value.eq_ignore_ascii_case(session_type))
                        .unwrap_or(false)
                })
                .unwrap_or(true);
            let session_segment_ok = query
                .session_segment
                .as_ref()
                .map(|session_segment| {
                    scope
                        .get("sessionSegment")
                        .and_then(|value| value.as_str())
                        .map(|value| value.eq_ignore_ascii_case(session_segment))
                        .unwrap_or(false)
                })
                .unwrap_or(true);
            let time_bucket_ok = query
                .time_bucket
                .as_ref()
                .map(|time_bucket| {
                    scope
                        .get("timeBucket")
                        .and_then(|value| value.as_str())
                        .map(|value| value == time_bucket)
                        .unwrap_or(false)
                })
                .unwrap_or(true);
            let day_type_ok = query
                .day_type
                .as_ref()
                .map(|day_type| {
                    scope
                        .get("dayType")
                        .and_then(|value| value.as_str())
                        .map(|value| value.eq_ignore_ascii_case(day_type))
                        .unwrap_or(false)
                })
                .unwrap_or(true);
            tag_ok && session_type_ok && session_segment_ok && time_bucket_ok && day_type_ok
        });
        Ok(items)
    }

    pub fn update_agent_insight_status(
        &self,
        id: &str,
        status: &str,
        updated_at_ms: f64,
    ) -> Result<(), DbError> {
        self.conn.execute(
            "UPDATE agent_insights SET status = ?1, updated_at_ms = ?2 WHERE id = ?3",
            params![status, updated_at_ms, id],
        )?;
        Ok(())
    }

    pub fn acknowledge_agent_insight(
        &self,
        id: &str,
        action: &str,
        surfaced_at_ms: f64,
    ) -> Result<Option<AgentInsightRecord>, DbError> {
        let insight = match self.get_agent_insight(id)? {
            Some(insight) => insight,
            None => return Ok(None),
        };
        let mut updated = insight.clone();
        updated.times_surfaced += 1;
        updated.last_surfaced_ms = Some(surfaced_at_ms);
        updated.updated_at_ms = surfaced_at_ms;
        match action {
            "helpful" => updated.helpful_count += 1,
            "irrelevant" => updated.irrelevant_count += 1,
            "wrong" => {
                updated.wrong_count += 1;
                updated.status = INSIGHT_DISMISSED.to_string();
            }
            "pin" => updated.status = INSIGHT_PINNED.to_string(),
            _ => {}
        }
        self.upsert_agent_insight(&updated)?;
        Ok(Some(updated))
    }

    pub fn supersede_agent_insight(
        &self,
        previous_id: &str,
        replacement_id: &str,
        updated_at_ms: f64,
    ) -> Result<(), DbError> {
        self.conn.execute(
            "UPDATE agent_insights
             SET status = ?1, superseded_by = ?2, updated_at_ms = ?3
             WHERE id = ?4",
            params![
                INSIGHT_SUPERSEDED,
                replacement_id,
                updated_at_ms,
                previous_id
            ],
        )?;
        Ok(())
    }

    pub fn deactivate_behavioral_patterns(&self) -> Result<(), DbError> {
        self.conn
            .execute("UPDATE behavioral_patterns SET active = 0", [])?;
        Ok(())
    }

    pub fn upsert_behavioral_pattern(
        &self,
        pattern: &BehavioralPatternRecord,
    ) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO behavioral_patterns (
                id, detected_at_ms, pattern_type, description, metric_json,
                scope_json, sample_size, confidence, active, superseded_by
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
            ON CONFLICT(id) DO UPDATE SET
                detected_at_ms = excluded.detected_at_ms,
                pattern_type = excluded.pattern_type,
                description = excluded.description,
                metric_json = excluded.metric_json,
                scope_json = excluded.scope_json,
                sample_size = excluded.sample_size,
                confidence = excluded.confidence,
                active = excluded.active,
                superseded_by = excluded.superseded_by",
            params![
                pattern.id,
                pattern.detected_at_ms,
                pattern.pattern_type,
                pattern.description,
                serde_json::to_string(&pattern.metric)?,
                serde_json::to_string(&pattern.scope)?,
                pattern.sample_size,
                pattern.confidence,
                i64::from(pattern.active),
                pattern.superseded_by,
            ],
        )?;
        Ok(())
    }

    pub fn list_behavioral_patterns(
        &self,
        query: &BehavioralPatternQuery,
    ) -> Result<Vec<BehavioralPatternRecord>, DbError> {
        let mut sql = String::from(
            "SELECT id, detected_at_ms, pattern_type, description, metric_json,
                    scope_json, sample_size, confidence, active, superseded_by
             FROM behavioral_patterns WHERE 1=1",
        );
        let mut params_vec: Vec<rusqlite::types::Value> = Vec::new();
        if let Some(pattern_type) = query.pattern_type.as_ref() {
            sql.push_str(&format!(" AND pattern_type = ?{}", params_vec.len() + 1));
            params_vec.push(pattern_type.clone().into());
        }
        if let Some(active_only) = query.active_only {
            sql.push_str(&format!(" AND active = ?{}", params_vec.len() + 1));
            params_vec.push(i64::from(active_only).into());
        }
        if let Some(min_sample_size) = query.min_sample_size {
            sql.push_str(&format!(" AND sample_size >= ?{}", params_vec.len() + 1));
            params_vec.push(min_sample_size.into());
        }
        sql.push_str(" ORDER BY detected_at_ms DESC, sample_size DESC");
        if let Some(limit) = query.limit {
            sql.push_str(&format!(" LIMIT ?{}", params_vec.len() + 1));
            params_vec.push((limit as i64).into());
        }
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(
            rusqlite::params_from_iter(params_vec),
            Self::behavioral_pattern_from_row,
        )?;
        let mut items = rows.collect::<Result<Vec<_>, _>>()?;
        items.retain(|pattern| {
            let scope = &pattern.scope;
            let setup_ok = query
                .setup_id
                .as_ref()
                .map(|setup_id| {
                    scope.get("setupId").and_then(|value| value.as_str()) == Some(setup_id.as_str())
                })
                .unwrap_or(true);
            let session_type_ok = query
                .session_type
                .as_ref()
                .map(|session_type| {
                    scope
                        .get("sessionType")
                        .and_then(|value| value.as_str())
                        .map(|value| value.eq_ignore_ascii_case(session_type))
                        .unwrap_or(false)
                })
                .unwrap_or(true);
            let session_segment_ok = query
                .session_segment
                .as_ref()
                .map(|session_segment| {
                    scope
                        .get("sessionSegment")
                        .and_then(|value| value.as_str())
                        .map(|value| value.eq_ignore_ascii_case(session_segment))
                        .unwrap_or(false)
                })
                .unwrap_or(true);
            let time_bucket_ok = query
                .time_bucket
                .as_ref()
                .map(|time_bucket| {
                    scope.get("timeBucket").and_then(|value| value.as_str())
                        == Some(time_bucket.as_str())
                })
                .unwrap_or(true);
            let day_type_ok = query
                .day_type
                .as_ref()
                .map(|day_type| {
                    scope
                        .get("dayType")
                        .and_then(|value| value.as_str())
                        .map(|value| value.eq_ignore_ascii_case(day_type))
                        .unwrap_or(false)
                })
                .unwrap_or(true);
            setup_ok && session_type_ok && session_segment_ok && time_bucket_ok && day_type_ok
        });
        Ok(items)
    }

    pub fn upsert_memory_followup(&self, followup: &MemoryFollowupRecord) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO memory_followups (
                id, created_at_ms, resolved_at_ms, session_id, trade_id, source,
                title, detail, status, tags_json, due_context_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            ON CONFLICT(id) DO UPDATE SET
                resolved_at_ms = excluded.resolved_at_ms,
                session_id = excluded.session_id,
                trade_id = excluded.trade_id,
                source = excluded.source,
                title = excluded.title,
                detail = excluded.detail,
                status = excluded.status,
                tags_json = excluded.tags_json,
                due_context_json = excluded.due_context_json",
            params![
                followup.id,
                followup.created_at_ms,
                followup.resolved_at_ms,
                followup.session_id,
                followup.trade_id,
                followup.source,
                followup.title,
                followup.detail,
                followup.status,
                serde_json::to_string(&followup.tags)?,
                serde_json::to_string(&followup.due_context)?,
            ],
        )?;
        Ok(())
    }

    pub fn get_memory_followup(&self, id: &str) -> Result<Option<MemoryFollowupRecord>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, created_at_ms, resolved_at_ms, session_id, trade_id, source,
                    title, detail, status, tags_json, due_context_json
             FROM memory_followups WHERE id = ?1",
        )?;
        let mut rows = stmt.query([id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(Self::memory_followup_from_row(row)?))
        } else {
            Ok(None)
        }
    }

    pub fn list_memory_followups(
        &self,
        query: &MemoryFollowupQuery,
    ) -> Result<Vec<MemoryFollowupRecord>, DbError> {
        let mut sql = String::from(
            "SELECT id, created_at_ms, resolved_at_ms, session_id, trade_id, source,
                    title, detail, status, tags_json, due_context_json
             FROM memory_followups WHERE 1=1",
        );
        let mut params_vec: Vec<rusqlite::types::Value> = Vec::new();
        if let Some(status) = query.status.as_ref() {
            sql.push_str(&format!(" AND status = ?{}", params_vec.len() + 1));
            params_vec.push(status.clone().into());
        }
        if let Some(session_id) = query.session_id.as_ref() {
            sql.push_str(&format!(" AND session_id = ?{}", params_vec.len() + 1));
            params_vec.push(session_id.clone().into());
        }
        if let Some(trade_id) = query.trade_id.as_ref() {
            sql.push_str(&format!(" AND trade_id = ?{}", params_vec.len() + 1));
            params_vec.push(trade_id.clone().into());
        }
        sql.push_str(" ORDER BY created_at_ms DESC");
        if let Some(limit) = query.limit {
            sql.push_str(&format!(" LIMIT ?{}", params_vec.len() + 1));
            params_vec.push((limit as i64).into());
        }
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(
            rusqlite::params_from_iter(params_vec),
            Self::memory_followup_from_row,
        )?;
        let mut items = rows.collect::<Result<Vec<_>, _>>()?;
        items.retain(|followup| {
            query
                .setup_id
                .as_ref()
                .map(|setup_id| {
                    followup
                        .due_context
                        .get("setupId")
                        .and_then(|value| value.as_str())
                        == Some(setup_id.as_str())
                })
                .unwrap_or(true)
        });
        Ok(items)
    }

    pub fn get_memory_maintenance_state(&self) -> Result<MemoryMaintenanceState, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT patterns_last_refreshed_at_ms, insights_lifecycle_last_refreshed_at_ms,
                    patterns_dirty, insights_lifecycle_dirty, dirty_since_ms,
                    dirty_reasons_json, last_refresh_reason
             FROM memory_maintenance_state
             WHERE singleton = 1",
        )?;
        let mut rows = stmt.query([])?;
        if let Some(row) = rows.next()? {
            Ok(Self::memory_maintenance_state_from_row(row)?)
        } else {
            Ok(MemoryMaintenanceState::default())
        }
    }

    pub fn upsert_memory_maintenance_state(
        &self,
        state: &MemoryMaintenanceState,
    ) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO memory_maintenance_state (
                singleton, patterns_last_refreshed_at_ms, insights_lifecycle_last_refreshed_at_ms,
                patterns_dirty, insights_lifecycle_dirty, dirty_since_ms, dirty_reasons_json,
                last_refresh_reason
            ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(singleton) DO UPDATE SET
                patterns_last_refreshed_at_ms = excluded.patterns_last_refreshed_at_ms,
                insights_lifecycle_last_refreshed_at_ms = excluded.insights_lifecycle_last_refreshed_at_ms,
                patterns_dirty = excluded.patterns_dirty,
                insights_lifecycle_dirty = excluded.insights_lifecycle_dirty,
                dirty_since_ms = excluded.dirty_since_ms,
                dirty_reasons_json = excluded.dirty_reasons_json,
                last_refresh_reason = excluded.last_refresh_reason",
            params![
                state.patterns_last_refreshed_at_ms,
                state.insights_lifecycle_last_refreshed_at_ms,
                i64::from(state.patterns_dirty),
                i64::from(state.insights_lifecycle_dirty),
                state.dirty_since_ms,
                serde_json::to_string(&state.dirty_reasons)?,
                state.last_refresh_reason.clone(),
            ],
        )?;
        Ok(())
    }

    fn agent_insight_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<AgentInsightRecord> {
        Ok(AgentInsightRecord {
            id: row.get(0)?,
            created_at_ms: row.get(1)?,
            updated_at_ms: row.get(2)?,
            session_id: row.get(3)?,
            trade_id: row.get(4)?,
            setup_id: row.get(5)?,
            category: row.get(6)?,
            status: row.get(7)?,
            summary: row.get(8)?,
            evidence: row
                .get::<_, String>(9)
                .ok()
                .and_then(|value| serde_json::from_str(&value).ok())
                .unwrap_or_else(|| serde_json::json!({})),
            tags: row
                .get::<_, String>(10)
                .ok()
                .and_then(|value| serde_json::from_str(&value).ok())
                .unwrap_or_default(),
            scope: row
                .get::<_, String>(11)
                .ok()
                .and_then(|value| serde_json::from_str(&value).ok())
                .unwrap_or_else(|| serde_json::json!({})),
            confidence: row.get(12)?,
            salience: row.get(13)?,
            times_surfaced: row.get(14)?,
            last_surfaced_ms: row.get(15)?,
            superseded_by: row.get(16)?,
            source: row.get(17)?,
            helpful_count: row.get(18)?,
            irrelevant_count: row.get(19)?,
            wrong_count: row.get(20)?,
        })
    }

    fn behavioral_pattern_from_row(
        row: &rusqlite::Row<'_>,
    ) -> rusqlite::Result<BehavioralPatternRecord> {
        Ok(BehavioralPatternRecord {
            id: row.get(0)?,
            detected_at_ms: row.get(1)?,
            pattern_type: row.get(2)?,
            description: row.get(3)?,
            metric: row
                .get::<_, String>(4)
                .ok()
                .and_then(|value| serde_json::from_str(&value).ok())
                .unwrap_or_else(|| serde_json::json!({})),
            scope: row
                .get::<_, String>(5)
                .ok()
                .and_then(|value| serde_json::from_str(&value).ok())
                .unwrap_or_else(|| serde_json::json!({})),
            sample_size: row.get(6)?,
            confidence: row.get(7)?,
            active: row.get::<_, i64>(8)? == 1,
            superseded_by: row.get(9)?,
        })
    }

    fn memory_followup_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<MemoryFollowupRecord> {
        Ok(MemoryFollowupRecord {
            id: row.get(0)?,
            created_at_ms: row.get(1)?,
            resolved_at_ms: row.get(2)?,
            session_id: row.get(3)?,
            trade_id: row.get(4)?,
            source: row.get(5)?,
            title: row.get(6)?,
            detail: row.get(7)?,
            status: row.get(8)?,
            tags: row
                .get::<_, String>(9)
                .ok()
                .and_then(|value| serde_json::from_str(&value).ok())
                .unwrap_or_default(),
            due_context: row
                .get::<_, String>(10)
                .ok()
                .and_then(|value| serde_json::from_str(&value).ok())
                .unwrap_or_else(|| serde_json::json!({})),
        })
    }

    fn memory_maintenance_state_from_row(
        row: &rusqlite::Row<'_>,
    ) -> rusqlite::Result<MemoryMaintenanceState> {
        let patterns_last_refreshed_at_ms: Option<f64> = row.get(0)?;
        let insights_lifecycle_last_refreshed_at_ms: Option<f64> = row.get(1)?;
        let patterns_dirty = row.get::<_, i64>(2)? != 0;
        let insights_lifecycle_dirty = row.get::<_, i64>(3)? != 0;
        Ok(MemoryMaintenanceState {
            patterns_last_refreshed_at_ms,
            insights_lifecycle_last_refreshed_at_ms,
            patterns_dirty,
            insights_lifecycle_dirty,
            dirty_since_ms: row.get(4)?,
            dirty_reasons: row
                .get::<_, String>(5)
                .ok()
                .and_then(|value| serde_json::from_str(&value).ok())
                .unwrap_or_default(),
            last_refresh_reason: row.get(6)?,
            refresh_suggested: patterns_dirty
                || insights_lifecycle_dirty
                || patterns_last_refreshed_at_ms.is_none()
                || insights_lifecycle_last_refreshed_at_ms.is_none(),
        })
    }

    pub fn insert_trade_import_batch(&self, batch: &TradeImportBatchRecord) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO trade_import_batches (batch_id, source, imported_at, notes, fill_count)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(batch_id) DO UPDATE SET
               source = excluded.source,
               imported_at = excluded.imported_at,
               notes = excluded.notes,
               fill_count = excluded.fill_count",
            params![
                batch.batch_id,
                batch.source,
                batch.imported_at,
                batch.notes,
                batch.fill_count,
            ],
        )?;
        Ok(())
    }

    pub fn imported_fill_exists(&self, fingerprint: &str) -> Result<bool, DbError> {
        let exists: i64 = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM trade_fill_imports WHERE fingerprint = ?1)",
            [fingerprint],
            |row| row.get(0),
        )?;
        Ok(exists == 1)
    }

    pub fn insert_imported_fill(&self, fill: &ImportedFillRecord) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO trade_fill_imports (
                fingerprint, batch_id, trade_id, symbol, trade_account, fill_time, order_side,
                open_close, quantity, price, status, external_order_id, service_order_id, raw_payload
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            params![
                fill.fingerprint,
                fill.batch_id,
                fill.trade_id,
                fill.symbol,
                fill.trade_account,
                fill.fill_time,
                fill.order_side,
                fill.open_close,
                fill.quantity,
                fill.price,
                fill.status,
                fill.external_order_id,
                fill.service_order_id,
                fill.raw_payload.to_string(),
            ],
        )?;
        Ok(())
    }

    // ------------------------------------------------------------------
    // Risk config
    // ------------------------------------------------------------------

    pub fn save_risk_config(&self, config: &RiskConfigRecord) -> Result<(), DbError> {
        let zones = serde_json::to_string(&config.no_trade_zones)?;
        self.conn.execute(
            "INSERT INTO risk_config (singleton, r_value_points, r_value_dollars, max_daily_loss_r,
                max_consecutive_losses, max_trades_per_session, no_trade_zones, max_daily_loss_dollars)
             VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(singleton) DO UPDATE SET
               r_value_points=excluded.r_value_points,
               r_value_dollars=excluded.r_value_dollars,
               max_daily_loss_r=excluded.max_daily_loss_r,
               max_consecutive_losses=excluded.max_consecutive_losses,
               max_trades_per_session=excluded.max_trades_per_session,
               no_trade_zones=excluded.no_trade_zones,
               max_daily_loss_dollars=excluded.max_daily_loss_dollars",
            params![
                config.r_value_points,
                config.r_value_dollars,
                config.max_daily_loss_r,
                config.max_consecutive_losses,
                config.max_trades_per_session,
                zones,
                config.max_daily_loss_dollars,
            ],
        )?;
        Ok(())
    }

    pub fn load_risk_config(&self) -> Result<RiskConfigRecord, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT r_value_points, r_value_dollars, max_daily_loss_r,
                    max_consecutive_losses, max_trades_per_session, no_trade_zones,
                    max_daily_loss_dollars
             FROM risk_config WHERE singleton = 1",
        )?;
        let mut rows = stmt.query([])?;
        if let Some(row) = rows.next()? {
            let zones_str: String = row.get::<_, String>(5)?;
            let zones: Vec<serde_json::Value> =
                serde_json::from_str(&zones_str).unwrap_or_default();
            let max_daily_loss_dollars: Option<f64> = row.get(6).ok();
            Ok(RiskConfigRecord {
                r_value_points: row.get(0)?,
                r_value_dollars: row.get(1)?,
                max_daily_loss_r: row.get(2)?,
                max_consecutive_losses: row.get(3)?,
                max_trades_per_session: row.get(4)?,
                no_trade_zones: zones,
                max_daily_loss_dollars,
            })
        } else {
            Ok(RiskConfigRecord::default())
        }
    }

    // ------------------------------------------------------------------
    // Account state (risk coach: balance, positions, Lucid params)
    // ------------------------------------------------------------------

    pub fn save_account_state(&self, state: &AccountStateRecord) -> Result<(), DbError> {
        let positions_json = serde_json::to_string(&state.open_positions)?;
        self.conn.execute(
            "INSERT INTO account_state (singleton, last_balance_dollars, last_balance_updated_at_ms,
                open_positions_json, lucid_daily_loss_dollars, lucid_account_size_dollars,
                profit_target_per_cycle, position_sizing_method, kelly_fraction)
             VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(singleton) DO UPDATE SET
               last_balance_dollars=excluded.last_balance_dollars,
               last_balance_updated_at_ms=excluded.last_balance_updated_at_ms,
               open_positions_json=excluded.open_positions_json,
               lucid_daily_loss_dollars=excluded.lucid_daily_loss_dollars,
               lucid_account_size_dollars=excluded.lucid_account_size_dollars,
               profit_target_per_cycle=excluded.profit_target_per_cycle,
               position_sizing_method=excluded.position_sizing_method,
               kelly_fraction=excluded.kelly_fraction",
            params![
                state.last_balance_dollars,
                state.last_balance_updated_at_ms,
                positions_json,
                state.lucid_daily_loss_dollars,
                state.lucid_account_size_dollars,
                state.profit_target_per_cycle,
                state.position_sizing_method,
                state.kelly_fraction,
            ],
        )?;
        Ok(())
    }

    pub fn load_account_state(&self) -> Result<Option<AccountStateRecord>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT last_balance_dollars, last_balance_updated_at_ms, open_positions_json,
                    lucid_daily_loss_dollars, lucid_account_size_dollars, profit_target_per_cycle,
                    position_sizing_method, kelly_fraction
             FROM account_state WHERE singleton = 1",
        )?;
        let mut rows = stmt.query([])?;
        if let Some(row) = rows.next()? {
            let pos_str: String = row.get(2)?;
            let open_positions: Vec<OpenPositionRecord> =
                serde_json::from_str(&pos_str).unwrap_or_default();
            Ok(Some(AccountStateRecord {
                last_balance_dollars: row.get(0)?,
                last_balance_updated_at_ms: row.get(1)?,
                open_positions,
                lucid_daily_loss_dollars: row.get(3)?,
                lucid_account_size_dollars: row.get(4)?,
                profit_target_per_cycle: row.get(5)?,
                position_sizing_method: row.get(6).unwrap_or_else(|_| "quarter_kelly".to_string()),
                kelly_fraction: row.get(7).unwrap_or(0.25),
            }))
        } else {
            Ok(None)
        }
    }

    // ------------------------------------------------------------------
    // Session events (extended)
    // ------------------------------------------------------------------

    pub fn add_event(&self, event: &SessionEventInput) -> Result<i64, DbError> {
        let payload = serde_json::to_string(&event.data)?;
        let ts = chrono::Utc::now().timestamp_millis() as f64;
        self.conn.execute(
            "INSERT INTO session_events (event_type, setup_id, data, session_id, timestamp)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                event.event_type,
                event.setup_id,
                payload,
                event.session_id,
                ts
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn list_session_events(&self, limit: usize) -> Result<Vec<SessionEventRecord>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, event_type, setup_id, data, session_id, timestamp
             FROM session_events
             ORDER BY id DESC
             LIMIT ?1",
        )?;
        let rows = stmt.query_map([limit as i64], |row| {
            let data_text: String = row.get(3)?;
            let data = serde_json::from_str::<serde_json::Value>(&data_text)
                .unwrap_or_else(|_| serde_json::json!({}));
            Ok(SessionEventRecord {
                id: row.get(0)?,
                event_type: row.get(1)?,
                setup_id: row.get(2)?,
                data,
                session_id: row.get(4)?,
                timestamp: row.get(5)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn list_events_for_session(
        &self,
        session_id: &str,
    ) -> Result<Vec<SessionEventRecord>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, event_type, setup_id, data, session_id, timestamp
             FROM session_events
             WHERE session_id = ?1
             ORDER BY id ASC",
        )?;
        let rows = stmt.query_map([session_id], |row| {
            let data_text: String = row.get(3)?;
            let data = serde_json::from_str::<serde_json::Value>(&data_text)
                .unwrap_or_else(|_| serde_json::json!({}));
            Ok(SessionEventRecord {
                id: row.get(0)?,
                event_type: row.get(1)?,
                setup_id: row.get(2)?,
                data,
                session_id: row.get(4)?,
                timestamp: row.get(5)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    // ------------------------------------------------------------------
    // Risk state persistence (extended with consecutive_wins)
    // ------------------------------------------------------------------

    pub fn save_risk_state(&self, state: &RiskState) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO risk_state (singleton, daily_pnl_r, trade_count, consecutive_losses, consecutive_wins, drawdown_r, max_daily_loss_r, at_limit)
            VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(singleton) DO UPDATE SET
              daily_pnl_r=excluded.daily_pnl_r,
              trade_count=excluded.trade_count,
              consecutive_losses=excluded.consecutive_losses,
              consecutive_wins=excluded.consecutive_wins,
              drawdown_r=excluded.drawdown_r,
              max_daily_loss_r=excluded.max_daily_loss_r,
              at_limit=excluded.at_limit",
            params![
                state.daily_pnl_r,
                state.trade_count as i64,
                state.consecutive_losses as i64,
                state.consecutive_wins as i64,
                state.drawdown_r,
                state.max_daily_loss_r,
                i64::from(state.at_limit),
            ],
        )?;
        Ok(())
    }

    pub fn load_risk_state(&self) -> Result<Option<RiskState>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT daily_pnl_r, trade_count, consecutive_losses, consecutive_wins, drawdown_r, max_daily_loss_r, at_limit
             FROM risk_state WHERE singleton = 1",
        )?;
        let mut rows = stmt.query([])?;
        if let Some(row) = rows.next()? {
            Ok(Some(RiskState {
                daily_pnl_r: row.get(0)?,
                trade_count: row.get::<_, i64>(1)? as usize,
                consecutive_losses: row.get::<_, i64>(2)? as usize,
                consecutive_wins: row.get::<_, i64>(3).unwrap_or(0) as usize,
                drawdown_r: row.get(4)?,
                max_daily_loss_r: row.get(5)?,
                at_limit: row.get::<_, i64>(6)? == 1,
            }))
        } else {
            Ok(None)
        }
    }

    // ------------------------------------------------------------------
    // Prior day levels (extended with VA/POC)
    // ------------------------------------------------------------------

    pub fn save_prior_day(
        &self,
        date: &str,
        high: f64,
        low: f64,
        close: f64,
    ) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO prior_day_levels (date, high, low, close)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(date) DO UPDATE SET high=excluded.high, low=excluded.low, close=excluded.close",
            params![date, high, low, close],
        )?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn save_prior_day_full(
        &self,
        date: &str,
        high: f64,
        low: f64,
        close: f64,
        va_high: f64,
        va_low: f64,
        poc: f64,
    ) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO prior_day_levels (date, high, low, close, va_high, va_low, poc)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(date) DO UPDATE SET
               high=excluded.high, low=excluded.low, close=excluded.close,
               va_high=excluded.va_high, va_low=excluded.va_low, poc=excluded.poc",
            params![date, high, low, close, va_high, va_low, poc],
        )?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn save_prior_day_full_with_dnva(
        &self,
        date: &str,
        high: f64,
        low: f64,
        close: f64,
        va_high: f64,
        va_low: f64,
        poc: f64,
        dnva_high: Option<f64>,
        dnva_low: Option<f64>,
        dnp: Option<f64>,
    ) -> Result<(), DbError> {
        self.save_prior_day_full_with_dnva_contract(
            date, high, low, close, va_high, va_low, poc, dnva_high, dnva_low, dnp, None, None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn save_prior_day_full_with_dnva_contract(
        &self,
        date: &str,
        high: f64,
        low: f64,
        close: f64,
        va_high: f64,
        va_low: f64,
        poc: f64,
        dnva_high: Option<f64>,
        dnva_low: Option<f64>,
        dnp: Option<f64>,
        root_symbol: Option<&str>,
        contract_symbol: Option<&str>,
    ) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO prior_day_levels
             (date, high, low, close, va_high, va_low, poc, dnva_high, dnva_low, dnp, root_symbol, contract_symbol)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
             ON CONFLICT(date) DO UPDATE SET
               high=excluded.high, low=excluded.low, close=excluded.close,
               va_high=excluded.va_high, va_low=excluded.va_low, poc=excluded.poc,
               dnva_high=excluded.dnva_high, dnva_low=excluded.dnva_low, dnp=excluded.dnp,
               root_symbol=excluded.root_symbol, contract_symbol=excluded.contract_symbol",
            params![
                date, high, low, close, va_high, va_low, poc,
                dnva_high, dnva_low, dnp,
                root_symbol, contract_symbol,
            ],
        )?;
        Ok(())
    }

    /// Load prior session DNVA (dnva_high, dnva_low, dnp) for a session type.
    /// Returns the most recent completed session of that type before the given date.
    pub fn load_prior_session_dnva(
        &self,
        session_type: &str,
        before_date: &str,
    ) -> Result<Option<(f64, f64, f64)>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT dnva_high, dnva_low, dnp FROM session_summaries
             WHERE session_type = ?1 AND session_date < ?2
             AND dnva_high IS NOT NULL AND dnva_low IS NOT NULL AND dnp IS NOT NULL
             ORDER BY session_date DESC LIMIT 1",
        )?;
        let mut rows = stmt.query(params![session_type, before_date])?;
        if let Some(row) = rows.next()? {
            Ok(Some((row.get(0)?, row.get(1)?, row.get(2)?)))
        } else {
            Ok(None)
        }
    }

    /// Load session DNVA for a specific session_date and session_type.
    pub fn load_session_dnva(
        &self,
        session_date: &str,
        session_type: &str,
    ) -> Result<Option<(f64, f64, f64)>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT dnva_high, dnva_low, dnp FROM session_summaries
             WHERE session_date = ?1 AND session_type = ?2
             AND dnva_high IS NOT NULL AND dnva_low IS NOT NULL AND dnp IS NOT NULL",
        )?;
        let mut rows = stmt.query(params![session_date, session_type])?;
        if let Some(row) = rows.next()? {
            Ok(Some((row.get(0)?, row.get(1)?, row.get(2)?)))
        } else {
            Ok(None)
        }
    }

    pub fn load_prior_day(&self, before_date: &str) -> Result<Option<(f64, f64, f64)>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT high, low, close FROM prior_day_levels WHERE date < ?1 ORDER BY date DESC LIMIT 1",
        )?;
        let mut rows = stmt.query(params![before_date])?;
        if let Some(row) = rows.next()? {
            Ok(Some((row.get(0)?, row.get(1)?, row.get(2)?)))
        } else {
            Ok(None)
        }
    }

    /// Load prior-day levels including VA/POC and DNVA if available.
    #[allow(clippy::type_complexity)]
    pub fn load_prior_day_full(
        &self,
        before_date: &str,
    ) -> Result<
        Option<(
            f64,
            f64,
            f64,
            Option<f64>,
            Option<f64>,
            Option<f64>,
            Option<f64>,
            Option<f64>,
            Option<f64>,
        )>,
        DbError,
    > {
        let mut stmt = self.conn.prepare(
            "SELECT high, low, close, va_high, va_low, poc, dnva_high, dnva_low, dnp
             FROM prior_day_levels WHERE date < ?1 ORDER BY date DESC LIMIT 1",
        )?;
        let mut rows = stmt.query(params![before_date])?;
        if let Some(row) = rows.next()? {
            Ok(Some((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
                row.get(8)?,
            )))
        } else {
            Ok(None)
        }
    }

    pub fn load_prior_day_reference_scoped(
        &self,
        before_date: &str,
        root_symbol: Option<&str>,
        contract_symbol: Option<&str>,
    ) -> Result<Option<PriorDayReference>, DbError> {
        let mut conditions = vec!["date < ?1".to_string()];
        let mut bind_values: Vec<Box<dyn rusqlite::types::ToSql>> =
            vec![Box::new(before_date.to_string())];
        if let Some(contract_symbol) = contract_symbol {
            conditions.push(format!("contract_symbol = ?{}", bind_values.len() + 1));
            bind_values.push(Box::new(contract_symbol.to_string()));
        } else if let Some(root_symbol) = root_symbol {
            conditions.push(format!("root_symbol = ?{}", bind_values.len() + 1));
            bind_values.push(Box::new(root_symbol.to_string()));
        }
        let sql = format!(
            "SELECT high, low, close, va_high, va_low, poc, dnva_high, dnva_low, dnp, root_symbol, contract_symbol
             FROM prior_day_levels WHERE {} ORDER BY date DESC LIMIT 1",
            conditions.join(" AND ")
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let params_ref: Vec<&dyn rusqlite::types::ToSql> =
            bind_values.iter().map(|value| value.as_ref()).collect();
        let mut rows = stmt.query(params_ref.as_slice())?;
        if let Some(row) = rows.next()? {
            Ok(Some(PriorDayReference {
                high: row.get(0)?,
                low: row.get(1)?,
                close: row.get(2)?,
                va_high: row.get(3)?,
                va_low: row.get(4)?,
                poc: row.get(5)?,
                dnva_high: row.get(6)?,
                dnva_low: row.get(7)?,
                dnp: row.get(8)?,
                root_symbol: row.get(9)?,
                contract_symbol: row.get(10)?,
            }))
        } else {
            Ok(None)
        }
    }

    // ------------------------------------------------------------------
    // Backend intelligence storage
    // ------------------------------------------------------------------

    #[allow(clippy::too_many_arguments)]
    pub fn insert_raw_tick(
        &self,
        timestamp_ms: f64,
        price: f64,
        volume: f64,
        bid: f64,
        ask: f64,
        is_buy: bool,
        session_date: &str,
    ) -> Result<(), DbError> {
        self.insert_raw_tick_with_contract(
            timestamp_ms,
            price,
            volume,
            bid,
            ask,
            is_buy,
            session_date,
            None,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn insert_raw_tick_with_contract(
        &self,
        timestamp_ms: f64,
        price: f64,
        volume: f64,
        bid: f64,
        ask: f64,
        is_buy: bool,
        session_date: &str,
        root_symbol: Option<&str>,
        contract_symbol: Option<&str>,
    ) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT OR IGNORE INTO raw_ticks
             (timestamp_ms, price, volume, bid, ask, is_buy, session_date, root_symbol, contract_symbol)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                timestamp_ms,
                price,
                volume,
                bid,
                ask,
                i64::from(is_buy),
                session_date,
                root_symbol,
                contract_symbol,
            ],
        )?;
        Ok(())
    }

    pub fn upsert_feature_state(
        &self,
        timestamp_ms: f64,
        payload: &serde_json::Value,
    ) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO feature_state (singleton, timestamp_ms, payload)
             VALUES (1, ?1, ?2)
             ON CONFLICT(singleton) DO UPDATE SET
               timestamp_ms=excluded.timestamp_ms,
               payload=excluded.payload",
            params![timestamp_ms, serde_json::to_string(payload)?],
        )?;
        Ok(())
    }

    pub fn insert_pipeline_snapshot(
        &self,
        timestamp_ms: f64,
        payload: &serde_json::Value,
    ) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO pipeline_snapshots (timestamp_ms, payload) VALUES (?1, ?2)",
            params![timestamp_ms, serde_json::to_string(payload)?],
        )?;
        Ok(())
    }

    pub fn insert_playbook_signal(
        &self,
        timestamp_ms: f64,
        setup_id: &str,
        payload: &serde_json::Value,
    ) -> Result<(), DbError> {
        use crate::session_date_from_timestamp_ms;
        let signal_id = format!("{setup_id}_{}", timestamp_ms as u64);
        self.insert_playbook_signal_record(&ReplaySignalRecord {
            signal_id,
            timestamp_ms,
            session_date: session_date_from_timestamp_ms(timestamp_ms),
            root_symbol: None,
            contract_symbol: None,
            setup_id: setup_id.to_string(),
            payload: payload.clone(),
            source: "live".to_string(),
            job_id: None,
        })
    }

    pub fn insert_playbook_signal_record(
        &self,
        signal: &ReplaySignalRecord,
    ) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT OR IGNORE INTO playbook_signals
             (signal_id, timestamp_ms, session_date, root_symbol, contract_symbol, setup_id, payload, source, job_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                signal.signal_id,
                signal.timestamp_ms,
                signal.session_date,
                signal.root_symbol,
                signal.contract_symbol,
                signal.setup_id,
                serde_json::to_string(&signal.payload)?,
                signal.source,
                signal.job_id
            ],
        )?;
        Ok(())
    }

    pub fn insert_microstructure_snapshot(
        &self,
        timestamp_ms: f64,
        payload: &serde_json::Value,
    ) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO microstructure_snapshots (timestamp_ms, payload) VALUES (?1, ?2)",
            params![timestamp_ms, serde_json::to_string(payload)?],
        )?;
        Ok(())
    }

    pub fn insert_depth_events_batch(
        &mut self,
        source_file: &str,
        records: &[DepthRecord],
        starting_batch_id: i64,
    ) -> Result<i64, DbError> {
        let tx = self.conn.transaction()?;
        let mut stmt = tx.prepare(
            "INSERT INTO depth_events
             (source_file, timestamp_ms, side, command, price, quantity, num_orders, end_of_batch, batch_id, trading_day)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        )?;
        let mut batch_id = starting_batch_id;
        for record in records {
            let side = record.side.map(|side| format!("{side:?}").to_lowercase());
            let trading_day = trading_day_from_timestamp_ms(record.timestamp_ms);
            stmt.execute(params![
                source_file,
                record.timestamp_ms,
                side,
                format!("{:?}", record.command),
                record.price,
                record.quantity as f64,
                record.num_orders as i64,
                i64::from(record.end_of_batch),
                batch_id,
                trading_day,
            ])?;
            if record.end_of_batch {
                batch_id += 1;
            }
        }
        drop(stmt);
        tx.commit()?;
        Ok(batch_id)
    }

    pub fn insert_dom_snapshot(
        &self,
        source_file: &str,
        timestamp_ms: f64,
        trading_day: &str,
        payload: &serde_json::Value,
    ) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO dom_snapshots (source_file, timestamp_ms, trading_day, payload)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                source_file,
                timestamp_ms,
                trading_day,
                serde_json::to_string(payload)?
            ],
        )?;
        Ok(())
    }

    pub fn insert_dom_feature_snapshot(
        &self,
        source_file: &str,
        timestamp_ms: f64,
        trading_day: &str,
        payload: &serde_json::Value,
    ) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO dom_feature_snapshots (source_file, timestamp_ms, trading_day, payload)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                source_file,
                timestamp_ms,
                trading_day,
                serde_json::to_string(payload)?
            ],
        )?;
        Ok(())
    }

    pub fn list_recent_ticks(&self, limit: usize) -> Result<Vec<RawTickRecord>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, timestamp_ms, price, volume, bid, ask, is_buy, session_date, root_symbol, contract_symbol
             FROM raw_ticks ORDER BY timestamp_ms DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map([limit as i64], |row| {
            Ok(RawTickRecord {
                id: row.get(0)?,
                timestamp_ms: row.get(1)?,
                price: row.get(2)?,
                volume: row.get(3)?,
                bid: row.get(4)?,
                ask: row.get(5)?,
                is_buy: row.get::<_, i64>(6)? == 1,
                session_date: row.get(7)?,
                root_symbol: row.get(8)?,
                contract_symbol: row.get(9)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Query ticks with optional filters for time range, price range, and session date.
    ///
    /// When a time range is provided, results are returned in ascending chronological order
    /// (oldest first) so the caller can read the auction from left to right. Without time
    /// filters the results are ordered most-recent first (same as `list_recent_ticks`).
    pub fn query_ticks_filtered(
        &self,
        start_ms: Option<f64>,
        end_ms: Option<f64>,
        price_low: Option<f64>,
        price_high: Option<f64>,
        session_date: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RawTickRecord>, DbError> {
        self.query_ticks_filtered_scoped(
            start_ms,
            end_ms,
            price_low,
            price_high,
            session_date,
            None,
            None,
            limit,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn query_ticks_filtered_scoped(
        &self,
        start_ms: Option<f64>,
        end_ms: Option<f64>,
        price_low: Option<f64>,
        price_high: Option<f64>,
        session_date: Option<&str>,
        root_symbol: Option<&str>,
        contract_symbol: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RawTickRecord>, DbError> {
        use rusqlite::types::Value;

        let mut conditions = Vec::<String>::new();
        let mut p: Vec<Value> = Vec::new();

        if let Some(v) = start_ms {
            p.push(Value::Real(v));
            conditions.push(format!("timestamp_ms >= ?{}", p.len()));
        }
        if let Some(v) = end_ms {
            p.push(Value::Real(v));
            conditions.push(format!("timestamp_ms <= ?{}", p.len()));
        }
        if let Some(v) = price_low {
            p.push(Value::Real(v));
            conditions.push(format!("price >= ?{}", p.len()));
        }
        if let Some(v) = price_high {
            p.push(Value::Real(v));
            conditions.push(format!("price <= ?{}", p.len()));
        }
        if let Some(v) = session_date {
            p.push(Value::Text(v.to_string()));
            conditions.push(format!("session_date = ?{}", p.len()));
        }
        if let Some(v) = root_symbol {
            p.push(Value::Text(v.to_string()));
            conditions.push(format!("root_symbol = ?{}", p.len()));
        }
        if let Some(v) = contract_symbol {
            p.push(Value::Text(v.to_string()));
            conditions.push(format!("contract_symbol = ?{}", p.len()));
        }
        p.push(Value::Integer(limit as i64));
        let limit_idx = p.len();

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        // Use ASC when a time window is given (chronological read), DESC otherwise.
        let order = if start_ms.is_some() || end_ms.is_some() {
            "ASC"
        } else {
            "DESC"
        };

        let sql = format!(
            "SELECT id, timestamp_ms, price, volume, bid, ask, is_buy, session_date, root_symbol, contract_symbol \
             FROM raw_ticks {where_clause} ORDER BY timestamp_ms {order} LIMIT ?{limit_idx}"
        );

        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(p), |row| {
            Ok(RawTickRecord {
                id: row.get(0)?,
                timestamp_ms: row.get(1)?,
                price: row.get(2)?,
                volume: row.get(3)?,
                bid: row.get(4)?,
                ask: row.get(5)?,
                is_buy: row.get::<_, i64>(6)? == 1,
                session_date: row.get(7)?,
                root_symbol: row.get(8)?,
                contract_symbol: row.get(9)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn list_ticks_in_range(
        &self,
        start_ms: f64,
        end_ms: f64,
    ) -> Result<Vec<RawTickRecord>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, timestamp_ms, price, volume, bid, ask, is_buy, session_date, root_symbol, contract_symbol
             FROM raw_ticks
             WHERE timestamp_ms >= ?1 AND timestamp_ms < ?2
             ORDER BY timestamp_ms ASC",
        )?;
        let rows = stmt.query_map(params![start_ms, end_ms], |row| {
            Ok(RawTickRecord {
                id: row.get(0)?,
                timestamp_ms: row.get(1)?,
                price: row.get(2)?,
                volume: row.get(3)?,
                bid: row.get(4)?,
                ask: row.get(5)?,
                is_buy: row.get::<_, i64>(6)? == 1,
                session_date: row.get(7)?,
                root_symbol: row.get(8)?,
                contract_symbol: row.get(9)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Return the pipeline snapshot stored closest in time to `timestamp_ms`.
    /// Also returns the actual snapshot timestamp so callers can see how close the match was.
    pub fn get_snapshot_near(
        &self,
        timestamp_ms: f64,
    ) -> Result<Option<(f64, serde_json::Value)>, DbError> {
        // Use two index-friendly lookups (before and after) and pick the nearer row.
        let before = {
            let mut stmt = self.conn.prepare(
                "SELECT timestamp_ms, payload FROM pipeline_snapshots \
                 WHERE timestamp_ms <= ?1 ORDER BY timestamp_ms DESC LIMIT 1",
            )?;
            let mut rows = stmt.query(params![timestamp_ms])?;
            if let Some(row) = rows.next()? {
                let ts: f64 = row.get(0)?;
                let payload: String = row.get(1)?;
                serde_json::from_str::<serde_json::Value>(&payload)
                    .ok()
                    .map(|v| (ts, v))
            } else {
                None
            }
        };
        let after = {
            let mut stmt = self.conn.prepare(
                "SELECT timestamp_ms, payload FROM pipeline_snapshots \
                 WHERE timestamp_ms >= ?1 ORDER BY timestamp_ms ASC LIMIT 1",
            )?;
            let mut rows = stmt.query(params![timestamp_ms])?;
            if let Some(row) = rows.next()? {
                let ts: f64 = row.get(0)?;
                let payload: String = row.get(1)?;
                serde_json::from_str::<serde_json::Value>(&payload)
                    .ok()
                    .map(|v| (ts, v))
            } else {
                None
            }
        };
        Ok(match (before, after) {
            (Some(b), Some(a)) => {
                if (b.0 - timestamp_ms).abs() <= (a.0 - timestamp_ms).abs() {
                    Some(b)
                } else {
                    Some(a)
                }
            }
            (Some(b), None) => Some(b),
            (None, Some(a)) => Some(a),
            (None, None) => None,
        })
    }

    pub fn raw_tick_count(&self) -> Result<i64, DbError> {
        Ok(self
            .conn
            .query_row("SELECT COUNT(1) FROM raw_ticks", [], |r| r.get(0))?)
    }

    pub fn latest_feature_state(&self) -> Result<Option<serde_json::Value>, DbError> {
        let mut stmt = self
            .conn
            .prepare("SELECT payload FROM feature_state WHERE singleton = 1")?;
        let mut rows = stmt.query([])?;
        if let Some(row) = rows.next()? {
            let payload: String = row.get(0)?;
            Ok(serde_json::from_str(&payload).ok())
        } else {
            Ok(None)
        }
    }

    /// Latest persisted pipeline feature snapshot and its `timestamp_ms` (market time when written).
    pub fn latest_feature_state_with_timestamp(
        &self,
    ) -> Result<Option<(f64, serde_json::Value)>, DbError> {
        let mut stmt = self
            .conn
            .prepare("SELECT timestamp_ms, payload FROM feature_state WHERE singleton = 1")?;
        let mut rows = stmt.query([])?;
        if let Some(row) = rows.next()? {
            let timestamp_ms: f64 = row.get(0)?;
            let payload: String = row.get(1)?;
            Ok(Some((timestamp_ms, serde_json::from_str(&payload)?)))
        } else {
            Ok(None)
        }
    }

    pub fn latest_dom_feature_state(&self) -> Result<Option<(f64, serde_json::Value)>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT timestamp_ms, payload
             FROM dom_feature_snapshots
             ORDER BY timestamp_ms DESC
             LIMIT 1",
        )?;
        let mut rows = stmt.query([])?;
        if let Some(row) = rows.next()? {
            let timestamp_ms: f64 = row.get(0)?;
            let payload: String = row.get(1)?;
            Ok(Some((timestamp_ms, serde_json::from_str(&payload)?)))
        } else {
            Ok(None)
        }
    }

    pub fn get_dom_snapshot_near(
        &self,
        timestamp_ms: f64,
    ) -> Result<Option<(f64, serde_json::Value)>, DbError> {
        self.get_json_snapshot_near("dom_snapshots", timestamp_ms)
    }

    pub fn get_dom_feature_near(
        &self,
        timestamp_ms: f64,
    ) -> Result<Option<(f64, serde_json::Value)>, DbError> {
        self.get_json_snapshot_near("dom_feature_snapshots", timestamp_ms)
    }

    pub fn query_dom_feature_snapshots(
        &self,
        start_ms: Option<f64>,
        end_ms: Option<f64>,
        limit: usize,
    ) -> Result<Vec<(f64, serde_json::Value)>, DbError> {
        use rusqlite::types::Value;

        let mut conditions = Vec::<String>::new();
        let mut params_vec = Vec::<Value>::new();
        if let Some(start_ms) = start_ms {
            params_vec.push(Value::Real(start_ms));
            conditions.push(format!("timestamp_ms >= ?{}", params_vec.len()));
        }
        if let Some(end_ms) = end_ms {
            params_vec.push(Value::Real(end_ms));
            conditions.push(format!("timestamp_ms <= ?{}", params_vec.len()));
        }
        params_vec.push(Value::Integer(limit as i64));
        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };
        let sql = format!(
            "SELECT timestamp_ms, payload
             FROM dom_feature_snapshots
             {where_clause}
             ORDER BY timestamp_ms ASC
             LIMIT ?{}",
            params_vec.len()
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(params_vec), |row| {
            Ok((row.get::<_, f64>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (timestamp_ms, payload) = row?;
            out.push((timestamp_ms, serde_json::from_str(&payload)?));
        }
        Ok(out)
    }

    pub fn query_dom_feature_snapshots_for_trading_day(
        &self,
        trading_day: &str,
        limit: usize,
    ) -> Result<Vec<(f64, serde_json::Value)>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT timestamp_ms, payload
             FROM dom_feature_snapshots
             WHERE trading_day = ?1
             ORDER BY timestamp_ms ASC
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![trading_day, limit as i64], |row| {
            Ok((row.get::<_, f64>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (timestamp_ms, payload) = row?;
            out.push((timestamp_ms, serde_json::from_str(&payload)?));
        }
        Ok(out)
    }

    pub fn list_dom_feature_snapshots_for_research(
        &self,
        start_date: Option<&str>,
        end_date: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, f64, serde_json::Value)>, DbError> {
        use rusqlite::types::Value;

        let mut conditions = Vec::<String>::new();
        let mut params_vec = Vec::<Value>::new();
        if let Some(start_date) = start_date {
            params_vec.push(Value::Text(start_date.to_string()));
            conditions.push(format!("trading_day >= ?{}", params_vec.len()));
        }
        if let Some(end_date) = end_date {
            params_vec.push(Value::Text(end_date.to_string()));
            conditions.push(format!("trading_day <= ?{}", params_vec.len()));
        }
        params_vec.push(Value::Integer(limit as i64));
        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };
        let sql = format!(
            "SELECT trading_day, timestamp_ms, payload
             FROM dom_feature_snapshots
             {where_clause}
             ORDER BY timestamp_ms ASC
             LIMIT ?{}",
            params_vec.len()
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(params_vec), |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, f64>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (trading_day, timestamp_ms, payload) = row?;
            out.push((trading_day, timestamp_ms, serde_json::from_str(&payload)?));
        }
        Ok(out)
    }

    pub fn query_depth_events(
        &self,
        start_ms: Option<f64>,
        end_ms: Option<f64>,
        price_low: Option<f64>,
        price_high: Option<f64>,
        limit: usize,
    ) -> Result<Vec<DepthEventRecord>, DbError> {
        use rusqlite::types::Value;

        let mut conditions = Vec::<String>::new();
        let mut params_vec = Vec::<Value>::new();
        if let Some(start_ms) = start_ms {
            params_vec.push(Value::Real(start_ms));
            conditions.push(format!("timestamp_ms >= ?{}", params_vec.len()));
        }
        if let Some(end_ms) = end_ms {
            params_vec.push(Value::Real(end_ms));
            conditions.push(format!("timestamp_ms <= ?{}", params_vec.len()));
        }
        if let Some(price_low) = price_low {
            params_vec.push(Value::Real(price_low));
            conditions.push(format!("price >= ?{}", params_vec.len()));
        }
        if let Some(price_high) = price_high {
            params_vec.push(Value::Real(price_high));
            conditions.push(format!("price <= ?{}", params_vec.len()));
        }
        params_vec.push(Value::Integer(limit as i64));
        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };
        let sql = format!(
            "SELECT id, source_file, timestamp_ms, side, command, price, quantity, num_orders, end_of_batch, batch_id, trading_day
             FROM depth_events
             {where_clause}
             ORDER BY timestamp_ms ASC
             LIMIT ?{}",
            params_vec.len()
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(params_vec), |row| {
            Ok(DepthEventRecord {
                id: row.get(0)?,
                source_file: row.get(1)?,
                timestamp_ms: row.get(2)?,
                side: row.get(3)?,
                command: row.get(4)?,
                price: row.get(5)?,
                quantity: row.get(6)?,
                num_orders: row.get(7)?,
                end_of_batch: row.get::<_, i64>(8)? == 1,
                batch_id: row.get(9)?,
                trading_day: row.get(10)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn list_depth_events_in_range(
        &self,
        start_ms: f64,
        end_ms: f64,
        source_file: Option<&str>,
    ) -> Result<Vec<DepthEventRecord>, DbError> {
        fn map_depth_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<DepthEventRecord> {
            Ok(DepthEventRecord {
                id: row.get(0)?,
                source_file: row.get(1)?,
                timestamp_ms: row.get(2)?,
                side: row.get(3)?,
                command: row.get(4)?,
                price: row.get(5)?,
                quantity: row.get(6)?,
                num_orders: row.get(7)?,
                end_of_batch: row.get::<_, i64>(8)? == 1,
                batch_id: row.get(9)?,
                trading_day: row.get(10)?,
            })
        }

        let mut sql = String::from(
            "SELECT id, source_file, timestamp_ms, side, command, price, quantity, num_orders, end_of_batch, batch_id, trading_day
             FROM depth_events
             WHERE timestamp_ms >= ?1 AND timestamp_ms < ?2",
        );
        if source_file.is_some() {
            sql.push_str(" AND source_file = ?3");
        }
        sql.push_str(" ORDER BY timestamp_ms ASC, id ASC");

        let mut stmt = self.conn.prepare(&sql)?;
        let out = if let Some(source_file) = source_file {
            let rows = stmt.query_map(params![start_ms, end_ms, source_file], map_depth_row)?;
            rows.collect::<Result<Vec<_>, _>>()?
        } else {
            let rows = stmt.query_map(params![start_ms, end_ms], map_depth_row)?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        Ok(out)
    }

    pub fn latest_depth_clear_before(
        &self,
        source_file: &str,
        timestamp_ms: f64,
    ) -> Result<Option<f64>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT timestamp_ms
             FROM depth_events
             WHERE source_file = ?1 AND command = 'ClearBook' AND timestamp_ms <= ?2
             ORDER BY timestamp_ms DESC, id DESC
             LIMIT 1",
        )?;
        let mut rows = stmt.query(params![source_file, timestamp_ms])?;
        if let Some(row) = rows.next()? {
            Ok(Some(row.get(0)?))
        } else {
            Ok(None)
        }
    }

    fn get_json_snapshot_near(
        &self,
        table: &str,
        timestamp_ms: f64,
    ) -> Result<Option<(f64, serde_json::Value)>, DbError> {
        let before_sql = format!(
            "SELECT timestamp_ms, payload FROM {table}
             WHERE timestamp_ms <= ?1 ORDER BY timestamp_ms DESC LIMIT 1"
        );
        let after_sql = format!(
            "SELECT timestamp_ms, payload FROM {table}
             WHERE timestamp_ms >= ?1 ORDER BY timestamp_ms ASC LIMIT 1"
        );
        let before = {
            let mut stmt = self.conn.prepare(&before_sql)?;
            let mut rows = stmt.query(params![timestamp_ms])?;
            if let Some(row) = rows.next()? {
                let ts: f64 = row.get(0)?;
                let payload: String = row.get(1)?;
                Some((ts, serde_json::from_str::<serde_json::Value>(&payload)?))
            } else {
                None
            }
        };
        let after = {
            let mut stmt = self.conn.prepare(&after_sql)?;
            let mut rows = stmt.query(params![timestamp_ms])?;
            if let Some(row) = rows.next()? {
                let ts: f64 = row.get(0)?;
                let payload: String = row.get(1)?;
                Some((ts, serde_json::from_str::<serde_json::Value>(&payload)?))
            } else {
                None
            }
        };
        Ok(match (before, after) {
            (Some(b), Some(a)) => {
                if (b.0 - timestamp_ms).abs() <= (a.0 - timestamp_ms).abs() {
                    Some(b)
                } else {
                    Some(a)
                }
            }
            (Some(b), None) => Some(b),
            (None, Some(a)) => Some(a),
            (None, None) => None,
        })
    }

    pub fn list_market_events_by_type(
        &self,
        event_type: &str,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT timestamp_ms, event_type, price, direction, metadata_json,
                    session_date, session_type, session_segment, trading_day
             FROM market_events WHERE event_type = ?1
             ORDER BY timestamp_ms DESC LIMIT ?2",
        )?;
        let rows = stmt.query_map(rusqlite::params![event_type, limit as i64], |row| {
            let metadata_str: Option<String> = row.get(4)?;
            let metadata: serde_json::Value = metadata_str
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_else(|| serde_json::json!({}));
            Ok(serde_json::json!({
                "timestampMs": row.get::<_, f64>(0)?,
                "eventType": row.get::<_, String>(1)?,
                "price": row.get::<_, f64>(2)?,
                "direction": row.get::<_, Option<String>>(3)?,
                "metadata": metadata,
                "sessionDate": row.get::<_, String>(5)?,
                "sessionType": row.get::<_, Option<String>>(6)?,
                "sessionSegment": row.get::<_, Option<String>>(7)?,
                "tradingDay": row.get::<_, Option<String>>(8)?,
            }))
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn list_market_events_by_prefix(
        &self,
        event_type_prefix: &str,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>, DbError> {
        let like_pattern = format!("{event_type_prefix}%");
        let mut stmt = self.conn.prepare(
            "SELECT timestamp_ms, event_type, price, direction, metadata_json,
                    session_date, session_type, session_segment, trading_day
             FROM market_events WHERE event_type LIKE ?1
             ORDER BY timestamp_ms DESC LIMIT ?2",
        )?;
        let rows = stmt.query_map(rusqlite::params![like_pattern, limit as i64], |row| {
            let metadata_str: Option<String> = row.get(4)?;
            let metadata: serde_json::Value = metadata_str
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_else(|| serde_json::json!({}));
            Ok(serde_json::json!({
                "timestampMs": row.get::<_, f64>(0)?,
                "eventType": row.get::<_, String>(1)?,
                "price": row.get::<_, f64>(2)?,
                "direction": row.get::<_, Option<String>>(3)?,
                "metadata": metadata,
                "sessionDate": row.get::<_, String>(5)?,
                "sessionType": row.get::<_, Option<String>>(6)?,
                "sessionSegment": row.get::<_, Option<String>>(7)?,
                "tradingDay": row.get::<_, Option<String>>(8)?,
            }))
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn list_market_events_for_research(
        &self,
        event_type: &str,
        start_date: Option<&str>,
        end_date: Option<&str>,
        scope: Option<&SessionScopeFilter>,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT timestamp_ms, event_type, level_name, price, direction, metadata_json,
                    session_date, session_type, session_segment, trading_day, root_symbol, contract_symbol
             FROM market_events WHERE event_type = ?1
             ORDER BY timestamp_ms ASC",
        )?;
        let rows = stmt.query_map([event_type], |row| {
            let metadata_str: Option<String> = row.get(5)?;
            let metadata: serde_json::Value = metadata_str
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_else(|| serde_json::json!({}));
            Ok((
                row.get::<_, f64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, f64>(3)?,
                row.get::<_, Option<String>>(4)?,
                metadata,
                row.get::<_, String>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, Option<String>>(8)?,
                row.get::<_, Option<String>>(9)?,
                row.get::<_, Option<String>>(10)?,
                row.get::<_, Option<String>>(11)?,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows.filter_map(|r| r.ok()) {
            let (
                timestamp_ms,
                actual_event_type,
                level_name,
                price,
                direction,
                metadata,
                session_date,
                session_type,
                session_segment,
                trading_day,
                root_symbol,
                contract_symbol,
            ) = row;
            if !contract_fields_match_scope(
                root_symbol.as_deref(),
                contract_symbol.as_deref(),
                scope,
            ) {
                continue;
            }
            let Some(analysis_day) = analysis_day_for_scope(&session_date, timestamp_ms, scope)
            else {
                continue;
            };
            if let Some(sd) = start_date {
                if analysis_day.as_str() < sd {
                    continue;
                }
            }
            if let Some(ed) = end_date {
                if analysis_day.as_str() > ed {
                    continue;
                }
            }
            out.push(serde_json::json!({
                "timestampMs": timestamp_ms,
                "eventType": actual_event_type,
                "levelName": level_name,
                "price": price,
                "direction": direction,
                "metadata": metadata,
                "sessionDate": session_date,
                "sessionType": session_type,
                "sessionSegment": session_segment,
                "tradingDay": trading_day.unwrap_or(analysis_day),
            }));
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    pub fn latest_microstructure_snapshot(&self) -> Result<Option<serde_json::Value>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT payload FROM microstructure_snapshots ORDER BY timestamp_ms DESC LIMIT 1",
        )?;
        let mut rows = stmt.query([])?;
        if let Some(row) = rows.next()? {
            let payload: String = row.get(0)?;
            Ok(serde_json::from_str(&payload).ok())
        } else {
            Ok(None)
        }
    }

    pub fn latest_tick_timestamp_ms(&self) -> Result<Option<f64>, DbError> {
        let ts = self
            .conn
            .query_row("SELECT MAX(timestamp_ms) FROM raw_ticks", [], |r| r.get(0))
            .ok()
            .flatten();
        Ok(ts)
    }

    /// Min/max timestamp and count for `raw_ticks` for a contract inside `[start_ms, end_ms_exclusive)`.
    pub fn raw_ticks_time_bounds_for_contract_in_range(
        &self,
        contract_symbol: &str,
        start_ms: f64,
        end_ms_exclusive: f64,
    ) -> Result<(Option<f64>, Option<f64>, i64), DbError> {
        if contract_symbol.is_empty() {
            return self
                .conn
                .query_row(
                    "SELECT MIN(timestamp_ms), MAX(timestamp_ms), COUNT(1) FROM raw_ticks \
                 WHERE timestamp_ms >= ?1 AND timestamp_ms < ?2",
                    params![start_ms, end_ms_exclusive],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .map_err(DbError::Sqlite);
        }
        self.conn
            .query_row(
                "SELECT MIN(timestamp_ms), MAX(timestamp_ms), COUNT(1) FROM raw_ticks \
                 WHERE contract_symbol = ?1 AND timestamp_ms >= ?2 AND timestamp_ms < ?3",
                params![contract_symbol, start_ms, end_ms_exclusive],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .map_err(DbError::Sqlite)
    }

    /// Min/max timestamp and row count for `raw_ticks` scoped to a contract (e.g. `NQM6.CME`).
    ///
    /// Empty contract string is treated as “no filter” (whole table).
    pub fn raw_ticks_time_bounds_for_contract(
        &self,
        contract_symbol: &str,
    ) -> Result<(Option<f64>, Option<f64>, i64), DbError> {
        if contract_symbol.is_empty() {
            return self.raw_ticks_time_bounds_unscoped();
        }
        let row = self.conn.query_row(
            "SELECT MIN(timestamp_ms), MAX(timestamp_ms), COUNT(1) FROM raw_ticks WHERE contract_symbol = ?1",
            params![contract_symbol],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        );
        match row {
            Ok(t) => Ok(t),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok((None, None, 0)),
            Err(e) => Err(DbError::Sqlite(e)),
        }
    }

    /// Min/max over all `raw_ticks` regardless of contract.
    pub fn raw_ticks_time_bounds_unscoped(
        &self,
    ) -> Result<(Option<f64>, Option<f64>, i64), DbError> {
        self.conn
            .query_row(
                "SELECT MIN(timestamp_ms), MAX(timestamp_ms), COUNT(1) FROM raw_ticks",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .map_err(DbError::Sqlite)
    }

    pub fn count_playbook_signals(&self) -> Result<i64, DbError> {
        Ok(self
            .conn
            .query_row("SELECT COUNT(1) FROM playbook_signals", [], |r| r.get(0))?)
    }

    /// Persist a data integrity validation run.
    pub fn insert_validation_run(
        &self,
        run_at_ms: f64,
        status: &str,
        details: &serde_json::Value,
    ) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO validation_runs (run_at_ms, status, details) VALUES (?1, ?2, ?3)",
            params![run_at_ms, status, serde_json::to_string(details)?],
        )?;
        Ok(())
    }

    /// Batch-insert raw ticks inside a single transaction.
    pub fn insert_raw_ticks_batch(&self, ticks: &[RawTickBatchRow]) -> Result<(), DbError> {
        let tx = self.conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO raw_ticks
                 (timestamp_ms, price, volume, bid, ask, is_buy, session_date, root_symbol, contract_symbol)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            )?;
            for (ts, price, vol, bid, ask, is_buy, session_date, root_symbol, contract_symbol) in
                ticks
            {
                stmt.execute(params![
                    ts,
                    price,
                    vol,
                    bid,
                    ask,
                    i64::from(*is_buy),
                    session_date,
                    root_symbol,
                    contract_symbol,
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Archive raw ticks older than `cutoff_date` (YYYY-MM-DD) into zstd-compressed
    /// monthly files under `archive_dir`. Returns the number of ticks archived.
    pub fn archive_cold_ticks(
        &self,
        cutoff_date: &str,
        archive_dir: &std::path::Path,
    ) -> Result<usize, DbError> {
        std::fs::create_dir_all(archive_dir).ok();
        let mut stmt = self.conn.prepare(
            "SELECT timestamp_ms, price, volume, bid, ask, is_buy, session_date
             FROM raw_ticks WHERE session_date < ?1 ORDER BY timestamp_ms",
        )?;
        let rows: Vec<(f64, f64, f64, f64, f64, i64, String)> = stmt
            .query_map(params![cutoff_date], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            })?
            .filter_map(|r| r.ok())
            .collect();

        if rows.is_empty() {
            return Ok(0);
        }

        // Group by month (YYYY-MM)
        let mut by_month: std::collections::BTreeMap<String, Vec<String>> =
            std::collections::BTreeMap::new();
        for (ts, price, vol, bid, ask, is_buy, session_date) in &rows {
            let month = if session_date.len() >= 7 {
                &session_date[..7]
            } else {
                "unknown"
            };
            let line = format!("{ts},{price},{vol},{bid},{ask},{is_buy},{session_date}");
            by_month.entry(month.to_string()).or_default().push(line);
        }

        for (month, lines) in &by_month {
            let path = archive_dir.join(format!("{month}.ticks.zst"));
            let data = lines.join("\n");
            let compressed = zstd::encode_all(data.as_bytes(), 3).map_err(|e| {
                DbError::Sqlite(rusqlite::Error::ToSqlConversionFailure(Box::new(e)))
            })?;
            std::fs::write(&path, compressed).ok();
        }

        let count = rows.len();
        self.conn.execute(
            "DELETE FROM raw_ticks WHERE session_date < ?1",
            params![cutoff_date],
        )?;

        Ok(count)
    }

    // ------------------------------------------------------------------
    // Market events (research infrastructure)
    // ------------------------------------------------------------------

    /// Batch-insert market events in a single transaction.
    pub fn insert_market_events_batch(&self, events: &[MarketEvent]) -> Result<(), DbError> {
        if events.is_empty() {
            return Ok(());
        }
        let tx = self.conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO market_events
                 (session_date, timestamp_ms, event_type, level_name, price, direction, sequence_num, metadata_json,
                  session_type, session_segment, trading_day)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            )?;
            for e in events {
                let meta = e
                    .metadata
                    .as_ref()
                    .map(|m| serde_json::to_string(m).unwrap_or_default());
                stmt.execute(params![
                    e.session_date,
                    e.timestamp_ms,
                    e.event_type,
                    e.level_name,
                    e.price,
                    e.direction,
                    e.sequence_num,
                    meta,
                    &e.session_type,
                    &e.session_segment,
                    &e.trading_day,
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Count events of a given type, optionally filtered by date range.
    pub fn count_events_by_type(
        &self,
        event_type: &str,
        start_date: Option<&str>,
        end_date: Option<&str>,
        scope: Option<&SessionScopeFilter>,
    ) -> Result<(i64, i64, i64), DbError> {
        let mut event_conditions = vec!["event_type = ?1".to_string()];
        let mut event_bind_values: Vec<Box<dyn rusqlite::types::ToSql>> =
            vec![Box::new(event_type.to_string())];
        if let Some(sd) = start_date {
            event_conditions.push(format!("session_date >= ?{}", event_bind_values.len() + 1));
            event_bind_values.push(Box::new(sd.to_string()));
        }
        if let Some(ed) = end_date {
            event_conditions.push(format!("session_date <= ?{}", event_bind_values.len() + 1));
            event_bind_values.push(Box::new(ed.to_string()));
        }
        let event_sql = format!(
            "SELECT timestamp_ms, session_date, session_type, session_segment, trading_day
             FROM market_events WHERE {}",
            event_conditions.join(" AND ")
        );
        let mut event_stmt = self.conn.prepare(&event_sql)?;
        let event_params_ref: Vec<&dyn rusqlite::types::ToSql> =
            event_bind_values.iter().map(|b| b.as_ref()).collect();
        let event_rows = event_stmt.query_map(event_params_ref.as_slice(), |row| {
            Ok((
                row.get::<_, f64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
            ))
        })?;

        let mut total_events = 0_i64;
        let mut sessions_with_event = BTreeSet::new();
        for row in event_rows.filter_map(|r| r.ok()) {
            let (ts, session_date, st, seg, td) = row;
            if let Some(trading_day) = trading_day_if_scope_match_for_event_row(
                ts,
                st.as_deref(),
                seg.as_deref(),
                td.as_deref(),
                Some(session_date.as_str()),
                scope,
            ) {
                total_events += 1;
                sessions_with_event.insert(trading_day);
            }
        }

        let mut all_session_conditions = Vec::new();
        let mut all_session_bind_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        if let Some(sd) = start_date {
            all_session_conditions.push(format!(
                "session_date >= ?{}",
                all_session_bind_values.len() + 1
            ));
            all_session_bind_values.push(Box::new(sd.to_string()));
        }
        if let Some(ed) = end_date {
            all_session_conditions.push(format!(
                "session_date <= ?{}",
                all_session_bind_values.len() + 1
            ));
            all_session_bind_values.push(Box::new(ed.to_string()));
        }
        let all_where_clause = if all_session_conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", all_session_conditions.join(" AND "))
        };
        let all_sql = format!(
            "SELECT timestamp_ms, session_date, session_type, session_segment, trading_day
             FROM market_events {all_where_clause}"
        );
        let mut all_stmt = self.conn.prepare(&all_sql)?;
        let all_params_ref: Vec<&dyn rusqlite::types::ToSql> =
            all_session_bind_values.iter().map(|b| b.as_ref()).collect();
        let all_rows = all_stmt.query_map(all_params_ref.as_slice(), |row| {
            Ok((
                row.get::<_, f64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
            ))
        })?;

        let mut total_sessions = BTreeSet::new();
        for row in all_rows.filter_map(|r| r.ok()) {
            let (ts, session_date, st, seg, td) = row;
            if let Some(trading_day) = trading_day_if_scope_match_for_event_row(
                ts,
                st.as_deref(),
                seg.as_deref(),
                td.as_deref(),
                Some(session_date.as_str()),
                scope,
            ) {
                total_sessions.insert(trading_day);
            }
        }

        // Include summarized sessions as denominator fallback when there are no event rows.
        let summary_start = scope
            .and_then(|s| s.trading_day_start.as_deref())
            .or(start_date);
        let summary_end = scope
            .and_then(|s| s.trading_day_end.as_deref())
            .or(end_date);
        let summaries = self.list_session_summaries_scoped(
            summary_start,
            summary_end,
            None,
            scope.and_then(|s| s.session_type.as_deref()),
            100_000,
            scope,
        )?;
        for s in summaries {
            total_sessions.insert(s.session_date);
        }

        Ok((
            total_events,
            sessions_with_event.len() as i64,
            total_sessions.len() as i64,
        ))
    }

    /// Count events of a specific type per session for conditional queries.
    pub fn event_counts_per_session(
        &self,
        event_type: &str,
        start_date: Option<&str>,
        end_date: Option<&str>,
        scope: Option<&SessionScopeFilter>,
    ) -> Result<Vec<(String, i64)>, DbError> {
        let mut conditions = vec!["event_type = ?1".to_string()];
        let mut bind_values: Vec<Box<dyn rusqlite::types::ToSql>> =
            vec![Box::new(event_type.to_string())];
        if let Some(sd) = start_date {
            conditions.push(format!("session_date >= ?{}", bind_values.len() + 1));
            bind_values.push(Box::new(sd.to_string()));
        }
        if let Some(ed) = end_date {
            conditions.push(format!("session_date <= ?{}", bind_values.len() + 1));
            bind_values.push(Box::new(ed.to_string()));
        }
        let sql = format!(
            "SELECT timestamp_ms, session_date, session_type, session_segment, trading_day
             FROM market_events WHERE {}",
            conditions.join(" AND ")
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let params_ref: Vec<&dyn rusqlite::types::ToSql> =
            bind_values.iter().map(|b| b.as_ref()).collect();
        let rows = stmt.query_map(params_ref.as_slice(), |row| {
            Ok((
                row.get::<_, f64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
            ))
        })?;

        let mut by_day: BTreeMap<String, i64> = BTreeMap::new();
        for row in rows.filter_map(|r| r.ok()) {
            let (ts, session_date, st, seg, td) = row;
            if let Some(trading_day) = trading_day_if_scope_match_for_event_row(
                ts,
                st.as_deref(),
                seg.as_deref(),
                td.as_deref(),
                Some(session_date.as_str()),
                scope,
            ) {
                *by_day.entry(trading_day).or_insert(0) += 1;
            }
        }
        Ok(by_day.into_iter().collect())
    }

    // ------------------------------------------------------------------
    // Session summaries (research infrastructure)
    // ------------------------------------------------------------------

    /// Insert or replace a session summary.
    pub fn upsert_session_summary(&self, s: &SessionSummary) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO session_summaries
             (session_date, session_type, root_symbol, contract_symbol, contract_month, symbol_resolution_mode,
              carry_forward_levels_valid, rollover_warning, open_price, high, low, close,
              poc, vah, val, ib_high, ib_low, ib_range, ib_mid,
              or_high, or_low, day_type, profile_shape, balance_state,
              total_volume, tick_count,
              session_delta, cumulative_delta, dnp, dnva_high, dnva_low,
              vwap_close, signal_count, single_prints_direction,
              excess_high, excess_low, poor_high, poor_low, rvol_ratio,
              close_vs_ib_mid, close_vs_vwap, close_vs_poc, snapshot_json)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26,?27,?28,?29,?30,?31,?32,?33,?34,?35,?36,?37,?38,?39,?40,?41,?42,?43)
             ON CONFLICT(session_date, session_type) DO UPDATE SET
              session_type=excluded.session_type, root_symbol=excluded.root_symbol,
              contract_symbol=excluded.contract_symbol, contract_month=excluded.contract_month,
              symbol_resolution_mode=excluded.symbol_resolution_mode,
              carry_forward_levels_valid=excluded.carry_forward_levels_valid,
              rollover_warning=excluded.rollover_warning, open_price=excluded.open_price,
               high=excluded.high, low=excluded.low, close=excluded.close,
               poc=excluded.poc, vah=excluded.vah, val=excluded.val,
               ib_high=excluded.ib_high, ib_low=excluded.ib_low, ib_range=excluded.ib_range,
               ib_mid=excluded.ib_mid, or_high=excluded.or_high, or_low=excluded.or_low,
               day_type=excluded.day_type, profile_shape=excluded.profile_shape,
               balance_state=excluded.balance_state, total_volume=excluded.total_volume,
               tick_count=excluded.tick_count, session_delta=excluded.session_delta,
               cumulative_delta=excluded.cumulative_delta, dnp=excluded.dnp,
               dnva_high=excluded.dnva_high, dnva_low=excluded.dnva_low,
               vwap_close=excluded.vwap_close, signal_count=excluded.signal_count,
               single_prints_direction=excluded.single_prints_direction,
               excess_high=excluded.excess_high, excess_low=excluded.excess_low,
               poor_high=excluded.poor_high, poor_low=excluded.poor_low,
               rvol_ratio=excluded.rvol_ratio, close_vs_ib_mid=excluded.close_vs_ib_mid,
               close_vs_vwap=excluded.close_vs_vwap, close_vs_poc=excluded.close_vs_poc,
               snapshot_json=excluded.snapshot_json",
            params![
                s.session_date, s.session_type, s.root_symbol, s.contract_symbol, s.contract_month,
                s.symbol_resolution_mode, i64::from(s.carry_forward_levels_valid), s.rollover_warning,
                s.open_price, s.high, s.low, s.close, s.poc, s.vah, s.val, s.ib_high, s.ib_low,
                s.ib_range, s.ib_mid, s.or_high, s.or_low, s.day_type, s.profile_shape, s.balance_state,
                s.total_volume, s.tick_count,
                s.session_delta, s.cumulative_delta, s.dnp, s.dnva_high, s.dnva_low,
                s.vwap_close, s.signal_count, s.single_prints_direction,
                i64::from(s.excess_high), i64::from(s.excess_low),
                i64::from(s.poor_high), i64::from(s.poor_low), s.rvol_ratio,
                s.close_vs_ib_mid, s.close_vs_vwap, s.close_vs_poc, s.snapshot_json,
            ],
        )?;
        Ok(())
    }

    /// Check if a session summary exists for a given date (RTH only, for backward compat).
    pub fn has_session_summary(&self, session_date: &str) -> Result<bool, DbError> {
        self.has_session_summary_for(session_date, "RTH")
    }

    /// Check if a session summary exists for a given date and session type.
    pub fn has_session_summary_for(
        &self,
        session_date: &str,
        session_type: &str,
    ) -> Result<bool, DbError> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(1) FROM session_summaries WHERE session_date = ?1 AND session_type = ?2",
            params![session_date, session_type],
            |r| r.get(0),
        )?;
        Ok(count > 0)
    }

    /// Save an untested DNP (price did not revisit DNP ± tolerance during the session).
    pub fn save_untested_dnp(
        &self,
        session_date: &str,
        session_type: &str,
        dnp: f64,
    ) -> Result<(), DbError> {
        let created_at = chrono::Utc::now().timestamp_millis();
        self.conn.execute(
            "INSERT OR REPLACE INTO untested_dnps (session_date, session_type, dnp, created_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![session_date, session_type, dnp, created_at],
        )?;
        Ok(())
    }

    /// Remove untested DNP record when DNP was tested during session (e.g. on reprocess).
    pub fn delete_untested_dnp_for_session(
        &self,
        session_date: &str,
        session_type: &str,
    ) -> Result<(), DbError> {
        self.conn.execute(
            "DELETE FROM untested_dnps WHERE session_date = ?1 AND session_type = ?2",
            params![session_date, session_type],
        )?;
        Ok(())
    }

    /// Remove untested DNP rows that have been touched by a completed session range.
    pub fn delete_untested_dnps_touched_by_range(
        &self,
        low: f64,
        high: f64,
        tolerance: f64,
        exclude_current_session: Option<(&str, &str)>,
    ) -> Result<usize, DbError> {
        if low <= 0.0 && high <= 0.0 {
            return Ok(0);
        }
        let tol = tolerance.max(0.0);
        let lo = low.min(high) - tol;
        let hi = low.max(high) + tol;
        let rows = if let Some((session_date, session_type)) = exclude_current_session {
            self.conn.execute(
                "DELETE FROM untested_dnps
                 WHERE dnp BETWEEN ?1 AND ?2
                 AND NOT (session_date = ?3 AND session_type = ?4)",
                params![lo, hi, session_date, session_type],
            )?
        } else {
            self.conn.execute(
                "DELETE FROM untested_dnps WHERE dnp BETWEEN ?1 AND ?2",
                params![lo, hi],
            )?
        };
        Ok(rows)
    }

    /// Load recent untested DNPs, most recent first. Used for key levels.
    pub fn load_untested_dnps(&self, limit: usize) -> Result<Vec<(String, String, f64)>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT session_date, session_type, dnp FROM untested_dnps
             ORDER BY created_at DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit as i64], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, f64>(2)?,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(DbError::from)?);
        }
        Ok(out)
    }

    /// Remove backfill-derived research rows for a session before force reprocess.
    pub fn purge_session_research(&self, session_date: &str) -> Result<(), DbError> {
        self.purge_historical_session(session_date, &["backfill", "backtest"])
    }

    /// Remove historical replay artifacts for a session while preserving live rows.
    /// Uses `unchecked_transaction` which acts as a savepoint when called inside an
    /// existing transaction (e.g. from `persist_historical_session`).
    pub fn purge_historical_session(
        &self,
        session_date: &str,
        sources: &[&str],
    ) -> Result<(), DbError> {
        let tx = self.conn.unchecked_transaction()?;
        tx.execute(
            "DELETE FROM market_events WHERE session_date = ?1",
            params![session_date],
        )?;
        tx.execute(
            "DELETE FROM session_summaries WHERE session_date = ?1",
            params![session_date],
        )?;
        tx.execute(
            "DELETE FROM session_volume_curves WHERE session_date = ?1",
            params![session_date],
        )?;
        tx.execute(
            "DELETE FROM untested_dnps WHERE session_date = ?1",
            params![session_date],
        )?;
        for source in sources {
            tx.execute(
                "DELETE FROM playbook_signals WHERE session_date = ?1 AND source = ?2",
                params![session_date, source],
            )?;
            tx.execute(
                "DELETE FROM signal_outcomes WHERE session_date = ?1 AND source = ?2",
                params![session_date, source],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Load recent RTH session volumes for RVOL baseline construction.
    pub fn recent_rth_session_volumes(&self, limit: usize) -> Result<Vec<f64>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT total_volume
             FROM session_summaries
             WHERE session_type = 'RTH' AND total_volume > 0
             ORDER BY session_date DESC
             LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit as i64], |row| row.get::<_, f64>(0))?;
        let mut volumes: Vec<f64> = rows.filter_map(|r| r.ok()).collect();
        volumes.reverse();
        Ok(volumes)
    }

    /// Save a per-bucket cumulative volume curve for a session.
    pub fn save_volume_curve(
        &self,
        session_date: &str,
        session_type: &str,
        curve: &[f64],
    ) -> Result<(), DbError> {
        let tx = self.conn.unchecked_transaction()?;
        // Clear any existing curve for this session/type.
        tx.execute(
            "DELETE FROM session_volume_curves WHERE session_date = ?1 AND session_type = ?2",
            params![session_date, session_type],
        )?;
        for (i, &vol) in curve.iter().enumerate() {
            tx.execute(
                "INSERT INTO session_volume_curves (session_date, session_type, bucket_index, cumulative_volume)
                 VALUES (?1, ?2, ?3, ?4)",
                params![session_date, session_type, i as i64, vol],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Load recent session volume curves from the session_volume_curves table.
    /// Returns actual per-bucket cumulative volumes for up to `limit` sessions.
    /// Falls back to `recent_rth_session_volumes` + linear interpolation if no curves stored.
    pub fn recent_session_volume_curves(
        &self,
        session_type: &str,
        limit: usize,
    ) -> Result<Vec<Vec<f64>>, DbError> {
        // Get distinct session dates with stored curves.
        let mut date_stmt = self.conn.prepare(
            "SELECT DISTINCT session_date FROM session_volume_curves
             WHERE session_type = ?1
             ORDER BY session_date DESC
             LIMIT ?2",
        )?;
        let dates: Vec<String> = date_stmt
            .query_map(params![session_type, limit as i64], |row| {
                row.get::<_, String>(0)
            })?
            .filter_map(|r| r.ok())
            .collect();

        if dates.is_empty() {
            // Fallback: build curves from total session volumes.
            use crate::pipelines::RvolPipeline;
            let volumes = self.recent_rth_session_volumes(limit)?;
            return Ok(volumes
                .into_iter()
                .map(RvolPipeline::curve_from_total_volume)
                .collect());
        }

        let mut curves = Vec::with_capacity(dates.len());
        let mut bucket_stmt = self.conn.prepare(
            "SELECT bucket_index, cumulative_volume FROM session_volume_curves
             WHERE session_date = ?1 AND session_type = ?2
             ORDER BY bucket_index ASC",
        )?;
        for date in dates.iter().rev() {
            let buckets: Vec<(usize, f64)> = bucket_stmt
                .query_map(params![date, session_type], |row| {
                    Ok((row.get::<_, i64>(0)? as usize, row.get::<_, f64>(1)?))
                })?
                .filter_map(|r| r.ok())
                .collect();
            if buckets.is_empty() {
                continue;
            }
            let max_idx = buckets.iter().map(|(i, _)| *i).max().unwrap_or(0);
            let mut curve = vec![0.0; max_idx + 1];
            for (i, vol) in buckets {
                if i < curve.len() {
                    curve[i] = vol;
                }
            }
            curves.push(curve);
        }

        Ok(curves)
    }

    /// List session summaries with optional filters.
    pub fn list_session_summaries(
        &self,
        start_date: Option<&str>,
        end_date: Option<&str>,
        day_type_filter: Option<&str>,
        session_type_filter: Option<&str>,
        limit: usize,
    ) -> Result<Vec<SessionSummary>, DbError> {
        self.list_session_summaries_scoped(
            start_date,
            end_date,
            day_type_filter,
            session_type_filter,
            limit,
            None,
        )
    }

    pub fn list_session_summaries_scoped(
        &self,
        start_date: Option<&str>,
        end_date: Option<&str>,
        day_type_filter: Option<&str>,
        session_type_filter: Option<&str>,
        limit: usize,
        scope: Option<&SessionScopeFilter>,
    ) -> Result<Vec<SessionSummary>, DbError> {
        let mut conditions = Vec::new();
        let mut bind_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(sd) = start_date {
            conditions.push(format!("session_date >= ?{}", bind_values.len() + 1));
            bind_values.push(Box::new(sd.to_string()));
        }
        if let Some(ed) = end_date {
            conditions.push(format!("session_date <= ?{}", bind_values.len() + 1));
            bind_values.push(Box::new(ed.to_string()));
        }
        if let Some(dt) = day_type_filter {
            conditions.push(format!("day_type = ?{}", bind_values.len() + 1));
            bind_values.push(Box::new(dt.to_string()));
        }
        if let Some(st) = session_type_filter {
            conditions.push(format!("session_type = ?{}", bind_values.len() + 1));
            bind_values.push(Box::new(st.to_string()));
        }
        if let Some(scope) = scope {
            if let Some(root) = scope.root_symbol.as_deref() {
                conditions.push(format!("root_symbol = ?{}", bind_values.len() + 1));
                bind_values.push(Box::new(root.to_string()));
            }
            if let Some(contract) = scope.contract_symbol.as_deref() {
                conditions.push(format!("contract_symbol = ?{}", bind_values.len() + 1));
                bind_values.push(Box::new(contract.to_string()));
            }
            if !scope.include_rollover_sessions {
                conditions.push("COALESCE(carry_forward_levels_valid, 1) = 1".to_string());
            }
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let sql = format!(
            "SELECT session_date, session_type, root_symbol, contract_symbol, contract_month,
                    symbol_resolution_mode, carry_forward_levels_valid, rollover_warning,
                    open_price, high, low, close,
                    poc, vah, val, ib_high, ib_low, ib_range, ib_mid,
                    or_high, or_low, day_type, profile_shape, balance_state,
                    total_volume, tick_count,
                    session_delta, cumulative_delta, dnp, dnva_high, dnva_low,
                    vwap_close, signal_count, single_prints_direction,
                    excess_high, excess_low, poor_high, poor_low, rvol_ratio,
                    close_vs_ib_mid, close_vs_vwap, close_vs_poc, snapshot_json
             FROM session_summaries {where_clause}
             ORDER BY session_date DESC LIMIT ?{}",
            bind_values.len() + 1
        );
        bind_values.push(Box::new(limit as i64));

        let mut stmt = self.conn.prepare(&sql)?;
        let params_ref: Vec<&dyn rusqlite::types::ToSql> =
            bind_values.iter().map(|b| b.as_ref()).collect();
        let rows = stmt.query_map(params_ref.as_slice(), |row| {
            Ok(SessionSummary {
                session_date: row.get(0)?,
                session_type: row.get(1)?,
                root_symbol: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                contract_symbol: row.get::<_, Option<String>>(3)?.unwrap_or_default(),
                contract_month: row.get(4)?,
                symbol_resolution_mode: row
                    .get::<_, Option<String>>(5)?
                    .unwrap_or_else(|| "hybrid".to_string()),
                carry_forward_levels_valid: row.get::<_, i64>(6)? != 0,
                rollover_warning: row.get(7)?,
                open_price: row.get(8)?,
                high: row.get(9)?,
                low: row.get(10)?,
                close: row.get(11)?,
                poc: row.get(12)?,
                vah: row.get(13)?,
                val: row.get(14)?,
                ib_high: row.get(15)?,
                ib_low: row.get(16)?,
                ib_range: row.get(17)?,
                ib_mid: row.get(18)?,
                or_high: row.get(19)?,
                or_low: row.get(20)?,
                day_type: row.get::<_, Option<String>>(21)?.unwrap_or_default(),
                profile_shape: row.get::<_, Option<String>>(22)?.unwrap_or_default(),
                balance_state: row.get::<_, Option<String>>(23)?.unwrap_or_default(),
                total_volume: row.get(24)?,
                tick_count: row.get(25)?,
                session_delta: row.get(26)?,
                cumulative_delta: row.get(27)?,
                dnp: row.get(28)?,
                dnva_high: row.get(29)?,
                dnva_low: row.get(30)?,
                vwap_close: row.get(31)?,
                signal_count: row.get(32)?,
                single_prints_direction: row.get::<_, Option<String>>(33)?.unwrap_or_default(),
                excess_high: row.get::<_, i64>(34)? != 0,
                excess_low: row.get::<_, i64>(35)? != 0,
                poor_high: row.get::<_, i64>(36)? != 0,
                poor_low: row.get::<_, i64>(37)? != 0,
                rvol_ratio: row.get(38)?,
                close_vs_ib_mid: row.get::<_, Option<String>>(39)?.unwrap_or_default(),
                close_vs_vwap: row.get::<_, Option<String>>(40)?.unwrap_or_default(),
                close_vs_poc: row.get::<_, Option<String>>(41)?.unwrap_or_default(),
                snapshot_json: row.get(42)?,
            })
        })?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    /// Get metric values from session_summaries for distribution analysis.
    pub fn metric_values(
        &self,
        column: &str,
        start_date: Option<&str>,
        end_date: Option<&str>,
        session_type_filter: Option<&str>,
    ) -> Result<Vec<f64>, DbError> {
        self.metric_values_scoped(column, start_date, end_date, session_type_filter, None)
    }

    pub fn metric_values_scoped(
        &self,
        column: &str,
        start_date: Option<&str>,
        end_date: Option<&str>,
        session_type_filter: Option<&str>,
        scope: Option<&SessionScopeFilter>,
    ) -> Result<Vec<f64>, DbError> {
        let allowed = [
            "ib_range",
            "high",
            "low",
            "close",
            "open_price",
            "poc",
            "vah",
            "val",
            "ib_high",
            "ib_low",
            "ib_mid",
            "or_high",
            "or_low",
            "total_volume",
            "tick_count",
            "session_delta",
            "cumulative_delta",
            "dnp",
            "dnva_high",
            "dnva_low",
            "vwap_close",
            "signal_count",
            "rvol_ratio",
        ];
        if !allowed.contains(&column) {
            return Ok(Vec::new());
        }
        let mut conditions = vec![format!("{column} IS NOT NULL")];
        let mut bind_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        if let Some(sd) = start_date {
            conditions.push(format!("session_date >= ?{}", bind_values.len() + 1));
            bind_values.push(Box::new(sd.to_string()));
        }
        if let Some(ed) = end_date {
            conditions.push(format!("session_date <= ?{}", bind_values.len() + 1));
            bind_values.push(Box::new(ed.to_string()));
        }
        if let Some(st) = session_type_filter {
            conditions.push(format!("session_type = ?{}", bind_values.len() + 1));
            bind_values.push(Box::new(st.to_string()));
        }
        if let Some(scope) = scope {
            if let Some(root) = scope.root_symbol.as_deref() {
                conditions.push(format!("root_symbol = ?{}", bind_values.len() + 1));
                bind_values.push(Box::new(root.to_string()));
            }
            if let Some(contract) = scope.contract_symbol.as_deref() {
                conditions.push(format!("contract_symbol = ?{}", bind_values.len() + 1));
                bind_values.push(Box::new(contract.to_string()));
            }
            if !scope.include_rollover_sessions {
                conditions.push("COALESCE(carry_forward_levels_valid, 1) = 1".to_string());
            }
        }
        let sql = format!(
            "SELECT {column} FROM session_summaries WHERE {} ORDER BY session_date",
            conditions.join(" AND ")
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let params_ref: Vec<&dyn rusqlite::types::ToSql> =
            bind_values.iter().map(|b| b.as_ref()).collect();
        let rows = stmt.query_map(params_ref.as_slice(), |row| row.get::<_, f64>(0))?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    // ------------------------------------------------------------------
    // Signal outcomes (research infrastructure)
    // ------------------------------------------------------------------

    /// Insert a new pending signal outcome.
    pub fn insert_signal_outcome(&self, o: &SignalOutcome) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT OR IGNORE INTO signal_outcomes
             (signal_id, setup_id, setup_name, session_date, root_symbol, contract_symbol, source, job_id,
              fired_at_ms, fired_price, target_price, stop_price, outcome, outcome_at_ms,
              max_favorable_excursion, max_adverse_excursion, r_result, time_to_outcome_ms,
              rvol_at_fire, rvol_bucket_at_fire)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20)",
            params![
                o.signal_id,
                o.setup_id,
                o.setup_name,
                o.session_date,
                o.root_symbol,
                o.contract_symbol,
                o.source,
                o.job_id,
                o.fired_at_ms,
                o.fired_price,
                o.target_price,
                o.stop_price,
                o.outcome,
                o.outcome_at_ms,
                o.max_favorable_excursion,
                o.max_adverse_excursion,
                o.r_result,
                o.time_to_outcome_ms,
                o.rvol_at_fire,
                o.rvol_bucket_at_fire,
            ],
        )?;
        Ok(())
    }

    /// Update a signal outcome after resolution.
    #[allow(clippy::too_many_arguments)]
    pub fn resolve_signal_outcome(
        &self,
        signal_id: &str,
        outcome: &str,
        outcome_at_ms: f64,
        mfe: f64,
        mae: f64,
        r_result: Option<f64>,
    ) -> Result<(), DbError> {
        let time_to = outcome_at_ms;
        self.conn.execute(
            "UPDATE signal_outcomes SET outcome=?2, outcome_at_ms=?3,
             max_favorable_excursion=?4, max_adverse_excursion=?5,
             r_result=?6, time_to_outcome_ms=?7
             WHERE signal_id=?1",
            params![
                signal_id,
                outcome,
                outcome_at_ms,
                mfe,
                mae,
                r_result,
                time_to
            ],
        )?;
        Ok(())
    }

    /// Resolve the most recent pending signal for a setup_id with a manual trade result.
    /// Used when record_trade_result is called with setup_id — bridges trades to signal_outcomes.
    pub fn resolve_pending_signal_by_setup_id(
        &self,
        setup_id: &str,
        result_r: f64,
        timestamp_ms: f64,
    ) -> Result<Option<String>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT signal_id, max_favorable_excursion, max_adverse_excursion
             FROM signal_outcomes
             WHERE setup_id = ?1 AND outcome = 'pending'
             ORDER BY fired_at_ms DESC LIMIT 1",
        )?;
        let mut rows = stmt.query(rusqlite::params![setup_id])?;
        if let Some(row) = rows.next()? {
            let signal_id: String = row.get(0)?;
            let mfe: f64 = row.get(1).unwrap_or(0.0);
            let mae: f64 = row.get(2).unwrap_or(0.0);
            let outcome = if result_r > 0.0 {
                "target_hit"
            } else {
                "stop_hit"
            };
            self.resolve_signal_outcome(
                &signal_id,
                outcome,
                timestamp_ms,
                mfe,
                mae,
                Some(result_r),
            )?;
            Ok(Some(signal_id))
        } else {
            Ok(None)
        }
    }

    /// Update MFE/MAE for a pending signal without resolving.
    pub fn update_signal_outcome_mfe_mae(
        &self,
        signal_id: &str,
        mfe: f64,
        mae: f64,
    ) -> Result<(), DbError> {
        self.conn.execute(
            "UPDATE signal_outcomes SET max_favorable_excursion=?2, max_adverse_excursion=?3
             WHERE signal_id=?1 AND outcome='pending'",
            rusqlite::params![signal_id, mfe, mae],
        )?;
        Ok(())
    }

    /// List pending signal outcomes (for the outcome evaluator to track).
    pub fn pending_signal_outcomes(&self) -> Result<Vec<SignalOutcome>, DbError> {
        self.pending_signal_outcomes_filtered(None, None)
    }

    pub fn pending_signal_outcomes_filtered(
        &self,
        source: Option<&str>,
        job_id: Option<&str>,
    ) -> Result<Vec<SignalOutcome>, DbError> {
        let mut sql = String::from(
            "SELECT signal_id, setup_id, setup_name, session_date, source, job_id,
                    fired_at_ms, fired_price, target_price, stop_price, outcome, outcome_at_ms,
                    max_favorable_excursion, max_adverse_excursion, r_result, time_to_outcome_ms,
                    rvol_at_fire, rvol_bucket_at_fire
             FROM signal_outcomes WHERE outcome = 'pending'",
        );
        let mut bind_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        if let Some(source) = source {
            sql.push_str(&format!(" AND source = ?{}", bind_values.len() + 1));
            bind_values.push(Box::new(source.to_string()));
        }
        if let Some(job_id) = job_id {
            sql.push_str(&format!(" AND job_id = ?{}", bind_values.len() + 1));
            bind_values.push(Box::new(job_id.to_string()));
        }
        let mut stmt = self.conn.prepare(&sql)?;
        let params_ref: Vec<&dyn rusqlite::types::ToSql> =
            bind_values.iter().map(|b| b.as_ref()).collect();
        let rows = stmt.query_map(params_ref.as_slice(), |row| {
            Ok(SignalOutcome {
                signal_id: row.get(0)?,
                setup_id: row.get(1)?,
                setup_name: row.get(2)?,
                session_date: row.get(3)?,
                root_symbol: None,
                contract_symbol: None,
                source: row.get(4)?,
                job_id: row.get(5)?,
                fired_at_ms: row.get(6)?,
                fired_price: row.get(7)?,
                target_price: row.get(8)?,
                stop_price: row.get(9)?,
                outcome: row.get(10)?,
                outcome_at_ms: row.get(11)?,
                max_favorable_excursion: row.get(12)?,
                max_adverse_excursion: row.get(13)?,
                r_result: row.get(14)?,
                time_to_outcome_ms: row.get(15)?,
                rvol_at_fire: row.get(16)?,
                rvol_bucket_at_fire: row.get(17)?,
            })
        })?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    /// List resolved signal outcomes with r_result for research queries.
    #[allow(clippy::type_complexity)]
    pub fn list_signal_outcomes_for_research(
        &self,
        setup_id: Option<&str>,
        start_date: Option<&str>,
        end_date: Option<&str>,
        scope: Option<&SessionScopeFilter>,
    ) -> Result<Vec<(String, String, Option<f64>, String)>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT setup_id, session_date, r_result, outcome, fired_at_ms, root_symbol, contract_symbol
             FROM signal_outcomes WHERE outcome != 'pending'",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<f64>>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, f64>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, Option<String>>(6)?,
            ))
        })?;
        let mut results = Vec::new();
        for row in rows.filter_map(|r| r.ok()) {
            let (sid, session_date, r_result, outcome, fired_at_ms, root_symbol, contract_symbol) =
                row;
            if let Some(filter_id) = setup_id {
                if sid != filter_id {
                    continue;
                }
            }
            if !contract_fields_match_scope(
                root_symbol.as_deref(),
                contract_symbol.as_deref(),
                scope,
            ) {
                continue;
            }
            let Some(analysis_day) = analysis_day_for_scope(&session_date, fired_at_ms, scope)
            else {
                continue;
            };
            if let Some(sd) = start_date {
                if analysis_day.as_str() < sd {
                    continue;
                }
            }
            if let Some(ed) = end_date {
                if analysis_day.as_str() > ed {
                    continue;
                }
            }
            results.push((sid, analysis_day, r_result, outcome));
        }
        Ok(results)
    }

    pub fn list_signal_outcomes_with_context(
        &self,
        setup_id: Option<&str>,
        start_date: Option<&str>,
        end_date: Option<&str>,
        scope: Option<&SessionScopeFilter>,
    ) -> Result<Vec<SignalOutcome>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT signal_id, setup_id, setup_name, session_date, root_symbol, contract_symbol,
                    source, job_id, fired_at_ms, fired_price, target_price, stop_price, outcome,
                    outcome_at_ms, max_favorable_excursion, max_adverse_excursion, r_result,
                    time_to_outcome_ms, rvol_at_fire, rvol_bucket_at_fire
             FROM signal_outcomes WHERE outcome != 'pending'",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(SignalOutcome {
                signal_id: row.get(0)?,
                setup_id: row.get(1)?,
                setup_name: row.get(2)?,
                session_date: row.get(3)?,
                root_symbol: row.get(4)?,
                contract_symbol: row.get(5)?,
                source: row.get(6)?,
                job_id: row.get(7)?,
                fired_at_ms: row.get(8)?,
                fired_price: row.get(9)?,
                target_price: row.get(10)?,
                stop_price: row.get(11)?,
                outcome: row.get(12)?,
                outcome_at_ms: row.get(13)?,
                max_favorable_excursion: row.get(14)?,
                max_adverse_excursion: row.get(15)?,
                r_result: row.get(16)?,
                time_to_outcome_ms: row.get(17)?,
                rvol_at_fire: row.get(18)?,
                rvol_bucket_at_fire: row.get(19)?,
            })
        })?;

        let mut out = Vec::new();
        for outcome in rows.filter_map(|r| r.ok()) {
            if let Some(filter_id) = setup_id {
                if outcome.setup_id != filter_id {
                    continue;
                }
            }
            if !contract_fields_match_scope(
                outcome.root_symbol.as_deref(),
                outcome.contract_symbol.as_deref(),
                scope,
            ) {
                continue;
            }
            let Some(analysis_day) =
                analysis_day_for_scope(&outcome.session_date, outcome.fired_at_ms, scope)
            else {
                continue;
            };
            if let Some(sd) = start_date {
                if analysis_day.as_str() < sd {
                    continue;
                }
            }
            if let Some(ed) = end_date {
                if analysis_day.as_str() > ed {
                    continue;
                }
            }
            out.push(outcome);
        }
        Ok(out)
    }

    /// List resolved signal outcomes with RVOL-at-fire context for regime analysis.
    /// Returns `(rvol_at_fire, r_result, outcome)` tuples, filtering to rows where
    /// `rvol_at_fire` is populated.
    pub fn list_signal_outcomes_with_rvol(
        &self,
        setup_id: Option<&str>,
        start_date: Option<&str>,
        end_date: Option<&str>,
        scope: Option<&SessionScopeFilter>,
    ) -> Result<Vec<(f64, Option<f64>, String)>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT setup_id, session_date, r_result, outcome, fired_at_ms, rvol_at_fire, root_symbol, contract_symbol
             FROM signal_outcomes
             WHERE outcome != 'pending' AND rvol_at_fire IS NOT NULL",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<f64>>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, f64>(4)?,
                row.get::<_, f64>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, Option<String>>(7)?,
            ))
        })?;
        let mut results = Vec::new();
        for row in rows.filter_map(|r| r.ok()) {
            let (
                sid,
                session_date,
                r_result,
                outcome,
                fired_at_ms,
                rvol,
                root_symbol,
                contract_symbol,
            ) = row;
            if let Some(filter_id) = setup_id {
                if sid != filter_id {
                    continue;
                }
            }
            if !contract_fields_match_scope(
                root_symbol.as_deref(),
                contract_symbol.as_deref(),
                scope,
            ) {
                continue;
            }
            let Some(analysis_day) = analysis_day_for_scope(&session_date, fired_at_ms, scope)
            else {
                continue;
            };
            if let Some(sd) = start_date {
                if analysis_day.as_str() < sd {
                    continue;
                }
            }
            if let Some(ed) = end_date {
                if analysis_day.as_str() > ed {
                    continue;
                }
            }
            results.push((rvol, r_result, outcome));
        }
        Ok(results)
    }

    /// List resolved signal outcomes with excursion fields for performance diagnostics.
    pub fn list_signal_outcomes_for_excursions_filtered(
        &self,
        setup_id: Option<&str>,
        start_date: Option<&str>,
        end_date: Option<&str>,
        scope: Option<&SessionScopeFilter>,
    ) -> Result<Vec<SignalOutcomeExcursionRow>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT setup_id, setup_name, session_date, fired_at_ms, outcome,
                    max_favorable_excursion, max_adverse_excursion, time_to_outcome_ms,
                    root_symbol, contract_symbol
             FROM signal_outcomes
             WHERE outcome != 'pending'",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, f64>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, Option<f64>>(5)?,
                row.get::<_, Option<f64>>(6)?,
                row.get::<_, Option<f64>>(7)?,
                row.get::<_, Option<String>>(8)?,
                row.get::<_, Option<String>>(9)?,
            ))
        })?;

        let mut results = Vec::new();
        for row in rows.filter_map(|r| r.ok()) {
            let (
                sid,
                setup_name,
                session_date,
                fired_at_ms,
                outcome,
                mfe,
                mae,
                tto_ms,
                root_symbol,
                contract_symbol,
            ) = row;
            if let Some(filter_id) = setup_id {
                if sid != filter_id {
                    continue;
                }
            }
            if !contract_fields_match_scope(
                root_symbol.as_deref(),
                contract_symbol.as_deref(),
                scope,
            ) {
                continue;
            }
            let Some(analysis_day) = analysis_day_for_scope(&session_date, fired_at_ms, scope)
            else {
                continue;
            };
            if let Some(sd) = start_date {
                if analysis_day.as_str() < sd {
                    continue;
                }
            }
            if let Some(ed) = end_date {
                if analysis_day.as_str() > ed {
                    continue;
                }
            }
            results.push(SignalOutcomeExcursionRow {
                setup_id: sid,
                setup_name,
                outcome,
                max_favorable_excursion: mfe,
                max_adverse_excursion: mae,
                time_to_outcome_ms: tto_ms,
                fired_at_ms,
            });
        }
        Ok(results)
    }

    /// Get signal performance stats for a setup.
    pub fn signal_performance(
        &self,
        setup_id: Option<&str>,
        start_date: Option<&str>,
        end_date: Option<&str>,
    ) -> Result<serde_json::Value, DbError> {
        self.signal_performance_filtered(setup_id, start_date, end_date, None, None, None)
    }

    pub fn signal_performance_filtered(
        &self,
        setup_id: Option<&str>,
        start_date: Option<&str>,
        end_date: Option<&str>,
        source: Option<&str>,
        job_id: Option<&str>,
        scope: Option<&SessionScopeFilter>,
    ) -> Result<serde_json::Value, DbError> {
        let mut conditions = Vec::new();
        let mut bind_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        if let Some(setup_id) = setup_id {
            conditions.push(format!("setup_id = ?{}", bind_values.len() + 1));
            bind_values.push(Box::new(setup_id.to_string()));
        }
        if let Some(source) = source {
            conditions.push(format!("source = ?{}", bind_values.len() + 1));
            bind_values.push(Box::new(source.to_string()));
        }
        if let Some(job_id) = job_id {
            conditions.push(format!("job_id = ?{}", bind_values.len() + 1));
            bind_values.push(Box::new(job_id.to_string()));
        }
        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };
        let sql = format!(
            "SELECT session_date, fired_at_ms, outcome, r_result, root_symbol, contract_symbol
             FROM signal_outcomes {where_clause}"
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let params_ref: Vec<&dyn rusqlite::types::ToSql> =
            bind_values.iter().map(|b| b.as_ref()).collect();
        let rows = stmt.query_map(params_ref.as_slice(), |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, f64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<f64>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
            ))
        })?;

        let mut total = 0_i64;
        let mut resolved = 0_i64;
        let mut pending = 0_i64;
        let mut target_hit = 0_i64;
        let mut stop_hit = 0_i64;
        let mut time_exit = 0_i64;
        let mut r_sum = 0.0_f64;
        let mut r_count = 0_i64;
        let mut winner_sum = 0.0_f64;
        let mut winner_count = 0_i64;
        let mut loser_sum = 0.0_f64;
        let mut loser_count = 0_i64;

        for row in rows.filter_map(|r| r.ok()) {
            let (session_date, fired_at_ms, outcome, r_result, root_symbol, contract_symbol) = row;
            if !contract_fields_match_scope(
                root_symbol.as_deref(),
                contract_symbol.as_deref(),
                scope,
            ) {
                continue;
            }
            let Some(analysis_day) = analysis_day_for_scope(&session_date, fired_at_ms, scope)
            else {
                continue;
            };
            if let Some(sd) = start_date {
                if analysis_day.as_str() < sd {
                    continue;
                }
            }
            if let Some(ed) = end_date {
                if analysis_day.as_str() > ed {
                    continue;
                }
            }
            total += 1;
            if outcome != "pending" {
                resolved += 1;
            } else {
                pending += 1;
            }
            if outcome == "target_hit" {
                target_hit += 1;
            } else if outcome == "stop_hit" {
                stop_hit += 1;
            } else if outcome == "time_exit" {
                time_exit += 1;
            }
            if let Some(r) = r_result {
                r_sum += r;
                r_count += 1;
                if r > 0.0 {
                    winner_sum += r;
                    winner_count += 1;
                } else if r < 0.0 {
                    loser_sum += r;
                    loser_count += 1;
                }
            }
        }

        let avg_r = if r_count > 0 {
            r_sum / r_count as f64
        } else {
            0.0
        };
        let avg_winner_r = if winner_count > 0 {
            Some(winner_sum / winner_count as f64)
        } else {
            None
        };
        let avg_loser_r = if loser_count > 0 {
            Some(loser_sum / loser_count as f64)
        } else {
            None
        };

        let mut result = serde_json::json!({
            "totalSignals": total,
            "resolved": resolved,
            "pending": pending,
            "targetHit": target_hit,
            "stopHit": stop_hit,
            "timeExit": time_exit,
            "winRate": if resolved > 0 { target_hit as f64 / resolved as f64 } else { 0.0 },
            "avgR": avg_r,
            "avgWinnerR": avg_winner_r,
            "avgLoserR": avg_loser_r,
        });
        if let Some(setup_id) = setup_id {
            result["setupId"] = serde_json::json!(setup_id);
        }
        if let Some(source) = source {
            result["source"] = serde_json::json!(source);
        }
        if let Some(job_id) = job_id {
            result["jobId"] = serde_json::json!(job_id);
        }
        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn setup_performance_matrix_filtered(
        &self,
        start_date: Option<&str>,
        end_date: Option<&str>,
        source: Option<&str>,
        job_id: Option<&str>,
        scope: Option<&SessionScopeFilter>,
        min_resolved: i64,
        sort_by: SetupPerformanceSortBy,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>, DbError> {
        #[derive(Default)]
        struct SetupPerfAgg {
            setup_name: Option<String>,
            total: i64,
            resolved: i64,
            pending: i64,
            target_hit: i64,
            stop_hit: i64,
            time_exit: i64,
            r_sum: f64,
            r_count: i64,
            winner_sum: f64,
            winner_count: i64,
            loser_sum: f64,
            loser_count: i64,
        }

        let mut sql = String::from(
            "SELECT setup_id, setup_name, session_date, fired_at_ms, outcome, r_result, root_symbol, contract_symbol
             FROM signal_outcomes WHERE 1=1",
        );
        let mut bind_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        if let Some(source) = source {
            sql.push_str(&format!(" AND source = ?{}", bind_values.len() + 1));
            bind_values.push(Box::new(source.to_string()));
        }
        if let Some(job_id) = job_id {
            sql.push_str(&format!(" AND job_id = ?{}", bind_values.len() + 1));
            bind_values.push(Box::new(job_id.to_string()));
        }
        let mut stmt = self.conn.prepare(&sql)?;
        let params_ref: Vec<&dyn rusqlite::types::ToSql> =
            bind_values.iter().map(|b| b.as_ref()).collect();
        let rows = stmt.query_map(params_ref.as_slice(), |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, f64>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, Option<f64>>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, Option<String>>(7)?,
            ))
        })?;

        let mut grouped: BTreeMap<String, SetupPerfAgg> = BTreeMap::new();
        for row in rows.filter_map(|r| r.ok()) {
            let (
                setup_id,
                setup_name,
                session_date,
                fired_at_ms,
                outcome,
                r_result,
                root_symbol,
                contract_symbol,
            ) = row;
            if !contract_fields_match_scope(
                root_symbol.as_deref(),
                contract_symbol.as_deref(),
                scope,
            ) {
                continue;
            }
            let Some(analysis_day) = analysis_day_for_scope(&session_date, fired_at_ms, scope)
            else {
                continue;
            };
            if let Some(sd) = start_date {
                if analysis_day.as_str() < sd {
                    continue;
                }
            }
            if let Some(ed) = end_date {
                if analysis_day.as_str() > ed {
                    continue;
                }
            }

            let agg = grouped.entry(setup_id).or_default();
            if agg.setup_name.is_none() {
                agg.setup_name = setup_name;
            }
            agg.total += 1;
            if outcome == "pending" {
                agg.pending += 1;
            } else {
                agg.resolved += 1;
            }
            if outcome == "target_hit" {
                agg.target_hit += 1;
            } else if outcome == "stop_hit" {
                agg.stop_hit += 1;
            } else if outcome == "time_exit" {
                agg.time_exit += 1;
            }
            if let Some(r) = r_result {
                agg.r_sum += r;
                agg.r_count += 1;
                if r > 0.0 {
                    agg.winner_sum += r;
                    agg.winner_count += 1;
                } else if r < 0.0 {
                    agg.loser_sum += r;
                    agg.loser_count += 1;
                }
            }
        }

        let mut rows: Vec<serde_json::Value> = grouped
            .into_iter()
            .filter_map(|(setup_id, agg)| {
                if agg.resolved < min_resolved {
                    return None;
                }
                let win_rate = if agg.resolved > 0 {
                    agg.target_hit as f64 / agg.resolved as f64
                } else {
                    0.0
                };
                let avg_r = if agg.r_count > 0 {
                    agg.r_sum / agg.r_count as f64
                } else {
                    0.0
                };
                let avg_winner_r = if agg.winner_count > 0 {
                    Some(agg.winner_sum / agg.winner_count as f64)
                } else {
                    None
                };
                let avg_loser_r = if agg.loser_count > 0 {
                    Some(agg.loser_sum / agg.loser_count as f64)
                } else {
                    None
                };
                Some(serde_json::json!({
                    "setupId": setup_id,
                    "setupName": agg.setup_name,
                    "totalSignals": agg.total,
                    "resolved": agg.resolved,
                    "pending": agg.pending,
                    "targetHit": agg.target_hit,
                    "stopHit": agg.stop_hit,
                    "timeExit": agg.time_exit,
                    "winRate": win_rate,
                    "avgR": avg_r,
                    "avgWinnerR": avg_winner_r,
                    "avgLoserR": avg_loser_r,
                }))
            })
            .collect();

        rows.sort_by(|a, b| {
            let ord = match sort_by {
                SetupPerformanceSortBy::WinRate => b
                    .get("winRate")
                    .and_then(|v| v.as_f64())
                    .partial_cmp(&a.get("winRate").and_then(|v| v.as_f64()))
                    .unwrap_or(std::cmp::Ordering::Equal),
                SetupPerformanceSortBy::AvgR => b
                    .get("avgR")
                    .and_then(|v| v.as_f64())
                    .partial_cmp(&a.get("avgR").and_then(|v| v.as_f64()))
                    .unwrap_or(std::cmp::Ordering::Equal),
                SetupPerformanceSortBy::Resolved => b
                    .get("resolved")
                    .and_then(|v| v.as_i64())
                    .cmp(&a.get("resolved").and_then(|v| v.as_i64())),
                SetupPerformanceSortBy::TotalSignals => b
                    .get("totalSignals")
                    .and_then(|v| v.as_i64())
                    .cmp(&a.get("totalSignals").and_then(|v| v.as_i64())),
            };
            if ord == std::cmp::Ordering::Equal {
                a.get("setupId")
                    .and_then(|v| v.as_str())
                    .cmp(&b.get("setupId").and_then(|v| v.as_str()))
            } else {
                ord
            }
        });

        if rows.len() > limit {
            rows.truncate(limit);
        }
        Ok(rows)
    }

    /// Count playbook signals fired between `start_ms` and `end_ms` (inclusive
    /// of `start`, exclusive of `end`). Used at RTH-close finalization to
    /// populate `session_summaries.signal_count` from the same source the live
    /// ingest path already wrote into.
    pub fn count_playbook_signals_in_range(
        &self,
        start_ms: f64,
        end_ms: f64,
    ) -> Result<i64, DbError> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(1) FROM playbook_signals
             WHERE timestamp_ms >= ?1 AND timestamp_ms < ?2",
            params![start_ms, end_ms],
            |r| r.get(0),
        )?;
        Ok(count)
    }

    /// Atomically persist the session_summaries row and the prior_day_levels
    /// carry-forward for a just-closed RTH (or other) session inside a single
    /// `BEGIN IMMEDIATE` transaction. This is the live counterpart of
    /// `persist_historical_session` for the boundary-driven path that runs
    /// inside the MCP ingest loop.
    ///
    /// `prior_day` packs `(high, low, close, va_high, va_low, poc, dnva_high,
    /// dnva_low, dnp)` for the carry-forward row. Both writes commit together
    /// or roll back together so a crash mid-write cannot leave the next
    /// session reading half-updated levels.
    pub fn persist_live_session_close(
        &self,
        summary: &SessionSummary,
        prior_day: (f64, f64, f64, f64, f64, f64, f64, f64, f64),
    ) -> Result<(), DbError> {
        self.conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;
        let result = (|| -> Result<(), DbError> {
            self.upsert_session_summary(summary)?;
            self.save_prior_day_full_with_dnva_contract(
                &summary.session_date,
                prior_day.0,
                prior_day.1,
                prior_day.2,
                prior_day.3,
                prior_day.4,
                prior_day.5,
                Some(prior_day.6),
                Some(prior_day.7),
                Some(prior_day.8),
                Some(summary.root_symbol.as_str()),
                Some(summary.contract_symbol.as_str()),
            )?;
            // Mirror persist_historical_session: track untested DNPs so the
            // research layer stays consistent regardless of whether the
            // session was closed live or via backfill.
            const DNP_TOLERANCE: f64 = 0.5;
            if summary.low > 0.0 || summary.high > 0.0 {
                self.delete_untested_dnps_touched_by_range(
                    summary.low,
                    summary.high,
                    DNP_TOLERANCE,
                    Some((&summary.session_date, &summary.session_type)),
                )?;
            }
            if summary.dnp > 0.0 {
                let dnp_tested = (summary.low <= summary.dnp + DNP_TOLERANCE)
                    && (summary.high >= summary.dnp - DNP_TOLERANCE);
                if dnp_tested {
                    self.delete_untested_dnp_for_session(
                        &summary.session_date,
                        &summary.session_type,
                    )?;
                } else {
                    self.save_untested_dnp(
                        &summary.session_date,
                        &summary.session_type,
                        summary.dnp,
                    )?;
                }
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn persist_historical_session(
        &self,
        session_date: &str,
        force: bool,
        purge_sources: &[&str],
        summary: &SessionSummary,
        events: &[MarketEvent],
        replay_signals: &[ReplaySignalRecord],
        signal_outcomes: &[SignalOutcome],
        prior_day: (f64, f64, f64, f64, f64, f64, f64, f64, f64),
    ) -> Result<(), DbError> {
        self.conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;
        let result = (|| -> Result<(), DbError> {
            if force {
                self.purge_historical_session(session_date, purge_sources)?;
            }
            self.upsert_session_summary(summary)?;
            if !events.is_empty() {
                let mut stmt = self.conn.prepare_cached(
                    "INSERT OR IGNORE INTO market_events
                     (session_date, timestamp_ms, event_type, level_name, price, direction, sequence_num, metadata_json,
                      session_type, session_segment, trading_day)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                )?;
                for e in events {
                    let meta = e
                        .metadata
                        .as_ref()
                        .map(|m| serde_json::to_string(m).unwrap_or_default());
                    stmt.execute(params![
                        e.session_date,
                        e.timestamp_ms,
                        e.event_type,
                        e.level_name,
                        e.price,
                        e.direction,
                        e.sequence_num,
                        meta,
                        &e.session_type,
                        &e.session_segment,
                        &e.trading_day,
                    ])?;
                }
            }
            for signal in replay_signals {
                self.insert_playbook_signal_record(signal)?;
            }
            for outcome in signal_outcomes {
                self.insert_signal_outcome(outcome)?;
            }
            self.save_prior_day_full_with_dnva_contract(
                session_date,
                prior_day.0,
                prior_day.1,
                prior_day.2,
                prior_day.3,
                prior_day.4,
                prior_day.5,
                Some(prior_day.6),
                Some(prior_day.7),
                Some(prior_day.8),
                Some(summary.root_symbol.as_str()),
                Some(summary.contract_symbol.as_str()),
            )?;
            // Track untested DNPs: price did not revisit DNP ± 2 NQ ticks (0.5 pts).
            const DNP_TOLERANCE: f64 = 0.5;
            if summary.low > 0.0 || summary.high > 0.0 {
                self.delete_untested_dnps_touched_by_range(
                    summary.low,
                    summary.high,
                    DNP_TOLERANCE,
                    Some((session_date, &summary.session_type)),
                )?;
            }
            if summary.dnp > 0.0 {
                let dnp_tested = (summary.low <= summary.dnp + DNP_TOLERANCE)
                    && (summary.high >= summary.dnp - DNP_TOLERANCE);
                if dnp_tested {
                    self.delete_untested_dnp_for_session(session_date, &summary.session_type)?;
                } else {
                    self.save_untested_dnp(session_date, &summary.session_type, summary.dnp)?;
                }
            }
            Ok(())
        })();

        match result {
            Ok(()) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    /// Count of session summaries in the database.
    pub fn session_summary_count(&self) -> Result<i64, DbError> {
        Ok(self
            .conn
            .query_row("SELECT COUNT(1) FROM session_summaries", [], |r| r.get(0))?)
    }

    /// Earliest and latest session_date in session_summaries (for coverage reporting).
    pub fn session_summary_date_range(&self) -> Result<(Option<String>, Option<String>), DbError> {
        let min_date =
            self.conn
                .query_row("SELECT MIN(session_date) FROM session_summaries", [], |r| {
                    r.get::<_, Option<String>>(0)
                })?;
        let max_date =
            self.conn
                .query_row("SELECT MAX(session_date) FROM session_summaries", [], |r| {
                    r.get::<_, Option<String>>(0)
                })?;
        Ok((min_date, max_date))
    }

    pub fn insert_historical_job_run(&self, run: &HistoricalJobRun) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO historical_job_runs
             (id, job_type, status, params_json, progress_json, result_json, warning_json, error_text,
              submitted_at_ms, started_at_ms, finished_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                run.id,
                run.job_type,
                run.status,
                serde_json::to_string(&run.params)?,
                serde_json::to_string(&run.progress)?,
                run.result.as_ref().map(serde_json::to_string).transpose()?,
                serde_json::to_string(&run.warnings)?,
                run.error,
                run.submitted_at_ms,
                run.started_at_ms,
                run.finished_at_ms
            ],
        )?;
        Ok(())
    }

    pub fn update_historical_job_run(
        &self,
        id: &str,
        update: &HistoricalJobRunUpdate<'_>,
    ) -> Result<(), DbError> {
        self.conn.execute(
            "UPDATE historical_job_runs
             SET status = ?2,
                 progress_json = ?3,
                 result_json = ?4,
                 warning_json = ?5,
                 error_text = ?6,
                 started_at_ms = COALESCE(?7, started_at_ms),
                 finished_at_ms = ?8
             WHERE id = ?1",
            params![
                id,
                update.status,
                serde_json::to_string(update.progress)?,
                update.result.map(serde_json::to_string).transpose()?,
                serde_json::to_string(update.warnings)?,
                update.error,
                update.started_at_ms,
                update.finished_at_ms
            ],
        )?;
        Ok(())
    }

    pub fn get_historical_job_run(&self, id: &str) -> Result<Option<HistoricalJobRun>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, job_type, status, params_json, progress_json, result_json, warning_json,
                    error_text, submitted_at_ms, started_at_ms, finished_at_ms
             FROM historical_job_runs WHERE id = ?1",
        )?;
        let mut rows = stmt.query(params![id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(HistoricalJobRun {
                id: row.get(0)?,
                job_type: row.get(1)?,
                status: row.get(2)?,
                params: serde_json::from_str(&row.get::<_, String>(3)?).unwrap_or_default(),
                progress: serde_json::from_str(&row.get::<_, String>(4)?).unwrap_or_default(),
                result: row
                    .get::<_, Option<String>>(5)?
                    .and_then(|s| serde_json::from_str(&s).ok()),
                warnings: row
                    .get::<_, Option<String>>(6)?
                    .and_then(|s| serde_json::from_str(&s).ok())
                    .unwrap_or_default(),
                error: row.get(7)?,
                submitted_at_ms: row.get(8)?,
                started_at_ms: row.get(9)?,
                finished_at_ms: row.get(10)?,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn latest_historical_job_run(&self) -> Result<Option<HistoricalJobRun>, DbError> {
        let mut stmt = self
            .conn
            .prepare("SELECT id FROM historical_job_runs ORDER BY submitted_at_ms DESC LIMIT 1")?;
        let mut rows = stmt.query([])?;
        if let Some(row) = rows.next()? {
            let id: String = row.get(0)?;
            self.get_historical_job_run(&id)
        } else {
            Ok(None)
        }
    }

    // ------------------------------------------------------------------
    // Backtest runs
    // ------------------------------------------------------------------

    /// Insert a backtest run record.
    pub fn insert_backtest_run(
        &self,
        id: &str,
        created_at_ms: f64,
        params: &serde_json::Value,
        metrics: &serde_json::Value,
        trades: &serde_json::Value,
    ) -> Result<(), DbError> {
        self.conn.execute(
            "INSERT INTO backtest_runs (id, created_at_ms, params, metrics, trades)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                id,
                created_at_ms,
                serde_json::to_string(params).unwrap_or_default(),
                serde_json::to_string(metrics).unwrap_or_default(),
                serde_json::to_string(trades).unwrap_or_default(),
            ],
        )?;
        Ok(())
    }

    /// List backtest runs, most recent first.
    pub fn list_backtest_runs(&self, limit: usize) -> Result<Vec<serde_json::Value>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, created_at_ms, params, metrics, trades
             FROM backtest_runs ORDER BY created_at_ms DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map([limit as i64], |row| {
            let id: String = row.get(0)?;
            let created_at_ms: f64 = row.get(1)?;
            let params_str: String = row.get(2)?;
            let metrics_str: String = row.get(3)?;
            let trades_str: String = row.get(4)?;
            Ok(serde_json::json!({
                "id": id,
                "createdAtMs": created_at_ms,
                "params": serde_json::from_str::<serde_json::Value>(&params_str).unwrap_or_default(),
                "metrics": serde_json::from_str::<serde_json::Value>(&metrics_str).unwrap_or_default(),
                "trades": serde_json::from_str::<serde_json::Value>(&trades_str).unwrap_or_default(),
            }))
        })?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    /// Get a single backtest run by ID.
    pub fn get_backtest_run(&self, id: &str) -> Result<Option<serde_json::Value>, DbError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, created_at_ms, params, metrics, trades
             FROM backtest_runs WHERE id = ?1",
        )?;
        let mut rows = stmt.query(rusqlite::params![id])?;
        if let Some(row) = rows.next()? {
            let id: String = row.get(0)?;
            let created_at_ms: f64 = row.get(1)?;
            let params_str: String = row.get(2)?;
            let metrics_str: String = row.get(3)?;
            let trades_str: String = row.get(4)?;
            Ok(Some(serde_json::json!({
                "id": id,
                "createdAtMs": created_at_ms,
                "params": serde_json::from_str::<serde_json::Value>(&params_str).unwrap_or_default(),
                "metrics": serde_json::from_str::<serde_json::Value>(&metrics_str).unwrap_or_default(),
                "trades": serde_json::from_str::<serde_json::Value>(&trades_str).unwrap_or_default(),
            })))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use chrono_tz::US::Eastern;
    use tempfile::NamedTempFile;

    fn test_db() -> Database {
        let file = NamedTempFile::new().expect("temp");
        Database::open(file.path().to_string_lossy().as_ref()).expect("open")
    }

    #[test]
    fn inserts_and_lists_setups() {
        let db = test_db();
        db.upsert_setup(&SetupDefinition {
            id: "s1".to_string(),
            name: "DNVA Reversion".to_string(),
            description: "Mean reversion at DNVA".to_string(),
            active: true,
            conditions: vec!["price_at_dnva".to_string()],
            min_delta: 0.0,
            require_above_vwap: false,
            duplicate_suppression_ms: 5000,
            ..Default::default()
        })
        .expect("insert");
        let setups = db.list_setups().expect("list");
        assert_eq!(setups.len(), 1);
    }

    #[test]
    fn toggle_and_delete_setup() {
        let db = test_db();
        db.upsert_setup(&SetupDefinition {
            id: "s1".to_string(),
            name: "Test".to_string(),
            active: true,
            ..Default::default()
        })
        .expect("insert");
        db.toggle_setup("s1", false).expect("toggle");
        let setups = db.list_setups().expect("list");
        assert!(!setups[0].active);
        db.delete_setup("s1").expect("delete");
        assert!(db.list_setups().expect("list").is_empty());
    }

    #[test]
    fn active_setup_queries_filter_and_lookup_specific_rows() {
        let db = test_db();
        db.upsert_setup(&SetupDefinition {
            id: "active_setup".to_string(),
            name: "Active".to_string(),
            active: true,
            ..Default::default()
        })
        .expect("insert active setup");
        db.upsert_setup(&SetupDefinition {
            id: "inactive_setup".to_string(),
            name: "Inactive".to_string(),
            active: false,
            ..Default::default()
        })
        .expect("insert inactive setup");

        let active = db.list_active_setups().expect("list active");
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].id, "active_setup");

        let inactive = db
            .get_setup("inactive_setup")
            .expect("get setup")
            .expect("inactive row exists");
        assert_eq!(inactive.id, "inactive_setup");
        assert!(!inactive.active);
    }

    #[test]
    fn playbook_runtime_seed_filters_inactive_and_loads_risk_gate() {
        let db = test_db();
        db.upsert_setup(&SetupDefinition {
            id: "active_setup".to_string(),
            name: "Active".to_string(),
            active: true,
            ..Default::default()
        })
        .expect("insert active setup");
        db.upsert_setup(&SetupDefinition {
            id: "inactive_setup".to_string(),
            name: "Inactive".to_string(),
            active: false,
            ..Default::default()
        })
        .expect("insert inactive setup");
        db.save_risk_state(&RiskState {
            at_limit: true,
            ..Default::default()
        })
        .expect("save risk state");

        let (setups, risk_at_limit) = db.load_playbook_runtime_seed().expect("runtime seed");
        assert_eq!(setups.len(), 1);
        assert_eq!(setups[0].id, "active_setup");
        assert!(risk_at_limit);
    }

    #[test]
    fn session_lifecycle() {
        let db = test_db();
        let session = SessionRecord {
            id: "sess1".into(),
            date: "2026-02-25".into(),
            session_type: "rth".into(),
            start_time: 1740000000.0,
            end_time: None,
            recording_path: None,
            pre_session_note: Some("Focus on VWAP plays".into()),
        };
        db.create_session(&session).expect("create");
        db.update_session_end("sess1", 1740030000.0, Some("/path/rec.desk"))
            .expect("update");
        let loaded = db.get_session("sess1").expect("get").expect("exists");
        assert!(loaded.end_time.is_some());
        assert_eq!(loaded.pre_session_note.unwrap(), "Focus on VWAP plays");
    }

    #[test]
    fn trade_lifecycle() {
        let db = test_db();
        db.create_session(&SessionRecord {
            id: "sess1".into(),
            date: "2026-02-25".into(),
            session_type: "rth".into(),
            start_time: 1740000000.0,
            end_time: None,
            recording_path: None,
            pre_session_note: None,
        })
        .expect("session");
        let trade = TradeRecord {
            id: "t1".into(),
            session_id: Some("sess1".into()),
            setup_id: None,
            instrument: Some("MNQ".into()),
            trade_account: Some("SIM".into()),
            entry_time: 1740001000.0,
            entry_price: 21000.0,
            exit_time: None,
            exit_price: None,
            direction: "long".into(),
            size: 1,
            max_open_size: Some(1),
            stop_price: Some(20990.0),
            target_prices: vec![21020.0, 21040.0],
            result_r: None,
            gross_points: None,
            planned: true,
            rules_followed: None,
            emotional_state: None,
            thesis: Some("VWAP reclaim".into()),
            review_tags: vec!["planned".into()],
            mistake_tags: Vec::new(),
            entry_fill_count: 1,
            exit_fill_count: 0,
            import_batch_id: None,
            notes: String::new(),
            source: "manual".into(),
        };
        db.insert_trade(&trade).expect("insert");
        let open = db.get_open_trade("sess1").expect("open").expect("exists");
        assert_eq!(open.entry_price, 21000.0);
        db.update_trade_exit("t1", 1740002000.0, 21020.0, 2.0)
            .expect("exit");
        assert!(db.get_open_trade("sess1").expect("open").is_none());
    }

    #[test]
    fn risk_config_persistence() {
        let db = test_db();
        let config = RiskConfigRecord {
            max_daily_loss_r: 5.0,
            ..Default::default()
        };
        db.save_risk_config(&config).expect("save");
        let loaded = db.load_risk_config().expect("load");
        assert_eq!(loaded.max_daily_loss_r, 5.0);
    }

    #[test]
    fn saves_and_loads_prior_day_levels() {
        let db = test_db();
        db.save_prior_day("2026-02-24", 21100.0, 20900.0, 21050.0)
            .expect("save");
        let result = db.load_prior_day("2026-02-25").expect("load");
        assert!(result.is_some());
        let (h, l, c) = result.unwrap();
        assert_eq!(h, 21100.0);
        assert_eq!(l, 20900.0);
        assert_eq!(c, 21050.0);
    }

    #[test]
    fn prior_day_full_with_va_poc() {
        let db = test_db();
        db.save_prior_day_full(
            "2026-02-24",
            21100.0,
            20900.0,
            21050.0,
            21080.0,
            20950.0,
            21020.0,
        )
        .expect("save");
        let result = db
            .load_prior_day_full("2026-02-25")
            .expect("load")
            .expect("exists");
        assert_eq!(result.3, Some(21080.0));
        assert_eq!(result.4, Some(20950.0));
        assert_eq!(result.5, Some(21020.0));
    }

    #[test]
    fn save_prior_day_full_preserves_existing_dnva_columns() {
        let db = test_db();
        db.save_prior_day_full_with_dnva(
            "2026-02-24",
            21100.0,
            20900.0,
            21050.0,
            21080.0,
            20950.0,
            21020.0,
            Some(21070.0),
            Some(20970.0),
            Some(21020.0),
        )
        .expect("save full dnva");
        db.save_prior_day_full(
            "2026-02-24",
            21110.0,
            20910.0,
            21060.0,
            21090.0,
            20960.0,
            21030.0,
        )
        .expect("save legacy");

        let result = db
            .load_prior_day_full("2026-02-25")
            .expect("load")
            .expect("exists");
        assert_eq!(result.0, 21110.0);
        assert_eq!(result.1, 20910.0);
        assert_eq!(result.2, 21060.0);
        assert_eq!(result.6, Some(21070.0));
        assert_eq!(result.7, Some(20970.0));
        assert_eq!(result.8, Some(21020.0));
    }

    #[test]
    fn untested_dnp_insert_delete_for_session_roundtrip() {
        let db = test_db();
        db.save_untested_dnp("2026-03-04", "RTH", 21000.0)
            .expect("insert");
        let entries = db.load_untested_dnps(10).expect("load");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, "2026-03-04");
        assert_eq!(entries[0].1, "RTH");
        assert_eq!(entries[0].2, 21000.0);

        db.delete_untested_dnp_for_session("2026-03-04", "RTH")
            .expect("delete");
        let entries = db.load_untested_dnps(10).expect("load after delete");
        assert!(entries.is_empty());
    }

    #[test]
    fn delete_untested_dnps_touched_by_range_removes_matching_levels() {
        let db = test_db();
        db.save_untested_dnp("2026-03-01", "Asia", 20990.0)
            .expect("insert a");
        db.save_untested_dnp("2026-03-02", "London", 21010.0)
            .expect("insert b");
        db.save_untested_dnp("2026-03-03", "RTH", 21025.0)
            .expect("insert c");

        let removed = db
            .delete_untested_dnps_touched_by_range(21005.0, 21015.0, 0.5, None)
            .expect("delete by range");
        assert_eq!(removed, 1);

        let mut dnps: Vec<f64> = db
            .load_untested_dnps(10)
            .expect("load")
            .into_iter()
            .map(|(_, _, dnp)| dnp)
            .collect();
        dnps.sort_by(|a, b| a.partial_cmp(b).expect("cmp"));
        assert_eq!(dnps, vec![20990.0, 21025.0]);
    }

    #[test]
    fn journal_entry_lifecycle() {
        let db = test_db();
        db.create_session(&SessionRecord {
            id: "sess1".into(),
            date: "2026-02-25".into(),
            session_type: "rth".into(),
            start_time: 1740000000.0,
            end_time: None,
            recording_path: None,
            pre_session_note: None,
        })
        .expect("session");
        let entry = JournalEntry {
            id: "j1".into(),
            session_id: Some("sess1".into()),
            date: "2026-02-25".into(),
            content: "Good session".into(),
            tags: vec!["discipline".into()],
            setup_references: vec![],
            trade_references: vec![],
            created_at: 1740030000.0,
        };
        db.insert_journal_entry(&entry).expect("insert");
        let entries = db.get_journal_for_session("sess1").expect("get");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].content, "Good session");
    }

    #[test]
    fn purge_historical_session_preserves_live_signal_rows() {
        let db = test_db();
        db.upsert_session_summary(&SessionSummary {
            session_date: "2026-03-02".into(),
            session_type: "RTH".into(),
            root_symbol: "NQ".into(),
            contract_symbol: "NQH26.CME".into(),
            contract_month: Some("2026-03".into()),
            symbol_resolution_mode: "hybrid".into(),
            carry_forward_levels_valid: true,
            rollover_warning: None,
            open_price: 21000.0,
            high: 21010.0,
            low: 20990.0,
            close: 21005.0,
            poc: 21002.0,
            vah: 21006.0,
            val: 20998.0,
            ib_high: 21007.0,
            ib_low: 20997.0,
            ib_range: 10.0,
            ib_mid: 21002.0,
            or_high: 21004.0,
            or_low: 20999.0,
            day_type: "Normal".into(),
            profile_shape: "DShape".into(),
            balance_state: "Balanced".into(),
            total_volume: 1000.0,
            tick_count: 100,
            session_delta: 10.0,
            cumulative_delta: 15.0,
            dnp: 21001.0,
            dnva_high: 21003.0,
            dnva_low: 20999.0,
            vwap_close: 21002.0,
            signal_count: 0,
            single_prints_direction: "None".into(),
            excess_high: false,
            excess_low: false,
            poor_high: false,
            poor_low: false,
            rvol_ratio: 1.0,
            close_vs_ib_mid: "above".into(),
            close_vs_vwap: "above".into(),
            close_vs_poc: "above".into(),
            snapshot_json: None,
        })
        .expect("summary");
        db.insert_market_events_batch(&[MarketEvent {
            session_date: "2026-03-02".into(),
            timestamp_ms: 1.0,
            event_type: "test".into(),
            level_name: None,
            price: 21000.0,
            direction: None,
            sequence_num: None,
            metadata: None,
            session_type: "RTH".into(),
            session_segment: "None".into(),
            trading_day: "2026-03-02".into(),
        }])
        .expect("event");
        db.insert_playbook_signal_record(&ReplaySignalRecord {
            signal_id: "live-1".into(),
            timestamp_ms: 1.0,
            session_date: "2026-03-02".into(),
            root_symbol: Some("NQ".into()),
            contract_symbol: Some("NQH26.CME".into()),
            setup_id: "setup".into(),
            payload: serde_json::json!({}),
            source: "live".into(),
            job_id: None,
        })
        .expect("live signal");
        db.insert_playbook_signal_record(&ReplaySignalRecord {
            signal_id: "backfill-1".into(),
            timestamp_ms: 2.0,
            session_date: "2026-03-02".into(),
            root_symbol: Some("NQ".into()),
            contract_symbol: Some("NQH26.CME".into()),
            setup_id: "setup".into(),
            payload: serde_json::json!({}),
            source: "backfill".into(),
            job_id: Some("job-1".into()),
        })
        .expect("backfill signal");
        db.insert_signal_outcome(&SignalOutcome {
            signal_id: "live-outcome".into(),
            setup_id: "setup".into(),
            setup_name: Some("Setup".into()),
            session_date: "2026-03-02".into(),
            root_symbol: Some("NQ".into()),
            contract_symbol: Some("NQH26.CME".into()),
            source: "live".into(),
            job_id: None,
            fired_at_ms: 1.0,
            fired_price: 21000.0,
            target_price: Some(21010.0),
            stop_price: Some(20990.0),
            outcome: "pending".into(),
            outcome_at_ms: None,
            max_favorable_excursion: None,
            max_adverse_excursion: None,
            r_result: None,
            time_to_outcome_ms: None,
            rvol_at_fire: None,
            rvol_bucket_at_fire: None,
        })
        .expect("live outcome");
        db.insert_signal_outcome(&SignalOutcome {
            signal_id: "backfill-outcome".into(),
            setup_id: "setup".into(),
            setup_name: Some("Setup".into()),
            session_date: "2026-03-02".into(),
            root_symbol: Some("NQ".into()),
            contract_symbol: Some("NQH26.CME".into()),
            source: "backfill".into(),
            job_id: Some("job-1".into()),
            fired_at_ms: 2.0,
            fired_price: 21000.0,
            target_price: Some(21010.0),
            stop_price: Some(20990.0),
            outcome: "pending".into(),
            outcome_at_ms: None,
            max_favorable_excursion: None,
            max_adverse_excursion: None,
            r_result: None,
            time_to_outcome_ms: None,
            rvol_at_fire: None,
            rvol_bucket_at_fire: None,
        })
        .expect("backfill outcome");

        db.purge_historical_session("2026-03-02", &["backfill"])
            .expect("purge");

        assert_eq!(db.session_summary_count().expect("count"), 0);
        assert_eq!(db.count_playbook_signals().expect("signal count"), 1);
        assert_eq!(
            db.pending_signal_outcomes_filtered(Some("live"), None)
                .expect("live pending")
                .len(),
            1
        );
    }

    #[test]
    fn historical_job_run_roundtrip() {
        let db = test_db();
        let run = HistoricalJobRun {
            id: "job-1".into(),
            job_type: "research_backfill".into(),
            status: "queued".into(),
            params: serde_json::json!({"startDate":"2026-03-01"}),
            progress: serde_json::json!({"currentPhase":"queued"}),
            result: None,
            warnings: Vec::new(),
            error: None,
            submitted_at_ms: 1.0,
            started_at_ms: None,
            finished_at_ms: None,
        };
        db.insert_historical_job_run(&run).expect("insert");
        db.update_historical_job_run(
            "job-1",
            &HistoricalJobRunUpdate {
                status: "completed",
                progress: &serde_json::json!({"currentPhase":"finalizing"}),
                result: Some(&serde_json::json!({"sessionsProcessed":1})),
                warnings: &["warning".into()],
                error: None,
                started_at_ms: Some(2.0),
                finished_at_ms: Some(3.0),
            },
        )
        .expect("update");
        let loaded = db
            .get_historical_job_run("job-1")
            .expect("load")
            .expect("exists");
        assert_eq!(loaded.status, "completed");
        assert_eq!(loaded.warnings.len(), 1);
        assert_eq!(loaded.result.expect("result")["sessionsProcessed"], 1);
    }

    #[test]
    fn market_event_rows_store_session_context() {
        let db = test_db();
        db.insert_market_events_batch(&[MarketEvent {
            session_date: "2026-03-03".into(),
            timestamp_ms: 1.0,
            event_type: "context_test".into(),
            level_name: None,
            price: 21000.0,
            direction: None,
            sequence_num: None,
            metadata: None,
            session_type: "Globex".into(),
            session_segment: "Asia".into(),
            trading_day: "2026-03-04".into(),
        }])
        .expect("insert");
        let row: (String, String, String) = db
            .conn
            .query_row(
                "SELECT session_type, session_segment, trading_day
                 FROM market_events WHERE event_type = 'context_test' LIMIT 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .expect("row");
        assert_eq!(row.0, "Globex");
        assert_eq!(row.1, "Asia");
        assert_eq!(row.2, "2026-03-04");
    }

    #[test]
    fn query_ticks_filtered_by_time_range() {
        let db = test_db();
        // Insert ticks spread across two time windows.
        db.insert_raw_tick(
            1_000.0,
            21000.0,
            10.0,
            20999.75,
            21000.25,
            true,
            "2026-03-04",
        )
        .expect("insert t1");
        db.insert_raw_tick(
            2_000.0,
            21001.0,
            5.0,
            21000.75,
            21001.25,
            false,
            "2026-03-04",
        )
        .expect("insert t2");
        db.insert_raw_tick(
            3_000.0,
            21002.0,
            8.0,
            21001.75,
            21002.25,
            true,
            "2026-03-04",
        )
        .expect("insert t3");
        db.insert_raw_tick(
            4_000.0,
            21003.0,
            12.0,
            21002.75,
            21003.25,
            false,
            "2026-03-04",
        )
        .expect("insert t4");

        // Time-range filter: only t2 and t3.
        let ticks = db
            .query_ticks_filtered(Some(1_500.0), Some(3_500.0), None, None, None, 100)
            .expect("query");
        assert_eq!(ticks.len(), 2);
        // Chronological order when time range given.
        assert_eq!(ticks[0].timestamp_ms, 2_000.0);
        assert_eq!(ticks[1].timestamp_ms, 3_000.0);

        // Price-range filter: only t1 (21000) and t2 (21001).
        let ticks = db
            .query_ticks_filtered(None, None, Some(20999.0), Some(21001.5), None, 100)
            .expect("query price");
        assert_eq!(ticks.len(), 2);

        // Session-date filter returns all ticks for that day.
        let ticks = db
            .query_ticks_filtered(None, None, None, None, Some("2026-03-04"), 100)
            .expect("query session");
        assert_eq!(ticks.len(), 4);

        // Combined time + price: only t3 (ts=3000, price=21002).
        let ticks = db
            .query_ticks_filtered(
                Some(2_500.0),
                Some(3_500.0),
                Some(21001.5),
                Some(21002.5),
                None,
                100,
            )
            .expect("query combined");
        assert_eq!(ticks.len(), 1);
        assert_eq!(ticks[0].price, 21002.0);
    }

    #[test]
    fn get_snapshot_near_returns_closest() {
        let db = test_db();
        // Insert three snapshots at t=1000, 5000, 9000.
        let snap_a = serde_json::json!({"label": "a"});
        let snap_b = serde_json::json!({"label": "b"});
        let snap_c = serde_json::json!({"label": "c"});
        db.insert_pipeline_snapshot(1_000.0, &snap_a).expect("a");
        db.insert_pipeline_snapshot(5_000.0, &snap_b).expect("b");
        db.insert_pipeline_snapshot(9_000.0, &snap_c).expect("c");

        // Query at t=3500 → closer to b (5000, diff=1500) than a (1000, diff=2500).
        let (ts, payload) = db.get_snapshot_near(3_500.0).expect("query").expect("some");
        assert_eq!(ts, 5_000.0);
        assert_eq!(payload["label"], "b");

        // Query at t=1100 → closer to a (1000, diff=100).
        let (ts, _) = db.get_snapshot_near(1_100.0).expect("q2").expect("some");
        assert_eq!(ts, 1_000.0);

        // Query exactly at t=9000 → c.
        let (ts, payload) = db.get_snapshot_near(9_000.0).expect("q3").expect("some");
        assert_eq!(ts, 9_000.0);
        assert_eq!(payload["label"], "c");

        // Query past the end → c (last snapshot).
        let (ts, _) = db.get_snapshot_near(99_000.0).expect("q4").expect("some");
        assert_eq!(ts, 9_000.0);
    }

    #[test]
    fn dom_snapshot_helpers_round_trip() {
        let mut db = test_db();
        let records = vec![
            DepthRecord {
                timestamp_ms: 1_000.0,
                command: crate::depth::DepthCommand::AddBidLevel,
                side: Some(crate::depth::DepthSide::Bid),
                end_of_batch: false,
                num_orders: 1,
                price: 21000.0,
                quantity: 10,
            },
            DepthRecord {
                timestamp_ms: 1_100.0,
                command: crate::depth::DepthCommand::AddAskLevel,
                side: Some(crate::depth::DepthSide::Ask),
                end_of_batch: true,
                num_orders: 1,
                price: 21000.25,
                quantity: 12,
            },
        ];
        let next_batch = db
            .insert_depth_events_batch("NQ.depth", &records, 42)
            .expect("insert depth events");
        assert_eq!(next_batch, 43);

        let snapshot = serde_json::json!({"bestBid": 21000.0, "bestAsk": 21000.25});
        let feature = serde_json::json!({
            "domSummary": { "liquidityBias": "bid_support", "pullStackBias": 15.0 },
            "activity": { "topPullLevels": [], "topStackLevels": [] }
        });
        db.insert_dom_snapshot("NQ.depth", 1_100.0, "2026-03-05", &snapshot)
            .expect("insert snapshot");
        db.insert_dom_feature_snapshot("NQ.depth", 1_100.0, "2026-03-05", &feature)
            .expect("insert feature");

        let events = db
            .query_depth_events(Some(900.0), Some(1_200.0), None, None, 10)
            .expect("query events");
        assert_eq!(events.len(), 2);
        let (ts, payload) = db
            .get_dom_feature_near(1_050.0)
            .expect("feature near")
            .expect("some");
        assert_eq!(ts, 1_100.0);
        assert_eq!(payload["domSummary"]["liquidityBias"], "bid_support");
        let snapshots = db
            .query_dom_feature_snapshots(Some(900.0), Some(1_200.0), 10)
            .expect("query features");
        assert_eq!(snapshots.len(), 1);
    }

    #[test]
    fn market_event_scope_falls_back_to_timestamp_when_context_missing() {
        let db = test_db();
        let ts = Eastern
            .with_ymd_and_hms(2026, 3, 2, 19, 0, 0)
            .single()
            .expect("ts")
            .timestamp_millis() as f64;
        db.conn
            .execute(
                "INSERT INTO market_events
                 (session_date, timestamp_ms, event_type, level_name, price, direction, sequence_num, metadata_json,
                  session_type, session_segment, trading_day)
                 VALUES (?1, ?2, ?3, NULL, ?4, NULL, NULL, NULL, NULL, NULL, NULL)",
                params!["2026-03-02", ts, "legacy_scope_test", 21000.0],
            )
            .expect("insert");

        let scope = SessionScopeFilter {
            session_type: Some("Globex".into()),
            session_segment: Some("Asia".into()),
            trading_day_start: None,
            trading_day_end: None,
            root_symbol: None,
            contract_symbol: None,
            include_rollover_sessions: true,
            continuous_mode: false,
        };
        let (total, sessions_with, total_sessions) = db
            .count_events_by_type("legacy_scope_test", None, None, Some(&scope))
            .expect("counts");
        assert_eq!(total, 1);
        assert_eq!(sessions_with, 1);
        assert_eq!(total_sessions, 1);
    }

    #[test]
    fn signal_performance_includes_pending_and_time_exit_and_source_filters() {
        let db = test_db();
        db.insert_signal_outcome(&SignalOutcome {
            signal_id: "p1".into(),
            setup_id: "s1".into(),
            setup_name: Some("Setup 1".into()),
            session_date: "2026-03-04".into(),
            root_symbol: Some("NQ".into()),
            contract_symbol: Some("NQH26.CME".into()),
            source: "live".into(),
            job_id: None,
            fired_at_ms: 1_000.0,
            fired_price: 21000.0,
            target_price: Some(21010.0),
            stop_price: Some(20990.0),
            outcome: "pending".into(),
            outcome_at_ms: None,
            max_favorable_excursion: Some(8.0),
            max_adverse_excursion: Some(2.0),
            r_result: None,
            time_to_outcome_ms: None,
            rvol_at_fire: None,
            rvol_bucket_at_fire: None,
        })
        .expect("insert pending");
        db.insert_signal_outcome(&SignalOutcome {
            signal_id: "p2".into(),
            setup_id: "s1".into(),
            setup_name: Some("Setup 1".into()),
            session_date: "2026-03-04".into(),
            root_symbol: Some("NQ".into()),
            contract_symbol: Some("NQH26.CME".into()),
            source: "live".into(),
            job_id: None,
            fired_at_ms: 2_000.0,
            fired_price: 21000.0,
            target_price: Some(21010.0),
            stop_price: Some(20990.0),
            outcome: "target_hit".into(),
            outcome_at_ms: Some(2_500.0),
            max_favorable_excursion: Some(12.0),
            max_adverse_excursion: Some(3.0),
            r_result: Some(1.2),
            time_to_outcome_ms: Some(500.0),
            rvol_at_fire: None,
            rvol_bucket_at_fire: None,
        })
        .expect("insert winner");
        db.insert_signal_outcome(&SignalOutcome {
            signal_id: "p3".into(),
            setup_id: "s1".into(),
            setup_name: Some("Setup 1".into()),
            session_date: "2026-03-04".into(),
            root_symbol: Some("NQ".into()),
            contract_symbol: Some("NQH26.CME".into()),
            source: "backtest".into(),
            job_id: Some("job-1".into()),
            fired_at_ms: 3_000.0,
            fired_price: 21000.0,
            target_price: Some(21010.0),
            stop_price: Some(20990.0),
            outcome: "time_exit".into(),
            outcome_at_ms: Some(3_900.0),
            max_favorable_excursion: Some(6.0),
            max_adverse_excursion: Some(7.0),
            r_result: Some(-0.3),
            time_to_outcome_ms: Some(900.0),
            rvol_at_fire: None,
            rvol_bucket_at_fire: None,
        })
        .expect("insert time exit");

        let overall = db
            .signal_performance_filtered(None, None, None, None, None, None)
            .expect("overall");
        assert_eq!(overall["totalSignals"].as_i64(), Some(3));
        assert_eq!(overall["resolved"].as_i64(), Some(2));
        assert_eq!(overall["pending"].as_i64(), Some(1));
        assert_eq!(overall["targetHit"].as_i64(), Some(1));
        assert_eq!(overall["timeExit"].as_i64(), Some(1));
        assert_eq!(overall["stopHit"].as_i64(), Some(0));
        assert_eq!(overall["winRate"].as_f64(), Some(0.5));

        let live_only = db
            .signal_performance_filtered(None, None, None, Some("live"), None, None)
            .expect("live only");
        assert_eq!(live_only["totalSignals"].as_i64(), Some(2));
        assert_eq!(live_only["timeExit"].as_i64(), Some(0));

        let backtest_job = db
            .signal_performance_filtered(None, None, None, Some("backtest"), Some("job-1"), None)
            .expect("backtest job");
        assert_eq!(backtest_job["totalSignals"].as_i64(), Some(1));
        assert_eq!(backtest_job["timeExit"].as_i64(), Some(1));
    }

    #[test]
    fn setup_performance_matrix_sorts_and_applies_min_resolved() {
        let db = test_db();
        let insert = |db: &Database,
                      signal_id: &str,
                      setup_id: &str,
                      outcome: &str,
                      r: Option<f64>,
                      fired_at_ms: f64| {
            db.insert_signal_outcome(&SignalOutcome {
                signal_id: signal_id.into(),
                setup_id: setup_id.into(),
                setup_name: Some(setup_id.into()),
                session_date: "2026-03-04".into(),
                root_symbol: Some("NQ".into()),
                contract_symbol: Some("NQH26.CME".into()),
                source: "live".into(),
                job_id: None,
                fired_at_ms,
                fired_price: 21000.0,
                target_price: Some(21010.0),
                stop_price: Some(20990.0),
                outcome: outcome.into(),
                outcome_at_ms: None,
                max_favorable_excursion: None,
                max_adverse_excursion: None,
                r_result: r,
                time_to_outcome_ms: None,
                rvol_at_fire: None,
                rvol_bucket_at_fire: None,
            })
            .expect("insert");
        };

        insert(&db, "a1", "setup-a", "target_hit", Some(1.0), 1_000.0);
        insert(&db, "a2", "setup-a", "stop_hit", Some(-1.0), 2_000.0);
        insert(&db, "a3", "setup-a", "time_exit", Some(0.2), 3_000.0);
        insert(&db, "a4", "setup-a", "pending", None, 4_000.0);
        insert(&db, "b1", "setup-b", "target_hit", Some(1.5), 5_000.0);

        let matrix = db
            .setup_performance_matrix_filtered(
                None,
                None,
                None,
                None,
                None,
                2,
                SetupPerformanceSortBy::WinRate,
                25,
            )
            .expect("matrix");
        assert_eq!(matrix.len(), 1);
        assert_eq!(matrix[0]["setupId"].as_str(), Some("setup-a"));
        assert_eq!(matrix[0]["resolved"].as_i64(), Some(3));
        assert_eq!(matrix[0]["pending"].as_i64(), Some(1));
        assert_eq!(matrix[0]["timeExit"].as_i64(), Some(1));

        let limited = db
            .setup_performance_matrix_filtered(
                None,
                None,
                None,
                None,
                None,
                0,
                SetupPerformanceSortBy::TotalSignals,
                1,
            )
            .expect("limited");
        assert_eq!(limited.len(), 1);
        assert_eq!(limited[0]["setupId"].as_str(), Some("setup-a"));
    }

    #[test]
    fn excursion_rows_respect_scope_filters() {
        let db = test_db();
        let asia_ts = Eastern
            .with_ymd_and_hms(2026, 3, 2, 19, 0, 0)
            .single()
            .expect("asia ts")
            .timestamp_millis() as f64;
        let rth_ts = Eastern
            .with_ymd_and_hms(2026, 3, 3, 10, 0, 0)
            .single()
            .expect("rth ts")
            .timestamp_millis() as f64;

        db.insert_signal_outcome(&SignalOutcome {
            signal_id: "asia-row".into(),
            setup_id: "s-asia".into(),
            setup_name: Some("Asia Setup".into()),
            session_date: "2026-03-03".into(),
            root_symbol: Some("NQ".into()),
            contract_symbol: Some("NQH26.CME".into()),
            source: "live".into(),
            job_id: None,
            fired_at_ms: asia_ts,
            fired_price: 21000.0,
            target_price: Some(21010.0),
            stop_price: Some(20990.0),
            outcome: "target_hit".into(),
            outcome_at_ms: Some(asia_ts + 120_000.0),
            max_favorable_excursion: Some(10.0),
            max_adverse_excursion: Some(2.0),
            r_result: Some(1.0),
            time_to_outcome_ms: Some(120_000.0),
            rvol_at_fire: None,
            rvol_bucket_at_fire: None,
        })
        .expect("insert asia");
        db.insert_signal_outcome(&SignalOutcome {
            signal_id: "rth-row".into(),
            setup_id: "s-rth".into(),
            setup_name: Some("RTH Setup".into()),
            session_date: "2026-03-03".into(),
            root_symbol: Some("NQ".into()),
            contract_symbol: Some("NQH26.CME".into()),
            source: "live".into(),
            job_id: None,
            fired_at_ms: rth_ts,
            fired_price: 21000.0,
            target_price: Some(21010.0),
            stop_price: Some(20990.0),
            outcome: "stop_hit".into(),
            outcome_at_ms: Some(rth_ts + 180_000.0),
            max_favorable_excursion: Some(3.0),
            max_adverse_excursion: Some(9.0),
            r_result: Some(-1.0),
            time_to_outcome_ms: Some(180_000.0),
            rvol_at_fire: None,
            rvol_bucket_at_fire: None,
        })
        .expect("insert rth");

        let asia_scope = SessionScopeFilter {
            session_type: Some("Globex".into()),
            session_segment: Some("Asia".into()),
            trading_day_start: None,
            trading_day_end: None,
            root_symbol: None,
            contract_symbol: None,
            include_rollover_sessions: true,
            continuous_mode: false,
        };
        let rows = db
            .list_signal_outcomes_for_excursions_filtered(None, None, None, Some(&asia_scope))
            .expect("rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].setup_id, "s-asia");
    }
}
