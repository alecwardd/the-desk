//! Free helper functions: error mapping, parsing, validation, payload shaping.

use chrono::{NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;
use rmcp::{model::*, ErrorData as McpError};
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use the_desk_backend::attention::AttentionPulseKind;
use the_desk_backend::backfill;
use the_desk_backend::backup::{self, SkipReason, StartupBackupReport};
use the_desk_backend::db::{
    Database, HistoricalJobRun, SessionScopeFilter, SetupPerformanceSortBy, TradeRecord,
    RESEARCH_DISTRIBUTION_METRICS,
};
use the_desk_backend::depth::{
    aggregate_trade_volume_by_level, build_dom_feature_snapshot, DepthReader, DomFeatureSnapshot,
    DomSummary, PullStackActivitySummary,
};
use the_desk_backend::feed::load_feed_config;
use the_desk_backend::feed::scid_reader::{ScanControl as ScidScanControl, ScidReader};
use the_desk_backend::observability::{RuntimeEvent, RuntimeEventLevel, RuntimeEventStore};
use the_desk_backend::options::OptionsSnapshot;
use the_desk_backend::pipelines::PipelineEngine;
use the_desk_backend::{
    classify_session, et_minutes_from_timestamp, SessionType, GLOBEX_OPEN_ET, RTH_CLOSE_ET,
    RTH_OPEN_ET,
};
use tokio::time::{sleep, Duration};

#[allow(unused_imports)]
use crate::{lifecycle::*, params::*, state::*};

pub(crate) fn db_error(e: impl std::fmt::Display) -> McpError {
    McpError::new(ErrorCode::INTERNAL_ERROR, e.to_string(), None)
}

pub(crate) fn lock_error() -> McpError {
    McpError::new(ErrorCode::INTERNAL_ERROR, "database lock poisoned", None)
}

pub(crate) fn freshness_status_from_age(age_ms: f64) -> &'static str {
    if age_ms < 0.0 || !age_ms.is_finite() {
        "unknown"
    } else if age_ms <= FRESHNESS_THRESHOLD_MS {
        "ok"
    } else {
        "stale"
    }
}

pub(crate) fn transition_hint(
    et_minutes: i32,
) -> Option<(&'static str, &'static str, &'static str)> {
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

pub(crate) fn text_result(mut json: serde_json::Value) -> CallToolResult {
    if let Some(obj) = json.as_object_mut() {
        if !obj.contains_key("freshnessStatus") {
            if let Some(age_ms) = obj.get("dataAgeMs").and_then(|v| v.as_f64()) {
                obj.insert(
                    "freshnessStatus".to_string(),
                    serde_json::json!(freshness_status_from_age(age_ms)),
                );
            }
        }
        if obj.contains_key("dataAgeMs") || obj.contains_key("freshnessStatus") {
            obj.entry("freshnessThresholdMs".to_string())
                .or_insert(serde_json::json!(FRESHNESS_THRESHOLD_MS));
        }
    }
    CallToolResult::success(vec![Content::text(json.to_string())])
}

pub(crate) fn runtime_event_json(event: RuntimeEvent, source: &str) -> serde_json::Value {
    let mut value = serde_json::to_value(event).unwrap_or_else(|_| serde_json::json!({}));
    if let Some(obj) = value.as_object_mut() {
        obj.insert("source".to_string(), serde_json::json!(source));
    }
    value
}

pub(crate) fn normalize_live_absorption_event(
    evt: &the_desk_backend::pipelines::AbsorptionEvent,
) -> serde_json::Value {
    serde_json::json!({
        "timestampMs": evt.timestamp_ms,
        "eventType": evt.event_type,
        "status": evt.status,
        "price": evt.price,
        "severity": evt.severity,
        "direction": evt.direction,
        "zoneLow": evt.zone_low,
        "zoneHigh": evt.zone_high,
        "keyLevel": evt.key_level,
        "confirmationDeadlineMs": evt.confirmation_deadline_ms,
        "confirmedAtMs": evt.confirmed_at_ms,
        "invalidatedAtMs": evt.invalidated_at_ms,
        "invalidationReason": evt.invalidation_reason,
        "pacePercentile": evt.pace_percentile,
        "rvolRatio": evt.rvol_ratio,
        "localVolatilityTicks": evt.local_volatility_ticks,
        "regimePhase": evt.regime_phase,
    })
}

pub(crate) fn normalize_db_absorption_event(row: &serde_json::Value) -> serde_json::Value {
    let metadata = row
        .get("metadata")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    let row_event_type = row
        .get("eventType")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let derived_status = if row_event_type.ends_with("_confirmed") {
        "confirmed"
    } else if row_event_type.ends_with("_invalidated") {
        "invalidated"
    } else {
        "candidate"
    };

    serde_json::json!({
        "timestampMs": row.get("timestampMs").cloned().unwrap_or(serde_json::json!(null)),
        "eventType": metadata.get("eventSubtype").cloned().unwrap_or_else(|| serde_json::json!(row_event_type)),
        "status": metadata.get("status").cloned().unwrap_or_else(|| serde_json::json!(derived_status)),
        "price": row.get("price").cloned().unwrap_or(serde_json::json!(null)),
        "severity": metadata.get("severity").cloned().unwrap_or(serde_json::json!(null)),
        "direction": row.get("direction").cloned().unwrap_or(serde_json::json!(null)),
        "zoneLow": metadata.get("zoneLow").cloned().unwrap_or(serde_json::json!(null)),
        "zoneHigh": metadata.get("zoneHigh").cloned().unwrap_or(serde_json::json!(null)),
        "keyLevel": metadata.get("keyLevel").cloned().unwrap_or(serde_json::json!(null)),
        "confirmationDeadlineMs": metadata.get("confirmationDeadlineMs").cloned().unwrap_or(serde_json::json!(null)),
        "confirmedAtMs": metadata.get("confirmedAtMs").cloned().unwrap_or(serde_json::json!(null)),
        "invalidatedAtMs": metadata.get("invalidatedAtMs").cloned().unwrap_or(serde_json::json!(null)),
        "invalidationReason": metadata.get("invalidationReason").cloned().unwrap_or(serde_json::json!(null)),
        "pacePercentile": metadata.get("pacePercentile").cloned().unwrap_or(serde_json::json!(null)),
        "rvolRatio": metadata.get("rvolRatio").cloned().unwrap_or(serde_json::json!(null)),
        "localVolatilityTicks": metadata.get("localVolatilityTicks").cloned().unwrap_or(serde_json::json!(null)),
        "regimePhase": metadata.get("regimePhase").cloned().unwrap_or(serde_json::json!(null)),
    })
}

pub(crate) fn no_data(msg: &str) -> CallToolResult {
    CallToolResult::success(vec![Content::text(msg.to_string())])
}

pub(crate) fn record_runtime_event(
    runtime_events: &Arc<RuntimeEventStore>,
    db: Option<&Arc<Mutex<Database>>>,
    level: RuntimeEventLevel,
    event_name: &str,
    category: &str,
    message: impl Into<String>,
    fields: serde_json::Value,
) -> RuntimeEvent {
    record_runtime_event_scoped(
        runtime_events,
        db,
        level,
        event_name,
        category,
        message,
        fields,
        None,
        None,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn record_runtime_event_scoped(
    runtime_events: &Arc<RuntimeEventStore>,
    db: Option<&Arc<Mutex<Database>>>,
    level: RuntimeEventLevel,
    event_name: &str,
    category: &str,
    message: impl Into<String>,
    fields: serde_json::Value,
    session_date: Option<String>,
    root_symbol: Option<String>,
    contract_symbol: Option<String>,
) -> RuntimeEvent {
    let event = RuntimeEvent::new(level, event_name, category, message, fields)
        .with_session_date(session_date)
        .with_contract(root_symbol, contract_symbol);
    let recorded = runtime_events.record(event.clone());
    if let Some(recorded) = &recorded {
        persist_runtime_event_if_enabled(runtime_events, db, recorded);
    }
    recorded.unwrap_or(event)
}

pub(crate) fn persist_runtime_event_if_enabled(
    runtime_events: &RuntimeEventStore,
    db: Option<&Arc<Mutex<Database>>>,
    event: &RuntimeEvent,
) {
    if !runtime_events.persist_runtime_events() {
        return;
    }
    let Some(db) = db else {
        return;
    };
    if let Ok(db) = db.lock() {
        persist_runtime_event_in_db(runtime_events, &db, event);
    }
}

pub(crate) fn persist_runtime_event_in_db(
    runtime_events: &RuntimeEventStore,
    db: &Database,
    event: &RuntimeEvent,
) {
    if !runtime_events.persist_runtime_events() {
        return;
    }
    let _ = db.insert_runtime_event(event);
}

pub(crate) fn prune_runtime_events_if_enabled(runtime_events: &RuntimeEventStore, db: &Database) {
    if runtime_events.persist_runtime_events() {
        let _ = db.prune_runtime_events(
            runtime_events.retention_days(),
            runtime_events.max_persisted_rows(),
        );
    }
}

pub(crate) fn spawn_runtime_event_pruner(
    runtime_events: Arc<RuntimeEventStore>,
    db: Arc<Mutex<Database>>,
) {
    tokio::spawn(async move {
        let interval = Duration::from_secs(60);
        loop {
            sleep(interval).await;
            if let Ok(db) = db.lock() {
                prune_runtime_events_if_enabled(runtime_events.as_ref(), &db);
            }
        }
    });
}

/// Take a verified database snapshot on startup, off the serving path.
///
/// Runs once in a background task: the `VACUUM INTO` happens inside
/// `spawn_blocking` (it briefly holds the writer mutex), and the resulting
/// runtime event is emitted only after the lock is released — `record_runtime_event`
/// re-locks the same mutex to persist, and `std::sync::Mutex` is not reentrant.
/// Gated by `[backup].enabled` and `min_interval_hours` so frequent restarts
/// don't accumulate snapshots.
pub(crate) fn spawn_startup_backup(
    runtime_events: Arc<RuntimeEventStore>,
    db: Arc<Mutex<Database>>,
) {
    let config = backup::load_backup_config();
    if !config.enabled {
        return;
    }
    tokio::spawn(async move {
        let db_for_blocking = Arc::clone(&db);
        let outcome = tokio::task::spawn_blocking(move || {
            let now = Utc::now();
            let guard = db_for_blocking
                .lock()
                .map_err(|_| "database mutex poisoned".to_string())?;
            backup::run_startup_backup(&guard, &config, now).map_err(|e| e.to_string())
        })
        .await;

        match outcome {
            Ok(Ok(StartupBackupReport::Created(o))) => {
                record_runtime_event(
                    &runtime_events,
                    Some(&db),
                    RuntimeEventLevel::Info,
                    "backup.created",
                    "backup",
                    "Database snapshot written on startup.",
                    serde_json::json!({
                        "path": o.path.to_string_lossy(),
                        "sizeBytes": o.size_bytes,
                        "verified": o.verified,
                        "prunedCount": o.pruned.len(),
                    }),
                );
            }
            Ok(Ok(StartupBackupReport::Skipped(reason))) => {
                let fields = match &reason {
                    SkipReason::Disabled => serde_json::json!({ "reason": "disabled" }),
                    SkipReason::WithinInterval {
                        hours_since_last,
                        min_interval_hours,
                    } => serde_json::json!({
                        "reason": "withinInterval",
                        "hoursSinceLast": hours_since_last,
                        "minIntervalHours": min_interval_hours,
                    }),
                };
                record_runtime_event(
                    &runtime_events,
                    Some(&db),
                    RuntimeEventLevel::Info,
                    "backup.skipped",
                    "backup",
                    "Startup database backup skipped.",
                    fields,
                );
            }
            Ok(Err(detail)) => {
                record_runtime_event(
                    &runtime_events,
                    Some(&db),
                    RuntimeEventLevel::Warn,
                    "backup.failed",
                    "backup",
                    "Startup database backup failed.",
                    serde_json::json!({ "error": detail }),
                );
            }
            Err(join_err) => {
                record_runtime_event(
                    &runtime_events,
                    Some(&db),
                    RuntimeEventLevel::Warn,
                    "backup.failed",
                    "backup",
                    "Startup database backup task did not complete.",
                    serde_json::json!({ "error": join_err.to_string() }),
                );
            }
        }
    });
}

pub(crate) fn spawn_attention_periodic_pulse(
    pipelines: Arc<Mutex<PipelineEngine>>,
    db: Arc<Mutex<Database>>,
    runtime_events: Arc<RuntimeEventStore>,
    last_bid: Arc<Mutex<f64>>,
    last_ask: Arc<Mutex<f64>>,
) {
    tokio::spawn(async move {
        let interval = Duration::from_secs(5);
        loop {
            sleep(interval).await;
            let timestamp_ms = chrono::Utc::now().timestamp_millis() as f64;
            let snapshot = {
                let Ok(pipelines) = pipelines.try_lock() else {
                    continue;
                };
                let (bid, ask) = current_best_bid_ask(&last_bid, &last_ask);
                pipelines.snapshot(bid, ask)
            };
            if snapshot.last_price <= 0.0 || snapshot.trading_day.is_empty() {
                continue;
            }
            if snapshot.session_type == "Unknown" {
                continue;
            }
            if let Ok(db) = db.lock() {
                expire_and_audit_attention_signals(
                    &db,
                    &runtime_events,
                    timestamp_ms,
                    Some("live"),
                );
                if snapshot.session_type == "RTH" {
                    compose_and_persist_attention(
                        &db,
                        &runtime_events,
                        &snapshot,
                        &[],
                        AttentionPulseKind::Periodic,
                        timestamp_ms,
                        "live",
                        None,
                    );
                }
                dispatch_attention_runtime_notifications(&db, &runtime_events, timestamp_ms);
            }
        }
    });
}

pub(crate) fn resolve_session_id(
    db: &Database,
    requested_session_id: Option<&str>,
) -> Result<Option<String>, McpError> {
    if let Some(session_id) = requested_session_id {
        return Ok(Some(session_id.to_string()));
    }
    Ok(db
        .get_latest_open_session()
        .map_err(db_error)?
        .map(|session| session.id))
}

pub(crate) fn infer_session_type_label(timestamp_ms: f64) -> String {
    match et_minutes_from_timestamp(timestamp_ms)
        .map(classify_session)
        .unwrap_or(SessionType::Unknown)
    {
        SessionType::Rth => "rth".to_string(),
        SessionType::Globex => "globex".to_string(),
        SessionType::Unknown => "unknown".to_string(),
    }
}

pub(crate) fn parse_import_timestamp(raw: &str, timezone: Tz) -> Result<f64, McpError> {
    let parsed = NaiveDateTime::parse_from_str(raw.trim(), "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(raw.trim(), "%Y-%m-%d  %H:%M:%S%.f"))
        .map_err(|e| invalid_params_error(format!("invalid fill timestamp `{raw}`: {e}")))?;
    timezone
        .from_local_datetime(&parsed)
        .single()
        .map(|dt| dt.with_timezone(&Utc).timestamp_millis() as f64)
        .ok_or_else(|| invalid_params_error(format!("ambiguous or invalid timestamp `{raw}`")))
}

#[derive(Debug, Clone)]
pub(crate) struct FillSlice {
    pub(crate) timestamp_ms: f64,
    pub(crate) price: f64,
    pub(crate) quantity: i64,
    pub(crate) symbol: String,
    pub(crate) trade_account: Option<String>,
    pub(crate) batch_id: String,
    pub(crate) fingerprint: String,
    pub(crate) order_side: String,
    pub(crate) open_close: Option<String>,
    pub(crate) service_order_id: Option<String>,
    pub(crate) external_order_id: Option<String>,
    pub(crate) raw_payload: serde_json::Value,
}

#[derive(Debug, Clone)]
pub(crate) struct ActiveImportedTrade {
    pub(crate) session_id: Option<String>,
    pub(crate) instrument: String,
    pub(crate) trade_account: Option<String>,
    pub(crate) direction: String,
    pub(crate) entry_start_ms: f64,
    pub(crate) last_exit_ms: f64,
    pub(crate) signed_position: i64,
    pub(crate) entry_qty_total: i64,
    pub(crate) exit_qty_total: i64,
    pub(crate) max_open_size: i64,
    pub(crate) weighted_entry_notional: f64,
    pub(crate) weighted_exit_notional: f64,
    pub(crate) fill_refs: Vec<FillSlice>,
}

pub(crate) fn signed_delta_for_fill(side: &str, quantity: i64) -> Result<i64, McpError> {
    match side.to_ascii_lowercase().as_str() {
        "buy" => Ok(quantity),
        "sell" => Ok(-quantity),
        other => Err(invalid_params_error(format!(
            "unsupported buy/sell value `{other}`"
        ))),
    }
}

pub(crate) fn build_imported_trade_record(
    state: &ActiveImportedTrade,
    source: &str,
    notes: &str,
) -> TradeRecord {
    let entry_price = if state.entry_qty_total > 0 {
        state.weighted_entry_notional / state.entry_qty_total as f64
    } else {
        0.0
    };
    let exit_price = if state.exit_qty_total > 0 {
        state.weighted_exit_notional / state.exit_qty_total as f64
    } else {
        0.0
    };
    let gross_points = if state.exit_qty_total > 0 {
        let per_contract = if state.direction == "long" {
            exit_price - entry_price
        } else {
            entry_price - exit_price
        };
        Some(per_contract * state.exit_qty_total as f64)
    } else {
        None
    };
    TradeRecord {
        id: uuid::Uuid::new_v4().to_string(),
        session_id: state.session_id.clone(),
        setup_id: None,
        instrument: Some(state.instrument.clone()),
        trade_account: state.trade_account.clone(),
        entry_time: state.entry_start_ms,
        entry_price,
        exit_time: Some(state.last_exit_ms),
        exit_price: Some(exit_price),
        direction: state.direction.clone(),
        size: state.max_open_size,
        max_open_size: Some(state.max_open_size),
        stop_price: None,
        target_prices: Vec::new(),
        result_r: None,
        gross_points,
        planned: false,
        rules_followed: None,
        emotional_state: None,
        thesis: None,
        review_tags: Vec::new(),
        mistake_tags: Vec::new(),
        entry_fill_count: state
            .fill_refs
            .iter()
            .filter(|fill| {
                signed_delta_for_fill(&fill.order_side, fill.quantity)
                    .unwrap_or_default()
                    .signum()
                    == if state.direction == "long" { 1 } else { -1 }
            })
            .count() as i64,
        exit_fill_count: state
            .fill_refs
            .iter()
            .filter(|fill| {
                signed_delta_for_fill(&fill.order_side, fill.quantity)
                    .unwrap_or_default()
                    .signum()
                    == if state.direction == "long" { -1 } else { 1 }
            })
            .count() as i64,
        import_batch_id: Some(state.fill_refs[0].batch_id.clone()),
        planned_r_points_at_entry: None,
        planned_r_dollars_at_entry: None,
        notes: notes.to_string(),
        source: source.to_string(),
    }
}

pub(crate) const TAPE_PACE_RESPONSE_KEYS: &[&str] = &[
    "ticksPerSec5s",
    "ticksPerSec30s",
    "ticksPerSec5m",
    "volumePerSec5s",
    "volumePerSec30s",
    "volumePerSec5m",
    "acceleration",
    "rawAcceleration",
    "pacePercentile",
    "rollingPacePercentile",
    "regimeTicksPerSec30mEma",
    "regimeVolumePerSec30mEma",
    "windowCoverage5s",
    "windowCoverage30s",
    "windowCoverage5m",
    "isValid5s",
    "isValid30s",
    "isValid5m",
    "windowAnchorTimestampMs",
    "lastTradeTimestampMs",
    "dwellAtCurrentPriceMs",
    "currentPrice",
];

pub(crate) fn build_tape_pace_response(
    mut payload: serde_json::Value,
    data_age_ms: f64,
    is_live: bool,
    now_ms: f64,
) -> serde_json::Value {
    if let Some(obj) = payload.as_object_mut() {
        let last_trade_timestamp_ms = obj.get("lastTradeTimestampMs").and_then(|v| v.as_f64());
        let has_all_keys = TAPE_PACE_RESPONSE_KEYS
            .iter()
            .all(|key| obj.contains_key(*key));
        let data_quality = if !has_all_keys {
            "PARTIAL"
        } else if is_live {
            "LIVE"
        } else {
            "STALE"
        };
        obj.insert(
            "eventTimeLagMs".to_string(),
            serde_json::json!(last_trade_timestamp_ms.map(|ts| (now_ms - ts).max(0.0))),
        );
        obj.insert("dataQuality".to_string(), serde_json::json!(data_quality));
        obj.insert("isLive".to_string(), serde_json::json!(is_live));
        obj.insert("dataAgeMs".to_string(), serde_json::json!(data_age_ms));
    }
    payload
}

pub(crate) fn invalid_params_error(msg: impl Into<String>) -> McpError {
    McpError::new(ErrorCode::INVALID_PARAMS, msg.into(), None)
}

pub(crate) fn normalize_options_root(
    requested_root: Option<&str>,
    default_root: &str,
) -> Result<String, McpError> {
    let root = requested_root
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(default_root)
        .trim()
        .to_uppercase();
    if root.is_empty() {
        return Err(invalid_params_error("root must not be empty"));
    }
    Ok(root)
}

pub(crate) fn normalize_options_exps(
    requested_exps: Option<Vec<u32>>,
    default_exps: &[u32],
) -> Vec<u32> {
    let mut exps = requested_exps.unwrap_or_else(|| default_exps.to_vec());
    exps.sort_unstable();
    exps.dedup();
    exps
}

pub(crate) fn options_cache_metadata(
    snapshot: &OptionsSnapshot,
    refreshed: bool,
) -> serde_json::Value {
    let now_ms = Utc::now().timestamp_millis() as f64;
    serde_json::json!({
        "fetchedAtMs": snapshot.fetched_at_ms,
        "snapshotAgeMs": snapshot.age_ms(now_ms),
        "cacheTtlMs": snapshot.cache_ttl_ms,
        "cacheStatus": if refreshed { "refreshed" } else { "hit" },
    })
}

pub(crate) fn validate_time_window(start_time_ms: f64, end_time_ms: f64) -> Result<(), McpError> {
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

pub(crate) fn depth_reader_for_timestamp(timestamp_ms: f64) -> Result<DepthReader, McpError> {
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

pub(crate) fn aggregate_window_trades(
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

pub(crate) fn latest_depth_reader() -> Result<Option<DepthReader>, McpError> {
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
pub(crate) fn compute_dom_feature_for_window(
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

pub(crate) fn dom_summary_from_payload(payload: &serde_json::Value) -> Option<DomSummary> {
    payload
        .get("domSummary")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
}

pub(crate) fn activity_from_payload(
    payload: &serde_json::Value,
) -> Option<PullStackActivitySummary> {
    payload
        .get("activity")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
}

pub(crate) fn dom_summaries_from_rows(rows: &[(f64, serde_json::Value)]) -> Vec<DomSummary> {
    rows.iter()
        .filter_map(|(_, payload)| dom_summary_from_payload(payload))
        .collect()
}

pub(crate) fn merge_dom_summary_into_snapshot(
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

pub(crate) fn footprint_from_ticks(
    ticks: &[the_desk_backend::db::RawTickRecord],
) -> Vec<serde_json::Value> {
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
pub(crate) struct SessionScopeParams {
    /// Session type filter: "RTH", "Globex", or "Unknown".
    #[serde(alias = "session_type")]
    pub(crate) session_type: Option<String>,
    /// Globex segment filter: "Asia", "London", or "None".
    #[serde(alias = "session_segment")]
    pub(crate) session_segment: Option<String>,
    /// Exact trading day (YYYY-MM-DD, 6 PM ET roll).
    #[serde(alias = "trading_day")]
    pub(crate) trading_day: Option<String>,
    /// Trading-day range start (YYYY-MM-DD, 6 PM ET roll).
    #[serde(alias = "trading_day_start")]
    pub(crate) trading_day_start: Option<String>,
    /// Trading-day range end (YYYY-MM-DD, 6 PM ET roll).
    #[serde(alias = "trading_day_end")]
    pub(crate) trading_day_end: Option<String>,
    /// Filter to a specific root symbol (e.g. NQ) across contract rolls.
    #[serde(alias = "root_symbol")]
    pub(crate) root_symbol: Option<String>,
    /// Filter to a specific contract symbol (e.g. NQM26.CME).
    #[serde(alias = "contract_symbol")]
    pub(crate) contract_symbol: Option<String>,
    /// Include sessions flagged as roll-boundary carry-forward mismatches. Default true.
    #[serde(alias = "include_rollover_sessions", default = "default_true")]
    pub(crate) include_rollover_sessions: bool,
    /// Treat matching root-symbol sessions as a continuous research stream. Default false.
    #[serde(alias = "continuous_mode", default)]
    pub(crate) continuous_mode: bool,
}

pub(crate) fn default_true() -> bool {
    true
}

pub(crate) fn normalize_session_type_param(value: &str) -> Option<&'static str> {
    match value.trim().to_ascii_lowercase().as_str() {
        "rth" => Some("RTH"),
        "globex" => Some("Globex"),
        "unknown" => Some("Unknown"),
        _ => None,
    }
}

pub(crate) fn normalize_session_segment_param(value: &str) -> Option<&'static str> {
    match value.trim().to_ascii_lowercase().as_str() {
        "asia" => Some("Asia"),
        "london" => Some("London"),
        "none" => Some("None"),
        _ => None,
    }
}

pub(crate) fn validate_ymd_opt(label: &str, value: Option<&str>) -> Result<(), McpError> {
    if let Some(date) = value {
        if chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").is_err() {
            return Err(invalid_params_error(format!(
                "{label} must be YYYY-MM-DD, got: {date}"
            )));
        }
    }
    Ok(())
}

pub(crate) fn validate_ymd_range(
    start_label: &str,
    start_value: Option<&str>,
    end_label: &str,
    end_value: Option<&str>,
) -> Result<(), McpError> {
    validate_ymd_opt(start_label, start_value)?;
    validate_ymd_opt(end_label, end_value)?;
    if let (Some(start), Some(end)) = (start_value, end_value) {
        if start > end {
            return Err(invalid_params_error(format!(
                "{start_label} must be on or before {end_label}"
            )));
        }
    }
    Ok(())
}

pub(crate) fn parse_non_empty_string(label: &str, raw: &str) -> Result<String, McpError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(invalid_params_error(format!("{label} must not be empty")));
    }
    Ok(trimmed.to_string())
}

pub(crate) fn parse_optional_non_empty_string(
    label: &str,
    raw: Option<&str>,
) -> Result<Option<String>, McpError> {
    raw.map(|value| parse_non_empty_string(label, value))
        .transpose()
}

pub(crate) fn parse_allowed_lowercase_value(
    label: &str,
    raw: &str,
    allowed: &[&str],
) -> Result<String, McpError> {
    let normalized = parse_non_empty_string(label, raw)?.to_ascii_lowercase();
    if allowed.contains(&normalized.as_str()) {
        Ok(normalized)
    } else {
        Err(invalid_params_error(format!(
            "{label} must be one of {}, got: {}",
            allowed.join("|"),
            raw.trim()
        )))
    }
}

pub(crate) fn build_session_scope_filter(
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
    validate_ymd_range(
        "tradingDayStart",
        trading_day_start.as_deref(),
        "tradingDayEnd",
        trading_day_end.as_deref(),
    )?;
    let root_symbol = parse_optional_non_empty_string("rootSymbol", params.root_symbol.as_deref())?;
    let contract_symbol =
        parse_optional_non_empty_string("contractSymbol", params.contract_symbol.as_deref())?;

    let scope = SessionScopeFilter {
        session_type,
        session_segment,
        trading_day_start,
        trading_day_end,
        root_symbol,
        contract_symbol,
        include_rollover_sessions: params.include_rollover_sessions,
        continuous_mode: params.continuous_mode,
    };
    if scope.session_type.is_none()
        && scope.session_segment.is_none()
        && scope.trading_day_start.is_none()
        && scope.trading_day_end.is_none()
        && scope.root_symbol.is_none()
        && scope.contract_symbol.is_none()
        && scope.include_rollover_sessions
        && !scope.continuous_mode
    {
        Ok(None)
    } else {
        Ok(Some(scope))
    }
}

pub(crate) fn parse_scope_value(
    scope: Option<serde_json::Value>,
) -> Result<Option<SessionScopeFilter>, McpError> {
    let Some(scope) = scope else {
        return Ok(None);
    };
    let parsed: SessionScopeParams = serde_json::from_value(scope)
        .map_err(|e| invalid_params_error(format!("invalid scope payload: {e}")))?;
    build_session_scope_filter(&parsed)
}

pub(crate) fn parse_setup_perf_sort(
    sort_by: Option<&str>,
) -> Result<SetupPerformanceSortBy, McpError> {
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

pub(crate) fn parse_research_event_type(raw: &str) -> Result<String, McpError> {
    let event_type = parse_non_empty_string("eventType", raw)?.to_ascii_lowercase();
    if RESEARCH_EVENT_TYPES.contains(&event_type.as_str()) {
        return Ok(event_type);
    }
    if let Some(level_name) = event_type.strip_suffix("_test") {
        if RESEARCH_LEVEL_TEST_NAMES.contains(&level_name) {
            return Ok(event_type);
        }
    }
    Err(invalid_params_error(format!(
        "eventType must be a supported research event type, got: {}",
        raw.trim()
    )))
}

pub(crate) fn parse_research_outcome_field(raw: &str) -> Result<String, McpError> {
    parse_allowed_lowercase_value("outcomeField", raw, RESEARCH_OUTCOME_FIELDS)
}

pub(crate) fn parse_distribution_metric(raw: &str) -> Result<String, McpError> {
    parse_allowed_lowercase_value("metric", raw, RESEARCH_DISTRIBUTION_METRICS)
}

pub(crate) fn parse_signal_outcome_session_field(raw: &str) -> Result<String, McpError> {
    parse_allowed_lowercase_value("sessionField", raw, SIGNAL_OUTCOME_SESSION_FIELDS)
}

pub(crate) fn parse_dom_behavior_name(raw: &str) -> Result<String, McpError> {
    parse_allowed_lowercase_value("behavior", raw, DOM_BEHAVIOR_NAMES)
}

pub(crate) fn parse_research_min_count(value: Option<i64>) -> Result<i64, McpError> {
    let min_count = value.unwrap_or(1);
    if !(1..=MAX_RESEARCH_MIN_COUNT).contains(&min_count) {
        return Err(invalid_params_error(format!(
            "minCount must be between 1 and {MAX_RESEARCH_MIN_COUNT}, got: {min_count}"
        )));
    }
    Ok(min_count)
}

pub(crate) fn parse_nonnegative_i64(
    label: &str,
    value: Option<i64>,
    default: i64,
    max: i64,
) -> Result<i64, McpError> {
    let parsed = value.unwrap_or(default);
    if parsed < 0 || parsed > max {
        return Err(invalid_params_error(format!(
            "{label} must be between 0 and {max}, got: {parsed}"
        )));
    }
    Ok(parsed)
}

pub(crate) fn parse_bounded_limit(
    label: &str,
    value: Option<u64>,
    default: u64,
    max: u64,
) -> Result<usize, McpError> {
    let limit = value.unwrap_or(default);
    if limit == 0 || limit > max {
        return Err(invalid_params_error(format!(
            "{label} must be between 1 and {max}, got: {limit}"
        )));
    }
    Ok(limit as usize)
}

pub(crate) fn parse_dom_behavior_min_duration(value: Option<f64>) -> Result<f64, McpError> {
    let min_duration_ms = value.unwrap_or(15_000.0);
    if !min_duration_ms.is_finite()
        || !(0.0..=MAX_DOM_BEHAVIOR_MIN_DURATION_MS).contains(&min_duration_ms)
    {
        return Err(invalid_params_error(format!(
            "minDurationMs must be a finite number between 0 and {MAX_DOM_BEHAVIOR_MIN_DURATION_MS}, got: {min_duration_ms}"
        )));
    }
    Ok(min_duration_ms)
}

pub(crate) fn normalize_signal_source(raw: &str) -> Option<&'static str> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "live" => Some("live"),
        "backtest" => Some("backtest"),
        "backfill" => Some("backfill"),
        _ => None,
    }
}

pub(crate) fn parse_optional_signal_source(
    source: Option<&str>,
) -> Result<Option<&'static str>, McpError> {
    source
        .map(|raw| {
            normalize_signal_source(raw).ok_or_else(|| {
                invalid_params_error(format!(
                    "source must be one of live|backtest|backfill, got: {raw}"
                ))
            })
        })
        .transpose()
}

pub(crate) fn load_contextual_prior_dnva(
    db: &Database,
    session_type: Option<&str>,
    session_segment: Option<&str>,
    trading_day: Option<&str>,
) -> (Option<DnvaTriple>, Option<DnvaTriple>) {
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

pub(crate) fn historical_job_response(
    run: &HistoricalJobRun,
    already_running: bool,
) -> serde_json::Value {
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

pub(crate) fn normalized_job_key(
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
