use chrono::{NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;
use rmcp::{
    handler::server::{tool::ToolRouter, wrapper::Parameters},
    model::*,
    tool, tool_handler, tool_router,
    transport::stdio,
    ErrorData as McpError, ServerHandler, ServiceExt,
};
use schemars::{json_schema, JsonSchema, Schema, SchemaGenerator};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;
use the_desk_backend::attention::{
    AttentionNotifierConfig, AttentionPulseKind, SignalComposer, SignalComposerInput,
};
use the_desk_backend::backfill;
use the_desk_backend::db::{
    AccountStateRecord, AttentionChangelogQuery, AttentionSignalQuery, AttentionSignalRecord,
    Database, HistoricalJobRun, ImportedFillRecord, JournalEntry, OpenPositionRecord,
    RawTickBatchRow, ReplaySignalRecord, RiskConfigRecord, SessionRecord, SessionScopeFilter,
    SetupPerformanceSortBy, SetupRuntimeStateRecord, SignalOutcome, TradeIdeaQuery,
    TradeImportBatchRecord, TradeRecord, TradeReviewUpdate, RESEARCH_DISTRIBUTION_METRICS,
};
use the_desk_backend::depth::{
    aggregate_trade_volume_by_level, build_dom_feature_snapshot, build_dom_summary,
    enrich_dom_summary, summarize_dom_narrative, DepthBook, DepthCommand, DepthReader,
    DomFeatureSnapshot, DomSummary, PullStackActivitySummary, ScanControl as DepthScanControl,
    DOM_NARRATIVE_HORIZON_MS,
};
use the_desk_backend::feed::monotonic::{
    MonotonicTickGuard, MonotonicTimestampDecision, MonotonicTimestampViolationKind,
};
use the_desk_backend::feed::scid_reader::{
    scid_tail_offset_after_shrink, ScanControl as ScidScanControl, ScidReader, ScidTick,
    SCID_RECORD_SIZE,
};
use the_desk_backend::feed::{
    load_feed_config, load_storage_config, resolve_contract_metadata, ContractMetadata, FeedConfig,
    TradeSide,
};
use the_desk_backend::mcp::hypotheses::{
    ActivateDraftSetupParams, HypothesisRunParams, ListHypothesesParams, RegisterHypothesisParams,
    SetHypothesisLifecycleParams,
};
use the_desk_backend::memory::{
    build_memory_brief as memory_build_memory_brief,
    detect_behavioral_patterns as memory_detect_behavioral_patterns,
    mark_memory_dirty as memory_mark_dirty, refresh_memory_state as memory_refresh_state,
    save_agent_insight as memory_save_agent_insight, AgentInsightQuery, BehavioralPatternQuery,
    MemoryBriefQuery, MemoryFollowupRecord, MemoryRefreshOptions, SaveAgentInsightInput,
};
use the_desk_backend::observability::{
    init_logging, load_logging_config, RuntimeEvent, RuntimeEventFilter, RuntimeEventLevel,
    RuntimeEventStore,
};
use the_desk_backend::options::{
    fetch_options_snapshot, load_options_config, OptionsCredentials, OptionsSnapshot,
};
use the_desk_backend::outcome_tracker;
use the_desk_backend::pipelines::{
    EventDetector, FlowEventEmitter, MarketEvent, PipelineEngine, PriorSessionData, RvolPipeline,
};
use the_desk_backend::research;
use the_desk_backend::research::hypothesis::{
    activate_draft_setup as hypothesis_activate_draft_setup,
    register_hypothesis as hypothesis_register_hypothesis,
    set_hypothesis_lifecycle as hypothesis_set_lifecycle,
    summarize_hypothesis_run as hypothesis_summarize_run, HypothesisMetadata,
    RegisterHypothesisRequest,
};
use the_desk_backend::risk::{RiskConfig, RiskState, RiskTracker};
use the_desk_backend::rollover::{
    build_contract_rollover_status, ContractRolloverStatus, PriorReferenceTrust,
};
use the_desk_backend::rules::{
    RulesEngine, SetupDefinition, SetupEvaluationOutcome, SetupRuntimeSnapshot, SetupState,
    SetupTransition,
};
use the_desk_backend::scid_tick_ingest::{self, TickIngestParams};
use the_desk_backend::scid_timestamp_diagnostics;
use the_desk_backend::{
    classify_delta_segment, classify_session, et_minutes_from_timestamp, globex_open_ms,
    minute_of_session_from_timestamp, session_date_from_timestamp_ms,
    trading_day_from_timestamp_ms, DeltaSegment, SessionType, GLOBEX_OPEN_ET, RTH_CLOSE_ET,
    RTH_OPEN_ET,
};
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::{sleep, Duration};

const FRESHNESS_THRESHOLD_MS: f64 = 15_000.0;
const JOB_PROGRESS_PERSIST_INTERVAL_MS: f64 = 1_000.0;
const JOB_PROGRESS_RECORD_STEP: usize = 50_000;
const JOB_PROGRESS_RATE_EMA_ALPHA: f64 = 0.25;
const PIPELINE_CONTENTION_RECENT_WINDOW_MS: u64 = 5_000;
const MONOTONIC_ANOMALY_RECENT_WINDOW_MS: f64 = 60_000.0;
const MAX_RESEARCH_RESULT_LIMIT: u64 = 500;
const MAX_RESEARCH_MIN_COUNT: i64 = 10_000;
const MAX_MIN_RESOLVED: i64 = 10_000;
const MAX_DOM_BEHAVIOR_MIN_DURATION_MS: f64 = 86_400_000.0;
const CONTRACT_RESOLUTION_CACHE_TTL_MS: u128 = 2_000;
const RESEARCH_EVENT_TYPES: &[&str] = &[
    "ib_formed",
    "or_formed",
    "ib_extension_hit",
    "ib_reentry",
    "ib_reentry_hit_mid",
    "ib_reentry_full_traverse",
    "new_session_high",
    "new_session_low",
    "day_type_change",
    "poor_high_detected",
    "poor_low_detected",
    "excess_high_detected",
    "excess_low_detected",
    "or5_mid_retest",
    "dnp_cross",
    "rvol_spike",
    "absorption_detected",
    "absorption_confirmed",
    "absorption_invalidated",
    "pinch_detected",
    "acceleration_zone_created",
    "acceleration_zone_held",
    "large_trade_cluster",
];
const RESEARCH_LEVEL_TEST_NAMES: &[&str] = &[
    "prior_day_high",
    "prior_day_low",
    "prior_day_close",
    "overnight_high",
    "overnight_low",
    "ib_high",
    "ib_low",
    "ib_mid",
    "previous_vah",
    "previous_val",
    "previous_poc",
    "vwap",
    "vwap_1sd_upper",
    "vwap_1sd_lower",
    "vwap_2sd_upper",
    "vwap_2sd_lower",
    "dnp",
    "dnva_high",
    "dnva_low",
];
const RESEARCH_OUTCOME_FIELDS: &[&str] = &[
    "close_vs_ib_mid",
    "close_vs_vwap",
    "close_vs_poc",
    "day_type",
    "profile_shape",
    "balance_state",
    "single_prints_direction",
    "poor_high",
    "poor_low",
    "excess_high",
    "excess_low",
];
const SIGNAL_OUTCOME_SESSION_FIELDS: &[&str] = &[
    "day_type",
    "profile_shape",
    "balance_state",
    "close_vs_ib_mid",
    "close_vs_vwap",
    "single_prints_direction",
];
const DOM_BEHAVIOR_NAMES: &[&str] = &[
    "bid_support_persisted",
    "ask_resistance_persisted",
    "liquidity_flip",
    "pulling_acceleration",
    "stacking_acceleration",
];
type DnvaTriple = (f64, f64, f64);

/// MCP clients (e.g. Cursor) may reject `tools/list` when `serde_json::Value` becomes JSON Schema
/// boolean `true`. Use explicit object schemas instead.
fn schemars_loose_object(_: &mut SchemaGenerator) -> Schema {
    json_schema!({
        "type": "object",
        "additionalProperties": true
    })
}

fn schemars_optional_loose_object(_: &mut SchemaGenerator) -> Schema {
    json_schema!({
        "anyOf": [
            { "type": "null" },
            { "type": "object", "additionalProperties": true }
        ]
    })
}

/// Atomics updated by SCID / `.depth` poll tasks for diagnostics and coherent `dataAgeMs` without extra DB locks.
#[derive(Clone)]
pub struct McpFeedRuntimeState {
    pub last_scid_tick_ms_bits: Arc<AtomicU64>,
    pub last_depth_timestamp_ms_bits: Arc<AtomicU64>,
    pub scid_tail_offset: Arc<AtomicU64>,
    pub scid_file_len: Arc<AtomicU64>,
    pub scid_tail_reset_count: Arc<AtomicU64>,
    pub scid_last_shrink_len: Arc<AtomicU64>,
    pub skipped_non_monotonic_ticks: Arc<AtomicU64>,
    pub duplicate_timestamp_ticks: Arc<AtomicU64>,
    pub backward_timestamp_ticks: Arc<AtomicU64>,
    pub last_non_monotonic_tick_ms_bits: Arc<AtomicU64>,
    pub last_scid_poll_wall_ms: Arc<AtomicU64>,
    pub pipeline_lock_contended_now: Arc<AtomicBool>,
    pub pipeline_last_contended_wall_ms: Arc<AtomicU64>,
    pub setup_runtime_rehydrated: Arc<AtomicBool>,
    pub rules_warm_replay_complete: Arc<AtomicBool>,
}

impl Default for McpFeedRuntimeState {
    fn default() -> Self {
        Self {
            last_scid_tick_ms_bits: Arc::new(AtomicU64::new(0)),
            last_depth_timestamp_ms_bits: Arc::new(AtomicU64::new(0)),
            scid_tail_offset: Arc::new(AtomicU64::new(0)),
            scid_file_len: Arc::new(AtomicU64::new(0)),
            scid_tail_reset_count: Arc::new(AtomicU64::new(0)),
            scid_last_shrink_len: Arc::new(AtomicU64::new(0)),
            skipped_non_monotonic_ticks: Arc::new(AtomicU64::new(0)),
            duplicate_timestamp_ticks: Arc::new(AtomicU64::new(0)),
            backward_timestamp_ticks: Arc::new(AtomicU64::new(0)),
            last_non_monotonic_tick_ms_bits: Arc::new(AtomicU64::new(0)),
            last_scid_poll_wall_ms: Arc::new(AtomicU64::new(0)),
            pipeline_lock_contended_now: Arc::new(AtomicBool::new(false)),
            pipeline_last_contended_wall_ms: Arc::new(AtomicU64::new(0)),
            setup_runtime_rehydrated: Arc::new(AtomicBool::new(false)),
            rules_warm_replay_complete: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct MonotonicRuntimeSnapshot {
    skipped_non_monotonic_ticks: u64,
    duplicate_timestamp_ticks: u64,
    backward_timestamp_ticks: u64,
    last_non_monotonic_timestamp_ms: Option<f64>,
}

impl MonotonicRuntimeSnapshot {
    fn has_recent_violation(&self, now_ms: f64) -> bool {
        self.last_non_monotonic_timestamp_ms
            .map(|ts| {
                now_ms.is_finite()
                    && now_ms >= ts
                    && now_ms - ts <= MONOTONIC_ANOMALY_RECENT_WINDOW_MS
            })
            .unwrap_or(false)
    }
}

impl McpFeedRuntimeState {
    fn record_pipeline_lock_sample(&self, contended: bool, observed_at_ms: u64) {
        self.pipeline_lock_contended_now
            .store(contended, Ordering::Release);
        if contended {
            self.pipeline_last_contended_wall_ms
                .store(observed_at_ms, Ordering::Release);
        }
    }

    fn pipeline_lock_recently_contended(&self, now_ms: u64) -> bool {
        if self.pipeline_lock_contended_now.load(Ordering::Acquire) {
            return true;
        }
        let last_contended = self.pipeline_last_contended_wall_ms.load(Ordering::Acquire);
        last_contended > 0
            && now_ms.saturating_sub(last_contended) <= PIPELINE_CONTENTION_RECENT_WINDOW_MS
    }

    fn record_non_monotonic_tick(&self, kind: MonotonicTimestampViolationKind, timestamp_ms: f64) {
        self.skipped_non_monotonic_ticks
            .fetch_add(1, Ordering::AcqRel);
        match kind {
            MonotonicTimestampViolationKind::EqualTimestamp => {
                self.duplicate_timestamp_ticks
                    .fetch_add(1, Ordering::AcqRel);
            }
            MonotonicTimestampViolationKind::BackwardTimestamp => {
                self.backward_timestamp_ticks.fetch_add(1, Ordering::AcqRel);
            }
        }
        self.last_non_monotonic_tick_ms_bits
            .store(tick_ms_to_bits(timestamp_ms), Ordering::Release);
    }

    fn monotonicity_snapshot(&self) -> MonotonicRuntimeSnapshot {
        MonotonicRuntimeSnapshot {
            skipped_non_monotonic_ticks: self.skipped_non_monotonic_ticks.load(Ordering::Acquire),
            duplicate_timestamp_ticks: self.duplicate_timestamp_ticks.load(Ordering::Acquire),
            backward_timestamp_ticks: self.backward_timestamp_ticks.load(Ordering::Acquire),
            last_non_monotonic_timestamp_ms: tick_ms_from_bits(
                self.last_non_monotonic_tick_ms_bits.load(Ordering::Acquire),
            ),
        }
    }
}

fn tick_ms_to_bits(ts: f64) -> u64 {
    if ts.is_finite() && ts > 0.0 {
        ts.to_bits()
    } else {
        0
    }
}

fn tick_ms_from_bits(bits: u64) -> Option<f64> {
    if bits == 0 {
        None
    } else {
        let v = f64::from_bits(bits);
        if v.is_finite() && v > 0.0 {
            Some(v)
        } else {
            None
        }
    }
}

/// Coherent live market view for MCP tools (Sierra `.scid` + optional `.depth`).
struct LiveMarketResolution {
    snapshot: serde_json::Value,
    snapshot_source: &'static str,
    dom_summary: Option<serde_json::Value>,
    dom_source: &'static str,
    as_of_timestamp_ms: f64,
    pipeline_processed_through_ms: Option<f64>,
    latest_db_tick_timestamp_ms: Option<f64>,
    latest_depth_timestamp_ms: Option<f64>,
    data_age_ms: f64,
    degradation_reason: Option<String>,
    pipelines_contended: bool,
    db_contended: bool,
}

impl LiveMarketResolution {
    fn freshness_status(&self) -> &'static str {
        if self.pipelines_contended {
            return "contended";
        }
        if !self.data_age_ms.is_finite() || self.data_age_ms < 0.0 {
            return "unknown";
        }
        if self.data_age_ms <= FRESHNESS_THRESHOLD_MS {
            "ok"
        } else {
            "stale"
        }
    }
}

fn merge_tool_live_metadata(target: &mut serde_json::Value, r: &LiveMarketResolution) {
    if let Some(obj) = target.as_object_mut() {
        obj.insert("liveDataSource".to_string(), serde_json::json!("scid"));
        obj.insert(
            "snapshotSource".to_string(),
            serde_json::json!(r.snapshot_source),
        );
        obj.insert("domSource".to_string(), serde_json::json!(r.dom_source));
        obj.insert(
            "asOfTimestampMs".to_string(),
            serde_json::json!(r.as_of_timestamp_ms),
        );
        obj.insert(
            "pipelineProcessedThroughMs".to_string(),
            serde_json::json!(r.pipeline_processed_through_ms),
        );
        obj.insert(
            "latestDbTickTimestampMs".to_string(),
            serde_json::json!(r.latest_db_tick_timestamp_ms),
        );
        obj.insert(
            "latestDepthTimestampMs".to_string(),
            serde_json::json!(r.latest_depth_timestamp_ms),
        );
        obj.insert("dataAgeMs".to_string(), serde_json::json!(r.data_age_ms));
        obj.insert(
            "freshnessStatus".to_string(),
            serde_json::json!(r.freshness_status()),
        );
        if let Some(ref reason) = r.degradation_reason {
            obj.insert("degradationReason".to_string(), serde_json::json!(reason));
        }
        obj.insert(
            "freshnessThresholdMs".to_string(),
            serde_json::json!(FRESHNESS_THRESHOLD_MS),
        );
        obj.insert(
            "dbLockContended".to_string(),
            serde_json::json!(r.db_contended),
        );
    }
}

fn render_market_snapshot_payload(r: &LiveMarketResolution) -> serde_json::Value {
    let snap = r.snapshot.clone();
    let top_dom = r
        .dom_summary
        .clone()
        .or_else(|| snap.get("domSummary").cloned());
    let snapshot_available = !snap.is_null();
    let mut out = serde_json::json!({
        "snapshot": snap,
        "domSummary": top_dom,
        "snapshotAvailable": snapshot_available,
    });
    if !snapshot_available {
        out["message"] = serde_json::json!(
            "Current market snapshot is temporarily unavailable while live pipeline contention is active. Retry shortly."
        );
    }
    merge_tool_live_metadata(&mut out, r);
    out
}

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
    feed_runtime: Arc<McpFeedRuntimeState>,
    runtime_events: Arc<RuntimeEventStore>,
    playbook_cache: Arc<PlaybookRuntimeCache>,
    backfill_manager: Arc<AsyncMutex<BackfillManager>>,
    options_cache: Arc<AsyncMutex<OptionsSnapshotCache>>,
    contract_cache: Arc<Mutex<ContractResolutionCache>>,
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

#[derive(Debug, Default)]
struct OptionsSnapshotCache {
    snapshot: Option<OptionsSnapshot>,
}

#[derive(Debug, Clone)]
struct CachedContractResolution {
    config: FeedConfig,
    contract: ContractMetadata,
    refreshed_at: Instant,
}

#[derive(Debug, Default)]
struct ContractResolutionCache {
    cached: Option<CachedContractResolution>,
}

#[derive(Debug, Default)]
struct PlaybookRuntimeCache {
    active_setups: RwLock<Arc<Vec<SetupDefinition>>>,
    risk_at_limit: AtomicBool,
}

impl PlaybookRuntimeCache {
    fn snapshot(&self) -> (Arc<Vec<SetupDefinition>>, bool) {
        let setups = match self.active_setups.read() {
            Ok(guard) => Arc::clone(&guard),
            Err(poisoned) => Arc::clone(&poisoned.into_inner()),
        };
        let risk_at_limit = self.risk_at_limit.load(Ordering::Acquire);
        (setups, risk_at_limit)
    }

    fn replace_active_setups(&self, setups: Vec<SetupDefinition>) {
        let replacement = Arc::new(setups);
        match self.active_setups.write() {
            Ok(mut guard) => {
                *guard = replacement;
            }
            Err(poisoned) => {
                let mut guard = poisoned.into_inner();
                *guard = replacement;
            }
        }
    }

    fn set_risk_at_limit(&self, at_limit: bool) {
        self.risk_at_limit.store(at_limit, Ordering::Release);
    }
}

fn db_error(e: impl std::fmt::Display) -> McpError {
    McpError::new(ErrorCode::INTERNAL_ERROR, e.to_string(), None)
}

fn lock_error() -> McpError {
    McpError::new(ErrorCode::INTERNAL_ERROR, "database lock poisoned", None)
}

fn freshness_status_from_age(age_ms: f64) -> &'static str {
    if age_ms < 0.0 || !age_ms.is_finite() {
        "unknown"
    } else if age_ms <= FRESHNESS_THRESHOLD_MS {
        "ok"
    } else {
        "stale"
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

fn runtime_event_json(event: RuntimeEvent, source: &str) -> serde_json::Value {
    let mut value = serde_json::to_value(event).unwrap_or_else(|_| serde_json::json!({}));
    if let Some(obj) = value.as_object_mut() {
        obj.insert("source".to_string(), serde_json::json!(source));
    }
    value
}

fn normalize_live_absorption_event(
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

fn normalize_db_absorption_event(row: &serde_json::Value) -> serde_json::Value {
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

fn no_data(msg: &str) -> CallToolResult {
    CallToolResult::success(vec![Content::text(msg.to_string())])
}

fn record_runtime_event(
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
fn record_runtime_event_scoped(
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

fn persist_runtime_event_if_enabled(
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

fn persist_runtime_event_in_db(
    runtime_events: &RuntimeEventStore,
    db: &Database,
    event: &RuntimeEvent,
) {
    if !runtime_events.persist_runtime_events() {
        return;
    }
    let _ = db.insert_runtime_event(event);
}

fn prune_runtime_events_if_enabled(runtime_events: &RuntimeEventStore, db: &Database) {
    if runtime_events.persist_runtime_events() {
        let _ = db.prune_runtime_events(
            runtime_events.retention_days(),
            runtime_events.max_persisted_rows(),
        );
    }
}

fn spawn_runtime_event_pruner(runtime_events: Arc<RuntimeEventStore>, db: Arc<Mutex<Database>>) {
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

fn spawn_attention_periodic_pulse(
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

fn resolve_session_id(
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

fn infer_session_type_label(timestamp_ms: f64) -> String {
    match et_minutes_from_timestamp(timestamp_ms)
        .map(classify_session)
        .unwrap_or(SessionType::Unknown)
    {
        SessionType::Rth => "rth".to_string(),
        SessionType::Globex => "globex".to_string(),
        SessionType::Unknown => "unknown".to_string(),
    }
}

fn parse_import_timestamp(raw: &str, timezone: Tz) -> Result<f64, McpError> {
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
struct FillSlice {
    timestamp_ms: f64,
    price: f64,
    quantity: i64,
    symbol: String,
    trade_account: Option<String>,
    batch_id: String,
    fingerprint: String,
    order_side: String,
    open_close: Option<String>,
    service_order_id: Option<String>,
    external_order_id: Option<String>,
    raw_payload: serde_json::Value,
}

#[derive(Debug, Clone)]
struct ActiveImportedTrade {
    session_id: Option<String>,
    instrument: String,
    trade_account: Option<String>,
    direction: String,
    entry_start_ms: f64,
    last_exit_ms: f64,
    signed_position: i64,
    entry_qty_total: i64,
    exit_qty_total: i64,
    max_open_size: i64,
    weighted_entry_notional: f64,
    weighted_exit_notional: f64,
    fill_refs: Vec<FillSlice>,
}

fn signed_delta_for_fill(side: &str, quantity: i64) -> Result<i64, McpError> {
    match side.to_ascii_lowercase().as_str() {
        "buy" => Ok(quantity),
        "sell" => Ok(-quantity),
        other => Err(invalid_params_error(format!(
            "unsupported buy/sell value `{other}`"
        ))),
    }
}

fn build_imported_trade_record(
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
        notes: notes.to_string(),
        source: source.to_string(),
    }
}

const TAPE_PACE_RESPONSE_KEYS: &[&str] = &[
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

fn build_tape_pace_response(
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

fn invalid_params_error(msg: impl Into<String>) -> McpError {
    McpError::new(ErrorCode::INVALID_PARAMS, msg.into(), None)
}

fn normalize_options_root(
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

fn normalize_options_exps(requested_exps: Option<Vec<u32>>, default_exps: &[u32]) -> Vec<u32> {
    let mut exps = requested_exps.unwrap_or_else(|| default_exps.to_vec());
    exps.sort_unstable();
    exps.dedup();
    exps
}

fn options_cache_metadata(snapshot: &OptionsSnapshot, refreshed: bool) -> serde_json::Value {
    let now_ms = Utc::now().timestamp_millis() as f64;
    serde_json::json!({
        "fetchedAtMs": snapshot.fetched_at_ms,
        "snapshotAgeMs": snapshot.age_ms(now_ms),
        "cacheTtlMs": snapshot.cache_ttl_ms,
        "cacheStatus": if refreshed { "refreshed" } else { "hit" },
    })
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

fn dom_summary_from_payload(payload: &serde_json::Value) -> Option<DomSummary> {
    payload
        .get("domSummary")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
}

fn activity_from_payload(payload: &serde_json::Value) -> Option<PullStackActivitySummary> {
    payload
        .get("activity")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
}

fn dom_summaries_from_rows(rows: &[(f64, serde_json::Value)]) -> Vec<DomSummary> {
    rows.iter()
        .filter_map(|(_, payload)| dom_summary_from_payload(payload))
        .collect()
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
    /// Filter to a specific root symbol (e.g. NQ) across contract rolls.
    #[serde(alias = "root_symbol")]
    root_symbol: Option<String>,
    /// Filter to a specific contract symbol (e.g. NQM26.CME).
    #[serde(alias = "contract_symbol")]
    contract_symbol: Option<String>,
    /// Include sessions flagged as roll-boundary carry-forward mismatches. Default true.
    #[serde(alias = "include_rollover_sessions", default = "default_true")]
    include_rollover_sessions: bool,
    /// Treat matching root-symbol sessions as a continuous research stream. Default false.
    #[serde(alias = "continuous_mode", default)]
    continuous_mode: bool,
}

fn default_true() -> bool {
    true
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

fn validate_ymd_range(
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

fn parse_non_empty_string(label: &str, raw: &str) -> Result<String, McpError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(invalid_params_error(format!("{label} must not be empty")));
    }
    Ok(trimmed.to_string())
}

fn parse_optional_non_empty_string(
    label: &str,
    raw: Option<&str>,
) -> Result<Option<String>, McpError> {
    raw.map(|value| parse_non_empty_string(label, value))
        .transpose()
}

fn parse_allowed_lowercase_value(
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

fn parse_scope_value(
    scope: Option<serde_json::Value>,
) -> Result<Option<SessionScopeFilter>, McpError> {
    let Some(scope) = scope else {
        return Ok(None);
    };
    let parsed: SessionScopeParams = serde_json::from_value(scope)
        .map_err(|e| invalid_params_error(format!("invalid scope payload: {e}")))?;
    build_session_scope_filter(&parsed)
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

fn parse_research_event_type(raw: &str) -> Result<String, McpError> {
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

fn parse_research_outcome_field(raw: &str) -> Result<String, McpError> {
    parse_allowed_lowercase_value("outcomeField", raw, RESEARCH_OUTCOME_FIELDS)
}

fn parse_distribution_metric(raw: &str) -> Result<String, McpError> {
    parse_allowed_lowercase_value("metric", raw, RESEARCH_DISTRIBUTION_METRICS)
}

fn parse_signal_outcome_session_field(raw: &str) -> Result<String, McpError> {
    parse_allowed_lowercase_value("sessionField", raw, SIGNAL_OUTCOME_SESSION_FIELDS)
}

fn parse_dom_behavior_name(raw: &str) -> Result<String, McpError> {
    parse_allowed_lowercase_value("behavior", raw, DOM_BEHAVIOR_NAMES)
}

fn parse_research_min_count(value: Option<i64>) -> Result<i64, McpError> {
    let min_count = value.unwrap_or(1);
    if !(1..=MAX_RESEARCH_MIN_COUNT).contains(&min_count) {
        return Err(invalid_params_error(format!(
            "minCount must be between 1 and {MAX_RESEARCH_MIN_COUNT}, got: {min_count}"
        )));
    }
    Ok(min_count)
}

fn parse_nonnegative_i64(
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

fn parse_bounded_limit(
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

fn parse_dom_behavior_min_duration(value: Option<f64>) -> Result<f64, McpError> {
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
#[serde(rename_all = "camelCase")]
struct OptionsSnapshotParams {
    /// Optional root symbol. Defaults to [options].convexvalue_probe_root.
    root: Option<String>,
    /// Optional expiration selectors accepted by ConvexValue.
    exps: Option<Vec<u32>>,
    /// Optional spot-relative range filter (for example 0.10 for +/-10%).
    range: Option<f64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct GammaLevelsParams {
    /// Optional root symbol. Defaults to [options].convexvalue_probe_root.
    root: Option<String>,
    /// Optional expiration selectors accepted by ConvexValue.
    exps: Option<Vec<u32>>,
    /// Optional spot-relative range filter (for example 0.10 for +/-10%).
    range: Option<f64>,
    /// Maximum number of strikes to return (default 12, max 50).
    top: Option<u64>,
    /// Force a network refresh instead of serving a warm cache hit.
    force_refresh: Option<bool>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct OptionsContextParams {
    /// Optional root symbol. Defaults to [options].convexvalue_probe_root.
    root: Option<String>,
    /// Optional expiration selectors accepted by ConvexValue.
    exps: Option<Vec<u32>>,
    /// Optional spot-relative range filter (for example 0.10 for +/-10%).
    range: Option<f64>,
    /// Force a network refresh instead of serving a warm cache hit.
    force_refresh: Option<bool>,
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
    /// Optional root-symbol filter (e.g. NQ).
    root_symbol: Option<String>,
    /// Optional contract-symbol filter (e.g. NQM26.CME).
    contract_symbol: Option<String>,
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
    include_aggregate: Option<bool>,
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
#[serde(rename_all = "camelCase")]
struct DomRegimeSummaryParams {
    timestamp_ms: Option<f64>,
    start_time_ms: Option<f64>,
    end_time_ms: Option<f64>,
    window_ms: Option<f64>,
    limit: Option<usize>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct DomBehaviorFrequencyParams {
    behavior: String,
    min_duration_ms: Option<f64>,
    start_date: Option<String>,
    end_date: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct DomBehaviorConditionalParams {
    behavior: String,
    setup_id: Option<String>,
    min_duration_ms: Option<f64>,
    start_date: Option<String>,
    end_date: Option<String>,
    #[schemars(schema_with = "schemars_optional_loose_object")]
    scope: Option<serde_json::Value>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct DomReactionAtLevelsParams {
    event_type: String,
    behavior: String,
    min_duration_ms: Option<f64>,
    start_date: Option<String>,
    end_date: Option<String>,
    #[schemars(schema_with = "schemars_optional_loose_object")]
    scope: Option<serde_json::Value>,
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

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct RawTickIngestGapParams {
    /// Optional start of clip window (YYYY-MM-DD, ET midnight).
    #[serde(alias = "start_date")]
    start_date: Option<String>,
    /// Optional end of clip window (YYYY-MM-DD, exclusive at next midnight).
    #[serde(alias = "end_date")]
    end_date: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct IngestRawTicksParams {
    #[serde(alias = "start_date")]
    start_date: Option<String>,
    #[serde(alias = "end_date")]
    end_date: Option<String>,
    /// When true (default), only SCID windows missing from raw_ticks for this contract.
    #[serde(alias = "only_gaps")]
    only_gaps: Option<bool>,
    #[serde(alias = "wait_for_completion")]
    wait_for_completion: Option<bool>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ScanScidTimestampAnomaliesParams {
    #[serde(alias = "start_date")]
    start_date: Option<String>,
    #[serde(alias = "end_date")]
    end_date: Option<String>,
    max_events_reported: Option<usize>,
    persist_result: Option<bool>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct RuntimeEventsParams {
    /// Maximum events to return (default 50, max 500).
    limit: Option<usize>,
    /// Only include events emitted at or after this Unix epoch millisecond timestamp.
    since_ms: Option<f64>,
    /// Exact level filter: trace, debug, info, warn, or error. Do not combine with minLevel.
    level: Option<String>,
    /// Minimum level filter: returns events at this level or higher. Prefer this for post-mortems; mutually exclusive with level.
    min_level: Option<String>,
    /// Exact category filter, e.g. scid, session, setup, depth, historical_job.
    category: Option<String>,
    /// Exact stable event name filter, e.g. scid.tail_reset.
    event_name: Option<String>,
    /// Include persisted SQLite events in addition to the in-memory ring buffer.
    include_persisted: Option<bool>,
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

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct StartTradingSessionParams {
    session_id: Option<String>,
    session_type: Option<String>,
    start_time_ms: Option<f64>,
    pre_session_note: Option<String>,
    recording_path: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct EndTradingSessionParams {
    session_id: Option<String>,
    end_time_ms: Option<f64>,
    recording_path: Option<String>,
    session_note: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct UpsertTradeEntryParams {
    id: Option<String>,
    session_id: Option<String>,
    setup_id: Option<String>,
    instrument: Option<String>,
    trade_account: Option<String>,
    entry_time_ms: Option<f64>,
    entry_price: f64,
    exit_time_ms: Option<f64>,
    exit_price: Option<f64>,
    direction: String,
    size: i64,
    max_open_size: Option<i64>,
    stop_price: Option<f64>,
    target_prices: Option<Vec<f64>>,
    result_r: Option<f64>,
    gross_points: Option<f64>,
    planned: Option<bool>,
    rules_followed: Option<bool>,
    emotional_state: Option<String>,
    thesis: Option<String>,
    review_tags: Option<Vec<String>>,
    mistake_tags: Option<Vec<String>>,
    entry_fill_count: Option<i64>,
    exit_fill_count: Option<i64>,
    import_batch_id: Option<String>,
    notes: Option<String>,
    source: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct CloseTradeEntryParams {
    id: String,
    exit_price: f64,
    exit_time_ms: Option<f64>,
    result_r: Option<f64>,
    gross_points: Option<f64>,
    notes: Option<String>,
    update_risk_state: Option<bool>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ReviewTradeEntryParams {
    id: String,
    planned: bool,
    rules_followed: Option<bool>,
    emotional_state: Option<String>,
    thesis: Option<String>,
    review_tags: Option<Vec<String>>,
    mistake_tags: Option<Vec<String>>,
    notes: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct SaveJournalEntryParams {
    id: Option<String>,
    session_id: Option<String>,
    date: Option<String>,
    content: String,
    tags: Option<Vec<String>>,
    setup_references: Option<Vec<String>>,
    trade_references: Option<Vec<String>>,
    created_at_ms: Option<f64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct TradeListParams {
    session_id: Option<String>,
    limit: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct TradeEntryIdParams {
    id: String,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct SessionJournalParams {
    session_id: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct RecentJournalNotesParams {
    limit: Option<u64>,
    tag: Option<String>,
    setup_reference: Option<String>,
    trade_reference: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct SessionReviewContextParams {
    session_id: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct JournalPatternParams {
    start_date: Option<String>,
    end_date: Option<String>,
    session_type: Option<String>,
    limit: Option<u64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct SaveAgentInsightParams {
    id: Option<String>,
    session_id: Option<String>,
    trade_id: Option<String>,
    setup_id: Option<String>,
    category: String,
    summary: String,
    #[schemars(schema_with = "schemars_loose_object")]
    evidence: serde_json::Value,
    tags: Option<Vec<String>>,
    #[schemars(schema_with = "schemars_optional_loose_object")]
    scope: Option<serde_json::Value>,
    confidence: Option<f64>,
    salience: Option<f64>,
    source: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct RecallAgentInsightsParams {
    category: Option<String>,
    setup_id: Option<String>,
    statuses: Option<Vec<String>>,
    tag: Option<String>,
    session_type: Option<String>,
    session_segment: Option<String>,
    time_bucket: Option<String>,
    day_type: Option<String>,
    start_date: Option<String>,
    end_date: Option<String>,
    limit: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct InsightAcknowledgeParams {
    id: String,
    action: String,
    surfaced_at_ms: Option<f64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct SupersedeInsightParams {
    previous_id: String,
    replacement_id: String,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct BehavioralPatternMemoryParams {
    pattern_type: Option<String>,
    session_type: Option<String>,
    session_segment: Option<String>,
    time_bucket: Option<String>,
    day_type: Option<String>,
    setup_id: Option<String>,
    min_sample_size: Option<i64>,
    active_only: Option<bool>,
    limit: Option<u64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct CreateMemoryFollowupParams {
    id: Option<String>,
    session_id: Option<String>,
    trade_id: Option<String>,
    source: Option<String>,
    title: String,
    detail: Option<String>,
    tags: Option<Vec<String>>,
    #[schemars(schema_with = "schemars_optional_loose_object")]
    due_context: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ResolveMemoryFollowupParams {
    id: String,
    resolution_note: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct SetupStateHistoryParams {
    setup_id: Option<String>,
    session_date: Option<String>,
    minutes: Option<f64>,
    limit: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct SetupLifecycleParams {
    setup_id: String,
}

#[derive(Debug, Default, Clone, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct AttentionCursorParams {
    #[serde(alias = "last_signal_id")]
    last_signal_id: Option<String>,
    #[serde(alias = "last_event_id")]
    last_event_id: Option<String>,
    #[serde(alias = "since_ms")]
    since_ms: Option<f64>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct AttentionInboxParams {
    status: Option<String>,
    #[serde(alias = "min_priority")]
    min_priority: Option<String>,
    limit: Option<usize>,
    #[serde(alias = "include_expired")]
    include_expired: Option<bool>,
    cursor: Option<AttentionCursorParams>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct AttentionSignalDetailParams {
    #[serde(alias = "signal_id")]
    signal_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct AttentionSignalAcknowledgeParams {
    #[serde(alias = "signal_id")]
    signal_id: String,
    #[serde(alias = "acknowledged_by")]
    acknowledged_by: String,
    note: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct WhatChangedSinceParams {
    cursor: Option<AttentionCursorParams>,
    limit: Option<usize>,
    #[serde(alias = "include_signals")]
    include_signals: Option<bool>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct AttentionChangelogParams {
    cursor: Option<AttentionCursorParams>,
    limit: Option<usize>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ActiveTradeIdeasParams {
    status: Option<String>,
    #[serde(alias = "setup_id")]
    setup_id: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct TradeIdeaConfirmParams {
    #[serde(alias = "idea_id")]
    idea_id: String,
    #[serde(alias = "evidence_note")]
    evidence_note: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct TradeIdeaInvalidateParams {
    #[serde(alias = "idea_id")]
    idea_id: String,
    #[serde(alias = "reason_code")]
    reason_code: String,
    note: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct TradeIdeaInTradeParams {
    #[serde(alias = "idea_id")]
    idea_id: String,
    #[serde(alias = "signal_outcome_id")]
    signal_outcome_id: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct TradeIdeaResolveParams {
    #[serde(alias = "idea_id")]
    idea_id: String,
    outcome: String,
    note: Option<String>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct MemoryBriefParams {
    intent: Option<String>,
    session_id: Option<String>,
    setup_id: Option<String>,
    session_type: Option<String>,
    session_segment: Option<String>,
    day_type: Option<String>,
    time_bucket: Option<String>,
    pre_session_note: Option<String>,
    limit: Option<u64>,
    include_recent_sessions: Option<bool>,
    include_patterns: Option<bool>,
    include_insights: Option<bool>,
    include_followups: Option<bool>,
    /// When true, `get_pre_session_briefing` skips the bounded automatic
    /// `refresh_memory_state` pass even if memory maintenance is dirty.
    skip_memory_refresh_if_dirty: Option<bool>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct RefreshMemoryStateParams {
    refresh_patterns: Option<bool>,
    refresh_insight_lifecycle: Option<bool>,
    include_patterns: Option<bool>,
    reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize, serde::Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ImportedFillRowInput {
    entry_time: String,
    last_activity_time: Option<String>,
    symbol: String,
    status: String,
    internal_order_id: Option<String>,
    order_type: Option<String>,
    buy_sell: String,
    open_close: Option<String>,
    order_quantity: Option<i64>,
    price: Option<f64>,
    filled_quantity: Option<i64>,
    average_fill_price: f64,
    parent_internal_order_id: Option<String>,
    service_order_id: Option<String>,
    trade_account: Option<String>,
    exchange_order_id: Option<String>,
    text_tag: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ImportTradeFillsParams {
    rows: Vec<ImportedFillRowInput>,
    batch_id: Option<String>,
    session_id: Option<String>,
    source: Option<String>,
    timezone: Option<String>,
    notes: Option<String>,
}

#[tool_router]
impl TheDeskMcp {
    #[cfg(test)]
    fn new(db: Database, pipelines: PipelineEngine, db_path: String) -> Self {
        let logging_config = the_desk_backend::observability::LoggingConfig {
            destination: "none".to_string(),
            runtime_event_suppression_window_ms: 0,
            ..the_desk_backend::observability::LoggingConfig::default()
        };
        Self::with_runtime_events(
            db,
            pipelines,
            db_path,
            Arc::new(RuntimeEventStore::new(&logging_config)),
        )
    }

    fn with_runtime_events(
        db: Database,
        pipelines: PipelineEngine,
        db_path: String,
        runtime_events: Arc<RuntimeEventStore>,
    ) -> Self {
        Self {
            db: Arc::new(Mutex::new(db)),
            db_path: Arc::new(db_path),
            pipelines: Arc::new(Mutex::new(pipelines)),
            detector: Arc::new(Mutex::new(EventDetector::new())),
            flow_emitter: Arc::new(Mutex::new(FlowEventEmitter::new())),
            rules: Arc::new(Mutex::new(RulesEngine::default())),
            last_bid: Arc::new(Mutex::new(0.0)),
            last_ask: Arc::new(Mutex::new(0.0)),
            feed_runtime: Arc::new(McpFeedRuntimeState::default()),
            runtime_events,
            playbook_cache: Arc::new(PlaybookRuntimeCache::default()),
            backfill_manager: Arc::new(AsyncMutex::new(BackfillManager::default())),
            options_cache: Arc::new(AsyncMutex::new(OptionsSnapshotCache::default())),
            contract_cache: Arc::new(Mutex::new(ContractResolutionCache::default())),
            tool_router: Self::tool_router(),
        }
    }

    fn resolve_contract_cached(&self) -> (FeedConfig, ContractMetadata) {
        if let Ok(mut cache) = self.contract_cache.lock() {
            if let Some(cached) = cache.cached.as_ref() {
                if cached.refreshed_at.elapsed().as_millis() <= CONTRACT_RESOLUTION_CACHE_TTL_MS {
                    return (cached.config.clone(), cached.contract.clone());
                }
            }
            let config = load_feed_config();
            let contract = resolve_contract_metadata(&config);
            cache.cached = Some(CachedContractResolution {
                config: config.clone(),
                contract: contract.clone(),
                refreshed_at: Instant::now(),
            });
            (config, contract)
        } else {
            let config = load_feed_config();
            let contract = resolve_contract_metadata(&config);
            (config, contract)
        }
    }

    fn refresh_playbook_setups_from_db(
        &self,
        db: &Database,
    ) -> Result<bool, the_desk_backend::db::DbError> {
        let (active_setups, risk_at_limit) = db.load_playbook_runtime_seed()?;
        self.playbook_cache.replace_active_setups(active_setups);
        Ok(risk_at_limit)
    }

    fn hydrate_playbook_runtime_cache(&self) -> Result<(), McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let risk_at_limit = self
            .refresh_playbook_setups_from_db(&db)
            .map_err(db_error)?;
        let session_date =
            trading_day_from_timestamp_ms(chrono::Utc::now().timestamp_millis() as f64);
        let runtime_rows = db
            .load_setup_runtime_state_for_session(&session_date)
            .map_err(db_error)?;
        let snapshots: Vec<SetupRuntimeSnapshot> = runtime_rows
            .into_iter()
            .map(|row| SetupRuntimeSnapshot {
                setup_id: row.setup_id,
                setup_name: row.setup_name,
                state: row.state,
                readiness: row.readiness,
                readiness_score: row.readiness_score,
                met_conditions: row.met_conditions,
                missing_conditions: row.missing_conditions,
                met_count: row.met_count.max(0) as usize,
                total_count: row.total_count.max(0) as usize,
                deterministic_all_met: row.deterministic_all_met,
                requires_discretionary: row.requires_discretionary,
                current_price: row.current_price,
                last_evaluated_at_ms: row.last_evaluated_at_ms,
                last_transition_at_ms: row.last_transition_at_ms,
                last_alert_emitted_at_ms: row.last_alert_emitted_at_ms,
                source: row.source,
            })
            .collect();
        if let Ok(mut rules) = self.rules.lock() {
            rules.rehydrate(snapshots);
        }
        self.feed_runtime
            .setup_runtime_rehydrated
            .store(true, Ordering::Release);
        self.playbook_cache.set_risk_at_limit(risk_at_limit);
        Ok(())
    }

    fn current_pipeline_contract_metadata(
        &self,
    ) -> Option<the_desk_backend::feed::ContractMetadata> {
        self.pipelines
            .lock()
            .ok()
            .map(|pipelines| pipelines.contract_metadata().clone())
    }

    fn rollover_status_for_date(
        &self,
        db: &Database,
        active_contract: &the_desk_backend::feed::ContractMetadata,
        server_contract: Option<&the_desk_backend::feed::ContractMetadata>,
        before_date: &str,
        data_age_ms: Option<f64>,
    ) -> Result<ContractRolloverStatus, McpError> {
        let status = build_rollover_status_from_db(
            db,
            active_contract,
            server_contract,
            before_date,
            data_age_ms,
        )
        .map_err(db_error)?;
        if status.status != the_desk_backend::rollover::ContractRolloverStatusKind::Ok {
            let event = RuntimeEvent::new(
                RuntimeEventLevel::Warn,
                "rollover.status_evaluated",
                "rollover",
                "Contract rollover status is not OK.",
                serde_json::json!({
                    "status": status.status,
                    "agentAction": status.agent_action,
                    "priorReferenceTrust": status.prior_reference_trust,
                    "activeContract": status.active_contract_symbol,
                    "beforeDate": before_date,
                }),
            );
            if let Some(recorded) = self.runtime_events.record(event) {
                persist_runtime_event_in_db(&self.runtime_events, db, &recorded);
            }
        }
        Ok(status)
    }

    fn persist_manual_setup_state_change(
        &self,
        setup_id: &str,
        before: Option<SetupRuntimeSnapshot>,
        after: SetupRuntimeSnapshot,
        reason: &str,
        timestamp_ms: f64,
    ) -> Result<(), McpError> {
        let session_date = trading_day_from_timestamp_ms(timestamp_ms);
        let (root_symbol, contract_symbol, current_price) =
            if let Ok(pipelines) = self.pipelines.try_lock() {
                let (bid, ask) = current_best_bid_ask(&self.last_bid, &self.last_ask);
                let snap = pipelines.snapshot(bid, ask);
                (snap.root_symbol, snap.contract_symbol, snap.last_price)
            } else {
                (String::new(), String::new(), after.current_price)
            };
        let transition = SetupTransition {
            setup_id: setup_id.to_string(),
            setup_name: after
                .setup_name
                .clone()
                .unwrap_or_else(|| setup_id.to_string()),
            previous_state: before
                .as_ref()
                .map(|s| s.state.clone())
                .unwrap_or(SetupState::NotActive),
            next_state: after.state.clone(),
            previous_readiness: before
                .as_ref()
                .map(|s| s.readiness.clone())
                .unwrap_or(the_desk_backend::rules::SetupReadiness::Inactive),
            next_readiness: after.readiness.clone(),
            readiness_score: after.readiness_score,
            met_count: after.met_count,
            total_count: after.total_count,
            met_conditions: after.met_conditions.clone(),
            missing_conditions: after.missing_conditions.clone(),
            deterministic_all_met: after.deterministic_all_met,
            requires_discretionary: after.requires_discretionary,
            current_price,
            timestamp_ms,
            reason: reason.to_string(),
            alert_emitted: false,
            last_alert_emitted_at_ms: after.last_alert_emitted_at_ms,
        };
        let db = self.db.lock().map_err(|_| lock_error())?;
        db.insert_setup_state_transition(
            &transition,
            &session_date,
            (!root_symbol.is_empty()).then_some(root_symbol.as_str()),
            (!contract_symbol.is_empty()).then_some(contract_symbol.as_str()),
            "manual",
        )
        .map_err(db_error)?;
        let record = runtime_record_from_snapshot(
            after,
            &session_date,
            (!root_symbol.is_empty()).then_some(root_symbol.as_str()),
            (!contract_symbol.is_empty()).then_some(contract_symbol.as_str()),
            "manual",
        );
        db.upsert_setup_runtime_state(&record).map_err(db_error)?;
        drop(db);
        record_runtime_event_scoped(
            &self.runtime_events,
            Some(&self.db),
            RuntimeEventLevel::Info,
            "setup.transition",
            "setup",
            "Manual setup lifecycle transition persisted.",
            serde_json::json!({
                "setupId": setup_id,
                "setupName": transition.setup_name,
                "previousState": transition.previous_state,
                "nextState": transition.next_state,
                "previousReadiness": transition.previous_readiness,
                "nextReadiness": transition.next_readiness,
                "reason": reason,
                "currentPrice": current_price,
                "source": "manual",
            }),
            Some(session_date),
            (!root_symbol.is_empty()).then_some(root_symbol),
            (!contract_symbol.is_empty()).then_some(contract_symbol),
        );
        Ok(())
    }

    fn transition_trade_idea_tool(
        &self,
        idea_id: &str,
        lifecycle: &str,
        status: &str,
        note: Option<&str>,
    ) -> Result<CallToolResult, McpError> {
        let timestamp_ms = chrono::Utc::now().timestamp_millis() as f64;
        let (changed, idea, linked_signal) = {
            let db = self.db.lock().map_err(|_| lock_error())?;
            let before = db.get_trade_idea_card(idea_id).map_err(db_error)?;
            db.transition_trade_idea(idea_id, lifecycle, status, note, timestamp_ms)
                .map_err(db_error)?;
            let idea = db.get_trade_idea_card(idea_id).map_err(db_error)?;
            let linked_signal = if matches!(lifecycle, "invalidated" | "resolved") {
                before
                    .as_ref()
                    .and_then(|idea| idea.linked_attention_signal_id.as_deref())
                    .or_else(|| {
                        idea.as_ref()
                            .and_then(|idea| idea.linked_attention_signal_id.as_deref())
                    })
                    .map(|signal_id| {
                        let (signal_status, event_type) = if lifecycle == "invalidated" {
                            ("invalidated", "invalidated")
                        } else {
                            ("acknowledged", "acknowledged")
                        };
                        db.update_attention_signal_status(
                            signal_id,
                            signal_status,
                            event_type,
                            Some("trade_idea"),
                            note,
                            timestamp_ms,
                        )
                    })
                    .transpose()
                    .map_err(db_error)?
                    .flatten()
            } else {
                None
            };
            let changed = idea
                .as_ref()
                .map(|idea| idea.updated_at_ms == timestamp_ms)
                .unwrap_or(false);
            (changed, idea, linked_signal)
        };
        if changed {
            let event_name = match lifecycle {
                "invalidated" => "attention.signal_invalidated",
                "resolved" => "attention.signal_acknowledged",
                _ => "attention.signal_emitted",
            };
            record_runtime_event(
                &self.runtime_events,
                Some(&self.db),
                RuntimeEventLevel::Info,
                event_name,
                "attention",
                "Trade idea lifecycle changed.",
                serde_json::json!({
                    "ideaId": idea_id,
                    "lifecycle": lifecycle,
                    "status": status,
                    "changed": changed,
                    "linkedSignalId": linked_signal.as_ref().map(|signal| signal.signal_id.clone()),
                    "note": note,
                }),
            );
        }
        Ok(text_result(serde_json::json!({
            "ideaId": idea_id,
            "lifecycle": lifecycle,
            "status": status,
            "changed": changed,
            "persisted": changed,
            "idea": idea,
            "linkedSignal": linked_signal
        })))
    }

    async fn get_or_refresh_options_snapshot(
        &self,
        root: Option<&str>,
        exps: Option<Vec<u32>>,
        range: Option<f64>,
        force_refresh: bool,
    ) -> Result<(OptionsSnapshot, bool), McpError> {
        let config = load_options_config();
        if !config.enabled {
            return Err(invalid_params_error(
                "options integration is disabled; set [options].enabled = true in ~/.the-desk/config.toml",
            ));
        }

        let root = normalize_options_root(root, &config.convexvalue_probe_root)?;
        let exps = normalize_options_exps(exps, &config.convexvalue_probe_exps);
        let range = range.or(config.convexvalue_probe_range);
        let now_ms = Utc::now().timestamp_millis() as f64;

        {
            let cache = self.options_cache.lock().await;
            if !force_refresh {
                if let Some(snapshot) = &cache.snapshot {
                    if snapshot.matches_request(
                        &root,
                        &exps,
                        range,
                        &config.convexvalue_probe_params,
                        &config.convexvalue_context_params,
                    ) && snapshot.is_fresh(now_ms)
                    {
                        return Ok((snapshot.clone(), false));
                    }
                }
            }
        }

        let credentials = OptionsCredentials::from_env(&config)
            .map_err(|e| invalid_params_error(e.to_string()))?;
        let snapshot = fetch_options_snapshot(
            &config,
            &credentials,
            &root,
            if exps.is_empty() {
                None
            } else {
                Some(exps.as_slice())
            },
            range,
        )
        .await
        .map_err(db_error)?;

        let mut cache = self.options_cache.lock().await;
        cache.snapshot = Some(snapshot.clone());
        Ok((snapshot, true))
    }

    /// Single coherent live view: in-memory pipeline when available, else persisted `feature_state`.
    fn resolve_live_market_view(&self) -> Option<LiveMarketResolution> {
        let now_wall_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let now_ms = now_wall_ms as f64;
        let atomic_ts = tick_ms_from_bits(
            self.feed_runtime
                .last_scid_tick_ms_bits
                .load(Ordering::Acquire),
        );
        let depth_atomic = tick_ms_from_bits(
            self.feed_runtime
                .last_depth_timestamp_ms_bits
                .load(Ordering::Acquire),
        );

        let db_guard = self.db.try_lock();
        let db_contended = db_guard.is_err();
        let (latest_db_tick, feature_with_ts, dom_state) = match db_guard {
            Ok(db) => (
                db.latest_tick_timestamp_ms().ok().flatten(),
                db.latest_feature_state_with_timestamp().ok().flatten(),
                db.latest_dom_feature_state().ok().flatten(),
            ),
            Err(_) => (None, None, None),
        };

        let pipelines_guard = self.pipelines.try_lock();
        let pipelines_contended = pipelines_guard.is_err();
        self.feed_runtime
            .record_pipeline_lock_sample(pipelines_contended, now_wall_ms);

        if let Ok(pipelines) = pipelines_guard {
            let bid = self.last_bid.lock().ok().map(|g| *g).unwrap_or(0.0);
            let ask = self.last_ask.lock().ok().map(|g| *g).unwrap_or(0.0);
            if bid > 0.0 || ask > 0.0 {
                if let Ok(snap_val) =
                    serde_json::to_value(pipelines.snapshot(bid.max(1e-9), ask.max(1e-9)))
                {
                    let tape_ts = snap_val
                        .get("tapeLastTradeTimestampMs")
                        .and_then(|v| v.as_f64())
                        .filter(|t| t.is_finite() && *t > 0.0);
                    let as_of = [atomic_ts, tape_ts, latest_db_tick]
                        .into_iter()
                        .flatten()
                        .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                        .unwrap_or(now_ms);
                    let data_age_ms = (now_ms - as_of).max(0.0);

                    let (dom_summary, dom_source, latest_depth_ts) =
                        if let Some(ds) = snap_val.get("domSummary") {
                            if ds.as_object().map(|o| !o.is_empty()).unwrap_or(false) {
                                let ts = ds.get("timestampMs").and_then(|v| v.as_f64());
                                (Some(ds.clone()), "depth_live", ts.or(depth_atomic))
                            } else if let Some((dts, pay)) = dom_state.as_ref() {
                                (
                                    pay.get("domSummary").cloned(),
                                    "persisted_dom_feature_state",
                                    Some(*dts).or(depth_atomic),
                                )
                            } else {
                                (None, "unavailable", depth_atomic)
                            }
                        } else if let Some((dts, pay)) = dom_state.as_ref() {
                            (
                                pay.get("domSummary").cloned(),
                                "persisted_dom_feature_state",
                                Some(*dts).or(depth_atomic),
                            )
                        } else {
                            (None, "unavailable", depth_atomic)
                        };

                    return Some(LiveMarketResolution {
                        snapshot: snap_val,
                        snapshot_source: "live_pipeline",
                        dom_summary,
                        dom_source,
                        as_of_timestamp_ms: as_of,
                        pipeline_processed_through_ms: atomic_ts.or(tape_ts),
                        latest_db_tick_timestamp_ms: latest_db_tick,
                        latest_depth_timestamp_ms: latest_depth_ts,
                        data_age_ms,
                        degradation_reason: None,
                        pipelines_contended: false,
                        db_contended,
                    });
                }
            }
        }

        if let Some((feat_ts, payload)) = feature_with_ts {
            let as_of = if feat_ts.is_finite() && feat_ts > 0.0 {
                feat_ts
            } else {
                latest_db_tick.unwrap_or(now_ms)
            };
            let data_age_ms = (now_ms - as_of).max(0.0);
            let (dom_summary, dom_source, latest_depth_ts) =
                if let Some((dts, pay)) = dom_state.as_ref() {
                    (
                        pay.get("domSummary").cloned(),
                        "persisted_dom_feature_state",
                        Some(*dts).or(depth_atomic),
                    )
                } else {
                    (None, "unavailable", depth_atomic)
                };
            let degradation_reason = if pipelines_contended {
                Some("pipeline_lock_contended; using persisted_feature_state".to_string())
            } else {
                None
            };
            return Some(LiveMarketResolution {
                snapshot: payload,
                snapshot_source: "persisted_feature_state",
                dom_summary,
                dom_source,
                as_of_timestamp_ms: as_of,
                pipeline_processed_through_ms: atomic_ts,
                latest_db_tick_timestamp_ms: latest_db_tick,
                latest_depth_timestamp_ms: latest_depth_ts,
                data_age_ms,
                degradation_reason,
                pipelines_contended,
                db_contended,
            });
        }

        None
    }

    /// Structured degraded snapshot metadata when the pipeline lock is contended before any
    /// readable market snapshot exists.
    fn resolve_market_snapshot_contention_gap(&self) -> Option<LiveMarketResolution> {
        let now_wall_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let now_ms = now_wall_ms as f64;
        let atomic_ts = tick_ms_from_bits(
            self.feed_runtime
                .last_scid_tick_ms_bits
                .load(Ordering::Acquire),
        );
        let depth_atomic = tick_ms_from_bits(
            self.feed_runtime
                .last_depth_timestamp_ms_bits
                .load(Ordering::Acquire),
        );

        let db_guard = self.db.try_lock();
        let db_contended = db_guard.is_err();
        let (latest_db_tick, dom_state) = match db_guard {
            Ok(db) => (
                db.latest_tick_timestamp_ms().ok().flatten(),
                db.latest_dom_feature_state().ok().flatten(),
            ),
            Err(_) => (None, None),
        };

        let pipelines_guard = self.pipelines.try_lock();
        let pipelines_contended = pipelines_guard.is_err();
        self.feed_runtime
            .record_pipeline_lock_sample(pipelines_contended, now_wall_ms);
        if !pipelines_contended {
            return None;
        }

        let latest_depth_ts = dom_state.as_ref().map(|(ts, _)| *ts).or(depth_atomic);
        let dom_summary = dom_state.as_ref().and_then(|(_, payload)| {
            payload
                .get("domSummary")
                .filter(|summary| !summary.is_null())
                .cloned()
        });
        let dom_source = if dom_summary.is_some() {
            "persisted_dom_feature_state"
        } else {
            "unavailable"
        };
        let known_as_of = [atomic_ts, latest_db_tick, latest_depth_ts]
            .into_iter()
            .flatten()
            .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let as_of = known_as_of.unwrap_or(now_ms);
        let data_age_ms = known_as_of.map(|ts| (now_ms - ts).max(0.0)).unwrap_or(-1.0);
        let degradation_reason = if db_contended {
            "pipeline_lock_contended; persisted_feature_state_unavailable_db_busy"
        } else {
            "pipeline_lock_contended; no_persisted_feature_state_available_yet"
        };

        Some(LiveMarketResolution {
            snapshot: serde_json::Value::Null,
            snapshot_source: "contention_unavailable",
            dom_summary,
            dom_source,
            as_of_timestamp_ms: as_of,
            pipeline_processed_through_ms: atomic_ts,
            latest_db_tick_timestamp_ms: latest_db_tick,
            latest_depth_timestamp_ms: latest_depth_ts,
            data_age_ms,
            degradation_reason: Some(degradation_reason.to_string()),
            pipelines_contended: true,
            db_contended,
        })
    }

    fn current_market_snapshot_payload(&self) -> Option<serde_json::Value> {
        self.resolve_live_market_view()
            .map(|r| render_market_snapshot_payload(&r))
            .or_else(|| {
                self.resolve_market_snapshot_contention_gap()
                    .map(|r| render_market_snapshot_payload(&r))
            })
    }

    fn data_age_from_db_or_atomic(&self) -> f64 {
        let now_ms = chrono::Utc::now().timestamp_millis() as f64;
        if let Some(ts) = tick_ms_from_bits(
            self.feed_runtime
                .last_scid_tick_ms_bits
                .load(Ordering::Acquire),
        ) {
            return (now_ms - ts).max(0.0);
        }
        if let Ok(db) = self.db.try_lock() {
            return compute_data_age(&db);
        }
        -1.0
    }

    /// Snapshot JSON for tools that only need market fields (compare_sessions, etc.).
    fn current_snapshot_value(&self) -> Option<serde_json::Value> {
        self.resolve_live_market_view()
            .map(|r| r.snapshot)
            .or_else(|| {
                self.db
                    .lock()
                    .ok()
                    .and_then(|d| d.latest_feature_state().ok().flatten())
            })
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
        let runtime_events = Arc::clone(&self.runtime_events);
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
                    record_runtime_event(
                        &runtime_events,
                        None,
                        RuntimeEventLevel::Error,
                        "historical_job.failed",
                        "historical_job",
                        "Historical job could not open SQLite.",
                        serde_json::json!({
                            "jobId": &job_id,
                            "jobType": worker_params.job_type.as_str(),
                            "error": err.to_string(),
                        }),
                    );
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

            let event = RuntimeEvent::new(
                RuntimeEventLevel::Info,
                "historical_job.started",
                "historical_job",
                "Historical job started.",
                serde_json::json!({
                    "jobId": &job_id,
                    "jobType": worker_params.job_type.as_str(),
                    "startedAtMs": started_at_ms,
                }),
            );
            if let Some(recorded) = runtime_events.record(event) {
                persist_runtime_event_in_db(&runtime_events, &db, &recorded);
            }
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
                let level = match state.run.status.as_str() {
                    "failed" => RuntimeEventLevel::Error,
                    "cancelled" => RuntimeEventLevel::Warn,
                    _ => RuntimeEventLevel::Info,
                };
                let event = RuntimeEvent::new(
                    level,
                    match state.run.status.as_str() {
                        "completed" => "historical_job.completed",
                        "cancelled" => "historical_job.cancelled",
                        "failed" => "historical_job.failed",
                        _ => "historical_job.finished",
                    },
                    "historical_job",
                    "Historical job finished.",
                    serde_json::json!({
                        "jobId": &job_id,
                        "jobType": worker_params.job_type.as_str(),
                        "status": &state.run.status,
                        "startedAtMs": state.run.started_at_ms,
                        "finishedAtMs": state.run.finished_at_ms,
                        "error": &state.run.error,
                        "warnings": &state.run.warnings,
                    }),
                );
                if let Some(recorded) = runtime_events.record(event) {
                    persist_runtime_event_in_db(&runtime_events, &db, &recorded);
                }
            }
            guard.active_job_id = None;
        });

        Ok((run, false))
    }

    #[tool(
        description = "Current market snapshot: last price, VWAP with 1/2/3 SD bands, TPO value area (high/low/POC), delta neutral value area (DNVA high/low/DNP), session delta, cumulative delta, key levels (prior day H/L/C, prior VA/POC, overnight range, OR, IB), Globex/London opening ranges, and session context (sessionType, sessionSegment, tradingDay), plus tape pace, imbalance count, absorption event count, and average trade size. Prefers live pipeline state; falls back to last persisted snapshot."
    )]
    async fn get_market_snapshot(&self) -> Result<CallToolResult, McpError> {
        if let Some(out) = self.current_market_snapshot_payload() {
            return Ok(text_result(out));
        }
        Ok(no_data(
            "No market snapshot available yet or database is temporarily busy. Ensure Sierra Chart is running and .scid data is being ingested.",
        ))
    }

    #[tool(
        description = "Current session context: sessionType (RTH/Globex/Unknown), sessionSegment (Asia/London/None), tradingDay (6 PM ET roll), data freshness, and contract rollover status."
    )]
    async fn get_session_context(&self) -> Result<CallToolResult, McpError> {
        if let Some(r) = self.resolve_live_market_view() {
            let s = &r.snapshot;
            let (_, active_contract) = self.resolve_contract_cached();
            let et_minutes = et_minutes_from_timestamp(r.as_of_timestamp_ms).unwrap_or(-1);
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
            let mut out = serde_json::json!({
                "sessionType": s.get("sessionType"),
                "sessionSegment": s.get("sessionSegment"),
                "tradingDay": s.get("tradingDay"),
                "rootSymbol": s.get("rootSymbol"),
                "contractSymbol": s.get("contractSymbol"),
                "contractMonth": s.get("contractMonth"),
                "symbolResolutionMode": s.get("symbolResolutionMode"),
                "symbolResolutionSource": s.get("symbolResolutionSource"),
                "rolloverWarning": s.get("rolloverWarning"),
                "carryForwardLevelsValid": s.get("carryForwardLevelsValid"),
                "isTransition": is_transition,
                "transitionFrom": transition_from,
                "transitionTo": transition_to,
                "transitionPhase": transition_phase,
                "etMinutes": et_minutes,
            });
            let rollover_date = s
                .get("tradingDay")
                .and_then(|v| v.as_str())
                .map(ToString::to_string)
                .unwrap_or_else(the_desk_backend::et_now_trading_day);
            let server_contract = self.current_pipeline_contract_metadata();
            let db = self.db.lock().map_err(|_| lock_error())?;
            let rollover_status = self.rollover_status_for_date(
                &db,
                &active_contract,
                server_contract.as_ref(),
                &rollover_date,
                Some(r.data_age_ms),
            )?;
            out["rolloverStatus"] =
                serde_json::to_value(rollover_status).unwrap_or_else(|_| serde_json::json!({}));
            merge_tool_live_metadata(&mut out, &r);
            return Ok(text_result(out));
        }
        Ok(no_data("No session context available"))
    }

    #[tool(
        description = "TPO (Time-Price-Opportunity) profile data: POC (point of control), value area high/low, opening range high/low (first 30 min), initial balance high/low (first 60 min). Use for auction market theory analysis."
    )]
    async fn get_tpo_profile(&self) -> Result<CallToolResult, McpError> {
        if let Some(r) = self.resolve_live_market_view() {
            let s = &r.snapshot;
            let mut out = serde_json::json!({
                "poc": s.get("poc"),
                "vaHigh": s.get("vaHigh"),
                "vaLow": s.get("vaLow"),
                "orHigh": s.get("orHigh"),
                "orLow": s.get("orLow"),
                "ibHigh": s.get("ibHigh"),
                "ibLow": s.get("ibLow"),
            });
            merge_tool_live_metadata(&mut out, &r);
            return Ok(text_result(out));
        }
        Ok(no_data("No TPO data available"))
    }

    #[tool(
        description = "Delta profile: segment delta (Asia-only, London-only, or RTH-only), combined Globex delta (Asia+London when in Globex), cumulative delta, DNVA high/low, DNP. Use for inventory and positioning analysis."
    )]
    async fn get_delta_profile(&self) -> Result<CallToolResult, McpError> {
        if let Some(r) = self.resolve_live_market_view() {
            let s = &r.snapshot;
            let mut out = serde_json::json!({
                "sessionDelta": s.get("sessionDelta"),
                "globexDelta": s.get("globexDelta"),
                "cumulativeDelta": s.get("cumulativeDelta"),
                "dnvaHigh": s.get("dnvaHigh"),
                "dnvaLow": s.get("dnvaLow"),
                "dnp": s.get("dnp"),
                "sessionSegment": s.get("sessionSegment"),
            });
            merge_tool_live_metadata(&mut out, &r);
            return Ok(text_result(out));
        }
        Ok(no_data("No delta data available"))
    }

    #[tool(
        description = "Key reference levels: prior day high/low/close, prior session value area high/low and POC, overnight (Globex) high/low, Globex OR30 and London OR60, and initial balance high/low. Includes sessionType, tradingDay, and contract rollover status so agents can gate carry-forward references."
    )]
    async fn get_key_levels(&self) -> Result<CallToolResult, McpError> {
        let Some(r) = self.resolve_live_market_view() else {
            return Ok(no_data("No key levels available"));
        };
        let s = &r.snapshot;
        let (_, active_contract) = self.resolve_contract_cached();
        let session_type = s.get("sessionType").and_then(|v| v.as_str());
        let session_segment = s.get("sessionSegment").and_then(|v| v.as_str());
        let trading_day = s.get("tradingDay").and_then(|v| v.as_str());
        let is_globex = session_type == Some("Globex");

        let server_contract = self.current_pipeline_contract_metadata();
        let db = self.db.lock().map_err(|_| lock_error())?;
        let mut out = serde_json::json!({
            "sessionType": s.get("sessionType"),
            "sessionSegment": s.get("sessionSegment"),
            "tradingDay": s.get("tradingDay"),
            "rootSymbol": s.get("rootSymbol"),
            "contractSymbol": s.get("contractSymbol"),
            "contractMonth": s.get("contractMonth"),
            "symbolResolutionMode": s.get("symbolResolutionMode"),
            "symbolResolutionSource": s.get("symbolResolutionSource"),
            "rolloverWarning": s.get("rolloverWarning"),
            "carryForwardLevelsValid": s.get("carryForwardLevelsValid"),
            "priorDayContractSymbol": s.get("priorDayContractSymbol"),
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
        });
        let rollover_date = trading_day
            .map(ToString::to_string)
            .unwrap_or_else(the_desk_backend::et_now_trading_day);
        let rollover_status = self.rollover_status_for_date(
            &db,
            &active_contract,
            server_contract.as_ref(),
            &rollover_date,
            Some(r.data_age_ms),
        )?;
        out["rolloverStatus"] =
            serde_json::to_value(rollover_status).unwrap_or_else(|_| serde_json::json!({}));
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
        merge_tool_live_metadata(&mut out, &r);
        Ok(text_result(out))
    }

    #[tool(
        description = "Top SPX/options gamma concentration strikes from ConvexValue, with call/put breakdown, open interest, OI change, volume bias, vomma, recent 5m volume, avg spread, expiration coverage, and cache metadata. Use for pre-session context like 'where are the likely gamma walls?' or 'where is new positioning opening today?'"
    )]
    async fn get_gamma_levels(
        &self,
        Parameters(params): Parameters<GammaLevelsParams>,
    ) -> Result<CallToolResult, McpError> {
        let top_n = params.top.unwrap_or(12).clamp(1, 50) as usize;
        let (snapshot, refreshed) = self
            .get_or_refresh_options_snapshot(
                params.root.as_deref(),
                params.exps.clone(),
                params.range,
                params.force_refresh.unwrap_or(false),
            )
            .await?;
        let mut report = snapshot.gamma_levels.clone();
        report
            .top_gamma_concentration_levels
            .truncate(top_n.min(report.top_gamma_concentration_levels.len()));
        let out = serde_json::json!({
            "root": snapshot.root,
            "requestedExpirations": snapshot.requested_exps,
            "requestedRange": snapshot.requested_range,
            "report": report,
            "optionsContextSummary": {
                "aggregateGxoi": snapshot.context.aggregate_gxoi,
                "aggregateDxoi": snapshot.context.aggregate_dxoi,
                "callGxoi": snapshot.context.call_gxoi,
                "putGxoi": snapshot.context.put_gxoi,
                "putCallRatio": snapshot.context.put_call_ratio,
                "flowDirection": snapshot.context.flow_direction,
                "volTermSpread": snapshot.context.vol_term_spread,
            },
            "cache": options_cache_metadata(&snapshot, refreshed),
        });
        Ok(text_result(out))
    }

    #[tool(
        description = "Aggregate ConvexValue options regime context: underlying price/change, aggregate gxoi/dxoi, call/put gxoi/dxoi splits, put-call ratio, flow decomposition (flowratio, call/put value/volume bias), vol surface (front/back IV, term spread), premium flow (value bought/sold), vanna/charm regime, and cache metadata. Use when an agent needs broad options positioning context rather than per-strike detail."
    )]
    async fn get_options_context(
        &self,
        Parameters(params): Parameters<OptionsContextParams>,
    ) -> Result<CallToolResult, McpError> {
        let (snapshot, refreshed) = self
            .get_or_refresh_options_snapshot(
                params.root.as_deref(),
                params.exps.clone(),
                params.range,
                params.force_refresh.unwrap_or(false),
            )
            .await?;
        let out = serde_json::json!({
            "root": snapshot.root,
            "requestedExpirations": snapshot.requested_exps,
            "requestedRange": snapshot.requested_range,
            "context": snapshot.context,
            "topGammaStrikes": snapshot
                .gamma_levels
                .top_gamma_concentration_levels
                .iter()
                .take(3)
                .cloned()
                .collect::<Vec<_>>(),
            "cache": options_cache_metadata(&snapshot, refreshed),
        });
        Ok(text_result(out))
    }

    #[tool(
        description = "Force-refresh the cached ConvexValue snapshot used by get_gamma_levels and get_options_context, then return the fresh options context plus a gamma-level preview."
    )]
    async fn refresh_options_snapshot(
        &self,
        Parameters(params): Parameters<OptionsSnapshotParams>,
    ) -> Result<CallToolResult, McpError> {
        let (snapshot, refreshed) = self
            .get_or_refresh_options_snapshot(
                params.root.as_deref(),
                params.exps,
                params.range,
                true,
            )
            .await?;
        let out = serde_json::json!({
            "root": snapshot.root,
            "requestedExpirations": snapshot.requested_exps,
            "requestedRange": snapshot.requested_range,
            "context": snapshot.context,
            "gammaLevelsPreview": snapshot
                .gamma_levels
                .top_gamma_concentration_levels
                .iter()
                .take(5)
                .cloned()
                .collect::<Vec<_>>(),
            "cache": options_cache_metadata(&snapshot, refreshed),
        });
        Ok(text_result(out))
    }

    #[tool(
        description = "Tape pace analytics with coverage-aware rolling ticks/sec and volume/sec over 5-second, 30-second, and 5-minute windows. Returns both session-relative and rolling-context pace percentiles, smoothed normalized acceleration plus raw acceleration, 30-minute regime baselines, window validity/coverage, dwell at current price, and explicit data quality metadata so agents can distinguish live vs stale tape context."
    )]
    async fn get_tape_pace(&self) -> Result<CallToolResult, McpError> {
        let now_ms = chrono::Utc::now().timestamp_millis() as f64;
        let live_view = self.resolve_live_market_view();
        let data_age_ms = live_view
            .as_ref()
            .map(|r| r.data_age_ms)
            .unwrap_or_else(|| self.data_age_from_db_or_atomic());
        // Try live pipeline first for full snapshot including volume/sec and dwell.
        // Use try_lock to avoid blocking when backfill/poll holds the lock.
        if let Ok(pipelines) = self.pipelines.try_lock() {
            let snap = pipelines.tape_pace.snapshot(now_ms);
            let last_price = pipelines.levels.last_price;
            let payload = serde_json::json!({
                "ticksPerSec5s": snap.ticks_per_sec_5s,
                "ticksPerSec30s": snap.ticks_per_sec_30s,
                "ticksPerSec5m": snap.ticks_per_sec_5m,
                "volumePerSec5s": snap.volume_per_sec_5s,
                "volumePerSec30s": snap.volume_per_sec_30s,
                "volumePerSec5m": snap.volume_per_sec_5m,
                "acceleration": snap.acceleration,
                "rawAcceleration": snap.raw_acceleration,
                "pacePercentile": snap.pace_percentile,
                "rollingPacePercentile": snap.rolling_pace_percentile,
                "regimeTicksPerSec30mEma": snap.regime_ticks_per_sec_30m_ema,
                "regimeVolumePerSec30mEma": snap.regime_volume_per_sec_30m_ema,
                "windowCoverage5s": snap.coverage_5s,
                "windowCoverage30s": snap.coverage_30s,
                "windowCoverage5m": snap.coverage_5m,
                "isValid5s": snap.valid_5s,
                "isValid30s": snap.valid_30s,
                "isValid5m": snap.valid_5m,
                "windowAnchorTimestampMs": snap.window_anchor_timestamp_ms,
                "lastTradeTimestampMs": snap.last_trade_timestamp_ms,
                "dwellAtCurrentPriceMs": if last_price > 0.0 {
                    pipelines.tape_pace.dwell_at_price(last_price, now_ms)
                } else {
                    None
                },
                "currentPrice": if last_price > 0.0 { Some(last_price) } else { None::<f64> },
            });
            let mut out = build_tape_pace_response(payload, data_age_ms, true, now_ms);
            if let Some(ref r) = live_view {
                merge_tool_live_metadata(&mut out, r);
            }
            return Ok(text_result(out));
        }
        // Fallback to DB
        match self
            .db
            .lock()
            .ok()
            .and_then(|d| d.latest_feature_state_with_timestamp().ok().flatten())
        {
            Some((_, s)) => {
                let payload = serde_json::json!({
                    "ticksPerSec5s": s.get("tapePace5s").cloned().unwrap_or(serde_json::Value::Null),
                    "ticksPerSec30s": s.get("tapePace30s").cloned().unwrap_or(serde_json::Value::Null),
                    "ticksPerSec5m": s.get("tapePace5m").cloned().unwrap_or(serde_json::Value::Null),
                    "volumePerSec5s": s.get("tapeVolumePerSec5s").cloned().unwrap_or(serde_json::Value::Null),
                    "volumePerSec30s": s.get("tapeVolumePerSec30s").cloned().unwrap_or(serde_json::Value::Null),
                    "volumePerSec5m": s.get("tapeVolumePerSec5m").cloned().unwrap_or(serde_json::Value::Null),
                    "acceleration": s.get("tapeAcceleration").cloned().unwrap_or(serde_json::Value::Null),
                    "rawAcceleration": s.get("tapeRawAcceleration").cloned().unwrap_or(serde_json::Value::Null),
                    "pacePercentile": s.get("pacePercentile").cloned().unwrap_or(serde_json::Value::Null),
                    "rollingPacePercentile": s.get("tapeRollingPercentile").cloned().unwrap_or(serde_json::Value::Null),
                    "regimeTicksPerSec30mEma": s.get("tapeRegimeTicksPerSec30mEma").cloned().unwrap_or(serde_json::Value::Null),
                    "regimeVolumePerSec30mEma": s.get("tapeRegimeVolumePerSec30mEma").cloned().unwrap_or(serde_json::Value::Null),
                    "windowCoverage5s": s.get("tapeCoverage5s").cloned().unwrap_or(serde_json::Value::Null),
                    "windowCoverage30s": s.get("tapeCoverage30s").cloned().unwrap_or(serde_json::Value::Null),
                    "windowCoverage5m": s.get("tapeCoverage5m").cloned().unwrap_or(serde_json::Value::Null),
                    "isValid5s": s.get("tapeValid5s").cloned().unwrap_or(serde_json::Value::Null),
                    "isValid30s": s.get("tapeValid30s").cloned().unwrap_or(serde_json::Value::Null),
                    "isValid5m": s.get("tapeValid5m").cloned().unwrap_or(serde_json::Value::Null),
                    "windowAnchorTimestampMs": s.get("tapeWindowAnchorTimestampMs").cloned().unwrap_or(serde_json::Value::Null),
                    "lastTradeTimestampMs": s.get("tapeLastTradeTimestampMs").cloned().unwrap_or(serde_json::Value::Null),
                    "dwellAtCurrentPriceMs": s.get("tapeDwellAtCurrentPriceMs").cloned().unwrap_or(serde_json::Value::Null),
                    "currentPrice": s.get("lastPrice").cloned().unwrap_or(serde_json::Value::Null),
                });
                let mut out = build_tape_pace_response(payload, data_age_ms, false, now_ms);
                if let Some(ref r) = live_view {
                    merge_tool_live_metadata(&mut out, r);
                }
                Ok(text_result(out))
            }
            None => Ok(no_data("No tape pace data")),
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

        let narrative_summaries = dom_summaries_from_rows(&snapshots);
        let session_reference = if let Some((latest_ts, _)) = snapshots.last() {
            let db = self.db.lock().map_err(|_| lock_error())?;
            let rows = db
                .query_dom_feature_snapshots_for_trading_day(
                    &trading_day_from_timestamp_ms(*latest_ts),
                    50_000,
                )
                .map_err(db_error)?;
            Some(dom_summaries_from_rows(&rows))
        } else {
            None
        };

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
        let aggregate =
            if params.include_aggregate.unwrap_or(true) && !narrative_summaries.is_empty() {
                Some(
                    serde_json::to_value(summarize_dom_narrative(
                        &narrative_summaries,
                        session_reference.as_deref(),
                        None,
                    ))
                    .unwrap_or_default(),
                )
            } else {
                None
            };
        Ok(text_result(serde_json::json!({
            "windowStartMs": params.start_time_ms,
            "windowEndMs": params.end_time_ms,
            "priceFilter": { "low": params.price_low, "high": params.price_high },
            "snapshots": snapshots.into_iter().map(|(ts, payload)| serde_json::json!({
                "timestampMs": ts,
                "payload": payload
            })).collect::<Vec<_>>(),
            "latest": latest,
            "aggregate": aggregate,
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
        let recent_rows = {
            let db = self.db.lock().map_err(|_| lock_error())?;
            db.query_dom_feature_snapshots(
                Some((params.timestamp_ms - DOM_NARRATIVE_HORIZON_MS).max(0.0)),
                Some(params.timestamp_ms),
                512,
            )
            .map_err(db_error)?
        };
        let mut dom_feature_payload = feature.map(|(_, payload)| payload);
        let mut dom_summary_struct = dom_feature_payload
            .as_ref()
            .and_then(dom_summary_from_payload);
        let activity_struct = dom_feature_payload.as_ref().and_then(activity_from_payload);
        let mut session_reference_summaries: Option<Vec<DomSummary>> = None;
        if let Some(summary) = dom_summary_struct.as_mut() {
            let recent_summaries: Vec<DomSummary> = dom_summaries_from_rows(&recent_rows)
                .into_iter()
                .filter(|row| row.timestamp_ms < summary.timestamp_ms - 0.001)
                .collect();
            let session_rows = {
                let db = self.db.lock().map_err(|_| lock_error())?;
                db.query_dom_feature_snapshots_for_trading_day(
                    &trading_day_from_timestamp_ms(summary.timestamp_ms),
                    50_000,
                )
                .unwrap_or_default()
            };
            let session_reference = if session_rows.is_empty() {
                None
            } else {
                Some(dom_summaries_from_rows(&session_rows))
            };
            session_reference_summaries = session_reference.clone();
            enrich_dom_summary(
                summary,
                activity_struct.as_ref(),
                &recent_summaries,
                session_reference.as_deref(),
            );
            if let Some(payload) = dom_feature_payload
                .as_mut()
                .and_then(|value| value.as_object_mut())
            {
                payload.insert(
                    "domSummary".to_string(),
                    serde_json::to_value(summary.clone()).unwrap_or_default(),
                );
            }
        }
        let dom_summary = dom_summary_struct
            .as_ref()
            .and_then(|summary| serde_json::to_value(summary).ok());
        let activity = activity_struct
            .as_ref()
            .and_then(|summary| serde_json::to_value(summary).ok());
        let dom_regime_summary = if let Some(summary) = dom_summary_struct.as_ref() {
            let mut history = dom_summaries_from_rows(&recent_rows);
            history.retain(|row| row.timestamp_ms < summary.timestamp_ms - 0.001);
            history.push(summary.clone());
            Some(
                serde_json::to_value(summarize_dom_narrative(
                    &history,
                    session_reference_summaries.as_deref(),
                    activity_struct.as_ref(),
                ))
                .unwrap_or_default(),
            )
        } else {
            None
        };
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
            "domFeature": dom_feature_payload,
            "domSummary": dom_summary,
            "activity": activity,
            "domRegimeSummary": dom_regime_summary,
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
        description = "Summarize delayed DOM behavior over a window so agents can tell whether liquidity has been persistent, flashing, or flipping. Returns time-in-state, flip counts, persistence, confidence, and a narrative summary."
    )]
    async fn get_dom_regime_summary(
        &self,
        Parameters(params): Parameters<DomRegimeSummaryParams>,
    ) -> Result<CallToolResult, McpError> {
        let end_time_ms = if let Some(end) = params.end_time_ms.or(params.timestamp_ms) {
            end
        } else {
            let db = self.db.lock().map_err(|_| lock_error())?;
            db.latest_dom_feature_state()
                .map_err(db_error)?
                .map(|(timestamp_ms, _)| timestamp_ms)
                .ok_or_else(|| {
                    invalid_params_error(
                        "timestampMs or endTimeMs is required when no DOM history is present",
                    )
                })?
        };
        let window_ms = params
            .window_ms
            .unwrap_or(DOM_NARRATIVE_HORIZON_MS)
            .clamp(5_000.0, 1_800_000.0);
        let start_time_ms = params.start_time_ms.unwrap_or(end_time_ms - window_ms);
        validate_time_window(start_time_ms, end_time_ms)?;
        let limit = params.limit.unwrap_or(512).clamp(1, 5_000);

        let rows = {
            let db = self.db.lock().map_err(|_| lock_error())?;
            db.query_dom_feature_snapshots(Some(start_time_ms), Some(end_time_ms), limit)
                .map_err(db_error)?
        };
        let summaries = dom_summaries_from_rows(&rows);
        if summaries.is_empty() {
            return Ok(no_data(
                "No DOM feature snapshots available for the requested window",
            ));
        }
        let latest_payload = rows.last().map(|(_, payload)| payload.clone());
        let latest_activity = latest_payload.as_ref().and_then(activity_from_payload);
        let session_reference = {
            let db = self.db.lock().map_err(|_| lock_error())?;
            let day = trading_day_from_timestamp_ms(end_time_ms);
            let session_rows = db
                .query_dom_feature_snapshots_for_trading_day(&day, 50_000)
                .map_err(db_error)?;
            let parsed = dom_summaries_from_rows(&session_rows);
            if parsed.is_empty() {
                None
            } else {
                Some(parsed)
            }
        };
        let regime = summarize_dom_narrative(
            &summaries,
            session_reference.as_deref(),
            latest_activity.as_ref(),
        );

        Ok(text_result(serde_json::json!({
            "window": { "startTimeMs": start_time_ms, "endTimeMs": end_time_ms, "windowMs": window_ms },
            "regime": regime,
            "latestSummary": latest_payload.as_ref().and_then(dom_summary_from_payload),
            "latestActivity": latest_activity,
            "sampleCount": summaries.len(),
        })))
    }

    #[tool(
        description = "Historical frequency of DOM behaviors such as persisted bid support, ask resistance, liquidity flips, pulling acceleration, or stacking acceleration. Uses persisted DOM feature snapshots and returns research metadata including sample reliability and truncation status."
    )]
    async fn query_dom_behavior_frequency(
        &self,
        Parameters(params): Parameters<DomBehaviorFrequencyParams>,
    ) -> Result<CallToolResult, McpError> {
        validate_ymd_range(
            "startDate",
            params.start_date.as_deref(),
            "endDate",
            params.end_date.as_deref(),
        )?;
        let behavior = parse_dom_behavior_name(&params.behavior)?;
        let min_duration_ms = parse_dom_behavior_min_duration(params.min_duration_ms)?;
        let db = self.db.lock().map_err(|_| lock_error())?;
        let result = research::dom_behavior_frequency(
            &db,
            &behavior,
            min_duration_ms,
            params.start_date.as_deref(),
            params.end_date.as_deref(),
        )
        .map_err(db_error)?;
        Ok(text_result(
            serde_json::to_value(result).unwrap_or_default(),
        ))
    }

    #[tool(
        description = "Historical setup outcome context when a DOM behavior was present near signal fire. Answers questions like whether persistent bid support improved setup follow-through, with research metadata for outcome sample reliability."
    )]
    async fn query_dom_behavior_conditional(
        &self,
        Parameters(params): Parameters<DomBehaviorConditionalParams>,
    ) -> Result<CallToolResult, McpError> {
        validate_ymd_range(
            "startDate",
            params.start_date.as_deref(),
            "endDate",
            params.end_date.as_deref(),
        )?;
        let scope = parse_scope_value(params.scope)?;
        let behavior = parse_dom_behavior_name(&params.behavior)?;
        let setup_id = parse_optional_non_empty_string("setupId", params.setup_id.as_deref())?;
        let min_duration_ms = parse_dom_behavior_min_duration(params.min_duration_ms)?;
        let db = self.db.lock().map_err(|_| lock_error())?;
        let result = research::dom_behavior_conditional(
            &db,
            &behavior,
            setup_id.as_deref(),
            min_duration_ms,
            params.start_date.as_deref(),
            params.end_date.as_deref(),
            scope.as_ref(),
        )
        .map_err(db_error)?;
        Ok(text_result(
            serde_json::to_value(result).unwrap_or_default(),
        ))
    }

    #[tool(
        description = "Historical DOM behavior around a specific event type or level interaction. Helps answer whether persisted support, flips, or pulling acceleration commonly accompanied a class of market events. Returns research metadata and marks capped market-event scans as non-reportable."
    )]
    async fn query_dom_reaction_at_levels(
        &self,
        Parameters(params): Parameters<DomReactionAtLevelsParams>,
    ) -> Result<CallToolResult, McpError> {
        validate_ymd_range(
            "startDate",
            params.start_date.as_deref(),
            "endDate",
            params.end_date.as_deref(),
        )?;
        let scope = parse_scope_value(params.scope)?;
        let event_type = parse_research_event_type(&params.event_type)?;
        let behavior = parse_dom_behavior_name(&params.behavior)?;
        let min_duration_ms = parse_dom_behavior_min_duration(params.min_duration_ms)?;
        let db = self.db.lock().map_err(|_| lock_error())?;
        let result = research::dom_reaction_at_levels(
            &db,
            &event_type,
            &behavior,
            min_duration_ms,
            params.start_date.as_deref(),
            params.end_date.as_deref(),
            scope.as_ref(),
        )
        .map_err(db_error)?;
        Ok(text_result(
            serde_json::to_value(result).unwrap_or_default(),
        ))
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
        description = "Recent absorption-flow lifecycle events (absorption, exhaustion, delta divergence). Each event includes subtype, candidate/confirmed/invalidated status, zone bounds, direction, regime metadata, and severity."
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
                    .map(normalize_live_absorption_event)
                    .collect();
                return Ok(text_result(serde_json::json!({
                    "events": events,
                    "count": events.len(),
                    "source": "live_pipeline",
                    "dataAgeMs": self.data_age_from_db_or_atomic()
                })));
            }
        }

        // Fall back to market_events table (FlowEventEmitter writes absorption_* lifecycle events)
        match self.db.try_lock().ok().and_then(|db| {
            let data_age_ms = compute_data_age(&db);
            db.list_market_events_by_prefix("absorption_", limit)
                .ok()
                .map(|events| (events, data_age_ms))
        }) {
            Some((events, data_age_ms)) => {
                let normalized: Vec<serde_json::Value> =
                    events.iter().map(normalize_db_absorption_event).collect();
                Ok(text_result(serde_json::json!({
                    "events": normalized,
                    "count": normalized.len(),
                    "source": "market_events_db",
                    "dataAgeMs": data_age_ms
                })))
            }
            None => Ok(no_data(
                "No absorption data available or database is temporarily busy.",
            )),
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
        let (setups, risk_at_limit) = self.playbook_cache.snapshot();
        let (fallback_price, count, data_age_ms) = {
            let db = self.db.lock().map_err(|_| lock_error())?;
            let fallback_price = db
                .latest_feature_state()
                .ok()
                .flatten()
                .and_then(|s| s.get("lastPrice").and_then(|v| v.as_f64()))
                .unwrap_or(0.0);
            let count = db.count_playbook_signals().unwrap_or(0);
            let data_age_ms = compute_data_age(&db);
            (fallback_price, count, data_age_ms)
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
        if let (Ok(pipelines), Ok(rules)) = (self.pipelines.try_lock(), self.rules.lock()) {
            let market = pipelines.snapshot(bid, ask);
            let mut preview_rules = rules.clone();
            for setup in setups.iter() {
                let outcome = preview_rules.evaluate_detailed(setup, &market, risk_at_limit);
                let runtime = preview_rules.runtime_snapshot(&setup.id);
                setup_statuses.push(serde_json::json!({
                    "setupId": setup.id,
                    "setupName": setup.name,
                    "state": outcome.evaluation.state,
                    "readiness": outcome.evaluation.readiness,
                    "readinessScore": outcome.evaluation.readiness_score,
                    "metConditions": outcome.evaluation.met_conditions,
                    "missingConditions": outcome.evaluation.missing_conditions,
                    "metCount": outcome.evaluation.met_count,
                    "totalCount": outcome.evaluation.total_count,
                    "deterministicAllMet": outcome.evaluation.deterministic_all_met,
                    "requiresDiscretionary": outcome.evaluation.requires_discretionary,
                    "lastEvaluatedAtMs": runtime.as_ref().map(|r| r.last_evaluated_at_ms),
                    "lastTransitionAtMs": runtime.as_ref().map(|r| r.last_transition_at_ms),
                    "stateSource": runtime.as_ref().map(|r| r.source.clone()).unwrap_or_else(|| "memory".to_string()),
                    "rehydrated": self.feed_runtime.setup_runtime_rehydrated.load(Ordering::Acquire),
                    "rulesWarmReplayComplete": self.feed_runtime.rules_warm_replay_complete.load(Ordering::Acquire),
                }));
            }
        } else {
            for setup in setups.iter() {
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
            "dataAgeMs": data_age_ms,
            "rehydrated": self.feed_runtime.setup_runtime_rehydrated.load(Ordering::Acquire),
            "rulesWarmReplayComplete": self.feed_runtime.rules_warm_replay_complete.load(Ordering::Acquire),
        })))
    }

    #[tool(
        description = "Return recent durable setup state/progress transitions for a setup or session. Use to answer what changed before/after a restart."
    )]
    async fn get_setup_state_history(
        &self,
        Parameters(params): Parameters<SetupStateHistoryParams>,
    ) -> Result<CallToolResult, McpError> {
        let now_ms = chrono::Utc::now().timestamp_millis() as f64;
        let since_ms = params
            .minutes
            .map(|minutes| now_ms - minutes.max(0.0) * 60_000.0);
        let limit = params.limit.unwrap_or(50).clamp(1, 500) as usize;
        let db = self.db.lock().map_err(|_| lock_error())?;
        let rows = db
            .query_setup_state_history(
                params.setup_id.as_deref(),
                params.session_date.as_deref(),
                since_ms,
                limit,
            )
            .map_err(db_error)?;
        Ok(text_result(serde_json::json!({
            "transitions": rows,
            "count": rows.len(),
            "rehydrated": self.feed_runtime.setup_runtime_rehydrated.load(Ordering::Acquire),
            "rulesWarmReplayComplete": self.feed_runtime.rules_warm_replay_complete.load(Ordering::Acquire),
        })))
    }

    #[tool(
        description = "Mark a setup's discretionary prompt as confirmed and persist the lifecycle transition."
    )]
    async fn acknowledge_setup_prompt(
        &self,
        Parameters(params): Parameters<SetupLifecycleParams>,
    ) -> Result<CallToolResult, McpError> {
        let timestamp_ms = chrono::Utc::now().timestamp_millis() as f64;
        let (before, after) = {
            let mut rules = self.rules.lock().map_err(|_| lock_error())?;
            let before = rules.runtime_snapshot(&params.setup_id);
            rules
                .acknowledge_prompt_at(&params.setup_id, timestamp_ms)
                .ok_or_else(|| invalid_params_error("unknown setup_id or no runtime state"))?;
            let after = rules
                .runtime_snapshot(&params.setup_id)
                .ok_or_else(|| invalid_params_error("setup runtime missing after acknowledge"))?;
            (before, after)
        };
        self.persist_manual_setup_state_change(
            &params.setup_id,
            before,
            after.clone(),
            "manualConfirmed",
            timestamp_ms,
        )?;
        Ok(text_result(serde_json::json!({
            "setupId": params.setup_id,
            "state": after.state,
            "readiness": after.readiness,
            "persisted": true,
        })))
    }

    #[tool(description = "Mark a setup as in-trade and persist the lifecycle transition.")]
    async fn mark_setup_in_trade(
        &self,
        Parameters(params): Parameters<SetupLifecycleParams>,
    ) -> Result<CallToolResult, McpError> {
        let timestamp_ms = chrono::Utc::now().timestamp_millis() as f64;
        let (before, after) = {
            let mut rules = self.rules.lock().map_err(|_| lock_error())?;
            let before = rules.runtime_snapshot(&params.setup_id);
            rules
                .mark_in_trade_at(&params.setup_id, timestamp_ms)
                .ok_or_else(|| invalid_params_error("unknown setup_id or no runtime state"))?;
            let after = rules
                .runtime_snapshot(&params.setup_id)
                .ok_or_else(|| invalid_params_error("setup runtime missing after mark in trade"))?;
            (before, after)
        };
        self.persist_manual_setup_state_change(
            &params.setup_id,
            before,
            after.clone(),
            "manualInTrade",
            timestamp_ms,
        )?;
        Ok(text_result(serde_json::json!({
            "setupId": params.setup_id,
            "state": after.state,
            "readiness": after.readiness,
            "persisted": true,
        })))
    }

    #[tool(description = "Close a setup lifecycle state and persist the transition.")]
    async fn close_setup_state(
        &self,
        Parameters(params): Parameters<SetupLifecycleParams>,
    ) -> Result<CallToolResult, McpError> {
        let timestamp_ms = chrono::Utc::now().timestamp_millis() as f64;
        let (before, after) = {
            let mut rules = self.rules.lock().map_err(|_| lock_error())?;
            let before = rules.runtime_snapshot(&params.setup_id);
            rules
                .close_trade_at(&params.setup_id, timestamp_ms)
                .ok_or_else(|| invalid_params_error("unknown setup_id or no runtime state"))?;
            let after = rules
                .runtime_snapshot(&params.setup_id)
                .ok_or_else(|| invalid_params_error("setup runtime missing after close"))?;
            (before, after)
        };
        self.persist_manual_setup_state_change(
            &params.setup_id,
            before,
            after.clone(),
            "manualClosed",
            timestamp_ms,
        )?;
        Ok(text_result(serde_json::json!({
            "setupId": params.setup_id,
            "state": after.state,
            "readiness": after.readiness,
            "persisted": true,
        })))
    }

    #[tool(
        description = "Ranked proactive attention inbox. Call this first when asking what deserves attention now; returns durable playbook-grounded signals, never raw ticks."
    )]
    async fn get_attention_inbox(
        &self,
        Parameters(params): Parameters<AttentionInboxParams>,
    ) -> Result<CallToolResult, McpError> {
        let limit = params.limit.unwrap_or(25).clamp(1, 100);
        let cursor = params.cursor.unwrap_or_default();
        let db = self.db.lock().map_err(|_| lock_error())?;
        let signals = db
            .query_attention_signals(&AttentionSignalQuery {
                status: params.status,
                min_priority: params.min_priority,
                include_expired: params.include_expired.unwrap_or(false),
                cursor_signal_id: cursor.last_signal_id,
                since_ms: cursor.since_ms,
                limit,
                ..AttentionSignalQuery::default()
            })
            .map_err(db_error)?;
        let next_cursor = signals.last().map(|signal| {
            serde_json::json!({
                "lastSignalId": signal.signal_id,
                "sinceMs": signal.updated_at_ms
            })
        });
        Ok(text_result(serde_json::json!({
            "signals": signals,
            "count": signals.len(),
            "nextCursor": next_cursor,
            "dataAgeMs": compute_data_age(&db)
        })))
    }

    #[tool(
        description = "Full detail for one attention signal: evidence links, setup/risk context references, priority breakdown, and suggested next MCP tools for agent routing."
    )]
    async fn get_signal_detail(
        &self,
        Parameters(params): Parameters<AttentionSignalDetailParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let signal = db
            .get_attention_signal(&params.signal_id)
            .map_err(db_error)?
            .ok_or_else(|| invalid_params_error("unknown signal_id"))?;
        let changelog = db
            .query_attention_changelog(&AttentionChangelogQuery {
                signal_id: Some(signal.signal_id.clone()),
                cursor_event_id: None,
                since_ms: Some(signal.created_at_ms - 1.0),
                limit: 50,
            })
            .map_err(db_error)?;
        Ok(text_result(serde_json::json!({
            "signal": signal,
            "changelog": changelog,
            "suggestedTools": signal.suggested_tools,
            "dataAgeMs": compute_data_age(&db)
        })))
    }

    #[tool(
        description = "Acknowledge an attention signal as reviewed by the trader or an agent. Use acknowledgedBy='trader' or 'agent:<name>'."
    )]
    async fn acknowledge_attention_signal(
        &self,
        Parameters(params): Parameters<AttentionSignalAcknowledgeParams>,
    ) -> Result<CallToolResult, McpError> {
        let actor = parse_non_empty_string("acknowledgedBy", &params.acknowledged_by)?;
        if actor != "trader" && !actor.starts_with("agent:") {
            return Err(invalid_params_error(
                "acknowledgedBy must be 'trader' or 'agent:<name>'",
            ));
        }
        let timestamp_ms = chrono::Utc::now().timestamp_millis() as f64;
        let (acknowledged, signal) = {
            let db = self.db.lock().map_err(|_| lock_error())?;
            let acknowledged = db
                .acknowledge_attention_signal(
                    &params.signal_id,
                    &actor,
                    params.note.as_deref(),
                    timestamp_ms,
                )
                .map_err(db_error)?;
            let signal = db
                .get_attention_signal(&params.signal_id)
                .map_err(db_error)?;
            (acknowledged, signal)
        };
        if let Some(signal) = &signal {
            record_runtime_event_scoped(
                &self.runtime_events,
                Some(&self.db),
                RuntimeEventLevel::Info,
                "attention.signal_acknowledged",
                "attention",
                "Attention signal acknowledged.",
                serde_json::json!({
                    "signalId": signal.signal_id,
                    "acknowledgedBy": actor,
                }),
                Some(signal.session_date.clone()),
                signal.root_symbol.clone(),
                signal.contract_symbol.clone(),
            );
        }
        Ok(text_result(serde_json::json!({
            "signalId": params.signal_id,
            "acknowledged": acknowledged,
            "signal": signal
        })))
    }

    #[tool(
        description = "Cursor-based catch-up feed for what changed since a prior attention cursor. Use when the trader asks what changed while away."
    )]
    async fn what_changed_since(
        &self,
        Parameters(params): Parameters<WhatChangedSinceParams>,
    ) -> Result<CallToolResult, McpError> {
        let cursor = params.cursor.unwrap_or_default();
        let limit = params.limit.unwrap_or(50).clamp(1, 200);
        let db = self.db.lock().map_err(|_| lock_error())?;
        let changelog = db
            .query_attention_changelog(&AttentionChangelogQuery {
                signal_id: None,
                cursor_event_id: cursor.last_event_id.clone(),
                since_ms: cursor.since_ms,
                limit,
            })
            .map_err(db_error)?;
        let signals = if params.include_signals.unwrap_or(true) {
            db.query_attention_signals(&AttentionSignalQuery {
                status: None,
                min_priority: None,
                include_expired: true,
                cursor_signal_id: cursor.last_signal_id,
                since_ms: cursor.since_ms,
                limit,
                ..AttentionSignalQuery::default()
            })
            .map_err(db_error)?
        } else {
            Vec::new()
        };
        let next_cursor = serde_json::json!({
            "lastEventId": changelog.last().map(|event| event.event_id.clone()),
            "lastSignalId": signals.last().map(|signal| signal.signal_id.clone()),
            "sinceMs": changelog
                .last()
                .map(|event| event.occurred_at_ms)
                .or_else(|| signals.last().map(|signal| signal.updated_at_ms))
                .or(cursor.since_ms)
        });
        Ok(text_result(serde_json::json!({
            "changes": changelog,
            "signals": signals,
            "nextCursor": next_cursor,
            "dataAgeMs": compute_data_age(&db)
        })))
    }

    #[tool(
        description = "Replay attention signal lifecycle deltas such as created, priority_changed, acknowledged, expired, invalidated, or notified. Use for agent catch-up and audit trails."
    )]
    async fn get_attention_changelog(
        &self,
        Parameters(params): Parameters<AttentionChangelogParams>,
    ) -> Result<CallToolResult, McpError> {
        let cursor = params.cursor.unwrap_or_default();
        let limit = params.limit.unwrap_or(50).clamp(1, 200);
        let db = self.db.lock().map_err(|_| lock_error())?;
        let events = db
            .query_attention_changelog(&AttentionChangelogQuery {
                signal_id: None,
                cursor_event_id: cursor.last_event_id,
                since_ms: cursor.since_ms,
                limit,
            })
            .map_err(db_error)?;
        let next_cursor = events.last().map(|event| {
            serde_json::json!({
                "lastEventId": event.event_id,
                "sinceMs": event.occurred_at_ms
            })
        });
        Ok(text_result(serde_json::json!({
            "events": events,
            "count": events.len(),
            "nextCursor": next_cursor,
            "dataAgeMs": compute_data_age(&db)
        })))
    }

    #[tool(
        description = "Current trade idea cards derived from playbook setup lifecycle and attention signals. These are idea-state overlays, not execution instructions."
    )]
    async fn get_active_trade_ideas(
        &self,
        Parameters(params): Parameters<ActiveTradeIdeasParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let ideas = db
            .query_trade_idea_cards(&TradeIdeaQuery {
                status: params.status.or_else(|| Some("active".to_string())),
                setup_id: params.setup_id,
                limit: params.limit.unwrap_or(25).clamp(1, 100),
            })
            .map_err(db_error)?;
        Ok(text_result(serde_json::json!({
            "ideas": ideas,
            "count": ideas.len(),
            "dataAgeMs": compute_data_age(&db)
        })))
    }

    #[tool(
        description = "Mark a trade idea as confirmed with evidence. Enforces typed lifecycle instead of a free-form state setter."
    )]
    async fn mark_trade_idea_confirmed(
        &self,
        Parameters(params): Parameters<TradeIdeaConfirmParams>,
    ) -> Result<CallToolResult, McpError> {
        self.transition_trade_idea_tool(
            &params.idea_id,
            "confirmed",
            "active",
            Some(params.evidence_note.as_str()),
        )
    }

    #[tool(description = "Mark a trade idea as invalidated with a reason code and optional note.")]
    async fn mark_trade_idea_invalidated(
        &self,
        Parameters(params): Parameters<TradeIdeaInvalidateParams>,
    ) -> Result<CallToolResult, McpError> {
        let note = params
            .note
            .as_deref()
            .map(|note| format!("{}: {}", params.reason_code, note))
            .unwrap_or(params.reason_code);
        self.transition_trade_idea_tool(&params.idea_id, "invalidated", "closed", Some(&note))
    }

    #[tool(description = "Mark a trade idea as in-trade, optionally linking a signal outcome ID.")]
    async fn mark_trade_idea_in_trade(
        &self,
        Parameters(params): Parameters<TradeIdeaInTradeParams>,
    ) -> Result<CallToolResult, McpError> {
        let note = params
            .signal_outcome_id
            .as_ref()
            .map(|id| format!("linked signal outcome: {id}"));
        self.transition_trade_idea_tool(&params.idea_id, "in_trade", "active", note.as_deref())
    }

    #[tool(description = "Mark a trade idea as resolved with an outcome and optional note.")]
    async fn mark_trade_idea_resolved(
        &self,
        Parameters(params): Parameters<TradeIdeaResolveParams>,
    ) -> Result<CallToolResult, McpError> {
        let note = params
            .note
            .as_deref()
            .map(|note| format!("{}: {}", params.outcome, note))
            .unwrap_or(params.outcome);
        self.transition_trade_idea_tool(&params.idea_id, "resolved", "closed", Some(&note))
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
        self.playbook_cache.set_risk_at_limit(state.at_limit);
        Ok(text_result(serde_json::json!({
            "initialized": true,
            "riskState": state
        })))
    }

    #[tool(
        description = "Start a trading session in the local journal store. Creates a session row that trades and journal entries can attach to. Use this at the beginning of a discretionary review or live session when you want Cursor agents to log journal context consistently."
    )]
    async fn start_trading_session(
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
    async fn end_trading_session(
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

    #[tool(
        description = "Create or update a trade journal entry. Supports manual chat-first trade logging as well as imported-fill normalization. If session_id is omitted, the latest open session is used when available."
    )]
    async fn upsert_trade_entry(
        &self,
        Parameters(params): Parameters<UpsertTradeEntryParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let entry_time = params
            .entry_time_ms
            .unwrap_or_else(|| Utc::now().timestamp_millis() as f64);
        let session_id = resolve_session_id(&db, params.session_id.as_deref())?;
        let direction = params.direction.clone();
        let trade = TradeRecord {
            id: params
                .id
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            session_id,
            setup_id: params.setup_id,
            instrument: params.instrument,
            trade_account: params.trade_account,
            entry_time,
            entry_price: params.entry_price,
            exit_time: params.exit_time_ms,
            exit_price: params.exit_price,
            direction: direction.clone(),
            size: params.size,
            max_open_size: params.max_open_size.or(Some(params.size)),
            stop_price: params.stop_price,
            target_prices: params.target_prices.unwrap_or_default(),
            result_r: params.result_r,
            gross_points: params.gross_points.or_else(|| {
                params.exit_price.map(|exit_price| {
                    let per_contract = if direction.eq_ignore_ascii_case("long") {
                        exit_price - params.entry_price
                    } else {
                        params.entry_price - exit_price
                    };
                    per_contract * params.size as f64
                })
            }),
            planned: params.planned.unwrap_or(false),
            rules_followed: params.rules_followed,
            emotional_state: params.emotional_state,
            thesis: params.thesis,
            review_tags: params.review_tags.unwrap_or_default(),
            mistake_tags: params.mistake_tags.unwrap_or_default(),
            entry_fill_count: params.entry_fill_count.unwrap_or(1),
            exit_fill_count: params
                .exit_fill_count
                .unwrap_or_else(|| i64::from(params.exit_price.is_some())),
            import_batch_id: params.import_batch_id,
            notes: params.notes.unwrap_or_default(),
            source: params.source.unwrap_or_else(|| "manual_chat".to_string()),
        };
        db.upsert_trade(&trade).map_err(db_error)?;
        let memory_maintenance =
            memory_mark_dirty(&db, true, false, "upsert_trade_entry").map_err(db_error)?;
        Ok(text_result(serde_json::json!({
            "saved": true,
            "trade": trade,
            "memoryMaintenance": memory_maintenance
        })))
    }

    #[tool(
        description = "Close a trade journal entry with exit details. Optionally updates risk state when result_r is supplied and update_risk_state is true."
    )]
    async fn close_trade_entry(
        &self,
        Parameters(params): Parameters<CloseTradeEntryParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let mut trade = db
            .get_trade(&params.id)
            .map_err(db_error)?
            .ok_or_else(|| invalid_params_error("trade not found"))?;
        let exit_time_ms = params
            .exit_time_ms
            .unwrap_or_else(|| Utc::now().timestamp_millis() as f64);
        trade.exit_time = Some(exit_time_ms);
        trade.exit_price = Some(params.exit_price);
        if let Some(result_r) = params.result_r {
            trade.result_r = Some(result_r);
        }
        trade.gross_points = params.gross_points.or_else(|| {
            let per_contract = if trade.direction.eq_ignore_ascii_case("long") {
                params.exit_price - trade.entry_price
            } else {
                trade.entry_price - params.exit_price
            };
            Some(per_contract * trade.size as f64)
        });
        if let Some(notes) = params.notes {
            trade.notes = notes;
        }
        trade.exit_fill_count = trade.exit_fill_count.max(1);
        db.upsert_trade(&trade).map_err(db_error)?;

        let mut updated_risk_state = None;
        if params.update_risk_state.unwrap_or(false) {
            let result_r = trade.result_r.ok_or_else(|| {
                invalid_params_error("result_r is required when update_risk_state is true")
            })?;
            let risk_state = db.load_risk_state().map_err(db_error)?.unwrap_or_default();
            let config = db.load_risk_config().map_err(db_error)?;
            let mut tracker = RiskTracker::new(RiskConfig {
                max_daily_loss_r: config.max_daily_loss_r,
                max_trades_per_session: config.max_trades_per_session.unwrap_or(8) as usize,
            });
            tracker.restore_state(risk_state);
            tracker.record_trade_result(result_r);
            let new_state = tracker.state();
            db.save_risk_state(&new_state).map_err(db_error)?;
            self.playbook_cache.set_risk_at_limit(new_state.at_limit);
            updated_risk_state = Some(new_state);
        }
        let memory_maintenance =
            memory_mark_dirty(&db, true, false, "close_trade_entry").map_err(db_error)?;

        Ok(text_result(serde_json::json!({
            "closed": true,
            "trade": trade,
            "updatedRiskState": updated_risk_state,
            "memoryMaintenance": memory_maintenance
        })))
    }

    #[tool(
        description = "Update structured trade review fields including thesis, review tags, mistake tags, discipline flags, and notes."
    )]
    async fn review_trade_entry(
        &self,
        Parameters(params): Parameters<ReviewTradeEntryParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        db.update_trade_review(
            &params.id,
            &TradeReviewUpdate {
                planned: params.planned,
                rules_followed: params.rules_followed,
                emotional_state: params.emotional_state,
                thesis: params.thesis,
                review_tags: params.review_tags.unwrap_or_default(),
                mistake_tags: params.mistake_tags.unwrap_or_default(),
                notes: params.notes.unwrap_or_default(),
            },
        )
        .map_err(db_error)?;
        let trade = db.get_trade(&params.id).map_err(db_error)?;
        let memory_maintenance =
            memory_mark_dirty(&db, true, false, "review_trade_entry").map_err(db_error)?;
        Ok(text_result(serde_json::json!({
            "updated": true,
            "trade": trade,
            "memoryMaintenance": memory_maintenance
        })))
    }

    #[tool(
        description = "Save a journal note. If session_id is omitted, the latest open session is used when available."
    )]
    async fn save_journal_entry(
        &self,
        Parameters(params): Parameters<SaveJournalEntryParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let created_at = params
            .created_at_ms
            .unwrap_or_else(|| Utc::now().timestamp_millis() as f64);
        let entry = JournalEntry {
            id: params
                .id
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            session_id: resolve_session_id(&db, params.session_id.as_deref())?,
            date: params
                .date
                .unwrap_or_else(|| trading_day_from_timestamp_ms(created_at)),
            content: params.content,
            tags: params.tags.unwrap_or_default(),
            setup_references: params.setup_references.unwrap_or_default(),
            trade_references: params.trade_references.unwrap_or_default(),
            created_at,
        };
        db.upsert_journal_entry(&entry).map_err(db_error)?;
        Ok(text_result(serde_json::json!({
            "saved": true,
            "journalEntry": entry
        })))
    }

    #[tool(
        description = "List trade journal entries. Without filters, returns the most recent trade entries across sessions."
    )]
    async fn list_trade_entries(
        &self,
        Parameters(params): Parameters<TradeListParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let limit = params.limit.unwrap_or(50).min(500) as usize;
        let trades = if let Some(session_id) = params.session_id {
            db.list_trades_for_session(&session_id).map_err(db_error)?
        } else {
            db.list_recent_trades(limit).map_err(db_error)?
        };
        Ok(text_result(serde_json::json!({
            "trades": trades,
            "count": trades.len()
        })))
    }

    #[tool(description = "Get a single trade journal entry by ID.")]
    async fn get_trade_entry(
        &self,
        Parameters(params): Parameters<TradeEntryIdParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        match db.get_trade(&params.id).map_err(db_error)? {
            Some(trade) => Ok(text_result(serde_json::json!({ "trade": trade }))),
            None => Ok(no_data("Trade not found.")),
        }
    }

    #[tool(
        description = "Return journal notes for a session. If session_id is omitted, uses the latest open session when available."
    )]
    async fn get_session_journal(
        &self,
        Parameters(params): Parameters<SessionJournalParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let session_id = resolve_session_id(&db, params.session_id.as_deref())?
            .ok_or_else(|| invalid_params_error("no session found"))?;
        let session = db.get_session(&session_id).map_err(db_error)?;
        let entries = db.get_journal_for_session(&session_id).map_err(db_error)?;
        Ok(text_result(serde_json::json!({
            "session": session,
            "journalEntries": entries,
            "count": entries.len()
        })))
    }

    #[tool(
        description = "Get a compact slice of recent journal notes. Supports filtering by tag, setup reference, or trade reference."
    )]
    async fn get_recent_journal_notes(
        &self,
        Parameters(params): Parameters<RecentJournalNotesParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let limit = params.limit.unwrap_or(5).min(25) as usize;
        let filtered: Vec<JournalEntry> = db
            .list_recent_journal_entries(limit * 10)
            .map_err(db_error)?
            .into_iter()
            .filter(|entry| {
                let tag_ok = params
                    .tag
                    .as_ref()
                    .map(|tag| entry.tags.iter().any(|t| t == tag))
                    .unwrap_or(true);
                let setup_ok = params
                    .setup_reference
                    .as_ref()
                    .map(|setup| entry.setup_references.iter().any(|value| value == setup))
                    .unwrap_or(true);
                let trade_ok = params
                    .trade_reference
                    .as_ref()
                    .map(|trade_id| entry.trade_references.iter().any(|value| value == trade_id))
                    .unwrap_or(true);
                tag_ok && setup_ok && trade_ok
            })
            .take(limit)
            .collect();
        Ok(text_result(serde_json::json!({
            "journalEntries": filtered,
            "count": filtered.len()
        })))
    }

    #[tool(
        description = "Return a structured session review bundle: session metadata, trade journal entries, journal notes, and deterministic summary metrics for debrief workflows."
    )]
    async fn get_session_review_context(
        &self,
        Parameters(params): Parameters<SessionReviewContextParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let session_id = resolve_session_id(&db, params.session_id.as_deref())?
            .ok_or_else(|| invalid_params_error("no session found"))?;
        let session = db.get_session(&session_id).map_err(db_error)?;
        let trades = db.list_trades_for_session(&session_id).map_err(db_error)?;
        let journal = db.get_journal_for_session(&session_id).map_err(db_error)?;
        let closed_trades = trades
            .iter()
            .filter(|trade| trade.exit_time.is_some())
            .count();
        let winning_trades = trades
            .iter()
            .filter(|trade| trade.gross_points.unwrap_or(0.0) > 0.0)
            .count();
        let losing_trades = trades
            .iter()
            .filter(|trade| trade.gross_points.unwrap_or(0.0) < 0.0)
            .count();
        let total_gross_points: f64 = trades.iter().filter_map(|trade| trade.gross_points).sum();
        let planned_count = trades.iter().filter(|trade| trade.planned).count();
        let rules_broken_count = trades
            .iter()
            .filter(|trade| matches!(trade.rules_followed, Some(false)))
            .count();

        Ok(text_result(serde_json::json!({
            "session": session,
            "trades": trades,
            "journalEntries": journal,
            "summary": {
                "tradeCount": trades.len(),
                "closedTradeCount": closed_trades,
                "winningTradeCount": winning_trades,
                "losingTradeCount": losing_trades,
                "plannedTradeCount": planned_count,
                "rulesBrokenTradeCount": rules_broken_count,
                "journalEntryCount": journal.len(),
                "grossPoints": total_gross_points
            }
        })))
    }

    #[tool(
        description = "Aggregate deterministic journal patterns across sessions: planned-vs-unplanned counts, rules adherence, emotional states, review tags, mistake tags, and gross points."
    )]
    async fn query_journal_patterns(
        &self,
        Parameters(params): Parameters<JournalPatternParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let limit = params.limit.unwrap_or(200).min(2000) as usize;
        let sessions = db.list_sessions(limit).map_err(db_error)?;
        let filtered_sessions: Vec<SessionRecord> = sessions
            .into_iter()
            .filter(|session| {
                let start_ok = params
                    .start_date
                    .as_ref()
                    .map(|start| &session.date >= start)
                    .unwrap_or(true);
                let end_ok = params
                    .end_date
                    .as_ref()
                    .map(|end| &session.date <= end)
                    .unwrap_or(true);
                let type_ok = params
                    .session_type
                    .as_ref()
                    .map(|session_type| session.session_type.eq_ignore_ascii_case(session_type))
                    .unwrap_or(true);
                start_ok && end_ok && type_ok
            })
            .collect();

        let mut planned = 0usize;
        let mut unplanned = 0usize;
        let mut rules_followed_true = 0usize;
        let mut rules_followed_false = 0usize;
        let mut emotional_counts: HashMap<String, usize> = HashMap::new();
        let mut review_tag_counts: HashMap<String, usize> = HashMap::new();
        let mut mistake_tag_counts: HashMap<String, usize> = HashMap::new();
        let mut total_gross_points = 0.0;
        let mut trade_count = 0usize;

        for session in &filtered_sessions {
            for trade in db.list_trades_for_session(&session.id).map_err(db_error)? {
                trade_count += 1;
                if trade.planned {
                    planned += 1;
                } else {
                    unplanned += 1;
                }
                match trade.rules_followed {
                    Some(true) => rules_followed_true += 1,
                    Some(false) => rules_followed_false += 1,
                    None => {}
                }
                if let Some(emotion) = trade.emotional_state.clone() {
                    *emotional_counts.entry(emotion).or_default() += 1;
                }
                for tag in trade.review_tags {
                    *review_tag_counts.entry(tag).or_default() += 1;
                }
                for tag in trade.mistake_tags {
                    *mistake_tag_counts.entry(tag).or_default() += 1;
                }
                total_gross_points += trade.gross_points.unwrap_or(0.0);
            }
        }

        Ok(text_result(serde_json::json!({
            "sessionCount": filtered_sessions.len(),
            "tradeCount": trade_count,
            "plannedCount": planned,
            "unplannedCount": unplanned,
            "rulesFollowedCount": rules_followed_true,
            "rulesBrokenCount": rules_followed_false,
            "emotionalStateCounts": emotional_counts,
            "reviewTagCounts": review_tag_counts,
            "mistakeTagCounts": mistake_tag_counts,
            "grossPoints": total_gross_points
        })))
    }

    #[tool(
        description = "Save an agent-authored memory insight. New insights start as candidate unless they are reinforced by a matching prior insight or explicitly backed by patternIds in evidence."
    )]
    async fn save_agent_insight(
        &self,
        Parameters(params): Parameters<SaveAgentInsightParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let session_id = resolve_session_id(&db, params.session_id.as_deref())?;
        let record = memory_save_agent_insight(
            &db,
            SaveAgentInsightInput {
                id: params.id,
                session_id,
                trade_id: params.trade_id,
                setup_id: params.setup_id,
                category: params.category,
                summary: params.summary,
                evidence: params.evidence,
                tags: params.tags,
                scope: params.scope,
                confidence: params.confidence,
                salience: params.salience,
                source: params.source,
            },
        )
        .map_err(|e| invalid_params_error(e.to_string()))?;
        let memory_maintenance = db.get_memory_maintenance_state().map_err(db_error)?;
        Ok(text_result(serde_json::json!({
            "agentInsight": record,
            "memoryMaintenance": memory_maintenance
        })))
    }

    #[tool(
        description = "Recall stored agent insights with filters for category, setup, status, and context scope."
    )]
    async fn recall_agent_insights(
        &self,
        Parameters(params): Parameters<RecallAgentInsightsParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let insights = db
            .list_agent_insights(&AgentInsightQuery {
                category: params.category,
                setup_id: params.setup_id,
                statuses: params.statuses,
                tag: params.tag,
                session_type: params.session_type,
                session_segment: params.session_segment,
                time_bucket: params.time_bucket,
                day_type: params.day_type,
                start_date: params.start_date,
                end_date: params.end_date,
                limit: params.limit.map(|limit| limit.min(200) as usize),
            })
            .map_err(db_error)?;
        Ok(text_result(serde_json::json!({
            "insights": insights,
            "count": insights.len()
        })))
    }

    #[tool(
        description = "Acknowledge an insight after surfacing it. Supported actions: surfaced, helpful, irrelevant, wrong, pin."
    )]
    async fn acknowledge_agent_insight(
        &self,
        Parameters(params): Parameters<InsightAcknowledgeParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let surfaced_at_ms = params
            .surfaced_at_ms
            .unwrap_or_else(|| Utc::now().timestamp_millis() as f64);
        let updated = db
            .acknowledge_agent_insight(&params.id, &params.action, surfaced_at_ms)
            .map_err(db_error)?;
        Ok(text_result(serde_json::json!({
            "insight": updated,
            "updated": updated.is_some()
        })))
    }

    #[tool(description = "Supersede an older insight with a newer replacement insight ID.")]
    async fn supersede_agent_insight(
        &self,
        Parameters(params): Parameters<SupersedeInsightParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        db.supersede_agent_insight(
            &params.previous_id,
            &params.replacement_id,
            Utc::now().timestamp_millis() as f64,
        )
        .map_err(db_error)?;
        Ok(text_result(serde_json::json!({
            "superseded": true,
            "previousId": params.previous_id,
            "replacementId": params.replacement_id
        })))
    }

    #[tool(
        description = "Run deterministic behavioral memory detection over stored sessions, trades, and reviews, then upsert active behavioral patterns."
    )]
    async fn detect_behavioral_patterns(&self) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let patterns = memory_detect_behavioral_patterns(&db).map_err(db_error)?;
        let memory_maintenance = db.get_memory_maintenance_state().map_err(db_error)?;
        Ok(text_result(serde_json::json!({
            "patterns": patterns,
            "count": patterns.len(),
            "memoryMaintenance": memory_maintenance
        })))
    }

    #[tool(
        description = "Explicitly refresh memory maintenance state without coupling recomputation to read requests. Can refresh behavioral patterns, insight lifecycle status, or both."
    )]
    async fn refresh_memory_state(
        &self,
        Parameters(params): Parameters<RefreshMemoryStateParams>,
    ) -> Result<CallToolResult, McpError> {
        let refresh_patterns = params.refresh_patterns.unwrap_or(true);
        let refresh_insight_lifecycle = params.refresh_insight_lifecycle.unwrap_or(true);
        if !refresh_patterns && !refresh_insight_lifecycle {
            return Err(invalid_params_error(
                "at least one refresh target must be enabled",
            ));
        }
        let include_patterns = params.include_patterns.unwrap_or(false);
        let db = self.db.lock().map_err(|_| lock_error())?;
        let refresh = memory_refresh_state(
            &db,
            MemoryRefreshOptions {
                refresh_patterns,
                refresh_insight_lifecycle,
            },
            params.reason.as_deref(),
        )
        .map_err(db_error)?;
        Ok(text_result(serde_json::json!({
            "refreshedAtMs": refresh.refreshed_at_ms,
            "staleInsightsUpdated": refresh.stale_insights_updated,
            "patternCount": refresh.patterns.len(),
            "patterns": if include_patterns {
                serde_json::json!(refresh.patterns)
            } else {
                serde_json::json!(null)
            },
            "memoryMaintenance": refresh.maintenance
        })))
    }

    #[tool(
        description = "Get active behavioral patterns with optional scope filters and minimum sample size."
    )]
    async fn get_behavioral_patterns(
        &self,
        Parameters(params): Parameters<BehavioralPatternMemoryParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let patterns = db
            .list_behavioral_patterns(&BehavioralPatternQuery {
                pattern_type: params.pattern_type,
                session_type: params.session_type,
                session_segment: params.session_segment,
                time_bucket: params.time_bucket,
                day_type: params.day_type,
                setup_id: params.setup_id,
                min_sample_size: params.min_sample_size,
                active_only: params.active_only.or(Some(true)),
                limit: params.limit.map(|limit| limit.min(200) as usize),
            })
            .map_err(db_error)?;
        Ok(text_result(serde_json::json!({
            "patterns": patterns,
            "count": patterns.len()
        })))
    }

    #[tool(description = "Create an open follow-up item for later session review or confirmation.")]
    async fn create_memory_followup(
        &self,
        Parameters(params): Parameters<CreateMemoryFollowupParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let session_id = resolve_session_id(&db, params.session_id.as_deref())?;
        let followup = MemoryFollowupRecord {
            id: params
                .id
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            created_at_ms: Utc::now().timestamp_millis() as f64,
            resolved_at_ms: None,
            session_id,
            trade_id: params.trade_id,
            source: params.source.unwrap_or_else(|| "agent".to_string()),
            title: params.title,
            detail: params.detail.unwrap_or_default(),
            status: "open".to_string(),
            tags: params.tags.unwrap_or_default(),
            due_context: params.due_context.unwrap_or_else(|| serde_json::json!({})),
        };
        db.upsert_memory_followup(&followup).map_err(db_error)?;
        Ok(text_result(serde_json::json!({
            "followup": followup
        })))
    }

    #[tool(
        description = "Resolve an open memory follow-up, optionally attaching a resolution note."
    )]
    async fn resolve_memory_followup(
        &self,
        Parameters(params): Parameters<ResolveMemoryFollowupParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let mut followup = db
            .get_memory_followup(&params.id)
            .map_err(db_error)?
            .ok_or_else(|| invalid_params_error("memory follow-up not found"))?;
        followup.status = "resolved".to_string();
        followup.resolved_at_ms = Some(Utc::now().timestamp_millis() as f64);
        if let Some(resolution_note) = params.resolution_note {
            followup.due_context["resolutionNote"] = serde_json::json!(resolution_note);
        }
        db.upsert_memory_followup(&followup).map_err(db_error)?;
        Ok(text_result(serde_json::json!({
            "followup": followup
        })))
    }

    #[tool(
        description = "Return a ranked memory brief for session_start, setup_check, trade_review, or weekly_review. Includes recent sessions, matching patterns, matching insights, and open follow-ups."
    )]
    async fn get_memory_brief(
        &self,
        Parameters(params): Parameters<MemoryBriefParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let brief = memory_build_memory_brief(
            &db,
            MemoryBriefQuery {
                intent: params
                    .intent
                    .ok_or_else(|| invalid_params_error("intent is required"))?,
                session_id: params.session_id,
                setup_id: params.setup_id,
                session_type: params.session_type,
                session_segment: params.session_segment,
                day_type: params.day_type,
                time_bucket: params.time_bucket,
                pre_session_note: params.pre_session_note,
                limit: params.limit.map(|limit| limit.min(10) as usize),
                include_recent_sessions: params.include_recent_sessions,
                include_patterns: params.include_patterns,
                include_insights: params.include_insights,
                include_followups: params.include_followups,
            },
        )
        .map_err(db_error)?;
        Ok(text_result(serde_json::json!(brief)))
    }

    #[tool(
        description = "Build a session-start packet that merges ranked memory, current account/risk context, and contract rollover status. When persisted memory maintenance is dirty (`memoryMaintenance.refreshSuggested`), runs a single bounded `refresh_memory_state` unless `skipMemoryRefreshIfDirty` is true."
    )]
    async fn get_pre_session_briefing(
        &self,
        Parameters(params): Parameters<MemoryBriefParams>,
    ) -> Result<CallToolResult, McpError> {
        let server_contract = self.current_pipeline_contract_metadata();
        let db = self.db.lock().map_err(|_| lock_error())?;
        let mut memory_auto_refreshed = false;
        let maintenance = db.get_memory_maintenance_state().map_err(db_error)?;
        if maintenance.refresh_suggested && !params.skip_memory_refresh_if_dirty.unwrap_or(false) {
            memory_refresh_state(
                &db,
                MemoryRefreshOptions {
                    refresh_patterns: true,
                    refresh_insight_lifecycle: true,
                },
                Some("get_pre_session_briefing"),
            )
            .map_err(db_error)?;
            memory_auto_refreshed = true;
        }
        let memory_brief = memory_build_memory_brief(
            &db,
            MemoryBriefQuery {
                intent: "session_start".to_string(),
                session_id: params.session_id,
                setup_id: params.setup_id,
                session_type: params.session_type,
                session_segment: params.session_segment,
                day_type: params.day_type,
                time_bucket: params.time_bucket,
                pre_session_note: params.pre_session_note,
                limit: params.limit.map(|limit| limit.min(10) as usize),
                include_recent_sessions: params.include_recent_sessions,
                include_patterns: params.include_patterns,
                include_insights: params.include_insights,
                include_followups: params.include_followups,
            },
        )
        .map_err(db_error)?;
        let account_state = db.load_account_state().map_err(db_error)?;
        let risk_state = db.load_risk_state().map_err(db_error)?;
        let (_, active_contract) = self.resolve_contract_cached();
        let data_age_ms = compute_data_age(&db);
        let rollover_status = self.rollover_status_for_date(
            &db,
            &active_contract,
            server_contract.as_ref(),
            &the_desk_backend::et_now_trading_day(),
            Some(data_age_ms),
        )?;
        Ok(text_result(serde_json::json!({
            "memoryBrief": memory_brief,
            "accountState": account_state,
            "riskState": risk_state,
            "memoryAutoRefreshed": memory_auto_refreshed,
            "rolloverStatus": rollover_status
        })))
    }

    #[tool(
        description = "Import broker-exported fills into the trade journal. Accepts an array of fill rows, skips duplicates idempotently, stores raw import rows, and synthesizes normalized round-trip trade entries."
    )]
    async fn import_trade_fills(
        &self,
        Parameters(params): Parameters<ImportTradeFillsParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let timezone: Tz = params
            .timezone
            .as_deref()
            .unwrap_or("America/New_York")
            .parse()
            .map_err(|e| invalid_params_error(format!("invalid timezone: {e}")))?;
        let batch_id = params
            .batch_id
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let source = params.source.unwrap_or_else(|| "imported_fill".to_string());

        let mut fills: Vec<FillSlice> = Vec::new();
        let mut skipped_duplicates = 0usize;
        for row in params.rows {
            if !row.status.eq_ignore_ascii_case("filled") {
                continue;
            }
            let quantity = row.filled_quantity.unwrap_or(0);
            if quantity <= 0 {
                continue;
            }
            let timestamp_ms = parse_import_timestamp(
                row.last_activity_time
                    .as_deref()
                    .unwrap_or(row.entry_time.as_str()),
                timezone,
            )?;
            let base_fingerprint = format!(
                "{}|{}|{}|{:.0}|{:.4}|{}|{}",
                row.symbol,
                row.trade_account.clone().unwrap_or_default(),
                row.service_order_id
                    .clone()
                    .or(row.internal_order_id.clone())
                    .unwrap_or_default(),
                timestamp_ms,
                row.average_fill_price,
                quantity,
                row.buy_sell.to_ascii_lowercase()
            );
            if db
                .imported_fill_exists(&format!("{base_fingerprint}:0"))
                .map_err(db_error)?
            {
                skipped_duplicates += 1;
                continue;
            }
            let raw_payload = serde_json::to_value(&row)
                .map_err(|e| invalid_params_error(format!("fill row serialization failed: {e}")))?;
            fills.push(FillSlice {
                timestamp_ms,
                price: row.average_fill_price,
                quantity,
                symbol: row.symbol,
                trade_account: row.trade_account,
                batch_id: batch_id.clone(),
                fingerprint: base_fingerprint,
                order_side: row.buy_sell,
                open_close: row.open_close,
                service_order_id: row.service_order_id,
                external_order_id: row.exchange_order_id,
                raw_payload,
            });
        }

        fills.sort_by(|a, b| {
            a.timestamp_ms
                .partial_cmp(&b.timestamp_ms)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        if fills.is_empty() {
            return Ok(text_result(serde_json::json!({
                "imported": false,
                "reason": "no new filled rows after duplicate filtering"
            })));
        }

        let session_id = if let Some(session_id) = params.session_id {
            session_id
        } else {
            let first_ts = fills[0].timestamp_ms;
            let session = SessionRecord {
                id: uuid::Uuid::new_v4().to_string(),
                date: trading_day_from_timestamp_ms(first_ts),
                session_type: infer_session_type_label(first_ts),
                start_time: first_ts,
                end_time: fills.last().map(|fill| fill.timestamp_ms),
                recording_path: None,
                pre_session_note: None,
            };
            db.upsert_session(&session).map_err(db_error)?;
            session.id
        };

        db.insert_trade_import_batch(&TradeImportBatchRecord {
            batch_id: batch_id.clone(),
            source: source.clone(),
            imported_at: Utc::now().timestamp_millis() as f64,
            notes: params.notes.unwrap_or_default(),
            fill_count: fills.len() as i64,
        })
        .map_err(db_error)?;

        let mut active_by_key: HashMap<(String, Option<String>), ActiveImportedTrade> =
            HashMap::new();
        let mut imported_trades: Vec<TradeRecord> = Vec::new();

        for fill in fills {
            let key = (fill.symbol.clone(), fill.trade_account.clone());
            let delta = signed_delta_for_fill(&fill.order_side, fill.quantity)?;
            let mut remaining = delta.unsigned_abs() as i64;
            let entry_sign = delta.signum();

            let state = active_by_key
                .entry(key.clone())
                .or_insert_with(|| ActiveImportedTrade {
                    session_id: Some(session_id.clone()),
                    instrument: fill.symbol.clone(),
                    trade_account: fill.trade_account.clone(),
                    direction: if entry_sign > 0 {
                        "long".to_string()
                    } else {
                        "short".to_string()
                    },
                    entry_start_ms: fill.timestamp_ms,
                    last_exit_ms: fill.timestamp_ms,
                    signed_position: 0,
                    entry_qty_total: 0,
                    exit_qty_total: 0,
                    max_open_size: 0,
                    weighted_entry_notional: 0.0,
                    weighted_exit_notional: 0.0,
                    fill_refs: Vec::new(),
                });

            if state.signed_position == 0 {
                state.direction = if entry_sign > 0 {
                    "long".to_string()
                } else {
                    "short".to_string()
                };
                state.entry_start_ms = fill.timestamp_ms;
            }

            if state.signed_position == 0 || state.signed_position.signum() == entry_sign {
                state.signed_position += delta;
                state.entry_qty_total += remaining;
                state.weighted_entry_notional += fill.price * remaining as f64;
                state.max_open_size = state.max_open_size.max(state.signed_position.abs());
                state.fill_refs.push(FillSlice {
                    fingerprint: format!("{}:0", fill.fingerprint),
                    quantity: remaining,
                    ..fill.clone()
                });
                continue;
            }

            let mut split_index = 0usize;
            while remaining > 0 {
                let closable = remaining.min(state.signed_position.abs());
                state.exit_qty_total += closable;
                state.weighted_exit_notional += fill.price * closable as f64;
                state.last_exit_ms = fill.timestamp_ms;
                state.fill_refs.push(FillSlice {
                    fingerprint: format!("{}:{split_index}", fill.fingerprint),
                    quantity: closable,
                    ..fill.clone()
                });
                split_index += 1;
                remaining -= closable;
                state.signed_position += if state.signed_position > 0 {
                    -closable
                } else {
                    closable
                };

                if state.signed_position == 0 {
                    let trade =
                        build_imported_trade_record(state, &source, "Imported from broker fills");
                    db.upsert_trade(&trade).map_err(db_error)?;
                    for fill_ref in &state.fill_refs {
                        if db
                            .imported_fill_exists(&fill_ref.fingerprint)
                            .map_err(db_error)?
                        {
                            continue;
                        }
                        db.insert_imported_fill(&ImportedFillRecord {
                            fingerprint: fill_ref.fingerprint.clone(),
                            batch_id: fill_ref.batch_id.clone(),
                            trade_id: Some(trade.id.clone()),
                            symbol: fill_ref.symbol.clone(),
                            trade_account: fill_ref.trade_account.clone(),
                            fill_time: fill_ref.timestamp_ms,
                            order_side: fill_ref.order_side.clone(),
                            open_close: fill_ref.open_close.clone(),
                            quantity: fill_ref.quantity,
                            price: fill_ref.price,
                            status: "Filled".to_string(),
                            external_order_id: fill_ref.external_order_id.clone(),
                            service_order_id: fill_ref.service_order_id.clone(),
                            raw_payload: fill_ref.raw_payload.clone(),
                        })
                        .map_err(db_error)?;
                    }
                    imported_trades.push(trade);
                    *state = ActiveImportedTrade {
                        session_id: Some(session_id.clone()),
                        instrument: fill.symbol.clone(),
                        trade_account: fill.trade_account.clone(),
                        direction: if entry_sign > 0 {
                            "long".to_string()
                        } else {
                            "short".to_string()
                        },
                        entry_start_ms: fill.timestamp_ms,
                        last_exit_ms: fill.timestamp_ms,
                        signed_position: 0,
                        entry_qty_total: 0,
                        exit_qty_total: 0,
                        max_open_size: 0,
                        weighted_entry_notional: 0.0,
                        weighted_exit_notional: 0.0,
                        fill_refs: Vec::new(),
                    };
                }

                if remaining > 0 && state.signed_position == 0 {
                    state.direction = if entry_sign > 0 {
                        "long".to_string()
                    } else {
                        "short".to_string()
                    };
                    state.entry_start_ms = fill.timestamp_ms;
                    state.signed_position = if entry_sign > 0 {
                        remaining
                    } else {
                        -remaining
                    };
                    state.entry_qty_total = remaining;
                    state.max_open_size = remaining;
                    state.weighted_entry_notional = fill.price * remaining as f64;
                    state.fill_refs.push(FillSlice {
                        fingerprint: format!("{}:{split_index}", fill.fingerprint),
                        quantity: remaining,
                        ..fill.clone()
                    });
                    remaining = 0;
                }
            }
        }

        for state in active_by_key
            .values()
            .filter(|state| state.signed_position != 0)
        {
            let mut trade =
                build_imported_trade_record(state, &source, "Imported from broker fills");
            trade.exit_time = None;
            trade.exit_price = None;
            trade.gross_points = None;
            trade.exit_fill_count = 0;
            db.upsert_trade(&trade).map_err(db_error)?;
            for fill_ref in &state.fill_refs {
                if db
                    .imported_fill_exists(&fill_ref.fingerprint)
                    .map_err(db_error)?
                {
                    continue;
                }
                db.insert_imported_fill(&ImportedFillRecord {
                    fingerprint: fill_ref.fingerprint.clone(),
                    batch_id: fill_ref.batch_id.clone(),
                    trade_id: Some(trade.id.clone()),
                    symbol: fill_ref.symbol.clone(),
                    trade_account: fill_ref.trade_account.clone(),
                    fill_time: fill_ref.timestamp_ms,
                    order_side: fill_ref.order_side.clone(),
                    open_close: fill_ref.open_close.clone(),
                    quantity: fill_ref.quantity,
                    price: fill_ref.price,
                    status: "Filled".to_string(),
                    external_order_id: fill_ref.external_order_id.clone(),
                    service_order_id: fill_ref.service_order_id.clone(),
                    raw_payload: fill_ref.raw_payload.clone(),
                })
                .map_err(db_error)?;
            }
            imported_trades.push(trade);
        }
        let memory_maintenance =
            memory_mark_dirty(&db, true, true, "import_trade_fills").map_err(db_error)?;

        Ok(text_result(serde_json::json!({
            "imported": true,
            "batchId": batch_id,
            "sessionId": session_id,
            "skippedDuplicates": skipped_duplicates,
            "createdTradeCount": imported_trades.len(),
            "trades": imported_trades,
            "memoryMaintenance": memory_maintenance
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
            session_id: resolve_session_id(&db, None)?,
            setup_id: params.setup_id.clone(),
            instrument: None,
            trade_account: None,
            entry_time: now_ms,
            entry_price: params.entry_price,
            exit_time: Some(now_ms),
            exit_price: Some(params.exit_price),
            direction: params.direction.clone(),
            size: params.size,
            max_open_size: Some(params.size),
            stop_price: params.stop_price,
            target_prices: Vec::new(),
            result_r: Some(params.result_r),
            gross_points: Some(if params.direction.eq_ignore_ascii_case("long") {
                (params.exit_price - params.entry_price) * params.size as f64
            } else {
                (params.entry_price - params.exit_price) * params.size as f64
            }),
            planned: true,
            rules_followed: None,
            emotional_state: None,
            thesis: None,
            review_tags: Vec::new(),
            mistake_tags: Vec::new(),
            entry_fill_count: 1,
            exit_fill_count: 1,
            import_batch_id: None,
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
        self.playbook_cache.set_risk_at_limit(new_state.at_limit);
        let memory_maintenance =
            memory_mark_dirty(&db, true, false, "record_trade_result").map_err(db_error)?;

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
            "memoryMaintenance": memory_maintenance,
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
            match db.query_ticks_filtered_scoped(
                params.start_time_ms,
                params.end_time_ms,
                params.price_low,
                params.price_high,
                params.session_date.as_deref(),
                params.root_symbol.as_deref(),
                params.contract_symbol.as_deref(),
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
                        "rootSymbol": params.root_symbol,
                        "contractSymbol": params.contract_symbol,
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
        description = "Feed health diagnostics: SCID path status, file metadata, latest DB tick timestamp, ingest lag, freshness/source state, and contract rollover status."
    )]
    async fn get_feed_health(&self) -> Result<CallToolResult, McpError> {
        let (config, contract) = self.resolve_contract_cached();
        let reader = ScidReader::with_price_scale(
            std::path::PathBuf::from(&contract.scid_path),
            config.price_scale,
        );
        let scid_path = reader.path().to_string_lossy().to_string();
        let meta = std::fs::metadata(reader.path()).ok();
        let file_exists = meta.is_some();
        let file_size_bytes = meta.as_ref().map(|m| m.len()).unwrap_or(0);
        let file_modified_ms = meta
            .and_then(|m| m.modified().ok())
            .and_then(|mtime| mtime.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as f64)
            .unwrap_or(-1.0);

        let server_contract = self.current_pipeline_contract_metadata();
        let db = self.db.lock().map_err(|_| lock_error())?;
        let tick_count = db.raw_tick_count().unwrap_or(0);
        let latest_tick_ms = db.latest_tick_timestamp_ms().ok().flatten();
        let data_age_ms = compute_data_age(&db);
        let rollover_status = self.rollover_status_for_date(
            &db,
            &contract,
            server_contract.as_ref(),
            &the_desk_backend::et_now_trading_day(),
            Some(data_age_ms),
        )?;
        let source_state = if !file_exists {
            "missing"
        } else {
            freshness_status_from_age(data_age_ms)
        };

        let fr = &self.feed_runtime;
        let last_scid_tick = tick_ms_from_bits(fr.last_scid_tick_ms_bits.load(Ordering::Acquire));
        let last_depth_ts =
            tick_ms_from_bits(fr.last_depth_timestamp_ms_bits.load(Ordering::Acquire));
        let scid_offset = fr.scid_tail_offset.load(Ordering::Acquire);
        let scid_len = fr.scid_file_len.load(Ordering::Acquire);
        let scid_resets = fr.scid_tail_reset_count.load(Ordering::Acquire);
        let shrink_len = fr.scid_last_shrink_len.load(Ordering::Acquire);
        let now_wall_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let pipeline_contended = fr.pipeline_lock_recently_contended(now_wall_ms);
        let monotonicity = fr.monotonicity_snapshot();
        let runtime_event_stats = self.runtime_events.stats();

        Ok(text_result(serde_json::json!({
            "liveDataSource": "scid",
            "rootSymbol": contract.root_symbol,
            "contractSymbol": contract.contract_symbol,
            "contractMonth": contract.contract_month,
            "symbolResolutionMode": contract.symbol_resolution_mode,
            "symbolResolutionSource": contract.symbol_resolution_source,
            "configuredSymbol": contract.configured_symbol,
            "activeSymbolOverride": contract.active_symbol_override,
            "scidPath": scid_path,
            "fileExists": file_exists,
            "fileSizeBytes": file_size_bytes,
            "fileModifiedMs": file_modified_ms,
            "depthFileCount": contract.depth_file_count,
            "warnings": contract.warnings,
            "rolloverStatus": rollover_status,
            "latestDbTickTimestampMs": latest_tick_ms,
            "dbTickCount": tick_count,
            "ingestLagMs": data_age_ms,
            "sourceState": source_state,
            "dataAgeMs": data_age_ms,
            "lastScidTickTimestampMs": last_scid_tick,
            "lastDepthTimestampMs": last_depth_ts,
            "scidTailOffsetBytes": scid_offset,
            "scidFileLenBytes": scid_len,
            "scidTailResetCount": scid_resets,
            "scidLastShrinkFileLenBytes": shrink_len,
            "skippedNonMonotonicTicks": monotonicity.skipped_non_monotonic_ticks,
            "duplicateTimestampTicks": monotonicity.duplicate_timestamp_ticks,
            "backwardTimestampTicks": monotonicity.backward_timestamp_ticks,
            "lastNonMonotonicTimestampMs": monotonicity.last_non_monotonic_timestamp_ms,
            "pipelineLockRecentlyContended": pipeline_contended,
            "recentRuntimeEventCount": runtime_event_stats.recent_event_count,
            "lastRuntimeWarningAtMs": runtime_event_stats.last_warning_at_ms,
            "lastRuntimeErrorAtMs": runtime_event_stats.last_error_at_ms,
            "lastRuntimeWarning": &runtime_event_stats.last_warning,
            "lastRuntimeError": &runtime_event_stats.last_error,
            "recentRuntimeEventNameCounts": &runtime_event_stats.recent_event_name_counts
        })))
    }

    #[tool(
        description = "Recent MCP runtime diagnostics: structured startup, feed, session-boundary, setup-transition, background-job, and worker events. Use this for post-mortems after get_feed_health/validate_data_integrity flags a problem; not for raw tick data."
    )]
    async fn get_runtime_events(
        &self,
        Parameters(params): Parameters<RuntimeEventsParams>,
    ) -> Result<CallToolResult, McpError> {
        let limit = params.limit.unwrap_or(50).clamp(1, 500);
        if params.level.is_some() && params.min_level.is_some() {
            return Err(invalid_params_error(
                "level and minLevel are mutually exclusive; use level for exact matches or minLevel for severity-or-higher queries",
            ));
        }
        let level = match params.level.as_deref() {
            Some(level) => Some(
                level
                    .parse::<RuntimeEventLevel>()
                    .map_err(invalid_params_error)?,
            ),
            None => None,
        };
        let min_level = match params.min_level.as_deref() {
            Some(level) => Some(
                level
                    .parse::<RuntimeEventLevel>()
                    .map_err(invalid_params_error)?,
            ),
            None => None,
        };
        let filter = RuntimeEventFilter {
            since_ms: params.since_ms,
            level,
            min_level,
            category: params.category.clone(),
            event_name: params.event_name.clone(),
            limit,
        };

        let recent_events = self.runtime_events.query(&filter);
        let include_persisted = params.include_persisted.unwrap_or(false);
        let persisted_events = if include_persisted {
            let db = self.db.lock().map_err(|_| lock_error())?;
            db.query_runtime_events(&filter).map_err(db_error)?
        } else {
            Vec::new()
        };

        let mut events: Vec<serde_json::Value> = recent_events
            .iter()
            .cloned()
            .map(|event| runtime_event_json(event, "memory"))
            .collect();
        events.extend(
            persisted_events
                .iter()
                .cloned()
                .map(|event| runtime_event_json(event, "sqlite")),
        );

        Ok(text_result(serde_json::json!({
            "events": events,
            "recentCount": recent_events.len(),
            "persistedCount": persisted_events.len(),
            "includePersisted": include_persisted,
            "limit": limit,
            "filters": {
                "sinceMs": filter.since_ms,
                "level": filter.level.map(|l| l.as_str()),
                "minLevel": filter.min_level.map(|l| l.as_str()),
                "category": filter.category,
                "eventName": filter.event_name,
            },
            "stats": self.runtime_events.stats()
        })))
    }

    #[tool(
        description = "Validate active futures contract rollover state before trusting prior-session references. Compares freshly resolved contract, live pipeline contract, current-contract prior levels, same-root legacy levels, resolver warnings, and feed freshness. Returns whether prior-day references are authoritative, legacy-context-only, or should be cleared/backfilled."
    )]
    async fn get_contract_rollover_status(&self) -> Result<CallToolResult, McpError> {
        let (_, contract) = self.resolve_contract_cached();
        let server_contract = self.current_pipeline_contract_metadata();
        let db = self.db.lock().map_err(|_| lock_error())?;
        let data_age_ms = compute_data_age(&db);
        let status = self.rollover_status_for_date(
            &db,
            &contract,
            server_contract.as_ref(),
            &the_desk_backend::et_now_trading_day(),
            Some(data_age_ms),
        )?;
        Ok(text_result(serde_json::json!(status)))
    }

    #[tool(
        description = "Validate active futures contract rollover state before trusting prior-session references. Alias of get_contract_rollover_status using validate_* taxonomy for pre-session safety gates."
    )]
    async fn validate_contract_rollover(&self) -> Result<CallToolResult, McpError> {
        self.get_contract_rollover_status().await
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
        description = "Report raw_ticks DB coverage vs the active .scid file for the configured contract: SCID first/last timestamps, DB min/max tick times, session_summary date span, and missing ranges (prefix/suffix only — internal tape holes are not detected). Optional startDate/endDate (YYYY-MM-DD) clip."
    )]
    async fn get_raw_tick_ingest_gaps(
        &self,
        Parameters(params): Parameters<RawTickIngestGapParams>,
    ) -> Result<CallToolResult, McpError> {
        let config = load_feed_config();
        let contract = resolve_contract_metadata(&config);
        let reader = ScidReader::from_feed_config(&config);
        if !reader.path().exists() {
            return Ok(no_data(
                "SCID file not found. Ensure Sierra Chart data path is configured in ~/.the-desk/config.toml",
            ));
        }
        let db = self.db.lock().map_err(|_| lock_error())?;
        let report = scid_tick_ingest::analyze_tick_ingest_gaps(
            &reader,
            &db,
            &contract,
            params.start_date.as_deref(),
            params.end_date.as_deref(),
        )
        .map_err(db_error)?;
        Ok(text_result(
            serde_json::to_value(&report).unwrap_or_else(|_| serde_json::json!({})),
        ))
    }

    #[tool(
        description = "Scan the active Sierra .scid file in byte order for equal or backward timestamps. Returns anomaly counts, worst backward delta, and capped samples for the requested date clip."
    )]
    async fn scan_scid_timestamp_anomalies(
        &self,
        Parameters(params): Parameters<ScanScidTimestampAnomaliesParams>,
    ) -> Result<CallToolResult, McpError> {
        let config = load_feed_config();
        let contract = resolve_contract_metadata(&config);
        let reader = ScidReader::from_feed_config(&config);
        if !reader.path().exists() {
            return Ok(no_data(
                "SCID file not found. Ensure Sierra Chart data path is configured in ~/.the-desk/config.toml",
            ));
        }

        let (start_ms, end_ms_exclusive) = backfill::parse_backfill_date_range(
            params.start_date.as_deref(),
            params.end_date.as_deref(),
        )
        .map_err(|e| invalid_params_error(e.to_string()))?;
        let sample_limit = params.max_events_reported.unwrap_or(20).min(200);
        let reader_for_scan = reader.clone();
        let report = tokio::task::spawn_blocking(move || {
            scid_timestamp_diagnostics::scan_scid_timestamp_anomalies(
                &reader_for_scan,
                start_ms,
                end_ms_exclusive,
                sample_limit,
            )
        })
        .await
        .map_err(|e| db_error(format!("timestamp anomaly scan task join: {e}")))?
        .map_err(db_error)?;

        let status = if report.monotonicity.has_violations() {
            "warning"
        } else {
            "ok"
        };
        let mut result = serde_json::json!({
            "status": status,
            "liveDataSource": "scid",
            "rootSymbol": contract.root_symbol,
            "contractSymbol": contract.contract_symbol,
            "contractMonth": contract.contract_month,
            "scidPath": report.scid_path,
            "scanStartMs": report.scan_start_ms,
            "scanEndMsExclusive": report.scan_end_ms_exclusive,
            "scidFirstTimestampMs": report.scid_first_timestamp_ms,
            "scidLastTimestampMs": report.scid_last_timestamp_ms,
            "recordsScanned": report.records_scanned,
            "acceptedTicks": report.monotonicity.accepted_ticks,
            "skippedNonMonotonicTicks": report.monotonicity.skipped_non_monotonic_ticks,
            "duplicateTimestampTicks": report.monotonicity.duplicate_timestamp_ticks,
            "backwardTimestampTicks": report.monotonicity.backward_timestamp_ticks,
            "largestBackwardDeltaMs": report.monotonicity.worst_backward_delta_ms,
            "lastNonMonotonicTimestampMs": report.monotonicity.last_non_monotonic_timestamp_ms,
            "samples": report.monotonicity.samples,
            "persistedToValidationRuns": false,
        });

        if params.persist_result.unwrap_or(false) {
            let now_ms = chrono::Utc::now().timestamp_millis() as f64;
            let db = self.db.lock().map_err(|_| lock_error())?;
            db.insert_validation_run(now_ms, status, &result)
                .map_err(db_error)?;
            result["persistedToValidationRuns"] = serde_json::json!(true);
        }

        Ok(text_result(result))
    }

    #[tool(
        description = "Load trades from the Sierra .scid file into SQLite raw_ticks using INSERT OR IGNORE. Default onlyGaps=true fills prefix/suffix gaps vs existing rows for the current contract; onlyGaps=false scans the full date clip. Separate from backfill_history (which replays pipelines / session summaries without persisting raw ticks). Large ingests: set waitForCompletion=false to avoid MCP timeouts (check dbTickCount via get_session_summary)."
    )]
    async fn ingest_raw_ticks_from_scid(
        &self,
        Parameters(params): Parameters<IngestRawTicksParams>,
    ) -> Result<CallToolResult, McpError> {
        let config = load_feed_config();
        let reader = ScidReader::from_feed_config(&config);
        if !reader.path().exists() {
            return Ok(no_data(
                "SCID file not found. Ensure Sierra Chart data path is configured in ~/.the-desk/config.toml",
            ));
        }
        let only_gaps = params.only_gaps.unwrap_or(true);
        let wait = params.wait_for_completion.unwrap_or(true);
        let start_date = params.start_date.clone();
        let end_date = params.end_date.clone();

        if wait {
            let db_path = Arc::clone(&self.db_path);
            let out = tokio::task::spawn_blocking(move || {
                let config = load_feed_config();
                let contract = resolve_contract_metadata(&config);
                let reader = ScidReader::from_feed_config(&config);
                let db = Database::open(db_path.as_str()).map_err(|e| e.to_string())?;
                scid_tick_ingest::run_tick_ingest(
                    &reader,
                    &db,
                    &contract,
                    TickIngestParams {
                        start_date: start_date.as_deref(),
                        end_date: end_date.as_deref(),
                        only_gaps,
                    },
                )
                .map_err(|e| e.to_string())
            })
            .await
            .map_err(|e| db_error(format!("ingest task join: {e}")))?
            .map_err(db_error)?;
            let (report, ingest) = out;
            return Ok(text_result(serde_json::json!({
                "gapReport": report,
                "ingest": ingest,
                "onlyGaps": only_gaps,
            })));
        }

        let db_path = Arc::clone(&self.db_path);
        let runtime_events = Arc::clone(&self.runtime_events);
        record_runtime_event(
            &runtime_events,
            Some(&self.db),
            RuntimeEventLevel::Info,
            "raw_tick_ingest.started",
            "raw_tick_ingest",
            "Raw tick ingest started in the background.",
            serde_json::json!({
                "onlyGaps": only_gaps,
                "startDate": start_date.clone(),
                "endDate": end_date.clone(),
            }),
        );
        tokio::task::spawn(async move {
            let runtime_events_blocking = Arc::clone(&runtime_events);
            let res = tokio::task::spawn_blocking(move || {
                let config = load_feed_config();
                let contract = resolve_contract_metadata(&config);
                let reader = ScidReader::from_feed_config(&config);
                let db = match Database::open(db_path.as_str()) {
                    Ok(d) => d,
                    Err(e) => {
                        record_runtime_event(
                            &runtime_events_blocking,
                            None,
                            RuntimeEventLevel::Error,
                            "raw_tick_ingest.failed",
                            "raw_tick_ingest",
                            "Raw tick ingest could not open SQLite.",
                            serde_json::json!({ "error": e.to_string() }),
                        );
                        return;
                    }
                };
                match scid_tick_ingest::run_tick_ingest(
                    &reader,
                    &db,
                    &contract,
                    TickIngestParams {
                        start_date: start_date.as_deref(),
                        end_date: end_date.as_deref(),
                        only_gaps,
                    },
                ) {
                    Ok((rep, ing)) => {
                        let event = RuntimeEvent::new(
                                RuntimeEventLevel::Info,
                                "raw_tick_ingest.finished",
                                "raw_tick_ingest",
                                "Raw tick ingest finished.",
                                serde_json::json!({
                                    "gapCount": rep.gaps.len(),
                                    "recordsScanned": ing.as_ref().map(|i| i.scid_records_scanned).unwrap_or(0),
                                    "ticksSubmitted": ing.as_ref().map(|i| i.ticks_submitted_to_insert).unwrap_or(0),
                                }),
                            );
                        if let Some(recorded) = runtime_events_blocking.record(event) {
                            persist_runtime_event_in_db(&runtime_events_blocking, &db, &recorded);
                        }
                    }
                    Err(e) => {
                        let event = RuntimeEvent::new(
                                RuntimeEventLevel::Error,
                                "raw_tick_ingest.failed",
                                "raw_tick_ingest",
                                "Raw tick ingest failed.",
                                serde_json::json!({ "error": e.to_string() }),
                            );
                        if let Some(recorded) = runtime_events_blocking.record(event) {
                            persist_runtime_event_in_db(&runtime_events_blocking, &db, &recorded);
                        }
                    }
                }
            })
            .await;
            if let Err(e) = res {
                record_runtime_event(
                    &runtime_events,
                    None,
                    RuntimeEventLevel::Error,
                    "raw_tick_ingest.failed",
                    "raw_tick_ingest",
                    "Raw tick ingest task failed to join.",
                    serde_json::json!({ "error": e.to_string() }),
                );
            }
        });
        Ok(text_result(serde_json::json!({
            "status": "started",
            "onlyGaps": only_gaps,
            "message": "Ingest running in background; use get_raw_tick_ingest_gaps or get_session_summary to verify dbTickCount.",
        })))
    }

    #[tool(
        description = "Register or dry-run validate a research hypothesis as an inactive per-version SetupDefinition. First-slice scope is RTH only; use run_backtest with the returned setupId to execute."
    )]
    async fn register_hypothesis(
        &self,
        Parameters(params): Parameters<RegisterHypothesisParams>,
    ) -> Result<CallToolResult, McpError> {
        let metadata: HypothesisMetadata = serde_json::from_value(params.metadata)
            .map_err(|e| invalid_params_error(e.to_string()))?;
        let setup_definition: SetupDefinition = serde_json::from_value(params.setup_definition)
            .map_err(|e| invalid_params_error(e.to_string()))?;
        let request = RegisterHypothesisRequest {
            metadata,
            setup_definition,
            dry_run: params.dry_run.unwrap_or(false),
        };
        let db = self.db.lock().map_err(|_| lock_error())?;
        let response =
            hypothesis_register_hypothesis(&db, request).map_err(invalid_params_error)?;
        drop(db);
        if response.registered {
            record_runtime_event(
                &self.runtime_events,
                Some(&self.db),
                RuntimeEventLevel::Info,
                "hypothesis.registered",
                "hypothesis",
                "Hypothesis registered.",
                serde_json::json!({
                    "setupId": response.setup_id,
                    "conditionFingerprint": response.condition_fingerprint,
                }),
            );
        }
        Ok(text_result(
            serde_json::to_value(response).unwrap_or_else(|_| serde_json::json!({})),
        ))
    }

    #[tool(
        description = "Summarize one completed hypothesis backtest run by explicit setupId and jobId. Reads signal_outcomes/backtest_runs and returns gate metrics without changing lifecycle."
    )]
    async fn summarize_hypothesis_run(
        &self,
        Parameters(params): Parameters<HypothesisRunParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let summary =
            hypothesis_summarize_run(&db, &params.setup_id, &params.job_id).map_err(db_error)?;
        drop(db);
        record_runtime_event(
            &self.runtime_events,
            Some(&self.db),
            RuntimeEventLevel::Info,
            "hypothesis.run_summarized",
            "hypothesis",
            "Hypothesis run summarized.",
            serde_json::json!({
                "setupId": params.setup_id,
                "jobId": params.job_id,
                "passed": summary.gate.passed,
                "reason": summary.gate.reason,
            }),
        );
        Ok(text_result(
            serde_json::to_value(summary).unwrap_or_else(|_| serde_json::json!({})),
        ))
    }

    #[tool(
        description = "List registered research hypotheses, optionally filtered by lifecycle (hypothesis/draft/failed/rejectedByHuman/retired/active). Use before proposing new hypotheses to avoid repeating rejected ideas."
    )]
    async fn list_hypotheses(
        &self,
        Parameters(params): Parameters<ListHypothesesParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let rows = db
            .list_research_hypotheses(params.lifecycle.as_deref())
            .map_err(db_error)?;
        let count = rows.len();
        Ok(text_result(serde_json::json!({
            "hypotheses": rows,
            "count": count,
            "lifecycle": params.lifecycle,
        })))
    }

    #[tool(
        description = "Evaluate the strict promotion gate for a hypothesis run and transition hypothesis->draft on pass, or hypothesis->failed on fail. Requires explicit setupId and completed jobId."
    )]
    async fn propose_draft_setup(
        &self,
        Parameters(params): Parameters<HypothesisRunParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let result = match the_desk_backend::research::hypothesis::propose_draft_setup(
            &db,
            &params.setup_id,
            &params.job_id,
        ) {
            Ok(result) => result,
            Err(err) if err.contains("engine_version_drift") => {
                drop(db);
                record_runtime_event(
                    &self.runtime_events,
                    Some(&self.db),
                    RuntimeEventLevel::Warn,
                    "hypothesis.engine_version_drift",
                    "hypothesis",
                    "Hypothesis draft proposal rejected because engine version drifted.",
                    serde_json::json!({
                        "setupId": params.setup_id,
                        "jobId": params.job_id,
                        "error": err.clone(),
                    }),
                );
                return Err(invalid_params_error(err));
            }
            Err(err) => return Err(db_error(err)),
        };
        drop(db);
        let passed = result
            .get("gate")
            .and_then(|g| g.get("passed"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let reason = result
            .get("gate")
            .and_then(|g| g.get("reason"))
            .cloned()
            .unwrap_or_else(|| serde_json::json!("unknown"));
        record_runtime_event(
            &self.runtime_events,
            Some(&self.db),
            if passed {
                RuntimeEventLevel::Info
            } else {
                RuntimeEventLevel::Warn
            },
            if passed {
                "hypothesis.gate_passed"
            } else {
                "hypothesis.gate_failed"
            },
            "hypothesis",
            "Hypothesis promotion gate evaluated.",
            serde_json::json!({
                "setupId": params.setup_id,
                "jobId": params.job_id,
                "passed": passed,
                "reason": reason,
            }),
        );
        if passed {
            record_runtime_event(
                &self.runtime_events,
                Some(&self.db),
                RuntimeEventLevel::Info,
                "hypothesis.promoted_to_draft",
                "hypothesis",
                "Hypothesis promoted to inactive draft setup.",
                serde_json::json!({
                    "setupId": params.setup_id,
                    "jobId": params.job_id,
                }),
            );
        }
        Ok(text_result(result))
    }

    #[tool(
        description = "Activate an inactive draft setup after human confirmation. Re-checks cached engine-version freshness before setting active=true."
    )]
    async fn activate_draft_setup(
        &self,
        Parameters(params): Parameters<ActivateDraftSetupParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let result = match hypothesis_activate_draft_setup(
            &db,
            &params.setup_id,
            &params.trader_confirmation,
        ) {
            Ok(result) => result,
            Err(err) if err.contains("engine_version_drift") => {
                drop(db);
                record_runtime_event(
                    &self.runtime_events,
                    Some(&self.db),
                    RuntimeEventLevel::Warn,
                    "hypothesis.engine_version_drift",
                    "hypothesis",
                    "Draft activation rejected because engine version drifted.",
                    serde_json::json!({
                        "setupId": params.setup_id,
                        "error": err.clone(),
                    }),
                );
                return Err(invalid_params_error(err));
            }
            Err(err) => return Err(invalid_params_error(err)),
        };
        drop(db);
        record_runtime_event(
            &self.runtime_events,
            Some(&self.db),
            RuntimeEventLevel::Info,
            "hypothesis.activated",
            "hypothesis",
            "Draft setup activated by trader confirmation.",
            serde_json::json!({ "setupId": params.setup_id }),
        );
        self.hydrate_playbook_runtime_cache()?;
        Ok(text_result(result))
    }

    #[tool(
        description = "Manually transition a hypothesis/draft to rejectedByHuman or retired with a required reason. Does not activate setups."
    )]
    async fn set_hypothesis_lifecycle(
        &self,
        Parameters(params): Parameters<SetHypothesisLifecycleParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let result =
            hypothesis_set_lifecycle(&db, &params.setup_id, &params.target, &params.reason)
                .map_err(invalid_params_error)?;
        drop(db);
        record_runtime_event(
            &self.runtime_events,
            Some(&self.db),
            RuntimeEventLevel::Info,
            if params.target == "retired" {
                "hypothesis.retired"
            } else {
                "hypothesis.rejected"
            },
            "hypothesis",
            "Hypothesis lifecycle changed manually.",
            serde_json::json!({
                "setupId": params.setup_id,
                "target": params.target,
            }),
        );
        self.hydrate_playbook_runtime_cache()?;
        Ok(text_result(result))
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
        description = "Compare current session structure against similar historical sessions. Uses multi-dimensional similarity: IB range, day type, profile shape, balance state, RVOL ratio, session delta sign, single prints direction. Returns the most similar past sessions, outcomes, and research metadata including rows considered, result cap, and truncation status."
    )]
    async fn compare_sessions(
        &self,
        Parameters(params): Parameters<CompareSessionsParams>,
    ) -> Result<CallToolResult, McpError> {
        let snapshot = self.current_snapshot_value();
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
        match research::compare_sessions_multi_with_meta(&db, &query, max) {
            Ok(result) => {
                let count = result.results.len();
                Ok(text_result(serde_json::json!({
                    "queryDimensions": {
                        "ibRange": ib_range,
                        "dayType": params.current_day_type,
                        "profileShape": query.profile_shape,
                        "balanceState": query.balance_state,
                        "rvolRatio": query.rvol_ratio,
                    },
                    "similarSessions": result.results,
                    "count": count,
                    "meta": result.meta,
                })))
            }
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Query how often a market event occurs. Returns total occurrences, sessions with event, per-session average, percentage of sessions, and research metadata. Session counts use resolved trading-day/session context under the requested scope; exact duplicate market-event rows are ignored by DB identity constraints, but distinct occurrences of the same phenomenon still count separately. Structural event types: *_test (level tests), ib_extension_hit, ib_formed, or_formed, new_session_high/low, day_type_change, poor_high/low_detected, excess_high/low_detected, or5_mid_retest, dnp_cross, rvol_spike. Flow event types: absorption_detected/absorption_confirmed/absorption_invalidated (metadata.eventSubtype: absorption/exhaustion/delta_divergence), pinch_detected (metadata.timeframe: 1m/5m/15m/30m), acceleration_zone_created, acceleration_zone_held, large_trade_cluster."
    )]
    async fn query_event_frequency(
        &self,
        Parameters(params): Parameters<FrequencyParams>,
    ) -> Result<CallToolResult, McpError> {
        validate_ymd_range(
            "startDate",
            params.start_date.as_deref(),
            "endDate",
            params.end_date.as_deref(),
        )?;
        let scope = build_session_scope_filter(&params.session_scope)?;
        let event_type = parse_research_event_type(&params.event_type)?;
        let db = self.db.lock().map_err(|_| lock_error())?;
        match research::event_frequency(
            &db,
            &event_type,
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
        description = "Conditional probability query: 'When event X happens N+ times in a resolved trading-day/session unit, how often does outcome Y occur in the matching session summary?' Example: 'If IB-mid is tested 3+ times, how often do we close above IB-mid?' Returns probability, sample size, counts, and metadata notes for missing summaries or truncation."
    )]
    async fn query_conditional(
        &self,
        Parameters(params): Parameters<ConditionalParams>,
    ) -> Result<CallToolResult, McpError> {
        validate_ymd_range(
            "startDate",
            params.start_date.as_deref(),
            "endDate",
            params.end_date.as_deref(),
        )?;
        let scope = build_session_scope_filter(&params.session_scope)?;
        let event_type = parse_research_event_type(&params.event_type)?;
        let min_count = parse_research_min_count(params.min_count)?;
        let outcome_field = parse_research_outcome_field(&params.outcome_field)?;
        let outcome_value = parse_non_empty_string("outcomeValue", &params.outcome_value)?;
        let db = self.db.lock().map_err(|_| lock_error())?;
        match research::conditional_probability(
            &db,
            &event_type,
            min_count,
            &outcome_field,
            &outcome_value,
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
        description = "Distribution of a numeric metric from session summaries. Returns mean, median, population stddev, Type-7 linear-interpolation percentiles (10/25/75/90), min, max, and metadata. Metrics: ib_range, session_delta, total_volume, rvol_ratio, tick_count, vwap_close, etc."
    )]
    async fn query_distribution(
        &self,
        Parameters(params): Parameters<DistributionParams>,
    ) -> Result<CallToolResult, McpError> {
        validate_ymd_range(
            "startDate",
            params.start_date.as_deref(),
            "endDate",
            params.end_date.as_deref(),
        )?;
        let scope = build_session_scope_filter(&params.session_scope)?;
        let metric = parse_distribution_metric(&params.metric)?;
        let db = self.db.lock().map_err(|_| lock_error())?;
        match research::metric_distribution(
            &db,
            &metric,
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
        description = "Distribution of R-results from signal_outcomes for a setup. Answers: 'When setup X fires, what is the distribution of R-results?' Returns mean, median, population stddev, Type-7 percentiles, and metadata. Requires signal_outcomes to be populated (run backtest or live tracking)."
    )]
    async fn query_signal_outcome_distribution(
        &self,
        Parameters(params): Parameters<SignalOutcomeDistributionParams>,
    ) -> Result<CallToolResult, McpError> {
        validate_ymd_range(
            "startDate",
            params.start_date.as_deref(),
            "endDate",
            params.end_date.as_deref(),
        )?;
        let scope = build_session_scope_filter(&params.session_scope)?;
        let setup_id = parse_non_empty_string("setupId", &params.setup_id)?;
        let db = self.db.lock().map_err(|_| lock_error())?;
        match research::signal_outcome_distribution(
            &db,
            &setup_id,
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
        description = "Conditional win rate for signal outcomes: when setup X fires and the matching resolved trading-day/session summary has field=value (e.g. day_type=Trend), what is the win rate? Joins signal_outcomes to session_summaries by compound session key and returns research metadata. Requires signal_outcomes to be populated."
    )]
    async fn query_signal_outcome_conditional(
        &self,
        Parameters(params): Parameters<SignalOutcomeConditionalParams>,
    ) -> Result<CallToolResult, McpError> {
        validate_ymd_range(
            "startDate",
            params.start_date.as_deref(),
            "endDate",
            params.end_date.as_deref(),
        )?;
        let scope = build_session_scope_filter(&params.session_scope)?;
        let setup_id = parse_non_empty_string("setupId", &params.setup_id)?;
        let session_field = parse_signal_outcome_session_field(&params.session_field)?;
        let field_value = parse_non_empty_string("fieldValue", &params.field_value)?;
        let db = self.db.lock().map_err(|_| lock_error())?;
        match research::signal_outcome_conditional(
            &db,
            &setup_id,
            &session_field,
            &field_value,
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
        description = "Outcome excursion diagnostics for signal outcomes. Returns distributions for max favorable excursion (MFE), max adverse excursion (MAE), time-to-outcome (minutes), and MFE/MAE ratio, plus resolved outcome breakdown and top-level research metadata. Use to evaluate execution quality and target/stop behavior."
    )]
    async fn query_signal_outcome_excursions(
        &self,
        Parameters(params): Parameters<SignalOutcomeExcursionsParams>,
    ) -> Result<CallToolResult, McpError> {
        validate_ymd_range(
            "startDate",
            params.start_date.as_deref(),
            "endDate",
            params.end_date.as_deref(),
        )?;
        let scope = build_session_scope_filter(&params.session_scope)?;
        let setup_id = parse_optional_non_empty_string("setupId", params.setup_id.as_deref())?;
        let db = self.db.lock().map_err(|_| lock_error())?;
        match research::signal_outcome_excursions(
            &db,
            setup_id.as_deref(),
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
        validate_ymd_range(
            "startDate",
            params.start_date.as_deref(),
            "endDate",
            params.end_date.as_deref(),
        )?;
        let scope = build_session_scope_filter(&params.session_scope)?;
        let start_date = scope
            .as_ref()
            .and_then(|s| s.trading_day_start.as_deref())
            .or(params.start_date.as_deref());
        let end_date = scope
            .as_ref()
            .and_then(|s| s.trading_day_end.as_deref())
            .or(params.end_date.as_deref());
        let day_type = parse_optional_non_empty_string("dayType", params.day_type.as_deref())?;
        let db = self.db.lock().map_err(|_| lock_error())?;
        let limit = parse_bounded_limit("limit", params.limit, 20, MAX_RESEARCH_RESULT_LIMIT)?;
        match db.list_session_summaries_scoped(
            start_date,
            end_date,
            day_type.as_deref(),
            scope.as_ref().and_then(|s| s.session_type.as_deref()),
            limit,
            scope.as_ref(),
        ) {
            Ok(sessions) => {
                let count = sessions.len();
                let mut previous_contract: Option<String> = None;
                let summaries: Vec<serde_json::Value> = sessions
                    .into_iter()
                    .map(|s| {
                        let rollover_boundary = previous_contract
                            .as_deref()
                            .map(|prev| prev != s.contract_symbol)
                            .unwrap_or(false);
                        previous_contract = Some(s.contract_symbol.clone());
                        serde_json::json!({
                            "sessionDate": s.session_date,
                            "sessionType": s.session_type,
                            "rootSymbol": s.root_symbol,
                            "contractSymbol": s.contract_symbol,
                            "contractMonth": s.contract_month,
                            "symbolResolutionMode": s.symbol_resolution_mode,
                            "carryForwardLevelsValid": s.carry_forward_levels_valid,
                            "rolloverWarning": s.rollover_warning,
                            "rolloverBoundary": rollover_boundary,
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
        validate_ymd_range(
            "startDate",
            params.start_date.as_deref(),
            "endDate",
            params.end_date.as_deref(),
        )?;
        let scope = build_session_scope_filter(&params.session_scope)?;
        let sort_by = parse_setup_perf_sort(params.sort_by.as_deref())?;
        let min_resolved =
            parse_nonnegative_i64("minResolved", params.min_resolved, 0, MAX_MIN_RESOLVED)?;
        let limit = parse_bounded_limit("limit", params.limit, 50, MAX_RESEARCH_RESULT_LIMIT)?;
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

            let session_delta = pipelines.delta.session_delta();
            drop(pipelines);

            let mut out = serde_json::json!({
                "price": price,
                "deltaAtPrice": delta,
                "confirmsBuy": confirms_buy,
                "confirmsSell": confirms_sell,
                "sessionDelta": session_delta,
                "topPricesByDelta": top,
            });
            if let Some(r) = self.resolve_live_market_view() {
                merge_tool_live_metadata(&mut out, &r);
            } else {
                out["dataAgeMs"] = serde_json::json!(self.data_age_from_db_or_atomic());
            }
            return Ok(text_result(out));
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
            let both = session_confirms && price_confirms;
            drop(pipelines);

            let mut out = serde_json::json!({
                "sessionDeltaConfirms": session_confirms,
                "sessionDelta": session_delta,
                "priceLevelDeltaConfirms": price_confirms,
                "deltaAtPrice": price_delta,
                "price": price,
                "bothConfirm": both,
                "direction": if is_buy { "long" } else { "short" },
            });
            if let Some(r) = self.resolve_live_market_view() {
                merge_tool_live_metadata(&mut out, &r);
            } else {
                out["dataAgeMs"] = serde_json::json!(self.data_age_from_db_or_atomic());
            }
            return Ok(text_result(out));
        }

        // Fallback: session-level only
        if let Some(r) = self.resolve_live_market_view() {
            let s = &r.snapshot;
            let session_delta = s
                .get("sessionDelta")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let confirmed = if is_buy {
                session_delta > 0.0
            } else {
                session_delta < 0.0
            };
            let mut out = serde_json::json!({
                "sessionDeltaConfirms": confirmed,
                "sessionDelta": session_delta,
                "direction": if is_buy { "long" } else { "short" },
                "note": "Price-level delta not available (pipeline not live).",
            });
            merge_tool_live_metadata(&mut out, &r);
            return Ok(text_result(out));
        }
        Ok(no_data("No delta data available"))
    }

    #[tool(
        description = "Full setup context for a named setup. Returns all computed data relevant to that setup type: OR5 levels, delta confirmation, RVOL, day type, nearby zones, risk state. One call = everything needed to discuss a potential trade."
    )]
    async fn get_setup_context(
        &self,
        Parameters(params): Parameters<SetupContextParams>,
    ) -> Result<CallToolResult, McpError> {
        let r = self.resolve_live_market_view();
        let snapshot = r.as_ref().map(|v| v.snapshot.clone()).or_else(|| {
            self.db
                .lock()
                .ok()
                .and_then(|d| d.latest_feature_state().ok().flatten())
        });
        let db = self.db.lock().map_err(|_| lock_error())?;
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

        let mut out = serde_json::json!({
            "setupName": setup_name,
            "marketSnapshot": snapshot,
            "domSummary": dom_feature.as_ref().and_then(|(_, payload)| payload.get("domSummary")).cloned(),
            "domFeature": dom_feature.as_ref().map(|(_, payload)| payload.clone()),
            "recentPullStackSummary": dom_feature.as_ref().and_then(|(_, payload)| payload.get("activity")).cloned(),
            "nearbyLevelReactionContext": nearby_levels,
            "riskState": risk,
            "guidance": "Your playbook defines this setup. Evaluate all conditions before entry."
        });
        if let Some(ref res) = r {
            merge_tool_live_metadata(&mut out, res);
        } else {
            out["dataAgeMs"] = serde_json::json!(compute_data_age(&db));
        }
        Ok(text_result(out))
    }

    #[tool(
        description = "Which key levels is price currently near (within specified tick distance). Returns levels sorted by distance ascending. Includes prior day H/L/C, VA/POC, overnight (Globex), Globex OR30, London OR60, IB, OR5 mid, and IB extensions. Response includes sessionType/sessionSegment/tradingDay."
    )]
    async fn get_proximity_report(
        &self,
        Parameters(params): Parameters<ProximityParams>,
    ) -> Result<CallToolResult, McpError> {
        if let Some(r) = self.resolve_live_market_view() {
            let s = &r.snapshot;
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
            let mut out = serde_json::json!({
                "sessionType": session_type,
                "sessionSegment": session_segment,
                "tradingDay": s.get("tradingDay"),
                "lastPrice": last_price,
                "maxDistanceTicks": max_ticks,
                "nearbyLevels": levels,
            });
            merge_tool_live_metadata(&mut out, &r);
            return Ok(text_result(out));
        }
        Ok(no_data("No market data available for proximity report"))
    }

    #[tool(
        description = "Validate data integrity: checks tick count, stream freshness, contract rollover status, and pipeline consistency invariants (POC within VA, VA contains ~70%% of TPOs, delta sum consistency). Returns pass/fail status with details."
    )]
    async fn validate_data_integrity(&self) -> Result<CallToolResult, McpError> {
        let db_snapshot = collect_validation_db_snapshot(&self.db)?;
        let pipeline_invariants = collect_pipeline_invariants(&self.pipelines)?;
        let tick_count = db_snapshot.tick_count;
        let last_ts = db_snapshot.last_ts;
        let now_ms = chrono::Utc::now().timestamp_millis() as f64;
        let age_ms = last_ts.map(|v| now_ms - v).unwrap_or(f64::INFINITY);
        let stream_fresh = age_ms.is_finite() && age_ms <= FRESHNESS_THRESHOLD_MS;
        let fr = &self.feed_runtime;
        let now_wall_ms = now_ms.max(0.0) as u64;
        let atomic_scid_ts = tick_ms_from_bits(fr.last_scid_tick_ms_bits.load(Ordering::Acquire));
        let atomic_age_ms = atomic_scid_ts
            .map(|t| (now_ms - t).max(0.0))
            .unwrap_or(f64::INFINITY);
        let stream_fresh_atomic =
            atomic_age_ms.is_finite() && atomic_age_ms <= FRESHNESS_THRESHOLD_MS;
        let monotonicity = fr.monotonicity_snapshot();
        let recent_monotonic_violation = monotonicity.has_recent_violation(now_ms);
        let (_, active_contract) = self.resolve_contract_cached();
        let server_contract = self.current_pipeline_contract_metadata();
        let runtime_event_stats = self.runtime_events.stats();
        let mut rollover_lock_failed = false;
        let rollover_status = if let Ok(db) = self.db.lock() {
            Some(self.rollover_status_for_date(
                &db,
                &active_contract,
                server_contract.as_ref(),
                &the_desk_backend::et_now_trading_day(),
                Some(age_ms),
            )?)
        } else {
            rollover_lock_failed = true;
            None
        };

        let mut checks = serde_json::json!({
            "rawTicksPresent": tick_count > 0,
            "streamFresh": stream_fresh,
            "streamFreshByPipelineAtomic": stream_fresh_atomic,
            "freshnessThresholdMs": FRESHNESS_THRESHOLD_MS,
        });
        let mut invariants_ok = true;
        if let Some(status) = &rollover_status {
            let passed = status.prior_reference_trust == PriorReferenceTrust::Authoritative
                && status.status == the_desk_backend::rollover::ContractRolloverStatusKind::Ok;
            checks["contractRollover"] = serde_json::json!({
                "passed": passed,
                "status": &status.status,
                "agentAction": &status.agent_action,
                "detail": status.notes.join(" ")
            });
            invariants_ok &= passed;
        } else if rollover_lock_failed {
            checks["contractRollover"] = serde_json::json!({
                "passed": false,
                "status": "unknown",
                "agentAction": "retry",
                "detail": "db_lock_unavailable"
            });
            invariants_ok = false;
        }
        checks["monotonicTimestamps"] = serde_json::json!({
            "passed": !recent_monotonic_violation,
            "detail": monotonicity_check_detail(monotonicity),
            "recentWindowMs": MONOTONIC_ANOMALY_RECENT_WINDOW_MS,
        });
        invariants_ok &= !recent_monotonic_violation;
        checks["runtimeEvents"] = serde_json::json!({
            "passed": true,
            "detail": "Use get_runtime_events for recent structured MCP diagnostics.",
            "recentRuntimeEventCount": runtime_event_stats.recent_event_count,
            "lastRuntimeWarningAtMs": runtime_event_stats.last_warning_at_ms,
            "lastRuntimeErrorAtMs": runtime_event_stats.last_error_at_ms,
            "lastRuntimeWarning": &runtime_event_stats.last_warning,
            "lastRuntimeError": &runtime_event_stats.last_error,
            "recentRuntimeEventNameCounts": &runtime_event_stats.recent_event_name_counts,
        });
        for (name, passed, detail) in pipeline_invariants {
            checks[name] = serde_json::json!({
                "passed": passed,
                "detail": detail
            });
            invariants_ok &= passed;
        }
        let status = if tick_count == 0 || (!stream_fresh && !stream_fresh_atomic) || !invariants_ok
        {
            "warning"
        } else {
            "ok"
        };

        let mut result = serde_json::json!({
            "status": status,
            "liveDataSource": "scid",
            "tickCount": tick_count,
            "lastTickAgeMs": if age_ms.is_finite() { age_ms } else { -1.0 },
            "pipelineAtomicTickAgeMs": if atomic_age_ms.is_finite() {
                atomic_age_ms
            } else {
                -1.0
            },
            "scidTailResetCount": fr.scid_tail_reset_count.load(Ordering::Acquire),
            "skippedNonMonotonicTicks": monotonicity.skipped_non_monotonic_ticks,
            "duplicateTimestampTicks": monotonicity.duplicate_timestamp_ticks,
            "backwardTimestampTicks": monotonicity.backward_timestamp_ticks,
            "lastNonMonotonicTimestampMs": monotonicity.last_non_monotonic_timestamp_ms,
            "pipelineLockRecentlyContended": fr.pipeline_lock_recently_contended(now_wall_ms),
            "lastDepthTimestampMs": tick_ms_from_bits(
                fr.last_depth_timestamp_ms_bits.load(Ordering::Acquire),
            ),
            "recentRuntimeEventCount": runtime_event_stats.recent_event_count,
            "lastRuntimeWarningAtMs": runtime_event_stats.last_warning_at_ms,
            "lastRuntimeErrorAtMs": runtime_event_stats.last_error_at_ms,
            "lastRuntimeWarning": &runtime_event_stats.last_warning,
            "lastRuntimeError": &runtime_event_stats.last_error,
            "recentRuntimeEventNameCounts": &runtime_event_stats.recent_event_name_counts,
            "checks": checks
        });
        if let Some(status) = rollover_status {
            result["rolloverStatus"] =
                serde_json::to_value(status).unwrap_or_else(|_| serde_json::json!({}));
        }

        if let Ok(db) = self.db.lock() {
            let _ = db.insert_validation_run(now_ms, status, &result);
        }

        Ok(text_result(result))
    }
}

#[tool_handler]
impl ServerHandler for TheDeskMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            instructions: Some(
                "The Desk - AI trading co-pilot backend for NQ futures. \
                 Live data: Sierra Chart `.scid` ticks plus optional `MarketDepthData` `.depth` files only. \
                 Provides real-time market structure (VWAP, TPO, Delta), \
                 microstructure analytics (tape pace, footprint, absorption), \
                 and playbook evaluation. \
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

#[derive(Debug, Clone, Copy)]
struct ValidationDbSnapshot {
    tick_count: i64,
    last_ts: Option<f64>,
}

fn collect_validation_db_snapshot(
    db: &Arc<Mutex<Database>>,
) -> Result<ValidationDbSnapshot, McpError> {
    let db = db.lock().map_err(|_| lock_error())?;
    Ok(ValidationDbSnapshot {
        tick_count: db.raw_tick_count().unwrap_or(0),
        last_ts: db.latest_tick_timestamp_ms().ok().flatten(),
    })
}

fn collect_pipeline_invariants(
    pipelines: &Arc<Mutex<PipelineEngine>>,
) -> Result<Vec<(String, bool, String)>, McpError> {
    let pipelines = pipelines
        .lock()
        .map_err(|_| McpError::new(ErrorCode::INTERNAL_ERROR, "pipeline lock poisoned", None))?;
    Ok(pipelines.validate_invariants())
}

fn monotonicity_check_detail(snapshot: MonotonicRuntimeSnapshot) -> String {
    match snapshot.last_non_monotonic_timestamp_ms {
        Some(last_ts) => format!(
            "skipped={} duplicate={} backward={} lastNonMonotonicTimestampMs={last_ts:.0}",
            snapshot.skipped_non_monotonic_ticks,
            snapshot.duplicate_timestamp_ticks,
            snapshot.backward_timestamp_ticks
        ),
        None => "no non-monotonic SCID ticks observed since startup".to_string(),
    }
}

/// `pipeline_invariants` must be collected under the pipeline mutex only; this function performs
/// DB reads and writes without holding the pipeline lock (avoids `db`→`pipelines` lock ordering).
fn persist_integrity_check(
    db: &Database,
    pipeline_invariants: &[(String, bool, String)],
    feed_rt: &McpFeedRuntimeState,
) {
    let tick_count = db.raw_tick_count().unwrap_or(0);
    let last_ts = db.latest_tick_timestamp_ms().ok().flatten();
    let now_ms = chrono::Utc::now().timestamp_millis() as f64;
    let age_ms = last_ts.map(|v| now_ms - v).unwrap_or(f64::INFINITY);
    let stream_fresh = age_ms.is_finite() && age_ms <= FRESHNESS_THRESHOLD_MS;
    let monotonicity = feed_rt.monotonicity_snapshot();
    let recent_monotonic_violation = monotonicity.has_recent_violation(now_ms);

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
    checks.insert(
        "monotonicTimestamps".to_string(),
        serde_json::json!({
            "passed": !recent_monotonic_violation,
            "detail": monotonicity_check_detail(monotonicity),
            "recentWindowMs": MONOTONIC_ANOMALY_RECENT_WINDOW_MS,
        }),
    );
    for (name, passed, detail) in pipeline_invariants {
        checks.insert(
            name.clone(),
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
        "skippedNonMonotonicTicks": monotonicity.skipped_non_monotonic_ticks,
        "duplicateTimestampTicks": monotonicity.duplicate_timestamp_ticks,
        "backwardTimestampTicks": monotonicity.backward_timestamp_ticks,
        "lastNonMonotonicTimestampMs": monotonicity.last_non_monotonic_timestamp_ms,
        "checks": checks
    });
    let _ = db.insert_validation_run(now_ms, status, &result);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SetupPersistencePolicy {
    Live,
    StartupReplay,
}

fn runtime_record_from_snapshot(
    snapshot: SetupRuntimeSnapshot,
    session_date: &str,
    root_symbol: Option<&str>,
    contract_symbol: Option<&str>,
    source: &str,
) -> SetupRuntimeStateRecord {
    let updated_at_ms = chrono::Utc::now().timestamp_millis() as f64;
    SetupRuntimeStateRecord {
        session_date: session_date.to_string(),
        root_symbol: root_symbol.map(str::to_string),
        contract_symbol: contract_symbol.map(str::to_string),
        setup_id: snapshot.setup_id,
        setup_name: snapshot.setup_name,
        state: snapshot.state,
        readiness: snapshot.readiness,
        readiness_score: snapshot.readiness_score,
        met_count: snapshot.met_count as i64,
        total_count: snapshot.total_count as i64,
        met_conditions: snapshot.met_conditions,
        missing_conditions: snapshot.missing_conditions,
        deterministic_all_met: snapshot.deterministic_all_met,
        requires_discretionary: snapshot.requires_discretionary,
        current_price: snapshot.current_price,
        last_evaluated_at_ms: snapshot.last_evaluated_at_ms,
        last_transition_at_ms: snapshot.last_transition_at_ms,
        last_alert_emitted_at_ms: snapshot.last_alert_emitted_at_ms,
        source: source.to_string(),
        updated_at_ms,
    }
}

fn persist_setup_evaluation(
    db: &Database,
    runtime_events: Option<&Arc<RuntimeEventStore>>,
    outcome: &SetupEvaluationOutcome,
    runtime_snapshot: Option<SetupRuntimeSnapshot>,
    market_snapshot: &the_desk_backend::pipelines::MarketState,
    session_date: &str,
    policy: SetupPersistencePolicy,
) {
    let source = match policy {
        SetupPersistencePolicy::Live => "live",
        SetupPersistencePolicy::StartupReplay => "startup_replay",
    };
    let root_symbol = Some(market_snapshot.root_symbol.as_str());
    let contract_symbol = Some(market_snapshot.contract_symbol.as_str());

    if let Some(transition) = &outcome.transition {
        let _ = db.insert_setup_state_transition(
            transition,
            session_date,
            root_symbol,
            contract_symbol,
            source,
        );
        if let Some(runtime_events) = runtime_events {
            let mut event = RuntimeEvent::new(
                RuntimeEventLevel::Info,
                "setup.transition",
                "setup",
                "Setup lifecycle transition persisted.",
                serde_json::json!({
                    "setupId": &transition.setup_id,
                    "setupName": &transition.setup_name,
                    "previousState": &transition.previous_state,
                    "nextState": &transition.next_state,
                    "previousReadiness": &transition.previous_readiness,
                    "nextReadiness": &transition.next_readiness,
                    "readinessScore": transition.readiness_score,
                    "reason": &transition.reason,
                    "alertEmitted": transition.alert_emitted,
                    "source": source,
                }),
            );
            event.session_date = Some(session_date.to_string());
            event.root_symbol = Some(market_snapshot.root_symbol.clone());
            event.contract_symbol = Some(market_snapshot.contract_symbol.clone());
            if let Some(recorded) = runtime_events.record(event) {
                persist_runtime_event_in_db(runtime_events, db, &recorded);
            }
        }
    }

    let should_persist_runtime = outcome.transition.is_some() || outcome.alert.is_some();
    if should_persist_runtime {
        if let Some(runtime_snapshot) = runtime_snapshot {
            let record = runtime_record_from_snapshot(
                runtime_snapshot,
                session_date,
                root_symbol,
                contract_symbol,
                source,
            );
            let _ = db.upsert_setup_runtime_state(&record);
        }
    }

    if policy != SetupPersistencePolicy::Live {
        return;
    }

    if let Some(alert) = &outcome.alert {
        let signal_id = format!("{}_{}", alert.setup_id, alert.timestamp as u64);
        let signal = ReplaySignalRecord {
            signal_id: signal_id.clone(),
            timestamp_ms: alert.timestamp,
            session_date: session_date.to_string(),
            root_symbol: Some(market_snapshot.root_symbol.clone()),
            contract_symbol: Some(market_snapshot.contract_symbol.clone()),
            setup_id: alert.setup_id.clone(),
            payload: serde_json::to_value(alert).unwrap_or_else(|_| serde_json::json!({})),
            source: "live".to_string(),
            job_id: None,
        };
        let _ = db.insert_playbook_signal_record(&signal);
        let outcome = SignalOutcome {
            signal_id,
            setup_id: alert.setup_id.clone(),
            setup_name: Some(alert.setup_name.clone()),
            session_date: session_date.to_string(),
            root_symbol: Some(market_snapshot.root_symbol.clone()),
            contract_symbol: Some(market_snapshot.contract_symbol.clone()),
            source: "live".to_string(),
            job_id: None,
            fired_at_ms: alert.timestamp,
            fired_price: alert.current_price,
            target_price: alert.target_prices.first().copied(),
            stop_price: alert.stop_price,
            outcome: "pending".to_string(),
            outcome_at_ms: None,
            max_favorable_excursion: None,
            max_adverse_excursion: None,
            r_result: None,
            time_to_outcome_ms: None,
            rvol_at_fire: Some(market_snapshot.rvol_ratio),
            rvol_bucket_at_fire: Some(market_snapshot.rvol_bucket_index as i32),
        };
        let _ = db.insert_signal_outcome(&outcome);
    }
}

#[allow(clippy::too_many_arguments)]
fn evaluate_setups_for_snapshot(
    rules: &Arc<Mutex<RulesEngine>>,
    playbook_cache: &Arc<PlaybookRuntimeCache>,
    db: &Arc<Mutex<Database>>,
    runtime_events: Option<&Arc<RuntimeEventStore>>,
    snapshot: &the_desk_backend::pipelines::MarketState,
    session_date: &str,
    evaluation_ts_ms: f64,
    policy: SetupPersistencePolicy,
) {
    let (setups, risk_at_limit) = playbook_cache.snapshot();
    let persist_items = if let Ok(mut r) = rules.lock() {
        let mut items = Vec::new();
        for setup in setups.iter() {
            let outcome = r.evaluate_detailed_at(setup, snapshot, risk_at_limit, evaluation_ts_ms);
            let runtime_snapshot = r.runtime_snapshot(&setup.id);
            items.push((outcome, runtime_snapshot));
        }
        r.update_prev_market(snapshot);
        items
    } else {
        return;
    };

    if let Ok(d) = db.lock() {
        for (outcome, runtime_snapshot) in persist_items {
            persist_setup_evaluation(
                &d,
                runtime_events,
                &outcome,
                runtime_snapshot,
                snapshot,
                session_date,
                policy,
            );
        }
    }
}

fn record_attention_runtime_event(
    runtime_events: &Arc<RuntimeEventStore>,
    db: &Database,
    event_name: &str,
    message: &str,
    signal: &AttentionSignalRecord,
    fields: serde_json::Value,
) {
    let mut event = RuntimeEvent::new(
        RuntimeEventLevel::Info,
        event_name,
        "attention",
        message,
        fields,
    );
    event.session_date = Some(signal.session_date.clone());
    event.root_symbol = signal.root_symbol.clone();
    event.contract_symbol = signal.contract_symbol.clone();
    if let Some(recorded) = runtime_events.record(event) {
        persist_runtime_event_in_db(runtime_events, db, &recorded);
    }
}

fn expire_and_audit_attention_signals(
    db: &Database,
    runtime_events: &Arc<RuntimeEventStore>,
    timestamp_ms: f64,
    source: Option<&str>,
) {
    let expired = db
        .expire_stale_attention_signals(timestamp_ms, source)
        .unwrap_or_default();
    for signal in expired {
        record_attention_runtime_event(
            runtime_events,
            db,
            "attention.signal_expired",
            "Attention signal expired.",
            &signal,
            serde_json::json!({
                "signalId": signal.signal_id,
                "kind": signal.kind,
                "priority": signal.priority,
                "expiresAtMs": signal.expires_at_ms,
            }),
        );
    }
}

fn dispatch_attention_runtime_notifications(
    db: &Database,
    runtime_events: &Arc<RuntimeEventStore>,
    timestamp_ms: f64,
) {
    let last_cursor = db
        .load_attention_notifier_cursor("runtime_event")
        .ok()
        .flatten();
    let since_ms = last_cursor.and_then(|(_, ts)| ts);
    let mut signals = db
        .query_attention_signals(&AttentionSignalQuery {
            status: Some("active".to_string()),
            min_priority: Some("high".to_string()),
            include_expired: false,
            cursor_signal_id: None,
            since_ms,
            source: Some("live".to_string()),
            limit: 100,
            ..AttentionSignalQuery::default()
        })
        .unwrap_or_default();
    signals.sort_by(|a, b| {
        a.created_at_ms
            .partial_cmp(&b.created_at_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.signal_id.cmp(&b.signal_id))
    });
    let config = AttentionNotifierConfig {
        enabled: true,
        ..AttentionNotifierConfig::default()
    };
    let mut last_dispatched: Option<String> = None;
    for signal in signals {
        if signal.updated_at_ms > timestamp_ms {
            continue;
        }
        let decision = config.evaluate(&signal);
        if !decision.should_dispatch {
            continue;
        }
        record_attention_runtime_event(
            runtime_events,
            db,
            "attention.notifier_dispatched",
            "Runtime-event attention notifier dispatched.",
            &signal,
            serde_json::json!({
                "signalId": signal.signal_id,
                "kind": signal.kind,
                "priority": signal.priority,
                "reason": decision.reason,
                "sink": "runtime_event",
            }),
        );
        let event_id = format!(
            "ase_{}",
            the_desk_backend::db::stable_hash_hex(&format!(
                "{}|notified|runtime_event|{timestamp_ms:.3}",
                signal.signal_id
            ))
        );
        let _ =
            db.insert_attention_signal_event(&the_desk_backend::db::AttentionSignalEventRecord {
                event_id,
                signal_id: signal.signal_id.clone(),
                event_type: "notified".to_string(),
                occurred_at_ms: timestamp_ms,
                session_date: signal.session_date.clone(),
                source: signal.source.clone(),
                actor: Some("runtime_event".to_string()),
                note: Some("runtime-event notifier sink".to_string()),
                payload: serde_json::json!({
                    "sink": "runtime_event",
                    "priority": signal.priority,
                    "kind": signal.kind,
                }),
            });
        last_dispatched = Some(signal.signal_id.clone());
    }
    if let Some(signal_id) = last_dispatched {
        let _ = db.save_attention_notifier_cursor("runtime_event", Some(&signal_id), timestamp_ms);
    }
}

#[allow(clippy::too_many_arguments)]
fn compose_and_persist_attention(
    db: &Database,
    runtime_events: &Arc<RuntimeEventStore>,
    snapshot: &the_desk_backend::pipelines::MarketState,
    new_events: &[MarketEvent],
    pulse_kind: AttentionPulseKind,
    timestamp_ms: f64,
    source: &str,
    job_id: Option<&str>,
) {
    if source == "live" {
        expire_and_audit_attention_signals(db, runtime_events, timestamp_ms, Some(source));
    }
    if new_events.is_empty() && pulse_kind == AttentionPulseKind::EventDriven {
        return;
    }
    let setup_states = db
        .load_setup_runtime_state_for_session(&snapshot.trading_day)
        .unwrap_or_default();
    let risk_state = db.load_risk_state().ok().flatten();
    let prior_active_signals = db
        .query_attention_signals(&AttentionSignalQuery {
            status: Some("active".to_string()),
            min_priority: None,
            include_expired: false,
            cursor_signal_id: None,
            since_ms: None,
            trading_day: Some(snapshot.trading_day.clone()),
            root_symbol: Some(snapshot.root_symbol.clone()).filter(|v| !v.is_empty()),
            contract_symbol: Some(snapshot.contract_symbol.clone()).filter(|v| !v.is_empty()),
            source: Some(source.to_string()),
            job_id: job_id.map(str::to_string),
            limit: 250,
            ..AttentionSignalQuery::default()
        })
        .unwrap_or_default();
    let composer = SignalComposer::default();
    let output = composer.compose(SignalComposerInput {
        pulse_kind,
        events: new_events,
        setup_states: &setup_states,
        risk_state: risk_state.as_ref(),
        market_snapshot: snapshot,
        prior_active_signals: &prior_active_signals,
        timestamp_ms,
        source,
        job_id,
    });
    for signal in &output.signals {
        let mut signal = signal.clone();
        if source != "live" {
            signal.status = "expired".to_string();
            signal.expires_at_ms = Some(timestamp_ms);
        }
        if db.upsert_attention_signal(&signal).is_ok() {
            record_attention_runtime_event(
                runtime_events,
                db,
                "attention.signal_emitted",
                "Attention signal emitted or updated.",
                &signal,
                serde_json::json!({
                    "signalId": signal.signal_id,
                    "kind": signal.kind,
                    "priority": signal.priority,
                    "dedupeKey": signal.dedupe_key,
                    "pulseKind": format!("{:?}", pulse_kind),
                }),
            );
        }
    }
    for event in &output.signal_events {
        let _ = db.insert_attention_signal_event(event);
        if event.event_type == "priority_changed" {
            if let Some(signal) = output
                .signals
                .iter()
                .find(|s| s.signal_id == event.signal_id)
            {
                record_attention_runtime_event(
                    runtime_events,
                    db,
                    "attention.signal_priority_changed",
                    "Attention signal priority changed.",
                    signal,
                    serde_json::json!({
                        "signalId": signal.signal_id,
                        "priority": signal.priority,
                    }),
                );
            }
        }
    }
    for idea in &output.idea_cards {
        let _ = db.upsert_trade_idea_card(idea);
    }
    if source == "live" {
        dispatch_attention_runtime_notifications(db, runtime_events, timestamp_ms);
    }
}

/// Process a single tick through the pipeline engine, event detector, rules engine, and outcome tracker.
#[allow(clippy::too_many_arguments)]
fn process_tick(
    pipelines: &Arc<Mutex<PipelineEngine>>,
    detector: &Arc<Mutex<EventDetector>>,
    flow_emitter: &Arc<Mutex<FlowEventEmitter>>,
    rules: &Arc<Mutex<RulesEngine>>,
    playbook_cache: &Arc<PlaybookRuntimeCache>,
    db: &Arc<Mutex<Database>>,
    runtime_events: &Arc<RuntimeEventStore>,
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
    let event_buffer_start = event_buffer.len();
    let snapshot = {
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

            snapshot
        } else {
            return;
        }
    };
    let new_attention_events: Vec<MarketEvent> = event_buffer[event_buffer_start..].to_vec();
    let setup_trading_day = trading_day_from_timestamp_ms(timestamp_ms);

    // Rules engine: evaluate setups and persist progress outside the pipeline lock.
    evaluate_setups_for_snapshot(
        rules,
        playbook_cache,
        db,
        Some(runtime_events),
        &snapshot,
        &setup_trading_day,
        timestamp_ms,
        SetupPersistencePolicy::Live,
    );

    // Outcome tracker: update MFE/MAE and resolve signals
    if let Ok(d) = db.lock() {
        let _ = outcome_tracker::on_tick(&d, price, timestamp_ms, None);
        if !new_attention_events.is_empty() {
            compose_and_persist_attention(
                &d,
                runtime_events,
                &snapshot,
                &new_attention_events,
                AttentionPulseKind::EventDriven,
                timestamp_ms,
                "live",
                None,
            );
        }
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

fn current_best_bid_ask(last_bid: &Arc<Mutex<f64>>, last_ask: &Arc<Mutex<f64>>) -> (f64, f64) {
    let bid = last_bid.lock().ok().map(|v| *v).unwrap_or_default();
    let ask = last_ask.lock().ok().map(|v| *v).unwrap_or_default();
    (bid, ask)
}

fn build_live_feature_state_snapshot_payload(
    pipelines: &Arc<Mutex<PipelineEngine>>,
    last_bid: &Arc<Mutex<f64>>,
    last_ask: &Arc<Mutex<f64>>,
    timestamp_ms: f64,
) -> Option<(f64, serde_json::Value)> {
    if !timestamp_ms.is_finite() || timestamp_ms <= 0.0 {
        return None;
    }
    let (bid, ask) = current_best_bid_ask(last_bid, last_ask);
    if bid <= 0.0 {
        return None;
    }
    let payload = pipelines.lock().ok().map(|p| {
        serde_json::to_value(p.snapshot(bid.max(0.0), ask.max(0.0))).unwrap_or_default()
    })?;
    Some((timestamp_ms, payload))
}

fn persist_feature_state_payload(
    db: &Arc<Mutex<Database>>,
    timestamp_ms: f64,
    payload: &serde_json::Value,
) {
    if let Ok(d) = db.lock() {
        let _ = d.upsert_feature_state(timestamp_ms, payload);
    }
}

/// Persist `feature_state` after `dom_summary` has been updated.
/// Uses either the live pipeline snapshot path (`pipelines` then `db`) or a single DB critical
/// section to merge DOM data into the previous snapshot, but never holds both mutexes at once.
fn persist_feature_state_after_dom_summary(
    db: &Arc<Mutex<Database>>,
    pipelines: &Arc<Mutex<PipelineEngine>>,
    last_bid: &Arc<Mutex<f64>>,
    last_ask: &Arc<Mutex<f64>>,
    timestamp_ms: f64,
    dom_summary: &DomSummary,
) {
    let (bid, ask) = current_best_bid_ask(last_bid, last_ask);
    if bid > 0.0 || ask > 0.0 {
        if let Some((ts, payload)) =
            build_live_feature_state_snapshot_payload(pipelines, last_bid, last_ask, timestamp_ms)
        {
            persist_feature_state_payload(db, ts, &payload);
        }
        return;
    }

    if let Ok(d) = db.lock() {
        let payload =
            merge_dom_summary_into_snapshot(d.latest_feature_state().ok().flatten(), dom_summary);
        let _ = d.upsert_feature_state(timestamp_ms, &payload);
    }
}

#[derive(Debug, Default)]
struct DepthPollWorkerState {
    active_path: Option<std::path::PathBuf>,
    offset: u64,
    batch_id: i64,
    book: DepthBook,
}

#[derive(Debug)]
struct DepthPersistWork {
    source_file: String,
    trading_day: String,
    last_record_timestamp_ms: f64,
    records: Vec<the_desk_backend::depth::DepthRecord>,
    snapshot: the_desk_backend::depth::DomSnapshot,
    feature: DomFeatureSnapshot,
    batch_id: i64,
}

fn default_depth_feature_snapshot(
    snapshot: &the_desk_backend::depth::DomSnapshot,
    source_file: &str,
    records: &[the_desk_backend::depth::DepthRecord],
    feature_window_start: f64,
    batch_end_ms: f64,
) -> DomFeatureSnapshot {
    let fallback_activity = PullStackActivitySummary {
        source_file: source_file.to_string(),
        start_time_ms: feature_window_start,
        end_time_ms: batch_end_ms,
        session_date: snapshot.session_date.clone(),
        record_count: records.len(),
        batch_count: records.iter().filter(|r| r.end_of_batch).count(),
        bid: Default::default(),
        ask: Default::default(),
        top_pull_levels: Vec::new(),
        top_stack_levels: Vec::new(),
    };
    DomFeatureSnapshot {
        source_file: source_file.to_string(),
        timestamp_ms: snapshot.snapshot_timestamp_ms,
        session_date: snapshot.session_date.clone(),
        dom_summary: build_dom_summary(snapshot, &fallback_activity),
        activity: fallback_activity,
    }
}

fn build_depth_feature_snapshot(
    reader: &DepthReader,
    snapshot: &the_desk_backend::depth::DomSnapshot,
    source_file: &str,
    records: &[the_desk_backend::depth::DepthRecord],
    batch_end_ms: f64,
) -> DomFeatureSnapshot {
    let feature_window_start = (batch_end_ms - 60_000.0).max(0.0);
    let config = load_feed_config();
    aggregate_window_trades(&config, feature_window_start, batch_end_ms)
        .ok()
        .and_then(|trades| {
            reader
                .summarize_window(feature_window_start, batch_end_ms, &trades, None, None)
                .ok()
        })
        .map(|activity| build_dom_feature_snapshot(snapshot, activity))
        .unwrap_or_else(|| {
            default_depth_feature_snapshot(
                snapshot,
                source_file,
                records,
                feature_window_start,
                batch_end_ms,
            )
        })
}

fn recover_depth_state_after_shrink(
    reader: &DepthReader,
    state: &mut DepthPollWorkerState,
) -> Result<Option<DepthPersistWork>, String> {
    let mut recovery_offset = reader.data_start_offset();
    let mut recovery_records = Vec::<the_desk_backend::depth::DepthRecord>::new();
    reader
        .scan_new_records(&mut recovery_offset, |record| {
            recovery_records.push(record);
            Ok(DepthScanControl::Continue)
        })
        .map_err(|e| e.to_string())?;

    state.offset = recovery_offset;
    if recovery_records.is_empty() {
        state.book = DepthBook::default();
        return Ok(None);
    }

    let contains_clear = recovery_records
        .iter()
        .any(|record| record.command == DepthCommand::ClearBook);
    let mut rebuilt_book = if contains_clear {
        DepthBook::default()
    } else {
        state.book.clone()
    };
    for record in &recovery_records {
        rebuilt_book.apply(record);
    }
    state.book = rebuilt_book.clone();

    let last_record = recovery_records
        .last()
        .expect("recovery_records not empty after guard");
    let source_file = reader.path().to_string_lossy().to_string();
    let trading_day = session_date_from_timestamp_ms(last_record.timestamp_ms);
    let snapshot = rebuilt_book.snapshot(&source_file, last_record.timestamp_ms, 10);
    let feature = build_depth_feature_snapshot(
        reader,
        &snapshot,
        &source_file,
        &recovery_records,
        last_record.timestamp_ms,
    );

    Ok(Some(DepthPersistWork {
        source_file,
        trading_day,
        last_record_timestamp_ms: last_record.timestamp_ms,
        records: Vec::new(),
        snapshot,
        feature,
        batch_id: state.batch_id,
    }))
}

fn compute_depth_poll_step(
    state: &mut DepthPollWorkerState,
) -> Result<Option<DepthPersistWork>, String> {
    let Some(reader) = latest_depth_reader().map_err(|e| e.to_string())? else {
        return Ok(None);
    };

    if state.active_path.as_deref() != Some(reader.path()) {
        state.active_path = Some(reader.path().to_path_buf());
        state.offset = reader.data_start_offset();
        state.batch_id = 0;
        state.book = DepthBook::default();
    } else {
        let file_len = reader.file_len().map_err(|e| e.to_string())?;
        if file_len < state.offset {
            return recover_depth_state_after_shrink(&reader, state);
        }
    }

    let mut new_records = Vec::<the_desk_backend::depth::DepthRecord>::new();
    reader
        .scan_new_records(&mut state.offset, |record| {
            state.book.apply(&record);
            new_records.push(record);
            Ok(DepthScanControl::Continue)
        })
        .map_err(|e| e.to_string())?;

    if new_records.is_empty() {
        return Ok(None);
    }

    let Some(last_record) = new_records.last() else {
        return Ok(None);
    };
    let source_file = reader.path().to_string_lossy().to_string();
    let trading_day = session_date_from_timestamp_ms(last_record.timestamp_ms);
    let snapshot = state
        .book
        .snapshot(&source_file, last_record.timestamp_ms, 10);
    let feature = build_depth_feature_snapshot(
        &reader,
        &snapshot,
        &source_file,
        &new_records,
        last_record.timestamp_ms,
    );

    Ok(Some(DepthPersistWork {
        source_file,
        trading_day,
        last_record_timestamp_ms: last_record.timestamp_ms,
        records: new_records,
        snapshot,
        feature,
        batch_id: state.batch_id,
    }))
}

#[allow(clippy::too_many_arguments)]
fn apply_depth_persist_work(
    db: &Arc<Mutex<Database>>,
    pipelines: &Arc<Mutex<PipelineEngine>>,
    last_bid: &Arc<Mutex<f64>>,
    last_ask: &Arc<Mutex<f64>>,
    mut work: DepthPersistWork,
    feed_rt: &McpFeedRuntimeState,
) -> i64 {
    let mut next_batch_id = work.batch_id;
    if let Ok(mut d) = db.lock() {
        if let Ok(next_batch) =
            d.insert_depth_events_batch(&work.source_file, &work.records, work.batch_id)
        {
            next_batch_id = next_batch;
        }
        let snapshot_json = serde_json::to_value(&work.snapshot).unwrap_or_default();
        let _ = d.insert_dom_snapshot(
            &work.source_file,
            work.last_record_timestamp_ms,
            &work.trading_day,
            &snapshot_json,
        );
    }

    let (recent_summary_rows, session_rows) = if let Ok(d) = db.lock() {
        (
            d.query_dom_feature_snapshots(
                Some((work.last_record_timestamp_ms - DOM_NARRATIVE_HORIZON_MS).max(0.0)),
                Some((work.last_record_timestamp_ms - 0.001).max(0.0)),
                512,
            )
            .unwrap_or_default(),
            d.query_dom_feature_snapshots_for_trading_day(&work.trading_day, 50_000)
                .unwrap_or_default(),
        )
    } else {
        (Vec::new(), Vec::new())
    };

    let recent_summaries = dom_summaries_from_rows(&recent_summary_rows);
    let session_summaries = dom_summaries_from_rows(&session_rows);
    enrich_dom_summary(
        &mut work.feature.dom_summary,
        Some(&work.feature.activity),
        &recent_summaries,
        Some(&session_summaries),
    );
    let feature_json = serde_json::to_value(&work.feature).unwrap_or_default();

    if let Ok(d) = db.lock() {
        let _ = d.insert_dom_feature_snapshot(
            &work.source_file,
            work.feature.timestamp_ms,
            &work.trading_day,
            &feature_json,
        );
    }

    if let Ok(mut pl) = pipelines.lock() {
        pl.set_dom_summary(Some(work.feature.dom_summary.clone()));
    }

    persist_feature_state_after_dom_summary(
        db,
        pipelines,
        last_bid,
        last_ask,
        work.feature.timestamp_ms,
        &work.feature.dom_summary,
    );

    feed_rt.last_depth_timestamp_ms_bits.store(
        tick_ms_to_bits(work.feature.timestamp_ms),
        Ordering::Release,
    );
    next_batch_id
}

#[derive(Debug, Clone, Copy)]
struct StartupWarmReplayResult {
    cutover_offset: u64,
    applied_tick_count: usize,
}

fn safe_scid_data_offset(reader: &ScidReader) -> u64 {
    ScidReader::header_size_bytes_for_path(reader.path()).unwrap_or(56)
}

#[derive(Debug)]
struct ScidPollReadStep {
    requested_offset: u64,
    start_offset: u64,
    next_offset: u64,
    file_len: u64,
    ticks: Vec<ScidTick>,
}

impl ScidPollReadStep {
    fn was_realigned(&self) -> bool {
        self.start_offset != self.requested_offset
    }

    fn was_shrink_reset(&self) -> bool {
        self.file_len < self.requested_offset
    }
}

fn read_scid_poll_step(
    reader: &ScidReader,
    requested_offset: u64,
) -> Result<ScidPollReadStep, String> {
    let header_size =
        ScidReader::header_size_bytes_for_path(reader.path()).map_err(|e| e.to_string())?;
    let file_len = std::fs::metadata(reader.path())
        .map_err(|e| e.to_string())?
        .len();
    let aligned_end = scid_tail_offset_after_shrink(file_len, header_size);

    let mut start_offset = requested_offset;
    if file_len < start_offset {
        start_offset = aligned_end;
    } else if start_offset >= header_size {
        let rel = start_offset - header_size;
        if !rel.is_multiple_of(SCID_RECORD_SIZE as u64) {
            start_offset =
                scid_tail_offset_after_shrink(start_offset, header_size).min(aligned_end);
        }
    } else {
        // Below header: resume from first record (header_size is valid even if file is shorter).
        start_offset = header_size;
    }

    let batch = reader
        .read_bulk_from_offset(start_offset)
        .map_err(|e| e.to_string())?;

    Ok(ScidPollReadStep {
        requested_offset,
        start_offset,
        next_offset: batch.next_offset,
        file_len,
        ticks: batch.ticks,
    })
}

/// Compute the RTH window in epoch milliseconds for a given session_date
/// (`YYYY-MM-DD` interpreted as Eastern). Returns `(start_ms, end_ms)` where
/// `start = 09:30 ET` and `end = 16:00 ET` on that date. Returns `None` on
/// parse failure or DST ambiguity.
fn rth_window_ms_for_date(session_date: &str) -> Option<(f64, f64)> {
    use chrono::NaiveDate;
    use chrono_tz::US::Eastern;
    let date = NaiveDate::parse_from_str(session_date, "%Y-%m-%d").ok()?;
    let open_naive = date.and_hms_opt(9, 30, 0)?;
    let close_naive = date.and_hms_opt(16, 0, 0)?;
    let open_ms = Eastern
        .from_local_datetime(&open_naive)
        .single()?
        .timestamp_millis() as f64;
    let close_ms = Eastern
        .from_local_datetime(&close_naive)
        .single()?
        .timestamp_millis() as f64;
    Some((open_ms, close_ms))
}

fn contract_scope(
    contract_metadata: &the_desk_backend::feed::ContractMetadata,
) -> (Option<&str>, Option<&str>) {
    let root_symbol = contract_metadata.root_symbol.trim();
    let contract_symbol = contract_metadata.contract_symbol.trim();
    (
        (!root_symbol.is_empty()).then_some(root_symbol),
        (!contract_symbol.is_empty()).then_some(contract_symbol),
    )
}

fn build_rollover_status_from_db(
    db: &Database,
    active_contract: &the_desk_backend::feed::ContractMetadata,
    server_contract: Option<&the_desk_backend::feed::ContractMetadata>,
    before_date: &str,
    data_age_ms: Option<f64>,
) -> Result<ContractRolloverStatus, the_desk_backend::db::DbError> {
    let root_symbol = active_contract.root_symbol.trim();
    let contract_symbol = active_contract.contract_symbol.trim();
    let current_contract_reference = if !root_symbol.is_empty() && !contract_symbol.is_empty() {
        db.load_prior_day_reference_for_contract(before_date, root_symbol, contract_symbol)?
    } else {
        None
    };
    let same_root_reference = if !root_symbol.is_empty() {
        db.load_prior_day_reference_for_root(before_date, root_symbol)?
    } else {
        None
    };

    Ok(build_contract_rollover_status(
        active_contract,
        server_contract,
        current_contract_reference,
        same_root_reference,
        data_age_ms,
        FRESHNESS_THRESHOLD_MS,
    ))
}

fn authoritative_prior_reference_from_db(
    db: &Database,
    active_contract: &the_desk_backend::feed::ContractMetadata,
    server_contract: Option<&the_desk_backend::feed::ContractMetadata>,
    before_date: &str,
) -> Result<
    (
        Option<the_desk_backend::db::PriorDayReference>,
        ContractRolloverStatus,
    ),
    the_desk_backend::db::DbError,
> {
    let root_symbol = active_contract.root_symbol.trim();
    let contract_symbol = active_contract.contract_symbol.trim();
    let current_contract_reference = if !root_symbol.is_empty() && !contract_symbol.is_empty() {
        db.load_prior_day_reference_for_contract(before_date, root_symbol, contract_symbol)?
    } else {
        None
    };
    let same_root_reference = if !root_symbol.is_empty() {
        db.load_prior_day_reference_for_root(before_date, root_symbol)?
    } else {
        None
    };
    let status = build_contract_rollover_status(
        active_contract,
        server_contract,
        current_contract_reference.clone(),
        same_root_reference,
        None,
        FRESHNESS_THRESHOLD_MS,
    );
    let authoritative = if status.prior_reference_trust == PriorReferenceTrust::Authoritative {
        current_contract_reference
    } else {
        None
    };
    Ok((authoritative, status))
}

fn contract_session_scope(
    contract_metadata: &the_desk_backend::feed::ContractMetadata,
) -> Option<SessionScopeFilter> {
    let (root_symbol, contract_symbol) = contract_scope(contract_metadata);
    if root_symbol.is_none() && contract_symbol.is_none() {
        return None;
    }
    Some(SessionScopeFilter {
        root_symbol: root_symbol.map(ToString::to_string),
        contract_symbol: contract_symbol.map(ToString::to_string),
        include_rollover_sessions: false,
        ..Default::default()
    })
}

/// Outcome of a successful RTH close finalization, used for logging/telemetry
/// and consumed by the boundary-recovery tests to assert the just-closed
/// metrics match what was persisted.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct RthCloseResult {
    session_date: String,
    high: f64,
    low: f64,
    close: f64,
    session_delta: f64,
}

#[derive(Debug)]
#[allow(dead_code)]
enum RthCloseFinalizeError {
    PipelineLockUnavailable(&'static str),
    DbLockUnavailable,
    Persist(the_desk_backend::db::DbError),
}

#[allow(clippy::too_many_arguments)]
/// Atomically finalize an RTH session at the first non-RTH tick after `RTH_CLOSE_ET`.
///
/// Builds a `SessionSummary` from the current pipeline state using the same
/// `summary_from_state` helper the backfill path uses, persists both the
/// `session_summaries` row and the `prior_day_levels` carry-forward row in a
/// single SQLite transaction, and then refreshes the in-memory carry-forward
/// state (`LevelsPipeline` prior_*, `SessionInventoryPipeline` prior_sessions)
/// directly from the just-built data so the next session reads consistent
/// levels without having to re-query SQLite immediately.
///
/// Returns `None` if no RTH session was active (cold-start or post-close
/// re-entry where the pipeline has already been reset). Otherwise returns a
/// summary of the just-closed session for logging.
fn finalize_rth_close(
    pipelines: &Arc<Mutex<PipelineEngine>>,
    db: &Arc<Mutex<Database>>,
    runtime_events: Option<&Arc<RuntimeEventStore>>,
    detector: Option<&Arc<Mutex<EventDetector>>>,
    flow_emitter: Option<&Arc<Mutex<FlowEventEmitter>>>,
    boundary_tick_ts: f64,
    last_bid_hint: f64,
    last_ask_hint: f64,
    contract_metadata: &the_desk_backend::feed::ContractMetadata,
) -> Result<Option<RthCloseResult>, RthCloseFinalizeError> {
    use the_desk_backend::pipelines::{MarketState, PriorSessionData, SessionEndState};

    struct CloseData {
        snapshot: MarketState,
        open_price: f64,
        tick_count: i64,
        total_volume: f64,
        end_state: SessionEndState,
        close_ts: f64,
        session_delta: f64,
    }

    let close_data = {
        let p = pipelines
            .lock()
            .map_err(|_| RthCloseFinalizeError::PipelineLockUnavailable("snapshot"))?;
        if !p.levels.rth_started() {
            return Ok(None);
        }
        let close_ts = p
            .tape_pace
            .last_trade_timestamp_ms()
            .unwrap_or(boundary_tick_ts);
        let bid = if last_bid_hint > 0.0 {
            last_bid_hint
        } else {
            (p.levels.last_price - 0.25).max(0.0)
        };
        let ask = if last_ask_hint > 0.0 {
            last_ask_hint
        } else {
            p.levels.last_price + 0.25
        };
        let snapshot = p.snapshot_at(bid, ask, close_ts);
        let open_price = p.levels.session_open_price;
        let tick_count = p.vwap.trade_count() as i64;
        let total_volume = p.rvol.session_volume();
        let end_state = p.session_end_state();
        let session_delta = snapshot.session_delta;
        CloseData {
            snapshot,
            open_price,
            tick_count,
            total_volume,
            end_state,
            close_ts,
            session_delta,
        }
    };

    let session_date = session_date_from_timestamp_ms(close_data.close_ts);
    let signal_count = if let Some((rth_start, rth_end)) = rth_window_ms_for_date(&session_date) {
        if let Ok(d) = db.lock() {
            d.count_playbook_signals_in_range(rth_start, rth_end)
                .unwrap_or(0)
        } else {
            0
        }
    } else {
        0
    };

    let mut summary = backfill::summary_from_state(
        &close_data.snapshot,
        &session_date,
        "RTH",
        close_data.open_price,
        close_data.tick_count,
        close_data.total_volume,
        signal_count,
    );

    // Stamp contract metadata so the persisted row matches the active contract
    // even if the snapshot was built before set_contract_metadata propagated.
    if summary.root_symbol.is_empty() {
        summary.root_symbol = contract_metadata.root_symbol.clone();
    }
    if summary.contract_symbol.is_empty() {
        summary.contract_symbol = contract_metadata.contract_symbol.clone();
    }
    if summary.contract_month.is_none() {
        summary.contract_month = contract_metadata.contract_month.clone();
    }
    if summary.symbol_resolution_mode.is_empty() {
        summary.symbol_resolution_mode = contract_metadata.symbol_resolution_mode.clone();
    }

    let prior_day_tuple = (
        close_data.end_state.high,
        close_data.end_state.low,
        close_data.end_state.close,
        close_data.end_state.va_high,
        close_data.end_state.va_low,
        close_data.end_state.poc,
        close_data.end_state.dnva_high,
        close_data.end_state.dnva_low,
        close_data.end_state.dnp,
    );

    db.lock()
        .map_err(|_| RthCloseFinalizeError::DbLockUnavailable)?
        .persist_live_session_close(&summary, prior_day_tuple)
        .map_err(RthCloseFinalizeError::Persist)?;

    let mut p = pipelines
        .lock()
        .map_err(|_| RthCloseFinalizeError::PipelineLockUnavailable("carry_forward_refresh"))?;
    // Refresh in-memory carry-forward directly from the just-built data so
    // the next session reads consistent state without re-querying SQLite.
    let just_closed = PriorSessionData {
        final_delta: close_data.session_delta,
        dnva_high: close_data.end_state.dnva_high,
        dnva_low: close_data.end_state.dnva_low,
        dnp: close_data.end_state.dnp,
    };
    // Anticipate Globex (the next visible session). Levels::reset_session()
    // copies session_high/low/close into prior_day_*; we then apply
    // VA/POC/DNVA from the just-built end_state directly because
    // reset_session() does not touch those.
    p.reset_session_with_type(true);
    p.levels.set_prior_profile(
        close_data.end_state.va_high,
        close_data.end_state.va_low,
        close_data.end_state.poc,
    );
    p.levels.set_prior_dnva(
        close_data.end_state.dnva_high,
        close_data.end_state.dnva_low,
        close_data.end_state.dnp,
    );
    p.levels.set_prior_day_contract_context(
        Some(contract_metadata.root_symbol.as_str()),
        Some(contract_metadata.contract_symbol.as_str()),
        Some(contract_metadata.contract_symbol.as_str()),
    );
    if just_closed.dnva_high > 0.0 && just_closed.dnva_low > 0.0 && just_closed.dnp > 0.0 {
        p.session_inventory.push_just_closed_session(just_closed, 5);
    }
    drop(p);

    if let Some(det) = detector {
        if let Ok(mut d) = det.lock() {
            d.reset();
        }
    }
    if let Some(fe) = flow_emitter {
        if let Ok(mut emitter) = fe.lock() {
            emitter.reset();
        }
    }

    if let Some(runtime_events) = runtime_events {
        record_runtime_event_scoped(
            runtime_events,
            Some(db),
            RuntimeEventLevel::Info,
            "session.rth_close_finalized",
            "session",
            "RTH close finalized atomically.",
            serde_json::json!({
                "high": close_data.end_state.high,
                "low": close_data.end_state.low,
                "close": close_data.end_state.close,
                "sessionDelta": close_data.session_delta,
                "signalCount": signal_count,
            }),
            Some(session_date.clone()),
            Some(contract_metadata.root_symbol.clone()),
            Some(contract_metadata.contract_symbol.clone()),
        );
    } else {
        tracing::info!(
            event_name = "session.rth_close_finalized",
            category = "session",
            session_date,
            high = close_data.end_state.high,
            low = close_data.end_state.low,
            close = close_data.end_state.close,
            session_delta = close_data.session_delta,
            signal_count,
            "RTH close finalized atomically."
        );
    }

    Ok(Some(RthCloseResult {
        session_date,
        high: close_data.end_state.high,
        low: close_data.end_state.low,
        close: close_data.end_state.close,
        session_delta: close_data.session_delta,
    }))
}

/// Reset pipelines for a new session and load the most recent prior-day /
/// prior-session inventory references from SQLite. Used at session boundaries
/// other than the RTH close (which goes through `finalize_rth_close`).
///
/// Idempotent: safe to invoke even when in-memory state was already prepared
/// by a prior `finalize_rth_close` (the DB read returns the same atomically
/// persisted values).
fn prepare_for_new_session(
    pipelines: &Arc<Mutex<PipelineEngine>>,
    db: &Arc<Mutex<Database>>,
    runtime_events: Option<&Arc<RuntimeEventStore>>,
    new_session: SessionType,
    new_segment: DeltaSegment,
    boundary_tick_ts: f64,
    contract_metadata: &the_desk_backend::feed::ContractMetadata,
) {
    let lookup_date = session_date_from_timestamp_ms(boundary_tick_ts);
    let (root_symbol, contract_symbol) = contract_scope(contract_metadata);
    let (prior, rollover_status) = if let Ok(d) = db.lock() {
        authoritative_prior_reference_from_db(
            &d,
            contract_metadata,
            Some(contract_metadata),
            &lookup_date,
        )
        .map(|(prior, status)| (prior, Some(status)))
        .unwrap_or((None, None))
    } else {
        (None, None)
    };

    if let Ok(mut p) = pipelines.lock() {
        p.reset_session_with_type(new_session == SessionType::Globex);
        if matches!(new_session, SessionType::Rth | SessionType::Globex) {
            if let Some(prior_ref) = prior {
                p.levels
                    .set_prior_day(prior_ref.high, prior_ref.low, prior_ref.close);
                p.levels.set_prior_day_contract_context(
                    prior_ref.root_symbol.as_deref(),
                    prior_ref.contract_symbol.as_deref(),
                    contract_symbol,
                );
                if let (Some(vh), Some(vl), Some(pc)) =
                    (prior_ref.va_high, prior_ref.va_low, prior_ref.poc)
                {
                    p.levels.set_prior_profile(vh, vl, pc);
                }
                if let (Some(dh), Some(dl), Some(dp)) =
                    (prior_ref.dnva_high, prior_ref.dnva_low, prior_ref.dnp)
                {
                    p.levels.set_prior_dnva(dh, dl, dp);
                }
            } else {
                p.levels.clear_prior_references();
                p.levels
                    .set_prior_day_contract_context(root_symbol, None, contract_symbol);
                if let Some(status) = rollover_status {
                    if let Some(runtime_events) = runtime_events {
                        let event = RuntimeEvent::new(
                            RuntimeEventLevel::Warn,
                            "rollover.prior_levels_cleared",
                            "rollover",
                            "Prior levels were cleared at a session boundary.",
                            serde_json::json!({
                                "status": status.status,
                                "agentAction": status.agent_action,
                                "priorReferenceTrust": status.prior_reference_trust,
                                "activeContract": status.active_contract_symbol,
                                "lookupDate": lookup_date,
                            }),
                        );
                        let _ = runtime_events.record(event);
                    } else {
                        tracing::warn!(
                            event_name = "rollover.prior_levels_cleared",
                            category = "rollover",
                            active_contract = status.active_contract_symbol,
                            lookup_date,
                            "Prior levels were cleared at a session boundary."
                        );
                    }
                }
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

    let scope = contract_session_scope(contract_metadata);
    let mut prior_inv: Vec<PriorSessionData> = if let Ok(d) = db.lock() {
        d.list_session_summaries_scoped(None, None, None, Some(inv_session_type), 5, scope.as_ref())
            .unwrap_or_default()
            .into_iter()
            .filter(|s| s.dnva_high > 0.0 && s.dnva_low > 0.0 && s.dnp > 0.0)
            .map(|s| PriorSessionData {
                final_delta: s.session_delta,
                dnva_high: s.dnva_high,
                dnva_low: s.dnva_low,
                dnp: s.dnp,
            })
            .collect()
    } else {
        Vec::new()
    };
    prior_inv.reverse();

    if let Ok(mut p) = pipelines.lock() {
        p.session_inventory.load_prior_sessions(prior_inv);
    }
}

/// Warm-replay SCID ticks into the live pipeline up to a pre-captured cutover offset.
///
/// The returned `cutover_offset` is the last fully consumed SCID offset, not the requested target,
/// so the live tail can safely resume after truncated/partial startup reads without skipping ticks.
#[allow(clippy::too_many_arguments)]
fn run_startup_warm_replay(
    reader: &ScidReader,
    pipelines: &Arc<Mutex<PipelineEngine>>,
    flow_emitter: &Arc<Mutex<FlowEventEmitter>>,
    rules: &Arc<Mutex<RulesEngine>>,
    playbook_cache: &Arc<PlaybookRuntimeCache>,
    db: &Arc<Mutex<Database>>,
    runtime_events: &Arc<RuntimeEventStore>,
    feed_rt: &McpFeedRuntimeState,
    since_ms: f64,
    requested_cutover_offset: u64,
    contract_metadata: &the_desk_backend::feed::ContractMetadata,
) -> StartupWarmReplayResult {
    let replay_batch =
        match reader.read_bulk_since_until_offset(Some(since_ms), requested_cutover_offset) {
            Ok(batch) => batch,
            Err(e) => {
                let fallback_offset = safe_scid_data_offset(reader);
                record_runtime_event(
                    runtime_events,
                    Some(db),
                    RuntimeEventLevel::Error,
                    "scid.warm_replay.failed",
                    "scid",
                    "Startup warm replay failed; live tail will resume from a safe offset.",
                    serde_json::json!({
                        "error": e.to_string(),
                        "fallbackOffset": fallback_offset,
                        "requestedCutoverOffset": requested_cutover_offset,
                    }),
                );
                return StartupWarmReplayResult {
                    cutover_offset: fallback_offset,
                    applied_tick_count: 0,
                };
            }
        };

    let actual_cutover_offset = replay_batch.next_offset;
    if actual_cutover_offset < requested_cutover_offset {
        record_runtime_event(
            runtime_events,
            Some(db),
            RuntimeEventLevel::Warn,
            "scid.warm_replay.truncated",
            "scid",
            "Startup warm replay stopped before the requested cutover offset.",
            serde_json::json!({
                "actualCutoverOffset": actual_cutover_offset,
                "requestedCutoverOffset": requested_cutover_offset,
            }),
        );
    }

    let ticks = replay_batch.ticks;
    if ticks.is_empty() {
        record_runtime_event(
            runtime_events,
            Some(db),
            RuntimeEventLevel::Info,
            "scid.warm_replay.empty",
            "scid",
            "Startup warm replay found no ticks since the prior Globex open.",
            serde_json::json!({
                "actualCutoverOffset": actual_cutover_offset,
                "sinceMs": since_ms,
            }),
        );
        return StartupWarmReplayResult {
            cutover_offset: actual_cutover_offset,
            applied_tick_count: 0,
        };
    }

    // Hold pipeline lock only during tick processing. Release pipelines before
    // acquiring DB at boundaries to avoid deadlock and let DB-only tools
    // (e.g. get_feed_health) run while backfill proceeds.
    let mut pipelines_guard = match pipelines.lock() {
        Ok(p) => p,
        Err(_) => {
            return StartupWarmReplayResult {
                cutover_offset: actual_cutover_offset,
                applied_tick_count: 0,
            };
        }
    };

    let mut current_session = SessionType::Unknown;
    let mut current_delta_segment = DeltaSegment::Unknown;
    let mut boundary_count = 0u32;
    let mut monotonic_guard = MonotonicTickGuard::default();
    let mut applied_tick_count = 0usize;
    let mut last_applied_tick: Option<ScidTick> = None;

    for tick in &ticks {
        match monotonic_guard.observe(tick.timestamp_ms) {
            MonotonicTimestampDecision::Accept => {}
            MonotonicTimestampDecision::Skip(kind) => {
                feed_rt.record_non_monotonic_tick(kind, tick.timestamp_ms);
                continue;
            }
        }
        if let Some(et_min) = et_minutes_from_timestamp(tick.timestamp_ms) {
            let new_session = classify_session(et_min);
            let new_segment = classify_delta_segment(et_min);
            let session_changed = new_session != current_session;
            let exiting_rth = current_session == SessionType::Rth && session_changed;

            if exiting_rth {
                // RTH close: atomically persist the session_summaries +
                // prior_day_levels rows together so the next session can
                // never observe half-updated carry-forward state if the
                // process crashes between 16:00 and 18:00 ET.
                drop(pipelines_guard);
                let finalize = finalize_rth_close(
                    pipelines,
                    db,
                    Some(runtime_events),
                    None,
                    Some(flow_emitter),
                    tick.timestamp_ms,
                    tick.bid,
                    tick.ask,
                    contract_metadata,
                );
                pipelines_guard = match pipelines.lock() {
                    Ok(p) => p,
                    Err(_) => {
                        return StartupWarmReplayResult {
                            cutover_offset: actual_cutover_offset,
                            applied_tick_count: 0,
                        };
                    }
                };
                match finalize {
                    Ok(_) => {
                        boundary_count += 1;
                    }
                    Err(err) => {
                        record_runtime_event(
                            runtime_events,
                            Some(db),
                            RuntimeEventLevel::Error,
                            "session.rth_close_finalize_failed",
                            "session",
                            "Warm-replay RTH close finalization failed; keeping boundary pinned for retry.",
                            serde_json::json!({
                                "timestampMs": tick.timestamp_ms,
                                "error": format!("{err:?}"),
                                "source": "startup_replay",
                            }),
                        );
                        continue;
                    }
                }
            } else if session_changed
                && new_session != SessionType::Unknown
                && current_session != SessionType::Unknown
            {
                // Other known→known session transitions (e.g. Globex→RTH at
                // 09:30 ET). Reuses prepare_for_new_session for consistency
                // with the live path.
                drop(pipelines_guard);
                prepare_for_new_session(
                    pipelines,
                    db,
                    Some(runtime_events),
                    new_session,
                    new_segment,
                    tick.timestamp_ms,
                    contract_metadata,
                );
                pipelines_guard = match pipelines.lock() {
                    Ok(p) => p,
                    Err(_) => {
                        return StartupWarmReplayResult {
                            cutover_offset: actual_cutover_offset,
                            applied_tick_count: 0,
                        };
                    }
                };
                boundary_count += 1;
            } else if session_changed
                && current_session == SessionType::Unknown
                && new_session != SessionType::Unknown
            {
                // Unknown→known transition (e.g. Unknown→Globex at 18:00 ET
                // after the 16:00 ET close finalization, or cold start
                // crossing into RTH/Globex). prepare_for_new_session is
                // idempotent: if state was already prepared by an earlier
                // finalize_rth_close, the DB read returns the same atomically
                // persisted values.
                drop(pipelines_guard);
                prepare_for_new_session(
                    pipelines,
                    db,
                    Some(runtime_events),
                    new_session,
                    new_segment,
                    tick.timestamp_ms,
                    contract_metadata,
                );
                pipelines_guard = match pipelines.lock() {
                    Ok(p) => p,
                    Err(_) => {
                        return StartupWarmReplayResult {
                            cutover_offset: actual_cutover_offset,
                            applied_tick_count: 0,
                        };
                    }
                };
                boundary_count += 1;
            } else if !session_changed
                && new_segment != current_delta_segment
                && current_delta_segment != DeltaSegment::Unknown
                && new_segment != DeltaSegment::Unknown
            {
                pipelines_guard.reset_segment(new_segment);
                boundary_count += 1;
            }

            // Track Unknown explicitly so a subsequent Unknown→known transition
            // can prepare the next session correctly even though the gap
            // window itself produces no pipeline updates.
            current_session = new_session;
            if new_segment != DeltaSegment::Unknown {
                current_delta_segment = new_segment;
            }
        }

        let is_buy = matches!(tick.side, TradeSide::Buy);
        let minute = minute_of_session_from_timestamp(tick.timestamp_ms);
        pipelines_guard.on_trade_with_timestamp(
            tick.price,
            tick.volume,
            is_buy,
            minute,
            tick.timestamp_ms,
        );
        if current_session == SessionType::Rth {
            let cur_bid = if tick.bid > 0.0 {
                tick.bid
            } else {
                tick.price - 0.25
            };
            let cur_ask = if tick.ask > 0.0 {
                tick.ask
            } else {
                tick.price + 0.25
            };
            let snapshot = pipelines_guard.snapshot(cur_bid, cur_ask);
            let setup_trading_day = trading_day_from_timestamp_ms(tick.timestamp_ms);
            drop(pipelines_guard);
            evaluate_setups_for_snapshot(
                rules,
                playbook_cache,
                db,
                Some(runtime_events),
                &snapshot,
                &setup_trading_day,
                tick.timestamp_ms,
                SetupPersistencePolicy::StartupReplay,
            );
            pipelines_guard = match pipelines.lock() {
                Ok(p) => p,
                Err(_) => {
                    return StartupWarmReplayResult {
                        cutover_offset: actual_cutover_offset,
                        applied_tick_count: 0,
                    };
                }
            };
        }
        applied_tick_count += 1;
        last_applied_tick = Some(tick.clone());
    }

    let warm_monotonicity = monotonic_guard.into_stats();
    if warm_monotonicity.has_violations() {
        record_runtime_event(
            runtime_events,
            Some(db),
            RuntimeEventLevel::Warn,
            "scid.non_monotonic_skip_summary",
            "scid",
            "Startup warm replay skipped non-monotonic SCID ticks.",
            serde_json::json!({
                "skippedNonMonotonicTicks": warm_monotonicity.skipped_non_monotonic_ticks,
                "duplicateTimestampTicks": warm_monotonicity.duplicate_timestamp_ticks,
                "backwardTimestampTicks": warm_monotonicity.backward_timestamp_ticks,
            }),
        );
    }
    let Some(last) = last_applied_tick else {
        record_runtime_event(
            runtime_events,
            Some(db),
            RuntimeEventLevel::Warn,
            "scid.warm_replay.skipped_all",
            "scid",
            "Startup warm replay skipped all candidate ticks due to non-monotonic timestamps.",
            serde_json::json!({
                "candidateTicks": ticks.len(),
            }),
        );
        return StartupWarmReplayResult {
            cutover_offset: actual_cutover_offset,
            applied_tick_count: 0,
        };
    };

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
    let snapshot = pipelines_guard.snapshot(bid, ask);

    // Sync flow emitter counts so live polling doesn't emit stale events.
    if let Ok(mut fe) = flow_emitter.lock() {
        fe.sync_counts(&pipelines_guard);
    }
    drop(pipelines_guard);
    if let Ok(db) = db.lock() {
        let _ = db.upsert_feature_state(
            last.timestamp_ms,
            &serde_json::to_value(&snapshot).unwrap_or_default(),
        );
    }

    // Post-replay reconciliation: if the warm-replay tail ended mid-RTH but
    // the wall clock has moved past 16:00 ET on that same date, the
    // SCID file is missing the post-close ticks that would normally drive
    // the boundary detector. Force the close finalization here so the
    // session_summaries / prior_day_levels rows exist before live polling
    // begins. This covers the "process started after RTH close, no Unknown
    // ticks in SCID" edge case.
    if current_session == SessionType::Rth {
        let now_ms = chrono::Utc::now().timestamp_millis() as f64;
        let last_session_date = session_date_from_timestamp_ms(last.timestamp_ms);
        let now_session_date = session_date_from_timestamp_ms(now_ms);
        let now_et = et_minutes_from_timestamp(now_ms).unwrap_or(0);
        let past_close = last_session_date == now_session_date && now_et >= RTH_CLOSE_ET;
        let summary_exists = db
            .lock()
            .ok()
            .and_then(|d| d.has_session_summary_for(&last_session_date, "RTH").ok())
            .unwrap_or(true);
        if past_close && !summary_exists {
            record_runtime_event(
                runtime_events,
                Some(db),
                RuntimeEventLevel::Warn,
                "session.rth_close_reconcile_started",
                "session",
                "Warm replay ended mid-RTH after the close; reconciling close from pipeline state.",
                serde_json::json!({
                    "sessionDate": &last_session_date,
                    "lastTickTimestampMs": last.timestamp_ms,
                }),
            );
            if let Err(err) = finalize_rth_close(
                pipelines,
                db,
                Some(runtime_events),
                None,
                Some(flow_emitter),
                last.timestamp_ms,
                bid,
                ask,
                contract_metadata,
            ) {
                record_runtime_event(
                    runtime_events,
                    Some(db),
                    RuntimeEventLevel::Error,
                    "session.rth_close_finalize_failed",
                    "session",
                    "Warm-replay reconciliation close finalization failed.",
                    serde_json::json!({
                        "sessionDate": &last_session_date,
                        "error": format!("{err:?}"),
                        "source": "startup_replay_reconciliation",
                    }),
                );
            }
        }
    }

    record_runtime_event(
        runtime_events,
        Some(db),
        RuntimeEventLevel::Info,
        "scid.warm_replay.completed",
        "scid",
        "Startup warm replay completed.",
        serde_json::json!({
            "appliedTicks": applied_tick_count,
            "sessionBoundaries": boundary_count,
            "lastPrice": last.price,
            "cutoverOffset": actual_cutover_offset,
        }),
    );

    StartupWarmReplayResult {
        cutover_offset: actual_cutover_offset,
        applied_tick_count,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let logging_config = load_logging_config();
    let mut effective_logging_config = logging_config.clone();
    let logging_runtime = match init_logging(&logging_config) {
        Ok(runtime) => runtime,
        Err(primary_err) => {
            let fallback = the_desk_backend::observability::LoggingConfig::stderr_only();
            match init_logging(&fallback) {
                Ok(runtime) => {
                    effective_logging_config = fallback;
                    eprintln!(
                        "[the-desk-mcp] logging initialization degraded to stderr: {primary_err}"
                    );
                    runtime
                }
                Err(fallback_err) => {
                    effective_logging_config = fallback;
                    eprintln!(
                        "[the-desk-mcp] logging initialization disabled: primary={primary_err}; fallback={fallback_err}"
                    );
                    the_desk_backend::observability::LoggingRuntime::disabled()
                }
            }
        }
    };
    let runtime_events = Arc::new(RuntimeEventStore::new(&effective_logging_config));

    let db_path = data_dir().join("data.db");
    let db = Database::open(&db_path.to_string_lossy())?;
    prune_runtime_events_if_enabled(runtime_events.as_ref(), &db);
    let config = load_feed_config();
    let contract_metadata = resolve_contract_metadata(&config);

    let mut pipelines = PipelineEngine::new();
    pipelines.set_contract_metadata(contract_metadata.clone());
    if let Ok(volumes) = db.recent_rth_session_volumes(20) {
        let curves: Vec<Vec<f64>> = volumes
            .into_iter()
            .map(RvolPipeline::curve_from_total_volume)
            .collect();
        pipelines.rvol.load_historical_curve(&curves);
    }

    // Load prior-day levels so MCP tools return correct values before backfill.
    let today = the_desk_backend::et_now_trading_day();
    let (root_symbol, contract_symbol) = contract_scope(&contract_metadata);
    let (startup_prior, startup_rollover_status) = authoritative_prior_reference_from_db(
        &db,
        &contract_metadata,
        Some(&contract_metadata),
        &today,
    )
    .unwrap_or((
        None,
        build_rollover_status_from_db(
            &db,
            &contract_metadata,
            Some(&contract_metadata),
            &today,
            None,
        )?,
    ));
    if let Some(prior_ref) = startup_prior {
        pipelines
            .levels
            .set_prior_day(prior_ref.high, prior_ref.low, prior_ref.close);
        pipelines.levels.set_prior_day_contract_context(
            prior_ref.root_symbol.as_deref(),
            prior_ref.contract_symbol.as_deref(),
            contract_symbol,
        );
        if let (Some(vh), Some(vl), Some(pc)) = (prior_ref.va_high, prior_ref.va_low, prior_ref.poc)
        {
            pipelines.levels.set_prior_profile(vh, vl, pc);
        }
        if let (Some(dh), Some(dl), Some(dp)) =
            (prior_ref.dnva_high, prior_ref.dnva_low, prior_ref.dnp)
        {
            pipelines.levels.set_prior_dnva(dh, dl, dp);
        }
    } else {
        pipelines.levels.clear_prior_references();
        pipelines
            .levels
            .set_prior_day_contract_context(root_symbol, None, contract_symbol);
        record_runtime_event(
            &runtime_events,
            None,
            RuntimeEventLevel::Warn,
            "rollover.startup_prior_levels_cleared",
            "rollover",
            "Startup prior levels were cleared because no authoritative prior reference was available.",
            serde_json::json!({
                "status": startup_rollover_status.status,
                "agentAction": startup_rollover_status.agent_action,
                "priorReferenceTrust": startup_rollover_status.prior_reference_trust,
                "activeContract": startup_rollover_status.active_contract_symbol,
                "lookupDate": today,
            }),
        );
    }

    let reader = ScidReader::from_feed_config(&config);
    let scid_available = reader.path().exists();
    let mut startup_cutover_rx = None;

    // Create the server immediately so stdio is ready before backfill starts.
    // The startup backfill runs in a background task and populates pipeline
    // state concurrently with tool serving.
    let server = TheDeskMcp::with_runtime_events(
        db,
        pipelines,
        db_path.to_string_lossy().to_string(),
        Arc::clone(&runtime_events),
    );
    spawn_runtime_event_pruner(Arc::clone(&server.runtime_events), Arc::clone(&server.db));
    spawn_attention_periodic_pulse(
        Arc::clone(&server.pipelines),
        Arc::clone(&server.db),
        Arc::clone(&server.runtime_events),
        Arc::clone(&server.last_bid),
        Arc::clone(&server.last_ask),
    );
    // Keep non-blocking file appenders alive for the lifetime of the MCP server.
    let _ = &logging_runtime;
    record_runtime_event(
        &server.runtime_events,
        Some(&server.db),
        RuntimeEventLevel::Info,
        "mcp.startup",
        "mcp",
        "The Desk MCP server initialized.",
        serde_json::json!({
            "dbPath": db_path.to_string_lossy(),
            "scidPath": reader.path().display().to_string(),
            "scidAvailable": scid_available,
            "contractSymbol": contract_metadata.contract_symbol,
            "rootSymbol": contract_metadata.root_symbol,
        }),
    );
    server.hydrate_playbook_runtime_cache().map_err(|e| {
        std::io::Error::other(format!(
            "failed to hydrate playbook runtime cache from SQLite: {e}"
        ))
    })?;

    if scid_available {
        let (startup_cutover_tx, rx) = tokio::sync::oneshot::channel::<u64>();
        startup_cutover_rx = Some(rx);
        // Spawn background startup backfill from 2 Globex opens ago.
        // Clones the shared Arcs from the server so the backfill can update
        // pipeline and DB state without blocking the MCP listener.
        let pipelines_startup = Arc::clone(&server.pipelines);
        let flow_emitter_startup = Arc::clone(&server.flow_emitter);
        let rules_startup = Arc::clone(&server.rules);
        let playbook_cache_startup = Arc::clone(&server.playbook_cache);
        let db_startup = Arc::clone(&server.db);
        let runtime_events_startup = Arc::clone(&server.runtime_events);
        let reader_startup = reader.clone();
        let contract_metadata_startup = contract_metadata.clone();
        let feed_rt_startup = (*server.feed_runtime).clone();
        let feed_rt_startup_status = feed_rt_startup.clone();

        tokio::spawn(async move {
            let fallback_cutover_offset = safe_scid_data_offset(&reader_startup);
            let db_for_replay = Arc::clone(&db_startup);
            let runtime_events_for_replay = Arc::clone(&runtime_events_startup);
            let startup = tokio::task::spawn_blocking(move || {
                let since = globex_open_ms(2);
                let requested_cutover_offset = reader_startup
                    .current_aligned_end_offset()
                    .unwrap_or(safe_scid_data_offset(&reader_startup));
                record_runtime_event(
                    &runtime_events_for_replay,
                    Some(&db_for_replay),
                    RuntimeEventLevel::Info,
                    "scid.warm_replay.started",
                    "scid",
                    "Startup warm replay started.",
                    serde_json::json!({
                        "scidPath": reader_startup.path().display().to_string(),
                        "sinceMs": since,
                        "requestedCutoverOffset": requested_cutover_offset,
                    }),
                );
                run_startup_warm_replay(
                    &reader_startup,
                    &pipelines_startup,
                    &flow_emitter_startup,
                    &rules_startup,
                    &playbook_cache_startup,
                    &db_for_replay,
                    &runtime_events_for_replay,
                    &feed_rt_startup,
                    since,
                    requested_cutover_offset,
                    &contract_metadata_startup,
                )
            })
            .await
            .unwrap_or_else(|err| {
                record_runtime_event(
                    &runtime_events_startup,
                    Some(&db_startup),
                    RuntimeEventLevel::Error,
                    "scid.warm_replay.failed",
                    "scid",
                    "Startup warm replay task failed to join.",
                    serde_json::json!({
                        "error": err.to_string(),
                        "fallbackCutoverOffset": fallback_cutover_offset,
                    }),
                );
                StartupWarmReplayResult {
                    cutover_offset: fallback_cutover_offset,
                    applied_tick_count: 0,
                }
            });
            record_runtime_event(
                &runtime_events_startup,
                Some(&db_startup),
                RuntimeEventLevel::Info,
                "scid.startup_cutover",
                "scid",
                "Startup SCID cutover selected.",
                serde_json::json!({
                    "cutoverOffset": startup.cutover_offset,
                    "warmTicksApplied": startup.applied_tick_count,
                }),
            );
            feed_rt_startup_status
                .rules_warm_replay_complete
                .store(true, Ordering::Release);
            let _ = startup_cutover_tx.send(startup.cutover_offset);
        });
    } else {
        record_runtime_event(
            &server.runtime_events,
            Some(&server.db),
            RuntimeEventLevel::Warn,
            "scid.file_missing",
            "scid",
            "Configured SCID file was not found.",
            serde_json::json!({
                "scidPath": reader.path().display().to_string(),
            }),
        );
        server
            .feed_runtime
            .rules_warm_replay_complete
            .store(true, Ordering::Release);
    }

    // Background: poll .scid for new ticks and update pipeline engine + DB
    if scid_available {
        let startup_cutover_rx = startup_cutover_rx.take();
        let pipelines_bg = Arc::clone(&server.pipelines);
        let detector_bg = Arc::clone(&server.detector);
        let flow_emitter_bg = Arc::clone(&server.flow_emitter);
        let rules_bg = Arc::clone(&server.rules);
        let playbook_cache_bg = Arc::clone(&server.playbook_cache);
        let last_bid_bg = Arc::clone(&server.last_bid);
        let last_ask_bg = Arc::clone(&server.last_ask);
        let db_bg = Arc::clone(&server.db);
        let runtime_events_bg = Arc::clone(&server.runtime_events);
        let poll_ms = config.flush_poll_ms;
        let price_scale = config.price_scale;
        let reader_path = reader.path().to_path_buf();
        let contract_metadata = contract_metadata.clone();
        let feed_rt_bg = Arc::clone(&server.feed_runtime);

        tokio::spawn(async move {
            use tokio::time::{sleep, Duration};

            let poll = Duration::from_millis(poll_ms.max(250));
            let mut offset: u64;
            let mut last_market_tick_ts: f64 = 0.0;
            let mut persist_counter: u64 = 0;
            let mut event_buffer = Vec::new();
            let mut tick_buffer: Vec<RawTickBatchRow> = Vec::new();
            let mut last_integrity_check =
                std::time::Instant::now() - std::time::Duration::from_secs(30);
            let mut monotonic_guard = MonotonicTickGuard::default();
            let mut last_reported_non_monotonic_skips = 0u64;
            let mut last_non_monotonic_summary_wall_ms = 0u64;
            // Seed current session and segment from the system clock so we can detect boundaries.
            let now_et = et_minutes_from_timestamp(chrono::Utc::now().timestamp_millis() as f64);
            let mut current_session = now_et.map(classify_session).unwrap_or(SessionType::Unknown);
            let mut current_delta_segment = now_et
                .map(classify_delta_segment)
                .unwrap_or(DeltaSegment::Unknown);

            offset = if let Some(rx) = startup_cutover_rx {
                match rx.await {
                    Ok(cutover_offset) => cutover_offset,
                    Err(_) => safe_scid_data_offset(&ScidReader::new(reader_path.clone())),
                }
            } else {
                let reader_for_offset =
                    ScidReader::with_price_scale(reader_path.clone(), price_scale);
                tokio::task::spawn_blocking(move || {
                    reader_for_offset
                        .current_aligned_end_offset()
                        .unwrap_or(safe_scid_data_offset(&reader_for_offset))
                })
                .await
                .unwrap_or_else(|_| safe_scid_data_offset(&ScidReader::new(reader_path.clone())))
            };
            feed_rt_bg.scid_tail_offset.store(offset, Ordering::Release);

            loop {
                sleep(poll).await;
                if last_integrity_check.elapsed() >= std::time::Duration::from_secs(15) {
                    let pipeline_invariants = pipelines_bg
                        .lock()
                        .ok()
                        .map(|p| p.validate_invariants())
                        .unwrap_or_default();
                    if let Ok(db) = db_bg.lock() {
                        persist_integrity_check(&db, &pipeline_invariants, &feed_rt_bg);
                    }
                    last_integrity_check = std::time::Instant::now();
                }

                let reader_for_step =
                    ScidReader::with_price_scale(reader_path.clone(), price_scale);
                let step = match tokio::task::spawn_blocking(move || {
                    read_scid_poll_step(&reader_for_step, offset)
                })
                .await
                {
                    Ok(Ok(step)) => step,
                    Ok(Err(err)) => {
                        record_runtime_event(
                            &runtime_events_bg,
                            Some(&db_bg),
                            RuntimeEventLevel::Error,
                            "scid.poll_failed",
                            "scid",
                            "SCID poll step failed.",
                            serde_json::json!({ "error": err.to_string(), "offset": offset }),
                        );
                        continue;
                    }
                    Err(err) => {
                        record_runtime_event(
                            &runtime_events_bg,
                            Some(&db_bg),
                            RuntimeEventLevel::Error,
                            "scid.poll_failed",
                            "scid",
                            "SCID poll task failed to join.",
                            serde_json::json!({ "error": err.to_string(), "offset": offset }),
                        );
                        continue;
                    }
                };
                feed_rt_bg
                    .scid_file_len
                    .store(step.file_len, Ordering::Release);
                feed_rt_bg.last_scid_poll_wall_ms.store(
                    chrono::Utc::now().timestamp_millis() as u64,
                    Ordering::Release,
                );

                if step.was_realigned() {
                    feed_rt_bg
                        .scid_tail_reset_count
                        .fetch_add(1, Ordering::AcqRel);
                    if step.was_shrink_reset() {
                        monotonic_guard = MonotonicTickGuard::default();
                        feed_rt_bg
                            .scid_last_shrink_len
                            .store(step.file_len, Ordering::Release);
                        record_runtime_event(
                            &runtime_events_bg,
                            Some(&db_bg),
                            RuntimeEventLevel::Warn,
                            "scid.tail_reset",
                            "scid",
                            "SCID file shrank below tail offset; reset tail offset.",
                            serde_json::json!({
                                "startOffset": step.start_offset,
                                "fileLen": step.file_len,
                            }),
                        );
                    } else {
                        record_runtime_event(
                            &runtime_events_bg,
                            Some(&db_bg),
                            RuntimeEventLevel::Warn,
                            "scid.tail_realign",
                            "scid",
                            "SCID tail offset was not record-aligned; realigned.",
                            serde_json::json!({
                                "startOffset": step.start_offset,
                                "fileLen": step.file_len,
                            }),
                        );
                    }
                }
                offset = step.next_offset;
                feed_rt_bg.scid_tail_offset.store(offset, Ordering::Release);

                let mut ticks_this_poll = 0u64;
                for tick in &step.ticks {
                    match monotonic_guard.observe(tick.timestamp_ms) {
                        MonotonicTimestampDecision::Accept => {}
                        MonotonicTimestampDecision::Skip(kind) => {
                            feed_rt_bg.record_non_monotonic_tick(kind, tick.timestamp_ms);
                            continue;
                        }
                    }
                    last_market_tick_ts = tick.timestamp_ms;
                    feed_rt_bg
                        .last_scid_tick_ms_bits
                        .store(tick_ms_to_bits(tick.timestamp_ms), Ordering::Release);
                    // Detect session and segment boundaries during live polling
                    if let Some(et_min) = et_minutes_from_timestamp(tick.timestamp_ms) {
                        let new_session = classify_session(et_min);
                        let new_segment = classify_delta_segment(et_min);
                        let session_changed = new_session != current_session;
                        let exiting_rth = current_session == SessionType::Rth && session_changed;

                        if exiting_rth {
                            // RTH close (RTH→Unknown at 16:00 ET, or RTH→Globex
                            // if the Unknown gap is empty in this feed): persist
                            // session_summaries + prior_day_levels atomically and
                            // refresh in-memory carry-forward so the next live
                            // tick already sees consistent state.
                            let (last_bid_hint, last_ask_hint) =
                                current_best_bid_ask(&last_bid_bg, &last_ask_bg);
                            match finalize_rth_close(
                                &pipelines_bg,
                                &db_bg,
                                Some(&runtime_events_bg),
                                Some(&detector_bg),
                                Some(&flow_emitter_bg),
                                tick.timestamp_ms,
                                last_bid_hint,
                                last_ask_hint,
                                &contract_metadata,
                            ) {
                                Ok(_) => {
                                    record_runtime_event(
                                        &runtime_events_bg,
                                        Some(&db_bg),
                                        RuntimeEventLevel::Info,
                                        "session.boundary",
                                        "session",
                                        "Live session boundary crossed after RTH close finalization.",
                                        serde_json::json!({
                                            "from": format!("{:?}", current_session),
                                            "to": format!("{:?}", new_session),
                                            "timestampMs": tick.timestamp_ms,
                                            "rthCloseFinalized": true,
                                        }),
                                    );
                                }
                                Err(err) => {
                                    record_runtime_event(
                                        &runtime_events_bg,
                                        Some(&db_bg),
                                        RuntimeEventLevel::Error,
                                        "session.rth_close_finalize_failed",
                                        "session",
                                        "Live RTH close finalization failed; skipping post-close tick so the next tick retries.",
                                        serde_json::json!({
                                            "timestampMs": tick.timestamp_ms,
                                            "error": format!("{err:?}"),
                                            "source": "live_tail",
                                        }),
                                    );
                                    continue;
                                }
                            }
                        } else if session_changed
                            && new_session != SessionType::Unknown
                            && current_session != SessionType::Unknown
                        {
                            // Other known→known transitions, e.g. Globex→RTH at
                            // 09:30 ET. Reuses the shared boundary helper.
                            prepare_for_new_session(
                                &pipelines_bg,
                                &db_bg,
                                Some(&runtime_events_bg),
                                new_session,
                                new_segment,
                                tick.timestamp_ms,
                                &contract_metadata,
                            );
                            if let Ok(mut det) = detector_bg.lock() {
                                det.reset();
                            }
                            if let Ok(mut fe) = flow_emitter_bg.lock() {
                                fe.reset();
                            }
                            record_runtime_event(
                                &runtime_events_bg,
                                Some(&db_bg),
                                RuntimeEventLevel::Info,
                                "session.boundary",
                                "session",
                                "Live session boundary crossed.",
                                serde_json::json!({
                                    "from": format!("{:?}", current_session),
                                    "to": format!("{:?}", new_session),
                                    "timestampMs": tick.timestamp_ms,
                                }),
                            );
                        } else if session_changed
                            && current_session == SessionType::Unknown
                            && new_session != SessionType::Unknown
                        {
                            // Unknown→known (e.g. 18:00 ET Globex open after RTH
                            // already closed earlier in this process, or cold
                            // start landing inside RTH/Globex). Idempotent with
                            // any in-memory state finalize_rth_close already
                            // installed.
                            prepare_for_new_session(
                                &pipelines_bg,
                                &db_bg,
                                Some(&runtime_events_bg),
                                new_session,
                                new_segment,
                                tick.timestamp_ms,
                                &contract_metadata,
                            );
                            if let Ok(mut det) = detector_bg.lock() {
                                det.reset();
                            }
                            if let Ok(mut fe) = flow_emitter_bg.lock() {
                                fe.reset();
                            }
                            record_runtime_event(
                                &runtime_events_bg,
                                Some(&db_bg),
                                RuntimeEventLevel::Info,
                                "session.boundary",
                                "session",
                                "Live session boundary crossed from Unknown.",
                                serde_json::json!({
                                    "from": "Unknown",
                                    "to": format!("{:?}", new_session),
                                    "timestampMs": tick.timestamp_ms,
                                }),
                            );
                        } else if !session_changed
                            && new_segment != current_delta_segment
                            && current_delta_segment != DeltaSegment::Unknown
                            && new_segment != DeltaSegment::Unknown
                        {
                            if let Ok(mut p) = pipelines_bg.lock() {
                                p.reset_segment(new_segment);
                                record_runtime_event(
                                    &runtime_events_bg,
                                    Some(&db_bg),
                                    RuntimeEventLevel::Info,
                                    "session.segment_boundary",
                                    "session",
                                    "Live delta segment boundary crossed.",
                                    serde_json::json!({
                                        "from": format!("{:?}", current_delta_segment),
                                        "to": format!("{:?}", new_segment),
                                        "timestampMs": tick.timestamp_ms,
                                    }),
                                );
                            }
                        }

                        // Track Unknown explicitly so the next Unknown→known
                        // transition triggers prepare_for_new_session.
                        current_session = new_session;
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
                        &playbook_cache_bg,
                        &db_bg,
                        &runtime_events_bg,
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
                        contract_metadata.root_symbol.clone(),
                        contract_metadata.contract_symbol.clone(),
                    ));

                    if tick_buffer.len() >= 100 {
                        if let Ok(db) = db_bg.lock() {
                            let _ = db.insert_raw_ticks_batch(&tick_buffer);
                        }
                        tick_buffer.clear();
                    }

                    ticks_this_poll += 1;
                }

                let monotonicity = feed_rt_bg.monotonicity_snapshot();
                let new_non_monotonic_skips = monotonicity
                    .skipped_non_monotonic_ticks
                    .saturating_sub(last_reported_non_monotonic_skips);
                let now_wall_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
                if new_non_monotonic_skips > 0
                    && (last_non_monotonic_summary_wall_ms == 0
                        || now_wall_ms.saturating_sub(last_non_monotonic_summary_wall_ms) >= 30_000)
                {
                    record_runtime_event(
                        &runtime_events_bg,
                        Some(&db_bg),
                        RuntimeEventLevel::Warn,
                        "scid.non_monotonic_skip_summary",
                        "scid",
                        "Live tail skipped non-monotonic SCID ticks.",
                        serde_json::json!({
                            "newSkippedNonMonotonicTicks": new_non_monotonic_skips,
                            "skippedNonMonotonicTicks": monotonicity.skipped_non_monotonic_ticks,
                            "duplicateTimestampTicks": monotonicity.duplicate_timestamp_ticks,
                            "backwardTimestampTicks": monotonicity.backward_timestamp_ticks,
                            "lastNonMonotonicTimestampMs": monotonicity.last_non_monotonic_timestamp_ms,
                        }),
                    );
                    last_reported_non_monotonic_skips = monotonicity.skipped_non_monotonic_ticks;
                    last_non_monotonic_summary_wall_ms = now_wall_ms;
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
                        if let Some((timestamp_ms, payload)) =
                            build_live_feature_state_snapshot_payload(
                                &pipelines_bg,
                                &last_bid_bg,
                                &last_ask_bg,
                                last_market_tick_ts,
                            )
                        {
                            persist_feature_state_payload(&db_bg, timestamp_ms, &payload);
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
        let feed_depth_rt = Arc::clone(&server.feed_runtime);
        let runtime_events_depth = Arc::clone(&server.runtime_events);

        tokio::spawn(async move {
            let poll = Duration::from_millis(1_000);
            let mut state = DepthPollWorkerState::default();

            loop {
                let state_for_step = state;
                let step = tokio::task::spawn_blocking(move || {
                    let mut next_state = state_for_step;
                    let work = compute_depth_poll_step(&mut next_state);
                    (next_state, work)
                })
                .await;

                let (next_state, work) = match step {
                    Ok(output) => output,
                    Err(err) => {
                        record_runtime_event(
                            &runtime_events_depth,
                            Some(&db_depth),
                            RuntimeEventLevel::Error,
                            "depth.poll_failed",
                            "depth",
                            "Depth poll task failed to join.",
                            serde_json::json!({ "error": err.to_string() }),
                        );
                        state = DepthPollWorkerState::default();
                        sleep(poll).await;
                        continue;
                    }
                };
                state = next_state;

                match work {
                    Ok(Some(work)) => {
                        state.batch_id = apply_depth_persist_work(
                            &db_depth,
                            &pipelines_depth,
                            &last_bid_depth,
                            &last_ask_depth,
                            work,
                            feed_depth_rt.as_ref(),
                        );
                    }
                    Ok(None) => {}
                    Err(err) => {
                        record_runtime_event(
                            &runtime_events_depth,
                            Some(&db_depth),
                            RuntimeEventLevel::Error,
                            "depth.worker_failed",
                            "depth",
                            "Depth worker failed.",
                            serde_json::json!({ "error": err.to_string() }),
                        );
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
    use std::io::Write;
    use std::path::Path;
    use tempfile::{tempdir, NamedTempFile};
    use the_desk_backend::db::SessionSummary;
    use the_desk_backend::rollover::{ContractRolloverAgentAction, ContractRolloverStatusKind};
    use the_desk_backend::rules::SetupReadiness;

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
            root_symbol: "NQ".to_string(),
            contract_symbol: "NQH26.CME".to_string(),
            contract_month: Some("2026-03".to_string()),
            symbol_resolution_mode: "hybrid".to_string(),
            carry_forward_levels_valid: true,
            rollover_warning: None,
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
        let server = TheDeskMcp::new(db, PipelineEngine::new(), ":memory:".into());
        server
            .hydrate_playbook_runtime_cache()
            .expect("hydrate playbook cache");
        server
    }

    fn test_contract_metadata() -> the_desk_backend::feed::ContractMetadata {
        the_desk_backend::feed::ContractMetadata {
            root_symbol: "NQ".to_string(),
            contract_symbol: "NQH26".to_string(),
            contract_month: Some("2026-03".to_string()),
            symbol_resolution_mode: "manual".to_string(),
            symbol_resolution_source: "test".to_string(),
            configured_symbol: "NQH26".to_string(),
            scid_file_exists: true,
            depth_file_count: 1,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn get_runtime_events_returns_recent_and_persisted_events() {
        let server = test_server();
        record_runtime_event(
            &server.runtime_events,
            Some(&server.db),
            RuntimeEventLevel::Warn,
            "scid.tail_reset",
            "scid",
            "test tail reset",
            serde_json::json!({ "offset": 512 }),
        );

        let payload = parse_text_tool_result(
            server
                .get_runtime_events(Parameters(RuntimeEventsParams {
                    limit: Some(10),
                    min_level: Some("warn".to_string()),
                    category: Some("scid".to_string()),
                    include_persisted: Some(true),
                    ..Default::default()
                }))
                .await
                .expect("runtime events"),
        );
        assert_eq!(payload["recentCount"].as_u64(), Some(1));
        assert_eq!(payload["persistedCount"].as_u64(), Some(1));
        let events = payload["events"].as_array().expect("events array");
        assert!(events
            .iter()
            .any(|event| event["eventName"].as_str() == Some("scid.tail_reset")));
    }

    #[tokio::test]
    async fn get_runtime_events_rejects_level_and_min_level_together() {
        let server = test_server();
        let result = server
            .get_runtime_events(Parameters(RuntimeEventsParams {
                level: Some("warn".to_string()),
                min_level: Some("info".to_string()),
                ..Default::default()
            }))
            .await;
        assert!(result.is_err());
    }

    #[test]
    fn rollover_runtime_event_does_not_relock_held_db_mutex() {
        use std::sync::mpsc;
        use std::time::Duration;

        let server = test_server();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let db = server.db.lock().expect("db lock");
            let contract = rollover_contract_metadata("NQM26");
            let result = server.rollover_status_for_date(
                &db,
                &contract,
                Some(&test_contract_metadata()),
                "2026-03-06",
                None,
            );
            let _ = tx.send(result.is_ok());
        });

        assert!(rx
            .recv_timeout(Duration::from_secs(2))
            .expect("no deadlock"));
    }

    fn rollover_contract_metadata(
        contract_symbol: &str,
    ) -> the_desk_backend::feed::ContractMetadata {
        the_desk_backend::feed::ContractMetadata {
            root_symbol: "NQ".to_string(),
            contract_symbol: contract_symbol.to_string(),
            contract_month: Some("2026-03".to_string()),
            symbol_resolution_mode: "manual".to_string(),
            symbol_resolution_source: "test".to_string(),
            configured_symbol: contract_symbol.to_string(),
            scid_file_exists: true,
            depth_file_count: 1,
            ..Default::default()
        }
    }

    #[test]
    fn rollover_status_helper_accepts_same_contract_prior_reference() {
        let db = Database::open(":memory:").expect("db");
        db.save_prior_day_full_with_dnva_contract(
            "2026-03-04",
            21_100.0,
            20_900.0,
            21_000.0,
            21_050.0,
            20_950.0,
            21_000.0,
            Some(21_025.0),
            Some(20_975.0),
            Some(21_000.0),
            Some("NQ"),
            Some("NQH26"),
        )
        .expect("save prior");
        let contract = rollover_contract_metadata("NQH26");
        let status = build_rollover_status_from_db(
            &db,
            &contract,
            Some(&contract),
            "2026-03-05",
            Some(1_000.0),
        )
        .expect("status");

        assert_eq!(status.status, ContractRolloverStatusKind::Ok);
        assert_eq!(
            status.agent_action,
            ContractRolloverAgentAction::UsePriorLevels
        );
        assert_eq!(
            status
                .prior_day_reference
                .as_ref()
                .and_then(|r| r.contract_symbol.as_deref()),
            Some("NQH26")
        );
        assert!(status.prior_references_authoritative);
    }

    #[test]
    fn rollover_status_helper_labels_previous_contract_reference_as_legacy() {
        let db = Database::open(":memory:").expect("db");
        db.save_prior_day_full_with_dnva_contract(
            "2026-03-04",
            21_100.0,
            20_900.0,
            21_000.0,
            21_050.0,
            20_950.0,
            21_000.0,
            Some(21_025.0),
            Some(20_975.0),
            Some(21_000.0),
            Some("NQ"),
            Some("NQH26"),
        )
        .expect("save prior");
        let active = rollover_contract_metadata("NQM26");
        let status =
            build_rollover_status_from_db(&db, &active, Some(&active), "2026-03-05", Some(1_000.0))
                .expect("status");

        assert_eq!(status.status, ContractRolloverStatusKind::RolloverDetected);
        assert_eq!(
            status.agent_action,
            ContractRolloverAgentAction::LegacyContextOnly
        );
        assert_eq!(
            status
                .legacy_contract_reference
                .as_ref()
                .and_then(|r| r.contract_symbol.as_deref()),
            Some("NQH26")
        );
        assert!(!status.prior_references_authoritative);
        assert!(status.should_clear_prior_levels);
    }

    #[tokio::test]
    async fn validate_contract_rollover_tool_returns_structured_status() {
        let server = test_server();
        let contract = resolve_contract_metadata(&load_feed_config());
        {
            let mut pipelines = server.pipelines.lock().expect("pipelines");
            pipelines.set_contract_metadata(contract.clone());
        }
        if !contract.root_symbol.is_empty() && !contract.contract_symbol.is_empty() {
            let db = server.db.lock().expect("db");
            db.save_prior_day_full_with_dnva_contract(
                "2026-03-04",
                21_100.0,
                20_900.0,
                21_000.0,
                21_050.0,
                20_950.0,
                21_000.0,
                Some(21_025.0),
                Some(20_975.0),
                Some(21_000.0),
                Some(contract.root_symbol.as_str()),
                Some(contract.contract_symbol.as_str()),
            )
            .expect("save prior");
        }

        let result = parse_text_tool_result(
            server
                .validate_contract_rollover()
                .await
                .expect("validate rollover"),
        );
        assert!(result.get("status").is_some());
        assert_eq!(
            result
                .get("activeContractSymbol")
                .and_then(|value| value.as_str()),
            Some(contract.contract_symbol.to_ascii_uppercase().as_str())
        );
        assert!(result.get("priorReferenceTrust").is_some());
    }

    fn write_scid_header(file: &mut NamedTempFile) {
        const SCID_HEADER_SIZE_TEST: usize = 56;
        let mut header = vec![0_u8; SCID_HEADER_SIZE_TEST];
        header[0..4].copy_from_slice(b"SCID");
        header[4..8].copy_from_slice(&(SCID_HEADER_SIZE_TEST as u32).to_le_bytes());
        header[8..12].copy_from_slice(&(SCID_RECORD_SIZE as u32).to_le_bytes());
        file.write_all(&header).expect("header");
        file.flush().expect("flush");
    }

    fn append_scid_record(file: &mut NamedTempFile, price: f32, timestamp_ms: f64) {
        const SC_TO_UNIX_EPOCH_US_TEST: i64 = 2_209_161_600_000_000;
        let mut rec = [0_u8; SCID_RECORD_SIZE];
        let unix_us = (timestamp_ms * 1_000.0).round() as i64;
        let sc_us = SC_TO_UNIX_EPOCH_US_TEST + unix_us;
        rec[0..8].copy_from_slice(&sc_us.to_le_bytes());
        rec[12..16].copy_from_slice(&(price + 0.25).to_le_bytes());
        rec[16..20].copy_from_slice(&(price - 0.25).to_le_bytes());
        rec[20..24].copy_from_slice(&price.to_le_bytes());
        rec[24..28].copy_from_slice(&(1_u32).to_le_bytes());
        rec[28..32].copy_from_slice(&(2_u32).to_le_bytes());
        rec[32..36].copy_from_slice(&(0_u32).to_le_bytes());
        rec[36..40].copy_from_slice(&(2_u32).to_le_bytes());
        file.write_all(&rec).expect("record");
    }

    fn append_scid_record_with_scale(
        file: &mut NamedTempFile,
        price: f64,
        timestamp_ms: f64,
        price_scale: f64,
    ) {
        const SC_TO_UNIX_EPOCH_US_TEST: i64 = 2_209_161_600_000_000;
        let mut rec = [0_u8; SCID_RECORD_SIZE];
        let unix_us = (timestamp_ms * 1_000.0).round() as i64;
        let sc_us = SC_TO_UNIX_EPOCH_US_TEST + unix_us;
        let raw_price = (price * price_scale) as f32;
        let raw_bid = ((price - 0.25) * price_scale) as f32;
        let raw_ask = ((price + 0.25) * price_scale) as f32;
        rec[0..8].copy_from_slice(&sc_us.to_le_bytes());
        rec[12..16].copy_from_slice(&raw_ask.to_le_bytes());
        rec[16..20].copy_from_slice(&raw_bid.to_le_bytes());
        rec[20..24].copy_from_slice(&raw_price.to_le_bytes());
        rec[24..28].copy_from_slice(&(1_u32).to_le_bytes());
        rec[28..32].copy_from_slice(&(2_u32).to_le_bytes());
        rec[32..36].copy_from_slice(&(0_u32).to_le_bytes());
        rec[36..40].copy_from_slice(&(2_u32).to_le_bytes());
        file.write_all(&rec).expect("scaled record");
    }

    fn append_scid_sequence(file: &mut NamedTempFile, start_idx: usize, prices: &[f32]) {
        let base_ts_ms = Utc
            .with_ymd_and_hms(2026, 3, 5, 15, 0, 0)
            .single()
            .expect("base timestamp")
            .timestamp_millis() as f64;
        for (idx, price) in prices.iter().enumerate() {
            let ts_ms = base_ts_ms + (start_idx + idx) as f64;
            append_scid_record(file, *price, ts_ms);
        }
        file.flush().expect("flush");
    }

    fn append_scid_scaled_sequence(
        file: &mut NamedTempFile,
        start_idx: usize,
        prices: &[f64],
        price_scale: f64,
    ) {
        let base_ts_ms = Utc
            .with_ymd_and_hms(2026, 3, 5, 15, 0, 0)
            .single()
            .expect("base timestamp")
            .timestamp_millis() as f64;
        for (idx, price) in prices.iter().enumerate() {
            let ts_ms = base_ts_ms + (start_idx + idx) as f64;
            append_scid_record_with_scale(file, *price, ts_ms, price_scale);
        }
        file.flush().expect("flush");
    }

    fn write_test_depth_file(path: &Path, records: &[(i64, u8, u8, u16, f32, u32)]) {
        const DEPTH_HEADER_SIZE_TEST: usize = 64;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"SCDD");
        bytes.extend_from_slice(&(DEPTH_HEADER_SIZE_TEST as u32).to_le_bytes());
        bytes.extend_from_slice(&(24_u32).to_le_bytes());
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        bytes.extend_from_slice(&[0_u8; DEPTH_HEADER_SIZE_TEST - 16]);
        for (dt, cmd, flags, num_orders, price, qty) in records {
            bytes.extend_from_slice(&dt.to_le_bytes());
            bytes.push(*cmd);
            bytes.push(*flags);
            bytes.extend_from_slice(&num_orders.to_le_bytes());
            bytes.extend_from_slice(&price.to_le_bytes());
            bytes.extend_from_slice(&qty.to_le_bytes());
            bytes.extend_from_slice(&0_u32.to_le_bytes());
        }
        std::fs::write(path, bytes).expect("write depth");
    }

    fn unix_ms_to_sc_depth(ms: i64) -> i64 {
        ms * 1_000 + 2_209_161_600_000_000
    }

    fn parse_text_tool_result(result: CallToolResult) -> serde_json::Value {
        match &result.content[0].raw {
            RawContent::Text(text) => serde_json::from_str(&text.text).expect("json text result"),
            other => panic!("expected text tool result, got {other:?}"),
        }
    }

    #[test]
    fn scid_poll_step_reads_new_ticks_once_from_resume_offset() {
        let mut file = NamedTempFile::new().expect("temp");
        write_scid_header(&mut file);
        append_scid_sequence(&mut file, 0, &[21000.0, 21000.25, 21000.5]);
        let reader = ScidReader::new(file.path());

        let first = read_scid_poll_step(&reader, safe_scid_data_offset(&reader)).expect("first");
        append_scid_sequence(&mut file, 3, &[21000.75, 21001.0]);
        let second = read_scid_poll_step(&reader, first.next_offset).expect("second");

        assert_eq!(first.ticks.len(), 3);
        assert_eq!(first.ticks[0].price, 21000.0);
        assert_eq!(second.ticks.len(), 2);
        assert_eq!(second.ticks[0].price, 21000.75);
        assert!(second.next_offset > first.next_offset);
    }

    #[test]
    fn scid_poll_step_preserves_configured_price_scale() {
        let mut file = NamedTempFile::new().expect("temp");
        write_scid_header(&mut file);
        append_scid_record_with_scale(&mut file, 21000.0, 1_700_000_000_000.0, 100.0);
        append_scid_record_with_scale(&mut file, 21000.25, 1_700_000_000_001.0, 100.0);
        file.flush().expect("flush");

        let reader = ScidReader::with_price_scale(file.path(), 100.0);
        let batch = read_scid_poll_step(&reader, safe_scid_data_offset(&reader)).expect("step");

        assert_eq!(batch.ticks.len(), 2);
        assert!((batch.ticks[0].price - 21000.0).abs() < 1e-9);
        assert!((batch.ticks[1].price - 21000.25).abs() < 1e-9);
        assert!((batch.ticks[0].ask - 21000.25).abs() < 1e-9);
        assert!((batch.ticks[0].bid - 20999.75).abs() < 1e-9);
    }

    #[test]
    fn tape_pace_response_marks_live_and_recomputes_event_lag() {
        let payload = serde_json::json!({
            "ticksPerSec5s": 1.2,
            "ticksPerSec30s": 1.0,
            "ticksPerSec5m": 0.8,
            "volumePerSec5s": 12.0,
            "volumePerSec30s": 10.0,
            "volumePerSec5m": 8.0,
            "acceleration": 0.15,
            "rawAcceleration": 0.2,
            "pacePercentile": 0.7,
            "rollingPacePercentile": 0.8,
            "regimeTicksPerSec30mEma": 0.9,
            "regimeVolumePerSec30mEma": 9.0,
            "windowCoverage5s": 1.0,
            "windowCoverage30s": 1.0,
            "windowCoverage5m": 1.0,
            "isValid5s": true,
            "isValid30s": true,
            "isValid5m": true,
            "windowAnchorTimestampMs": 12_000.0,
            "lastTradeTimestampMs": 12_000.0,
            "dwellAtCurrentPriceMs": 2_500.0,
            "currentPrice": 21000.25
        });
        let rendered = build_tape_pace_response(payload, 250.0, true, 12_900.0);
        assert_eq!(
            rendered.get("dataQuality").and_then(|v| v.as_str()),
            Some("LIVE")
        );
        assert_eq!(rendered.get("isLive").and_then(|v| v.as_bool()), Some(true));
        assert_eq!(
            rendered.get("eventTimeLagMs").and_then(|v| v.as_f64()),
            Some(900.0)
        );
    }

    #[test]
    fn tape_pace_response_marks_partial_when_payload_is_missing_fields() {
        let payload = serde_json::json!({
            "ticksPerSec5s": 1.2,
            "pacePercentile": 0.7,
            "lastTradeTimestampMs": 12_000.0
        });
        let rendered = build_tape_pace_response(payload, 2_000.0, false, 13_000.0);
        assert_eq!(
            rendered.get("dataQuality").and_then(|v| v.as_str()),
            Some("PARTIAL")
        );
        assert_eq!(
            rendered.get("isLive").and_then(|v| v.as_bool()),
            Some(false)
        );
        assert_eq!(
            rendered.get("eventTimeLagMs").and_then(|v| v.as_f64()),
            Some(1_000.0)
        );
    }

    #[test]
    fn tick_ms_bits_roundtrip_positive() {
        let t = 1_700_000_000_123.0;
        assert_eq!(tick_ms_from_bits(tick_ms_to_bits(t)), Some(t));
        assert_eq!(tick_ms_to_bits(0.0), 0);
        assert_eq!(tick_ms_from_bits(0), None);
    }

    #[test]
    fn documented_mcp_tool_count_matches_source() {
        let needle = concat!("#", "[tool");
        let tool_count = include_str!("the-desk-mcp.rs").matches(needle).count();
        let expected = format!("{tool_count} MCP tools");

        assert!(include_str!("../../AGENT.md").contains(&expected));
        assert!(include_str!("../../README.md").contains(&expected));
        assert!(include_str!("../../CLAUDE.md").contains(&expected));
    }

    #[test]
    fn pipeline_lock_recently_contended_uses_a_latched_window() {
        let runtime = McpFeedRuntimeState::default();
        runtime.record_pipeline_lock_sample(true, 10_000);
        assert!(runtime.pipeline_lock_recently_contended(10_000));

        runtime.record_pipeline_lock_sample(false, 10_500);
        assert!(runtime.pipeline_lock_recently_contended(14_999));
        assert!(!runtime
            .pipeline_lock_recently_contended(10_000 + PIPELINE_CONTENTION_RECENT_WINDOW_MS + 1));
    }

    #[test]
    fn current_market_snapshot_payload_surfaces_structured_contention_gap() {
        let server = test_server();
        let pipeline_ts = 1_700_000_000_000.0;
        server
            .feed_runtime
            .last_scid_tick_ms_bits
            .store(tick_ms_to_bits(pipeline_ts), Ordering::Release);

        let _pipeline_guard = server.pipelines.lock().expect("pipelines");
        let payload = server
            .current_market_snapshot_payload()
            .expect("structured contention payload");

        assert_eq!(
            payload.get("snapshotAvailable").and_then(|v| v.as_bool()),
            Some(false)
        );
        assert_eq!(
            payload.get("snapshotSource").and_then(|v| v.as_str()),
            Some("contention_unavailable")
        );
        assert_eq!(
            payload.get("freshnessStatus").and_then(|v| v.as_str()),
            Some("contended")
        );
        assert_eq!(
            payload.get("degradationReason").and_then(|v| v.as_str()),
            Some("pipeline_lock_contended; no_persisted_feature_state_available_yet")
        );
        assert_eq!(
            payload
                .get("pipelineProcessedThroughMs")
                .and_then(|v| v.as_f64()),
            Some(pipeline_ts)
        );
        assert_eq!(
            payload.get("dbLockContended").and_then(|v| v.as_bool()),
            Some(false)
        );
        assert_eq!(
            payload.get("message").and_then(|v| v.as_str()),
            Some(
                "Current market snapshot is temporarily unavailable while live pipeline contention is active. Retry shortly."
            )
        );
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

        let root_only = SessionScopeParams {
            root_symbol: Some("NQ".into()),
            ..Default::default()
        };
        let scope = build_session_scope_filter(&root_only)
            .expect("root-only scope")
            .expect("some");
        assert_eq!(scope.root_symbol.as_deref(), Some("NQ"));
    }

    #[test]
    fn parse_scope_value_validates_loose_scope_payloads() {
        assert!(parse_scope_value(Some(serde_json::json!({
            "sessionType": "bad"
        })))
        .is_err());

        let scope = parse_scope_value(Some(serde_json::json!({
            "rootSymbol": "NQ",
            "continuousMode": true
        })))
        .expect("scope")
        .expect("some");
        assert_eq!(scope.root_symbol.as_deref(), Some("NQ"));
        assert!(scope.continuous_mode);
    }

    #[test]
    fn research_field_validators_accept_supported_values() {
        assert_eq!(
            parse_research_event_type("ib_mid_test").expect("event"),
            "ib_mid_test"
        );
        assert_eq!(
            parse_research_event_type("IB_REENTRY").expect("event"),
            "ib_reentry"
        );
        assert_eq!(
            parse_research_outcome_field("close_vs_vwap").expect("field"),
            "close_vs_vwap"
        );
        assert_eq!(
            parse_distribution_metric("session_delta").expect("metric"),
            "session_delta"
        );
        assert_eq!(
            parse_distribution_metric("IB_RANGE").expect("metric"),
            "ib_range"
        );
        assert!(RESEARCH_DISTRIBUTION_METRICS.contains(&"ib_range"));
        assert!(RESEARCH_DISTRIBUTION_METRICS.contains(&"rvol_ratio"));
        assert_eq!(
            parse_signal_outcome_session_field("balance_state").expect("session field"),
            "balance_state"
        );
        assert_eq!(
            parse_dom_behavior_name("Liquidity_Flip").expect("behavior"),
            "liquidity_flip"
        );
        assert_eq!(
            research::RESEARCH_PERCENTILE_METHOD,
            "linear_interpolation_type7"
        );
        assert_eq!(research::RESEARCH_STDDEV_METHOD, "population");
    }

    #[test]
    fn research_field_validators_reject_invalid_inputs() {
        assert!(parse_research_event_type("made_up_event").is_err());
        assert!(parse_research_event_type("made_up_test").is_err());
        assert!(parse_research_outcome_field("not_a_field").is_err());
        assert!(parse_distribution_metric("not_a_metric").is_err());
        assert!(parse_signal_outcome_session_field("not_a_field").is_err());
        assert!(parse_dom_behavior_name("not_a_behavior").is_err());
        assert!(parse_research_min_count(Some(-1)).is_err());
        assert!(parse_research_min_count(Some(0)).is_err());
        assert!(parse_nonnegative_i64("minResolved", Some(-1), 0, MAX_MIN_RESOLVED).is_err());
        assert!(parse_bounded_limit("limit", Some(0), 20, MAX_RESEARCH_RESULT_LIMIT).is_err());
        assert!(parse_dom_behavior_min_duration(Some(f64::INFINITY)).is_err());
        assert!(parse_dom_behavior_min_duration(Some(-1.0)).is_err());
    }

    #[test]
    fn research_json_payloads_expose_metadata_contract() {
        let db = Database::open(":memory:").expect("db");
        let mut summary = summary_row("2026-03-05", "RTH", 21_010.0, 20_990.0, 21_000.0);
        summary.ib_range = 20.0;
        db.upsert_session_summary(&summary).expect("summary");

        let payload = serde_json::to_value(
            research::metric_distribution(&db, "ib_range", None, None, None).expect("metric"),
        )
        .expect("json");
        assert_eq!(
            payload
                .pointer("/meta/percentileMethod")
                .and_then(|v| v.as_str()),
            Some(research::RESEARCH_PERCENTILE_METHOD)
        );
        assert_eq!(
            payload
                .pointer("/meta/effectiveSampleSize")
                .and_then(|v| v.as_u64()),
            Some(1)
        );
    }

    #[test]
    fn validate_ymd_range_rejects_invalid_and_reversed_dates() {
        assert!(validate_ymd_range(
            "startDate",
            Some("2026-03-04"),
            "endDate",
            Some("2026-03-05")
        )
        .is_ok());
        assert!(validate_ymd_range(
            "startDate",
            Some("2026-03-05"),
            "endDate",
            Some("2026-03-04")
        )
        .is_err());
        assert!(validate_ymd_range(
            "startDate",
            Some("03-05-2026"),
            "endDate",
            Some("2026-03-06")
        )
        .is_err());
    }

    #[test]
    fn normalize_signal_source_validates_values() {
        assert_eq!(normalize_signal_source("live"), Some("live"));
        assert_eq!(normalize_signal_source("backtest"), Some("backtest"));
        assert_eq!(normalize_signal_source("paper"), None);
    }

    #[test]
    fn normalize_db_absorption_event_matches_live_shape() {
        let row = serde_json::json!({
            "timestampMs": 1234.0,
            "eventType": "absorption_confirmed",
            "price": 21000.0,
            "direction": "down",
            "metadata": {
                "eventSubtype": "absorption",
                "status": "confirmed",
                "severity": 3.5,
                "zoneLow": 20999.5,
                "zoneHigh": 21000.5,
                "keyLevel": "PriorDayHigh",
                "confirmationDeadlineMs": 1500.0,
                "confirmedAtMs": 1400.0,
                "invalidatedAtMs": null,
                "invalidationReason": null,
                "pacePercentile": 0.8,
                "rvolRatio": 1.1,
                "localVolatilityTicks": 4.0,
                "regimePhase": "open"
            }
        });

        let normalized = normalize_db_absorption_event(&row);
        assert_eq!(normalized["eventType"], "absorption");
        assert_eq!(normalized["status"], "confirmed");
        assert_eq!(normalized["zoneLow"], 20999.5);
        assert_eq!(normalized["pacePercentile"], 0.8);
        assert!(normalized.get("metadata").is_none());
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
                include_aggregate: Some(true),
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

    /// Regression for Comment 1: exercise the actual validation and live-snapshot helper paths in
    /// opposing phase order. If either path starts nesting `db` and `pipelines` again, this test
    /// becomes a deadlock candidate instead of a clean join.
    #[test]
    fn validation_and_live_snapshot_helpers_join_under_opposing_phase_order() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let server = test_server();
        *server.last_bid.lock().expect("bid lock") = 21_000.0;
        *server.last_ask.lock().expect("ask lock") = 21_000.25;

        let barrier = Arc::new(Barrier::new(2));

        let validation_server = server.clone();
        let validation_barrier = Arc::clone(&barrier);
        let validation = thread::spawn(move || {
            for _ in 0..200 {
                let _ = collect_validation_db_snapshot(&validation_server.db).expect("db snapshot");
                validation_barrier.wait();
                let _ = collect_pipeline_invariants(&validation_server.pipelines)
                    .expect("pipeline invariants");
            }
        });

        let snapshot_server = server.clone();
        let snapshot_barrier = Arc::clone(&barrier);
        let snapshot = thread::spawn(move || {
            for idx in 0..200 {
                let (timestamp_ms, payload) = build_live_feature_state_snapshot_payload(
                    &snapshot_server.pipelines,
                    &snapshot_server.last_bid,
                    &snapshot_server.last_ask,
                    1_000.0 + idx as f64,
                )
                .expect("live snapshot payload");
                snapshot_barrier.wait();
                persist_feature_state_payload(&snapshot_server.db, timestamp_ms, &payload);
            }
        });

        validation.join().expect("validation join");
        snapshot.join().expect("snapshot join");

        let db = server.db.lock().expect("db lock");
        assert!(db
            .latest_feature_state()
            .expect("latest feature state")
            .is_some());
        assert_eq!(db.raw_tick_count().expect("raw tick count"), 0);
    }

    #[test]
    fn startup_cutover_replay_plus_live_resume_applies_ticks_once() {
        let server = test_server();
        let mut file = NamedTempFile::new().expect("temp");
        write_scid_header(&mut file);
        append_scid_sequence(&mut file, 0, &[21000.0, 21000.25, 21000.5]);

        let reader = ScidReader::new(file.path());
        let since = Utc
            .with_ymd_and_hms(2026, 3, 5, 15, 0, 0)
            .single()
            .expect("since timestamp")
            .timestamp_millis() as f64;
        let cutover = reader.current_aligned_end_offset().expect("cutover");

        // Simulate ticks arriving during startup while warm replay is in progress.
        append_scid_sequence(&mut file, 3, &[21000.75, 21001.0]);

        let warm = run_startup_warm_replay(
            &reader,
            &server.pipelines,
            &server.flow_emitter,
            &server.rules,
            &server.playbook_cache,
            &server.db,
            &server.runtime_events,
            &server.feed_runtime,
            since,
            cutover,
            &test_contract_metadata(),
        );
        let live = reader
            .read_bulk_from_offset(warm.cutover_offset)
            .expect("live resume");
        let mut event_buffer = Vec::new();
        for tick in &live.ticks {
            process_tick(
                &server.pipelines,
                &server.detector,
                &server.flow_emitter,
                &server.rules,
                &server.playbook_cache,
                &server.db,
                &server.runtime_events,
                &server.last_bid,
                &server.last_ask,
                tick.price,
                tick.volume,
                matches!(tick.side, TradeSide::Buy),
                tick.timestamp_ms,
                tick.bid,
                tick.ask,
                &mut event_buffer,
            );
        }

        let (bid, ask) = current_best_bid_ask(&server.last_bid, &server.last_ask);
        let snapshot = server
            .pipelines
            .lock()
            .expect("pipelines lock")
            .snapshot(bid, ask);

        assert_eq!(warm.cutover_offset, cutover);
        assert_eq!(warm.applied_tick_count, 3);
        assert_eq!(live.ticks.len(), 2);
        assert_eq!(snapshot.last_price, 21001.0);
        assert!((snapshot.vwap - 21000.5).abs() < 1e-9);
        assert_eq!(snapshot.session_low, 21000.0);
        assert_eq!(snapshot.session_high, 21001.0);
    }

    #[test]
    fn startup_cutover_and_live_resume_preserve_scaled_prices() {
        let server = test_server();
        let mut file = NamedTempFile::new().expect("temp");
        write_scid_header(&mut file);
        append_scid_scaled_sequence(&mut file, 0, &[21000.0, 21000.25, 21000.5], 100.0);

        let reader = ScidReader::with_price_scale(file.path(), 100.0);
        let since = Utc
            .with_ymd_and_hms(2026, 3, 5, 15, 0, 0)
            .single()
            .expect("since timestamp")
            .timestamp_millis() as f64;
        let cutover = reader.current_aligned_end_offset().expect("cutover");

        append_scid_scaled_sequence(&mut file, 3, &[21000.75, 21001.0], 100.0);

        let warm = run_startup_warm_replay(
            &reader,
            &server.pipelines,
            &server.flow_emitter,
            &server.rules,
            &server.playbook_cache,
            &server.db,
            &server.runtime_events,
            &server.feed_runtime,
            since,
            cutover,
            &test_contract_metadata(),
        );
        let live = read_scid_poll_step(&reader, warm.cutover_offset).expect("live step");

        assert_eq!(warm.applied_tick_count, 3);
        assert_eq!(live.ticks.len(), 2);
        assert!((live.ticks[0].price - 21000.75).abs() < 1e-9);
        assert!((live.ticks[1].price - 21001.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn warm_replay_reports_non_monotonic_ticks_in_health_and_integrity() {
        let server = test_server();
        let mut file = NamedTempFile::new().expect("temp");
        write_scid_header(&mut file);
        let base_ts_ms = Utc::now().timestamp_millis() as f64;
        append_scid_record(&mut file, 21000.0, base_ts_ms);
        append_scid_record(&mut file, 21000.25, base_ts_ms);
        append_scid_record(&mut file, 21000.5, base_ts_ms - 1.0);
        append_scid_record(&mut file, 21000.75, base_ts_ms + 2.0);
        file.flush().expect("flush");

        let reader = ScidReader::new(file.path());
        let warm = run_startup_warm_replay(
            &reader,
            &server.pipelines,
            &server.flow_emitter,
            &server.rules,
            &server.playbook_cache,
            &server.db,
            &server.runtime_events,
            &server.feed_runtime,
            base_ts_ms - 10.0,
            reader.current_aligned_end_offset().expect("cutover"),
            &test_contract_metadata(),
        );

        assert_eq!(warm.applied_tick_count, 2);

        let health = parse_text_tool_result(server.get_feed_health().await.expect("feed health"));
        assert_eq!(health["skippedNonMonotonicTicks"].as_u64(), Some(2));
        assert_eq!(health["duplicateTimestampTicks"].as_u64(), Some(1));
        assert_eq!(health["backwardTimestampTicks"].as_u64(), Some(1));
        assert_eq!(
            health["lastNonMonotonicTimestampMs"].as_f64(),
            Some(base_ts_ms - 1.0)
        );

        let integrity = parse_text_tool_result(
            server
                .validate_data_integrity()
                .await
                .expect("validate integrity"),
        );
        assert_eq!(integrity["skippedNonMonotonicTicks"].as_u64(), Some(2));
        assert_eq!(integrity["duplicateTimestampTicks"].as_u64(), Some(1));
        assert_eq!(integrity["backwardTimestampTicks"].as_u64(), Some(1));
        assert_eq!(
            integrity["checks"]["monotonicTimestamps"]["passed"].as_bool(),
            Some(false)
        );
    }

    #[test]
    fn depth_shrink_recovery_preserves_previous_book_when_fragment_has_no_clear() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("NQ.depth");
        write_test_depth_file(
            &path,
            &[
                (unix_ms_to_sc_depth(1_000), 1, 0, 0, 0.0, 0),
                (unix_ms_to_sc_depth(1_000), 2, 0, 1, 100.0, 10),
                (unix_ms_to_sc_depth(1_000), 2, 0, 1, 99.75, 5),
                (unix_ms_to_sc_depth(1_000), 3, 0, 1, 100.25, 7),
            ],
        );

        let reader = DepthReader::new(&path, 1.0);
        let mut state = DepthPollWorkerState {
            active_path: Some(path.clone()),
            offset: reader.current_aligned_end_offset().expect("aligned end"),
            batch_id: 12,
            book: DepthBook::default(),
        };
        for record in reader.read_bulk().expect("read bulk") {
            state.book.apply(&record);
        }

        write_test_depth_file(&path, &[(unix_ms_to_sc_depth(2_000), 4, 0, 1, 100.0, 8)]);

        let work = recover_depth_state_after_shrink(&reader, &mut state)
            .expect("recover")
            .expect("work");

        let snapshot = work.snapshot;
        assert!(work.records.is_empty());
        assert_eq!(
            state.offset,
            reader.current_aligned_end_offset().expect("aligned end")
        );
        assert_eq!(snapshot.best_bid, Some(100.0));
        assert_eq!(snapshot.best_ask, Some(100.25));
        assert_eq!(
            snapshot
                .bids
                .iter()
                .find(|level| (level.price - 100.0).abs() < 1e-9)
                .map(|level| level.quantity),
            Some(8)
        );
        assert_eq!(
            snapshot
                .bids
                .iter()
                .find(|level| (level.price - 99.75).abs() < 1e-9)
                .map(|level| level.quantity),
            Some(5)
        );
    }

    #[test]
    fn playbook_cache_hydration_loads_active_setups_and_risk_gate() {
        let db = Database::open(":memory:").expect("db");
        db.upsert_setup(&SetupDefinition {
            id: "active_seed".to_string(),
            name: "Active Seed".to_string(),
            active: true,
            ..Default::default()
        })
        .expect("insert active");
        db.upsert_setup(&SetupDefinition {
            id: "inactive_seed".to_string(),
            name: "Inactive Seed".to_string(),
            active: false,
            ..Default::default()
        })
        .expect("insert inactive");
        db.save_risk_state(&RiskState {
            at_limit: true,
            ..Default::default()
        })
        .expect("save risk state");

        let server = TheDeskMcp::new(db, PipelineEngine::new(), ":memory:".into());
        server
            .hydrate_playbook_runtime_cache()
            .expect("hydrate playbook cache");
        let (setups, risk_at_limit) = server.playbook_cache.snapshot();

        assert_eq!(setups.len(), 1);
        assert_eq!(setups[0].id, "active_seed");
        assert!(risk_at_limit);
    }

    #[test]
    fn playbook_cache_hydration_rehydrates_setup_runtime_state() {
        let db = Database::open(":memory:").expect("db");
        db.upsert_setup(&SetupDefinition {
            id: "rehydrated_setup".to_string(),
            name: "Rehydrated Setup".to_string(),
            active: true,
            ..Default::default()
        })
        .expect("insert setup");
        db.upsert_setup_runtime_state(&SetupRuntimeStateRecord {
            session_date: the_desk_backend::et_now_trading_day(),
            root_symbol: Some("NQ".to_string()),
            contract_symbol: Some("NQH26.CME".to_string()),
            setup_id: "rehydrated_setup".to_string(),
            setup_name: Some("Rehydrated Setup".to_string()),
            state: SetupState::Approaching,
            readiness: SetupReadiness::DeterministicReady,
            readiness_score: 1.0,
            met_count: 1,
            total_count: 1,
            met_conditions: vec!["min_delta".to_string()],
            missing_conditions: Vec::new(),
            deterministic_all_met: true,
            requires_discretionary: true,
            current_price: 21010.0,
            last_evaluated_at_ms: 1_000.0,
            last_transition_at_ms: 1_000.0,
            last_alert_emitted_at_ms: Some(1_000.0),
            source: "live".to_string(),
            updated_at_ms: 1_000.0,
        })
        .expect("seed runtime");

        let server = TheDeskMcp::new(db, PipelineEngine::new(), ":memory:".into());
        server
            .hydrate_playbook_runtime_cache()
            .expect("hydrate playbook cache");
        let snapshot = server
            .rules
            .lock()
            .expect("rules lock")
            .runtime_snapshot("rehydrated_setup")
            .expect("runtime snapshot");

        assert_eq!(snapshot.readiness, SetupReadiness::DeterministicReady);
        assert!(server
            .feed_runtime
            .setup_runtime_rehydrated
            .load(Ordering::Acquire));
    }

    #[test]
    fn process_tick_uses_cached_risk_gate_for_alert_suppression() {
        let server = test_server();
        server
            .playbook_cache
            .replace_active_setups(vec![SetupDefinition {
                id: "risk_gated_setup".to_string(),
                name: "Risk Gated Setup".to_string(),
                active: true,
                min_delta: 0.0,
                conditions: Vec::new(),
                ..Default::default()
            }]);
        server.playbook_cache.set_risk_at_limit(true);

        let mut event_buffer = Vec::new();
        process_tick(
            &server.pipelines,
            &server.detector,
            &server.flow_emitter,
            &server.rules,
            &server.playbook_cache,
            &server.db,
            &server.runtime_events,
            &server.last_bid,
            &server.last_ask,
            21_000.0,
            1.0,
            true,
            Utc::now().timestamp_millis() as f64,
            20_999.75,
            21_000.25,
            &mut event_buffer,
        );

        let db = server.db.lock().expect("db lock");
        assert_eq!(db.count_playbook_signals().expect("signal count"), 0);
        drop(db);
        let state = server
            .rules
            .lock()
            .expect("rules lock")
            .get_state("risk_gated_setup");
        assert_eq!(format!("{state:?}"), "NotActive");
    }

    #[test]
    fn process_tick_persists_setup_runtime_and_history() {
        let server = test_server();
        server
            .playbook_cache
            .replace_active_setups(vec![SetupDefinition {
                id: "persisted_setup".to_string(),
                name: "Persisted Setup".to_string(),
                active: true,
                min_delta: 0.0,
                conditions: Vec::new(),
                ..Default::default()
            }]);
        server.playbook_cache.set_risk_at_limit(false);
        let ts = Utc
            .with_ymd_and_hms(2026, 3, 5, 15, 0, 0)
            .single()
            .expect("timestamp")
            .timestamp_millis() as f64;

        let mut event_buffer = Vec::new();
        process_tick(
            &server.pipelines,
            &server.detector,
            &server.flow_emitter,
            &server.rules,
            &server.playbook_cache,
            &server.db,
            &server.runtime_events,
            &server.last_bid,
            &server.last_ask,
            21_000.0,
            1.0,
            true,
            ts,
            20_999.75,
            21_000.25,
            &mut event_buffer,
        );

        let db = server.db.lock().expect("db lock");
        let rows = db
            .load_setup_runtime_state_for_session(&session_date_from_timestamp_ms(ts))
            .expect("runtime rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].setup_id, "persisted_setup");
        assert_eq!(rows[0].last_evaluated_at_ms, ts);
        let history = db
            .query_setup_state_history(
                Some("persisted_setup"),
                Some(&session_date_from_timestamp_ms(ts)),
                None,
                10,
            )
            .expect("history");
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].timestamp_ms, ts);
        let outcomes = db.pending_signal_outcomes().expect("pending outcomes");
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].fired_at_ms, ts);
        assert_eq!(db.count_playbook_signals().expect("signals"), 1);
    }

    #[test]
    fn setup_lifecycle_uses_trading_day_across_globex_manual_and_live_paths() {
        use chrono::NaiveDate;
        use chrono_tz::US::Eastern;

        let server = test_server();
        server
            .playbook_cache
            .replace_active_setups(vec![SetupDefinition {
                id: "globex_setup".to_string(),
                name: "Globex Setup".to_string(),
                active: true,
                min_delta: 0.0,
                conditions: Vec::new(),
                ..Default::default()
            }]);
        server.playbook_cache.set_risk_at_limit(false);
        let globex_ts = Eastern
            .from_local_datetime(
                &NaiveDate::from_ymd_opt(2026, 3, 5)
                    .expect("date")
                    .and_hms_opt(18, 30, 0)
                    .expect("time"),
            )
            .single()
            .expect("non-ambiguous ET timestamp")
            .timestamp_millis() as f64;
        assert_ne!(
            session_date_from_timestamp_ms(globex_ts),
            trading_day_from_timestamp_ms(globex_ts)
        );

        let mut event_buffer = Vec::new();
        process_tick(
            &server.pipelines,
            &server.detector,
            &server.flow_emitter,
            &server.rules,
            &server.playbook_cache,
            &server.db,
            &server.runtime_events,
            &server.last_bid,
            &server.last_ask,
            21_000.0,
            1.0,
            true,
            globex_ts,
            20_999.75,
            21_000.25,
            &mut event_buffer,
        );

        let manual_ts = globex_ts + 60_000.0;
        let (before, after) = {
            let mut rules = server.rules.lock().expect("rules lock");
            let before = rules.runtime_snapshot("globex_setup");
            rules
                .acknowledge_prompt_at("globex_setup", manual_ts)
                .expect("acknowledge setup");
            let after = rules
                .runtime_snapshot("globex_setup")
                .expect("runtime snapshot");
            (before, after)
        };
        server
            .persist_manual_setup_state_change(
                "globex_setup",
                before,
                after,
                "manualConfirmed",
                manual_ts,
            )
            .expect("persist manual state");

        let db = server.db.lock().expect("db lock");
        let trading_day_rows = db
            .load_setup_runtime_state_for_session(&trading_day_from_timestamp_ms(globex_ts))
            .expect("trading-day runtime rows");
        assert_eq!(trading_day_rows.len(), 1);
        assert_eq!(trading_day_rows[0].setup_id, "globex_setup");
        assert_eq!(trading_day_rows[0].state, SetupState::Confirmed);

        let calendar_rows = db
            .load_setup_runtime_state_for_session(&session_date_from_timestamp_ms(globex_ts))
            .expect("calendar-date runtime rows");
        assert!(calendar_rows.is_empty());
    }

    #[test]
    fn process_tick_skips_runtime_write_when_progress_is_unchanged() {
        let server = test_server();
        server
            .playbook_cache
            .replace_active_setups(vec![SetupDefinition {
                id: "coalesced_setup".to_string(),
                name: "Coalesced Setup".to_string(),
                active: true,
                min_delta: 0.0,
                conditions: Vec::new(),
                ..Default::default()
            }]);
        server.playbook_cache.set_risk_at_limit(false);
        let ts = Utc
            .with_ymd_and_hms(2026, 3, 5, 15, 0, 0)
            .single()
            .expect("timestamp")
            .timestamp_millis() as f64;
        let mut event_buffer = Vec::new();

        process_tick(
            &server.pipelines,
            &server.detector,
            &server.flow_emitter,
            &server.rules,
            &server.playbook_cache,
            &server.db,
            &server.runtime_events,
            &server.last_bid,
            &server.last_ask,
            21_000.0,
            1.0,
            true,
            ts,
            20_999.75,
            21_000.25,
            &mut event_buffer,
        );
        process_tick(
            &server.pipelines,
            &server.detector,
            &server.flow_emitter,
            &server.rules,
            &server.playbook_cache,
            &server.db,
            &server.runtime_events,
            &server.last_bid,
            &server.last_ask,
            21_000.25,
            1.0,
            true,
            ts + 1_000.0,
            21_000.0,
            21_000.5,
            &mut event_buffer,
        );

        let db = server.db.lock().expect("db lock");
        let rows = db
            .load_setup_runtime_state_for_session(&session_date_from_timestamp_ms(ts))
            .expect("runtime rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].last_evaluated_at_ms, ts);
        let history = db
            .query_setup_state_history(
                Some("coalesced_setup"),
                Some(&session_date_from_timestamp_ms(ts)),
                None,
                10,
            )
            .expect("history");
        assert_eq!(history.len(), 1);
        assert_eq!(db.count_playbook_signals().expect("signals"), 1);
    }

    #[test]
    fn startup_warm_replay_persists_setup_runtime_without_live_signals() {
        let server = test_server();
        server
            .playbook_cache
            .replace_active_setups(vec![SetupDefinition {
                id: "replay_setup".to_string(),
                name: "Replay Setup".to_string(),
                active: true,
                min_delta: 0.0,
                conditions: Vec::new(),
                ..Default::default()
            }]);
        server.playbook_cache.set_risk_at_limit(false);
        let mut file = NamedTempFile::new().expect("temp");
        write_scid_header(&mut file);
        append_scid_sequence(&mut file, 0, &[21000.0, 21000.25]);
        let reader = ScidReader::new(file.path());
        let since = Utc
            .with_ymd_and_hms(2026, 3, 5, 14, 59, 0)
            .single()
            .expect("since timestamp")
            .timestamp_millis() as f64;
        let cutover = reader.current_aligned_end_offset().expect("cutover");

        let warm = run_startup_warm_replay(
            &reader,
            &server.pipelines,
            &server.flow_emitter,
            &server.rules,
            &server.playbook_cache,
            &server.db,
            &server.runtime_events,
            &server.feed_runtime,
            since,
            cutover,
            &test_contract_metadata(),
        );

        assert_eq!(warm.applied_tick_count, 2);
        let trading_day = trading_day_from_timestamp_ms(
            Utc.with_ymd_and_hms(2026, 3, 5, 15, 0, 0)
                .single()
                .expect("base timestamp")
                .timestamp_millis() as f64,
        );
        let db = server.db.lock().expect("db lock");
        let rows = db
            .load_setup_runtime_state_for_session(&trading_day)
            .expect("runtime rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].setup_id, "replay_setup");
        let history = db
            .query_setup_state_history(Some("replay_setup"), Some(&trading_day), None, 10)
            .expect("history");
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].source, "startup_replay");
        assert_eq!(db.count_playbook_signals().expect("signals"), 0);
        assert!(db
            .pending_signal_outcomes()
            .expect("pending outcomes")
            .is_empty());
    }

    #[tokio::test]
    async fn evaluate_playbook_reads_cache_snapshot() {
        let server = test_server();
        server
            .playbook_cache
            .replace_active_setups(vec![SetupDefinition {
                id: "cache_only_setup".to_string(),
                name: "Cache Only Setup".to_string(),
                active: true,
                min_delta: 0.0,
                conditions: Vec::new(),
                ..Default::default()
            }]);
        server.playbook_cache.set_risk_at_limit(false);
        *server.last_bid.lock().expect("bid lock") = 21_000.0;
        *server.last_ask.lock().expect("ask lock") = 21_000.25;

        let result = server.evaluate_playbook().await.expect("evaluate");
        let rendered = format!("{result:?}");
        assert!(rendered.contains("cache_only_setup"));
        assert_eq!(
            server
                .rules
                .lock()
                .expect("rules lock")
                .get_state("cache_only_setup"),
            SetupState::NotActive
        );

        let ts = Utc
            .with_ymd_and_hms(2026, 3, 5, 15, 0, 0)
            .single()
            .expect("timestamp")
            .timestamp_millis() as f64;
        let mut event_buffer = Vec::new();
        process_tick(
            &server.pipelines,
            &server.detector,
            &server.flow_emitter,
            &server.rules,
            &server.playbook_cache,
            &server.db,
            &server.runtime_events,
            &server.last_bid,
            &server.last_ask,
            21_000.0,
            1.0,
            true,
            ts,
            20_999.75,
            21_000.25,
            &mut event_buffer,
        );
        assert_eq!(
            server
                .db
                .lock()
                .expect("db lock")
                .count_playbook_signals()
                .expect("signals"),
            1
        );
    }

    #[tokio::test]
    async fn manual_setup_lifecycle_persists_runtime_transition_timestamp() {
        let server = test_server();
        server
            .playbook_cache
            .replace_active_setups(vec![SetupDefinition {
                id: "manual_setup".to_string(),
                name: "Manual Setup".to_string(),
                active: true,
                min_delta: 0.0,
                conditions: Vec::new(),
                ..Default::default()
            }]);
        server.playbook_cache.set_risk_at_limit(false);
        let ts = Utc
            .with_ymd_and_hms(2026, 3, 5, 15, 0, 0)
            .single()
            .expect("timestamp")
            .timestamp_millis() as f64;
        let mut event_buffer = Vec::new();
        process_tick(
            &server.pipelines,
            &server.detector,
            &server.flow_emitter,
            &server.rules,
            &server.playbook_cache,
            &server.db,
            &server.runtime_events,
            &server.last_bid,
            &server.last_ask,
            21_000.0,
            1.0,
            true,
            ts,
            20_999.75,
            21_000.25,
            &mut event_buffer,
        );

        server
            .acknowledge_setup_prompt(Parameters(SetupLifecycleParams {
                setup_id: "manual_setup".to_string(),
            }))
            .await
            .expect("acknowledge setup");

        let db = server.db.lock().expect("db lock");
        let latest_history = db
            .query_setup_state_history(Some("manual_setup"), None, None, 1)
            .expect("history")
            .pop()
            .expect("manual history row");
        assert_eq!(latest_history.reason, "manualConfirmed");

        let rows = db
            .load_setup_runtime_state_for_session(&the_desk_backend::et_now_trading_day())
            .expect("runtime rows");
        let manual_row = rows
            .iter()
            .find(|row| row.setup_id == "manual_setup")
            .expect("manual runtime row");
        assert_eq!(
            manual_row.last_transition_at_ms,
            latest_history.timestamp_ms
        );
    }

    #[tokio::test]
    async fn risk_state_mutation_tools_sync_playbook_cache() {
        let server = test_server();
        {
            let db = server.db.lock().expect("db lock");
            db.save_risk_config(&RiskConfigRecord {
                max_daily_loss_r: 1.0,
                ..Default::default()
            })
            .expect("save risk config");
        }

        server.playbook_cache.set_risk_at_limit(true);
        server.init_risk_state().await.expect("init risk");
        assert!(!server.playbook_cache.snapshot().1);
        {
            let db = server.db.lock().expect("db lock");
            assert!(
                !db.load_risk_state()
                    .expect("load risk")
                    .expect("risk state")
                    .at_limit
            );
        }

        server
            .record_trade_result(Parameters(RecordTradeResultParams {
                direction: "long".to_string(),
                size: 1,
                entry_price: 21_000.0,
                exit_price: 20_990.0,
                result_r: -2.0,
                setup_id: None,
                stop_price: None,
                notes: None,
            }))
            .await
            .expect("record trade");
        assert!(server.playbook_cache.snapshot().1);
        {
            let db = server.db.lock().expect("db lock");
            assert!(
                db.load_risk_state()
                    .expect("load risk")
                    .expect("risk state")
                    .at_limit
            );
        }

        let trade_id = "risk_sync_trade".to_string();
        server
            .upsert_trade_entry(Parameters(UpsertTradeEntryParams {
                id: Some(trade_id.clone()),
                direction: "long".to_string(),
                size: 1,
                entry_price: 21_005.0,
                ..Default::default()
            }))
            .await
            .expect("upsert trade");
        server
            .close_trade_entry(Parameters(CloseTradeEntryParams {
                id: trade_id,
                exit_price: 21_015.0,
                exit_time_ms: None,
                result_r: Some(5.0),
                gross_points: Some(10.0),
                notes: None,
                update_risk_state: Some(true),
            }))
            .await
            .expect("close trade");

        assert!(!server.playbook_cache.snapshot().1);
        let db = server.db.lock().expect("db lock");
        assert!(
            !db.load_risk_state()
                .expect("load risk")
                .expect("risk state")
                .at_limit
        );
    }

    /// Build an epoch-ms timestamp for an RTH wall-clock time on a fixed test
    /// date (2026-03-05, a Thursday in DST). Used by the boundary-recovery
    /// tests to drive `finalize_rth_close` deterministically.
    fn rth_ts(hour: u32, minute: u32, second: u32) -> f64 {
        use chrono::NaiveDate;
        use chrono_tz::US::Eastern;
        let naive = NaiveDate::from_ymd_opt(2026, 3, 5)
            .expect("date")
            .and_hms_opt(hour, minute, second)
            .expect("time");
        Eastern
            .from_local_datetime(&naive)
            .single()
            .expect("non-ambiguous ET timestamp")
            .timestamp_millis() as f64
    }

    /// Drive a few RTH ticks through the pipeline so finalize_rth_close has
    /// real session state to snapshot. Mirrors the live ingest call shape but
    /// skips the rules engine to keep tests focused on boundary persistence.
    fn warm_rth_session(server: &TheDeskMcp, prices: &[f64]) {
        let mut p = server.pipelines.lock().expect("pipelines");
        for (i, price) in prices.iter().enumerate() {
            let ts = rth_ts(15, 30, i as u32);
            let minute = minute_of_session_from_timestamp(ts);
            p.on_trade_with_timestamp(*price, 1.0, i % 2 == 0, minute, ts);
        }
    }

    /// Boundary recovery: a single live RTH→Unknown transition must persist
    /// `session_summaries` and `prior_day_levels` in one transaction, refresh
    /// in-memory carry-forward, and leave `session_inventory` aware of the
    /// just-closed session before any further DB read happens.
    #[test]
    fn finalize_rth_close_persists_summary_and_carry_forward_atomically() {
        let server = test_server();
        warm_rth_session(&server, &[21_000.0, 21_005.0, 21_010.0, 21_015.0, 21_012.0]);

        let boundary_ts = rth_ts(16, 0, 1);
        let result = finalize_rth_close(
            &server.pipelines,
            &server.db,
            None,
            None,
            None,
            boundary_ts,
            21_011.75,
            21_012.25,
            &test_contract_metadata(),
        )
        .expect("close finalize")
        .expect("close result");

        assert_eq!(result.session_date, "2026-03-05");
        assert!((result.high - 21_015.0).abs() < 1e-6);
        assert!((result.low - 21_000.0).abs() < 1e-6);

        let db = server.db.lock().expect("db");
        assert!(db
            .has_session_summary_for("2026-03-05", "RTH")
            .expect("summary lookup"));
        let prior = db
            .load_prior_day_full("2026-03-06")
            .expect("prior load")
            .expect("prior row exists");
        assert!((prior.0 - 21_015.0).abs() < 1e-6);
        assert!((prior.1 - 21_000.0).abs() < 1e-6);
        drop(db);

        // In-memory carry-forward should match the just-built end-state without
        // any extra DB reload.
        let p = server.pipelines.lock().expect("pipelines");
        assert!((p.levels.prior_day_high - 21_015.0).abs() < 1e-6);
        assert!((p.levels.prior_day_low - 21_000.0).abs() < 1e-6);
        assert!(!p.levels.rth_started());
    }

    /// Restart idempotency: calling `finalize_rth_close` again after the
    /// session has been reset must be a no-op (returns None) and must not
    /// clobber the persisted summary or write a duplicate row.
    #[test]
    fn finalize_rth_close_is_idempotent_on_replay() {
        let server = test_server();
        warm_rth_session(&server, &[21_000.0, 21_005.0, 21_010.0]);

        let boundary_ts = rth_ts(16, 0, 1);
        let _ = finalize_rth_close(
            &server.pipelines,
            &server.db,
            None,
            None,
            None,
            boundary_ts,
            21_009.75,
            21_010.25,
            &test_contract_metadata(),
        )
        .expect("first close");

        let summary_v1 = {
            let db = server.db.lock().expect("db");
            db.list_session_summaries(None, None, None, Some("RTH"), 5)
                .expect("list")
        };
        assert_eq!(summary_v1.len(), 1);

        // Second call: pipeline has been reset, so finalize_rth_close should
        // return None rather than re-persisting an empty snapshot.
        let second = finalize_rth_close(
            &server.pipelines,
            &server.db,
            None,
            None,
            None,
            boundary_ts,
            21_009.75,
            21_010.25,
            &test_contract_metadata(),
        )
        .expect("second finalize");
        assert!(second.is_none());

        let summary_v2 = {
            let db = server.db.lock().expect("db");
            db.list_session_summaries(None, None, None, Some("RTH"), 5)
                .expect("list")
        };
        assert_eq!(summary_v2.len(), 1);
        assert_eq!(summary_v1[0].session_date, summary_v2[0].session_date);
        assert!((summary_v1[0].high - summary_v2[0].high).abs() < 1e-9);
    }

    /// Cross-session inventory must see the just-closed RTH session via the
    /// in-memory `prior_sessions()` list immediately after `finalize_rth_close`,
    /// without waiting for a same-turn DB reload (which can race with the
    /// `date < ?1` semantics in `load_prior_day_full`).
    #[test]
    fn finalize_rth_close_makes_session_inventory_visible_in_memory() {
        let server = test_server();
        warm_rth_session(&server, &[21_000.0, 21_010.0, 21_005.0, 21_015.0]);

        // Before close: session_inventory has no prior sessions.
        {
            let p = server.pipelines.lock().expect("pipelines");
            assert!(p.session_inventory.prior_sessions().is_empty());
        }

        let _ = finalize_rth_close(
            &server.pipelines,
            &server.db,
            None,
            None,
            None,
            rth_ts(16, 0, 1),
            21_014.75,
            21_015.25,
            &test_contract_metadata(),
        )
        .expect("close finalize")
        .expect("close result");

        let p = server.pipelines.lock().expect("pipelines");
        let inv = p.session_inventory.prior_sessions();
        assert_eq!(
            inv.len(),
            1,
            "session_inventory should expose the just-closed RTH session"
        );
        assert!(
            inv[0].dnp > 0.0,
            "just-closed entry must carry a usable DNP"
        );
    }

    /// `persist_live_session_close` must commit `session_summaries` and
    /// `prior_day_levels` in one transaction. This direct DB-level test
    /// guards against the row-by-row regression where a crash between writes
    /// would leave the next session reading half-updated levels.
    #[test]
    fn persist_live_session_close_writes_summary_and_prior_day_together() {
        let db = Database::open(":memory:").expect("db");
        let summary = summary_row("2026-03-05", "RTH", 21_010.0, 20_990.0, 21_000.0);
        db.persist_live_session_close(
            &summary,
            (
                21_020.0, 20_980.0, 21_000.0, 21_015.0, 20_995.0, 21_005.0, 21_010.0, 20_990.0,
                21_000.0,
            ),
        )
        .expect("atomic close");

        assert!(db
            .has_session_summary_for("2026-03-05", "RTH")
            .expect("summary check"));
        let row = db
            .load_prior_day_full("2026-03-06")
            .expect("prior load")
            .expect("prior row");
        assert!((row.0 - 21_020.0).abs() < 1e-9);
        assert!((row.1 - 20_980.0).abs() < 1e-9);
        assert_eq!(row.6, Some(21_010.0));
    }

    #[test]
    fn prepare_for_new_session_scopes_contract_data_and_restores_inventory_order() {
        let server = test_server();
        {
            let db = server.db.lock().expect("db");
            db.save_prior_day_full_with_dnva_contract(
                "2026-03-04",
                22_000.0,
                21_900.0,
                21_950.0,
                21_980.0,
                21_920.0,
                21_950.0,
                Some(21_970.0),
                Some(21_930.0),
                Some(21_950.0),
                Some("NQ"),
                Some("NQM26"),
            )
            .expect("wrong-contract prior day");
            db.save_prior_day_full_with_dnva_contract(
                "2026-03-03",
                21_100.0,
                20_900.0,
                21_000.0,
                21_050.0,
                20_950.0,
                21_000.0,
                Some(21_025.0),
                Some(20_975.0),
                Some(21_000.0),
                Some("NQ"),
                Some("NQH26"),
            )
            .expect("matching-contract prior day");

            let mut older = summary_row("2026-03-03", "RTH", 21_025.0, 20_975.0, 21_000.0);
            older.contract_symbol = "NQH26".to_string();
            let mut newer = summary_row("2026-03-04", "RTH", 21_075.0, 21_000.0, 21_050.0);
            newer.contract_symbol = "NQH26".to_string();
            let mut wrong_contract = summary_row("2026-03-02", "RTH", 22_075.0, 22_000.0, 22_050.0);
            wrong_contract.contract_symbol = "NQM26".to_string();
            db.upsert_session_summary(&older).expect("older summary");
            db.upsert_session_summary(&newer).expect("newer summary");
            db.upsert_session_summary(&wrong_contract)
                .expect("wrong-contract summary");
        }

        prepare_for_new_session(
            &server.pipelines,
            &server.db,
            None,
            SessionType::Rth,
            DeltaSegment::Rth,
            rth_ts(9, 30, 0),
            &test_contract_metadata(),
        );

        let p = server.pipelines.lock().expect("pipelines");
        assert!((p.levels.prior_day_high - 21_100.0).abs() < 1e-9);
        assert_eq!(p.levels.prior_day_contract_symbol.as_deref(), Some("NQH26"));
        let inv = p.session_inventory.prior_sessions();
        assert_eq!(inv.len(), 2);
        assert!(
            (inv.last().expect("newest prior session").dnp - 21_050.0).abs() < 1e-9,
            "newest same-contract session should be the comparison anchor"
        );
    }
}
