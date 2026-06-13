//! Shared constants, runtime caches, and the `TheDeskMcp` service state.

use rmcp::handler::server::tool::ToolRouter;
use schemars::{json_schema, Schema, SchemaGenerator};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;
use the_desk_backend::db::{Database, HistoricalJobRun};
use the_desk_backend::feed::monotonic::MonotonicTimestampViolationKind;
use the_desk_backend::feed::{ContractMetadata, FeedConfig};
use the_desk_backend::observability::RuntimeEventStore;
use the_desk_backend::options::OptionsSnapshot;
use the_desk_backend::pipelines::{EventDetector, FlowEventEmitter, PipelineEngine};
use the_desk_backend::research;
use the_desk_backend::rules::{RulesEngine, SetupDefinition};
use tokio::sync::Mutex as AsyncMutex;

#[allow(unused_imports)]
use crate::{helpers::*, lifecycle::*, params::*};

pub(crate) const FRESHNESS_THRESHOLD_MS: f64 = 15_000.0;
pub(crate) const JOB_PROGRESS_PERSIST_INTERVAL_MS: f64 = 1_000.0;
pub(crate) const JOB_PROGRESS_RECORD_STEP: usize = 50_000;
pub(crate) const JOB_PROGRESS_RATE_EMA_ALPHA: f64 = 0.25;
pub(crate) const PIPELINE_CONTENTION_RECENT_WINDOW_MS: u64 = 5_000;
pub(crate) const MONOTONIC_ANOMALY_RECENT_WINDOW_MS: f64 = 60_000.0;
pub(crate) const MAX_RESEARCH_RESULT_LIMIT: u64 = 500;
pub(crate) const MAX_RESEARCH_MIN_COUNT: i64 = 10_000;
pub(crate) const MAX_MIN_RESOLVED: i64 = 10_000;
pub(crate) const MAX_DOM_BEHAVIOR_MIN_DURATION_MS: f64 = 86_400_000.0;
pub(crate) const CONTRACT_RESOLUTION_CACHE_TTL_MS: u128 = 2_000;
pub(crate) const CONTEXT_FRAME_CACHE_LIMIT: usize = 128;
pub(crate) const LIVE_CONTEXT_FRAME_SNAPSHOT_INTERVAL_MS: f64 = 60_000.0;
pub(crate) static LAST_LIVE_CONTEXT_SNAPSHOT_MS_BITS: AtomicU64 = AtomicU64::new(0);
pub(crate) const RESEARCH_EVENT_TYPES: &[&str] = &[
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
pub(crate) const RESEARCH_LEVEL_TEST_NAMES: &[&str] = &[
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
pub(crate) const RESEARCH_OUTCOME_FIELDS: &[&str] = &[
    "close_vs_ib_mid",
    "close_vs_vwap",
    "close_vs_poc",
    "day_type",
    "profile_shape",
    "balance_state",
    "single_prints_direction",
    "ib_extension_state",
    "first_ib_extension_direction",
    "poor_high",
    "poor_low",
    "excess_high",
    "excess_low",
];
pub(crate) const SIGNAL_OUTCOME_SESSION_FIELDS: &[&str] = &[
    "day_type",
    "profile_shape",
    "balance_state",
    "close_vs_ib_mid",
    "close_vs_vwap",
    "single_prints_direction",
    "ib_extension_state",
    "first_ib_extension_direction",
];
pub(crate) const DOM_BEHAVIOR_NAMES: &[&str] = &[
    "bid_support_persisted",
    "ask_resistance_persisted",
    "liquidity_flip",
    "pulling_acceleration",
    "stacking_acceleration",
];
pub(crate) type DnvaTriple = (f64, f64, f64);

/// MCP clients (e.g. Cursor) may reject `tools/list` when `serde_json::Value` becomes JSON Schema
/// boolean `true`. Use explicit object schemas instead.
pub(crate) fn schemars_loose_object(_: &mut SchemaGenerator) -> Schema {
    json_schema!({
        "type": "object",
        "additionalProperties": true
    })
}

pub(crate) fn schemars_optional_loose_object(_: &mut SchemaGenerator) -> Schema {
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
pub(crate) struct MonotonicRuntimeSnapshot {
    pub(crate) skipped_non_monotonic_ticks: u64,
    pub(crate) duplicate_timestamp_ticks: u64,
    pub(crate) backward_timestamp_ticks: u64,
    pub(crate) last_non_monotonic_timestamp_ms: Option<f64>,
}

impl MonotonicRuntimeSnapshot {
    pub(crate) fn has_recent_violation(&self, now_ms: f64) -> bool {
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
    pub(crate) fn record_pipeline_lock_sample(&self, contended: bool, observed_at_ms: u64) {
        self.pipeline_lock_contended_now
            .store(contended, Ordering::Release);
        if contended {
            self.pipeline_last_contended_wall_ms
                .store(observed_at_ms, Ordering::Release);
        }
    }

    pub(crate) fn pipeline_lock_recently_contended(&self, now_ms: u64) -> bool {
        if self.pipeline_lock_contended_now.load(Ordering::Acquire) {
            return true;
        }
        let last_contended = self.pipeline_last_contended_wall_ms.load(Ordering::Acquire);
        last_contended > 0
            && now_ms.saturating_sub(last_contended) <= PIPELINE_CONTENTION_RECENT_WINDOW_MS
    }

    pub(crate) fn record_non_monotonic_tick(
        &self,
        kind: MonotonicTimestampViolationKind,
        timestamp_ms: f64,
    ) {
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

    pub(crate) fn monotonicity_snapshot(&self) -> MonotonicRuntimeSnapshot {
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

pub(crate) fn tick_ms_to_bits(ts: f64) -> u64 {
    if ts.is_finite() && ts > 0.0 {
        ts.to_bits()
    } else {
        0
    }
}

pub(crate) fn tick_ms_from_bits(bits: u64) -> Option<f64> {
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
pub(crate) struct LiveMarketResolution {
    pub(crate) snapshot: serde_json::Value,
    pub(crate) snapshot_source: &'static str,
    pub(crate) dom_summary: Option<serde_json::Value>,
    pub(crate) dom_source: &'static str,
    pub(crate) as_of_timestamp_ms: f64,
    pub(crate) pipeline_processed_through_ms: Option<f64>,
    pub(crate) latest_db_tick_timestamp_ms: Option<f64>,
    pub(crate) latest_depth_timestamp_ms: Option<f64>,
    pub(crate) data_age_ms: f64,
    pub(crate) degradation_reason: Option<String>,
    pub(crate) pipelines_contended: bool,
    pub(crate) db_contended: bool,
}

impl LiveMarketResolution {
    pub(crate) fn freshness_status(&self) -> &'static str {
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

pub(crate) fn merge_tool_live_metadata(target: &mut serde_json::Value, r: &LiveMarketResolution) {
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

pub(crate) fn render_market_snapshot_payload(r: &LiveMarketResolution) -> serde_json::Value {
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
    pub(crate) db: Arc<Mutex<Database>>,
    pub(crate) db_path: Arc<String>,
    /// Pool of read-only connections for `query_*` / `get_*` tools, so long
    /// research queries do not contend on the single writer mutex (`db`).
    pub(crate) read_pool: crate::read_pool::ReadPool,
    pub(crate) pipelines: Arc<Mutex<PipelineEngine>>,
    pub(crate) detector: Arc<Mutex<EventDetector>>,
    pub(crate) flow_emitter: Arc<Mutex<FlowEventEmitter>>,
    pub(crate) rules: Arc<Mutex<RulesEngine>>,
    pub(crate) last_bid: Arc<Mutex<f64>>,
    pub(crate) last_ask: Arc<Mutex<f64>>,
    pub(crate) feed_runtime: Arc<McpFeedRuntimeState>,
    pub(crate) runtime_events: Arc<RuntimeEventStore>,
    pub(crate) playbook_cache: Arc<PlaybookRuntimeCache>,
    pub(crate) backfill_manager: Arc<AsyncMutex<BackfillManager>>,
    pub(crate) options_cache: Arc<AsyncMutex<OptionsSnapshotCache>>,
    pub(crate) contract_cache: Arc<Mutex<ContractResolutionCache>>,
    pub(crate) context_frame_cache:
        Arc<Mutex<HashMap<String, research::context_frame::ContextFrame>>>,
    pub(crate) tool_router: ToolRouter<Self>,
}

#[derive(Debug)]
pub(crate) struct InMemoryJobState {
    pub(crate) run: HistoricalJobRun,
    pub(crate) request_key: String,
    pub(crate) cancel_flag: Arc<AtomicBool>,
}

#[derive(Debug, Default)]
pub(crate) struct BackfillManager {
    pub(crate) active_job_id: Option<String>,
    pub(crate) last_job_id: Option<String>,
    pub(crate) jobs: HashMap<String, InMemoryJobState>,
}

#[derive(Debug, Default)]
pub(crate) struct OptionsSnapshotCache {
    pub(crate) snapshot: Option<OptionsSnapshot>,
}

#[derive(Debug, Clone)]
pub(crate) struct CachedContractResolution {
    pub(crate) config: FeedConfig,
    pub(crate) contract: ContractMetadata,
    pub(crate) refreshed_at: Instant,
}

#[derive(Debug, Default)]
pub(crate) struct ContractResolutionCache {
    pub(crate) cached: Option<CachedContractResolution>,
}

#[derive(Debug, Default)]
pub(crate) struct PlaybookRuntimeCache {
    pub(crate) active_setups: RwLock<Arc<Vec<SetupDefinition>>>,
    pub(crate) risk_at_limit: AtomicBool,
}

impl PlaybookRuntimeCache {
    pub(crate) fn snapshot(&self) -> (Arc<Vec<SetupDefinition>>, bool) {
        let setups = match self.active_setups.read() {
            Ok(guard) => Arc::clone(&guard),
            Err(poisoned) => Arc::clone(&poisoned.into_inner()),
        };
        let risk_at_limit = self.risk_at_limit.load(Ordering::Acquire);
        (setups, risk_at_limit)
    }

    pub(crate) fn replace_active_setups(&self, setups: Vec<SetupDefinition>) {
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

    pub(crate) fn set_risk_at_limit(&self, at_limit: bool) {
        self.risk_at_limit.store(at_limit, Ordering::Release);
    }
}
