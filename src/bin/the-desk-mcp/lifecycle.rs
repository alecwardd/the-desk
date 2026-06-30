//! Feed runtime: tick processing, depth polling, session transitions, warm replay.

use chrono::TimeZone;
use rmcp::{model::*, ErrorData as McpError};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use the_desk_backend::attention::{
    AttentionNotifierConfig, AttentionPulseKind, SignalComposer, SignalComposerInput,
};
use the_desk_backend::backfill;
use the_desk_backend::db::{
    AttentionSignalQuery, AttentionSignalRecord, Database, ReplaySignalRecord, SessionScopeFilter,
    SetupRuntimeStateRecord, SignalOutcome,
};
use the_desk_backend::depth::{
    build_dom_feature_snapshot, build_dom_summary, enrich_dom_summary, DepthBook, DepthCommand,
    DepthReader, DomFeatureSnapshot, DomSummary, PullStackActivitySummary,
    ScanControl as DepthScanControl, DOM_NARRATIVE_HORIZON_MS,
};
use the_desk_backend::feed::monotonic::{MonotonicTickGuard, MonotonicTimestampDecision};
use the_desk_backend::feed::scid_reader::{
    scid_tail_offset_after_shrink, ScidReader, ScidTick, SCID_RECORD_SIZE,
};
use the_desk_backend::feed::{load_feed_config, TradeSide};
use the_desk_backend::observability::{RuntimeEvent, RuntimeEventLevel, RuntimeEventStore};
use the_desk_backend::pipelines::{
    EventDetector, FlowEventEmitter, MarketEvent, MarketState, PipelineEngine, PriorSessionData,
};
use the_desk_backend::research;
use the_desk_backend::rollover::{
    build_contract_rollover_status, ContractRolloverStatus, PriorReferenceTrust,
};
use the_desk_backend::rules::{
    RulesEngine, SetupDefinition, SetupEvaluationOutcome, SetupRuntimeSnapshot,
};
use the_desk_backend::{
    classify_delta_segment, classify_session, et_minutes_from_timestamp,
    minute_of_session_from_timestamp, session_date_from_timestamp_ms,
    trading_day_from_timestamp_ms, DeltaSegment, SessionType, RTH_CLOSE_ET,
};
use the_desk_backend::{outcome_tracker, outcomes};

#[allow(unused_imports)]
use crate::{helpers::*, params::*, state::*};

pub(crate) fn data_dir() -> std::path::PathBuf {
    let home = std::env::var("USERPROFILE")
        .or_else(|_| std::env::var("HOME"))
        .unwrap_or_else(|_| ".".to_string());
    let dir = std::path::PathBuf::from(home).join(".the-desk");
    std::fs::create_dir_all(&dir).ok();
    dir
}

pub(crate) fn compute_data_age(db: &Database) -> f64 {
    let now_ms = chrono::Utc::now().timestamp_millis() as f64;
    db.latest_tick_timestamp_ms()
        .ok()
        .flatten()
        .map(|ts| now_ms - ts)
        .unwrap_or(-1.0)
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ValidationDbSnapshot {
    pub(crate) tick_count: i64,
    pub(crate) last_ts: Option<f64>,
}

pub(crate) fn collect_validation_db_snapshot(
    db: &Arc<Mutex<Database>>,
) -> Result<ValidationDbSnapshot, McpError> {
    let db = db.lock().map_err(|_| lock_error())?;
    Ok(ValidationDbSnapshot {
        tick_count: db.raw_tick_count().unwrap_or(0),
        last_ts: db.latest_tick_timestamp_ms().ok().flatten(),
    })
}

pub(crate) fn collect_pipeline_invariants(
    pipelines: &Arc<Mutex<PipelineEngine>>,
) -> Result<Vec<(String, bool, String)>, McpError> {
    let pipelines = pipelines
        .lock()
        .map_err(|_| McpError::new(ErrorCode::INTERNAL_ERROR, "pipeline lock poisoned", None))?;
    Ok(pipelines.validate_invariants())
}

pub(crate) fn monotonicity_check_detail(snapshot: MonotonicRuntimeSnapshot) -> String {
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
pub(crate) fn persist_integrity_check(
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
pub(crate) enum SetupPersistencePolicy {
    Live,
    StartupReplay,
}

pub(crate) fn runtime_record_from_snapshot(
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

#[allow(clippy::too_many_arguments)]
pub(crate) fn persist_setup_evaluation(
    db: &Database,
    runtime_events: Option<&Arc<RuntimeEventStore>>,
    setup: &SetupDefinition,
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
        let mut outcome = SignalOutcome {
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
            rvol_at_fire: market_snapshot
                .rvol_ratio
                .is_finite()
                .then_some(market_snapshot.rvol_ratio),
            rvol_bucket_at_fire: Some(market_snapshot.rvol_bucket_index as i32),
            direction: None,
            entry_price: None,
            risk_points: None,
            exit_price: None,
            outcome_quality: outcomes::QUALITY_LEGACY_UNVERIFIED.to_string(),
            quality_flags: Vec::new(),
            outcome_engine_version: None,
            rules_schema_version: None,
            setup_template_hash: None,
            last_observed_price: None,
            last_observed_at_ms: None,
        };
        outcomes::initialize_outcome(&mut outcome, setup);
        let _ = db.insert_signal_outcome(&outcome);
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn evaluate_setups_for_snapshot(
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
            items.push((setup.clone(), outcome, runtime_snapshot));
        }
        r.update_prev_market(snapshot);
        items
    } else {
        return;
    };

    if let Ok(d) = db.lock() {
        for (setup, outcome, runtime_snapshot) in persist_items {
            persist_setup_evaluation(
                &d,
                runtime_events,
                &setup,
                &outcome,
                runtime_snapshot,
                snapshot,
                session_date,
                policy,
            );
        }
    }
}

pub(crate) fn record_attention_runtime_event(
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

pub(crate) fn expire_and_audit_attention_signals(
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

pub(crate) fn dispatch_attention_runtime_notifications(
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
pub(crate) fn compose_and_persist_attention(
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

#[derive(Debug, Clone)]
pub(crate) struct IngestTickOutcome {
    pub(crate) snapshot: MarketState,
    pub(crate) new_events: Vec<MarketEvent>,
}

#[derive(Debug, Default)]
pub(crate) struct PendingOutcomeSet {
    pending: HashMap<String, SignalOutcome>,
    dirty: HashSet<String>,
    resolved: HashSet<String>,
}

impl PendingOutcomeSet {
    pub(crate) fn reconcile_from_db(&mut self, db: &Database) -> Result<(), String> {
        let pending = db
            .pending_signal_outcomes_filtered(None, None)
            .map_err(|e| e.to_string())?;
        let mut live_ids = HashSet::new();
        for outcome in pending {
            live_ids.insert(outcome.signal_id.clone());
            self.pending
                .entry(outcome.signal_id.clone())
                .or_insert(outcome);
        }
        self.pending.retain(|id, _| {
            live_ids.contains(id) || self.dirty.contains(id) || self.resolved.contains(id)
        });
        Ok(())
    }

    pub(crate) fn observe_tick(
        &mut self,
        price: f64,
        timestamp_ms: f64,
    ) -> Vec<outcome_tracker::Resolution> {
        if self.pending.is_empty() {
            return Vec::new();
        }
        let current_session = session_date_from_timestamp_ms(timestamp_ms);
        let mut resolutions = Vec::new();
        for outcome in self.pending.values_mut() {
            if self.resolved.contains(&outcome.signal_id) {
                continue;
            }
            let mut tick_result = None;
            let fired_session = session_date_from_timestamp_ms(outcome.fired_at_ms);
            if fired_session != current_session {
                let exit_price = outcome.last_observed_price.unwrap_or(price);
                let exit_ts = outcome.last_observed_at_ms.unwrap_or(timestamp_ms);
                tick_result = Some(outcomes::finalize_time_exit(outcome, exit_price, exit_ts));
            }
            let tick_result =
                tick_result.unwrap_or_else(|| outcomes::apply_tick(outcome, price, timestamp_ms));
            match tick_result {
                outcomes::OutcomeTickResult::Resolved => {
                    self.dirty.insert(outcome.signal_id.clone());
                    self.resolved.insert(outcome.signal_id.clone());
                    resolutions.push(outcome_tracker::Resolution {
                        signal_id: outcome.signal_id.clone(),
                        outcome: outcome.outcome.clone(),
                        r_result: outcome.r_result,
                    });
                }
                outcomes::OutcomeTickResult::StillPending => {
                    self.dirty.insert(outcome.signal_id.clone());
                }
                outcomes::OutcomeTickResult::Ignored => {}
            }
        }
        resolutions
    }

    pub(crate) fn flush_to_db(&mut self, db: &Database) -> Result<(), String> {
        let resolved_ids: Vec<String> = self.resolved.iter().cloned().collect();
        for signal_id in resolved_ids {
            if let Some(outcome) = self.pending.remove(&signal_id) {
                db.update_signal_outcome_state(&outcome)
                    .map_err(|e| e.to_string())?;
            }
            self.dirty.remove(&signal_id);
            self.resolved.remove(&signal_id);
        }

        let dirty_ids: Vec<String> = self.dirty.iter().cloned().collect();
        for signal_id in dirty_ids {
            if let Some(outcome) = self.pending.get(&signal_id) {
                db.update_signal_outcome_progress(
                    &outcome.signal_id,
                    outcome.max_favorable_excursion,
                    outcome.max_adverse_excursion,
                    outcome.last_observed_price,
                    outcome.last_observed_at_ms,
                )
                .map_err(|e| e.to_string())?;
            }
            self.dirty.remove(&signal_id);
        }
        Ok(())
    }
}

/// Ingest a single tick through deterministic state only. No SQLite work happens here.
#[allow(clippy::too_many_arguments)]
pub(crate) fn ingest_tick(
    pipelines: &Arc<Mutex<PipelineEngine>>,
    detector: &Arc<Mutex<EventDetector>>,
    flow_emitter: &Arc<Mutex<FlowEventEmitter>>,
    pending_outcomes: Option<&Arc<Mutex<PendingOutcomeSet>>>,
    last_bid: &Arc<Mutex<f64>>,
    last_ask: &Arc<Mutex<f64>>,
    price: f64,
    volume: f64,
    is_buy: bool,
    timestamp_ms: f64,
    bid: f64,
    ask: f64,
    event_buffer: &mut Vec<the_desk_backend::pipelines::MarketEvent>,
) -> Option<IngestTickOutcome> {
    let session_type = et_minutes_from_timestamp(timestamp_ms)
        .map(classify_session)
        .unwrap_or(if minute_of_session_from_timestamp(timestamp_ms) < 0 {
            SessionType::Globex
        } else {
            SessionType::Rth
        });
    if session_type == SessionType::Unknown {
        return None;
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
            return None;
        }
    };
    let new_attention_events: Vec<MarketEvent> = event_buffer[event_buffer_start..].to_vec();

    if let Some(pending_outcomes) = pending_outcomes {
        if let Ok(mut pending) = pending_outcomes.lock() {
            let _ = pending.observe_tick(price, timestamp_ms);
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

    Some(IngestTickOutcome {
        snapshot,
        new_events: new_attention_events,
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_analysis_pass(
    rules: &Arc<Mutex<RulesEngine>>,
    playbook_cache: &Arc<PlaybookRuntimeCache>,
    db: &Arc<Mutex<Database>>,
    runtime_events: &Arc<RuntimeEventStore>,
    pending_outcomes: Option<&Arc<Mutex<PendingOutcomeSet>>>,
    snapshot: &MarketState,
    new_attention_events: &[MarketEvent],
    timestamp_ms: f64,
    pulse_kind: AttentionPulseKind,
) {
    let setup_trading_day = trading_day_from_timestamp_ms(timestamp_ms);
    evaluate_setups_for_snapshot(
        rules,
        playbook_cache,
        db,
        Some(runtime_events),
        snapshot,
        &setup_trading_day,
        timestamp_ms,
        SetupPersistencePolicy::Live,
    );

    if let Ok(d) = db.lock() {
        if let Some(pending_outcomes) = pending_outcomes {
            if let Ok(mut pending) = pending_outcomes.lock() {
                let _ = pending.flush_to_db(&d);
                let _ = pending.reconcile_from_db(&d);
            }
        }
        if !new_attention_events.is_empty() {
            compose_and_persist_attention(
                &d,
                runtime_events,
                snapshot,
                new_attention_events,
                pulse_kind,
                timestamp_ms,
                "live",
                None,
            );
        }
    }
}

/// Process a single tick through the old all-in-one path. Kept for tests and replay utilities.
#[allow(dead_code)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn process_tick(
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
    let pending_outcomes = Arc::new(Mutex::new(PendingOutcomeSet::default()));
    if let Ok(d) = db.lock() {
        let _ = pending_outcomes.lock().map(|mut pending| {
            let _ = pending.reconcile_from_db(&d);
        });
    }
    let Some(outcome) = ingest_tick(
        pipelines,
        detector,
        flow_emitter,
        Some(&pending_outcomes),
        last_bid,
        last_ask,
        price,
        volume,
        is_buy,
        timestamp_ms,
        bid,
        ask,
        event_buffer,
    ) else {
        return;
    };
    run_analysis_pass(
        rules,
        playbook_cache,
        db,
        runtime_events,
        Some(&pending_outcomes),
        &outcome.snapshot,
        &outcome.new_events,
        timestamp_ms,
        AttentionPulseKind::EventDriven,
    );

    // Flush event buffer periodically
    if event_buffer.len() >= 50 {
        if let Ok(d) = db.lock() {
            let _ = d.insert_market_events_batch(event_buffer);
        }
        event_buffer.clear();
    }
}

pub(crate) fn current_best_bid_ask(
    last_bid: &Arc<Mutex<f64>>,
    last_ask: &Arc<Mutex<f64>>,
) -> (f64, f64) {
    let bid = last_bid.lock().ok().map(|v| *v).unwrap_or_default();
    let ask = last_ask.lock().ok().map(|v| *v).unwrap_or_default();
    (bid, ask)
}

pub(crate) fn build_live_feature_state_snapshot_payload(
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

pub(crate) fn context_frame_warm_event(event_type: &str) -> bool {
    matches!(event_type, "day_type_change" | "rvol_spike")
}

pub(crate) fn warm_context_frame_cache(
    db: &Arc<Mutex<Database>>,
    cache: &Arc<Mutex<HashMap<String, research::context_frame::ContextFrame>>>,
    runtime_events: &Arc<RuntimeEventStore>,
    snapshot: &serde_json::Value,
    options: research::context_frame::ContextFrameOptions,
) {
    let cache_key = research::context_frame::cache_key_for_snapshot(snapshot, &options);
    match cache.lock() {
        Ok(cache) => {
            if cache.get(&cache_key).is_some() {
                return;
            }
        }
        Err(_) => {
            record_runtime_event(
                runtime_events,
                Some(db),
                RuntimeEventLevel::Warn,
                "context_frame.cache_warm_failed",
                "context_frame",
                "Context-frame cache warm skipped because the cache lock was unavailable.",
                serde_json::json!({ "cacheKey": cache_key, "reason": "cache_lock_failed" }),
            );
            return;
        }
    }
    let build_result = match db.lock() {
        Ok(db) => research::context_frame::build_context_frame(&db, snapshot, options),
        Err(_) => {
            record_runtime_event(
                runtime_events,
                Some(db),
                RuntimeEventLevel::Warn,
                "context_frame.cache_warm_failed",
                "context_frame",
                "Context-frame cache warm skipped because the database lock was unavailable.",
                serde_json::json!({ "cacheKey": cache_key, "reason": "db_lock_failed" }),
            );
            return;
        }
    };
    let Ok(mut frame) = build_result else {
        record_runtime_event(
            runtime_events,
            Some(db),
            RuntimeEventLevel::Warn,
            "context_frame.cache_warm_failed",
            "context_frame",
            "Context-frame cache warm failed while building the frame.",
            serde_json::json!({ "cacheKey": cache_key, "reason": "build_failed" }),
        );
        return;
    };
    frame.meta.cache_status = "warmed".to_string();
    let Ok(mut cache) = cache.lock() else {
        record_runtime_event(
            runtime_events,
            Some(db),
            RuntimeEventLevel::Warn,
            "context_frame.cache_warm_failed",
            "context_frame",
            "Context-frame cache warm built a frame but could not store it.",
            serde_json::json!({ "cacheKey": cache_key, "reason": "cache_store_lock_failed" }),
        );
        return;
    };
    if cache.len() >= CONTEXT_FRAME_CACHE_LIMIT {
        if let Some(first_key) = cache.keys().next().cloned() {
            cache.remove(&first_key);
        }
    }
    cache.insert(cache_key, frame);
}

pub(crate) fn persist_feature_state_payload(
    db: &Arc<Mutex<Database>>,
    timestamp_ms: f64,
    payload: &serde_json::Value,
) {
    if let Ok(d) = db.lock() {
        let _ = d.upsert_feature_state(timestamp_ms, payload);
        if should_persist_live_context_snapshot(timestamp_ms) {
            let context = research::context_frame::snapshot_context_buckets(payload, timestamp_ms);
            let _ = d.insert_pipeline_snapshot_with_context(timestamp_ms, payload, &context);
        }
    }
}

pub(crate) fn should_persist_live_context_snapshot(timestamp_ms: f64) -> bool {
    if !timestamp_ms.is_finite() || timestamp_ms <= 0.0 {
        return false;
    }
    let last = tick_ms_from_bits(LAST_LIVE_CONTEXT_SNAPSHOT_MS_BITS.load(Ordering::Acquire));
    if last
        .map(|last| timestamp_ms - last < LIVE_CONTEXT_FRAME_SNAPSHOT_INTERVAL_MS)
        .unwrap_or(false)
    {
        return false;
    }
    LAST_LIVE_CONTEXT_SNAPSHOT_MS_BITS.store(tick_ms_to_bits(timestamp_ms), Ordering::Release);
    true
}

/// Persist `feature_state` after `dom_summary` has been updated.
/// Uses either the live pipeline snapshot path (`pipelines` then `db`) or a single DB critical
/// section to merge DOM data into the previous snapshot, but never holds both mutexes at once.
pub(crate) fn persist_feature_state_after_dom_summary(
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
pub(crate) struct DepthPollWorkerState {
    pub(crate) active_path: Option<std::path::PathBuf>,
    pub(crate) offset: u64,
    pub(crate) batch_id: i64,
    pub(crate) book: DepthBook,
}

#[derive(Debug)]
pub(crate) struct DepthPersistWork {
    pub(crate) source_file: String,
    pub(crate) trading_day: String,
    pub(crate) last_record_timestamp_ms: f64,
    pub(crate) records: Vec<the_desk_backend::depth::DepthRecord>,
    pub(crate) snapshot: the_desk_backend::depth::DomSnapshot,
    pub(crate) feature: DomFeatureSnapshot,
    pub(crate) batch_id: i64,
}

pub(crate) fn default_depth_feature_snapshot(
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

pub(crate) fn build_depth_feature_snapshot(
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

pub(crate) fn recover_depth_state_after_shrink(
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

pub(crate) fn compute_depth_poll_step(
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
pub(crate) fn apply_depth_persist_work(
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
pub(crate) struct StartupWarmReplayResult {
    pub(crate) cutover_offset: u64,
    pub(crate) applied_tick_count: usize,
}

pub(crate) fn safe_scid_data_offset(reader: &ScidReader) -> u64 {
    ScidReader::header_size_bytes_for_path(reader.path()).unwrap_or(56)
}

#[derive(Debug)]
pub(crate) struct ScidPollReadStep {
    pub(crate) requested_offset: u64,
    pub(crate) start_offset: u64,
    pub(crate) next_offset: u64,
    pub(crate) file_len: u64,
    pub(crate) ticks: Vec<ScidTick>,
}

impl ScidPollReadStep {
    pub(crate) fn was_realigned(&self) -> bool {
        self.start_offset != self.requested_offset
    }

    pub(crate) fn was_shrink_reset(&self) -> bool {
        self.file_len < self.requested_offset
    }
}

#[allow(dead_code)]
pub(crate) fn read_scid_poll_step(
    reader: &ScidReader,
    requested_offset: u64,
) -> Result<ScidPollReadStep, String> {
    read_scid_poll_step_inner(reader, requested_offset, None)
}

pub(crate) fn read_scid_poll_step_capped(
    reader: &ScidReader,
    requested_offset: u64,
    max_records: usize,
) -> Result<ScidPollReadStep, String> {
    read_scid_poll_step_inner(reader, requested_offset, Some(max_records))
}

fn read_scid_poll_step_inner(
    reader: &ScidReader,
    requested_offset: u64,
    max_records: Option<usize>,
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

    let batch = match max_records {
        Some(max_records) => reader
            .read_bulk_from_offset_capped(start_offset, max_records)
            .map_err(|e| e.to_string())?,
        None => reader
            .read_bulk_from_offset(start_offset)
            .map_err(|e| e.to_string())?,
    };

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
pub(crate) fn rth_window_ms_for_date(session_date: &str) -> Option<(f64, f64)> {
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

pub(crate) fn contract_scope(
    contract_metadata: &the_desk_backend::feed::ContractMetadata,
) -> (Option<&str>, Option<&str>) {
    let root_symbol = contract_metadata.root_symbol.trim();
    let contract_symbol = contract_metadata.contract_symbol.trim();
    (
        (!root_symbol.is_empty()).then_some(root_symbol),
        (!contract_symbol.is_empty()).then_some(contract_symbol),
    )
}

pub(crate) fn build_rollover_status_from_db(
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

pub(crate) fn authoritative_prior_reference_from_db(
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

pub(crate) fn contract_session_scope(
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
pub(crate) struct RthCloseResult {
    pub(crate) session_date: String,
    pub(crate) high: f64,
    pub(crate) low: f64,
    pub(crate) close: f64,
    pub(crate) session_delta: f64,
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum RthCloseFinalizeError {
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
pub(crate) fn finalize_rth_close(
    pipelines: &Arc<Mutex<PipelineEngine>>,
    db: &Arc<Mutex<Database>>,
    pending_events: &[MarketEvent],
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
        volume_curve: Vec<f64>,
        end_state: SessionEndState,
        close_ts: f64,
        session_delta: f64,
    }

    let close_data = {
        let mut p = pipelines
            .lock()
            .map_err(|_| RthCloseFinalizeError::PipelineLockUnavailable("snapshot"))?;
        if !p.levels.rth_started() {
            return Ok(None);
        }
        p.refresh_day_type_classification();
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
        let volume_curve = p.rvol.current_curve();
        let end_state = p.session_end_state();
        let session_delta = snapshot.session_delta;
        CloseData {
            snapshot,
            open_price,
            tick_count,
            total_volume,
            volume_curve,
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
    if let Ok(d) = db.lock() {
        if let Ok(flushed_events) = d.list_ib_extension_events_for_session(&session_date, "RTH") {
            backfill::apply_ib_extension_events(&mut summary, &flushed_events);
        }
    }
    backfill::apply_ib_extension_events(&mut summary, pending_events);

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
        .persist_live_session_close(&summary, prior_day_tuple, Some(&close_data.volume_curve))
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

fn boundary_inventory_session_type(
    new_session: SessionType,
    new_segment: DeltaSegment,
) -> &'static str {
    if new_session == SessionType::Rth {
        "RTH"
    } else if new_segment == DeltaSegment::Asia {
        "Asia"
    } else {
        "London"
    }
}

pub(crate) fn load_boundary_session_cache_entry(
    db: &Database,
    new_session: SessionType,
    new_segment: DeltaSegment,
    boundary_tick_ts: f64,
    contract_metadata: &the_desk_backend::feed::ContractMetadata,
) -> BoundarySessionCacheEntry {
    let lookup_date = session_date_from_timestamp_ms(boundary_tick_ts);
    let (prior_reference, rollover_status) = authoritative_prior_reference_from_db(
        db,
        contract_metadata,
        Some(contract_metadata),
        &lookup_date,
    )
    .map(|(prior, status)| (prior, Some(status)))
    .unwrap_or((None, None));

    let inv_session_type = boundary_inventory_session_type(new_session, new_segment);
    let scope = contract_session_scope(contract_metadata);
    let mut prior_inventory: Vec<PriorSessionData> = db
        .list_session_summaries_scoped(None, None, None, Some(inv_session_type), 5, scope.as_ref())
        .unwrap_or_default()
        .into_iter()
        .filter(|s| s.dnva_high > 0.0 && s.dnva_low > 0.0 && s.dnp > 0.0)
        .map(|s| PriorSessionData {
            final_delta: s.session_delta,
            dnva_high: s.dnva_high,
            dnva_low: s.dnva_low,
            dnp: s.dnp,
        })
        .collect();
    prior_inventory.reverse();
    let rth_rvol_curves = db
        .recent_session_volume_curves_for_contract(
            "RTH",
            20,
            Some(&contract_metadata.root_symbol),
            Some(&contract_metadata.contract_symbol),
        )
        .unwrap_or_default();
    let globex_rvol_curves = db
        .recent_session_volume_curves_for_contract(
            "Globex",
            20,
            Some(&contract_metadata.root_symbol),
            Some(&contract_metadata.contract_symbol),
        )
        .unwrap_or_default();

    BoundarySessionCacheEntry {
        lookup_date,
        new_session,
        new_segment,
        contract_symbol: contract_metadata.contract_symbol.clone(),
        prior_reference,
        rollover_status,
        prior_inventory,
        rth_rvol_curves,
        globex_rvol_curves,
        refreshed_at: std::time::Instant::now(),
    }
}

fn apply_prepared_session_data(
    pipelines: &Arc<Mutex<PipelineEngine>>,
    runtime_events: Option<&Arc<RuntimeEventStore>>,
    prepared: BoundarySessionCacheEntry,
    contract_metadata: &the_desk_backend::feed::ContractMetadata,
) {
    let lookup_date = prepared.lookup_date.clone();
    let (root_symbol, contract_symbol) = contract_scope(contract_metadata);
    if let Ok(mut p) = pipelines.lock() {
        p.reset_session_with_type(prepared.new_session == SessionType::Globex);
        p.rvol.load_historical_curve(&prepared.rth_rvol_curves);
        p.rvol
            .load_globex_historical_curve(&prepared.globex_rvol_curves);
        if matches!(prepared.new_session, SessionType::Rth | SessionType::Globex) {
            if let Some(prior_ref) = prepared.prior_reference {
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
                if let Some(status) = prepared.rollover_status {
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
        p.session_inventory
            .load_prior_sessions(prepared.prior_inventory);
    }
}

pub(crate) fn persist_current_globex_volume_curve(
    pipelines: &Arc<Mutex<PipelineEngine>>,
    db: &Arc<Mutex<Database>>,
    boundary_tick_ts: f64,
    contract_metadata: &the_desk_backend::feed::ContractMetadata,
) {
    let curve = match pipelines.lock() {
        Ok(p) if p.rvol.is_globex() && p.rvol.session_volume() > 0.0 => p.rvol.current_curve(),
        _ => return,
    };
    let session_date = session_date_from_timestamp_ms(boundary_tick_ts);
    if let Ok(d) = db.lock() {
        let _ = d.save_volume_curve_with_contract(
            &session_date,
            "Globex",
            &curve,
            Some(&contract_metadata.root_symbol),
            Some(&contract_metadata.contract_symbol),
        );
    }
}

/// Reset pipelines for a new session and load the most recent prior-day /
/// prior-session inventory references from SQLite. Used at session boundaries
/// other than the RTH close (which goes through `finalize_rth_close`).
///
/// Idempotent: safe to invoke even when in-memory state was already prepared
/// by a prior `finalize_rth_close` (the DB read returns the same atomically
/// persisted values).
pub(crate) fn prepare_for_new_session(
    pipelines: &Arc<Mutex<PipelineEngine>>,
    db: &Arc<Mutex<Database>>,
    runtime_events: Option<&Arc<RuntimeEventStore>>,
    new_session: SessionType,
    new_segment: DeltaSegment,
    boundary_tick_ts: f64,
    contract_metadata: &the_desk_backend::feed::ContractMetadata,
) {
    let prepared = db.lock().ok().map(|d| {
        load_boundary_session_cache_entry(
            &d,
            new_session,
            new_segment,
            boundary_tick_ts,
            contract_metadata,
        )
    });
    if let Some(prepared) = prepared {
        apply_prepared_session_data(pipelines, runtime_events, prepared, contract_metadata);
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_for_new_session_with_cache(
    pipelines: &Arc<Mutex<PipelineEngine>>,
    db: &Arc<Mutex<Database>>,
    runtime_events: Option<&Arc<RuntimeEventStore>>,
    boundary_cache: &Arc<Mutex<BoundarySessionCache>>,
    new_session: SessionType,
    new_segment: DeltaSegment,
    boundary_tick_ts: f64,
    contract_metadata: &the_desk_backend::feed::ContractMetadata,
) {
    let lookup_date = session_date_from_timestamp_ms(boundary_tick_ts);
    let cached = boundary_cache.lock().ok().and_then(|cache| {
        cache.cached.as_ref().and_then(|entry| {
            entry
                .matches(
                    &lookup_date,
                    new_session,
                    new_segment,
                    &contract_metadata.contract_symbol,
                )
                .then(|| entry.clone())
        })
    });

    let prepared = if let Some(entry) = cached {
        entry
    } else {
        if let Some(runtime_events) = runtime_events {
            let event = RuntimeEvent::new(
                RuntimeEventLevel::Warn,
                "session.boundary_cache_cold",
                "session",
                "Session boundary cache was cold; falling back to inline SQLite reads.",
                serde_json::json!({
                    "lookupDate": lookup_date,
                    "newSession": format!("{:?}", new_session),
                    "newSegment": format!("{:?}", new_segment),
                    "contractSymbol": contract_metadata.contract_symbol,
                }),
            );
            let _ = runtime_events.record(event);
        }
        match db.lock() {
            Ok(d) => load_boundary_session_cache_entry(
                &d,
                new_session,
                new_segment,
                boundary_tick_ts,
                contract_metadata,
            ),
            Err(_) => return,
        }
    };

    apply_prepared_session_data(pipelines, runtime_events, prepared, contract_metadata);
}

/// Warm-replay SCID ticks into the live pipeline up to a pre-captured cutover offset.
///
/// The returned `cutover_offset` is the last fully consumed SCID offset, not the requested target,
/// so the live tail can safely resume after truncated/partial startup reads without skipping ticks.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_startup_warm_replay(
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
                    &[],
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
                if current_session == SessionType::Globex && new_session == SessionType::Rth {
                    persist_current_globex_volume_curve(
                        pipelines,
                        db,
                        tick.timestamp_ms,
                        contract_metadata,
                    );
                }
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
                &[],
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
