//! Constructors, shared service methods, and the combined tool router.

use chrono::Utc;
use rmcp::{handler::server::tool::ToolRouter, model::*, ErrorData as McpError};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use the_desk_backend::backfill;
use the_desk_backend::db::{Database, HistoricalJobRun};
use the_desk_backend::feed::scid_reader::ScidReader;
use the_desk_backend::feed::{
    load_feed_config, resolve_contract_metadata, ContractMetadata, FeedConfig,
};
use the_desk_backend::observability::{RuntimeEvent, RuntimeEventLevel, RuntimeEventStore};
use the_desk_backend::options::{
    fetch_options_snapshot, load_options_config, OptionsCredentials, OptionsSnapshot,
};
use the_desk_backend::pipelines::{EventDetector, FlowEventEmitter, PipelineEngine};
use the_desk_backend::rollover::ContractRolloverStatus;
use the_desk_backend::rules::{RulesEngine, SetupRuntimeSnapshot, SetupState, SetupTransition};
use the_desk_backend::trading_day_from_timestamp_ms;
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::{sleep, Duration};

#[allow(unused_imports)]
use crate::{helpers::*, lifecycle::*, params::*, state::*};

impl TheDeskMcp {
    /// Combined router across all tool domain modules.
    pub(crate) fn tool_router() -> ToolRouter<Self> {
        Self::market_router()
            + Self::dom_router()
            + Self::options_router()
            + Self::playbook_router()
            + Self::risk_router()
            + Self::journal_router()
            + Self::memory_router()
            + Self::research_router()
            + Self::admin_router()
    }

    #[cfg(test)]
    pub(crate) fn new(db: Database, pipelines: PipelineEngine, db_path: String) -> Self {
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

    pub(crate) fn with_runtime_events(
        db: Database,
        pipelines: PipelineEngine,
        db_path: String,
        runtime_events: Arc<RuntimeEventStore>,
    ) -> Self {
        let read_pool = crate::read_pool::ReadPool::new(
            db_path.clone(),
            crate::read_pool::DEFAULT_READ_POOL_SIZE,
        );
        Self {
            db: Arc::new(Mutex::new(db)),
            db_path: Arc::new(db_path),
            read_pool,
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
            boundary_cache: Arc::new(Mutex::new(BoundarySessionCache::default())),
            context_frame_cache: Arc::new(Mutex::new(HashMap::new())),
            tool_router: Self::tool_router(),
        }
    }

    /// Run a read-only query on a pooled `SQLITE_OPEN_READ_ONLY` connection.
    ///
    /// The closure executes inside `spawn_blocking`, so a heavy query neither
    /// contends on the single writer mutex (`self.db`) nor blocks the async
    /// runtime worker thread. Use this for read-only `query_*` / `get_*` tools;
    /// keep writes on the mutex-guarded writer connection.
    pub(crate) async fn with_read_db<T, F>(&self, f: F) -> Result<T, McpError>
    where
        F: FnOnce(&Database) -> Result<T, McpError> + Send + 'static,
        T: Send + 'static,
    {
        let mut guard = self.read_pool.acquire().await.map_err(db_error)?;
        let db = guard.take();
        let (db, result) = tokio::task::spawn_blocking(move || {
            let result = f(&db);
            (db, result)
        })
        .await
        .map_err(|e| db_error(format!("read query task failed: {e}")))?;
        guard.restore(db);
        result
    }

    pub(crate) fn resolve_contract_cached(&self) -> (FeedConfig, ContractMetadata) {
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

    pub(crate) fn refresh_playbook_setups_from_db(
        &self,
        db: &Database,
    ) -> Result<bool, the_desk_backend::db::DbError> {
        let (active_setups, risk_at_limit) = db.load_playbook_runtime_seed()?;
        self.playbook_cache.replace_active_setups(active_setups);
        Ok(risk_at_limit)
    }

    pub(crate) fn hydrate_playbook_runtime_cache(&self) -> Result<(), McpError> {
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

    pub(crate) fn current_pipeline_contract_metadata(
        &self,
    ) -> Option<the_desk_backend::feed::ContractMetadata> {
        self.pipelines
            .lock()
            .ok()
            .map(|pipelines| pipelines.contract_metadata().clone())
    }

    pub(crate) fn rollover_status_for_date(
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

    pub(crate) fn persist_manual_setup_state_change(
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

    pub(crate) fn transition_trade_idea_tool(
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

    pub(crate) async fn get_or_refresh_options_snapshot(
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
    pub(crate) fn resolve_live_market_view(&self) -> Option<LiveMarketResolution> {
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
    pub(crate) fn resolve_market_snapshot_contention_gap(&self) -> Option<LiveMarketResolution> {
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

    pub(crate) fn current_market_snapshot_payload(&self) -> Option<serde_json::Value> {
        self.resolve_live_market_view()
            .map(|r| render_market_snapshot_payload(&r))
            .or_else(|| {
                self.resolve_market_snapshot_contention_gap()
                    .map(|r| render_market_snapshot_payload(&r))
            })
    }

    pub(crate) fn data_age_from_db_or_atomic(&self) -> f64 {
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
    pub(crate) fn current_snapshot_value(&self) -> Option<serde_json::Value> {
        self.resolve_live_market_view()
            .map(|r| r.snapshot)
            .or_else(|| {
                self.db
                    .lock()
                    .ok()
                    .and_then(|d| d.latest_feature_state().ok().flatten())
            })
    }

    pub(crate) async fn wait_for_job_terminal(&self, job_id: &str) -> Option<HistoricalJobRun> {
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

    pub(crate) async fn get_job_run(
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

    pub(crate) async fn queue_historical_job(
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
        // Optional per-job contract routing: replay an explicit contract's .scid
        // without mutating global feed config, keeping live trading isolated.
        let job_contract_metadata = params.contract_symbol.as_deref().map(|symbol| {
            the_desk_backend::feed::resolve_contract_metadata_for_symbol(
                &load_feed_config(),
                symbol,
            )
        });
        let initial_estimated_records = {
            let config = load_feed_config();
            let reader = match &job_contract_metadata {
                Some(meta) => {
                    ScidReader::with_price_scale(meta.scid_path.clone(), config.price_scale)
                }
                None => ScidReader::from_feed_config(&config),
            };
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
            "contractSymbol": params.contract_symbol,
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
            let reader = match &job_contract_metadata {
                Some(meta) => {
                    ScidReader::with_price_scale(meta.scid_path.clone(), config.price_scale)
                }
                None => ScidReader::from_feed_config(&config),
            };
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
            let replay_options = match job_contract_metadata {
                Some(meta) => backfill::BackfillReplayOptions {
                    contract_metadata: Some(meta),
                    ..Default::default()
                },
                None => backfill::BackfillReplayOptions::default(),
            };
            let result = backfill::run_backfill_job_with_options(
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
                replay_options,
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
}
