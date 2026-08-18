//! The Desk MCP server binary.
//!
//! Thin entry point: module wiring lives here; all behavior is in the sibling
//! modules. Tools are grouped by domain under `tools/` and combined into a
//! single router in `service.rs`. See docs/mcp/README.md for the architecture.

use chrono::{Datelike, Days, TimeZone, Timelike};
use rmcp::{transport::stdio, ServiceExt};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use the_desk_backend::attention::AttentionPulseKind;
use the_desk_backend::db::{Database, RawTickBatchRow};
use the_desk_backend::engine::RouterRoot;
use the_desk_backend::feed::monotonic::{MonotonicTickGuard, MonotonicTimestampDecision};
use the_desk_backend::feed::scid_reader::ScidReader;
use the_desk_backend::feed::{load_feed_config, resolve_contract_metadata, TradeSide};
use the_desk_backend::observability::{
    init_logging, load_logging_config, RuntimeEventLevel, RuntimeEventStore,
};
use the_desk_backend::pipelines::{MarketEvent, PipelineEngine};
use the_desk_backend::research;
use the_desk_backend::{
    classify_delta_segment, classify_session, et_minutes_from_timestamp, globex_open_ms,
    session_date_from_timestamp_ms, DeltaSegment, SessionType, GLOBEX_OPEN_ET, RTH_CLOSE_ET,
    RTH_OPEN_ET,
};
use tokio::time::{sleep, Duration};

mod docs;
mod handler;
mod helpers;
mod lifecycle;
mod params;
mod read_pool;
mod service;
mod state;
mod tool_telemetry;
mod tools;

#[cfg(test)]
mod tests;

#[allow(unused_imports)]
use crate::{helpers::*, lifecycle::*, params::*, state::*};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().any(|a| a == "--write-tool-docs") {
        return docs::write_tool_reference();
    }
    if std::env::args().any(|a| a == "--write-catalog-docs") {
        the_desk_backend::catalog::write_catalog_docs()?;
        eprintln!(
            "Wrote {} and {}",
            the_desk_backend::catalog::catalog_json_path().display(),
            the_desk_backend::catalog::catalog_markdown_path().display()
        );
        return Ok(());
    }
    if std::env::args().any(|a| a == "--write-sil-m0-baseline") {
        return write_sil_m0_baseline_async().await;
    }
    if std::env::args().any(|a| a == "--seed-templates") {
        let activate = std::env::args().any(|a| a == "--activate");
        return seed_templates_cli(activate);
    }
    if std::env::args().any(|a| a == "--seed-positioning-corpus") {
        return seed_positioning_corpus_cli();
    }
    if std::env::args().any(|a| a == "--seed-backtest-db") {
        return seed_backtest_db_cli();
    }
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
    if let Ok(curves) = db.recent_session_volume_curves_for_contract(
        "RTH",
        20,
        Some(&contract_metadata.root_symbol),
        Some(&contract_metadata.contract_symbol),
    ) {
        pipelines.rvol.load_historical_curve(&curves);
    }
    if let Ok(curves) = db.recent_session_volume_curves_for_contract(
        "Globex",
        20,
        Some(&contract_metadata.root_symbol),
        Some(&contract_metadata.contract_symbol),
    ) {
        pipelines.rvol.load_globex_historical_curve(&curves);
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
            "engineMode": server.sil_config.engine_mode.as_str(),
            "engineBind": server.engine_bind_addr(),
        }),
    );
    server.hydrate_playbook_runtime_cache().map_err(|e| {
        std::io::Error::other(format!(
            "failed to hydrate playbook runtime cache from SQLite: {e}"
        ))
    })?;

    // Take a verified database snapshot in the background (off the serving path).
    spawn_startup_backup(Arc::clone(&server.runtime_events), Arc::clone(&server.db));

    // SIL-M2a: external engine mode — MCP is a thin adapter; do not own ingest.
    let external_engine = server.uses_external_engine();
    if !external_engine {
        // Single-writer: embedded MCP owns the cold root. External mode leaves
        // dumps to `the-desk-engine` so two processes never share the hive.
        server
            .market_router
            .set_cold_frame_store(the_desk_backend::engine::ColdFrameStore::new(
                server.cold_frames_dir.clone(),
            ));
    }
    if external_engine {
        server.spawn_engine_adapter_refresh();
        server
            .feed_runtime
            .rules_warm_replay_complete
            .store(true, Ordering::Release);
        record_runtime_event(
            &server.runtime_events,
            Some(&server.db),
            RuntimeEventLevel::Info,
            "engine.adapter_mode",
            "engine",
            "MCP running as external-engine adapter; ingest owned by the-desk-engine.",
            serde_json::json!({
                "engineMode": "external",
                "engineBind": server.engine_bind_addr(),
            }),
        );
    }

    // MarketRouter v0: embedded ES lane on the same clock as NQ coaching ingest.
    // Isolated PipelineEngine — session scope cannot mix with NQ. Missing ES SCID
    // degrades only the ES slice of get_state; NQ coaching path is unchanged.
    if !external_engine {
        let router = Arc::clone(&server.market_router);
        let feed = config.clone();
        let poll_ms = config.flush_poll_ms.max(250);
        let max_ticks = config.max_ticks_per_poll.max(1);
        let runtime_events_es = Arc::clone(&server.runtime_events);
        let db_es = Arc::clone(&server.db);
        tokio::spawn(async move {
            use the_desk_backend::engine::{FileProvider, RouterRoot, SourceProvider};
            use tokio::time::{sleep, Duration};

            let mut cfg = feed.clone();
            cfg.base_symbol = "ES".into();
            cfg.symbol = "ES".into();
            cfg.active_symbol_override = None;
            let es_meta = resolve_contract_metadata(&cfg);
            router.set_contract_metadata(RouterRoot::Es, es_meta.clone());
            if !es_meta.scid_file_exists {
                record_runtime_event(
                    &runtime_events_es,
                    Some(&db_es),
                    RuntimeEventLevel::Info,
                    "market_router.es_scid_missing",
                    "engine",
                    "MarketRouter ES FileProvider has no .scid yet; ES get_state slice stays degraded. NQ coaching path is unchanged.",
                    serde_json::json!({
                        "scidPath": es_meta.scid_path,
                        "rootSymbol": "ES",
                    }),
                );
            }
            let mut provider = FileProvider::from_feed_config_for_root(&feed, RouterRoot::Es);
            // Live tail: start at EOF so we do not replay the whole ES history on boot.
            if let Ok(end) = std::fs::metadata(provider.scid_path()).map(|m| m.len()) {
                provider.seek(end);
            }
            let mut providers = std::collections::BTreeMap::new();
            providers.insert(
                RouterRoot::Es,
                Box::new(provider) as Box<dyn SourceProvider>,
            );
            loop {
                match router.poll_once(&mut providers, max_ticks) {
                    Ok(n) => {
                        let db_es = Arc::clone(&db_es);
                        let persist_router = Arc::clone(&router);
                        if let Err(err) = tokio::task::spawn_blocking(move || match db_es.lock() {
                            Ok(db) => {
                                if let Err(err) = persist_router.persist_journal(&db) {
                                    tracing::warn!(
                                        error = %err,
                                        "market_router.journal_persist"
                                    );
                                }
                            }
                            Err(err) => {
                                tracing::warn!(
                                    error = %err,
                                    "market_router.journal_persist_lock"
                                );
                            }
                        })
                        .await
                        {
                            tracing::warn!(
                                error = %err,
                                "market_router.journal_persist_join"
                            );
                        }
                        if n > 0 {
                            continue;
                        }
                    }
                    Err(err) => {
                        tracing::debug!(error = %err, "market_router.es_poll_error");
                    }
                }
                sleep(Duration::from_millis(poll_ms)).await;
            }
        });
    }

    if !external_engine && scid_available {
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
    } else if !external_engine {
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
    // (embedded mode only — external mode leaves ingest to the-desk-engine).
    if !external_engine && scid_available {
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
        let context_frame_cache_bg = Arc::clone(&server.context_frame_cache);
        let poll_ms = config.flush_poll_ms;
        let price_scale = config.price_scale;
        let max_ticks_per_poll = config.max_ticks_per_poll.max(1);
        let analysis_min_interval_ms = config.analysis_min_interval_ms.max(1.0);
        let analysis_max_ticks = config.analysis_max_ticks.max(1);
        let reader_path = reader.path().to_path_buf();
        let contract_metadata = contract_metadata.clone();
        let feed_rt_bg = Arc::clone(&server.feed_runtime);
        let boundary_cache_bg = Arc::clone(&server.boundary_cache);
        let market_router_bg = Arc::clone(&server.market_router);
        {
            let feed_rt_watchdog = Arc::clone(&server.feed_runtime);
            let runtime_events_watchdog = Arc::clone(&server.runtime_events);
            let db_watchdog = Arc::clone(&server.db);
            let reader_path_watchdog = reader.path().to_path_buf();
            tokio::spawn(async move {
                use tokio::time::{sleep, Duration};

                const WATCHDOG_INTERVAL_MS: u64 = 5_000;
                const STALL_THRESHOLD_MS: u64 = 10_000;

                let mut last_reported_wall_ms = 0u64;
                loop {
                    sleep(Duration::from_millis(WATCHDOG_INTERVAL_MS)).await;
                    let now_wall_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
                    let fresh_len = std::fs::metadata(&reader_path_watchdog)
                        .map(|m| m.len())
                        .unwrap_or(0);
                    let processed = feed_rt_watchdog
                        .scid_processed_offset
                        .load(Ordering::Acquire);
                    let last_completed = feed_rt_watchdog
                        .last_scid_poll_completed_wall_ms
                        .load(Ordering::Acquire);
                    let started = feed_rt_watchdog
                        .last_scid_poll_started_wall_ms
                        .load(Ordering::Acquire);
                    let last_progress = last_completed.max(started);
                    let backlog = fresh_len.saturating_sub(processed);
                    let stalled = backlog > 0
                        && last_progress > 0
                        && now_wall_ms.saturating_sub(last_progress) >= STALL_THRESHOLD_MS;
                    if stalled && now_wall_ms.saturating_sub(last_reported_wall_ms) >= 30_000 {
                        record_runtime_event(
                            &runtime_events_watchdog,
                            Some(&db_watchdog),
                            RuntimeEventLevel::Warn,
                            "scid.ingest_stalled",
                            "scid",
                            "SCID file is growing while processed offset is not advancing.",
                            serde_json::json!({
                                "freshFileLenBytes": fresh_len,
                                "scidProcessedOffsetBytes": processed,
                                "scidReadOffsetBytes": feed_rt_watchdog.scid_read_offset.load(Ordering::Acquire),
                                "scidBacklogBytes": backlog,
                                "workerPhase": feed_rt_watchdog.scid_worker_phase_label(),
                                "lastPollStartedWallMs": started,
                                "lastPollCompletedWallMs": last_completed,
                            }),
                        );
                        last_reported_wall_ms = now_wall_ms;
                    }
                }
            });
        }
        {
            let db_prewarm = Arc::clone(&server.db);
            let boundary_cache_prewarm = Arc::clone(&server.boundary_cache);
            let runtime_events_prewarm = Arc::clone(&server.runtime_events);
            let contract_metadata_prewarm = contract_metadata.clone();
            tokio::spawn(async move {
                use chrono_tz::US::Eastern;
                use tokio::time::{sleep, Duration};

                const PREWARM_INTERVAL_MS: u64 = 60_000;

                loop {
                    let now_et = chrono::Utc::now().with_timezone(&Eastern);
                    let et_minutes = (now_et.hour() as i32 * 60) + now_et.minute() as i32;
                    let today = now_et.date_naive();
                    let (target_date, hour, minute, new_session, new_segment) =
                        if et_minutes < RTH_OPEN_ET {
                            (today, 9, 30, SessionType::Rth, DeltaSegment::Rth)
                        } else if (RTH_CLOSE_ET..GLOBEX_OPEN_ET).contains(&et_minutes) {
                            (today, 18, 0, SessionType::Globex, DeltaSegment::Asia)
                        } else if et_minutes >= GLOBEX_OPEN_ET {
                            let next_day = today.checked_add_days(Days::new(1)).unwrap_or(today);
                            (next_day, 9, 30, SessionType::Rth, DeltaSegment::Rth)
                        } else {
                            (today, 18, 0, SessionType::Globex, DeltaSegment::Asia)
                        };

                    if let Some(target_dt) = Eastern
                        .with_ymd_and_hms(
                            target_date.year(),
                            target_date.month(),
                            target_date.day(),
                            hour,
                            minute,
                            0,
                        )
                        .single()
                    {
                        let boundary_ts = target_dt.timestamp_millis() as f64;
                        let lookup_date = session_date_from_timestamp_ms(boundary_ts);
                        let cache_warm = boundary_cache_prewarm
                            .lock()
                            .ok()
                            .and_then(|cache| cache.cached.clone())
                            .map(|entry| {
                                entry.matches(
                                    &lookup_date,
                                    new_session,
                                    new_segment,
                                    &contract_metadata_prewarm.contract_symbol,
                                )
                            })
                            .unwrap_or(false);
                        if !cache_warm {
                            let db_for_load = Arc::clone(&db_prewarm);
                            let contract_for_load = contract_metadata_prewarm.clone();
                            let loaded = tokio::task::spawn_blocking(move || {
                                db_for_load.lock().ok().map(|db| {
                                    load_boundary_session_cache_entry(
                                        &db,
                                        new_session,
                                        new_segment,
                                        boundary_ts,
                                        &contract_for_load,
                                    )
                                })
                            })
                            .await
                            .ok()
                            .flatten();
                            if let Some(entry) = loaded {
                                if let Ok(mut cache) = boundary_cache_prewarm.lock() {
                                    cache.cached = Some(entry);
                                }
                                record_runtime_event(
                                    &runtime_events_prewarm,
                                    Some(&db_prewarm),
                                    RuntimeEventLevel::Info,
                                    "session.boundary_cache_prewarmed",
                                    "session",
                                    "Pre-warmed session boundary references.",
                                    serde_json::json!({
                                        "lookupDate": lookup_date,
                                        "newSession": format!("{:?}", new_session),
                                        "newSegment": format!("{:?}", new_segment),
                                        "contractSymbol": contract_metadata_prewarm.contract_symbol,
                                    }),
                                );
                            }
                        }
                    }

                    sleep(Duration::from_millis(PREWARM_INTERVAL_MS)).await;
                }
            });
        }

        tokio::spawn(async move {
            use tokio::time::{sleep, Duration};

            let coaching_root =
                RouterRoot::parse(&contract_metadata.root_symbol).unwrap_or(RouterRoot::Nq);

            let poll = Duration::from_millis(poll_ms.max(250));
            let mut caught_up = true;
            let mut offset: u64;
            let mut last_market_tick_ts: f64 = 0.0;
            let mut persist_counter: u64 = 0;
            let mut event_buffer = Vec::new();
            let mut analysis_event_buffer: Vec<MarketEvent> = Vec::new();
            let mut latest_analysis_snapshot = None;
            let mut last_analysis_market_ts = 0.0_f64;
            let mut ticks_since_analysis = 0usize;
            let pending_outcomes = Arc::new(Mutex::new(PendingOutcomeSet::default()));
            if let Ok(db) = db_bg.lock() {
                if let Ok(mut pending) = pending_outcomes.lock() {
                    let _ = pending.reconcile_from_db(&db);
                }
            }
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
            feed_rt_bg.scid_read_offset.store(offset, Ordering::Release);
            feed_rt_bg
                .scid_processed_offset
                .store(offset, Ordering::Release);

            loop {
                if caught_up {
                    sleep(poll).await;
                } else {
                    tokio::task::yield_now().await;
                }
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
                let poll_started_wall_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
                feed_rt_bg
                    .last_scid_poll_started_wall_ms
                    .store(poll_started_wall_ms, Ordering::Release);
                feed_rt_bg
                    .scid_worker_phase
                    .store(SCID_WORKER_READING, Ordering::Release);
                let step = match tokio::task::spawn_blocking(move || {
                    read_scid_poll_step_capped(&reader_for_step, offset, max_ticks_per_poll)
                })
                .await
                {
                    Ok(Ok(step)) => step,
                    Ok(Err(err)) => {
                        feed_rt_bg
                            .scid_worker_phase
                            .store(SCID_WORKER_IDLE, Ordering::Release);
                        feed_rt_bg.last_scid_poll_completed_wall_ms.store(
                            chrono::Utc::now().timestamp_millis().max(0) as u64,
                            Ordering::Release,
                        );
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
                        feed_rt_bg
                            .scid_worker_phase
                            .store(SCID_WORKER_IDLE, Ordering::Release);
                        feed_rt_bg.last_scid_poll_completed_wall_ms.store(
                            chrono::Utc::now().timestamp_millis().max(0) as u64,
                            Ordering::Release,
                        );
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
                feed_rt_bg
                    .scid_read_offset
                    .store(step.next_offset, Ordering::Release);
                feed_rt_bg.last_scid_poll_wall_ms.store(
                    chrono::Utc::now().timestamp_millis() as u64,
                    Ordering::Release,
                );
                feed_rt_bg
                    .scid_worker_phase
                    .store(SCID_WORKER_PROCESSING, Ordering::Release);

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
                let mut ticks_this_poll = 0u64;
                let batch_process_started = std::time::Instant::now();
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
                    let mut session_changed_for_context_warm = false;
                    if let Some(et_min) = et_minutes_from_timestamp(tick.timestamp_ms) {
                        let new_session = classify_session(et_min);
                        let new_segment = classify_delta_segment(et_min);
                        let session_changed = new_session != current_session;
                        session_changed_for_context_warm = session_changed;
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
                                &event_buffer,
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
                            if current_session == SessionType::Globex
                                && new_session == SessionType::Rth
                            {
                                persist_current_globex_volume_curve(
                                    &pipelines_bg,
                                    &db_bg,
                                    tick.timestamp_ms,
                                    &contract_metadata,
                                );
                            }
                            // Other known→known transitions, e.g. Globex→RTH at
                            // 09:30 ET. Reuses the shared boundary helper.
                            prepare_for_new_session_with_cache(
                                &pipelines_bg,
                                &db_bg,
                                Some(&runtime_events_bg),
                                &boundary_cache_bg,
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
                            prepare_for_new_session_with_cache(
                                &pipelines_bg,
                                &db_bg,
                                Some(&runtime_events_bg),
                                &boundary_cache_bg,
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
                    let ingest_outcome = ingest_tick(
                        &pipelines_bg,
                        &detector_bg,
                        &flow_emitter_bg,
                        Some(&pending_outcomes),
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
                    let new_events = if let Some(ingest_outcome) = ingest_outcome {
                        latest_analysis_snapshot = Some(ingest_outcome.snapshot);
                        let new_events = ingest_outcome.new_events;
                        analysis_event_buffer.extend(new_events.iter().cloned());
                        ticks_since_analysis = ticks_since_analysis.saturating_add(1);
                        new_events
                    } else {
                        Vec::new()
                    };
                    if !new_events.is_empty() {
                        market_router_bg.note_transition_events(coaching_root, &new_events);
                    }
                    market_router_bg.queue_journal_frames();
                    let should_run_analysis = latest_analysis_snapshot.is_some()
                        && (ticks_since_analysis >= analysis_max_ticks
                            || last_analysis_market_ts <= 0.0
                            || tick.timestamp_ms - last_analysis_market_ts
                                >= analysis_min_interval_ms);
                    if should_run_analysis {
                        let snapshot = latest_analysis_snapshot
                            .clone()
                            .expect("analysis snapshot checked above");
                        let events = std::mem::take(&mut analysis_event_buffer);
                        let rules_analysis = Arc::clone(&rules_bg);
                        let playbook_cache_analysis = Arc::clone(&playbook_cache_bg);
                        let db_analysis = Arc::clone(&db_bg);
                        let runtime_events_analysis = Arc::clone(&runtime_events_bg);
                        let pending_analysis = Arc::clone(&pending_outcomes);
                        let feed_rt_analysis = Arc::clone(&feed_rt_bg);
                        let router_analysis = Arc::clone(&market_router_bg);
                        let analysis_timestamp_ms = tick.timestamp_ms;
                        feed_rt_bg
                            .scid_worker_phase
                            .store(SCID_WORKER_ANALYSIS, Ordering::Release);
                        match tokio::task::spawn_blocking(move || {
                            run_analysis_pass(
                                &rules_analysis,
                                &playbook_cache_analysis,
                                &db_analysis,
                                &runtime_events_analysis,
                                Some(&pending_analysis),
                                &snapshot,
                                &events,
                                analysis_timestamp_ms,
                                AttentionPulseKind::EventDriven,
                                Some(router_analysis.as_ref()),
                            );
                            feed_rt_analysis.last_analysis_pass_wall_ms.store(
                                chrono::Utc::now().timestamp_millis().max(0) as u64,
                                Ordering::Release,
                            );
                        })
                        .await
                        {
                            Ok(()) => {
                                last_analysis_market_ts = tick.timestamp_ms;
                                ticks_since_analysis = 0;
                            }
                            Err(err) => {
                                record_runtime_event(
                                    &runtime_events_bg,
                                    Some(&db_bg),
                                    RuntimeEventLevel::Error,
                                    "scid.analysis_failed",
                                    "scid",
                                    "Coalesced SCID analysis pass failed to join.",
                                    serde_json::json!({
                                        "timestampMs": tick.timestamp_ms,
                                        "error": err.to_string(),
                                    }),
                                );
                            }
                        }
                        feed_rt_bg
                            .scid_worker_phase
                            .store(SCID_WORKER_PROCESSING, Ordering::Release);
                    }
                    if session_changed_for_context_warm
                        || new_events
                            .iter()
                            .any(|event| context_frame_warm_event(&event.event_type))
                    {
                        if let Some((timestamp_ms, payload)) =
                            build_live_feature_state_snapshot_payload(
                                &pipelines_bg,
                                &last_bid_bg,
                                &last_ask_bg,
                                tick.timestamp_ms,
                            )
                        {
                            let db_warm = Arc::clone(&db_bg);
                            let context_frame_cache_warm = Arc::clone(&context_frame_cache_bg);
                            let runtime_events_warm = Arc::clone(&runtime_events_bg);
                            tokio::task::spawn_blocking(move || {
                                warm_context_frame_cache(
                                    &db_warm,
                                    &context_frame_cache_warm,
                                    &runtime_events_warm,
                                    &payload,
                                    research::context_frame::ContextFrameOptions {
                                        mode: research::context_frame::ContextFrameMode::Live,
                                        snapshot_timestamp_ms: Some(timestamp_ms),
                                        include_historical: true,
                                        ..Default::default()
                                    },
                                );
                            });
                        }
                    }

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
                        feed_rt_bg
                            .scid_worker_phase
                            .store(SCID_WORKER_DB, Ordering::Release);
                        if let Ok(db) = db_bg.lock() {
                            let _ = db.insert_raw_ticks_batch(&tick_buffer);
                        }
                        feed_rt_bg
                            .scid_worker_phase
                            .store(SCID_WORKER_PROCESSING, Ordering::Release);
                        tick_buffer.clear();
                    }

                    ticks_this_poll += 1;
                }
                if ticks_since_analysis > 0 {
                    if let Some(snapshot) = latest_analysis_snapshot.clone() {
                        let events = std::mem::take(&mut analysis_event_buffer);
                        let rules_analysis = Arc::clone(&rules_bg);
                        let playbook_cache_analysis = Arc::clone(&playbook_cache_bg);
                        let db_analysis = Arc::clone(&db_bg);
                        let runtime_events_analysis = Arc::clone(&runtime_events_bg);
                        let pending_analysis = Arc::clone(&pending_outcomes);
                        let feed_rt_analysis = Arc::clone(&feed_rt_bg);
                        let router_analysis = Arc::clone(&market_router_bg);
                        let analysis_timestamp_ms = last_market_tick_ts;
                        feed_rt_bg
                            .scid_worker_phase
                            .store(SCID_WORKER_ANALYSIS, Ordering::Release);
                        match tokio::task::spawn_blocking(move || {
                            run_analysis_pass(
                                &rules_analysis,
                                &playbook_cache_analysis,
                                &db_analysis,
                                &runtime_events_analysis,
                                Some(&pending_analysis),
                                &snapshot,
                                &events,
                                analysis_timestamp_ms,
                                AttentionPulseKind::EventDriven,
                                Some(router_analysis.as_ref()),
                            );
                            feed_rt_analysis.last_analysis_pass_wall_ms.store(
                                chrono::Utc::now().timestamp_millis().max(0) as u64,
                                Ordering::Release,
                            );
                        })
                        .await
                        {
                            Ok(()) => {
                                last_analysis_market_ts = last_market_tick_ts;
                                ticks_since_analysis = 0;
                            }
                            Err(err) => {
                                record_runtime_event(
                                    &runtime_events_bg,
                                    Some(&db_bg),
                                    RuntimeEventLevel::Error,
                                    "scid.analysis_failed",
                                    "scid",
                                    "Forced SCID analysis pass failed to join.",
                                    serde_json::json!({
                                        "timestampMs": last_market_tick_ts,
                                        "error": err.to_string(),
                                    }),
                                );
                            }
                        }
                        feed_rt_bg
                            .scid_worker_phase
                            .store(SCID_WORKER_PROCESSING, Ordering::Release);
                    }
                }
                offset = step.next_offset;
                feed_rt_bg.scid_tail_offset.store(offset, Ordering::Release);
                feed_rt_bg
                    .scid_processed_offset
                    .store(offset, Ordering::Release);
                feed_rt_bg
                    .last_scid_batch_tick_count
                    .store(ticks_this_poll, Ordering::Release);

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
                    feed_rt_bg
                        .scid_worker_phase
                        .store(SCID_WORKER_DB, Ordering::Release);
                    if let Ok(db) = db_bg.lock() {
                        let _ = db.insert_market_events_batch_scoped(
                            Some(coaching_root.as_str()),
                            &event_buffer,
                        );
                    }
                    event_buffer.clear();
                }

                if ticks_this_poll > 0 {
                    let db_persist = Arc::clone(&db_bg);
                    let router_persist = Arc::clone(&market_router_bg);
                    if let Err(err) = tokio::task::spawn_blocking(move || match db_persist.lock() {
                        Ok(db) => {
                            if let Err(err) = router_persist.persist_journal(&db) {
                                tracing::warn!(error = %err, "scid.journal_persist");
                            }
                        }
                        Err(err) => {
                            tracing::warn!(error = %err, "scid.journal_persist_lock");
                        }
                    })
                    .await
                    {
                        tracing::warn!(error = %err, "scid.journal_persist_join");
                    }
                }

                // Flush remaining raw ticks
                if !tick_buffer.is_empty() {
                    feed_rt_bg
                        .scid_worker_phase
                        .store(SCID_WORKER_DB, Ordering::Release);
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
                let process_wall_ms = batch_process_started.elapsed().as_millis() as u64;
                feed_rt_bg
                    .last_scid_batch_process_wall_ms
                    .store(process_wall_ms, Ordering::Release);
                feed_rt_bg.last_scid_poll_completed_wall_ms.store(
                    chrono::Utc::now().timestamp_millis().max(0) as u64,
                    Ordering::Release,
                );
                feed_rt_bg
                    .scid_worker_phase
                    .store(SCID_WORKER_IDLE, Ordering::Release);
                caught_up = offset >= step.file_len;
            }
        });
    }

    // ~1s `.depth` poll: reconstruct book, persist compact DOM snapshots/features,
    // publish `domSummary`. SIL-M3f: does not bulk-append SQLite `depth_events`.
    if latest_depth_reader()?.is_some() {
        let pipelines_depth = Arc::clone(&server.pipelines);
        let db_depth = Arc::clone(&server.db);
        let last_bid_depth = Arc::clone(&server.last_bid);
        let last_ask_depth = Arc::clone(&server.last_ask);
        let feed_depth_rt = Arc::clone(&server.feed_runtime);
        let runtime_events_depth = Arc::clone(&server.runtime_events);
        let flow_emitter_depth = Arc::clone(&server.flow_emitter);
        let market_router_depth = Arc::clone(&server.market_router);

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
                            &flow_emitter_depth,
                            &last_bid_depth,
                            &last_ask_depth,
                            work,
                            feed_depth_rt.as_ref(),
                            Some(market_router_depth.as_ref()),
                            RouterRoot::Nq,
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

    let journal_router = std::sync::Arc::clone(&server.market_router);
    let journal_db = std::sync::Arc::clone(&server.db);
    let service = server.serve(stdio()).await?;
    service.waiting().await?;
    if !external_engine {
        match journal_db.lock() {
            Ok(d) => {
                if let Err(err) = journal_router.flush_journal_on_stop(&d) {
                    tracing::warn!(error = %err, "mcp.journal_persist_on_stop");
                }
            }
            Err(err) => tracing::warn!(error = %err, "mcp.journal_persist_lock_on_stop"),
        }
    }
    Ok(())
}

/// Build an isolated backtest database seeded with only reference tables and exit.
///
/// Invoked via `--seed-backtest-db --to <dest.db> [--from <live.db>]`. The replay
/// reads ticks from `.scid`, so the huge `raw_ticks` table is not copied — the
/// artifact stays tiny and the run is isolated from the live DB's writer.
fn seed_backtest_db_cli() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let arg_val = |flag: &str| -> Option<String> {
        args.iter()
            .position(|a| a == flag)
            .and_then(|i| args.get(i + 1))
            .cloned()
    };
    let from = arg_val("--from")
        .unwrap_or_else(|| data_dir().join("data.db").to_string_lossy().to_string());
    let to = match arg_val("--to") {
        Some(t) => t,
        None => {
            eprintln!("usage: the-desk-mcp --seed-backtest-db --to <dest.db> [--from <live.db>]");
            std::process::exit(2);
        }
    };
    println!("Seeding isolated backtest DB at {to} (reference tables from {from})...");
    let report = the_desk_backend::db::export_backtest_reference_db(&from, &to)?;
    for (table, rows) in &report {
        println!("  {table}: {rows} rows");
    }
    println!(
        "Done. raw_ticks was NOT copied — the replay reads ticks from .scid. \
         Point your backtest at {to}."
    );
    Ok(())
}

/// Seed the canonical PTT setup templates into the playbook DB and exit.
///
/// Invoked via `--seed-templates [--activate]`. Without `--activate`, templates
/// load inactive (review-then-arm). With `--activate`, missing templates are
/// inserted already armed. Idempotent: existing setups are never modified.
fn seed_templates_cli(activate: bool) -> Result<(), Box<dyn std::error::Error>> {
    let db_path = data_dir().join("data.db");
    let db = Database::open(&db_path.to_string_lossy())?;
    let report = the_desk_backend::rules::setup_templates::seed_templates(&db, activate)?;
    println!(
        "Seeded playbook templates into {} (activate={activate})",
        db_path.display()
    );
    println!(
        "  inserted ({}): {}",
        report.inserted.len(),
        report.inserted.join(", ")
    );
    println!(
        "  skipped existing ({}): {}",
        report.skipped_existing.len(),
        report.skipped_existing.join(", ")
    );
    if activate {
        println!("  inserted templates are ACTIVE and will evaluate live this session.");
    } else {
        println!(
            "  inserted templates are INACTIVE. Re-run with --activate to arm them directly, \
             or run them through the backtest loop before activating."
        );
    }
    Ok(())
}

/// Seed pinned Positioning interpretation exemplars into `agent_insights` and exit.
///
/// Invoked via `--seed-positioning-corpus`. Idempotent: existing insight ids are
/// never modified. Levels-Only backlog days and Slice-annotated sessions both
/// land as first-class Positioning teaching context (SIL-P-VS-c / #17).
fn seed_positioning_corpus_cli() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = data_dir().join("data.db");
    let db = Database::open(&db_path.to_string_lossy())?;
    let report = the_desk_backend::memory::seed_positioning_exemplar_corpus(&db)?;
    println!(
        "Seeded Positioning exemplar corpus `{}` into {} ({} sessions)",
        report.corpus_id,
        db_path.display(),
        report.session_count
    );
    println!(
        "  inserted ({}): {}",
        report.inserted.len(),
        report.inserted.join(", ")
    );
    println!(
        "  skipped existing ({}): {}",
        report.skipped_existing.len(),
        report.skipped_existing.join(", ")
    );
    println!(
        "  Recall via recall_agent_insights(category=\"positioning_annotation\", tag=\"positioning\") \
         or get_memory_brief."
    );
    Ok(())
}

async fn write_sil_m0_baseline_async() -> Result<(), Box<dyn std::error::Error>> {
    let surface = TheDeskMcp::tool_router().list_all().len();
    let db = Database::open(":memory:")?;
    let server = TheDeskMcp::with_runtime_events(
        db,
        PipelineEngine::new(),
        ":memory:".into(),
        Arc::new(RuntimeEventStore::new(
            &the_desk_backend::observability::LoggingConfig {
                destination: "none".to_string(),
                ..the_desk_backend::observability::LoggingConfig::default()
            },
        )),
    );
    // One cold orientation-chain probe — durable numeric before-figure for M1b.
    let probe = tool_telemetry::ToolTelemetry::new();
    for tool in tool_telemetry::ORIENTATION_CHAIN {
        let result = match *tool {
            "get_session_context" => server.get_session_context().await,
            "get_market_snapshot" => server.get_market_snapshot().await,
            "get_risk_state" => server.get_risk_state().await,
            "get_risk_config" => server.get_risk_config().await,
            "get_account_state" => server.get_account_state().await,
            other => {
                return Err(format!("ORIENTATION_CHAIN tool not wired for probe: {other}").into())
            }
        };
        let result =
            result.map_err(|e| format!("orientation-chain probe failed for `{tool}`: {e}"))?;
        probe.record(tool, &Ok(result));
    }
    let baseline = tool_telemetry::baseline_with_orientation_probe(surface, &probe);
    if !baseline.orientation_chain_cost.fully_observed {
        return Err("orientation-chain probe did not observe every chain tool".into());
    }
    if baseline
        .per_tool
        .values()
        .any(|stats| stats.error_count > 0)
    {
        return Err("orientation-chain probe recorded tool errors; refusing baseline write".into());
    }
    let path = tool_telemetry::checked_in_baseline_path();
    tool_telemetry::write_snapshot_file(&path, &baseline)?;
    let verified = tool_telemetry::read_snapshot_file(&path)?;
    if verified.tool_surface_count != surface {
        return Err("SIL-M0 baseline round-trip surface count mismatch".into());
    }
    println!(
        "Wrote SIL-M0 telemetry baseline ({} tools on surface; orientation ≈{} tokens / {} calls) to {}",
        surface,
        baseline.orientation_chain_cost.total_approx_tokens,
        baseline.orientation_chain_cost.call_count,
        path.display()
    );
    Ok(())
}
