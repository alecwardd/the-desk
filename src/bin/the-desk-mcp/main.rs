//! The Desk MCP server binary.
//!
//! Thin entry point: module wiring lives here; all behavior is in the sibling
//! modules. Tools are grouped by domain under `tools/` and combined into a
//! single router in `service.rs`. See docs/mcp/README.md for the architecture.

use rmcp::{transport::stdio, ServiceExt};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use the_desk_backend::db::{Database, RawTickBatchRow};
use the_desk_backend::feed::monotonic::{MonotonicTickGuard, MonotonicTimestampDecision};
use the_desk_backend::feed::scid_reader::ScidReader;
use the_desk_backend::feed::{load_feed_config, resolve_contract_metadata, TradeSide};
use the_desk_backend::observability::{
    init_logging, load_logging_config, RuntimeEventLevel, RuntimeEventStore,
};
use the_desk_backend::pipelines::{PipelineEngine, RvolPipeline};
use the_desk_backend::research;
use the_desk_backend::{
    classify_delta_segment, classify_session, et_minutes_from_timestamp, globex_open_ms,
    session_date_from_timestamp_ms, DeltaSegment, SessionType,
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
        let context_frame_cache_bg = Arc::clone(&server.context_frame_cache);
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
                    let event_start = event_buffer.len();
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
                    if session_changed_for_context_warm
                        || event_buffer[event_start..]
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
                            warm_context_frame_cache(
                                &db_bg,
                                &context_frame_cache_bg,
                                &runtime_events_bg,
                                &payload,
                                research::context_frame::ContextFrameOptions {
                                    mode: research::context_frame::ContextFrameMode::Live,
                                    snapshot_timestamp_ms: Some(timestamp_ms),
                                    include_historical: true,
                                    ..Default::default()
                                },
                            );
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
