//! Operations: feed health, ingestion, rollover, archival, data integrity.

use rmcp::{
    handler::server::wrapper::Parameters, model::*, tool, tool_router, ErrorData as McpError,
};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use the_desk_backend::backfill;
use the_desk_backend::db::Database;
use the_desk_backend::feed::scid_reader::ScidReader;
use the_desk_backend::feed::{load_feed_config, load_storage_config, resolve_contract_metadata};
use the_desk_backend::observability::{RuntimeEvent, RuntimeEventFilter, RuntimeEventLevel};
use the_desk_backend::rollover::PriorReferenceTrust;
use the_desk_backend::scid_tick_ingest::{self, TickIngestParams};
use the_desk_backend::scid_timestamp_diagnostics;

#[allow(unused_imports)]
use crate::{helpers::*, lifecycle::*, params::*, state::*};

#[tool_router(router = admin_router, vis = "pub(crate)")]
impl TheDeskMcp {
    #[tool(
        description = "Query raw tick data. Without filters, returns the most recent ticks (most-recent first). With start_time_ms/end_time_ms, returns ticks in that time window in chronological order (ASC) — ideal for reconstructing the tape at a specific moment. With price_low/price_high, limits to trades in that price range. With session_date (YYYY-MM-DD), limits to that trading day. All filters can be combined. Use get_market_snapshot to get the current timestamp_ms and work backward from there."
    )]
    pub(crate) async fn query_ticks(
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
        description = "Feed health diagnostics: SCID path status, file metadata, latest DB tick timestamp, ingest lag, freshness/source state, and contract rollover status."
    )]
    pub(crate) async fn get_feed_health(&self) -> Result<CallToolResult, McpError> {
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
    pub(crate) async fn get_runtime_events(
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
    pub(crate) async fn get_contract_rollover_status(&self) -> Result<CallToolResult, McpError> {
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
    pub(crate) async fn validate_contract_rollover(&self) -> Result<CallToolResult, McpError> {
        self.get_contract_rollover_status().await
    }

    #[tool(
        description = "Queue a historical backfill job and return a job id. Processes past sessions through all 14 pipelines, detects market events, and persists session summaries without blocking the MCP server."
    )]
    pub(crate) async fn backfill_history(
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
    pub(crate) async fn get_raw_tick_ingest_gaps(
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
    pub(crate) async fn scan_scid_timestamp_anomalies(
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
    pub(crate) async fn ingest_raw_ticks_from_scid(
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
        description = "Storage tier status: shows hot (current session), warm (SQLite ticks), and cold (archived) tier sizes. Includes session summary count and last archive date. Use to monitor data lifecycle."
    )]
    pub(crate) async fn archive_status(&self) -> Result<CallToolResult, McpError> {
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
        description = "Validate data integrity: checks tick count, stream freshness, contract rollover status, and pipeline consistency invariants (POC within VA, VA contains ~70%% of TPOs, delta sum consistency). Returns pass/fail status with details."
    )]
    pub(crate) async fn validate_data_integrity(&self) -> Result<CallToolResult, McpError> {
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
        if let Ok(db) = self.db.lock() {
            if let Ok(report) = db.signal_outcome_integrity_report(None, None, None) {
                let passed = report.get("status").and_then(|v| v.as_str()) != Some("failed");
                checks["signalOutcomes"] = serde_json::json!({
                    "passed": passed,
                    "status": report.get("status"),
                    "totalRows": report.get("totalRows"),
                    "qualityCounts": report.get("qualityCounts"),
                });
                invariants_ok &= passed;
            }
        }
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
