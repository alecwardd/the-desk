//! Historical research: hypotheses, backtests, frequency/conditional/distribution queries.

use rmcp::{
    handler::server::wrapper::Parameters, model::*, tool, tool_router, ErrorData as McpError,
};
use std::sync::atomic::Ordering;
use the_desk_backend::backfill;
use the_desk_backend::feed::load_feed_config;
use the_desk_backend::feed::scid_reader::ScidReader;
use the_desk_backend::mcp::hypotheses::{
    ActivateDraftSetupParams, HypothesisRunParams, ListHypothesesParams, RegisterHypothesisParams,
    SetHypothesisLifecycleParams,
};
use the_desk_backend::observability::RuntimeEventLevel;
use the_desk_backend::research;
use the_desk_backend::research::hypothesis::{
    activate_draft_setup as hypothesis_activate_draft_setup,
    register_hypothesis as hypothesis_register_hypothesis,
    set_hypothesis_lifecycle as hypothesis_set_lifecycle,
    summarize_hypothesis_run as hypothesis_summarize_run, HypothesisMetadata,
    RegisterHypothesisRequest,
};
use the_desk_backend::rules::SetupDefinition;

#[allow(unused_imports)]
use crate::{helpers::*, lifecycle::*, params::*, state::*};

#[tool_router(router = research_router, vis = "pub(crate)")]
impl TheDeskMcp {
    #[tool(
        description = "Register or dry-run validate a research hypothesis as an inactive per-version SetupDefinition. First-slice scope is RTH only; use run_backtest with the returned setupId to execute."
    )]
    pub(crate) async fn register_hypothesis(
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
    pub(crate) async fn summarize_hypothesis_run(
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
    pub(crate) async fn list_hypotheses(
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
    pub(crate) async fn propose_draft_setup(
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
    pub(crate) async fn activate_draft_setup(
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
    pub(crate) async fn set_hypothesis_lifecycle(
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
    pub(crate) async fn run_backtest(
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
    pub(crate) async fn get_backfill_status(
        &self,
        Parameters(params): Parameters<BackfillStatusParams>,
    ) -> Result<CallToolResult, McpError> {
        match self.get_job_run(params.job_id.as_deref()).await? {
            Some(run) => Ok(text_result(historical_job_response(&run, false))),
            None => Ok(no_data("No historical job found")),
        }
    }

    #[tool(description = "Cancel an in-flight historical backfill or backtest job.")]
    pub(crate) async fn cancel_backfill(
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
    pub(crate) async fn get_backtest_results(
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
    pub(crate) async fn compare_backtests(
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
    pub(crate) async fn compare_sessions(
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
    pub(crate) async fn query_event_frequency(
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
    pub(crate) async fn query_conditional(
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
    pub(crate) async fn query_distribution(
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
    pub(crate) async fn query_signal_outcome_distribution(
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
        let source = parse_optional_signal_source(params.source.as_deref())?;
        let db = self.db.lock().map_err(|_| lock_error())?;
        match research::signal_outcome_distribution(
            &db,
            &setup_id,
            params.start_date.as_deref(),
            params.end_date.as_deref(),
            scope.as_ref(),
            source,
            params.job_id.as_deref(),
            params.include_unverified.unwrap_or(true),
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
    pub(crate) async fn query_signal_outcome_conditional(
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
        let source = parse_optional_signal_source(params.source.as_deref())?;
        let db = self.db.lock().map_err(|_| lock_error())?;
        match research::signal_outcome_conditional(
            &db,
            &setup_id,
            &session_field,
            &field_value,
            params.start_date.as_deref(),
            params.end_date.as_deref(),
            scope.as_ref(),
            source,
            params.job_id.as_deref(),
            params.include_unverified.unwrap_or(true),
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
    pub(crate) async fn query_signal_outcome_excursions(
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
        let source = parse_optional_signal_source(params.source.as_deref())?;
        let db = self.db.lock().map_err(|_| lock_error())?;
        match research::signal_outcome_excursions(
            &db,
            setup_id.as_deref(),
            params.start_date.as_deref(),
            params.end_date.as_deref(),
            scope.as_ref(),
            source,
            params.job_id.as_deref(),
            params.include_unverified.unwrap_or(true),
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
    pub(crate) async fn get_session_history(
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
                            "ibExtensionState": s.ib_extension_state,
                            "firstIbExtensionDirection": s.first_ib_extension_direction,
                            "firstIbExtensionTimestampMs": s.first_ib_extension_timestamp_ms,
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
    pub(crate) async fn get_signal_performance(
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
        description = "Validate signal_outcomes integrity for research/backtest trust. Returns quality counts, failed invariant counts, legacy ratio, and ok/warning/failed status. Filter by source, jobId, or setupId before running setup studies."
    )]
    pub(crate) async fn validate_signal_outcome_integrity(
        &self,
        Parameters(params): Parameters<SignalOutcomeIntegrityParams>,
    ) -> Result<CallToolResult, McpError> {
        let source = params
            .source
            .as_deref()
            .map(|raw| {
                let normalized = raw.trim().to_ascii_lowercase();
                match normalized.as_str() {
                    "live" | "backtest" | "backfill" => Ok(normalized),
                    _ => Err(invalid_params_error(format!(
                        "source must be one of live|backtest|backfill, got: {raw}"
                    ))),
                }
            })
            .transpose()?;
        let db = self.db.lock().map_err(|_| lock_error())?;
        match db.signal_outcome_integrity_report(
            source.as_deref(),
            params.job_id.as_deref(),
            params.setup_id.as_deref(),
        ) {
            Ok(report) => Ok(text_result(report)),
            Err(e) => Err(db_error(e)),
        }
    }

    #[tool(
        description = "Per-setup performance matrix in one call. Returns aggregated setup metrics: total/resolved/pending counts, target/stop/time-exit breakdown, win rate, avg R, avg winner/loser R. Supports date + session scope filters, minimum resolved threshold, sorting, and limit."
    )]
    pub(crate) async fn get_setup_performance_matrix(
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
    pub(crate) async fn get_research_summary(&self) -> Result<CallToolResult, McpError> {
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
}
