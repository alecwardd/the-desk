//! Trader memory: agent insights, behavioral patterns, follow-ups, briefings.

use chrono::Utc;
use rmcp::{
    handler::server::wrapper::Parameters, model::*, tool, tool_router, ErrorData as McpError,
};
use the_desk_backend::mcp::memory::TraderContextFitParams;
use the_desk_backend::memory::trader_context::{
    build_trader_context_fit as memory_build_trader_context_fit, TraderContextFitQuery,
    TraderContextIntent,
};
use the_desk_backend::memory::{
    build_memory_brief as memory_build_memory_brief,
    detect_behavioral_patterns as memory_detect_behavioral_patterns,
    refresh_memory_state as memory_refresh_state, save_agent_insight as memory_save_agent_insight,
    AgentInsightQuery, BehavioralPatternQuery, MemoryBriefQuery, MemoryFollowupRecord,
    MemoryRefreshOptions, SaveAgentInsightInput,
};

#[allow(unused_imports)]
use crate::{helpers::*, lifecycle::*, params::*, state::*};

#[tool_router(router = memory_router, vis = "pub(crate)")]
impl TheDeskMcp {
    #[tool(
        description = "Save an agent-authored memory insight. New insights start as candidate unless they are reinforced by a matching prior insight or explicitly backed by patternIds in evidence."
    )]
    pub(crate) async fn save_agent_insight(
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
    pub(crate) async fn recall_agent_insights(
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
    pub(crate) async fn acknowledge_agent_insight(
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
    pub(crate) async fn supersede_agent_insight(
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
    pub(crate) async fn detect_behavioral_patterns(&self) -> Result<CallToolResult, McpError> {
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
    pub(crate) async fn refresh_memory_state(
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
    pub(crate) async fn get_behavioral_patterns(
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
    pub(crate) async fn create_memory_followup(
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
    pub(crate) async fn resolve_memory_followup(
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
    pub(crate) async fn get_memory_brief(
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
    pub(crate) async fn get_pre_session_briefing(
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
                session_id: params.session_id.clone(),
                setup_id: params.setup_id.clone(),
                session_type: params.session_type.clone(),
                session_segment: params.session_segment.clone(),
                day_type: params.day_type.clone(),
                time_bucket: params.time_bucket.clone(),
                pre_session_note: params.pre_session_note.clone(),
                limit: params.limit.map(|limit| limit.min(10) as usize),
                include_recent_sessions: params.include_recent_sessions,
                include_patterns: Some(false),
                include_insights: Some(false),
                include_followups: Some(false),
            },
        )
        .map_err(db_error)?;
        let trader_context_fit = memory_build_trader_context_fit(
            &db,
            TraderContextFitQuery {
                intent: TraderContextIntent::SessionStart,
                session_id: params.session_id,
                setup_id: params.setup_id,
                session_type: params.session_type,
                session_segment: params.session_segment,
                day_type: params.day_type,
                time_bucket: params.time_bucket,
                timestamp_ms: Some(Utc::now().timestamp_millis() as f64),
                include_opportunity: Some(true),
                include_coaching_memory: Some(true),
                ..TraderContextFitQuery::default()
            },
        )
        .map_err(db_error)?;
        let mut memory_brief_json = serde_json::to_value(memory_brief).map_err(db_error)?;
        if let Some(obj) = memory_brief_json.as_object_mut() {
            let delegated_patterns = if params.include_patterns.unwrap_or(true) {
                trader_context_fit
                    .execution_fit
                    .get("matchingSlices")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!([]))
            } else {
                serde_json::json!([])
            };
            let delegated_insights = if params.include_insights.unwrap_or(true) {
                trader_context_fit
                    .coaching_memory
                    .get("insights")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!([]))
            } else {
                serde_json::json!([])
            };
            let delegated_followups = if params.include_followups.unwrap_or(true) {
                trader_context_fit
                    .coaching_memory
                    .get("followups")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!([]))
            } else {
                serde_json::json!([])
            };
            if let Some(summary) = obj
                .get_mut("summary")
                .and_then(|value| value.as_object_mut())
            {
                summary.insert(
                    "patternCount".to_string(),
                    serde_json::json!(delegated_patterns.as_array().map(Vec::len).unwrap_or(0)),
                );
                summary.insert(
                    "insightCount".to_string(),
                    serde_json::json!(delegated_insights.as_array().map(Vec::len).unwrap_or(0)),
                );
                summary.insert(
                    "followupCount".to_string(),
                    serde_json::json!(delegated_followups.as_array().map(Vec::len).unwrap_or(0)),
                );
                summary.insert(
                    "topPatternType".to_string(),
                    delegated_patterns
                        .as_array()
                        .and_then(|values| values.first())
                        .and_then(|value| value.get("patternType"))
                        .cloned()
                        .unwrap_or(serde_json::Value::Null),
                );
                summary.insert(
                    "topInsightStatus".to_string(),
                    delegated_insights
                        .as_array()
                        .and_then(|values| values.first())
                        .and_then(|value| value.get("status"))
                        .cloned()
                        .unwrap_or(serde_json::Value::Null),
                );
                if let Some(requested_sections) = summary
                    .get_mut("requestedSections")
                    .and_then(|value| value.as_object_mut())
                {
                    requested_sections.insert(
                        "patterns".to_string(),
                        serde_json::json!(params.include_patterns.unwrap_or(true)),
                    );
                    requested_sections.insert(
                        "insights".to_string(),
                        serde_json::json!(params.include_insights.unwrap_or(true)),
                    );
                    requested_sections.insert(
                        "followups".to_string(),
                        serde_json::json!(params.include_followups.unwrap_or(true)),
                    );
                }
            }
            obj.insert("patterns".to_string(), delegated_patterns);
            obj.insert("insights".to_string(), delegated_insights);
            obj.insert("followups".to_string(), delegated_followups);
        }
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
            "memoryBrief": memory_brief_json,
            "traderContextFit": trader_context_fit,
            "accountState": account_state,
            "riskState": risk_state,
            "memoryAutoRefreshed": memory_auto_refreshed,
            "rolloverStatus": rollover_status
        })))
    }

    #[tool(
        description = "Typed trader memory context fit. Separates executed-trade memory, setup opportunity context, coaching reminders, live post-loss/ordinal state, reliability, and provenance. Memory reports context only and must not drive sizing by itself."
    )]
    pub(crate) async fn get_trader_context_fit(
        &self,
        Parameters(params): Parameters<TraderContextFitParams>,
    ) -> Result<CallToolResult, McpError> {
        let live_view = self.resolve_live_market_view();
        let snapshot = live_view.as_ref().map(|view| &view.snapshot);
        let intent = params
            .intent
            .as_deref()
            .unwrap_or("setupCheck")
            .parse::<TraderContextIntent>()
            .map_err(db_error)?;
        let query = TraderContextFitQuery {
            intent,
            setup_id: params.setup_id,
            session_id: params.session_id,
            trade_account: params.trade_account,
            trading_day: params.trading_day,
            timestamp_ms: params
                .timestamp_ms
                .or_else(|| live_view.as_ref().map(|view| view.as_of_timestamp_ms)),
            session_type: params.session_type.or_else(|| {
                snapshot
                    .and_then(|s| s.get("sessionType"))
                    .and_then(|value| value.as_str())
                    .map(str::to_string)
            }),
            session_segment: params.session_segment.or_else(|| {
                snapshot
                    .and_then(|s| s.get("sessionSegment"))
                    .and_then(|value| value.as_str())
                    .map(str::to_string)
            }),
            time_bucket: params.time_bucket,
            day_type: params.day_type.or_else(|| {
                snapshot
                    .and_then(|s| s.get("dayType"))
                    .and_then(|value| value.as_str())
                    .map(str::to_string)
            }),
            profile_shape: params.profile_shape,
            balance_state: params.balance_state,
            include_opportunity: params.include_opportunity,
            include_coaching_memory: params.include_coaching_memory,
            context_snapshot: snapshot.cloned(),
        };
        let db = self.db.lock().map_err(|_| lock_error())?;
        let fit = memory_build_trader_context_fit(&db, query).map_err(db_error)?;
        Ok(text_result(serde_json::json!({
            "traderContextFit": fit,
            "snapshotSource": live_view.as_ref().map(|view| view.snapshot_source),
            "dataAgeMs": live_view.as_ref().map(|view| view.data_age_ms)
        })))
    }
}
