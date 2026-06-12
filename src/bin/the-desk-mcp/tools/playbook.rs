//! Playbook evaluation, setup lifecycle, attention signals, and trade idea cards.

use rmcp::{
    handler::server::wrapper::Parameters, model::*, tool, tool_router, ErrorData as McpError,
};
use std::sync::atomic::Ordering;
use the_desk_backend::db::{AttentionChangelogQuery, AttentionSignalQuery, TradeIdeaQuery};
use the_desk_backend::observability::RuntimeEventLevel;

#[allow(unused_imports)]
use crate::{helpers::*, lifecycle::*, params::*, state::*};

#[tool_router(router = playbook_router, vis = "pub(crate)")]
impl TheDeskMcp {
    #[tool(
        description = "Evaluate all active playbook setups against current market state. Returns per-setup status (conditionsMet, approaching, notActive) and recent signal count. Always frames results as 'your playbook says...' -- never advisory."
    )]
    pub(crate) async fn evaluate_playbook(&self) -> Result<CallToolResult, McpError> {
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
    pub(crate) async fn get_setup_state_history(
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
    pub(crate) async fn acknowledge_setup_prompt(
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
    pub(crate) async fn mark_setup_in_trade(
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
    pub(crate) async fn close_setup_state(
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
    pub(crate) async fn get_attention_inbox(
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
    pub(crate) async fn get_signal_detail(
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
    pub(crate) async fn acknowledge_attention_signal(
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
    pub(crate) async fn what_changed_since(
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
    pub(crate) async fn get_attention_changelog(
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
    pub(crate) async fn get_active_trade_ideas(
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
    pub(crate) async fn mark_trade_idea_confirmed(
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
    pub(crate) async fn mark_trade_idea_invalidated(
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
    pub(crate) async fn mark_trade_idea_in_trade(
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
    pub(crate) async fn mark_trade_idea_resolved(
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
        description = "Full setup context for a named setup. Returns all computed data relevant to that setup type: OR5 levels, delta confirmation, RVOL, day type, nearby zones, risk state. One call = everything needed to discuss a potential trade."
    )]
    pub(crate) async fn get_setup_context(
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
}
