//! Trade entries, fills import, journal notes, and trade review.

use chrono::Utc;
use chrono_tz::Tz;
use rmcp::{
    handler::server::wrapper::Parameters, model::*, tool, tool_router, ErrorData as McpError,
};
use std::collections::HashMap;
use the_desk_backend::db::{
    ImportedFillRecord, JournalEntry, SessionRecord, TradeImportBatchRecord, TradeRecord,
    TradeReviewUpdate,
};
use the_desk_backend::memory::mark_memory_dirty as memory_mark_dirty;
use the_desk_backend::risk::{RiskConfig, RiskTracker};
use the_desk_backend::trading_day_from_timestamp_ms;

#[allow(unused_imports)]
use crate::{helpers::*, lifecycle::*, params::*, state::*};

#[tool_router(router = journal_router, vis = "pub(crate)")]
impl TheDeskMcp {
    #[tool(
        description = "Create or update a trade journal entry. Supports manual chat-first trade logging as well as imported-fill normalization. If session_id is omitted, the latest open session is used when available."
    )]
    pub(crate) async fn upsert_trade_entry(
        &self,
        Parameters(params): Parameters<UpsertTradeEntryParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;
        let entry_time = params
            .entry_time_ms
            .unwrap_or_else(|| Utc::now().timestamp_millis() as f64);
        let session_id = resolve_session_id(&db, params.session_id.as_deref())?;
        let direction = params.direction.clone();
        let risk_config = db.load_risk_config().map_err(db_error)?;
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
            planned_r_points_at_entry: Some(risk_config.r_value_points),
            planned_r_dollars_at_entry: Some(risk_config.r_value_dollars),
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
    pub(crate) async fn close_trade_entry(
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
    pub(crate) async fn review_trade_entry(
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
    pub(crate) async fn save_journal_entry(
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
    pub(crate) async fn list_trade_entries(
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
    pub(crate) async fn get_trade_entry(
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
    pub(crate) async fn get_session_journal(
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
    pub(crate) async fn get_recent_journal_notes(
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
    pub(crate) async fn get_session_review_context(
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
    pub(crate) async fn query_journal_patterns(
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
        description = "Import broker-exported fills into the trade journal. Accepts an array of fill rows, skips duplicates idempotently, stores raw import rows, and synthesizes normalized round-trip trade entries."
    )]
    pub(crate) async fn import_trade_fills(
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
    pub(crate) async fn record_trade_result(
        &self,
        Parameters(params): Parameters<RecordTradeResultParams>,
    ) -> Result<CallToolResult, McpError> {
        let db = self.db.lock().map_err(|_| lock_error())?;

        // 1. Insert trade record
        let trade_id = uuid::Uuid::new_v4().to_string();
        let now_ms = chrono::Utc::now().timestamp_millis() as f64;
        let config = db.load_risk_config().map_err(db_error)?;
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
            planned_r_points_at_entry: Some(config.r_value_points),
            planned_r_dollars_at_entry: Some(config.r_value_dollars),
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
}
