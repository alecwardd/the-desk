use crate::db::{Database, SessionSummary, SignalOutcome};
use crate::feed::scid_reader::ScidReader;
use crate::feed::TradeSide;
use crate::outcome_tracker;
use crate::pipelines::{
    EventDetector, FlowEventEmitter, MarketState, PipelineEngine, RvolPipeline,
};
use crate::rules::RulesEngine;
use crate::{
    classify_session, et_minutes_from_timestamp, minute_of_session_from_timestamp,
    session_date_from_timestamp_ms, SessionType,
};
use serde::{Deserialize, Serialize};

/// Configuration for backfill, including optional rules engine replay.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackfillConfig {
    /// When true, run the rules engine on each tick and track signal outcomes.
    pub run_rules: bool,
    /// Optional setup IDs to evaluate. If None, all active setups are used.
    pub setup_ids: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TickGap {
    pub from_ms: f64,
    pub to_ms: f64,
    pub duration_minutes: f64,
    pub session_date: String,
    pub session_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackfillResult {
    pub sessions_processed: usize,
    pub sessions_skipped: usize,
    pub total_ticks: usize,
    pub total_events: usize,
    pub session_dates: Vec<String>,
    pub gaps: Vec<TickGap>,
    /// When run_rules was true: total signals fired during backfill.
    pub signals_fired: Option<usize>,
    /// When run_rules was true: backtest run ID if persisted.
    pub backtest_run_id: Option<String>,
}

/// Build a SessionSummary from the final MarketState of an RTH session.
pub fn summary_from_state(
    state: &MarketState,
    session_date: &str,
    session_type: &str,
    open_price: f64,
    tick_count: i64,
    total_volume: f64,
    signal_count: i64,
) -> SessionSummary {
    let session_close = if state.rth_close_price > 0.0 {
        state.rth_close_price
    } else {
        state.last_price
    };
    let ib_mid = if state.ib_high > 0.0 && state.ib_low > 0.0 {
        (state.ib_high + state.ib_low) / 2.0
    } else {
        0.0
    };

    let close_vs_ib_mid = if ib_mid <= 0.0 {
        "n/a".to_string()
    } else if session_close > ib_mid + 0.25 {
        "above".to_string()
    } else if session_close < ib_mid - 0.25 {
        "below".to_string()
    } else {
        "at".to_string()
    };

    let close_vs_vwap = if state.vwap <= 0.0 {
        "n/a".to_string()
    } else if session_close > state.vwap + 0.25 {
        "above".to_string()
    } else if session_close < state.vwap - 0.25 {
        "below".to_string()
    } else {
        "at".to_string()
    };

    let close_vs_poc = if state.poc <= 0.0 {
        "n/a".to_string()
    } else if session_close > state.poc + 0.25 {
        "above".to_string()
    } else if session_close < state.poc - 0.25 {
        "below".to_string()
    } else {
        "at".to_string()
    };

    SessionSummary {
        session_date: session_date.to_string(),
        session_type: session_type.to_string(),
        open_price,
        high: state.session_high,
        low: state.session_low,
        close: session_close,
        poc: state.poc,
        vah: state.va_high,
        val: state.va_low,
        ib_high: state.ib_high,
        ib_low: state.ib_low,
        ib_range: if state.ib_high > 0.0 && state.ib_low > 0.0 {
            state.ib_high - state.ib_low
        } else {
            0.0
        },
        ib_mid,
        or_high: state.or_high,
        or_low: state.or_low,
        day_type: format!("{:?}", state.day_type),
        profile_shape: format!("{:?}", state.profile_shape),
        balance_state: format!("{:?}", state.balance_state),
        total_volume,
        tick_count,
        session_delta: state.session_delta,
        cumulative_delta: state.cumulative_delta,
        dnp: state.dnp,
        dnva_high: state.dnva_high,
        dnva_low: state.dnva_low,
        vwap_close: state.vwap,
        signal_count,
        single_prints_direction: format!("{:?}", state.single_prints_direction),
        excess_high: state.excess_high,
        excess_low: state.excess_low,
        poor_high: state.poor_high,
        poor_low: state.poor_low,
        rvol_ratio: state.rvol_ratio,
        close_vs_ib_mid,
        close_vs_vwap,
        close_vs_poc,
        snapshot_json: serde_json::to_string(state).ok(),
    }
}

/// Process historical .scid data through all pipelines, detect events,
/// and persist session summaries. Optionally run rules engine for backtest replay.
pub fn run_backfill(
    reader: &ScidReader,
    db: &Database,
    since_ms: Option<f64>,
    force: bool,
    config: Option<BackfillConfig>,
) -> Result<BackfillResult, String> {
    let ticks = reader
        .read_bulk_since(since_ms)
        .map_err(|e| format!("SCID read error: {e}"))?;

    if ticks.is_empty() {
        return Ok(BackfillResult {
            sessions_processed: 0,
            sessions_skipped: 0,
            total_ticks: 0,
            total_events: 0,
            session_dates: Vec::new(),
            gaps: Vec::new(),
            signals_fired: None,
            backtest_run_id: None,
        });
    }

    let run_rules = config.as_ref().map(|c| c.run_rules).unwrap_or(false);
    let setup_ids_filter = config.as_ref().and_then(|c| c.setup_ids.clone());
    let mut rules = if run_rules {
        Some(RulesEngine::default())
    } else {
        None
    };
    let mut signals_fired_this_session = 0_i64;
    let mut total_signals_fired = 0_usize;

    let mut pipeline = PipelineEngine::new();
    let mut detector = EventDetector::new();
    let mut flow_emitter = FlowEventEmitter::new();
    let mut rvol_curves: Vec<Vec<f64>> = db
        .recent_rth_session_volumes(20)
        .unwrap_or_default()
        .into_iter()
        .map(RvolPipeline::curve_from_total_volume)
        .collect();
    pipeline.rvol.load_historical_curve(&rvol_curves);
    let mut current_session = SessionType::Unknown;
    let mut current_date = String::new();
    let mut session_open_price = 0.0_f64;
    let mut session_tick_count = 0_i64;
    let mut session_volume = 0.0_f64;
    let mut event_buffer = Vec::new();
    let mut sessions_processed = 0_usize;
    let mut sessions_skipped = 0_usize;
    let mut total_events = 0_usize;
    let mut session_dates = Vec::new();
    let mut gaps: Vec<TickGap> = Vec::new();
    let mut prev_ts: Option<f64> = None;
    let mut prev_class = SessionType::Unknown;

    for tick in &ticks {
        let tick_class = et_minutes_from_timestamp(tick.timestamp_ms)
            .map(classify_session)
            .unwrap_or(SessionType::Unknown);
        if let Some(prev) = prev_ts {
            let gap_ms = tick.timestamp_ms - prev;
            if gap_ms > 0.0 && tick_class == prev_class {
                let threshold_ms = match tick_class {
                    SessionType::Rth => 5.0 * 60_000.0,
                    SessionType::Globex => 30.0 * 60_000.0,
                    SessionType::Unknown => f64::INFINITY,
                };
                if gap_ms > threshold_ms {
                    gaps.push(TickGap {
                        from_ms: prev,
                        to_ms: tick.timestamp_ms,
                        duration_minutes: gap_ms / 60_000.0,
                        session_date: session_date_from_timestamp_ms(tick.timestamp_ms),
                        session_type: format!("{tick_class:?}"),
                    });
                }
            }
        }
        prev_ts = Some(tick.timestamp_ms);
        prev_class = tick_class;

        let date = session_date_from_timestamp_ms(tick.timestamp_ms);

        if let Some(et_min) = et_minutes_from_timestamp(tick.timestamp_ms) {
            let new_session = classify_session(et_min);

            if new_session != current_session
                && current_session != SessionType::Unknown
                && new_session != SessionType::Unknown
            {
                // Session boundary: finalize the outgoing session
                if current_session == SessionType::Rth && !current_date.is_empty() {
                    let should_process =
                        force || !db.has_session_summary(&current_date).unwrap_or(true);

                    if should_process {
                        if force {
                            let _ = db.purge_session_research(&current_date);
                        }
                        let snapshot = pipeline.snapshot(
                            tick.bid.max(tick.price - 0.25),
                            tick.ask.max(tick.price + 0.25),
                        );
                        let summary = summary_from_state(
                            &snapshot,
                            &current_date,
                            "RTH",
                            session_open_price,
                            session_tick_count,
                            session_volume,
                            signals_fired_this_session,
                        );
                        let _ = db.upsert_session_summary(&summary);
                        let _ = db.insert_market_events_batch(&event_buffer);
                        total_events += event_buffer.len();
                        session_dates.push(current_date.clone());
                        sessions_processed += 1;
                        rvol_curves
                            .push(RvolPipeline::curve_from_total_volume(summary.total_volume));
                        if rvol_curves.len() > 20 {
                            let _ = rvol_curves.remove(0);
                        }
                        pipeline.rvol.load_historical_curve(&rvol_curves);
                    } else {
                        sessions_skipped += 1;
                    }

                    let end_state = pipeline.session_end_state();
                    let _ = db.save_prior_day_full(
                        &current_date,
                        end_state.high,
                        end_state.low,
                        end_state.close,
                        end_state.va_high,
                        end_state.va_low,
                        end_state.poc,
                    );
                }

                pipeline.reset_session();
                detector.reset();
                flow_emitter.reset();
                if let Some(ref mut r) = rules {
                    r.reset();
                }
                event_buffer.clear();
                session_tick_count = 0;
                session_volume = 0.0;
                session_open_price = 0.0;
                signals_fired_this_session = 0;

                // Load prior day levels for the new session
                if new_session == SessionType::Rth || new_session == SessionType::Globex {
                    if let Ok(Some((h, l, c, va_h, va_l, poc))) = db.load_prior_day_full(&date) {
                        pipeline.levels.set_prior_day(h, l, c);
                        if let (Some(vh), Some(vl), Some(pc)) = (va_h, va_l, poc) {
                            pipeline.levels.set_prior_profile(vh, vl, pc);
                        }
                    }
                }
            }
            if new_session != SessionType::Unknown {
                current_session = new_session;
            }
        }

        if current_date != date {
            current_date = date.clone();
        }

        let is_buy = matches!(tick.side, TradeSide::Buy);
        let minute = minute_of_session_from_timestamp(tick.timestamp_ms);

        if session_open_price <= 0.0 {
            session_open_price = tick.price;
        }
        session_tick_count += 1;
        session_volume += tick.volume;

        pipeline.on_trade_with_timestamp(
            tick.price,
            tick.volume,
            is_buy,
            minute,
            tick.timestamp_ms,
        );

        // Only detect events during RTH
        if current_session == SessionType::Rth {
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
            let snapshot = pipeline.snapshot(bid, ask);
            let events = detector.detect(&snapshot, tick.timestamp_ms, &current_date, minute);
            event_buffer.extend(events);

            // Flow events (absorption, pinch, acceleration zones, large trade clusters)
            let flow_events = flow_emitter.detect(&pipeline, tick.timestamp_ms, &current_date);
            event_buffer.extend(flow_events);

            // Rules engine: evaluate setups and track signal outcomes when run_rules is true
            if let Some(ref mut r) = rules {
                let setups = db.list_setups().unwrap_or_default();
                let setups: Vec<_> = if let Some(ref ids) = setup_ids_filter {
                    setups.into_iter().filter(|s| ids.contains(&s.id)).collect()
                } else {
                    setups.into_iter().filter(|s| s.active).collect()
                };
                for setup in &setups {
                    if let Some(alert) = r.evaluate(setup, &snapshot, false) {
                        let _ = db.insert_playbook_signal(
                            tick.timestamp_ms,
                            &alert.setup_id,
                            &serde_json::to_value(&alert).unwrap_or_else(|_| serde_json::json!({})),
                        );
                        let signal_id = format!("{}_{}", alert.setup_id, tick.timestamp_ms as u64);
                        let outcome = SignalOutcome {
                            signal_id: signal_id.clone(),
                            setup_id: alert.setup_id.clone(),
                            setup_name: Some(alert.setup_name.clone()),
                            fired_at_ms: tick.timestamp_ms,
                            fired_price: alert.current_price,
                            target_price: alert.target_prices.first().copied(),
                            stop_price: alert.stop_price,
                            outcome: "pending".to_string(),
                            outcome_at_ms: None,
                            max_favorable_excursion: None,
                            max_adverse_excursion: None,
                            r_result: None,
                            time_to_outcome_ms: None,
                        };
                        let _ = db.insert_signal_outcome(&outcome);
                        signals_fired_this_session += 1;
                        total_signals_fired += 1;
                    }
                }
                r.update_prev_market(&snapshot);
            }

            // Outcome tracker: update MFE/MAE and resolve signals
            if run_rules {
                let _ = outcome_tracker::on_tick(db, tick.price, tick.timestamp_ms, None);
            }
        }
    }

    // Finalize the last session if it was RTH
    if current_session == SessionType::Rth
        && !current_date.is_empty()
        && (force || !db.has_session_summary(&current_date).unwrap_or(true))
    {
        if force {
            let _ = db.purge_session_research(&current_date);
        }
        let last_tick = ticks.last().unwrap();
        let bid = if last_tick.bid > 0.0 {
            last_tick.bid
        } else {
            last_tick.price - 0.25
        };
        let ask = if last_tick.ask > 0.0 {
            last_tick.ask
        } else {
            last_tick.price + 0.25
        };
        let snapshot = pipeline.snapshot(bid, ask);
        let summary = summary_from_state(
            &snapshot,
            &current_date,
            "RTH",
            session_open_price,
            session_tick_count,
            session_volume,
            signals_fired_this_session,
        );
        let _ = db.upsert_session_summary(&summary);
        let _ = db.insert_market_events_batch(&event_buffer);
        total_events += event_buffer.len();
        session_dates.push(current_date);
        sessions_processed += 1;
        rvol_curves.push(RvolPipeline::curve_from_total_volume(summary.total_volume));
        if rvol_curves.len() > 20 {
            let _ = rvol_curves.remove(0);
        }
        pipeline.rvol.load_historical_curve(&rvol_curves);
    }

    let backtest_run_id = if run_rules && total_signals_fired > 0 {
        let run_id = uuid::Uuid::new_v4().to_string();
        let now_ms = chrono::Utc::now().timestamp_millis() as f64;
        let params = serde_json::json!({
            "runRules": true,
            "setupIds": setup_ids_filter,
            "sessionsProcessed": sessions_processed,
        });
        let metrics = serde_json::json!({
            "signalsFired": total_signals_fired,
            "totalTicks": ticks.len(),
            "totalEvents": total_events,
        });
        let perf = db.signal_performance(None, None, None).unwrap_or_default();
        let trades = serde_json::json!({ "signalPerformance": perf });
        if db
            .insert_backtest_run(&run_id, now_ms, &params, &metrics, &trades)
            .is_ok()
        {
            Some(run_id)
        } else {
            None
        }
    } else {
        None
    };

    Ok(BackfillResult {
        sessions_processed,
        sessions_skipped,
        total_ticks: ticks.len(),
        total_events,
        session_dates,
        gaps,
        signals_fired: if run_rules {
            Some(total_signals_fired)
        } else {
            None
        },
        backtest_run_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summary_computes_close_vs_levels() {
        let mut state = MarketState::default();
        state.last_price = 21010.0;
        state.ib_high = 21020.0;
        state.ib_low = 20980.0;
        state.vwap = 21000.0;
        state.poc = 21005.0;

        let summary = summary_from_state(&state, "2026-02-26", "RTH", 21000.0, 1000, 5000.0, 0);
        assert_eq!(summary.ib_mid, 21000.0);
        assert_eq!(summary.close_vs_ib_mid, "above");
        assert_eq!(summary.close_vs_vwap, "above");
        assert_eq!(summary.close_vs_poc, "above");
    }
}
