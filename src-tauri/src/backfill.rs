use crate::db::{Database, SessionSummary};
use crate::feed::scid_reader::ScidReader;
use crate::feed::TradeSide;
use crate::pipelines::{EventDetector, MarketState, PipelineEngine};
use crate::{
    classify_session, et_minutes_from_timestamp, minute_of_session_from_timestamp,
    session_date_from_timestamp_ms, SessionType,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackfillResult {
    pub sessions_processed: usize,
    pub sessions_skipped: usize,
    pub total_ticks: usize,
    pub total_events: usize,
    pub session_dates: Vec<String>,
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
    let ib_mid = if state.ib_high > 0.0 && state.ib_low > 0.0 {
        (state.ib_high + state.ib_low) / 2.0
    } else {
        0.0
    };

    let close_vs_ib_mid = if ib_mid <= 0.0 {
        "n/a".to_string()
    } else if state.last_price > ib_mid + 0.25 {
        "above".to_string()
    } else if state.last_price < ib_mid - 0.25 {
        "below".to_string()
    } else {
        "at".to_string()
    };

    let close_vs_vwap = if state.vwap <= 0.0 {
        "n/a".to_string()
    } else if state.last_price > state.vwap + 0.25 {
        "above".to_string()
    } else if state.last_price < state.vwap - 0.25 {
        "below".to_string()
    } else {
        "at".to_string()
    };

    let close_vs_poc = if state.poc <= 0.0 {
        "n/a".to_string()
    } else if state.last_price > state.poc + 0.25 {
        "above".to_string()
    } else if state.last_price < state.poc - 0.25 {
        "below".to_string()
    } else {
        "at".to_string()
    };

    SessionSummary {
        session_date: session_date.to_string(),
        session_type: session_type.to_string(),
        open_price,
        high: state.last_price.max(state.prior_day_high).max(0.0),
        low: if state.prior_day_low > 0.0 {
            state.last_price.min(state.prior_day_low)
        } else {
            state.last_price
        },
        close: state.last_price,
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
/// and persist session summaries.
pub fn run_backfill(
    reader: &ScidReader,
    db: &Database,
    since_ms: Option<f64>,
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
        });
    }

    let mut pipeline = PipelineEngine::new();
    let mut detector = EventDetector::new();
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

    for tick in &ticks {
        let date = session_date_from_timestamp_ms(tick.timestamp_ms);

        if let Some(et_min) = et_minutes_from_timestamp(tick.timestamp_ms) {
            let new_session = classify_session(et_min);

            if new_session != current_session
                && current_session != SessionType::Unknown
                && new_session != SessionType::Unknown
            {
                // Session boundary: finalize the outgoing session
                if current_session == SessionType::Rth && !current_date.is_empty() {
                    let should_process = !db.has_session_summary(&current_date).unwrap_or(true);

                    if should_process {
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
                            0,
                        );
                        // Use actual high/low from pipeline
                        let mut summary = summary;
                        summary.high = pipeline.levels.session_high;
                        summary.low = pipeline.levels.session_low;

                        let _ = db.upsert_session_summary(&summary);
                        let _ = db.insert_market_events_batch(&event_buffer);
                        total_events += event_buffer.len();
                        session_dates.push(current_date.clone());
                        sessions_processed += 1;
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
                event_buffer.clear();
                session_tick_count = 0;
                session_volume = 0.0;
                session_open_price = 0.0;

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
        }
    }

    // Finalize the last session if it was RTH
    if current_session == SessionType::Rth
        && !current_date.is_empty()
        && !db.has_session_summary(&current_date).unwrap_or(true)
    {
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
        let mut summary = summary_from_state(
            &snapshot,
            &current_date,
            "RTH",
            session_open_price,
            session_tick_count,
            session_volume,
            0,
        );
        summary.high = pipeline.levels.session_high;
        summary.low = pipeline.levels.session_low;
        let _ = db.upsert_session_summary(&summary);
        let _ = db.insert_market_events_batch(&event_buffer);
        total_events += event_buffer.len();
        session_dates.push(current_date);
        sessions_processed += 1;
    }

    Ok(BackfillResult {
        sessions_processed,
        sessions_skipped,
        total_ticks: ticks.len(),
        total_events,
        session_dates,
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
