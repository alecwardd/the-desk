use crate::db::{Database, ReplaySignalRecord, SessionSummary, SignalOutcome};
use crate::feed::scid_reader::{ScanControl, ScidReader};
use crate::feed::TradeSide;
use crate::pipelines::{
    EventDetector, FlowEventEmitter, MarketState, PipelineEngine, RvolPipeline,
};
use crate::rules::{RulesEngine, SetupDefinition};
use crate::{session_date_from_timestamp_ms, tick_time_context_from_timestamp_ms, SessionType};
use chrono::{Duration, NaiveDate, TimeZone};
use chrono_tz::US::Eastern;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HistoricalJobType {
    ResearchBackfill,
    Backtest,
}

impl HistoricalJobType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ResearchBackfill => "research_backfill",
            Self::Backtest => "backtest",
        }
    }

    pub fn replay_source(self) -> &'static str {
        match self {
            Self::ResearchBackfill => "backfill",
            Self::Backtest => "backtest",
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackfillConfig {
    pub run_rules: bool,
    pub setup_ids: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackfillJobParams {
    pub job_id: String,
    pub job_type: HistoricalJobType,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub force: bool,
    pub run_rules: bool,
    pub setup_ids: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackfillProgress {
    pub estimated_records: usize,
    pub records_scanned: usize,
    pub sessions_completed: usize,
    pub sessions_skipped: usize,
    pub current_session_date: Option<String>,
    pub current_phase: String,
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
    pub signals_fired: usize,
    pub backtest_run_id: Option<String>,
    pub integrity_status: String,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum BackfillJobError {
    InvalidParams(String),
    Cancelled,
    Runtime(String),
}

impl std::fmt::Display for BackfillJobError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidParams(msg) => write!(f, "{msg}"),
            Self::Cancelled => write!(f, "backfill cancelled"),
            Self::Runtime(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for BackfillJobError {}

struct SessionBuffers {
    event_buffer: Vec<crate::pipelines::event_detector::MarketEvent>,
    replay_signals: Vec<ReplaySignalRecord>,
    signal_outcomes: Vec<SignalOutcome>,
    session_open_price: f64,
    session_tick_count: i64,
    session_volume: f64,
}

impl Default for SessionBuffers {
    fn default() -> Self {
        Self {
            event_buffer: Vec::new(),
            replay_signals: Vec::new(),
            signal_outcomes: Vec::new(),
            session_open_price: 0.0,
            session_tick_count: 0,
            session_volume: 0.0,
        }
    }
}

/// Mutable accumulation state for a backfill run, grouped to reduce parameter counts.
struct BackfillRunState {
    pipeline: PipelineEngine,
    rvol_curves: Vec<Vec<f64>>,
    progress: BackfillProgress,
    warnings: Vec<String>,
    sessions_processed: usize,
    sessions_skipped: usize,
    total_events: usize,
    total_signals_fired: usize,
    session_dates: Vec<String>,
    buffers: SessionBuffers,
}

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

pub fn parse_backfill_date_range(
    start_date: Option<&str>,
    end_date: Option<&str>,
) -> Result<(Option<f64>, Option<f64>), BackfillJobError> {
    fn parse_date(input: &str, label: &str) -> Result<NaiveDate, BackfillJobError> {
        NaiveDate::parse_from_str(input, "%Y-%m-%d")
            .map_err(|_| BackfillJobError::InvalidParams(format!("invalid {label}: {input}")))
    }

    let start = match start_date {
        Some(value) => Some(parse_date(value, "startDate")?),
        None => None,
    };
    let end = match end_date {
        Some(value) => Some(parse_date(value, "endDate")?),
        None => None,
    };
    if let (Some(start), Some(end)) = (start, end) {
        if start > end {
            return Err(BackfillJobError::InvalidParams(
                "startDate must be on or before endDate".to_string(),
            ));
        }
    }

    let start_ms = start.map(|date| {
        Eastern
            .from_local_datetime(&date.and_hms_opt(0, 0, 0).expect("midnight"))
            .single()
            .map(|dt| dt.timestamp_millis() as f64)
            .unwrap_or(0.0)
    });
    let end_ms = end.map(|date| {
        let next = date + Duration::days(1);
        Eastern
            .from_local_datetime(&next.and_hms_opt(0, 0, 0).expect("midnight"))
            .single()
            .map(|dt| dt.timestamp_millis() as f64)
            .unwrap_or(0.0)
    });
    Ok((start_ms, end_ms))
}

pub fn run_backfill_job<F>(
    reader: &ScidReader,
    db: &Database,
    params: &BackfillJobParams,
    mut on_progress: F,
    cancel_flag: &AtomicBool,
) -> Result<BackfillResult, BackfillJobError>
where
    F: FnMut(&BackfillProgress),
{
    let (start_ms, end_ms_exclusive) =
        parse_backfill_date_range(params.start_date.as_deref(), params.end_date.as_deref())?;
    let source = params.job_type.replay_source();
    let setups = prepare_backfill_setups(db, params)?;
    let r_value_points = db
        .load_risk_config()
        .ok()
        .map(|cfg| cfg.r_value_points)
        .unwrap_or(50.0);

    let mut state = BackfillRunState {
        pipeline: PipelineEngine::new(),
        rvol_curves: db
            .recent_rth_session_volumes(20)
            .unwrap_or_default()
            .into_iter()
            .map(RvolPipeline::curve_from_total_volume)
            .collect(),
        progress: BackfillProgress {
            current_phase: "scanning".to_string(),
            ..Default::default()
        },
        warnings: Vec::new(),
        sessions_processed: 0,
        sessions_skipped: 0,
        total_events: 0,
        total_signals_fired: 0,
        session_dates: Vec::new(),
        buffers: SessionBuffers::default(),
    };
    state
        .pipeline
        .rvol
        .load_historical_curve(&state.rvol_curves);

    let mut detector = EventDetector::new();
    let mut flow_emitter = FlowEventEmitter::new();
    let mut rules = if params.run_rules {
        Some(RulesEngine::default())
    } else {
        None
    };

    if params.run_rules && setups.is_empty() {
        state
            .warnings
            .push("runRules=true but no active setups matched the request".to_string());
    }

    state.progress.estimated_records = reader
        .estimate_range_records(start_ms, end_ms_exclusive)
        .map_err(runtime_err)?;
    on_progress(&state.progress);

    let mut current_session = SessionType::Unknown;
    let mut current_date = String::new();
    let mut current_date_key: Option<i32> = None;
    let mut prev_ts: Option<f64> = None;
    let mut prev_class = SessionType::Unknown;
    let mut last_tick_meta: Option<(f64, f64, f64, f64)> = None;
    let mut tick_events = Vec::new();
    let mut gaps = Vec::new();
    let mut cancelled = false;

    let scan_stats = reader
        .scan_range(start_ms, end_ms_exclusive, |tick| {
            if cancel_flag.load(Ordering::Relaxed) {
                cancelled = true;
                return Ok(ScanControl::Stop);
            }

            state.progress.records_scanned += 1;
            if state.progress.records_scanned == 1 {
                state.progress.current_phase = "processing_session".to_string();
            }

            let tick_ctx = match tick_time_context_from_timestamp_ms(tick.timestamp_ms) {
                Some(ctx) => ctx,
                None => return Ok(ScanControl::Continue),
            };
            let tick_class = tick_ctx.session_type;
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

            if current_date_key != Some(tick_ctx.session_date_key) {
                current_date = tick_ctx.session_date.clone();
                current_date_key = Some(tick_ctx.session_date_key);
            }
            state.progress.current_session_date = Some(current_date.clone());

            let new_session = tick_ctx.session_type;
            if new_session != current_session
                && current_session != SessionType::Unknown
                && new_session != SessionType::Unknown
            {
                if current_session == SessionType::Rth {
                    finalize_rth_session(
                        db,
                        params,
                        &mut state,
                        &current_date,
                        last_tick_meta,
                        source,
                        r_value_points,
                        cancel_flag,
                        &mut on_progress,
                    )
                    .map_err(|e| e.to_string())?;
                }

                state.pipeline.reset_session();
                detector.reset();
                flow_emitter.reset();
                if let Some(ref mut rules) = rules {
                    rules.reset();
                }
                state.buffers = SessionBuffers::default();

                if new_session == SessionType::Rth || new_session == SessionType::Globex {
                    if let Some((h, l, c, va_h, va_l, poc)) = db
                        .load_prior_day_full(&current_date)
                        .map_err(runtime_err)
                        .map_err(|e| e.to_string())?
                    {
                        state.pipeline.levels.set_prior_day(h, l, c);
                        if let (Some(vh), Some(vl), Some(pc)) = (va_h, va_l, poc) {
                            state.pipeline.levels.set_prior_profile(vh, vl, pc);
                        }
                    }
                }
            }
            if new_session != SessionType::Unknown {
                current_session = new_session;
            }

            let is_buy = matches!(tick.side, TradeSide::Buy);
            let minute = tick_ctx.minute_of_session;
            if state.buffers.session_open_price <= 0.0 {
                state.buffers.session_open_price = tick.price;
            }
            state.buffers.session_tick_count += 1;
            state.buffers.session_volume += tick.volume;

            state.pipeline.on_trade_with_session_flag(
                tick.price,
                tick.volume,
                is_buy,
                minute,
                tick.timestamp_ms,
                current_session != SessionType::Rth,
            );

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
            last_tick_meta = Some((bid, ask, tick.price, tick.timestamp_ms));

            if current_session == SessionType::Rth {
                let snapshot = if params.run_rules {
                    state.pipeline.snapshot_at(bid, ask, tick.timestamp_ms)
                } else {
                    state
                        .pipeline
                        .snapshot_for_detection(bid, ask, tick.timestamp_ms)
                };
                state.progress.current_phase = "processing_session".to_string();

                tick_events.clear();
                detector.detect_into(
                    &snapshot,
                    tick.timestamp_ms,
                    &current_date,
                    minute,
                    &mut tick_events,
                );
                flow_emitter.detect_into(
                    &state.pipeline,
                    tick.timestamp_ms,
                    &current_date,
                    tick.price,
                    &mut tick_events,
                );
                state.buffers.event_buffer.append(&mut tick_events);

                if let Some(ref mut rules) = rules {
                    for setup in &setups {
                        if let Some(alert) = rules.evaluate(setup, &snapshot, false) {
                            let signal_id = format!(
                                "{}_{}_{}",
                                params.job_id, alert.setup_id, tick.timestamp_ms as u64
                            );
                            state.buffers.replay_signals.push(ReplaySignalRecord {
                                signal_id: signal_id.clone(),
                                timestamp_ms: tick.timestamp_ms,
                                session_date: current_date.clone(),
                                setup_id: alert.setup_id.clone(),
                                payload: serde_json::to_value(&alert)
                                    .unwrap_or_else(|_| serde_json::json!({})),
                                source: source.to_string(),
                                job_id: Some(params.job_id.clone()),
                            });
                            state.buffers.signal_outcomes.push(SignalOutcome {
                                signal_id,
                                setup_id: alert.setup_id.clone(),
                                setup_name: Some(alert.setup_name.clone()),
                                session_date: current_date.clone(),
                                source: source.to_string(),
                                job_id: Some(params.job_id.clone()),
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
                            });
                        }
                    }
                    rules.update_prev_market(&snapshot);
                }

                if params.run_rules {
                    update_signal_outcomes(
                        &mut state.buffers.signal_outcomes,
                        tick.price,
                        tick.timestamp_ms,
                        r_value_points,
                    );
                }
            }

            if state.progress.records_scanned.is_multiple_of(5_000) {
                on_progress(&state.progress);
            }
            Ok(ScanControl::Continue)
        })
        .map_err(runtime_err)?;

    if state.progress.estimated_records == 0 {
        state.progress.estimated_records = scan_stats.estimated_records;
    }
    state.progress.records_scanned = scan_stats.records_scanned;
    on_progress(&state.progress);

    if cancelled || cancel_flag.load(Ordering::Relaxed) {
        return Err(BackfillJobError::Cancelled);
    }

    state.progress.current_phase = "finalizing".to_string();
    on_progress(&state.progress);
    if current_session == SessionType::Rth {
        finalize_rth_session(
            db,
            params,
            &mut state,
            &current_date,
            last_tick_meta,
            source,
            r_value_points,
            cancel_flag,
            &mut on_progress,
        )?;
    }

    if cancel_flag.load(Ordering::Relaxed) {
        return Err(BackfillJobError::Cancelled);
    }

    if !gaps.is_empty() {
        state
            .warnings
            .push(format!("detected {} large tick gaps", gaps.len()));
    }
    if state.sessions_skipped > 0 {
        state.warnings.push(format!(
            "skipped {} already-processed sessions",
            state.sessions_skipped
        ));
    }

    let backtest_run_id =
        if params.job_type == HistoricalJobType::Backtest && state.total_signals_fired > 0 {
            Some(persist_backtest_run(
                db,
                params,
                state.total_signals_fired,
                state.total_events,
                scan_stats.records_scanned,
            )?)
        } else {
            None
        };

    Ok(BackfillResult {
        sessions_processed: state.sessions_processed,
        sessions_skipped: state.sessions_skipped,
        total_ticks: scan_stats.records_scanned,
        total_events: state.total_events,
        session_dates: state.session_dates,
        gaps,
        signals_fired: state.total_signals_fired,
        backtest_run_id,
        integrity_status: if state.warnings.is_empty() {
            "ok".to_string()
        } else {
            "warning".to_string()
        },
        warnings: state.warnings,
    })
}

fn prepare_backfill_setups(
    db: &Database,
    params: &BackfillJobParams,
) -> Result<Vec<SetupDefinition>, BackfillJobError> {
    if !params.run_rules {
        return Ok(Vec::new());
    }
    let setups = db.list_setups().map_err(runtime_err)?;
    let filtered = if let Some(ref ids) = params.setup_ids {
        setups
            .into_iter()
            .filter(|setup| ids.contains(&setup.id))
            .collect()
    } else {
        setups.into_iter().filter(|setup| setup.active).collect()
    };
    Ok(filtered)
}

#[allow(clippy::too_many_arguments)]
fn finalize_rth_session<F>(
    db: &Database,
    params: &BackfillJobParams,
    state: &mut BackfillRunState,
    current_date: &str,
    last_tick_meta: Option<(f64, f64, f64, f64)>,
    source: &str,
    r_value_points: f64,
    cancel_flag: &AtomicBool,
    on_progress: &mut F,
) -> Result<(), BackfillJobError>
where
    F: FnMut(&BackfillProgress),
{
    if current_date.is_empty() {
        return Ok(());
    }
    if cancel_flag.load(Ordering::Relaxed) {
        return Err(BackfillJobError::Cancelled);
    }
    if last_tick_meta.is_none() {
        return Ok(());
    }

    let should_process =
        params.force || !db.has_session_summary(current_date).map_err(runtime_err)?;
    let end_state = state.pipeline.session_end_state();
    if should_process {
        state.progress.current_phase = "persisting_session".to_string();
        state.progress.current_session_date = Some(current_date.to_string());
        on_progress(&state.progress);

        let (bid, ask, exit_price, exit_time_ms) = last_tick_meta.expect("checked");
        if params.run_rules {
            finalize_pending_outcomes(
                &mut state.buffers.signal_outcomes,
                exit_price,
                exit_time_ms,
                r_value_points,
            );
        }
        let snapshot = if params.run_rules {
            state.pipeline.snapshot_at(bid, ask, exit_time_ms)
        } else {
            state
                .pipeline
                .snapshot_for_detection(bid, ask, exit_time_ms)
        };
        let summary = summary_from_state(
            &snapshot,
            current_date,
            "RTH",
            state.buffers.session_open_price,
            state.buffers.session_tick_count,
            state.buffers.session_volume,
            state.buffers.replay_signals.len() as i64,
        );
        db.persist_historical_session(
            current_date,
            params.force,
            &[source],
            &summary,
            &state.buffers.event_buffer,
            &state.buffers.replay_signals,
            &state.buffers.signal_outcomes,
            (
                end_state.high,
                end_state.low,
                end_state.close,
                end_state.va_high,
                end_state.va_low,
                end_state.poc,
            ),
        )
        .map_err(runtime_err)?;

        state.total_events += state.buffers.event_buffer.len();
        state.total_signals_fired += state.buffers.replay_signals.len();
        state.sessions_processed += 1;
        state.progress.sessions_completed = state.sessions_processed;
        state.session_dates.push(current_date.to_string());

        if state.buffers.event_buffer.is_empty() {
            state
                .warnings
                .push(format!("session {current_date} produced zero events"));
        }

        state
            .rvol_curves
            .push(RvolPipeline::curve_from_total_volume(summary.total_volume));
        if state.rvol_curves.len() > 20 {
            let _ = state.rvol_curves.remove(0);
        }
        state
            .pipeline
            .rvol
            .load_historical_curve(&state.rvol_curves);
    } else {
        db.save_prior_day_full(
            current_date,
            end_state.high,
            end_state.low,
            end_state.close,
            end_state.va_high,
            end_state.va_low,
            end_state.poc,
        )
        .map_err(runtime_err)?;
        state.sessions_skipped += 1;
        state.progress.sessions_skipped = state.sessions_skipped;
    }

    Ok(())
}

fn update_signal_outcomes(
    signal_outcomes: &mut [SignalOutcome],
    price: f64,
    timestamp_ms: f64,
    r_value_points: f64,
) {
    for signal in signal_outcomes
        .iter_mut()
        .filter(|signal| signal.outcome == "pending")
    {
        let target = signal.target_price.unwrap_or(0.0);
        let stop = signal.stop_price.unwrap_or(0.0);
        let entry = signal.fired_price;
        let (is_long, is_short) = infer_direction(entry, signal.target_price, signal.stop_price);

        let mut mfe = signal.max_favorable_excursion.unwrap_or(0.0);
        let mut mae = signal.max_adverse_excursion.unwrap_or(0.0);
        if is_long {
            mfe = mfe.max(price - entry);
            mae = mae.max(entry - price);
        } else if is_short {
            mfe = mfe.max(entry - price);
            mae = mae.max(price - entry);
        }
        signal.max_favorable_excursion = Some(mfe);
        signal.max_adverse_excursion = Some(mae);

        let target_hit = if is_long {
            target > 0.0 && price >= target
        } else if is_short {
            target > 0.0 && price <= target
        } else {
            false
        };
        let stop_hit = if is_long {
            stop != 0.0 && price <= stop
        } else if is_short {
            stop != 0.0 && price >= stop
        } else {
            false
        };

        if target_hit {
            signal.outcome = "target_hit".to_string();
            signal.outcome_at_ms = Some(timestamp_ms);
            signal.r_result = Some(if is_long {
                (target - entry) / r_value_points
            } else {
                (entry - target) / r_value_points
            });
            signal.time_to_outcome_ms = Some(timestamp_ms - signal.fired_at_ms);
        } else if stop_hit {
            signal.outcome = "stop_hit".to_string();
            signal.outcome_at_ms = Some(timestamp_ms);
            signal.r_result = Some(if is_long {
                (stop - entry) / r_value_points
            } else {
                (entry - stop) / r_value_points
            });
            signal.time_to_outcome_ms = Some(timestamp_ms - signal.fired_at_ms);
        }
    }
}

fn finalize_pending_outcomes(
    signal_outcomes: &mut [SignalOutcome],
    exit_price: f64,
    exit_time_ms: f64,
    r_value_points: f64,
) {
    for signal in signal_outcomes
        .iter_mut()
        .filter(|signal| signal.outcome == "pending")
    {
        let (is_long, _) =
            infer_direction(signal.fired_price, signal.target_price, signal.stop_price);
        signal.outcome = "time_exit".to_string();
        signal.outcome_at_ms = Some(exit_time_ms);
        signal.r_result = Some(if is_long {
            (exit_price - signal.fired_price) / r_value_points
        } else {
            (signal.fired_price - exit_price) / r_value_points
        });
        signal.time_to_outcome_ms = Some(exit_time_ms - signal.fired_at_ms);
    }
}

fn infer_direction(entry: f64, target: Option<f64>, stop: Option<f64>) -> (bool, bool) {
    let target = target.unwrap_or(0.0);
    let stop = stop.unwrap_or(0.0);
    let is_long = target > entry && (stop == 0.0 || stop < entry);
    let is_short = target < entry && (stop == 0.0 || stop > entry);
    (is_long, is_short)
}

fn persist_backtest_run(
    db: &Database,
    params: &BackfillJobParams,
    total_signals_fired: usize,
    total_events: usize,
    total_ticks: usize,
) -> Result<String, BackfillJobError> {
    let run_id = uuid::Uuid::new_v4().to_string();
    let now_ms = chrono::Utc::now().timestamp_millis() as f64;
    let metrics = serde_json::json!({
        "signalsFired": total_signals_fired,
        "totalTicks": total_ticks,
        "totalEvents": total_events,
    });
    let perf = db
        .signal_performance_filtered(None, None, None, Some("backtest"), Some(&params.job_id))
        .map_err(runtime_err)?;
    let trades = serde_json::json!({ "signalPerformance": perf });
    let params_json = serde_json::json!({
        "jobId": params.job_id,
        "startDate": params.start_date,
        "endDate": params.end_date,
        "force": params.force,
        "setupIds": params.setup_ids,
        "jobType": params.job_type.as_str(),
    });
    db.insert_backtest_run(&run_id, now_ms, &params_json, &metrics, &trades)
        .map_err(runtime_err)?;
    Ok(run_id)
}

fn runtime_err(err: impl std::fmt::Display) -> BackfillJobError {
    BackfillJobError::Runtime(err.to_string())
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

    #[test]
    fn rejects_invalid_date_range() {
        let result = parse_backfill_date_range(Some("2026-03-10"), Some("2026-03-01"));
        assert!(matches!(result, Err(BackfillJobError::InvalidParams(_))));
    }

    #[test]
    fn parses_eastern_date_bounds() {
        let (start, end) =
            parse_backfill_date_range(Some("2026-03-01"), Some("2026-03-01")).expect("range");
        assert!(start.is_some());
        assert!(end.is_some());
        assert!(end.unwrap() > start.unwrap());
    }
}
