use crate::attention::{AttentionPulseKind, SignalComposer, SignalComposerInput};
use crate::db::{
    AttentionSignalQuery, Database, PriorDayReference, ReplaySignalRecord, SessionSummary,
    SetupRuntimeStateRecord, SignalOutcome,
};
use crate::feed::monotonic::{MonotonicTickGuard, MonotonicTimestampStats};
use crate::feed::scid_reader::{ScanControl, ScidReader};
use crate::feed::TradeSide;
use crate::feed::{load_feed_config, resolve_contract_metadata, ContractMetadata};
use crate::pipelines::{EventDetector, FlowEventEmitter, MarketState, PipelineEngine};
use crate::research::context_frame::snapshot_context_buckets;
use crate::research::hypothesis::current_engine_version;
use crate::rollover::{build_contract_rollover_status, PriorReferenceTrust};
use crate::rules::{RulesEngine, SetupDefinition, SetupRuntimeSnapshot};
use crate::{
    classify_delta_segment, session_date_from_timestamp_ms, tick_time_context_from_timestamp_ms,
    DeltaSegment, SessionType,
};
use chrono::{Duration, NaiveDate, TimeZone};
use chrono_tz::US::Eastern;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};

const CONTEXT_FRAME_SNAPSHOT_INTERVAL_MS: f64 = 60_000.0;

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

/// Optional replay seed data for hermetic historical-job tests.
///
/// `None` RVOL curves preserve production behavior by loading recent curves from SQLite.
/// `prior_day_references` are explicit seeds inserted before replay; an empty list means
/// the replay uses whatever references are already present in the database.
#[derive(Debug, Clone, Default)]
pub struct BackfillReplayOptions {
    pub contract_metadata: Option<ContractMetadata>,
    pub rth_rvol_curves: Option<Vec<Vec<f64>>>,
    pub globex_rvol_curves: Option<Vec<Vec<f64>>>,
    pub prior_day_references: Vec<PriorDayReference>,
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
    pub scid_timestamp_monotonicity: MonotonicTimestampStats,
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
    setup_runtime_states: Vec<SetupRuntimeStateRecord>,
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
            setup_runtime_states: Vec::new(),
            session_open_price: 0.0,
            session_tick_count: 0,
            session_volume: 0.0,
        }
    }
}

#[derive(Debug, Clone)]
struct SegmentBuffers {
    open_price: f64,
    high: f64,
    low: f64,
    tick_count: i64,
    volume: f64,
}

impl Default for SegmentBuffers {
    fn default() -> Self {
        Self {
            open_price: 0.0,
            high: 0.0,
            low: 0.0,
            tick_count: 0,
            volume: 0.0,
        }
    }
}

impl SegmentBuffers {
    fn has_data(&self) -> bool {
        self.tick_count > 0
    }

    fn observe_trade(&mut self, price: f64, volume: f64) {
        if self.tick_count == 0 {
            self.open_price = price;
            self.high = price;
            self.low = price;
        } else {
            self.high = self.high.max(price);
            self.low = self.low.min(price);
        }
        self.tick_count += 1;
        self.volume += volume;
    }
}

/// Mutable accumulation state for a backfill run, grouped to reduce parameter counts.
struct BackfillRunState {
    pipeline: PipelineEngine,
    rvol_curves: Vec<Vec<f64>>,
    globex_rvol_curves: Vec<Vec<f64>>,
    progress: BackfillProgress,
    warnings: Vec<String>,
    sessions_processed: usize,
    sessions_skipped: usize,
    total_events: usize,
    total_signals_fired: usize,
    session_dates: Vec<String>,
    buffers: SessionBuffers,
    segment_buffers: SegmentBuffers,
    last_context_snapshot_ms: Option<f64>,
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
        root_symbol: state.root_symbol.clone(),
        contract_symbol: state.contract_symbol.clone(),
        contract_month: state.contract_month.clone(),
        symbol_resolution_mode: state.symbol_resolution_mode.clone(),
        carry_forward_levels_valid: state.carry_forward_levels_valid,
        rollover_warning: state.rollover_warning.clone(),
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

fn should_persist_context_snapshot(state: &mut BackfillRunState, timestamp_ms: f64) -> bool {
    if !timestamp_ms.is_finite() || timestamp_ms <= 0.0 {
        return false;
    }
    match state.last_context_snapshot_ms {
        Some(last) if timestamp_ms - last < CONTEXT_FRAME_SNAPSHOT_INTERVAL_MS => false,
        _ => {
            state.last_context_snapshot_ms = Some(timestamp_ms);
            true
        }
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
    run_backfill_job_with_options(
        reader,
        db,
        params,
        &mut on_progress,
        cancel_flag,
        BackfillReplayOptions::default(),
    )
}

pub fn run_backfill_job_with_options<F>(
    reader: &ScidReader,
    db: &Database,
    params: &BackfillJobParams,
    mut on_progress: F,
    cancel_flag: &AtomicBool,
    options: BackfillReplayOptions,
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
    for prior in &options.prior_day_references {
        db.save_prior_day_full_with_dnva_contract(
            &prior.date,
            prior.high,
            prior.low,
            prior.close,
            prior.va_high.unwrap_or(0.0),
            prior.va_low.unwrap_or(0.0),
            prior.poc.unwrap_or(0.0),
            prior.dnva_high,
            prior.dnva_low,
            prior.dnp,
            prior.root_symbol.as_deref(),
            prior.contract_symbol.as_deref(),
        )
        .map_err(runtime_err)?;
    }

    let rvol_curves = options.rth_rvol_curves.unwrap_or_else(|| {
        db.recent_session_volume_curves("RTH", 20)
            .unwrap_or_default()
    });
    let globex_rvol_curves = options.globex_rvol_curves.unwrap_or_else(|| {
        db.recent_session_volume_curves("Globex", 20)
            .unwrap_or_default()
    });

    let mut pipeline = PipelineEngine::new();
    let contract_metadata = options
        .contract_metadata
        .unwrap_or_else(|| resolve_contract_metadata(&load_feed_config()));
    pipeline.set_contract_metadata(contract_metadata.clone());
    let mut state = BackfillRunState {
        pipeline,
        rvol_curves,
        globex_rvol_curves,
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
        segment_buffers: SegmentBuffers::default(),
        last_context_snapshot_ms: None,
    };
    state
        .pipeline
        .rvol
        .load_historical_curve(&state.rvol_curves);
    state
        .pipeline
        .rvol
        .load_globex_historical_curve(&state.globex_rvol_curves);

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
    let mut current_delta_segment = DeltaSegment::Unknown;
    let mut current_date = String::new();
    let mut current_date_key: Option<i32> = None;
    let mut prev_ts: Option<f64> = None;
    let mut prev_class = SessionType::Unknown;
    let mut last_tick_meta: Option<(f64, f64, f64, f64)> = None;
    let mut tick_events = Vec::new();
    let mut gaps = Vec::new();
    let mut cancelled = false;
    let mut monotonic_guard = MonotonicTickGuard::default();

    let scan_stats = reader
        .scan_range_in_file_order(start_ms, end_ms_exclusive, |tick| {
            if cancel_flag.load(Ordering::Relaxed) {
                cancelled = true;
                return Ok(ScanControl::Stop);
            }

            state.progress.records_scanned += 1;
            if state.progress.records_scanned == 1 {
                state.progress.current_phase = "processing_session".to_string();
            }

            if !matches!(
                monotonic_guard.observe(tick.timestamp_ms),
                crate::feed::monotonic::MonotonicTimestampDecision::Accept
            ) {
                return Ok(ScanControl::Continue);
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
            let new_segment = classify_delta_segment(tick_ctx.et_minutes);

            if new_session != current_session
                && current_session != SessionType::Unknown
                && new_session != SessionType::Unknown
            {
                finalize_session_period(
                    db,
                    params,
                    &mut state,
                    current_session,
                    current_delta_segment,
                    &current_date,
                    last_tick_meta,
                    source,
                    r_value_points,
                    cancel_flag,
                    &mut on_progress,
                )
                .map_err(|e| e.to_string())?;

                state
                    .pipeline
                    .reset_session_with_type(new_session == SessionType::Globex);
                detector.reset();
                flow_emitter.reset();
                if let Some(ref mut rules) = rules {
                    rules.reset();
                }
                state.buffers = SessionBuffers::default();
                state.segment_buffers = SegmentBuffers::default();
                state.last_context_snapshot_ms = None;

                if new_session == SessionType::Rth || new_session == SessionType::Globex {
                    let current_contract_reference = db
                        .load_prior_day_reference_for_contract(
                            &current_date,
                            contract_metadata.root_symbol.as_str(),
                            contract_metadata.contract_symbol.as_str(),
                        )
                        .map_err(runtime_err)
                        .map_err(|e| e.to_string())?;
                    let same_root_reference = db
                        .load_prior_day_reference_for_root(
                            &current_date,
                            contract_metadata.root_symbol.as_str(),
                        )
                        .map_err(runtime_err)
                        .map_err(|e| e.to_string())?;
                    let rollover_status = build_contract_rollover_status(
                        &contract_metadata,
                        Some(&contract_metadata),
                        current_contract_reference.clone(),
                        same_root_reference,
                        None,
                        15_000.0,
                    );
                    if rollover_status.prior_reference_trust == PriorReferenceTrust::Authoritative {
                        if let Some(prior_ref) = current_contract_reference {
                            state.pipeline.levels.set_prior_day(
                                prior_ref.high,
                                prior_ref.low,
                                prior_ref.close,
                            );
                            state.pipeline.levels.set_prior_day_contract_context(
                                prior_ref.root_symbol.as_deref(),
                                prior_ref.contract_symbol.as_deref(),
                                Some(contract_metadata.contract_symbol.as_str()),
                            );
                            if let (Some(vh), Some(vl), Some(pc)) =
                                (prior_ref.va_high, prior_ref.va_low, prior_ref.poc)
                            {
                                state.pipeline.levels.set_prior_profile(vh, vl, pc);
                            }
                            if let (Some(dh), Some(dl), Some(dp)) =
                                (prior_ref.dnva_high, prior_ref.dnva_low, prior_ref.dnp)
                            {
                                state.pipeline.levels.set_prior_dnva(dh, dl, dp);
                            }
                        }
                    } else {
                        state.pipeline.levels.clear_prior_references();
                        state.pipeline.levels.set_prior_day_contract_context(
                            Some(contract_metadata.root_symbol.as_str()),
                            None,
                            Some(contract_metadata.contract_symbol.as_str()),
                        );
                    }
                }
            } else if new_segment != current_delta_segment
                && current_delta_segment != DeltaSegment::Unknown
                && new_segment != DeltaSegment::Unknown
            {
                // Persist the segment we're leaving (Asia or London) before reset.
                if current_delta_segment == DeltaSegment::Asia {
                    persist_segment_summary(
                        db,
                        params,
                        &mut state,
                        &current_date,
                        "Asia",
                        last_tick_meta,
                        source,
                        &mut on_progress,
                    )
                    .map_err(|e| e.to_string())?;
                }
                state.pipeline.reset_segment(new_segment);
                state.segment_buffers = SegmentBuffers::default();
            }

            if new_session != SessionType::Unknown {
                current_session = new_session;
            }
            if new_segment != DeltaSegment::Unknown {
                current_delta_segment = new_segment;
            }
            if tick_class == SessionType::Unknown {
                return Ok(ScanControl::Continue);
            }

            let is_buy = matches!(tick.side, TradeSide::Buy);
            let minute = tick_ctx.minute_of_session;
            if state.buffers.session_open_price <= 0.0 {
                state.buffers.session_open_price = tick.price;
            }
            state.buffers.session_tick_count += 1;
            state.buffers.session_volume += tick.volume;
            state.segment_buffers.observe_trade(tick.price, tick.volume);

            state.pipeline.on_trade_with_session_flag(
                tick.price,
                tick.volume,
                is_buy,
                minute,
                tick.timestamp_ms,
                current_session != SessionType::Rth,
                tick_ctx.et_minutes,
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

            let snapshot = if params.run_rules && current_session == SessionType::Rth {
                state.pipeline.snapshot_at(bid, ask, tick.timestamp_ms)
            } else {
                state
                    .pipeline
                    .snapshot_for_detection(bid, ask, tick.timestamp_ms)
            };
            if should_persist_context_snapshot(&mut state, tick.timestamp_ms) {
                let payload = serde_json::to_value(&snapshot).unwrap_or_default();
                let context = snapshot_context_buckets(&payload, tick.timestamp_ms);
                let _ =
                    db.insert_pipeline_snapshot_with_context(tick.timestamp_ms, &payload, &context);
            }
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

            if current_session == SessionType::Rth {
                if let Some(ref mut rules) = rules {
                    for setup in &setups {
                        let alert = rules.evaluate(setup, &snapshot, false);
                        if let Some(runtime) = rules.runtime_snapshot(&setup.id) {
                            upsert_buffered_setup_runtime(
                                &mut state.buffers.setup_runtime_states,
                                runtime_record_from_replay_snapshot(
                                    runtime,
                                    &current_date,
                                    &contract_metadata,
                                    source,
                                    tick.timestamp_ms,
                                ),
                            );
                        }
                        if let Some(alert) = alert {
                            let signal_id = format!(
                                "{}_{}_{}",
                                params.job_id, alert.setup_id, tick.timestamp_ms as u64
                            );
                            state.buffers.replay_signals.push(ReplaySignalRecord {
                                signal_id: signal_id.clone(),
                                timestamp_ms: tick.timestamp_ms,
                                session_date: current_date.clone(),
                                root_symbol: Some(contract_metadata.root_symbol.clone()),
                                contract_symbol: Some(contract_metadata.contract_symbol.clone()),
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
                                root_symbol: Some(contract_metadata.root_symbol.clone()),
                                contract_symbol: Some(contract_metadata.contract_symbol.clone()),
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
                                rvol_at_fire: Some(state.pipeline.rvol.rvol_ratio()),
                                rvol_bucket_at_fire: Some(
                                    state.pipeline.rvol.bucket_index() as i32,
                                ),
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
    if current_session != SessionType::Unknown {
        finalize_session_period(
            db,
            params,
            &mut state,
            current_session,
            current_delta_segment,
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
    let monotonicity = monotonic_guard.into_stats();
    if monotonicity.has_violations() {
        state.warnings.push(format!(
            "skipped {} non-monotonic SCID ticks during replay (duplicate={}, backward={})",
            monotonicity.skipped_non_monotonic_ticks,
            monotonicity.duplicate_timestamp_ticks,
            monotonicity.backward_timestamp_ticks
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
        scid_timestamp_monotonicity: monotonicity,
        warnings: state.warnings,
    })
}

#[allow(clippy::too_many_arguments)]
fn persist_segment_summary<F>(
    db: &Database,
    params: &BackfillJobParams,
    state: &mut BackfillRunState,
    session_date: &str,
    session_type: &str,
    last_tick_meta: Option<(f64, f64, f64, f64)>,
    _source: &str,
    on_progress: &mut F,
) -> Result<(), BackfillJobError>
where
    F: FnMut(&BackfillProgress),
{
    const DNP_TOLERANCE: f64 = 0.5;
    let should_process = params.force
        || !db
            .has_session_summary_for(session_date, session_type)
            .map_err(runtime_err)?;
    if !should_process {
        return Ok(());
    }
    if !state.segment_buffers.has_data() {
        return Ok(());
    }
    let (bid, ask, _exit_price, exit_time_ms) = match last_tick_meta {
        Some(m) => m,
        None => return Ok(()),
    };
    state.progress.current_phase = "persisting_session".to_string();
    state.progress.current_session_date = Some(session_date.to_string());
    on_progress(&state.progress);

    let snapshot = state
        .pipeline
        .snapshot_for_detection(bid, ask, exit_time_ms);
    let mut summary = summary_from_state(
        &snapshot,
        session_date,
        session_type,
        state.segment_buffers.open_price,
        state.segment_buffers.tick_count,
        state.segment_buffers.volume,
        0,
    );
    summary.high = state.segment_buffers.high;
    summary.low = state.segment_buffers.low;
    db.upsert_session_summary(&summary).map_err(runtime_err)?;
    db.delete_untested_dnps_touched_by_range(
        summary.low,
        summary.high,
        DNP_TOLERANCE,
        Some((session_date, session_type)),
    )
    .map_err(runtime_err)?;
    // Track untested DNPs: price did not revisit DNP ± 2 NQ ticks (0.5 pts).
    if summary.dnp > 0.0 {
        let dnp_tested = (summary.low <= summary.dnp + DNP_TOLERANCE)
            && (summary.high >= summary.dnp - DNP_TOLERANCE);
        if dnp_tested {
            db.delete_untested_dnp_for_session(session_date, session_type)
                .map_err(runtime_err)?;
        } else {
            db.save_untested_dnp(session_date, session_type, summary.dnp)
                .map_err(runtime_err)?;
        }
    }
    state.sessions_processed += 1;
    state.progress.sessions_completed = state.sessions_processed;
    state.session_dates.push(session_date.to_string());
    Ok(())
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
fn finalize_session_period<F>(
    db: &Database,
    params: &BackfillJobParams,
    state: &mut BackfillRunState,
    session_type: SessionType,
    current_delta_segment: DeltaSegment,
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
    if last_tick_meta.is_none() || session_type == SessionType::Unknown {
        return Ok(());
    }

    if session_type == SessionType::Globex {
        // Persist London summary when transitioning London→RTH (segment is London).
        if current_delta_segment == DeltaSegment::London {
            persist_segment_summary(
                db,
                params,
                state,
                current_date,
                "London",
                last_tick_meta,
                source,
                on_progress,
            )?;
        }
        state.progress.current_phase = "persisting_session".to_string();
        state.progress.current_session_date = Some(current_date.to_string());
        on_progress(&state.progress);
        db.insert_market_events_batch(&state.buffers.event_buffer)
            .map_err(runtime_err)?;
        if let Some((bid, ask, _price, timestamp_ms)) = last_tick_meta {
            let snapshot = state
                .pipeline
                .snapshot_for_detection(bid, ask, timestamp_ms);
            let payload = serde_json::to_value(&snapshot).unwrap_or_default();
            let context = snapshot_context_buckets(&payload, timestamp_ms);
            let _ = db.insert_pipeline_snapshot_with_context(timestamp_ms, &payload, &context);
            persist_attention_for_replay(
                db,
                params,
                state,
                &snapshot,
                current_date,
                source,
                timestamp_ms,
            )?;
        }
        state.total_events += state.buffers.event_buffer.len();
        state.sessions_processed += 1;
        state.progress.sessions_completed = state.sessions_processed;
        state.session_dates.push(current_date.to_string());
        if state.buffers.event_buffer.is_empty() {
            state
                .warnings
                .push(format!("session {current_date} produced zero events"));
        }

        // Persist the actual per-bucket Globex volume curve and update rolling baseline.
        let curve = state.pipeline.rvol.current_curve();
        let _ = db.save_volume_curve(current_date, "Globex", &curve);
        state.globex_rvol_curves.push(curve);
        if state.globex_rvol_curves.len() > 20 {
            state.globex_rvol_curves.remove(0);
        }
        state
            .pipeline
            .rvol
            .load_globex_historical_curve(&state.globex_rvol_curves);

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
        let payload = serde_json::to_value(&snapshot).unwrap_or_default();
        let context = snapshot_context_buckets(&payload, exit_time_ms);
        let _ = db.insert_pipeline_snapshot_with_context(exit_time_ms, &payload, &context);
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
                end_state.dnva_high,
                end_state.dnva_low,
                end_state.dnp,
            ),
        )
        .map_err(runtime_err)?;
        persist_attention_for_replay(
            db,
            params,
            state,
            &snapshot,
            current_date,
            source,
            exit_time_ms,
        )?;

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

        // Persist the actual per-bucket RTH volume curve and update rolling baseline.
        let curve = state.pipeline.rvol.current_curve();
        let _ = db.save_volume_curve(current_date, "RTH", &curve);
        state.rvol_curves.push(curve);
        if state.rvol_curves.len() > 20 {
            state.rvol_curves.remove(0);
        }
        state
            .pipeline
            .rvol
            .load_historical_curve(&state.rvol_curves);
    } else {
        db.save_prior_day_full_with_dnva(
            current_date,
            end_state.high,
            end_state.low,
            end_state.close,
            end_state.va_high,
            end_state.va_low,
            end_state.poc,
            Some(end_state.dnva_high),
            Some(end_state.dnva_low),
            Some(end_state.dnp),
        )
        .map_err(runtime_err)?;
        state.sessions_skipped += 1;
        state.progress.sessions_skipped = state.sessions_skipped;
    }

    Ok(())
}

fn runtime_record_from_replay_snapshot(
    snapshot: SetupRuntimeSnapshot,
    session_date: &str,
    contract_metadata: &ContractMetadata,
    source: &str,
    updated_at_ms: f64,
) -> SetupRuntimeStateRecord {
    SetupRuntimeStateRecord {
        session_date: session_date.to_string(),
        root_symbol: Some(contract_metadata.root_symbol.clone()),
        contract_symbol: Some(contract_metadata.contract_symbol.clone()),
        setup_id: snapshot.setup_id,
        setup_name: snapshot.setup_name,
        state: snapshot.state,
        readiness: snapshot.readiness,
        readiness_score: snapshot.readiness_score,
        met_count: snapshot.met_count as i64,
        total_count: snapshot.total_count as i64,
        met_conditions: snapshot.met_conditions,
        missing_conditions: snapshot.missing_conditions,
        deterministic_all_met: snapshot.deterministic_all_met,
        requires_discretionary: snapshot.requires_discretionary,
        current_price: snapshot.current_price,
        last_evaluated_at_ms: snapshot.last_evaluated_at_ms,
        last_transition_at_ms: snapshot.last_transition_at_ms,
        last_alert_emitted_at_ms: snapshot.last_alert_emitted_at_ms,
        source: source.to_string(),
        updated_at_ms,
    }
}

fn upsert_buffered_setup_runtime(
    records: &mut Vec<SetupRuntimeStateRecord>,
    record: SetupRuntimeStateRecord,
) {
    if let Some(existing) = records.iter_mut().find(|existing| {
        existing.session_date == record.session_date && existing.setup_id == record.setup_id
    }) {
        *existing = record;
    } else {
        records.push(record);
    }
}

fn persist_attention_for_replay(
    db: &Database,
    params: &BackfillJobParams,
    state: &BackfillRunState,
    snapshot: &MarketState,
    _current_date: &str,
    source: &str,
    timestamp_ms: f64,
) -> Result<(), BackfillJobError> {
    if state.buffers.event_buffer.is_empty() {
        return Ok(());
    }
    let prior_active_signals = db
        .query_attention_signals(&AttentionSignalQuery {
            status: Some("active".to_string()),
            min_priority: None,
            include_expired: false,
            cursor_signal_id: None,
            since_ms: None,
            trading_day: Some(snapshot.trading_day.clone()),
            root_symbol: Some(snapshot.root_symbol.clone()).filter(|v| !v.is_empty()),
            contract_symbol: Some(snapshot.contract_symbol.clone()).filter(|v| !v.is_empty()),
            source: Some(source.to_string()),
            job_id: Some(params.job_id.clone()),
            limit: 250,
            ..AttentionSignalQuery::default()
        })
        .map_err(runtime_err)?;
    let composer = SignalComposer::default();
    let output = composer.compose(SignalComposerInput {
        pulse_kind: AttentionPulseKind::EventDriven,
        events: &state.buffers.event_buffer,
        setup_states: &state.buffers.setup_runtime_states,
        risk_state: None,
        market_snapshot: snapshot,
        prior_active_signals: &prior_active_signals,
        timestamp_ms,
        source,
        job_id: Some(params.job_id.as_str()),
    });
    for signal in &output.signals {
        db.upsert_attention_signal(signal).map_err(runtime_err)?;
    }
    for event in &output.signal_events {
        db.insert_attention_signal_event(event)
            .map_err(runtime_err)?;
    }
    for idea in &output.idea_cards {
        db.upsert_trade_idea_card(idea).map_err(runtime_err)?;
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
        .signal_performance_filtered(
            None,
            None,
            None,
            Some("backtest"),
            Some(&params.job_id),
            None,
        )
        .map_err(runtime_err)?;
    let trades = serde_json::json!({ "signalPerformance": perf });
    let params_json = serde_json::json!({
        "jobId": params.job_id,
        "startDate": params.start_date,
        "endDate": params.end_date,
        "force": params.force,
        "setupIds": params.setup_ids,
        "jobType": params.job_type.as_str(),
        "engineVersion": current_engine_version(),
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
    use crate::db::Database;
    use crate::feed::scid_reader::{ScidReader, SCID_RECORD_SIZE};
    use std::io::Write;
    use std::sync::atomic::AtomicBool;
    use tempfile::NamedTempFile;

    const SCID_HEADER_SIZE_TEST: usize = 56;
    const SCID_MAGIC_TEST: &[u8; 4] = b"SCID";
    const SC_TO_UNIX_EPOCH_US_TEST: i64 = 2_209_161_600_000_000;

    fn write_scid_header(file: &mut NamedTempFile) {
        let mut header = vec![0_u8; SCID_HEADER_SIZE_TEST];
        header[0..4].copy_from_slice(SCID_MAGIC_TEST);
        header[4..8].copy_from_slice(&(SCID_HEADER_SIZE_TEST as u32).to_le_bytes());
        header[8..12].copy_from_slice(&(SCID_RECORD_SIZE as u32).to_le_bytes());
        file.write_all(&header).expect("header");
    }

    fn write_record(file: &mut NamedTempFile, timestamp_ms: f64, price: f32) {
        let mut rec = [0_u8; SCID_RECORD_SIZE];
        let unix_us = (timestamp_ms * 1_000.0).round() as i64;
        let sc_us = SC_TO_UNIX_EPOCH_US_TEST + unix_us;
        rec[0..8].copy_from_slice(&sc_us.to_le_bytes());
        rec[12..16].copy_from_slice(&(price + 0.25).to_le_bytes());
        rec[16..20].copy_from_slice(&(price - 0.25).to_le_bytes());
        rec[20..24].copy_from_slice(&price.to_le_bytes());
        rec[24..28].copy_from_slice(&(1_u32).to_le_bytes());
        rec[28..32].copy_from_slice(&(2_u32).to_le_bytes());
        rec[32..36].copy_from_slice(&(0_u32).to_le_bytes());
        rec[36..40].copy_from_slice(&(2_u32).to_le_bytes());
        file.write_all(&rec).expect("record");
    }

    #[test]
    fn summary_computes_close_vs_levels() {
        let state = MarketState {
            last_price: 21010.0,
            ib_high: 21020.0,
            ib_low: 20980.0,
            vwap: 21000.0,
            poc: 21005.0,
            ..Default::default()
        };

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

    #[test]
    fn segment_buffers_track_high_low_ticks_and_volume() {
        let mut segment = SegmentBuffers::default();
        segment.observe_trade(21000.0, 4.0);
        segment.observe_trade(20998.5, 2.0);
        segment.observe_trade(21002.0, 1.0);

        assert!(segment.has_data());
        assert_eq!(segment.open_price, 21000.0);
        assert_eq!(segment.high, 21002.0);
        assert_eq!(segment.low, 20998.5);
        assert_eq!(segment.tick_count, 3);
        assert_eq!(segment.volume, 7.0);
    }

    #[test]
    fn segment_buffers_reset_prevents_asia_data_leaking_into_london() {
        let mut segment = SegmentBuffers::default();
        segment.observe_trade(20995.0, 3.0);
        segment.observe_trade(21001.0, 1.0);
        assert_eq!(segment.high, 21001.0);
        assert_eq!(segment.low, 20995.0);

        // Simulate Asia→London boundary reset.
        segment = SegmentBuffers::default();
        segment.observe_trade(21010.0, 5.0);
        segment.observe_trade(21012.0, 2.0);

        assert_eq!(segment.open_price, 21010.0);
        assert_eq!(segment.low, 21010.0);
        assert_eq!(segment.high, 21012.0);
        assert_eq!(segment.tick_count, 2);
        assert_eq!(segment.volume, 7.0);
    }

    #[test]
    fn backfill_reports_non_monotonic_tick_warnings() {
        let mut file = NamedTempFile::new().expect("temp");
        write_scid_header(&mut file);
        let base = chrono::Utc
            .with_ymd_and_hms(2026, 3, 5, 21, 30, 0)
            .single()
            .expect("base timestamp")
            .timestamp_millis() as f64;
        write_record(&mut file, base, 21000.0);
        write_record(&mut file, base, 21000.25);
        write_record(&mut file, base - 1.0, 21000.5);
        write_record(&mut file, base + 2.0, 21000.75);
        file.flush().expect("flush");

        let db = Database::open(":memory:").expect("db");
        let cancel_flag = AtomicBool::new(false);
        let result = run_backfill_job(
            &ScidReader::new(file.path()),
            &db,
            &BackfillJobParams {
                job_id: "job-1".to_string(),
                job_type: HistoricalJobType::ResearchBackfill,
                start_date: Some("2026-03-05".to_string()),
                end_date: Some("2026-03-05".to_string()),
                force: true,
                run_rules: false,
                setup_ids: None,
            },
            |_| {},
            &cancel_flag,
        )
        .expect("backfill");

        assert_eq!(
            result
                .scid_timestamp_monotonicity
                .skipped_non_monotonic_ticks,
            2
        );
        assert!(result
            .warnings
            .iter()
            .any(|warning| warning.contains("non-monotonic SCID ticks")));
    }
}
