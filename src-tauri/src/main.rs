#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod commands;

use std::time::{Duration, Instant};

use tauri::async_runtime::JoinHandle;
use tauri::{AppHandle, Emitter, Manager};
use the_desk_backend::db::Database;
use the_desk_backend::dom_replay::DomReplayClip;
use the_desk_backend::dtc::DtcClient;
use the_desk_backend::feed::scid_reader::ScidReader;
use the_desk_backend::feed::{load_feed_config, resolve_contract_metadata, FeedEvent, TradeSide};
use the_desk_backend::outcome_tracker;
use the_desk_backend::pipelines::{EventDetector, FlowEventEmitter, PipelineEngine};
use the_desk_backend::recording::{RecordingEntry, SessionRecorder};
use the_desk_backend::risk::{RiskConfig, RiskTracker};
use the_desk_backend::rules::RulesEngine;
use the_desk_backend::{
    classify_delta_segment, classify_session, et_minutes_from_timestamp, globex_open_ms,
    minute_of_session_from_timestamp, session_date_from_timestamp_ms, DeltaSegment, SessionType,
};
use tokio::sync::{broadcast, watch, Mutex};

pub(crate) struct ReplayRuntime {
    pub entries: Vec<RecordingEntry>,
    pub cursor: usize,
    pub speed: f64,
    pub is_playing: bool,
    pub source_path: Option<String>,
    pub task: Option<JoinHandle<()>>,
    pub stop_tx: Option<watch::Sender<bool>>,
}

impl Default for ReplayRuntime {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            cursor: 0,
            speed: 1.0,
            is_playing: false,
            source_path: None,
            task: None,
            stop_tx: None,
        }
    }
}

pub(crate) struct DomReplayRuntime {
    pub clip: Option<DomReplayClip>,
    pub cursor: usize,
    pub speed: f64,
    pub is_playing: bool,
    pub current_timestamp_ms: Option<f64>,
    pub task: Option<JoinHandle<()>>,
    pub stop_tx: Option<watch::Sender<bool>>,
}

impl Default for DomReplayRuntime {
    fn default() -> Self {
        Self {
            clip: None,
            cursor: 0,
            speed: 1.0,
            is_playing: false,
            current_timestamp_ms: None,
            task: None,
            stop_tx: None,
        }
    }
}

pub(crate) struct AppState {
    pub dtc: Mutex<DtcClient>,
    pub pipelines: Mutex<PipelineEngine>,
    pub detector: Mutex<EventDetector>,
    pub flow_emitter: Mutex<FlowEventEmitter>,
    pub rules: Mutex<RulesEngine>,
    pub risk: Mutex<RiskTracker>,
    pub db: Mutex<Database>,
    pub recorder: Mutex<SessionRecorder>,
    pub dtc_tx: broadcast::Sender<FeedEvent>,
    pub session_id: Mutex<Option<String>>,
    pub replay: Mutex<ReplayRuntime>,
    pub dom_replay: Mutex<DomReplayRuntime>,
    pub scid_feed_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    pub scid_shutdown_tx: Mutex<Option<watch::Sender<bool>>>,
}

fn data_dir() -> std::path::PathBuf {
    let home = std::env::var("USERPROFILE")
        .or_else(|_| std::env::var("HOME"))
        .unwrap_or_else(|_| ".".to_string());
    let dir = std::path::PathBuf::from(home).join(".the-desk");
    std::fs::create_dir_all(&dir).ok();
    dir
}

/// Background task: listens to feed events and drives pipelines, rules, and UI emission.
async fn processing_loop(handle: AppHandle, mut rx: broadcast::Receiver<FeedEvent>) {
    let mut last_market_emit = Instant::now();
    let mut last_risk_emit = Instant::now();
    let mut last_snapshot_persist = Instant::now();
    let mut last_tick_flush = Instant::now();
    let market_interval = Duration::from_millis(250); // 4 Hz
    let risk_interval = Duration::from_secs(2);
    let snapshot_interval = Duration::from_secs(30);
    let tick_flush_interval = Duration::from_millis(500);
    let mut last_bid: f64 = 0.0;
    let mut last_ask: f64 = 0.0;
    let mut current_session_type = SessionType::Unknown;
    let mut current_delta_segment = DeltaSegment::Unknown;
    let feed_contract = resolve_contract_metadata(&load_feed_config());
    let mut tick_buffer: Vec<(f64, f64, f64, f64, f64, bool, String, String, String)> =
        Vec::with_capacity(128);
    let mut event_buffer: Vec<the_desk_backend::pipelines::MarketEvent> = Vec::new();

    loop {
        match rx.recv().await {
            Ok(FeedEvent::Connected) => {
                handle.emit("dtc-status", "connected").ok();
            }
            Ok(FeedEvent::Disconnected) => {
                handle.emit("dtc-status", "disconnected").ok();
            }
            Ok(FeedEvent::Trade {
                price,
                volume,
                side,
                timestamp,
                ..
            }) => {
                let state = handle.state::<AppState>();
                let is_buy = matches!(side, TradeSide::Buy);

                // --- Session and segment boundary detection ---
                if let Some(et_min) = et_minutes_from_timestamp(timestamp) {
                    let new_session = classify_session(et_min);
                    let new_segment = classify_delta_segment(et_min);

                    // Session boundary (RTH↔Globex): full reset, prior-day, volume curve
                    if new_session != current_session_type
                        && current_session_type != SessionType::Unknown
                        && new_session != SessionType::Unknown
                    {
                        let mut pipelines = state.pipelines.lock().await;
                        let end_state = pipelines.session_end_state();
                        {
                            let date = session_date_from_timestamp_ms(timestamp);
                            let session_label = if current_session_type == SessionType::Rth {
                                "RTH"
                            } else {
                                "Globex"
                            };
                            let curve = pipelines.rvol.current_curve();
                            let _ = state.db.lock().await.save_volume_curve(
                                &date,
                                session_label,
                                &curve,
                            );
                        }
                        if current_session_type == SessionType::Rth {
                            let date = session_date_from_timestamp_ms(timestamp);
                            let _ = state.db.lock().await.save_prior_day_full_with_dnva_contract(
                                &date,
                                end_state.high,
                                end_state.low,
                                end_state.close,
                                end_state.va_high,
                                end_state.va_low,
                                end_state.poc,
                                Some(end_state.dnva_high),
                                Some(end_state.dnva_low),
                                Some(end_state.dnp),
                                Some(feed_contract.root_symbol.as_str()),
                                Some(feed_contract.contract_symbol.as_str()),
                            );
                            pipelines.levels.set_prior_day(
                                end_state.high,
                                end_state.low,
                                end_state.close,
                            );
                            pipelines.levels.set_prior_day_contract_context(
                                Some(feed_contract.root_symbol.as_str()),
                                Some(feed_contract.contract_symbol.as_str()),
                                Some(feed_contract.contract_symbol.as_str()),
                            );
                            pipelines.levels.set_prior_profile(
                                end_state.va_high,
                                end_state.va_low,
                                end_state.poc,
                            );
                            pipelines.levels.set_prior_dnva(
                                end_state.dnva_high,
                                end_state.dnva_low,
                                end_state.dnp,
                            );
                        }
                        pipelines.reset_session_with_type(new_session == SessionType::Globex);
                        state.detector.lock().await.reset();
                        state.flow_emitter.lock().await.reset();

                        if !event_buffer.is_empty() {
                            let _ = state
                                .db
                                .lock()
                                .await
                                .insert_market_events_batch(&event_buffer);
                            event_buffer.clear();
                        }

                        handle
                            .emit(
                                "session-boundary",
                                serde_json::json!({
                                    "from": format!("{:?}", current_session_type),
                                    "to": format!("{:?}", new_session),
                                    "timestampMs": timestamp
                                }),
                            )
                            .ok();
                    }
                    // Segment-only boundary (Asia→London at 2 AM): delta reset, keep Globex range
                    else if new_segment != current_delta_segment
                        && current_delta_segment != DeltaSegment::Unknown
                        && new_segment != DeltaSegment::Unknown
                    {
                        let mut pipelines = state.pipelines.lock().await;
                        pipelines.reset_segment(new_segment);
                    }

                    if new_session != SessionType::Unknown {
                        current_session_type = new_session;
                    }
                    if new_segment != DeltaSegment::Unknown {
                        current_delta_segment = new_segment;
                    }
                    if new_session == SessionType::Unknown {
                        continue;
                    }
                }

                let minute_of_session = minute_of_session_from_timestamp(timestamp);

                // Update pipelines
                {
                    let mut pipelines = state.pipelines.lock().await;
                    pipelines.on_trade_with_timestamp(
                        price,
                        volume,
                        is_buy,
                        minute_of_session,
                        timestamp,
                    );

                    // Detect and buffer market events in both RTH and Globex.
                    let bid = if last_bid > 0.0 {
                        last_bid
                    } else {
                        price - 0.25
                    };
                    let ask = if last_ask > 0.0 {
                        last_ask
                    } else {
                        price + 0.25
                    };
                    let snapshot = pipelines.snapshot(bid, ask);
                    let session_date = session_date_from_timestamp_ms(timestamp);
                    let mut detector = state.detector.lock().await;
                    detector.detect_into(
                        &snapshot,
                        timestamp,
                        &session_date,
                        minute_of_session,
                        &mut event_buffer,
                    );
                    drop(detector);
                    let mut flow_emitter = state.flow_emitter.lock().await;
                    flow_emitter.detect_into(
                        &pipelines,
                        timestamp,
                        &session_date,
                        price,
                        &mut event_buffer,
                    );
                    drop(flow_emitter);

                    if event_buffer.len() >= 50 {
                        let _ = state
                            .db
                            .lock()
                            .await
                            .insert_market_events_batch(&event_buffer);
                        event_buffer.clear();
                    }

                    if last_market_emit.elapsed() >= market_interval {
                        let bid = if last_bid > 0.0 {
                            last_bid
                        } else {
                            price - 0.25
                        };
                        let ask = if last_ask > 0.0 {
                            last_ask
                        } else {
                            price + 0.25
                        };
                        let snapshot = pipelines.snapshot(bid, ask);
                        handle.emit("market-state", &snapshot).ok();
                        let _ = state.db.lock().await.upsert_feature_state(
                            timestamp,
                            &serde_json::to_value(&snapshot)
                                .unwrap_or_else(|_| serde_json::json!({})),
                        );
                        last_market_emit = Instant::now();
                    }
                    if last_snapshot_persist.elapsed() >= snapshot_interval {
                        let snapshot = pipelines.snapshot(
                            if last_bid > 0.0 {
                                last_bid
                            } else {
                                price - 0.25
                            },
                            if last_ask > 0.0 {
                                last_ask
                            } else {
                                price + 0.25
                            },
                        );
                        let payload = serde_json::to_value(&snapshot)
                            .unwrap_or_else(|_| serde_json::json!({}));
                        let _ = state
                            .db
                            .lock()
                            .await
                            .insert_pipeline_snapshot(timestamp, &payload);
                        let micro = serde_json::json!({
                            "tapePace5s": snapshot.tape_pace_5s,
                            "tapePace30s": snapshot.tape_pace_30s,
                            "tapePace5m": snapshot.tape_pace_5m,
                            "tapeAcceleration": snapshot.tape_acceleration,
                            "tapeRawAcceleration": snapshot.tape_raw_acceleration,
                            "pacePercentile": snapshot.pace_percentile,
                            "rollingPacePercentile": snapshot.tape_rolling_percentile,
                            "volumePerSec5s": snapshot.tape_volume_per_sec_5s,
                            "volumePerSec30s": snapshot.tape_volume_per_sec_30s,
                            "volumePerSec5m": snapshot.tape_volume_per_sec_5m,
                            "tapeCoverage5s": snapshot.tape_coverage_5s,
                            "tapeCoverage30s": snapshot.tape_coverage_30s,
                            "tapeCoverage5m": snapshot.tape_coverage_5m,
                            "tapeValid5s": snapshot.tape_valid_5s,
                            "tapeValid30s": snapshot.tape_valid_30s,
                            "tapeValid5m": snapshot.tape_valid_5m,
                            "tapeWindowAnchorTimestampMs": snapshot.tape_window_anchor_timestamp_ms,
                            "tapeLastTradeTimestampMs": snapshot.tape_last_trade_timestamp_ms,
                            "tapeEventTimeLagMs": snapshot.tape_event_time_lag_ms,
                            "dwellAtCurrentPriceMs": snapshot.tape_dwell_at_current_price_ms,
                            "regimeTicksPerSec30mEma": snapshot.tape_regime_ticks_per_sec_30m_ema,
                            "regimeVolumePerSec30mEma": snapshot.tape_regime_volume_per_sec_30m_ema,
                            "imbalanceCount": snapshot.imbalance_count,
                            "absorptionEventCount": snapshot.absorption_event_count,
                            "avgTradeSize": snapshot.avg_trade_size
                        });
                        let _ = state
                            .db
                            .lock()
                            .await
                            .insert_microstructure_snapshot(timestamp, &micro);
                        last_snapshot_persist = Instant::now();
                    }
                }

                // Buffer raw ticks for batch insert
                {
                    let session_date = session_date_from_timestamp_ms(timestamp);
                    let bid = if last_bid > 0.0 {
                        last_bid
                    } else {
                        price - 0.25
                    };
                    let ask = if last_ask > 0.0 {
                        last_ask
                    } else {
                        price + 0.25
                    };
                    tick_buffer.push((
                        timestamp,
                        price,
                        volume,
                        bid,
                        ask,
                        is_buy,
                        session_date,
                        feed_contract.root_symbol.clone(),
                        feed_contract.contract_symbol.clone(),
                    ));

                    if tick_buffer.len() >= 100 || last_tick_flush.elapsed() >= tick_flush_interval
                    {
                        let _ = state.db.lock().await.insert_raw_ticks_batch(&tick_buffer);
                        tick_buffer.clear();
                        last_tick_flush = Instant::now();
                    }
                }

                // Evaluate rules against all active setups
                {
                    let setups = {
                        let db = state.db.lock().await;
                        db.list_setups().unwrap_or_default()
                    };
                    let risk_state = state.risk.lock().await.state();
                    let market = {
                        let pipelines = state.pipelines.lock().await;
                        let bid = if last_bid > 0.0 {
                            last_bid
                        } else {
                            price - 0.25
                        };
                        let ask = if last_ask > 0.0 {
                            last_ask
                        } else {
                            price + 0.25
                        };
                        pipelines.snapshot(bid, ask)
                    };
                    let mut rules = state.rules.lock().await;
                    if risk_state.at_limit {
                        handle
                            .emit(
                                "setup-suppressed",
                                serde_json::json!({
                                    "reason": "risk_limit_reached",
                                    "dailyPnlR": risk_state.daily_pnl_r
                                }),
                            )
                            .ok();
                    }
                    for setup in &setups {
                        if let Some(alert) = rules.evaluate(setup, &market, risk_state.at_limit) {
                            handle.emit("setup-alert", &alert).ok();
                            let _ = state.db.lock().await.insert_playbook_signal(
                                timestamp,
                                &alert.setup_id,
                                &serde_json::to_value(&alert)
                                    .unwrap_or_else(|_| serde_json::json!({})),
                            );
                            // Track signal outcome for MFE/MAE and resolution
                            let signal_id = format!("{}_{}", alert.setup_id, timestamp as u64);
                            let outcome = the_desk_backend::db::SignalOutcome {
                                signal_id: signal_id.clone(),
                                setup_id: alert.setup_id.clone(),
                                setup_name: Some(alert.setup_name.clone()),
                                session_date: the_desk_backend::session_date_from_timestamp_ms(
                                    timestamp,
                                ),
                                root_symbol: Some(feed_contract.root_symbol.clone()),
                                contract_symbol: Some(feed_contract.contract_symbol.clone()),
                                source: "live".to_string(),
                                job_id: None,
                                fired_at_ms: timestamp,
                                fired_price: alert.current_price,
                                target_price: alert.target_prices.first().copied(),
                                stop_price: alert.stop_price,
                                outcome: "pending".to_string(),
                                outcome_at_ms: None,
                                max_favorable_excursion: None,
                                max_adverse_excursion: None,
                                r_result: None,
                                time_to_outcome_ms: None,
                                rvol_at_fire: Some(market.rvol_ratio),
                                rvol_bucket_at_fire: Some(market.rvol_bucket_index as i32),
                            };
                            let _ = state.db.lock().await.insert_signal_outcome(&outcome);
                            if let Ok(alert_json) = serde_json::to_value(&alert) {
                                let mut recorder = state.recorder.lock().await;
                                recorder.push_alert(&alert_json);
                            }
                        }
                    }
                    rules.update_prev_market(&market);
                }

                // Outcome tracker: update MFE/MAE and resolve signals that hit target/stop
                {
                    let db = state.db.lock().await;
                    let _ = outcome_tracker::on_tick(&db, price, timestamp, None);
                }

                // Throttled risk state emission
                if last_risk_emit.elapsed() >= risk_interval {
                    let risk_state = state.risk.lock().await.state();
                    handle.emit("risk-state", &risk_state).ok();
                    last_risk_emit = Instant::now();
                }

                // Record trade
                {
                    let mut recorder = state.recorder.lock().await;
                    recorder.push(RecordingEntry {
                        timestamp,
                        record_type: "trade".to_string(),
                        payload: serde_json::json!({
                            "price": price,
                            "volume": volume,
                            "side": if is_buy { "buy" } else { "sell" }
                        }),
                    });
                }
            }
            Ok(FeedEvent::Quote { bid, ask, .. }) => {
                last_bid = bid;
                last_ask = ask;
                let state = handle.state::<AppState>();
                let pipelines = state.pipelines.lock().await;
                let snapshot = pipelines.snapshot(bid, ask);
                handle.emit("market-state", &snapshot).ok();
            }
            Ok(FeedEvent::Error { message }) => {
                handle.emit("dtc-status", &format!("error: {message}")).ok();
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                eprintln!("Processing loop lagged by {n} messages");
            }
            Err(broadcast::error::RecvError::Closed) => {
                let state = handle.state::<AppState>();
                if !tick_buffer.is_empty() {
                    let _ = state.db.lock().await.insert_raw_ticks_batch(&tick_buffer);
                }
                if !event_buffer.is_empty() {
                    let _ = state
                        .db
                        .lock()
                        .await
                        .insert_market_events_batch(&event_buffer);
                }
                break;
            }
        }
    }
}

fn main() {
    let (tx, rx) = broadcast::channel::<FeedEvent>(4096);
    let dir = data_dir();
    let db =
        Database::open(&dir.join("data.db").to_string_lossy()).expect("Failed to open database");
    let mut risk = RiskTracker::new(RiskConfig::default());
    if let Ok(Some(saved_state)) = db.load_risk_state() {
        risk.restore_state(saved_state);
    }

    let mut pipelines = PipelineEngine::new();
    let feed_config = load_feed_config();
    let feed_contract = resolve_contract_metadata(&feed_config);
    pipelines.set_contract_metadata(feed_contract.clone());
    if let Ok(curves) = db.recent_session_volume_curves("RTH", 20) {
        pipelines.rvol.load_historical_curve(&curves);
    }
    if let Ok(curves) = db.recent_session_volume_curves("Globex", 20) {
        pipelines.rvol.load_globex_historical_curve(&curves);
    }
    let today = the_desk_backend::et_now_trading_day();
    if let Ok(Some(prior_ref)) = db.load_prior_day_reference_scoped(
        &today,
        Some(feed_contract.root_symbol.as_str()),
        Some(feed_contract.contract_symbol.as_str()),
    )
    {
        pipelines
            .levels
            .set_prior_day(prior_ref.high, prior_ref.low, prior_ref.close);
        pipelines.levels.set_prior_day_contract_context(
            prior_ref.root_symbol.as_deref(),
            prior_ref.contract_symbol.as_deref(),
            Some(feed_contract.contract_symbol.as_str()),
        );
        if let (Some(vh), Some(vl), Some(pc)) = (prior_ref.va_high, prior_ref.va_low, prior_ref.poc) {
            pipelines.levels.set_prior_profile(vh, vl, pc);
        }
        if let (Some(dh), Some(dl), Some(dp)) =
            (prior_ref.dnva_high, prior_ref.dnva_low, prior_ref.dnp)
        {
            pipelines.levels.set_prior_dnva(dh, dl, dp);
        }
    }
    if let Ok(summaries) = db.list_session_summaries(None, None, None, Some("RTH"), 5) {
        let prior: Vec<the_desk_backend::pipelines::PriorSessionData> = summaries
            .into_iter()
            .filter(|s| s.dnva_high > 0.0 && s.dnva_low > 0.0 && s.dnp > 0.0)
            .map(|s| the_desk_backend::pipelines::PriorSessionData {
                final_delta: s.session_delta,
                dnva_high: s.dnva_high,
                dnva_low: s.dnva_low,
                dnp: s.dnp,
            })
            .collect();
        pipelines.session_inventory.load_prior_sessions(prior);
    }

    let state = AppState {
        dtc: Mutex::new(DtcClient::new(tx.clone())),
        pipelines: Mutex::new(pipelines),
        detector: Mutex::new(EventDetector::new()),
        flow_emitter: Mutex::new(FlowEventEmitter::new()),
        rules: Mutex::new(RulesEngine::default()),
        risk: Mutex::new(risk),
        db: Mutex::new(db),
        recorder: Mutex::new(SessionRecorder::new(
            dir.join("current_session.desk")
                .to_string_lossy()
                .to_string(),
        )),
        dtc_tx: tx,
        session_id: Mutex::new(None),
        replay: Mutex::new(ReplayRuntime::default()),
        dom_replay: Mutex::new(DomReplayRuntime::default()),
        scid_feed_task: Mutex::new(None),
        scid_shutdown_tx: Mutex::new(None),
    };

    tauri::Builder::default()
        .manage(state)
        .invoke_handler(tauri::generate_handler![
            commands::connect_dtc,
            commands::disconnect_dtc,
            commands::dtc_status,
            commands::list_setups,
            commands::create_setup,
            commands::update_setup,
            commands::delete_setup,
            commands::duplicate_setup,
            commands::toggle_setup,
            commands::list_templates,
            commands::get_risk_state,
            commands::get_risk_config,
            commands::save_risk_config,
            commands::init_risk_state,
            commands::get_account_state,
            commands::save_account_state,
            commands::start_session,
            commands::stop_session,
            commands::list_sessions,
            commands::add_session_event,
            commands::add_trade,
            commands::create_trade,
            commands::close_trade,
            commands::list_trades,
            commands::get_open_trade,
            commands::review_trade,
            commands::save_journal_entry,
            commands::get_journal,
            commands::get_memory_brief,
            commands::get_pre_session_briefing,
            commands::call_claude_api,
            commands::load_recording,
            commands::start_replay,
            commands::pause_replay,
            commands::seek_replay,
            commands::stop_replay,
            commands::dom_replay_load,
            commands::dom_replay_start,
            commands::dom_replay_pause,
            commands::dom_replay_stop,
            commands::dom_replay_seek,
            commands::dom_replay_status,
            commands::list_session_events,
            commands::list_recordings,
            commands::start_mock_feed,
            commands::start_scid_feed,
            commands::stop_scid_feed,
            commands::set_prior_day_levels,
        ])
        .setup(move |app| {
            let handle = app.handle().clone();
            tauri::async_runtime::spawn(processing_loop(handle, rx));
            let app_handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                let config = load_feed_config();
                let reader = ScidReader::from_feed_config(&config);
                if !reader.path().exists() {
                    eprintln!(
                        "SCID file not found: {} — skipping auto-feed",
                        reader.path().display()
                    );
                    return;
                }
                eprintln!("SCID file found: {}", reader.path().display());
                let state = app_handle.state::<AppState>();

                // Backfill from the most recent Globex open (6 PM ET yesterday).
                // This gives us the full overnight + RTH session without reading
                // the entire contract history (24M+ ticks).
                let backfill_since_ms = globex_open_ms(1);

                let t0 = Instant::now();
                eprintln!("Starting session backfill (since Globex open)...");

                match reader.read_bulk_since(Some(backfill_since_ms)) {
                    Ok(ticks) if !ticks.is_empty() => {
                        let tick_count = ticks.len();
                        let mut pipelines = state.pipelines.lock().await;
                        let mut last_bid = 0.0_f64;
                        let mut last_ask = 0.0_f64;
                        for tick in &ticks {
                            let is_buy = matches!(tick.side, TradeSide::Buy);
                            let minute = minute_of_session_from_timestamp(tick.timestamp_ms);
                            pipelines.on_trade_with_timestamp(
                                tick.price,
                                tick.volume,
                                is_buy,
                                minute,
                                tick.timestamp_ms,
                            );
                            if tick.bid > 0.0 {
                                last_bid = tick.bid;
                            }
                            if tick.ask > 0.0 {
                                last_ask = tick.ask;
                            }
                        }
                        let last_ts = ticks.last().map(|t| t.timestamp_ms).unwrap_or(0.0);
                        let bid = if last_bid > 0.0 {
                            last_bid
                        } else {
                            ticks.last().map(|t| t.price - 0.25).unwrap_or(0.0)
                        };
                        let ask = if last_ask > 0.0 {
                            last_ask
                        } else {
                            ticks.last().map(|t| t.price + 0.25).unwrap_or(0.0)
                        };
                        let snapshot = pipelines.snapshot(bid, ask);
                        let _ = state.db.lock().await.upsert_feature_state(
                            last_ts,
                            &serde_json::to_value(&snapshot)
                                .unwrap_or_else(|_| serde_json::json!({})),
                        );
                        eprintln!(
                            "Backfill complete: {} ticks in {:.1}s",
                            tick_count,
                            t0.elapsed().as_secs_f64()
                        );
                    }
                    Ok(_) => {
                        eprintln!("No ticks found since Globex open");
                    }
                    Err(e) => {
                        eprintln!("Backfill error: {e}");
                    }
                }

                // Start live tail loop for new ticks
                let (stop_tx, stop_rx) = watch::channel(false);
                *state.scid_shutdown_tx.lock().await = Some(stop_tx);
                let task =
                    reader.spawn_tail_loop(state.dtc_tx.clone(), stop_rx, config.flush_poll_ms);
                *state.scid_feed_task.lock().await = Some(task);
                eprintln!(
                    "Live tail loop started (polling every {}ms)",
                    config.flush_poll_ms
                );
            });
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
