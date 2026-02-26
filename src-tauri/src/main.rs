#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod commands;

use std::time::{Duration, Instant};

use chrono::{TimeZone, Timelike, Utc};
use chrono_tz::US::Eastern;
use tauri::async_runtime::JoinHandle;
use tauri::{AppHandle, Emitter, Manager};
use the_desk_backend::db::Database;
use the_desk_backend::dtc::DtcClient;
use the_desk_backend::feed::scid_reader::ScidReader;
use the_desk_backend::feed::{load_feed_config, FeedEvent, TradeSide};
use the_desk_backend::pipelines::PipelineEngine;
use the_desk_backend::recording::{RecordingEntry, SessionRecorder};
use the_desk_backend::risk::{RiskConfig, RiskTracker};
use the_desk_backend::rules::RulesEngine;
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

pub(crate) struct AppState {
    pub dtc: Mutex<DtcClient>,
    pub pipelines: Mutex<PipelineEngine>,
    pub rules: Mutex<RulesEngine>,
    pub risk: Mutex<RiskTracker>,
    pub db: Mutex<Database>,
    pub recorder: Mutex<SessionRecorder>,
    pub dtc_tx: broadcast::Sender<FeedEvent>,
    pub session_id: Mutex<Option<String>>,
    pub replay: Mutex<ReplayRuntime>,
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

/// NQ session boundaries in Eastern Time (hour * 60 + minute).
const RTH_OPEN_ET: i32 = 9 * 60 + 30; // 09:30
const RTH_CLOSE_ET: i32 = 16 * 60 + 15; // 16:15
const GLOBEX_OPEN_ET: i32 = 18 * 60; // 18:00

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionType {
    Rth,
    Globex,
    Unknown,
}

fn classify_session(et_minutes: i32) -> SessionType {
    if (RTH_OPEN_ET..RTH_CLOSE_ET).contains(&et_minutes) {
        SessionType::Rth
    } else if !(RTH_OPEN_ET..GLOBEX_OPEN_ET).contains(&et_minutes) {
        SessionType::Globex
    } else {
        SessionType::Unknown
    }
}

fn et_minutes_from_timestamp(timestamp_ms: f64) -> Option<i32> {
    let ts = timestamp_ms as i64;
    Utc.timestamp_millis_opt(ts).single().map(|utc| {
        let et = utc.with_timezone(&Eastern);
        (et.hour() as i32 * 60) + et.minute() as i32
    })
}

/// Background task: listens to feed events and drives pipelines, rules, and UI emission.
async fn processing_loop(handle: AppHandle, mut rx: broadcast::Receiver<FeedEvent>) {
    let session_start = Instant::now();
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
    let mut tick_buffer: Vec<(f64, f64, f64, f64, f64, bool, String)> = Vec::with_capacity(128);

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

                // --- Session boundary detection ---
                if let Some(et_min) = et_minutes_from_timestamp(timestamp) {
                    let new_session = classify_session(et_min);
                    if new_session != current_session_type
                        && current_session_type != SessionType::Unknown
                    {
                        let mut pipelines = state.pipelines.lock().await;
                        let end_state = pipelines.session_end_state();
                        if current_session_type == SessionType::Rth {
                            let date = session_date_from_timestamp_ms(timestamp);
                            let _ = state.db.lock().await.save_prior_day_full(
                                &date,
                                end_state.high,
                                end_state.low,
                                end_state.close,
                                end_state.va_high,
                                end_state.va_low,
                                end_state.poc,
                            );
                            pipelines.levels.set_prior_day(
                                end_state.high,
                                end_state.low,
                                end_state.close,
                            );
                            pipelines.levels.set_prior_profile(
                                end_state.va_high,
                                end_state.va_low,
                                end_state.poc,
                            );
                        }
                        pipelines.reset_session();
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
                    current_session_type = new_session;
                }

                let minute_of_session = minute_of_session(timestamp, session_start);

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
                    tick_buffer.push((timestamp, price, volume, bid, ask, is_buy, session_date));

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
                            if let Ok(alert_json) = serde_json::to_value(&alert) {
                                let mut recorder = state.recorder.lock().await;
                                recorder.push_alert(&alert_json);
                            }
                        }
                    }
                    rules.update_prev_market(&market);
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
                // Flush remaining ticks before exit
                let state = handle.state::<AppState>();
                if !tick_buffer.is_empty() {
                    let _ = state.db.lock().await.insert_raw_ticks_batch(&tick_buffer);
                }
                break;
            }
        }
    }
}

fn session_date_from_timestamp_ms(timestamp_ms: f64) -> String {
    let ts = timestamp_ms as i64;
    if let Some(dt) = Utc.timestamp_millis_opt(ts).single() {
        dt.with_timezone(&Eastern).format("%Y-%m-%d").to_string()
    } else {
        chrono::Local::now().format("%Y-%m-%d").to_string()
    }
}

fn minute_of_session(timestamp: f64, session_start: Instant) -> i32 {
    let dt_utc = if timestamp > 1_000_000_000_000.0 {
        Utc.timestamp_millis_opt(timestamp as i64).single()
    } else if timestamp > 1_000_000_000.0 {
        Utc.timestamp_opt(timestamp as i64, 0).single()
    } else {
        None
    };

    if let Some(utc) = dt_utc {
        let et = utc.with_timezone(&Eastern);
        let total_minutes = (et.hour() as i32 * 60) + et.minute() as i32;
        let rth_open_minutes = (9 * 60) + 30;
        return total_minutes - rth_open_minutes;
    }

    // Mock feeds often emit synthetic timestamps; keep a short overnight pre-roll.
    session_start.elapsed().as_secs() as i32 - 30
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
    let today = chrono::Local::now().format("%Y-%m-%d").to_string();
    if let Ok(Some((high, low, close, va_h, va_l, poc))) = db.load_prior_day_full(&today) {
        pipelines.levels.set_prior_day(high, low, close);
        if let (Some(vh), Some(vl), Some(pc)) = (va_h, va_l, poc) {
            pipelines.levels.set_prior_profile(vh, vl, pc);
        }
    }

    let state = AppState {
        dtc: Mutex::new(DtcClient::new(tx.clone())),
        pipelines: Mutex::new(pipelines),
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
            commands::call_claude_api,
            commands::load_recording,
            commands::start_replay,
            commands::pause_replay,
            commands::seek_replay,
            commands::stop_replay,
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
                    return;
                }
                let state = app_handle.state::<AppState>();

                // Bulk backfill: feed directly into pipeline engine (bypass broadcast channel)
                if let Ok(ticks) = reader.read_bulk() {
                    if !ticks.is_empty() {
                        let mut pipelines = state.pipelines.lock().await;
                        let mut last_bid = 0.0_f64;
                        let mut last_ask = 0.0_f64;
                        for tick in &ticks {
                            let is_buy = matches!(tick.side, TradeSide::Buy);
                            let minute = minute_of_session(tick.timestamp_ms, Instant::now());
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
                            "Backfill complete: {} ticks replayed into pipelines",
                            ticks.len()
                        );
                    }
                }

                // Start live tail loop using broadcast channel for new ticks only
                let (stop_tx, stop_rx) = watch::channel(false);
                *state.scid_shutdown_tx.lock().await = Some(stop_tx);
                let task =
                    reader.spawn_tail_loop(state.dtc_tx.clone(), stop_rx, config.flush_poll_ms);
                *state.scid_feed_task.lock().await = Some(task);
            });
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
