#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod commands;

use std::time::{Duration, Instant};

use chrono::{TimeZone, Timelike, Utc};
use chrono_tz::US::Eastern;
use tauri::async_runtime::JoinHandle;
use tauri::{AppHandle, Emitter, Manager};
use the_desk_backend::db::Database;
use the_desk_backend::dtc::{DtcClient, DtcEvent, TradeSide};
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
    pub dtc_tx: broadcast::Sender<DtcEvent>,
    pub session_id: Mutex<Option<String>>,
    pub replay: Mutex<ReplayRuntime>,
}

fn data_dir() -> std::path::PathBuf {
    let home = std::env::var("USERPROFILE")
        .or_else(|_| std::env::var("HOME"))
        .unwrap_or_else(|_| ".".to_string());
    let dir = std::path::PathBuf::from(home).join(".the-desk");
    std::fs::create_dir_all(&dir).ok();
    dir
}

/// Background task: listens to DTC events and drives pipelines, rules, and UI emission.
async fn processing_loop(handle: AppHandle, mut rx: broadcast::Receiver<DtcEvent>) {
    let session_start = Instant::now();
    let mut last_market_emit = Instant::now();
    let mut last_risk_emit = Instant::now();
    let market_interval = Duration::from_millis(250); // 4 Hz
    let risk_interval = Duration::from_secs(2);
    let mut last_bid: f64 = 0.0;
    let mut last_ask: f64 = 0.0;

    loop {
        match rx.recv().await {
            Ok(DtcEvent::Connected) => {
                handle.emit("dtc-status", "connected").ok();
            }
            Ok(DtcEvent::Disconnected) => {
                handle.emit("dtc-status", "disconnected").ok();
            }
            Ok(DtcEvent::Trade {
                price,
                volume,
                side,
                timestamp,
                ..
            }) => {
                let state = handle.state::<AppState>();
                let is_buy = matches!(side, TradeSide::Buy);

                let minute_of_session = minute_of_session(timestamp, session_start);

                // Update pipelines
                {
                    let mut pipelines = state.pipelines.lock().await;
                    pipelines.on_trade(price, volume, is_buy, minute_of_session);

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
                        last_market_emit = Instant::now();
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
            Ok(DtcEvent::Quote { bid, ask, .. }) => {
                last_bid = bid;
                last_ask = ask;
                let state = handle.state::<AppState>();
                let pipelines = state.pipelines.lock().await;
                let snapshot = pipelines.snapshot(bid, ask);
                handle.emit("market-state", &snapshot).ok();
            }
            Ok(DtcEvent::Error { message }) => {
                handle.emit("dtc-status", &format!("error: {message}")).ok();
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                eprintln!("Processing loop lagged by {n} messages");
            }
            Err(broadcast::error::RecvError::Closed) => {
                break;
            }
        }
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
    let (tx, rx) = broadcast::channel::<DtcEvent>(4096);
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
            commands::set_prior_day_levels,
        ])
        .setup(move |app| {
            let handle = app.handle().clone();
            tauri::async_runtime::spawn(processing_loop(handle, rx));
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
