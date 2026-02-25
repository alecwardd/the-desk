use std::time::Duration;

use tauri::State;
use the_desk_backend::db::{SessionEventInput, SessionEventRecord};
use the_desk_backend::dtc::{run_mock_dtc_server, DtcEvent, TradeSide};
use the_desk_backend::recording::{ReplayEngine, SessionRecorder};
use the_desk_backend::risk::RiskState;
use the_desk_backend::rules::SetupDefinition;

use super::AppState;

/// Frontend payload describing a trade to record.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TradeInput {
    pub setup_id: Option<String>,
    pub direction: String,
    pub size: f64,
    pub entry_price: f64,
    #[serde(default)]
    pub exit_price: Option<f64>,
    #[serde(default)]
    pub result_r: Option<f64>,
}

/// A single message in a Claude API conversation (role + content).
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct ClaudeMessage {
    pub role: String,
    pub content: String,
}

#[derive(serde::Serialize)]
struct ClaudeRequest {
    model: String,
    max_tokens: u32,
    messages: Vec<ClaudeMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    system: Option<String>,
}

#[derive(serde::Deserialize)]
struct ClaudeResponse {
    content: Vec<ClaudeContentBlock>,
}

#[derive(serde::Deserialize)]
struct ClaudeContentBlock {
    #[serde(rename = "type")]
    block_type: String,
    text: Option<String>,
}

fn anthropic_api_key() -> Result<String, String> {
    std::env::var("ANTHROPIC_API_KEY")
        .or_else(|_| std::env::var("CLAUDE_API_KEY"))
        .map_err(|_| {
            "Missing API key: set ANTHROPIC_API_KEY (or CLAUDE_API_KEY) in environment".to_string()
        })
}

/// Connect to a DTC server and begin streaming market data for the given symbol.
#[tauri::command]
pub async fn connect_dtc(
    state: State<'_, AppState>,
    host: String,
    port: u16,
    symbol: String,
) -> Result<(), String> {
    let mut dtc = state.dtc.lock().await;
    dtc.start_live_feed(&host, port, &symbol)
        .await
        .map_err(|e| e.to_string())
}

/// Disconnect the active DTC data feed.
#[tauri::command]
pub async fn disconnect_dtc(state: State<'_, AppState>) -> Result<(), String> {
    let mut dtc = state.dtc.lock().await;
    dtc.disconnect().await.map_err(|e| e.to_string())
}

/// Return the current DTC connection state as a human-readable string.
#[tauri::command]
pub async fn dtc_status(state: State<'_, AppState>) -> Result<String, String> {
    let dtc = state.dtc.lock().await;
    let status = match dtc.state() {
        the_desk_backend::dtc::ConnectionState::Disconnected => "disconnected",
        the_desk_backend::dtc::ConnectionState::Connecting => "connecting",
        the_desk_backend::dtc::ConnectionState::EncodingNegotiated => "negotiating",
        the_desk_backend::dtc::ConnectionState::Authenticated => "authenticated",
        the_desk_backend::dtc::ConnectionState::Subscribed => "connected",
    };
    Ok(status.to_string())
}

/// List all persisted setup definitions.
#[tauri::command]
pub async fn list_setups(state: State<'_, AppState>) -> Result<Vec<SetupDefinition>, String> {
    let db = state.db.lock().await;
    db.list_setups().map_err(|e| e.to_string())
}

/// Create or update a setup definition in the database.
#[tauri::command]
pub async fn create_setup(
    state: State<'_, AppState>,
    setup: SetupDefinition,
) -> Result<SetupDefinition, String> {
    let db = state.db.lock().await;
    db.upsert_setup(&setup).map_err(|e| e.to_string())?;
    Ok(setup)
}

/// Return the current intraday risk state snapshot.
#[tauri::command]
pub async fn get_risk_state(state: State<'_, AppState>) -> Result<RiskState, String> {
    let risk = state.risk.lock().await;
    Ok(risk.state())
}

/// Begin a new trading session, resetting pipelines and starting a recorder.
#[tauri::command]
pub async fn start_session(state: State<'_, AppState>) -> Result<String, String> {
    let session_id = uuid::Uuid::new_v4().to_string();
    *state.session_id.lock().await = Some(session_id.clone());

    state.pipelines.lock().await.reset_session();
    state.rules.lock().await.reset();

    let today = chrono::Local::now().format("%Y-%m-%d").to_string();
    let db = state.db.lock().await;
    if let Ok(Some((high, low, close))) = db.load_prior_day(&today) {
        drop(db);
        state
            .pipelines
            .lock()
            .await
            .levels
            .set_prior_day(high, low, close);
        let db = state.db.lock().await;
        db.add_event(&SessionEventInput {
            event_type: "session_start".to_string(),
            setup_id: None,
            data: serde_json::json!({ "sessionId": session_id }),
        })
        .map_err(|e| e.to_string())?;
    } else {
        db.add_event(&SessionEventInput {
            event_type: "session_start".to_string(),
            setup_id: None,
            data: serde_json::json!({ "sessionId": session_id }),
        })
        .map_err(|e| e.to_string())?;
    }

    let dir = super::data_dir();
    let rec_path = dir
        .join(format!("session_{session_id}.desk"))
        .to_string_lossy()
        .to_string();
    *state.recorder.lock().await = SessionRecorder::new(rec_path);

    Ok(session_id)
}

/// End the current session, flush the recording, and persist prior-day levels.
#[tauri::command]
pub async fn stop_session(state: State<'_, AppState>) -> Result<(), String> {
    let session_id = state.session_id.lock().await.take();

    let pipelines = state.pipelines.lock().await;
    let session_high = pipelines.levels.session_high;
    let session_low = pipelines.levels.session_low;
    let last_price = pipelines.levels.last_price;
    drop(pipelines);

    let db = state.db.lock().await;
    db.add_event(&SessionEventInput {
        event_type: "session_stop".to_string(),
        setup_id: None,
        data: serde_json::json!({ "sessionId": session_id }),
    })
    .map_err(|e| e.to_string())?;

    if session_high > 0.0 {
        let today = chrono::Local::now().format("%Y-%m-%d").to_string();
        db.save_prior_day(&today, session_high, session_low, last_price)
            .map_err(|e| e.to_string())?;
    }
    drop(db);

    let recorder = state.recorder.lock().await;
    recorder.flush().map_err(|e| e.to_string())?;

    Ok(())
}

/// Persist an arbitrary session event to the database.
#[tauri::command]
pub async fn add_session_event(
    state: State<'_, AppState>,
    event: SessionEventInput,
) -> Result<(), String> {
    let db = state.db.lock().await;
    db.add_event(&event).map_err(|e| e.to_string())
}

/// Record a trade event and update risk state if a result is provided.
#[tauri::command]
pub async fn add_trade(state: State<'_, AppState>, trade: TradeInput) -> Result<(), String> {
    let db = state.db.lock().await;
    db.add_event(&SessionEventInput {
        event_type: "trade_entry".to_string(),
        setup_id: trade.setup_id.clone(),
        data: serde_json::json!({
            "direction": trade.direction,
            "size": trade.size,
            "entryPrice": trade.entry_price,
            "exitPrice": trade.exit_price,
            "resultR": trade.result_r,
        }),
    })
    .map_err(|e| e.to_string())?;
    if let Some(result_r) = trade.result_r {
        drop(db);
        let mut risk = state.risk.lock().await;
        risk.record_trade_result(result_r);
        let snapshot = risk.state();
        drop(risk);
        let db = state.db.lock().await;
        db.save_risk_state(&snapshot).map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// Send messages to the Anthropic Claude API and return the text response.
#[tauri::command]
pub async fn call_claude_api(
    messages: Vec<ClaudeMessage>,
    system: Option<String>,
) -> Result<String, String> {
    if messages.is_empty() {
        return Err("No messages provided".to_string());
    }

    let api_key = anthropic_api_key()?;
    let model =
        std::env::var("ANTHROPIC_MODEL").unwrap_or_else(|_| "claude-3-5-haiku-latest".to_string());
    let request_body = ClaudeRequest {
        model,
        max_tokens: 300,
        messages,
        system,
    };

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .map_err(|e| format!("Failed to build HTTP client: {e}"))?;

    let response = client
        .post("https://api.anthropic.com/v1/messages")
        .header("x-api-key", api_key)
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .json(&request_body)
        .send()
        .await
        .map_err(|e| format!("Claude API request failed: {e}"))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "Failed to read error body".to_string());
        return Err(format!("Claude API error ({status}): {body}"));
    }

    let parsed: ClaudeResponse = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse Claude response: {e}"))?;

    let text = parsed
        .content
        .into_iter()
        .find(|b| b.block_type == "text")
        .and_then(|b| b.text)
        .filter(|t| !t.trim().is_empty())
        .ok_or_else(|| "Claude returned no text output".to_string())?;

    Ok(text)
}

/// Return the most recent session events, newest first.
#[tauri::command]
pub async fn list_session_events(
    state: State<'_, AppState>,
    limit: Option<usize>,
) -> Result<Vec<SessionEventRecord>, String> {
    let db = state.db.lock().await;
    db.list_session_events(limit.unwrap_or(200))
        .map_err(|e| e.to_string())
}

/// Load a compressed session recording from disk into the replay engine.
#[tauri::command]
pub async fn load_recording(state: State<'_, AppState>, path: String) -> Result<usize, String> {
    let entries = ReplayEngine::load(&path).map_err(|e| e.to_string())?;
    let mut replay = state.replay.lock().await;
    if let Some(stop_tx) = replay.stop_tx.take() {
        let _ = stop_tx.send(true);
    }
    if let Some(task) = replay.task.take() {
        task.abort();
    }
    replay.cursor = 0;
    replay.entries = entries;
    replay.source_path = Some(path);
    replay.is_playing = false;
    Ok(replay.entries.len())
}

/// Begin or resume playback of a loaded recording at the given speed multiplier.
#[tauri::command]
pub async fn start_replay(state: State<'_, AppState>, speed: Option<f64>) -> Result<(), String> {
    let mut replay = state.replay.lock().await;
    if replay.entries.is_empty() {
        return Err("No replay loaded. Call load_recording first.".to_string());
    }
    if let Some(stop_tx) = replay.stop_tx.take() {
        let _ = stop_tx.send(true);
    }
    if let Some(task) = replay.task.take() {
        task.abort();
    }

    let start_index = replay.cursor.min(replay.entries.len().saturating_sub(1));
    let entries = replay.entries.clone();
    let speed = speed.unwrap_or(1.0).clamp(0.25, 8.0);
    let tx = state.dtc_tx.clone();
    let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
    replay.stop_tx = Some(stop_tx);
    replay.task = Some(tauri::async_runtime::spawn(async move {
        let _ = tx.send(DtcEvent::Connected);
        for index in start_index..entries.len() {
            if *stop_rx.borrow() {
                break;
            }
            if index > start_index {
                let prev = entries[index - 1].timestamp;
                let curr = entries[index].timestamp;
                let wait_secs = ((curr - prev).max(0.0) / speed).min(5.0);
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs_f64(wait_secs)) => {}
                    _ = stop_rx.changed() => { break; }
                }
            }
            if let Some(event) = replay_entry_to_dtc_event(&entries[index]) {
                let _ = tx.send(event);
            }
        }
        let _ = tx.send(DtcEvent::Disconnected);
    }));
    replay.speed = speed;
    replay.is_playing = true;
    Ok(())
}

/// Pause the active replay without resetting the cursor position.
#[tauri::command]
pub async fn pause_replay(state: State<'_, AppState>) -> Result<(), String> {
    let mut replay = state.replay.lock().await;
    if let Some(stop_tx) = replay.stop_tx.take() {
        let _ = stop_tx.send(true);
    }
    if let Some(task) = replay.task.take() {
        task.abort();
    }
    replay.is_playing = false;
    Ok(())
}

/// Move the replay cursor to a specific entry index.
#[tauri::command]
pub async fn seek_replay(state: State<'_, AppState>, index: usize) -> Result<(), String> {
    let mut replay = state.replay.lock().await;
    if replay.entries.is_empty() {
        return Err("No replay loaded".to_string());
    }
    replay.cursor = index.min(replay.entries.len().saturating_sub(1));
    Ok(())
}

/// Stop the active replay and reset the cursor to the beginning.
#[tauri::command]
pub async fn stop_replay(state: State<'_, AppState>) -> Result<(), String> {
    let mut replay = state.replay.lock().await;
    if let Some(stop_tx) = replay.stop_tx.take() {
        let _ = stop_tx.send(true);
    }
    if let Some(task) = replay.task.take() {
        task.abort();
    }
    replay.cursor = 0;
    replay.is_playing = false;
    Ok(())
}

/// Start the mock DTC server and auto-connect for development.
#[tauri::command]
pub async fn start_mock_feed(state: State<'_, AppState>) -> Result<(), String> {
    let addr = "127.0.0.1:11099";

    tauri::async_runtime::spawn(async move {
        if let Err(e) = run_mock_dtc_server(addr).await {
            eprintln!("Mock server error: {e}");
        }
    });

    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut dtc = state.dtc.lock().await;
    dtc.start_live_feed("127.0.0.1", 11099, "NQ")
        .await
        .map_err(|e| e.to_string())
}

/// Manually set prior-day high/low/close levels and persist them.
#[tauri::command]
pub async fn set_prior_day_levels(
    state: State<'_, AppState>,
    high: f64,
    low: f64,
    close: f64,
) -> Result<(), String> {
    state
        .pipelines
        .lock()
        .await
        .levels
        .set_prior_day(high, low, close);
    let today = chrono::Local::now().format("%Y-%m-%d").to_string();
    let db = state.db.lock().await;
    db.save_prior_day(&today, high, low, close)
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn replay_entry_to_dtc_event(
    entry: &the_desk_backend::recording::RecordingEntry,
) -> Option<DtcEvent> {
    match entry.record_type.as_str() {
        "trade" => {
            let price = entry.payload.get("price")?.as_f64()?;
            let volume = entry.payload.get("volume")?.as_f64()?;
            let side = match entry.payload.get("side").and_then(|s| s.as_str()) {
                Some("buy") => TradeSide::Buy,
                Some("sell") => TradeSide::Sell,
                _ => TradeSide::Unknown,
            };
            Some(DtcEvent::Trade {
                symbol_id: 1,
                price,
                volume,
                side,
                timestamp: entry.timestamp,
            })
        }
        "quote" => {
            let bid = entry.payload.get("bid")?.as_f64()?;
            let ask = entry.payload.get("ask")?.as_f64()?;
            let bid_size = entry
                .payload
                .get("bidSize")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let ask_size = entry
                .payload
                .get("askSize")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            Some(DtcEvent::Quote {
                symbol_id: 1,
                bid,
                ask,
                bid_size,
                ask_size,
                timestamp: entry.timestamp,
            })
        }
        _ => None,
    }
}
