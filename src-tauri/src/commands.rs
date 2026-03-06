use std::time::Duration;

use tauri::State;
use the_desk_backend::db::{
    AccountStateRecord, JournalEntry, OpenPositionRecord, RiskConfigRecord, SessionEventInput,
    SessionEventRecord, SessionRecord, TradeRecord,
};
use the_desk_backend::dtc::{run_mock_dtc_server, TradeSide};
use the_desk_backend::feed::scid_reader::ScidReader;
use the_desk_backend::feed::{load_feed_config, FeedEvent};
use the_desk_backend::recording::{ReplayEngine, SessionRecorder};
use the_desk_backend::risk::RiskState;
use the_desk_backend::rules::SetupDefinition;
use the_desk_backend::templates;

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
    if let Ok(Some((high, low, close, va_h, va_l, p, dnva_h, dnva_l, dnp))) =
        db.load_prior_day_full(&today)
    {
        drop(db);
        let mut pipelines = state.pipelines.lock().await;
        pipelines.levels.set_prior_day(high, low, close);
        if let (Some(vh), Some(vl), Some(pc)) = (va_h, va_l, p) {
            pipelines.levels.set_prior_profile(vh, vl, pc);
        }
        if let (Some(dh), Some(dl), Some(dp)) = (dnva_h, dnva_l, dnp) {
            pipelines.levels.set_prior_dnva(dh, dl, dp);
        }
        drop(pipelines);
        let db = state.db.lock().await;
        db.add_event(&SessionEventInput {
            event_type: "session_start".to_string(),
            setup_id: None,
            data: serde_json::json!({ "sessionId": session_id }),
            session_id: Some(session_id.clone()),
        })
        .map_err(|e| e.to_string())?;
    } else {
        db.add_event(&SessionEventInput {
            event_type: "session_start".to_string(),
            setup_id: None,
            data: serde_json::json!({ "sessionId": session_id }),
            session_id: Some(session_id.clone()),
        })
        .map_err(|e| e.to_string())?;
    }

    let dir = super::data_dir();
    let today = chrono::Local::now().format("%Y-%m-%d").to_string();
    let rec_path = dir
        .join(format!("{today}_{session_id}.desk"))
        .to_string_lossy()
        .to_string();
    let mut recorder = state.recorder.lock().await;
    *recorder = SessionRecorder::new(rec_path);
    recorder.start();
    drop(recorder);

    Ok(session_id)
}

/// End the current session, flush the recording, and persist prior-day levels.
#[tauri::command]
pub async fn stop_session(state: State<'_, AppState>) -> Result<(), String> {
    let session_id = state.session_id.lock().await.take();

    let pipelines = state.pipelines.lock().await;
    let session_end = pipelines.session_end_state();
    drop(pipelines);

    let db = state.db.lock().await;
    db.add_event(&SessionEventInput {
        event_type: "session_stop".to_string(),
        setup_id: None,
        data: serde_json::json!({ "sessionId": &session_id }),
        session_id: session_id.clone(),
    })
    .map_err(|e| e.to_string())?;

    if session_end.high > 0.0 {
        let today = chrono::Local::now().format("%Y-%m-%d").to_string();
        db.save_prior_day_full_with_dnva(
            &today,
            session_end.high,
            session_end.low,
            session_end.close,
            session_end.va_high,
            session_end.va_low,
            session_end.poc,
            Some(session_end.dnva_high),
            Some(session_end.dnva_low),
            Some(session_end.dnp),
        )
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
    db.add_event(&event).map_err(|e| e.to_string())?;
    Ok(())
}

/// Record a trade event and update risk state if a result is provided.
#[tauri::command]
pub async fn add_trade(state: State<'_, AppState>, trade: TradeInput) -> Result<(), String> {
    let db = state.db.lock().await;
    let session_id = state.session_id.lock().await.clone();
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
        session_id,
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
/// The `model` parameter selects the model tier: "opus" for high-reasoning tasks,
/// "sonnet" for real-time coaching, or omit for the configured default.
#[tauri::command]
pub async fn call_claude_api(
    messages: Vec<ClaudeMessage>,
    system: Option<String>,
    model: Option<String>,
) -> Result<String, String> {
    if messages.is_empty() {
        return Err("No messages provided".to_string());
    }

    let api_key = anthropic_api_key()?;
    let model = match model.as_deref() {
        Some("opus") => "claude-sonnet-4-20250514".to_string(),
        Some("sonnet") => "claude-3-5-haiku-latest".to_string(),
        Some(explicit) => explicit.to_string(),
        None => std::env::var("ANTHROPIC_MODEL")
            .unwrap_or_else(|_| "claude-3-5-haiku-latest".to_string()),
    };
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
        let _ = tx.send(FeedEvent::Connected);
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
            if let Some(event) = replay_entry_to_feed_event(&entries[index]) {
                let _ = tx.send(event);
            }
        }
        let _ = tx.send(FeedEvent::Disconnected);
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

/// Start SCID tail feed using `~/.the-desk/config.toml` feed settings.
#[tauri::command]
pub async fn start_scid_feed(state: State<'_, AppState>) -> Result<(), String> {
    // Stop previous feed task, if any.
    if let Some(stop_tx) = state.scid_shutdown_tx.lock().await.take() {
        let _ = stop_tx.send(true);
    }
    if let Some(task) = state.scid_feed_task.lock().await.take() {
        task.abort();
    }

    let cfg = load_feed_config();
    let reader = ScidReader::from_feed_config(&cfg);
    if !reader.path().exists() {
        return Err(format!(
            "SCID file not found: {}",
            reader.path().to_string_lossy()
        ));
    }

    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let task = reader.spawn_tail_loop(state.dtc_tx.clone(), stop_rx, cfg.flush_poll_ms);
    *state.scid_shutdown_tx.lock().await = Some(stop_tx);
    *state.scid_feed_task.lock().await = Some(task);
    Ok(())
}

/// Stop the active SCID tail feed, if running.
#[tauri::command]
pub async fn stop_scid_feed(state: State<'_, AppState>) -> Result<(), String> {
    if let Some(stop_tx) = state.scid_shutdown_tx.lock().await.take() {
        let _ = stop_tx.send(true);
    }
    if let Some(task) = state.scid_feed_task.lock().await.take() {
        task.abort();
    }
    Ok(())
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

// ---------------------------------------------------------------------------
// Setup management commands (B1)
// ---------------------------------------------------------------------------

/// Update an existing setup definition.
#[tauri::command]
pub async fn update_setup(
    state: State<'_, AppState>,
    setup: SetupDefinition,
) -> Result<SetupDefinition, String> {
    let db = state.db.lock().await;
    db.upsert_setup(&setup).map_err(|e| e.to_string())?;
    Ok(setup)
}

/// Delete a setup by ID.
#[tauri::command]
pub async fn delete_setup(state: State<'_, AppState>, id: String) -> Result<(), String> {
    let db = state.db.lock().await;
    db.delete_setup(&id).map_err(|e| e.to_string())
}

/// Duplicate a setup with a new UUID and "(Copy)" suffix.
#[tauri::command]
pub async fn duplicate_setup(
    state: State<'_, AppState>,
    id: String,
) -> Result<SetupDefinition, String> {
    let db = state.db.lock().await;
    let original = db
        .get_setup(&id)
        .map_err(|e| e.to_string())?
        .ok_or("Setup not found")?;
    let mut copy = original;
    copy.id = uuid::Uuid::new_v4().to_string();
    copy.name = format!("{} (Copy)", copy.name);
    db.upsert_setup(&copy).map_err(|e| e.to_string())?;
    Ok(copy)
}

/// Toggle a setup active/inactive.
#[tauri::command]
pub async fn toggle_setup(
    state: State<'_, AppState>,
    id: String,
    active: bool,
) -> Result<(), String> {
    let db = state.db.lock().await;
    db.toggle_setup(&id, active).map_err(|e| e.to_string())
}

/// Return the list of built-in setup templates.
#[tauri::command]
pub async fn list_templates() -> Result<Vec<SetupDefinition>, String> {
    Ok(templates::builtin_templates())
}

// ---------------------------------------------------------------------------
// Trade commands (B2)
// ---------------------------------------------------------------------------

/// Create a trade record from the "Took it" flow.
#[tauri::command]
pub async fn create_trade(
    state: State<'_, AppState>,
    trade: TradeRecord,
) -> Result<TradeRecord, String> {
    let db = state.db.lock().await;
    db.insert_trade(&trade).map_err(|e| e.to_string())?;
    Ok(trade)
}

/// Close an open trade with exit details.
#[tauri::command]
pub async fn close_trade(
    state: State<'_, AppState>,
    id: String,
    exit_price: f64,
    result_r: f64,
) -> Result<(), String> {
    let exit_time = chrono::Utc::now().timestamp_millis() as f64;
    let db = state.db.lock().await;
    db.update_trade_exit(&id, exit_time, exit_price, result_r)
        .map_err(|e| e.to_string())?;
    drop(db);

    let mut risk = state.risk.lock().await;
    risk.record_trade_result(result_r);
    let snapshot = risk.state();
    drop(risk);

    let db = state.db.lock().await;
    db.save_risk_state(&snapshot).map_err(|e| e.to_string())?;
    Ok(())
}

/// List trades for a session.
#[tauri::command]
pub async fn list_trades(
    state: State<'_, AppState>,
    session_id: String,
) -> Result<Vec<TradeRecord>, String> {
    let db = state.db.lock().await;
    db.list_trades_for_session(&session_id)
        .map_err(|e| e.to_string())
}

/// Get the currently open trade for a session (if any).
#[tauri::command]
pub async fn get_open_trade(
    state: State<'_, AppState>,
    session_id: String,
) -> Result<Option<TradeRecord>, String> {
    let db = state.db.lock().await;
    db.get_open_trade(&session_id).map_err(|e| e.to_string())
}

/// Update trade review fields (planned, rules_followed, emotional_state, notes).
#[tauri::command]
pub async fn review_trade(
    state: State<'_, AppState>,
    id: String,
    planned: bool,
    rules_followed: Option<bool>,
    emotional_state: Option<String>,
    notes: String,
) -> Result<(), String> {
    let db = state.db.lock().await;
    db.update_trade_review(
        &id,
        planned,
        rules_followed,
        emotional_state.as_deref(),
        &notes,
    )
    .map_err(|e| e.to_string())
}

// ---------------------------------------------------------------------------
// Session management commands (extended)
// ---------------------------------------------------------------------------

/// List saved sessions.
#[tauri::command]
pub async fn list_sessions(
    state: State<'_, AppState>,
    limit: Option<usize>,
) -> Result<Vec<SessionRecord>, String> {
    let db = state.db.lock().await;
    db.list_sessions(limit.unwrap_or(50))
        .map_err(|e| e.to_string())
}

// ---------------------------------------------------------------------------
// Journal commands
// ---------------------------------------------------------------------------

/// Save a journal entry.
#[tauri::command]
pub async fn save_journal_entry(
    state: State<'_, AppState>,
    entry: JournalEntry,
) -> Result<(), String> {
    let db = state.db.lock().await;
    db.insert_journal_entry(&entry).map_err(|e| e.to_string())
}

/// Get journal entries for a session.
#[tauri::command]
pub async fn get_journal(
    state: State<'_, AppState>,
    session_id: String,
) -> Result<Vec<JournalEntry>, String> {
    let db = state.db.lock().await;
    db.get_journal_for_session(&session_id)
        .map_err(|e| e.to_string())
}

// ---------------------------------------------------------------------------
// Risk config commands
// ---------------------------------------------------------------------------

/// Get the trader's risk configuration.
#[tauri::command]
pub async fn get_risk_config(state: State<'_, AppState>) -> Result<RiskConfigRecord, String> {
    let db = state.db.lock().await;
    db.load_risk_config().map_err(|e| e.to_string())
}

/// Save updated risk configuration.
#[tauri::command]
pub async fn save_risk_config(
    state: State<'_, AppState>,
    config: RiskConfigRecord,
) -> Result<(), String> {
    let db = state.db.lock().await;
    db.save_risk_config(&config).map_err(|e| e.to_string())
}

/// Initialize or reset risk state for a new session. Creates the initial risk state row
/// (0 P&L, 0 trades, no streaks) so get_risk_state returns valid data. Call at session start.
#[tauri::command]
pub async fn init_risk_state(state: State<'_, AppState>) -> Result<RiskState, String> {
    let db = state.db.lock().await;
    let config = db.load_risk_config().map_err(|e| e.to_string())?;
    let risk_state = RiskState {
        daily_pnl_r: 0.0,
        trade_count: 0,
        consecutive_losses: 0,
        consecutive_wins: 0,
        drawdown_r: 0.0,
        max_daily_loss_r: config.max_daily_loss_r,
        at_limit: false,
    };
    db.save_risk_state(&risk_state).map_err(|e| e.to_string())?;
    Ok(risk_state)
}

// ---------------------------------------------------------------------------
// Account state commands (risk coach: balance, positions, Lucid params)
// ---------------------------------------------------------------------------

/// Get account state for risk coach: last balance, open positions, Lucid params.
#[tauri::command]
pub async fn get_account_state(
    state: State<'_, AppState>,
) -> Result<Option<AccountStateRecord>, String> {
    let db = state.db.lock().await;
    db.load_account_state().map_err(|e| e.to_string())
}

/// Save account state. Partial updates: only provided fields are updated.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveAccountStateInput {
    pub last_balance_dollars: Option<f64>,
    pub open_positions: Option<Vec<OpenPositionRecord>>,
    pub lucid_daily_loss_dollars: Option<f64>,
    pub lucid_account_size_dollars: Option<f64>,
    pub profit_target_per_cycle: Option<f64>,
    pub position_sizing_method: Option<String>,
    pub kelly_fraction: Option<f64>,
}

#[tauri::command]
pub async fn save_account_state(
    state: State<'_, AppState>,
    input: SaveAccountStateInput,
) -> Result<AccountStateRecord, String> {
    let db = state.db.lock().await;
    let existing = db.load_account_state().map_err(|e| e.to_string())?;
    let base = existing.unwrap_or(AccountStateRecord {
        last_balance_dollars: 0.0,
        last_balance_updated_at_ms: 0,
        open_positions: Vec::new(),
        lucid_daily_loss_dollars: None,
        lucid_account_size_dollars: None,
        profit_target_per_cycle: None,
        position_sizing_method: "quarter_kelly".to_string(),
        kelly_fraction: 0.25,
    });
    let now_ms = chrono::Utc::now().timestamp_millis();
    let has_updates = input.last_balance_dollars.is_some() || input.open_positions.is_some();
    let state_rec = AccountStateRecord {
        last_balance_dollars: input
            .last_balance_dollars
            .unwrap_or(base.last_balance_dollars),
        last_balance_updated_at_ms: if has_updates {
            now_ms
        } else {
            base.last_balance_updated_at_ms
        },
        open_positions: input.open_positions.unwrap_or(base.open_positions),
        lucid_daily_loss_dollars: input
            .lucid_daily_loss_dollars
            .or(base.lucid_daily_loss_dollars),
        lucid_account_size_dollars: input
            .lucid_account_size_dollars
            .or(base.lucid_account_size_dollars),
        profit_target_per_cycle: input
            .profit_target_per_cycle
            .or(base.profit_target_per_cycle),
        position_sizing_method: input
            .position_sizing_method
            .unwrap_or_else(|| base.position_sizing_method.clone()),
        kelly_fraction: input.kelly_fraction.unwrap_or(base.kelly_fraction),
    };
    db.save_account_state(&state_rec)
        .map_err(|e| e.to_string())?;
    Ok(state_rec)
}

// ---------------------------------------------------------------------------
// Recording management
// ---------------------------------------------------------------------------

/// List saved session recordings.
#[tauri::command]
pub async fn list_recordings() -> Result<Vec<the_desk_backend::recording::RecordingInfo>, String> {
    let dir = super::data_dir();
    the_desk_backend::recording::list_recordings(&dir.to_string_lossy()).map_err(|e| e.to_string())
}

// ---------------------------------------------------------------------------
// Replay helpers
// ---------------------------------------------------------------------------

fn replay_entry_to_feed_event(
    entry: &the_desk_backend::recording::RecordingEntry,
) -> Option<FeedEvent> {
    match entry.record_type.as_str() {
        "trade" => {
            let price = entry.payload.get("price")?.as_f64()?;
            let volume = entry.payload.get("volume")?.as_f64()?;
            let side = match entry.payload.get("side").and_then(|s| s.as_str()) {
                Some("buy") => TradeSide::Buy,
                Some("sell") => TradeSide::Sell,
                _ => TradeSide::Unknown,
            };
            Some(FeedEvent::Trade {
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
            Some(FeedEvent::Quote {
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
