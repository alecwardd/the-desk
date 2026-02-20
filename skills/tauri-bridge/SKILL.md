---
name: TauriBridge
description: Tauri 2.x IPC patterns between Rust backend and React frontend. USE WHEN implementing commands, event streaming, state management across the Rust/TypeScript boundary, or troubleshooting Tauri communication.
---

# Tauri Bridge Patterns

Reference for implementing the Rust ↔ React communication layer in The Desk.

---

## Architecture Overview

```
┌──────────────────────────┐     ┌──────────────────────────┐
│  Rust Backend (src-tauri) │     │  React Frontend (src/)   │
│                          │     │                          │
│  DTC Client              │     │  Dashboard               │
│  Market Pipelines        │◄═══►│  Coaching Panel           │
│  Rules Engine            │ IPC │  Playbook Builder         │
│  Risk Tracker            │     │  Replay Controls          │
│  Session Recorder        │     │  Settings                 │
│  SQLite                  │     │                          │
└──────────────────────────┘     └──────────────────────────┘
```

Two IPC mechanisms in Tauri 2.x:
1. **Commands** — Frontend calls Rust functions (request/response)
2. **Events** — Rust pushes data to frontend (streaming)

---

## Commands (Frontend → Rust)

Use for: user actions, configuration changes, data queries, one-off operations.

### Rust Side

```rust
// src-tauri/src/commands/playbook.rs
use tauri::State;
use crate::db::Database;
use crate::models::Setup;

#[tauri::command]
async fn get_setups(db: State<'_, Database>) -> Result<Vec<Setup>, String> {
    db.get_all_setups()
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn create_setup(db: State<'_, Database>, setup: Setup) -> Result<Setup, String> {
    db.insert_setup(&setup)
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn update_risk_config(
    db: State<'_, Database>,
    config: RiskConfig,
) -> Result<(), String> {
    db.update_risk_config(&config)
        .map_err(|e| e.to_string())
}
```

Register in `main.rs`:
```rust
fn main() {
    tauri::Builder::default()
        .manage(Database::new("~/.the-desk/data.db").unwrap())
        .manage(DtcClient::new())
        .invoke_handler(tauri::generate_handler![
            get_setups,
            create_setup,
            update_risk_config,
            connect_dtc,
            start_session,
            stop_session,
            import_trades,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
```

### TypeScript Side

```typescript
// src/lib/tauri-bridge.ts
import { invoke } from '@tauri-apps/api/core';
import type { Setup, RiskConfig, DtcStatus } from './types';

export const playbook = {
  getSetups: () => invoke<Setup[]>('get_setups'),
  createSetup: (setup: Setup) => invoke<Setup>('create_setup', { setup }),
  updateSetup: (setup: Setup) => invoke<Setup>('update_setup', { setup }),
  deleteSetup: (id: string) => invoke<void>('delete_setup', { id }),
};

export const risk = {
  getConfig: () => invoke<RiskConfig>('get_risk_config'),
  updateConfig: (config: RiskConfig) => invoke<void>('update_risk_config', { config }),
};

export const dtc = {
  connect: (host: string, port: number) => invoke<void>('connect_dtc', { host, port }),
  disconnect: () => invoke<void>('disconnect_dtc'),
  status: () => invoke<DtcStatus>('dtc_status'),
};

export const session = {
  start: () => invoke<string>('start_session'), // returns session_id
  stop: () => invoke<void>('stop_session'),
  importTrades: (csvPath: string) => invoke<void>('import_trades', { csvPath }),
};
```

### Key Rules for Commands

1. **Always return `Result<T, String>`** from Rust commands. Tauri requires this for error handling across the IPC boundary.
2. **Use `State<'_, T>`** to access managed state (database, DTC client, etc.).
3. **Keep commands thin** — they should delegate to service modules, not contain business logic.
4. **Parameter names in TypeScript must match Rust** — Tauri maps by name, not position. Use `{ setup }` not `setup` in the invoke call.
5. **Async by default** — commands that touch the database or network should be async to avoid blocking the main thread.

---

## Events (Rust → Frontend)

Use for: streaming market data, coaching prompts, risk alerts, connection status changes, anything real-time.

### Rust Side

```rust
// src-tauri/src/pipelines/mod.rs
use tauri::{AppHandle, Emitter};
use serde::Serialize;

#[derive(Serialize, Clone)]
struct MarketState {
    vwap: f64,
    vwap_upper_1sd: f64,
    vwap_lower_1sd: f64,
    va_high: f64,
    va_low: f64,
    poc: f64,
    dnva_high: f64,
    dnva_low: f64,
    dnp: f64,
    session_delta: f64,
    last_price: f64,
    bid: f64,
    ask: f64,
}

#[derive(Serialize, Clone)]
struct CoachingPrompt {
    id: String,
    setup_name: String,
    message: String,
    priority: String,     // "info", "alert", "warning", "critical"
    setup_id: Option<String>,
    timestamp: f64,
}

#[derive(Serialize, Clone)]
struct RiskState {
    daily_pnl_r: f64,
    trade_count: i32,
    consecutive_losses: i32,
    max_daily_loss_r: f64,
    at_limit: bool,
}

// In your pipeline processing loop:
fn emit_market_state(app: &AppHandle, state: &MarketState) {
    app.emit("market-state", state).ok();
}

fn emit_coaching_prompt(app: &AppHandle, prompt: &CoachingPrompt) {
    app.emit("coaching-prompt", prompt).ok();
}

fn emit_risk_state(app: &AppHandle, state: &RiskState) {
    app.emit("risk-state", state).ok();
}

fn emit_connection_status(app: &AppHandle, connected: bool) {
    app.emit("dtc-status", connected).ok();
}
```

### TypeScript Side

```typescript
// src/hooks/useMarketState.ts
import { listen } from '@tauri-apps/api/event';
import { useState, useEffect } from 'react';
import type { MarketState } from '../lib/types';

export function useMarketState() {
  const [state, setState] = useState<MarketState | null>(null);

  useEffect(() => {
    const unlisten = listen<MarketState>('market-state', (event) => {
      setState(event.payload);
    });

    return () => { unlisten.then(fn => fn()); };
  }, []);

  return state;
}

// src/hooks/useCoachingPrompts.ts
import { listen } from '@tauri-apps/api/event';
import { useState, useEffect, useRef } from 'react';
import type { CoachingPrompt } from '../lib/types';

export function useCoachingPrompts() {
  const [prompts, setPrompts] = useState<CoachingPrompt[]>([]);
  const promptsRef = useRef(prompts);
  promptsRef.current = prompts;

  useEffect(() => {
    const unlisten = listen<CoachingPrompt>('coaching-prompt', (event) => {
      setPrompts(prev => [...prev, event.payload]);
    });

    return () => { unlisten.then(fn => fn()); };
  }, []);

  return prompts;
}

// src/hooks/useRiskState.ts
export function useRiskState() {
  const [risk, setRisk] = useState<RiskState | null>(null);

  useEffect(() => {
    const unlisten = listen<RiskState>('risk-state', (event) => {
      setRisk(event.payload);
    });

    return () => { unlisten.then(fn => fn()); };
  }, []);

  return risk;
}
```

### Key Rules for Events

1. **Emit structured data, not raw market ticks.** The frontend should never see individual trades. Emit aggregated state (MarketState) at a controlled rate (e.g., every 250ms or on significant change).
2. **Use `.ok()` on emit calls.** Events may fail if no listener is attached — that's fine, don't panic.
3. **Throttle high-frequency events.** Market data arrives at 100+ messages/second. Throttle UI updates to ~4/second (every 250ms) to prevent React from choking.
4. **Events are fire-and-forget.** No acknowledgment from frontend. If you need a response, use a command.
5. **Clean up listeners.** Always return the unlisten function from useEffect to prevent memory leaks.

---

## Streaming Data Pattern (The Desk's Primary Pattern)

The most important data flow in The Desk:

```
DTC Trades ──> Pipelines ──> Aggregated State ──> Throttled Emit ──> React UI
                   │
                   ├──> Rules Engine ──> Alert ──> LLM ──> Coaching Emit ──> React UI
                   │
                   └──> Risk Tracker ──> Risk State Emit ──> React UI
```

### Rust Implementation

```rust
// Main processing loop (runs in a background tokio task)
async fn processing_loop(
    app: AppHandle,
    mut dtc_rx: broadcast::Receiver<DtcEvent>,
    pipelines: Arc<Mutex<Pipelines>>,
    rules_engine: Arc<RulesEngine>,
    risk_tracker: Arc<Mutex<RiskTracker>>,
) {
    let mut last_emit = Instant::now();
    let emit_interval = Duration::from_millis(250); // 4 Hz UI updates

    loop {
        match dtc_rx.recv().await {
            Ok(DtcEvent::Trade { price, volume, side, timestamp, .. }) => {
                // Update all pipelines (fast, deterministic)
                let mut pipes = pipelines.lock().await;
                pipes.vwap.add_trade(price, volume);
                pipes.tpo.add_trade(price, timestamp);
                pipes.delta.add_trade(price, volume, side);
                pipes.levels.check_price(price);

                // Check rules (fast, deterministic)
                let signals = pipes.to_signals();
                if let Some(alert) = rules_engine.evaluate(&signals) {
                    // Rules matched — send to LLM for coaching prompt (async, ~1-2s)
                    let app_clone = app.clone();
                    tokio::spawn(async move {
                        let prompt = generate_coaching_prompt(&alert).await;
                        emit_coaching_prompt(&app_clone, &prompt);
                    });
                }

                // Throttled UI update
                if last_emit.elapsed() >= emit_interval {
                    let state = pipes.to_market_state();
                    emit_market_state(&app, &state);
                    last_emit = Instant::now();
                }
            }
            Ok(DtcEvent::Disconnected) => {
                emit_connection_status(&app, false);
            }
            _ => {}
        }
    }
}
```

---

## State Management Pattern

### Managed State in Rust

```rust
// Shared state managed by Tauri
pub struct AppState {
    pub db: Database,
    pub dtc_client: Mutex<DtcClient>,
    pub pipelines: Mutex<Pipelines>,
    pub rules_engine: RulesEngine,  // Read-only after setup (playbook changes require rebuild)
    pub risk_tracker: Mutex<RiskTracker>,
    pub session_recorder: Mutex<SessionRecorder>,
}
```

### React State Architecture

```
AppState (React Context)
├── marketState (from 'market-state' events)
├── riskState (from 'risk-state' events)
├── coachingPrompts (from 'coaching-prompt' events, append-only log)
├── connectionStatus (from 'dtc-status' events)
├── activeSetups (loaded via command at startup, updated on playbook changes)
└── sessionInfo (current session metadata)
```

Use React Context for global state (market, risk, connection) and local state for component-specific UI.

---

## Common Pitfalls

1. **Serialization mismatch.** Rust structs must derive `Serialize` exactly matching the TypeScript types. Use `#[serde(rename_all = "camelCase")]` to match JS naming conventions.

2. **Blocking the main thread.** Long-running Rust operations (DTC connection, LLM API calls) must run in background tokio tasks, never in a command handler.

3. **Event flooding.** Emitting on every trade tick will freeze the React UI. Always throttle to ≤4 Hz for UI updates.

4. **State mutation from commands during streaming.** If a command modifies state that the processing loop reads, use proper synchronization (Mutex/RwLock). Prefer RwLock when reads are frequent and writes are rare (pipeline state).

5. **Missing `Clone` derive on event payloads.** Tauri events require `Clone + Serialize`. Forget `Clone` and you get cryptic compile errors.

6. **String errors.** Tauri commands must return `Result<T, String>`. Create a proper error type with `impl From<YourError> for String` or use `.map_err(|e| e.to_string())`.

---

## File Organization

```
src/
├── lib/
│   ├── tauri-bridge.ts      // All invoke() calls organized by domain
│   ├── types.ts              // Shared TypeScript types matching Rust structs
│   └── events.ts             // Event name constants and listener setup
├── hooks/
│   ├── useMarketState.ts     // market-state event listener
│   ├── useCoachingPrompts.ts // coaching-prompt event listener
│   ├── useRiskState.ts       // risk-state event listener
│   └── useConnection.ts      // dtc-status event listener
├── context/
│   └── AppContext.tsx         // Combines all hooks into global context
└── components/
    └── ...                   // UI components consume context
```
