# Tauri Bridge Examples

Use these as default patterns when implementing IPC in The Desk.

## Example 1: Rust command + TypeScript bridge wrapper

Rust command:

```rust
use tauri::State;
use crate::db::Database;
use crate::models::Setup;

#[tauri::command]
pub async fn get_setups(db: State<'_, Database>) -> Result<Vec<Setup>, String> {
    db.get_all_setups().map_err(|e| e.to_string())
}
```

TypeScript wrapper (`src/lib/tauri-bridge.ts`):

```ts
import { invoke } from "@tauri-apps/api/core";
import type { Setup } from "./types";

export const playbook = {
  getSetups: () => invoke<Setup[]>("get_setups"),
};
```

Why this is correct:
- `invoke()` is centralized in `tauri-bridge.ts` (not in components)
- Rust boundary returns `Result<T, String>`
- Type stays explicit end-to-end

## Example 2: Rust command with parameters

Rust command:

```rust
#[tauri::command]
pub async fn update_risk_config(
    db: State<'_, Database>,
    config: RiskConfig,
) -> Result<(), String> {
    db.update_risk_config(&config).map_err(|e| e.to_string())
}
```

TypeScript wrapper:

```ts
import { invoke } from "@tauri-apps/api/core";
import type { RiskConfig } from "./types";

export const risk = {
  updateConfig: (config: RiskConfig) =>
    invoke<void>("update_risk_config", { config }),
};
```

Why this is correct:
- Parameter object key `config` matches Rust arg name `config`
- Wrapper remains thin and typed

## Example 3: Rust event payload + emit

Rust payload:

```rust
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketState {
    pub last_price: f64,
    pub vwap: f64,
    pub value_area_high: f64,
    pub value_area_low: f64,
}
```

Rust emit:

```rust
use tauri::{AppHandle, Emitter};

pub fn emit_market_state(app: &AppHandle, state: &MarketState) {
    app.emit("market-state", state).ok();
}
```

Why this is correct:
- Payload is serializable and cloneable
- Camel case is guaranteed for TypeScript interoperability
- Emit failure is non-fatal

## Example 4: React hook listener with cleanup

```ts
import { listen } from "@tauri-apps/api/event";
import { useEffect, useState } from "react";
import type { MarketState } from "../lib/types";

export function useMarketState() {
  const [state, setState] = useState<MarketState | null>(null);

  useEffect(() => {
    const unlisten = listen<MarketState>("market-state", (event) => {
      setState(event.payload);
    });

    return () => {
      unlisten.then((fn) => fn());
    };
  }, []);

  return state;
}
```

Why this is correct:
- Listener is located in `src/hooks/`
- Cleanup avoids duplicate listeners and leaks

## Example 5: Throttled event emission in processing loop

```rust
use std::time::{Duration, Instant};

let mut last_emit = Instant::now();
let emit_interval = Duration::from_millis(250); // 4 Hz

// inside processing loop:
if last_emit.elapsed() >= emit_interval {
    let state = pipelines.to_market_state();
    app.emit("market-state", state).ok();
    last_emit = Instant::now();
}
```

Why this is correct:
- UI receives snapshots at a controlled cadence
- Prevents rendering overload from high-frequency market updates

## Example 6: Component consumption (no invoke)

```tsx
import { useMarketState } from "../hooks/use-market-state";

export function MarketHeader() {
  const state = useMarketState();
  if (!state) return null;

  return (
    <div>
      <span>Last: {state.lastPrice.toFixed(2)}</span>
      <span>VWAP: {state.vwap.toFixed(2)}</span>
    </div>
  );
}
```

Why this is correct:
- Component consumes hook state only
- No direct `invoke()` or bridge logic in the UI layer

