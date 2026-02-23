# Tauri Bridge Anti-Patterns

Use these before/after examples to spot and correct common IPC mistakes in The Desk.

## 1) Direct invoke in a component

Bad:

```tsx
import { invoke } from "@tauri-apps/api/core";

export function SettingsButton() {
  return (
    <button onClick={() => invoke("update_risk_config", { config: {} })}>
      Save
    </button>
  );
}
```

Good:

```ts
// src/lib/tauri-bridge.ts
import { invoke } from "@tauri-apps/api/core";
import type { RiskConfig } from "./types";

export const risk = {
  updateConfig: (config: RiskConfig) =>
    invoke<void>("update_risk_config", { config }),
};
```

```tsx
// component
import { risk } from "../lib/tauri-bridge";

export function SettingsButton({ config }: { config: RiskConfig }) {
  return <button onClick={() => risk.updateConfig(config)}>Save</button>;
}
```

Why:
- Keeps IPC boundaries centralized and testable.

## 2) Mismatched parameter names between TS and Rust

Bad:

```rust
#[tauri::command]
pub async fn update_setup(setup: Setup) -> Result<(), String> {
    Ok(())
}
```

```ts
invoke<void>("update_setup", { payload: setup }); // wrong key
```

Good:

```ts
invoke<void>("update_setup", { setup }); // key matches Rust arg name
```

Why:
- Tauri maps command parameters by name, not by position.

## 3) Non-serialized field naming mismatch

Bad:

```rust
#[derive(serde::Serialize, Clone)]
pub struct MarketState {
    pub last_price: f64,
}
```

```ts
interface MarketState {
  lastPrice: number;
}
```

Good:

```rust
#[derive(serde::Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MarketState {
    pub last_price: f64,
}
```

Why:
- Ensures Rust payloads align with TypeScript camelCase interfaces.

## 4) Event flooding on every tick

Bad:

```rust
// called for each incoming trade
app.emit("market-state", current_state).ok();
```

Good:

```rust
use std::time::{Duration, Instant};

let mut last_emit = Instant::now();
let emit_interval = Duration::from_millis(250);

if last_emit.elapsed() >= emit_interval {
    app.emit("market-state", current_state).ok();
    last_emit = Instant::now();
}
```

Why:
- Protects React from rendering overload.

## 5) Missing listener cleanup in hooks

Bad:

```ts
useEffect(() => {
  listen("market-state", (event) => setState(event.payload));
}, []);
```

Good:

```ts
useEffect(() => {
  const unlisten = listen("market-state", (event) => setState(event.payload));
  return () => {
    unlisten.then((fn) => fn());
  };
}, []);
```

Why:
- Avoids duplicate handlers and memory leaks across remounts.

## 6) Business logic in the frontend

Bad:

```ts
// component-level market logic
const shouldAlert = delta > threshold && valueAreaBreak && riskOk;
```

Good:

```ts
// frontend consumes deterministic alert state
const shouldAlert = alert?.kind === "rule-match";
```

Why:
- Rule evaluation belongs in Rust rules engine, not UI components.

