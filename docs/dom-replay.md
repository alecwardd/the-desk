# DOM Replay Visualizer

Historical DOM replay lets you inspect NQ order book and tape behavior inside the Tauri desktop app without returning to Sierra Chart. It reconstructs the ladder from historical depth plus tick data and replays the auction at original event timing, with seek and speed controls.

## For Users

### What it shows

- **Center ladder:** resting bid and ask size around the inside market
- **Left profile:** session-to-cursor volume profile, not clip-only volume
- **Right tape:** recent time and sales with side coloring and larger prints emphasized
- **Replay transport:** load, play, pause, stop, speed presets, and a time scrubber

### How to open it

1. Launch the Tauri app.
2. Use the footer navigation and select `DOM`.
3. Set a `Start` and `End` time for the clip you want to review.
4. Click `Load Clip`.
5. Use `Play`, `Pause`, `Stop`, the speed buttons, or the scrubber to navigate.

### Recommended workflow

- Use **30-minute windows** for the best balance of fidelity and responsiveness.
- Start with **1x** if you want the exact market tempo, then switch to `0.5x` or `0.25x` when reviewing detail.
- Use the scrubber to jump near the setup, then replay forward from there.
- Watch the **warning/source line** above the panes. It tells you whether replay is coming from SQLite history, Sierra files, or a hybrid fallback.

### Data requirements

The visualizer needs both:

- **Trade history**: from `raw_ticks` in SQLite or `.scid` fallback
- **Depth history**: from `depth_events` in SQLite or `.depth` fallback

If depth is missing for a requested range, the app does **not** invent a fake DOM. It will show an error or a fallback warning instead.

### What the colors mean

- **Blue bars:** bid liquidity
- **Red bars:** ask liquidity
- **Blue row highlight:** current last-trade price row
- **Green text:** buy-side prints / supportive activity
- **Red text:** sell-side prints / aggressive selling
- **Warm highlight:** pulling / liquidity removal

### Current limitations

- V1 is scoped to **NQ** data already configured in this repo.
- Fidelity is limited by **historical Sierra depth granularity**.
- The replay tool is for **visual analysis only**. It does not include simulated order-entry controls.

## For Agents

### Read this before modifying DOM replay

The DOM replay system is separate from the older `.desk` session replay path.

- **Old replay path:** session recording replay in `src-tauri/src/recording/mod.rs`
- **DOM replay path:** historical depth/tick replay in `src-tauri/src/dom_replay.rs`

Do not extend the older recording replay when the task is about historical ladder playback.

### Backend architecture

- `src-tauri/src/dom_replay.rs`
  - builds a preloaded `DomReplayClip`
  - merges trade and depth-batch events chronologically
  - seeds the ladder and the session-to-cursor profile
  - reconstructs replay state from checkpoints for seek support
- `src-tauri/src/commands.rs`
  - exposes `dom_replay_load`
  - exposes `dom_replay_start`, `dom_replay_pause`, `dom_replay_stop`
  - exposes `dom_replay_seek` using timestamp-based seeking
  - exposes `dom_replay_status`
  - emits `dom-replay-frame`
- `src-tauri/src/main.rs`
  - owns `DomReplayRuntime` in `AppState`
- `src-tauri/src/db/mod.rs`
  - provides replay-specific range helpers for `raw_ticks` and `depth_events`
- `src-tauri/src/depth/mod.rs`
  - exposes full-book reconstruction helpers used to seed replay

### Frontend architecture

- `src/hooks/use-dom-replay.ts` subscribes to `dom-replay-frame` and wraps bridge actions
- `src/components/dom/dom-visualizer.tsx` is the top-level replay workspace
- `src/components/dom/dom-ladder.tsx`, `volume-profile.tsx`, and `tape-strip.tsx` are presentational panes
- `src/lib/tauri-bridge.ts` contains the DOM replay invoke bridge and event constant
- `src/lib/types.ts` defines `DomReplayFrame`, `DomReplayStatus`, `DomReplayLoadResult`, and related UI payloads

### Source-of-truth behavior

- **Preferred source order:** SQLite first, Sierra file fallback second
- **Profile scope:** full session start through the current replay cursor
- **Playback timing:** exact event-time gaps scaled by the requested speed
- **Seek semantics:** seek by timestamp, not by array index in the UI API
- **Depth fidelity:** replay is based on real historical depth changes or file-backed reconstruction; never approximate a DOM from prints alone

### Guardrails for future changes

- Keep DOM replay state separate from live feed state and `.desk` recording replay state.
- Prefer extending `DomReplayFrame` over introducing one-off frontend side channels.
- If you add new panes or overlays, derive them from the same replay frame so scrubbing remains deterministic.
- If you change replay sourcing, preserve the current contract:
  - SQLite first
  - explicit fallback messaging
  - no fake DOM when depth is unavailable

### Validation

Before concluding DOM replay work, run:

```bash
cd src-tauri && cargo check
cd src-tauri && cargo test dom_replay -- --nocapture
cd .. && npm run lint
cd .. && npm test
```
