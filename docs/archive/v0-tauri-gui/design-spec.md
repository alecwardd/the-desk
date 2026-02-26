# The Desk — UI/UX Design Specification

**Version:** 1.0
**Date:** 2026-02-25
**Status:** Draft

**Companion specs:**
- [core-flows.md](core-flows.md) — User flows with wireframes and interaction details
- [phase-1-prd.md](phase-1-prd.md) — Feature requirements (UI-01 through UI-20)

---

## 1. Design Principles

| Principle | Implication |
|-----------|-------------|
| **Keyboard-first** | Every action achievable without a mouse during a live session. Mouse is for configuration and review, not real-time trading. |
| **Information density over decoration** | Traders want data, not whitespace. Default to compact; let users configure comfortable mode. |
| **Coaching, not distraction** | Prompts appear prominently when they need attention, then recede into the log. Never compete with Sierra Chart for visual dominance. |
| **Always-on context** | Market state, risk, and connection status are always visible. No navigation required to check critical state. |
| **Dark by default** | Trading environments are dark. No light mode planned for Phase 1. |

---

## 2. Layout Regions

The Desk occupies a dedicated monitor (minimum 1920x1080). The layout is a fixed three-region design.

```
+------------------------------------------------------------------+
|  RISK BAR (always visible, full width, ~48px height)             |
|  Daily P&L | Trades | Consec. Losses | Max Loss | Clock | Status |
+----------------------------------------------+-------------------+
|                                              |                   |
|  COACHING FEED (scrollable, primary focus)   |  SIDEBAR (fixed)  |
|                                              |                   |
|  - Active prompt cards (top)                 |  Market State     |
|  - Feed history (chronological, scrolls)     |  - Last, VWAP     |
|  - Risk warnings (inline, distinct style)    |  - VA/POC         |
|  - Trader notes (inline)                     |  - DNVA/DNP       |
|  - System events (session start, etc.)       |  - Delta           |
|                                              |                   |
|                                              |  Key Levels       |
|                                              |  - PDH/PDL/PDC    |
|                                              |  - ON H/L         |
|                                              |  - OR/IB          |
|                                              |                   |
|                                              |  Active Setups    |
|                                              |  - State badges    |
|                                              |                   |
+----------------------------------------------+-------------------+
|  NOTE BAR (always visible, full width, ~40px height)             |
|  [N] Quick note input...              | DTC Status | Last Price  |
+------------------------------------------------------------------+
```

### 2.1 Region Sizing

| Region | Default Width/Height | Behavior |
|--------|---------------------|----------|
| Risk Bar | Full width, 48px fixed | Never collapses. Content wraps if window narrow. |
| Coaching Feed | Flex (fills remaining width) | Scrollable. Auto-scrolls to newest unless user scrolled up. |
| Sidebar | 220px fixed | Collapsible via `S` key. Content stacks vertically, scrolls independently if overflows. |
| Note Bar | Full width, 40px fixed | Never collapses. Input field gains focus on `N` key. |

### 2.2 Responsive Behavior

| Width | Adjustment |
|-------|-----------|
| >= 1920px | Full layout as designed |
| 1600-1919px | Sidebar narrows to 180px. Font size decreases one step. |
| 1280-1599px | Sidebar collapses by default (toggle with `S`). Feed uses full width. |
| < 1280px | Warning banner: "The Desk is designed for 1920x1080 minimum." Layout still functional but cramped. |

---

## 3. Color System

Built on shadcn/ui with Tailwind CSS. All colors use CSS custom properties for consistency.

### 3.1 Base Palette

| Token | Value | Usage |
|-------|-------|-------|
| `--background` | `#0f0f0f` | App background |
| `--surface` | `#1a1a1a` | Cards, panels, input backgrounds |
| `--surface-raised` | `#222222` | Elevated elements (modals, dropdowns) |
| `--border` | `#333333` | Default borders |
| `--border-subtle` | `#1e1e1e` | Section dividers, feed item separators |
| `--text-primary` | `#e0e0e0` | Primary content text |
| `--text-secondary` | `#888888` | Labels, metadata, timestamps |
| `--text-muted` | `#555555` | Hints, placeholders, disabled text |
| `--text-faint` | `#444444` | De-emphasized system events |

### 3.2 Semantic Colors

| Token | Value | Usage |
|-------|-------|-------|
| `--accent` | `#2196f3` | Primary actions, links, active states |
| `--positive` | `#4caf50` | Positive P&L, connected status, "Took it" action |
| `--warning` | `#ff9800` | Risk warnings, approaching limits, journal notes accent |
| `--critical` | `#f44336` | Breached limits, errors, disconnected status |
| `--info` | `#64b5f6` | "Watching" state, informational badges |

### 3.3 Prompt Priority Colors

| Priority | Left Border | Background | Usage |
|----------|-------------|------------|-------|
| `alert` (setup triggered) | `--positive` | `#1e2a1e` | Active coaching prompt cards |
| `warning` (risk) | `--warning` | `#2a1a00` | Risk warning cards |
| `critical` (limit breached) | `--critical` | `#2a1010` | Urgent risk violations |
| `info` (watching) | `--info` | `#1a2a3a` | Setup approaching, system events |

---

## 4. Typography

| Element | Font | Size | Weight | Color |
|---------|------|------|--------|-------|
| Coaching prompt text | System monospace | 13px | Normal | `--text-primary` |
| Prompt card header | System monospace | 10px | Normal (uppercase, letter-spacing 0.5px) | `--text-secondary` |
| Sidebar values | System monospace | 12-14px | Bold | `--text-primary` |
| Sidebar labels | System monospace | 10-12px | Normal | `--text-muted` |
| Risk bar values | System monospace | 15px | Bold | Semantic color |
| Risk bar labels | System monospace | 10px | Normal (uppercase) | `--text-muted` |
| Section headers | System monospace | 10px | Normal (uppercase, letter-spacing 0.5px) | `--text-faint` |
| Feed timestamps | System monospace | 11px | Normal | `--text-faint` |
| Quick note input | System monospace | 12px | Normal | `--text-primary` |

**Why monospace:** Traders read numbers. Monospace keeps columns aligned, prices readable, and the interface consistent with the Sierra Chart environment.

### 4.1 Information Density Settings

| Setting | Font Scale | Spacing | Line Height |
|---------|-----------|---------|-------------|
| Compact | 0.9x | Tight (4-6px gaps) | 1.3 |
| Normal (default) | 1.0x | Standard (8-10px gaps) | 1.5 |
| Comfortable | 1.1x | Relaxed (12-16px gaps) | 1.6 |

Controlled via Settings > Display > Information Density.

---

## 5. Component Specifications

### 5.1 Coaching Prompt Card

The most important UI element. Appears at the top of the coaching feed when a setup triggers.

```
+---+-----------------------------------------------------------+
| B |  SETUP NAME · Setup Alert                          10:14  |
| O |                                                           |
| R |  Coaching text from Claude API or raw alert. 2-4          |
| D |  sentences with specific prices and rules.               |
| E |                                                           |
| R |  WR: 64%  Avg R: 1.8R  Samples: 47  Stop: below 21,428  |
|   |                                                           |
|   |  [1 Took it]  [2 Watching]  [3 Passed]                   |
+---+-----------------------------------------------------------+
```

| Element | Spec |
|---------|------|
| Left border | 3px, color by priority (see 3.3) |
| Background | Tinted by priority (see 3.3) |
| Setup name | `--accent` color, bold |
| Prompt text | `--text-primary`, 13px, line-height 1.5 |
| Metrics row | `--text-secondary`, 11px, inline with separator dots |
| Action buttons | Styled per action (see below) |
| Timestamp | `--text-faint`, 11px, top-right |

**Action button styles:**

| Button | Background | Border | Text |
|--------|-----------|--------|------|
| Took it | `#1b3a1b` | `--positive` | `--positive` |
| Watching | `#1a2a3a` | `--accent` | `--accent` |
| Passed | `#2a2a2a` | `--border` | `--text-muted` |

**Prompt card states:**
- **Active** — prominently displayed at top of feed, action buttons visible
- **Responded** — collapsed to single line in feed log, shows which action was taken and timestamp
- **Expired** — conditions no longer met, dimmed, action buttons disabled

### 5.2 Risk Bar

Always visible at the top of the window.

| Metric | Normal State | Warning State | Critical State |
|--------|-------------|---------------|----------------|
| Daily P&L | `--positive` (green if positive) / `--text-primary` (if zero) | `--warning` (within 1R of limit) | `--critical` (at or past limit) |
| Trade count | `--text-primary` | `--warning` (within 1 of max) | `--critical` (at max) |
| Consecutive losses | `--text-primary` | `--warning` (within 1 of max) | `--critical` (at max) |
| Max loss proximity | `--text-primary` | `--warning` | `--critical` |

Metrics are separated by 1px vertical dividers (`--border-subtle`). Session time and connection status display on the right side.

### 5.3 Market State Sidebar

Organized into collapsible sections. Each section has a header (10px uppercase) and rows of label-value pairs.

**Sections (top to bottom):**

1. **Market State** — Last price, VWAP, Session Delta
2. **Value Areas** — VAH, POC, VAL, DNVA Hi, DNP, DNVA Lo
3. **Key Levels** — PDH, PDL, PDC, ON Hi, ON Lo, OR Hi, OR Lo, IB Hi, IB Lo
4. **Active Setups** — List of setup names with state badges (ALERT, WATCH, idle)

**Value formatting:**
- Prices: 2 decimal places, comma-separated thousands (e.g., 21,432.50)
- Delta: signed integer with +/- prefix, `--positive` for positive, `--critical` for negative
- State badges: small pill-shaped labels using semantic colors

### 5.4 Feed Log Items

Collapsed items in the coaching feed log:

```
09:58  [WATCH]  Single Print Retest: Price approaching zone at 21,415.
09:47  [PASSED] DNVA Reversion at 21,408. Note: "spread too wide at open"
09:32  [RISK]   No trading in first 5 minutes of RTH.
09:15           Pre-session briefing generated. 2 setups in play today.
```

| Element | Style |
|---------|-------|
| Timestamp | `--text-faint`, 11px |
| Tag badge | Pill-shaped, color by type (WATCH=info, PASSED=muted, RISK=warning, TOOK IT=positive) |
| Message text | `--text-secondary`, 12px |
| Separator | `--border-subtle`, 1px bottom border |

### 5.5 Note Bar

Fixed at the bottom of the window. The note input is always visible but de-emphasized until focused.

| Element | Normal State | Focused State |
|---------|-------------|---------------|
| Shortcut hint | `--text-faint` "Note (N):" | Hidden (replaced by input cursor) |
| Input field | `--background` bg, `--border-subtle` border | `--surface` bg, `--border` border |
| DTC status | `--positive` dot + text (connected) or `--critical` (disconnected) | Unchanged |
| Last price | `--text-secondary` | Unchanged |

---

## 6. Keyboard Shortcuts

### 6.1 Live Session Shortcuts

| Key | Action | Context |
|-----|--------|---------|
| `N` | Focus quick note input | Any time during live session |
| `Enter` | Save note / confirm dialog | When note input or dialog is focused |
| `Escape` | Dismiss note / cancel dialog | When note input or dialog is focused |
| `Ctrl+E` | End session (with confirmation) | During live session |
| `1` | Respond "Took it" | When a prompt card is the active/focused card |
| `2` | Respond "Watching" | When a prompt card is the active/focused card |
| `3` | Respond "Passed" | When a prompt card is the active/focused card |
| `S` | Toggle sidebar | Any time |
| `?` | Show keyboard shortcut reference | Any time |

### 6.2 Replay Shortcuts

| Key | Action |
|-----|--------|
| `Space` | Play / Pause |
| `Left Arrow` | Scrub backward 30 seconds |
| `Right Arrow` | Scrub forward 30 seconds |
| `[` | Decrease speed (min 1x) |
| `]` | Increase speed (max 8x) |

### 6.3 Global Shortcuts

| Key | Action |
|-----|--------|
| `Ctrl+,` | Open Settings |
| `Ctrl+P` | Open Playbook Builder |

---

## 7. Notification Behavior

### 7.1 Visual Notifications

| Event | Visual Treatment |
|-------|-----------------|
| Setup conditions_met | Prompt card appears at top of feed with `alert` priority styling. Feed auto-scrolls to show it. |
| Setup approaching | Single-line log entry with `info` tag. No scroll interruption. |
| Risk warning | Prompt card with `warning` priority styling. Feed auto-scrolls. |
| Risk limit breached | Prompt card with `critical` priority styling. Risk bar metric flashes for 3 seconds. |
| Trade logged | Brief confirmation toast (2 seconds, bottom-right). |
| DTC disconnected | Connection status in note bar changes to red. Persistent until reconnected. |
| DTC reconnected | Connection status returns to green. Brief toast: "Reconnected." |
| Claude API degraded | Subtle indicator appears next to prompt: "[raw]" label. |

### 7.2 Audio Notifications (Optional, P1)

Configurable per event type in Settings. All disabled by default.

| Event | Sound | Configurable |
|-------|-------|-------------|
| Setup triggered | Short chime | On/Off, volume |
| Risk warning | Two-tone alert | On/Off, volume |
| Risk limit breached | Urgent tone | On/Off, volume |
| DTC disconnected | Low tone | On/Off, volume |

Sounds must be non-jarring, low-frequency, and distinct from Sierra Chart's native sounds.

---

## 8. View States

### 8.1 Main Dashboard (No Active Session)

- Risk bar shows "No active session"
- Coaching feed is empty with call-to-action: "Start a session or review past sessions"
- Sidebar shows last known market state (greyed out if DTC disconnected)
- Navigation options: Start Session, Replay, Playbook, Session History, Settings

### 8.2 Live Session

- Full layout as described in Section 2
- Risk bar active with live metrics
- Coaching feed streaming
- Note bar active

### 8.3 Pre-Session Briefing

- Replaces coaching feed area with briefing content
- Sidebar still shows live market state
- Risk bar still active
- "Start Session" button at bottom

### 8.4 Post-Session Review

- Replaces coaching feed with review interface
- Trade cards with tagging inputs
- Journal text area
- Summary card with adherence metrics

### 8.5 Replay

- Identical to live session layout
- Playback controls replace the note bar area (play/pause, speed, scrub bar)
- Risk bar shows "Replay Mode" label
- Post-replay summary modal on completion

### 8.6 Onboarding

- Centered single-column layout (480px max width)
- Step indicator at top (4 steps)
- One step visible at a time
- No sidebar, no risk bar, no coaching feed

---

## 9. Accessibility

| Requirement | Implementation |
|-------------|---------------|
| Keyboard navigation | All interactive elements reachable via Tab. Focus indicators visible. |
| Focus management | When a new prompt card appears, focus moves to it (screen reader announces). |
| Color contrast | All text meets WCAG AA contrast ratio (4.5:1 for normal text, 3:1 for large text). |
| Screen reader | Coaching prompts have proper ARIA labels. Action buttons have descriptive names. |
| Reduced motion | Respect `prefers-reduced-motion`. Disable feed auto-scroll animation when set. |
| Font scaling | Respects OS-level font scaling preferences in addition to in-app density setting. |

---

*The Desk — Where serious traders do serious work.*
