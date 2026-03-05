# The Desk

### The Trading Partner You've Been Missing

---

## The Problem

Every day, thousands of traders sit down alone and go to war with the markets. They've done the work — studied price action, built strategies, backtested ideas, watched hundreds of hours of tape. They know what they're supposed to do.

And then the bell rings, and they don't do it.

They chase. They hesitate. They move their stop. They take a B- setup because they're bored. They skip the A+ because the last one stopped them out. They overtrade on Fridays. They revenge trade after a loss. They know all of this about themselves — and they do it anyway.

This isn't a knowledge problem. It's an execution problem. It's the single biggest reason discretionary traders fail, and no tool on the market actually addresses it.

Trading journals help you reflect *after* the damage is done. AI signal generators tell you what *they* think you should trade. Psychology coaches talk about mindset with no connection to your live session. Charting platforms give you data but no guidance. Prop firm dashboards track your metrics but don't help you improve them.

Surgeons have teams. Pilots have co-pilots. Athletes have coaches on the sideline during every game. Traders have a chair and a screen and their own thoughts.

**The Desk changes that.**

---

## What Is The Desk?

The Desk is a real-time AI trading co-pilot for discretionary futures traders. It runs alongside your existing charting platform — Sierra Chart, NinjaTrader, or whatever you use — on a second monitor, connected to live market data via the DTC protocol. It helps you execute your own process: the rules you defined, the setups you backtested, the plan you made before the open.

It does not place orders. It does not generate signals. It does not tell you what to trade.

It does something harder and more valuable: **it helps you trade the way you already decided to trade.**

When you sit down, your Desk is already prepared. It's reviewed the overnight session. It knows your playbook. It's computed the delta neutral value area, identified single print zones from the prior session's TPO, checked the gamma exposure landscape, and mapped out which of your setups are most likely in play. It's pulled your journal notes from the last time this context appeared. It's ready to work.

During your session, it watches alongside you. When conditions align with one of your setups, it surfaces a contextual prompt — not "BUY NOW," but: *"Price entering the delta neutral value area from below. Your rules say look for a rebid at 21,430 with strong initiation on the DOM. Backtest shows 64% win rate with first target at the delta neutral pivot, second target at the upper boundary of the DNVA. Current GEX profile supports a move higher — positive gamma above, dealer short gamma below."*

You still watch the DOM. You still read the tape. You still decide when to queue your order and pull the trigger. The Desk just makes sure you're looking at the right thing at the right time, with all your own data at your fingertips.

After the session, it helps you review — not just P&L, but decisions. Where did you follow the plan? Where did you deviate? Why?

Over weeks and months, it identifies the behavioral patterns you can't see — the tendencies that are quietly helping or hurting your edge.

**The Desk is your second seat. Your spotter. The partner every solo trader wishes they had.**

---

## The Target Trader

The Desk is built for a specific kind of trader:

**Discretionary prop-firm and independent futures traders** who have specific, quantifiable setups and have experienced some success — but struggle with consistency. They can define their edge mathematically. They've backtested their ideas. They know what they're looking for. But they have trouble sticking to the plan when the market is live and the pressure is real.

Many of these traders operate under additional constraints — prop firm rules, small account sizes, daily loss limits — that make their edge hard to express. One bad impulse trade can blow a day or an evaluation. They need a tool that helps them stay disciplined under pressure.

Most of these traders trade alone. The Desk helps them feel — and perform — like they have a partner in the process.

**Phase 1 focus:** NQ futures scalpers and short-term discretionary traders running Sierra Chart on Rithmic. Trades lasting 10 minutes to 2 hours. Setups based on market structure (TPO, value area, delta profiles) with DOM/order flow confirmation for entry timing.

---

## Core Principles

### Your Rules, Not Ours

Every trader is different. The Desk doesn't impose a methodology. It takes *your* strategies, *your* rules, *your* playbook, and turns them into a living system that coaches you on your own terms. The value isn't in the AI's intelligence — it's in the discipline it helps you maintain.

### Coaching, Not Automation

The Desk will never push a button for you. The moment a system starts executing trades, it becomes something you watch instead of something you use. The Desk keeps you in the driver's seat. You watch the DOM. You read the tape. You time the entry. The Desk makes sure you're looking at the right setup at the right time with the right context.

### Co-Pilot, Not Cockpit

The Desk does not replace your charting platform, your execution platform, or your existing tools. It runs alongside them — specifically designed to sit on a second monitor next to your Sierra Chart (or other platform) instance. It's the intelligence and accountability layer on top of whatever setup you already use. Your charts stay cleaner. Your Desk handles the context.

---

## How It Works

### The Playbook

Everything starts here. Your playbook is the structured, formalized version of your trading strategy — the setups you trade, the rules that govern each one, and the conditions under which they apply.

Most traders carry their playbook loosely in their heads, scattered across notebooks, screenshots, and half-remembered rules. The Desk gives it a home. A guided builder helps you specify each setup with precision: entry conditions, confirmation triggers, stop logic, targets, position sizing, market context requirements, and invalidation criteria.

**This is the single most important thing The Desk does.** A trader who can't articulate their rules precisely can't follow them consistently. The playbook builder forces clarity — and that clarity is what makes everything else possible.

The Desk acts as a sparring partner during this process, powered by a high-quality LLM: *"What confirms this entry? Where exactly is your stop? What market context does this need? What invalidates the setup before entry?"*

**Defining setups works in two modes:**

1. **Structured/Quantitative** (preferred): The trader defines their setup conditions mathematically — price relative to delta neutral value area, TPO single print retest conditions, delta divergence thresholds. These become deterministic rules the system can evaluate automatically against live data.

2. **Natural Language + LLM Interpretation** (fallback): For conditions that are harder to codify — "strong initiation on the DOM," "convincing rejection of a level" — the trader describes what they're looking for, and the LLM helps interpret and prompt contextually. The goal is always to move toward more deterministic definitions over time.

**Backtest results are imported, not generated.** Traders attach performance metrics from their existing backtesting tools — Sierra Chart, NinjaTrader, TradingView, custom Python scripts, spreadsheets. The Desk provides a standardized schema (win rate, avg R, samples, profit factor, max drawdown, performance by context) and import pipelines for common formats. Results are always available contextually when a setup triggers live.

#### Example Setups

**Delta Neutral Value Area Reversion:**
```
Setup: DNVA Reversion
Context: Price is outside the delta neutral value area
Trigger: Price re-enters the DNVA boundary
Confirmation: Rebid/reoffer at the boundary level, strong initiation visible on DOM
Stop: Beyond the DNVA boundary (failed re-entry)
Target 1: Delta neutral pivot (take off one contract)
Target 2: Opposite DNVA boundary (hold runner)
Edge: Setup improves when price has breached and returned — the "retest" version
Backtest: [imported from trader's testing]
```

**TPO Single Print Retest:**
```
Setup: Single Print Retest
Context: Single prints exist from the opening range (first 30 min) or initial balance (first hour)
Trigger: Price returns to the single print zone
Confirmation: Absorption or initiative activity at the level
Stop: Through the single print zone
Target 1: Prior swing / POC
Target 2: Opposite end of value area
Edge: Classic auction theory — single prints represent initiative activity that often gets retested
Backtest: [imported from trader's testing]
```

### The Session

**Pre-Session Briefing:** Before the open, The Desk generates a briefing based on current market context and your playbook. It identifies which setups are likely in play given overnight action, key levels, scheduled events, and the current options landscape (gamma positioning, key GEX levels). It reminds you of relevant journal notes. It sets the framework for what you're looking for — and what you're ignoring.

*"Overnight range is tight, value area forming above yesterday's close. Two setups are in play: DNVA reversion if we open outside and rotate back in, and single print retest from yesterday's IB. Note: large negative GEX below 21,400 — moves could accelerate if we break there. You noted last Thursday that you force trades in low-range environments. Consider reduced size until the market picks a direction."*

**Live Coaching:** During the session, The Desk connects to real-time NQ data via the DTC protocol (Sierra Chart's DTC server on Rithmic) and continuously processes market structure:

- TPO profiles building in real time (value area, POC, single prints)
- Delta neutral value area and pivot calculation
- Cumulative session delta and delta divergences
- Options flow and gamma exposure levels (via external data providers)
- Your risk state (daily P&L, drawdown, trade count)

When conditions match a setup, the deterministic rules engine fires first — fast, sub-second. Then the LLM orchestrator synthesizes context and produces the coaching prompt within 1-2 seconds:

*"DNVA Reversion triggering. Price just re-entered the DNVA from below at 21,432. Delta neutral pivot at 21,448 — that's your T1. Upper DNVA at 21,461 — that's your T2. Session delta is positive. GEX supportive above this level. Your backtest shows 64% WR on this setup. Watch the DOM for initiation before entry."*

When you're in a trade, it tracks your position against your management plan:
- *"First target hit at the pivot. Your rules say take off one contract here."*
- *"Price stalling at T2 with delta rolling over. Your plan says consider closing the runner."*
- *"You're at 2R drawdown. Your rule says one more full loss and you're done for the day."*

**You still make every decision.** The Desk watches the structure; you watch the DOM and pull the trigger.

### The Review

After each session, trades are imported from your execution platform (Sierra Chart trade log CSV initially) and matched against your playbook. Each trade is tagged: Was it a planned setup? Did you follow your rules? Where did you deviate?

The Desk provides a structured review framework — not just "what happened" but *why*, with tracking for emotional state, plan adherence, and setup quality. Over time, this builds a searchable archive of your trading decisions, not just your trading results.

*"You followed the plan on 4 of 6 trades. The two deviations were both impulse entries in the first 15 minutes. Your journal from last week shows the same pattern. Your win rate on planned setups this month: 61%. On unplanned entries: 33%."*

### The Evolution

Across weeks and months, The Desk's pattern recognition identifies behavioral trends and strategic insights invisible in daily review:

- Are your results consistently better on certain days?
- Does your win rate drop after a big green day?
- Is one setup slowly decaying while another improves?
- Do you perform differently in trend vs. range environments?
- How does your performance change when gamma is elevated vs. flat?

These insights feed back into your playbook — generating refinements, new research questions, and evidence-based adjustments to your process.

*"Over the last 8 weeks, your DNVA reversion has produced 2.4R on trend days but -0.3R on range days. Your single print retest works best in the first 2 hours of RTH. Consider adding a time-of-day filter."*

---

## The Day One Experience

The cold start matters. The Desk ships with smart defaults so a trader isn't starting from zero:

**Pre-loaded market structure (active by default):**
- VWAP and standard deviation bands
- Prior day high/low/close, overnight high/low
- Opening range (first 30 min), Initial balance (first 60 min)
- TPO value area, POC, single prints
- Delta neutral value area and pivot
- Session cumulative delta
- Major round numbers (every 100 points on NQ)
- RTH open, Globex open, London open

**Pre-loaded risk rules (customizable):**
- Max daily loss: 3R (adjustable)
- Max consecutive losses before mandatory pause: 3
- Max trades per session: configurable
- No trading first 5 minutes after RTH open
- Warning before trading within 5 minutes of scheduled news

**Pre-loaded setup templates (inactive, available to activate and customize):**
- VWAP pullback (trend day play)
- Opening range breakout / failure
- DNVA reversion
- Single print retest
- Mean reversion from overnight extremes

**First session flow:**

1. **Connect your feed (10 minutes).** Point The Desk at Sierra Chart's DTC server — IP, port, done.
2. **Define your primary setup (15 minutes).** The playbook builder walks you through formalizing your highest-conviction setup with structured fields and LLM-assisted refinement.
3. **Run a tape replay (30 minutes).** Practice against a recorded NQ session with The Desk coaching you through it — surfacing your setup when conditions match, tracking simulated trades, reminding you of your rules. This is where the value clicks.
4. **Go live.** One well-defined setup, a replay session under your belt, and smart defaults running. Everything else compounds from here.

---

## Tape Replay

Tape replay is a first-class feature, not an afterthought. It lets traders practice their process against real recorded market sessions with full Desk coaching — same prompts, same rules evaluation, same everything — without risking capital.

**Recording:** During every live session, the Rust-based data pipeline records all tick data and DOM snapshots to a compressed binary format. NQ generates roughly 50-100k ticks per RTH session (~1-2MB uncompressed). DOM snapshots at 100ms intervals add ~2MB. Compressed with zstd, each session is a few hundred KB.

**Playback:** The replay engine reads recorded sessions and emits data at original timestamps (or accelerated — 2x, 4x, 8x). The entire system processes it identically to live data: pipelines compute structure, rules engine evaluates setups, LLM generates coaching prompts.

**Session Library:** The Desk ships with a curated library of notable NQ sessions — trend days, range days, FOMC reactions, overnight gaps, flash moves — so new traders have material to practice with immediately. Traders can also save and replay their own sessions.

**Why this matters:** Tape replay gives traders a risk-free environment to build trust in The Desk's coaching before going live. It also enables deliberate practice — something most traders have no way to do outside of live markets.

---

## Architecture

### Integration Model: The Co-Pilot

The Desk is designed to run on a dedicated monitor alongside your trading platform:

```
[Monitor 1: Sierra Chart]          [Monitor 2: The Desk]
  - Your charts                      - Session briefing
  - DOM / order entry                - Live coaching prompts
  - Order flow tools                 - Playbook status
  - Your usual layout                - Risk dashboard
                                     - Session log
```

The Desk connects to Sierra Chart via its DTC protocol server (TCP connection, binary or JSON encoding). Your charting and execution stay exactly where they are. The Desk adds the intelligence layer without touching your existing workflow.

### Data Processing Architecture

The system uses a layered architecture that separates deterministic computation from AI reasoning:

```
Sierra Chart (DTC Server on Rithmic)
         │
         ▼
┌─────────────────────────────────────────────────────┐
│  LAYER 1: Deterministic Pipelines (Rust)            │
│                                                     │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────┐ │
│  │ TPO/Auction  │  │ Order Flow   │  │ Options   │ │
│  │ - Value area │  │ - Cum delta  │  │ - GEX     │ │
│  │ - POC        │  │ - Delta div  │  │ - Gamma   │ │
│  │ - Singles    │  │ - Imbalances │  │ - Charm   │ │
│  │ - DNVA/DNP  │  │ - Absorption │  │ - Flows   │ │
│  │ - Key levels │  │              │  │           │ │
│  └──────┬───────┘  └──────┬───────┘  └─────┬─────┘ │
│         │                 │                │        │
│         ▼                 ▼                ▼        │
│  ┌──────────────────────────────────────────────┐   │
│  │  Signal Aggregator                           │   │
│  │  Structured output: levels, conditions, flags│   │
│  └──────────────────┬───────────────────────────┘   │
└─────────────────────┼───────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────┐
│  LAYER 2: Rules Engine (Rust)                       │
│  Deterministic playbook condition evaluation        │
│  Sub-second matching against aggregated signals     │
│  Fires alerts when setup conditions are met         │
└─────────────────────┬───────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────┐
│  LAYER 3: LLM Orchestrator (TypeScript → Claude API)│
│  Synthesizes signals + playbook + risk + history     │
│  Produces contextual coaching prompts (1-2 sec)      │
│  Handles ambiguous rules and natural language         │
│  Generates pre/post session analysis                  │
└─────────────────────┬───────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────┐
│  UI Layer (React/TypeScript in Tauri)               │
│  Dashboard, coaching messages, playbook, session log │
└─────────────────────────────────────────────────────┘
```

**Why this layered approach matters:**

- **Layers 1 and 2 are fast.** TPO calculations, delta tracking, VWAP computation, and rules matching are pure math — no LLM latency involved. These run at data-feed speed.
- **Layer 3 is smart but only called when needed.** The LLM receives pre-processed, structured data, not raw ticks. It only activates when there's something worth saying to the trader. This keeps latency at 1-2 seconds for coaching prompts.
- **Each layer is independently testable and debuggable.** You can verify that the TPO pipeline produces correct value areas without involving the LLM. You can test rules matching without the UI.

### Options Data Integration

The options/gamma pipeline ingests data from external providers to give traders a view of the derivatives landscape affecting NQ:

**Preferred integration (ADR-013):**

1. **Databento** — Raw options chains from OPRA (NDX, SPX, SPY, QQQ) and CME Globex (NQ futures options). We compute all Greeks and GEX ourselves in Rust for a robust, fully controlled model. Official Rust client, strong docs. See `docs/phase-2-options-databento-memo.md`.

**Alternatives (in priority order):**

2. **Unusual Whales API** — Pre-computed GEX by strike/expiry, delta, gamma, charm, vanna. Fastest path if we don't build our own model.
3. **ConvexValue** — Pre-computed gamma, gxoi, gxvolm, charm, vanna. Evaluate if Databento build proves too heavy.
4. **CBOE direct data** — Raw options chains for SPX/NDX only (no NQ futures options). Compute GEX locally.
5. **OptionData.io** — Real-time WebSocket streaming of options trades with full Greeks ($599/mo).

The options pipeline is a separate module. Traders who don't use gamma/options data can disable it entirely.

### Technology Stack

```
┌─────────────────────────────────────────────────┐
│  Tauri Desktop Application                      │
│                                                 │
│  ┌───────────────────┐  ┌────────────────────┐  │
│  │  Rust Backend     │  │  React/TS Frontend │  │
│  │                   │  │                    │  │
│  │  - DTC client     │  │  - Dashboard       │  │
│  │  - TPO pipeline   │  │  - Playbook builder│  │
│  │  - Delta pipeline │  │  - Session log     │  │
│  │  - Options pipe   │  │  - Coaching panel  │  │
│  │  - Rules engine   │  │  - Risk display    │  │
│  │  - Tape recorder  │  │  - Tape replay UI  │  │
│  │  - Replay engine  │  │  - Review/journal  │  │
│  │  - SQLite storage │  │  - shadcn/ui       │  │
│  └────────┬──────────┘  └─────────┬──────────┘  │
│           │                       │              │
│           └───────────┬───────────┘              │
│                       │                          │
│              ┌────────▼─────────┐                │
│              │  Claude API      │                │
│              │  (LLM coaching)  │                │
│              └──────────────────┘                │
└─────────────────────────────────────────────────┘
              ↕ DTC Protocol (TCP)
┌─────────────────────────────────────────────────┐
│  Sierra Chart (Rithmic data feed)               │
└─────────────────────────────────────────────────┘
              ↕ REST/WebSocket
┌─────────────────────────────────────────────────┐
│  Options Data Providers                         │
│  (Databento preferred; Unusual Whales, ConvexValue) │
└─────────────────────────────────────────────────┘
```

**Tauri** — Desktop framework with Rust backend and web frontend. Small binary, low memory footprint (critical when running alongside Sierra Chart), native system tray and multi-window support.

**Rust** — All performance-critical work: DTC protocol client, market structure pipelines, rules engine, tape recording/replay, SQLite operations.

**React + TypeScript + shadcn/ui** — All UI: dashboard, playbook builder, coaching panel, review interface. This is where LLM-assisted coding is most productive.

**SQLite** — Local embedded database for all persistent data: playbook definitions, journal entries, session logs, processed market data, backtest results, behavioral patterns.

**Claude API** — LLM inference for the coaching layer: contextual prompts, session briefings, post-session review, natural language rule interpretation, behavioral pattern analysis.

### Data Philosophy

**Local-first with optional cloud.** Your data lives on your machine — playbooks, journals, trade history, behavioral patterns, recorded sessions. The LLM sees pre-processed signals and your playbook context via API calls, but your raw data never leaves your machine.

Cloud services are used for:
- LLM API calls (Claude) — sends structured context, not raw market data
- Options data APIs — receiving data, not sending yours
- Optional encrypted backup/sync (future feature)

---

## What The Desk Is Not

- **Not an auto-trader.** It will never place an order.
- **Not a signal service.** It doesn't tell you what to trade — it helps you execute what *you* decided to trade. Every alert traces to a rule in *your* playbook.
- **Not a black box.** Every prompt is traceable to a specific rule, backtest result, or journal entry. You can always ask "why did you say that?" and get a deterministic answer.
- **Not a replacement for skill.** You still develop strategies, read the DOM, time entries, and make decisions. The Desk makes your existing skill more consistent.
- **Not a charting platform.** It runs alongside Sierra Chart (or your platform of choice). Your charts stay where they are.
- **Not investment advice.** The Desk reflects your own rules back to you. It's a coaching and discipline tool — a productivity app for traders, not a financial advisor.

---

## The Market

**What exists today:**

| Category | Examples | Gap |
|----------|----------|-----|
| Trading journals | TraderSync, Edgewonk, Trademetria | Retrospective only — no real-time coaching |
| AI signal tools | Trade Ideas, TrendSpider | Their models, not your rules |
| Psychology coaching | PushButton Trading, coaching services | No connection to live data or personal strategy |
| Prop firm dashboards | Topstep Coach T | Ecosystem-locked, evaluation-focused |
| Options analytics | SpotGamma, OmegaMind, ConvexValue | Data only — no integration with trading process |
| General AI | ChatGPT + trading prompts | No structured playbook, no live data, no discipline loop |

**What no one has built:** A system that takes your backtested rules, connects to live market data, integrates the options/gamma landscape, and provides real-time contextual coaching during your session — with a full lifecycle from preparation through evolution.

**The addressable market** is every discretionary trader who has a process but struggles to follow it consistently. Starting with NQ futures traders on prop firms and independent accounts, expanding to ES, other futures, and eventually other markets.

**The business model** is coaching, not signals. The Desk doesn't provide investment advice. It helps you follow your own plan. This positions it as a productivity/coaching tool rather than a financial advisory service, sidestepping the regulatory complexity that constrains signal services.

**The moat** is accumulated personalization. After months of use, a trader's Desk contains their entire trading practice — playbook, journal history, backtest data, behavioral patterns, coaching preferences, recorded sessions. That's not something a competitor can replicate by copying features. It's built over time, and it makes switching costly.

---

## Roadmap

### Phase 1: The Live Co-Pilot (Months 1-3)

The core product: The Desk running on a second monitor alongside Sierra Chart, connected to NQ data via DTC, with real-time coaching.

**Includes:**
- DTC protocol client receiving NQ tick/bar/DOM data from Sierra Chart (Rithmic)
- Core market structure pipelines: VWAP, TPO (value area, POC, single prints), delta neutral value area and pivot, cumulative session delta
- Playbook builder with structured setup definitions and LLM-assisted refinement
- Deterministic rules engine matching live data against playbook conditions
- LLM coaching layer producing contextual prompts when setups trigger
- Risk tracking: daily P&L, drawdown, trade count, rule compliance
- Pre-session briefing based on overnight data and playbook
- Basic session log (what was surfaced, what the trader did)
- Tape recording (every session recorded automatically)
- Tape replay with full coaching simulation
- Trade import from Sierra Chart CSV for post-session review

**Does not include (yet):**
- Options/gamma pipeline (Phase 2)
- Long-term behavioral analysis (Phase 2)
- Adaptive coaching that learns from your responses (Phase 3)
- Multiple instrument support (Phase 3)

**Success metric:** A trader runs The Desk alongside Sierra Chart for a full NQ session and says "this changed how I traded today."

### Phase 2: Intelligence Expansion (Months 4-7)

**Includes:**
- Options/gamma data integration (Unusual Whales API first, then CBOE/OptionData.io)
- Order flow analysis pipeline (delta divergences, volume imbalances, absorption detection)
- Post-session structured review with plan-adherence scoring
- Behavioral pattern analysis across sessions (overtrade detection, time-of-day patterns, consecutive loss behavior)
- Expanded backtest import formats
- Session comparison and trend analysis

**Success metric:** The Desk surfaces a behavioral insight the trader didn't know about themselves — and acting on it improves their results.

### Phase 3: Maturity (Months 8-12)

**Includes:**
- Adaptive coaching (learns which prompts you respond to, adjusts timing and tone)
- Advanced pattern recognition across months of structured data
- Additional instruments (ES, other futures)
- Additional data feed support (Denali, other DTC-compatible feeds)
- Performance analytics dashboard with multi-dimensional breakdowns
- Playbook versioning (track how your setups evolve over time)

**Success metric:** Traders report that their Desk is genuinely unique to them — a tool no one else could replicate by downloading the same software.

---

## The Vision

The gap between a trader's knowledge and their execution is the single largest source of lost edge in discretionary trading. Every tool on the market today addresses one piece of this problem. Nothing addresses the whole thing.

The Desk closes that gap — not by adding more information, more indicators, or more complexity, but by helping traders do what they already know they should do. Consistently. Every session. With a partner who knows their plan, watches the market alongside them, and holds them accountable.

**Define your edge. Prove it. Plan each session. Execute with discipline. Reflect honestly. Evolve deliberately.**

The Desk is where serious traders do serious work.
