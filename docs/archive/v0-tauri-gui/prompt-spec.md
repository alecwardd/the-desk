# The Desk — LLM Prompt Engineering Specification

**Version:** 1.0
**Date:** 2026-02-25
**Status:** Draft

---

## 1. Overview

This document specifies how The Desk constructs, sends, and manages prompts to the Claude API. Every coaching prompt must trace to a specific playbook rule — never speculative. All language must use the "your rules say..." framing, never advisory language like "you should buy/sell."

**Canonical constraints:**
- Only `conditions_met` state transitions trigger Claude API calls (per Tech Plan)
- Rust emits a minimal `SetupAlert`; TypeScript assembles the full context via 3 Tauri command calls
- All prompts are stateless — context is rebuilt per call, no long conversations maintained
- Graceful degradation: if the Claude API is unreachable, raw alerts display without coaching prose

---

## 2. Model Selection

| Use Case | Model | Rationale | Latency Budget |
|----------|-------|-----------|----------------|
| Live coaching prompts | Claude Sonnet | Fast, structured prompts with known context | <2s total |
| Risk warnings | Claude Sonnet | Speed-critical, formulaic prompts | <2s total |
| In-trade management | Claude Sonnet | Speed-critical during active trades | <2s total |
| Pre-session briefing | Claude Opus | Longer analysis, more nuanced reasoning | <10s |
| Post-session review | Claude Opus | Deep analysis of session patterns | <30s |
| Playbook builder chat | Claude Sonnet | Interactive, multi-turn with the trader | <3s per turn |

**Cost estimate:** ~20-50 Claude calls per trading session, predominantly Sonnet. Estimated cost: <$1 per session.

---

## 3. System Prompts

### 3.1 Base System Prompt (All Use Cases)

This preamble is prepended to every prompt sent to Claude.

```
You are the coaching engine for The Desk, a real-time trading co-pilot for
discretionary NQ futures traders. You are NOT a trading advisor. You do not
generate signals, recommend trades, or give financial advice.

Your role:
- Reflect the trader's own playbook rules back to them in real time
- Reference specific setup conditions, backtest metrics, and risk state
- Use the framing "your rules say..." / "your plan indicates..." / "your
  playbook specifies..." — NEVER "you should buy/sell" or "this is a good trade"
- Be concise and actionable — traders are watching a live market
- Every statement must trace to a specific rule, metric, or data point provided
  in the context — never speculate beyond the data given

Language rules:
- Never use "I recommend", "I suggest", "you should", "consider buying/selling"
- Always attribute to the trader's rules: "your rules say", "your plan says",
  "your backtest shows", "your risk config specifies"
- Never characterize market conditions as "good" or "bad" for trading
- Never predict price direction or outcomes
- Use present tense for current state, past tense for backtest data
```

### 3.2 Live Coaching Prompt

Triggered when the rules engine transitions a setup to `conditions_met`.

```
SYSTEM: {base_system_prompt}

You are generating a live coaching prompt during an active trading session.
Be concise — 2-4 sentences maximum. Include the setup name, specific entry
level, stop, and targets. Reference backtest metrics. Note any relevant risk
state constraints.

CONTEXT:
Setup: {setup_name}
Description: {setup_description}
State transition: conditions_met
Triggered conditions: {triggered_conditions_list}
Current price: {current_price}

Entry logic: {entry_logic}
Stop logic: {stop_logic}
Targets: {targets_with_management_rules}
Position sizing: {position_sizing}

Backtest metrics:
- Win rate: {win_rate}% over {samples} samples ({period})
- Avg winner: {avg_winner_r}R | Avg loser: {avg_loser_r}R
- Profit factor: {profit_factor} | Expectancy: {expectancy_r}R
- Max consecutive losses: {max_consecutive_losses}

Current risk state:
- Daily P&L: {daily_pnl_r}R
- Trades today: {trade_count} / {max_trades}
- Consecutive losses: {consecutive_losses} / {max_consecutive_losses}
- Drawdown: {drawdown_r}R / {max_daily_loss_r}R limit

Market state:
- VWAP: {vwap} | Last: {last_price}
- VA: {va_high} / {poc} / {va_low}
- DNVA: {dnva_high} / {dnp} / {dnva_low}
- Session delta: {session_delta} | Cumulative: {cumulative_delta}
- OR: {or_high} / {or_low} | IB: {ib_high} / {ib_low}

Relevant journal notes (if any):
{journal_notes}

Generate a coaching prompt that tells the trader their setup conditions are met.
Reference specific prices, rules, and metrics. Do not suggest whether to take
the trade.
```

### 3.3 Risk Warning Prompt

Triggered when the risk tracker detects the trader is approaching or breaching limits.

```
SYSTEM: {base_system_prompt}

You are generating a risk warning. Be direct and factual. Reference the
trader's own risk rules. State the current numbers and the limit. Do not
editorialize about whether the trader should continue.

CONTEXT:
Warning type: {warning_type}
Current state: {current_risk_state}
Limit: {risk_limit}
Rule source: {rule_description}

Generate a risk warning that states the facts and references the trader's rule.
```

**Warning types:**
- `approaching_daily_loss` — within 1R of max daily loss
- `daily_loss_breached` — max daily loss hit
- `consecutive_losses` — max consecutive losses reached
- `max_trades` — max trades per session reached
- `no_trade_zone` — currently in a configured no-trade period

### 3.4 In-Trade Management Prompt

Triggered when price reaches a target level or conditions change while a trade is open.

```
SYSTEM: {base_system_prompt}

You are generating an in-trade management prompt. The trader has an open
position. Reference their specific management rules for this target level.
Be brief — 1-2 sentences.

CONTEXT:
Setup: {setup_name}
Position: {direction} {size} contracts at {entry_price}
Current price: {current_price}
Current P&L: {pnl_r}R ({pnl_points} points)
Stop: {stop_price}

Target being reached: {target_level} ({target_name})
Management rule for this target: {management_rule}

Targets hit so far: {targets_hit}
Remaining targets: {remaining_targets}

Generate a management prompt referencing the trader's specific rule for this
target level and their current position state.
```

**Condition-based variant** (when no trade is logged via "Took it"):

Replace the position context with:
```
No live position logged. The trader may or may not be in this trade.
Use conditional framing: "If you're in this trade, your rules say..."
```

### 3.5 Pre-Session Briefing Prompt

Triggered at configurable time before RTH open (default: 9:15 AM ET).

```
SYSTEM: {base_system_prompt}

You are generating a pre-session briefing. Analyze the overnight context and
identify which of the trader's setups are most likely in play. For each setup,
explain WHY it's relevant given the current market context — reference specific
levels and conditions. Surface relevant journal notes from similar past contexts.

Be thorough but structured. Use sections: Key Levels, Setups in Play, Relevant
Journal Notes.

CONTEXT:
Date: {date}
RTH opens in: {minutes_until_open} minutes

Overnight data:
- Overnight high: {overnight_high}
- Overnight low: {overnight_low}
- Prior day high: {prior_day_high}
- Prior day low: {prior_day_low}
- Prior day close: {prior_day_close}
- Prior VA: {prior_va_high} / {prior_poc} / {prior_va_low}
- Prior DNVA: {prior_dnva_high} / {prior_dnp} / {prior_dnva_low}

Active setups:
{for each setup:}
  Name: {setup_name}
  Conditions: {conditions_summary}
  Backtest: WR {win_rate}% | {samples} samples | Expectancy {expectancy_r}R
{end for}

Risk state:
- Daily P&L carried forward: {daily_pnl_r}R (if multi-session day)
- Max daily loss: {max_daily_loss_r}R

Recent journal notes (similar contexts, last 30 days):
{journal_notes_with_dates}

For each active setup, provide:
1. A likelihood tag: "Likely" or "Possible"
2. A 1-2 sentence explanation of why, referencing specific levels
3. Inline backtest metrics
```

### 3.6 Post-Session Review Prompt

Triggered after the trader ends a session and completes trade review.

```
SYSTEM: {base_system_prompt}

You are generating a post-session review analysis. Analyze the trader's
session against their own plan. Focus on process, not P&L. Identify where
they followed their rules and where they deviated. Be honest but constructive.
Reference specific moments, setups, and data.

CONTEXT:
Session date: {date}
Session duration: {duration}
Pre-session note: {presession_note}

Coaching activity:
- Total prompts surfaced: {total_prompts}
- Responses: {took_it_count} took it, {watching_count} watching, {passed_count} passed
- Prompt adherence: {prompt_adherence}%

Trades (if available):
{for each trade:}
  Setup: {setup_name_or_unplanned}
  Direction: {direction} | Entry: {entry_price} | Exit: {exit_price}
  Result: {result_r}R
  Planned: {planned} | Rules followed: {rules_followed}
  Emotional state: {emotional_state}
  Note: {trade_note}
{end for}

Rules adherence: {rules_adherence}% (of traded setups where rules were followed)
Session P&L: {session_pnl_r}R

Risk events during session:
{risk_warnings_fired}

Generate a review that:
1. Summarizes what happened (2-3 sentences)
2. Highlights where the trader followed their plan (specific moments)
3. Identifies deviations without judgment — state what happened vs. what the
   plan specified
4. Notes any patterns if visible (e.g., "This is the third session where you
   passed on the first setup and took a lower-quality entry later")
5. Ends with one concrete focus area for the next session
```

### 3.7 Playbook Builder Chat Prompt

Used during the interactive setup definition flow.

```
SYSTEM: {base_system_prompt}

You are helping a trader define a trading setup in structured format. Your job
is to ask clarifying questions that turn a natural-language description into
machine-evaluable conditions. Be conversational but precise.

You need to help them define:
- Entry conditions (deterministic fields the rules engine can evaluate)
- Discretionary conditions (things the trader observes on their DOM/charts)
- Stop logic
- Targets with management rules at each level
- Position sizing
- Market context requirements
- Invalidation criteria

Available condition fields:
{list of SetupCondition field enums with descriptions}

The trader will describe their setup in natural language. Ask focused questions
to fill in the structured fields. When you have enough information, present the
structured setup definition for their confirmation.
```

---

## 4. Context Construction Strategy

### 4.1 Data Flow

When the rules engine fires a `conditions_met` transition, the TypeScript LLM Orchestrator:

1. Receives the `SetupAlert` event (setup_id, state_transition, triggered_conditions, current_price)
2. Calls `get_setup(setup_id)` — retrieves full playbook rules for the setup
3. Calls `get_risk_state()` — retrieves current risk configuration and state
4. Calls `get_recent_journal_notes(context)` — retrieves up to 3 relevant journal entries
5. Assembles the prompt template with the retrieved data
6. Sends the prompt to the Claude API
7. Stores the response as a `SessionEvent` (event_type: coaching_prompt)
8. Emits a `coaching-prompt` Tauri event to the UI
9. Acknowledges to Rust (transitions setup to `confirmed` state)

**Latency budget:** Steps 2-4 add ~5-15ms (3 IPC round-trips). Step 6 is ~1-2s (Claude API). Total: <2s.

### 4.2 What Goes Into Each Prompt Type

| Prompt Type | MarketState | Setup | RiskState | Journal | Trades |
|-------------|:-----------:|:-----:|:---------:|:-------:|:------:|
| Live coaching | Full snapshot | Full rules + backtest | Current state | Up to 3 notes | -- |
| Risk warning | -- | -- | Current state + config | -- | -- |
| In-trade mgmt | Last price only | Target rules | Current P&L | -- | Open trade |
| Pre-session | Overnight levels | All active setups | Carried P&L | Up to 5 notes | -- |
| Post-session | -- | Referenced setups | Session summary | Pre-session note | All session trades |
| Playbook chat | -- | In-progress setup | -- | -- | -- |

### 4.3 Token Budget

| Prompt Type | Estimated Input Tokens | Estimated Output Tokens | Model |
|-------------|----------------------:|------------------------:|-------|
| Live coaching | ~800-1,200 | ~100-200 | Sonnet |
| Risk warning | ~300-500 | ~50-100 | Sonnet |
| In-trade mgmt | ~500-800 | ~50-100 | Sonnet |
| Pre-session | ~2,000-4,000 | ~500-1,000 | Opus |
| Post-session | ~3,000-6,000 | ~500-1,500 | Opus |
| Playbook chat | ~1,000-2,000 | ~200-500 | Sonnet |

**Token management:** Context is rebuilt per call — no conversation history accumulates. The largest prompts (post-session with many trades) stay well under Sonnet/Opus context limits.

---

## 5. Graceful Degradation

When the Claude API is unreachable, The Desk displays raw alerts without coaching prose.

### 5.1 Raw Alert Format

```
[RAW ALERT] {setup_name}

Conditions met:
{for each triggered_condition:}
  - {condition_description}
{end for}

Entry: {entry_logic_summary}
Stop: {stop_logic_summary}
Targets: {targets_summary}

Backtest: WR {win_rate}% | {samples} samples | {expectancy_r}R expectancy

[Took it] [Watching] [Passed]
```

### 5.2 Raw Risk Warning Format

```
[RISK WARNING] {warning_type}

{current_value} / {limit_value} {unit}
Rule: {rule_description}
```

### 5.3 Degradation Behavior

| Scenario | Behavior |
|----------|----------|
| Claude API timeout (>5s) | Display raw alert immediately, retry prompt in background. If retry succeeds, replace raw alert with coaching prompt. |
| Claude API error (4xx/5xx) | Display raw alert. Log error. Do not retry until next alert. |
| Claude API key missing | Display raw alert for all prompts. Show persistent UI indicator: "Coaching mode: raw alerts (no API key configured)." |
| Network unreachable | Display raw alert. Check connectivity every 30s. Resume coaching when connection restored. |
| Rate limited (429) | Display raw alert. Respect `Retry-After` header. Resume when allowed. |

### 5.4 Source Labeling

Every `CoachingPrompt` event includes a `source` field:
- `"llm"` — generated by Claude API
- `"raw"` — generated from template without Claude API

The UI displays a subtle indicator so the trader knows which mode is active.

---

## 6. Prompt Quality Criteria

Every prompt generated by The Desk must satisfy these criteria:

| Criterion | Description | Validation Method |
|-----------|-------------|-------------------|
| **Traceability** | Every statement traces to a specific playbook rule, backtest metric, or market data point | Snapshot test: verify template variables are populated |
| **Non-advisory** | No advisory language ("you should", "I recommend", "consider buying") | Regex scan of output for banned phrases |
| **Actionability** | Prompt includes specific prices, levels, and rules the trader can act on | Template structure ensures required fields |
| **Conciseness** | Live prompts: 2-4 sentences. Management: 1-2 sentences. Risk: 1-2 sentences | Token count check on output |
| **Accuracy** | All numbers (prices, percentages, R-values) match the source data exactly | Cross-reference output against input context |
| **Compliance** | Language follows regulatory coaching-vs-advisory positioning | Prompt quality evaluator subagent review |

### 6.1 Banned Phrases

The following phrases must never appear in any coaching prompt:

```
you should buy/sell
I recommend
I suggest
consider buying/selling
this is a good/bad trade
the market will/won't
price will/should go up/down
take this trade
don't take this trade
this looks bullish/bearish (as recommendation)
strong buy/sell signal
```

### 6.2 Required Attributions

Every coaching statement must use one of these attribution patterns:

```
your rules say...
your plan specifies...
your playbook indicates...
your backtest shows...
your risk config specifies...
your setup conditions show...
based on your {setup_name} rules...
per your management rules...
```

---

## 7. Testing Approach

### 7.1 Snapshot Tests

Each prompt template has snapshot tests that:
1. Provide known input data (setup, market state, risk state, journal notes)
2. Verify the assembled prompt contains all required fields
3. Verify no banned phrases appear in the template itself
4. Verify the prompt structure matches the expected format

### 7.2 Output Quality Tests (Offline)

For development and CI, use saved Claude API responses:
1. Generate prompts with a representative set of market scenarios
2. Save the responses as golden files
3. Run banned-phrase and attribution-pattern checks on the saved responses
4. Manual review of golden files for quality and accuracy

### 7.3 Live Monitoring

During live sessions:
- Log all prompts sent and responses received (in session events)
- Track response latency (alert-to-prompt-displayed)
- Track degradation events (raw alerts served)
- Track token usage per session for cost monitoring

---

## 8. Interface Contract Reference

The prompt spec relies on these contracts defined in [tech-plan.md](tech-plan.md):

- **`SetupAlert`** (Rust -> TypeScript): setup_id, setup_name, state_transition, triggered_conditions, current_price, timestamp
- **`MarketState`** (Rust -> TypeScript, 4Hz): all pipeline outputs
- **`CoachingPrompt`** (TypeScript -> TypeScript): id, session_event_id, setup_id, setup_name, message, priority, source, timestamp

---

*The Desk — Where serious traders do serious work.*
