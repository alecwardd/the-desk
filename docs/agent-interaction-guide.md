# The Desk — Agent & MCP Interaction Guide

How to get the most out of The Desk as a *trader* talking to the agents, not as a
developer editing them. This is the "driver's manual": what to say, which tools
fire, what comes back, and how to phrase requests so the system routes correctly.

> **Scope.** This guide is about *using* the system day to day. For the tool
> catalog see [docs/mcp/tool-reference.md](mcp/tool-reference.md); for scenario →
> tool routing see [skills/mcp-tools/SKILL.md](../skills/mcp-tools/SKILL.md); for
> server internals see [docs/mcp/README.md](mcp/README.md).

---

## 1. The mental model: prompt → agent → MCP → answer

Every answer you get is built in four stages. Understanding them tells you where
to push when an answer is weak.

```
YOU (prompt)
   │  natural language, e.g. "setup check on OR5 Mid Retest"
   ▼
ORCHESTRATOR AGENT          ← classifies intent, runs baseline + risk calls
   │  routes to a specialist framework (≤2 routes/turn)
   ▼
MCP SERVER (all tools)      ← deterministic Rust pipelines + SQLite, never an LLM
   │  returns structured JSON (snapshots, profiles, events, stats, freshness)
   ▼
SPECIALIST SYNTHESIS        ← Dalton / orderflow / levels / risk framework applied
   │  conditional language, reliability tiers, sample sizes
   ▼
ANSWER + mandatory risk footer
```

Three load-bearing facts:

1. **The data layer is deterministic and runs *before* the model reasons.** The
   pipelines and rules engine compute market structure in Rust; the model only
   ever sees typed tool output. If a number looks wrong, it is a data/feed issue,
   not the model "guessing" — escalate to a data-health check, not a re-prompt.
2. **The orchestrator always runs a risk + session baseline first**
   (`get_session_context`, `get_market_snapshot`, `get_risk_state`,
   `get_risk_config`, `get_account_state`) regardless of what you asked. Every
   response ends with a risk footer. This is by design — you cannot get an
   analysis without risk context attached.
3. **Live vs historical is the single most important distinction.** Live tools
   read the in-memory pipeline (current session, needs an active feed). Historical
   tools read SQLite and return *nothing useful until `backfill_history` has run*.
   Half of "the agent gave me a thin answer" cases are really "the research DB is
   empty." See §5.

---

## 2. Which client are you in? (This changes how you invoke agents)

The `agents/*.md` files are **Cursor agent definitions** (note the
`model: composer-2` / `model: claude-opus-*` frontmatter — those are Cursor model
IDs). The MCP server itself is client-agnostic (stdio). How the orchestration
behaves depends on where you sit:

| Client | How the agents work | How you start |
|--------|--------------------|---------------|
| **Cursor** (primary, intended) | `agents/` is symlinked into `.cursor/agents/`. Each file is a selectable Custom Agent. The **orchestrator** is your default driver; specialists are selectable focused modes. MCP comes from `.cursor/mcp.json`. | Pick the **orchestrator** agent in the agent selector, then talk normally. Switch to a specialist (e.g. `orderflow-analyst`) only when you want a deep single-domain read without the risk-footer overhead. |
| **Claude Code** | These `agents/*.md` are **not** auto-discovered (Claude Code subagents live in `.claude/agents/`). The MCP tools still work once the server is connected. | Connect the MCP server in your Claude Code MCP config, then either (a) tell Claude to "follow `agents/orchestrator.md`" as its operating instructions, or (b) just call tools directly using §4 as your cheat sheet. The mandatory risk footer / auto-routing only happens if you point Claude at the orchestrator prompt. |
| **Codex** | Reads the `AGENT.md` convention. MCP via its own config. | Same as Claude Code: connect the server, reference `agents/orchestrator.md` for the routing behavior. |

> **Key takeaway:** the "multi-agent" experience (auto risk footer, intent
> routing, specialist hand-offs) is a *Cursor-orchestrator* behavior. In other
> clients you get the same MCP intelligence but you drive the routing yourself —
> §4 is how you do that.

**One-time setup checklist (any client):**

1. `~/.the-desk/config.toml` exists with your real `[feed]` block (Sierra data
   dir, active contract symbol). See [README.md](../README.md#configuration).
2. The release binary is built: `CARGO_TARGET_DIR=target_alt cargo build --release --bin the-desk-mcp`.
3. Your MCP config points at that binary (template: `.cursor/mcp.example.json`).
4. Sierra Chart is running with a live feed (for live tools) **and** you have run
   at least one `backfill_history` (for historical tools).

---

## 3. The daily rhythm (what to say, in order)

You do not need to memorize tool names — say the intent in plain language and the
orchestrator routes it. These phrasings reliably hit the right route.

### Start of session
> **"Brief me"** / **"Starting my session"**

Fires the full session-start sweep: session context, market snapshot + context
frame, RVOL + tape pace, day type (RTH only), key levels + proximity, rollover
status, session history, **pre-session briefing** (carry-forward memory), and the
risk-coach session-start protocol.

**You must answer two questions it asks back:** your current account balance and
whether you hold any positions not discussed in chat. The risk-coach *gates on
this* — it will not derive R or sizing confidently until you confirm. Answer them
and it calls `save_account_state` for you.

### During the session
| You want… | Say something like… | Primary route |
|-----------|--------------------|---------------|
| A market read | "What's the market doing?", "Give me a read" | market-structure + snapshot + context frame |
| Flow / tape quality | "Is there conviction here?", "Who's in control?" | orderflow-analyst |
| Book / liquidity | "What's the book doing at 21,500?", "Are bids getting pulled?" | orderflow DOM (Tier B) |
| Levels in play | "What levels matter?", "Where's support?" | levels-analyst |
| Setup status | "Setup check on **OR5 Mid Retest**" (name it!) | playbook-evaluator |
| "What deserves attention?" | "What changed?", "Anything I should look at?" | `get_attention_inbox` / `what_changed_since` |
| Record entry | "I took 2 MNQ long at 21,480, stop 21,460, OR5 Mid Retest" | risk-coach + account state |
| Record exit | "Closed at 21,520", "Stopped out" | `record_trade_result` + risk update |
| Can I still trade? | "Where am I on risk?", "Can I take another?" | risk-coach pre-trade checklist |

### End of session
> **"Debrief the session"** / **"Review my trades"** / **"Journal this day"**

Fires the review bundle, refreshes memory, pulls journal patterns, and frames each
trade with `get_context_frame(timestampMs=entryTimestamp)` so the review reflects
context *at entry*, not now. Say **"next session, focus on X"** to create a
carry-forward follow-up that resurfaces in tomorrow's briefing.

---

## 4. Getting the most out of the MCP server (tool cheat sheet)

If you are driving tool calls yourself (Claude Code / Codex), or you want to ask
for a specific tool by name in Cursor, these are the high-value call orders. This
is the condensed form of [skills/mcp-tools/SKILL.md](../skills/mcp-tools/SKILL.md).

**The two questions to ask before any read:**
1. *Live or historical?* (current session pipeline vs SQLite history)
2. *Raw values or interpretation?* → `get_market_snapshot` for raw numbers +
   freshness; `get_context_frame` when you want bucketed, session-relative framing
   with historical analogs and reliability tiers.

### Market read
```
get_market_snapshot  +  get_context_frame   (raw + framing)
get_tpo_profile, get_key_levels, get_day_type        (structure)
get_delta_profile, get_tape_pace, get_footprint,
  get_imbalances, get_absorption_events              (flow)
get_or5_status, get_rvol, get_pinch_events,
  get_rebid_reoffer_zones, get_session_inventory     (PTT indicators)
```

### Price approaching a level (pre-entry)
```
get_proximity_report                         (what's near, by distance)
get_delta_at_price  +  check_delta_confirmation   (delta supporting direction? — REQUIRED before entry talk)
get_liquidity_behavior_at_level / get_dom_window / get_dom_regime_summary  (book quality of the test)
query_dom_reaction_at_levels / query_event_frequency  (historical reaction)
```

### Setup / trade-idea lifecycle (keep state in the system, not in chat)
```
evaluate_playbook            → all setups: met / approaching / notActive
get_setup_context            → full context for one named setup
get_active_trade_ideas       → current idea cards
mark_trade_idea_confirmed → mark_trade_idea_in_trade → mark_trade_idea_resolved / _invalidated
```

### Sizing / recording a real trade
```
get_kelly_position_size      (1/4 Kelly, confidence-scaled) + get_risk_state / get_risk_config gates
upsert_trade_entry → close_trade_entry → record_trade_result   (updates risk state)
import_trade_fills           (pull fills from the platform)
```

### Research ("how often…", "what happens after…")
```
get_research_summary         ← ALWAYS FIRST: checks session count / sample coverage
query_event_frequency        how often does event X happen per session
query_conditional            when X happens N+ times, how often does Y follow
query_distribution           distribution stats for a numeric metric
compare_sessions             analog sessions by similarity
query_signal_outcome_distribution / _conditional / _excursions   outcome stats
```

### Backtests / hypothesis promotion
```
list_hypotheses              ← first, so you never re-test a rejected idea
register_hypothesis (dryRun to validate) → run_backtest → poll get_backfill_status → summarize_hypothesis_run
get_backtest_results / compare_backtests
propose_draft_setup → (human confirm) → activate_draft_setup
```

### "Is the data trustworthy?"
```
get_feed_health              first call for "is the feed alive?"
validate_data_integrity      pipeline invariants (POC in VA, delta sums, monotonicity)
get_session_summary          tick counts + latest snapshot sanity
get_contract_rollover_status near roll dates, before trusting carry-forward levels
create_database_backup       before risky bulk operations / migrations
```

### Anti-patterns (these produce bad answers)
- Don't ask for a market read off `query_ticks` — use snapshot/profile tools; raw
  ticks are for forensics only.
- Don't interpret without a frame — pair `get_market_snapshot` with
  `get_context_frame`.
- Don't run research before `get_research_summary` confirms sample coverage.
- Don't track potential trades in chat prose — use trade-idea cards so state
  survives the session.
- Don't discuss entry before `check_delta_confirmation` (playbook doctrine).
- Don't trust prior-day levels near a contract roll without
  `get_contract_rollover_status`.

---

## 5. Historical questions need a populated database (the #1 gotcha)

Historical/backtest/performance tools read SQLite. They return empty or thin
results until you have backfilled history.

**Before any deep historical analysis:**
1. Ask **"Do we have enough history?"** → runs `get_research_summary` (session
   count + baseline distributions).
2. If the count is low, **"Backfill the last 60 sessions"** → `backfill_history`,
   then poll `get_backfill_status`. The analyst's stated minimum is **60 RTH
   sessions** for reliable work.
3. Or do it offline (no MCP needed, good for weekend prep):
   ```
   cargo run --bin the-desk-backfill -- --start 2026-03-02 --end 2026-03-06 --run-rules
   ```

Then expect every statistic to come back with a **sample size `N` and a
reliability tier** (Insufficient < 20, Directional 20–29, Reportable ≥ 30). If you
want a strong edge/sizing claim, ask for `N ≥ 30` explicitly.

---

## 6. How to prompt for the best results

The agents are heavily structured, so small wording choices change the route.

**Do:**
- **Name the thing.** "Setup check on **OR5 Mid Retest**" routes and pulls setup
  context far better than "is this a setup?". Setup names live in
  `src/rules/setup_templates.rs` and the playbook-evaluator docs.
- **State the session scope** for research: "in **RTH only**", "**Globex/Asia**".
  Mixed-scope stats get a confidence downgrade — be explicit to avoid it.
- **Give full trade details when recording:** direction, size, entry, stop, setup.
  Missing fields force the risk-coach to assume (e.g. 1R per position for heat).
- **Confirm balance/positions** when asked at session start — the whole risk
  framework (dynamic R, heat, payout cycle) keys off it.
- **Use memory triggers:** "remember this", "note that", "next session focus on
  X" → creates durable insights/follow-ups that resurface in tomorrow's briefing.
- **Ask for precedent explicitly** when you want history: "has this happened
  before?", "what usually follows?" → pulls `get_context_frame` / research tools
  instead of a pure live read.
- **Escalate data doubt directly:** "data looks wrong / stale / gappy" trips the
  data-integrity gate and blocks analysis until it's clean — exactly what you want.

**Don't:**
- Don't accept hedging when you asked for a call. Under the Grounded Partnership
  doctrine (`AGENT.md`), the agent gives straight opinions and trade proposals —
  direction, entry, stop, target — when the data supports them. If it hedges, ask
  "what evidence is missing?" — the honest answers are things like "N=14, below the
  reportable threshold" or "data quality is degraded", not vague caution.
- Don't expect it to hide conflicting reads — when structure and flow disagree it
  will *say so* ("mixed-context environment") before offering its lean. Treat that
  as a feature: you see the disagreement, then the grounded lean, then you decide.
- Don't pile multiple intents into one turn. The orchestrator caps at two
  specialist routes per turn; one clear question per turn gets a sharper answer.

**Reading the answer — the metadata block before the footer tells you how much to
trust it:**
```
Route: [primary_intent] (+ secondary)
Session Scope: [RTH / Globex / Asia / London / Combined]
Data Quality: [ok / warning / failed]
Confidence: [high / medium / low]
---
Risk: [P&L]R | Trades [N/max] | Streak [W/L] | Drawdown [X]R | Heat [Y]R | [OK / HALF SIZE / AT LIMIT / STOPPED]
```
If `Data Quality` is `warning`/`failed` or `Confidence` is `low`, the read is
provisional — push on freshness/sample size before acting.

---

## 7. Agent-definition cleanup — applied 2026-06-22

A full pass over `agents/`, `AGENT.md`, `AGENTS.md`, the skills, and the `.cursor`
config produced the changes below. All are doc/frontmatter only — no Rust or MCP
tool behavior changed.

### 7.1 ✅ Fixed — `AGENTS.md` corruption
The untracked `AGENTS.md` was a near-verbatim copy of `CLAUDE.md` with a botched
find/replace that turned **"Claude" → "Codex"** throughout ("Cursor (and Codex,
Codex)", "Never call the **Codex** API from Rust", "LLM | **Codex** API"). Those
statements were nonsensical and contradicted the real rules, and `AGENTS.md` is a
filename many tools auto-load by convention.
**Done:** replaced it with a thin pointer to the canonical `AGENT.md` + `CLAUDE.md`
(+ this guide). It can no longer drift because it contains no duplicated rules.

### 7.2 ✅ Applied (verify the model string) — model tiers
Previously the orchestrator used `model: claude-opus-4-6` while **every** specialist
— including the safety-critical `risk-coach` and the heaviest analytical agents —
ran on `model: composer-2`, i.e. the deepest reasoning and the hard-stop/sizing
logic were on the fast tier.
**Done:** promoted the reasoning-heavy / safety-critical agents to the frontier
model the orchestrator already uses (`claude-opus-4-6`):
`risk-coach`, `market-structure-analyst`, `orderflow-analyst`, `playbook-evaluator`.
Left on `composer-2` (lighter / more mechanical): `levels-analyst`,
`backtest-analyst`, `performance-analyst`, `data-integrity-validator`,
`pipeline-verifier`, `prompt-quality-evaluator`, `options-api-researcher`.
> **Verify:** `claude-opus-4-6` is the string that was already in `orchestrator.md`
> — reused for safety, not invented. If your Cursor build offers a newer Opus,
> bump every `model: claude-opus-4-6` occurrence in one pass. Promoting agents to a
> frontier model raises per-call cost in Cursor; revert any agent to `composer-2`
> if you'd rather economize there.

### 7.3 ✅ Fixed — consistency gap
`backtest-analyst.md` now opens with the standard `**Tool routing:**` header that
every other specialist uses. (`commands/pipeline-test.md` and
`commands/coaching-test.md`, referenced by the dev agents, exist and were left as-is.)

### 7.4 ✅ Fixed — trimmed redundant always-applied reads
Six agents opened by instructing a re-read of `CLAUDE.md` / `AGENT.md` on **every**
interaction, even though Cursor injects both via `.cursor/rules/*.mdc`
(`alwaysApply: true`) and the other six agents already omitted them.
**Done:** removed the unconditional `CLAUDE.md` / `AGENT.md` read steps from
`market-structure-analyst`, `orderflow-analyst`, `data-integrity-validator`,
`pipeline-verifier`, `prompt-quality-evaluator`, and `options-api-researcher`
(replaced with a one-line portability note), and renumbered their sequences. The
conditional, task-specific reads (trading-domain / compliance skills, `commands/*`
test workflows) were kept. All 12 agents are now consistent.

### 7.5 ✅ Fixed — orchestration model made explicit
`AGENT.md`'s Subagent Patterns section now states that, in Cursor, specialists are
frameworks the orchestrator *applies* (not auto-spawned services), and that in
clients without an auto-spawn mechanism you drive routing yourself (this guide).

### 7.6 ✅ Fixed — count drift + stale docs (repo-wide)
- **Tool-count drift guard widened.** `121 MCP tools` was hardcoded in ~20 spots
  but `documented_mcp_tool_count_matches_router` only checked three —
  `read_pool.rs` had already drifted to "120". Now the test also guards
  `.cursorrules`, `docs/mcp/README.md`, `skills/mcp-tools/SKILL.md`, and the
  `handler.rs` server-instructions string (all normalized to the `N MCP tools`
  phrasing). Decorative mentions in the agent headers and this guide were
  de-numbered so they can't drift, and `read_pool.rs` no longer hardcodes a count.
- **Stale concurrency doc fixed.** `docs/mcp/README.md` called the read-only
  connection pool "the planned improvement" — it has shipped (`read_pool.rs`,
  `with_read_db`). Rewritten to describe the actual model.
- **`commands/` documented.** The nine Cursor slash-commands are now listed in
  `CLAUDE.md`'s File Structure.
- **Stale paths repointed.** `skills/tauri-bridge/SKILL.md` → `src/bin/the-desk-mcp/`
  (now a directory); `options-api-researcher` → `docs/phase-2-options-databento-memo.md`.

### Optional / not done (your call)
- **Newer model string** — see the 7.2 verify note.
- **`docs/setup-ideas-and-backtesting.md`** still references the pre-split
  `src/bin/the-desk-mcp.rs` single file and recommends splitting it (since done).
  Left as-is: it's a point-in-time analysis doc, not a live spec.

### What's already strong (left untouched)
- Uniform agent skeleton: `Always Do This First` → primary tool tables → decision
  tree → output format → cross-agent boundaries → compliance → "When uncertain".
- Disciplined live-vs-historical separation and the staleness/`dataAgeMs`
  handling baked into every agent.
- The Research Sample Size Policy and reliability tiers applied consistently.
- Hard non-advisory framing and the mandatory risk footer — these are the
  product's spine and they are enforced everywhere. *(Superseded 2026-07-05: the
  non-advisory framing was replaced by Grounded Partnership — see §8. The risk
  footer, sample-size discipline, and hard stops remain the spine.)*
- `skills/mcp-tools/SKILL.md` as a single, generated-doc-backed routing source.

---

## 8. Grounded-partnership doctrine + map-vs-territory conventions — applied 2026-07-05

A second full pass over the agent surface. Two changes, both docs/prompts only — no Rust
or MCP tool behavior changed.

### 8.1 Advisory language replaced by grounding discipline

The repo's prompts enforced a public-product "non-advisory / coaching-only" boundary
(never recommend, forbidden-phrase lists, a compliance-research skill) that contradicted
both CLAUDE.md rule #4 and how the trader actually uses the system. Replaced everywhere
with **Grounded Partnership** (canonical text: `AGENT.md`): agents proactively propose
trade ideas — direction, entry, stop, target — and give straight opinions, with grounding
mandatory (playbook rules, structure/flow evidence, or backtest stats with `N` +
reliability tier). What did **not** change: risk footers, binary hard stops and circuit
breakers, the sample-size policy, conflict reporting before any lean, Layer 2 alerts
firing only from the trader's own rules, and the trader pressing the buttons.

Touched: `CLAUDE.md`, `AGENT.md`, `.cursorrules`, `README.md`, `orchestrator`,
`risk-coach`, `playbook-evaluator`, `market-structure-analyst`, `orderflow-analyst`,
`levels-analyst`, `performance-analyst`, `backtest-analyst`, `options-api-researcher`,
`prompt-quality-evaluator` (re-missioned as the grounding evaluator),
`commands/coaching-test.md` (now tests grounding in both failure directions), and the
social-intelligence docs. `skills/compliance-research/` is archived to
`docs/archive/compliance-research/` with a banner.

### 8.2 Map-vs-territory conventions added

- **`/unknowns-pass`** (`commands/unknowns-pass.md`) — pre-implementation blindspot
  checklist built from this repo's own paid-for incidents (stale rules binary, silent
  contract mismatch, IDEA-000 gate/entry contradiction, over-firing state flags,
  golden-bless discipline). Wired into `AGENT.md`'s workflow and the dev-facing agents.
- **`AGENT.md` "Map vs Territory Conventions"** — interview protocol for ADR-scale
  features (Pending ADR + one question at a time), dated implementation notes in the
  nearest doc, and this very section's format as the post-change explainer standard.
- **Idea template** in `docs/setup-ideas-and-backtesting.md` — tweakables first, known
  unknowns / invalidation criteria, instrumentation dependencies, consistency check,
  settled-verdict check.
- **De-numbered** the per-domain tool-count table in `AGENT.md` (counts live in the
  generated `docs/mcp/tool-reference.md`, which cannot drift).

### 8.3 Check yourself (the quiz)

1. *The agent says "I'd take this long here." What must accompany that sentence for it
   to be valid output?* — Cited evidence: the playbook rule / structure / flow read, and
   any statistic with `N` + reliability tier; full conviction only at `N ≥ 30` verified.
2. *Can the agent propose a brand-new setup idea directly into the live playbook?* —
   No. Agent ideas route through `register_hypothesis` → backtest → `propose_draft_setup`
   → trader-confirmed `activate_draft_setup`. Layer 2 still fires only trader-owned rules.
3. *You're at 3 consecutive losses and the agent has a grounded A+ setup read. What
   happens?* — Hard stop wins. Circuit breakers are binary; no idea survives them.

---

## 9. Quick reference card

```
SESSION START   → "Brief me"   (answer the balance + positions questions)
MARKET READ     → "Give me a read"
FLOW            → "Who's in control / is there conviction?"
BOOK            → "What's the book doing at <price>?"
LEVELS          → "What levels are in play?"
SETUP           → "Setup check on <SETUP NAME>"
ATTENTION       → "What changed / anything I should look at?"
RECORD ENTRY    → "<dir> <size> <symbol> at <price>, stop <price>, <setup>"
RECORD EXIT     → "Closed at <price>" / "Stopped out"
RISK            → "Where am I on risk? Can I take another?"
HISTORY         → "Do we have enough history?" → "How often does <event> happen in RTH?"
BACKTEST        → "Backtest <idea>"  (list_hypotheses first)
REVIEW          → "Debrief the session"  → "Next session, focus on <X>"
DATA DOUBT      → "Data looks stale/wrong"
```

Trust ladder for any statistic: **N < 20 = directional only · 20–29 = caveated ·
≥ 30 = reportable.** Always check the `Data Quality` / `Confidence` line before
acting on a read.
