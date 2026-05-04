---
name: Trader Memory Layer
overview: Implement trader memory as a typed, provenance-rich decision-support layer. Reuse the existing `behavioral_patterns` refresh path for execution memory, keep setup opportunity separate from trader execution, capture planned R-at-entry before analytics need it, add a bounded `get_trader_context_fit` MCP envelope with traceable evidence, render markdown only as derived capsules, and extend `get_context_frame` only if existing analogs prove insufficient.
todos:
  - id: phase0-contract-doc
    content: "Write `docs/trader-memory/architecture.md`: taxonomy, source ownership, reliability contract, intent budgets, small-N suppression, account scoping, ordinal/post-loss/scratch scoping, casing/ID conventions, cost budget, and no-sizing-from-memory rule."
    status: completed
  - id: phase0-risk-coach-guardrail
    content: "Land the risk-coach prompt guardrail before the new tool exists: memory reports context, pattern memory is not a Kelly input, and memory never adjusts size by itself."
    status: completed
  - id: phase0-dirty-write-audit
    content: Audit every trade/session/session-summary write path and verify it calls `mark_memory_dirty` or intentionally documents why it does not.
    status: completed
  - id: phase0-memory-module-split
    content: Mechanically rename `src/memory.rs` to `src/memory/mod.rs` so `src/memory/trader_context.rs` and future digest modules can live under the memory namespace.
    status: cancelled
  - id: phase0-shared-reliability
    content: Promote or move `ReliabilityTier` into a shared public module and expose one `reliability_tier(sample_size)` helper used by research and memory.
    status: completed
  - id: phase0-trade-r-capture
    content: Add `planned_r_points_at_entry` and `planned_r_dollars_at_entry` columns to `trades`; capture them in trade-write/import paths so future risk-deviation analytics have truthful historical inputs.
    status: completed
  - id: phase1-db-context-row
    content: Add `db.list_closed_trades_with_session_context_for_pattern_refresh(...)` for dirty-refresh aggregation and a separate recent-session-trades helper for live risk context.
    status: completed
  - id: phase1-pattern-slices
    content: "Extend `detect_behavioral_patterns` with stable metric schemas for intersected execution slices: setup x time bucket, setup x day type, post-loss-after-1, and post-loss-after-2-plus, globally and by setup."
    status: completed
  - id: phase1-context-fit-adapter
    content: Build `src/memory/trader_context.rs` as a retrieval/ranking/shaping adapter over `behavioral_patterns`, `agent_insights`, `memory_followups`, and live session trades; include evidence IDs/timestamps and do not recompute existing aggregate stats on every read.
    status: completed
  - id: phase1-mcp-tool
    content: Add `get_trader_context_fit` MCP support with Rust-enforced intent budgets, traceable provenance, small-N suppression, maintenance state, and an opportunity capability pointer to `get_context_frame`; make `get_pre_session_briefing` delegate memory ranking to the same builder.
    status: completed
  - id: phase1-agent-wiring
    content: Wire orchestrator, risk-coach, and performance-analyst to use `get_trader_context_fit`; lead risk-coach changes with the rule that memory never adjusts sizing by itself.
    status: completed
  - id: phase2-opportunity-overlay
    content: Add optional opportunity detail from existing `signal_outcomes`, setup performance tools, and `get_context_frame`, while keeping signal/backtest opportunity separate from executed-trade memory.
    status: pending
  - id: phase3-memory-capsules
    content: Add short deterministic markdown capsules under `~/.the-desk/memory/` with freshness policy, trigger wiring, and no auto-read outside session-start/session-review flows.
    status: cancelled
  - id: phase4-context-frame-extension
    content: Extend existing `get_context_frame` analog output only if a measured gap remains; do not create `src/memory/analog.rs`, `session_fingerprints`, or parallel analog MCP tools.
    status: pending
  - id: validation
    content: Validate with unit tests, behavioral-pattern tests, MCP response-shape tests, hand-rolled JSON fixture snapshot tests, prompt checks, targeted cargo tests, and full `cargo test` before implementation is complete.
    status: completed
isProject: false
---

# Trader Memory Layer

## Core Decision

Build memory as a typed decision-support contract over existing memory infrastructure, not as a second aggregation system.

The existing repository already has most of the execution-memory substrate:

- `detect_behavioral_patterns` aggregates actual trade behavior into `behavioral_patterns`.
- `mark_memory_dirty` and `memory_maintenance_state` already gate refresh after trade/review/import writes.
- `get_memory_brief` and `get_pre_session_briefing` already rank memory against context.
- `agent_insights` and `memory_followups` already provide qualitative coaching memory.
- `signal_outcomes`, setup performance tools, and `get_context_frame` already provide setup/opportunity and analog context.

The new feature should therefore do two things:

1. Add missing execution slices to the existing pattern-refresh path.
2. Add a typed `get_trader_context_fit` envelope that retrieves, ranks, truncates, and explains those facts for the current intent.

It should not rescan and recompute all trade statistics on every MCP read. That would duplicate math, risk inconsistent numbers against `get_memory_brief`, and bypass the existing dirty/refresh contract.

## Memory Taxonomy

Keep three models separate in code, tool output, and agent phrasing.

### Opportunity Model

Question: "What has this setup or market context done historically?"

Sources:

- `signal_outcomes`
- `get_signal_performance`
- `get_setup_performance_matrix`
- `query_signal_outcome_*`
- `get_context_frame`

Consumers:

- setup checks
- market reads
- performance research

Guardrail:

- Opportunity stats describe setup signals, backtests, or market analogs. They are not the trader's executed fills.

### Execution Model

Question: "How has this trader actually behaved and performed in this situation?"

Sources:

- `trades`
- `sessions`
- `session_summaries`
- trade review fields
- `behavioral_patterns`

Consumers:

- risk coach
- setup checks
- session starts
- session reviews

Guardrail:

- Only human/imported executed trades count as execution memory. Do not treat `signal_outcomes` as actual trader performance.

### Coaching Memory

Question: "What should the agent remember in language?"

Sources:

- `agent_insights`
- `behavioral_patterns`
- `memory_followups`
- journal entries
- optional curated doctrine

Consumers:

- orchestrator synthesis
- risk-coach reminders
- session review

Guardrail:

- Rank and scope reminders. Do not dump all notes into every turn.

## Response Contract

Do not implement `todayBias: green | amber | red`.

The tool returns a context-fit envelope with evidence and caveats. It does not produce trade advice.

```json
{
  "intent": "setupCheck",
  "currentContext": {
    "tradingDay": "2026-05-01",
    "accountScope": "allAccounts",
    "sessionType": "RTH",
    "sessionSegment": "None",
    "timeBucket": "rthOpen",
    "dayType": "DoubleDistribution",
    "profileShape": "bShape",
    "balanceState": "imbalanced",
    "setupId": "tpl_dnva_retest"
  },
  "executionFit": {
    "summary": "Limited personal execution sample in this slice.",
    "matchingSlices": [],
    "strongestPositiveEvidence": [],
    "strongestCautionEvidence": [],
    "missingData": []
  },
  "opportunityFit": {
    "summary": "Opportunity data is separate from trader execution.",
    "setupOutcome": null,
    "contextFrameAnalog": {
      "available": true,
      "source": "get_context_frame",
      "detailAvailableByCalling": "get_context_frame",
      "caveats": ["Context-frame analogs are not executed-trade memory."]
    },
    "missingData": []
  },
  "coachingMemory": {
    "patterns": [],
    "insights": [],
    "followups": []
  },
  "riskContext": {
    "tradeOrdinalInSession": null,
    "postLossStateInSession": null,
    "riskDeviation": {
      "available": false,
      "availability": {
        "kind": "preCapture",
        "reason": "Risk deviation cannot be evaluated for this row; do not infer risk compliance from this absence."
      }
    },
    "ruleAdherenceFlags": []
  },
  "reliability": {
    "overallTier": "insufficient",
    "caveats": [
      "Execution and opportunity samples are reported separately.",
      "Numeric stats are suppressed below the configured small-N floor."
    ]
  },
  "provenance": {
    "executionSources": ["behavioral_patterns", "trades", "sessions", "session_summaries"],
    "opportunitySources": ["signal_outcomes", "context_frame"],
    "coachingSources": ["behavioral_patterns", "agent_insights", "memory_followups"],
    "evidenceIds": {
      "patterns": [],
      "insights": [],
      "followups": []
    },
    "evidenceTimestampsMs": {
      "patternsRefreshedAtMs": null,
      "insightsLastUpdatedMs": null
    }
  },
  "maintenance": {
    "refreshSuggested": false,
    "dirtyReasons": []
  }
}
```

Agents may say:

- "Your executed trades in this slice show..."
- "The playbook/setup record says..."
- "The coaching memory that matters here is..."
- "Reliability: N=..., tier=..., caveat=..."

Agents must not say:

- "You should take/skip this."
- "Your edge is green/red."
- "This is a good/bad trade."
- "Size up/down because memory says so."

## Phase 0 - Contract And Mechanical Prep

Goal: pin the contract before logic lands.

### Phase 0A - Architecture Doc

Create `docs/trader-memory/architecture.md` with:

- The three-bucket taxonomy above.
- Source ownership rules.
- Response envelope rules.
- Reliability rules.
- Intent budgets.
- Small-N suppression rules.
- Time-bucket casing convention.
- `TraderContextIntent` and legacy `MemoryBriefQuery.intent` coexistence.
- Pattern ID conventions.
- `unclassified` setup convention.
- Account scoping behavior.
- Trade ordinal, post-loss, scratch, and open-trade scoping.
- Ranking tie-break policy.
- Cost budget.
- Module layout.
- Risk-coach memory/sizing guardrail.
- Dirty-write audit scope.
- Capsule path resolution.
- Known debt around stale insight evidence pointing to deactivated pattern IDs.

### Reliability

Use one shared helper for sample-size reliability.

Current research code already has reliability-tier logic. Promote or move it so memory and research use the same source:

```rust
pub enum ReliabilityTier {
    Insufficient,
    Directional,
    Reportable,
}

pub fn reliability_tier(sample_size: usize) -> ReliabilityTier {
    match sample_size {
        0..=19 => ReliabilityTier::Insufficient,
        20..=29 => ReliabilityTier::Directional,
        _ => ReliabilityTier::Reportable,
    }
}
```

Serialize tier strings in lower camel case:

- `insufficient`
- `directional`
- `reportable`

Add a test that freezes these strings.

### Casing And Compatibility Conventions

Persisted memory scope values keep existing conventions. Output envelopes may normalize values for agent readability.

Rules:

- Persisted `timeBucket` values in `behavioral_patterns.scope_json` stay snake_case: `rth_open`, `rth_midday`, `globex_asia`, etc.
- `TraderContextFit.currentContext.timeBucket` emits lower camel case: `rthOpen`, `rthMidday`, `globexAsia`, etc.
- Add a small output-boundary helper such as `time_bucket_to_camel(&str) -> &str`.
- Do not migrate existing pattern rows or change `scope_value` string matching.
- `currentContext` includes `tradingDay` from the same 6 PM ET roll key used by session context tooling.

`TraderContextIntent` coexists with `MemoryBriefQuery.intent`:

- New `get_trader_context_fit` uses the typed enum.
- Existing `get_memory_brief` keeps its free-form string for backward compatibility.
- Add `impl FromStr for TraderContextIntent` so future tools can opt into the same vocabulary without breaking old callers.

Setup conventions:

- Trades without `setup_id` continue to use `"unclassified"`.
- New intersected slices must use `"unclassified"` rather than `null`, empty string, or `"unknown"`.

Account scoping:

- `TradeRecord.trade_account` is optional.
- Phase 1 default is `accountScope = "allAccounts"` so existing data remains usable.
- `TraderContextFitQuery` supports optional `trade_account`; when provided, execution memory filters to that account and `currentContext.accountScope` reports the account.
- When aggregating across all accounts, add a caveat if more than one non-null `trade_account` appears in the sample.
- Do not silently mix accounts without surfacing the scope.

Pattern ID conventions:

- `win_rate_by_setup_time_bucket:{setup_id}:{bucket}`
- `avg_r_by_setup_day_type:{setup_id}:{day_type}`
- `post_loss_after_one:global`
- `post_loss_after_one:setup:{setup_id}`
- `post_loss_after_two_plus:global`
- `post_loss_after_two_plus:setup:{setup_id}`

These IDs must be stable across the deactivate-then-upsert refresh path in `detect_behavioral_patterns`.

Do not add a separate `post_loss_by_setup:{setup_id}` pattern in Phase 1. It overlaps the after-one and after-two-plus setup-scoped rows and would make ranking ambiguous. If a combined post-loss-by-setup view becomes useful later, derive it from the two explicit states or define it as a new aggregate with a separate metric schema.

### Intent Budgets

Budgets must be enforced in Rust, not only in agent prompts.

```rust
pub enum TraderContextIntent {
    SessionStart,
    SetupCheck,
    TradeTaken,
    TradeClosed,
    SessionReview,
}
```

Budget table:

| Intent | Execution facts | Opportunity facts | Coaching reminders | Notes |
|---|---:|---:|---:|---|
| `sessionStart` | 3 | 1 capability pointer | 5 | Compact carry-forward only |
| `setupCheck` | 3 | 2 | 3 | Tightest real-time budget |
| `tradeTaken` | 2 | 1 | 2 | Focus on active risk/behavior |
| `tradeClosed` | 3 | 1 | 3 | Same-flow writes may require refresh |
| `sessionReview` | 12 | 6 | 12 | Review mode can expand |

### Small-N Suppression

Memory should not create noise by quoting tiny samples.

Rules:

- `n == 0`: omit the slice from `matchingSlices`.
- `1 <= n < 3`: allow only `n`, filters, source, and `reliabilityTier: insufficient`; suppress `winRate`, `avgR`, `medianR`, and similar numeric claims.
- `3 <= n < 20`: emit numeric fields only with `reliabilityTier: insufficient` and caveats.
- `20 <= n < 30`: emit numeric fields with `reliabilityTier: directional`.
- `n >= 30`: emit numeric fields with `reliabilityTier: reportable`.
- Cap `matchingSlices` after ranking even when more slices qualify.

### Trade Ordinal And Post-Loss Scope

Lock the semantics so future implementations do not drift:

- `tradeOrdinalInSession` means first, second, or third-plus trade within the current session.
- Historical ordinal patterns are also within-session.
- `postLossStateInSession` means the immediately prior resolved trade in the same session was a loss, or there are two-plus consecutive same-session losses.
- A zero-R scratch does not count as a loss. In Phase 1 it resets the consecutive-loss count to neutral rather than extending the loss streak.
- An open trade is not resolved evidence. If the current session has an open trade and no later closed result, report `postLossStateInSession` from the latest closed trade and include an `openTradePresent` flag rather than treating the open trade as a win/loss/scratch.
- If there are no prior resolved trades in the session, `postLossStateInSession` is `null`, not `false`.
- Do not compute global rolling post-loss state in Phase 1.

### Ranking Policy

Ranking should prefer specificity only when the more-specific slice has enough sample size to be usable.

Rules:

- A more-specific slice such as `setup x dayType` outranks a broader `setup` slice only when the specific slice is at least `directional`.
- If the specific slice is `insufficient`, reliability takes precedence and the broader reportable/directional slice should remain visible.
- `strongestPositiveEvidence` and `strongestCautionEvidence` prefer reportable evidence. Directional evidence may appear when it is the best scoped match, but must carry the standard caveat.
- Insufficient evidence can appear in `matchingSlices` for transparency, but should not lead positive/caution lists unless no higher-tier evidence exists.
- When the active-pattern load cap is hit, truncate by reliability tier first, then scope specificity, then effect size, and include `provenance.truncationWarning`.

### Cost Budget

The Phase 1 read path should query small memory tables and current-session trades, not scan all historical trades.

Target:

- `get_trader_context_fit` should be effectively O(number of active behavioral patterns + current session trades) in normal use.
- If any path becomes O(N closed trades), it is acceptable only during dirty refresh, not every MCP read.
- If trade count grows beyond roughly 10K and refresh time becomes visible, materialize more specific pattern slices or batch refresh by session.
- Load active patterns with an explicit cap large enough for intersected slices, initially `limit=Some(300)`.
- Expected active-pattern ceiling after Phase 1 is roughly 200 rows: setup x time, setup x day type, post-loss, and existing broad pattern rows.

### Module Layout

Preferred path:

1. Mechanically rename `src/memory.rs` to `src/memory/mod.rs`.
2. Add `src/memory/trader_context.rs`.
3. Later add `src/memory/digest.rs`.

Do the rename as a no-logic-change step and run tests. If the rename creates avoidable friction, temporary fallback is `src/memory_context.rs`, but the target architecture is the module split.

### Planned R Capture

Capture planned R-at-entry before the analytics depend on it.

Add a migration that extends `trades`:

```sql
ALTER TABLE trades ADD COLUMN planned_r_points_at_entry REAL NULL;
ALTER TABLE trades ADD COLUMN planned_r_dollars_at_entry REAL NULL;
```

Capture these fields on new trade writes/imports when available:

- `record_trade_result`
- `import_trade_fills`
- any future trade-open/write path that has risk config in scope

Rules:

- Use the risk configuration as it existed at the time of the write.
- Do not backfill historical rows from current config.
- Phase 1 read path may still return `riskDeviation.available = false` until analytics are implemented.
- This is data plumbing, not a permission to use memory for sizing.

Reason:

- The historical configured R value cannot be recovered later if account/risk settings change.
- Capturing the fields now makes future risk-deviation analytics truthful without changing Phase 1 coaching behavior.

### Documentation Drift

When the implementation lands, update tool-count references in `CLAUDE.md`, `AGENT.md`, and this plan to the actual MCP tool count. Tool-count drift is cosmetic, but it makes architecture docs less trustworthy.

### Dirty-Write Audit

Before Phase 1 relies on dirty-state freshness, enumerate every write path that can affect memory and verify it calls `mark_memory_dirty` or documents why it does not.

Audit at least:

- `start_trading_session`
- `upsert_trade_entry`
- `close_trade_entry`
- `record_trade_result`
- `review_trade_entry`
- `import_trade_fills`
- session-close finalization
- session-summary updates
- any future trade write/import path introduced during this work

Expected result:

- Trade writes dirty behavioral patterns.
- Review/insight writes dirty the relevant memory lifecycle.
- Session-summary writes dirty patterns when day type/profile/balance context can change execution slices.
- Any intentionally skipped write path has a doc comment explaining why it cannot affect ranked memory.

## Phase 1 - Execution Memory And Context Fit

Goal: ship trader memory as a retrieval/ranking/shaping layer over refreshed execution patterns, plus live-only risk context.

Phase 1 has five implementation pieces:

1. Add planned R-at-entry capture.
2. Add a denormalized trade/session context query.
3. Extend `detect_behavioral_patterns`.
4. Add `trader_context` adapter.
5. Expose `get_trader_context_fit`.

### 1. Planned R-At-Entry Capture

This is implemented before analytics consume it.

Write path requirements:

- Add nullable `planned_r_points_at_entry` and `planned_r_dollars_at_entry` to `TradeRecord`.
- Populate them when `record_trade_result` can resolve the then-current `RiskConfigRecord`.
- Populate them during `import_trade_fills` when the import path can safely associate fills with a risk config snapshot; otherwise leave null.
- Tests should prove older rows with null fields still deserialize.

Read path requirements:

- `riskDeviation.available` stays false unless both planned-R fields and the rest of the required trade fields are present.
- Do not derive missing planned-R fields from current config.

### 2. Trade With Session Context Query

Add a DB helper such as:

```rust
pub struct TradeWithSessionContext {
    pub trade: TradeRecord,
    pub session_date: Option<String>,
    pub session_type: Option<String>,
    pub session_segment: Option<String>,
    pub trade_account: Option<String>,
    pub day_type: Option<String>,
    pub profile_shape: Option<String>,
    pub balance_state: Option<String>,
    pub contract_symbol: Option<String>,
    pub root_symbol: Option<String>,
}

pub fn list_closed_trades_with_session_context_for_pattern_refresh(
    &self,
    limit_sessions: Option<usize>,
) -> Result<Vec<TradeWithSessionContext>, DbError>
```

Use a single query path joining:

- `trades`
- `sessions`
- `session_summaries`

The helper should:

- Be used by `detect_behavioral_patterns`, not by normal MCP read paths.
- Return only trades with `exit_time IS NOT NULL` or `result_r IS NOT NULL` for outcome slices.
- Preserve trades with review metadata where useful for discipline slices.
- Keep joins tolerant when a session summary is missing.
- Use the same session-date/session-type logic as existing memory code.
- Support optional account filtering later without changing row shape.

This makes the new intersected pattern tests easier and avoids expanding the current lookup-by-date approach.

Add a separate helper for live risk context, for example:

```rust
pub fn list_recent_session_trades(
    &self,
    session_id: &str,
    limit: usize,
) -> Result<Vec<TradeRecord>, DbError>
```

This helper is the one the context-fit adapter should use to compute current-session ordinal/post-loss state.

### 3. Extend `detect_behavioral_patterns`

`detect_behavioral_patterns` remains the owner of execution aggregation. Add missing slices there.

New pattern types:

- `win_rate_by_setup_time_bucket`
- `avg_r_by_setup_day_type`
- `post_loss_after_one`
- `post_loss_after_two_plus`

Keep existing pattern types:

- `win_rate_by_setup`
- `win_rate_by_time_bucket`
- `rules_broken_after_loss`
- `planned_vs_unplanned_by_session_segment`
- `emotional_state_by_outcome`
- `gross_points_avg_r_by_day_type`
- `trade_count_position_vs_outcome`
- `mistake_tag_frequency`

Use stable metric JSON schemas. Example:

```json
{
  "resolved": 24,
  "wins": 11,
  "losses": 10,
  "scratches": 3,
  "winRate": 0.4583,
  "avgR": -0.06,
  "totalR": -1.44,
  "plannedRate": 0.79,
  "rulesFollowedRate": 0.83
}
```

Use stable scope JSON schemas. Persisted `timeBucket` values stay snake_case. Examples:

```json
{ "setupId": "tpl_dnva_retest", "timeBucket": "rth_open" }
```

```json
{ "setupId": "tpl_dnva_retest", "dayType": "DoubleDistribution" }
```

```json
{ "postLossState": "afterTwoPlusLosses", "setupId": "tpl_dnva_retest" }
```

Important:

- Do not parse descriptions in the context-fit adapter.
- The adapter should parse known `pattern_type`, `metric`, and `scope` shapes.
- Retain human-readable `description` for existing memory brief output.
- Use the pattern ID conventions from Phase 0 exactly.
- Use `"unclassified"` for missing setup IDs.

### 4. Context-Fit Adapter

Add `src/memory/trader_context.rs` after the module split.

This module owns:

- Query params and response structs.
- `TraderContextIntent`.
- Budget constants.
- Pattern-to-evidence parsing.
- Ranking into positive/caution/matching slices.
- Small-N suppression at output boundary.
- Live-only current-session risk context.

It does not own:

- Historical execution aggregation.
- Setup/backtest research.
- LLM prose.
- Markdown rendering.

Public entry point:

```rust
pub fn build_trader_context_fit(
    db: &Database,
    query: TraderContextFitQuery,
) -> Result<TraderContextFit, MemoryError>
```

Query shape:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TraderContextFitQuery {
    pub intent: TraderContextIntent,
    pub setup_id: Option<String>,
    pub session_id: Option<String>,
    pub trade_account: Option<String>,
    pub trading_day: Option<String>,
    pub timestamp_ms: Option<f64>,
    pub session_type: Option<String>,
    pub session_segment: Option<String>,
    pub time_bucket: Option<String>,
    pub day_type: Option<String>,
    pub profile_shape: Option<String>,
    pub balance_state: Option<String>,
    pub include_opportunity: Option<bool>,
    pub include_coaching_memory: Option<bool>,
}
```

Use explicit context when provided. Infer only safe fields:

- `accountScope` from `trade_account` when provided, otherwise `allAccounts`.
- `trading_day` from timestamp using the existing 6 PM ET trading-day logic.
- `time_bucket` from timestamp.
- session context from a resolved session.
- current/latest summary fields only when session identity is clear.
- `setup_id` only when supplied by caller or active setup context.

Do not invent `regime` until regime is a first-class field.

### Ranking

Load active `behavioral_patterns` and rank against current context using existing context matching. Then convert known execution pattern types into typed evidence:

- `matchingSlices`: patterns with scope overlap.
- `strongestPositiveEvidence`: positive `avgR`, high rule adherence, helpful history.
- `strongestCautionEvidence`: negative `avgR`, high post-loss rule breaks, high mistake frequency, weak reliability caveats.

Load cap:

- Use `limit=Some(300)` when loading active behavioral patterns for `trader_context`.
- Apply ranking and intent budgets after loading.
- Do not reuse the smaller `get_memory_brief` default pattern limit for this adapter; intersected slices can sit below broad rows before ranking.
- If the load cap is hit, prefer reportable over directional over insufficient rows before truncating, then apply scope specificity and effect size. Emit `provenance.truncationWarning` with the cap and loaded count.

Ranking priorities:

1. Scope specificity: setup + time/day beats setup-only, which beats broad time/day.
2. Reliability tier.
3. Absolute effect size.
4. Recency if available.
5. Existing salience/confidence as a tie-breaker.

Apply the intent budget after ranking.

### Traceable Provenance

Every surfaced evidence item should carry enough identity to debug why it appeared.

Add:

```json
{
  "evidenceIds": {
    "patterns": ["win_rate_by_setup_time_bucket:tpl_dnva_retest:rth_open"],
    "insights": ["insight-uuid"],
    "followups": []
  },
  "evidenceTimestampsMs": {
    "patternsRefreshedAtMs": 1770000000000,
    "insightsLastUpdatedMs": 1770000000000
  }
}
```

Evidence IDs belong both at the top-level provenance summary and, where useful, on individual evidence rows.

Each execution slice should also include a `queryHint`, for example:

```json
{
  "queryHint": {
    "setupId": "tpl_dnva_retest",
    "timeBucket": "rth_open",
    "tradeAccount": "allAccounts"
  }
}
```

This makes it possible to debug or re-query the underlying trade population without reverse-engineering the pattern ID.

### Live-Only Risk Context

Compute these from current-session trades directly:

- `tradeOrdinalInSession`
- `postLossStateInSession`
- latest planned/rules-followed state when relevant
- current open-trade linkage if available

Do not aggregate all history here.

### Risk-Deviation Availability

Do not calculate actual R risked from `result_r` or current config.

Risk deviation is available only when all are present at the historical trade entry:

- entry price
- stop price
- size
- instrument/point value
- configured R points or dollars at entry time

Phase 0 captures planned R-at-entry for new rows. Older rows may still be missing it. If configured R-at-entry is not persisted for a row, return:

```json
{
  "available": false,
  "availability": {
    "kind": "preCapture",
    "reason": "Risk deviation cannot be evaluated for this row; do not infer risk compliance from this absence."
  }
}
```

Availability kinds:

- `available`
- `preCapture`
- `missingFields`
- `configUnavailable`

Agents must treat any non-`available` kind as "not evaluated", not as "risk compliant."

Future subfeature:

- Enable risk-deviation analytics once enough new trades have planned-R fields populated.

### Maintenance

Do not invent a second refresh system.

`get_trader_context_fit` should:

- Read `memory_maintenance_state`.
- Return `maintenance.refreshSuggested`.
- Optionally perform one bounded refresh only if we explicitly decide to mirror `get_pre_session_briefing`; otherwise let orchestrator call `refresh_memory_state` first when needed.

Preferred Phase 1 behavior:

- Read-only by default.
- If dirty, return useful stale data plus `maintenance.refreshSuggested: true`.
- Agent routing calls `refresh_memory_state` first when same-flow writes occurred or freshness matters.
- `maintenance.refreshSuggested: true` is not the same as missing data. Agents may cite stale-but-present evidence with explicit freshness and small-N caveats.

### Relationship To `get_pre_session_briefing`

Keep `get_pre_session_briefing` as the session-start envelope for account, risk, rollover, and carry-forward context.

Change its memory portion to delegate to `build_trader_context_fit(intent=sessionStart)` once the builder exists. This prevents adjacent session-start tool calls from ranking the same memory differently.

Rules:

- `get_pre_session_briefing` keeps its current public shape where possible.
- Account/risk/rollover assembly stays in the existing tool.
- Memory ranking, execution evidence, coaching reminders, and provenance come from the same context-fit builder used by `get_trader_context_fit`.
- Backward-compatible fields can be populated from the context-fit envelope during a transition period.
- Add a regression test proving a fixed seeded DB preserves expected `get_memory_brief` top-N ordering, or documents the intentional ranking change, after the new pattern types are added.

### 5. MCP Tool

Add one MCP tool:

- `get_trader_context_fit`

Implementation location:

- It is acceptable to register it in `src/bin/the-desk-mcp.rs` first.
- Do not block the feature on MCP modularization.
- If the memory module split is clean, place param structs in `src/mcp/memory.rs` later.

Tool response:

- JSON only.
- No coaching prose generated in Rust.
- Rust-enforced budgets by `TraderContextIntent`.
- `opportunityFit.contextFrameAnalog.available = true` capability pointer in Phase 1.
- Traceable `evidenceIds` and `evidenceTimestampsMs`.
- Maintenance state included.

### Agent Wiring

Update `agents/risk-coach.md` first.

Add this as a top-level risk rule:

> Memory reports context. Memory never adjusts position size by itself. Sizing can only come from configured risk rules, hard circuit breakers, and explicit trader confirmation.

Then wire:

- Pre-trade checklist calls `get_trader_context_fit` after hard risk limits.
- The result may inform caution phrasing.
- It cannot override circuit breakers.
- It cannot recommend sizing changes.
- Pattern memory is not a Kelly input.

Update `agents/orchestrator.md`:

- Setup Check calls `get_trader_context_fit` in parallel with market/setup/risk tools.
- Session Start calls it with `intent=sessionStart` and compact budget.
- Trade Taken / Trade Closed flows refresh memory first when same-flow writes occurred.
- Synthesis must separate "your execution" from "playbook/setup record."

Update `agents/performance-analyst.md`:

- Use `get_trader_context_fit` for trader behavior.
- Keep `get_setup_performance_matrix` and `query_signal_outcome_*` for setup opportunity.
- Report conflicts explicitly: "setup signal record differs from executed-trade record."

Land the risk-coach guardrail as a tiny Phase 0 prompt change before the new MCP tool exists. This anchors behavior before new memory fields can tempt sizing language.

### Phase 1 Tests

DB helper tests:

- Returns closed trades with session summary context.
- Tolerates missing session summaries.
- Keeps session type/date scoping correct.
- Does not get called by the normal MCP read path.

Behavioral pattern tests:

- Emits `setup x time_bucket` with expected metric/scope JSON.
- Emits `setup x day_type` with expected metric/scope JSON.
- Emits post-loss-after-1 and post-loss-after-2-plus separately.
- Does not count trades without `result_r` toward win rate.
- Preserves existing pattern outputs.
- Preserves expected `get_memory_brief` ranking for existing broad patterns unless the fixture documents an intentional change.

Context adapter tests:

- Empty DB returns missing-data caveats.
- `tradingDay` is emitted when timestamp/session context can resolve it.
- Trading-day boundary cases: 5:55 PM ET remains prior trading day, 6:01 PM ET rolls forward, Sunday 5:59 PM behavior, and weekday close behavior.
- Persisted snake_case time buckets are converted to output lower camel case without migrating stored scope values.
- Account scoping defaults to `allAccounts`, filters when `trade_account` is supplied, and caveats when multiple accounts are pooled.
- Dirty maintenance is surfaced.
- Small-N suppression nulls numeric stats below floor.
- Intent budgets are enforced server-side.
- Load-cap truncation emits `provenance.truncationWarning`.
- Positive/caution ranking is deterministic.
- More-specific insufficient slices do not outrank broader reportable/directional slices.
- Live `tradeOrdinalInSession` is within-session.
- Live `postLossStateInSession` is within-session.
- Scratch trades reset the same-session loss streak to neutral.
- Open trades are flagged as open and not treated as resolved evidence.
- Risk-deviation is unavailable when config-at-entry is missing.
- Evidence IDs and timestamps are included for surfaced patterns/insights/followups.
- Insights with inactive pattern references are surfaced with `stalePatternEvidence: true`.

MCP tests:

- Seed sessions, summaries, trades, behavioral patterns, insights, and followups.
- Call `get_trader_context_fit`.
- Assert execution/opportunity/coaching/provenance/maintenance separation.

Snapshot test:

- Serialize `TraderContextFit` for a fixed seeded DB.
- Freeze camelCase keys, tier strings, section names, and budgeted array lengths.
- Use hand-rolled `serde_json::Value` equality against a checked-in fixture under `tests/fixtures/trader_context_fit/`.
- Do not add `insta` for this; the envelope is small enough to keep as a normal JSON fixture.

## Phase 2 - Opportunity Overlay

Goal: enrich `opportunityFit` without blending it into executed-trade memory.

Use existing capabilities:

- `get_signal_performance`
- `get_setup_performance_matrix`
- `query_signal_outcome_distribution`
- `query_signal_outcome_conditional`
- `query_signal_outcome_excursions`
- `get_context_frame`

The context-fit tool may include compact opportunity facts only when requested and within budget.

Example:

```json
{
  "setupOutcome": {
    "setupId": "tpl_dnva_retest",
    "n": 32,
    "reliabilityTier": "reportable",
    "avgR": 0.18,
    "medianR": 0.05,
    "source": "signal_outcomes"
  },
  "contextFrameAnalog": {
    "available": true,
    "source": "get_context_frame",
    "sampleSize": 24,
    "reliabilityTier": "directional",
    "caveats": ["Analog context is market precedent, not executed-trade memory."]
  },
  "executionConflict": {
    "detected": true,
    "executionAvgR": -0.30,
    "executionN": 14,
    "opportunityAvgR": 0.18,
    "opportunityN": 32,
    "interpretation": "Setup signal historically positive; trader's executed result negative."
  },
  "caveats": [
    "Opportunity stats describe setup signals, not trader execution."
  ]
}
```

`executionConflict` is deterministic Rust output, not prompt-side inference. It should be emitted only when both execution and opportunity samples pass the configured floor for comparison, with separate reliability tiers preserved on each side.

Do not add `src/memory/analog.rs` or `session_fingerprints` here. `get_context_frame` already has weighted analogs.

## Phase 3 - Markdown Memory Capsules

Goal: provide readable memory without turning every agent turn into a context dump.

SQLite remains source of truth. Markdown is a derived projection.

### Generated Capsules

Write local files under the same user config root used elsewhere by The Desk. On Windows this resolves via `dirs::home_dir()` to `%USERPROFILE%\.the-desk\memory`; do not raw-concatenate a literal `~` path.

- `session-start.md`
- `last-session.md`
- `this-week.md`
- `open-followups.md`

Budgets:

- `session-start.md`: max 40 lines.
- `last-session.md`: max 80 lines.
- `this-week.md`: max 120 lines.
- `open-followups.md`: max 40 lines.

Each capsule includes:

- generated timestamp
- source tables/tools
- freshness status
- sample-size caveats
- "generated from SQLite; do not edit" header

### Freshness Policy

Markdown that is stale is worse than no markdown.

Rules:

- Agents may read `session-start.md` only during session start.
- A capsule is stale when `capsule_generated_at_ms < memory_maintenance.dirty_since_ms`.
- A 12-hour max age is only a clock-drift/session-safety upper bound, not the primary freshness signal.
- If stale, call `regenerate_memory_capsules` or fall back to `get_trader_context_fit`.
- Session review can regenerate `last-session.md` and `open-followups.md`.
- Specialist agents should not auto-read capsules on every turn.

### Trigger Wiring

Reuse existing dirty paths.

Regenerate or mark capsule-refresh-needed after:

- `record_trade_result`
- `import_trade_fills`
- `review_trade_entry`
- session-close finalization
- explicit trader request

Do not make markdown freshness independent from SQLite maintenance state.

Insight evidence liveness:

- `agent_insights.evidence` can cite pattern IDs that later become inactive after metric/scope schema changes.
- In Phase 1, when assembling `coachingMemory.insights`, mark any insight with inactive pattern references as `stalePatternEvidence: true`.
- Full insight lifecycle invalidation can remain later work, but the context-fit envelope should not surface these insights as fully fresh.

### Optional Curated Doctrine

Add committed doctrine only if the trader wants editable identity/rules text:

- `docs/trader-memory/README.md`
- `docs/trader-memory/identity.md`
- `docs/trader-memory/playbook-doctrine.md`
- `docs/trader-memory/lessons-learned.md`

Privacy:

- Committed doctrine may contain principles and rules.
- It must not contain private PnL, exact trade prices, account history, or imported fills.

### Renderer

Add `src/memory/digest.rs` only after Phase 1 proves useful.

Renderer inputs:

- `TraderContextFit`
- `MemoryBrief`
- session review context
- open followups

Renderer constraints:

- deterministic output
- no LLM calls
- no raw tick streams
- no advisory phrasing
- short by default

MCP tool:

- `regenerate_memory_capsules(scope?)`

Scopes:

- `sessionStart`
- `lastSession`
- `week`
- `followups`
- `all`

## Phase 4 - Context-Frame Extension Only If Needed

Goal: avoid duplicate analog infrastructure.

The repository already has a v1 analog layer in `get_context_frame` with weighted distance, top-K fallback, reliability tiers, and analog metadata. Use that first.

Only extend context-frame analogs if a concrete gap appears:

- Need analogs at a specific intraday timestamp rather than session-close summaries.
- Need event replay from matched context forward.
- Need setup-specific analogs with outcome windows.
- Need materialized summaries for large historical data.

Preferred extensions:

- Add richer fields to `ContextBuckets`.
- Add optional event replay to `get_context_frame`.
- Materialize bucket forward-outcome summaries in research infrastructure.
- Add golden JSON envelope tests for context-frame output.

Do not create:

- `src/memory/analog.rs`
- `session_fingerprints`
- `find_analog_sessions`
- `replay_analog_outcome`

unless `get_context_frame` cannot reasonably support the needed behavior.

## Implementation Order

0. Sit on the plan and resolve the required semantics in `docs/trader-memory/architecture.md`: post-loss IDs, ranking tie-breaks, typed risk-deviation availability, and account scoping.
1. Land the risk-coach guardrail prompt change as a tiny no-tool PR.
2. Phase 0 PR: architecture doc, mechanical `src/memory.rs` -> `src/memory/mod.rs` rename, shared `ReliabilityTier`, planned-R columns/capture, and dirty-write audit. Run `cargo test`.
3. Phase 1 PR1: add `db.list_closed_trades_with_session_context_for_pattern_refresh`, add `list_recent_session_trades`, extend `detect_behavioral_patterns` with intersected slices, and add pattern/ranking regression tests. No new MCP tool in this PR.
4. Phase 1 PR2: add `src/memory/trader_context.rs`, hand-written empty/non-empty JSON fixtures, `get_trader_context_fit`, `get_pre_session_briefing` memory-section delegation, and agent wiring.
5. Run targeted tests, then full `cargo test`.
6. Add Phase 2 opportunity details after Phase 1 output is stable in real use.
7. Defer Phase 3 markdown capsules until at least five real sessions of Phase 1 usage show they are needed.
8. Defer Phase 4 with a measurement loop: record `docs/trader-memory/measured-gaps.md` entries when `get_context_frame` falls short; revisit after three concrete entries.

## Acceptance Criteria

The first shippable version is done when:

- `get_trader_context_fit` returns useful results with zero markdown files present.
- Phase 0 architecture doc pins account scoping, post-loss/scratch/open-trade semantics, ranking tie-breaks, pattern IDs, and risk-deviation availability kinds.
- Dirty-write audit is complete and documented.
- Risk-coach guardrail is landed before the new memory tool is exposed.
- Execution evidence comes from `behavioral_patterns` generated from actual trades, plus live current-session lookups.
- No second full trade-stat aggregator exists on the MCP read path.
- Pattern refresh helper is not used by the normal MCP read path.
- Opportunity stats come only from setup/signal/context-frame historical data.
- Phase 2 conflict detection, when implemented, is deterministic Rust output rather than prompt inference.
- Coaching memory comes from existing insight/pattern/follow-up infrastructure.
- Insights with inactive pattern evidence are flagged.
- New trades capture planned R-at-entry when available, without backfilling old rows from current config.
- Every statistic includes `n`, reliability tier, filters/window, source, and caveats.
- Surfaced evidence includes traceable IDs and relevant timestamps.
- Surfaced slices include `queryHint`.
- Small-N numeric claims are suppressed according to the Phase 0 floor.
- Intent budgets are enforced in Rust.
- Load-cap truncation emits a warning instead of silently dropping rows.
- `get_pre_session_briefing` and `get_trader_context_fit(intent=sessionStart)` share the same memory ranking path.
- Agent prompts separate "your execution" from "the playbook/setup record."
- Risk coach states that memory never adjusts size by itself.
- No LLM calls are introduced in Rust.
- Hand-rolled response fixture snapshot test passes.
- `cargo test` passes.

## Why This Approach

This keeps the product goal: agents become trading partners that remember the trader.

It also avoids the main engineering failure mode: two memory systems returning different numbers from the same trade history. The existing behavioral-pattern refresh path should own execution aggregation. The new context-fit layer should make that memory easier to retrieve, rank, trust, and phrase in the moment.
