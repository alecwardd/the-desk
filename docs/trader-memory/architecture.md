# Trader Memory Architecture

Trader memory is a typed decision-support layer over existing deterministic data. It must not become a second aggregation system that can disagree with `get_memory_brief`.

## Source Models

### Opportunity

Opportunity describes what a setup signal or market context has done historically.

Sources:

- `signal_outcomes`
- `get_signal_performance`
- `get_setup_performance_matrix`
- `query_signal_outcome_*`
- `get_context_frame`

Opportunity stats are not executed-trade stats.

### Execution

Execution describes how the trader actually behaved and performed.

Sources:

- `trades`
- `sessions`
- `session_summaries`
- trade review fields
- `behavioral_patterns`

Execution aggregation belongs in `detect_behavioral_patterns`. `get_trader_context_fit` retrieves, ranks, shapes, and budgets that memory; it must not rescan all historical trades on every MCP read.

### Coaching

Coaching memory is language and follow-through.

Sources:

- `agent_insights`
- `behavioral_patterns`
- `memory_followups`
- journal entries
- optional curated doctrine files

Coaching reminders are ranked and scoped. Agents must not dump all notes into every turn.

## Reliability

Use one reliability helper everywhere:

- `N < 20`: `insufficient`
- `20 <= N < 30`: `directional`
- `N >= 30`: `reportable`

Small-N suppression:

- `n == 0`: omit the slice.
- `1 <= n < 3`: emit only `n`, source, filters, and reliability; suppress numeric claims.
- `3 <= n < 20`: numeric fields may appear only with insufficient caveats.

## Intent Budgets

Budgets are enforced in Rust by `TraderContextIntent`.

| Intent | Execution facts | Opportunity facts | Coaching reminders |
|---|---:|---:|---:|
| `sessionStart` | 3 | 1 capability pointer | 5 |
| `setupCheck` | 3 | 2 | 3 |
| `tradeTaken` | 2 | 1 | 2 |
| `tradeClosed` | 3 | 1 | 3 |
| `sessionReview` | 12 | 6 | 12 |

## Scope Conventions

- Persisted `timeBucket` values in `behavioral_patterns.scope_json` stay snake_case, e.g. `rth_open`.
- `TraderContextFit.currentContext.timeBucket` emits lower camel case, e.g. `rthOpen`.
- Missing `setup_id` is always `"unclassified"`.
- `currentContext.tradingDay` uses the 6 PM ET roll key.
- Phase 1 defaults to `accountScope = "allAccounts"`.
- When `trade_account` is supplied, execution memory filters to that account.
- When multiple non-null accounts are pooled, output must include a caveat.

Pattern IDs:

- `win_rate_by_setup_time_bucket:{setup_id}:{bucket}`
- `avg_r_by_setup_day_type:{setup_id}:{day_type}`
- `post_loss_after_one:global`
- `post_loss_after_one:setup:{setup_id}`
- `post_loss_after_two_plus:global`
- `post_loss_after_two_plus:setup:{setup_id}`

Do not add a separate `post_loss_by_setup:{setup_id}` pattern in Phase 1.

## Post-Loss Semantics

- Trade ordinal is within the current session.
- Historical ordinal patterns are also within-session.
- A zero-R scratch resets the consecutive-loss count to neutral.
- Open trades are not resolved evidence. Surface `openTradePresent` instead of treating an open trade as a win, loss, or scratch.
- If there are no prior resolved trades in the session, `postLossStateInSession` is `null`.

## Ranking

Specificity wins only when the specific slice has enough sample size to be useful.

- `setup x dayType` can outrank `setup` only if the specific slice is at least directional.
- Broader reportable/directional evidence outranks more-specific insufficient evidence.
- Positive/caution lists prefer reportable evidence; directional evidence is allowed with caveats.
- If the active-pattern load cap is hit, truncate by reliability tier, then scope specificity, then effect size, and emit `provenance.truncationWarning`.

## Risk Guardrail

Memory reports context. Memory never adjusts position size by itself.

Pattern memory is not a Kelly input. Sizing can only come from configured risk rules, hard circuit breakers, existing risk tooling, and explicit trader confirmation.

If `riskDeviation.available` is false, agents must treat that as not evaluated. They must not infer risk compliance from absence.

## Dirty State

Before relying on ranked memory, write paths that affect memory must call `mark_memory_dirty` or document why they do not.

Audit scope:

- session start/end
- trade upsert/close/result/import
- trade review
- session-summary updates/finalization
- future trade write/import paths

## Capsules

SQLite is source of truth. Markdown capsules are derived projections.

Capsules live under the same user config root as `~/.the-desk/config.toml` using the repo's home-dir resolution, not raw `~` concatenation.

A capsule is stale when `capsule_generated_at_ms < memory_maintenance.dirty_since_ms`; 12 hours is only a safety max-age.

## Known Debt

`agent_insights.evidence` can cite pattern IDs that later become inactive. Phase 1 should mark these surfaced insights with `stalePatternEvidence: true`; full lifecycle invalidation can follow later.
