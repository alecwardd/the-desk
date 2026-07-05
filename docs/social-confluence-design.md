# Social Account Confluence — v1 Implementation Spec

**Status:** Proposed (no code yet)
**Date:** 2026-06-30
**Parent roadmap:** [social-intelligence-roadmap.md](social-intelligence-roadmap.md) (full vision, phases B–F, subagent learning model, external idea queue)
**Related:** [decision-log.md](decision-log.md) ADR-020 (Pending), CLAUDE.md layer rules, `AGENT.md` "Grounded Partnership"

> **Scope.** This document is the **Phase A** build spec only: curated watchlist, post cache, one MCP tool, background polling. Continual learning, external hypothesis ingestion, and research conditionals are specified in the roadmap — not here.

---

## Goal

Let the agent pull "what a short list of trusted traders are currently saying" as
**context** to compare against the trader's own market read, looking for confluence or
divergence. This is a research/context aid — **not** a signal source.

Source: the X MCP server (`https://api.x.com/mcp`, see https://docs.x.com/tools/mcp),
read-only endpoints (`Users: read user posts/timelines`, `Posts: fetch`).

Out of scope for v1: open-firehose market sentiment indexing, external idea queue, event logging for research conditionals (see roadmap Phases B–D).

---

## Hard constraints (from CLAUDE.md — do not violate)

1. **Layer 3 only.** Social data is network + (for sentiment) LLM-classified. It must
   not touch `pipelines/` (Layer 1) or `rules/` (Layer 2), which are pure, deterministic,
   network-free.
2. **Never fires a playbook alert.** Rule #3: every alert must trace to the trader's own
   playbook. "Twitter is bearish" traces to no rule. Social data is surfaced as context
   the agent reads, never as a rules-engine condition — unless the trader explicitly
   encodes a playbook rule that references a stored social field.
3. **No Claude API calls from Rust.** Rust fetches + stores raw posts. Sentiment/lean
   synthesis happens agent-side at conversation time. (A deterministic lexicon score in
   Rust is permissible as an optional add-on; nuanced classification is not.)
4. **Never block the main thread.** X API fetches run in a background task.
5. **Graceful degradation.** Feature-flagged; the rest of The Desk works unchanged when
   the X API is absent or the flag is off.
6. **Attribution gate.** Third-party trader opinions must stay attributed and grounded
   (`AGENT.md` "Grounded Partnership") and respect X's content-storage terms. Never
   "X says take the trade" — always "@handle posted [idea]; your data shows [stats]".

---

## Proposed shape

```
src/social/                 # NEW, isolated module (Layer 3 support)
  mod.rs                    # SocialConfig, types, feature flag
  x_client.rs               # X MCP/API read calls, background fetch task
  store.rs                  # cache + accessors over the social_posts table

src/db/mod.rs               # + social_posts table (cached posts, idempotent upsert)
src/bin/the-desk-mcp/tools/ # + get_account_confluence tool (read-only, with_read_db)
~/.the-desk/config.toml     # [social] watchlist, poll cadence, enabled flag, token ref
```

- **Watchlist** of handles lives in config (no hardcoded values per house rules).
- **Cache table** `social_posts` keyed by post id (idempotent), so reads are cheap and
  the agent isn't re-billed per question. Pruned like other tiers (see ADR-015).
- **MCP tool** `get_account_confluence` returns recent posts per watched account +
  metadata (handle, timestamp, engagement). The agent synthesizes the lean; the tool
  returns structured data only (consistent with "MCP tools return structured data only").
- **Polling** in a background task on a config cadence (default: every 30 min during RTH;
  configurable). Token/credential never committed — `.env` or keychain.

### Draft `social_posts` columns (v1)

| Column | Notes |
|--------|-------|
| `post_id` | Primary key (X post id) |
| `handle` | Author handle |
| `posted_at_ms` | Source timestamp |
| `text` | Raw post text |
| `fetched_at_ms` | Cache write time |
| `engagement_json` | Likes/reposts/replies if available |
| `in_reply_to_id` | Optional thread context |

Phase B may add `contribution_type` / tags per [roadmap confluence types](social-intelligence-roadmap.md#confluence-types-multi-reason-design).

---

## Agent integration (v1)

On setup check or explicit confluence request, orchestrator calls:

1. Baseline: `get_session_context`, `get_market_snapshot`, setup evaluation tools (unchanged)
2. **New:** `get_account_confluence` — recent watchlist posts
3. Synthesize: directional / structural / divergent framing per roadmap; attribute all third-party reads

Example compliant framing:

- *"Your playbook indicates OR5 acceptance long. Structure shows price above OR high with positive session delta. Three accounts on your watchlist also referenced acceptance above this zone in the last 4 hours; two others have not posted since the open."*

Example non-compliant framing (avoid):

- *"Twitter is bullish — take the long."*

---

## Cost model (drives the access decision — still open)

X is pay-per-use by default for new developers as of Feb 2026: ~$0.005/post read, 2M
reads/month cap, then Enterprise ($42k+/mo). Legacy Basic/Pro are closed to new signups.

- **This design (curated list):** ~12 accounts × recent posts × ~13 polls/day × ~21 days
  ≈ low tens of thousands of reads/month ≈ **~$150–250/mo**, controllable via cadence and
  cache. Read-only Bearer token is sufficient.
- The cache is the main cost lever: poll once, answer many agent questions from SQLite.

**Open decision (ADR-020):** read-only Bearer token vs OAuth 2.0, and whether cost
justifies live polling vs on-demand fetch. Recorded as Pending until the trader decides.

---

## Deferred (see roadmap)

| Item | Phase |
|------|-------|
| Open-firehose sentiment indicator | Shelved — Enterprise cost, low signal quality |
| Curated-list sentiment score | Optional v1 add-on; roadmap leaves trader choice open |
| `external_ideas` hypothesis queue | Phase B+ |
| Social × structure research conditionals | Phase C |
| Per-account calibration memory | Phase D |
| Post embeddings / RAG | Phase E |

Full-archive / keyword search over the open platform ("NQ", "ES", "SPX") returns
hundreds–thousands of posts per poll and can blow the 2M read cap quickly → Enterprise
territory. If sentiment is pursued, compute bull/bear/neutral **only over the curated watchlist**.

---

## Open questions for the trader

1. Watchlist contents (the short list of handles).
2. Access mode + budget ceiling (feeds ADR-020).
3. Poll cadence, and RTH-only vs 24h.
4. Should a curated-list sentiment score ship in v1, or confluence-context only first.

See also [roadmap open questions](social-intelligence-roadmap.md#open-questions-trader-decisions) for idea-queue and subagent routing decisions (post–v1).
