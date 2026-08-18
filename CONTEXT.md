# CONTEXT.md — Desk vocabulary (SIL)

Canonical glossary for The Desk / Sierra Intelligence Layer. Use these terms
exactly in code, docs, ADRs, and agent copy. Do not invent synonyms.

| Term | Meaning |
|------|---------|
| **Trust Level** | Capability property of a tool on the MCP surface. Read/query kernel operators run at **L0** and cannot carry mutation or order authority. |
| **Trust Ceiling** | System-wide maximum Trust Level. Stays at **L3** (drafts proposals; the human executes). Raising it requires a new ADR. |
| **Base Detector** | Stateful Rust detector (absorption, pinch, …). Math stays in reviewed Rust; schema, provenance, and promotion (`candidate` → `shadow` → `active`, human-gated) are Feature Registry–governed. Discovery rides `search_catalog` / catalog descriptors when `[sil].catalog_discovery` is on. The write verb is `feature_registry`. |
| **Derived Feature** | Declared Feature-IR program over catalog fields using the five funded Operator Families. Registered via `feature_registry` (`kind=derivedFeature`); discovery rides `search_catalog`. |
| **Feature-IR** | Typed intermediate representation for Derived Features. Not Turing-complete; live-shadow and historical path labels share one evaluator over Journal Frames (`clock_ms <= asOf`). Evaluation is capped at 8192 frames (equal to the live pending Journal Frame buffer). Session-percentile `n` and dwell fail closed when that bound would drop in-session frames. Historical loads use a newest-first `LIMIT cap+1` sentinel — never an unbounded `list_journal_frames` scan. SIL-M5c codegen emits five kernel artifacts from one accepted (`active`) descriptor: runtime field (`get_state`), Journal Frame payload key, query dimension, catalog-field rules binding, agent schema. |
| **Operator Family** | One of the five funded Feature-IR operator classes (Cross-symbol references, Session-distribution percentiles, Dwell / time-since-predicate, Event sequences, Historical baselines). A new family requires a registry change proposal. Surface lookup / interpolation is unfunded. |
| **Journal Frame** | 1 Hz persisted market-state frame on the MarketRouter clock (NQ+ES). Compute/publish may stay at ~250 ms (ADR-019); only 1 Hz frames are persisted. A root is recorded for a second only when that root printed in that second (session scope stays per-lane). `get_state(as_of=…)` is served from Journal Frames (provenance source = Journal). Transition event rows join via `(journal_frame_second, root_symbol)` where `journal_frame_second = floor(event.timestamp_ms / 1000)` — this pair is the event **frame_ref**. The **hot window** is SQLite; **cold** dumps are session-partitioned JSONL.zst (`trading_day` / `session_type` / `root`), rebuildable from `.scid`/`.depth`, and do not require DuckDB. Research operators may set `store=cold` without changing L0 contracts. |
| **Event** | Formalized detector row on `get_events`: lifecycle (`open` → `updated` → `resolved`\|`expired`), severity, occurrence identity, **dedup identity** (stable condition key), **frame_ref**, and **capsuleRef** on DOM-family rows. Occurrence rows persist in SQLite for research counts; the coaching view (`get_events` / `get_attention_inbox`) is latest-per-dedup so a persistent condition is not a stream of new Events. Trust Level **L0**. |
| **frame_ref** | Join from an Event to the Journal Frame that produced it: `(journal_frame_second, root_symbol)`. Always present on `get_events` rows (nulls when unknown — never silently omitted). The serialized `get_events` field is `frameRef`. |
| **DOM-family** | Event types Capsules key off: `stop_run`, `iceberg_reload`, `pull_intent`, `book_velocity_regime_shift` (registry may extend). Capsules are mandatory for these types. |
| **Capsule** | High-resolution dump around a salient Event (~30s before → ~60s after) from a rolling ~250 ms in-memory ring. Mandatory for DOM-family events. Joinable to the triggering Event (`trigger_identity_id`) and surrounding Journal Frames (`[start_frame_second, end_frame_second] × root_symbol`). Completeness is `pending` / `complete` / `incomplete` (incomplete + degraded if the session/feed ends before the after-window). **Not** trader-memory markdown files. The forever journal stays 1 Hz — the ring is never persisted as a 250 ms table. |
| **Episode Query** | Conjunctive historical query over Desk Catalog fields across NQ+ES Journal Frames / events, with tick-driven forward returns / MFE/MAE (`query_episodes`). Missing detector or vendor fields fail closed (empty/degraded + provenance) — they are never invented. SIL-M3e measured DuckDB vs SQLite dense columns vs JSON blobs and **deferred DuckDB** (Path B p95 ≪ 2–3 s over ~2 weeks of 1 Hz frames). Harness: `the-desk-eq-bench` (not an MCP tool). |
| **Positioning** | Dealer/options domain with four record kinds: position-grid, by-strike, Slice, Levels-Only Record. Manual Levels-Only Records are written via the `positioning_entry` workflow verb (same schema a later capture adapter will use). No live Vs3dProvider yet. |
| **Slice** | Price-indexed greek values plus Desk-derived levels at capture time (Positioning record kind). |
| **Levels-Only Record** | First-class manual Positioning path (ToS-denial steady state and historical backlog). Written via `positioning_entry`; completeness `levels_only` is not a fallback. Provenance is manual/as-of — never live vendor data. |
| **MarketRouter** | Concurrent per-symbol pipeline host (NQ+ES) on one clock. Each root owns an isolated pipeline set; ticks are merge-sorted by `(timestamp_ms, root)` so cross-market predicates are co-recorded from the first row. Session scope (RTH/Globex) is classified per tick on the owning lane — never mixed across symbols. MES/MNQ are out of scope. |
| **StateEnvelope** | `get_state` response: `values`, per-domain `provenance`, per-domain `degraded`, `catalogVersion`. Absence of provenance is a failure. Multi-symbol reads (NQ+ES) live in one envelope: values keyed `{ROOT}.{catalogFieldId}`, symbol-scoped provenance keyed `{ROOT}.{domain}`, plus `clockMs` for the aligned MarketRouter clock. |
| **Desk Catalog** | Versioned schema waist over annotated runtime fields + Positioning (Levels-Only Records via `positioning_entry`; no live Vs3dProvider). Discovery is metadata-only. |
| **SourceProvider** | Market-data provider seam for the engine host. **FileProvider** covers Sierra `.scid`/`.depth`; **SierraProvider** is stubbed until ACSIL (#23). |
| **FileProvider** | Real SourceProvider adapter over on-disk `.scid` + discovered `.depth` paths. |
| **SierraProvider** | Stubbed SourceProvider slot for a future ACSIL bridge — not implemented in M2a. |
| **Engine host** | `the-desk-engine` process owning ingest, pipelines, and event detection so intelligence survives MCP/agent disconnect. |
| **Embedded-engine fallback** | MCP-alone topology (`[sil].engine_mode = "embedded"`) — today's rollback when no external engine is running. |
| **Published state** | Lock-free swapped coaching snapshot + health served on the engine's read-only state socket. |

## Resolution bands

| Band | Role | Served by |
|------|------|-----------|
| **R0** | Orientation | `get_state` |
| **R1** | State | `get_state` |
| **R2** | Evidence | `query_series`, `query_episodes` |
| **R3** | Raw (hard caps) | `query_raw` (use `run_job` for bulk artifacts) |

## Read-kernel operators (behind `[sil].catalog_discovery`)

`describe_environment`, `describe_domain`, `search_catalog`, `get_state`, `get_events`, `query_series`, `query_episodes`, `query_raw`, `run_job`.

Default-off keeps the 123-tool surface unchanged. Orientation specialty getters
(`get_session_context`, `get_market_snapshot`) shim to `get_state` when the flag
is on (`deprecated: true` + `suggestedReplacementOperator`).

`query_*` / `run_job` are Trust Level **L0**: they cannot mutate workflow-verb
state or place orders. Unbounded windows are rejected. Every result carries
sample-size `N` and `reliabilityTier` (AGENT.md Research Sample Size Policy).
`run_job` returns a job id + artifact handle (columnar path + summary), never a
token flood. Capsules are joinable evidence and are not required for Episode
Query to be expressible.

SIL-M3e (`the-desk-eq-bench`) measured the flagship Episode Query on SQLite JSON
blobs vs a dense side table vs DuckDB `read_json` of the M3d JSONL.zst hive.
**DuckDB is deferred** — it is not a default MCP/engine runtime. Feature
`duckdb-bench` stays off (Path C ran only when that feature was compiled in).
