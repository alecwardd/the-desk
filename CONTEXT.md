# CONTEXT.md — Desk vocabulary (SIL)

Canonical glossary for The Desk / Sierra Intelligence Layer. Use these terms
exactly in code, docs, ADRs, and agent copy. Do not invent synonyms.

| Term | Meaning |
|------|---------|
| **Trust Level** | Capability property of a tool on the MCP surface. Read/query kernel operators run at **L0** and cannot carry mutation or order authority. |
| **Trust Ceiling** | System-wide maximum Trust Level. Stays at **L3** (drafts proposals; the human executes). Raising it requires a new ADR. |
| **Base Detector** | Stateful Rust detector (absorption, pinch, …). Math stays in Rust; schema/provenance/promotion become registry-governed later. |
| **Derived Feature** | Declared Feature-IR program over catalog fields using funded Operator Families. |
| **Feature-IR** | Typed intermediate representation for Derived Features (later milestones). |
| **Operator Family** | One of the funded Feature-IR operator classes (cross-symbol, session percentiles, dwell/time-since, event sequences, historical baselines). |
| **Journal Frame** | 1 Hz persisted market-state frame on the MarketRouter clock (NQ+ES). Compute/publish may stay at ~250 ms (ADR-019); only 1 Hz frames are persisted. `get_state(as_of=…)` is served from Journal Frames (provenance source = Journal). Transition event rows join via `(journal_frame_second, root_symbol)`. |
| **Capsule** | High-resolution dump around a salient event (~30s before → ~60s after); mandatory for DOM-family events later. |
| **Episode Query** | Conjunctive historical query over catalog fields with forward returns / MFE/MAE (later research kernel). |
| **Positioning** | Dealer/options domain with four record kinds: position-grid, by-strike, Slice, Levels-Only Record. Catalog stub first; providers later. |
| **Slice** | Price-indexed greek values plus Desk-derived levels at capture time (Positioning record kind). |
| **Levels-Only Record** | First-class manual Positioning path when capture is unavailable. |
| **MarketRouter** | Concurrent per-symbol pipeline host (NQ+ES) on one clock. Each root owns an isolated pipeline set; ticks are merge-sorted by `(timestamp_ms, root)` so cross-market predicates are co-recorded from the first row. Session scope (RTH/Globex) is classified per tick on the owning lane — never mixed across symbols. MES/MNQ are out of scope. |
| **StateEnvelope** | `get_state` response: `values`, per-domain `provenance`, per-domain `degraded`, `catalogVersion`. Absence of provenance is a failure. Multi-symbol reads (NQ+ES) live in one envelope: values keyed `{ROOT}.{catalogFieldId}`, symbol-scoped provenance keyed `{ROOT}.{domain}`, plus `clockMs` for the aligned MarketRouter clock. |
| **Desk Catalog** | Versioned schema waist over annotated runtime fields + Positioning stub. Discovery is metadata-only. |
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
| **R2** | Evidence | later `query_*` operators |
| **R3** | Raw (hard caps) | later `query_raw` |

## Read-kernel operators (behind `[sil].catalog_discovery`)

`describe_environment`, `describe_domain`, `search_catalog`, `get_state`, `get_events`.

Default-off keeps the 121-tool surface unchanged. Orientation specialty getters
(`get_session_context`, `get_market_snapshot`) shim to `get_state` when the flag
is on (`deprecated: true` + `suggestedReplacementOperator`).
