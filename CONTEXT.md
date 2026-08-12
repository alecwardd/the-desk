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
| **Journal Frame** | 1 Hz persisted market-state frame (later milestone). `get_state(as_of=…)` is served from persisted snapshots until frames ship. |
| **Capsule** | High-resolution dump around a salient event (~30s before → ~60s after); mandatory for DOM-family events later. |
| **Episode Query** | Conjunctive historical query over catalog fields with forward returns / MFE/MAE (later research kernel). |
| **Positioning** | Dealer/options domain with four record kinds: position-grid, by-strike, Slice, Levels-Only Record. Catalog stub first; providers later. |
| **Slice** | Price-indexed greek values plus Desk-derived levels at capture time (Positioning record kind). |
| **Levels-Only Record** | First-class manual Positioning path when capture is unavailable. |
| **MarketRouter** | Concurrent per-symbol pipeline host (NQ+ES) on one clock — later milestone; not implemented in M1b. |
| **StateEnvelope** | `get_state` response: `values`, per-domain `provenance`, per-domain `degraded`, `catalogVersion`. Absence of provenance is a failure. |
| **Desk Catalog** | Versioned schema waist over annotated runtime fields + Positioning stub. Discovery is metadata-only. |

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
