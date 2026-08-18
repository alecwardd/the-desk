# The Desk — MCP Server Architecture

The MCP server is the central interface of The Desk: it exposes all pipeline
state, playbook evaluation, research, memory, and operations as MCP tools over
stdio. Any MCP client (Cursor, Claude Code, Codex) becomes a trading partner by
connecting to it.

- **Binary:** `the-desk-mcp` (default run target)
- **Transport:** stdio (`rmcp` crate). stdout is protocol-only; logs go to
  stderr/file (enforced by the `mcp_stdio` integration test).
- **Tool surface:** 123 MCP tools in 9 domains. The exhaustive, generated catalog is
  [tool-reference.md](tool-reference.md). Scenario routing for agents is
  [skills/mcp-tools/SKILL.md](../../skills/mcp-tools/SKILL.md).

---

## Module Layout

```
src/bin/the-desk-mcp/
├── main.rs        # Entry point: arg handling, logging, feed wiring, serve loop
├── state.rs       # Constants, runtime caches, TheDeskMcp service state
├── service.rs     # Constructors, shared service methods, combined tool router
├── handler.rs     # rmcp ServerHandler (server info + instructions)
├── helpers.rs     # Error mapping, parsing, validation, payload shaping
├── params.rs      # Tool parameter structs (serde + JsonSchema)
├── lifecycle.rs   # Tick processing, depth polling, session transitions, warm replay
├── docs.rs        # Tool-reference generator (--write-tool-docs) + domain registry
├── tests.rs       # Binary test module
└── tools/
    ├── market.rs    # Live market structure reads (24 tools)
    ├── dom.rs       # Depth-of-market analysis (10)
    ├── options.rs   # Gamma / dealer positioning (3)
    ├── playbook.rs  # Setups, attention signals, trade ideas (16)
    ├── risk.rs      # Risk, account, sizing, session bookends (9)
    ├── journal.rs   # Trade entries, fills, journal, reviews (12)
    ├── memory.rs    # Insights, patterns, follow-ups, briefings, positioning_entry (13)
    ├── research.rs  # Hypotheses, backtests, feature_registry, statistical queries (24)
    └── admin.rs     # Feed health, ingestion, rollover, integrity, backups (12)
```

### The named-router pattern

Each domain module holds one `impl TheDeskMcp` block annotated with a named
router:

```rust
#[tool_router(router = market_router, vis = "pub(crate)")]
impl TheDeskMcp {
    #[tool(description = "…")]
    async fn get_market_snapshot(&self) -> Result<CallToolResult, McpError> { … }
}
```

`service.rs` combines them into the single router the server registers:

```rust
pub(crate) fn tool_router() -> ToolRouter<Self> {
    Self::market_router()
        + Self::dom_router()
        + …
        + Self::admin_router()
}
```

`docs.rs` keeps a parallel registry (`tool_domains()`) used to generate the
reference doc. The test `domain_routers_cover_combined_router_exactly`
guarantees the two lists can never diverge.

## How to Add a Tool

> **SIL-M0 / Catalog v0 / M1b / M5a:** **no new specialty market tools** without a Desk
> Catalog entry. Specialty market tools are the `market` domain router
> (`tools/market.rs`). The rule is **no catalog entry → no new market tool**
> (allowlist in `docs/mcp/desk-catalog-v0.json`). New detector concepts also need a
> Feature Registry entry (`feature_registry` write verb; Base Detectors and Derived
> Features with Feature-IR; an accepted Derived Feature is codegen'd onto the existing
> kernel — discovery via `search_catalog` when `[sil].catalog_discovery = true`). The write verb is always on the default
> surface; reading descriptors back requires the discovery flag (same pattern as
> `positioning_entry` / `get_state`).
> Workflow domains (playbook /
> risk / journal / memory / research / admin) and SIL kernel operators
> (`describe_*` / `search_catalog` / `get_state` / `get_events` /
> `query_series` / `query_episodes` / `query_raw` / `run_job`, behind
> `[sil].catalog_discovery`) are not specialty market tools; do not delete
> existing tools in the name of this policy.

1. Pick the domain module under `tools/` (or create a new one — add it to
   `tools/mod.rs`, `service.rs`'s combiner, and `docs.rs`'s `tool_domains()`).
   Exception: SIL kernel operators live in `tools/discovery.rs` and are
   wired only via `tool_router_with_sil` when `[sil].catalog_discovery` is on —
   they stay out of the default `tool_domains()` registry so the 123-tool
   surface remains unchanged when the flag is off.
2. Add the parameter struct to `params.rs` deriving
   `Deserialize + JsonSchema + Default` with `#[serde(rename_all = "camelCase")]`.
   For free-form JSON values use the `schemars_loose_object` schema helper —
   Cursor rejects boolean schemas.
3. Write the `#[tool(description = "…")]` method in the domain module. The
   description is agent-facing documentation: say what it returns, when to use
   it, and name the alternatives (see existing descriptions for tone).
4. Return structured JSON via `text_result(...)`; use `no_data(...)` for the
   empty case with a actionable message. Map DB errors with `db_error`.
5. Regenerate the catalog: `cargo run --bin the-desk-mcp -- --write-tool-docs`.
6. If the routing story changed, update
   [skills/mcp-tools/SKILL.md](../../skills/mcp-tools/SKILL.md).
7. `cargo test --bin the-desk-mcp` — the drift guards will tell you if you
   missed a step:
   - `documented_mcp_tool_count_matches_router` — AGENT.md / README.md /
     CLAUDE.md state the true tool count.
   - `domain_routers_cover_combined_router_exactly` — docs registry matches the
     combined router, with no tool in two domains.
   - `tool_reference_doc_is_current` — tool-reference.md matches the compiled
     server.
   - `specialty_market_tools_are_frozen_until_catalog_v0` — market tool names
     match the SIL-M0 freeze set.
   - `specialty_market_tools_require_catalog_allowlist_entry` — market tools ⊆
     Catalog v0 specialty allowlist.
   - `detector_specialty_tools_partition_live_market_router` — every live market
     tool is classified as detector-backed (must cite an active Feature Registry
     id) or pinned non-detector; an unclassified addition fails the build.
   - `desk_catalog_docs_are_current` — `desk-catalog-v0.json` / `.md` match
     `build_catalog()`.

## Desk Catalog v0 (SIL-M1a)

The Desk Catalog is the schema waist: a generated, versioned inventory of
domains and fields (unit, session scope, freshness, cost hint) derived from
annotated `MarketState` plus the **Positioning** domain (grid / by-strike /
Slice / first-class Levels-Only Records via `positioning_entry`; no live provider).

- Artifacts: [desk-catalog-v0.json](desk-catalog-v0.json),
  [desk-catalog-v0.md](desk-catalog-v0.md)
- Regenerate: `cargo run --bin the-desk-mcp -- --write-catalog-docs`
- Discovery / read-kernel operators: `describe_environment`, `describe_domain`,
  `search_catalog`, `get_state` (StateEnvelope, R0|R1), `get_events` (identity
  rows), `query_series`, `query_episodes`, `query_raw`, `run_job` (artifact
  handle) — registered only when `[sil].catalog_discovery = true` in
  `~/.the-desk/config.toml`. Default off → 123-tool surface unchanged.
  Trust Level L0 (no mutation / order authority). Unbounded query windows are
  rejected. See [CONTEXT.md](../../CONTEXT.md).

## SIL-M0 tool-call telemetry

Every MCP `tools/call` is observed in `ServerHandler::call_tool`:

- per-tool call counts
- approximate response token cost (`ceil(response_bytes / 4)`)
- orientation-chain cost for the documented session-start sequence
  (`get_session_context` → `get_market_snapshot` → `get_risk_state` /
  `get_risk_config` / `get_account_state`)

The checked-in baseline is
[sil-m0-tool-telemetry-baseline.json](sil-m0-tool-telemetry-baseline.json).
It is the **immutable M0 before-figure** for M1b (orientation-chain tools, freeze
set, and cold probe costs). Do not regenerate it as part of routine tool-surface
churn; re-bless only with an intentional
`cargo run --bin the-desk-mcp -- --write-sil-m0-baseline` when the orientation-chain
contract itself changes. Live counters accumulate in-process and periodically
flush to `~/.the-desk/telemetry/tool-call-snapshot.json`.

## SIL-M1b read kernel + orientation shims

With `[sil].catalog_discovery = true`:

- `get_state` returns a StateEnvelope (`values`, per-domain `provenance`,
  per-domain `degraded`, `catalogVersion`). Missing provenance is a failure;
  degraded domains set their flag and stay visible.
- `get_events` returns formalized rows: lifecycle (`open` → `updated` → `resolved`|`expired`), severity, occurrence + dedup identity, `frameRef` joining each event to the producing Journal Frame, and `capsuleRef` on DOM-family rows. Trust Level L0.
- `get_attention_inbox` is a ranked view over that event stream (`viewOf: eventStream`), not a parallel source of truth.
- `get_session_context` / `get_market_snapshot` still answer and include
  `deprecated: true` + `suggestedReplacementOperator: "get_state"`.
- Opinionated bundles remain: `get_context_frame`, `get_attention_inbox`,
  `evaluate_playbook`.

## SIL-M3c research query kernel

With `[sil].catalog_discovery = true`:

- `query_series` (R2) — time series of catalog fields from 1 Hz Journal Frames.
  Optional `store=hot` (default) or `store=cold`.
- `query_episodes` (R2) — conjunctive Episode Query over catalog fields /
  events; flagship five-predicate query is expressible; tick-driven MFE/MAE.
  Optional `store=hot|cold` for frames.
- `query_raw` (R3) — hard-capped raw read of `journal_frames` / `events` /
  `ticks`. Unbounded windows are rejected. Optional `store=hot|cold` for
  `journal_frames` only.
- `run_job` — job id + artifact handle (columnar path + summary), never a
  token flood. Does not mutate playbook / risk / journal / memory / orders.
  Optional `store=hot|cold`.

Every result includes `N` and `reliabilityTier`. Missing detector/vendor
fields fail closed. These operators stay out of `tool_domains()` so the
generated 123-tool reference is unchanged when the flag is off.

## SIL-M3d cold session-partitioned frames

SQLite remains the hot window and transactional/control plane. 1 Hz Journal
Frames are also dumped to hive-partitioned JSONL.zst under
`~/.the-desk/journal-frames` (`desk-journal-frames-v1`; not DuckDB, not a
`config.toml` knob). Partitions never mix RTH with Globex or NQ with ES.
Rebuild from `.scid`/`.depth` matches the M3a golden strict fingerprint.

Live writes are **append-only** (concatenated zstd frames). Duplicate
`(frame_second, root)` keys are suppressed in memory; session close
`compact()` rewrites one sorted frame. A cold IO error is best-effort
(`tracing::warn!`); SQLite persist, attention, and Capsules still complete.

**Single-writer:** only the process that owns ingest writes the hive.
Embedded MCP (`[sil].engine_mode = "embedded"`) attaches the dump store.
`engine_mode = "external"` leaves dumps to `the-desk-engine`. Do not run
both writers against the same root. Compact rewrites use a unique temp
suffix (`frames.jsonl.zst.{pid}.{nanos}.tmp`). MCP `store=cold` reads are
safe from either process — they construct a store at query time from
`~/.the-desk/journal-frames`.

The same four operators accept optional `store=hot` (default) or `store=cold`.
Envelope fields, Trust Level L0, window/session/`N`/reliability contracts are
unchanged. Events and ticks stay on SQLite; `query_raw` + `store=cold` +
`source=events|ticks` fails closed. Unknown store labels fail closed.

## SIL-M3e Episode Query benchmark (DuckDB deferred)

Not an MCP tool. `cargo run --release --bin the-desk-eq-bench` times the
flagship Episode Query on (A) SQLite JSON blobs, (B) a rebuildable dense side
table, (C) DuckDB `read_json` of the M3d JSONL.zst hive when compiled with
`--features duckdb-bench` (off by default; CI must not require DuckDB).

Measured verdict: **DEFER DuckDB**. Path B p95 was 34 ms over 10 RTH days /
486k 1 Hz NQ+ES frames vs an owner SLO of p95 ≤ 2–3 s. Path C (`read_json` of
the full unpruned JSONL.zst hive, not Parquet) agreed on match fingerprint at
golden and 10-day scale (~520 ms on the 10-day corpus). That number is a
lower bound on DuckDB in this layout, not a ceiling on DuckDB. See
[docs/decision-log.md](../decision-log.md) SIL-M3e. Do not convert the hive to
Parquet. Do not add DuckDB to `the-desk-mcp`.

## SIL-M2a engine extract + embedded fallback

- Default `[sil].engine_mode = "embedded"`: MCP owns ingest (today's topology /
  true rollback).
- `[sil].engine_mode = "external"`: MCP is a thin adapter over
  `the-desk-engine`'s read-only localhost state socket (`engine_bind`, default
  `127.0.0.1:17843`). Ingest/pipelines/events live in the engine so they survive
  agent disconnect (Globex overnight coverage via Task Scheduler — see
  `docs/ops/engine-lifecycle.md`).
- Published coaching state uses a lock-free swap; kill-the-engine degrades
  cleanly (`engine.adapter_degraded`) and recovers on reconnect.
- `SourceProvider`: FileProvider (`.scid`/`.depth`) real; SierraProvider stubbed.
- **MarketRouter v0:** concurrent NQ + ES on one clock. `get_state` can return both
  symbols in one StateEnvelope (`{ROOT}.{catalogFieldId}` values). Micros (MES/MNQ)
  are out of scope. Missing ES does not degrade the NQ coaching path.
- Trust Ceiling stays L3; read/query kernel remains Trust Level L0.

## Runtime Model

- **State:** `TheDeskMcp` (in `state.rs`) holds `Arc`-shared handles: the SQLite
  `Database` writer behind a mutex, a bounded read-only WAL connection pool
  (`ReadPool`), the `PipelineEngine`, rules engine, event detector,
  attention/runtime-event stores, and caches (contract resolution, context
  frames, options snapshots, playbook runtime). In external engine mode it also
  holds a lock-free `PublishedStateStore` refreshed from the engine socket.
- **Feed loop:** In embedded mode, `main.rs` spawns the `.scid` poll loop;
  `lifecycle.rs` owns tick processing, depth polling, RTH close finalization,
  new-session preparation, and startup warm replay. In external mode, ingest is
  owned by `the-desk-engine`.
- **Concurrency model:** writes go through the single `Database` writer behind
  `Arc<Mutex<Database>>`; read-only tools borrow a dedicated
  `SQLITE_OPEN_READ_ONLY` connection from a bounded WAL pool (`read_pool.rs`,
  `DEFAULT_READ_POOL_SIZE = 4`) via `with_read_db`, so long research queries no
  longer block live market reads or the writer. The pool opens connections
  lazily, returns them to an idle stack on drop, and self-heals if one is lost.
  Heavy synchronous paths still run under `spawn_blocking`.

## Hard Rules (from CLAUDE.md)

- No Claude API calls from Rust — tools return structured data only.
- Tools never return raw tick streams; summaries and structured slices only.
- The rules engine evaluates before any LLM sees market state.
- The server must work without network connectivity.
