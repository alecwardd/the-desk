# The Desk — MCP Server Architecture

The MCP server is the central interface of The Desk: it exposes all pipeline
state, playbook evaluation, research, memory, and operations as MCP tools over
stdio. Any MCP client (Cursor, Claude Code, Codex) becomes a trading partner by
connecting to it.

- **Binary:** `the-desk-mcp` (default run target)
- **Transport:** stdio (`rmcp` crate). stdout is protocol-only; logs go to
  stderr/file (enforced by the `mcp_stdio` integration test).
- **Tool surface:** 121 MCP tools in 9 domains. The exhaustive, generated catalog is
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
    ├── memory.rs    # Insights, patterns, follow-ups, briefings (12)
    ├── research.rs  # Hypotheses, backtests, statistical queries (23)
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

> **SIL-M0 / Catalog v0:** **no new specialty market tools** without a Desk
> Catalog entry. Specialty market tools are the `market` domain router
> (`tools/market.rs`). The rule is **no catalog entry → no new market tool**
> (allowlist in `docs/mcp/desk-catalog-v0.json`). Workflow domains (playbook /
> risk / journal / memory / research / admin) and SIL discovery operators
> (`describe_*` / `search_catalog`, behind `[sil].catalog_discovery`) are not
> specialty market tools; do not delete existing tools in the name of this policy.

1. Pick the domain module under `tools/` (or create a new one — add it to
   `tools/mod.rs`, `service.rs`'s combiner, and `docs.rs`'s `tool_domains()`).
   Exception: SIL discovery operators live in `tools/discovery.rs` and are
   wired only via `tool_router_with_sil` when `[sil].catalog_discovery` is on —
   they stay out of the default `tool_domains()` registry so the 121-tool
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
   - `desk_catalog_docs_are_current` — `desk-catalog-v0.json` / `.md` match
     `build_catalog()`.

## Desk Catalog v0 (SIL-M1a)

The Desk Catalog is the schema waist: a generated, versioned inventory of
domains and fields (unit, session scope, freshness, cost hint) derived from
annotated `MarketState` plus the **Positioning** domain stub (grid / by-strike /
Slice / Levels-Only Record; no live provider).

- Artifacts: [desk-catalog-v0.json](desk-catalog-v0.json),
  [desk-catalog-v0.md](desk-catalog-v0.md)
- Regenerate: `cargo run --bin the-desk-mcp -- --write-catalog-docs`
- Discovery operators (metadata only): `describe_environment`, `describe_domain`,
  `search_catalog` — registered only when `[sil].catalog_discovery = true` in
  `~/.the-desk/config.toml`. Default off → 121-tool surface unchanged.

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

## Runtime Model

- **State:** `TheDeskMcp` (in `state.rs`) holds `Arc`-shared handles: the SQLite
  `Database` writer behind a mutex, a bounded read-only WAL connection pool
  (`ReadPool`), the `PipelineEngine`, rules engine, event detector,
  attention/runtime-event stores, and caches (contract resolution, context
  frames, options snapshots, playbook runtime).
- **Feed loop:** `main.rs` spawns the `.scid` poll loop; `lifecycle.rs` owns
  tick processing, depth polling, RTH close finalization, new-session
  preparation, and startup warm replay.
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
