# AGENTS.md

This repository's canonical agent instructions live in two files — read both:

- **[AGENT.md](AGENT.md)** — universal workflow for any LLM coding agent (Claude Code, Cursor, Codex): architecture summary, subagent patterns, the MCP Tools Reference (live vs historical mapping + agent-to-capability matrix), the Research Sample Size Policy, and the Lucid Direct context.
- **[CLAUDE.md](CLAUDE.md)** — project rules: the mandatory three-layer architecture, trading terminology, code conventions, and the Never-Do list.

For trader-facing usage — how to talk to the agents and the MCP server to get the
best results — see **[docs/agent-interaction-guide.md](docs/agent-interaction-guide.md)**.

> This file is intentionally a thin pointer so it cannot drift from the canonical
> rules. Do not paste project rules or architecture here — edit `AGENT.md` and
> `CLAUDE.md` instead.

## Cursor Cloud specific instructions

Durable, non-obvious notes for cloud agents. Standard lint/test/build/run commands
already live in the README "Development" section — use those, don't duplicate them.

- **Rust toolchain is pinned via `rust-toolchain.toml`** (`channel = "stable"`,
  components `rustfmt`/`clippy`). MSRV is also declared in `Cargo.toml`
  (`rust-version = "1.87"`) — the code uses APIs stable since 1.87 (e.g.
  `usize::is_multiple_of`), and a transitive dependency (`crypto-common`, via
  `sha2`) additionally requires the `edition2024` feature (Rust ≥ 1.85). A base
  VM shipping Rust 1.83 will fail at manifest-parse with an `edition2024 is
  required` error unless rustup picks up the in-repo toolchain file (it does
  automatically on `cargo`/`rustc` invocation). Do not hand-pin a different
  channel.
- **Repo-managed environment config:** `.cursor/environment.json` runs
  `cargo fetch` + debug + release `the-desk-mcp` builds as the `install` step.
  There is no `start` / `terminals` entry — the MCP server is stdio (launched by
  Cursor via `.cursor/mcp.json`), not a long-running daemon.
- **No external services.** SQLite is compiled in (`rusqlite` `bundled`, needs a C
  compiler — `clang` is present) and auto-creates `~/.the-desk/data.db`. There is
  no DB/broker/network daemon to start.
- **The MCP server speaks JSON-RPC over stdio, not a TCP port.** `the-desk-mcp` is
  normally launched by Cursor via `.cursor/mcp.json` (template:
  `.cursor/mcp.example.json`). To exercise it manually, pipe newline-delimited
  JSON-RPC (`initialize` → `notifications/initialized` → `tools/call`) into the
  built binary's stdin. It exposes 122 tools.
- **No live Sierra Chart `.scid` feed exists in cloud (it is Windows-only).** The
  server still starts and serves every tool; it logs `scid.file_missing` and
  live-market tools (`get_market_snapshot`, `get_session_context`) return
  "no data". This is expected — it is not a broken environment.
- **To validate the stack end-to-end without a feed:** seed the playbook with
  `the-desk-mcp --seed-templates --activate` (writes 13 PTT setups to SQLite),
  then call the `evaluate_playbook` tool — the rules engine (Layer 2) evaluates
  the seeded setups and returns per-setup readiness. `cargo test` (494 tests,
  incl. `tests/session_replay_golden.rs` and `tests/mcp_stdio.rs`) covers the
  deterministic pipelines end-to-end.
- **Config** lives at `~/.the-desk/config.toml` (not committed; see README for the
  schema). It is optional — the server runs on defaults when it is absent.
- **Pre-commit quality gate is opt-in:** `git config core.hooksPath .githooks`
  runs fmt/clippy/tests + a secret/advisory-language scan (see `.githooks/pre-commit`).
