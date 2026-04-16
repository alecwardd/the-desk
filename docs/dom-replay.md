# DOM replay visualizer (removed)

The historical DOM **desktop visualizer** and Tauri IPC commands for DOM replay were removed from this repository. The Desk is **backend-only**: Sierra `.scid` / optional `.depth` ingestion, Rust pipelines, SQLite, and MCP tools.

**Still available for agents:**

- Live and historical **DOM summaries** and depth-backed tools via MCP (see `src/depth/` and `the-desk-mcp` handlers that expose DOM-related JSON).
- **Session recordings** (`.zst` under `~/.the-desk/recordings/`) and the `recording` module for compressed tick/session replay data used by tooling — not a GUI replay workspace.

For end-to-end ladder review, use Sierra Chart or another DOM front end; use MCP tools here for structured reads over the same files and database.
