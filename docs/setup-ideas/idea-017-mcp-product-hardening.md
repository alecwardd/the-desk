# IDEA-017: MCP Product Hardening — Playbook & Guidance as First-Class Data

> Part of the [setup-ideas tracker](index.md) — statuses, catalog, and the
> backtesting queue live there; new ideas start from [_template.md](_template.md).

**Status:** Idea
**Source:** Product review — MCP exposes market intelligence well; playbook and trading philosophy remain primarily in repository markdown
**Complements:** All Cursor agents; orchestrator and specialist prompts that should cite canonical definitions

**Framing:** This is **MCP product hardening**, not a defect in the current server. The live surface already exposes market state, risk state, setup evaluation, and setup-oriented context. What it does *not* yet expose as first-class, queryable MCP data are the canonical artifacts: playbook rules, setup templates, methodology notes, and trader-specific guidance that today live in markdown under the repo (and in agent definitions).

**Gap (precise):** `get_setup_context()` in `src/bin/the-desk-mcp.rs` returns **market and risk context** around a **named** setup — not the setup’s **definition** (conditions, template fields, narrative guardrails). Agents still infer playbook semantics from files on disk rather than from structured tool responses.

**Implementation direction:**
- There are **no MCP resource handlers** in `the-desk-mcp.rs` today. **Dedicated read tools** (e.g. list templates, fetch template by id, fetch playbook section or checksum) are likely the **simplest first increment** before investing in full MCP resources (`list_resources` / `read_resource`).
- **Next concrete step:** add one or more read-only tools that return structured JSON (or similar) for setup templates and playbook excerpts, with stable ids and versioning metadata where useful. Iterate on shape and granularity with real agent prompts; consider resources later if clients benefit from URI-based discovery.

**Success criteria (initial):** An agent can answer “what are the conditions for setup X?” and “what does the desk mean by term Y?” using MCP output alone, without opening arbitrary markdown paths unless the trader opts into repo-local files.
