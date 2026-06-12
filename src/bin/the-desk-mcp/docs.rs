//! Self-maintaining MCP tool reference.
//!
//! `render_tool_reference()` builds docs/mcp/tool-reference.md from the live
//! domain routers, so the documented tool surface can never drift from the
//! compiled server. The binary writes it via `--write-tool-docs`; the test
//! `tool_reference_doc_is_current` fails the build when the file is stale.

use rmcp::handler::server::router::tool::ToolRouter;

use crate::state::TheDeskMcp;

/// One tool domain: a named router plus the editorial framing used in docs.
pub(crate) struct ToolDomain {
    /// Display name used as the section heading.
    pub(crate) name: &'static str,
    /// Module path under `src/bin/the-desk-mcp/tools/`.
    pub(crate) module: &'static str,
    /// One-line summary of what the domain covers.
    pub(crate) summary: &'static str,
    /// When an agent should reach for this domain.
    pub(crate) reach_for_when: &'static str,
    /// Constructor for the domain's router.
    pub(crate) router: fn() -> ToolRouter<TheDeskMcp>,
}

/// All tool domains in presentation order. `service.rs` combines the same
/// routers; keep the two lists in sync (the drift test enforces the count).
pub(crate) fn tool_domains() -> Vec<ToolDomain> {
    vec![
        ToolDomain {
            name: "Market",
            module: "market",
            summary: "Live market structure reads: snapshot, TPO, delta, key levels, tape pace, footprint, and per-pipeline state.",
            reach_for_when: "you need current market state or session-relative framing during live conversation",
            router: TheDeskMcp::market_router,
        },
        ToolDomain {
            name: "DOM",
            module: "dom",
            summary: "Depth-of-market analysis: DOM snapshots, pull/stack activity, liquidity behavior at levels, and book-reaction explanations.",
            reach_for_when: "the trader asks how the order book behaved at a price or wants liquidity context",
            router: TheDeskMcp::dom_router,
        },
        ToolDomain {
            name: "Options",
            module: "options",
            summary: "Options integration: gamma levels and dealer-positioning context for the current session.",
            reach_for_when: "gamma exposure or options-derived levels are relevant to the trade discussion",
            router: TheDeskMcp::options_router,
        },
        ToolDomain {
            name: "Playbook",
            module: "playbook",
            summary: "Playbook evaluation, setup lifecycle state, attention signals, and trade idea cards.",
            reach_for_when: "you are tracking setups, triaging what deserves attention, or moving a trade idea through its lifecycle",
            router: TheDeskMcp::playbook_router,
        },
        ToolDomain {
            name: "Risk",
            module: "risk",
            summary: "Risk and account state: limits, position sizing, risk config, and trading session open/close.",
            reach_for_when: "sizing a trade, checking limits, or starting/ending a trading session",
            router: TheDeskMcp::risk_router,
        },
        ToolDomain {
            name: "Journal",
            module: "journal",
            summary: "Trade entries, fill imports, journal notes, session reviews, and journal pattern queries.",
            reach_for_when: "recording or reviewing actual trades and journaling the session",
            router: TheDeskMcp::journal_router,
        },
        ToolDomain {
            name: "Memory",
            module: "memory",
            summary: "Trader memory: agent insights, behavioral patterns, follow-ups, briefings, and trader-context fit.",
            reach_for_when: "you want durable context about the trader, or to persist an insight worth remembering",
            router: TheDeskMcp::memory_router,
        },
        ToolDomain {
            name: "Research",
            module: "research",
            summary: "Historical research: hypotheses, backtests, and frequency/conditional/distribution queries over recorded sessions.",
            reach_for_when: "answering \"how often\" / \"what happens after\" questions or running and comparing backtests",
            router: TheDeskMcp::research_router,
        },
        ToolDomain {
            name: "Admin",
            module: "admin",
            summary: "Operations: feed health, tick ingestion, contract rollover, archival, and data integrity validation.",
            reach_for_when: "diagnosing data problems, backfilling history, or verifying feed and database health",
            router: TheDeskMcp::admin_router,
        },
    ]
}

/// Render the complete tool reference markdown from the live routers.
pub(crate) fn render_tool_reference() -> String {
    let domains = tool_domains();
    let listings: Vec<(usize, Vec<rmcp::model::Tool>)> = domains
        .iter()
        .enumerate()
        .map(|(i, d)| (i, (d.router)().list_all()))
        .collect();
    let total: usize = listings.iter().map(|(_, tools)| tools.len()).sum();

    let mut out = String::new();
    out.push_str("# The Desk — MCP Tool Reference\n\n");
    out.push_str("> **Generated file — do not edit by hand.**\n");
    out.push_str("> Regenerate with `cargo run --bin the-desk-mcp -- --write-tool-docs`.\n");
    out.push_str("> The test `tool_reference_doc_is_current` fails when this file is stale.\n\n");
    out.push_str(&format!(
        "The Desk MCP server exposes **{total} MCP tools** across **{} domains**. \
         Each domain maps to a module under `src/bin/the-desk-mcp/tools/`. \
         For scenario-based routing (\"which tool do I call when…\"), read \
         `skills/mcp-tools/SKILL.md` first; this file is the exhaustive catalog.\n\n",
        domains.len()
    ));

    out.push_str("| Domain | Tools | Reach for it when… |\n|---|---|---|\n");
    for (i, tools) in &listings {
        let d = &domains[*i];
        out.push_str(&format!(
            "| [{}](#{}) | {} | {} |\n",
            d.name,
            d.name.to_lowercase(),
            tools.len(),
            d.reach_for_when
        ));
    }
    out.push('\n');

    for (i, tools) in &listings {
        let d = &domains[*i];
        out.push_str(&format!("## {}\n\n", d.name));
        out.push_str(&format!(
            "{}\n\nModule: `src/bin/the-desk-mcp/tools/{}.rs`\n\n",
            d.summary, d.module
        ));
        for tool in tools {
            out.push_str(&format!("### `{}`\n\n", tool.name));
            let desc = tool
                .description
                .as_deref()
                .unwrap_or("(no description)")
                .trim()
                .to_string();
            out.push_str(&desc);
            out.push_str("\n\n");
        }
    }
    out
}

/// Default on-disk location of the generated reference (repo-relative).
pub(crate) fn tool_reference_path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("docs")
        .join("mcp")
        .join("tool-reference.md")
}

/// Write the generated reference to docs/mcp/tool-reference.md.
pub(crate) fn write_tool_reference() -> Result<(), Box<dyn std::error::Error>> {
    let path = tool_reference_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&path, render_tool_reference())?;
    eprintln!("wrote {}", path.display());
    Ok(())
}
