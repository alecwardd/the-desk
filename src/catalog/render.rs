//! Render / write checked-in Desk Catalog artifacts.

use super::{build_catalog, DeskCatalog};
use std::path::{Path, PathBuf};

/// Checked-in JSON catalog path (repo-relative from crate root).
pub fn catalog_json_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("docs/mcp/desk-catalog-v0.json")
}

/// Checked-in markdown catalog path (repo-relative from crate root).
pub fn catalog_markdown_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("docs/mcp/desk-catalog-v0.md")
}

/// Pretty-printed JSON for the Desk Catalog.
pub fn render_catalog_json(catalog: &DeskCatalog) -> String {
    let body = serde_json::to_string_pretty(catalog).expect("serialize DeskCatalog");
    format!("{body}\n")
}

/// Human-readable markdown summary of the Desk Catalog.
pub fn render_catalog_markdown(catalog: &DeskCatalog) -> String {
    let mut out = String::new();
    out.push_str("# Desk Catalog v0\n\n");
    out.push_str("> **Generated file — do not edit by hand.**\n");
    out.push_str("> Regenerate with `cargo run --bin the-desk-mcp -- --write-catalog-docs`.\n");
    out.push_str("> The test `desk_catalog_docs_are_current` fails when this file is stale.\n\n");
    out.push_str(&format!(
        "- **catalogVersion:** `{}`\n",
        catalog.catalog_version
    ));
    out.push_str("- **Trust Ceiling:** L3 (ADR-022)\n");
    out.push_str(&format!(
        "- **domains:** {}\n- **fields:** {}\n",
        catalog.domains.len(),
        catalog.fields.len()
    ));
    out.push_str(
        "- **Positioning provider:** none (no Vs3dProvider). Levels-Only Records are first-class via `positioning_entry` (manual/as-of).\n\n",
    );

    out.push_str("## Domains\n\n");
    for domain in &catalog.domains {
        out.push_str(&format!(
            "### `{}` — {}\n\n{}\n\n",
            domain.id, domain.name, domain.summary
        ));
        if !domain.record_kinds.is_empty() {
            out.push_str(&format!(
                "Record kinds: {}\n\n",
                domain.record_kinds.join(", ")
            ));
        }
        out.push_str(
            "| Field id | Unit | Session scope | Freshness | Cost |\n|---|---|---|---|---|\n",
        );
        for field in catalog.fields.iter().filter(|f| f.domain_id == domain.id) {
            out.push_str(&format!(
                "| `{}` | {:?} | {:?} | {:?} | {:?} |\n",
                field.id, field.unit, field.session_scope, field.freshness, field.cost_hint
            ));
        }
        out.push('\n');
    }

    out.push_str("## Positioning record kinds\n\n");
    for kind in &catalog.positioning_record_kinds {
        out.push_str(&format!(
            "- **{}** (`{}`): {}\n",
            kind.name, kind.id, kind.summary
        ));
    }
    out.push('\n');

    out.push_str("## Specialty market tools (allowlist)\n\n");
    out.push_str(
        "Catalog v0 enforces **no catalog entry → no new market tool**. \
         The allowlist below matches the SIL-M0 freeze set:\n\n",
    );
    for tool in &catalog.specialty_market_tools {
        out.push_str(&format!("- `{tool}`\n"));
    }
    out.push('\n');
    out
}

/// Write both checked-in catalog artifacts next to `docs/mcp/`.
pub fn write_catalog_docs() -> Result<(), String> {
    let catalog = build_catalog();
    write_atomic(&catalog_json_path(), &render_catalog_json(&catalog))?;
    write_atomic(&catalog_markdown_path(), &render_catalog_markdown(&catalog))?;
    Ok(())
}

fn write_atomic(path: &Path, body: &str) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("create {}: {e}", parent.display()))?;
    }
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| format!("catalog path missing file name: {}", path.display()))?;
    let tmp = path.with_file_name(format!(".{file_name}.{}.tmp", std::process::id()));
    std::fs::write(&tmp, body).map_err(|e| format!("write temp {}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, path).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        format!("replace {}: {e}", path.display())
    })?;
    Ok(())
}
