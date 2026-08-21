//! Catalog lint for `docs/setup-ideas/`.
//!
//! `cargo test docs_lint` is the advertised check in `docs/setup-ideas/index.md`.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|err| panic!("read {}: {err}", path.display()))
}

fn parse_index(index: &str) -> Vec<(String, String)> {
    let mut rows = Vec::new();
    for line in index.lines() {
        let Some(caps) = regex_idea_row(line) else {
            continue;
        };
        rows.push(caps);
    }
    assert!(
        !rows.is_empty(),
        "docs/setup-ideas/index.md must list IDEA rows"
    );
    rows
}

fn regex_idea_row(line: &str) -> Option<(String, String)> {
    // | IDEA-000 | Title | [file.md](file.md) |
    let line = line.trim();
    if !line.starts_with("| IDEA-") {
        return None;
    }
    let cols: Vec<&str> = line
        .split('|')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    if cols.len() < 3 {
        return None;
    }
    let id = cols[0].to_string();
    if !id.starts_with("IDEA-") {
        return None;
    }
    let link = cols[2];
    let file = link.split('(').nth(1)?.trim_end_matches(')').to_string();
    Some((id, file))
}

fn frontmatter(text: &str) -> &str {
    let rest = text.strip_prefix("---\n").unwrap_or(text);
    rest.split("\n---").next().unwrap_or(rest)
}

fn fm_line<'a>(fm: &'a str, key: &str) -> Option<&'a str> {
    let prefix = format!("{key}:");
    fm.lines()
        .find(|line| line.starts_with(&prefix))
        .map(|line| line[prefix.len()..].trim())
}

const ALLOWED_STATUS: &[&str] = &[
    "Idea",
    "Researched",
    "Prototyped",
    "Backtesting-ready",
    "Backtesting",
    "Validated",
    "In Playbook",
    "Rejected",
];

#[test]
fn docs_lint() {
    let root = repo_root();
    let ideas_dir = root.join("docs/setup-ideas");
    let index_path = ideas_dir.join("index.md");
    let hub_path = root.join("docs/setup-ideas-and-backtesting.md");
    let index = read(&index_path);
    let hub = read(&hub_path);
    let rows = parse_index(&index);

    let mut index_ids = BTreeSet::new();
    for (id, file) in &rows {
        assert!(index_ids.insert(id.clone()), "duplicate index row for {id}");
        let path = ideas_dir.join(file);
        assert!(
            path.is_file(),
            "{id} index file missing: {}",
            path.display()
        );
        assert!(
            file.starts_with(id) && file.ends_with(".md"),
            "{id} file name {file} must start with the id"
        );
        assert!(
            hub.contains(&format!("<a id=\"{}\"></a>", id.to_lowercase())),
            "hub must contain anchor <a id=\"{}\"> for {id}",
            id.to_lowercase()
        );
        assert!(
            hub.contains(&format!("setup-ideas/{file}")),
            "hub stub must link to setup-ideas/{file}"
        );
    }

    assert!(
        !hub.contains("<!-- hypothesis-anchor: IDEA-000 -->"),
        "hypothesis-anchor must live in IDEA-000 detail file, not the hub"
    );

    let mut disk_files = BTreeSet::new();
    for entry in fs::read_dir(&ideas_dir).expect("read setup-ideas") {
        let entry = entry.expect("dirent");
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with("IDEA-") || !name.ends_with(".md") {
            continue;
        }
        disk_files.insert(name.to_string());
        let text = read(&entry.path());
        let fm = frontmatter(&text);
        let id = fm_line(fm, "id").unwrap_or_else(|| panic!("{name} missing id:"));
        assert!(
            name.starts_with(id),
            "{name} frontmatter id {id} does not match filename"
        );
        assert!(
            index_ids.contains(id),
            "{name} is not listed in docs/setup-ideas/index.md"
        );
        let status = fm_line(fm, "status").unwrap_or_else(|| panic!("{name} missing status:"));
        assert!(
            ALLOWED_STATUS.contains(&status),
            "{name} status {status:?} is not in the template vocabulary"
        );
        let anchor = fm_line(fm, "hypothesisAnchor")
            .unwrap_or_else(|| panic!("{name} missing hypothesisAnchor:"));
        if id == "IDEA-000" {
            assert_eq!(anchor, "true", "IDEA-000 must set hypothesisAnchor: true");
            assert!(
                text.contains("<!-- hypothesis-anchor: IDEA-000 -->"),
                "IDEA-000 must keep the hypothesis-anchor comment"
            );
            assert!(
                text.contains("```json"),
                "IDEA-000 must keep the typed hypothesis JSON example"
            );
        } else {
            assert_eq!(anchor, "false", "{id} must set hypothesisAnchor: false");
            assert!(
                !text.contains("<!-- hypothesis-anchor:"),
                "{id} must not carry a hypothesis-anchor comment"
            );
        }
        if let Some(related) = fm_line(fm, "related") {
            for other in related
                .trim_start_matches('[')
                .trim_end_matches(']')
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                assert!(
                    index_ids.contains(other),
                    "{id} related {other} is not in the index"
                );
            }
        }
    }

    let indexed_files: BTreeSet<String> = rows.into_iter().map(|(_, file)| file).collect();
    assert_eq!(
        disk_files, indexed_files,
        "docs/setup-ideas/ IDEA files must match the index exactly"
    );
}
