//! SIL feature flags loaded from `~/.the-desk/config.toml`.

use crate::feed::default_config_path;
use serde::{Deserialize, Serialize};

/// How the MCP adapter obtains live coaching state (SIL-M2a).
///
/// - [`Embedded`](Self::Embedded): MCP owns ingest (today's topology / rollback).
/// - [`External`](Self::External): MCP is a thin client over `the-desk-engine`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EngineMode {
    /// MCP alone — no external engine process (true rollback).
    #[default]
    Embedded,
    /// MCP reads published state from the engine state socket.
    External,
}

impl EngineMode {
    /// Parse wire labels used in config.toml / tests.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "embedded" => Some(Self::Embedded),
            "external" | "socket" | "ipc" => Some(Self::External),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Embedded => "embedded",
            Self::External => "external",
        }
    }
}

/// SIL (Sierra Intelligence Layer) configuration.
///
/// Default-off: when `catalog_discovery` is false, discovery / read-kernel tools
/// are omitted from the MCP router and today's 123-tool surface is unchanged.
///
/// `engine_mode` defaults to [`EngineMode::Embedded`] — MCP owns ingest (today's
/// topology / true rollback). Set `external` to attach MCP as a thin adapter over
/// `the-desk-engine`'s read-only state socket.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct SilConfig {
    /// When true, register catalog discovery + read-kernel operators
    /// (`describe_environment`, `describe_domain`, `search_catalog`,
    /// `get_state`, `get_events`) on the MCP tool router.
    #[serde(default)]
    pub catalog_discovery: bool,
    /// Embedded (default) vs external engine topology (SIL-M2a).
    #[serde(default)]
    pub engine_mode: EngineMode,
    /// Optional override for the engine state socket (`host:port`).
    #[serde(default)]
    pub engine_bind: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct RootSilConfig {
    #[serde(default)]
    sil: SilConfig,
}

/// Load `[sil]` from the Desk config file; missing file / table → defaults (off).
pub fn load_sil_config() -> SilConfig {
    let path = default_config_path();
    match std::fs::read_to_string(path) {
        Ok(content) => toml::from_str::<RootSilConfig>(&content)
            .map(|cfg| cfg.sil)
            .unwrap_or_default(),
        Err(_) => SilConfig::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sil_config_defaults_discovery_off() {
        assert!(!SilConfig::default().catalog_discovery);
    }

    #[test]
    fn sil_config_parses_enabled_flag() {
        let raw = r#"
[sil]
catalog_discovery = true
"#;
        let parsed: RootSilConfig = toml::from_str(raw).expect("parse");
        assert!(parsed.sil.catalog_discovery);
        assert_eq!(parsed.sil.engine_mode, EngineMode::Embedded);
    }

    #[test]
    fn sil_config_parses_external_engine_mode() {
        let raw = r#"
[sil]
catalog_discovery = true
engine_mode = "external"
engine_bind = "127.0.0.1:19001"
"#;
        let parsed: RootSilConfig = toml::from_str(raw).expect("parse");
        assert_eq!(parsed.sil.engine_mode, EngineMode::External);
        assert_eq!(parsed.sil.engine_bind.as_deref(), Some("127.0.0.1:19001"));
    }
}
