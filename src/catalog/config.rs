//! SIL feature flags loaded from `~/.the-desk/config.toml`.

use crate::feed::default_config_path;
use serde::{Deserialize, Serialize};

/// SIL (Sierra Intelligence Layer) configuration.
///
/// Default-off: when `catalog_discovery` is false, discovery / read-kernel tools
/// are omitted from the MCP router and today's 121-tool surface is unchanged.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct SilConfig {
    /// When true, register catalog discovery + read-kernel operators
    /// (`describe_environment`, `describe_domain`, `search_catalog`,
    /// `get_state`, `get_events`) on the MCP tool router.
    #[serde(default)]
    pub catalog_discovery: bool,
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
    }
}
