//! Engine bind-address helpers (mode enum lives in `catalog::EngineMode`).

use serde::Deserialize;
use std::path::PathBuf;

use crate::catalog::EngineMode;
use crate::feed::default_config_path;

/// Default TCP bind for the read-only engine state socket (localhost only).
pub const ENGINE_DEFAULT_BIND: &str = "127.0.0.1:17843";

/// Default SQLite path shared with MCP (`~/.the-desk/data.db`).
pub fn default_engine_database_path() -> PathBuf {
    let home = std::env::var("USERPROFILE")
        .or_else(|_| std::env::var("HOME"))
        .unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join(".the-desk").join("data.db")
}

#[derive(Debug, Deserialize, Default)]
struct RootEngineBind {
    #[serde(default)]
    sil: SilBindSection,
}

#[derive(Debug, Deserialize, Default)]
struct SilBindSection {
    #[serde(default)]
    engine_bind: Option<String>,
}

/// Resolve the engine socket bind/connect address from config (or default).
pub fn load_engine_bind_addr() -> String {
    let path = default_config_path();
    match std::fs::read_to_string(path) {
        Ok(content) => toml::from_str::<RootEngineBind>(&content)
            .ok()
            .and_then(|cfg| cfg.sil.engine_bind)
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| ENGINE_DEFAULT_BIND.to_string()),
        Err(_) => ENGINE_DEFAULT_BIND.to_string(),
    }
}

/// Convenience: read engine mode from an already-loaded SIL config.
pub fn engine_mode_from_sil(sil: &crate::catalog::SilConfig) -> EngineMode {
    sil.engine_mode
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_bind_is_localhost() {
        assert!(ENGINE_DEFAULT_BIND.starts_with("127.0.0.1:"));
    }

    #[test]
    fn engine_mode_defaults_embedded() {
        assert_eq!(EngineMode::default(), EngineMode::Embedded);
    }
}
