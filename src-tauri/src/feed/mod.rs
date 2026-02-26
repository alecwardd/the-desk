use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub mod scid_reader;

/// Side of a trade execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TradeSide {
    Buy,
    Sell,
    Unknown,
}

/// Unified event stream for all market-data sources.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FeedEvent {
    Connected,
    Disconnected,
    Trade {
        symbol_id: u32,
        price: f64,
        volume: f64,
        side: TradeSide,
        timestamp: f64,
    },
    Quote {
        symbol_id: u32,
        bid: f64,
        ask: f64,
        bid_size: f64,
        ask_size: f64,
        timestamp: f64,
    },
    Error {
        message: String,
    },
}

/// Runtime feed configuration loaded from `~/.the-desk/config.toml`.
/// Uses snake_case field names to match TOML convention.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FeedConfig {
    pub sierra_data_dir: String,
    pub symbol: String,
    pub flush_poll_ms: u64,
}

impl Default for FeedConfig {
    fn default() -> Self {
        Self {
            sierra_data_dir: "C:\\SierraChart\\Data".to_string(),
            symbol: "NQ".to_string(),
            flush_poll_ms: 1_000,
        }
    }
}

#[derive(Debug, Deserialize)]
struct RootConfig {
    #[serde(default)]
    feed: FeedConfig,
}

/// Resolve `~/.the-desk/config.toml` for feed startup.
pub fn default_config_path() -> PathBuf {
    let home = std::env::var("USERPROFILE")
        .or_else(|_| std::env::var("HOME"))
        .unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join(".the-desk").join("config.toml")
}

/// Load feed config from disk; fall back to defaults if missing/invalid.
pub fn load_feed_config() -> FeedConfig {
    let path = default_config_path();
    let raw = std::fs::read_to_string(path);
    match raw {
        Ok(content) => toml::from_str::<RootConfig>(&content)
            .map(|cfg| cfg.feed)
            .unwrap_or_default(),
        Err(_) => FeedConfig::default(),
    }
}
