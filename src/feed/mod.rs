use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub mod monotonic;
pub mod scid_reader;
pub mod symbol_resolution;

pub use symbol_resolution::{
    resolve_contract_metadata, resolve_contract_metadata_for_symbol, ContractMetadata, SymbolMode,
};

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
    /// Legacy symbol field preserved for backward compatibility.
    pub symbol: String,
    #[serde(default = "default_base_symbol")]
    pub base_symbol: String,
    #[serde(default)]
    pub symbol_mode: SymbolMode,
    #[serde(default)]
    pub active_symbol_override: Option<String>,
    pub flush_poll_ms: u64,
    /// Divisor applied to raw .scid prices. Rithmic stores NQ prices
    /// multiplied by 100 (e.g., 24966.75 → 2496675), so set this to 100.
    #[serde(default = "default_price_scale")]
    pub price_scale: f64,
    /// Maximum live SCID records to drain in one poll iteration.
    #[serde(default = "default_max_ticks_per_poll")]
    pub max_ticks_per_poll: usize,
    /// Minimum market-time interval between coalesced analysis passes.
    #[serde(default = "default_analysis_min_interval_ms")]
    pub analysis_min_interval_ms: f64,
    /// Force a coalesced analysis pass after this many ingested ticks.
    #[serde(default = "default_analysis_max_ticks")]
    pub analysis_max_ticks: usize,
}

fn default_price_scale() -> f64 {
    100.0
}

fn default_max_ticks_per_poll() -> usize {
    5_000
}

fn default_analysis_min_interval_ms() -> f64 {
    250.0
}

fn default_analysis_max_ticks() -> usize {
    500
}

fn default_base_symbol() -> String {
    "NQ".to_string()
}

impl Default for FeedConfig {
    fn default() -> Self {
        Self {
            sierra_data_dir: "C:\\SierraChart\\Data".to_string(),
            symbol: "NQ".to_string(),
            base_symbol: default_base_symbol(),
            symbol_mode: SymbolMode::Hybrid,
            active_symbol_override: None,
            flush_poll_ms: 1_000,
            price_scale: 100.0,
            max_ticks_per_poll: default_max_ticks_per_poll(),
            analysis_min_interval_ms: default_analysis_min_interval_ms(),
            analysis_max_ticks: default_analysis_max_ticks(),
        }
    }
}

impl FeedConfig {
    pub fn effective_configured_symbol(&self) -> String {
        let symbol = self.symbol.trim();
        if !symbol.is_empty() {
            symbol.to_string()
        } else {
            self.base_symbol.trim().to_string()
        }
    }
}

/// Storage tier lifecycle configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StorageConfig {
    /// Days to keep raw ticks in SQLite (warm tier). Default: 30.
    #[serde(default = "default_warm_days")]
    pub warm_retention_days: u32,
    /// Directory for zstd-compressed cold archives.
    #[serde(default = "default_archive_dir")]
    pub cold_archive_dir: String,
    /// Whether to auto-archive at session close.
    #[serde(default)]
    pub auto_archive: bool,
    /// Days to keep DOM `depth_events` in SQLite. Default: 7. The `.depth` source
    /// files remain the durable record, so older depth rows are re-ingestable.
    #[serde(default = "default_depth_retention_days")]
    pub depth_retention_days: u32,
}

fn default_warm_days() -> u32 {
    30
}

fn default_depth_retention_days() -> u32 {
    7
}

fn default_archive_dir() -> String {
    let home = std::env::var("USERPROFILE")
        .or_else(|_| std::env::var("HOME"))
        .unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home)
        .join(".the-desk")
        .join("archive")
        .to_string_lossy()
        .to_string()
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            warm_retention_days: 30,
            cold_archive_dir: default_archive_dir(),
            auto_archive: false,
            depth_retention_days: default_depth_retention_days(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct RootConfig {
    #[serde(default)]
    feed: FeedConfig,
    #[serde(default)]
    storage: StorageConfig,
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

/// Load storage config from disk; fall back to defaults if missing/invalid.
pub fn load_storage_config() -> StorageConfig {
    let path = default_config_path();
    let raw = std::fs::read_to_string(path);
    match raw {
        Ok(content) => toml::from_str::<RootConfig>(&content)
            .map(|cfg| cfg.storage)
            .unwrap_or_default(),
        Err(_) => StorageConfig::default(),
    }
}
