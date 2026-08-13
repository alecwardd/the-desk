//! `SourceProvider` seam: FileProvider (.scid/.depth) + stubbed SierraProvider.
//!
//! ACSIL (`SierraProvider`) stays stubbed until #23. FileProvider is the real
//! adapter for Sierra Chart file-spine ingest.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::depth::DepthReader;
use crate::feed::scid_reader::{ScidError, ScidReader, ScidTick};
use crate::feed::{resolve_contract_metadata, FeedConfig, TradeSide};

/// Kind of market-data source behind the engine host.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SourceProviderKind {
    /// Sierra Chart `.scid` / `.depth` files on disk.
    File,
    /// Future ACSIL / Sierra Chart live bridge (stubbed in M2a).
    Sierra,
}

/// Normalized trade tick emitted by a [`SourceProvider`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceTick {
    pub timestamp_ms: f64,
    pub price: f64,
    pub volume: f64,
    pub bid: f64,
    pub ask: f64,
    pub side: TradeSide,
    /// MarketRouter root when known (`NQ` / `ES`). Optional for single-host paths.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_symbol: Option<String>,
}

impl From<ScidTick> for SourceTick {
    fn from(t: ScidTick) -> Self {
        Self {
            timestamp_ms: t.timestamp_ms,
            price: t.price,
            volume: t.volume,
            bid: t.bid,
            ask: t.ask,
            side: t.side,
            root_symbol: None,
        }
    }
}

#[derive(Debug, Error)]
pub enum SourceError {
    #[error("scid error: {0}")]
    Scid(#[from] ScidError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("SierraProvider is stubbed (ACSIL is later work); use FileProvider for .scid/.depth")]
    SierraStubbed,
    #[error("source provider error: {0}")]
    Other(String),
}

/// Observability snapshot for a source adapter (ops / agents).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SourceHealth {
    pub provider_kind: SourceProviderKind,
    pub scid_path: Option<String>,
    pub scid_exists: bool,
    pub scid_file_len_bytes: u64,
    pub scid_read_offset_bytes: u64,
    pub depth_file_count: usize,
    pub depth_paths: Vec<String>,
    pub last_tick_timestamp_ms: Option<f64>,
    pub ticks_emitted: u64,
    pub stubbed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// Market-data provider seam used by the engine host.
pub trait SourceProvider: Send {
    fn provider_kind(&self) -> SourceProviderKind;

    /// Poll up to `max_ticks` new trades since the provider's read cursor.
    fn poll_ticks(&mut self, max_ticks: usize) -> Result<Vec<SourceTick>, SourceError>;

    /// Current source health / lag inputs for ops observability.
    fn health(&self) -> SourceHealth;

    /// Whether this provider is a non-functional stub.
    fn is_stubbed(&self) -> bool {
        false
    }
}

/// File-spine provider: owns Sierra Chart `.scid` + discovers `.depth` paths.
pub struct FileProvider {
    scid: ScidReader,
    depth_paths: Vec<PathBuf>,
    read_offset: u64,
    ticks_emitted: u64,
    last_tick_timestamp_ms: Option<f64>,
}

impl FileProvider {
    /// Build from feed config for one MarketRouter root (exact NQ or ES).
    ///
    /// Uses the same Sierra data dir as the live feed, with `base_symbol` pinned
    /// to the requested root so ES resolution cannot pick NQ (and vice versa).
    pub fn from_feed_config_for_root(config: &FeedConfig, root: super::root::RouterRoot) -> Self {
        let mut cfg = config.clone();
        cfg.base_symbol = root.as_str().to_string();
        cfg.symbol = root.as_str().to_string();
        cfg.active_symbol_override = None;
        Self::from_feed_config(&cfg)
    }

    /// Build from feed config (resolves contract SCID path + depth files).
    pub fn from_feed_config(config: &FeedConfig) -> Self {
        let contract = resolve_contract_metadata(config);
        let scid = ScidReader::from_feed_config(config);
        let depth_paths = DepthReader::list_symbol_depth_files(config).unwrap_or_default();
        // Start at EOF for live tail; callers that want warm replay reset via
        // [`Self::seek`].
        let read_offset = scid
            .current_aligned_end_offset()
            .unwrap_or(SCID_HEADER_SIZE as u64);
        let _ = contract;
        Self {
            scid,
            depth_paths,
            read_offset,
            ticks_emitted: 0,
            last_tick_timestamp_ms: None,
        }
    }

    /// Explicit paths (tests / fixtures). Starts at the first record (header end).
    pub fn from_paths(
        scid_path: impl Into<PathBuf>,
        depth_paths: Vec<PathBuf>,
        price_scale: f64,
    ) -> Self {
        let scid = ScidReader::with_price_scale(scid_path, price_scale);
        Self {
            scid,
            depth_paths,
            read_offset: SCID_HEADER_SIZE as u64,
            ticks_emitted: 0,
            last_tick_timestamp_ms: None,
        }
    }

    /// Seek the SCID read cursor (e.g. warm-replay cutover or test fixtures).
    pub fn seek(&mut self, offset: u64) {
        self.read_offset = offset.max(SCID_HEADER_SIZE as u64);
    }

    pub fn scid_path(&self) -> &Path {
        self.scid.path()
    }

    pub fn depth_paths(&self) -> &[PathBuf] {
        &self.depth_paths
    }

    pub fn read_offset(&self) -> u64 {
        self.read_offset
    }
}

const SCID_HEADER_SIZE: usize = 56;

impl SourceProvider for FileProvider {
    fn provider_kind(&self) -> SourceProviderKind {
        SourceProviderKind::File
    }

    fn poll_ticks(&mut self, max_ticks: usize) -> Result<Vec<SourceTick>, SourceError> {
        let max = max_ticks.max(1);
        let batch = self
            .scid
            .read_bulk_from_offset_capped(self.read_offset, max)?;
        self.read_offset = batch.next_offset;
        if let Some(last) = batch.ticks.last() {
            self.last_tick_timestamp_ms = Some(last.timestamp_ms);
        }
        self.ticks_emitted = self.ticks_emitted.saturating_add(batch.ticks.len() as u64);
        Ok(batch.ticks.into_iter().map(SourceTick::from).collect())
    }

    fn health(&self) -> SourceHealth {
        let meta = std::fs::metadata(self.scid.path()).ok();
        let exists = meta.is_some();
        let len = meta.map(|m| m.len()).unwrap_or(0);
        SourceHealth {
            provider_kind: SourceProviderKind::File,
            scid_path: Some(self.scid.path().display().to_string()),
            scid_exists: exists,
            scid_file_len_bytes: len,
            scid_read_offset_bytes: self.read_offset,
            depth_file_count: self.depth_paths.len(),
            depth_paths: self
                .depth_paths
                .iter()
                .map(|p| p.display().to_string())
                .collect(),
            last_tick_timestamp_ms: self.last_tick_timestamp_ms,
            ticks_emitted: self.ticks_emitted,
            stubbed: false,
            note: None,
        }
    }
}

/// Stubbed ACSIL / Sierra live provider slot (additive later; #23).
#[derive(Debug, Default)]
pub struct SierraProvider;

impl SourceProvider for SierraProvider {
    fn provider_kind(&self) -> SourceProviderKind {
        SourceProviderKind::Sierra
    }

    fn poll_ticks(&mut self, _max_ticks: usize) -> Result<Vec<SourceTick>, SourceError> {
        Err(SourceError::SierraStubbed)
    }

    fn health(&self) -> SourceHealth {
        SourceHealth {
            provider_kind: SourceProviderKind::Sierra,
            scid_path: None,
            scid_exists: false,
            scid_file_len_bytes: 0,
            scid_read_offset_bytes: 0,
            depth_file_count: 0,
            depth_paths: Vec::new(),
            last_tick_timestamp_ms: None,
            ticks_emitted: 0,
            stubbed: true,
            note: Some(
                "SierraProvider is stubbed pending ACSIL POC (#23); FileProvider owns .scid/.depth"
                    .into(),
            ),
        }
    }

    fn is_stubbed(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    const SC_TO_UNIX_EPOCH_US: i64 = 2_209_161_600_000_000;
    const SCID_RECORD_SIZE: usize = 40;

    fn write_scid(ticks: &[(f64, f64, f64)]) -> NamedTempFile {
        let mut file = NamedTempFile::new().expect("temp");
        let mut header = vec![0u8; SCID_HEADER_SIZE];
        header[0..4].copy_from_slice(b"SCID");
        header[4..8].copy_from_slice(&(SCID_HEADER_SIZE as u32).to_le_bytes());
        header[8..12].copy_from_slice(&(SCID_RECORD_SIZE as u32).to_le_bytes());
        file.write_all(&header).unwrap();
        for &(ts_ms, price, volume) in ticks {
            let mut rec = [0u8; SCID_RECORD_SIZE];
            let sc_us = (ts_ms * 1_000.0) as i64 + SC_TO_UNIX_EPOCH_US;
            rec[0..8].copy_from_slice(&sc_us.to_le_bytes());
            let ask = (price + 0.25) as f32;
            let bid = (price - 0.25) as f32;
            rec[12..16].copy_from_slice(&ask.to_le_bytes());
            rec[16..20].copy_from_slice(&bid.to_le_bytes());
            rec[20..24].copy_from_slice(&(price as f32).to_le_bytes());
            rec[28..32].copy_from_slice(&(volume as u32).to_le_bytes());
            rec[36..40].copy_from_slice(&1u32.to_le_bytes()); // ask vol → buy
            file.write_all(&rec).unwrap();
        }
        file.flush().unwrap();
        file
    }

    #[test]
    fn file_provider_reads_scid_and_reports_depth_paths() {
        let scid = write_scid(&[
            (1_700_000_000_000.0, 20_000.0, 1.0),
            (1_700_000_000_250.0, 20_000.25, 2.0),
        ]);
        let depth = NamedTempFile::new().unwrap();
        let mut provider =
            FileProvider::from_paths(scid.path(), vec![depth.path().to_path_buf()], 1.0);
        provider.seek(SCID_HEADER_SIZE as u64);
        let ticks = provider.poll_ticks(10).expect("poll");
        assert_eq!(ticks.len(), 2);
        assert_eq!(ticks[0].price, 20_000.0);
        let health = provider.health();
        assert_eq!(health.provider_kind, SourceProviderKind::File);
        assert!(health.scid_exists);
        assert_eq!(health.depth_file_count, 1);
        assert!(!health.stubbed);
    }

    #[test]
    fn sierra_provider_is_stubbed_and_errors_on_poll() {
        let mut p = SierraProvider;
        assert!(p.is_stubbed());
        assert!(matches!(p.poll_ticks(1), Err(SourceError::SierraStubbed)));
        assert!(p.health().stubbed);
    }

    #[test]
    fn providers_are_interchangeable_at_trait_seam() {
        let scid = write_scid(&[(1_700_000_000_000.0, 20_000.0, 1.0)]);
        let mut file: Box<dyn SourceProvider> =
            Box::new(FileProvider::from_paths(scid.path(), vec![], 1.0));
        assert_eq!(file.provider_kind(), SourceProviderKind::File);
        assert_eq!(file.poll_ticks(1).unwrap().len(), 1);

        let sierra: Box<dyn SourceProvider> = Box::new(SierraProvider);
        assert_eq!(sierra.provider_kind(), SourceProviderKind::Sierra);
        assert!(sierra.is_stubbed());
    }
}
