use crate::feed::FeedConfig;
use crate::feed::TradeSide;
use crate::session_date_from_timestamp_ms;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use thiserror::Error;

const DEPTH_HEADER_SIZE: usize = 64;
const DEPTH_RECORD_SIZE: usize = 24;
const DEPTH_MAGIC: &[u8; 4] = b"SCDD";
const SC_TO_UNIX_EPOCH_US: i64 = 2_209_161_600_000_000;
const NQ_TICK_SIZE: f64 = 0.25;
const CLEAR_SEARCH_CHUNK_RECORDS: usize = 16_384;

#[derive(Debug, Error)]
pub enum DepthError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid depth header: {0}")]
    InvalidHeader(String),
    #[error("scan callback error: {0}")]
    Callback(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DepthSide {
    Bid,
    Ask,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DepthCommand {
    NoCommand,
    ClearBook,
    AddBidLevel,
    AddAskLevel,
    ModifyBidLevel,
    ModifyAskLevel,
    DeleteBidLevel,
    DeleteAskLevel,
}

impl DepthCommand {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::ClearBook,
            2 => Self::AddBidLevel,
            3 => Self::AddAskLevel,
            4 => Self::ModifyBidLevel,
            5 => Self::ModifyAskLevel,
            6 => Self::DeleteBidLevel,
            7 => Self::DeleteAskLevel,
            _ => Self::NoCommand,
        }
    }

    fn side(self) -> Option<DepthSide> {
        match self {
            Self::AddBidLevel | Self::ModifyBidLevel | Self::DeleteBidLevel => Some(DepthSide::Bid),
            Self::AddAskLevel | Self::ModifyAskLevel | Self::DeleteAskLevel => Some(DepthSide::Ask),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct DepthHeader {
    header_size: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DepthRecord {
    pub timestamp_ms: f64,
    pub command: DepthCommand,
    pub side: Option<DepthSide>,
    pub end_of_batch: bool,
    pub num_orders: u16,
    pub price: f64,
    pub quantity: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DomLevel {
    pub price: f64,
    pub quantity: u32,
    pub num_orders: u16,
    pub distance_from_touch_ticks: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DomSnapshot {
    pub source_file: String,
    pub snapshot_timestamp_ms: f64,
    pub session_date: String,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub spread_ticks: Option<i32>,
    pub touch_imbalance_ratio: Option<f64>,
    pub total_bid_levels: usize,
    pub total_ask_levels: usize,
    pub bids: Vec<DomLevel>,
    pub asks: Vec<DomLevel>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SideActivitySummary {
    pub add_events: u64,
    pub modify_up_events: u64,
    pub modify_down_events: u64,
    pub delete_events: u64,
    pub stacked_quantity: f64,
    pub removed_quantity: f64,
    pub estimated_filled_quantity: f64,
    pub estimated_pulled_quantity: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PriceActivitySummary {
    pub side: DepthSide,
    pub price: f64,
    pub stacked_quantity: f64,
    pub removed_quantity: f64,
    pub estimated_filled_quantity: f64,
    pub estimated_pulled_quantity: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PullStackActivitySummary {
    pub source_file: String,
    pub start_time_ms: f64,
    pub end_time_ms: f64,
    pub session_date: String,
    pub record_count: usize,
    pub batch_count: usize,
    pub bid: SideActivitySummary,
    pub ask: SideActivitySummary,
    pub top_pull_levels: Vec<PriceActivitySummary>,
    pub top_stack_levels: Vec<PriceActivitySummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct DomSummary {
    pub source_file: String,
    pub timestamp_ms: f64,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub spread_ticks: Option<i32>,
    pub touch_imbalance_ratio: Option<f64>,
    pub near_touch_bid_depth: f64,
    pub near_touch_ask_depth: f64,
    pub near_touch_depth_ratio: Option<f64>,
    pub bid_pull_rate: f64,
    pub ask_pull_rate: f64,
    pub stack_bias: f64,
    pub pull_stack_bias: f64,
    pub liquidity_bias: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DomFeatureSnapshot {
    pub source_file: String,
    pub timestamp_ms: f64,
    pub session_date: String,
    pub dom_summary: DomSummary,
    pub activity: PullStackActivitySummary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanControl {
    Continue,
    Stop,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ScanStats {
    pub estimated_records: usize,
    pub records_scanned: usize,
}

#[derive(Debug, Clone)]
pub struct DepthReader {
    path: PathBuf,
    price_scale: f64,
}

#[derive(Debug, Clone, Default)]
pub struct DepthBook {
    bids: BTreeMap<i64, LevelState>,
    asks: BTreeMap<i64, LevelState>,
}

#[derive(Debug, Clone, Copy, Default)]
struct LevelState {
    quantity: u32,
    num_orders: u16,
}

#[derive(Debug, Clone)]
struct MutablePriceActivity {
    side: DepthSide,
    key: i64,
    stacked_quantity: f64,
    removed_quantity: f64,
    estimated_filled_quantity: f64,
    estimated_pulled_quantity: f64,
}

impl DepthReader {
    /// Create a reader for a specific `.depth` file path.
    pub fn new(path: impl Into<PathBuf>, price_scale: f64) -> Self {
        Self {
            path: path.into(),
            price_scale,
        }
    }

    /// Path to the depth file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Offset to the first record after the fixed depth header.
    pub fn data_start_offset(&self) -> u64 {
        DEPTH_HEADER_SIZE as u64
    }

    /// Current file length for tail-loop callers.
    pub fn file_len(&self) -> Result<u64, DepthError> {
        Ok(std::fs::metadata(&self.path)?.len())
    }

    /// Read all depth records in the file.
    pub fn read_bulk(&self) -> Result<Vec<DepthRecord>, DepthError> {
        self.read_bulk_since(None)
    }

    /// Read depth records from the file starting at an optional minimum timestamp.
    pub fn read_bulk_since(&self, since_ms: Option<f64>) -> Result<Vec<DepthRecord>, DepthError> {
        let mut out = Vec::new();
        self.scan_range(since_ms, None, |record| {
            out.push(record);
            Ok(ScanControl::Continue)
        })?;
        Ok(out)
    }

    /// Discover `.depth` files in Sierra's `MarketDepthData` directory for the configured symbol.
    pub fn list_symbol_depth_files(config: &FeedConfig) -> Result<Vec<PathBuf>, DepthError> {
        let dir = PathBuf::from(&config.sierra_data_dir).join("MarketDepthData");
        if !dir.exists() {
            return Ok(Vec::new());
        }

        let prefix = format!("{}.", config.symbol.to_ascii_lowercase());
        let mut out = Vec::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|v| v.to_str()) else {
                continue;
            };
            let name_lower = name.to_ascii_lowercase();
            if name_lower.starts_with(&prefix) && name_lower.ends_with(".depth") {
                out.push(path);
            }
        }
        out.sort();
        Ok(out)
    }

    /// Choose the best depth file for a given timestamp by inspecting file time bounds.
    pub fn find_file_for_timestamp(
        config: &FeedConfig,
        timestamp_ms: f64,
    ) -> Result<Option<PathBuf>, DepthError> {
        let candidates = Self::list_symbol_depth_files(config)?;
        if candidates.is_empty() {
            return Ok(None);
        }

        let mut best: Option<(PathBuf, f64)> = None;
        for path in candidates {
            let reader = Self::new(&path, config.price_scale);
            let Ok((first_ms, last_ms)) = reader.time_bounds() else {
                continue;
            };
            if timestamp_ms >= first_ms && timestamp_ms <= last_ms {
                return Ok(Some(path));
            }
            let distance = if timestamp_ms < first_ms {
                first_ms - timestamp_ms
            } else {
                timestamp_ms - last_ms
            };
            match &best {
                Some((_, best_distance)) if *best_distance <= distance => {}
                _ => best = Some((path, distance)),
            }
        }
        Ok(best.map(|(path, _)| path))
    }

    /// Return the first and last timestamps in this depth file.
    pub fn time_bounds(&self) -> Result<(f64, f64), DepthError> {
        let mut file = File::open(&self.path)?;
        let header = Self::read_header(&mut file)?;
        let data_start = header.header_size as u64;
        let file_len = file.metadata()?.len();
        if file_len <= data_start {
            return Err(DepthError::InvalidHeader(
                "depth file has no records".to_string(),
            ));
        }
        let total_records = (file_len - data_start) / DEPTH_RECORD_SIZE as u64;
        let first = self.read_record_at(&mut file, data_start, 0)?;
        let last = self.read_record_at(&mut file, data_start, total_records.saturating_sub(1))?;
        Ok((first.timestamp_ms, last.timestamp_ms))
    }

    /// Scan depth records in time order without materializing the entire file.
    pub fn scan_range<F>(
        &self,
        start_ms: Option<f64>,
        end_ms_exclusive: Option<f64>,
        mut on_record: F,
    ) -> Result<ScanStats, DepthError>
    where
        F: FnMut(DepthRecord) -> Result<ScanControl, String>,
    {
        let mut file = File::open(&self.path)?;
        let header = Self::read_header(&mut file)?;
        let data_start = header.header_size as u64;
        let file_len = file.metadata()?.len();
        if file_len <= data_start {
            return Ok(ScanStats::default());
        }

        let total_records = (file_len - data_start) / DEPTH_RECORD_SIZE as u64;
        let start_record = if let Some(start_ms) = start_ms {
            self.binary_search_record(&mut file, data_start, total_records, start_ms)?
        } else {
            0
        };
        let end_record = if let Some(end_ms) = end_ms_exclusive {
            self.binary_search_record(&mut file, data_start, total_records, end_ms)?
        } else {
            total_records
        };

        let mut stats = ScanStats {
            estimated_records: end_record.saturating_sub(start_record) as usize,
            records_scanned: 0,
        };
        file.seek(SeekFrom::Start(
            data_start + start_record * DEPTH_RECORD_SIZE as u64,
        ))?;
        let mut buf = [0_u8; DEPTH_RECORD_SIZE];
        while file.read_exact(&mut buf).is_ok() {
            if let Some(record) = self.parse_record(&buf) {
                if start_ms.is_some() && record.timestamp_ms < start_ms.unwrap_or_default() {
                    continue;
                }
                if let Some(end_ms) = end_ms_exclusive {
                    if record.timestamp_ms >= end_ms {
                        break;
                    }
                }
                stats.records_scanned += 1;
                match on_record(record).map_err(DepthError::Callback)? {
                    ScanControl::Continue => {}
                    ScanControl::Stop => break,
                }
            }
        }

        Ok(stats)
    }

    /// Estimate how many records fall in a time range.
    pub fn estimate_range_records(
        &self,
        start_ms: Option<f64>,
        end_ms_exclusive: Option<f64>,
    ) -> Result<usize, DepthError> {
        let mut file = File::open(&self.path)?;
        let header = Self::read_header(&mut file)?;
        let data_start = header.header_size as u64;
        let file_len = file.metadata()?.len();
        if file_len <= data_start {
            return Ok(0);
        }
        let total_records = (file_len - data_start) / DEPTH_RECORD_SIZE as u64;
        let start_record = if let Some(start_ms) = start_ms {
            self.binary_search_record(&mut file, data_start, total_records, start_ms)?
        } else {
            0
        };
        let end_record = if let Some(end_ms) = end_ms_exclusive {
            self.binary_search_record(&mut file, data_start, total_records, end_ms)?
        } else {
            total_records
        };
        Ok(end_record.saturating_sub(start_record) as usize)
    }

    /// Scan newly appended records starting at an absolute file offset, updating the offset in place.
    pub fn scan_new_records<F>(
        &self,
        offset: &mut u64,
        mut on_record: F,
    ) -> Result<ScanStats, DepthError>
    where
        F: FnMut(DepthRecord) -> Result<ScanControl, String>,
    {
        let mut file = File::open(&self.path)?;
        let header = Self::read_header(&mut file)?;
        let data_start = header.header_size as u64;
        if *offset < data_start {
            *offset = data_start;
        }

        let len = file.metadata()?.len();
        if len <= *offset {
            return Ok(ScanStats::default());
        }
        file.seek(SeekFrom::Start(*offset))?;

        let mut stats = ScanStats {
            estimated_records: ((len - *offset) / DEPTH_RECORD_SIZE as u64) as usize,
            ..ScanStats::default()
        };
        let mut buf = [0_u8; DEPTH_RECORD_SIZE];
        while file.read_exact(&mut buf).is_ok() {
            *offset = offset.saturating_add(DEPTH_RECORD_SIZE as u64);
            if let Some(record) = self.parse_record(&buf) {
                stats.records_scanned += 1;
                match on_record(record).map_err(DepthError::Callback)? {
                    ScanControl::Continue => {}
                    ScanControl::Stop => break,
                }
            }
        }
        Ok(stats)
    }

    /// Reconstruct the DOM state at or immediately before `timestamp_ms`.
    pub fn snapshot_at(
        &self,
        timestamp_ms: f64,
        levels_per_side: usize,
    ) -> Result<DomSnapshot, DepthError> {
        let mut file = File::open(&self.path)?;
        let header = Self::read_header(&mut file)?;
        let data_start = header.header_size as u64;
        let file_len = file.metadata()?.len();
        let total_records = (file_len.saturating_sub(data_start)) / DEPTH_RECORD_SIZE as u64;
        if total_records == 0 {
            return Err(DepthError::InvalidHeader(
                "depth file has no records".to_string(),
            ));
        }

        let target_record =
            self.binary_search_record(&mut file, data_start, total_records, timestamp_ms)?;
        let clear_record = self.find_prior_clear_record(&mut file, data_start, target_record)?;

        let mut book = DepthBook::default();
        let mut last_ts = 0.0;
        let mut record_index = clear_record;
        while record_index < total_records {
            let record = self.read_record_at(&mut file, data_start, record_index)?;
            if record.timestamp_ms > timestamp_ms {
                break;
            }
            last_ts = record.timestamp_ms;
            book.apply(&record);
            record_index += 1;
        }

        Ok(book.snapshot(
            self.path.to_string_lossy().as_ref(),
            last_ts,
            levels_per_side,
        ))
    }

    /// Summarize stacking and pulling behavior in a time window.
    pub fn summarize_window(
        &self,
        start_time_ms: f64,
        end_time_ms: f64,
        trade_volume_by_level: &HashMap<(DepthSide, i64), f64>,
        price_low: Option<f64>,
        price_high: Option<f64>,
    ) -> Result<PullStackActivitySummary, DepthError> {
        let mut file = File::open(&self.path)?;
        let header = Self::read_header(&mut file)?;
        let data_start = header.header_size as u64;
        let file_len = file.metadata()?.len();
        let total_records = (file_len.saturating_sub(data_start)) / DEPTH_RECORD_SIZE as u64;
        if total_records == 0 {
            return Err(DepthError::InvalidHeader(
                "depth file has no records".to_string(),
            ));
        }

        let start_record =
            self.binary_search_record(&mut file, data_start, total_records, start_time_ms)?;
        let clear_record = self.find_prior_clear_record(&mut file, data_start, start_record)?;
        let mut book = DepthBook::default();
        let mut record_index = clear_record;

        while record_index < start_record {
            let record = self.read_record_at(&mut file, data_start, record_index)?;
            book.apply(&record);
            record_index += 1;
        }

        let mut remaining_trade_volume = trade_volume_by_level.clone();
        let mut bid = SideActivitySummary::default();
        let mut ask = SideActivitySummary::default();
        let mut by_level: HashMap<(DepthSide, i64), MutablePriceActivity> = HashMap::new();
        let mut record_count = 0usize;
        let mut batch_count = 0usize;

        while record_index < total_records {
            let record = self.read_record_at(&mut file, data_start, record_index)?;
            if record.timestamp_ms >= end_time_ms {
                break;
            }
            if record.timestamp_ms >= start_time_ms {
                let in_range = price_in_range(record.price, price_low, price_high);
                if in_range {
                    book.apply_with_activity(
                        &record,
                        &mut bid,
                        &mut ask,
                        &mut by_level,
                        &mut remaining_trade_volume,
                    );
                    record_count += 1;
                    if record.end_of_batch {
                        batch_count += 1;
                    }
                } else {
                    book.apply(&record);
                }
            } else {
                book.apply(&record);
            }
            record_index += 1;
        }

        let mut price_rows: Vec<_> = by_level.into_values().collect();
        price_rows.sort_by(|a, b| {
            b.estimated_pulled_quantity
                .partial_cmp(&a.estimated_pulled_quantity)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let top_pull_levels = price_rows
            .iter()
            .filter(|row| row.estimated_pulled_quantity > 0.0)
            .take(10)
            .map(|row| PriceActivitySummary {
                side: row.side,
                price: tick_key_to_price(row.key),
                stacked_quantity: row.stacked_quantity,
                removed_quantity: row.removed_quantity,
                estimated_filled_quantity: row.estimated_filled_quantity,
                estimated_pulled_quantity: row.estimated_pulled_quantity,
            })
            .collect();

        price_rows.sort_by(|a, b| {
            b.stacked_quantity
                .partial_cmp(&a.stacked_quantity)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let top_stack_levels = price_rows
            .iter()
            .filter(|row| row.stacked_quantity > 0.0)
            .take(10)
            .map(|row| PriceActivitySummary {
                side: row.side,
                price: tick_key_to_price(row.key),
                stacked_quantity: row.stacked_quantity,
                removed_quantity: row.removed_quantity,
                estimated_filled_quantity: row.estimated_filled_quantity,
                estimated_pulled_quantity: row.estimated_pulled_quantity,
            })
            .collect();

        Ok(PullStackActivitySummary {
            source_file: self.path.to_string_lossy().to_string(),
            start_time_ms,
            end_time_ms,
            session_date: session_date_from_timestamp_ms(start_time_ms),
            record_count,
            batch_count,
            bid,
            ask,
            top_pull_levels,
            top_stack_levels,
        })
    }

    fn read_header(file: &mut File) -> Result<DepthHeader, DepthError> {
        let mut buf = [0_u8; DEPTH_HEADER_SIZE];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut buf)?;

        if &buf[0..4] != DEPTH_MAGIC {
            return Err(DepthError::InvalidHeader("missing SCDD magic".to_string()));
        }

        let header_size = u32::from_le_bytes(buf[4..8].try_into().expect("slice len"));
        let record_size = u32::from_le_bytes(buf[8..12].try_into().expect("slice len"));
        let _version = u32::from_le_bytes(buf[12..16].try_into().expect("slice len"));
        if header_size as usize != DEPTH_HEADER_SIZE {
            return Err(DepthError::InvalidHeader(format!(
                "unexpected header size {header_size}"
            )));
        }
        if record_size as usize != DEPTH_RECORD_SIZE {
            return Err(DepthError::InvalidHeader(format!(
                "unexpected record size {record_size}"
            )));
        }

        Ok(DepthHeader { header_size })
    }

    fn read_record_at(
        &self,
        file: &mut File,
        data_start: u64,
        record_index: u64,
    ) -> Result<DepthRecord, DepthError> {
        let offset = data_start + record_index * DEPTH_RECORD_SIZE as u64;
        let mut buf = [0_u8; DEPTH_RECORD_SIZE];
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut buf)?;
        self.parse_record(&buf).ok_or_else(|| {
            DepthError::InvalidHeader(format!(
                "unable to parse depth record at index {record_index}"
            ))
        })
    }

    fn parse_record(&self, record: &[u8]) -> Option<DepthRecord> {
        if record.len() < DEPTH_RECORD_SIZE {
            return None;
        }

        let sc_time_us = i64::from_le_bytes(record[0..8].try_into().ok()?);
        let command = DepthCommand::from_u8(record[8]);
        let flags = record[9];
        let num_orders = u16::from_le_bytes(record[10..12].try_into().ok()?);
        let raw_price = f32::from_le_bytes(record[12..16].try_into().ok()?) as f64;
        let quantity = u32::from_le_bytes(record[16..20].try_into().ok()?);

        let unix_us = sc_time_us.saturating_sub(SC_TO_UNIX_EPOCH_US);
        let timestamp_ms = unix_us as f64 / 1_000.0;
        let price = if self.price_scale > 1.0 {
            raw_price / self.price_scale
        } else {
            raw_price
        };

        Some(DepthRecord {
            timestamp_ms,
            command,
            side: command.side(),
            end_of_batch: flags & 0x01 != 0,
            num_orders,
            price,
            quantity,
        })
    }

    fn binary_search_record(
        &self,
        file: &mut File,
        data_start: u64,
        total_records: u64,
        target_ms: f64,
    ) -> Result<u64, DepthError> {
        let mut lo = 0_u64;
        let mut hi = total_records;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let record = self.read_record_at(file, data_start, mid)?;
            if record.timestamp_ms < target_ms {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo.min(total_records.saturating_sub(1)))
    }

    fn find_prior_clear_record(
        &self,
        file: &mut File,
        data_start: u64,
        target_record: u64,
    ) -> Result<u64, DepthError> {
        let mut end = target_record.saturating_add(1);
        while end > 0 {
            let chunk_start = end.saturating_sub(CLEAR_SEARCH_CHUNK_RECORDS as u64);
            let chunk_len = (end - chunk_start) as usize;
            let offset = data_start + chunk_start * DEPTH_RECORD_SIZE as u64;
            let mut bytes = vec![0_u8; chunk_len * DEPTH_RECORD_SIZE];
            file.seek(SeekFrom::Start(offset))?;
            file.read_exact(&mut bytes)?;
            for idx_in_chunk in (0..chunk_len).rev() {
                let start = idx_in_chunk * DEPTH_RECORD_SIZE;
                if let Some(record) = self.parse_record(&bytes[start..start + DEPTH_RECORD_SIZE]) {
                    if record.command == DepthCommand::ClearBook {
                        return Ok(chunk_start + idx_in_chunk as u64);
                    }
                }
            }
            if chunk_start == 0 {
                break;
            }
            end = chunk_start;
        }
        Ok(0)
    }
}

impl DepthBook {
    pub fn apply(&mut self, record: &DepthRecord) {
        match record.command {
            DepthCommand::ClearBook => {
                self.bids.clear();
                self.asks.clear();
            }
            DepthCommand::AddBidLevel | DepthCommand::ModifyBidLevel => {
                let key = price_to_tick_key(record.price);
                self.bids.insert(
                    key,
                    LevelState {
                        quantity: record.quantity,
                        num_orders: record.num_orders,
                    },
                );
            }
            DepthCommand::AddAskLevel | DepthCommand::ModifyAskLevel => {
                let key = price_to_tick_key(record.price);
                self.asks.insert(
                    key,
                    LevelState {
                        quantity: record.quantity,
                        num_orders: record.num_orders,
                    },
                );
            }
            DepthCommand::DeleteBidLevel => {
                self.bids.remove(&price_to_tick_key(record.price));
            }
            DepthCommand::DeleteAskLevel => {
                self.asks.remove(&price_to_tick_key(record.price));
            }
            DepthCommand::NoCommand => {}
        }
    }

    fn apply_with_activity(
        &mut self,
        record: &DepthRecord,
        bid: &mut SideActivitySummary,
        ask: &mut SideActivitySummary,
        by_level: &mut HashMap<(DepthSide, i64), MutablePriceActivity>,
        remaining_trade_volume: &mut HashMap<(DepthSide, i64), f64>,
    ) {
        if record.command == DepthCommand::ClearBook {
            self.apply(record);
            return;
        }

        let Some(side) = record.side else {
            self.apply(record);
            return;
        };
        let key = price_to_tick_key(record.price);
        let old_state = self.level(side, key).copied().unwrap_or_default();
        let target = match side {
            DepthSide::Bid => bid,
            DepthSide::Ask => ask,
        };
        let row = by_level
            .entry((side, key))
            .or_insert_with(|| MutablePriceActivity {
                side,
                key,
                stacked_quantity: 0.0,
                removed_quantity: 0.0,
                estimated_filled_quantity: 0.0,
                estimated_pulled_quantity: 0.0,
            });

        match record.command {
            DepthCommand::AddBidLevel | DepthCommand::AddAskLevel => {
                let added = record.quantity as f64;
                target.add_events += 1;
                target.stacked_quantity += added;
                row.stacked_quantity += added;
            }
            DepthCommand::ModifyBidLevel | DepthCommand::ModifyAskLevel => {
                if record.quantity >= old_state.quantity {
                    let added = (record.quantity - old_state.quantity) as f64;
                    target.modify_up_events += 1;
                    target.stacked_quantity += added;
                    row.stacked_quantity += added;
                } else {
                    let removed = (old_state.quantity - record.quantity) as f64;
                    let estimated_filled =
                        estimate_fill_consumption(remaining_trade_volume, side, key, removed);
                    let estimated_pulled = (removed - estimated_filled).max(0.0);
                    target.modify_down_events += 1;
                    target.removed_quantity += removed;
                    target.estimated_filled_quantity += estimated_filled;
                    target.estimated_pulled_quantity += estimated_pulled;
                    row.removed_quantity += removed;
                    row.estimated_filled_quantity += estimated_filled;
                    row.estimated_pulled_quantity += estimated_pulled;
                }
            }
            DepthCommand::DeleteBidLevel | DepthCommand::DeleteAskLevel => {
                let removed = old_state.quantity as f64;
                let estimated_filled =
                    estimate_fill_consumption(remaining_trade_volume, side, key, removed);
                let estimated_pulled = (removed - estimated_filled).max(0.0);
                target.delete_events += 1;
                target.removed_quantity += removed;
                target.estimated_filled_quantity += estimated_filled;
                target.estimated_pulled_quantity += estimated_pulled;
                row.removed_quantity += removed;
                row.estimated_filled_quantity += estimated_filled;
                row.estimated_pulled_quantity += estimated_pulled;
            }
            DepthCommand::ClearBook | DepthCommand::NoCommand => {}
        }

        self.apply(record);
    }

    fn level(&self, side: DepthSide, key: i64) -> Option<&LevelState> {
        match side {
            DepthSide::Bid => self.bids.get(&key),
            DepthSide::Ask => self.asks.get(&key),
        }
    }

    pub fn snapshot(
        &self,
        source_file: &str,
        snapshot_timestamp_ms: f64,
        levels_per_side: usize,
    ) -> DomSnapshot {
        let best_bid_key = self.bids.keys().next_back().copied();
        let best_ask_key = self.asks.keys().next().copied();
        let best_bid = best_bid_key.map(tick_key_to_price);
        let best_ask = best_ask_key.map(tick_key_to_price);

        let bids = self
            .bids
            .iter()
            .rev()
            .take(levels_per_side)
            .map(|(key, level)| DomLevel {
                price: tick_key_to_price(*key),
                quantity: level.quantity,
                num_orders: level.num_orders,
                distance_from_touch_ticks: best_bid_key
                    .map(|best| (best - *key) as i32)
                    .unwrap_or_default(),
            })
            .collect::<Vec<_>>();

        let asks = self
            .asks
            .iter()
            .take(levels_per_side)
            .map(|(key, level)| DomLevel {
                price: tick_key_to_price(*key),
                quantity: level.quantity,
                num_orders: level.num_orders,
                distance_from_touch_ticks: best_ask_key
                    .map(|best| (*key - best) as i32)
                    .unwrap_or_default(),
            })
            .collect::<Vec<_>>();

        let spread_ticks = match (best_bid_key, best_ask_key) {
            (Some(bid_key), Some(ask_key)) => Some((ask_key - bid_key) as i32),
            _ => None,
        };
        let touch_imbalance_ratio = match (bids.first(), asks.first()) {
            (Some(bid), Some(ask)) if ask.quantity > 0 => {
                Some(bid.quantity as f64 / ask.quantity as f64)
            }
            _ => None,
        };

        DomSnapshot {
            source_file: source_file.to_string(),
            snapshot_timestamp_ms,
            session_date: session_date_from_timestamp_ms(snapshot_timestamp_ms),
            best_bid,
            best_ask,
            spread_ticks,
            touch_imbalance_ratio,
            total_bid_levels: self.bids.len(),
            total_ask_levels: self.asks.len(),
            bids,
            asks,
        }
    }
}

pub fn build_dom_summary(
    snapshot: &DomSnapshot,
    activity: &PullStackActivitySummary,
) -> DomSummary {
    let near_touch_bid_depth: f64 = snapshot
        .bids
        .iter()
        .take(3)
        .map(|level| level.quantity as f64)
        .sum();
    let near_touch_ask_depth: f64 = snapshot
        .asks
        .iter()
        .take(3)
        .map(|level| level.quantity as f64)
        .sum();
    let near_touch_depth_ratio = if near_touch_ask_depth > 0.0 {
        Some(near_touch_bid_depth / near_touch_ask_depth)
    } else {
        None
    };
    let bid_pull_rate = if activity.bid.removed_quantity > 0.0 {
        activity.bid.estimated_pulled_quantity / activity.bid.removed_quantity
    } else {
        0.0
    };
    let ask_pull_rate = if activity.ask.removed_quantity > 0.0 {
        activity.ask.estimated_pulled_quantity / activity.ask.removed_quantity
    } else {
        0.0
    };
    let stack_bias = if (activity.bid.stacked_quantity + activity.ask.stacked_quantity) > 0.0 {
        (activity.bid.stacked_quantity - activity.ask.stacked_quantity)
            / (activity.bid.stacked_quantity + activity.ask.stacked_quantity)
    } else {
        0.0
    };
    let pull_stack_bias = (activity.bid.stacked_quantity - activity.bid.estimated_pulled_quantity)
        - (activity.ask.stacked_quantity - activity.ask.estimated_pulled_quantity);
    let liquidity_bias = if pull_stack_bias > 25.0 || near_touch_depth_ratio.unwrap_or(1.0) > 1.2 {
        "bid_support".to_string()
    } else if pull_stack_bias < -25.0 || near_touch_depth_ratio.unwrap_or(1.0) < 0.8 {
        "ask_resistance".to_string()
    } else {
        "balanced".to_string()
    };

    DomSummary {
        source_file: snapshot.source_file.clone(),
        timestamp_ms: snapshot.snapshot_timestamp_ms,
        best_bid: snapshot.best_bid,
        best_ask: snapshot.best_ask,
        spread_ticks: snapshot.spread_ticks,
        touch_imbalance_ratio: snapshot.touch_imbalance_ratio,
        near_touch_bid_depth,
        near_touch_ask_depth,
        near_touch_depth_ratio,
        bid_pull_rate,
        ask_pull_rate,
        stack_bias,
        pull_stack_bias,
        liquidity_bias,
    }
}

pub fn build_dom_feature_snapshot(
    snapshot: &DomSnapshot,
    activity: PullStackActivitySummary,
) -> DomFeatureSnapshot {
    DomFeatureSnapshot {
        source_file: snapshot.source_file.clone(),
        timestamp_ms: snapshot.snapshot_timestamp_ms,
        session_date: snapshot.session_date.clone(),
        dom_summary: build_dom_summary(snapshot, &activity),
        activity,
    }
}

fn estimate_fill_consumption(
    remaining_trade_volume: &mut HashMap<(DepthSide, i64), f64>,
    side: DepthSide,
    key: i64,
    removed: f64,
) -> f64 {
    if removed <= 0.0 {
        return 0.0;
    }
    let Some(remaining) = remaining_trade_volume.get_mut(&(side, key)) else {
        return 0.0;
    };
    let consumed = (*remaining).min(removed);
    *remaining -= consumed;
    consumed
}

fn price_to_tick_key(price: f64) -> i64 {
    (price / NQ_TICK_SIZE).round() as i64
}

fn tick_key_to_price(key: i64) -> f64 {
    key as f64 * NQ_TICK_SIZE
}

fn price_in_range(price: f64, price_low: Option<f64>, price_high: Option<f64>) -> bool {
    if let Some(low) = price_low {
        if price < low {
            return false;
        }
    }
    if let Some(high) = price_high {
        if price > high {
            return false;
        }
    }
    true
}

/// Aggregate `.scid` trade volume by side and price level for a window so DOM decreases can be
/// loosely classified as likely fills versus likely pulls.
pub fn aggregate_trade_volume_by_level(
    trades: impl IntoIterator<Item = (f64, TradeSide, f64)>,
) -> HashMap<(DepthSide, i64), f64> {
    let mut out = HashMap::new();
    for (price, side, volume) in trades {
        let depth_side = match side {
            TradeSide::Buy => Some(DepthSide::Ask),
            TradeSide::Sell => Some(DepthSide::Bid),
            TradeSide::Unknown => None,
        };
        let Some(depth_side) = depth_side else {
            continue;
        };
        let key = price_to_tick_key(price);
        *out.entry((depth_side, key)).or_insert(0.0) += volume;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn write_test_depth_file(path: &Path, records: &[(i64, u8, u8, u16, f32, u32)]) {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(DEPTH_MAGIC);
        bytes.extend_from_slice(&(DEPTH_HEADER_SIZE as u32).to_le_bytes());
        bytes.extend_from_slice(&(DEPTH_RECORD_SIZE as u32).to_le_bytes());
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        bytes.extend_from_slice(&[0_u8; DEPTH_HEADER_SIZE - 16]);
        for (dt, cmd, flags, num_orders, price, qty) in records {
            bytes.extend_from_slice(&dt.to_le_bytes());
            bytes.push(*cmd);
            bytes.push(*flags);
            bytes.extend_from_slice(&num_orders.to_le_bytes());
            bytes.extend_from_slice(&price.to_le_bytes());
            bytes.extend_from_slice(&qty.to_le_bytes());
            bytes.extend_from_slice(&0_u32.to_le_bytes());
        }
        std::fs::write(path, bytes).expect("write depth");
    }

    fn unix_ms_to_sc(us_ms: i64) -> i64 {
        us_ms * 1_000 + SC_TO_UNIX_EPOCH_US
    }

    #[test]
    fn parses_snapshot_at_time() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("NQ.depth");
        write_test_depth_file(
            &path,
            &[
                (unix_ms_to_sc(1_000), 1, 0, 0, 0.0, 0),
                (unix_ms_to_sc(1_000), 2, 0, 1, 100.0, 10),
                (unix_ms_to_sc(1_000), 3, 1, 1, 101.0, 12),
                (unix_ms_to_sc(1_500), 4, 0, 1, 100.0, 8),
                (unix_ms_to_sc(1_500), 5, 1, 1, 101.0, 15),
            ],
        );

        let reader = DepthReader::new(path, 1.0);
        let snap = reader.snapshot_at(1_500.0, 2).expect("snapshot");
        assert_eq!(snap.best_bid, Some(100.0));
        assert_eq!(snap.best_ask, Some(101.0));
        assert_eq!(snap.bids[0].quantity, 8);
        assert_eq!(snap.asks[0].quantity, 15);
    }

    #[test]
    fn summarizes_pulls_and_stacks() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("NQ.depth");
        write_test_depth_file(
            &path,
            &[
                (unix_ms_to_sc(1_000), 1, 0, 0, 0.0, 0),
                (unix_ms_to_sc(1_000), 2, 0, 1, 100.0, 10),
                (unix_ms_to_sc(1_000), 3, 1, 1, 101.0, 12),
                (unix_ms_to_sc(2_000), 4, 0, 1, 100.0, 15),
                (unix_ms_to_sc(2_000), 5, 0, 1, 101.0, 9),
                (unix_ms_to_sc(2_100), 6, 1, 0, 100.0, 0),
            ],
        );
        let reader = DepthReader::new(path, 1.0);
        let trades = aggregate_trade_volume_by_level([(101.0, TradeSide::Buy, 2.0)]);
        let summary = reader
            .summarize_window(2_000.0, 2_200.0, &trades, None, None)
            .expect("summary");
        assert_eq!(summary.ask.estimated_filled_quantity, 2.0);
        assert_eq!(summary.ask.estimated_pulled_quantity, 1.0);
        assert_eq!(summary.bid.stacked_quantity, 5.0);
        assert_eq!(summary.bid.estimated_pulled_quantity, 15.0);
    }

    #[test]
    fn aggregates_trade_volume_by_level() {
        let agg = aggregate_trade_volume_by_level([
            (100.0, TradeSide::Sell, 3.0),
            (100.0, TradeSide::Sell, 2.0),
            (101.0, TradeSide::Buy, 4.0),
            (102.0, TradeSide::Unknown, 99.0),
        ]);
        assert_eq!(
            agg.get(&(DepthSide::Bid, price_to_tick_key(100.0))),
            Some(&5.0)
        );
        assert_eq!(
            agg.get(&(DepthSide::Ask, price_to_tick_key(101.0))),
            Some(&4.0)
        );
    }

    #[test]
    fn scan_range_respects_bounds() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("NQ.depth");
        write_test_depth_file(
            &path,
            &[
                (unix_ms_to_sc(1_000), 1, 0, 0, 0.0, 0),
                (unix_ms_to_sc(1_000), 2, 0, 1, 100.0, 10),
                (unix_ms_to_sc(2_000), 3, 1, 1, 101.0, 12),
                (unix_ms_to_sc(3_000), 4, 0, 1, 100.0, 8),
            ],
        );
        let reader = DepthReader::new(path, 1.0);
        let mut timestamps = Vec::new();
        let stats = reader
            .scan_range(Some(1_500.0), Some(3_000.0), |record| {
                timestamps.push(record.timestamp_ms);
                Ok(ScanControl::Continue)
            })
            .expect("scan");
        assert_eq!(timestamps, vec![2_000.0]);
        assert_eq!(stats.records_scanned, 1);
    }

    #[test]
    fn scan_new_records_tails_appends() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("NQ.depth");
        write_test_depth_file(
            &path,
            &[
                (unix_ms_to_sc(1_000), 1, 0, 0, 0.0, 0),
                (unix_ms_to_sc(1_000), 2, 1, 1, 100.0, 10),
            ],
        );
        let reader = DepthReader::new(&path, 1.0);
        let mut offset = reader.data_start_offset();
        let mut first_pass = Vec::new();
        reader
            .scan_new_records(&mut offset, |record| {
                first_pass.push(record.timestamp_ms);
                Ok(ScanControl::Continue)
            })
            .expect("first scan");
        assert_eq!(first_pass.len(), 2);

        let mut bytes = std::fs::read(&path).expect("read");
        for (dt, cmd, flags, num_orders, price, qty) in [
            (unix_ms_to_sc(2_000), 3_u8, 0_u8, 1_u16, 101.0_f32, 12_u32),
            (unix_ms_to_sc(2_100), 5_u8, 1_u8, 1_u16, 101.0_f32, 15_u32),
        ] {
            bytes.extend_from_slice(&dt.to_le_bytes());
            bytes.push(cmd);
            bytes.push(flags);
            bytes.extend_from_slice(&num_orders.to_le_bytes());
            bytes.extend_from_slice(&price.to_le_bytes());
            bytes.extend_from_slice(&qty.to_le_bytes());
            bytes.extend_from_slice(&0_u32.to_le_bytes());
        }
        std::fs::write(&path, bytes).expect("append");

        let mut second_pass = Vec::new();
        reader
            .scan_new_records(&mut offset, |record| {
                second_pass.push(record.timestamp_ms);
                Ok(ScanControl::Continue)
            })
            .expect("second scan");
        assert_eq!(second_pass, vec![2_000.0, 2_100.0]);
    }

    #[test]
    fn build_dom_summary_uses_snapshot_and_activity() {
        let snapshot = DomSnapshot {
            source_file: "test.depth".into(),
            snapshot_timestamp_ms: 2_000.0,
            session_date: "2026-03-05".into(),
            best_bid: Some(100.0),
            best_ask: Some(100.25),
            spread_ticks: Some(1),
            touch_imbalance_ratio: Some(1.5),
            total_bid_levels: 2,
            total_ask_levels: 2,
            bids: vec![
                DomLevel {
                    price: 100.0,
                    quantity: 12,
                    num_orders: 1,
                    distance_from_touch_ticks: 0,
                },
                DomLevel {
                    price: 99.75,
                    quantity: 8,
                    num_orders: 1,
                    distance_from_touch_ticks: 1,
                },
            ],
            asks: vec![
                DomLevel {
                    price: 100.25,
                    quantity: 10,
                    num_orders: 1,
                    distance_from_touch_ticks: 0,
                },
                DomLevel {
                    price: 100.5,
                    quantity: 6,
                    num_orders: 1,
                    distance_from_touch_ticks: 1,
                },
            ],
        };
        let activity = PullStackActivitySummary {
            source_file: "test.depth".into(),
            start_time_ms: 1_000.0,
            end_time_ms: 2_000.0,
            session_date: "2026-03-05".into(),
            record_count: 4,
            batch_count: 2,
            bid: SideActivitySummary {
                stacked_quantity: 20.0,
                removed_quantity: 10.0,
                estimated_pulled_quantity: 4.0,
                ..Default::default()
            },
            ask: SideActivitySummary {
                stacked_quantity: 8.0,
                removed_quantity: 12.0,
                estimated_pulled_quantity: 6.0,
                ..Default::default()
            },
            top_pull_levels: Vec::new(),
            top_stack_levels: Vec::new(),
        };
        let summary = build_dom_summary(&snapshot, &activity);
        assert_eq!(summary.near_touch_bid_depth, 20.0);
        assert_eq!(summary.near_touch_ask_depth, 16.0);
        assert_eq!(summary.liquidity_bias, "bid_support");
    }

    #[test]
    fn validates_real_sierra_depth_file_when_available() {
        let path = Path::new(r"T:\SierraChart\Data\MarketDepthData\NQH6.CME.2026-03-05.depth");
        if !path.exists() {
            return;
        }
        let config = crate::feed::load_feed_config();
        let reader = DepthReader::new(path, config.price_scale);
        let (first_ms, last_ms) = reader.time_bounds().expect("time bounds");
        assert!(last_ms >= first_ms);

        let snapshot = reader.snapshot_at(last_ms, 5).expect("snapshot");
        assert!(snapshot.best_bid.is_some() || snapshot.best_ask.is_some());

        let start_ms = (last_ms - 30_000.0).max(first_ms);
        let trades = HashMap::new();
        let summary = reader
            .summarize_window(start_ms, last_ms, &trades, None, None)
            .expect("summary");
        assert!(summary.record_count > 0 || summary.batch_count > 0);
    }
}
