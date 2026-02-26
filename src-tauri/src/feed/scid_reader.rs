use crate::feed::{FeedConfig, FeedEvent, TradeSide};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};

const SCID_HEADER_SIZE: usize = 56;
const SCID_RECORD_SIZE: usize = 40;
const SCID_MAGIC: &[u8; 4] = b"SCID";
const SC_TO_UNIX_EPOCH_US: i64 = 2_209_161_600_000_000;

#[derive(Debug, Error)]
pub enum ScidError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid scid header: {0}")]
    InvalidHeader(String),
}

#[derive(Debug, Clone)]
pub struct ScidTick {
    pub timestamp_ms: f64,
    pub price: f64,
    pub volume: f64,
    pub bid: f64,
    pub ask: f64,
    pub side: TradeSide,
}

#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
struct ScidHeader {
    header_size: u32,
    record_size: u32,
}

/// Reader for Sierra Chart `.scid` intraday tick files.
#[derive(Debug, Clone)]
pub struct ScidReader {
    path: PathBuf,
    /// Divisor for raw prices (e.g., 100.0 for Rithmic NQ data).
    price_scale: f64,
}

impl ScidReader {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            price_scale: 1.0,
        }
    }

    pub fn from_feed_config(config: &FeedConfig) -> Self {
        let path = PathBuf::from(&config.sierra_data_dir).join(symbol_to_scid_file(&config.symbol));
        Self {
            path,
            price_scale: config.price_scale,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn read_header(file: &mut File) -> Result<ScidHeader, ScidError> {
        let mut buf = [0_u8; SCID_HEADER_SIZE];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut buf)?;

        let magic = &buf[0..4];
        if magic != SCID_MAGIC {
            return Err(ScidError::InvalidHeader("missing SCID magic".to_string()));
        }
        let header_size = u32::from_le_bytes(buf[4..8].try_into().expect("slice len"));
        let record_size = u32::from_le_bytes(buf[8..12].try_into().expect("slice len"));
        if header_size as usize != SCID_HEADER_SIZE {
            return Err(ScidError::InvalidHeader(format!(
                "unexpected header size {header_size}"
            )));
        }
        if record_size as usize != SCID_RECORD_SIZE {
            return Err(ScidError::InvalidHeader(format!(
                "unexpected record size {record_size}"
            )));
        }
        Ok(ScidHeader {
            header_size,
            record_size,
        })
    }

    fn parse_record(&self, record: &[u8]) -> Option<ScidTick> {
        if record.len() < SCID_RECORD_SIZE {
            return None;
        }

        let sc_time_us = i64::from_le_bytes(record[0..8].try_into().ok()?);
        let raw_ask = f32::from_le_bytes(record[12..16].try_into().ok()?) as f64;
        let raw_bid = f32::from_le_bytes(record[16..20].try_into().ok()?) as f64;
        let raw_price = f32::from_le_bytes(record[20..24].try_into().ok()?) as f64;
        let volume = u32::from_le_bytes(record[28..32].try_into().ok()?) as f64;
        let bid_volume = u32::from_le_bytes(record[32..36].try_into().ok()?) as f64;
        let ask_volume = u32::from_le_bytes(record[36..40].try_into().ok()?) as f64;

        let s = self.price_scale;
        let price = if s > 1.0 { raw_price / s } else { raw_price };
        let bid = if s > 1.0 { raw_bid / s } else { raw_bid };
        let ask = if s > 1.0 { raw_ask / s } else { raw_ask };

        let unix_us = sc_time_us.saturating_sub(SC_TO_UNIX_EPOCH_US);
        let timestamp_ms = unix_us as f64 / 1_000.0;

        let side = if ask_volume > bid_volume {
            TradeSide::Buy
        } else if bid_volume > ask_volume {
            TradeSide::Sell
        } else if raw_price >= raw_ask && raw_ask > 0.0 {
            TradeSide::Buy
        } else if raw_price <= raw_bid && raw_bid > 0.0 {
            TradeSide::Sell
        } else {
            TradeSide::Unknown
        };

        Some(ScidTick {
            timestamp_ms,
            price,
            volume,
            bid,
            ask,
            side,
        })
    }

    /// Read an entire SCID file in-order for historical backfill.
    pub fn read_bulk(&self) -> Result<Vec<ScidTick>, ScidError> {
        self.read_bulk_since(None)
    }

    /// Read ticks from the SCID file, optionally starting from a minimum timestamp.
    ///
    /// If `since_ms` is provided, uses binary search on the sorted file to skip
    /// directly to the approximate position, avoiding a full sequential scan.
    pub fn read_bulk_since(&self, since_ms: Option<f64>) -> Result<Vec<ScidTick>, ScidError> {
        let mut file = File::open(&self.path)?;
        let header = Self::read_header(&mut file)?;
        let data_start = header.header_size as u64;
        let file_len = file.metadata()?.len();

        if file_len <= data_start {
            return Ok(Vec::new());
        }

        let total_records = (file_len - data_start) / SCID_RECORD_SIZE as u64;
        let start_record = match since_ms {
            Some(ts) => self.binary_search_record(&mut file, data_start, total_records, ts)?,
            None => 0,
        };

        let offset = data_start + start_record * SCID_RECORD_SIZE as u64;
        file.seek(SeekFrom::Start(offset))?;

        let estimated = (total_records - start_record) as usize;
        let mut out = Vec::with_capacity(estimated);
        let mut record = [0_u8; SCID_RECORD_SIZE];
        while file.read_exact(&mut record).is_ok() {
            if let Some(tick) = self.parse_record(&record) {
                if since_ms.is_none() || tick.timestamp_ms >= since_ms.unwrap_or(0.0) {
                    out.push(tick);
                }
            }
        }
        Ok(out)
    }

    /// Binary search for the first record at or after `target_ms`.
    fn binary_search_record(
        &self,
        file: &mut File,
        data_start: u64,
        total_records: u64,
        target_ms: f64,
    ) -> Result<u64, ScidError> {
        if total_records == 0 {
            return Ok(0);
        }
        let target_us = (target_ms * 1_000.0) as i64 + SC_TO_UNIX_EPOCH_US;
        let mut lo: u64 = 0;
        let mut hi: u64 = total_records;
        let mut buf = [0_u8; 8];

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let pos = data_start + mid * SCID_RECORD_SIZE as u64;
            file.seek(SeekFrom::Start(pos))?;
            file.read_exact(&mut buf)?;
            let sc_time_us = i64::from_le_bytes(buf);
            if sc_time_us < target_us {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    }

    /// Start a continuous tail loop over a live SCID file.
    pub fn spawn_tail_loop(
        &self,
        tx: broadcast::Sender<FeedEvent>,
        mut stop_rx: watch::Receiver<bool>,
        poll_ms: u64,
    ) -> JoinHandle<()> {
        let path = self.path.clone();
        let price_scale = self.price_scale;
        tokio::spawn(async move {
            let _ = tx.send(FeedEvent::Connected);

            let mut offset: u64 = 0;
            let mut header_checked = false;
            let poll = Duration::from_millis(poll_ms.max(100));

            loop {
                if *stop_rx.borrow() {
                    let _ = tx.send(FeedEvent::Disconnected);
                    break;
                }

                let mut file = match File::open(&path) {
                    Ok(f) => f,
                    Err(err) => {
                        let _ = tx.send(FeedEvent::Error {
                            message: format!("scid open failed: {err}"),
                        });
                        sleep(poll).await;
                        continue;
                    }
                };

                if !header_checked {
                    match Self::read_header(&mut file) {
                        Ok(h) => {
                            offset = h.header_size as u64;
                            header_checked = true;
                        }
                        Err(err) => {
                            let _ = tx.send(FeedEvent::Error {
                                message: err.to_string(),
                            });
                            sleep(poll).await;
                            continue;
                        }
                    }
                }

                let len = match file.metadata() {
                    Ok(m) => m.len(),
                    Err(_) => {
                        sleep(poll).await;
                        continue;
                    }
                };

                if len <= offset {
                    sleep(poll).await;
                    continue;
                }

                if file.seek(SeekFrom::Start(offset)).is_err() {
                    sleep(poll).await;
                    continue;
                }

                let mut record = [0_u8; SCID_RECORD_SIZE];
                while file.read_exact(&mut record).is_ok() {
                    offset = offset.saturating_add(SCID_RECORD_SIZE as u64);
                    if let Some(tick) = parse_record_scaled(&record, price_scale) {
                        let _ = tx.send(FeedEvent::Quote {
                            symbol_id: 1,
                            bid: tick.bid,
                            ask: tick.ask,
                            bid_size: 0.0,
                            ask_size: 0.0,
                            timestamp: tick.timestamp_ms,
                        });
                        let _ = tx.send(FeedEvent::Trade {
                            symbol_id: 1,
                            price: tick.price,
                            volume: tick.volume,
                            side: tick.side,
                            timestamp: tick.timestamp_ms,
                        });
                    }
                }

                tokio::select! {
                    _ = sleep(poll) => {}
                    _ = stop_rx.changed() => {}
                }
            }
        })
    }
}

/// Standalone record parser with explicit price scale (for use in spawned tasks).
pub fn parse_record_scaled(record: &[u8], price_scale: f64) -> Option<ScidTick> {
    if record.len() < SCID_RECORD_SIZE {
        return None;
    }

    let sc_time_us = i64::from_le_bytes(record[0..8].try_into().ok()?);
    let raw_ask = f32::from_le_bytes(record[12..16].try_into().ok()?) as f64;
    let raw_bid = f32::from_le_bytes(record[16..20].try_into().ok()?) as f64;
    let raw_price = f32::from_le_bytes(record[20..24].try_into().ok()?) as f64;
    let volume = u32::from_le_bytes(record[28..32].try_into().ok()?) as f64;
    let bid_volume = u32::from_le_bytes(record[32..36].try_into().ok()?) as f64;
    let ask_volume = u32::from_le_bytes(record[36..40].try_into().ok()?) as f64;

    let s = price_scale;
    let price = if s > 1.0 { raw_price / s } else { raw_price };
    let bid = if s > 1.0 { raw_bid / s } else { raw_bid };
    let ask = if s > 1.0 { raw_ask / s } else { raw_ask };

    let unix_us = sc_time_us.saturating_sub(SC_TO_UNIX_EPOCH_US);
    let timestamp_ms = unix_us as f64 / 1_000.0;

    let side = if ask_volume > bid_volume {
        TradeSide::Buy
    } else if bid_volume > ask_volume {
        TradeSide::Sell
    } else if raw_price >= raw_ask && raw_ask > 0.0 {
        TradeSide::Buy
    } else if raw_price <= raw_bid && raw_bid > 0.0 {
        TradeSide::Sell
    } else {
        TradeSide::Unknown
    };

    Some(ScidTick {
        timestamp_ms,
        price,
        volume,
        bid,
        ask,
        side,
    })
}

fn symbol_to_scid_file(symbol: &str) -> String {
    let trimmed = symbol.trim();
    if trimmed.to_ascii_lowercase().ends_with(".scid") {
        trimmed.to_string()
    } else {
        format!("{trimmed}.scid")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_scid(file: &mut NamedTempFile, prices: &[f32]) {
        let mut header = vec![0_u8; SCID_HEADER_SIZE];
        header[0..4].copy_from_slice(SCID_MAGIC);
        header[4..8].copy_from_slice(&(SCID_HEADER_SIZE as u32).to_le_bytes());
        header[8..12].copy_from_slice(&(SCID_RECORD_SIZE as u32).to_le_bytes());
        file.write_all(&header).expect("header");
        for (idx, p) in prices.iter().enumerate() {
            let mut rec = [0_u8; SCID_RECORD_SIZE];
            let sc_us = SC_TO_UNIX_EPOCH_US + ((1_700_000_000_000_i64 + idx as i64) * 1000);
            rec[0..8].copy_from_slice(&sc_us.to_le_bytes());
            rec[12..16].copy_from_slice(&(p + 0.25).to_le_bytes());
            rec[16..20].copy_from_slice(&(p - 0.25).to_le_bytes());
            rec[20..24].copy_from_slice(&p.to_le_bytes());
            rec[24..28].copy_from_slice(&(1_u32).to_le_bytes());
            rec[28..32].copy_from_slice(&(2_u32).to_le_bytes());
            rec[32..36].copy_from_slice(&(0_u32).to_le_bytes());
            rec[36..40].copy_from_slice(&(2_u32).to_le_bytes());
            file.write_all(&rec).expect("record");
        }
        file.flush().expect("flush");
    }

    #[test]
    fn bulk_reader_parses_ticks() {
        let mut file = NamedTempFile::new().expect("temp");
        write_scid(&mut file, &[21000.0, 21000.25]);
        let reader = ScidReader::new(file.path());
        let ticks = reader.read_bulk().expect("read");
        assert_eq!(ticks.len(), 2);
        assert_eq!(ticks[0].price, 21000.0);
        assert!(matches!(ticks[0].side, TradeSide::Buy));
    }
}
