use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, Read, Write};
use thiserror::Error;

/// A single timestamped record within a session recording.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecordingEntry {
    /// Unix-epoch seconds (fractional) when this entry was captured.
    pub timestamp: f64,
    /// Entry kind (e.g. "trade", "quote").
    pub record_type: String,
    /// Arbitrary JSON payload for this entry.
    pub payload: serde_json::Value,
}

const MAGIC: &[u8; 4] = b"DESK";
const VERSION: u16 = 1;

#[derive(Debug, Error)]
pub enum RecordingError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Accumulates recording entries in memory and flushes them to a zstd-compressed file.
pub struct SessionRecorder {
    path: String,
    entries: Vec<RecordingEntry>,
}

impl SessionRecorder {
    /// Create recorder for a session file.
    pub fn new(path: String) -> Self {
        Self {
            path,
            entries: Vec::new(),
        }
    }

    /// Append in-memory record.
    pub fn push(&mut self, entry: RecordingEntry) {
        self.entries.push(entry);
    }

    /// Flush all records to a zstd-compressed binary file.
    pub fn flush(&self) -> Result<(), RecordingError> {
        let file = File::create(&self.path)?;
        let mut encoder = zstd::Encoder::new(file, 3)?;
        encoder.write_all(MAGIC)?;
        encoder.write_all(&VERSION.to_le_bytes())?;
        encoder.write_all(&(self.entries.len() as u32).to_le_bytes())?;
        for entry in &self.entries {
            encoder.write_all(&entry.timestamp.to_le_bytes())?;
            let record_type = entry.record_type.as_bytes();
            let payload = serde_json::to_vec(&entry.payload)?;
            encoder.write_all(&(record_type.len() as u16).to_le_bytes())?;
            encoder.write_all(record_type)?;
            encoder.write_all(&(payload.len() as u32).to_le_bytes())?;
            encoder.write_all(&payload)?;
        }
        encoder.finish()?;
        Ok(())
    }
}

/// Loads and decodes zstd-compressed session recordings for replay.
pub struct ReplayEngine;

impl ReplayEngine {
    /// Read compressed recording and return ordered entries.
    pub fn load(path: &str) -> Result<Vec<RecordingEntry>, RecordingError> {
        let file = File::open(path)?;
        let decoder = zstd::Decoder::new(file)?;
        let mut reader = BufReader::new(decoder);
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if &magic != MAGIC {
            return Err(RecordingError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid recording magic",
            )));
        }
        let mut version_bytes = [0u8; 2];
        reader.read_exact(&mut version_bytes)?;
        let _version = u16::from_le_bytes(version_bytes);
        let mut count_bytes = [0u8; 4];
        reader.read_exact(&mut count_bytes)?;
        let expected = u32::from_le_bytes(count_bytes) as usize;
        let mut entries = Vec::with_capacity(expected);
        for _ in 0..expected {
            let mut ts_bytes = [0u8; 8];
            reader.read_exact(&mut ts_bytes)?;
            let timestamp = f64::from_le_bytes(ts_bytes);
            let mut rt_len_bytes = [0u8; 2];
            reader.read_exact(&mut rt_len_bytes)?;
            let rt_len = u16::from_le_bytes(rt_len_bytes) as usize;
            let mut record_type_bytes = vec![0u8; rt_len];
            reader.read_exact(&mut record_type_bytes)?;
            let record_type = String::from_utf8(record_type_bytes).map_err(|err| {
                RecordingError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, err))
            })?;
            let mut payload_len_bytes = [0u8; 4];
            reader.read_exact(&mut payload_len_bytes)?;
            let payload_len = u32::from_le_bytes(payload_len_bytes) as usize;
            let mut payload_bytes = vec![0u8; payload_len];
            reader.read_exact(&mut payload_bytes)?;
            let payload = serde_json::from_slice::<serde_json::Value>(&payload_bytes)?;
            entries.push(RecordingEntry {
                timestamp,
                record_type,
                payload,
            });
        }
        entries.sort_by(|a, b| {
            a.timestamp
                .partial_cmp(&b.timestamp)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn round_trip_recording_replay() {
        let file = NamedTempFile::new().expect("temp");
        let path = file.path().to_string_lossy().to_string();
        let mut recorder = SessionRecorder::new(path.clone());
        recorder.push(RecordingEntry {
            timestamp: 1.0,
            record_type: "trade".to_string(),
            payload: serde_json::json!({"price": 21000.0}),
        });
        recorder.flush().expect("flush");
        let entries = ReplayEngine::load(&path).expect("load");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].record_type, "trade");
    }
}
