use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::PathBuf;
use thiserror::Error;

/// A single timestamped record within a session recording.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecordingEntry {
    /// Unix-epoch seconds (fractional) when this entry was captured.
    pub timestamp: f64,
    /// Entry kind: "trade", "quote", "pipeline_state", "alert", "coaching_prompt",
    /// "prompt_response", "trade_entry", "trade_exit", "note".
    pub record_type: String,
    /// Arbitrary JSON payload for this entry.
    pub payload: serde_json::Value,
}

const MAGIC: &[u8; 4] = b"DESK";
const VERSION: u16 = 2;

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
    auto_start: bool,
    recording: bool,
    flush_threshold: usize,
}

impl SessionRecorder {
    /// Create recorder for a session file.
    pub fn new(path: String) -> Self {
        Self {
            path,
            entries: Vec::new(),
            auto_start: true,
            recording: false,
            flush_threshold: 10_000,
        }
    }

    /// Start recording.
    pub fn start(&mut self) {
        self.recording = true;
    }

    /// Stop recording.
    pub fn stop(&mut self) {
        self.recording = false;
    }

    /// Whether recording is active.
    pub fn is_recording(&self) -> bool {
        self.recording
    }

    /// Set whether recording auto-starts when the market feed connects.
    pub fn set_auto_start(&mut self, auto_start: bool) {
        self.auto_start = auto_start;
    }

    /// Whether auto-start is enabled.
    pub fn auto_start(&self) -> bool {
        self.auto_start
    }

    /// Get current recording path.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Set a new recording path (for session file management).
    pub fn set_path(&mut self, path: String) {
        self.path = path;
    }

    /// Append in-memory record. Auto-starts recording if configured.
    pub fn push(&mut self, entry: RecordingEntry) {
        if self.auto_start && !self.recording {
            self.recording = true;
        }
        if !self.recording {
            return;
        }
        self.entries.push(entry);

        if self.entries.len() >= self.flush_threshold {
            let _ = self.incremental_flush();
        }
    }

    /// Record a pipeline state snapshot (periodically).
    pub fn push_pipeline_state(&mut self, state: &serde_json::Value) {
        self.push(RecordingEntry {
            timestamp: now_secs(),
            record_type: "pipeline_state".to_string(),
            payload: state.clone(),
        });
    }

    /// Record a rules engine alert.
    pub fn push_alert(&mut self, alert: &serde_json::Value) {
        self.push(RecordingEntry {
            timestamp: now_secs(),
            record_type: "alert".to_string(),
            payload: alert.clone(),
        });
    }

    /// Record a coaching prompt.
    pub fn push_coaching_prompt(&mut self, prompt: &serde_json::Value) {
        self.push(RecordingEntry {
            timestamp: now_secs(),
            record_type: "coaching_prompt".to_string(),
            payload: prompt.clone(),
        });
    }

    /// Record a prompt response.
    pub fn push_prompt_response(&mut self, response: &serde_json::Value) {
        self.push(RecordingEntry {
            timestamp: now_secs(),
            record_type: "prompt_response".to_string(),
            payload: response.clone(),
        });
    }

    /// Record a trader note.
    pub fn push_note(&mut self, note: &str) {
        self.push(RecordingEntry {
            timestamp: now_secs(),
            record_type: "note".to_string(),
            payload: serde_json::json!({ "note": note }),
        });
    }

    /// Flush all records to a zstd-compressed binary file.
    pub fn flush(&self) -> Result<(), RecordingError> {
        if self.entries.is_empty() {
            return Ok(());
        }
        let file = File::create(&self.path)?;
        let mut encoder = zstd::Encoder::new(file, 3)?;
        encoder.write_all(MAGIC)?;
        encoder.write_all(&VERSION.to_le_bytes())?;
        encoder.write_all(&(self.entries.len() as u32).to_le_bytes())?;
        for entry in &self.entries {
            write_entry(&mut encoder, entry)?;
        }
        encoder.finish()?;
        Ok(())
    }

    /// Incremental flush — append to file without rewriting everything.
    /// For simplicity, this rewrites the entire file (the entries are kept in memory).
    fn incremental_flush(&self) -> Result<(), RecordingError> {
        self.flush()
    }

    /// Total entry count.
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// Clear entries (after flush, for memory management).
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

fn write_entry<W: Write>(writer: &mut W, entry: &RecordingEntry) -> Result<(), RecordingError> {
    writer.write_all(&entry.timestamp.to_le_bytes())?;
    let record_type = entry.record_type.as_bytes();
    let payload = serde_json::to_vec(&entry.payload)?;
    writer.write_all(&(record_type.len() as u16).to_le_bytes())?;
    writer.write_all(record_type)?;
    writer.write_all(&(payload.len() as u32).to_le_bytes())?;
    writer.write_all(&payload)?;
    Ok(())
}

fn now_secs() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}

/// List saved recording files from the recordings directory.
pub fn list_recordings(dir: &str) -> Result<Vec<RecordingInfo>, RecordingError> {
    let path = PathBuf::from(dir);
    if !path.exists() {
        return Ok(Vec::new());
    }

    let mut recordings = Vec::new();
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let file_path = entry.path();
        if file_path.extension().and_then(|e| e.to_str()) == Some("desk") {
            let metadata = entry.metadata()?;
            let size_bytes = metadata.len();
            let file_name = file_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string();

            recordings.push(RecordingInfo {
                path: file_path.to_string_lossy().to_string(),
                file_name,
                size_bytes,
            });
        }
    }

    recordings.sort_by(|a, b| b.file_name.cmp(&a.file_name));
    Ok(recordings)
}

/// Metadata about a saved recording file.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecordingInfo {
    pub path: String,
    pub file_name: String,
    pub size_bytes: u64,
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
        recorder.start();
        recorder.push(RecordingEntry {
            timestamp: 1.0,
            record_type: "trade".to_string(),
            payload: serde_json::json!({"price": 21000.0}),
        });
        recorder.push_alert(&serde_json::json!({"setupId": "s1", "state": "conditionsMet"}));
        recorder.push_note("Test note");
        recorder.flush().expect("flush");
        let entries = ReplayEngine::load(&path).expect("load");
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].record_type, "trade");
        assert_eq!(entries[1].record_type, "alert");
        assert_eq!(entries[2].record_type, "note");
    }

    #[test]
    fn auto_start_begins_on_first_push() {
        let file = NamedTempFile::new().expect("temp");
        let path = file.path().to_string_lossy().to_string();
        let mut recorder = SessionRecorder::new(path);
        assert!(!recorder.is_recording());
        recorder.push(RecordingEntry {
            timestamp: 1.0,
            record_type: "trade".to_string(),
            payload: serde_json::json!({}),
        });
        assert!(recorder.is_recording());
        assert_eq!(recorder.entry_count(), 1);
    }

    #[test]
    fn manual_mode_ignores_push_when_stopped() {
        let file = NamedTempFile::new().expect("temp");
        let path = file.path().to_string_lossy().to_string();
        let mut recorder = SessionRecorder::new(path);
        recorder.set_auto_start(false);
        recorder.push(RecordingEntry {
            timestamp: 1.0,
            record_type: "trade".to_string(),
            payload: serde_json::json!({}),
        });
        assert_eq!(recorder.entry_count(), 0);
    }
}
