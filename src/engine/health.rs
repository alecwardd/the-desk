//! Engine / feed / stall observability contracts.

use serde::{Deserialize, Serialize};

use super::source::{SourceHealth, SourceProviderKind};

/// Stall / behind classification for ops and agents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FeedStallState {
    /// No backlog; ingest advancing or idle with empty file.
    Ok,
    /// SCID file growing while processed offset is not advancing.
    Stalled,
    /// Processed offset behind file length but still making progress.
    Behind,
    /// Source missing / engine unreachable / stubbed provider.
    Unavailable,
}

/// Aggregated engine health published to MCP / ops tools.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct EngineHealth {
    pub engine_alive: bool,
    pub engine_pid: u32,
    pub mode: String,
    pub stall_state: FeedStallState,
    pub scid_backlog_bytes: u64,
    pub last_ingest_wall_ms: Option<u64>,
    pub last_publish_wall_ms: Option<u64>,
    pub ticks_ingested: u64,
    pub events_detected: u64,
    pub generation: u64,
    pub source: SourceHealth,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

impl EngineHealth {
    pub fn unavailable(note: impl Into<String>) -> Self {
        let note = note.into();
        Self {
            engine_alive: false,
            engine_pid: 0,
            mode: "unavailable".into(),
            stall_state: FeedStallState::Unavailable,
            scid_backlog_bytes: 0,
            last_ingest_wall_ms: None,
            last_publish_wall_ms: None,
            ticks_ingested: 0,
            events_detected: 0,
            generation: 0,
            source: SourceHealth {
                provider_kind: SourceProviderKind::File,
                scid_path: None,
                scid_exists: false,
                scid_file_len_bytes: 0,
                scid_read_offset_bytes: 0,
                depth_file_count: 0,
                depth_paths: Vec::new(),
                last_tick_timestamp_ms: None,
                ticks_emitted: 0,
                stubbed: false,
                note: Some(note.clone()),
            },
            note: Some(note),
        }
    }

    /// Classify stall/behind from backlog + progress timestamps.
    pub fn classify_stall(
        backlog_bytes: u64,
        last_progress_wall_ms: u64,
        now_wall_ms: u64,
        stall_threshold_ms: u64,
    ) -> FeedStallState {
        if backlog_bytes == 0 {
            return FeedStallState::Ok;
        }
        if last_progress_wall_ms == 0 {
            return FeedStallState::Behind;
        }
        if now_wall_ms.saturating_sub(last_progress_wall_ms) >= stall_threshold_ms {
            FeedStallState::Stalled
        } else {
            FeedStallState::Behind
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stall_classifier_distinguishes_ok_behind_stalled() {
        assert_eq!(
            EngineHealth::classify_stall(0, 100, 200, 10_000),
            FeedStallState::Ok
        );
        assert_eq!(
            EngineHealth::classify_stall(100, 100, 200, 10_000),
            FeedStallState::Behind
        );
        assert_eq!(
            EngineHealth::classify_stall(100, 100, 20_000, 10_000),
            FeedStallState::Stalled
        );
    }
}
