use serde::{Deserialize, Serialize};

const DEFAULT_SAMPLE_CAPACITY: usize = 10;

/// Classification of a non-monotonic timestamp relative to the last accepted tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MonotonicTimestampViolationKind {
    EqualTimestamp,
    BackwardTimestamp,
}

/// A bounded sample of a skipped non-monotonic tick for operator diagnostics.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MonotonicTimestampSample {
    pub kind: MonotonicTimestampViolationKind,
    pub timestamp_ms: f64,
    pub last_accepted_timestamp_ms: f64,
    pub delta_ms: f64,
}

/// Aggregated monotonicity stats for a stream of SCID ticks.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MonotonicTimestampStats {
    pub accepted_ticks: usize,
    pub skipped_non_monotonic_ticks: usize,
    pub duplicate_timestamp_ticks: usize,
    pub backward_timestamp_ticks: usize,
    pub last_accepted_timestamp_ms: Option<f64>,
    pub last_non_monotonic_timestamp_ms: Option<f64>,
    pub worst_backward_delta_ms: Option<f64>,
    pub samples: Vec<MonotonicTimestampSample>,
}

impl MonotonicTimestampStats {
    pub fn has_violations(&self) -> bool {
        self.skipped_non_monotonic_ticks > 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MonotonicTimestampDecision {
    Accept,
    Skip(MonotonicTimestampViolationKind),
}

/// Stateful helper that enforces a strict increasing-timestamp policy.
#[derive(Debug, Clone)]
pub struct MonotonicTickGuard {
    sample_capacity: usize,
    stats: MonotonicTimestampStats,
}

impl Default for MonotonicTickGuard {
    fn default() -> Self {
        Self::new(DEFAULT_SAMPLE_CAPACITY)
    }
}

impl MonotonicTickGuard {
    pub fn new(sample_capacity: usize) -> Self {
        Self {
            sample_capacity,
            stats: MonotonicTimestampStats {
                samples: Vec::with_capacity(sample_capacity),
                ..MonotonicTimestampStats::default()
            },
        }
    }

    pub fn observe(&mut self, timestamp_ms: f64) -> MonotonicTimestampDecision {
        if let Some(last_accepted) = self.stats.last_accepted_timestamp_ms {
            if timestamp_ms <= last_accepted {
                let kind = if timestamp_ms == last_accepted {
                    self.stats.duplicate_timestamp_ticks += 1;
                    MonotonicTimestampViolationKind::EqualTimestamp
                } else {
                    self.stats.backward_timestamp_ticks += 1;
                    let backward_delta_ms = last_accepted - timestamp_ms;
                    self.stats.worst_backward_delta_ms = Some(
                        self.stats
                            .worst_backward_delta_ms
                            .map(|worst| worst.max(backward_delta_ms))
                            .unwrap_or(backward_delta_ms),
                    );
                    MonotonicTimestampViolationKind::BackwardTimestamp
                };
                self.stats.skipped_non_monotonic_ticks += 1;
                self.stats.last_non_monotonic_timestamp_ms = Some(timestamp_ms);
                if self.stats.samples.len() < self.sample_capacity {
                    self.stats.samples.push(MonotonicTimestampSample {
                        kind,
                        timestamp_ms,
                        last_accepted_timestamp_ms: last_accepted,
                        delta_ms: timestamp_ms - last_accepted,
                    });
                }
                return MonotonicTimestampDecision::Skip(kind);
            }
        }

        self.stats.accepted_ticks += 1;
        self.stats.last_accepted_timestamp_ms = Some(timestamp_ms);
        MonotonicTimestampDecision::Accept
    }

    pub fn last_accepted_timestamp_ms(&self) -> Option<f64> {
        self.stats.last_accepted_timestamp_ms
    }

    pub fn stats(&self) -> &MonotonicTimestampStats {
        &self.stats
    }

    pub fn into_stats(self) -> MonotonicTimestampStats {
        self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guard_skips_equal_and_backward_timestamps() {
        let mut guard = MonotonicTickGuard::new(8);
        assert_eq!(guard.observe(100.0), MonotonicTimestampDecision::Accept);
        assert_eq!(
            guard.observe(100.0),
            MonotonicTimestampDecision::Skip(MonotonicTimestampViolationKind::EqualTimestamp)
        );
        assert_eq!(
            guard.observe(99.0),
            MonotonicTimestampDecision::Skip(MonotonicTimestampViolationKind::BackwardTimestamp)
        );
        assert_eq!(guard.observe(101.0), MonotonicTimestampDecision::Accept);

        let stats = guard.into_stats();
        assert_eq!(stats.accepted_ticks, 2);
        assert_eq!(stats.skipped_non_monotonic_ticks, 2);
        assert_eq!(stats.duplicate_timestamp_ticks, 1);
        assert_eq!(stats.backward_timestamp_ticks, 1);
        assert_eq!(stats.last_accepted_timestamp_ms, Some(101.0));
        assert_eq!(stats.last_non_monotonic_timestamp_ms, Some(99.0));
        assert_eq!(stats.worst_backward_delta_ms, Some(1.0));
        assert_eq!(stats.samples.len(), 2);
    }
}
