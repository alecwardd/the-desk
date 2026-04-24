use serde::{Deserialize, Serialize};

/// Whether current session builds on or clears prior session inventory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Default)]
pub enum InventoryState {
    Building,
    Clearing,
    #[default]
    Neutral,
}

/// Directional bias of delta positioning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Default)]
pub enum InventoryDirection {
    Long,
    Short,
    #[default]
    Flat,
}

/// Prior session summary for cross-session comparison.
#[derive(Debug, Clone)]
pub struct PriorSessionData {
    pub final_delta: f64,
    pub dnva_high: f64,
    pub dnva_low: f64,
    pub dnp: f64,
}

/// Cross-session delta positioning tracker.
#[derive(Debug, Default)]
pub struct SessionInventoryPipeline {
    prior_sessions: Vec<PriorSessionData>,
    current_session_delta: f64,
    current_dnp: f64,
    state: InventoryState,
    direction: InventoryDirection,
    sessions_in_trend: usize,
}

const NEUTRAL_THRESHOLD: f64 = 50.0;

impl SessionInventoryPipeline {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reset(&mut self) {
        self.current_session_delta = 0.0;
        self.current_dnp = 0.0;
        self.state = InventoryState::Neutral;
        self.direction = InventoryDirection::Flat;
    }

    /// Load prior session data from SQLite at session start.
    pub fn load_prior_sessions(&mut self, sessions: Vec<PriorSessionData>) {
        self.prior_sessions = sessions;
        self.compute_trend_count();
    }

    /// Append a just-closed session as the new most-recent prior session and
    /// trim the list to the most recent `max_keep` entries. Used at boundary
    /// finalization so cross-session inventory tracking sees the latest close
    /// without round-tripping through SQLite.
    pub fn push_just_closed_session(&mut self, data: PriorSessionData, max_keep: usize) {
        self.prior_sessions.push(data);
        if max_keep > 0 && self.prior_sessions.len() > max_keep {
            let drop = self.prior_sessions.len() - max_keep;
            self.prior_sessions.drain(0..drop);
        }
        self.compute_trend_count();
    }

    /// Read-only access to the loaded prior sessions (oldest first, newest last).
    pub fn prior_sessions(&self) -> &[PriorSessionData] {
        &self.prior_sessions
    }

    /// Update with current session's live delta and DNP.
    pub fn update(&mut self, session_delta: f64, dnp: f64) {
        self.current_session_delta = session_delta;
        self.current_dnp = dnp;
        self.classify();
    }

    fn classify(&mut self) {
        // Direction based on current session delta
        self.direction = if self.current_session_delta > NEUTRAL_THRESHOLD {
            InventoryDirection::Long
        } else if self.current_session_delta < -NEUTRAL_THRESHOLD {
            InventoryDirection::Short
        } else {
            InventoryDirection::Flat
        };

        // State: compare against prior session direction
        if let Some(prior) = self.prior_sessions.last() {
            let prior_dir = if prior.final_delta > NEUTRAL_THRESHOLD {
                InventoryDirection::Long
            } else if prior.final_delta < -NEUTRAL_THRESHOLD {
                InventoryDirection::Short
            } else {
                InventoryDirection::Flat
            };

            self.state = if self.direction == InventoryDirection::Flat
                || prior_dir == InventoryDirection::Flat
            {
                InventoryState::Neutral
            } else if self.direction == prior_dir {
                InventoryState::Building
            } else {
                InventoryState::Clearing
            };
        } else {
            self.state = InventoryState::Neutral;
        }
    }

    fn compute_trend_count(&mut self) {
        if self.prior_sessions.is_empty() {
            self.sessions_in_trend = 0;
            return;
        }
        let last_dir = if self.prior_sessions.last().unwrap().final_delta > NEUTRAL_THRESHOLD {
            1
        } else if self.prior_sessions.last().unwrap().final_delta < -NEUTRAL_THRESHOLD {
            -1
        } else {
            0
        };

        let mut count = 0usize;
        for session in self.prior_sessions.iter().rev() {
            let dir = if session.final_delta > NEUTRAL_THRESHOLD {
                1
            } else if session.final_delta < -NEUTRAL_THRESHOLD {
                -1
            } else {
                0
            };
            if dir == last_dir && dir != 0 {
                count += 1;
            } else {
                break;
            }
        }
        self.sessions_in_trend = count;
    }

    /// Is the DNP shifting relative to prior sessions?
    pub fn dnp_shift(&self) -> f64 {
        if let Some(prior) = self.prior_sessions.last() {
            self.current_dnp - prior.dnp
        } else {
            0.0
        }
    }

    pub fn state(&self) -> InventoryState {
        self.state
    }

    pub fn direction(&self) -> InventoryDirection {
        self.direction
    }

    pub fn sessions_in_trend(&self) -> usize {
        self.sessions_in_trend
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_building_inventory() {
        let mut p = SessionInventoryPipeline::new();
        p.load_prior_sessions(vec![PriorSessionData {
            final_delta: 200.0,
            dnva_high: 21050.0,
            dnva_low: 21000.0,
            dnp: 21025.0,
        }]);
        p.update(150.0, 21030.0);
        assert_eq!(p.state(), InventoryState::Building);
        assert_eq!(p.direction(), InventoryDirection::Long);
    }

    #[test]
    fn detects_clearing_inventory() {
        let mut p = SessionInventoryPipeline::new();
        p.load_prior_sessions(vec![PriorSessionData {
            final_delta: 200.0,
            dnva_high: 21050.0,
            dnva_low: 21000.0,
            dnp: 21025.0,
        }]);
        p.update(-150.0, 21010.0);
        assert_eq!(p.state(), InventoryState::Clearing);
        assert_eq!(p.direction(), InventoryDirection::Short);
    }

    #[test]
    fn trend_count_consecutive() {
        let mut p = SessionInventoryPipeline::new();
        p.load_prior_sessions(vec![
            PriorSessionData {
                final_delta: -100.0,
                dnva_high: 0.0,
                dnva_low: 0.0,
                dnp: 0.0,
            },
            PriorSessionData {
                final_delta: 200.0,
                dnva_high: 0.0,
                dnva_low: 0.0,
                dnp: 0.0,
            },
            PriorSessionData {
                final_delta: 150.0,
                dnva_high: 0.0,
                dnva_low: 0.0,
                dnp: 0.0,
            },
            PriorSessionData {
                final_delta: 180.0,
                dnva_high: 0.0,
                dnva_low: 0.0,
                dnp: 0.0,
            },
        ]);
        assert_eq!(p.sessions_in_trend(), 3);
    }
}
