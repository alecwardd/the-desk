use crate::db::AttentionSignalRecord;

#[derive(Debug, Clone)]
pub struct AttentionNotifierConfig {
    pub enabled: bool,
    pub min_priority: String,
    pub allowed_kinds: Vec<String>,
    pub rth_only: bool,
}

impl Default for AttentionNotifierConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            min_priority: "high".to_string(),
            allowed_kinds: vec![
                "setup_lifecycle_change".to_string(),
                "risk_context_change".to_string(),
                "trade_management_change".to_string(),
            ],
            rth_only: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AttentionNotifierDecision {
    pub should_dispatch: bool,
    pub reason: String,
}

impl AttentionNotifierConfig {
    pub fn evaluate(&self, signal: &AttentionSignalRecord) -> AttentionNotifierDecision {
        if !self.enabled {
            return decision(false, "notifier disabled");
        }
        if self.rth_only && signal.session_type != "RTH" {
            return decision(false, "outside RTH");
        }
        if !self.allowed_kinds.iter().any(|kind| kind == &signal.kind) {
            return decision(false, "kind disabled");
        }
        if priority_rank(&signal.priority) < priority_rank(&self.min_priority) {
            return decision(false, "below priority threshold");
        }
        decision(true, "dispatch")
    }
}

fn decision(should_dispatch: bool, reason: &str) -> AttentionNotifierDecision {
    AttentionNotifierDecision {
        should_dispatch,
        reason: reason.to_string(),
    }
}

fn priority_rank(priority: &str) -> i64 {
    match priority {
        "urgent" => 4,
        "high" => 3,
        "normal" => 2,
        "low" => 1,
        _ => 0,
    }
}
