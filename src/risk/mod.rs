use serde::{Deserialize, Serialize};

/// Trader-defined risk limits for a single session.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RiskConfig {
    /// Maximum allowed daily loss in R units before trading is halted.
    pub max_daily_loss_r: f64,
    /// Maximum number of trades permitted per session.
    pub max_trades_per_session: usize,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            max_daily_loss_r: 3.0,
            max_trades_per_session: 8,
        }
    }
}

/// Current risk metrics for the active session.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct RiskState {
    /// Cumulative session P&L in R units.
    pub daily_pnl_r: f64,
    /// Number of trades taken this session.
    pub trade_count: usize,
    /// Current streak of consecutive losing trades.
    pub consecutive_losses: usize,
    /// Current streak of consecutive winning trades.
    pub consecutive_wins: usize,
    /// Drawdown from session high-water mark in R units.
    pub drawdown_r: f64,
    /// Configured maximum daily loss limit in R units.
    pub max_daily_loss_r: f64,
    /// Whether a risk limit has been breached (loss or trade count).
    pub at_limit: bool,
}

/// Tracks intraday risk metrics and enforces configured limits.
#[derive(Debug)]
pub struct RiskTracker {
    config: RiskConfig,
    high_water_pnl_r: f64,
    state: RiskState,
}

impl RiskTracker {
    /// Create tracker with config.
    pub fn new(config: RiskConfig) -> Self {
        Self {
            state: RiskState {
                max_daily_loss_r: config.max_daily_loss_r,
                ..Default::default()
            },
            config,
            high_water_pnl_r: 0.0,
        }
    }

    /// Record one trade result in R.
    pub fn record_trade_result(&mut self, result_r: f64) {
        self.state.trade_count += 1;
        self.state.daily_pnl_r += result_r;
        if result_r < 0.0 {
            self.state.consecutive_losses += 1;
            self.state.consecutive_wins = 0;
        } else {
            self.state.consecutive_wins += 1;
            self.state.consecutive_losses = 0;
        }
        self.high_water_pnl_r = self.high_water_pnl_r.max(self.state.daily_pnl_r);
        self.state.drawdown_r = self.high_water_pnl_r - self.state.daily_pnl_r;
        self.state.at_limit = self.state.daily_pnl_r <= -self.config.max_daily_loss_r
            || self.state.trade_count >= self.config.max_trades_per_session;
    }

    /// Current risk state.
    pub fn state(&self) -> RiskState {
        self.state.clone()
    }

    /// Restore tracker from a persisted state snapshot.
    pub fn restore_state(&mut self, state: RiskState) {
        self.high_water_pnl_r = state.daily_pnl_r + state.drawdown_r;
        self.state = state;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marks_limit_when_drawdown_exceeds_max_loss() {
        let mut tracker = RiskTracker::new(RiskConfig {
            max_daily_loss_r: 2.0,
            max_trades_per_session: 10,
        });
        tracker.record_trade_result(-1.0);
        tracker.record_trade_result(-1.5);
        assert!(tracker.state().at_limit);
    }
}
