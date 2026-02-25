mod delta;
mod levels;
mod tpo;
mod vwap;

pub use delta::DeltaPipeline;
pub use levels::{KeyLevel, KeyLevelType, LevelsPipeline};
pub use tpo::TpoPipeline;
pub use vwap::VwapPipeline;

use serde::{Deserialize, Serialize};

/// Consolidated snapshot of all pipeline outputs for the current session.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct MarketState {
    /// Most recent trade price.
    pub last_price: f64,
    /// Current best bid price.
    pub bid: f64,
    /// Current best ask price.
    pub ask: f64,
    /// Session volume-weighted average price.
    pub vwap: f64,
    /// VWAP plus one standard deviation.
    pub vwap_1sd_upper: f64,
    /// VWAP minus one standard deviation.
    pub vwap_1sd_lower: f64,
    /// TPO value area high (70% of TPOs).
    pub va_high: f64,
    /// TPO value area low (70% of TPOs).
    pub va_low: f64,
    /// Point of control — price with highest TPO count.
    pub poc: f64,
    /// Delta neutral value area high (70% of absolute delta).
    pub dnva_high: f64,
    /// Delta neutral value area low (70% of absolute delta).
    pub dnva_low: f64,
    /// Delta neutral pivot — where cumulative delta crosses zero.
    pub dnp: f64,
    /// Net buy minus sell volume for the current session.
    pub session_delta: f64,
    /// Running cumulative delta across sessions.
    pub cumulative_delta: f64,
    /// Previous RTH session high.
    pub prior_day_high: f64,
    /// Previous RTH session low.
    pub prior_day_low: f64,
    /// Previous RTH session closing price.
    pub prior_day_close: f64,
    /// Overnight (Globex) session high.
    pub overnight_high: f64,
    /// Overnight (Globex) session low.
    pub overnight_low: f64,
    /// Opening range high (first 30 minutes of RTH).
    pub or_high: f64,
    /// Opening range low (first 30 minutes of RTH).
    pub or_low: f64,
    /// Initial balance high (first 60 minutes of RTH).
    pub ib_high: f64,
    /// Initial balance low (first 60 minutes of RTH).
    pub ib_low: f64,
}

pub struct PipelineEngine {
    pub vwap: VwapPipeline,
    pub tpo: TpoPipeline,
    pub delta: DeltaPipeline,
    pub levels: LevelsPipeline,
}

impl Default for PipelineEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl PipelineEngine {
    /// Create all deterministic pipelines with NQ tick size defaults.
    pub fn new() -> Self {
        Self {
            vwap: VwapPipeline::new(),
            tpo: TpoPipeline::new(0.25),
            delta: DeltaPipeline::new(0.25),
            levels: LevelsPipeline::default(),
        }
    }

    /// Reset all pipelines for a new trading session.
    /// Current session data rolls into prior-day references.
    pub fn reset_session(&mut self) {
        self.levels.reset_session();
        self.vwap.reset();
        self.tpo.reset();
        self.delta.reset();
    }

    /// Apply a single trade incrementally to all pipelines.
    pub fn on_trade(&mut self, price: f64, volume: f64, is_buy: bool, minute_of_session: i32) {
        self.vwap.add_trade(price, volume);
        self.tpo.add_trade(price, minute_of_session);
        self.delta.add_trade(price, volume, is_buy);
        self.levels.on_trade(price, minute_of_session);
    }

    /// Build current market state snapshot.
    pub fn snapshot(&self, bid: f64, ask: f64) -> MarketState {
        MarketState {
            last_price: self.levels.last_price,
            bid,
            ask,
            vwap: self.vwap.vwap(),
            vwap_1sd_upper: self.vwap.vwap() + self.vwap.std_dev(),
            vwap_1sd_lower: self.vwap.vwap() - self.vwap.std_dev(),
            va_high: self.tpo.va_high(),
            va_low: self.tpo.va_low(),
            poc: self.tpo.poc(),
            dnva_high: self.delta.dnva_high(),
            dnva_low: self.delta.dnva_low(),
            dnp: self.delta.dnp(),
            session_delta: self.delta.session_delta(),
            cumulative_delta: self.delta.session_delta(),
            prior_day_high: self.levels.prior_day_high,
            prior_day_low: self.levels.prior_day_low,
            prior_day_close: self.levels.prior_day_close,
            overnight_high: self.levels.overnight_high,
            overnight_low: self.levels.overnight_low,
            or_high: self.tpo.or_high(),
            or_low: self.tpo.or_low(),
            ib_high: self.tpo.ib_high(),
            ib_low: self.tpo.ib_low(),
        }
    }
}
