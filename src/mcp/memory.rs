use schemars::JsonSchema;
use serde::Deserialize;

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TraderContextFitParams {
    pub intent: Option<String>,
    pub setup_id: Option<String>,
    pub session_id: Option<String>,
    pub trade_account: Option<String>,
    pub trading_day: Option<String>,
    pub timestamp_ms: Option<f64>,
    pub session_type: Option<String>,
    pub session_segment: Option<String>,
    pub time_bucket: Option<String>,
    pub day_type: Option<String>,
    pub profile_shape: Option<String>,
    pub balance_state: Option<String>,
    pub include_opportunity: Option<bool>,
    pub include_coaching_memory: Option<bool>,
}
