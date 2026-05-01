use schemars::JsonSchema;
use serde::Deserialize;

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RegisterHypothesisParams {
    pub metadata: serde_json::Value,
    #[serde(alias = "setup_definition")]
    pub setup_definition: serde_json::Value,
    #[serde(default, alias = "dry_run")]
    pub dry_run: Option<bool>,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HypothesisRunParams {
    #[serde(alias = "setup_id")]
    pub setup_id: String,
    #[serde(alias = "job_id")]
    pub job_id: String,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ActivateDraftSetupParams {
    #[serde(alias = "setup_id")]
    pub setup_id: String,
    #[serde(alias = "trader_confirmation")]
    pub trader_confirmation: String,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SetHypothesisLifecycleParams {
    #[serde(alias = "setup_id")]
    pub setup_id: String,
    pub target: String,
    pub reason: String,
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListHypothesesParams {
    pub lifecycle: Option<String>,
}
