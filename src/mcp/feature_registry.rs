use schemars::JsonSchema;
use serde::Deserialize;

/// Typed Feature Registry lifecycle verb (`register` | `promote`).
///
/// Reads stay on `search_catalog` / catalog descriptors — this is not a getter.
#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FeatureRegistryParams {
    /// `register` (Base Detector at candidate) or `promote` (human-gated).
    pub action: Option<String>,
    #[serde(alias = "feature_id")]
    pub feature_id: Option<String>,
    /// `shadow` or `active` for promote.
    #[serde(alias = "target_state")]
    pub target_state: Option<String>,
    /// Required for promote. Empty / whitespace is rejected.
    #[serde(alias = "trader_confirmation")]
    pub trader_confirmation: Option<String>,
    pub name: Option<String>,
    pub description: Option<String>,
    #[serde(alias = "domain_id")]
    pub domain_id: Option<String>,
    /// Must be `baseDetector` when set. Derived Features are rejected (SIL-M5b).
    pub kind: Option<String>,
    #[serde(alias = "catalog_field_ids")]
    pub catalog_field_ids: Option<Vec<String>>,
    #[serde(alias = "event_types")]
    pub event_types: Option<Vec<String>>,
    /// Catalog unit label (`count`, `ticks`, `enumLabel`, …). Defaults to `count`.
    pub unit: Option<String>,
    #[serde(alias = "session_scope")]
    pub session_scope: Option<String>,
    /// Catalog freshness label. Defaults to `liveTickAnchored`.
    pub freshness: Option<String>,
    #[serde(alias = "cost_hint")]
    pub cost_hint: Option<String>,
    #[serde(alias = "rust_module")]
    pub rust_module: Option<String>,
    pub source: Option<String>,
}
