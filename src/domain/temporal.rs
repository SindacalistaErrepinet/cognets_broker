//! Temporal query input and output payloads.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

/// Shared temporal query parameters accepted by API endpoints.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct TemporalQueryInput {
    #[serde(rename = "timerel")]
    pub time_rel: Option<String>,
    #[serde(rename = "timeAt")]
    pub time_at: Option<String>,
    #[serde(rename = "endTimeAt")]
    pub end_time_at: Option<String>,
    #[serde(rename = "timeproperty")]
    pub time_property: Option<String>,
    #[serde(rename = "lastN")]
    pub last_n: Option<u32>,
    #[serde(rename = "aggrMethods")]
    pub aggr_methods: Option<String>,
    #[serde(rename = "aggrPeriodDuration")]
    pub aggr_period_duration: Option<String>,
    pub options: Option<String>,
    pub format: Option<String>,
}

/// Temporal query result returned by service layer.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct TemporalQueryResult {
    /// JSON response payload.
    pub body: Value,
    /// Count before HTTP-layer pagination headers are applied.
    pub total_count: usize,
}
