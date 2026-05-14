use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::{IntoParams, ToSchema};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Representation {
    Normalized,
    KeyValues,
    GeoJson,
    TemporalValues,
    AggregatedValues,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
pub struct EntityQuery {
    pub id: Option<String>,
    #[serde(rename = "type")]
    pub entity_type: Option<String>,
    #[serde(rename = "idPattern")]
    pub id_pattern: Option<String>,
    pub attrs: Option<String>,
    pub pick: Option<String>,
    pub omit: Option<String>,
    pub q: Option<String>,
    #[serde(rename = "expandValues")]
    pub expand_values: Option<String>,
    #[serde(rename = "jsonKeys")]
    pub json_keys: Option<String>,
    pub csf: Option<String>,
    pub geometry: Option<String>,
    pub georel: Option<String>,
    pub coordinates: Option<String>,
    pub geoproperty: Option<String>,
    #[serde(rename = "geometryProperty")]
    pub geometry_property: Option<String>,
    pub lang: Option<String>,
    #[serde(rename = "scopeQ")]
    pub scope_q: Option<String>,
    #[serde(rename = "containedBy")]
    pub contained_by: Option<String>,
    pub join: Option<String>,
    #[serde(rename = "joinLevel")]
    pub join_level: Option<u32>,
    #[serde(rename = "datasetId")]
    pub dataset_id: Option<String>,
    #[serde(rename = "entityMap")]
    pub entity_map: Option<bool>,
    pub limit: Option<usize>,
    pub count: Option<bool>,
    pub options: Option<String>,
    pub format: Option<String>,
    pub local: Option<bool>,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
pub struct EntityTypeQuery {
    #[serde(rename = "type")]
    pub entity_type: Option<String>,
    pub local: Option<bool>,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
pub struct LocalOnlyQuery {
    pub local: Option<bool>,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
pub struct AppendAttrsQuery {
    #[serde(rename = "type")]
    pub entity_type: Option<String>,
    pub options: Option<String>,
    pub local: Option<bool>,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
pub struct DeleteAttrQuery {
    #[serde(rename = "type")]
    pub entity_type: Option<String>,
    #[serde(rename = "deleteAll")]
    pub delete_all: Option<bool>,
    #[serde(rename = "datasetId")]
    pub dataset_id: Option<String>,
    pub local: Option<bool>,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
pub struct UpsertBatchQuery {
    pub options: Option<String>,
    pub local: Option<bool>,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
pub struct BatchUpdateQuery {
    pub options: Option<String>,
    pub local: Option<bool>,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
pub struct SubscriptionQuery {
    pub limit: Option<usize>,
    pub count: Option<bool>,
    pub local: Option<bool>,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
pub struct DiscoveryQuery {
    pub details: Option<bool>,
    pub local: Option<bool>,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize, ToSchema)]
pub struct TemporalEntityQuery {
    #[serde(flatten)]
    pub entity: EntityQuery,
    #[serde(rename = "timeproperty")]
    pub time_property: Option<String>,
    #[serde(rename = "timerel")]
    pub time_rel: Option<String>,
    #[serde(rename = "timeAt")]
    pub time_at: Option<String>,
    #[serde(rename = "endTimeAt")]
    pub end_time_at: Option<String>,
    #[serde(rename = "lastN")]
    pub last_n: Option<u32>,
    #[serde(rename = "aggrMethods")]
    pub aggr_methods: Option<String>,
    #[serde(rename = "aggrPeriodDuration")]
    pub aggr_period_duration: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct QueryResult {
    pub body: Value,
    pub total_count: usize,
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Path)]
pub struct EntityIdPath {
    #[param(rename = "entity_id")]
    pub entity_id: String,
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Path)]
pub struct EntityAttrPath {
    #[param(rename = "entity_id")]
    pub entity_id: String,
    #[param(rename = "attr_id")]
    pub attr_id: String,
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Path)]
pub struct EntityAttrInstancePath {
    #[param(rename = "entity_id")]
    pub entity_id: String,
    #[param(rename = "attr_id")]
    pub attr_id: String,
    #[param(rename = "instance_id")]
    pub instance_id: String,
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Path)]
pub struct SubscriptionIdPath {
    #[param(rename = "subscription_id")]
    pub subscription_id: String,
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Path)]
pub struct TypeNamePath {
    #[param(rename = "type")]
    pub type_name: String,
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Path)]
pub struct AttributeNamePath {
    #[param(rename = "attrId")]
    pub attribute_name: String,
}

#[derive(Debug, Default, Clone, Deserialize, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
pub struct TemporalEntityQueryParams {
    #[serde(flatten)]
    #[param(inline)]
    pub entity: EntityQuery,
    #[serde(rename = "timeproperty")]
    pub time_property: Option<String>,
    #[serde(rename = "timerel")]
    pub time_rel: Option<String>,
    #[serde(rename = "timeAt")]
    pub time_at: Option<String>,
    #[serde(rename = "endTimeAt")]
    pub end_time_at: Option<String>,
    #[serde(rename = "lastN")]
    pub last_n: Option<u32>,
    #[serde(rename = "aggrMethods")]
    pub aggr_methods: Option<String>,
    #[serde(rename = "aggrPeriodDuration")]
    pub aggr_period_duration: Option<String>,
}
