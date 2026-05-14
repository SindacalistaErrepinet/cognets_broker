use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct StoredDocument {
    pub tenant: String,
    #[serde(rename = "id")]
    pub ngsi_id: String,
    #[serde(rename = "doc")]
    pub doc: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TemporalEntityDocument {
    pub tenant: String,
    #[serde(rename = "id")]
    pub ngsi_id: String,
    #[serde(rename = "doc")]
    pub doc: Value,
    #[serde(default)]
    pub history: Vec<TemporalAttributeRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TemporalAttributeRecord {
    #[serde(rename = "attrId")]
    pub attr_id: String,
    #[serde(rename = "instanceId")]
    pub instance_id: String,
    #[serde(rename = "observedAt", skip_serializing_if = "Option::is_none")]
    pub observed_at: Option<String>,
    #[serde(rename = "createdAt", skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(rename = "modifiedAt", skip_serializing_if = "Option::is_none")]
    pub modified_at: Option<String>,
    #[serde(rename = "deletedAt", skip_serializing_if = "Option::is_none")]
    pub deleted_at: Option<String>,
    pub value: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SubscriptionDocument {
    pub tenant: String,
    #[serde(rename = "id")]
    pub ngsi_id: String,
    #[serde(rename = "doc")]
    pub doc: Value,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ContextSourceIdentity {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(rename = "contextSourceAlias")]
    pub context_source_alias: String,
    #[serde(rename = "contextSourceUpTime")]
    pub context_source_up_time: String,
    #[serde(rename = "contextSourceTimeAt")]
    pub context_source_time_at: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EntityEventKind {
    Created,
    Updated,
    Deleted,
}

#[derive(Clone, Debug)]
pub struct EntityEvent {
    pub kind: EntityEventKind,
    pub changed_attributes: Vec<String>,
}
