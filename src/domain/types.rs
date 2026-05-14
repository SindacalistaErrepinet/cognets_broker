//! Core persisted and runtime entity types.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

/// Stored entity wrapper used by repository implementations.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct StoredDocument {
    /// Tenant owning this entity.
    pub tenant: String,
    /// Logical NGSI-LD entity id.
    #[serde(rename = "id")]
    pub ngsi_id: String,
    /// Raw NGSI-LD entity payload.
    #[serde(rename = "doc")]
    pub doc: Value,
}

/// Stored temporal entity wrapper with current snapshot and history.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TemporalEntityDocument {
    /// Tenant owning this temporal entity.
    pub tenant: String,
    /// Logical NGSI-LD entity id.
    #[serde(rename = "id")]
    pub ngsi_id: String,
    /// Latest entity snapshot.
    #[serde(rename = "doc")]
    pub doc: Value,
    /// Flattened temporal attribute history.
    #[serde(default)]
    pub history: Vec<TemporalAttributeRecord>,
}

/// One stored temporal attribute instance.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TemporalAttributeRecord {
    /// Attribute name this record belongs to.
    #[serde(rename = "attrId")]
    pub attr_id: String,
    /// Stable instance identifier for one temporal observation.
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
    /// Attribute payload captured for this instance.
    pub value: Value,
}

/// Stored subscription wrapper used by repository implementations.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SubscriptionDocument {
    /// Tenant owning this subscription.
    pub tenant: String,
    /// Logical subscription id.
    #[serde(rename = "id")]
    pub ngsi_id: String,
    /// Raw NGSI-LD subscription payload.
    #[serde(rename = "doc")]
    pub doc: Value,
}

/// Broker identity payload returned by `/info/sourceIdentity`.
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

/// Lifecycle event kind emitted after entity mutations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EntityEventKind {
    /// Entity was created.
    Created,
    /// Entity was updated.
    Updated,
    /// Entity was deleted.
    Deleted,
}

/// Entity lifecycle event passed to notification matching code.
#[derive(Clone, Debug)]
pub struct EntityEvent {
    /// Event category.
    pub kind: EntityEventKind,
    /// Top-level attributes that changed for this event.
    pub changed_attributes: Vec<String>,
}
