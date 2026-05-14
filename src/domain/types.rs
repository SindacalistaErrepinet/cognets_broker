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

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, Eq, PartialEq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PeerStatus {
    #[default]
    Alive,
    Suspect,
    Dead,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PeerDocument {
    pub tenant: String,
    #[serde(rename = "peerId")]
    pub peer_id: String,
    pub endpoint: String,
    #[serde(default)]
    pub aliases: Vec<String>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub neighbors: Vec<String>,
    #[serde(default)]
    pub status: PeerStatus,
    #[serde(default)]
    pub incarnation: u64,
    #[serde(rename = "updatedAt")]
    pub updated_at: String,
    #[serde(rename = "lastSyncedAt", skip_serializing_if = "Option::is_none")]
    pub last_synced_at: Option<String>,
    #[serde(rename = "lastSyncedAtNanos", skip_serializing_if = "Option::is_none")]
    pub last_synced_at_nanos: Option<i64>,
    #[serde(
        rename = "lastMutationCursorNanos",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_mutation_cursor_nanos: Option<i64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SwarmResourceKind {
    Entity,
    TemporalEntity,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SwarmOperation {
    Upsert,
    Delete,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SwarmEventKind {
    Created,
    Updated,
    Deleted,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SwarmMutationDocument {
    pub tenant: String,
    #[serde(rename = "mutationId")]
    pub mutation_id: String,
    #[serde(rename = "resourceKind")]
    pub resource_kind: SwarmResourceKind,
    pub operation: SwarmOperation,
    #[serde(rename = "entityId")]
    pub entity_id: String,
    #[serde(rename = "versionAt")]
    pub version_at: String,
    #[serde(rename = "versionAtNanos")]
    pub version_at_nanos: i64,
    #[serde(rename = "recordedAt")]
    pub recorded_at: String,
    #[serde(rename = "recordedAtNanos")]
    pub recorded_at_nanos: i64,
    #[serde(rename = "sourcePeerId")]
    pub source_peer_id: String,
    #[serde(rename = "eventKind")]
    pub event_kind: SwarmEventKind,
    #[serde(default)]
    pub changed_attributes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<Value>,
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

impl From<EntityEventKind> for SwarmEventKind {
    /// Maps local entity event kinds into replicated swarm event kinds.
    fn from(value: EntityEventKind) -> Self {
        match value {
            EntityEventKind::Created => Self::Created,
            EntityEventKind::Updated => Self::Updated,
            EntityEventKind::Deleted => Self::Deleted,
        }
    }
}
