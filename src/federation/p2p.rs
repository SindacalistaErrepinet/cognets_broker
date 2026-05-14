use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::domain::types::{PeerStatus, SwarmMutationDocument};

/// SWIM view of peer membership shared between brokers.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SwimPeer {
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
}

/// SWIM event kinds used for membership and mutation dissemination.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SwimEventKind {
    Alive,
    Suspect,
    Dead,
    Mutation,
}

/// Wire envelope for root-level internal SWIM endpoints.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SwimEventEnvelope {
    pub kind: SwimEventKind,
    pub source: SwimPeer,
    pub peer: SwimPeer,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mutation: Option<SwarmMutationDocument>,
    #[serde(rename = "recordedAt")]
    pub recorded_at: String,
}
