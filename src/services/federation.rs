use std::time::Duration;

use log::warn;
use serde_json::Value;
use tokio::time::sleep;
use uuid::Uuid;

use crate::{
    app::state::AppState,
    config::AppConfig,
    domain::types::{
        EntityEvent, PeerDocument, PeerStatus, StoredDocument, SwarmEventKind,
        SwarmMutationDocument, SwarmOperation, SwarmResourceKind, TemporalEntityDocument,
    },
    error::BrokerError,
    federation::{
        p2p::{SwimEventEnvelope, SwimEventKind, SwimPeer},
        queue::{QueueMessage, SwimEnvelope},
    },
    persistence::repository::{
        EntityRepository, PeerRepository, SwarmMutationRepository, TemporalRepository,
    },
    services::notifications,
    utils::time::{now_timestamp, now_timestamp_nanos, timestamp_to_nanos},
};

#[derive(Clone, Debug)]
pub struct SwarmMutationInput {
    pub tenant: String,
    pub resource_kind: SwarmResourceKind,
    pub operation: SwarmOperation,
    pub entity_id: String,
    pub version_at: String,
    pub source_peer_id: String,
    pub event_kind: SwarmEventKind,
    pub changed_attributes: Vec<String>,
    pub snapshot: Option<Value>,
}

/// Runs periodic tenant-wide SWIM maintenance loop.
pub async fn start_swarm_sync_worker(state: AppState) {
    if !state.config.p2p_enabled {
        return;
    }

    loop {
        let tenants = discover_swarm_tenants(&state).await;
        for tenant in tenants {
            if let Err(error) = sync_swarm_once(&state, &tenant).await {
                warn!("SWIM round failed for tenant {tenant}: {error}");
            }
        }

        sleep(Duration::from_millis(
            state.config.p2p_sync_interval_ms.max(1),
        ))
        .await;
    }
}

/// Executes one SWIM maintenance round for tenant.
pub async fn sync_swarm_once(state: &AppState, tenant: &str) -> Result<(), BrokerError> {
    if !state.config.p2p_enabled {
        return Ok(());
    }

    sync_seed_peers(state, tenant).await?;
    announce_local_peer(state, tenant).await?;
    expire_peer_membership(state, tenant).await?;

    let peers = state.repositories.peers.list(tenant).await?;
    for peer in peers {
        if should_skip_peer_sync(state, &peer) {
            continue;
        }
        if let Err(error) = sync_peer_mutations(state, tenant, &peer).await {
            warn!(
                "failed syncing peer {} for tenant {tenant}: {error}",
                peer.peer_id
            );
        }
    }

    Ok(())
}

/// Ensures configured seed peers exist in membership store.
pub async fn sync_seed_peers(state: &AppState, tenant: &str) -> Result<(), BrokerError> {
    if !state.config.p2p_enabled {
        return Ok(());
    }

    for endpoint in &state.config.p2p_seeds {
        let endpoint = normalize_peer_endpoint(endpoint);
        let peer_id = endpoint.clone();
        let existing = state.repositories.peers.get(tenant, &peer_id).await?;
        state
            .repositories
            .peers
            .upsert(PeerDocument {
                tenant: tenant.to_string(),
                peer_id,
                endpoint: endpoint.clone(),
                aliases: merge_strings(
                    existing
                        .as_ref()
                        .map(|peer| peer.aliases.clone())
                        .unwrap_or_default(),
                    vec![endpoint.clone()],
                ),
                capabilities: existing
                    .as_ref()
                    .map(|peer| peer.capabilities.clone())
                    .unwrap_or_else(|| vec!["p2p".to_string(), "swim".to_string()]),
                neighbors: existing
                    .as_ref()
                    .map(|peer| peer.neighbors.clone())
                    .unwrap_or_default(),
                status: existing
                    .as_ref()
                    .map(|peer| peer.status)
                    .unwrap_or(PeerStatus::Suspect),
                incarnation: existing.as_ref().map(|peer| peer.incarnation).unwrap_or(0),
                updated_at: existing
                    .as_ref()
                    .map(|peer| peer.updated_at.clone())
                    .unwrap_or_else(now_timestamp),
                last_synced_at: existing
                    .as_ref()
                    .and_then(|peer| peer.last_synced_at.clone()),
                last_synced_at_nanos: existing.as_ref().and_then(|peer| peer.last_synced_at_nanos),
                last_mutation_cursor_nanos: existing
                    .as_ref()
                    .and_then(|peer| peer.last_mutation_cursor_nanos),
            })
            .await?;
    }

    Ok(())
}

/// Announces current broker as alive to known neighbors.
pub async fn announce_local_peer(state: &AppState, tenant: &str) -> Result<(), BrokerError> {
    announce_local_peer_with_min_incarnation(state, tenant, None).await
}

/// Accepts single inbound SWIM event from peer.
pub async fn accept_swim_event(
    state: &AppState,
    tenant: &str,
    event: &SwimEventEnvelope,
) -> Result<(), BrokerError> {
    apply_swim_event(state, tenant, event).await
}

/// Lists local mutation log entries after optional cursor.
pub async fn list_swarm_mutations_since(
    state: &AppState,
    tenant: &str,
    since_nanos: Option<i64>,
    limit: usize,
) -> Result<Vec<SwarmMutationDocument>, BrokerError> {
    use crate::persistence::repository::SwarmMutationRepository;

    state
        .repositories
        .swarm_mutations
        .list_since(tenant, since_nanos, limit)
        .await
}

/// Persists local mutation record and publishes it to peers.
pub async fn record_local_swarm_mutation(
    state: &AppState,
    input: SwarmMutationInput,
) -> Result<SwarmMutationDocument, BrokerError> {
    let version_at_nanos =
        timestamp_to_nanos(&input.version_at).unwrap_or_else(now_timestamp_nanos);
    let mutation = SwarmMutationDocument {
        tenant: input.tenant,
        mutation_id: format!(
            "urn:ngsi-ld:SwarmMutation:{}:{}",
            input.source_peer_id,
            Uuid::new_v4()
        ),
        resource_kind: input.resource_kind,
        operation: input.operation,
        entity_id: input.entity_id,
        version_at: input.version_at,
        version_at_nanos,
        recorded_at: now_timestamp(),
        recorded_at_nanos: now_timestamp_nanos(),
        source_peer_id: input.source_peer_id,
        event_kind: input.event_kind,
        changed_attributes: input.changed_attributes,
        snapshot: input.snapshot,
    };

    state
        .repositories
        .swarm_mutations
        .insert(mutation.clone())
        .await?;

    if state.config.p2p_enabled {
        publish_swarm_mutation(state, &mutation).await?;
    }

    Ok(mutation)
}

/// Announces local peer using incarnation floor when conflict detected.
async fn announce_local_peer_with_min_incarnation(
    state: &AppState,
    tenant: &str,
    min_incarnation: Option<u64>,
) -> Result<(), BrokerError> {
    let peer = upsert_local_peer(state, tenant, min_incarnation).await?;
    let neighbors = peer.neighbors.clone();
    publish_swim_event(
        state,
        &neighbors,
        SwimEventEnvelope {
            kind: SwimEventKind::Alive,
            source: peer.clone(),
            peer,
            mutation: None,
            recorded_at: now_timestamp(),
        },
    )
    .await
}

/// Upserts current broker membership record and increments incarnation.
async fn upsert_local_peer(
    state: &AppState,
    tenant: &str,
    min_incarnation: Option<u64>,
) -> Result<SwimPeer, BrokerError> {
    let existing = state
        .repositories
        .peers
        .get(tenant, &state.config.broker_id)
        .await?;
    let incarnation = existing
        .as_ref()
        .map(|peer| peer.incarnation.saturating_add(1))
        .unwrap_or(1)
        .max(min_incarnation.unwrap_or(0));
    let peer = local_swim_peer(&state.config, tenant, incarnation, PeerStatus::Alive);
    merge_peer(state, tenant, peer.clone()).await?;
    Ok(peer)
}

/// Loads current broker membership record, creating one if absent.
async fn current_local_peer(state: &AppState, tenant: &str) -> Result<SwimPeer, BrokerError> {
    if let Some(peer) = state
        .repositories
        .peers
        .get(tenant, &state.config.broker_id)
        .await?
    {
        return Ok(swim_peer_from_document(&peer));
    }

    let peer = local_swim_peer(&state.config, tenant, 1, PeerStatus::Alive);
    merge_peer(state, tenant, peer.clone()).await?;
    Ok(peer)
}

/// Broadcasts mutation event to neighbor peers.
async fn publish_swarm_mutation(
    state: &AppState,
    mutation: &SwarmMutationDocument,
) -> Result<(), BrokerError> {
    let peer = current_local_peer(state, &mutation.tenant).await?;
    let neighbors = peer.neighbors.clone();
    publish_swim_event(
        state,
        &neighbors,
        SwimEventEnvelope {
            kind: SwimEventKind::Mutation,
            source: peer.clone(),
            peer,
            mutation: Some(mutation.clone()),
            recorded_at: now_timestamp(),
        },
    )
    .await
}

/// Broadcasts peer status change to neighbor peers.
async fn publish_peer_status(
    state: &AppState,
    tenant: &str,
    peer: &PeerDocument,
    status: PeerStatus,
) -> Result<(), BrokerError> {
    let source = current_local_peer(state, tenant).await?;
    let neighbors = source.neighbors.clone();
    let event_peer = SwimPeer {
        status,
        tenant: Some(tenant.to_string()),
        ..swim_peer_from_document(peer)
    };
    let kind = match status {
        PeerStatus::Alive => SwimEventKind::Alive,
        PeerStatus::Suspect => SwimEventKind::Suspect,
        PeerStatus::Dead => SwimEventKind::Dead,
    };
    publish_swim_event(
        state,
        &neighbors,
        SwimEventEnvelope {
            kind,
            source,
            peer: event_peer,
            mutation: None,
            recorded_at: now_timestamp(),
        },
    )
    .await
}

/// Enqueues SWIM envelope for each target endpoint.
async fn publish_swim_event(
    state: &AppState,
    endpoints: &[String],
    envelope: SwimEventEnvelope,
) -> Result<(), BrokerError> {
    for endpoint in endpoints {
        let endpoint = normalize_peer_endpoint(endpoint);
        state
            .queue
            .enqueue(QueueMessage::Swim(SwimEnvelope {
                tenant: envelope
                    .peer
                    .tenant
                    .clone()
                    .unwrap_or_else(|| "default".to_string()),
                endpoint,
                payload: envelope.clone(),
            }))
            .await
            .map_err(|error| {
                BrokerError::internal(format!("failed to enqueue SWIM message: {error}"))
            })?;
    }
    Ok(())
}

/// Merges inbound peer state into persisted membership document.
async fn merge_peer(
    state: &AppState,
    tenant: &str,
    peer: SwimPeer,
) -> Result<PeerDocument, BrokerError> {
    let incoming = peer_document_from_swim(tenant, peer, now_timestamp());
    let merged = match state
        .repositories
        .peers
        .get(tenant, &incoming.peer_id)
        .await?
    {
        Some(existing) => merge_peer_documents(existing, incoming),
        None => incoming,
    };
    state.repositories.peers.upsert(merged.clone()).await?;
    Ok(merged)
}

/// Advances peers from alive to suspect to dead on timeout.
async fn expire_peer_membership(state: &AppState, tenant: &str) -> Result<(), BrokerError> {
    let now_nanos = now_timestamp_nanos();
    let suspect_timeout = state.config.p2p_swim_suspect_timeout_ms as i64 * 1_000_000;

    for peer in state.repositories.peers.list(tenant).await? {
        if same_local_peer(&peer, &state.config) {
            continue;
        }
        let Some(updated_at_nanos) = timestamp_to_nanos(&peer.updated_at) else {
            continue;
        };
        let age = now_nanos.saturating_sub(updated_at_nanos);

        if peer.status == PeerStatus::Alive && age >= suspect_timeout {
            publish_peer_status(state, tenant, &peer, PeerStatus::Suspect).await?;
        } else if peer.status == PeerStatus::Suspect && age >= suspect_timeout.saturating_mul(2) {
            publish_peer_status(state, tenant, &peer, PeerStatus::Dead).await?;
        }
    }

    Ok(())
}

/// Pulls unseen mutation log entries from one peer.
async fn sync_peer_mutations(
    state: &AppState,
    tenant: &str,
    peer: &PeerDocument,
) -> Result<(), BrokerError> {
    let since_nanos = peer.last_mutation_cursor_nanos;
    let mut url = crate::federation::queue::build_target_url(
        &peer.endpoint,
        "/internal/swim/mutations",
        Some("limit=200"),
    )
    .map_err(|error| {
        BrokerError::internal(format!("failed to build SWIM mutation URL: {error}"))
    })?;
    if let Some(since_nanos) = since_nanos {
        url.push_str(&format!("&sinceNanos={since_nanos}"));
    }

    let response = state
        .http_client
        .get(url)
        .header(crate::context::headers::HEADER_TENANT, tenant)
        .send()
        .await?;
    if !response.status().is_success() {
        return Err(BrokerError::internal(format!(
            "peer {} returned {} while serving SWIM mutations",
            peer.peer_id,
            response.status()
        )));
    }

    let mutations = response.json::<Vec<SwarmMutationDocument>>().await?;
    let mut cursor = since_nanos;
    for mutation in mutations {
        cursor = Some(cursor.map_or(mutation.recorded_at_nanos, |current| {
            current.max(mutation.recorded_at_nanos)
        }));

        if state
            .repositories
            .swarm_mutations
            .get(tenant, &mutation.mutation_id)
            .await?
            .is_some()
        {
            continue;
        }

        apply_swarm_mutation(state, tenant, &mutation).await?;
        state.repositories.swarm_mutations.insert(mutation).await?;
    }

    let mut updated_peer = peer.clone();
    let now = now_timestamp();
    updated_peer.status = PeerStatus::Alive;
    updated_peer.updated_at = now.clone();
    updated_peer.last_synced_at = Some(now.clone());
    updated_peer.last_synced_at_nanos = timestamp_to_nanos(&now);
    updated_peer.last_mutation_cursor_nanos = cursor.or(peer.last_mutation_cursor_nanos);
    state.repositories.peers.upsert(updated_peer).await
}

/// Applies inbound SWIM event and deduplicates mutation replay.
async fn apply_swim_event(
    state: &AppState,
    tenant: &str,
    event: &SwimEventEnvelope,
) -> Result<(), BrokerError> {
    if event.kind != SwimEventKind::Mutation
        && event.kind != SwimEventKind::Alive
        && same_local_swim_peer(&event.peer, &state.config)
    {
        return announce_local_peer_with_min_incarnation(
            state,
            tenant,
            Some(event.peer.incarnation.saturating_add(1)),
        )
        .await;
    }

    record_peer(state, tenant, event.peer.clone()).await?;

    if event.kind != SwimEventKind::Mutation {
        return Ok(());
    }

    let Some(mutation) = event.mutation.clone() else {
        return Ok(());
    };
    if state
        .repositories
        .swarm_mutations
        .get(tenant, &mutation.mutation_id)
        .await?
        .is_some()
    {
        return Ok(());
    }

    apply_swarm_mutation(state, tenant, &mutation).await?;
    state.repositories.swarm_mutations.insert(mutation).await
}

/// Persists peer state learned from remote SWIM event.
pub async fn record_peer(
    state: &AppState,
    tenant: &str,
    peer: SwimPeer,
) -> Result<(), BrokerError> {
    merge_peer(state, tenant, peer).await.map(|_| ())
}

/// Dispatches swarm mutation to entity or temporal apply path.
async fn apply_swarm_mutation(
    state: &AppState,
    tenant: &str,
    mutation: &SwarmMutationDocument,
) -> Result<(), BrokerError> {
    match mutation.resource_kind {
        SwarmResourceKind::Entity => apply_entity_mutation(state, tenant, mutation).await,
        SwarmResourceKind::TemporalEntity => apply_temporal_mutation(state, tenant, mutation).await,
    }
}

/// Applies entity mutation snapshot or delete locally.
async fn apply_entity_mutation(
    state: &AppState,
    tenant: &str,
    mutation: &SwarmMutationDocument,
) -> Result<(), BrokerError> {
    match mutation.operation {
        SwarmOperation::Delete => {
            let should_apply = state
                .repositories
                .entities
                .get(tenant, &mutation.entity_id)
                .await?
                .map(|existing| {
                    mutation_is_newer(existing.doc.get("modifiedAt"), &mutation.version_at)
                })
                .unwrap_or(true);
            if should_apply {
                state
                    .repositories
                    .entities
                    .delete(tenant, &mutation.entity_id)
                    .await?;
            }
        }
        SwarmOperation::Upsert => {
            let Some(snapshot) = mutation.snapshot.clone() else {
                return Ok(());
            };
            let should_apply = state
                .repositories
                .entities
                .get(tenant, &mutation.entity_id)
                .await?
                .map(|existing| {
                    mutation_is_newer(existing.doc.get("modifiedAt"), &mutation.version_at)
                })
                .unwrap_or(true);
            if !should_apply {
                return Ok(());
            }

            state
                .repositories
                .entities
                .replace(StoredDocument {
                    tenant: tenant.to_string(),
                    ngsi_id: mutation.entity_id.clone(),
                    doc: snapshot.clone(),
                })
                .await?;

            notifications::enqueue_notifications(
                state,
                tenant,
                &snapshot,
                &EntityEvent {
                    kind: match mutation.event_kind {
                        SwarmEventKind::Created => crate::domain::types::EntityEventKind::Created,
                        SwarmEventKind::Updated => crate::domain::types::EntityEventKind::Updated,
                        SwarmEventKind::Deleted => crate::domain::types::EntityEventKind::Deleted,
                    },
                    changed_attributes: mutation.changed_attributes.clone(),
                },
            )
            .await?;
        }
    }

    Ok(())
}

/// Applies temporal mutation snapshot or delete locally.
async fn apply_temporal_mutation(
    state: &AppState,
    tenant: &str,
    mutation: &SwarmMutationDocument,
) -> Result<(), BrokerError> {
    match mutation.operation {
        SwarmOperation::Delete => {
            let should_apply = state
                .repositories
                .temporals
                .get(tenant, &mutation.entity_id)
                .await?
                .map(|existing| {
                    mutation_is_newer(existing.doc.get("modifiedAt"), &mutation.version_at)
                })
                .unwrap_or(true);
            if should_apply {
                state
                    .repositories
                    .temporals
                    .delete(tenant, &mutation.entity_id)
                    .await?;
            }
        }
        SwarmOperation::Upsert => {
            let Some(snapshot) = mutation.snapshot.clone() else {
                return Ok(());
            };
            let should_apply = state
                .repositories
                .temporals
                .get(tenant, &mutation.entity_id)
                .await?
                .map(|existing| {
                    mutation_is_newer(existing.doc.get("modifiedAt"), &mutation.version_at)
                })
                .unwrap_or(true);
            if !should_apply {
                return Ok(());
            }

            let history = snapshot
                .get("__swarmHistory")
                .and_then(Value::as_array)
                .cloned()
                .map(|items| {
                    items
                        .into_iter()
                        .filter_map(|item| serde_json::from_value(item).ok())
                        .collect()
                })
                .unwrap_or_default();
            let mut doc = snapshot;
            if let Some(object) = doc.as_object_mut() {
                object.remove("__swarmHistory");
            }
            state
                .repositories
                .temporals
                .upsert(TemporalEntityDocument {
                    tenant: tenant.to_string(),
                    ngsi_id: mutation.entity_id.clone(),
                    doc,
                    history,
                })
                .await?;
        }
    }

    Ok(())
}

/// Discovers tenants that currently have swarm-relevant data.
async fn discover_swarm_tenants(state: &AppState) -> Vec<String> {
    let mut tenants = vec!["default".to_string()];
    for collection in [
        state.db.collection::<mongodb::bson::Document>("entities"),
        state
            .db
            .collection::<mongodb::bson::Document>("temporal_entities"),
        state.db.collection::<mongodb::bson::Document>("peers"),
    ] {
        if let Ok(values) = collection.distinct("tenant", mongodb::bson::doc! {}).await {
            for value in values {
                if let Some(tenant) = value.as_str()
                    && !tenants.iter().any(|item| item == tenant)
                {
                    tenants.push(tenant.to_string());
                }
            }
        }
    }
    tenants
}

/// Builds local broker membership payload for SWIM messages.
fn local_swim_peer(
    config: &AppConfig,
    tenant: &str,
    incarnation: u64,
    status: PeerStatus,
) -> SwimPeer {
    SwimPeer {
        peer_id: config.broker_id.clone(),
        endpoint: config.public_endpoint.clone(),
        aliases: vec![config.broker_id.clone(), config.public_endpoint.clone()],
        capabilities: vec![
            "p2p".to_string(),
            "swim".to_string(),
            "swarm-sync".to_string(),
        ],
        neighbors: config
            .p2p_seeds
            .iter()
            .map(|seed| normalize_peer_endpoint(seed))
            .collect(),
        status,
        incarnation,
        tenant: Some(tenant.to_string()),
    }
}

/// Converts persisted peer document into SWIM wire model.
fn swim_peer_from_document(peer: &PeerDocument) -> SwimPeer {
    SwimPeer {
        peer_id: peer.peer_id.clone(),
        endpoint: peer.endpoint.clone(),
        aliases: peer.aliases.clone(),
        capabilities: peer.capabilities.clone(),
        neighbors: peer.neighbors.clone(),
        status: peer.status,
        incarnation: peer.incarnation,
        tenant: Some(peer.tenant.clone()),
    }
}

/// Converts inbound SWIM peer into persisted peer document.
fn peer_document_from_swim(tenant: &str, peer: SwimPeer, updated_at: String) -> PeerDocument {
    PeerDocument {
        tenant: tenant.to_string(),
        peer_id: peer.peer_id,
        endpoint: normalize_peer_endpoint(&peer.endpoint),
        aliases: peer.aliases,
        capabilities: peer.capabilities,
        neighbors: peer
            .neighbors
            .into_iter()
            .map(|neighbor| normalize_peer_endpoint(&neighbor))
            .collect(),
        status: peer.status,
        incarnation: peer.incarnation,
        updated_at,
        last_synced_at: None,
        last_synced_at_nanos: None,
        last_mutation_cursor_nanos: None,
    }
}

/// Merges existing and incoming peer documents using SWIM ordering rules.
fn merge_peer_documents(existing: PeerDocument, incoming: PeerDocument) -> PeerDocument {
    let use_incoming_state = incoming_membership_is_newer(&existing, &incoming);
    PeerDocument {
        tenant: existing.tenant,
        peer_id: existing.peer_id,
        endpoint: incoming.endpoint,
        aliases: merge_strings(existing.aliases, incoming.aliases),
        capabilities: merge_strings(existing.capabilities, incoming.capabilities),
        neighbors: merge_strings(existing.neighbors, incoming.neighbors),
        status: if use_incoming_state {
            incoming.status
        } else {
            existing.status
        },
        incarnation: if use_incoming_state {
            incoming.incarnation
        } else {
            existing.incarnation
        },
        updated_at: if use_incoming_state {
            incoming.updated_at
        } else {
            existing.updated_at
        },
        last_synced_at: existing.last_synced_at,
        last_synced_at_nanos: existing.last_synced_at_nanos,
        last_mutation_cursor_nanos: existing.last_mutation_cursor_nanos,
    }
}

/// Returns true when incoming membership state supersedes existing state.
fn incoming_membership_is_newer(existing: &PeerDocument, incoming: &PeerDocument) -> bool {
    incoming.incarnation > existing.incarnation
        || (incoming.incarnation == existing.incarnation
            && status_rank(incoming.status) > status_rank(existing.status))
        || (incoming.incarnation == existing.incarnation
            && incoming.status == existing.status
            && timestamp_to_nanos(&incoming.updated_at).unwrap_or_default()
                >= timestamp_to_nanos(&existing.updated_at).unwrap_or_default())
}

/// Extends first string list with unique non-empty values from second.
fn merge_strings(mut left: Vec<String>, right: Vec<String>) -> Vec<String> {
    for item in right {
        if !item.is_empty() && !left.iter().any(|existing| existing == &item) {
            left.push(item);
        }
    }
    left
}

/// Ranks SWIM statuses for tie-breaking.
fn status_rank(status: PeerStatus) -> u8 {
    match status {
        PeerStatus::Alive => 1,
        PeerStatus::Suspect => 2,
        PeerStatus::Dead => 3,
    }
}

/// Returns true when candidate mutation version is not older.
fn mutation_is_newer(current: Option<&Value>, candidate: &str) -> bool {
    let current = current.and_then(Value::as_str).and_then(timestamp_to_nanos);
    let candidate = timestamp_to_nanos(candidate);
    match (current, candidate) {
        (_, None) => true,
        (None, Some(_)) => true,
        (Some(current), Some(candidate)) => candidate >= current,
    }
}

/// Skips sync for current broker aliases and endpoint.
fn should_skip_peer_sync(state: &AppState, peer: &PeerDocument) -> bool {
    peer.peer_id == state.config.broker_id
        || peer.endpoint == state.config.public_endpoint
        || peer
            .aliases
            .iter()
            .any(|alias| alias == &state.config.broker_id || alias == &state.config.public_endpoint)
}

/// Normalizes peer endpoint to canonical NGSI-LD base URL.
fn normalize_peer_endpoint(endpoint: &str) -> String {
    if endpoint.ends_with("/ngsi-ld/v1") {
        endpoint.to_string()
    } else {
        format!("{}/ngsi-ld/v1", endpoint.trim_end_matches('/'))
    }
}

/// Returns true when persisted peer record refers to local broker.
fn same_local_peer(peer: &PeerDocument, config: &AppConfig) -> bool {
    peer.peer_id == config.broker_id
        || peer.endpoint == config.public_endpoint
        || peer
            .aliases
            .iter()
            .any(|alias| alias == &config.broker_id || alias == &config.public_endpoint)
}

/// Returns true when SWIM peer payload refers to local broker.
fn same_local_swim_peer(peer: &SwimPeer, config: &AppConfig) -> bool {
    peer.peer_id == config.broker_id
        || peer.endpoint == config.public_endpoint
        || peer
            .aliases
            .iter()
            .any(|alias| alias == &config.broker_id || alias == &config.public_endpoint)
}

#[cfg(test)]
mod tests {
    use std::{net::TcpListener, sync::Arc};

    use actix_web::{App, HttpResponse, HttpServer, http::StatusCode, web};
    use mongodb::options::{ClientOptions, ServerAddress};
    use serde_json::{Value, json};

    use super::*;
    use crate::{
        app::state::AppState,
        config::AppConfig,
        federation::queue::InMemoryEventQueue,
        persistence::{
            mongo::MongoRepositories,
            repository::{EntityRepository, PeerRepository, SwarmMutationRepository},
        },
    };

    #[derive(Clone)]
    struct MockMutationResponse {
        status: StatusCode,
        mutations: Vec<SwarmMutationDocument>,
    }

    async fn mock_swim_mutations(response: web::Data<MockMutationResponse>) -> HttpResponse {
        HttpResponse::build(response.status).json(&response.mutations)
    }

    fn test_state() -> AppState {
        let config = AppConfig::for_tests();
        let mut options = ClientOptions::default();
        options.hosts = vec![ServerAddress::Tcp {
            host: "127.0.0.1".to_string(),
            port: Some(27017),
        }];
        let client = mongodb::Client::with_options(options).unwrap();
        let db = client.database(&config.mongo_database);
        let repositories = MongoRepositories::new_without_indexes(&db);
        let queue = Arc::new(InMemoryEventQueue::default());

        AppState::new(config, db, repositories, queue).unwrap()
    }

    async fn spawn_mutation_server(
        status: StatusCode,
        mutations: Vec<SwarmMutationDocument>,
    ) -> (String, actix_web::dev::ServerHandle) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = HttpServer::new(move || {
            App::new()
                .app_data(web::Data::new(MockMutationResponse {
                    status,
                    mutations: mutations.clone(),
                }))
                .route(
                    "/internal/swim/mutations",
                    web::get().to(mock_swim_mutations),
                )
        })
        .listen(listener)
        .unwrap()
        .run();
        let handle = server.handle();
        actix_web::rt::spawn(server);

        (
            format!("http://{}:{}/ngsi-ld/v1", address.ip(), address.port()),
            handle,
        )
    }

    fn unused_peer_endpoint() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        format!("http://{}:{}/ngsi-ld/v1", address.ip(), address.port())
    }

    fn remote_peer(peer_id: &str, endpoint: &str, status: PeerStatus) -> PeerDocument {
        PeerDocument {
            tenant: "tenant-a".to_string(),
            peer_id: peer_id.to_string(),
            endpoint: endpoint.to_string(),
            aliases: vec![peer_id.to_string(), endpoint.to_string()],
            capabilities: vec!["swim".to_string(), "swarm-sync".to_string()],
            neighbors: Vec::new(),
            status,
            incarnation: 1,
            updated_at: "2024-01-01T00:00:00Z".to_string(),
            last_synced_at: None,
            last_synced_at_nanos: None,
            last_mutation_cursor_nanos: None,
        }
    }

    fn sample_mutation(
        peer_id: &str,
        entity_id: &str,
        recorded_at_nanos: i64,
    ) -> SwarmMutationDocument {
        SwarmMutationDocument {
            tenant: "tenant-a".to_string(),
            mutation_id: format!("urn:ngsi-ld:SwarmMutation:{peer_id}:{recorded_at_nanos}"),
            resource_kind: SwarmResourceKind::Entity,
            operation: SwarmOperation::Upsert,
            entity_id: entity_id.to_string(),
            version_at: "2024-01-02T00:00:00Z".to_string(),
            version_at_nanos: 1,
            recorded_at: "2024-01-02T00:00:01Z".to_string(),
            recorded_at_nanos,
            source_peer_id: peer_id.to_string(),
            event_kind: SwarmEventKind::Updated,
            changed_attributes: vec!["speed".to_string()],
            snapshot: Some(json!({
                "id": entity_id,
                "type": "Vehicle",
                "modifiedAt": "2024-01-02T00:00:00Z",
                "speed": {"type": "Property", "value": 42}
            })),
        }
    }

    fn peer(status: PeerStatus, incarnation: u64, updated_at: &str) -> PeerDocument {
        PeerDocument {
            tenant: "tenant-a".to_string(),
            peer_id: "peer-a".to_string(),
            endpoint: "http://peer-a:1026/ngsi-ld/v1".to_string(),
            aliases: vec!["peer-a".to_string()],
            capabilities: vec!["swim".to_string()],
            neighbors: Vec::new(),
            status,
            incarnation,
            updated_at: updated_at.to_string(),
            last_synced_at: None,
            last_synced_at_nanos: None,
            last_mutation_cursor_nanos: None,
        }
    }

    #[test]
    fn higher_incarnation_wins_membership_merge() {
        let merged = merge_peer_documents(
            peer(PeerStatus::Alive, 1, "2024-01-01T00:00:00Z"),
            peer(PeerStatus::Suspect, 2, "2024-01-02T00:00:00Z"),
        );

        assert_eq!(merged.status, PeerStatus::Suspect);
        assert_eq!(merged.incarnation, 2);
    }

    #[test]
    fn dead_wins_over_alive_at_same_incarnation() {
        let merged = merge_peer_documents(
            peer(PeerStatus::Alive, 3, "2024-01-01T00:00:00Z"),
            peer(PeerStatus::Dead, 3, "2024-01-02T00:00:00Z"),
        );

        assert_eq!(merged.status, PeerStatus::Dead);
        assert_eq!(merged.incarnation, 3);
    }

    #[test]
    fn normalizes_peer_endpoint_to_ngsi_base_path() {
        assert_eq!(
            normalize_peer_endpoint("http://peer-b:1026"),
            "http://peer-b:1026/ngsi-ld/v1"
        );
        assert_eq!(
            normalize_peer_endpoint("http://peer-b:1026/ngsi-ld/v1"),
            "http://peer-b:1026/ngsi-ld/v1"
        );
    }

    #[actix_web::test]
    async fn dead_peer_recovers_by_periodic_mutation_pull() {
        let state = test_state();
        let mutation = sample_mutation("peer-b", "urn:ngsi-ld:Vehicle:recovered", 10);
        let (endpoint, handle) =
            spawn_mutation_server(StatusCode::OK, vec![mutation.clone()]).await;

        state
            .repositories
            .peers
            .upsert(remote_peer("peer-b", &endpoint, PeerStatus::Dead))
            .await
            .unwrap();

        sync_swarm_once(&state, "tenant-a").await.unwrap();

        let stored = state
            .repositories
            .entities
            .get("tenant-a", &mutation.entity_id)
            .await
            .unwrap()
            .expect("entity to be synchronized after peer recovery");
        assert_eq!(stored.ngsi_id, mutation.entity_id);
        assert_eq!(
            stored.doc.get("type"),
            Some(&Value::String("Vehicle".to_string()))
        );

        let peer = state
            .repositories
            .peers
            .get("tenant-a", "peer-b")
            .await
            .unwrap()
            .expect("peer record");
        assert_eq!(peer.status, PeerStatus::Alive);
        assert_eq!(
            peer.last_mutation_cursor_nanos,
            Some(mutation.recorded_at_nanos)
        );
        assert!(peer.last_synced_at.is_some());

        let stored_mutation = state
            .repositories
            .swarm_mutations
            .get("tenant-a", &mutation.mutation_id)
            .await
            .unwrap();
        assert!(stored_mutation.is_some());

        handle.stop(true).await;
    }

    #[actix_web::test]
    async fn failing_peer_does_not_block_sync_with_other_peers() {
        let state = test_state();
        let failed_endpoint = unused_peer_endpoint();
        let mutation = sample_mutation("peer-good", "urn:ngsi-ld:Vehicle:good", 20);
        let (good_endpoint, handle) =
            spawn_mutation_server(StatusCode::OK, vec![mutation.clone()]).await;

        state
            .repositories
            .peers
            .upsert(remote_peer(
                "peer-failed",
                &failed_endpoint,
                PeerStatus::Alive,
            ))
            .await
            .unwrap();
        state
            .repositories
            .peers
            .upsert(remote_peer("peer-good", &good_endpoint, PeerStatus::Alive))
            .await
            .unwrap();

        sync_swarm_once(&state, "tenant-a").await.unwrap();

        let stored = state
            .repositories
            .entities
            .get("tenant-a", &mutation.entity_id)
            .await
            .unwrap();
        assert!(stored.is_some(), "healthy peer should still synchronize");

        let good_peer = state
            .repositories
            .peers
            .get("tenant-a", "peer-good")
            .await
            .unwrap()
            .expect("healthy peer record");
        assert_eq!(
            good_peer.last_mutation_cursor_nanos,
            Some(mutation.recorded_at_nanos)
        );
        assert!(good_peer.last_synced_at.is_some());

        let failed_peer = state
            .repositories
            .peers
            .get("tenant-a", "peer-failed")
            .await
            .unwrap()
            .expect("failing peer record");
        assert!(failed_peer.last_synced_at.is_none());

        handle.stop(true).await;
    }
}
