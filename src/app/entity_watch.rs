use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use log::{info, warn};
use serde_json::Value;

use crate::{
    app::state::AppState,
    domain::types::{EntityEvent, EntityEventKind},
    error::BrokerError,
    query::planner::MongoQueryPlan,
    services::notifications,
    utils::json::{entity_attribute_names, reserved_member},
};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct EntityKey {
    tenant: String,
    entity_id: String,
}

#[derive(Clone, Default)]
pub struct EntityWatchState {
    pending_local: Arc<Mutex<HashMap<EntityKey, Option<Value>>>>,
}

impl EntityWatchState {
    /// Records local write so watcher can suppress duplicate subscription delivery.
    pub fn record_local_upsert(&self, tenant: &str, entity_id: &str, entity: &Value) {
        self.pending_local
            .lock()
            .unwrap()
            .insert(key(tenant, entity_id), Some(entity.clone()));
    }

    /// Records local delete so watcher can suppress duplicate subscription delivery.
    pub fn record_local_delete(&self, tenant: &str, entity_id: &str) {
        self.pending_local
            .lock()
            .unwrap()
            .insert(key(tenant, entity_id), None);
    }

    fn suppress_if_local(&self, key: &EntityKey, current: Option<&Value>) -> bool {
        let mut pending = self.pending_local.lock().unwrap();
        match pending.get(key) {
            Some(Some(expected)) if current == Some(expected) => {
                pending.remove(key);
                true
            }
            Some(None) if current.is_none() => {
                pending.remove(key);
                true
            }
            _ => false,
        }
    }
}

/// Starts polling worker that turns DefraDB-replicated entity changes into local notifications.
pub fn spawn(state: AppState) {
    if !state.config.entity_watch_enabled {
        return;
    }

    let interval = Duration::from_millis(state.config.entity_watch_interval_ms.max(100));
    tokio::spawn(async move {
        if let Err(error) = entity_watch_loop(state, interval).await {
            warn!("entity watch worker stopped: {error}");
        }
    });
}

async fn entity_watch_loop(state: AppState, interval: Duration) -> Result<(), BrokerError> {
    info!("entity watch worker started");
    let mut previous = HashMap::new();

    loop {
        let current = match collect_snapshots(&state).await {
            Ok(current) => current,
            Err(error) => {
                warn!("entity watch poll failed: {error}");
                tokio::time::sleep(interval).await;
                continue;
            }
        };

        let mut events = Vec::new();
        for (entity_key, document) in &current {
            match previous.get(entity_key) {
                None => events.push((
                    entity_key.clone(),
                    document.clone(),
                    EntityEvent {
                        kind: EntityEventKind::Created,
                        changed_attributes: entity_attribute_names(document),
                    },
                )),
                Some(previous_document) if previous_document != document => events.push((
                    entity_key.clone(),
                    document.clone(),
                    EntityEvent {
                        kind: EntityEventKind::Updated,
                        changed_attributes: diff_attributes(previous_document, document),
                    },
                )),
                _ => {}
            }
        }

        for (entity_key, document) in &previous {
            if !current.contains_key(entity_key) {
                events.push((
                    entity_key.clone(),
                    document.clone(),
                    EntityEvent {
                        kind: EntityEventKind::Deleted,
                        changed_attributes: entity_attribute_names(document),
                    },
                ));
            }
        }

        for (entity_key, document, event) in events {
            let current_document = current.get(&entity_key);
            if state
                .entity_watch
                .suppress_if_local(&entity_key, current_document)
            {
                continue;
            }

            if let Err(error) =
                notifications::enqueue_notifications(&state, &entity_key.tenant, &document, &event)
                    .await
            {
                warn!(
                    "entity watch notification failed for {} in tenant {}: {}",
                    entity_key.entity_id, entity_key.tenant, error
                );
            }
        }

        previous = current;
        tokio::time::sleep(interval).await;
    }
}

async fn collect_snapshots(state: &AppState) -> Result<HashMap<EntityKey, Value>, BrokerError> {
    let tenants = watched_tenants(state).await?;
    let mut snapshots = HashMap::new();

    for tenant in tenants {
        let documents = state
            .repositories
            .entities
            .query(&tenant, &MongoQueryPlan::default())
            .await?;
        for document in documents {
            snapshots.insert(key(&tenant, &document.ngsi_id), document.doc);
        }
    }

    Ok(snapshots)
}

async fn watched_tenants(state: &AppState) -> Result<Vec<String>, BrokerError> {
    let mut tenants = HashSet::new();
    tenants.extend(state.repositories.entities.list_tenants().await?);
    tenants.extend(state.repositories.subscriptions.list_tenants().await?);
    Ok(tenants.into_iter().collect())
}

fn diff_attributes(previous: &Value, current: &Value) -> Vec<String> {
    let previous = previous
        .as_object()
        .map(|object| object.iter().collect::<HashMap<_, _>>())
        .unwrap_or_default();
    let current = current
        .as_object()
        .map(|object| object.iter().collect::<HashMap<_, _>>())
        .unwrap_or_default();

    let mut changed = HashSet::new();
    for key in previous.keys().chain(current.keys()) {
        if reserved_member(key) {
            continue;
        }
        if previous.get(key) != current.get(key) {
            changed.insert((*key).to_string());
        }
    }

    changed.into_iter().collect()
}

fn key(tenant: &str, entity_id: &str) -> EntityKey {
    EntityKey {
        tenant: tenant.to_string(),
        entity_id: entity_id.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::diff_attributes;

    #[test]
    fn diff_attributes_ignores_reserved_members() {
        let previous = json!({
            "id": "urn:ngsi-ld:Vehicle:1",
            "type": "Vehicle",
            "speed": {"type": "Property", "value": 10},
            "modifiedAt": "2024-01-01T00:00:00Z"
        });
        let current = json!({
            "id": "urn:ngsi-ld:Vehicle:1",
            "type": "Vehicle",
            "speed": {"type": "Property", "value": 20},
            "modifiedAt": "2024-01-01T00:00:01Z"
        });

        let changed = diff_attributes(&previous, &current);

        assert_eq!(changed, vec!["speed".to_string()]);
    }
}
