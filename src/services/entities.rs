use std::{collections::HashMap, sync::Arc};

use actix_web::ResponseError;
use serde_json::Value;

use crate::{
    app::state::AppState,
    context::headers::RequestContext,
    domain::{
        batch::{BatchEntityError, BatchOperationResult, NotUpdatedDetails, UpdateResult},
        types::{
            EntityEvent, EntityEventKind, StoredDocument, SwarmEventKind, SwarmOperation,
            SwarmResourceKind,
        },
    },
    error::{BrokerError, ProblemDetails},
    persistence::repository::EntityRepository,
    query::{
        context::resolve_context_terms,
        planner::MongoQueryPlan,
        types::{EntityQuery, QueryResult, Representation},
    },
    services::{
        common::{
            build_query_match_options, changed_attribute_names, ensure_attribute_exists,
            ensure_fragment_id_matches, ensure_requested_type, entity_matches_query_with_options,
            prepare_entity_for_create, prepare_entity_for_replace, project_entity,
            query_needs_linked_graph, representation_from_request, validate_entity,
        },
        federation, notifications,
    },
    utils::{
        json::{apply_merge_patch, editable_fragment_members},
        time::now_timestamp,
    },
};

/// Queries local entities and applies NGSI-LD projection rules.
pub async fn query(
    state: &AppState,
    request: &actix_web::HttpRequest,
    context: &RequestContext,
    query: &EntityQuery,
) -> Result<QueryResult, BrokerError> {
    let representation =
        representation_from_request(request, query.format.as_deref(), query.options.as_deref());
    let plan = MongoQueryPlan::from_entity_query(query)?;
    let items = state
        .repositories
        .entities
        .query(&context.tenant, &plan)
        .await?;

    let entities = dedupe(items.into_iter().map(|item| item.doc).collect());
    let linked_entities = entity_query_linked_graph(state, context, query, &entities).await?;
    let mut matched = Vec::new();
    for entity in entities {
        let options = build_query_match_options(
            query,
            resolve_context_terms(
                &state.http_client,
                entity.get("@context"),
                context.link_header.as_deref(),
            )
            .await?,
            linked_entities.clone(),
        );
        if entity_matches_query_with_options(&entity, query, &options) {
            matched.push(entity);
        }
    }
    let entities = matched;
    let total_count = entities.len();

    let projected = entities
        .into_iter()
        .map(|entity| {
            project_entity(
                &entity,
                query.attrs.as_deref(),
                query.pick.as_deref(),
                query.omit.as_deref(),
                representation,
            )
        })
        .take(query.limit.unwrap_or(usize::MAX))
        .collect::<Vec<_>>();

    let body = if representation == Representation::GeoJson {
        serde_json::json!({"type": "FeatureCollection", "features": projected})
    } else {
        Value::Array(projected)
    };

    Ok(QueryResult { body, total_count })
}

/// Retrieves single entity by id from local tenant store.
pub async fn get(
    state: &AppState,
    request: &actix_web::HttpRequest,
    context: &RequestContext,
    entity_id: &str,
    query: &EntityQuery,
) -> Result<Value, BrokerError> {
    let representation =
        representation_from_request(request, query.format.as_deref(), query.options.as_deref());
    if let Some(document) = state
        .repositories
        .entities
        .get(&context.tenant, entity_id)
        .await?
    {
        ensure_requested_type(&document.doc, query.entity_type.as_deref())?;
        return Ok(project_entity(
            &document.doc,
            query.attrs.as_deref(),
            query.pick.as_deref(),
            query.omit.as_deref(),
            representation,
        ));
    }

    Err(BrokerError::NotFound(format!(
        "entity {entity_id} was not found"
    )))
}

/// Creates new entity, notifies subscriptions, and records swarm mutation.
pub async fn create(
    state: &AppState,
    context: &RequestContext,
    mut entity: Value,
    local_only: bool,
    _query_string: Option<String>,
) -> Result<String, BrokerError> {
    let entity_id = prepare_entity_for_create(&mut entity, context)?;
    if state
        .repositories
        .entities
        .get(&context.tenant, &entity_id)
        .await?
        .is_some()
    {
        return Err(BrokerError::Conflict(format!(
            "entity {entity_id} already exists"
        )));
    }

    state
        .repositories
        .entities
        .insert(StoredDocument {
            tenant: context.tenant.clone(),
            ngsi_id: entity_id.clone(),
            doc: entity.clone(),
        })
        .await?;

    notifications::enqueue_notifications(
        state,
        &context.tenant,
        &entity,
        &EntityEvent {
            kind: EntityEventKind::Created,
            changed_attributes: changed_attribute_names(&entity),
        },
    )
    .await?;

    if !local_only {
        federation::record_local_swarm_mutation(
            state,
            federation::SwarmMutationInput {
                tenant: context.tenant.clone(),
                resource_kind: SwarmResourceKind::Entity,
                operation: SwarmOperation::Upsert,
                entity_id: entity_id.clone(),
                version_at: entity
                    .get("modifiedAt")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                source_peer_id: state.config.broker_id.clone(),
                event_kind: SwarmEventKind::Created,
                changed_attributes: changed_attribute_names(&entity),
                snapshot: Some(entity.clone()),
            },
        )
        .await?;
    }

    Ok(entity_id)
}

/// Deletes entity, emits notifications, and records swarm deletion.
pub async fn delete(
    state: &AppState,
    context: &RequestContext,
    entity_id: &str,
    requested_type: Option<&str>,
    local_only: bool,
    _query_string: Option<String>,
) -> Result<(), BrokerError> {
    let document = state
        .repositories
        .entities
        .get(&context.tenant, entity_id)
        .await?
        .ok_or_else(|| BrokerError::NotFound(format!("entity {entity_id} was not found")))?;
    ensure_requested_type(&document.doc, requested_type)?;

    state
        .repositories
        .entities
        .delete(&context.tenant, entity_id)
        .await?;

    notifications::enqueue_notifications(
        state,
        &context.tenant,
        &document.doc,
        &EntityEvent {
            kind: EntityEventKind::Deleted,
            changed_attributes: changed_attribute_names(&document.doc),
        },
    )
    .await?;

    if !local_only {
        federation::record_local_swarm_mutation(
            state,
            federation::SwarmMutationInput {
                tenant: context.tenant.clone(),
                resource_kind: SwarmResourceKind::Entity,
                operation: SwarmOperation::Delete,
                entity_id: entity_id.to_string(),
                version_at: document
                    .doc
                    .get("modifiedAt")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                source_peer_id: state.config.broker_id.clone(),
                event_kind: SwarmEventKind::Deleted,
                changed_attributes: changed_attribute_names(&document.doc),
                snapshot: None,
            },
        )
        .await?;
    }

    Ok(())
}

/// Applies merge patch to entity and records resulting mutation.
pub async fn merge(
    state: &AppState,
    context: &RequestContext,
    entity_id: &str,
    requested_type: Option<&str>,
    patch: Value,
    local_only: bool,
    _query_string: Option<String>,
) -> Result<(), BrokerError> {
    ensure_fragment_id_matches(&patch, entity_id)?;
    let mut existing = state
        .repositories
        .entities
        .get(&context.tenant, entity_id)
        .await?
        .ok_or_else(|| BrokerError::NotFound(format!("entity {entity_id} was not found")))?;
    ensure_requested_type(&existing.doc, requested_type)?;

    apply_merge_patch(&mut existing.doc, &patch);
    if let Some(object) = existing.doc.as_object_mut() {
        object.insert("id".to_string(), Value::String(entity_id.to_string()));
        object.insert("modifiedAt".to_string(), Value::String(now_timestamp()));
    }
    state
        .repositories
        .entities
        .replace(existing.clone())
        .await?;

    notifications::enqueue_notifications(
        state,
        &context.tenant,
        &existing.doc,
        &EntityEvent {
            kind: EntityEventKind::Updated,
            changed_attributes: changed_attribute_names(&patch),
        },
    )
    .await?;

    if !local_only {
        federation::record_local_swarm_mutation(
            state,
            federation::SwarmMutationInput {
                tenant: context.tenant.clone(),
                resource_kind: SwarmResourceKind::Entity,
                operation: SwarmOperation::Upsert,
                entity_id: entity_id.to_string(),
                version_at: existing
                    .doc
                    .get("modifiedAt")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                source_peer_id: state.config.broker_id.clone(),
                event_kind: SwarmEventKind::Updated,
                changed_attributes: changed_attribute_names(&patch),
                snapshot: Some(existing.doc.clone()),
            },
        )
        .await?;
    }

    Ok(())
}

/// Replaces entire entity document while preserving fixed metadata.
pub async fn replace(
    state: &AppState,
    context: &RequestContext,
    entity_id: &str,
    requested_type: Option<&str>,
    mut entity: Value,
    local_only: bool,
    _query_string: Option<String>,
) -> Result<(), BrokerError> {
    let existing = state
        .repositories
        .entities
        .get(&context.tenant, entity_id)
        .await?
        .ok_or_else(|| BrokerError::NotFound(format!("entity {entity_id} was not found")))?;
    ensure_requested_type(&existing.doc, requested_type)?;
    prepare_entity_for_replace(
        &mut entity,
        entity_id,
        context,
        existing.doc.get("createdAt"),
    )?;

    state
        .repositories
        .entities
        .replace(StoredDocument {
            tenant: context.tenant.clone(),
            ngsi_id: entity_id.to_string(),
            doc: entity.clone(),
        })
        .await?;

    notifications::enqueue_notifications(
        state,
        &context.tenant,
        &entity,
        &EntityEvent {
            kind: EntityEventKind::Updated,
            changed_attributes: changed_attribute_names(&entity),
        },
    )
    .await?;

    if !local_only {
        federation::record_local_swarm_mutation(
            state,
            federation::SwarmMutationInput {
                tenant: context.tenant.clone(),
                resource_kind: SwarmResourceKind::Entity,
                operation: SwarmOperation::Upsert,
                entity_id: entity_id.to_string(),
                version_at: entity
                    .get("modifiedAt")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                source_peer_id: state.config.broker_id.clone(),
                event_kind: SwarmEventKind::Updated,
                changed_attributes: changed_attribute_names(&entity),
                snapshot: Some(entity.clone()),
            },
        )
        .await?;
    }

    Ok(())
}

/// Appends attributes and optionally rejects overwrites.
pub async fn append_attrs(
    state: &AppState,
    context: &RequestContext,
    entity_id: &str,
    requested_type: Option<&str>,
    fragment: Value,
    no_overwrite: bool,
    local_only: bool,
    _query_string: Option<String>,
) -> Result<UpdateResult, BrokerError> {
    ensure_fragment_id_matches(&fragment, entity_id)?;
    let mut existing = state
        .repositories
        .entities
        .get(&context.tenant, entity_id)
        .await?
        .ok_or_else(|| BrokerError::NotFound(format!("entity {entity_id} was not found")))?;
    ensure_requested_type(&existing.doc, requested_type)?;

    let attrs = editable_fragment_members(&fragment);
    let mut result = UpdateResult::default();
    if let Some(object) = existing.doc.as_object_mut() {
        for (key, value) in attrs {
            if no_overwrite && object.contains_key(&key) {
                result.not_updated.push(not_updated(
                    &key,
                    409,
                    format!("attribute {key} already exists"),
                ));
            } else {
                object.insert(key.clone(), value);
                result.updated.push(key);
            }
        }
        object.insert("modifiedAt".to_string(), Value::String(now_timestamp()));
    }

    state
        .repositories
        .entities
        .replace(existing.clone())
        .await?;
    if !result.updated.is_empty() {
        notifications::enqueue_notifications(
            state,
            &context.tenant,
            &existing.doc,
            &EntityEvent {
                kind: EntityEventKind::Updated,
                changed_attributes: result.updated.clone(),
            },
        )
        .await?;
    }
    if !local_only && !result.updated.is_empty() {
        federation::record_local_swarm_mutation(
            state,
            federation::SwarmMutationInput {
                tenant: context.tenant.clone(),
                resource_kind: SwarmResourceKind::Entity,
                operation: SwarmOperation::Upsert,
                entity_id: entity_id.to_string(),
                version_at: existing
                    .doc
                    .get("modifiedAt")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                source_peer_id: state.config.broker_id.clone(),
                event_kind: SwarmEventKind::Updated,
                changed_attributes: result.updated.clone(),
                snapshot: Some(existing.doc.clone()),
            },
        )
        .await?;
    }

    Ok(result)
}

/// Updates only attributes that already exist.
pub async fn update_attrs(
    state: &AppState,
    context: &RequestContext,
    entity_id: &str,
    requested_type: Option<&str>,
    fragment: Value,
    local_only: bool,
    _query_string: Option<String>,
) -> Result<UpdateResult, BrokerError> {
    ensure_fragment_id_matches(&fragment, entity_id)?;
    let mut existing = state
        .repositories
        .entities
        .get(&context.tenant, entity_id)
        .await?
        .ok_or_else(|| BrokerError::NotFound(format!("entity {entity_id} was not found")))?;
    ensure_requested_type(&existing.doc, requested_type)?;

    let attrs = editable_fragment_members(&fragment);
    let mut result = UpdateResult::default();
    if let Some(object) = existing.doc.as_object_mut() {
        for (key, value) in attrs {
            if object.contains_key(&key) {
                object.insert(key.clone(), value);
                result.updated.push(key);
            } else {
                result.not_updated.push(not_updated(
                    &key,
                    404,
                    format!("attribute {key} does not exist"),
                ));
            }
        }
        object.insert("modifiedAt".to_string(), Value::String(now_timestamp()));
    }

    state
        .repositories
        .entities
        .replace(existing.clone())
        .await?;
    if !result.updated.is_empty() {
        notifications::enqueue_notifications(
            state,
            &context.tenant,
            &existing.doc,
            &EntityEvent {
                kind: EntityEventKind::Updated,
                changed_attributes: result.updated.clone(),
            },
        )
        .await?;
    }
    if !local_only && !result.updated.is_empty() {
        federation::record_local_swarm_mutation(
            state,
            federation::SwarmMutationInput {
                tenant: context.tenant.clone(),
                resource_kind: SwarmResourceKind::Entity,
                operation: SwarmOperation::Upsert,
                entity_id: entity_id.to_string(),
                version_at: existing
                    .doc
                    .get("modifiedAt")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                source_peer_id: state.config.broker_id.clone(),
                event_kind: SwarmEventKind::Updated,
                changed_attributes: result.updated.clone(),
                snapshot: Some(existing.doc.clone()),
            },
        )
        .await?;
    }

    Ok(result)
}

/// Patches single attribute in place.
pub async fn patch_attr(
    state: &AppState,
    context: &RequestContext,
    entity_id: &str,
    attr_id: &str,
    requested_type: Option<&str>,
    patch: Value,
    local_only: bool,
    _query_string: Option<String>,
) -> Result<(), BrokerError> {
    let mut existing = state
        .repositories
        .entities
        .get(&context.tenant, entity_id)
        .await?
        .ok_or_else(|| BrokerError::NotFound(format!("entity {entity_id} was not found")))?;
    ensure_requested_type(&existing.doc, requested_type)?;
    ensure_attribute_exists(&existing.doc, attr_id)?;

    if let Some(object) = existing.doc.as_object_mut() {
        if let Some(attribute) = object.get_mut(attr_id) {
            apply_merge_patch(attribute, &patch);
        }
        object.insert("modifiedAt".to_string(), Value::String(now_timestamp()));
    }
    state
        .repositories
        .entities
        .replace(existing.clone())
        .await?;

    notifications::enqueue_notifications(
        state,
        &context.tenant,
        &existing.doc,
        &EntityEvent {
            kind: EntityEventKind::Updated,
            changed_attributes: vec![attr_id.to_string()],
        },
    )
    .await?;

    if !local_only {
        federation::record_local_swarm_mutation(
            state,
            federation::SwarmMutationInput {
                tenant: context.tenant.clone(),
                resource_kind: SwarmResourceKind::Entity,
                operation: SwarmOperation::Upsert,
                entity_id: entity_id.to_string(),
                version_at: existing
                    .doc
                    .get("modifiedAt")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                source_peer_id: state.config.broker_id.clone(),
                event_kind: SwarmEventKind::Updated,
                changed_attributes: vec![attr_id.to_string()],
                snapshot: Some(existing.doc.clone()),
            },
        )
        .await?;
    }
    Ok(())
}

/// Deletes one attribute from entity.
pub async fn delete_attr(
    state: &AppState,
    context: &RequestContext,
    entity_id: &str,
    attr_id: &str,
    requested_type: Option<&str>,
    local_only: bool,
    _query_string: Option<String>,
) -> Result<(), BrokerError> {
    let mut existing = state
        .repositories
        .entities
        .get(&context.tenant, entity_id)
        .await?
        .ok_or_else(|| BrokerError::NotFound(format!("entity {entity_id} was not found")))?;
    ensure_requested_type(&existing.doc, requested_type)?;
    ensure_attribute_exists(&existing.doc, attr_id)?;

    if let Some(object) = existing.doc.as_object_mut() {
        object.remove(attr_id);
        object.insert("modifiedAt".to_string(), Value::String(now_timestamp()));
    }
    state
        .repositories
        .entities
        .replace(existing.clone())
        .await?;

    notifications::enqueue_notifications(
        state,
        &context.tenant,
        &existing.doc,
        &EntityEvent {
            kind: EntityEventKind::Updated,
            changed_attributes: vec![attr_id.to_string()],
        },
    )
    .await?;

    if !local_only {
        federation::record_local_swarm_mutation(
            state,
            federation::SwarmMutationInput {
                tenant: context.tenant.clone(),
                resource_kind: SwarmResourceKind::Entity,
                operation: SwarmOperation::Upsert,
                entity_id: entity_id.to_string(),
                version_at: existing
                    .doc
                    .get("modifiedAt")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                source_peer_id: state.config.broker_id.clone(),
                event_kind: SwarmEventKind::Updated,
                changed_attributes: vec![attr_id.to_string()],
                snapshot: Some(existing.doc.clone()),
            },
        )
        .await?;
    }

    Ok(())
}

/// Replaces single attribute value.
pub async fn replace_attr(
    state: &AppState,
    context: &RequestContext,
    entity_id: &str,
    attr_id: &str,
    requested_type: Option<&str>,
    value: Value,
    local_only: bool,
    _query_string: Option<String>,
) -> Result<(), BrokerError> {
    let mut existing = state
        .repositories
        .entities
        .get(&context.tenant, entity_id)
        .await?
        .ok_or_else(|| BrokerError::NotFound(format!("entity {entity_id} was not found")))?;
    ensure_requested_type(&existing.doc, requested_type)?;
    ensure_attribute_exists(&existing.doc, attr_id)?;

    if let Some(object) = existing.doc.as_object_mut() {
        object.insert(attr_id.to_string(), value.clone());
        object.insert("modifiedAt".to_string(), Value::String(now_timestamp()));
    }
    state
        .repositories
        .entities
        .replace(existing.clone())
        .await?;

    notifications::enqueue_notifications(
        state,
        &context.tenant,
        &existing.doc,
        &EntityEvent {
            kind: EntityEventKind::Updated,
            changed_attributes: vec![attr_id.to_string()],
        },
    )
    .await?;

    if !local_only {
        federation::record_local_swarm_mutation(
            state,
            federation::SwarmMutationInput {
                tenant: context.tenant.clone(),
                resource_kind: SwarmResourceKind::Entity,
                operation: SwarmOperation::Upsert,
                entity_id: entity_id.to_string(),
                version_at: existing
                    .doc
                    .get("modifiedAt")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                source_peer_id: state.config.broker_id.clone(),
                event_kind: SwarmEventKind::Updated,
                changed_attributes: vec![attr_id.to_string()],
                snapshot: Some(existing.doc.clone()),
            },
        )
        .await?;
    }

    Ok(())
}

/// Creates batch of entities and returns partial failure report when needed.
pub async fn batch_create(
    state: &AppState,
    context: &RequestContext,
    entities: Vec<Value>,
    local_only: bool,
    _query_string: Option<String>,
) -> Result<Result<Vec<String>, BatchOperationResult>, BrokerError> {
    let mut created = Vec::new();
    let mut result = BatchOperationResult::default();
    let mut docs = Vec::new();

    for mut entity in entities {
        match prepare_entity_for_create(&mut entity, context) {
            Ok(entity_id) => {
                if state
                    .repositories
                    .entities
                    .get(&context.tenant, &entity_id)
                    .await?
                    .is_some()
                {
                    result.errors.push(batch_error(
                        &entity_id,
                        409,
                        format!("entity {entity_id} already exists"),
                    ));
                } else {
                    created.push(entity_id.clone());
                    result.success.push(entity_id.clone());
                    docs.push(entity.clone());
                    state
                        .repositories
                        .entities
                        .insert(StoredDocument {
                            tenant: context.tenant.clone(),
                            ngsi_id: entity_id,
                            doc: entity,
                        })
                        .await?;
                }
            }
            Err(error) => result.errors.push(batch_error(
                "unknown",
                error.status_code().as_u16(),
                error.to_string(),
            )),
        }
    }

    for entity in &docs {
        notifications::enqueue_notifications(
            state,
            &context.tenant,
            entity,
            &EntityEvent {
                kind: EntityEventKind::Created,
                changed_attributes: changed_attribute_names(entity),
            },
        )
        .await?;
    }

    if !local_only && !docs.is_empty() {
        for entity in &docs {
            let entity_id = entity
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            federation::record_local_swarm_mutation(
                state,
                federation::SwarmMutationInput {
                    tenant: context.tenant.clone(),
                    resource_kind: SwarmResourceKind::Entity,
                    operation: SwarmOperation::Upsert,
                    entity_id,
                    version_at: entity
                        .get("modifiedAt")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    source_peer_id: state.config.broker_id.clone(),
                    event_kind: SwarmEventKind::Created,
                    changed_attributes: changed_attribute_names(entity),
                    snapshot: Some(entity.clone()),
                },
            )
            .await?;
        }
    }

    if result.errors.is_empty() {
        Ok(Ok(created))
    } else {
        Ok(Err(result))
    }
}

/// Upserts batch of entities using merge or replace semantics.
pub async fn batch_upsert(
    state: &AppState,
    context: &RequestContext,
    entities: Vec<Value>,
    update_mode: bool,
    local_only: bool,
    _query_string: Option<String>,
) -> Result<Result<crate::domain::batch::BatchUpsertOutcome, BatchOperationResult>, BrokerError> {
    let mut created_ids = Vec::new();
    let mut had_updates = false;
    let mut result = BatchOperationResult::default();
    let mut docs = Vec::new();

    for mut entity in entities {
        match prepare_entity_for_create(&mut entity, context) {
            Ok(entity_id) => {
                let existing = state
                    .repositories
                    .entities
                    .get(&context.tenant, &entity_id)
                    .await?;
                if let Some(mut existing) = existing {
                    had_updates = true;
                    if update_mode {
                        apply_merge_patch(&mut existing.doc, &entity);
                    } else {
                        prepare_entity_for_replace(
                            &mut entity,
                            &entity_id,
                            context,
                            existing.doc.get("createdAt"),
                        )?;
                        existing.doc = entity.clone();
                    }
                    state
                        .repositories
                        .entities
                        .replace(existing.clone())
                        .await?;
                    docs.push(existing.doc.clone());
                    result.success.push(entity_id);
                } else {
                    created_ids.push(entity_id.clone());
                    result.success.push(entity_id.clone());
                    docs.push(entity.clone());
                    state
                        .repositories
                        .entities
                        .insert(StoredDocument {
                            tenant: context.tenant.clone(),
                            ngsi_id: entity_id,
                            doc: entity,
                        })
                        .await?;
                }
            }
            Err(error) => result.errors.push(batch_error(
                "unknown",
                error.status_code().as_u16(),
                error.to_string(),
            )),
        }
    }

    if !local_only && !docs.is_empty() {
        for entity in &docs {
            let entity_id = entity
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            federation::record_local_swarm_mutation(
                state,
                federation::SwarmMutationInput {
                    tenant: context.tenant.clone(),
                    resource_kind: SwarmResourceKind::Entity,
                    operation: SwarmOperation::Upsert,
                    entity_id,
                    version_at: entity
                        .get("modifiedAt")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    source_peer_id: state.config.broker_id.clone(),
                    event_kind: SwarmEventKind::Updated,
                    changed_attributes: changed_attribute_names(entity),
                    snapshot: Some(entity.clone()),
                },
            )
            .await?;
        }
    }

    if result.errors.is_empty() {
        Ok(Ok(crate::domain::batch::BatchUpsertOutcome {
            created_ids,
            had_updates,
        }))
    } else {
        Ok(Err(result))
    }
}

/// Applies partial updates to batch of existing entities.
pub async fn batch_update(
    state: &AppState,
    context: &RequestContext,
    entities: Vec<Value>,
    no_overwrite: bool,
    local_only: bool,
    _query_string: Option<String>,
) -> Result<Result<(), BatchOperationResult>, BrokerError> {
    let mut result = BatchOperationResult::default();
    let mut docs = Vec::new();

    for entity in entities {
        let Some(entity_id) = entity.get("id").and_then(Value::as_str) else {
            result.errors.push(batch_error(
                "unknown",
                400,
                "payload entity must include an id",
            ));
            continue;
        };

        match state
            .repositories
            .entities
            .get(&context.tenant, entity_id)
            .await?
        {
            Some(mut existing) => {
                let attrs = editable_fragment_members(&entity);
                if let Some(object) = existing.doc.as_object_mut() {
                    for (key, value) in attrs {
                        if no_overwrite && object.contains_key(&key) {
                            continue;
                        }
                        object.insert(key, value);
                    }
                    object.insert("modifiedAt".to_string(), Value::String(now_timestamp()));
                }
                state
                    .repositories
                    .entities
                    .replace(existing.clone())
                    .await?;
                docs.push(existing.doc.clone());
                result.success.push(entity_id.to_string());
            }
            None => result.errors.push(batch_error(
                entity_id,
                404,
                format!("entity {entity_id} was not found"),
            )),
        }
    }

    if !local_only && !docs.is_empty() {
        for entity in &docs {
            let entity_id = entity
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            federation::record_local_swarm_mutation(
                state,
                federation::SwarmMutationInput {
                    tenant: context.tenant.clone(),
                    resource_kind: SwarmResourceKind::Entity,
                    operation: SwarmOperation::Upsert,
                    entity_id,
                    version_at: entity
                        .get("modifiedAt")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    source_peer_id: state.config.broker_id.clone(),
                    event_kind: SwarmEventKind::Updated,
                    changed_attributes: changed_attribute_names(entity),
                    snapshot: Some(entity.clone()),
                },
            )
            .await?;
        }
    }

    if result.errors.is_empty() {
        Ok(Ok(()))
    } else {
        Ok(Err(result))
    }
}

/// Applies JSON Merge Patch to batch of existing entities.
pub async fn batch_merge(
    state: &AppState,
    context: &RequestContext,
    entities: Vec<Value>,
    local_only: bool,
    query_string: Option<String>,
) -> Result<Result<(), BatchOperationResult>, BrokerError> {
    let mut result = BatchOperationResult::default();

    for entity in entities {
        let entity_id = entity
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();

        match validate_entity(&entity) {
            Ok(()) => match merge(
                state,
                context,
                &entity_id,
                None,
                entity,
                local_only,
                query_string.clone(),
            )
            .await
            {
                Ok(()) => result.success.push(entity_id),
                Err(error) => result.errors.push(batch_error(
                    &entity_id,
                    error.status_code().as_u16(),
                    error.to_string(),
                )),
            },
            Err(error) => result.errors.push(batch_error(
                &entity_id,
                error.status_code().as_u16(),
                error.to_string(),
            )),
        }
    }

    if result.errors.is_empty() {
        Ok(Ok(()))
    } else {
        Ok(Err(result))
    }
}

/// Deletes batch of entities and reports per-item failures.
pub async fn batch_delete(
    state: &AppState,
    context: &RequestContext,
    entity_ids: Vec<String>,
    local_only: bool,
    _query_string: Option<String>,
) -> Result<Result<(), BatchOperationResult>, BrokerError> {
    let mut result = BatchOperationResult::default();
    for entity_id in entity_ids.clone() {
        match state
            .repositories
            .entities
            .delete(&context.tenant, &entity_id)
            .await?
        {
            Some(_) => result.success.push(entity_id),
            None => result.errors.push(batch_error(
                &entity_id,
                404,
                format!("entity {entity_id} was not found"),
            )),
        }
    }

    if !local_only && !result.success.is_empty() {
        for entity_id in &result.success {
            federation::record_local_swarm_mutation(
                state,
                federation::SwarmMutationInput {
                    tenant: context.tenant.clone(),
                    resource_kind: SwarmResourceKind::Entity,
                    operation: SwarmOperation::Delete,
                    entity_id: entity_id.clone(),
                    version_at: now_timestamp(),
                    source_peer_id: state.config.broker_id.clone(),
                    event_kind: SwarmEventKind::Deleted,
                    changed_attributes: Vec::new(),
                    snapshot: None,
                },
            )
            .await?;
        }
    }

    if result.errors.is_empty() {
        Ok(Ok(()))
    } else {
        Ok(Err(result))
    }
}

/// Removes duplicate entity payloads by id.
fn dedupe(items: Vec<Value>) -> Vec<Value> {
    let mut seen = std::collections::HashSet::new();
    let mut deduped = Vec::new();
    for item in items {
        let id = item
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if seen.insert(id) {
            deduped.push(item);
        }
    }
    deduped
}

/// Builds linked-entity graph when query traversal requires it.
async fn entity_query_linked_graph(
    state: &AppState,
    context: &RequestContext,
    query: &EntityQuery,
    entities: &[Value],
) -> Result<Arc<HashMap<String, Value>>, BrokerError> {
    if !query_needs_linked_graph(query) {
        return Ok(Arc::default());
    }

    let mut linked_entities = state
        .repositories
        .entities
        .query(&context.tenant, &MongoQueryPlan::default())
        .await?
        .into_iter()
        .map(|document| document.doc)
        .filter_map(|entity| {
            let id = entity.get("id").and_then(Value::as_str)?.to_string();
            Some((id, entity))
        })
        .collect::<HashMap<_, _>>();

    for entity in entities {
        if let Some(id) = entity.get("id").and_then(Value::as_str) {
            linked_entities
                .entry(id.to_string())
                .or_insert_with(|| entity.clone());
        }
    }

    Ok(Arc::new(linked_entities))
}

/// Builds batch error payload for multi-status responses.
fn batch_error(entity_id: &str, status: u16, detail: impl Into<String>) -> BatchEntityError {
    BatchEntityError {
        entity_id: entity_id.to_string(),
        registration_id: None,
        error: ProblemDetails {
            r#type: Some("about:blank".to_string()),
            title: Some("BatchOperationFailed".to_string()),
            status,
            detail: detail.into(),
        },
    }
}

/// Builds attribute-level update failure payload.
fn not_updated(attribute_name: &str, status: u16, detail: impl Into<String>) -> NotUpdatedDetails {
    NotUpdatedDetails {
        attribute_name: attribute_name.to_string(),
        reason: ProblemDetails {
            r#type: Some("about:blank".to_string()),
            title: Some("UpdateFailed".to_string()),
            status,
            detail: detail.into(),
        },
    }
}
