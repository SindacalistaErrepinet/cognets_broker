//! Local entity service operations.
//!
//! `src/api.rs` parses HTTP requests and routes them here. This module performs
//! tenant-scoped entity reads and writes, keeps stored payloads normalized,
//! applies query-time filtering that cannot be expressed entirely in storage,
//! and triggers local subscription fan-out for single-entity write paths.
//!
//! Single-entity write helpers generally follow same flow: validate and
//! normalize payload, persist under `context.tenant`, then record entity-watch
//! state and enqueue matching notifications.
//!
//! Batch helpers intentionally use nested `Result`s. Outer `Err(BrokerError)`
//! means infrastructure or request-level failure. Inner
//! `Err(BatchOperationResult)` means request itself completed but one or more
//! entities failed logically, which API layer maps to HTTP `207 Multi-Status`.
//!
//! Several signatures still carry `_local_only` and `_query_string` for API
//! symmetry. Current implementation of this module does not branch on those
//! values.
use std::{collections::HashMap, sync::Arc};

use actix_web::ResponseError;
use serde_json::Value;

use crate::{
    app::state::AppState,
    context::headers::RequestContext,
    domain::{
        batch::{BatchEntityError, BatchOperationResult, NotUpdatedDetails, UpdateResult},
        types::{EntityEvent, EntityEventKind, StoredDocument},
    },
    error::{BrokerError, ProblemDetails},
    query::{
        context::resolve_context_terms,
        planner::QueryPlan,
        types::{EntityQuery, QueryResult, Representation},
    },
    services::{
        common::{
            build_query_match_options, changed_attribute_names, ensure_attribute_exists,
            ensure_fragment_id_matches, ensure_requested_type, entity_matches_query_with_options,
            prepare_entity_for_create, prepare_entity_for_replace, project_entity,
            query_needs_linked_graph, representation_from_request, validate_entity,
        },
        notifications,
    },
    utils::{
        json::{apply_merge_patch, editable_fragment_members},
        time::now_timestamp,
    },
};

/// Queries local entities for current tenant and projects them into response shape.
///
/// Flow:
/// 1. Build repository query plan for filters storage can evaluate cheaply.
/// 2. Load candidate documents from local tenant-scoped entity store.
/// 3. Deduplicate payloads by logical entity id.
/// 4. Resolve per-entity JSON-LD context and optional linked-entity graph data
///    for in-memory `q` evaluation.
/// 5. Apply NGSI-LD projection (`attrs`, `pick`, `omit`, representation) and
///    limit.
///
/// `total_count` reflects number of entities that matched after in-memory
/// filtering and before `limit` truncation.
pub async fn query(
    state: &AppState,
    request: &actix_web::HttpRequest,
    context: &RequestContext,
    query: &EntityQuery,
) -> Result<QueryResult, BrokerError> {
    let representation =
        representation_from_request(request, query.format.as_deref(), query.options.as_deref());
    let plan = QueryPlan::from_entity_query(query)?;
    let items = state
        .repositories
        .entities
        .query(&context.tenant, &plan)
        .await?;

    // Storage narrows candidate set first, but final NGSI-LD matching still runs
    // against full payloads in memory. Collapse duplicate logical entities
    // before that second pass.
    let entities = dedupe(items.into_iter().map(|item| item.doc).collect());
    // Relationship traversal inside `q` expressions may need access to other
    // entities from same tenant, not only current query candidates.
    let linked_entities = entity_query_linked_graph(state, context, query, &entities).await?;
    let mut matched = Vec::new();
    for entity in entities {
        // Context terms are resolved per entity because `@context` can come from
        // payload itself or from request `Link` header backfill.
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

    // Projection happens after filtering so predicates can still inspect full
    // stored documents even when response asks for subset of attributes.
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

    // GeoJSON needs `FeatureCollection` wrapper. Other representations return
    // plain arrays of projected entities.
    let body = if representation == Representation::GeoJson {
        serde_json::json!({"type": "FeatureCollection", "features": projected})
    } else {
        Value::Array(projected)
    };

    Ok(QueryResult { body, total_count })
}

/// Retrieves one entity by id from current tenant store.
///
/// This is direct point lookup, not broad query execution. After loading stored
/// document it still enforces optional `type` selector and response projection
/// so single-entity reads stay consistent with collection reads.
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

/// Creates one new entity in current tenant.
///
/// `prepare_entity_for_create` validates payload, applies `Link`-header
/// `@context` backfill when needed, stamps broker-managed timestamps, and
/// returns canonical entity id. After conflict check, entity is stored as
/// `StoredDocument { tenant, ngsi_id, doc }`, then local watch state and
/// matching subscription notifications are enqueued.
pub async fn create(
    state: &AppState,
    context: &RequestContext,
    mut entity: Value,
    _local_only: bool,
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
        // Repository persists tenant wrapper plus raw NGSI-LD payload.
        .insert(StoredDocument {
            tenant: context.tenant.clone(),
            ngsi_id: entity_id.clone(),
            doc: entity.clone(),
        })
        .await?;

    enqueue_local_notifications(
        state,
        &context.tenant,
        &entity_id,
        &entity,
        EntityEvent {
            kind: EntityEventKind::Created,
            changed_attributes: changed_attribute_names(&entity),
        },
    )
    .await?;

    Ok(entity_id)
}

/// Deletes one entity from current tenant.
///
/// Entity is loaded before deletion so service can enforce optional `type`
/// selector and still have original payload available for delete-side
/// notifications after repository row is removed.
pub async fn delete(
    state: &AppState,
    context: &RequestContext,
    entity_id: &str,
    requested_type: Option<&str>,
    _local_only: bool,
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

    enqueue_local_notifications(
        state,
        &context.tenant,
        entity_id,
        &document.doc,
        EntityEvent {
            kind: EntityEventKind::Deleted,
            changed_attributes: changed_attribute_names(&document.doc),
        },
    )
    .await?;

    Ok(())
}

/// Applies JSON Merge Patch to existing entity.
///
/// Patch must target requested entity id when it carries `id`. After patching,
/// broker reasserts managed fields such as `id` and `modifiedAt` before stored
/// document is replaced and update notifications are emitted.
pub async fn merge(
    state: &AppState,
    context: &RequestContext,
    entity_id: &str,
    requested_type: Option<&str>,
    patch: Value,
    _local_only: bool,
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
        // Merge Patch may delete or overwrite these keys, but broker keeps id
        // stable and always advances entity-level modification timestamp.
        object.insert("id".to_string(), Value::String(entity_id.to_string()));
        object.insert("modifiedAt".to_string(), Value::String(now_timestamp()));
    }
    state
        .repositories
        .entities
        .replace(existing.clone())
        .await?;

    enqueue_local_notifications(
        state,
        &context.tenant,
        entity_id,
        &existing.doc,
        EntityEvent {
            kind: EntityEventKind::Updated,
            changed_attributes: changed_attribute_names(&patch),
        },
    )
    .await?;

    Ok(())
}

/// Replaces full entity payload while preserving broker-managed invariants.
///
/// Replacement semantics are stricter than merge semantics: caller supplies full
/// entity body, then `prepare_entity_for_replace` forces target `id`, preserves
/// original `createdAt`, refreshes `modifiedAt`, clears stale `deletedAt`, and
/// validates resulting payload before persistence.
pub async fn replace(
    state: &AppState,
    context: &RequestContext,
    entity_id: &str,
    requested_type: Option<&str>,
    mut entity: Value,
    _local_only: bool,
    _query_string: Option<String>,
) -> Result<(), BrokerError> {
    let existing = state
        .repositories
        .entities
        .get(&context.tenant, entity_id)
        .await?
        .ok_or_else(|| BrokerError::NotFound(format!("entity {entity_id} was not found")))?;
    ensure_requested_type(&existing.doc, requested_type)?;
    // Full replacement keeps stable identity and original creation timestamp.
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

    enqueue_local_notifications(
        state,
        &context.tenant,
        entity_id,
        &entity,
        EntityEvent {
            kind: EntityEventKind::Updated,
            changed_attributes: changed_attribute_names(&entity),
        },
    )
    .await?;

    Ok(())
}

/// Appends top-level attributes and optionally rejects overwrites.
///
/// `no_overwrite` converts existing-key collisions into `not_updated` entries
/// instead of failing whole request. Successful keys are reported in `updated`
/// and become notification `changed_attributes`.
pub async fn append_attrs(
    state: &AppState,
    context: &RequestContext,
    entity_id: &str,
    requested_type: Option<&str>,
    fragment: Value,
    no_overwrite: bool,
    _local_only: bool,
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
        // Attribute-level outcomes are accumulated so caller can return partial
        // success information without aborting entire request at first conflict.
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
        enqueue_local_notifications(
            state,
            &context.tenant,
            entity_id,
            &existing.doc,
            EntityEvent {
                kind: EntityEventKind::Updated,
                changed_attributes: result.updated.clone(),
            },
        )
        .await?;
    }

    Ok(result)
}

/// Updates only top-level attributes that already exist.
///
/// Missing keys are accumulated in `not_updated`, allowing caller to return
/// per-attribute partial outcome instead of aborting whole request.
pub async fn update_attrs(
    state: &AppState,
    context: &RequestContext,
    entity_id: &str,
    requested_type: Option<&str>,
    fragment: Value,
    _local_only: bool,
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
        // Unlike append, this path treats missing keys as logical errors and
        // only mutates attributes already present on stored entity.
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
        enqueue_local_notifications(
            state,
            &context.tenant,
            entity_id,
            &existing.doc,
            EntityEvent {
                kind: EntityEventKind::Updated,
                changed_attributes: result.updated.clone(),
            },
        )
        .await?;
    }

    Ok(result)
}

/// Applies JSON Merge Patch to one existing top-level attribute.
///
/// Entity must exist, match optional `type` selector, and already contain
/// `attr_id`. Entity-level `modifiedAt` is always advanced after patching.
pub async fn patch_attr(
    state: &AppState,
    context: &RequestContext,
    entity_id: &str,
    attr_id: &str,
    requested_type: Option<&str>,
    patch: Value,
    _local_only: bool,
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
            // Patch is scoped to attribute payload only; entity metadata is
            // refreshed separately below.
            apply_merge_patch(attribute, &patch);
        }
        object.insert("modifiedAt".to_string(), Value::String(now_timestamp()));
    }
    state
        .repositories
        .entities
        .replace(existing.clone())
        .await?;

    enqueue_local_notifications(
        state,
        &context.tenant,
        entity_id,
        &existing.doc,
        EntityEvent {
            kind: EntityEventKind::Updated,
            changed_attributes: vec![attr_id.to_string()],
        },
    )
    .await?;
    Ok(())
}

/// Deletes one top-level attribute from existing entity.
///
/// Attribute existence is checked before mutation so function can return `404`
/// without modifying entity state.
pub async fn delete_attr(
    state: &AppState,
    context: &RequestContext,
    entity_id: &str,
    attr_id: &str,
    requested_type: Option<&str>,
    _local_only: bool,
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

    enqueue_local_notifications(
        state,
        &context.tenant,
        entity_id,
        &existing.doc,
        EntityEvent {
            kind: EntityEventKind::Updated,
            changed_attributes: vec![attr_id.to_string()],
        },
    )
    .await?;

    Ok(())
}

/// Replaces one top-level attribute value on existing entity.
///
/// This is direct replacement, not merge patching. Service still enforces entity
/// existence, optional `type` selector, and pre-existing attribute check before
/// mutating stored document.
pub async fn replace_attr(
    state: &AppState,
    context: &RequestContext,
    entity_id: &str,
    attr_id: &str,
    requested_type: Option<&str>,
    value: Value,
    _local_only: bool,
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

    enqueue_local_notifications(
        state,
        &context.tenant,
        entity_id,
        &existing.doc,
        EntityEvent {
            kind: EntityEventKind::Updated,
            changed_attributes: vec![attr_id.to_string()],
        },
    )
    .await?;

    Ok(())
}

/// Creates multiple entities sequentially for same tenant.
///
/// There is no transaction or rollback across items: each entity is validated,
/// conflict-checked, and inserted inline. Successful inserts are remembered and
/// notified after loop so side effects only run for documents that were actually
/// persisted.
///
/// Outer `Err(BrokerError)` means infrastructure failure. Inner
/// `Err(BatchOperationResult)` means request completed with logical per-entity
/// failures and should be rendered as `207 Multi-Status`.
pub async fn batch_create(
    state: &AppState,
    context: &RequestContext,
    entities: Vec<Value>,
    _local_only: bool,
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
                    docs.push((entity_id.clone(), entity.clone()));
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

    // Deliver side effects only for writes that committed successfully during
    // first pass; rejected entities never reach this loop.
    for (entity_id, entity) in &docs {
        enqueue_local_notifications(
            state,
            &context.tenant,
            entity_id,
            entity,
            EntityEvent {
                kind: EntityEventKind::Created,
                changed_attributes: changed_attribute_names(entity),
            },
        )
        .await?;
    }

    if result.errors.is_empty() {
        Ok(Ok(created))
    } else {
        Ok(Err(result))
    }
}

/// Upserts multiple entities sequentially.
///
/// Missing entities are created. Existing entities are either merge-patched
/// (`update_mode = true`) or fully replaced after normalization
/// (`update_mode = false`). Returned outcome tells API layer whether any new
/// ids were created so it can choose between `201 Created` and `204 No Content`.
///
/// Current implementation persists inline and reports per-item success/failure;
/// it does not perform a second notification pass.
pub async fn batch_upsert(
    state: &AppState,
    context: &RequestContext,
    entities: Vec<Value>,
    update_mode: bool,
    _local_only: bool,
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
                    // Update mode keeps untouched fields via Merge Patch.
                    // Replace mode normalizes a fresh full document while
                    // preserving broker-managed metadata.
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
///
/// Each payload must carry an `id`. Missing entities become batch errors.
/// When `no_overwrite` is enabled, existing keys are left untouched silently
/// instead of producing attribute-level error payloads.
///
/// Current implementation persists successful updates inline and only returns
/// batch status summary.
pub async fn batch_update(
    state: &AppState,
    context: &RequestContext,
    entities: Vec<Value>,
    no_overwrite: bool,
    _local_only: bool,
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
                        // Batch `noOverwrite` behavior is silent skip rather
                        // than per-attribute `notUpdated` reporting.
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

    if result.errors.is_empty() {
        Ok(Ok(()))
    } else {
        Ok(Err(result))
    }
}

/// Applies single-entity `merge` semantics across batch.
///
/// Each payload is validated first, then delegated to `merge`, so timestamp,
/// id-reassertion, and notification behavior stay aligned with single-entity
/// merge endpoint.
pub async fn batch_merge(
    state: &AppState,
    context: &RequestContext,
    entities: Vec<Value>,
    _local_only: bool,
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
                false,
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

/// Deletes multiple entity ids and reports per-item failures.
///
/// Each id is attempted independently and successful deletions are not rolled
/// back if later ids fail. This helper deletes directly by id from repository,
/// so it does not fetch original payload first and therefore cannot apply
/// optional type guards or emit delete notifications from this path.
pub async fn batch_delete(
    state: &AppState,
    context: &RequestContext,
    entity_ids: Vec<String>,
    _local_only: bool,
    _query_string: Option<String>,
) -> Result<Result<(), BatchOperationResult>, BrokerError> {
    let mut result = BatchOperationResult::default();
    for entity_id in entity_ids.clone() {
        // This path is intentionally direct repository delete, with no pre-load
        // of full entity document.
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

    if result.errors.is_empty() {
        Ok(Ok(()))
    } else {
        Ok(Err(result))
    }
}

/// Removes duplicate logical entities while preserving first occurrence order.
///
/// Query planning can assemble overlapping candidate lists, so this helper
/// defensively collapses payloads by `id` before in-memory filtering and
/// projection.
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

async fn enqueue_local_notifications(
    state: &AppState,
    tenant: &str,
    entity_id: &str,
    entity: &Value,
    event: EntityEvent,
) -> Result<(), BrokerError> {
    // Local watch cache mirrors most recent broker-side observation so other
    // components can reason about recent mutations without rereading storage.
    match event.kind {
        EntityEventKind::Deleted => state.entity_watch.record_local_delete(tenant, entity_id),
        EntityEventKind::Created | EntityEventKind::Updated => state
            .entity_watch
            .record_local_upsert(tenant, entity_id, entity),
    }

    notifications::enqueue_notifications(state, tenant, entity, &event).await
}

/// Builds linked-entity graph when query traversal requires it.
///
/// If `q` expression does not traverse relationships, this returns empty map and
/// avoids full-tenant scan. When traversal is needed, current implementation
/// loads all local entities for tenant, keys them by `id`, and then makes sure
/// current query candidates are also present in graph.
async fn entity_query_linked_graph(
    state: &AppState,
    context: &RequestContext,
    query: &EntityQuery,
    entities: &[Value],
) -> Result<Arc<HashMap<String, Value>>, BrokerError> {
    if !query_needs_linked_graph(query) {
        return Ok(Arc::default());
    }

    // Linked-entity predicates can jump to arbitrary referenced entities, so
    // current strategy is full local tenant scan keyed by logical entity id.
    let mut linked_entities = state
        .repositories
        .entities
        .query(&context.tenant, &QueryPlan::default())
        .await?
        .into_iter()
        .map(|document| document.doc)
        .filter_map(|entity| {
            let id = entity.get("id").and_then(Value::as_str)?.to_string();
            Some((id, entity))
        })
        .collect::<HashMap<_, _>>();

    // Seed graph with current candidates as well. This keeps map complete even
    // if repository query above ever becomes narrower than candidate set.
    for entity in entities {
        if let Some(id) = entity.get("id").and_then(Value::as_str) {
            linked_entities
                .entry(id.to_string())
                .or_insert_with(|| entity.clone());
        }
    }

    Ok(Arc::new(linked_entities))
}

/// Constructs per-entity error payload used in batch `207 Multi-Status` bodies.
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

/// Constructs per-attribute failure payload for append/update attribute APIs.
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
