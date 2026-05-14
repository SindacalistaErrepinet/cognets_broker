//! Shared service helpers for validation, projection, and response shaping.
use std::sync::Arc;

use actix_web::{HttpRequest, http::header};
use regex::Regex;
use serde_json::{Map, Value, json};
use uuid::Uuid;

use crate::{
    context::headers::{RequestContext, apply_context_link, content_type_for_representation},
    error::BrokerError,
    query::{
        language::{
            QueryMatchOptions, extract_scope_values, q_expression_uses_linked_entity,
            query_matches_value_with_options, scope_query_matches_values,
        },
        types::{EntityQuery, Representation, TemporalEntityQuery},
    },
    utils::{
        json::{editable_fragment_members, entity_attribute_names, parse_csv, reserved_member},
        time::now_timestamp,
    },
};

/// Root path for public NGSI-LD API routes.
pub const BASE_PATH: &str = "/ngsi-ld/v1";

/// Selects response representation from Accept, format, and options.
pub fn representation_from_request(
    request: &HttpRequest,
    format: Option<&str>,
    options: Option<&str>,
) -> Representation {
    let accept = request
        .headers()
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();

    if accept.contains("application/geo+json") {
        Representation::GeoJson
    } else if format == Some("aggregatedValues")
        || options
            .map(|value| value.contains("aggregatedValues"))
            .unwrap_or(false)
    {
        Representation::AggregatedValues
    } else if format == Some("temporalValues")
        || options
            .map(|value| value.contains("temporalValues"))
            .unwrap_or(false)
    {
        Representation::TemporalValues
    } else if format
        .or(options)
        .map(|value| value.contains("keyValues") || value.contains("simplified"))
        .unwrap_or(false)
    {
        Representation::KeyValues
    } else {
        Representation::Normalized
    }
}

/// Returns response content type for chosen representation.
pub fn response_content_type(
    request: &HttpRequest,
    representation: Representation,
) -> &'static str {
    content_type_for_representation(request, representation)
}

/// Builds canonical resource location under NGSI-LD base path.
pub fn resource_location(resource: &str, resource_id: &str) -> String {
    format!("{BASE_PATH}/{resource}/{resource_id}")
}

/// Normalizes entity payload for create and returns entity id.
pub fn prepare_entity_for_create(
    entity: &mut Value,
    context: &RequestContext,
) -> Result<String, BrokerError> {
    {
        let entity_object = entity.as_object_mut().ok_or_else(|| {
            BrokerError::BadRequest("entity payload must be a JSON object".to_string())
        })?;
        apply_context_link(entity_object, context.link_header.as_deref());
    }
    validate_entity(entity)?;

    let now = now_timestamp();
    let entity_object = entity.as_object_mut().ok_or_else(|| {
        BrokerError::BadRequest("entity payload must be a JSON object".to_string())
    })?;

    let entity_id = entity_object
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| BrokerError::BadRequest("entity id is required".to_string()))?
        .to_string();

    entity_object.insert("createdAt".to_string(), Value::String(now.clone()));
    entity_object.insert("modifiedAt".to_string(), Value::String(now));
    entity_object.remove("deletedAt");

    Ok(entity_id)
}

/// Normalizes full entity replacement while preserving fixed fields.
pub fn prepare_entity_for_replace(
    entity: &mut Value,
    entity_id: &str,
    context: &RequestContext,
    created_at: Option<&Value>,
) -> Result<(), BrokerError> {
    {
        let entity_object = entity.as_object_mut().ok_or_else(|| {
            BrokerError::BadRequest("entity payload must be a JSON object".to_string())
        })?;
        apply_context_link(entity_object, context.link_header.as_deref());
        entity_object.insert("id".to_string(), Value::String(entity_id.to_string()));
    }
    validate_entity(entity)?;

    let now = now_timestamp();
    let entity_object = entity.as_object_mut().ok_or_else(|| {
        BrokerError::BadRequest("entity payload must be a JSON object".to_string())
    })?;

    if let Some(created_at) = created_at {
        entity_object.insert("createdAt".to_string(), created_at.clone());
    }
    entity_object.insert("modifiedAt".to_string(), Value::String(now));
    entity_object.remove("deletedAt");
    Ok(())
}

/// Normalizes subscription payload for creation and returns id.
pub fn prepare_subscription(
    subscription: &mut Value,
    context: &RequestContext,
) -> Result<String, BrokerError> {
    let now = now_timestamp();
    {
        let object = subscription.as_object_mut().ok_or_else(|| {
            BrokerError::BadRequest("subscription payload must be a JSON object".to_string())
        })?;

        apply_context_link(object, context.link_header.as_deref());
        if !object.contains_key("id") {
            object.insert(
                "id".to_string(),
                Value::String(format!("urn:ngsi-ld:Subscription:{}", Uuid::new_v4())),
            );
        }
        object.insert("createdAt".to_string(), Value::String(now.clone()));
        object.insert("modifiedAt".to_string(), Value::String(now));
        object.insert(
            "status".to_string(),
            Value::String(
                if object
                    .get("isActive")
                    .and_then(Value::as_bool)
                    .unwrap_or(true)
                {
                    "active"
                } else {
                    "paused"
                }
                .to_string(),
            ),
        );

        if let Some(notification) = object
            .get_mut("notification")
            .and_then(Value::as_object_mut)
        {
            notification.insert("status".to_string(), Value::String("ok".to_string()));
            notification.insert("timesSent".to_string(), Value::from(0_u64));
            notification.insert("timesFailed".to_string(), Value::from(0_u64));
        }
    }

    validate_subscription(subscription)?;
    Ok(subscription
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string())
}

/// Validates basic NGSI-LD entity shape.
pub fn validate_entity(entity: &Value) -> Result<(), BrokerError> {
    let Some(object) = entity.as_object() else {
        return Err(BrokerError::BadRequest(
            "entity payload must be a JSON object".to_string(),
        ));
    };

    let id = object
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| BrokerError::BadRequest("entity id is required".to_string()))?;
    ensure_uri_like(id, "entity id")?;

    let entity_type = object
        .get("type")
        .ok_or_else(|| BrokerError::BadRequest("entity type is required".to_string()))?;
    if !matches!(entity_type, Value::String(_) | Value::Array(_)) {
        return Err(BrokerError::BadRequest(
            "entity type must be a string or an array of strings".to_string(),
        ));
    }

    Ok(())
}

/// Validates basic NGSI-LD subscription shape.
pub fn validate_subscription(subscription: &Value) -> Result<(), BrokerError> {
    let Some(object) = subscription.as_object() else {
        return Err(BrokerError::BadRequest(
            "subscription payload must be a JSON object".to_string(),
        ));
    };

    ensure_uri_like(
        object
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| BrokerError::BadRequest("subscription id is required".to_string()))?,
        "subscription id",
    )?;

    if object.get("type").and_then(Value::as_str) != Some("Subscription") {
        return Err(BrokerError::BadRequest(
            "subscription type must be Subscription".to_string(),
        ));
    }

    let endpoint = object
        .get("notification")
        .and_then(Value::as_object)
        .and_then(|notification| notification.get("endpoint"))
        .and_then(Value::as_object)
        .and_then(|endpoint| endpoint.get("uri"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            BrokerError::BadRequest(
                "subscription notification.endpoint.uri is required".to_string(),
            )
        })?;
    ensure_url(endpoint, "notification endpoint")
}

/// Validates that identifier looks URI-like.
pub fn ensure_uri_like(value: &str, label: &str) -> Result<(), BrokerError> {
    if value.contains(':') {
        Ok(())
    } else {
        Err(BrokerError::BadRequest(format!("{label} must be a URI")))
    }
}

/// Validates outbound URL field.
pub fn ensure_url(value: &str, label: &str) -> Result<(), BrokerError> {
    url::Url::parse(value)
        .map(|_| ())
        .map_err(|_| BrokerError::BadRequest(format!("{label} must be a valid URL")))
}

/// Ensures fragment id matches target entity id when present.
pub fn ensure_fragment_id_matches(fragment: &Value, entity_id: &str) -> Result<(), BrokerError> {
    if let Some(fragment_id) = fragment.get("id").and_then(Value::as_str) {
        if fragment_id != entity_id {
            return Err(BrokerError::BadRequest(
                "payload id must match the entity id in the path".to_string(),
            ));
        }
    }
    Ok(())
}

/// Ensures stored entity matches requested type selector.
pub fn ensure_requested_type(
    entity: &Value,
    requested_type: Option<&str>,
) -> Result<(), BrokerError> {
    if let Some(requested_type) = requested_type {
        if !entity_type_matches(entity.get("type"), requested_type) {
            return Err(BrokerError::NotFound(format!(
                "entity type {requested_type} does not match the stored entity"
            )));
        }
    }
    Ok(())
}

/// Ensures target attribute exists on entity.
pub fn ensure_attribute_exists(entity: &Value, attr_id: &str) -> Result<(), BrokerError> {
    if entity.get(attr_id).is_none() {
        return Err(BrokerError::NotFound(format!(
            "attribute {attr_id} was not found"
        )));
    }
    Ok(())
}

/// Returns true when entity type field contains expected type.
pub fn entity_type_matches(value: Option<&Value>, expected: &str) -> bool {
    match value {
        Some(Value::String(actual)) => actual == expected,
        Some(Value::Array(items)) => items.iter().any(|item| item.as_str() == Some(expected)),
        _ => false,
    }
}

/// Returns first type name from entity payload.
pub fn entity_primary_type(entity: &Value) -> Option<String> {
    match entity.get("type") {
        Some(Value::String(value)) => Some(value.clone()),
        Some(Value::Array(items)) => items
            .first()
            .and_then(Value::as_str)
            .map(ToString::to_string),
        _ => None,
    }
}

/// Projects entity into requested attribute set and representation.
pub fn project_entity(
    entity: &Value,
    attrs: Option<&str>,
    pick: Option<&str>,
    omit: Option<&str>,
    representation: Representation,
) -> Value {
    let mut projected = entity.clone();
    if let Some(object) = projected.as_object_mut() {
        let selected = if let Some(pick) = pick {
            parse_csv(Some(pick))
        } else if let Some(attrs) = attrs {
            parse_csv(Some(attrs))
        } else {
            Vec::new()
        };

        if !selected.is_empty() {
            object.retain(|key, _| reserved_member(key) || selected.iter().any(|item| item == key));
        }

        for omitted in parse_csv(omit) {
            object.remove(&omitted);
        }
    }

    match representation {
        Representation::Normalized => projected,
        Representation::KeyValues => simplify_entity(&projected),
        Representation::GeoJson => entity_to_feature(&projected),
        Representation::TemporalValues | Representation::AggregatedValues => projected,
    }
}

/// Simplifies NGSI-LD entity into key-values-like structure.
pub fn simplify_entity(entity: &Value) -> Value {
    let mut simplified = Map::new();

    if let Some(object) = entity.as_object() {
        for (key, value) in object {
            if reserved_member(key) {
                simplified.insert(key.clone(), value.clone());
            } else {
                simplified.insert(key.clone(), simplify_attribute_value(value));
            }
        }
    }

    Value::Object(simplified)
}

/// Simplifies single NGSI-LD attribute value recursively.
pub fn simplify_attribute_value(value: &Value) -> Value {
    match value {
        Value::Object(object) => match object.get("type").and_then(Value::as_str) {
            Some("Property") | Some("GeoProperty") => {
                object.get("value").cloned().unwrap_or(Value::Null)
            }
            Some("Relationship") | Some("ListRelationship") => {
                object.get("object").cloned().unwrap_or(Value::Null)
            }
            Some("LanguageProperty") => object.get("languageMap").cloned().unwrap_or(Value::Null),
            Some("VocabProperty") => object.get("vocab").cloned().unwrap_or(Value::Null),
            _ => Value::Object(
                object
                    .iter()
                    .map(|(key, inner)| (key.clone(), simplify_attribute_value(inner)))
                    .collect(),
            ),
        },
        Value::Array(items) => Value::Array(items.iter().map(simplify_attribute_value).collect()),
        _ => value.clone(),
    }
}

/// Converts entity payload into GeoJSON feature.
pub fn entity_to_feature(entity: &Value) -> Value {
    let simplified = simplify_entity(entity);
    let mut properties = simplified.as_object().cloned().unwrap_or_default();
    let id = properties
        .remove("id")
        .and_then(|value| value.as_str().map(ToString::to_string))
        .unwrap_or_default();
    properties.remove("@context");
    let geometry = properties.remove("location").unwrap_or(Value::Null);

    let mut feature = Map::new();
    feature.insert("id".to_string(), Value::String(id));
    feature.insert("type".to_string(), Value::String("Feature".to_string()));
    feature.insert("geometry".to_string(), geometry);
    feature.insert("properties".to_string(), Value::Object(properties));
    if let Some(context) = entity.get("@context") {
        feature.insert("@context".to_string(), context.clone());
    }
    Value::Object(feature)
}

/// Builds temporal response payload from snapshot and history.
pub fn temporal_entity_to_response(
    entity: &Value,
    history: &[Value],
    query: &TemporalEntityQuery,
    representation: Representation,
) -> Value {
    let mut temporal = project_entity(
        entity,
        query.entity.attrs.as_deref(),
        query.entity.pick.as_deref(),
        query.entity.omit.as_deref(),
        Representation::Normalized,
    );

    if let Some(object) = temporal.as_object_mut() {
        let grouped = history.iter().fold(Map::new(), |mut acc, item| {
            if let Some(attr_id) = item.get("attrId").and_then(Value::as_str) {
                acc.entry(attr_id.to_string())
                    .or_insert_with(|| Value::Array(Vec::new()));
                if let Some(list) = acc.get_mut(attr_id).and_then(Value::as_array_mut) {
                    list.push(item.clone());
                }
            }
            acc
        });

        for (key, value) in grouped {
            object.insert(key, value);
        }
    }

    match representation {
        Representation::TemporalValues
        | Representation::AggregatedValues
        | Representation::Normalized => temporal,
        Representation::KeyValues => simplify_entity(&temporal),
        Representation::GeoJson => entity_to_feature(&temporal),
    }
}

/// Returns editable members for attribute fragment payload.
pub fn editable_attr_fragment(fragment: &Value) -> Map<String, Value> {
    editable_fragment_members(fragment)
}

/// Applies entity query with default match options.
pub fn entity_matches_query(entity: &Value, query: &EntityQuery) -> bool {
    entity_matches_query_with_options(entity, query, &QueryMatchOptions::default())
}

/// Applies entity query using caller-supplied query match options.
pub fn entity_matches_query_with_options(
    entity: &Value,
    query: &EntityQuery,
    options: &QueryMatchOptions,
) -> bool {
    if let Some(ids) = &query.id {
        let entity_id = entity.get("id").and_then(Value::as_str).unwrap_or_default();
        if !ids
            .split(',')
            .any(|candidate| candidate.trim() == entity_id)
        {
            return false;
        }
    }

    if let Some(entity_type) = &query.entity_type {
        let types = parse_csv(Some(entity_type));
        if !types
            .iter()
            .any(|candidate| entity_type_matches(entity.get("type"), candidate))
        {
            return false;
        }
    }

    if let Some(id_pattern) = &query.id_pattern {
        let entity_id = entity.get("id").and_then(Value::as_str).unwrap_or_default();
        match Regex::new(id_pattern) {
            Ok(regex) if regex.is_match(entity_id) => {}
            Ok(_) | Err(_) => return false,
        }
    }

    let attrs = parse_csv(query.attrs.as_deref());
    if !attrs.is_empty() && !attrs.iter().all(|attr| entity.get(attr).is_some()) {
        return false;
    }

    if !query_matches_value_with_options(entity, query.q.as_deref(), options).unwrap_or(false) {
        return false;
    }

    scope_query_matches_values(&extract_scope_values(entity), query.scope_q.as_deref())
        .unwrap_or(false)
}

/// Builds reusable query match options from entity query parameters.
pub fn build_query_match_options(
    query: &EntityQuery,
    context_terms: std::collections::HashMap<String, String>,
    linked_entities: Arc<std::collections::HashMap<String, Value>>,
) -> QueryMatchOptions {
    QueryMatchOptions {
        expand_values: parse_csv(query.expand_values.as_deref())
            .into_iter()
            .collect(),
        json_keys: parse_csv(query.json_keys.as_deref()).into_iter().collect(),
        context_terms,
        linked_entities,
    }
}

/// Returns true when entity query needs linked-graph evaluation.
pub fn query_needs_linked_graph(query: &EntityQuery) -> bool {
    query
        .q
        .as_deref()
        .and_then(|raw| {
            crate::query::language::parse_q_expression(Some(raw))
                .ok()
                .flatten()
        })
        .as_ref()
        .is_some_and(q_expression_uses_linked_entity)
}

/// Builds NGSI-LD notification payload for matched subscription.
pub fn build_notification_payload(
    subscription_id: &str,
    subscription: &Value,
    entity: &Value,
) -> Value {
    let accept = subscription
        .get("notification")
        .and_then(Value::as_object)
        .and_then(|notification| notification.get("endpoint"))
        .and_then(Value::as_object)
        .and_then(|endpoint| endpoint.get("accept"))
        .and_then(Value::as_str)
        .unwrap_or("application/json");
    let format = subscription
        .get("notification")
        .and_then(Value::as_object)
        .and_then(|notification| notification.get("format"))
        .and_then(Value::as_str);

    let representation = if accept == "application/geo+json" {
        Representation::GeoJson
    } else if format == Some("keyValues") {
        Representation::KeyValues
    } else {
        Representation::Normalized
    };

    let data = match representation {
        Representation::GeoJson => json!({
            "type": "FeatureCollection",
            "features": [project_entity(entity, None, None, None, Representation::GeoJson)],
        }),
        _ => Value::Array(vec![project_entity(
            entity,
            None,
            None,
            None,
            representation,
        )]),
    };

    let mut notification = Map::new();
    notification.insert(
        "id".to_string(),
        Value::String(format!("urn:ngsi-ld:Notification:{}", Uuid::new_v4())),
    );
    notification.insert(
        "type".to_string(),
        Value::String("Notification".to_string()),
    );
    notification.insert(
        "subscriptionId".to_string(),
        Value::String(subscription_id.to_string()),
    );
    notification.insert("notifiedAt".to_string(), Value::String(now_timestamp()));
    notification.insert("data".to_string(), data);

    if let Some(context) = subscription
        .get("jsonldContext")
        .cloned()
        .or_else(|| entity.get("@context").cloned())
    {
        notification.insert("@context".to_string(), context);
    }

    Value::Object(notification)
}

/// Converts temporal attribute value into stored history record shape.
pub fn make_temporal_instance(attr_id: &str, value: &Value) -> Value {
    let mut record = Map::new();
    record.insert("attrId".to_string(), Value::String(attr_id.to_string()));
    record.insert(
        "instanceId".to_string(),
        value
            .get("instanceId")
            .cloned()
            .unwrap_or_else(|| Value::String(format!("urn:ngsi-ld:instance:{}", Uuid::new_v4()))),
    );
    if let Some(observed_at) = value.get("observedAt") {
        record.insert("observedAt".to_string(), observed_at.clone());
    }
    if let Some(created_at) = value.get("createdAt") {
        record.insert("createdAt".to_string(), created_at.clone());
    }
    if let Some(modified_at) = value.get("modifiedAt") {
        record.insert("modifiedAt".to_string(), modified_at.clone());
    }
    if let Some(deleted_at) = value.get("deletedAt") {
        record.insert("deletedAt".to_string(), deleted_at.clone());
    }
    record.insert("value".to_string(), value.clone());
    Value::Object(record)
}

/// Returns changed top-level attribute names from fragment.
pub fn changed_attribute_names(fragment: &Value) -> Vec<String> {
    entity_attribute_names(fragment)
}
