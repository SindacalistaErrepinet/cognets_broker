use std::collections::{BTreeMap, BTreeSet};

use actix_web::HttpRequest;
use serde_json::Value;

use crate::{
    app::state::AppState,
    context::headers::RequestContext,
    domain::discovery::{AttributeInfo, AttributeList, EntityTypeInfo, EntityTypeList},
    error::BrokerError,
    persistence::repository::EntityRepository,
    query::planner::MongoQueryPlan,
    services::common::entity_primary_type,
    utils::json::entity_attribute_names,
};

#[derive(Default)]
struct DiscoveryAccumulator {
    by_type: BTreeMap<String, TypeAggregate>,
    by_attribute: BTreeMap<String, AttributeAggregate>,
}

#[derive(Default, Clone)]
struct TypeAggregate {
    count: u64,
    attributes: BTreeMap<String, AttributeAggregate>,
}

#[derive(Default, Clone)]
struct AttributeAggregate {
    count: u64,
    attribute_types: BTreeSet<String>,
    type_names: BTreeSet<String>,
}

/// Lists discovered entity types from local tenant data.
pub async fn list_types(
    state: &AppState,
    request: &HttpRequest,
    context: &RequestContext,
    local_only: bool,
    details: bool,
) -> Result<Value, BrokerError> {
    let entities = discovery_entities(state, request, context, local_only).await?;
    let accumulator = build_discovery(&entities);

    if details {
        Ok(Value::Array(
            accumulator
                .by_type
                .into_iter()
                .map(|(type_name, aggregate)| {
                    serde_json::to_value(type_info(type_name, aggregate)).unwrap()
                })
                .collect(),
        ))
    } else {
        Ok(serde_json::to_value(EntityTypeList {
            id: "urn:ngsi-ld:EntityTypeList".to_string(),
            r#type: "EntityTypeList".to_string(),
            type_list: accumulator.by_type.into_keys().collect(),
        })
        .unwrap())
    }
}

/// Returns detailed discovery payload for single entity type.
pub async fn get_type(
    state: &AppState,
    request: &HttpRequest,
    context: &RequestContext,
    type_name: &str,
    local_only: bool,
) -> Result<Value, BrokerError> {
    let entities = discovery_entities(state, request, context, local_only).await?;
    let accumulator = build_discovery(&entities);
    let aggregate =
        accumulator.by_type.get(type_name).cloned().ok_or_else(|| {
            BrokerError::NotFound(format!("entity type {type_name} was not found"))
        })?;

    Ok(serde_json::to_value(type_info(type_name.to_string(), aggregate)).unwrap())
}

/// Lists discovered attribute names or details from local data.
pub async fn list_attributes(
    state: &AppState,
    request: &HttpRequest,
    context: &RequestContext,
    local_only: bool,
    details: bool,
) -> Result<Value, BrokerError> {
    let entities = discovery_entities(state, request, context, local_only).await?;
    let accumulator = build_discovery(&entities);

    if details {
        Ok(Value::Array(
            accumulator
                .by_attribute
                .into_iter()
                .map(|(attribute_name, aggregate)| {
                    serde_json::to_value(attribute_info(attribute_name, aggregate)).unwrap()
                })
                .collect(),
        ))
    } else {
        Ok(serde_json::to_value(AttributeList {
            id: "urn:ngsi-ld:AttributeList".to_string(),
            r#type: "AttributeList".to_string(),
            attribute_list: accumulator.by_attribute.into_keys().collect(),
        })
        .unwrap())
    }
}

/// Returns detailed discovery payload for single attribute.
pub async fn get_attribute(
    state: &AppState,
    request: &HttpRequest,
    context: &RequestContext,
    attribute_name: &str,
    local_only: bool,
) -> Result<Value, BrokerError> {
    let entities = discovery_entities(state, request, context, local_only).await?;
    let accumulator = build_discovery(&entities);
    let aggregate = accumulator
        .by_attribute
        .get(attribute_name)
        .cloned()
        .ok_or_else(|| {
            BrokerError::NotFound(format!("attribute {attribute_name} was not found"))
        })?;

    Ok(serde_json::to_value(attribute_info(attribute_name.to_string(), aggregate)).unwrap())
}

/// Loads deduplicated local entities used for discovery aggregation.
async fn discovery_entities(
    state: &AppState,
    _request: &HttpRequest,
    context: &RequestContext,
    _local_only: bool,
) -> Result<Vec<Value>, BrokerError> {
    let plan = MongoQueryPlan::default();
    let items = state
        .repositories
        .entities
        .query(&context.tenant, &plan)
        .await?;

    Ok(dedupe(items.into_iter().map(|item| item.doc).collect()))
}

/// Aggregates entity types and attributes for discovery endpoints.
fn build_discovery(entities: &[Value]) -> DiscoveryAccumulator {
    let mut accumulator = DiscoveryAccumulator::default();

    for entity in entities {
        let Some(type_name) = entity_primary_type(entity) else {
            continue;
        };

        let type_entry = accumulator.by_type.entry(type_name.clone()).or_default();
        type_entry.count += 1;

        for attr_name in entity_attribute_names(entity) {
            let attr_types = attribute_type_names(entity.get(&attr_name));

            let global_attr = accumulator
                .by_attribute
                .entry(attr_name.clone())
                .or_default();
            global_attr.count += 1;
            global_attr.type_names.insert(type_name.clone());
            global_attr
                .attribute_types
                .extend(attr_types.iter().cloned());

            let type_attr = type_entry.attributes.entry(attr_name).or_default();
            type_attr.count += 1;
            type_attr.type_names.insert(type_name.clone());
            type_attr.attribute_types.extend(attr_types);
        }
    }

    accumulator
}

/// Collects attribute type names from NGSI-LD attribute value.
fn attribute_type_names(value: Option<&Value>) -> BTreeSet<String> {
    /// Recursively walks nested attribute values collecting `type` fields.
    fn collect(value: &Value, into: &mut BTreeSet<String>) {
        match value {
            Value::Object(object) => {
                if let Some(type_name) = object.get("type").and_then(Value::as_str) {
                    into.insert(type_name.to_string());
                }
            }
            Value::Array(items) => {
                for item in items {
                    collect(item, into);
                }
            }
            _ => {}
        }
    }

    let mut types = BTreeSet::new();
    if let Some(value) = value {
        collect(value, &mut types);
    }
    if types.is_empty() {
        types.insert("Property".to_string());
    }
    types
}

/// Builds detailed entity type discovery record.
fn type_info(type_name: String, aggregate: TypeAggregate) -> EntityTypeInfo {
    EntityTypeInfo {
        id: type_name.clone(),
        r#type: "EntityTypeInfo".to_string(),
        type_name,
        entity_count: aggregate.count,
        attribute_details: aggregate
            .attributes
            .into_iter()
            .map(|(attribute_name, details)| attribute_info(attribute_name, details))
            .collect(),
    }
}

/// Builds detailed attribute discovery record.
fn attribute_info(attribute_name: String, aggregate: AttributeAggregate) -> AttributeInfo {
    AttributeInfo {
        id: attribute_name.clone(),
        r#type: "Attribute".to_string(),
        attribute_name,
        attribute_count: aggregate.count,
        attribute_types: aggregate.attribute_types.into_iter().collect(),
        type_names: aggregate.type_names.into_iter().collect(),
    }
}

/// Removes duplicate entities by `id` during discovery aggregation.
fn dedupe(items: Vec<Value>) -> Vec<Value> {
    let mut seen = BTreeSet::new();
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
