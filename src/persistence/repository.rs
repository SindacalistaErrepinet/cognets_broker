//! Storage abstraction shared by application services.
//!
//! Services talk to these traits instead of concrete backends. This keeps core
//! broker logic independent from in-memory test storage and DefraDB-backed
//! runtime storage.
use std::sync::Arc;

use async_trait::async_trait;
use regex::Regex;
use serde_json::Value;

use crate::{
    domain::types::{StoredDocument, SubscriptionDocument, TemporalEntityDocument},
    error::BrokerError,
    query::planner::{GeoFilter, QueryPlan, TemporalFilter},
};

/// Bundle of repository implementations used by shared application state.
#[derive(Clone)]
pub struct Repositories {
    /// Entity snapshot storage.
    pub entities: Arc<dyn EntityRepository>,
    /// Temporal entity storage.
    pub temporals: Arc<dyn TemporalRepository>,
    /// Subscription storage.
    pub subscriptions: Arc<dyn SubscriptionRepository>,
}

impl Repositories {
    /// Bundles repository implementations used by application state.
    pub fn new(
        entities: Arc<dyn EntityRepository>,
        temporals: Arc<dyn TemporalRepository>,
        subscriptions: Arc<dyn SubscriptionRepository>,
    ) -> Self {
        Self {
            entities,
            temporals,
            subscriptions,
        }
    }
}

#[async_trait]
/// Entity storage operations scoped by tenant and logical entity id.
pub trait EntityRepository: Send + Sync {
    /// Retrieves one entity document by tenant and id.
    async fn get(
        &self,
        tenant: &str,
        entity_id: &str,
    ) -> Result<Option<StoredDocument>, BrokerError>;
    /// Inserts new entity document.
    async fn insert(&self, document: StoredDocument) -> Result<(), BrokerError>;
    /// Replaces existing entity document or upserts it.
    async fn replace(&self, document: StoredDocument) -> Result<(), BrokerError>;
    /// Deletes entity document and returns removed value.
    async fn delete(
        &self,
        tenant: &str,
        entity_id: &str,
    ) -> Result<Option<StoredDocument>, BrokerError>;
    /// Queries entity documents using prepared query plan.
    async fn query(
        &self,
        tenant: &str,
        plan: &QueryPlan,
    ) -> Result<Vec<StoredDocument>, BrokerError>;
    /// Lists tenants currently present in entity storage.
    async fn list_tenants(&self) -> Result<Vec<String>, BrokerError>;
}

#[async_trait]
/// Temporal entity storage operations.
pub trait TemporalRepository: Send + Sync {
    /// Retrieves one temporal entity document by tenant and id.
    async fn get(
        &self,
        tenant: &str,
        entity_id: &str,
    ) -> Result<Option<TemporalEntityDocument>, BrokerError>;
    /// Upserts temporal entity and returns whether it was newly created.
    async fn upsert(&self, document: TemporalEntityDocument) -> Result<bool, BrokerError>;
    /// Deletes temporal entity and returns whether anything was removed.
    async fn delete(&self, tenant: &str, entity_id: &str) -> Result<bool, BrokerError>;
    /// Queries temporal documents using entity and temporal filters.
    async fn query(
        &self,
        tenant: &str,
        plan: &QueryPlan,
        temporal: &TemporalFilter,
        geo: Option<&GeoFilter>,
    ) -> Result<Vec<TemporalEntityDocument>, BrokerError>;
}

#[async_trait]
/// Subscription storage and delivery-accounting operations.
pub trait SubscriptionRepository: Send + Sync {
    /// Retrieves one subscription by tenant and id.
    async fn get(
        &self,
        tenant: &str,
        subscription_id: &str,
    ) -> Result<Option<SubscriptionDocument>, BrokerError>;
    /// Inserts new subscription document.
    async fn insert(&self, document: SubscriptionDocument) -> Result<(), BrokerError>;
    /// Replaces existing subscription document or upserts it.
    async fn replace(&self, document: SubscriptionDocument) -> Result<(), BrokerError>;
    /// Deletes subscription document and returns removed value.
    async fn delete(
        &self,
        tenant: &str,
        subscription_id: &str,
    ) -> Result<Option<SubscriptionDocument>, BrokerError>;
    /// Lists subscriptions for tenant with optional limit.
    async fn list(
        &self,
        tenant: &str,
        limit: Option<usize>,
    ) -> Result<Vec<SubscriptionDocument>, BrokerError>;
    /// Lists tenants currently present in subscription storage.
    async fn list_tenants(&self) -> Result<Vec<String>, BrokerError>;
    /// Updates delivery accounting fields after notification attempt.
    async fn mark_delivery(
        &self,
        tenant: &str,
        subscription_id: &str,
        success: bool,
        now: &str,
    ) -> Result<(), BrokerError>;
}

/// Updates one top-level status field in JSON payload.
pub fn update_status_field(document: &mut Value, field: &str, value: Value) {
    if let Some(object) = document.as_object_mut() {
        object.insert(field.to_string(), value);
    }
}

/// Applies basic query-plan filters to entity wrapper documents.
pub fn filter_entity_documents(
    mut documents: Vec<StoredDocument>,
    plan: &QueryPlan,
) -> Result<Vec<StoredDocument>, BrokerError> {
    filter_documents(&mut documents, plan, |document| &document.doc)?;
    Ok(documents)
}

/// Applies basic query-plan filters to temporal wrapper documents.
pub fn filter_temporal_documents(
    mut documents: Vec<TemporalEntityDocument>,
    plan: &QueryPlan,
) -> Result<Vec<TemporalEntityDocument>, BrokerError> {
    filter_documents(&mut documents, plan, |document| &document.doc)?;
    Ok(documents)
}

fn filter_documents<T>(
    documents: &mut Vec<T>,
    plan: &QueryPlan,
    doc_of: impl Fn(&T) -> &Value,
) -> Result<(), BrokerError> {
    let id_pattern = plan
        .id_pattern
        .as_deref()
        .map(Regex::new)
        .transpose()
        .map_err(|error| BrokerError::BadRequest(format!("invalid idPattern: {error}")))?;

    documents.retain(|document| matches_query_plan(doc_of(document), plan, id_pattern.as_ref()));
    if let Some(limit) = plan.limit {
        documents.truncate(limit);
    }
    Ok(())
}

fn matches_query_plan(entity: &Value, plan: &QueryPlan, id_pattern: Option<&Regex>) -> bool {
    if !plan.ids.is_empty() {
        let entity_id = entity.get("id").and_then(Value::as_str).unwrap_or_default();
        if !plan.ids.iter().any(|candidate| candidate == entity_id) {
            return false;
        }
    }

    if !plan.entity_types.is_empty() {
        let entity_type = entity.get("type");
        if !plan
            .entity_types
            .iter()
            .any(|candidate| entity_type_matches(entity_type, candidate))
        {
            return false;
        }
    }

    if let Some(regex) = id_pattern {
        let entity_id = entity.get("id").and_then(Value::as_str).unwrap_or_default();
        if !regex.is_match(entity_id) {
            return false;
        }
    }

    plan.attrs.iter().all(|attr| entity.get(attr).is_some())
}

fn entity_type_matches(value: Option<&Value>, expected: &str) -> bool {
    match value {
        Some(Value::String(actual)) => actual == expected,
        Some(Value::Array(items)) => items.iter().any(|item| item.as_str() == Some(expected)),
        _ => false,
    }
}
