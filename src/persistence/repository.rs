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
    domain::types::{
        EntityMutationDocument, StoredDocument, SubscriptionDocument, TemporalEntityDocument,
    },
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
    /// Replicated entity mutation-log storage.
    pub entity_mutations: Arc<dyn EntityMutationRepository>,
}

impl Repositories {
    /// Bundles repository implementations used by application state.
    pub fn new(
        entities: Arc<dyn EntityRepository>,
        temporals: Arc<dyn TemporalRepository>,
        subscriptions: Arc<dyn SubscriptionRepository>,
        entity_mutations: Arc<dyn EntityMutationRepository>,
    ) -> Self {
        Self {
            entities,
            temporals,
            subscriptions,
            entity_mutations,
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
    /// Inserts multiple new entity documents.
    async fn insert_many(&self, documents: Vec<StoredDocument>) -> Result<(), BrokerError> {
        for document in documents {
            self.insert(document).await?;
        }
        Ok(())
    }
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
    ///
    /// Implementations may coalesce or defer these writes because delivery
    /// accounting is observability metadata, not entity replication state.
    async fn mark_delivery(
        &self,
        tenant: &str,
        subscription_id: &str,
        success: bool,
        now: &str,
    ) -> Result<(), BrokerError>;

    /// Coalesces delivery accounting for many notification attempts.
    async fn mark_delivery_batch(
        &self,
        tenant: &str,
        subscription_id: &str,
        successes: u64,
        failures: u64,
        now: &str,
    ) -> Result<(), BrokerError> {
        for _ in 0..successes {
            self.mark_delivery(tenant, subscription_id, true, now)
                .await?;
        }
        for _ in 0..failures {
            self.mark_delivery(tenant, subscription_id, false, now)
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
/// Replicated mutation-log operations used by fast cross-node notification worker.
pub trait EntityMutationRepository: Send + Sync {
    /// Appends one entity mutation event.
    async fn insert(&self, document: EntityMutationDocument) -> Result<(), BrokerError>;

    /// Appends multiple entity mutation events.
    async fn insert_many(&self, documents: Vec<EntityMutationDocument>) -> Result<(), BrokerError> {
        for document in documents {
            self.insert(document).await?;
        }
        Ok(())
    }

    /// Lists events at or after given origin timestamp.
    ///
    /// Implementations may over-return and filter in process; callers dedupe by
    /// `event_id`, so exact storage-side cursor semantics are not required.
    async fn list_after(
        &self,
        created_after_millis: i64,
    ) -> Result<Vec<EntityMutationDocument>, BrokerError>;
}

/// Updates one top-level status field in JSON payload.
pub fn update_status_field(document: &mut Value, field: &str, value: Value) {
    if let Some(object) = document.as_object_mut() {
        object.insert(field.to_string(), value);
    }
}

/// Applies coalesced notification delivery counters to subscription payload.
pub fn update_delivery_fields(document: &mut Value, successes: u64, failures: u64, now: &str) {
    let attempts = successes + failures;
    if attempts == 0 {
        return;
    }

    if let Some(notification) = document
        .get_mut("notification")
        .and_then(Value::as_object_mut)
    {
        let sent = notification
            .get("timesSent")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            + attempts;
        notification.insert("timesSent".to_string(), Value::from(sent));
        notification.insert(
            "status".to_string(),
            Value::String(if failures == 0 { "ok" } else { "failed" }.to_string()),
        );
        notification.insert(
            "lastNotification".to_string(),
            Value::String(now.to_string()),
        );

        if successes > 0 {
            notification.insert("lastSuccess".to_string(), Value::String(now.to_string()));
        }
        if failures > 0 {
            let failed = notification
                .get("timesFailed")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                + failures;
            notification.insert("timesFailed".to_string(), Value::from(failed));
            notification.insert("lastFailure".to_string(), Value::String(now.to_string()));
        }
    }

    update_status_field(document, "modifiedAt", Value::String(now.to_string()));
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
