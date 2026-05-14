use async_trait::async_trait;
use serde_json::Value;

use crate::{
    domain::types::{
        PeerDocument, StoredDocument, SubscriptionDocument, SwarmMutationDocument,
        TemporalEntityDocument,
    },
    error::BrokerError,
    query::planner::{GeoFilter, MongoQueryPlan, TemporalFilter},
};

#[async_trait]
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
    /// Queries entity documents using prepared Mongo query plan.
    async fn query(
        &self,
        tenant: &str,
        plan: &MongoQueryPlan,
    ) -> Result<Vec<StoredDocument>, BrokerError>;
}

#[async_trait]
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
        plan: &MongoQueryPlan,
        temporal: &TemporalFilter,
        geo: Option<&GeoFilter>,
    ) -> Result<Vec<TemporalEntityDocument>, BrokerError>;
}

#[async_trait]
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
    /// Updates delivery accounting fields after notification attempt.
    async fn mark_delivery(
        &self,
        tenant: &str,
        subscription_id: &str,
        success: bool,
        now: &str,
    ) -> Result<(), BrokerError>;
}

#[async_trait]
pub trait PeerRepository: Send + Sync {
    /// Retrieves one peer membership record.
    async fn get(&self, tenant: &str, peer_id: &str) -> Result<Option<PeerDocument>, BrokerError>;
    /// Upserts peer membership record.
    async fn upsert(&self, document: PeerDocument) -> Result<(), BrokerError>;
    /// Lists all peers for tenant.
    async fn list(&self, tenant: &str) -> Result<Vec<PeerDocument>, BrokerError>;
}

#[async_trait]
pub trait SwarmMutationRepository: Send + Sync {
    /// Retrieves one swarm mutation by tenant and mutation id.
    async fn get(
        &self,
        tenant: &str,
        mutation_id: &str,
    ) -> Result<Option<SwarmMutationDocument>, BrokerError>;
    /// Inserts new swarm mutation document.
    async fn insert(&self, document: SwarmMutationDocument) -> Result<(), BrokerError>;
    /// Lists mutation log entries after optional cursor.
    async fn list_since(
        &self,
        tenant: &str,
        since_nanos: Option<i64>,
        limit: usize,
    ) -> Result<Vec<SwarmMutationDocument>, BrokerError>;
}

/// Updates one top-level status field in JSON payload.
pub fn update_status_field(document: &mut Value, field: &str, value: Value) {
    if let Some(object) = document.as_object_mut() {
        object.insert(field.to_string(), value);
    }
}
