//! In-memory repository implementations used mainly by tests.
use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;

use crate::{
    domain::types::{
        EntityMutationDocument, StoredDocument, SubscriptionDocument, TemporalEntityDocument,
    },
    error::BrokerError,
    persistence::repository::{
        EntityMutationRepository, EntityRepository, Repositories, SubscriptionRepository,
        TemporalRepository, filter_entity_documents, filter_temporal_documents,
        update_delivery_fields,
    },
    query::planner::{GeoFilter, QueryPlan, TemporalFilter},
};

type Key = (String, String);

/// Builds in-memory repositories used by tests.
pub fn repositories() -> Repositories {
    Repositories::new(
        Arc::new(MemoryEntityRepository::default()),
        Arc::new(MemoryTemporalRepository::default()),
        Arc::new(MemorySubscriptionRepository::default()),
        Arc::new(MemoryEntityMutationRepository::default()),
    )
}

/// In-memory entity repository keyed by `(tenant, entity_id)`.
#[derive(Default)]
pub struct MemoryEntityRepository {
    documents: Mutex<HashMap<Key, StoredDocument>>,
}

#[async_trait]
impl EntityRepository for MemoryEntityRepository {
    async fn get(
        &self,
        tenant: &str,
        entity_id: &str,
    ) -> Result<Option<StoredDocument>, BrokerError> {
        Ok(self
            .documents
            .lock()
            .unwrap()
            .get(&(tenant.to_string(), entity_id.to_string()))
            .cloned())
    }

    async fn insert(&self, document: StoredDocument) -> Result<(), BrokerError> {
        let key = key(&document.tenant, &document.ngsi_id);
        let mut documents = self.documents.lock().unwrap();
        if documents.contains_key(&key) {
            return Err(BrokerError::Conflict(format!(
                "entity {} already exists",
                document.ngsi_id
            )));
        }
        documents.insert(key, document);
        Ok(())
    }

    async fn insert_many(&self, new_documents: Vec<StoredDocument>) -> Result<(), BrokerError> {
        let mut documents = self.documents.lock().unwrap();
        for document in &new_documents {
            let key = key(&document.tenant, &document.ngsi_id);
            if documents.contains_key(&key) {
                return Err(BrokerError::Conflict(format!(
                    "entity {} already exists",
                    document.ngsi_id
                )));
            }
        }
        for document in new_documents {
            documents.insert(key(&document.tenant, &document.ngsi_id), document);
        }
        Ok(())
    }

    async fn replace(&self, document: StoredDocument) -> Result<(), BrokerError> {
        self.documents
            .lock()
            .unwrap()
            .insert(key(&document.tenant, &document.ngsi_id), document);
        Ok(())
    }

    async fn delete(
        &self,
        tenant: &str,
        entity_id: &str,
    ) -> Result<Option<StoredDocument>, BrokerError> {
        Ok(self
            .documents
            .lock()
            .unwrap()
            .remove(&(tenant.to_string(), entity_id.to_string())))
    }

    async fn query(
        &self,
        tenant: &str,
        plan: &QueryPlan,
    ) -> Result<Vec<StoredDocument>, BrokerError> {
        let documents = self
            .documents
            .lock()
            .unwrap()
            .values()
            .filter(|document| document.tenant == tenant)
            .cloned()
            .collect::<Vec<_>>();
        filter_entity_documents(documents, plan)
    }

    async fn list_tenants(&self) -> Result<Vec<String>, BrokerError> {
        Ok(self
            .documents
            .lock()
            .unwrap()
            .values()
            .map(|document| document.tenant.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect())
    }
}

/// In-memory temporal repository keyed by `(tenant, entity_id)`.
#[derive(Default)]
pub struct MemoryTemporalRepository {
    documents: Mutex<HashMap<Key, TemporalEntityDocument>>,
}

#[async_trait]
impl TemporalRepository for MemoryTemporalRepository {
    async fn get(
        &self,
        tenant: &str,
        entity_id: &str,
    ) -> Result<Option<TemporalEntityDocument>, BrokerError> {
        Ok(self
            .documents
            .lock()
            .unwrap()
            .get(&(tenant.to_string(), entity_id.to_string()))
            .cloned())
    }

    async fn upsert(&self, document: TemporalEntityDocument) -> Result<bool, BrokerError> {
        let key = key(&document.tenant, &document.ngsi_id);
        let mut documents = self.documents.lock().unwrap();
        let created = !documents.contains_key(&key);
        documents.insert(key, document);
        Ok(created)
    }

    async fn delete(&self, tenant: &str, entity_id: &str) -> Result<bool, BrokerError> {
        Ok(self
            .documents
            .lock()
            .unwrap()
            .remove(&(tenant.to_string(), entity_id.to_string()))
            .is_some())
    }

    async fn query(
        &self,
        tenant: &str,
        plan: &QueryPlan,
        _temporal: &TemporalFilter,
        _geo: Option<&GeoFilter>,
    ) -> Result<Vec<TemporalEntityDocument>, BrokerError> {
        let documents = self
            .documents
            .lock()
            .unwrap()
            .values()
            .filter(|document| document.tenant == tenant)
            .cloned()
            .collect::<Vec<_>>();
        filter_temporal_documents(documents, plan)
    }
}

/// In-memory subscription repository keyed by `(tenant, subscription_id)`.
#[derive(Default)]
pub struct MemorySubscriptionRepository {
    documents: Mutex<HashMap<Key, SubscriptionDocument>>,
}

#[async_trait]
impl SubscriptionRepository for MemorySubscriptionRepository {
    async fn get(
        &self,
        tenant: &str,
        subscription_id: &str,
    ) -> Result<Option<SubscriptionDocument>, BrokerError> {
        Ok(self
            .documents
            .lock()
            .unwrap()
            .get(&(tenant.to_string(), subscription_id.to_string()))
            .cloned())
    }

    async fn insert(&self, document: SubscriptionDocument) -> Result<(), BrokerError> {
        let key = key(&document.tenant, &document.ngsi_id);
        let mut documents = self.documents.lock().unwrap();
        if documents.contains_key(&key) {
            return Err(BrokerError::Conflict(format!(
                "subscription {} already exists",
                document.ngsi_id
            )));
        }
        documents.insert(key, document);
        Ok(())
    }

    async fn replace(&self, document: SubscriptionDocument) -> Result<(), BrokerError> {
        self.documents
            .lock()
            .unwrap()
            .insert(key(&document.tenant, &document.ngsi_id), document);
        Ok(())
    }

    async fn delete(
        &self,
        tenant: &str,
        subscription_id: &str,
    ) -> Result<Option<SubscriptionDocument>, BrokerError> {
        Ok(self
            .documents
            .lock()
            .unwrap()
            .remove(&(tenant.to_string(), subscription_id.to_string())))
    }

    async fn list(
        &self,
        tenant: &str,
        limit: Option<usize>,
    ) -> Result<Vec<SubscriptionDocument>, BrokerError> {
        let mut documents = self
            .documents
            .lock()
            .unwrap()
            .values()
            .filter(|document| document.tenant == tenant)
            .cloned()
            .collect::<Vec<_>>();
        if let Some(limit) = limit {
            documents.truncate(limit);
        }
        Ok(documents)
    }

    async fn list_tenants(&self) -> Result<Vec<String>, BrokerError> {
        Ok(self
            .documents
            .lock()
            .unwrap()
            .values()
            .map(|document| document.tenant.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect())
    }

    async fn mark_delivery(
        &self,
        tenant: &str,
        subscription_id: &str,
        success: bool,
        now: &str,
    ) -> Result<(), BrokerError> {
        self.mark_delivery_batch(
            tenant,
            subscription_id,
            u64::from(success),
            u64::from(!success),
            now,
        )
        .await
    }

    async fn mark_delivery_batch(
        &self,
        tenant: &str,
        subscription_id: &str,
        successes: u64,
        failures: u64,
        now: &str,
    ) -> Result<(), BrokerError> {
        let key = (tenant.to_string(), subscription_id.to_string());
        let mut documents = self.documents.lock().unwrap();
        let Some(subscription) = documents.get_mut(&key) else {
            return Ok(());
        };

        update_delivery_fields(&mut subscription.doc, successes, failures, now);
        Ok(())
    }
}

/// In-memory mutation-log repository keyed by event id.
#[derive(Default)]
pub struct MemoryEntityMutationRepository {
    documents: Mutex<HashMap<String, EntityMutationDocument>>,
}

#[async_trait]
impl EntityMutationRepository for MemoryEntityMutationRepository {
    async fn insert(&self, document: EntityMutationDocument) -> Result<(), BrokerError> {
        self.documents
            .lock()
            .unwrap()
            .insert(document.event_id.clone(), document);
        Ok(())
    }

    async fn insert_many(&self, documents: Vec<EntityMutationDocument>) -> Result<(), BrokerError> {
        let mut stored = self.documents.lock().unwrap();
        for document in documents {
            stored.insert(document.event_id.clone(), document);
        }
        Ok(())
    }

    async fn list_after(
        &self,
        created_after_millis: i64,
    ) -> Result<Vec<EntityMutationDocument>, BrokerError> {
        let mut documents = self
            .documents
            .lock()
            .unwrap()
            .values()
            .filter(|document| document.created_at_millis >= created_after_millis)
            .cloned()
            .collect::<Vec<_>>();
        documents.sort_by(|left, right| {
            left.created_at_millis
                .cmp(&right.created_at_millis)
                .then_with(|| left.event_id.cmp(&right.event_id))
        });
        Ok(documents)
    }
}

fn key(tenant: &str, id: &str) -> Key {
    (tenant.to_string(), id.to_string())
}
