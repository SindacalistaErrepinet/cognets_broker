use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use serde_json::Value;

use crate::{
    domain::types::{StoredDocument, SubscriptionDocument, TemporalEntityDocument},
    error::BrokerError,
    persistence::repository::{
        EntityRepository, Repositories, SubscriptionRepository, TemporalRepository,
        filter_entity_documents, filter_temporal_documents, update_status_field,
    },
    query::planner::{GeoFilter, MongoQueryPlan, TemporalFilter},
};

type Key = (String, String);

/// Builds in-memory repositories used by tests.
pub fn repositories() -> Repositories {
    Repositories::new(
        Arc::new(MemoryEntityRepository::default()),
        Arc::new(MemoryTemporalRepository::default()),
        Arc::new(MemorySubscriptionRepository::default()),
    )
}

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
        plan: &MongoQueryPlan,
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
        plan: &MongoQueryPlan,
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
        let key = (tenant.to_string(), subscription_id.to_string());
        let mut documents = self.documents.lock().unwrap();
        let Some(subscription) = documents.get_mut(&key) else {
            return Ok(());
        };

        if let Some(notification) = subscription
            .doc
            .get_mut("notification")
            .and_then(Value::as_object_mut)
        {
            let sent = notification
                .get("timesSent")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                + 1;
            notification.insert("timesSent".to_string(), Value::from(sent));
            notification.insert(
                "status".to_string(),
                Value::String(if success { "ok" } else { "failed" }.to_string()),
            );
            notification.insert(
                "lastNotification".to_string(),
                Value::String(now.to_string()),
            );

            if success {
                notification.insert("lastSuccess".to_string(), Value::String(now.to_string()));
            } else {
                let failed = notification
                    .get("timesFailed")
                    .and_then(Value::as_u64)
                    .unwrap_or(0)
                    + 1;
                notification.insert("timesFailed".to_string(), Value::from(failed));
                notification.insert("lastFailure".to_string(), Value::String(now.to_string()));
            }
        }

        update_status_field(
            &mut subscription.doc,
            "modifiedAt",
            Value::String(now.to_string()),
        );
        Ok(())
    }
}

fn key(tenant: &str, id: &str) -> Key {
    (tenant.to_string(), id.to_string())
}
