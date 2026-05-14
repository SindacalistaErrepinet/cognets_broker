use async_trait::async_trait;
use bson::{Document, doc, from_document, to_document};
use mongodb::{Collection, Database, IndexModel, options::IndexOptions};
use serde_json::Value;

use crate::{
    domain::types::{
        PeerDocument, StoredDocument, SubscriptionDocument, SwarmMutationDocument,
        TemporalEntityDocument,
    },
    error::BrokerError,
    persistence::repository::{
        EntityRepository, PeerRepository, SubscriptionRepository, SwarmMutationRepository,
        TemporalRepository, update_status_field,
    },
    query::planner::{GeoFilter, MongoQueryPlan, TemporalFilter},
};

#[derive(Clone)]
pub struct MongoRepositories {
    pub entities: MongoEntityRepository,
    pub temporals: MongoTemporalRepository,
    pub subscriptions: MongoSubscriptionRepository,
    pub peers: MongoPeerRepository,
    pub swarm_mutations: MongoSwarmMutationRepository,
}

impl MongoRepositories {
    /// Builds Mongo-backed repositories and creates required indexes.
    pub async fn new(db: &Database) -> Result<Self, BrokerError> {
        let repositories = Self {
            entities: MongoEntityRepository::new(db.collection("entities")),
            temporals: MongoTemporalRepository::new(db.collection("temporal_entities")),
            subscriptions: MongoSubscriptionRepository::new(db.collection("subscriptions")),
            peers: MongoPeerRepository::new(db.collection("peers")),
            swarm_mutations: MongoSwarmMutationRepository::new(db.collection("swarm_mutations")),
        };

        repositories.ensure_indexes().await?;
        Ok(repositories)
    }

    #[cfg(test)]
    /// Builds Mongo-backed repositories without creating indexes for tests.
    pub fn new_without_indexes(db: &Database) -> Self {
        Self {
            entities: MongoEntityRepository::new(db.collection("entities")),
            temporals: MongoTemporalRepository::new(db.collection("temporal_entities")),
            subscriptions: MongoSubscriptionRepository::new(db.collection("subscriptions")),
            peers: MongoPeerRepository::new(db.collection("peers")),
            swarm_mutations: MongoSwarmMutationRepository::new(db.collection("swarm_mutations")),
        }
    }

    /// Creates indexes for all Mongo-backed repositories.
    async fn ensure_indexes(&self) -> Result<(), BrokerError> {
        self.entities.ensure_indexes().await?;
        self.temporals.ensure_indexes().await?;
        self.subscriptions.ensure_indexes().await?;
        self.peers.ensure_indexes().await?;
        self.swarm_mutations.ensure_indexes().await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct MongoEntityRepository {
    collection: Collection<Document>,
}

impl MongoEntityRepository {
    /// Wraps Mongo collection storing entity documents.
    fn new(collection: Collection<Document>) -> Self {
        Self { collection }
    }

    /// Ensures entity collection indexes used by queries and uniqueness checks.
    async fn ensure_indexes(&self) -> Result<(), BrokerError> {
        self.collection
            .create_index(
                IndexModel::builder()
                    .keys(doc! {"tenant": 1, "id": 1})
                    .options(IndexOptions::builder().unique(true).build())
                    .build(),
            )
            .await
            .map_err(mongo_error)?;

        self.collection
            .create_index(IndexModel::builder().keys(doc! {"doc.type": 1}).build())
            .await
            .map_err(mongo_error)?;

        self.collection
            .create_index(
                IndexModel::builder()
                    .keys(doc! {"doc.location.value": "2dsphere"})
                    .build(),
            )
            .await
            .ok();

        Ok(())
    }
}

#[async_trait]
impl EntityRepository for MongoEntityRepository {
    /// Loads one entity document by tenant and NGSI-LD id.
    async fn get(
        &self,
        tenant: &str,
        entity_id: &str,
    ) -> Result<Option<StoredDocument>, BrokerError> {
        let document = self
            .collection
            .find_one(doc! {"tenant": tenant, "id": entity_id})
            .await
            .map_err(mongo_error)?;
        document.map(decode).transpose()
    }

    /// Inserts new entity document.
    async fn insert(&self, document: StoredDocument) -> Result<(), BrokerError> {
        self.collection
            .insert_one(encode(&document)?)
            .await
            .map_err(mongo_error)?;
        Ok(())
    }

    /// Replaces or upserts entity document by tenant and id.
    async fn replace(&self, document: StoredDocument) -> Result<(), BrokerError> {
        self.collection
            .replace_one(
                doc! {"tenant": &document.tenant, "id": &document.ngsi_id},
                encode(&document)?,
            )
            .upsert(true)
            .await
            .map_err(mongo_error)?;
        Ok(())
    }

    /// Deletes one entity document and returns removed payload.
    async fn delete(
        &self,
        tenant: &str,
        entity_id: &str,
    ) -> Result<Option<StoredDocument>, BrokerError> {
        let document = self
            .collection
            .find_one_and_delete(doc! {"tenant": tenant, "id": entity_id})
            .await
            .map_err(mongo_error)?;
        document.map(decode).transpose()
    }

    /// Queries entity documents using prepared Mongo filter plan.
    async fn query(
        &self,
        tenant: &str,
        plan: &MongoQueryPlan,
    ) -> Result<Vec<StoredDocument>, BrokerError> {
        let filter = plan.to_bson_filter(tenant)?;
        let mut find = self.collection.find(filter);
        if let Some(limit) = plan.limit {
            find = find.limit(limit as i64);
        }

        let documents = collect_documents(find.await.map_err(mongo_error)?).await?;
        documents.into_iter().map(decode).collect()
    }
}

#[derive(Clone)]
pub struct MongoTemporalRepository {
    collection: Collection<Document>,
}

impl MongoTemporalRepository {
    /// Wraps Mongo collection storing temporal entity documents.
    fn new(collection: Collection<Document>) -> Self {
        Self { collection }
    }

    /// Ensures temporal collection indexes used by lookups and uniqueness checks.
    async fn ensure_indexes(&self) -> Result<(), BrokerError> {
        self.collection
            .create_index(
                IndexModel::builder()
                    .keys(doc! {"tenant": 1, "id": 1})
                    .options(IndexOptions::builder().unique(true).build())
                    .build(),
            )
            .await
            .map_err(mongo_error)?;
        Ok(())
    }
}

#[async_trait]
impl TemporalRepository for MongoTemporalRepository {
    /// Loads one temporal entity document by tenant and NGSI-LD id.
    async fn get(
        &self,
        tenant: &str,
        entity_id: &str,
    ) -> Result<Option<TemporalEntityDocument>, BrokerError> {
        let document = self
            .collection
            .find_one(doc! {"tenant": tenant, "id": entity_id})
            .await
            .map_err(mongo_error)?;
        document.map(decode).transpose()
    }

    /// Replaces or upserts temporal entity and reports creation state.
    async fn upsert(&self, document: TemporalEntityDocument) -> Result<bool, BrokerError> {
        let exists = self
            .get(&document.tenant, &document.ngsi_id)
            .await?
            .is_some();
        self.collection
            .replace_one(
                doc! {"tenant": &document.tenant, "id": &document.ngsi_id},
                encode(&document)?,
            )
            .upsert(true)
            .await
            .map_err(mongo_error)?;
        Ok(!exists)
    }

    /// Deletes one temporal entity and reports whether anything was removed.
    async fn delete(&self, tenant: &str, entity_id: &str) -> Result<bool, BrokerError> {
        let deleted = self
            .collection
            .find_one_and_delete(doc! {"tenant": tenant, "id": entity_id})
            .await
            .map_err(mongo_error)?;
        Ok(deleted.is_some())
    }

    /// Queries temporal entity documents using prepared Mongo filter plan.
    async fn query(
        &self,
        tenant: &str,
        plan: &MongoQueryPlan,
        _temporal: &TemporalFilter,
        _geo: Option<&GeoFilter>,
    ) -> Result<Vec<TemporalEntityDocument>, BrokerError> {
        let filter = plan.to_bson_filter(tenant)?;
        let mut find = self.collection.find(filter);
        if let Some(limit) = plan.limit {
            find = find.limit(limit as i64);
        }

        let documents = collect_documents(find.await.map_err(mongo_error)?).await?;
        documents.into_iter().map(decode).collect()
    }
}

#[derive(Clone)]
pub struct MongoSubscriptionRepository {
    collection: Collection<Document>,
}

impl MongoSubscriptionRepository {
    /// Wraps Mongo collection storing subscription documents.
    fn new(collection: Collection<Document>) -> Self {
        Self { collection }
    }

    /// Ensures subscription collection indexes used by lookups and uniqueness checks.
    async fn ensure_indexes(&self) -> Result<(), BrokerError> {
        self.collection
            .create_index(
                IndexModel::builder()
                    .keys(doc! {"tenant": 1, "id": 1})
                    .options(IndexOptions::builder().unique(true).build())
                    .build(),
            )
            .await
            .map_err(mongo_error)?;
        Ok(())
    }
}

#[async_trait]
impl SubscriptionRepository for MongoSubscriptionRepository {
    /// Loads one subscription document by tenant and id.
    async fn get(
        &self,
        tenant: &str,
        subscription_id: &str,
    ) -> Result<Option<SubscriptionDocument>, BrokerError> {
        let document = self
            .collection
            .find_one(doc! {"tenant": tenant, "id": subscription_id})
            .await
            .map_err(mongo_error)?;
        document.map(decode).transpose()
    }

    /// Inserts new subscription document.
    async fn insert(&self, document: SubscriptionDocument) -> Result<(), BrokerError> {
        self.collection
            .insert_one(encode(&document)?)
            .await
            .map_err(mongo_error)?;
        Ok(())
    }

    /// Replaces or upserts subscription document by tenant and id.
    async fn replace(&self, document: SubscriptionDocument) -> Result<(), BrokerError> {
        self.collection
            .replace_one(
                doc! {"tenant": &document.tenant, "id": &document.ngsi_id},
                encode(&document)?,
            )
            .upsert(true)
            .await
            .map_err(mongo_error)?;
        Ok(())
    }

    /// Deletes one subscription document and returns removed payload.
    async fn delete(
        &self,
        tenant: &str,
        subscription_id: &str,
    ) -> Result<Option<SubscriptionDocument>, BrokerError> {
        let document = self
            .collection
            .find_one_and_delete(doc! {"tenant": tenant, "id": subscription_id})
            .await
            .map_err(mongo_error)?;
        document.map(decode).transpose()
    }

    /// Lists tenant subscriptions with optional limit.
    async fn list(
        &self,
        tenant: &str,
        limit: Option<usize>,
    ) -> Result<Vec<SubscriptionDocument>, BrokerError> {
        let mut find = self.collection.find(doc! {"tenant": tenant});
        if let Some(limit) = limit {
            find = find.limit(limit as i64);
        }

        let documents = collect_documents(find.await.map_err(mongo_error)?).await?;
        documents.into_iter().map(decode).collect()
    }

    /// Updates notification delivery counters after one delivery attempt.
    async fn mark_delivery(
        &self,
        tenant: &str,
        subscription_id: &str,
        success: bool,
        now: &str,
    ) -> Result<(), BrokerError> {
        let mut subscription = match self.get(tenant, subscription_id).await? {
            Some(subscription) => subscription,
            None => return Ok(()),
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
        self.replace(subscription).await
    }
}

#[derive(Clone)]
pub struct MongoPeerRepository {
    collection: Collection<Document>,
}

impl MongoPeerRepository {
    /// Wraps Mongo collection storing SWIM peer membership records.
    fn new(collection: Collection<Document>) -> Self {
        Self { collection }
    }

    /// Ensures peer collection indexes used by membership lookups.
    async fn ensure_indexes(&self) -> Result<(), BrokerError> {
        self.collection
            .create_index(
                IndexModel::builder()
                    .keys(doc! {"tenant": 1, "peerId": 1})
                    .options(IndexOptions::builder().unique(true).build())
                    .build(),
            )
            .await
            .map_err(mongo_error)?;
        Ok(())
    }
}

#[async_trait]
impl PeerRepository for MongoPeerRepository {
    /// Loads one SWIM peer membership record.
    async fn get(&self, tenant: &str, peer_id: &str) -> Result<Option<PeerDocument>, BrokerError> {
        let document = self
            .collection
            .find_one(doc! {"tenant": tenant, "peerId": peer_id})
            .await
            .map_err(mongo_error)?;
        document.map(decode).transpose()
    }

    /// Replaces or upserts one SWIM peer membership record.
    async fn upsert(&self, document: PeerDocument) -> Result<(), BrokerError> {
        self.collection
            .replace_one(
                doc! {"tenant": &document.tenant, "peerId": &document.peer_id},
                encode(&document)?,
            )
            .upsert(true)
            .await
            .map_err(mongo_error)?;
        Ok(())
    }

    /// Lists SWIM peer membership records for tenant.
    async fn list(&self, tenant: &str) -> Result<Vec<PeerDocument>, BrokerError> {
        let documents = collect_documents(
            self.collection
                .find(doc! {"tenant": tenant})
                .await
                .map_err(mongo_error)?,
        )
        .await?;
        documents.into_iter().map(decode).collect()
    }
}

#[derive(Clone)]
pub struct MongoSwarmMutationRepository {
    collection: Collection<Document>,
}

impl MongoSwarmMutationRepository {
    /// Wraps Mongo collection storing replicated mutation log entries.
    fn new(collection: Collection<Document>) -> Self {
        Self { collection }
    }

    /// Ensures mutation log indexes used by dedupe and cursor scans.
    async fn ensure_indexes(&self) -> Result<(), BrokerError> {
        self.collection
            .create_index(
                IndexModel::builder()
                    .keys(doc! {"tenant": 1, "mutationId": 1})
                    .options(IndexOptions::builder().unique(true).build())
                    .build(),
            )
            .await
            .map_err(mongo_error)?;

        self.collection
            .create_index(
                IndexModel::builder()
                    .keys(doc! {"tenant": 1, "recordedAtNanos": 1})
                    .build(),
            )
            .await
            .map_err(mongo_error)?;

        self.collection
            .create_index(
                IndexModel::builder()
                    .keys(doc! {"tenant": 1, "entityId": 1, "versionAtNanos": 1})
                    .build(),
            )
            .await
            .map_err(mongo_error)?;

        Ok(())
    }
}

#[async_trait]
impl SwarmMutationRepository for MongoSwarmMutationRepository {
    /// Loads one swarm mutation by tenant and mutation id.
    async fn get(
        &self,
        tenant: &str,
        mutation_id: &str,
    ) -> Result<Option<SwarmMutationDocument>, BrokerError> {
        let document = self
            .collection
            .find_one(doc! {"tenant": tenant, "mutationId": mutation_id})
            .await
            .map_err(mongo_error)?;
        document.map(decode).transpose()
    }

    /// Inserts new swarm mutation log entry.
    async fn insert(&self, document: SwarmMutationDocument) -> Result<(), BrokerError> {
        self.collection
            .insert_one(encode(&document)?)
            .await
            .map_err(mongo_error)?;
        Ok(())
    }

    /// Lists swarm mutations after optional cursor in recorded order.
    async fn list_since(
        &self,
        tenant: &str,
        since_nanos: Option<i64>,
        limit: usize,
    ) -> Result<Vec<SwarmMutationDocument>, BrokerError> {
        let filter = match since_nanos {
            Some(since_nanos) => doc! {
                "tenant": tenant,
                "recordedAtNanos": {"$gt": since_nanos},
            },
            None => doc! {"tenant": tenant},
        };

        let documents = collect_documents(
            self.collection
                .find(filter)
                .sort(doc! {"recordedAtNanos": 1})
                .limit(limit as i64)
                .await
                .map_err(mongo_error)?,
        )
        .await?;
        documents.into_iter().map(decode).collect()
    }
}

/// Encodes typed document into BSON document.
fn encode<T: serde::Serialize>(value: &T) -> Result<Document, BrokerError> {
    to_document(value)
        .map_err(|error| BrokerError::internal(format!("bson encoding error: {error}")))
}

/// Decodes BSON document into typed value.
fn decode<T: serde::de::DeserializeOwned>(value: Document) -> Result<T, BrokerError> {
    from_document(value)
        .map_err(|error| BrokerError::internal(format!("bson decoding error: {error}")))
}

/// Maps MongoDB driver error into broker internal error.
fn mongo_error(error: mongodb::error::Error) -> BrokerError {
    BrokerError::internal(format!("mongodb error: {error}"))
}

/// Collects all documents from Mongo cursor into memory.
async fn collect_documents(
    mut cursor: mongodb::Cursor<Document>,
) -> Result<Vec<Document>, BrokerError> {
    let mut items = Vec::new();
    while cursor.advance().await.map_err(mongo_error)? {
        items.push(cursor.deserialize_current().map_err(mongo_error)?);
    }
    Ok(items)
}
