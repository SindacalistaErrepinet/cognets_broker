//! DefraDB-backed repository implementations.
//!
//! This module stores opaque NGSI-LD payloads through DefraDB's GraphQL API and
//! applies additional filtering in process when storage-side filtering is not
//! expressive enough for full NGSI-LD semantics.
use std::sync::Arc;

use async_trait::async_trait;
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    config::AppConfig,
    domain::types::{
        EntityMutationDocument, EntityMutationOperation, StoredDocument, SubscriptionDocument,
        TemporalEntityDocument,
    },
    error::BrokerError,
    persistence::repository::{
        EntityMutationRepository, EntityRepository, Repositories, SubscriptionRepository,
        TemporalRepository, filter_entity_documents, filter_temporal_documents,
        update_delivery_fields,
    },
    query::planner::{GeoFilter, QueryPlan, TemporalFilter},
};

const ENTITY_COLLECTION: &str = "EntityRecord";
const TEMPORAL_COLLECTION: &str = "TemporalRecord";
const SUBSCRIPTION_COLLECTION: &str = "SubscriptionRecord";
const ENTITY_MUTATION_COLLECTION: &str = "EntityMutationRecord";
const BULK_INSERT_CHUNK_SIZE: usize = 100;
const TRANSIENT_RETRY_ATTEMPTS: usize = 8;
const TRANSIENT_RETRY_BASE_DELAY_MS: u64 = 100;

/// Builds DefraDB-backed repositories over the GraphQL API.
pub struct DefraDbRepositories;

impl DefraDbRepositories {
    /// Creates repository set using configured DefraDB GraphQL endpoint.
    pub fn new(config: &AppConfig) -> Result<Repositories, BrokerError> {
        let client = Arc::new(DefraDbClient::new(
            &config.defradb_url,
            config.defradb_timeout_ms,
        )?);
        Ok(Repositories::new(
            Arc::new(DefraDbEntityRepository::new(client.clone())),
            Arc::new(DefraDbTemporalRepository::new(client.clone())),
            Arc::new(DefraDbSubscriptionRepository::new(client.clone())),
            Arc::new(DefraDbEntityMutationRepository::new(client)),
        ))
    }
}

#[derive(Clone)]
/// Small GraphQL client wrapper used by DefraDB repositories.
struct DefraDbClient {
    http: Client,
    graphql_url: String,
}

impl DefraDbClient {
    /// Creates GraphQL client with configured request timeout.
    fn new(graphql_url: &str, timeout_ms: u64) -> Result<Self, BrokerError> {
        Ok(Self {
            http: Client::builder()
                .timeout(std::time::Duration::from_millis(timeout_ms))
                .build()
                .map_err(|error| {
                    BrokerError::internal(format!("failed to create DefraDB HTTP client: {error}"))
                })?,
            graphql_url: graphql_url.to_string(),
        })
    }

    /// Executes GraphQL request and extracts typed `data` payload.
    async fn execute<T: DeserializeOwned>(
        &self,
        query: &str,
        variables: Value,
    ) -> Result<T, BrokerError> {
        for attempt in 1..=TRANSIENT_RETRY_ATTEMPTS {
            match self.execute_once(query, variables.clone()).await {
                Ok(data) => return Ok(data),
                Err(error) if attempt < TRANSIENT_RETRY_ATTEMPTS && is_transient_error(&error) => {
                    let delay_ms = TRANSIENT_RETRY_BASE_DELAY_MS * 2_u64.pow((attempt - 1) as u32);
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                }
                Err(error) => return Err(error),
            }
        }

        unreachable!("DefraDB retry loop always returns before exhaustion")
    }

    /// Executes one GraphQL request without retry logic.
    async fn execute_once<T: DeserializeOwned>(
        &self,
        query: &str,
        variables: Value,
    ) -> Result<T, BrokerError> {
        let response = self
            .http
            .post(&self.graphql_url)
            .json(&json!({
                "query": query,
                "variables": variables,
            }))
            .send()
            .await
            .map_err(|error| BrokerError::internal(format!("DefraDB request failed: {error}")))?;
        let status = response.status();
        let body = response.text().await.map_err(|error| {
            BrokerError::internal(format!("failed reading DefraDB response: {error}"))
        })?;

        if !status.is_success() {
            return Err(BrokerError::internal(format!(
                "DefraDB returned HTTP {}: {}",
                status.as_u16(),
                body
            )));
        }

        let parsed: GraphQlResponse<T> = serde_json::from_str(&body).map_err(|error| {
            BrokerError::internal(format!("invalid DefraDB GraphQL response: {error}"))
        })?;
        if let Some(errors) = parsed.errors
            && !errors.is_empty()
        {
            return Err(BrokerError::internal(format!(
                "DefraDB GraphQL error: {}",
                errors
                    .into_iter()
                    .map(|error| error.message)
                    .collect::<Vec<_>>()
                    .join("; ")
            )));
        }
        parsed.data.ok_or_else(|| {
            BrokerError::internal("DefraDB GraphQL response missing data".to_string())
        })
    }
}

/// Returns true for DefraDB optimistic transaction conflicts that are safe to retry.
fn is_transient_error(error: &BrokerError) -> bool {
    matches!(
        error,
        BrokerError::Internal(message)
            if message.contains("transaction conflict") && message.contains("Please retry")
    )
}

#[derive(Debug, Deserialize)]
/// Generic GraphQL response envelope.
struct GraphQlResponse<T> {
    data: Option<T>,
    errors: Option<Vec<GraphQlError>>,
}

#[derive(Debug, Deserialize)]
/// GraphQL error item returned by DefraDB.
struct GraphQlError {
    message: String,
}

#[derive(Debug, Deserialize, Clone)]
/// Opaque row used for entity and subscription records.
struct OpaqueRecordRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    tenant: String,
    #[serde(rename = "ngsiId")]
    ngsi_id: String,
    payload: String,
}

#[derive(Debug, Deserialize, Clone)]
/// Row used for temporal records with snapshot plus encoded history.
struct TemporalRecordRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    tenant: String,
    #[serde(rename = "ngsiId")]
    ngsi_id: String,
    payload: String,
    #[serde(rename = "historyJson")]
    history_json: String,
}

#[derive(Debug, Deserialize, Clone)]
/// Row used for replicated entity mutation-log records.
struct EntityMutationRecordRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    tenant: String,
    #[serde(rename = "eventId")]
    event_id: String,
    #[serde(rename = "entityId")]
    entity_id: String,
    operation: String,
    payload: String,
    #[serde(rename = "changedAttributesJson")]
    changed_attributes_json: String,
    #[serde(rename = "originBrokerId")]
    origin_broker_id: String,
    #[serde(rename = "createdAtMillis")]
    created_at_millis: f64,
}

#[derive(Clone)]
/// DefraDB-backed entity repository.
pub struct DefraDbEntityRepository {
    client: Arc<DefraDbClient>,
}

impl DefraDbEntityRepository {
    fn new(client: Arc<DefraDbClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl EntityRepository for DefraDbEntityRepository {
    async fn get(
        &self,
        tenant: &str,
        entity_id: &str,
    ) -> Result<Option<StoredDocument>, BrokerError> {
        let row = find_opaque_record(&self.client, ENTITY_COLLECTION, tenant, entity_id).await?;
        row.map(decode_entity_row).transpose()
    }

    async fn insert(&self, document: StoredDocument) -> Result<(), BrokerError> {
        if self
            .get(&document.tenant, &document.ngsi_id)
            .await?
            .is_some()
        {
            return Err(BrokerError::Conflict(format!(
                "entity {} already exists",
                document.ngsi_id
            )));
        }
        create_opaque_record(
            &self.client,
            ENTITY_COLLECTION,
            &document.tenant,
            &document.ngsi_id,
            &encode_json(&document.doc, "entity payload")?,
        )
        .await
    }

    async fn insert_many(&self, documents: Vec<StoredDocument>) -> Result<(), BrokerError> {
        if documents.is_empty() {
            return Ok(());
        }

        let tenant = documents[0].tenant.clone();
        if documents.iter().any(|document| document.tenant != tenant) {
            return Err(BrokerError::internal(
                "bulk entity insert requires one tenant".to_string(),
            ));
        }

        let mut existing = std::collections::HashSet::new();
        for document in &documents {
            if !existing.insert(document.ngsi_id.clone()) {
                return Err(BrokerError::Conflict(format!(
                    "entity {} already exists",
                    document.ngsi_id
                )));
            }
        }

        let inputs = documents
            .iter()
            .map(|document| {
                Ok(json!({
                    "tenant": document.tenant,
                    "ngsiId": document.ngsi_id,
                    "payload": encode_json(&document.doc, "entity payload")?,
                }))
            })
            .collect::<Result<Vec<_>, BrokerError>>()?;
        create_opaque_records(&self.client, ENTITY_COLLECTION, inputs).await
    }

    async fn replace(&self, document: StoredDocument) -> Result<(), BrokerError> {
        let payload = encode_json(&document.doc, "entity payload")?;
        if let Some(row) = find_opaque_record(
            &self.client,
            ENTITY_COLLECTION,
            &document.tenant,
            &document.ngsi_id,
        )
        .await?
        {
            update_opaque_record(
                &self.client,
                ENTITY_COLLECTION,
                &row.doc_id,
                &document.tenant,
                &document.ngsi_id,
                &payload,
            )
            .await
        } else {
            create_opaque_record(
                &self.client,
                ENTITY_COLLECTION,
                &document.tenant,
                &document.ngsi_id,
                &payload,
            )
            .await
        }
    }

    async fn delete(
        &self,
        tenant: &str,
        entity_id: &str,
    ) -> Result<Option<StoredDocument>, BrokerError> {
        let Some(row) =
            find_opaque_record(&self.client, ENTITY_COLLECTION, tenant, entity_id).await?
        else {
            return Ok(None);
        };
        delete_opaque_record(&self.client, ENTITY_COLLECTION, &row.doc_id).await?;
        Ok(Some(decode_entity_row(row)?))
    }

    async fn query(
        &self,
        tenant: &str,
        plan: &QueryPlan,
    ) -> Result<Vec<StoredDocument>, BrokerError> {
        let rows = list_opaque_records(&self.client, ENTITY_COLLECTION, tenant).await?;
        let documents = rows
            .into_iter()
            .map(decode_entity_row)
            .collect::<Result<Vec<_>, _>>()?;
        filter_entity_documents(documents, plan)
    }

    async fn list_tenants(&self) -> Result<Vec<String>, BrokerError> {
        list_distinct_tenants(&self.client, ENTITY_COLLECTION).await
    }
}

#[derive(Clone)]
/// DefraDB-backed temporal repository.
pub struct DefraDbTemporalRepository {
    client: Arc<DefraDbClient>,
}

impl DefraDbTemporalRepository {
    fn new(client: Arc<DefraDbClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl TemporalRepository for DefraDbTemporalRepository {
    async fn get(
        &self,
        tenant: &str,
        entity_id: &str,
    ) -> Result<Option<TemporalEntityDocument>, BrokerError> {
        let row = find_temporal_record(&self.client, tenant, entity_id).await?;
        row.map(decode_temporal_row).transpose()
    }

    async fn upsert(&self, document: TemporalEntityDocument) -> Result<bool, BrokerError> {
        let payload = encode_json(&document.doc, "temporal payload")?;
        let history_json = encode_json(&document.history, "temporal history")?;
        if let Some(row) =
            find_temporal_record(&self.client, &document.tenant, &document.ngsi_id).await?
        {
            update_temporal_record(
                &self.client,
                &row.doc_id,
                &document.tenant,
                &document.ngsi_id,
                &payload,
                &history_json,
            )
            .await?;
            Ok(false)
        } else {
            create_temporal_record(
                &self.client,
                &document.tenant,
                &document.ngsi_id,
                &payload,
                &history_json,
            )
            .await?;
            Ok(true)
        }
    }

    async fn delete(&self, tenant: &str, entity_id: &str) -> Result<bool, BrokerError> {
        let Some(row) = find_temporal_record(&self.client, tenant, entity_id).await? else {
            return Ok(false);
        };
        delete_temporal_record(&self.client, &row.doc_id).await?;
        Ok(true)
    }

    async fn query(
        &self,
        tenant: &str,
        plan: &QueryPlan,
        _temporal: &TemporalFilter,
        _geo: Option<&GeoFilter>,
    ) -> Result<Vec<TemporalEntityDocument>, BrokerError> {
        let rows = list_temporal_records(&self.client, tenant).await?;
        let documents = rows
            .into_iter()
            .map(decode_temporal_row)
            .collect::<Result<Vec<_>, _>>()?;
        filter_temporal_documents(documents, plan)
    }
}

#[derive(Clone)]
/// DefraDB-backed subscription repository.
pub struct DefraDbSubscriptionRepository {
    client: Arc<DefraDbClient>,
}

impl DefraDbSubscriptionRepository {
    fn new(client: Arc<DefraDbClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl SubscriptionRepository for DefraDbSubscriptionRepository {
    async fn get(
        &self,
        tenant: &str,
        subscription_id: &str,
    ) -> Result<Option<SubscriptionDocument>, BrokerError> {
        let row = find_opaque_record(
            &self.client,
            SUBSCRIPTION_COLLECTION,
            tenant,
            subscription_id,
        )
        .await?;
        row.map(decode_subscription_row).transpose()
    }

    async fn insert(&self, document: SubscriptionDocument) -> Result<(), BrokerError> {
        if self
            .get(&document.tenant, &document.ngsi_id)
            .await?
            .is_some()
        {
            return Err(BrokerError::Conflict(format!(
                "subscription {} already exists",
                document.ngsi_id
            )));
        }
        create_opaque_record(
            &self.client,
            SUBSCRIPTION_COLLECTION,
            &document.tenant,
            &document.ngsi_id,
            &encode_json(&document.doc, "subscription payload")?,
        )
        .await
    }

    async fn replace(&self, document: SubscriptionDocument) -> Result<(), BrokerError> {
        let payload = encode_json(&document.doc, "subscription payload")?;
        if let Some(row) = find_opaque_record(
            &self.client,
            SUBSCRIPTION_COLLECTION,
            &document.tenant,
            &document.ngsi_id,
        )
        .await?
        {
            update_opaque_record(
                &self.client,
                SUBSCRIPTION_COLLECTION,
                &row.doc_id,
                &document.tenant,
                &document.ngsi_id,
                &payload,
            )
            .await
        } else {
            create_opaque_record(
                &self.client,
                SUBSCRIPTION_COLLECTION,
                &document.tenant,
                &document.ngsi_id,
                &payload,
            )
            .await
        }
    }

    async fn delete(
        &self,
        tenant: &str,
        subscription_id: &str,
    ) -> Result<Option<SubscriptionDocument>, BrokerError> {
        let Some(row) = find_opaque_record(
            &self.client,
            SUBSCRIPTION_COLLECTION,
            tenant,
            subscription_id,
        )
        .await?
        else {
            return Ok(None);
        };
        delete_opaque_record(&self.client, SUBSCRIPTION_COLLECTION, &row.doc_id).await?;
        Ok(Some(decode_subscription_row(row)?))
    }

    async fn list(
        &self,
        tenant: &str,
        limit: Option<usize>,
    ) -> Result<Vec<SubscriptionDocument>, BrokerError> {
        let mut documents = list_opaque_records(&self.client, SUBSCRIPTION_COLLECTION, tenant)
            .await?
            .into_iter()
            .map(decode_subscription_row)
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(limit) = limit {
            documents.truncate(limit);
        }
        Ok(documents)
    }

    async fn list_tenants(&self) -> Result<Vec<String>, BrokerError> {
        list_distinct_tenants(&self.client, SUBSCRIPTION_COLLECTION).await
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
        let Some(mut document) = self.get(tenant, subscription_id).await? else {
            return Ok(());
        };

        update_delivery_fields(&mut document.doc, successes, failures, now);
        self.replace(document).await
    }
}

#[derive(Clone)]
/// DefraDB-backed replicated entity mutation-log repository.
pub struct DefraDbEntityMutationRepository {
    client: Arc<DefraDbClient>,
}

impl DefraDbEntityMutationRepository {
    fn new(client: Arc<DefraDbClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl EntityMutationRepository for DefraDbEntityMutationRepository {
    async fn insert(&self, document: EntityMutationDocument) -> Result<(), BrokerError> {
        let _: Value = self
            .client
            .execute(
                &format!(
                    "mutation($tenant: String!, $eventId: String!, $entityId: String!, $operation: String!, $payload: String!, $changedAttributesJson: String!, $originBrokerId: String!, $createdAtMillis: Float64!) {{ add_{ENTITY_MUTATION_COLLECTION}(input: [{{tenant: $tenant, eventId: $eventId, entityId: $entityId, operation: $operation, payload: $payload, changedAttributesJson: $changedAttributesJson, originBrokerId: $originBrokerId, createdAtMillis: $createdAtMillis}}]) {{ _docID }} }}"
                ),
                json!({
                    "tenant": document.tenant,
                    "eventId": document.event_id,
                    "entityId": document.entity_id,
                    "operation": document.operation.as_str(),
                    "payload": encode_json(&document.payload, "entity mutation payload")?,
                    "changedAttributesJson": encode_json(
                        &document.changed_attributes,
                        "entity mutation changed attributes",
                    )?,
                    "originBrokerId": document.origin_broker_id,
                    "createdAtMillis": document.created_at_millis as f64,
                }),
            )
            .await?;
        Ok(())
    }

    async fn insert_many(&self, documents: Vec<EntityMutationDocument>) -> Result<(), BrokerError> {
        if documents.is_empty() {
            return Ok(());
        }

        let inputs = documents
            .iter()
            .map(|document| {
                Ok(json!({
                    "tenant": document.tenant,
                    "eventId": document.event_id,
                    "entityId": document.entity_id,
                    "operation": document.operation.as_str(),
                    "payload": encode_json(&document.payload, "entity mutation payload")?,
                    "changedAttributesJson": encode_json(
                        &document.changed_attributes,
                        "entity mutation changed attributes",
                    )?,
                    "originBrokerId": document.origin_broker_id,
                    "createdAtMillis": document.created_at_millis as f64,
                }))
            })
            .collect::<Result<Vec<_>, BrokerError>>()?;
        let query = format!(
            "mutation($input: [EntityMutationRecordMutationInputArg!]!) {{ add_{ENTITY_MUTATION_COLLECTION}(input: $input) {{ _docID }} }}"
        );
        for chunk in inputs.chunks(BULK_INSERT_CHUNK_SIZE) {
            let _: Value = self.client.execute(&query, json!({"input": chunk})).await?;
        }
        Ok(())
    }

    async fn list_after(
        &self,
        created_after_millis: i64,
    ) -> Result<Vec<EntityMutationDocument>, BrokerError> {
        let data: EntityMutationRecordListData = self
            .client
            .execute(
                &format!(
                    "query($createdAtMillis: Float64!) {{ {ENTITY_MUTATION_COLLECTION}(filter: {{createdAtMillis: {{_geq: $createdAtMillis}}}}) {{ _docID tenant eventId entityId operation payload changedAttributesJson originBrokerId createdAtMillis }} }}"
                ),
                json!({"createdAtMillis": created_after_millis as f64}),
            )
            .await?;
        let mut documents = data
            .records
            .into_iter()
            .filter(|row| (row.created_at_millis as i64) >= created_after_millis)
            .map(decode_entity_mutation_row)
            .collect::<Result<Vec<_>, _>>()?;
        documents.sort_by(|left, right| {
            left.created_at_millis
                .cmp(&right.created_at_millis)
                .then_with(|| left.event_id.cmp(&right.event_id))
        });
        Ok(documents)
    }
}

/// Finds one opaque record by tenant and logical id.
async fn find_opaque_record(
    client: &DefraDbClient,
    collection: &str,
    tenant: &str,
    ngsi_id: &str,
) -> Result<Option<OpaqueRecordRow>, BrokerError> {
    let query = format!(
        "query($tenant: String!, $ngsiId: String!) {{ {collection}(filter: {{tenant: {{_eq: $tenant}}, ngsiId: {{_eq: $ngsiId}}}}) {{ _docID tenant ngsiId payload }} }}"
    );
    let data: OpaqueRecordListData = client
        .execute(&query, json!({"tenant": tenant, "ngsiId": ngsi_id}))
        .await?;
    Ok(data.records.into_iter().next())
}

/// Lists opaque records for one tenant.
async fn list_opaque_records(
    client: &DefraDbClient,
    collection: &str,
    tenant: &str,
) -> Result<Vec<OpaqueRecordRow>, BrokerError> {
    let query = format!(
        "query($tenant: String!) {{ {collection}(filter: {{tenant: {{_eq: $tenant}}}}) {{ _docID tenant ngsiId payload }} }}"
    );
    let data: OpaqueRecordListData = client.execute(&query, json!({"tenant": tenant})).await?;
    Ok(data.records)
}

/// Lists distinct tenant values present in one collection.
async fn list_distinct_tenants(
    client: &DefraDbClient,
    collection: &str,
) -> Result<Vec<String>, BrokerError> {
    let query = format!("query {{ {collection} {{ tenant }} }}");
    let data: TenantListData = client.execute(&query, json!({})).await?;
    let mut tenants = data
        .records
        .into_iter()
        .map(|record| record.tenant)
        .collect::<Vec<_>>();
    tenants.sort();
    tenants.dedup();
    Ok(tenants)
}

/// Creates one opaque record row in target collection.
async fn create_opaque_record(
    client: &DefraDbClient,
    collection: &str,
    tenant: &str,
    ngsi_id: &str,
    payload: &str,
) -> Result<(), BrokerError> {
    let query = format!(
        "mutation($tenant: String!, $ngsiId: String!, $payload: String!) {{ add_{collection}(input: [{{tenant: $tenant, ngsiId: $ngsiId, payload: $payload}}]) {{ _docID }} }}"
    );
    let _: Value = client
        .execute(
            &query,
            json!({"tenant": tenant, "ngsiId": ngsi_id, "payload": payload}),
        )
        .await?;
    Ok(())
}

/// Creates opaque record rows in one storage request.
async fn create_opaque_records(
    client: &DefraDbClient,
    collection: &str,
    inputs: Vec<Value>,
) -> Result<(), BrokerError> {
    let query = format!(
        "mutation($input: [{collection}MutationInputArg!]!) {{ add_{collection}(input: $input) {{ _docID }} }}"
    );
    for chunk in inputs.chunks(BULK_INSERT_CHUNK_SIZE) {
        let _: Value = client.execute(&query, json!({"input": chunk})).await?;
    }
    Ok(())
}

/// Updates one opaque record row in target collection.
async fn update_opaque_record(
    client: &DefraDbClient,
    collection: &str,
    doc_id: &str,
    tenant: &str,
    ngsi_id: &str,
    payload: &str,
) -> Result<(), BrokerError> {
    let query = format!(
        "mutation($docID: [ID!], $tenant: String!, $ngsiId: String!, $payload: String!) {{ update_{collection}(docID: $docID, input: {{tenant: $tenant, ngsiId: $ngsiId, payload: $payload}}) {{ _docID }} }}"
    );
    let _: Value = client
        .execute(
            &query,
            json!({"docID": [doc_id], "tenant": tenant, "ngsiId": ngsi_id, "payload": payload}),
        )
        .await?;
    Ok(())
}

/// Deletes one opaque record row by internal document id.
async fn delete_opaque_record(
    client: &DefraDbClient,
    collection: &str,
    doc_id: &str,
) -> Result<(), BrokerError> {
    let query =
        format!("mutation($docID: [ID!]) {{ delete_{collection}(docID: $docID) {{ _docID }} }}");
    let _: Value = client.execute(&query, json!({"docID": [doc_id]})).await?;
    Ok(())
}

/// Finds one temporal record by tenant and logical id.
async fn find_temporal_record(
    client: &DefraDbClient,
    tenant: &str,
    ngsi_id: &str,
) -> Result<Option<TemporalRecordRow>, BrokerError> {
    let data: TemporalRecordListData = client
        .execute(
            &format!(
                "query($tenant: String!, $ngsiId: String!) {{ {TEMPORAL_COLLECTION}(filter: {{tenant: {{_eq: $tenant}}, ngsiId: {{_eq: $ngsiId}}}}) {{ _docID tenant ngsiId payload historyJson }} }}"
            ),
            json!({"tenant": tenant, "ngsiId": ngsi_id}),
        )
        .await?;
    Ok(data.records.into_iter().next())
}

/// Lists temporal records for one tenant.
async fn list_temporal_records(
    client: &DefraDbClient,
    tenant: &str,
) -> Result<Vec<TemporalRecordRow>, BrokerError> {
    let data: TemporalRecordListData = client
        .execute(
            &format!(
                "query($tenant: String!) {{ {TEMPORAL_COLLECTION}(filter: {{tenant: {{_eq: $tenant}}}}) {{ _docID tenant ngsiId payload historyJson }} }}"
            ),
            json!({"tenant": tenant}),
        )
        .await?;
    Ok(data.records)
}

/// Creates one temporal record row.
async fn create_temporal_record(
    client: &DefraDbClient,
    tenant: &str,
    ngsi_id: &str,
    payload: &str,
    history_json: &str,
) -> Result<(), BrokerError> {
    let _: Value = client
        .execute(
            &format!(
                "mutation($tenant: String!, $ngsiId: String!, $payload: String!, $historyJson: String!) {{ add_{TEMPORAL_COLLECTION}(input: [{{tenant: $tenant, ngsiId: $ngsiId, payload: $payload, historyJson: $historyJson}}]) {{ _docID }} }}"
            ),
            json!({
                "tenant": tenant,
                "ngsiId": ngsi_id,
                "payload": payload,
                "historyJson": history_json,
            }),
        )
        .await?;
    Ok(())
}

/// Updates one temporal record row.
async fn update_temporal_record(
    client: &DefraDbClient,
    doc_id: &str,
    tenant: &str,
    ngsi_id: &str,
    payload: &str,
    history_json: &str,
) -> Result<(), BrokerError> {
    let _: Value = client
        .execute(
            &format!(
                "mutation($docID: [ID!], $tenant: String!, $ngsiId: String!, $payload: String!, $historyJson: String!) {{ update_{TEMPORAL_COLLECTION}(docID: $docID, input: {{tenant: $tenant, ngsiId: $ngsiId, payload: $payload, historyJson: $historyJson}}) {{ _docID }} }}"
            ),
            json!({
                "docID": [doc_id],
                "tenant": tenant,
                "ngsiId": ngsi_id,
                "payload": payload,
                "historyJson": history_json,
            }),
        )
        .await?;
    Ok(())
}

/// Deletes one temporal record row by internal document id.
async fn delete_temporal_record(client: &DefraDbClient, doc_id: &str) -> Result<(), BrokerError> {
    let _: Value = client
        .execute(
            &format!(
                "mutation($docID: [ID!]) {{ delete_{TEMPORAL_COLLECTION}(docID: $docID) {{ _docID }} }}"
            ),
            json!({"docID": [doc_id]}),
        )
        .await?;
    Ok(())
}

#[derive(Debug, Deserialize)]
struct OpaqueRecordListData {
    #[serde(rename = "EntityRecord", alias = "SubscriptionRecord")]
    records: Vec<OpaqueRecordRow>,
}

#[derive(Debug, Deserialize)]
struct TenantRow {
    tenant: String,
}

#[derive(Debug, Deserialize)]
struct TenantListData {
    #[serde(rename = "EntityRecord", alias = "SubscriptionRecord")]
    records: Vec<TenantRow>,
}

#[derive(Debug, Deserialize)]
struct TemporalRecordListData {
    #[serde(rename = "TemporalRecord")]
    records: Vec<TemporalRecordRow>,
}

#[derive(Debug, Deserialize)]
struct EntityMutationRecordListData {
    #[serde(rename = "EntityMutationRecord")]
    records: Vec<EntityMutationRecordRow>,
}

/// Decodes entity wrapper from opaque storage row.
fn decode_entity_row(row: OpaqueRecordRow) -> Result<StoredDocument, BrokerError> {
    Ok(StoredDocument {
        tenant: row.tenant,
        ngsi_id: row.ngsi_id,
        doc: decode_json(&row.payload, "entity payload")?,
    })
}

/// Decodes subscription wrapper from opaque storage row.
fn decode_subscription_row(row: OpaqueRecordRow) -> Result<SubscriptionDocument, BrokerError> {
    Ok(SubscriptionDocument {
        tenant: row.tenant,
        ngsi_id: row.ngsi_id,
        doc: decode_json(&row.payload, "subscription payload")?,
    })
}

/// Decodes temporal wrapper from storage row.
fn decode_temporal_row(row: TemporalRecordRow) -> Result<TemporalEntityDocument, BrokerError> {
    Ok(TemporalEntityDocument {
        tenant: row.tenant,
        ngsi_id: row.ngsi_id,
        doc: decode_json(&row.payload, "temporal payload")?,
        history: decode_json(&row.history_json, "temporal history")?,
    })
}

/// Decodes replicated entity mutation from storage row.
fn decode_entity_mutation_row(
    row: EntityMutationRecordRow,
) -> Result<EntityMutationDocument, BrokerError> {
    let operation = EntityMutationOperation::from_str(&row.operation).ok_or_else(|| {
        BrokerError::internal(format!(
            "unknown entity mutation operation {} in {}",
            row.operation, row.doc_id
        ))
    })?;
    let created_at_millis = row.created_at_millis as i64;
    Ok(EntityMutationDocument {
        tenant: row.tenant,
        event_id: row.event_id,
        entity_id: row.entity_id,
        operation,
        payload: decode_json(&row.payload, "entity mutation payload")?,
        changed_attributes: decode_json(
            &row.changed_attributes_json,
            "entity mutation changed attributes",
        )?,
        origin_broker_id: row.origin_broker_id,
        created_at_millis,
    })
}

/// Serializes payload into string field stored in DefraDB.
fn encode_json<T: Serialize>(value: &T, label: &str) -> Result<String, BrokerError> {
    serde_json::to_string(value)
        .map_err(|error| BrokerError::internal(format!("failed encoding {label}: {error}")))
}

/// Deserializes payload stored as string field in DefraDB.
fn decode_json<T: DeserializeOwned>(value: &str, label: &str) -> Result<T, BrokerError> {
    serde_json::from_str(value)
        .map_err(|error| BrokerError::internal(format!("failed decoding {label}: {error}")))
}
