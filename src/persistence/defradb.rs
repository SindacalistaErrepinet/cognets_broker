use std::sync::Arc;

use async_trait::async_trait;
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    config::AppConfig,
    domain::types::{StoredDocument, SubscriptionDocument, TemporalEntityDocument},
    error::BrokerError,
    persistence::repository::{
        EntityRepository, Repositories, SubscriptionRepository, TemporalRepository,
        filter_entity_documents, filter_temporal_documents, update_status_field,
    },
    query::planner::{GeoFilter, MongoQueryPlan, TemporalFilter},
};

const ENTITY_COLLECTION: &str = "EntityRecord";
const TEMPORAL_COLLECTION: &str = "TemporalRecord";
const SUBSCRIPTION_COLLECTION: &str = "SubscriptionRecord";

/// Builds DefraDB-backed repositories over the GraphQL API.
pub struct DefraDbRepositories;

impl DefraDbRepositories {
    /// Creates repository set using configured DefraDB GraphQL endpoint.
    pub fn new(config: &AppConfig) -> Result<Repositories, BrokerError> {
        let client = Arc::new(DefraDbClient::new(
            &config.defradb_url,
            config.outbound_timeout_ms,
        )?);
        Ok(Repositories::new(
            Arc::new(DefraDbEntityRepository::new(client.clone())),
            Arc::new(DefraDbTemporalRepository::new(client.clone())),
            Arc::new(DefraDbSubscriptionRepository::new(client)),
        ))
    }
}

#[derive(Clone)]
struct DefraDbClient {
    http: Client,
    graphql_url: String,
}

impl DefraDbClient {
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

    async fn execute<T: DeserializeOwned>(
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

#[derive(Debug, Deserialize)]
struct GraphQlResponse<T> {
    data: Option<T>,
    errors: Option<Vec<GraphQlError>>,
}

#[derive(Debug, Deserialize)]
struct GraphQlError {
    message: String,
}

#[derive(Debug, Deserialize, Clone)]
struct OpaqueRecordRow {
    #[serde(rename = "_docID")]
    doc_id: String,
    tenant: String,
    #[serde(rename = "ngsiId")]
    ngsi_id: String,
    payload: String,
}

#[derive(Debug, Deserialize, Clone)]
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

#[derive(Clone)]
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
        plan: &MongoQueryPlan,
    ) -> Result<Vec<StoredDocument>, BrokerError> {
        let rows = list_opaque_records(&self.client, ENTITY_COLLECTION, tenant).await?;
        let documents = rows
            .into_iter()
            .map(decode_entity_row)
            .collect::<Result<Vec<_>, _>>()?;
        filter_entity_documents(documents, plan)
    }
}

#[derive(Clone)]
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
        plan: &MongoQueryPlan,
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

    async fn mark_delivery(
        &self,
        tenant: &str,
        subscription_id: &str,
        success: bool,
        now: &str,
    ) -> Result<(), BrokerError> {
        let Some(mut document) = self.get(tenant, subscription_id).await? else {
            return Ok(());
        };

        if let Some(notification) = document
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
            &mut document.doc,
            "modifiedAt",
            Value::String(now.to_string()),
        );
        self.replace(document).await
    }
}

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

async fn create_opaque_record(
    client: &DefraDbClient,
    collection: &str,
    tenant: &str,
    ngsi_id: &str,
    payload: &str,
) -> Result<(), BrokerError> {
    let query = format!(
        "mutation($tenant: String!, $ngsiId: String!, $payload: String!) {{ create_{collection}(input: {{tenant: $tenant, ngsiId: $ngsiId, payload: $payload}}) {{ _docID }} }}"
    );
    let _: Value = client
        .execute(
            &query,
            json!({"tenant": tenant, "ngsiId": ngsi_id, "payload": payload}),
        )
        .await?;
    Ok(())
}

async fn update_opaque_record(
    client: &DefraDbClient,
    collection: &str,
    doc_id: &str,
    tenant: &str,
    ngsi_id: &str,
    payload: &str,
) -> Result<(), BrokerError> {
    let query = format!(
        "mutation($docID: ID!, $tenant: String!, $ngsiId: String!, $payload: String!) {{ update_{collection}(docID: $docID, input: {{tenant: $tenant, ngsiId: $ngsiId, payload: $payload}}) {{ _docID }} }}"
    );
    let _: Value = client
        .execute(
            &query,
            json!({"docID": doc_id, "tenant": tenant, "ngsiId": ngsi_id, "payload": payload}),
        )
        .await?;
    Ok(())
}

async fn delete_opaque_record(
    client: &DefraDbClient,
    collection: &str,
    doc_id: &str,
) -> Result<(), BrokerError> {
    let query =
        format!("mutation($docID: ID!) {{ delete_{collection}(docID: $docID) {{ _docID }} }}");
    let _: Value = client.execute(&query, json!({"docID": doc_id})).await?;
    Ok(())
}

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
                "mutation($tenant: String!, $ngsiId: String!, $payload: String!, $historyJson: String!) {{ create_{TEMPORAL_COLLECTION}(input: {{tenant: $tenant, ngsiId: $ngsiId, payload: $payload, historyJson: $historyJson}}) {{ _docID }} }}"
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
                "mutation($docID: ID!, $tenant: String!, $ngsiId: String!, $payload: String!, $historyJson: String!) {{ update_{TEMPORAL_COLLECTION}(docID: $docID, input: {{tenant: $tenant, ngsiId: $ngsiId, payload: $payload, historyJson: $historyJson}}) {{ _docID }} }}"
            ),
            json!({
                "docID": doc_id,
                "tenant": tenant,
                "ngsiId": ngsi_id,
                "payload": payload,
                "historyJson": history_json,
            }),
        )
        .await?;
    Ok(())
}

async fn delete_temporal_record(client: &DefraDbClient, doc_id: &str) -> Result<(), BrokerError> {
    let _: Value = client
        .execute(
            &format!(
                "mutation($docID: ID!) {{ delete_{TEMPORAL_COLLECTION}(docID: $docID) {{ _docID }} }}"
            ),
            json!({"docID": doc_id}),
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
struct TemporalRecordListData {
    #[serde(rename = "TemporalRecord")]
    records: Vec<TemporalRecordRow>,
}

fn decode_entity_row(row: OpaqueRecordRow) -> Result<StoredDocument, BrokerError> {
    Ok(StoredDocument {
        tenant: row.tenant,
        ngsi_id: row.ngsi_id,
        doc: decode_json(&row.payload, "entity payload")?,
    })
}

fn decode_subscription_row(row: OpaqueRecordRow) -> Result<SubscriptionDocument, BrokerError> {
    Ok(SubscriptionDocument {
        tenant: row.tenant,
        ngsi_id: row.ngsi_id,
        doc: decode_json(&row.payload, "subscription payload")?,
    })
}

fn decode_temporal_row(row: TemporalRecordRow) -> Result<TemporalEntityDocument, BrokerError> {
    Ok(TemporalEntityDocument {
        tenant: row.tenant,
        ngsi_id: row.ngsi_id,
        doc: decode_json(&row.payload, "temporal payload")?,
        history: decode_json(&row.history_json, "temporal history")?,
    })
}

fn encode_json<T: Serialize>(value: &T, label: &str) -> Result<String, BrokerError> {
    serde_json::to_string(value)
        .map_err(|error| BrokerError::internal(format!("failed encoding {label}: {error}")))
}

fn decode_json<T: DeserializeOwned>(value: &str, label: &str) -> Result<T, BrokerError> {
    serde_json::from_str(value)
        .map_err(|error| BrokerError::internal(format!("failed decoding {label}: {error}")))
}
