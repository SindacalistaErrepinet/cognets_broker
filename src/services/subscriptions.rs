//! Subscription CRUD services.
use serde_json::Value;

use crate::{
    app::state::AppState,
    context::headers::RequestContext,
    domain::types::SubscriptionDocument,
    error::BrokerError,
    query::types::{QueryResult, SubscriptionQuery},
    services::common::prepare_subscription,
    utils::{json::apply_merge_patch, time::now_timestamp},
};

/// Creates and stores subscription document for tenant.
pub async fn create(
    state: &AppState,
    context: &RequestContext,
    mut subscription: Value,
) -> Result<String, BrokerError> {
    let subscription_id = prepare_subscription(&mut subscription, context)?;
    state
        .repositories
        .subscriptions
        .insert(SubscriptionDocument {
            tenant: context.tenant.clone(),
            ngsi_id: subscription_id.clone(),
            doc: subscription,
        })
        .await?;
    Ok(subscription_id)
}

/// Lists subscriptions stored for tenant.
pub async fn list(
    state: &AppState,
    context: &RequestContext,
    query: &SubscriptionQuery,
) -> Result<QueryResult, BrokerError> {
    let items = state
        .repositories
        .subscriptions
        .list(&context.tenant, query.limit)
        .await?;

    let total_count = items.len();
    Ok(QueryResult {
        body: Value::Array(items.into_iter().map(|item| item.doc).collect()),
        total_count,
    })
}

/// Retrieves subscription payload by id.
pub async fn get(
    state: &AppState,
    context: &RequestContext,
    subscription_id: &str,
) -> Result<Value, BrokerError> {
    state
        .repositories
        .subscriptions
        .get(&context.tenant, subscription_id)
        .await?
        .map(|document| document.doc)
        .ok_or_else(|| {
            BrokerError::NotFound(format!("subscription {subscription_id} was not found"))
        })
}

/// Applies merge patch to stored subscription.
pub async fn patch(
    state: &AppState,
    context: &RequestContext,
    subscription_id: &str,
    patch: Value,
) -> Result<(), BrokerError> {
    let mut existing = state
        .repositories
        .subscriptions
        .get(&context.tenant, subscription_id)
        .await?
        .ok_or_else(|| {
            BrokerError::NotFound(format!("subscription {subscription_id} was not found"))
        })?;

    apply_merge_patch(&mut existing.doc, &patch);
    if let Some(object) = existing.doc.as_object_mut() {
        object.insert("id".to_string(), Value::String(subscription_id.to_string()));
        object.insert("modifiedAt".to_string(), Value::String(now_timestamp()));
    }

    state.repositories.subscriptions.replace(existing).await
}

/// Deletes subscription by id.
pub async fn delete(
    state: &AppState,
    context: &RequestContext,
    subscription_id: &str,
) -> Result<(), BrokerError> {
    state
        .repositories
        .subscriptions
        .delete(&context.tenant, subscription_id)
        .await?
        .ok_or_else(|| {
            BrokerError::NotFound(format!("subscription {subscription_id} was not found"))
        })?;
    Ok(())
}
