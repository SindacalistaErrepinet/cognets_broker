use std::{sync::Mutex, time::Duration};

use async_trait::async_trait;
use log::{error, warn};
use redis::{RedisError, streams::StreamReadReply};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::time::sleep;
use url::Url;

use crate::{
    config::AppConfig, federation::p2p::SwimEventEnvelope, persistence::mongo::MongoRepositories,
    persistence::repository::SubscriptionRepository, utils::time::now_timestamp,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum NotificationTargetKind {
    Subscription,
}

/// Returns default notification target kind for deserialization.
fn default_notification_target_kind() -> NotificationTargetKind {
    NotificationTargetKind::Subscription
}

#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    #[error("redis error: {0}")]
    Redis(#[from] RedisError),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("http client error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("queue error: {0}")]
    Other(String),
}

#[async_trait]
pub trait EventQueue: Send + Sync {
    /// Enqueues message for asynchronous delivery.
    async fn enqueue(&self, message: QueueMessage) -> Result<(), QueueError>;
}

/// Wraps outbound SWIM HTTP delivery details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwimEnvelope {
    pub tenant: String,
    pub endpoint: String,
    pub payload: SwimEventEnvelope,
}

/// Wraps outbound NGSI-LD notification delivery details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationEnvelope {
    pub tenant: String,
    #[serde(rename = "subscriptionId", alias = "targetId")]
    pub subscription_id: String,
    #[serde(rename = "targetKind", default = "default_notification_target_kind")]
    pub target_kind: NotificationTargetKind,
    pub endpoint: String,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QueueMessage {
    Notification(NotificationEnvelope),
    Swim(SwimEnvelope),
}

/// Redis-backed queue used for internal delivery workers.
#[derive(Clone)]
pub struct RedisEventQueue {
    client: redis::Client,
    stream: String,
}

impl RedisEventQueue {
    /// Creates Redis-backed event queue for given stream.
    pub fn new(redis_url: &str, stream: &str) -> Result<Self, QueueError> {
        Ok(Self {
            client: redis::Client::open(redis_url)?,
            stream: stream.to_string(),
        })
    }

    /// Starts background worker that drains queued messages.
    pub async fn start_worker(
        &self,
        repositories: MongoRepositories,
        config: AppConfig,
    ) -> Result<(), QueueError> {
        self.ensure_consumer_group(&config.redis_consumer_group)
            .await?;
        let queue = self.clone();

        tokio::spawn(async move {
            queue.worker_loop(repositories, config).await;
        });

        Ok(())
    }

    /// Ensures worker consumer group exists before polling.
    async fn ensure_consumer_group(&self, group: &str) -> Result<(), QueueError> {
        let mut connection = self.client.get_multiplexed_async_connection().await?;
        let result = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&self.stream)
            .arg(group)
            .arg("0")
            .arg("MKSTREAM")
            .query_async::<String>(&mut connection)
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(error) if error.code() == Some("BUSYGROUP") => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    /// Runs blocking worker loop over Redis stream messages.
    async fn worker_loop(&self, repositories: MongoRepositories, config: AppConfig) {
        let http_client = match Client::builder()
            .timeout(Duration::from_millis(config.outbound_timeout_ms))
            .build()
        {
            Ok(client) => client,
            Err(error) => {
                error!("failed to create outbound client for worker: {error}");
                return;
            }
        };

        loop {
            match self
                .read_pending_messages(&config.redis_consumer_group, &config.redis_consumer_name)
                .await
            {
                Ok(messages) => {
                    for (message_id, message) in messages {
                        if let Err(error) =
                            process_queue_message(&http_client, &repositories, &config, &message)
                                .await
                        {
                            warn!("failed to process queue message {message_id}: {error}");
                        }

                        if let Err(error) =
                            self.ack(&config.redis_consumer_group, &message_id).await
                        {
                            warn!("failed to ACK queue message {message_id}: {error}");
                        }
                    }
                }
                Err(error) => {
                    error!("worker failed reading Redis stream: {error}");
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    /// Reads next batch of queued messages from Redis.
    async fn read_pending_messages(
        &self,
        group: &str,
        consumer: &str,
    ) -> Result<Vec<(String, QueueMessage)>, QueueError> {
        let mut connection = self.client.get_multiplexed_async_connection().await?;
        let reply = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(group)
            .arg(consumer)
            .arg("COUNT")
            .arg(16)
            .arg("BLOCK")
            .arg(5000)
            .arg("STREAMS")
            .arg(&self.stream)
            .arg(">")
            .query_async::<StreamReadReply>(&mut connection)
            .await?;

        let mut messages = Vec::new();

        for stream_key in reply.keys {
            for stream_id in stream_key.ids {
                if let Some(payload) = stream_id.map.get("payload") {
                    let payload: String = redis::from_redis_value(payload)?;
                    messages.push((stream_id.id, serde_json::from_str(&payload)?));
                }
            }
        }

        Ok(messages)
    }

    /// ACKs processed message in Redis stream.
    async fn ack(&self, group: &str, message_id: &str) -> Result<(), QueueError> {
        let mut connection = self.client.get_multiplexed_async_connection().await?;
        let _ = redis::cmd("XACK")
            .arg(&self.stream)
            .arg(group)
            .arg(message_id)
            .query_async::<i64>(&mut connection)
            .await?;

        Ok(())
    }
}

#[async_trait]
impl EventQueue for RedisEventQueue {
    /// Enqueues message into Redis stream for worker delivery.
    async fn enqueue(&self, message: QueueMessage) -> Result<(), QueueError> {
        let payload = serde_json::to_string(&message)?;
        let mut connection = self.client.get_multiplexed_async_connection().await?;

        let _ = redis::cmd("XADD")
            .arg(&self.stream)
            .arg("*")
            .arg("payload")
            .arg(payload)
            .query_async::<String>(&mut connection)
            .await?;

        Ok(())
    }
}

#[derive(Default)]
pub struct InMemoryEventQueue {
    messages: Mutex<Vec<QueueMessage>>,
}

impl InMemoryEventQueue {
    /// Returns queued messages captured during tests.
    pub fn messages(&self) -> Vec<QueueMessage> {
        self.messages.lock().unwrap().clone()
    }
}

#[async_trait]
impl EventQueue for InMemoryEventQueue {
    /// Captures message in memory for tests.
    async fn enqueue(&self, message: QueueMessage) -> Result<(), QueueError> {
        self.messages.lock().unwrap().push(message);
        Ok(())
    }
}

/// Processes single queued message through HTTP or repository side effects.
async fn process_queue_message(
    http_client: &Client,
    repositories: &MongoRepositories,
    _config: &AppConfig,
    message: &QueueMessage,
) -> Result<(), QueueError> {
    match message {
        QueueMessage::Notification(job) => {
            let response = http_client
                .post(&job.endpoint)
                .json(&job.payload)
                .send()
                .await;
            let success = response
                .as_ref()
                .map(|resp| resp.status().is_success())
                .unwrap_or(false);
            match job.target_kind {
                NotificationTargetKind::Subscription => {
                    repositories
                        .subscriptions
                        .mark_delivery(&job.tenant, &job.subscription_id, success, &now_timestamp())
                        .await
                        .ok();
                }
            }
            Ok(())
        }
        QueueMessage::Swim(job) => {
            let target_url = build_target_url(&job.endpoint, "/internal/swim", None)?;
            let _ = http_client
                .post(target_url)
                .header(crate::context::headers::HEADER_TENANT, &job.tenant)
                .json(&job.payload)
                .send()
                .await?;
            Ok(())
        }
    }
}

/// Builds outbound target URL from broker base endpoint and resource path.
pub(crate) fn build_target_url(
    endpoint: &str,
    path: &str,
    query: Option<&str>,
) -> Result<String, QueueError> {
    let endpoint = if endpoint.ends_with('/') {
        endpoint.to_string()
    } else {
        format!("{endpoint}/")
    };

    let base_url = Url::parse(&endpoint)
        .map_err(|error| QueueError::Other(format!("invalid endpoint {endpoint}: {error}")))?;
    let base_path = base_url.path().trim_end_matches('/');
    let uses_ngsi_base = base_path.ends_with(crate::services::common::BASE_PATH);
    let mut join_base = base_url.clone();
    let relative_path = if uses_ngsi_base {
        if path.starts_with(crate::services::common::BASE_PATH) {
            path.strip_prefix(crate::services::common::BASE_PATH)
                .unwrap_or(path)
                .trim_start_matches('/')
        } else {
            let prefix = base_path
                .strip_suffix(crate::services::common::BASE_PATH)
                .unwrap_or(base_path)
                .trim_end_matches('/');
            join_base.set_path(&format!("{}/", prefix));
            join_base.set_query(None);
            path.trim_start_matches('/')
        }
    } else {
        path.trim_start_matches('/')
    };

    let mut url = join_base
        .join(relative_path)
        .map_err(|error| QueueError::Other(format!("invalid path {path}: {error}")))?;

    if let Some(query_string) = query {
        url.set_query(Some(query_string));
    }

    Ok(url.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_url_from_root_endpoint() {
        let url = build_target_url(
            "http://broker.example:1026",
            "/ngsi-ld/v1/entities",
            Some("limit=10"),
        )
        .unwrap();

        assert_eq!(
            url,
            "http://broker.example:1026/ngsi-ld/v1/entities?limit=10"
        );
    }

    #[test]
    fn avoids_duplicate_base_path_when_endpoint_already_contains_ngsi_base() {
        let url = build_target_url(
            "http://broker.example:1026/ngsi-ld/v1",
            "/ngsi-ld/v1/entities",
            None,
        )
        .unwrap();

        assert_eq!(url, "http://broker.example:1026/ngsi-ld/v1/entities");
    }

    #[test]
    fn preserves_custom_prefix_before_ngsi_base_path() {
        let url = build_target_url(
            "http://broker.example:1026/custom/ngsi-ld/v1",
            "/ngsi-ld/v1/entities/urn:ngsi-ld:Vehicle:1",
            None,
        )
        .unwrap();

        assert_eq!(
            url,
            "http://broker.example:1026/custom/ngsi-ld/v1/entities/urn:ngsi-ld:Vehicle:1"
        );
    }

    #[test]
    fn builds_root_internal_url_from_ngsi_base_endpoint() {
        let url = build_target_url(
            "http://broker.example:1026/ngsi-ld/v1",
            "/internal/swim",
            None,
        )
        .unwrap();

        assert_eq!(url, "http://broker.example:1026/internal/swim");
    }

    #[test]
    fn preserves_custom_prefix_for_root_internal_url() {
        let url = build_target_url(
            "http://broker.example:1026/custom/ngsi-ld/v1",
            "/internal/swim",
            Some("limit=200"),
        )
        .unwrap();

        assert_eq!(
            url,
            "http://broker.example:1026/custom/internal/swim?limit=200"
        );
    }
}
