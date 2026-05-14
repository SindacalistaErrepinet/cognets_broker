use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use mongodb::Database;
use reqwest::Client;

use crate::{
    config::AppConfig, error::BrokerError, federation::queue::EventQueue,
    persistence::mongo::MongoRepositories,
};

#[derive(Clone)]
pub struct AppState {
    pub config: AppConfig,
    pub db: Database,
    pub repositories: MongoRepositories,
    pub queue: Arc<dyn EventQueue>,
    pub http_client: Client,
    pub started_at: Instant,
}

impl AppState {
    /// Creates shared application state and outbound HTTP client.
    pub fn new(
        config: AppConfig,
        db: Database,
        repositories: MongoRepositories,
        queue: Arc<dyn EventQueue>,
    ) -> Result<Self, BrokerError> {
        let http_client = Client::builder()
            .timeout(Duration::from_millis(config.outbound_timeout_ms))
            .build()
            .map_err(|error| {
                BrokerError::internal(format!("failed to create HTTP client: {error}"))
            })?;

        Ok(Self {
            config,
            db,
            repositories,
            queue,
            http_client,
            started_at: Instant::now(),
        })
    }
}
