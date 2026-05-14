use std::time::{Duration, Instant};

use reqwest::Client;

use crate::{
    app::entity_watch::EntityWatchState, config::AppConfig, error::BrokerError,
    persistence::repository::Repositories,
};

#[derive(Clone)]
pub struct AppState {
    pub config: AppConfig,
    pub repositories: Repositories,
    pub http_client: Client,
    pub started_at: Instant,
    pub entity_watch: EntityWatchState,
}

impl AppState {
    /// Creates shared application state and outbound HTTP client.
    pub fn new(config: AppConfig, repositories: Repositories) -> Result<Self, BrokerError> {
        let http_client = Client::builder()
            .timeout(Duration::from_millis(config.outbound_timeout_ms))
            .build()
            .map_err(|error| {
                BrokerError::internal(format!("failed to create HTTP client: {error}"))
            })?;

        Ok(Self {
            config,
            repositories,
            http_client,
            started_at: Instant::now(),
            entity_watch: EntityWatchState::default(),
        })
    }
}
