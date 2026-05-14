use std::env;

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub host: String,
    pub port: u16,
    pub broker_id: String,
    pub public_endpoint: String,
    pub mongo_url: String,
    pub mongo_database: String,
    pub redis_url: String,
    pub redis_stream: String,
    pub redis_consumer_group: String,
    pub redis_consumer_name: String,
    pub outbound_timeout_ms: u64,
    pub p2p_enabled: bool,
    pub p2p_sync_interval_ms: u64,
    pub p2p_swim_suspect_timeout_ms: u64,
    pub p2p_seeds: Vec<String>,
}

impl AppConfig {
    /// Builds runtime configuration from process environment.
    pub fn from_env() -> Self {
        let host = env_or("BROKER_HOST", "127.0.0.1");
        let port = env_or("BROKER_PORT", "8080").parse().unwrap_or(8080);
        let broker_id = env_or("BROKER_ID", "cognets-broker");
        let public_endpoint = env_or(
            "BROKER_PUBLIC_ENDPOINT",
            &format!("http://{host}:{port}/ngsi-ld/v1"),
        );

        Self {
            host,
            port,
            broker_id: broker_id.clone(),
            public_endpoint,
            mongo_url: env_or("BROKER_MONGO_URL", "mongodb://127.0.0.1:27017"),
            mongo_database: env_or("BROKER_MONGO_DATABASE", "cognets_broker"),
            redis_url: env_or("BROKER_REDIS_URL", "redis://127.0.0.1/"),
            redis_stream: env_or("BROKER_REDIS_STREAM", "ngsild:internal"),
            redis_consumer_group: env_or("BROKER_REDIS_CONSUMER_GROUP", "ngsild-brokers"),
            redis_consumer_name: env_or("BROKER_REDIS_CONSUMER_NAME", &broker_id),
            outbound_timeout_ms: env_or("BROKER_OUTBOUND_TIMEOUT_MS", "5000")
                .parse()
                .unwrap_or(5000),
            p2p_enabled: env_or("BROKER_P2P_ENABLED", "true").parse().unwrap_or(true),
            p2p_sync_interval_ms: env_or("BROKER_P2P_SYNC_INTERVAL_MS", "10000")
                .parse()
                .unwrap_or(10000),
            p2p_swim_suspect_timeout_ms: env_or("BROKER_P2P_SWIM_SUSPECT_TIMEOUT_MS", "15000")
                .parse()
                .unwrap_or(15000),
            p2p_seeds: split_csv(&env_or("BROKER_P2P_SEEDS", "")),
        }
    }

    #[cfg(test)]
    /// Builds isolated configuration defaults for tests.
    pub fn for_tests() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8080,
            broker_id: "test-broker".to_string(),
            public_endpoint: "http://127.0.0.1:8080/ngsi-ld/v1".to_string(),
            mongo_url: "mongodb://127.0.0.1:27017".to_string(),
            mongo_database: format!("cognets_broker_test_{}", uuid::Uuid::new_v4()),
            redis_url: "redis://127.0.0.1/".to_string(),
            redis_stream: "ngsild:test-stream".to_string(),
            redis_consumer_group: "ngsild-test-group".to_string(),
            redis_consumer_name: "test-consumer".to_string(),
            outbound_timeout_ms: 2000,
            p2p_enabled: true,
            p2p_sync_interval_ms: 1000,
            p2p_swim_suspect_timeout_ms: 1000,
            p2p_seeds: Vec::new(),
        }
    }
}

/// Reads environment variable or falls back to default value.
fn env_or(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

/// Splits comma-separated configuration values into trimmed entries.
fn split_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(ToString::to_string)
        .collect()
}
