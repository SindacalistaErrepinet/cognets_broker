use std::env;

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub host: String,
    pub port: u16,
    pub broker_id: String,
    pub public_endpoint: String,
    pub defradb_url: String,
    pub outbound_timeout_ms: u64,
    pub entity_watch_enabled: bool,
    pub entity_watch_interval_ms: u64,
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
            defradb_url: env_or("BROKER_DEFRADB_URL", "http://127.0.0.1:9181/api/v0/graphql"),
            outbound_timeout_ms: env_or("BROKER_OUTBOUND_TIMEOUT_MS", "5000")
                .parse()
                .unwrap_or(5000),
            entity_watch_enabled: env_bool_or("BROKER_ENTITY_WATCH_ENABLED", true),
            entity_watch_interval_ms: env_or("BROKER_ENTITY_WATCH_INTERVAL_MS", "1000")
                .parse()
                .unwrap_or(1000),
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
            defradb_url: "http://127.0.0.1:9181/api/v0/graphql".to_string(),
            outbound_timeout_ms: 2000,
            entity_watch_enabled: true,
            entity_watch_interval_ms: 1000,
        }
    }
}

/// Reads environment variable or falls back to default value.
fn env_or(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

/// Reads boolean environment variable or falls back to default value.
fn env_bool_or(name: &str, default: bool) -> bool {
    env::var(name)
        .map(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}
