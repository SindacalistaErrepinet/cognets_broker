//! HTTP header and request-context helpers.
use actix_web::{HttpRequest, http::header};
use serde_json::{Map, Value};

use crate::{config::AppConfig, query::types::Representation};

/// Default tenant used when request omits `NGSILD-Tenant`.
pub const DEFAULT_TENANT: &str = "default";
/// Tenant header used for tenant-scoped operations.
pub const HEADER_TENANT: &str = "NGSILD-Tenant";
/// Response header used for total-count reporting.
pub const HEADER_RESULTS_COUNT: &str = "NGSILD-Results-Count";
/// Standard HTTP `Link` header used for JSON-LD context backfill.
pub const HEADER_LINK: &str = "Link";
/// Standard HTTP `Via` header used for hop tracking.
pub const HEADER_VIA: &str = "Via";

/// Request-derived metadata shared across service calls.
#[derive(Clone, Debug)]
pub struct RequestContext {
    /// Effective tenant name for request.
    pub tenant: String,
    /// Parsed `Via` chain used for loop avoidance and forwarding.
    pub via: Vec<String>,
    /// Raw `Link` header value when present.
    pub link_header: Option<String>,
}

impl RequestContext {
    /// Extracts tenant, Via, and Link metadata from request headers.
    pub fn from_request(request: &HttpRequest) -> Self {
        Self {
            tenant: request
                .headers()
                .get(HEADER_TENANT)
                .and_then(|value| value.to_str().ok())
                .filter(|value| !value.is_empty())
                .unwrap_or(DEFAULT_TENANT)
                .to_string(),
            via: request
                .headers()
                .get(HEADER_VIA)
                .and_then(|value| value.to_str().ok())
                .map(parse_via)
                .unwrap_or_default(),
            link_header: request
                .headers()
                .get(HEADER_LINK)
                .and_then(|value| value.to_str().ok())
                .map(ToString::to_string),
        }
    }
}

/// Parses comma-separated Via header into hop markers.
pub fn parse_via(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(ToString::to_string)
        .collect()
}

/// Appends local broker markers to outgoing Via chain.
pub fn extend_via(existing: &[String], config: &AppConfig) -> Vec<String> {
    let mut via = existing.to_vec();

    for marker in [&config.broker_id, &config.public_endpoint] {
        if !via.iter().any(|token| token == marker) {
            via.push(marker.to_string());
        }
    }

    via
}

/// Backfills `@context` from Link header when payload omits it.
pub fn apply_context_link(document: &mut Map<String, Value>, link_header: Option<&str>) {
    if document.contains_key("@context") {
        return;
    }

    if let Some(context_value) = link_header.and_then(parse_context_link) {
        document.insert("@context".to_string(), context_value);
    }
}

/// Extracts JSON-LD context targets from Link header value.
pub fn parse_context_link(link_header: &str) -> Option<Value> {
    let mut contexts = Vec::new();

    for entry in link_header.split(',') {
        let entry = entry.trim();
        if !entry.contains("json-ld#context") {
            continue;
        }

        let start = entry.find('<')? + 1;
        let end = entry[start..].find('>')? + start;
        contexts.push(Value::String(entry[start..end].to_string()));
    }

    match contexts.len() {
        0 => None,
        1 => contexts.into_iter().next(),
        _ => Some(Value::Array(contexts)),
    }
}

/// Chooses response content type for requested representation.
pub fn content_type_for_representation(
    request: &HttpRequest,
    representation: Representation,
) -> &'static str {
    if representation == Representation::GeoJson {
        return "application/geo+json";
    }

    let accept = request
        .headers()
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();

    if accept.contains("application/ld+json") {
        "application/ld+json"
    } else if accept.contains("application/json+ld") {
        "application/json+ld"
    } else {
        "application/json"
    }
}
