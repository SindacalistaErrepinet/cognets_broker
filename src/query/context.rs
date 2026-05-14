//! JSON-LD context resolution helpers.
use std::collections::{HashMap, HashSet, VecDeque};

use reqwest::Client;
use serde_json::Value;

use crate::{context::headers::parse_context_link, error::BrokerError};

/// Resolves compacted JSON-LD terms from inline and linked contexts.
///
/// Resolution walks local `@context` values plus optional `Link` header target,
/// follows nested `@context` members breadth-first, fetches remote HTTP(S)
/// contexts once, and returns compact-term to expanded-IRI mapping used by
/// query evaluation.
pub async fn resolve_context_terms(
    http_client: &Client,
    context_value: Option<&Value>,
    link_header: Option<&str>,
) -> Result<HashMap<String, String>, BrokerError> {
    let mut queue = VecDeque::new();
    let mut terms = HashMap::new();
    let mut visited_urls = HashSet::new();

    if let Some(context_value) = context_value {
        queue.push_back(context_value.clone());
    }
    if let Some(link_value) = link_header.and_then(parse_context_link) {
        queue.push_back(link_value);
    }

    while let Some(value) = queue.pop_front() {
        match value {
            Value::String(url) => {
                if !visited_urls.insert(url.clone()) {
                    continue;
                }
                if !url.starts_with("http://") && !url.starts_with("https://") {
                    continue;
                }

                let response = http_client.get(&url).send().await.map_err(|error| {
                    BrokerError::BadRequest(format!("failed to resolve @context {url}: {error}"))
                })?;
                if !response.status().is_success() {
                    return Err(BrokerError::BadRequest(format!(
                        "failed to resolve @context {url}: HTTP {}",
                        response.status()
                    )));
                }

                let payload = response.json::<Value>().await.map_err(|error| {
                    BrokerError::BadRequest(format!("invalid @context document {url}: {error}"))
                })?;
                queue.push_back(payload);
            }
            Value::Array(items) => {
                for item in items {
                    queue.push_back(item);
                }
            }
            Value::Object(object) => {
                if let Some(nested) = object.get("@context") {
                    queue.push_back(nested.clone());
                }

                for (term, definition) in object {
                    if term == "@context" {
                        continue;
                    }

                    let Some(expanded) = term_definition_uri(&definition) else {
                        continue;
                    };
                    terms.insert(term, expanded.to_string());
                }
            }
            _ => {}
        }
    }

    Ok(terms)
}

/// Extracts expanded URI from one term definition entry.
fn term_definition_uri(definition: &Value) -> Option<&str> {
    match definition {
        Value::String(uri) => Some(uri),
        Value::Object(object) => object.get("@id").and_then(Value::as_str),
        _ => None,
    }
}
