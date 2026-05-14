use url::Url;

use crate::{
    app::state::AppState,
    context::headers::RequestContext,
    domain::types::ContextSourceIdentity,
    error::BrokerError,
    utils::time::{duration_to_iso8601, now_timestamp},
};

/// Returns identity information for this broker and tenant.
pub fn source_identity(
    state: &AppState,
    context: &RequestContext,
) -> Result<ContextSourceIdentity, BrokerError> {
    Ok(ContextSourceIdentity {
        id: source_identity_id(&state.config.public_endpoint, &context.tenant)?,
        kind: "ContextSourceIdentity".to_string(),
        context_source_alias: source_identity_alias(&state.config.broker_id, &context.tenant),
        context_source_up_time: duration_to_iso8601(state.started_at.elapsed()),
        context_source_time_at: now_timestamp(),
    })
}

fn source_identity_id(public_endpoint: &str, tenant: &str) -> Result<String, BrokerError> {
    let mut url = Url::parse(public_endpoint).map_err(|error| {
        BrokerError::internal(format!(
            "invalid BROKER_PUBLIC_ENDPOINT for source identity: {error}"
        ))
    })?;
    let path = format!("{}/info/sourceIdentity", url.path().trim_end_matches('/'));
    url.set_path(&path);
    url.query_pairs_mut().append_pair("tenant", tenant);
    Ok(url.to_string())
}

fn source_identity_alias(broker_id: &str, tenant: &str) -> String {
    format!(
        "{}-{}",
        sanitize_pseudonym_component(broker_id),
        sanitize_pseudonym_component(tenant)
    )
}

fn sanitize_pseudonym_component(value: &str) -> String {
    let sanitized = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric()
                || matches!(
                    ch,
                    '!' | '#'
                        | '$'
                        | '%'
                        | '&'
                        | '\''
                        | '*'
                        | '+'
                        | '-'
                        | '.'
                        | '^'
                        | '_'
                        | '`'
                        | '|'
                        | '~'
                )
            {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();

    if sanitized.is_empty() {
        "default".to_string()
    } else {
        sanitized
    }
}
