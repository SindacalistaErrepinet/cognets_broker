use actix_web::{
    HttpResponse, ResponseError,
    http::{StatusCode, header::ContentType},
};
use serde::Serialize;
use thiserror::Error;
use utoipa::ToSchema;

#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    NotImplemented(String),
    #[error("{0}")]
    Internal(String),
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ProblemDetails {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub status: u16,
    pub detail: String,
}

impl BrokerError {
    /// Builds internal error variant from displayable message.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }

    /// Maps broker error variant to Problem Details title.
    fn title(&self) -> &'static str {
        match self {
            Self::BadRequest(_) => "InvalidRequest",
            Self::NotFound(_) => "ResourceNotFound",
            Self::Conflict(_) => "AlreadyExists",
            Self::NotImplemented(_) => "NotImplemented",
            Self::Internal(_) => "InternalError",
        }
    }

    /// Converts broker error into RFC7807-compatible payload.
    fn details(&self) -> ProblemDetails {
        ProblemDetails {
            r#type: Some("about:blank".to_string()),
            title: Some(self.title().to_string()),
            status: self.status_code().as_u16(),
            detail: self.to_string(),
        }
    }
}

impl ResponseError for BrokerError {
    /// Maps broker error variant to HTTP status code.
    fn status_code(&self) -> StatusCode {
        match self {
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Builds JSON HTTP response for broker error.
    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code())
            .insert_header(ContentType::json())
            .json(self.details())
    }
}

impl From<serde_json::Error> for BrokerError {
    /// Converts JSON parsing failures into bad request errors.
    fn from(error: serde_json::Error) -> Self {
        Self::BadRequest(format!("invalid JSON payload: {error}"))
    }
}

impl From<reqwest::Error> for BrokerError {
    /// Converts outbound HTTP failures into internal errors.
    fn from(error: reqwest::Error) -> Self {
        Self::Internal(format!("outbound HTTP client error: {error}"))
    }
}
