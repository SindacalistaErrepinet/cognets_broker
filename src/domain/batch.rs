//! Batch-operation result payloads.
use serde::Serialize;
use utoipa::ToSchema;

use crate::error::ProblemDetails;

/// Per-entity failure entry returned by batch endpoints.
#[derive(Debug, Serialize, ToSchema)]
pub struct BatchEntityError {
    /// Entity id associated with failed item.
    #[serde(rename = "entityId")]
    pub entity_id: String,
    /// Optional registration id for registry-style responses.
    #[serde(rename = "registrationId", skip_serializing_if = "Option::is_none")]
    pub registration_id: Option<String>,
    /// RFC 7807-compatible failure payload.
    pub error: ProblemDetails,
}

/// Aggregate success and error ids for batch operation responses.
#[derive(Debug, Default, Serialize, ToSchema)]
pub struct BatchOperationResult {
    /// Entity ids processed successfully.
    pub success: Vec<String>,
    /// Per-entity failures.
    pub errors: Vec<BatchEntityError>,
}

/// Per-attribute failure detail for partial attribute updates.
#[derive(Debug, Serialize, ToSchema)]
pub struct NotUpdatedDetails {
    /// Attribute name that could not be updated.
    #[serde(rename = "attributeName")]
    pub attribute_name: String,
    /// Error payload explaining why update was skipped.
    pub reason: ProblemDetails,
}

/// Attribute update outcome with partial-success reporting.
#[derive(Debug, Default, Serialize, ToSchema)]
pub struct UpdateResult {
    /// Attribute names updated successfully.
    pub updated: Vec<String>,
    /// Attribute-level failures that did not abort whole request.
    #[serde(rename = "notUpdated")]
    pub not_updated: Vec<NotUpdatedDetails>,
}

/// Internal batch-upsert outcome used by API layer to choose status code.
#[derive(Debug)]
pub struct BatchUpsertOutcome {
    /// Entity ids created during upsert run.
    pub created_ids: Vec<String>,
    /// Reports whether at least one existing entity was updated.
    pub had_updates: bool,
}
