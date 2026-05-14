use serde::Serialize;
use utoipa::ToSchema;

use crate::error::ProblemDetails;

#[derive(Debug, Serialize, ToSchema)]
pub struct BatchEntityError {
    #[serde(rename = "entityId")]
    pub entity_id: String,
    #[serde(rename = "registrationId", skip_serializing_if = "Option::is_none")]
    pub registration_id: Option<String>,
    pub error: ProblemDetails,
}

#[derive(Debug, Default, Serialize, ToSchema)]
pub struct BatchOperationResult {
    pub success: Vec<String>,
    pub errors: Vec<BatchEntityError>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct NotUpdatedDetails {
    #[serde(rename = "attributeName")]
    pub attribute_name: String,
    pub reason: ProblemDetails,
}

#[derive(Debug, Default, Serialize, ToSchema)]
pub struct UpdateResult {
    pub updated: Vec<String>,
    #[serde(rename = "notUpdated")]
    pub not_updated: Vec<NotUpdatedDetails>,
}

#[derive(Debug)]
pub struct BatchUpsertOutcome {
    pub created_ids: Vec<String>,
    pub had_updates: bool,
}
