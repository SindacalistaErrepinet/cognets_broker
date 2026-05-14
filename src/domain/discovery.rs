//! Discovery response payloads for entity types and attributes.
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Detailed discovery record for one attribute name.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AttributeInfo {
    /// Stable identifier for record.
    pub id: String,
    /// NGSI-LD type of discovery payload.
    #[serde(rename = "type")]
    pub r#type: String,
    /// Attribute name being described.
    #[serde(rename = "attributeName")]
    pub attribute_name: String,
    /// Number of entities that expose this attribute.
    #[serde(rename = "attributeCount")]
    pub attribute_count: u64,
    /// Distinct NGSI-LD attribute kinds seen under this name.
    #[serde(rename = "attributeTypes")]
    pub attribute_types: Vec<String>,
    /// Entity type names that use this attribute.
    #[serde(rename = "typeNames")]
    pub type_names: Vec<String>,
}

/// Summary list of discovered attribute names.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AttributeList {
    pub id: String,
    #[serde(rename = "type")]
    pub r#type: String,
    #[serde(rename = "attributeList")]
    pub attribute_list: Vec<String>,
}

/// Detailed discovery record for one entity type.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct EntityTypeInfo {
    pub id: String,
    #[serde(rename = "type")]
    pub r#type: String,
    #[serde(rename = "typeName")]
    pub type_name: String,
    #[serde(rename = "entityCount")]
    pub entity_count: u64,
    #[serde(rename = "attributeDetails")]
    pub attribute_details: Vec<AttributeInfo>,
}

/// Summary list of discovered entity type names.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct EntityTypeList {
    pub id: String,
    #[serde(rename = "type")]
    pub r#type: String,
    #[serde(rename = "typeList")]
    pub type_list: Vec<String>,
}
