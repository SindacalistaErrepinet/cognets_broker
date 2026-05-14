use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AttributeInfo {
    pub id: String,
    #[serde(rename = "type")]
    pub r#type: String,
    #[serde(rename = "attributeName")]
    pub attribute_name: String,
    #[serde(rename = "attributeCount")]
    pub attribute_count: u64,
    #[serde(rename = "attributeTypes")]
    pub attribute_types: Vec<String>,
    #[serde(rename = "typeNames")]
    pub type_names: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AttributeList {
    pub id: String,
    #[serde(rename = "type")]
    pub r#type: String,
    #[serde(rename = "attributeList")]
    pub attribute_list: Vec<String>,
}

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

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct EntityTypeList {
    pub id: String,
    #[serde(rename = "type")]
    pub r#type: String,
    #[serde(rename = "typeList")]
    pub type_list: Vec<String>,
}
