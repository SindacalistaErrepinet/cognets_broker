//! Query planning helpers.
//!
//! This module converts NGSI-LD request parameters into reusable filter objects.
//! Service code uses [`QueryPlan`] to narrow candidate documents before running
//! richer in-memory query evaluation.
use bson::{Document, doc};
use regex::Regex;
use serde_json::{Value, json};

use crate::{
    error::BrokerError,
    query::{
        language::{QExpression, parse_q_expression, parse_scope_expression},
        types::{EntityQuery, Representation, TemporalEntityQuery},
    },
};

/// Repository-facing filter plan derived from entity query parameters.
#[derive(Debug, Clone, Default)]
pub struct QueryPlan {
    /// Explicit entity ids requested by caller.
    pub ids: Vec<String>,
    /// Allowed entity types requested by caller.
    pub entity_types: Vec<String>,
    /// Optional regex string applied to entity ids.
    pub id_pattern: Option<String>,
    /// Attributes that must exist on candidate entities.
    pub attrs: Vec<String>,
    /// Parsed `q` expression kept for later in-memory evaluation.
    pub q_expression: Option<QExpression>,
    /// Parsed geo filter when request includes geo clauses.
    pub geo: Option<GeoFilter>,
    /// Result limit that can be pushed into storage-side filtering safely.
    pub limit: Option<usize>,
}

/// Parsed geo constraint from entity query parameters.
#[derive(Debug, Clone)]
pub struct GeoFilter {
    /// GeoJSON geometry type such as `Point` or `Polygon`.
    pub geometry: String,
    /// NGSI-LD georel operator string.
    pub georel: String,
    /// Target entity property that holds geometry value.
    pub geoproperty: String,
    /// Parsed GeoJSON coordinates payload.
    pub coordinates: Value,
}

/// Parsed temporal window and projection settings.
#[derive(Debug, Clone)]
pub struct TemporalFilter {
    /// Timestamp member used to compare history records.
    pub time_property: String,
    /// Temporal relation such as `before`, `after`, or `between`.
    pub time_rel: String,
    /// Lower-bound or anchor timestamp from request.
    pub time_at: String,
    /// Upper-bound timestamp for `between` queries.
    pub end_time_at: Option<String>,
    /// Optional `lastN` truncation applied after filtering.
    pub last_n: Option<u32>,
    /// Requested aggregation methods for aggregated views.
    pub aggr_methods: Vec<String>,
    /// Optional aggregation bucket duration.
    pub aggr_period_duration: Option<String>,
    /// Temporal response representation to render.
    pub representation: Representation,
}

impl QueryPlan {
    /// Builds repository query plan from NGSI-LD entity query parameters.
    pub fn from_entity_query(query: &EntityQuery) -> Result<Self, BrokerError> {
        let q_expression = parse_q_expression(query.q.as_deref())?;
        let _ = parse_scope_expression(query.scope_q.as_deref())?;

        Ok(Self {
            ids: split_csv(query.id.as_deref()),
            entity_types: split_csv(query.entity_type.as_deref()),
            id_pattern: query.id_pattern.clone(),
            attrs: split_csv(query.attrs.as_deref()),
            q_expression,
            geo: parse_geo_filter(
                query.geometry.as_deref(),
                query.georel.as_deref(),
                query.coordinates.as_deref(),
                query.geoproperty.as_deref(),
            )?,
            limit: if query.q.is_some() || query.scope_q.is_some() || query.count.unwrap_or(false) {
                None
            } else {
                query.limit
            },
        })
    }

    /// Converts query plan into BSON filter document used by current backend.
    pub fn to_bson_filter(&self, tenant: &str) -> Result<Document, BrokerError> {
        let mut clauses = vec![doc! {"tenant": tenant}];

        if !self.ids.is_empty() {
            clauses.push(doc! {"id": {"$in": self.ids.clone()}});
        }

        if !self.entity_types.is_empty() {
            if self.entity_types.len() == 1 {
                clauses.push(doc! {
                    "$or": [
                        {"doc.type": &self.entity_types[0]},
                        {"doc.type": {"$elemMatch": {"$eq": &self.entity_types[0]}}}
                    ]
                });
            } else {
                clauses.push(doc! {
                    "$or": [
                        {"doc.type": {"$in": self.entity_types.clone()}},
                        {"doc.type": {"$elemMatch": {"$in": self.entity_types.clone()}}}
                    ]
                });
            }
        }

        if let Some(id_pattern) = &self.id_pattern {
            let _ = Regex::new(id_pattern)
                .map_err(|error| BrokerError::BadRequest(format!("invalid idPattern: {error}")))?;
            clauses.push(doc! {"id": {"$regex": id_pattern}});
        }

        for attr in &self.attrs {
            clauses.push(doc! {format!("doc.{attr}"): {"$exists": true}});
        }

        if let Some(geo) = &self.geo {
            clauses.push(geo.to_bson()?);
        }

        Ok(if clauses.len() == 1 {
            clauses.remove(0)
        } else {
            doc! {"$and": clauses}
        })
    }
}

impl GeoFilter {
    /// Converts geo filter into BSON geospatial predicate used by current backend.
    pub fn to_bson(&self) -> Result<Document, BrokerError> {
        let property = format!("doc.{}.value", self.geoproperty);
        let geo_json = match self.geometry.as_str() {
            "Point" | "MultiPoint" | "LineString" | "MultiLineString" | "Polygon"
            | "MultiPolygon" => {
                json!({
                    "type": self.geometry,
                    "coordinates": self.coordinates,
                })
            }
            other => {
                return Err(BrokerError::BadRequest(format!(
                    "unsupported geometry {other}"
                )));
            }
        };

        let geo_doc = bson::to_bson(&geo_json).map_err(|error| {
            BrokerError::BadRequest(format!("invalid geo coordinates: {error}"))
        })?;

        if self.georel.starts_with("near;") {
            let mut spec = doc! {"$geometry": geo_doc};
            for token in self.georel.split(';').skip(1) {
                if let Some((distance_type, raw_value)) = token.split_once("==") {
                    let distance = raw_value.parse::<i64>().map_err(|error| {
                        BrokerError::BadRequest(format!(
                            "invalid near distance {raw_value}: {error}"
                        ))
                    })?;
                    match distance_type {
                        "maxDistance" => {
                            spec.insert("$maxDistance", distance);
                        }
                        "minDistance" => {
                            spec.insert("$minDistance", distance);
                        }
                        _ => {
                            return Err(BrokerError::BadRequest(format!(
                                "invalid georel near clause {distance_type}"
                            )));
                        }
                    }
                }
            }
            Ok(doc! {property: {"$near": spec}})
        } else {
            let operator = match self.georel.as_str() {
                "within" => "$geoWithin",
                "intersects" => "$geoIntersects",
                "contains" => "$geoWithin",
                "overlaps" => "$geoIntersects",
                "equals" => "$geoIntersects",
                "disjoint" => {
                    return Err(BrokerError::NotImplemented(
                        "georel disjoint is not supported by current query planner".to_string(),
                    ));
                }
                other => {
                    return Err(BrokerError::BadRequest(format!("invalid georel {other}")));
                }
            };

            Ok(doc! {property: {operator: {"$geometry": geo_doc}}})
        }
    }
}

impl TemporalFilter {
    /// Builds temporal filter from temporal query parameters.
    pub fn from_query(query: &TemporalEntityQuery) -> Result<Self, BrokerError> {
        let time_rel = query
            .time_rel
            .clone()
            .ok_or_else(|| BrokerError::BadRequest("timerel is required".to_string()))?;
        let time_at = query
            .time_at
            .clone()
            .ok_or_else(|| BrokerError::BadRequest("timeAt is required".to_string()))?;

        if time_rel == "between" && query.end_time_at.is_none() {
            return Err(BrokerError::BadRequest(
                "endTimeAt is required when timerel=between".to_string(),
            ));
        }

        Ok(Self {
            time_property: query
                .time_property
                .clone()
                .unwrap_or_else(|| "observedAt".to_string()),
            time_rel,
            time_at,
            end_time_at: query.end_time_at.clone(),
            last_n: query.last_n,
            aggr_methods: split_csv(query.aggr_methods.as_deref()),
            aggr_period_duration: query.aggr_period_duration.clone(),
            representation: temporal_representation(
                query.entity.format.as_deref(),
                query.entity.options.as_deref(),
            ),
        })
    }
}

/// Parses geo query components into validated geo filter.
pub fn parse_geo_filter(
    geometry: Option<&str>,
    georel: Option<&str>,
    coordinates: Option<&str>,
    geoproperty: Option<&str>,
) -> Result<Option<GeoFilter>, BrokerError> {
    if geometry.is_none() && georel.is_none() && coordinates.is_none() {
        return Ok(None);
    }

    let geometry = geometry.ok_or_else(|| {
        BrokerError::BadRequest("geometry is required for geo queries".to_string())
    })?;
    let georel = georel
        .ok_or_else(|| BrokerError::BadRequest("georel is required for geo queries".to_string()))?;
    let coordinates = coordinates.ok_or_else(|| {
        BrokerError::BadRequest("coordinates is required for geo queries".to_string())
    })?;

    let coordinates: Value = serde_json::from_str(coordinates).map_err(|error| {
        BrokerError::BadRequest(format!(
            "coordinates must be valid JSON coordinates: {error}"
        ))
    })?;

    Ok(Some(GeoFilter {
        geometry: geometry.to_string(),
        georel: georel.to_string(),
        geoproperty: geoproperty.unwrap_or("location").to_string(),
        coordinates,
    }))
}

/// Splits CSV query field into trimmed values.
fn split_csv(value: Option<&str>) -> Vec<String> {
    value
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// Selects temporal response representation from format/options.
fn temporal_representation(format: Option<&str>, options: Option<&str>) -> Representation {
    if format == Some("aggregatedValues")
        || options
            .map(|value| value.contains("aggregatedValues"))
            .unwrap_or(false)
    {
        Representation::AggregatedValues
    } else if format == Some("temporalValues")
        || options
            .map(|value| value.contains("temporalValues"))
            .unwrap_or(false)
    {
        Representation::TemporalValues
    } else {
        Representation::Normalized
    }
}

#[cfg(test)]
mod tests {
    use bson::{Bson, doc};

    use super::*;

    fn temporal_query() -> TemporalEntityQuery {
        TemporalEntityQuery {
            entity: EntityQuery::default(),
            time_property: Some("observedAt".to_string()),
            time_rel: Some("after".to_string()),
            time_at: Some("2024-01-01T00:00:00Z".to_string()),
            end_time_at: None,
            last_n: None,
            aggr_methods: None,
            aggr_period_duration: None,
        }
    }

    #[test]
    fn builds_bson_filter_from_entity_query() -> Result<(), BrokerError> {
        let query = EntityQuery {
            id: Some("urn:ngsi-ld:Vehicle:1,urn:ngsi-ld:Vehicle:2".to_string()),
            entity_type: Some("Vehicle".to_string()),
            attrs: Some("speed".to_string()),
            q: Some("speed>=50".to_string()),
            ..Default::default()
        };

        let plan = QueryPlan::from_entity_query(&query)?;
        let filter = plan.to_bson_filter("tenant-a")?;

        assert_eq!(
            filter,
            doc! {
                "$and": [
                    {"tenant": "tenant-a"},
                    {"id": {"$in": ["urn:ngsi-ld:Vehicle:1", "urn:ngsi-ld:Vehicle:2"]}},
                    {"$or": [
                        {"doc.type": "Vehicle"},
                        {"doc.type": {"$elemMatch": {"$eq": "Vehicle"}}}
                    ]},
                    {"doc.speed": {"$exists": true}}
                ]
            }
        );

        assert!(plan.q_expression.is_some());
        assert_eq!(plan.limit, None);

        Ok(())
    }

    #[test]
    fn count_queries_do_not_push_limit_into_storage() -> Result<(), BrokerError> {
        let query = EntityQuery {
            entity_type: Some("Vehicle".to_string()),
            limit: Some(1),
            count: Some(true),
            ..Default::default()
        };

        let plan = QueryPlan::from_entity_query(&query)?;

        assert_eq!(plan.limit, None);
        Ok(())
    }

    #[test]
    fn rejects_incomplete_geo_query() {
        let error = parse_geo_filter(Some("Point"), Some("within"), None, None).unwrap_err();
        assert!(
            matches!(error, BrokerError::BadRequest(message) if message.contains("coordinates is required"))
        );
    }

    #[test]
    fn builds_near_geo_filter_with_distance_bounds() -> Result<(), BrokerError> {
        let geo = GeoFilter {
            geometry: "Point".to_string(),
            georel: "near;maxDistance==1000;minDistance==10".to_string(),
            geoproperty: "location".to_string(),
            coordinates: json!([12.3, 45.6]),
        };

        let filter = geo.to_bson()?;
        let near = filter
            .get_document("doc.location.value")
            .unwrap()
            .get_document("$near")
            .unwrap();

        assert_eq!(near.get_i64("$maxDistance").unwrap(), 1000);
        assert_eq!(near.get_i64("$minDistance").unwrap(), 10);

        let geometry = near.get_document("$geometry").unwrap();
        assert_eq!(geometry.get_str("type").unwrap(), "Point");
        assert_eq!(
            geometry.get_array("coordinates").unwrap(),
            &vec![Bson::Double(12.3), Bson::Double(45.6)]
        );

        Ok(())
    }

    #[test]
    fn rejects_between_temporal_query_without_end_time() {
        let mut query = temporal_query();
        query.time_rel = Some("between".to_string());

        let error = TemporalFilter::from_query(&query).unwrap_err();
        assert!(
            matches!(error, BrokerError::BadRequest(message) if message.contains("endTimeAt is required"))
        );
    }

    #[test]
    fn maps_aggregated_temporal_representation_from_query() -> Result<(), BrokerError> {
        let mut query = temporal_query();
        query.entity.format = Some("aggregatedValues".to_string());

        let temporal = TemporalFilter::from_query(&query)?;

        assert_eq!(temporal.time_property, "observedAt");
        assert_eq!(temporal.representation, Representation::AggregatedValues);

        Ok(())
    }
}
