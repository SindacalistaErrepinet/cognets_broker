use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveTime};
use regex::Regex;
use serde_json::Value;

use crate::error::BrokerError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryAttribute {
    ValuePath {
        segments: Vec<String>,
        trailing_path: Option<TrailingPath>,
    },
    LinkedEntity {
        relation: String,
        entity_types: Vec<String>,
        attribute: Box<QueryAttribute>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrailingPath {
    Members(Vec<String>),
    AnyLanguage,
}

#[derive(Debug, Clone, PartialEq)]
pub enum EqualityOperand {
    Single(Value),
    List(Vec<Value>),
    Range(Value, Value),
}

#[derive(Debug, Clone, PartialEq)]
pub enum QExpression {
    Exists(QueryAttribute),
    Eq(QueryAttribute, EqualityOperand),
    Neq(QueryAttribute, EqualityOperand),
    Gt(QueryAttribute, Value),
    Gte(QueryAttribute, Value),
    Lt(QueryAttribute, Value),
    Lte(QueryAttribute, Value),
    Pattern(QueryAttribute, String),
    NotPattern(QueryAttribute, String),
    And(Vec<QExpression>),
    Or(Vec<QExpression>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeExpression {
    AnyNonEmpty,
    Pattern(ScopePattern),
    And(Vec<ScopePattern>),
    Or(Vec<ScopeExpression>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopePattern {
    pub levels: Vec<ScopeLevel>,
    pub include_descendants: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeLevel {
    Exact(String),
    SingleLevelWildcard,
}

#[derive(Debug, Clone, Default)]
pub struct QueryMatchOptions {
    pub expand_values: HashSet<String>,
    pub json_keys: HashSet<String>,
    pub context_terms: HashMap<String, String>,
    pub linked_entities: Arc<HashMap<String, Value>>,
}

impl QueryMatchOptions {
    /// Builds match options from expansion lists and linked-entity cache.
    pub fn with_value_lists(
        expand_values: &[String],
        json_keys: &[String],
        context_terms: HashMap<String, String>,
        linked_entities: HashMap<String, Value>,
    ) -> Self {
        Self {
            expand_values: expand_values.iter().cloned().collect(),
            json_keys: json_keys.iter().cloned().collect(),
            context_terms,
            linked_entities: Arc::new(linked_entities),
        }
    }
}

/// Parses optional `q` string into expression tree.
pub fn parse_q_expression(q: Option<&str>) -> Result<Option<QExpression>, BrokerError> {
    let Some(q) = q else {
        return Ok(None);
    };
    let q = q.trim();
    if q.is_empty() {
        return Ok(None);
    }

    parse_q_or(q).map(Some)
}

/// Evaluates raw `q` filter against value with default options.
pub fn query_matches_value(value: &Value, raw: Option<&str>) -> Result<bool, BrokerError> {
    query_matches_value_with_options(value, raw, &QueryMatchOptions::default())
}

/// Evaluates raw `q` filter against value with explicit options.
pub fn query_matches_value_with_options(
    value: &Value,
    raw: Option<&str>,
    options: &QueryMatchOptions,
) -> Result<bool, BrokerError> {
    let Some(expression) = parse_q_expression(raw)? else {
        return Ok(true);
    };

    Ok(q_expression_matches_value_with_options(
        &expression,
        value,
        options,
    ))
}

/// Evaluates parsed `q` expression against value with default options.
pub fn q_expression_matches_value(expression: &QExpression, value: &Value) -> bool {
    q_expression_matches_value_with_options(expression, value, &QueryMatchOptions::default())
}

/// Evaluates parsed `q` expression against value with explicit options.
pub fn q_expression_matches_value_with_options(
    expression: &QExpression,
    value: &Value,
    options: &QueryMatchOptions,
) -> bool {
    match expression {
        QExpression::Exists(attribute) => {
            !resolve_query_attribute(value, attribute, options).is_empty()
        }
        QExpression::Eq(attribute, operand) => resolve_query_attribute(value, attribute, options)
            .iter()
            .any(|candidate| equality_matches(candidate, attribute, operand, options)),
        QExpression::Neq(attribute, operand) => {
            let candidates = resolve_query_attribute(value, attribute, options);
            !candidates.is_empty()
                && candidates
                    .iter()
                    .all(|candidate| !equality_matches(candidate, attribute, operand, options))
        }
        QExpression::Gt(attribute, expected) => {
            compare_query_attribute(value, attribute, expected, options, |ordering| {
                ordering == Ordering::Greater
            })
        }
        QExpression::Gte(attribute, expected) => {
            compare_query_attribute(value, attribute, expected, options, |ordering| {
                matches!(ordering, Ordering::Greater | Ordering::Equal)
            })
        }
        QExpression::Lt(attribute, expected) => {
            compare_query_attribute(value, attribute, expected, options, |ordering| {
                ordering == Ordering::Less
            })
        }
        QExpression::Lte(attribute, expected) => {
            compare_query_attribute(value, attribute, expected, options, |ordering| {
                matches!(ordering, Ordering::Less | Ordering::Equal)
            })
        }
        QExpression::Pattern(attribute, pattern) => Regex::new(pattern).ok().is_some_and(|regex| {
            resolve_query_attribute(value, attribute, options)
                .iter()
                .any(|candidate| pattern_matches(candidate, attribute, &regex, options))
        }),
        QExpression::NotPattern(attribute, pattern) => {
            Regex::new(pattern).ok().is_some_and(|regex| {
                let candidates = resolve_query_attribute(value, attribute, options);
                !candidates.is_empty()
                    && candidates
                        .iter()
                        .all(|candidate| not_pattern_matches(candidate, attribute, &regex, options))
            })
        }
        QExpression::And(parts) => parts
            .iter()
            .all(|part| q_expression_matches_value_with_options(part, value, options)),
        QExpression::Or(parts) => parts
            .iter()
            .any(|part| q_expression_matches_value_with_options(part, value, options)),
    }
}

/// Reports whether parsed `q` expression dereferences linked entities.
pub fn q_expression_uses_linked_entity(expression: &QExpression) -> bool {
    match expression {
        QExpression::Exists(attribute)
        | QExpression::Eq(attribute, _)
        | QExpression::Neq(attribute, _)
        | QExpression::Gt(attribute, _)
        | QExpression::Gte(attribute, _)
        | QExpression::Lt(attribute, _)
        | QExpression::Lte(attribute, _)
        | QExpression::Pattern(attribute, _)
        | QExpression::NotPattern(attribute, _) => attribute_uses_linked_entity(attribute),
        QExpression::And(parts) | QExpression::Or(parts) => {
            parts.iter().any(q_expression_uses_linked_entity)
        }
    }
}

/// Parses optional `scopeQ` string into expression tree.
pub fn parse_scope_expression(
    scope_q: Option<&str>,
) -> Result<Option<ScopeExpression>, BrokerError> {
    let Some(scope_q) = scope_q else {
        return Ok(None);
    };
    let scope_q = scope_q.trim();
    if scope_q.is_empty() {
        return Ok(None);
    }

    parse_scope_or(scope_q).map(Some)
}

/// Evaluates raw `scopeQ` filter against extracted scope values.
pub fn scope_query_matches_values(
    scopes: &[String],
    scope_q: Option<&str>,
) -> Result<bool, BrokerError> {
    let Some(expression) = parse_scope_expression(scope_q)? else {
        return Ok(true);
    };

    Ok(scope_expression_matches_values(&expression, scopes))
}

/// Evaluates parsed `scopeQ` expression against scope values.
pub fn scope_expression_matches_values(expression: &ScopeExpression, scopes: &[String]) -> bool {
    match expression {
        ScopeExpression::AnyNonEmpty => scopes.iter().any(|scope| !scope.trim().is_empty()),
        ScopeExpression::Pattern(pattern) => scopes
            .iter()
            .any(|scope| scope_pattern_matches(scope, pattern)),
        ScopeExpression::And(patterns) => patterns.iter().all(|pattern| {
            scopes
                .iter()
                .any(|scope| scope_pattern_matches(scope, pattern))
        }),
        ScopeExpression::Or(parts) => parts
            .iter()
            .any(|part| scope_expression_matches_values(part, scopes)),
    }
}

/// Extracts scope values from entity payload.
pub fn extract_scope_values(value: &Value) -> Vec<String> {
    match value.get("scope") {
        Some(Value::String(scope)) => vec![scope.clone()],
        Some(Value::Array(scopes)) => scopes
            .iter()
            .filter_map(Value::as_str)
            .map(ToString::to_string)
            .collect(),
        _ => Vec::new(),
    }
}

/// Parses top-level OR branches in `q` expression.
fn parse_q_or(expression: &str) -> Result<QExpression, BrokerError> {
    let expression = strip_wrapping_parentheses(expression);
    let parts = split_top_level(expression, &['|']);
    if parts.len() > 1 {
        Ok(QExpression::Or(
            parts
                .into_iter()
                .map(parse_q_and)
                .collect::<Result<Vec<_>, _>>()?,
        ))
    } else {
        parse_q_and(expression)
    }
}

/// Parses top-level AND branches in `q` expression.
fn parse_q_and(expression: &str) -> Result<QExpression, BrokerError> {
    let expression = strip_wrapping_parentheses(expression);
    let parts = split_top_level(expression, &[';']);
    if parts.len() > 1 {
        Ok(QExpression::And(
            parts
                .into_iter()
                .map(parse_q_term)
                .collect::<Result<Vec<_>, _>>()?,
        ))
    } else {
        parse_q_term(expression)
    }
}

/// Parses one `q` term or parenthesized group.
fn parse_q_term(term: &str) -> Result<QExpression, BrokerError> {
    let term = term.trim();
    if is_wrapped_by_parentheses(term) {
        return parse_q_or(&term[1..term.len() - 1]);
    }
    let Some((index, operator)) = find_top_level_operator(term) else {
        return Ok(QExpression::Exists(parse_query_attribute(term)?));
    };

    let attribute = parse_query_attribute(term[..index].trim())?;
    let right = term[index + operator.len()..].trim();
    if right.is_empty() {
        return Err(BrokerError::BadRequest(format!(
            "missing right-hand side in q expression term: {term}"
        )));
    }

    match operator {
        "==" => Ok(QExpression::Eq(attribute, parse_equality_operand(right)?)),
        "!=" => Ok(QExpression::Neq(attribute, parse_equality_operand(right)?)),
        ">" => Ok(QExpression::Gt(attribute, parse_comparable_value(right)?)),
        ">=" => Ok(QExpression::Gte(attribute, parse_comparable_value(right)?)),
        "<" => Ok(QExpression::Lt(attribute, parse_comparable_value(right)?)),
        "<=" => Ok(QExpression::Lte(attribute, parse_comparable_value(right)?)),
        "~=" => Ok(QExpression::Pattern(attribute, parse_regex_pattern(right)?)),
        "!~=" => Ok(QExpression::NotPattern(
            attribute,
            parse_regex_pattern(right)?,
        )),
        _ => Err(BrokerError::BadRequest(format!(
            "invalid q expression term: {term}"
        ))),
    }
}

/// Parses query attribute path, including linked-entity syntax.
fn parse_query_attribute(raw: &str) -> Result<QueryAttribute, BrokerError> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(BrokerError::BadRequest(
            "empty q attribute path".to_string(),
        ));
    }

    if let Some(open) = find_top_level_char(raw, '{') {
        let relation = raw[..open].trim();
        validate_name(relation, "query relation")?;
        let Some(close) = find_matching_delimiter(raw, open, '{', '}') else {
            return Err(BrokerError::BadRequest(format!(
                "invalid linked entity path {raw}"
            )));
        };
        if close + 1 != raw.len() {
            return Err(BrokerError::BadRequest(format!(
                "invalid linked entity path {raw}"
            )));
        }

        let (entity_types, attribute) = parse_linked_entity_path(&raw[open + 1..close])?;
        return Ok(QueryAttribute::LinkedEntity {
            relation: relation.to_string(),
            entity_types,
            attribute: Box::new(attribute),
        });
    }

    parse_value_path(raw)
}

/// Parses linked-entity type filter and nested attribute path.
fn parse_linked_entity_path(raw: &str) -> Result<(Vec<String>, QueryAttribute), BrokerError> {
    if let Some(index) = find_top_level_char(raw, ':') {
        let entity_types = raw[..index]
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| {
                validate_name(value, "query entity type")?;
                Ok(value.to_string())
            })
            .collect::<Result<Vec<_>, BrokerError>>()?;
        if entity_types.is_empty() {
            return Err(BrokerError::BadRequest(format!(
                "invalid linked entity path {raw}"
            )));
        }
        Ok((
            entity_types,
            parse_query_attribute(raw[index + 1..].trim())?,
        ))
    } else {
        Ok((Vec::new(), parse_query_attribute(raw.trim())?))
    }
}

/// Parses plain attribute path with optional trailing member selector.
fn parse_value_path(raw: &str) -> Result<QueryAttribute, BrokerError> {
    let (base, trailing_path) = if let Some(start) = find_top_level_char(raw, '[') {
        let Some(end) = find_matching_delimiter(raw, start, '[', ']') else {
            return Err(BrokerError::BadRequest(format!(
                "invalid query attribute path {raw}"
            )));
        };
        if end + 1 != raw.len() {
            return Err(BrokerError::BadRequest(format!(
                "invalid query attribute path {raw}"
            )));
        }
        if find_top_level_char(&raw[end + 1..], '[').is_some() {
            return Err(BrokerError::BadRequest(format!(
                "invalid query attribute path {raw}"
            )));
        }

        let trailing = raw[start + 1..end].trim();
        let trailing_path = if trailing == "*" {
            Some(TrailingPath::AnyLanguage)
        } else {
            Some(TrailingPath::Members(parse_dotted_names(
                trailing,
                "query trailing path",
            )?))
        };
        (&raw[..start], trailing_path)
    } else {
        (raw, None)
    };

    Ok(QueryAttribute::ValuePath {
        segments: parse_dotted_names(base.trim(), "query attribute")?,
        trailing_path,
    })
}

/// Splits dotted path into validated segment names.
fn parse_dotted_names(raw: &str, label: &str) -> Result<Vec<String>, BrokerError> {
    let names = raw
        .split('.')
        .map(str::trim)
        .filter(|segment| !segment.is_empty())
        .map(|segment| {
            validate_name(segment, label)?;
            Ok(segment.to_string())
        })
        .collect::<Result<Vec<_>, BrokerError>>()?;
    if names.is_empty() {
        Err(BrokerError::BadRequest(format!(
            "invalid {label} path {raw}"
        )))
    } else {
        Ok(names)
    }
}

/// Parses `q` literal into JSON value.
fn parse_query_value(raw: &str) -> Result<Value, BrokerError> {
    if raw.starts_with('"') || raw.starts_with('[') || raw.starts_with('{') {
        serde_json::from_str(raw)
            .map_err(|error| BrokerError::BadRequest(format!("invalid q literal {raw}: {error}")))
    } else if raw.eq_ignore_ascii_case("true") || raw.eq_ignore_ascii_case("false") {
        Ok(Value::Bool(raw.eq_ignore_ascii_case("true")))
    } else if let Ok(number) = raw.parse::<f64>() {
        Ok(serde_json::Number::from_f64(number)
            .map(Value::Number)
            .unwrap_or_else(|| Value::String(raw.to_string())))
    } else {
        Ok(Value::String(raw.to_string()))
    }
}

/// Parses comparable `q` literal restricted to strings or numbers.
fn parse_comparable_value(raw: &str) -> Result<Value, BrokerError> {
    let value = parse_query_value(raw)?;
    match value {
        Value::Number(_) | Value::String(_) => Ok(value),
        _ => Err(BrokerError::BadRequest(format!(
            "invalid comparable q literal {raw}"
        ))),
    }
}

/// Parses equality operand as single value, list, or range.
fn parse_equality_operand(raw: &str) -> Result<EqualityOperand, BrokerError> {
    if let Some((lower, upper)) = split_once_top_level(raw, "..") {
        return Ok(EqualityOperand::Range(
            parse_comparable_value(lower.trim())?,
            parse_comparable_value(upper.trim())?,
        ));
    }

    let values = split_top_level(raw, &[',']);
    if values.len() > 1 {
        return Ok(EqualityOperand::List(
            values
                .into_iter()
                .map(|value| parse_query_value(value.trim()))
                .collect::<Result<Vec<_>, _>>()?,
        ));
    }

    Ok(EqualityOperand::Single(parse_query_value(raw.trim())?))
}

/// Validates and normalizes regex operand.
fn parse_regex_pattern(raw: &str) -> Result<String, BrokerError> {
    let pattern = if raw.starts_with('"') {
        serde_json::from_str::<String>(raw)
            .map_err(|error| BrokerError::BadRequest(format!("invalid q regex {raw}: {error}")))?
    } else {
        raw.to_string()
    };

    Regex::new(&pattern)
        .map_err(|error| BrokerError::BadRequest(format!("invalid q regex {pattern}: {error}")))?;
    Ok(pattern)
}

/// Parses top-level `scopeQ` OR branches.
fn parse_scope_or(expression: &str) -> Result<ScopeExpression, BrokerError> {
    let expression = expression.trim();
    let parts = split_top_level(expression, &['|', ',']);
    if parts.len() > 1 {
        Ok(ScopeExpression::Or(
            parts
                .into_iter()
                .map(parse_scope_branch)
                .collect::<Result<Vec<_>, _>>()?,
        ))
    } else {
        parse_scope_branch(expression)
    }
}

/// Parses one `scopeQ` branch or grouped conjunction.
fn parse_scope_branch(expression: &str) -> Result<ScopeExpression, BrokerError> {
    let expression = expression.trim();
    if expression == "/#" {
        return Ok(ScopeExpression::AnyNonEmpty);
    }

    if is_wrapped_by_parentheses(expression) {
        let inner = &expression[1..expression.len() - 1];
        let patterns = split_top_level(inner, &[';'])
            .into_iter()
            .map(parse_scope_pattern)
            .collect::<Result<Vec<_>, _>>()?;
        if patterns.len() == 1 {
            Ok(ScopeExpression::Pattern(
                patterns.into_iter().next().unwrap(),
            ))
        } else {
            Ok(ScopeExpression::And(patterns))
        }
    } else {
        if split_top_level(expression, &[';']).len() > 1 {
            return Err(BrokerError::BadRequest(
                "scopeQ conjunctions require parentheses".to_string(),
            ));
        }
        Ok(ScopeExpression::Pattern(parse_scope_pattern(expression)?))
    }
}

/// Parses one concrete `scopeQ` pattern.
fn parse_scope_pattern(raw: &str) -> Result<ScopePattern, BrokerError> {
    if raw == "/#" {
        return Ok(ScopePattern {
            levels: Vec::new(),
            include_descendants: true,
        });
    }

    let (raw, include_descendants) = if let Some(raw) = raw.strip_suffix("/#") {
        (raw, true)
    } else {
        (raw, false)
    };

    if !raw.starts_with('/') {
        return Err(BrokerError::BadRequest(format!(
            "invalid scopeQ scope {raw}"
        )));
    }

    let levels = raw
        .split('/')
        .skip(1)
        .map(str::trim)
        .filter(|level| !level.is_empty())
        .map(|level| {
            if level == "+" {
                Ok(ScopeLevel::SingleLevelWildcard)
            } else {
                validate_name(level, "scope")?;
                Ok(ScopeLevel::Exact(level.to_string()))
            }
        })
        .collect::<Result<Vec<_>, BrokerError>>()?;

    if levels.is_empty() {
        return Err(BrokerError::BadRequest(format!(
            "invalid scopeQ scope {raw}"
        )));
    }

    Ok(ScopePattern {
        levels,
        include_descendants,
    })
}

/// Validates attribute, relation, and scope segment names.
fn validate_name(value: &str, label: &str) -> Result<(), BrokerError> {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return Err(BrokerError::BadRequest(format!("invalid {label} name")));
    };
    if !first.is_alphabetic() {
        return Err(BrokerError::BadRequest(format!(
            "invalid {label} name {value}"
        )));
    }
    if chars.all(|ch| ch.is_alphanumeric() || ch == '_') {
        Ok(())
    } else {
        Err(BrokerError::BadRequest(format!(
            "invalid {label} name {value}"
        )))
    }
}

/// Resolves attribute path to candidate values from root entity payload.
fn resolve_query_attribute<'a>(
    root: &'a Value,
    attribute: &QueryAttribute,
    options: &'a QueryMatchOptions,
) -> Vec<&'a Value> {
    match attribute {
        QueryAttribute::ValuePath { segments, .. } => resolve_path_nodes(root, segments)
            .into_iter()
            .flat_map(expand_attribute_instances)
            .collect(),
        QueryAttribute::LinkedEntity {
            relation,
            entity_types,
            attribute,
        } => resolve_path_nodes(root, std::slice::from_ref(relation))
            .into_iter()
            .flat_map(expand_attribute_instances)
            .filter(|candidate| relationship_like(attribute_kind(candidate)))
            .flat_map(|candidate| linked_entity_targets(candidate, entity_types, options))
            .flat_map(|linked| resolve_query_attribute(linked, attribute, options))
            .collect(),
    }
}

/// Walks object and array nodes across dotted path segments.
fn resolve_path_nodes<'a>(root: &'a Value, segments: &[String]) -> Vec<&'a Value> {
    segments.iter().fold(vec![root], |current, segment| {
        current
            .into_iter()
            .flat_map(|candidate| match candidate {
                Value::Object(object) => object.get(segment).into_iter().collect::<Vec<_>>(),
                Value::Array(items) => items
                    .iter()
                    .filter_map(|item| item.as_object().and_then(|object| object.get(segment)))
                    .collect::<Vec<_>>(),
                _ => Vec::new(),
            })
            .collect()
    })
}

/// Splits multi-instance attributes into per-instance values.
fn expand_attribute_instances(value: &Value) -> Vec<&Value> {
    match value {
        Value::Array(items)
            if items
                .iter()
                .all(|item| item.is_object() && item.get("type").is_some()) =>
        {
            items.iter().collect()
        }
        _ => vec![value],
    }
}

/// Resolves relationship targets into linked entity payloads.
fn linked_entity_targets<'a>(
    relationship: &'a Value,
    entity_types: &[String],
    options: &'a QueryMatchOptions,
) -> Vec<&'a Value> {
    let Some(object) = relationship.as_object() else {
        return Vec::new();
    };

    let targets = match object.get("object") {
        Some(Value::Object(_)) => object.get("object").into_iter().collect::<Vec<_>>(),
        Some(Value::String(id)) => options
            .linked_entities
            .get(id)
            .into_iter()
            .collect::<Vec<_>>(),
        Some(Value::Array(items)) => items
            .iter()
            .flat_map(|item| match item {
                Value::Object(_) => Some(item),
                Value::String(id) => options.linked_entities.get(id),
                _ => None,
            })
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    };

    targets
        .into_iter()
        .filter(|target| {
            entity_types.is_empty()
                || entity_type_matches(target.get("type"), object.get("objectType"), entity_types)
        })
        .collect()
}

/// Checks whether linked entity type matches requested type filters.
fn entity_type_matches(
    inline_type: Option<&Value>,
    object_type: Option<&Value>,
    expected_types: &[String],
) -> bool {
    expected_types.iter().any(|expected| {
        value_matches_string(inline_type, expected) || value_matches_string(object_type, expected)
    })
}

/// Matches string or string-array value against expected text.
fn value_matches_string(value: Option<&Value>, expected: &str) -> bool {
    match value {
        Some(Value::String(actual)) => actual == expected,
        Some(Value::Array(items)) => items.iter().any(|item| item.as_str() == Some(expected)),
        _ => false,
    }
}

/// Evaluates equality, list membership, or range match for candidate.
fn equality_matches(
    candidate: &Value,
    attribute: &QueryAttribute,
    operand: &EqualityOperand,
    options: &QueryMatchOptions,
) -> bool {
    match operand {
        EqualityOperand::Single(expected) => {
            let expected = coerce_expected_value(attribute, expected, options);
            target_values(candidate, attribute, options)
                .iter()
                .any(|target| eq_value(target, &expected))
        }
        EqualityOperand::List(expected) => {
            let expected = expected
                .iter()
                .map(|value| coerce_expected_value(attribute, value, options))
                .collect::<Vec<_>>();
            target_values(candidate, attribute, options)
                .iter()
                .any(|target| expected.iter().any(|expected| eq_value(target, expected)))
        }
        EqualityOperand::Range(lower, upper) => {
            let lower = coerce_expected_value(attribute, lower, options);
            let upper = coerce_expected_value(attribute, upper, options);
            target_values(candidate, attribute, options)
                .iter()
                .any(|target| range_contains(target, &lower, &upper))
        }
    }
}

/// Compares resolved attribute values against one expected value.
fn compare_query_attribute<F>(
    root: &Value,
    attribute: &QueryAttribute,
    expected: &Value,
    options: &QueryMatchOptions,
    predicate: F,
) -> bool
where
    F: Fn(Ordering) -> bool,
{
    let expected = coerce_expected_value(attribute, expected, options);
    resolve_query_attribute(root, attribute, options)
        .iter()
        .any(|candidate| {
            let kind = attribute_kind(candidate);
            if relationship_like(kind) {
                return false;
            }

            flattened_values(target_values(candidate, attribute, options))
                .into_iter()
                .filter_map(|target| compare_query_values(&target, &expected))
                .any(&predicate)
        })
}

/// Matches regex against resolved target values.
fn pattern_matches(
    candidate: &Value,
    attribute: &QueryAttribute,
    regex: &Regex,
    options: &QueryMatchOptions,
) -> bool {
    target_values(candidate, attribute, options)
        .iter()
        .any(|target| match target {
            Value::String(text) => regex.is_match(text),
            Value::Array(items) => items
                .iter()
                .filter_map(Value::as_str)
                .any(|text| regex.is_match(text)),
            _ => false,
        })
}

/// Matches when all resolved target values fail regex.
fn not_pattern_matches(
    candidate: &Value,
    attribute: &QueryAttribute,
    regex: &Regex,
    options: &QueryMatchOptions,
) -> bool {
    let targets = target_values(candidate, attribute, options);
    !targets.is_empty()
        && targets.iter().all(|target| match target {
            Value::String(text) => !regex.is_match(text),
            Value::Array(items) if items.iter().all(Value::is_string) => items
                .iter()
                .filter_map(Value::as_str)
                .all(|text| !regex.is_match(text)),
            _ => false,
        })
}

/// Returns comparable target values for attribute, applying trailing selectors.
fn target_values(
    candidate: &Value,
    attribute: &QueryAttribute,
    options: &QueryMatchOptions,
) -> Vec<Value> {
    let trailing_path = attribute.trailing_path();
    match trailing_path {
        Some(trailing_path) => {
            target_values_with_trailing(candidate, attribute, trailing_path, options)
        }
        None => target_values_without_trailing(candidate, attribute, options),
    }
}

/// Returns attribute values without trailing member traversal.
fn target_values_without_trailing(
    candidate: &Value,
    attribute: &QueryAttribute,
    options: &QueryMatchOptions,
) -> Vec<Value> {
    match attribute_kind(candidate) {
        AttributeKind::Raw => vec![candidate.clone()],
        AttributeKind::Property | AttributeKind::GeoProperty => {
            coerce_property_values(candidate, attribute, options)
        }
        AttributeKind::Relationship | AttributeKind::ListRelationship => {
            candidate.get("object").cloned().into_iter().collect()
        }
        AttributeKind::LanguageProperty => language_map_values(candidate.get("languageMap")),
        AttributeKind::VocabProperty => coerce_vocab_values(candidate, attribute, options),
        AttributeKind::ListProperty => candidate.get("valueList").cloned().into_iter().collect(),
        AttributeKind::JsonProperty => coerce_json_values(candidate, attribute, options),
    }
}

/// Traverses trailing members after initial attribute value extraction.
fn target_values_with_trailing(
    candidate: &Value,
    attribute: &QueryAttribute,
    trailing_path: &TrailingPath,
    options: &QueryMatchOptions,
) -> Vec<Value> {
    match (attribute_kind(candidate), trailing_path) {
        (AttributeKind::LanguageProperty, TrailingPath::AnyLanguage) => {
            language_map_values(candidate.get("languageMap"))
        }
        (AttributeKind::LanguageProperty, TrailingPath::Members(path)) => {
            let Some(language_map) = candidate.get("languageMap").and_then(Value::as_object) else {
                return Vec::new();
            };
            let Some((language, rest)) = path.split_first() else {
                return Vec::new();
            };
            let seeds = if language == "*" {
                language_map.values().collect::<Vec<_>>()
            } else {
                language_map.get(language).into_iter().collect::<Vec<_>>()
            };

            if rest.is_empty() {
                seeds.into_iter().flat_map(flatten_language_value).collect()
            } else {
                seeds
                    .into_iter()
                    .flat_map(|seed| traverse_member_path(seed, rest))
                    .collect()
            }
        }
        (_, TrailingPath::AnyLanguage) => Vec::new(),
        (kind, TrailingPath::Members(path)) => {
            seed_target_values(candidate, kind, attribute, options)
                .into_iter()
                .flat_map(|seed| traverse_member_path(&seed, path))
                .collect()
        }
    }
}

/// Produces intermediate traversal seeds for trailing path resolution.
fn seed_target_values(
    candidate: &Value,
    kind: AttributeKind,
    attribute: &QueryAttribute,
    options: &QueryMatchOptions,
) -> Vec<Value> {
    match kind {
        AttributeKind::Raw => vec![candidate.clone()],
        AttributeKind::Property | AttributeKind::GeoProperty => {
            coerce_property_values(candidate, attribute, options)
        }
        AttributeKind::Relationship | AttributeKind::ListRelationship => {
            candidate.get("object").cloned().into_iter().collect()
        }
        AttributeKind::LanguageProperty => {
            candidate.get("languageMap").cloned().into_iter().collect()
        }
        AttributeKind::VocabProperty => coerce_vocab_values(candidate, attribute, options),
        AttributeKind::ListProperty => candidate.get("valueList").cloned().into_iter().collect(),
        AttributeKind::JsonProperty => coerce_json_values(candidate, attribute, options),
    }
}

/// Extracts property values, expanding compacted terms when requested.
fn coerce_property_values(
    candidate: &Value,
    attribute: &QueryAttribute,
    options: &QueryMatchOptions,
) -> Vec<Value> {
    let Some(attr_name) = attribute.root_name() else {
        return candidate.get("value").cloned().into_iter().collect();
    };

    if options.json_keys.contains(attr_name) {
        if let Some(json) = candidate.get("json").cloned() {
            return vec![json];
        }
    }

    if options.expand_values.contains(attr_name)
        && let Some(value) = candidate.get("value")
    {
        return vec![expand_compacted_value(value, &options.context_terms)];
    }

    candidate.get("value").cloned().into_iter().collect()
}

/// Extracts vocab values, expanding compacted terms when requested.
fn coerce_vocab_values(
    candidate: &Value,
    attribute: &QueryAttribute,
    options: &QueryMatchOptions,
) -> Vec<Value> {
    let Some(vocab) = candidate.get("vocab") else {
        return Vec::new();
    };
    let Some(attr_name) = attribute.root_name() else {
        return vec![vocab.clone()];
    };

    if options.expand_values.contains(attr_name) {
        return vec![expand_compacted_value(vocab, &options.context_terms)];
    }

    vec![vocab.clone()]
}

/// Extracts JSON property values honoring `json_keys` behavior.
fn coerce_json_values(
    candidate: &Value,
    attribute: &QueryAttribute,
    options: &QueryMatchOptions,
) -> Vec<Value> {
    let Some(attr_name) = attribute.root_name() else {
        return candidate.get("json").cloned().into_iter().collect();
    };

    if options.json_keys.contains(attr_name) {
        candidate.get("json").cloned().into_iter().collect()
    } else {
        candidate
            .get("value")
            .or_else(|| candidate.get("json"))
            .cloned()
            .into_iter()
            .collect()
    }
}

/// Normalizes expected query value to match attribute coercion rules.
fn coerce_expected_value(
    attribute: &QueryAttribute,
    value: &Value,
    options: &QueryMatchOptions,
) -> Value {
    let Some(attr_name) = attribute.root_name() else {
        return value.clone();
    };

    if options.json_keys.contains(attr_name) {
        return value.clone();
    }

    if options.expand_values.contains(attr_name) {
        return expand_compacted_value(value, &options.context_terms);
    }

    value.clone()
}

/// Expands compacted terms recursively using request context aliases.
fn expand_compacted_value(value: &Value, context_terms: &HashMap<String, String>) -> Value {
    match value {
        Value::String(text) => context_terms
            .get(text)
            .cloned()
            .map(Value::String)
            .unwrap_or_else(|| value.clone()),
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|item| expand_compacted_value(item, context_terms))
                .collect(),
        ),
        _ => value.clone(),
    }
}

/// Traverses nested object members starting from seed value.
fn traverse_member_path(root: &Value, path: &[String]) -> Vec<Value> {
    path.iter()
        .fold(vec![root], |current, segment| {
            current
                .into_iter()
                .flat_map(|candidate| match candidate {
                    Value::Object(object) => object.get(segment).into_iter().collect::<Vec<_>>(),
                    Value::Array(items) => items
                        .iter()
                        .filter_map(|item| item.as_object().and_then(|object| object.get(segment)))
                        .collect::<Vec<_>>(),
                    _ => Vec::new(),
                })
                .collect()
        })
        .into_iter()
        .cloned()
        .collect()
}

/// Flattens all values stored in language map entries.
fn language_map_values(language_map: Option<&Value>) -> Vec<Value> {
    language_map
        .and_then(Value::as_object)
        .map(|entries| entries.values().flat_map(flatten_language_value).collect())
        .unwrap_or_default()
}

/// Flattens one language-map value into scalar list.
fn flatten_language_value(value: &Value) -> Vec<Value> {
    match value {
        Value::Array(items) => items.to_vec(),
        _ => vec![value.clone()],
    }
}

/// Flattens one level of arrays produced during target extraction.
fn flattened_values(values: Vec<Value>) -> Vec<Value> {
    let mut flattened = Vec::new();
    for value in values {
        match value {
            Value::Array(items) => flattened.extend(items),
            _ => flattened.push(value),
        }
    }
    flattened
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AttributeKind {
    Raw,
    Property,
    GeoProperty,
    Relationship,
    ListRelationship,
    LanguageProperty,
    VocabProperty,
    ListProperty,
    JsonProperty,
}

/// Classifies NGSI-LD attribute representation from payload shape.
fn attribute_kind(value: &Value) -> AttributeKind {
    match value.get("type").and_then(Value::as_str) {
        Some("Property") => AttributeKind::Property,
        Some("GeoProperty") => AttributeKind::GeoProperty,
        Some("Relationship") => AttributeKind::Relationship,
        Some("ListRelationship") => AttributeKind::ListRelationship,
        Some("LanguageProperty") => AttributeKind::LanguageProperty,
        Some("VocabProperty") => AttributeKind::VocabProperty,
        Some("ListProperty") => AttributeKind::ListProperty,
        Some("JsonProperty") => AttributeKind::JsonProperty,
        _ => AttributeKind::Raw,
    }
}

/// Reports whether attribute kind stores relationship targets.
fn relationship_like(kind: AttributeKind) -> bool {
    matches!(
        kind,
        AttributeKind::Relationship | AttributeKind::ListRelationship
    )
}

/// Matches scalar or array target against expected value.
fn eq_value(target: &Value, expected: &Value) -> bool {
    values_equal(target, expected)
        || matches!(target, Value::Array(items) if items.iter().any(|item| values_equal(item, expected)))
}

/// Checks whether target falls inside inclusive range bounds.
fn range_contains(target: &Value, lower: &Value, upper: &Value) -> bool {
    matches!(
        compare_query_values(target, lower),
        Some(Ordering::Greater | Ordering::Equal)
    ) && matches!(
        compare_query_values(target, upper),
        Some(Ordering::Less | Ordering::Equal)
    ) || matches!(target, Value::Array(items) if items.iter().any(|item| range_contains(item, lower, upper)))
}

/// Compares JSON values using query-language equality semantics.
fn values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => left.as_f64() == right.as_f64(),
        (Value::String(left), Value::String(right)) => left == right,
        (Value::Bool(left), Value::Bool(right)) => left == right,
        (Value::Array(_), Value::Array(_)) | (Value::Object(_), Value::Object(_)) => left == right,
        _ => false,
    }
}

#[derive(Clone, Copy)]
enum ComparableScalar<'a> {
    Number(f64),
    Bool(bool),
    DateTime(DateTime<FixedOffset>),
    Date(NaiveDate),
    Time(NaiveTime),
    Text(&'a str),
}

/// Compares two query values after scalar coercion.
fn compare_query_values(left: &Value, right: &Value) -> Option<Ordering> {
    let left = comparable_scalar(left)?;
    let right = comparable_scalar(right)?;
    match (left, right) {
        (ComparableScalar::Number(left), ComparableScalar::Number(right)) => {
            left.partial_cmp(&right)
        }
        (ComparableScalar::Bool(left), ComparableScalar::Bool(right)) => Some(left.cmp(&right)),
        (ComparableScalar::DateTime(left), ComparableScalar::DateTime(right)) => {
            Some(left.cmp(&right))
        }
        (ComparableScalar::Date(left), ComparableScalar::Date(right)) => Some(left.cmp(&right)),
        (ComparableScalar::Time(left), ComparableScalar::Time(right)) => Some(left.cmp(&right)),
        (ComparableScalar::Text(left), ComparableScalar::Text(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

/// Converts JSON value into comparable scalar used by range operators.
fn comparable_scalar(value: &Value) -> Option<ComparableScalar<'_>> {
    match value {
        Value::Number(number) => number.as_f64().map(ComparableScalar::Number),
        Value::Bool(flag) => Some(ComparableScalar::Bool(*flag)),
        Value::String(text) => {
            if let Ok(value) = DateTime::parse_from_rfc3339(text) {
                Some(ComparableScalar::DateTime(value))
            } else if let Ok(value) = NaiveDate::parse_from_str(text, "%Y-%m-%d") {
                Some(ComparableScalar::Date(value))
            } else if let Ok(value) = NaiveTime::parse_from_str(text, "%H:%M:%S") {
                Some(ComparableScalar::Time(value))
            } else if let Ok(value) = NaiveTime::parse_from_str(text, "%H:%M:%S%.f") {
                Some(ComparableScalar::Time(value))
            } else {
                Some(ComparableScalar::Text(text))
            }
        }
        _ => None,
    }
}

/// Matches one concrete scope string against parsed scope pattern.
fn scope_pattern_matches(scope: &str, pattern: &ScopePattern) -> bool {
    if pattern.levels.is_empty() {
        return !scope.trim().is_empty();
    }

    let levels = scope
        .split('/')
        .skip(1)
        .filter(|level| !level.is_empty())
        .collect::<Vec<_>>();
    if levels.is_empty() {
        return false;
    }

    if pattern.include_descendants {
        if levels.len() < pattern.levels.len() {
            return false;
        }
    } else if levels.len() != pattern.levels.len() {
        return false;
    }

    pattern
        .levels
        .iter()
        .zip(levels.iter())
        .all(|(expected, actual)| match expected {
            ScopeLevel::Exact(expected) => expected == actual,
            ScopeLevel::SingleLevelWildcard => true,
        })
}

/// Splits expression by separators while respecting nesting.
fn split_top_level<'a>(expression: &'a str, separators: &[char]) -> Vec<&'a str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut state = ScanState::default();

    for (index, ch) in expression.char_indices() {
        if state.is_top_level() && separators.contains(&ch) {
            parts.push(expression[start..index].trim());
            start = index + ch.len_utf8();
            continue;
        }
        state.step(ch);
    }

    parts.push(expression[start..].trim());
    parts
}

/// Splits expression once on top-level needle.
fn split_once_top_level<'a>(expression: &'a str, needle: &str) -> Option<(&'a str, &'a str)> {
    let mut state = ScanState::default();
    for (index, ch) in expression.char_indices() {
        if state.is_top_level() && expression[index..].starts_with(needle) {
            return Some((&expression[..index], &expression[index + needle.len()..]));
        }
        state.step(ch);
    }
    None
}

/// Finds first top-level query operator in expression.
fn find_top_level_operator(expression: &str) -> Option<(usize, &'static str)> {
    const OPERATORS: [&str; 8] = ["!~=", "~=", ">=", "<=", "!=", "==", ">", "<"];
    let mut state = ScanState::default();
    for (index, ch) in expression.char_indices() {
        if state.is_top_level()
            && let Some(operator) = OPERATORS
                .iter()
                .copied()
                .find(|operator| expression[index..].starts_with(operator))
        {
            return Some((index, operator));
        }
        state.step(ch);
    }
    None
}

/// Finds first top-level occurrence of character.
fn find_top_level_char(expression: &str, needle: char) -> Option<usize> {
    let mut state = ScanState::default();
    for (index, ch) in expression.char_indices() {
        if state.is_top_level() && ch == needle {
            return Some(index);
        }
        state.step(ch);
    }
    None
}

/// Finds closing delimiter matching given opening delimiter.
fn find_matching_delimiter(
    expression: &str,
    open_index: usize,
    open: char,
    close: char,
) -> Option<usize> {
    let mut depth = 0_i32;
    let mut in_string = false;
    let mut escaped = false;
    for (index, ch) in expression[open_index..].char_indices() {
        let absolute_index = open_index + index;
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }

        if ch == '"' {
            in_string = true;
            continue;
        }

        if ch == open {
            depth += 1;
        } else if ch == close {
            depth -= 1;
            if depth == 0 {
                return Some(absolute_index);
            }
        }
    }
    None
}

/// Removes redundant wrapping parentheses around expression.
fn strip_wrapping_parentheses(expression: &str) -> &str {
    let mut expression = expression.trim();
    while is_wrapped_by_parentheses(expression) {
        expression = expression[1..expression.len() - 1].trim();
    }
    expression
}

/// Reports whether expression is fully wrapped by one parenthesis pair.
fn is_wrapped_by_parentheses(expression: &str) -> bool {
    if !expression.starts_with('(') || !expression.ends_with(')') {
        return false;
    }

    let mut state = ScanState::default();
    for (index, ch) in expression.char_indices() {
        state.step(ch);
        if state.is_top_level() {
            return index + ch.len_utf8() == expression.len();
        }
    }

    false
}

#[derive(Default)]
struct ScanState {
    paren_depth: i32,
    brace_depth: i32,
    bracket_depth: i32,
    in_string: bool,
    escaped: bool,
}

impl ScanState {
    /// Reports whether scanner currently sits at top level.
    fn is_top_level(&self) -> bool {
        !self.in_string && self.paren_depth == 0 && self.brace_depth == 0 && self.bracket_depth == 0
    }

    /// Advances nesting state for one scanned character.
    fn step(&mut self, ch: char) {
        if self.in_string {
            if self.escaped {
                self.escaped = false;
            } else if ch == '\\' {
                self.escaped = true;
            } else if ch == '"' {
                self.in_string = false;
            }
            return;
        }

        match ch {
            '"' => self.in_string = true,
            '(' => self.paren_depth += 1,
            ')' => self.paren_depth -= 1,
            '{' => self.brace_depth += 1,
            '}' => self.brace_depth -= 1,
            '[' => self.bracket_depth += 1,
            ']' => self.bracket_depth -= 1,
            _ => {}
        }
    }
}

impl QueryAttribute {
    /// Returns trailing member selector associated with attribute.
    fn trailing_path(&self) -> Option<&TrailingPath> {
        match self {
            QueryAttribute::ValuePath { trailing_path, .. } => trailing_path.as_ref(),
            QueryAttribute::LinkedEntity { attribute, .. } => attribute.trailing_path(),
        }
    }

    /// Returns root attribute name used for option lookups.
    fn root_name(&self) -> Option<&str> {
        match self {
            QueryAttribute::ValuePath { segments, .. } => segments.first().map(String::as_str),
            QueryAttribute::LinkedEntity { attribute, .. } => attribute.root_name(),
        }
    }
}

/// Reports whether attribute path includes linked-entity traversal.
fn attribute_uses_linked_entity(attribute: &QueryAttribute) -> bool {
    match attribute {
        QueryAttribute::ValuePath { .. } => false,
        QueryAttribute::LinkedEntity { .. } => true,
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use serde_json::json;

    use super::*;

    #[test]
    fn parses_and_evaluates_grouped_query() -> Result<(), BrokerError> {
        let expression = parse_q_expression(Some(
            "(speed==20,30|brandName~=^Mer.*$);address[city]==\"Berlin\"",
        ))?
        .expect("expression");
        let entity = json!({
            "speed": {"type": "Property", "value": 30},
            "brandName": {"type": "Property", "value": "Mercedes"},
            "address": {"type": "Property", "value": {"city": "Berlin"}}
        });

        assert!(q_expression_matches_value(&expression, &entity));
        Ok(())
    }

    #[test]
    fn evaluates_relationship_and_language_property_queries() -> Result<(), BrokerError> {
        let relationship =
            parse_q_expression(Some("isParked==urn:ngsi-ld:OffStreetParking:Downtown1"))?
                .expect("relationship");
        let language = parse_q_expression(Some("color[*]==\"red\""))?.expect("language");

        let entity = json!({
            "isParked": {
                "type": "Relationship",
                "object": "urn:ngsi-ld:OffStreetParking:Downtown1"
            },
            "color": {
                "type": "LanguageProperty",
                "languageMap": {
                    "fr": "rouge",
                    "en": ["red", "bright"]
                }
            }
        });

        assert!(q_expression_matches_value(&relationship, &entity));
        assert!(q_expression_matches_value(&language, &entity));
        Ok(())
    }

    #[test]
    fn parses_scope_query_language() -> Result<(), BrokerError> {
        let scopes = vec!["/Madrid/Gardens/ParqueNorte".to_string()];

        assert!(scope_query_matches_values(
            &scopes,
            Some("/Madrid/Gardens/#")
        )?);
        assert!(scope_query_matches_values(
            &scopes,
            Some("/Madrid/+/ParqueNorte")
        )?);
        assert!(scope_query_matches_values(&scopes, Some("/#"))?);
        assert!(!scope_query_matches_values(&scopes, Some("/Madrid"))?);
        assert!(!scope_query_matches_values(
            &scopes,
            Some("(/Madrid/Gardens;/CompanyA)")
        )?);

        Ok(())
    }

    #[test]
    fn expands_values_using_context_terms() -> Result<(), BrokerError> {
        let expression =
            parse_q_expression(Some("brandCode==https://example.org/brands/Mercedes"))?
                .expect("expression");
        let entity = json!({
            "brandCode": {"type": "Property", "value": "MercedesBrand"}
        });
        let options = QueryMatchOptions {
            expand_values: ["brandCode".to_string()].into_iter().collect(),
            context_terms: HashMap::from([(
                "MercedesBrand".to_string(),
                "https://example.org/brands/Mercedes".to_string(),
            )]),
            ..Default::default()
        };

        assert!(!q_expression_matches_value(&expression, &entity));
        assert!(q_expression_matches_value_with_options(
            &expression,
            &entity,
            &options
        ));
        Ok(())
    }

    #[test]
    fn uses_json_keys_for_trailing_json_members() -> Result<(), BrokerError> {
        let expression = parse_q_expression(Some("metadata[nested]==1"))?.expect("expression");
        let entity = json!({
            "metadata": {
                "type": "JsonProperty",
                "value": "opaque",
                "json": {"nested": 1}
            }
        });
        let options = QueryMatchOptions {
            json_keys: ["metadata".to_string()].into_iter().collect(),
            ..Default::default()
        };

        assert!(!q_expression_matches_value(&expression, &entity));
        assert!(q_expression_matches_value_with_options(
            &expression,
            &entity,
            &options
        ));
        Ok(())
    }

    #[test]
    fn resolves_linked_entities_from_relationship_object_ids() -> Result<(), BrokerError> {
        let expression =
            parse_q_expression(Some("sensor{Device:humidity}==40"))?.expect("expression");
        let entity = json!({
            "sensor": {
                "type": "Relationship",
                "object": "urn:ngsi-ld:Device:1"
            }
        });
        let options = QueryMatchOptions {
            linked_entities: Arc::new(HashMap::from([(
                "urn:ngsi-ld:Device:1".to_string(),
                json!({
                    "id": "urn:ngsi-ld:Device:1",
                    "type": "Device",
                    "humidity": {"type": "Property", "value": 40}
                }),
            )])),
            ..Default::default()
        };

        assert!(q_expression_matches_value_with_options(
            &expression,
            &entity,
            &options
        ));
        Ok(())
    }
}
