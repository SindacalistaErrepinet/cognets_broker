use serde_json::{Map, Value};

/// Applies RFC7386-style JSON merge patch in place.
pub fn apply_merge_patch(target: &mut Value, patch: &Value) {
    match patch {
        Value::Object(patch_object) => {
            if !target.is_object() {
                *target = Value::Object(Map::new());
            }

            let target_object = target.as_object_mut().expect("target object");
            for (key, value) in patch_object {
                if value.is_null() {
                    target_object.remove(key);
                } else {
                    apply_merge_patch(
                        target_object.entry(key.clone()).or_insert(Value::Null),
                        value,
                    );
                }
            }
        }
        _ => *target = patch.clone(),
    }
}

/// Parses comma-separated query string into trimmed values.
pub fn parse_csv(value: Option<&str>) -> Vec<String> {
    value
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|entry| !entry.is_empty())
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// Returns true for NGSI-LD reserved top-level members.
pub fn reserved_member(key: &str) -> bool {
    matches!(
        key,
        "id" | "type" | "scope" | "@context" | "createdAt" | "modifiedAt" | "deletedAt"
    )
}

/// Returns editable fragment members excluding reserved fields.
pub fn editable_fragment_members(fragment: &Value) -> Map<String, Value> {
    fragment
        .as_object()
        .map(|object| {
            object
                .iter()
                .filter(|(key, _)| !reserved_member(key))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect()
        })
        .unwrap_or_default()
}

/// Returns attribute names from entity excluding reserved fields.
pub fn entity_attribute_names(entity: &Value) -> Vec<String> {
    entity
        .as_object()
        .map(|object| {
            object
                .keys()
                .filter(|key| !reserved_member(key))
                .cloned()
                .collect()
        })
        .unwrap_or_default()
}
