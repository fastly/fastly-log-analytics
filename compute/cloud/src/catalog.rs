use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::sync::OnceLock;

pub const LOG_LINE_MAX_BYTES: i64 = 16_384;
pub const LOG_FORMAT_MAX_BYTES: usize = 12_288;

pub(crate) fn data() -> &'static Value {
    static DATA: OnceLock<Value> = OnceLock::new();
    DATA.get_or_init(|| {
        serde_json::from_str(include_str!("logging/catalog.json"))
            .expect("checked-in v3 catalog must be valid JSON")
    })
}

pub fn get_catalog_for_api(field_limits: Option<&Value>) -> Value {
    let mut fields = data()["api_fields"].clone();
    if let Some(fields) = fields.as_array_mut() {
        for field in fields {
            if field["has_limit"] == true {
                if let Some(limit) = field_limits.and_then(|v| v.get(field["id"].as_str()?)) {
                    field["limit"] = limit.clone();
                }
            }
        }
    }
    fields
}

pub fn get_groups_for_api() -> Value {
    data()["groups"].clone()
}

pub fn get_insights_for_api() -> Value {
    data()["insights"].clone()
}

pub fn get_presets_for_api() -> Value {
    data()["presets"].clone()
}

pub fn resolve_enabled_fields(config: &Value) -> BTreeSet<String> {
    let mut groups: BTreeSet<String> = config["groups"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect();
    if config.is_null() {
        groups.extend(["A", "C", "D", "I"].map(str::to_owned));
    }
    for (group, required) in [("E", "D"), ("G", "F")] {
        if groups.contains(group) {
            groups.insert(required.to_owned());
        }
    }
    let mut enabled = BTreeSet::new();
    for field in data()["fields"].as_array().into_iter().flatten() {
        let Some(id) = field["id"].as_str() else {
            continue;
        };
        if field["group"].is_null()
            || (field["group"].as_str().is_some_and(|g| groups.contains(g))
                && field["default_off"] != true)
        {
            enabled.insert(id.to_owned());
        }
    }
    for (id, on) in config["field_overrides"].as_object().into_iter().flatten() {
        if id == "cookie_session" {
            continue;
        }
        if on == true {
            enabled.insert(id.clone());
        } else {
            enabled.remove(id);
        }
    }
    enabled
}

pub fn get_required_edge_headers(config: &Value) -> BTreeSet<String> {
    let enabled = resolve_enabled_fields(config);
    let mut required = BTreeSet::new();
    for field in data()["fields"].as_array().into_iter().flatten() {
        if !field["id"].as_str().is_some_and(|id| enabled.contains(id)) {
            continue;
        }
        for tail in field["vcl"]
            .as_str()
            .unwrap_or("")
            .split("req.http.x-fos-edge-data:")
            .skip(1)
        {
            let key: String = tail
                .chars()
                .take_while(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == '_')
                .collect();
            if !key.is_empty() {
                required.insert(key);
            }
        }
    }
    required
}

pub(crate) fn integer(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| {
        value
            .as_str()
            .and_then(|s| s.trim().parse().ok())
            .or_else(|| value.as_bool().map(i64::from))
    })
}

pub fn normalize_custom_field(field: &Value) -> Value {
    let mut result = field.clone();
    if field.get("vcl").is_none() && field.get("parquet_type").is_none() {
        return result;
    }
    let Some(object) = result.as_object_mut() else {
        return result;
    };
    if let Some(vcl) = field.get("vcl") {
        object.insert("vcl_log_expression".into(), vcl.clone());
    }
    if let Some(kind) = field["parquet_type"].as_str() {
        let (duck, value) = match kind {
            "Utf8" | "VARCHAR" => ("VARCHAR", "string"),
            "Int64" | "BIGINT" => ("BIGINT", "numeric"),
            "INTEGER" => ("INTEGER", "numeric"),
            "Float64" | "DOUBLE" => ("DOUBLE", "numeric"),
            "Boolean" | "BOOLEAN" => ("BOOLEAN", "boolean"),
            _ => (kind, ""),
        };
        object.insert("duckdb_type".into(), json!(duck));
        object.entry("value_type").or_insert(json!(value));
    }
    object
        .entry("label")
        .or_insert_with(|| field["name"].clone());
    object.entry("bytes_estimate").or_insert(json!(40));
    result
}

pub fn validate_custom_field(field: &Value, existing_names: &[String]) -> Vec<String> {
    let field = normalize_custom_field(field);
    let mut errors = Vec::new();
    for key in [
        "name",
        "label",
        "vcl_log_expression",
        "duckdb_type",
        "value_type",
        "bytes_estimate",
    ] {
        if field.get(key).is_none_or(Value::is_null) {
            errors.push(format!("Missing required field: '{key}'"));
        }
    }
    if !errors.is_empty() {
        return errors;
    }
    let name = field["name"].as_str().unwrap_or("");
    if crate::logging::cmcd::reserved_name(name) {
        errors.push("CMCD field names are reserved for system-managed capture".into());
    }
    if name.is_empty()
        || name.len() > 48
        || !name.as_bytes()[0].is_ascii_lowercase()
        || !name
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_')
    {
        errors.push("Field name must be lowercase alphanumeric + underscore, start with a letter, 1–48 chars".into());
    }
    if field
        .get("show_in_dashboard")
        .is_some_and(|v| !v.is_boolean())
    {
        errors.push("show_in_dashboard must be a boolean".into());
    }
    if data()["reserved"]
        .as_array()
        .is_some_and(|v| v.iter().any(|v| v == name))
    {
        errors.push(format!(
            "'{name}' is a DuckDB/SQL reserved word and cannot be used as a field name"
        ));
    }
    if data()["fields"]
        .as_array()
        .is_some_and(|v| v.iter().any(|v| v["id"] == name))
        || name == "cookie_session"
    {
        errors.push(format!("'{name}' is already a built-in field name"));
    }
    if existing_names.iter().any(|n| n == name) {
        errors.push(format!(
            "A custom field named '{name}' already exists on this service"
        ));
    }
    let label_length = field["label"].as_str().unwrap_or("").chars().count();
    if !(1..=80).contains(&label_length) {
        errors.push("'label' must be 1–80 characters".into());
    }
    if field["description"].as_str().unwrap_or("").chars().count() > 500 {
        errors.push("'description' must not exceed 500 characters".into());
    }
    let expression = field["vcl_log_expression"].as_str().unwrap_or("");
    if expression.trim().is_empty() {
        errors.push("'vcl_log_expression' must not be empty".into());
    } else if expression.chars().count() > 512 {
        errors.push(format!(
            "'vcl_log_expression' must be ≤ 512 characters (got {})",
            expression.chars().count()
        ));
    } else if expression.contains('\n') {
        errors.push("'vcl_log_expression' must not contain raw newlines".into());
    } else {
        if expression.contains(';') {
            errors.push("'vcl_log_expression' must not contain semicolons (;)".into());
        }
        if expression.contains("//") || expression.contains("/*") || expression.contains('#') {
            errors.push("'vcl_log_expression' must not contain VCL comments (//, /*, or #)".into());
        }
    }
    let kind = field["duckdb_type"].as_str().unwrap_or("");
    let compatible: &[&str] = match kind {
        "VARCHAR" => &["ip", "string", "url"],
        "INTEGER" | "BIGINT" | "DOUBLE" => &["numeric"],
        "BOOLEAN" => &["boolean"],
        _ => &[],
    };
    if compatible.is_empty() {
        errors
            .push("'duckdb_type' must be one of: BIGINT, BOOLEAN, DOUBLE, INTEGER, VARCHAR".into());
    }
    let value_type = field["value_type"].as_str().unwrap_or("");
    if !["string", "numeric", "boolean", "ip", "url"].contains(&value_type) {
        errors.push("'value_type' must be one of: boolean, ip, numeric, string, url".into());
    } else if !compatible.is_empty() && !compatible.contains(&value_type) {
        errors.push(format!("'value_type' '{value_type}' is not compatible with 'duckdb_type' '{kind}'. Compatible value_types: {}", compatible.join(", ")));
    }
    let estimate = integer(&field["bytes_estimate"]);
    match estimate {
        Some(1..=1024) => {}
        Some(_) => errors.push("'bytes_estimate' must be between 1 and 1024".into()),
        None => errors.push("'bytes_estimate' must be an integer".into()),
    }
    let stage = field["collection_stage"].as_str().unwrap_or("edge");
    if stage != "edge" && stage != "origin" {
        errors.push(format!(
            "'collection_stage' must be 'edge' or 'origin' (got '{stage}')"
        ));
    }
    if let Some(estimate) = estimate {
        let minimum = name.chars().count() as i64 + 5;
        if estimate >= 1 && estimate < minimum {
            errors.push(format!("WARN: 'bytes_estimate' ({estimate}) is less than the field name overhead ({minimum} bytes for \"{name}\":). The estimate is likely too low."));
        }
    }
    errors
}

pub fn estimate_log_line_bytes(config: &Value) -> i64 {
    let enabled = resolve_enabled_fields(config);
    let fields = data()["fields"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|f| f["id"].as_str().is_some_and(|id| enabled.contains(id)))
        .map(|f| f["typical_bytes"].as_i64().unwrap_or(0))
        .sum::<i64>();
    let custom: Vec<&Value> = config["custom_fields"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|f| f["enabled"] != false)
        .collect();
    2 + (enabled.len() + custom.len()) as i64 * 5
        + fields
        + custom
            .iter()
            .map(|f| integer(&normalize_custom_field(f)["bytes_estimate"]).unwrap_or(0))
            .sum::<i64>()
}

/// fla v3 `custom_dashboard_cards`: enabled custom fields marked `show_in_dashboard`, in name
/// order, with the value type the dashboard restores from summaries. CMCD, RUM, edge and
/// request-id fields are system-managed and never get a card.
pub fn dashboard_cards(log_fields: &Value) -> Value {
    let fields: Vec<&Value> = log_fields["custom_fields"]
        .as_array()
        .into_iter()
        .flatten()
        .collect();
    let cards: Vec<Value> = crate::ingest::convert::custom_columns(log_fields)
        .into_iter()
        .filter(|column| {
            let name = column.name.as_str();
            name != "fastly_req_id"
                && !["cmcd_", "edge_", "rum_"]
                    .iter()
                    .any(|prefix| name.starts_with(prefix))
        })
        .filter_map(|column| {
            let field = fields.iter().find(|f| f["name"] == column.name.as_str())?;
            if field["enabled"] == false
                || field["show_in_dashboard"] != true
                || field["system_managed"] == true
            {
                return None;
            }
            let kind = match column.kind {
                crate::ingest::convert::Kind::Int64 | crate::ingest::convert::Kind::Float64 => {
                    "number"
                }
                crate::ingest::convert::Kind::Bool => "boolean",
                _ => "string",
            };
            let label = field["label"]
                .as_str()
                .filter(|label| !label.trim().is_empty())
                .unwrap_or(&column.name);
            Some(json!({"id": column.name, "label": label, "type": kind}))
        })
        .collect();
    Value::Array(cards)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dashboard_cards_list_enabled_marked_custom_fields_only() {
        let fields = json!({"custom_fields": [
            {"name": "tenant", "vcl": "x", "parquet_type": "VARCHAR", "enabled": true,
             "show_in_dashboard": true, "label": "Tenant"},
            {"name": "score", "vcl": "x", "parquet_type": "BIGINT", "show_in_dashboard": true},
            {"name": "ratio", "vcl": "x", "parquet_type": "DOUBLE", "show_in_dashboard": true},
            {"name": "flag", "vcl": "x", "parquet_type": "BOOLEAN", "show_in_dashboard": true},
            {"name": "hidden", "vcl": "x", "parquet_type": "VARCHAR", "enabled": true},
            {"name": "off", "vcl": "x", "parquet_type": "VARCHAR", "enabled": false,
             "committed": true, "show_in_dashboard": true},
            {"name": "cmcd_br", "vcl": "x", "parquet_type": "BIGINT", "show_in_dashboard": true},
            {"name": "rum_lcp", "vcl": "x", "parquet_type": "DOUBLE", "show_in_dashboard": true},
            {"name": "edge_x", "vcl": "x", "parquet_type": "VARCHAR", "show_in_dashboard": true},
            {"name": "fastly_req_id", "vcl": "x", "parquet_type": "VARCHAR",
             "show_in_dashboard": true},
            {"name": "managed", "vcl": "x", "parquet_type": "VARCHAR", "system_managed": true,
             "show_in_dashboard": true},
            {"name": "host", "vcl": "x", "parquet_type": "VARCHAR", "show_in_dashboard": true},
        ]});
        assert_eq!(
            dashboard_cards(&fields),
            json!([
                {"id": "flag", "label": "flag", "type": "boolean"},
                {"id": "ratio", "label": "ratio", "type": "number"},
                {"id": "score", "label": "score", "type": "number"},
                {"id": "tenant", "label": "Tenant", "type": "string"},
            ])
        );
        assert_eq!(dashboard_cards(&json!({})), json!([]));
        let field = json!({"name": "tenant", "vcl": "x", "parquet_type": "VARCHAR"});
        assert!(validate_custom_field(&field, &[]).is_empty());
        let mut flagged = field.clone();
        flagged["show_in_dashboard"] = json!("yes");
        assert_eq!(
            validate_custom_field(&flagged, &[]),
            ["show_in_dashboard must be a boolean"]
        );
    }
}
