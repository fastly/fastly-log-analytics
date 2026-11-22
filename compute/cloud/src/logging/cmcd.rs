use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::sync::OnceLock;

#[derive(Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    #[default]
    QueryString,
    Headers,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub enabled: bool,
    pub mode: Mode,
    #[serde(deserialize_with = "deserialize_version")]
    pub version: u8,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: false,
            mode: Mode::QueryString,
            version: 1,
        }
    }
}

fn deserialize_version<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<u8, D::Error> {
    match u8::deserialize(deserializer)? {
        version @ (1 | 2) => Ok(version),
        _ => Err(serde::de::Error::custom("CMCD version must be 1 or 2")),
    }
}

fn reference() -> &'static Value {
    static DATA: OnceLock<Value> = OnceLock::new();
    DATA.get_or_init(|| {
        serde_json::from_str(include_str!("cmcd.json")).expect("pinned CMCD resources")
    })
}

pub fn generate_cmcd_vcl(config: &Config) -> Result<Value, String> {
    reference()["variants"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|v| v["mode"] == json!(config.mode) && v["version"] == config.version)
        .map(|v| v["vcl"].clone())
        .ok_or_else(|| "Invalid CMCD configuration".into())
}

pub fn reserved_name(name: &str) -> bool {
    name.starts_with("cmcd_")
}

fn canonical(field: &Value, enabled: bool) -> Value {
    let mut field = field.clone();
    let string = field["value_type"] == "string";
    field["vcl"] = field["vcl_log_expression"].clone();
    field["parquet_type"] = json!(if string { "Utf8" } else { "Int64" });
    field["duckdb_type"] = json!(if string { "VARCHAR" } else { "BIGINT" });
    field["value_type"] = json!(if string { "string" } else { "numeric" });
    field["system_managed"] = json!(true);
    field["committed"] = json!(true);
    field["enabled"] = json!(enabled);
    field
}

pub fn managed_field(field: &Value) -> bool {
    reference()["fields"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|f| f["name"] == field["name"])
        .is_some_and(|f| canonical(f, field["enabled"] == true) == *field)
}

pub fn reconcile_fields(
    fields: &mut Value,
    config: &Config,
    previous: Option<&Value>,
    preview: bool,
) -> Result<(), String> {
    let submitted = fields["custom_fields"]
        .as_array()
        .ok_or("Invalid custom fields")?;
    let old: Vec<&Value> = previous
        .into_iter()
        .flat_map(|v| v["custom_fields"].as_array().into_iter().flatten())
        .filter(|f| managed_field(f))
        .collect();
    let mut seen = BTreeSet::new();
    for field in submitted {
        let name = field["name"].as_str().unwrap_or("");
        if reserved_name(name) {
            let mut candidate = field.clone();
            if preview {
                candidate["committed"] = json!(true);
            }
            if !seen.insert(name)
                || !(old.iter().any(|old| **old == *field)
                    || (preview && managed_field(&candidate)))
            {
                return Err("CMCD fields are system-managed and cannot be changed".into());
            }
        } else if field["system_managed"] == true {
            return Err("The system_managed flag is reserved".into());
        }
    }
    let retain = config.enabled || !old.is_empty() || (preview && !seen.is_empty());
    let mut custom: Vec<Value> = submitted
        .iter()
        .filter(|f| !reserved_name(f["name"].as_str().unwrap_or("")))
        .cloned()
        .collect();
    if retain {
        custom.extend(
            reference()["fields"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|f| canonical(f, config.enabled)),
        );
    }
    fields["custom_fields"] = custom.into();
    Ok(())
}
