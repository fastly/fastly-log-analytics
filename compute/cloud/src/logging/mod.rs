use crate::catalog;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::sync::OnceLock;

pub mod cmcd;
pub mod rum;

pub(crate) fn templates() -> &'static Value {
    static DATA: OnceLock<Value> = OnceLock::new();
    DATA.get_or_init(|| {
        serde_json::from_str(include_str!("../../vcl/capture/templates.json"))
            .expect("checked-in capture templates must be valid JSON")
    })
}

fn text<'a>(value: &'a Value, key: &str, default: &'a str) -> &'a str {
    value[key].as_str().unwrap_or(default)
}

fn custom_fields(config: &Value) -> Vec<Value> {
    let mut fields: Vec<Value> = config["custom_fields"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|f| f["enabled"] != false)
        .map(catalog::normalize_custom_field)
        .collect();
    fields.sort_by(|a, b| text(a, "name", "").cmp(text(b, "name", "")));
    fields
}

fn number(value: &Value, default: i64) -> Result<i64, String> {
    if value.is_null() {
        return Ok(default);
    }
    catalog::integer(value).ok_or_else(|| format!("Expected integer, got {value}"))
}

fn optional_limit(value: &Value) -> Result<Option<i64>, String> {
    if value.is_null() || value == false || value.as_i64() == Some(0) || value.as_str() == Some("")
    {
        return Ok(None);
    }
    number(value, 0).map(Some)
}

pub fn generate_log_format(config: &Value) -> Result<String, String> {
    if config["wire_format"].as_str() == Some("array") {
        return generate_compact_log_format(config);
    }
    let enabled = catalog::resolve_enabled_fields(config);
    let custom = custom_fields(config);
    let mut budget = catalog::LOG_LINE_MAX_BYTES - (2 + (enabled.len() + custom.len()) as i64 * 5);
    for field in catalog::data()["fields"].as_array().into_iter().flatten() {
        let id = text(field, "id", "");
        if enabled.contains(id) && !["url", "ua", "referer"].contains(&id) {
            budget -= field["typical_bytes"].as_i64().unwrap_or(0);
        }
    }
    let mut parts = Vec::new();
    for field in catalog::data()["fields"].as_array().into_iter().flatten() {
        let id = text(field, "id", "");
        let Some(mut vcl) = catalog::logged_vcl(field).map(str::to_owned) else {
            continue;
        };
        if !enabled.contains(id) {
            continue;
        }
        if id == "url" {
            let limit = number(&config["field_limits"]["url"], 2000)?
                .min(budget)
                .max(0);
            budget -= limit;
            vcl = vcl.replace(
                "substr(req.url, 0, 2000)",
                &format!("substr(req.url, 0, {})", limit / 6),
            );
        } else if id == "ua" || id == "referer" {
            let limit = number(&config["field_limits"][id], 1000)?
                .min(budget)
                .max(0);
            budget -= limit;
            let header = if id == "ua" { "User-Agent" } else { "Referer" };
            vcl = format!("\"{id}\":\"%{{json.escape(substr(if(req.http.x-fos-edge-data:{id} != \"\", req.http.x-fos-edge-data:{id}, req.http.{header}), 0, {}))}}V\"", limit / 6);
        }
        parts.push(vcl);
    }
    for field in custom {
        let name = text(&field, "name", "");
        if name.is_empty()
            || !name
                .bytes()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_')
        {
            return Err("Invalid custom field name".into());
        }
        let estimate = number(&field["bytes_estimate"], 0)?;
        let default_limit = if estimate != 0 {
            estimate.saturating_mul(4).clamp(40, 2000)
        } else {
            2000
        };
        let mut limit = match optional_limit(&field["byte_limit"])? {
            Some(limit) => limit,
            None => optional_limit(&config["field_limits"][name])?.unwrap_or(default_limit),
        };
        let numeric = ["numeric", "boolean"].contains(&text(&field, "value_type", "string"));
        if numeric {
            limit = limit.min(20);
        }
        limit = limit.min(budget).max(0);
        budget -= limit;
        let stage = text(&field, "collection_stage", "edge");
        let expression = if stage == "deliver" {
            text(&field, "vcl_log_expression", "").to_owned()
        } else {
            format!(
                "req.http.x-fos-{}-data:{name}",
                if stage == "origin" { "origin" } else { "edge" }
            )
        };
        let expression = if expression.is_empty() {
            format!("req.http.x-fos-edge-data:{name}")
        } else {
            expression
        };
        if numeric {
            let guard = if stage == "deliver" {
                "fastly.ff.visits_this_service == 0 && "
            } else {
                ""
            };
            parts.push(format!("\"{name}\":%{{if({guard}substr({expression}, 0, {limit}) ~ \"^-?[0-9]+(\\\\.[0-9]+)?$\", substr({expression}, 0, {limit}), \"null\")}}V"));
        } else if stage == "deliver" {
            parts.push(format!("\"{name}\":\"%{{json.escape(if(fastly.ff.visits_this_service == 0, substr({expression}, 0, {}), \"\"))}}V\"", limit / 6));
        } else {
            parts.push(format!(
                "\"{name}\":\"%{{json.escape(substr({expression}, 0, {}))}}V\"",
                limit / 6
            ));
        }
    }
    let format = format!("{{{}}}", parts.join(","))
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if format.len() > catalog::LOG_FORMAT_MAX_BYTES {
        return Err(format!("Log format is {} bytes, which exceeds Fastly's maximum allowed limit of 12288 bytes. Disable some fields.", format.len()));
    }
    Ok(format)
}

/// Generates a compact positional JSON array `[v0, v1, ...]` format: one slot per logged catalog
/// field ([`catalog::logged_vcl`]), then the custom columns, in the order
/// [`crate::ingest::convert::RequestSchema`] decodes. Disabled catalog or committed custom
/// fields emit `null`, and `timestamp` (slot 0) emits `null` when `ts_ms` is enabled so
/// `convert_object_capped` populates `timestamp` directly from `ts_ms` without logging a redundant
/// 28-byte ISO string.
pub fn generate_compact_log_format(config: &Value) -> Result<String, String> {
    let enabled = catalog::resolve_enabled_fields(config);
    let fields = catalog::data()["fields"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default();
    let custom = catalog::logged_custom_fields(config);
    let mut budget = catalog::LOG_LINE_MAX_BYTES - (2 + (fields.len() + custom.len()) as i64 * 2);
    for field in fields {
        let id = text(field, "id", "");
        if enabled.contains(id) && !["url", "ua", "referer"].contains(&id) {
            budget -= field["typical_bytes"].as_i64().unwrap_or(0);
        }
    }
    let has_ts_ms = enabled.contains("ts_ms");
    let mut parts = Vec::with_capacity(fields.len() + custom.len());
    for field in fields {
        let id = text(field, "id", "");
        let Some(mut vcl) = catalog::logged_vcl(field).map(str::to_owned) else {
            continue;
        };
        if !enabled.contains(id) || (id == "timestamp" && has_ts_ms) {
            parts.push("null".to_owned());
            continue;
        }
        if id == "url" {
            let limit = number(&config["field_limits"]["url"], 2000)?
                .min(budget)
                .max(0);
            budget -= limit;
            vcl = vcl.replace(
                "substr(req.url, 0, 2000)",
                &format!("substr(req.url, 0, {})", limit / 6),
            );
        } else if id == "ua" || id == "referer" {
            let limit = number(&config["field_limits"][id], 1000)?
                .min(budget)
                .max(0);
            budget -= limit;
            let header = if id == "ua" { "User-Agent" } else { "Referer" };
            vcl = format!("\"{id}\":\"%{{json.escape(substr(if(req.http.x-fos-edge-data:{id} != \"\", req.http.x-fos-edge-data:{id}, req.http.{header}), 0, {}))}}V\"", limit / 6);
        }
        let prefix = format!("\"{id}\":");
        let expr = vcl.strip_prefix(&prefix).unwrap_or(&vcl).to_owned();
        parts.push(expr);
    }
    for field in custom {
        let name = text(&field, "name", "");
        if name.is_empty()
            || !name
                .bytes()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_')
        {
            return Err("Invalid custom field name".into());
        }
        if field["enabled"] == false {
            parts.push("null".to_owned());
            continue;
        }
        let estimate = number(&field["bytes_estimate"], 0)?;
        let default_limit = if estimate != 0 {
            estimate.saturating_mul(4).clamp(40, 2000)
        } else {
            2000
        };
        let mut limit = match optional_limit(&field["byte_limit"])? {
            Some(limit) => limit,
            None => optional_limit(&config["field_limits"][name])?.unwrap_or(default_limit),
        };
        let numeric = ["numeric", "boolean"].contains(&text(&field, "value_type", "string"));
        if numeric {
            limit = limit.min(20);
        }
        limit = limit.min(budget).max(0);
        budget -= limit;
        let stage = text(&field, "collection_stage", "edge");
        let expression = if stage == "deliver" {
            text(&field, "vcl_log_expression", "").to_owned()
        } else {
            format!(
                "req.http.x-fos-{}-data:{name}",
                if stage == "origin" { "origin" } else { "edge" }
            )
        };
        let expression = if expression.is_empty() {
            format!("req.http.x-fos-edge-data:{name}")
        } else {
            expression
        };
        if numeric {
            let guard = if stage == "deliver" {
                "fastly.ff.visits_this_service == 0 && "
            } else {
                ""
            };
            parts.push(format!("%{{if({guard}substr({expression}, 0, {limit}) ~ \"^-?[0-9]+(\\\\.[0-9]+)?$\", substr({expression}, 0, {limit}), \"null\")}}V"));
        } else if stage == "deliver" {
            parts.push(format!("\"%{{json.escape(if(fastly.ff.visits_this_service == 0, substr({expression}, 0, {}), \"\"))}}V\"", limit / 6));
        } else {
            parts.push(format!(
                "\"%{{json.escape(substr({expression}, 0, {}))}}V\"",
                limit / 6
            ));
        }
    }
    let format = format!("[{}]", parts.join(","))
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if format.len() > catalog::LOG_FORMAT_MAX_BYTES {
        return Err(format!("Log format is {} bytes, which exceeds Fastly's maximum allowed limit of 12288 bytes. Disable some fields.", format.len()));
    }
    Ok(format)
}

pub fn format_hash(config: &Value) -> Result<String, String> {
    Ok(format!(
        "sha256:{}",
        hex::encode(Sha256::digest(generate_log_format(config)?.as_bytes()))
    ))
}

pub fn load_log_format(config: &Value) -> Result<String, String> {
    let mut config = if config.is_object() {
        config.clone()
    } else {
        json!({})
    };
    if config["groups"].as_array().is_none_or(Vec::is_empty)
        && config["preset"].as_str().is_none_or(str::is_empty)
    {
        config["groups"] = json!(["A", "C", "D", "I"]);
    }
    generate_log_format(&config)
}

fn scrub_statements(config: &Value) -> Vec<String> {
    let mut lines: Vec<String> = templates()["scrub_headers"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(|h| format!("unset req.http.{h};"))
        .collect();
    for field in custom_fields(config) {
        let name = text(&field, "name", "");
        for prefix in ["x-fos-edge-data", "x-fos-origin-data", "x-edge-score"] {
            lines.push(format!("unset req.http.{prefix}:{name};"));
        }
    }
    lines
}

fn group_enabled(config: &Value, group: &str) -> bool {
    config["groups"]
        .as_array()
        .is_some_and(|groups| groups.iter().any(|g| g == group))
}

fn capture_statements(config: &Value) -> Result<Vec<String>, String> {
    let mut lines = vec![
        "if (req.http.Fastly-Client-IP) {".into(),
        "  set req.http.x-fos-edge-data:ip = req.http.Fastly-Client-IP;".into(),
        "} else {".into(),
        "  set req.http.x-fos-edge-data:ip = client.ip;".into(),
        "}".into(),
    ];
    for key in catalog::get_required_edge_headers(config) {
        if key == "ip" {
            continue;
        }
        if let Some(expression) = templates()["edge_mapping"][&key].as_str() {
            let expression = if key == "ua" || key == "referer" {
                format!(
                    "substr({expression}, 0, {})",
                    number(&config["field_limits"][&key], 1000)?
                )
            } else {
                expression.to_owned()
            };
            lines.push(format!(
                "set req.http.x-fos-edge-data:{key} = {expression};"
            ));
        }
    }
    for field in custom_fields(config) {
        if text(&field, "collection_stage", "edge") == "edge" {
            let name = text(&field, "name", "");
            let expression = field["vcl_log_expression"]
                .as_str()
                .ok_or_else(|| format!("Missing VCL expression for custom field '{name}'"))?;
            lines.push(format!(
                "set req.http.x-fos-edge-data:{name} = {expression};"
            ));
        }
    }
    if group_enabled(config, "L") {
        lines.push("set req.http.x-req-id = randomstr(8);".into());
    }
    Ok(lines)
}

pub fn generate_capture_vcl(config: &Value) -> Result<Value, String> {
    generate_capture_vcl_with_cmcd(config, &cmcd::Config::default())
}

pub fn generate_capture_vcl_with_cmcd(
    config: &Value,
    cmcd: &cmcd::Config,
) -> Result<Value, String> {
    let empty = json!({});
    let config = if config.is_null() { &empty } else { config };
    let custom = custom_fields(config);
    let origin: Vec<&Value> = custom
        .iter()
        .filter(|f| text(f, "collection_stage", "edge") == "origin")
        .collect();
    let deliver: Vec<&Value> = custom
        .iter()
        .filter(|f| text(f, "collection_stage", "edge") == "deliver")
        .collect();
    let l = group_enabled(config, "L");
    let m = group_enabled(config, "M");
    let mut snippets = Map::new();
    let mut recv = vec![
        "# [security] strip client-supplied internal-routing headers".into(),
        "if (req.restarts == 0 && fastly.ff.visits_this_service == 0) {".into(),
    ];
    recv.extend(
        scrub_statements(config)
            .iter()
            .map(|line| format!("  {line}")),
    );
    if cmcd.enabled {
        recv.push("  # Section 3: CMCD Extraction (vcl_recv) — must precede field capture".into());
        let extraction = cmcd::generate_cmcd_vcl(cmcd)?;
        recv.extend(
            extraction["CMCD Extraction"]
                .as_str()
                .unwrap_or("")
                .lines()
                .map(|line| {
                    if line.is_empty() {
                        String::new()
                    } else {
                        format!("  {line}")
                    }
                }),
        );
    }
    recv.extend(
        capture_statements(config)?
            .iter()
            .map(|line| format!("  {line}")),
    );
    recv.push("}".into());
    snippets.insert("recv".into(), json!(recv.join("\n")));
    for routine in ["miss", "pass"] {
        snippets.insert(
            routine.into(),
            templates()[if l { "L" } else { "base" }][routine].clone(),
        );
    }
    if catalog::get_required_edge_headers(config).contains("ip") {
        snippets.insert(
            "recv_reset".into(),
            templates()["base"]["recv_reset"].clone(),
        );
    }
    let mut fetch_lines = Vec::new();
    if l {
        fetch_lines.push(text(&templates()["L"], "fetch", "").to_owned());
        snippets.insert("error".into(), templates()["L"]["error"].clone());
    }
    if !origin.is_empty() {
        fetch_lines.push("# --- Custom Origin Fields Start ---".into());
        fetch_lines.push("if (req.backend.is_origin) {".into());
        for field in &origin {
            fetch_lines.push(format!(
                "  set beresp.http.x-fos-origin-data:{} = {};",
                text(field, "name", ""),
                text(field, "vcl_log_expression", "")
            ));
        }
        fetch_lines.push("}".into());
        fetch_lines.push("# --- Custom Origin Fields End ---".into());
    }
    if !fetch_lines.is_empty() {
        snippets.insert("fetch".into(), json!(fetch_lines.join("\n")));
    }
    let mut deliver_lines = Vec::new();
    if l {
        deliver_lines.push(text(&templates()["L"], "deliver", "").to_owned());
    }
    if m {
        deliver_lines.push(text(&templates()["M"], "deliver", "").to_owned());
    }
    if !origin.is_empty() {
        deliver_lines.push("# --- Custom Origin Fields Start ---".into());
        for field in origin {
            let name = text(field, "name", "");
            deliver_lines.push(format!(
                "if (resp.http.x-fos-origin-data:{name} != \"\") {{"
            ));
            if text(field, "origin_log_frequency", "all") == "miss_pass" {
                deliver_lines.push("  if (fastly_info.state !~ \"HIT\") {".into());
                deliver_lines.push(format!("    set req.http.x-fos-origin-data:{name} = resp.http.x-fos-origin-data:{name};"));
                deliver_lines.push("  }".into());
            } else {
                deliver_lines.push(format!(
                    "  set req.http.x-fos-origin-data:{name} = resp.http.x-fos-origin-data:{name};"
                ));
            }
            deliver_lines.push("}".into());
            deliver_lines.push("if (req.http.x-is-cluster-fetch != \"1\") {".into());
            deliver_lines.push(format!("  unset resp.http.x-fos-origin-data:{name};"));
            deliver_lines.push("}".into());
        }
        deliver_lines.push("# --- Custom Origin Fields End ---".into());
    }
    if !deliver.is_empty() {
        deliver_lines.push("# --- Custom Deliver Fields Start ---".into());
        for field in deliver {
            let name = text(field, "name", "");
            let expression = text(field, "vcl_log_expression", "");
            deliver_lines.push(format!("if ({expression} != \"\") {{"));
            deliver_lines.push(format!(
                "  set req.http.x-fos-edge-data:{name} = {expression};"
            ));
            deliver_lines.push("}".into());
        }
        deliver_lines.push("# --- Custom Deliver Fields End ---".into());
    }
    if !deliver_lines.is_empty() {
        snippets.insert("deliver".into(), json!(deliver_lines.join("\n")));
    }
    Ok(Value::Object(snippets))
}

pub fn rum_from_state(state: &Value) -> Result<rum::Config, String> {
    let default_period = number(&state["log_period"], 60)?;
    let rum_obj = state.get("rum").filter(|v| v.is_object());
    let mut cfg: rum::Config = match rum_obj {
        Some(value) => serde_json::from_value(value.clone()).map_err(|e| e.to_string())?,
        None => rum::Config::default(),
    };
    if rum_obj.and_then(|v| v.get("log_period")).is_none() {
        cfg.log_period = default_period as u32;
    }
    if let Some(enabled) = state.get("rum_enabled").and_then(Value::as_bool) {
        cfg.enabled = enabled;
    }
    if let Some(version) = state.get("faro_version").and_then(Value::as_str) {
        cfg.faro_version = version.to_owned();
    }
    if let Some(period_val) = state.get("rum_log_period") {
        if !period_val.is_null() {
            cfg.log_period = number(period_val, default_period)? as u32;
        }
    }
    if let Some(cond_val) = state.get("rum_custom_condition") {
        cfg.custom_condition = cond_val.as_str().map(str::trim).unwrap_or("").to_owned();
    }
    if let Some(name) = state
        .get("rum_endpoint_name")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
    {
        cfg.endpoint_name = name.to_owned();
    }
    for (key, target) in [
        ("capture_vitals", &mut cfg.capture_vitals),
        ("capture_performance", &mut cfg.capture_performance),
        ("capture_errors", &mut cfg.capture_errors),
        ("capture_events", &mut cfg.capture_events),
    ] {
        if let Some(flag) = state.get(key).and_then(Value::as_bool) {
            *target = flag;
        }
    }
    Ok(cfg)
}

fn effective_log_fields(config: &Value, rum_enabled: bool) -> Value {
    let mut lf = if config.is_object() {
        config.clone()
    } else {
        json!({})
    };
    if rum_enabled {
        let existing = lf
            .as_object_mut()
            .expect("object")
            .entry("custom_fields")
            .or_insert_with(|| json!([]));
        if let Some(arr) = existing.as_array_mut() {
            let names: std::collections::BTreeSet<String> = arr
                .iter()
                .filter_map(|f| f["name"].as_str().map(str::to_owned))
                .collect();
            for rf in rum::rum_custom_fields().as_array().into_iter().flatten() {
                if let Some(name) = rf["name"].as_str() {
                    if !names.contains(name) {
                        arr.push(rf.clone());
                    }
                }
            }
        }
    }
    lf
}

pub fn desired_snippets(state: &Value) -> Result<Value, String> {
    reject_future_features(state)?;
    let enabled = state["logging_enabled"] != false;
    let rum = rum_from_state(state)?;
    let empty = json!({});
    let raw_config = state.get("log_fields").unwrap_or(&empty);
    let config = effective_log_fields(raw_config, rum.enabled);
    let cmcd: cmcd::Config = state
        .get("cmcd")
        .map(|value| serde_json::from_value(value.clone()))
        .transpose()
        .map_err(|error| error.to_string())?
        .unwrap_or_default();
    let capture = if enabled {
        generate_capture_vcl(&config)?
    } else {
        json!({})
    };
    let shield_pop = state
        .get("cdn_shield")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| {
            state
                .get("fos_region")
                .and_then(Value::as_str)
                .filter(|s| !s.trim().is_empty())
                .map(rum::shield_for_region)
                .unwrap_or("iad-va-us")
        });
    let mut snippets = Vec::new();
    for subroutine in [
        "vcl_recv",
        "vcl_miss",
        "vcl_pass",
        "vcl_fetch",
        "vcl_deliver",
        "vcl_error",
    ] {
        let body = if subroutine == "vcl_recv" {
            let mut restarts_lines = Vec::new();
            if enabled {
                restarts_lines.push("  # Section 6: Log Field Capture (vcl_recv)".to_owned());
                restarts_lines.push(
                    "  # [security] strip client-supplied internal-routing headers".to_owned(),
                );
                restarts_lines.extend(
                    scrub_statements(&config)
                        .iter()
                        .map(|line| format!("  {line}")),
                );
            }
            if rum.enabled {
                restarts_lines
                    .push("  # Mint a per-request ID for RUM beacons on fresh requests".to_owned());
                restarts_lines.push("  set req.http.x-rum-req-id = randomstr(12);".to_owned());
                restarts_lines.push("  # Section 5: RUM field extraction (vcl_recv)".to_owned());
                restarts_lines.push("  if (req.url.path == \"/rum-beacon\") {".to_owned());
                for line in rum::beacon_field_extraction_vcl().lines() {
                    restarts_lines.push(format!("    {line}"));
                }
                restarts_lines.push("  }".to_owned());
            }
            if enabled {
                restarts_lines.push(String::new());
                if cmcd.enabled {
                    restarts_lines.push(
                        "  # Section 3: CMCD Extraction (vcl_recv) — before field capture"
                            .to_owned(),
                    );
                    let extraction = cmcd::generate_cmcd_vcl(&cmcd)?;
                    restarts_lines.extend(
                        extraction["CMCD Extraction"]
                            .as_str()
                            .unwrap_or("")
                            .lines()
                            .filter(|line| !line.trim().is_empty())
                            .map(|line| format!("  {line}")),
                    );
                }
                restarts_lines.push(
                    "  # Capture edge data for logging before shielding or backend fetch"
                        .to_owned(),
                );
                restarts_lines.extend(
                    capture_statements(&config)?
                        .iter()
                        .map(|line| format!("  {line}")),
                );
            }
            if rum.enabled {
                restarts_lines.push("  # Section 5: RUM (vcl_recv)".to_owned());
                restarts_lines.push("  if (req.url.path == \"/rum-beacon\") {".to_owned());
                for line in rum::beacon_response_vcl().lines() {
                    if line.trim().is_empty() {
                        restarts_lines.push(String::new());
                    } else {
                        restarts_lines.push(format!("    {line}"));
                    }
                }
                restarts_lines.push("  }".to_owned());
            }
            let mut parts = Vec::new();
            if !restarts_lines.is_empty() {
                parts.push(format!(
                    "if (req.restarts == 0 && fastly.ff.visits_this_service == 0) {{\n{}\n}}",
                    restarts_lines.join("\n")
                ));
            }
            if rum.enabled {
                parts.push(rum::asset_fetch_vcl(shield_pop, Some(&rum.faro_version))?);
            }
            parts.join("\n\n")
        } else {
            let mut parts = Vec::new();
            if enabled {
                if let Some(section) = capture[subroutine.trim_start_matches("vcl_")].as_str() {
                    if !section.trim().is_empty() {
                        parts.push(format!(
                            "# Section 6: Log Field Capture ({subroutine})\n{section}"
                        ));
                    }
                }
            }
            if rum.enabled {
                match subroutine {
                    "vcl_miss" => {
                        parts.push(format!(
                            "# Section 5: RUM (vcl_miss)\n{}",
                            rum::sigv4_sign_vcl(Some(&rum.faro_version))?
                        ));
                    }
                    "vcl_fetch" => {
                        parts.push(format!(
                            "# Section 5: RUM (vcl_fetch)\n{}",
                            rum::fetch_caching_vcl(Some(&rum.faro_version))?
                        ));
                    }
                    "vcl_error" => {
                        parts.push(format!(
                            "# Section 5: RUM (vcl_error)\n{}",
                            rum::error_beacon_vcl()
                        ));
                    }
                    _ => {}
                }
            }
            parts.join("\n\n")
        };
        snippets.push(json!({"name": format!("Fastly Log Analytics - {subroutine}"), "priority": 10, "body": body, "subroutine": subroutine}));
    }
    Ok(json!(snippets))
}

pub fn logging_service_snippets(state: &Value) -> Result<Value, String> {
    reject_future_features(state)?;
    if state["logging_enabled"] == false {
        return Ok(json!([]));
    }
    let rum = rum_from_state(state)?;
    let config = effective_log_fields(&state["log_fields"], rum.enabled);
    let capture = generate_capture_vcl(&config)?;
    let mut snippets = Vec::new();
    for (routine, name, priority) in [
        ("recv", "Fastly Log Analysis Capture", 1),
        ("recv_reset", "Fastly Log Analysis Reset Client IP", -100),
        ("miss", "Fastly Log Analysis Miss", 10),
        ("pass", "Fastly Log Analysis Pass", 10),
        ("fetch", "Fastly Log Analysis Origin Fetch", 10),
        ("deliver", "Fastly Log Analysis Origin Deliver", 10),
        ("error", "Fastly Log Analysis Origin Error", 10),
    ] {
        if let Some(body) = capture.get(routine) {
            snippets.push(json!({"name":name, "priority":priority, "body":body,
                "subroutine":format!("vcl_{}", if routine == "recv_reset" { "recv" } else { routine })}));
        }
    }
    Ok(json!(snippets))
}

fn reject_future_features(state: &Value) -> Result<(), String> {
    if state["scoring"]["enabled"] == true || state["scoring_enabled"] == true {
        return Err("scoring is outside Goal 2".into());
    }
    Ok(())
}

pub fn desired_logging_endpoints(state: &Value) -> Result<Value, String> {
    reject_future_features(state)?;
    let logging_enabled = state["logging_enabled"] != false;
    let rum = rum_from_state(state)?;
    let codec = state["compression_codec"]
        .as_str()
        .or_else(|| state["log_fields"]["compression_codec"].as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("zstd");
    let array_wire = state["wire_format"].as_str() == Some("array")
        || state["log_fields"]["wire_format"].as_str() == Some("array");
    let mut endpoints = Vec::new();
    if logging_enabled {
        let period = number(&state["log_period"], 60)?;
        if !(1..=3600).contains(&period) {
            return Err("log_period must be in range [1, 3600]".into());
        }
        let mut request_log_fields = state["log_fields"].clone();
        if array_wire {
            if let Some(obj) = request_log_fields.as_object_mut() {
                obj.insert("wire_format".into(), "array".into());
            }
        }
        if rum.enabled {
            if let Some(arr) = request_log_fields
                .as_object_mut()
                .and_then(|obj| obj.get_mut("custom_fields"))
                .and_then(Value::as_array_mut)
            {
                if array_wire {
                    for f in arr.iter_mut() {
                        if f["name"]
                            .as_str()
                            .is_some_and(|name| rum::RUM_FIELD_NAMES.contains(&name))
                        {
                            if let Some(obj) = f.as_object_mut() {
                                obj.insert("enabled".into(), Value::Bool(false));
                            }
                        }
                    }
                    arr.retain(|f| f["committed"] == true || f["enabled"] != false);
                } else {
                    arr.retain(|f| {
                        f["name"]
                            .as_str()
                            .is_none_or(|name| !rum::RUM_FIELD_NAMES.contains(&name))
                    });
                }
            }
        }
        let ep = json!({
            "name":text(state,"logging_endpoint_name","Fastly Log Analytics"), "type":"s3",
            "path":"raw/request/year=%Y/month=%m/day=%d/hour=%H/minute=%M/",
            "period":period, "response_condition":"log_analytics_condition",
            "format":load_log_format(&request_log_fields)?,
            "placement":null, "response_object_name":"",
            "compression_codec":codec
        });
        endpoints.push(ep);
    }
    if rum.enabled {
        if !(1..=3600).contains(&rum.log_period) {
            return Err("rum_log_period must be in range [1, 3600]".into());
        }
        let rum_format = if array_wire {
            rum::generate_compact_rum_log_format()?
        } else {
            rum::generate_rum_log_format()?
        };
        let ep = json!({
            "name": rum.endpoint_name,
            "type": "s3",
            "path": "raw/rum/year=%Y/month=%m/day=%d/hour=%H/minute=%M/",
            "period": rum.log_period,
            "response_condition": "rum_log_condition",
            "format": rum_format,
            "placement": null,
            "response_object_name": "",
            "compression_codec": codec
        });
        endpoints.push(ep);
    }
    Ok(json!(endpoints))
}

pub fn logging_endpoint_body(endpoint: &Value, connection: &Value) -> Value {
    let codec = endpoint["compression_codec"]
        .as_str()
        .filter(|s| !s.is_empty())
        .unwrap_or("zstd");
    let mut body = json!({
        "name":endpoint["name"],"path":endpoint["path"],"period":endpoint["period"],
        "format":endpoint["format"],"placement":endpoint["placement"],
        "response_object_name":endpoint["response_object_name"], "format_version":2,
        "compression_codec":codec
    });
    for key in [
        "response_condition",
        "bucket_name",
        "domain",
        "access_key",
        "secret_key",
    ] {
        let value = if key == "response_condition" {
            &endpoint[key]
        } else {
            &connection[key]
        };
        if value.as_str().is_some_and(|s| !s.is_empty()) {
            body[key] = value.clone();
        }
    }
    body
}

pub fn desired_conditions(state: &Value) -> Result<Value, String> {
    reject_future_features(state)?;
    let logging_enabled = state["logging_enabled"] != false;
    let rum = rum_from_state(state)?;
    let mut conditions = Vec::new();
    if logging_enabled {
        let sample_rate = number(&state["sample_rate"], 100)?;
        if !(1..=100).contains(&sample_rate) {
            return Err("sample_rate must be in range [1, 100]".into());
        }
        let mut parts = vec!["!segmented_caching.is_inner_req".to_owned()];
        if rum.enabled {
            parts.push("req.url.path != \"/rum-beacon\"".into());
        }
        if state["edge_only"] == true {
            parts.push("fastly.ff.visits_this_service == 0".into());
        }
        if sample_rate < 100 {
            parts.push(format!("randombool({sample_rate}, 100)"));
        }
        let condition = text(state, "custom_condition", "").trim();
        if !condition.is_empty() {
            parts.push(format!("({condition})"));
        }
        conditions.push(
            json!({"name":"log_analytics_condition", "statement":parts.join(" && "), "type":"RESPONSE"}),
        );
    }
    if rum.enabled {
        let rum_statement = rum::build_rum_log_condition(&rum.custom_condition)?;
        conditions.push(json!({
            "name": "rum_log_condition",
            "statement": rum_statement,
            "type": "RESPONSE"
        }));
        conditions.push(json!({
            "name": "fastly_log_analytics_false",
            "statement": "false",
            "type": "REQUEST"
        }));
    }
    Ok(json!(conditions))
}
