use super::*;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

include!(concat!(env!("OUT_DIR"), "/scorer.rs"));

pub const SCORING_BACKEND_API_NAME: &str = "session_scorer";
pub const SCORING_BACKEND_VCL_NAME: &str = "F_session_scorer";
pub const SCORING_RECV_NAME: &str = "Session Scoring - Recv";
pub const SCORING_PASS_NAME: &str = "Session Scoring - Pass";
pub const SCORING_FETCH_NAME: &str = "Session Scoring - Fetch";
pub const SCORING_DELIVER_NAME: &str = "Session Scoring - Deliver";
pub const SCORING_MISS_NAME: &str = "Session Scoring - Miss";
pub const SCORING_ENFORCE_NAME: &str = "Session Scoring - Enforce";
pub const SCORING_SNIPPET_PRIORITY: i64 = 100;
pub const DEFAULT_ENFORCE_STATUS_CODE: u16 = 429;
pub const DEFAULT_ASSET_EXT_REGEX: &str = concat!(
    r"^[^?#;]*",
    r"\.(aif|aiff|au|avi|bin|bmp|cab|carb|cct|cdf|class|css|dcr|doc|",
    r"dtd|exe|flv|gcf|gff|gif|grv|hdml|hqx|ico|ini|jpeg|jpg|js|mov|",
    r"mp3|mp4|nc|pct|pdf|png|ppc|pws|svg|swa|swf|txt|vbs|w32|wav|",
    r"wbmp|wml|wmlc|wmls|wmlsc|xsd|zip|webp|woff|woff2|ttf|bz2|gz|",
    r"tgz|tar|lzma|rar|war|bz|7z|ts|m3u8)($|\?|#)"
);

const KEYS_RESOURCE_LINK_NAME: &str = "scoring_keys";
const CONFIG_RESOURCE_LINK_NAME: &str = "scoring_config";
const MATRIX_RESOURCE_LINK_NAME: &str = "scoring_matrix";
const MATRIX_KEY_NAME: &str = "matrix";
pub const REQUEST_SECRET_PLACEHOLDER: &str = "__FLP_SCORING_REQUEST_SECRET__";
const SCORING_ENABLED_AT_KEY: &str = "scoring_enabled_at";
const L2_ENABLED_AT_KEY: &str = "l2_enabled_at";
const L2_ENFORCE_ENABLED_KEY: &str = "l2_enforce_enabled";
const ENFORCE_THRESHOLD_KEY: &str = "enforce_threshold";
const DEBUG_LOG_KEY: &str = "debug_logging_enabled";
const CURRENT_KEY_HEX: &str = "current_key_hex";
const PREVIOUS_KEY_HEX: &str = "previous_key_hex";
const REQUEST_SECRET_KEY: &str = "request_secret";
const L2_RAMP_DAYS: f64 = 3.0;
const L2_READINESS_DAYS: f64 = 7.0;
const MAX_MATRIX_HISTORY: usize = 50;
const MAX_AUDIT_ENTRIES: usize = 200;
const MAX_VOCAB_SIZE: usize = 100_000;

pub const SCORING_FIELD_NAMES: &[&str] = &[
    "edge_score",
    "edge_score_l1",
    "edge_score_l2",
    "edge_cookie_compliance",
    "edge_score_reason",
    "edge_sid",
    "edge_score_rtt_us",
    "edge_score_exec_us",
    "edge_matrix_version",
];

pub fn scoring_snippet_names() -> [&'static str; 6] {
    [
        SCORING_RECV_NAME,
        SCORING_PASS_NAME,
        SCORING_FETCH_NAME,
        SCORING_DELIVER_NAME,
        SCORING_MISS_NAME,
        SCORING_ENFORCE_NAME,
    ]
}

pub fn is_scoring_snippet_name(name: &str) -> bool {
    scoring_snippet_names().contains(&name)
}

pub fn valid_matrix_version(v: &str) -> bool {
    !v.is_empty()
        && v.len() <= 64
        && !matches!(v, "index" | "." | "..")
        && !v.contains("..")
        && v.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

pub(crate) fn valid_store_id(id: &str) -> bool {
    (1..=64).contains(&id.len())
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
}

pub(crate) fn own_stores(runtime: &impl Runtime) -> Vec<String> {
    let mut stores = Vec::new();
    for key in ["secret_store_id", "config_store_id", "kv_store_id"] {
        if let Ok(Some(val)) = runtime.config(key) {
            if !val.is_empty() && !stores.contains(&val) {
                stores.push(val);
            }
        }
    }
    stores
}

pub fn scoring_custom_fields() -> Vec<Value> {
    vec![
        json!({
            "name": "edge_score",
            "label": "Edge Score",
            "description": "Combined session-anomaly score (0-100, quantized to nearest 5) from the edge scorer.",
            "vcl_log_expression": "req.http.x-edge-score:score",
            "collection_stage": "deliver",
            "sql_type": "INTEGER",
            "duckdb_type": "INTEGER",
            "value_type": "numeric",
            "bytes_estimate": 4,
            "enabled": true,
        }),
        json!({
            "name": "edge_score_l1",
            "label": "Edge Score (Layer 1)",
            "description": "Layer-1 (universal behavioral) score contribution.",
            "vcl_log_expression": "req.http.x-edge-score:l1",
            "collection_stage": "deliver",
            "sql_type": "INTEGER",
            "duckdb_type": "INTEGER",
            "value_type": "numeric",
            "bytes_estimate": 4,
            "enabled": true,
        }),
        json!({
            "name": "edge_score_l2",
            "label": "Edge Score (Layer 2)",
            "description": "Layer-2 (route transition) score contribution.",
            "vcl_log_expression": "req.http.x-edge-score:l2",
            "collection_stage": "deliver",
            "sql_type": "INTEGER",
            "duckdb_type": "INTEGER",
            "value_type": "numeric",
            "bytes_estimate": 4,
            "enabled": true,
        }),
        json!({
            "name": "edge_cookie_compliance",
            "label": "Cookie Compliance",
            "description": "ok | missing | tampered | unknown.",
            "vcl_log_expression": "req.http.x-edge-score:compliance",
            "collection_stage": "deliver",
            "sql_type": "VARCHAR",
            "duckdb_type": "VARCHAR",
            "value_type": "string",
            "bytes_estimate": 10,
            "enabled": true,
        }),
        json!({
            "name": "edge_score_reason",
            "label": "Score Reason",
            "description": "Comma-separated list of fired scoring rules.",
            "vcl_log_expression": "req.http.x-edge-score:reason",
            "collection_stage": "deliver",
            "sql_type": "VARCHAR",
            "duckdb_type": "VARCHAR",
            "value_type": "string",
            "bytes_estimate": 60,
            "enabled": true,
        }),
        json!({
            "name": "edge_sid",
            "label": "Session ID",
            "description": "12-hex-char rotating session id from the edge scorer cookie.",
            "vcl_log_expression": "req.http.x-edge-score:sid",
            "collection_stage": "deliver",
            "sql_type": "VARCHAR",
            "duckdb_type": "VARCHAR",
            "value_type": "string",
            "bytes_estimate": 12,
            "enabled": true,
        }),
        json!({
            "name": "edge_score_rtt_us",
            "label": "Scorer Round-Trip (us)",
            "description": "Edge-observed round-trip latency for the scoring sub-fetch, in microseconds.",
            "vcl_log_expression": "req.http.x-edge-score:rtt",
            "collection_stage": "deliver",
            "sql_type": "INTEGER",
            "duckdb_type": "INTEGER",
            "value_type": "numeric",
            "bytes_estimate": 6,
            "enabled": true,
        }),
        json!({
            "name": "edge_score_exec_us",
            "label": "Scorer Exec (us)",
            "description": "Scorer-reported Wasm execution time in microseconds.",
            "vcl_log_expression": "req.http.x-edge-score:exec",
            "collection_stage": "deliver",
            "sql_type": "INTEGER",
            "duckdb_type": "INTEGER",
            "value_type": "numeric",
            "bytes_estimate": 5,
            "enabled": true,
        }),
        json!({
            "name": "edge_matrix_version",
            "label": "Matrix Version",
            "description": "Transition-matrix version (YYYY-MM-DD-suffix) that scored this request.",
            "vcl_log_expression": "req.http.x-edge-score:matrix",
            "collection_stage": "deliver",
            "sql_type": "VARCHAR",
            "duckdb_type": "VARCHAR",
            "value_type": "string",
            "bytes_estimate": 18,
            "enabled": true,
        }),
    ]
}

pub fn managed_field(field: &Value) -> bool {
    const ALLOWED_KEYS: &[&str] = &[
        "name",
        "label",
        "description",
        "vcl_log_expression",
        "collection_stage",
        "sql_type",
        "duckdb_type",
        "value_type",
        "bytes_estimate",
        "byte_limit",
        "enabled",
        "committed",
    ];
    let Some(obj) = field.as_object() else {
        return false;
    };
    if obj.keys().any(|k| !ALLOWED_KEYS.contains(&k.as_str())) {
        return false;
    }
    if let Some(v) = obj.get("label") {
        let Some(s) = v.as_str() else {
            return false;
        };
        if s.trim().is_empty() || s.chars().count() > 80 || s.chars().any(char::is_control) {
            return false;
        }
    }
    if let Some(v) = obj.get("description") {
        let Some(s) = v.as_str() else {
            return false;
        };
        if s.chars().count() > 500 || s.chars().any(char::is_control) {
            return false;
        }
    }
    if obj.get("enabled").is_some_and(|v| !v.is_boolean())
        || obj.get("committed").is_some_and(|v| !v.is_boolean())
    {
        return false;
    }
    if let Some(v) = obj.get("byte_limit") {
        if !v.is_null()
            && (v.is_boolean()
                || v.is_string()
                || !crate::catalog::integer(v)
                    .is_some_and(|n| (1..=crate::catalog::LOG_LINE_MAX_BYTES).contains(&n)))
        {
            return false;
        }
    }
    (obj.contains_key("sql_type") || obj.contains_key("duckdb_type"))
        && scoring_custom_fields().iter().any(|canonical| {
            canonical["name"] == field["name"]
                && canonical["vcl_log_expression"] == field["vcl_log_expression"]
                && canonical["collection_stage"] == field["collection_stage"]
                && obj
                    .get("sql_type")
                    .is_none_or(|v| v == &canonical["sql_type"])
                && obj
                    .get("duckdb_type")
                    .is_none_or(|v| v == &canonical["sql_type"])
                && canonical["value_type"] == field["value_type"]
                && canonical["bytes_estimate"] == field["bytes_estimate"]
        })
}

pub fn merge_scoring_custom_fields(log_fields: &mut Value) {
    let mut existing: Vec<Value> = log_fields["custom_fields"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    existing.retain(|cf| {
        cf["name"]
            .as_str()
            .is_none_or(|name| !SCORING_FIELD_NAMES.contains(&name))
    });
    existing.extend(scoring_custom_fields());
    if let Some(obj) = log_fields.as_object_mut() {
        obj.insert("custom_fields".into(), Value::Array(existing));
    }
}

pub fn remove_scoring_custom_fields(log_fields: &mut Value) {
    if let Some(arr) = log_fields
        .get_mut("custom_fields")
        .and_then(|v| v.as_array_mut())
    {
        arr.retain(|cf| {
            cf["name"]
                .as_str()
                .is_none_or(|name| !SCORING_FIELD_NAMES.contains(&name))
        });
    }
}

pub fn resolve_exclude_url_regex(override_val: Option<&str>) -> String {
    match override_val.map(str::trim).filter(|s| !s.is_empty()) {
        Some(v) => v.to_owned(),
        None => DEFAULT_ASSET_EXT_REGEX.to_owned(),
    }
}

fn contains_vcl_percent_null(s: &str) -> bool {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 < bytes.len() && bytes[i + 1] == b'0' && bytes[i + 2] == b'0' {
                return true;
            }
            if i + 5 < bytes.len()
                && matches!(bytes[i + 1], b'u' | b'U')
                && &bytes[i + 2..i + 6] == b"0000"
            {
                return true;
            }
            if i + 3 < bytes.len() && matches!(bytes[i + 1], b'u' | b'U') && bytes[i + 2] == b'{' {
                let mut j = i + 3;
                while j < bytes.len() && bytes[j] == b'0' {
                    j += 1;
                }
                if j > i + 3 && j < bytes.len() && bytes[j] == b'}' {
                    return true;
                }
            }
        }
        i += 1;
    }
    false
}

pub fn validate_exclude_url_regex(raw: &str) -> Result<String, &'static str> {
    if raw.contains(['\n', '\r'])
        || raw
            .chars()
            .any(|c| c.is_control() || matches!(c, '\u{2028}' | '\u{2029}'))
    {
        return Err("exclude_regex_unsafe_chars");
    }
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(String::new());
    }
    if trimmed.len() > 4096 {
        return Err("exclude_regex_too_long");
    }
    if trimmed.chars().any(|c| {
        c.is_control() || matches!(c, '"' | '\n' | '\r' | '{' | '}' | '\u{2028}' | '\u{2029}')
    }) || contains_vcl_percent_null(trimmed)
    {
        return Err("exclude_regex_unsafe_chars");
    }
    // Check balanced parentheses and brackets as a fast regex sanity guard.
    let mut paren_depth: i32 = 0;
    let mut bracket_depth: i32 = 0;
    let mut escaped = false;
    for ch in trimmed.chars() {
        if escaped {
            escaped = false;
            continue;
        }
        match ch {
            '\\' => escaped = true,
            '[' => bracket_depth += 1,
            ']' if bracket_depth > 0 => bracket_depth -= 1,
            '(' if bracket_depth == 0 => paren_depth += 1,
            ')' if bracket_depth == 0 => {
                paren_depth -= 1;
                if paren_depth < 0 {
                    return Err("exclude_regex_unbalanced_parens");
                }
            }
            _ => {}
        }
    }
    if escaped || paren_depth != 0 || bracket_depth != 0 {
        return Err("exclude_regex_invalid_syntax");
    }
    Ok(trimmed.to_owned())
}

pub fn resolve_enforce_status_code(code: Option<u16>) -> u16 {
    match code {
        Some(c) if (400..=599).contains(&c) => c,
        _ => DEFAULT_ENFORCE_STATUS_CODE,
    }
}

fn enforce_reason_phrase(code: u16) -> &'static str {
    match code {
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        429 => "Too Many Requests",
        503 => "Service Unavailable",
        _ => "Blocked",
    }
}

pub fn validate_vcl_literal(value: &str, allow_backslash: bool) -> Result<(), Error> {
    if value.chars().any(|c| {
        c.is_control()
            || matches!(c, '"' | '\n' | '\r' | '{' | '}' | '\u{2028}' | '\u{2029}')
            || (!allow_backslash && c == '\\')
    }) || contains_vcl_percent_null(value)
    {
        return Err(Error::new(400, "invalid_vcl_literal"));
    }
    Ok(())
}

pub fn recv_snippet(exclude_url_regex: Option<&str>) -> Result<String, Error> {
    let effective = resolve_exclude_url_regex(exclude_url_regex);
    validate_vcl_literal(&effective, true)?;
    Ok(format!(
        r#"# Session Scoring: client-edge header scrub (anti-spoofing).
if (req.restarts == 0 && fastly.ff.visits_this_service == 0) {{
  unset req.http.X-Edge-Scoring-Pass;
  unset req.http.X-Edge-Score;
  unset req.http.X-Edge-Score-Reason;
  unset req.http.X-Edge-Score-Enforce;
  unset req.http.X-Edge-Sid;
  unset req.http.X-Edge-Score-Set-Cookie;
  unset req.http.X-Edge-Prev-Anchor;
  unset req.http.x-sigsci-skip-inspection-once;
}}

if (fastly.ff.visits_this_service == 0 && req.restarts == 0 && req.http.X-Edge-Scoring-Pass != "1" && !fastly.ddos_detected && std.tolower(req.url) !~ "{effective}") {{
  set req.backend = {SCORING_BACKEND_VCL_NAME};
  set req.http.x-sigsci-skip-inspection-once = "true";
  set req.http.X-Edge-Scoring-Pass = "1";
  set req.http.x-edge-score-t0 = time.elapsed.usec;
  return(pass);
}}

if (req.restarts == 1 && req.http.x-edge-score) {{
  set var.fastly_req_do_shield = true;
}}
if (req.restarts == 1) {{
  unset req.http.x-sigsci-skip-inspection-once;
}}"#
    ))
}

pub fn pass_snippet(logging_service_id: &str, request_secret: &str) -> Result<String, Error> {
    validate_vcl_literal(logging_service_id, false)?;
    validate_vcl_literal(request_secret, false)?;
    Ok(format!(
        r#"# Session Scoring: inject auth + service-id on the scorer sub-fetch.
if (req.backend == {SCORING_BACKEND_VCL_NAME}) {{
  set bereq.method = "GET";
  set bereq.http.X-Edge-Service-Id = "{logging_service_id}";
  set bereq.http.X-Edge-Scorer-Auth = "{request_secret}";
  unset bereq.http.X-Edge-Scoring-Pass;
  declare local var.scorer_state_cookie STRING;
  declare local var.scorer_ess_cookie STRING;
  set var.scorer_state_cookie = subfield(req.http.Cookie, "X-Session-State", ";");
  set var.scorer_ess_cookie = subfield(req.http.Cookie, "_ess", ";");
  unset bereq.http.Cookie;
  if (var.scorer_state_cookie != "" && var.scorer_ess_cookie != "") {{
    set bereq.http.Cookie = "X-Session-State=" + var.scorer_state_cookie + "; _ess=" + var.scorer_ess_cookie;
  }} else if (var.scorer_state_cookie != "") {{
    set bereq.http.Cookie = "X-Session-State=" + var.scorer_state_cookie;
  }} else if (var.scorer_ess_cookie != "") {{
    set bereq.http.Cookie = "_ess=" + var.scorer_ess_cookie;
  }}
}}
unset bereq.http.x-edge-score;"#
    ))
}

pub fn fetch_snippet() -> String {
    format!(
        r#"# Session Scoring: skip cache handling for the scorer sub-fetch.
if (req.backend == {SCORING_BACKEND_VCL_NAME}) {{
  return(deliver);
}}"#
    )
}

pub fn deliver_snippet() -> &'static str {
    r#"# Session Scoring: pass-1 stash + naked restart; pass-2 emit cookie.
if (req.http.X-Edge-Scoring-Pass == "1") {
  unset req.http.X-Edge-Scoring-Pass;
  if (req.http.x-edge-score-t0 != "") {
    declare local var.rtt INTEGER;
    set var.rtt = std.atoi(time.elapsed.usec);
    set var.rtt -= std.atoi(req.http.x-edge-score-t0);
    set req.http.x-edge-score:rtt = "" + var.rtt;
  }
  if (resp.status == 200) {
    set req.http.x-edge-score:score = resp.http.x-edge-score;
    set req.http.x-edge-score:l1 = resp.http.x-edge-score-l1;
    set req.http.x-edge-score:l2 = resp.http.x-edge-score-l2;
    set req.http.x-edge-score:compliance = resp.http.X-Edge-Cookie-Compliance;
    set req.http.x-edge-score:reason = resp.http.x-edge-score-reason;
    set req.http.x-edge-score:sid = resp.http.x-edge-sid;
    set req.http.x-edge-score:enforce = resp.http.x-edge-score-enforce;
    set req.http.x-edge-score:exec = resp.http.x-edge-score-exec-us;
    set req.http.x-edge-score:matrix = resp.http.X-Edge-Matrix-Version;
  } else {
    set req.http.x-edge-score:score = "0";
    set req.http.x-edge-score:l1 = "0";
    set req.http.x-edge-score:l2 = "0";
    set req.http.x-edge-score:compliance = "unknown";
    set req.http.x-edge-score:reason = "compute-unavailable-" + resp.status;
  }
  set req.http.x-edge-score:set-cookie = resp.http.Set-Cookie;
  unset resp.http.x-edge-score;
  unset resp.http.x-edge-score-l1;
  unset resp.http.x-edge-score-l2;
  unset resp.http.x-edge-score-reason;
  unset resp.http.x-edge-sid;
  unset resp.http.X-Edge-Cookie-Compliance;
  unset resp.http.X-Edge-Matrix-Version;
  unset resp.http.x-edge-score-enforce;
  unset resp.http.x-edge-score-exec-us;
  restart;
}

if (fastly.ff.visits_this_service == 0 && req.http.x-edge-score:set-cookie != "") {
  add resp.http.Set-Cookie = req.http.x-edge-score:set-cookie;
}"#
}

pub fn miss_snippet() -> &'static str {
    r#"# Session Scoring: strip internal scoring headers before forwarding to origin.
if (req.backend.is_origin) {
  unset bereq.http.x-edge-score;
  unset bereq.http.X-Edge-Scoring-Pass;
}"#
}

pub fn enforce_snippet(status_code: Option<u16>) -> String {
    let code = resolve_enforce_status_code(status_code);
    let reason = enforce_reason_phrase(code);
    format!(
        r#"# Session Scoring: enforce committed threshold by erroring flagged requests.
if (fastly.ff.visits_this_service == 0 && req.restarts == 1 && req.http.x-edge-score:enforce == "1") {{
  error {code} "{reason}";
}}"#
    )
}

pub fn desired_scoring_snippets(
    logging_service_id: &str,
    request_secret: &str,
    exclude_url_regex: Option<&str>,
    enforce_status_code: Option<u16>,
) -> Result<Vec<Value>, Error> {
    Ok(vec![
        json!({"name": SCORING_RECV_NAME, "subroutine": "vcl_recv", "priority": SCORING_SNIPPET_PRIORITY, "body": recv_snippet(exclude_url_regex)?}),
        json!({"name": SCORING_PASS_NAME, "subroutine": "vcl_pass", "priority": SCORING_SNIPPET_PRIORITY, "body": pass_snippet(logging_service_id, request_secret)?}),
        json!({"name": SCORING_FETCH_NAME, "subroutine": "vcl_fetch", "priority": SCORING_SNIPPET_PRIORITY, "body": fetch_snippet()}),
        json!({"name": SCORING_DELIVER_NAME, "subroutine": "vcl_deliver", "priority": SCORING_SNIPPET_PRIORITY, "body": deliver_snippet()}),
        json!({"name": SCORING_MISS_NAME, "subroutine": "vcl_miss", "priority": SCORING_SNIPPET_PRIORITY, "body": miss_snippet()}),
        json!({"name": SCORING_ENFORCE_NAME, "subroutine": "vcl_recv", "priority": SCORING_SNIPPET_PRIORITY, "body": enforce_snippet(enforce_status_code)}),
    ])
}

pub fn valid_scoring_domain(domain: &str) -> bool {
    if domain.len() > 253 {
        return false;
    }
    let Some(label) = domain.strip_suffix(".edgecompute.app") else {
        return false;
    };
    let bytes = label.as_bytes();
    (2..=63).contains(&bytes.len())
        && (bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
        && (bytes[bytes.len() - 1].is_ascii_lowercase() || bytes[bytes.len() - 1].is_ascii_digit())
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
}

pub fn checked_scoring_backend(scoring_domain: &str) -> Result<Value, Error> {
    if !valid_scoring_domain(scoring_domain) {
        return Err(Error::new(400, "invalid_scoring_domain"));
    }
    Ok(desired_scoring_backend(scoring_domain))
}

pub fn desired_scoring_backend(scoring_domain: &str) -> Value {
    json!({
        "name": SCORING_BACKEND_API_NAME,
        "address": scoring_domain,
        "port": 443,
        "use_ssl": true,
        "ssl_check_cert": true,
        "ssl_cert_hostname": scoring_domain,
        "ssl_sni_hostname": scoring_domain,
        "override_host": scoring_domain,
        "connect_timeout": 100,
        "first_byte_timeout": 100,
        "between_bytes_timeout": 200,
        "auto_loadbalance": false,
        "prefer_ipv6": false,
        "shield": "",
        "request_condition": "fastly_log_analytics_false",
    })
}

pub fn scorer_vcl_fingerprint(logging_service_id: &str) -> String {
    let snippets = serde_json::json!({
        SCORING_RECV_NAME: recv_snippet(None).unwrap_or_default(),
        SCORING_PASS_NAME: pass_snippet(logging_service_id, "drift-fingerprint").unwrap_or_default(),
        SCORING_FETCH_NAME: fetch_snippet(),
        SCORING_DELIVER_NAME: deliver_snippet(),
        SCORING_MISS_NAME: miss_snippet(),
        SCORING_ENFORCE_NAME: enforce_snippet(None),
    });
    hex::encode(Sha256::digest(snippets.to_string().as_bytes()))
}

fn push_uvarint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            out.push(byte | 0x80);
        } else {
            out.push(byte);
            return;
        }
    }
}

/// Encode a matrix JSON object into the FSM1 binary KV payload expected by `compute/scorer`.
pub fn serialize_kv(matrix: &Value) -> Result<Vec<u8>, Error> {
    let version = matrix["version"].as_str().unwrap_or("");
    let vocab_size = matrix["vocab_size"]
        .as_u64()
        .and_then(|v| u32::try_from(v).ok())
        .ok_or_else(|| Error::new(400, "invalid_matrix_vocab_size"))?;
    if vocab_size as usize > MAX_VOCAB_SIZE {
        return Err(Error::new(400, "matrix_vocab_too_large"));
    }
    let counts = matrix["counts"]
        .as_object()
        .ok_or_else(|| Error::new(400, "invalid_matrix_counts"))?;
    let row_totals = matrix["row_totals"]
        .as_object()
        .ok_or_else(|| Error::new(400, "invalid_matrix_row_totals"))?;

    let mut routes_set: BTreeSet<&str> = BTreeSet::new();
    for (src, row) in counts {
        routes_set.insert(src.as_str());
        let row_obj = row
            .as_object()
            .ok_or_else(|| Error::new(400, "invalid_matrix_row"))?;
        for dst in row_obj.keys() {
            routes_set.insert(dst.as_str());
        }
    }
    for src in row_totals.keys() {
        routes_set.insert(src.as_str());
    }
    let sorted_routes: Vec<&str> = routes_set.into_iter().collect();
    let n_routes =
        u32::try_from(sorted_routes.len()).map_err(|_| Error::new(400, "matrix_too_large"))?;
    let mut rid: BTreeMap<&str, u32> = BTreeMap::new();
    for (idx, route) in sorted_routes.iter().enumerate() {
        rid.insert(*route, idx as u32);
    }

    let ver_bytes = version.as_bytes();
    let ver_len =
        u16::try_from(ver_bytes.len()).map_err(|_| Error::new(400, "invalid_matrix_version"))?;

    let mut out = Vec::new();
    out.extend_from_slice(b"FSM1");
    out.push(1u8);
    out.extend_from_slice(&vocab_size.to_le_bytes());
    out.extend_from_slice(&n_routes.to_le_bytes());
    out.extend_from_slice(&ver_len.to_le_bytes());
    out.extend_from_slice(ver_bytes);

    let mut blob = Vec::new();
    let mut offsets: Vec<u32> = Vec::with_capacity(sorted_routes.len() + 1);
    offsets.push(0);
    for r in &sorted_routes {
        blob.extend_from_slice(r.as_bytes());
        let off =
            u32::try_from(blob.len()).map_err(|_| Error::new(400, "matrix_blob_too_large"))?;
        offsets.push(off);
    }
    for off in offsets {
        out.extend_from_slice(&off.to_le_bytes());
    }
    out.extend_from_slice(&blob);

    for r in &sorted_routes {
        let total = row_totals.get(*r).and_then(|v| v.as_u64()).unwrap_or(0);
        push_uvarint(&mut out, total);
    }

    let mut row_off: Vec<u64> = Vec::with_capacity(sorted_routes.len() + 1);
    row_off.push(0);
    let mut pair_bytes = Vec::new();
    for r in &sorted_routes {
        let mut cols: Vec<(u32, u64)> = Vec::new();
        if let Some(row) = counts.get(*r).and_then(|v| v.as_object()) {
            for (dst, cnt_val) in row {
                let col_id = *rid
                    .get(dst.as_str())
                    .ok_or_else(|| Error::new(400, "invalid_matrix_col"))?;
                let cnt = cnt_val
                    .as_u64()
                    .ok_or_else(|| Error::new(400, "invalid_matrix_count"))?;
                cols.push((col_id, cnt));
            }
        }
        cols.sort_by_key(|(col_id, _)| *col_id);
        for (col_id, cnt) in &cols {
            push_uvarint(&mut pair_bytes, u64::from(*col_id));
            push_uvarint(&mut pair_bytes, *cnt);
        }
        let prev = *row_off.last().unwrap_or(&0);
        row_off.push(prev + cols.len() as u64);
    }
    for off in row_off {
        push_uvarint(&mut out, off);
    }
    out.extend_from_slice(&pair_bytes);
    Ok(out)
}

fn hex_random_32(runtime: &impl Runtime) -> Result<String, Error> {
    let bytes = runtime
        .random()
        .map_err(|_| Error::new(503, "random_unavailable"))?;
    Ok(hex::encode(bytes))
}

fn scoring_service_name(sid: &str) -> String {
    format!("Session Scoring Service for {sid}")
}

const MAX_SCORING_DOMAIN_COLLISION_ATTEMPTS: usize = 5;

pub fn opaque_scoring_domain(runtime: &impl Runtime) -> Result<String, Error> {
    let bytes = runtime
        .random()
        .map_err(|_| Error::new(503, "random_unavailable"))?;
    Ok(format!("fla-{}.edgecompute.app", hex::encode(&bytes[..12])))
}

fn keys_store_name(scorer_sid: &str) -> String {
    format!("scoring_keys_{scorer_sid}")
}

fn config_store_name(scorer_sid: &str) -> String {
    format!("scoring_config_{scorer_sid}")
}

fn matrix_store_name(scorer_sid: &str) -> String {
    format!("scoring_matrix_{scorer_sid}")
}

pub fn request_secret_ref(sid: &str) -> String {
    format!("scoring_request_secret.{sid}")
}

pub(super) fn load_or_recover_request_secret(
    context: &Context<'_, impl Runtime>,
    service: &Service,
    token: &str,
) -> Result<Option<String>, Error> {
    let secret_ref = request_secret_ref(&service.service_id);
    match context.secret(&secret_ref) {
        Ok(secret) => return Ok(Some(secret)),
        Err(err) if err.code != "credentials_pending" => return Err(err),
        Err(_) => {}
    }
    let sc = service
        .scoring
        .as_ref()
        .filter(|sc| !sc.scoring_keys_store_id.is_empty())
        .ok_or_else(|| Error::new(409, "credentials_pending"))?;
    if context.remaining_calls() < 10 {
        return Err(Error::new(503, "backend_budget"));
    }
    verify_scoring_resources(context, &service.service_id, sc, token)?;
    let recovered = config_store_get_item(
        context,
        &sc.scoring_keys_store_id,
        REQUEST_SECRET_KEY,
        token,
    )?;
    if recovered.trim().is_empty() {
        return Err(Error::new(409, "credentials_pending"));
    }
    validate_vcl_literal(&recovered, false)?;
    put_secret(context, token, &secret_ref, &recovered)?;
    Ok(None)
}

fn resolve_brokered_token(
    context: &Context<'_, impl Runtime>,
    service: &Service,
    optional_token: Option<&str>,
) -> Result<String, Error> {
    if let Some(tok) = optional_token.map(str::trim).filter(|t| !t.is_empty()) {
        let customer = validate_token(context, tok, Some(&service.service_id))?;
        let target = context.api(
            "GET",
            &format!("/service/{}", service.service_id),
            tok,
            None,
        )?;
        if target["customer_id"] != customer {
            return Err(Error::new(403, "wrong_account"));
        }
        put_secret(context, tok, &service.fastly_token_ref, tok)?;
        return Ok(tok.to_owned());
    }
    match context.secret(&service.fastly_token_ref) {
        Ok(tok) if !tok.is_empty() => Ok(tok),
        Err(e) if e.status >= 500 => Err(e),
        _ => Err(Error::new(403, "fastly_token_scope_insufficient")),
    }
}

fn check_entitlement(
    context: &Context<'_, impl Runtime>,
    token: &str,
    product: &str,
    err_code: &'static str,
) -> Result<(), Error> {
    let (status, body) = context.api_status(
        "GET",
        &format!("/enabled-products/v1/{product}"),
        token,
        None,
    )?;
    if status == 401 || status == 403 {
        return Err(Error::new(403, "fastly_token_scope_insufficient"));
    }
    if status == 404 || (status == 200 && body["enabled"].as_bool() == Some(false)) {
        return Err(Error::new(403, err_code));
    }
    if !(200..300).contains(&status) {
        return Err(Error::new(502, "entitlement_check_failed"));
    }
    Ok(())
}

pub fn service_has_scoring_pragmas(
    context: &Context<'_, impl Runtime>,
    sid: &str,
    token: &str,
) -> Option<bool> {
    let ver = reconcile::active_version(context, sid, token).ok()?;
    service_has_scoring_pragmas_at_version(context, sid, ver, token)
}

fn service_has_scoring_pragmas_at_version(
    context: &Context<'_, impl Runtime>,
    sid: &str,
    ver: u64,
    token: &str,
) -> Option<bool> {
    let (status, body) = context
        .api_status(
            "GET",
            &format!("/service/{sid}/version/{ver}/generated_vcl"),
            token,
            None,
        )
        .ok()?;
    if !(200..300).contains(&status) {
        return None;
    }
    let content = body["content"].as_str()?;
    let has_drain = content.contains("fix_unsent_body_drain true")
        || content.contains("fix_unsent_body_drain  true");
    let has_no_body = content.contains("no_body_if_bereq_is_get_or_head true")
        || content.contains("no_body_if_bereq_is_get_or_head  true");
    Some(has_drain && has_no_body)
}

fn append_scoring_audit(
    context: &Context<'_, impl Runtime>,
    sid: &str,
    actor: &str,
    action: &str,
    details: Value,
) {
    let key = format!("scoring:audit:{sid}");
    let now = context.runtime.now();
    let entry = json!({
        "at": rfc3339_from_secs(now),
        "ts": now,
        "actor": actor,
        "action": action,
        "details": details,
    });
    context.runtime.admin_event(json!({
        "event": "scoring.audit",
        "service_id": sid,
        "action": action,
        "actor": actor,
    }));
    if let Ok(existing) = context.get(&key) {
        let mut entries: Vec<Value> = existing
            .as_ref()
            .and_then(|r| r.value["entries"].as_array())
            .cloned()
            .unwrap_or_default();
        entries.insert(0, entry);
        entries.truncate(MAX_AUDIT_ENTRIES);
        let _ = context.cas(
            &key,
            existing.as_ref().map(|r| r.generation.as_str()),
            &json!({"entries": entries}),
        );
    }
}

fn find_store_by_name(
    context: &Context<'_, impl Runtime>,
    endpoint: &str,
    name: &str,
    token: &str,
) -> Result<Option<Value>, Error> {
    let (status, resp) = context.api_status("GET", endpoint, token, None)?;
    if status == 401 || status == 403 {
        return Err(Error::new(403, "fastly_token_scope_insufficient"));
    }
    if !(200..300).contains(&status) {
        return Err(Error::new(502, "store_lookup_failed"));
    }
    let items = if let Some(arr) = resp.as_array() {
        arr.clone()
    } else if let Some(arr) = resp["data"].as_array() {
        arr.clone()
    } else {
        vec![]
    };
    Ok(items.into_iter().find(|item| item["name"] == name))
}

fn store_id_of(val: &Value) -> Option<String> {
    val["id"]
        .as_str()
        .or_else(|| val["data"]["id"].as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

fn check_scoring_store_id(runtime: &impl Runtime, store_id: &str) -> Result<(), Error> {
    if !valid_store_id(store_id) || own_stores(runtime).iter().any(|id| id == store_id) {
        return Err(Error::new(400, "invalid_scoring_store_id"));
    }
    Ok(())
}

fn verify_single_store_ownership(
    context: &Context<'_, impl Runtime>,
    kind: &str,
    store_id: &str,
    expected_name: &str,
    token: &str,
    allow_missing: bool,
) -> Result<bool, Error> {
    check_scoring_store_id(context.runtime, store_id)?;
    let encoded = encode(store_id);
    let (status, resp) = context.api_status(
        "GET",
        &format!("/resources/stores/{kind}/{encoded}"),
        token,
        None,
    )?;
    if status == 401 || status == 403 {
        return Err(Error::new(403, "fastly_token_scope_insufficient"));
    }
    if status == 404 {
        if allow_missing {
            return Ok(false);
        }
        return Err(Error::new(400, "invalid_scoring_store_id"));
    }
    if !(200..300).contains(&status) {
        return Err(Error::new(502, "store_lookup_failed"));
    }
    let actual_name = resp["name"]
        .as_str()
        .or_else(|| resp["data"]["name"].as_str());
    if actual_name != Some(expected_name) {
        return Err(Error::new(400, "invalid_scoring_store_id"));
    }
    Ok(true)
}

pub(super) fn verify_scoring_resources(
    context: &Context<'_, impl Runtime>,
    service_id: &str,
    sc: &ScoringConfig,
    token: &str,
) -> Result<(), Error> {
    if !valid_service_id(&sc.scoring_service_id)
        || sc.scoring_service_id == service_id
        || own_services(context.runtime)?
            .iter()
            .any(|id| id == &sc.scoring_service_id)
    {
        return Err(Error::new(400, "invalid_scoring_service_id"));
    }
    let encoded_sid = encode(&sc.scoring_service_id);
    let (svc_status, svc_body) =
        context.api_status("GET", &format!("/service/{encoded_sid}"), token, None)?;
    if svc_status == 401 || svc_status == 403 {
        return Err(Error::new(403, "fastly_token_scope_insufficient"));
    }
    if svc_status == 404 {
        return Err(Error::new(400, "invalid_scoring_service_id"));
    }
    if !(200..300).contains(&svc_status) {
        return Err(Error::new(502, "scorer_service_lookup_failed"));
    }
    let expected_svc_name = scoring_service_name(service_id);
    if svc_body["type"].as_str() != Some("wasm")
        || svc_body["name"].as_str() != Some(expected_svc_name.as_str())
    {
        return Err(Error::new(400, "invalid_scoring_service_id"));
    }
    let need_keys = sc.enabled || !sc.scoring_keys_store_id.is_empty();
    let need_cfg = sc.enabled || !sc.scoring_config_store_id.is_empty();
    let need_matrix = sc.enabled || !sc.scoring_matrix_store_id.is_empty();
    let keys_expected = keys_store_name(&sc.scoring_service_id);
    let cfg_expected = config_store_name(&sc.scoring_service_id);
    if need_keys && need_cfg {
        check_scoring_store_id(context.runtime, &sc.scoring_keys_store_id)?;
        check_scoring_store_id(context.runtime, &sc.scoring_config_store_id)?;
        let (cfg_status, cfg_resp) =
            context.api_status("GET", "/resources/stores/config", token, None)?;
        if cfg_status == 401 || cfg_status == 403 {
            return Err(Error::new(403, "fastly_token_scope_insufficient"));
        }
        if !(200..300).contains(&cfg_status) {
            return Err(Error::new(502, "store_lookup_failed"));
        }
        let items: &[Value] = if let Some(arr) = cfg_resp.as_array() {
            arr.as_slice()
        } else if let Some(arr) = cfg_resp["data"].as_array() {
            arr.as_slice()
        } else {
            &[]
        };
        for (store_id, expected_name) in [
            (sc.scoring_keys_store_id.as_str(), keys_expected.as_str()),
            (sc.scoring_config_store_id.as_str(), cfg_expected.as_str()),
        ] {
            if let Some(found) = items
                .iter()
                .find(|it| store_id_of(it).as_deref() == Some(store_id))
            {
                let actual_name = found["name"]
                    .as_str()
                    .or_else(|| found["data"]["name"].as_str());
                if actual_name != Some(expected_name) {
                    return Err(Error::new(400, "invalid_scoring_store_id"));
                }
            } else {
                verify_single_store_ownership(
                    context,
                    "config",
                    store_id,
                    expected_name,
                    token,
                    false,
                )?;
            }
        }
    } else {
        if need_keys {
            verify_single_store_ownership(
                context,
                "config",
                &sc.scoring_keys_store_id,
                &keys_expected,
                token,
                false,
            )?;
        }
        if need_cfg {
            verify_single_store_ownership(
                context,
                "config",
                &sc.scoring_config_store_id,
                &cfg_expected,
                token,
                false,
            )?;
        }
    }
    if need_matrix {
        verify_single_store_ownership(
            context,
            "kv",
            &sc.scoring_matrix_store_id,
            &matrix_store_name(&sc.scoring_service_id),
            token,
            false,
        )?;
    }
    Ok(())
}

fn write_matrix_to_kv(
    context: &Context<'_, impl Runtime>,
    matrix_store_id: &str,
    matrix: &Value,
    token: &str,
) -> Result<(), Error> {
    if matrix_store_id.is_empty() {
        return Err(Error::new(400, "scoring_matrix_store_missing"));
    }
    check_scoring_store_id(context.runtime, matrix_store_id)?;
    let encoded_store = encode(matrix_store_id);
    let payload = serialize_kv(matrix)?;
    context.api_raw(
        "PUT",
        &format!("/resources/stores/kv/{encoded_store}/keys/{MATRIX_KEY_NAME}"),
        token,
        "application/octet-stream",
        &payload,
    )?;
    Ok(())
}

fn config_store_get_item(
    context: &Context<'_, impl Runtime>,
    store_id: &str,
    key: &str,
    token: &str,
) -> Result<String, Error> {
    if store_id.is_empty() {
        return Ok(String::new());
    }
    check_scoring_store_id(context.runtime, store_id)?;
    let encoded_store = encode(store_id);
    let encoded_key = encode(key);
    let (status, body) = context.api_status(
        "GET",
        &format!("/resources/stores/config/{encoded_store}/item/{encoded_key}"),
        token,
        None,
    )?;
    if status == 404 {
        return Ok(String::new());
    }
    if status == 401 || status == 403 {
        return Err(Error::new(403, "fastly_token_scope_insufficient"));
    }
    if !(200..300).contains(&status) {
        return Err(Error::new(502, "config_store_read_failed"));
    }
    Ok(body["item_value"].as_str().unwrap_or("").to_owned())
}

fn config_store_upsert_item(
    context: &Context<'_, impl Runtime>,
    store_id: &str,
    key: &str,
    value: &str,
    token: &str,
) -> Result<(), Error> {
    check_scoring_store_id(context.runtime, store_id)?;
    let encoded_store = encode(store_id);
    let encoded_key = encode(key);
    let (status, _) = context.api_status(
        "PUT",
        &format!("/resources/stores/config/{encoded_store}/item/{encoded_key}"),
        token,
        Some(json!({"item_value": value})),
    )?;
    if status == 404 {
        context.api(
            "POST",
            &format!("/resources/stores/config/{encoded_store}/item"),
            token,
            Some(json!({"item_key": key, "item_value": value})),
        )?;
        return Ok(());
    }
    if status == 401 || status == 403 {
        return Err(Error::new(403, "fastly_token_scope_insufficient"));
    }
    if !(200..300).contains(&status) {
        return Err(Error::new(502, "config_store_write_failed"));
    }
    Ok(())
}

fn fos_json_get_with_creds(
    context: &Context<'_, impl Runtime>,
    service: &Service,
    creds: &(String, String),
    key: &str,
) -> Result<Option<Value>, Error> {
    let Some(bytes) = fos::get_object(context, service, creds, key)? else {
        return Ok(None);
    };
    let parsed: Value =
        serde_json::from_slice(&bytes).map_err(|_| Error::new(502, "fos_json_invalid"))?;
    Ok(Some(parsed))
}

fn fos_json_get(
    context: &Context<'_, impl Runtime>,
    service: &Service,
    key: &str,
) -> Result<Option<Value>, Error> {
    let creds = fos::credentials(context, service)?;
    fos_json_get_with_creds(context, service, &creds, key)
}

fn fos_json_put_with_creds(
    context: &Context<'_, impl Runtime>,
    service: &Service,
    creds: &(String, String),
    key: &str,
    value: &Value,
) -> Result<(), Error> {
    let bytes = serde_json::to_vec(value).map_err(|_| Error::new(500, "state_encoding_failed"))?;
    fos::put_object(context, service, creds, key, bytes)
}

fn fos_json_put(
    context: &Context<'_, impl Runtime>,
    service: &Service,
    key: &str,
    value: &Value,
) -> Result<(), Error> {
    let creds = fos::credentials(context, service)?;
    fos_json_put_with_creds(context, service, &creds, key, value)
}

fn scoring_fos_prefix(service: &Service) -> String {
    format!("{}processed/scoring/", crate::ingest::scope(service))
}

fn save_scoring_service(
    context: &Context<'_, impl Runtime>,
    record: &Record,
    service: &Service,
    actor: &str,
    event: &str,
) -> Result<String, Error> {
    save_service(context, record, service, actor, event)?;
    let updated = context
        .get(&format!("svc:{}", service.service_id))?
        .ok_or_else(|| Error::new(503, "state_unavailable"))?;
    Ok(updated.generation)
}

fn checkpoint_scoring_job_service(
    context: &Context<'_, impl Runtime>,
    record: &Record,
    service: &Service,
    job: &mut Job,
    event: &str,
    next: &str,
) -> Result<(), Error> {
    save_job_service(context, record, service, job, event, next)?;
    let updated = context
        .get(&format!("svc:{}", service.service_id))?
        .ok_or_else(|| Error::new(503, "state_unavailable"))?;
    job.expected_generation = updated.generation;
    Ok(())
}

fn checkpoint_scoring_rollback_service(
    context: &Context<'_, impl Runtime>,
    record: &Record,
    service: &Service,
    job: &mut Job,
    event: &str,
    next: &str,
) -> Result<(), Error> {
    save_job_service_recovery(context, record, service, job, event, next)?;
    let updated = context
        .get(&format!("svc:{}", service.service_id))?
        .ok_or_else(|| Error::new(503, "state_unavailable"))?;
    job.expected_generation = updated.generation;
    Ok(())
}

fn budget_yield(progressed: bool) -> Result<(), Error> {
    if progressed {
        Ok(())
    } else {
        Err(Error::new(503, "backend_budget"))
    }
}

fn ensure_scorer_service_step(
    context: &Context<'_, impl Runtime>,
    job: &mut Job,
    record: &Record,
    service: &mut Service,
    token: &str,
) -> Result<(), Error> {
    let mut progressed = false;
    if !job.scorer_ownership_verified {
        reconcile::verify_managed_customer_vcl_ownership(context, service, token)?;
        job.scorer_ownership_verified = true;
        progressed = true;
        if context.remaining_calls() < 16 {
            return Ok(());
        }
    }
    check_entitlement(context, token, "kv_store", "kv_store_not_enabled")?;
    let mut current_record = record.clone();
    let name = scoring_service_name(&service.service_id);
    let mut domain = service
        .scoring
        .as_ref()
        .map(|s| s.scoring_domain.trim())
        .filter(|d| !d.is_empty())
        .map(str::to_owned);
    if domain.is_none() {
        let minted = opaque_scoring_domain(context.runtime)?;
        let mut sc = service.scoring.clone().unwrap_or_default();
        sc.scoring_domain = minted.clone();
        service.scoring = Some(sc);
        checkpoint_scoring_job_service(
            context,
            &current_record,
            service,
            job,
            "scoring.domain_assigned",
            "scorer_service",
        )?;
        current_record = context
            .get(&format!("svc:{}", service.service_id))?
            .ok_or_else(|| Error::new(503, "state_unavailable"))?;
        domain = Some(minted);
    }
    let mut domain = domain.unwrap_or_default();
    let existing_scorer_sid = service
        .scoring
        .as_ref()
        .map(|s| s.scoring_service_id.trim())
        .filter(|s| !s.is_empty())
        .map(str::to_owned);
    let services = context.api("GET", "/service", token, None)?;
    let existing = existing_scorer_sid
        .as_deref()
        .and_then(|sid| {
            list(&services)
                .ok()?
                .iter()
                .find(|s| s["id"].as_str() == Some(sid))
                .cloned()
        })
        .or_else(|| {
            list(&services)
                .ok()?
                .iter()
                .find(|s| s["name"] == name)
                .cloned()
        });
    let (scorer_sid, v1_unlocked) = if let Some(svc) = existing {
        let sid = required(&svc, "id")?.to_owned();
        let encoded_sid = encode(&sid);
        let versions = context.api(
            "GET",
            &format!("/service/{encoded_sid}/version"),
            token,
            None,
        )?;
        let unlocked = list(&versions)?
            .iter()
            .find(|v| v["number"].as_u64() == Some(1))
            .is_some_and(|v| {
                v["active"].as_bool() != Some(true) && v["locked"].as_bool() != Some(true)
            });
        (sid, unlocked)
    } else {
        if context.remaining_calls() < 8 {
            return budget_yield(progressed);
        }
        let (status, created) = context.api_status(
            "POST",
            "/service",
            token,
            Some(json!({"name": name, "type": "wasm"})),
        )?;
        if status == 401 || status == 403 {
            return Err(Error::new(403, "fastly_token_scope_insufficient"));
        }
        if (400..500).contains(&status) {
            return Err(Error::new(403, "compute_not_enabled"));
        }
        if !(200..300).contains(&status) {
            return Err(Error::new(502, "scorer_service_create_failed"));
        }
        progressed = true;
        let sid = required(&created, "id")?.to_owned();
        (sid, true)
    };

    if v1_unlocked {
        let encoded_sid = encode(&scorer_sid);
        let domains = context.api(
            "GET",
            &format!("/service/{encoded_sid}/version/1/domain"),
            token,
            None,
        )?;
        let existing_domain = list(&domains)?
            .iter()
            .filter_map(|d| d["name"].as_str())
            .find(|d| valid_scoring_domain(d))
            .map(str::to_owned);
        if let Some(existing_domain) = existing_domain {
            domain = existing_domain;
        } else {
            let mut attached = false;
            for attempt in 0..MAX_SCORING_DOMAIN_COLLISION_ATTEMPTS {
                if context.remaining_calls() < 8 {
                    return budget_yield(progressed);
                }
                let (status, _) = context.api_status(
                    "POST",
                    &format!("/service/{encoded_sid}/version/1/domain"),
                    token,
                    Some(json!({"name": domain})),
                )?;
                if (200..300).contains(&status) {
                    progressed = true;
                    attached = true;
                    break;
                }
                if status == 409 {
                    if attempt + 1 == MAX_SCORING_DOMAIN_COLLISION_ATTEMPTS {
                        return Err(Error::new(409, "scoring_domain_collision"));
                    }
                    domain = opaque_scoring_domain(context.runtime)?;
                    let mut sc = service.scoring.clone().unwrap_or_default();
                    sc.scoring_domain = domain.clone();
                    service.scoring = Some(sc);
                    checkpoint_scoring_job_service(
                        context,
                        &current_record,
                        service,
                        job,
                        "scoring.domain_rotated",
                        "scorer_service",
                    )?;
                    current_record = context
                        .get(&format!("svc:{}", service.service_id))?
                        .ok_or_else(|| Error::new(503, "state_unavailable"))?;
                    progressed = true;
                    continue;
                }
                return Err(Error::new(
                    if status == 401 || status == 403 {
                        403
                    } else {
                        502
                    },
                    "fastly_api_rejected",
                ));
            }
            if !attached {
                return Err(Error::new(409, "scoring_domain_collision"));
            }
        }
        let backends = context.api(
            "GET",
            &format!("/service/{encoded_sid}/version/1/backend"),
            token,
            None,
        )?;
        let has_backend = list(&backends)?
            .iter()
            .any(|b| b["name"].as_str() == Some("placeholder_origin"));
        if !has_backend {
            if context.remaining_calls() < 8 {
                return budget_yield(progressed);
            }
            let (status, _) = context.api_status(
                "POST",
                &format!("/service/{encoded_sid}/version/1/backend"),
                token,
                Some(json!({
                    "name": "placeholder_origin",
                    "address": "127.0.0.1",
                    "port": 80,
                    "override_host": "example.com"
                })),
            )?;
            if !(200..300).contains(&status) && status != 409 {
                return Err(Error::new(
                    if status == 401 || status == 403 {
                        403
                    } else {
                        502
                    },
                    "fastly_api_rejected",
                ));
            }
            progressed = true;
        }
    }

    if context.remaining_calls() < 6 {
        return budget_yield(progressed);
    }
    let mut sc = service.scoring.clone().unwrap_or_default();
    sc.scoring_service_id = scorer_sid;
    sc.scoring_service_name = name;
    sc.scoring_domain = domain;
    if sc.exclude_url_regex.is_empty() {
        sc.exclude_url_regex = DEFAULT_ASSET_EXT_REGEX.to_owned();
    }
    service.scoring = Some(sc);
    checkpoint_scoring_job_service(
        context,
        &current_record,
        service,
        job,
        "scoring.scorer_service",
        "scorer_stores",
    )?;
    advance(job, "scorer_stores");
    Ok(())
}

fn store_items(resp: &Value) -> &[Value] {
    if let Some(arr) = resp.as_array() {
        arr.as_slice()
    } else if let Some(arr) = resp["data"].as_array() {
        arr.as_slice()
    } else {
        &[]
    }
}

fn ensure_scorer_stores_step(
    context: &Context<'_, impl Runtime>,
    job: &mut Job,
    record: &Record,
    service: &mut Service,
    token: &str,
) -> Result<(), Error> {
    let mut progressed = false;
    let mut sc = service
        .scoring
        .clone()
        .ok_or_else(|| Error::new(409, "scoring_config_missing"))?;
    let scorer_sid = sc.scoring_service_id.clone();

    // List /resources/stores/config once for both scoring_keys and scoring_config.
    let (cfg_list_status, cfg_list_resp) =
        context.api_status("GET", "/resources/stores/config", token, None)?;
    if cfg_list_status == 401 || cfg_list_status == 403 {
        return Err(Error::new(403, "fastly_token_scope_insufficient"));
    }
    if !(200..300).contains(&cfg_list_status) {
        return Err(Error::new(502, "store_lookup_failed"));
    }
    let cfg_items = store_items(&cfg_list_resp);

    let keys_name = keys_store_name(&scorer_sid);
    let keys_store_id = if let Some(found) = cfg_items.iter().find(|it| it["name"] == keys_name) {
        store_id_of(found).ok_or_else(|| Error::new(502, "config_store_id_missing"))?
    } else {
        if context.remaining_calls() < 9 {
            return budget_yield(progressed);
        }
        let (status, created) = context.api_status(
            "POST",
            "/resources/stores/config",
            token,
            Some(json!({"name": keys_name})),
        )?;
        if (400..500).contains(&status) {
            return Err(Error::new(403, "config_store_not_enabled"));
        }
        if !(200..300).contains(&status) {
            return Err(Error::new(502, "config_store_create_failed"));
        }
        progressed = true;
        store_id_of(&created).ok_or_else(|| Error::new(502, "config_store_id_missing"))?
    };
    sc.scoring_keys_store_id = keys_store_id;

    let cfg_name = config_store_name(&scorer_sid);
    let cfg_store_id = if let Some(found) = cfg_items.iter().find(|it| it["name"] == cfg_name) {
        store_id_of(found).ok_or_else(|| Error::new(502, "config_store_id_missing"))?
    } else {
        if context.remaining_calls() < 9 {
            if service.scoring.as_ref() != Some(&sc) && context.remaining_calls() >= 6 {
                service.scoring = Some(sc);
                checkpoint_scoring_job_service(
                    context,
                    record,
                    service,
                    job,
                    "scoring.scorer_stores",
                    "scorer_stores",
                )?;
                return Ok(());
            }
            return budget_yield(progressed);
        }
        let (status, created) = context.api_status(
            "POST",
            "/resources/stores/config",
            token,
            Some(json!({"name": cfg_name})),
        )?;
        if (400..500).contains(&status) {
            return Err(Error::new(403, "config_store_not_enabled"));
        }
        if !(200..300).contains(&status) {
            return Err(Error::new(502, "config_store_create_failed"));
        }
        progressed = true;
        store_id_of(&created).ok_or_else(|| Error::new(502, "config_store_id_missing"))?
    };
    sc.scoring_config_store_id = cfg_store_id;

    let matrix_name = matrix_store_name(&scorer_sid);
    let matrix_store_id = if let Some(found) =
        find_store_by_name(context, "/resources/stores/kv", &matrix_name, token)?
    {
        store_id_of(&found).ok_or_else(|| Error::new(502, "kv_store_id_missing"))?
    } else {
        if context.remaining_calls() < 9 {
            if service.scoring.as_ref() != Some(&sc) && context.remaining_calls() >= 6 {
                service.scoring = Some(sc);
                checkpoint_scoring_job_service(
                    context,
                    record,
                    service,
                    job,
                    "scoring.scorer_stores",
                    "scorer_stores",
                )?;
                return Ok(());
            }
            return budget_yield(progressed);
        }
        let (status, created) = context.api_status(
            "POST",
            "/resources/stores/kv",
            token,
            Some(json!({"name": matrix_name})),
        )?;
        if (400..500).contains(&status) {
            return Err(Error::new(403, "kv_store_not_enabled"));
        }
        if !(200..300).contains(&status) {
            return Err(Error::new(502, "kv_store_create_failed"));
        }
        progressed = true;
        store_id_of(&created).ok_or_else(|| Error::new(502, "kv_store_id_missing"))?
    };

    sc.scoring_matrix_store_id = matrix_store_id;
    if context.remaining_calls() < 6 {
        return budget_yield(progressed);
    }
    service.scoring = Some(sc);
    checkpoint_scoring_job_service(
        context,
        record,
        service,
        job,
        "scoring.scorer_stores",
        "scorer_store_keys",
    )?;
    job.scorer_ownership_verified = true;
    advance(job, "scorer_store_keys");
    Ok(())
}

fn ensure_scorer_store_keys_step(
    context: &Context<'_, impl Runtime>,
    job: &mut Job,
    service: &Service,
    token: &str,
) -> Result<(), Error> {
    let mut progressed = false;
    let sc = service
        .scoring
        .as_ref()
        .ok_or_else(|| Error::new(409, "scoring_config_missing"))?;
    if !job.scorer_ownership_verified {
        verify_scoring_resources(context, &service.service_id, sc, token)?;
        job.scorer_ownership_verified = true;
        progressed = true;
        if context.remaining_calls() < 14 {
            return Ok(());
        }
    }
    let keys_store_id = &sc.scoring_keys_store_id;
    let encoded_keys_store = encode(keys_store_id);

    // Write-only AES key: never stored in Cloud Secret Store or KV.
    let existing_aes = config_store_get_item(context, keys_store_id, CURRENT_KEY_HEX, token)?;
    if existing_aes.is_empty() {
        if context.remaining_calls() < 8 {
            return budget_yield(progressed);
        }
        let aes_key_hex = hex_random_32(context.runtime)?;
        context.api(
            "POST",
            &format!("/resources/stores/config/{encoded_keys_store}/item"),
            token,
            Some(json!({"item_key": CURRENT_KEY_HEX, "item_value": aes_key_hex})),
        )?;
        progressed = true;
    }

    // Shared VCL <-> scorer secret: stored in Cloud Secret Store AND scorer ConfigStore.
    let req_secret_ref = request_secret_ref(&service.service_id);
    let existing_req_secret =
        config_store_get_item(context, keys_store_id, REQUEST_SECRET_KEY, token)?;
    let req_secret = if !existing_req_secret.is_empty() {
        existing_req_secret
    } else {
        if context.remaining_calls() < 8 {
            return budget_yield(progressed);
        }
        let secret = hex_random_32(context.runtime)?;
        context.api(
            "POST",
            &format!("/resources/stores/config/{encoded_keys_store}/item"),
            token,
            Some(json!({"item_key": REQUEST_SECRET_KEY, "item_value": secret})),
        )?;
        progressed = true;
        secret
    };
    if context.remaining_calls() < 8 {
        return budget_yield(progressed);
    }
    put_secret(context, token, &req_secret_ref, &req_secret)?;

    advance(job, "scorer_store_config");
    Ok(())
}

fn ensure_scorer_store_config_step(
    context: &Context<'_, impl Runtime>,
    job: &mut Job,
    service: &Service,
    token: &str,
) -> Result<(), Error> {
    let mut progressed = false;
    let sc = service
        .scoring
        .as_ref()
        .ok_or_else(|| Error::new(409, "scoring_config_missing"))?;
    if !job.scorer_ownership_verified {
        verify_scoring_resources(context, &service.service_id, sc, token)?;
        job.scorer_ownership_verified = true;
        progressed = true;
        if context.remaining_calls() < 14 {
            return Ok(());
        }
    }
    let scorer_sid = encode(&sc.scoring_service_id);
    let keys_store_id = &sc.scoring_keys_store_id;
    let cfg_store_id = &sc.scoring_config_store_id;
    let encoded_cfg_store = encode(cfg_store_id);
    let matrix_store_id = &sc.scoring_matrix_store_id;

    // Config store defaults (write-once scoring_enabled_at).
    if config_store_get_item(context, cfg_store_id, DEBUG_LOG_KEY, token)?.is_empty() {
        if context.remaining_calls() < 8 {
            return budget_yield(progressed);
        }
        context.api(
            "POST",
            &format!("/resources/stores/config/{encoded_cfg_store}/item"),
            token,
            Some(json!({"item_key": DEBUG_LOG_KEY, "item_value": "0"})),
        )?;
        progressed = true;
    }
    if config_store_get_item(context, cfg_store_id, SCORING_ENABLED_AT_KEY, token)?.is_empty() {
        if context.remaining_calls() < 8 {
            return budget_yield(progressed);
        }
        let now_s = context.runtime.now().to_string();
        context.api(
            "POST",
            &format!("/resources/stores/config/{encoded_cfg_store}/item"),
            token,
            Some(json!({"item_key": SCORING_ENABLED_AT_KEY, "item_value": now_s})),
        )?;
        progressed = true;
    }

    if context.remaining_calls() < 6 {
        return budget_yield(progressed);
    }
    let versions = context.api(
        "GET",
        &format!("/service/{scorer_sid}/version"),
        token,
        None,
    )?;
    let v1_unlocked = list(&versions)?
        .iter()
        .find(|v| v["number"].as_u64() == Some(1))
        .is_some_and(|v| {
            v["active"].as_bool() != Some(true) && v["locked"].as_bool() != Some(true)
        });
    if v1_unlocked
        && !sync_scorer_resource_links(
            context,
            &scorer_sid,
            1,
            [
                (KEYS_RESOURCE_LINK_NAME, keys_store_id.as_str()),
                (CONFIG_RESOURCE_LINK_NAME, cfg_store_id.as_str()),
                (MATRIX_RESOURCE_LINK_NAME, matrix_store_id.as_str()),
            ],
            token,
            &mut progressed,
        )?
    {
        return budget_yield(progressed);
    }

    job.scorer_ownership_verified = false;
    advance(job, "scorer_deploy");
    Ok(())
}

fn sync_scorer_resource_links(
    context: &Context<'_, impl Runtime>,
    scorer_sid: &str,
    version: u64,
    links: [(&str, &str); 3],
    token: &str,
    progressed: &mut bool,
) -> Result<bool, Error> {
    if context.remaining_calls() < 6 {
        return if *progressed {
            Ok(false)
        } else {
            Err(Error::new(503, "backend_budget"))
        };
    }
    let existing_res = context.api(
        "GET",
        &format!("/service/{scorer_sid}/version/{version}/resource"),
        token,
        None,
    )?;
    let existing_links = list(&existing_res)?;
    for (link_name, res_id) in links {
        if res_id.is_empty() {
            continue;
        }
        if let Some(existing) = existing_links
            .iter()
            .find(|r| r["name"].as_str() == Some(link_name))
        {
            if existing["resource_id"].as_str() == Some(res_id) {
                continue;
            }
            if context.remaining_calls() < 8 {
                return if *progressed {
                    Ok(false)
                } else {
                    Err(Error::new(503, "backend_budget"))
                };
            }
            let link_id = existing["id"]
                .as_str()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| Error::new(502, "scorer_resource_link_failed"))?;
            let (del_status, _) = context.api_status(
                "DELETE",
                &format!(
                    "/service/{scorer_sid}/version/{version}/resource/{}",
                    encode(link_id)
                ),
                token,
                None,
            )?;
            if !(200..300).contains(&del_status) && del_status != 404 {
                return Err(Error::new(502, "scorer_resource_link_failed"));
            }
            *progressed = true;
        }
        if context.remaining_calls() < 8 {
            return if *progressed {
                Ok(false)
            } else {
                Err(Error::new(503, "backend_budget"))
            };
        }
        let (status, post_body) = context.api_status(
            "POST",
            &format!("/service/{scorer_sid}/version/{version}/resource"),
            token,
            Some(json!({"name": link_name, "resource_id": res_id})),
        )?;
        if (200..300).contains(&status) {
            *progressed = true;
            continue;
        }
        if status == 409 {
            let matches_body = post_body["resource_id"].as_str() == Some(res_id);
            let matches_live = if matches_body {
                true
            } else {
                let refreshed = context.api(
                    "GET",
                    &format!("/service/{scorer_sid}/version/{version}/resource"),
                    token,
                    None,
                )?;
                list(&refreshed)?.iter().any(|r| {
                    r["name"].as_str() == Some(link_name)
                        && r["resource_id"].as_str() == Some(res_id)
                })
            };
            if matches_live {
                continue;
            }
        }
        return Err(Error::new(502, "scorer_resource_link_failed"));
    }
    Ok(true)
}

fn deploy_scorer_step(
    context: &Context<'_, impl Runtime>,
    job: &mut Job,
    record: &Record,
    service: &mut Service,
    token: &str,
) -> Result<(), Error> {
    if service.capture == Capture::Attached {
        return Err(Error::new(409, "managed_resources_not_owned"));
    }
    let mut progressed = false;
    let mut sc = service
        .scoring
        .clone()
        .ok_or_else(|| Error::new(409, "scoring_config_missing"))?;
    if !job.scorer_ownership_verified {
        reconcile::verify_managed_customer_vcl_ownership(context, service, token)?;
        verify_scoring_resources(context, &service.service_id, &sc, token)?;
        job.scorer_ownership_verified = true;
        progressed = true;
        if context.remaining_calls() < 16 {
            return Ok(());
        }
    }
    let pkg_bytes = context
        .runtime
        .scorer_package()
        .ok_or_else(|| Error::new(500, "scorer_package_missing"))?;
    let pkg_sha = hex::encode(Sha256::digest(&pkg_bytes));
    let scorer_sid = encode(&sc.scoring_service_id);

    let versions = context.api(
        "GET",
        &format!("/service/{scorer_sid}/version"),
        token,
        None,
    )?;
    let version_list = list(&versions)?;
    let active_ver = version_list
        .iter()
        .find(|v| v["active"].as_bool() == Some(true))
        .and_then(|v| v["number"].as_u64());
    let mut skip_upload = false;
    let mut files_hash = String::new();
    if let Some(ver) = active_ver {
        if sc.deployed_package_sha == pkg_sha && !sc.deployed_package_files_hash.is_empty() {
            let (status, pkg_meta) = context.api_status(
                "GET",
                &format!("/service/{scorer_sid}/version/{ver}/package"),
                token,
                None,
            )?;
            if (200..300).contains(&status) {
                let live_hash = pkg_meta["metadata"]["files_hash"]
                    .as_str()
                    .or_else(|| pkg_meta["metadata"]["hashsum"].as_str())
                    .unwrap_or("");
                if !live_hash.is_empty() && live_hash == sc.deployed_package_files_hash {
                    skip_upload = true;
                    files_hash = live_hash.to_owned();
                }
            }
        }
    }

    if !skip_upload {
        let base_ver = version_list
            .iter()
            .filter_map(|v| v["number"].as_u64())
            .max()
            .unwrap_or(1);
        let base_is_locked = version_list
            .iter()
            .find(|v| v["number"].as_u64() == Some(base_ver))
            .is_some_and(|v| {
                v["locked"].as_bool() == Some(true) || v["active"].as_bool() == Some(true)
            });
        let deploy_ver = if base_is_locked {
            if context.remaining_calls() < 8 {
                return budget_yield(progressed);
            }
            let cloned = context.api(
                "PUT",
                &format!("/service/{scorer_sid}/version/{base_ver}/clone"),
                token,
                None,
            )?;
            progressed = true;
            cloned["number"]
                .as_u64()
                .ok_or_else(|| Error::new(502, "scorer_clone_failed"))?
        } else {
            base_ver
        };

        if !sync_scorer_resource_links(
            context,
            &scorer_sid,
            deploy_ver,
            [
                (KEYS_RESOURCE_LINK_NAME, sc.scoring_keys_store_id.as_str()),
                (
                    CONFIG_RESOURCE_LINK_NAME,
                    sc.scoring_config_store_id.as_str(),
                ),
                (
                    MATRIX_RESOURCE_LINK_NAME,
                    sc.scoring_matrix_store_id.as_str(),
                ),
            ],
            token,
            &mut progressed,
        )? {
            return budget_yield(progressed);
        }

        if context.remaining_calls() < 11 {
            return budget_yield(progressed);
        }
        let boundary = "----flp-scorer-package-boundary";
        let mut body = Vec::new();
        body.extend_from_slice(
            format!(
                "--{boundary}\r\n\
                 Content-Disposition: form-data; name=\"package\"; filename=\"session-scorer.tar.gz\"\r\n\
                 Content-Type: application/octet-stream\r\n\r\n"
            )
            .as_bytes(),
        );
        body.extend_from_slice(&pkg_bytes);
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
        context.api_raw(
            "PUT",
            &format!("/service/{scorer_sid}/version/{deploy_ver}/package"),
            token,
            &format!("multipart/form-data; boundary={boundary}"),
            &body,
        )?;
        context.api(
            "PUT",
            &format!("/service/{scorer_sid}/version/{deploy_ver}/activate"),
            token,
            None,
        )?;
        progressed = true;
        let (status, pkg_meta) = context.api_status(
            "GET",
            &format!("/service/{scorer_sid}/version/{deploy_ver}/package"),
            token,
            None,
        )?;
        if (200..300).contains(&status) {
            files_hash = pkg_meta["metadata"]["files_hash"]
                .as_str()
                .or_else(|| pkg_meta["metadata"]["hashsum"].as_str())
                .unwrap_or("")
                .to_owned();
        }
        if !files_hash.is_empty() {
            sc.deployed_package_sha = pkg_sha;
            sc.deployed_package_files_hash = files_hash;
            service.scoring = Some(sc);
            checkpoint_scoring_job_service(
                context,
                record,
                service,
                job,
                "scoring.scorer_package_activated",
                "scorer_deploy",
            )?;
            return Ok(());
        }
    }

    if context.remaining_calls() < 9 {
        return budget_yield(progressed);
    }
    // Seed initial matrix in KV if FOS has matrix_latest.json (or embedded empty default).
    let latest_key = format!("{}matrix_latest.json", scoring_fos_prefix(service));
    let matrix = fos_json_get(context, service, &latest_key)?.unwrap_or_else(|| {
        json!({
            "version": "default-empty",
            "vocab_size": 0,
            "session_count": 0,
            "transition_count": 0,
            "counts": {},
            "row_totals": {},
        })
    });
    if !sc.scoring_matrix_store_id.is_empty() {
        write_matrix_to_kv(context, &sc.scoring_matrix_store_id, &matrix, token)?;
    }

    sc.deployed_package_sha = pkg_sha;
    sc.deployed_package_files_hash = files_hash;
    service.scoring = Some(sc);
    checkpoint_scoring_job_service(
        context,
        record,
        service,
        job,
        "scoring.scorer_deploy",
        "logging_reconcile",
    )?;
    advance(job, "logging_reconcile");
    Ok(())
}

fn logging_reconcile_step(
    context: &Context<'_, impl Runtime>,
    job: &mut Job,
    record: &Record,
    service: &mut Service,
    token: &str,
) -> Result<(), Error> {
    reconcile::verify_managed_customer_vcl_ownership(context, service, token)?;
    let mut sc = service
        .scoring
        .clone()
        .ok_or_else(|| Error::new(409, "scoring_config_missing"))?;
    sc.enabled = true;
    sc.enabled_at = rfc3339_from_secs(context.runtime.now());
    sc.deployed_vcl_sha = scorer_vcl_fingerprint(&service.service_id);
    if sc.exclude_url_regex.is_empty() {
        sc.exclude_url_regex = DEFAULT_ASSET_EXT_REGEX.to_owned();
    }
    service.scoring = Some(sc);
    merge_scoring_custom_fields(&mut service.log_fields);
    service.config_version += 1;
    service.updated_at = context.runtime.now();
    checkpoint_scoring_job_service(
        context,
        record,
        service,
        job,
        "scoring.logging_reconcile",
        "discover",
    )?;
    advance(job, "discover");
    Ok(())
}

fn strip_logging_vcl_step(
    context: &Context<'_, impl Runtime>,
    job: &mut Job,
    record: &Record,
    service: &mut Service,
    token: &str,
) -> Result<(), Error> {
    if job.kind == "teardown" && !job.options.remove_snippets {
        if !job.options.remove_logging {
            service.scoring = None;
            remove_scoring_custom_fields(&mut service.log_fields);
            service.config_version += 1;
            service.updated_at = context.runtime.now();
            checkpoint_scoring_job_service(
                context,
                record,
                service,
                job,
                "scoring.local_teardown",
                "remove_front",
            )?;
            advance(job, "remove_front");
            return Ok(());
        }
        reconcile::verify_managed_customer_vcl_ownership(context, service, token)?;
        service.scoring = None;
        remove_scoring_custom_fields(&mut service.log_fields);
        service.config_version += 1;
        service.updated_at = context.runtime.now();
        checkpoint_scoring_job_service(
            context,
            record,
            service,
            job,
            "scoring.keep_snippets_teardown",
            "discover",
        )?;
        advance(job, "discover");
        return Ok(());
    }
    // Verify customer VCL ownership & absence of primary/attach coexistence BEFORE cloning customer VCL
    // or deleting any Compute scorer WASM service or stores.
    let active = reconcile::verify_managed_customer_vcl_ownership(context, service, token)?;
    let versions = context.api(
        "GET",
        &format!("/service/{}/version", service.service_id),
        token,
        None,
    )?;
    let versions = list(&versions)?;
    let tag = format!("flp-job:{}", job.id);
    if versions
        .iter()
        .any(|v| v["comment"] == tag && v["active"] == true && v["number"].as_u64() == Some(active))
    {
        job.source_version = Some(active);
        job.draft_version = Some(active);
        job.clone_intent = None;
        advance(job, "scoring_strip_activate");
        return Ok(());
    }
    if let Some(draft) = versions
        .iter()
        .filter(|v| v["comment"] == tag && v["active"] != true && v["locked"] != true)
        .filter_map(|v| v["number"].as_u64())
        .filter(|&n| n > active)
        .max()
    {
        job.source_version = Some(active);
        job.draft_version = Some(draft);
        job.clone_intent = None;
        advance(job, "scoring_strip_apply");
        return Ok(());
    }
    if let Some(intent) = job.clone_intent {
        if versions
            .iter()
            .any(|v| v["number"].as_u64().is_some_and(|n| n > intent))
        {
            return Err(Error::new(409, "uncheckpointed_clone_requires_review"));
        }
        if Some(active) != job.source_version {
            return Err(Error::new(409, "active_version_changed"));
        }
        if context.remaining_calls() < 8 {
            return Err(Error::new(503, "backend_budget"));
        }
        let cloned = context.api(
            "PUT",
            &format!("/service/{}/version/{active}/clone", service.service_id),
            token,
            None,
        )?;
        let draft = cloned["number"]
            .as_u64()
            .ok_or_else(|| Error::new(502, "logging_clone_failed"))?;
        context.api(
            "PUT",
            &format!("/service/{}/version/{draft}", service.service_id),
            token,
            Some(json!({"comment": tag})),
        )?;
        job.draft_version = Some(draft);
        job.clone_intent = None;
        advance(job, "scoring_strip_apply");
    } else {
        job.source_version = Some(active);
        job.clone_intent = Some(
            versions
                .iter()
                .filter_map(|v| v["number"].as_u64())
                .max()
                .unwrap_or(active),
        );
    }
    Ok(())
}

fn strip_logging_apply_step(
    context: &Context<'_, impl Runtime>,
    job: &mut Job,
    service: &Service,
    token: &str,
) -> Result<(), Error> {
    let mut progressed = false;
    let draft = job
        .draft_version
        .ok_or_else(|| Error::new(409, "draft_missing"))?;
    let snippets = context.api(
        "GET",
        &format!("/service/{}/version/{draft}/snippet", service.service_id),
        token,
        None,
    )?;
    let present: BTreeSet<String> = list(&snippets)?
        .iter()
        .filter_map(|v| v["name"].as_str().map(str::to_owned))
        .collect();
    for name in scoring_snippet_names() {
        if !present.contains(name) {
            continue;
        }
        if context.remaining_calls() < 8 {
            return budget_yield(progressed);
        }
        let encoded = encode(name);
        let (status, _) = context.api_status(
            "DELETE",
            &format!(
                "/service/{}/version/{draft}/snippet/{encoded}",
                service.service_id
            ),
            token,
            None,
        )?;
        if !(200..300).contains(&status) && status != 404 {
            return Err(Error::new(502, "scoring_snippet_delete_failed"));
        }
        progressed = true;
    }

    if context.remaining_calls() < 8 {
        return budget_yield(progressed);
    }
    let (status, _) = context.api_status(
        "DELETE",
        &format!(
            "/service/{}/version/{draft}/backend/{}",
            service.service_id, SCORING_BACKEND_API_NAME
        ),
        token,
        None,
    )?;
    if !(200..300).contains(&status) && status != 404 {
        return Err(Error::new(502, "scoring_backend_delete_failed"));
    }

    advance(job, "scoring_strip_activate");
    Ok(())
}

fn strip_logging_activate_step(
    context: &Context<'_, impl Runtime>,
    job: &mut Job,
    record: &Record,
    service: &mut Service,
    token: &str,
) -> Result<(), Error> {
    let draft = job
        .draft_version
        .ok_or_else(|| Error::new(409, "draft_missing"))?;
    let active = reconcile::active_version(context, &service.service_id, token)?;
    if active != draft {
        if Some(active) != job.source_version {
            return Err(Error::new(409, "active_version_changed"));
        }
        let validate = context.api(
            "GET",
            &format!("/service/{}/version/{draft}/validate", service.service_id),
            token,
            None,
        )?;
        if validate["status"] != "ok" {
            return Err(Error::new(400, "draft_validation_failed"));
        }
        reconcile::activation_generation(context, job)?;
        fence(context, job)?;
        context.api(
            "PUT",
            &format!("/service/{}/version/{draft}/activate", service.service_id),
            token,
            None,
        )?;
    }
    job.source_version = Some(draft);
    if let Some(sc) = service.scoring.as_mut() {
        sc.enabled = false;
    }
    remove_scoring_custom_fields(&mut service.log_fields);
    service.config_version += 1;
    service.updated_at = context.runtime.now();
    checkpoint_scoring_job_service(
        context,
        record,
        service,
        job,
        "scoring.strip_logging_vcl",
        "scoring_delete_resources",
    )?;
    advance(job, "scoring_delete_resources");
    Ok(())
}

fn delete_scorer_resources_step(
    context: &Context<'_, impl Runtime>,
    job: &mut Job,
    service: &Service,
    token: &str,
) -> Result<(), Error> {
    job.scorer_ownership_verified = false;
    if let Some(sc) = &service.scoring {
        if !sc.scoring_service_id.is_empty() {
            if !valid_service_id(&sc.scoring_service_id)
                || sc.scoring_service_id == service.service_id
                || own_services(context.runtime)?
                    .iter()
                    .any(|id| id == &sc.scoring_service_id)
            {
                return Err(Error::new(400, "invalid_scoring_service_id"));
            }
            let encoded_sid = encode(&sc.scoring_service_id);
            let (svc_status, svc_body) =
                context.api_status("GET", &format!("/service/{encoded_sid}"), token, None)?;
            if svc_status == 401 || svc_status == 403 {
                return Err(Error::new(403, "fastly_token_scope_insufficient"));
            }
            if svc_status != 404 {
                if !(200..300).contains(&svc_status) {
                    return Err(Error::new(502, "scorer_service_lookup_failed"));
                }
                let expected_name = scoring_service_name(&service.service_id);
                if svc_body["type"].as_str() != Some("wasm")
                    || svc_body["name"].as_str() != Some(expected_name.as_str())
                {
                    return Err(Error::new(400, "invalid_scoring_service_id"));
                }
                job.scorer_ownership_verified = true;
                let (status, versions) = context.api_status(
                    "GET",
                    &format!("/service/{encoded_sid}/version"),
                    token,
                    None,
                )?;
                if (200..300).contains(&status) {
                    for v in list(&versions).unwrap_or(&vec![]) {
                        if v["active"].as_bool() == Some(true) {
                            if let Some(num) = v["number"].as_u64() {
                                let _ = context.api_status(
                                    "PUT",
                                    &format!("/service/{encoded_sid}/version/{num}/deactivate"),
                                    token,
                                    None,
                                );
                            }
                        }
                    }
                }
                let (del_status, _) = context.api_status(
                    "DELETE",
                    &format!("/service/{encoded_sid}"),
                    token,
                    None,
                )?;
                if !(200..300).contains(&del_status) && del_status != 404 {
                    job.warnings.push(format!(
                        "Could not delete scoring service {}",
                        sc.scoring_service_id
                    ));
                }
            }
        }
    }
    advance(job, "scoring_delete_stores");
    Ok(())
}

fn delete_scorer_stores_step(
    context: &Context<'_, impl Runtime>,
    job: &mut Job,
    service: &Service,
    token: &str,
) -> Result<(), Error> {
    if let Some(sc) = &service.scoring {
        for (label, store_id, expected_name) in [
            (
                "scoring_keys",
                sc.scoring_keys_store_id.as_str(),
                keys_store_name(&sc.scoring_service_id),
            ),
            (
                "scoring_config",
                sc.scoring_config_store_id.as_str(),
                config_store_name(&sc.scoring_service_id),
            ),
        ] {
            if store_id.is_empty() {
                continue;
            }
            if sc.scoring_service_id.is_empty() {
                return Err(Error::new(400, "invalid_scoring_store_id"));
            }
            let exists = verify_single_store_ownership(
                context,
                "config",
                store_id,
                &expected_name,
                token,
                true,
            )?;
            if !exists {
                continue;
            }
            if !job.scorer_ownership_verified {
                job.warnings.push(format!(
                    "Skipped deleting unverified {label} store {store_id}"
                ));
                continue;
            }
            let encoded_store = encode(store_id);
            let (status, _) = context.api_status(
                "DELETE",
                &format!("/resources/stores/config/{encoded_store}"),
                token,
                None,
            )?;
            if !(200..300).contains(&status) && status != 404 {
                job.warnings
                    .push(format!("Could not delete {label} store {store_id}"));
            }
        }
    }
    advance(job, "scoring_delete_matrix");
    Ok(())
}

fn delete_scorer_matrix_step(
    context: &Context<'_, impl Runtime>,
    job: &mut Job,
    service: &Service,
    token: &str,
) -> Result<(), Error> {
    if let Some(sc) = &service.scoring {
        if !sc.scoring_matrix_store_id.is_empty() {
            if sc.scoring_service_id.is_empty() {
                return Err(Error::new(400, "invalid_scoring_store_id"));
            }
            let expected_name = matrix_store_name(&sc.scoring_service_id);
            let exists = verify_single_store_ownership(
                context,
                "kv",
                &sc.scoring_matrix_store_id,
                &expected_name,
                token,
                true,
            )?;
            if exists {
                if !job.scorer_ownership_verified {
                    job.warnings.push(format!(
                        "Skipped deleting unverified scoring_matrix store {}",
                        sc.scoring_matrix_store_id
                    ));
                } else {
                    let encoded_store = encode(&sc.scoring_matrix_store_id);
                    let _ = context.api_status(
                        "DELETE",
                        &format!("/resources/stores/kv/{encoded_store}/keys/{MATRIX_KEY_NAME}"),
                        token,
                        None,
                    );
                    let (status, _) = context.api_status(
                        "DELETE",
                        &format!("/resources/stores/kv/{encoded_store}"),
                        token,
                        None,
                    )?;
                    if !(200..300).contains(&status) && status != 404 {
                        job.warnings.push(format!(
                            "Could not delete scoring_matrix store {}",
                            sc.scoring_matrix_store_id
                        ));
                    }
                }
            }
        }
    }
    advance(job, "scoring_cleanup_state");
    Ok(())
}

fn cleanup_scoring_state_step(
    context: &Context<'_, impl Runtime>,
    job: &mut Job,
    record: &Record,
    service: &mut Service,
    token: &str,
) -> Result<(), Error> {
    let mut progressed = false;
    // Delete scoring_request_secret.<sid> from Secret Store (best-effort).
    if let Ok(store) = config(context.runtime, "secret_store_id") {
        let req_secret_ref = request_secret_ref(&service.service_id);
        let encoded_ref = encode(&req_secret_ref);
        if let Ok((200..300, _)) = context.api_status(
            "GET",
            &format!("/resources/stores/secret/{store}/secrets/{encoded_ref}"),
            token,
            None,
        ) {
            if context.remaining_calls() < 8 {
                return budget_yield(progressed);
            }
            let _ = context.api_status(
                "DELETE",
                &format!("/resources/stores/secret/{store}/secrets/{encoded_ref}"),
                token,
                None,
            );
            context.invalidate_secret(&req_secret_ref);
            progressed = true;
        }
    }

    // Delete processed/scoring/* in FOS on teardown, or on disable when delete_matrices is true.
    if job.kind == "scoring_teardown"
        || job.kind == "scoring_disable" && job.delete_matrices
        || job.kind == "teardown"
    {
        if context.remaining_calls() < 11 {
            return budget_yield(progressed);
        }
        if let Ok(creds) = fos::credentials(context, service) {
            let prefix = scoring_fos_prefix(service);
            let latest = format!("{prefix}matrix_latest.json");
            let index_key = format!("{prefix}matrices/index.json");
            let mut need_index_delete = false;
            let mut s3_write_failed = false;
            match fos_json_get_with_creds(context, service, &creds, &index_key) {
                Ok(Some(idx)) => {
                    let mut remaining = Vec::new();
                    let mut total_versions = 0usize;
                    if let Some(versions) = idx["versions"].as_array() {
                        total_versions = versions.len();
                        for entry in versions {
                            if context.remaining_calls() <= 7 {
                                remaining.push(entry.clone());
                                continue;
                            }
                            if let Some(ver) = entry["version"]
                                .as_str()
                                .filter(|v| valid_matrix_version(v))
                            {
                                if fos::delete_object(
                                    context,
                                    service,
                                    &creds,
                                    &format!("{prefix}matrices/{ver}.json"),
                                )
                                .is_ok()
                                {
                                    progressed = true;
                                }
                            }
                        }
                    }
                    if !remaining.is_empty() {
                        if remaining.len() < total_versions
                            && fos_json_put_with_creds(
                                context,
                                service,
                                &creds,
                                &index_key,
                                &json!({"versions": remaining}),
                            )
                            .is_ok()
                        {
                            return Ok(());
                        }
                        if remaining.len() == total_versions && !progressed {
                            return Err(Error::new(503, "backend_budget"));
                        }
                        s3_write_failed = true;
                        need_index_delete = true;
                    } else {
                        let index_deleted =
                            fos::delete_object(context, service, &creds, &index_key).is_ok();
                        if index_deleted {
                            progressed = true;
                        }
                        if index_deleted && context.remaining_calls() < 8 {
                            return Ok(());
                        }
                        if !index_deleted {
                            s3_write_failed = true;
                        }
                    }
                }
                Ok(None) => {}
                Err(_) => {
                    need_index_delete = true;
                }
            }
            if !s3_write_failed && context.remaining_calls() < if need_index_delete { 9 } else { 8 }
            {
                return budget_yield(progressed);
            }
            if need_index_delete
                && context.remaining_calls() >= 7
                && fos::delete_object(context, service, &creds, &index_key).is_ok()
            {
                progressed = true;
            }
            if context.remaining_calls() >= 7
                && fos::delete_object(context, service, &creds, &latest).is_ok()
            {
                progressed = true;
            }
            if (job.kind == "scoring_teardown" || job.kind == "teardown")
                && context.remaining_calls() >= 7
                && fos::delete_object(context, service, &creds, &format!("{prefix}labels.json"))
                    .is_ok()
            {
                progressed = true;
            }
        }
    }

    if context.remaining_calls() < 6 {
        return budget_yield(progressed);
    }
    service.scoring = None;
    remove_scoring_custom_fields(&mut service.log_fields);
    service.config_version += 1;
    service.updated_at = context.runtime.now();
    let next_step = if job.kind == "teardown" {
        "discover"
    } else {
        "finalize"
    };
    checkpoint_scoring_job_service(
        context,
        record,
        service,
        job,
        "scoring.cleanup_state",
        next_step,
    )?;
    advance(job, next_step);
    Ok(())
}

pub fn run_step(
    context: &Context<'_, impl Runtime>,
    job: &mut Job,
    record: &Record,
    service: &mut Service,
    token: &str,
) -> Result<bool, Error> {
    match job.step.as_str() {
        "scorer_service" => {
            ensure_scorer_service_step(context, job, record, service, token)?;
            Ok(true)
        }
        "scorer_stores" => {
            ensure_scorer_stores_step(context, job, record, service, token)?;
            Ok(true)
        }
        "scorer_store_keys" => {
            ensure_scorer_store_keys_step(context, job, service, token)?;
            Ok(true)
        }
        "scorer_store_config" => {
            ensure_scorer_store_config_step(context, job, service, token)?;
            Ok(true)
        }
        "scorer_deploy" => {
            deploy_scorer_step(context, job, record, service, token)?;
            Ok(true)
        }
        "logging_reconcile" => {
            logging_reconcile_step(context, job, record, service, token)?;
            Ok(true)
        }
        "scoring_strip_logging_vcl" => {
            strip_logging_vcl_step(context, job, record, service, token)?;
            Ok(true)
        }
        "scoring_strip_apply" => {
            strip_logging_apply_step(context, job, service, token)?;
            Ok(true)
        }
        "scoring_strip_activate" => {
            strip_logging_activate_step(context, job, record, service, token)?;
            Ok(true)
        }
        "scoring_delete_resources" => {
            delete_scorer_resources_step(context, job, service, token)?;
            Ok(true)
        }
        "scoring_delete_stores" => {
            delete_scorer_stores_step(context, job, service, token)?;
            Ok(true)
        }
        "scoring_delete_matrix" => {
            delete_scorer_matrix_step(context, job, service, token)?;
            Ok(true)
        }
        "scoring_cleanup_state" => {
            cleanup_scoring_state_step(context, job, record, service, token)?;
            Ok(true)
        }
        "scoring_rollback" => {
            rollback_failed_enable_step(context, job, record, service)?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

pub fn needs_failed_enable_rollback(job: &Job, failed_step: &str) -> bool {
    job.kind == "scoring_enable"
        && failed_step != "scoring_rollback"
        && !job.completed_steps.iter().any(|s| s == "activate")
        && (job.completed_steps.iter().any(|s| s == "logging_reconcile")
            || matches!(
                failed_step,
                "discover" | "clone" | "apply" | "validate" | "activate"
            ))
}

pub fn needs_failed_strip_rewind(job: &Job, failed_step: &str, failure_code: &str) -> bool {
    let draft_already_activated = failed_step == "scoring_strip_activate"
        && failure_code != "active_version_changed"
        && job.draft_version.is_some()
        && job.source_version == job.draft_version;
    matches!(
        job.kind.as_str(),
        "scoring_disable" | "scoring_teardown" | "teardown"
    ) && matches!(
        failed_step,
        "scoring_strip_logging_vcl" | "scoring_strip_apply" | "scoring_strip_activate"
    ) && !draft_already_activated
        && !job
            .completed_steps
            .iter()
            .any(|s| s == "scoring_strip_activate")
}

pub fn rewind_failed_strip_vcl(job: &mut Job) {
    job.step = "scoring_strip_logging_vcl".into();
    job.completed_steps.retain(|s| {
        !matches!(
            s.as_str(),
            "scoring_strip_logging_vcl" | "scoring_strip_apply" | "scoring_strip_activate"
        )
    });
    job.source_version = None;
    job.draft_version = None;
    job.clone_intent = None;
}

fn has_scoring_custom_fields(log_fields: &Value) -> bool {
    log_fields["custom_fields"].as_array().is_some_and(|arr| {
        arr.iter().any(|cf| {
            cf.get("name")
                .and_then(Value::as_str)
                .is_some_and(|n| SCORING_FIELD_NAMES.contains(&n))
        })
    })
}

fn rollback_failed_enable_step(
    context: &Context<'_, impl Runtime>,
    job: &mut Job,
    record: &Record,
    service: &mut Service,
) -> Result<(), Error> {
    if service.scoring.as_ref().is_some_and(|sc| sc.enabled)
        || has_scoring_custom_fields(&service.log_fields)
    {
        if context.remaining_calls() < 6 {
            return Err(Error::new(503, "backend_budget"));
        }
        if let Some(sc) = service.scoring.as_mut() {
            sc.enabled = false;
        }
        remove_scoring_custom_fields(&mut service.log_fields);
        checkpoint_scoring_rollback_service(
            context,
            record,
            service,
            job,
            "scoring.enable_rolled_back",
            "logging_reconcile",
        )?;
    }
    rewind_scoring_enable_to_logging_reconcile(job);
    let code = job
        .pending_failure
        .take()
        .unwrap_or_else(|| "scoring_rollback_failed".into());
    job.status = "failed".into();
    job.error = Some(code);
    job.retry = None;
    Ok(())
}

pub fn rollback_failed_enable(
    context: &Context<'_, impl Runtime>,
    service: &mut Service,
    record: &Record,
    job: &mut Job,
) -> bool {
    let fresh_rec;
    let rec: &Record = if record.generation != job.expected_generation {
        if context.remaining_calls() < 7 {
            return false;
        }
        if let Ok((loaded_rec, loaded_svc)) = load_service_from(context, &job.service_id, true) {
            *service = loaded_svc;
            fresh_rec = loaded_rec;
            &fresh_rec
        } else {
            return false;
        }
    } else {
        if context.remaining_calls() < 6 {
            return false;
        }
        record
    };
    if service.pending_job_id.as_deref() != Some(job.id.as_str()) {
        return false;
    }
    let passed_logging_reconcile = job.kind == "scoring_enable"
        && (job.completed_steps.iter().any(|s| s == "logging_reconcile")
            || matches!(
                job.step.as_str(),
                "discover"
                    | "clone"
                    | "apply"
                    | "validate"
                    | "activate"
                    | "sync_rum"
                    | "finalize"
                    | "scoring_rollback"
            ));
    let next_step = if passed_logging_reconcile {
        "logging_reconcile".to_owned()
    } else {
        job.step.clone()
    };
    let needs_state_revert = service.scoring.as_ref().is_some_and(|sc| sc.enabled)
        || has_scoring_custom_fields(&service.log_fields);
    if needs_state_revert {
        if let Some(sc) = service.scoring.as_mut() {
            sc.enabled = false;
        }
        remove_scoring_custom_fields(&mut service.log_fields);
        if checkpoint_scoring_rollback_service(
            context,
            rec,
            service,
            job,
            "scoring.enable_rolled_back",
            &next_step,
        )
        .is_err()
        {
            return false;
        }
        if passed_logging_reconcile {
            rewind_scoring_enable_to_logging_reconcile(job);
        }
        true
    } else {
        if passed_logging_reconcile {
            rewind_scoring_enable_to_logging_reconcile(job);
        }
        true
    }
}

fn rewind_scoring_enable_to_logging_reconcile(job: &mut Job) {
    job.step = "logging_reconcile".into();
    job.completed_steps.retain(|s| {
        !matches!(
            s.as_str(),
            "logging_reconcile"
                | "discover"
                | "clone"
                | "apply"
                | "validate"
                | "activate"
                | "sync_rum"
                | "finalize"
        )
    });
    job.source_version = None;
    job.draft_version = None;
    job.clone_intent = None;
    job.changes.clear();
    job.change_cursor = 0;
}

pub fn on_job_finalized(context: &Context<'_, impl Runtime>, job: &Job, service: &Service) {
    let action = match job.kind.as_str() {
        "scoring_enable" => "scoring_enabled",
        "scoring_disable" => "scoring_disabled",
        "scoring_teardown" => "scoring_torn_down",
        _ => return,
    };
    let details = service
        .scoring
        .as_ref()
        .map(|sc| json!({"scoring_service_id": sc.scoring_service_id, "matrix_version": sc.matrix_version}))
        .unwrap_or_else(|| json!({"warnings": job.warnings}));
    append_scoring_audit(context, &service.service_id, &job.actor, action, details);
}

fn status_payload(
    context: &Context<'_, impl Runtime>,
    service: &Service,
    generation: &str,
) -> Value {
    let sid = &service.service_id;
    let current_vcl_sha = scorer_vcl_fingerprint(sid);
    let embedded_pkg_sha = context
        .runtime
        .scorer_package()
        .map(|b| hex::encode(Sha256::digest(&b)));

    let (enabled, scoring_obj, drift, drift_detail, preflight_ready) = match &service.scoring {
        Some(sc) => {
            let wasm_drift = embedded_pkg_sha.as_ref().is_some_and(|sha| {
                !sc.deployed_package_sha.is_empty() && sha != &sc.deployed_package_sha
            });
            let vcl_drift =
                !sc.deployed_vcl_sha.is_empty() && sc.deployed_vcl_sha != current_vcl_sha;
            let detail = match (wasm_drift, vcl_drift) {
                (true, true) => Some("wasm+vcl"),
                (true, false) => Some("wasm"),
                (false, true) => Some("vcl"),
                (false, false) => None,
            };
            let sc_val = serde_json::to_value(sc).unwrap_or(Value::Null);
            (
                sc.enabled,
                Some(sc_val),
                wasm_drift || vcl_drift,
                detail,
                None,
            )
        }
        None => {
            let ready = context
                .secret(&service.fastly_token_ref)
                .ok()
                .and_then(|tok| service_has_scoring_pragmas(context, sid, &tok));
            (false, None, false, None, ready)
        }
    };

    json!({
        "service_id": sid,
        "generation": generation,
        "enabled": enabled,
        "supported": true,
        "drift_known": true,
        "scorer_drift": drift,
        "drift_detail": drift_detail,
        "preflight_pragmas_ready": preflight_ready,
        "scoring": scoring_obj,
        "capabilities": {
            "historical_analytics": true,
            "management": true,
            "labels": true,
            "evaluation": true,
            "thresholds": true,
            "matrix_versions": true,
            "audit": true,
        },
    })
}

fn require_scoring_enabled(service: &Service) -> Result<&ScoringConfig, Error> {
    service
        .scoring
        .as_ref()
        .filter(|sc| sc.enabled)
        .ok_or_else(|| Error::new(400, "scoring_not_enabled"))
}

fn require_scoring_mutable(
    context: &Context<'_, impl Runtime>,
    sid: &str,
    service: &Service,
) -> Result<(), Error> {
    if service.status == "torn_down" {
        return Err(Error::new(409, "service_torn_down"));
    }
    reject_blocking_job(context, service)?;
    check_unlocked(context, sid)?;
    if service.data_reset.is_some() {
        return Err(Error::new(409, "data_reset_in_progress"));
    }
    Ok(())
}

fn require_confirm(query: &str) -> Result<(), Error> {
    let confirmed = url::form_urlencoded::parse(query.as_bytes())
        .any(|(k, v)| k == "confirm" && matches!(v.as_ref(), "true" | "1" | "yes"));
    if !confirmed {
        return Err(Error::new(400, "confirm_required"));
    }
    Ok(())
}

fn build_l2_enforce_block(
    scoring_enabled_at_raw: &str,
    l2_enforce_raw: &str,
    l2_enabled_at_raw: &str,
    now_epoch: u64,
) -> Value {
    let enabled = l2_enforce_raw.trim() == "1";
    let scoring_enabled_at: Option<u64> = scoring_enabled_at_raw.trim().parse().ok();
    let l2_enabled_at: Option<u64> = l2_enabled_at_raw.trim().parse().ok();
    let deployment_age_days =
        scoring_enabled_at.map(|ts| (now_epoch.saturating_sub(ts) as f64) / 86_400.0);
    let ready = deployment_age_days.is_some_and(|d| d >= L2_READINESS_DAYS);
    let days_since_optin = l2_enabled_at.map(|ts| (now_epoch.saturating_sub(ts) as f64) / 86_400.0);
    let ramp_progress = days_since_optin
        .map(|d| (d / L2_RAMP_DAYS).clamp(0.0, 1.0))
        .unwrap_or(0.0);
    let fully_ramped = days_since_optin.is_some_and(|d| d >= L2_RAMP_DAYS);
    let warmup_days_remaining = days_since_optin.map(|d| (L2_RAMP_DAYS - d).max(0.0));
    json!({
        "available": true,
        "enabled": enabled,
        "l2_enabled_at": l2_enabled_at,
        "days_since_optin": days_since_optin,
        "ramp_progress": ramp_progress,
        "fully_ramped": fully_ramped,
        "warmup_days_remaining": warmup_days_remaining,
        "scoring_enabled_at": scoring_enabled_at,
        "deployment_age_days": deployment_age_days,
        "ready": ready,
        "ramp_days": L2_RAMP_DAYS,
        "readiness_days": L2_READINESS_DAYS,
    })
}

pub fn route(
    context: &Context<'_, impl Runtime>,
    actor: &str,
    sid: &str,
    rest: &[&str],
    query: &str,
    request: &Request,
) -> Result<Value, Error> {
    let method = request.method.as_str();
    match (method, rest) {
        ("GET", ["status"]) => {
            let (record, service) = load_service(context, sid)?;
            Ok(status_payload(context, &service, &record.generation))
        }
        ("GET", ["config"]) => {
            let (record, service) = load_service(context, sid)?;
            let status = status_payload(context, &service, &record.generation);
            let sc = service.scoring.as_ref();
            Ok(json!({
                "status": status,
                "capabilities": status["capabilities"],
                "threshold": {
                    "threshold": sc.and_then(|s| s.threshold),
                },
                "exclude_regex": {
                    "exclude_url_regex": sc.map(|s| s.exclude_url_regex.as_str()).unwrap_or(""),
                    "effective_regex": resolve_exclude_url_regex(sc.map(|s| s.exclude_url_regex.as_str())),
                },
                "enforce_status_code": {
                    "status_code": resolve_enforce_status_code(sc.and_then(|s| s.enforce_status_code)),
                },
            }))
        }
        ("POST", ["enable"]) => {
            let input: Value = if request.body.is_empty() {
                json!({})
            } else {
                body(request)?
            };
            let (record, mut service) = load_service(context, sid)?;
            if service.capture == Capture::Attached {
                return Err(Error::new(409, "scoring_not_supported_in_attach_mode"));
            }
            if service.status == "torn_down" {
                return Err(Error::new(409, "service_torn_down"));
            }
            require_scoring_mutable(context, sid, &service)?;
            if super::audit_outbox_len(&record) >= super::AUDIT_OUTBOX_LIMIT {
                return Err(Error::new(409, "audit_archive_required"));
            }
            let token = resolve_brokered_token(context, &service, input["token"].as_str())?;
            let active =
                reconcile::verify_managed_customer_vcl_ownership(context, &service, &token)?;
            if service_has_scoring_pragmas_at_version(context, sid, active, &token) == Some(false) {
                return Err(Error::new(409, "missing_scoring_pragmas"));
            }
            let has_persisted_domain = service
                .scoring
                .as_ref()
                .is_some_and(|sc| !sc.scoring_domain.trim().is_empty());
            if !has_persisted_domain {
                let mut sc = service.scoring.clone().unwrap_or_default();
                sc.scoring_domain = opaque_scoring_domain(context.runtime)?;
                service.scoring = Some(sc);
            }
            let mut job = new_job(context, actor, &service, "scoring_enable", false, true)?;
            job.step = "scorer_service".into();
            start_job(context, &record, &mut service, job)
        }
        ("POST", ["disable"]) => {
            let input: Value = if request.body.is_empty() {
                json!({})
            } else {
                body(request)?
            };
            let (record, mut service) = load_service(context, sid)?;
            if service.capture == Capture::Attached {
                return Err(Error::new(409, "scoring_not_supported_in_attach_mode"));
            }
            require_scoring_mutable(context, sid, &service)?;
            require_scoring_enabled(&service)?;
            if super::audit_outbox_len(&record) >= super::AUDIT_OUTBOX_LIMIT {
                return Err(Error::new(409, "audit_archive_required"));
            }
            let _ = resolve_brokered_token(context, &service, input["token"].as_str())?;
            let mut job = new_job(context, actor, &service, "scoring_disable", false, true)?;
            job.step = "scoring_strip_logging_vcl".into();
            job.delete_matrices = input["delete_matrices"].as_bool().unwrap_or(false);
            start_job(context, &record, &mut service, job)
        }
        ("POST", ["teardown"]) => {
            let input: Value = body(request)?;
            if input["confirm_service_id"] != sid {
                return Err(Error::new(400, "typed_service_id_required"));
            }
            let (record, mut service) = load_service(context, sid)?;
            if service.capture == Capture::Attached {
                return Err(Error::new(409, "scoring_not_supported_in_attach_mode"));
            }
            check_generation(&record, &input)?;
            if service.scoring.is_none() {
                return Err(Error::new(400, "scoring_not_provisioned"));
            }
            require_scoring_mutable(context, sid, &service)?;
            if super::audit_outbox_len(&record) >= super::AUDIT_OUTBOX_LIMIT {
                return Err(Error::new(409, "audit_archive_required"));
            }
            let _ = resolve_brokered_token(context, &service, input["token"].as_str())?;
            let mut job = new_job(context, actor, &service, "scoring_teardown", false, true)?;
            job.step = "scoring_strip_logging_vcl".into();
            job.delete_matrices = true;
            start_job(context, &record, &mut service, job)
        }
        ("GET", ["threshold"]) => {
            let (_, service) = load_service(context, sid)?;
            let threshold = service.scoring.as_ref().and_then(|s| s.threshold);
            Ok(json!({"threshold": threshold}))
        }
        ("PUT", ["threshold"]) => {
            let input: Value = body(request)?;
            let threshold = input["threshold"]
                .as_u64()
                .filter(|&v| v <= 100)
                .map(|v| v as u8)
                .ok_or_else(|| Error::new(400, "invalid_threshold"))?;
            let (record, mut service) = load_service(context, sid)?;
            require_scoring_mutable(context, sid, &service)?;
            let mut sc = require_scoring_enabled(&service)?.clone();
            if super::audit_outbox_len(&record) >= super::AUDIT_OUTBOX_LIMIT {
                return Err(Error::new(409, "audit_archive_required"));
            }
            sc.threshold = Some(threshold);
            service.scoring = Some(sc);
            service.updated_at = context.runtime.now();
            let generation =
                save_scoring_service(context, &record, &service, actor, "scoring.threshold")?;
            append_scoring_audit(
                context,
                sid,
                actor,
                "threshold_updated",
                json!({"threshold": threshold}),
            );
            Ok(json!({"ok": true, "threshold": threshold, "generation": generation}))
        }
        ("GET", ["enforce-threshold"]) => {
            let (_, service) = load_service(context, sid)?;
            let sc = require_scoring_enabled(&service)?;
            let token = resolve_brokered_token(context, &service, None)?;
            verify_scoring_resources(context, &service.service_id, sc, &token)?;
            let raw = config_store_get_item(
                context,
                &sc.scoring_config_store_id,
                ENFORCE_THRESHOLD_KEY,
                &token,
            )?;
            let threshold: Option<u8> = raw.trim().parse().ok();
            Ok(json!({"enforce_threshold": threshold}))
        }
        ("PUT", ["enforce-threshold"]) => {
            require_confirm(query)?;
            let input: Value = body(request)?;
            let val = match &input["enforce_threshold"] {
                Value::Null => String::new(),
                v => {
                    let n = v
                        .as_u64()
                        .filter(|&n| n <= 100)
                        .ok_or_else(|| Error::new(400, "invalid_threshold"))?;
                    n.to_string()
                }
            };
            let (_, service) = load_service(context, sid)?;
            require_scoring_mutable(context, sid, &service)?;
            let sc = require_scoring_enabled(&service)?;
            let token = resolve_brokered_token(context, &service, None)?;
            reconcile::verify_managed_customer_vcl_ownership(context, &service, &token)?;
            verify_scoring_resources(context, &service.service_id, sc, &token)?;
            config_store_upsert_item(
                context,
                &sc.scoring_config_store_id,
                ENFORCE_THRESHOLD_KEY,
                &val,
                &token,
            )?;
            append_scoring_audit(
                context,
                sid,
                actor,
                "enforce_threshold_updated",
                json!({"enforce_threshold": input["enforce_threshold"]}),
            );
            Ok(json!({"ok": true, "enforce_threshold": input["enforce_threshold"]}))
        }
        ("GET", ["l2-enforce"]) => {
            let (_, service) = load_service(context, sid)?;
            let sc = require_scoring_enabled(&service)?;
            let token = resolve_brokered_token(context, &service, None)?;
            verify_scoring_resources(context, &service.service_id, sc, &token)?;
            let enabled_at = config_store_get_item(
                context,
                &sc.scoring_config_store_id,
                SCORING_ENABLED_AT_KEY,
                &token,
            )?;
            let l2_enabled = config_store_get_item(
                context,
                &sc.scoring_config_store_id,
                L2_ENFORCE_ENABLED_KEY,
                &token,
            )?;
            let l2_at = config_store_get_item(
                context,
                &sc.scoring_config_store_id,
                L2_ENABLED_AT_KEY,
                &token,
            )?;
            Ok(build_l2_enforce_block(
                &enabled_at,
                &l2_enabled,
                &l2_at,
                context.runtime.now(),
            ))
        }
        ("PUT", ["l2-enforce"]) => {
            require_confirm(query)?;
            let input: Value = body(request)?;
            let enabled = input["enabled"]
                .as_bool()
                .ok_or_else(|| Error::new(400, "invalid_input"))?;
            let (_, service) = load_service(context, sid)?;
            require_scoring_mutable(context, sid, &service)?;
            let sc = require_scoring_enabled(&service)?;
            let token = resolve_brokered_token(context, &service, None)?;
            reconcile::verify_managed_customer_vcl_ownership(context, &service, &token)?;
            verify_scoring_resources(context, &service.service_id, sc, &token)?;
            let now_s = context.runtime.now().to_string();
            config_store_upsert_item(
                context,
                &sc.scoring_config_store_id,
                L2_ENFORCE_ENABLED_KEY,
                if enabled { "1" } else { "0" },
                &token,
            )?;
            if enabled {
                let existing = config_store_get_item(
                    context,
                    &sc.scoring_config_store_id,
                    L2_ENABLED_AT_KEY,
                    &token,
                )?;
                if existing.is_empty() {
                    config_store_upsert_item(
                        context,
                        &sc.scoring_config_store_id,
                        L2_ENABLED_AT_KEY,
                        &now_s,
                        &token,
                    )?;
                }
            }
            append_scoring_audit(
                context,
                sid,
                actor,
                if enabled {
                    "l2_enforce_enabled"
                } else {
                    "l2_enforce_disabled"
                },
                json!({"enabled": enabled}),
            );
            let enabled_at = config_store_get_item(
                context,
                &sc.scoring_config_store_id,
                SCORING_ENABLED_AT_KEY,
                &token,
            )?;
            let l2_at = config_store_get_item(
                context,
                &sc.scoring_config_store_id,
                L2_ENABLED_AT_KEY,
                &token,
            )?;
            Ok(build_l2_enforce_block(
                &enabled_at,
                if enabled { "1" } else { "0" },
                &l2_at,
                context.runtime.now(),
            ))
        }
        ("GET", ["exclude-regex"]) => {
            let (_, service) = load_service(context, sid)?;
            let sc = service.scoring.as_ref();
            let raw = sc.map(|s| s.exclude_url_regex.as_str()).unwrap_or("");
            Ok(json!({
                "exclude_url_regex": raw,
                "effective_regex": resolve_exclude_url_regex(Some(raw)),
                "is_default": raw.trim().is_empty() || raw.trim() == DEFAULT_ASSET_EXT_REGEX,
            }))
        }
        ("POST", ["exclude-regex", "validate"]) => {
            let input: Value = body(request)?;
            let raw = input["exclude_url_regex"].as_str().unwrap_or("");
            match validate_exclude_url_regex(raw) {
                Ok(cleaned) => Ok(json!({
                    "valid": true,
                    "cleaned": cleaned,
                    "effective_regex": resolve_exclude_url_regex(Some(&cleaned)),
                })),
                Err(err) => Ok(json!({
                    "valid": false,
                    "error": err,
                })),
            }
        }
        ("PUT", ["exclude-regex"]) => {
            require_confirm(query)?;
            let input: Value = body(request)?;
            let raw = input["exclude_url_regex"].as_str().unwrap_or("");
            let cleaned = validate_exclude_url_regex(raw)
                .map_err(|_| Error::new(400, "invalid_exclude_regex"))?;
            let (record, mut service) = load_service(context, sid)?;
            require_scoring_mutable(context, sid, &service)?;
            let mut sc = require_scoring_enabled(&service)?.clone();
            if super::audit_outbox_len(&record) >= super::AUDIT_OUTBOX_LIMIT {
                return Err(Error::new(409, "audit_archive_required"));
            }
            let token = resolve_brokered_token(context, &service, None)?;
            context.confirm_with_token(&token);
            let snippet_body = recv_snippet(Some(&cleaned))?;
            update_single_snippet_and_activate(
                context,
                &service,
                &token,
                SCORING_RECV_NAME,
                "recv",
                SCORING_SNIPPET_PRIORITY,
                &snippet_body,
            )?;
            sc.exclude_url_regex = cleaned.clone();
            service.scoring = Some(sc);
            service.updated_at = context.runtime.now();
            let generation =
                save_scoring_service(context, &record, &service, actor, "scoring.exclude_regex")?;
            append_scoring_audit(
                context,
                sid,
                actor,
                "exclude_regex_updated",
                json!({"exclude_url_regex": cleaned}),
            );
            Ok(json!({
                "ok": true,
                "exclude_url_regex": cleaned,
                "effective_regex": resolve_exclude_url_regex(Some(&cleaned)),
                "generation": generation,
            }))
        }
        ("GET", ["enforce-status-code"]) => {
            let (_, service) = load_service(context, sid)?;
            let code = resolve_enforce_status_code(
                service.scoring.as_ref().and_then(|s| s.enforce_status_code),
            );
            Ok(json!({"status_code": code}))
        }
        ("PUT", ["enforce-status-code"]) => {
            require_confirm(query)?;
            let input: Value = body(request)?;
            let code = input["status_code"]
                .as_u64()
                .and_then(|v| u16::try_from(v).ok())
                .filter(|&c| (400..=599).contains(&c))
                .ok_or_else(|| Error::new(400, "invalid_status_code"))?;
            let (record, mut service) = load_service(context, sid)?;
            require_scoring_mutable(context, sid, &service)?;
            let mut sc = require_scoring_enabled(&service)?.clone();
            if super::audit_outbox_len(&record) >= super::AUDIT_OUTBOX_LIMIT {
                return Err(Error::new(409, "audit_archive_required"));
            }
            let token = resolve_brokered_token(context, &service, None)?;
            context.confirm_with_token(&token);
            let snippet_body = enforce_snippet(Some(code));
            update_single_snippet_and_activate(
                context,
                &service,
                &token,
                SCORING_ENFORCE_NAME,
                "recv",
                SCORING_SNIPPET_PRIORITY,
                &snippet_body,
            )?;
            sc.enforce_status_code = Some(code);
            service.scoring = Some(sc);
            service.updated_at = context.runtime.now();
            let generation = save_scoring_service(
                context,
                &record,
                &service,
                actor,
                "scoring.enforce_status_code",
            )?;
            append_scoring_audit(
                context,
                sid,
                actor,
                "enforce_status_code_updated",
                json!({"status_code": code}),
            );
            Ok(json!({"ok": true, "status_code": code, "generation": generation}))
        }
        ("POST", ["rotate-key"]) => {
            require_confirm(query)?;
            let (_, service) = load_service(context, sid)?;
            require_scoring_mutable(context, sid, &service)?;
            let sc = require_scoring_enabled(&service)?;
            let token = resolve_brokered_token(context, &service, None)?;
            reconcile::verify_managed_customer_vcl_ownership(context, &service, &token)?;
            verify_scoring_resources(context, &service.service_id, sc, &token)?;
            let current =
                config_store_get_item(context, &sc.scoring_keys_store_id, CURRENT_KEY_HEX, &token)?;
            let next_key = hex_random_32(context.runtime)?;
            if !current.is_empty() {
                config_store_upsert_item(
                    context,
                    &sc.scoring_keys_store_id,
                    PREVIOUS_KEY_HEX,
                    &current,
                    &token,
                )?;
            }
            config_store_upsert_item(
                context,
                &sc.scoring_keys_store_id,
                CURRENT_KEY_HEX,
                &next_key,
                &token,
            )?;
            append_scoring_audit(context, sid, actor, "key_rotated", json!({}));
            Ok(json!({"ok": true, "message": "AES-256 cookie key rotated."}))
        }
        ("POST", ["retrain"]) => {
            if request.body.len() > 4 * 1024 * 1024 {
                return Err(Error::new(413, "body_too_large"));
            }
            let input: Value = serde_json::from_slice(&request.body)
                .map_err(|_| Error::new(400, "invalid_input"))?;
            let matrix = &input["matrix"];
            if !matrix.is_object() {
                return Err(Error::new(400, "invalid_matrix"));
            }
            let version = matrix["version"]
                .as_str()
                .filter(|v| valid_matrix_version(v))
                .ok_or_else(|| Error::new(400, "invalid_matrix_version"))?
                .to_owned();
            let (record, mut service) = load_service(context, sid)?;
            require_scoring_mutable(context, sid, &service)?;
            let mut sc = require_scoring_enabled(&service)?.clone();
            if super::audit_outbox_len(&record) >= super::AUDIT_OUTBOX_LIMIT {
                return Err(Error::new(409, "audit_archive_required"));
            }
            let token = resolve_brokered_token(context, &service, None)?;
            context.confirm_with_token(&token);
            reconcile::verify_managed_customer_vcl_ownership(context, &service, &token)?;
            verify_scoring_resources(context, &service.service_id, &sc, &token)?;

            // Validate binary FSM1 encoding before writing anything.
            write_matrix_to_kv(context, &sc.scoring_matrix_store_id, matrix, &token)?;

            let creds = fos::credentials(context, &service)?;
            let prefix = scoring_fos_prefix(&service);
            let ver_key = format!("{prefix}matrices/{version}.json");
            let latest_key = format!("{prefix}matrix_latest.json");
            let index_key = format!("{prefix}matrices/index.json");

            fos_json_put_with_creds(context, &service, &creds, &ver_key, matrix)?;
            fos_json_put_with_creds(context, &service, &creds, &latest_key, matrix)?;

            let mut versions: Vec<Value> =
                fos_json_get_with_creds(context, &service, &creds, &index_key)?
                    .and_then(|v| v["versions"].as_array().cloned())
                    .unwrap_or_default();
            let now_iso = rfc3339_from_secs(context.runtime.now());
            versions.retain(|v| v["version"].as_str() != Some(&version));
            versions.insert(
                0,
                json!({
                    "version": version,
                    "created_at": now_iso,
                    "vocab_size": matrix["vocab_size"],
                    "session_count": matrix["session_count"],
                    "transition_count": matrix["transition_count"],
                    "stats": input["stats"],
                    "auc_against_labels": input["auc_against_labels"],
                }),
            );
            if versions.len() > MAX_MATRIX_HISTORY {
                for old in versions.drain(MAX_MATRIX_HISTORY..) {
                    if context.remaining_calls() <= 7 {
                        break;
                    }
                    if let Some(old_ver) =
                        old["version"].as_str().filter(|v| valid_matrix_version(v))
                    {
                        let _ = fos::delete_object(
                            context,
                            &service,
                            &creds,
                            &format!("{prefix}matrices/{old_ver}.json"),
                        );
                    }
                }
            }
            fos_json_put_with_creds(
                context,
                &service,
                &creds,
                &index_key,
                &json!({"versions": versions}),
            )?;

            sc.matrix_version = Some(version.clone());
            sc.last_retrain_at = Some(now_iso.clone());
            service.scoring = Some(sc);
            service.updated_at = context.runtime.now();
            let generation =
                save_scoring_service(context, &record, &service, actor, "scoring.retrain")?;
            append_scoring_audit(
                context,
                sid,
                actor,
                "matrix_retrained",
                json!({"version": version, "vocab_size": matrix["vocab_size"]}),
            );
            Ok(json!({
                "ok": true,
                "version": version,
                "retrained_at": now_iso,
                "generation": generation,
                "kv_pushed": true,
            }))
        }
        ("GET", ["matrix", "versions"]) => {
            let (_, service) = load_service(context, sid)?;
            let prefix = scoring_fos_prefix(&service);
            let index_key = format!("{prefix}matrices/index.json");
            let versions = fos_json_get(context, &service, &index_key)?
                .and_then(|v| v["versions"].as_array().cloned())
                .unwrap_or_default();
            let active_version = service
                .scoring
                .as_ref()
                .and_then(|s| s.matrix_version.clone());
            Ok(json!({"versions": versions, "active_version": active_version}))
        }
        ("GET", ["matrix", "active"]) => {
            let (_, service) = load_service(context, sid)?;
            let prefix = scoring_fos_prefix(&service);
            let latest_key = format!("{prefix}matrix_latest.json");
            let matrix = fos_json_get(context, &service, &latest_key)?;
            Ok(json!({"matrix": matrix}))
        }
        ("POST", ["matrix", "versions", "restore"]) => {
            require_confirm(query)?;
            let input: Value = body(request)?;
            let version = input["version"]
                .as_str()
                .filter(|v| valid_matrix_version(v))
                .ok_or_else(|| Error::new(400, "invalid_matrix_version"))?
                .to_owned();
            let (record, mut service) = load_service(context, sid)?;
            require_scoring_mutable(context, sid, &service)?;
            let mut sc = require_scoring_enabled(&service)?.clone();
            if super::audit_outbox_len(&record) >= super::AUDIT_OUTBOX_LIMIT {
                return Err(Error::new(409, "audit_archive_required"));
            }
            let token = resolve_brokered_token(context, &service, None)?;
            context.confirm_with_token(&token);
            reconcile::verify_managed_customer_vcl_ownership(context, &service, &token)?;
            verify_scoring_resources(context, &service.service_id, &sc, &token)?;
            let creds = fos::credentials(context, &service)?;
            let prefix = scoring_fos_prefix(&service);
            let ver_key = format!("{prefix}matrices/{version}.json");
            let matrix = fos_json_get_with_creds(context, &service, &creds, &ver_key)?
                .ok_or_else(|| Error::new(404, "matrix_version_not_found"))?;
            write_matrix_to_kv(context, &sc.scoring_matrix_store_id, &matrix, &token)?;
            fos_json_put_with_creds(
                context,
                &service,
                &creds,
                &format!("{prefix}matrix_latest.json"),
                &matrix,
            )?;
            sc.matrix_version = Some(version.clone());
            service.scoring = Some(sc);
            service.updated_at = context.runtime.now();
            let generation =
                save_scoring_service(context, &record, &service, actor, "scoring.matrix_restored")?;
            append_scoring_audit(
                context,
                sid,
                actor,
                "matrix_restored",
                json!({"version": version}),
            );
            Ok(json!({"ok": true, "version": version, "generation": generation}))
        }
        ("GET", ["labels"]) => {
            let (_, service) = load_service(context, sid)?;
            let labels_key = format!("{}labels.json", scoring_fos_prefix(&service));
            let labels = fos_json_get(context, &service, &labels_key)?
                .and_then(|v| v["labels"].as_array().cloned())
                .unwrap_or_default();
            let mut n_good = 0u64;
            let mut n_bad = 0u64;
            for item in &labels {
                match item["label"].as_str() {
                    Some("good") => n_good += 1,
                    Some("bad") => n_bad += 1,
                    _ => {}
                }
            }
            Ok(json!({
                "labels": labels,
                "counts": {"good": n_good, "bad": n_bad, "total": labels.len()},
            }))
        }
        ("POST", ["labels"]) => {
            let input: Value = body(request)?;
            let edge_sid = input["edge_sid"]
                .as_str()
                .or_else(|| input["sid"].as_str())
                .filter(|v| !v.is_empty())
                .ok_or_else(|| Error::new(400, "invalid_input"))?
                .to_owned();
            let label = required(&input, "label")?.to_owned();
            if !matches!(label.as_str(), "good" | "bad" | "neutral") {
                return Err(Error::new(400, "invalid_label"));
            }
            let (_, service) = load_service(context, sid)?;
            require_scoring_mutable(context, sid, &service)?;
            let labels_key = format!("{}labels.json", scoring_fos_prefix(&service));
            let mut labels = fos_json_get(context, &service, &labels_key)?
                .and_then(|v| v["labels"].as_array().cloned())
                .unwrap_or_default();
            let id = hex_random_32(context.runtime)?;
            let now_iso = rfc3339_from_secs(context.runtime.now());
            let item = json!({
                "id": &id[..16],
                "edge_sid": edge_sid,
                "label": label,
                "notes": input["notes"].as_str().unwrap_or(""),
                "created_by": actor,
                "created_at": now_iso,
            });
            labels.retain(|l| l["edge_sid"] != item["edge_sid"]);
            labels.insert(0, item.clone());
            fos_json_put(context, &service, &labels_key, &json!({"labels": labels}))?;
            append_scoring_audit(
                context,
                sid,
                actor,
                "label_created",
                json!({"edge_sid": item["edge_sid"], "label": item["label"]}),
            );
            Ok(json!({"ok": true, "label": item}))
        }
        ("PATCH", ["labels", label_id]) => {
            let input: Value = body(request)?;
            let (_, service) = load_service(context, sid)?;
            require_scoring_mutable(context, sid, &service)?;
            let labels_key = format!("{}labels.json", scoring_fos_prefix(&service));
            let mut labels = fos_json_get(context, &service, &labels_key)?
                .and_then(|v| v["labels"].as_array().cloned())
                .unwrap_or_default();
            let Some(found) = labels.iter_mut().find(|l| l["id"] == *label_id) else {
                return Err(Error::new(404, "label_not_found"));
            };
            if let Some(label) = input["label"].as_str() {
                if !matches!(label, "good" | "bad" | "neutral") {
                    return Err(Error::new(400, "invalid_label"));
                }
                found["label"] = label.into();
            }
            if let Some(notes) = input["notes"].as_str() {
                found["notes"] = notes.into();
            }
            let updated = found.clone();
            fos_json_put(context, &service, &labels_key, &json!({"labels": labels}))?;
            Ok(json!({"ok": true, "label": updated}))
        }
        ("DELETE", ["labels", label_id]) => {
            let (_, service) = load_service(context, sid)?;
            require_scoring_mutable(context, sid, &service)?;
            let labels_key = format!("{}labels.json", scoring_fos_prefix(&service));
            let mut labels = fos_json_get(context, &service, &labels_key)?
                .and_then(|v| v["labels"].as_array().cloned())
                .unwrap_or_default();
            let before = labels.len();
            labels.retain(|l| l["id"] != *label_id);
            if labels.len() == before {
                return Err(Error::new(404, "label_not_found"));
            }
            fos_json_put(context, &service, &labels_key, &json!({"labels": labels}))?;
            Ok(json!({"ok": true}))
        }
        ("GET", ["audit"]) => {
            let _ = load_service(context, sid)?;
            let key = format!("scoring:audit:{sid}");
            let entries = context
                .get(&key)?
                .and_then(|r| r.value["entries"].as_array().cloned())
                .unwrap_or_default();
            Ok(json!({"entries": entries}))
        }
        _ => Err(Error::new(404, "not_found")),
    }
}

fn update_single_snippet_and_activate(
    context: &Context<'_, impl Runtime>,
    service: &Service,
    token: &str,
    snippet_name: &str,
    snippet_type: &str,
    priority: i64,
    content: &str,
) -> Result<(), Error> {
    let sid = &service.service_id;
    let active = reconcile::verify_managed_customer_vcl_ownership(context, service, token)?;
    let cloned = context.api(
        "PUT",
        &format!("/service/{sid}/version/{active}/clone"),
        token,
        None,
    )?;
    let draft = cloned["number"]
        .as_u64()
        .ok_or_else(|| Error::new(502, "logging_clone_failed"))?;
    let body = json!({
        "name": snippet_name,
        "type": snippet_type,
        "priority": priority,
        "content": content,
        "dynamic": 0,
    });
    let encoded = encode(snippet_name);
    let (status, _) = context.api_status(
        "PUT",
        &format!("/service/{sid}/version/{draft}/snippet/{encoded}"),
        token,
        Some(body.clone()),
    )?;
    if status == 404 {
        context.api(
            "POST",
            &format!("/service/{sid}/version/{draft}/snippet"),
            token,
            Some(body),
        )?;
    } else if !(200..300).contains(&status) {
        return Err(Error::new(502, "snippet_update_failed"));
    }
    let validate = context.api(
        "GET",
        &format!("/service/{sid}/version/{draft}/validate"),
        token,
        None,
    )?;
    if validate["status"] != "ok" {
        return Err(Error::new(400, "draft_validation_failed"));
    }
    context.api(
        "PUT",
        &format!("/service/{sid}/version/{draft}/activate"),
        token,
        None,
    )?;
    Ok(())
}
