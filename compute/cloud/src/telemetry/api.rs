//! `api_telemetry` and `platform_errors` for Compute API requests, outbound Fastly API calls and
//! browser query beacons (design spec 2026-10-30, §2.6, §3.6 and §3.7).

use super::{
    redact, write_log, LogBuffer, LogKind, OperationalLogSettings, WritableLogKind, REDACTED,
};
use crate::{auth::settings::UsageLogging, shell::Runtime};
use serde_json::{json, Map, Value};
use std::cell::{Cell, RefCell};

pub const BEACON_MAX_BYTES: usize = 4096;
pub const BEACON_PER_MINUTE: u64 = 60;
pub const BEACON_MAX_NUMBER: u64 = 1_000_000_000_000;
const BEACON_FIELDS: [&str; 7] = [
    "files_listed",
    "files_fetched",
    "files_cached",
    "cdn_hits",
    "rows_scanned",
    "bytes_scanned",
    "duration_ms",
];
const BEACON_SURFACES: [&str; 2] = ["dashboard", "explorer"];
const BEACON_OUTCOMES: [&str; 3] = ["ok", "error", "aborted"];
const MAX_API_CALLS: usize = 64;
const MAX_ERROR_BYTES: usize = 512;
const MAX_PATH_BYTES: usize = 200;
const SETTINGS_KEY: &str = "settings:operational_logs";
const ID_AFTER: &[&str] = &[
    "users",
    "invites",
    "jobs",
    "services",
    "passkeys",
    "service",
    "version",
    "backend",
    "dictionary",
    "snippet",
    "domain",
    "items",
    "secrets",
    "kv",
    "secret",
    "tls",
    "acl",
    "condition",
    "header",
    "dictionary_item",
    "config",
];

/// Literal path segments of our own routes and of the Fastly API calls we make. Anything else
/// is attacker-chosen and collapses to `:unknown`, which bounds path cardinality.
const KNOWN_SEGMENTS: &[&str] = &[
    "api",
    "auth",
    "setup",
    "accept",
    "session",
    "login",
    "register",
    "options",
    "verify",
    "logout",
    "services",
    "log-fields",
    "catalog",
    "rum",
    "versions",
    "log-extents",
    "files",
    "file",
    "ingest",
    "events",
    "sync-status",
    "cron-runs",
    "trigger",
    "admin",
    "users",
    "invites",
    "revoke",
    "passkeys",
    "usage-logging",
    "audit-logs",
    "ingested-files",
    "raw-tree",
    "storage-tree",
    "quarantine",
    "purge",
    "system-checks",
    "provision",
    "validate",
    "execute",
    "jobs",
    "step",
    "reconcile",
    "preview",
    "telemetry",
    "query",
    "logs",
    "live",
    "operational-logging",
    "flush",
    "clear-cache",
    "delete-history",
    "sweep",
    "backfill",
    "usage",
    "reconcile-storage",
    "compute-billing",
    "logged-traffic",
    "edge_requests",
    "field",
    "compute_requests",
    "compute_execution_time_ms",
    "compute_request_time_billed_ms",
    "status",
    "service",
    "version",
    "backend",
    "dictionary",
    "snippet",
    "domain",
    "items",
    "secrets",
    "stores",
    "kv",
    "secret",
    "config",
    "tls",
    "acl",
    "condition",
    "header",
    "logging",
    "s3",
    "resources",
    "access-keys",
    "activate",
    "clone",
    "cache",
    "purge",
    "stats",
    "detail",
    "vcl",
    "settings",
    "healthcheck",
    "dictionary_item",
    "tokens",
    "current",
    "keys",
    "entries",
    "info",
    "ops",
];

/// Drops the query and replaces identifiers with `:id` and unrecognized segments with
/// `:unknown`, so path cardinality stays bounded.
pub fn normalize_path(path: &str) -> String {
    let path = path.split(['?', '#']).next().unwrap_or(path);
    let mut out = String::new();
    let mut previous = "";
    for segment in path.split('/').filter(|segment| !segment.is_empty()) {
        let known = KNOWN_SEGMENTS.contains(&segment) || LogKind::parse(segment).is_some();
        out.push('/');
        out.push_str(if ID_AFTER.contains(&previous) {
            ":id"
        } else if known {
            segment
        } else {
            ":unknown"
        });
        previous = segment;
    }
    if out.is_empty() {
        out.push('/');
    }
    super::truncate_str(&mut out, MAX_PATH_BYTES);
    out
}

/// A successful live read or beacon is the payload itself, so it must not log a row for itself.
pub fn is_exempt(method: &str, normalized_path: &str, status: u16) -> bool {
    match method {
        "GET" => {
            status == 200
                && normalized_path.starts_with("/api/logs/")
                && normalized_path.ends_with("/live")
        }
        "POST" => normalized_path == "/api/telemetry/query" && matches!(status, 200 | 204),
        _ => false,
    }
}

/// A successful dashboard file read flagged `x-flp-fold-telemetry: 1` is counted in the page's
/// query beacon instead of its own row. Failures still log per request.
pub fn folds_into_beacon(
    method: &str,
    normalized_path: &str,
    status: u16,
    fold_header: Option<&str>,
) -> bool {
    method == "GET" && normalized_path == "/api/file" && status == 200 && fold_header == Some("1")
}

/// The one `compute_api` row standing in for a beacon's folded `/api/file` reads, carrying their
/// count in `files_fetched` and their summed `vcpu_ms`. `None` when the beacon folded none.
pub fn folded_file_row(
    body: &[u8],
    header_page_load_id: Option<&str>,
    harness: Option<&str>,
) -> Option<Value> {
    let Ok(Value::Object(input)) = serde_json::from_slice::<Value>(body) else {
        return None;
    };
    let requests = input.get("compute_requests").and_then(clamp)?;
    if requests == 0 {
        return None;
    }
    let vcpu_ms = input.get("vcpu_ms").and_then(clamp).unwrap_or(0);
    let page_load_id = sanitize_page_load_id(
        input
            .get("page_load_id")
            .and_then(Value::as_str)
            .or(header_page_load_id),
    );
    let mut details = serde_json::Map::from_iter([("folded".to_owned(), json!(true))]);
    if let Some(label) = harness {
        details.insert("harness".into(), json!(label));
    }
    if let Some(id) = page_load_id {
        details.insert("page_load_id".into(), json!(id));
    }
    let details = Value::Object(details);
    Some(json!({
        "source": "compute_api", "method": "GET", "path": "/api/file", "status_code": 200,
        "files_fetched": requests, "vcpu_ms": vcpu_ms, "details_json": details.to_string(),
    }))
}

struct Call {
    method: String,
    path: String,
    status: Option<u16>,
    duration_ms: u64,
    error: Option<String>,
}

/// Outbound Fastly API calls made during one request, drained into rows at its end.
#[derive(Default)]
pub struct ApiLog {
    calls: RefCell<Vec<Call>>,
}

impl ApiLog {
    pub fn is_empty(&self) -> bool {
        self.calls.borrow().is_empty()
    }

    /// `token` is scrubbed from `error` in addition to the structural redaction.
    pub fn record(
        &self,
        method: &str,
        path: &str,
        status: Option<u16>,
        duration_ms: u64,
        error: Option<&str>,
        token: &str,
    ) {
        let mut calls = self.calls.borrow_mut();
        if calls.len() >= MAX_API_CALLS {
            return;
        }
        let error = error.map(|error| {
            let scrubbed = if token.is_empty() {
                error.to_owned()
            } else {
                error.replace(token, REDACTED)
            };
            let Value::String(mut text) = redact(Value::String(scrubbed)) else {
                unreachable!("redacting a string yields a string")
            };
            super::truncate_str(&mut text, MAX_ERROR_BYTES);
            text
        });
        calls.push(Call {
            method: method.to_owned(),
            path: normalize_path(path),
            status,
            duration_ms,
            error,
        });
    }
}

const MAX_PAGE_LOAD_ID_BYTES: usize = 64;

/// Validates a client `X-Page-Load-ID` token so only short, safe ASCII tokens enter telemetry.
pub fn sanitize_page_load_id(value: Option<&str>) -> Option<String> {
    let trimmed = value?.trim();
    if trimmed.is_empty() || trimmed.len() > MAX_PAGE_LOAD_ID_BYTES {
        return None;
    }
    trimmed
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        .then(|| trimmed.to_owned())
}

const MAX_HARNESS_LABEL_BYTES: usize = 48;

/// Validates a load-harness `X-FLP-Harness` label before it enters telemetry: a stage or page label
/// `<campaign>-p<period>-s<N>[-pg<NN>-<mode>]`, or `lt-<call>` for the harness's own API calls.
/// Anything else is ignored, so it is never attributed to the harness.
pub fn sanitize_harness_label(value: Option<&str>) -> Option<String> {
    let trimmed = value?.trim();
    if trimmed.len() > MAX_HARNESS_LABEL_BYTES {
        return None;
    }
    let lower_alnum = |s: &str| {
        !s.is_empty()
            && s.bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
    };
    let numbered = |s: &str, prefix: &str, min_digits: usize| {
        s.strip_prefix(prefix)
            .is_some_and(|d| d.len() >= min_digits && d.bytes().all(|b| b.is_ascii_digit()))
    };
    let valid = if let Some(call) = trimmed.strip_prefix("lt-") {
        call.split('-').all(lower_alnum)
    } else {
        let mut parts: Vec<&str> = trimmed.split('-').collect();
        if parts.len() >= 2 {
            let n = parts.len();
            let mode = parts[n - 1];
            if numbered(parts[n - 2], "pg", 2)
                && !mode.is_empty()
                && mode.bytes().all(|b| b.is_ascii_lowercase())
            {
                parts.truncate(n - 2);
            }
        }
        let n = parts.len();
        n >= 3
            && numbered(parts[n - 1], "s", 1)
            && numbered(parts[n - 2], "p", 1)
            && parts[..n - 2].iter().all(|s| lower_alnum(s))
    };
    valid.then(|| trimmed.to_owned())
}

/// Moves the request's `fos_usage` aggregates and Fastly API calls into the log buffer.
pub fn drain_runtime_rows(runtime: &impl Runtime, buffer: &LogBuffer) {
    let _ = drain_runtime_rows_with_page_load_id(runtime, buffer, None);
}

/// Moves the request's `fos_usage` aggregates and Fastly API calls into the log buffer,
/// attaching `page_load_id` to `fastly_api` rows and returning compact debug call summaries.
pub fn drain_runtime_rows_with_page_load_id(
    runtime: &impl Runtime,
    buffer: &LogBuffer,
    page_load_id: Option<&str>,
) -> Vec<Value> {
    let mut debug_calls = Vec::new();
    for (kind, mut row) in runtime_rows(runtime) {
        match kind {
            WritableLogKind::ApiTelemetry => {
                if let Some(id) = page_load_id {
                    if let Some(obj) = row.as_object_mut() {
                        obj.insert(
                            "details_json".into(),
                            json!({"page_load_id": id}).to_string().into(),
                        );
                    }
                }
                let path = row
                    .get("path")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .unwrap_or("/");
                let status = row
                    .get("status_code")
                    .and_then(Value::as_u64)
                    .filter(|&s| s > 0);
                debug_calls.push(json!({
                    "source": row.get("source").and_then(Value::as_str).unwrap_or("fastly_api"),
                    "method": row.get("method").and_then(Value::as_str).unwrap_or("GET"),
                    "path": path,
                    "endpoint": path,
                    "status": status,
                    "status_code": status.unwrap_or(0),
                    "duration_ms": row.get("duration_ms").and_then(Value::as_u64).unwrap_or(0),
                }));
            }
            WritableLogKind::FosUsage => {
                let op = row.get("op").and_then(Value::as_str).unwrap_or("GET");
                let op_count = row.get("op_count").and_then(Value::as_u64).unwrap_or(1);
                let sample_key = row
                    .get("sample_key")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .or_else(|| {
                        row.get("dataset")
                            .and_then(Value::as_str)
                            .filter(|s| !s.is_empty())
                    })
                    .unwrap_or("fos");
                let display_path = if op_count > 1 {
                    format!("{sample_key} ({op_count} ops)")
                } else {
                    sample_key.to_owned()
                };
                let cdn_cache = row.get("cdn_cache").and_then(Value::as_str);
                let source = if cdn_cache == Some("HIT") {
                    "cdn_cache"
                } else {
                    "fos_s3"
                };
                let status = row
                    .get("status_code")
                    .and_then(Value::as_u64)
                    .filter(|&s| s > 0)
                    .unwrap_or(200);
                let bytes_out = row.get("bytes_out").and_then(Value::as_u64).unwrap_or(0);
                let bytes_in = row.get("bytes_in").and_then(Value::as_u64).unwrap_or(0);
                let bytes = if bytes_out > 0 {
                    Some(bytes_out)
                } else if bytes_in > 0 {
                    Some(bytes_in)
                } else {
                    None
                };
                debug_calls.push(json!({
                    "source": source,
                    "method": op,
                    "path": display_path,
                    "endpoint": sample_key,
                    "status": status,
                    "status_code": status,
                    "duration_ms": row.get("duration_ms").and_then(Value::as_u64).unwrap_or(0),
                    "cdn_cache": cdn_cache,
                    "op_count": op_count,
                    "bytes": bytes,
                    "bytes_out": bytes_out,
                }));
            }
            _ => {}
        }
        write_log(runtime, buffer, kind, row);
    }
    debug_calls
}

/// Drains the execution's FOS usage and Fastly API call records as log rows, for a caller that
/// writes them elsewhere (a worker execution returns them to the tick that dispatched it).
pub fn runtime_rows(runtime: &impl Runtime) -> Vec<(WritableLogKind, Value)> {
    let mut rows = Vec::new();
    if let Some(usage) = runtime.usage_agg() {
        for row in usage.drain_rows(&UsageLogging::default()) {
            rows.push((WritableLogKind::FosUsage, row));
        }
    }
    let Some(log) = runtime.api_log() else {
        return rows;
    };
    for call in std::mem::take(&mut *log.calls.borrow_mut()) {
        rows.push((
            WritableLogKind::ApiTelemetry,
            json!({
                "source": "fastly_api", "method": call.method, "path": call.path,
                "status_code": call.status, "duration_ms": call.duration_ms,
            }),
        ));
        let failed = call.status.is_none_or(|status| status >= 400);
        if !failed && call.error.is_none() {
            continue;
        }
        let code = call
            .status
            .map_or_else(|| "transport_error".to_owned(), |s| format!("http_{s}"));
        rows.push((
            WritableLogKind::PlatformErrors,
            json!({
                "error_domain": "fastly_api", "code": code, "severity": "error",
                "caller": "admin_api", "status_code": call.status,
                "message": format!("{} {}", call.method, call.path),
                "sample_json": json!({"error": call.error}).to_string(),
            }),
        ));
    }
    rows
}

#[derive(Debug, PartialEq, Eq)]
pub enum BeaconError {
    TooLarge,
    Invalid,
}

fn clamp(value: &Value) -> Option<u64> {
    let number = value.as_f64().filter(|n| n.is_finite())?;
    Some(number.clamp(0.0, BEACON_MAX_NUMBER as f64) as u64)
}

fn enum_field(map: &Map<String, Value>, name: &str, allowed: &[&str]) -> Result<String, ()> {
    match map.get(name) {
        None => Ok(allowed[0].to_owned()),
        Some(Value::String(text)) if allowed.contains(&text.as_str()) => Ok(text.clone()),
        Some(_) => Err(()),
    }
}

/// Keeps only the allowlisted fields of a browser beacon. Identity, source and timestamp are
/// assigned server-side, so nothing the client sends for them survives.
pub fn sanitize_beacon(body: &[u8]) -> Result<Value, BeaconError> {
    sanitize_beacon_with_page_load_id(body, None)
}

/// Sanitizes a browser query beacon and optionally attaches a validated `page_load_id`.
pub fn sanitize_beacon_with_page_load_id(
    body: &[u8],
    header_page_load_id: Option<&str>,
) -> Result<Value, BeaconError> {
    if body.len() > BEACON_MAX_BYTES {
        return Err(BeaconError::TooLarge);
    }
    let Ok(Value::Object(input)) = serde_json::from_slice::<Value>(body) else {
        return Err(BeaconError::Invalid);
    };
    let surface =
        enum_field(&input, "surface", &BEACON_SURFACES).map_err(|()| BeaconError::Invalid)?;
    let outcome =
        enum_field(&input, "outcome", &BEACON_OUTCOMES).map_err(|()| BeaconError::Invalid)?;
    let page_load_id = sanitize_page_load_id(
        input
            .get("page_load_id")
            .and_then(Value::as_str)
            .or(header_page_load_id),
    );
    let mut row = Map::new();
    for field in BEACON_FIELDS {
        if let Some(number) = input.get(field).and_then(clamp) {
            row.insert(field.into(), number.into());
        }
    }
    row.insert("source".into(), "browser_query".into());
    row.insert("method".into(), "POST".into());
    row.insert("path".into(), format!("/query/{surface}").into());
    let details = match page_load_id {
        Some(id) => json!({"surface": surface, "outcome": outcome, "page_load_id": id}),
        None => json!({"surface": surface, "outcome": outcome}),
    };
    row.insert("details_json".into(), details.to_string().into());
    Ok(Value::Object(row))
}

/// Counts a beacon against the session's current minute. A cache failure allows the beacon,
/// because the limit only protects best-effort telemetry.
pub fn beacon_allowed(runtime: &impl Runtime, session_key: &str, now: u64) -> bool {
    let key = format!("flp:beacon:v1:{session_key}:{}", now / 60);
    let count = Cell::new(0u64);
    let update = |existing: Option<crate::shell::CoreCacheEntry>| {
        let previous = existing
            .and_then(|(body, _)| String::from_utf8(body).ok())
            .and_then(|text| text.parse::<u64>().ok())
            .unwrap_or(0);
        count.set(previous + 1);
        (count.get().to_string().into_bytes(), Vec::new())
    };
    match runtime.core_cache_replace(
        &key,
        &["flp-beacon"],
        std::time::Duration::from_secs(120),
        &update,
    ) {
        Ok(()) => count.get() <= BEACON_PER_MINUTE,
        Err(_) => true,
    }
}

/// Stored settings, or the defaults when the record is missing, unreadable or no longer valid.
pub fn operational_settings(runtime: &impl Runtime) -> OperationalLogSettings {
    let mut settings = runtime
        .admin_get(SETTINGS_KEY)
        .ok()
        .flatten()
        .and_then(|record| serde_json::from_value::<OperationalLogSettings>(record.value).ok())
        .filter(|settings| settings.validate().is_ok())
        .unwrap_or_default();
    if !super::dev_telemetry_available(runtime) {
        settings.dev_telemetry_enabled = false;
    }
    super::set_dev_telemetry_enabled(settings.dev_telemetry_enabled);
    settings
}

/// Merges a partial settings object into the stored settings, validates the result and saves it.
/// `retention_days_by_kind` merges per kind. Returns the saved settings and the sorted names of
/// the top-level fields that changed.
pub fn patch_operational_settings(
    runtime: &impl Runtime,
    patch: &Value,
) -> Result<(OperationalLogSettings, Vec<String>), String> {
    let fields = patch.as_object().ok_or("settings must be an object")?;
    if fields.get("dev_telemetry_enabled").and_then(Value::as_bool) == Some(true)
        && !super::dev_telemetry_available(runtime)
    {
        return Err("dev_telemetry_unavailable".into());
    }
    let record = runtime.admin_get(SETTINGS_KEY)?;
    let current = record
        .as_ref()
        .and_then(|r| serde_json::from_value::<OperationalLogSettings>(r.value.clone()).ok())
        .filter(|settings| settings.validate().is_ok())
        .unwrap_or_default();
    let mut merged = serde_json::to_value(&current).map_err(|_| "settings_encoding_failed")?;
    for (name, value) in fields {
        match (name.as_str(), merged.get_mut(name)) {
            (_, None) => return Err(format!("unknown field {name}")),
            ("retention_days_by_kind", Some(existing)) => {
                let kinds = value
                    .as_object()
                    .ok_or("retention_days_by_kind must be an object")?;
                for (kind, days) in kinds {
                    existing[kind] = days.clone();
                }
            }
            (_, Some(existing)) => *existing = value.clone(),
        }
    }
    let next: OperationalLogSettings =
        serde_json::from_value(merged).map_err(|_| "a settings value has the wrong type")?;
    next.validate()?;
    let before = serde_json::to_value(&current).map_err(|_| "settings_encoding_failed")?;
    let after = serde_json::to_value(&next).map_err(|_| "settings_encoding_failed")?;
    let mut changed: Vec<String> = fields
        .keys()
        .filter(|name| before[name.as_str()] != after[name.as_str()])
        .cloned()
        .collect();
    changed.sort();
    if !changed.is_empty() {
        let generation = record.as_ref().map(|r| r.generation.as_str());
        if !runtime.admin_cas(SETTINGS_KEY, generation, &after)? {
            return Err("settings_conflict".into());
        }
    }
    super::set_dev_telemetry_enabled(next.dev_telemetry_enabled);
    Ok((next, changed))
}
