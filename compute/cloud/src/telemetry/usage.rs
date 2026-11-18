//! Per-request aggregation of Fastly Object Storage operations into `fos_usage` rows.

use crate::{admin::transport::FASTLY_API_TIMEOUT, auth::settings::UsageLogging, shell::Runtime};
use serde_json::{json, Value};
use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, BTreeSet},
};

const MAX_SIZES: usize = 4096;
const MAX_PLATFORM_METRICS_PAGES: u32 = 4;

fn platform_metrics_time(timestamp: u64) -> Result<String, String> {
    let timestamp = i64::try_from(timestamp).map_err(|_| "platform_metrics_time_invalid")?;
    time::OffsetDateTime::from_unix_timestamp(timestamp)
        .map_err(|_| "platform_metrics_time_invalid")?
        .format(&time::format_description::well_known::Rfc3339)
        .map_err(|_| "platform_metrics_time_invalid".into())
}

#[cfg(target_arch = "wasm32")]
pub(crate) fn fetch_platform_metrics(
    service_id: &str,
    from: u64,
    to: u64,
    metrics: &[&str],
    fetch: impl FnMut(&str, &str) -> Result<crate::admin::ApiResponse, String>,
) -> Result<Value, String> {
    fetch_platform_entity_metrics(service_id, "hourly", from, to, metrics, fetch)
}

fn fetch_platform_entity_metrics(
    entity_id: &str,
    granularity: &str,
    from: u64,
    to: u64,
    metrics: &[&str],
    fetch: impl FnMut(&str, &str) -> Result<crate::admin::ApiResponse, String>,
) -> Result<Value, String> {
    fetch_platform_metrics_inner(Some(entity_id), granularity, from, to, metrics, fetch)
}

fn fetch_platform_aggregate_metrics(
    granularity: &str,
    from: u64,
    to: u64,
    metrics: &[&str],
    fetch: impl FnMut(&str, &str) -> Result<crate::admin::ApiResponse, String>,
) -> Result<Value, String> {
    fetch_platform_metrics_inner(None, granularity, from, to, metrics, fetch)
}

fn fetch_platform_metrics_inner(
    entity_id: Option<&str>,
    granularity: &str,
    from: u64,
    to: u64,
    metrics: &[&str],
    mut fetch: impl FnMut(&str, &str) -> Result<crate::admin::ApiResponse, String>,
) -> Result<Value, String> {
    if metrics.is_empty() {
        return Err("platform_metrics_fields_empty".into());
    }
    if from >= to {
        return Err("platform_metrics_range_invalid".into());
    }
    let bucket_seconds = match granularity {
        "hourly" => 3_600,
        "daily" => 86_400,
        _ => return Err("platform_metrics_granularity_invalid".into()),
    };
    let path = entity_id.map_or_else(
        || format!("/metrics/v2/platform/aggregate/{granularity}"),
        |entity_id| {
            format!(
                "/metrics/v2/platform/entities/{}/{granularity}",
                crate::admin::transport::encode(entity_id)
            )
        },
    );
    let base_query = format!(
        "from={}&to={}&metric={}&limit=10000",
        platform_metrics_time(from)?,
        platform_metrics_time(to)?,
        metrics.join(",")
    );
    let mut cursor = None::<String>;
    let mut cursors = BTreeSet::new();
    let mut timestamps = BTreeSet::new();
    let mut rows = Vec::new();
    let first_bucket = from - from % bucket_seconds;
    let last_bucket = (to - 1) - (to - 1) % bucket_seconds;
    let mut pages = 0;
    loop {
        let query = cursor.as_ref().map_or_else(
            || base_query.clone(),
            |next| {
                format!(
                    "{base_query}&cursor={}",
                    crate::admin::transport::encode(next)
                )
            },
        );
        let response = fetch(&path, &query)?;
        pages += 1;
        if response.status != 200 {
            return Err(format!("platform_metrics_status_{}", response.status));
        }
        let page = response.body["data"]
            .as_array()
            .ok_or("platform_metrics_shape")?;
        for row in page {
            let timestamp = row["timestamp"]
                .as_str()
                .ok_or("platform_metrics_timestamp_invalid")?;
            let parsed = time::OffsetDateTime::parse(
                timestamp,
                &time::format_description::well_known::Rfc3339,
            )
            .map_err(|_| "platform_metrics_timestamp_invalid")?;
            let start = u64::try_from(parsed.unix_timestamp())
                .map_err(|_| "platform_metrics_timestamp_invalid")?;
            if start % bucket_seconds != 0 || start < first_bucket || start > last_bucket {
                return Err("platform_metrics_timestamp_out_of_window".into());
            }
            if !timestamps.insert(start) {
                return Err("platform_metrics_duplicate_timestamp".into());
            }
            rows.push(row.clone());
        }
        cursor = match response.body.get("next_cursor") {
            None | Some(Value::Null) => None,
            Some(Value::String(next)) if !next.is_empty() && cursors.insert(next.clone()) => {
                Some(next.clone())
            }
            Some(Value::String(_)) => {
                return Err("platform_metrics_pagination_loop".into());
            }
            Some(_) => return Err("platform_metrics_pagination_invalid".into()),
        };
        if cursor.is_none() {
            break;
        }
        if pages >= MAX_PLATFORM_METRICS_PAGES {
            return Err("platform_metrics_page_limit".into());
        }
    }
    Ok(json!({"data": rows}))
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct Key {
    op: &'static str,
    dataset: &'static str,
    caller: &'static str,
    status: u16,
    cdn_cache: &'static str,
}

#[derive(Default)]
struct Row {
    count: u64,
    sample_key: String,
    duration_ms: u64,
    bytes_in: u64,
    bytes_out: u64,
    bytes_deleted: u64,
}

pub struct UsageAgg {
    caller: Cell<&'static str>,
    harness: RefCell<Option<String>>,
    rows: RefCell<BTreeMap<Key, Row>>,
    sizes: RefCell<BTreeMap<String, u64>>,
}

impl Default for UsageAgg {
    fn default() -> Self {
        Self {
            caller: Cell::new("unattributed"),
            harness: RefCell::default(),
            rows: RefCell::default(),
            sizes: RefCell::default(),
        }
    }
}

fn dataset_of(path: &str) -> &'static str {
    if path.contains("processed/ops/") {
        "pq_ops"
    } else if path.contains("processed/rum/") {
        "pq_rum"
    } else if path.contains("processed/") {
        "pq_request"
    } else if path.contains("raw/rum/") {
        "raw_rum"
    } else {
        "raw_request"
    }
}

fn op_class(op: &str, cdn_cache: &str) -> &'static str {
    match (op, cdn_cache) {
        (_, "HIT") => "CDN_HIT",
        ("GET" | "HEAD", _) => "B",
        _ => "A",
    }
}

impl UsageAgg {
    pub fn set_caller(&self, caller: &'static str) {
        self.caller.set(caller);
    }

    /// Labels every drained row with `label` and prefixes its caller with `harness:` so load-test
    /// usage can be subtracted.
    pub fn mark_harness(&self, label: &str) {
        *self.harness.borrow_mut() = Some(label.to_owned());
    }

    /// Remembers object sizes from a LIST so a later delete can record `bytes_deleted`.
    pub fn note_sizes<'a>(&self, objects: impl IntoIterator<Item = (&'a str, u64)>) {
        let mut sizes = self.sizes.borrow_mut();
        for (key, size) in objects {
            if sizes.len() >= MAX_SIZES {
                break;
            }
            sizes.insert(key.to_owned(), size);
        }
    }

    /// Credits the remembered sizes of confirmed `DeleteObjects` keys to `bytes_deleted`.
    pub fn record_deleted(&self, keys: &[String]) {
        let mut sizes = self.sizes.borrow_mut();
        let bytes: u64 = keys.iter().filter_map(|key| sizes.remove(key)).sum();
        drop(sizes);
        if let (Some(first), true) = (keys.first(), bytes > 0) {
            self.add("DELETE_OBJECTS", first, Some(200), "DIRECT", 0, 0, 0, bytes);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn add(
        &self,
        op: &'static str,
        path: &str,
        status: Option<u16>,
        cdn_cache: &'static str,
        duration_ms: u64,
        bytes_in: u64,
        bytes_out: u64,
        bytes_deleted: u64,
    ) {
        let key = Key {
            op,
            dataset: dataset_of(path),
            caller: self.caller.get(),
            status: status.unwrap_or(0),
            cdn_cache,
        };
        let mut rows = self.rows.borrow_mut();
        let row = rows.entry(key).or_default();
        if row.count == 0 {
            row.sample_key = path.to_owned();
        }
        row.count += 1;
        row.duration_ms += duration_ms;
        row.bytes_in += bytes_in;
        row.bytes_out += bytes_out;
        row.bytes_deleted += bytes_deleted;
    }

    /// A direct S3 call. A GET with no key is a LIST; its `prefix` query decides the dataset.
    #[allow(clippy::too_many_arguments)]
    pub fn record_fos(
        &self,
        method: &str,
        key: &str,
        query: &str,
        status: Option<u16>,
        duration_ms: u64,
        bytes_in: u64,
        bytes_out: u64,
    ) {
        let mut path = key.to_owned();
        let op = match method {
            "PUT" => "PUT",
            "HEAD" => "HEAD",
            "DELETE" => "DELETE",
            "POST" if query.contains("delete") => "DELETE_OBJECTS",
            "GET" if key.is_empty() => {
                path = url::form_urlencoded::parse(query.as_bytes())
                    .find(|(name, _)| name == "prefix")
                    .map(|(_, value)| value.into_owned())
                    .unwrap_or_default();
                "LIST"
            }
            "GET" => "GET",
            _ => return,
        };
        let deleted = if op == "DELETE" && status.is_some_and(|s| (200..300).contains(&s)) {
            self.sizes.borrow_mut().remove(key).unwrap_or(0)
        } else {
            0
        };
        self.add(
            op,
            &path,
            status,
            "DIRECT",
            duration_ms,
            bytes_in,
            bytes_out,
            deleted,
        );
    }

    /// A read through `/fos` on the front service; a MISS (or unknown) costs one Class B.
    pub fn record_cdn(
        &self,
        key: &str,
        status: Option<u16>,
        cdn_cache: Option<&str>,
        duration_ms: u64,
        bytes_out: u64,
    ) {
        let cache = if cdn_cache == Some("HIT") {
            "HIT"
        } else {
            "MISS"
        };
        self.add("GET", key, status, cache, duration_ms, 0, bytes_out, 0);
    }

    pub fn is_empty(&self) -> bool {
        self.rows.borrow().is_empty()
    }

    /// Takes the accumulated rows, priced with the configured rates.
    pub fn drain_rows(&self, rates: &UsageLogging) -> Vec<Value> {
        let harness = self.harness.borrow().clone();
        std::mem::take(&mut *self.rows.borrow_mut())
            .into_iter()
            .map(|(key, row)| {
                let class = op_class(key.op, key.cdn_cache);
                let unit = match class {
                    "A" => rates.class_a_rate_per_1k / 1000.0,
                    "B" => rates.class_b_rate_per_10k / 10_000.0,
                    _ => 0.0,
                };
                let caller = if harness.is_some() {
                    format!("harness:{}", key.caller)
                } else {
                    key.caller.to_owned()
                };
                let mut out = json!({
                    "op": key.op, "op_class": class, "op_count": row.count,
                    "dataset": key.dataset, "sample_key": row.sample_key,
                    "caller": caller, "status_code": key.status,
                    "duration_ms": row.duration_ms, "bytes_in": row.bytes_in,
                    "bytes_out": row.bytes_out, "bytes_deleted": row.bytes_deleted,
                    "cdn_cache": key.cdn_cache, "est_cost_usd": unit * row.count as f64,
                });
                if let Some(label) = &harness {
                    out["harness"] = json!(label);
                }
                out
            })
            .collect()
    }
}

/// Fastly Stats API fields billed on the app and worker Compute services.
pub const BILLING_FIELDS: [&str; 6] = [
    "compute_requests",
    "compute_execution_time_ms",
    "compute_request_time_billed_ms",
    "bandwidth",
    "kv_store_class_a_operations",
    "kv_store_class_b_operations",
];

/// Stats API fields read from the logged service to size the cost estimate.
pub const TRAFFIC_FIELDS: [&str; 2] = ["requests", "edge_requests"];

/// Sums hourly `{start_time, <field>}` buckets into `(utc_day_start, total)` pairs, oldest first.
pub fn daily_totals(buckets: &[Value], field: &str) -> Vec<(u64, f64)> {
    let mut days = std::collections::BTreeMap::<u64, f64>::new();
    for bucket in buckets {
        if let Some(start) = bucket["start_time"].as_u64() {
            *days.entry(start - start % 86_400).or_default() +=
                bucket[field].as_f64().unwrap_or(0.0);
        }
    }
    days.into_iter().collect()
}

/// Billed Stats API fields on the front VCL service, besides its hourly `bandwidth`.
pub const FRONT_BILLING_FIELDS: [&str; 3] = [
    "requests",
    "kv_store_class_a_operations",
    "kv_store_class_b_operations",
];

pub const PLATFORM_COMPUTE_FIELDS: [&str; 6] = [
    "compute_requests",
    "compute_request_time_billed_ms",
    "compute_service_bereq_error",
    "compute_cache_operations_count",
    "compute_sandboxes",
    "compute_service_resource_limits_error",
];

pub const PLATFORM_STORAGE_FIELDS: [&str; 2] =
    ["object_storage_buckets_count", "object_storage_bytes_count"];

pub const PLATFORM_NGWAF_FIELDS: [&str; 7] = [
    "ngwaf_requests_total_count",
    "ngwaf_requests_allowed_count",
    "ngwaf_requests_blocked_count",
    "ngwaf_requests_challenged_count",
    "ngwaf_requests_logged_count",
    "ngwaf_requests_timeout_count",
    "ngwaf_requests_unknown_count",
];

fn platform_rows<'a>(
    body: &'a Value,
    from: u64,
    to: u64,
    bucket_seconds: u64,
) -> Result<Vec<(u64, &'a Value)>, String> {
    if from >= to {
        return Err("platform_metrics_range_invalid".into());
    }
    let rows = body["data"].as_array().ok_or("platform_metrics_shape")?;
    let first = from - from % bucket_seconds;
    let last = (to - 1) - (to - 1) % bucket_seconds;
    let mut starts = BTreeSet::new();
    let mut output = Vec::with_capacity(rows.len());
    for row in rows {
        let timestamp = row["timestamp"]
            .as_str()
            .ok_or("platform_metrics_timestamp_invalid")?;
        let parsed =
            time::OffsetDateTime::parse(timestamp, &time::format_description::well_known::Rfc3339)
                .map_err(|_| "platform_metrics_timestamp_invalid")?;
        let start = u64::try_from(parsed.unix_timestamp())
            .map_err(|_| "platform_metrics_timestamp_invalid")?;
        if start % bucket_seconds != 0 || start < first || start > last {
            return Err("platform_metrics_timestamp_out_of_window".into());
        }
        if !starts.insert(start) {
            return Err("platform_metrics_duplicate_timestamp".into());
        }
        output.push((start, row));
    }
    Ok(output)
}

fn platform_number(row: &Value, field: &str) -> Result<Option<f64>, String> {
    let Some(value) = row.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let number = value
        .as_f64()
        .filter(|value| value.is_finite() && *value >= 0.0)
        .ok_or("platform_metrics_value_invalid")?;
    Ok(Some(number))
}

fn platform_count(row: &Value, field: &str) -> Result<Option<u64>, String> {
    let Some(value) = row.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_u64()
        .ok_or_else(|| "platform_metrics_value_invalid".into())
        .map(Some)
}

fn platform_timestamp(timestamp: u64) -> Result<String, String> {
    let timestamp = i64::try_from(timestamp).map_err(|_| "platform_metrics_time_invalid")?;
    time::OffsetDateTime::from_unix_timestamp(timestamp)
        .map_err(|_| "platform_metrics_time_invalid")?
        .format(&time::format_description::well_known::Rfc3339)
        .map_err(|_| "platform_metrics_time_invalid".into())
}

fn unavailable_section(scope: &str) -> Value {
    if scope.is_empty() {
        json!({"status": "unavailable", "reason": "platform_metrics_unavailable"})
    } else {
        json!({
            "status": "unavailable",
            "scope": scope,
            "reason": "platform_metrics_unavailable"
        })
    }
}

fn clean_status(section: &Value) -> &'static str {
    match section["status"].as_str() {
        Some("ok") => "ok",
        Some("not_configured") => "not_configured",
        _ => "unavailable",
    }
}

fn clean_number(section: &Value, field: &str) -> Option<Value> {
    let value = section.get(field)?;
    let number = value.as_f64()?;
    (number.is_finite() && number >= 0.0).then(|| value.clone())
}

fn clean_platform_storage(section: Option<&Value>) -> Value {
    let Some(section) = section else {
        return unavailable_section("account");
    };
    if clean_status(section) != "ok" {
        return unavailable_section("account");
    }
    let mut output = json!({"status": "ok", "scope": "account"});
    for field in ["bytes", "buckets", "gb_hours"] {
        if let Some(value) = clean_number(section, field) {
            output[field] = value;
        }
    }
    if let Some(observed_at) = section["observed_at"].as_str() {
        if time::OffsetDateTime::parse(observed_at, &time::format_description::well_known::Rfc3339)
            .is_ok()
        {
            output["observed_at"] = json!(observed_at);
        }
    }
    output
}

fn clean_platform_compute(section: Option<&Value>) -> Value {
    let Some(section) = section else {
        return unavailable_section("app_and_worker");
    };
    if clean_status(section) != "ok" {
        return unavailable_section("app_and_worker");
    }
    let mut output = json!({"status": "ok", "scope": "app_and_worker"});
    for field in [
        "requests",
        "billed_time_ms",
        "backend_errors",
        "cache_operations",
        "sandboxes",
        "resource_errors",
    ] {
        output[field] = clean_number(section, field).unwrap_or_else(|| json!(0.0));
    }
    output
}

fn clean_platform_ngwaf(section: Option<&Value>) -> Value {
    let Some(section) = section else {
        return unavailable_section("workspace");
    };
    match clean_status(section) {
        "not_configured" => return json!({"status": "not_configured", "scope": "workspace"}),
        "ok" => {}
        _ => return unavailable_section("workspace"),
    }
    let mut output = json!({"status": "ok", "scope": "workspace"});
    for field in [
        "total",
        "allowed",
        "blocked",
        "challenged",
        "logged",
        "timeout",
        "unknown",
    ] {
        output[field] = clean_number(section, field).unwrap_or_else(|| json!(0.0));
    }
    output
}

pub fn build_platform_usage_response(
    storage: Option<Value>,
    compute: Option<Value>,
    ngwaf: Option<Value>,
) -> Value {
    let storage = clean_platform_storage(storage.as_ref());
    let compute = clean_platform_compute(compute.as_ref());
    let ngwaf = clean_platform_ngwaf(ngwaf.as_ref());
    let statuses = [&storage["status"], &compute["status"], &ngwaf["status"]];
    let successful = statuses.iter().filter(|status| **status == "ok").count();
    let unavailable = statuses
        .iter()
        .filter(|status| **status == "unavailable")
        .count();
    let source = if successful > 0 && unavailable == 0 {
        "fastly"
    } else if successful > 0 {
        "partial"
    } else {
        "unavailable"
    };
    json!({"source": source, "storage": storage, "compute": compute, "ngwaf": ngwaf})
}

pub fn platform_storage(body: &Value, from: u64, to: u64) -> Result<Value, String> {
    let rows = platform_rows(body, from, to, 86_400)?;
    let mut bytes = Vec::new();
    let mut buckets = Vec::new();
    for (start, row) in rows {
        if let Some(value) = platform_number(row, "object_storage_bytes_count")? {
            if value.fract() != 0.0 || value > u64::MAX as f64 {
                return Err("platform_metrics_value_invalid".into());
            }
            bytes.push((start, value as u64));
        }
        if let Some(value) = platform_count(row, "object_storage_buckets_count")? {
            buckets.push((start, value));
        }
    }
    bytes.sort_unstable_by_key(|(start, _)| *start);
    buckets.sort_unstable_by_key(|(start, _)| *start);
    if bytes.is_empty() {
        return Err("platform_metrics_storage_empty".into());
    }
    let mut gb_hours = 0.0;
    for (index, (start, value)) in bytes.iter().copied().enumerate() {
        let next = bytes.get(index + 1).map_or(to, |(next, _)| *next);
        let overlap = next.min(to).saturating_sub(start.max(from));
        gb_hours += value as f64 / 1_073_741_824.0 * overlap as f64 / 3_600.0;
    }
    let (observed, latest_bytes) = bytes[bytes.len() - 1];
    let mut output = json!({
        "status": "ok",
        "scope": "account",
        "observed_at": platform_timestamp(observed)?,
        "bytes": latest_bytes,
        "gb_hours": gb_hours,
    });
    if let Some((_, latest_buckets)) = buckets.last() {
        output["buckets"] = json!(latest_buckets);
    }
    Ok(output)
}

fn sum_platform_field(bodies: &[Value], field: &str, from: u64, to: u64) -> Result<f64, String> {
    let mut total = 0.0;
    for body in bodies {
        for (_, row) in platform_rows(body, from, to, 3_600)? {
            total += platform_number(row, field)?.unwrap_or(0.0);
        }
    }
    Ok(total)
}

fn sum_platform_count(bodies: &[Value], field: &str, from: u64, to: u64) -> Result<u64, String> {
    let mut total = 0u64;
    for body in bodies {
        for (_, row) in platform_rows(body, from, to, 3_600)? {
            total = total
                .checked_add(platform_count(row, field)?.unwrap_or(0))
                .ok_or("platform_metrics_value_invalid")?;
        }
    }
    Ok(total)
}

pub fn platform_compute(bodies: &[Value], from: u64, to: u64) -> Result<Value, String> {
    if bodies.is_empty() {
        return Err("platform_metrics_compute_empty".into());
    }
    let fields = [
        "compute_requests",
        "compute_request_time_billed_ms",
        "compute_service_bereq_error",
        "compute_cache_operations_count",
        "compute_sandboxes",
        "compute_service_resource_limits_error",
    ];
    let mut has_metric = false;
    for body in bodies {
        let rows = platform_rows(body, from, to, 3_600)?;
        if rows.is_empty() {
            return Err("platform_metrics_compute_empty".into());
        }
        for (_, row) in rows {
            for field in fields {
                has_metric |= platform_number(row, field)?.is_some();
            }
        }
    }
    if !has_metric {
        return Err("platform_metrics_compute_empty".into());
    }
    Ok(json!({
        "status": "ok",
        "scope": "app_and_worker",
        "requests": sum_platform_count(bodies, "compute_requests", from, to)?,
        "billed_time_ms": sum_platform_field(
            bodies, "compute_request_time_billed_ms", from, to
        )?,
        "backend_errors": sum_platform_count(
            bodies, "compute_service_bereq_error", from, to
        )?,
        "cache_operations": sum_platform_count(
            bodies, "compute_cache_operations_count", from, to
        )?,
        "sandboxes": sum_platform_count(bodies, "compute_sandboxes", from, to)?,
        "resource_errors": sum_platform_count(
            bodies, "compute_service_resource_limits_error", from, to
        )?,
    }))
}

pub fn platform_ngwaf(body: &Value, from: u64, to: u64) -> Result<Value, String> {
    let bodies = [body.clone()];
    let rows = platform_rows(body, from, to, 3_600)?;
    if rows.is_empty() {
        return Err("platform_metrics_ngwaf_empty".into());
    }
    let fields = [
        "ngwaf_requests_total_count",
        "ngwaf_requests_allowed_count",
        "ngwaf_requests_blocked_count",
        "ngwaf_requests_challenged_count",
        "ngwaf_requests_logged_count",
        "ngwaf_requests_timeout_count",
        "ngwaf_requests_unknown_count",
    ];
    let mut has_metric = false;
    for (_, row) in rows {
        for field in fields {
            has_metric |= platform_count(row, field)?.is_some();
        }
    }
    if !has_metric {
        return Err("platform_metrics_ngwaf_empty".into());
    }
    Ok(json!({
        "status": "ok",
        "scope": "workspace",
        "total": sum_platform_count(&bodies, "ngwaf_requests_total_count", from, to)?,
        "allowed": sum_platform_count(&bodies, "ngwaf_requests_allowed_count", from, to)?,
        "blocked": sum_platform_count(&bodies, "ngwaf_requests_blocked_count", from, to)?,
        "challenged": sum_platform_count(
            &bodies, "ngwaf_requests_challenged_count", from, to
        )?,
        "logged": sum_platform_count(&bodies, "ngwaf_requests_logged_count", from, to)?,
        "timeout": sum_platform_count(&bodies, "ngwaf_requests_timeout_count", from, to)?,
        "unknown": sum_platform_count(&bodies, "ngwaf_requests_unknown_count", from, to)?,
    }))
}

pub(crate) fn platform_usage(runtime: &impl Runtime, from: u64, to: u64) -> Value {
    let token = runtime
        .secret("fastly_token")
        .ok()
        .flatten()
        .filter(|token| !token.is_empty());
    let fetch = |path: &str, query: &str, token: &str| {
        runtime.admin_api(crate::admin::ApiRequest {
            method: "GET",
            path,
            query,
            token,
            body: None,
            timeout: FASTLY_API_TIMEOUT,
        })
    };

    let storage = token.as_deref().and_then(|token| {
        fetch_platform_aggregate_metrics(
            "daily",
            from,
            to,
            &PLATFORM_STORAGE_FIELDS,
            |path, query| fetch(path, query, token),
        )
        .and_then(|body| platform_storage(&body, from, to))
        .ok()
    });

    let compute = token.as_deref().and_then(|token| {
        let ids = ["flp_service_id", "worker_service_id"]
            .into_iter()
            .map(|name| {
                runtime
                    .config(name)?
                    .filter(|id| !id.is_empty())
                    .ok_or_else(|| "platform_compute_not_configured".into())
            })
            .collect::<Result<Vec<_>, String>>()
            .ok()?;
        let bodies = ids
            .iter()
            .map(|id| {
                fetch_platform_entity_metrics(
                    id,
                    "hourly",
                    from,
                    to,
                    &PLATFORM_COMPUTE_FIELDS,
                    |path, query| fetch(path, query, token),
                )
            })
            .collect::<Result<Vec<_>, _>>()
            .ok()?;
        platform_compute(&bodies, from, to).ok()
    });

    let ngwaf = match runtime.config("ngwaf_workspace_id") {
        Ok(Some(workspace)) if !workspace.is_empty() => token.as_deref().and_then(|token| {
            fetch_platform_entity_metrics(
                &workspace,
                "hourly",
                from,
                to,
                &PLATFORM_NGWAF_FIELDS,
                |path, query| fetch(path, query, token),
            )
            .and_then(|body| platform_ngwaf(&body, from, to))
            .ok()
        }),
        Ok(None) | Ok(Some(_)) => Some(json!({"status": "not_configured"})),
        Err(_) => None,
    };

    build_platform_usage_response(storage, compute, ngwaf)
}

/// Sums one Platform Metrics response, returning the total and the first and last bucket start
/// times (unix s) it covered.
pub fn sum_stats_field(
    body: &Value,
    field: &str,
) -> Result<(f64, Option<u64>, Option<u64>), String> {
    let (total, first, last, _) = stats_field_buckets(body, field)?;
    Ok((total, first, last))
}

pub type StatsBuckets = (f64, Option<u64>, Option<u64>, Vec<Value>);

/// Writes one app or worker service's Stats API `field` onto `service`: its total, the hours
/// covered and, for the fields the cost page charts, the hourly buckets.
pub fn record_billing_field(service: &mut Value, field: &str, body: &Value) -> Result<(), String> {
    let (total, first, last, buckets) = stats_field_buckets(body, field)?;
    let bucket_key = match field {
        "kv_store_class_a_operations" => Some("kv_class_a_buckets".to_string()),
        "kv_store_class_b_operations" => Some("kv_class_b_buckets".to_string()),
        "compute_requests" | "compute_execution_time_ms" | "compute_request_time_billed_ms" => {
            Some(format!("{field}_buckets"))
        }
        _ => None,
    };
    service[field] = total.into();
    if let Some(key) = bucket_key {
        service[key] = buckets.into();
    }
    service["covered_from"] = first.into();
    service["covered_to"] = last.map(|start| start + 3600).into();
    Ok(())
}

/// Sums one Platform Metrics response and returns `(total, first, last, buckets)` where each bucket
/// has `{"start_time": u64, "<field>": f64}`.
pub fn stats_field_buckets(body: &Value, field: &str) -> Result<StatsBuckets, String> {
    let rows = body["data"].as_array().ok_or("platform_metrics_shape")?;
    let mut total = 0.0;
    let (mut first, mut last) = (None::<u64>, None::<u64>);
    let mut starts = BTreeSet::new();
    let mut buckets = Vec::with_capacity(rows.len());
    for row in rows {
        let timestamp = row["timestamp"]
            .as_str()
            .ok_or("platform_metrics_timestamp_invalid")?;
        let timestamp =
            time::OffsetDateTime::parse(timestamp, &time::format_description::well_known::Rfc3339)
                .map_err(|_| "platform_metrics_timestamp_invalid")?;
        let start = u64::try_from(timestamp.unix_timestamp())
            .map_err(|_| "platform_metrics_timestamp_invalid")?;
        if !starts.insert(start) {
            return Err("platform_metrics_duplicate_timestamp".into());
        }
        let value = row[field].as_f64().unwrap_or(0.0);
        total += value;
        first = Some(first.map_or(start, |f| f.min(start)));
        last = Some(last.map_or(start, |l| l.max(start)));
        buckets.push(json!({"start_time": start, field: value}));
    }
    Ok((total, first, last, buckets))
}

#[cfg(test)]
mod billing_tests {
    use super::{
        build_platform_usage_response, daily_totals, fetch_platform_aggregate_metrics,
        fetch_platform_entity_metrics, platform_compute, platform_ngwaf, platform_storage,
        record_billing_field, sum_stats_field, UsageAgg, BILLING_FIELDS,
    };
    use crate::admin::ApiResponse;
    use crate::auth::settings::UsageLogging;
    use serde_json::json;
    use std::cell::Cell;

    #[test]
    fn keeps_the_hourly_buckets_of_every_compute_and_kv_billing_field() {
        let hour = 1_793_469_600u64;
        let mut service = json!({"role": "app", "service_id": "svc"});
        for field in BILLING_FIELDS {
            let body = json!({"data": [
                {"timestamp": "2026-10-31T18:00:00Z", field: 2.0},
                {"timestamp": "2026-10-31T19:00:00Z", field: 3.0},
            ], "next_cursor": null});
            record_billing_field(&mut service, field, &body).unwrap();
        }
        for field in BILLING_FIELDS {
            assert_eq!(service[field], 5.0, "{field}");
        }
        for (key, field) in [
            ("compute_requests_buckets", "compute_requests"),
            (
                "compute_execution_time_ms_buckets",
                "compute_execution_time_ms",
            ),
            (
                "compute_request_time_billed_ms_buckets",
                "compute_request_time_billed_ms",
            ),
            ("kv_class_a_buckets", "kv_store_class_a_operations"),
            ("kv_class_b_buckets", "kv_store_class_b_operations"),
        ] {
            assert_eq!(
                service[key],
                json!([
                    {"start_time": hour, field: 2.0},
                    {"start_time": hour + 3_600, field: 3.0},
                ]),
                "{key}"
            );
        }
        assert!(service.get("bandwidth_buckets").is_none());
        assert_eq!(service["covered_from"], hour);
        assert_eq!(service["covered_to"], hour + 7_200);
    }

    #[test]
    fn harness_mark_labels_and_prefixes_every_drained_row() {
        let usage = UsageAgg::default();
        usage.set_caller("api:file");
        usage.record_fos(
            "GET",
            "processed/request/a.parquet",
            "",
            Some(200),
            1,
            0,
            10,
        );
        let plain = usage.drain_rows(&UsageLogging::default());
        assert_eq!(plain[0]["caller"], "api:file");
        assert!(plain[0].get("harness").is_none());

        usage.mark_harness("camp-p10-s1");
        usage.record_fos(
            "GET",
            "processed/request/a.parquet",
            "",
            Some(200),
            1,
            0,
            10,
        );
        let rows = usage.drain_rows(&UsageLogging::default());
        assert_eq!(rows[0]["caller"], "harness:api:file");
        assert_eq!(rows[0]["harness"], "camp-p10-s1");
    }

    #[test]
    fn sums_a_stats_field_and_reports_the_covered_buckets() {
        let body = json!({"data": [
            {"compute_requests": 35, "timestamp": "2026-10-31T18:00:00Z"},
            {"compute_requests": 30, "timestamp": "2026-10-31T19:00:00Z"},
            {"timestamp": "2026-10-31T20:00:00Z"},
        ]});
        assert_eq!(
            sum_stats_field(&body, "compute_requests").unwrap(),
            (65.0, Some(1_793_469_600), Some(1_793_476_800))
        );
        let ms = json!({"data": [{"compute_execution_time_ms": 1.25, "timestamp": "1970-01-01T00:00:01Z"}]});
        assert_eq!(
            sum_stats_field(&ms, "compute_execution_time_ms").unwrap().0,
            1.25
        );
    }

    #[test]
    fn groups_hourly_buckets_into_utc_days() {
        let day = 1_793_404_800u64;
        let buckets = [
            json!({"start_time": day + 82_800, "requests": 5.0}),
            json!({"start_time": day, "requests": 2.0}),
            json!({"start_time": day + 86_400, "requests": 7.0}),
            json!({"start_time": day + 3_600}),
        ];
        assert_eq!(
            daily_totals(&buckets, "requests"),
            vec![(day, 7.0), (day + 86_400, 7.0)]
        );
    }

    #[test]
    fn rejects_malformed_platform_rows() {
        assert!(sum_stats_field(
            &json!({"data": [
                {"timestamp": "not-a-time", "compute_requests": 1}
            ]}),
            "compute_requests"
        )
        .is_err());
        assert!(sum_stats_field(&json!({"next_cursor": null}), "compute_requests").is_err());
    }

    #[test]
    fn rejects_duplicate_platform_timestamps() {
        let body = json!({"data": [
            {"timestamp": "2026-10-31T18:00:00Z", "compute_requests": 1},
            {"timestamp": "2026-10-31T18:00:00+00:00", "compute_requests": 2},
        ]});
        assert!(sum_stats_field(&body, "compute_requests").is_err());
    }

    #[test]
    fn platform_usage_keeps_partial_optional_sections_without_exposing_inputs() {
        let body = build_platform_usage_response(
            Some(json!({
                "status": "ok", "scope": "account", "bytes": 2048,
                "buckets": 2, "gb_hours": 0.000001,
            })),
            None,
            Some(json!({
                "status": "ok", "scope": "workspace", "total": 5, "blocked": 3,
            })),
        );

        assert_eq!(body["source"], "partial");
        assert_eq!(body["storage"]["bytes"], 2048);
        assert_eq!(body["compute"]["status"], "unavailable");
        assert_eq!(body["ngwaf"]["blocked"], 3);
        assert!(body.to_string().contains("unavailable"));
        assert!(!body.to_string().contains("service/id"));
        assert!(!body.to_string().contains("cursor"));
    }

    #[test]
    fn platform_storage_uses_daily_gauges_and_carries_sparse_values() {
        let day = 1_794_355_200u64;
        let body = platform_storage(
            &json!({"data": [
                {
                    "timestamp": "2026-11-11T00:00:00Z",
                    "object_storage_bytes_count": 1_073_741_824,
                    "object_storage_buckets_count": 4,
                },
                {
                    "timestamp": "2026-11-12T00:00:00Z",
                    "object_storage_bytes_count": 2_147_483_648u64
                }
            ]}),
            day,
            day + 2 * 86_400,
        )
        .unwrap();

        assert_eq!(body["bytes"], 2_147_483_648u64);
        assert_eq!(body["buckets"], 4);
        assert_eq!(body["gb_hours"], 72.0);
        assert_eq!(body["observed_at"], "2026-11-12T00:00:00Z");
    }

    #[test]
    fn platform_storage_rejects_empty_duplicate_and_out_of_window_rows() {
        for body in [
            json!({"data": []}),
            json!({"data": [
                {"timestamp": "2026-11-11T00:00:00Z", "object_storage_bytes_count": 1},
                {"timestamp": "2026-11-11T00:00:00+00:00", "object_storage_bytes_count": 2},
            ]}),
            json!({"data": [
                {"timestamp": "2026-11-10T00:00:00Z", "object_storage_bytes_count": 1},
            ]}),
        ] {
            assert!(platform_storage(&body, 1_794_355_200, 1_794_355_200 + 86_400).is_err());
        }
    }

    #[test]
    fn platform_compute_sums_app_and_worker_counters_from_one_response_per_service() {
        let bodies = [
            json!({"data": [{
                "timestamp": "2026-11-11T00:00:00Z",
                "compute_requests": 3,
                "compute_request_time_billed_ms": 50.0,
                "compute_service_bereq_error": 2,
                "compute_cache_operations_count": 4,
                "compute_sandboxes": 5,
                "compute_service_resource_limits_error": 6
            }]}),
            json!({"data": [{
                "timestamp": "2026-11-11T00:00:00Z",
                "compute_requests": 7,
                "compute_request_time_billed_ms": 100.0,
                "compute_service_bereq_error": 11,
                "compute_cache_operations_count": 13,
                "compute_sandboxes": 17,
                "compute_service_resource_limits_error": 19
            }]}),
        ];
        let body = platform_compute(&bodies, 1_794_355_200, 1_794_355_200 + 3_600).unwrap();

        assert_eq!(body["requests"], 10);
        assert_eq!(body["billed_time_ms"], 150.0);
        assert_eq!(body["backend_errors"], 13);
        assert_eq!(body["cache_operations"], 17);
        assert_eq!(body["sandboxes"], 22);
        assert_eq!(body["resource_errors"], 25);
    }

    #[test]
    fn platform_compute_and_ngwaf_reject_metricless_rows() {
        let body = json!({"data": [{"timestamp": "2026-11-11T00:00:00Z"}]});
        assert!(platform_compute(
            &[body.clone(), body.clone()],
            1_794_355_200,
            1_794_355_200 + 3_600
        )
        .is_err());
        assert!(platform_ngwaf(&body, 1_794_355_200, 1_794_355_200 + 3_600).is_err());
    }

    #[test]
    fn platform_ngwaf_sums_only_the_sanitized_workspace_counters() {
        let body = platform_ngwaf(
            &json!({"data": [{
                "timestamp": "2026-11-11T00:00:00Z",
                "ngwaf_requests_total_count": 100,
                "ngwaf_requests_allowed_count": 80,
                "ngwaf_requests_blocked_count": 10,
                "ngwaf_requests_challenged_count": 4,
                "ngwaf_requests_logged_count": 3,
                "ngwaf_requests_timeout_count": 2,
                "ngwaf_requests_unknown_count": 1
            }]}),
            1_794_355_200,
            1_794_355_200 + 3_600,
        )
        .unwrap();

        assert_eq!(body["total"], 100);
        assert_eq!(body["blocked"], 10);
        assert_eq!(body.get("workspace_id"), None);
    }

    #[test]
    fn platform_fetch_uses_the_requested_entity_and_granularity() {
        let calls = std::cell::RefCell::new(Vec::new());
        let body = fetch_platform_aggregate_metrics(
            "daily",
            0,
            86_400,
            &["object_storage_bytes_count"],
            |path, query| {
                calls.borrow_mut().push((path.to_owned(), query.to_owned()));
                Ok(ApiResponse {
                    status: 200,
                    body: json!({
                        "data": [{
                            "timestamp": "1970-01-01T00:00:00Z",
                            "object_storage_bytes_count": 1
                        }]
                    }),
                    retry_after: None,
                })
            },
        )
        .unwrap();

        assert_eq!(body["data"].as_array().unwrap().len(), 1);
        let calls = calls.into_inner();
        assert_eq!(calls[0].0, "/metrics/v2/platform/aggregate/daily");
        assert!(calls[0].1.contains("metric=object_storage_bytes_count"));
    }

    #[test]
    fn platform_fetch_bounds_unique_cursor_pages() {
        let calls = Cell::new(0);
        let result = fetch_platform_entity_metrics(
            "service/id",
            "hourly",
            0,
            3_600,
            &["compute_requests"],
            |_, _| {
                calls.set(calls.get() + 1);
                Ok(ApiResponse {
                    status: 200,
                    body: json!({
                        "data": [{"timestamp": "1970-01-01T00:00:00Z"}],
                        "next_cursor": format!("page-{}", calls.get())
                    }),
                    retry_after: None,
                })
            },
        );

        assert_eq!(result.unwrap_err(), "platform_metrics_duplicate_timestamp");
        assert_eq!(calls.get(), 2);
    }

    #[test]
    fn platform_fetch_rejects_a_malicious_pagination_chain_at_the_page_limit() {
        let calls = Cell::new(0);
        let result = fetch_platform_entity_metrics(
            "service/id",
            "hourly",
            0,
            18_000,
            &["compute_requests"],
            |_, _| {
                calls.set(calls.get() + 1);
                Ok(ApiResponse {
                    status: 200,
                    body: json!({
                        "data": [{
                            "timestamp": format!(
                                "1970-01-01T{:02}:00:00Z",
                                calls.get() - 1
                            )
                        }],
                        "next_cursor": format!("page-{}", calls.get())
                    }),
                    retry_after: None,
                })
            },
        );

        assert_eq!(result.unwrap_err(), "platform_metrics_page_limit");
        assert_eq!(calls.get(), 4);
    }
}
