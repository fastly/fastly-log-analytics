//! Per-request aggregation of Fastly Object Storage operations into `fos_usage` rows.

use crate::auth::settings::UsageLogging;
use serde_json::{json, Value};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
};

const MAX_SIZES: usize = 4096;

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

/// Sums one `/stats/service/<id>/field/<field>` response, returning the total and the first and
/// last bucket start times (unix s) it covered.
pub fn sum_stats_field(
    body: &Value,
    field: &str,
) -> Result<(f64, Option<u64>, Option<u64>), String> {
    let (total, first, last, _) = stats_field_buckets(body, field)?;
    Ok((total, first, last))
}

pub type StatsBuckets = (f64, Option<u64>, Option<u64>, Vec<Value>);

/// Sums one `/stats/service/<id>/field/<field>` response and returns `(total, first, last, buckets)`
/// where each bucket has `{"start_time": u64, "<field>": f64}`.
pub fn stats_field_buckets(body: &Value, field: &str) -> Result<StatsBuckets, String> {
    if body["status"] != "success" {
        return Err("stats_api_failed".into());
    }
    let rows = body["data"].as_array().ok_or("stats_api_shape")?;
    let mut total = 0.0;
    let (mut first, mut last) = (None::<u64>, None::<u64>);
    let mut buckets = Vec::with_capacity(rows.len());
    for row in rows {
        let value = row[field].as_f64().unwrap_or(0.0);
        total += value;
        if let Some(start) = row["start_time"].as_u64() {
            first = Some(first.map_or(start, |f| f.min(start)));
            last = Some(last.map_or(start, |l| l.max(start)));
            buckets.push(json!({"start_time": start, field: value}));
        }
    }
    Ok((total, first, last, buckets))
}

#[cfg(test)]
mod billing_tests {
    use super::{daily_totals, sum_stats_field, UsageAgg};
    use crate::auth::settings::UsageLogging;
    use serde_json::json;

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
        let body = json!({"status": "success", "data": [
            {"compute_requests": 35, "start_time": 1_793_469_600u64},
            {"compute_requests": 30, "start_time": 1_793_473_200u64},
            {"start_time": 1_793_476_800u64},
        ]});
        assert_eq!(
            sum_stats_field(&body, "compute_requests").unwrap(),
            (65.0, Some(1_793_469_600), Some(1_793_476_800))
        );
        let ms = json!({"status": "success", "data": [{"compute_execution_time_ms": 1.25, "start_time": 1}]});
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
    fn rejects_a_failed_stats_response() {
        let body = json!({"status": "error", "msg": "bad", "data": []});
        assert!(sum_stats_field(&body, "compute_requests").is_err());
        assert!(sum_stats_field(&json!({"status": "success"}), "compute_requests").is_err());
    }
}
