use super::*;
use crate::{
    admin::Record,
    setup::SetupStores,
    shell::{CoreCacheEntry, CoreCacheError, KvPage, Runtime},
};
use arrow_array::{
    Array, BooleanArray, Float64Array, Int64Array, StringArray, TimestampMillisecondArray,
};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{json, Value};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
};

const NOW_S: u64 = 1_700_000_100;
const NOW_MIN: u64 = NOW_S / 60;

#[derive(Default)]
struct Rt {
    cache: RefCell<BTreeMap<String, (CoreCacheEntry, Vec<String>)>>,
    kv: RefCell<BTreeMap<String, (u64, Value)>>,
    shield: bool,
    fail_replace_containing: RefCell<Option<String>>,
    replaces: RefCell<Vec<String>>,
    counter: Cell<u32>,
    gets: Cell<u32>,
    fail_cursor_read: Cell<bool>,
    cursor_reads: Cell<u32>,
    api_log: api::ApiLog,
    /// Updates that lose their CAS to a concurrent writer appending its own line first.
    races: Cell<u32>,
    /// Models a budget whose remaining count does not move as calls are made (a separate
    /// telemetry allowance): `admin_remaining_calls` reports this, and CASes are counted.
    frozen_remaining: Cell<Option<u32>>,
    cas_calls: Cell<u32>,
    no_dev_telemetry: bool,
}

impl Rt {
    fn on_shield() -> Self {
        Self {
            shield: true,
            ..Default::default()
        }
    }
    fn bucket_rows(&self, kind: &str, minute: u64, shard: u32) -> Vec<Value> {
        let key = bucket_key(LogKind::parse(kind).unwrap(), "global", minute, shard);
        self.cache
            .borrow()
            .get(&key)
            .map(|((body, _), _)| {
                String::from_utf8_lossy(body)
                    .lines()
                    .map(|l| serde_json::from_str(l).unwrap())
                    .collect()
            })
            .unwrap_or_default()
    }
    fn meta(&self, kind: &str, minute: u64, shard: u32) -> Value {
        let key = bucket_key(LogKind::parse(kind).unwrap(), "global", minute, shard);
        let cache = self.cache.borrow();
        serde_json::from_slice(&cache.get(&key).unwrap().0 .1).unwrap()
    }
    fn put_bucket(&self, kind: LogKind, sid: &str, minute: u64, shard: u32, rows: &[Value]) {
        let body: String = rows.iter().map(|r| format!("{r}\n")).collect();
        self.cache.borrow_mut().insert(
            bucket_key(kind, sid, minute, shard),
            ((body.into_bytes(), b"{}".to_vec()), vec![]),
        );
    }
    fn put_kv(&self, key: &str, value: Value) {
        self.kv.borrow_mut().insert(key.into(), (1, value));
    }
}

impl SetupStores for Rt {
    fn config_present(&self, _: &str) -> Result<bool, String> {
        Ok(true)
    }
    fn secret_present(&self, _: &str) -> Result<bool, String> {
        Ok(true)
    }
    fn state_linked(&self) -> Result<bool, String> {
        Ok(true)
    }
}

impl Runtime for Rt {
    fn config(&self, name: &str) -> Result<Option<String>, String> {
        Ok((name == "dev_telemetry").then(|| if self.no_dev_telemetry { "0" } else { "1" }.into()))
    }
    fn secret(&self, _: &str) -> Result<Option<String>, String> {
        Ok(None)
    }
    fn random(&self) -> Result<[u8; 32], String> {
        let next = self.counter.get() + 1;
        self.counter.set(next);
        let mut bytes = [0; 32];
        bytes[..4].copy_from_slice(&next.to_be_bytes());
        Ok(bytes)
    }
    fn now(&self) -> u64 {
        NOW_S
    }
    fn checks(&self, _: bool) -> Result<Value, String> {
        unreachable!()
    }
    fn versions(&self) -> Result<Value, String> {
        unreachable!()
    }
    fn compute_service_id(&self) -> Option<String> {
        None
    }
    fn compute_service_version(&self) -> Option<String> {
        None
    }
    fn admin_remaining_calls(&self) -> u32 {
        self.frozen_remaining
            .get()
            .unwrap_or_else(|| 32u32.saturating_sub(self.gets.get()))
    }
    fn admin_get(&self, key: &str) -> Result<Option<Record>, String> {
        self.gets.set(self.gets.get() + 1);
        if key.starts_with("logs:cursor:") {
            self.cursor_reads.set(self.cursor_reads.get() + 1);
            if self.fail_cursor_read.get() {
                return Err("cursor_unavailable".into());
            }
        }
        Ok(self.kv.borrow().get(key).map(|(g, v)| Record {
            generation: g.to_string(),
            value: v.clone(),
        }))
    }
    fn admin_cas(
        &self,
        key: &str,
        generation: Option<&str>,
        value: &Value,
    ) -> Result<bool, String> {
        self.cas_calls.set(self.cas_calls.get() + 1);
        let mut kv = self.kv.borrow_mut();
        if generation.is_some() && self.races.get() > 0 {
            self.races.set(self.races.get() - 1);
            if let Some((g, v)) = kv.get(key).cloned() {
                let mut v = v;
                let line = format!("{}\n{{\"racer\":{g}}}", v["ndjson"].as_str().unwrap_or(""));
                v["ndjson"] = line.into();
                kv.insert(key.into(), (g + 1, v));
                return Ok(false);
            }
        }
        match (kv.get(key), generation) {
            (None, None) => {
                kv.insert(key.into(), (1, value.clone()));
                Ok(true)
            }
            (Some((g, _)), Some(expected)) if g.to_string() == expected => {
                let next = g + 1;
                kv.insert(key.into(), (next, value.clone()));
                Ok(true)
            }
            _ => Ok(false),
        }
    }
    fn core_cache_replace(
        &self,
        key: &str,
        surrogate_keys: &[&str],
        _ttl: std::time::Duration,
        update: &dyn Fn(Option<CoreCacheEntry>) -> CoreCacheEntry,
    ) -> Result<(), CoreCacheError> {
        self.replaces.borrow_mut().push(key.into());
        if let Some(part) = &*self.fail_replace_containing.borrow() {
            if key.contains(part.as_str()) {
                return Err(CoreCacheError::ReplaceFailed);
            }
        }
        let mut cache = self.cache.borrow_mut();
        let existing = cache.get(key).map(|(entry, _)| entry.clone());
        let keys = surrogate_keys.iter().map(|k| k.to_string()).collect();
        cache.insert(key.into(), (update(existing), keys));
        Ok(())
    }
    fn core_cache_lookup(&self, key: &str) -> Result<Option<CoreCacheEntry>, CoreCacheError> {
        Ok(self.cache.borrow().get(key).map(|(entry, _)| entry.clone()))
    }
    fn kv_list(&self, prefix: &str, cursor: Option<&str>, limit: u32) -> Result<KvPage, String> {
        if self.admin_remaining_calls() == 0 {
            return Err("backend_budget_exhausted".into());
        }
        self.gets.set(self.gets.get() + 1);
        let kv = self.kv.borrow();
        let mut keys: Vec<String> = kv
            .keys()
            .filter(|k| k.starts_with(prefix) && cursor.is_none_or(|c| k.as_str() > c))
            .take(limit as usize + 1)
            .cloned()
            .collect();
        let more = keys.len() > limit as usize;
        keys.truncate(limit as usize);
        let next_cursor = more.then(|| keys.last().cloned()).flatten();
        Ok(KvPage { keys, next_cursor })
    }
    fn on_log_shield(&self) -> bool {
        self.shield
    }
    fn api_log(&self) -> Option<&api::ApiLog> {
        Some(&self.api_log)
    }
    fn pop(&self) -> Option<String> {
        Some("IAD".into())
    }
}

fn outbox_keys(rt: &Rt) -> Vec<String> {
    rt.kv
        .borrow()
        .keys()
        .filter(|k| k.starts_with("logq_global_"))
        .cloned()
        .collect()
}

#[test]
fn fnv1a_matches_reference_vectors() {
    assert_eq!(fnv1a(""), 0x811c_9dc5);
    assert_eq!(fnv1a("a"), 0xe40c_292c);
    assert_eq!(shard_of("a"), 0xe40c_292c % 4);
}

#[test]
fn write_log_routes_by_tier() {
    let rt = Rt::on_shield();
    let buffer = LogBuffer::new("global", "req-1");
    for kind in [
        WritableLogKind::FosUsage,
        WritableLogKind::ApiTelemetry,
        WritableLogKind::PlatformErrors,
        WritableLogKind::IngestFiles,
        WritableLogKind::CronRuns,
    ] {
        write_log(&rt, &buffer, kind, json!({"message": "m"}));
    }
    let outcome = flush_request_buffer(&rt, &buffer);
    assert_eq!(outcome.dropped, 0);
    let shard = shard_of("req-1");
    for kind in ["fos_usage", "api_telemetry", "platform_errors"] {
        assert_eq!(rt.bucket_rows(kind, NOW_MIN, shard).len(), 1, "{kind}");
    }
    for kind in ["ingest_files", "cron_runs", "cron_ticks"] {
        assert!(rt.bucket_rows(kind, NOW_MIN, shard).is_empty());
    }
    assert_eq!(WritableLogKind::parse("cron_ticks"), None, "retired");
    let keys = outbox_keys(&rt);
    assert_eq!(keys.len(), 1);
    let ndjson = rt.kv.borrow()[&keys[0]].1["ndjson"]
        .as_str()
        .unwrap()
        .to_string();
    let kinds: Vec<String> = ndjson
        .lines()
        .map(|l| {
            serde_json::from_str::<Value>(l).unwrap()["kind"]
                .as_str()
                .unwrap()
                .into()
        })
        .collect();
    assert_eq!(kinds, ["ingest_files", "cron_runs"]);
    assert_eq!(LogKind::from(WritableLogKind::CronRuns), LogKind::CronRuns);
    assert_eq!(LogKind::parse("admin_audit"), Some(LogKind::AdminAudit));
}

#[test]
fn flush_replaces_once_per_touched_kind() {
    let rt = Rt::on_shield();
    let buffer = LogBuffer::new("global", "req-1");
    for _ in 0..3 {
        write_log(&rt, &buffer, WritableLogKind::ApiTelemetry, json!({}));
    }
    for _ in 0..2 {
        write_log(&rt, &buffer, WritableLogKind::FosUsage, json!({}));
    }
    flush_request_buffer(&rt, &buffer);
    assert_eq!(rt.replaces.borrow().len(), 2);
    assert_eq!(
        rt.meta("api_telemetry", NOW_MIN, shard_of("req-1"))["count"],
        3
    );
}

#[test]
fn replace_failure_is_counted_and_queued_as_platform_error() {
    let rt = Rt::on_shield();
    *rt.fail_replace_containing.borrow_mut() = Some("fos_usage".into());
    let buffer = LogBuffer::new("global", "req-1");
    write_log(&rt, &buffer, WritableLogKind::ApiTelemetry, json!({}));
    write_log(&rt, &buffer, WritableLogKind::FosUsage, json!({}));
    let outcome = flush_request_buffer(&rt, &buffer);
    assert_eq!(outcome.dropped, 1);
    assert_eq!(
        outcome.header(),
        Some(("x-flp-log-dropped", "1".to_string()))
    );
    let errors = rt.bucket_rows("platform_errors", NOW_MIN, shard_of("req-1"));
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0]["code"], "core_cache_replace_failed");
    assert_eq!(errors[0]["error_domain"], "core_cache");
}

#[test]
fn capacity_telemetry_missing_or_malformed_metrics_are_not_zero_resource_successes() {
    use super::execution::WorkerExecutionMetrics;
    for reply in [
        json!({"status": "ok"}),
        json!({"status": "error", "calls": -1, "vcpu_ms": "0", "heap_mib": u64::MAX}),
        json!({"status": "ok", "calls": 9, "vcpu_ms": 8, "execution_metrics": {}}),
    ] {
        let sample = serde_json::to_value(WorkerExecutionMetrics::from_reply(
            "request_ingest",
            Some("0"),
            Some(&reply),
        ))
        .unwrap();
        assert_eq!(sample["answered"], true);
        for field in [
            "calls",
            "vcpu_ms",
            "duration_ms",
            "heap_mib",
            "summary_vcpu_ms",
        ] {
            assert!(sample[field].is_null(), "{sample}");
        }
    }
}

#[test]
fn capacity_telemetry_cron_details_survive_outbox_and_live_api_without_truncation() {
    let rt = Rt::on_shield();
    let buffer = LogBuffer::new("global", "req-1");
    let samples: Vec<Value> = (0..138)
        .map(|shard| {
            json!({
                "kind": "request_ingest", "shard": shard.to_string(), "answered": true,
                "status": "ok", "calls": 31, "vcpu_ms": shard + 1,
                "summary_vcpu_ms": shard, "duration_ms": 1000, "heap_mib": null,
            })
        })
        .collect();
    let details = json!({"worker_executions_metrics": samples});
    write_log(
        &rt,
        &buffer,
        WritableLogKind::CronRuns,
        json!({
            "cron_kind": "request_ingest", "details_json": details.to_string(),
        }),
    );
    flush_request_buffer(&rt, &buffer);
    let result = read_live(&rt, "global", LogKind::CronRuns, None, None, None).unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        serde_json::from_str::<Value>(result.rows[0]["details_json"].as_str().unwrap()).unwrap(),
        details
    );
}

#[test]
fn bounds_drop_and_truncate() {
    let rt = Rt::on_shield();
    let buffer = LogBuffer::new("global", "req-1");
    for _ in 0..257 {
        write_log(&rt, &buffer, WritableLogKind::ApiTelemetry, json!({}));
    }
    write_log(
        &rt,
        &buffer,
        WritableLogKind::FosUsage,
        json!({"sample_key": "k".repeat(10_000)}),
    );
    let outcome = flush_request_buffer(&rt, &buffer);
    assert_eq!(outcome.dropped, 1);
    assert_eq!(
        rt.bucket_rows("api_telemetry", NOW_MIN, shard_of("req-1"))
            .len(),
        256
    );
    let errors = rt.bucket_rows("platform_errors", NOW_MIN, shard_of("req-1"));
    assert_eq!(errors[0]["code"], "log_buffer_full");
    let fos = rt.bucket_rows("fos_usage", NOW_MIN, shard_of("req-1"));
    assert!(serde_json::to_string(&fos[0]).unwrap().len() <= MAX_ROW_BYTES);

    let rt = Rt::on_shield();
    let big = vec![json!({"message": "x".repeat(4000)}); 262];
    rt.put_bucket(
        LogKind::ApiTelemetry,
        "global",
        NOW_MIN,
        shard_of("req-2"),
        &big,
    );
    let before = rt
        .bucket_rows("api_telemetry", NOW_MIN, shard_of("req-2"))
        .len();
    let buffer = LogBuffer::new("global", "req-2");
    write_log(
        &rt,
        &buffer,
        WritableLogKind::ApiTelemetry,
        json!({"message": "y".repeat(3000)}),
    );
    let outcome = flush_request_buffer(&rt, &buffer);
    assert_eq!(outcome.dropped, 1);
    assert_eq!(
        rt.bucket_rows("api_telemetry", NOW_MIN, shard_of("req-2"))
            .len(),
        before
    );
    assert_eq!(
        rt.meta("api_telemetry", NOW_MIN, shard_of("req-2"))["dropped"],
        1
    );
}

#[test]
fn off_shield_drops_best_effort_but_keeps_durable() {
    let rt = Rt::default();
    let buffer = LogBuffer::new("global", "req-1");
    write_log(&rt, &buffer, WritableLogKind::ApiTelemetry, json!({}));
    write_log(&rt, &buffer, WritableLogKind::IngestFiles, json!({}));
    let outcome = flush_request_buffer(&rt, &buffer);
    assert_eq!(outcome.dropped, 1);
    assert!(rt.cache.borrow().is_empty());
    assert_eq!(outbox_keys(&rt).len(), 1);
}

#[test]
fn read_live_shows_legacy_per_request_outbox_keys() {
    let rt = Rt::on_shield();
    let row = json!({"kind":"cron_runs","timestamp":1_700_000_000_000i64,"event_id":"legacy","service_id":"global","severity":"info"});
    rt.put_kv(
        "logq:global:1700000000000-legacy",
        json!({"ndjson": row.to_string()}),
    );
    let page = read_live(&rt, "global", LogKind::CronRuns, None, None, None).unwrap();
    assert_eq!(page.rows.len(), 1);
    assert_eq!(page.rows[0]["event_id"], "legacy");
}

#[test]
fn read_live_takes_since_in_seconds_as_well_as_milliseconds() {
    let rt = Rt::on_shield();
    let rows: String = [("old", 1_700_000_000_000i64), ("new", 1_700_000_900_000)]
        .into_iter()
        .map(|(id, timestamp)| {
            format!(
                "{}\n",
                json!({"kind":"cron_runs","timestamp":timestamp,"event_id":id,"service_id":"global"})
            )
        })
        .collect();
    rt.put_kv("logq:global:1700000000000-mixed", json!({"ndjson": rows}));
    for since in [1_700_000_600i64, 1_700_000_600_000] {
        let page = read_live(&rt, "global", LogKind::CronRuns, Some(since), None, None).unwrap();
        let ids = page
            .rows
            .iter()
            .map(|row| row["event_id"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(ids, ["new"], "since={since}");
    }
}

#[test]
fn read_live_source_pages_cover_old_and_legacy_sources_without_timestamp_pruning() {
    let rt = Rt::on_shield();
    let since = (NOW_S * 1000) as i64;
    for n in 0..75 {
        let key = match n % 3 {
            0 => format!("logq:global:{n:013}-legacy"),
            1 => format!("logq_global_{n:010}_1"),
            _ => format!("logq_global_{n:010}_overflow"),
        };
        let rows: String = [since + 100 - n, since - 1, since + n]
            .into_iter()
            .enumerate()
            .map(|(i, timestamp)| {
                format!(
                    "{}\n",
                    json!({"kind":"cron_runs","timestamp":timestamp,"event_id":format!("{n}-{i}")})
                )
            })
            .collect();
        rt.put_kv(&key, json!({"ndjson":rows}));
    }
    rt.put_kv("logq_other_1", json!({"ndjson":"not our service"}));
    for limit in [3, MAX_PAGE] {
        let mut cursor = Some("source-v1".to_string());
        let mut cursors = BTreeSet::new();
        let mut ids = BTreeSet::new();
        let mut pages = 0;
        while let Some(current) = cursor {
            rt.gets.set(0);
            let page = read_live(
                &rt,
                "global",
                LogKind::CronRuns,
                Some(since),
                Some(limit),
                Some(&current),
            )
            .unwrap();
            assert!(!page.truncated);
            assert!(rt.gets.get() <= 28);
            assert!(page.rows.len() <= limit);
            for row in page.rows {
                assert!(ids.insert(row["event_id"].as_str().unwrap().to_string()));
            }
            cursor = page.next_cursor;
            if let Some(next) = &cursor {
                assert!(cursors.insert(next.clone()), "source cursor repeated");
            }
            pages += 1;
            assert!(pages <= 100);
        }
        assert_eq!(ids.len(), 150);
        assert!(pages > 1);
    }
}

#[test]
fn read_live_source_page_lists_sources_in_batches_so_reads_get_the_call_budget() {
    let rt = Rt::on_shield();
    let since = (NOW_S * 1000) as i64;
    for n in 0..24 {
        rt.put_kv(
            &format!("logq_global_{n:010}"),
            json!({"ndjson": format!("{}\n", json!({"kind":"cron_runs","timestamp":since + n,"event_id":format!("{n}")}))}),
        );
    }
    rt.gets.set(0);
    let page = read_live(
        &rt,
        "global",
        LogKind::CronRuns,
        Some(since),
        Some(MAX_PAGE),
        Some("source-v1"),
    )
    .unwrap();
    assert_eq!(page.rows.len(), 24, "{:?}", page.next_cursor);
    assert!(page.next_cursor.is_none());
    // One list call plus one read per source; listing one key at a time cost 48.
    assert_eq!(rt.gets.get(), 25);
}

#[test]
fn read_live_source_cursor_binds_query_and_rejects_changed_partial_source() {
    let rt = Rt::on_shield();
    let key = "logq_global_0000000001";
    let body = format!(
        "{}\n{}\n",
        json!({"kind":"cron_runs","timestamp":3,"event_id":"new"}),
        json!({"kind":"cron_runs","timestamp":1,"event_id":"old"})
    );
    rt.put_kv(key, json!({"ndjson":body}));
    let first = read_live(
        &rt,
        "global",
        LogKind::CronRuns,
        Some(0),
        Some(1),
        Some("source-v1"),
    )
    .unwrap();
    let cursor = first.next_cursor.unwrap();
    assert!(read_live(
        &rt,
        "other",
        LogKind::CronRuns,
        Some(0),
        Some(1),
        Some(&cursor)
    )
    .is_err());
    assert!(read_live(
        &rt,
        "global",
        LogKind::CronRuns,
        Some(1),
        Some(1),
        Some(&cursor)
    )
    .is_err());
    assert!(read_live(
        &rt,
        "global",
        LogKind::IngestFiles,
        Some(0),
        Some(1),
        Some(&cursor)
    )
    .is_err());
    assert!(read_live(
        &rt,
        "global",
        LogKind::CronRuns,
        Some(0),
        Some(1),
        Some("source-v1:invalid")
    )
    .is_err());
    rt.put_kv(key, json!({"ndjson":format!("{body}{}\n", json!({"kind":"cron_runs","timestamp":2,"event_id":"append"}))}));
    assert!(read_live(
        &rt,
        "global",
        LogKind::CronRuns,
        Some(0),
        Some(1),
        Some(&cursor)
    )
    .is_err());
    rt.kv.borrow_mut().remove(key);
    assert!(read_live(
        &rt,
        "global",
        LogKind::CronRuns,
        Some(0),
        Some(1),
        Some(&cursor)
    )
    .is_err());
}

#[test]
fn read_live_source_pages_fail_closed_on_malformed_rows_and_exhausted_budget() {
    let rt = Rt::on_shield();
    for body in [
        "not json".to_string(),
        json!({"kind":"cron_runs","event_id":"missing-time"}).to_string(),
    ] {
        rt.put_kv("logq_global_0000000001", json!({"ndjson":body}));
        assert!(read_live(
            &rt,
            "global",
            LogKind::CronRuns,
            Some(1),
            None,
            Some("source-v1")
        )
        .is_err());
    }
    rt.gets.set(32);
    assert!(read_live(
        &rt,
        "global",
        LogKind::CronRuns,
        None,
        None,
        Some("source-v1")
    )
    .is_err());
    let empty = Rt::on_shield();
    assert_eq!(
        read_live(
            &empty,
            "global",
            LogKind::CronRuns,
            None,
            None,
            Some("source-v1")
        )
        .unwrap(),
        LivePage::default()
    );
}

#[test]
fn read_live_source_row_scan_limit_resumes_after_an_empty_page() {
    let rt = Rt::on_shield();
    let mut body = String::new();
    for n in 0..MAX_PAGE {
        body.push_str(&format!(
            "{}\n",
            json!({"kind":"cron_runs","timestamp":0,"event_id":format!("old-{n}")})
        ));
    }
    body.push_str(&json!({"kind":"cron_runs","timestamp":10,"event_id":"recent"}).to_string());
    rt.put_kv("logq_global_0000000001", json!({"ndjson":body}));
    let first = read_live(
        &rt,
        "global",
        LogKind::CronRuns,
        Some(1),
        None,
        Some("source-v1"),
    )
    .unwrap();
    assert!(first.rows.is_empty());
    assert!(!first.truncated);
    let second = read_live(
        &rt,
        "global",
        LogKind::CronRuns,
        Some(1),
        None,
        first.next_cursor.as_deref(),
    )
    .unwrap();
    assert_eq!(second.rows.len(), 1);
    assert_eq!(second.rows[0]["event_id"], "recent");
    assert!(!second.truncated);
    assert_eq!(second.next_cursor, None);
}

#[test]
fn read_live_outbox_does_not_depend_on_the_flush_cursor() {
    let rt = Rt::on_shield();
    rt.fail_cursor_read.set(true);
    rt.put_kv(
        "logq:global:1700000000000-legacy",
        json!({"ndjson": json!({"kind":"cron_runs","timestamp":1_700_000_000_000i64,"event_id":"legacy"}).to_string()}),
    );
    let page = read_live(&rt, "global", LogKind::CronRuns, None, None, None).unwrap();
    assert_eq!(page.rows[0]["event_id"], "legacy");
    assert_eq!(rt.gets.get(), 2);
    assert_eq!(rt.cursor_reads.get(), 0);
    for kind in [LogKind::ApiTelemetry, LogKind::AdminAudit] {
        assert_eq!(
            read_live(&rt, "global", kind, None, None, None).unwrap_err(),
            "cursor_unavailable"
        );
    }
    assert_eq!(rt.cursor_reads.get(), 2);
}

#[test]
fn event_identity_is_stable() {
    let rt = Rt::on_shield();
    let buffer = LogBuffer::new("global", "req-1");
    let id = write_log(
        &rt,
        &buffer,
        WritableLogKind::CronRuns,
        json!({"run_id": "r"}),
    );
    flush_request_buffer(&rt, &buffer);
    let first = read_live(&rt, "global", LogKind::CronRuns, None, None, None).unwrap();
    let second = read_live(&rt, "global", LogKind::CronRuns, None, None, None).unwrap();
    assert_eq!(first.rows.len(), 1);
    assert_eq!(first.rows[0]["event_id"], id.as_str());
    assert_eq!(first.rows, second.rows);
    assert_eq!(
        audit_event_id("auth_archive", 7),
        audit_event_id("auth_archive", 7)
    );
    assert_ne!(
        audit_event_id("auth_archive", 7),
        audit_event_id("svc_outbox", 7)
    );
}

#[test]
fn read_live_scans_pending_and_open_minutes_and_paginates() {
    let rt = Rt::on_shield();
    let ts = |n: u64| NOW_S * 1000 + n;
    let row = |n: u64| json!({"timestamp": ts(n), "event_id": format!("e{n}")});
    rt.put_bucket(LogKind::ApiTelemetry, "global", NOW_MIN - 10, 1, &[row(1)]);
    rt.put_bucket(LogKind::ApiTelemetry, "global", NOW_MIN - 20, 1, &[row(9)]);
    rt.put_bucket(
        LogKind::ApiTelemetry,
        "global",
        NOW_MIN - 1,
        2,
        &[row(2), row(3)],
    );
    rt.put_bucket(
        LogKind::ApiTelemetry,
        "global",
        NOW_MIN,
        0,
        &[row(4), row(5)],
    );
    rt.put_kv(
        "logs:cursor:global",
        json!({"scan_min": NOW_MIN - 2, "pending": {"nonempty": [["api_telemetry", NOW_MIN - 10, 1, 1]]}}),
    );
    let mut seen = Vec::new();
    let mut cursor = None;
    loop {
        let page = read_live(
            &rt,
            "global",
            LogKind::ApiTelemetry,
            None,
            Some(2),
            cursor.as_deref(),
        )
        .unwrap();
        seen.extend(
            page.rows
                .iter()
                .map(|r| r["event_id"].as_str().unwrap().to_string()),
        );
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    assert_eq!(seen, ["e5", "e4", "e3", "e2", "e1"]);
    assert_eq!(page_limit(None), 500);
    assert_eq!(page_limit(Some(5000)), 2000);
}

#[test]
fn read_live_caps_the_closed_scan_at_max_scan_min() {
    let rt = Rt::on_shield();
    let row = |n: u64| json!({"timestamp": n, "event_id": format!("e{n}")});
    for (minute, id) in [
        (NOW_MIN - MAX_SCAN_MIN - 2, 1),
        (NOW_MIN - MAX_SCAN_MIN - 1, 2),
        (NOW_MIN - 2, 3),
        (NOW_MIN - 1, 4),
        (NOW_MIN, 5),
    ] {
        rt.put_bucket(LogKind::ApiTelemetry, "global", minute, 0, &[row(id)]);
    }
    rt.put_kv("logs:cursor:global", json!({"scan_min": NOW_MIN - 100}));
    let page = read_live(&rt, "global", LogKind::ApiTelemetry, None, None, None).unwrap();
    let ids: Vec<&str> = page
        .rows
        .iter()
        .map(|r| r["event_id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["e5", "e4", "e3", "e2"]);
    assert_eq!(MAX_SCAN_MIN, 30);
}

#[test]
fn read_live_audit_has_only_unarchived_auth_rows_not_service_outbox() {
    let rt = Rt::on_shield();
    rt.put_kv(
        "logs:cursor:svc1",
        json!({"scan_min": NOW_MIN - 2, "audit_seq": 1, "svc_outbox_archived": 1}),
    );
    rt.put_kv(
        &format!("audit:{:020}", 1),
        json!({"actor":"a","action":"old","target":"t","time":NOW_S,"ip":"1.1.1.1"}),
    );
    rt.put_kv(
        &format!("audit:{:020}", 2),
        json!({"actor":"b","action":"login","target":"t","time":NOW_S,"ip":"2.2.2.2"}),
    );
    rt.put_kv(
        "svc:svc1",
        json!({"_audit_outbox": [
            {"actor":"x","at":NOW_S,"event":"e0"},
            {"actor":"y","at":NOW_S+1,"event":"e1","job_id":"j"}
        ]}),
    );
    let page = read_live(&rt, "svc1", LogKind::AdminAudit, None, None, None).unwrap();
    let mut got: Vec<(String, i64)> = page
        .rows
        .iter()
        .map(|r| {
            (
                r["source"].as_str().unwrap().into(),
                r["source_seq"].as_i64().unwrap(),
            )
        })
        .collect();
    got.sort();
    assert_eq!(got, [("auth_archive".to_string(), 2)]);
    assert!(page.rows.iter().all(|r| r["event_id"].as_str().is_some()));

    let buffer = LogBuffer::new("svc1", "req-1");
    write_log(
        &rt,
        &buffer,
        WritableLogKind::IngestFiles,
        json!({"raw_key": "k"}),
    );
    flush_request_buffer(&rt, &buffer);
    let page = read_live(&rt, "svc1", LogKind::IngestFiles, None, None, None).unwrap();
    assert_eq!(page.rows.len(), 1);
    assert!(page.rows[0].get("kind").is_none());
}

#[test]
fn read_live_reads_the_outbox_and_audit_within_the_backend_budget() {
    let rt = Rt::on_shield();
    for i in 0..40 {
        let line = json!({"kind": "ingest_files", "timestamp": i, "event_id": format!("e{i:02}")});
        rt.put_kv(
            &outbox_key("svc1", NOW_S / 120 - 2, Some(&format!("{i:03}"))),
            json!({"ndjson": line.to_string()}),
        );
        rt.put_kv(
            &format!("audit:{i:020}"),
            json!({"actor":"a","action":"x","target":"t","time":NOW_S,"ip":"1.1.1.1"}),
        );
    }
    let page = read_live(&rt, "svc1", LogKind::IngestFiles, None, Some(500), None).unwrap();
    assert!(page.truncated);
    assert!(rt.gets.get() <= 30);
    assert_eq!(page.rows[0]["event_id"], "e39");
    rt.gets.set(0);
    let page = read_live(&rt, "svc1", LogKind::AdminAudit, None, Some(500), None).unwrap();
    assert!(page.truncated);
    assert!(rt.gets.get() <= 30);
    assert!(page.rows.len() < 40);
    assert!(page.rows.iter().any(|r| r["source_seq"] == 39));
    assert!(page.rows.iter().all(|r| r["source_seq"] != 0));
}

#[test]
fn read_live_degrades_to_truncated_instead_of_failing_when_the_budget_is_spent() {
    let rt = Rt::on_shield();
    rt.put_kv(
        "audit:00000000000000000001",
        json!({"actor":"a","action":"x","target":"t","time":NOW_S,"ip":"1.1.1.1"}),
    );
    rt.put_kv("svc:svc1", json!({"_audit_outbox": []}));
    for spent in [31, 32] {
        rt.gets.set(spent);
        for kind in [LogKind::AdminAudit, LogKind::IngestFiles] {
            let page = read_live(&rt, "svc1", kind, None, None, None).unwrap();
            assert!(page.truncated, "{spent} {kind:?}");
        }
    }
}

fn sample(column: &crate::ingest::convert::Column) -> Value {
    use crate::ingest::convert::Kind;
    match column.kind {
        Kind::Utf8 => json!(format!("v-{}", column.id)),
        Kind::Int64 | Kind::TimestampMs => json!(1_700_000_000_123_i64),
        Kind::Float64 => json!(1.5),
        Kind::Bool => json!(true),
    }
}

fn parse_back(bytes: Vec<u8>) -> arrow_array::RecordBatch {
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
        .unwrap()
        .build()
        .unwrap();
    reader.next().unwrap().unwrap()
}

#[test]
fn every_kind_round_trips_every_column() {
    use crate::ingest::convert::Kind;
    for kind in LogKind::ALL {
        let columns = schema::columns(kind);
        let row: Value = Value::Object(
            columns
                .iter()
                .map(|c| (c.id.to_string(), sample(c)))
                .chain([("unknown_field".to_string(), json!("ignored"))])
                .collect(),
        );
        let batch = parse_back(schema::to_parquet(kind, &[row]).unwrap());
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), columns.len(), "{kind:?}");
        for (index, column) in columns.iter().enumerate() {
            let array = batch.column(index);
            assert_eq!(batch.schema().field(index).name(), column.id);
            match column.kind {
                Kind::Utf8 => assert_eq!(
                    array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(0),
                    format!("v-{}", column.id)
                ),
                Kind::Int64 => assert_eq!(
                    array
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                    1_700_000_000_123
                ),
                Kind::TimestampMs => assert_eq!(
                    array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap()
                        .value(0),
                    1_700_000_000_123
                ),
                Kind::Float64 => assert_eq!(
                    array
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .value(0),
                    1.5
                ),
                Kind::Bool => assert!(array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .value(0)),
            }
        }
    }
}

#[test]
fn strict_conversion_nulls_mismatches() {
    let batch = parse_back(
        schema::to_parquet(
            LogKind::IngestFiles,
            &[json!({"rows": "12", "bad_lines": 3, "raw_key": 5, "raw_deleted": "true", "state_version": 1.5})],
        )
        .unwrap(),
    );
    let names: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let col = |name: &str| batch.column(names.iter().position(|n| n == name).unwrap());
    assert!(col("rows").is_null(0));
    assert!(col("raw_key").is_null(0));
    assert!(col("raw_deleted").is_null(0));
    assert!(col("state_version").is_null(0));
    assert_eq!(
        col("bad_lines")
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        3
    );
}

#[test]
fn latest_ingest_state_prefers_highest_version_then_time() {
    let rows = vec![
        json!({"raw_key":"a","state_version":2,"timestamp":1,"event_id":"x"}),
        json!({"raw_key":"a","state_version":1,"timestamp":9,"event_id":"y"}),
        json!({"raw_key":"b","state_version":1,"timestamp":1,"event_id":"a"}),
        json!({"raw_key":"b","state_version":1,"timestamp":1,"event_id":"b"}),
    ];
    let latest = latest_ingest_state(&rows);
    assert_eq!(latest.len(), 2);
    assert_eq!(latest[0]["event_id"], "x");
    assert_eq!(latest[1]["event_id"], "b");
}

#[test]
fn redaction_is_recursive_and_precedes_truncation() {
    let secret = "SUPERSECRET";
    let value = json!({
        "Authorization": "Bearer SUPERSECRET",
        "headers": {"Set-Cookie": "s=SUPERSECRET", "ok": "fine"},
        "a": {"b": {"c": {"api_token": "SUPERSECRET", "list": [{"password": "SUPERSECRET", "keep": 1}]}}},
        "url": "https://x/y?X-Amz-Signature=SUPERSECRET&X-Amz-Credential=SUPERSECRET&a=1",
    });
    let redacted = redact(value);
    let text = redacted.to_string();
    assert!(!text.contains(secret), "{text}");
    assert_eq!(redacted["headers"]["ok"], "fine");
    assert_eq!(redacted["a"]["b"]["c"]["list"][0]["keep"], 1);
    assert!(redacted["url"].as_str().unwrap().contains("a=1"));

    let rt = Rt::on_shield();
    let buffer = LogBuffer::new("global", "req-1");
    let long = format!("{}token=SUPERSECRET{}", "p".repeat(2040), "q".repeat(500));
    write_log(
        &rt,
        &buffer,
        WritableLogKind::PlatformErrors,
        json!({"sample_json": long, "details": {"x": {"y": {"Signature": "SUPERSECRET"}}}}),
    );
    flush_request_buffer(&rt, &buffer);
    let rows = rt.bucket_rows("platform_errors", NOW_MIN, shard_of("req-1"));
    let stored = rows[0]["sample_json"].as_str().unwrap();
    assert!(stored.len() <= MAX_JSON_FIELD_BYTES);
    assert!(!stored.contains("SUPERSECRET") && !stored.contains("token=SUP"));
    assert!(!rows[0].to_string().contains(secret));
}

#[test]
fn settings_validate_ranges() {
    let defaults = OperationalLogSettings::default();
    assert!(defaults.validate().is_ok());
    assert_eq!(defaults.retention_days_by_kind["admin_audit"], 365);
    let bad = [
        OperationalLogSettings {
            flush_min_interval_s: 4000,
            ..Default::default()
        },
        OperationalLogSettings {
            flush_max_rows: 10,
            ..Default::default()
        },
        OperationalLogSettings {
            flush_min_interval_s: 600,
            flush_max_age_s: 300,
            ..Default::default()
        },
        OperationalLogSettings {
            retention_days_by_kind: BTreeMap::from([("fos_usage".to_string(), 5)]),
            ..Default::default()
        },
    ];
    for settings in bad {
        assert!(settings.validate().is_err(), "{settings:?}");
    }
}

#[test]
fn paths_are_normalized_without_query_and_with_ids_collapsed() {
    for (raw, expected) in [
        (
            "/api/admin/users/u-victim?token=abc",
            "/api/admin/users/:id",
        ),
        (
            "/api/admin/users/u-1/passkeys/CRED9/revoke",
            "/api/admin/users/:id/passkeys/:id/revoke",
        ),
        ("/api/admin/jobs/j/step/", "/api/admin/jobs/:id/step"),
        (
            "/api/admin/services/svcName/reconcile",
            "/api/admin/services/:id/reconcile",
        ),
        (
            "/service/AbCdEf0123456789xyz/version/12/backend/my-origin",
            "/service/:id/version/:id/backend/:id",
        ),
        ("/api/Zx9attacker/ABC?x=1", "/api/:unknown/:unknown"),
        ("/api/files/%2e%2e/etc", "/api/files/:unknown/:unknown"),
        ("/api/auth/session", "/api/auth/session"),
        (
            "/api/logs/api_telemetry/live",
            "/api/logs/api_telemetry/live",
        ),
    ] {
        assert_eq!(api::normalize_path(raw), expected, "{raw}");
    }
}

#[test]
fn live_reads_and_beacons_do_not_log_themselves_on_success() {
    assert!(api::is_exempt("GET", "/api/logs/platform_errors/live", 200));
    assert!(!api::is_exempt(
        "GET",
        "/api/logs/platform_errors/live",
        500
    ));
    assert!(api::is_exempt("POST", "/api/telemetry/query", 204));
    assert!(!api::is_exempt("POST", "/api/telemetry/query", 401));
    assert!(!api::is_exempt("GET", "/api/files", 200));
}

#[test]
fn only_successful_flagged_file_reads_fold_into_the_beacon() {
    assert!(api::folds_into_beacon("GET", "/api/file", 200, Some("1")));
    assert!(!api::folds_into_beacon("GET", "/api/file", 200, None));
    assert!(!api::folds_into_beacon("GET", "/api/file", 404, Some("1")));
    assert!(!api::folds_into_beacon("GET", "/api/files", 200, Some("1")));
    assert!(!api::folds_into_beacon("POST", "/api/file", 200, Some("1")));
}

#[test]
fn beacons_with_file_requests_become_one_folded_compute_row() {
    let body = json!({"compute_requests": 7, "vcpu_ms": 35.6, "page_load_id": "pl_1"});
    let row = api::folded_file_row(body.to_string().as_bytes(), None, None).unwrap();
    assert_eq!(row["source"], "compute_api");
    assert_eq!(row["method"], "GET");
    assert_eq!(row["path"], "/api/file");
    assert_eq!(row["status_code"], 200);
    assert_eq!(row["files_fetched"], 7);
    assert_eq!(row["vcpu_ms"], 35);
    assert_eq!(
        row["details_json"],
        r#"{"folded":true,"page_load_id":"pl_1"}"#
    );
    for empty in [
        json!({"compute_requests": 0, "vcpu_ms": 5}),
        json!({"files_fetched": 3}),
        json!({"compute_requests": "x"}),
    ] {
        assert!(api::folded_file_row(empty.to_string().as_bytes(), None, None).is_none());
    }
    assert!(api::folded_file_row(b"[1]", None, None).is_none());
}

#[test]
fn folded_compute_row_carries_the_harness_label() {
    let body = json!({"compute_requests": 2, "vcpu_ms": 4, "page_load_id": "pl_1"});
    let row = api::folded_file_row(body.to_string().as_bytes(), None, Some("camp-p10-s1")).unwrap();
    assert_eq!(
        row["details_json"],
        r#"{"folded":true,"harness":"camp-p10-s1","page_load_id":"pl_1"}"#
    );
}

#[test]
fn fos_usage_parquet_keeps_the_harness_label() {
    let row = json!({"op": "GET", "caller": "harness:api:file", "harness": "camp-p10-s1"});
    let rows = schema::from_parquet(
        LogKind::FosUsage,
        schema::to_parquet(LogKind::FosUsage, &[row]).unwrap(),
    )
    .unwrap();
    assert_eq!(rows[0]["harness"], "camp-p10-s1");
}

#[test]
fn fastly_api_calls_become_rows_and_failures_add_redacted_platform_errors() {
    let rt = Rt::on_shield();
    rt.api_log.record(
        "GET",
        "/service/AbCdEf0123456789xyz/version/3",
        Some(200),
        12,
        None,
        "tok-123",
    );
    rt.api_log.record(
        "PUT",
        "/service/AbCdEf0123456789xyz/backend/x?token=qq",
        Some(503),
        40,
        Some(r#"{"error":{"headers":{"Authorization":"Bearer zzz"},"url":"https://h/?X-Amz-Signature=sig1&a=b"}}"#),
        "tok-123",
    );
    rt.api_log.record(
        "POST",
        "/resources/stores/kv",
        None,
        5,
        Some("connect failed using tok-123"),
        "tok-123",
    );
    let buffer = LogBuffer::new("global", "req-1");
    api::drain_runtime_rows(&rt, &buffer);
    flush_request_buffer(&rt, &buffer);
    let shard = shard_of("req-1");
    let api_rows = rt.bucket_rows("api_telemetry", NOW_MIN, shard);
    assert_eq!(api_rows.len(), 3);
    assert!(api_rows.iter().all(|r| r["source"] == "fastly_api"));
    assert_eq!(api_rows[1]["path"], "/service/:id/backend/:id");
    assert_eq!(api_rows[1]["status_code"], 503);
    let errors = rt.bucket_rows("platform_errors", NOW_MIN, shard);
    assert_eq!(errors.len(), 2);
    assert!(errors.iter().all(|r| r["error_domain"] == "fastly_api"));
    let everything = format!("{}{}", Value::Array(api_rows), Value::Array(errors));
    for secret in ["tok-123", "zzz", "sig1", "qq", "Authorization"] {
        assert!(
            !everything.contains(secret),
            "{secret} leaked: {everything}"
        );
    }
}

#[test]
fn beacons_keep_only_allowlisted_clamped_fields() {
    let body = json!({
        "files_listed": 3, "files_fetched": 2.9, "bytes_scanned": 9e18,
        "rows_scanned": -5, "duration_ms": "x", "surface": "explorer",
        "event_id": "evil", "actor": "evil", "service_id": "evil",
        "source": "evil", "timestamp": 1, "extra": {"a": 1},
    });
    let row = api::sanitize_beacon(body.to_string().as_bytes()).unwrap();
    assert_eq!(row["files_listed"], 3);
    assert_eq!(row["files_fetched"], 2);
    assert_eq!(row["rows_scanned"], 0);
    assert_eq!(row["bytes_scanned"], api::BEACON_MAX_NUMBER);
    assert_eq!(row["path"], "/query/explorer");
    let keys: Vec<&str> = row
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    for dropped in [
        "event_id",
        "actor",
        "service_id",
        "timestamp",
        "extra",
        "duration_ms",
    ] {
        assert!(!keys.contains(&dropped), "{dropped} survived");
    }
    assert_eq!(row["source"], "browser_query");
    assert_eq!(
        api::sanitize_beacon(br#"{"surface":"bogus"}"#),
        Err(api::BeaconError::Invalid)
    );
    assert_eq!(api::sanitize_beacon(b"[1]"), Err(api::BeaconError::Invalid));
    let big = format!(
        r#"{{"files_listed":1,"pad":"{}"}}"#,
        "x".repeat(api::BEACON_MAX_BYTES)
    );
    assert_eq!(
        api::sanitize_beacon(big.as_bytes()),
        Err(api::BeaconError::TooLarge)
    );
}

#[test]
fn beacon_rate_limit_is_sixty_per_session_minute() {
    let rt = Rt::on_shield();
    for i in 0..60 {
        assert!(api::beacon_allowed(&rt, "sess-a", NOW_S), "beacon {i}");
    }
    assert!(!api::beacon_allowed(&rt, "sess-a", NOW_S + 5));
    assert!(api::beacon_allowed(&rt, "sess-b", NOW_S));
    assert!(api::beacon_allowed(&rt, "sess-a", NOW_S + 60));
}

fn outbox_lines(rt: &Rt, key: &str) -> Vec<String> {
    rt.kv.borrow()[key].1["ndjson"]
        .as_str()
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect()
}

#[test]
fn outbox_appends_batch_into_the_bucket_key_and_merge_after_lost_cas() {
    let rt = Rt::default();
    let key = outbox_key("svc1", NOW_S / OUTBOX_BUCKET_S, None);
    append_outbox(&rt, "svc1", "e1", "{\"a\":1}").unwrap();
    rt.races.set(2);
    append_outbox(&rt, "svc1", "e2", "{\"a\":2}").unwrap();
    assert_eq!(rt.kv.borrow().len(), 1, "one key per bucket");
    let lines = outbox_lines(&rt, &key);
    assert_eq!(lines.len(), 4, "{lines:?}");
    assert_eq!(lines[0], "{\"a\":1}");
    assert_eq!(lines[3], "{\"a\":2}");
}

#[test]
fn outbox_falls_back_to_an_overflow_key_after_bounded_retries() {
    let rt = Rt::default();
    append_outbox(&rt, "svc1", "e1", "{\"a\":1}").unwrap();
    rt.races.set(OUTBOX_RETRIES + 1);
    let before = rt.gets.get();
    append_outbox(&rt, "svc1", "e2", "{\"a\":2}").unwrap();
    assert!(rt.gets.get() - before <= OUTBOX_RETRIES + 1);
    let overflow = outbox_key("svc1", NOW_S / OUTBOX_BUCKET_S, Some("xe2"));
    assert_eq!(outbox_lines(&rt, &overflow), ["{\"a\":2}"]);
    let base = outbox_key("svc1", NOW_S / OUTBOX_BUCKET_S, None);
    assert!(!outbox_lines(&rt, &base).contains(&"{\"a\":2}".to_owned()));
    assert!(rt
        .kv
        .borrow()
        .keys()
        .all(|k| k.starts_with(&outbox_stem("svc1"))));
}

#[test]
fn a_full_outbox_key_rolls_over_and_a_short_budget_costs_one_call() {
    let rt = Rt::default();
    let bucket = NOW_S / OUTBOX_BUCKET_S;
    rt.put_kv(
        &outbox_key("svc1", bucket, None),
        json!({"ndjson": "x".repeat(OUTBOX_VALUE_CAP - 4)}),
    );
    append_outbox(&rt, "svc1", "e1", "{\"a\":1}").unwrap();
    assert_eq!(
        outbox_lines(&rt, &outbox_key("svc1", bucket, Some("1"))),
        ["{\"a\":1}"]
    );
    rt.gets.set(30);
    append_outbox(&rt, "svc1", "e2", "{\"a\":2}").unwrap();
    assert_eq!(rt.gets.get(), 30, "no read when only one call fits");
    assert_eq!(
        outbox_lines(&rt, &outbox_key("svc1", bucket, Some("xe2"))),
        ["{\"a\":2}"]
    );
}

#[test]
fn a_reserve_keeps_the_outbox_append_to_one_overflow_call() {
    let rt = Rt::default();
    let bucket = NOW_S / OUTBOX_BUCKET_S;
    rt.gets.set(28);
    append_outbox_reserving(&rt, "svc1", "e3", "{\"a\":3}", 3).unwrap();
    assert_eq!(
        rt.gets.get(),
        28,
        "no read when the reserve leaves one call"
    );
    assert_eq!(rt.cas_calls.get(), 1);
    assert_eq!(
        outbox_lines(&rt, &outbox_key("svc1", bucket, Some("xe3"))),
        ["{\"a\":3}"]
    );
}

#[test]
fn the_outbox_append_counts_its_own_calls_when_remaining_does_not_move() {
    let rt = Rt::default();
    let bucket = NOW_S / OUTBOX_BUCKET_S;
    rt.put_kv(
        &outbox_key("svc1", bucket, None),
        json!({"ndjson": "x".repeat(OUTBOX_VALUE_CAP - 4)}),
    );
    rt.put_kv(
        &outbox_key("svc1", bucket, Some("1")),
        json!({"ndjson": "y"}),
    );
    rt.frozen_remaining.set(Some(32));
    rt.races.set(3);
    append_outbox(&rt, "svc1", "e4", "{\"a\":4}").unwrap();
    assert!(
        rt.gets.get() + rt.cas_calls.get() <= OUTBOX_APPEND_CALLS + 1,
        "{} calls",
        rt.gets.get() + rt.cas_calls.get()
    );
    assert_eq!(
        outbox_lines(&rt, &outbox_key("svc1", bucket, Some("xe4"))),
        ["{\"a\":4}"]
    );
}

#[test]
fn dev_telemetry_gate_and_toggle_drop_dev_only_streams_while_preserving_audit_and_cron_runs() {
    let prod_rt = Rt {
        no_dev_telemetry: true,
        ..Default::default()
    };
    assert!(!dev_telemetry_available(&prod_rt));
    assert!(!effective_dev_telemetry(&prod_rt));
    let buf = LogBuffer::new("svc1", "req1");
    write_log(
        &prod_rt,
        &buf,
        WritableLogKind::FosUsage,
        json!({"source": "ingest", "op": "GET", "bucket": "raw", "status_code": 200, "bytes": 10, "duration_ms": 1}),
    );
    write_log(
        &prod_rt,
        &buf,
        WritableLogKind::ApiTelemetry,
        json!({"source": "compute_api", "method": "GET", "path": "/api/admin/state", "status_code": 200, "duration_ms": 2}),
    );
    write_log(
        &prod_rt,
        &buf,
        WritableLogKind::IngestFiles,
        json!({"domain": "request", "raw_key": "k", "raw_bytes": 100, "status": "committed", "rows": 1}),
    );
    write_log(
        &prod_rt,
        &buf,
        WritableLogKind::CronRuns,
        json!({"job": "ingest", "status": "ok", "duration_ms": 5, "items_processed": 1}),
    );
    assert!(!buf.inner.borrow().cache.contains_key(&LogKind::FosUsage));
    assert!(!buf
        .inner
        .borrow()
        .cache
        .contains_key(&LogKind::ApiTelemetry));
    let outbox = buf.inner.borrow().outbox.clone();
    assert_eq!(
        outbox.len(),
        2,
        "ingest_files stays on: it backs Ingestion History"
    );
    assert!(outbox[0].contains("\"kind\":\"ingest_files\""));
    assert!(outbox[1].contains("\"kind\":\"cron_runs\""));

    // When dev_telemetry config is available, admin toggle controls dev-only streams.
    let dev_rt = Rt::on_shield();
    assert!(dev_telemetry_available(&dev_rt));
    api::patch_operational_settings(&dev_rt, &json!({"dev_telemetry_enabled": false})).unwrap();
    assert!(!effective_dev_telemetry(&dev_rt));
    let buf2 = LogBuffer::new("svc1", "req2");
    write_log(
        &dev_rt,
        &buf2,
        WritableLogKind::ApiTelemetry,
        json!({"source": "compute_api", "method": "GET", "path": "/api/admin/state", "status_code": 200, "duration_ms": 2}),
    );
    assert!(!buf2
        .inner
        .borrow()
        .cache
        .contains_key(&LogKind::ApiTelemetry));
    api::patch_operational_settings(&dev_rt, &json!({"dev_telemetry_enabled": true})).unwrap();
    assert!(effective_dev_telemetry(&dev_rt));
    write_log(
        &dev_rt,
        &buf2,
        WritableLogKind::ApiTelemetry,
        json!({"source": "compute_api", "method": "GET", "path": "/api/admin/state", "status_code": 200, "duration_ms": 2}),
    );
    assert_eq!(
        buf2.inner
            .borrow()
            .cache
            .get(&LogKind::ApiTelemetry)
            .map(Vec::len),
        Some(1)
    );
}

#[test]
fn harness_labels_must_have_the_documented_shape() {
    for good in [
        "c1-p10-s1",
        "dc-11050100-p10-s1",
        "dc-11050100-p10-s12-pg03-cold",
        "camp-p300-s9-pg112-warm",
        "lt-files",
        "lt-fos-get",
    ] {
        assert_eq!(
            api::sanitize_harness_label(Some(good)).as_deref(),
            Some(good),
            "{good}"
        );
    }
    for bad in [
        "camp-s1",
        "c1-p10",
        "c1-p10-s1x",
        "c1-p-s1",
        "-p10-s1",
        "p10-s1",
        "C1-p10-s1",
        "c1--x-p10-s1",
        "c1-p10-s1-pg3-cold",
        "c1-p10-s1-pg03",
        "c1-p10-s1-pg03-Cold",
        "lt-",
        "lt-Files",
        "bad label!",
        "abcdef",
        &format!("{}-p10-s1", "a".repeat(42)),
    ] {
        assert_eq!(api::sanitize_harness_label(Some(bad)), None, "{bad}");
    }
    assert_eq!(
        api::sanitize_harness_label(Some(" c1-p10-s1 ")).as_deref(),
        Some("c1-p10-s1")
    );
}
