use super::*;
use crate::telemetry::outbox_key;
use crate::{
    admin::{FosResponse, Record},
    setup::SetupStores,
    shell::{CoreCacheEntry, CoreCacheError, KvPage, CACHE_OP_BUDGET},
};
use std::cell::{Cell, RefCell};

const NOW_S: u64 = 1_700_000_100;
const NOW_MIN: u64 = NOW_S / 60;
const SID: &str = "svc1";
const PREFIX: &str = "svc1/processed/ops/";

struct Rt {
    now: Cell<u64>,
    cache: RefCell<BTreeMap<String, CoreCacheEntry>>,
    kv: RefCell<BTreeMap<String, (u64, Value)>>,
    objects: RefCell<BTreeMap<String, Vec<u8>>>,
    lookups: Cell<u32>,
    cache_ops: Cell<u32>,
    calls: Cell<u32>,
    budget: Cell<u32>,
    elapsed_s: Cell<u64>,
    fail_put: Cell<bool>,
    fail_delete: Cell<bool>,
    fail_purge: Cell<bool>,
    fail_lookup: Cell<bool>,
    purges: RefCell<Vec<String>>,
    puts: Cell<u32>,
    /// After this many more clock reads, the clock moves to the given time: a run that crosses a
    /// boundary mid-flight.
    jump: Cell<Option<(u32, u64)>>,
}

impl Rt {
    fn new() -> Self {
        Self {
            now: Cell::new(NOW_S),
            cache: Default::default(),
            kv: Default::default(),
            objects: Default::default(),
            lookups: Cell::new(0),
            cache_ops: Cell::new(0),
            calls: Cell::new(0),
            budget: Cell::new(1000),
            elapsed_s: Cell::new(0),
            fail_put: Cell::new(false),
            fail_delete: Cell::new(false),
            fail_purge: Cell::new(false),
            fail_lookup: Cell::new(false),
            purges: Default::default(),
            puts: Cell::new(0),
            jump: Cell::new(None),
        }
    }
    fn charge(&self) {
        self.calls.set(self.calls.get() + 1);
    }
    /// Counts a Core Cache operation and refuses it past the per-execution budget, as
    /// `FastlyRuntime` does.
    fn cache_op(&self) -> Result<(), CoreCacheError> {
        let used = self.cache_ops.get();
        assert!(
            used < CACHE_OP_BUDGET,
            "an execution exceeded the cache-op budget"
        );
        self.cache_ops.set(used + 1);
        Ok(())
    }
    fn put_kv(&self, key: &str, value: Value) {
        self.kv.borrow_mut().insert(key.into(), (1, value));
    }
    fn bump(&self, key: &str) {
        if let Some((g, _)) = self.kv.borrow_mut().get_mut(key) {
            *g += 1;
        }
    }
    fn get_kv(&self, key: &str) -> Option<Value> {
        self.kv.borrow().get(key).map(|(_, v)| v.clone())
    }
    fn cursor(&self) -> Cursor {
        serde_json::from_value(self.get_kv("logs:cursor:svc1").unwrap()).unwrap()
    }
    fn bucket(&self, kind: LogKind, sid: &str, minute: u64, shard: u32, rows: &[Value]) {
        let body: String = rows.iter().map(|r| format!("{r}\n")).collect();
        let meta = json!({"count": rows.len()}).to_string().into_bytes();
        self.cache.borrow_mut().insert(
            bucket_key(kind, sid, minute, shard),
            (body.into_bytes(), meta),
        );
    }
    fn set_cursor(&self, scan_min: u64) {
        self.put_kv(
            "logs:cursor:svc1",
            json!({"scan_min": scan_min, "cache_min": scan_min, "global_end": 0}),
        );
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
    fn config(&self, _: &str) -> Result<Option<String>, String> {
        Ok(None)
    }
    fn secret(&self, _: &str) -> Result<Option<String>, String> {
        Ok(None)
    }
    fn random(&self) -> Result<[u8; 32], String> {
        Ok([7; 32])
    }
    fn now(&self) -> u64 {
        match self.jump.get() {
            Some((0, next)) => {
                self.now.set(next);
                self.jump.set(None);
            }
            Some((reads, next)) => self.jump.set(Some((reads - 1, next))),
            None => {}
        }
        self.now.get()
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
    fn admin_get(&self, key: &str) -> Result<Option<Record>, String> {
        self.charge();
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
        self.charge();
        let mut kv = self.kv.borrow_mut();
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
    fn admin_fos(&self, request: FosRequest<'_>) -> Result<FosResponse, String> {
        self.charge();
        if request.method == "PUT" {
            self.puts.set(self.puts.get() + 1);
            if self.fail_put.get() {
                return Ok(FosResponse {
                    status: 503,
                    body: vec![],
                    cdn_cache: None,
                    retry_after: None,
                });
            }
            self.objects
                .borrow_mut()
                .insert(request.key.into(), request.body);
        }
        if request.method == "DELETE" {
            self.objects.borrow_mut().remove(request.key);
        }
        Ok(FosResponse {
            status: 200,
            body: vec![],
            cdn_cache: None,
            retry_after: None,
        })
    }
    fn admin_remaining_calls(&self) -> u32 {
        self.budget.get().saturating_sub(self.calls.get())
    }
    fn admin_elapsed(&self, _: Instant) -> Duration {
        Duration::from_secs(self.elapsed_s.get())
    }
    fn cache_ops(&self) -> u32 {
        self.cache_ops.get()
    }
    fn core_cache_lookup(&self, key: &str) -> Result<Option<CoreCacheEntry>, CoreCacheError> {
        self.cache_op()?;
        self.lookups.set(self.lookups.get() + 1);
        if self.fail_lookup.get() {
            return Err(CoreCacheError::PurgeRateLimited);
        }
        Ok(self.cache.borrow().get(key).cloned())
    }
    fn core_cache_purge(&self, surrogate: &str) -> Result<(), CoreCacheError> {
        self.cache_op()?;
        if self.fail_purge.get() {
            return Err(CoreCacheError::PurgeRateLimited);
        }
        self.purges.borrow_mut().push(surrogate.into());
        Ok(())
    }
    fn kv_list(&self, prefix: &str, cursor: Option<&str>, limit: u32) -> Result<KvPage, String> {
        self.charge();
        let kv = self.kv.borrow();
        Ok(KvPage::from_sorted_keys(
            kv.keys().cloned(),
            prefix,
            cursor,
            limit,
        ))
    }
    fn kv_delete(&self, key: &str) -> Result<(), String> {
        self.charge();
        if self.fail_delete.get() && !key.starts_with("logs:") {
            return Err("delete_failed".into());
        }
        self.kv.borrow_mut().remove(key);
        Ok(())
    }
}

fn target() -> FlushTarget<'static> {
    FlushTarget {
        region: "us-east",
        bucket: "b",
        access_key: "ak",
        secret_key: "sk",
        ops_prefix: PREFIX,
    }
}

fn row(ts_s: u64, id: &str) -> Value {
    json!({"timestamp": ts_s * 1000, "event_id": id, "service_id": SID, "severity": "info",
           "method": "GET", "status_code": 200})
}

fn run(rt: &Rt, manual: bool) -> FlushReport {
    rt.cache_ops.set(0);
    log_flush(
        rt,
        SID,
        Some(&target()),
        &OperationalLogSettings::default(),
        manual,
        Instant::now(),
    )
    .unwrap()
}

fn run_err(rt: &Rt) -> String {
    rt.cache_ops.set(0);
    log_flush(
        rt,
        SID,
        Some(&target()),
        &OperationalLogSettings::default(),
        true,
        Instant::now(),
    )
    .unwrap_err()
}

/// An outbox key in a bucket the flush treats as closed.
fn q(id: &str) -> String {
    outbox_key(SID, NOW_S / OUTBOX_BUCKET_S - 2, Some(id))
}

fn seed_api(rt: &Rt, minute: u64, ids: &[&str]) {
    let rows: Vec<Value> = ids.iter().map(|id| row(minute * 60, id)).collect();
    rt.bucket(LogKind::ApiTelemetry, SID, minute, 0, &rows);
}

fn string_rows(rt: &Rt, kind: &str) -> Vec<Value> {
    use arrow_array::{Array, StringArray};
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let mut out = Vec::new();
    for (key, body) in rt.objects.borrow().iter() {
        if !key.contains(&format!("/{kind}/")) {
            continue;
        }
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(body.clone()))
            .unwrap()
            .build()
            .unwrap();
        for batch in reader {
            let batch = batch.unwrap();
            let schema = batch.schema();
            for i in 0..batch.num_rows() {
                let mut map = serde_json::Map::new();
                for (c, field) in schema.fields().iter().enumerate() {
                    if let Some(s) = batch.column(c).as_any().downcast_ref::<StringArray>() {
                        if !s.is_null(i) {
                            map.insert(field.name().clone(), s.value(i).into());
                        }
                    }
                }
                out.push(Value::Object(map));
            }
        }
    }
    out
}

#[test]
fn scan_checks_only_new_closed_minutes_and_checkpoints_once() {
    let rt = Rt::new();
    rt.set_cursor(NOW_MIN - 4);
    seed_api(&rt, NOW_MIN - 3, &["a", "b"]);
    seed_api(&rt, NOW_MIN - 1, &["open"]);
    seed_api(&rt, NOW_MIN, &["current"]);
    let report = run(&rt, false);
    assert_eq!(report.status, FlushStatus::NotDue);
    assert_eq!(report.lookups, 2 * 12);
    let cursor = rt.cursor();
    assert_eq!(cursor.scan_min, NOW_MIN - 2);
    assert_eq!(cursor.pending.rows, 2);
    assert_eq!(cursor.pending.nonempty.len(), 1);
    assert_eq!(rt.puts.get(), 0);
}

#[test]
fn long_outage_scans_whole_minutes_within_the_cache_op_budget() {
    let rt = Rt::new();
    rt.set_cursor(NOW_MIN - 360);
    let report = run(&rt, false);
    assert_eq!(report.lookups, 4 * 12);
    assert_eq!(report.scanned_minutes, 4);
    assert!(rt.cache_ops.get() <= CACHE_OP_BUDGET - FLUSH_CACHE_RESERVE);
    assert_eq!(rt.cursor().scan_min, NOW_MIN - 356);
    assert_eq!(rt.cursor().cache_min, NOW_MIN - 356);
    run(&rt, false);
    assert_eq!(rt.cursor().scan_min, NOW_MIN - 352);
}

#[test]
fn a_due_flush_reserves_cache_reads_only_for_already_pending_buckets() {
    let rt = Rt::new();
    seed_api(&rt, NOW_MIN - 359, &["a"]);
    rt.put_kv(
        "logs:cursor:svc1",
        json!({"scan_min": NOW_MIN - 358, "cache_min": NOW_MIN - 360, "global_end": 0,
               "pending": {"rows": 1, "oldest_min": NOW_MIN - 359,
                           "nonempty": [["api_telemetry", NOW_MIN - 359, 0, 1, SID]]}}),
    );
    let report = run(&rt, false);
    assert_eq!(report.status, FlushStatus::Committed);
    assert_eq!(report.scanned_minutes, 4);
    assert_eq!(string_rows(&rt, "api_telemetry").len(), 1);
}

#[test]
fn a_due_flush_with_many_pending_buckets_stays_within_the_soft_cache_op_budget() {
    let rt = Rt::new();
    let entries: Vec<Value> = (0..80u64)
        .map(|i| {
            let minute = NOW_MIN - 200 + i;
            seed_api(&rt, minute, &[&format!("r{i}")]);
            json!(["api_telemetry", minute, 0, 1, SID])
        })
        .collect();
    rt.put_kv(
        "logs:cursor:svc1",
        json!({"scan_min": NOW_MIN - 2, "cache_min": NOW_MIN - 200, "global_end": 0,
               "pending": {"rows": 80, "oldest_min": NOW_MIN - 200, "nonempty": entries}}),
    );
    // The `/_flush` probe's schedule claim precedes the flush in the same execution.
    rt.cache_ops.set(1);
    let report = log_flush(
        &rt,
        SID,
        Some(&target()),
        &OperationalLogSettings::default(),
        false,
        Instant::now(),
    )
    .unwrap();
    assert_eq!(report.status, FlushStatus::Committed);
    assert!(
        rt.cache_ops.get() <= 56,
        "flush used {} of the 56-op soft budget",
        rt.cache_ops.get()
    );
}

#[test]
fn a_fresh_cursor_looks_back_only_as_far_as_live_reads() {
    let rt = Rt::new();
    seed_api(&rt, NOW_MIN - 2 - MAX_SCAN_MIN, &["old"]);
    seed_api(&rt, NOW_MIN - 1 - MAX_SCAN_MIN, &["kept"]);
    for _ in 0..40 {
        run(&rt, true);
    }
    let ids: Vec<Value> = string_rows(&rt, "api_telemetry")
        .into_iter()
        .map(|row| row["event_id"].clone())
        .collect();
    assert_eq!(ids, vec![json!("kept")]);
}

#[test]
fn scan_stops_at_the_pending_entry_cap() {
    let rt = Rt::new();
    let start = NOW_MIN - 360;
    let entries: Vec<Value> = (0..MAX_PENDING_ENTRIES as u64 - 2)
        .map(|_| json!(["api_telemetry", NOW_MIN - 3, 0, 0, SID]))
        .collect();
    rt.put_kv(
        "logs:cursor:svc1",
        json!({"scan_min": start, "cache_min": start, "global_end": 0, "last_flush_s": NOW_S,
               "pending": {"nonempty": entries}}),
    );
    for shard in 0..4 {
        rt.bucket(LogKind::ApiTelemetry, SID, start + 1, shard, &[row(0, "a")]);
    }
    run(&rt, false);
    let cursor = rt.cursor();
    assert_eq!(cursor.scan_min, start + 1);
    assert_eq!(cursor.pending.nonempty.len(), MAX_PENDING_ENTRIES + 2);
    let lookups = run(&rt, false).lookups;
    assert_eq!(lookups, 0);
    assert_eq!(rt.cursor().scan_min, start + 1);
}

#[test]
fn expired_minutes_are_skipped_and_reported_in_the_next_flush() {
    let rt = Rt::new();
    rt.set_cursor(NOW_MIN - 1540);
    let report = run(&rt, false);
    assert_eq!(report.lookups, 4 * 12);
    assert_eq!(rt.cursor().expired, 99);
    assert_eq!(rt.cursor().scan_min, NOW_MIN - 1440 + 3);

    let mut cursor = rt.get_kv("logs:cursor:svc1").unwrap();
    cursor["scan_min"] = (NOW_MIN - 4).into();
    rt.put_kv("logs:cursor:svc1", cursor);
    seed_api(&rt, NOW_MIN - 3, &["x"]);
    run(&rt, true);
    let errors = string_rows(&rt, "platform_errors");
    assert!(errors.iter().any(|r| r["code"] == "log_scan_expired"));
}

#[test]
fn due_rules_gate_flush_without_fos_calls() {
    let rt = Rt::new();
    rt.set_cursor(NOW_MIN - 4);
    seed_api(&rt, NOW_MIN - 3, &["a"]);
    assert_eq!(run(&rt, false).status, FlushStatus::NotDue);
    assert_eq!(rt.puts.get(), 0);

    let settings = OperationalLogSettings {
        flush_max_rows: 50,
        ..Default::default()
    };
    let rt = Rt::new();
    rt.set_cursor(NOW_MIN - 4);
    let ids: Vec<String> = (0..60).map(|i| format!("e{i:02}")).collect();
    let rows: Vec<Value> = ids.iter().map(|id| row((NOW_MIN - 3) * 60, id)).collect();
    rt.bucket(LogKind::ApiTelemetry, SID, NOW_MIN - 3, 0, &rows);
    let set_last_flush = |ago: u64| {
        let mut cursor = rt.get_kv("logs:cursor:svc1").unwrap();
        cursor["last_flush_s"] = (NOW_S - ago).into();
        rt.put_kv("logs:cursor:svc1", cursor);
    };
    set_last_flush(100);
    let early = log_flush(&rt, SID, Some(&target()), &settings, false, Instant::now()).unwrap();
    assert_eq!(
        early.status,
        FlushStatus::NotDue,
        "the 5-minute floor holds"
    );
    set_last_flush(400);
    let report = log_flush(&rt, SID, Some(&target()), &settings, false, Instant::now()).unwrap();
    assert_eq!(report.status, FlushStatus::Committed);
    assert_eq!(report.rows, 60);
    let keys: Vec<String> = rt.objects.borrow().keys().cloned().collect();
    assert_eq!(keys.len(), 1);
    assert!(keys[0].starts_with("svc1/processed/ops/api_telemetry/year=2023/month=11/day=14/hour="));
    assert!(keys[0].contains("window=") && keys[0].ends_with("-n60.parquet"));

    let rt = Rt::new();
    rt.put_kv(
        "logs:cursor:svc1",
        json!({"scan_min": NOW_MIN - 4, "cache_min": NOW_MIN - 4, "last_flush_s": NOW_S - 10}),
    );
    seed_api(&rt, NOW_MIN - 3, &["a"]);
    assert_eq!(
        run(&rt, true).status,
        FlushStatus::Committed,
        "manual ignores the floor"
    );
}

#[test]
fn manifest_is_deterministic_and_reused_after_a_failed_put() {
    let a = Rt::new();
    let b = Rt::new();
    for rt in [&a, &b] {
        rt.set_cursor(NOW_MIN - 4);
        seed_api(rt, NOW_MIN - 3, &["a", "b"]);
        run(rt, true);
    }
    assert_eq!(*a.objects.borrow(), *b.objects.borrow());
    assert_eq!(a.objects.borrow().len(), 1);

    let rt = Rt::new();
    rt.set_cursor(NOW_MIN - 4);
    seed_api(&rt, NOW_MIN - 3, &["a", "b"]);
    rt.fail_put.set(true);
    run_err(&rt);
    let manifest = rt.get_kv("logs:flush_txn:svc1").unwrap();
    assert_eq!(manifest["committed"], false);
    assert_eq!(rt.cursor().pending.rows, 0, "cursor unchanged");
    rt.fail_put.set(false);
    assert_eq!(run(&rt, true).status, FlushStatus::Committed);
    assert_eq!(rt.get_kv("logs:flush_txn:svc1"), None);
    assert_eq!(rt.cursor().txn_id, manifest["txn_id"]);
    assert_eq!(
        rt.objects.borrow().keys().next().unwrap(),
        manifest["outputs"][0].as_str().unwrap()
    );
}

#[test]
fn fresh_flush_prepares_groups_once_and_resume_keeps_exact_outputs() {
    let seed = |rt: &Rt| {
        rt.set_cursor(NOW_MIN - 4);
        seed_api(rt, NOW_MIN - 3, &["duplicate", "duplicate", "later"]);
        let earlier_hour = (NOW_MIN / 60 - 1) * 3600;
        let mut rows = vec![
            json!({"kind":"cron_runs","timestamp":earlier_hour * 1000,"event_id":"earlier","service_id":SID}),
            json!({"kind":"cron_runs","timestamp":NOW_S * 1000,"event_id":"later-hour","service_id":SID}),
        ];
        for i in 0..3 {
            rows.push(json!({"kind":"cron_ticks","timestamp":(earlier_hour+i)*1000,
                "event_id":format!("idle-{i}"),"service_id":SID,"outcome":"idle","trigger":"clock","duration_ms":2}));
        }
        let ndjson: String = rows.iter().map(|row| format!("{row}\n")).collect();
        rt.put_kv(&q("mixed"), json!({"ndjson":ndjson}));
    };
    let direct = Rt::new();
    seed(&direct);
    BUILD_FILES_CALLS.with(|calls| calls.set(0));
    assert_eq!(run(&direct, true).status, FlushStatus::Committed);
    assert_eq!(BUILD_FILES_CALLS.with(|calls| calls.get()), 1);
    assert_eq!(string_rows(&direct, "api_telemetry").len(), 2);
    assert_eq!(string_rows(&direct, "cron_runs").len(), 2);
    assert_eq!(string_rows(&direct, "cron_ticks").len(), 1);

    let resumed = Rt::new();
    seed(&resumed);
    resumed.fail_put.set(true);
    BUILD_FILES_CALLS.with(|calls| calls.set(0));
    assert_eq!(run_err(&resumed), "object_put_failed:503");
    assert_eq!(BUILD_FILES_CALLS.with(|calls| calls.get()), 1);
    let manifest = resumed.get_kv("logs:flush_txn:svc1").unwrap();
    assert_eq!(manifest["committed"], false);
    assert_eq!(resumed.puts.get(), 1);
    resumed.fail_put.set(false);
    assert_eq!(run(&resumed, true).status, FlushStatus::Committed);
    assert_eq!(BUILD_FILES_CALLS.with(|calls| calls.get()), 2);
    assert_eq!(resumed.cursor().txn_id, manifest["txn_id"]);
    assert_eq!(*resumed.objects.borrow(), *direct.objects.borrow());
    let keys: Vec<String> = resumed.objects.borrow().keys().cloned().collect();
    let mut outputs: Vec<String> = serde_json::from_value(manifest["outputs"].clone()).unwrap();
    outputs.sort();
    assert_eq!(keys, outputs);
}

#[test]
fn held_lease_is_busy_and_stale_commit_is_fenced() {
    let rt = Rt::new();
    rt.set_cursor(NOW_MIN - 4);
    seed_api(&rt, NOW_MIN - 3, &["a"]);
    rt.put_kv("logs:flush_lease", json!({"expires_at": NOW_S + 60}));
    assert_eq!(run(&rt, true).status, FlushStatus::Busy);
    assert_eq!(rt.puts.get(), 0);

    rt.put_kv("logs:flush_lease", json!({"expires_at": NOW_S - 1}));
    rt.fail_put.set(true);
    run_err(&rt);
    let stale: Manifest =
        serde_json::from_value(rt.get_kv("logs:flush_txn:svc1").unwrap()).unwrap();
    rt.fail_put.set(false);
    assert_eq!(run(&rt, true).status, FlushStatus::Committed);

    let committed = rt
        .admin_cas(
            "logs:cursor:svc1",
            stale.cursor_generation.as_deref(),
            &to_value(&stale.next_cursor),
        )
        .unwrap();
    assert!(!committed, "a stale generation can never commit");
    let before = rt.puts.get();
    let mut again = stale.clone();
    again.committed = false;
    let mut report = FlushReport::default();
    run_txn(
        &rt,
        SID,
        &target(),
        &mut again,
        None,
        Instant::now(),
        &mut report,
    )
    .unwrap();
    assert_eq!(
        rt.puts.get(),
        before,
        "the generation moved, so only GC remains"
    );
}

#[test]
fn commit_then_crash_before_gc_does_not_duplicate_rows() {
    let rt = Rt::new();
    rt.set_cursor(NOW_MIN - 4);
    rt.put_kv(
        &q("aa"),
        json!({"ndjson": json!({"kind":"cron_runs","timestamp":1_700_000_000_000i64,"event_id":"r1","service_id":SID,"severity":"info","cron_kind":"request_ingest"}).to_string()}),
    );
    rt.fail_delete.set(true);
    assert!(run_err(&rt).contains("delete_failed"));
    let txn = rt.get_kv("logs:flush_txn:svc1").unwrap()["txn_id"].clone();
    assert_eq!(
        rt.get_kv("logs:cursor:svc1").unwrap()["txn_id"],
        txn,
        "the cursor records the commit"
    );
    assert!(rt.get_kv(&q("aa")).is_some());
    let files = rt.objects.borrow().len();
    rt.fail_delete.set(false);
    let report = run(&rt, true);
    assert_eq!(report.rows, 0);
    assert_eq!(rt.get_kv(&q("aa")), None);
    assert_eq!(rt.objects.borrow().len(), files, "no second file");
    assert_eq!(rt.get_kv("logs:flush_txn:svc1"), None);
}

#[test]
fn legacy_per_request_outbox_keys_are_archived_and_removed() {
    let rt = Rt::new();
    rt.set_cursor(NOW_MIN - 4);
    let row = |id: &str| json!({"ndjson": json!({"kind":"cron_runs","timestamp":1_700_000_000_000i64,"event_id":id,"service_id":SID,"severity":"info","cron_kind":"request_ingest"}).to_string()});
    let legacy = format!("logq:{SID}:{:013}-old", (NOW_S - 86_400) * 1000);
    let other = outbox_key("othersvc", NOW_S / OUTBOX_BUCKET_S - 2, None);
    rt.put_kv(&legacy, row("legacy"));
    rt.put_kv(&other, row("other"));
    assert_eq!(run(&rt, true).status, FlushStatus::Committed);
    let runs = string_rows(&rt, "cron_runs");
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0]["event_id"], "legacy");
    assert_eq!(rt.get_kv(&legacy), None);
    assert!(
        rt.get_kv(&other).is_some(),
        "another service's outbox is left alone"
    );
}

#[test]
fn wall_time_stop_leaves_manifest_uncommitted_and_resumes() {
    let rt = Rt::new();
    rt.set_cursor(NOW_MIN - 4);
    seed_api(&rt, NOW_MIN - 3, &["a"]);
    rt.elapsed_s.set(41);
    assert_eq!(run(&rt, true).status, FlushStatus::Stopped);
    assert_eq!(rt.puts.get(), 0);
    assert_eq!(
        rt.get_kv("logs:flush_txn:svc1").unwrap()["committed"],
        false
    );
    rt.elapsed_s.set(0);
    assert_eq!(run(&rt, true).status, FlushStatus::Committed);
    assert_eq!(rt.puts.get(), 1);
}

#[test]
fn budget_admission_defers_and_splits_large_backlogs() {
    let rt = Rt::new();
    rt.set_cursor(NOW_MIN - 4);
    seed_api(&rt, NOW_MIN - 3, &["a"]);
    rt.budget.set(10);
    assert_eq!(run(&rt, true).status, FlushStatus::Deferred);
    assert_eq!(rt.puts.get(), 0);

    let rt = Rt::new();
    rt.set_cursor(NOW_MIN - 4);
    for i in 0..40u64 {
        let line = json!({"kind":"ingest_files","timestamp":1_700_000_000_000i64 + i as i64,
                          "event_id":format!("f{i}"),"service_id":SID,"severity":"info","raw_key":"k"});
        rt.put_kv(&q(&format!("{i:02}")), json!({"ndjson": line.to_string()}));
    }
    rt.budget.set(rt.calls.get() + 32);
    run(&rt, true);
    let left = rt
        .kv
        .borrow()
        .keys()
        .filter(|k| k.starts_with("logq_"))
        .count();
    assert!(
        left > 0 && left < 40,
        "a bounded transaction drained part: {left}"
    );
    assert!(rt.calls.get() <= 32);
}

#[test]
fn a_flush_crossing_an_hour_mid_run_stays_within_budget() {
    // The flush starts in the last second of an hour and its clock reads the next hour later on.
    // Its warning row must land in the file group it was budgeted for, or the extra PUT takes
    // calls the rest of the transaction was promised.
    let start = 1_700_002_799;
    let next_hour_ms = (start + 1) * 1000;
    let flush = |budget: u32, jump: Option<(u32, u64)>| {
        let rt = Rt::new();
        rt.now.set(start);
        rt.jump.set(jump);
        // The expired minute makes a platform_errors warning row stamped with the flush time.
        rt.put_kv(
            "logs:cursor:svc1",
            json!({"scan_min": start / 60 - 4, "cache_min": start / 60 - 4, "global_end": 0,
                   "expired": 1}),
        );
        for i in 0..24u64 {
            let rows: String = [
                json!({"kind": "platform_errors", "timestamp": next_hour_ms + i,
                       "event_id": format!("e{i}"), "service_id": SID, "severity": "error",
                       "code": "x", "message": "m"}),
                json!({"kind": "ingest_files", "timestamp": next_hour_ms + i,
                       "event_id": format!("f{i}"), "service_id": SID, "severity": "info",
                       "raw_key": "k"}),
            ]
            .iter()
            .map(|row| format!("{row}\n"))
            .collect();
            rt.put_kv(
                &outbox_key(SID, start / OUTBOX_BUCKET_S - 2, Some(&format!("{i:02}"))),
                json!({"ndjson": rows}),
            );
        }
        rt.budget.set(budget);
        let report = run(&rt, true);
        assert!(
            rt.calls.get() <= budget,
            "budget {budget}: {}",
            rt.calls.get()
        );
        (report.status, rt.calls.get(), rt.puts.get())
    };
    for budget in 22..48 {
        let steady = flush(budget, None);
        assert_eq!(steady.0, FlushStatus::Committed, "budget {budget}");
        for reads in 1..6 {
            assert_eq!(
                flush(budget, Some((reads, start + 1))),
                steady,
                "budget {budget}, clock moved after {reads} reads"
            );
        }
    }
}

#[test]
fn idle_ticks_roll_up_into_one_row() {
    let rows: Vec<Value> = (0..5)
        .map(|i| {
            json!({"timestamp": 1_700_000_000_000i64 + i * 1000, "event_id": format!("t{i}"),
                   "outcome": "idle", "trigger": "clock", "duration_ms": 2, "service_id": SID})
        })
        .collect();
    let merged = rollup_idle_ticks(rows.clone());
    assert_eq!(merged.len(), 1);
    assert_eq!(merged[0]["ticks_count"], 5);
    assert_eq!(merged[0]["trigger"], "idle_rollup");
    assert_eq!(merged[0]["duration_ms"], 10);
    let kept = build_files(
        PREFIX,
        0,
        "t",
        true,
        vec![
            (LogKind::CronTicks, rows[0].clone()),
            (LogKind::CronTicks, rows[1].clone()),
        ],
    );
    assert_eq!(kept[0].2.len(), 2, "log_idle_ticks keeps every row");
    let mut mixed = rows;
    mixed.insert(
        2,
        json!({"timestamp": 1_700_000_002_500i64, "event_id": "busy", "outcome": "active"}),
    );
    assert_eq!(rollup_idle_ticks(mixed).len(), 3);
}

#[test]
fn audit_drains_and_outbox_trims_with_stable_indexes() {
    let rt = Rt::new();
    rt.set_cursor(NOW_MIN - 4);
    let outbox: Vec<Value> = (0..60)
        .map(|i| json!({"actor": "a", "at": 1_700_000_000 + i, "event": "e", "config_version": 1}))
        .collect();
    seed_api(&rt, NOW_MIN - 3, &["a"]);
    rt.put_kv(
        "svc:svc1",
        json!({"service_id": SID, "_audit_outbox": outbox}),
    );
    rt.put_kv(
        "audit:00000000000000000007",
        json!({"time": 1_700_000_000u64, "actor": "u", "action": "login"}),
    );
    assert_eq!(run(&rt, true).status, FlushStatus::Committed);
    assert_eq!(string_rows(&rt, "admin_audit").len(), 61);
    let svc = rt.get_kv("svc:svc1").unwrap();
    assert_eq!(
        svc["_audit_outbox"].as_array().unwrap().len(),
        SVC_OUTBOX_KEEP
    );
    assert_eq!(svc["_audit_outbox_base"], 10);
    let cursor = rt.cursor();
    assert_eq!(cursor.svc_outbox_archived, 60);
    assert_eq!(cursor.audit_seq, Some(7));

    let before = rt.objects.borrow().len();
    run(&rt, true);
    assert_eq!(
        rt.objects.borrow().len(),
        before,
        "nothing re-archived after the trim"
    );
}

#[test]
fn audit_key_gc_requires_archived_old_and_keeps_newest_fifty() {
    let rt = Rt::new();
    for seq in 1..=61u64 {
        rt.put_kv(
            &format!("audit:{seq:020}"),
            json!({"time": NOW_S - 400 * 86_400}),
        );
    }
    assert_eq!(
        audit_key_gc(&rt, Some(5), 365, 0).unwrap(),
        5,
        "only archived keys"
    );
    assert_eq!(audit_key_gc(&rt, None, 365, 0).unwrap(), 0);
    assert_eq!(
        audit_key_gc(&rt, Some(100), 365, 0).unwrap(),
        6,
        "newest 50 stay"
    );
    let left = rt
        .kv
        .borrow()
        .keys()
        .filter(|k| k.starts_with("audit:"))
        .count();
    assert_eq!(left, AUDIT_KEEP);

    let rt = Rt::new();
    for seq in 1..=60u64 {
        rt.put_kv(
            &format!("audit:{seq:020}"),
            json!({"time": NOW_S - 10 * 86_400}),
        );
    }
    assert_eq!(
        audit_key_gc(&rt, Some(100), 365, 0).unwrap(),
        0,
        "younger than retention"
    );
}

#[test]
fn nothing_is_written_before_provisioning() {
    let rt = Rt::new();
    seed_api(&rt, NOW_MIN - 3, &["a"]);
    let report = log_flush(&rt, SID, None, &Default::default(), true, Instant::now()).unwrap();
    assert_eq!(report.status, FlushStatus::Unprovisioned);
    assert_eq!(rt.calls.get(), 0);
    assert!(rt.kv.borrow().is_empty());
}

#[test]
fn first_flush_drains_global_buckets_and_deep_outbox_warns() {
    let rt = Rt::new();
    rt.bucket(
        LogKind::ApiTelemetry,
        "global",
        NOW_MIN - 10,
        1,
        &[row((NOW_MIN - 10) * 60, "g1")],
    );
    for _ in 0..50 {
        run(&rt, true);
    }
    assert_eq!(string_rows(&rt, "api_telemetry").len(), 1);

    let manifest = Manifest {
        notes: Notes {
            outbox_depth: 10_001,
            ..Default::default()
        },
        start_min: NOW_MIN,
        ..Default::default()
    };
    let rows = note_rows(SID, &manifest);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1["code"], "logq_depth_high");
}

#[test]
fn purges_each_flushed_hour_once_after_it_closes_and_retries_on_failure() {
    let rt = Rt::new();
    let hour = NOW_MIN / 60 - 1;
    let hour_start = hour * 60;
    rt.set_cursor(hour_start + 4);
    seed_api(&rt, hour_start + 5, &["a"]);
    rt.bucket(
        LogKind::ApiTelemetry,
        SID,
        hour_start + 6,
        2,
        &[row((hour_start + 6) * 60, "b")],
    );
    rt.fail_purge.set(true);
    assert_eq!(run(&rt, true).status, FlushStatus::Committed);
    assert!(rt.purges.borrow().is_empty());
    let cursor = rt.cursor();
    assert_eq!(cursor.purge_due.len(), 1, "one (kind, hour) after dedup");
    assert_eq!(
        cursor.purged_hour, 0,
        "purged_hour does not advance on failure"
    );

    // Three closed minutes per budgeted scan: the hour closes for purging after 18 runs.
    for _ in 0..20 {
        run(&rt, false);
    }
    assert!(rt.purges.borrow().is_empty());
    assert_eq!(rt.cursor().purge_due.len(), 1);
    rt.fail_purge.set(false);
    for _ in 0..4 {
        run(&rt, false);
    }
    assert_eq!(
        *rt.purges.borrow(),
        vec![format!("flp-log-api_telemetry-svc1-h{hour}")]
    );
    assert!(rt.cursor().purge_due.is_empty());
    assert_eq!(rt.cursor().purged_hour, hour);
}

#[test]
fn current_hour_is_never_purged() {
    let rt = Rt::new();
    rt.set_cursor(NOW_MIN - 5);
    seed_api(&rt, NOW_MIN - 3, &["a"]);
    run(&rt, true);
    run(&rt, false);
    assert!(rt.purges.borrow().is_empty());
}

#[test]
fn clear_cache_is_busy_during_a_transaction_and_scan_behind_for_one_kind() {
    let rt = Rt::new();
    rt.set_cursor(NOW_MIN - 4);
    seed_api(&rt, NOW_MIN - 3, &["a"]);
    rt.fail_put.set(true);
    run_err(&rt);
    assert_eq!(clear_cache(&rt, SID, None), Err(ClearError::Busy));

    let rt = Rt::new();
    rt.set_cursor(NOW_MIN - 100);
    assert_eq!(
        clear_cache(&rt, SID, Some(LogKind::ApiTelemetry)),
        Err(ClearError::ScanBehind)
    );

    let rt = Rt::new();
    rt.set_cursor(NOW_MIN - 10);
    assert_eq!(
        clear_cache(&rt, SID, Some(LogKind::ApiTelemetry)),
        Err(ClearError::ScanBehind),
        "eight minutes of three kinds exceed one execution's cache-op budget"
    );
    assert_eq!(rt.cursor().scan_min, NOW_MIN - 10);

    let rt = Rt::new();
    rt.set_cursor(NOW_MIN - 6);
    seed_api(&rt, NOW_MIN - 5, &["a"]);
    rt.bucket(
        LogKind::FosUsage,
        SID,
        NOW_MIN - 5,
        1,
        &[row((NOW_MIN - 5) * 60, "f")],
    );
    clear_cache(&rt, SID, Some(LogKind::ApiTelemetry)).unwrap();
    let cursor = rt.cursor();
    assert_eq!(cursor.scan_min, NOW_MIN - 2);
    assert_eq!(cursor.pending.nonempty.len(), 1);
    assert_eq!(cursor.pending.nonempty[0].0, "fos_usage");
    assert_eq!(
        *rt.purges.borrow(),
        vec!["flp-log-api_telemetry".to_string()]
    );

    clear_cache(&rt, SID, None).unwrap();
    assert!(rt.cursor().pending.nonempty.is_empty());
    assert_eq!(rt.purges.borrow().last().unwrap(), "flp-logs-all");
}

fn cron_line(id: &str) -> Value {
    json!({"ndjson": json!({"kind":"cron_runs","timestamp":1_700_000_000_000i64,"event_id":id,"service_id":SID,"severity":"info","cron_kind":"request_ingest"}).to_string()})
}

#[test]
fn flush_after_a_gc_resume_uses_the_fresh_cursor_generation() {
    let rt = Rt::new();
    let hour = NOW_MIN / 60 - 1;
    rt.put_kv(
        "logs:cursor:svc1",
        json!({"scan_min": NOW_MIN - 2, "cache_min": NOW_MIN, "purge_due": [["api_telemetry", hour, "svc1"]]}),
    );
    rt.put_kv(&q("aa"), cron_line("r1"));
    rt.fail_delete.set(true);
    run_err(&rt);
    rt.fail_delete.set(false);
    rt.put_kv(&q("bb"), cron_line("r2"));
    run(&rt, true);
    assert_eq!(rt.get_kv(&q("bb")), None);
    assert_eq!(
        string_rows(&rt, "cron_runs").len(),
        2,
        "r2 was archived before its key was deleted"
    );
}

#[test]
fn a_lease_is_held_for_exactly_lease_seconds_and_stealable_at_expiry() {
    let rt = Rt::new();
    let _held = acquire_lease(&rt, None).unwrap().expect("acquired");
    assert_eq!(
        rt.get_kv(LEASE_KEY).unwrap()["expires_at"],
        NOW_S + LEASE_SECONDS
    );
    assert!(acquire_lease(&rt, None).unwrap().is_none());
    rt.now.set(NOW_S + LEASE_SECONDS - 1);
    assert!(acquire_lease(&rt, None).unwrap().is_none());
    rt.now.set(NOW_S + LEASE_SECONDS);
    let stolen = acquire_lease(&rt, None).unwrap();
    assert!(stolen.is_some(), "an expired lease is taken over");
}

#[test]
fn an_intervening_cursor_write_never_counts_as_a_commit_and_the_txn_is_abandoned() {
    let rt = Rt::new();
    rt.set_cursor(NOW_MIN - 4);
    rt.put_kv(&q("aa"), cron_line("r1"));
    rt.fail_put.set(true);
    run_err(&rt);
    let manifest = rt.get_kv("logs:flush_txn:svc1").unwrap();
    let orphan = manifest["outputs"][0].as_str().unwrap().to_owned();
    rt.objects.borrow_mut().insert(orphan.clone(), vec![1]);
    rt.bump("logs:cursor:svc1");
    rt.fail_put.set(false);
    assert_eq!(run(&rt, true).status, FlushStatus::NotDue);
    assert!(rt.get_kv(&q("aa")).is_some(), "unarchived key survives");
    assert_eq!(rt.get_kv("logs:flush_txn:svc1"), None);
    assert!(
        !rt.objects.borrow().contains_key(&orphan),
        "orphan output removed"
    );
    assert_eq!(run(&rt, true).status, FlushStatus::Committed);
    assert_eq!(rt.get_kv(&q("aa")), None);
    assert_eq!(string_rows(&rt, "cron_runs").len(), 1);
}

#[test]
fn a_cursor_carrying_the_txn_id_is_a_commit_and_only_gc_remains() {
    let rt = Rt::new();
    rt.set_cursor(NOW_MIN - 4);
    rt.put_kv(&q("aa"), cron_line("r1"));
    rt.fail_put.set(true);
    run_err(&rt);
    let manifest = rt.get_kv("logs:flush_txn:svc1").unwrap();
    let mut cursor = rt.get_kv("logs:cursor:svc1").unwrap();
    cursor["txn_id"] = manifest["txn_id"].clone();
    rt.put_kv("logs:cursor:svc1", cursor);
    rt.bump("logs:cursor:svc1");
    rt.fail_put.set(false);
    run(&rt, true);
    assert_eq!(rt.puts.get(), 1, "no new PUT after the commit");
    assert_eq!(rt.get_kv(&q("aa")), None);
}

#[test]
fn cursor_writers_respect_the_lease() {
    let rt = Rt::new();
    rt.set_cursor(NOW_MIN - 100);
    rt.put_kv("logs:flush_lease", json!({"expires_at": NOW_S + 60}));
    run(&rt, false);
    assert_eq!(
        rt.cursor().scan_min,
        NOW_MIN - 100,
        "not-due path leaves the cursor alone"
    );
    assert_eq!(clear_cache(&rt, SID, None), Err(ClearError::Busy));
}

#[test]
fn note_rows_survive_a_resume_at_a_later_time() {
    let rt = Rt::new();
    rt.put_kv(
        "logs:cursor:svc1",
        json!({"scan_min": NOW_MIN - 4, "cache_min": NOW_MIN - 4, "expired": 3}),
    );
    seed_api(&rt, NOW_MIN - 3, &["a"]);
    rt.fail_put.set(true);
    run_err(&rt);
    let manifest = rt.get_kv("logs:flush_txn:svc1").unwrap();
    rt.now.set(NOW_S + 3600);
    rt.fail_put.set(false);
    run(&rt, true);
    let keys: Vec<String> = rt.objects.borrow().keys().cloned().collect();
    let outputs: Vec<String> = manifest["outputs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|k| k.as_str().unwrap().to_string())
        .collect();
    assert_eq!(keys, outputs);
}

#[test]
fn clear_cache_aborts_when_a_scan_fails() {
    let rt = Rt::new();
    rt.set_cursor(NOW_MIN - 10);
    rt.fail_lookup.set(true);
    let result = clear_cache(&rt, SID, Some(LogKind::ApiTelemetry));
    assert!(matches!(result, Err(ClearError::Failed(_))), "{result:?}");
    assert_eq!(rt.cursor().scan_min, NOW_MIN - 10);
    assert!(rt.purges.borrow().is_empty());
}

#[test]
fn collect_caps_rows_per_flush_and_drains_the_rest_next_time() {
    let rt = Rt::new();
    rt.set_cursor(NOW_MIN - 6);
    for minute in 0..3u64 {
        let ids: Vec<String> = (0..3000).map(|i| format!("m{minute}-{i}")).collect();
        let refs: Vec<&str> = ids.iter().map(String::as_str).collect();
        seed_api(&rt, NOW_MIN - 5 + minute, &refs);
    }
    assert_eq!(run(&rt, true).status, FlushStatus::Committed);
    assert_eq!(string_rows(&rt, "api_telemetry").len(), 3000);
    run(&rt, true);
    run(&rt, true);
    assert_eq!(string_rows(&rt, "api_telemetry").len(), 9000);
}

#[test]
fn due_flush_commits_on_an_idle_tick_budget_when_run_first() {
    let rt = Rt::new();
    rt.set_cursor(NOW_MIN - 4);
    seed_api(&rt, NOW_MIN - 3, &["a"]);
    // 32 calls minus cron state, service load and two uncounted secret reads.
    let start = 32 - 5;
    rt.budget.set(rt.calls.get() + start);
    assert_eq!(run(&rt, true).status, FlushStatus::Committed);
    assert!(rt.calls.get() <= start);

    // The same flush after an idle ingest (9 more calls) never fits.
    let rt = Rt::new();
    rt.set_cursor(NOW_MIN - 4);
    seed_api(&rt, NOW_MIN - 3, &["a"]);
    rt.budget.set(rt.calls.get() + start - 9);
    assert_eq!(run(&rt, true).status, FlushStatus::Deferred);
}

#[test]
fn every_bucket_busy_for_hours_drains_with_bounded_pending_and_scan_lag() {
    let rt = Rt::new();
    rt.set_cursor(NOW_MIN - 2);
    let mut expected = BTreeSet::new();
    let (mut max_pending, mut max_lag) = (0, 0);
    let probes = 6 * 30;
    for probe in 0..probes + 30 {
        let before = rt.now.get();
        rt.now.set(before + 120);
        if probe < probes {
            // All 16 buckets of every minute that just passed get a row: 16 per minute.
            for minute in before / 60..rt.now.get() / 60 {
                for kind in CACHE_KINDS {
                    for shard in 0..LOG_SHARDS {
                        let id = format!("{}-{minute}-{shard}", kind.as_str());
                        rt.bucket(kind, SID, minute, shard, &[row(minute * 60, &id)]);
                        expected.insert((kind.as_str().to_owned(), id));
                    }
                }
            }
        }
        rt.budget.set(rt.calls.get() + 32);
        run(&rt, false);
        if (10..probes).contains(&probe) {
            let cursor = rt.cursor();
            max_pending = max_pending.max(cursor.pending.nonempty.len());
            max_lag = max_lag.max(rt.now.get() / 60 - cursor.scan_min);
        }
    }
    // Pending peaks where flush_max_rows (500) makes it due, then drains; the scan stays well
    // inside the live-read window.
    assert!(max_pending <= 600, "pending grew to {max_pending} buckets");
    assert!(max_lag <= 20, "the scan fell {max_lag} min behind");
    let mut archived = BTreeSet::new();
    for kind in CACHE_KINDS {
        for row in string_rows(&rt, kind.as_str()) {
            assert!(
                archived.insert((
                    kind.as_str().to_owned(),
                    row["event_id"].as_str().unwrap().to_owned()
                )),
                "duplicate {row}"
            );
        }
    }
    assert_eq!(
        archived.len(),
        expected.len(),
        "every row archived exactly once"
    );
}

#[test]
fn a_bucket_backlog_cannot_starve_the_outbox_or_audit_sources() {
    let rt = Rt::new();
    let mut pending = Vec::new();
    for hour in 1..=6u64 {
        for shard in 0..LOG_SHARDS {
            let minute = NOW_MIN - hour * 60;
            rt.bucket(
                LogKind::ApiTelemetry,
                SID,
                minute,
                shard,
                &[row(minute * 60, &format!("h{hour}-{shard}"))],
            );
            pending.push(json!(["api_telemetry", minute, shard, 1, SID]));
        }
    }
    rt.put_kv(
        "logs:cursor:svc1",
        json!({"scan_min": NOW_MIN - 2, "cache_min": NOW_MIN - 400, "global_end": NOW_MIN - 2,
               "pending": {"rows": pending.len(), "oldest_min": NOW_MIN - 360, "nonempty": pending}}),
    );
    rt.put_kv(&q("aa"), cron_line("r1"));
    rt.put_kv(
        "svc:svc1",
        json!({"service_id": SID, "_audit_outbox": [{"actor": "a", "at": 1_700_000_000u64, "event": "e", "config_version": 1}]}),
    );
    rt.put_kv(
        "audit:00000000000000000007",
        json!({"time": 1_700_000_000u64, "actor": "u", "action": "login"}),
    );
    rt.budget.set(rt.calls.get() + 32);
    assert_eq!(run(&rt, false).status, FlushStatus::Committed);
    assert_eq!(string_rows(&rt, "cron_runs").len(), 1, "outbox read");
    assert_eq!(
        string_rows(&rt, "admin_audit").len(),
        2,
        "audit key and svc outbox read"
    );
    assert!(!string_rows(&rt, "api_telemetry").is_empty());
    assert!(
        rt.cursor().behind,
        "the rest is left for the next flush, past the floor"
    );
}

/// Live g2 (56b6d24): about 200 per-request `logq:` keys left from before batching, each one
/// cron_runs row, drained two or three per commit while the clock and the flush's own overflow
/// row added two batch keys per 2-minute `/_flush` probe, so the depth never fell. The commit
/// now leaves room for four keys, and the flush row joins the bucket key, so one key arrives
/// per probe.
#[test]
fn a_legacy_outbox_backlog_drains_while_new_batches_keep_arriving() {
    let rt = Rt::new();
    rt.set_cursor(NOW_MIN - 2);
    let line = |id: &str, ts_s: u64| {
        json!({"ndjson": json!({"kind": "cron_runs", "timestamp": ts_s * 1000, "event_id": id,
                                "service_id": SID, "severity": "info",
                                "cron_kind": "log_flush"}).to_string()})
    };
    let start = NOW_S - 4 * 3600;
    let mut expected = 0;
    for i in 0..200u64 {
        let at = start + i * 72;
        rt.put_kv(
            &format!("logq:{SID}:{:013}-{i:032x}", at * 1000),
            line(&format!("legacy{i}"), at),
        );
        expected += 1;
    }
    let depth = |rt: &Rt| {
        rt.kv
            .borrow()
            .keys()
            .filter(|k| outbox_bucket(k, SID).is_some())
            .count()
    };
    let mut flushes = 0;
    // The two newest buckets are still open, so they stay until a later probe.
    while depth(&rt) > 2 {
        assert!(
            flushes < 80,
            "outbox depth {} after {flushes} flushes",
            depth(&rt)
        );
        let bucket = rt.now.get() / OUTBOX_BUCKET_S;
        rt.put_kv(
            &outbox_key(SID, bucket, None),
            line(&format!("new{flushes}"), rt.now.get()),
        );
        expected += 1;
        rt.now.set(rt.now.get() + OUTBOX_BUCKET_S);
        // `record_flush` spends five calls before the flush and keeps three for its row.
        rt.budget.set(rt.calls.get() + 32 - 5);
        rt.cache_ops.set(0);
        log_flush_reserving(
            &rt,
            SID,
            Some(&target()),
            &OperationalLogSettings::default(),
            false,
            Instant::now(),
            3,
        )
        .unwrap();
        flushes += 1;
    }
    let archived = string_rows(&rt, "cron_runs").len();
    assert_eq!(
        archived + depth(&rt),
        expected,
        "rows archived exactly once"
    );
}

#[test]
fn multi_hour_svc_audit_outbox_drains_across_flushes_without_livelocking_behind() {
    let rt = Rt::new();
    rt.set_cursor(NOW_MIN - 2);
    // Seed 8 audit entries across 8 distinct hours (> MAX_GROUPS = 6) plus an outbox row.
    let entries: Vec<Value> = (0..8u64)
        .map(|h| {
            json!({
                "actor": "admin",
                "at": NOW_S - h * 3600,
                "event": format!("evt-{h}"),
                "config_version": h + 1,
            })
        })
        .collect();
    rt.put_kv(
        "svc:svc1",
        json!({"service_id": SID, "_audit_outbox": entries}),
    );
    rt.put_kv(
        &outbox_key(SID, NOW_S / OUTBOX_BUCKET_S - 40, Some("aa")),
        cron_line("r1"),
    );
    rt.budget.set(rt.calls.get() + 32);
    let first = run(&rt, false);
    assert_eq!(first.status, FlushStatus::Committed);
    assert!(
        rt.cursor().svc_outbox_archived > 0 && rt.cursor().svc_outbox_archived < 8,
        "first flush archives up to MAX_GROUPS hours: {}",
        rt.cursor().svc_outbox_archived
    );
    assert!(rt.cursor().behind, "remaining hours stay marked behind");

    rt.budget.set(rt.calls.get() + 32);
    let second = run(&rt, false);
    assert_eq!(second.status, FlushStatus::Committed);
    assert_eq!(rt.cursor().svc_outbox_archived, 8, "all 8 entries archived");
    assert!(!rt.cursor().behind, "behind clears once all hours drain");
    assert_eq!(string_rows(&rt, "admin_audit").len(), 8);

    rt.budget.set(rt.calls.get() + 32);
    let third = run(&rt, false);
    assert_eq!(third.status, FlushStatus::NotDue);
}
