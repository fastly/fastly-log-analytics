//! Retention sweep state writes against a KV store whose reads can lag its own writes, as
//! Fastly KV lookups may right after an insert in the same execution.

use fastly_log_analytics::{
    admin::{FosRequest, FosResponse, Record, Service},
    ingest,
    setup::SetupStores,
    shell::Runtime,
    telemetry::{LogKind, OperationalLogSettings},
};
use serde_json::{json, Value};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    time::Instant,
};

const BASE: u64 = 1_800_000_000; // 2027-01-15T08:00:00Z
const SID: &str = "AbCdEfGhIjKlMnOpQrSt12";

#[derive(Default)]
struct LaggingKv {
    kv: RefCell<BTreeMap<String, (u64, Value)>>,
    /// What a read returns for a key written in this execution: the record before the write.
    before_write: RefCell<BTreeMap<String, Option<(u64, Value)>>>,
    bucket: RefCell<BTreeMap<String, usize>>,
    calls: Cell<u32>,
    /// LastModified per object key; unset objects were written at `BASE`.
    mtimes: RefCell<BTreeMap<String, u64>>,
    /// Seconds past `BASE`.
    clock: Cell<u64>,
    vcpu: Cell<u64>,
    /// vCPU milliseconds each FOS request costs.
    vcpu_per_fos: Cell<u64>,
    /// The next CAS of this key is rejected and not applied.
    drop_cas: RefCell<Option<String>>,
}

impl LaggingKv {
    fn charge(&self) -> Result<(), String> {
        if self.calls.get() >= 32 {
            return Err("backend_budget_exhausted".into());
        }
        self.calls.set(self.calls.get() + 1);
        Ok(())
    }

    fn new_execution(&self) {
        self.calls.set(0);
        self.vcpu.set(0);
        self.before_write.borrow_mut().clear();
    }

    fn list(&self, query: &str) -> String {
        let q: BTreeMap<String, String> = url::form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .collect();
        let prefix = q.get("prefix").cloned().unwrap_or_default();
        let max: usize = q
            .get("max-keys")
            .and_then(|m| m.parse().ok())
            .unwrap_or(1000);
        let after = q
            .get("continuation-token")
            .or_else(|| q.get("start-after"))
            .cloned();
        let (mut contents, mut count, mut last, mut truncated) =
            (String::new(), 0, String::new(), false);
        for (key, size) in self.bucket.borrow().iter() {
            if !key.starts_with(&prefix) || after.as_ref().is_some_and(|a| key <= a) {
                continue;
            }
            if count == max {
                truncated = true;
                break;
            }
            count += 1;
            last = key.clone();
            let at = self.mtimes.borrow().get(key).copied().unwrap_or(BASE);
            let at = time::OffsetDateTime::from_unix_timestamp(at as i64)
                .unwrap()
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap();
            contents.push_str(&format!(
                "<Contents><Key>{key}</Key><Size>{size}</Size><LastModified>{at}</LastModified></Contents>"
            ));
        }
        let token = if truncated {
            format!("<NextContinuationToken>{last}</NextContinuationToken>")
        } else {
            String::new()
        };
        format!(
            "<?xml version=\"1.0\"?><ListBucketResult><IsTruncated>{truncated}</IsTruncated>{contents}{token}</ListBucketResult>"
        )
    }
}

impl SetupStores for LaggingKv {
    fn config_present(&self, _name: &str) -> Result<bool, String> {
        Ok(true)
    }
    fn secret_present(&self, _name: &str) -> Result<bool, String> {
        Ok(true)
    }
    fn state_linked(&self) -> Result<bool, String> {
        Ok(true)
    }
}

impl Runtime for LaggingKv {
    fn config(&self, _name: &str) -> Result<Option<String>, String> {
        Ok(None)
    }
    fn secret(&self, _name: &str) -> Result<Option<String>, String> {
        Ok(None)
    }
    fn random(&self) -> Result<[u8; 32], String> {
        Ok([7; 32])
    }
    fn now(&self) -> u64 {
        BASE + self.clock.get()
    }
    fn vcpu_ms(&self) -> u64 {
        self.vcpu.get()
    }
    fn checks(&self, _front_rl: bool) -> Result<Value, String> {
        Ok(json!({}))
    }
    fn versions(&self) -> Result<Value, String> {
        Ok(json!({}))
    }
    fn compute_service_id(&self) -> Option<String> {
        None
    }
    fn compute_service_version(&self) -> Option<String> {
        None
    }
    fn admin_remaining_calls(&self) -> u32 {
        32 - self.calls.get()
    }
    fn admin_get(&self, key: &str) -> Result<Option<Record>, String> {
        self.charge()?;
        let record = match self.before_write.borrow().get(key) {
            Some(stale) => stale.clone(),
            None => self.kv.borrow().get(key).cloned(),
        };
        Ok(record.map(|(generation, value)| Record {
            generation: generation.to_string(),
            value,
        }))
    }
    fn admin_cas(
        &self,
        key: &str,
        generation: Option<&str>,
        value: &Value,
    ) -> Result<bool, String> {
        self.charge()?;
        if self.drop_cas.borrow().as_deref() == Some(key) {
            self.drop_cas.borrow_mut().take();
            return Ok(false);
        }
        let mut kv = self.kv.borrow_mut();
        let current = kv.get(key).cloned();
        let matches = match (&current, generation) {
            (None, None) => true,
            (Some((g, _)), Some(expected)) => g.to_string() == expected,
            _ => false,
        };
        if !matches {
            return Ok(false);
        }
        self.before_write
            .borrow_mut()
            .entry(key.into())
            .or_insert(current.clone());
        let next = current.map_or(1, |(g, _)| g + 1);
        kv.insert(key.into(), (next, value.clone()));
        Ok(true)
    }
    fn admin_fos(&self, request: FosRequest<'_>) -> Result<FosResponse, String> {
        self.charge()?;
        self.vcpu.set(self.vcpu.get() + self.vcpu_per_fos.get());
        let body = match request.method {
            "GET" if request.query.contains("list-type") => self.list(request.query),
            "POST" if request.query == "delete" => {
                let xml = String::from_utf8(request.body).unwrap();
                let mut out = String::from("<DeleteResult>");
                for part in xml.split("<Key>").skip(1) {
                    let key = part.split("</Key>").next().unwrap();
                    self.bucket.borrow_mut().remove(key);
                    out.push_str(&format!("<Deleted><Key>{key}</Key></Deleted>"));
                }
                out.push_str("</DeleteResult>");
                out
            }
            other => panic!("unexpected FOS {other} {}", request.key),
        };
        Ok(FosResponse {
            cdn_cache: None,
            status: 200,
            body: body.into_bytes(),
            retry_after: None,
        })
    }
}

fn service() -> Service {
    serde_json::from_value(json!({
        "service_id": SID, "name": "n", "status": "active", "created_at": 0, "updated_at": 0,
        "config_version": 1, "logging_enabled": true, "logging_endpoint_name": "e",
        "log_period": 60, "sample_rate": 1, "edge_only": false, "custom_condition": "",
        "fos_bucket": "fos-abcdefghijklmnopqrst12-logs", "fos_prefix": "", "fos_region": "us-east-1",
        "fos_endpoint": "x", "log_fields": [], "field_limits": {}, "fastly_token_ref": "t",
        "fos_access_key_ref": "ak", "fos_secret_key_ref": "sk", "owner": "o",
        "bucket_adopted": false, "bucket_checked": false, "last_activated_version": null,
        "last_reconcile": null, "pending_job_id": null
    }))
    .unwrap()
}

#[test]
fn a_sweep_that_lists_and_deletes_in_one_execution_survives_a_lagging_read_of_its_own_state() {
    let fake = LaggingKv::default();
    for hour in 0..3 {
        fake.bucket.borrow_mut().insert(
            format!(
                "processed/ops/cron_runs/year=2027/month=01/day=10/hour={hour:02}/part.parquet"
            ),
            100,
        );
    }
    let live = "processed/ops/cron_runs/year=2027/month=01/day=15/hour=07/part.parquet";
    fake.bucket.borrow_mut().insert(live.into(), 100);
    let service = service();
    let store = ingest::storage::Store {
        runtime: &fake,
        service: &service,
        access_key: "ak".into(),
        secret_key: "sk".into(),
        started: Instant::now(),
        vcpu_start: 0,
    };
    let cutoff = BASE as i64 - 4 * 86_400; // 2027-01-11T08:00Z
    fake.new_execution();
    let report = ingest::retention::history_sweep(
        &store,
        LogKind::CronRuns,
        cutoff,
        &OperationalLogSettings::default(),
        0,
    )
    .expect("the sweep's own earlier write must not make its later write conflict");
    assert_eq!(report.deleted, 3);
    assert!(!report.more_pending);
    assert_eq!(fake.bucket.borrow().keys().collect::<Vec<_>>(), vec![live]);
    let state = &fake.kv.borrow()["history:cron_runs"].1;
    assert_eq!(state["pending"], json!([]), "{state}");
}

#[test]
fn cleanup_is_bounded_and_resumes_across_executions() {
    use fastly_log_analytics::admin::{reset, DataReset, ResetPhase};
    use ingest::minute_dir;
    let fake = LaggingKv::default();
    fake.vcpu_per_fos.set(60);
    let reset_at = BASE - 3600;
    let cas_at = reset_at + 450;
    let floor = format!("raw/request/{}", minute_dir(reset_at as i64));
    let manifest = "processed/_catalog/reset.json".to_owned();
    let mut old = Vec::new();
    for i in 0..10_000u64 {
        let minute = reset_at - 60 * (1 + i % 50);
        old.push(format!(
            "raw/request/{}{i:05}.zst",
            minute_dir(minute as i64)
        ));
    }
    for i in 0..10_000 {
        old.push(format!("processed/request/day=1/old-{i:05}.parquet"));
    }
    for i in 0..5_000 {
        old.push(format!("processed/_catalog/old-{i:05}.json"));
    }
    let mut kept = vec![
        format!("{floor}new.zst"),
        format!("raw/request/{}new.zst", minute_dir(reset_at as i64 + 60)),
        manifest.clone(),
    ];
    for i in 0..5 {
        kept.push(format!("processed/request/day=2/new-{i}.parquet"));
    }
    {
        let (mut bucket, mut mtimes) = (fake.bucket.borrow_mut(), fake.mtimes.borrow_mut());
        for key in old.iter().chain(&kept) {
            bucket.insert(key.clone(), 100);
            let at = if key.contains("/new") {
                cas_at + 100
            } else {
                cas_at - 100
            };
            mtimes.insert(key.clone(), at);
        }
    }
    let mut service = service();
    service.data_reset = Some(DataReset {
        job_id: "j1".into(),
        reset_at,
        phase: ResetPhase::Cleanup,
        started_at: reset_at,
        actor: "admin".into(),
        rum_held: false,
    });
    let put = |key: &str, value: Value| {
        fake.kv.borrow_mut().insert(key.into(), (1, value));
    };
    put(
        &format!("svc:{SID}"),
        serde_json::to_value(&service).unwrap(),
    );
    put(
        "ingest:watermark",
        json!({"floor_key": floor, "at": cas_at, "catalog": {"version": 2, "generation": 5,
            "manifest": manifest, "recent": [{"cursor": format!("{SID}:5"), "generation": 5,
            "committed_at": null, "manifest": manifest}]}}),
    );
    put(
        reset::JOB_KEY,
        json!({"job_id": "j1", "status": "cleanup", "reset_at": reset_at, "started_at": reset_at,
            "reset_cas_at": cas_at, "reset_manifest": manifest,
            "steps_done": ["rum_watermark", "rum_released", "keys", "deleteq", "quarantine"]}),
    );
    let store = ingest::storage::Store {
        runtime: &fake,
        service: &service,
        access_key: "ak".into(),
        secret_key: "sk".into(),
        started: Instant::now(),
        vcpu_start: 0,
    };
    let (mut lost, mut vcpu_bound, mut job) = (0, false, Value::Null);
    for execution in 0..100 {
        fake.new_execution();
        let before = fake.bucket.borrow().len();
        if execution == 2 {
            *fake.drop_cas.borrow_mut() = Some(reset::JOB_KEY.into());
        }
        let result = reset::advance(&store).expect("the marker drives the step");
        if execution == 2 {
            assert_eq!(result, Err("data_reset_conflict".into()));
            lost = before - fake.bucket.borrow().len();
            assert!(lost > 0, "the dropped execution deleted objects");
        } else {
            result.unwrap();
        }
        assert!(fake.calls.get() <= 32);
        assert!(fake.vcpu.get() <= 700 + 2 * 60, "{}", fake.vcpu.get());
        vcpu_bound |= fake.vcpu.get() >= 700 && fake.calls.get() < 28;
        job = fake.kv.borrow()[reset::JOB_KEY].1.clone();
        if job["status"] == "complete" {
            break;
        }
        assert_eq!(job["status"], "cleanup", "{job}");
        fake.clock.set(fake.clock.get() + 60);
    }
    assert_eq!(job["status"], "complete", "{job}");
    assert!(vcpu_bound, "some execution stopped at the vCPU bound");
    let left: Vec<String> = fake.bucket.borrow().keys().cloned().collect();
    kept.sort();
    assert_eq!(left, kept);
    let counted: u64 = job["deleted"]
        .as_object()
        .unwrap()
        .values()
        .map(|n| n.as_u64().unwrap())
        .sum();
    assert_eq!(counted, old.len() as u64 - lost as u64, "{job}");
    assert_eq!(job["bytes"], counted * 100);
    assert!(fake.kv.borrow()[&format!("svc:{SID}")].1["data_reset"].is_null());
}

#[test]
fn a_retention_sweep_after_an_earlier_stage_stops_at_the_execution_vcpu_bound() {
    use fastly_log_analytics::telemetry::LogKind;
    let fake = LaggingKv::default();
    fake.vcpu_per_fos.set(30);
    for kind in LogKind::ALL {
        for day in 1..4 {
            fake.bucket.borrow_mut().insert(
                format!(
                    "processed/ops/{}/year=2026/month=01/day={day:02}/hour=00/part.parquet",
                    kind.as_str()
                ),
                100,
            );
        }
    }
    let service = service();
    let store = ingest::storage::Store {
        runtime: &fake,
        service: &service,
        access_key: "ak".into(),
        secret_key: "sk".into(),
        started: Instant::now(),
        vcpu_start: 0,
    };
    fake.new_execution();
    // A Delete Data step earlier in the same execution already spent this much.
    fake.vcpu.set(600);
    let report = ingest::retention::retention_sweep_from(
        &store,
        &OperationalLogSettings::default(),
        1,
        None,
        0,
    )
    .unwrap();
    assert!(report.deleted > 0);
    assert!(report.more_pending, "{report:?}");
    assert!(fake.vcpu.get() <= 700 + 2 * 30, "{}", fake.vcpu.get());
}
