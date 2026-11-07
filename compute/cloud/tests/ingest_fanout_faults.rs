//! Fault cases for ingest fan-out (coordinator + `/_internal/ingest` worker). The fake runtime
//! runs the real worker in-process with a fresh 32-call budget per worker execution, stores FOS
//! credentials the way provisioning does, and charges the dispatch's worker-secret read.
#![allow(dead_code)]

use arrow_array::{Array, RecordBatch, StringArray};
use bytes::Bytes;
use fastly_log_analytics::{
    admin::{FosRequest, FosResponse, Record, Service},
    ingest,
    setup::SetupStores,
    shell::{CoreCacheEntry, CoreCacheError, KvPage, Runtime},
    telemetry::{self, usage::UsageAgg, LogBuffer, LogKind},
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{json, Value};
use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, BTreeSet},
    io::Write,
    time::Instant,
};

const BASE: i64 = 1_800_000_000; // 2027-01-15T08:00:00Z
const SID: &str = "AbCdEfGhIjKlMnOpQrSt12";
const BUDGET: u32 = 32;
const AK: &str = "AKIDFANOUT";
const SK: &str = "real-secret-key";

/// Runs once inside the next `worker_dispatch`, while the coordinator waits on its workers.
type DispatchHook = Box<dyn FnOnce(&Fake)>;

struct Fake {
    kv: RefCell<BTreeMap<String, (u64, Value)>>,
    bucket: RefCell<BTreeMap<String, Vec<u8>>>,
    calls: Cell<u32>,
    secrets_read: RefCell<BTreeSet<String>>,
    now: Cell<u64>,
    window: RefCell<Option<String>>,
    puts: Cell<u32>,
    delete_posts: RefCell<Vec<usize>>,
    delete_status: Cell<u16>,
    worker_calls: RefCell<Vec<usize>>,
    fail_delete_keys: RefCell<BTreeSet<String>>,
    fail_put_number: Cell<Option<u32>>,
    fail_watermark: Cell<bool>,
    cdn_gets: RefCell<Vec<String>>,
    api_calls: RefCell<Vec<(String, String)>>,
    kv_reads: RefCell<Vec<String>>,
    seq: std::cell::Cell<u64>,
    cache: RefCell<BTreeMap<String, CoreCacheEntry>>,
    vcpu: Cell<u64>,
    elapsed_s: Cell<u64>,
    usage: UsageAgg,
    /// Calls the coordinator (main execution) may make before every further call fails, to
    /// simulate the execution dying at that point.
    crash_after: Cell<Option<u32>>,
    /// Per dispatched ingest request, in order: how the fake worker transport behaves.
    worker_faults: RefCell<std::collections::VecDeque<WorkerFault>>,
    worker_max_calls: Cell<u32>,
    worker_runs: Cell<u32>,
    in_worker: Cell<bool>,
    bad_etag_for: RefCell<Option<String>>,
    fail_kv_delete: Cell<bool>,
    reserve: Cell<u32>,
    /// Runs once, inside the next dispatch, before any worker: another tick acting meanwhile.
    on_dispatch: RefCell<Option<DispatchHook>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WorkerFault {
    Ok,
    /// The request never reaches the worker.
    Unreachable,
    /// The worker runs (and writes) but its answer is lost.
    LostReply,
    /// The worker answers with a result stamped with another attempt.
    StaleAttempt,
    /// The worker only gets to run after its plan lease has expired.
    Late,
}

impl Fake {
    fn new(now: i64) -> Self {
        Self {
            kv: Default::default(),
            bucket: Default::default(),
            calls: Cell::new(0),
            secrets_read: Default::default(),
            now: Cell::new(now as u64),
            window: Default::default(),
            puts: Cell::new(0),
            delete_posts: Default::default(),
            delete_status: Cell::new(200),
            worker_calls: Default::default(),
            fail_delete_keys: Default::default(),
            fail_put_number: Cell::new(None),
            fail_watermark: Cell::new(false),
            cdn_gets: Default::default(),
            api_calls: Default::default(),
            kv_reads: Default::default(),
            seq: Default::default(),
            cache: Default::default(),
            vcpu: Cell::new(0),
            elapsed_s: Cell::new(0),
            usage: UsageAgg::default(),
            crash_after: Cell::new(None),
            worker_faults: Default::default(),
            worker_max_calls: Cell::new(0),
            worker_runs: Cell::new(0),
            in_worker: Cell::new(false),
            bad_etag_for: Default::default(),
            fail_kv_delete: Cell::new(false),
            reserve: Cell::new(0),
            on_dispatch: Default::default(),
        }
    }
    fn charge(&self) -> Result<(), String> {
        if self.calls.get() >= BUDGET {
            return Err("backend_budget_exhausted".into());
        }
        if !self.in_worker.get()
            && self
                .crash_after
                .get()
                .is_some_and(|n| self.calls.get() >= n)
        {
            return Err("injected_crash".into());
        }
        self.calls.set(self.calls.get() + 1);
        Ok(())
    }
    fn new_request(&self) {
        self.calls.set(0);
        self.vcpu.set(0);
        self.secrets_read.borrow_mut().clear();
    }
    fn keys(&self, prefix: &str) -> Vec<String> {
        self.bucket
            .borrow()
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect()
    }
}

impl SetupStores for Fake {
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

impl Runtime for Fake {
    fn config(&self, name: &str) -> Result<Option<String>, String> {
        Ok(if name == "window_s" {
            self.window.borrow().clone()
        } else if name == "dev_telemetry" {
            Some("1".into())
        } else {
            Some("v".into())
        })
    }
    fn secret(&self, name: &str) -> Result<Option<String>, String> {
        if self.secrets_read.borrow_mut().insert(name.into()) {
            self.charge()?;
        }
        Ok(Some(
            match name {
                "cdn_key" => "ck",
                "worker_secret" => "wk",
                name if name.starts_with("fos_ak.") => AK,
                // Provisioning stores the secret key inside a JSON envelope (admin/fos.rs).
                name if name.starts_with("fos_sk.") => {
                    return Ok(Some(
                        json!({"access_key": AK, "secret_key": SK}).to_string(),
                    ))
                }
                _ => "other-secret",
            }
            .into(),
        ))
    }
    fn random(&self) -> Result<[u8; 32], String> {
        let n = self.seq.get() + 1;
        self.seq.set(n);
        let mut bytes = [7u8; 32];
        bytes[..8].copy_from_slice(&n.to_be_bytes());
        Ok(bytes)
    }
    fn now(&self) -> u64 {
        self.now.get()
    }
    fn checks(&self, _: bool) -> Result<Value, String> {
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
    fn admin_get(&self, key: &str) -> Result<Option<Record>, String> {
        self.charge()?;
        self.kv_reads.borrow_mut().push(key.into());
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
        self.charge()?;
        if key == ingest::WATERMARK_KEY && self.fail_watermark.get() {
            return Err("injected_watermark_failure".into());
        }
        let mut kv = self.kv.borrow_mut();
        let current = kv.get(key).map(|(g, _)| g.to_string());
        if current.as_deref() != generation {
            return Ok(false);
        }
        let next = kv.get(key).map_or(1, |(g, _)| g + 1);
        kv.insert(key.into(), (next, value.clone()));
        Ok(true)
    }
    fn admin_fos(&self, request: FosRequest<'_>) -> Result<FosResponse, String> {
        self.charge()?;
        if (request.access_key, request.secret_key) != (AK, SK) {
            // SigV4 signed with anything but the real secret key.
            return Ok(FosResponse {
                cdn_cache: None,
                status: 403,
                body: vec![],
                retry_after: None,
            });
        }
        match request.method {
            "PUT" => {
                let n = self.puts.get() + 1;
                self.puts.set(n);
                if self.fail_put_number.get() == Some(n) {
                    return Err("injected_put_failure".into());
                }
                self.bucket
                    .borrow_mut()
                    .insert(request.key.into(), request.body);
                Ok(FosResponse {
                    cdn_cache: None,
                    status: 200,
                    body: vec![],
                    retry_after: None,
                })
            }
            "GET" if request.query.contains("list-type") => Ok(FosResponse {
                cdn_cache: None,
                status: 200,
                body: self.list(request.query).into_bytes(),
                retry_after: None,
            }),
            "GET" => Ok(match self.bucket.borrow().get(request.key) {
                Some(body) => FosResponse {
                    cdn_cache: None,
                    status: 200,
                    body: body.clone(),
                    retry_after: None,
                },
                None => FosResponse {
                    cdn_cache: None,
                    status: 404,
                    body: vec![],
                    retry_after: None,
                },
            }),
            "DELETE" => {
                self.bucket.borrow_mut().remove(request.key);
                Ok(FosResponse {
                    cdn_cache: None,
                    status: 204,
                    body: vec![],
                    retry_after: None,
                })
            }
            "POST" if request.query == "delete" => {
                let xml = String::from_utf8(request.body).unwrap();
                let keys: Vec<String> = xml
                    .split("<Key>")
                    .skip(1)
                    .map(|part| part.split("</Key>").next().unwrap().to_owned())
                    .collect();
                self.delete_posts.borrow_mut().push(keys.len());
                if self.delete_status.get() != 200 {
                    return Ok(FosResponse {
                        cdn_cache: None,
                        status: self.delete_status.get(),
                        body: vec![],
                        retry_after: None,
                    });
                }
                let mut out = String::from("<DeleteResult>");
                for key in keys {
                    if self.fail_delete_keys.borrow().contains(&key) {
                        out.push_str(&format!(
                            "<Error><Key>{key}</Key><Code>InternalError</Code></Error>"
                        ));
                    } else {
                        self.bucket.borrow_mut().remove(&key);
                        out.push_str(&format!("<Deleted><Key>{key}</Key></Deleted>"));
                    }
                }
                out.push_str("</DeleteResult>");
                Ok(FosResponse {
                    cdn_cache: None,
                    status: 200,
                    body: out.into_bytes(),
                    retry_after: None,
                })
            }
            _ => Ok(FosResponse {
                cdn_cache: None,
                status: 405,
                body: vec![],
                retry_after: None,
            }),
        }
    }
    fn admin_cdn_get(
        &self,
        service_id: &str,
        key: &str,
        cdn_key: &str,
        _max: usize,
        _timeout: std::time::Duration,
    ) -> Result<FosResponse, String> {
        self.charge()?;
        assert_eq!(cdn_key, "ck");
        assert_eq!(service_id, SID);
        self.cdn_gets.borrow_mut().push(key.into());
        Ok(match self.bucket.borrow().get(key) {
            Some(body) => FosResponse {
                cdn_cache: None,
                status: 200,
                body: body.clone(),
                retry_after: None,
            },
            None => FosResponse {
                cdn_cache: None,
                status: 404,
                body: vec![],
                retry_after: None,
            },
        })
    }
    fn admin_api(
        &self,
        request: fastly_log_analytics::admin::ApiRequest<'_>,
    ) -> Result<fastly_log_analytics::admin::ApiResponse, String> {
        self.charge()?;
        self.api_calls
            .borrow_mut()
            .push((request.method.into(), request.path.into()));
        Ok(fastly_log_analytics::admin::ApiResponse {
            status: 200,
            body: json!({"status": "ok"}),
            retry_after: None,
        })
    }
    fn admin_remaining_calls(&self) -> u32 {
        BUDGET - self.calls.get()
    }
    fn admin_elapsed(&self, _: Instant) -> std::time::Duration {
        std::time::Duration::from_secs(self.elapsed_s.get())
    }
    fn vcpu_ms(&self) -> u64 {
        self.vcpu.set(self.vcpu.get() + 1);
        self.vcpu.get()
    }
    fn pop(&self) -> Option<String> {
        Some("IAD".into())
    }
    fn on_log_shield(&self) -> bool {
        true
    }
    fn usage_agg(&self) -> Option<&UsageAgg> {
        Some(&self.usage)
    }
    fn core_cache_replace(
        &self,
        key: &str,
        _surrogate_keys: &[&str],
        _ttl: std::time::Duration,
        update: &dyn Fn(Option<CoreCacheEntry>) -> CoreCacheEntry,
    ) -> Result<(), CoreCacheError> {
        let existing = self.cache.borrow().get(key).cloned();
        self.cache.borrow_mut().insert(key.into(), update(existing));
        Ok(())
    }
    fn core_cache_lookup(&self, key: &str) -> Result<Option<CoreCacheEntry>, CoreCacheError> {
        Ok(self.cache.borrow().get(key).cloned())
    }
    fn kv_list(&self, prefix: &str, cursor: Option<&str>, limit: u32) -> Result<KvPage, String> {
        self.charge()?;
        self.kv_reads.borrow_mut().push(format!("list:{prefix}"));
        let kv = self.kv.borrow();
        Ok(KvPage::from_sorted_keys(
            kv.keys().cloned(),
            prefix,
            cursor,
            limit,
        ))
    }
    fn kv_delete(&self, key: &str) -> Result<(), String> {
        self.charge()?;
        if self.fail_kv_delete.get() {
            return Err("injected_kv_delete_failure".into());
        }
        self.kv.borrow_mut().remove(key);
        Ok(())
    }
    fn admin_fos_etag(
        &self,
        request: FosRequest<'_>,
    ) -> Result<(FosResponse, Option<String>), String> {
        use md5::{Digest, Md5};
        let bad = self
            .bad_etag_for
            .borrow()
            .as_deref()
            .is_some_and(|pattern| request.key.contains(pattern));
        let tag = if bad {
            "\"00000000000000000000000000000000\"".to_owned()
        } else {
            format!("\"{}\"", hex::encode(Md5::digest(&request.body)))
        };
        self.admin_fos(request)
            .map(|response| (response, Some(tag)))
    }
    fn worker_dispatch(
        &self,
        requests: &[(String, Value)],
        _deadline: std::time::Duration,
    ) -> Vec<Result<Value, String>> {
        let mut out = Vec::new();
        // Like the SDK: the first dispatch of an execution reads the worker secret.
        if let Err(error) = self.secret("worker_secret") {
            return requests.iter().map(|_| Err(error.clone())).collect();
        }
        for (path, body) in requests {
            if let Err(error) = self.charge() {
                out.push(Err(error));
                continue;
            }
            if path != ingest::worker::PATH {
                out.push(Ok(json!({"path": path, "echo": body})));
                continue;
            }
            let is_maintenance = body.get("maintenance").is_some();
            let fault = if is_maintenance {
                WorkerFault::Ok
            } else {
                self.worker_faults
                    .borrow_mut()
                    .pop_front()
                    .unwrap_or(WorkerFault::Ok)
            };
            if fault == WorkerFault::Unreachable {
                out.push(Err("worker_deadline".into()));
                continue;
            }
            let request = fastly_log_analytics::http::Request {
                method: "POST".into(),
                path: path.clone(),
                headers: [("x-flp-worker-key".to_owned(), "wk".to_owned())].into(),
                body: body.to_string().into_bytes(),
            };
            // A fresh execution: its own 32-call budget and Secret Store reads.
            let spent = self.calls.replace(0);
            let vcpu = self.vcpu.replace(0);
            let secrets = std::mem::take(&mut *self.secrets_read.borrow_mut());
            self.in_worker.set(true);
            let now = self.now.get();
            if fault == WorkerFault::Late {
                // The worker runs long after the lease it was dispatched under.
                self.now.set(now + ingest::coordinator::LEASE_S);
            }
            let response = ingest::worker::handle_worker_ingest(self, &request);
            self.now.set(now);
            self.in_worker.set(false);
            let used = self.calls.replace(spent);
            self.vcpu.set(vcpu);
            *self.secrets_read.borrow_mut() = secrets;
            assert!(used <= BUDGET, "worker used {used} calls");
            self.worker_max_calls
                .set(self.worker_max_calls.get().max(used));
            if is_maintenance {
                if let Some(hook) = self.on_dispatch.borrow_mut().take() {
                    hook(self);
                }
            } else {
                self.worker_runs.set(self.worker_runs.get() + 1);
            }
            if fault == WorkerFault::LostReply {
                out.push(Err("worker_deadline".into()));
            } else if response.status != 200 {
                out.push(Err(format!("worker_http_{}", response.status)));
            } else {
                let mut reply: Value = serde_json::from_slice(&response.body).unwrap();
                if fault == WorkerFault::StaleAttempt && reply["result"].is_object() {
                    let attempt = reply["result"]["attempt"].as_u64().unwrap();
                    reply["result"]["attempt"] = (attempt + 1).into();
                }
                out.push(Ok(reply));
            }
        }
        out
    }
    fn worker_delete(&self, body: &Value) -> Result<Value, String> {
        self.charge()?;
        self.worker_calls
            .borrow_mut()
            .push(body["keys"].as_array().map_or(0, Vec::len));
        let request = fastly_log_analytics::http::Request {
            method: "POST".into(),
            path: "/_internal/delete".into(),
            headers: [("x-flp-worker-key".to_owned(), "wk".to_owned())].into(),
            body: body.to_string().into_bytes(),
        };
        let spent = self.calls.replace(0);
        let response = ingest::retention::handle_worker_delete(self, &request);
        self.calls.set(spent);
        if response.status != 200 {
            return Err(format!("worker_http_{}", response.status));
        }
        serde_json::from_slice(&response.body).map_err(|e| e.to_string())
    }
}

impl Fake {
    fn list(&self, query: &str) -> String {
        let q: BTreeMap<String, String> = url::form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .collect();
        assert_eq!(q.get("list-type").map(String::as_str), Some("2"));
        let prefix = q.get("prefix").cloned().unwrap_or_default();
        let max: usize = q
            .get("max-keys")
            .and_then(|m| m.parse().ok())
            .unwrap_or(1000);
        let after = q
            .get("continuation-token")
            .or_else(|| q.get("start-after"))
            .cloned();
        let delimiter = q.get("delimiter").cloned();
        let mut contents = String::new();
        let mut prefixes = BTreeSet::new();
        let (mut count, mut truncated, mut last) = (0, false, String::new());
        for (key, body) in self.bucket.borrow().iter() {
            if !key.starts_with(&prefix) || after.as_ref().is_some_and(|a| key <= a) {
                continue;
            }
            if let Some(d) = &delimiter {
                if let Some(i) = key[prefix.len()..].find(d.as_str()) {
                    prefixes.insert(key[..prefix.len() + i + d.len()].to_owned());
                    continue;
                }
            }
            if count == max {
                truncated = true;
                break;
            }
            count += 1;
            last = key.clone();
            contents.push_str(&format!(
                "<Contents><Key>{key}</Key><Size>{}</Size><LastModified>2027-01-15T08:00:00.000Z</LastModified></Contents>",
                body.len()
            ));
        }
        let prefixes: String = prefixes
            .iter()
            .map(|p| format!("<CommonPrefixes><Prefix>{p}</Prefix></CommonPrefixes>"))
            .collect();
        let token = if truncated {
            format!("<NextContinuationToken>{last}</NextContinuationToken>")
        } else {
            String::new()
        };
        format!(
            "<?xml version=\"1.0\"?><ListBucketResult><IsTruncated>{truncated}</IsTruncated>{contents}{prefixes}{token}</ListBucketResult>"
        )
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

fn iso(seconds: i64) -> String {
    ingest::rfc3339(seconds).replace("Z", "+0000")
}

fn raw_key(minute_start: i64, name: &str) -> String {
    let t = time::OffsetDateTime::from_unix_timestamp(minute_start).unwrap();
    format!(
        "raw/request/year={:04}/month={:02}/day={:02}/hour={:02}/minute={:02}/{name}.zst",
        t.year(),
        t.month() as u8,
        t.day(),
        t.hour(),
        t.minute()
    )
}

fn gz(lines: &[String]) -> Vec<u8> {
    let mut text = Vec::new();
    text.write_all(lines.join("\n").as_bytes()).unwrap();
    text.write_all(b"\n").unwrap();
    zstd::stream::encode_all(text.as_slice(), 1).unwrap()
}

fn line(at: i64, id: &str) -> String {
    json!({"timestamp": iso(at), "ip": id, "status": 200}).to_string()
}

fn put_raw(fake: &Fake, minute_start: i64, name: &str, lines: &[String]) -> String {
    let key = raw_key(minute_start, name);
    fake.bucket.borrow_mut().insert(key.clone(), gz(lines));
    key
}

fn tick(fake: &Fake) -> Result<ingest::Report, String> {
    fake.new_request();
    let buffer = LogBuffer::new(SID, "req");
    let report = ingest::run_capped(
        fake,
        &service(),
        AK,
        SK,
        Instant::now(),
        &buffer,
        fake.reserve.get(),
    );
    telemetry::flush_request_buffer(fake, &buffer);
    report
}

fn rows(fake: &Fake, kind: LogKind) -> Vec<Value> {
    fake.new_request();
    telemetry::read_live(fake, SID, kind, None, Some(2000), None)
        .unwrap()
        .rows
}

fn batches(bytes: &[u8]) -> Vec<RecordBatch> {
    ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))
        .unwrap()
        .build()
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn processed_ips(fake: &Fake) -> Vec<String> {
    let mut ips = Vec::new();
    for key in fake.keys("processed/request/") {
        for batch in batches(&fake.bucket.borrow()[&key]) {
            let col = batch
                .column_by_name("ip")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            ips.extend((0..col.len()).map(|i| col.value(i).to_owned()));
        }
    }
    ips.sort();
    ips
}

fn plan(fake: &Fake) -> Option<Value> {
    fake.kv
        .borrow()
        .get(ingest::PLAN_KEY)
        .map(|(_, value)| value.clone())
}

fn committed(fake: &Fake) -> bool {
    let kv = fake.kv.borrow();
    let watermark = kv
        .get(ingest::WATERMARK_KEY)
        .map(|(_, v)| v["run_id"].clone());
    kv.get(ingest::PLAN_KEY)
        .is_none_or(|(_, p)| watermark.as_ref() == Some(&p["plan_id"]))
}

/// Ticks two minutes apart (the clock bucket), so a lease left by the previous tick has expired.
fn tick_later(fake: &Fake) -> Result<ingest::Report, String> {
    fake.now.set(fake.now.get() + 120);
    tick(fake)
}

fn seed(fake: &Fake, minutes: i64, per_minute: i64) -> Vec<String> {
    let mut expected = Vec::new();
    for m in 0..minutes {
        let minute = BASE + m * 60;
        for i in 0..per_minute {
            let id = format!("m{m:03}o{i:03}");
            put_raw(
                fake,
                minute,
                &format!("o{i:03}"),
                &[line(minute + i % 60, &id)],
            );
            expected.push(id);
        }
    }
    expected.sort();
    expected
}

fn store<'a>(fake: &'a Fake, service: &'a Service) -> ingest::storage::Store<'a, Fake> {
    ingest::storage::Store {
        runtime: fake,
        service,
        access_key: AK.into(),
        secret_key: SK.into(),
        started: Instant::now(),
        vcpu_start: 0,
    }
}

fn delete_tick(fake: &Fake) -> ingest::retention::DeleteReport {
    fake.new_request();
    let buffer = LogBuffer::new(SID, "del");
    let report =
        ingest::retention::raw_delete(&store(fake, &service()), &buffer, 4, &Default::default())
            .unwrap();
    telemetry::flush_request_buffer(fake, &buffer);
    report
}

fn edit_plan(fake: &Fake, edit: impl FnOnce(&mut Value)) {
    let mut kv = fake.kv.borrow_mut();
    let (generation, value) = kv.get_mut(ingest::PLAN_KEY).unwrap();
    *generation += 1;
    edit(value);
}

fn deleteq_keys(fake: &Fake) -> Vec<String> {
    fake.kv
        .borrow()
        .keys()
        .filter(|k| k.starts_with(ingest::DELETEQ_PREFIX))
        .cloned()
        .collect()
}

fn ingest_files_once(fake: &Fake, raw: &[String]) {
    let files = rows(fake, LogKind::IngestFiles);
    let keys: Vec<&str> = files
        .iter()
        .filter(|r| r["state_version"] == 1)
        .map(|r| r["raw_key"].as_str().unwrap())
        .collect();
    let distinct: BTreeSet<&str> = keys.iter().copied().collect();
    assert_eq!(keys.len(), distinct.len(), "duplicate ingest_files rows");
    assert_eq!(
        distinct,
        raw.iter().map(String::as_str).collect::<BTreeSet<_>>()
    );
}

/// Ticks two minutes apart until the plan commits and nothing is pending.
fn drain(fake: &Fake) {
    for _ in 0..12 {
        let report = tick_later(fake).unwrap();
        if committed(fake) && !report.more_pending {
            return;
        }
    }
    panic!("ingest did not drain: {:?}", plan(fake));
}

fn abandon_with_schema_change(fake: &Fake) {
    edit_plan(fake, |p| {
        p["schema"] = "0000000000000000".into();
        p["lease_until"] = 0.into();
    });
    let report = tick_later(fake).unwrap();
    assert!(!report.busy);
    assert_eq!(
        plan(fake).unwrap()["abandoning"],
        "schema_changed",
        "the plan is abandoned"
    );
}

#[test]
fn worker_signs_with_the_secret_key_unwrapped_from_its_envelope() {
    let fake = Fake::new(BASE + 3600);
    let expected = seed(&fake, 1, 3);
    let report = tick(&fake).unwrap();
    assert_eq!(report.converted, 3, "{report:?}");
    assert!(committed(&fake));
    assert_eq!(processed_ips(&fake), expected);
}

#[test]
fn worker_delete_signs_with_the_secret_key_unwrapped_from_its_envelope() {
    let fake = Fake::new(BASE);
    let key = "p/processed/request/year=2020/month=01/day=01/hour=00/window=a.parquet";
    fake.bucket.borrow_mut().insert(key.into(), vec![1]);
    let response = ingest::retention::handle_worker_delete(
        &fake,
        &fastly_log_analytics::http::Request {
            method: "POST".into(),
            path: "/_internal/delete".into(),
            headers: [("x-flp-worker-key".to_owned(), "wk".to_owned())].into(),
            body: json!({"service_id": SID, "region": "us-east-1",
                "bucket": "fos-abcdefghijklmnopqrst12-logs", "prefix": "p/", "keys": [key]})
            .to_string()
            .into_bytes(),
        },
    );
    assert_eq!(response.status, 200);
    let body: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(body["deleted"], json!([key]));
    assert!(!fake.bucket.borrow().contains_key(key));
}

#[test]
fn dispatch_budgets_the_worker_secret_read_and_leaves_the_reserve() {
    let fake = Fake::new(BASE + 3600);
    let expected = seed(&fake, 8, 22);
    let reserve = 14;
    fake.reserve.set(reserve);
    let report = tick(&fake).unwrap();
    assert!(report.shards_done > 0, "{report:?}");
    assert!(
        report.backend_calls + reserve + ingest::TICK_RESERVE <= BUDGET,
        "ingest used {} calls with reserve {reserve}",
        report.backend_calls
    );
    fake.reserve.set(0);
    drain(&fake);
    assert_eq!(processed_ips(&fake), expected);
}

#[test]
fn abandon_cleanup_leaves_the_reserve() {
    let fake = Fake::new(BASE + 3600);
    let expected = seed(&fake, 1, 3);
    fake.worker_faults
        .borrow_mut()
        .push_back(WorkerFault::LostReply);
    tick(&fake).unwrap();
    assert!(!fake.keys("processed/_inflight/").is_empty());
    abandon_with_schema_change(&fake);
    // Plan read, watermark read, lease CAS and the manifest LIST leave 28 calls.
    let reserve = 20;
    fake.reserve.set(reserve);
    let report = tick_later(&fake).unwrap();
    assert!(
        report.backend_calls + reserve + ingest::TICK_RESERVE <= BUDGET,
        "cleanup used {} calls with reserve {reserve}",
        report.backend_calls
    );
    fake.reserve.set(0);
    drain(&fake);
    assert_eq!(processed_ips(&fake), expected);
}

#[test]
fn a_result_from_another_attempt_is_not_recorded() {
    let fake = Fake::new(BASE + 3600);
    let expected = seed(&fake, 1, 3);
    fake.worker_faults
        .borrow_mut()
        .push_back(WorkerFault::StaleAttempt);
    tick(&fake).unwrap();
    assert!(!committed(&fake));
    let plan = plan(&fake).unwrap();
    assert_eq!(plan["results"], json!({}));
    assert_eq!(plan["failures"]["0"], 1);
    drain(&fake);
    assert_eq!(processed_ips(&fake), expected);
}

#[test]
fn raw_delete_skips_the_queue_of_a_plan_whose_commit_failed() {
    let fake = Fake::new(BASE + 3600);
    let expected = seed(&fake, 1, 3);
    let raw = fake.keys("raw/request/");
    fake.fail_watermark.set(true);
    fake.fail_kv_delete.set(true);
    assert!(tick(&fake).is_err());
    fake.fail_watermark.set(false);
    fake.fail_kv_delete.set(false);
    assert_eq!(deleteq_keys(&fake).len(), 1, "the orphan queue record");
    delete_tick(&fake);
    assert_eq!(
        fake.keys("raw/request/"),
        raw,
        "raw objects of an uncommitted plan"
    );

    // A release with another schema abandons the plan; its raw objects are converted again.
    abandon_with_schema_change(&fake);
    tick_later(&fake).unwrap();
    delete_tick(&fake);
    assert_eq!(fake.keys("raw/request/"), raw);
    drain(&fake);
    assert_eq!(processed_ips(&fake), expected);
    delete_tick(&fake);
    assert!(fake.keys("raw/request/").is_empty());
    assert!(fake.keys("processed/_inflight/").is_empty());
    ingest_files_once(&fake, &raw);
}

#[test]
fn a_plan_whose_shards_all_have_results_commits_despite_a_schema_change() {
    let fake = Fake::new(BASE + 3600);
    let expected = seed(&fake, 2, 22);
    fake.worker_faults
        .borrow_mut()
        .extend([WorkerFault::Ok, WorkerFault::Unreachable]);
    tick(&fake).unwrap();
    let before = plan(&fake).unwrap();
    assert_eq!(before["shards"].as_object().unwrap().len(), 2);
    assert!(before["results"]["0"].is_object());
    // Narrow the plan to its finished shard, as if it had only that one.
    let keys = before["shards"]["0"].clone();
    edit_plan(&fake, |p| {
        p["shards"] = json!({"0": keys.clone()});
        p["raw_keys"] = keys.clone();
        p["sizes"] = json!(vec![0; keys.as_array().unwrap().len()]);
        p["failures"] = json!({});
        p["schema"] = "0000000000000000".into();
        p["lease_until"] = 0.into();
    });
    let report = tick_later(&fake).unwrap();
    assert_eq!(report.converted, 22, "{report:?}");
    assert!(committed(&fake));
    assert_eq!(plan(&fake).unwrap()["plan_id"], before["plan_id"]);
    drain(&fake);
    assert_eq!(processed_ips(&fake), expected);
}

#[test]
fn a_tick_that_lost_its_lease_does_not_commit() {
    let fake = Fake::new(BASE + 3600);
    let expected = seed(&fake, 1, 3);
    *fake.on_dispatch.borrow_mut() = Some(Box::new(|fake: &Fake| {
        edit_plan(fake, |p| {
            p["attempt"] = (p["attempt"].as_u64().unwrap() + 1).into();
        });
    }));
    let report = tick(&fake).unwrap();
    assert!(report.busy);
    assert!(!fake.kv.borrow().contains_key(ingest::WATERMARK_KEY));
    assert!(deleteq_keys(&fake).is_empty());
    assert!(rows(&fake, LogKind::IngestFiles).is_empty());
    edit_plan(&fake, |p| p["lease_until"] = 0.into());
    drain(&fake);
    assert_eq!(processed_ips(&fake), expected);
}

#[test]
fn a_lost_watermark_race_keeps_the_winners_queue_and_writes_no_rows() {
    let fake = Fake::new(BASE + 3600);
    seed(&fake, 1, 3);
    let raw = fake.keys("raw/request/");
    *fake.on_dispatch.borrow_mut() = Some(Box::new(move |fake: &Fake| {
        let plan_id = plan(fake).unwrap()["plan_id"].clone();
        let mut kv = fake.kv.borrow_mut();
        kv.insert(
            format!("{}{}", ingest::DELETEQ_PREFIX, plan_id.as_str().unwrap()),
            (
                1,
                json!({"run_id": plan_id, "keys": raw, "sizes": [], "at": 0}),
            ),
        );
        kv.insert(
            ingest::WATERMARK_KEY.into(),
            (1, json!({"key": raw.last(), "run_id": plan_id, "at": 0})),
        );
    }));
    let report = tick(&fake).unwrap();
    assert!(report.busy);
    assert_eq!(deleteq_keys(&fake).len(), 1, "the winner's queue record");
    assert!(rows(&fake, LogKind::IngestFiles).is_empty());
}

#[test]
fn an_invalid_manifest_falls_back_to_a_token_sweep() {
    let fake = Fake::new(BASE + 3600);
    let expected = seed(&fake, 1, 3);
    fake.worker_faults
        .borrow_mut()
        .push_back(WorkerFault::LostReply);
    tick(&fake).unwrap();
    let old = plan(&fake).unwrap()["plan_id"].as_str().unwrap().to_owned();
    let manifests = fake.keys("processed/_inflight/");
    assert_eq!(manifests.len(), 1);
    fake.bucket
        .borrow_mut()
        .insert(manifests[0].clone(), b"not json".to_vec());
    abandon_with_schema_change(&fake);
    tick_later(&fake).unwrap();
    // Live rows age out of the read window, so check the log right after the cleanup tick.
    assert!(rows(&fake, LogKind::PlatformErrors)
        .iter()
        .any(|r| r["code"] == "ingest_manifest_invalid"));
    drain(&fake);
    assert_eq!(processed_ips(&fake), expected);
    assert!(fake
        .keys("processed/")
        .iter()
        .all(|k| !k.contains(&format!("-{old}s"))));
}

#[test]
fn a_worker_past_its_lease_writes_nothing() {
    let fake = Fake::new(BASE + 3600);
    let expected = seed(&fake, 1, 3);
    fake.worker_faults.borrow_mut().push_back(WorkerFault::Late);
    tick(&fake).unwrap();
    assert!(
        fake.keys("processed/").is_empty(),
        "{:?}",
        fake.keys("processed/")
    );
    assert!(!committed(&fake));
    drain(&fake);
    assert_eq!(processed_ips(&fake), expected);
}
