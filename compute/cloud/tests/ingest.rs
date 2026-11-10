use arrow_array::{Array, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray};
use bytes::Bytes;
use fastly_log_analytics::{
    admin::{FosRequest, FosResponse, Record, Service},
    ingest::{self, api},
    setup::SetupStores,
    shell::{CoreCacheEntry, CoreCacheError, KvPage, Runtime, PREFETCH_UNREAD},
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

#[path = "ingest/freshness.rs"]
mod freshness;

const BASE: i64 = 1_800_000_000; // 2027-01-15T08:00:00Z
const SID: &str = "AbCdEfGhIjKlMnOpQrSt12";
const BUDGET: u32 = 32;

/// A multipart upload's key and the parts received.
type Upload = (String, BTreeMap<u32, Vec<u8>>);

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
    conflict_watermark: Cell<bool>,
    fail_compact_after_publish: Cell<bool>,
    compact_conflict_after: Cell<Option<u32>>,
    stale_compact_state: RefCell<Option<Record>>,
    /// Each compaction state write leaves the next fresh read showing the record it replaced.
    lag_compact_reads: Cell<bool>,
    lagging_compact_state: RefCell<Option<Record>>,
    /// The injected compaction state conflict writes another writer's value, not the same one.
    foreign_compact_conflict: Cell<bool>,
    fail_fresh_compact_state: Cell<bool>,
    /// Compaction state CAS attempts.
    compact_state_writes: Cell<u32>,
    cdn_gets: RefCell<Vec<String>>,
    /// GET keys of each `admin_fos_many` round.
    fos_rounds: RefCell<Vec<Vec<String>>>,
    fos_concurrent: Cell<bool>,
    /// Keys of every direct object GET.
    fos_gets: RefCell<Vec<String>>,
    /// Sends `admin_fos_many`'s speculative reads but leaves them unread, as Fastly's runtime
    /// does with CDN reads.
    fos_defer: Cell<bool>,
    /// Deferred reads not read yet, with their response limits.
    fos_unread: RefCell<BTreeMap<String, usize>>,
    /// Most response-limit bytes ever outstanding in `fos_unread`.
    fos_unread_peak: Cell<usize>,
    range_reads: RefCell<Vec<(String, u64, usize)>>,
    source_bytes: Cell<u64>,
    api_calls: RefCell<Vec<(String, String)>>,
    /// Status every Fastly API call answers with.
    api_status: Cell<u16>,
    kv_reads: RefCell<Vec<String>>,
    seq: std::cell::Cell<u64>,
    cache: RefCell<BTreeMap<String, CoreCacheEntry>>,
    vcpu: Cell<u64>,
    vcpu_step: Cell<u64>,
    elapsed_s: Cell<u64>,
    /// Milliseconds each backend call advances `now` by, and the part of a second not yet added.
    call_ms: Cell<u64>,
    call_ms_carry: Cell<u64>,
    usage: UsageAgg,
    /// Calls the coordinator (main execution) may make before every further call fails, to
    /// simulate the execution dying at that point.
    crash_after: Cell<Option<u32>>,
    /// Per dispatched ingest request, in order: how the fake worker transport behaves.
    worker_faults: RefCell<std::collections::VecDeque<WorkerFault>>,
    worker_max_calls: Cell<u32>,
    worker_runs: Cell<u32>,
    /// Worker requests sent by the current main execution, and by every main execution so far.
    worker_sent: Cell<u32>,
    worker_sent_total: Cell<u32>,
    in_worker: Cell<bool>,
    bad_etag_for: RefCell<Option<String>>,
    /// Core Cache operations attempted in the current execution, and surrogate keys per entry.
    cache_ops: Cell<u32>,
    cache_tags: RefCell<BTreeMap<String, Vec<String>>>,
    /// Most backend calls and cache ops any one execution of each kind has used.
    peaks: RefCell<BTreeMap<&'static str, (u32, u32)>>,
    /// Most vCPU milliseconds any one execution of each kind has reported.
    vcpu_peaks: RefCell<BTreeMap<&'static str, u64>>,
    stage_runs: RefCell<Vec<Value>>,
    events: RefCell<Vec<Value>>,
    /// A `maint_next` another tick saves while the stage worker runs.
    race_maint_next: Cell<Option<u64>>,
    /// `cron:state` CAS attempts that lose to a concurrent writer before succeeding.
    race_cron_state: Cell<u32>,
    /// Whether this POP is the log shield (IAD), and `cron:state` CAS writes so far.
    shield: Cell<bool>,
    cron_cas: Cell<u32>,
    /// The FOS key secrets read as empty: an install not yet provisioned with credentials.
    no_fos: Cell<bool>,
    fos_envelope: Cell<bool>,
    /// Every `cron:state` read fails.
    fail_cron_state: Cell<bool>,
    /// LIST `LastModified` per key; others read as `BASE`.
    mtimes: RefCell<BTreeMap<String, i64>>,
    /// What `heap_mib` reports.
    heap_mib: Cell<Option<u32>>,
    linear_memory_high_water_bytes: Cell<Option<u64>>,
    /// Transient answers the next PUTs (`"PUT"`) or CDN GETs (`"GET"`) get instead of running,
    /// as `(method, status, Retry-After seconds)`.
    transient: RefCell<std::collections::VecDeque<(&'static str, u16, Option<u64>)>>,
    /// Every `admin_wait`, in milliseconds.
    waits: RefCell<Vec<u64>>,
    /// The `content_md5` each PUT carried, per key.
    put_md5s: RefCell<BTreeMap<String, Option<String>>>,
    /// Open multipart uploads by id: the key and the parts received.
    uploads: RefCell<BTreeMap<String, Upload>>,
    upload_seq: Cell<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WorkerFault {
    Ok,
    /// The request never reaches the worker.
    Unreachable,
    /// The worker runs (and writes) but its answer is lost.
    LostReply,
}

impl Fake {
    fn recorded_get(&self, key: &str) -> Option<Vec<u8>> {
        self.fos_gets.borrow_mut().push(key.to_owned());
        self.bucket.borrow().get(key).cloned()
    }

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
            conflict_watermark: Cell::new(false),
            fail_compact_after_publish: Cell::new(false),
            compact_conflict_after: Cell::new(None),
            stale_compact_state: Default::default(),
            lag_compact_reads: Cell::new(false),
            lagging_compact_state: Default::default(),
            foreign_compact_conflict: Cell::new(false),
            fail_fresh_compact_state: Cell::new(false),
            compact_state_writes: Cell::new(0),
            cdn_gets: Default::default(),
            fos_rounds: Default::default(),
            fos_concurrent: Cell::new(true),
            fos_gets: Default::default(),
            fos_defer: Cell::new(false),
            fos_unread: Default::default(),
            fos_unread_peak: Cell::new(0),
            range_reads: Default::default(),
            source_bytes: Cell::new(0),
            api_calls: Default::default(),
            api_status: Cell::new(200),
            kv_reads: Default::default(),
            seq: Default::default(),
            cache: Default::default(),
            vcpu: Cell::new(0),
            vcpu_step: Cell::new(1),
            elapsed_s: Cell::new(0),
            call_ms: Cell::new(0),
            call_ms_carry: Cell::new(0),
            usage: UsageAgg::default(),
            crash_after: Cell::new(None),
            worker_faults: Default::default(),
            worker_max_calls: Cell::new(0),
            worker_runs: Cell::new(0),
            worker_sent: Cell::new(0),
            worker_sent_total: Cell::new(0),
            in_worker: Cell::new(false),
            bad_etag_for: Default::default(),
            cache_ops: Cell::new(0),
            cache_tags: Default::default(),
            peaks: Default::default(),
            vcpu_peaks: Default::default(),
            stage_runs: Default::default(),
            events: Default::default(),
            race_maint_next: Cell::new(None),
            race_cron_state: Cell::new(0),
            shield: Cell::new(true),
            cron_cas: Cell::new(0),
            no_fos: Cell::new(false),
            fos_envelope: Cell::new(false),
            fail_cron_state: Cell::new(false),
            mtimes: Default::default(),
            heap_mib: Cell::new(None),
            linear_memory_high_water_bytes: Cell::new(None),
            transient: Default::default(),
            waits: Default::default(),
            put_md5s: Default::default(),
            uploads: RefCell::new(BTreeMap::new()),
            upload_seq: Cell::new(0),
        }
    }
    fn take_transient(&self, method: &str) -> Option<FosResponse> {
        let mut queue = self.transient.borrow_mut();
        let (_, status, retry_after) = queue.front().filter(|(m, _, _)| *m == method).copied()?;
        queue.pop_front();
        Some(FosResponse {
            cdn_cache: None,
            retry_after: retry_after.map(std::time::Duration::from_secs),
            status,
            body: if status == 503 {
                b"<Error><Code>SlowDown</Code></Error>".to_vec()
            } else {
                vec![]
            },
        })
    }
    fn cache_op(&self) -> Result<(), CoreCacheError> {
        self.cache_ops.set(self.cache_ops.get() + 1);
        if self.cache_ops.get() > fastly_log_analytics::shell::CACHE_OP_BUDGET {
            return Err(CoreCacheError::Failed("cache_op_budget_exhausted".into()));
        }
        Ok(())
    }
    /// Records the current execution's calls and cache ops under `kind`.
    fn sent_to_worker(&self) {
        self.worker_sent.set(self.worker_sent.get() + 1);
        if !self.in_worker.get() {
            self.worker_sent_total.set(self.worker_sent_total.get() + 1);
        }
    }
    fn note_peak(&self, kind: &'static str) {
        let mut peaks = self.peaks.borrow_mut();
        let peak = peaks.entry(kind).or_default();
        peak.0 = peak.0.max(self.calls.get());
        peak.1 = peak.1.max(self.cache_ops.get());
    }
    /// Runs `body` as a fresh execution with its own call budget, Secret Store reads and cache
    /// ops, then restores the caller's.
    fn execution<T>(&self, kind: &'static str, body: impl FnOnce() -> T) -> T {
        let spent = self.calls.replace(0);
        let bytes = self.source_bytes.replace(0);
        let vcpu = self.vcpu.replace(0);
        let ops = self.cache_ops.replace(0);
        let sent = self.worker_sent.replace(0);
        let secrets = std::mem::take(&mut *self.secrets_read.borrow_mut());
        let in_worker = self.in_worker.replace(true);
        let out = body();
        self.in_worker.set(in_worker);
        self.note_peak(kind);
        let mut peaks = self.vcpu_peaks.borrow_mut();
        let peak = peaks.entry(kind).or_default();
        *peak = (*peak).max(self.vcpu.get());
        drop(peaks);
        self.calls.set(spent);
        self.source_bytes.set(bytes);
        self.vcpu.set(vcpu);
        self.cache_ops.set(ops);
        self.worker_sent.set(sent);
        *self.secrets_read.borrow_mut() = secrets;
        out
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
        let ms = self.call_ms_carry.get() + self.call_ms.get();
        self.now.set(self.now.get() + ms / 1000);
        self.call_ms_carry.set(ms % 1000);
        Ok(())
    }
    fn new_request(&self) {
        self.calls.set(0);
        self.source_bytes.set(0);
        self.vcpu.set(0);
        self.worker_sent.set(0);
        self.cache_ops.set(0);
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
        if self.no_fos.get() && matches!(name, "ak" | "sk") {
            return Ok(Some(String::new()));
        }
        if self.fos_envelope.get() {
            if name.starts_with("fos_ak.") {
                return Ok(None);
            }
            if name.starts_with("fos_sk.") {
                return Ok(Some(
                    json!({"access_key": "fos-secret", "secret_key": "fos-secret"}).to_string(),
                ));
            }
        }
        Ok(Some(
            match name {
                "cdn_key" => "ck",
                "session_key" => "session-signing-material-for-the-cursor",
                _ => "fos-secret",
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
        if key == "cron:state" && self.fail_cron_state.get() {
            return Err("injected_cron_state_failure".into());
        }
        if key == ingest::compaction::STATE_KEY {
            if let Some(record) = self.stale_compact_state.borrow().as_ref() {
                return Ok(Some(record.clone()));
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
        self.charge()?;
        if key == ingest::WATERMARK_KEY && self.fail_watermark.get() {
            return Err("injected_watermark_failure".into());
        }
        if key == ingest::WATERMARK_KEY && self.conflict_watermark.get() {
            let mut kv = self.kv.borrow_mut();
            if let Some((g, v)) = kv.get(key).cloned() {
                kv.insert(key.into(), (g + 1, v));
            }
            return Ok(false);
        }
        if key == ingest::compaction::STATE_KEY {
            self.compact_state_writes
                .set(self.compact_state_writes.get() + 1);
        }
        if key == ingest::compaction::STATE_KEY
            && self.fail_compact_after_publish.get()
            && value["job"].is_null()
            && value["queue"]
                .as_array()
                .is_some_and(|queue| !queue.is_empty())
        {
            return Err("injected_post_publication_failure".into());
        }
        let mut kv = self.kv.borrow_mut();
        if key == ingest::compaction::STATE_KEY {
            if let Some(remaining) = self.compact_conflict_after.get() {
                if remaining == 0 {
                    self.compact_conflict_after.set(None);
                    if let Some((g, mut v)) = kv.get(key).cloned() {
                        if self.foreign_compact_conflict.get() {
                            v["lease_until"] = json!(u64::MAX);
                        }
                        kv.insert(key.into(), (g + 1, v));
                    }
                    return Ok(false);
                }
                self.compact_conflict_after.set(Some(remaining - 1));
            }
        }
        if key == "cron:state" {
            self.cron_cas.set(self.cron_cas.get() + 1);
        }
        if key == "cron:state" && generation.is_some() && self.race_cron_state.get() > 0 {
            self.race_cron_state.set(self.race_cron_state.get() - 1);
            if let Some((g, v)) = kv.get(key).cloned() {
                kv.insert(key.into(), (g + 1, v));
            }
            return Ok(false);
        }
        let current = kv.get(key).map(|(g, _)| g.to_string());
        if current.as_deref() != generation {
            return Ok(false);
        }
        let next = kv.get(key).map_or(1, |(g, _)| g + 1);
        if key == ingest::compaction::STATE_KEY && self.lag_compact_reads.get() {
            *self.lagging_compact_state.borrow_mut() = kv.get(key).map(|(g, v)| Record {
                generation: g.to_string(),
                value: v.clone(),
            });
        }
        kv.insert(key.into(), (next, value.clone()));
        Ok(true)
    }
    fn admin_get_fresh(&self, key: &str, token: &str) -> Result<Option<Record>, String> {
        assert_eq!(
            token, "fos-secret",
            "fresh KV confirmation requires the configured API token"
        );
        if key == ingest::compaction::STATE_KEY {
            self.charge()?;
            self.kv_reads.borrow_mut().push(key.into());
            if self.fail_fresh_compact_state.get() {
                return Err("injected_fresh_compact_state_failure".into());
            }
            if let Some(record) = self.lagging_compact_state.borrow_mut().take() {
                return Ok(Some(record));
            }
            return Ok(self.kv.borrow().get(key).map(|(g, v)| Record {
                generation: g.to_string(),
                value: v.clone(),
            }));
        }
        self.admin_get(key)
    }
    fn admin_fos_concurrent(&self) -> bool {
        self.fos_concurrent.get()
    }
    fn admin_fos_many(&self, requests: Vec<FosRequest<'_>>) -> Vec<Result<FosResponse, String>> {
        self.fos_rounds
            .borrow_mut()
            .push(requests.iter().map(|r| r.key.to_owned()).collect());
        if !self.fos_defer.get() {
            return requests.into_iter().map(|r| self.admin_fos(r)).collect();
        }
        let mut requests = requests.into_iter();
        let mut results: Vec<_> = requests
            .next()
            .map(|r| self.admin_fos(r))
            .into_iter()
            .collect();
        for request in requests {
            if !self.fos_unread.borrow().contains_key(request.key) && self.charge().is_ok() {
                self.fos_unread
                    .borrow_mut()
                    .insert(request.key.to_owned(), request.response_limit);
            }
            results.push(Err(PREFETCH_UNREAD.to_owned()));
        }
        let outstanding = self.fos_unread.borrow().values().sum();
        self.fos_unread_peak
            .set(self.fos_unread_peak.get().max(outstanding));
        results
    }
    fn release_unread(&self) {
        self.fos_unread.borrow_mut().clear();
    }
    fn admin_unread(&self, _: &str) -> BTreeMap<String, usize> {
        self.fos_unread.borrow().clone()
    }
    fn admin_fos(&self, request: FosRequest<'_>) -> Result<FosResponse, String> {
        let resumed = (request.method == "GET")
            .then(|| self.fos_unread.borrow_mut().remove(request.key))
            .flatten();
        if let Some(reserved) = resumed {
            assert!(
                request.response_limit <= reserved,
                "{} resumed at {} bytes past its {reserved} byte reservation",
                request.key,
                request.response_limit
            );
        } else {
            self.charge()?;
        }
        let query: BTreeMap<String, String> = url::form_urlencoded::parse(request.query.as_bytes())
            .into_owned()
            .collect();
        let ok = |status: u16, body: String| {
            Ok(FosResponse {
                cdn_cache: None,
                status,
                body: body.into_bytes(),
                retry_after: None,
            })
        };
        if let Some(id) = query.get("uploadId") {
            let mut uploads = self.uploads.borrow_mut();
            let Some((key, parts)) = uploads.get_mut(id) else {
                return ok(404, "<Error><Code>NoSuchUpload</Code></Error>".into());
            };
            assert_eq!(key, request.key);
            return match request.method {
                "PUT" => {
                    if let Some(response) = self.take_transient("PART") {
                        return Ok(response);
                    }
                    let number: u32 = query["partNumber"].parse().unwrap();
                    parts.insert(number, request.body);
                    ok(200, String::new())
                }
                "POST" => {
                    let xml = String::from_utf8(request.body).unwrap();
                    let mut body = Vec::new();
                    let mut sizes = Vec::new();
                    for part in xml.split("<PartNumber>").skip(1) {
                        let number: u32 =
                            part.split("</PartNumber>").next().unwrap().parse().unwrap();
                        let bytes = &parts[&number];
                        sizes.push(bytes.len());
                        body.extend_from_slice(bytes);
                    }
                    if sizes[..sizes.len() - 1]
                        .iter()
                        .any(|&n| n < 5 * 1024 * 1024)
                    {
                        return ok(400, "<Error><Code>EntityTooSmall</Code></Error>".into());
                    }
                    let key = key.clone();
                    uploads.remove(id);
                    self.bucket.borrow_mut().insert(key, body);
                    ok(200, "<CompleteMultipartUploadResult/>".into())
                }
                "DELETE" => {
                    uploads.remove(id);
                    ok(204, String::new())
                }
                _ => ok(405, String::new()),
            };
        }
        if request.method == "POST" && request.query == "uploads" {
            let n = self.upload_seq.get() + 1;
            self.upload_seq.set(n);
            let id = format!("up/{n}+x");
            self.uploads
                .borrow_mut()
                .insert(id.clone(), (request.key.into(), BTreeMap::new()));
            return ok(
                200,
                format!("<InitiateMultipartUploadResult><UploadId>{id}</UploadId></InitiateMultipartUploadResult>"),
            );
        }
        match request.method {
            "PUT" => {
                if let Some(response) = self.take_transient("PUT") {
                    return Ok(response);
                }
                let n = self.puts.get() + 1;
                self.puts.set(n);
                if self.fail_put_number.get() == Some(n) {
                    return Err("injected_put_failure".into());
                }
                self.put_md5s
                    .borrow_mut()
                    .insert(request.key.into(), request.content_md5.map(str::to_owned));
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
            "GET" => Ok(match self.recorded_get(request.key) {
                Some(body) => FosResponse {
                    cdn_cache: None,
                    status: 200,
                    body: fastly_log_analytics::admin::transport::read_fos_body(
                        body.as_slice(),
                        200,
                        request.response_limit,
                        &mut 0,
                    )?,
                    retry_after: None,
                },
                None => FosResponse {
                    cdn_cache: None,
                    status: 404,
                    body: vec![],
                    retry_after: None,
                },
            }),
            "HEAD" => ok(
                if self.bucket.borrow().contains_key(request.key) {
                    200
                } else {
                    404
                },
                String::new(),
            ),
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
    fn admin_cdn_read(
        &self,
        service_id: &str,
        key: &str,
        cdn_key: &str,
        max: usize,
        timeout: std::time::Duration,
        range: Option<(u64, u64)>,
    ) -> Result<FosResponse, String> {
        let Some((start, end)) = range else {
            return self.admin_cdn_get(service_id, key, cdn_key, max, timeout);
        };
        self.charge()?;
        assert_eq!(service_id, SID);
        assert_eq!(cdn_key, "ck");
        let length = usize::try_from(end - start + 1).unwrap();
        assert!(length <= max);
        self.range_reads
            .borrow_mut()
            .push((key.into(), start, length));
        self.source_bytes
            .set(self.source_bytes.get() + length as u64);
        self.cdn_gets.borrow_mut().push(key.into());
        let bucket = self.bucket.borrow();
        let body = bucket
            .get(key)
            .and_then(|bytes| bytes.get(start as usize..=end as usize))
            .map(<[u8]>::to_vec);
        Ok(FosResponse {
            status: if body.is_some() { 206 } else { 404 },
            body: body.unwrap_or_default(),
            cdn_cache: None,
            retry_after: None,
        })
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
        if let Some(response) = self.take_transient("GET") {
            return Ok(response);
        }
        self.source_bytes.set(
            self.source_bytes.get()
                + self
                    .bucket
                    .borrow()
                    .get(key)
                    .map_or(0, |bytes| bytes.len() as u64),
        );
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
            status: self.api_status.get(),
            body: json!({"status": "ok"}),
            retry_after: None,
        })
    }
    fn admin_remaining_calls(&self) -> u32 {
        BUDGET - self.calls.get()
    }
    fn execution_backend_calls(&self) -> Option<u32> {
        Some(self.calls.get())
    }
    fn execution_vcpu_ms(&self) -> Option<u64> {
        Some(self.vcpu_ms())
    }
    fn admin_elapsed(&self, _: Instant) -> std::time::Duration {
        std::time::Duration::from_secs(self.elapsed_s.get())
    }
    fn admin_wait(&self, millis: u64) {
        self.waits.borrow_mut().push(millis);
    }
    fn heap_mib(&self) -> Option<u32> {
        self.heap_mib.get()
    }
    fn linear_memory_high_water_bytes(&self) -> Option<u64> {
        self.linear_memory_high_water_bytes.get()
    }
    fn vcpu_ms(&self) -> u64 {
        self.vcpu.set(self.vcpu.get() + self.vcpu_step.get());
        self.vcpu.get()
    }
    fn pop(&self) -> Option<String> {
        Some("IAD".into())
    }
    fn on_log_shield(&self) -> bool {
        self.shield.get()
    }
    fn usage_agg(&self) -> Option<&UsageAgg> {
        Some(&self.usage)
    }
    fn logged_traffic(&self, _: &str, _: u64, _: u64) -> Result<Value, String> {
        Ok(json!({"days": []}))
    }
    fn core_cache_replace(
        &self,
        key: &str,
        _surrogate_keys: &[&str],
        _ttl: std::time::Duration,
        update: &dyn Fn(Option<CoreCacheEntry>) -> CoreCacheEntry,
    ) -> Result<(), CoreCacheError> {
        self.cache_op()?;
        let existing = self.cache.borrow().get(key).cloned();
        self.cache.borrow_mut().insert(key.into(), update(existing));
        self.cache_tags.borrow_mut().insert(
            key.into(),
            _surrogate_keys.iter().map(|k| (*k).to_owned()).collect(),
        );
        Ok(())
    }
    fn core_cache_lookup(&self, key: &str) -> Result<Option<CoreCacheEntry>, CoreCacheError> {
        self.cache_op()?;
        Ok(self.cache.borrow().get(key).cloned())
    }
    fn core_cache_purge(&self, surrogate: &str) -> Result<(), CoreCacheError> {
        self.cache_op()?;
        let tagged: Vec<String> = self
            .cache_tags
            .borrow()
            .iter()
            .filter(|(_, tags)| tags.iter().any(|t| t == surrogate))
            .map(|(key, _)| key.clone())
            .collect();
        for key in tagged {
            self.cache.borrow_mut().remove(&key);
            self.cache_tags.borrow_mut().remove(&key);
        }
        Ok(())
    }
    fn worker_requests(&self) -> u32 {
        self.worker_sent.get()
    }
    fn admin_event(&self, event: Value) {
        self.events.borrow_mut().push(event);
    }
    fn cache_ops(&self) -> u32 {
        self.cache_ops.get()
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
        for (path, body) in requests {
            if let Err(error) = self.charge() {
                out.push(Err(error));
                continue;
            }
            self.sent_to_worker();
            if path == fastly_log_analytics::admin::stage::PATH {
                let request = fastly_log_analytics::http::Request {
                    method: "POST".into(),
                    path: path.clone(),
                    headers: [("x-flp-worker-key".to_owned(), "fos-secret".to_owned())].into(),
                    body: body.to_string().into_bytes(),
                };
                let response = self.execution("stage", || {
                    fastly_log_analytics::admin::stage::handle_worker_stage(self, &request)
                });
                let reply: Result<Value, String> = if response.status == 200 {
                    serde_json::from_slice(&response.body).map_err(|e| e.to_string())
                } else {
                    Err(format!("worker_http_{}", response.status))
                };
                if let Ok(reply) = &reply {
                    self.stage_runs.borrow_mut().push(reply.clone());
                }
                if let Some(next) = self.race_maint_next.take() {
                    let mut kv = self.kv.borrow_mut();
                    let (generation, mut state) = kv["cron:state"].clone();
                    state["maint_next"] = next.into();
                    kv.insert("cron:state".into(), (generation + 1, state));
                }
                out.push(reply);
                continue;
            }
            if path != ingest::worker::PATH {
                out.push(Ok(json!({"path": path, "echo": body})));
                continue;
            }
            let fault = self
                .worker_faults
                .borrow_mut()
                .pop_front()
                .unwrap_or(WorkerFault::Ok);
            if fault == WorkerFault::Unreachable {
                out.push(Err("worker_deadline".into()));
                continue;
            }
            let request = fastly_log_analytics::http::Request {
                method: "POST".into(),
                path: path.clone(),
                headers: [("x-flp-worker-key".to_owned(), "fos-secret".to_owned())].into(),
                body: body.to_string().into_bytes(),
            };
            // A fresh execution: its own 32-call budget and Secret Store reads.
            let kind = if body["maintenance"] == "request_compact" {
                "request_compact_worker"
            } else if body["maintenance"] == "source_index" {
                "source_index_worker"
            } else {
                "ingest_worker"
            };
            let (response, used) = self.execution(kind, || {
                let response = ingest::worker::handle_worker_ingest(self, &request);
                (response, self.calls.get())
            });
            assert!(used <= BUDGET, "worker used {used} calls");
            self.worker_max_calls
                .set(self.worker_max_calls.get().max(used));
            self.worker_runs.set(self.worker_runs.get() + 1);
            if fault == WorkerFault::LostReply {
                out.push(Err("worker_deadline".into()));
            } else if response.status != 200 {
                out.push(Err(format!("worker_http_{}", response.status)));
            } else {
                out.push(serde_json::from_slice(&response.body).map_err(|e| e.to_string()));
            }
        }
        out
    }
    fn worker_delete(&self, body: &Value) -> Result<Value, String> {
        self.charge()?;
        self.sent_to_worker();
        self.worker_calls
            .borrow_mut()
            .push(body["keys"].as_array().map_or(0, Vec::len));
        let request = fastly_log_analytics::http::Request {
            method: "POST".into(),
            path: "/_internal/delete".into(),
            headers: [("x-flp-worker-key".to_owned(), "fos-secret".to_owned())].into(),
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
        LISTS.with(|n| n.set(n.get() + 1));
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
            let modified = self.mtimes.borrow().get(key).map_or_else(
                || "2027-01-15T08:00:00.000Z".to_owned(),
                |t| ingest::rfc3339(*t),
            );
            contents.push_str(&format!(
                "<Contents><Key>{key}</Key><Size>{}</Size><LastModified>{modified}</LastModified></Contents>",
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
    let report = ingest::run(fake, &service(), "ak", "sk", Instant::now(), &buffer);
    telemetry::flush_request_buffer(fake, &buffer);
    report
}

#[test]
fn a_tick_that_arrives_with_execution_vcpu_already_spent_still_commits() {
    // Live ticks reach the catalog commit with 40-50 ms of execution vCPU behind them (tick
    // startup, plan parsing, the source-index dispatch). The catalog's admission gate must not
    // turn that into a tick that re-plans forever and never writes a row.
    let fake = Fake::new(BASE + 3600);
    for (m, name) in ["a", "b", "c"].iter().enumerate() {
        let minute = BASE + m as i64 * 60;
        let lines: Vec<String> = (0..4)
            .map(|i| line(minute + i * 10, &format!("{name}{i}")))
            .collect();
        put_raw(&fake, minute, name, &lines);
    }
    let mut report = None;
    for _ in 0..4 {
        fake.new_request();
        fake.vcpu.set(45);
        let buffer = LogBuffer::new(SID, "req");
        let r = ingest::run(&fake, &service(), "ak", "sk", Instant::now(), &buffer).unwrap();
        let committed = fake.kv.borrow()[ingest::WATERMARK_KEY].1["run_id"].is_string();
        report = Some(r);
        if committed {
            break;
        }
    }
    let report = report.unwrap();
    assert_eq!((report.converted, report.rows), (3, 12));
    assert_eq!(fake.keys("processed/request/").len(), 3);
    assert!(fake.kv.borrow()[ingest::WATERMARK_KEY].1["run_id"].is_string());
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
    let keys = fake.keys("processed/request/");
    for key in fastly_log_analytics::telemetry::compact::visible(&keys, &[]) {
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

#[test]
fn every_row_lands_exactly_once_and_routes_by_window() {
    let fake = Fake::new(BASE + 3600);
    let mut expected = Vec::new();
    for (m, name) in ["a", "b", "c"].iter().enumerate() {
        let minute = BASE + m as i64 * 60;
        let lines: Vec<String> = (0..4)
            .map(|i| {
                let id = format!("{name}{i}");
                expected.push(id.clone());
                line(minute + i * 10, &id)
            })
            .collect();
        put_raw(&fake, minute, name, &lines);
    }
    let report = tick(&fake).unwrap();
    assert_eq!((report.converted, report.rows), (3, 12));
    expected.sort();
    assert_eq!(processed_ips(&fake), expected);
    let files = fake.keys("processed/request/");
    assert_eq!(files.len(), 3, "one file per 60s window: {files:?}");
    let summaries = fake.keys("processed/summary/request/");
    let mut paired: Vec<String> = summaries
        .iter()
        .map(|k| {
            k.replace("processed/summary/request/", "processed/request/")
                .replace("/summary=", "/window=")
        })
        .collect();
    paired.sort();
    assert_eq!(paired, files, "one summary beside each window");
    for key in &files {
        let name = ingest::parse_processed(key).unwrap();
        assert_eq!(name.end - name.start, 60);
        assert_eq!(name.rows, Some(4));
        assert!(key.contains("year=2027/month=01/day=15/hour=08/window="));
        for batch in batches(&fake.bucket.borrow()[key]) {
            let ts = batch
                .column_by_name("timestamp")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            assert!((0..ts.len()).all(|i| {
                let s = ts.value(i) / 1000;
                s >= name.start && s < name.end
            }));
        }
    }
    let again = tick(&fake).unwrap();
    assert_eq!(again.converted, 0);
    assert_eq!(processed_ips(&fake), expected);
}

#[test]
fn convert_object_counts_every_non_empty_line() {
    let data = gz(&[
        line(BASE, "a"),
        String::new(),
        "{not json".into(),
        line(BASE, "b"),
    ]);
    let rows = ingest::convert::convert_object(&data, 60, BASE).unwrap();
    assert_eq!((rows.rows, rows.bad_lines, rows.lines_read), (2, 1, 3));
}

fn ms_column(batch: &RecordBatch, name: &str) -> Vec<Option<i64>> {
    let col = batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    (0..col.len())
        .map(|i| col.is_valid(i).then(|| col.value(i)))
        .collect()
}

#[test]
fn rows_route_by_millisecond_ts_ms_and_fall_back_to_timestamp() {
    let at = |s: i64, ms: i64, ip: &str| {
        json!({"timestamp": iso(s), "ts_ms": s * 1000 + ms, "ip": ip}).to_string()
    };
    // ts_ms wins over a disagreeing timestamp; a legacy line without ts_ms uses timestamp.
    let lying = json!({"timestamp": iso(BASE + 61), "ts_ms": BASE * 1000 + 999, "ip": "c"});
    let data = gz(&[
        at(BASE + 5, 250, "a"),
        line(BASE + 70, "legacy"),
        lying.to_string(),
    ]);
    let rows = ingest::convert::convert_object(&data, 60, BASE).unwrap();
    assert_eq!(
        rows.windows.keys().copied().collect::<Vec<_>>(),
        [BASE, BASE + 60]
    );
    let first = &rows.windows[&BASE];
    assert_eq!(
        ms_column(first, "ts_ms"),
        [Some((BASE + 5) * 1000 + 250), Some(BASE * 1000 + 999)]
    );
    let second = &rows.windows[&(BASE + 60)];
    assert_eq!(ms_column(second, "ts_ms"), [None]);
    assert_eq!(ms_column(second, "timestamp"), [Some((BASE + 70) * 1000)]);
}

#[test]
fn window_files_are_sorted_by_time_and_declare_sorting_columns() {
    let row = |s: i64, ms: Option<i64>, ip: &str| match ms {
        Some(ms) => json!({"timestamp": iso(s), "ts_ms": s * 1000 + ms, "ip": ip, "status": 200})
            .to_string(),
        None => line(s, ip),
    };
    let one = ingest::convert::convert_object(
        &gz(&[
            row(BASE + 40, Some(5), "d"),
            row(BASE + 2, None, "legacy-late"),
            row(BASE + 10, Some(900), "b"),
            row(BASE + 1, None, "legacy-early"),
        ]),
        60,
        BASE,
    )
    .unwrap();
    let two = ingest::convert::convert_object(
        &gz(&[row(BASE + 10, Some(100), "a"), row(BASE + 30, Some(0), "c")]),
        60,
        BASE,
    )
    .unwrap();
    let batches_in = [&one.windows[&BASE], &two.windows[&BASE]];
    let bytes = ingest::convert::write_parquet(&batches_in).unwrap();
    assert_eq!(bytes, ingest::convert::write_parquet(&batches_in).unwrap());
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(&bytes)).unwrap();
    let schema = builder.parquet_schema().clone();
    for group in builder.metadata().row_groups() {
        let sorting: Vec<(String, bool, bool)> = group
            .sorting_columns()
            .unwrap()
            .iter()
            .map(|c| {
                (
                    schema.column(c.column_idx as usize).name().to_owned(),
                    c.descending,
                    c.nulls_first,
                )
            })
            .collect();
        assert_eq!(
            sorting,
            [
                ("host".into(), false, false),
                ("status".into(), false, false),
                ("cache".into(), false, false),
                ("method".into(), false, false),
                ("ts_ms".into(), false, false),
                ("timestamp".into(), false, false)
            ]
        );
    }
    let ips: Vec<String> = batches(&bytes)
        .iter()
        .flat_map(|b| {
            let col = b
                .column_by_name("ip")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .clone();
            (0..col.len()).map(move |i| col.value(i).to_owned())
        })
        .collect();
    assert_eq!(ips, ["a", "b", "c", "d", "legacy-early", "legacy-late"]);
}

#[test]
fn parquet_carries_schema_metadata_and_nulls_mismatched_values() {
    let fake = Fake::new(BASE + 3600);
    let bad = json!({"timestamp": iso(BASE), "ip": "x", "status": "oops", "unknown_key": 5});
    put_raw(&fake, BASE, "m", &[bad.to_string(), "{not json".into()]);
    let report = tick(&fake).unwrap();
    assert_eq!((report.rows, report.bad_lines), (1, 1));
    let key = &fake.keys("processed/request/")[0];
    let bytes = fake.bucket.borrow()[key].clone();
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(&bytes)).unwrap();
    let meta = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .unwrap();
    assert!(meta
        .iter()
        .any(|kv| kv.key == "flp_schema" && kv.value.as_deref() == Some("v3")));
    assert!(builder.schema().column_with_name("unknown_key").is_none());
    let batch = batches(&bytes).remove(0);
    let status = batch
        .column_by_name("status")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert!(status.is_null(0));
}

#[test]
fn window_size_setting_groups_rows() {
    let fake = Fake::new(BASE + 3600);
    *fake.window.borrow_mut() = Some("300".into());
    let lines: Vec<String> = (0..5)
        .map(|i| line(BASE + i * 60, &format!("r{i}")))
        .collect();
    put_raw(&fake, BASE, "w", &lines);
    tick(&fake).unwrap();
    let files = fake.keys("processed/request/");
    assert_eq!(files.len(), 1);
    let name = ingest::parse_processed(&files[0]).unwrap();
    assert_eq!(
        (name.start, name.end, name.rows),
        (BASE, BASE + 300, Some(5))
    );
}

#[test]
fn crash_before_commit_retries_without_duplicates() {
    for crash in ["put", "watermark"] {
        let fake = Fake::new(BASE + 3600);
        let mut expected = Vec::new();
        for m in 0..3i64 {
            let id = format!("r{m}");
            expected.push(id.clone());
            put_raw(&fake, BASE + m * 60, "x", &[line(BASE + m * 60, &id)]);
        }
        if crash == "put" {
            // PUT 1 is the worker's intent manifest; PUT 2 its first window file. The worker
            // fails, the tick doesn't: the shard is retried by the next tick.
            fake.fail_put_number.set(Some(2));
            assert!(tick(&fake).unwrap().more_pending);
        } else {
            fake.fail_watermark.set(true);
            assert!(tick(&fake).is_err());
            assert!(deleteq(&fake).is_empty(), "uncommitted queue withdrawn");
            fake.now
                .set(fake.now.get() + ingest::coordinator::LEASE_S + 1);
        }
        fake.fail_put_number.set(None);
        fake.fail_watermark.set(false);
        let retry = tick(&fake).unwrap();
        assert!(retry.resumed, "{crash}");
        assert_eq!(processed_ips(&fake), expected, "{crash}");
        assert_eq!(fake.keys("processed/request/").len(), 3, "{crash}");
        assert_eq!(fake.keys("processed/summary/request/").len(), 3, "{crash}");
        assert_eq!(tick(&fake).unwrap().converted, 0);
        assert_eq!(processed_ips(&fake), expected, "{crash}");
        assert!(!fake.kv.borrow().keys().any(|k| k.starts_with("ingested:")));
    }
}

#[test]
fn a_stage_reply_counts_as_a_turn_even_when_the_ingest_step_fails() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    put_raw(&fake, BASE, "x", &[line(BASE, "r0")]);
    fake.fail_watermark.set(true);
    run_tick(&fake);
    let runs = fake.stage_runs.borrow().clone();
    assert_eq!(runs.len(), 1, "the stage ran in the ingest batch");
    let state = fake.kv.borrow()["cron:state"].1.clone();
    assert_eq!(state["maint_misses"], 0, "not counted as a miss");
    assert_eq!(state["maint_next"], runs[0]["next"]);
}

#[test]
fn a_stale_stage_reply_never_moves_maint_next_backwards() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    run_tick(&fake);
    let start = fake.kv.borrow()["cron:state"].1["maint_next"]
        .as_u64()
        .unwrap();
    let raced = (start + 2) % 4;
    fake.race_maint_next.set(Some(raced));
    fake.now.set(fake.now.get() + 120);
    run_tick(&fake);
    assert_eq!(fake.stage_runs.borrow().len(), 2);
    let state = fake.kv.borrow()["cron:state"].1.clone();
    assert_eq!(
        state["maint_next"], raced,
        "the concurrent writer's value stands"
    );
}

#[test]
fn a_cron_state_save_that_loses_twice_is_an_error_not_silence() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    run_tick(&fake);
    fake.race_cron_state.set(2);
    fake.now.set(fake.now.get() + 120);
    fake.new_request();
    let result = fastly_log_analytics::admin::record_tick(&fake, "clock", "clock");
    assert!(result.is_err(), "{result:?}");
}

#[test]
fn backfill_retained_quarantine_is_not_attempted_again_after_queue_consumption() {
    let fake = Fake::new(BASE + 3600);
    let (bad, good) = quarantined_fixture(&fake);
    delete_tick(&fake, &service());
    assert!(deleteq(&fake).is_empty());
    assert_eq!(quarantine_records(&fake)[0]["objects"][0]["key"], bad);
    let reads = fake.cdn_gets.borrow().len();
    let watermark = fake.kv.borrow()[ingest::WATERMARK_KEY].1.clone();

    let report = backfill(&fake, "");

    assert_eq!(
        (report.converted, report.failed, report.shards_dispatched),
        (0, 0, 0)
    );
    assert_eq!(fake.cdn_gets.borrow().len(), reads);
    assert!(fake.bucket.borrow().contains_key(&bad));
    assert!(!fake.bucket.borrow().contains_key(&good));
    assert_eq!(processed_ips(&fake), vec!["good".to_owned()]);
    assert_eq!(fake.kv.borrow()[ingest::WATERMARK_KEY].1, watermark);
    let job = &fake.kv.borrow()[ingest::BACKFILL_KEY].1;
    assert_eq!(
        (job["scanned"].as_u64(), job["planned"].as_u64()),
        (Some(1), Some(0))
    );
    assert_eq!(job["after_key"], bad);
    assert_eq!(job["status"], "complete");
}

fn backfill(fake: &Fake, to_key: &str) -> ingest::Report {
    let job = ingest::BackfillJob {
        status: "running".into(),
        actor: "admin".into(),
        from_ts: None,
        to_ts: None,
        after_key: String::new(),
        to_key: to_key.into(),
        scanned: 0,
        planned: 0,
        started_at: fake.now.get(),
        updated_at: fake.now.get(),
    };
    fake.kv.borrow_mut().insert(
        ingest::BACKFILL_KEY.into(),
        (1, serde_json::to_value(job).unwrap()),
    );
    fake.new_request();
    let buffer = LogBuffer::new(SID, "backfill");
    let mut options = ingest::coordinator::Options::request(fake);
    options.allow_backfill = true;
    ingest::coordinator::coordinate(
        fake,
        &service(),
        "ak",
        "sk",
        Instant::now(),
        &buffer,
        &options,
    )
    .unwrap()
}

#[test]
fn backfill_ineligible_pages_do_not_list_or_read_known_records() {
    for case in ["empty", "invalid", "unsettled", "to_key"] {
        let fake = Fake::new(BASE + 3600);
        let key = match case {
            "empty" => None,
            "invalid" => Some("raw/request/invalid.txt".to_owned()),
            "unsettled" => Some(raw_key(BASE + 3600, "fresh")),
            _ => Some(raw_key(BASE, "boundary")),
        };
        if let Some(key) = &key {
            fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
        }
        set_watermark(&fake, &raw_key(BASE + 7200, "watermark"));
        for prefix in [ingest::DELETEQ_PREFIX, ingest::QUARANTINE_PREFIX] {
            fake.kv
                .borrow_mut()
                .insert(format!("{prefix}probe"), (1, json!({"keys": []})));
        }
        let watermark = fake.kv.borrow()[ingest::WATERMARK_KEY].1.clone();
        let report = backfill(
            &fake,
            if case == "to_key" {
                key.as_deref().unwrap()
            } else {
                ""
            },
        );
        assert_eq!(
            (report.listed, report.converted, report.shards_dispatched),
            (0, 0, 0),
            "{case}"
        );
        assert_eq!(report.backend_calls, 6, "{case}");
        assert_eq!(
            fake.kv_reads
                .borrow()
                .iter()
                .filter(|key| {
                    key.contains(ingest::DELETEQ_PREFIX) || key.contains(ingest::QUARANTINE_PREFIX)
                })
                .count(),
            0,
            "{case}: no known-key LIST or record GET is needed"
        );
        assert_eq!(fake.kv.borrow()[ingest::WATERMARK_KEY].1, watermark);
        let job = &fake.kv.borrow()[ingest::BACKFILL_KEY].1;
        assert_eq!(job["status"], "complete", "{case}");
        assert_eq!(job["after_key"], "", "{case}");
        assert_eq!(job["scanned"], 0, "{case}");
        assert_eq!(job["planned"], 0, "{case}");
    }
}

#[test]
fn backfill_eligible_candidates_read_fresh_known_records_and_keep_legacy_shapes() {
    for shape in ["keys", "held", "raw_key", "objects"] {
        let fake = Fake::new(BASE + 3600);
        let key = put_raw(&fake, BASE, "known", &[line(BASE, "duplicate")]);
        set_watermark(&fake, &raw_key(BASE + 60, "watermark"));
        let value = match shape {
            "keys" => json!({"keys": [key]}),
            "held" => json!({"held": [{"key": key}]}),
            "raw_key" => json!({"raw_key": key}),
            _ => json!({"objects": [{"key": key}]}),
        };
        let record_key = format!("{}known", ingest::QUARANTINE_PREFIX);
        fake.kv.borrow_mut().insert(record_key.clone(), (2, value));
        let report = backfill(&fake, "");
        assert_eq!(
            (report.converted, report.shards_dispatched),
            (0, 0),
            "{shape}"
        );
        assert_eq!(report.backend_calls, 9, "{shape}");
        let reads = fake.kv_reads.borrow();
        assert_eq!(
            reads
                .iter()
                .filter(|key| key.as_str() == "list:ingest:deleteq:")
                .count(),
            1
        );
        assert_eq!(
            reads
                .iter()
                .filter(|key| key.as_str() == "list:ingest:quarantine:")
                .count(),
            1
        );
        assert_eq!(reads.iter().filter(|key| *key == &record_key).count(), 1);
        let job = &fake.kv.borrow()[ingest::BACKFILL_KEY].1;
        assert_eq!(job["after_key"], key);
        assert_eq!(
            (job["scanned"].as_u64(), job["planned"].as_u64()),
            (Some(1), Some(0))
        );
    }
}

#[test]
fn backfill_deep_relist_converts_missed_raw_files_without_regressing_watermark() {
    let fake = Fake::new(BASE + 3600);
    // Normal tick ingests minute +120 ("late") first, advancing watermark past BASE ("missed").
    let late_key = put_raw(&fake, BASE + 120, "late", &[line(BASE + 120, "ip_late")]);
    let r1 = tick(&fake).unwrap();
    assert_eq!(r1.converted, 1);
    let wm_before = fake.kv.borrow()[ingest::WATERMARK_KEY].1["key"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(wm_before, late_key);

    // A historical raw file arrives at BASE (behind watermark). Normal forward tick skips it.
    let _missed_key = put_raw(&fake, BASE, "missed", &[line(BASE, "ip_missed")]);
    assert_eq!(tick(&fake).unwrap().converted, 0);

    // Enqueue a deep re-LIST backfill job.
    let job = ingest::BackfillJob {
        status: "running".into(),
        actor: "admin".into(),
        from_ts: None,
        to_ts: None,
        after_key: String::new(),
        to_key: String::new(),
        scanned: 0,
        planned: 0,
        started_at: fake.now.get(),
        updated_at: fake.now.get(),
    };
    fake.kv.borrow_mut().insert(
        ingest::BACKFILL_KEY.into(),
        (1, serde_json::to_value(&job).unwrap()),
    );

    fake.new_request();
    let buffer = LogBuffer::new(SID, "req");
    let mut opts = ingest::coordinator::Options::request(&fake);
    opts.allow_backfill = true;
    let r2 = ingest::coordinator::coordinate(
        &fake,
        &service(),
        "ak",
        "sk",
        Instant::now(),
        &buffer,
        &opts,
    )
    .unwrap();
    assert_eq!(r2.converted, 1);
    assert_eq!(
        processed_ips(&fake),
        vec!["ip_late".to_owned(), "ip_missed".to_owned()]
    );
    let wm_after = fake.kv.borrow()[ingest::WATERMARK_KEY].1["key"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        wm_after, wm_before,
        "watermark must not regress when backfilling older keys"
    );
}

#[test]
fn a_newer_plan_version_is_left_alone() {
    let fake = Fake::new(BASE + 3600);
    put_raw(&fake, BASE, "x", &[line(BASE, "r0")]);
    let newer = json!({"version": 4, "plan_id": "p", "shards": []});
    fake.kv
        .borrow_mut()
        .insert("ingest:plan".into(), (7, newer.clone()));
    let report = tick(&fake).unwrap();
    assert!(report.busy);
    assert_eq!(report.converted, 0);
    assert_eq!(fake.kv.borrow()["ingest:plan"], (7, newer));
    assert!(fake.keys("processed/request/").is_empty());
}

#[test]
fn ingest_plans_and_dispatches_nothing_past_the_dispatch_deadline() {
    let fake = Fake::new(BASE + 7200);
    for i in 0..4 {
        let at = BASE + i * 60;
        put_raw(&fake, at, "o", &[line(at, &format!("w{i}"))]);
    }
    fake.elapsed_s
        .set(ingest::coordinator::DISPATCH_BY.as_secs());
    let first = tick(&fake).unwrap();
    assert_eq!(first.converted, 0);
    assert!(first.more_pending);
    assert!(!fake.kv.borrow().contains_key(ingest::PLAN_KEY));
    assert_eq!(fake.worker_runs.get(), 0);
    fake.elapsed_s.set(0);
    for _ in 0..4 {
        tick(&fake).unwrap();
    }
    assert_eq!(processed_ips(&fake), ["w0", "w1", "w2", "w3"]);
}

#[test]
fn backlog_names_unconverted_raw_only() {
    let fake = Fake::new(BASE + 7200);
    for i in 0..4 {
        let at = BASE + i * 60;
        put_raw(&fake, at, "o", &[line(at, &format!("w{i}"))]);
    }
    fake.elapsed_s
        .set(ingest::coordinator::DISPATCH_BY.as_secs());
    let deferred = tick(&fake).unwrap();
    assert!(deferred.more_pending && deferred.backlog);
    fake.elapsed_s.set(0);
    let mut last = deferred;
    for _ in 0..8 {
        last = tick(&fake).unwrap();
        if processed_ips(&fake).len() == 4 {
            break;
        }
    }
    assert_eq!(processed_ips(&fake), ["w0", "w1", "w2", "w3"]);
    assert!(
        !last.backlog,
        "commit of the last raw object leaves no backlog"
    );
    // Recent-window scans and sweeps keep the scheduler ticking but name no unconverted raw.
    for _ in 0..3 {
        let idle = tick(&fake).unwrap();
        assert_eq!(idle.converted, 0);
        assert!(!idle.backlog);
    }
}

#[test]
fn immutable_requests_commit_before_the_closed_partition_allowance() {
    let fake = Fake::new(BASE + 60 + ingest::SETTLE_S as i64 - 1);
    put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    assert_eq!(tick(&fake).unwrap().converted, 1);
    assert_eq!(processed_ips(&fake), ["a"]);
    fake.now.set(BASE as u64 + 60 + ingest::SETTLE_S);
    assert_eq!(tick(&fake).unwrap().converted, 0);
}

fn tick_service(fake: &Fake, service: &Service) -> Result<ingest::Report, String> {
    fake.new_request();
    let buffer = LogBuffer::new(SID, "req");
    let report = ingest::run(fake, service, "ak", "sk", Instant::now(), &buffer);
    telemetry::flush_request_buffer(fake, &buffer);
    report
}

#[test]
fn settle_waits_for_the_log_period_so_a_late_upload_is_still_planned() {
    let mut svc = service();
    svc.log_period = 300;
    let fake = Fake::new(BASE + 60 + ingest::SETTLE_S as i64);
    put_raw(&fake, BASE, "b", &[line(BASE, "b")]);
    tick_service(&fake, &svc).unwrap();
    // Another edge node's 300 s batch for the same minute lands 200 s after the minute closed,
    // under a key that sorts before the first one.
    fake.now.set(BASE as u64 + 60 + 200);
    put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    tick_service(&fake, &svc).unwrap();
    fake.now.set(BASE as u64 + 300 + ingest::SETTLE_S);
    for _ in 0..3 {
        tick_service(&fake, &svc).unwrap();
    }
    assert_eq!(processed_ips(&fake), ["a", "b"]);
}

#[test]
fn a_list_page_without_gz_keys_still_advances_the_watermark() {
    let fake = Fake::new(BASE + 3600);
    let mut last = String::new();
    for i in 0..ingest::LIST_PAGE {
        last = raw_key(BASE, &format!("n{i:03}")).replace(".zst", ".json");
        fake.bucket
            .borrow_mut()
            .insert(last.clone(), b"{}".to_vec());
    }
    put_raw(&fake, BASE + 60, "z", &[line(BASE + 60, "z")]);
    let first = tick(&fake).unwrap();
    assert_eq!(first.converted, 0);
    assert!(first.more_pending);
    assert_eq!(
        fake.kv.borrow()[ingest::WATERMARK_KEY].1["key"].as_str(),
        Some(last.as_str())
    );
    tick(&fake).unwrap();
    assert_eq!(processed_ips(&fake), ["z"]);
}

fn sweep_tick(fake: &Fake) -> ingest::Report {
    fake.new_request();
    let buffer = LogBuffer::new(SID, "req");
    let mut opts = ingest::coordinator::Options::request(fake);
    opts.sweep = true;
    let report = ingest::coordinator::coordinate(
        fake,
        &service(),
        "ak",
        "sk",
        Instant::now(),
        &buffer,
        &opts,
    )
    .unwrap();
    telemetry::flush_request_buffer(fake, &buffer);
    report
}

#[test]
fn the_sweep_ingests_an_object_uploaded_after_the_watermark_passed_its_key_once() {
    let fake = Fake::new(BASE + 3600);
    put_raw(&fake, BASE + 60, "b", &[line(BASE + 60, "b")]);
    let seen = put_raw(&fake, BASE, "c", &[line(BASE, "c")]);
    fake.mtimes.borrow_mut().insert(seen, BASE + 3590);
    assert_eq!(tick(&fake).unwrap().converted, 2);
    let late = put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    fake.mtimes.borrow_mut().insert(late.clone(), BASE + 3700);
    fake.now.set(BASE as u64 + 3800);
    assert_eq!(tick(&fake).unwrap().converted, 0);

    let report = sweep_tick(&fake);
    assert_eq!((report.stragglers, report.converted), (1, 1));
    assert!(report.backend_calls + ingest::TICK_RESERVE <= BUDGET);
    assert_eq!(processed_ips(&fake), ["a", "b", "c"]);
    let files = rows(&fake, LogKind::IngestFiles);
    let mut statuses: Vec<&str> = files
        .iter()
        .filter(|row| row["raw_key"] == late.as_str())
        .map(|row| row["status"].as_str().unwrap())
        .collect();
    statuses.sort_unstable();
    assert_eq!(statuses, ["converted", "straggler"]);
    let latest = telemetry::latest_ingest_state(&files);
    let state = latest.iter().find(|row| row["raw_key"] == late.as_str());
    assert_eq!(state.unwrap()["status"], "converted");

    for _ in 0..3 {
        fake.now.set(fake.now.get() + ingest::SWEEP_INTERVAL_S);
        let again = sweep_tick(&fake);
        assert_eq!((again.stragglers, again.converted), (0, 0));
    }
    assert_eq!(processed_ips(&fake), ["a", "b", "c"]);
}

#[test]
fn the_sweep_lists_at_most_two_pages_a_tick_and_resumes_from_its_cursor() {
    let fake = Fake::new(BASE + 3600);
    let pages = ingest::SWEEP_LISTS * ingest::LIST_PAGE;
    for i in 0..pages + 20 {
        let key = raw_key(BASE, &format!("n{i:04}")).replace(".zst", ".json");
        fake.bucket.borrow_mut().insert(key, b"{}".to_vec());
    }
    put_raw(&fake, BASE + 60, "b", &[line(BASE + 60, "b")]);
    while tick(&fake).unwrap().more_pending {}
    assert_eq!(processed_ips(&fake), ["b"]);
    let late = put_raw(&fake, BASE, "z", &[line(BASE, "z")]);
    fake.mtimes.borrow_mut().insert(late, BASE + 3700);
    fake.now.set(BASE as u64 + 3800);

    let first = sweep_tick(&fake);
    assert_eq!(first.stragglers, 0);
    assert!(first.backend_calls + ingest::TICK_RESERVE <= BUDGET);
    let cursor = fake.kv.borrow()[ingest::WATERMARK_KEY].1["sweep"]["after"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        cursor,
        raw_key(BASE, &format!("n{:04}", pages - 1)).replace(".zst", ".json")
    );

    fake.now.set(fake.now.get() + ingest::SWEEP_INTERVAL_S);
    let second = sweep_tick(&fake);
    assert_eq!((second.stragglers, second.converted), (1, 1));
    assert_eq!(processed_ips(&fake), ["b", "z"]);
}

fn raw_floor(stream: &str, minute_start: i64) -> String {
    format!("raw/{stream}/{}", ingest::minute_dir(minute_start))
}

fn set_floor(fake: &Fake, watermark_key: &str, floor: &str) {
    let mut kv = fake.kv.borrow_mut();
    let (generation, mut value) = kv.get(watermark_key).cloned().unwrap_or((0, json!({})));
    value[ingest::FLOOR_FIELD] = floor.into();
    kv.insert(watermark_key.into(), (generation + 1, value));
}

fn floor_of(fake: &Fake, watermark_key: &str) -> Option<String> {
    fake.kv.borrow()[watermark_key].1[ingest::FLOOR_FIELD]
        .as_str()
        .map(str::to_owned)
}

fn queued_keys(fake: &Fake) -> Vec<String> {
    let mut keys: Vec<String> = deleteq(fake)
        .iter()
        .flat_map(|queue| queue["keys"].as_array().cloned().unwrap_or_default())
        .filter_map(|key| key.as_str().map(str::to_owned))
        .collect();
    keys.sort();
    keys
}

#[test]
fn below_floor_compares_keys_only_against_a_string_floor() {
    let floor = raw_floor("request", BASE + 60);
    let below = raw_key(BASE, "a");
    let at = raw_key(BASE + 60, "a");
    let watermark = json!({ "floor_key": floor });
    assert!(ingest::below_floor(&below, Some(&watermark)));
    assert!(!ingest::below_floor(&at, Some(&watermark)));
    assert!(!ingest::below_floor(&below, None));
    assert!(!ingest::below_floor(&below, Some(&json!({}))));
    assert!(!ingest::below_floor(
        &below,
        Some(&json!({ "floor_key": 7 }))
    ));
}

#[test]
fn floor_blocks_start_after_listing_below_reset_minute() {
    let fake = Fake::new(BASE + 3600);
    let reset = BASE + 600;
    put_raw(&fake, reset - 60, "old", &[line(reset - 60, "old")]);
    let new = put_raw(&fake, reset, "new", &[line(reset, "new")]);
    let floor = raw_floor("request", reset);
    set_floor(&fake, ingest::WATERMARK_KEY, &floor);
    for _ in 0..8 {
        if !tick(&fake).unwrap().more_pending {
            break;
        }
    }
    assert_eq!(processed_ips(&fake), ["new"]);
    assert_eq!(queued_keys(&fake), std::slice::from_ref(&new));
    assert_eq!(
        fake.kv.borrow()[ingest::WATERMARK_KEY].1["key"].as_str(),
        Some(new.as_str())
    );
    assert_eq!(floor_of(&fake, ingest::WATERMARK_KEY), Some(floor));
}

#[test]
fn floor_blocks_recent_scan_candidates() {
    let reset = BASE + 600;
    let fake = Fake::new(reset + 120);
    put_raw(&fake, reset - 60, "old", &[line(reset - 60, "old")]);
    let fresh = put_raw(&fake, reset, "z", &[line(reset, "z")]);
    let floor = raw_floor("request", reset);
    fake.kv.borrow_mut().insert(
        ingest::WATERMARK_KEY.into(),
        (
            1,
            json!({"key": raw_key(reset, "a"), "run_id": "abcdefabcdef", "raw_sources": {},
                   "raw_scan_due": true, "floor_key": floor}),
        ),
    );
    for _ in 0..6 {
        sweep_tick(&fake);
        fake.now.set(fake.now.get() + 1);
    }
    assert_eq!(processed_ips(&fake), ["z"]);
    assert_eq!(queued_keys(&fake), [fresh]);
    assert_eq!(floor_of(&fake, ingest::WATERMARK_KEY), Some(floor));
}

#[test]
fn floor_blocks_straggler_sweep() {
    let fake = Fake::new(BASE + 3600);
    put_raw(&fake, BASE + 60, "b", &[line(BASE + 60, "b")]);
    let seen = put_raw(&fake, BASE, "c", &[line(BASE, "c")]);
    fake.mtimes.borrow_mut().insert(seen, BASE + 3590);
    assert_eq!(tick(&fake).unwrap().converted, 2);
    let floor = raw_floor("request", BASE + 60);
    set_floor(&fake, ingest::WATERMARK_KEY, &floor);
    let late = put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    fake.mtimes.borrow_mut().insert(late, BASE + 3700);
    fake.now.set(BASE as u64 + 3800);
    assert_eq!(tick(&fake).unwrap().converted, 0);
    for _ in 0..3 {
        let report = sweep_tick(&fake);
        assert_eq!((report.stragglers, report.converted), (0, 0));
        fake.now.set(fake.now.get() + ingest::SWEEP_INTERVAL_S);
    }
    assert_eq!(processed_ips(&fake), ["b", "c"]);
    assert_eq!(floor_of(&fake, ingest::WATERMARK_KEY), Some(floor));
}

#[test]
fn floor_blocks_backfill() {
    let fake = Fake::new(BASE + 3600);
    put_raw(&fake, BASE + 120, "late", &[line(BASE + 120, "ip_late")]);
    assert_eq!(tick(&fake).unwrap().converted, 1);
    let floor = raw_floor("request", BASE + 60);
    set_floor(&fake, ingest::WATERMARK_KEY, &floor);
    put_raw(&fake, BASE, "missed", &[line(BASE, "ip_missed")]);
    let report = backfill(&fake, "");
    assert_eq!((report.listed, report.converted), (0, 0));
    assert_eq!(processed_ips(&fake), ["ip_late"]);
    assert_eq!(
        fake.kv.borrow()[ingest::BACKFILL_KEY].1["status"],
        "complete"
    );
    assert_eq!(floor_of(&fake, ingest::WATERMARK_KEY), Some(floor));
}

#[test]
fn floor_blocks_rum_planning() {
    let fake = Fake::new(BASE + 3600);
    let svc = rum_service();
    put_rum(&fake, BASE, "old", &[rum_beacon(BASE + 5, "LCP", "100")]);
    let new = put_rum(
        &fake,
        BASE + 60,
        "new",
        &[rum_beacon(BASE + 65, "LCP", "200")],
    );
    let key = ingest::watermark_key("rum");
    let floor = raw_floor("rum", BASE + 60);
    set_floor(&fake, &key, &floor);
    for _ in 0..6 {
        if !rum_tick(&fake, &svc).unwrap().more_pending {
            break;
        }
    }
    assert_eq!(queued_keys(&fake), std::slice::from_ref(&new));
    assert_eq!(fake.kv.borrow()[&key].1["key"].as_str(), Some(new.as_str()));
    fake.now.set(fake.now.get() + ingest::SWEEP_INTERVAL_S);
    rum_tick(&fake, &svc).unwrap();
    assert_eq!(queued_keys(&fake), [new]);
    assert_eq!(floor_of(&fake, &key), Some(floor));
}

#[test]
fn floor_key_survives_every_watermark_writer() {
    let fake = Fake::new(BASE + 600);
    let floor = raw_floor("request", BASE);
    set_floor(&fake, ingest::WATERMARK_KEY, &floor);
    let wm = || fake.kv.borrow()[ingest::WATERMARK_KEY].1.clone();

    // A legacy v1 plan's `finish`.
    let old = put_raw(&fake, BASE + 300, "old", &[line(BASE + 300, "old")]);
    fake.kv.borrow_mut().insert(
        ingest::PLAN_KEY.into(),
        (
            3,
            json!({"run_id": "abcdefabcdef", "window_s": 60, "schema": "v3",
                "raw_keys": [old], "created_at": 1}),
        ),
    );
    assert!(tick(&fake).unwrap().resumed);
    assert_eq!(wm()["run_id"], "abcdefabcdef");
    assert_eq!(
        floor_of(&fake, ingest::WATERMARK_KEY).as_ref(),
        Some(&floor)
    );

    // A plan and its commit.
    let new = put_raw(&fake, BASE + 360, "new", &[line(BASE + 360, "new")]);
    assert_eq!(tick(&fake).unwrap().converted, 1);
    assert_eq!(wm()["key"].as_str(), Some(new.as_str()));
    assert_eq!(
        floor_of(&fake, ingest::WATERMARK_KEY).as_ref(),
        Some(&floor)
    );

    // `plan_new` passing a page without raw keys.
    let passed = raw_key(BASE + 420, "n").replace(".zst", ".json");
    fake.bucket
        .borrow_mut()
        .insert(passed.clone(), b"{}".to_vec());
    tick(&fake).unwrap();
    assert_eq!(wm()["key"].as_str(), Some(passed.as_str()));
    assert_eq!(
        floor_of(&fake, ingest::WATERMARK_KEY).as_ref(),
        Some(&floor)
    );

    // The straggler sweep moving its cursor with nothing found.
    fake.now.set(BASE as u64 + 720);
    sweep_tick(&fake);
    assert_eq!(wm()["sweep"]["at"], BASE + 720);
    assert_eq!(
        floor_of(&fake, ingest::WATERMARK_KEY).as_ref(),
        Some(&floor)
    );

    // `recent_candidates` clearing a due scan whose cursor isn't recent.
    {
        let mut kv = fake.kv.borrow_mut();
        let entry = kv.get_mut(ingest::WATERMARK_KEY).unwrap();
        entry.0 += 1;
        entry.1["raw_sources"] = json!({});
        entry.1["raw_scan_due"] = true.into();
    }
    sweep_tick(&fake);
    assert_eq!(wm()["raw_scan_due"], false);
    assert_eq!(
        floor_of(&fake, ingest::WATERMARK_KEY).as_ref(),
        Some(&floor)
    );

    // `recent_candidates` finishing a recent scan with no candidates.
    {
        let now = fake.now.get() as i64;
        let mut kv = fake.kv.borrow_mut();
        let entry = kv.get_mut(ingest::WATERMARK_KEY).unwrap();
        entry.0 += 1;
        entry.1["key"] = raw_key(now - now.rem_euclid(60) - 60, "zzz").into();
        entry.1["raw_scan_due"] = true.into();
        entry.1["raw_scan"] = "".into();
    }
    sweep_tick(&fake);
    assert_eq!(wm()["raw_scan_due"], false);
    assert_eq!(floor_of(&fake, ingest::WATERMARK_KEY), Some(floor));
}

#[test]
fn tick_is_bounded_and_continues_next_tick() {
    let fake = Fake::new(BASE + 7200);
    let capacity = ingest::coordinator::MAX_FANOUT * ingest::worker::SHARD_MAX_OBJECTS;
    let mut expected = Vec::new();
    for i in 0..capacity + 40 {
        let id = format!("r{i:03}");
        expected.push(id.clone());
        let minute = BASE + (i / 25) as i64 * 60;
        put_raw(&fake, minute, &format!("o{i:03}"), &[line(minute, &id)]);
    }
    expected.sort();
    let first = tick(&fake).unwrap();
    assert!(first.converted as usize <= capacity);
    assert!(first.converted > 0);
    assert!(first.more_pending);
    assert!(first.backend_calls + ingest::TICK_RESERVE <= BUDGET);
    for _ in 0..6 {
        tick(&fake).unwrap();
    }
    assert_eq!(processed_ips(&fake), expected);
    assert!(fake.worker_max_calls.get() <= BUDGET);
}

#[test]
fn corrupt_objects_are_recorded_and_skipped() {
    let fake = Fake::new(BASE + 3600);
    fake.bucket
        .borrow_mut()
        .insert(raw_key(BASE, "bad"), b"not gzip".to_vec());
    put_raw(&fake, BASE + 60, "ok", &[line(BASE + 60, "good")]);
    let report = tick(&fake).unwrap();
    assert_eq!((report.failed, report.converted), (1, 1));
    assert_eq!(processed_ips(&fake), vec!["good"]);
    assert_eq!(tick(&fake).unwrap().listed, 0);
}

#[test]
fn an_object_spanning_too_many_windows_is_quarantined_without_blocking_others() {
    let fake = Fake::new(BASE + 7200);
    let spread: Vec<String> = (0..ingest::MAX_WINDOWS_PER_OBJECT as i64 + 1)
        .map(|i| line(BASE + i * 60, &format!("w{i}")))
        .collect();
    put_raw(&fake, BASE, "spread", &spread);
    put_raw(&fake, BASE + 60, "ok", &[line(BASE + 60, "good")]);
    let report = tick(&fake).unwrap();
    assert_eq!((report.failed, report.converted), (1, 1));
    assert_eq!(processed_ips(&fake), vec!["good"]);
    let errors = rows(&fake, LogKind::PlatformErrors);
    assert!(errors
        .iter()
        .any(|r| r["code"] == "too_many_windows" && r["error_domain"] == "ingest_corrupt"));
    let files = rows(&fake, LogKind::IngestFiles);
    let spread_row = files
        .iter()
        .find(|r| r["raw_key"].as_str().unwrap().ends_with("spread.zst"))
        .unwrap();
    assert_eq!(spread_row["status"], "quarantined");
    assert_eq!(tick(&fake).unwrap().listed, 0);
    assert!(deleteq(&fake)
        .iter()
        .all(|q| q["keys"].to_string().contains("ok.zst")
            && !q["keys"].to_string().contains("spread")));
}

#[test]
fn cumulative_decompressed_bytes_per_tick_are_capped_and_the_rest_defers() {
    let fake = Fake::new(BASE + 7200);
    let big = |at: i64, tag: &str| -> Vec<String> {
        (0..130)
            .map(|i| line(at, &format!("{tag}{i}-{}", "x".repeat(200_000))))
            .collect()
    };
    for i in 0..3 {
        put_raw(
            &fake,
            BASE + i * 60,
            "big",
            &big(BASE + i * 60, &format!("b{i}")),
        );
    }
    // One shard cannot hold all three in memory: it splits before any PUT, until each half fits.
    let mut converted = 0;
    for _ in 0..4 {
        let report = tick(&fake).unwrap();
        assert_eq!(report.failed, 0);
        converted += report.converted;
    }
    assert_eq!(converted, 3);
    assert_eq!(fake.keys("processed/request/").len(), 3);
    assert!(fake.keys("processed/_inflight/").is_empty() || !deleteq(&fake).is_empty());
}

fn deleteq(fake: &Fake) -> Vec<Value> {
    fake.kv
        .borrow()
        .iter()
        .filter(|(k, _)| k.starts_with("ingest:deleteq:"))
        .map(|(_, (_, v))| v.clone())
        .collect()
}

#[test]
fn converted_raw_keys_including_fresh_objects_are_queued_but_failed_objects_are_not() {
    let fake = Fake::new(BASE + 3600);
    let bad = raw_key(BASE, "bad");
    fake.bucket
        .borrow_mut()
        .insert(bad.clone(), b"not gzip".to_vec());
    let good = put_raw(&fake, BASE + 60, "ok", &[line(BASE + 60, "good")]);
    let fresh = put_raw(&fake, BASE + 3590, "new", &[line(BASE + 3590, "fresh")]);
    tick(&fake).unwrap();
    let queues = deleteq(&fake);
    assert_eq!(queues.len(), 1);
    assert_eq!(queues[0]["keys"], json!([good, fresh]));
    assert_eq!(queues[0]["sizes"].as_array().unwrap().len(), 2);
    assert!(!fake.kv.borrow().keys().any(|k| k.starts_with("ingested:")));
    let bucket = fake.bucket.borrow();
    assert!(bucket.contains_key(&bad) && bucket.contains_key(&good) && bucket.contains_key(&fresh));
    drop(bucket);
    assert_eq!(processed_ips(&fake), vec!["fresh", "good"]);
}

#[test]
fn a_tick_never_reads_or_lists_the_old_ledger() {
    let fake = Fake::new(BASE + 7200);
    for minute in 0..6 {
        put_raw(
            &fake,
            BASE + minute * 60,
            "a",
            &[line(BASE + minute * 60, "a")],
        );
    }
    for _ in 0..3 {
        tick(&fake).unwrap();
    }
    assert!(fake
        .kv_reads
        .borrow()
        .iter()
        .all(|k| !k.contains("ingested:") && !k.contains("ingest:sweep")));
    assert!(fake.kv.borrow().get("ingest:sweep").is_none());
}

#[test]
fn watermark_alone_is_not_proof_for_deletion() {
    let fake = Fake::new(BASE + 7200);
    let old = put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    put_raw(&fake, BASE + 60, "a", &[line(BASE + 60, "b")]);
    tick(&fake).unwrap();
    let watermark = fake.kv.borrow()[ingest::WATERMARK_KEY].1.clone();
    assert!(watermark["key"].as_str().unwrap() >= old.as_str());
    fake.kv
        .borrow_mut()
        .retain(|k, _| !k.starts_with("ingest:deleteq:"));
    for _ in 0..3 {
        tick(&fake).unwrap();
    }
    assert_eq!(fake.keys("raw/request/").len(), 2);
}

#[test]
fn backlog_below_the_watermark_drains_across_ticks_without_reingesting() {
    let fake = Fake::new(BASE + 7200);
    for minute in 0..40 {
        put_raw(
            &fake,
            BASE + minute * 60,
            "a",
            &[line(BASE + minute * 60, "a")],
        );
    }
    let mut converted = 0;
    for _ in 0..12 {
        converted += tick(&fake).unwrap().converted;
    }
    assert_eq!(converted, 40);
    assert_eq!(processed_ips(&fake).len(), 40);
    let queued: usize = deleteq(&fake)
        .iter()
        .map(|q| q["keys"].as_array().unwrap().len())
        .sum();
    assert_eq!(queued, 40);
}

#[test]
fn ingest_files_rows_are_one_per_raw_object_in_one_outbox_key() {
    let fake = Fake::new(BASE + 3600);
    fake.bucket
        .borrow_mut()
        .insert(raw_key(BASE, "bad"), b"not gzip".to_vec());
    let good = put_raw(
        &fake,
        BASE + 60,
        "ok",
        &[line(BASE + 60, "g"), "{oops".into(), line(BASE + 60, "h")],
    );
    let report = tick(&fake).unwrap();
    assert_eq!(
        (report.lines_read, report.rows, report.bad_lines),
        (3, 2, 1)
    );
    assert_eq!(report.lines_read, report.rows + report.bad_lines);
    let outbox: Vec<String> = fake
        .kv
        .borrow()
        .keys()
        .filter(|k| k.starts_with(&format!("logq_{SID}_")))
        .cloned()
        .collect();
    assert_eq!(outbox.len(), 1);
    let rows = rows(&fake, LogKind::IngestFiles);
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|r| r["state_version"] == 1));
    let converted = rows.iter().find(|r| r["raw_key"] == good.as_str()).unwrap();
    assert_eq!(converted["status"], "converted");
    assert_eq!(converted["rows"], 2);
    assert_eq!(converted["bad_lines"], 1);
    assert_eq!(converted["lines_read"], 3);
    let outputs: Value =
        serde_json::from_str(converted["output_keys_json"].as_str().unwrap()).unwrap();
    assert_eq!(outputs.as_array().unwrap().len(), 1);
    let failed = rows.iter().find(|r| r["status"] != "converted").unwrap();
    assert_eq!(failed["status"], "quarantined");
    assert_eq!(failed["reason"], "corrupt_gzip");
}

#[test]
fn corrupt_gzip_and_bad_lines_emit_bounded_platform_errors() {
    let fake = Fake::new(BASE + 3600);
    fake.bucket
        .borrow_mut()
        .insert(raw_key(BASE, "bad"), b"not gzip".to_vec());
    let huge = format!("{{broken {}", "x".repeat(10_000));
    put_raw(&fake, BASE + 60, "ok", &[line(BASE + 60, "g"), huge]);
    tick(&fake).unwrap();
    let errors = rows(&fake, LogKind::PlatformErrors);
    let codes: BTreeSet<&str> = errors.iter().filter_map(|r| r["code"].as_str()).collect();
    assert_eq!(codes, BTreeSet::from(["bad_ndjson_line", "corrupt_gzip"]));
    for row in &errors {
        assert_eq!(row["error_domain"], "ingest_corrupt");
        assert!(row["sample_json"].as_str().is_none_or(|s| s.len() <= 2048));
    }
    let bad = errors
        .iter()
        .find(|r| r["code"] == "bad_ndjson_line")
        .unwrap();
    assert!(bad["sample_json"].as_str().unwrap().contains("broken"));
}

#[test]
fn bad_line_samples_stay_valid_json_within_the_field_cap() {
    let fake = Fake::new(BASE + 3600);
    let dense = format!("{{{}", "\"\\\u{1}é".repeat(1500));
    let long = format!("{{broken {}", "x".repeat(10_000));
    put_raw(&fake, BASE + 60, "a", &[dense]);
    put_raw(&fake, BASE + 120, "b", &[long]);
    tick(&fake).unwrap();
    let errors = rows(&fake, LogKind::PlatformErrors);
    let samples: Vec<_> = errors
        .iter()
        .filter(|r| r["code"] == "bad_ndjson_line")
        .collect();
    assert_eq!(samples.len(), 2);
    for row in samples {
        let text = row["sample_json"].as_str().unwrap();
        assert!(text.len() <= 2048, "{}", text.len());
        let parsed: Value = serde_json::from_str(text).unwrap();
        assert!(parsed["line"].as_str().is_some_and(|l| l.starts_with('{')));
    }
}

fn tick_runtime(fake: &Fake) {
    let sid = SID;
    fake.kv.borrow_mut().insert(
        "admin:install".into(),
        (1, json!({"service_id": sid, "job_id": null, "fence": 0})),
    );
    fake.kv.borrow_mut().insert(
        format!("svc:{sid}"),
        (1, serde_json::to_value(service()).unwrap()),
    );
}

fn run_tick(fake: &Fake) -> Value {
    fake.new_request();
    let value = fastly_log_analytics::admin::record_tick(fake, "clock", "clock").unwrap();
    fake.note_peak("tick");
    value
}

/// The front service's `/_flush` probe: one flp execution running only the log flush.
fn run_flush(fake: &Fake) -> Value {
    fake.new_request();
    let value = fastly_log_analytics::admin::record_flush(fake).unwrap();
    fake.note_peak("flush");
    value
}

#[test]
fn cron_metrics_count_successful_raw_objects_without_quarantined_objects() {
    for kind in ["request_ingest", "rum_ingest"] {
        let fake = Fake::new(BASE + 3600);
        tick_runtime(&fake);
        let bad = if kind == "rum_ingest" {
            store_service(&fake, &rum_service());
            put_rum(
                &fake,
                BASE + 60,
                "ok",
                &[rum_beacon(BASE + 60, "LCP", "123")],
            );
            rum_key(BASE, "bad")
        } else {
            put_raw(&fake, BASE + 60, "ok", &[line(BASE + 60, "ok")]);
            raw_key(BASE, "bad")
        };
        fake.bucket.borrow_mut().insert(bad, b"not gzip".to_vec());
        for _ in 0..8 {
            run_tick(&fake);
            if rows(&fake, LogKind::CronRuns)
                .iter()
                .any(|row| row["cron_kind"] == kind && row["converted_files"] == 1)
            {
                break;
            }
            fake.now.set(fake.now.get() + 1);
        }
        let runs = rows(&fake, LogKind::CronRuns);
        let run = runs
            .iter()
            .find(|r| r["cron_kind"] == kind && r["converted_files"] == 1)
            .unwrap();
        assert_eq!(run["listed_files"], 2, "{kind}");
        assert_eq!(run["converted_files"], 1, "{kind}");
        assert_eq!(run["rows_written"], 1, "{kind}");
        assert_eq!(
            runs.iter()
                .filter(|row| row["cron_kind"] == kind)
                .filter_map(|row| row["converted_files"].as_u64())
                .sum::<u64>(),
            1
        );
    }
}

#[test]
fn cron_metrics_leave_unmeasured_bytes_null_but_preserve_successful_zero_counts() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    run_tick(&fake);
    let runs = rows(&fake, LogKind::CronRuns);
    let run = runs
        .iter()
        .find(|r| r["cron_kind"] == "request_ingest")
        .unwrap();
    assert_eq!(run["outcome"], "idle");
    assert_eq!(run.get("bytes_read"), Some(&Value::Null));
    for field in [
        "listed_files",
        "converted_files",
        "deleted_files",
        "lines_read",
        "rows_written",
        "bad_lines",
        "bytes_written",
        "parquet_files_written",
    ] {
        assert_eq!(run[field], 0, "{field}");
    }
    let parquet =
        telemetry::schema::to_parquet(LogKind::CronRuns, std::slice::from_ref(run)).unwrap();
    let archived = batches(&parquet);
    assert!(archived[0].column_by_name("bytes_read").unwrap().is_null(0));
    let decoded = telemetry::schema::from_parquet(LogKind::CronRuns, parquet).unwrap();
    assert_eq!(decoded[0].get("bytes_read"), None);
    assert_eq!(decoded[0]["rows_written"], 0);
}

#[test]
fn cron_metrics_errors_leave_unknown_counts_null_and_keep_reported_progress() {
    for partial in [false, true] {
        let fake = Fake::new(BASE + 3600);
        tick_runtime(&fake);
        if partial {
            put_raw(&fake, BASE, "ok", &[line(BASE, "ok")]);
            fake.fail_watermark.set(true);
        } else {
            fake.kv.borrow_mut().insert(
                ingest::PLAN_KEY.into(),
                (1, json!({"version": ingest::coordinator::PLAN_VERSION})),
            );
        }
        freshness::finish_tick(&fake);
        let runs = rows(&fake, LogKind::CronRuns);
        let run = runs
            .iter()
            .find(|r| r["cron_kind"] == "request_ingest" && r["outcome"] == "error")
            .unwrap();
        assert_eq!(run["outcome"], "error", "{run}");
        assert_eq!(
            run.get("listed_files"),
            Some(&if partial { json!(1) } else { Value::Null })
        );
        for field in [
            "converted_files",
            "deleted_files",
            "lines_read",
            "rows_written",
            "bad_lines",
            "bytes_read",
            "bytes_written",
            "parquet_files_written",
        ] {
            assert_eq!(run.get(field), Some(&Value::Null), "{field}");
        }
        assert!(run["backend_requests_used"].as_u64().unwrap() > 0);
        let parquet =
            telemetry::schema::to_parquet(LogKind::CronRuns, std::slice::from_ref(run)).unwrap();
        let archived = batches(&parquet);
        assert!(archived[0]
            .column_by_name("rows_written")
            .unwrap()
            .is_null(0));
        let decoded = telemetry::schema::from_parquet(LogKind::CronRuns, parquet).unwrap();
        assert_eq!(decoded[0].get("rows_written"), None);
        assert_eq!(decoded[0]["listed_files"], run["listed_files"]);
    }
}

#[test]
fn record_tick_logs_only_runs_carries_idle_ticks_and_keeps_fifty_runs() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    freshness::finish_tick(&fake);
    let committed_at = fake.kv.borrow()["cron:state"].1["last_ingest_at"].clone();
    assert!(
        rows(&fake, LogKind::CronTicks).is_empty(),
        "cron_ticks is retired"
    );
    let runs = rows(&fake, LogKind::CronRuns);
    let deferred_before = fastly_log_analytics::admin::schedule::peek(
        &fake,
        fastly_log_analytics::admin::schedule::SCHED_KEY,
    )
    .unwrap()
    .deferred_idle
    .len();
    let ingest_runs: Vec<_> = runs
        .iter()
        .filter(|r| r["cron_kind"] == "request_ingest")
        .collect();
    assert_eq!(
        ingest_runs
            .iter()
            .filter(|run| run["converted_files"] == 1)
            .count(),
        1
    );
    assert!(ingest_runs
        .iter()
        .all(|run| run["vcpu_ms"].as_i64().unwrap() > 0));
    let converted = ingest_runs
        .iter()
        .find(|run| {
            run["details_json"].as_str().is_some_and(|details| {
                serde_json::from_str::<Value>(details).unwrap()["summary_vcpu_ms"]
                    .as_u64()
                    .unwrap_or(0)
                    > 0
            })
        })
        .unwrap();
    let details: Value = serde_json::from_str(converted["details_json"].as_str().unwrap()).unwrap();
    let worker = details["worker_vcpu_ms"].as_u64().unwrap();
    let summary = details["summary_vcpu_ms"].as_u64().unwrap();
    assert!(summary > 0 && summary < worker, "{details}");

    for _ in 0..30 {
        fake.now.set(fake.now.get() + 1);
        run_tick(&fake);
        assert_eq!(fake.calls.get(), 0, "an idle tick makes no backend call");
    }
    assert_eq!(rows(&fake, LogKind::CronRuns).len(), runs.len());
    fake.now.set(fake.now.get() + 60);
    run_tick(&fake);
    let mut carried: Vec<_> = rows(&fake, LogKind::CronRuns)
        .into_iter()
        .filter(|r| r["cron_kind"] == "request_ingest")
        .collect();
    carried.extend(
        fastly_log_analytics::admin::schedule::peek(
            &fake,
            fastly_log_analytics::admin::schedule::SCHED_KEY,
        )
        .unwrap()
        .deferred_idle
        .into_iter()
        .map(|run| run.row),
    );
    assert_eq!(
        carried.len(),
        ingest_runs.len() + deferred_before + 1,
        "a scheduled ingest is logged even when idle"
    );
    let idle = carried
        .iter()
        .find(|r| r["outcome"] == "idle" && r["idle_ticks"] == 30)
        .unwrap();
    assert_eq!(idle["idle_ticks"], 30);
    assert!(idle["idle_vcpu_ms"].as_i64().unwrap() > 0);

    for _ in 0..55 {
        fake.now.set(fake.now.get() + 120);
        run_tick(&fake);
    }
    let state = fake.kv.borrow()["cron:state"].1.clone();
    assert_eq!(state["runs"].as_array().unwrap().len(), 50);
    assert!(
        state["runs"]
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r["converted_raw_files"] == 0),
        "later runs have pushed the converting run out of the ring buffer"
    );
    assert_eq!(
        state["last_ingest_at"], committed_at,
        "the last ingest outlives the ring buffer"
    );
    assert!(fastly_log_analytics::admin::last_ingest_at(&fake, Some(SID)).is_some());
    assert_eq!(
        fastly_log_analytics::admin::last_ingest_at(&fake, Some("OtherServiceId0000000000")),
        None,
        "another service must not inherit this service's last ingest"
    );
    assert_eq!(
        fastly_log_analytics::admin::last_ingest_at(&fake, None),
        None
    );
}

#[test]
fn capacity_telemetry_preserves_worker_costs_and_missing_replies() {
    let fake = Fake::new(BASE + 3600);
    fake.heap_mib.set(Some(37));
    for i in 0..(2 * ingest::worker::SHARD_MAX_OBJECTS + 1) {
        put_raw(&fake, BASE, &format!("sample-{i}"), &[line(BASE, "a")]);
    }
    fake.worker_faults.borrow_mut().extend([
        WorkerFault::Ok,
        WorkerFault::LostReply,
        WorkerFault::Ok,
    ]);
    fake.transient.borrow_mut().push_back(("GET", 503, None));
    let report = tick(&fake).unwrap();
    let value = serde_json::to_value(&report).unwrap();
    let samples = value["worker_executions_metrics"].as_array().unwrap();
    let shards: Vec<_> = samples
        .iter()
        .filter(|sample| sample["kind"] == "request_ingest")
        .collect();
    assert_eq!(shards.len(), report.shards_dispatched as usize);
    assert_eq!(shards.iter().filter(|s| s["answered"] == true).count(), 2);
    let missing = shards.iter().find(|s| s["answered"] == false).unwrap();
    for field in [
        "calls",
        "vcpu_ms",
        "summary_vcpu_ms",
        "duration_ms",
        "heap_mib",
    ] {
        assert!(missing[field].is_null(), "{missing}");
    }
    let measured: Vec<_> = shards.iter().filter(|s| s["answered"] == true).collect();
    assert_ne!(measured[0]["calls"], measured[1]["calls"], "{samples:?}");
    assert!(measured.iter().all(|s| s["heap_mib"] == 37));
    assert_eq!(
        samples
            .iter()
            .filter_map(|s| s["vcpu_ms"].as_u64())
            .sum::<u64>(),
        report.worker_vcpu_ms
    );
}

#[test]
fn capacity_telemetry_keeps_tick_execution_separate_from_worker_totals() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    fake.heap_mib.set(Some(29));
    fake.linear_memory_high_water_bytes
        .set(Some(80 * 1024 * 1024));
    put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    run_tick(&fake);
    let runs = rows(&fake, LogKind::CronRuns);
    let details: Value = runs
        .iter()
        .find_map(|row| {
            let details: Value = serde_json::from_str(row["details_json"].as_str()?).ok()?;
            details.get("execution_metrics").map(|_| details.clone())
        })
        .unwrap();
    assert_eq!(
        details["execution_metrics"]["scope"],
        "tick_before_telemetry_flush"
    );
    assert_eq!(details["execution_metrics"]["heap_mib"], 29);
    assert_eq!(
        details["execution_metrics"]["linear_memory_high_water_bytes"],
        80 * 1024 * 1024
    );
    assert_eq!(details["workers_dispatched"], fake.worker_sent_total.get());
    assert_eq!(details["workers_answered"], details["workers_dispatched"]);
    let samples = details["worker_executions_metrics"].as_array().unwrap();
    assert_eq!(samples.len(), fake.worker_sent_total.get() as usize);
    assert!(samples.iter().any(|sample| sample["kind"] == "maintenance"));
    assert!(samples
        .iter()
        .all(|sample| sample["linear_memory_high_water_bytes"] == 80 * 1024 * 1024));
    assert!(details["execution_metrics"]["vcpu_ms"].as_u64().unwrap() > 0);
    let stage: Value = serde_json::from_str(
        runs.iter()
            .find(|r| r["cron_kind"] == "request_ingest")
            .unwrap()["details_json"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert!(stage["stage_execution_metrics"]["heap_mib"] == 29);
}

#[test]
fn capacity_telemetry_reports_explicit_zero_fanout_without_inventing_heap() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    fake.shield.set(false);
    let reply = fastly_log_analytics::admin::record_tick(&fake, "manual", "admin").unwrap();
    assert_eq!(reply["execution_metrics"]["scope"], "tick_end");
    assert!(reply["execution_metrics"]["heap_mib"].is_null());
    assert_eq!(
        reply["execution_metrics"].get("linear_memory_high_water_bytes"),
        Some(&Value::Null)
    );
    let runs = rows(&fake, LogKind::CronRuns);
    let details: Value = runs
        .iter()
        .find_map(|row| {
            let details: Value = serde_json::from_str(row["details_json"].as_str()?).ok()?;
            details.get("execution_metrics").map(|_| details.clone())
        })
        .unwrap();
    assert_eq!(details["workers_dispatched"], 0);
    assert_eq!(details["workers_answered"], 0);
    assert_eq!(details["worker_executions_metrics"], json!([]));
    assert_eq!(details["root_run_id"], reply["run"]["id"]);
}

#[test]
fn capacity_telemetry_keeps_maintenance_only_worker_coverage_in_root_stdout() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    run_tick(&fake);
    fake.now.set(fake.now.get() + 1);
    {
        let mut kv = fake.kv.borrow_mut();
        let state = kv.get_mut("cron:state").unwrap();
        state.0 += 1;
        state.1["jobs"]["maint"]["more_pending"] = true.into();
    }
    fake.cache.borrow_mut().clear();
    fake.events.borrow_mut().clear();
    run_tick(&fake);
    let events = fake.events.borrow();
    let event = events
        .iter()
        .find(|event| event["event"] == "cron.execution")
        .unwrap();
    assert_eq!(event["workers_dispatched"], 1);
    assert_eq!(event["workers_answered"], 1);
    assert_eq!(event["worker_executions_metrics"][0]["kind"], "maintenance");
    assert_eq!(event["execution_metrics"]["scope"], "tick_end");
    assert_eq!(event["root_run_id"], event["run_id"]);
}

#[test]
fn cron_runs_count_every_worker_execution_the_tick_dispatched() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    for (i, name) in ["a", "b", "c", "d"].iter().enumerate() {
        put_raw(&fake, BASE + i as i64, name, &[line(BASE + i as i64, name)]);
    }
    for _ in 0..6 {
        run_tick(&fake);
        fake.now.set(fake.now.get() + 120);
    }
    let sent = fake.worker_sent_total.get();
    assert!(sent > 0, "the ticks dispatched worker executions");
    let logged: i64 = rows(&fake, LogKind::CronRuns)
        .iter()
        .map(|r| r["worker_executions"].as_i64().unwrap_or(0))
        .sum();
    assert_eq!(
        logged,
        i64::from(sent),
        "each worker execution is logged once"
    );
}

fn ingest_runs(fake: &Fake) -> usize {
    rows(fake, LogKind::CronRuns)
        .iter()
        .filter(|r| r["cron_kind"] == "request_ingest")
        .count()
}

#[test]
fn idle_ticks_and_flush_probes_make_no_backend_call_or_kv_write() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    freshness::finish_tick(&fake);
    run_flush(&fake);
    for _ in 0..20 {
        fake.now.set(fake.now.get() + 1);
        let kv = fake.kv.borrow().clone();
        let tick = run_tick(&fake);
        assert_eq!(tick["outcome"], "idle", "{tick}");
        assert_eq!(fake.calls.get(), 0);
        assert_eq!(fake.cache_ops.get(), 1, "one schedule claim");
        let flush = run_flush(&fake);
        assert_eq!(flush["outcome"], "idle", "{flush}");
        assert_eq!(fake.calls.get(), 0);
        assert_eq!(fake.cache_ops.get(), 1);
        assert!(*fake.kv.borrow() == kv, "an idle probe wrote KV");
    }
}

fn outbox_keys(fake: &Fake) -> Vec<String> {
    fake.kv
        .borrow()
        .keys()
        .filter(|k| k.starts_with(&format!("logq_{SID}_")))
        .cloned()
        .collect()
}

#[test]
fn catch_up_dispatches_in_one_bucket_share_its_outbox_key() {
    let fake = Fake::new(BASE + 3 * 3600);
    tick_runtime(&fake);
    // A few ticks of backlog: every dispatch spends ingest and raw delete down to their reserves,
    // and all of their rows fit one outbox key.
    seed(&fake, 9, 50);
    // Start on an outbox bucket boundary so every dispatch below falls in the same bucket.
    fake.now.set(fake.now.get() / 120 * 120);
    let mut dispatches = 0;
    while !fake.keys("raw/").is_empty() {
        assert!(dispatches < 20, "the catch-up did not finish");
        fake.now.set(fake.now.get() + 1);
        run_tick(&fake);
        dispatches += 1;
    }
    assert!(dispatches >= 3, "a {dispatches}-tick catch-up");
    let keys = outbox_keys(&fake);
    assert_eq!(keys.len(), 1, "{dispatches} dispatches made keys {keys:?}");
}

#[test]
fn a_committed_flush_left_behind_runs_again_on_the_next_probe() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    let bucket = (BASE as u64 + 3600) / 120 - 40;
    for i in 0..40 {
        let line = json!({"kind": "ingest_files", "timestamp": (bucket * 120) * 1000 + i,
                          "event_id": format!("f{i:02}"), "service_id": SID,
                          "severity": "info", "raw_key": "k"});
        fake.kv.borrow_mut().insert(
            telemetry::outbox_key(SID, bucket, Some(&format!("x{i:02}"))),
            (1, json!({"ndjson": line.to_string()})),
        );
    }
    let first = run_flush(&fake);
    assert_eq!(first["outcome"], "partial", "{first}");
    assert_eq!(first["more_pending"], true, "{first}");
    let left = outbox_keys(&fake).len();
    assert!(left > 0, "the backlog outlasts one flush");
    fake.now.set(fake.now.get() + 1);
    let next = run_flush(&fake);
    assert_ne!(next["outcome"], "idle", "{next}");
    assert!(outbox_keys(&fake).len() < left);
}

#[test]
fn an_install_without_fos_credentials_backs_off_instead_of_dispatching_every_second() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    fake.no_fos.set(true);
    run_tick(&fake);
    let writes = fake.cron_cas.get();
    for _ in 0..20 {
        fake.now.set(fake.now.get() + 1);
        run_tick(&fake);
    }
    assert_eq!(
        fake.cron_cas.get(),
        writes,
        "an unprovisioned install wrote cron:state on a 1 s tick"
    );
}

#[test]
fn a_failing_dispatch_backs_off_instead_of_retrying_every_second() {
    use fastly_log_analytics::admin::schedule::RETRY_S;
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    freshness::finish_tick(&fake);
    // The cached mirror now says ingest is due again when the next period closes.
    put_raw(&fake, BASE + 3600, "b", &[line(BASE + 3600, "b")]);
    fake.now.set(fake.now.get() + 300);
    fake.kv_reads.borrow_mut().clear();
    fake.fail_cron_state.set(true);
    let dispatches = || {
        fake.kv_reads
            .borrow()
            .iter()
            .filter(|key| *key == "cron:state")
            .count()
    };
    let tick = || {
        fake.new_request();
        fastly_log_analytics::admin::record_tick(&fake, "clock", "clock")
    };
    let start = fake.now.get();
    for _ in 0..20 {
        let _ = tick();
        fake.now.set(fake.now.get() + 1);
    }
    assert_eq!(dispatches(), 1, "dispatches in 20 one-second ticks");
    fake.now.set(start + RETRY_S);
    assert!(tick().is_err());
    assert_eq!(dispatches(), 2, "retried once the backoff passed");
    fake.fail_cron_state.set(false);
    fake.now.set(start + 2 * RETRY_S);
    tick().unwrap();
    freshness::finish_tick(&fake);
    assert_eq!(processed_ips(&fake), ["a", "b"]);
}

#[test]
fn off_shield_probes_return_without_any_call_or_cache_op() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    fake.shield.set(false);
    for probe in [run_tick, run_flush] {
        let value = probe(&fake);
        assert_eq!(value["outcome"], "off_shield", "{value}");
        assert_eq!((fake.calls.get(), fake.cache_ops.get()), (0, 0));
    }
    assert_eq!(fake.keys("raw/").len(), 1);
    assert!(fake.cache.borrow().is_empty());
}

#[test]
fn duplicate_and_concurrent_ticks_run_a_due_job_once() {
    use fastly_log_analytics::admin::schedule::{self, Claim, SCHED_KEY};
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    // Another execution holds the lease: ticks and manual runs back off without a call.
    fake.new_request();
    let held = schedule::claim(&fake, SCHED_KEY, "other", false, schedule::tick_wanted).unwrap();
    assert!(matches!(held, Claim::Run { .. }));
    assert_eq!(run_tick(&fake)["outcome"], "busy");
    assert_eq!(fake.calls.get(), 0);
    fake.new_request();
    assert_eq!(
        fastly_log_analytics::admin::record_tick(&fake, "manual", "admin"),
        Err("cron_busy".into())
    );
    assert_eq!(ingest_runs(&fake), 0);
    // The holder's lease lapses: the next tick takes over, and the stale holder's release
    // leaves the new owner's lease alone.
    fake.now.set(fake.now.get() + schedule::LEASE_S + 1);
    fake.new_request();
    let taken = schedule::claim(&fake, SCHED_KEY, "next", false, schedule::tick_wanted).unwrap();
    assert!(matches!(taken, Claim::Run { .. }));
    schedule::release(&fake, SCHED_KEY, "other", &|_| {}, (0, 0)).unwrap();
    let record = schedule::peek(&fake, SCHED_KEY).unwrap();
    assert_eq!(record.owner.as_deref(), Some("next"));
    schedule::release(&fake, SCHED_KEY, "next", &|_| {}, (0, 0)).unwrap();
    // A stale holder releasing after that cannot overwrite the new owner's record.
    let released = schedule::peek(&fake, SCHED_KEY).unwrap();
    fake.now.set(fake.now.get() + 1);
    let stale = |record: &mut schedule::Sched| record.wake_at = 1;
    schedule::release(&fake, SCHED_KEY, "other", &stale, (0, 0)).unwrap();
    assert_eq!(schedule::peek(&fake, SCHED_KEY).unwrap(), released);
    // Two ticks in the same second, once the record is due a refresh: one ingest.
    fake.now.set(fake.now.get() + schedule::REFRESH_S);
    let cas = fake.cron_cas.get();
    run_tick(&fake);
    assert_eq!(fake.cron_cas.get(), cas + 1, "one cron:state write per run");
    freshness::finish_tick(&fake);
    let completed = ingest_runs(&fake);
    run_tick(&fake);
    assert_eq!(fake.calls.get(), 0);
    assert_eq!(ingest_runs(&fake), completed);
    assert_eq!(processed_ips(&fake), ["a"]);
}

#[test]
fn an_evicted_schedule_is_rebuilt_from_kv_without_rerunning_jobs() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    freshness::finish_tick(&fake);
    let completed = ingest_runs(&fake);
    fake.now.set(fake.now.get() + 1);
    fake.cache.borrow_mut().clear();
    let kv = fake.kv.borrow().clone();
    let rebuilt = run_tick(&fake);
    assert!(rebuilt["run"].is_null(), "{rebuilt}");
    assert!(fake.calls.get() <= 3, "{} calls", fake.calls.get());
    assert!(*fake.kv.borrow() == kv, "the rebuild wrote KV");
    fake.now.set(fake.now.get() + 1);
    run_tick(&fake);
    assert_eq!(
        fake.calls.get(),
        0,
        "the rebuilt record serves the next tick"
    );
    assert_eq!(ingest_runs(&fake), completed);
    assert_eq!(processed_ips(&fake), ["a"]);
}

#[test]
fn a_backlog_is_caught_up_on_consecutive_seconds() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    let expected = seed(&fake, 10, 60);
    let mut runs = Vec::new();
    for second in 0..120 {
        fake.now.set(fake.now.get() + 1);
        if run_tick(&fake).get("run").is_some() {
            runs.push(second);
        }
        if fake.keys("raw/").is_empty() {
            break;
        }
    }
    assert!(fake.keys("raw/").is_empty(), "backlog left after {runs:?}");
    assert!(runs.len() > 2, "{runs:?}");
    assert!(
        runs.windows(2).all(|w| w[1] - w[0] <= 1),
        "catch-up waits: {runs:?}"
    );
    assert_eq!(processed_ips(&fake), expected);
}

#[test]
fn ingest_is_scheduled_by_the_clamped_log_period_after_its_grace() {
    use fastly_log_analytics::admin::schedule::{
        due, next_ingest_at, period_s, ready_period, Mirror, INGEST_GRACE_S,
    };
    assert_eq!(period_s(1), 5);
    assert_eq!(period_s(5), 5);
    assert_eq!(period_s(60), 60);
    let mut mirror = Mirror {
        configured: true,
        log_period_s: 60,
        ..Default::default()
    };
    let now = 1_000_000 * 60 + INGEST_GRACE_S;
    assert_eq!(ready_period(now, 60), 1_000_000);
    mirror.jobs.ingest.record_success(1_000_000, 60);
    mirror.jobs.maint.last_run = Some(now);
    assert!(!due(&mirror, now).any());
    assert_eq!(next_ingest_at(&mirror), now + 60);
    assert!(!due(&mirror, now + 59).any());
    assert_eq!(due(&mirror, now + 60).ingest, Some(1_000_001));
    mirror.jobs.ingest.more_pending = true;
    assert_eq!(due(&mirror, now + 1).ingest, Some(1_000_000), "catch-up");
    mirror.jobs.ingest.retry_after = now + 30;
    assert!(due(&mirror, now + 29).ingest.is_none(), "error backoff");
    mirror.log_period_s = 2;
    mirror.jobs.ingest = Default::default();
    mirror.jobs.ingest.record_success(ready_period(now, 2), 2);
    assert_eq!(
        next_ingest_at(&mirror),
        (ready_period(now, 2) + 1) * 5 + INGEST_GRACE_S
    );
    mirror.configured = false;
    assert!(!due(&mirror, now + 3600).any());
}

#[test]
fn request_ingest_keeps_the_logging_cadence_after_consecutive_idles() {
    use fastly_log_analytics::admin::schedule::{next_ingest_at, peek, tick_wake_at, SCHED_KEY};

    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    let mut svc = service();
    svc.log_period = 10;
    fake.kv.borrow_mut().insert(
        format!("svc:{SID}"),
        (1, serde_json::to_value(svc).unwrap()),
    );
    put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    let first = freshness::finish_tick(&fake);
    assert!(
        first["runs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|run| run["outcome"] == "done"),
        "{first}"
    );
    let record = peek(&fake, SCHED_KEY).unwrap();
    assert_eq!(
        record.mirror.jobs.ingest.idle_streak, 1,
        "the resumed snapshot is followed by one empty forward poll"
    );
    assert!(tick_wake_at(&record) <= fake.now.get() + 10);
    let total_ingest_runs = |fake: &Fake| {
        ingest_runs(fake) + peek(fake, SCHED_KEY).map_or(0, |s| s.deferred_idle.len())
    };
    for step in 0..4 {
        let next = next_ingest_at(&peek(&fake, SCHED_KEY).unwrap().mirror);
        let advance_s = next - fake.now.get();
        assert!((1..=10).contains(&advance_s));
        let before_ingest_runs = total_ingest_runs(&fake);
        for dt in 1..advance_s {
            fake.now.set(fake.now.get() + 1);
            run_tick(&fake);
            assert_eq!(
                total_ingest_runs(&fake),
                before_ingest_runs,
                "step {step} (+{dt}s) ran request_ingest early"
            );
        }
        fake.now.set(fake.now.get() + 1);
        let ran = freshness::finish_tick(&fake);
        assert!(
            (before_ingest_runs + 1..=before_ingest_runs + 2).contains(&total_ingest_runs(&fake))
        );
        assert_eq!(ran["run"]["outcome"], "idle", "step {step}: {ran}");
        let record = peek(&fake, SCHED_KEY).unwrap();
        assert_eq!(record.mirror.jobs.ingest.idle_streak, step + 2);
        let now = fake.now.get();
        assert!((now + 1..=now + 10).contains(&next_ingest_at(&record.mirror)));
    }

    put_raw(&fake, BASE + 120, "b", &[line(BASE + 120, "b")]);
    fake.now
        .set(next_ingest_at(&peek(&fake, SCHED_KEY).unwrap().mirror));
    let productive = freshness::finish_tick(&fake);
    assert_eq!(
        productive["runs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|run| run["converted_raw_files"].as_u64().unwrap())
            .sum::<u64>(),
        2,
        "{productive}"
    );
    assert_eq!(processed_ips(&fake), ["a", "b"]);
    let record = peek(&fake, SCHED_KEY).unwrap();
    assert_eq!(record.mirror.jobs.ingest.idle_streak, 1);
    assert_eq!(record.mirror.jobs.ingest.retry_after, 0);
    assert!(record.deferred_idle.is_empty());
    assert!((fake.now.get() + 1..=fake.now.get() + 10).contains(&tick_wake_at(&record)));
}

#[test]
fn steady_state_idle_dispatches_batch_kv_writes_and_reconcile_on_flush() {
    use fastly_log_analytics::admin::schedule::{peek, IDLE_WRITE_BATCH, SCHED_KEY};
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    // Hold maintenance stage dispatch beyond the window so 6 consecutive pure request_ingest idle
    // dispatches are exercised uninterrupted.
    fake.kv.borrow_mut().insert(
        "cron:state".into(),
        (
            1,
            json!({
                "jobs": {
                    "maint": {
                        "retry_after": (BASE + 7200) as u64
                    }
                }
            }),
        ),
    );
    // First idle dispatch establishes the schedule and idle_streak = 1 in KV.
    let first = run_tick(&fake);
    assert_eq!(first["run"]["outcome"], "idle", "{first}");
    assert_eq!(ingest_runs(&fake), 1);
    let cas_before = fake.cron_cas.get();
    let outbox_before = rows(&fake, LogKind::CronRuns).len();

    // Next 5 steady-state idle dispatches (with 3 non-due probe ticks before each) buffer in
    // Core Cache with zero `cron:state` CAS writes and zero new `logq` rows.
    for i in 1..(IDLE_WRITE_BATCH as usize) {
        for _ in 0..3 {
            fake.now.set(fake.now.get() + 1);
            assert_eq!(run_tick(&fake)["outcome"], "idle");
        }
        fake.now.set(fake.now.get() + 60);
        let tick = run_tick(&fake);
        assert_eq!(tick["run"]["outcome"], "idle", "batch step {i}: {tick}");
        assert_eq!(
            fake.cron_cas.get(),
            cas_before,
            "steady-state idle step {i} should defer cron:state CAS"
        );
        assert_eq!(
            rows(&fake, LogKind::CronRuns).len(),
            outbox_before,
            "steady-state idle step {i} should defer logq write"
        );
        let sched = peek(&fake, SCHED_KEY).unwrap();
        assert_eq!(sched.deferred_idle.len(), i);
    }

    // The 6th steady-state idle dispatch reaches IDLE_WRITE_BATCH (6) and flushes all 6 runs in a
    // single `cron:state` CAS write and a single `logq` append, preserving every run's idle_ticks
    // and idle_vcpu_ms diagnostics.
    for _ in 0..3 {
        fake.now.set(fake.now.get() + 1);
        assert_eq!(run_tick(&fake)["outcome"], "idle");
    }
    fake.now.set(fake.now.get() + 60);
    let sixth = run_tick(&fake);
    assert_eq!(sixth["run"]["outcome"], "idle", "{sixth}");
    assert_eq!(
        fake.cron_cas.get(),
        cas_before + 1,
        "batch of 6 steady-state idle dispatches writes cron:state once"
    );
    let sched = peek(&fake, SCHED_KEY).unwrap();
    assert!(sched.deferred_idle.is_empty());

    let first_run_id = format!("{}-request_ingest", first["run"]["id"].as_str().unwrap());
    let cron_runs: Vec<_> = rows(&fake, LogKind::CronRuns)
        .into_iter()
        .filter(|r| r["cron_kind"] == "request_ingest")
        .collect();
    assert_eq!(cron_runs.len(), 1 + IDLE_WRITE_BATCH as usize);
    let batched: Vec<_> = cron_runs
        .iter()
        .filter(|r| r["run_id"].as_str() != Some(first_run_id.as_str()))
        .collect();
    assert_eq!(batched.len(), IDLE_WRITE_BATCH as usize);
    let batch_idle_ticks: i64 = batched
        .iter()
        .map(|r| r["idle_ticks"].as_i64().unwrap())
        .sum();
    assert_eq!(
        batch_idle_ticks,
        (IDLE_WRITE_BATCH as i64) * 3,
        "all 18 intermediate idle probe ticks reconcile across the 6 batched runs"
    );
    assert!(batched
        .iter()
        .all(|r| r["idle_ticks"] == 3 && r["idle_vcpu_ms"].as_i64().unwrap() > 0));

    let state = &fake.kv.borrow()["cron:state"].1;
    assert_eq!(
        state["runs"].as_array().unwrap().len(),
        1 + IDLE_WRITE_BATCH as usize,
        "cron:state ring buffer receives all 6 batched runs on flush"
    );
}

#[test]
fn rum_ingest_is_due_by_its_own_log_period_independent_of_requests() {
    use fastly_log_analytics::admin::schedule::{due, ready_period, tick_wake_at, Mirror, Sched};
    let now = 1_000_000 * 300 + ingest::settle_s(300);
    let mut mirror = Mirror {
        configured: true,
        log_period_s: 60,
        ..Default::default()
    };
    mirror.jobs.ingest.record_success(ready_period(now, 60), 60);
    mirror.jobs.maint.last_run = Some(now);
    assert_eq!(due(&mirror, now).rum, None, "RUM off");

    mirror = mirror.with_rum(Some(300));
    assert_eq!(mirror.rum_log_period_s, 300);
    assert_eq!(due(&mirror, now).rum, Some(1_000_000), "first RUM run");
    mirror.jobs.rum.record_success(1_000_000, 300);
    assert!(!due(&mirror, now).any());
    // Requests come due every minute without RUM, and RUM only after its own period.
    assert_eq!(due(&mirror, now + 60).rum, None);
    assert!(due(&mirror, now + 60).ingest.is_some());
    assert_eq!(due(&mirror, now + 300).rum, Some(1_000_001));
    mirror
        .jobs
        .ingest
        .record_success(ready_period(now + 400, 60), 60);
    mirror.jobs.maint.last_run = Some(now + 290);
    let record = Sched {
        refreshed_at: now + 290,
        mirror: mirror.clone(),
        ..Default::default()
    };
    assert_eq!(tick_wake_at(&record), now + 300);
    mirror.jobs.rum.more_pending = true;
    assert_eq!(due(&mirror, now + 1).rum, Some(1_000_000), "catch-up");
    mirror.jobs.rum.retry_after = now + 30;
    assert_eq!(due(&mirror, now + 29).rum, None, "error backoff");
    assert_eq!(mirror.with_rum(None).rum_log_period_s, 0);
}

#[test]
fn schedule_job_merge_keeps_the_later_run_and_the_furthest_period() {
    use fastly_log_analytics::admin::schedule::{JobState, Jobs};
    let mut stored = Jobs {
        ingest: JobState {
            last_run: Some(20),
            last_success_period: Some(9),
            last_success_period_s: Some(60),
            ..Default::default()
        },
        ..Default::default()
    };
    let ours = Jobs {
        ingest: JobState {
            last_run: Some(10),
            last_success_period: Some(11),
            last_success_period_s: Some(60),
            more_pending: true,
            ..Default::default()
        },
        maint: JobState {
            last_run: Some(30),
            ..Default::default()
        },
        ..Default::default()
    };
    stored.merge(&ours);
    assert_eq!(stored.ingest.last_run, Some(20));
    assert!(!stored.ingest.more_pending);
    assert_eq!(stored.ingest.last_success_period, Some(11));
    assert_eq!(stored.maint.last_run, Some(30));
}

#[test]
fn stages_run_in_budget_order_and_admit_only_when_everything_fits() {
    use fastly_log_analytics::admin::{admit_stage, TICK_STAGES};
    assert_eq!(TICK_STAGES, ["request_ingest", "rum_ingest", "raw_delete"]);
    assert!(admit_stage(10, 10));
    assert!(!admit_stage(9, 10));
}

#[test]
fn idle_ticks_and_flush_probes_run_an_overdue_flush_sweep_and_reconcile_within_a_bounded_window() {
    let fake = Fake::new(BASE + 40 * DAY);
    tick_runtime(&fake);
    let done: serde_json::Map<String, Value> = LogKind::ALL
        .iter()
        .map(|k| (k.as_str().to_owned(), json!((BASE + 40 * DAY) / DAY)))
        .collect();
    fake.kv
        .borrow_mut()
        .insert("logs:compact_state".into(), (1, json!({"done": done})));
    let now_ms = (BASE + 40 * DAY) * 1000;
    for i in 0..3i64 {
        let line = json!({"kind":"ingest_files","timestamp":now_ms - 7_200_000 + i,
                          "event_id":format!("f{i}"),"service_id":SID,"severity":"info","raw_key":"k"});
        fake.kv.borrow_mut().insert(
            format!("logq_{SID}_{:010}_{i:02}", (now_ms / 1000 - 7_200) / 120),
            (1, json!({"ndjson": line.to_string()})),
        );
    }
    fake.kv.borrow_mut().insert(
        format!("logs:cursor:{SID}"),
        (
            1,
            json!({"scan_min": 1, "cache_min": 1, "pending": {"rows": 3,
                   "oldest_min": (BASE + 40 * DAY) as u64 / 60 - 120, "nonempty": []}}),
        ),
    );
    fake.new_request();
    assert!(ingest::retention::request_storage_reconcile(&fake, "admin").unwrap());
    for _ in 0..8 {
        fake.now.set(fake.now.get() + 120);
        run_tick(&fake);
        run_flush(&fake);
    }
    assert!(
        fake.bucket
            .borrow()
            .keys()
            .any(|k| k.contains("processed/ops/")),
        "the overdue flush committed ops files"
    );
    let kinds = rows(&fake, LogKind::CronRuns)
        .iter()
        .map(|r| r["cron_kind"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    assert!(kinds.contains(&"storage_reconcile".to_owned()), "{kinds:?}");
    // With nothing expired the sweep still logs its run, so its six-hour cadence stays visible.
    let sweeps: Vec<Value> = rows(&fake, LogKind::CronRuns)
        .into_iter()
        .filter(|r| r["cron_kind"] == "retention_sweep")
        .collect();
    assert!(
        sweeps
            .iter()
            .any(|r| r["outcome"] == "done" && r["deleted_files"] == 0),
        "{sweeps:?}"
    );
    let ran: Vec<Value> = fake
        .stage_runs
        .borrow()
        .iter()
        .map(|r| r["ran"].clone())
        .collect();
    assert!(ran.contains(&json!("retention_sweep")), "{ran:?}");
    assert!(fake.kv.borrow()["retention:last_sweep"].1["complete"] == true);
}

#[test]
fn fos_usage_aggregates_per_op_dataset_caller_status_and_cache_state() {
    use fastly_log_analytics::auth::settings::UsageLogging;
    let usage = UsageAgg::default();
    usage.set_caller("cron:ingest");
    for _ in 0..3 {
        usage.record_fos(
            "PUT",
            "processed/request/a.parquet",
            "",
            Some(200),
            10,
            100,
            0,
        );
    }
    usage.record_fos(
        "GET",
        "",
        "list-type=2&prefix=raw%2Frequest%2F",
        Some(200),
        5,
        0,
        50,
    );
    usage.note_sizes([("raw/request/a.gz", 777u64)]);
    usage.record_fos("DELETE", "raw/request/a.gz", "", Some(204), 4, 0, 0);
    usage.record_cdn("raw/request/b.gz", Some(200), Some("HIT"), 2, 900);
    usage.record_cdn("raw/request/c.gz", Some(200), Some("MISS"), 20, 900);
    usage.record_cdn("raw/request/d.gz", Some(200), None, 20, 900);
    let rows = usage.drain_rows(&UsageLogging::default());
    let find = |op: &str, cache: &str| {
        rows.iter()
            .find(|r| r["op"] == op && r["cdn_cache"] == cache)
            .unwrap_or_else(|| panic!("{op} {cache}: {rows:?}"))
    };
    let put = find("PUT", "DIRECT");
    assert_eq!(
        (
            put["op_count"].as_i64(),
            put["op_class"].as_str(),
            put["dataset"].as_str()
        ),
        (Some(3), Some("A"), Some("pq_request"))
    );
    assert_eq!(put["bytes_in"], 300);
    assert!((put["est_cost_usd"].as_f64().unwrap() - 3.0 * 0.0025 / 1000.0).abs() < 1e-12);
    let list = find("LIST", "DIRECT");
    assert_eq!(
        (list["op_class"].as_str(), list["dataset"].as_str()),
        (Some("A"), Some("raw_request"))
    );
    let delete = find("DELETE", "DIRECT");
    assert_eq!(delete["op_class"], "A");
    assert_eq!(delete["bytes_deleted"], 777);
    let hit = find("GET", "HIT");
    assert_eq!(hit["op_class"], "CDN_HIT");
    assert_eq!(hit["est_cost_usd"].as_f64(), Some(0.0));
    let miss = rows
        .iter()
        .find(|r| r["op"] == "GET" && r["cdn_cache"] == "MISS")
        .unwrap();
    assert_eq!(miss["op_class"], "B");
    assert_eq!(miss["op_count"], 2);
    assert!((miss["est_cost_usd"].as_f64().unwrap() - 2.0 * 0.004 / 10_000.0).abs() < 1e-12);
    assert!(rows.iter().all(|r| r["caller"] == "cron:ingest"));
    assert!(usage.drain_rows(&UsageLogging::default()).is_empty());
}

#[test]
fn raw_reads_use_the_cdn_path() {
    let fake = Fake::new(BASE + 3600);
    let key = put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    tick(&fake).unwrap();
    assert_eq!(*fake.cdn_gets.borrow(), vec![key]);
}

fn seed_files(fake: &Fake, count: i64) {
    for i in 0..count {
        let start = BASE + i * 60;
        let key = ingest::processed_key(
            "processed/request/",
            start,
            start + 60,
            "abcdef012345",
            i as u64 + 1,
        );
        fake.bucket.borrow_mut().insert(key, vec![1, 2, 3]);
    }
    let keys = fake.keys("processed/request/");
    for batch in keys.chunks(128) {
        publish_files(fake, batch, &[], &[]);
    }
}

fn files_page(fake: &Fake, query: &str) -> Result<Value, (u16, &'static str)> {
    fake.new_request();
    let page = api::files(fake, &service(), query, Instant::now());
    fake.release_unread();
    page
}

fn publish_files(fake: &Fake, keys: &[String], summaries: &[String], replaces: &[String]) -> Value {
    let outputs = |keys: &[String]| {
        keys.iter()
            .map(|key| ingest::worker::Output {
                key: key.clone(),
                rows: ingest::parse_processed(key)
                    .and_then(|name| name.rows)
                    .or_else(|| telemetry::compact::parse_compact_name(key).map(|name| name.rows))
                    .unwrap_or(1),
                bytes: fake
                    .bucket
                    .borrow()
                    .get(key)
                    .map_or(0, |bytes| bytes.len() as u64),
                ..Default::default()
            })
            .collect::<Vec<_>>()
    };
    publish_outputs(fake, &outputs(keys), &outputs(summaries), replaces)
}

fn publish_outputs(
    fake: &Fake,
    files: &[ingest::worker::Output],
    summaries: &[ingest::worker::Output],
    replaces: &[String],
) -> Value {
    let step = fake.vcpu_step.replace(0);
    fake.vcpu.set(0);
    for _ in 0..100 {
        fake.new_request();
        let record = fake.admin_get(ingest::WATERMARK_KEY).unwrap();
        let previous = record
            .as_ref()
            .map(|r| r.value.clone())
            .unwrap_or_else(|| json!({}));
        match ingest::catalog::prepare(
            &store(fake, &service()),
            &previous,
            "fixture-commit",
            "schema",
            files,
            summaries,
            &[],
            replaces,
        ) {
            Ok(value) => {
                assert!(fake
                    .admin_cas(
                        ingest::WATERMARK_KEY,
                        record.as_ref().map(|r| r.generation.as_str()),
                        &value
                    )
                    .unwrap());
                fake.vcpu_step.set(step);
                fake.vcpu.set(0);
                fake.new_request();
                return value;
            }
            Err(error) if error == "catalog_prepare_pending" => {}
            Err(error) => panic!("fixture publication failed: {error}"),
        }
    }
    panic!("fixture catalog publication did not make progress");
}

fn catalog_keys(fake: &Fake, query: &str) -> (u64, BTreeSet<String>) {
    let mut next = None::<String>;
    let mut keys = BTreeSet::new();
    let mut generation = None;
    for _ in 0..200 {
        let query = next.as_ref().map_or_else(
            || query.to_owned(),
            |cursor| format!("{query}&cursor={cursor}"),
        );
        let page = files_page(fake, &query).unwrap();
        let current = page["catalog_generation"].as_u64().unwrap();
        assert_eq!(*generation.get_or_insert(current), current);
        for file in page["files"].as_array().unwrap() {
            assert!(keys.insert(file["key"].as_str().unwrap().to_owned()));
        }
        next = page["next_cursor"].as_str().map(str::to_owned);
        if next.is_none() {
            assert_eq!(page["complete"], true);
            return (current, keys);
        }
    }
    panic!("catalog pagination did not make bounded progress");
}

/// Twelve committed hours, then an open hour still taking a two-minute window per commit;
/// returns the files query and every published file and summary key.
fn publish_active_day(fake: &Fake) -> (String, BTreeSet<String>, BTreeSet<String>) {
    use fastly_log_analytics::ingest::summary;
    let open = BASE - BASE.rem_euclid(3600);
    let mut want_files = BTreeSet::new();
    let mut want_summaries = BTreeSet::new();
    // Twelve committed hours, then an open hour still taking a two-minute window per commit.
    for hour in (0..13).map(|h| open - (12 - h) * 3600) {
        let windows = if hour == open { 30 } else { 4 };
        let (mut hour_files, mut hour_sums) = (Vec::new(), Vec::new());
        for w in 0..windows {
            let start = hour + w * 120;
            let file = ingest::processed_key("processed/request/", start, start + 120, "abc", 3);
            let sum =
                summary::summary_key("processed/summary/request/", start, start + 120, "p1s0", 3);
            fake.bucket.borrow_mut().insert(file.clone(), vec![1]);
            fake.bucket.borrow_mut().insert(sum.clone(), vec![1]);
            want_files.insert(file.clone());
            want_summaries.insert(sum.clone());
            if hour == open {
                publish_files(fake, &[file], &[sum], &[]);
            } else {
                hour_files.push(file);
                hour_sums.push(sum);
            }
        }
        if hour != open {
            publish_files(fake, &hour_files, &hour_sums, &[]);
        }
    }
    let query = format!(
        "summaries=1&from={}&to={}",
        iso_url(open - 12 * 3600),
        iso_url(open + 3600)
    );
    (query, want_files, want_summaries)
}

/// Every page of `query`'s file walk: the files, summary files and page count.
fn walk_files(fake: &Fake, query: &str) -> (BTreeSet<String>, BTreeSet<String>, usize) {
    let (mut files, mut summaries, mut next) = (BTreeSet::new(), BTreeSet::new(), None::<String>);
    let mut pages = 0;
    fake.fos_rounds.borrow_mut().clear();
    loop {
        let query = next
            .as_ref()
            .map_or_else(|| query.to_owned(), |c| format!("{query}&cursor={c}"));
        let page = files_page(fake, &query).unwrap();
        pages += 1;
        for (field, out) in [("files", &mut files), ("summary_files", &mut summaries)] {
            for file in page[field].as_array().unwrap() {
                assert!(out.insert(file["key"].as_str().unwrap().to_owned()));
            }
        }
        next = page["next_cursor"].as_str().map(str::to_owned);
        if next.is_none() {
            return (files, summaries, pages);
        }
    }
}

#[test]
fn catalog_discovery_reads_an_active_day_in_few_concurrent_rounds() {
    let fake = Fake::new(BASE);
    fake.heap_mib.set(Some(40));
    let (query, want_files, want_summaries) = publish_active_day(&fake);
    let walk = || walk_files(&fake, &query);
    let (files, summaries, pages) = walk();
    assert_eq!(files, want_files);
    assert_eq!(summaries, want_summaries);
    {
        let rounds = fake.fos_rounds.borrow();
        let reads: usize = rounds.iter().map(Vec::len).sum();
        // Each hour reads its head block; one at a time that is one round trip per hour.
        assert!(reads >= 13, "{reads} reads");
        assert!(
            rounds.len() <= 2 * pages + 1,
            "{} rounds for {reads} reads",
            rounds.len()
        );
    }

    // Near the heap admission, speculative reads shrink to the headroom left below 96 MiB:
    // at most 1 MiB, two 512 KiB manifests at their full limit, beside the required read.
    fake.heap_mib.set(Some(95));
    let (files, summaries, _) = walk();
    assert_eq!(
        (files, summaries),
        (want_files.clone(), want_summaries.clone())
    );
    assert!(fake
        .fos_rounds
        .borrow()
        .iter()
        .all(|round| round.len() <= 3));
    assert!(fake.fos_rounds.borrow().iter().any(|round| round.len() > 1));
    fake.heap_mib.set(None);
    let (files, summaries, _) = walk();
    assert_eq!(
        (files, summaries),
        (want_files.clone(), want_summaries.clone())
    );
    {
        let rounds = fake.fos_rounds.borrow();
        assert!(!rounds.is_empty());
        assert!(rounds.iter().all(|round| round.len() == 1), "{rounds:?}");
    }

    // A runtime that cannot send concurrently reads one object at a time, never speculatively.
    fake.heap_mib.set(Some(40));
    fake.fos_concurrent.set(false);
    let (files, summaries, _) = walk();
    assert_eq!((files, summaries), (want_files, want_summaries));
    assert!(fake.fos_rounds.borrow().is_empty());
}

#[test]
fn catalog_prefetch_bounds_unread_reads_by_their_full_limits_across_batches() {
    let fake = Fake::new(BASE);
    let (query, want_files, want_summaries) = publish_active_day(&fake);
    fake.fos_defer.set(true);
    // Unread speculative reads stay outstanding across batches; at 95 MiB their declared
    // limits together stay within the 1 MiB left below the 96 MiB admission.
    for (heap, bound) in [(Some(95), 1 << 20), (Some(64), 32 << 20)] {
        fake.heap_mib.set(heap);
        fake.fos_unread_peak.set(0);
        let (files, summaries, _) = walk_files(&fake, &query);
        assert_eq!(
            (files, summaries),
            (want_files.clone(), want_summaries.clone())
        );
        let peak = fake.fos_unread_peak.get();
        assert!(peak > 0 && peak <= bound, "{peak} outstanding bytes");
        assert!(fake.fos_unread.borrow().is_empty());
    }
    fake.heap_mib.set(None);
    fake.fos_unread_peak.set(0);
    let (files, summaries, _) = walk_files(&fake, &query);
    assert_eq!((files, summaries), (want_files, want_summaries));
    assert_eq!(fake.fos_unread_peak.get(), 0);
}

#[test]
fn catalog_prefetch_accounts_for_allocated_held_response_capacity() {
    let fake = Fake::new(BASE);
    fake.heap_mib.set(Some(95));
    let svc = service();
    let store = ingest::storage::Store {
        runtime: &fake,
        service: &svc,
        access_key: String::new(),
        secret_key: String::new(),
        started: Instant::now(),
        vcpu_start: 0,
    };
    let mut prefetched = ingest::catalog::Prefetched::new();
    let mut held = Vec::with_capacity(1 << 20);
    held.push(0);
    prefetched.insert("held".into(), Ok(Some(held)));

    assert_eq!(
        ingest::catalog::fetch_batched(
            &store,
            &mut prefetched,
            "required",
            512 << 10,
            16,
            || vec![("next-a".into(), 512 << 10), ("next-b".into(), 512 << 10)],
        ),
        Ok(None)
    );
    assert_eq!(
        fake.fos_rounds.borrow().as_slice(),
        &[vec!["required".to_owned()]]
    );
}

#[test]
fn catalog_pins_generations_and_replaces_exact_sources_only_after_cas() {
    let fake = Fake::new(BASE);
    let hour = BASE - BASE.rem_euclid(3600);
    let keys = (0..1005)
        .map(|i| {
            let key = ingest::processed_key(
                "processed/request/",
                hour,
                hour + 60,
                &format!("source{i:04}"),
                1,
            );
            fake.bucket.borrow_mut().insert(key.clone(), vec![1, 2, 3]);
            key
        })
        .collect::<Vec<_>>();
    publish_files(&fake, &keys, &[], &[]);
    let query = format!("from={}&to={}", iso_url(hour), iso_url(hour + 3600));
    let first = files_page(&fake, &query).unwrap();
    assert_eq!(first["files"].as_array().unwrap().len(), 500);
    let cursor = first["next_cursor"].as_str().unwrap().to_owned();
    let replacement = telemetry::compact::compact_key(
        &format!("processed/request/{}", ingest::hour_dir(hour)),
        hour as u64,
        (hour + 60) as u64,
        &keys[..32],
        32,
    );
    fake.bucket
        .borrow_mut()
        .insert(replacement.clone(), vec![4, 5, 6]);
    fake.new_request();
    let record = fake.admin_get(ingest::WATERMARK_KEY).unwrap().unwrap();
    let output = ingest::worker::Output {
        key: replacement.clone(),
        rows: 32,
        bytes: 3,
        ..Default::default()
    };
    let candidate = ingest::catalog::prepare(
        &store(&fake, &service()),
        &record.value,
        "compact",
        "schema",
        &[output],
        &[],
        &[],
        &keys[..32],
    )
    .unwrap();
    assert_eq!(
        catalog_keys(&fake, &query).1,
        keys.iter().cloned().collect()
    );
    fake.new_request();
    assert!(fake
        .admin_cas(ingest::WATERMARK_KEY, Some(&record.generation), &candidate)
        .unwrap());
    let current = catalog_keys(&fake, &query);
    assert_eq!(current.0, 2);
    assert_eq!(current.1.len(), 974);
    assert!(current.1.contains(&replacement));
    assert!(keys[..32].iter().all(|key| !current.1.contains(key)));
    let mut pinned = first["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|file| file["key"].as_str().unwrap().to_owned())
        .collect::<BTreeSet<_>>();
    let mut cursor = Some(cursor);
    while let Some(value) = cursor {
        let page = files_page(&fake, &format!("{query}&cursor={value}")).unwrap();
        assert_eq!(page["catalog_generation"], 1);
        for file in page["files"].as_array().unwrap() {
            assert!(pinned.insert(file["key"].as_str().unwrap().to_owned()));
        }
        cursor = page["next_cursor"].as_str().map(str::to_owned);
    }
    assert_eq!(pinned, keys.into_iter().collect());
}

#[test]
fn catalog_retention_retires_expired_hours_before_deleting_their_objects() {
    let fake = Fake::new(BASE + 86_400 + 7 * 3600);
    let key = ingest::processed_key("processed/request/", BASE, BASE + 60, "retained", 1);
    let active = ingest::processed_key(
        "processed/request/",
        BASE + 8 * 3600,
        BASE + 8 * 3600 + 60,
        "active",
        2,
    );
    fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
    fake.bucket.borrow_mut().insert(active.clone(), vec![2]);
    publish_files(&fake, &[key.clone(), active.clone()], &[], &[]);
    let mut svc = service();
    svc.log_retention_days = 1;
    for _ in 0..10 {
        fake.new_request();
        let report =
            ingest::retention::retention_sweep(&store(&fake, &svc), &Default::default(), 4, None)
                .unwrap();
        if !report.more_pending {
            break;
        }
    }
    assert!(fake.bucket.borrow().contains_key(&key));
    let query = format!("from={}&to={}", iso_url(BASE), iso_url(BASE + 3600));
    assert!(!catalog_keys(&fake, &query).1.contains(&key));
    let query = format!("from={}&to={}", iso_url(BASE), iso_url(BASE + 24 * 3600));
    assert_eq!(
        catalog_keys(&fake, &query).1,
        BTreeSet::from([active.clone()])
    );
    fake.new_request();
    assert_eq!(
        api::extents(&fake, &svc, Instant::now()).unwrap()["total_rows"],
        2
    );
    fake.now
        .set(fake.now.get() + ingest::compaction::DELETE_GRACE_S + 1);
    for _ in 0..10 {
        fake.new_request();
        let report =
            ingest::retention::retention_sweep(&store(&fake, &svc), &Default::default(), 4, None)
                .unwrap();
        if !report.more_pending {
            break;
        }
    }
    assert!(!fake.bucket.borrow().contains_key(&key));
    assert!(fake.bucket.borrow().contains_key(&active));
}

#[test]
fn catalog_delta_requires_proven_base_ancestry_and_pins_empty_continuation_pages() {
    let fake = Fake::new(BASE + 3600);
    let keys = (0..3)
        .map(|n| {
            ingest::processed_key(
                "processed/request/",
                BASE,
                BASE + 60,
                &format!("delta{n}"),
                1,
            )
        })
        .collect::<Vec<_>>();
    for key in &keys {
        fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
    }
    publish_files(&fake, &keys[..1], &[], &[]);
    let base_manifest = fake.kv.borrow()[ingest::WATERMARK_KEY].1["catalog"]["manifest"]
        .as_str()
        .unwrap()
        .to_owned();
    publish_files(&fake, &keys[1..2], &[], &[]);
    let query = format!(
        "from={}&to={}&since_generation=1&previous_from={}&previous_to={}",
        iso_url(BASE),
        iso_url(BASE + 3600),
        iso_url(BASE),
        iso_url(BASE + 3600)
    );
    fake.bucket.borrow_mut().remove(&base_manifest);
    assert_eq!(
        files_page(&fake, &query).unwrap_err(),
        (409, "catalog_resync_required")
    );
}

#[test]
fn catalog_delta_reads_recent_commit_manifests_in_one_round() {
    let fake = Fake::new(BASE + 3600);
    fake.heap_mib.set(Some(40));
    let keys = (0..7)
        .map(|n| {
            let start = BASE + n * 120;
            ingest::processed_key("processed/request/", start, start + 120, "delta", 1)
        })
        .collect::<Vec<_>>();
    for key in &keys {
        fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
        publish_files(&fake, std::slice::from_ref(key), &[], &[]);
    }
    let query = format!(
        "from={}&to={}&since_generation=1&previous_from={}&previous_to={}",
        iso_url(BASE),
        iso_url(BASE + 3600),
        iso_url(BASE),
        iso_url(BASE + 3600)
    );
    fake.fos_rounds.borrow_mut().clear();
    let page = files_page(&fake, &query).unwrap();
    let changed: Vec<&str> = page["changes"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|change| change["files"].as_array().unwrap())
        .map(|file| file["key"].as_str().unwrap())
        .collect();
    assert_eq!(
        changed,
        keys[1..].iter().map(String::as_str).collect::<Vec<_>>()
    );
    let rounds = fake.fos_rounds.borrow();
    assert_eq!(rounds.len(), 1, "{rounds:?}");
    assert_eq!(rounds[0].len(), 7);
}

#[test]
fn catalog_delta_keeps_a_prefetched_manifest_answer_instead_of_reading_it_twice() {
    let fake = Fake::new(BASE + 3600);
    fake.heap_mib.set(Some(40));
    let keys = (0..4)
        .map(|n| {
            let start = BASE + n * 120;
            ingest::processed_key("processed/request/", start, start + 120, "delta", 1)
        })
        .collect::<Vec<_>>();
    let mut manifests = Vec::new();
    for key in &keys {
        fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
        publish_files(&fake, std::slice::from_ref(key), &[], &[]);
        manifests.push(
            fake.kv.borrow()[ingest::WATERMARK_KEY].1["catalog"]["manifest"]
                .as_str()
                .unwrap()
                .to_owned(),
        );
    }
    // The second-newest commit's manifest is gone: its prefetched 404 must decide the page.
    fake.bucket.borrow_mut().remove(&manifests[2]);
    let query = format!(
        "from={}&to={}&since_generation=1&previous_from={}&previous_to={}",
        iso_url(BASE),
        iso_url(BASE + 3600),
        iso_url(BASE),
        iso_url(BASE + 3600)
    );
    fake.fos_rounds.borrow_mut().clear();
    fake.fos_gets.borrow_mut().clear();
    assert_eq!(
        files_page(&fake, &query).unwrap_err(),
        (409, "catalog_resync_required")
    );
    let rounds = fake.fos_rounds.borrow();
    assert_eq!(rounds.len(), 1, "{rounds:?}");
    let gets = fake.fos_gets.borrow();
    assert_eq!(gets.iter().filter(|key| **key == manifests[2]).count(), 1);
    assert_eq!(gets.len(), rounds[0].len(), "{gets:?}");
}

#[test]
fn catalog_delta_pins_a_target_across_empty_pages_and_reports_expired_cursors() {
    let fake = Fake::new(BASE + 3600);
    let keys = (0..3)
        .map(|n| {
            ingest::processed_key(
                "processed/request/",
                BASE,
                BASE + 60,
                &format!("delta{n}"),
                1,
            )
        })
        .collect::<Vec<_>>();
    for key in &keys {
        fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
    }
    publish_files(&fake, &keys[..1], &[], &[]);
    publish_files(&fake, &keys[1..2], &[], &[]);
    let query = format!(
        "from={}&to={}&since_generation=1&previous_from={}&previous_to={}",
        iso_url(BASE),
        iso_url(BASE + 3600),
        iso_url(BASE),
        iso_url(BASE + 3600)
    );
    fake.new_request();
    fake.calls.set(BUDGET - 6);
    let first = api::files(&fake, &service(), &query, Instant::now()).unwrap();
    assert_eq!(first["complete"], false);
    assert!(fake.calls.get() <= BUDGET);
    let cursor = first["next_cursor"].as_str().unwrap();
    publish_files(&fake, &keys[2..], &[], &[]);
    let next = files_page(&fake, &format!("{query}&cursor={cursor}")).unwrap();
    assert_eq!(next["base_generation"], 1);
    assert_eq!(next["catalog_generation"], 2);
    assert_eq!(next["complete"], true);
    assert_eq!(next["changes"][0]["files"][0]["key"], keys[1]);
    fake.now.set(fake.now.get() + 601);
    assert_eq!(
        files_page(&fake, &format!("{query}&cursor={cursor}")).unwrap_err(),
        (409, "catalog_resync_required")
    );
}

#[test]
fn catalog_delta_treats_a_temporarily_missing_head_as_retryable_not_a_resync() {
    let fake = Fake::new(BASE);
    let query = format!(
        "from={}&to={}&since_generation=1&previous_from={}&previous_to={}",
        iso_url(BASE),
        iso_url(BASE + 3600),
        iso_url(BASE),
        iso_url(BASE + 3600)
    );
    assert_eq!(
        files_page(&fake, &query).unwrap_err(),
        (503, "catalog_head_behind")
    );
}

#[test]
fn catalog_delta_returns_only_new_raw_changes_and_authoritative_summary_partitions() {
    let fake = Fake::new(BASE + 3600);
    let first = ingest::processed_key("processed/request/", BASE, BASE + 60, "first", 1);
    let second = ingest::processed_key("processed/request/", BASE + 60, BASE + 120, "second", 1);
    let summary_first =
        ingest::summary::summary_key("processed/summary/request/", BASE, BASE + 60, "first", 1);
    let summary_second = ingest::summary::summary_key(
        "processed/summary/request/",
        BASE + 60,
        BASE + 120,
        "second",
        1,
    );
    for key in [&first, &second, &summary_first, &summary_second] {
        fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
    }
    publish_files(&fake, &[first], &[summary_first], &[]);
    publish_files(&fake, std::slice::from_ref(&second), &[summary_second], &[]);
    let range = format!("from={}&to={}", iso_url(BASE), iso_url(BASE + 3600));
    let page = files_page(
        &fake,
        &format!(
            "{range}&summaries=1&since_generation=1&previous_from={}&previous_to={}",
            iso_url(BASE),
            iso_url(BASE + 3600)
        ),
    )
    .unwrap();
    assert_eq!(page["base_generation"], 1);
    assert_eq!(page["catalog_generation"], 2);
    assert_eq!(page["complete"], true);
    assert_eq!(page["files"].as_array().unwrap().len(), 0);
    let changes = page["changes"].as_array().unwrap();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0]["generation"], 2);
    assert_eq!(changes[0]["files"][0]["key"], second);
    assert_eq!(page["summary_partitions"].as_array().unwrap().len(), 1);
    assert_eq!(page["summary_files"].as_array().unwrap().len(), 2);
}

#[test]
fn catalog_pending_compaction_does_not_lease_block_ingestion_publication() {
    let fake = Fake::new(BASE);
    let hour = BASE - BASE.rem_euclid(3600);
    let sources = (0..2)
        .map(|n| {
            let key =
                ingest::processed_key("processed/request/", hour, hour + 60, &format!("old{n}"), 1);
            fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
            key
        })
        .collect::<Vec<_>>();
    publish_files(&fake, &sources, &[], &[]);
    let replacement = ingest::worker::Output {
        key: telemetry::compact::compact_key(
            &format!("processed/request/{}", ingest::hour_dir(hour)),
            hour as u64,
            (hour + 60) as u64,
            &sources,
            2,
        ),
        rows: 2,
        bytes: 1,
        ..Default::default()
    };
    fake.bucket
        .borrow_mut()
        .insert(replacement.key.clone(), vec![2]);
    fake.new_request();
    let previous = fake.admin_get(ingest::WATERMARK_KEY).unwrap().unwrap();
    assert_eq!(
        ingest::catalog::prepare_reserving(
            &store(&fake, &service()),
            &previous.value,
            "compaction",
            "",
            std::slice::from_ref(&replacement),
            &[],
            &[],
            &sources,
            26,
        )
        .unwrap_err(),
        "catalog_prepare_pending"
    );
    assert!(fake.calls.get() <= 32);
    let arriving = ingest::worker::Output {
        key: ingest::processed_key("processed/request/", hour, hour + 60, "arriving", 1),
        rows: 1,
        bytes: 1,
        ..Default::default()
    };
    fake.bucket
        .borrow_mut()
        .insert(arriving.key.clone(), vec![3]);
    let committed_at = fake.now.get();
    let mut published = false;
    for _ in 0..3 {
        fake.new_request();
        match ingest::catalog::prepare(
            &store(&fake, &service()),
            &previous.value,
            "ingestion",
            "schema",
            std::slice::from_ref(&arriving),
            &[],
            &[],
            &[],
        ) {
            Ok(value) => {
                assert!(fake
                    .admin_cas(ingest::WATERMARK_KEY, Some(&previous.generation), &value)
                    .unwrap());
                published = true;
                break;
            }
            Err(error) if error == "catalog_prepare_pending" => {}
            Err(error) => panic!("{error}"),
        }
        assert!(fake.calls.get() <= 32);
    }
    assert!(
        published,
        "ingestion waited for an unrelated compaction lease"
    );
    assert_eq!(fake.now.get(), committed_at);
    publish_outputs(&fake, std::slice::from_ref(&replacement), &[], &sources);
    let query = format!("from={}&to={}", iso_url(hour), iso_url(hour + 3600));
    let visible = catalog_keys(&fake, &query);
    assert_eq!(visible.0, 3);
    assert_eq!(visible.1, BTreeSet::from([replacement.key, arriving.key]));
}

#[test]
fn catalog_missing_immutable_root_is_a_retryable_error_not_an_empty_dataset() {
    let fake = Fake::new(BASE);
    let key = ingest::processed_key("processed/request/", BASE, BASE + 60, "source", 1);
    fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
    let value = publish_files(&fake, std::slice::from_ref(&key), &[], &[]);
    let root = value["catalog"]["days"]
        .as_object()
        .unwrap()
        .values()
        .next()
        .unwrap()
        .as_str()
        .unwrap()
        .to_owned();
    let bytes = fake.bucket.borrow_mut().remove(&root).unwrap();
    let query = format!("from={}&to={}", iso_url(BASE), iso_url(BASE + 60));
    assert_eq!(files_page(&fake, &query).unwrap_err().0, 503);
    fake.bucket.borrow_mut().insert(root, bytes);
    assert_eq!(catalog_keys(&fake, &query).1, BTreeSet::from([key]));
}

#[test]
fn catalog_discovery_excludes_uploaded_but_uncommitted_managed_files() {
    let fake = Fake::new(BASE);
    let key = ingest::processed_key(
        "processed/request/",
        BASE,
        BASE + 60,
        "catalog1_uncommitted",
        7,
    );
    fake.bucket.borrow_mut().insert(key, vec![1, 2, 3]);
    let query = format!("from={}&to={}", iso_url(BASE), iso_url(BASE + 60));
    let mut query = query.clone();
    for _ in 0..100 {
        let page = files_page(&fake, &query).unwrap();
        assert!(
            page["files"].as_array().unwrap().is_empty(),
            "an uploaded output is not published until the catalog CAS"
        );
        if page["complete"] == true {
            return;
        }
        query = format!(
            "from={}&to={}&cursor={}",
            iso_url(BASE),
            iso_url(BASE + 60),
            page["next_cursor"].as_str().unwrap()
        );
    }
    panic!("bounded backfill must eventually complete");
}

#[test]
fn files_paginates_and_completes_only_on_the_last_page() {
    let fake = Fake::new(BASE);
    seed_files(&fake, 1200);
    let base_query = format!("from={}&to={}", iso_url(BASE), iso_url(BASE + 1200 * 60));
    let (mut cursor, mut seen, mut pages) = (None::<String>, BTreeSet::new(), 0);
    loop {
        let query = match &cursor {
            Some(c) => format!("{base_query}&cursor={c}"),
            None => base_query.clone(),
        };
        let page = files_page(&fake, &query).unwrap();
        let files = page["files"].as_array().unwrap();
        assert!(files.len() <= api::FILES_PAGE);
        for f in files {
            assert!(seen.insert(f["key"].as_str().unwrap().to_owned()));
        }
        pages += 1;
        if page["complete"] == true {
            assert!(page["next_cursor"].is_null());
            break;
        }
        cursor = Some(page["next_cursor"].as_str().unwrap().to_owned());
        assert!(pages < 10);
    }
    assert_eq!(seen.len(), 1200);
    assert!(pages >= 3);
}

fn iso_url(seconds: i64) -> String {
    ingest::rfc3339(seconds).replace(':', "%3A")
}

#[test]
fn files_filters_by_overlap_and_shapes_entries() {
    let fake = Fake::new(BASE);
    seed_files(&fake, 10);
    let query = format!("from={}&to={}", iso_url(BASE + 120), iso_url(BASE + 240));
    let page = files_page(&fake, &query).unwrap();
    let files = page["files"].as_array().unwrap();
    assert_eq!(files.len(), 2);
    assert_eq!(files[0]["start"], ingest::rfc3339(BASE + 120));
    assert_eq!(files[0]["end"], ingest::rfc3339(BASE + 180));
    assert_eq!(
        (files[0]["size"].as_u64(), files[0]["rows"].as_u64()),
        (Some(3), Some(3))
    );
    assert_eq!(page["complete"], true);
}

thread_local! {
    /// LIST requests the fake bucket answered on this test's thread.
    static LISTS: Cell<u32> = const { Cell::new(0) };
}

fn lists() -> u32 {
    LISTS.with(Cell::get)
}

/// A request `compact=` key in `start`'s hour folder covering `sources`.
fn request_compact_key(sources: &[String]) -> String {
    let names: Vec<_> = sources
        .iter()
        .map(|k| ingest::parse_processed(k).unwrap())
        .collect();
    let a = names.iter().map(|n| n.start).min().unwrap();
    let b = names.iter().map(|n| n.end).max().unwrap();
    let rows = names.iter().map(|n| n.rows.unwrap()).sum();
    let prefix = format!("processed/request/{}", ingest::hour_dir(a));
    fastly_log_analytics::telemetry::compact::compact_key(
        &prefix, a as u64, b as u64, sources, rows,
    )
}

fn request_windows(fake: &Fake) -> Vec<String> {
    fake.keys("processed/request/")
        .into_iter()
        .filter(|k| k.contains("/window="))
        .collect()
}

#[test]
fn files_returns_a_compact_in_place_of_its_sources_and_keeps_uncovered_windows() {
    let fake = Fake::new(BASE + 10 * 86_400);
    seed_files(&fake, 4);
    let windows = request_windows(&fake);
    let compact = request_compact_key(&windows[..3]);
    fake.bucket.borrow_mut().insert(compact.clone(), vec![9; 7]);
    publish_files(&fake, std::slice::from_ref(&compact), &[], &windows[..3]);
    let query = format!("from={}&to={}", iso_url(BASE), iso_url(BASE + 3600));
    let page = files_page(&fake, &query).unwrap();
    let files = page["files"].as_array().unwrap();
    let keys: Vec<&str> = files.iter().map(|f| f["key"].as_str().unwrap()).collect();
    assert_eq!(keys, vec![compact.as_str(), windows[3].as_str()]);
    assert_eq!(files[0]["start"], ingest::rfc3339(BASE));
    assert_eq!(files[0]["end"], ingest::rfc3339(BASE + 180));
    assert_eq!(files[0]["rows"], 6);
    assert_eq!(files[0]["sources"], 3);
    assert_eq!(files[0]["size"], 7);
    // The same answer from the cached closed hour.
    assert_eq!(files_page(&fake, &query).unwrap()["files"], page["files"]);
}

#[test]
fn files_hides_sources_on_a_later_page_of_an_hour_than_their_compact() {
    let fake = Fake::new(BASE + 10 * 86_400);
    for i in 0..(api::FILES_PAGE as i64 + 20) {
        let start = BASE + i * 6;
        let key = ingest::processed_key("processed/request/", start, start + 6, "abcdef012345", 1);
        fake.bucket.borrow_mut().insert(key, vec![1]);
    }
    let windows = request_windows(&fake);
    publish_files(&fake, &windows, &[], &[]);
    let late = &windows[windows.len() - 3..];
    let compact = request_compact_key(late);
    fake.bucket.borrow_mut().insert(compact.clone(), vec![1]);
    publish_files(&fake, std::slice::from_ref(&compact), &[], late);
    let query = format!("from={}&to={}", iso_url(BASE), iso_url(BASE + 3600));
    let (mut cursor, mut seen) = (None::<String>, Vec::new());
    for _ in 0..5 {
        let q = match &cursor {
            Some(c) => format!("{query}&cursor={c}"),
            None => query.clone(),
        };
        let page = files_page(&fake, &q).unwrap();
        seen.extend(
            page["files"]
                .as_array()
                .unwrap()
                .iter()
                .map(|f| f["key"].as_str().unwrap().to_owned()),
        );
        if page["complete"] == true {
            break;
        }
        cursor = Some(page["next_cursor"].as_str().unwrap().to_owned());
    }
    assert!(seen.contains(&compact));
    for source in late {
        assert!(!seen.contains(source), "{source} is covered");
    }
    assert_eq!(seen.len(), windows.len() - 3 + 1);
}

#[test]
fn files_stops_listing_after_the_last_hour_of_the_range() {
    let fake = Fake::new(BASE);
    seed_files(&fake, 960);
    let query = format!("from={}&to={}", iso_url(BASE + 3600), iso_url(BASE + 7200));
    let before = lists();
    let page = files_page(&fake, &query).unwrap();
    assert_eq!(
        lists() - before,
        0,
        "catalog discovery never lists uploaded objects"
    );
    assert_eq!(page["complete"], true);
    assert_eq!(page["files"].as_array().unwrap().len(), 60);
}

#[test]
fn closed_hours_are_listed_once_and_a_late_commit_reveals_its_window() {
    let fake = Fake::new(BASE + 3 * 3600);
    put_raw(&fake, BASE + 60, "b", &[line(BASE + 60, "b")]);
    assert_eq!(tick(&fake).unwrap().converted, 1);
    let query = format!("from={}&to={}", iso_url(BASE), iso_url(BASE + 2 * 3600));
    let keys = |page: &Value| -> Vec<String> {
        page["files"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["key"].as_str().unwrap().to_owned())
            .collect()
    };
    let first = files_page(&fake, &query).unwrap();
    assert_eq!(keys(&first).len(), 1);
    assert_eq!(
        first["catalog_generation"], 1,
        "the catalog atomically committed the first window"
    );
    let before = lists();
    let again = files_page(&fake, &query).unwrap();
    assert_eq!(
        lists() - before,
        0,
        "both closed hours come from the immutable catalog"
    );
    assert_eq!(keys(&again), keys(&first));

    // A late upload for the closed hour commits a new window and a new generation.
    put_raw(&fake, BASE + 120, "late", &[line(BASE + 120, "late")]);
    assert_eq!(tick(&fake).unwrap().converted, 1);
    assert_eq!(fake.kv.borrow()[ingest::WATERMARK_KEY].1["manifest_gen"], 2);
    let after = files_page(&fake, &query).unwrap();
    assert_eq!(keys(&after).len(), 2);
    assert_eq!(after["catalog_generation"], 2);

    // A commit into an hour still open to caches keeps the generation.
    put_raw(&fake, BASE + 7260, "open", &[line(BASE + 7260, "open")]);
    assert_eq!(tick(&fake).unwrap().converted, 1);
    assert_eq!(fake.kv.borrow()[ingest::WATERMARK_KEY].1["manifest_gen"], 2);
}

#[test]
fn open_hours_and_active_plans_do_not_expose_uncommitted_catalog_outputs() {
    let fake = Fake::new(BASE + 1800);
    seed_files(&fake, 30);
    let query = format!("from={}&to={}", iso_url(BASE), iso_url(BASE + 1800));
    files_page(&fake, &query).unwrap();
    let before = lists();
    assert_eq!(
        files_page(&fake, &query).unwrap()["files"]
            .as_array()
            .unwrap()
            .len(),
        30
    );
    assert_eq!(
        lists() - before,
        0,
        "open hours also use committed catalog metadata"
    );

    let fake = Fake::new(BASE + 10 * 86_400);
    seed_files(&fake, 120);
    fake.kv
        .borrow_mut()
        .insert(ingest::PLAN_KEY.into(), (1, json!({"plan_id": "r2"})));
    let query = format!("from={}&to={}", iso_url(BASE), iso_url(BASE + 7200));
    let page = files_page(&fake, &query).unwrap();
    assert_eq!(page["catalog_generation"], 1);
    let before = lists();
    files_page(&fake, &query).unwrap();
    assert_eq!(lists() - before, 0);

    fake.kv.borrow_mut().remove(ingest::PLAN_KEY);
    files_page(&fake, &query).unwrap();
    let before = lists();
    assert_eq!(
        files_page(&fake, &query).unwrap()["files"]
            .as_array()
            .unwrap()
            .len(),
        120
    );
    assert_eq!(lists() - before, 0);
}

#[test]
fn a_cached_listing_stays_within_the_cache_op_budget_and_pages_exactly() {
    let fake = Fake::new(BASE + 10 * 86_400);
    seed_files(&fake, 1200);
    let base = format!("from={}&to={}", iso_url(BASE), iso_url(BASE + 1200 * 60));
    let collect = |fake: &Fake, spare_ops: u32| -> (Vec<String>, u32) {
        let (mut cursor, mut keys, mut list_calls) = (None::<String>, Vec::new(), 0);
        for _ in 0..20 {
            let query = match &cursor {
                Some(c) => format!("{base}&cursor={c}"),
                None => base.clone(),
            };
            fake.new_request();
            fake.cache_ops
                .set(fastly_log_analytics::shell::CACHE_OP_BUDGET - spare_ops);
            let before = lists();
            let page = api::files(fake, &service(), &query, Instant::now()).unwrap();
            list_calls += lists() - before;
            assert!(fake.cache_ops.get() <= fastly_log_analytics::shell::CACHE_OP_BUDGET);
            assert!(page["files"].as_array().unwrap().len() <= api::FILES_PAGE);
            keys.extend(
                page["files"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|f| f["key"].as_str().unwrap().to_owned()),
            );
            if page["complete"] == true {
                return (keys, list_calls);
            }
            cursor = Some(page["next_cursor"].as_str().unwrap().to_owned());
        }
        panic!("listing did not complete");
    };
    let want: Vec<String> = fake.keys("processed/request/");
    let (cold, _) = collect(&fake, 0);
    assert_eq!(cold, want, "no cache ops left: plain listing");
    let (warm_up, _) = collect(&fake, 64);
    assert_eq!(warm_up, want);
    let (warm, list_calls) = collect(&fake, 64);
    assert_eq!(warm, want);
    assert_eq!(list_calls, 0);
}

#[test]
fn files_accepts_browser_iso_timestamps_with_milliseconds() {
    let fake = Fake::new(BASE);
    seed_files(&fake, 10);
    let from = ingest::rfc3339(BASE)
        .replace('Z', ".123Z")
        .replace(':', "%3A");
    let to = ingest::rfc3339(BASE + 600)
        .replace('Z', ".456Z")
        .replace(':', "%3A");
    let page = files_page(&fake, &format!("from={from}&to={to}")).unwrap();
    assert_eq!(page["files"].as_array().unwrap().len(), 10);
}

#[test]
fn files_rejects_forged_or_mismatched_cursors_and_bad_ranges() {
    let fake = Fake::new(BASE);
    seed_files(&fake, 600);
    let q = format!("from={}&to={}", iso_url(BASE), iso_url(BASE + 36_000));
    let page = files_page(&fake, &q).unwrap();
    let cursor = page["next_cursor"].as_str().unwrap().to_owned();
    let other = format!(
        "from={}&to={}&cursor={cursor}",
        iso_url(BASE),
        iso_url(BASE + 7200)
    );
    assert_eq!(files_page(&fake, &other).unwrap_err().0, 400);
    let forged = format!("{q}&cursor=x{cursor}");
    assert_eq!(files_page(&fake, &forged).unwrap_err().0, 400);
    let backwards = format!("from={}&to={}", iso_url(BASE + 60), iso_url(BASE));
    assert_eq!(files_page(&fake, &backwards).unwrap_err().0, 400);
}

#[test]
fn file_serves_only_processed_keys_of_this_service() {
    let fake = Fake::new(BASE);
    seed_files(&fake, 1);
    let key = fake.keys("processed/request/")[0].clone();
    let get = |k: &str| {
        fake.new_request();
        api::file(
            &fake,
            &service(),
            &format!(
                "key={}",
                url::form_urlencoded::byte_serialize(k.as_bytes()).collect::<String>()
            ),
            None,
        )
        .map(|upstream| fastly_log_analytics::shell::fetch_upstream(&fake, &upstream))
    };
    assert_eq!(get(&key).unwrap().body, vec![1, 2, 3]);
    for bad in [
        "raw/request/year=2027/month=01/day=15/hour=08/minute=00/a.gz",
        "processed/request/../raw/request/a.parquet",
        "processed/requestx/a.parquet",
        "other/processed/request/a.parquet",
        "processed/request/year=2027/a.gz",
        "processed/request//a.parquet",
    ] {
        assert_eq!(get(bad).err().unwrap().0, 400, "{bad}");
    }
    assert_eq!(
        get("processed/request/year=2027/missing.parquet")
            .unwrap()
            .status,
        404
    );
}

#[test]
fn file_streams_past_the_8_mib_cap_and_passes_a_range_through() {
    let fake = Fake::new(BASE);
    let key = "processed/request/year=2027/big.parquet";
    let body: Vec<u8> = (0..9 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();
    fake.bucket.borrow_mut().insert(key.into(), body.clone());
    let get = |range: Option<&str>| {
        fake.new_request();
        fake.cdn_gets.borrow_mut().clear();
        let upstream = api::file(&fake, &service(), &format!("key={key}"), range).unwrap();
        assert_eq!(upstream.key, key);
        assert_eq!(upstream.range.as_deref(), range);
        assert!(
            fake.cdn_gets.borrow().is_empty(),
            "the body is fetched when the response is sent"
        );
        fastly_log_analytics::shell::fetch_upstream(&fake, &upstream)
    };
    let full = get(None);
    assert_eq!(full.status, 200);
    assert!(full.body == body);
    assert_eq!(full.headers["accept-ranges"], "bytes");
    let part = get(Some("bytes=10-19"));
    assert_eq!(part.status, 206);
    assert_eq!(part.body, body[10..20].to_vec());
    assert_eq!(
        part.headers["content-range"],
        format!("bytes 10-19/{}", body.len())
    );
    let tail = get(Some("bytes=-4"));
    assert_eq!(tail.body, body[body.len() - 4..].to_vec());
    fake.new_request();
    let mut upstream =
        api::file(&fake, &service(), &format!("key={key}"), Some("bytes=0-")).unwrap();
    upstream.head_only = true;
    let head = fastly_log_analytics::shell::fetch_upstream(&fake, &upstream);
    assert_eq!(head.status, 206);
    assert!(head.body.is_empty());
    assert_eq!(head.headers["content-length"], body.len().to_string());
    assert_eq!(
        head.headers["content-range"],
        format!("bytes 0-{}/{}", body.len() - 1, body.len())
    );
    let beyond = get(Some(&format!("bytes={}-", body.len())));
    assert_eq!(beyond.status, 416);
    assert_eq!(
        beyond.headers["content-range"],
        format!("bytes */{}", body.len())
    );
}

#[test]
fn log_extents_reports_first_and_last_windows_or_nulls() {
    let fake = Fake::new(BASE);
    let extents = |fake: &Fake| {
        fake.new_request();
        api::extents(fake, &service(), Instant::now()).unwrap()
    };
    let empty = extents(&fake);
    assert!(empty["earliest"].is_null() && empty["latest"].is_null());
    assert!(empty["total_rows"].is_null());
    seed_files(&fake, 1);
    for (start, run) in [(BASE + 86_400 * 3 + 7200, "a"), (BASE - 86_400 * 40, "b")] {
        let key = ingest::processed_key("processed/request/", start, start + 60, run, 1);
        fake.bucket.borrow_mut().insert(key.clone(), vec![0]);
        publish_files(&fake, &[key], &[], &[]);
    }
    let found = extents(&fake);
    assert_eq!(found["earliest"], ingest::rfc3339(BASE - 86_400 * 40));
    assert_eq!(found["latest"], ingest::rfc3339(BASE + 86_400 * 3 + 7260));
    assert_eq!(found["total_rows"], 3);
    assert_eq!(found["total_rows_partial"], false);
}

#[test]
fn log_extents_count_a_compact_once_while_its_sources_remain_and_after_they_go() {
    let fake = Fake::new(BASE + 10 * 86_400);
    seed_files(&fake, 4);
    let windows = request_windows(&fake);
    let compact = request_compact_key(&windows[1..]);
    fake.bucket.borrow_mut().insert(compact.clone(), vec![1]);
    publish_files(&fake, &[compact], &[], &windows[1..]);
    let extents = |fake: &Fake| {
        fake.new_request();
        api::extents(fake, &service(), Instant::now()).unwrap()
    };
    let found = extents(&fake);
    assert_eq!(found["total_rows"], 10);
    assert_eq!(found["latest"], ingest::rfc3339(BASE + 240));
    for source in &windows[1..] {
        fake.bucket.borrow_mut().remove(source);
    }
    let found = extents(&fake);
    assert_eq!(found["total_rows"], 10);
    assert_eq!(found["earliest"], ingest::rfc3339(BASE));
    assert_eq!(found["latest"], ingest::rfc3339(BASE + 240));
}

#[test]
fn log_extents_uses_complete_committed_statistics_beyond_the_old_list_cap() {
    let fake = Fake::new(BASE);
    seed_files(&fake, 4001);
    fake.new_request();
    let found = api::extents(&fake, &service(), Instant::now()).unwrap();
    assert_eq!(found["total_rows_partial"], false);
    assert_eq!(found["total_rows"], 4001u64 * 4002 / 2);
    assert_eq!(found["latest"], ingest::rfc3339(BASE + 4001 * 60));
}

fn admin_page(fake: &Fake, raw: Option<bool>, query: &str) -> Result<Value, (u16, &'static str)> {
    fake.new_request();
    match raw {
        Some(raw) => api::tree(fake, &service(), query, Instant::now(), raw),
        None => api::ingested_files(fake, &service(), query, Instant::now()),
    }
}

fn cursor_query(cursor: &str) -> String {
    format!("cursor={cursor}")
}

#[test]
fn trees_list_one_folder_per_request_without_object_gets() {
    let fake = Fake::new(BASE);
    for minute in 0..130 {
        put_raw(&fake, BASE + minute * 60, "a", &[line(BASE, "a")]);
    }
    fake.bucket
        .borrow_mut()
        .insert("raw/request/odd.gz".into(), vec![1, 2]);
    let top = admin_page(&fake, Some(true), "").unwrap();
    assert_eq!(top["root"], "raw/");
    assert_eq!(top["nodes"].as_array().unwrap().len(), 1);
    assert_eq!(top["nodes"][0]["name"], "request");
    assert_eq!(top["nodes"][0]["prefix"], "request/");
    let root = admin_page(&fake, Some(true), "prefix=request%2F").unwrap();
    assert_eq!(root["complete"], true);
    let nodes = root["nodes"].as_array().unwrap();
    assert_eq!(nodes[0]["name"], "year=2027");
    assert_eq!(nodes[0]["type"], "directory");
    assert_eq!(nodes[0]["prefix"], "request/year=2027/");
    assert_eq!(nodes[1]["name"], "odd.gz");
    assert_eq!(nodes[1]["size"], 2);
    assert_eq!(nodes[1]["mtime"], "2027-01-15T08:00:00.000Z");

    let hour = admin_page(
        &fake,
        Some(true),
        "prefix=request%2Fyear%3D2027%2Fmonth%3D01%2Fday%3D15%2F",
    )
    .unwrap();
    let names: Vec<_> = hour["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["hour=08", "hour=09", "hour=10"]);
    let minute = admin_page(
        &fake,
        Some(true),
        "prefix=request%2Fyear%3D2027%2Fmonth%3D01%2Fday%3D15%2Fhour%3D08%2Fminute%3D00%2F",
    )
    .unwrap();
    assert_eq!(minute["nodes"][0]["name"], "a.zst");
    assert_eq!(
        minute["nodes"][0]["key"],
        "raw/request/year=2027/month=01/day=15/hour=08/minute=00/a.zst"
    );
    assert!(fake.cdn_gets.borrow().is_empty());

    seed_files(&fake, 3);
    let storage = admin_page(
        &fake,
        Some(false),
        "prefix=request%2Fyear%3D2027%2Fmonth%3D01%2Fday%3D15%2Fhour%3D08%2F",
    )
    .unwrap();
    assert_eq!(storage["root"], "processed/");
    let top = admin_page(&fake, Some(false), "").unwrap();
    let names: Vec<_> = top["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["request"]);
    assert_eq!(storage["nodes"].as_array().unwrap().len(), 3);
    assert_eq!(storage["nodes"][0]["rows"], 1);
    assert_eq!(storage["nodes"][0]["size"], 3);
}

#[test]
fn tree_pages_are_bounded_and_cursors_bind_endpoint_and_prefix() {
    let fake = Fake::new(BASE);
    for i in 0..1500 {
        fake.bucket.borrow_mut().insert(
            format!("raw/request/year=2027/month=01/day=15/hour=08/minute=00/{i:05}.gz"),
            vec![0],
        );
    }
    let folder =
        "prefix=request%2Fyear%3D2027%2Fmonth%3D01%2Fday%3D15%2Fhour%3D08%2Fminute%3D00%2F";
    let page = admin_page(&fake, Some(true), folder).unwrap();
    assert_eq!(page["nodes"].as_array().unwrap().len(), 1000);
    assert_eq!(page["complete"], false);
    let next = cursor_query(page["next_cursor"].as_str().unwrap());
    let rest = admin_page(&fake, Some(true), &format!("{folder}&{next}")).unwrap();
    assert_eq!(rest["nodes"].as_array().unwrap().len(), 500);
    assert_eq!(rest["complete"], true);
    for bad in [next.clone(), format!("{folder}x%2F&{next}")] {
        assert_eq!(admin_page(&fake, Some(true), &bad).unwrap_err().0, 400);
    }
    assert_eq!(
        admin_page(&fake, Some(false), &format!("{folder}&{next}"))
            .unwrap_err()
            .0,
        400
    );
    for prefix in [
        "prefix=..%2F",
        "prefix=request%2Fyear%3D2027",
        "prefix=%2Fetc%2F",
        "prefix=a%2F%2Fb%2F",
    ] {
        assert_eq!(admin_page(&fake, Some(true), prefix).unwrap_err().0, 400);
    }
}

fn store<'a>(fake: &'a Fake, service: &'a Service) -> ingest::storage::Store<'a, Fake> {
    ingest::storage::Store {
        runtime: fake,
        service,
        access_key: "ak".into(),
        secret_key: "sk".into(),
        started: Instant::now(),
        vcpu_start: 0,
    }
}

#[test]
fn multipart_uploads_assemble_their_parts_or_abort() {
    let fake = Fake::new(BASE);
    let service = service();
    let store = store(&fake, &service);
    let big = vec![7u8; 5 * 1024 * 1024];
    let id = store.multipart_create("a/b.parquet").unwrap();
    let mut parts = vec![store
        .multipart_part("a/b.parquet", &id, 1, big.clone())
        .unwrap()];
    parts.push(
        store
            .multipart_part("a/b.parquet", &id, 2, vec![1, 2])
            .unwrap(),
    );
    store
        .multipart_complete("a/b.parquet", &id, &parts)
        .unwrap();
    let stored = fake.bucket.borrow()["a/b.parquet"].clone();
    assert_eq!(stored.len(), big.len() + 2);
    assert_eq!(&stored[big.len()..], &[1, 2]);
    assert!(fake.uploads.borrow().is_empty());

    let id = store.multipart_create("a/c.parquet").unwrap();
    let small = store
        .multipart_part("a/c.parquet", &id, 1, vec![1])
        .unwrap();
    let last = store
        .multipart_part("a/c.parquet", &id, 2, vec![2])
        .unwrap();
    assert!(store
        .multipart_complete("a/c.parquet", &id, &[small, last])
        .is_err());
    store.multipart_abort("a/c.parquet", &id).unwrap();
    assert!(fake.uploads.borrow().is_empty());
    assert!(!fake.bucket.borrow().contains_key("a/c.parquet"));
}

fn delete_tick(fake: &Fake, service: &Service) -> ingest::retention::DeleteReport {
    fake.new_request();
    let buffer = LogBuffer::new(SID, "del");
    let report =
        ingest::retention::raw_delete(&store(fake, service), &buffer, 4, &Default::default())
            .unwrap();
    telemetry::flush_request_buffer(fake, &buffer);
    report
}

fn converted_fixture(fake: &Fake, count: i64) -> Vec<String> {
    let keys = (0..count)
        .map(|m| put_raw(fake, BASE + m * 60, "a", &[line(BASE + m * 60, "a")]))
        .collect();
    tick(fake).unwrap();
    keys
}

#[test]
fn services_default_to_delete_after_and_thirty_day_retention() {
    let svc = service();
    assert!(svc.delete_after);
    assert_eq!((svc.log_retention_days, svc.rum_retention_days), (30, 30));
}

#[test]
fn raw_delete_removes_only_queued_keys_and_writes_version_two_rows() {
    let fake = Fake::new(BASE + 3600);
    let keys = converted_fixture(&fake, 3);
    let bystander = raw_key(BASE + 1800, "unqueued");
    fake.bucket.borrow_mut().insert(bystander.clone(), vec![1]);
    let svc = service();
    let report = delete_tick(&fake, &svc);
    assert_eq!(
        (report.deleted, report.failed, report.more_pending),
        (3, 0, false)
    );
    for key in &keys {
        assert!(!fake.bucket.borrow().contains_key(key));
    }
    assert!(fake.bucket.borrow().contains_key(&bystander));
    assert!(deleteq(&fake).is_empty());
    let latest = telemetry::latest_ingest_state(&rows(&fake, LogKind::IngestFiles));
    assert_eq!(latest.len(), 3);
    for row in latest {
        assert_eq!(row["state_version"], 2);
        assert_eq!(row["status"], "converted");
        assert_eq!(row["raw_deleted"], true);
        assert!(row["raw_deleted_at"].is_i64() || row["raw_deleted_at"].is_string());
        assert!(keys.contains(&row["raw_key"].as_str().unwrap().to_owned()));
    }
    // The three raw objects plus the committed plan's one intent manifest, in one request.
    assert_eq!(*fake.delete_posts.borrow(), vec![4]);
    assert!(fake.keys("processed/_inflight/").is_empty());
}

#[test]
fn manifest_cleanup_has_one_queue_owner_when_raw_deletion_is_disabled() {
    for inline in [false, true] {
        let fake = Fake::new(BASE + 3600);
        let raw = put_raw(&fake, BASE, "retained", &[line(BASE, "one")]);
        let mut svc = service();
        svc.delete_after = false;
        let buffer = LogBuffer::new(SID, "commit");
        let mut options = ingest::coordinator::Options::request(&fake);
        options.defer_deleteq = inline;
        let report = ingest::coordinator::coordinate(
            &fake,
            &svc,
            "ak",
            "sk",
            Instant::now(),
            &buffer,
            &options,
        )
        .unwrap();
        assert_eq!(report.converted, 1);
        assert!(
            fake.delete_posts.borrow().is_empty(),
            "commit must not eagerly delete"
        );
        let queues: Vec<_> = report.pending_deleteq.into_iter().collect();
        let cleanup = ingest::retention::raw_delete_with_inline(
            &store(&fake, &svc),
            &buffer,
            4,
            &Default::default(),
            &queues,
            !inline,
        )
        .unwrap();
        assert_eq!((cleanup.queues, cleanup.failed, cleanup.deleted), (1, 0, 0));
        assert_eq!(cleanup.backend_calls, if inline { 1 } else { 5 });
        assert_eq!(*fake.delete_posts.borrow(), vec![1]);
        assert!(fake.bucket.borrow().contains_key(&raw));
        assert!(fake.keys("processed/_inflight/").is_empty());
        assert!(deleteq(&fake).is_empty());
        assert!(fake.calls.get() <= BUDGET);
    }
}

#[test]
fn all_held_commit_keeps_a_durable_manifest_cleanup_queue() {
    let fake = Fake::new(BASE + 3600);
    let bad = raw_key(BASE, "bad");
    fake.bucket
        .borrow_mut()
        .insert(bad.clone(), b"broken".to_vec());
    let mut svc = service();
    svc.delete_after = false;
    let buffer = LogBuffer::new(SID, "commit");
    let mut options = ingest::coordinator::Options::request(&fake);
    options.defer_deleteq = true;
    let report =
        ingest::coordinator::coordinate(&fake, &svc, "ak", "sk", Instant::now(), &buffer, &options)
            .unwrap();
    assert_eq!((report.converted, report.failed), (0, 1));
    assert!(report.pending_deleteq.is_none());
    assert_eq!(deleteq(&fake).len(), 1);
    assert!(fake.delete_posts.borrow().is_empty());
    let cleanup = delete_tick(&fake, &svc);
    assert_eq!(
        (cleanup.queues, cleanup.quarantined, cleanup.failed),
        (1, 1, 0)
    );
    assert_eq!(*fake.delete_posts.borrow(), vec![1]);
    assert!(fake.bucket.borrow().contains_key(&bad));
    assert_eq!(quarantine_records(&fake)[0]["objects"][0]["key"], bad);
    assert!(deleteq(&fake).is_empty());
}

#[test]
fn inline_manifest_failure_or_budget_deferral_keeps_a_durable_retry() {
    for (remaining, elapsed) in [(5, 0), (6, 0), (BUDGET, 0), (7, 40)] {
        let fake = Fake::new(BASE + 3600);
        let raw = put_raw(&fake, BASE, "retained", &[line(BASE, "one")]);
        let mut svc = service();
        svc.delete_after = false;
        let buffer = LogBuffer::new(SID, "commit");
        let mut options = ingest::coordinator::Options::request(&fake);
        options.defer_deleteq = true;
        let report = ingest::coordinator::coordinate(
            &fake,
            &svc,
            "ak",
            "sk",
            Instant::now(),
            &buffer,
            &options,
        )
        .unwrap();
        let queue = report.pending_deleteq.unwrap();
        let manifest = queue.1["inflight_keys"][0].as_str().unwrap().to_owned();
        fake.fail_delete_keys.borrow_mut().insert(manifest.clone());
        fake.new_request();
        fake.calls.set(BUDGET - remaining);
        fake.elapsed_s.set(elapsed);
        let cleanup = ingest::retention::raw_delete_with_inline(
            &store(&fake, &svc),
            &buffer,
            4,
            &Default::default(),
            &[queue],
            false,
        )
        .unwrap();
        assert!(cleanup.more_pending, "remaining={remaining}");
        assert_eq!(cleanup.failed, 1, "remaining={remaining}");
        assert_eq!(
            cleanup.backend_calls,
            if remaining == 5 || elapsed == 40 {
                1
            } else {
                2
            }
        );
        assert_eq!(deleteq(&fake).len(), 1, "remaining={remaining}");
        assert_eq!(deleteq(&fake)[0]["inflight_keys"], json!([manifest]));
        assert!(fake.calls.get() <= BUDGET - 4, "tail reserve must survive");
        if remaining == 5 || elapsed == 40 {
            assert!(
                fake.delete_posts.borrow().is_empty(),
                "reserve retry persistence before FOS"
            );
        }
        fake.fail_delete_keys.borrow_mut().clear();
        fake.elapsed_s.set(0);
        let retry = delete_tick(&fake, &svc);
        assert_eq!((retry.queues, retry.failed, retry.deleted), (1, 0, 0));
        assert!(fake.bucket.borrow().contains_key(&raw));
        assert!(deleteq(&fake).is_empty());
        assert!(fake.keys("processed/_inflight/").is_empty());
    }
}

#[test]
fn scheduler_consumes_all_held_queues_with_raw_retention_enabled_from_the_start() {
    for stream in ["request", "rum"] {
        let fake = Fake::new(BASE + 3600);
        tick_runtime(&fake);
        let mut svc = rum_service();
        svc.delete_after = false;
        store_service(&fake, &svc);
        let bad = if stream == "rum" {
            rum_key(BASE, "bad")
        } else {
            raw_key(BASE, "bad")
        };
        fake.bucket
            .borrow_mut()
            .insert(bad.clone(), b"broken".to_vec());
        for _ in 0..3 {
            freshness::finish_tick(&fake);
            if !quarantine_records(&fake).is_empty() {
                break;
            }
            fake.now.set(fake.now.get() + 120);
        }
        assert_eq!(quarantine_records(&fake).len(), 1, "{stream}");
        assert!(deleteq(&fake).is_empty(), "{stream}");
        assert!(fake.keys("processed/_inflight/").is_empty(), "{stream}");
        assert!(fake.bucket.borrow().contains_key(&bad), "{stream}");
        assert_eq!(*fake.delete_posts.borrow(), vec![1], "{stream}");
    }
}

#[test]
fn scheduler_retries_a_durable_rum_held_queue_alongside_inline_request_cleanup() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    let mut svc = rum_service();
    svc.delete_after = false;
    store_service(&fake, &svc);
    let bad = rum_key(BASE, "bad");
    fake.bucket
        .borrow_mut()
        .insert(bad.clone(), b"broken".to_vec());
    assert_eq!(rum_tick(&fake, &svc).unwrap().failed, 1);
    assert_eq!(deleteq(&fake).len(), 1);
    for minute in 0..3 {
        put_raw(
            &fake,
            BASE + minute * 60,
            "retained",
            &[line(BASE + minute * 60, "request")],
        );
        run_tick(&fake);
        fake.now.set(fake.now.get() + 120);
    }
    assert!(
        deleteq(&fake).is_empty(),
        "due raw-delete discovery must not starve behind inline commits: {:?}; jobs={}",
        deleteq(&fake),
        fake.kv.borrow()["cron:state"].1["jobs"],
    );
    assert_eq!(quarantine_records(&fake)[0]["objects"][0]["key"], bad);
    assert!(fake.bucket.borrow().contains_key(&bad));
    assert!(fake.keys("processed/_inflight/").is_empty());
}

#[test]
fn delete_queue_decoding_preserves_legacy_times_streams_and_partial_retries() {
    for inline in [false, true] {
        let fake = Fake::new(BASE + 3600);
        let mut svc = service();
        svc.delete_after = false;
        let mut queues = Vec::new();
        for stream in ["request", "rum"] {
            let run = format!("legacy-{stream}");
            let manifest = format!("processed/_inflight/{run}.json");
            let raw = format!("raw/{stream}/held.zst");
            fake.bucket.borrow_mut().insert(manifest.clone(), vec![1]);
            fake.bucket.borrow_mut().insert(raw.clone(), vec![2]);
            fake.fail_delete_keys.borrow_mut().insert(manifest.clone());
            let value = json!({
                "run_id": run, "stream": stream, "keys": [], "sizes": "legacy-invalid",
                "inflight_keys": [manifest], "held": [{"key": raw, "reason": "corrupt_gzip"}],
                "at": BASE,
            });
            let key = format!("{}{run}", ingest::DELETEQ_PREFIX);
            if !inline {
                fake.kv.borrow_mut().insert(key.clone(), (7, value.clone()));
            }
            queues.push((key, value));
        }
        let buffer = LogBuffer::new(SID, "delete");
        let report = ingest::retention::raw_delete_with_inline(
            &store(&fake, &svc),
            &buffer,
            4,
            &Default::default(),
            if inline { &queues } else { &[] },
            !inline,
        )
        .unwrap();
        assert_eq!(
            (report.failed, report.quarantined, report.deleted),
            (2, 2, 0)
        );
        let quarantine = quarantine_records(&fake);
        assert_eq!(quarantine.len(), 2);
        assert!(quarantine.iter().all(|record| record["first_seen"] == BASE));
        for (key, original) in queues {
            let kv = fake.kv.borrow();
            let (generation, value) = &kv[&key];
            assert_eq!(*generation, if inline { 1 } else { 8 });
            assert_eq!(value["stream"], original["stream"]);
            assert_eq!(value["run_id"], original["run_id"]);
            assert_eq!(value["inflight_keys"], original["inflight_keys"]);
            assert_eq!(value["held"], json!([]));
            assert_eq!(value["planned_at"], BASE);
            assert_eq!(value["sizes"], json!([]));
        }
        fake.fail_delete_keys.borrow_mut().clear();
        assert_eq!(delete_tick(&fake, &svc).queues, 2);
        assert!(deleteq(&fake).is_empty());
        assert_eq!(quarantine_records(&fake).len(), 2);
    }
}

#[test]
fn malformed_inline_and_stored_delete_queues_return_the_existing_error() {
    for inline in [false, true] {
        let fake = Fake::new(BASE + 3600);
        let value = json!({"run_id": "invalid", "keys": [null], "held": []});
        let key = format!("{}invalid", ingest::DELETEQ_PREFIX);
        if !inline {
            fake.kv.borrow_mut().insert(key.clone(), (1, value.clone()));
        }
        let buffer = LogBuffer::new(SID, "delete");
        let queues = vec![(key, value)];
        let result = ingest::retention::raw_delete_with_inline(
            &store(&fake, &service()),
            &buffer,
            4,
            &Default::default(),
            if inline { &queues } else { &[] },
            !inline,
        );
        assert_eq!(result.unwrap_err(), "deleteq_invalid");
        assert!(fake.delete_posts.borrow().is_empty());
    }
}

#[test]
fn raw_delete_keeps_raw_objects_but_consumes_the_queue_when_delete_after_is_off() {
    let fake = Fake::new(BASE + 3600);
    let keys = converted_fixture(&fake, 2);
    let mut svc = service();
    svc.delete_after = false;
    let report = delete_tick(&fake, &svc);
    assert_eq!(report.deleted, 0);
    // Only the plan's intent manifest is deleted; the queue record is consumed, not kept.
    assert_eq!(*fake.delete_posts.borrow(), vec![1]);
    assert!(deleteq(&fake).is_empty());
    assert!(fake.keys("processed/_inflight/").is_empty());
    assert!(keys.iter().all(|k| fake.bucket.borrow().contains_key(k)));
}

#[test]
fn a_failed_inflight_delete_counts_as_failed_when_delete_after_is_off() {
    let fake = Fake::new(BASE + 3600);
    converted_fixture(&fake, 2);
    let inflight = deleteq(&fake)[0]["inflight_keys"][0]
        .as_str()
        .unwrap()
        .to_owned();
    fake.fail_delete_keys.borrow_mut().insert(inflight.clone());
    let mut svc = service();
    svc.delete_after = false;
    let report = delete_tick(&fake, &svc);
    assert_eq!((report.deleted, report.failed), (0, 1));
    assert_eq!(deleteq(&fake)[0]["inflight_keys"], json!([inflight]));
    fake.fail_delete_keys.borrow_mut().clear();
    assert_eq!(delete_tick(&fake, &svc).failed, 0);
    assert!(deleteq(&fake).is_empty());
}

#[test]
fn per_key_delete_errors_are_requeued_and_retried() {
    let fake = Fake::new(BASE + 3600);
    let keys = converted_fixture(&fake, 3);
    fake.fail_delete_keys.borrow_mut().insert(keys[1].clone());
    let svc = service();
    let report = delete_tick(&fake, &svc);
    assert_eq!((report.deleted, report.failed), (2, 1));
    let queued = deleteq(&fake);
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0]["keys"], json!([keys[1]]));
    assert_eq!(queued[0]["sizes"].as_array().unwrap().len(), 1);
    assert!(fake.bucket.borrow().contains_key(&keys[1]));
    let rows_now = telemetry::latest_ingest_state(&rows(&fake, LogKind::IngestFiles));
    assert_eq!(
        rows_now.iter().filter(|r| r["raw_deleted"] == true).count(),
        2
    );
    fake.fail_delete_keys.borrow_mut().clear();
    assert_eq!(delete_tick(&fake, &svc).deleted, 1);
    assert!(deleteq(&fake).is_empty());
}

#[test]
fn delete_objects_batches_at_one_thousand_keys() {
    let fake = Fake::new(BASE);
    let keys: Vec<String> = (0..2500).map(|i| format!("raw/request/k{i:04}")).collect();
    for key in &keys {
        fake.bucket.borrow_mut().insert(key.clone(), vec![0; 3]);
    }
    let svc = service();
    fake.new_request();
    let result = store(&fake, &svc).delete_objects(&keys).unwrap();
    assert_eq!(result.deleted.len(), 2500);
    assert!(result.failed.is_empty());
    assert_eq!(*fake.delete_posts.borrow(), vec![1000, 1000, 500]);
}

#[test]
fn raw_delete_leaves_the_queue_when_the_budget_cannot_cover_it() {
    let fake = Fake::new(BASE + 3600);
    converted_fixture(&fake, 2);
    let svc = service();
    fake.new_request();
    fake.calls.set(BUDGET - 3);
    let buffer = LogBuffer::new(SID, "del");
    let report =
        ingest::retention::raw_delete(&store(&fake, &svc), &buffer, 4, &Default::default())
            .unwrap();
    assert_eq!(report.deleted, 0);
    assert!(report.more_pending);
    assert_eq!(deleteq(&fake).len(), 1);
    assert!(fake.delete_posts.borrow().is_empty());
}

fn quarantine_records(fake: &Fake) -> Vec<Value> {
    fake.kv
        .borrow()
        .iter()
        .filter(|(k, _)| k.starts_with(ingest::QUARANTINE_PREFIX))
        .map(|(_, (_, v))| v.clone())
        .collect()
}

/// One committed plan with a corrupt object and a good one; returns (bad, good).
fn quarantined_fixture(fake: &Fake) -> (String, String) {
    let bad = raw_key(BASE, "bad");
    fake.bucket
        .borrow_mut()
        .insert(bad.clone(), b"not gzip".to_vec());
    let good = put_raw(fake, BASE + 60, "ok", &[line(BASE + 60, "good")]);
    tick(fake).unwrap();
    (bad, good)
}

fn seed_quarantine(fake: &Fake, run: &str, keys: &[String]) {
    let objects: Vec<Value> = keys
        .iter()
        .map(|key| {
            fake.bucket.borrow_mut().insert(key.clone(), vec![1, 2, 3]);
            json!({"key": key, "size": 3, "status": "quarantined", "reason": "corrupt_gzip",
                "attempts": 2})
        })
        .collect();
    fake.kv.borrow_mut().insert(
        format!("{}{run}", ingest::QUARANTINE_PREFIX),
        (
            1,
            json!({"run_id": run, "objects": objects, "first_seen": BASE, "at": BASE + 10}),
        ),
    );
}

fn set_watermark(fake: &Fake, key: &str) {
    fake.kv.borrow_mut().insert(
        ingest::WATERMARK_KEY.into(),
        (1, json!({"key": key, "run_id": "seeded", "at": BASE})),
    );
}

fn quarantine_page(fake: &Fake, query: &str) -> Result<Value, (u16, &'static str)> {
    fake.new_request();
    api::quarantine(fake, &service(), query)
}

fn purge_as(fake: &Fake, actor: &str, body: Value) -> Result<Value, (u16, &'static str)> {
    fake.new_request();
    let buffer = LogBuffer::new(SID, "purge");
    let result = api::quarantine_purge(
        fake,
        &service(),
        actor,
        body.to_string().as_bytes(),
        &buffer,
        Instant::now(),
    );
    assert!(
        fake.calls.get() + api::PURGE_RESERVE <= BUDGET,
        "a purge step used {} calls",
        fake.calls.get()
    );
    telemetry::flush_request_buffer(fake, &buffer);
    result.map(|purge| purge.body)
}

fn purge(fake: &Fake, mut body: Value) -> Result<Value, (u16, &'static str)> {
    let issued = purge_as(fake, "u-admin", body.clone())?;
    assert_eq!(issued["status"], "confirm_required");
    body["confirm"] = issued["confirm_token"].clone();
    purge_as(fake, "u-admin", body)
}

#[test]
fn held_objects_move_to_a_quarantine_record_once_their_plan_is_committed() {
    let fake = Fake::new(BASE + 3600);
    let (bad, good) = quarantined_fixture(&fake);
    let queues = deleteq(&fake);
    assert_eq!(queues[0]["held"][0]["key"], bad);
    assert_eq!(queues[0]["held"][0]["reason"], "corrupt_gzip");
    assert_eq!(queues[0]["held"][0]["attempts"], 1);
    assert!(quarantine_records(&fake).is_empty());
    let report = delete_tick(&fake, &service());
    assert_eq!((report.deleted, report.quarantined), (1, 1));
    let records = quarantine_records(&fake);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["objects"][0]["key"], bad);
    assert_eq!(records[0]["objects"][0]["status"], "quarantined");
    assert!(records[0]["first_seen"].as_u64().unwrap() > 0);
    assert!(deleteq(&fake).is_empty());
    let bucket = fake.bucket.borrow();
    assert!(bucket.contains_key(&bad) && !bucket.contains_key(&good));
}

#[test]
fn held_objects_of_an_uncommitted_plan_are_never_quarantined() {
    let fake = Fake::new(BASE + 3600);
    let held = raw_key(BASE, "held");
    fake.kv.borrow_mut().insert(
        ingest::PLAN_KEY.into(),
        (1, json!({"plan_id": "p-open", "version": 99})),
    );
    fake.kv.borrow_mut().insert(
        "ingest:deleteq:p-open".into(),
        (
            1,
            json!({"run_id": "p-open", "keys": [], "sizes": [], "inflight_keys": [],
            "held": [{"key": held, "size": 1, "status": "failed", "reason": "x", "attempts": 1}],
            "planned_at": BASE, "at": BASE}),
        ),
    );
    delete_tick(&fake, &service());
    assert!(quarantine_records(&fake).is_empty());
    assert_eq!(deleteq(&fake).len(), 1);
}

#[test]
fn a_held_only_queue_is_quarantined_even_when_delete_after_is_off() {
    let fake = Fake::new(BASE + 3600);
    let (bad, _) = quarantined_fixture(&fake);
    let mut svc = service();
    svc.delete_after = false;
    delete_tick(&fake, &svc);
    assert_eq!(quarantine_records(&fake)[0]["objects"][0]["key"], bad);
    assert!(deleteq(&fake).is_empty());
}

#[test]
fn quarantine_lists_reason_size_first_seen_and_attempts_in_signed_pages() {
    let fake = Fake::new(BASE + 3600);
    let (bad, _) = quarantined_fixture(&fake);
    delete_tick(&fake, &service());
    for i in 0..12 {
        seed_quarantine(
            &fake,
            &format!("seed{i:02}"),
            &[raw_key(BASE - 3600, &format!("s{i}"))],
        );
    }
    let first = quarantine_page(&fake, "").unwrap();
    assert_eq!(first["complete"], false);
    let items = first["items"].as_array().unwrap();
    assert_eq!(items.len(), 10);
    let mut all: Vec<Value> = items.clone();
    let cursor = first["next_cursor"].as_str().unwrap();
    let second = quarantine_page(&fake, &cursor_query(cursor)).unwrap();
    assert_eq!(second["complete"], true);
    all.extend(second["items"].as_array().unwrap().iter().cloned());
    assert_eq!(all.len(), 13);
    let item = all.iter().find(|i| i["key"] == bad).unwrap();
    assert_eq!(item["reason"], "corrupt_gzip");
    assert_eq!(item["status"], "quarantined");
    assert_eq!(item["size"], 8);
    assert_eq!(item["attempts"], 1);
    assert!(item["first_seen"].as_str().unwrap().ends_with('Z'));
    assert_eq!(item["purgeable"], true);
    let seeded = all.iter().find(|i| i["record"] == "seed03").unwrap();
    assert_eq!(seeded["attempts"], 2);
    assert_eq!(
        quarantine_page(&fake, "cursor=forged.abc").unwrap_err().0,
        400
    );
}

#[test]
fn purge_requires_a_confirm_token_bound_to_actor_target_and_time() {
    let fake = Fake::new(BASE + 3600);
    let (bad, _) = quarantined_fixture(&fake);
    delete_tick(&fake, &service());
    let issued = purge_as(&fake, "u-admin", json!({"target": "all"})).unwrap();
    assert_eq!(issued["status"], "confirm_required");
    assert!(
        fake.bucket.borrow().contains_key(&bad),
        "issuing a token deletes nothing"
    );
    let token = issued["confirm_token"].clone();
    let other_actor = purge_as(&fake, "u-other", json!({"target": "all", "confirm": token}));
    let other_target = purge_as(
        &fake,
        "u-admin",
        json!({"target": bad, "record": "x", "confirm": token}),
    );
    let forged = purge_as(
        &fake,
        "u-admin",
        json!({"target": "all", "confirm": "e30.00"}),
    );
    for rejected in [other_actor, other_target, forged] {
        assert_eq!(rejected.unwrap_err().0, 400);
    }
    fake.now.set(fake.now.get() + 601);
    let expired = purge_as(&fake, "u-admin", json!({"target": "all", "confirm": token}));
    assert_eq!(expired.unwrap_err().0, 400);
    assert!(fake.bucket.borrow().contains_key(&bad));
    assert_eq!(quarantine_records(&fake).len(), 1);
}

#[test]
fn purge_all_deletes_only_quarantined_keys_at_or_below_the_watermark_and_never_reingests() {
    let fake = Fake::new(BASE + 3600);
    let (bad, good) = quarantined_fixture(&fake);
    delete_tick(&fake, &service());
    let bystander = raw_key(BASE - 600, "unqueued");
    fake.bucket.borrow_mut().insert(bystander.clone(), vec![1]);
    let pending = put_raw(&fake, BASE + 120, "pending", &[line(BASE + 120, "pending")]);
    let ahead = raw_key(BASE + 1200, "ahead");
    seed_quarantine(&fake, "zz-ahead", std::slice::from_ref(&ahead));
    let ips = processed_ips(&fake);
    let done = purge(&fake, json!({"target": "all"})).unwrap();
    assert_eq!(
        (
            done["purged"].as_u64(),
            done["skipped"].as_u64(),
            done["complete"].as_bool()
        ),
        (Some(1), Some(1), Some(true))
    );
    assert_eq!(done["bytes"], 8);
    let bucket = fake.bucket.borrow();
    assert!(!bucket.contains_key(&bad));
    for kept in [&bystander, &pending, &ahead] {
        assert!(bucket.contains_key(kept), "{kept} must survive a purge");
    }
    assert!(!bucket.contains_key(&good));
    drop(bucket);
    let records = quarantine_records(&fake);
    assert_eq!(
        records.len(),
        1,
        "only the record above the watermark is left"
    );
    assert_eq!(records[0]["objects"][0]["key"], ahead);
    let latest = telemetry::latest_ingest_state(&rows(&fake, LogKind::IngestFiles));
    let row = latest.iter().find(|r| r["raw_key"] == bad).unwrap();
    assert_eq!(
        (&row["state_version"], &row["status"], &row["raw_deleted"]),
        (&json!(2), &json!("quarantined"), &json!(true))
    );
    let next = tick(&fake).unwrap();
    assert_eq!(
        next.listed, 2,
        "only the unconverted objects above the watermark"
    );
    assert_eq!(next.converted, 1);
    let mut expected = ips;
    expected.push("pending".into());
    expected.sort();
    assert_eq!(processed_ips(&fake), expected);
}

#[test]
fn purge_one_deletes_just_that_object_and_keeps_the_rest_of_its_record() {
    let fake = Fake::new(BASE + 3600);
    let keys: Vec<String> = (0..3)
        .map(|i| raw_key(BASE - 600, &format!("q{i}")))
        .collect();
    seed_quarantine(&fake, "run1", &keys);
    set_watermark(&fake, &raw_key(BASE, "mark"));
    let done = purge(&fake, json!({"target": keys[1], "record": "run1"})).unwrap();
    assert_eq!(
        (done["purged"].as_u64(), done["complete"].as_bool()),
        (Some(1), Some(true))
    );
    let bucket = fake.bucket.borrow();
    assert!(bucket.contains_key(&keys[0]) && !bucket.contains_key(&keys[1]));
    drop(bucket);
    let left: Vec<Value> = quarantine_records(&fake)[0]["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["key"].clone())
        .collect();
    assert_eq!(left, vec![json!(keys[0]), json!(keys[2])]);
    let unknown = raw_key(BASE - 600, "never-quarantined");
    fake.bucket.borrow_mut().insert(unknown.clone(), vec![1]);
    let missing = purge(&fake, json!({"target": unknown, "record": "run1"}));
    assert_eq!(missing.unwrap_err().0, 404);
    assert!(fake.bucket.borrow().contains_key(&unknown));
    let ahead = raw_key(BASE + 600, "ahead");
    seed_quarantine(&fake, "run2", std::slice::from_ref(&ahead));
    let refused = purge(&fake, json!({"target": ahead, "record": "run2"}));
    assert_eq!(refused.unwrap_err().0, 409);
    assert!(fake.bucket.borrow().contains_key(&ahead));
    let bad_record = purge_as(
        &fake,
        "u-admin",
        json!({"target": keys[0], "record": "../x"}),
    );
    assert_eq!(bad_record.unwrap_err().0, 400);
}

#[test]
fn purge_all_is_bounded_per_step_and_resumes_from_its_cursor() {
    let fake = Fake::new(BASE + 3600);
    let mut keys = Vec::new();
    for run in 0..25 {
        let pair: Vec<String> = (0..2)
            .map(|i| raw_key(BASE - 3600 + run * 60, &format!("r{i}")))
            .collect();
        seed_quarantine(&fake, &format!("run{run:02}"), &pair);
        keys.extend(pair);
    }
    set_watermark(&fake, &raw_key(BASE, "mark"));
    let mut body = json!({"target": "all"});
    let issued = purge_as(&fake, "u-admin", body.clone()).unwrap();
    body["confirm"] = issued["confirm_token"].clone();
    let (mut steps, mut purged) = (0, 0);
    loop {
        steps += 1;
        assert!(steps < 20, "purge never completed");
        let posts = fake.delete_posts.borrow().len();
        let step = purge_as(&fake, "u-admin", body.clone()).unwrap();
        assert!(fake.delete_posts.borrow().len() - posts <= 1);
        purged += step["purged"].as_u64().unwrap();
        if step["complete"] == true {
            break;
        }
        assert!(
            step["purged"].as_u64().unwrap() > 0,
            "a step must make progress"
        );
        body["cursor"] = step["next_cursor"].clone();
        body["confirm"] = step["confirm_token"].clone();
    }
    assert!(steps > 2, "25 records cannot fit one step");
    assert_eq!(purged, 50);
    assert!(keys.iter().all(|k| !fake.bucket.borrow().contains_key(k)));
    assert!(quarantine_records(&fake).is_empty());
    body["cursor"] = json!("forged.cursor");
    assert_eq!(purge_as(&fake, "u-admin", body).unwrap_err().0, 400);
}

#[test]
fn a_purge_step_with_too_little_budget_deletes_nothing() {
    let fake = Fake::new(BASE + 3600);
    let key = raw_key(BASE - 600, "q");
    seed_quarantine(&fake, "run1", std::slice::from_ref(&key));
    set_watermark(&fake, &raw_key(BASE, "mark"));
    let issued = purge_as(&fake, "u-admin", json!({"target": "all"})).unwrap();
    fake.new_request();
    fake.calls.set(BUDGET - api::PURGE_RESERVE - 5);
    let buffer = LogBuffer::new(SID, "purge");
    let body = json!({"target": "all", "confirm": issued["confirm_token"]});
    let result = api::quarantine_purge(
        &fake,
        &service(),
        "u-admin",
        body.to_string().as_bytes(),
        &buffer,
        Instant::now(),
    );
    assert_eq!(result.err().map(|e| e.0), Some(503));
    assert!(fake.bucket.borrow().contains_key(&key));
    assert_eq!(quarantine_records(&fake).len(), 1);
}

const DAY: i64 = 86_400;

fn put_processed(fake: &Fake, prefix: &str, hour_start: i64, name: &str) -> String {
    let key = format!(
        "{prefix}{}window={name}.parquet",
        ingest::hour_dir(hour_start)
    );
    fake.bucket.borrow_mut().insert(key.clone(), vec![0; 10]);
    key
}

fn sweep(
    fake: &Fake,
    service: &Service,
    settings: &telemetry::OperationalLogSettings,
) -> ingest::retention::SweepReport {
    fake.new_request();
    ingest::retention::retention_sweep(&store(fake, service), settings, 4, None).unwrap()
}

#[test]
fn sweep_deletes_whole_hour_folders_older_than_each_datasets_cutoff() {
    let fake = Fake::new(BASE + 40 * DAY);
    let old_req = put_processed(&fake, "processed/request/", BASE, "1-2-a-n1");
    let new_req = put_processed(&fake, "processed/request/", BASE + 20 * DAY, "1-2-a-n1");
    let old_rum = put_processed(&fake, "processed/rum/", BASE, "1-2-a-n1");
    let old_ops = put_processed(&fake, "processed/ops/cron_ticks/", BASE, "1-2-a-n1");
    let new_ops = put_processed(
        &fake,
        "processed/ops/cron_ticks/",
        BASE + 39 * DAY,
        "1-2-a-n1",
    );
    let mut svc = service();
    svc.rum_retention_days = 0;
    let mut settings = telemetry::OperationalLogSettings::default();
    settings
        .retention_days_by_kind
        .insert("cron_ticks".into(), 7);
    let report = sweep(&fake, &svc, &settings);
    let bucket = fake.bucket.borrow();
    assert!(!bucket.contains_key(&old_req) && bucket.contains_key(&new_req));
    assert!(bucket.contains_key(&old_rum), "0 days keeps forever");
    assert!(!bucket.contains_key(&old_ops) && bucket.contains_key(&new_ops));
    assert_eq!(report.deleted, 2);
    assert!(report.bytes >= 20);
}

fn sweep_request(fake: &Fake) -> ingest::retention::SweepReport {
    fake.new_request();
    let svc = service();
    ingest::retention::retention_sweep(&store(fake, &svc), &Default::default(), 4, Some("request"))
        .unwrap()
}

#[test]
fn sweep_purges_the_service_edge_cache_only_after_deleting() {
    let fake = Fake::new(BASE + 40 * DAY);
    let fresh = put_processed(&fake, "processed/request/", BASE + 39 * DAY, "1-2-a-n1");
    sweep_request(&fake);
    assert!(fake.bucket.borrow().contains_key(&fresh));
    assert!(
        fake.api_calls.borrow().is_empty(),
        "nothing deleted, nothing purged"
    );

    put_processed(&fake, "processed/request/", BASE, "1-2-a-n1");
    fake.now.set(fake.now.get() + DAY as u64);
    let report = sweep_request(&fake);
    assert_eq!(report.deleted, 1);
    assert!(!report.more_pending);
    assert_eq!(
        *fake.api_calls.borrow(),
        vec![(
            "POST".to_string(),
            format!("/service/v/purge/flp-fos-{SID}")
        )]
    );
    assert!(!fake.kv.borrow().contains_key("retention:purge"));
}

#[test]
fn failed_edge_purge_is_retried_by_the_next_sweep() {
    let fake = Fake::new(BASE + 40 * DAY);
    let old = put_processed(&fake, "processed/request/", BASE, "1-2-a-n1");
    fake.api_status.set(503);
    let first = sweep_request(&fake);
    assert!(!fake.bucket.borrow().contains_key(&old));
    assert!(
        first.more_pending,
        "a pending purge keeps the sweep running"
    );
    assert!(fake.kv.borrow().contains_key("retention:purge"));

    fake.api_status.set(200);
    let second = sweep_request(&fake);
    assert_eq!(second.deleted, 0);
    assert!(!second.more_pending);
    assert_eq!(fake.api_calls.borrow().len(), 2);
    assert!(!fake.kv.borrow().contains_key("retention:purge"));
    sweep_request(&fake);
    assert_eq!(fake.api_calls.borrow().len(), 2);
}

#[test]
fn sweep_without_room_to_purge_leaves_the_purge_pending() {
    let fake = Fake::new(BASE + 40 * DAY);
    put_processed(&fake, "processed/request/", BASE, "1-2-a-n1");
    let svc = service();
    // Catalog read, state get, list, mark, delete and state CAS fit; purge does not.
    fake.new_request();
    fake.calls.set(BUDGET - 4 - 6);
    let report = ingest::retention::retention_sweep(
        &store(&fake, &svc),
        &Default::default(),
        4,
        Some("request"),
    )
    .unwrap();
    assert_eq!(report.deleted, 1);
    assert!(report.more_pending);
    assert!(fake.api_calls.borrow().is_empty());
    assert!(fake.kv.borrow().contains_key("retention:purge"));
}

#[test]
fn sweep_keeps_expired_day_compaction_only_when_its_day_is_still_inside_retention() {
    let fake = Fake::new(BASE + 40 * DAY);
    let day = |offset: i64| {
        let t = ingest::hour_dir(BASE + offset * DAY);
        t.rsplit_once("hour=").unwrap().0.to_owned()
    };
    let old = format!("processed/request/{}compact=a.parquet", day(0));
    let fresh = format!("processed/request/{}compact=b.parquet", day(20));
    for key in [&old, &fresh] {
        fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
    }
    sweep(&fake, &service(), &Default::default());
    assert!(!fake.bucket.borrow().contains_key(&old));
    assert!(fake.bucket.borrow().contains_key(&fresh));
}

#[test]
fn sweep_persists_each_batch_before_deleting_and_resumes_after_a_crash() {
    let fake = Fake::new(BASE + 40 * DAY);
    let keys: Vec<String> = (0..3)
        .map(|h| put_processed(&fake, "processed/request/", BASE + h * 3600, "1-2-a-n1"))
        .collect();
    let svc = service();
    // Catalog read, list, state get and persistence fit; delete does not.
    fake.new_request();
    fake.calls.set(BUDGET - 4 - 5);
    let first = ingest::retention::retention_sweep(
        &store(&fake, &svc),
        &Default::default(),
        4,
        Some("request"),
    )
    .unwrap();
    assert_eq!(first.deleted, 0);
    assert!(fake.delete_posts.borrow().is_empty());
    let state = fake.kv.borrow()["retention:request"].1.clone();
    assert_eq!(state["pending"].as_array().unwrap().len(), 3);
    let second = sweep(&fake, &svc, &Default::default());
    assert_eq!(second.deleted, 3);
    assert!(keys.iter().all(|k| !fake.bucket.borrow().contains_key(k)));
    let state = fake.kv.borrow()["retention:request"].1.clone();
    assert!(state["pending"].as_array().unwrap().is_empty());
    assert_eq!(state["done"], true);
}

#[test]
fn failed_keys_stay_pending_and_a_completed_generation_is_not_rescanned() {
    let fake = Fake::new(BASE + 40 * DAY);
    let key = put_processed(&fake, "processed/request/", BASE, "1-2-a-n1");
    fake.fail_delete_keys.borrow_mut().insert(key.clone());
    let svc = service();
    let first = sweep(&fake, &svc, &Default::default());
    assert_eq!((first.deleted, first.failed), (0, 1));
    fake.fail_delete_keys.borrow_mut().clear();
    assert_eq!(sweep(&fake, &svc, &Default::default()).deleted, 1);
    fake.delete_posts.borrow_mut().clear();
    let again = sweep(&fake, &svc, &Default::default());
    assert_eq!(again.deleted, 0);
    assert!(!fake.bucket.borrow().contains_key(&key));
}

#[test]
fn shortening_retention_starts_a_new_generation_from_the_beginning() {
    let fake = Fake::new(BASE + 40 * DAY);
    let mid = put_processed(&fake, "processed/request/", BASE + 20 * DAY, "1-2-a-n1");
    put_processed(&fake, "processed/request/", BASE, "1-2-a-n1");
    let mut svc = service();
    sweep(&fake, &svc, &Default::default());
    assert!(fake.bucket.borrow().contains_key(&mid));
    let before = fake.kv.borrow()["retention:request"].1["generation"].clone();
    svc.log_retention_days = 10;
    sweep(&fake, &svc, &Default::default());
    assert!(!fake.bucket.borrow().contains_key(&mid));
    assert_ne!(
        fake.kv.borrow()["retention:request"].1["generation"],
        before
    );
}

#[test]
fn unsupported_delete_objects_falls_back_to_bounded_worker_fanout() {
    let fake = Fake::new(BASE + 40 * DAY);
    fake.delete_status.set(501);
    let keys: Vec<String> = (0..70)
        .map(|i| {
            put_processed(
                &fake,
                "processed/request/",
                BASE + i * 60,
                &format!("{i}-a-n1"),
            )
        })
        .collect();
    let mut settings = telemetry::OperationalLogSettings {
        retention_fanout: 2,
        ..Default::default()
    };
    let report = sweep(&fake, &service(), &settings);
    assert_eq!(*fake.worker_calls.borrow(), vec![28, 28]);
    assert_eq!(report.deleted, 56);
    assert_eq!(report.failed, 14);
    let state = fake.kv.borrow()["retention:request"].1.clone();
    assert_eq!(state["pending"].as_array().unwrap().len(), 14);
    assert!(report.more_pending);
    settings.retention_fanout = 8;
    fake.worker_calls.borrow_mut().clear();
    assert_eq!(sweep(&fake, &service(), &settings).deleted, 14);
    assert!(keys.iter().all(|k| !fake.bucket.borrow().contains_key(k)));
}

#[test]
fn worker_delete_authenticates_bounds_and_scopes_keys() {
    let fake = Fake::new(BASE);
    let in_scope = "p/processed/request/year=2020/month=01/day=01/hour=00/window=a.parquet";
    fake.bucket.borrow_mut().insert(in_scope.into(), vec![1]);
    let call = |key: Option<&str>, body: Value| {
        let mut headers = BTreeMap::new();
        if let Some(key) = key {
            headers.insert("x-flp-worker-key".to_owned(), key.to_owned());
        }
        ingest::retention::handle_worker_delete(
            &fake,
            &fastly_log_analytics::http::Request {
                method: "POST".into(),
                path: "/_internal/delete".into(),
                headers,
                body: body.to_string().into_bytes(),
            },
        )
    };
    let base = |keys: Vec<String>| {
        json!({"service_id": SID, "region": "us-east-1", "bucket": "fos-abcdefghijklmnopqrst12-logs",
               "prefix": "p/", "keys": keys})
    };
    assert_eq!(call(None, base(vec![in_scope.into()])).status, 403);
    assert_eq!(call(Some("wrong"), base(vec![in_scope.into()])).status, 403);
    let many: Vec<String> = (0..29)
        .map(|i| format!("p/processed/request/{i}.parquet"))
        .collect();
    assert_eq!(call(Some("fos-secret"), base(many)).status, 400);
    for bad in [
        "other/processed/request/a.parquet",
        "p/processed/../secret",
        "p/svc/a",
    ] {
        assert_eq!(
            call(Some("fos-secret"), base(vec![bad.into()])).status,
            400,
            "{bad}"
        );
    }
    assert!(fake.bucket.borrow().contains_key(in_scope));
    let ok = call(Some("fos-secret"), base(vec![in_scope.into()]));
    assert_eq!(ok.status, 200);
    let body: Value = serde_json::from_slice(&ok.body).unwrap();
    assert_eq!(body["deleted"], json!([in_scope]));
    assert!(body["logs"].is_array());
    assert!(!fake.bucket.borrow().contains_key(in_scope));
}

fn ops_parquet(fake: &Fake, kind: LogKind, hour_start: i64, rows: &[Value]) -> String {
    let key = format!(
        "processed/ops/{}/{}window=1-2-abc-n{}.parquet",
        kind.as_str(),
        ingest::hour_dir(hour_start),
        rows.len()
    );
    let bytes = telemetry::schema::to_parquet(kind, rows).unwrap();
    fake.bucket.borrow_mut().insert(key.clone(), bytes);
    key
}

#[test]
fn ingested_files_reports_the_latest_state_per_raw_key_from_outbox_then_parquet() {
    let fake = Fake::new(BASE + 3600);
    let keys = converted_fixture(&fake, 3);
    fake.fail_delete_keys.borrow_mut().insert(keys[2].clone());
    delete_tick(&fake, &service());
    let old = raw_key(BASE - 7200, "old");
    ops_parquet(
        &fake,
        LogKind::IngestFiles,
        BASE - 7200,
        &[
            json!({"timestamp": (BASE - 7200) * 1000, "event_id": "a", "service_id": SID,
                   "severity": "info", "run_id": "r0", "dataset": "request", "raw_key": old,
                   "raw_size": 9, "status": "converted", "state_version": 1, "rows": 4,
                   "raw_deleted": false}),
            json!({"timestamp": (BASE - 7000) * 1000, "event_id": "b", "service_id": SID,
                   "severity": "info", "run_id": "r1", "dataset": "request", "raw_key": keys[0],
                   "raw_size": 9, "status": "converted", "state_version": 1, "rows": 1,
                   "raw_deleted": false}),
        ],
    );
    let page = admin_page(&fake, None, "").unwrap();
    let files = page["files"].as_array().unwrap();
    assert_eq!(files.len(), 4);
    let by_key = |key: &str| files.iter().find(|f| f["key"] == key).unwrap().clone();
    assert_eq!(by_key(&keys[0])["raw_deleted"], true);
    assert_eq!(by_key(&keys[1])["raw_deleted"], true);
    assert_eq!(by_key(&keys[2])["raw_deleted"], false);
    assert_eq!(by_key(&old)["raw_deleted"], false);
    assert_eq!(by_key(&old)["status"], "converted");
    let minute = by_key(&keys[0]);
    assert_eq!(minute["start"], ingest::rfc3339(BASE));
    assert_eq!(minute["end"], ingest::rfc3339(BASE + 60));
    assert_eq!(minute["rows"], 1);
    assert!(minute["size"].as_u64().unwrap() > 0);
    assert!(minute["last_modified"].is_string());
    assert_eq!(
        (page["complete"].clone(), page["next_cursor"].clone()),
        (json!(true), Value::Null)
    );
    assert_eq!(page["skipped_objects"], 0);
    assert!(fake
        .cdn_gets
        .borrow()
        .iter()
        .any(|k| k.starts_with("processed/ops/ingest_files/")));
}

fn files_for(fake: &Fake, dataset: &str) -> Result<Value, (u16, &'static str)> {
    files_page(
        fake,
        &format!(
            "from={}&to={}&dataset={dataset}",
            iso_url(BASE),
            iso_url(BASE + 3600)
        ),
    )
}

#[test]
fn files_dataset_selects_request_rum_or_one_ops_kind() {
    let fake = Fake::new(BASE);
    for (prefix, name) in [
        ("processed/request/", 1),
        ("processed/rum/", 2),
        ("processed/ops/cron_ticks/", 3),
        ("processed/ops/platform_errors/", 4),
    ] {
        let (start, end) = if prefix.contains("ops") {
            (BASE / 60 + name, BASE / 60 + name)
        } else {
            (BASE + name, BASE + name + 60)
        };
        put_processed(&fake, prefix, BASE, &format!("{start}-{end}-a-n{name}"));
    }
    publish_files(&fake, &fake.keys("processed/request/"), &[], &[]);
    let keys = |dataset: &str| -> Vec<String> {
        files_for(&fake, dataset).unwrap()["files"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["key"].as_str().unwrap().to_owned())
            .collect()
    };
    assert!(keys("rum")[0].starts_with("processed/rum/"));
    assert!(keys("ops:cron_ticks")[0].starts_with("processed/ops/cron_ticks/"));
    assert_eq!(keys("ops:cron_ticks").len(), 1);
    assert!(keys("request")[0].starts_with("processed/request/"));
    for bad in [
        "ops",
        "ops:",
        "ops:nope",
        "ops:../x",
        "raw",
        "ops:cron_ticks/x",
    ] {
        assert_eq!(files_for(&fake, bad).unwrap_err().0, 400, "{bad}");
    }
    let no_param = files_page(
        &fake,
        &format!("from={}&to={}", iso_url(BASE), iso_url(BASE + 3600)),
    )
    .unwrap();
    assert!(no_param["files"][0]["key"]
        .as_str()
        .unwrap()
        .starts_with("processed/request/"));
}

#[test]
fn files_cursor_is_bound_to_its_dataset() {
    let fake = Fake::new(BASE);
    for i in 0..600 {
        put_processed(
            &fake,
            "processed/rum/",
            BASE,
            &format!("{}-{}-a-n1", BASE + i, BASE + i + 60),
        );
    }
    let first = files_for(&fake, "rum").unwrap();
    let cursor = first["next_cursor"].as_str().unwrap().to_owned();
    let query = format!(
        "from={}&to={}&dataset=ops:cron_ticks&cursor={cursor}",
        iso_url(BASE),
        iso_url(BASE + 3600)
    );
    assert_eq!(files_page(&fake, &query).unwrap_err().0, 400);
}

#[test]
fn file_reads_request_rum_and_ops_roots_only() {
    let fake = Fake::new(BASE);
    let get = |k: &str| {
        fake.new_request();
        api::file(&fake, &service(), &format!("key={k}"), None)
            .map(|upstream| fastly_log_analytics::shell::fetch_upstream(&fake, &upstream).body)
    };
    for good in [
        "processed/request/year=2027/a.parquet",
        "processed/rum/year=2027/a.parquet",
        "processed/ops/cron_ticks/year=2027/a.parquet",
    ] {
        fake.bucket.borrow_mut().insert(good.into(), vec![7]);
        assert_eq!(get(good).unwrap(), vec![7], "{good}");
        assert!(api::valid_processed_key(&service(), good));
    }
    for bad in [
        "processed/other/a.parquet",
        "processed/a.parquet",
        "processed/rum/../request/a.parquet",
        "processed/ops//a.parquet",
        "processed/ops/a.txt",
        "processed/rumx/a.parquet",
    ] {
        assert_eq!(get(bad).unwrap_err().0, 400, "{bad}");
    }
    assert!(api::is_ops_key(
        &service(),
        "processed/ops/cron_ticks/a.parquet"
    ));
    assert!(!api::is_ops_key(&service(), "processed/rum/a.parquet"));
}

#[test]
fn storage_tree_can_list_the_ops_and_rum_roots() {
    let fake = Fake::new(BASE);
    put_processed(&fake, "processed/ops/cron_ticks/", BASE, "1-2-a-n1");
    put_processed(&fake, "processed/rum/", BASE, "1-2-a-n1");
    let ops = {
        fake.new_request();
        api::tree(&fake, &service(), "dataset=ops", Instant::now(), false).unwrap()
    };
    assert_eq!(ops["root"], "processed/ops/");
    assert_eq!(ops["nodes"][0]["name"], "cron_ticks");
    fake.new_request();
    let rum = api::tree(
        &fake,
        &service(),
        "dataset=rum&prefix=year%3D2027%2F",
        Instant::now(),
        false,
    )
    .unwrap();
    assert_eq!(rum["root"], "processed/rum/");
    fake.new_request();
    assert_eq!(
        api::tree(&fake, &service(), "dataset=nope", Instant::now(), false)
            .unwrap_err()
            .0,
        400
    );
    fake.new_request();
    assert_eq!(
        api::tree(&fake, &service(), "dataset=ops", Instant::now(), true)
            .unwrap_err()
            .0,
        400
    );
}

#[test]
fn history_sweep_deletes_one_kind_up_to_its_fixed_cutoff_and_resumes() {
    let fake = Fake::new(BASE + 40 * DAY);
    let old = put_processed(&fake, "processed/ops/cron_ticks/", BASE, "1-2-a-n1");
    let mid = put_processed(
        &fake,
        "processed/ops/cron_ticks/",
        BASE + 30 * DAY,
        "1-2-a-n1",
    );
    let other = put_processed(&fake, "processed/ops/cron_runs/", BASE, "1-2-a-n1");
    let svc = service();
    let cutoff = BASE + 20 * DAY;
    fake.new_request();
    let report = ingest::retention::history_sweep(
        &store(&fake, &svc),
        LogKind::CronTicks,
        cutoff,
        &Default::default(),
        0,
    )
    .unwrap();
    assert!(!report.more_pending);
    let bucket = fake.bucket.borrow();
    assert!(!bucket.contains_key(&old));
    assert!(bucket.contains_key(&mid) && bucket.contains_key(&other));
    drop(bucket);
    assert!(fake.kv.borrow().contains_key("history:cron_ticks"));
    assert!(!fake.kv.borrow().contains_key("retention:ops:cron_ticks"));
}

#[test]
fn history_sweep_with_a_future_cutoff_clears_the_whole_kind() {
    let fake = Fake::new(BASE + 40 * DAY);
    let keys: Vec<String> = (0..3)
        .map(|h| {
            put_processed(
                &fake,
                "processed/ops/fos_usage/",
                BASE + 39 * DAY + h * 3600,
                "1-2-a-n1",
            )
        })
        .collect();
    fake.new_request();
    ingest::retention::history_sweep(
        &store(&fake, &service()),
        LogKind::FosUsage,
        BASE + 41 * DAY,
        &Default::default(),
        0,
    )
    .unwrap();
    assert!(keys.iter().all(|k| !fake.bucket.borrow().contains_key(k)));
}

fn reconcile(fake: &Fake) -> Option<ingest::retention::ReconcileReport> {
    fake.new_request();
    ingest::retention::storage_reconcile_step(&store(fake, &service()), 0).unwrap()
}

#[test]
fn storage_reconcile_does_nothing_until_requested() {
    let fake = Fake::new(BASE);
    put_processed(&fake, "processed/request/", BASE, "1-2-a-n1");
    assert!(reconcile(&fake).is_none());
    assert!(fake.kv.borrow().get("storage:reconcile").is_none());
}

#[test]
fn storage_reconcile_folds_excess_ops_kind_folders_into_other() {
    let fake = Fake::new(BASE);
    let total = ingest::retention::MAX_RECONCILE_KINDS + 5;
    for i in 0..total {
        put_processed(
            &fake,
            &format!("processed/ops/kind{i:03}/"),
            BASE,
            "1-2-a-n1",
        );
    }
    fake.new_request();
    assert!(ingest::retention::request_storage_reconcile(&fake, "admin").unwrap());
    let mut done = None;
    for _ in 0..8 {
        if let Some(snapshot) = reconcile(&fake).and_then(|r| r.snapshot) {
            done = Some(snapshot);
            break;
        }
    }
    let by_kind = done.expect("completes")["datasets"]["pq_ops"]["by_kind"].clone();
    let by_kind = by_kind.as_object().unwrap();
    assert_eq!(by_kind.len(), ingest::retention::MAX_RECONCILE_KINDS + 1);
    assert_eq!(by_kind["other"]["objects"], 5);
}

#[test]
fn storage_reconcile_persists_its_cursor_once_per_step() {
    // A KV read right after a write can still return the old generation, so a step that
    // re-read its own write to CAS again lost that CAS and stalled as `busy` after one page.
    let fake = Fake::new(BASE);
    put_processed(&fake, "processed/request/", BASE, "1-2-a-n1");
    fake.new_request();
    assert!(ingest::retention::request_storage_reconcile(&fake, "admin").unwrap());
    let generation = || fake.kv.borrow()["storage:reconcile"].0;
    let before = generation();
    fake.new_request();
    fake.calls.set(BUDGET - 8);
    let report = ingest::retention::storage_reconcile_step(&store(&fake, &service()), 0)
        .unwrap()
        .unwrap();
    assert!(report.snapshot.is_none(), "budget stops it mid-way");
    assert_eq!(generation(), before + 1);
    let state = fake.kv.borrow()["storage:reconcile"].1.clone();
    assert!(state["index"].as_u64().unwrap() >= 3, "{state}");
}

#[test]
fn storage_reconcile_sums_bytes_and_objects_per_dataset_across_resumed_steps() {
    let fake = Fake::new(BASE);
    put_processed(&fake, "processed/request/", BASE, "1-2-a-n1");
    put_processed(&fake, "processed/request/", BASE + 3600, "1-2-a-n1");
    put_processed(&fake, "processed/rum/", BASE, "1-2-a-n1");
    put_processed(&fake, "processed/ops/cron_ticks/", BASE, "1-2-a-n1");
    fake.bucket.borrow_mut().insert(
        "raw/request/year=2027/month=01/day=15/hour=08/minute=00/a.zst".into(),
        vec![0; 5],
    );
    fake.new_request();
    assert!(ingest::retention::request_storage_reconcile(&fake, "admin").unwrap());
    assert!(
        !ingest::retention::request_storage_reconcile(&fake, "admin").unwrap(),
        "a running reconcile is not restarted"
    );
    // Two LIST pages per request: each step advances the persisted cursor.
    let mut done = None;
    for _ in 0..8 {
        fake.new_request();
        fake.calls.set(BUDGET - 4);
        let report = ingest::retention::storage_reconcile_step(&store(&fake, &service()), 0)
            .unwrap()
            .unwrap();
        if let Some(snapshot) = report.snapshot {
            done = Some(snapshot);
            break;
        }
    }
    let snapshot = done.expect("completes");
    assert_eq!(
        snapshot["datasets"]["pq_request"],
        json!({"bytes": 20, "objects": 2})
    );
    assert_eq!(
        snapshot["datasets"]["pq_rum"],
        json!({"bytes": 10, "objects": 1})
    );
    assert_eq!(snapshot["datasets"]["pq_ops"]["objects"], 1);
    assert_eq!(
        snapshot["datasets"]["pq_ops"]["by_kind"]["cron_ticks"]["bytes"],
        10
    );
    assert_eq!(
        snapshot["datasets"]["raw_request"],
        json!({"bytes": 5, "objects": 1})
    );
    assert_eq!(
        snapshot["datasets"]["raw_rum"],
        json!({"bytes": 0, "objects": 0})
    );
    assert!(fake.kv.borrow().get("storage:reconcile").is_none());
    assert!(reconcile(&fake).is_none());
}

fn patch_settings(
    fake: &Fake,
    patch: Value,
) -> Result<(telemetry::OperationalLogSettings, Vec<String>), String> {
    fake.new_request();
    telemetry::api::patch_operational_settings(fake, &patch)
}

#[test]
fn operational_settings_patch_merges_validates_persists_and_reports_changes() {
    let fake = Fake::new(BASE);
    let (saved, changed) = patch_settings(
        &fake,
        json!({"flush_max_rows": 1000, "retention_days_by_kind": {"cron_ticks": 3}}),
    )
    .unwrap();
    assert_eq!(changed, vec!["flush_max_rows", "retention_days_by_kind"]);
    assert_eq!(saved.flush_max_rows, 1000);
    assert_eq!(saved.retention_days_by_kind["cron_ticks"], 3);
    assert_eq!(
        saved.retention_days_by_kind["cron_runs"], 30,
        "other kinds keep their days"
    );
    assert_eq!(telemetry::api::operational_settings(&fake), saved);
    let (_, none) = patch_settings(&fake, json!({"flush_max_rows": 1000})).unwrap();
    assert!(none.is_empty(), "an unchanged value is not reported");
    for bad in [
        json!({"nope": 1}),
        json!({"log_idle_ticks": true}),
        json!({"flush_max_rows": 10}),
        json!({"flush_max_rows": "many"}),
        json!({"retention_days_by_kind": {"fos_usage": 5}}),
        json!({"retention_days_by_kind": {"cron_ticks": 0}}),
        json!({"retention_fanout": 17}),
        json!([1]),
    ] {
        assert!(patch_settings(&fake, bad.clone()).is_err(), "{bad}");
    }
    assert_eq!(telemetry::api::operational_settings(&fake), saved);
}

#[test]
fn an_invalid_stored_settings_record_falls_back_to_defaults() {
    let fake = Fake::new(BASE);
    fake.kv.borrow_mut().insert(
        "settings:operational_logs".into(),
        (1, json!({"flush_max_rows": 1, "retention_fanout": 99})),
    );
    assert_eq!(
        telemetry::api::operational_settings(&fake),
        telemetry::OperationalLogSettings::default()
    );
}

fn logs_call(
    fake: &Fake,
    method: &str,
    path: &str,
    body: Value,
) -> (u16, Value, Option<(String, String)>) {
    fake.new_request();
    let request = fastly_log_analytics::http::Request {
        method: method.into(),
        path: path.into(),
        headers: Default::default(),
        body: if body.is_null() {
            vec![]
        } else {
            body.to_string().into_bytes()
        },
    };
    let outcome = fastly_log_analytics::admin::logs::handle(fake, "admin-1", &request);
    let body = serde_json::from_slice(&outcome.response.body).unwrap_or(Value::Null);
    (outcome.response.status, body, outcome.audit)
}

#[test]
fn logged_traffic_reports_the_compaction_setting() {
    let fake = Fake::new(BASE);
    tick_runtime(&fake);
    let (status, body, _) = logs_call(&fake, "GET", "/api/admin/usage/logged-traffic", Value::Null);
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["logging"]["compaction"],
        json!({"mode": "off", "size_target_mb": 128})
    );
}

#[test]
fn logged_traffic_reports_the_ingest_relay_setting() {
    let fake = Fake::new(BASE);
    tick_runtime(&fake);
    let (status, body, _) = logs_call(&fake, "GET", "/api/admin/usage/logged-traffic", Value::Null);
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["logging"]["ingest_relay"], json!(0));
}

#[test]
fn logs_endpoints_get_and_patch_settings_with_an_audit_note() {
    let fake = Fake::new(BASE);
    let (status, body, audit) =
        logs_call(&fake, "GET", "/api/admin/operational-logging", Value::Null);
    assert_eq!(status, 200);
    assert_eq!(body["settings"]["flush_max_rows"], 500);
    assert_eq!(body["defaults"]["flush_max_rows"], 500);
    assert!(audit.is_none());
    let (status, body, audit) = logs_call(
        &fake,
        "PATCH",
        "/api/admin/operational-logging",
        json!({"flush_max_rows": 750, "compaction_enabled": false}),
    );
    assert_eq!(status, 200);
    assert_eq!(body["settings"]["flush_max_rows"], 750);
    assert_eq!(
        body["changed"],
        json!(["compaction_enabled", "flush_max_rows"])
    );
    assert_eq!(
        audit,
        Some((
            "operational_logging_updated".into(),
            "compaction_enabled,flush_max_rows".into()
        ))
    );
    let (status, body, audit) = logs_call(
        &fake,
        "PATCH",
        "/api/admin/operational-logging",
        json!({"flush_max_rows": 1}),
    );
    assert_eq!((status, body["detail"].as_str().is_some()), (400, true));
    assert!(
        audit.is_none(),
        "a rejected patch is not audited as applied"
    );
    assert_eq!(
        logs_call(&fake, "PUT", "/api/admin/operational-logging", json!({})).0,
        404
    );
}

#[test]
fn logs_clear_cache_accepts_a_best_effort_kind_or_all_and_rejects_durable_kinds() {
    let fake = Fake::new(BASE);
    for bad in [
        json!({}),
        json!({"kind": "admin_audit"}),
        json!({"kind": "cron_runs"}),
        json!({"kind": "cron_ticks"}),
        json!({"kind": "nope"}),
        json!({"kind": 3}),
    ] {
        let (status, _, audit) =
            logs_call(&fake, "POST", "/api/admin/logs/clear-cache", bad.clone());
        assert_eq!(status, 400, "{bad}");
        assert!(audit.is_none());
    }
}

fn history_service(fake: &Fake) {
    tick_runtime(fake);
}

#[test]
fn delete_history_runs_as_a_resumable_job_ending_in_one_front_purge() {
    let fake = Fake::new(BASE + 40 * DAY);
    history_service(&fake);
    let old = put_processed(&fake, "processed/ops/cron_ticks/", BASE, "1-2-a-n1");
    let new = put_processed(
        &fake,
        "processed/ops/cron_ticks/",
        BASE + 39 * DAY,
        "1-2-a-n1",
    );
    let other = put_processed(&fake, "processed/ops/cron_runs/", BASE, "1-2-a-n1");
    let (status, body, audit) = logs_call(
        &fake,
        "POST",
        "/api/admin/logs/delete-history",
        json!({"kind": "cron_ticks", "older_than_days": 10}),
    );
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["job"]["status"], "complete");
    assert_eq!(
        audit,
        Some((
            "log_history_delete_started".into(),
            "cron_ticks:older_than_days=10".into()
        ))
    );
    let bucket = fake.bucket.borrow();
    assert!(!bucket.contains_key(&old) && bucket.contains_key(&new) && bucket.contains_key(&other));
    drop(bucket);
    assert_eq!(
        *fake.api_calls.borrow(),
        vec![(
            "POST".to_string(),
            "/service/v/purge/flp-fos-ops-cron_ticks".to_string()
        )]
    );
    let (_, again, _) = logs_call(
        &fake,
        "POST",
        "/api/admin/logs/delete-history",
        json!({"kind": "cron_ticks"}),
    );
    assert_eq!(
        again["job"]["status"], "complete",
        "a finished job can be replaced"
    );
    assert!(
        !fake.bucket.borrow().contains_key(&new),
        "no older_than_days clears everything"
    );
    for bad in [
        json!({}),
        json!({"kind": "nope"}),
        json!({"kind": "cron_ticks", "older_than_days": -1}),
        json!({"kind": "cron_ticks", "older_than_days": 99999}),
    ] {
        assert_eq!(
            logs_call(&fake, "POST", "/api/admin/logs/delete-history", bad).0,
            400
        );
    }
}

#[test]
fn delete_history_refuses_a_second_job_and_resumes_from_the_tick() {
    let fake = Fake::new(BASE + 40 * DAY);
    history_service(&fake);
    let seeded: Vec<String> = (0..3)
        .map(|h| {
            put_processed(
                &fake,
                "processed/ops/cron_ticks/",
                BASE + h * 3600,
                "1-2-a-n1",
            )
        })
        .collect();
    // A starved request records the job but cannot finish it.
    fake.new_request();
    fake.calls.set(BUDGET - 10);
    let request = fastly_log_analytics::http::Request {
        method: "POST".into(),
        path: "/api/admin/logs/delete-history".into(),
        headers: Default::default(),
        body: json!({"kind": "cron_ticks"}).to_string().into_bytes(),
    };
    let first = fastly_log_analytics::admin::logs::handle(&fake, "a", &request);
    let body: Value = serde_json::from_slice(&first.response.body).unwrap();
    assert_ne!(body["job"]["status"], "complete");
    let (status, other, _) = logs_call(
        &fake,
        "POST",
        "/api/admin/logs/delete-history",
        json!({"kind": "cron_runs"}),
    );
    assert_eq!(
        (status, other["detail"].clone()),
        (409, json!("history_job_in_progress"))
    );
    for _ in 0..16 {
        run_tick(&fake);
        fake.now.set(fake.now.get() + 120);
    }
    assert!(seeded.iter().all(|k| !fake.bucket.borrow().contains_key(k)));
    let (_, status_body, _) = logs_call(&fake, "GET", "/api/admin/logs/status", Value::Null);
    assert_eq!(status_body["delete_history"]["status"], "complete");
    // The retention sweep also expires these and purges the whole service once.
    let purges = fake.api_calls.borrow().clone();
    assert_eq!(
        purges
            .iter()
            .filter(|(_, p)| p.contains("/purge/flp-fos-ops-"))
            .count(),
        1
    );
    assert!(purges.len() <= 2);
}

#[test]
fn sweep_and_reconcile_endpoints_run_on_demand_and_are_audited() {
    let fake = Fake::new(BASE + 40 * DAY);
    history_service(&fake);
    let old = put_processed(&fake, "processed/request/", BASE, "1-2-a-n1");
    for idx in 0..25 {
        put_processed(&fake, "processed/request/", BASE, &format!("1-2-b-n{idx}"));
    }
    let (status, body, audit) = logs_call(&fake, "POST", "/api/admin/logs/sweep", Value::Null);
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        audit,
        Some(("log_retention_sweep_requested".into(), "retention".into()))
    );
    assert!(body["deleted"].as_u64().unwrap_or(0) >= 1, "{body}");
    assert!(!fake.bucket.borrow().contains_key(&old));
    assert!(
        fake.kv.borrow().contains_key("retention:last_sweep"),
        "sweep must reserve budget to persist retention:last_sweep"
    );
    assert!(
        fastly_log_analytics::shell::Runtime::admin_remaining_calls(&fake) >= 6,
        "sweep must reserve >= 6 calls for ProjectedStore::record_admin_action"
    );
    let mut completed = body["more_pending"] != true;
    for _ in 0..8 {
        if completed {
            break;
        }
        let (s, next_body, _) = logs_call(&fake, "POST", "/api/admin/logs/sweep", Value::Null);
        assert_eq!(s, 200, "{next_body}");
        assert!(
            fastly_log_analytics::shell::Runtime::admin_remaining_calls(&fake) >= 6,
            "resumed sweep must also reserve >= 6 calls for ProjectedStore::record_admin_action"
        );
        completed = next_body["more_pending"] != true;
    }
    assert!(
        completed,
        "manual sweep must resume and finish across calls"
    );
    let (status, body, audit) = logs_call(
        &fake,
        "POST",
        "/api/admin/usage/reconcile-storage",
        Value::Null,
    );
    assert_eq!((status, body["started"].clone()), (200, json!(true)));
    assert_eq!(
        audit,
        Some(("storage_reconcile_requested".into(), "storage".into()))
    );
    let (_, again, _) = logs_call(
        &fake,
        "POST",
        "/api/admin/usage/reconcile-storage",
        Value::Null,
    );
    assert_eq!(again["started"], false);
    for _ in 0..6 {
        run_tick(&fake);
        fake.now.set(fake.now.get() + 120);
    }
    let runs = rows(&fake, LogKind::CronRuns);
    let reconcile: Vec<_> = runs
        .iter()
        .filter(|r| r["cron_kind"] == "storage_reconcile" && r["outcome"] == "done")
        .collect();
    assert_eq!(reconcile.len(), 1);
    let details: Value =
        serde_json::from_str(reconcile[0]["details_json"].as_str().unwrap()).unwrap();
    assert_eq!(details["datasets"]["pq_request"]["objects"], 0, "{details}");
    assert!(fake.kv.borrow().get("storage:reconcile").is_none());
}

#[test]
fn ticks_delete_converted_raw_objects_and_sweep_every_six_hours() {
    let fake = Fake::new(BASE + 40 * DAY);
    tick_runtime(&fake);
    let done: serde_json::Map<String, Value> = LogKind::ALL
        .iter()
        .map(|k| (k.as_str().to_owned(), json!((BASE + 40 * DAY) / DAY)))
        .collect();
    fake.kv
        .borrow_mut()
        .insert("logs:compact_state".into(), (1, json!({"done": done})));
    let raws: Vec<String> = (0..2)
        .map(|m| {
            put_raw(
                &fake,
                BASE + 40 * DAY - 3600 + m * 60,
                "a",
                &[line(BASE + 40 * DAY - 3600 + m * 60, "a")],
            )
        })
        .collect();
    let old = put_processed(&fake, "processed/request/", BASE, "1-2-a-n1");
    let first = freshness::finish_tick(&fake);
    for _ in 0..16 {
        fake.now.set(fake.now.get() + 120);
        run_tick(&fake);
    }
    assert!(raws.iter().all(|k| !fake.bucket.borrow().contains_key(k)));
    assert_eq!(
        first["runs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|run| run["deleted_raw_files"].as_u64().unwrap())
            .sum::<u64>(),
        2
    );
    let runs = rows(&fake, LogKind::CronRuns);
    let del: Vec<_> = runs
        .iter()
        .filter(|r| r["cron_kind"] == "raw_delete")
        .collect();
    assert_eq!(del.len(), 1);
    assert_eq!(del[0]["deleted_files"], 2);
    let sweeps: Vec<_> = runs
        .iter()
        .filter(|r| r["cron_kind"] == "retention_sweep")
        .collect();
    assert!(
        !sweeps.is_empty(),
        "a sweep spanning ticks reports each leg"
    );
    assert!(
        sweeps
            .iter()
            .map(|r| r["deleted_files"].as_i64().unwrap())
            .sum::<i64>()
            >= 1
    );
    assert!(!fake.bucket.borrow().contains_key(&old));
    let marker = || fake.kv.borrow()["retention:last_sweep"].1.clone();
    assert_eq!(marker()["complete"], true);
    let swept_at = marker()["at"].as_u64().unwrap();
    fake.now.set(fake.now.get() + 120);
    run_tick(&fake);
    assert_eq!(
        marker()["at"].as_u64().unwrap(),
        swept_at,
        "not due again for six hours"
    );
    fake.now.set(fake.now.get() + 6 * 3600);
    for _ in 0..4 {
        run_tick(&fake);
        fake.now.set(fake.now.get() + 120);
    }
    assert!(marker()["at"].as_u64().unwrap() >= swept_at + 6 * 3600);
}

mod compaction {
    use super::*;
    use fastly_log_analytics::telemetry::compact::{
        classify, compact_key, log_compact, CompactReport,
    };

    const KIND: &str = "api_telemetry";

    fn day_start() -> i64 {
        BASE - 8 * 3600 - DAY
    }

    fn day_dir(day_start: i64) -> String {
        ingest::hour_dir(day_start)
            .rsplit_once("hour=")
            .unwrap()
            .0
            .to_owned()
    }

    fn ops_prefix(kind: &str) -> String {
        format!("processed/ops/{kind}/")
    }

    fn row(ts_ms: i64, id: &str) -> Value {
        json!({"timestamp": ts_ms, "event_id": id, "service_id": SID, "severity": "info",
               "method": "GET", "status_code": 200})
    }

    fn start_min(index: i64) -> i64 {
        day_start() / 60 + index * 5
    }

    fn window_key(prefix: &str, index: i64) -> String {
        let start = start_min(index);
        let hour_ts = (start * 60).div_euclid(3600) * 3600;
        format!(
            "{prefix}{}window={start}-{}-t{index:03}-n3.parquet",
            ingest::hour_dir(hour_ts),
            start + 1
        )
    }

    fn put_window(fake: &Fake, kind: &str, index: i64) -> String {
        let key = window_key(&ops_prefix(kind), index);
        let ts = start_min(index) * 60_000;
        let rows = [
            row(ts, &format!("w{index}-a")),
            row(ts + 1000, &format!("w{index}-b")),
            row(ts + 2000, "dup"),
        ];
        let parsed = LogKind::parse(kind).unwrap();
        let bytes = telemetry::schema::to_parquet(parsed, &rows).unwrap();
        fake.bucket.borrow_mut().insert(key.clone(), bytes);
        key
    }

    fn seed_windows(fake: &Fake, count: i64) -> Vec<String> {
        (0..count).map(|i| put_window(fake, KIND, i)).collect()
    }

    fn seed_state(fake: &Fake) {
        let today = day_start().div_euclid(DAY);
        let map: serde_json::Map<String, Value> = LogKind::ALL
            .iter()
            .map(|k| {
                let done = if k.as_str() == KIND { today - 1 } else { today };
                (k.as_str().to_owned(), json!(done))
            })
            .collect();
        let mut checked = serde_json::Map::new();
        for kind in LogKind::ALL.iter().filter(|k| k.as_str() != KIND) {
            for day in today - 2..=today {
                let t = time::OffsetDateTime::from_unix_timestamp(day * DAY).unwrap();
                checked.insert(
                    format!(
                        "{}:{:04}-{:02}-{:02}",
                        kind.as_str(),
                        t.year(),
                        t.month() as u8,
                        t.day()
                    ),
                    json!(BASE),
                );
            }
        }
        fake.kv.borrow_mut().insert(
            "logs:compact_state".into(),
            (1, json!({"done": map, "checked": checked})),
        );
    }

    fn compact(fake: &Fake, reserve: u32) -> CompactReport {
        compact_with(fake, &Default::default(), reserve)
    }

    fn compact_with(
        fake: &Fake,
        settings: &telemetry::OperationalLogSettings,
        reserve: u32,
    ) -> CompactReport {
        fake.new_request();
        log_compact(&store(fake, &service()), "ck", settings, reserve).unwrap()
    }

    fn compacts(fake: &Fake, kind: &str) -> Vec<String> {
        fake.keys(&ops_prefix(kind))
            .into_iter()
            .filter(|k| k.contains("/compact="))
            .collect()
    }

    fn queue_key() -> String {
        let t = time::OffsetDateTime::from_unix_timestamp(day_start()).unwrap();
        format!(
            "logs:compact_delete:{KIND}:{:04}-{:02}-{:02}",
            t.year(),
            t.month() as u8,
            t.day()
        )
    }

    fn decode(fake: &Fake, key: &str) -> Vec<Value> {
        let bytes = fake.bucket.borrow()[key].clone();
        telemetry::schema::from_parquet(LogKind::ApiTelemetry, bytes).unwrap()
    }

    fn settle(fake: &Fake) {
        for _ in 0..30 {
            let report = compact(fake, 3);
            if !report.more_pending && report.chunks == 0 {
                return;
            }
        }
        panic!("compaction did not settle");
    }

    #[test]
    fn closed_day_is_merged_in_chunks_of_at_most_24_and_deduplicated() {
        let fake = Fake::new(BASE);
        seed_state(&fake);
        seed_windows(&fake, 50);
        settle(&fake);
        let dir = format!("{}{}", ops_prefix(KIND), day_dir(day_start()));
        let found = compacts(&fake, KIND);
        let shape = |i: usize, a: i64, b: i64, tail: &str| {
            let name = found[i].strip_prefix(&dir).unwrap();
            assert!(
                name.starts_with(&format!("compact={a}-{b}-{tail}-")),
                "{name}"
            );
        };
        assert_eq!(found.len(), 3);
        shape(0, start_min(0), start_min(23), "c24-n49");
        shape(1, start_min(24), start_min(47), "c24-n49");
        shape(2, start_min(48), start_min(49), "c2-n5");
        let expected = found.clone();
        let rows = decode(&fake, &expected[0]);
        let ids: BTreeSet<_> = rows
            .iter()
            .map(|r| r["event_id"].as_str().unwrap())
            .collect();
        assert_eq!((rows.len(), ids.len()), (49, 49));
        let remaining: Vec<_> = fake
            .keys(&ops_prefix(KIND))
            .into_iter()
            .filter(|k| k.contains("/window="))
            .collect();
        assert!(
            remaining.is_empty(),
            "sources are deleted after the commits"
        );
    }

    fn dir() -> &'static str {
        "processed/ops/api_telemetry/year=2027/month=01/day=14/"
    }

    fn w(hour: u32, start: u64) -> String {
        format!(
            "{}hour={hour:02}/window={start}-{}-x-n1.parquet",
            dir(),
            start + 1
        )
    }

    #[test]
    fn coverage_is_the_recorded_source_set_and_a_late_window_in_range_is_still_read() {
        let sources = [w(0, 100), w(0, 150), w(1, 200)];
        let c = compact_key(dir(), 100, 200, &sources, 9);
        let late = w(0, 120);
        let mut keys = sources.to_vec();
        keys.extend([late.clone(), w(1, 201), w(2, 99), c.clone()]);
        let listing = classify(&keys);
        assert_eq!(listing.valid, vec![c]);
        assert!(listing.ignored.is_empty());
        assert_eq!(listing.covered, sources.to_vec());
        assert_eq!(
            listing.read,
            vec![late, w(1, 201), w(2, 99)],
            "only recorded sources are skipped"
        );
    }

    #[test]
    fn partial_source_deletes_never_hide_rows_or_invalidate_the_compact() {
        let sources = [w(0, 100), w(0, 150), w(1, 200)];
        let c = compact_key(dir(), 100, 200, &sources, 9);
        let partial = classify(&[w(0, 150), c.clone()]);
        assert_eq!(partial.valid, vec![c.clone()]);
        assert_eq!(partial.covered, vec![w(0, 150)]);
        assert!(partial.read.is_empty() && partial.ignored.is_empty());
        let after_delete = classify(std::slice::from_ref(&c));
        assert_eq!(after_delete.valid, vec![c]);
    }

    #[test]
    fn a_legacy_compact_name_without_a_source_set_keeps_the_range_rule() {
        let c = format!("{}compact=100-200-c3-n9.parquet", dir());
        let partial = classify(&[w(0, 100), w(0, 150), c.clone()]);
        assert_eq!(partial.ignored, vec![c.clone()]);
        assert_eq!(partial.read, vec![w(0, 100), w(0, 150)]);
        let full = classify(&[w(0, 100), w(0, 150), w(1, 200), c.clone()]);
        assert!(full.read.is_empty() && full.valid == vec![c]);
    }

    #[test]
    fn one_chunk_is_written_when_only_one_fits_and_the_next_tick_continues() {
        let fake = Fake::new(BASE);
        seed_state(&fake);
        let sources = seed_windows(&fake, 48);
        let first = compact(&fake, 3);
        assert_eq!((first.chunks, first.sources), (1, 24));
        assert!(first.more_pending);
        assert_eq!(compacts(&fake, KIND).len(), 1);
        let listing = classify(&fake.keys(&ops_prefix(KIND)));
        assert_eq!(
            listing.read,
            sources[24..].to_vec(),
            "uncovered windows stay readable"
        );
        let second = compact(&fake, 3);
        assert_eq!(second.chunks, 1);
        assert_eq!(compacts(&fake, KIND).len(), 2);
    }

    #[test]
    fn a_chunk_is_sized_to_the_calls_that_fit_and_never_exceeds_them() {
        let fake = Fake::new(BASE);
        seed_state(&fake);
        seed_windows(&fake, 24);
        // state read, list and queue read leave 29; reserve 21 leaves room for 6 GETs, a PUT and the insert.
        let small = compact(&fake, 21);
        assert_eq!((small.chunks, small.sources), (1, 6));
        assert!(small.more_pending);
        let none = compact(&fake, 30);
        assert_eq!(none.chunks, 0);
        assert!(none.more_pending);
        assert!(fake.calls.get() <= BUDGET);
    }

    #[test]
    fn sources_are_queued_after_the_commit_and_deleted_on_a_later_tick() {
        let fake = Fake::new(BASE);
        seed_state(&fake);
        let sources = seed_windows(&fake, 24);
        compact(&fake, 3);
        assert_eq!(compacts(&fake, KIND).len(), 1);
        assert!(sources.iter().all(|k| fake.bucket.borrow().contains_key(k)));
        let queued = fake.kv.borrow()[&queue_key()].1["keys"].clone();
        assert_eq!(queued.as_array().unwrap().len(), 24);
        assert!(fake.delete_posts.borrow().is_empty());
        let later = compact(&fake, 3);
        assert_eq!(later.deleted, 24);
        assert_eq!(*fake.delete_posts.borrow(), vec![24]);
        assert!(sources
            .iter()
            .all(|k| !fake.bucket.borrow().contains_key(k)));
        assert!(!fake.kv.borrow().contains_key(&queue_key()));
        assert_eq!(compacts(&fake, KIND).len(), 1);
    }

    #[test]
    fn a_failed_commit_queues_and_deletes_nothing() {
        let fake = Fake::new(BASE);
        seed_state(&fake);
        let sources = seed_windows(&fake, 5);
        fake.fail_put_number.set(Some(1));
        let report = compact(&fake, 3);
        assert_eq!(report.chunks, 0);
        assert!(!report.errors.is_empty());
        assert!(!fake.kv.borrow().contains_key(&queue_key()));
        assert!(compacts(&fake, KIND).is_empty());
        assert!(sources.iter().all(|k| fake.bucket.borrow().contains_key(k)));
        assert_eq!(compact(&fake, 3).chunks, 1, "the next tick retries");
    }

    #[test]
    fn request_and_rum_files_are_never_touched() {
        let fake = Fake::new(BASE);
        seed_state(&fake);
        seed_windows(&fake, 3);
        let mut guarded = Vec::new();
        for root in ["processed/request/", "processed/rum/"] {
            for index in 0..3 {
                let key = window_key(root, index);
                fake.bucket.borrow_mut().insert(key.clone(), vec![1; 10]);
                guarded.push(key);
            }
        }
        settle(&fake);
        let bucket = fake.bucket.borrow();
        assert!(guarded.iter().all(|k| bucket.get(k) == Some(&vec![1; 10])));
        assert!(!bucket
            .keys()
            .any(|k| !k.starts_with("processed/ops/") && k.contains("compact=")));
    }

    #[test]
    fn rerunning_produces_the_same_keys_and_bytes_and_repairs_a_lost_queue() {
        let run = || {
            let fake = Fake::new(BASE);
            seed_state(&fake);
            seed_windows(&fake, 24);
            compact(&fake, 3);
            fake
        };
        let (a, b) = (run(), run());
        assert_eq!(compacts(&a, KIND), compacts(&b, KIND));
        let key = &compacts(&a, KIND)[0];
        assert_eq!(a.bucket.borrow()[key], b.bucket.borrow()[key]);

        // Crash between the PUT and the queue insert: no queue, state not saved.
        a.kv.borrow_mut().remove(&queue_key());
        a.kv.borrow_mut().remove("logs:compact_state");
        seed_state(&a);
        compact(&a, 3);
        assert_eq!(compacts(&a, KIND).len(), 1, "no second compact file");
        compact(&a, 3);
        assert!(!a.bucket.borrow().keys().any(|k| k.contains("/window=")));
    }

    #[test]
    fn a_compact_with_the_wrong_row_count_never_causes_deletes() {
        let fake = Fake::new(BASE);
        seed_state(&fake);
        let sources = seed_windows(&fake, 3);
        let bad = format!(
            "{}{}compact={}-{}-c3-n99.parquet",
            ops_prefix(KIND),
            day_dir(day_start()),
            start_min(0),
            start_min(2)
        );
        let one = telemetry::schema::to_parquet(LogKind::ApiTelemetry, &[row(1, "x")]).unwrap();
        fake.bucket.borrow_mut().insert(bad, one);
        compact(&fake, 3);
        compact(&fake, 3);
        assert!(!fake.kv.borrow().contains_key(&queue_key()));
        assert!(sources.iter().all(|k| fake.bucket.borrow().contains_key(k)));
    }

    #[test]
    fn disabled_setting_makes_no_calls() {
        let fake = Fake::new(BASE);
        seed_windows(&fake, 5);
        let settings = telemetry::OperationalLogSettings {
            compaction_enabled: false,
            ..Default::default()
        };
        let report = compact_with(&fake, &settings, 3);
        assert_eq!((fake.calls.get(), report.chunks), (0, 0));
    }

    fn listed_files(fake: &Fake, from: i64, to: i64) -> Vec<Value> {
        let base = format!(
            "from={}&to={}&dataset=ops:{KIND}",
            iso_url(from),
            iso_url(to)
        );
        let (mut cursor, mut files) = (None::<String>, Vec::new());
        for _ in 0..10 {
            let query = match &cursor {
                Some(c) => format!("{base}&cursor={c}"),
                None => base.clone(),
            };
            let page = files_page(fake, &query).unwrap();
            files.extend(page["files"].as_array().unwrap().iter().cloned());
            match page["next_cursor"].as_str() {
                Some(next) => cursor = Some(next.to_owned()),
                None => return files,
            }
        }
        panic!("files did not complete");
    }

    fn keys_of(files: &[Value]) -> Vec<String> {
        let mut keys: Vec<String> = files
            .iter()
            .map(|f| f["key"].as_str().unwrap().to_owned())
            .collect();
        keys.sort();
        keys
    }

    #[test]
    fn files_lists_each_compact_once_in_place_of_its_sources_and_keeps_late_windows() {
        let fake = Fake::new(BASE);
        seed_state(&fake);
        let sources = seed_windows(&fake, 30);
        assert_eq!(compact(&fake, 3).chunks, 1);
        let first = compacts(&fake, KIND);
        assert!(sources[..24]
            .iter()
            .all(|k| fake.bucket.borrow().contains_key(k)));
        // `from` lies past hour 00, so an hour-prefix start-after would skip the day's compacts.
        let (from, to) = (day_start() + 5400, day_start() + 3 * 3600);
        let files = listed_files(&fake, from, to);
        let mut expected = first.clone();
        expected.extend(sources[24..].iter().cloned());
        expected.sort();
        assert_eq!(keys_of(&files), expected, "pending sources stay hidden");
        let entry = files.iter().find(|f| f["key"] == first[0]).unwrap();
        assert_eq!(entry["start"], ingest::rfc3339(start_min(0) * 60));
        assert_eq!(entry["end"], ingest::rfc3339(day_start() + 2 * 3600));
        assert_eq!(
            entry["span"],
            json!([
                ingest::rfc3339(start_min(0) * 60),
                ingest::rfc3339(start_min(23) * 60)
            ])
        );
        assert_eq!(
            (entry["rows"].clone(), entry["sources"].clone()),
            (json!(49), json!(24))
        );

        settle(&fake);
        let late = window_key(&ops_prefix(KIND), 25).replace("-t025-", "-late-");
        fake.bucket.borrow_mut().insert(late.clone(), vec![1]);
        let all = compacts(&fake, KIND);
        assert_eq!(all.len(), 2);
        let mut expected = all.clone();
        expected.push(late);
        expected.sort();
        assert_eq!(keys_of(&listed_files(&fake, from, to)), expected);
    }

    #[test]
    fn files_hides_sources_listed_on_a_later_page_than_their_compact() {
        let fake = Fake::new(BASE);
        let prefix = format!("{}{}", ops_prefix(KIND), day_dir(day_start()));
        let first = day_start() / 60;
        let filler: Vec<String> = (0..api::FILES_PAGE as i64 + 20)
            .map(|i| format!("{prefix}hour=00/window={first}-{first}-f{i:04}-n1.parquet"))
            .collect();
        let late = day_start() / 60 + 23 * 60;
        let sources: Vec<String> = (0..3)
            .map(|i| {
                format!(
                    "{prefix}hour=23/window={}-{}-s{i}-n1.parquet",
                    late + i,
                    late + i
                )
            })
            .collect();
        let compact = compact_key(&prefix, late as u64, late as u64 + 2, &sources, 3);
        for key in filler.iter().chain(&sources).chain([&compact]) {
            fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
        }
        let mut expected = filler.clone();
        expected.push(compact);
        expected.sort();
        assert_eq!(
            keys_of(&listed_files(&fake, day_start(), day_start() + DAY)),
            expected
        );
    }

    fn ingest_row(id: &str, raw: &str, ts_s: i64) -> Value {
        json!({"timestamp": ts_s * 1000, "event_id": id, "service_id": SID,
               "severity": "info", "run_id": "r", "dataset": "request", "raw_key": raw,
               "raw_size": 9, "status": "converted", "state_version": 1, "rows": 2,
               "raw_deleted": false})
    }

    fn put_ingest(fake: &Fake, key: &str, rows: &[Value]) {
        let bytes = telemetry::schema::to_parquet(LogKind::IngestFiles, rows).unwrap();
        fake.bucket.borrow_mut().insert(key.to_owned(), bytes);
    }

    #[test]
    fn ingested_files_reads_compacts_instead_of_their_sources_and_keeps_late_windows() {
        let fake = Fake::new(BASE);
        let prefix = format!("processed/ops/ingest_files/{}", day_dir(day_start()));
        let minute = day_start() / 60;
        let window = |hour: i64, tag: &str| {
            let m = minute + hour * 60;
            format!("{prefix}hour={hour:02}/window={m}-{m}-{tag}-n1.parquet")
        };
        let raws: Vec<String> = (0..3)
            .map(|i| raw_key(day_start() + i * 3600, &format!("r{i}")))
            .collect();
        let rows: Vec<Value> = (0..3)
            .map(|i| ingest_row(&format!("e{i}"), &raws[i as usize], day_start() + i * 3600))
            .collect();
        let sources = [window(0, "a"), window(1, "b")];
        for (key, row) in sources.iter().zip(&rows) {
            put_ingest(&fake, key, std::slice::from_ref(row));
        }
        let compact = compact_key(&prefix, minute as u64, minute as u64 + 60, &sources, 2);
        put_ingest(&fake, &compact, &rows[..2]);
        let late = window(2, "c");
        put_ingest(&fake, &late, &rows[2..]);

        let page = admin_page(&fake, None, "").unwrap();
        let mut keys: Vec<&str> = page["files"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["key"].as_str().unwrap())
            .collect();
        keys.sort();
        let mut want: Vec<&str> = raws.iter().map(String::as_str).collect();
        want.sort();
        assert_eq!(keys, want, "every raw file once");
        assert_eq!(page["skipped_objects"], 0);
        let read: Vec<String> = fake
            .cdn_gets
            .borrow()
            .iter()
            .filter(|k| k.starts_with("processed/ops/ingest_files/"))
            .cloned()
            .collect();
        assert_eq!(read, vec![compact.clone(), late.clone()]);

        for key in &sources {
            fake.bucket.borrow_mut().remove(key);
        }
        let page = admin_page(&fake, None, "").unwrap();
        assert_eq!(page["files"].as_array().unwrap().len(), 3);
        assert_eq!(page["skipped_objects"], 0);
    }

    #[test]
    fn a_day_is_closed_two_hours_after_midnight_and_a_fresh_install_scans_back() {
        let fake = Fake::new(BASE - 7 * 3600);
        seed_windows(&fake, 5);
        for _ in 0..12 {
            compact(&fake, 3);
        }
        assert!(
            compacts(&fake, KIND).is_empty(),
            "01:00 UTC is inside the grace period"
        );
        fake.now.set(BASE as u64);
        for _ in 0..12 {
            compact(&fake, 3);
        }
        assert_eq!(
            compacts(&fake, KIND).len(),
            1,
            "no saved state: scans the lookback window"
        );
    }

    #[test]
    fn retention_removes_a_compact_file_with_its_expired_day() {
        let fake = Fake::new(BASE);
        seed_state(&fake);
        seed_windows(&fake, 24);
        settle(&fake);
        assert_eq!(compacts(&fake, KIND).len(), 1);
        fake.now.set((BASE + 40 * DAY) as u64);
        let mut settings = telemetry::OperationalLogSettings::default();
        settings.retention_days_by_kind.insert(KIND.into(), 14);
        sweep(&fake, &service(), &settings);
        assert!(compacts(&fake, KIND).is_empty());
    }

    fn put_late(fake: &Fake, start: i64, tag: &str) -> String {
        let key = format!(
            "{}{}hour=00/window={start}-{}-late{tag}-n1.parquet",
            ops_prefix(KIND),
            day_dir(day_start()),
            start + 1
        );
        let bytes = telemetry::schema::to_parquet(
            LogKind::ApiTelemetry,
            &[row(start * 60_000, &format!("late-{tag}"))],
        )
        .unwrap();
        fake.bucket.borrow_mut().insert(key.clone(), bytes);
        key
    }

    #[test]
    fn late_windows_inside_or_after_a_compact_are_compacted_on_a_later_revisit() {
        let fake = Fake::new(BASE);
        seed_state(&fake);
        seed_windows(&fake, 6);
        settle(&fake);
        let first = compacts(&fake, KIND);
        assert_eq!(first.len(), 1);
        let inside = put_late(&fake, start_min(2), "a");
        let after = put_late(&fake, start_min(5) + 2, "b");
        settle(&fake);
        assert_eq!(compacts(&fake, KIND), first, "not revisited immediately");
        let listing = classify(&fake.keys(&ops_prefix(KIND)));
        assert_eq!(listing.valid, first);
        assert_eq!(listing.read, vec![inside.clone(), after.clone()]);
        fake.now.set(fake.now.get() + 7 * 3600);
        settle(&fake);
        let all = compacts(&fake, KIND);
        assert_eq!(all.len(), 2);
        assert!(all.contains(&first[0]));
        let late: Vec<_> = all.iter().filter(|k| **k != first[0]).collect();
        let ids: BTreeSet<String> = decode(&fake, late[0])
            .iter()
            .map(|r| r["event_id"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(
            ids,
            BTreeSet::from(["late-a".to_owned(), "late-b".to_owned()])
        );
        assert!(!fake.bucket.borrow().contains_key(&inside));
        assert!(!fake.bucket.borrow().contains_key(&after));
    }

    #[test]
    fn the_delete_queue_only_removes_this_services_ops_window_and_compact_keys() {
        let fake = Fake::new(BASE);
        seed_state(&fake);
        let good = put_window(&fake, KIND, 0);
        let other_kind = format!(
            "processed/ops/cron_runs/{}",
            &good[good.find("year=").unwrap()..]
        );
        let foreign = [
            "raw/request/x.gz".to_owned(),
            "processed/request/year=2027/month=01/day=14/hour=00/window=1-2-a-n1.parquet".to_owned(),
            "other-scope/processed/ops/api_telemetry/year=2027/month=01/day=14/hour=00/window=1-2-a-n1.parquet".to_owned(),
            format!("{}../x.parquet", ops_prefix(KIND)),
            format!("{}notes.txt", ops_prefix(KIND)),
            other_kind,
        ];
        for key in &foreign {
            fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
        }
        let mut keys = vec![good.clone()];
        keys.extend(foreign.iter().cloned());
        fake.kv.borrow_mut().insert(
            queue_key(),
            (
                1,
                json!({"keys": keys, "sizes": vec![1; keys.len()], "at": 1}),
            ),
        );
        let mut state = json!({"done": {}, "pending": [format!("{KIND}:{}", queue_key().rsplit(':').next().unwrap())]});
        state["done"] = fake.kv.borrow()["logs:compact_state"].1["done"].clone();
        fake.kv
            .borrow_mut()
            .insert("logs:compact_state".into(), (2, state));
        let report = compact(&fake, 3);
        assert_eq!(report.deleted, 1);
        assert!(!fake.bucket.borrow().contains_key(&good));
        for key in &foreign {
            assert!(fake.bucket.borrow().contains_key(key), "{key} was deleted");
        }
        assert!(!fake.kv.borrow().contains_key(&queue_key()));
    }

    fn put_declared(fake: &Fake, index: i64, declared_rows: u64) -> String {
        let start = start_min(index);
        let key = format!(
            "{}{}hour=00/window={start}-{}-big{index:03}-n{declared_rows}.parquet",
            ops_prefix(KIND),
            day_dir(day_start()),
            start + 1
        );
        let bytes = telemetry::schema::to_parquet(
            LogKind::ApiTelemetry,
            &[row(start * 60_000, &format!("big-{index}"))],
        )
        .unwrap();
        fake.bucket.borrow_mut().insert(key.clone(), bytes);
        key
    }

    #[test]
    fn a_large_day_is_split_by_the_row_cap_across_ticks_and_loses_nothing() {
        let fake = Fake::new(BASE);
        seed_state(&fake);
        let sources: Vec<String> = (0..6).map(|i| put_declared(&fake, i, 4000)).collect();
        let first = compact(&fake, 22);
        assert_eq!((first.chunks, first.sources), (1, 2));
        assert!(first.more_pending);
        settle(&fake);
        assert_eq!(compacts(&fake, KIND).len(), 3);
        let ids: BTreeSet<String> = compacts(&fake, KIND)
            .iter()
            .flat_map(|k| decode(&fake, k))
            .map(|r| r["event_id"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(ids.len(), 6);
        assert!(sources
            .iter()
            .all(|k| !fake.bucket.borrow().contains_key(k)));
    }

    #[test]
    fn a_window_over_the_row_cap_stays_loose_and_readable() {
        let fake = Fake::new(BASE);
        seed_state(&fake);
        let huge = put_declared(&fake, 0, 20_000);
        let rest: Vec<String> = (1..4).map(|i| put_declared(&fake, i, 10)).collect();
        settle(&fake);
        assert!(fake.bucket.borrow().contains_key(&huge));
        assert!(rest.iter().all(|k| !fake.bucket.borrow().contains_key(k)));
        assert_eq!(classify(&fake.keys(&ops_prefix(KIND))).read, vec![huge]);
    }

    #[test]
    fn compaction_stops_cleanly_at_the_wall_time_limit_and_resumes_next_tick() {
        let fake = Fake::new(BASE);
        seed_state(&fake);
        let sources = seed_windows(&fake, 6);
        fake.elapsed_s.set(41);
        let stopped = compact(&fake, 3);
        assert_eq!((stopped.chunks, stopped.stopped), (0, Some("wall_time")));
        assert!(stopped.more_pending);
        assert_eq!(fake.puts.get(), 0);
        assert!(sources.iter().all(|k| fake.bucket.borrow().contains_key(k)));
        fake.elapsed_s.set(0);
        settle(&fake);
        assert_eq!(compacts(&fake, KIND).len(), 1);
    }

    #[test]
    fn a_tick_with_a_backlog_stays_inside_the_lookup_and_call_limits() {
        let fake = Fake::new(BASE);
        seed_idle_backlog(&fake);
        for _ in 0..6 {
            fake.now.set(fake.now.get() + 120);
            fake.kv_reads.borrow_mut().clear();
            run_tick(&fake);
            assert!(fake.kv_reads.borrow().len() <= 32);
            assert!(fake.calls.get() <= BUDGET);
        }
    }

    fn seed_idle_backlog(fake: &Fake) {
        tick_runtime(fake);
        seed_state(fake);
        seed_windows(fake, 4);
    }

    fn cron_kinds(fake: &Fake) -> Vec<String> {
        rows(fake, LogKind::CronRuns)
            .iter()
            .map(|r| r["cron_kind"].as_str().unwrap().to_owned())
            .collect()
    }

    #[test]
    fn an_idle_tick_runs_log_compact() {
        let fake = Fake::new(BASE);
        seed_idle_backlog(&fake);
        fake.kv.borrow_mut().insert(
            "retention:last_sweep".into(),
            (1, json!({"at": BASE, "complete": true})),
        );
        for _ in 0..16 {
            fake.now.set(fake.now.get() + 120);
            run_tick(&fake);
        }
        assert!(cron_kinds(&fake).contains(&"log_compact".to_owned()));
        assert!(!compacts(&fake, KIND).is_empty());
        let listing = classify(&fake.keys(&ops_prefix(KIND)));
        assert!(listing.read.len() <= 1, "{:?}", listing.read);
    }

    #[test]
    fn the_retention_sweep_still_runs_on_ticks_with_a_compaction_backlog() {
        let fake = Fake::new(BASE);
        seed_idle_backlog(&fake);
        let old = put_processed(&fake, "processed/request/", BASE - 40 * DAY, "1-2-a-n1");
        for _ in 0..8 {
            fake.now.set(fake.now.get() + 120);
            run_tick(&fake);
        }
        let kinds = cron_kinds(&fake);
        assert!(kinds.contains(&"retention_sweep".to_owned()), "{kinds:?}");
        assert!(!fake.bucket.borrow().contains_key(&old));
        assert!(kinds.contains(&"log_compact".to_owned()), "{kinds:?}");
    }
}

#[test]
fn retention_loops_stop_at_the_wall_and_resume() {
    let fake = Fake::new(BASE + 40 * DAY);
    let queued = converted_fixture(&fake, 2);
    let old = put_processed(&fake, "processed/request/", BASE, "1-2-a-n1");
    let svc = service();
    fake.elapsed_s.set(41);
    let deleted = delete_tick(&fake, &svc);
    assert_eq!((deleted.deleted, deleted.more_pending), (0, true));
    assert!(queued.iter().all(|k| fake.bucket.borrow().contains_key(k)));
    sweep(&fake, &svc, &Default::default());
    assert!(fake.bucket.borrow().contains_key(&old));
    fake.new_request();
    assert!(ingest::retention::request_storage_reconcile(&fake, "admin").unwrap());
    fake.new_request();
    let step = ingest::retention::storage_reconcile_step(&store(&fake, &svc), 0)
        .unwrap()
        .unwrap();
    assert!(step.snapshot.is_none());
    assert!(fake.kv.borrow().contains_key("storage:reconcile"));
    fake.elapsed_s.set(0);
    assert_eq!(delete_tick(&fake, &svc).deleted, 2);
    sweep(&fake, &svc, &Default::default());
    assert!(
        fake.bucket.borrow().contains_key(&old),
        "catalog readers retain their grace"
    );
    fake.now
        .set(fake.now.get() + ingest::compaction::DELETE_GRACE_S);
    sweep(&fake, &svc, &Default::default());
    assert!(!fake.bucket.borrow().contains_key(&old));
}

#[test]
fn worker_delete_stops_at_the_wall_and_reports_the_rest_failed() {
    let fake = Fake::new(BASE);
    let svc = service();
    let keys: Vec<String> = (0..3)
        .map(|i| put_processed(&fake, "processed/request/", BASE + i * 3600, "1-2-a-n1"))
        .collect();
    let request = fastly_log_analytics::http::Request {
        method: "POST".into(),
        path: "/_internal/delete".into(),
        headers: [("x-flp-worker-key".to_owned(), "fos-secret".to_owned())].into(),
        body: json!({
            "service_id": svc.service_id, "region": svc.fos_region,
            "bucket": svc.fos_bucket, "prefix": ingest::scope(&svc), "keys": keys,
        })
        .to_string()
        .into_bytes(),
    };
    fake.elapsed_s.set(41);
    let response = ingest::retention::handle_worker_delete(&fake, &request);
    let reply: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(reply["deleted"].as_array().unwrap().len(), 0);
    assert_eq!(reply["failed"].as_array().unwrap().len(), 3);
    assert!(keys.iter().all(|k| fake.bucket.borrow().contains_key(k)));
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

#[test]
fn sustained_load_of_sixty_files_a_minute_drains_without_backlog() {
    let fake = Fake::new(BASE);
    let mut expected = Vec::new();
    let mut max_calls = 0;
    let mut produced_until = BASE;
    for round in 0..12 {
        // Two minutes of 60 files each arrive between ticks.
        for _ in 0..2 {
            for i in 0..60 {
                let id = format!("t{produced_until}i{i:02}");
                put_raw(
                    &fake,
                    produced_until,
                    &format!("f{i:02}"),
                    &[
                        line(produced_until + i, &id),
                        line(produced_until + i, &format!("{id}b")),
                    ],
                );
                expected.push(id.clone());
                expected.push(format!("{id}b"));
            }
            produced_until += 60;
        }
        fake.now.set(produced_until as u64);
        let report = tick(&fake).unwrap();
        assert!(!report.busy, "round {round}");
        max_calls = max_calls.max(report.backend_calls);
        // Only objects that have not settled yet (the last three minutes) may wait.
        let watermark = fake
            .kv
            .borrow()
            .get(ingest::WATERMARK_KEY)
            .map(|(_, v)| v["key"].as_str().unwrap().to_owned())
            .unwrap_or_default();
        let unconverted = fake
            .keys("raw/request/")
            .into_iter()
            .filter(|key| key.as_str() > watermark.as_str())
            .count();
        assert!(
            unconverted <= 3 * 60,
            "round {round}: backlog {unconverted}"
        );
    }
    for _ in 0..3 {
        let report = tick_later(&fake).unwrap();
        max_calls = max_calls.max(report.backend_calls);
    }
    expected.sort();
    assert_eq!(processed_ips(&fake), expected);
    assert!(committed(&fake));
    assert!(
        max_calls + ingest::TICK_RESERVE <= BUDGET,
        "coordinator {max_calls}"
    );
    assert!(fake.worker_max_calls.get() <= BUDGET);
    let files = rows(&fake, LogKind::IngestFiles);
    assert_eq!(files.len(), expected.len() / 2);
}

#[test]
fn a_crash_at_any_call_of_the_tick_never_duplicates_or_loses_rows() {
    for lost in [false, true] {
        let mut crash = 0;
        loop {
            let fake = Fake::new(BASE + 7200);
            let expected = seed(&fake, 3, 20);
            if lost {
                // One shard's answer is lost, so the tick saves partial results instead.
                fake.worker_faults
                    .borrow_mut()
                    .push_back(WorkerFault::LostReply);
            }
            fake.crash_after.set(Some(crash));
            let crashed = tick(&fake);
            let finished = crashed.is_ok() && fake.calls.get() < crash;
            fake.crash_after.set(None);
            for _ in 0..6 {
                tick_later(&fake).unwrap();
            }
            assert!(committed(&fake), "lost {lost} crash {crash}");
            assert_eq!(processed_ips(&fake), expected, "lost {lost} crash {crash}");
            // Telemetry of a tick that dies after its commit is lost with it, never doubled.
            let files = rows(&fake, LogKind::IngestFiles);
            let distinct: BTreeSet<&str> = files
                .iter()
                .map(|r| r["raw_key"].as_str().unwrap())
                .collect();
            assert_eq!(distinct.len(), files.len(), "lost {lost} crash {crash}");
            if finished {
                break;
            }
            crash += 1;
        }
        assert!(crash > 8, "every call index of a full tick was crashed");
    }
}

#[test]
fn lost_and_failed_worker_calls_are_retried_without_duplicates() {
    let fake = Fake::new(BASE + 7200);
    let expected = seed(&fake, 3, 20);
    fake.worker_faults.borrow_mut().extend([
        WorkerFault::LostReply,
        WorkerFault::Unreachable,
        WorkerFault::Ok,
    ]);
    let first = tick(&fake).unwrap();
    assert!(first.more_pending);
    assert_eq!(first.converted, 0);
    assert_eq!((first.shards_dispatched, first.shards_done), (3, 1));
    // An unanswered worker may still be writing: the lease holds until it expires.
    assert!(tick(&fake).unwrap().busy);
    let second = tick_later(&fake).unwrap();
    assert!(second.resumed);
    assert_eq!(second.shards_dispatched, 2);
    assert_eq!(second.converted, 60);
    assert_eq!(processed_ips(&fake), expected);
}

#[test]
fn a_shard_with_too_many_windows_for_its_budget_splits_before_writing() {
    let fake = Fake::new(BASE + 7200);
    let mut expected = Vec::new();
    for i in 0..10i64 {
        // All in one minute directory, but each spans 6 windows; 10 objects reach 15 windows,
        // more than the 8 windows (two PUTs each) one 10-object worker has room for.
        let lines: Vec<String> = (0..6)
            .map(|w| {
                let id = format!("o{i}w{w:02}");
                expected.push(id.clone());
                line(BASE + (i + w) * 60, &id)
            })
            .collect();
        put_raw(&fake, BASE, &format!("o{i}"), &lines);
    }
    expected.sort();
    let first = tick(&fake).unwrap();
    assert_eq!(first.converted, 0);
    assert!(
        fake.keys("processed/").is_empty(),
        "nothing written before the split"
    );
    let shards = plan(&fake).unwrap()["shards"].clone();
    assert_eq!(
        shards.as_object().unwrap().keys().collect::<Vec<_>>(),
        ["0a", "0b"]
    );
    assert_eq!(tick_later(&fake).unwrap().converted, 10);
    assert_eq!(processed_ips(&fake), expected);
    let token = format!("{}s0-", plan(&fake).unwrap()["plan_id"].as_str().unwrap());
    assert!(fake
        .keys("processed/request/")
        .iter()
        .all(|k| !k.contains(&token)));
}

#[test]
fn a_shard_failing_three_times_abandons_the_plan_and_replans_without_duplicates() {
    let fake = Fake::new(BASE + 7200);
    let expected = seed(&fake, 2, 5);
    // The first attempt writes its files but the answer is lost; two more attempts never arrive.
    fake.worker_faults.borrow_mut().extend([
        WorkerFault::LostReply,
        WorkerFault::Unreachable,
        WorkerFault::Unreachable,
    ]);
    tick(&fake).unwrap();
    let stale = fake.keys("processed/request/");
    assert!(!stale.is_empty());
    assert!(!fake.keys("processed/_inflight/").is_empty());
    let old = plan(&fake).unwrap();
    tick_later(&fake).unwrap();
    tick_later(&fake).unwrap();
    assert_eq!(plan(&fake).unwrap()["abandoning"], "shard_failures");
    // The deciding tick leaves cleanup to a later tick; that one removes every stale file.
    let cleanup = tick_later(&fake).unwrap();
    assert_eq!(cleanup.converted, 0);
    assert!(fake.keys("processed/").is_empty());
    let replanned = plan(&fake).unwrap();
    assert_ne!(replanned["plan_id"], old["plan_id"]);
    assert_eq!(replanned["raw_keys"], old["raw_keys"]);
    assert_eq!(replanned["replans"], 1);
    assert_eq!(tick_later(&fake).unwrap().converted, 10);
    assert_eq!(processed_ips(&fake), expected);
    assert!(stale.iter().all(|k| !fake.bucket.borrow().contains_key(k)));
}

#[test]
fn a_plan_from_another_schema_is_abandoned_and_its_outputs_replaced() {
    let fake = Fake::new(BASE + 7200);
    let expected = seed(&fake, 2, 5);
    fake.worker_faults
        .borrow_mut()
        .push_back(WorkerFault::LostReply);
    tick(&fake).unwrap();
    let stale = fake.keys("processed/request/");
    assert!(!stale.is_empty());
    {
        let mut kv = fake.kv.borrow_mut();
        let entry = kv.get_mut(ingest::PLAN_KEY).unwrap();
        entry.1["schema"] = "0000000000000000".into();
    }
    let deciding = tick_later(&fake).unwrap();
    assert_eq!(deciding.shards_dispatched, 0);
    assert_eq!(plan(&fake).unwrap()["abandoning"], "schema_changed");
    tick_later(&fake).unwrap();
    assert!(fake.keys("processed/").is_empty());
    assert_eq!(tick_later(&fake).unwrap().converted, 10);
    assert_eq!(processed_ips(&fake), expected);
}

#[test]
fn a_leftover_deleteq_of_an_abandoned_plan_is_removed_before_the_replan() {
    let fake = Fake::new(BASE + 7200);
    let expected = seed(&fake, 2, 5);
    fake.worker_faults
        .borrow_mut()
        .push_back(WorkerFault::LostReply);
    tick(&fake).unwrap();
    let old = plan(&fake).unwrap();
    // A commit whose queue write landed but whose watermark CAS was lost.
    let leftover = format!("ingest:deleteq:{}", old["plan_id"].as_str().unwrap());
    fake.kv.borrow_mut().insert(
        leftover.clone(),
        (1, json!({"run_id": old["plan_id"], "keys": old["raw_keys"], "sizes": [], "inflight_keys": []})),
    );
    fake.kv.borrow_mut().get_mut(ingest::PLAN_KEY).unwrap().1["schema"] = "0000000000000000".into();
    tick_later(&fake).unwrap();
    tick_later(&fake).unwrap();
    assert!(!fake.kv.borrow().contains_key(&leftover));
    assert_ne!(plan(&fake).unwrap()["plan_id"], old["plan_id"]);
    assert_eq!(tick_later(&fake).unwrap().converted, 10);
    assert_eq!(processed_ips(&fake), expected);
    assert_eq!(deleteq(&fake).len(), 1, "only the committed plan's queue");
}

#[test]
fn a_live_lease_makes_another_tick_busy_without_writes() {
    let fake = Fake::new(BASE + 7200);
    seed(&fake, 1, 3);
    fake.worker_faults
        .borrow_mut()
        .push_back(WorkerFault::Unreachable);
    tick(&fake).unwrap();
    let before = fake.kv.borrow()[ingest::PLAN_KEY].clone();
    fake.now
        .set(fake.now.get() + ingest::coordinator::LEASE_S - 1);
    let report = tick(&fake).unwrap();
    assert!(report.busy);
    assert_eq!(fake.kv.borrow()[ingest::PLAN_KEY], before);
    assert_eq!(report.shards_dispatched, 0);
}

#[test]
fn an_uncommitted_v1_plan_is_finished_once_in_tick_before_v2_planning() {
    let fake = Fake::new(BASE + 7200);
    let old = put_raw(&fake, BASE, "old", &[line(BASE, "old")]);
    let new = put_raw(&fake, BASE + 60, "new", &[line(BASE + 60, "new")]);
    fake.kv.borrow_mut().insert(
        ingest::PLAN_KEY.into(),
        (
            3,
            json!({"run_id": "abcdefabcdef", "window_s": 60, "schema": "v3",
                "raw_keys": [old], "created_at": 1}),
        ),
    );
    let first = tick(&fake).unwrap();
    assert!(first.resumed);
    assert_eq!(first.converted, 1);
    assert_eq!(fake.worker_runs.get(), 0, "v1 plans finish in-tick");
    assert_eq!(
        fake.kv.borrow()[ingest::WATERMARK_KEY].1["run_id"],
        "abcdefabcdef"
    );
    let second = tick(&fake).unwrap();
    assert_eq!(second.converted, 1);
    assert_eq!(plan(&fake).unwrap()["version"], 2);
    assert_eq!(plan(&fake).unwrap()["raw_keys"], json!([new]));
    assert_eq!(processed_ips(&fake), ["new", "old"]);
}

#[test]
fn an_etag_that_is_not_the_body_md5_is_never_recorded() {
    let fake = Fake::new(BASE + 7200);
    let expected = seed(&fake, 1, 3);
    *fake.bad_etag_for.borrow_mut() = Some(".parquet".into());
    let first = tick(&fake).unwrap();
    assert_eq!(first.converted, 0);
    let saved = plan(&fake).unwrap();
    assert!(saved["results"].as_object().unwrap().is_empty());
    assert_eq!(saved["failures"]["0"], 1);
    assert!(rows(&fake, LogKind::PlatformErrors)
        .iter()
        .any(|r| r["code"] == "etag_mismatch"));
    *fake.bad_etag_for.borrow_mut() = None;
    assert_eq!(tick_later(&fake).unwrap().converted, 3);
    assert_eq!(processed_ips(&fake), expected);
}

#[test]
fn outputs_are_window_files_named_by_plan_and_shard_and_readers_skip_manifests() {
    let fake = Fake::new(BASE + 7200);
    seed(&fake, 2, 2);
    tick(&fake).unwrap();
    let plan_id = plan(&fake).unwrap()["plan_id"].as_str().unwrap().to_owned();
    for key in fake.keys("processed/request/") {
        let name = ingest::parse_processed(&key).unwrap();
        assert_eq!(name.end - name.start, 60);
        assert!(key.contains(&format!("-{plan_id}s0-n")), "{key}");
    }
    let manifests = fake.keys("processed/_inflight/");
    assert_eq!(manifests, [format!("processed/_inflight/{plan_id}/0.json")]);
    let query = format!("from={}&to={}", iso_url(BASE), iso_url(BASE + 3600));
    let listing = files_page(&fake, &query).unwrap();
    assert_eq!(listing["files"].as_array().unwrap().len(), 2);
    assert!(!listing.to_string().contains("_inflight"));
}

#[test]
fn worker_ingest_authenticates_validates_and_reports_schema_mismatch() {
    let fake = Fake::new(BASE + 7200);
    let key = put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    let body = json!({
        "service_id": SID, "region": "us-east-1", "bucket": "fos-abcdefghijklmnopqrst12-logs",
        "prefix": "", "plan_id": "0123456789ab", "shard": "0", "attempt": 1,
        "stream": "request", "table": "request", "schema": ingest::convert::schema_hash(),
        "window_s": 60, "keys": [key], "stop_ms": 20_000,
    });
    let call = |header: &str, body: &Value| {
        fake.new_request();
        ingest::worker::handle_worker_ingest(
            &fake,
            &fastly_log_analytics::http::Request {
                method: "POST".into(),
                path: ingest::worker::PATH.into(),
                headers: [("x-flp-worker-key".to_owned(), header.to_owned())].into(),
                body: body.to_string().into_bytes(),
            },
        )
    };
    assert_eq!(call("wrong", &body).status, 403);
    for (field, value) in [
        ("keys", json!(["processed/request/x.zst"])),
        ("keys", json!([])),
        ("plan_id", json!("../x")),
        ("prefix", json!("/abs/")),
        ("shard", json!("A")),
    ] {
        let mut bad = body.clone();
        bad[field] = value;
        assert_eq!(call("fos-secret", &bad).status, 400, "{field}");
    }
    let mut other = body.clone();
    other["schema"] = "ffffffffffffffff".into();
    let reply: Value = serde_json::from_slice(&call("fos-secret", &other).body).unwrap();
    assert_eq!(reply["status"], "schema_mismatch");
    assert!(fake.keys("processed/").is_empty());
    let reply: Value = serde_json::from_slice(&call("fos-secret", &body).body).unwrap();
    assert_eq!(reply["status"], "ok");
    assert!(reply["calls"].as_u64().unwrap() <= u64::from(BUDGET));
    assert_eq!(reply["result"]["objects"][0]["status"], "converted");
    let summary = reply["summary_vcpu_ms"].as_u64().unwrap();
    assert!(summary > 0 && summary < reply["vcpu_ms"].as_u64().unwrap());
    assert!(fake.kv.borrow().is_empty(), "the worker writes no KV");
}

#[test]
fn worker_routes_untimestamped_rows_by_the_plan_time_so_retries_write_the_same_keys() {
    let fake = Fake::new(BASE + 7200);
    let key = "raw/request/odd/object.zst".to_owned();
    let untimed = json!({"ip": "a", "status": 200}).to_string();
    fake.bucket.borrow_mut().insert(key.clone(), gz(&[untimed]));
    let body = json!({
        "service_id": SID, "region": "us-east-1", "bucket": "fos-abcdefghijklmnopqrst12-logs",
        "prefix": "", "plan_id": "0123456789ab", "shard": "0", "attempt": 1,
        "stream": "request", "table": "request", "schema": ingest::convert::schema_hash(),
        "window_s": 60, "keys": [key], "stop_ms": 20_000, "created_at": BASE,
    });
    let mut written = Vec::new();
    for now in [BASE + 7200, BASE + 7200 + 3600] {
        fake.now.set(now as u64);
        fake.new_request();
        let response = ingest::worker::handle_worker_ingest(
            &fake,
            &fastly_log_analytics::http::Request {
                method: "POST".into(),
                path: ingest::worker::PATH.into(),
                headers: [("x-flp-worker-key".to_owned(), "fos-secret".to_owned())].into(),
                body: body.to_string().into_bytes(),
            },
        );
        let reply: Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(reply["status"], "ok", "{reply}");
        written.push(fake.keys("processed/request/"));
    }
    assert_eq!(written[0].len(), 1);
    assert_eq!(written[0], written[1]);
    let name = ingest::parse_processed(&written[0][0]).unwrap();
    assert_eq!(name.start, BASE - BASE.rem_euclid(60));
}

#[test]
fn extra_worker_requests_ride_the_same_dispatch_batch() {
    let fake = Fake::new(BASE + 7200);
    seed(&fake, 1, 2);
    fake.new_request();
    let buffer = LogBuffer::new(SID, "req");
    let options = ingest::coordinator::Options {
        extra: vec![("/_internal/echo".into(), json!({"stage": "log_flush"}))],
        ..ingest::coordinator::Options::request(&fake)
    };
    let report = ingest::coordinator::coordinate(
        &fake,
        &service(),
        "ak",
        "sk",
        Instant::now(),
        &buffer,
        &options,
    )
    .unwrap();
    assert_eq!(report.converted, 2);
    assert_eq!(report.extra_results.len(), 1);
    assert_eq!(
        report.extra_results[0].as_ref().unwrap()["path"],
        "/_internal/echo"
    );
    // Nothing to ingest: the extras still go out, alone.
    fake.new_request();
    let alone = ingest::coordinator::coordinate(
        &fake,
        &service(),
        "ak",
        "sk",
        Instant::now(),
        &buffer,
        &options,
    )
    .unwrap();
    assert_eq!(alone.converted, 0);
    assert!(alone.extra_results[0].is_ok());
}

#[test]
fn raw_delete_requeues_manifests_it_could_not_delete() {
    let fake = Fake::new(BASE + 3600);
    converted_fixture(&fake, 2);
    let manifest = fake.keys("processed/_inflight/")[0].clone();
    fake.fail_delete_keys.borrow_mut().insert(manifest.clone());
    let report = delete_tick(&fake, &service());
    assert_eq!(report.deleted, 2);
    let queued = deleteq(&fake);
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0]["keys"], json!([]));
    assert_eq!(queued[0]["inflight_keys"], json!([manifest]));
    fake.fail_delete_keys.borrow_mut().clear();
    delete_tick(&fake, &service());
    assert!(deleteq(&fake).is_empty());
    assert!(fake.keys("processed/_inflight/").is_empty());
    assert_eq!(processed_ips(&fake), ["a", "a"]);
}

/// F5 and the 1 s dispatcher: the front service's `/_tick` and `/_flush` probes reach flp every
/// second, fast-forwarded over a 52-files-per-period load with a 60 s log period.
mod simulation {
    use super::*;
    use fastly_log_analytics::admin::logs::MAINT_STAGES;
    use fastly_log_analytics::admin::schedule::MAINT_INTERVAL_S;
    use fastly_log_analytics::ingest::retention::SWEEP_INTERVAL_S;
    use std::collections::VecDeque;

    const PERIOD_S: i64 = 60;
    const WINDOW_S: i64 = 120;
    const FILES_PER_PERIOD: usize = 52;
    const LINES_PER_FILE: usize = 2;
    /// A requested maintenance stage runs within this many seconds: one stage dispatch every
    /// `MAINT_INTERVAL_S`, round-robin over the stages, plus a tick.
    const TURN_S: i64 = MAINT_STAGES.len() as i64 * MAINT_INTERVAL_S as i64 + 1;
    const RECONCILE_KEY: &str = "storage:reconcile";
    const HISTORY_KEY: &str = "logs:delete_history";
    /// A fresh cursor starts `MAX_SCAN_MIN` back with the `global` sid as well, then scans
    /// three minutes a flush; it has caught up by then.
    const WARMUP_S: i64 = 3 * 3600;
    /// Outbox keys left at the end of a day: one per 2-minute bucket, waiting up to the default
    /// `flush_max_age_s` (an hour, 30 buckets) when nothing else makes the flush due, plus the
    /// two buckets inside the read grace and the odd rollover or overflow key.
    const OUTBOX_BOUND: usize = 3600 / 120 + 6;
    /// Period close to Parquet: the grace (`INGEST_GRACE_S` past the period start, so 120 s
    /// past its close) and the next tick.
    const LATENCY_BOUND_S: i64 = 125;

    #[derive(Default)]
    struct Day {
        raw_pending: usize,
        logq_pending: usize,
        flush_commits: usize,
        ops_files: usize,
        /// Most minutes the flush cursor trailed the clock after any flush past the warm-up.
        max_scan_lag_min: u64,
        /// Executions that ran a job, and probes that ended at the schedule claim.
        tick_runs: usize,
        idle_ticks: usize,
        flush_runs: usize,
        /// Seconds from a period's close until its raw objects were converted and deleted.
        max_latency_s: i64,
        latency_sum_s: i64,
        latency_n: i64,
    }

    #[derive(Default)]
    struct Sim {
        days: Vec<Day>,
        /// Stage-worker turns by stage, as simulated seconds.
        turns: BTreeMap<String, Vec<i64>>,
        /// Most seconds a requested reconcile or history job waited for its first turn.
        max_wait: BTreeMap<&'static str, i64>,
        /// Most seconds the completed-sweep marker aged.
        max_sweep_age_s: i64,
        expected: Vec<String>,
        /// Admin audit rows written, from `audit:` keys and the service's audit outbox.
        audits: usize,
    }

    /// Durable outbox keys of this service, whatever their layout.
    fn outbox_depth(fake: &Fake) -> usize {
        fake.kv
            .borrow()
            .keys()
            .filter(|k| k.starts_with(&format!("logq_{SID}_")))
            .count()
    }

    /// One admin login (`audit:<seq>`) and one service config change (svc audit outbox).
    fn audit(fake: &Fake, seq: usize) {
        let now = fake.now.get();
        let mut kv = fake.kv.borrow_mut();
        kv.insert(
            format!("audit:{seq:020}"),
            (1, json!({"time": now, "actor": "admin", "action": "login"})),
        );
        let (generation, svc) = kv.get_mut(&format!("svc:{SID}")).unwrap();
        *generation += 1;
        if !svc["_audit_outbox"].is_array() {
            svc["_audit_outbox"] = json!([]);
        }
        svc["_audit_outbox"]
            .as_array_mut()
            .unwrap()
            .push(json!({"actor": "admin", "at": now, "event": "config", "config_version": seq}));
    }

    fn archived_audits(fake: &Fake) -> usize {
        fake.keys("processed/ops/admin_audit/")
            .iter()
            .flat_map(|key| batches(&fake.bucket.borrow()[key]))
            .map(|batch| batch.num_rows())
            .sum()
    }

    fn start(fake: &Fake) {
        tick_runtime(fake);
        let mut svc = serde_json::to_value(service()).unwrap();
        svc["log_period"] = json!(PERIOD_S);
        svc["compaction"] = json!({"mode": "size_target", "size_target_mb": 64});
        fake.kv.borrow_mut().insert(format!("svc:{SID}"), (1, svc));
        *fake.window.borrow_mut() = Some(WINDOW_S.to_string());
    }

    /// One log period: Fastly writes `FILES_PER_PERIOD` objects for the period just closed.
    /// Returns one of them, to time the period's conversion.
    fn deliver(fake: &Fake, period_n: usize, expected: &mut Vec<String>) -> String {
        let period = fake.now.get() as i64 - PERIOD_S;
        let mut first = String::new();
        for file in 0..FILES_PER_PERIOD {
            let lines: Vec<String> = (0..LINES_PER_FILE)
                .map(|n| {
                    let ip = format!("{period_n:05}-{file:02}-{n}");
                    expected.push(ip.clone());
                    line(period + (file as i64 % PERIOD_S), &ip)
                })
                .collect();
            let key = put_raw(fake, period, &format!("t{period_n:05}f{file:02}"), &lines);
            if file == 0 {
                first = key;
            }
        }
        first
    }

    type KvRecord = Option<(u64, Value)>;

    fn record(fake: &Fake, key: &str) -> KvRecord {
        fake.kv.borrow().get(key).cloned()
    }

    fn simulate(fake: &Fake, seconds: i64) -> Sim {
        start(fake);
        // Start on a period boundary, as Fastly's log periods are aligned.
        fake.now
            .set(fake.now.get() / PERIOD_S as u64 * PERIOD_S as u64);
        let mut sim = Sim::default();
        // A requested stage's KV record changes on its first turn.
        let mut asked: BTreeMap<&'static str, (i64, KvRecord)> = BTreeMap::new();
        let begun = fake.now.get() as i64;
        let cursor_key = format!("logs:cursor:{SID}");
        let mut txn = Value::Null;
        let mut day = Day::default();
        let mut waiting: VecDeque<(i64, String)> = VecDeque::new();
        for second in 1..=seconds {
            fake.now.set(fake.now.get() + 1);
            let now = fake.now.get() as i64;
            if second % PERIOD_S == 0 {
                let period_n = (second / PERIOD_S) as usize;
                waiting.push_back((now, deliver(fake, period_n, &mut sim.expected)));
                if period_n % 30 == 7 {
                    audit(fake, period_n);
                    sim.audits += 2;
                }
            }
            if second % DAY == DAY / 3 {
                fake.new_request();
                assert!(ingest::retention::request_storage_reconcile(fake, "admin").unwrap());
                asked.insert(RECONCILE_KEY, (second, record(fake, RECONCILE_KEY)));
            }
            if second % DAY == DAY / 2 {
                fake.kv.borrow_mut().insert(
                    HISTORY_KEY.into(),
                    (
                        1,
                        json!({"kind": "cron_runs", "older_than_days": 30,
                               "cutoff_ts": (now - 30 * DAY) / 3600 * 3600,
                               "status": "deleting", "actor": "admin", "started_at": now,
                               "updated_at": now, "deleted": 0, "error": null}),
                    ),
                );
                asked.insert(HISTORY_KEY, (second, record(fake, HISTORY_KEY)));
            }
            let before = fake.stage_runs.borrow().len();
            let report = run_tick(fake);
            assert!(report["ok"] == true, "{report}");
            if report.get("run").is_some() {
                day.tick_runs += usize::from(!report["run"].is_null());
                for run in &fake.stage_runs.borrow()[before..] {
                    if let Some(ran) = run["ran"].as_str() {
                        sim.turns.entry(ran.to_owned()).or_default().push(second);
                    }
                }
                while waiting
                    .front()
                    .is_some_and(|(_, key)| !fake.bucket.borrow().contains_key(key))
                {
                    let (closed, _) = waiting.pop_front().unwrap();
                    let latency = now - closed;
                    day.max_latency_s = day.max_latency_s.max(latency);
                    day.latency_sum_s += latency;
                    day.latency_n += 1;
                }
                let changed: Vec<&'static str> = asked
                    .iter()
                    .filter(|(key, (_, seen))| record(fake, key) != *seen)
                    .map(|(key, _)| *key)
                    .collect();
                for key in changed {
                    asked.remove(key);
                }
            } else {
                assert_eq!(fake.calls.get(), 0, "an idle tick made a backend call");
                day.idle_ticks += 1;
            }
            for (key, (at, _)) in &asked {
                let wait = sim.max_wait.entry(key).or_default();
                *wait = (*wait).max(second - *at);
            }
            if second % 60 == 0 {
                let marker = record(fake, "retention:last_sweep").map_or(Value::Null, |r| r.1);
                let swept = marker["at"].as_i64().unwrap_or(begun);
                sim.max_sweep_age_s = sim.max_sweep_age_s.max(now - swept);
            }

            let flushed = run_flush(fake);
            if flushed.get("configured").is_some() {
                day.flush_runs += 1;
                let cursor = record(fake, &cursor_key);
                let committed = cursor
                    .as_ref()
                    .map_or(Value::Null, |c| c.1["txn_id"].clone());
                if !committed.is_null() && committed != txn {
                    day.flush_commits += 1;
                    txn = committed;
                }
                let scan_min = cursor.map_or(0, |c| c.1["scan_min"].as_u64().unwrap_or(0));
                if second >= WARMUP_S {
                    day.max_scan_lag_min = day
                        .max_scan_lag_min
                        .max((now as u64 / 60).saturating_sub(scan_min));
                }
            } else {
                assert_eq!(
                    fake.calls.get(),
                    0,
                    "an idle flush probe made a backend call"
                );
            }
            if second % DAY == 0 || second == seconds {
                day.raw_pending = fake.keys("raw/").len();
                day.logq_pending = outbox_depth(fake);
                day.ops_files = fake.keys("processed/ops/").len();
                sim.days.push(std::mem::take(&mut day));
            }
        }
        sim
    }

    fn report(fake: &Fake, sim: &Sim) {
        for (n, day) in sim.days.iter().enumerate() {
            eprintln!(
                "day {n}: raw pending {}, logq {}, flush commits {}, max scan lag {} min, \
                 ops files {}, tick runs {}, idle ticks {}, flush runs {}, \
                 ingest latency max {} s mean {} s",
                day.raw_pending,
                day.logq_pending,
                day.flush_commits,
                day.max_scan_lag_min,
                day.ops_files,
                day.tick_runs,
                day.idle_ticks,
                day.flush_runs,
                day.max_latency_s,
                day.latency_sum_s / day.latency_n.max(1),
            );
        }
        for (kind, (calls, ops)) in fake.peaks.borrow().iter() {
            eprintln!("{kind}: max {calls} backend calls, {ops} cache ops");
        }
        for (stage, turns) in &sim.turns {
            eprintln!("{stage}: {} turns", turns.len());
        }
        eprintln!(
            "max waits {:?} s, max sweep age {} s",
            sim.max_wait, sim.max_sweep_age_s
        );
    }

    fn assert_within_budgets(fake: &Fake) {
        for (kind, (calls, ops)) in fake.peaks.borrow().iter() {
            assert!(*calls <= BUDGET, "{kind} used {calls} calls");
            assert!(
                *ops <= fastly_log_analytics::shell::CACHE_OP_BUDGET,
                "{kind} used {ops} cache ops"
            );
        }
    }

    #[test]
    fn the_log_flush_commits_and_keeps_up_under_sustained_ingest() {
        let fake = Fake::new(BASE + 40 * DAY);
        let sim = simulate(&fake, DAY / 4);
        report(&fake, &sim);
        let day = &sim.days[0];
        assert!(
            day.flush_commits >= 10,
            "{} flush commits",
            day.flush_commits
        );
        assert!(
            day.max_scan_lag_min <= 10,
            "flush lag {} min",
            day.max_scan_lag_min
        );
        assert_within_budgets(&fake);
    }

    #[test]
    #[ignore = "soak: run with `make -C app soak`"]
    fn every_maintenance_stage_gets_a_turn_within_four_intervals_under_load() {
        let fake = Fake::new(BASE + 40 * DAY);
        let sim = simulate(&fake, DAY);
        report(&fake, &sim);
        for key in [RECONCILE_KEY, HISTORY_KEY] {
            let wait = sim.max_wait.get(key).copied().unwrap_or(i64::MAX);
            assert!(wait <= TURN_S, "{key} waited {wait} s");
        }
        assert!(
            sim.max_sweep_age_s <= SWEEP_INTERVAL_S as i64 + TURN_S + 60,
            "the sweep went {} s without completing",
            sim.max_sweep_age_s
        );
        for stage in MAINT_STAGES.iter().filter(|stage| **stage != "delete_data") {
            assert!(sim.turns.contains_key(*stage), "{stage} never ran");
        }
    }

    #[test]
    #[ignore = "soak: run with `make -C app soak`"]
    fn two_days_of_1s_ticks_at_52_files_per_period_convert_exactly_without_a_backlog() {
        let fake = Fake::new(BASE + 40 * DAY);
        let sim = simulate(&fake, 2 * DAY);
        report(&fake, &sim);
        assert_within_budgets(&fake);
        for (n, day) in sim.days.iter().enumerate() {
            assert!(
                day.raw_pending <= 3 * FILES_PER_PERIOD,
                "backlog on day {n}: {}",
                day.raw_pending
            );
            assert!(day.flush_commits > 0, "no flush committed on day {n}");
            assert!(
                day.max_latency_s <= LATENCY_BOUND_S,
                "ingest latency on day {n}: {} s",
                day.max_latency_s
            );
            // About one run per period plus maintenance every 120 s; every other tick is idle.
            assert!(
                day.tick_runs <= (DAY / PERIOD_S + DAY / MAINT_INTERVAL_S as i64) as usize * 2,
                "{} tick runs on day {n}",
                day.tick_runs
            );
            assert!(
                day.logq_pending <= OUTBOX_BOUND,
                "outbox depth on day {n}: {}",
                day.logq_pending
            );
        }
        assert!(sim.days.windows(2).all(|d| d[1].ops_files > d[0].ops_files));
        // Drain: no new deliveries until the last period is converted and its raws deleted.
        for _ in 0..300 {
            fake.now.set(fake.now.get() + 1);
            run_tick(&fake);
            run_flush(&fake);
        }
        assert!(fake.keys("raw/").is_empty());
        let mut expected = sim.expected;
        expected.sort();
        let got = processed_ips(&fake);
        assert_eq!(got.len(), expected.len(), "missing or duplicate rows");
        assert!(got == expected, "the Parquet rows differ from the input");
        // Past the default flush_max_age_s, only the flush's own rows since its last commit remain
        // queued: each commit's row waits for the next one, at most the max age later.
        for _ in 0..3600 + 600 {
            fake.now.set(fake.now.get() + 1);
            run_flush(&fake);
        }
        let now = fake.now.get();
        let left: Vec<(String, Value)> = fake
            .kv
            .borrow()
            .iter()
            .filter(|(k, _)| k.starts_with(&format!("logq_{SID}_")))
            .map(|(k, v)| (k.clone(), v.1.clone()))
            .collect();
        assert!(left.len() <= 3, "the outbox drains: {} keys", left.len());
        for (key, value) in &left {
            for line in value["ndjson"].as_str().unwrap().lines() {
                let row: Value = serde_json::from_str(line).unwrap();
                assert_eq!(row["cron_kind"], "log_flush", "{key} holds {line}");
                assert!(
                    row["bucket"].as_u64().unwrap() * 120 + 3600 + 240 >= now,
                    "{key} is stale"
                );
            }
        }
        assert_eq!(
            archived_audits(&fake),
            sim.audits,
            "every audit row archived once"
        );
    }
}

/// Task 22.2: closed hours' window summaries merge into hour bundles, closed days into day
/// bundles, bounded per execution and resumable.
mod summary_bundles {
    use super::*;
    use fastly_log_analytics::ingest::{bundle, convert, summary};
    use serde_json::Map;

    const HOUR: i64 = 3600;
    const WINDOWS: &str = "processed/summary/request/";
    const BUNDLES: &str = "processed/summary_bundle/request/";

    fn pick<T: Copy>(values: &[T], i: i64) -> T {
        values[i as usize % values.len()]
    }

    fn line(at_ms: i64, i: i64) -> String {
        let mut row = json!({
            "timestamp": time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(at_ms) * 1_000_000)
                .unwrap()
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap(),
            "status": pick(&[200, 200, 404, 503, 0], i),
            "cache": pick(&["HIT", "MISS", "PASS", "HIT-CLUSTER", "SYNTH"], i),
            "resp_bytes": 100 + i * 7,
            "elapsed": 5_000 + i * 13,
            "ttfb": 0.001 * (i % 17) as f64,
            "ottfb": 2_000 + i,
            "ottlb": 4_000 + i * 2,
            "waf": i % 5 == 0,
            "waf_resp": pick(&[406, 200], i),
            "waf_sig": pick(&["a, b", "", "c"], i),
            "country": pick(&["US", "DE", "JP"], i),
            "asn": pick(&[13335, 15169], i),
            "tcp_rtt": i % 50,
            "edge": i % 4 != 0,
            "ua": format!("ua{}", i % 5),
            "pop": pick(&["IAD", "LHR", "NRT"], i),
            "ost": pick(&[200, 502], i),
            "ploss": (i % 10) as f64 / 100.0,
            "url": format!("/p{}", i % 23),
        });
        if i % 11 != 0 {
            row["ip"] = format!("10.0.{}.{}", i % 3, i % 41).into();
        }
        if i % 9 != 0 {
            row["ja4"] = pick(&["t13d", "t12d"], i).into();
        }
        row.to_string()
    }

    /// Writes the summary of `n` rows in the minute window at `start`; returns its batch.
    fn window(fake: &Fake, start: i64, shard: u32, n: i64) -> RecordBatch {
        let lines: Vec<String> = (0..n)
            .map(|i| {
                line(
                    start * 1000 + i * 977 % 60_000,
                    i + start + i64::from(shard) * 7,
                )
            })
            .collect();
        let rows = convert::convert_object(&gz(&lines), 60, start).unwrap();
        let batch = rows.windows.into_values().next().unwrap();
        let key = summary::summary_key(
            WINDOWS,
            start,
            start + 60,
            &format!("p1s{shard}"),
            batch.num_rows() as u64,
        );
        fake.bucket.borrow_mut().insert(
            key.clone(),
            summary::write_summary(&[&batch], start).unwrap(),
        );
        let bytes = fake.bucket.borrow()[&key].len() as u64;
        publish_outputs(
            fake,
            &[],
            &[ingest::worker::Output {
                key: key.clone(),
                rows: batch.num_rows() as u64,
                bytes,
                ..Default::default()
            }],
            &[],
        );
        batch
    }

    fn watermark(fake: &Fake, at: i64) {
        let previous = fake.kv.borrow().get(ingest::WATERMARK_KEY).cloned();
        let mut value = previous
            .as_ref()
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| json!({}));
        value["key"] = raw_key(at - at % 60, "zzz").into();
        fake.kv.borrow_mut().insert(
            ingest::WATERMARK_KEY.into(),
            (previous.map_or(1, |(generation, _)| generation + 1), value),
        );
    }

    fn run(fake: &Fake) -> bundle::BundleReport {
        fake.new_request();
        let svc = service();
        let report = bundle::summary_bundle(&store(fake, &svc), "ck", 1).unwrap();
        assert!(fake.calls.get() <= BUDGET, "{} calls", fake.calls.get());
        report
    }

    fn settle(fake: &Fake) -> u32 {
        for n in 1..=40 {
            if !run(fake).more_pending {
                return n;
            }
        }
        panic!("bundling did not settle");
    }

    fn read(bytes: &[u8]) -> Vec<Map<String, Value>> {
        use arrow_array::{BinaryArray, BooleanArray, Float64Array};
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes)).unwrap();
        let mut out = Vec::new();
        for batch in builder.build().unwrap() {
            let batch = batch.unwrap();
            for i in 0..batch.num_rows() {
                let mut row = Map::new();
                for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
                    if column.is_null(i) {
                        continue;
                    }
                    let any = column.as_any();
                    let value: Value = if let Some(a) = any.downcast_ref::<StringArray>() {
                        a.value(i).into()
                    } else if let Some(a) = any.downcast_ref::<Int64Array>() {
                        a.value(i).into()
                    } else if let Some(a) = any.downcast_ref::<Float64Array>() {
                        a.value(i).into()
                    } else if let Some(a) = any.downcast_ref::<BooleanArray>() {
                        a.value(i).into()
                    } else if let Some(a) = any.downcast_ref::<TimestampMillisecondArray>() {
                        a.value(i).into()
                    } else if let Some(a) = any.downcast_ref::<BinaryArray>() {
                        hex::encode(a.value(i)).into()
                    } else {
                        panic!("unexpected column type {}", field.name());
                    };
                    row.insert(field.name().clone(), value);
                }
                out.push(row);
            }
        }
        out
    }

    /// Bundle keys whose file name starts with `marker`.
    fn of(fake: &Fake, marker: &str) -> Vec<String> {
        fake.keys(BUNDLES)
            .into_iter()
            .filter(|k| k.rsplit('/').next().unwrap().starts_with(marker))
            .collect()
    }

    fn one_of(fake: &Fake, marker: &str) -> String {
        let keys = of(fake, marker);
        assert_eq!(keys.len(), 1, "{keys:?}");
        keys[0].clone()
    }

    /// Row-by-row equality; floats to 1e-9 relative, and a merged sketch's estimate stands in
    /// for the exact distinct count a single-pass summary records.
    fn assert_same(got: &[Map<String, Value>], want: &[Map<String, Value>]) {
        assert_eq!(got.len(), want.len());
        for (n, (g, w)) in got.iter().zip(want).enumerate() {
            let mut w = w.clone();
            if let Some(sketch) = w.get("ip_sketch").and_then(Value::as_str) {
                let hll = summary::Hll::from_bytes(&hex::decode(sketch).unwrap()).unwrap();
                w.insert("ip_count_observed".into(), (hll.count() as i64).into());
            }
            assert_eq!(
                g.keys().collect::<Vec<_>>(),
                w.keys().collect::<Vec<_>>(),
                "row {n}"
            );
            for (k, a) in g {
                let b = &w[k];
                if a.is_f64() && b.is_f64() {
                    let (a, b) = (a.as_f64().unwrap(), b.as_f64().unwrap());
                    assert!(
                        (a - b).abs() <= 1e-9 * b.abs().max(1.0),
                        "row {n} {k}: {a} != {b}"
                    );
                } else {
                    assert_eq!(a, b, "row {n} {k}: {g:?}");
                }
            }
        }
    }

    fn seed_hour(fake: &Fake, hour: i64, minutes: i64) -> Vec<RecordBatch> {
        let mut batches = Vec::new();
        for m in 0..minutes {
            batches.push(window(fake, hour + m * 60, 0, 20 + m));
            if m == 5 {
                batches.push(window(fake, hour + m * 60, 1, 9));
            }
        }
        batches
    }

    /// Every `view=summary` page for `[from, to)` with `spare` calls left per execution, as
    /// `(kind, key, rows)` in order; asserts no key repeats.
    fn summary_view(fake: &Fake, from: i64, to: i64, spare: u32) -> Vec<(String, String, u64)> {
        let base = format!("view=summary&from={}&to={}", iso_url(from), iso_url(to));
        let (mut cursor, mut out) = (None::<String>, Vec::new());
        for _ in 0..50 {
            let query = match &cursor {
                Some(c) => format!("{base}&cursor={c}"),
                None => base.clone(),
            };
            fake.new_request();
            fake.calls.set(BUDGET - spare);
            let page = api::files(fake, &service(), &query, Instant::now()).unwrap();
            assert_eq!(page["approximate"]["top_k"]["hour"], bundle::HOUR_TOP_K);
            for f in page["files"].as_array().unwrap() {
                let key = f["key"].as_str().unwrap().to_owned();
                assert!(out.iter().all(|(_, k, _)| *k != key), "{key} repeated");
                out.push((
                    f["kind"].as_str().unwrap().to_owned(),
                    key,
                    f["rows"].as_u64().unwrap(),
                ));
            }
            if page["complete"] == true {
                assert!(page["next_cursor"].is_null());
                return out;
            }
            cursor = Some(page["next_cursor"].as_str().unwrap().to_owned());
        }
        panic!("summary view did not complete");
    }

    fn kinds(view: &[(String, String, u64)]) -> Vec<&str> {
        view.iter().map(|(kind, _, _)| kind.as_str()).collect()
    }

    fn day_rows(fake: &Fake) -> u64 {
        fake.keys(WINDOWS)
            .iter()
            .filter(|k| k.contains("/day=15/"))
            .map(|k| {
                k.rsplit_once("-n")
                    .unwrap()
                    .1
                    .trim_end_matches(".parquet")
                    .parse::<u64>()
                    .unwrap()
            })
            .sum()
    }

    #[test]
    fn summary_bundles_do_not_suppress_window_fallbacks_for_fractional_range_boundaries() {
        let fake = Fake::new(BASE + 3 * HOUR);
        watermark(&fake, BASE + 3 * HOUR);
        seed_hour(&fake, BASE, 3);
        settle(&fake);
        let from = ingest::rfc3339(BASE).replace('Z', ".500Z");
        let page = files_page(
            &fake,
            &format!("view=summary&from={from}&to={}", iso_url(BASE + HOUR)),
        )
        .unwrap();
        let files = page["files"].as_array().unwrap();
        assert_eq!(files.len(), 3);
        assert!(files.iter().all(|file| file["kind"] == "window"));
        window(&fake, BASE + HOUR, 0, 1);
        let to = ingest::rfc3339(BASE + HOUR).replace('Z', ".500Z");
        let page = files_page(
            &fake,
            &format!("view=summary&from={}&to={to}", iso_url(BASE)),
        )
        .unwrap();
        let files = page["files"].as_array().unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(files[0]["kind"], "hour");
        assert_eq!(files[1]["kind"], "window");
    }

    #[test]
    fn catalog_delta_reconciles_the_summary_boundary_without_replaying_raw_history() {
        let fake = Fake::new(BASE + 3 * HOUR);
        watermark(&fake, BASE + 3 * HOUR);
        seed_hour(&fake, BASE, 3);
        settle(&fake);
        window(&fake, BASE + HOUR, 0, 20);
        let generation = fake.kv.borrow()[ingest::WATERMARK_KEY].1["catalog"]["generation"]
            .as_u64()
            .unwrap();
        let old = summary_view(&fake, BASE, BASE + HOUR, 30);
        assert_eq!(kinds(&old), ["hour"]);
        let query=format!("summaries=1&from={}&to={}&since_generation={generation}&previous_from={}&previous_to={}",
            iso_url(BASE+60),iso_url(BASE+HOUR+60),iso_url(BASE),iso_url(BASE+HOUR));
        let page = files_page(&fake, &query).unwrap();
        assert_eq!(page["complete"], true);
        assert_eq!(page["changes"].as_array().unwrap().len(), 0);
        assert_eq!(page["files"].as_array().unwrap().len(), 0);
        assert_eq!(page["summary_partitions"].as_array().unwrap().len(), 1);
        let summaries = page["summary_files"].as_array().unwrap();
        assert_eq!(summaries.len(), 3);
        assert!(summaries.iter().all(|file| file["kind"] == "window"));
        assert_eq!(
            summaries
                .iter()
                .map(|file| file["rows"].as_u64().unwrap())
                .sum::<u64>(),
            63
        );
    }

    #[test]
    fn catalog_retention_preserves_a_pinned_bundle_reader_through_the_retirement_grace() {
        let day = BASE - BASE.rem_euclid(86_400);
        let fake = Fake::new(day + 2 * 86_400 - 10);
        watermark(&fake, day + 2 * 86_400 - 10);
        seed_hour(&fake, BASE, 2);
        settle(&fake);
        let query = format!(
            "view=summary&from={}&to={}",
            iso_url(day),
            iso_url(day + 86_400)
        );
        fake.new_request();
        fake.calls.set(BUDGET - 5);
        let first = api::files(&fake, &service(), &query, Instant::now()).unwrap();
        assert_eq!(first["complete"], false);
        let cursor = first["next_cursor"].as_str().unwrap();
        let canonical = one_of(&fake, "day_bundle=");
        let mut svc = service();
        svc.log_retention_days = 1;
        fake.now.set((day + 2 * 86_400 + 10) as u64);
        fake.new_request();
        ingest::retention::retention_sweep(&store(&fake, &svc), &Default::default(), 4, None)
            .unwrap();
        assert!(fake.bucket.borrow().contains_key(&canonical));
        fake.new_request();
        let pinned = api::files(
            &fake,
            &svc,
            &format!("{query}&cursor={cursor}"),
            Instant::now(),
        )
        .unwrap();
        assert_eq!(pinned["catalog_generation"], first["catalog_generation"]);
        assert_eq!(pinned["files"][0]["key"], canonical);
        let current = summary_view(&fake, day, day + 86_400, 30);
        assert!(current.is_empty());
        fake.now
            .set(fake.now.get() + ingest::compaction::DELETE_GRACE_S + 1);
        for _ in 0..10 {
            fake.new_request();
            let report = ingest::retention::retention_sweep(
                &store(&fake, &svc),
                &Default::default(),
                4,
                None,
            )
            .unwrap();
            if !report.more_pending {
                break;
            }
        }
        assert!(!fake.bucket.borrow().contains_key(&canonical));
    }

    #[test]
    fn v2_summary_catalog_serves_committed_day_coverage_and_late_windows_exactly_once() {
        let fake = Fake::new(BASE + 17 * HOUR);
        watermark(&fake, BASE + 17 * HOUR);
        seed_hour(&fake, BASE, 2);
        seed_hour(&fake, BASE + HOUR, 1);
        settle(&fake);
        let day = BASE - BASE.rem_euclid(86_400);
        let before = summary_view(&fake, day, day + 86_400, 30);
        assert_eq!(kinds(&before), ["day"]);
        assert_eq!(before.iter().map(|f| f.2).sum::<u64>(), 61);
        window(&fake, BASE + 120, 9, 7);
        let after = summary_view(&fake, day, day + 86_400, 30);
        assert_eq!(kinds(&after), ["day", "window"]);
        assert_eq!(after.iter().map(|f| f.2).sum::<u64>(), 68);
        let requests: i64 = after
            .iter()
            .flat_map(|(_, key, _)| read(&fake.bucket.borrow()[key]))
            .filter(|row| row["bundle"] == "overview")
            .map(|row| row["requests"].as_i64().unwrap())
            .sum();
        assert_eq!(requests, 68);
        let partial = summary_view(&fake, BASE + 60, BASE + HOUR, 30);
        assert_eq!(kinds(&partial), ["window", "window"]);
        assert_eq!(partial.iter().map(|f| f.2).sum::<u64>(), 28);
    }

    #[test]
    fn summary_view_uses_committed_bundles_without_duplicate_window_coverage() {
        let fake = Fake::new(BASE + 17 * HOUR);
        watermark(&fake, BASE + 17 * HOUR);
        seed_hour(&fake, BASE, 6);
        seed_hour(&fake, BASE + HOUR, 2);
        settle(&fake);
        let open = window(&fake, BASE + 16 * HOUR + 60, 0, 5);
        let day = BASE - 8 * HOUR;
        let view = summary_view(&fake, day, BASE + 17 * HOUR, 30);
        assert_eq!(kinds(&view), ["day", "window"]);
        assert!(view
            .iter()
            .any(|entry| entry.1 == one_of(&fake, "day_bundle=")));
        assert_eq!(
            view.iter().map(|entry| entry.2).sum::<u64>(),
            day_rows(&fake) + open.num_rows() as u64
        );

        // A range that starts mid-day reads the hours inside it, never the day bundle.
        let view = summary_view(&fake, BASE + HOUR, BASE + 2 * HOUR, 30);
        assert_eq!(kinds(&view), ["hour"]);
        assert!(view
            .iter()
            .all(|entry| entry.1.contains("/hour_bundle=09-")));
    }

    #[test]
    fn summary_view_and_inline_metadata_keep_late_committed_windows_exact_without_list() {
        let fake = Fake::new(BASE + 17 * HOUR);
        watermark(&fake, BASE + 17 * HOUR);
        seed_hour(&fake, BASE, 6);
        seed_hour(&fake, BASE + HOUR, 2);
        settle(&fake);
        window(&fake, BASE + 30 * 60, 0, 13);
        let late = fake
            .keys(WINDOWS)
            .into_iter()
            .find(|k| k.contains(&format!("summary={}-", BASE + 30 * 60)))
            .unwrap();
        fake.mtimes
            .borrow_mut()
            .insert(late.clone(), BASE + 20 * HOUR);
        let view = summary_view(&fake, BASE - 8 * HOUR, BASE + 16 * HOUR, 30);
        assert_eq!(kinds(&view), ["day", "window"]);
        assert!(view.iter().any(|entry| entry.1 == late));
        assert_eq!(view.iter().map(|v| v.2).sum::<u64>(), day_rows(&fake));

        // An hour cut by the range is read per window, only those overlapping it.
        let view = summary_view(&fake, BASE + 3 * 60 + 30, BASE + 2 * HOUR, 30);
        let windows: Vec<&str> = view
            .iter()
            .filter(|v| v.0 == "window" && v.1.contains("/hour=08/"))
            .map(|v| v.1.as_str())
            .collect();
        assert_eq!(view.iter().filter(|entry| entry.0 == "hour").count(), 1);
        assert!(windows.iter().all(|k| k.contains("/hour=08/")));
        assert_eq!(
            windows.len(),
            fake.keys(WINDOWS)
                .iter()
                .filter(|k| k.contains("/hour=08/summary="))
                .filter(|k| {
                    let end: i64 = k
                        .split("summary=")
                        .nth(1)
                        .unwrap()
                        .split('-')
                        .nth(1)
                        .unwrap()
                        .parse()
                        .unwrap();
                    end > BASE + 3 * 60 + 30
                })
                .count()
        );

        let view = summary_view(&fake, BASE + HOUR, BASE + HOUR + 2 * 60, 30);
        assert_eq!(kinds(&view), ["window", "window"]);
        fake.new_request();
        let inline = api::files(
            &fake,
            &service(),
            &format!(
                "summaries=1&from={}&to={}",
                iso_url(BASE + HOUR),
                iso_url(BASE + HOUR + 2 * 60)
            ),
            Instant::now(),
        )
        .unwrap();
        let summary_files = inline["summary_files"].as_array().unwrap();
        assert_eq!(summary_files.len(), 2, "{summary_files:?}");
        assert_eq!(summary_files[0]["kind"], "window");
        assert!(summary_files[0]["key"]
            .as_str()
            .unwrap()
            .contains("/hour=09/summary="));

        for k in fake.keys(WINDOWS) {
            let req_key =
                k.replacen(WINDOWS, "processed/request/", 1)
                    .replacen("/summary=", "/window=", 1);
            fake.bucket.borrow_mut().insert(req_key, vec![1]);
        }
        fake.new_request();
        let lists_before = super::lists();
        let inline2 = api::files(
            &fake,
            &service(),
            &format!(
                "summaries=1&from={}&to={}",
                iso_url(BASE + HOUR),
                iso_url(BASE + 2 * HOUR)
            ),
            Instant::now(),
        )
        .unwrap();
        let lists_used = super::lists() - lists_before;
        assert_eq!(
            lists_used, 0,
            "catalog discovery must not LIST physical objects"
        );
        let sf2 = inline2["summary_files"].as_array().unwrap();
        assert_eq!(sf2.len(), 1);
        assert_eq!(sf2[0]["kind"], "hour");
        assert!(sf2[0]["key"].as_str().unwrap().contains("/hour_bundle=09-"));

        fake.cache.borrow_mut().clear();
        fake.new_request();
        let lists_before_cold = super::lists();
        let inline3 = api::files(
            &fake,
            &service(),
            &format!(
                "summaries=1&from={}&to={}",
                iso_url(BASE + HOUR),
                iso_url(BASE + 2 * HOUR)
            ),
            Instant::now(),
        )
        .unwrap();
        let lists_used_cold = super::lists() - lists_before_cold;
        assert_eq!(
            lists_used_cold, 0,
            "cold discovery must use the same committed snapshot"
        );
        assert_eq!(inline3["summary_files"], inline2["summary_files"]);
    }

    #[test]
    fn a_closing_hour_bundles_windows_committed_after_its_open_hour_snapshot() {
        let fake = Fake::new(BASE + 30 * 60);
        for m in 0..50 {
            window(&fake, BASE + m * 30, 0, 5);
        }
        watermark(&fake, BASE + 30 * 60);
        // The open hour pre-folds part of its 50 windows and keeps its catalog snapshot.
        assert!(run(&fake).more_pending);
        for m in 50..70 {
            window(&fake, BASE + m * 30, 0, 5);
        }
        fake.now.set((BASE + 2 * HOUR) as u64);
        watermark(&fake, BASE + 2 * HOUR);
        settle(&fake);
        // Only the complete hour is ever published, not one built from the open-hour snapshot.
        assert!(one_of(&fake, "hour_bundle=").contains("hour_bundle=08-c70-"));
        assert!(fake.keys(BUNDLES).iter().all(|k| !k.contains("-c50-")));
    }

    #[test]
    fn summary_bundle_closes_an_idle_hour_once_sweep_confirms_no_pending_raw_files() {
        let fake = Fake::new(BASE + 2 * HOUR);
        seed_hour(&fake, BASE, 4);
        // Last raw key arrived mid-hour at BASE + 20 * 60; without a completed sweep, hour=08 stays
        // open even after wall-clock passes BASE + 2 * HOUR.
        fake.kv.borrow_mut().insert(
            ingest::WATERMARK_KEY.into(),
            (1, json!({"key": raw_key(BASE + 20 * 60, "zzz")})),
        );
        settle(&fake);
        assert!(of(&fake, "hour_bundle=").is_empty());

        // Once straggler sweep completes past the hour close slack, hour=08 closes and bundles.
        fake.kv.borrow_mut().insert(
            ingest::WATERMARK_KEY.into(),
            (
                2,
                json!({
                    "key": raw_key(BASE + 20 * 60, "zzz"),
                    "sweep": {"after": "", "at": BASE + 2 * HOUR}
                }),
            ),
        );
        settle(&fake);
        assert!(one_of(&fake, "hour_bundle=").contains("hour_bundle=08-c4-"));
    }

    #[test]
    fn summary_view_pages_at_hour_boundaries_within_the_call_budget() {
        let fake = Fake::new(BASE);
        let day = BASE - 8 * HOUR;
        let mut keys = BTreeSet::new();
        for i in 0..2_600 {
            let start = day + i * 30;
            let key = summary::summary_key(WINDOWS, start, start + 30, "p1s0", 1);
            fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
            keys.insert(key);
        }
        for d in 1..5 {
            let start = day + d * 24 * HOUR;
            let key = summary::summary_key(WINDOWS, start, start + 60, "p1s0", 2);
            fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
            keys.insert(key);
        }
        publish_files(&fake, &[], &keys.iter().cloned().collect::<Vec<_>>(), &[]);
        let view = summary_view(&fake, day, day + 5 * 24 * HOUR, 9);
        assert!(view.iter().all(|v| v.0 == "window"));
        let got: BTreeSet<String> = view.into_iter().map(|v| v.1).collect();
        assert_eq!(got, keys);

        fake.new_request();
        fake.calls.set(BUDGET - 3);
        let query = format!(
            "view=summary&from={}&to={}",
            iso_url(day),
            iso_url(day + HOUR)
        );
        assert_eq!(
            api::files(&fake, &service(), &query, Instant::now())
                .unwrap_err()
                .0,
            503
        );
        assert!(fake.calls.get() <= BUDGET);
        let raw = format!("from={}&to={}", iso_url(day), iso_url(day + HOUR));
        fake.new_request();
        let page = api::files(
            &fake,
            &service(),
            &format!("{raw}&view=summary&dataset=rum"),
            Instant::now(),
        );
        assert_eq!(page.unwrap_err().0, 400);
    }

    #[test]
    fn summary_view_reads_only_the_requested_committed_hour_without_listing() {
        let fake = Fake::new(BASE);
        for i in 0..1_920 {
            let start = BASE + i * 30;
            let key = summary::summary_key(WINDOWS, start, start + 30, "p1s0", 1);
            fake.bucket.borrow_mut().insert(key, vec![1]);
        }
        publish_files(&fake, &[], &fake.keys(WINDOWS), &[]);
        let before = super::lists();
        let view = summary_view(&fake, BASE + HOUR, BASE + 2 * HOUR, 30);
        assert_eq!(view.len(), 120);
        assert_eq!(super::lists() - before, 0);
    }

    #[test]
    fn summary_view_reads_immutable_catalogs_until_a_commit_moves_the_generation() {
        let fake = Fake::new(BASE + 3 * HOUR);
        let put = |start: i64| {
            let key = summary::summary_key(WINDOWS, start, start + 30, "p1s0", 1);
            fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
            publish_files(&fake, &[], &[key], &[]);
        };
        for i in 0..120 {
            put(BASE + i * 30);
        }
        let first = summary_view(&fake, BASE, BASE + 2 * HOUR, 30);
        assert_eq!(first.len(), 120);
        let before = super::lists();
        assert_eq!(summary_view(&fake, BASE, BASE + 2 * HOUR, 30), first);
        assert_eq!(super::lists() - before, 0);

        put(BASE + 45 * 60 + 15);
        assert_eq!(summary_view(&fake, BASE, BASE + 2 * HOUR, 30).len(), 121);
    }

    #[test]
    fn a_closed_hour_bundle_equals_the_summary_of_all_its_rows() {
        let fake = Fake::new(BASE + 3 * HOUR);
        watermark(&fake, BASE + 3 * HOUR);
        let batches = seed_hour(&fake, BASE, 11);
        seed_hour(&fake, BASE + 2 * HOUR, 3);
        settle(&fake);
        let key = one_of(&fake, "hour_bundle=");
        assert!(
            key.starts_with(&format!("{BUNDLES}sha256="))
                && key.contains("/year=2027/month=01/day=15/hour_bundle=08-c12-n"),
            "{key}"
        );
        let refs: Vec<&RecordBatch> = batches.iter().collect();
        let want = read(&summary::write_summary(&refs, BASE).unwrap());
        let got = read(&fake.bucket.borrow()[&key]);
        assert_same(&got, &want);
        let name = bundle::parse_name(&key).unwrap();
        assert_eq!((name.unit, name.count), (8, 12));
        assert_eq!(
            name.rows,
            batches.iter().map(|b| b.num_rows() as u64).sum::<u64>()
        );
        assert!(
            of(&fake, "hour_bundle=")
                .iter()
                .all(|k| !k.contains("hour_bundle=10")),
            "the open hour stays per-window"
        );
        assert!(of(&fake, "day_bundle=").is_empty(), "the day is still open");
        assert!(fake.keys(WINDOWS).iter().all(|k| !k.contains("partial=")));
    }

    #[test]
    fn a_resumed_build_matches_a_one_shot_merge_and_reruns_reproduce_the_same_bytes() {
        let fake = Fake::new(BASE + 3 * HOUR);
        watermark(&fake, BASE + 3 * HOUR);
        seed_hour(&fake, BASE, 50);
        let first = run(&fake);
        assert!(first.more_pending);
        assert!(
            fake.keys(WINDOWS)
                .iter()
                .any(|k| k.contains("/hour=08/partial=c")),
            "an unfinished hour leaves a partial to resume from"
        );
        assert!(settle(&fake) >= 2);
        let key = one_of(&fake, "hour_bundle=");
        assert!(fake.keys(WINDOWS).iter().all(|k| !k.contains("partial=")));
        let sources: Vec<Vec<u8>> = fake
            .keys(WINDOWS)
            .iter()
            .map(|k| fake.bucket.borrow()[k].clone())
            .collect();
        let refs: Vec<&[u8]> = sources.iter().map(Vec::as_slice).collect();
        let one_shot = bundle::merge(&refs, BASE, bundle::Unit::Hour).unwrap();
        let bytes = fake.bucket.borrow()[&key].clone();
        assert_eq!(bytes, one_shot);

        fake.kv.borrow_mut().remove(bundle::STATE_KEY);
        fake.bucket.borrow_mut().remove(&key);
        settle(&fake);
        assert_eq!(one_of(&fake, "hour_bundle="), key);
        assert_eq!(fake.bucket.borrow()[&key], bytes);
    }

    #[test]
    fn a_full_partial_defers_final_parquet_write_when_vcpu_exceeds_write_budget_and_ignores_legacy_partials(
    ) {
        let fake = Fake::new(BASE + 3 * HOUR);
        watermark(&fake, BASE + 3 * HOUR);
        seed_hour(&fake, BASE, 7);
        fake.new_request();
        fake.calls.set(BUDGET - 10);
        let prime = bundle::summary_bundle(&store(&fake, &service()), "ck", 1).unwrap();
        assert!(prime.more_pending);
        assert_eq!(prime.partials, 1);
        // The committed inventory and first fold are durable. The remaining seven folds
        // cross the 680ms write cutoff without crossing the 700ms fold cutoff.
        fake.vcpu_step.set(80);
        let first = run(&fake);
        assert!(first.more_pending);
        assert_eq!(first.partials, 1);
        assert_eq!(first.stopped, Some("vcpu"));
        let partials: Vec<String> = fake
            .keys(WINDOWS)
            .into_iter()
            .filter(|k| k.contains("/hour=08/partial=c8-"))
            .collect();
        assert_eq!(partials.len(), 1, "{partials:?}");
        assert!(of(&fake, "hour_bundle=").is_empty());

        // Turn 2 decodes partial=c8 and writes the final hour_bundle in its own execution.
        let second = run(&fake);
        assert_eq!(second.hours, 1);
        assert_eq!(second.sources, 1);
        assert!(fake.keys(WINDOWS).iter().all(|k| !k.contains("partial=")));
        let key = one_of(&fake, "hour_bundle=");
        let want = fake.bucket.borrow()[&key].clone();
        fake.vcpu_step.set(1);
        assert_eq!(run(&fake).hours, 1);

        // If a legacy non-PARTIAL_MAGIC partial= exists, summary_bundle ignores and deletes it.
        fake.kv.borrow_mut().remove(bundle::STATE_KEY);
        fake.bucket.borrow_mut().remove(&key);
        let legacy_partial = partials[0].replace("partial=c8-", "partial=c1-");
        fake.bucket
            .borrow_mut()
            .insert(legacy_partial.clone(), b"PAR1legacy".to_vec());
        settle(&fake);
        assert!(!fake.bucket.borrow().contains_key(&legacy_partial));
        assert_eq!(fake.bucket.borrow()[&key], want);
    }

    #[test]
    fn a_late_window_rebuilds_its_hour_and_day_and_retires_the_old_bundles_after_grace() {
        let fake = Fake::new(BASE + 17 * HOUR);
        watermark(&fake, BASE + 17 * HOUR);
        seed_hour(&fake, BASE, 6);
        seed_hour(&fake, BASE + HOUR, 2);
        settle(&fake);
        let old_hour = of(&fake, "hour_bundle=")
            .into_iter()
            .find(|k| k.contains("hour_bundle=08-"))
            .unwrap();
        let old_day = one_of(&fake, "day_bundle=");
        assert!(
            old_day.starts_with(&format!("{BUNDLES}sha256="))
                && old_day.contains("/year=2027/month=01/day=15/day_bundle=c2-n"),
            "{old_day}"
        );
        let day_rows = read(&fake.bucket.borrow()[&old_day]);
        assert_eq!(
            day_rows
                .iter()
                .filter(|r| r["bundle"] == "overview")
                .count(),
            2,
            "a day keeps one overview row per hour"
        );

        let late = window(&fake, BASE + 30 * 60, 0, 13);
        fake.now.set((BASE + 24 * HOUR) as u64);
        watermark(&fake, BASE + 24 * HOUR);
        settle(&fake);
        let hours: Vec<String> = of(&fake, "hour_bundle=")
            .into_iter()
            .filter(|k| k.contains("hour_bundle=08-"))
            .collect();
        assert_eq!(
            hours.len(),
            2,
            "the replaced bundle waits out the reader grace"
        );
        let new_hour = hours.iter().find(|k| **k != old_hour).unwrap().clone();
        assert!(new_hour.contains("hour_bundle=08-c8-"), "{new_hour}");
        let new_day = of(&fake, "day_bundle=")
            .into_iter()
            .find(|k| *k != old_day)
            .expect("the day bundle is rebuilt");
        let overview: i64 = read(&fake.bucket.borrow()[&new_day])
            .iter()
            .filter(|r| r["bundle"] == "overview")
            .map(|r| r["requests"].as_i64().unwrap())
            .sum();
        let old_total: i64 = day_rows
            .iter()
            .filter(|r| r["bundle"] == "overview")
            .map(|r| r["requests"].as_i64().unwrap())
            .sum();
        assert_eq!(overview, old_total + late.num_rows() as i64);

        fake.now
            .set(fake.now.get() + ingest::compaction::DELETE_GRACE_S + 1);
        settle(&fake);
        let bucket = fake.bucket.borrow();
        assert!(!bucket.contains_key(&old_hour) && !bucket.contains_key(&old_day));
        assert!(bucket.contains_key(&new_hour) && bucket.contains_key(&new_day));
        drop(bucket);
        let day = BASE - BASE.rem_euclid(86_400);
        let visible = summary_view(&fake, day, day + 86_400, 30);
        assert_eq!(kinds(&visible), ["day"]);
        assert_eq!(visible[0].1, new_day);
    }

    #[test]
    fn floor_key_survives_bundle_publish_and_cleanup() {
        let fake = Fake::new(BASE + 17 * HOUR);
        watermark(&fake, BASE + 17 * HOUR);
        let floor = raw_floor("request", BASE);
        set_floor(&fake, ingest::WATERMARK_KEY, &floor);
        seed_hour(&fake, BASE, 6);
        settle(&fake);
        let old_hour = of(&fake, "hour_bundle=")
            .into_iter()
            .find(|k| k.contains("hour_bundle=08-"))
            .unwrap();
        assert_eq!(
            floor_of(&fake, ingest::WATERMARK_KEY).as_ref(),
            Some(&floor)
        );

        window(&fake, BASE + 30 * 60, 0, 13);
        fake.now.set((BASE + 24 * HOUR) as u64);
        watermark(&fake, BASE + 24 * HOUR);
        settle(&fake);
        assert_eq!(
            floor_of(&fake, ingest::WATERMARK_KEY).as_ref(),
            Some(&floor)
        );

        fake.now
            .set(fake.now.get() + ingest::compaction::DELETE_GRACE_S + 1);
        settle(&fake);
        assert!(!fake.bucket.borrow().contains_key(&old_hour));
        assert_eq!(floor_of(&fake, ingest::WATERMARK_KEY), Some(floor));
    }

    #[test]
    fn reader_views_use_a_bundle_and_read_only_the_windows_it_does_not_cover() {
        let fake = Fake::new(BASE + 3 * HOUR);
        watermark(&fake, BASE + 3 * HOUR);
        seed_hour(&fake, BASE, 4);
        settle(&fake);
        let svc = service();
        let s = store(&fake, &svc);
        let list = |prefix: &str| s.list(prefix, None, None, 1000, None).unwrap().objects;
        let windows = list(&format!("{WINDOWS}{}", ingest::hour_dir(BASE)));
        let bundles = list(BUNDLES);
        let view = bundle::hour_view(8, &windows, &bundles);
        assert_eq!(
            view.bundle.as_deref(),
            Some(one_of(&fake, "hour_bundle=").as_str())
        );
        assert!(view.windows.is_empty());

        let late = summary::summary_key(WINDOWS, BASE + 600, BASE + 660, "p2s0", 1);
        fake.bucket.borrow_mut().insert(late.clone(), vec![0]);
        fake.mtimes
            .borrow_mut()
            .insert(late.clone(), BASE + 4 * HOUR);
        for b in &bundles {
            fake.mtimes
                .borrow_mut()
                .insert(b.key.clone(), BASE + 3 * HOUR);
        }
        let windows = list(&format!("{WINDOWS}{}", ingest::hour_dir(BASE)));
        let bundles = list(BUNDLES);
        let view = bundle::hour_view(8, &windows, &bundles);
        assert!(view.bundle.is_some());
        assert_eq!(view.windows, std::slice::from_ref(&late));

        // A window gone from the listing invalidates the bundle: read every window instead.
        let gone = windows.iter().find(|o| o.key != late).unwrap().key.clone();
        fake.bucket.borrow_mut().remove(&gone);
        let windows = list(&format!("{WINDOWS}{}", ingest::hour_dir(BASE)));
        let view = bundle::hour_view(8, &windows, &bundles);
        assert!(view.bundle.is_none());
        assert_eq!(view.windows.len(), 4);
    }

    #[test]
    fn retention_sweeps_hour_and_day_bundles_with_request_retention() {
        let fake = Fake::new(BASE + 40 * DAY);
        let day = |offset: i64| {
            let t = ingest::hour_dir(BASE + offset * DAY);
            t.rsplit_once("hour=").unwrap().0.to_owned()
        };
        let old = [
            format!(
                "{BUNDLES}{}hour_bundle=08-c1-n1-h0000000000000001.parquet",
                day(0)
            ),
            format!("{BUNDLES}{}partial=c1-n1-h0000000000000001.parquet", day(0)),
            format!(
                "{BUNDLES}{}day_bundle=c1-n1-h0000000000000001.parquet",
                day(0)
            ),
            format!(
                "{BUNDLES}sha256={}/{}day_bundle=c1-n1-h0000000000000001.parquet",
                "f".repeat(64),
                day(0)
            ),
        ];
        let fresh = [
            format!(
                "{BUNDLES}{}hour_bundle=08-c1-n1-h0000000000000001.parquet",
                day(20)
            ),
            format!(
                "{BUNDLES}{}day_bundle=c1-n1-h0000000000000001.parquet",
                day(20)
            ),
            format!(
                "{BUNDLES}sha256={}/{}day_bundle=c1-n1-h0000000000000001.parquet",
                "0".repeat(64),
                day(20)
            ),
        ];
        for key in old.iter().chain(&fresh) {
            fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
        }
        let svc = service();
        let mut report = sweep(&fake, &svc, &Default::default());
        let mut datasets = report.datasets.clone();
        while report.more_pending {
            fake.new_request();
            report = ingest::retention::retention_sweep_from(
                &store(&fake, &svc),
                &Default::default(),
                4,
                None,
                report.resume_from,
            )
            .unwrap();
            datasets.extend(report.datasets.clone());
        }
        assert!(
            datasets
                .iter()
                .any(|d| d.to_string().contains("summary_bundle")),
            "{datasets:?}"
        );
        let bucket = fake.bucket.borrow();
        for key in &old {
            assert!(!bucket.contains_key(key), "{key}");
        }
        for key in &fresh {
            assert!(bucket.contains_key(key), "{key}");
        }
    }

    #[test]
    fn the_tick_runs_summary_bundle_as_a_maintenance_stage() {
        let fake = Fake::new(BASE + 3 * HOUR);
        tick_runtime(&fake);
        watermark(&fake, BASE + 3 * HOUR);
        seed_hour(&fake, BASE, 3);
        fake.kv.borrow_mut().insert(
            "retention:last_sweep".into(),
            (1, json!({"at": BASE + 3 * HOUR, "complete": true})),
        );
        for _ in 0..16 {
            fake.now.set(fake.now.get() + 120);
            watermark(&fake, fake.now.get() as i64);
            run_tick(&fake);
        }
        let kinds: Vec<String> = rows(&fake, LogKind::CronRuns)
            .iter()
            .filter_map(|r| r["cron_kind"].as_str().map(str::to_owned))
            .collect();
        assert!(kinds.contains(&"summary_bundle".to_owned()), "{kinds:?}");
        assert!(of(&fake, "hour_bundle=")
            .iter()
            .any(|k| k.contains("hour_bundle=08-c3-")));
    }
}

/// Native decode cost per decompressed MB, for comparing converter changes (wasm differs).
/// Run with `cargo test --release --test ingest convert_cpu_per_mb -- --ignored --nocapture`.
#[test]
#[ignore]
fn convert_cpu_per_mb() {
    use ingest::convert::{columns, Kind};
    let lines: Vec<String> = (0..12_000i64)
        .map(|i| {
            let mut row = serde_json::Map::new();
            for column in columns() {
                let value = match (column.id, column.kind) {
                    ("timestamp", _) => json!(iso(BASE + i % 60)),
                    ("ts_ms", _) => json!((BASE + i % 60) * 1000 + i % 1000),
                    ("url", _) => json!(format!("/assets/{i}/image-{}.jpg?w=640&q=80", i % 97)),
                    (_, Kind::Utf8) => json!(format!("value-{}-\u{e9}", i % 113)),
                    (_, Kind::Int64) => json!(i % 5000),
                    (_, Kind::Float64) => json!((i % 1000) as f64 / 7.0),
                    (_, Kind::Bool) => json!(i % 2 == 0),
                    (_, Kind::TimestampMs) => json!(iso(BASE)),
                };
                row.insert(column.id.into(), value);
            }
            Value::Object(row).to_string()
        })
        .collect();
    let data = gz(&lines);
    let (mut decode, mut write) = (f64::MAX, f64::MAX);
    let mut decompressed = 0;
    for _ in 0..5 {
        let started = Instant::now();
        let rows = ingest::convert::convert_object(&data, 60, BASE).unwrap();
        decode = decode.min(started.elapsed().as_secs_f64() * 1000.0);
        let started = Instant::now();
        let bytes =
            ingest::convert::write_parquet(&rows.windows.values().collect::<Vec<_>>()).unwrap();
        write = write.min(started.elapsed().as_secs_f64() * 1000.0);
        decompressed = rows.decompressed;
        assert!(!bytes.is_empty());
    }
    let mb = decompressed as f64 / (1024.0 * 1024.0);
    println!(
        "{mb:.1} MiB: decode {:.2} ms/MiB, write {:.2} ms/MiB",
        decode / mb,
        write / mb
    );
}

fn worker_call(fake: &Fake, keys: &[String], stop_ms: u64) -> Value {
    fake.new_request();
    let body = json!({
        "service_id": SID, "region": "us-east-1", "bucket": "fos-abcdefghijklmnopqrst12-logs",
        "prefix": "", "plan_id": "0123456789ab", "shard": "0", "attempt": 1,
        "stream": "request", "table": "request", "schema": ingest::convert::schema_hash(),
        "window_s": 60, "keys": keys, "stop_ms": stop_ms, "created_at": BASE,
    });
    let response = ingest::worker::handle_worker_ingest(
        fake,
        &fastly_log_analytics::http::Request {
            method: "POST".into(),
            path: ingest::worker::PATH.into(),
            headers: [("x-flp-worker-key".to_owned(), "fos-secret".to_owned())].into(),
            body: body.to_string().into_bytes(),
        },
    );
    assert_eq!(response.status, 200);
    serde_json::from_slice(&response.body).unwrap()
}

#[test]
fn worker_retries_one_transient_get_or_put_honouring_retry_after() {
    let fake = Fake::new(BASE + 7200);
    let keys = [put_raw(&fake, BASE, "a", &[line(BASE, "a")])];
    let clean = worker_call(&fake, &keys, 20_000);
    assert_eq!(clean["status"], "ok", "{clean}");

    fake.transient.borrow_mut().push_back(("GET", 429, None));
    let reply = worker_call(&fake, &keys, 20_000);
    assert_eq!(reply["status"], "ok", "{reply}");
    assert_eq!(reply["calls"], clean["calls"].as_u64().unwrap() + 1);
    assert_eq!(fake.waits.borrow().len(), 1);

    fake.waits.borrow_mut().clear();
    fake.transient.borrow_mut().push_back(("PUT", 503, Some(2)));
    let reply = worker_call(&fake, &keys, 20_000);
    assert_eq!(reply["status"], "ok", "{reply}");
    assert_eq!(*fake.waits.borrow(), [2000]);

    // One spare call: a second transient answer is final.
    fake.waits.borrow_mut().clear();
    fake.transient
        .borrow_mut()
        .extend([("PUT", 503, None), ("PUT", 500, None)]);
    let reply = worker_call(&fake, &keys, 20_000);
    assert_eq!(reply["status"], "error");
    assert_eq!(reply["error"], "put_http_500");
    assert_eq!(fake.waits.borrow().len(), 1);

    // A Retry-After past the worker's stop is not waited out.
    fake.waits.borrow_mut().clear();
    fake.transient
        .borrow_mut()
        .push_back(("PUT", 503, Some(25)));
    let reply = worker_call(&fake, &keys, 20_000);
    assert_eq!(reply["error"], "put_http_503");
    assert!(fake.waits.borrow().is_empty());
}

#[test]
fn transient_worker_errors_do_not_count_toward_abandoning_the_plan() {
    let fake = Fake::new(BASE + 7200);
    let expected = seed(&fake, 1, 3);
    for _ in 0..ingest::coordinator::MAX_SHARD_FAILURES + 1 {
        fake.transient
            .borrow_mut()
            .extend([("PUT", 503, None), ("PUT", 503, None)]);
        let report = tick_later(&fake).unwrap();
        assert_eq!(report.converted, 0);
        let plan = plan(&fake).unwrap();
        assert!(plan["abandoning"].is_null(), "{plan}");
        assert!(
            plan["failures"].as_object().is_none_or(|f| f.is_empty()),
            "{plan}"
        );
    }
    assert_eq!(tick_later(&fake).unwrap().converted, 3);
    assert_eq!(processed_ips(&fake), expected);
}

#[test]
fn worker_splits_when_the_heap_nears_its_limit() {
    let fake = Fake::new(BASE + 7200);
    let a = put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    let b = put_raw(&fake, BASE + 60, "b", &[line(BASE + 60, "b")]);
    fake.heap_mib.set(Some(120));
    let reply = worker_call(&fake, &[a.clone(), b], 20_000);
    assert_eq!(reply["status"], "split", "{reply}");
    assert!(fake.keys("processed/").is_empty());
    // A lone object cannot split further, so it still converts.
    let reply = worker_call(&fake, &[a], 20_000);
    assert_eq!(reply["status"], "ok", "{reply}");
}

#[test]
fn worker_puts_carry_content_md5() {
    use base64::Engine;
    use md5::{Digest, Md5};
    let fake = Fake::new(BASE + 7200);
    let key = put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    assert_eq!(worker_call(&fake, &[key], 20_000)["status"], "ok");
    let md5s = fake.put_md5s.borrow();
    assert_eq!(md5s.len(), 3, "manifest, window file and summary");
    for (key, md5) in md5s.iter() {
        let body = &fake.bucket.borrow()[key];
        let want = base64::engine::general_purpose::STANDARD.encode(Md5::digest(body));
        assert_eq!(md5.as_deref(), Some(want.as_str()), "{key}");
    }
}

#[test]
fn per_object_bytes_are_each_objects_row_share_of_the_window_file() {
    let fake = Fake::new(BASE + 7200);
    let a = put_raw(&fake, BASE, "a", &[line(BASE, "a1")]);
    let b = put_raw(
        &fake,
        BASE,
        "b",
        &[line(BASE, "b1"), line(BASE + 1, "b2"), line(BASE + 2, "b3")],
    );
    let reply = worker_call(&fake, &[a, b], 20_000);
    assert_eq!(reply["status"], "ok", "{reply}");
    let result = &reply["result"];
    let size = result["outputs"][0]["bytes"].as_u64().unwrap();
    let shares: Vec<u64> = result["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["outputs"][0]["bytes"].as_u64().unwrap())
        .collect();
    assert_eq!(shares, [size / 4, size * 3 / 4]);
}

#[test]
fn a_shard_holds_about_24_mib_decompressed_but_a_lone_larger_object_converts() {
    let fake = Fake::new(BASE + 7200);
    let big = |at: i64, tag: &str, n: usize| -> Vec<String> {
        (0..n)
            .map(|i| line(at, &format!("{tag}{i}-{}", "x".repeat(200_000))))
            .collect()
    };
    // About 13 MiB each: together past the batch cap, each alone well under it.
    let a = put_raw(&fake, BASE, "a", &big(BASE, "a", 68));
    let b = put_raw(&fake, BASE + 60, "b", &big(BASE + 60, "b", 68));
    let reply = worker_call(&fake, &[a, b], 20_000);
    assert_eq!(reply["status"], "split", "{reply}");
    // About 30 MiB: over the batch cap, but the only object of its shard.
    let c = put_raw(&fake, BASE + 120, "c", &big(BASE + 120, "c", 155));
    let reply = worker_call(&fake, &[c], 20_000);
    assert_eq!(reply["status"], "ok", "{reply}");
}

fn custom_service() -> Service {
    let mut svc = service();
    let mut fields = json!({"groups": ["A"], "field_overrides": {}, "custom_fields": [
        {"name": "tenant", "vcl": "req.http.x-tenant", "parquet_type": "Utf8",
         "enabled": true, "committed": true},
        {"name": "score", "vcl": "req.http.x-score", "parquet_type": "Int64",
         "enabled": true, "committed": false},
        {"name": "old_flag", "vcl": "req.http.x-old", "parquet_type": "Boolean",
         "enabled": false, "committed": true},
        {"name": "draft", "vcl": "req.http.x-draft", "parquet_type": "Utf8",
         "enabled": false, "committed": false},
    ]});
    let cmcd = fastly_log_analytics::logging::cmcd::Config {
        enabled: true,
        ..Default::default()
    };
    fastly_log_analytics::logging::cmcd::reconcile_fields(&mut fields, &cmcd, None, false).unwrap();
    svc.log_fields = fields;
    svc.cmcd = cmcd;
    svc
}

fn store_service(fake: &Fake, svc: &Service) {
    fake.kv.borrow_mut().insert(
        format!("svc:{SID}"),
        (1, serde_json::to_value(svc).unwrap()),
    );
}

#[test]
fn service_schema_adds_committed_and_enabled_custom_fields_in_name_order() {
    use ingest::convert::{CustomColumn, Kind, RequestSchema};
    let base = RequestSchema::for_fields(&service().log_fields);
    assert!(base.custom().is_empty());
    assert_eq!(base.hash(), ingest::convert::schema_hash());
    let schema = RequestSchema::for_fields(&custom_service().log_fields);
    let names: Vec<&str> = schema.custom().iter().map(|c| c.name.as_str()).collect();
    let mut sorted = names.clone();
    sorted.sort_unstable();
    assert_eq!(names, sorted);
    assert!(names.contains(&"tenant") && names.contains(&"score") && names.contains(&"old_flag"));
    assert!(
        !names.contains(&"draft"),
        "a never-committed disabled field has no column"
    );
    assert_eq!(names.iter().filter(|n| n.starts_with("cmcd_")).count(), 14);
    let kind = |name: &str| {
        schema
            .custom()
            .iter()
            .find(|c| c.name == name)
            .unwrap()
            .kind
    };
    assert_eq!(kind("score"), Kind::Int64);
    assert_eq!(kind("old_flag"), Kind::Bool);
    assert_eq!(kind("cmcd_br"), Kind::Int64);
    assert_eq!(kind("cmcd_sid"), Kind::Utf8);
    assert_ne!(schema.hash(), base.hash());
    let retyped = RequestSchema::new(
        schema
            .custom()
            .iter()
            .map(|c| CustomColumn {
                name: c.name.clone(),
                kind: if c.name == "score" {
                    Kind::Utf8
                } else {
                    c.kind
                },
                slot_only: c.slot_only,
            })
            .collect(),
    );
    assert_ne!(
        retyped.hash(),
        schema.hash(),
        "a type change is a new schema"
    );
    assert_eq!(
        RequestSchema::new(schema.custom().iter().rev().cloned().collect()).hash(),
        schema.hash(),
        "field order in the config does not matter"
    );
}

#[test]
fn custom_and_cmcd_fields_are_written_as_typed_columns_and_committed() {
    let fake = Fake::new(BASE + 3600);
    let svc = custom_service();
    store_service(&fake, &svc);
    let row = json!({"timestamp": iso(BASE), "ip": "a", "status": 200, "tenant": "acme",
        "score": 42, "cmcd_br": 3000, "cmcd_sid": "s-1", "cmcd_bs": "1", "draft": "x"});
    put_raw(&fake, BASE, "a", &[row.to_string()]);
    assert_eq!(tick_service(&fake, &svc).unwrap().converted, 1);
    let schema = ingest::convert::RequestSchema::for_fields(&svc.log_fields);
    let keys = fake.keys("processed/request/");
    assert_eq!(keys.len(), 1);
    let batch = &batches(&fake.bucket.borrow()[&keys[0]])[0];
    let text = |name: &str| {
        let col = batch.column_by_name(name).unwrap();
        col.as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_owned()
    };
    let int = |name: &str| {
        let col = batch.column_by_name(name).unwrap();
        col.as_any().downcast_ref::<Int64Array>().unwrap().value(0)
    };
    assert_eq!(text("tenant"), "acme");
    assert_eq!(int("score"), 42);
    assert_eq!(int("cmcd_br"), 3000);
    assert_eq!(int("cmcd_bs"), 1);
    assert_eq!(text("cmcd_sid"), "s-1");
    assert!(batch.column_by_name("old_flag").unwrap().is_null(0));
    assert!(batch.column_by_name("draft").is_none());
    assert_eq!(plan(&fake).unwrap()["schema"], schema.hash());
    let stored = &fake.kv.borrow()[&format!("svc:{SID}")].1;
    let score = stored["log_fields"]["custom_fields"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["name"] == "score")
        .unwrap()
        .clone();
    assert_eq!(score["committed"], true, "a planned field is locked");
    let draft = stored["log_fields"]["custom_fields"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["name"] == "draft")
        .unwrap()
        .clone();
    assert_ne!(draft["committed"], true);
    assert!(stored["_audit_outbox"]
        .as_array()
        .unwrap()
        .iter()
        .any(|a| a["event"] == "custom_fields.committed"));
}

#[test]
fn a_custom_field_type_change_abandons_the_open_plan() {
    let fake = Fake::new(BASE + 7200);
    let svc = custom_service();
    store_service(&fake, &svc);
    put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    fake.worker_faults
        .borrow_mut()
        .push_back(WorkerFault::LostReply);
    tick_service(&fake, &svc).unwrap();
    let mut changed = custom_service();
    for field in changed.log_fields["custom_fields"].as_array_mut().unwrap() {
        if field["name"] == "draft" {
            field["enabled"] = true.into();
        }
    }
    store_service(&fake, &changed);
    fake.now
        .set(fake.now.get() + ingest::coordinator::LEASE_S + 1);
    tick_service(&fake, &changed).unwrap();
    assert_eq!(plan(&fake).unwrap()["abandoning"], "schema_changed");
}

#[test]
fn worker_rejects_custom_columns_that_do_not_match_the_schema_hash() {
    let fake = Fake::new(BASE + 7200);
    let key = put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
    let schema = ingest::convert::RequestSchema::for_fields(&custom_service().log_fields);
    let mut body = json!({
        "service_id": SID, "region": "us-east-1", "bucket": "fos-abcdefghijklmnopqrst12-logs",
        "prefix": "", "plan_id": "0123456789ab", "shard": "0", "attempt": 1,
        "stream": "request", "table": "request", "schema": ingest::convert::schema_hash(),
        "custom": schema.custom(), "window_s": 60, "keys": [key], "stop_ms": 20_000,
    });
    let call = |body: &Value| -> Value {
        fake.new_request();
        let response = ingest::worker::handle_worker_ingest(
            &fake,
            &fastly_log_analytics::http::Request {
                method: "POST".into(),
                path: ingest::worker::PATH.into(),
                headers: [("x-flp-worker-key".to_owned(), "fos-secret".to_owned())].into(),
                body: body.to_string().into_bytes(),
            },
        );
        serde_json::from_slice(&response.body).unwrap()
    };
    assert_eq!(call(&body)["status"], "schema_mismatch");
    body["schema"] = schema.hash().into();
    let reply = call(&body);
    assert_eq!(reply["status"], "ok", "{reply}");
    let output = reply["result"]["outputs"][0]["key"].as_str().unwrap();
    let batch = &batches(&fake.bucket.borrow()[output])[0];
    assert!(batch.column_by_name("cmcd_sid").is_some());
}

fn rum_key(minute_start: i64, name: &str) -> String {
    raw_key(minute_start, name).replacen("raw/request/", "raw/rum/", 1)
}

fn put_rum(fake: &Fake, minute_start: i64, name: &str, lines: &[String]) -> String {
    let key = rum_key(minute_start, name);
    fake.bucket.borrow_mut().insert(key.clone(), gz(lines));
    key
}

fn rum_beacon(at: i64, metric: &str, value: &str) -> String {
    json!({
        "timestamp": iso(at), "ts_ms": at * 1000 + 250, "method": "GET",
        "url": format!("/rum?rum_metric_name={metric}&rum_metric_value={value}&rum_metric_rating=good&cid=c1&rum_pathname=/home"),
        "referer": "https://ex.com/r", "city": "Paris", "country": "FR", "pop": "CDG",
        "tls": "1.3", "ttfb": 0.012, "fastly_req_id": "r1", "service_id": "",
        "rum_metric_name": "", "rum_metric_value": null, "rum_error_message": "",
        "rum_body": "",
    })
    .to_string()
}

fn rum_tick(fake: &Fake, service: &Service) -> Result<ingest::Report, String> {
    fake.new_request();
    let buffer = LogBuffer::new(SID, "req");
    let options = ingest::coordinator::Options::rum(fake);
    let report = ingest::coordinator::coordinate(
        fake,
        service,
        "ak",
        "sk",
        Instant::now(),
        &buffer,
        &options,
    );
    telemetry::flush_request_buffer(fake, &buffer);
    report
}

fn rum_service() -> Service {
    let mut svc = serde_json::to_value(service()).unwrap();
    svc["rum"] = json!({"enabled": true, "endpoint_name": "rum", "log_period": 60});
    serde_json::from_value(svc).unwrap()
}

fn strings(batch: &RecordBatch, name: &str) -> Vec<Option<String>> {
    let col = batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("{name}"))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..col.len())
        .map(|i| col.is_valid(i).then(|| col.value(i).to_owned()))
        .collect()
}

fn floats(batch: &RecordBatch, name: &str) -> Vec<Option<f64>> {
    let col = batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::Float64Array>()
        .unwrap();
    (0..col.len())
        .map(|i| col.is_valid(i).then(|| col.value(i)))
        .collect()
}

fn ints32(batch: &RecordBatch, name: &str) -> Vec<i32> {
    let col = batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::Int32Array>()
        .unwrap();
    col.values().to_vec()
}

#[test]
fn rum_beacons_split_into_vitals_and_errors_like_v3() {
    let faro = json!({
        "meta": {"browser": {"name": "Firefox", "mobile": true}, "os": {"name": "Android"},
                 "page": {"url": "https://ex.com/p/x?q=1"}},
        "measurements": [{"type": "web-vitals", "values": {"cls": 0.1, "delta": 0.1},
                          "context": {"rating": "good"}}],
        "exceptions": [{"value": "TypeError", "stacktrace": {"frames": [
            {"filename": "app.js", "lineno": 10, "colno": 5}]}}],
    });
    let mut faro_line: Value = serde_json::from_str(&rum_beacon(BASE + 7, "", "")).unwrap();
    faro_line["url"] = "/rum".into();
    faro_line["rum_body"] = faro.to_string().into();
    let mut error_line: Value = serde_json::from_str(&rum_beacon(BASE + 3, "", "")).unwrap();
    error_line["rum_error_message"] = "boom".into();
    error_line["url"] = "/rum".into();
    let mut options_line: Value = serde_json::from_str(&rum_beacon(BASE, "LCP", "1")).unwrap();
    options_line["method"] = "OPTIONS".into();
    let mut other_line: Value = serde_json::from_str(&rum_beacon(BASE, "LCP", "1")).unwrap();
    other_line["service_id"] = "OTHER".into();
    let data = gz(&[
        rum_beacon(BASE + 5, "LCP", "1234.5"),
        error_line.to_string(),
        faro_line.to_string(),
        options_line.to_string(),
        other_line.to_string(),
        json!({"method": "GET", "url": "/rum?rum_metric_name=LCP"}).to_string(),
        "{not json".into(),
    ]);
    let rows = ingest::rum::convert_object(&data, 60, BASE, 1 << 20, SID).unwrap();
    assert_eq!(
        (rows.lines_read, rows.bad_lines, rows.rows),
        (7, 2, 4),
        "OPTIONS and other-service beacons are skipped, not bad"
    );
    assert!(rows.bad_sample.is_some());
    let vitals = &rows.windows[&BASE];
    assert_eq!(vitals.schema(), ingest::rum::vitals_schema());
    assert_eq!(
        ms_column(vitals, "timestamp"),
        [Some((BASE + 5) * 1000 + 250), Some((BASE + 7) * 1000 + 250)]
    );
    assert_eq!(
        strings(vitals, "metric_name"),
        [Some("LCP".into()), Some("cls".into())]
    );
    assert_eq!(floats(vitals, "metric_value"), [Some(1234.5), Some(0.1)]);
    assert_eq!(
        strings(vitals, "metric_rating"),
        [Some("good".into()), Some("good".into())]
    );
    assert_eq!(
        strings(vitals, "pathname"),
        [Some("/home".into()), Some("/p/x".into())]
    );
    assert_eq!(
        strings(vitals, "browser"),
        [Some("Chrome".into()), Some("Firefox".into())]
    );
    assert_eq!(
        strings(vitals, "device"),
        [Some("Desktop".into()), Some("Mobile".into())]
    );
    assert_eq!(strings(vitals, "cid")[0].as_deref(), Some("c1"));
    assert_eq!(strings(vitals, "req_id")[0].as_deref(), Some("r1"));
    assert_eq!(strings(vitals, "city")[0].as_deref(), Some("Paris"));
    assert_eq!(floats(vitals, "ttfb")[0], Some(0.012));
    let errors = &rows.errors[&BASE];
    assert_eq!(errors.schema(), ingest::rum::errors_schema());
    assert_eq!(
        strings(errors, "error_message"),
        [Some("boom".into()), Some("TypeError".into())]
    );
    assert_eq!(
        strings(errors, "error_file"),
        [Some("unknown.js".into()), Some("app.js".into())]
    );
    assert_eq!(ints32(errors, "error_line"), [0, 10]);
    assert_eq!(ints32(errors, "error_col"), [0, 5]);
    assert_eq!(
        strings(errors, "pathname"),
        [Some("/r".into()), Some("/p/x".into())],
        "a beacon without rum_pathname falls back to its referer's path"
    );
}

/// One batched beacon as the lean RUM log format writes it.
fn rum_batch(at: i64, query: &str) -> String {
    json!({
        "ts_ms": at * 1000 + 250, "service_id": SID, "fastly_req_id": "r1",
        "url": format!("/rum-beacon?service_id={SID}&{query}"),
        "city": "Paris", "region": "IDF", "country": "FR", "pop": "CDG", "tls": "1.3",
        "ttfb": 0.012,
    })
    .to_string()
}

#[test]
fn one_batched_beacon_expands_into_a_row_per_metric_and_error() {
    let data = gz(&[
        rum_batch(
            BASE + 5,
            "cid=c%2C1&p=%2Fshop%2Fa%3Ab&m=LCP:1200:good,FCP:400.5:needs-improvement,ttfb:85:,event_a%3Ab:1:\
             &e=Type%7CError%3A%20x|app.js|10|5&e=boom|||",
        ),
        rum_beacon(BASE + 6, "INP", "40"),
    ]);
    let rows = ingest::rum::convert_object(&data, 60, BASE, 1 << 20, SID).unwrap();
    assert_eq!((rows.lines_read, rows.bad_lines, rows.rows), (2, 0, 7));
    let vitals = &rows.windows[&BASE];
    assert_eq!(
        strings(vitals, "metric_name"),
        ["LCP", "FCP", "ttfb", "event_a:b", "INP"].map(|s| Some(s.into()))
    );
    assert_eq!(
        floats(vitals, "metric_value"),
        [Some(1200.0), Some(400.5), Some(85.0), Some(1.0), Some(40.0)]
    );
    assert_eq!(
        strings(vitals, "metric_rating"),
        ["good", "needs-improvement", "", "", "good"].map(|s| Some(s.into()))
    );
    assert_eq!(
        strings(vitals, "pathname"),
        ["/shop/a:b", "/shop/a:b", "/shop/a:b", "/shop/a:b", "/home"].map(|s| Some(s.into()))
    );
    assert_eq!(
        strings(vitals, "cid"),
        ["c,1", "c,1", "c,1", "c,1", "c1"].map(|s| Some(s.into()))
    );
    assert_eq!(strings(vitals, "req_id")[0].as_deref(), Some("r1"));
    assert_eq!(strings(vitals, "region")[0].as_deref(), Some("IDF"));
    let errors = &rows.errors[&BASE];
    assert_eq!(
        strings(errors, "error_message"),
        [Some("Type|Error: x".into()), Some("boom".into())]
    );
    assert_eq!(
        strings(errors, "error_file"),
        [Some("app.js".into()), Some("unknown.js".into())]
    );
    assert_eq!(ints32(errors, "error_line"), [10, 0]);
    assert_eq!(ints32(errors, "error_col"), [5, 0]);
    assert_eq!(
        strings(errors, "pathname"),
        [Some("/shop/a:b".into()), Some("/shop/a:b".into())]
    );
}

#[test]
fn the_rum_stream_converts_into_both_tables_under_its_own_plan_keys() {
    let fake = Fake::new(BASE + 3600);
    let svc = rum_service();
    let mut error_line: Value = serde_json::from_str(&rum_beacon(BASE + 70, "", "")).unwrap();
    error_line["rum_error_message"] = "boom".into();
    let raw = [
        put_rum(&fake, BASE, "a", &[rum_beacon(BASE + 5, "LCP", "100")]),
        put_rum(
            &fake,
            BASE + 60,
            "b",
            &[rum_beacon(BASE + 65, "INP", "40"), error_line.to_string()],
        ),
    ];
    put_raw(&fake, BASE, "req", &[line(BASE, "q")]);
    let report = rum_tick(&fake, &svc).unwrap();
    assert_eq!((report.converted, report.rows), (2, 3));
    let vitals = fake.keys("processed/rum/client_vitals/");
    let errors = fake.keys("processed/rum/client_errors/");
    assert_eq!(
        (vitals.len(), errors.len()),
        (2, 1),
        "{vitals:?} {errors:?}"
    );
    for key in vitals.iter().chain(&errors) {
        let name = ingest::parse_processed(key).unwrap();
        assert_eq!(name.end - name.start, 60);
        assert!(
            key.contains("year=2027/month=01/day=15/hour=08/window="),
            "{key}"
        );
    }
    assert!(
        fake.keys("processed/summary/").is_empty(),
        "RUM writes no summaries"
    );
    assert!(
        fake.keys("processed/request/").is_empty(),
        "the request stream is untouched"
    );
    let kv = fake.kv.borrow();
    assert!(!kv.contains_key(ingest::PLAN_KEY) && !kv.contains_key(ingest::WATERMARK_KEY));
    assert_eq!(
        kv[&ingest::watermark_key("rum")].1["key"].as_str(),
        Some(raw[1].as_str())
    );
    assert!(kv.contains_key(&ingest::plan_key("rum")));
    drop(kv);
    let queued = deleteq(&fake);
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0]["stream"], "rum");
    assert_eq!(queued[0]["keys"], json!(raw));
    assert_eq!(rum_tick(&fake, &svc).unwrap().converted, 0);
    let deleted = delete_tick(&fake, &svc);
    assert_eq!(deleted.deleted, 2);
    assert!(fake.keys("raw/rum/").is_empty());
    assert_eq!(fake.keys("raw/request/").len(), 1);
}

#[test]
fn raw_delete_keeps_the_raw_keys_of_an_uncommitted_rum_plan() {
    let fake = Fake::new(BASE + 3600);
    let svc = rum_service();
    let key = put_rum(&fake, BASE, "a", &[rum_beacon(BASE + 5, "LCP", "100")]);
    let mut kv = fake.kv.borrow_mut();
    kv.insert(
        ingest::plan_key("rum"),
        (1, json!({"plan_id": "aaaaaaaaaaaa", "version": 2})),
    );
    kv.insert(
        ingest::watermark_key("rum"),
        (1, json!({"run_id": "bbbbbbbbbbbb", "key": ""})),
    );
    kv.insert(
        format!("{}aaaaaaaaaaaa", ingest::DELETEQ_PREFIX),
        (
            1,
            json!({"run_id": "aaaaaaaaaaaa", "stream": "rum", "keys": [key], "sizes": [1],
                   "inflight_keys": [], "held": [], "planned_at": 0, "at": 0}),
        ),
    );
    drop(kv);
    let report = delete_tick(&fake, &svc);
    assert_eq!(report.deleted, 0);
    assert_eq!(fake.keys("raw/rum/").len(), 1);
    fake.kv
        .borrow_mut()
        .get_mut(&ingest::watermark_key("rum"))
        .unwrap()
        .1["run_id"] = "aaaaaaaaaaaa".into();
    assert_eq!(delete_tick(&fake, &svc).deleted, 1);
}

#[test]
fn logging_period_changes_resume_both_streams_after_a_successful_ten_second_schedule() {
    use fastly_log_analytics::admin::schedule::{ready_period, ready_rum_period};

    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    let mut svc = rum_service();
    svc.log_period = 10;
    svc.rum.log_period = 10;
    store_service(&fake, &svc);
    run_tick(&fake);
    for kind in ["ingest", "rum"] {
        assert_eq!(
            fake.kv.borrow()["cron:state"].1["jobs"][kind]["last_success_period"],
            if kind == "rum" {
                ready_rum_period(fake.now.get(), 10)
            } else {
                ready_period(fake.now.get(), 10)
            },
            "initial successful 10s {kind} schedule"
        );
    }

    for (index, (request_period, rum_period)) in
        [(60, 300), (300, 60), (10, 10)].into_iter().enumerate()
    {
        svc.log_period = request_period;
        svc.rum.log_period = rum_period;
        store_service(&fake, &svc);
        fake.now.set(fake.now.get() + 60);
        let at = BASE + index as i64 * 60;
        let name = format!("period-{index}");
        put_raw(&fake, at, &name, &[line(at, &name)]);
        put_rum(&fake, at, &name, &[rum_beacon(at, "LCP", "100")]);
        for _ in 0..8 {
            freshness::finish_tick(&fake);
            let jobs = serde_json::to_value(
                fastly_log_analytics::admin::schedule::peek(
                    &fake,
                    fastly_log_analytics::admin::schedule::SCHED_KEY,
                )
                .unwrap()
                .mirror
                .jobs,
            )
            .unwrap();
            if jobs["ingest"]["last_success_period"]
                == ready_period(fake.now.get(), u64::from(request_period))
                && jobs["rum"]["last_success_period"]
                    == ready_rum_period(fake.now.get(), u64::from(rum_period))
            {
                break;
            }
            fake.now.set(fake.now.get() + 1);
        }
        let jobs = serde_json::to_value(
            fastly_log_analytics::admin::schedule::peek(
                &fake,
                fastly_log_analytics::admin::schedule::SCHED_KEY,
            )
            .unwrap()
            .mirror
            .jobs,
        )
        .unwrap();
        for (kind, period) in [("ingest", request_period), ("rum", rum_period)] {
            assert_eq!(
                jobs[kind]["last_success_period"],
                if kind == "rum" {
                    ready_rum_period(fake.now.get(), u64::from(period))
                } else {
                    ready_period(fake.now.get(), u64::from(period))
                },
                "{kind} must use the changed {period}s cadence"
            );
            assert_eq!(jobs[kind]["last_success_period_s"], period);
        }
        assert_eq!(processed_ips(&fake).len(), index + 1);
        assert_eq!(fake.keys("processed/rum/client_vitals/").len(), index + 1);

        let next = fake.now.get() + 1;
        let expected = fastly_log_analytics::admin::schedule::due(
            &fastly_log_analytics::admin::schedule::peek(
                &fake,
                fastly_log_analytics::admin::schedule::SCHED_KEY,
            )
            .unwrap()
            .mirror,
            next,
        );
        fake.now.set(next);
        run_tick(&fake);
        let after = serde_json::to_value(
            fastly_log_analytics::admin::schedule::peek(
                &fake,
                fastly_log_analytics::admin::schedule::SCHED_KEY,
            )
            .unwrap()
            .mirror
            .jobs,
        )
        .unwrap();
        assert_eq!(
            after["ingest"]["last_run"],
            if expected.ingest.is_some() {
                json!(next)
            } else {
                jobs["ingest"]["last_run"].clone()
            },
            "a drained stream must follow its configured polling period"
        );
        assert_eq!(
            after["rum"]["last_run"],
            if expected.rum.is_some() {
                json!(next)
            } else {
                jobs["rum"]["last_run"].clone()
            }
        );
    }
}

#[test]
fn legacy_scheduler_state_migrates_after_cache_eviction_and_a_cas_conflict() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    let mut svc = rum_service();
    svc.log_period = 10;
    svc.rum.log_period = 10;
    store_service(&fake, &svc);
    run_tick(&fake);
    {
        let mut kv = fake.kv.borrow_mut();
        let jobs = &mut kv.get_mut("cron:state").unwrap().1["jobs"];
        for kind in ["ingest", "rum"] {
            jobs[kind]
                .as_object_mut()
                .unwrap()
                .remove("last_success_period_s");
        }
    }
    fake.cache.borrow_mut().clear();
    fake.race_cron_state.set(1);
    fake.now.set(fake.now.get() + 1);
    run_tick(&fake);
    for kind in ["ingest", "rum"] {
        assert_eq!(
            fake.kv.borrow()["cron:state"].1["jobs"][kind]["last_success_period_s"],
            10,
            "{kind} must persist its cadence even after a CAS retry"
        );
    }
    fake.now.set(fake.now.get() + 1);
    run_tick(&fake);
    assert_eq!(fake.calls.get(), 0, "migration must not repeat every tick");
}

#[test]
fn rum_ingest_runs_only_when_its_own_log_period_is_due() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    let mut svc = rum_service();
    svc.rum.log_period = 300;
    store_service(&fake, &svc);
    let count = |fake: &Fake, kind: &str| {
        rows(fake, LogKind::CronRuns)
            .into_iter()
            .filter(|r| r["cron_kind"] == kind)
            .count()
    };
    run_tick(&fake);
    assert_eq!(count(&fake, "rum_ingest"), 1, "the first RUM period is due");
    let mut rum_ticks = Vec::new();
    for minute in 1..=10 {
        fake.now.set((BASE + 3600 + minute * 60) as u64);
        put_raw(
            &fake,
            BASE + minute * 60,
            "a",
            &[line(BASE + minute * 60, "q")],
        );
        let before = count(&fake, "rum_ingest");
        run_tick(&fake);
        if count(&fake, "rum_ingest") > before {
            rum_ticks.push(minute);
        }
    }
    assert_eq!(count(&fake, "request_ingest"), 11);
    assert_eq!(rum_ticks.len(), 2, "every 5 minutes: {rum_ticks:?}");
    assert_eq!(rum_ticks[1] - rum_ticks[0], 5);
}

#[test]
fn a_tick_ingests_the_rum_stream_after_requests_and_a_deferred_rum_goes_first_next() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    store_service(&fake, &rum_service());
    let rum_first = |fake: &Fake| fake.kv.borrow()["cron:state"].1["rum_first"].clone();
    let rum_rows = |fake: &Fake| -> Vec<Value> {
        rows(fake, LogKind::CronRuns)
            .into_iter()
            .filter(|r| r["cron_kind"] == "rum_ingest")
            .collect()
    };
    put_raw(&fake, BASE, "a", &[line(BASE, "q")]);
    put_rum(&fake, BASE, "a", &[rum_beacon(BASE + 5, "LCP", "100")]);
    run_tick(&fake);
    assert_eq!(processed_ips(&fake), ["q"]);
    assert!(
        fake.keys("processed/rum/").is_empty(),
        "after a converting request step only the RUM listing fits the budget"
    );
    let rum = rum_rows(&fake);
    assert!(rum.len() <= 1);
    if let Some(stage) = rum.first() {
        assert_eq!(stage["more_pending"], true);
    }
    assert_eq!(rum_first(&fake), true);

    put_raw(&fake, BASE + 60, "b", &[line(BASE + 60, "r")]);
    put_rum(
        &fake,
        BASE + 60,
        "b",
        &[rum_beacon(BASE + 65, "LCP", "100")],
    );
    fake.now.set(fake.now.get() + 120);
    run_tick(&fake);
    assert_eq!(
        fake.keys("processed/rum/client_vitals/").len(),
        2,
        "the deferred stream goes first"
    );
    assert_eq!(rum_first(&fake), false);
    fake.now.set(fake.now.get() + 120);
    freshness::finish_tick(&fake);
    assert_eq!(processed_ips(&fake), ["q", "r"]);

    put_rum(
        &fake,
        BASE + 120,
        "c",
        &[rum_beacon(BASE + 125, "LCP", "100")],
    );
    fake.now.set(fake.now.get() + 120);
    fake.elapsed_s
        .set(ingest::coordinator::DISPATCH_BY.as_secs());
    let before = rum_rows(&fake).len();
    run_tick(&fake);
    assert_eq!(
        rum_rows(&fake).len(),
        before,
        "no second step after the dispatch window"
    );
    assert_eq!(rum_first(&fake), true, "a skipped RUM step goes first next");
}

#[test]
fn zstd_positional_array_raw_logs_and_period_aligned_windows_round_trip() {
    let fake = Fake::new(BASE + 3600);
    let mut svc = service();
    svc.log_period = 300;

    let fmt = fastly_log_analytics::logging::generate_compact_log_format(&json!({
        "preset": "full",
        "fields": ingest::convert::columns()
            .iter()
            .map(|c| c.id)
            .collect::<Vec<_>>()
    }))
    .unwrap();
    assert!(fmt.starts_with("[null,") && fmt.ends_with(']'));

    let make_array_line = |ts_ms: i64, ip: &str, host: &str, status: i64| {
        let mut arr = vec![Value::Null; ingest::convert::columns().len()];
        for (idx, col) in ingest::convert::columns().iter().enumerate() {
            match col.id {
                "ts_ms" => arr[idx] = json!(ts_ms),
                "ip" => arr[idx] = json!(ip),
                "host" => arr[idx] = json!(host),
                "status" => arr[idx] = json!(status),
                "cache" => arr[idx] = json!("HIT"),
                "method" => arr[idx] = json!("GET"),
                _ => {}
            }
        }
        serde_json::to_string(&arr).unwrap()
    };

    let raw_lines = [
        make_array_line((BASE + 200) * 1000, "198.51.100.2", "b.example.com", 200),
        make_array_line((BASE + 50) * 1000, "198.51.100.1", "a.example.com", 404),
        make_array_line((BASE + 10) * 1000, "198.51.100.3", "a.example.com", 200),
    ];
    let payload = raw_lines.join("\n") + "\n";
    let zstd_bytes = zstd::stream::encode_all(payload.as_bytes(), 3).unwrap();
    let zst_key = raw_key(BASE, "batch");
    fake.bucket.borrow_mut().insert(zst_key, zstd_bytes);

    let buffer = LogBuffer::new(SID, "req");
    let report = ingest::run(&fake, &svc, "ak", "sk", Instant::now(), &buffer).unwrap();
    assert_eq!((report.rows, report.bad_lines), (3, 0));
    let keys = fake.keys("processed/request/");
    assert_eq!(keys.len(), 1);
    assert!(
        keys[0].contains(&format!("/window={}-{}-", BASE, BASE + 300)),
        "expected 300s window file, got {:?}",
        keys
    );
    let summary_keys = fake.keys("processed/summary/");
    assert_eq!(summary_keys.len(), 1);
    assert!(
        summary_keys[0].contains(&format!("/summary={}-{}-", BASE, BASE + 300)),
        "expected 300s summary file, got {:?}",
        summary_keys
    );
    // Verify low-cardinality sort order: ("a.example.com", 200) then ("a.example.com", 404) then ("b.example.com", 200)
    let ordered_ips: Vec<String> = batches(&fake.bucket.borrow()[&keys[0]])
        .iter()
        .flat_map(|b| {
            let col = b
                .column_by_name("ip")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .clone();
            (0..col.len()).map(move |i| col.value(i).to_owned())
        })
        .collect();
    assert_eq!(
        ordered_ips,
        ["198.51.100.3", "198.51.100.1", "198.51.100.2"]
    );
}

/// Renders a compact log format the way Fastly would: every `%{expr}V` becomes the value of the
/// custom field `expr` reads, or an empty string (quoted) or `null` (bare) for anything else.
fn render_compact_line(format: &str, values: &[(&str, &str)]) -> String {
    let mut line = String::new();
    let mut rest = format;
    while let Some(start) = rest.find("%{") {
        let end = start + rest[start..].find("}V").expect("unterminated expression");
        let expr = &rest[start + 2..end];
        let quoted = rest[..start].ends_with('"');
        line.push_str(&rest[..start]);
        let value = values
            .iter()
            .find(|(name, _)| expr.contains(&format!("-data:{name},")))
            .map(|(_, value)| *value);
        line.push_str(value.unwrap_or(if quoted { "" } else { "null" }));
        rest = &rest[end + 2..];
    }
    line.push_str(rest);
    line
}

/// Generates `fields`' compact log format, renders one line of it with `values`, and converts
/// it the way a worker does: with the schema rebuilt from the custom columns its shard carries.
fn convert_compact_line(fields: &Value, values: &[(&str, &str)]) -> RecordBatch {
    let format = fastly_log_analytics::logging::generate_compact_log_format(fields).unwrap();
    let line = render_compact_line(&format, values);
    serde_json::from_str::<Value>(&line).expect("rendered line is JSON");
    let zstd_bytes = zstd::stream::encode_all(format!("{line}\n").as_bytes(), 3).unwrap();
    let planned = ingest::convert::RequestSchema::for_fields(fields);
    let shard_custom = serde_json::from_value(json!(planned.custom())).unwrap();
    let schema = ingest::convert::RequestSchema::new(shard_custom);
    assert_eq!(schema.hash(), planned.hash());
    let rows = ingest::convert::convert_object_capped(
        &zstd_bytes,
        300,
        BASE,
        ingest::convert::MAX_DECOMPRESSED_BYTES,
        &schema,
    )
    .unwrap();
    assert_eq!((rows.rows, rows.bad_lines), (1, 0));
    rows.windows.into_values().next().unwrap()
}

fn text_at(batch: &RecordBatch, name: &str) -> Option<String> {
    let column = batch.column_by_name(name).unwrap();
    let column = column.as_any().downcast_ref::<StringArray>().unwrap();
    column.is_valid(0).then(|| column.value(0).to_owned())
}

fn number_at(batch: &RecordBatch, name: &str) -> Option<i64> {
    let column = batch.column_by_name(name).unwrap();
    let column = column.as_any().downcast_ref::<Int64Array>().unwrap();
    column.is_valid(0).then(|| column.value(0))
}

#[test]
fn compact_log_format_custom_fields_land_in_their_own_columns() {
    let mut fields = json!({
        "preset": "full",
        "fields": ingest::convert::columns().iter().map(|c| c.id).collect::<Vec<_>>(),
        "custom_fields": [
            {"name": "lt_run_id", "vcl": "req.http.x-lt-run-id", "parquet_type": "Utf8",
             "enabled": true, "committed": true},
            {"name": "lt_request_id", "vcl": "req.http.x-lt-request-id", "parquet_type": "Utf8",
             "enabled": true, "committed": true},
        ],
    });
    let cmcd = fastly_log_analytics::logging::cmcd::Config {
        enabled: true,
        ..Default::default()
    };
    fastly_log_analytics::logging::cmcd::reconcile_fields(&mut fields, &cmcd, None, false).unwrap();
    let batch = convert_compact_line(
        &fields,
        &[
            ("lt_run_id", "run-7"),
            ("lt_request_id", "req-9"),
            ("cmcd_sid", "sess-3"),
            ("cmcd_br", "1200"),
        ],
    );
    assert_eq!(text_at(&batch, "lt_run_id").as_deref(), Some("run-7"));
    assert_eq!(text_at(&batch, "lt_request_id").as_deref(), Some("req-9"));
    assert_eq!(text_at(&batch, "cmcd_sid").as_deref(), Some("sess-3"));
    assert_eq!(number_at(&batch, "cmcd_br"), Some(1200));
    assert_eq!(number_at(&batch, "cmcd_bl"), None);
    assert_eq!(text_at(&batch, "_source_file"), None);
    assert_eq!(number_at(&batch, "requests"), None);
}

#[test]
fn compact_log_format_custom_field_of_unsupported_type_keeps_its_slot() {
    let fields = json!({
        "preset": "full",
        "fields": ingest::convert::columns().iter().map(|c| c.id).collect::<Vec<_>>(),
        "custom_fields": [
            {"name": "event_at", "vcl": "req.http.x-event-at", "parquet_type": "TIMESTAMP",
             "enabled": true, "committed": true},
            {"name": "lt_run_id", "vcl": "req.http.x-lt-run-id", "parquet_type": "Utf8",
             "enabled": true, "committed": true},
        ],
    });
    let batch = convert_compact_line(
        &fields,
        &[("event_at", "2027-01-15T08:00:00Z"), ("lt_run_id", "run-7")],
    );
    assert!(batch.column_by_name("event_at").is_none());
    assert_eq!(text_at(&batch, "lt_run_id").as_deref(), Some("run-7"));
}

#[test]
fn rum_zstd_positional_array_and_sorted_parquet_round_trip() {
    let fake = Fake::new(BASE + 3600);
    let mut svc = rum_service();
    svc.rum.log_period = 300;

    let fmt = fastly_log_analytics::logging::rum::generate_compact_rum_log_format().unwrap();
    assert!(fmt.starts_with('[') && fmt.ends_with(']'));

    let make_rum_array = |ts_ms: i64, req_id: &str, query: &str| {
        json!([
            ts_ms,
            SID,
            req_id,
            format!("/rum-beacon?service_id={SID}&{query}"),
            "Paris",
            "IDF",
            "FR",
            "CDG",
            "1.3",
            0.012
        ])
        .to_string()
    };

    let raw_lines = [
        make_rum_array((BASE + 200) * 1000, "r-lcp", "m=LCP:1200:good"),
        make_rum_array(
            (BASE + 50) * 1000,
            "r-cls",
            "m=CLS:0.05:good&e=boom|app.js|10|2",
        ),
    ];
    let payload = raw_lines.join("\n") + "\n";
    let zstd_bytes = zstd::stream::encode_all(payload.as_bytes(), 3).unwrap();
    let zst_key = "raw/rum/year=2027/month=01/day=15/hour=08/minute=00/rum-batch.zst".to_string();
    fake.bucket.borrow_mut().insert(zst_key, zstd_bytes);

    let report = rum_tick(&fake, &svc).unwrap();
    assert_eq!((report.rows, report.bad_lines), (3, 0));
    let vitals_keys = fake.keys("processed/rum/client_vitals/");
    let errors_keys = fake.keys("processed/rum/client_errors/");
    assert_eq!(vitals_keys.len(), 1);
    assert_eq!(errors_keys.len(), 1);
    assert!(
        vitals_keys[0].contains(&format!("/window={}-{}-", BASE, BASE + 300)),
        "expected 300s RUM vitals window file, got {:?}",
        vitals_keys
    );
    let vitals_batch = &batches(&fake.bucket.borrow()[&vitals_keys[0]])[0];
    // Sorted by metric_name ascending: CLS before LCP
    assert_eq!(
        strings(vitals_batch, "metric_name"),
        [Some("CLS".into()), Some("LCP".into())]
    );
    assert_eq!(
        strings(vitals_batch, "req_id"),
        [Some("r-cls".into()), Some("r-lcp".into())]
    );
    let vitals_bytes = Bytes::from(fake.bucket.borrow()[&vitals_keys[0]].clone());
    let builder = ParquetRecordBatchReaderBuilder::try_new(vitals_bytes).unwrap();
    let rg = builder.metadata().row_group(0);
    let col_meta = |name: &str| {
        rg.columns()
            .iter()
            .find(|c| c.column_descr().name() == name)
            .unwrap()
    };
    assert!(col_meta("timestamp")
        .encodings()
        .any(|e| e == parquet::basic::Encoding::DELTA_BINARY_PACKED));
    assert!(col_meta("metric_value")
        .encodings()
        .any(|e| e == parquet::basic::Encoding::BYTE_STREAM_SPLIT));
    assert!(col_meta("ttfb")
        .encodings()
        .any(|e| e == parquet::basic::Encoding::BYTE_STREAM_SPLIT));
    assert!(col_meta("req_id").dictionary_page_offset().is_none());
}

/// Task 23: size-aware compaction of closed request hours.
mod request_compaction {
    use super::*;
    use arrow_array::BinaryArray;
    use fastly_log_analytics::ingest::compaction::{self, Report, DELETE_GRACE_S};
    use std::sync::Arc;

    const HOUR: i64 = BASE - BASE.rem_euclid(3600);
    const NOW: i64 = HOUR + 4 * 3600;
    const TARGET: u64 = 64 << 20;
    const ROOT: &str = "processed/request/";

    fn window(fake: &Fake, start: i64, ips: &[&str]) -> String {
        let lines: Vec<String> = ips
            .iter()
            .enumerate()
            .map(|(i, ip)| line(start + i as i64, ip))
            .collect();
        let rows = ingest::convert::convert_object(&gz(&lines), 60, start).unwrap();
        let bytes = ingest::convert::write_parquet(&[&rows.windows[&start]]).unwrap();
        let key = ingest::processed_key(ROOT, start, start + 60, "abcdef012345", ips.len() as u64);
        fake.bucket.borrow_mut().insert(key.clone(), bytes);
        let now = fake.now.replace(start.saturating_add(60) as u64);
        publish_files(fake, std::slice::from_ref(&key), &[], &[]);
        fake.now.set(now);
        key
    }

    /// A window file whose schema differs from the request schema: one binary column.
    fn odd_window(fake: &Fake, start: i64, values: &[Vec<u8>]) -> String {
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "blob",
            arrow_schema::DataType::Binary,
            false,
        )]));
        let column = BinaryArray::from_iter_values(values.iter());
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(column)]).unwrap();
        let props = parquet::file::properties::WriterProperties::builder()
            .set_compression(parquet::basic::Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .build();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(Vec::new(), schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        let bytes = writer.into_inner().unwrap();
        let key =
            ingest::processed_key(ROOT, start, start + 60, "abcdef012345", values.len() as u64);
        fake.bucket.borrow_mut().insert(key.clone(), bytes);
        let now = fake.now.replace(start.saturating_add(60) as u64);
        publish_files(fake, std::slice::from_ref(&key), &[], &[]);
        fake.now.set(now);
        key
    }

    fn run(fake: &Fake, target: u64) -> Report {
        fake.new_request();
        fake.vcpu.set(0);
        let service = service();
        compaction::request_compact(&store(fake, &service), "ck", target, 0).unwrap()
    }

    fn finish(fake: &Fake, target: u64) -> Report {
        for _ in 0..64 {
            let report = run(fake, target);
            if report.compacts > 0 || !report.more_pending {
                return report;
            }
        }
        panic!("resumable compaction failed to finish a bounded job");
    }

    fn compacts(fake: &Fake) -> Vec<String> {
        fake.keys(ROOT)
            .into_iter()
            .filter(|k| k.contains("/compact="))
            .collect()
    }

    fn column(bytes: &[u8], name: &str) -> Vec<String> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))
            .unwrap()
            .build()
            .unwrap();
        let mut out = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            let values = batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .clone();
            out.extend(values.iter().map(|v| v.unwrap_or_default().to_owned()));
        }
        out
    }

    fn manifest_gen(fake: &Fake) -> u64 {
        fake.kv
            .borrow()
            .get(ingest::WATERMARK_KEY)
            .and_then(|(_, v)| v["manifest_gen"].as_u64())
            .unwrap_or(0)
    }

    fn seed(fake: &Fake, count: i64) -> Vec<String> {
        (0..count)
            .map(|i| window(fake, HOUR + i * 60, &[&format!("a{i}"), &format!("b{i}")]))
            .collect()
    }

    #[test]
    fn small_sources_merge_into_shared_row_groups_without_losing_rows() {
        let fake = Fake::new(NOW);
        seed(&fake, 24);
        let report = finish(&fake, TARGET);
        assert_eq!(report.sources, 24);
        let outputs = compacts(&fake);
        assert_eq!(outputs.len(), 1);
        let bytes = fake.bucket.borrow()[&outputs[0]].clone();
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(&bytes)).unwrap();
        let metadata = reader.metadata().clone();
        assert_eq!(metadata.file_metadata().num_rows(), 48);
        // One row group per source would cap an hour's compact at the footer limit long
        // before its byte target; merged groups keep a load hour to one or two files.
        assert!(
            metadata.num_row_groups() < 6,
            "{} row groups",
            metadata.num_row_groups()
        );
        for group in metadata.row_groups() {
            assert!(!group.sorting_columns().unwrap_or(&Vec::new()).is_empty());
        }
        let mut values = column(&bytes, "ip");
        values.sort();
        let mut expected: Vec<String> = (0..24)
            .flat_map(|i| [format!("a{i}"), format!("b{i}")])
            .collect();
        expected.sort();
        assert_eq!(values, expected);
    }

    /// `count` window files of `rows` distinct clients from `from`, five shards per minute as
    /// a load hour's ingest writes them, each minute published in its own commit.
    fn load_windows(fake: &Fake, from: i64, count: i64, rows: i64) -> Vec<String> {
        let mut all = Vec::new();
        for minute in 0..(count + 4) / 5 {
            let start = from + minute * 60;
            let mut keys = Vec::new();
            for shard in 0..(count - minute * 5).min(5) {
                let lines: Vec<String> = (0..rows)
                    .map(|n| {
                        let ip = format!("10.{}.{shard}.{n}", minute % 256);
                        line(start + n % 60, &ip)
                    })
                    .collect();
                let converted = ingest::convert::convert_object(&gz(&lines), 60, start).unwrap();
                let bytes = ingest::convert::write_parquet(&[&converted.windows[&start]]).unwrap();
                let shard_id = format!("abcdef01234{shard}");
                let key = ingest::processed_key(ROOT, start, start + 60, &shard_id, rows as u64);
                fake.bucket.borrow_mut().insert(key.clone(), bytes);
                keys.push(key);
            }
            let now = fake.now.replace((start + 120) as u64);
            publish_files(fake, &keys, &[], &[]);
            fake.now.set(now);
            all.extend(keys);
        }
        all
    }

    /// Pass `pass` of maintenance turn `turn`, 20 s into the stage's 40 s of wall time.
    fn turn_pass(fake: &Fake, turn: &str, pass: u32) -> Report {
        try_turn_pass(fake, turn, pass).unwrap()
    }

    fn try_turn_pass(fake: &Fake, turn: &str, pass: u32) -> Result<Report, String> {
        fake.new_request();
        fake.vcpu.set(0);
        fake.elapsed_s.set(20);
        let service = service();
        let store = store(fake, &service);
        let turn = compaction::Turn {
            id: turn,
            pass,
            last: false,
        };
        let report = compaction::request_compact_pass(&store, "ck", TARGET, 1, Some(turn));
        fake.elapsed_s.set(0);
        report
    }

    /// A maintenance turn's chained passes hand the lease on rather than each taking it with
    /// its own state write, and a pass that won't complete its job records the checkpoint in its
    /// final state write. That leaves two passes enough calls to upload a 32-source compact.
    #[test]
    fn a_turn_uploads_a_load_hours_32_source_compact_in_two_passes() {
        let fake = Fake::new(NOW);
        load_windows(&fake, HOUR, 32, 50);
        let first = turn_pass(&fake, "turn-1", 1);
        assert_eq!((first.compacts, first.bytes_written), (0, 0), "{first:?}");
        assert!(first.more_pending);
        let second = turn_pass(&fake, "turn-1", 2);
        assert!(second.bytes_written > 0, "{second:?}");
        let third = turn_pass(&fake, "turn-1", 3);
        assert_eq!((third.compacts, third.sources), (1, 32), "{third:?}");
    }

    /// The handed-on lease admits only the same turn's next pass; another turn waits for it to
    /// lapse, which it does within the stage's wall time.
    #[test]
    fn a_handed_on_lease_admits_only_the_same_turn() {
        let fake = Fake::new(NOW);
        load_windows(&fake, HOUR, 32, 50);
        assert!(turn_pass(&fake, "turn-1", 1).more_pending);
        for (turn, pass) in [("turn-2", 2), ("turn-1", 1), ("turn-1", 3)] {
            let other = turn_pass(&fake, turn, pass);
            assert_eq!(other.stopped.as_deref(), Some("lease_held"), "{other:?}");
        }
        let state = fake.kv.borrow()[compaction::STATE_KEY].1.clone();
        let lease = state["lease_until"].as_u64().unwrap();
        assert!(lease <= fake.now.get() + 30, "{state}");
        fake.now.set(lease + 1);
        let later = turn_pass(&fake, "turn-2", 1);
        assert_eq!(later.stopped, None, "{later:?}");
        assert!(later.bytes_written > 0, "{later:?}");
    }

    /// A handed-on lease runs from when it is handed on, not from when the handing pass began.
    /// Otherwise it lapses while the next pass still works, an overlapping turn takes it, and
    /// the next pass's state write then fails with `compact_state_conflict`.
    #[test]
    fn a_handed_on_lease_covers_the_next_pass_after_a_long_handing_pass() {
        let fake = Fake::new(NOW);
        load_windows(&fake, HOUR, 32, 50);
        let began = fake.now.get();
        fake.call_ms.set(300);
        assert!(turn_pass(&fake, "turn-1", 1).more_pending);
        fake.call_ms.set(0);
        let handed = fake.now.get();
        assert!(handed >= began + 6, "pass 1 took {} s", handed - began);
        let left_by_pass_1 = fake.kv.borrow()[compaction::STATE_KEY].clone();
        // Pass 2 starts right after pass 1 with the 20 s of wall time pass 1 had left. Another
        // turn's cron arrives near the end of that wall time.
        fake.now.set(handed + 18);
        let other = turn_pass(&fake, "turn-2", 1);
        // Pass 2 read the state before that turn arrived.
        *fake.lagging_compact_state.borrow_mut() = Some(Record {
            generation: left_by_pass_1.0.to_string(),
            value: left_by_pass_1.1,
        });
        fake.now.set(handed + 1);
        let second = try_turn_pass(&fake, "turn-1", 2);
        let second = second.unwrap();
        assert_eq!(second.stopped, None, "{second:?}");
        assert!(second.bytes_written > 0, "{second:?}");
        assert_eq!(other.stopped.as_deref(), Some("lease_held"), "{other:?}");
    }

    /// While an hour still has window files to merge, new compacts take only windows; the
    /// hour's compacts are merged together once, after its windows are done. So a row is
    /// rewritten at most twice instead of once per compact the hour grows by.
    #[test]
    fn an_hours_compact_is_not_rewritten_with_each_new_batch_of_windows() {
        let fake = Fake::new(NOW);
        let windows = load_windows(&fake, HOUR, 96, 10);
        let mut rewritten = 0;
        for _ in 0..64 {
            let report = run(&fake, TARGET);
            rewritten += report.rows;
            if !report.more_pending {
                break;
            }
        }
        let outputs = compacts(&fake);
        let live: Vec<_> = fake
            .keys(ROOT)
            .into_iter()
            .filter(|key| windows.contains(key) || key.contains("/compact="))
            .collect();
        assert_eq!(outputs.len(), 4, "{live:?}");
        let query = format!("from={}&to={}", iso_url(HOUR), iso_url(HOUR + 3600));
        let listed = catalog_keys(&fake, &query).1;
        assert_eq!(listed.len(), 1, "{listed:?}");
        assert!(rewritten <= 2 * 960, "{rewritten} rows rewritten");
    }

    /// A pass that reads a job's last source completes it without first checkpointing it.
    /// Native time for each pass of a load hour's job: 32 windows of 400 distinct clients each,
    /// published in one commit as the catalog's compaction keeps them. Compute runs WebAssembly,
    /// so expect a small multiple of these numbers there.
    #[test]
    #[ignore = "timing: cargo test --release --test ingest pass_timing -- --ignored --nocapture"]
    fn pass_timing_for_a_load_hour_job() {
        let fake = Fake::new(NOW);
        load_windows(&fake, HOUR, 32, 400);
        for pass in 1..=16 {
            let began = Instant::now();
            let report = run(&fake, TARGET);
            eprintln!(
                "pass {pass}: {:.1} ms, {} sources, {} rows, {} compacts, stopped {:?}",
                began.elapsed().as_secs_f64() * 1000.0,
                report.sources,
                report.rows,
                report.compacts,
                report.stopped,
            );
            if report.compacts > 0 {
                return;
            }
        }
        panic!("the job did not finish");
    }

    #[test]
    fn floor_key_survives_compaction_and_catalog_retirement() {
        let fake = Fake::new(NOW);
        seed(&fake, 2);
        let floor = raw_floor("request", HOUR);
        set_floor(&fake, ingest::WATERMARK_KEY, &floor);
        let report = finish(&fake, TARGET);
        assert_eq!(report.sources, 2);
        assert_eq!(compacts(&fake).len(), 1);
        for _ in 0..8 {
            if !run(&fake, TARGET).more_pending {
                break;
            }
        }
        assert_eq!(
            floor_of(&fake, ingest::WATERMARK_KEY).as_ref(),
            Some(&floor)
        );

        let generation = fake.kv.borrow()[ingest::WATERMARK_KEY].0;
        fake.now.set((NOW + 30 * 86_400) as u64);
        let mut svc = service();
        svc.log_retention_days = 1;
        fake.new_request();
        assert!(ingest::catalog::retire(&store(&fake, &svc)).unwrap() > 0);
        assert!(fake.kv.borrow()[ingest::WATERMARK_KEY].0 > generation);
        assert_eq!(floor_of(&fake, ingest::WATERMARK_KEY), Some(floor));
    }

    #[test]
    fn stale_compaction_state_does_not_prevent_resuming_the_current_generation() {
        let fake = Fake::new(NOW);
        seed(&fake, 2);
        let state = serde_json::to_value(compaction::State::default()).unwrap();
        fake.kv
            .borrow_mut()
            .insert(compaction::STATE_KEY.into(), (2, state.clone()));
        *fake.stale_compact_state.borrow_mut() = Some(Record {
            generation: "1".into(),
            value: state,
        });
        let report = run(&fake, TARGET);
        assert_eq!(report.stopped, None);
        assert!(report.errors.is_empty());
        assert!(fake.kv.borrow()[compaction::STATE_KEY].0 > 2);
        let report = if report.compacts > 0 {
            report
        } else {
            finish(&fake, TARGET)
        };
        assert_eq!(report.sources, 2);
        let outputs = compacts(&fake);
        assert_eq!(outputs.len(), 1);
        let mut values = column(&fake.bucket.borrow()[&outputs[0]], "ip");
        values.sort();
        assert_eq!(values, ["a0", "a1", "b0", "b1"]);
    }

    #[test]
    fn fresh_compaction_state_preserves_an_owner_hidden_by_a_stale_lookup() {
        let fake = Fake::new(NOW);
        seed(&fake, 2);
        let active = serde_json::to_value(compaction::State {
            lease_until: NOW as u64 + 70,
            ..Default::default()
        })
        .unwrap();
        fake.kv
            .borrow_mut()
            .insert(compaction::STATE_KEY.into(), (2, active.clone()));
        *fake.stale_compact_state.borrow_mut() = Some(Record {
            generation: "1".into(),
            value: serde_json::to_value(compaction::State::default()).unwrap(),
        });
        let before = fake.bucket.borrow().clone();
        let report = run(&fake, TARGET);
        assert!(report.more_pending);
        assert_eq!(report.stopped.as_deref(), Some("lease_held"));
        assert_eq!(fake.kv.borrow()[compaction::STATE_KEY], (2, active));
        assert_eq!(*fake.bucket.borrow(), before);
    }

    #[test]
    fn unavailable_fresh_compaction_state_fails_before_lease_or_object_mutation() {
        let fake = Fake::new(NOW);
        seed(&fake, 2);
        let before = fake.bucket.borrow().clone();
        fake.fail_fresh_compact_state.set(true);
        fake.new_request();
        let error =
            compaction::request_compact(&store(&fake, &service()), "ck", TARGET, 0).unwrap_err();
        assert_eq!(error, "injected_fresh_compact_state_failure");
        assert!(!fake.kv.borrow().contains_key(compaction::STATE_KEY));
        assert_eq!(*fake.bucket.borrow(), before);
        assert!(fake.uploads.borrow().is_empty());
    }

    #[test]
    fn compaction_state_conflict_before_lease_is_deferred_without_io() {
        let fake = Fake::new(NOW);
        seed(&fake, 2);
        let state = compaction::State {
            upload: Some(compaction::Upload {
                key: format!("{ROOT}prior.parquet"),
                id: "prior-upload".into(),
            }),
            ..Default::default()
        };
        let state = serde_json::to_value(state).unwrap();
        fake.kv
            .borrow_mut()
            .insert(compaction::STATE_KEY.into(), (1, state.clone()));
        fake.uploads.borrow_mut().insert(
            "prior-upload".into(),
            (format!("{ROOT}prior.parquet"), BTreeMap::new()),
        );
        let bucket_before = fake.bucket.borrow().clone();
        let uploads_before = fake.uploads.borrow().clone();
        fake.compact_conflict_after.set(Some(0));
        fake.new_request();
        let report =
            compaction::request_compact(&store(&fake, &service()), "ck", TARGET, 0).unwrap();
        assert!(report.more_pending);
        assert!(report.errors.is_empty());
        let value = serde_json::to_value(&report).unwrap();
        assert_eq!(value["stopped"], "lease_conflict");
        assert_eq!(value["lease_generation"], "1");
        assert_eq!(report.compacts, 0);
        assert_eq!(*fake.bucket.borrow(), bucket_before);
        assert_eq!(*fake.uploads.borrow(), uploads_before);
        assert_eq!(fake.kv.borrow()[compaction::STATE_KEY], (2, state));
        assert_eq!(finish(&fake, TARGET).sources, 2);
        let outputs = compacts(&fake);
        assert_eq!(outputs.len(), 1);
        assert_eq!(column(&fake.bucket.borrow()[&outputs[0]], "ip").len(), 4);
    }

    #[test]
    fn compaction_state_conflict_after_lease_still_fails() {
        let fake = Fake::new(NOW);
        fake.kv.borrow_mut().insert(
            compaction::STATE_KEY.into(),
            (
                1,
                serde_json::to_value(compaction::State::default()).unwrap(),
            ),
        );
        fake.compact_conflict_after.set(Some(1));
        fake.foreign_compact_conflict.set(true);
        fake.new_request();
        let error =
            compaction::request_compact(&store(&fake, &service()), "ck", TARGET, 0).unwrap_err();
        assert_eq!(error, "compact_state_conflict");
        assert_eq!(fake.kv.borrow()[compaction::STATE_KEY].0, 3);
    }

    #[test]
    fn a_lagging_read_of_the_pass_own_state_write_is_not_a_conflict() {
        let fake = Fake::new(NOW);
        seed(&fake, 2);
        fake.lag_compact_reads.set(true);
        let report = finish(&fake, TARGET);
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(report.sources, 2);
        let state = fake.kv.borrow()[compaction::STATE_KEY].1.clone();
        assert_eq!(state["lease_until"], 0, "the pass released its lease");
    }

    #[test]
    fn adaptive_policy_compacts_current_hour_and_revisits_late_inputs() {
        let fake = Fake::new(HOUR + 1800);
        seed(&fake, 2);
        assert_eq!(finish(&fake, TARGET).sources, 2);
        window(&fake, HOUR + 180, &["late-a"]);
        window(&fake, HOUR + 240, &["late-b"]);
        fake.now.set((HOUR + 3600) as u64);
        assert_eq!(
            finish(&fake, TARGET).sources,
            2,
            "late windows merge on their own"
        );
        assert_eq!(compacts(&fake).len(), 2);
        fake.now.set((HOUR + 3600) as u64 + compaction::MIN_AGE_S);
        assert_eq!(
            finish(&fake, TARGET).sources,
            2,
            "the closed hour's compacts merge"
        );
        assert_eq!(compacts(&fake).len(), 3);
        let query = format!("from={}&to={}", iso_url(HOUR), iso_url(HOUR + 3600));
        let active = catalog_keys(&fake, &query).1;
        assert_eq!(active.len(), 1);
        assert_eq!(
            column(&fake.bucket.borrow()[active.first().unwrap()], "ip").len(),
            6
        );
    }

    #[test]
    fn compaction_cpu_admission_does_not_bypass_the_first_job() {
        let fake = Fake::new(NOW);
        seed(&fake, 2);
        fake.vcpu_step.set(45);
        let report = run(&fake, TARGET);
        assert_eq!(report.compacts, 0);
        assert!(report.more_pending);
        assert_eq!(report.stopped.as_deref(), Some("vcpu_limit"));
        assert!(compacts(&fake).is_empty());
    }

    #[test]
    fn compaction_cpu_budget_excludes_vcpu_spent_before_the_pass() {
        // Live workers reach the pass with 45-60 ms of execution vCPU already spent.
        let fake = Fake::new(NOW);
        seed(&fake, 2);
        fake.vcpu_step.set(0);
        let mut report = Report::default();
        for _ in 0..16 {
            fake.new_request();
            fake.vcpu.set(60);
            report =
                compaction::request_compact(&store(&fake, &service()), "ck", TARGET, 0).unwrap();
            if report.compacts > 0 || !report.more_pending {
                break;
            }
        }
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(report.compacts, 1, "stopped: {:?}", report.stopped);
        assert_eq!(compacts(&fake).len(), 1);
    }

    #[test]
    fn interrupted_compaction_resumes_without_reading_checkpointed_sources() {
        let fake = Fake::new(NOW);
        // Two 6 MiB sources fill the pass's source-byte budget, so the third waits for a resume.
        let sources = (0..3)
            .map(|i| odd_window(&fake, HOUR + i * 60, &[vec![i as u8; 6 << 20]]))
            .collect::<Vec<_>>();
        fake.vcpu_step.set(3);
        let first = run(&fake, TARGET);
        assert_eq!(first.compacts, 0);
        assert!(first.more_pending);
        let state = fake.kv.borrow()[compaction::STATE_KEY].1.clone();
        let completed = state["job"]["next"].as_u64().unwrap() as usize;
        assert!((1..sources.len()).contains(&completed));
        assert!(state["job"]["checkpoint"].is_string());
        let checkpoint = state["job"]["checkpoint"].as_str().unwrap();
        assert!(fake.bucket.borrow()[checkpoint].len() > 2 * 1024 * 1024);
        assert_eq!(fake.uploads.borrow().len(), 1);
        fake.vcpu_step.set(0);
        fake.cdn_gets.borrow_mut().clear();
        let next = run(&fake, TARGET);
        assert!(next.errors.is_empty(), "{:?}", next.errors);
        assert_eq!(next.compacts, 1);
        for key in &sources[..completed] {
            assert!(
                !fake.cdn_gets.borrow().contains(key),
                "checkpointed source was reread: {key}"
            );
        }
        let key = &compacts(&fake)[0];
        let batches = batches(&fake.bucket.borrow()[key]);
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
        let blobs = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .iter()
                    .map(|value| value.unwrap().to_vec())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            blobs,
            (0..3).map(|i| vec![i as u8; 6 << 20]).collect::<Vec<_>>()
        );
        assert!(fake.uploads.borrow().is_empty());
        assert!(fake.calls.get() <= 32);
    }

    #[test]
    fn missing_work_checkpoint_aborts_explicitly_without_publishing_or_deleting_sources() {
        let fake = Fake::new(NOW);
        let sources = seed(&fake, 4);
        fake.vcpu_step.set(3);
        assert!(run(&fake, TARGET).more_pending);
        let key = fake.kv.borrow()[compaction::STATE_KEY].1["job"]["checkpoint"]
            .as_str()
            .unwrap()
            .to_owned();
        fake.bucket.borrow_mut().remove(&key);
        fake.vcpu_step.set(0);
        let report = run(&fake, TARGET);
        assert_eq!(report.compacts, 0);
        assert!(report
            .errors
            .iter()
            .any(|e| e == "compact_checkpoint_missing"));
        assert!(fake.uploads.borrow().is_empty());
        assert!(compacts(&fake).is_empty());
        assert!(sources
            .iter()
            .all(|key| fake.bucket.borrow().contains_key(key)));
        assert_eq!(manifest_gen(&fake), 0);
    }

    #[test]
    fn a_missing_upload_restarts_from_committed_sources_without_losing_rows() {
        for final_part in [false, true] {
            let fake = Fake::new(NOW);
            let sources = if final_part {
                seed(&fake, 3)
            } else {
                (0..3)
                    .map(|i| odd_window(&fake, HOUR + i * 60, &[vec![i as u8; 6 << 20]]))
                    .collect::<Vec<_>>()
            };
            if final_part {
                fake.transient.borrow_mut().push_back(("PART", 503, None));
                let mut failure = None;
                for _ in 0..8 {
                    fake.new_request();
                    match compaction::request_compact(&store(&fake, &service()), "ck", TARGET, 0) {
                        Err(error) => {
                            failure = Some(error);
                            break;
                        }
                        Ok(report) => assert!(report.more_pending),
                    }
                }
                assert_eq!(failure.as_deref(), Some("multipart_part_http_503"));
            } else {
                fake.vcpu_step.set(3);
                assert!(run(&fake, TARGET).more_pending);
            }
            let job = fake.kv.borrow()[compaction::STATE_KEY].1["job"].clone();
            assert_eq!(job["next"].as_u64() == Some(3), final_part);
            let checkpoint = job["checkpoint"].as_str().unwrap().to_owned();
            fake.uploads.borrow_mut().clear();
            fake.now.set(fake.now.get() + 70);
            fake.vcpu_step.set(0);
            let report = run(&fake, TARGET);
            assert_eq!(report.compacts, 0);
            assert!(report.more_pending);
            assert!(report.errors.is_empty(), "{:?}", report.errors);
            assert_eq!(manifest_gen(&fake), 0);
            assert!(compacts(&fake).is_empty());
            assert!(sources
                .iter()
                .all(|key| fake.bucket.borrow().contains_key(key)));
            let state = fake.kv.borrow()[compaction::STATE_KEY].1.clone();
            assert!(state["job"].is_null());
            assert_eq!(state["skip"], json!([]));
            assert!(fake.calls.get() <= 32);

            assert_eq!(finish(&fake, TARGET).rows, if final_part { 6 } else { 3 });
            let query = format!("from={}&to={}", iso_url(HOUR), iso_url(HOUR + 3600));
            let active = catalog_keys(&fake, &query).1;
            assert_eq!(active.len(), 1);
            let output = fake.bucket.borrow()[active.first().unwrap()].clone();
            if final_part {
                assert_eq!(column(&output, "ip"), ["a0", "b0", "a1", "b1", "a2", "b2"]);
            } else {
                let values = batches(&output)
                    .iter()
                    .flat_map(|batch| {
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<BinaryArray>()
                            .unwrap()
                            .iter()
                            .map(|value| value.unwrap().to_vec())
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>();
                assert_eq!(values, (0..3).map(|i| vec![i; 6 << 20]).collect::<Vec<_>>());
            }
            run(&fake, TARGET);
            assert!(!fake.bucket.borrow().contains_key(&checkpoint));
            assert!(fake.uploads.borrow().is_empty());
        }
    }

    #[test]
    fn an_unclassified_part_404_keeps_the_checkpoint_and_fails_explicitly() {
        let fake = Fake::new(NOW);
        seed(&fake, 3);
        fake.transient.borrow_mut().push_back(("PART", 404, None));
        let mut failure = None;
        for _ in 0..8 {
            fake.new_request();
            match compaction::request_compact(&store(&fake, &service()), "ck", TARGET, 0) {
                Err(error) => {
                    failure = Some(error);
                    break;
                }
                Ok(report) => assert!(report.more_pending),
            }
        }
        assert_eq!(failure.as_deref(), Some("multipart_part_http_404"));
        assert!(fake.kv.borrow()[compaction::STATE_KEY].1["job"]["checkpoint"].is_string());
        assert_eq!(fake.uploads.borrow().len(), 1);
        assert_eq!(manifest_gen(&fake), 0);
    }

    #[test]
    fn a_missing_range_copy_upload_does_not_skip_the_valid_source() {
        let fake = Fake::new(NOW);
        odd_window(&fake, HOUR, &[vec![1; 10 << 20]]);
        odd_window(&fake, HOUR + 60, &[vec![2; 10 << 20]]);
        assert_eq!(finish(&fake, TARGET).rows, 2);
        let source = compacts(&fake)[0].clone();
        odd_window(&fake, HOUR + 120, &[vec![3; 10 << 20]]);
        fake.now.set((NOW as u64) + compaction::MIN_AGE_S);
        fake.vcpu_step.set(3);
        let mut interrupted = false;
        for _ in 0..16 {
            run(&fake, TARGET);
            let job = fake.kv.borrow()[compaction::STATE_KEY].1["job"].clone();
            if job["copying"]["footer"].is_string() {
                interrupted = true;
                break;
            }
        }
        assert!(interrupted);
        fake.uploads.borrow_mut().clear();
        fake.vcpu_step.set(0);
        let report = run(&fake, TARGET);
        assert!(report.more_pending);
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        let state = fake.kv.borrow()[compaction::STATE_KEY].1.clone();
        assert!(state["job"].is_null());
        assert_eq!(state["skip"], json!([]));
        assert!(fake.bucket.borrow().contains_key(&source));
        assert_eq!(finish(&fake, TARGET).rows, 3);
        let query = format!("from={}&to={}", iso_url(HOUR), iso_url(HOUR + 3600));
        let active = catalog_keys(&fake, &query).1;
        assert_eq!(active.len(), 1);
        let output = fake.bucket.borrow()[active.first().unwrap()].clone();
        let blobs = batches(&output)
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .iter()
                    .map(|value| value.unwrap().to_vec())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            blobs,
            vec![vec![1; 10 << 20], vec![2; 10 << 20], vec![3; 10 << 20]]
        );
        assert!(fake.calls.get() <= 32);
    }

    #[test]
    fn compaction_heap_admission_leaves_sources_unchanged() {
        let fake = Fake::new(NOW);
        let sources = seed(&fake, 4);
        fake.heap_mib.set(Some(96));
        let report = run(&fake, TARGET);
        assert!(report.more_pending);
        assert_eq!(report.compacts, 0);
        assert!(fake.cdn_gets.borrow().is_empty());
        assert!(sources
            .iter()
            .all(|key| fake.bucket.borrow().contains_key(key)));
    }

    #[test]
    fn larger_compacts_merge_again_with_bounded_range_reads() {
        let fake = Fake::new(NOW);
        fake.vcpu_step.set(0);
        odd_window(&fake, HOUR, &[vec![1; 10 * 1024 * 1024]]);
        odd_window(&fake, HOUR + 60, &[vec![2; 10 * 1024 * 1024]]);
        assert_eq!(finish(&fake, TARGET).rows, 2);
        let first = compacts(&fake)[0].clone();
        assert!(fake.bucket.borrow()[&first].len() > compaction::MAX_SOURCE_BYTES as usize);
        odd_window(&fake, HOUR + 120, &[vec![3; 10 * 1024 * 1024]]);
        fake.now.set((NOW as u64) + compaction::MIN_AGE_S);
        assert_eq!(finish(&fake, TARGET).rows, 3);
        let query = format!("from={}&to={}", iso_url(HOUR), iso_url(HOUR + 3600));
        let active = catalog_keys(&fake, &query).1;
        assert_eq!(active.len(), 1);
        let bytes = fake.bucket.borrow()[active.first().unwrap()].clone();
        assert_eq!(
            batches(&bytes)
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>(),
            3
        );
        assert!(fake.calls.get() <= 32);
    }

    #[test]
    fn an_open_hours_compacts_merge_together_once_they_pile_up() {
        let fake = Fake::new(NOW);
        fake.vcpu_step.set(0);
        // `NOW` is on the hour: this hour is still open for the whole test.
        let hour = NOW;
        let mut second = 0;
        for round in 0..compaction::COMPACT_TRIGGER {
            for _ in 0..compaction::FILE_TRIGGER {
                odd_window(&fake, hour + second, &[vec![round as u8; 16]]);
                second += 1;
            }
            fake.now.set((hour + second + 60) as u64);
            assert!(
                finish(&fake, TARGET).compacts >= 1,
                "round {round} merges its windows"
            );
        }
        let query = format!("from={}&to={}", iso_url(hour), iso_url(hour + 3600));
        assert_eq!(
            catalog_keys(&fake, &query).1.len(),
            compaction::COMPACT_TRIGGER
        );
        assert!(
            ((fake.now.get() as i64) + compaction::MIN_AGE_S as i64)
                < hour + 3600 + compaction::MIN_AGE_S as i64,
            "hour still open"
        );
        let report = finish(&fake, TARGET);
        assert_eq!(report.compacts, 1);
        assert_eq!(
            report.rows,
            (compaction::COMPACT_TRIGGER * compaction::FILE_TRIGGER) as u64
        );
        assert_eq!(catalog_keys(&fake, &query).1.len(), 1);
    }

    #[test]
    fn a_failed_range_copy_part_keeps_every_unacknowledged_byte_for_retry() {
        let fake = Fake::new(NOW);
        fake.vcpu_step.set(0);
        odd_window(&fake, HOUR, &[vec![1; 10 << 20]]);
        odd_window(&fake, HOUR + 60, &[vec![2; 10 << 20]]);
        finish(&fake, TARGET);
        odd_window(&fake, HOUR + 120, &[vec![3; 10 << 20]]);
        fake.now.set((NOW as u64) + compaction::MIN_AGE_S);
        fake.transient.borrow_mut().push_back(("PART", 503, None));
        let report = finish(&fake, TARGET);
        assert_eq!(report.rows, 3);
        assert!(fake.transient.borrow().is_empty());
        let query = format!("from={}&to={}", iso_url(HOUR), iso_url(HOUR + 3600));
        let active = catalog_keys(&fake, &query).1;
        assert_eq!(active.len(), 1);
        let actual = batches(&fake.bucket.borrow()[active.first().unwrap()]);
        let blobs = actual
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .iter()
                    .map(|value| value.unwrap().to_vec())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            blobs,
            vec![vec![1; 10 << 20], vec![2; 10 << 20], vec![3; 10 << 20]]
        );
    }

    #[test]
    fn a_128_mib_target_admits_a_job_larger_than_one_execution_and_checkpoints_it() {
        let fake = Fake::new(NOW);
        fake.vcpu_step.set(0);
        for i in 0..9 {
            odd_window(&fake, HOUR + i * 60, &[vec![i as u8; 15 * 1024 * 1024]]);
        }
        let target = 128 << 20;
        let mut planned_large = false;
        let mut published = false;
        let mut executions = 0;
        for _ in 0..64 {
            let report = run(&fake, target);
            executions += 1;
            assert!(fake.calls.get() <= 32);
            assert!(fake.source_bytes.get() <= compaction::MAX_EXECUTION_SOURCE_BYTES);
            let state = fake.kv.borrow()[compaction::STATE_KEY].1.clone();
            if let Some(sources) = state["job"]["sources"].as_array() {
                let bytes = sources
                    .iter()
                    .map(|source| source["size"].as_u64().unwrap())
                    .sum::<u64>();
                assert!(bytes <= target);
                planned_large |= bytes > 100 << 20;
            }
            if report.compacts > 0 {
                assert!(report.bytes_written > 100 << 20);
                published = true;
                break;
            }
        }
        assert!(
            planned_large,
            "job admission must not charge all future reads to this execution"
        );
        assert!(published);
        assert!(executions > 1);
        let query = format!("from={}&to={}", iso_url(HOUR), iso_url(HOUR + 3600));
        let active = catalog_keys(&fake, &query).1;
        assert_eq!(active.len(), 2);
        let rows = active
            .iter()
            .map(|key| {
                batches(&fake.bucket.borrow()[key])
                    .iter()
                    .map(|batch| batch.num_rows())
                    .sum::<usize>()
            })
            .sum::<usize>();
        assert_eq!(rows, 9);
    }

    #[test]
    fn a_crash_after_catalog_cas_recovers_publication_and_original_cleanup_grace() {
        let fake = Fake::new(NOW);
        fake.vcpu_step.set(0);
        let sources = seed(&fake, 5);
        fake.fail_compact_after_publish.set(true);
        let mut failed = false;
        for _ in 0..32 {
            fake.new_request();
            match compaction::request_compact(&store(&fake, &service()), "ck", TARGET, 0) {
                Err(error) if error == "injected_post_publication_failure" => {
                    failed = true;
                    break;
                }
                Ok(_) => {}
                Err(error) => panic!("unexpected failure: {error}"),
            }
        }
        assert!(failed);
        let generation = fake.kv.borrow()[ingest::WATERMARK_KEY].1["catalog"]["generation"].clone();
        let query = format!("from={}&to={}", iso_url(HOUR), iso_url(HOUR + 3600));
        let committed = catalog_keys(&fake, &query).1;
        assert_eq!(committed.len(), 1);
        fake.fail_compact_after_publish.set(false);
        fake.now.set((NOW as u64) + 120);
        let recovered = run(&fake, TARGET);
        assert!(recovered.errors.is_empty());
        assert_eq!(
            fake.kv.borrow()[ingest::WATERMARK_KEY].1["catalog"]["generation"],
            generation
        );
        assert!(fake.kv.borrow()[compaction::STATE_KEY].1["job"].is_null());
        assert_eq!(
            fake.kv.borrow()[compaction::STATE_KEY].1["queue"][0]["at"],
            NOW
        );
        fake.now.set((NOW as u64) + DELETE_GRACE_S - 1);
        run(&fake, TARGET);
        assert!(sources
            .iter()
            .all(|key| fake.bucket.borrow().contains_key(key)));
        fake.now.set((NOW as u64) + DELETE_GRACE_S);
        run(&fake, TARGET);
        assert!(sources
            .iter()
            .all(|key| !fake.bucket.borrow().contains_key(key)));
        assert_eq!(catalog_keys(&fake, &query).1, committed);
    }

    #[test]
    fn retention_during_a_durable_job_abandons_obsolete_publication_and_resumes() {
        let fake = Fake::new(NOW);
        seed(&fake, 5);
        fake.vcpu_step.set(3);
        assert!(run(&fake, TARGET).more_pending);
        assert!(fake.kv.borrow()[compaction::STATE_KEY].1["job"].is_object());
        fake.vcpu_step.set(0);
        fake.now.set((NOW + 30 * 86_400) as u64);
        let mut svc = service();
        svc.log_retention_days = 1;
        fake.new_request();
        ingest::catalog::retire(&store(&fake, &svc)).unwrap();
        let mut abandoned = false;
        for _ in 0..32 {
            let report = run(&fake, TARGET);
            assert!(fake.calls.get() <= 32);
            abandoned |= report
                .errors
                .iter()
                .any(|error| error == "catalog_replacement_not_active");
            if fake.kv.borrow()[compaction::STATE_KEY].1["job"].is_null() {
                break;
            }
        }
        assert!(abandoned);
        assert!(fake.kv.borrow()[compaction::STATE_KEY].1["job"].is_null());
        let now = fake.now.get() as i64;
        window(&fake, now - 2400, &["new-a"]);
        window(&fake, now - 2340, &["new-b"]);
        assert_eq!(finish(&fake, TARGET).sources, 2);
    }

    #[test]
    fn aged_mergeable_sources_survive_catalog_page_and_execution_boundaries() {
        let fake = Fake::new(NOW);
        fake.vcpu_step.set(0);
        let a = window(&fake, HOUR, &["a"]);
        let b = window(&fake, HOUR + 60, &["b"]);
        let mut outputs = (0..1002)
            .map(|i| ingest::worker::Output {
                key: ingest::processed_key(ROOT, HOUR, HOUR + 60, &format!("full{i:04}"), 1),
                rows: 1,
                bytes: TARGET,
                ..Default::default()
            })
            .collect::<Vec<_>>();
        outputs[1] = ingest::worker::Output {
            key: a.clone(),
            rows: 1,
            bytes: fake.bucket.borrow()[&a].len() as u64,
            ..Default::default()
        };
        outputs[1001] = ingest::worker::Output {
            key: b.clone(),
            rows: 1,
            bytes: fake.bucket.borrow()[&b].len() as u64,
            ..Default::default()
        };
        publish_outputs(&fake, &outputs, &[], &[]);
        fake.now.set((NOW + compaction::MIN_AGE_S as i64) as u64);
        let report = finish(&fake, TARGET);
        assert_eq!((report.compacts, report.sources, report.rows), (1, 2, 2));
        assert!(fake.calls.get() <= 32);
    }

    #[test]
    fn compaction_paginates_beyond_one_thousand_files_without_losing_coverage() {
        let fake = Fake::new(NOW);
        fake.vcpu_step.set(0);
        let original = window(&fake, HOUR, &["x"]);
        let bytes = fake.bucket.borrow()[&original].clone();
        let mut keys = Vec::new();
        for i in 0..1004 {
            let key = ingest::processed_key(ROOT, HOUR, HOUR + 60, &format!("legacy{i:04}"), 1);
            fake.bucket.borrow_mut().insert(key.clone(), bytes.clone());
            keys.push(key);
        }
        publish_files(&fake, &keys, &[], &[]);
        fake.now.set(fake.now.get() + compaction::MIN_AGE_S);
        let mut complete = false;
        for _ in 0..500 {
            let report = run(&fake, TARGET);
            assert!(fake.calls.get() <= 32);
            if !report.more_pending {
                complete = true;
                break;
            }
        }
        assert!(
            complete,
            "the paginated inventory must make durable progress: compacts={} state={}",
            compacts(&fake).len(), fake.kv.borrow().get(compaction::STATE_KEY)
                .map(|r| json!({"job":r.1["job"]["next"],"skip":r.1["skip"].as_array().map(Vec::len),
                    "cursor":r.1["catalog_cursor"],"objects":r.1["catalog_objects"].as_array().map(Vec::len)}))
                .unwrap_or_default()
        );
        let query = format!("from={}&to={}", iso_url(HOUR), iso_url(HOUR + 3600));
        let active = catalog_keys(&fake, &query).1;
        assert!(
            active.len() <= 1005usize.div_ceil(compaction::MAX_SOURCES) + 1,
            "{} files remain after bounded-footer compaction",
            active.len()
        );
        let windows = active
            .iter()
            .filter(|key| ingest::parse_processed(key).is_some())
            .collect::<Vec<_>>();
        assert!(
            windows.len() <= 1,
            "remaining={windows:?}, skip={}",
            fake.kv.borrow()[compaction::STATE_KEY].1["skip"]
        );
        let rows = active
            .iter()
            .map(|key| {
                batches(&fake.bucket.borrow()[key])
                    .iter()
                    .map(|batch| batch.num_rows() as u64)
                    .sum::<u64>()
            })
            .sum::<u64>();
        assert_eq!(rows, 1005);
    }

    #[test]
    fn merges_a_closed_hours_small_windows_and_deletes_them_after_the_grace() {
        let fake = Fake::new(NOW);
        let sources = seed(&fake, 5);
        let open = window(&fake, NOW - 600, &["open"]);
        let report = finish(&fake, TARGET);
        assert_eq!((report.compacts, report.sources, report.rows), (1, 5, 10));
        let made = compacts(&fake);
        assert_eq!(made.len(), 1);
        assert!(made[0].starts_with(&format!("{ROOT}{}catalog1/", ingest::hour_dir(HOUR))));
        let name = fastly_log_analytics::telemetry::compact::parse_compact_name(&made[0]).unwrap();
        assert_eq!(
            (name.a, name.b, name.sources, name.rows),
            (HOUR as u64, (HOUR + 300) as u64, 5, 10)
        );
        let mut ips = column(&fake.bucket.borrow()[&made[0]], "ip");
        ips.sort();
        let mut expected: Vec<String> = (0..5)
            .flat_map(|i| [format!("a{i}"), format!("b{i}")])
            .collect();
        expected.sort();
        assert_eq!(ips, expected);
        assert_eq!(
            manifest_gen(&fake),
            1,
            "readers' hour caches are invalidated"
        );
        let query = format!("from={}&to={}", iso_url(HOUR), iso_url(NOW));
        assert_eq!(
            catalog_keys(&fake, &query).1,
            BTreeSet::from([made[0].clone(), open.clone()])
        );

        fake.now.set((NOW as u64) + DELETE_GRACE_S - 1);
        run(&fake, TARGET);
        assert!(
            sources.iter().all(|k| fake.bucket.borrow().contains_key(k)),
            "inside the grace"
        );
        fake.now.set((NOW as u64) + DELETE_GRACE_S);
        let report = run(&fake, TARGET);
        assert_eq!(report.deleted, 5);
        assert!(sources
            .iter()
            .all(|k| !fake.bucket.borrow().contains_key(k)));
        assert!(fake.bucket.borrow().contains_key(&open));
        assert!(fake.bucket.borrow().contains_key(&made[0]));
        assert_eq!(
            fake.kv.borrow()[compaction::STATE_KEY].1["queue"],
            json!([])
        );
    }

    mod committed_catalog {
        use super::*;
        use fastly_log_analytics::ingest::catalog;

        fn publish(fake: &Fake, batch: &str) -> Value {
            fake.new_request();
            let service = service();
            let previous = fake
                .admin_get(ingest::WATERMARK_KEY)
                .unwrap()
                .map(|r| r.value)
                .unwrap_or_else(|| json!({}));
            let value = catalog::prepare(
                &store(fake, &service),
                &previous,
                batch,
                "schema-v1",
                &[],
                &[],
                &[],
                &[],
            )
            .unwrap();
            let generation = fake
                .kv
                .borrow()
                .get(ingest::WATERMARK_KEY)
                .map(|r| r.0.to_string());
            assert!(fake
                .admin_cas(ingest::WATERMARK_KEY, generation.as_deref(), &value)
                .unwrap());
            value
        }

        #[test]
        fn finite_events_replay_only_committed_catalog_generations() {
            let fake = Fake::new(BASE);
            let service = service();
            let ready = catalog::events(&fake, &service, "").unwrap();
            assert!(ready.contains("retry: 5000\n"));
            assert!(ready.contains("event: ready\n"));
            let before = format!("{SID}:0");
            publish(&fake, "batch-a");
            publish(&fake, "batch-b");
            fake.new_request();
            let replay = catalog::events(&fake, &service, &format!("cursor={before}")).unwrap();
            assert!(replay.contains("event: commit\n"));
            assert!(replay.contains("\"generation\":2"));
            fake.new_request();
            let unchanged = catalog::events(&fake, &service, &format!("cursor={SID}:2")).unwrap();
            assert!(!unchanged.contains("event:"));
            assert!(unchanged.contains("retry: 5000\n"));
            fake.new_request();
            let resync = catalog::events(&fake, &service, "cursor=other-install:1").unwrap();
            assert!(resync.contains("event: resync\n"));
        }

        #[test]
        fn uncommitted_manifest_is_not_a_visible_commit_and_retry_is_immutable() {
            let fake = Fake::new(BASE);
            let service = service();
            let store = store(&fake, &service);
            let first =
                catalog::prepare(&store, &json!({}), "batch-a", "v1", &[], &[], &[], &[]).unwrap();
            let second =
                catalog::prepare(&store, &json!({}), "batch-a", "v1", &[], &[], &[], &[]).unwrap();
            assert_eq!(first, second);
            fake.new_request();
            let event = catalog::events(&fake, &service, "").unwrap();
            assert!(event.contains("\"generation\":0"));
            assert!(!event.contains("event: commit"));
        }

        #[test]
        fn events_expired_cursor_requires_resync_and_retry_tracks_logging_period() {
            let fake = Fake::new(BASE);
            for n in 0..40 {
                publish(&fake, &format!("batch-{n}"));
            }
            fake.new_request();
            let mut service = service();
            service.log_period = 3;
            let event = catalog::events(&fake, &service, &format!("cursor={SID}:0")).unwrap();
            assert!(event.contains("event: resync\n"));
            assert!(event.contains("retry: 1500\n"));
            service.log_period = 1;
            fake.new_request();
            assert!(catalog::events(&fake, &service, "")
                .unwrap()
                .contains("retry: 1000\n"));
        }

        #[test]
        fn a_transiently_stale_head_never_regresses_an_acknowledged_generation() {
            let fake = Fake::new(BASE);
            publish(&fake, "batch-a");
            fake.new_request();
            let event = catalog::events(&fake, &service(), &format!("cursor={SID}:2")).unwrap();
            assert!(!event.contains("event:"));
            assert!(event.contains("catalog head behind cursor"));
        }

        #[test]
        fn actual_ingestion_preserves_the_ordered_catalog_across_watermark_commits() {
            let fake = Fake::new(BASE + 600);
            put_raw(&fake, BASE, "a", &[line(BASE, "a")]);
            tick(&fake).unwrap();
            let first = fake.kv.borrow()[ingest::WATERMARK_KEY].1["catalog"].clone();
            assert_eq!(first["generation"], 1);
            put_raw(&fake, BASE + 60, "b", &[line(BASE + 60, "b")]);
            tick_later(&fake).unwrap();
            let next = fake.kv.borrow()[ingest::WATERMARK_KEY].1["catalog"].clone();
            assert_eq!(next["generation"], 2);
            let manifest: catalog::Manifest =
                serde_json::from_slice(&fake.bucket.borrow()[next["manifest"].as_str().unwrap()])
                    .unwrap();
            assert_eq!(manifest.previous.as_deref(), first["manifest"].as_str());
            assert_eq!(manifest.rows, 1);
            assert_eq!(manifest.files.len(), 1);
            assert_eq!(manifest.sources.len(), 1);
            assert!(!manifest.files[0].md5.is_empty());
            assert!(fake.bucket.borrow().contains_key(&manifest.files[0].key));
            let metadata = manifest.files[0]
                .metadata
                .as_ref()
                .expect("standard files retain usable metadata");
            assert!(!metadata.row_groups.is_empty());
            for column in metadata.row_groups.iter().flat_map(|group| &group.columns) {
                assert!(column.offset + column.length < manifest.files[0].bytes);
            }
        }
    }

    #[test]
    fn is_idempotent() {
        let fake = Fake::new(NOW);
        seed(&fake, 4);
        finish(&fake, TARGET);
        let after_first = fake.keys(ROOT);
        let generation = manifest_gen(&fake);
        let report = run(&fake, TARGET);
        assert_eq!(report.compacts, 0);
        assert_eq!(fake.keys(ROOT), after_first);
        assert_eq!(manifest_gen(&fake), generation);
    }

    #[test]
    fn keeps_windows_at_or_above_the_target_and_groups_up_to_it() {
        let fake = Fake::new(NOW);
        let sources = seed(&fake, 6);
        let size = fake.bucket.borrow()[&sources[0]].len() as u64;
        // Room for two windows per compact; a window at the target is never merged.
        let big = odd_window(&fake, HOUR + 3000, &[vec![0; (2 * size + 1) as usize]]);
        for _ in 0..64 {
            if !run(&fake, 2 * size + 1).more_pending {
                break;
            }
        }
        // Merged windows rewrite smaller than their sources, so compacts can merge again;
        // every active compact still stays within the target.
        let query = format!("from={}&to={}", iso_url(HOUR), iso_url(HOUR + 3600));
        let active = catalog_keys(&fake, &query).1;
        assert!(active.contains(&big));
        let made: Vec<&String> = active.iter().filter(|k| k.contains("/compact=")).collect();
        assert_eq!(active.len(), made.len() + 1);
        assert!((1..6).contains(&made.len()), "{made:?}");
        let mut ips = Vec::new();
        for key in made {
            assert!(fake.bucket.borrow()[key].len() as u64 <= 2 * size + 1);
            ips.extend(column(&fake.bucket.borrow()[key], "ip"));
        }
        assert_eq!(ips.len(), 12);
    }

    #[test]
    fn a_crash_between_the_upload_and_the_queue_is_repaired_by_the_next_run() {
        let fake = Fake::new(NOW);
        let sources = seed(&fake, 3);
        fake.fail_watermark.set(true);
        let service = service();
        let mut failed = false;
        for _ in 0..32 {
            fake.new_request();
            if compaction::request_compact(&store(&fake, &service), "ck", TARGET, 0).is_err() {
                failed = true;
                break;
            }
        }
        assert!(failed);
        assert_eq!(compacts(&fake).len(), 1);
        assert_eq!(manifest_gen(&fake), 0);
        fake.fail_watermark.set(false);
        fake.now.set(fake.now.get() + 70);
        let report = run(&fake, TARGET);
        assert_eq!(
            report.compacts, 1,
            "the completed upload is recovered without copying sources"
        );
        assert_eq!(compacts(&fake).len(), 1);
        assert_eq!(manifest_gen(&fake), 1);
        fake.now.set(fake.now.get() + DELETE_GRACE_S);
        run(&fake, TARGET);
        assert!(sources
            .iter()
            .all(|k| !fake.bucket.borrow().contains_key(k)));
    }

    #[test]
    fn watermark_conflict_after_merge_saves_completed_job_and_releases_lease_immediately() {
        let fake = Fake::new(NOW);
        seed(&fake, 3);
        fake.conflict_watermark.set(true);
        let service = service();
        let mut failed = false;
        for _ in 0..32 {
            fake.new_request();
            if let Err(err) = compaction::request_compact(&store(&fake, &service), "ck", TARGET, 0)
            {
                assert_eq!(err, "compact_watermark_conflict");
                failed = true;
                break;
            }
        }
        assert!(failed);
        let raw_state = fake.kv.borrow()[compaction::STATE_KEY].1.clone();
        let saved: compaction::State = serde_json::from_value(raw_state.clone()).unwrap();
        assert_eq!(saved.lease_until, 0, "lease released on watermark conflict");
        assert_eq!(
            raw_state["job"]["complete"],
            json!(true),
            "completed merge job preserved in state"
        );
        // Immediate retry without advancing clock or re-reading source windows commits the compact.
        fake.conflict_watermark.set(false);
        fake.cdn_gets.borrow_mut().clear();
        let report = run(&fake, TARGET);
        assert_eq!(report.compacts, 1);
        assert!(
            fake.cdn_gets.borrow().is_empty(),
            "no source re-reads needed after checkpointed watermark conflict"
        );
        assert_eq!(manifest_gen(&fake), 1);
    }

    #[test]
    fn a_window_arriving_after_its_hours_compact_stays_visible() {
        let fake = Fake::new(NOW);
        seed(&fake, 3);
        finish(&fake, TARGET);
        let late = window(&fake, HOUR + 1200, &["late"]);
        let query = format!("from={}&to={}", iso_url(HOUR), iso_url(HOUR + 3600));
        assert_eq!(
            catalog_keys(&fake, &query).1,
            BTreeSet::from([compacts(&fake)[0].clone(), late])
        );
        fake.now.set((NOW as u64) + DELETE_GRACE_S);
        finish(&fake, TARGET);
        let active = catalog_keys(&fake, &query).1;
        let ips = active
            .iter()
            .flat_map(|key| column(&fake.bucket.borrow()[key], "ip"))
            .collect::<Vec<_>>();
        assert_eq!(ips.len(), 7);
        assert_eq!(ips.iter().filter(|ip| *ip == "late").count(), 1);
    }

    #[test]
    fn a_schema_mismatch_leaves_the_odd_window_uncompacted() {
        let fake = Fake::new(NOW);
        let first = seed(&fake, 2);
        let odd = odd_window(&fake, HOUR + 150, &[vec![1, 2, 3]]);
        let rest: Vec<String> = (3..5)
            .map(|i| window(&fake, HOUR + i * 60, &[&format!("c{i}")]))
            .collect();
        for _ in 0..64 {
            if !run(&fake, TARGET).more_pending {
                break;
            }
        }
        let mut covered = first.clone();
        covered.extend(rest);
        let query = format!("from={}&to={}", iso_url(HOUR), iso_url(HOUR + 3600));
        let active = catalog_keys(&fake, &query).1;
        assert!(active.contains(&odd));
        assert!(covered.iter().all(|key| !active.contains(key)));
        assert_eq!(
            active
                .iter()
                .filter(|key| ingest::parse_processed(key).is_some())
                .cloned()
                .collect::<Vec<_>>(),
            vec![odd]
        );
        let rows: usize = compacts(&fake)
            .iter()
            .map(|k| column(&fake.bucket.borrow()[k], "ip").len())
            .sum();
        assert_eq!(rows, 6);
    }

    #[test]
    fn streams_a_large_compact_as_multipart_parts() {
        let fake = Fake::new(NOW);
        let blob = |seed: u8| -> Vec<u8> {
            (0..3 * 1024 * 1024u32)
                .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8 ^ seed)
                .collect()
        };
        let sources: Vec<String> = (0..4)
            .map(|i| odd_window(&fake, HOUR + i * 60, &[blob(i as u8)]))
            .collect();
        let report = finish(&fake, TARGET);
        assert_eq!(report.compacts, 1);
        assert_eq!(fake.upload_seq.get(), 1, "one multipart upload");
        assert!(fake.uploads.borrow().is_empty());
        let made = compacts(&fake);
        let bytes = fake.bucket.borrow()[&made[0]].clone();
        assert!(bytes.len() > 12 * 1024 * 1024);
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
            .unwrap()
            .build()
            .unwrap();
        let mut blobs = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .clone();
            blobs.extend(column.iter().map(|v| v.unwrap().to_vec()));
        }
        assert_eq!(blobs, (0..4).map(|i| blob(i as u8)).collect::<Vec<_>>());
        let query = format!("from={}&to={}", iso_url(HOUR), iso_url(HOUR + 3600));
        assert_eq!(
            catalog_keys(&fake, &query).1,
            BTreeSet::from([made[0].clone()])
        );
        assert!(sources
            .iter()
            .all(|key| fake.bucket.borrow().contains_key(key)));
    }

    fn stage_turn(fake: &Fake, start: usize, compact_only: bool) -> Value {
        let body = json!({
            "service_id": SID,
            "run_id": "run-1",
            "tick_id": "",
            "trigger": "clock",
            "actor": "system",
            "bucket": 0,
            "start": start,
            "compact_only": compact_only,
        });
        fake.new_request();
        let path = fastly_log_analytics::admin::stage::PATH.to_owned();
        fake.worker_dispatch(&[(path, body)], std::time::Duration::from_secs(30))
            .remove(0)
            .unwrap()
    }

    fn compaction_service(fake: &Fake) {
        tick_runtime(fake);
        let mut svc = serde_json::to_value(service()).unwrap();
        svc["compaction"] = json!({"mode": "size_target", "size_target_mb": 64});
        fake.kv.borrow_mut().insert(format!("svc:{SID}"), (1, svc));
    }

    /// Request compaction is its own job: a maintenance turn never runs it, whichever stage the
    /// rotation starts from, and a compact-only execution runs nothing but it.
    #[test]
    fn request_compaction_runs_on_its_own_execution_not_in_the_rotation() {
        for start in [0, 5] {
            let fake = Fake::new(NOW);
            compaction_service(&fake);
            seed(&fake, 40);
            let reply = stage_turn(&fake, start, false);
            assert_eq!(reply["ran"], "retention_sweep", "{reply}");
            assert_eq!(reply["next"], 1, "{reply}");
            assert!(compacts(&fake).is_empty(), "{reply}");
            let reply = stage_turn(&fake, start, true);
            assert_eq!(reply["ran"], "request_compact", "{reply}");
            assert!(reply.get("next").is_none(), "{reply}");
            assert!(!compacts(&fake).is_empty(), "{reply}");
            let rows = rows(&fake, LogKind::CronRuns);
            assert!(
                rows.iter().any(|row| row["cron_kind"] == "request_compact"),
                "{rows:?}"
            );
        }
    }

    /// Live KV paces writes to one key about a second apart, so the turn's passes go only as
    /// fast as their state writes. Chained passes hand the lease on, and only a pass completing
    /// its job saves the state before its final write.
    #[test]
    fn a_maintenance_turns_passes_write_the_state_about_once_each() {
        let fake = Fake::new(NOW);
        compaction_service(&fake);
        load_windows(&fake, HOUR, 128, 50);
        let reply = stage_turn(&fake, 0, true);
        let rows = rows(&fake, LogKind::CronRuns);
        let row = rows
            .iter()
            .find(|row| row["cron_kind"] == "request_compact")
            .expect("a request_compact row");
        let details: Value = serde_json::from_str(row["details_json"].as_str().unwrap()).unwrap();
        let passes = details["passes"].as_u64().unwrap() as u32;
        let jobs = compacts(&fake).len() as u32;
        assert!(passes > 1 && jobs > 1, "{reply}");
        let writes = fake.compact_state_writes.get();
        assert!(
            writes <= passes + 2 * jobs + 1,
            "{writes} writes in {passes} passes, {jobs} jobs"
        );
        let state = fake.kv.borrow()[compaction::STATE_KEY].1.clone();
        assert_eq!(
            state["lease_until"], 0,
            "the last pass released its lease: {state}"
        );
    }

    /// Every pass is its own worker execution, so the row's `vcpu_ms` is the admin execution's
    /// own time (what the per-execution budget applies to), and the passes' worker time sits in
    /// `details.worker_vcpu_ms` as the ingest rows report it.
    #[test]
    fn a_compaction_rows_vcpu_is_the_admin_executions_not_the_sum_of_its_passes() {
        let fake = Fake::new(NOW);
        compaction_service(&fake);
        load_windows(&fake, HOUR, 128, 50);
        fake.vcpu_step.set(45);
        stage_turn(&fake, 0, true);
        let rows = rows(&fake, LogKind::CronRuns);
        let row = rows
            .iter()
            .find(|row| row["cron_kind"] == "request_compact")
            .expect("a request_compact row");
        let details: Value = serde_json::from_str(row["details_json"].as_str().unwrap()).unwrap();
        let passes = details["passes"].as_u64().unwrap();
        let worker = details["worker_vcpu_ms"].as_u64().unwrap();
        let own = details["stage_execution_metrics"]["vcpu_ms"]
            .as_u64()
            .unwrap();
        assert!(passes > 1, "{details}");
        assert!(worker >= passes * 45, "{details}");
        assert_eq!(row["vcpu_ms"].as_u64().unwrap(), own, "{row}");
        assert!(row["vcpu_ms"].as_u64().unwrap() < worker, "{row}");
    }

    /// The tick dispatches the compaction job beside the maintenance stage. A run that leaves a
    /// backlog brings the job's next run forward to `COMPACT_CATCHUP_S` without touching the
    /// maintenance cadence; a caught-up job waits `COMPACT_INTERVAL_S`.
    #[test]
    fn a_compaction_backlog_brings_the_jobs_next_run_forward() {
        use fastly_log_analytics::admin::schedule::{COMPACT_CATCHUP_S, COMPACT_INTERVAL_S};
        let fake = Fake::new(NOW);
        compaction_service(&fake);
        seed(&fake, 230);
        run_tick(&fake);
        let runs = fake.stage_runs.borrow().clone();
        assert_eq!(runs.len(), 2, "a maintenance turn and a compaction run");
        assert_ne!(runs[0]["ran"], "request_compact", "{}", runs[0]);
        assert_eq!(runs[1]["ran"], "request_compact", "{}", runs[1]);
        assert_eq!(runs[1]["compact_pending"], true, "{}", runs[1]);
        let jobs = fake.kv.borrow()["cron:state"].1["jobs"].clone();
        assert_eq!(jobs["compact"]["more_pending"], true, "{jobs}");
        assert_eq!(
            jobs["compact"]["retry_after"],
            fake.now.get() + COMPACT_CATCHUP_S
        );
        assert_eq!(jobs["maint"]["more_pending"], false, "{jobs}");
        assert_eq!(jobs["maint"]["retry_after"], 0, "{jobs}");

        fake.now.set(fake.now.get() + COMPACT_CATCHUP_S);
        let made = compacts(&fake).len();
        run_tick(&fake);
        let runs = fake.stage_runs.borrow().clone();
        assert_eq!(runs.len(), 3, "only the compaction job came back early");
        assert_eq!(runs[2]["ran"], "request_compact", "{}", runs[2]);
        assert!(compacts(&fake).len() > made, "{}", runs[2]);

        while fake.kv.borrow()["cron:state"].1["jobs"]["compact"]["more_pending"] == true {
            fake.now.set(fake.now.get() + COMPACT_CATCHUP_S);
            run_tick(&fake);
        }
        let caught_up = fake.stage_runs.borrow().len();
        fake.now.set(fake.now.get() + COMPACT_INTERVAL_S - 1);
        run_tick(&fake);
        assert_eq!(fake.stage_runs.borrow().len(), caught_up, "not due yet");
        fake.now.set(fake.now.get() + 1);
        run_tick(&fake);
        let runs = fake.stage_runs.borrow().clone();
        assert_eq!(
            runs.len(),
            caught_up + 1,
            "{COMPACT_INTERVAL_S} s after the last run"
        );
        assert_eq!(runs[caught_up]["status"], "ok");
        assert!(
            runs[caught_up]["ran"].is_null() || runs[caught_up]["ran"] == "request_compact",
            "{}",
            runs[caught_up]
        );
    }

    /// Maintenance rides the ingest tick's dispatch, so a plan is in flight for nearly every
    /// turn under load. The tick re-reads the watermark before its commit, so both land.
    #[test]
    fn compacts_alongside_the_dispatching_ticks_in_flight_plan() {
        let fake = Fake::new(NOW);
        tick_runtime(&fake);
        let mut svc = serde_json::to_value(service()).unwrap();
        svc["compaction"] = json!({"mode": "size_target", "size_target_mb": 64});
        fake.kv.borrow_mut().insert(format!("svc:{SID}"), (1, svc));
        let sources = seed(&fake, 3);
        let period = NOW - 180;
        put_raw(&fake, period, "fresh", &[line(period, "fresh")]);
        let stage = serde_json::to_value(fastly_log_analytics::admin::stage::StageRequest {
            service_id: SID.into(),
            run_id: "run-1".into(),
            tick_id: String::new(),
            trigger: "clock".into(),
            actor: "system".into(),
            bucket: 0,
            start: 5,
            compact_only: true,
        })
        .unwrap();
        fake.new_request();
        let buffer = LogBuffer::new(SID, "req");
        let options = ingest::coordinator::Options {
            extra: vec![(fastly_log_analytics::admin::stage::PATH.into(), stage)],
            ..ingest::coordinator::Options::request(&fake)
        };
        let report = ingest::coordinator::coordinate(
            &fake,
            &service(),
            "ak",
            "sk",
            Instant::now(),
            &buffer,
            &options,
        )
        .unwrap();
        assert_eq!(report.converted, 1, "{report:?}");
        assert!(!report.busy);
        let made = compacts(&fake);
        assert_eq!(made.len(), 1, "{:?}", report.extra_results);
        let query = format!("from={}&to={}", iso_url(HOUR), iso_url(NOW));
        let (_, catalog) = catalog_keys(&fake, &query);
        assert!(catalog.contains(&made[0]));
        assert!(sources.iter().all(|key| !catalog.contains(key)));
        assert!(
            catalog
                .iter()
                .any(|key| !key.contains("/compact=") && !sources.contains(key)),
            "{catalog:?}"
        );
    }

    /// A window file with enough wide string columns that a few of them fill a compact's footer.
    fn wide_window(fake: &Fake, start: i64) -> String {
        let names = (0..200).map(|c| format!("c{c:03}")).collect::<Vec<_>>();
        let schema = Arc::new(arrow_schema::Schema::new(
            names
                .iter()
                .map(|name| arrow_schema::Field::new(name, arrow_schema::DataType::Utf8, false))
                .chain([arrow_schema::Field::new(
                    "blob",
                    arrow_schema::DataType::Binary,
                    false,
                )])
                .collect::<Vec<_>>(),
        ));
        // A binary column is outside the window writer's types, so these one-row files are
        // copied as they are and each keeps its own footer entry.
        let columns = names
            .iter()
            .map(|name| {
                Arc::new(arrow_array::StringArray::from(vec![format!(
                    "{start}-{name}-{}",
                    "x".repeat(96)
                )])) as arrow_array::ArrayRef
            })
            .chain([Arc::new(BinaryArray::from_iter_values([b"x"])) as arrow_array::ArrayRef])
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let mut writer = parquet::arrow::ArrowWriter::try_new(Vec::new(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        let bytes = writer.into_inner().unwrap();
        let key = ingest::processed_key(ROOT, start, start + 60, "abcdef012345", 1);
        fake.bucket.borrow_mut().insert(key.clone(), bytes);
        let now = fake.now.replace(start.saturating_add(60) as u64);
        publish_files(fake, std::slice::from_ref(&key), &[], &[]);
        fake.now.set(now);
        key
    }

    #[test]
    fn a_full_compact_footer_regroups_without_an_error_and_is_remembered() {
        let fake = Fake::new(NOW);
        let sources = (0..2)
            .flat_map(|h| (0..8).map(move |i| HOUR + h * 3600 + i * 60))
            .map(|start| wide_window(&fake, start))
            .collect::<Vec<_>>();
        let mut regroups = 0;
        for _ in 0..64 {
            let report = run(&fake, TARGET);
            assert!(report.errors.is_empty(), "{:?}", report.errors);
            regroups += usize::from(report.stopped.as_deref() == Some("footer_limit"));
            if !report.more_pending {
                break;
            }
        }
        assert_eq!(
            regroups, 1,
            "the learned group size must outlive the commit"
        );
        let state = fake.kv.borrow()[compaction::STATE_KEY].1.clone();
        assert_eq!(
            state["skip"],
            json!([]),
            "a full compact is left alone, not skipped"
        );
        let compacts = compacts(&fake);
        assert!(compacts.len() >= 4, "{compacts:?}");
        let rows = compacts
            .iter()
            .map(|key| {
                batches(&fake.bucket.borrow()[key])
                    .iter()
                    .map(|b| b.num_rows())
                    .sum::<usize>()
            })
            .sum::<usize>();
        assert_eq!(rows, sources.len());
    }

    #[test]
    fn a_pass_spends_its_spare_calls_reading_sources() {
        let fake = Fake::new(NOW);
        for i in 0..32 {
            window(&fake, HOUR + i * 60, &["x"]);
        }
        let mut passes = 0;
        let report = loop {
            passes += 1;
            fake.new_request();
            fake.vcpu.set(0);
            let report =
                compaction::request_compact(&store(&fake, &service()), "ck", TARGET, 1).unwrap();
            assert!(fake.calls.get() <= BUDGET);
            if report.compacts > 0 || passes > 16 {
                break report;
            }
        };
        assert_eq!(report.sources, 32);
        assert!(
            passes <= 3,
            "{passes} worker passes for one 32-source compact"
        );
    }

    #[test]
    fn stays_within_the_backend_call_budget() {
        let fake = Fake::new(NOW);
        for hour in 0..3 {
            for i in 0..30 {
                window(&fake, HOUR + hour * 3600 - 2 * 86_400 + i * 60, &["x"]);
            }
        }
        let mut runs = 0;
        loop {
            runs += 1;
            let report = run(&fake, TARGET);
            assert!(fake.calls.get() <= BUDGET);
            if !report.more_pending || runs > 100 {
                break;
            }
        }
        assert!(runs <= 100);
        let query = format!("from={}&to={}", iso_url(HOUR - 2 * 86_400), iso_url(HOUR));
        let active = catalog_keys(&fake, &query).1;
        assert_eq!(active.len(), 3);
        assert!(active.iter().all(|key| key.contains("/compact=")));
        assert_eq!(
            active
                .iter()
                .map(|key| column(&fake.bucket.borrow()[key], "ip").len())
                .sum::<usize>(),
            90
        );
    }

    fn ticks(fake: &Fake, enabled: bool) -> Vec<String> {
        tick_runtime(fake);
        if enabled {
            let mut svc = serde_json::to_value(service()).unwrap();
            svc["compaction"] = json!({"mode": "size_target", "size_target_mb": 64});
            fake.kv.borrow_mut().insert(format!("svc:{SID}"), (1, svc));
        }
        fake.kv.borrow_mut().insert(
            "retention:last_sweep".into(),
            (1, json!({"at": NOW, "complete": true})),
        );
        for _ in 0..16 {
            fake.now.set(fake.now.get() + 120);
            run_tick(fake);
        }
        rows(fake, LogKind::CronRuns)
            .iter()
            .map(|r| r["cron_kind"].as_str().unwrap().to_owned())
            .collect()
    }

    #[test]
    fn compaction_peer_accepts_a_serialized_service_with_a_nonempty_install_prefix() {
        let fake = Fake::new(NOW);
        let mut svc = service();
        svc.fos_prefix = "campaign/capacity".into();
        svc.compaction =
            serde_json::from_value(json!({"mode": "size_target", "size_target_mb": 64})).unwrap();
        assert!(fastly_log_analytics::admin::valid_prefix(&svc.fos_prefix));
        let request = fastly_log_analytics::http::Request {
            method: "POST".into(),
            path: ingest::worker::PATH.into(),
            headers: [("x-flp-worker-key".into(), "fos-secret".into())].into(),
            body: json!({"maintenance": "request_compact", "service": svc})
                .to_string()
                .into_bytes(),
        };
        let response = ingest::worker::handle_worker_ingest(&fake, &request);
        assert_eq!(
            response.status,
            200,
            "{}",
            String::from_utf8_lossy(&response.body)
        );
        let reply: Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(reply["status"], "ok");
        assert!(reply["execution_metrics"]["calls"].as_u64().unwrap() > 0);
    }

    #[test]
    fn compaction_peer_rejects_traversal_and_ambiguous_install_prefixes() {
        for prefix in [
            "../campaign",
            "/campaign",
            "campaign//capacity",
            "campaign/./capacity",
        ] {
            let fake = Fake::new(NOW);
            let mut svc = service();
            svc.fos_prefix = prefix.into();
            let request = fastly_log_analytics::http::Request {
                method: "POST".into(),
                path: ingest::worker::PATH.into(),
                headers: [("x-flp-worker-key".into(), "fos-secret".into())].into(),
                body: json!({"maintenance": "request_compact", "service": svc})
                    .to_string()
                    .into_bytes(),
            };
            assert_eq!(
                ingest::worker::handle_worker_ingest(&fake, &request).status,
                400,
                "{prefix}"
            );
            assert!(
                fake.kv_reads.borrow().is_empty(),
                "invalid scopes cannot touch storage"
            );
        }
    }

    #[test]
    fn an_idle_tick_runs_request_compaction_when_the_service_enables_it() {
        let fake = Fake::new(NOW);
        seed(&fake, 4);
        let kinds = ticks(&fake, true);
        assert!(kinds.contains(&"request_compact".to_owned()), "{kinds:?}");
        assert_eq!(
            compacts(&fake).len(),
            1,
            "{:?}; {:?}",
            rows(&fake, LogKind::CronRuns)
                .iter()
                .filter(|row| row["cron_kind"] == "request_compact")
                .collect::<Vec<_>>(),
            fake.kv.borrow().get(compaction::STATE_KEY)
        );
        let records = rows(&fake, LogKind::CronRuns);
        let details: Value = records
            .iter()
            .find_map(|row| {
                let details: Value = serde_json::from_str(row["details_json"].as_str()?).ok()?;
                details["worker_executions_metrics"]
                    .as_array()?
                    .iter()
                    .any(|sample| sample["kind"] == "request_compact")
                    .then_some(details)
            })
            .unwrap();
        let samples = details["worker_executions_metrics"].as_array().unwrap();
        assert_eq!(
            samples.len() as u64,
            details["workers_dispatched"].as_u64().unwrap()
        );
        assert_eq!(details["workers_answered"], details["workers_dispatched"]);
        let child = samples
            .iter()
            .find(|sample| sample["kind"] == "request_compact")
            .unwrap();
        assert_eq!(child["status"], "ok");
        assert!(child["calls"].as_u64().unwrap() > 0);
        assert!(samples.iter().any(|sample| sample["kind"] == "maintenance"));
    }

    #[test]
    fn one_maintenance_turn_chains_compaction_passes_until_a_compact_commits() {
        let fake = Fake::new(NOW);
        seed(&fake, 32);
        tick_runtime(&fake);
        let mut svc = serde_json::to_value(service()).unwrap();
        svc["compaction"] = json!({"mode": "size_target", "size_target_mb": 64});
        fake.kv.borrow_mut().insert(format!("svc:{SID}"), (1, svc));
        fake.kv.borrow_mut().insert(
            "retention:last_sweep".into(),
            (1, json!({"at": NOW, "complete": true})),
        );
        let first = (0..16)
            .find_map(|_| {
                fake.now.set(fake.now.get() + 120);
                run_tick(&fake);
                rows(&fake, LogKind::CronRuns)
                    .into_iter()
                    .find(|row| row["cron_kind"] == "request_compact")
            })
            .expect("a request_compact turn");
        assert_eq!(first["parquet_files_written"], 1, "{first}");
        assert_eq!(first["listed_files"], 32, "{first}");
        assert_eq!(compacts(&fake).len(), 1);
        let details: Value = serde_json::from_str(first["details_json"].as_str().unwrap()).unwrap();
        assert!(details["passes"].as_u64().unwrap() > 1, "{details}");
    }

    #[test]
    fn a_compaction_turn_starts_no_further_pass_without_wall_time_for_one() {
        let fake = Fake::new(NOW);
        seed(&fake, 32);
        tick_runtime(&fake);
        let mut svc = serde_json::to_value(service()).unwrap();
        svc["compaction"] = json!({"mode": "size_target", "size_target_mb": 64});
        fake.kv.borrow_mut().insert(format!("svc:{SID}"), (1, svc));
        let turn = |fake: &Fake| {
            fake.new_request();
            let body = json!({"service_id": SID, "run_id": "run1", "tick_id": "", "trigger": "clock",
                "actor": "clock", "bucket": 0, "start": 5, "compact_only": true});
            let path = fastly_log_analytics::admin::stage::PATH.to_owned();
            let reply = fake.worker_dispatch(&[(path, body)], std::time::Duration::from_secs(32));
            assert_eq!(reply[0].as_ref().unwrap()["status"], "ok");
        };
        let wall = ingest::storage::WALL_STOP.as_secs();
        fake.elapsed_s.set(wall - 4);
        turn(&fake);
        assert_eq!(fake.worker_runs.get(), 1);
        assert!(compacts(&fake).is_empty());
        fake.elapsed_s.set(0);
        turn(&fake);
        assert!(fake.worker_runs.get() > 2);
        assert_eq!(compacts(&fake).len(), 1);
    }

    #[test]
    fn compaction_off_by_default_costs_nothing() {
        let fake = Fake::new(NOW);
        seed(&fake, 4);
        let kinds = ticks(&fake, false);
        assert!(!kinds.contains(&"request_compact".to_owned()), "{kinds:?}");
        assert!(compacts(&fake).is_empty());
        assert!(!fake
            .kv_reads
            .borrow()
            .iter()
            .any(|k| k == compaction::STATE_KEY));
    }

    #[test]
    fn request_compaction_respects_cpu_wall_and_call_budgets_without_changing_other_stages() {
        assert_eq!(ingest::bundle::VCPU_STOP_MS, 700);
        assert_eq!(ingest::bundle::WRITE_VCPU_STOP_MS, 680);
        assert_eq!(compaction::VCPU_STOP_MS, 40);
        assert_eq!(fastly_log_analytics::telemetry::compact::VCPU_STOP_MS, 700);
        assert_eq!(
            fastly_log_analytics::telemetry::flush::FLUSH_VCPU_SOFT_MS,
            700
        );
        assert_eq!(ingest::coordinator::VCPU_STOP_MS, 800);
        assert_eq!(ingest::retention::VCPU_STOP_MS, 700);

        let fake = Fake::new(NOW + 4 * 3600);
        for h in 0..3 {
            let start = HOUR + h * 3600;
            window(&fake, start + 60, &["198.51.100.1"]);
            window(&fake, start + 120, &["198.51.100.2"]);
        }

        // Wall-clock gate (40s < 60s runtime limit) halts work before starting and resumes once
        // elapsed time resets for the next execution.
        fake.elapsed_s.set(40);
        let wall_blocked = run(&fake, TARGET);
        assert_eq!(wall_blocked.compacts, 0);
        assert!(wall_blocked.more_pending);
        assert!(fake.calls.get() <= BUDGET);

        fake.elapsed_s.set(0);
        fake.vcpu_step.set(750);
        let first = run(&fake, TARGET);
        assert_eq!(
            first.compacts, 0,
            "CPU admission applies before the first merge"
        );
        assert!(
            first.more_pending,
            "remaining hours stay pending for next run"
        );
        assert!(
            fake.calls.get() <= BUDGET,
            "{} calls > 32",
            fake.calls.get()
        );

        fake.vcpu_step.set(1);
        for _ in 0..16 {
            if !run(&fake, TARGET).more_pending {
                break;
            }
        }
        assert_eq!(compacts(&fake).len(), 3);
        assert!(fake.calls.get() <= BUDGET);
    }
}

mod delete_data_gate {
    use super::*;
    use fastly_log_analytics::admin::{reset::JOB_KEY, DataReset, ResetPhase};

    fn mark(fake: &Fake, phase: Option<ResetPhase>) {
        let status = match phase {
            Some(ResetPhase::Cleanup) => "cleanup",
            _ => "fencing",
        };
        let mut svc = rum_service();
        svc.data_reset = phase.map(|phase| DataReset {
            job_id: "job1".into(),
            reset_at: BASE as u64,
            phase,
            started_at: BASE as u64,
            actor: "tester".into(),
            rum_held: false,
        });
        store_service(fake, &svc);
        // A fence still waiting out its leases for the whole scenario, so the job stays put.
        let started_at = fake.now.get() + 100_000;
        fake.kv.borrow_mut().insert(
            JOB_KEY.into(),
            (
                1,
                json!({"job_id": "job1", "status": status, "started_at": started_at}),
            ),
        );
    }

    fn scenario(phase: Option<ResetPhase>, ticks: usize) -> Vec<String> {
        let fake = Fake::new(BASE + 3600);
        tick_runtime(&fake);
        mark(&fake, phase.clone());
        put_raw(&fake, BASE + 60, "ok", &[line(BASE + 60, "ok")]);
        put_rum(
            &fake,
            BASE + 60,
            "ok",
            &[rum_beacon(BASE + 60, "LCP", "123")],
        );
        for _ in 0..ticks {
            fake.now.set(fake.now.get() + 120);
            run_tick(&fake);
        }
        let kinds: Vec<String> = rows(&fake, LogKind::CronRuns)
            .iter()
            .filter_map(|r| r["cron_kind"].as_str().map(str::to_owned))
            .collect();
        if phase == Some(ResetPhase::Fencing) {
            assert_eq!(fake.keys("raw/request/").len(), 1, "{kinds:?}");
        }
        kinds
    }

    fn count(kinds: &[String], kind: &str) -> usize {
        kinds.iter().filter(|k| *k == kind).count()
    }

    #[test]
    fn fencing_marker_skips_ingest_rum_and_raw_delete_but_runs_delete_data() {
        let control = scenario(None, 4);
        assert!(count(&control, "request_ingest") > 0, "{control:?}");
        assert!(count(&control, "rum_ingest") > 0, "{control:?}");
        let kinds = scenario(Some(ResetPhase::Fencing), 4);
        for kind in ["request_ingest", "rum_ingest", "raw_delete"] {
            assert_eq!(count(&kinds, kind), 0, "{kind}: {kinds:?}");
        }
        assert!(count(&kinds, "delete_data") > 0, "{kinds:?}");
    }

    #[test]
    fn fencing_marker_skips_request_data_maintenance_stages() {
        let control = scenario(None, 16);
        assert!(count(&control, "retention_sweep") > 0, "{control:?}");
        let kinds = scenario(Some(ResetPhase::Fencing), 16);
        for kind in [
            "retention_sweep",
            "summary_bundle",
            "request_compact",
            "storage_reconcile",
        ] {
            assert_eq!(count(&kinds, kind), 0, "{kind}: {kinds:?}");
        }
    }

    #[test]
    fn delete_data_stage_runs_every_maintenance_tick_until_complete() {
        let kinds = scenario(Some(ResetPhase::Fencing), 2);
        assert_eq!(count(&kinds, "delete_data"), 2, "{kinds:?}");
    }

    #[test]
    fn cleanup_phase_holds_rum_ingest_until_its_cursor_reset_lands() {
        let fake = Fake::new(BASE + 3600);
        tick_runtime(&fake);
        let hold = |rum_held: bool| {
            let mut svc = rum_service();
            svc.data_reset = Some(DataReset {
                job_id: "job1".into(),
                reset_at: BASE as u64,
                phase: ResetPhase::Cleanup,
                started_at: BASE as u64,
                actor: "tester".into(),
                rum_held,
            });
            store_service(&fake, &svc);
        };
        hold(true);
        // A job the marker doesn't own: only the marker's gate is under test.
        fake.kv.borrow_mut().insert(
            JOB_KEY.into(),
            (1, json!({"job_id": "other", "status": "complete"})),
        );
        put_raw(&fake, BASE + 60, "ok", &[line(BASE + 60, "ok")]);
        put_rum(
            &fake,
            BASE + 60,
            "ok",
            &[rum_beacon(BASE + 60, "LCP", "123")],
        );
        let ticks = |n: usize| {
            for _ in 0..n {
                fake.now.set(fake.now.get() + 120);
                run_tick(&fake);
            }
            rows(&fake, LogKind::CronRuns)
                .iter()
                .filter_map(|r| r["cron_kind"].as_str().map(str::to_owned))
                .collect::<Vec<String>>()
        };
        let kinds = ticks(4);
        assert!(count(&kinds, "request_ingest") > 0, "{kinds:?}");
        assert_eq!(count(&kinds, "rum_ingest"), 0, "{kinds:?}");
        assert!(!fake.keys("processed/request/").is_empty(), "{kinds:?}");
        assert!(fake.keys("processed/rum/").is_empty(), "{kinds:?}");
        assert_eq!(fake.keys("raw/rum/").len(), 1);
        hold(false);
        let kinds = ticks(4);
        assert!(count(&kinds, "rum_ingest") > 0, "{kinds:?}");
    }

    /// RUM floats in every processed RUM file.
    fn rum_values(fake: &Fake) -> Vec<f64> {
        let mut values = Vec::new();
        for key in fake.keys("processed/rum/") {
            for batch in batches(&fake.bucket.borrow()[&key]) {
                if batch.column_by_name("metric_value").is_some() {
                    values.extend(floats(&batch, "metric_value").into_iter().flatten());
                }
            }
        }
        values.sort_by(f64::total_cmp);
        values
    }

    #[test]
    fn a_held_rum_stream_resumes_from_the_floor_and_never_converts_pre_reset_beacons() {
        // Reset well before the clock, so new objects (LIST mtime `BASE`) postdate the CAS.
        let reset_at = BASE - 1200;
        let cas_at = reset_at + 30;
        let fake = Fake::new(BASE);
        tick_runtime(&fake);
        let mut svc = rum_service();
        // A long request settle keeps cleanup from deleting the raw keys during the test.
        svc.log_period = 1800;
        svc.data_reset = Some(DataReset {
            job_id: "job1".into(),
            reset_at: reset_at as u64,
            phase: ResetPhase::Cleanup,
            started_at: reset_at as u64,
            actor: "tester".into(),
            rum_held: true,
        });
        store_service(&fake, &svc);
        let minute = |t: i64| ingest::minute_dir(t);
        let floor = format!("raw/request/{}", minute(reset_at));
        // The post-CAS bound resumed request ingest; the RUM cursor reset never landed.
        fake.kv.borrow_mut().insert(
            ingest::WATERMARK_KEY.into(),
            (
                1,
                json!({"key": floor, "floor_key": floor, "at": cas_at, "passes": [], "swept": [],
                "sweep": {"after": floor, "at": cas_at}, "manifest_gen": 1}),
            ),
        );
        fake.kv.borrow_mut().insert(
            "ingest:rum:watermark".into(),
            (
                1,
                json!({"key": format!("raw/rum/{}", minute(reset_at - 600)), "at": 1,
                "passes": [], "swept": []}),
            ),
        );
        fake.kv.borrow_mut().insert(
            JOB_KEY.into(),
            (
                1,
                json!({"job_id": "job1", "status": "cleanup", "reset_at": reset_at,
                "started_at": reset_at, "resetting_at": cas_at - 2, "reset_cas_at": cas_at,
                "steps_done": ["keys", "deleteq", "quarantine"]}),
            ),
        );
        put_rum(
            &fake,
            reset_at - 60,
            "old",
            &[rum_beacon(reset_at - 30, "LCP", "111")],
        );
        put_rum(
            &fake,
            reset_at + 60,
            "new",
            &[rum_beacon(reset_at + 90, "LCP", "222")],
        );
        let mut kinds = Vec::new();
        for _ in 0..6 {
            fake.now.set(fake.now.get() + 120);
            run_tick(&fake);
            kinds = rows(&fake, LogKind::CronRuns)
                .iter()
                .filter_map(|r| r["cron_kind"].as_str().map(str::to_owned))
                .collect();
        }
        let svc: Service =
            serde_json::from_value(fake.kv.borrow()[&format!("svc:{SID}")].1.clone()).unwrap();
        assert!(
            !fastly_log_analytics::admin::reset::rum_held(&svc),
            "{kinds:?}"
        );
        assert!(count(&kinds, "rum_ingest") > 0, "{kinds:?}");
        assert_eq!(rum_values(&fake), [222.0], "{kinds:?}");
    }

    #[test]
    fn cleanup_phase_runs_normal_stages_again() {
        let kinds = scenario(Some(ResetPhase::Cleanup), 16);
        assert!(count(&kinds, "request_ingest") > 0, "{kinds:?}");
        assert!(count(&kinds, "rum_ingest") > 0, "{kinds:?}");
        assert!(count(&kinds, "retention_sweep") > 0, "{kinds:?}");
        assert!(count(&kinds, "delete_data") > 0, "{kinds:?}");
    }

    #[test]
    fn delete_data_and_a_rotating_stage_share_one_execution_vcpu_allowance() {
        let fake = Fake::new(BASE + 3600);
        tick_runtime(&fake);
        let reset_at = BASE as u64 + 540;
        let cas_at = reset_at + 30;
        let mut svc = rum_service();
        svc.data_reset = Some(DataReset {
            job_id: "job1".into(),
            reset_at,
            phase: ResetPhase::Cleanup,
            started_at: reset_at,
            actor: "tester".into(),
            rum_held: false,
        });
        store_service(&fake, &svc);
        let floor = format!("raw/request/{}", ingest::minute_dir(reset_at as i64));
        fake.kv.borrow_mut().insert(
            ingest::WATERMARK_KEY.into(),
            (
                1,
                json!({"key": floor, "floor_key": floor, "at": cas_at, "passes": [], "swept": [],
                "sweep": {"after": floor, "at": cas_at}, "manifest_gen": 1}),
            ),
        );
        fake.kv.borrow_mut().insert(
            JOB_KEY.into(),
            (
                1,
                json!({"job_id": "job1", "status": "cleanup", "reset_at": reset_at,
                "started_at": reset_at, "resetting_at": cas_at, "reset_cas_at": cas_at,
                "steps_done": ["rum_watermark", "rum_released", "keys", "deleteq", "quarantine"]}),
            ),
        );
        // Pre-reset objects (LIST mtime `BASE`) for cleanup, and old ops logs for the others.
        for i in 0..6000 {
            fake.bucket.borrow_mut().insert(
                format!("processed/request/day=1/old-{i:05}.parquet"),
                vec![1],
            );
        }
        for day in 1..9 {
            for hour in 0..24 {
                fake.bucket.borrow_mut().insert(
                    format!(
                        "processed/ops/cron_runs/year=2026/month=12/day={day:02}/hour={hour:02}/p.parquet"
                    ),
                    vec![1],
                );
            }
        }
        fake.vcpu_step.set(60);
        let mut kinds = Vec::new();
        for _ in 0..16 {
            fake.now.set(fake.now.get() + 120);
            run_tick(&fake);
            kinds = rows(&fake, LogKind::CronRuns)
                .iter()
                .filter_map(|r| r["cron_kind"].as_str().map(str::to_owned))
                .collect();
        }
        assert!(count(&kinds, "delete_data") > 0, "{kinds:?}");
        let peak = fake.vcpu_peaks.borrow().get("stage").copied().unwrap_or(0);
        assert!(
            peak <= 1000,
            "stage execution used {peak}ms vCPU: {kinds:?}"
        );
    }
}

mod delete_data_reset {
    use super::*;
    use fastly_log_analytics::admin::{self, reset::JOB_KEY, DataReset, ResetPhase};
    use ingest::catalog;

    const RESET_AT: u64 = BASE as u64 + 600;

    /// Runs a Delete Data job's "resetting" step at `RESET_AT`, as the maintenance stage would.
    fn reset(fake: &Fake) {
        let mut svc = rum_service();
        svc.data_reset = Some(DataReset {
            job_id: "job1".into(),
            reset_at: RESET_AT,
            phase: ResetPhase::Fencing,
            started_at: RESET_AT,
            actor: "tester".into(),
            rum_held: false,
        });
        store_service(fake, &svc);
        fake.kv.borrow_mut().insert(
            JOB_KEY.into(),
            (
                1,
                json!({"job_id": "job1", "status": "resetting", "reset_at": RESET_AT,
                    "started_at": RESET_AT}),
            ),
        );
        fake.new_request();
        admin::reset::advance(&store(fake, &svc)).unwrap().unwrap();
        let job = fake.kv.borrow()[JOB_KEY].1.clone();
        assert_eq!(job["status"], "cleanup", "{job}");
        assert!(fake.calls.get() <= BUDGET);
    }

    fn publish_two(fake: &Fake) {
        for (n, name) in ["a", "b"].iter().enumerate() {
            let start = BASE + n as i64 * 60;
            let key = ingest::processed_key("processed/request/", start, start + 60, name, 1);
            fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
            publish_files(fake, &[key], &[], &[]);
        }
    }

    /// The ips of every file the current catalog head's recent commits added.
    fn catalog_ips(fake: &Fake) -> Vec<String> {
        let watermark = fake.kv.borrow()[ingest::WATERMARK_KEY].1.clone();
        let head = catalog::head(&watermark).unwrap();
        let mut ips = Vec::new();
        for event in &head.recent {
            let body = fake.bucket.borrow()[event.manifest.as_ref().unwrap()].clone();
            let manifest: catalog::Manifest = serde_json::from_slice(&body).unwrap();
            for file in &manifest.files {
                for batch in batches(&fake.bucket.borrow()[&file.key]) {
                    ips.extend(strings(&batch, "ip").into_iter().flatten());
                }
            }
        }
        ips.sort();
        ips
    }

    #[test]
    fn reset_head_raises_generation_and_empties_catalog() {
        let fake = Fake::new(BASE + 3600);
        publish_two(&fake);
        let watermark = fake.kv.borrow()[ingest::WATERMARK_KEY].1.clone();
        let prev = catalog::head(&watermark).unwrap();
        assert!(!prev.days.is_empty() && !prev.statistics.is_empty());
        let now = (BASE + 3600) as u64;
        let (head, manifest) = catalog::reset_head(&prev, now, SID);
        assert_eq!(head.generation, prev.generation + 1);
        assert_eq!(head.recent.len(), 1);
        assert_eq!(head.recent[0].generation, head.generation);
        assert_eq!(head.recent[0].cursor, format!("{SID}:{}", head.generation));
        assert!(head.days.is_empty());
        assert!(head.statistics.is_empty());
        assert!(head.hour_bundles.is_empty() && head.day_bundles.is_empty());
        assert!(head.bundle_retired.is_empty());
        assert!(head.last_compaction.is_none());
        assert_eq!(
            head.retirement_grace_until,
            now + ingest::compaction::DELETE_GRACE_S
        );
        assert_eq!(manifest.batch, "reset");
        assert_eq!(manifest.generation, head.generation);
        assert_eq!(manifest.service_id, SID);
        assert!(manifest.files.is_empty() && manifest.summaries.is_empty());
        assert!(manifest.previous.is_none());
        let mut value = watermark;
        value["catalog"] = serde_json::to_value(&head).unwrap();
        assert_eq!(catalog::head(&value).unwrap().generation, head.generation);
    }

    #[test]
    fn delta_walk_over_reset_manifest_requires_resync() {
        let fake = Fake::new(BASE + 3600);
        publish_two(&fake);
        reset(&fake);
        let window = format!(
            "from={}&to={}&previous_from={}&previous_to={}",
            iso_url(BASE),
            iso_url(BASE + 3600),
            iso_url(BASE),
            iso_url(BASE + 3600)
        );
        for base in [1, 2] {
            assert_eq!(
                files_page(&fake, &format!("{window}&since_generation={base}")).unwrap_err(),
                (409, "catalog_resync_required"),
                "base {base}"
            );
        }
    }

    #[test]
    fn sse_cursor_from_before_reset_gets_resync() {
        let fake = Fake::new(BASE + 3600);
        publish_two(&fake);
        reset(&fake);
        fake.new_request();
        let stale = catalog::events(&fake, &service(), &format!("cursor={SID}:2")).unwrap();
        assert!(stale.contains("event: resync\n"), "{stale}");
        assert!(stale.contains(&format!("id: {SID}:3\n")), "{stale}");
        fake.new_request();
        let current = catalog::events(&fake, &service(), &format!("cursor={SID}:3")).unwrap();
        assert!(!current.contains("event:"), "{current}");
    }

    #[test]
    fn reset_clears_committed_plans_so_no_pre_reset_raw_key_is_ingested_again() {
        let fake = Fake::new(BASE + 600);
        put_raw(&fake, BASE, "old", &[line(BASE, "old")]);
        tick(&fake).unwrap();
        let committed = fake.kv.borrow()[ingest::WATERMARK_KEY].1["run_id"].clone();
        assert_eq!(
            fake.kv.borrow()[ingest::PLAN_KEY].1["plan_id"],
            committed,
            "a committed plan record stays in place and names pre-reset raw keys"
        );
        fake.kv.borrow_mut().insert(
            ingest::plan_key("rum"),
            (
                1,
                json!({"version": 3, "plan_id": "rum-old", "lease_until": 0}),
            ),
        );
        reset(&fake);
        for key in [ingest::plan_key("request"), ingest::plan_key("rum")] {
            assert!(!fake.kv.borrow().contains_key(&key), "{key}");
        }
        // The pre-reset raw object is still in the bucket and a late one lands below the floor.
        put_raw(&fake, BASE + 60, "late", &[line(BASE + 60, "late")]);
        put_raw(
            &fake,
            RESET_AT as i64,
            "new",
            &[line(RESET_AT as i64, "new")],
        );
        for _ in 0..6 {
            tick_later(&fake).unwrap();
        }
        assert_eq!(catalog_ips(&fake), vec!["new".to_owned()]);
        let watermark = fake.kv.borrow()[ingest::WATERMARK_KEY].1.clone();
        assert_ne!(watermark["run_id"], committed);
        assert!(watermark["catalog"]["generation"].as_u64().unwrap() > 2);
    }

    #[test]
    fn a_commit_whose_watermark_read_predates_the_reset_cannot_land_after_it() {
        let fake = Fake::new(BASE + 600);
        put_raw(&fake, BASE, "old", &[line(BASE, "old")]);
        tick(&fake).unwrap();
        // A coordinator read this watermark before the reset CAS.
        fake.new_request();
        let stale = fake.admin_get(ingest::WATERMARK_KEY).unwrap().unwrap();
        reset(&fake);
        let after = fake.kv.borrow()[ingest::WATERMARK_KEY].clone();
        assert!(
            after.0 > stale.generation.parse::<u64>().unwrap(),
            "the reset is an overwrite CAS, never a delete and re-create"
        );
        let key = ingest::processed_key("processed/request/", BASE, BASE + 60, "stale", 1);
        fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
        let output = ingest::worker::Output {
            key,
            rows: 1,
            bytes: 1,
            ..Default::default()
        };
        fake.new_request();
        let value = catalog::prepare(
            &store(&fake, &service()),
            &stale.value,
            "stale-plan",
            "schema-v1",
            &[output],
            &[],
            &[],
            &[],
        )
        .unwrap();
        assert!(!fake
            .admin_cas(ingest::WATERMARK_KEY, Some(&stale.generation), &value)
            .unwrap());
        assert_eq!(fake.kv.borrow()[ingest::WATERMARK_KEY], after);
    }
}
