#![allow(dead_code)]
use super::common;
use arrow_array::{Array, RecordBatch, StringArray};
use bytes::Bytes;
use fastly_log_analytics::{
    admin::{FosRequest, FosResponse, Record, Service},
    ingest::{
        self,
        convert::{self, RequestSchema, Segment},
        worker,
    },
    setup::SetupStores,
    shell::{CoreCacheEntry, CoreCacheError, KvPage, Runtime},
    telemetry::{self, usage::UsageAgg, LogBuffer},
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{json, Value};
use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, BTreeSet, VecDeque},
    time::Instant,
};

pub(super) const BASE: i64 = 1_800_000_000;
pub(super) const SID: &str = "AbCdEfGhIjKlMnOpQrSt12";
pub(super) const BUDGET: u32 = 32;
pub(super) const AK: &str = "AKIDHIGHRPS";
pub(super) const SK: &str = "real-secret-key";

pub(super) fn iso(seconds: i64) -> String {
    fastly_log_analytics::ingest::rfc3339(seconds).replace("Z", "+0000")
}

pub(super) fn line(at: i64, id: &str) -> String {
    json!({"timestamp": iso(at), "ip": id, "status": 200, "url": "/a/b/c?d=e"}).to_string()
}

pub(super) fn gz(text: &[u8]) -> Vec<u8> {
    zstd::stream::encode_all(text, 1).unwrap()
}

pub(super) fn zst(text: &[u8]) -> Vec<u8> {
    zstd::stream::encode_all(text, 3).unwrap()
}

pub(super) fn ips(rows: &convert::ObjectRows) -> Vec<String> {
    let mut out = Vec::new();
    for batch in rows.windows.values() {
        out.extend((0..batch.num_rows()).filter_map(|i| common::col_str(batch, "ip", i)));
    }
    out
}

/// Ids `0..n` as NDJSON, with blank lines and one over-long line mixed in.
pub(super) fn ndjson(n: usize) -> (Vec<u8>, Vec<String>) {
    let mut text = Vec::new();
    let mut ids = Vec::new();
    for i in 0..n {
        let id = format!("id{i:06}");
        text.extend_from_slice(line(BASE + (i as i64 % 120), &id).as_bytes());
        text.push(b'\n');
        if i % 97 == 0 {
            text.extend_from_slice(b"\n  \n");
        }
        if i == n / 3 {
            text.extend(std::iter::repeat_n(
                b'x',
                convert::MAX_LINE_BYTES as usize + 10,
            ));
            text.push(b'\n');
        }
        ids.push(id);
    }
    (text, ids)
}

pub(super) fn convert_segments(object: &[u8], cuts: &[u64]) -> (Vec<String>, u64, u64) {
    let mut seen = Vec::new();
    let (mut lines, mut bad) = (0, 0);
    let mut start = 0;
    for (index, &cut) in cuts.iter().chain(std::iter::once(&u64::MAX)).enumerate() {
        let end = (index < cuts.len()).then_some(cut);
        let rows = convert::convert_object_range(
            object,
            60,
            BASE,
            Segment { start, end },
            convert::MAX_DECOMPRESSED_BYTES,
            RequestSchema::base(),
            &|| false,
        )
        .unwrap();
        lines += rows.lines_read;
        bad += rows.bad_lines;
        seen.extend(ips(&rows));
        start = cut;
    }
    (seen, lines, bad)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Fault {
    Ok,
    /// The request never reaches the execution.
    Unreachable,
    /// The execution runs (and writes) but its answer is lost.
    LostReply,
}

type Upload = (String, BTreeMap<u32, Vec<u8>>);

/// The SDK's KV read cap (`sdk.rs` `bounded_json`).
pub(super) const KV_READ_LIMIT: usize = 2_097_152;

#[derive(Default)]
pub(super) struct Fake {
    pub(super) kv: RefCell<BTreeMap<String, (u64, Value)>>,
    pub(super) bucket: RefCell<BTreeMap<String, Vec<u8>>>,
    pub(super) uploads: RefCell<BTreeMap<String, Upload>>,
    pub(super) completed: RefCell<Vec<(String, usize)>>,
    pub(super) upload_seq: Cell<u64>,
    pub(super) calls: Cell<u32>,
    pub(super) secrets_read: RefCell<BTreeSet<String>>,
    pub(super) now: Cell<u64>,
    pub(super) config: RefCell<BTreeMap<String, String>>,
    pub(super) cdn_gets: RefCell<Vec<String>>,
    pub(super) stall_after_get: Cell<bool>,
    pub(super) seq: Cell<u64>,
    pub(super) cache: RefCell<BTreeMap<String, CoreCacheEntry>>,
    pub(super) usage: UsageAgg,
    pub(super) crash_after: Cell<Option<u32>>,
    pub(super) faults: RefCell<VecDeque<Fault>>,
    pub(super) relay_faults: RefCell<VecDeque<Fault>>,
    pub(super) depth: Cell<u32>,
    pub(super) sent: Cell<u32>,
    pub(super) worker_runs: Cell<u32>,
    pub(super) relay_runs: Cell<u32>,
    pub(super) max_calls: Cell<u32>,
    pub(super) paths: RefCell<Vec<String>>,
    pub(super) plan_peak: Cell<usize>,
    pub(super) vcpu: Cell<u64>,
    pub(super) vcpu_after_get: Cell<u64>,
    pub(super) stall_keys: RefCell<BTreeSet<String>>,
}

impl Fake {
    pub(super) fn new() -> Self {
        let fake = Self::default();
        fake.now.set(BASE as u64 + 3600);
        fake
    }
    pub(super) fn set(&self, name: &str, value: &str) {
        self.config.borrow_mut().insert(name.into(), value.into());
    }
    pub(super) fn charge(&self) -> Result<(), String> {
        if self.calls.get() >= BUDGET {
            return Err("backend_budget_exhausted".into());
        }
        if self.depth.get() == 0
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
    pub(super) fn new_request(&self) {
        self.calls.set(0);
        self.sent.set(0);
        self.vcpu.set(0);
        self.secrets_read.borrow_mut().clear();
    }
    pub(super) fn keys(&self, prefix: &str) -> Vec<String> {
        self.bucket
            .borrow()
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect()
    }
    pub(super) fn reply(status: u16, body: impl Into<Vec<u8>>) -> Result<FosResponse, String> {
        Ok(FosResponse {
            cdn_cache: None,
            status,
            body: body.into(),
            retry_after: None,
        })
    }
    /// Runs `handle` as a fresh execution with its own budget and Secret Store reads.
    pub(super) fn execution(
        &self,
        handle: impl FnOnce() -> fastly_log_analytics::http::Response,
    ) -> (u32, fastly_log_analytics::http::Response) {
        let spent = self.calls.replace(0);
        let sent = self.sent.replace(0);
        let vcpu = self.vcpu.replace(0);
        let secrets = std::mem::take(&mut *self.secrets_read.borrow_mut());
        self.depth.set(self.depth.get() + 1);
        let response = handle();
        self.depth.set(self.depth.get() - 1);
        self.sent.set(sent);
        self.vcpu.set(vcpu);
        let used = self.calls.replace(spent);
        *self.secrets_read.borrow_mut() = secrets;
        assert!(used <= BUDGET, "execution used {used} calls");
        self.max_calls.set(self.max_calls.get().max(used));
        (used, response)
    }
    pub(super) fn list(&self, query: &str) -> String {
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
    pub(super) fn multipart(&self, request: &FosRequest<'_>, id: &str) -> Result<FosResponse, String> {
        let mut uploads = self.uploads.borrow_mut();
        let Some((key, parts)) = uploads.get_mut(id) else {
            return Self::reply(404, "<Error><Code>NoSuchUpload</Code></Error>");
        };
        assert_eq!(key, request.key);
        match request.method {
            "PUT" => {
                let query: BTreeMap<String, String> =
                    url::form_urlencoded::parse(request.query.as_bytes())
                        .into_owned()
                        .collect();
                parts.insert(query["partNumber"].parse().unwrap(), request.body.clone());
                Self::reply(200, "")
            }
            "POST" => {
                let xml = String::from_utf8(request.body.clone()).unwrap();
                let mut body = Vec::new();
                let mut sizes = Vec::new();
                for part in xml.split("<PartNumber>").skip(1) {
                    let number: u32 = part.split("</PartNumber>").next().unwrap().parse().unwrap();
                    sizes.push(parts[&number].len());
                    body.extend_from_slice(&parts[&number]);
                }
                if sizes[..sizes.len() - 1]
                    .iter()
                    .any(|&n| n < 5 * 1024 * 1024)
                {
                    return Self::reply(400, "<Error><Code>EntityTooSmall</Code></Error>");
                }
                let key = key.clone();
                uploads.remove(id);
                self.completed.borrow_mut().push((key.clone(), sizes.len()));
                self.bucket.borrow_mut().insert(key, body);
                Self::reply(200, "<CompleteMultipartUploadResult/>")
            }
            "DELETE" => {
                uploads.remove(id);
                Self::reply(204, "")
            }
            _ => Self::reply(405, ""),
        }
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
        Ok(match name {
            "window_s" => None,
            name => Some(
                self.config
                    .borrow()
                    .get(name)
                    .cloned()
                    .unwrap_or("v".into()),
            ),
        })
    }
    fn secret(&self, name: &str) -> Result<Option<String>, String> {
        if self.secrets_read.borrow_mut().insert(name.into()) {
            self.charge()?;
        }
        Ok(Some(match name {
            "cdn_key" => "ck".into(),
            "worker_secret" => "wk".into(),
            "ak" => AK.into(),
            "sk" => SK.into(),
            name if name.starts_with("fos_ak.") => AK.into(),
            name if name.starts_with("fos_sk.") => {
                json!({"access_key": AK, "secret_key": SK}).to_string()
            }
            _ => "other-secret".into(),
        }))
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
        if self
            .kv
            .borrow()
            .get(key)
            .is_some_and(|(_, v)| v.to_string().len() > KV_READ_LIMIT)
        {
            return Err("backend_response_too_large".into());
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
        let mut kv = self.kv.borrow_mut();
        let current = kv.get(key).map(|(g, _)| g.to_string());
        if current.as_deref() != generation {
            return Ok(false);
        }
        let next = kv.get(key).map_or(1, |(g, _)| g + 1);
        if key == ingest::PLAN_KEY {
            self.plan_peak
                .set(self.plan_peak.get().max(value.to_string().len()));
        }
        kv.insert(key.into(), (next, value.clone()));
        Ok(true)
    }
    fn admin_fos(&self, request: FosRequest<'_>) -> Result<FosResponse, String> {
        self.charge()?;
        assert_eq!((request.access_key, request.secret_key), (AK, SK));
        let query: BTreeMap<String, String> = url::form_urlencoded::parse(request.query.as_bytes())
            .into_owned()
            .collect();
        if let Some(id) = query.get("uploadId") {
            return self.multipart(&request, id);
        }
        match request.method {
            "POST" if request.query == "uploads" => {
                let n = self.upload_seq.get() + 1;
                self.upload_seq.set(n);
                let id = format!("up{n}");
                self.uploads
                    .borrow_mut()
                    .insert(id.clone(), (request.key.into(), BTreeMap::new()));
                Self::reply(
                    200,
                    format!("<InitiateMultipartUploadResult><UploadId>{id}</UploadId></InitiateMultipartUploadResult>"),
                )
            }
            "PUT" => {
                self.bucket
                    .borrow_mut()
                    .insert(request.key.into(), request.body);
                Self::reply(200, "")
            }
            "GET" if request.query.contains("list-type") => {
                Self::reply(200, self.list(request.query))
            }
            "GET" => match self.bucket.borrow().get(request.key) {
                Some(body) => Self::reply(200, body.clone()),
                None => Self::reply(404, ""),
            },
            "DELETE" => {
                self.bucket.borrow_mut().remove(request.key);
                Self::reply(204, "")
            }
            "POST" if request.query == "delete" => {
                let xml = String::from_utf8(request.body).unwrap();
                let mut out = String::from("<DeleteResult>");
                for part in xml.split("<Key>").skip(1) {
                    let key = part.split("</Key>").next().unwrap();
                    self.bucket.borrow_mut().remove(key);
                    out.push_str(&format!("<Deleted><Key>{key}</Key></Deleted>"));
                }
                out.push_str("</DeleteResult>");
                Self::reply(200, out)
            }
            _ => Self::reply(405, ""),
        }
    }
    fn admin_fos_etag(
        &self,
        request: FosRequest<'_>,
    ) -> Result<(FosResponse, Option<String>), String> {
        use md5::{Digest, Md5};
        let tag = format!("\"{}\"", hex::encode(Md5::digest(&request.body)));
        self.admin_fos(request)
            .map(|response| (response, Some(tag)))
    }
    fn admin_cdn_get(
        &self,
        service_id: &str,
        key: &str,
        cdn_key: &str,
        max: usize,
        _timeout: std::time::Duration,
    ) -> Result<FosResponse, String> {
        self.charge()?;
        assert_eq!((service_id, cdn_key), (SID, "ck"));
        self.cdn_gets.borrow_mut().push(key.into());
        if self.vcpu_after_get.get() > 0 || self.stall_keys.borrow().contains(key) {
            let add = self.vcpu_after_get.get().max(worker::VCPU_STOP_MS);
            self.vcpu.set(self.vcpu.get().saturating_add(add));
        }
        match self.bucket.borrow().get(key) {
            Some(body) if body.len() > max => Err("cdn_body_too_large".into()),
            Some(body) => Self::reply(200, body.clone()),
            None => Self::reply(404, ""),
        }
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
        if self.stall_after_get.get() && !self.cdn_gets.borrow().is_empty() {
            return std::time::Duration::from_secs(59);
        }
        std::time::Duration::ZERO
    }
    fn admin_wait(&self, _: u64) {}
    fn vcpu_ms(&self) -> u64 {
        self.vcpu.get()
    }
    fn worker_requests(&self) -> u32 {
        self.sent.get()
    }
    fn usage_agg(&self) -> Option<&UsageAgg> {
        Some(&self.usage)
    }
    fn on_log_shield(&self) -> bool {
        true
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
    fn worker_dispatch(
        &self,
        requests: &[(String, Value)],
        _deadline: std::time::Duration,
    ) -> Vec<Result<Value, String>> {
        if let Err(error) = self.secret("worker_secret") {
            return requests.iter().map(|_| Err(error.clone())).collect();
        }
        let mut out = Vec::new();
        for (path, body) in requests {
            if let Err(error) = self.charge() {
                out.push(Err(error));
                continue;
            }
            self.paths.borrow_mut().push(path.clone());
            self.sent.set(self.sent.get() + 1);
            if path == worker::RELAY_PATH {
                assert!(body.to_string().len() <= worker::RELAY_BODY_LIMIT);
                let fault = self
                    .relay_faults
                    .borrow_mut()
                    .pop_front()
                    .unwrap_or(Fault::Ok);
                if fault == Fault::Unreachable {
                    out.push(Err("worker_deadline".into()));
                    continue;
                }
                let request = fastly_log_analytics::http::Request {
                    method: "POST".into(),
                    path: path.clone(),
                    headers: [("x-flp-worker-key".to_owned(), "wk".to_owned())].into(),
                    body: body.to_string().into_bytes(),
                };
                let (_, response) = self.execution(|| {
                    self.relay_runs.set(self.relay_runs.get() + 1);
                    worker::handle_worker_relay(self, &request)
                });
                if fault == Fault::LostReply {
                    out.push(Err("worker_deadline".into()));
                } else if response.status != 200 {
                    out.push(Err(format!("worker_http_{}", response.status)));
                } else {
                    out.push(Ok(serde_json::from_slice(&response.body).unwrap()));
                }
                continue;
            }
            if path != worker::PATH && path != worker::RANGE_PATH {
                out.push(Ok(json!({"path": path, "echo": body})));
                continue;
            }
            let fault = self.faults.borrow_mut().pop_front().unwrap_or(Fault::Ok);
            if fault == Fault::Unreachable {
                out.push(Err("worker_deadline".into()));
                continue;
            }
            let request = fastly_log_analytics::http::Request {
                method: "POST".into(),
                path: path.clone(),
                headers: [("x-flp-worker-key".to_owned(), "wk".to_owned())].into(),
                body: body.to_string().into_bytes(),
            };
            let (_, response) = self.execution(|| {
                self.worker_runs.set(self.worker_runs.get() + 1);
                worker::handle_worker_ingest(self, &request)
            });
            if fault == Fault::LostReply {
                out.push(Err("worker_deadline".into()));
            } else if response.status != 200 {
                out.push(Err(format!("worker_http_{}", response.status)));
            } else {
                out.push(Ok(serde_json::from_slice(&response.body).unwrap()));
            }
        }
        out
    }
}

pub(super) fn service() -> Service {
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

pub(super) fn raw_key(minute_start: i64, name: &str, ext: &str) -> String {
    let t = time::OffsetDateTime::from_unix_timestamp(minute_start).unwrap();
    format!(
        "raw/request/year={:04}/month={:02}/day={:02}/hour={:02}/minute={:02}/{name}.{ext}",
        t.year(),
        t.month() as u8,
        t.day(),
        t.hour(),
        t.minute()
    )
}

/// Stores `n` lines (ids `<name>-<i>`, each padded with `pad` url bytes) and returns the ids.
pub(super) fn put_object(
    fake: &Fake,
    minute: i64,
    name: &str,
    n: usize,
    pad: usize,
    zstd: bool,
) -> Vec<String> {
    let mut seed = 0x517c_c1b7_2722_0a95u64;
    let mut text = Vec::new();
    let mut ids = Vec::new();
    for i in 0..n {
        let id = format!("{name}-{i:06}");
        let url = if !zstd && pad <= 100 {
            let rand_len = pad.min(8);
            format!(
                "/{}{}",
                random_text(&mut seed, rand_len),
                "a".repeat(pad - rand_len)
            )
        } else {
            format!("/{}", "a".repeat(pad))
        };
        text.extend_from_slice(
            json!({"timestamp": iso(minute + (i as i64 % 60)), "ip": id, "status": 200, "url": url})
                .to_string()
                .as_bytes(),
        );
        text.push(b'\n');
        ids.push(id);
    }
    let (body, ext) = if zstd {
        (zst(&text), "zst")
    } else {
        (gz(&text), "zst")
    };
    fake.bucket
        .borrow_mut()
        .insert(raw_key(minute, name, ext), body);
    ids
}

pub(super) fn tick(fake: &Fake) -> Result<ingest::Report, String> {
    fake.now.set(fake.now.get() + 120);
    fake.new_request();
    let buffer = LogBuffer::new(SID, "req");
    let report = ingest::run_capped(fake, &service(), AK, SK, Instant::now(), &buffer, 0);
    telemetry::flush_request_buffer(fake, &buffer);
    report
}

pub(super) fn plan(fake: &Fake) -> Value {
    fake.kv.borrow()[ingest::PLAN_KEY].1.clone()
}

pub(super) fn committed(fake: &Fake) -> bool {
    let kv = fake.kv.borrow();
    let watermark = kv
        .get(ingest::WATERMARK_KEY)
        .map(|(_, v)| v["run_id"].clone());
    kv.get(ingest::PLAN_KEY)
        .is_none_or(|(_, p)| watermark.as_ref() == Some(&p["plan_id"]))
}

/// Ticks until every raw object has been planned and committed; returns the tick count.
pub(super) fn drain(fake: &Fake, limit: usize) -> usize {
    for n in 1..=limit {
        let report = tick(fake);
        if let Ok(report) = &report {
            if committed(fake) && !report.more_pending && report.listed == 0 {
                return n;
            }
        }
    }
    panic!("not drained after {limit} ticks: {}", plan(fake));
}

pub(super) fn batches(bytes: &[u8]) -> Vec<RecordBatch> {
    ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))
        .unwrap()
        .build()
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

pub(super) fn processed_ips(fake: &Fake) -> Vec<String> {
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

pub(super) fn sorted(mut ids: Vec<String>) -> Vec<String> {
    ids.sort();
    ids
}

pub(super) fn deleteq_keys(fake: &Fake) -> Vec<String> {
    fake.kv
        .borrow()
        .iter()
        .filter(|(k, _)| k.starts_with(ingest::DELETEQ_PREFIX))
        .flat_map(|(_, (_, v))| {
            v["keys"]
                .as_array()
                .unwrap()
                .iter()
                .map(|k| k.as_str().unwrap().to_owned())
                .collect::<Vec<_>>()
        })
        .collect()
}

pub(super) fn segmented_plan_seen(fake: &Fake, seen: &RefCell<bool>) {
    let plan = plan(fake);
    if plan["segments"].as_object().is_some_and(|s| !s.is_empty()) {
        assert_eq!(plan["version"], 3);
        *seen.borrow_mut() = true;
    }
}

pub(super) fn random_text(seed: &mut u64, len: usize) -> String {
    use base64::Engine;
    let bytes: Vec<u8> = (0..len * 3 / 4)
        .map(|_| {
            *seed ^= *seed << 13;
            *seed ^= *seed >> 7;
            *seed ^= *seed << 17;
            *seed as u8
        })
        .collect();
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

pub(super) fn shard_request(keys: &[String]) -> worker::ShardRequest {
    let service = service();
    worker::ShardRequest {
        service_id: SID.into(),
        region: service.fos_region.clone(),
        bucket: service.fos_bucket.clone(),
        prefix: String::new(),
        plan_id: "0123456789ab".into(),
        shard: "0".into(),
        attempt: 1,
        stream: "request".into(),
        table: "request".into(),
        schema: RequestSchema::base().hash().into(),
        custom: Vec::new(),
        window_s: 60,
        keys: keys.to_vec(),
        stop_ms: 25_000,
        lease_until: 0,
        created_at: BASE as u64,
        segment: None,
        segment_bytes: 0,
    }
}

pub(super) fn run_worker(fake: &Fake, shard: &worker::ShardRequest, path: &str) -> Value {
    let request = fastly_log_analytics::http::Request {
        method: "POST".into(),
        path: path.into(),
        headers: [("x-flp-worker-key".to_owned(), "wk".to_owned())].into(),
        body: serde_json::to_vec(shard).unwrap(),
    };
    fake.new_request();
    let response = worker::handle_worker_ingest(fake, &request);
    assert_eq!(response.status, 200);
    serde_json::from_slice(&response.body).unwrap()
}

pub(super) fn put_minutes(fake: &Fake, count: usize, lines: usize) -> Vec<String> {
    let mut expected = Vec::new();
    for i in 0..count {
        expected.extend(put_object(
            fake,
            BASE,
            &format!("o{i:03}"),
            lines,
            10,
            false,
        ));
    }
    expected
}

pub(super) fn relay_request(fake: &Fake, requests: Value) -> fastly_log_analytics::http::Response {
    let request = fastly_log_analytics::http::Request {
        method: "POST".into(),
        path: worker::RELAY_PATH.into(),
        headers: [("x-flp-worker-key".to_owned(), "wk".to_owned())].into(),
        body: json!({"deadline_ms": 20_000, "requests": requests})
            .to_string()
            .into_bytes(),
    };
    fake.new_request();
    worker::handle_worker_relay(fake, &request)
}

pub(super) fn long_name(prefix: &str, len: usize) -> String {
    format!("{prefix}-{}", "k".repeat(len - prefix.len() - 1))
}

pub(super) fn held_keys(fake: &Fake) -> Vec<String> {
    fake.kv
        .borrow()
        .iter()
        .filter(|(k, _)| k.starts_with(ingest::DELETEQ_PREFIX))
        .flat_map(|(_, (_, v))| {
            v["held"]
                .as_array()
                .unwrap()
                .iter()
                .map(|h| h["key"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>()
        })
        .collect()
}
