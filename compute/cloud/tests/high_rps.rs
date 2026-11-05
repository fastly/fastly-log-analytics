//! High-RPS ingest (Task 25): raw objects split into decompressed-byte segments converted by
//! separate workers, multipart window output and relayed fan-out past the coordinator's
//! direct dispatch width.
#![allow(dead_code)]

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
    telemetry::{self, usage::UsageAgg, LogBuffer, LogKind},
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{json, Value};
use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, BTreeSet, VecDeque},
    time::Instant,
};

const BASE: i64 = 1_800_000_000;
const SID: &str = "AbCdEfGhIjKlMnOpQrSt12";
const BUDGET: u32 = 32;
const AK: &str = "AKIDHIGHRPS";
const SK: &str = "real-secret-key";

fn iso(seconds: i64) -> String {
    fastly_log_analytics::ingest::rfc3339(seconds).replace("Z", "+0000")
}

fn line(at: i64, id: &str) -> String {
    json!({"timestamp": iso(at), "ip": id, "status": 200, "url": "/a/b/c?d=e"}).to_string()
}

fn gz(text: &[u8]) -> Vec<u8> {
    zstd::stream::encode_all(text, 1).unwrap()
}

fn zst(text: &[u8]) -> Vec<u8> {
    zstd::stream::encode_all(text, 3).unwrap()
}

mod common;

fn ips(rows: &convert::ObjectRows) -> Vec<String> {
    let mut out = Vec::new();
    for batch in rows.windows.values() {
        out.extend((0..batch.num_rows()).filter_map(|i| common::col_str(batch, "ip", i)));
    }
    out
}

/// Ids `0..n` as NDJSON, with blank lines and one over-long line mixed in.
fn ndjson(n: usize) -> (Vec<u8>, Vec<String>) {
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

fn convert_segments(object: &[u8], cuts: &[u64]) -> (Vec<String>, u64, u64) {
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

#[test]
fn segments_count_every_line_exactly_once_for_gzip_and_zstd() {
    let (text, ids) = ndjson(3000);
    let total = text.len() as u64;
    let whole = convert::convert_object(&gz(&text), 60, BASE).unwrap();
    // Cuts on line starts, mid-line, inside the over-long line, past the end and at 0.
    let first_line = text.iter().position(|&b| b == b'\n').unwrap() as u64 + 1;
    let long_at = text.windows(4).position(|w| w == b"xxxx").unwrap() as u64;
    let cut_sets: Vec<Vec<u64>> = vec![
        vec![first_line],
        vec![1, 2, 3],
        vec![total / 7, total / 3, long_at + 5, total / 2, total - 1],
        vec![0, total / 2, total, total + 100],
        (1..40).map(|i| i * total / 40 + i % 3).collect(),
    ];
    for object in [gz(&text), zst(&text)] {
        for cuts in &cut_sets {
            let (mut seen, lines, bad) = convert_segments(&object, cuts);
            assert_eq!(lines, whole.lines_read, "cuts {cuts:?}");
            assert_eq!(bad, whole.bad_lines);
            assert_eq!(seen.len(), ids.len(), "cuts {cuts:?}");
            seen.sort();
            assert_eq!(seen, ids);
            assert_eq!(seen.iter().collect::<BTreeSet<_>>().len(), ids.len());
        }
    }
}

#[test]
fn segment_content_cap_and_measured_length() {
    let (text, _) = ndjson(2000);
    let object = gz(&text);
    let total = text.len() as u64;
    assert_eq!(
        convert::decompressed_len(&object, total, &|| false),
        Ok(total)
    );
    assert_eq!(
        convert::decompressed_len(&object, total - 1, &|| false),
        Err(convert::ConvertError::TooLarge)
    );
    let open = |start, cap| {
        convert::convert_object_range(
            &object,
            60,
            BASE,
            Segment { start, end: None },
            cap,
            RequestSchema::base(),
            &|| false,
        )
    };
    // The cap counts only the segment's own bytes, not the skipped prefix.
    assert!(open(total / 2, total / 2 + 1024).is_ok());
    assert_eq!(
        open(1, total / 2).err(),
        Some(convert::ConvertError::TooLarge)
    );
    let mut broken = object.clone();
    let mid = broken.len() / 2;
    broken[mid..mid + 64].fill(0);
    assert!(convert::decompressed_len(&broken, u64::MAX, &|| false).is_err());
}

#[test]
fn prefix_decompression_checks_its_budget_every_checkpoint_and_stops() {
    let (text, _) = ndjson(40_000);
    let total = text.len() as u64;
    assert!(total > 3 * convert::CHECKPOINT_BYTES);
    let tail = Segment {
        start: total - 1000,
        end: None,
    };
    for object in [gz(&text), zst(&text)] {
        let checks = Cell::new(0u64);
        let never = || {
            checks.set(checks.get() + 1);
            false
        };
        assert_eq!(convert::decompressed_len(&object, total, &never), Ok(total));
        assert!(checks.get() >= total / convert::CHECKPOINT_BYTES);
        checks.set(0);
        let rows = convert::convert_object_range(
            &object,
            60,
            BASE,
            tail,
            convert::MAX_DECOMPRESSED_BYTES,
            RequestSchema::base(),
            &never,
        )
        .unwrap();
        assert!(rows.lines_read > 0);
        assert!(checks.get() >= total / convert::CHECKPOINT_BYTES);

        let second = || {
            checks.set(checks.get() + 1);
            checks.get() >= 2
        };
        checks.set(0);
        assert_eq!(
            convert::decompressed_len(&object, total, &second),
            Err(convert::ConvertError::Stopped)
        );
        assert_eq!(checks.get(), 2);
        checks.set(0);
        assert_eq!(
            convert::convert_object_range(
                &object,
                60,
                BASE,
                tail,
                convert::MAX_DECOMPRESSED_BYTES,
                RequestSchema::base(),
                &second,
            )
            .err(),
            Some(convert::ConvertError::Stopped)
        );
        assert_eq!(checks.get(), 2);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    Ok,
    /// The request never reaches the execution.
    Unreachable,
    /// The execution runs (and writes) but its answer is lost.
    LostReply,
}

type Upload = (String, BTreeMap<u32, Vec<u8>>);

/// The SDK's KV read cap (`sdk.rs` `bounded_json`).
const KV_READ_LIMIT: usize = 2_097_152;

#[derive(Default)]
struct Fake {
    kv: RefCell<BTreeMap<String, (u64, Value)>>,
    bucket: RefCell<BTreeMap<String, Vec<u8>>>,
    uploads: RefCell<BTreeMap<String, Upload>>,
    completed: RefCell<Vec<(String, usize)>>,
    upload_seq: Cell<u64>,
    calls: Cell<u32>,
    secrets_read: RefCell<BTreeSet<String>>,
    now: Cell<u64>,
    config: RefCell<BTreeMap<String, String>>,
    cdn_gets: RefCell<Vec<String>>,
    stall_after_get: Cell<bool>,
    seq: Cell<u64>,
    cache: RefCell<BTreeMap<String, CoreCacheEntry>>,
    usage: UsageAgg,
    crash_after: Cell<Option<u32>>,
    faults: RefCell<VecDeque<Fault>>,
    relay_faults: RefCell<VecDeque<Fault>>,
    depth: Cell<u32>,
    sent: Cell<u32>,
    worker_runs: Cell<u32>,
    relay_runs: Cell<u32>,
    max_calls: Cell<u32>,
    paths: RefCell<Vec<String>>,
    plan_peak: Cell<usize>,
    vcpu: Cell<u64>,
    vcpu_after_get: Cell<u64>,
    stall_keys: RefCell<BTreeSet<String>>,
}

impl Fake {
    fn new() -> Self {
        let fake = Self::default();
        fake.now.set(BASE as u64 + 3600);
        fake
    }
    fn set(&self, name: &str, value: &str) {
        self.config.borrow_mut().insert(name.into(), value.into());
    }
    fn charge(&self) -> Result<(), String> {
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
    fn new_request(&self) {
        self.calls.set(0);
        self.sent.set(0);
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
    fn reply(status: u16, body: impl Into<Vec<u8>>) -> Result<FosResponse, String> {
        Ok(FosResponse {
            cdn_cache: None,
            status,
            body: body.into(),
            retry_after: None,
        })
    }
    /// Runs `handle` as a fresh execution with its own budget and Secret Store reads.
    fn execution(
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
    fn multipart(&self, request: &FosRequest<'_>, id: &str) -> Result<FosResponse, String> {
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

fn raw_key(minute_start: i64, name: &str, ext: &str) -> String {
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
fn put_object(
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

fn tick(fake: &Fake) -> Result<ingest::Report, String> {
    fake.now.set(fake.now.get() + 120);
    fake.new_request();
    let buffer = LogBuffer::new(SID, "req");
    let report = ingest::run_capped(fake, &service(), AK, SK, Instant::now(), &buffer, 0);
    telemetry::flush_request_buffer(fake, &buffer);
    report
}

fn plan(fake: &Fake) -> Value {
    fake.kv.borrow()[ingest::PLAN_KEY].1.clone()
}

fn committed(fake: &Fake) -> bool {
    let kv = fake.kv.borrow();
    let watermark = kv
        .get(ingest::WATERMARK_KEY)
        .map(|(_, v)| v["run_id"].clone());
    kv.get(ingest::PLAN_KEY)
        .is_none_or(|(_, p)| watermark.as_ref() == Some(&p["plan_id"]))
}

/// Ticks until every raw object has been planned and committed; returns the tick count.
fn drain(fake: &Fake, limit: usize) -> usize {
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

fn sorted(mut ids: Vec<String>) -> Vec<String> {
    ids.sort();
    ids
}

fn deleteq_keys(fake: &Fake) -> Vec<String> {
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

fn segmented_plan_seen(fake: &Fake, seen: &RefCell<bool>) {
    let plan = plan(fake);
    if plan["segments"].as_object().is_some_and(|s| !s.is_empty()) {
        assert_eq!(plan["version"], 3);
        *seen.borrow_mut() = true;
    }
}

#[test]
fn large_objects_split_into_segments_convert_every_line_once() {
    let fake = Fake::new();
    fake.set("ingest_segment_kib", "64");
    let minute = BASE;
    let mut expected = put_object(&fake, minute, "a-small", 50, 10, false);
    expected.extend(put_object(&fake, minute, "b-big", 6000, 40, false));
    expected.extend(put_object(&fake, minute, "c-big", 5000, 40, true));
    expected.extend(put_object(&fake, minute, "d-small", 30, 10, false));
    let big = raw_key(minute, "b-big", "zst");
    assert!(fake.bucket.borrow()[&big].len() * 8 > 2 * 64 * 1024);
    tick(&fake).unwrap();
    let first = plan(&fake);
    assert_eq!(first["version"], 3, "{first}");
    assert!(first["segments"].as_object().unwrap().len() >= 4);
    drain(&fake, 10);
    assert_eq!(processed_ips(&fake), sorted(expected));
    assert!(fake.cdn_gets.borrow().iter().filter(|k| **k == big).count() >= 2);
    let queued = deleteq_keys(&fake);
    assert_eq!(queued.iter().filter(|k| **k == big).count(), 1);
    assert_eq!(queued.len(), 4);
    assert!(fake.paths.borrow().iter().any(|p| p == worker::RANGE_PATH));
}

#[test]
fn small_objects_keep_version_two_plans_and_the_whole_object_path() {
    let fake = Fake::new();
    fake.set("ingest_segment_kib", "64");
    let expected = put_object(&fake, BASE, "a", 40, 10, false);
    tick(&fake).unwrap();
    assert_eq!(plan(&fake)["version"], 2);
    assert!(plan(&fake).get("segments").is_none());
    drain(&fake, 4);
    assert_eq!(processed_ips(&fake), sorted(expected));
    assert!(fake.paths.borrow().iter().all(|p| p == worker::PATH));
}

#[test]
fn an_object_that_decompresses_past_the_cap_is_measured_and_segmented() {
    let fake = Fake::new();
    fake.set("ingest_segment_kib", "64");
    let expected = put_object(&fake, BASE, "dense", 400, 4000, false);
    let key = raw_key(BASE, "dense", "zst");
    let gz_len = fake.bucket.borrow()[&key].len();
    assert!(
        gz_len * 8 <= 2 * 64 * 1024,
        "gz {gz_len} would be pre-segmented"
    );
    tick(&fake).unwrap();
    let after = plan(&fake);
    assert_eq!(after["version"], 3, "{after}");
    assert!(after["segments"].as_object().unwrap().len() >= 10);
    drain(&fake, 10);
    assert_eq!(processed_ips(&fake), sorted(expected));
    assert_eq!(deleteq_keys(&fake), vec![key]);
}

#[test]
fn crashes_and_lost_replies_mid_split_neither_lose_nor_duplicate_lines() {
    let fake = Fake::new();
    fake.set("ingest_segment_kib", "64");
    let mut expected = put_object(&fake, BASE, "big", 6000, 40, false);
    expected.extend(put_object(&fake, BASE + 60, "next", 3000, 40, false));
    // The first segment writes but its reply is lost; the second never runs.
    fake.faults.borrow_mut().extend([
        Fault::LostReply,
        Fault::Unreachable,
        Fault::Ok,
        Fault::LostReply,
    ]);
    tick(&fake).unwrap();
    assert!(!committed(&fake));
    // The next tick dies after re-taking the lease, before any worker answers.
    fake.crash_after.set(Some(4));
    assert!(tick(&fake).is_err());
    fake.crash_after.set(None);
    drain(&fake, 12);
    let got = processed_ips(&fake);
    assert_eq!(got.len(), expected.len());
    assert_eq!(got, sorted(expected));
}

fn random_text(seed: &mut u64, len: usize) -> String {
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

fn shard_request(keys: &[String]) -> worker::ShardRequest {
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

fn run_worker(fake: &Fake, shard: &worker::ShardRequest, path: &str) -> Value {
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

#[test]
fn a_segment_whose_prefix_skip_outlasts_the_worker_stop_fails_visibly_and_keeps_its_raw() {
    let fake = Fake::new();
    let (text, _) = ndjson(40_000);
    let key = raw_key(BASE, "skip", "zst");
    let body = gz(&text);
    fake.bucket.borrow_mut().insert(key.clone(), body.clone());
    let mut shard = shard_request(std::slice::from_ref(&key));
    shard.segment = Some(Segment {
        start: text.len() as u64 - 1000,
        end: None,
    });
    shard.segment_bytes = 64 * 1024;
    fake.stall_after_get.set(true);
    let reply = run_worker(&fake, &shard, worker::RANGE_PATH);
    assert_eq!(reply["status"], "error", "{reply}");
    assert_eq!(reply["error"], "worker_budget_stopped");
    assert!(fake.keys("processed/").is_empty());
    assert_eq!(fake.bucket.borrow()[&key], body);
}

#[test]
fn a_large_window_is_written_as_multipart_parts() {
    use md5::{Digest, Md5};
    let fake = Fake::new();
    let mut seed = 0x9e37_79b9_7f4a_7c15u64;
    let mut keys = Vec::new();
    let mut expected = Vec::new();
    for name in ["p0", "p1"] {
        let mut text = Vec::new();
        for i in 0..2000 {
            let id = format!("{name}-{i:05}");
            let url = format!("/{}", random_text(&mut seed, 4000));
            text.extend_from_slice(
                json!({"timestamp": iso(BASE + i % 60), "ip": id, "status": 200, "url": url})
                    .to_string()
                    .as_bytes(),
            );
            text.push(b'\n');
            expected.push(id);
        }
        let key = raw_key(BASE, name, "zst");
        let body = gz(&text);
        assert!(body.len() < 8 * 1024 * 1024);
        fake.bucket.borrow_mut().insert(key.clone(), body);
        keys.push(key);
    }
    let reply = run_worker(&fake, &shard_request(&keys), worker::PATH);
    assert_eq!(reply["status"], "ok", "{reply}");
    let output = &reply["result"]["outputs"][0];
    let key = output["key"].as_str().unwrap().to_owned();
    let completed = fake.completed.borrow().clone();
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].0, key);
    assert!(completed[0].1 >= 2);
    let body = fake.bucket.borrow()[&key].clone();
    assert!(body.len() > ingest::compaction::PART_BYTES);
    assert_eq!(output["md5"], hex::encode(Md5::digest(&body)));
    assert_eq!(output["bytes"], body.len() as u64);
    assert!(fake.uploads.borrow().is_empty());
    let mut got = processed_ips(&fake);
    got.dedup();
    assert_eq!(got, sorted(expected));
    assert!(reply["calls"].as_u64().unwrap() <= u64::from(BUDGET));
}

#[test]
fn a_large_window_without_spare_calls_falls_back_to_one_put() {
    let fake = Fake::new();
    let mut seed = 7u64;
    let mut keys = Vec::new();
    for name in ["q0", "q1", "q2"] {
        let mut text = Vec::new();
        for i in 0..1900 {
            let url = format!("/{}", random_text(&mut seed, 4000));
            text.extend_from_slice(
                json!({"timestamp": iso(BASE + i % 60), "ip": format!("{name}-{i}"), "status": 200, "url": url})
                    .to_string()
                    .as_bytes(),
            );
            text.push(b'\n');
        }
        let key = raw_key(BASE, name, "zst");
        fake.bucket.borrow_mut().insert(key.clone(), gz(&text));
        keys.push(key);
    }
    // With 22 objects only 6 calls remain after the manifest: a 3-part upload needs 7.
    for i in 0..19 {
        put_object(&fake, BASE, &format!("r{i:02}"), 1, 10, false);
        keys.push(raw_key(BASE, &format!("r{i:02}"), "zst"));
    }
    keys.sort();
    let reply = run_worker(&fake, &shard_request(&keys), worker::PATH);
    assert_eq!(reply["status"], "ok", "{reply}");
    assert!(
        reply["result"]["outputs"][0]["bytes"].as_u64().unwrap()
            > 2 * ingest::compaction::PART_BYTES as u64
    );
    assert_eq!(fake.upload_seq.get(), 0);
    assert_eq!(processed_ips(&fake).len(), 3 * 1900 + 19);
}

/// `count` small objects in one minute: shards fill by object count, `SHARD_MAX_OBJECTS` each.
fn put_minutes(fake: &Fake, count: usize, lines: usize) -> Vec<String> {
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

#[test]
fn relayed_fanout_runs_more_shards_than_worker_calls() {
    let fake = Fake::new();
    fake.set("ingest_fanout", "2");
    fake.set("ingest_relay", "4");
    let expected = put_minutes(&fake, 8 * worker::SHARD_MAX_OBJECTS, 3);
    let report = tick(&fake).unwrap();
    assert_eq!(report.shards_dispatched, 8, "{}", plan(&fake));
    assert_eq!(fake.relay_runs.get(), 2);
    let catalog_workers = report
        .worker_executions_metrics
        .iter()
        .filter(|sample| sample.kind == "catalog")
        .count() as u32;
    assert_eq!(
        fake.worker_runs.get(),
        report.shards_dispatched + catalog_workers
    );
    assert_eq!(
        fake.paths
            .borrow()
            .iter()
            .filter(|p| *p == worker::RELAY_PATH)
            .count(),
        2
    );
    assert!(committed(&fake));
    drain(&fake, 4);
    assert_eq!(processed_ips(&fake), sorted(expected));
}

#[test]
fn capacity_telemetry_retains_relay_and_lost_child_execution_coverage() {
    let fake = Fake::new();
    fake.set("ingest_fanout", "2");
    fake.set("ingest_relay", "4");
    put_minutes(&fake, 8 * worker::SHARD_MAX_OBJECTS, 3);
    fake.relay_faults.borrow_mut().push_back(Fault::LostReply);
    let report = tick(&fake).unwrap();
    let value = serde_json::to_value(&report).unwrap();
    let samples = value["worker_executions_metrics"].as_array().unwrap();
    assert_eq!(
        samples
            .iter()
            .filter(|s| s["kind"] == "ingest_relay")
            .count(),
        2
    );
    assert_eq!(
        samples
            .iter()
            .filter(|s| s["kind"] == "request_ingest")
            .count(),
        8
    );
    assert_eq!(samples.iter().filter(|s| s["answered"] == false).count(), 5);
    for sample in samples.iter().filter(|s| s["answered"] == false) {
        assert!(sample["vcpu_ms"].is_null(), "{sample}");
        assert!(sample["calls"].is_null(), "{sample}");
        assert!(sample["heap_mib"].is_null(), "{sample}");
    }
    assert!(samples.iter().all(|s| s["heap_mib"].is_null()));
}

#[test]
fn cron_runs_count_the_workers_each_relay_dispatched_even_when_its_reply_is_lost() {
    let fake = Fake::new();
    fake.set("ingest_fanout", "2");
    fake.set("ingest_relay", "4");
    put_minutes(&fake, 8 * worker::SHARD_MAX_OBJECTS, 3);
    fake.relay_faults.borrow_mut().push_back(Fault::LostReply);
    fake.kv.borrow_mut().insert(
        "admin:install".into(),
        (1, json!({"service_id": SID, "job_id": null, "fence": 0})),
    );
    fake.kv.borrow_mut().insert(
        format!("svc:{SID}"),
        (1, serde_json::to_value(service()).unwrap()),
    );
    fake.new_request();
    fastly_log_analytics::admin::record_tick(&fake, "clock", "clock").unwrap();
    assert_eq!(fake.relay_runs.get(), 2, "{:?}", fake.paths.borrow());
    assert_eq!(fake.worker_runs.get(), 8);
    fake.new_request();
    let logged: i64 = telemetry::read_live(&fake, SID, LogKind::CronRuns, None, Some(2000), None)
        .unwrap()
        .rows
        .iter()
        .map(|r| r["worker_executions"].as_i64().unwrap_or(0))
        .sum();
    assert_eq!(
        logged,
        fake.paths.borrow().len() as i64,
        "{:?}",
        fake.paths.borrow()
    );
}

#[test]
fn without_relay_the_width_stays_the_worker_call_count() {
    let fake = Fake::new();
    fake.set("ingest_fanout", "2");
    let expected = put_minutes(&fake, 8 * worker::SHARD_MAX_OBJECTS, 3);
    let report = tick(&fake).unwrap();
    assert_eq!(report.shards_dispatched, 2);
    assert_eq!(fake.relay_runs.get(), 0);
    drain(&fake, 12);
    assert_eq!(processed_ips(&fake), sorted(expected));
}

#[test]
fn a_failed_or_lost_relay_retries_its_shards_without_loss_or_duplicates() {
    let fake = Fake::new();
    fake.set("ingest_fanout", "2");
    fake.set("ingest_relay", "4");
    let expected = put_minutes(&fake, 8 * worker::SHARD_MAX_OBJECTS, 3);
    fake.relay_faults
        .borrow_mut()
        .extend([Fault::Unreachable, Fault::LostReply]);
    tick(&fake).unwrap();
    assert!(!committed(&fake));
    assert_eq!(fake.worker_runs.get(), 4);
    let first = plan(&fake);
    assert_eq!(
        first["results"].as_object().map_or(0, |r| r.len()),
        0,
        "{first}"
    );
    assert_ne!(first["lease_until"], 0);
    drain(&fake, 6);
    let got = processed_ips(&fake);
    assert_eq!(got.len(), expected.len());
    assert_eq!(got, sorted(expected));
}

fn relay_request(fake: &Fake, requests: Value) -> fastly_log_analytics::http::Response {
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

#[test]
fn a_relay_forwards_only_shard_requests_within_its_bounds() {
    let fake = Fake::new();
    let shard = serde_json::to_value(shard_request(&[raw_key(BASE, "x", "zst")])).unwrap();
    for path in [worker::RELAY_PATH, "/_internal/delete", "/_internal/stage"] {
        let response = relay_request(&fake, json!([[worker::PATH, shard], [path, shard]]));
        assert_eq!(response.status, 400, "{path}");
    }
    let many: Vec<Value> = (0..=worker::MAX_RELAY_SHARDS)
        .map(|_| json!([worker::PATH, shard]))
        .collect();
    assert_eq!(relay_request(&fake, Value::from(many)).status, 400);
    assert!(fake.paths.borrow().is_empty());
    let mut unauthorized = fastly_log_analytics::http::Request {
        method: "POST".into(),
        path: worker::RELAY_PATH.into(),
        headers: Default::default(),
        body: b"{}".to_vec(),
    };
    assert_eq!(
        worker::handle_worker_relay(&fake, &unauthorized).status,
        403
    );
    unauthorized
        .headers
        .insert("x-flp-worker-key".into(), "wk".into());
    assert_eq!(
        worker::handle_worker_relay(&fake, &unauthorized).status,
        400
    );
}

/// A raw object name `len` bytes long.
fn long_name(prefix: &str, len: usize) -> String {
    format!("{prefix}-{}", "k".repeat(len - prefix.len() - 1))
}

fn held_keys(fake: &Fake) -> Vec<String> {
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

#[test]
fn a_fine_segment_size_cuts_a_large_object_into_at_most_the_object_segment_cap() {
    let fake = Fake::new();
    fake.set("ingest_segment_kib", "64");
    let mut seed = 7;
    let mut text = Vec::new();
    let mut expected = Vec::new();
    for i in 0..20_000 {
        let id = format!("big-{i:06}");
        let url = format!("/{}", random_text(&mut seed, 80));
        text.extend_from_slice(
            json!({"timestamp": iso(BASE + (i % 60)), "ip": id, "status": 200, "url": url})
                .to_string()
                .as_bytes(),
        );
        text.push(b'\n');
        expected.push(id);
    }
    let name = long_name("big", 900);
    let key = raw_key(BASE, &name, "zst");
    let body = gz(&text);
    assert!(body.len() as u64 * 8 / (64 * 1024) > 3 * 22);
    fake.bucket.borrow_mut().insert(key.clone(), body);
    tick(&fake).unwrap();
    let first = plan(&fake);
    let segments = first["segments"].as_object().unwrap().len();
    assert!((3..=22).contains(&segments), "{segments} segments");
    drain(&fake, 30);
    assert_eq!(processed_ips(&fake), sorted(expected));
    assert_eq!(deleteq_keys(&fake), vec![key]);
    assert!(fake.plan_peak.get() <= KV_READ_LIMIT);
}

#[test]
fn a_resegment_past_the_plan_bound_holds_its_object_visibly_and_loses_no_line() {
    let fake = Fake::new();
    fake.set("ingest_segment_kib", "64");
    fake.set("ingest_relay", "16");
    let mut by_key = BTreeMap::new();
    for i in 0..100 {
        let name = long_name(&format!("dense{i:03}"), 900);
        let ids = put_object(&fake, BASE, &name, 600, 1000, false);
        by_key.insert(raw_key(BASE, &name, "zst"), ids);
    }
    drain(&fake, 40);
    let held = held_keys(&fake);
    assert!(!held.is_empty(), "no object was held");
    let mut expected: Vec<String> = by_key
        .iter()
        .filter(|(k, _)| !held.contains(k))
        .flat_map(|(_, ids)| ids.clone())
        .collect();
    expected.sort();
    assert_eq!(processed_ips(&fake), expected);
    for key in &held {
        assert!(fake.bucket.borrow().contains_key(key));
    }
    let mut accounted = deleteq_keys(&fake);
    accounted.extend(held);
    accounted.sort();
    assert_eq!(accounted, by_key.keys().cloned().collect::<Vec<_>>());
    assert!(
        fake.plan_peak.get() <= KV_READ_LIMIT,
        "{}",
        fake.plan_peak.get()
    );
}

#[test]
fn straggler_replan_fits_within_kv_limit_and_preserves_sweep_cursor() {
    let fake = Fake::new();
    let prior_cursor = raw_key(BASE, "cursor-before-straggler-batch", "zst");
    let mark_key = raw_key(BASE + 3600, "watermark", "zst");
    fake.kv.borrow_mut().insert(
        ingest::WATERMARK_KEY.into(),
        (
            1,
            json!({
                "key": mark_key,
                "run_id": "wm0",
                "at": BASE as u64 + 3600,
                "sweep": {"after": prior_cursor, "at": BASE as u64 + 3600},
                "passes": [],
                "swept": [],
            }),
        ),
    );
    let mut raw_keys = Vec::new();
    let mut sizes = Vec::new();
    for i in 0..240 {
        let name = long_name(&format!("strag{i:03}"), 900);
        put_object(&fake, BASE + 60, &name, 2, 10, false);
        let key = raw_key(BASE + 60, &name, "zst");
        let sz = fake.bucket.borrow()[&key].len() as u64;
        raw_keys.push(key);
        sizes.push(sz);
    }
    let mut shards = BTreeMap::new();
    for (idx, chunk) in raw_keys.chunks(worker::SHARD_MAX_OBJECTS).enumerate() {
        shards.insert(idx.to_string(), chunk.to_vec());
    }
    let straggler_plan = json!({
        "version": 2,
        "plan_id": "abandonedstr",
        "nonce": "0102030405060708",
        "stream": "request",
        "table": "request",
        "schema": RequestSchema::base().hash(),
        "custom": [],
        "window_s": 60,
        "sizes": sizes,
        "raw_keys": raw_keys,
        "shards": shards,
        "segments": {},
        "segment_bytes": 0,
        "results": {},
        "failures": {"0": 3},
        "attempt": 3,
        "lease_until": 0,
        "created_at": BASE as u64 + 3600,
        "abandoning": "shard_failures",
        "replans": 3,
        "straggler": true,
        "sweep_next": "",
    });
    fake.kv
        .borrow_mut()
        .insert(ingest::PLAN_KEY.into(), (1, straggler_plan));
    tick(&fake).unwrap();
    let replanned = plan(&fake);
    assert_eq!(replanned["straggler"], true, "{replanned}");
    assert!(replanned["abandoning"].is_null(), "{replanned}");
    let kept = replanned["raw_keys"].as_array().unwrap().len();
    assert!(kept > 0 && kept < raw_keys.len(), "kept={kept}");
    assert_eq!(replanned["sweep_next"], prior_cursor);
    assert!(
        fake.plan_peak.get() <= KV_READ_LIMIT,
        "plan_peak={}",
        fake.plan_peak.get()
    );
}

#[test]
fn worker_segment_prefix_skip_stops_at_vcpu_budget_before_wall_stop() {
    let fake = Fake::new();
    let (text, _) = ndjson(40_000);
    let key = raw_key(BASE, "vcpu-skip", "zst");
    let body = gz(&text);
    fake.bucket.borrow_mut().insert(key.clone(), body.clone());
    let mut shard = shard_request(std::slice::from_ref(&key));
    shard.segment = Some(Segment {
        start: text.len() as u64 - 1000,
        end: None,
    });
    shard.segment_bytes = 64 * 1024;
    // Wall clock does not advance (stall_after_get is false), but worker vCPU reaches 800 ms.
    fake.vcpu_after_get.set(worker::VCPU_STOP_MS);
    let reply = run_worker(&fake, &shard, worker::RANGE_PATH);
    assert_eq!(reply["status"], "error", "{reply}");
    assert_eq!(reply["error"], "worker_budget_stopped");
    assert!(fake.keys("processed/").is_empty());
    assert_eq!(fake.bucket.borrow()[&key], body);
}

#[test]
fn persistently_budget_stopping_single_segment_shard_is_held_on_replan_and_other_shards_commit() {
    let fake = Fake::new();
    fake.set("ingest_segment_kib", "64");
    let (bad_text, _) = ndjson(25_000);
    assert!(bad_text.len() as u64 > 2 * convert::CHECKPOINT_BYTES);
    let bad_key = raw_key(BASE, "a-bad-seg", "zst");
    let bad_body = gz(&bad_text);
    assert!(bad_body.len() * 8 > 2 * 64 * 1024);
    fake.bucket
        .borrow_mut()
        .insert(bad_key.clone(), bad_body.clone());
    let good_ids = put_object(&fake, BASE + 60, "b-good", 40, 10, false);
    let good_key = raw_key(BASE + 60, "b-good", "zst");
    // Fetching `bad_key` burns the worker vCPU budget, so segments whose prefix >= CHECKPOINT_BYTES
    // fail with `worker_budget_stopped` on every attempt.
    fake.stall_keys.borrow_mut().insert(bad_key.clone());

    drain(&fake, 15);
    assert!(committed(&fake));
    assert_eq!(processed_ips(&fake), sorted(good_ids));
    assert_eq!(deleteq_keys(&fake), vec![good_key]);
    assert_eq!(held_keys(&fake), vec![bad_key.clone()]);
    assert_eq!(fake.bucket.borrow()[&bad_key], bad_body);
    let held_reasons: Vec<String> = fake
        .kv
        .borrow()
        .iter()
        .filter(|(k, _)| k.starts_with(ingest::DELETEQ_PREFIX))
        .flat_map(|(_, (_, v))| {
            v["held"]
                .as_array()
                .unwrap()
                .iter()
                .map(|h| {
                    assert_eq!(h["status"], "failed");
                    h["reason"].as_str().unwrap().to_owned()
                })
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(held_reasons, vec!["worker_budget_stopped".to_owned()]);
}
