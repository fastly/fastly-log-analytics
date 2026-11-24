#![allow(dead_code)]
use fastly_log_analytics::{
    admin::{self, fixture::FixtureAdapter, ApiRequest, FosRequest, FosResponse, Record},
    http::{Request, Response},
    setup::SetupStores,
    shell::Runtime,
};
use serde_json::{json, Value};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeSet,
};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc, Arc, Mutex,
    },
};

thread_local! {
    pub(super) static CALLS:Cell<u32>=const {Cell::new(0)};
    pub(super) static SECRETS:RefCell<BTreeSet<String>>=const {RefCell::new(BTreeSet::new())};
    pub(super) static ELAPSED_MS:Cell<u64>=const {Cell::new(0)};
    /// Every billable KV op as (billing class, key), in call order. `GetKey` is Class B,
    /// `SetKey` is Class A, `DeleteKey` is free; the Cache API is not a KV op at all.
    pub(super) static KV_CLASS:RefCell<Vec<(&'static str,String)>>=const {RefCell::new(Vec::new())};
    pub(super) static FAIL_KV_DELETE:Cell<bool>=const {Cell::new(false)};
    /// Deletes of this one key fail.
    pub(super) static FAIL_KV_DELETE_KEY:RefCell<Option<String>>=const {RefCell::new(None)};
    /// Every CAS of this one key loses, as to a concurrent writer.
    pub(super) static CONFLICT_CAS:RefCell<Option<String>>=const {RefCell::new(None)};
    /// Every FOS request as (method, key, query), in call order.
    pub(super) static FOS_LOG:RefCell<Vec<(String,String,String)>>=const {RefCell::new(Vec::new())};
    /// Every Core Cache surrogate purged, in call order.
    pub(super) static CACHE_PURGES:RefCell<Vec<String>>=const {RefCell::new(Vec::new())};
    /// Paged LISTs with LastModified and DeleteObjects POSTs, which the fixture lacks.
    pub(super) static RICH_FOS:Cell<bool>=const {Cell::new(false)};
    /// LastModified per object key, set on each PUT while `RICH_FOS` is on.
    pub(super) static MTIMES:RefCell<BTreeMap<String,u64>>=const {RefCell::new(BTreeMap::new())};
}

/// The fixture's objects as a paged S3 LIST, with the LastModified each was given.
pub(super) fn rich_list(objects: &BTreeMap<String, Vec<u8>>, query: &str) -> FosResponse {
    let q: BTreeMap<String, String> = url::form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect();
    let prefix = q.get("prefix").cloned().unwrap_or_default();
    let max: usize = q["max-keys"].parse().unwrap();
    let after = q
        .get("continuation-token")
        .or_else(|| q.get("start-after"))
        .cloned();
    let (mut contents, mut count, mut last, mut truncated) =
        (String::new(), 0, String::new(), false);
    for (key, body) in objects {
        if !key.starts_with(&prefix) || after.as_ref().is_some_and(|a| key <= a) {
            continue;
        }
        if count == max {
            truncated = true;
            break;
        }
        count += 1;
        last = key.clone();
        let at = MTIMES.with(|m| m.borrow().get(key).copied().unwrap_or(1_700_000_000));
        let at = time::OffsetDateTime::from_unix_timestamp(at as i64)
            .unwrap()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();
        contents.push_str(&format!(
            "<Contents><Key>{key}</Key><Size>{}</Size><LastModified>{at}</LastModified></Contents>",
            body.len()
        ));
    }
    let token = if truncated {
        format!("<NextContinuationToken>{last}</NextContinuationToken>")
    } else {
        String::new()
    };
    FosResponse {
        cdn_cache: None,
        status: 200,
        body: format!(
            "<ListBucketResult><IsTruncated>{truncated}</IsTruncated>{contents}{token}</ListBucketResult>"
        )
        .into_bytes(),
        retry_after: None,
    }
}
pub(super) fn record_kv(class: &'static str, key: &str) {
    KV_CLASS.with(|ops| ops.borrow_mut().push((class, key.to_owned())));
}
pub(super) fn charge() -> Result<(), String> {
    CALLS.with(|calls| {
        if calls.get() >= 32 {
            return Err("backend_budget_exhausted".into());
        }
        calls.set(calls.get() + 1);
        Ok(())
    })
}

pub(super) struct Platform {
    pub(super) adapter: Arc<FixtureAdapter>,
    pub(super) clock: AtomicU64,
    pub(super) random: AtomicU64,
    pub(super) before_lease: Mutex<Option<LeasePause>>,
    pub(super) api_delays: Mutex<Vec<ApiDelay>>,
    pub(super) api_timeouts: Mutex<Vec<std::time::Duration>>,
    pub(super) fos_delays: Mutex<Vec<FosDelay>>,
    pub(super) fos_timeouts: Mutex<Vec<std::time::Duration>>,
    pub(super) api_faults: Mutex<Vec<ApiFault>>,
    pub(super) api_attempts: AtomicU64,
    pub(super) late_requests: Mutex<Vec<LateRequest>>,
    pub(super) fail_timeout_checkpoint: AtomicBool,
    pub(super) fail_checkpoint_step: Mutex<Option<&'static str>>,
    pub(super) journal_delay_ms: AtomicU64,
    pub(super) reject_job_claim: AtomicBool,
    pub(super) shield: AtomicBool,
    pub(super) status_cache: Mutex<BTreeMap<String, fastly_log_analytics::shell::CoreCacheEntry>>,
    pub(super) enable_status_cache: AtomicBool,
    pub(super) metadata_reads: Mutex<Vec<String>>,
    pub(super) cron_during_extents: Mutex<Option<Value>>,
    /// Requests whose first fresh lease read fails, as a transient KV REST API error would.
    pub(super) failing_lease_reads: Mutex<u32>,
    pub(super) lease_read_failed: AtomicBool,
}
pub(super) struct LateRequest {
    pub(super) method: String,
    pub(super) path: String,
    pub(super) body: Option<Value>,
}
pub(super) struct ApiFault {
    pub(super) method: &'static str,
    pub(super) path: &'static str,
    pub(super) name: Option<&'static str>,
    pub(super) status: u16,
    pub(super) retry_after: Option<std::time::Duration>,
}
pub(super) struct ApiDelay {
    pub(super) method: &'static str,
    pub(super) path: &'static str,
    pub(super) millis: u64,
    pub(super) after_effect: bool,
}
pub(super) struct FosDelay {
    pub(super) method: &'static str,
    pub(super) millis: u64,
}
pub(super) struct LeasePause {
    pub(super) loaded: mpsc::Sender<()>,
    pub(super) resume: mpsc::Receiver<()>,
}
impl Platform {
    pub(super) fn new() -> Self {
        Self {
            adapter: Arc::new(FixtureAdapter::new()),
            clock: AtomicU64::new(1_800_000_000),
            random: AtomicU64::new(1),
            before_lease: Mutex::new(None),
            reject_job_claim: AtomicBool::new(false),
            shield: AtomicBool::new(false),
            status_cache: Default::default(),
            enable_status_cache: AtomicBool::new(false),
            metadata_reads: Default::default(),
            cron_during_extents: Default::default(),
            failing_lease_reads: Default::default(),
            lease_read_failed: AtomicBool::new(false),
            api_delays: Mutex::new(Vec::new()),
            api_timeouts: Mutex::new(Vec::new()),
            fos_delays: Mutex::new(Vec::new()),
            fos_timeouts: Mutex::new(Vec::new()),
            api_faults: Mutex::new(Vec::new()),
            api_attempts: AtomicU64::new(0),
            late_requests: Mutex::new(Vec::new()),
            fail_timeout_checkpoint: AtomicBool::new(false),
            fail_checkpoint_step: Mutex::new(None),
            journal_delay_ms: AtomicU64::new(0),
        }
    }
    pub(super) fn call(&self, method: &str, path: &str, value: Value) -> Response {
        ELAPSED_MS.with(|elapsed| elapsed.set(0));
        CALLS.with(|calls| calls.set(7));
        self.lease_read_failed.store(false, Ordering::SeqCst);
        SECRETS.with(|secrets| {
            *secrets.borrow_mut() = BTreeSet::from(["front_key".into(), "session_key".into()])
        });
        let response = admin::handle(
            self,
            "fixtureAdmin",
            &Request {
                method: method.into(),
                path: path.into(),
                headers: BTreeMap::new(),
                body: value.to_string().into_bytes(),
            },
        );
        assert!(CALLS.with(Cell::get) <= 32);
        assert!(SECRETS.with(|secrets| secrets.borrow().len()) <= 5);
        response
    }
    pub(super) fn json(&self, method: &str, path: &str, value: Value) -> Value {
        let response = self.call(method, path, value);
        assert_eq!(
            response.status,
            200,
            "{}",
            String::from_utf8_lossy(&response.body)
        );
        serde_json::from_slice(&response.body).unwrap()
    }
    pub(super) fn provision(&self) -> Value {
        self.json("POST","/api/admin/provision/execute",json!({
            "token":"fixture-token","service_id":"fixtureService","fos_bucket":"fos-fixture-logs","fos_region":"us-east-1",
            "log_fields":{"groups":["A","C"],"custom_fields":[],"field_overrides":{},"field_limits":{}}
        }))
    }
    pub(super) fn drain(&self, mut job: Value) -> Value {
        for _ in 0..150 {
            if job["status"] == "complete" {
                return job;
            }
            assert_ne!(job["status"], "failed", "{job}");
            if let Some(at) = job["retry_at"].as_u64() {
                self.clock.fetch_max(at, Ordering::SeqCst);
            }
            job = self.json(
                "POST",
                &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
                json!({}),
            )["job"]
                .clone();
        }
        panic!("job did not finish: {job}");
    }
    pub(super) fn service(&self) -> Value {
        self.json("GET", "/api/admin/services/fixtureService", json!({}))
    }
    pub(super) fn until(&self, mut job: Value, step: &str) -> Value {
        for _ in 0..150 {
            if job["step"] == step {
                return job;
            }
            assert_ne!(job["status"], "failed", "{job}");
            if let Some(at) = job["retry_at"].as_u64() {
                self.clock.fetch_max(at, Ordering::SeqCst);
            }
            job = self.json(
                "POST",
                &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
                json!({}),
            )["job"]
                .clone();
        }
        panic!("missing step {step}: {job}");
    }
    pub(super) fn retry(&self, job: &Value) -> Value {
        self.clock.fetch_add(61, Ordering::SeqCst);
        self.json(
            "POST",
            &format!("/api/admin/jobs/{}/retry", job["id"].as_str().unwrap()),
            json!({}),
        )["job"]
            .clone()
    }
    pub(super) fn exhaust(&self, mut job: Value) -> Value {
        for _ in 0..40 {
            if job["status"] != "pending" {
                return job;
            }
            if let Some(at) = job["retry_at"].as_u64() {
                self.clock.fetch_max(at, Ordering::SeqCst);
            }
            job = self.json(
                "POST",
                &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
                json!({}),
            )["job"]
                .clone();
        }
        panic!("job kept retrying: {job}");
    }
    pub(super) fn fail_checkpoint_before(&self, mut job: Value, step: &'static str) -> Response {
        *self.fail_checkpoint_step.lock().unwrap() = Some(step);
        for _ in 0..150 {
            if let Some(at) = job["retry_at"].as_u64() {
                self.clock.fetch_max(at, Ordering::SeqCst);
            }
            let response = self.call(
                "POST",
                &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
                json!({}),
            );
            if response.status == 503 {
                return response;
            }
            assert_eq!(response.status, 200);
            job = serde_json::from_slice::<Value>(&response.body).unwrap()["job"].clone();
            assert_ne!(job["status"], "failed", "{job}");
        }
        panic!("checkpoint was not reached: {job}");
    }
}
impl SetupStores for Platform {
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
impl Runtime for Platform {
    fn config(&self, key: &str) -> Result<Option<String>, String> {
        Ok(Some(
            match key {
                "flp_service_id" => "fixtureApp",
                "worker_service_id" => "fixtureWorker",
                "front_service_id" => "fixtureFront",
                "install_name" => "fixture",
                "secret_store_id" => "fixtureSecrets",
                "buckets_dictionary_id" => "fixtureBuckets",
                "credentials_dictionary_id" => "fixtureCredentials",
                "front_auth_dictionary_id" => "fixtureAuth",
                "public_base_url" => "https://logs.example.com",
                _ => "fixtureValue",
            }
            .into(),
        ))
    }
    fn secret(&self, key: &str) -> Result<Option<String>, String> {
        let fresh = SECRETS.with(|secrets| secrets.borrow_mut().insert(key.into()));
        if fresh {
            charge()?;
        }
        if SECRETS.with(|secrets| secrets.borrow().len()) > 5 {
            return Err("secret_read_budget_exhausted".into());
        }
        if key == "cdn_key" {
            return Ok(Some("fixture-cdn-key".into()));
        }
        if key == "front_key" {
            return Ok(Some("fixture-front-key".into()));
        }
        if key == "session_key" {
            return Ok(Some("fixture-session-signing-material-32-bytes".into()));
        }
        if key == "fastly_token" {
            return Ok(Some("fixture-token".into()));
        }
        self.adapter.secret(key)
    }
    fn random(&self) -> Result<[u8; 32], String> {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&self.random.fetch_add(1, Ordering::SeqCst).to_be_bytes());
        Ok(bytes)
    }
    fn now(&self) -> u64 {
        self.clock.load(Ordering::SeqCst)
    }
    fn on_log_shield(&self) -> bool {
        self.shield.load(Ordering::SeqCst)
    }
    fn checks(&self, _: bool) -> Result<Value, String> {
        Ok(json!({}))
    }
    fn versions(&self) -> Result<Value, String> {
        Ok(json!({}))
    }
    fn compute_service_id(&self) -> Option<String> {
        Some("fixtureApp".into())
    }
    fn compute_service_version(&self) -> Option<String> {
        Some("1".into())
    }
    fn admin_get(&self, key: &str) -> Result<Option<Record>, String> {
        if key == fastly_log_analytics::ingest::WATERMARK_KEY {
            if let Some(value) = self.cron_during_extents.lock().unwrap().take() {
                self.adapter.cache_invalidate("cron:state");
                let old = self.adapter.get("cron:state")?;
                assert!(self.adapter.cas(
                    "cron:state",
                    old.as_ref().map(|r| r.generation.as_str()),
                    &value
                )?);
            }
        }
        self.metadata_reads.lock().unwrap().push(key.to_owned());
        if let Some(cached) = self.adapter.cached_get(key) {
            return Ok(cached);
        }
        charge()?;
        record_kv("read", key);
        self.adapter.get(key)
    }
    fn admin_get_fresh(&self, key: &str, _: &str) -> Result<Option<Record>, String> {
        charge()?;
        record_kv("read_api", key);
        if key.starts_with("reconcile_lock:") && !self.lease_read_failed.load(Ordering::SeqCst) {
            let mut failing = self.failing_lease_reads.lock().unwrap();
            if *failing > 0 {
                *failing -= 1;
                self.lease_read_failed.store(true, Ordering::SeqCst);
                return Err("admin_state_confirmation_rejected".into());
            }
        }
        self.adapter.confirm(key)
    }
    fn admin_cas(
        &self,
        key: &str,
        generation: Option<&str>,
        value: &Value,
    ) -> Result<bool, String> {
        self.adapter.cache_invalidate(key);
        let pause = if key.starts_with("reconcile_lock:") {
            self.before_lease.lock().map_err(|_| "test_lock")?.take()
        } else {
            None
        };
        if let Some(pause) = pause {
            pause.loaded.send(()).map_err(|_| "test_send")?;
            pause
                .resume
                .recv_timeout(std::time::Duration::from_secs(5))
                .map_err(|_| "test_resume_timeout")?;
        }
        charge()?;
        record_kv("write", key);
        if CONFLICT_CAS.with(|k| k.borrow().as_deref() == Some(key)) {
            return Ok(false);
        }
        if key.starts_with("job:")
            && value["lease_until"].as_u64().is_some_and(|until| until > 0)
            && value["uncertain_operations"]
                .as_array()
                .is_some_and(|operations| !operations.is_empty())
        {
            let delay = self.journal_delay_ms.swap(0, Ordering::SeqCst);
            ELAPSED_MS.with(|elapsed| elapsed.set(elapsed.get() + delay));
        }
        if key.starts_with("job:")
            && value["status"] == "running"
            && self.reject_job_claim.swap(false, Ordering::SeqCst)
        {
            return Ok(false);
        }
        if key.starts_with("job:") && value["lease_until"] == 0 {
            let mut checkpoint = self.fail_checkpoint_step.lock().map_err(|_| "test_lock")?;
            if checkpoint.is_some_and(|step| value["step"] == step) {
                checkpoint.take();
                return Err("injected_checkpoint_failure".into());
            }
        }
        self.adapter.paced_cas(key, generation, value)
    }
    fn admin_api(&self, request: ApiRequest<'_>) -> Result<admin::ApiResponse, String> {
        charge()?;
        self.api_attempts.fetch_add(1, Ordering::SeqCst);
        {
            let mut faults = self.api_faults.lock().map_err(|_| "test_lock")?;
            if let Some(index) = faults.iter().position(|fault| {
                fault.method == request.method
                    && fault.path == request.path
                    && fault.name.is_none_or(|name| {
                        request
                            .body
                            .as_ref()
                            .is_some_and(|body| body["name"] == name)
                    })
            }) {
                let fault = faults.remove(index);
                return Ok(admin::ApiResponse {
                    status: fault.status,
                    body: json!({}),
                    retry_after: fault.retry_after,
                });
            }
        }
        let delay = {
            let mut delays = self.api_delays.lock().map_err(|_| "test_lock")?;
            delays
                .iter()
                .position(|delay| delay.method == request.method && delay.path == request.path)
                .map(|index| delays.remove(index))
        };
        let Some(delay) = delay else {
            return self.adapter.api(request).map(Into::into);
        };
        self.api_timeouts
            .lock()
            .map_err(|_| "test_lock")?
            .push(request.timeout);
        let timeout_ms = u64::try_from(request.timeout.as_millis()).map_err(|_| "test_timeout")?;
        ELAPSED_MS.with(|elapsed| elapsed.set(elapsed.get() + delay.millis.min(timeout_ms)));
        if delay.after_effect {
            let result = self.adapter.api(request);
            if delay.millis > timeout_ms {
                Err("backend_deadline".into())
            } else {
                result.map(Into::into)
            }
        } else if delay.millis > timeout_ms {
            if !matches!(request.method, "GET" | "HEAD") {
                self.late_requests
                    .lock()
                    .map_err(|_| "test_lock")?
                    .push(LateRequest {
                        method: request.method.into(),
                        path: request.path.into(),
                        body: request.body,
                    });
            }
            if self.fail_timeout_checkpoint.swap(false, Ordering::SeqCst) {
                self.adapter.fail_next("KV_CAS", "job:", false);
            }
            Err("backend_deadline".into())
        } else {
            self.adapter.api(request).map(Into::into)
        }
    }
    fn admin_fos(&self, request: FosRequest<'_>) -> Result<FosResponse, String> {
        if let Some(value) = self.cron_during_extents.lock().unwrap().take() {
            let old = self.adapter.get("cron:state")?;
            assert!(self.adapter.cas(
                "cron:state",
                old.as_ref().map(|r| r.generation.as_str()),
                &value
            )?);
        }
        charge()?;
        FOS_LOG.with(|log| {
            log.borrow_mut().push((
                request.method.into(),
                request.key.into(),
                request.query.into(),
            ))
        });
        self.fos_timeouts.lock().unwrap().push(request.timeout);
        let delay = {
            let mut delays = self.fos_delays.lock().unwrap();
            delays
                .iter()
                .position(|delay| delay.method == request.method)
                .map(|index| delays.remove(index))
        };
        if let Some(delay) = delay {
            let granted = request.timeout.as_millis() as u64;
            ELAPSED_MS.with(|elapsed| elapsed.set(elapsed.get() + delay.millis.min(granted)));
            if delay.millis >= granted {
                return Err("backend_deadline".into());
            }
        }
        if RICH_FOS.with(Cell::get) {
            if request.method == "PUT" && !request.key.is_empty() {
                MTIMES.with(|m| m.borrow_mut().insert(request.key.into(), self.now()));
            }
            if request.method == "GET" && request.key.is_empty() {
                return Ok(rich_list(
                    &self.adapter.bucket_objects(request.bucket),
                    request.query,
                ));
            }
            if request.method == "POST" && request.query == "delete" {
                let xml = String::from_utf8(request.body.clone()).unwrap();
                let mut out = String::from("<DeleteResult>");
                for part in xml.split("<Key>").skip(1) {
                    let key = part.split("</Key>").next().unwrap();
                    self.adapter.fos(FosRequest {
                        method: "DELETE",
                        key,
                        query: "",
                        body: Vec::new(),
                        content_md5: None,
                        ..request
                    })?;
                    out.push_str(&format!("<Deleted><Key>{key}</Key></Deleted>"));
                }
                out.push_str("</DeleteResult>");
                return Ok(FosResponse {
                    cdn_cache: None,
                    status: 200,
                    body: out.into_bytes(),
                    retry_after: None,
                });
            }
        }
        self.adapter.fos(request)
    }
    fn admin_fos_etag(
        &self,
        request: FosRequest<'_>,
    ) -> Result<(FosResponse, Option<String>), String> {
        use md5::{Digest, Md5};
        let etag = (request.method == "PUT")
            .then(|| format!("\"{}\"", hex::encode(Md5::digest(&request.body))));
        let response = self.admin_fos(request)?;
        let etag = etag.filter(|_| (200..300).contains(&response.status));
        Ok((response, etag))
    }
    fn admin_remaining_calls(&self) -> u32 {
        CALLS.with(|calls| 32u32.saturating_sub(calls.get()))
    }
    fn core_cache_lookup(
        &self,
        key: &str,
    ) -> Result<
        Option<fastly_log_analytics::shell::CoreCacheEntry>,
        fastly_log_analytics::shell::CoreCacheError,
    > {
        self.metadata_reads.lock().unwrap().push(key.to_owned());
        if !self.enable_status_cache.load(Ordering::SeqCst) || !key.starts_with("flp-status-") {
            return Err(fastly_log_analytics::shell::CoreCacheError::Unsupported);
        }
        Ok(self.status_cache.lock().unwrap().get(key).cloned())
    }
    fn core_cache_replace(
        &self,
        key: &str,
        _: &[&str],
        _: std::time::Duration,
        update: &dyn Fn(
            Option<fastly_log_analytics::shell::CoreCacheEntry>,
        ) -> fastly_log_analytics::shell::CoreCacheEntry,
    ) -> Result<(), fastly_log_analytics::shell::CoreCacheError> {
        if !self.enable_status_cache.load(Ordering::SeqCst) || !key.starts_with("flp-status-") {
            return Err(fastly_log_analytics::shell::CoreCacheError::Unsupported);
        }
        let old = self.status_cache.lock().unwrap().get(key).cloned();
        self.status_cache
            .lock()
            .unwrap()
            .insert(key.into(), update(old));
        Ok(())
    }
    fn core_cache_purge(
        &self,
        surrogate: &str,
    ) -> Result<(), fastly_log_analytics::shell::CoreCacheError> {
        CACHE_PURGES.with(|purges| purges.borrow_mut().push(surrogate.to_owned()));
        Ok(())
    }
    fn kv_list(
        &self,
        prefix: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<fastly_log_analytics::shell::KvPage, String> {
        charge()?;
        Ok(fastly_log_analytics::shell::KvPage::from_sorted_keys(
            self.adapter.records().into_keys(),
            prefix,
            cursor,
            limit,
        ))
    }
    fn kv_delete(&self, key: &str) -> Result<(), String> {
        charge()?;
        if FAIL_KV_DELETE.with(Cell::get)
            || FAIL_KV_DELETE_KEY.with(|k| k.borrow().as_deref() == Some(key))
        {
            return Err("kv_delete_failed".into());
        }
        record_kv("delete", key);
        self.adapter.delete(key)
    }
    fn admin_elapsed(&self, _: std::time::Instant) -> std::time::Duration {
        std::time::Duration::from_millis(ELAPSED_MS.with(Cell::get))
    }
    fn admin_wait(&self, millis: u64) {
        ELAPSED_MS.with(|elapsed| elapsed.set(elapsed.get() + millis));
        self.adapter.advance_kv_clock(millis);
    }
}
