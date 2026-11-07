use fastly_log_analytics::admin::{valid_bucket, valid_prefix, valid_service_id};
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
    static CALLS:Cell<u32>=const {Cell::new(0)};
    static SECRETS:RefCell<BTreeSet<String>>=const {RefCell::new(BTreeSet::new())};
    static ELAPSED_MS:Cell<u64>=const {Cell::new(0)};
    /// Every billable KV op as (billing class, key), in call order. `GetKey` is Class B,
    /// `SetKey` is Class A, `DeleteKey` is free; the Cache API is not a KV op at all.
    static KV_CLASS:RefCell<Vec<(&'static str,String)>>=const {RefCell::new(Vec::new())};
    static FAIL_KV_DELETE:Cell<bool>=const {Cell::new(false)};
    /// Deletes of this one key fail.
    static FAIL_KV_DELETE_KEY:RefCell<Option<String>>=const {RefCell::new(None)};
    /// Every CAS of this one key loses, as to a concurrent writer.
    static CONFLICT_CAS:RefCell<Option<String>>=const {RefCell::new(None)};
    /// Every FOS request as (method, key, query), in call order.
    static FOS_LOG:RefCell<Vec<(String,String,String)>>=const {RefCell::new(Vec::new())};
    /// Every Core Cache surrogate purged, in call order.
    static CACHE_PURGES:RefCell<Vec<String>>=const {RefCell::new(Vec::new())};
    /// Paged LISTs with LastModified and DeleteObjects POSTs, which the fixture lacks.
    static RICH_FOS:Cell<bool>=const {Cell::new(false)};
    /// LastModified per object key, set on each PUT while `RICH_FOS` is on.
    static MTIMES:RefCell<BTreeMap<String,u64>>=const {RefCell::new(BTreeMap::new())};
}

/// The fixture's objects as a paged S3 LIST, with the LastModified each was given.
fn rich_list(objects: &BTreeMap<String, Vec<u8>>, query: &str) -> FosResponse {
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
fn record_kv(class: &'static str, key: &str) {
    KV_CLASS.with(|ops| ops.borrow_mut().push((class, key.to_owned())));
}
fn charge() -> Result<(), String> {
    CALLS.with(|calls| {
        if calls.get() >= 32 {
            return Err("backend_budget_exhausted".into());
        }
        calls.set(calls.get() + 1);
        Ok(())
    })
}

struct Platform {
    adapter: Arc<FixtureAdapter>,
    clock: AtomicU64,
    random: AtomicU64,
    before_lease: Mutex<Option<LeasePause>>,
    api_delays: Mutex<Vec<ApiDelay>>,
    api_timeouts: Mutex<Vec<std::time::Duration>>,
    fos_delays: Mutex<Vec<FosDelay>>,
    fos_timeouts: Mutex<Vec<std::time::Duration>>,
    api_faults: Mutex<Vec<ApiFault>>,
    api_attempts: AtomicU64,
    late_requests: Mutex<Vec<LateRequest>>,
    fail_timeout_checkpoint: AtomicBool,
    fail_checkpoint_step: Mutex<Option<&'static str>>,
    journal_delay_ms: AtomicU64,
    reject_job_claim: AtomicBool,
    shield: AtomicBool,
    status_cache: Mutex<BTreeMap<String, fastly_log_analytics::shell::CoreCacheEntry>>,
    enable_status_cache: AtomicBool,
    metadata_reads: Mutex<Vec<String>>,
    cron_during_extents: Mutex<Option<Value>>,
}
struct LateRequest {
    method: String,
    path: String,
    body: Option<Value>,
}
struct ApiFault {
    method: &'static str,
    path: &'static str,
    name: Option<&'static str>,
    status: u16,
    retry_after: Option<std::time::Duration>,
}
struct ApiDelay {
    method: &'static str,
    path: &'static str,
    millis: u64,
    after_effect: bool,
}
struct FosDelay {
    method: &'static str,
    millis: u64,
}
struct LeasePause {
    loaded: mpsc::Sender<()>,
    resume: mpsc::Receiver<()>,
}
impl Platform {
    fn new() -> Self {
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
    fn call(&self, method: &str, path: &str, value: Value) -> Response {
        ELAPSED_MS.with(|elapsed| elapsed.set(0));
        CALLS.with(|calls| calls.set(7));
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
    fn json(&self, method: &str, path: &str, value: Value) -> Value {
        let response = self.call(method, path, value);
        assert_eq!(
            response.status,
            200,
            "{}",
            String::from_utf8_lossy(&response.body)
        );
        serde_json::from_slice(&response.body).unwrap()
    }
    fn provision(&self) -> Value {
        self.json("POST","/api/admin/provision/execute",json!({
            "token":"fixture-token","service_id":"fixtureService","fos_bucket":"fos-fixture-logs","fos_region":"us-east-1",
            "log_fields":{"groups":["A","C"],"custom_fields":[],"field_overrides":{},"field_limits":{}}
        }))
    }
    fn drain(&self, mut job: Value) -> Value {
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
    fn service(&self) -> Value {
        self.json("GET", "/api/admin/services/fixtureService", json!({}))
    }
    fn until(&self, mut job: Value, step: &str) -> Value {
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
    fn retry(&self, job: &Value) -> Value {
        self.clock.fetch_add(61, Ordering::SeqCst);
        self.json(
            "POST",
            &format!("/api/admin/jobs/{}/retry", job["id"].as_str().unwrap()),
            json!({}),
        )["job"]
            .clone()
    }
    fn exhaust(&self, mut job: Value) -> Value {
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
    fn fail_checkpoint_before(&self, mut job: Value, step: &'static str) -> Response {
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

#[test]
fn provisioning_waits_for_the_persisted_service_generation() {
    let platform = Platform::new();
    platform.adapter.delay_kv_visibility(1);
    let result = platform.provision();
    let saved = platform.adapter.records();
    assert_eq!(result["generation"], saved["svc:fixtureService"].generation);
}

#[test]
fn job_claim_and_release_do_not_use_stale_post_write_records() {
    let platform = Platform::new();
    let result = platform.provision();
    platform.adapter.delay_kv_visibility(u32::MAX);
    let job = platform.json(
        "POST",
        &format!(
            "/api/admin/jobs/{}/step",
            result["job"]["id"].as_str().unwrap()
        ),
        json!({}),
    )["job"]
        .clone();
    assert_eq!(job["status"], "pending");
    let saved = platform.adapter.records();
    assert_eq!(
        saved["reconcile_lock:fixtureService"].value["expires_at"],
        0
    );
    assert_eq!(
        saved[&format!("job:{}", job["id"].as_str().unwrap())].value["step"],
        job["step"]
    );
}

#[test]
fn unpropagated_writes_fail_explicitly_without_guessing_a_generation() {
    let platform = Platform::new();
    platform.json("GET", "/api/admin/services", json!({}));
    platform.adapter.delay_confirmation_visibility(100);
    let context = admin::transport::Context::new(&platform);
    context.confirm_with_token("fixture-token");
    context.cas("probe", None, &json!({"state":"new"})).unwrap();
    let error = context.get("probe").err().expect("unconfirmed write");
    assert_eq!((error.status, error.code), (503, "state_propagating"));
    assert_eq!(platform.adapter.records()["probe"].value["state"], "new");
}

#[test]
fn post_write_confirmation_rejects_another_writers_record() {
    let platform = Platform::new();
    platform.json("GET", "/api/admin/services", json!({}));
    let context = admin::transport::Context::new(&platform);
    context.confirm_with_token("fixture-token");
    context
        .cas("probe", None, &json!({"state":"ours"}))
        .unwrap();
    let generation = platform.adapter.records()["probe"].generation.clone();
    assert!(platform
        .adapter
        .cas("probe", Some(&generation), &json!({"state":"theirs"}))
        .unwrap());
    let error = context.get("probe").err().expect("concurrent write");
    assert_eq!((error.status, error.code), (409, "generation_conflict"));
    assert_eq!(platform.adapter.records()["probe"].value["state"], "theirs");
}

#[test]
fn confirmation_distinguishes_an_older_identical_value_from_our_write() {
    let platform = Platform::new();
    platform.json("GET", "/api/admin/services", json!({}));
    let first = admin::transport::Context::new(&platform);
    first.cas("probe", None, &json!({"state":"ours"})).unwrap();
    let old = platform.adapter.records()["probe"].clone();
    let second = admin::transport::Context::new(&platform);
    second
        .cas("probe", Some(&old.generation), &json!({"state":"theirs"}))
        .unwrap();
    let previous = platform.adapter.records()["probe"].generation.clone();
    let context = admin::transport::Context::new(&platform);
    context.confirm_with_token("fixture-token");
    context
        .cas("probe", Some(&previous), &json!({"state":"ours"}))
        .unwrap();
    let actual = platform.adapter.records()["probe"].generation.clone();
    platform.adapter.stale_kv_read("probe", 1, Some(old));
    let observed = context.get("probe").unwrap().unwrap();
    assert_eq!(observed.generation, actual);
}

#[test]
fn service_identifiers_cannot_escape_api_paths() {
    assert!(valid_service_id("0123456789ABCDEFGHIJKL"));
    for value in ["", "../service", "a/b", "é", "a?token=x", "a%2fb"] {
        assert!(!valid_service_id(value), "{value}");
    }
}

#[test]
fn bucket_and_prefix_validation_rejects_ambiguous_names() {
    assert!(valid_bucket("fos-example-logs"));
    assert!(valid_prefix("customer/logs/"));
    for value in ["a", "Bucket", "a..b", "192.168.1.1", "../bucket"] {
        assert!(!valid_bucket(value), "{value}");
    }
    for value in ["/raw", "../raw", "a/../b", "x?secret", "a%2fb", "a\\b"] {
        assert!(!valid_prefix(value), "{value}");
    }
}

#[test]
fn provisioning_reconcile_and_teardown_are_durable_and_secret_free() {
    let platform = Platform::new();
    let result = platform.provision();
    let job = platform.drain(result["job"].clone());
    assert_eq!(job["status"], "complete");
    let service = platform.service();
    assert_eq!(service["service"]["status"], "active");
    let endpoints = platform.adapter.resources("fixtureService", "logging/s3");
    assert_eq!(endpoints.len(), 1);
    assert_eq!(endpoints[0]["compression_codec"], "zstd");
    assert!(endpoints[0].get("gzip_level").is_none());
    assert_eq!(endpoints[0]["format_version"], 2);
    assert_eq!(endpoints[0]["message_type"], "blank");
    assert_eq!(
        endpoints[0]["path"],
        "raw/request/year=%Y/month=%m/day=%d/hour=%H/minute=%M/"
    );
    let records = serde_json::to_string(&platform.adapter.records()).unwrap();
    for secret in [
        "fixture-token",
        "fixture-ak-",
        "fixture-sk-",
        "fixture-cdn-key",
    ] {
        assert!(!records.contains(secret), "{secret} persisted");
    }
    let result=platform.json("POST","/api/admin/provision/teardown",json!({
            "service_id":"fixtureService","confirm_service_id":"fixtureService","token":"fixture-token",
            "generation":service["generation"],"delete_fos_bucket":true,"delete_data":true
        }));
    platform.drain(result["job"].clone());
    assert_eq!(platform.service()["service"]["status"], "torn_down");
    assert_eq!(platform.adapter.key_count(), 0);
    assert_eq!(
        platform.json("GET", "/api/services", json!({}))["services"],
        json!([])
    );
    platform.drain(platform.provision()["job"].clone());
    assert_eq!(platform.service()["service"]["status"], "active");
}

#[test]
fn preflight_rejects_bad_tokens_accounts_self_and_managed_resources_before_mutations() {
    for (token, sid, expected) in [
        ("bad-token", "fixtureService", 403),
        ("fixture-read-only", "fixtureService", 403),
        ("fixture-wrong-account", "fixtureService", 403),
        ("fixture-token", "fixtureApp", 400),
        ("fixture-token", "../escape", 400),
    ] {
        let platform = Platform::new();
        let response = platform.call(
            "POST",
            "/api/admin/provision/validate",
            json!({"token":token,"service_id":sid}),
        );
        assert_eq!(
            response.status,
            expected,
            "{}",
            String::from_utf8_lossy(&response.body)
        );
        assert!(platform
            .adapter
            .calls()
            .iter()
            .all(|(m, _)| m == "GET" || m == "KV_GET"));
    }
    let platform = Platform::new();
    platform.adapter.seed_resource(
        "fixtureService",
        "snippet",
        json!({"name":"Fastly Log Analytics - vcl_recv","content":"customer-owned","type":"recv"}),
    );
    assert_eq!(platform.call("POST","/api/admin/provision/execute",json!({"token":"fixture-token","service_id":"fixtureService","fos_bucket":"fos-test-logs","fos_region":"us-east-1"})).status,409);
    assert_eq!(platform.adapter.key_count(), 0);
}

#[test]
fn pending_install_blocks_a_second_service_and_stale_config_updates() {
    let platform = Platform::new();
    platform.provision();
    assert_eq!(platform.call("POST","/api/admin/provision/execute",json!({"token":"fixture-token","service_id":"fixtureOther","fos_bucket":"fos-other-logs","fos_region":"us-east-1"})).status,409);
    let response = platform.call(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation":"stale","log_period":1}),
    );
    assert_eq!(response.status, 409);
    assert_eq!(platform.service()["service"]["log_period"], 60);
}

#[test]
fn log_period_longer_than_the_object_window_cap_is_rejected() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    let service = platform.service();
    let response = platform.call(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation":service["generation"],"log_period":3600}),
    );
    assert_eq!(response.status, 400);
    let body: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(body["error"], "log_period_exceeds_window_capacity");
    assert_eq!(platform.service()["service"]["log_period"], 60);
    platform.json(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation":service["generation"],"log_period":300}),
    );
}

#[test]
fn rum_log_period_longer_than_the_object_window_cap_is_rejected() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    let service = platform.service();
    let rum = |log_period: u32| {
        json!({"enabled": true, "log_period": log_period, "custom_condition": "",
            "faro_version": "2.10.0", "endpoint_name": "Fastly RUM Logs",
            "capture_vitals": true, "capture_performance": true, "capture_errors": true,
            "capture_events": true})
    };
    let response = platform.call(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation":service["generation"],"rum":rum(3600)}),
    );
    assert_eq!(response.status, 400);
    let body: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(body["error"], "rum_log_period_exceeds_window_capacity");
    platform.json(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation":service["generation"],"rum":rum(300)}),
    );
}

#[test]
fn dry_run_and_validate_only_never_activate_and_preserve_unmanaged_configuration() {
    let platform = Platform::new();
    platform.adapter.seed_resource("fixtureService","snippet",json!({"name":"customer-snippet","type":"recv","priority":20,"content":"set req.http.Customer = \"kept\";"}));
    platform.drain(platform.provision()["job"].clone());
    let service = platform.service();
    let result = platform.json(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation":service["generation"],"sample_rate":50}),
    );
    platform.adapter.clear_calls();
    let job = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation":result["generation"],"dry_run":true}),
    );
    platform.drain(job["job"].clone());
    assert!(!platform
        .adapter
        .calls()
        .iter()
        .any(|(m, p)| m == "PUT" && (p.ends_with("/clone") || p.ends_with("/activate"))));
    let service = platform.service();
    platform.adapter.clear_calls();
    let job = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation":service["generation"],"activate":false}),
    );
    platform.drain(job["job"].clone());
    assert!(!platform
        .adapter
        .calls()
        .iter()
        .any(|(_, p)| p.ends_with("/activate")));
    assert!(platform
        .adapter
        .resources("fixtureService", "snippet")
        .iter()
        .any(|v| v["name"] == "customer-snippet"
            && v["content"] == "set req.http.Customer = \"kept\";"));
}

#[test]
fn service_checkpoint_gap_resumes_without_rotating_credentials() {
    let platform = Platform::new();
    let job = platform.until(platform.provision()["job"].clone(), "bucket_probe");
    let response = platform.fail_checkpoint_before(job.clone(), "bucket_create");
    assert_eq!(response.status, 503);
    let resumed = platform.retry(&job);
    assert_ne!(resumed["status"], "failed", "{resumed}");
    platform.drain(resumed);
    assert_eq!(platform.adapter.key_count(), 1);
    let created = platform
        .adapter
        .calls()
        .iter()
        .filter(|(m, p)| m == "POST" && p == "/resources/object-storage/access-keys")
        .count();
    assert_eq!(
        created, 2,
        "temporary + permanent, without duplicate rotation"
    );
}

#[test]
fn partial_secret_pair_write_recovers_only_the_scoped_managed_key() {
    let platform = Platform::new();
    let job = platform.until(platform.provision()["job"].clone(), "key");
    platform.api_faults.lock().unwrap().push(ApiFault {
        method: "PUT",
        path: "/resources/stores/secret/fixtureSecrets/secrets",
        name: Some("fos_ak.fixtureService"),
        status: 400,
        retry_after: None,
    });
    let result = platform.json(
        "POST",
        &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
        json!({}),
    );
    assert_eq!(result["job"]["status"], "failed");
    let resumed = platform.retry(&job);
    platform.drain(resumed);
    assert_eq!(platform.adapter.key_count(), 1);
    let endpoint = &platform.adapter.resources("fixtureService", "logging/s3")[0];
    assert!(endpoint["access_key"]
        .as_str()
        .unwrap()
        .starts_with("fixture-ak-"));
    assert!(endpoint["secret_key"]
        .as_str()
        .unwrap()
        .starts_with("fixture-sk-"));
}

#[test]
fn lost_activation_response_is_recovered_without_a_second_activation() {
    let platform = Platform::new();
    let job = platform.until(platform.provision()["job"].clone(), "activate");
    let path = format!(
        "/service/fixtureService/version/{}/activate",
        job["draft_version"].as_u64().unwrap()
    );
    platform.adapter.fail_next("PUT", &path, true);
    let result = platform.json(
        "POST",
        &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
        json!({}),
    );
    assert_eq!(result["job"]["status"], "failed");
    platform.drain(platform.retry(&job));
    assert_eq!(
        platform
            .adapter
            .calls()
            .iter()
            .filter(|(m, p)| m == "PUT" && p == &path)
            .count(),
        1
    );
}

#[test]
fn validation_failure_and_config_generation_change_leave_draft_inactive() {
    let platform = Platform::new();
    let job = platform.until(platform.provision()["job"].clone(), "validate");
    platform.adapter.invalid_draft(true);
    let result = platform.json(
        "POST",
        &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
        json!({}),
    );
    assert_eq!(result["job"]["error"], "draft_validation_failed");
    assert!(!platform
        .adapter
        .calls()
        .iter()
        .any(|(_, p)| p.starts_with("/service/fixtureService/") && p.ends_with("/activate")));
    platform.adapter.invalid_draft(false);
    let job = platform.retry(&job);
    assert_eq!(job["step"], "activate");
    let mut record = platform.adapter.get("svc:fixtureService").unwrap().unwrap();
    record.value["sample_rate"] = 7.into();
    assert!(platform
        .adapter
        .cas(
            "svc:fixtureService",
            Some(&record.generation),
            &record.value
        )
        .unwrap());
    let result = platform.json(
        "POST",
        &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
        json!({}),
    );
    assert_eq!(result["job"]["error"], "service_changed_rerun");
    assert!(!platform
        .adapter
        .calls()
        .iter()
        .any(|(_, p)| p.starts_with("/service/fixtureService/") && p.ends_with("/activate")));
}

#[test]
fn adopted_bucket_protects_unrelated_objects_and_whole_bucket_deletion() {
    let platform = Platform::new();
    platform
        .adapter
        .seed_object("fos-fixture-logs", "unrelated/customer-data");
    platform.drain(platform.provision()["job"].clone());
    let service = platform.service();
    assert_eq!(service["service"]["bucket_adopted"], true);
    let result=platform.call("POST","/api/admin/provision/teardown",json!({
        "service_id":"fixtureService","confirm_service_id":"fixtureService","generation":service["generation"],
        "token":"fixture-token","delete_fos_bucket":true,"delete_data":true
    }));
    assert_eq!(result.status, 409);
    assert_eq!(
        platform.adapter.object_keys("fos-fixture-logs"),
        ["unrelated/customer-data"]
    );
}

#[test]
fn concurrent_provision_reservations_cannot_install_two_services() {
    let platform = Arc::new(Platform::new());
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let mut workers = Vec::new();
    for (sid, bucket) in [
        ("fixtureService", "fos-fixture-logs"),
        ("fixtureOther", "fos-other-logs"),
    ] {
        let platform = platform.clone();
        let barrier = barrier.clone();
        workers.push(std::thread::spawn(move|| {
            barrier.wait();
            platform.call("POST","/api/admin/provision/execute",json!({"token":"fixture-token","service_id":sid,"fos_bucket":bucket,"fos_region":"us-east-1"})).status
        }));
    }
    let mut statuses: Vec<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();
    statuses.sort();
    assert_eq!(statuses, [200, 409]);
    assert_eq!(
        platform
            .adapter
            .records()
            .keys()
            .filter(|k| k.starts_with("svc:"))
            .count(),
        1
    );
}

#[test]
fn interrupted_job_creation_keeps_a_recoverable_single_install_reservation() {
    let platform = Platform::new();
    platform.adapter.fail_next("KV_CAS", "job:", false);
    let response=platform.call("POST","/api/admin/provision/execute",json!({"token":"fixture-token","service_id":"fixtureService","fos_bucket":"fos-fixture-logs","fos_region":"us-east-1"}));
    assert_eq!(response.status, 503);
    assert!(platform.adapter.records().contains_key("admin:install"));
    platform.drain(platform.provision()["job"].clone());
    assert_eq!(platform.service()["service"]["status"], "active");
}

#[test]
fn original_v3_execute_shape_is_supported_without_accepting_future_goal_settings() {
    let platform = Platform::new();
    let result=platform.json("POST","/api/provision/execute",json!({
        "token":"fixture-token","service_id":"fixtureService","service_name":"Spoofed name ignored",
        "fos_bucket_name":"fos-fixture-logs","fos_region":"us-east-1","endpoint_name":"Fastly Object Storage Logs",
        "log_period":"1 minute","sample_rate":"100","custom_condition":null,
        "log_fields":"{\"groups\":[\"A\"],\"custom_fields\":[],\"field_overrides\":{},\"field_limits\":{}}"
    }));
    assert_eq!(result["service"]["name"], "Fixture delivery service");
    platform.drain(result["job"].clone());
    assert_eq!(
        platform.adapter.resources("fixtureService", "logging/s3")[0]["name"],
        "Fastly Object Storage Logs"
    );
    assert_eq!(platform.call("POST","/api/provision/execute",json!({"token":"fixture-token","service_id":"fixtureOther","fos_bucket_name":"fos-other-logs","fos_region":"us-east-1","scoring_enabled":true})).status,400);
}

#[test]
fn shell_enforces_roles_front_auth_origin_and_json_for_all_provision_aliases() {
    use fastly_log_analytics::{
        auth::{self, webauthn::Credential, AuthState, Role, User},
        shell::Shell,
        state::{MemoryStore, StateStore},
    };
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;
    for role in [Role::Admin, Role::Analyst, Role::AnalystNoPii] {
        let platform = Platform::new();
        platform.drain(platform.provision()["job"].clone());
        let user = User {
            id: "signed-user".into(),
            display_name: "Test".into(),
            role,
            credentials: vec![Credential {
                id: "credential".into(),
                public_key: vec![1],
                sign_count: 0,
                revoked: false,
            }],
            revoked: false,
            created_by: "test".into(),
            created_at: platform.now(),
        };
        let mut state = AuthState::default();
        state.users.insert(user.id.clone(), user.clone());
        let store = MemoryStore::default();
        assert!(store.compare_and_swap(None, &state).unwrap());
        let claims = auth::encode(
            json!({"sub":user.id,"role":role,"policy_version":0,"exp":platform.now()+3600})
                .to_string()
                .as_bytes(),
        );
        let mut mac =
            Hmac::<Sha256>::new_from_slice(b"fixture-session-signing-material-32-bytes").unwrap();
        mac.update(claims.as_bytes());
        let cookie = format!(
            "flp_session={claims}.{}",
            auth::encode(&mac.finalize().into_bytes())
        );
        for path in ["/api/services", "/api/log-fields/catalog"] {
            let request = Request {
                method: "GET".into(),
                path: path.into(),
                headers: BTreeMap::from([
                    ("x-flp-front-key".into(), "fixture-front-key".into()),
                    ("fastly-client-ip".into(), "192.0.2.1".into()),
                    ("x-flp-front-rl".into(), "1".into()),
                    ("cookie".into(), cookie.clone()),
                ]),
                body: vec![],
            };
            CALLS.with(|calls| calls.set(0));
            SECRETS.with(|v| v.borrow_mut().clear());
            assert_eq!(
                Shell::new(&platform, &store, &[]).handle(&request).status,
                200,
                "{path} {role:?}"
            );
        }
        for path in ["/api/provision/validate", "/api/admin/provision/validate"] {
            let mut request = Request {
                method: "POST".into(),
                path: path.into(),
                headers: BTreeMap::from([
                    ("x-flp-front-key".into(), "fixture-front-key".into()),
                    ("fastly-client-ip".into(), "192.0.2.1".into()),
                    ("x-flp-front-rl".into(), "1".into()),
                    ("origin".into(), "https://logs.example.com".into()),
                    ("content-type".into(), "application/json".into()),
                    ("cookie".into(), cookie.clone()),
                ]),
                body: json!({"token":"fixture-token","service_id":"fixtureService"})
                    .to_string()
                    .into_bytes(),
            };
            CALLS.with(|calls| calls.set(0));
            SECRETS.with(|v| v.borrow_mut().clear());
            assert_eq!(
                Shell::new(&platform, &store, &[]).handle(&request).status,
                if role == Role::Admin { 200 } else { 403 }
            );
            request.headers.remove("origin");
            assert_eq!(
                Shell::new(&platform, &store, &[]).handle(&request).status,
                403
            );
            request.headers.remove("cookie");
            request
                .headers
                .insert("origin".into(), "https://logs.example.com".into());
            assert_eq!(
                Shell::new(&platform, &store, &[]).handle(&request).status,
                401
            );
            request.headers.remove("x-flp-front-key");
            assert_eq!(
                Shell::new(&platform, &store, &[]).handle(&request).status,
                403
            );
        }
        for (method, path) in [
            ("PATCH", "/api/admin/services/fixtureService"),
            ("POST", "/api/admin/services/fixtureService/reconcile"),
            ("POST", "/api/admin/log-fields/preview"),
        ] {
            let current = platform.service();
            let mut request = Request {
                method: method.into(),
                path: path.into(),
                headers: BTreeMap::from([
                    ("x-flp-front-key".into(), "fixture-front-key".into()),
                    ("fastly-client-ip".into(), "192.0.2.1".into()),
                    ("x-flp-front-rl".into(), "1".into()),
                    ("origin".into(), "https://logs.example.com".into()),
                    ("content-type".into(), "application/json".into()),
                    ("cookie".into(), cookie.clone()),
                ]),
                body: json!({"generation":current["generation"],
                    "log_fields":current["service"]["log_fields"],
                    "cmcd":{"enabled":true,"mode":"headers","version":2}
                })
                .to_string()
                .into_bytes(),
            };
            assert_eq!(
                Shell::new(&platform, &store, &[]).handle(&request).status,
                if role == Role::Admin { 200 } else { 403 },
                "{method} {path}"
            );
            request.headers.remove("origin");
            assert_eq!(
                Shell::new(&platform, &store, &[]).handle(&request).status,
                403
            );
        }
    }
}

#[test]
fn service_prefix_is_applied_and_teardown_deletes_only_that_prefix() {
    let platform = Platform::new();
    platform
        .adapter
        .seed_object("fos-fixture-logs", "tenant/own/raw/request/file&one.gz");
    platform
        .adapter
        .seed_object("fos-fixture-logs", "tenant/own/raw/request/file-two.gz");
    platform
        .adapter
        .seed_object("fos-fixture-logs", "tenant/other/keep.gz");
    let result=platform.json("POST","/api/provision/execute",json!({
                "token":"fixture-token","service_id":"fixtureService","fos_bucket_name":"fos-fixture-logs",
                "fos_region":"us-east-1","fos_prefix":"tenant/own/"
            }));
    platform.drain(result["job"].clone());
    assert_eq!(
        platform.adapter.resources("fixtureService", "logging/s3")[0]["path"],
        "tenant/own/raw/request/year=%Y/month=%m/day=%d/hour=%H/minute=%M/"
    );
    let service = platform.service();
    let job=platform.json("POST","/api/provision/teardown",json!({
                "service_id":"fixtureService","confirm_service_id":"fixtureService","token":"fixture-token",
                "generation":service["generation"],"delete_data":true,"delete_fos_bucket":false
            }));
    platform.drain(job["job"].clone());
    assert_eq!(
        platform.adapter.object_keys("fos-fixture-logs"),
        ["tenant/other/keep.gz"]
    );
}

#[test]
fn empty_reconcile_does_not_clone_and_ip_reset_precedes_capture() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    let snippets = platform.adapter.resources("fixtureService", "snippet");
    assert!(snippets
        .iter()
        .any(|v| v["name"] == "Fastly Log Analysis Reset Client IP" && v["priority"] == (-100)));
    let service = platform.service();
    platform.adapter.clear_calls();
    let job = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation":service["generation"]}),
    );
    let result = platform.drain(job["job"].clone());
    assert_eq!(result["changes"], json!([]));
    assert!(!platform
        .adapter
        .calls()
        .iter()
        .any(|(_, p)| p.ends_with("/clone")));
}

#[test]
fn reconcile_forces_blank_message_type_on_the_request_endpoint() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    let mut endpoint = platform.adapter.resources("fixtureService", "logging/s3")[0].clone();
    endpoint["message_type"] = json!("classic");
    platform
        .adapter
        .seed_resource("fixtureService", "logging/s3", endpoint);
    let service = platform.service();
    let job = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation":service["generation"]}),
    );
    let result = platform.drain(job["job"].clone());
    assert!(result["changes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|c| c["resource"] == "logging/s3" && c["action"] == "upsert"));
    let endpoints = platform.adapter.resources("fixtureService", "logging/s3");
    assert_eq!(endpoints[0]["message_type"], "blank");
}

#[test]
fn expired_lease_can_be_reclaimed_but_live_lease_cannot_be_stolen() {
    let platform = Platform::new();
    let job = platform.provision()["job"].clone();
    platform
        .adapter
        .cas(
            "reconcile_lock:fixtureService",
            None,
            &json!({"job_id":"another-job","fence":41,"expires_at":platform.now()+30}),
        )
        .unwrap();
    assert_eq!(
        platform
            .call(
                "POST",
                &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
                json!({})
            )
            .status,
        409
    );
    platform.clock.fetch_add(31, Ordering::SeqCst);
    let result = platform.json(
        "POST",
        &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
        json!({}),
    );
    assert_eq!(result["job"]["status"], "pending");
    assert_eq!(
        platform
            .adapter
            .get("reconcile_lock:fixtureService")
            .unwrap()
            .unwrap()
            .value["fence"],
        42
    );
}

#[test]
fn committed_custom_field_name_and_semantic_type_cannot_change() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    let mut record = platform.adapter.get("svc:fixtureService").unwrap().unwrap();
    record.value["log_fields"]["custom_fields"] = json!([{"name":"locked_value","vcl":"req.http.X-Locked","parquet_type":"Utf8","committed":true,"enabled":true}]);
    platform
        .adapter
        .cas(
            "svc:fixtureService",
            Some(&record.generation),
            &record.value,
        )
        .unwrap();
    let current = platform.service();
    for fields in [
        json!([]),
        json!([{"name":"locked_value","vcl":"req.http.X-Locked","parquet_type":"Int64","committed":true,"enabled":true}]),
        json!([{"name":"renamed_value","vcl":"req.http.X-Locked","parquet_type":"Utf8","committed":true,"enabled":true}]),
    ] {
        let response=platform.call("PATCH","/api/admin/services/fixtureService",json!({"generation":current["generation"],"log_fields":{"groups":["A"],"custom_fields":fields,"field_overrides":{},"field_limits":{}}}));
        assert_eq!(response.status, 409);
    }
    let updated=platform.json("PATCH","/api/admin/services/fixtureService",json!({"generation":current["generation"],"log_fields":{"groups":["A"],"custom_fields":[{"name":"locked_value","vcl":"req.http.X-Locked","parquet_type":"VARCHAR","committed":true,"enabled":false}],"field_overrides":{},"field_limits":{}}}));
    assert_eq!(
        updated["service"]["log_fields"]["custom_fields"][0]["enabled"],
        false
    );
}

#[test]
fn s3_paths_use_sigv4_encoding_not_html_form_encoding() {
    assert_eq!(
        admin::fos_path("fos-test-logs", "raw/~a* b+é.gz"),
        "/fos-test-logs/raw/~a%2A%20b%2B%C3%A9.gz"
    );
    let headers = admin::fos_headers(
        &FosRequest {
            method: "HEAD",
            region: "us-east-1",
            bucket: "fos-test-logs",
            key: "",
            query: "",
            access_key: "synthetic-ak",
            secret_key: "synthetic-sk",
            body: vec![],
            timeout: std::time::Duration::from_secs(60),
            response_limit: admin::transport::FOS_RESPONSE_LIMIT,
            content_md5: None,
        },
        1_577_836_800,
    )
    .unwrap();
    assert!(headers
        .iter()
        .any(|(k, v)| k == "x-amz-date" && v == "20200101T000000Z"));
    assert!(headers.iter().any(|(k, v)| k == "x-amz-content-sha256"
        && v == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"));
    assert!(headers.iter().any(|(k, v)| k == "authorization"
        && v.starts_with(
            "AWS4-HMAC-SHA256 Credential=synthetic-ak/20200101/us-east-1/s3/aws4_request,"
        )));
}

#[test]
fn endpoint_rename_removes_old_owned_name_and_never_adopts_customer_endpoint() {
    let platform = Platform::new();
    let start=platform.json("POST","/api/provision/execute",json!({"token":"fixture-token","service_id":"fixtureService","fos_bucket":"fos-fixture-logs","fos_region":"us-east-1","logging_endpoint_name":"our-old-logs"}));
    platform.drain(start["job"].clone());
    let current = platform.service();
    let saved = platform.json(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation":current["generation"],"logging_endpoint_name":"our-new-logs"}),
    );
    let job = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation":saved["generation"]}),
    );
    platform.drain(job["job"].clone());
    let endpoints = platform.adapter.resources("fixtureService", "logging/s3");
    assert_eq!(endpoints.len(), 1);
    assert_eq!(endpoints[0]["name"], "our-new-logs");
    let customer =
        json!({"name":"customer-audit","path":"customer/private/","bucket_name":"customer-bucket"});
    platform
        .adapter
        .seed_resource("fixtureService", "logging/s3", customer.clone());
    let current = platform.service();
    let saved = platform.json(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation":current["generation"],"logging_endpoint_name":"customer-audit"}),
    );
    let job = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation":saved["generation"]}),
    );
    let failed = platform.json(
        "POST",
        &format!(
            "/api/admin/jobs/{}/step",
            job["job"]["id"].as_str().unwrap()
        ),
        json!({}),
    );
    assert_eq!(failed["job"]["status"], "failed");
    assert!(failed.to_string().contains("resource_name_collision"));
    let current = platform.service();
    let teardown=platform.json("POST","/api/provision/teardown",json!({"service_id":"fixtureService","confirm_service_id":"fixtureService","token":"fixture-token","generation":current["generation"]}));
    platform.drain(teardown["job"].clone());
    assert_eq!(
        platform.adapter.resources("fixtureService", "logging/s3"),
        vec![customer]
    );
}

#[test]
fn delayed_stale_or_failed_secret_reads_never_rotate_a_checkpointed_key() {
    for mode in ["missing", "stale", "error", "metadata_error"] {
        let platform = Platform::new();
        let job = platform.until(platform.provision()["job"].clone(), "key");
        assert_eq!(
            platform
                .fail_checkpoint_before(job.clone(), "cleanup_temp")
                .status,
            503
        );
        let value = match mode {
            "stale" => Ok(Some(
                json!({"access_key":"previous-key","secret_key":"previous-secret"}).to_string(),
            )),
            "error" => Err("synthetic-secret-read-failure".into()),
            _ => Ok(None),
        };
        platform.adapter.override_secret_reads("fos_sk.", value);
        platform
            .adapter
            .override_secret_reads("fos_recovery.", Ok(None));
        if mode == "metadata_error" {
            platform.adapter.fail_next(
                "GET",
                "/resources/stores/secret/fixtureSecrets/secrets/fos_recovery.",
                false,
            );
        }
        let mut failed = platform.retry(&job);
        if matches!(mode, "missing" | "stale") {
            assert_eq!(failed["status"], "pending", "{mode}: {failed}");
            assert_eq!(failed["retry_reason"], "credentials_pending", "{mode}");
            failed = platform.exhaust(failed);
        }
        assert_eq!(failed["status"], "failed", "{mode}: {failed}");
        assert_eq!(platform.adapter.key_count(), 2, "{mode}");
        assert!(
            !platform
                .adapter
                .calls()
                .iter()
                .any(|(m, p)| m == "DELETE"
                    && p.starts_with("/resources/object-storage/access-keys/")),
            "{mode}"
        );
        platform.adapter.clear_secret_reads();
        platform.drain(platform.retry(&failed));
        assert_eq!(platform.adapter.key_count(), 1);
        assert_eq!(
            platform
                .adapter
                .calls()
                .iter()
                .filter(|(m, p)| m == "POST" && p == "/resources/object-storage/access-keys")
                .count(),
            2,
            "{mode}"
        );
    }
}

#[test]
fn lost_token_delete_ack_and_final_checkpoint_resume_without_service_credentials() {
    for checkpoint in [false, true] {
        let platform = Platform::new();
        platform.drain(platform.provision()["job"].clone());
        let service = platform.service();
        let result=platform.json("POST","/api/provision/teardown",json!({"service_id":"fixtureService","confirm_service_id":"fixtureService","token":"fixture-token","generation":service["generation"]}));
        let job = platform.until(result["job"].clone(), "finalize");
        if checkpoint {
            *platform.fail_checkpoint_step.lock().unwrap() = Some("complete");
        } else {
            platform.adapter.fail_next(
                "DELETE",
                "/resources/stores/secret/fixtureSecrets/secrets/fastly_token.fixtureService",
                true,
            );
        }
        let response = platform.call(
            "POST",
            &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
            json!({}),
        );
        assert_eq!(response.status, if checkpoint { 503 } else { 200 });
        assert_eq!(
            platform
                .adapter
                .secret("fastly_token.fixtureService")
                .unwrap(),
            None
        );
        platform.drain(platform.retry(&job));
        assert_eq!(platform.service()["service"]["status"], "torn_down");
    }
}

#[test]
fn shell_setup_grant_remains_usable_for_registration_and_system_checks() {
    use fastly_log_analytics::{auth, shell::Shell, state::MemoryStore};
    let platform = Platform::new();
    platform.adapter.override_secret_reads(
        "admin_setup_hash",
        Ok(Some(
            json!({"sha256":auth::hash("fixture-setup"),"expires_at":platform.now()+900})
                .to_string(),
        )),
    );
    let state = MemoryStore::default();
    let shell = Shell::new(&platform, &state, &[]);
    let mut request = Request {
        method: "POST".into(),
        path: "/api/setup/accept".into(),
        headers: BTreeMap::from([
            ("x-flp-front-key".into(), "fixture-front-key".into()),
            ("fastly-client-ip".into(), "192.0.2.11".into()),
            ("origin".into(), "https://logs.example.com".into()),
            ("content-type".into(), "application/json".into()),
            ("x-flp-front-rl".into(), "1".into()),
        ]),
        body: json!({"code":"fixture-setup"}).to_string().into_bytes(),
    };
    CALLS.with(|v| v.set(0));
    SECRETS.with(|v| v.borrow_mut().clear());
    let accepted = shell.handle(&request);
    assert_eq!(
        accepted.status,
        200,
        "{}",
        String::from_utf8_lossy(&accepted.body)
    );
    let cookie = accepted.headers["set-cookie"]
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    request.headers.insert("cookie".into(), cookie);
    request.path = "/api/auth/register/options".into();
    request.body = json!({"display_name":"Fixture admin"})
        .to_string()
        .into_bytes();
    CALLS.with(|v| v.set(0));
    SECRETS.with(|v| v.borrow_mut().clear());
    let options = shell.handle(&request);
    assert_eq!(
        options.status,
        200,
        "{}",
        String::from_utf8_lossy(&options.body)
    );
    assert!(
        serde_json::from_slice::<Value>(&options.body).unwrap()["public_key"]["challenge"]
            .is_string()
    );
    request.method = "GET".into();
    request.path = "/api/admin/system-checks".into();
    request.body.clear();
    CALLS.with(|v| v.set(0));
    SECRETS.with(|v| v.borrow_mut().clear());
    assert_eq!(shell.handle(&request).status, 200);
}

#[test]
fn shared_write_pacing_covers_claim_checkpoint_release_and_rapid_polls() {
    let adapter = FixtureAdapter::new();
    adapter.enforce_kv_write_rate();
    assert!(adapter.cas("rate-check", None, &json!({})).unwrap());
    let record = adapter.get("rate-check").unwrap().unwrap();
    adapter.advance_kv_clock(100);
    assert_eq!(
        adapter
            .cas("rate-check", Some(&record.generation), &json!({}))
            .unwrap_err(),
        "kv_write_rate_exceeded"
    );
    assert!(adapter
        .paced_cas("rate-check", Some(&record.generation), &json!({}))
        .unwrap());
    assert_eq!(adapter.kv_waits(), 1);
    assert_eq!(admin::kv_write_delay_ms(1100, 1100).unwrap(), 1100);
    assert_eq!(admin::kv_write_delay_ms(1100, 1099).unwrap(), 1100);
    let platform = Platform::new();
    platform.adapter.enforce_kv_write_rate();
    let mut job = platform.provision()["job"].clone();
    for _ in 0..150 {
        if job["status"] == "complete" {
            break;
        }
        platform.adapter.advance_kv_clock(100);
        if let Some(at) = job["retry_at"].as_u64() {
            platform.clock.fetch_max(at, Ordering::SeqCst);
        }
        job = platform.json(
            "POST",
            &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
            json!({}),
        )["job"]
            .clone();
        assert_ne!(job["status"], "failed", "{job}");
    }
    assert_eq!(job["status"], "complete");
    assert!(
        platform.adapter.kv_waits() > 10,
        "claim/checkpoint/release must be paced"
    );
    let service = platform.service();
    let job=platform.json("POST","/api/provision/teardown",json!({"service_id":"fixtureService","confirm_service_id":"fixtureService","token":"fixture-token","generation":service["generation"]}));
    platform.drain(job["job"].clone());
    assert_eq!(platform.service()["service"]["status"], "torn_down");
}

#[test]
fn omitted_provision_fields_match_catalog_all_groups_without_cookie_session() {
    let platform = Platform::new();
    let catalog = platform.json("GET", "/api/log-fields/catalog", json!({}));
    let insights = catalog["insights"]
        .as_array()
        .expect("catalog should include insights array");
    assert_eq!(insights.len(), 51);
    assert!(insights.iter().any(|insight| {
        insight["id"] == "error_spikes"
            && insight["title"] == "Error Spikes"
            && insight["required_groups"] == json!(["A"])
    }));
    assert!(insights.iter().any(|insight| {
        insight["id"] == "botnet_grouping"
            && insight["title"] == "Botnet Grouping"
            && insight["required_groups"] == json!(["H"])
    }));
    let result=platform.json("POST","/api/provision/execute",json!({"token":"fixture-token","service_id":"fixtureService","fos_bucket":"fos-fixture-logs","fos_region":"us-east-1"}));
    assert_eq!(result["service"]["log_fields"], catalog["default_config"]);
    assert_eq!(
        result["service"]["log_fields"]["groups"],
        json!(["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M"])
    );
    platform.drain(result["job"].clone());
    let endpoint = &platform.adapter.resources("fixtureService", "logging/s3")[0];
    assert!(!endpoint["format"]
        .as_str()
        .unwrap()
        .contains("\"cookie_session\""));
}

#[test]
fn teardown_does_not_claim_customer_endpoint_created_after_reservation() {
    let platform = Platform::new();
    platform.provision();
    let customer = json!({"name":"Fastly Object Storage Logs","path":"customer/private/","bucket_name":"customer-bucket"});
    platform
        .adapter
        .seed_resource("fixtureService", "logging/s3", customer.clone());
    let service = platform.service();
    let teardown=platform.json("POST","/api/provision/teardown",json!({"service_id":"fixtureService","confirm_service_id":"fixtureService","token":"fixture-token","generation":service["generation"]}));
    let result = platform.json(
        "POST",
        &format!(
            "/api/admin/jobs/{}/step",
            teardown["job"]["id"].as_str().unwrap()
        ),
        json!({}),
    );
    assert_eq!(result["job"]["status"], "failed");
    assert_eq!(result["job"]["error"], "managed_resources_not_owned");
    assert_eq!(
        platform.adapter.resources("fixtureService", "logging/s3"),
        vec![customer]
    );
}

#[test]
fn superseded_teardown_cannot_delete_data_after_loading_service_before_lease() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    let object = "raw/request/preserve.gz";
    platform.adapter.seed_object("fos-fixture-logs", object);
    let service = platform.service();
    let teardown = platform.json(
        "POST",
        "/api/provision/teardown",
        json!({
            "service_id":"fixtureService","confirm_service_id":"fixtureService",
            "token":"fixture-token","generation":service["generation"],"delete_data":true
        }),
    );
    let deleting = platform.until(teardown["job"].clone(), "delete_page");
    let (loaded, ready) = mpsc::channel();
    let (resume, waiting) = mpsc::channel();
    *platform.before_lease.lock().unwrap() = Some(LeasePause {
        loaded,
        resume: waiting,
    });
    std::thread::scope(|scope| {
        let paused = scope.spawn(|| {
            platform.json(
                "POST",
                &format!("/api/admin/jobs/{}/step", deleting["id"].as_str().unwrap()),
                json!({}),
            )
        });
        ready
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let current = platform.service();
        let preserving = platform.json(
            "POST",
            "/api/provision/teardown",
            json!({
                "service_id":"fixtureService","confirm_service_id":"fixtureService",
                "token":"fixture-token","generation":current["generation"],"delete_data":false
            }),
        );
        resume.send(()).unwrap();
        let rejected = paused.join().unwrap();
        assert_eq!(
            platform.adapter.object_keys("fos-fixture-logs"),
            vec![object]
        );
        assert_eq!(rejected["job"]["status"], "failed");
        assert_eq!(rejected["job"]["error"], "service_changed_rerun");
        platform.drain(preserving["job"].clone());
    });
    assert_eq!(
        platform.adapter.object_keys("fos-fixture-logs"),
        vec![object]
    );
    assert_eq!(platform.service()["service"]["status"], "torn_down");
}

#[test]
fn cron_schedule_projects_due_jobs_without_claiming_or_fabricating_stage_history() {
    let platform = Platform::new();
    let job = platform.provision();
    platform.drain(job["job"].clone());
    let now = platform.now();
    let old = platform.adapter.get("svc:fixtureService").unwrap().unwrap();
    let mut service = old.value;
    service["log_period"] = json!(5);
    service["rum"]["enabled"] = json!(true);
    service["rum"]["log_period"] = json!(300);
    assert!(platform
        .adapter
        .cas("svc:fixtureService", Some(&old.generation), &service)
        .unwrap());
    let period = admin::schedule::ready_rum_period(now, 300);
    let state = json!({
        "jobs": {
            "ingest": {"last_run": now - 5, "more_pending": true, "retry_after": now + 30},
            "raw_delete": {"last_run": now - 10, "more_pending": false, "last_status": "idle",
                "service_id": "fixtureService"},
            "maint": {"last_run": now - 20, "retry_after": now + 200},
            "rum": {"last_run": now - 300, "last_success_period": period,
                "last_success_period_s": 300, "retry_after": now + 10}
        }
    });
    assert!(platform.adapter.cas("cron:state", None, &state).unwrap());
    let before = platform.adapter.get("cron:state").unwrap().unwrap();
    let snapshot = admin::cron_audit_snapshot(&platform);
    let find = |task: &str| {
        snapshot
            .schedules
            .iter()
            .find(|row| row["task"] == task)
            .unwrap()
    };
    let time = |at| {
        time::OffsetDateTime::from_unix_timestamp(at as i64)
            .unwrap()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap()
    };
    assert_eq!(find("request_ingest")["interval_s"], 5);
    assert_eq!(find("request_ingest")["next_run_time"], time(now + 30));
    assert!(find("raw_delete")["next_run_time"].is_null());
    assert_eq!(
        find("raw_delete")["execution"],
        json!({
            "service_id": "fixtureService", "last_run": now - 10, "status": "idle",
            "more_pending": false, "retry_after": 0,
        })
    );
    assert_eq!(find("maintenance")["next_run_time"], time(now + 200));
    assert_eq!(
        find("rum_ingest")["next_run_time"],
        time((period + 1) * 300 + fastly_log_analytics::ingest::settle_s(300))
    );
    assert!(find("rum_ingest")["disabled_reason"].is_null());
    for task in admin::logs::MAINT_STAGES {
        assert!(find(task)["last_run_time"].is_null());
        assert!(find(task)["next_run_time"].is_null());
    }
    assert!(snapshot.runs.is_empty());
    let after = platform.adapter.get("cron:state").unwrap().unwrap();
    assert_eq!(before.generation, after.generation);
    assert_eq!(before.value, after.value);
}

#[test]
fn cron_schedule_raw_delete_execution_rejects_unscoped_state_and_exports_only_known_statuses() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    for (service_id, status, expected_status) in [
        (None, "idle", None),
        (Some("otherService"), "idle", None),
        (
            Some("fixtureService"),
            "unexpected-private-detail",
            Some("unknown"),
        ),
        (Some("fixtureService"), "error", Some("error")),
    ] {
        let prior = platform.adapter.get("cron:state").unwrap();
        let value = json!({"jobs": {"raw_delete": {
            "service_id": service_id, "last_run": platform.now(), "last_status": status,
            "more_pending": true, "retry_after": platform.now() + 30,
        }}});
        assert!(platform
            .adapter
            .cas(
                "cron:state",
                prior.as_ref().map(|r| r.generation.as_str()),
                &value
            )
            .unwrap());
        let snapshot = admin::cron_audit_snapshot(&platform);
        let row = snapshot
            .schedules
            .iter()
            .find(|row| row["task"] == "raw_delete")
            .unwrap();
        if let Some(status) = expected_status {
            assert_eq!(
                row["execution"],
                json!({
                    "service_id": "fixtureService", "last_run": platform.now(), "status": status,
                    "more_pending": true, "retry_after": platform.now() + 30,
                })
            );
        } else {
            assert!(row["execution"].is_null());
        }
        assert!(!serde_json::to_string(&snapshot.schedules)
            .unwrap()
            .contains("unexpected-private-detail"));
    }
}

#[test]
fn cron_schedule_natural_idle_raw_delete_records_the_executing_service_without_inventing_a_run() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    platform.shield.store(true, Ordering::SeqCst);
    CALLS.with(|c| c.set(0));
    SECRETS.with(|s| s.borrow_mut().clear());
    admin::record_tick(&platform, "clock", "clock").unwrap();
    let state = platform.adapter.get("cron:state").unwrap().unwrap().value;
    assert_eq!(state["jobs"]["raw_delete"]["last_status"], "idle");
    assert_eq!(state["jobs"]["raw_delete"]["service_id"], "fixtureService");
    let snapshot = admin::cron_audit_snapshot(&platform);
    let row = snapshot
        .schedules
        .iter()
        .find(|row| row["task"] == "raw_delete")
        .unwrap();
    assert_eq!(row["execution"]["status"], "idle");
    assert_eq!(row["execution"]["last_run"], platform.now());
    assert_eq!(snapshot.runs.len(), 1);
}

#[test]
fn metadata_status_fresh_bypasses_cache_and_reads_one_post_extents_cron_snapshot() {
    use fastly_log_analytics::{
        auth::{self, webauthn::Credential, AuthState, Role, User},
        shell::Shell,
        state::{MemoryStore, StateStore},
    };
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;

    let platform = Platform::new();
    platform.enable_status_cache.store(true, Ordering::SeqCst);
    let job = platform.provision();
    platform.drain(job["job"].clone());
    let store = MemoryStore::default();
    let user = User {
        id: "metadata-admin".into(),
        display_name: "Admin".into(),
        role: Role::Admin,
        credentials: vec![Credential {
            id: "cred".into(),
            public_key: vec![1],
            sign_count: 0,
            revoked: false,
        }],
        revoked: false,
        created_by: "setup".into(),
        created_at: platform.now(),
    };
    let mut state = AuthState::default();
    state.users.insert(user.id.clone(), user.clone());
    assert!(store.compare_and_swap(None, &state).unwrap());
    let claims = auth::encode(
        json!({"sub":user.id,"role":user.role,"policy_version":0,"exp":platform.now()+3600})
            .to_string()
            .as_bytes(),
    );
    let mut mac =
        Hmac::<Sha256>::new_from_slice(b"fixture-session-signing-material-32-bytes").unwrap();
    mac.update(claims.as_bytes());
    let cookie = format!(
        "flp_session={claims}.{}",
        auth::encode(&mac.finalize().into_bytes())
    );
    let fetch = |path: &str, cookie: Option<&str>| {
        CALLS.with(|c| c.set(0));
        SECRETS.with(|s| s.borrow_mut().clear());
        platform.metadata_reads.lock().unwrap().clear();
        let mut headers = BTreeMap::from([
            ("x-flp-front-key".into(), "fixture-front-key".into()),
            ("fastly-client-ip".into(), "192.0.2.1".into()),
            ("x-flp-front-rl".into(), "1".into()),
        ]);
        if let Some(cookie) = cookie {
            headers.insert("cookie".into(), cookie.into());
        }
        Shell::new(&platform, &store, &[]).handle(&Request {
            method: "GET".into(),
            path: path.into(),
            headers,
            body: vec![],
        })
    };
    let before_fos = platform.fos_timeouts.lock().unwrap().len();
    let warm = fetch("/api/sync-status", Some(&cookie));
    let extents_fos = platform.fos_timeouts.lock().unwrap().len() - before_fos;
    assert_eq!(warm.status, 200, "{}", String::from_utf8_lossy(&warm.body));
    let stale: Value = serde_json::from_slice(&warm.body).unwrap();
    let old = platform.adapter.get("svc:fixtureService").unwrap().unwrap();
    let mut service = old.value;
    service["_audit_outbox"] = json!([
        {"at":1800000001u64,"event":"older"},
        {"at":1800000002u64,"event":"newest"}
    ]);
    assert!(platform
        .adapter
        .cas("svc:fixtureService", Some(&old.generation), &service)
        .unwrap());
    assert_eq!(fetch("/api/sync-status", Some(&cookie)).body, warm.body);
    assert!(!platform
        .metadata_reads
        .lock()
        .unwrap()
        .iter()
        .any(|key| key == "cron:state"));
    *platform.cron_during_extents.lock().unwrap() = Some(json!({
        "last_tick":1800000003u64, "last_ingest_at":1800000003u64, "last_ingest_service_id":"fixtureService",
        "runs":[{
            "id":"during-extents", "started_at":"2027-01-15T08:00:03Z", "finished_at":"2027-01-15T08:00:03Z",
            "duration_ms":0,"outcome":"done","trigger":"clock","actor":"clock","bucket":1,
            "service_id":"fixtureService","listed_raw_files":1,"converted_raw_files":1,
            "deleted_raw_files":0,"rows_written":1,"bytes_written":1,"backend_requests_used":1,
            "fos_manifest_key":null,"error":null
        }]
    }));
    let before_fos = platform.fos_timeouts.lock().unwrap().len();
    let fresh = fetch("/api/sync-status?fresh=true", Some(&cookie));
    assert_eq!(
        platform.fos_timeouts.lock().unwrap().len() - before_fos,
        extents_fos
    );
    assert_eq!(fresh.status, 200);
    assert_eq!(fresh.headers["cache-control"], "private, no-store");
    let value: Value = serde_json::from_slice(&fresh.body).unwrap();
    assert_eq!(value["cron_runs"][0]["id"], "during-extents");
    assert_eq!(value["latest_ingested_file_at"], "2027-01-15T08:00:03Z");
    assert_eq!(value["audit_logs"][0]["action"], "newest");
    assert_eq!(
        platform
            .metadata_reads
            .lock()
            .unwrap()
            .iter()
            .filter(|k| k.as_str() == "cron:state")
            .count(),
        1
    );
    assert!(!platform
        .metadata_reads
        .lock()
        .unwrap()
        .iter()
        .any(|k| k == &admin::status_cache_key("sync-status", "fixtureService")));
    assert_ne!(value, stale);
    for path in ["/api/sync-status", "/api/sync-status?fresh=false"] {
        assert_eq!(fetch(path, Some(&cookie)).body, fresh.body);
        assert!(!platform
            .metadata_reads
            .lock()
            .unwrap()
            .iter()
            .any(|k| k == "cron:state"));
    }
    platform.shield.store(true, Ordering::SeqCst);
    let audit = fetch("/api/admin/audit-logs", Some(&cookie));
    let audit: Value = serde_json::from_slice(&audit.body).unwrap();
    assert_eq!(audit["entries"], value["audit_logs"]);
    assert!(!platform
        .metadata_reads
        .lock()
        .unwrap()
        .iter()
        .any(|k| k == "cron:state" || k == admin::schedule::SCHED_KEY));
    platform.shield.store(false, Ordering::SeqCst);
    let cron = fetch("/api/cron-runs", Some(&cookie));
    let cron: Value = serde_json::from_slice(&cron.body).unwrap();
    assert_eq!(cron["runs"], value["cron_runs"]);
    assert_eq!(cron["last_ingest_at"], value["latest_ingested_file_at"]);
    let schedules = cron["schedules"].as_array().expect("native task schedules");
    let tasks: Vec<_> = schedules
        .iter()
        .map(|row| row["task"].as_str().unwrap())
        .collect();
    assert!(tasks.contains(&"request_ingest"));
    assert!(tasks.contains(&"log_flush"));
    assert!(tasks.contains(&"cloud_ingest_clock"));
    for task in admin::logs::MAINT_STAGES {
        assert!(tasks.contains(&task), "missing maintenance task {task}");
    }
    let rum = schedules
        .iter()
        .find(|row| row["task"] == "rum_ingest")
        .unwrap();
    assert_eq!(rum["disabled_reason"], "RUM is disabled");
    assert!(rum["next_run_time"].is_null());
    assert_eq!(
        platform
            .metadata_reads
            .lock()
            .unwrap()
            .iter()
            .filter(|k| k.as_str() == "cron:state")
            .count(),
        1
    );
    assert_eq!(
        fetch("/api/sync-status?fresh=invalid", Some(&cookie)).status,
        422
    );
    assert_eq!(fetch("/api/sync-status?fresh=true", None).status, 401);
    let install = platform.adapter.get("admin:install").unwrap().unwrap();
    let mut uninstalled = install.value;
    uninstalled["service_id"] = Value::Null;
    assert!(platform
        .adapter
        .cas("admin:install", Some(&install.generation), &uninstalled)
        .unwrap());
    let absent = fetch("/api/admin/audit-logs", Some(&cookie));
    assert_eq!(absent.status, 200);
    assert_eq!(
        serde_json::from_slice::<Value>(&absent.body).unwrap(),
        json!({"entries":[]})
    );
    assert!(!platform
        .metadata_reads
        .lock()
        .unwrap()
        .iter()
        .any(|key| key == "cron:state"));
    let mut state = store.load().unwrap();
    state.state.users.get_mut(&user.id).unwrap().role = Role::Analyst;
    assert!(store
        .compare_and_swap(state.generation.as_deref(), &state.state)
        .unwrap());
    assert_eq!(
        fetch("/api/sync-status?fresh=true", Some(&cookie)).status,
        403
    );
}

#[test]
fn shield_ticks_manual_trigger_fos_manifest_and_audit_logs_end_to_end() {
    use fastly_log_analytics::{
        auth::{self, webauthn::Credential, AuditEntry, AuthState, Role, User},
        shell::Shell,
        state::{MemoryStore, StateStore},
    };
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;

    let platform = Platform::new();

    let user = User {
        id: "admin-user".into(),
        display_name: "Admin".into(),
        role: Role::Admin,
        credentials: vec![Credential {
            id: "cred-1".into(),
            public_key: vec![1],
            sign_count: 0,
            revoked: false,
        }],
        revoked: false,
        created_by: "setup".into(),
        created_at: platform.now(),
    };
    let mut auth_state = AuthState::default();
    auth_state.users.insert(user.id.clone(), user.clone());
    auth_state.recent_audit.push(AuditEntry {
        time: platform.now().saturating_sub(5),
        actor: "setup".into(),
        action: "setup.complete".into(),
        target: user.id.clone(),
        ip: "192.0.2.1".into(),
    });
    let store = MemoryStore::default();
    assert!(store.compare_and_swap(None, &auth_state).unwrap());

    let claims = auth::encode(
        json!({"sub":user.id,"role":user.role,"policy_version":0,"exp":platform.now()+3600})
            .to_string()
            .as_bytes(),
    );
    let mut mac =
        Hmac::<Sha256>::new_from_slice(b"fixture-session-signing-material-32-bytes").unwrap();
    mac.update(claims.as_bytes());
    let cookie = format!(
        "flp_session={claims}.{}",
        auth::encode(&mac.finalize().into_bytes())
    );

    let tick_req = Request {
        method: "GET".into(),
        path: "/_tick".into(),
        headers: BTreeMap::from([("x-flp-front-key".into(), "fixture-front-key".into())]),
        body: vec![],
    };

    // 0. An off-shield probe returns 200 without work: no heartbeat, no CronRun row, and a
    // manual trigger before provisioning returns 409.
    CALLS.with(|c| c.set(0));
    SECRETS.with(|s| s.borrow_mut().clear());
    let resp0 = Shell::new(&platform, &store, &[]).handle(&tick_req);
    assert_eq!(resp0.status, 200);
    assert_eq!(
        resp0.headers.get("cache-control").map(String::as_str),
        Some("public, max-age=1")
    );
    let sync_before_req = Request {
        method: "GET".into(),
        path: "/api/sync-status".into(),
        headers: BTreeMap::from([
            ("x-flp-front-key".into(), "fixture-front-key".into()),
            ("fastly-client-ip".into(), "192.0.2.1".into()),
            ("x-flp-front-rl".into(), "1".into()),
            ("cookie".into(), cookie.clone()),
        ]),
        body: vec![],
    };
    CALLS.with(|c| c.set(0));
    SECRETS.with(|s| s.borrow_mut().clear());
    let sync_before_resp = Shell::new(&platform, &store, &[]).handle(&sync_before_req);
    assert_eq!(sync_before_resp.status, 200);
    let sync_before_body: Value = serde_json::from_slice(&sync_before_resp.body).unwrap();
    assert!(sync_before_body["last_tick_at"].is_null());
    assert_eq!(sync_before_body["cron_runs"].as_array().unwrap().len(), 0);

    let unconfigured_trigger_req = Request {
        method: "POST".into(),
        path: "/api/cron-runs/trigger".into(),
        headers: BTreeMap::from([
            ("x-flp-front-key".into(), "fixture-front-key".into()),
            ("fastly-client-ip".into(), "192.0.2.1".into()),
            ("x-flp-front-rl".into(), "1".into()),
            ("origin".into(), "https://logs.example.com".into()),
            ("content-type".into(), "application/json".into()),
            ("cookie".into(), cookie.clone()),
        ]),
        body: b"{}".to_vec(),
    };
    CALLS.with(|c| c.set(0));
    SECRETS.with(|s| s.borrow_mut().clear());
    let unconfigured_trigger_resp =
        Shell::new(&platform, &store, &[]).handle(&unconfigured_trigger_req);
    assert_eq!(unconfigured_trigger_resp.status, 409);

    // Provision a service and move the clock onto the shield.
    platform.drain(platform.provision()["job"].clone());
    platform.clock.fetch_add(120, Ordering::SeqCst);
    platform.shield.store(true, Ordering::SeqCst);

    // 1-3. Shield ticks a second apart, then a period later. With no raw files nothing runs,
    // so nothing is written to FOS or to the run history. When Core Cache is unavailable, the
    // response falls back to a 1s TTL so VCL keeps polling until the schedule record returns.
    for step in [0, 1, 120] {
        platform.clock.fetch_add(step, Ordering::SeqCst);
        CALLS.with(|c| c.set(0));
        SECRETS.with(|s| s.borrow_mut().clear());
        let resp = Shell::new(&platform, &store, &[]).handle(&tick_req);
        assert_eq!(resp.status, 200);
        assert_eq!(
            resp.headers.get("x-flp-tick-ttl").map(String::as_str),
            Some("1")
        );
    }
    assert!(platform
        .adapter
        .object_keys("fos-fixture-logs")
        .iter()
        .all(|k| !k.starts_with("meta/")));

    // 4. Manual trigger via POST /api/cron-runs/trigger -> writes meta/cron-runs/... JSON to FOS.
    let trigger_req = Request {
        method: "POST".into(),
        path: "/api/cron-runs/trigger".into(),
        headers: BTreeMap::from([
            ("x-flp-front-key".into(), "fixture-front-key".into()),
            ("fastly-client-ip".into(), "192.0.2.1".into()),
            ("x-flp-front-rl".into(), "1".into()),
            ("origin".into(), "https://logs.example.com".into()),
            ("content-type".into(), "application/json".into()),
            ("cookie".into(), cookie.clone()),
        ]),
        body: b"{}".to_vec(),
    };
    CALLS.with(|c| c.set(0));
    SECRETS.with(|s| s.borrow_mut().clear());
    let trigger_resp = Shell::new(&platform, &store, &[]).handle(&trigger_req);
    assert_eq!(
        trigger_resp.status,
        200,
        "{}",
        String::from_utf8_lossy(&trigger_resp.body)
    );
    let trigger_body: Value = serde_json::from_slice(&trigger_resp.body).unwrap();
    assert_eq!(trigger_body["run"]["trigger"], "manual");
    assert_eq!(trigger_body["run"]["outcome"], "done");
    assert_eq!(trigger_body["run"]["actor"], "admin-user");
    let manifest_key = trigger_body["run"]["fos_manifest_key"]
        .as_str()
        .expect("manual trigger should write FOS manifest")
        .to_owned();
    assert!(manifest_key.starts_with("meta/cron-runs/dt="));
    assert!(manifest_key.ends_with(".json"));

    let bucket_objects = platform.adapter.bucket_objects("fos-fixture-logs");
    let manifest_bytes = bucket_objects
        .get(&manifest_key)
        .expect("manifest object must exist in FOS bucket");
    let manifest_json: Value = serde_json::from_slice(manifest_bytes).unwrap();
    assert_eq!(manifest_json["id"], trigger_body["run"]["id"]);
    assert_eq!(manifest_json["service_id"], "fixtureService");

    // Newest first: the manual run, then the two due ingests; the tick between wrote nothing.
    let runs = trigger_body["runs"].as_array().unwrap();
    let outcomes: Vec<_> = runs.iter().map(|r| r["outcome"].clone()).collect();
    assert_eq!(outcomes, ["done", "idle", "idle"]);
    assert_eq!(runs[0]["trigger"], "manual");

    // 5. Verify GET /api/sync-status and GET /api/admin/audit-logs include both cron runs and merged audit logs.
    for path in [
        "/api/sync-status",
        "/api/admin/audit-logs",
        "/api/cron-runs",
    ] {
        let get_req = Request {
            method: "GET".into(),
            path: path.into(),
            headers: BTreeMap::from([
                ("x-flp-front-key".into(), "fixture-front-key".into()),
                ("fastly-client-ip".into(), "192.0.2.1".into()),
                ("x-flp-front-rl".into(), "1".into()),
                ("cookie".into(), cookie.clone()),
            ]),
            body: vec![],
        };
        CALLS.with(|c| c.set(0));
        SECRETS.with(|s| s.borrow_mut().clear());
        let get_resp = Shell::new(&platform, &store, &[]).handle(&get_req);
        assert_eq!(get_resp.status, 200, "{path}");
        let body: Value = serde_json::from_slice(&get_resp.body).unwrap();
        if path == "/api/sync-status" {
            assert_eq!(body["configured"], true);
            assert_eq!(body["clock_interval_s"], 1);
            assert!(body["last_tick_at"].is_string());
            assert_eq!(body["request"]["last_sync_at"], body["last_tick_at"]);
            assert_eq!(body["cron_runs"].as_array().unwrap().len(), 3);
            let audit = body["audit_logs"].as_array().unwrap();
            assert!(audit.iter().any(|e| e["action"] == "job.completed"));
            assert!(audit.iter().all(|e| e["category"] == "control_plane"));
            assert!(!audit.iter().any(|e| e["action"] == "setup.complete"));
        } else if path == "/api/admin/audit-logs" {
            let entries = body["entries"].as_array().unwrap();
            assert!(entries.iter().any(|e| e["action"] == "job.completed"));
            assert!(entries.iter().all(|e| e["category"] == "control_plane"));
            assert!(!entries.iter().any(|e| e["action"] == "setup.complete"));
        } else {
            assert_eq!(body["configured"], true);
            assert_eq!(body["service_id"], "fixtureService");
            assert_eq!(body["runs"].as_array().unwrap().len(), 3);
        }
    }
}

#[test]
fn cmcd_provision_reconcile_disable_and_teardown_preserve_owned_typed_fields() {
    let platform = Platform::new();
    let result = platform.json(
        "POST",
        "/api/admin/provision/execute",
        json!({
            "token":"fixture-token","service_id":"fixtureService","fos_bucket":"fos-fixture-logs",
            "fos_region":"us-east-1","cmcd":{"enabled":true,"mode":"headers","version":2}
        }),
    );
    platform.drain(result["job"].clone());
    let current = platform.service();
    assert_eq!(
        current["service"]["cmcd"],
        json!({"enabled":true,"mode":"headers","version":2})
    );
    let custom = current["service"]["log_fields"]["custom_fields"]
        .as_array()
        .unwrap();
    assert_eq!(custom.len(), 14);
    for field in custom {
        assert_eq!(field["system_managed"], true);
        assert_eq!(field["committed"], true);
        assert_eq!(field["enabled"], true);
    }
    let bs = custom.iter().find(|f| f["name"] == "cmcd_bs").unwrap();
    assert_eq!(bs["parquet_type"], "Int64");
    let sid = custom.iter().find(|f| f["name"] == "cmcd_sid").unwrap();
    assert_eq!(sid["byte_limit"], 288);
    let snippets = platform.adapter.resources("fixtureService", "snippet");
    let recv = snippets
        .iter()
        .find(|s| s["name"] == "Fastly Log Analytics - vcl_recv")
        .unwrap()["content"]
        .as_str()
        .unwrap();
    assert!(
        recv.find("unset req.http.x-cmcd;").unwrap() < recv.find("CMCD v2 header-mode").unwrap()
    );
    assert!(
        recv.find("CMCD v2 header-mode").unwrap()
            < recv
                .find("\n  set req.http.x-fos-edge-data:cmcd_sid")
                .unwrap()
    );
    assert_eq!(
        platform
            .adapter
            .resources("fixtureService", "logging/s3")
            .len(),
        1
    );
    let job = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({
            "generation":current["generation"],"cmcd":{"enabled":false,"mode":"headers","version":2}
        }),
    )["job"]
        .clone();
    let pending = platform.service();
    for (method, path) in [
        ("PATCH", "/api/admin/services/fixtureService"),
        ("POST", "/api/admin/services/fixtureService/reconcile"),
    ] {
        assert_eq!(platform.call(method,path,json!({
            "generation":pending["generation"],"cmcd":{"enabled":true,"mode":"query_string","version":1}
        })).status,409);
    }
    assert_eq!(
        platform.service()["service"]["cmcd"],
        pending["service"]["cmcd"]
    );
    platform.drain(job);
    let disabled = platform.service();
    assert_eq!(
        disabled["service"]["log_fields"]["custom_fields"]
            .as_array()
            .unwrap()
            .len(),
        14
    );
    assert!(disabled["service"]["log_fields"]["custom_fields"]
        .as_array()
        .unwrap()
        .iter()
        .all(|f| f["enabled"] == false && f["committed"] == true));
    let endpoints = platform.adapter.resources("fixtureService", "logging/s3");
    assert!(!endpoints[0]["format"]
        .as_str()
        .unwrap()
        .contains("\"cmcd_sid\""));
    let snippets = platform.adapter.resources("fixtureService", "snippet");
    assert!(snippets.iter().all(|s| !s["content"]
        .as_str()
        .unwrap_or("")
        .contains("CMCD v2 header-mode")));
    let job=platform.json("POST","/api/admin/services/fixtureService/reconcile",json!({
        "generation":disabled["generation"],"cmcd":{"enabled":true,"mode":"query_string","version":1}
    }))["job"].clone();
    let job = platform.until(job, "activate");
    let path = format!(
        "/service/fixtureService/version/{}/activate",
        job["draft_version"].as_u64().unwrap()
    );
    platform.adapter.fail_next("PUT", &path, true);
    let failed = platform.json(
        "POST",
        &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
        json!({}),
    );
    assert_eq!(failed["job"]["status"], "failed");
    platform.drain(platform.retry(&job));
    assert_eq!(
        platform
            .adapter
            .calls()
            .iter()
            .filter(|(m, p)| m == "PUT" && p == &path)
            .count(),
        1
    );
    let current = platform.service();
    let job=platform.json("POST","/api/admin/provision/teardown",json!({
        "token":"fixture-token","service_id":"fixtureService","confirm_service_id":"fixtureService",
        "generation":current["generation"],"remove_logging":true,"remove_snippets":true,
        "delete_fos_keys":true,"delete_data":false,"delete_fos_bucket":false
    }))["job"].clone();
    platform.drain(job);
    assert!(platform
        .adapter
        .resources("fixtureService", "snippet")
        .iter()
        .all(|s| !s["content"].as_str().unwrap_or("").contains("CMCD")));
    assert_eq!(
        platform.service()["service"]["log_fields"]["custom_fields"]
            .as_array()
            .unwrap()
            .len(),
        14
    );
    platform.drain(platform.provision()["job"].clone());
    let reinstalled = platform.service();
    assert_eq!(reinstalled["service"]["cmcd"]["enabled"], false);
    assert_eq!(
        reinstalled["service"]["log_fields"]["custom_fields"]
            .as_array()
            .unwrap()
            .len(),
        14
    );
    assert!(reinstalled["service"]["log_fields"]["custom_fields"]
        .as_array()
        .unwrap()
        .iter()
        .all(|field| field["committed"] == true && field["enabled"] == false));
}

#[test]
fn cmcd_settings_and_managed_fields_cannot_be_forged_or_mutated() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    let current = platform.service();
    for cmcd in [
        json!({"enabled":true,"mode":"cookie","version":1}),
        json!({"enabled":true,"mode":"headers","version":3}),
        json!({"enabled":"yes","mode":"headers","version":1}),
    ] {
        assert_eq!(
            platform
                .call(
                    "PATCH",
                    "/api/admin/services/fixtureService",
                    json!({
                        "generation":current["generation"],"cmcd":cmcd
                    })
                )
                .status,
            400
        );
    }
    let collision =
        json!({"name":"cmcd_sid","vcl":"req.http.Evil","parquet_type":"Utf8","enabled":true});
    let mut fields = current["service"]["log_fields"].clone();
    fields["custom_fields"] = json!([collision.clone()]);
    assert_eq!(platform.call("PATCH","/api/admin/services/fixtureService",json!({
        "generation":current["generation"],"cmcd":{"enabled":true,"mode":"headers","version":1},
        "log_fields":fields
    })).status,400);
    assert_eq!(
        platform.json(
            "POST",
            "/api/admin/log-fields/custom/validate",
            json!({"field":collision})
        )["valid"],
        false
    );
    let enabled=platform.json("PATCH","/api/admin/services/fixtureService",json!({
        "generation":current["generation"],"cmcd":{"enabled":true,"mode":"query_string","version":1}
    }));
    for (key, value) in [
        ("vcl", json!("req.http.Evil")),
        ("parquet_type", json!("Boolean")),
        ("byte_limit", json!(16384)),
        ("enabled", json!(false)),
        ("system_managed", json!(false)),
    ] {
        let mut fields = enabled["service"]["log_fields"].clone();
        fields["custom_fields"][0][key] = value;
        assert_eq!(
            platform
                .call(
                    "PATCH",
                    "/api/admin/services/fixtureService",
                    json!({
                        "generation":enabled["generation"],"log_fields":fields
                    })
                )
                .status,
            400,
            "{key}"
        );
    }
    let mut fields = enabled["service"]["log_fields"].clone();
    fields["custom_fields"] = json!([]);
    let restored = platform.json(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({
            "generation":enabled["generation"],"log_fields":fields
        }),
    );
    assert_eq!(
        restored["service"]["log_fields"]["custom_fields"]
            .as_array()
            .unwrap()
            .len(),
        14
    );
    let mut persisted = restored["service"]["log_fields"].clone();
    for field in persisted["custom_fields"].as_array_mut().unwrap() {
        field.as_object_mut().unwrap().remove("committed");
    }
    let preview = platform.json(
        "POST",
        "/api/admin/log-fields/preview",
        json!({
            "log_fields":persisted,"cmcd":restored["service"]["cmcd"]
        }),
    );
    assert!(preview["format"].as_str().unwrap().contains("\"cmcd_sid\""));
    let limits=platform.json("PATCH","/api/admin/log-fields/limits",json!({
        "service_id":"fixtureService","generation":restored["generation"],"field_limits":{"url":1000}
    }));
    assert_eq!(
        limits["log_fields"]["custom_fields"],
        restored["service"]["log_fields"]["custom_fields"]
    );
    let preview = platform.json(
        "POST",
        "/api/admin/log-fields/preview",
        json!({
            "log_fields":{"groups":["A"],"custom_fields":[],"field_overrides":{},"field_limits":{}},
            "cmcd":{"enabled":true,"mode":"headers","version":2}
        }),
    );
    assert!(preview["format"].as_str().unwrap().contains("\"cmcd_sid\""));
}

#[test]
fn cmcd_snapshot_recovery_does_not_replace_committed_capture_configuration() {
    let platform = Platform::new();
    platform
        .adapter
        .fail_next("KV_CAS", "svc:fixtureService", false);
    let input = json!({"token":"fixture-token","service_id":"fixtureService","fos_bucket":"fos-fixture-logs",
        "fos_region":"us-east-1","cmcd":{"enabled":true,"mode":"headers","version":2}});
    assert_eq!(
        platform
            .call("POST", "/api/admin/provision/execute", input)
            .status,
        503
    );
    platform.drain(platform.provision()["job"].clone());
    let current = platform.service();
    assert_eq!(
        current["service"]["cmcd"],
        json!({"enabled":true,"mode":"headers","version":2})
    );
    assert_eq!(
        current["service"]["log_fields"]["custom_fields"]
            .as_array()
            .unwrap()
            .len(),
        14
    );
}

#[test]
fn cmcd_draft_validation_failure_stays_inactive_and_retries_the_same_fields() {
    let platform = Platform::new();
    let response=platform.json("POST","/api/admin/provision/execute",json!({
        "token":"fixture-token","service_id":"fixtureService","fos_bucket":"fos-fixture-logs","fos_region":"us-east-1",
        "cmcd":{"enabled":true,"mode":"headers","version":2}
    }));
    let job = platform.until(response["job"].clone(), "validate");
    platform.adapter.invalid_draft(true);
    let failed = platform.json(
        "POST",
        &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
        json!({}),
    );
    assert_eq!(failed["job"]["error"], "draft_validation_failed");
    assert!(platform
        .adapter
        .calls()
        .iter()
        .all(|(_, path)| !path.starts_with("/service/fixtureService/")
            || !path.ends_with("/activate")));
    platform.adapter.invalid_draft(false);
    platform.drain(platform.retry(&job));
    assert_eq!(
        platform.service()["service"]["cmcd"],
        json!({"enabled":true,"mode":"headers","version":2})
    );
    assert_eq!(
        platform.service()["service"]["log_fields"]["custom_fields"]
            .as_array()
            .unwrap()
            .len(),
        14
    );
}

#[test]
fn cmcd_capture_never_adopts_an_existing_extraction_snippet_or_local_variable() {
    let platform = Platform::new();
    platform.adapter.seed_resource("fixtureService","snippet",json!({
        "name":"CMCD Extraction","type":"recv","priority":1,"content":"set req.http.Customer-CMCD = \"owned\";"
    }));
    let response=platform.call("POST","/api/admin/provision/execute",json!({
        "token":"fixture-token","service_id":"fixtureService","fos_bucket":"fos-fixture-logs","fos_region":"us-east-1",
        "cmcd":{"enabled":true,"mode":"headers","version":1}
    }));
    assert_eq!(response.status, 409);
    assert!(platform
        .adapter
        .records()
        .keys()
        .all(|key| !key.starts_with("job:")));
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    platform.adapter.seed_resource("fixtureService","snippet",json!({
        "name":"Customer recv","type":"recv","priority":1,"content":"declare local var.cmcd STRING;"
    }));
    let current = platform.service();
    let job=platform.json("POST","/api/admin/services/fixtureService/reconcile",json!({
        "generation":current["generation"],"cmcd":{"enabled":true,"mode":"query_string","version":1}
    }))["job"].clone();
    let failed = platform.json(
        "POST",
        &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
        json!({}),
    );
    assert_eq!(failed["job"]["status"], "failed");
    assert_eq!(failed["job"]["error"], "cmcd_vcl_variable_collision");
    assert!(failed["job"]["draft_version"].is_null());
}

#[test]
fn teardown_preserves_customer_cmcd_snippet_added_after_installation() {
    for enabled in [false, true] {
        let platform = Platform::new();
        let installed = platform.json("POST", "/api/admin/provision/execute", json!({
            "token":"fixture-token","service_id":"fixtureService","fos_bucket":"fos-fixture-logs",
            "fos_region":"us-east-1","cmcd":{"enabled":enabled,"mode":"headers","version":2}
        }));
        platform.drain(installed["job"].clone());
        let customer = json!({
            "name":"CMCD Extraction","type":"recv","priority":1,
            "content":"declare local var.cmcd STRING;\nset req.http.Customer-CMCD = \"customer-owned\";"
        });
        platform
            .adapter
            .seed_resource("fixtureService", "snippet", customer.clone());
        let current = platform.service();
        let teardown = platform.json("POST", "/api/admin/provision/teardown", json!({
            "token":"fixture-token","service_id":"fixtureService","confirm_service_id":"fixtureService",
            "generation":current["generation"],"remove_logging":true,"remove_snippets":true,
            "delete_fos_keys":true,"delete_data":false,"delete_fos_bucket":false
        }));
        platform.drain(teardown["job"].clone());
        assert_eq!(platform.service()["service"]["status"], "torn_down");
        assert_eq!(
            platform.adapter.resources("fixtureService", "snippet"),
            vec![customer]
        );
        assert!(platform
            .adapter
            .resources("fixtureService", "logging/s3")
            .is_empty());
    }
}

#[test]
fn slow_control_plane_inventory_completes_inside_the_step_wall_budget() {
    for millis in [7500, 8500] {
        let platform = Platform::new();
        let job = platform.provision()["job"].clone();
        platform.api_delays.lock().unwrap().push(ApiDelay {
            method: "GET",
            path: "/resources/object-storage/access-keys",
            millis,
            after_effect: false,
        });
        let result = platform.json(
            "POST",
            &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
            json!({}),
        );
        assert_eq!(result["job"]["status"], "pending", "{result}");
        assert_eq!(platform.adapter.key_count(), 1, "{millis}ms: {result}");
        let granted = platform.api_timeouts.lock().unwrap()[0];
        assert!(granted > std::time::Duration::from_millis(millis));
        assert!(granted <= std::time::Duration::from_secs(30));
        assert_eq!(ELAPSED_MS.with(Cell::get), millis);
        platform.drain(result["job"].clone());
        assert_eq!(platform.service()["service"]["status"], "active");
    }
}

#[test]
fn slow_key_creation_keeps_checkpoint_reserve_and_recovers_unknown_side_effects() {
    for (millis, expected) in [
        (35000, "fastly_backend_deadline"),
        (28000, "wall_time_budget"),
    ] {
        let platform = Platform::new();
        let job = platform.provision()["job"].clone();
        platform.api_delays.lock().unwrap().extend([
            ApiDelay {
                method: "GET",
                path: "/resources/object-storage/access-keys",
                millis: 15000,
                after_effect: false,
            },
            ApiDelay {
                method: "POST",
                path: "/resources/object-storage/access-keys",
                millis,
                after_effect: true,
            },
        ]);
        let failed = platform.json(
            "POST",
            &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
            json!({}),
        )["job"]
            .clone();
        assert_eq!(failed["status"], "pending", "{failed}");
        assert_eq!(failed["retry_reason"], expected);
        assert_eq!(
            platform.adapter.key_count(),
            1,
            "a timeout cannot disprove key creation"
        );
        let persisted = platform.adapter.records()[&format!("job:{}", job["id"].as_str().unwrap())]
            .value
            .clone();
        assert_eq!(persisted["status"], "pending");
        assert_eq!(persisted["lease_until"], 0);
        assert_eq!(persisted["step"], "bucket");
        assert_eq!(ELAPSED_MS.with(Cell::get), 43000);
        assert_eq!(
            *platform.api_timeouts.lock().unwrap(),
            vec![
                std::time::Duration::from_millis(30000),
                std::time::Duration::from_millis(28000)
            ]
        );
        platform.drain(failed);
        assert_eq!(platform.service()["service"]["status"], "active");
        assert_eq!(platform.adapter.key_count(), 1);
        assert_eq!(
            platform
                .adapter
                .calls()
                .iter()
                .filter(|(method, path)| method == "POST"
                    && path == "/resources/object-storage/access-keys")
                .count(),
            3,
            "orphan temporary key is replaced before permanent scoped key creation"
        );
    }
}

#[test]
fn transient_inventory_http_error_retries_without_manual_resume() {
    let platform = Platform::new();
    let job = platform.provision()["job"].clone();
    platform.api_faults.lock().unwrap().push(ApiFault {
        method: "GET",
        path: "/resources/object-storage/access-keys",
        name: None,
        status: 503,
        retry_after: None,
    });
    let result = platform.json(
        "POST",
        &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
        json!({}),
    );
    assert_eq!(result["job"]["status"], "pending", "{result}");
    assert_eq!(result["job"]["step"], "bucket");
    assert_eq!(platform.adapter.key_count(), 1);
    platform.drain(result["job"].clone());
    assert_eq!(platform.service()["service"]["status"], "active");
}

#[test]
fn permanent_api_rejection_does_not_retry() {
    for status in [400, 401, 403, 422] {
        let platform = Platform::new();
        let job = platform.provision()["job"].clone();
        platform.api_faults.lock().unwrap().push(ApiFault {
            method: "GET",
            path: "/resources/object-storage/access-keys",
            name: None,
            status,
            retry_after: None,
        });
        let before = platform.api_attempts.load(Ordering::SeqCst);
        let result = platform.json(
            "POST",
            &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
            json!({}),
        );
        assert_eq!(result["job"]["status"], "failed");
        assert_eq!(result["job"]["error"], "fastly_api_rejected");
        assert_eq!(platform.api_attempts.load(Ordering::SeqCst) - before, 1);
        assert_eq!(platform.adapter.key_count(), 0);
    }
}

#[test]
fn retry_after_is_durable_and_early_poll_does_not_repeat_the_request() {
    let platform = Platform::new();
    let job = platform.provision()["job"].clone();
    platform.api_faults.lock().unwrap().push(ApiFault {
        method: "GET",
        path: "/resources/object-storage/access-keys",
        name: None,
        status: 429,
        retry_after: Some(std::time::Duration::from_secs(30)),
    });
    let path = format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap());
    let pending = platform.json("POST", &path, json!({}))["job"].clone();
    assert_eq!(pending["status"], "pending");
    assert_eq!(pending["retry_at"], platform.now() + 30);
    assert_eq!(pending["retry_attempt"], 1);
    assert_eq!(pending["retry_reason"], "fastly_api_rate_limited");
    let before = platform.api_attempts.load(Ordering::SeqCst);
    assert_eq!(platform.json("POST", &path, json!({}))["job"], pending);
    assert_eq!(platform.api_attempts.load(Ordering::SeqCst), before);
    platform.drain(pending);
    assert_eq!(platform.service()["service"]["status"], "active");
}

#[test]
fn automatic_retries_stop_after_four_attempts_across_step_requests() {
    let platform = Platform::new();
    let mut job = platform.provision()["job"].clone();
    platform
        .api_faults
        .lock()
        .unwrap()
        .extend((0..4).map(|_| ApiFault {
            method: "GET",
            path: "/resources/object-storage/access-keys",
            name: None,
            status: 503,
            retry_after: None,
        }));
    let before = platform.api_attempts.load(Ordering::SeqCst);
    let path = format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap());
    for _ in 0..5 {
        if job["status"] == "failed" {
            break;
        }
        if let Some(at) = job["retry_at"].as_u64() {
            platform.clock.fetch_max(at, Ordering::SeqCst);
        }
        job = platform.json("POST", &path, json!({}))["job"].clone();
    }
    assert_eq!(job["status"], "failed");
    assert_eq!(job["retry_attempt"], 4);
    assert_eq!(job["error"], "fastly_api_unavailable");
    assert_eq!(platform.api_attempts.load(Ordering::SeqCst) - before, 4);
    assert_eq!(platform.json("POST", &path, json!({}))["job"], job);
    assert_eq!(platform.adapter.key_count(), 0);
}

#[test]
fn ambiguous_secret_write_recovers_automatically_without_duplicate_keys() {
    let platform = Platform::new();
    let job = platform.provision()["job"].clone();
    platform.api_delays.lock().unwrap().push(ApiDelay {
        method: "PUT",
        path: "/resources/stores/secret/fixtureSecrets/secrets",
        millis: 31000,
        after_effect: true,
    });
    let result = platform.json(
        "POST",
        &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
        json!({}),
    );
    assert_eq!(result["job"]["status"], "pending");
    assert_eq!(result["job"]["retry_reason"], "fastly_backend_deadline");
    assert_eq!(platform.adapter.key_count(), 1);
    platform.drain(result["job"].clone());
    assert_eq!(platform.adapter.key_count(), 1);
    assert_eq!(
        platform
            .adapter
            .calls()
            .iter()
            .filter(|(method, path)| method == "POST"
                && path == "/resources/object-storage/access-keys")
            .count(),
        2
    );
    assert_eq!(platform.service()["service"]["status"], "active");
}

#[test]
fn ambiguous_activation_readback_finishes_without_reactivating() {
    let platform = Platform::new();
    let job = platform.until(platform.provision()["job"].clone(), "activate");
    let path = "/service/fixtureService/version/2/activate";
    platform.api_delays.lock().unwrap().push(ApiDelay {
        method: "PUT",
        path,
        millis: 31000,
        after_effect: true,
    });
    let result = platform.json(
        "POST",
        &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
        json!({}),
    );
    assert_eq!(result["job"]["status"], "pending");
    platform.drain(result["job"].clone());
    assert_eq!(
        platform
            .adapter
            .calls()
            .iter()
            .filter(|(method, value)| method == "PUT" && value == path)
            .count(),
        1
    );
    assert_eq!(platform.service()["service"]["status"], "active");
}

#[test]
fn inventory_network_timeout_resumes_automatically_in_the_next_request() {
    let platform = Platform::new();
    let job = platform.provision()["job"].clone();
    platform
        .api_delays
        .lock()
        .unwrap()
        .extend((0..2).map(|_| ApiDelay {
            method: "GET",
            path: "/resources/object-storage/access-keys",
            millis: 31000,
            after_effect: false,
        }));
    let result = platform.json(
        "POST",
        &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
        json!({}),
    );
    assert_eq!(result["job"]["status"], "pending");
    assert_eq!(result["job"]["retry_reason"], "fastly_backend_deadline");
    assert_eq!(result["job"]["retry_attempt"], 2);
    assert_eq!(ELAPSED_MS.with(Cell::get), 43000);
    assert_eq!(platform.adapter.key_count(), 0);
    platform.drain(result["job"].clone());
    assert_eq!(platform.service()["service"]["status"], "active");
}

#[test]
fn ambiguous_unmarked_clone_fails_closed_instead_of_cloning_again() {
    let platform = Platform::new();
    let job = platform.until(platform.provision()["job"].clone(), "clone");
    let step = format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap());
    platform.json("POST", &step, json!({}));
    let path = "/service/fixtureService/version/1/clone";
    platform.api_delays.lock().unwrap().push(ApiDelay {
        method: "PUT",
        path,
        millis: 31000,
        after_effect: true,
    });
    let pending = platform.json("POST", &step, json!({}))["job"].clone();
    assert_eq!(pending["status"], "pending");
    platform
        .clock
        .fetch_max(pending["retry_at"].as_u64().unwrap(), Ordering::SeqCst);
    let rejected = platform.json("POST", &step, json!({}))["job"].clone();
    assert_eq!(rejected["status"], "failed");
    assert_eq!(rejected["error"], "uncheckpointed_clone_requires_review");
    assert_eq!(
        platform
            .adapter
            .calls()
            .iter()
            .filter(|(method, value)| method == "PUT" && value == path)
            .count(),
        1
    );
}

#[test]
fn delayed_mutation_completion_cannot_be_replayed_even_after_manual_retry() {
    for clone in [false, true] {
        let platform = Platform::new();
        let job = platform.provision()["job"].clone();
        let job = if clone {
            platform.until(job, "clone")
        } else {
            job
        };
        let step = format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap());
        if clone {
            platform.json("POST", &step, json!({}));
        }
        let (method, path) = if clone {
            ("PUT", "/service/fixtureService/version/1/clone")
        } else {
            ("POST", "/resources/object-storage/access-keys")
        };
        platform.api_delays.lock().unwrap().push(ApiDelay {
            method,
            path,
            millis: 31000,
            after_effect: false,
        });
        let pending = platform.json("POST", &step, json!({}))["job"].clone();
        assert_eq!(pending["status"], "pending");
        platform
            .clock
            .fetch_max(pending["retry_at"].as_u64().unwrap(), Ordering::SeqCst);
        let rejected = platform.json("POST", &step, json!({}))["job"].clone();
        assert_eq!(rejected["status"], "failed", "{rejected}");
        assert_eq!(rejected["error"], "mutation_outcome_unknown");
        let rejected = platform.retry(&rejected);
        assert_eq!(rejected["error"], "mutation_outcome_unknown");
        assert_eq!(
            platform
                .adapter
                .calls()
                .iter()
                .filter(|(verb, value)| verb == method && value == path)
                .count(),
            0,
            "the original request has not completed yet"
        );
        let late = platform.late_requests.lock().unwrap().pop().unwrap();
        platform
            .adapter
            .api(ApiRequest {
                method: &late.method,
                path: &late.path,
                query: "",
                token: "fixture-token",
                body: late.body,
                timeout: std::time::Duration::from_secs(6),
            })
            .unwrap();
        let resumed = platform.retry(&rejected);
        if clone {
            assert_eq!(resumed["error"], "uncheckpointed_clone_requires_review");
            assert_eq!(
                platform
                    .adapter
                    .calls()
                    .iter()
                    .filter(|(verb, value)| verb == method && value == path)
                    .count(),
                1
            );
        } else {
            platform.drain(resumed);
            assert_eq!(platform.adapter.key_count(), 1);
            assert_eq!(platform.service()["service"]["status"], "active");
        }
    }
}

#[test]
fn response_condition_is_created_before_the_endpoint_that_references_it() {
    let platform = Platform::new();
    let job = platform.until(platform.provision()["job"].clone(), "apply");
    let position = |resource: &str| {
        job["changes"]
            .as_array()
            .unwrap()
            .iter()
            .position(|change| change["resource"] == resource)
            .unwrap()
    };
    assert!(position("condition") < position("logging/s3"));
}

#[test]
fn draft_upsert_readback_replaces_unrecorded_mutation_uncertainty() {
    let platform = Platform::new();
    let job = platform.until(platform.provision()["job"].clone(), "apply");
    let step = format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap());
    platform.api_delays.lock().unwrap().push(ApiDelay {
        method: "POST",
        path: "/service/fixtureService/version/2/snippet",
        millis: 31000,
        after_effect: false,
    });
    platform
        .fail_timeout_checkpoint
        .store(true, Ordering::SeqCst);
    assert_eq!(platform.call("POST", &step, json!({})).status, 503);
    platform.clock.fetch_add(61, Ordering::SeqCst);
    let resumed = platform.json("POST", &step, json!({}))["job"].clone();
    assert_ne!(resumed["error"], "mutation_outcome_unknown", "{resumed}");
    assert_eq!(
        platform
            .adapter
            .calls()
            .iter()
            .filter(|(verb, path)| verb == "POST"
                && path == "/service/fixtureService/version/2/snippet")
            .count(),
        1
    );
}

#[test]
fn unrecorded_delete_outcome_is_replayed_because_deletes_are_idempotent() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    let service = platform.service();
    let teardown = platform.json(
        "POST",
        "/api/provision/teardown",
        json!({
            "service_id":"fixtureService","confirm_service_id":"fixtureService",
            "token":"fixture-token","generation":service["generation"]
        }),
    );
    let job = platform.until(teardown["job"].clone(), "revoke_secret_ak");
    let step = format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap());
    let path = "/resources/stores/secret/fixtureSecrets/secrets/fos_ak.fixtureService";
    platform.api_delays.lock().unwrap().push(ApiDelay {
        method: "DELETE",
        path,
        millis: 31000,
        after_effect: false,
    });
    platform
        .fail_timeout_checkpoint
        .store(true, Ordering::SeqCst);
    assert_eq!(platform.call("POST", &step, json!({})).status, 503);
    platform.clock.fetch_add(61, Ordering::SeqCst);
    let resumed = platform.json("POST", &step, json!({}))["job"].clone();
    assert_ne!(resumed["error"], "mutation_outcome_unknown", "{resumed}");
    assert_eq!(platform.drain(resumed)["status"], "complete");
}

#[test]
fn provision_accepts_the_rum_capture_field_names_the_wizard_sends() {
    let platform = Platform::new();
    let response = platform.call(
        "POST",
        "/api/provision/execute",
        json!({
            "token":"fixture-token","service_id":"fixtureService","service_name":"Fixture",
            "fos_bucket_name":"fos-fixture-logs","fos_region":"us-east-1",
            "rum_enabled":false,"rum_capture_vitals":true,"rum_capture_performance":true,
            "rum_capture_errors":true,"rum_capture_events":true
        }),
    );
    assert_eq!(
        response.status,
        200,
        "{}",
        String::from_utf8_lossy(&response.body)
    );
}

#[test]
fn provision_persists_non_default_auto_delete_and_retention_settings() {
    let platform = Platform::new();
    platform.json(
        "POST",
        "/api/admin/provision/execute",
        json!({
            "token":"fixture-token","service_id":"fixtureService","fos_bucket":"fos-fixture-logs",
            "fos_region":"us-east-1","delete_after":false,"log_retention_days":7,
            "rum_retention_days":90
        }),
    );
    let service = platform.adapter.records()["svc:fixtureService"]
        .value
        .clone();
    assert_eq!(service["delete_after"], false, "{service}");
    assert_eq!(service["log_retention_days"], 7);
    assert_eq!(service["rum_retention_days"], 90);
}

#[test]
fn compaction_setting_defaults_off_and_patch_validates_it() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    let current = platform.service();
    assert_eq!(
        current["service"]["compaction"],
        json!({"mode": "off", "size_target_mb": 128})
    );
    for bad in [
        json!({"mode": "always", "size_target_mb": 64}),
        json!({"mode": "size_target", "size_target_mb": 0}),
        json!({"mode": "size_target", "size_target_mb": 257}),
    ] {
        let response = platform.call(
            "PATCH",
            "/api/admin/services/fixtureService",
            json!({"generation": current["generation"], "compaction": bad}),
        );
        assert_eq!(response.status, 400, "{bad}");
    }
    let updated = platform.json(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation": current["generation"],
               "compaction": {"mode": "size_target", "size_target_mb": 32}}),
    );
    assert_eq!(
        updated["service"]["compaction"],
        json!({"mode": "size_target", "size_target_mb": 32})
    );
}

#[test]
fn provision_persists_a_chosen_compaction_setting() {
    let platform = Platform::new();
    platform.json(
        "POST",
        "/api/admin/provision/execute",
        json!({
            "token":"fixture-token","service_id":"fixtureService","fos_bucket":"fos-fixture-logs",
            "fos_region":"us-east-1","compaction":{"mode":"size_target","size_target_mb":128}
        }),
    );
    let service = platform.adapter.records()["svc:fixtureService"]
        .value
        .clone();
    assert_eq!(
        service["compaction"],
        json!({"mode": "size_target", "size_target_mb": 128})
    );
}

#[test]
fn rejected_job_claim_releases_the_service_lease() {
    let platform = Platform::new();
    let job = platform.provision()["job"].clone();
    let step = format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap());
    platform.reject_job_claim.store(true, Ordering::SeqCst);
    let rejected = platform.call("POST", &step, json!({}));
    assert_eq!(rejected.status, 409);
    assert_eq!(
        platform.adapter.records()["reconcile_lock:fixtureService"].value["expires_at"],
        0
    );
    let retried = platform.call("POST", &step, json!({}));
    assert_eq!(retried.status, 200);
}

#[test]
fn mutation_attempt_limit_survives_intervening_readback_failures() {
    let platform = Platform::new();
    let mut job = platform.until(platform.provision()["job"].clone(), "apply");
    assert_eq!(job["changes"][0]["name"], "Fastly Log Analytics - vcl_recv");
    let step = format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap());
    for attempt in 1..=4 {
        platform.api_faults.lock().unwrap().push(ApiFault {
            method: "POST",
            path: "/service/fixtureService/version/2/snippet",
            name: None,
            status: 429,
            retry_after: None,
        });
        if let Some(at) = job["retry_at"].as_u64() {
            platform.clock.fetch_max(at, Ordering::SeqCst);
        }
        job = platform.json("POST", &step, json!({}))["job"].clone();
        assert_eq!(job["retry_attempt"], attempt, "{job}");
        if attempt == 4 {
            break;
        }
        platform.api_faults.lock().unwrap().push(ApiFault {
            method: "GET",
            path:
                "/service/fixtureService/version/2/snippet/Fastly%20Log%20Analytics%20-%20vcl_recv",
            name: None,
            status: 503,
            retry_after: Some(std::time::Duration::from_secs(30)),
        });
        platform
            .clock
            .fetch_max(job["retry_at"].as_u64().unwrap(), Ordering::SeqCst);
        job = platform.json("POST", &step, json!({}))["job"].clone();
        assert_eq!(job["retry_reason"], "fastly_api_unavailable", "{job}");
    }
    assert_eq!(job["status"], "failed", "{job}");
    assert_eq!(job["error"], "fastly_api_rate_limited");
}

#[test]
fn uncertain_canonical_secret_write_stops_without_exceeding_secret_budget() {
    let platform = Platform::new();
    let job = platform.until(platform.provision()["job"].clone(), "key");
    platform.api_faults.lock().unwrap().push(ApiFault {
        method: "PUT",
        path: "/resources/stores/secret/fixtureSecrets/secrets",
        name: Some("fos_ak.fixtureService"),
        status: 503,
        retry_after: None,
    });
    let step = format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap());
    let pending = platform.json("POST", &step, json!({}))["job"].clone();
    assert_eq!(pending["status"], "pending");
    platform
        .clock
        .fetch_max(pending["retry_at"].as_u64().unwrap(), Ordering::SeqCst);
    let rejected = platform.exhaust(platform.json("POST", &step, json!({}))["job"].clone());
    assert_eq!(rejected["status"], "failed", "{rejected}");
    assert_eq!(rejected["error"], "credentials_pending");
    assert_eq!(platform.adapter.key_count(), 2);
    assert_eq!(
        platform.adapter.records()[&format!("job:{}", job["id"].as_str().unwrap())].value
            ["uncertain_operations"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn delayed_secret_visibility_retries_on_schedule_and_resumes_without_rotating_keys() {
    let platform = Platform::new();
    let job = platform.until(platform.provision()["job"].clone(), "bucket_probe");
    platform.adapter.override_secret_reads("fos_tmp_", Ok(None));
    let step = format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap());
    let first = platform.json("POST", &step, json!({}))["job"].clone();
    assert_eq!(first["status"], "pending", "{first}");
    assert_eq!(first["step"], "bucket_probe");
    assert_eq!(first["retry_attempt"], 1);
    assert_eq!(first["retry_reason"], "credentials_pending");
    assert_eq!(first["retry_at"], platform.now() + 10);

    let before = platform.adapter.calls().len();
    let waiting = platform.json("POST", &step, json!({}))["job"].clone();
    assert_eq!(waiting["retry_attempt"], 1);
    assert_eq!(
        platform.adapter.calls()[before..],
        [(
            "KV_GET".to_string(),
            format!("job:{}", job["id"].as_str().unwrap())
        )]
    );

    platform
        .clock
        .fetch_max(first["retry_at"].as_u64().unwrap(), Ordering::SeqCst);
    let second = platform.json("POST", &step, json!({}))["job"].clone();
    assert_eq!(second["retry_attempt"], 2, "{second}");

    platform.adapter.clear_secret_reads();
    platform.drain(second);
    assert_eq!(platform.service()["service"]["status"], "active");
    assert_eq!(platform.adapter.key_count(), 1);
    assert_eq!(
        platform
            .adapter
            .calls()
            .iter()
            .filter(|(m, p)| m == "POST" && p == "/resources/object-storage/access-keys")
            .count(),
        2
    );
}

#[test]
fn secret_propagation_retries_stop_at_the_budget_and_a_manual_retry_resets_it() {
    let platform = Platform::new();
    let job = platform.until(platform.provision()["job"].clone(), "bucket_probe");
    platform.adapter.override_secret_reads("fos_tmp_", Ok(None));
    let step = format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap());
    let first = platform.json("POST", &step, json!({}))["job"].clone();
    let failed = platform.exhaust(first);
    assert_eq!(failed["status"], "failed", "{failed}");
    assert_eq!(failed["error"], "credentials_pending");
    assert_eq!(failed["retry_attempt"], 24);
    assert!(failed["retry_at"].is_null(), "{failed}");

    platform.adapter.clear_secret_reads();
    let resumed = platform.retry(&failed);
    assert_eq!(resumed["retry_attempt"], 0, "{resumed}");
    platform.drain(resumed);
    assert_eq!(platform.service()["service"]["status"], "active");
}

#[test]
fn mutation_dispatch_intent_survives_a_failed_timeout_checkpoint() {
    for clone in [false, true] {
        let platform = Platform::new();
        let job = platform.provision()["job"].clone();
        let job = if clone {
            platform.until(job, "clone")
        } else {
            job
        };
        let step = format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap());
        if clone {
            platform.json("POST", &step, json!({}));
        }

        let (method, path) = if clone {
            ("PUT", "/service/fixtureService/version/1/clone")
        } else {
            ("POST", "/resources/object-storage/access-keys")
        };
        platform.api_delays.lock().unwrap().push(ApiDelay {
            method,
            path,
            millis: 31000,
            after_effect: false,
        });
        platform
            .fail_timeout_checkpoint
            .store(true, Ordering::SeqCst);
        let response = platform.call("POST", &step, json!({}));
        assert_eq!(response.status, 503);
        assert_eq!(
            serde_json::from_slice::<Value>(&response.body).unwrap()["error"],
            "state_write_failed"
        );
        let persisted =
            &platform.adapter.records()[&format!("job:{}", job["id"].as_str().unwrap())].value;
        assert_eq!(
            persisted["uncertain_operations"].as_array().unwrap().len(),
            1,
            "dispatch uncertainty must predate the external mutation"
        );
        platform.clock.fetch_add(45, Ordering::SeqCst);
        assert_eq!(
            platform.call("POST", &step, json!({})).status,
            409,
            "a claimed job must remain leased throughout its step wall budget"
        );
        platform.clock.fetch_add(16, Ordering::SeqCst);
        let rejected = platform.json("POST", &step, json!({}))["job"].clone();
        assert_eq!(rejected["error"], "mutation_outcome_unknown", "{rejected}");
        assert_eq!(
            platform
                .adapter
                .calls()
                .iter()
                .filter(|(verb, value)| verb == method && value == path)
                .count(),
            0
        );
        assert_eq!(platform.late_requests.lock().unwrap().len(), 1);
    }
}

#[test]
fn expired_grant_after_journaling_does_not_poison_an_undispatched_mutation() {
    let platform = Platform::new();
    let job = platform.provision()["job"].clone();
    platform.journal_delay_ms.store(43500, Ordering::SeqCst);
    let step = format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap());
    let pending = platform.json("POST", &step, json!({}))["job"].clone();
    assert_eq!(pending["status"], "pending");
    assert_eq!(pending["retry_reason"], "wall_time_budget");
    assert_eq!(pending["retry_attempt"], 0);
    let saved = platform.adapter.records();
    let record = &saved[&format!("job:{}", job["id"].as_str().unwrap())].value;
    assert_eq!(record["uncertain_operations"], json!([]));
    assert_eq!(record["retry_attempts"], json!({}));
    assert_eq!(platform.adapter.key_count(), 0);
    assert_eq!(
        platform
            .adapter
            .calls()
            .iter()
            .filter(|(method, path)| method == "POST"
                && path == "/resources/object-storage/access-keys")
            .count(),
        0
    );
    platform.drain(pending);
    assert_eq!(platform.service()["service"]["status"], "active");
}

fn slow_fos_step_completes(method: &'static str, step: &str) {
    for millis in [7500, 8500] {
        let platform = Platform::new();
        let provision = platform.provision()["job"].clone();
        let job = if method == "GET" {
            platform.drain(provision);
            platform
                .adapter
                .seed_object("fos-fixture-logs", "raw/request/remove.gz");
            let service = platform.service();
            let teardown = platform.json(
                "POST",
                "/api/provision/teardown",
                json!({
                    "service_id":"fixtureService","confirm_service_id":"fixtureService",
                    "token":"fixture-token","generation":service["generation"],"delete_data":true
                }),
            );
            platform.until(teardown["job"].clone(), step)
        } else {
            let job = platform.until(provision, step);
            if method == "HEAD" {
                platform
                    .adapter
                    .seed_object("fos-fixture-logs", "customer/preserve.gz");
            }
            job
        };
        platform.fos_timeouts.lock().unwrap().clear();
        platform
            .fos_delays
            .lock()
            .unwrap()
            .push(FosDelay { method, millis });
        let result = platform.json(
            "POST",
            &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
            json!({}),
        );
        assert_eq!(
            result["job"]["status"], "pending",
            "{method} {millis}ms: {result}"
        );
        assert!(result["job"]["error"].is_null(), "{result}");
        assert!(result["job"]["retry_reason"].is_null(), "{result}");
        assert!(platform.fos_delays.lock().unwrap().is_empty());
        assert_eq!(ELAPSED_MS.with(Cell::get), millis);
        assert_eq!(
            platform.fos_timeouts.lock().unwrap()[0],
            std::time::Duration::from_secs(43)
        );
        platform.drain(result["job"].clone());
        assert_eq!(
            platform.service()["service"]["status"],
            if method == "GET" {
                "torn_down"
            } else {
                "active"
            }
        );
        if method == "HEAD" {
            assert_eq!(
                platform.adapter.object_keys("fos-fixture-logs"),
                vec!["customer/preserve.gz"]
            );
        } else if method == "GET" {
            assert!(platform.adapter.object_keys("fos-fixture-logs").is_empty());
        }
    }
}

#[test]
fn slow_fos_head_completes_inside_the_step_wall_budget() {
    slow_fos_step_completes("HEAD", "bucket_probe");
}

#[test]
fn slow_fos_put_completes_inside_the_step_wall_budget() {
    slow_fos_step_completes("PUT", "bucket_create");
}

#[test]
fn slow_fos_list_completes_inside_the_step_wall_budget() {
    slow_fos_step_completes("GET", "delete_page");
}

#[test]
fn fos_does_not_dispatch_after_the_business_grant_is_exhausted() {
    for elapsed in [43000, 44999, 45000] {
        let platform = Platform::new();
        CALLS.with(|calls| calls.set(7));
        ELAPSED_MS.with(|clock| clock.set(0));
        let context = admin::transport::Context::new(&platform);
        ELAPSED_MS.with(|clock| clock.set(elapsed));
        let error = context
            .fos(FosRequest {
                method: "PUT",
                region: "us-east-1",
                bucket: "fos-fixture-logs",
                key: "",
                query: "",
                access_key: "never-dispatched-ak",
                secret_key: "never-dispatched-sk",
                body: vec![],
                timeout: std::time::Duration::from_secs(60),
                response_limit: admin::transport::FOS_RESPONSE_LIMIT,
                content_md5: None,
            })
            .err()
            .expect("an exhausted business grant must reject before dispatch");
        assert_eq!(error.code, "wall_time_budget");
        assert_eq!(error.status, 503);
        let retry = error
            .retry
            .expect("an undispatched request must checkpoint for the next execution");
        assert_eq!(retry.reason, "wall_time_budget");
        assert_eq!(retry.at, platform.now() + 1);
        assert!(platform.fos_timeouts.lock().unwrap().is_empty());
        assert!(platform.adapter.calls().is_empty());
        assert_eq!(CALLS.with(Cell::get), 7);
    }
}

#[test]
fn fos_deadline_is_clipped_to_remaining_time_and_reports_expiration() {
    for millis in [8500, 28001] {
        let platform = Platform::new();
        platform.until(platform.provision()["job"].clone(), "bucket_probe");
        platform
            .adapter
            .seed_object("fos-fixture-logs", "customer/preserve.gz");
        let ak = platform
            .adapter
            .secret("fos_tmp_ak.fixtureService")
            .unwrap()
            .unwrap();
        let pair: Value = serde_json::from_str(
            &platform
                .adapter
                .secret("fos_tmp_sk.fixtureService")
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        platform.fos_delays.lock().unwrap().push(FosDelay {
            method: "HEAD",
            millis,
        });
        CALLS.with(|calls| calls.set(7));
        ELAPSED_MS.with(|clock| clock.set(0));
        let context = admin::transport::Context::new(&platform);
        ELAPSED_MS.with(|clock| clock.set(15000));
        let result = context.fos(FosRequest {
            method: "HEAD",
            region: "us-east-1",
            bucket: "fos-fixture-logs",
            key: "",
            query: "",
            access_key: &ak,
            secret_key: pair["secret_key"].as_str().unwrap(),
            body: vec![],
            timeout: std::time::Duration::from_secs(60),
            response_limit: admin::transport::FOS_RESPONSE_LIMIT,
            content_md5: None,
        });
        if millis == 8500 {
            assert_eq!(result.unwrap().status, 200);
            assert_eq!(ELAPSED_MS.with(Cell::get), 23500);
        } else {
            let error = result
                .err()
                .expect("the remaining grant must bound the request");
            assert_eq!(error.code, "object_storage_backend_deadline");
            assert_eq!(error.status, 502);
            assert_eq!(ELAPSED_MS.with(Cell::get), 43000);
        }
        assert_eq!(
            *platform.fos_timeouts.lock().unwrap(),
            vec![std::time::Duration::from_secs(28)]
        );
    }
}

#[test]
fn rum_provision_reconcile_and_teardown_manage_backend_dictionary_fos_assets_and_endpoints() {
    let platform = Platform::new();
    let versions = platform.json("GET", "/api/admin/rum/versions", json!({}));
    assert_eq!(versions["default"], "2.13.0");
    assert_eq!(versions["versions"], json!(["2.13.0", "2.10.0"]));

    let result = platform.json(
        "POST",
        "/api/admin/provision/execute",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1",
            "logging_enabled": true,
            "rum": {
                "enabled": true,
                "log_period": 15,
                "custom_condition": "req.http.Host == \"example.com\"",
                "faro_version": "2.10.0",
                "endpoint_name": "Fastly RUM Logs",
                "capture_vitals": true,
                "capture_performance": true,
                "capture_errors": true,
                "capture_events": true
            }
        }),
    );
    platform.drain(result["job"].clone());
    let current = platform.service();
    assert_eq!(current["service"]["status"], "active");
    assert_eq!(current["service"]["rum_enabled"], true);
    assert_eq!(current["service"]["rum"]["enabled"], true);
    assert_eq!(current["service"]["rum"]["log_period"], 15);
    assert_eq!(
        current["service"]["owned_endpoint_names"],
        json!(["Fastly Object Storage Logs", "Fastly RUM Logs"])
    );

    let endpoints = platform.adapter.resources("fixtureService", "logging/s3");
    assert_eq!(endpoints.len(), 2);
    assert!(endpoints
        .iter()
        .any(|e| e["name"] == "Fastly Object Storage Logs"));
    assert!(endpoints
        .iter()
        .any(|e| e["name"] == "Fastly RUM Logs" && e["period"] == 15));

    let backends = platform.adapter.resources("fixtureService", "backend");
    assert_eq!(backends.len(), 1);
    assert_eq!(backends[0]["name"], "fos_origin");
    assert_eq!(
        backends[0]["request_condition"],
        "fastly_log_analytics_false"
    );

    let dicts = platform.adapter.resources("fixtureService", "dictionary");
    assert_eq!(dicts.len(), 1);
    assert_eq!(dicts[0]["name"], "fos_credentials");
    let items = platform
        .adapter
        .dictionary_items("dict-fixtureService-fos_credentials");
    assert_eq!(
        items.get("bucket").map(String::as_str),
        Some("fos-fixture-logs")
    );
    assert_eq!(items.get("region").map(String::as_str), Some("us-east-1"));
    assert!(items.get("access_key").unwrap().starts_with("fixture-ak-"));

    let objects = platform.adapter.bucket_objects("fos-fixture-logs");
    assert_eq!(
        objects
            .get("rum/faro-web-sdk-v2.10.0.iife.js")
            .map(Vec::len),
        Some(103553)
    );
    let tracker = String::from_utf8(objects.get("rum/rum-tracker.js").unwrap().clone()).unwrap();
    assert!(tracker.contains("var CAPTURE_VITALS = true;"));

    // Toggle capture_vitals via PATCH + reconcile (no VCL diff, updates rum-tracker.js in FOS).
    let patched = platform.json(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({
            "generation": current["generation"],
            "capture_vitals": false
        }),
    );
    assert_eq!(patched["service"]["capture_vitals"], false);
    let job = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({ "generation": patched["generation"] }),
    )["job"]
        .clone();
    platform.drain(job);
    let objects_after = platform.adapter.bucket_objects("fos-fixture-logs");
    let tracker_after =
        String::from_utf8(objects_after.get("rum/rum-tracker.js").unwrap().clone()).unwrap();
    assert!(tracker_after.contains("var CAPTURE_VITALS = false;"));
    assert_eq!(platform.service()["service"]["faro_version"], "2.10.0");

    // The pinned Faro version only changes on an explicit, supported upgrade.
    let current = platform.service();
    let unsupported = platform.call(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation": current["generation"], "faro_version": "2.11.0"}),
    );
    assert_eq!(unsupported.status, 400);
    let upgraded = platform.json(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation": current["generation"], "faro_version": "2.13.0"}),
    );
    assert_eq!(upgraded["service"]["faro_version"], "2.13.0");
    assert_eq!(upgraded["service"]["rum"]["faro_version"], "2.13.0");
    let job = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({ "generation": upgraded["generation"] }),
    )["job"]
        .clone();
    platform.drain(job);
    let objects_upgraded = platform.adapter.bucket_objects("fos-fixture-logs");
    assert_eq!(
        objects_upgraded
            .get("rum/faro-web-sdk-v2.13.0.iife.js")
            .map(Vec::len),
        Some(126190)
    );
    assert!(!objects_upgraded.contains_key("rum/faro-web-sdk-v2.10.0.iife.js"));
    let snippets = platform
        .adapter
        .resources("fixtureService", "snippet")
        .iter()
        .map(|snippet| snippet["content"].as_str().unwrap_or_default().to_owned())
        .collect::<String>();
    assert!(snippets.contains("/rum/faro-web-sdk-v2.13.0.iife.js"));
    assert!(!snippets.contains("faro-web-sdk-v2.10.0"));

    // Services always use the smallest compact wire format (zstd + positional array) and reject gzip/json.
    let current = platform.service();
    assert_eq!(current["service"]["compression_codec"], "zstd");
    assert_eq!(current["service"]["wire_format"], "array");
    for ep in platform.adapter.resources("fixtureService", "logging/s3") {
        assert_eq!(ep["compression_codec"], "zstd", "{ep}");
        assert!(ep.get("gzip_level").is_none(), "{ep}");
        assert!(ep["format"].as_str().unwrap().starts_with('['), "{ep}");
    }
    assert_eq!(
        platform
            .call(
                "PATCH",
                "/api/admin/services/fixtureService",
                json!({"generation": current["generation"], "compression_codec": "gzip"}),
            )
            .status,
        400
    );
    assert_eq!(
        platform
            .call(
                "PATCH",
                "/api/admin/services/fixtureService",
                json!({"generation": current["generation"], "compression_codec": "lz4"}),
            )
            .status,
        400
    );
    assert_eq!(
        platform
            .call(
                "PATCH",
                "/api/admin/services/fixtureService",
                json!({"generation": current["generation"], "wire_format": "json"}),
            )
            .status,
        400
    );
    assert_eq!(
        platform
            .call(
                "PATCH",
                "/api/admin/services/fixtureService",
                json!({"generation": current["generation"], "wire_format": "csv"}),
            )
            .status,
        400
    );
    let restored = platform.json(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({
            "generation": current["generation"],
            "compression_codec": "",
            "wire_format": ""
        }),
    );
    assert_eq!(restored["service"]["compression_codec"], "zstd");
    assert_eq!(restored["service"]["wire_format"], "array");
    let job = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({ "generation": restored["generation"] }),
    )["job"]
        .clone();
    platform.drain(job);
    for ep in platform.adapter.resources("fixtureService", "logging/s3") {
        assert_eq!(ep["compression_codec"], "zstd", "{ep}");
        assert!(ep.get("gzip_level").is_none(), "{ep}");
        assert!(ep["format"].as_str().unwrap().starts_with('['), "{ep}");
    }

    // Teardown removes backend, dictionary, endpoints, snippets, conditions, and FOS RUM assets.
    let current = platform.service();
    let teardown = platform.json(
        "POST",
        "/api/admin/provision/teardown",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "confirm_service_id": "fixtureService",
            "generation": current["generation"],
            "remove_logging": true,
            "remove_snippets": true,
            "delete_fos_keys": true,
            "delete_data": false,
            "delete_fos_bucket": false
        }),
    );
    platform.drain(teardown["job"].clone());
    assert_eq!(platform.service()["service"]["status"], "torn_down");
    assert!(platform
        .adapter
        .resources("fixtureService", "backend")
        .is_empty());
    assert!(platform
        .adapter
        .resources("fixtureService", "dictionary")
        .is_empty());
    assert!(platform
        .adapter
        .resources("fixtureService", "logging/s3")
        .is_empty());
    assert!(platform.adapter.object_keys("fos-fixture-logs").is_empty());
}

#[test]
fn retention_settings_patch_validates_and_records_a_service_audit_event() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    let current = platform.service();
    assert_eq!(current["service"]["delete_after"], true);
    assert_eq!(current["service"]["log_retention_days"], 30);
    let bad = platform.call(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation":current["generation"],"log_retention_days":4000}),
    );
    assert_eq!(bad.status, 400);
    let updated = platform.json(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation":current["generation"],"delete_after":false,"log_retention_days":0,"rum_retention_days":90}),
    );
    assert_eq!(updated["service"]["delete_after"], false);
    assert_eq!(updated["service"]["log_retention_days"], 0);
    assert_eq!(updated["service"]["rum_retention_days"], 90);
    let record = platform.adapter.get("svc:fixtureService").unwrap().unwrap();
    assert!(record.value["_audit"]
        .to_string()
        .contains("config.updated"));
}

/// Pins the KV billing shape of a steady-state idle tick: the cost analysis in
/// `docs/investigations/2026-11-02-compute-cost-optimization.md` rests on reads dominating
/// writes, and on the reported `kv_ops` exceeding the ops actually performed.
#[test]
fn an_idle_tick_is_read_dominated_and_over_reports_its_kv_ops() {
    use fastly_log_analytics::{shell::Shell, state::MemoryStore};

    let platform = Platform::new();
    let store = MemoryStore::default();
    platform.drain(platform.provision()["job"].clone());
    platform.clock.fetch_add(120, Ordering::SeqCst);
    platform.shield.store(true, Ordering::SeqCst);

    let tick_req = Request {
        method: "GET".into(),
        path: "/_tick".into(),
        headers: BTreeMap::from([("x-flp-front-key".into(), "fixture-front-key".into())]),
        body: vec![],
    };

    // Warm past the one-off first-tick work so the measured tick is steady state.
    for _ in 0..3 {
        platform.clock.fetch_add(120, Ordering::SeqCst);
        CALLS.with(|c| c.set(0));
        SECRETS.with(|s| s.borrow_mut().clear());
        assert_eq!(
            Shell::new(&platform, &store, &[]).handle(&tick_req).status,
            200
        );
    }

    platform.clock.fetch_add(120, Ordering::SeqCst);
    CALLS.with(|c| c.set(0));
    SECRETS.with(|s| s.borrow_mut().clear());
    KV_CLASS.with(|ops| ops.borrow_mut().clear());
    assert_eq!(
        Shell::new(&platform, &store, &[]).handle(&tick_req).status,
        200
    );
    let ops = KV_CLASS.with(|ops| ops.borrow().clone());
    let reads = ops.iter().filter(|(c, _)| c.starts_with("read")).count();
    let writes = ops.iter().filter(|(c, _)| *c == "write").count();
    eprintln!("idle tick KV ops ({} total): {ops:#?}", ops.len());
    eprintln!("reads={reads} writes={writes}");

    assert_eq!(
        (reads, writes),
        (5, 2),
        "steady-state idle tick should perform 5 KV reads and 2 KV writes: {ops:#?}"
    );
    assert!(
        reads > writes,
        "idle tick should be read-dominated, got {reads} reads vs {writes} writes: {ops:#?}"
    );
    assert!(
        !ops.iter().any(|(c, _)| *c == "read_api"),
        "an idle tick should not reach the control-plane API: {ops:#?}"
    );
    // Mutable schedule and ingest-coordination keys remain uncached KV reads every tick.
    for key in [
        "cron:state",
        "ingest:plan",
        "ingest:watermark",
        "ingest:backfill",
    ] {
        assert!(
            ops.iter().any(|(c, k)| *c == "read" && k == key),
            "expected an uncached read of {key}: {ops:#?}"
        );
    }
    // Stable config keys (`admin:install`, `svc:{sid}`, `settings:operational_logs`) are served
    // from Core Cache after warm-up rather than billing Class B reads on every tick.
    for key in [
        "admin:install",
        "svc:fixtureService",
        "settings:operational_logs",
    ] {
        assert!(
            !ops.iter().any(|(_, k)| k == key),
            "expected {key} to be served from KV cache on steady-state tick: {ops:#?}"
        );
    }
}

fn shell_post(
    platform: &Platform,
    role: fastly_log_analytics::auth::Role,
    store: &fastly_log_analytics::state::MemoryStore,
    path: &str,
    body: Value,
) -> Response {
    use fastly_log_analytics::{
        auth::{self, webauthn::Credential, AuthState, User},
        shell::Shell,
        state::StateStore,
    };
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;
    if store.load().unwrap().generation.is_none() {
        let mut state = AuthState::default();
        state.users.insert(
            "signed-user".into(),
            User {
                id: "signed-user".into(),
                display_name: "Test".into(),
                role,
                credentials: vec![Credential {
                    id: "credential".into(),
                    public_key: vec![1],
                    sign_count: 0,
                    revoked: false,
                }],
                revoked: false,
                created_by: "test".into(),
                created_at: platform.now(),
            },
        );
        assert!(store.compare_and_swap(None, &state).unwrap());
    }
    let claims = auth::encode(
        json!({"sub":"signed-user","role":role,"policy_version":0,"exp":platform.now()+3600})
            .to_string()
            .as_bytes(),
    );
    let mut mac =
        Hmac::<Sha256>::new_from_slice(b"fixture-session-signing-material-32-bytes").unwrap();
    mac.update(claims.as_bytes());
    let cookie = format!(
        "flp_session={claims}.{}",
        auth::encode(&mac.finalize().into_bytes())
    );
    CALLS.with(|calls| calls.set(7));
    SECRETS.with(|v| *v.borrow_mut() = BTreeSet::from(["front_key".into(), "session_key".into()]));
    Shell::new(platform, store, &[]).handle(&Request {
        method: "POST".into(),
        path: path.into(),
        headers: BTreeMap::from([
            ("x-flp-front-key".into(), "fixture-front-key".into()),
            ("fastly-client-ip".into(), "192.0.2.1".into()),
            ("x-flp-front-rl".into(), "1".into()),
            ("origin".into(), "https://logs.example.com".into()),
            ("content-type".into(), "application/json".into()),
            ("cookie".into(), cookie),
        ]),
        body: body.to_string().into_bytes(),
    })
}

fn provisioned() -> Platform {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    platform
}

const DELETE_DATA: &str = "/api/admin/services/fixtureService/delete-data";

fn delete_data_body(platform: &Platform) -> Value {
    json!({"confirm_service_id":"fixtureService","generation":platform.service()["generation"]})
}

#[test]
fn delete_data_requires_admin_and_matching_confirm_id() {
    use fastly_log_analytics::{auth::Role, state::MemoryStore};
    let platform = provisioned();
    let mut body = delete_data_body(&platform);
    body["confirm_service_id"] = "otherService".into();
    let response = platform.call("POST", DELETE_DATA, body);
    assert_eq!(response.status, 400);
    assert_eq!(
        serde_json::from_slice::<Value>(&response.body).unwrap()["error"],
        "confirm_service_id_mismatch"
    );
    for role in [Role::Analyst, Role::AnalystNoPii] {
        let store = MemoryStore::default();
        let response = shell_post(
            &platform,
            role,
            &store,
            DELETE_DATA,
            delete_data_body(&platform),
        );
        assert_eq!(response.status, 403, "{role:?}");
    }
    let records = platform.adapter.records();
    assert!(!records.contains_key(admin::reset::JOB_KEY));
    assert!(platform.service()["service"].get("data_reset").is_none());
}

#[test]
fn delete_data_start_sets_marker_job_and_audit() {
    use fastly_log_analytics::{
        auth::Role,
        state::{MemoryStore, StateStore},
    };
    let platform = provisioned();
    let store = MemoryStore::default();
    let now = platform.now();
    let response = shell_post(
        &platform,
        Role::Admin,
        &store,
        DELETE_DATA,
        delete_data_body(&platform),
    );
    assert_eq!(
        response.status,
        200,
        "{}",
        String::from_utf8_lossy(&response.body)
    );
    let started: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(started["job"]["status"], "fencing");
    let service = platform.service();
    let marker: admin::DataReset =
        serde_json::from_value(service["service"]["data_reset"].clone()).unwrap();
    assert_eq!(marker.phase, admin::ResetPhase::Fencing);
    assert_eq!(marker.reset_at, now - now % 60);
    assert_eq!(marker.started_at, now);
    assert_eq!(marker.actor, "signed-user");
    assert_eq!(marker.job_id, started["job"]["job_id"]);
    assert_eq!(
        started["service"]["data_reset"],
        service["service"]["data_reset"]
    );
    let records = platform.adapter.records();
    let job = &records[admin::reset::JOB_KEY].value;
    assert_eq!(job["status"], "fencing");
    assert_eq!(job["messages"].as_array().unwrap().len(), 1);
    assert_eq!(
        job["messages"][0]["text"],
        "Pausing ingest for this service…"
    );
    let state = store.load().unwrap().state;
    let entry = state
        .recent_audit
        .iter()
        .chain(&state.audit)
        .find(|entry| entry.action == "delete_data_started")
        .expect("audit entry");
    assert_eq!(entry.actor, "signed-user");
    assert_eq!(entry.target, "fixtureService");
}

#[test]
fn second_delete_data_start_is_rejected() {
    let platform = provisioned();
    let first = platform.json("POST", DELETE_DATA, delete_data_body(&platform));
    let response = platform.call("POST", DELETE_DATA, delete_data_body(&platform));
    assert_eq!(response.status, 409);
    assert_eq!(
        serde_json::from_slice::<Value>(&response.body).unwrap()["error"],
        "data_reset_in_progress"
    );
    let records = platform.adapter.records();
    assert_eq!(
        records[admin::reset::JOB_KEY].value["job_id"],
        first["job"]["job_id"]
    );
}

#[test]
fn data_reset_marker_survives_service_record_round_trip() {
    let platform = provisioned();
    platform.json("POST", DELETE_DATA, delete_data_body(&platform));
    let record = platform.adapter.records()["svc:fixtureService"].clone();
    let service: admin::Service = serde_json::from_value(record.value).unwrap();
    let marker = service.data_reset.clone().expect("marker");
    let again: admin::Service =
        serde_json::from_value(serde_json::to_value(&service).unwrap()).unwrap();
    assert_eq!(again.data_reset, Some(marker));
}

#[test]
fn delete_data_status_reports_phase_counts_and_last_50_messages() {
    let platform = provisioned();
    assert_eq!(
        platform.json("GET", DELETE_DATA, json!({}))["status"],
        "none"
    );
    let mut job = admin::reset::ResetJob {
        job_id: "job1".into(),
        status: "cleanup".into(),
        bytes: 42,
        pending: 7,
        error: Some("boom".into()),
        ..Default::default()
    };
    job.deleted.insert("request".into(), 3);
    for i in 0..60 {
        admin::reset::push_message(&mut job, 1_800_000_000 + i, format!("message {i}"));
    }
    assert!(platform
        .adapter
        .cas(
            admin::reset::JOB_KEY,
            None,
            &serde_json::to_value(&job).unwrap()
        )
        .unwrap());
    let status = platform.json("GET", DELETE_DATA, json!({}));
    assert_eq!(status["status"], "cleanup");
    assert_eq!(status["bytes"], 42);
    assert_eq!(status["pending"], 7);
    assert_eq!(status["error"], "boom");
    assert_eq!(status["deleted"]["request"], 3);
    let messages = status["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 50);
    assert_eq!(messages[0]["text"], "message 10");
    assert_eq!(messages[49]["text"], "message 59");
}

#[test]
fn delete_data_cancel_is_allowed_only_while_fencing() {
    let platform = provisioned();
    platform.json("POST", DELETE_DATA, delete_data_body(&platform));
    let cancel = format!("{DELETE_DATA}/cancel");
    let cancelled = platform.json("POST", &cancel, json!({}));
    assert_eq!(cancelled["job"]["status"], "cancelled");
    assert!(platform.service()["service"].get("data_reset").is_none());
    let response = platform.call("POST", &cancel, json!({}));
    assert_eq!(response.status, 409);
    platform.json("POST", DELETE_DATA, delete_data_body(&platform));
    assert_eq!(
        platform.json("GET", DELETE_DATA, json!({}))["status"],
        "fencing"
    );
}

#[test]
fn delete_data_cancel_retries_after_the_service_write_fails() {
    let platform = provisioned();
    let started = platform.json("POST", DELETE_DATA, delete_data_body(&platform));
    let cancel = format!("{DELETE_DATA}/cancel");
    platform
        .adapter
        .fail_next("KV_CAS", "svc:fixtureService", false);
    assert_eq!(platform.call("POST", &cancel, json!({})).status, 503);
    let records = platform.adapter.records();
    assert_eq!(records[admin::reset::JOB_KEY].value["status"], "cancelled");
    assert!(platform.service()["service"].get("data_reset").is_some());
    let retried = platform.json("POST", &cancel, json!({}));
    assert_eq!(retried["job"]["status"], "cancelled");
    assert!(platform.service()["service"].get("data_reset").is_none());
    let restarted = platform.json("POST", DELETE_DATA, delete_data_body(&platform));
    assert_ne!(restarted["job"]["job_id"], started["job"]["job_id"]);
    assert_eq!(restarted["job"]["status"], "fencing");
}

#[test]
fn sweep_backfill_and_quarantine_purge_refuse_while_fencing() {
    use fastly_log_analytics::{auth::Role, state::MemoryStore};
    let platform = provisioned();
    let store = MemoryStore::default();
    let started = platform.json("POST", DELETE_DATA, delete_data_body(&platform));
    assert_eq!(started["job"]["status"], "fencing");
    for (path, body) in [
        ("/api/admin/logs/sweep", json!({})),
        ("/api/admin/logs/backfill", json!({})),
        ("/api/admin/quarantine/purge", json!({"target": "all"})),
    ] {
        let response = shell_post(&platform, Role::Admin, &store, path, body);
        assert_eq!(
            response.status,
            409,
            "{path}: {}",
            String::from_utf8_lossy(&response.body)
        );
        let text = String::from_utf8_lossy(&response.body);
        assert!(text.contains("data_reset_in_progress"), "{path}: {text}");
    }
}

fn fence_tick(platform: &Platform) -> Value {
    if let Some(result) = fence_try(platform) {
        result.unwrap();
    }
    platform.adapter.records()[admin::reset::JOB_KEY]
        .value
        .clone()
}

fn fence_try(platform: &Platform) -> Option<Result<(), String>> {
    use fastly_log_analytics::ingest::storage::Store;
    CALLS.with(|calls| calls.set(0));
    SECRETS.with(|secrets| secrets.borrow_mut().clear());
    ELAPSED_MS.with(|elapsed| elapsed.set(0));
    let service: admin::Service = serde_json::from_value(
        platform.adapter.records()["svc:fixtureService"]
            .value
            .clone(),
    )
    .unwrap();
    let (access_key, secret_key) = admin::fos_credentials(platform, &service).unwrap();
    let store = Store {
        runtime: platform,
        service: &service,
        access_key,
        secret_key,
        started: std::time::Instant::now(),
    };
    let result = admin::reset::advance(&store);
    assert!(CALLS.with(Cell::get) <= 32);
    result
}

fn fence_started() -> (Platform, admin::Service) {
    let platform = provisioned();
    platform.json("POST", DELETE_DATA, delete_data_body(&platform));
    let service = serde_json::from_value(
        platform.adapter.records()["svc:fixtureService"]
            .value
            .clone(),
    )
    .unwrap();
    (platform, service)
}

fn seed(platform: &Platform, key: &str, value: Value) {
    let old = platform
        .adapter
        .records()
        .get(key)
        .map(|r| r.generation.clone());
    assert!(platform.adapter.cas(key, old.as_deref(), &value).unwrap());
}

fn snapshot(platform: &Platform, key: &str) -> (String, Value) {
    let record = platform.adapter.records()[key].clone();
    (record.generation, record.value)
}

fn advance_clock(platform: &Platform, seconds: u64) {
    platform.clock.fetch_add(seconds, Ordering::SeqCst);
}

fn last_message(job: &Value) -> String {
    job["messages"].as_array().unwrap().last().unwrap()["text"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn messages(job: &Value) -> Vec<String> {
    job["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["text"].as_str().unwrap().to_owned())
        .collect()
}

#[test]
fn fence_waits_out_leases_before_resetting() {
    use admin::reset::FENCE_WAIT_S;
    let (platform, _) = fence_started();
    let now = platform.now();
    seed(
        &platform,
        "ingest:plan",
        json!({"version": 3, "plan_id": "p1", "lease_until": now + FENCE_WAIT_S + 30}),
    );
    advance_clock(&platform, 30);
    let job = fence_tick(&platform);
    assert_eq!(job["status"], "fencing");
    assert_eq!(
        last_message(&job),
        format!(
            "Waiting for in-flight ingest to finish ({}s left)…",
            FENCE_WAIT_S - 30
        )
    );
    advance_clock(&platform, FENCE_WAIT_S - 20);
    let job = fence_tick(&platform);
    assert_eq!(job["status"], "fencing");
    assert_eq!(
        last_message(&job),
        "Waiting for in-flight ingest to finish (20s left)…"
    );
    assert!(platform.adapter.records().contains_key("ingest:plan"));
    advance_clock(&platform, 20);
    let job = fence_tick(&platform);
    assert_eq!(job["status"], "resetting", "{job}");
    assert!(platform.service()["service"].get("data_reset").is_some());
}

#[test]
fn fence_wait_covers_the_svc_read_cache_and_a_plan_lease() {
    use fastly_log_analytics::{ingest::coordinator::LEASE_S, shell::KV_READ_CACHE_TTL_S};
    const { assert!(admin::reset::FENCE_WAIT_S >= KV_READ_CACHE_TTL_S + LEASE_S) };
    assert_eq!(admin::reset::FENCE_WAIT_S, 420);
    assert_eq!(admin::reset::FENCE_GIVE_UP_S, 780);
}

#[test]
fn fence_abandons_uncommitted_plans_and_their_inflight_outputs() {
    let (platform, service) = fence_started();
    let inflight = fastly_log_analytics::ingest::inflight_prefix(&service);
    seed(
        &platform,
        "ingest:watermark",
        json!({"run_id": "committed", "catalog": {"generation": 4}}),
    );
    seed(
        &platform,
        "ingest:plan",
        json!({"version": 3, "plan_id": "p1", "lease_until": 0}),
    );
    seed(
        &platform,
        "ingest:rum:plan",
        json!({"version": 3, "plan_id": "r1", "lease_until": 0}),
    );
    let bucket = "fos-fixture-logs";
    for key in ["p1/0.json", "p1/1a.json", "r1/0.json", "other/0.json"] {
        platform
            .adapter
            .seed_object(bucket, &format!("{inflight}{key}"));
    }
    let watermark = snapshot(&platform, "ingest:watermark");
    advance_clock(&platform, admin::reset::FENCE_WAIT_S);
    let job = fence_tick(&platform);
    assert_eq!(job["status"], "resetting", "{job}");
    let records = platform.adapter.records();
    assert!(!records.contains_key("ingest:plan"));
    assert!(!records.contains_key("ingest:rum:plan"));
    assert_eq!(snapshot(&platform, "ingest:watermark"), watermark);
    let left: Vec<String> = platform
        .adapter
        .object_keys(bucket)
        .into_iter()
        .filter(|key| key.starts_with(&inflight))
        .collect();
    assert_eq!(left, vec![format!("{inflight}other/0.json")]);
    let texts = messages(&job);
    assert!(texts.iter().any(|t| t.contains("p1")), "{texts:?}");
    assert!(texts.iter().any(|t| t.contains("r1")), "{texts:?}");
}

#[test]
fn fence_aborts_in_progress_compaction_upload_and_clears_preparing_records() {
    let (platform, _) = fence_started();
    seed(
        &platform,
        "ingest:compact_state",
        json!({
            "lease_until": 0,
            "upload": {"key": "processed/request/stale.parquet", "id": "u1"},
            "job": {"upload": {"key": "processed/request/open.parquet", "id": "u2"}},
            "queue": []
        }),
    );
    for key in [
        "ingest:catalog_preparing",
        "ingest:catalog_replacement_preparing",
        "ingest:source_index_preparing",
    ] {
        seed(&platform, key, json!({"identity": "x"}));
    }
    FOS_LOG.with(|log| log.borrow_mut().clear());
    advance_clock(&platform, admin::reset::FENCE_WAIT_S);
    let job = fence_tick(&platform);
    assert_eq!(job["status"], "resetting", "{job}");
    let log = FOS_LOG.with(|log| log.borrow().clone());
    for (key, id) in [("stale", "u1"), ("open", "u2")] {
        let key = format!("processed/request/{key}.parquet");
        let query = format!("uploadId={id}");
        assert!(
            log.iter()
                .any(|(m, k, q)| m == "DELETE" && *k == key && *q == query),
            "{log:?}"
        );
    }
    let records = platform.adapter.records();
    for key in [
        "ingest:compact_state",
        "ingest:catalog_preparing",
        "ingest:catalog_replacement_preparing",
        "ingest:source_index_preparing",
    ] {
        assert!(!records.contains_key(key), "{key}");
    }
    assert!(
        messages(&job).iter().any(|t| t.contains("compaction")),
        "{job}"
    );
}

#[test]
fn committed_plan_is_left_alone() {
    let (platform, service) = fence_started();
    let inflight = fastly_log_analytics::ingest::inflight_prefix(&service);
    seed(&platform, "ingest:watermark", json!({"run_id": "p1"}));
    seed(
        &platform,
        "ingest:plan",
        json!({"version": 3, "plan_id": "p1", "lease_until": 0}),
    );
    platform
        .adapter
        .seed_object("fos-fixture-logs", &format!("{inflight}p1/0.json"));
    let plan = snapshot(&platform, "ingest:plan");
    advance_clock(&platform, admin::reset::FENCE_WAIT_S);
    let job = fence_tick(&platform);
    assert_eq!(job["status"], "resetting", "{job}");
    assert_eq!(snapshot(&platform, "ingest:plan"), plan);
    assert!(platform
        .adapter
        .object_keys("fos-fixture-logs")
        .contains(&format!("{inflight}p1/0.json")));
}

#[test]
fn fence_gives_up_after_bound_and_resumes_ingest_unchanged() {
    let (platform, _) = fence_started();
    let now = platform.now();
    seed(
        &platform,
        "ingest:watermark",
        json!({"run_id": "committed", "catalog": {"generation": 4}}),
    );
    seed(
        &platform,
        "ingest:plan",
        json!({"version": 3, "plan_id": "p1", "lease_until": now + 100_000}),
    );
    let before: Vec<_> = ["ingest:watermark", "ingest:plan"]
        .map(|key| snapshot(&platform, key))
        .into();
    advance_clock(&platform, admin::reset::FENCE_WAIT_S);
    assert_eq!(fence_tick(&platform)["status"], "fencing");
    advance_clock(
        &platform,
        admin::reset::FENCE_GIVE_UP_S - admin::reset::FENCE_WAIT_S,
    );
    let job = fence_tick(&platform);
    assert_eq!(job["status"], "failed", "{job}");
    assert!(job["error"].is_string());
    assert!(platform.service()["service"].get("data_reset").is_none());
    let after: Vec<_> = ["ingest:watermark", "ingest:plan"]
        .map(|key| snapshot(&platform, key))
        .into();
    assert_eq!(after, before);
    assert!(last_message(&job).contains("ingest resumes"), "{job}");
    let service: admin::Service = serde_json::from_value(
        platform.adapter.records()["svc:fixtureService"]
            .value
            .clone(),
    )
    .unwrap();
    assert!(!admin::reset::fencing(&service));
    assert_eq!(fence_tick(&platform), job);
}

#[test]
fn fence_gives_up_after_bound_on_a_persistent_error() {
    let (platform, _) = fence_started();
    seed(
        &platform,
        "ingest:watermark",
        json!({"run_id": "committed", "catalog": {"generation": 4}}),
    );
    let watermark = snapshot(&platform, "ingest:watermark");
    FAIL_KV_DELETE.with(|fail| fail.set(true));
    advance_clock(&platform, admin::reset::FENCE_WAIT_S);
    let error = fence_try(&platform).unwrap().unwrap_err();
    assert_eq!(error, "kv_delete_failed");
    let job = platform.adapter.records()[admin::reset::JOB_KEY]
        .value
        .clone();
    assert_eq!(job["status"], "fencing", "{job}");
    assert_eq!(job["error"], "kv_delete_failed");
    assert!(platform.service()["service"].get("data_reset").is_some());
    advance_clock(
        &platform,
        admin::reset::FENCE_GIVE_UP_S - admin::reset::FENCE_WAIT_S,
    );
    fence_try(&platform).unwrap().unwrap();
    FAIL_KV_DELETE.with(|fail| fail.set(false));
    let job = platform.adapter.records()[admin::reset::JOB_KEY]
        .value
        .clone();
    assert_eq!(job["status"], "failed", "{job}");
    assert_eq!(job["error"], "fence_timeout: kv_delete_failed");
    assert!(last_message(&job).contains("ingest resumes"), "{job}");
    assert!(platform.service()["service"].get("data_reset").is_none());
    let service: admin::Service = serde_json::from_value(
        platform.adapter.records()["svc:fixtureService"]
            .value
            .clone(),
    )
    .unwrap();
    assert!(!admin::reset::fencing(&service));
    assert_eq!(snapshot(&platform, "ingest:watermark"), watermark);
}

/// A fencing job past its wait whose watermarks hold pre-reset state, with every record the
/// reset clears.
fn reset_seeded() -> (Platform, admin::Service) {
    let (platform, service) = fence_started();
    let raw = fastly_log_analytics::ingest::raw_prefix(&service);
    let old = |prefix: &str| format!("{prefix}year=2027/month=01/day=15/hour=07/minute=00/a.zst");
    seed(
        &platform,
        "ingest:watermark",
        json!({
            "run_id": "committed", "key": old(&raw), "at": 1,
            "passes": [[old(&raw), 1]], "swept": [[old(&raw), 1]],
            "sweep": {"after": old(&raw), "at": 1},
            "raw_sources": {"index": "x"}, "raw_scan": old(&raw), "raw_scan_due": true,
            "manifest_gen": 7, "custom": "kept",
            "catalog": {
                "version": 2, "generation": 4, "manifest": "processed/_catalog/old.json",
                "recent": [{"cursor": "fixtureService:4", "generation": 4,
                    "committed_at": null, "manifest": "processed/_catalog/old.json"}],
                "days": {"1800000000": "processed/_catalog/day.json"},
                "statistics": {"1800000000": {"earliest": 1, "latest": 2, "rows": 3}},
                "hour_bundles": {"1800000000": {"key": "processed/_catalog/h.json", "generation": 4}},
                "bundle_retired": {"processed/summary_bundle/x.parquet": 1},
                "retirement_grace_until": 0
            }
        }),
    );
    let rum = raw.replace("raw/request/", "raw/rum/");
    seed(
        &platform,
        "ingest:rum:watermark",
        json!({"run_id": "rum-committed", "key": old(&rum), "at": 1, "passes": [[old(&rum), 1]],
            "swept": [], "sweep": {"after": old(&rum), "at": 1}, "manifest_gen": 2}),
    );
    for key in [
        "ingest:plan",
        "ingest:rum:plan",
        "ingest:backfill",
        "ingest:deleteq:p1",
        "ingest:deleteq:p2",
        "ingest:quarantine:raw/a.zst",
        "summary:bundle_state",
        "retention:request",
        "retention:rum",
        "retention:summary:request",
        "retention:summary_bundle:request",
    ] {
        seed(
            &platform,
            key,
            json!({"plan_id": "committed", "lease_until": 0}),
        );
    }
    for key in [
        "retention:last_sweep",
        "retention:purge",
        "history:x",
        "cron:state",
    ] {
        seed(&platform, key, json!({"kept": true}));
    }
    advance_clock(&platform, admin::reset::FENCE_WAIT_S);
    (platform, service)
}

/// [`reset_seeded`] after the fence step, so the next step resets.
fn reset_ready() -> (Platform, admin::Service) {
    let (platform, service) = reset_seeded();
    assert_eq!(fence_tick(&platform)["status"], "resetting");
    advance_clock(&platform, 2);
    (platform, service)
}

/// Steps the job until it leaves "resetting", as maintenance ticks would.
fn reset_tick(platform: &Platform) -> Value {
    let mut job = Value::Null;
    for _ in 0..4 {
        job = fence_tick(platform);
        if job["status"] != "resetting" {
            break;
        }
        advance_clock(platform, 2);
    }
    job
}

fn floor(service: &admin::Service, stream: &str, reset_at: u64) -> String {
    format!(
        "{}{}",
        fastly_log_analytics::ingest::raw_prefix(service)
            .replace("raw/request/", &format!("raw/{stream}/")),
        fastly_log_analytics::ingest::minute_dir(reset_at as i64)
    )
}

#[test]
fn reset_state_moves_cursor_and_floor_to_reset_minute() {
    let (platform, service) = reset_ready();
    let reset_at = service.data_reset.as_ref().unwrap().reset_at;
    let job = reset_tick(&platform);
    assert_eq!(job["status"], "cleanup", "{job}");
    let records = platform.adapter.records();
    let watermark = &records["ingest:watermark"].value;
    let request_floor = floor(&service, "request", reset_at);
    assert_eq!(watermark["key"], request_floor);
    assert_eq!(watermark["floor_key"], request_floor);
    assert_eq!(watermark["manifest_gen"], 8);
    assert_eq!(watermark["passes"], json!([]));
    assert_eq!(watermark["swept"], json!([]));
    assert_eq!(watermark["sweep"]["after"], request_floor);
    for field in ["raw_sources", "raw_scan", "raw_scan_due"] {
        assert!(watermark.get(field).is_none(), "{field}: {watermark}");
    }
    assert_eq!(watermark["run_id"], "committed");
    assert_eq!(watermark["custom"], "kept");
    let catalog = &watermark["catalog"];
    assert_eq!(catalog["generation"], 5);
    assert_eq!(catalog["recent"].as_array().unwrap().len(), 1);
    assert_eq!(catalog["days"], json!({}));
    assert_eq!(catalog["hour_bundles"], json!({}));
    assert_eq!(catalog["bundle_retired"], json!({}));
    let manifest = catalog["manifest"].as_str().unwrap();
    assert_eq!(catalog["recent"][0]["manifest"], manifest);
    assert_eq!(job["reset_manifest"], manifest);
    assert!(platform
        .adapter
        .object_keys("fos-fixture-logs")
        .contains(&manifest.to_owned()));
    assert_eq!(job["reset_cas_at"], watermark["at"]);
    let rum = &records["ingest:rum:watermark"].value;
    let rum_floor = floor(&service, "rum", reset_at);
    assert_eq!(rum["key"], rum_floor);
    assert_eq!(rum["floor_key"], rum_floor);
    assert_eq!(rum["manifest_gen"], 3);
    assert_eq!(rum["passes"], json!([]));
    assert_eq!(rum["sweep"]["after"], rum_floor);
    assert_eq!(rum["run_id"], "rum-committed");
    for key in [
        "ingest:plan",
        "ingest:rum:plan",
        "ingest:backfill",
        "ingest:deleteq:p1",
        "ingest:deleteq:p2",
        "ingest:quarantine:raw/a.zst",
        "summary:bundle_state",
        "retention:request",
        "retention:rum",
        "retention:summary:request",
        "retention:summary_bundle:request",
    ] {
        assert!(!records.contains_key(key), "{key}");
    }
    for key in [
        "retention:last_sweep",
        "retention:purge",
        "history:x",
        "cron:state",
    ] {
        assert_eq!(records[key].value["kept"], true, "{key}");
    }
}

#[test]
fn reset_state_purges_edge_and_status_cache_then_resumes_ingest() {
    let (platform, _) = reset_ready();
    platform.adapter.clear_calls();
    CACHE_PURGES.with(|purges| purges.borrow_mut().clear());
    let job = reset_tick(&platform);
    assert_eq!(job["status"], "cleanup", "{job}");
    assert!(platform.adapter.calls().contains(&(
        "POST".into(),
        "/service/fixtureFront/purge/flp-fos-fixtureService".into()
    )));
    assert_eq!(
        CACHE_PURGES.with(|purges| purges.borrow().clone()),
        vec![admin::status_surrogate("fixtureService")]
    );
    let service: admin::Service = serde_json::from_value(
        platform.adapter.records()["svc:fixtureService"]
            .value
            .clone(),
    )
    .unwrap();
    let marker = service.data_reset.as_ref().unwrap();
    assert_eq!(marker.phase, admin::ResetPhase::Cleanup);
    assert!(!admin::reset::fencing(&service), "ingest dispatch resumes");
    let texts = messages(&job);
    for text in ["Edge cache purged", "Catalog reset; ingest resumed"] {
        assert!(texts.iter().any(|t| t == text), "{text}: {texts:?}");
    }
}

#[test]
fn reset_state_retries_the_cleanup_marker_without_repeating_the_watermark_cas() {
    let (platform, _) = reset_ready();
    platform
        .adapter
        .fail_next("KV_CAS", "svc:fixtureService", false);
    let error = fence_try(&platform).unwrap().unwrap_err();
    assert!(error.contains("injected_failure"), "{error}");
    let job = platform.adapter.records()[admin::reset::JOB_KEY]
        .value
        .clone();
    assert_eq!(job["status"], "cleanup", "{job}");
    assert!(job["reset_cas_at"].is_u64());
    assert_eq!(
        platform.service()["service"]["data_reset"]["phase"],
        "fencing"
    );
    let watermark = snapshot(&platform, "ingest:watermark");
    advance_clock(&platform, 2);
    let job = fence_tick(&platform);
    assert_eq!(snapshot(&platform, "ingest:watermark"), watermark);
    assert_eq!(
        platform.service()["service"]["data_reset"]["phase"],
        "cleanup"
    );
    assert!(messages(&job)
        .iter()
        .any(|t| t == "Catalog reset; ingest resumed"));
}

#[test]
fn reset_state_after_a_lost_job_save_does_not_repeat_the_watermark_cas() {
    let (platform, _) = reset_ready();
    platform
        .adapter
        .fail_next("KV_CAS", admin::reset::JOB_KEY, false);
    assert_eq!(
        fence_try(&platform).unwrap().unwrap_err(),
        "injected_failure"
    );
    let job = platform.adapter.records()[admin::reset::JOB_KEY]
        .value
        .clone();
    assert_eq!(job["status"], "resetting", "{job}");
    assert!(job["reset_cas_at"].is_null());
    let watermark = snapshot(&platform, "ingest:watermark");
    assert_eq!(watermark.1["catalog"]["generation"], 5);
    advance_clock(&platform, 2);
    let job = reset_tick(&platform);
    assert_eq!(job["status"], "cleanup", "{job}");
    assert_eq!(snapshot(&platform, "ingest:watermark"), watermark);
    // The watermark's `at` moves on with commits once ingest resumes: the fence's end bounds it.
    assert_eq!(job["reset_cas_at"], job["resetting_at"]);
    assert!(job_u64(&job, "reset_cas_at") <= watermark.1["at"].as_u64().unwrap());
    assert_eq!(
        job["retirement_grace_until"],
        watermark.1["at"].as_u64().unwrap()
            + fastly_log_analytics::ingest::compaction::DELETE_GRACE_S
    );
    assert_eq!(job["reset_manifest"], watermark.1["catalog"]["manifest"]);
}

#[test]
fn cancel_after_reset_cas_is_refused() {
    let (platform, _) = reset_ready();
    assert_eq!(reset_tick(&platform)["status"], "cleanup");
    let response = platform.call("POST", &format!("{DELETE_DATA}/cancel"), json!({}));
    assert_eq!(response.status, 409);
    assert_eq!(
        serde_json::from_slice::<Value>(&response.body).unwrap()["error"],
        "reset_committed"
    );
    assert!(platform.service()["service"]["data_reset"].is_object());
}

#[test]
fn cancel_while_fencing_clears_marker_and_changes_nothing() {
    use fastly_log_analytics::{
        auth::Role,
        state::{MemoryStore, StateStore},
    };
    let (platform, _) = reset_seeded();
    let before: Vec<_> = ["ingest:watermark", "ingest:rum:watermark", "ingest:plan"]
        .map(|key| snapshot(&platform, key))
        .into();
    let store = MemoryStore::default();
    let response = shell_post(
        &platform,
        Role::Admin,
        &store,
        &format!("{DELETE_DATA}/cancel"),
        json!({}),
    );
    assert_eq!(
        response.status,
        200,
        "{}",
        String::from_utf8_lossy(&response.body)
    );
    let cancelled: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(cancelled["job"]["status"], "cancelled");
    assert!(platform.service()["service"].get("data_reset").is_none());
    let after: Vec<_> = ["ingest:watermark", "ingest:rum:watermark", "ingest:plan"]
        .map(|key| snapshot(&platform, key))
        .into();
    assert_eq!(after, before);
    assert!(
        fence_try(&platform).is_none(),
        "no step runs without a marker"
    );
    let state = store.load().unwrap().state;
    assert!(state
        .recent_audit
        .iter()
        .chain(&state.audit)
        .any(|entry| entry.action == "delete_data_cancelled" && entry.target == "fixtureService"));
}

#[test]
fn a_persistent_purge_failure_after_the_reset_cas_enters_cleanup_at_the_bound() {
    let (platform, _) = reset_ready();
    for _ in 0..20 {
        platform.api_faults.lock().unwrap().push(ApiFault {
            method: "POST",
            path: "/service/fixtureFront/purge/flp-fos-fixtureService",
            name: None,
            status: 403,
            retry_after: None,
        });
    }
    let error = fence_try(&platform).unwrap().unwrap_err();
    assert_eq!(error, "edge_purge_failed: purge_status_403");
    let job = platform.adapter.records()[admin::reset::JOB_KEY]
        .value
        .clone();
    assert_eq!(job["status"], "resetting", "{job}");
    assert!(job["reset_cas_at"].is_u64());
    let watermark = snapshot(&platform, "ingest:watermark");
    advance_clock(&platform, admin::reset::RESET_GIVE_UP_S - 2);
    assert!(fence_try(&platform).unwrap().is_err());
    assert_eq!(
        platform.adapter.records()[admin::reset::JOB_KEY].value["status"],
        "resetting"
    );
    advance_clock(&platform, 2);
    let job = fence_tick(&platform);
    assert_eq!(job["status"], "cleanup", "{job}");
    assert_eq!(job["error"], "edge_purge_failed: purge_status_403");
    assert_eq!(job["purge_pending"], true);
    assert!(
        messages(&job)
            .iter()
            .any(|t| t
                == "Edge purge failed: purge_status_403; continuing, will retry during cleanup")
    );
    assert_eq!(snapshot(&platform, "ingest:watermark"), watermark);
    let service: admin::Service = serde_json::from_value(
        platform.adapter.records()["svc:fixtureService"]
            .value
            .clone(),
    )
    .unwrap();
    assert_eq!(
        service.data_reset.as_ref().unwrap().phase,
        admin::ResetPhase::Cleanup
    );
    assert!(!admin::reset::fencing(&service), "ingest dispatch resumes");
}

#[test]
fn a_corrupt_head_fails_the_reset_at_its_bound_and_resumes_ingest_unchanged() {
    let (platform, _) = reset_ready();
    let (_, mut corrupt) = snapshot(&platform, "ingest:watermark");
    corrupt["catalog"]["version"] = 99.into();
    seed(&platform, "ingest:watermark", corrupt);
    let watermark = snapshot(&platform, "ingest:watermark");
    let error = fence_try(&platform).unwrap().unwrap_err();
    assert_eq!(error, "catalog_version_unsupported");
    assert_eq!(
        platform.adapter.records()[admin::reset::JOB_KEY].value["status"],
        "resetting"
    );
    let cancel = platform.call("POST", &format!("{DELETE_DATA}/cancel"), json!({}));
    assert_eq!(
        cancel.status, 409,
        "no cancel while the reset may race its CAS"
    );
    advance_clock(&platform, admin::reset::RESET_GIVE_UP_S);
    let job = fence_tick(&platform);
    assert_eq!(job["status"], "failed", "{job}");
    assert_eq!(job["error"], "reset_timeout: catalog_version_unsupported");
    assert!(job["reset_cas_at"].is_null());
    assert!(last_message(&job).contains("ingest resumes"), "{job}");
    assert!(platform.service()["service"].get("data_reset").is_none());
    assert_eq!(snapshot(&platform, "ingest:watermark"), watermark);
}

#[test]
fn a_missing_purge_token_is_recorded_before_the_reset_cas() {
    let (platform, service) = reset_ready();
    platform
        .adapter
        .override_secret_reads(&service.fastly_token_ref, Ok(None));
    let job = reset_tick(&platform);
    platform.adapter.clear_secret_reads();
    assert_eq!(job["status"], "cleanup", "{job}");
    assert_eq!(job["error"], "fastly_token_missing");
    let texts = messages(&job);
    assert!(
        texts
            .iter()
            .any(|t| t == "Edge cache purge will be skipped: fastly_token_missing"),
        "{texts:?}"
    );
    assert_eq!(
        platform.service()["service"]["data_reset"]["phase"],
        "cleanup"
    );
}

const BUCKET: &str = "fos-fixture-logs";
const CLEANUP_PREFIXES: [&str; 10] = [
    "raw/request/",
    "raw/rum/",
    "processed/request/",
    "processed/rum/",
    "processed/summary/request/",
    "processed/summary_bundle/request/",
    "processed/_catalog/",
    "processed/_sources/",
    "processed/_inflight/",
    "processed/_compact_work/",
];

fn rich_fos() {
    RICH_FOS.with(|on| on.set(true));
    MTIMES.with(|m| m.borrow_mut().clear());
}

/// A job in cleanup right after its reset, with FOS objects listed by LastModified.
fn cleanup_ready() -> (Platform, admin::Service, Value) {
    rich_fos();
    let (platform, service) = reset_ready();
    let job = reset_tick(&platform);
    assert_eq!(job["status"], "cleanup", "{job}");
    (platform, service, job)
}

fn seed_at(platform: &Platform, key: &str, at: u64) {
    platform.adapter.seed_object(BUCKET, key);
    MTIMES.with(|m| m.borrow_mut().insert(key.into(), at));
}

fn bucket_keys(platform: &Platform) -> BTreeSet<String> {
    platform.adapter.object_keys(BUCKET).into_iter().collect()
}

fn raw_key(service: &admin::Service, stream: &str, minute: u64, name: &str) -> String {
    format!(
        "{}{}{name}.zst",
        fastly_log_analytics::ingest::raw_prefix(service)
            .replace("raw/request/", &format!("raw/{stream}/")),
        fastly_log_analytics::ingest::minute_dir(minute as i64)
    )
}

fn job_u64(job: &Value, field: &str) -> u64 {
    job[field]
        .as_u64()
        .unwrap_or_else(|| panic!("{field}: {job}"))
}

/// Steps cleanup a minute apart, as maintenance ticks would, until the job completes.
fn run_cleanup(platform: &Platform) -> Value {
    for _ in 0..60 {
        let job = fence_tick(platform);
        if job["status"] == "complete" {
            return job;
        }
        assert_eq!(job["status"], "cleanup", "{job}");
        advance_clock(platform, 60);
    }
    panic!("cleanup did not finish");
}

#[test]
fn cleanup_deletes_pre_reset_objects_and_keeps_new_ones() {
    let (platform, service, job) = cleanup_ready();
    let (cas_at, reset_at) = (job_u64(&job, "reset_cas_at"), job_u64(&job, "reset_at"));
    let scope = fastly_log_analytics::ingest::scope(&service);
    let (mut old, mut new) = (BTreeSet::new(), BTreeSet::new());
    for prefix in CLEANUP_PREFIXES
        .iter()
        .filter(|p| p.starts_with("processed/"))
    {
        let gone = format!("{scope}{prefix}x/old.parquet");
        seed_at(&platform, &gone, cas_at - 30);
        old.insert(gone);
        let kept = format!("{scope}{prefix}x/new.parquet");
        seed_at(&platform, &kept, cas_at + 30);
        new.insert(kept);
    }
    for stream in ["request", "rum"] {
        let gone = raw_key(&service, stream, reset_at - 120, "old");
        seed_at(&platform, &gone, cas_at + 30);
        old.insert(gone);
        let kept = raw_key(&service, stream, reset_at, "new");
        seed_at(&platform, &kept, cas_at - 30);
        new.insert(kept);
    }
    let job = run_cleanup(&platform);
    let left = bucket_keys(&platform);
    for key in &old {
        assert!(!left.contains(key), "{key} survived");
    }
    for key in &new {
        assert!(left.contains(key), "{key} was deleted");
    }
    let texts = messages(&job);
    for prefix in CLEANUP_PREFIXES {
        assert_eq!(job["deleted"][prefix], 1, "{prefix}: {job}");
        let text = format!("Deleted 1 objects under {prefix}");
        assert!(texts.contains(&text), "{text}: {texts:?}");
    }
    assert_eq!(job["bytes"], old.len());
    assert_eq!(job["pending"], 0);
}

#[test]
fn cleanup_keeps_object_modified_exactly_at_reset_cas_time() {
    let (platform, service, job) = cleanup_ready();
    let cas_at = job_u64(&job, "reset_cas_at");
    let scope = fastly_log_analytics::ingest::scope(&service);
    let equal = format!("{scope}processed/request/x/equal.parquet");
    let before = format!("{scope}processed/request/x/before.parquet");
    seed_at(&platform, &equal, cas_at);
    seed_at(&platform, &before, cas_at - 1);
    run_cleanup(&platform);
    let left = bucket_keys(&platform);
    assert!(left.contains(&equal));
    assert!(!left.contains(&before));
}

#[test]
fn cleanup_never_deletes_objects_the_new_head_references() {
    let (platform, service, job) = cleanup_ready();
    let cas_at = job_u64(&job, "reset_cas_at");
    let scope = fastly_log_analytics::ingest::scope(&service);
    let manifest = job["reset_manifest"].as_str().unwrap().to_owned();
    // Written before the CAS, in the reset's own execution, so its LastModified is earlier.
    MTIMES.with(|m| m.borrow_mut().insert(manifest.clone(), cas_at - 5));
    let day = format!("{scope}processed/_catalog/day-new.json");
    let bundle =
        format!("{scope}processed/summary_bundle/request/year=2027/hour_bundle=08.parquet");
    let rum_file = format!("{scope}processed/rum/x/rum-new.parquet");
    let (_, mut watermark) = snapshot(&platform, "ingest:watermark");
    watermark["catalog"]["days"] = json!({"1800000000": day});
    watermark["catalog"]["hour_bundles"] = json!({"1800000000": {"key": bundle, "generation": 6}});
    seed(&platform, "ingest:watermark", watermark);
    let (_, mut rum) = snapshot(&platform, "ingest:rum:watermark");
    rum["catalog"] = json!({"version": 2, "generation": 1, "manifest": rum_file,
        "recent": [{"cursor": "fixtureService:1", "generation": 1, "committed_at": null}]});
    seed(&platform, "ingest:rum:watermark", rum);
    // Clock skew between the CAS and FOS: referenced objects that look older still stay.
    for key in [&day, &bundle, &rum_file] {
        seed_at(&platform, key, cas_at - 5);
    }
    let fresh = format!("{scope}processed/request/x/new-ingest.parquet");
    seed_at(&platform, &fresh, cas_at + 200);
    let stale = format!("{scope}processed/_catalog/old-manifest.json");
    seed_at(&platform, &stale, cas_at - 5);
    run_cleanup(&platform);
    let left = bucket_keys(&platform);
    for key in [&manifest, &day, &bundle, &rum_file, &fresh] {
        assert!(left.contains(key), "{key} was deleted");
    }
    assert!(!left.contains(&stale));
}

#[test]
fn cleanup_never_lists_or_deletes_ops_logs() {
    let (platform, service, job) = cleanup_ready();
    let cas_at = job_u64(&job, "reset_cas_at");
    let ops = format!(
        "{}processed/ops/",
        fastly_log_analytics::ingest::scope(&service)
    );
    let log = format!("{ops}cron_runs/year=2027/month=01/day=15/hour=07/part.parquet");
    seed_at(&platform, &log, cas_at - 100);
    let parquet = format!(
        "{}processed/request/x/old.parquet",
        fastly_log_analytics::ingest::scope(&service)
    );
    seed_at(&platform, &parquet, cas_at - 100);
    FOS_LOG.with(|log| log.borrow_mut().clear());
    run_cleanup(&platform);
    let left = bucket_keys(&platform);
    assert!(left.contains(&log));
    assert!(!left.contains(&parquet));
    let requests = FOS_LOG.with(|log| log.borrow().clone());
    assert!(requests.iter().any(|(m, _, _)| m == "POST"), "{requests:?}");
    for (method, key, query) in requests {
        assert!(!key.starts_with(&ops), "{method} {key}");
        let prefix = url::form_urlencoded::parse(query.as_bytes())
            .find(|(k, _)| k == "prefix")
            .map(|(_, v)| v.into_owned());
        if let Some(prefix) = prefix {
            assert!(
                !prefix.starts_with(&ops) && !ops.starts_with(&prefix),
                "LIST {prefix} covers ops logs"
            );
        }
    }
}

#[test]
fn cleanup_waits_for_grace_and_raw_settle_and_rechecks_raw_once() {
    use fastly_log_analytics::ingest::compaction::DELETE_GRACE_S;
    rich_fos();
    let (platform, service) = reset_ready();
    let (_, mut svc) = snapshot(&platform, "svc:fixtureService");
    svc["log_period"] = 600.into();
    seed(&platform, "svc:fixtureService", svc);
    let settle = fastly_log_analytics::ingest::settle_s(600);
    let job = reset_tick(&platform);
    assert_eq!(job["status"], "cleanup", "{job}");
    let (cas_at, reset_at) = (job_u64(&job, "reset_cas_at"), job_u64(&job, "reset_at"));
    assert!(reset_at + settle > cas_at && reset_at + settle < cas_at + DELETE_GRACE_S);
    let scope = fastly_log_analytics::ingest::scope(&service);
    let processed = format!("{scope}processed/request/x/old.parquet");
    seed_at(&platform, &processed, cas_at - 30);
    let raw = raw_key(&service, "request", reset_at - 60, "old");
    seed_at(&platform, &raw, cas_at - 30);
    let at = |t: u64| platform.clock.store(t, Ordering::SeqCst);

    at(reset_at + settle - 1);
    fence_tick(&platform);
    let left = bucket_keys(&platform);
    assert!(left.contains(&raw), "raw waits for T + settle");
    assert!(left.contains(&processed));

    at(reset_at + settle);
    let job = fence_tick(&platform);
    let left = bucket_keys(&platform);
    assert!(!left.contains(&raw));
    assert!(left.contains(&processed), "processed waits for the grace");
    assert_eq!(job["raw_recheck_at"], reset_at + 2 * settle);
    let late = raw_key(&service, "request", reset_at - 60, "late");
    seed_at(&platform, &late, reset_at + settle + 1);

    at(cas_at + DELETE_GRACE_S - 1);
    fence_tick(&platform);
    assert!(bucket_keys(&platform).contains(&processed));

    at(cas_at + DELETE_GRACE_S);
    fence_tick(&platform);
    let left = bucket_keys(&platform);
    assert!(!left.contains(&processed));
    assert!(
        left.contains(&late),
        "the recheck waits another settle interval"
    );

    at(reset_at + 2 * settle);
    let job = fence_tick(&platform);
    assert!(!bucket_keys(&platform).contains(&late), "{job}");
    assert_eq!(job["deleted"]["raw/request/"], 2);
    let later = raw_key(&service, "request", reset_at - 60, "later");
    seed_at(&platform, &later, reset_at + 2 * settle + 1);
    let job = run_cleanup(&platform);
    assert!(
        bucket_keys(&platform).contains(&later),
        "exactly one recheck"
    );
    assert_eq!(job["deleted"]["raw/request/"], 2);
}

#[test]
fn finish_reconciles_storage_clears_marker_and_audits() {
    let (platform, service, job) = cleanup_ready();
    let cas_at = job_u64(&job, "reset_cas_at");
    let scope = fastly_log_analytics::ingest::scope(&service);
    seed_at(
        &platform,
        &format!("{scope}processed/request/x/old.parquet"),
        cas_at - 1,
    );
    CACHE_PURGES.with(|purges| purges.borrow_mut().clear());
    platform.adapter.clear_calls();
    let job = run_cleanup(&platform);
    assert_eq!(job["status"], "complete");
    let records = platform.adapter.records();
    assert!(records.contains_key("storage:reconcile"));
    let svc = &records["svc:fixtureService"].value;
    assert!(svc.get("data_reset").is_none(), "{svc}");
    let audit = svc["_audit_outbox"].as_array().unwrap().last().unwrap();
    assert_eq!(audit["event"], "delete_data_completed");
    assert_eq!(audit["job_id"], job["job_id"]);
    assert_eq!(audit["deleted"]["processed/request/"], 1);
    assert_eq!(audit["bytes"], 1);
    assert!(
        last_message(&job).starts_with("Delete Data complete"),
        "{job}"
    );
    assert!(platform.adapter.calls().contains(&(
        "POST".into(),
        "/service/fixtureFront/purge/flp-fos-fixtureService".into()
    )));
    assert!(!CACHE_PURGES.with(|purges| purges.borrow().is_empty()));
    let service: admin::Service = serde_json::from_value(svc.clone()).unwrap();
    assert!(service.data_reset.is_none());
    assert!(fence_try(&platform).is_none(), "nothing runs once complete");
}

/// The reset's KV clears that cleanup finishes when the reset step may not have.
const LEFTOVER_KEYS: [&str; 5] = [
    "ingest:rum:plan",
    "ingest:backfill",
    "ingest:deleteq:p1",
    "ingest:deleteq:p2",
    "ingest:quarantine:raw/a.zst",
];

/// A reset whose watermark CAS landed but whose job save was lost.
fn reset_cas_with_lost_job_save() -> (Platform, admin::Service, Value) {
    rich_fos();
    let (platform, service) = reset_ready();
    platform
        .adapter
        .fail_next("KV_CAS", admin::reset::JOB_KEY, false);
    assert_eq!(
        fence_try(&platform).unwrap().unwrap_err(),
        "injected_failure"
    );
    let job = platform.adapter.records()[admin::reset::JOB_KEY]
        .value
        .clone();
    assert_eq!(job["status"], "resetting", "{job}");
    assert!(job["reset_cas_at"].is_null());
    let (_, watermark) = snapshot(&platform, "ingest:watermark");
    (platform, service, watermark)
}

#[test]
fn a_reset_whose_cas_landed_is_never_failed_at_its_bound_and_cleanup_finishes_it() {
    let (platform, service, watermark) = reset_cas_with_lost_job_save();
    let reset_at = service.data_reset.as_ref().unwrap().reset_at;
    // The lost save also lost the record of the reset steps after the CAS: put them back undone.
    seed(
        &platform,
        "ingest:rum:watermark",
        json!({"run_id": "rum-committed", "key": "raw/rum/old", "at": 1, "manifest_gen": 2}),
    );
    for key in LEFTOVER_KEYS.into_iter().chain(PRE_RESET_STATE) {
        seed(
            &platform,
            key,
            json!({"plan_id": "committed", "lease_until": 0}),
        );
    }
    platform.adapter.override_secret_reads(
        &service.fastly_token_ref,
        Err("secret_store_unavailable".into()),
    );
    advance_clock(&platform, admin::reset::RESET_GIVE_UP_S);
    let job = fence_tick(&platform);
    platform.adapter.clear_secret_reads();
    assert_eq!(job["status"], "cleanup", "{job}");
    assert_eq!(job["reset_cas_at"], job["resetting_at"]);
    assert_eq!(
        job["retirement_grace_until"],
        watermark["at"].as_u64().unwrap()
            + fastly_log_analytics::ingest::compaction::DELETE_GRACE_S
    );
    assert_eq!(job["reset_manifest"], watermark["catalog"]["manifest"]);
    assert_eq!(job["purge_pending"], true);
    assert_eq!(snapshot(&platform, "ingest:watermark").1, watermark);
    assert_eq!(
        platform.service()["service"]["data_reset"]["phase"],
        "cleanup"
    );
    let job = run_cleanup(&platform);
    assert_eq!(job["purge_pending"], false);
    assert!(messages(&job).iter().any(|t| t == "Edge cache purged"));
    let records = platform.adapter.records();
    assert_eq!(
        records["ingest:rum:watermark"].value["floor_key"],
        floor(&service, "rum", reset_at)
    );
    for key in LEFTOVER_KEYS {
        assert!(!records.contains_key(key), "{key}");
    }
    // Stage state is cleared before the CAS; what exists after it is new and stays.
    for key in PRE_RESET_STATE {
        assert!(records.contains_key(key), "{key}");
    }
    assert!(platform.service()["service"].get("data_reset").is_none());
}

#[test]
fn a_failed_job_whose_reset_cas_landed_resumes_cleanup_and_finishes() {
    let (platform, _, watermark) = reset_cas_with_lost_job_save();
    let (_, mut job) = snapshot(&platform, admin::reset::JOB_KEY);
    job["status"] = "failed".into();
    job["error"] = "reset_timeout".into();
    seed(&platform, admin::reset::JOB_KEY, job);
    advance_clock(&platform, 2);
    let job = fence_tick(&platform);
    assert_eq!(job["status"], "cleanup", "{job}");
    assert_eq!(job["reset_cas_at"], job["resetting_at"]);
    assert_eq!(
        job["retirement_grace_until"],
        watermark["at"].as_u64().unwrap()
            + fastly_log_analytics::ingest::compaction::DELETE_GRACE_S
    );
    // The lost save had already moved the marker to cleanup, so cleanup retried the purge.
    assert_eq!(job["purge_pending"], false);
    let texts = messages(&job);
    let adopted = texts
        .iter()
        .position(|t| t == "The catalog reset had landed; continuing with cleanup.")
        .expect("adopted");
    assert!(
        texts[adopted..].iter().any(|t| t == "Edge cache purged"),
        "{texts:?}"
    );
    assert_eq!(
        platform.service()["service"]["data_reset"]["phase"],
        "cleanup"
    );
    let job = run_cleanup(&platform);
    assert_eq!(job["status"], "complete");
    assert!(platform.service()["service"].get("data_reset").is_none());
    assert_eq!(snapshot(&platform, "ingest:watermark").1, watermark);
}

#[test]
fn a_post_cas_bound_holds_rum_ingest_until_its_cursor_reset_lands() {
    let (platform, _) = reset_ready();
    seed(
        &platform,
        "ingest:rum:plan",
        json!({"version": 3, "plan_id": "r-old", "lease_until": 0}),
    );
    // The RUM plan clear after the reset CAS keeps failing, so the RUM reset never lands.
    FAIL_KV_DELETE_KEY.with(|key| *key.borrow_mut() = Some("ingest:rum:plan".into()));
    assert_eq!(
        fence_try(&platform).unwrap().unwrap_err(),
        "kv_delete_failed"
    );
    let job = platform.adapter.records()[admin::reset::JOB_KEY]
        .value
        .clone();
    assert_eq!(job["status"], "resetting", "{job}");
    let cas_at = job_u64(&job, "reset_cas_at");
    advance_clock(&platform, admin::reset::RESET_GIVE_UP_S);
    let job = fence_tick(&platform);
    FAIL_KV_DELETE_KEY.with(|key| key.borrow_mut().take());
    assert_eq!(job["status"], "cleanup", "{job}");
    let load = || -> admin::Service {
        serde_json::from_value(
            platform.adapter.records()["svc:fixtureService"]
                .value
                .clone(),
        )
        .unwrap()
    };
    let service = load();
    assert!(!admin::reset::fencing(&service), "request ingest resumes");
    assert!(admin::reset::rum_held(&service), "RUM ingest stays held");
    assert!(platform.adapter.records().contains_key("ingest:rum:plan"));
    // A backfill a user starts once request ingest resumed is new work, not pre-reset state.
    seed(
        &platform,
        "ingest:backfill",
        json!({"status": "running", "actor": "a", "started_at": cas_at + 100, "updated_at": cas_at + 100}),
    );
    advance_clock(&platform, 2);
    let job = fence_tick(&platform);
    let service = load();
    assert!(!admin::reset::rum_held(&service), "{job}");
    assert!(service.data_reset.is_some());
    let records = platform.adapter.records();
    assert!(!records.contains_key("ingest:rum:plan"));
    assert!(!records.contains_key("ingest:compact_state"));
    assert!(records.contains_key("ingest:backfill"), "{job}");
    assert!(messages(&job).iter().any(|t| t == "RUM ingest resumed"));
}

#[test]
fn a_purge_that_keeps_failing_gives_up_and_is_recorded() {
    let (platform, _, _) = cleanup_ready();
    for _ in 0..3 {
        platform.api_faults.lock().unwrap().push(ApiFault {
            method: "POST",
            path: "/service/fixtureFront/purge/flp-fos-fixtureService",
            name: None,
            status: 503,
            retry_after: None,
        });
    }
    let job = run_cleanup(&platform);
    assert_eq!(
        job["error"],
        "edge_purge_failed: purge_status_503; gave up after 3 tries"
    );
    assert!(messages(&job)
        .iter()
        .any(|t| t == "Edge purge gave up after 3 tries; cached objects expire within 24h"));
}

/// The post-CAS bound resumes request ingest with RUM held, and that step's job save is lost.
fn held_resume_with_lost_job_save() -> (Platform, Value) {
    let (platform, _) = reset_ready();
    seed(
        &platform,
        "ingest:rum:plan",
        json!({"version": 3, "plan_id": "r-old", "lease_until": 0}),
    );
    FAIL_KV_DELETE_KEY.with(|key| *key.borrow_mut() = Some("ingest:rum:plan".into()));
    assert_eq!(
        fence_try(&platform).unwrap().unwrap_err(),
        "kv_delete_failed"
    );
    let job = snapshot(&platform, admin::reset::JOB_KEY).1;
    advance_clock(&platform, admin::reset::RESET_GIVE_UP_S);
    platform
        .adapter
        .fail_next("KV_CAS", admin::reset::JOB_KEY, false);
    assert_eq!(
        fence_try(&platform).unwrap().unwrap_err(),
        "injected_failure"
    );
    FAIL_KV_DELETE_KEY.with(|key| key.borrow_mut().take());
    let marker = &platform.service()["service"]["data_reset"];
    assert_eq!(marker["phase"], "cleanup");
    assert_eq!(marker["rum_held"], true);
    assert_eq!(
        snapshot(&platform, admin::reset::JOB_KEY).1["status"],
        "resetting"
    );
    (platform, job)
}

fn held(platform: &Platform) -> bool {
    let service: admin::Service = serde_json::from_value(
        platform.adapter.records()["svc:fixtureService"]
            .value
            .clone(),
    )
    .unwrap();
    admin::reset::rum_held(&service)
}

#[test]
fn a_lost_save_after_a_held_resume_still_lifts_the_rum_hold() {
    let (platform, _) = held_resume_with_lost_job_save();
    // The re-run lands the RUM reset, then finds the stored marker already in cleanup and held.
    for _ in 0..3 {
        advance_clock(&platform, 2);
        fence_tick(&platform);
    }
    assert!(!held(&platform), "RUM ingest resumes once its reset lands");
    assert!(!platform.adapter.records().contains_key("ingest:rum:plan"));
}

#[test]
fn a_reset_rerun_after_ingest_resumed_keeps_new_plans_and_backfills() {
    let (platform, job) = held_resume_with_lost_job_save();
    let cas_at = job_u64(&job, "reset_cas_at");
    // Started by resumed request ingest and by a user after the reset.
    let plan =
        json!({"version": 3, "plan_id": "p-new", "created_at": cas_at + 100, "lease_until": 0});
    seed(&platform, "ingest:plan", plan.clone());
    let backfill = json!({"status": "running", "actor": "a", "started_at": cas_at + 100,
        "updated_at": cas_at + 100});
    seed(&platform, "ingest:backfill", backfill.clone());
    for _ in 0..3 {
        advance_clock(&platform, 2);
        fence_tick(&platform);
    }
    let records = platform.adapter.records();
    assert_eq!(records["ingest:plan"].value, plan);
    assert_eq!(records["ingest:backfill"].value, backfill);
}

fn marker(platform: &Platform) -> Value {
    platform.adapter.records()["svc:fixtureService"].value["data_reset"].clone()
}

fn last_audit(platform: &Platform) -> Value {
    platform.adapter.records()["svc:fixtureService"].value["_audit_outbox"]
        .as_array()
        .unwrap()
        .last()
        .unwrap()
        .clone()
}

#[test]
fn a_start_whose_service_save_landed_but_failed_clears_its_stranded_marker() {
    let platform = provisioned();
    platform
        .adapter
        .fail_next("KV_CAS", "svc:fixtureService", true);
    let response = platform.call("POST", DELETE_DATA, delete_data_body(&platform));
    assert_eq!(response.status, 503);
    let job = snapshot(&platform, admin::reset::JOB_KEY).1;
    assert_eq!(job["status"], "cancelled", "{job}");
    assert_eq!(marker(&platform)["phase"], "fencing", "the save landed");
    assert_eq!(fence_try(&platform), Some(Ok(())));
    assert!(marker(&platform).is_null(), "ingest resumes");
    assert_eq!(last_audit(&platform)["event"], "data_reset.cancelled");
    assert!(fence_try(&platform).is_none());
    let restarted = platform.json("POST", DELETE_DATA, delete_data_body(&platform));
    assert_eq!(restarted["job"]["status"], "fencing");
}

#[test]
fn a_cancel_whose_service_save_failed_clears_its_stranded_marker_on_the_next_tick() {
    let platform = provisioned();
    platform.json("POST", DELETE_DATA, delete_data_body(&platform));
    platform
        .adapter
        .fail_next("KV_CAS", "svc:fixtureService", false);
    let cancel = format!("{DELETE_DATA}/cancel");
    assert_eq!(platform.call("POST", &cancel, json!({})).status, 503);
    assert_eq!(
        snapshot(&platform, admin::reset::JOB_KEY).1["status"],
        "cancelled"
    );
    assert_eq!(marker(&platform)["phase"], "fencing");
    assert_eq!(fence_try(&platform), Some(Ok(())));
    assert!(marker(&platform).is_null(), "ingest resumes");
    assert_eq!(last_audit(&platform)["event"], "data_reset.cancelled");
}

#[test]
fn a_non_terminal_job_no_marker_names_is_stale_after_the_fence_bound() {
    let platform = provisioned();
    // The job CAS landed, the service save failed and so did the rollback.
    let started_at = platform.now();
    seed(
        &platform,
        admin::reset::JOB_KEY,
        json!({"job_id": "lost", "status": "fencing", "reset_at": started_at - started_at % 60,
            "started_at": started_at}),
    );
    advance_clock(&platform, admin::reset::FENCE_GIVE_UP_S);
    let response = platform.call("POST", DELETE_DATA, delete_data_body(&platform));
    assert_eq!(response.status, 409, "still within the fence bound");
    advance_clock(&platform, 1);
    let started = platform.json("POST", DELETE_DATA, delete_data_body(&platform));
    assert_eq!(started["job"]["status"], "fencing");
    assert_ne!(started["job"]["job_id"], "lost");
}

#[test]
fn a_full_audit_outbox_does_not_block_the_marker_clear() {
    let (platform, _) = fence_started();
    let (_, mut svc) = snapshot(&platform, "svc:fixtureService");
    let entry = json!({"actor": "a", "at": 1, "event": "e", "config_version": 1});
    svc["_audit_outbox"] = Value::Array(vec![entry; 4096]);
    seed(&platform, "svc:fixtureService", svc);
    let held = platform.now() + 10 * admin::reset::FENCE_GIVE_UP_S;
    seed(
        &platform,
        "ingest:plan",
        json!({"version": 3, "plan_id": "p1", "lease_until": held}),
    );
    advance_clock(&platform, admin::reset::FENCE_GIVE_UP_S);
    let job = fence_tick(&platform);
    assert_eq!(job["status"], "failed", "{job}");
    let svc = snapshot(&platform, "svc:fixtureService").1;
    assert!(svc["data_reset"].is_null(), "ingest resumes");
    assert_eq!(svc["_audit_outbox"].as_array().unwrap().len(), 4096);
}

const PRE_RESET_STATE: [&str; 6] = [
    "ingest:compact_state",
    "summary:bundle_state",
    "retention:request",
    "retention:rum",
    "retention:summary:request",
    "retention:summary_bundle:request",
];

#[test]
fn a_persistent_rum_conflict_after_the_cas_never_keeps_pre_reset_bundle_state() {
    let (platform, _) = reset_ready();
    seed(
        &platform,
        "ingest:compact_state",
        json!({"plan_id": "committed", "lease_until": 0}),
    );
    CONFLICT_CAS.with(|k| *k.borrow_mut() = Some("ingest:rum:watermark".into()));
    let result = fence_try(&platform).unwrap();
    assert_eq!(result, Err("rum_watermark_conflict".into()));
    let job = snapshot(&platform, admin::reset::JOB_KEY).1;
    let cas_at = job_u64(&job, "reset_cas_at");
    let records = platform.adapter.records();
    for key in PRE_RESET_STATE {
        assert!(!records.contains_key(key), "{key} outlived the reset CAS");
    }
    advance_clock(&platform, admin::reset::RESET_GIVE_UP_S);
    fence_try(&platform).unwrap().unwrap();
    assert_eq!(
        snapshot(&platform, admin::reset::JOB_KEY).1["status"],
        "cleanup"
    );
    assert_eq!(marker(&platform)["rum_held"], true);
    // Written by the stages that resumed with request ingest.
    let new_bundle = json!({"built_after": cas_at + 100});
    seed(&platform, "summary:bundle_state", new_bundle.clone());
    let new_queue = json!({"run_id": "p-new", "keys": [], "at": cas_at + 100});
    seed(&platform, "ingest:deleteq:p-new", new_queue.clone());
    for _ in 0..3 {
        advance_clock(&platform, 60);
        assert_eq!(
            fence_try(&platform).unwrap(),
            Err("rum_watermark_conflict".into())
        );
    }
    CONFLICT_CAS.with(|k| k.borrow_mut().take());
    let records = platform.adapter.records();
    for key in [
        "ingest:deleteq:p1",
        "ingest:deleteq:p2",
        "ingest:quarantine:raw/a.zst",
    ] {
        assert!(!records.contains_key(key), "{key}: pre-reset queue kept");
    }
    assert_eq!(records["ingest:deleteq:p-new"].value, new_queue);
    assert_eq!(records["summary:bundle_state"].value, new_bundle);
    let head = &records["ingest:watermark"].value["catalog"];
    assert_eq!(head["hour_bundles"], json!({}));
    assert_eq!(head["days"], json!({}));
}

#[test]
fn the_first_cleanup_tick_removes_pre_reset_rum_files_without_the_catalog_grace() {
    let (platform, service, job) = cleanup_ready();
    let cas_at = job_u64(&job, "reset_cas_at");
    let scope = fastly_log_analytics::ingest::scope(&service);
    let old = format!("{scope}processed/rum/x/old.parquet");
    let new = format!("{scope}processed/rum/x/new.parquet");
    let request = format!("{scope}processed/request/x/old.parquet");
    seed_at(&platform, &old, cas_at - 30);
    seed_at(&platform, &new, cas_at + 1);
    seed_at(&platform, &request, cas_at - 30);
    advance_clock(&platform, 2);
    let job = fence_tick(&platform);
    let left = bucket_keys(&platform);
    // RUM queries LIST processed/rum/ directly; no catalog snapshot reader needs the grace.
    assert!(!left.contains(&old), "{job}");
    assert!(left.contains(&new));
    assert!(left.contains(&request), "catalog files wait for the grace");
    assert_eq!(job["deleted"]["processed/rum/"], 1);
}

#[test]
fn cleanup_finishes_past_more_kept_queue_records_than_one_step_can_read() {
    let (platform, service, job) = cleanup_ready();
    let (cas_at, reset_at) = (job_u64(&job, "reset_cas_at"), job_u64(&job, "reset_at"));
    // The post-CAS bound left the queues uncleared, and resumed ingest wrote new records.
    let (_, mut stored) = snapshot(&platform, admin::reset::JOB_KEY);
    let steps: Vec<Value> = stored["steps_done"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|step| !matches!(step.as_str(), Some("deleteq" | "quarantine")))
        .cloned()
        .collect();
    stored["steps_done"] = steps.into();
    seed(&platform, admin::reset::JOB_KEY, stored);
    let kept: Vec<String> = (0..40)
        .map(|i| format!("ingest:deleteq:new-{i:02}"))
        .chain((0..10).map(|i| format!("ingest:quarantine:new-{i:02}")))
        .collect();
    for key in &kept {
        seed(
            &platform,
            key,
            json!({"run_id": key, "keys": [], "at": cas_at + 100}),
        );
    }
    seed(
        &platform,
        "ingest:deleteq:old",
        json!({"run_id": "old", "keys": [], "at": cas_at - 100}),
    );
    let scope = fastly_log_analytics::ingest::scope(&service);
    let old = [
        format!("{scope}processed/rum/x/old.parquet"),
        format!("{scope}processed/request/x/old.parquet"),
        raw_key(&service, "request", reset_at - 120, "old"),
    ];
    for key in &old {
        seed_at(&platform, key, cas_at - 30);
    }
    let job = run_cleanup(&platform);
    assert_eq!(job["status"], "complete");
    let records = platform.adapter.records();
    for key in &kept {
        assert!(records.contains_key(key), "{key} was deleted");
    }
    assert!(!records.contains_key("ingest:deleteq:old"));
    let left = bucket_keys(&platform);
    for key in &old {
        assert!(!left.contains(key), "{key} survived");
    }
}
