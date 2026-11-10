use crate::{
    auth::{self, AuthConfig, AuthService, Error as AuthError, Invite, Role, SignedIn, User},
    http::{Request, Response},
    setup::{missing_entries, SetupStores},
    site::{self, Asset},
    state::StateStore,
};
use serde::Deserialize;
use serde_json::{json, Value};

pub type CoreCacheEntry = (Vec<u8>, Vec<u8>);

/// Core Cache lookups, replaces and purges one execution may make. Fastly enforces an
/// undocumented per-execution transaction limit, after which every later backend send fails
/// with `LIMIT_EXCEEDED`; `FastlyRuntime` refuses operations past this bound instead.
pub const CACHE_OP_BUDGET: u32 = 64;

/// Core Cache operations this execution may still make.
pub fn cache_ops_remaining(runtime: &impl Runtime) -> u32 {
    CACHE_OP_BUDGET.saturating_sub(runtime.cache_ops())
}

fn trace_id_from_parent(value: Option<&str>) -> Option<String> {
    let value = value?;
    let parts: Vec<&str> = value.split('-').collect();
    (parts.len() == 4
        && parts[0] == "00"
        && parts[1].len() == 32
        && parts[2].len() == 16
        && parts[3].len() == 2
        && parts[1].chars().all(|c| c.is_ascii_hexdigit())
        && parts[2].chars().all(|c| c.is_ascii_hexdigit())
        && parts[3].chars().all(|c| c.is_ascii_hexdigit())
        && parts[1].chars().any(|c| c != '0')
        && parts[2].chars().any(|c| c != '0'))
    .then(|| parts[1].to_owned())
}

/// How long a POP may serve a cacheable KV read (such as `svc:<id>`) written from another POP.
pub const KV_READ_CACHE_TTL_S: u64 = 300;

/// KV keys whose values only change on explicit admin config transitions and can be cached across
/// requests in Core Cache (invalidated on write). Mutable per-tick keys like `cron:state` and
/// cross-POP/cross-service coordination keys (`ingest:plan`, `ingest:watermark`, `ingest:backfill`)
/// must not be included.
pub fn is_cacheable_kv_key(key: &str) -> bool {
    key == "settings:operational_logs"
        || key == "admin:install"
        || (key.starts_with("svc:") && key.len() > 4)
}

#[derive(Debug, PartialEq, Eq)]
pub enum CoreCacheError {
    ReplaceFailed,
    PurgeRateLimited,
    Unsupported,
    Failed(String),
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct KvPage {
    pub keys: Vec<String>,
    pub next_cursor: Option<String>,
}

impl KvPage {
    pub fn from_sorted_keys(
        all_keys: impl IntoIterator<Item = String>,
        prefix: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Self {
        let (keys, next_cursor) =
            compute_common::stores::kv_page_from_keys(all_keys, prefix, cursor, limit);
        Self { keys, next_cursor }
    }
}

/// The longest searchable leading part of `prefix`; the caller filters the rest client-side.
pub fn kv_list_stem(prefix: &str) -> &str {
    compute_common::stores::kv_list_stem(prefix)
}

/// Narrows a stem listing to `prefix`. Keys are sorted, so a key past the prefix range ends paging.
pub fn kv_filter_stem_page(prefix: &str, keys: Vec<String>, next: Option<String>) -> KvPage {
    let (keys, next_cursor) = compute_common::stores::kv_filter_stem_page(keys, next, prefix);
    KvPage { keys, next_cursor }
}

pub struct VcpuSpan {
    vcpu_ms: u64,
    kv_ops: u32,
    cache_lookups: u32,
    cache_replaces: u32,
    purges: u32,
}

impl VcpuSpan {
    pub fn start(runtime: &impl Runtime) -> Self {
        Self {
            vcpu_ms: runtime.vcpu_ms(),
            kv_ops: runtime.kv_ops(),
            cache_lookups: runtime.cache_lookups(),
            cache_replaces: runtime.cache_replaces(),
            purges: runtime.purges(),
        }
    }
    pub fn elapsed(&self, runtime: &impl Runtime) -> u64 {
        runtime.vcpu_ms().saturating_sub(self.vcpu_ms)
    }
    pub fn kv_ops(&self, runtime: &impl Runtime) -> u32 {
        runtime.kv_ops().saturating_sub(self.kv_ops)
    }
    pub fn cache_lookups(&self, runtime: &impl Runtime) -> u32 {
        runtime.cache_lookups().saturating_sub(self.cache_lookups)
    }
    pub fn cache_replaces(&self, runtime: &impl Runtime) -> u32 {
        runtime.cache_replaces().saturating_sub(self.cache_replaces)
    }
    pub fn purges(&self, runtime: &impl Runtime) -> u32 {
        runtime.purges().saturating_sub(self.purges)
    }
}

/// A speculative batch read whose answer this call did not read; read the key again when it is
/// needed. A runtime that sent the read resumes it then instead of sending another.
pub const PREFETCH_UNREAD: &str = "prefetch_unread";

pub trait Runtime: SetupStores {
    fn config(&self, name: &str) -> Result<Option<String>, String>;
    fn secret(&self, name: &str) -> Result<Option<String>, String>;
    fn random(&self) -> Result<[u8; 32], String>;
    fn now(&self) -> u64;
    fn checks(&self, front_rl: bool) -> Result<Value, String>;
    fn versions(&self) -> Result<Value, String>;
    fn compute_service_id(&self) -> Option<String>;
    fn compute_service_version(&self) -> Option<String>;
    fn admin_get(&self, _key: &str) -> Result<Option<crate::admin::Record>, String> {
        Err("control_plane_unavailable".into())
    }
    fn admin_get_fresh(
        &self,
        key: &str,
        _token: &str,
    ) -> Result<Option<crate::admin::Record>, String> {
        self.admin_get(key)
    }
    fn admin_cas(
        &self,
        _key: &str,
        _generation: Option<&str>,
        _value: &Value,
    ) -> Result<bool, String> {
        Err("control_plane_unavailable".into())
    }
    fn admin_api(
        &self,
        _request: crate::admin::ApiRequest<'_>,
    ) -> Result<crate::admin::ApiResponse, String> {
        Err("control_plane_unavailable".into())
    }
    fn admin_fos(
        &self,
        _request: crate::admin::FosRequest<'_>,
    ) -> Result<crate::admin::FosResponse, String> {
        Err("control_plane_unavailable".into())
    }
    fn admin_cdn_get(
        &self,
        _service_id: &str,
        _key: &str,
        _cdn_key: &str,
        _max_bytes: usize,
        _timeout: std::time::Duration,
    ) -> Result<crate::admin::FosResponse, String> {
        Err("control_plane_unavailable".into())
    }
    #[allow(clippy::too_many_arguments)]
    fn admin_cdn_read(
        &self,
        service_id: &str,
        key: &str,
        cdn_key: &str,
        max_bytes: usize,
        timeout: std::time::Duration,
        range: Option<(u64, u64)>,
    ) -> Result<crate::admin::FosResponse, String> {
        if range.is_some() {
            return Err("cdn_range_unavailable".into());
        }
        self.admin_cdn_get(service_id, key, cdn_key, max_bytes, timeout)
    }
    /// [`Self::admin_cdn_get`] of the first `(key, max_bytes)`, within `timeout`. The rest
    /// answer [`PREFETCH_UNREAD`]; a runtime with concurrency first sends them, one backend call
    /// each, and leaves their answers unread until a later [`Self::admin_cdn_read`] of the key,
    /// so no speculative header or body wait can hold this read.
    fn admin_cdn_get_many(
        &self,
        service_id: &str,
        keys: &[(String, usize)],
        cdn_key: &str,
        timeout: std::time::Duration,
    ) -> Vec<Result<crate::admin::FosResponse, String>> {
        keys.iter()
            .enumerate()
            .map(|(index, (key, max))| {
                if index == 0 {
                    self.admin_cdn_get(service_id, key, cdn_key, *max, timeout)
                } else {
                    Err(PREFETCH_UNREAD.to_owned())
                }
            })
            .collect()
    }
    /// Records speculative reads sent by [`Self::admin_cdn_get_many`] and never read.
    fn release_unread(&self) {}
    /// Speculative reads of `service_id` sent and charged but not read yet, by object key, with
    /// the response limit each was sent for. A later read of the key resumes it without
    /// another backend call.
    fn admin_unread(&self, _service_id: &str) -> std::collections::BTreeMap<String, usize> {
        Default::default()
    }
    /// Whether [`Self::admin_fos_many`] sends its requests concurrently; only then is it used.
    fn admin_fos_concurrent(&self) -> bool {
        false
    }
    /// Sends every request concurrently, charging one backend call each; results keep request
    /// order. Called only when [`Self::admin_fos_concurrent`] is true.
    fn admin_fos_many(
        &self,
        requests: Vec<crate::admin::FosRequest<'_>>,
    ) -> Vec<Result<crate::admin::FosResponse, String>> {
        requests
            .into_iter()
            .map(|request| self.admin_fos(request))
            .collect()
    }
    fn admin_remaining_calls(&self) -> u32 {
        32
    }
    fn usage_agg(&self) -> Option<&crate::telemetry::usage::UsageAgg> {
        None
    }
    fn set_telemetry_budget(&self, _on: bool) {}
    fn api_log(&self) -> Option<&crate::telemetry::api::ApiLog> {
        None
    }
    fn admin_elapsed(&self, started: std::time::Instant) -> std::time::Duration {
        started.elapsed()
    }
    fn admin_wait(&self, millis: u64) {
        std::thread::sleep(std::time::Duration::from_millis(millis));
    }
    fn admin_event(&self, _event: Value) {}
    fn core_cache_replace(
        &self,
        _key: &str,
        _surrogate_keys: &[&str],
        _ttl: std::time::Duration,
        _update: &dyn Fn(Option<CoreCacheEntry>) -> CoreCacheEntry,
    ) -> Result<(), CoreCacheError> {
        Err(CoreCacheError::Unsupported)
    }
    fn core_cache_lookup(&self, _key: &str) -> Result<Option<CoreCacheEntry>, CoreCacheError> {
        Err(CoreCacheError::Unsupported)
    }
    fn core_cache_purge(&self, _surrogate: &str) -> Result<(), CoreCacheError> {
        Err(CoreCacheError::Unsupported)
    }
    fn admin_charge_call(&self) -> Result<(), String> {
        Ok(())
    }
    /// Core Cache operations made so far in this execution.
    fn cache_ops(&self) -> u32 {
        0
    }
    fn kv_ops(&self) -> u32 {
        0
    }
    fn cache_lookups(&self) -> u32 {
        0
    }
    fn cache_replaces(&self) -> u32 {
        0
    }
    fn purges(&self) -> u32 {
        0
    }
    /// Requests sent to the peer worker so far in this execution; each is a billed execution.
    fn worker_requests(&self) -> u32 {
        0
    }
    fn execution_backend_calls(&self) -> Option<u32> {
        None
    }
    fn execution_vcpu_ms(&self) -> Option<u64> {
        None
    }
    /// Fastly-billed Compute usage of the app and worker services over `[from, to]` (unix s).
    fn compute_billing(&self, _from: u64, _to: u64) -> Result<Value, String> {
        Err("compute_billing_unavailable".into())
    }
    /// Fastly Stats `requests` and `edge_requests` per UTC day for the logged service over `[from, to]` (unix s).
    fn logged_traffic(&self, _service_id: &str, _from: u64, _to: u64) -> Result<Value, String> {
        Err("logged_traffic_unavailable".into())
    }
    fn kv_list(&self, _prefix: &str, _cursor: Option<&str>, _limit: u32) -> Result<KvPage, String> {
        Err("unsupported".into())
    }
    fn kv_delete(&self, _key: &str) -> Result<(), String> {
        Err("unsupported".into())
    }
    /// One bounded delete request to the peer worker service.
    fn worker_delete(&self, _body: &Value) -> Result<Value, String> {
        Err("worker_unavailable".into())
    }
    /// Sends every `(path, body)` POST to the peer worker concurrently, charging one call each up
    /// front, and waits at most `deadline` for all of them. Results keep request order; a request
    /// that has not answered by then is `Err("worker_deadline")`.
    fn worker_dispatch(
        &self,
        requests: &[(String, Value)],
        _deadline: std::time::Duration,
    ) -> Vec<Result<Value, String>> {
        requests
            .iter()
            .map(|_| Err("worker_unavailable".into()))
            .collect()
    }
    /// Like `admin_fos`, also returning the response `ETag` header when there is one.
    fn admin_fos_etag(
        &self,
        request: crate::admin::FosRequest<'_>,
    ) -> Result<(crate::admin::FosResponse, Option<String>), String> {
        self.admin_fos(request).map(|response| (response, None))
    }
    fn vcpu_ms(&self) -> u64 {
        0
    }
    /// The sandbox's current memory use in MiB, when the runtime reports it.
    fn heap_mib(&self) -> Option<u32> {
        None
    }
    fn linear_memory_high_water_bytes(&self) -> Option<u64> {
        None
    }
    fn pop(&self) -> Option<String> {
        None
    }
    fn on_log_shield(&self) -> bool {
        false
    }
    fn setup_missing(&self, _control_plane: bool) -> Result<Vec<crate::setup::MissingEntry>, String>
    where
        Self: Sized,
    {
        missing_entries(self)
    }
}

#[derive(Default)]
struct Trace {
    actor: Option<String>,
    rows: Vec<Value>,
    dropped: u64,
}

pub struct Shell<'a, R: Runtime, S: StateStore> {
    runtime: &'a R,
    state: &'a S,
    assets: &'a [Asset],
    trace: std::cell::RefCell<Trace>,
}

pub fn unauthenticated_probe_status(
    runtime: &impl Runtime,
    peer_probe: impl FnOnce() -> Result<u16, String>,
) -> Result<u16, String> {
    let local = Shell::new(runtime, &crate::state::MemoryStore::default(), &[])
        .handle(&Request {
            method: "GET".into(),
            path: "/".into(),
            ..Default::default()
        })
        .status;
    if local != 403 {
        return Ok(local);
    }
    peer_probe()
}

impl<'a, R: Runtime, S: StateStore> Shell<'a, R, S> {
    pub fn new(runtime: &'a R, state: &'a S, assets: &'a [Asset]) -> Self {
        Self {
            runtime,
            state,
            assets,
            trace: Default::default(),
        }
    }

    fn front_authorized(&self, request: &Request) -> bool {
        let front_key = match self.runtime.secret("front_key") {
            Ok(Some(key)) if !key.is_empty() => key,
            _ => return false,
        };
        let supplied = request.header("x-flp-front-key").unwrap_or("");
        compute_common::crypto::constant_time_secret_eq(supplied, &front_key)
    }

    fn clock_response(&self, path: &str) -> Response {
        let ttl = crate::admin::clock_ttl_s(self.runtime, path);
        Response::new(200, r#"{"ok":true}"#)
            .header("content-type", "application/json")
            .header("cache-control", format!("public, max-age={ttl}"))
            .header("x-flp-tick-ttl", ttl.to_string())
    }

    /// The response a front-authorized clock probe (`GET /_tick` or `/_flush`) gets before its
    /// work runs, so the health check never waits on a long job. `None` for anything else,
    /// which `handle` answers as usual.
    pub fn clock_ack(&self, request: &Request) -> Option<Response> {
        let path = request.path.split('?').next().unwrap_or_default();
        (request.method == "GET"
            && matches!(path, "/_tick" | "/_flush")
            && self.front_authorized(request)
            && !crate::admin::clock_forbidden(self.runtime))
        .then(|| self.clock_response(path))
    }

    pub fn handle(&self, request: &Request) -> Response {
        if !self.front_authorized(request) {
            return error(403, "Front authentication required");
        }
        let started = std::time::Instant::now();
        let vcpu = VcpuSpan::start(self.runtime);
        let mut response = match self.route(request) {
            Ok(response) => response,
            Err(failure) => auth_error(failure, &request.path),
        };
        if matches!(response.status, 401 | 403)
            && request.cookie("flp_cdn").is_some()
            && !response.headers.contains_key("set-cookie")
        {
            response = response.header("set-cookie", auth::clear_cdn_cookie());
        }
        self.record_request(request, response, started, vcpu)
    }

    fn record_request(
        &self,
        request: &Request,
        mut response: Response,
        started: std::time::Instant,
        vcpu: VcpuSpan,
    ) -> Response {
        use crate::telemetry::{
            api::{self, drain_runtime_rows_with_page_load_id},
            flush_request_buffer, write_log, LogBuffer, WritableLogKind,
        };
        let path = request.path.split('?').next().unwrap_or(&request.path);
        if !(path.starts_with("/api/") || path.starts_with("/auth/")) {
            return response;
        }
        let unauthenticated_failure =
            self.trace.borrow().actor.is_none() && matches!(response.status, 401 | 403 | 404);
        if unauthenticated_failure {
            return response;
        }
        let vcpu_ms = vcpu.elapsed(self.runtime);
        let duration_ms = started.elapsed().as_millis() as u64;
        let trace = self.trace.take();
        let page_load_id = api::sanitize_page_load_id(request.header("x-page-load-id"));
        let harness = trace
            .actor
            .as_ref()
            .and_then(|_| api::sanitize_harness_label(request.header("x-flp-harness")));
        if let Some(label) = harness.as_deref() {
            if let Some(usage) = self.runtime.usage_agg() {
                usage.mark_harness(label);
            }
        }
        if trace.actor.is_some() {
            response = response.header("x-flp-vcpu-ms", vcpu_ms.to_string());
        }
        if trace.actor.is_some() && request.header("x-debug-responses") == Some("1") {
            if let Some(trace_id) = trace_id_from_parent(request.header("traceparent")) {
                response = response
                    .header("x-flp-trace-id", trace_id)
                    .header("server-timing", format!("compute;dur={duration_ms}"));
            }
        }
        let normalized = api::normalize_path(path);
        let compute_row = !api::is_exempt(&request.method, &normalized, response.status)
            && !api::folds_into_beacon(
                &request.method,
                &normalized,
                response.status,
                request.header("x-flp-fold-telemetry"),
            );
        self.runtime.release_unread();
        let runtime_rows = self.runtime.usage_agg().is_some_and(|u| !u.is_empty())
            || self.runtime.api_log().is_some_and(|l| !l.is_empty());
        if !compute_row && trace.rows.is_empty() && trace.dropped == 0 && !runtime_rows {
            return response;
        }
        self.runtime.set_telemetry_budget(true);
        let sid = crate::admin::active_service(self.runtime)
            .ok()
            .flatten()
            .map_or_else(|| "global".to_owned(), |service| service.service_id);
        let request_id = self
            .runtime
            .random()
            .map_or_else(|_| "request".to_owned(), |bytes| hex::encode(&bytes[..8]));
        let buffer = LogBuffer::new(&sid, &request_id);
        if compute_row {
            let mut row = json!({
                "source": "compute_api", "method": request.method, "path": normalized,
                "status_code": response.status, "duration_ms": duration_ms,
                "vcpu_ms": vcpu_ms, "actor": trace.actor,
            });
            let mut details = serde_json::Map::new();
            if let Some(label) = harness.as_deref() {
                details.insert("harness".into(), json!(label));
            }
            if let Some(id) = page_load_id.as_deref() {
                details.insert("page_load_id".into(), json!(id));
            }
            if !details.is_empty() {
                row["details_json"] = json!(Value::Object(details).to_string());
            }
            write_log(self.runtime, &buffer, WritableLogKind::ApiTelemetry, row);
        }
        for mut row in trace.rows {
            row["actor"] = json!(trace.actor);
            write_log(self.runtime, &buffer, WritableLogKind::ApiTelemetry, row);
        }
        for _ in 0..trace.dropped {
            buffer.note_dropped();
        }
        let debug_calls =
            drain_runtime_rows_with_page_load_id(self.runtime, &buffer, page_load_id.as_deref());
        if trace.actor.is_some()
            && request.header("x-debug-responses") == Some("1")
            && !debug_calls.is_empty()
        {
            if let Ok(encoded) = serde_json::to_string(&debug_calls) {
                response = response.header("x-flp-debug-calls", encoded);
            }
        }
        let outcome = flush_request_buffer(self.runtime, &buffer);
        self.runtime.set_telemetry_budget(false);
        if let Some((name, value)) = outcome.header() {
            response = response.header(name, value);
        }
        if let Some(error) = outcome.outbox_error {
            self.runtime
                .admin_event(json!({"event": "log.outbox_error", "error": error}));
        }
        response
    }

    fn beacon(&self, request: &Request) -> Response {
        use crate::telemetry::api::{self, BeaconError, BEACON_MAX_BYTES};
        if request.body.len() > BEACON_MAX_BYTES {
            return error(413, "Beacon too large");
        }
        let page_load_id = request.header("x-page-load-id");
        // Folded file reads are request accounting, so they are kept even with query telemetry off.
        let harness = api::sanitize_harness_label(request.header("x-flp-harness"));
        let mut rows: Vec<Value> =
            api::folded_file_row(&request.body, page_load_id, harness.as_deref())
                .into_iter()
                .collect();
        if api::operational_settings(self.runtime).browser_query_telemetry {
            match api::sanitize_beacon_with_page_load_id(&request.body, page_load_id) {
                Ok(row) => rows.push(row),
                Err(BeaconError::TooLarge) => return error(413, "Beacon too large"),
                Err(BeaconError::Invalid) => return error(400, "Invalid beacon"),
            }
        }
        if rows.is_empty() || !crate::telemetry::effective_dev_telemetry(self.runtime) {
            return Response::new(204, Vec::new()).private();
        }
        let session = request.cookie("flp_session").unwrap_or_default();
        let session_key = &compute_common::crypto::sha256_hex(session.as_bytes())[..16];
        let mut trace = self.trace.borrow_mut();
        if api::beacon_allowed(self.runtime, session_key, self.runtime.now()) {
            trace.rows.extend(rows);
        } else {
            trace.dropped += rows.len() as u64;
        }
        Response::new(204, Vec::new()).private()
    }

    fn route(&self, request: &Request) -> Result<Response, AuthError> {
        let path = request
            .path
            .split('?')
            .next()
            .unwrap_or(&request.path)
            .trim_end_matches('/');
        if matches!(request.method.as_str(), "GET" | "HEAD")
            && (matches!(
                path,
                "/how-it-works"
                    | "/how-it-works.html"
                    | "/how-it-works.txt"
                    | "/usage"
                    | "/usage.html"
                    | "/usage.txt"
                    | "/fastly.svg"
                    | "/favicon.ico"
            ) || path.starts_with("/_next/")
                || path.starts_with("/duckdb/")
                || path.starts_with("/geo/")
                || path.starts_with("/maplibre-gl-")
                || (path.starts_with("/how-it-works/__next.")
                    || path.starts_with("/usage/__next."))
                    && path.ends_with(".txt"))
        {
            return Ok(self.static_response(request));
        }
        if request.method == "GET" && path == "/healthz" {
            return Ok(json_response(
                json!({"healthy": missing_entries(self.runtime).map_err(storage)?.is_empty()}),
            ));
        }
        if request.method == "GET" && path == "/version" {
            return Ok(json_response(json!({"build_sha": env!("FLP_BUILD_SHA")})));
        }
        if request.method == "GET" && path == "/_tick" {
            if request.path.split('?').next() != Some("/_tick") {
                return Err(AuthError::Forbidden);
            }
            return match crate::admin::record_tick(self.runtime, "clock", "clock") {
                Ok(_) => Ok(self.clock_response("/_tick")),
                Err(reason) if reason == "worker_tick_forbidden" => Err(AuthError::Forbidden),
                Err(reason) => Err(storage(reason)),
            };
        }
        if request.method == "GET" && path == "/_flush" {
            if request.path.split('?').next() != Some("/_flush") {
                return Err(AuthError::Forbidden);
            }
            return match crate::admin::record_flush(self.runtime) {
                Ok(_) => Ok(self.clock_response("/_flush")),
                Err(reason) if reason == "worker_tick_forbidden" => Err(AuthError::Forbidden),
                Err(reason) => Err(storage(reason)),
            };
        }
        let control_plane = path.starts_with("/api/admin/provision/")
            || path.starts_with("/api/provision/")
            || path.starts_with("/api/admin/services")
            || path.starts_with("/api/admin/jobs/")
            || path.starts_with("/api/admin/log-fields/")
            || path.starts_with("/api/admin/logs/")
            || path.starts_with("/api/admin/usage/")
            || path.starts_with("/api/cron-runs")
            || matches!(
                path,
                "/api/services"
                    | "/api/log-fields/catalog"
                    | "/api/admin/audit-logs"
                    | "/api/admin/operational-logging"
            );
        let missing = self.runtime.setup_missing(control_plane).map_err(storage)?;
        let setup_page = is_setup_page(path);
        if !missing.is_empty() {
            if request.method == "GET" && path == "/api/setup" {
                let missing: Vec<Value> = missing
                    .iter()
                    .map(|entry| {
                        json!({
                            "store": entry.store, "name": entry.name, "command": entry.command,
                        })
                    })
                    .collect();
                return Ok(json_response(
                    json!({"complete":false,"accepted":false,"missing":missing}),
                ));
            }
            if matches!(request.method.as_str(), "GET" | "HEAD")
                && (setup_page
                    || path.starts_with("/_next/")
                    || matches!(path, "/favicon.ico" | "/fastly.svg"))
            {
                let response = self.static_response(request);
                return Ok(if setup_page {
                    response.private()
                } else {
                    response
                });
            }
            return Ok(if path.starts_with("/api/") {
                error(503, "Complete store setup first")
            } else {
                Response::redirect("/setup")
            });
        }
        let config = self.auth_config(!control_plane)?;
        let auth = AuthService::new(self.state, &config);
        let ip = request
            .header("fastly-client-ip")
            .ok_or(AuthError::InvalidInput)?;
        if ip.parse::<std::net::IpAddr>().is_err() {
            return Err(AuthError::InvalidInput);
        }
        let now = self.runtime.now();
        let front_rl = request.header("x-flp-front-rl") == Some("1");
        let setup_grant = request.cookie("flp_setup");
        let setup_grant = setup_grant.as_deref();
        if request.method != "GET" && request.method != "HEAD" {
            if request.header("origin") != Some(config.origin.as_str()) {
                return Err(AuthError::Forbidden);
            }
            if request
                .header("content-type")
                .is_none_or(|value| value.split(';').next() != Some("application/json"))
            {
                return Err(AuthError::InvalidInput);
            }
        }
        let is_auth = path == "/api/setup/accept" || path.starts_with("/api/auth/");
        if !front_rl && (setup_page || path.starts_with("/api/") || path.starts_with("/auth/")) {
            auth.check_rate_limit(ip, is_auth && path.ends_with("/options"), now)?;
        }
        let outcome = self.auth_route(&auth, request, path, setup_grant, ip, now);
        if let Some(result) = outcome {
            return match result {
                Err(failure) if is_signin_failure(&failure) => {
                    if !front_rl {
                        auth.record_failure(
                            ip,
                            auth_endpoint(path),
                            "authentication_failure",
                            now,
                        )?;
                    }
                    Ok(auth_error(failure, path).header("x-flp-auth-fail", "1"))
                }
                other => other,
            };
        }
        let user = request
            .cookie("flp_session")
            .and_then(|cookie| match auth.authenticate(&cookie, now) {
                Ok(user) => Some(Ok(user)),
                Err(AuthError::Unauthorized) => None,
                Err(other) => Some(Err(other)),
            })
            .transpose()?;
        self.trace.borrow_mut().actor = user.as_ref().map(|user| user.id.clone());
        if request.method == "POST" && path == "/api/telemetry/query" {
            user.as_ref().ok_or(AuthError::Unauthorized)?;
            return Ok(self.beacon(request));
        }
        if path == "/api/admin/system-checks" && matches!(request.method.as_str(), "GET" | "POST") {
            match &user {
                Some(user) if user.role == Role::Admin => (),
                Some(_) => return Err(AuthError::Forbidden),
                None => auth.check_setup_grant(setup_grant.ok_or(AuthError::Unauthorized)?, now)?,
            }
            return Ok(json_response(
                self.runtime.checks(front_rl).map_err(storage)?,
            ));
        }
        if matches!(
            path,
            "/api/services" | "/api/log-fields/catalog" | "/api/rum/versions"
        ) && request.method == "GET"
        {
            let user = user.as_ref().ok_or(AuthError::Unauthorized)?;
            let response = crate::admin::handle(self.runtime, &user.id, request);
            if path == "/api/services" && user.role != Role::Admin {
                return Ok(redact_services(response));
            }
            return Ok(response);
        }
        if path == "/api/views" || path.starts_with("/api/views/") {
            user.as_ref().ok_or(AuthError::Unauthorized)?;
            if !matches!(request.method.as_str(), "GET" | "POST" | "DELETE") {
                return Err(AuthError::NotFound);
            }
            let service = crate::admin::active_service(self.runtime).map_err(storage)?;
            let Some(service) = service else {
                return Ok(error(409, "No active service is provisioned."));
            };
            return crate::views::handle(self.runtime, &service.service_id, request);
        }
        let admin_page = is_admin_page_path(path);
        if path.starts_with("/api/admin/") || path.starts_with("/api/provision/") || admin_page {
            if admin_page && matches!(request.method.as_str(), "GET" | "HEAD") && user.is_none() {
                return Ok(Response::redirect(if auth.setup_complete()? {
                    "/sign-in"
                } else {
                    "/setup"
                }));
            }
            let user = user.as_ref().ok_or(AuthError::Unauthorized)?;
            authorize_admin(user.role)?;
            if admin_page && matches!(request.method.as_str(), "GET" | "HEAD") {
                return Ok(self.static_response(request).private());
            }
            return self.admin_route(&auth, user, request, path, ip, now);
        }
        if matches!(path, "/setup" | "/setup.html" | "/setup.txt")
            && matches!(request.method.as_str(), "GET" | "HEAD")
            && auth.setup_complete()?
        {
            return Ok(Response::redirect("/admin"));
        }
        if request.method == "GET" && path.starts_with("/api/logs/") && path.ends_with("/live") {
            let user = user.as_ref().ok_or(AuthError::Unauthorized)?;
            authorize_admin(user.role)?;
            let kind = path
                .strip_prefix("/api/logs/")
                .and_then(|rest| rest.strip_suffix("/live"))
                .and_then(crate::telemetry::LogKind::parse);
            let Some(kind) = kind else {
                return Ok(error(404, "Unknown log kind."));
            };
            let Some(service) = crate::admin::active_service(self.runtime).map_err(storage)? else {
                return Ok(error(409, "No active service is provisioned."));
            };
            let query = request.path.split_once('?').map_or("", |(_, query)| query);
            let param = |name: &str| {
                url::form_urlencoded::parse(query.as_bytes())
                    .find(|(key, _)| key == name)
                    .map(|(_, value)| value.into_owned())
            };
            let page = crate::telemetry::read_live(
                self.runtime,
                &service.service_id,
                kind,
                param("since").and_then(|v| v.parse().ok()),
                param("limit").and_then(|v| v.parse().ok()),
                param("cursor").as_deref(),
            );
            return Ok(match page {
                Ok(page) => json_response(json!({
                    "rows": page.rows,
                    "next_cursor": page.next_cursor,
                    "truncated": page.truncated,
                })),
                Err(_) => error(503, "Live log read failed."),
            });
        }
        if matches!(
            path,
            "/api/log-extents" | "/api/files" | "/api/file" | "/api/ingest/events"
        ) && (request.method == "GET" || request.method == "HEAD" && path == "/api/file")
        {
            let user = user.as_ref().ok_or(AuthError::Unauthorized)?;
            if path == "/api/file" && user.role == Role::AnalystNoPii {
                return Err(AuthError::Forbidden);
            }
            let service = crate::admin::active_service(self.runtime).map_err(storage)?;
            let Some(service) = service else {
                return Ok(if path == "/api/log-extents" {
                    json_response(
                        json!({"configured":false,"earliest":null,"latest":null,"earliest_log_at":null,"latest_log_at":null}),
                    )
                } else {
                    error(409, "No active service is provisioned.")
                });
            };
            let query = request.path.split_once('?').map_or("", |(_, query)| query);
            let wanted = |name: &str| {
                url::form_urlencoded::parse(query.as_bytes())
                    .find(|(key, _)| key == name)
                    .map(|(_, value)| value.into_owned())
            };
            use crate::ingest::api;
            let operational = match path {
                "/api/files" => wanted("dataset").is_some_and(|d| d.starts_with("ops")),
                "/api/file" => wanted("key").is_some_and(|k| api::is_ops_key(&service, &k)),
                _ => false,
            };
            if operational {
                authorize_admin(user.role)?;
            }
            let started = std::time::Instant::now();
            let mut response = match path {
                "/api/ingest/events" => {
                    match crate::ingest::catalog::events(self.runtime, &service, query) {
                        Ok(body) => Response::new(200, body)
                            .header("content-type", "text/event-stream")
                            .header("x-accel-buffering", "no")
                            .private(),
                        Err(_) => error(503, "Ingestion catalog is unavailable."),
                    }
                }
                "/api/files" => api_json(api::files(self.runtime, &service, query, started)),
                "/api/file" => {
                    match api::file(self.runtime, &service, query, request.header("range")) {
                        Ok(mut upstream) => {
                            upstream.head_only = request.method == "HEAD";
                            Response::streamed(upstream)
                                .header("content-type", "application/vnd.apache.parquet")
                                .header("cache-control", "private, max-age=31536000, immutable")
                                .header("vary", "Cookie")
                        }
                        Err((status, detail)) => error(status, detail),
                    }
                }
                _ => api_json(api::extents(self.runtime, &service, started)),
            };
            if response.status < 400 && !response.headers.contains_key("set-cookie") {
                if let Some(cookie) = self.cdn_cookie_for(request, &service, user.role, now) {
                    response = response.header("set-cookie", cookie);
                }
            }
            return Ok(response);
        }
        if request.method == "GET" && matches!(path, "/api/sync-status" | "/api/cron-runs") {
            let user = user.as_ref().ok_or(AuthError::Unauthorized)?;
            authorize_admin(user.role)?;
            let fresh = if path == "/api/sync-status" {
                let query = request.path.split_once('?').map_or("", |(_, query)| query);
                url::form_urlencoded::parse(query.as_bytes())
                    .find(|(key, _)| key == "fresh")
                    .map(|(_, value)| value.parse::<bool>().map_err(|_| AuthError::InvalidInput))
                    .transpose()?
                    .unwrap_or(false)
            } else {
                false
            };
            let name = path.trim_start_matches("/api/");
            let sid = crate::admin::installed_service_id(self.runtime);
            let cache_key = sid
                .as_deref()
                .map(|sid| crate::admin::status_cache_key(name, sid));
            if !fresh {
                if let Some(key) = &cache_key {
                    if let Ok(Some((body, _))) = self.runtime.core_cache_lookup(key) {
                        return Ok(Response::new(200, body)
                            .header("content-type", "application/json")
                            .private());
                    }
                }
            }
            let payload = if path == "/api/sync-status" {
                self.sync_status()
            } else {
                self.cron_runs()
            };
            if let (Some(key), Some(sid)) = (&cache_key, &sid) {
                let body = payload.to_string().into_bytes();
                let surrogate = crate::admin::status_surrogate(sid);
                let _ = self.runtime.core_cache_replace(
                    key,
                    &[surrogate.as_str()],
                    crate::admin::STATUS_CACHE_TTL,
                    &|_| (body.clone(), Vec::new()),
                );
            }
            return Ok(json_response(payload));
        }
        if path == "/api/cron-runs/trigger" && request.method == "POST" {
            let user = user.as_ref().ok_or(AuthError::Unauthorized)?;
            authorize_admin(user.role)?;
            let _: Empty = parse(request)?;
            let result = match crate::admin::record_tick(self.runtime, "manual", &user.id) {
                Ok(val) => val,
                Err(reason) if reason == "worker_tick_forbidden" => {
                    return Err(AuthError::Forbidden)
                }
                Err(reason) if reason == "cron_busy" => {
                    return Ok(error(
                        409,
                        "A scheduled cron run is in progress. Try again in a minute.",
                    ))
                }
                Err(reason) if reason == "service_not_configured" => {
                    return Ok(error(
                        409,
                        "No active service is provisioned. Provision a service in Admin before triggering a cron run.",
                    ))
                }
                Err(reason) => return Err(storage(reason)),
            };
            return Ok(json_response(result));
        }
        if path.starts_with("/api/") {
            if user.is_none() {
                return Err(AuthError::Unauthorized);
            }
            return Ok(error(404, "Not found"));
        }
        if request.method != "GET" && request.method != "HEAD" {
            return Ok(error(405, "Method not allowed").header("allow", "GET, HEAD"));
        }
        let response = self.static_response(request);
        Ok(if setup_page || path.starts_with("/auth/") {
            response.private()
        } else {
            response
        })
    }

    fn cdn_cookie_for(
        &self,
        request: &Request,
        service: &crate::admin::Service,
        role: Role,
        now: u64,
    ) -> Option<String> {
        let session = request.cookie("flp_session")?;
        let cdn_key = self
            .runtime
            .secret("cdn_key")
            .ok()
            .flatten()
            .filter(|k| !k.is_empty())?;
        let scope = crate::ingest::scope(service);
        if let Some(existing) = request.cookie("flp_cdn") {
            if auth::cdn_cookie_valid(
                &existing,
                &service.service_id,
                &scope,
                role,
                &session,
                now,
                &cdn_key,
            ) {
                return None;
            }
        }
        auth::issue_cdn_cookie(&service.service_id, &scope, role, &session, now, &cdn_key)
    }

    fn sync_status(&self) -> Value {
        let (configured, extents) = match crate::admin::active_service(self.runtime) {
            Ok(Some(service)) => (
                true,
                crate::ingest::api::extents(self.runtime, &service, std::time::Instant::now()).ok(),
            ),
            _ => (false, None),
        };
        let snapshot = crate::admin::cron_audit_snapshot(self.runtime);
        let cron_runs = snapshot.runs;
        let audit_logs = crate::admin::service_history(snapshot.service_audit);
        let (last_tick_at, next_tick_at) =
            crate::admin::tick_timestamps_for(self.runtime, snapshot.last_tick);
        let edge = |name: &str| extents.as_ref().map(|value| value[name].clone());
        let latest_log_at = edge("latest_log_at");
        let total_rows = edge("total_rows").unwrap_or(Value::Null);
        let total_rows_partial = edge("total_rows_partial").unwrap_or(Value::Bool(false));
        let last_sync_at = last_tick_at
            .clone()
            .or_else(|| cron_runs.first().map(|r| r.started_at.clone()));
        let rum = configured
            .then(|| crate::ingest::rum_extents(self.runtime).ok().flatten())
            .flatten()
            .unwrap_or(Value::Null);
        let request = configured.then(|| {
            json!({
                "latest_log_at": latest_log_at,
                "total_rows": total_rows,
                "total_rows_partial": total_rows_partial,
                "last_sync_at": last_sync_at,
            })
        });
        json!({
            "configured":configured,"busy":false,
            "earliest_log_at":edge("earliest_log_at"),"latest_log_at":latest_log_at,
            "local_rows":total_rows,"request":request,"rum":rum,"storage_mode":null,
            "latest_ingested_file_at":snapshot.last_ingest_at,"latest_available_file_at":null,
            "duckdb_size_bytes":null,"duckdb_exists":null,"active_run":null,
            "last_tick_at":last_tick_at,"next_tick_at":next_tick_at,"clock_interval_s":crate::admin::CLOCK_INTERVAL_S,
            "cron_runs":cron_runs,"audit_logs":audit_logs,
        })
    }

    fn cron_runs(&self) -> Value {
        let snapshot = crate::admin::cron_audit_snapshot(self.runtime);
        let service_id = snapshot.service_id;
        let (last_tick_at, next_tick_at) =
            crate::admin::tick_timestamps_for(self.runtime, snapshot.last_tick);
        json!({
            "configured": service_id.is_some(),
            "service_id": service_id,
            "last_tick_at": last_tick_at,
            "next_tick_at": next_tick_at,
            "clock_interval_s": crate::admin::CLOCK_INTERVAL_S,
            "last_ingest_at": snapshot.last_ingest_at,
            "runs": snapshot.runs,
            "schedules": snapshot.schedules,
        })
    }

    fn auth_config(&self, with_setup_hash: bool) -> Result<AuthConfig, AuthError> {
        let origin = self
            .runtime
            .config("public_base_url")
            .map_err(storage)?
            .ok_or(AuthError::Storage)?;
        let session_key = self
            .runtime
            .secret("session_key")
            .map_err(storage)?
            .ok_or(AuthError::Storage)?;
        let hash = if with_setup_hash {
            self.runtime.secret("admin_setup_hash").map_err(storage)?
        } else {
            None
        };
        AuthConfig::from_hash(&origin, session_key.into_bytes(), hash)
    }

    #[allow(dead_code)]
    fn session_body(&self, user: Option<Value>) -> Value {
        self.session_body_with_debug(user, None)
    }

    fn session_body_with_debug(
        &self,
        user: Option<Value>,
        debug: Option<(bool, bool, String, String)>,
    ) -> Value {
        let service_id = self
            .runtime
            .compute_service_id()
            .filter(|id| !id.is_empty());
        let version = self
            .runtime
            .compute_service_version()
            .and_then(|version| version.trim().parse::<u64>().ok());
        match debug {
            Some((debug_queries, debug_api_calls, query_vis, api_vis)) => json!({
                "user": user,
                "compute_service_id": service_id,
                "compute_service_version": version,
                "debug_queries": debug_queries,
                "debug_api_calls": debug_api_calls,
                "query_debug_visibility": query_vis,
                "api_call_debug_visibility": api_vis,
                "dev_telemetry_available": crate::telemetry::dev_telemetry_available(self.runtime),
                // An unreadable service shows the built-in cards only; it must not sign anyone out.
                "custom_dashboard_cards": crate::admin::active_service(self.runtime)
                    .ok()
                    .flatten()
                    .map_or_else(|| json!([]), |service| {
                        crate::catalog::dashboard_cards(&service.log_fields)
                    }),
            }),
            None => json!({
                "user": user,
                "compute_service_id": service_id,
                "compute_service_version": version,
            }),
        }
    }

    fn auth_route(
        &self,
        auth: &AuthService<'_, S>,
        request: &Request,
        path: &str,
        grant: Option<&str>,
        ip: &str,
        now: u64,
    ) -> Option<Result<Response, AuthError>> {
        Some((|| match (request.method.as_str(), path) {
            ("GET", "/api/setup") => Ok(json_response(json!({
                "complete":auth.setup_complete()?,
                "accepted":grant.is_some_and(|token| auth.check_setup_grant(token, now).is_ok()),
                "missing":[],
            }))),
            ("GET", "/api/auth/session") => {
                let (user, debug) = match request.cookie("flp_session") {
                    Some(cookie) => match auth.authenticate_with_settings(&cookie, now) {
                        Ok((user, settings)) => {
                            let debug_queries = auth::settings::allows_debug_role(
                                &settings.query_debug_visibility,
                                user.role,
                            );
                            let debug_api_calls = auth::settings::allows_debug_role(
                                &settings.api_call_debug_visibility,
                                user.role,
                            );
                            (
                                Some(user_json(&user)),
                                Some((
                                    debug_queries,
                                    debug_api_calls,
                                    settings.query_debug_visibility,
                                    settings.api_call_debug_visibility,
                                )),
                            )
                        }
                        Err(AuthError::Unauthorized) => (None, None),
                        Err(other) => return Err(other),
                    },
                    None => (None, None),
                };
                Ok(json_response(self.session_body_with_debug(user, debug)))
            }
            ("POST", "/api/setup/accept") => {
                let body: SetupAccept = parse(request)?;
                let token = auth.accept_setup(
                    &body.code,
                    ip,
                    now,
                    self.runtime.random().map_err(storage)?,
                )?;
                Ok(json_response(json!({"accepted":true}))
                    .header("set-cookie", auth::setup_cookie(&token)))
            }
            ("POST", "/api/auth/register/options") => {
                let body: Register = parse(request)?;
                let options = auth.registration_options(
                    grant,
                    body.invite_token.as_deref(),
                    &body.display_name,
                    ip,
                    now,
                    self.runtime.random().map_err(storage)?,
                )?;
                Ok(json_response(
                    serde_json::to_value(options).map_err(|_| AuthError::Storage)?,
                ))
            }
            ("POST", "/api/auth/register/verify") | ("POST", "/api/auth/login/verify") => {
                let body: Verify = parse(request)?;
                let signed = if path == "/api/auth/register/verify" {
                    auth.finish_registration(&body.challenge_id, &body.credential, ip, now)?
                } else {
                    auth.finish_login(&body.challenge_id, &body.credential, ip, now)?
                };
                Ok(signed_in_response(signed))
            }
            ("POST", "/api/auth/login/options") => {
                let _: Empty = parse(request)?;
                let options =
                    auth.login_options(ip, now, self.runtime.random().map_err(storage)?)?;
                Ok(json_response(
                    serde_json::to_value(options).map_err(|_| AuthError::Storage)?,
                ))
            }
            ("POST", "/api/auth/logout") => {
                let _: Empty = parse(request)?;
                Ok(json_response(json!({"ok":true})).header("set-cookie", auth::logout_cookie()))
            }
            _ => Err(AuthError::NotFound),
        })())
        .filter(|result| !matches!(result, Err(AuthError::NotFound)))
    }

    fn admin_route(
        &self,
        auth: &AuthService<'_, S>,
        actor: &User,
        request: &Request,
        path: &str,
        ip: &str,
        now: u64,
    ) -> Result<Response, AuthError> {
        if path == "/api/admin/operational-logging"
            || path.starts_with("/api/admin/logs/")
            || path.starts_with("/api/admin/usage/")
        {
            let outcome = crate::admin::logs::handle(self.runtime, &actor.id, request);
            if let Some((action, target)) = &outcome.audit {
                auth.record_admin_action(&actor.id, action, target, ip, now)?;
            }
            return Ok(outcome.response);
        }
        if path.starts_with("/api/admin/provision/")
            || path.starts_with("/api/provision/")
            || path.starts_with("/api/admin/services")
            || path.starts_with("/api/admin/jobs/")
            || path.starts_with("/api/admin/log-fields/")
            || path.starts_with("/api/admin/rum/")
        {
            let (response, audit) = crate::admin::handle_audited(self.runtime, &actor.id, request);
            if let Some((action, target)) = audit {
                auth.record_admin_action(&actor.id, action, &target, ip, now)?;
            }
            return Ok(response);
        }
        match (request.method.as_str(), path) {
            ("GET", "/api/admin/users") => {
                let users: Vec<_> = auth.users(&actor.id)?.iter().map(user_json).collect();
                Ok(json_response(json!({"users":users})))
            }
            ("GET", "/api/admin/invites") => {
                let invites: Result<Vec<_>, _> =
                    auth.invites(&actor.id)?.iter().map(invite_json).collect();
                Ok(json_response(json!({"invites":invites?})))
            }
            ("POST", "/api/admin/invites") => {
                let body: InviteCreate = parse(request)?;
                let (invite, token) = auth.create_invite(
                    &actor.id,
                    &body.display_name,
                    body.role,
                    ip,
                    now,
                    self.runtime.random().map_err(storage)?,
                )?;
                Ok(json_response(
                    json!({"invite":invite_json(&invite)?,"token":token}),
                ))
            }
            ("GET", "/api/admin/versions") => {
                Ok(json_response(self.runtime.versions().map_err(storage)?))
            }
            ("GET", "/api/admin/usage-logging") => Ok(json_response(
                serde_json::to_value(auth.usage_logging(&actor.id)?)
                    .map_err(|_| AuthError::Storage)?,
            )),
            ("PATCH", "/api/admin/usage-logging") => Ok(json_response(
                serde_json::to_value(auth.set_usage_logging(
                    &actor.id,
                    parse(request)?,
                    ip,
                    now,
                )?)
                .map_err(|_| AuthError::Storage)?,
            )),
            (
                "GET",
                "/api/admin/ingested-files" | "/api/admin/raw-tree" | "/api/admin/storage-tree",
            ) => {
                let Some(service) = crate::admin::active_service(self.runtime).map_err(storage)?
                else {
                    return Ok(error(409, "No active service is provisioned."));
                };
                let query = request.path.split_once('?').map_or("", |(_, query)| query);
                let started = std::time::Instant::now();
                use crate::ingest::api;
                Ok(api_json(match path {
                    "/api/admin/ingested-files" => {
                        api::ingested_files(self.runtime, &service, query, started)
                    }
                    "/api/admin/raw-tree" => {
                        api::tree(self.runtime, &service, query, started, true)
                    }
                    _ => api::tree(self.runtime, &service, query, started, false),
                }))
            }
            ("GET" | "POST", "/api/admin/quarantine" | "/api/admin/quarantine/purge") => {
                let Some(service) = crate::admin::active_service(self.runtime).map_err(storage)?
                else {
                    return Ok(error(409, "No active service is provisioned."));
                };
                let started = std::time::Instant::now();
                use crate::ingest::api;
                match (request.method.as_str(), path) {
                    ("GET", "/api/admin/quarantine") => {
                        let query = request.path.split_once('?').map_or("", |(_, query)| query);
                        Ok(api_json(api::quarantine(self.runtime, &service, query)))
                    }
                    ("POST", "/api/admin/quarantine/purge") => {
                        use crate::telemetry::{flush_request_buffer, LogBuffer};
                        let buffer = LogBuffer::new(&service.service_id, "quarantine-purge");
                        let result = api::quarantine_purge(
                            self.runtime,
                            &service,
                            &actor.id,
                            &request.body,
                            &buffer,
                            started,
                        );
                        self.runtime.set_telemetry_budget(true);
                        flush_request_buffer(self.runtime, &buffer);
                        self.runtime.set_telemetry_budget(false);
                        let result = match result {
                            Ok(purge) => {
                                if let Some(target) = &purge.audit {
                                    auth.record_admin_action(
                                        &actor.id,
                                        "quarantine_purged",
                                        target,
                                        ip,
                                        now,
                                    )?;
                                }
                                Ok(purge.body)
                            }
                            Err(failure) => Err(failure),
                        };
                        Ok(api_json(result))
                    }
                    _ => Ok(error(405, "Method not allowed")),
                }
            }
            ("GET", "/api/admin/audit-logs") => {
                let entries =
                    crate::admin::service_history(crate::admin::service_audit(self.runtime));
                Ok(json_response(json!({"entries": entries})))
            }
            _ => {
                let parts: Vec<_> = path.split('/').filter(|part| !part.is_empty()).collect();
                match (request.method.as_str(), parts.as_slice()) {
                    ("PATCH", ["api", "admin", "users", target]) => {
                        let body: RoleChange = parse(request)?;
                        auth.set_role(&actor.id, target, body.role, ip, now)?;
                        let user = auth
                            .users(&actor.id)?
                            .into_iter()
                            .find(|user| user.id == *target)
                            .ok_or(AuthError::NotFound)?;
                        Ok(json_response(user_json(&user)))
                    }
                    ("POST", ["api", "admin", "users", target, "revoke"]) => {
                        let _: Empty = parse(request)?;
                        auth.revoke_user(&actor.id, target, ip, now)?;
                        Ok(json_response(json!({"ok":true})))
                    }
                    (
                        "POST",
                        ["api", "admin", "users", target, "passkeys", credential, "revoke"],
                    ) => {
                        let _: Empty = parse(request)?;
                        auth.revoke_credential(&actor.id, target, credential, ip, now)?;
                        Ok(json_response(json!({"ok":true})))
                    }
                    ("POST", ["api", "admin", "invites", target, "revoke"]) => {
                        let _: Empty = parse(request)?;
                        auth.revoke_invite(&actor.id, target, ip, now)?;
                        Ok(json_response(json!({"ok":true})))
                    }
                    _ => Ok(error(404, "Not found")),
                }
            }
        }
    }

    fn static_response(&self, request: &Request) -> Response {
        let path = request.path.split('?').next().unwrap_or(&request.path);
        if site::resolve(self.assets, path).is_none() {
            if let Some(target) = legacy_auth_redirect(&request.path, path) {
                return Response::redirect(&target);
            }
        }
        let (asset, status) = match site::resolve(self.assets, path) {
            Some(asset) => (Some(asset), 200),
            None => (site::resolve(self.assets, "/404.html"), 404),
        };
        match asset {
            Some(asset) => {
                let Some((body, encoding)) = asset.negotiate(request.header("accept-encoding"))
                else {
                    println!(
                        "{}",
                        json!({"event":"static.not_acceptable","level":"warn","path":asset.path})
                    );
                    return error(406, "No acceptable content encoding")
                        .header("vary", "Accept-Encoding");
                };
                let mut response = Response::new(status, body.to_vec())
                    .header("content-type", asset.content_type)
                    .header("cache-control", asset.cache_control())
                    .header("surrogate-key", asset.surrogate_key());
                if asset.varies_by_encoding() {
                    response = response.header("vary", "Accept-Encoding");
                }
                if let Some(encoding) = encoding {
                    response = response.header("content-encoding", encoding);
                }
                response
            }
            None => error(404, "Not found"),
        }
    }
}

fn storage(reason: String) -> AuthError {
    println!(
        "{}",
        json!({"event":"runtime.error","level":"error","reason":reason})
    );
    AuthError::Storage
}

fn authorize_admin(role: Role) -> Result<(), AuthError> {
    if role == Role::Admin {
        Ok(())
    } else {
        Err(AuthError::Forbidden)
    }
}

fn json_response(value: Value) -> Response {
    Response::new(200, value.to_string())
        .header("content-type", "application/json")
        .private()
}

fn api_json(result: crate::ingest::api::Api) -> Response {
    match result {
        Ok(value) => json_response(value),
        Err((status, detail)) => error(status, detail),
    }
}

/// Status for an upstream answer to a streamed body: success, range and validator answers pass
/// through, a missing object is a 404 and anything else a 502. `None` passes the upstream on.
pub fn upstream_failure(status: u16) -> Option<Response> {
    match status {
        200 | 206 | 304 | 416 => None,
        404 => Some(error(404, "File not found")),
        _ => Some(error(502, "File read failed")),
    }
}

/// Headers copied from a streamed body's upstream answer.
pub const UPSTREAM_HEADERS: [&str; 5] = [
    "content-length",
    "content-range",
    "accept-ranges",
    "etag",
    "last-modified",
];

/// Reads a streamed body in full and serves its `Range` itself: the native stand-in for the
/// SDK's streaming send, which the native runtimes don't have.
pub fn fetch_upstream<R: Runtime>(runtime: &R, upstream: &crate::http::Upstream) -> Response {
    let fetched = runtime.admin_cdn_get(
        &upstream.service_id,
        &upstream.key,
        &upstream.cdn_key,
        usize::MAX - 1,
        std::time::Duration::from_secs(30),
    );
    let body = match fetched {
        Ok(answer) => match upstream_failure(answer.status) {
            None => answer.body,
            Some(failure) => return failure,
        },
        Err(_) => return error(502, "File read failed"),
    };
    let total = body.len() as u64;
    let range = upstream.range.as_deref().map(|r| byte_range(r, total));
    let mut response = match range {
        Some(Some(Some((first, last)))) => {
            Response::new(206, body[first as usize..=last as usize].to_vec())
                .header("content-range", format!("bytes {first}-{last}/{total}"))
        }
        Some(Some(None)) => {
            Response::new(416, Vec::new()).header("content-range", format!("bytes */{total}"))
        }
        _ => Response::new(200, body),
    };
    let length = response.body.len().to_string();
    response = response
        .header("accept-ranges", "bytes")
        .header("content-length", length)
        .header("x-flp-cdn-cache", "MISS");
    if upstream.head_only {
        response.body.clear();
    }
    response
}

/// A single `bytes=` range of a `total`-byte body: `None` when the header isn't one (served
/// whole), `Some(None)` when unsatisfiable, else the inclusive first and last byte.
fn byte_range(header: &str, total: u64) -> Option<Option<(u64, u64)>> {
    let spec = header.trim().strip_prefix("bytes=")?;
    if spec.contains(',') {
        return None;
    }
    let (first, last) = spec.split_once('-')?;
    let (first, last) = match (first.trim(), last.trim()) {
        ("", suffix) => {
            let suffix: u64 = suffix.parse().ok()?;
            if suffix == 0 {
                return Some(None);
            }
            (total.saturating_sub(suffix), total.checked_sub(1))
        }
        (first, "") => (first.parse().ok()?, total.checked_sub(1)),
        (first, last) => {
            let (first, last): (u64, u64) = (first.parse().ok()?, last.parse().ok()?);
            if last < first {
                return None;
            }
            (first, total.checked_sub(1).map(|end| end.min(last)))
        }
    };
    Some(last.filter(|last| first <= *last).map(|last| (first, last)))
}

/// `response` with any streamed body read in place, for the native runtimes.
pub fn resolve_upstream<R: Runtime>(runtime: &R, mut response: Response) -> Response {
    let Some(upstream) = response.upstream.take() else {
        return response;
    };
    let fetched = fetch_upstream(runtime, &upstream);
    if !matches!(fetched.status, 200 | 206 | 304 | 416) {
        return fetched;
    }
    response.status = fetched.status;
    response.body = fetched.body;
    response.headers.extend(fetched.headers);
    response
}

fn error(status: u16, detail: &str) -> Response {
    let mut response = json_response(json!({"detail":detail}));
    response.status = status;
    response
}

fn auth_error(failure: AuthError, path: &str) -> Response {
    let path = path.split('?').next().unwrap_or(path);
    let setup = path.trim_end_matches('/') == "/api/setup/accept";
    match failure {
        AuthError::Unauthorized if setup => error(
            401,
            "That setup code was not accepted. Use the latest code printed by admin-reset. If you just generated it, wait about a minute or longer for it to reach this app before trying the same code once more. Generating another code restarts propagation.",
        ),
        AuthError::SetupClosed if setup => error(
            401,
            "This setup code has expired, already been used, or been disabled. If you just generated a replacement, wait about a minute or longer for it to reach this app before trying once more. Otherwise, run make admin-reset from the repository root to get a new code valid for 15 minutes.",
        ),
        AuthError::Forbidden => error(403, "Forbidden"),
        AuthError::Conflict => error(409, "The entry changed; reload and try again"),
        AuthError::NotFound => error(404, "Not found"),
        AuthError::InvalidInput => error(422, "Invalid request"),
        AuthError::Storage => error(503, "Store or API unavailable"),
        AuthError::RateLimited { retry_after } => {
            error(429, "Too many attempts").header("retry-after", retry_after.to_string())
        }
        _ => error(401, "Sign-in could not be completed"),
    }
}

pub const ANALYTICS_SITE_ROUTES: &[&str] = &[
    "/dashboard",
    "/origin",
    "/performance",
    "/security",
    "/network",
    "/fastly-value",
    "/assets-shield",
    "/rum",
    "/streaming",
    "/sessions",
    "/sessions/stream",
    "/query",
    "/insights",
    "/usage",
    "/how-it-works",
    "/share-login",
    "/share-login/acknowledge",
    "/setup",
    "/sign-in",
    "/invite",
    "/admin",
    "/admin/share",
    "/admin/usage-log",
    "/admin/rum",
    "/admin/session-scoring",
    "/admin/tick",
    "/logs",
    "/provision",
    "/log-fields",
    "/services",
];

pub fn is_admin_page_path(path: &str) -> bool {
    let base = path
        .strip_suffix(".html")
        .or_else(|| path.strip_suffix(".txt"))
        .unwrap_or(path);
    matches!(
        base,
        "/admin"
            | "/admin/share"
            | "/admin/usage-log"
            | "/admin/rum"
            | "/admin/session-scoring"
            | "/admin/tick"
            | "/provision"
            | "/log-fields"
            | "/services"
            | "/logs"
    )
}

fn is_setup_page(path: &str) -> bool {
    matches!(
        path,
        "/setup"
            | "/setup.html"
            | "/setup.txt"
            | "/share-login"
            | "/share-login.html"
            | "/share-login.txt"
    )
}

fn legacy_auth_redirect(request_path: &str, path: &str) -> Option<String> {
    let query = request_path.split_once('?').map(|(_, q)| q).unwrap_or("");
    let base = path
        .strip_suffix(".html")
        .or_else(|| path.strip_suffix(".txt"))
        .unwrap_or(path);
    match base {
        "/setup" | "/sign-in" | "/invite" => Some(if query.is_empty() {
            "/share-login".to_owned()
        } else {
            format!("/share-login?{query}")
        }),
        _ => None,
    }
}

fn is_signin_failure(error: &AuthError) -> bool {
    matches!(
        error,
        AuthError::Unauthorized
            | AuthError::SetupClosed
            | AuthError::InvalidCredential
            | AuthError::CounterRegression
            | AuthError::ChallengeExpired
            | AuthError::InvalidInput
    )
}

fn auth_endpoint(path: &str) -> &'static str {
    match path {
        "/api/setup/accept" => "setup",
        "/api/auth/register/verify" => "register_verify",
        "/api/auth/login/verify" => "login_verify",
        _ => "auth",
    }
}

fn user_json(user: &User) -> Value {
    let passkeys: Vec<_> = user
        .credentials
        .iter()
        .filter(|credential| !credential.revoked)
        .map(|credential| json!({"id":credential.id}))
        .collect();
    json!({"id":user.id,"display_name":user.display_name,"role":user.role,"revoked":user.revoked,"passkeys":passkeys})
}

fn invite_json(invite: &Invite) -> Result<Value, AuthError> {
    let expires = time::OffsetDateTime::from_unix_timestamp(
        i64::try_from(invite.expires_at).map_err(|_| AuthError::Storage)?,
    )
    .map_err(|_| AuthError::Storage)?
    .format(&time::format_description::well_known::Rfc3339)
    .map_err(|_| AuthError::Storage)?;
    Ok(
        json!({"id":invite.id,"display_name":invite.display_name,"role":invite.role,"expires_at":expires,"revoked":invite.revoked,"used":invite.used}),
    )
}

fn signed_in_response(signed: SignedIn) -> Response {
    json_response(json!({"user":user_json(&signed.user)}))
        .header("set-cookie", auth::cookie_header(&signed.cookie))
}

fn parse<T: serde::de::DeserializeOwned>(request: &Request) -> Result<T, AuthError> {
    if request.body.len() > 65536 {
        return Err(AuthError::InvalidInput);
    }
    serde_json::from_slice(&request.body).map_err(|_| AuthError::InvalidInput)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SetupAccept {
    code: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Register {
    display_name: String,
    invite_token: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Verify {
    challenge_id: String,
    credential: auth::BrowserCredential,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InviteCreate {
    display_name: String,
    role: Role,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RoleChange {
    role: Role,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Empty {}

/// Non-admin callers get only the fields the shell needs, never storage or
/// Secret Store reference names.
fn redact_services(mut response: Response) -> Response {
    const KEEP: [&str; 6] = [
        "service_id",
        "name",
        "status",
        "logging_enabled",
        "created_at",
        "updated_at",
    ];
    if response.status != 200 {
        return response;
    }
    let Ok(mut body) = serde_json::from_slice::<Value>(&response.body) else {
        return Response::new(500, "{\"error\":\"internal\"}");
    };
    let catalog = crate::catalog::get_catalog_for_api(None);
    if let Some(services) = body["services"].as_array_mut() {
        for service in services {
            if let Some(object) = service.as_object_mut() {
                let log_fields = object.get("log_fields").map(|fields| {
                    let custom_fields: Vec<Value> = fields["custom_fields"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter(|field| field["enabled"] != false)
                        .map(|field| {
                            let mut field = field.clone();
                            if let Some(field) = field.as_object_mut() {
                                field.retain(|key, _| {
                                    [
                                        "name",
                                        "label",
                                        "description",
                                        "duckdb_type",
                                        "sql_type",
                                        "value_type",
                                        "filterable",
                                        "enabled",
                                        "show_in_dashboard",
                                        "show_in_logs",
                                    ]
                                    .contains(&key.as_str())
                                });
                            }
                            field
                        })
                        .collect();
                    let groups: Vec<&Value> = fields["groups"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter(|group| {
                            group.as_str().is_some_and(|id| {
                                [
                                    "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
                                ]
                                .contains(&id)
                            })
                        })
                        .collect();
                    let overrides: serde_json::Map<String, Value> = fields["field_overrides"]
                        .as_object()
                        .into_iter()
                        .flatten()
                        .filter(|(id, value)| {
                            value.is_boolean()
                                && catalog
                                    .as_array()
                                    .into_iter()
                                    .flatten()
                                    .any(|field| field["id"].as_str() == Some(id.as_str()))
                        })
                        .map(|(id, value)| (id.clone(), value.clone()))
                        .collect();
                    json!({
                        "groups": groups,
                        "field_overrides": overrides,
                        "custom_fields": custom_fields,
                    })
                });
                let rum_enabled = json!(object
                    .get("rum_enabled")
                    .and_then(Value::as_bool)
                    .unwrap_or(false));
                let faro_version = json!(object.get("faro_version").and_then(Value::as_str));
                let cmcd_enabled = object
                    .get("cmcd")
                    .and_then(|cmcd| cmcd.get("enabled"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let scoring_enabled = object
                    .get("scoring")
                    .and_then(|scoring| scoring.get("enabled"))
                    .and_then(Value::as_bool)
                    .or_else(|| object.get("scoring_enabled").and_then(Value::as_bool))
                    .unwrap_or(false);
                object.retain(|key, _| KEEP.contains(&key.as_str()));
                if let Some(fields) = log_fields {
                    object.insert("log_fields".into(), fields);
                }
                object.insert("rum_enabled".into(), rum_enabled.clone());
                object.insert(
                    "rum".into(),
                    json!({"enabled":rum_enabled,"faro_version":faro_version}),
                );
                object.insert("faro_version".into(), faro_version);
                object.insert("cmcd".into(), json!({"enabled":cmcd_enabled}));
                object.insert("scoring".into(), json!({"enabled":scoring_enabled}));
            }
        }
    }
    response.body = body.to_string().into_bytes();
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::MemoryStore;
    use std::collections::BTreeMap;

    #[test]
    fn analyst_services_keep_safe_field_metadata_without_management_secrets() {
        let response = redact_services(json_response(serde_json::json!({"services":[{
            "service_id":"fixture-service", "name":"Fixture", "fastly_token":"PRIVATE_TOKEN",
            "fos_secret":"PRIVATE_STORAGE", "generation":"PRIVATE_GENERATION",
            "rum_enabled":true, "faro_version":"1.0.0",
            "cmcd":{"enabled":true,"secret":"PRIVATE_CMCD"},
            "scoring":{"enabled":true,"secret":"PRIVATE_SCORER"},
            "log_fields":{"groups":["A",{"secret":"PRIVATE_GROUP"}],"field_overrides":{"url":true,"secret":"PRIVATE_OVERRIDE"},
                "custom_fields":[{"name":"tenant","duckdb_type":"DOUBLE","enabled":true,
                    "vcl_log_expression":"PRIVATE_EXPRESSION"},
                    {"name":"disabled","enabled":false}]}
        }]})));
        let body: Value = serde_json::from_slice(&response.body).unwrap();
        let service = &body["services"][0];
        assert_eq!(service["log_fields"]["custom_fields"][0]["name"], "tenant");
        assert_eq!(
            service["log_fields"]["custom_fields"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(service["rum"]["enabled"], true);
        assert_eq!(service["cmcd"]["enabled"], true);
        assert_eq!(service["scoring"]["enabled"], true);
        assert_eq!(service["log_fields"]["groups"], json!(["A"]));
        assert_eq!(
            service["log_fields"]["field_overrides"],
            json!({"url":true})
        );
        assert_eq!(service["faro_version"], "1.0.0");
        assert!(!body.to_string().contains("PRIVATE_"));
    }

    #[test]
    fn kv_prefix_queries_drop_unsearchable_characters() {
        assert_eq!(kv_list_stem("ingest:deleteq:"), "ingest");
        assert_eq!(kv_list_stem("logq_svc_"), "logq_svc_");
        assert_eq!(kv_list_stem("plain_prefix-1"), "plain_prefix-1");
        assert_eq!(kv_list_stem(""), "");
    }

    #[test]
    fn kv_stem_pages_filter_to_prefix_and_stop_past_range() {
        let keys = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let page = kv_filter_stem_page(
            "ingest:deleteq:",
            keys(&["ingest:deleteq:a", "ingest:deleteq:b", "ingest:plan"]),
            Some("cursor".into()),
        );
        assert_eq!(page.keys, keys(&["ingest:deleteq:a", "ingest:deleteq:b"]));
        assert_eq!(page.next_cursor, None);
        let before = kv_filter_stem_page(
            "logs:flush_txn:",
            keys(&["logs:cursor:a", "logs:cursor:b"]),
            Some("logs:cursor:b".into()),
        );
        assert!(before.keys.is_empty());
        assert_eq!(before.next_cursor.as_deref(), Some("logs:cursor:b"));
    }

    struct Platform {
        config: BTreeMap<String, String>,
        secrets: BTreeMap<String, String>,
        complete: bool,
        service_id: Option<String>,
        service_version: Option<String>,
    }

    impl SetupStores for Platform {
        fn config_present(&self, _: &str) -> Result<bool, String> {
            Ok(self.complete)
        }
        fn secret_present(&self, _: &str) -> Result<bool, String> {
            Ok(self.complete)
        }
        fn state_linked(&self) -> Result<bool, String> {
            Ok(true)
        }
    }

    impl Runtime for Platform {
        fn config(&self, name: &str) -> Result<Option<String>, String> {
            Ok(self.config.get(name).cloned())
        }
        fn secret(&self, name: &str) -> Result<Option<String>, String> {
            Ok(self.secrets.get(name).cloned())
        }
        fn random(&self) -> Result<[u8; 32], String> {
            Ok([9; 32])
        }
        fn now(&self) -> u64 {
            100
        }
        fn checks(&self, _: bool) -> Result<Value, String> {
            panic!("checks must not run before code acceptance")
        }
        fn versions(&self) -> Result<Value, String> {
            panic!("versions must require admin")
        }
        fn compute_service_id(&self) -> Option<String> {
            self.service_id.clone()
        }
        fn compute_service_version(&self) -> Option<String> {
            self.service_version.clone()
        }
    }

    #[derive(Default)]
    struct MemRuntime {
        cache: std::cell::RefCell<BTreeMap<String, (CoreCacheEntry, Vec<String>)>>,
        kv: std::cell::RefCell<BTreeMap<String, String>>,
        budget: crate::admin::BackendBudget,
        replace_timeout: bool,
        purge_limited: bool,
        vcpu: std::cell::Cell<u64>,
        pop: Option<String>,
        shield: bool,
        store_calls: std::cell::Cell<u32>,
    }

    impl SetupStores for MemRuntime {
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

    impl Runtime for MemRuntime {
        fn config(&self, name: &str) -> Result<Option<String>, String> {
            Ok((name == "dev_telemetry").then(|| "1".into()))
        }
        fn secret(&self, _: &str) -> Result<Option<String>, String> {
            Ok(None)
        }
        fn random(&self) -> Result<[u8; 32], String> {
            Ok([0; 32])
        }
        fn now(&self) -> u64 {
            0
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
            self.budget.remaining()
        }
        fn admin_cas(
            &self,
            key: &str,
            generation: Option<&str>,
            value: &Value,
        ) -> Result<bool, String> {
            self.budget.charge()?;
            if self.replace_timeout {
                return Err("handoff_failed".into());
            }
            let mut kv = self.kv.borrow_mut();
            if generation.is_some() || kv.contains_key(key) {
                return Ok(false);
            }
            kv.insert(key.into(), value.to_string());
            Ok(true)
        }
        fn core_cache_replace(
            &self,
            key: &str,
            surrogate_keys: &[&str],
            _ttl: std::time::Duration,
            update: &dyn Fn(Option<CoreCacheEntry>) -> CoreCacheEntry,
        ) -> Result<(), CoreCacheError> {
            if self.replace_timeout {
                return Err(CoreCacheError::ReplaceFailed);
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
        fn core_cache_purge(&self, surrogate: &str) -> Result<(), CoreCacheError> {
            if self.purge_limited {
                return Err(CoreCacheError::PurgeRateLimited);
            }
            self.cache
                .borrow_mut()
                .retain(|_, (_, keys)| !keys.iter().any(|k| k == surrogate));
            Ok(())
        }
        fn kv_list(
            &self,
            prefix: &str,
            cursor: Option<&str>,
            limit: u32,
        ) -> Result<KvPage, String> {
            self.budget.charge()?;
            self.store_calls.set(self.store_calls.get() + 1);
            let kv = self.kv.borrow();
            Ok(KvPage::from_sorted_keys(
                kv.keys().cloned(),
                prefix,
                cursor,
                limit,
            ))
        }
        fn kv_delete(&self, key: &str) -> Result<(), String> {
            self.budget.charge()?;
            self.store_calls.set(self.store_calls.get() + 1);
            self.kv.borrow_mut().remove(key);
            Ok(())
        }
        fn vcpu_ms(&self) -> u64 {
            self.vcpu.get()
        }
        fn pop(&self) -> Option<String> {
            self.pop.clone()
        }
        fn on_log_shield(&self) -> bool {
            self.shield
        }
    }

    #[test]
    fn failed_cleanup_handoff_preserves_inline_queues_without_overwriting_existing_work() {
        let rt = MemRuntime::default();
        let queues: Vec<(String, Value)> = vec![
            (
                "ingest:deleteq:request".into(),
                json!({"run_id":"request","inflight_keys":["request.json"]}),
            ),
            (
                "ingest:deleteq:rum".into(),
                json!({"run_id":"rum","stream":"rum","inflight_keys":["rum.json"]}),
            ),
        ];
        let progress = json!({"run_id":"rum","stream":"rum","inflight_keys":[]});
        rt.kv
            .borrow_mut()
            .insert(queues[1].0.clone(), progress.to_string());
        crate::admin::preserve_inline_deleteqs(&rt, &queues).unwrap();
        assert_eq!(rt.kv.borrow()[&queues[0].0], queues[0].1.to_string());
        assert_eq!(rt.kv.borrow()[&queues[1].0], progress.to_string());
        let failed = MemRuntime {
            replace_timeout: true,
            ..Default::default()
        };
        assert_eq!(
            crate::admin::preserve_inline_deleteqs(&failed, &queues),
            Err("handoff_failed".into())
        );
        for _ in 0..28 {
            failed.budget.charge().unwrap();
        }
        assert_eq!(
            crate::admin::preserve_inline_deleteqs(&failed, &queues),
            Err("deleteq_handoff_budget_exhausted".into())
        );
    }

    #[test]
    fn core_cache_replace_applies_update_and_reports_lost_waits() {
        let rt = MemRuntime::default();
        let ttl = std::time::Duration::from_secs(60);
        let append = |old: Option<CoreCacheEntry>| {
            let (mut body, _) = old.unwrap_or_default();
            body.extend_from_slice(b"x");
            (body, b"meta".to_vec())
        };
        rt.core_cache_replace("k", &["s"], ttl, &append).unwrap();
        rt.core_cache_replace("k", &["s"], ttl, &append).unwrap();
        assert_eq!(
            rt.core_cache_lookup("k").unwrap(),
            Some((b"xx".to_vec(), b"meta".to_vec()))
        );
        assert_eq!(rt.core_cache_lookup("absent").unwrap(), None);
        rt.core_cache_purge("s").unwrap();
        assert_eq!(rt.core_cache_lookup("k").unwrap(), None);

        let lost = MemRuntime {
            replace_timeout: true,
            purge_limited: true,
            ..Default::default()
        };
        assert_eq!(
            lost.core_cache_replace("k", &[], ttl, &append),
            Err(CoreCacheError::ReplaceFailed)
        );
        assert_eq!(
            lost.core_cache_purge("s"),
            Err(CoreCacheError::PurgeRateLimited)
        );
    }

    #[test]
    fn runtime_defaults_are_unsupported_noops() {
        let rt = platform(true);
        let ttl = std::time::Duration::from_secs(1);
        assert!(matches!(
            rt.core_cache_replace("k", &[], ttl, &|_| Default::default()),
            Err(CoreCacheError::Unsupported)
        ));
        assert!(rt.kv_list("", None, 1).is_err());
        assert!(rt.kv_delete("k").is_err());
        assert_eq!(
            (rt.vcpu_ms(), rt.pop(), rt.on_log_shield()),
            (0, None, false)
        );
    }

    #[test]
    fn kv_list_pages_in_key_order_and_sees_mid_pagination_writes() {
        let rt = MemRuntime::default();
        for k in ["p:a", "p:c", "p:e", "q:z"] {
            rt.kv.borrow_mut().insert(k.into(), "1".into());
        }
        let first = rt.kv_list("p:", None, 2).unwrap();
        assert_eq!(first.keys, ["p:a", "p:c"]);
        rt.kv.borrow_mut().insert("p:d".into(), "1".into());
        let second = rt.kv_list("p:", first.next_cursor.as_deref(), 2).unwrap();
        assert_eq!(second.keys, ["p:d", "p:e"]);
        assert_eq!(second.next_cursor, None);
        rt.kv_delete("p:d").unwrap();
        assert!(!rt.kv.borrow().contains_key("p:d"));
    }

    #[test]
    fn kv_list_and_delete_charge_budget_before_touching_the_store() {
        let rt = MemRuntime::default();
        for _ in 0..30 {
            rt.budget.charge().unwrap();
        }
        rt.kv_list("", None, 1).unwrap();
        rt.kv_delete("k").unwrap();
        assert_eq!(rt.store_calls.get(), 2);
        assert_eq!(
            rt.kv_list("", None, 1).unwrap_err(),
            "backend_budget_exhausted"
        );
        assert_eq!(rt.kv_delete("k").unwrap_err(), "backend_budget_exhausted");
        assert_eq!(rt.store_calls.get(), 2);
    }

    #[test]
    fn vcpu_span_is_a_saturating_delta_and_identity_values_pass_through() {
        let rt = MemRuntime {
            pop: Some("IAD".into()),
            shield: true,
            ..Default::default()
        };
        rt.vcpu.set(40);
        let span = VcpuSpan::start(&rt);
        rt.vcpu.set(55);
        assert_eq!(span.elapsed(&rt), 15);
        rt.vcpu.set(10);
        assert_eq!(span.elapsed(&rt), 0);
        assert_eq!(rt.pop().as_deref(), Some("IAD"));
        assert!(rt.on_log_shield());
    }

    fn platform(complete: bool) -> Platform {
        Platform {
            config: BTreeMap::from([("public_base_url".into(), "https://example.invalid".into())]),
            secrets: BTreeMap::from([
                ("front_key".into(), "front-test-key-never-real".into()),
                ("session_key".into(), "s".repeat(48)),
                (
                    "admin_setup_hash".into(),
                    json!({"sha256":crate::auth::hash("setup-test"),"expires_at":1000}).to_string(),
                ),
            ]),
            complete,
            service_id: None,
            service_version: None,
        }
    }

    #[test]
    fn session_reports_compute_identity_for_signed_in_and_out_users() {
        let state = MemoryStore::default();
        let user = Some(json!({"id":"admin"}));
        for (id, version, want_id, want_version) in [
            (Some("svc123"), Some("42"), json!("svc123"), json!(42)),
            (None, None, Value::Null, Value::Null),
            (Some("svc123"), Some("latest"), json!("svc123"), Value::Null),
            (Some(""), Some("-1"), Value::Null, Value::Null),
        ] {
            let mut platform = platform(true);
            platform.service_id = id.map(Into::into);
            platform.service_version = version.map(Into::into);
            let shell = Shell::new(&platform, &state, &[]);
            for user in [None, user.clone()] {
                let body = shell.session_body(user.clone());
                assert_eq!(body["user"], user.clone().unwrap_or(Value::Null));
                assert_eq!(body["compute_service_id"], want_id);
                assert_eq!(body["compute_service_version"], want_version);
            }
        }
    }

    #[test]
    fn signed_in_session_lists_custom_dashboard_cards() {
        let state = MemoryStore::default();
        let platform = platform(true);
        let shell = Shell::new(&platform, &state, &[]);
        let body = shell.session_body_with_debug(
            Some(json!({"id":"admin"})),
            Some((false, false, "admin".into(), "admin".into())),
        );
        assert_eq!(body["custom_dashboard_cards"], json!([]));
        assert!(shell
            .session_body(None)
            .get("custom_dashboard_cards")
            .is_none());
    }

    #[test]
    fn signed_out_session_route_includes_compute_identity() {
        let state = MemoryStore::default();
        let mut platform = platform(true);
        platform.service_id = Some("svc123".into());
        platform.service_version = Some("7".into());
        let shell = Shell::new(&platform, &state, &[]);
        let request = Request {
            method: "GET".into(),
            path: "/api/auth/session".into(),
            headers: BTreeMap::from([
                ("x-flp-front-key".into(), "front-test-key-never-real".into()),
                ("fastly-client-ip".into(), "192.0.2.1".into()),
            ]),
            ..Default::default()
        };
        let response = shell.handle(&request);
        assert_eq!(response.status, 200);
        let body: Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(
            body,
            json!({"user":null,"compute_service_id":"svc123","compute_service_version":7})
        );
    }

    #[test]
    fn setup_code_rejections_explain_recovery_without_signin_redirects() {
        use crate::auth::{AuthState, SetupState};
        use crate::state::StateStore;

        for (code, failures, expires_at, explanation) in [
            ("outdated-code", 0, 1000, "latest code"),
            ("setup-test", 20, 1000, "disabled"),
            ("setup-test", 0, 100, "expired"),
        ] {
            let mut platform = platform(true);
            platform.secrets.insert(
                "admin_setup_hash".into(),
                json!({"sha256":crate::auth::hash("setup-test"),"expires_at":expires_at})
                    .to_string(),
            );
            let state = MemoryStore::default();
            let initial = AuthState {
                setup: SetupState {
                    hash_generation: crate::auth::hash("setup-test"),
                    failures,
                    ..Default::default()
                },
                ..Default::default()
            };
            assert!(state.compare_and_swap(None, &initial).unwrap());
            let shell = Shell::new(&platform, &state, &[]);
            let mut request = Request {
                method: "POST".into(),
                path: "/api/setup/accept".into(),
                headers: BTreeMap::from([
                    ("x-flp-front-key".into(), "front-test-key-never-real".into()),
                    ("fastly-client-ip".into(), "192.0.2.1".into()),
                    ("origin".into(), "https://example.invalid".into()),
                    ("content-type".into(), "application/json".into()),
                    ("x-flp-front-rl".into(), "1".into()),
                ]),
                body: serde_json::to_vec(&json!({"code":code})).unwrap(),
            };
            let response = shell.handle(&request);
            assert_eq!(response.status, 401);
            assert!(!response.headers.contains_key("location"));
            let body: Value = serde_json::from_slice(&response.body).unwrap();
            assert!(
                body["detail"].as_str().unwrap().contains(explanation),
                "{body}"
            );
            assert!(body["detail"].as_str().unwrap().contains("minute"));
            if failures == 0 && expires_at > 100 {
                assert_eq!(
                    response.headers.get("x-flp-auth-fail").map(String::as_str),
                    Some("1")
                );
                request.body = serde_json::to_vec(&json!({"code":"setup-test"})).unwrap();
                assert_eq!(shell.handle(&request).status, 200);
            } else {
                assert!(body["detail"]
                    .as_str()
                    .unwrap()
                    .contains("make admin-reset"));
                assert!(body["detail"].as_str().unwrap().contains("15 minutes"));
            }
        }
    }

    #[test]
    fn direct_compute_rejects_every_route_even_with_spoofed_ip() {
        let platform = platform(true);
        let state = MemoryStore::default();
        let shell = Shell::new(&platform, &state, &[]);
        for path in [
            "/",
            "/setup",
            "/healthz",
            "/version",
            "/api/setup",
            "/_next/static/main.js",
            "/_tick",
            "/_flush",
        ] {
            let mut request = Request {
                method: "GET".into(),
                path: path.into(),
                ..Default::default()
            };
            request
                .headers
                .insert("fastly-client-ip".into(), "127.0.0.1".into());
            assert_eq!(shell.handle(&request).status, 403, "{path}");
        }
    }

    #[test]
    fn unauthenticated_probe_verifies_local_front_gate_and_single_hop_peer_status() {
        let platform = platform(true);
        assert_eq!(unauthenticated_probe_status(&platform, || Ok(403)), Ok(403));
        assert_eq!(unauthenticated_probe_status(&platform, || Ok(200)), Ok(200));
        assert_eq!(
            unauthenticated_probe_status(&platform, || Err("peer_probe_unavailable".into())),
            Err("peer_probe_unavailable".into())
        );
    }

    #[test]
    fn incomplete_install_only_returns_missing_instructions() {
        let platform = platform(false);
        let state = MemoryStore::default();
        let shell = Shell::new(&platform, &state, &[]);
        let mut request = Request {
            method: "GET".into(),
            path: "/api/setup".into(),
            ..Default::default()
        };
        request
            .headers
            .insert("x-flp-front-key".into(), "front-test-key-never-real".into());
        request
            .headers
            .insert("fastly-client-ip".into(), "192.0.2.1".into());
        let response = shell.handle(&request);
        assert_eq!(response.status, 200);
        let body: Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body["accepted"], false);
        assert!(!body["missing"].as_array().unwrap().is_empty());
        let text = String::from_utf8(response.body).unwrap();
        assert!(!text.contains("front-test-key-never-real"));
        assert!(!text.contains(&"s".repeat(48)));
    }

    #[test]
    fn public_information_assets_do_not_require_setup_or_session_bootstrap() {
        let assets = [
            Asset {
                path: "/how-it-works.html",
                content_type: "text/html",
                identity: Some(b"how-it-works"),
                gzip: None,
                br: None,
            },
            Asset {
                path: "/how-it-works.txt",
                content_type: "text/plain",
                identity: Some(b"how-it-works-rsc"),
                gzip: None,
                br: None,
            },
            Asset {
                path: "/usage.html",
                content_type: "text/html",
                identity: Some(b"usage"),
                gzip: None,
                br: None,
            },
            Asset {
                path: "/usage.txt",
                content_type: "text/plain",
                identity: Some(b"usage-rsc"),
                gzip: None,
                br: None,
            },
            Asset {
                path: "/usage/__next.usage.__PAGE__.txt",
                content_type: "text/plain",
                identity: Some(b"usage-segment"),
                gzip: None,
                br: None,
            },
            Asset {
                path: "/_next/static/public.js",
                content_type: "application/javascript",
                identity: Some(b"public-script"),
                gzip: None,
                br: None,
            },
            Asset {
                path: "/fastly.svg",
                content_type: "image/svg+xml",
                identity: Some(b"<svg/>"),
                gzip: None,
                br: None,
            },
        ];
        let state = MemoryStore::default();
        for complete in [false, true] {
            let mut platform = platform(complete);
            platform.config.clear();
            platform.secrets.retain(|name, _| name == "front_key");
            let shell = Shell::new(&platform, &state, &assets);
            for path in [
                "/how-it-works",
                "/how-it-works/",
                "/how-it-works.html",
                "/how-it-works.txt",
                "/usage",
                "/usage.html",
                "/usage.txt",
                "/usage/__next.usage.__PAGE__.txt",
                "/_next/static/public.js",
                "/fastly.svg",
            ] {
                for method in ["GET", "HEAD"] {
                    let mut request = Request {
                        method: method.into(),
                        path: path.into(),
                        headers: BTreeMap::from([
                            ("x-flp-front-key".into(), "front-test-key-never-real".into()),
                            ("cookie".into(), "flp_session=invalid-session".into()),
                        ]),
                        ..Default::default()
                    };
                    let response = shell.handle(&request);
                    assert_eq!(response.status, 200, "{complete} {method} {path}");
                    assert!(!response.headers.contains_key("set-cookie"));
                    request.headers.remove("x-flp-front-key");
                    assert_eq!(shell.handle(&request).status, 403, "{method} {path}");
                }
            }
            let private = Request {
                method: "GET".into(),
                path: "/usage/private".into(),
                headers: BTreeMap::from([(
                    "x-flp-front-key".into(),
                    "front-test-key-never-real".into(),
                )]),
                ..Default::default()
            };
            assert_ne!(shell.handle(&private).status, 200);
        }
    }

    #[test]
    fn setup_static_export_variants_and_logo_preserve_incomplete_and_post_setup_rules() {
        use crate::auth::{AuthState, SetupState};
        use crate::state::StateStore;

        let assets = [
            Asset {
                path: "/setup.html",
                content_type: "text/html; charset=utf-8",
                identity: Some(b"setup-html"),
                gzip: None,
                br: None,
            },
            Asset {
                path: "/setup.txt",
                content_type: "text/plain; charset=utf-8",
                identity: Some(b"setup-rsc"),
                gzip: None,
                br: None,
            },
            Asset {
                path: "/fastly.svg",
                content_type: "image/svg+xml",
                identity: Some(b"<svg/>"),
                gzip: None,
                br: None,
            },
        ];
        let incomplete_platform = platform(false);
        let state = MemoryStore::default();
        let incomplete_shell = Shell::new(&incomplete_platform, &state, &assets);
        for path in [
            "/setup",
            "/setup.html",
            "/setup.txt",
            "/fastly.svg",
            "/fastly.svg?v=1",
        ] {
            let request = Request {
                method: "GET".into(),
                path: path.into(),
                headers: BTreeMap::from([
                    ("x-flp-front-key".into(), "front-test-key-never-real".into()),
                    ("fastly-client-ip".into(), "192.0.2.1".into()),
                ]),
                ..Default::default()
            };
            let response = incomplete_shell.handle(&request);
            assert_eq!(response.status, 200, "incomplete {path}");
            if !path.starts_with("/fastly.svg") {
                assert_eq!(
                    response.headers.get("cache-control").map(String::as_str),
                    Some("private, no-store"),
                    "incomplete cache-control {path}"
                );
            }
        }

        let ready_platform = platform(true);
        let ready_shell = Shell::new(&ready_platform, &state, &assets);
        for path in ["/setup", "/setup.html", "/setup.txt"] {
            let request = Request {
                method: "GET".into(),
                path: path.into(),
                headers: BTreeMap::from([
                    ("x-flp-front-key".into(), "front-test-key-never-real".into()),
                    ("fastly-client-ip".into(), "192.0.2.1".into()),
                    ("x-flp-front-rl".into(), "1".into()),
                ]),
                ..Default::default()
            };
            let response = ready_shell.handle(&request);
            assert_eq!(response.status, 200, "pre-setup {path}");
            assert_eq!(
                response.headers.get("cache-control").map(String::as_str),
                Some("private, no-store"),
                "pre-setup cache-control {path}"
            );
        }

        let mut completed = AuthState {
            setup: SetupState {
                hash_generation: crate::auth::hash("setup-test"),
                spent: true,
                ..Default::default()
            },
            ..Default::default()
        };
        completed.users.insert(
            "admin".into(),
            User {
                id: "admin".into(),
                display_name: "Admin".into(),
                role: Role::Admin,
                credentials: vec![],
                revoked: false,
                created_by: "setup".into(),
                created_at: 99,
            },
        );
        assert!(state.compare_and_swap(None, &completed).unwrap());
        for path in ["/setup", "/setup.html", "/setup.txt"] {
            let request = Request {
                method: "GET".into(),
                path: path.into(),
                headers: BTreeMap::from([
                    ("x-flp-front-key".into(), "front-test-key-never-real".into()),
                    ("fastly-client-ip".into(), "192.0.2.1".into()),
                    ("x-flp-front-rl".into(), "1".into()),
                ]),
                ..Default::default()
            };
            let response = ready_shell.handle(&request);
            assert_eq!(response.status, 303, "post-setup {path}");
            assert_eq!(
                response.headers.get("location").map(String::as_str),
                Some("/admin"),
                "post-setup redirect {path}"
            );
        }
    }

    #[test]
    fn system_checks_require_a_setup_grant_and_private_api_responses() {
        let platform = platform(true);
        let state = MemoryStore::default();
        let shell = Shell::new(&platform, &state, &[]);
        let mut request = Request {
            method: "GET".into(),
            path: "/api/admin/system-checks".into(),
            ..Default::default()
        };
        request
            .headers
            .insert("x-flp-front-key".into(), "front-test-key-never-real".into());
        request
            .headers
            .insert("fastly-client-ip".into(), "192.0.2.1".into());
        let response = shell.handle(&request);
        assert_eq!(response.status, 401);
        assert_eq!(
            response.headers.get("cache-control").unwrap(),
            "private, no-store"
        );
    }

    #[test]
    fn signed_out_admin_navigation_uses_html_auth_flow_not_json_api_errors() {
        use crate::auth::{AuthState, SetupState};
        use crate::state::StateStore;

        let platform = platform(true);
        for complete in [false, true] {
            let state = MemoryStore::default();
            if complete {
                let mut initial = AuthState {
                    setup: SetupState {
                        hash_generation: crate::auth::hash("setup-test"),
                        spent: true,
                        ..Default::default()
                    },
                    ..Default::default()
                };
                initial.users.insert(
                    "admin".into(),
                    User {
                        id: "admin".into(),
                        display_name: "Existing admin".into(),
                        role: Role::Admin,
                        credentials: vec![],
                        revoked: false,
                        created_by: "setup".into(),
                        created_at: 99,
                    },
                );
                assert!(state.compare_and_swap(None, &initial).unwrap());
            }
            let assets = [
                Asset {
                    path: "/admin.html",
                    content_type: "text/html; charset=utf-8",
                    identity: Some(b"admin"),
                    gzip: None,
                    br: None,
                },
                Asset {
                    path: "/admin.txt",
                    content_type: "text/plain; charset=utf-8",
                    identity: Some(b"admin-rsc"),
                    gzip: None,
                    br: None,
                },
                Asset {
                    path: "/admin/tick.html",
                    content_type: "text/html; charset=utf-8",
                    identity: Some(b"tick"),
                    gzip: None,
                    br: None,
                },
                Asset {
                    path: "/logs.html",
                    content_type: "text/html; charset=utf-8",
                    identity: Some(b"logs"),
                    gzip: None,
                    br: None,
                },
            ];
            let shell = Shell::new(&platform, &state, &assets);
            for path in [
                "/admin",
                "/admin.html",
                "/admin.txt",
                "/admin/tick",
                "/logs",
                "/logs?tab=cron",
                "/api/admin/users",
            ] {
                for method in ["GET", "HEAD"] {
                    let request = Request {
                        method: method.into(),
                        path: path.into(),
                        headers: BTreeMap::from([
                            ("x-flp-front-key".into(), "front-test-key-never-real".into()),
                            ("fastly-client-ip".into(), "192.0.2.1".into()),
                            ("cookie".into(), "flp_session=expired-or-invalid".into()),
                        ]),
                        ..Default::default()
                    };
                    let response = shell.handle(&request);
                    if path.starts_with("/api/") {
                        if method == "GET" {
                            assert_eq!(response.status, 401);
                        }
                    } else {
                        assert_eq!(response.status, 303, "{method} {path}, complete={complete}");
                        assert_eq!(
                            response.headers.get("location").map(String::as_str),
                            Some(if complete { "/sign-in" } else { "/setup" })
                        );
                    }
                    assert_eq!(
                        response.headers.get("cache-control").map(String::as_str),
                        Some("private, no-store")
                    );
                }
            }
        }
    }

    #[test]
    fn fallback_rate_limit_on_options_does_not_perform_redundant_store_load() {
        use crate::state::{StateError, VersionedState};
        use std::cell::Cell;

        struct CountingStore {
            inner: MemoryStore,
            loads: Cell<u32>,
        }
        impl StateStore for CountingStore {
            fn load(&self) -> Result<VersionedState, StateError> {
                self.loads.set(self.loads.get() + 1);
                self.inner.load()
            }
            fn load_user(&self, id: &str) -> Result<Option<User>, StateError> {
                self.inner.load_user(id)
            }
            fn compare_and_swap(
                &self,
                generation: Option<&str>,
                next: &auth::AuthState,
            ) -> Result<bool, StateError> {
                self.inner.compare_and_swap(generation, next)
            }
        }

        let platform = platform(true);
        let state = CountingStore {
            inner: MemoryStore::default(),
            loads: Cell::new(0),
        };
        let shell = Shell::new(&platform, &state, &[]);
        let request = Request {
            method: "POST".into(),
            path: "/api/auth/login/options".into(),
            headers: BTreeMap::from([
                ("x-flp-front-key".into(), "front-test-key-never-real".into()),
                ("fastly-client-ip".into(), "192.0.2.1".into()),
                ("origin".into(), "https://example.invalid".into()),
                ("content-type".into(), "application/json".into()),
                ("x-flp-front-rl".into(), "0".into()),
            ]),
            body: b"{}".to_vec(),
        };
        let response = shell.handle(&request);
        assert_eq!(response.status, 200);
        assert_eq!(state.loads.get(), 2);
    }

    #[test]
    fn worker_compute_service_rejects_tick_while_coordinator_accepts_it() {
        let state = MemoryStore::default();
        let mut platform = platform(true);
        platform
            .config
            .insert("flp_service_id".into(), "svc-coordinator".into());
        platform
            .config
            .insert("worker_service_id".into(), "svc-worker".into());

        let tick_request = Request {
            method: "GET".into(),
            path: "/_tick".into(),
            headers: BTreeMap::from([(
                "x-flp-front-key".into(),
                "front-test-key-never-real".into(),
            )]),
            ..Default::default()
        };

        platform.service_id = Some("svc-worker".into());
        let worker_shell = Shell::new(&platform, &state, &[]);
        assert!(worker_shell.clock_ack(&tick_request).is_none());
        assert_eq!(worker_shell.handle(&tick_request).status, 403);

        platform.service_id = Some("svc-coordinator".into());
        let coord_shell = Shell::new(&platform, &state, &[]);
        let ack = coord_shell.clock_ack(&tick_request).expect("early 200");
        assert_eq!(ack.status, 200);
        assert_eq!(
            ack.headers.get("cache-control").map(String::as_str),
            Some("public, max-age=1")
        );
        assert!(!ack.headers.contains_key("vary"));
        for (method, path, key) in [
            ("GET", "/_tick", "wrong-key"),
            ("POST", "/_tick", "front-test-key-never-real"),
            ("GET", "/_tick/x", "front-test-key-never-real"),
            ("GET", "/api/sync-status", "front-test-key-never-real"),
        ] {
            let request = Request {
                method: method.into(),
                path: path.into(),
                headers: BTreeMap::from([("x-flp-front-key".into(), key.into())]),
                ..Default::default()
            };
            assert!(coord_shell.clock_ack(&request).is_none(), "{method} {path}");
        }
        let resp = coord_shell.handle(&tick_request);
        assert_eq!(resp.status, 200);
        assert_eq!(
            resp.headers.get("cache-control").map(String::as_str),
            Some("public, max-age=1")
        );

        let flush_request = Request {
            path: "/_flush".into(),
            ..tick_request
        };
        platform.service_id = Some("svc-worker".into());
        let worker_shell = Shell::new(&platform, &state, &[]);
        assert_eq!(worker_shell.handle(&flush_request).status, 403);
        platform.service_id = Some("svc-coordinator".into());
        let coord_shell = Shell::new(&platform, &state, &[]);
        assert!(coord_shell.clock_ack(&flush_request).is_some());
        let resp = coord_shell.handle(&flush_request);
        assert_eq!(resp.status, 200);
        assert_eq!(
            resp.headers.get("cache-control").map(String::as_str),
            Some("public, max-age=1")
        );
    }

    #[test]
    fn duckdb_wasm_serves_stored_encodings_and_refuses_identity_loudly() {
        let assets = [Asset {
            path: "/duckdb/1.0.0/duckdb-eh.wasm",
            content_type: "application/wasm",
            identity: None,
            gzip: Some(b"gz-wasm"),
            br: Some(b"br-wasm"),
        }];
        let state = MemoryStore::default();
        let platform = platform(false);
        let shell = Shell::new(&platform, &state, &assets);
        let request = |accept: Option<&str>| {
            let mut headers = BTreeMap::from([(
                "x-flp-front-key".to_owned(),
                "front-test-key-never-real".to_owned(),
            )]);
            if let Some(accept) = accept {
                headers.insert("accept-encoding".into(), accept.into());
            }
            Request {
                method: "GET".into(),
                path: "/duckdb/1.0.0/duckdb-eh.wasm".into(),
                headers,
                ..Default::default()
            }
        };

        for (accept, encoding, body) in [
            ("br", "br", &b"br-wasm"[..]),
            ("gzip", "gzip", &b"gz-wasm"[..]),
        ] {
            let response = shell.handle(&request(Some(accept)));
            assert_eq!(response.status, 200);
            assert_eq!(
                response.headers.get("content-type").map(String::as_str),
                Some("application/wasm")
            );
            assert_eq!(
                response.headers.get("content-encoding").map(String::as_str),
                Some(encoding)
            );
            assert_eq!(
                response.headers.get("vary").map(String::as_str),
                Some("Accept-Encoding")
            );
            assert_eq!(response.body, body);
        }

        let refused = shell.handle(&request(None));
        assert_eq!(refused.status, 406);
        assert!(!refused.headers.contains_key("content-encoding"));
        assert_eq!(
            refused.headers.get("vary").map(String::as_str),
            Some("Accept-Encoding")
        );
    }

    #[test]
    fn kv_cache_allowlist_covers_stable_config_keys_and_excludes_mutable_coordination_keys() {
        for key in [
            "settings:operational_logs",
            "admin:install",
            "svc:fixtureService",
        ] {
            assert!(is_cacheable_kv_key(key), "expected {key} to be cacheable");
        }
        for key in [
            "svc:",
            "cron:state",
            "ingest:plan",
            "ingest:plan:rum",
            "ingest:watermark",
            "ingest:watermark:rum",
            "ingest:backfill",
        ] {
            assert!(
                !is_cacheable_kv_key(key),
                "expected {key} to remain uncached"
            );
        }
    }
}
