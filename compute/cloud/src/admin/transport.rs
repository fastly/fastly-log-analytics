use super::{Error, Record, RetryState};
use crate::shell::Runtime;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, BTreeSet},
    io::Read,
    rc::Rc,
    time::{Duration, Instant},
};

pub const STEP_WALL_BUDGET: Duration = Duration::from_secs(45);
pub const CHECKPOINT_RESERVE: Duration = Duration::from_secs(2);
pub const FASTLY_API_TIMEOUT: Duration = Duration::from_secs(30);
pub const FOS_TIMEOUT: Duration = Duration::from_secs(60);
pub const FOS_RESPONSE_LIMIT: usize = 2 * 1024 * 1024;
/// One compaction checkpoint: 8 MiB part buffer + 16 MiB source + the footer limit + header.
/// Bounding the read and its restored copy leaves room within the unchanged 128 MiB heap.
pub const MAX_FOS_RESPONSE_LIMIT: usize =
    24 * 1024 * 1024 + crate::ingest::compaction::checkpoint::MAX_FOOTER_BYTES + 16;
pub const PEER_TIMEOUT: Duration = Duration::from_secs(60);
pub const STATE_CONFIRMATION_TIMEOUT: Duration = Duration::from_secs(2);
pub const REQUEST_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_API_ATTEMPTS: u8 = 4;
const RETRY_AFTER_CAP: Duration = Duration::from_secs(30);
// Secret Store writes reach Compute reads after a delay that was measured in the minute range.
const SECRET_PROPAGATION: &str = "secret_propagation";
const SECRET_PROPAGATION_DELAY: Duration = Duration::from_secs(10);
const SECRET_PROPAGATION_ATTEMPTS: u8 = 24;
// A KV REST API read can fail briefly; a few waits keep that from failing the whole job.
const STATE_READ: &str = "state_read";
const STATE_READ_DELAY: Duration = Duration::from_secs(5);
const STATE_READ_ATTEMPTS: u8 = 6;
const BACKEND_BUDGET_WAIT: &str = "backend_budget";
const BACKEND_BUDGET_DELAY: Duration = Duration::from_secs(1);
const BACKEND_BUDGET_ATTEMPTS: u8 = 24;

pub fn request_timeout(ceiling: Duration, elapsed: Duration) -> Duration {
    STEP_WALL_BUDGET
        .saturating_sub(CHECKPOINT_RESERVE)
        .saturating_sub(elapsed)
        .min(ceiling)
}

#[derive(Clone)]
pub struct BackendBudget(Rc<Cell<u32>>, Rc<Cell<(bool, u32)>>);

/// Calls a request may still make for its own telemetry after the main budget is spent.
const TELEMETRY_ALLOWANCE: u32 = 8;
pub fn kv_write_delay_ms(last: u64, now: u64) -> Result<u64, String> {
    let delay = last.saturating_add(1100).saturating_sub(now);
    Ok(delay.min(1100))
}

/// Picks the fresher of a KV REST API read (`api`) and a local Compute `KVStore::lookup` (`local`).
/// Prefers `local` when it carries a strictly newer `_kv_written_ms`, or when `api` returned a
/// known-dead generation and `local` has a different generation at least as new.
pub fn pick_fresher_record(api: Record, local: Option<Record>, dead_gen: Option<&str>) -> Record {
    let Some(local) = local else {
        return api;
    };
    let api_ts = api.value["_kv_written_ms"].as_u64().unwrap_or(0);
    let local_ts = local.value["_kv_written_ms"].as_u64().unwrap_or(0);
    if local_ts > api_ts
        || (dead_gen == Some(api.generation.as_str())
            && local.generation != api.generation
            && local_ts >= api_ts)
    {
        return local;
    }
    api
}

/// Whether `admin_cas` may heal a stuck local POP KV cache via `InsertMode::Overwrite` after
/// `ItemPreconditionFailed`.
///
/// Watermark keys (`ingest:watermark`, `ingest:rum:watermark`, etc.) hold the committed catalog
/// head pointer and have concurrent writers across POPs (ingest, compaction, summary_bundle,
/// retention). A blind `Overwrite` from a stale read rolls back the catalog head and permanently
/// orphans committed Parquet files whose raw logs have already been deleted, so watermark keys
/// must never use `InsertMode::Overwrite`. Other coordinated keys may only heal when this POP's
/// Core Cache actually recorded a prior write (`has_local_write`), so an unverified conflict
/// against another POP's commit never clobbers it.
pub fn allow_stale_pop_overwrite(
    key: &str,
    has_local_write: bool,
    dead_generation: Option<&str>,
    dead_since_ms: u64,
    tried_gen: &str,
    now_ms: u64,
) -> bool {
    if key == crate::ingest::WATERMARK_KEY || key.ends_with(":watermark") {
        return false;
    }
    has_local_write
        && dead_generation == Some(tried_gen)
        && dead_since_ms > 0
        && now_ms.saturating_sub(dead_since_ms) >= 2_000
}
impl Default for BackendBudget {
    fn default() -> Self {
        Self(Rc::new(Cell::new(0)), Rc::new(Cell::new((false, 0))))
    }
}
impl BackendBudget {
    pub fn used(&self) -> u32 {
        self.0.get() + self.1.get().1
    }
    /// While on, calls draw on a small separate allowance instead of the main budget.
    pub fn set_telemetry(&self, on: bool) {
        self.1.set((on, self.1.get().1));
    }
    pub fn charge(&self) -> Result<(), String> {
        let (telemetry, used) = self.1.get();
        if telemetry {
            if used >= TELEMETRY_ALLOWANCE {
                return Err("backend_budget_exhausted".into());
            }
            self.1.set((true, used + 1));
            return Ok(());
        }
        if self.0.get() >= 32 {
            return Err("backend_budget_exhausted".into());
        }
        self.0.set(self.0.get() + 1);
        Ok(())
    }
    /// While the telemetry allowance is on, what is left of it, so callers sizing their writes
    /// against the remaining count stay within the calls they can actually make.
    pub fn remaining(&self) -> u32 {
        match self.1.get() {
            (true, used) => TELEMETRY_ALLOWANCE.saturating_sub(used),
            _ => 32u32.saturating_sub(self.0.get()),
        }
    }
}

pub struct ApiRequest<'a> {
    pub method: &'a str,
    pub path: &'a str,
    /// Form-encoded query string without the `?`; empty for none.
    pub query: &'a str,
    pub token: &'a str,
    pub body: Option<Value>,
    pub timeout: Duration,
}
pub struct ApiRawRequest<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub token: &'a str,
    pub content_type: &'a str,
    pub body: &'a [u8],
    pub timeout: Duration,
}
pub struct ApiResponse {
    pub status: u16,
    pub body: Value,
    pub retry_after: Option<Duration>,
}
impl From<(u16, Value)> for ApiResponse {
    fn from((status, body): (u16, Value)) -> Self {
        Self {
            status,
            body,
            retry_after: None,
        }
    }
}

pub struct FosRequest<'a> {
    pub method: &'a str,
    pub region: &'a str,
    pub bucket: &'a str,
    pub key: &'a str,
    pub query: &'a str,
    pub access_key: &'a str,
    pub secret_key: &'a str,
    pub body: Vec<u8>,
    pub timeout: Duration,
    /// Local successful-response body bound, at most `MAX_FOS_RESPONSE_LIMIT`; not an S3 header.
    /// Error responses are also capped at `FOS_RESPONSE_LIMIT`.
    pub response_limit: usize,
    /// Base64 MD5 of `body` when the caller already has it; a PUT without one is hashed when signed.
    pub content_md5: Option<&'a str>,
}
pub struct FosResponse {
    pub status: u16,
    pub body: Vec<u8>,
    pub cdn_cache: Option<String>,
    pub retry_after: Option<Duration>,
}

pub fn read_fos_body(
    mut body: impl Read,
    status: u16,
    response_limit: usize,
    bytes_read: &mut usize,
) -> Result<Vec<u8>, String> {
    *bytes_read = 0;
    if response_limit > MAX_FOS_RESPONSE_LIMIT {
        return Err("fos_response_limit".into());
    }
    let limit = if (200..300).contains(&status) {
        response_limit
    } else {
        response_limit.min(FOS_RESPONSE_LIMIT)
    };
    let mut bytes = Vec::new();
    let mut buffer = [0; 8192];
    loop {
        let remaining = limit - bytes.len();
        let read_limit = buffer.len().min(remaining + 1);
        let count = match body.read(&mut buffer[..read_limit]) {
            Ok(count) => count,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => return Err("fos_body_unavailable".into()),
        };
        *bytes_read += count;
        if count == 0 {
            return Ok(bytes);
        }
        if count > remaining {
            return Err("fos_body_too_large".into());
        }
        if bytes.capacity() - bytes.len() < count {
            let capacity = bytes.capacity().saturating_mul(2).max(bytes.len() + count);
            bytes
                .try_reserve_exact(capacity.min(limit) - bytes.len())
                .map_err(|_| "fos_body_unavailable")?;
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
}

pub struct Context<'a, R: Runtime> {
    pub runtime: &'a R,
    started: Instant,
    calls: Cell<u32>,
    writes: RefCell<BTreeMap<String, WrittenRecord>>,
    secrets: RefCell<BTreeMap<String, Option<String>>>,
    confirmation: RefCell<Option<Confirmation>>,
    retry: RefCell<Option<RetryState>>,
    retry_attempts: RefCell<BTreeMap<String, u8>>,
    uncertain_operations: RefCell<BTreeSet<String>>,
    dispatch_journal: RefCell<Option<(String, Record)>>,
}
#[derive(Clone)]
enum Confirmation {
    Token(String),
    Secret(String),
}
#[derive(Clone)]
enum WrittenRecord {
    Pending {
        previous: Option<String>,
        value: Value,
    },
    Confirmed(Record),
}
impl<'a, R: Runtime> Context<'a, R> {
    pub fn new(runtime: &'a R) -> Self {
        Self {
            runtime,
            started: Instant::now(),
            calls: Cell::new(0),
            writes: RefCell::new(BTreeMap::new()),
            secrets: RefCell::new(BTreeMap::new()),
            confirmation: RefCell::new(None),
            retry: RefCell::new(None),
            retry_attempts: RefCell::new(BTreeMap::new()),
            uncertain_operations: RefCell::new(BTreeSet::new()),
            dispatch_journal: RefCell::new(None),
        }
    }
    pub fn confirm_with_token(&self, token: &str) {
        *self.confirmation.borrow_mut() = Some(Confirmation::Token(token.into()));
    }
    pub fn set_retry(&self, retry: Option<RetryState>) {
        // These waits end when their delay elapses; the next run reads again rather than replaying.
        *self.retry.borrow_mut() = retry.filter(|retry| {
            !matches!(
                retry.operation.as_str(),
                SECRET_PROPAGATION | STATE_READ | BACKEND_BUDGET_WAIT
            )
        });
    }
    fn wait_retry(
        &self,
        operation: &str,
        delay: Duration,
        attempts: u8,
        reason: &str,
    ) -> RetryState {
        let attempt = self
            .retry_attempts
            .borrow()
            .get(operation)
            .copied()
            .unwrap_or(0)
            .saturating_add(1);
        self.retry_attempts
            .borrow_mut()
            .insert(operation.into(), attempt);
        RetryState {
            operation: operation.into(),
            attempt,
            at: if attempt >= attempts {
                0
            } else {
                self.runtime.now() + delay.as_secs()
            },
            reason: reason.into(),
        }
    }
    pub fn secret_propagation_retry(&self) -> RetryState {
        self.wait_retry(
            SECRET_PROPAGATION,
            SECRET_PROPAGATION_DELAY,
            SECRET_PROPAGATION_ATTEMPTS,
            "credentials_pending",
        )
    }
    pub fn state_read_retry(&self, reason: &str) -> RetryState {
        self.wait_retry(STATE_READ, STATE_READ_DELAY, STATE_READ_ATTEMPTS, reason)
    }
    pub fn backend_budget_retry(&self) -> RetryState {
        self.wait_retry(
            BACKEND_BUDGET_WAIT,
            BACKEND_BUDGET_DELAY,
            BACKEND_BUDGET_ATTEMPTS,
            "backend_budget",
        )
    }
    pub fn settle_waits(&self) {
        let mut attempts = self.retry_attempts.borrow_mut();
        attempts.remove(SECRET_PROPAGATION);
        attempts.remove(STATE_READ);
        attempts.remove(BACKEND_BUDGET_WAIT);
    }
    pub fn retry(&self) -> Option<RetryState> {
        self.retry.borrow().clone()
    }
    pub fn set_retry_attempts(&self, attempts: BTreeMap<String, u8>) {
        *self.retry_attempts.borrow_mut() = attempts;
    }
    pub fn retry_attempts(&self) -> BTreeMap<String, u8> {
        self.retry_attempts.borrow().clone()
    }
    pub fn set_uncertain_operations(&self, operations: BTreeSet<String>) {
        *self.uncertain_operations.borrow_mut() = operations;
    }
    pub fn uncertain_operations(&self) -> BTreeSet<String> {
        self.uncertain_operations.borrow().clone()
    }
    pub fn journal_job(&self, key: String, record: Record) {
        *self.dispatch_journal.borrow_mut() = Some((key, record));
    }
    pub fn journal_generation(&self) -> Option<String> {
        self.dispatch_journal
            .borrow()
            .as_ref()
            .map(|(_, record)| record.generation.clone())
    }
    pub fn checkpoint_generation(&self, key: &str, fallback: &str) -> String {
        if matches!(
            self.writes.borrow().get(key),
            Some(WrittenRecord::Pending { .. })
        ) {
            if let Ok(Some(confirmed)) = self.get(key) {
                return confirmed.generation;
            }
        }
        self.journal_generation()
            .unwrap_or_else(|| fallback.to_owned())
    }
    fn record_dispatch(&self, operation: &str) -> Result<(), Error> {
        let Some((key, mut record)) = self.dispatch_journal.borrow().clone() else {
            return Ok(());
        };
        let mut uncertain = self.uncertain_operations();
        uncertain.insert(operation.into());
        record.value["uncertain_operations"] = serde_json::to_value(&uncertain)
            .map_err(|_| Error::new(500, "state_encoding_failed"))?;
        record.value["retry_attempts"] = serde_json::to_value(self.retry_attempts())
            .map_err(|_| Error::new(500, "state_encoding_failed"))?;
        self.cas(&key, Some(&record.generation), &record.value)?;
        let confirmed = self
            .get(&key)?
            .ok_or_else(|| Error::new(503, "state_unavailable"))?;
        *self.uncertain_operations.borrow_mut() = uncertain;
        *self.dispatch_journal.borrow_mut() = Some((key, confirmed));
        Ok(())
    }
    pub fn api_uncertain(&self, method: &str, path: &str, body: Option<&Value>) -> bool {
        self.uncertain_operations
            .borrow()
            .contains(&operation_id(method, path, body))
    }
    pub fn discard_uncertainty(&self, method: &str, path: &str, body: Option<&Value>) {
        self.uncertain_operations
            .borrow_mut()
            .remove(&operation_id(method, path, body));
    }
    pub fn confirm_api_effect(&self, method: &str, path: &str, body: Option<&Value>) {
        let operation = operation_id(method, path, body);
        if self.uncertain_operations.borrow_mut().remove(&operation) {
            self.event(serde_json::json!({"event":"admin.external.confirmed","level":"info","operation":operation,"method":method}));
        }
        self.retry_attempts.borrow_mut().remove(&operation);
    }
    fn api_time(&self) -> Duration {
        request_timeout(FASTLY_API_TIMEOUT, self.runtime.admin_elapsed(self.started))
    }
    fn retry_error(
        &self,
        operation: &str,
        attempt: u8,
        delay: Duration,
        code: &'static str,
    ) -> Error {
        let at = if attempt >= MAX_API_ATTEMPTS {
            0
        } else {
            self.runtime.now() + delay.as_secs() + u64::from(delay.subsec_nanos() > 0)
        };
        Error {
            status: if code == "wall_time_budget" { 503 } else { 502 },
            code,
            retry: Some(RetryState {
                operation: operation.into(),
                attempt,
                at,
                reason: code.into(),
            }),
        }
    }
    pub fn confirm_with_secret(&self, name: &str) {
        if self.confirmation.borrow().is_none() {
            *self.confirmation.borrow_mut() = Some(Confirmation::Secret(name.into()));
        }
    }
    fn confirmation_token(&self) -> Result<String, Error> {
        let credential = self
            .confirmation
            .borrow()
            .clone()
            .ok_or_else(|| Error::new(503, "state_confirmation_credentials_missing"))?;
        match credential {
            Confirmation::Token(token) => Ok(token),
            Confirmation::Secret(name) => {
                let token = self.secret(&name)?;
                self.confirm_with_token(&token);
                Ok(token)
            }
        }
    }
    fn call_cap(&self) -> u32 {
        // Background job steps reserve 8 of Fastly Compute's 32 backend calls for
        // checkpointing and lease release; one-shot synchronous endpoints (such as DR
        // recovery) have no job checkpoint/lease and may use up to 30 of the 32 calls.
        if self.dispatch_journal.borrow().is_some() {
            24
        } else {
            30
        }
    }
    pub fn check(&self) -> Result<(), Error> {
        if self.runtime.admin_elapsed(self.started) >= STEP_WALL_BUDGET {
            return Err(Error::new(503, "wall_time_budget"));
        }
        if self.calls.get() >= self.call_cap() || self.runtime.admin_remaining_calls() == 0 {
            return Err(Error::new(503, "backend_budget"));
        }
        Ok(())
    }
    pub fn remaining_calls(&self) -> u32 {
        self.call_cap()
            .saturating_sub(self.calls.get())
            .min(self.runtime.admin_remaining_calls())
    }
    fn charge(&self) -> Result<(), Error> {
        self.check()?;
        self.calls.set(self.calls.get() + 1);
        Ok(())
    }
    pub fn get(&self, key: &str) -> Result<Option<Record>, Error> {
        let written = self.writes.borrow().get(key).cloned();
        if let Some(WrittenRecord::Confirmed(record)) = written {
            return Ok(Some(record));
        }
        let token = if written.is_some() {
            Some(self.confirmation_token()?)
        } else {
            None
        };
        let mut conflict = false;
        for attempt in 0..3 {
            self.charge()?;
            let record = match &token {
                Some(token) => self.runtime.admin_get_fresh(key, token),
                None => self.runtime.admin_get(key),
            }
            .map_err(|_| Error::new(503, "state_unavailable"))?;
            let Some(WrittenRecord::Pending { previous, value }) = &written else {
                return Ok(record);
            };
            if let Some(record) = record {
                let newer = match (
                    previous.as_deref().and_then(|p| p.parse::<u64>().ok()),
                    record.generation.parse::<u64>().ok(),
                ) {
                    (Some(prev), Some(curr)) => curr > prev,
                    _ => Some(&record.generation) != previous.as_ref(),
                };
                if newer {
                    let mut observed = record.value.clone();
                    let mut expected = value.clone();
                    for value in [&mut observed, &mut expected] {
                        if let Some(object) = value.as_object_mut() {
                            object.remove("_kv_written_ms");
                        }
                    }
                    if observed == expected {
                        self.writes
                            .borrow_mut()
                            .insert(key.into(), WrittenRecord::Confirmed(record.clone()));
                        return Ok(Some(record));
                    }
                    conflict = true;
                }
            }
            if attempt < 2 {
                self.check()?;
                self.runtime.admin_wait(500 * (attempt + 1));
            }
        }
        Err(if conflict {
            Error::new(409, "generation_conflict")
        } else {
            Error::new(503, "state_propagating")
        })
    }
    /// Reads past the edge read cache, which can hold a record read inside KV's propagation window.
    pub fn get_confirmed(&self, key: &str) -> Result<Option<Record>, Error> {
        if self.writes.borrow().contains_key(key) {
            return self.get(key);
        }
        let token = self.confirmation_token()?;
        self.charge()?;
        self.runtime
            .admin_get_fresh(key, &token)
            .map_err(|_| Error::new(503, "state_unavailable"))
    }
    pub fn cas(&self, key: &str, generation: Option<&str>, value: &Value) -> Result<(), Error> {
        self.charge()?;
        let mut value = value.clone();
        let nonce = self
            .runtime
            .random()
            .map_err(|_| Error::new(503, "random_unavailable"))?;
        value
            .as_object_mut()
            .ok_or_else(|| Error::new(500, "state_encoding_failed"))?
            .insert("_kv_write_id".into(), URL_SAFE_NO_PAD.encode(nonce).into());
        if self
            .runtime
            .admin_cas(key, generation, &value)
            .map_err(|_| Error::new(503, "state_write_failed"))?
        {
            self.writes.borrow_mut().insert(
                key.into(),
                WrittenRecord::Pending {
                    previous: generation.map(str::to_owned),
                    value,
                },
            );
            Ok(())
        } else {
            Err(Error::new(409, "generation_conflict"))
        }
    }
    pub fn secret(&self, name: &str) -> Result<String, Error> {
        if let Some(cached) = self.secrets.borrow().get(name).cloned() {
            return cached.ok_or_else(|| Error::new(409, "credentials_pending"));
        }
        self.charge()?;
        let found = self
            .runtime
            .secret(name)
            .map_err(|_| Error::new(503, "secret_unavailable"))?
            .filter(|value| !value.is_empty());
        self.secrets
            .borrow_mut()
            .insert(name.to_owned(), found.clone());
        found.ok_or_else(|| Error::new(409, "credentials_pending"))
    }
    pub fn cache_secret(&self, name: &str, value: &str) {
        let normalized = (!value.is_empty()).then(|| value.to_owned());
        self.secrets
            .borrow_mut()
            .insert(name.to_owned(), normalized);
    }
    pub fn invalidate_secret(&self, name: &str) {
        self.secrets.borrow_mut().remove(name);
    }
    pub fn api(
        &self,
        method: &str,
        path: &str,
        token: &str,
        body: Option<Value>,
    ) -> Result<Value, Error> {
        let (status, value) = self.api_status(method, path, token, body)?;
        if (200..300).contains(&status) {
            Ok(value)
        } else {
            Err(Error::new(
                if status == 401 || status == 403 {
                    403
                } else {
                    502
                },
                "fastly_api_rejected",
            ))
        }
    }
    pub fn api_raw(
        &self,
        method: &str,
        path: &str,
        token: &str,
        content_type: &str,
        body: &[u8],
    ) -> Result<Value, Error> {
        let timeout = request_timeout(FOS_TIMEOUT, self.started.elapsed());
        if timeout.is_zero() {
            return Err(Error::new(503, "wall_time_budget"));
        }
        let response = self
            .runtime
            .admin_api_raw(ApiRawRequest {
                method,
                path,
                token,
                content_type,
                body,
                timeout,
            })
            .map_err(|code| Error::new(502, fastly_transport_error(&code)))?;
        if (200..300).contains(&response.status) {
            Ok(response.body)
        } else if response.status == 401 || response.status == 403 {
            Err(Error::new(403, "fastly_token_scope_insufficient"))
        } else {
            Err(Error::new(
                if (400..500).contains(&response.status) {
                    400
                } else {
                    502
                },
                "fastly_api_rejected",
            ))
        }
    }
    pub fn api_status(
        &self,
        method: &str,
        path: &str,
        token: &str,
        body: Option<Value>,
    ) -> Result<(u16, Value), Error> {
        let operation = operation_id(method, path, body.as_ref());
        // Every DELETE caller accepts 404, so replaying an unrecorded delete is safe.
        if method != "DELETE" && self.uncertain_operations.borrow().contains(&operation) {
            self.event(serde_json::json!({"event":"admin.external.uncertain","level":"error","operation":operation,"method":method,"reason":"mutation_outcome_unknown"}));
            return Err(Error::new(409, "mutation_outcome_unknown"));
        }
        let mut attempt = self
            .retry_attempts
            .borrow()
            .get(&operation)
            .copied()
            .or_else(|| {
                self.retry
                    .borrow()
                    .as_ref()
                    .filter(|retry| retry.operation == operation)
                    .map(|retry| retry.attempt)
            })
            .unwrap_or(0);
        loop {
            if attempt >= MAX_API_ATTEMPTS {
                return Err(self.retry_error(
                    &operation,
                    attempt,
                    Duration::ZERO,
                    "fastly_retry_exhausted",
                ));
            }
            if !matches!(method, "GET" | "HEAD")
                && self.dispatch_journal.borrow().is_some()
                && (self.calls.get() + 1 + 4 > 24 || self.runtime.admin_remaining_calls() < 5)
            {
                return Err(self.retry_error(
                    &operation,
                    attempt,
                    Duration::from_secs(1),
                    "backend_budget",
                ));
            }
            self.charge()?;
            // The final two seconds remain available to checkpoint and release the lease.
            let timeout = self.api_time();
            if timeout.is_zero() {
                return Err(self.retry_error(
                    &operation,
                    attempt,
                    Duration::from_secs(1),
                    "wall_time_budget",
                ));
            }
            attempt += 1;
            self.retry_attempts
                .borrow_mut()
                .insert(operation.clone(), attempt);
            if !matches!(method, "GET" | "HEAD") {
                if let Err(err) = self.record_dispatch(&operation) {
                    if attempt <= 1 {
                        self.retry_attempts.borrow_mut().remove(&operation);
                    } else {
                        self.retry_attempts
                            .borrow_mut()
                            .insert(operation.clone(), attempt - 1);
                    }
                    return Err(if err.status >= 500 {
                        self.retry_error(
                            &operation,
                            attempt.saturating_sub(1),
                            Duration::from_secs(1),
                            err.code,
                        )
                    } else {
                        err
                    });
                }
            }
            let timeout = self.api_time();
            if timeout.is_zero() {
                self.uncertain_operations.borrow_mut().remove(&operation);
                if attempt <= 1 {
                    self.retry_attempts.borrow_mut().remove(&operation);
                } else {
                    self.retry_attempts
                        .borrow_mut()
                        .insert(operation.clone(), attempt - 1);
                }
                return Err(self.retry_error(
                    &operation,
                    attempt.saturating_sub(1),
                    Duration::from_secs(1),
                    "wall_time_budget",
                ));
            }
            let response = self.runtime.admin_api(ApiRequest {
                method,
                path,
                query: "",
                token,
                body: body.clone(),
                timeout,
            });
            let (code, hint) = match response {
                Ok(response) if matches!(response.status, 429 | 500 | 502 | 503 | 504) => {
                    if response.status == 429 {
                        self.uncertain_operations.borrow_mut().remove(&operation);
                    } else if !matches!(method, "GET" | "HEAD") {
                        self.uncertain_operations
                            .borrow_mut()
                            .insert(operation.clone());
                    }
                    (
                        if response.status == 429 {
                            "fastly_api_rate_limited"
                        } else {
                            "fastly_api_unavailable"
                        },
                        if method == "GET" && matches!(response.status, 429 | 503) {
                            response.retry_after
                        } else {
                            None
                        },
                    )
                }
                Ok(response) => {
                    self.uncertain_operations.borrow_mut().remove(&operation);
                    self.retry_attempts.borrow_mut().remove(&operation);
                    if self
                        .retry
                        .borrow()
                        .as_ref()
                        .is_some_and(|retry| retry.operation == operation)
                    {
                        self.set_retry(None);
                    }
                    return Ok((response.status, response.body));
                }
                Err(error) => {
                    if !matches!(method, "GET" | "HEAD") {
                        self.uncertain_operations
                            .borrow_mut()
                            .insert(operation.clone());
                    }
                    let code = fastly_transport_error(&error);
                    if !retryable_transport(&error) {
                        return Err(Error::new(502, code));
                    }
                    (code, None)
                }
            };
            let delay = if attempt >= MAX_API_ATTEMPTS {
                Duration::ZERO
            } else if let Some(hint) = hint {
                hint.min(RETRY_AFTER_CAP)
            } else {
                let random = self
                    .runtime
                    .random()
                    .map_err(|_| Error::new(503, "random_unavailable"))?;
                let jitter = u64::from_be_bytes(
                    random[..8]
                        .try_into()
                        .map_err(|_| Error::new(503, "random_unavailable"))?,
                ) % 2001;
                Duration::from_millis(
                    (1000u64 << attempt.saturating_sub(1).min(3)).min(8000) + jitter,
                )
            };
            let failure = self.retry_error(&operation, attempt, delay, code);
            let deferred = attempt >= MAX_API_ATTEMPTS
                || !matches!(method, "GET" | "HEAD")
                || delay >= self.api_time()
                || self.calls.get() + 6 >= 24
                || self.runtime.admin_remaining_calls() <= 6;
            self.event(serde_json::json!({"event":"admin.external.retry","level":"warn","operation":operation,"method":method,"attempt":attempt,"reason":code,"delay_ms":delay.as_millis(),"deferred":deferred}));
            if deferred {
                return Err(failure);
            }
            self.runtime.admin_wait(
                u64::try_from(delay.as_millis())
                    .map_err(|_| Error::new(503, "wall_time_budget"))?,
            );
        }
    }
    pub fn fos(&self, mut request: FosRequest<'_>) -> Result<FosResponse, Error> {
        request.timeout = request_timeout(
            request.timeout.min(FOS_TIMEOUT),
            self.runtime.admin_elapsed(self.started),
        );
        if request.timeout.is_zero() {
            let operation = operation_id(
                request.method,
                &format!(
                    "fos://{}/{}/{}?{}",
                    request.region, request.bucket, request.key, request.query
                ),
                None,
            );
            return Err(self.retry_error(
                &operation,
                0,
                Duration::from_secs(1),
                "wall_time_budget",
            ));
        }
        self.charge()?;
        self.runtime.admin_fos(request).map_err(|error| {
            Error::new(
                502,
                if error == "backend_deadline" {
                    "object_storage_backend_deadline"
                } else {
                    "object_storage_transport_failed"
                },
            )
        })
    }
    pub fn event(&self, event: Value) {
        self.runtime.admin_event(event);
    }
}

fn operation_id(method: &str, path: &str, body: Option<&Value>) -> String {
    let name = body.and_then(|body| body["name"].as_str()).unwrap_or("");
    let description = body
        .and_then(|body| body["description"].as_str())
        .unwrap_or("");
    // Resource identity is safe to hash; request payloads may contain credentials.
    hex::encode(Sha256::digest(
        format!("{method} {path}\n{name}\n{description}").as_bytes(),
    ))
}

pub fn parse_retry_after(value: Option<&str>) -> Option<Duration> {
    let seconds = value?.trim().parse::<f64>().ok()?;
    if seconds.is_nan() || seconds < 0.0 {
        return None;
    }
    Some(Duration::from_secs_f64(
        seconds.min(RETRY_AFTER_CAP.as_secs_f64()),
    ))
}
pub(crate) fn retryable_transport(error: &str) -> bool {
    matches!(
        error,
        "backend_deadline"
            | "backend_send_failed"
            | "backend_poll_failed"
            | "backend_dns_timeout"
            | "backend_dns_error"
            | "backend_not_found"
            | "backend_unavailable"
            | "backend_unroutable"
            | "backend_connection_refused"
            | "backend_connection_terminated"
            | "backend_connection_timeout"
            | "backend_connection_limit"
            | "backend_tls_protocol_error"
            | "backend_tls_certificate_error"
            | "backend_tls_alert"
            | "backend_tls_configuration_error"
            | "backend_response_timeout"
            | "backend_incomplete_response"
            | "backend_io_error"
            | "backend_internal_error"
            | "backend_body_unavailable"
    )
}

const URI_UNRESERVED: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

pub fn encode(value: &str) -> String {
    percent_encoding::utf8_percent_encode(value, URI_UNRESERVED).to_string()
}
pub fn list(value: &Value) -> Result<&Vec<Value>, Error> {
    value
        .as_array()
        .or_else(|| value["data"].as_array())
        .ok_or_else(|| Error::new(502, "invalid_fastly_response"))
}

fn fastly_transport_error(error: &str) -> &'static str {
    match error {
        "backend_deadline" => "fastly_backend_deadline",
        "backend_send_failed" => "fastly_backend_send_failed",
        "backend_poll_failed" => "fastly_backend_poll_failed",
        "backend_dns_timeout" => "fastly_backend_dns_timeout",
        "backend_dns_error" => "fastly_backend_dns_error",
        "backend_not_found" => "fastly_backend_not_found",
        "backend_unavailable" => "fastly_backend_unavailable",
        "backend_unroutable" => "fastly_backend_unroutable",
        "backend_connection_refused" => "fastly_backend_connection_refused",
        "backend_connection_terminated" => "fastly_backend_connection_terminated",
        "backend_connection_timeout" => "fastly_backend_connection_timeout",
        "backend_connection_limit" => "fastly_backend_connection_limit",
        "backend_tls_protocol_error" => "fastly_backend_tls_protocol_error",
        "backend_tls_certificate_error" => "fastly_backend_tls_certificate_error",
        "backend_tls_alert" => "fastly_backend_tls_alert",
        "backend_tls_configuration_error" => "fastly_backend_tls_configuration_error",
        "backend_response_timeout" => "fastly_backend_response_timeout",
        "backend_incomplete_response" => "fastly_backend_incomplete_response",
        "backend_response_limit" => "fastly_backend_response_limit",
        "backend_http_protocol_error" => "fastly_backend_http_protocol_error",
        "backend_io_error" => "fastly_backend_io_error",
        "backend_internal_error" => "fastly_backend_internal_error",
        "backend_body_unavailable" => "fastly_backend_body_unavailable",
        "backend_response_too_large" => "fastly_backend_response_too_large",
        "backend_json_invalid" => "fastly_backend_json_invalid",
        "backend_budget_exhausted" => "fastly_backend_budget_exhausted",
        "api_path_invalid" => "fastly_api_path_invalid",
        "invalid_api_body" => "fastly_invalid_api_body",
        _ => "fastly_transport_failed",
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Read};

    #[test]
    fn fos_response_reader_accepts_exact_limits_and_rejects_one_over() {
        for limit in [0, 31, 2 * 1024 * 1024, 24 * 1024 * 1024 + 256 * 1024 + 16] {
            let mut read = 0;
            let bytes =
                super::read_fos_body(io::repeat(0xa5).take(limit as u64), 200, limit, &mut read)
                    .unwrap();
            assert_eq!(bytes.len(), limit);
            assert!(bytes.iter().all(|&b| b == 0xa5));
            assert_eq!(read, limit);

            let mut body = io::repeat(0xa5).take(limit as u64 + 2);
            assert_eq!(
                super::read_fos_body(&mut body, 200, limit, &mut read).unwrap_err(),
                "fos_body_too_large"
            );
            assert_eq!(read, limit + 1);
            assert_eq!(body.limit(), 1);
        }
    }

    #[test]
    fn fos_response_reader_keeps_error_bodies_within_the_control_budget() {
        for status in [400, 404, 429, 503] {
            let mut body = io::repeat(b'x').take(3 * 1024 * 1024);
            let mut read = 0;
            assert_eq!(
                super::read_fos_body(&mut body, status, 24 * 1024 * 1024, &mut read).unwrap_err(),
                "fos_body_too_large"
            );
            assert_eq!(read, 2 * 1024 * 1024 + 1);
            assert_eq!(body.limit(), 1024 * 1024 - 1);
        }
    }

    #[test]
    fn fos_response_reader_rejects_invalid_limits_without_reading() {
        for limit in [super::MAX_FOS_RESPONSE_LIMIT + 1, usize::MAX] {
            let mut body = io::repeat(b'x').take(1);
            let mut read = 0;
            assert_eq!(
                super::read_fos_body(&mut body, 200, limit, &mut read).unwrap_err(),
                "fos_response_limit"
            );
            assert_eq!(read, 0);
            assert_eq!(body.limit(), 1);
        }
    }

    #[test]
    fn fos_response_reader_counts_partial_reads_without_exposing_io_errors() {
        struct Failed;
        impl Read for Failed {
            fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::other("private backend"))
            }
        }
        let mut body = io::Cursor::new(b"abc").chain(Failed);
        let mut read = 0;
        assert_eq!(
            super::read_fos_body(&mut body, 200, 8, &mut read).unwrap_err(),
            "fos_body_unavailable"
        );
        assert_eq!(read, 3);
    }

    #[test]
    fn remaining_reports_the_telemetry_allowance_while_it_is_on() {
        let budget = super::BackendBudget::default();
        budget.charge().unwrap();
        budget.set_telemetry(true);
        budget.charge().unwrap();
        budget.charge().unwrap();
        assert_eq!(budget.remaining(), 6);
        assert_eq!(budget.used(), 3);
        budget.set_telemetry(false);
        assert_eq!(budget.remaining(), 31);
        assert_eq!(budget.used(), 3);
    }

    use super::{fastly_transport_error, parse_retry_after};

    #[test]
    fn retry_after_matches_fla_delay_seconds_and_cap() {
        for (value, expected) in [
            ("0", Some(0)),
            (" 2.5 ", Some(2500)),
            ("60", Some(30000)),
            ("inf", Some(30000)),
            ("-1", None),
            ("NaN", None),
            ("", None),
            ("Thu, 29 Oct 2026 10:00:00 GMT", None),
        ] {
            assert_eq!(
                parse_retry_after(Some(value)).map(|delay| delay.as_millis()),
                expected
            );
        }
    }

    #[test]
    fn api_failure_categories_preserve_safe_runtime_diagnostics() {
        for (error, expected) in [
            ("backend_deadline", "fastly_backend_deadline"),
            ("backend_dns_timeout", "fastly_backend_dns_timeout"),
            (
                "backend_tls_certificate_error",
                "fastly_backend_tls_certificate_error",
            ),
            ("backend_json_invalid", "fastly_backend_json_invalid"),
            (
                "backend_body_unavailable",
                "fastly_backend_body_unavailable",
            ),
            (
                "backend_budget_exhausted",
                "fastly_backend_budget_exhausted",
            ),
        ] {
            assert_eq!(fastly_transport_error(error), expected);
        }
    }

    #[test]
    fn unknown_runtime_diagnostics_never_expose_messages_or_credentials() {
        for error in [
            "https://private.example/path?token=do-not-expose",
            "backend_deadline token=do-not-expose",
            "backend_unrecognized_response_payload",
        ] {
            assert_eq!(fastly_transport_error(error), "fastly_transport_failed");
        }
    }

    #[test]
    fn kv_write_delay_clamps_cross_pop_clock_skew_to_one_pacing_interval() {
        assert_eq!(super::kv_write_delay_ms(1_000, 2_100).unwrap(), 0);
        assert_eq!(super::kv_write_delay_ms(1_000, 1_500).unwrap(), 600);
        assert_eq!(super::kv_write_delay_ms(1_000, 1_000).unwrap(), 1_100);
        // Cross-POP clock skew where `now < last` clamps to 1,100ms instead of failing.
        assert_eq!(super::kv_write_delay_ms(1_000, 995).unwrap(), 1_100);
        assert_eq!(super::kv_write_delay_ms(10_000, 0).unwrap(), 1_100);
    }

    #[test]
    fn pick_fresher_record_prefers_newer_local_kv_over_stale_api_generation() {
        let stale_api = super::Record {
            generation: "1795643318773026".into(),
            value: serde_json::json!({"lease_until": 100, "_kv_written_ms": 1_000}),
        };
        let fresh_local = super::Record {
            generation: "1795643350000001".into(),
            value: serde_json::json!({"lease_until": 0, "_kv_written_ms": 2_000}),
        };
        let picked = super::pick_fresher_record(stale_api.clone(), Some(fresh_local.clone()), None);
        assert_eq!(picked.generation, fresh_local.generation);

        let newer_api = super::Record {
            generation: "1795643390000002".into(),
            value: serde_json::json!({"lease_until": 0, "_kv_written_ms": 3_000}),
        };
        let picked = super::pick_fresher_record(newer_api.clone(), Some(fresh_local.clone()), None);
        assert_eq!(picked.generation, newer_api.generation);

        let same_ts_local = super::Record {
            generation: "1795643350000009".into(),
            value: serde_json::json!({"lease_until": 0, "_kv_written_ms": 1_000}),
        };
        let picked = super::pick_fresher_record(
            stale_api,
            Some(same_ts_local.clone()),
            Some("1795643318773026"),
        );
        assert_eq!(picked.generation, same_ts_local.generation);
    }

    #[test]
    fn allow_stale_pop_overwrite_blocks_watermarks_and_unverified_cross_pop_conflicts() {
        // Never allow blind Overwrite on request or RUM watermark keys.
        assert!(!super::allow_stale_pop_overwrite(
            crate::ingest::WATERMARK_KEY,
            true,
            Some("100"),
            1_000,
            "100",
            5_000,
        ));
        assert!(!super::allow_stale_pop_overwrite(
            "ingest:rum:watermark",
            true,
            Some("100"),
            1_000,
            "100",
            5_000,
        ));
        // Never allow Overwrite on other keys without a local POP write in coord cache.
        assert!(!super::allow_stale_pop_overwrite(
            crate::ingest::compaction::STATE_KEY,
            false,
            Some("100"),
            1_000,
            "100",
            5_000,
        ));
        // Allow Overwrite on non-watermark coordinated keys when local write + >=2s dead gen.
        assert!(super::allow_stale_pop_overwrite(
            crate::ingest::compaction::STATE_KEY,
            true,
            Some("100"),
            1_000,
            "100",
            3_000,
        ));
        // Too soon (< 2s) still waits.
        assert!(!super::allow_stale_pop_overwrite(
            crate::ingest::compaction::STATE_KEY,
            true,
            Some("100"),
            1_000,
            "100",
            2_999,
        ));
    }
}
