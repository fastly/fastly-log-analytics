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
/// One compaction checkpoint: 8 MiB part buffer + 16 MiB source + 256 KiB footer + header.
/// Bounding the read and its restored copy leaves room within the unchanged 128 MiB heap.
pub const MAX_FOS_RESPONSE_LIMIT: usize = 24 * 1024 * 1024 + 256 * 1024 + 16;
pub const PEER_TIMEOUT: Duration = Duration::from_secs(60);
pub const STATE_CONFIRMATION_TIMEOUT: Duration = Duration::from_secs(2);
pub const REQUEST_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_API_ATTEMPTS: u8 = 4;
const RETRY_AFTER_CAP: Duration = Duration::from_secs(30);
// Secret Store writes reach Compute reads after a delay that was measured in the minute range.
const SECRET_PROPAGATION: &str = "secret_propagation";
const SECRET_PROPAGATION_DELAY: Duration = Duration::from_secs(10);
const SECRET_PROPAGATION_ATTEMPTS: u8 = 24;

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
        // A propagation wait ends when its delay elapses; the next run reads the secret again.
        *self.retry.borrow_mut() = retry.filter(|retry| retry.operation != SECRET_PROPAGATION);
    }
    pub fn secret_propagation_retry(&self) -> RetryState {
        let attempt = self
            .retry_attempts
            .borrow()
            .get(SECRET_PROPAGATION)
            .copied()
            .unwrap_or(0)
            .saturating_add(1);
        self.retry_attempts
            .borrow_mut()
            .insert(SECRET_PROPAGATION.into(), attempt);
        RetryState {
            operation: SECRET_PROPAGATION.into(),
            attempt,
            at: if attempt >= SECRET_PROPAGATION_ATTEMPTS {
                0
            } else {
                self.runtime.now() + SECRET_PROPAGATION_DELAY.as_secs()
            },
            reason: "credentials_pending".into(),
        }
    }
    pub fn settle_secret_propagation(&self) {
        self.retry_attempts.borrow_mut().remove(SECRET_PROPAGATION);
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
    pub fn check(&self) -> Result<(), Error> {
        if self.runtime.admin_elapsed(self.started) >= STEP_WALL_BUDGET {
            return Err(Error::new(503, "wall_time_budget"));
        }
        if self.calls.get() >= 24 || self.runtime.admin_remaining_calls() == 0 {
            return Err(Error::new(503, "backend_budget"));
        }
        Ok(())
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
        self.charge()?;
        self.runtime
            .secret(name)
            .map_err(|_| Error::new(503, "secret_unavailable"))?
            .filter(|value| !value.is_empty())
            .ok_or_else(|| Error::new(409, "credentials_pending"))
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
                && (self.calls.get() + 3 + 4 > 24 || self.runtime.admin_remaining_calls() < 7)
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
        for limit in [24 * 1024 * 1024 + 256 * 1024 + 17, usize::MAX] {
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
}
