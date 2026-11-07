//! Size-aware compaction of request window files (the `compaction` service setting).
//!
//! Eligible small `window=` files are merged, in key order, into `compact=` files of up to
//! the size target inside the same hour folder. Column chunks are copied without decoding, one
//! source at a time, and a compact over [`PART_BYTES`] streams out as multipart parts. Readers
//! prefer a valid compact over the windows it covers (see [`crate::telemetry::compact`]), so the
//! sources are deleted only after [`DELETE_GRACE_S`], once the watermark's `manifest_gen` bump has
//! invalidated cached hour listings.
//!
//! Source identities, multipart progress, rebased footer and buffered bytes are checkpointed.
//! Neither a Parquet writer nor process memory needs to survive a request.

mod checkpoint;

use super::{
    bump_manifest_gen, hour_dir, parse_processed, processed_prefix,
    storage::{Object, Store},
    PLAN_KEY, WATERMARK_KEY,
};
use crate::{shell::Runtime, telemetry::compact};
use bytes::Bytes;
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;

pub const STATE_KEY: &str = "ingest:compact_state";
/// Sources stay readable this long after their compact is committed.
pub const DELETE_GRACE_S: u64 = 660;
/// A part is uploaded once this much output is buffered.
pub const PART_BYTES: usize = 8 * 1024 * 1024;
/// Larger windows are left alone; this bounds the bytes held at once.
pub const MAX_SOURCE_BYTES: u64 = 16 * 1024 * 1024;
/// Keeps a compact's sources within the 512-byte key limit of `/api/file` callers.
pub const MAX_SOURCES: usize = 32;
/// A window this recently written may belong to a plan that is later abandoned.
pub const MIN_AGE_S: u64 = 15 * 60;
pub const FILE_TRIGGER: usize = 32;
pub const VCPU_STOP_MS: u64 = 40;
pub const MAX_EXECUTION_SOURCE_BYTES: u64 = 16 * 1024 * 1024;
pub const MAX_EXECUTION_ROWS: u64 = 100_000;
pub const HEAP_STOP_MIB: u32 = 96;
/// Calls held back for the watermark bump (with one retry) and the state write.
const COMMIT_RESERVE: u32 = 10;
/// Calls that persist a pass's progress after its last source read: the checkpoint PUT, the
/// state save's CAS and fresh read, and the final state write. A job that can't also complete in
/// the same pass commits in a later one, so source reads need not hold [`COMMIT_RESERVE`].
const CHECKPOINT_CALLS: u32 = 4;
const MAX_DELETES: usize = 1000;
const MAX_STATE_BYTES: usize = 512 * 1024;

pub(super) fn handle_authorized_worker<R: Runtime>(
    runtime: &R,
    request: &crate::http::Request,
    started: std::time::Instant,
) -> crate::http::Response {
    let calls_at_start = runtime.admin_remaining_calls();
    let response = |mut value: Value| {
        value["calls"] = calls_at_start
            .saturating_sub(runtime.admin_remaining_calls())
            .into();
        value["duration_ms"] = (started.elapsed().as_millis() as u64).into();
        value["heap_mib"] = runtime.heap_mib().into();
        if let Some(usage) = runtime.usage_agg() {
            value["usage"] = usage
                .drain_rows(&crate::auth::settings::UsageLogging::default())
                .into();
        }
        let metrics = crate::telemetry::execution::ExecutionMetrics::snapshot(runtime, started);
        value["vcpu_ms"] = metrics.vcpu_ms.into();
        value["execution_metrics"] = serde_json::json!(metrics);
        crate::http::Response::new(200, value.to_string())
            .header("content-type", "application/json")
            .private()
    };
    let body: Value = match serde_json::from_slice(&request.body) {
        Ok(body) => body,
        Err(_) => return crate::http::Response::new(400, "Invalid request").private(),
    };
    let service: crate::admin::Service = match serde_json::from_value(body["service"].clone()) {
        Ok(service) => service,
        Err(_) => return crate::http::Response::new(400, "Invalid service").private(),
    };
    if !super::retention::safe_scope(&super::scope(&service))
        || !crate::admin::valid_bucket(&service.fos_bucket)
        || !crate::admin::REGIONS.contains(&service.fos_region.as_str())
        || !service.compaction.valid()
    {
        return crate::http::Response::new(400, "Invalid service").private();
    }
    let Some(target) = service.compaction.target_bytes() else {
        return response(serde_json::json!({"status":"ok","report":Report::default()}));
    };
    let Some((access_key, secret_key)) = crate::admin::fos_credentials(runtime, &service) else {
        return response(serde_json::json!({"status":"error","error":"credentials_unavailable"}));
    };
    let cdn_key = match runtime.secret("cdn_key") {
        Ok(Some(key)) if !key.is_empty() => key,
        _ => return response(serde_json::json!({"status":"error","error":"cdn_key_unavailable"})),
    };
    if let Some(usage) = runtime.usage_agg() {
        usage.set_caller("worker:request_compact");
    }
    // A chained pass stops within the maintenance stage's remaining `wall_ms`: the storage helpers
    // stop at `WALL_STOP` after `started`, so backdating it shortens (never extends) the pass.
    let wall_stop = super::storage::WALL_STOP;
    let pass_started = body["wall_ms"].as_u64().map_or(started, |ms| {
        let wall = std::time::Duration::from_millis(ms).min(wall_stop);
        started.checked_sub(wall_stop - wall).unwrap_or(started)
    });
    let store = Store {
        runtime,
        service: &service,
        access_key,
        secret_key,
        started: pass_started,
        vcpu_start: 0,
    };
    match request_compact(&store, &cdn_key, target, 1) {
        Ok(report) => response(serde_json::json!({"status":"ok","report":report})),
        Err(error) => response(serde_json::json!({"status":"error","error":error})),
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Report {
    pub compacts: u32,
    pub sources: u64,
    pub rows: u64,
    pub bytes_written: u64,
    pub deleted: u64,
    pub failed: u64,
    pub more_pending: bool,
    pub errors: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopped: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_generation: Option<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Queued {
    pub compact: String,
    pub keys: Vec<String>,
    pub at: u64,
    #[serde(default)]
    pub bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<super::file_metadata::Metadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Upload {
    pub key: String,
    pub id: String,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct State {
    pub next_hour: Option<i64>,
    #[serde(default)]
    pub queue: Vec<Queued>,
    #[serde(default)]
    pub upload: Option<Upload>,
    /// Windows that can't join their neighbours (schema or row-count mismatch).
    #[serde(default)]
    pub skip: Vec<String>,
    #[serde(default)]
    pub job: Option<Job>,
    #[serde(default)]
    pub lease_until: u64,
    #[serde(default)]
    pub list_after: Option<String>,
    #[serde(default)]
    pub context: Vec<String>,
    #[serde(default)]
    pub work_cleanup: Vec<String>,
    #[serde(default)]
    pub catalog_cursor: Option<String>,
    #[serde(default)]
    pub catalog_to: Option<i64>,
    #[serde(default)]
    pub catalog_objects: Vec<Object>,
    #[serde(default)]
    pub catalog_generation: Option<u64>,
    #[serde(default)]
    pub catalog_carry: Vec<Object>,
    #[serde(default)]
    pub catalog_open_hour: Option<i64>,
    #[serde(default)]
    pub group_limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    version: u32,
    sources: Vec<Candidate>,
    upload: Upload,
    next: usize,
    etags: Vec<String>,
    checkpoint: Option<String>,
    complete: bool,
    bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    metadata: Option<super::file_metadata::Metadata>,
    #[serde(default)]
    aborting: bool,
    #[serde(default)]
    copying: Option<SourceCopy>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct SourceCopy {
    footer: String,
    data_end: u64,
    group: usize,
    copied: u64,
    start: u64,
    length: u64,
    started: bool,
}

struct Run<'s, 'a, R: Runtime> {
    store: &'s Store<'a, R>,
    cdn_key: &'s str,
    root: String,
    target: u64,
    tail: u32,
    now: u64,
    state: State,
    generation: Option<String>,
    /// The state this pass last wrote, to recognise its own write behind a lagging read.
    saved: Option<Value>,
    report: Report,
    changed: bool,
    bump: bool,
}

/// One bounded compaction pass: abort a stale upload, delete sources past the grace, then merge
/// closed hours from the cursor while calls, wall time and vCPU allow.
pub fn request_compact<R: Runtime>(
    store: &Store<'_, R>,
    cdn_key: &str,
    target: u64,
    tail: u32,
) -> Result<Report, String> {
    let runtime = store.runtime;
    let now = runtime.now();
    let token = runtime
        .secret(&store.service.fastly_token_ref)?
        .filter(|token| !token.is_empty())
        .ok_or("fastly_token_missing")?;
    let record = runtime.admin_get_fresh(STATE_KEY, &token)?;
    let plan = runtime.admin_get(PLAN_KEY)?;
    let watermark = runtime.admin_get(WATERMARK_KEY)?;
    let state: State = record
        .as_ref()
        .map(|r| {
            serde_json::from_value(r.value.clone()).map_err(|_| "compact_state_invalid".to_owned())
        })
        .transpose()?
        .unwrap_or_default();
    // [`VCPU_STOP_MS`] bounds the pass's own work. Live workers have spent 45-60 ms on startup
    // and these state reads by now, so counting from the execution start admits nothing.
    let store = &Store {
        runtime,
        service: store.service,
        access_key: store.access_key.clone(),
        secret_key: store.secret_key.clone(),
        started: store.started,
        vcpu_start: runtime.vcpu_ms(),
    };
    let mut run = Run {
        store,
        cdn_key,
        root: processed_prefix(store.service),
        target,
        tail,
        now,
        state,
        generation: record.map(|r| r.generation),
        saved: None,
        report: Report::default(),
        changed: false,
        bump: false,
    };
    if run.state.lease_until > now {
        run.report.more_pending = true;
        run.report.stopped = Some("lease_held".into());
        return Ok(run.report);
    }
    if let Some(stop) = run.limit_stop(4) {
        run.report.more_pending = true;
        run.report.stopped = Some(stop.into());
        return Ok(run.report);
    }
    run.state.lease_until = now.saturating_add(70);
    match run.save() {
        Err(error) if error == "compact_state_conflict" => {
            run.report.more_pending = true;
            run.report.stopped = Some("lease_conflict".into());
            run.report.lease_generation = run.generation;
            return Ok(run.report);
        }
        result => result?,
    }
    if let Some(upload) = run.state.upload.take() {
        run.changed = true;
        if let Err(error) = store.multipart_abort(&upload.key, &upload.id) {
            run.report.errors.push(error);
            run.state.upload = Some(upload);
        }
    }
    run.drain();
    let in_flight = plan.is_some_and(|p| {
        let id = p.value["plan_id"]
            .as_str()
            .or_else(|| p.value["run_id"].as_str());
        id != watermark.as_ref().and_then(|w| w.value["run_id"].as_str())
    });
    let mut pending = Vec::new();
    if run.state.job.is_some() {
        run.resume_job(&mut pending)?;
    } else if !in_flight {
        pending = run.scan()?;
    } else {
        run.report.stopped = Some("plan_in_flight".into());
    }
    if run.bump {
        let committed = match commit_watermark(store, watermark, &mut pending, tail + 2) {
            Ok(new) => {
                if !new {
                    run.report.compacts = 0;
                    run.report.sources = 0;
                    run.report.rows = 0;
                }
                true
            }
            Err(error) if error == "catalog_prepare_pending" => {
                run.report.more_pending = true;
                run.report.compacts = 0;
                run.report.sources = 0;
                run.report.rows = 0;
                false
            }
            Err(error) if error == "catalog_replacement_not_active" => {
                run.report.errors.push(error);
                run.report.compacts = 0;
                run.report.sources = 0;
                run.report.rows = 0;
                if let Some(job) = run.state.job.take() {
                    if let Some(key) = job.checkpoint {
                        run.state.work_cleanup.push(key);
                    }
                    if let Some(copying) = job.copying {
                        run.state.work_cleanup.push(copying.footer);
                    }
                }
                run.state.catalog_objects.clear();
                run.state.catalog_carry.clear();
                run.state.catalog_cursor = None;
                run.state.catalog_to = None;
                run.report.more_pending = true;
                false
            }
            Err(error) if error == "compact_watermark_conflict" => {
                run.state.lease_until = 0;
                if run.changed && run.remaining() > run.tail {
                    if let Ok(value) = serde_json::to_value(&run.state) {
                        if serde_json::to_vec(&value)
                            .is_ok_and(|bytes| bytes.len() <= MAX_STATE_BYTES)
                        {
                            let _ = run.write(&value, false);
                        }
                    }
                }
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        if committed {
            run.state.queue.extend(pending);
            run.state.catalog_objects.clear();
            run.state.catalog_carry.clear();
            run.state.catalog_cursor = None;
            run.state.catalog_to = None;
        }
        if committed && run.state.job.as_ref().is_some_and(|job| job.complete) {
            if let Some(key) = run
                .state
                .job
                .as_ref()
                .and_then(|job| job.checkpoint.clone())
            {
                run.state.work_cleanup.push(key);
            }
            run.state.job = None;
        }
        run.changed = true;
    }
    run.state.lease_until = 0;
    if !run.bump
        && run.state.job.is_none()
        && run.state.catalog_to.is_none()
        && run.state.catalog_objects.is_empty()
        && run.state.catalog_cursor.is_none()
        && run.remaining() >= run.tail + 3
    {
        let current = runtime.admin_get(WATERMARK_KEY)?;
        if let Some(record) = current {
            let mut value = record.value;
            let mut catalog = super::catalog::head(&value)?;
            if run.state.catalog_generation == Some(catalog.generation) {
                catalog.compaction_due = false;
                catalog.next_compaction_check = now.saturating_add(MIN_AGE_S);
                value["catalog"] = serde_json::to_value(catalog).map_err(|e| e.to_string())?;
                if !runtime.admin_cas(WATERMARK_KEY, Some(&record.generation), &value)? {
                    run.report.more_pending = true;
                }
            }
        }
    }
    run.report.more_pending |= run.report.compacts > 0;
    run.changed = true;
    if run.changed {
        let value = serde_json::to_value(&run.state).map_err(|e| e.to_string())?;
        if serde_json::to_vec(&value).map_err(|e| e.to_string())?.len() > MAX_STATE_BYTES {
            return Err("compact_state_limit".into());
        }
        run.write(&value, false)?;
    }
    Ok(run.report)
}

fn commit_watermark<R: Runtime>(
    store: &Store<'_, R>,
    mut watermark: Option<crate::admin::Record>,
    pending: &mut [Queued],
    reserve: u32,
) -> Result<bool, String> {
    let runtime = store.runtime;
    for _ in 0..2 {
        let prev = watermark.as_ref().map(|r| &r.value);
        let catalog = super::catalog::head(prev.unwrap_or(&Value::Null))?;
        if let Some(receipt) = &catalog.last_compaction {
            let sources = pending
                .iter()
                .flat_map(|queued| queued.keys.iter().cloned())
                .collect::<BTreeSet<_>>();
            if receipt.replaces == sources
                && receipt.files.len() == pending.len()
                && pending.iter().all(|queued| {
                    receipt
                        .files
                        .iter()
                        .any(|file| file.key == queued.compact && file.bytes == queued.bytes)
                })
            {
                for queued in pending.iter_mut() {
                    queued.at = receipt.at;
                }
                return Ok(false);
            }
        }
        let value = bump_manifest_gen(
            prev,
            prev.cloned().unwrap_or(Value::Object(Default::default())),
        );
        let files = pending
            .iter()
            .map(|queued| {
                let name = compact::parse_compact_name(&queued.compact);
                super::worker::Output {
                    key: queued.compact.clone(),
                    rows: name.map_or(0, |name| name.rows),
                    bytes: queued.bytes,
                    metadata: queued.metadata.clone(),
                    ..Default::default()
                }
            })
            .collect::<Vec<_>>();
        let replaces = pending
            .iter()
            .flat_map(|q| q.keys.iter().cloned())
            .collect::<Vec<_>>();
        let value = super::catalog::prepare_reserving(
            store,
            &value,
            "compaction",
            "",
            &files,
            &[],
            &[],
            &replaces,
            reserve,
        )?;
        let generation = watermark.as_ref().map(|r| r.generation.as_str());
        if runtime.admin_cas(WATERMARK_KEY, generation, &value)? {
            let at = value["catalog"]["last_compaction"]["at"]
                .as_u64()
                .ok_or("compact_publication_receipt_invalid")?;
            for queued in pending.iter_mut() {
                queued.at = at;
            }
            return Ok(true);
        }
        watermark = runtime.admin_get(WATERMARK_KEY)?;
    }
    Err("compact_watermark_conflict".into())
}

fn mtime(object: &Object) -> Option<u64> {
    let at = object.last_modified.as_deref()?;
    let at =
        time::OffsetDateTime::parse(at, &time::format_description::well_known::Rfc3339).ok()?;
    u64::try_from(at.unix_timestamp()).ok()
}

/// The hour a request file's name places it in.
fn file_hour(key: &str) -> Option<i64> {
    let start = match parse_processed(key) {
        Some(name) => name.start,
        None => i64::try_from(compact::parse_compact_name(key)?.a).ok()?,
    };
    Some(start - start.rem_euclid(3600))
}

/// The footer share of a group's input: a compact carries one row group per merged input.
fn source_weight(key: &str) -> usize {
    compact::parse_compact_name(key).map_or(1, |name| name.sources.max(1) as usize)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Candidate {
    key: String,
    start: i64,
    end: i64,
    rows: u64,
    size: u64,
}

enum Outcome {
    /// The first `n` sources were merged, or the first was skipped (`n` = 1, nothing written).
    Consumed(usize),
    /// The hour can't continue in this run.
    Stop,
}

impl<R: Runtime> Run<'_, '_, R> {
    fn save(&mut self) -> Result<(), String> {
        let value = serde_json::to_value(&self.state).map_err(|e| e.to_string())?;
        if serde_json::to_vec(&value).map_err(|e| e.to_string())?.len() > MAX_STATE_BYTES {
            return Err("compact_state_limit".into());
        }
        self.write(&value, true)
    }

    /// Writes the state over the pass's generation. KV has no write that returns the new
    /// generation, and a fresh read can still show the value this write replaced; a generation
    /// taken from that read would fail the pass's next write against its own state. So a write
    /// is read back until it shows, and a conflict whose current value is the pass's own last
    /// write takes that generation and retries once. Any other value is another writer's.
    fn write(&mut self, value: &Value, read_back: bool) -> Result<(), String> {
        let runtime = self.store.runtime;
        if !runtime.admin_cas(STATE_KEY, self.generation.as_deref(), value)? {
            let recovered = match self.saved.clone() {
                Some(own) => {
                    self.read_back(&own)?
                        && runtime.admin_cas(STATE_KEY, self.generation.as_deref(), value)?
                }
                None => false,
            };
            if !recovered {
                return Err("compact_state_conflict".into());
            }
        }
        self.saved = Some(value.clone());
        if read_back {
            self.read_back(value)?;
        }
        Ok(())
    }

    /// Reads the state until it shows `expected` (twice at most), taking the generation read.
    fn read_back(&mut self, expected: &Value) -> Result<bool, String> {
        let runtime = self.store.runtime;
        let token = runtime
            .secret(&self.store.service.fastly_token_ref)?
            .filter(|token| !token.is_empty())
            .ok_or("fastly_token_missing")?;
        for _ in 0..2 {
            let record = runtime.admin_get_fresh(STATE_KEY, &token)?;
            self.generation = record.as_ref().map(|r| r.generation.clone());
            let Some(mut value) = record.map(|r| r.value) else {
                return Ok(false);
            };
            if let Some(map) = value.as_object_mut() {
                map.remove("_kv_written_ms");
            }
            if &value == expected {
                return Ok(true);
            }
        }
        Ok(false)
    }
    fn remaining(&self) -> u32 {
        self.store.runtime.admin_remaining_calls()
    }

    fn affordable(&self, calls: u32) -> bool {
        self.remaining() >= self.tail + COMMIT_RESERVE + calls
    }

    /// Whether `calls` more still leave room to checkpoint the job and save the state.
    fn checkpointable(&self, calls: u32) -> bool {
        self.remaining() >= self.tail + CHECKPOINT_CALLS + calls
    }

    fn drain(&mut self) {
        for _ in 0..4 {
            let Some(key) = self.state.work_cleanup.last().cloned() else {
                break;
            };
            if !self.affordable(1) || !self.within_limits() {
                break;
            }
            match self.store.delete(&key) {
                Ok(()) => {
                    self.state.work_cleanup.pop();
                    self.changed = true;
                }
                Err(error) => {
                    self.report.errors.push(error);
                    break;
                }
            }
        }
        let mut due = Vec::new();
        let mut keys = Vec::new();
        for (index, queued) in self.state.queue.iter().enumerate() {
            if queued.at + DELETE_GRACE_S <= self.now
                && keys.len() + queued.keys.len() <= MAX_DELETES
            {
                due.push(index);
                keys.extend(queued.keys.iter().cloned());
            }
        }
        if due.is_empty() || !self.affordable(1) {
            return;
        }
        let result = match self.store.delete_objects(&keys) {
            Ok(result) => result,
            Err(error) => {
                self.report.errors.push(error);
                return;
            }
        };
        self.report.deleted += result.deleted.len() as u64;
        self.report.failed += result.failed.len() as u64;
        let failed: BTreeSet<&String> = result.failed.iter().collect();
        for index in due.into_iter().rev() {
            let queued = &mut self.state.queue[index];
            queued.keys.retain(|k| failed.contains(k));
            if queued.keys.is_empty() {
                self.state.queue.remove(index);
            }
        }
        self.changed = true;
    }

    fn within_limits(&self) -> bool {
        self.exceeded().is_none()
    }

    fn exceeded(&self) -> Option<&'static str> {
        if !self.store.within_wall() {
            Some("wall_limit")
        } else if self.store.vcpu_ms() >= VCPU_STOP_MS {
            Some("vcpu_limit")
        } else if self
            .store
            .runtime
            .heap_mib()
            .is_some_and(|heap| heap >= HEAP_STOP_MIB)
        {
            Some("heap_limit")
        } else {
            None
        }
    }

    /// Why the pass can't start more work needing `calls`, if it can't.
    fn limit_stop(&self, calls: u32) -> Option<&'static str> {
        if self.affordable(calls) {
            self.exceeded()
        } else {
            Some("call_budget")
        }
    }

    /// Only the pinned committed catalog may supply compaction inputs.
    fn scan(&mut self) -> Result<Vec<Queued>, String> {
        let mut pending = Vec::new();
        loop {
            if let Some(stop) = self.limit_stop(4) {
                self.report.more_pending = true;
                self.report.stopped.get_or_insert_with(|| stop.into());
                break;
            }
            if self.state.catalog_objects.is_empty() {
                let to = *self.state.catalog_to.get_or_insert(i64::MAX);
                let page = match super::catalog::discovery::page(
                    self.store,
                    0,
                    to,
                    false,
                    false,
                    self.state.catalog_cursor.as_deref(),
                ) {
                    Ok(page) => page,
                    Err(error) if error == "catalog_cursor_expired" => {
                        self.state.catalog_cursor = None;
                        self.state.catalog_to = None;
                        self.state.catalog_carry.clear();
                        self.state.catalog_open_hour = None;
                        self.changed = true;
                        self.report.more_pending = true;
                        break;
                    }
                    Err(error) => return Err(error),
                };
                self.state.catalog_cursor = page.next;
                self.state.catalog_open_hour = page.open_hour;
                self.state.catalog_generation = Some(page.generation);
                self.state.catalog_objects = page
                    .files
                    .into_iter()
                    .map(|file| Object {
                        key: file["key"].as_str().unwrap_or_default().to_owned(),
                        size: file["size"].as_u64().unwrap_or(0),
                        last_modified: file["committed_at"].as_str().map(str::to_owned),
                    })
                    .collect();
                self.state
                    .catalog_objects
                    .append(&mut self.state.catalog_carry);
            }
            let mut hours: std::collections::BTreeMap<i64, Vec<Object>> = Default::default();
            for object in &self.state.catalog_objects {
                if let Some(hour) = file_hour(&object.key) {
                    hours.entry(hour).or_default().push(object.clone());
                }
            }
            for (hour, objects) in hours {
                if !self.hour(hour, &objects, &mut pending)? {
                    self.changed = true;
                    self.report.more_pending = true;
                    return Ok(pending);
                }
                if self.state.catalog_open_hour == Some(hour) {
                    let mut carry = objects
                        .into_iter()
                        .filter(|object| {
                            object.size < self.target
                                && !self.state.skip.contains(&object.key)
                                && !self
                                    .state
                                    .queue
                                    .iter()
                                    .any(|queued| queued.keys.contains(&object.key))
                                && super::catalog::file_bounds(&object.key).is_some()
                        })
                        .collect::<Vec<_>>();
                    carry.sort_by(|a, b| a.size.cmp(&b.size).then_with(|| a.key.cmp(&b.key)));
                    carry.truncate(MAX_SOURCES);
                    self.state.catalog_carry = carry;
                }
                self.state
                    .catalog_objects
                    .retain(|object| file_hour(&object.key) != Some(hour));
            }
            self.changed = true;
            if self.state.catalog_cursor.is_none() {
                self.state.catalog_to = None;
                break;
            }
        }
        Ok(pending)
    }

    /// Repairs and merges one closed hour; false when it stopped before finishing.
    fn hour(
        &mut self,
        hour: i64,
        objects: &[Object],
        pending: &mut Vec<Queued>,
    ) -> Result<bool, String> {
        let queued: BTreeSet<String> = self
            .state
            .queue
            .iter()
            .chain(pending.iter())
            .flat_map(|q| q.keys.iter())
            .cloned()
            .collect();
        let mut oldest = self.now;
        let mut candidates = Vec::new();
        for object in objects {
            let Some((start, end)) = super::catalog::file_bounds(&object.key) else {
                continue;
            };
            let rows = parse_processed(&object.key)
                .and_then(|name| name.rows)
                .or_else(|| compact::parse_compact_name(&object.key).map(|name| name.rows));
            let Some(rows) = rows else { continue };
            if object.size >= self.target
                || self.state.skip.contains(&object.key)
                || queued.contains(&object.key)
            {
                continue;
            }
            oldest = oldest.min(mtime(object).unwrap_or(self.now));
            candidates.push(Candidate {
                key: object.key.clone(),
                start,
                end,
                rows,
                size: object.size,
            });
        }
        let bytes: u64 = candidates.iter().map(|c| c.size).sum();
        if candidates.len() < 2
            || (bytes < self.target
                && candidates.len() < FILE_TRIGGER
                && oldest.saturating_add(MIN_AGE_S) > self.now)
        {
            return Ok(true);
        }
        let prefix = format!("{}{}", self.root, hour_dir(hour));
        let mut index = 0;
        while candidates.len() - index >= 2 {
            if !self.within_limits() {
                return Ok(false);
            }
            let rest = &candidates[index..];
            let (mut take, mut total, mut weight) = (0, 0, 0);
            let limit = match self.state.group_limit {
                0 => usize::MAX,
                limit => limit.max(2),
            };
            while take < rest.len()
                && take < MAX_SOURCES
                && total + rest[take].size <= self.target
                && weight + source_weight(&rest[take].key) <= limit
            {
                total += rest[take].size;
                weight += source_weight(&rest[take].key);
                take += 1;
            }
            if take < 2 {
                index += 1;
                continue;
            }
            match self.merge(&prefix, &candidates[index..index + take], pending)? {
                Outcome::Consumed(n) => index += n,
                Outcome::Stop => return Ok(false),
            }
            if self.state.job.is_some() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn merge(
        &mut self,
        prefix: &str,
        group: &[Candidate],
        pending: &mut Vec<Queued>,
    ) -> Result<Outcome, String> {
        let names = group.iter().map(|c| c.key.clone()).collect::<Vec<_>>();
        use sha2::{Digest, Sha256};
        let digest = hex::encode(Sha256::digest(
            serde_json::to_vec(&names).map_err(|e| e.to_string())?,
        ));
        let output_prefix = format!("{prefix}catalog1/{digest}/");
        let key = compact::compact_key(
            &output_prefix,
            group.iter().map(|c| c.start).min().unwrap_or(0) as u64,
            group.iter().map(|c| c.end).max().unwrap_or(0) as u64,
            &names,
            group.iter().map(|c| c.rows).sum(),
        );
        if key.len() > 512 {
            if group.len() > 2 {
                return self.merge(prefix, &group[..group.len() - 1], pending);
            }
            return Err("compact_key_admission".into());
        }
        // The upload, the job's save and a first source read with its checkpoint.
        if !self.checkpointable(5) {
            return Ok(Outcome::Stop);
        }
        let id = self.store.multipart_create(&key)?;
        self.state.job = Some(Job {
            version: 1,
            sources: group.to_vec(),
            upload: Upload {
                key: key.clone(),
                id: id.clone(),
            },
            next: 0,
            etags: Vec::new(),
            checkpoint: None,
            complete: false,
            bytes: 0,
            metadata: None,
            aborting: false,
            copying: None,
        });
        if let Err(error) = self.save() {
            if let Err(abort) = self.store.multipart_abort(&key, &id) {
                self.report.errors.push(abort);
            }
            return Err(error);
        }
        self.resume_job(pending)?;
        if self.state.job.as_ref().is_some_and(|job| job.complete) {
            Ok(Outcome::Consumed(group.len()))
        } else {
            Ok(Outcome::Stop)
        }
    }

    fn checkpoint_job(&mut self, job: &mut Job, writer: &checkpoint::Writer) -> Result<(), String> {
        use sha2::{Digest, Sha256};
        let body = writer.checkpoint()?;
        let key = format!(
            "{}processed/_compact_work/{}.bin",
            super::scope(self.store.service),
            hex::encode(Sha256::digest(&body)),
        );
        self.store.put_verified(&key, body)?;
        let previous = job.checkpoint.replace(key.clone());
        self.state.job = Some(job.clone());
        if let Some(previous) = previous.filter(|previous| previous != &key) {
            self.state.work_cleanup.push(previous);
        }
        self.save()
    }

    fn completed_job(&mut self, job: &Job, pending: &mut Vec<Queued>) {
        self.report.compacts += 1;
        self.report.sources += job.sources.len() as u64;
        self.report.rows += job.sources.iter().map(|c| c.rows).sum::<u64>();
        self.report.bytes_written += job.bytes;
        self.bump = true;
        pending.push(Queued {
            compact: job.upload.key.clone(),
            keys: job.sources.iter().map(|c| c.key.clone()).collect(),
            at: self.now,
            bytes: job.bytes,
            metadata: job.metadata.clone(),
        });
        self.state.job = Some(job.clone());
        self.changed = true;
    }

    fn abort_job(&mut self, job: Job, error: String) {
        self.report.errors.push(error);
        self.discard_job(job);
    }

    fn discard_job(&mut self, mut job: Job) {
        job.aborting = true;
        match self.store.multipart_abort(&job.upload.key, &job.upload.id) {
            Ok(()) => {
                if let Some(key) = job.checkpoint {
                    self.state.work_cleanup.push(key);
                }
                self.state.job = None;
            }
            Err(error) => {
                self.report.errors.push(error);
                self.state.job = Some(job);
            }
        }
        self.changed = true;
        self.report.more_pending = true;
    }

    fn reject_input(&mut self, job: Job, source: &str, error: String) {
        if !matches!(
            error.as_str(),
            "compact_footer_limit" | "compact_row_group_limit"
        ) {
            self.state.skip.push(source.to_owned());
            self.abort_job(job, error);
            return;
        }
        // A full output is planning, not a failure. The key names its sources, so the job can't
        // be cut short in place: replan with the sources that fit, or leave a first source that
        // fits with no other (a full compact) as it is.
        let stopped = if job.next > 1 {
            self.state.group_limit = job.sources[..job.next]
                .iter()
                .map(|source| source_weight(&source.key))
                .sum();
            error.trim_start_matches("compact_")
        } else {
            self.state.skip.push(job.sources[0].key.clone());
            "source_full"
        };
        self.report
            .stopped
            .get_or_insert_with(|| stopped.to_owned());
        self.discard_job(job);
    }

    fn upload_parts(
        &mut self,
        job: &mut Job,
        writer: &mut checkpoint::Writer,
    ) -> Result<(), String> {
        while writer.buffer.len() >= PART_BYTES && self.affordable(9) && self.within_limits() {
            let etag = match self.store.multipart_part(
                &job.upload.key,
                &job.upload.id,
                job.etags.len() as u32 + 1,
                writer.buffer[..PART_BYTES].to_vec(),
            ) {
                Err(error) if error == "multipart_upload_missing" => {
                    self.state.job = Some(job.clone());
                    return Err(error);
                }
                result => result?,
            };
            writer.buffer.drain(..PART_BYTES);
            job.etags.push(etag);
            writer.flushed += PART_BYTES as u64;
        }
        Ok(())
    }

    fn resume_job(&mut self, pending: &mut Vec<Queued>) -> Result<(), String> {
        match self.resume_upload(pending) {
            Err(error) if error == "multipart_upload_missing" => {
                // A vanished upload has lost its acknowledged parts, not its immutable inputs.
                // Rediscover active sources next tick rather than reusing offsets or stale inputs.
                if let Some(job) = self.state.job.take() {
                    if let Some(key) = job.checkpoint {
                        self.state.work_cleanup.push(key);
                    }
                    if let Some(copying) = job.copying {
                        self.state.work_cleanup.push(copying.footer);
                    }
                }
                self.state.catalog_cursor = None;
                self.state.catalog_to = None;
                self.state.catalog_objects.clear();
                self.state.catalog_carry.clear();
                self.state.catalog_generation = None;
                self.state.catalog_open_hour = None;
                self.changed = true;
                self.report.more_pending = true;
                Ok(())
            }
            result => result,
        }
    }

    fn resume_upload(&mut self, pending: &mut Vec<Queued>) -> Result<(), String> {
        let Some(mut job) = self.state.job.clone() else {
            return Ok(());
        };
        if job.version != 1 || job.aborting {
            self.abort_job(job, "compact_checkpoint_unrecoverable".into());
            return Ok(());
        }
        if job.complete {
            self.completed_job(&job, pending);
            return Ok(());
        }
        if !self.checkpointable(2) || !self.within_limits() {
            self.report.more_pending = true;
            return Ok(());
        }
        let mut writer = match job.checkpoint.as_deref() {
            Some(key) => {
                let bytes = self.store.get_direct(
                    key,
                    PART_BYTES + MAX_SOURCE_BYTES as usize + checkpoint::MAX_FOOTER_BYTES + 16,
                )?;
                let restored = bytes
                    .as_deref()
                    .ok_or("compact_checkpoint_missing".to_owned())
                    .and_then(checkpoint::Writer::restore);
                match restored {
                    Ok(writer) => Some(writer),
                    Err(error) => {
                        self.abort_job(job, error);
                        return Ok(());
                    }
                }
            }
            None => None,
        };
        let (mut bytes_read, mut rows_read) = (0u64, 0u64);
        let mut touched_groups = BTreeSet::new();
        let initial_next = job.next;
        let initial_parts = job.etags.len();
        let initial_copy = job.copying.clone();
        while job.next < job.sources.len() {
            if let Some(writer) = writer.as_mut() {
                self.upload_parts(&mut job, writer)?;
                if writer.buffer.len() >= PART_BYTES {
                    break;
                }
            }
            let source = job.sources[job.next].clone();
            if source.size > MAX_SOURCE_BYTES || source.rows > MAX_EXECUTION_ROWS {
                if !self.affordable(9) || !self.within_limits() {
                    break;
                }
                let before = bytes_read;
                match self.copy_group_slice(
                    &mut job,
                    &mut writer,
                    &mut bytes_read,
                    &mut rows_read,
                    &mut touched_groups,
                ) {
                    Ok(true) => job.next += 1,
                    Ok(false) if bytes_read > before => continue,
                    Ok(false) => break,
                    Err(error) if error == "multipart_upload_missing" => return Err(error),
                    Err(error) if super::storage::transient_error(&error) => {
                        if let Some(writer) = writer.as_ref() {
                            self.checkpoint_job(&mut job, writer)?;
                        }
                        self.report.errors.push(error);
                        self.report.more_pending = true;
                        return Ok(());
                    }
                    Err(error) => {
                        self.reject_input(job, &source.key, error);
                        return Ok(());
                    }
                }
                continue;
            }
            if !self.checkpointable(1)
                || !self.within_limits()
                || bytes_read.saturating_add(source.size) > MAX_EXECUTION_SOURCE_BYTES
                || rows_read.saturating_add(source.rows) > MAX_EXECUTION_ROWS
            {
                break;
            }
            let bytes = self
                .store
                .get(self.cdn_key, &source.key, MAX_SOURCE_BYTES as usize)?;
            let Some(bytes) = bytes else {
                self.abort_job(job, "compact_source_missing".into());
                return Ok(());
            };
            if bytes.len() as u64 != source.size {
                self.abort_job(job, "compact_source_identity_changed".into());
                return Ok(());
            }
            let bytes = Bytes::from(bytes);
            let reader = SerializedFileReader::new(bytes.clone()).map_err(|e| e.to_string())?;
            if reader.metadata().file_metadata().num_rows() as u64 != source.rows {
                self.abort_job(job, "compact_source_row_count_mismatch".into());
                return Ok(());
            }
            let w = writer.get_or_insert_with(|| checkpoint::Writer::new(reader.metadata()));
            if let Err(error) = w.append(&bytes, reader.metadata()) {
                self.reject_input(job, &source.key, error);
                return Ok(());
            }
            bytes_read += source.size;
            rows_read += source.rows;
            self.upload_parts(&mut job, w)?;
            job.next += 1;
        }
        let Some(mut writer) = writer else {
            self.report.more_pending = true;
            return Ok(());
        };
        if job.next != initial_next
            || job.etags.len() != initial_parts
            || job.copying != initial_copy
        {
            self.checkpoint_job(&mut job, &writer)?;
        }
        if job.next < job.sources.len() || !self.affordable(5) || !self.within_limits() {
            self.report.more_pending = true;
            return Ok(());
        }
        let footer = writer.footer()?;
        job.bytes = writer.flushed + writer.buffer.len() as u64 + footer.len() as u64;
        job.metadata = super::file_metadata::from_metadata(writer.metadata());
        if initial_next != job.sources.len() || !self.store.exists(&job.upload.key)? {
            writer.buffer.extend_from_slice(&footer);
            job.etags.push(self.store.multipart_part(
                &job.upload.key,
                &job.upload.id,
                job.etags.len() as u32 + 1,
                writer.buffer,
            )?);
            self.store
                .multipart_complete(&job.upload.key, &job.upload.id, &job.etags)?;
        }
        job.complete = true;
        self.completed_job(&job, pending);
        Ok(())
    }

    fn copy_group_slice(
        &mut self,
        job: &mut Job,
        writer: &mut Option<checkpoint::Writer>,
        bytes_read: &mut u64,
        rows_read: &mut u64,
        touched: &mut BTreeSet<(usize, usize)>,
    ) -> Result<bool, String> {
        use parquet::file::metadata::ParquetMetaDataReader;
        use sha2::{Digest, Sha256};
        let source = &job.sources[job.next];
        let footer = if let Some(copy) = &job.copying {
            self.store
                .get_direct(&copy.footer, checkpoint::MAX_FOOTER_BYTES)?
                .ok_or("compact_source_footer_missing")?
        } else {
            if source.size < 12 {
                return Err("compact_source_footer_invalid".into());
            }
            let tail = self
                .store
                .range(self.cdn_key, &source.key, source.size - 8, 8)?;
            *bytes_read = bytes_read.saturating_add(8);
            if &tail[4..] != b"PAR1" {
                return Err("compact_source_footer_invalid".into());
            }
            let length = u32::from_le_bytes(
                tail[..4]
                    .try_into()
                    .map_err(|_| "compact_source_footer_invalid")?,
            ) as usize;
            if length > checkpoint::MAX_FOOTER_BYTES || length as u64 + 12 > source.size {
                return Err("compact_source_footer_admission".into());
            }
            let end = source.size - 8 - length as u64;
            let bytes = self.store.range(self.cdn_key, &source.key, end, length)?;
            *bytes_read = bytes_read.saturating_add(length as u64);
            let key = format!(
                "{}processed/_compact_work/source-{}.footer",
                super::scope(self.store.service),
                hex::encode(Sha256::digest(source.key.as_bytes()))
            );
            self.store.put_verified(&key, bytes.clone())?;
            job.copying = Some(SourceCopy {
                footer: key,
                data_end: end,
                group: 0,
                copied: 0,
                start: 0,
                length: 0,
                started: false,
            });
            bytes
        };
        let metadata =
            ParquetMetaDataReader::decode_metadata(&footer).map_err(|e| e.to_string())?;
        if metadata.file_metadata().num_rows() as u64 != source.rows {
            return Err("compact_source_row_count_mismatch".into());
        }
        let copy = job
            .copying
            .as_mut()
            .ok_or("compact_source_checkpoint_invalid")?;
        if copy.group == metadata.num_row_groups() {
            self.state.work_cleanup.push(copy.footer.clone());
            job.copying = None;
            return Ok(true);
        }
        let group = metadata
            .row_groups()
            .get(copy.group)
            .ok_or("compact_row_group_invalid")?;
        let rows = u64::try_from(group.num_rows()).map_err(|_| "compact_row_group_invalid")?;
        if rows > MAX_EXECUTION_ROWS {
            return Err("compact_row_group_admission".into());
        }
        if !touched.contains(&(job.next, copy.group)) {
            if rows_read.saturating_add(rows) > MAX_EXECUTION_ROWS {
                return Ok(false);
            }
            *rows_read += rows;
            touched.insert((job.next, copy.group));
        }
        let writer = writer.get_or_insert_with(|| checkpoint::Writer::new(&metadata));
        if !copy.started {
            let (start, length) = writer.append_group_metadata(&metadata, copy.group)?;
            if start < 4
                || start
                    .checked_add(length)
                    .is_none_or(|end| end > copy.data_end)
            {
                return Err("compact_column_out_of_bounds".into());
            }
            copy.start = start;
            copy.length = length;
            copy.started = true;
        }
        const SLICE: u64 = 4 * 1024 * 1024;
        let length = (copy.length - copy.copied)
            .min(SLICE)
            .min(MAX_EXECUTION_SOURCE_BYTES.saturating_sub(*bytes_read));
        if length == 0 {
            return Ok(false);
        }
        let bytes = self.store.range(
            self.cdn_key,
            &source.key,
            copy.start + copy.copied,
            length as usize,
        )?;
        writer.buffer.extend_from_slice(&bytes);
        *bytes_read += length;
        copy.copied += length;
        if copy.copied == copy.length {
            copy.group += 1;
            copy.copied = 0;
            copy.started = false;
        }
        self.upload_parts(job, writer)?;
        Ok(false)
    }
}
