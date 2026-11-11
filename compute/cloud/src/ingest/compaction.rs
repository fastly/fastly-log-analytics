//! Size-aware compaction of request and RUM window files (the `compaction` service setting).
//!
//! Eligible small `window=` files are merged, in key order, into `compact=` files of up to
//! the size target inside the same hour folder. Column chunks are copied without decoding, one
//! source at a time, and a compact over [`PART_BYTES`] streams out as multipart parts. Readers
//! prefer a valid compact over the windows it covers (see [`crate::telemetry::compact`]), so the
//! sources are deleted only after [`DELETE_GRACE_S`], once the watermark's `manifest_gen` bump has
//! invalidated cached hour listings.
//!
//! RUM tables have no catalog: a pass lists each table folder itself, files the compact beside
//! its sources, queues them as soon as the compact exists and deletes them after the same grace.
//! A compact merged from compacts hides them by its name's tags, so each is deleted only after
//! its own sources are.
//!
//! Source identities, multipart progress, rebased footer and buffered bytes are checkpointed.
//! Neither a Parquet writer nor process memory needs to survive a request.

pub mod checkpoint;

use super::{
    bump_manifest_gen, hour_dir, parse_processed, plan_id_of, plan_key, processed_prefix,
    storage::{Object, Store},
    watermark_key, WATERMARK_KEY,
};
use crate::{shell::Runtime, telemetry::compact};
use bytes::Bytes;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;

pub const STATE_KEY: &str = "ingest:compact_state";
pub const RUM_TABLES: [&str; 2] = [super::rum::ERRORS, super::rum::VITALS];
/// The state record of each of [`RUM_TABLES`], in the same order.
pub const RUM_STATE_KEYS: [&str; 2] = [
    "ingest:compact_state:rum:client_errors",
    "ingest:compact_state:rum:client_vitals",
];
/// How long a RUM table with nothing left to merge waits before it is listed again.
pub const RUM_CHECK_S: u64 = 300;
/// A finished RUM scan starts its next one this far back, so windows written late into an hour
/// that was already merged still join it.
const RUM_REVISIT_S: i64 = 2 * 3600;
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
/// An open hour's compacts merge together once this many have piled up, so an hour that is
/// still receiving windows keeps a bounded file count instead of one compact per trigger.
pub const COMPACT_TRIGGER: usize = 8;
pub const VCPU_STOP_MS: u64 = 40;
pub const MAX_EXECUTION_SOURCE_BYTES: u64 = 16 * 1024 * 1024;
pub const MAX_EXECUTION_ROWS: u64 = 100_000;
pub const HEAP_STOP_MIB: u32 = 96;
/// Window files of at most this many rows are decoded and rewritten together. Copied as they
/// are, each keeps its own row group, and those groups' footer entries fill a compact's
/// footer limit long before its byte target.
pub const MERGE_SOURCE_ROWS: u64 = 2048;
/// Rows rewritten into one row group, which bounds the encode work a pass adds.
pub const MERGE_GROUP_ROWS: u64 = 4096;
/// Calls held back for the watermark bump (with one retry) and the state write.
const COMMIT_RESERVE: u32 = 10;
/// Calls that persist a pass's progress after its last source read: the checkpoint PUT and the
/// final state write, which records it. A job that can't also complete in the same pass commits
/// in a later one, so source reads need not hold [`COMMIT_RESERVE`].
const CHECKPOINT_CALLS: u32 = 2;
/// How long a handed-on lease outlasts the wall time its turn's next pass may still use.
const HANDOFF_MARGIN_S: u64 = 5;
/// The wall time the maintenance stage needs left to chain another pass
/// (`MIN_COMPACT_PASS_WALL`); with less, a pass releases its lease instead of handing it on.
const HANDOFF_MIN_WALL_S: u64 = 5;
const MAX_DELETES: usize = 1000;
const MAX_STATE_BYTES: usize = 512 * 1024;

/// The window files a pass merges. Each has its own state record, so a lease or job held on one
/// never stalls another.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dataset {
    Request,
    /// One of [`RUM_TABLES`].
    Rum(&'static str),
}

impl Dataset {
    pub fn rum(table: &str) -> Option<Self> {
        RUM_TABLES
            .into_iter()
            .find(|t| *t == table)
            .map(Dataset::Rum)
    }

    pub fn state_key(self) -> &'static str {
        match self {
            Dataset::Request => STATE_KEY,
            Dataset::Rum(table) => {
                let at = RUM_TABLES.iter().position(|t| *t == table).unwrap_or(1);
                RUM_STATE_KEYS[at]
            }
        }
    }

    fn root(self, service: &crate::admin::Service) -> String {
        match self {
            Dataset::Request => processed_prefix(service),
            Dataset::Rum(table) => format!("{}processed/rum/{table}/", super::scope(service)),
        }
    }

    fn caller(self) -> &'static str {
        match self {
            Dataset::Request => "worker:request_compact",
            Dataset::Rum(_) => "worker:rum_compact",
        }
    }
}

/// Whether a RUM table's state (`None` when it has none yet) asks for a pass at `now`.
pub fn rum_due(state: Option<&Value>, now: u64) -> bool {
    let Some(value) = state else { return true };
    serde_json::from_value::<State>(value.clone()).map_or(true, |state| {
        state.job.is_some()
            || state.upload.is_some()
            || state.list_after.is_some()
            || state.next_check <= now
    })
}

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
    let dataset = match body["maintenance"].as_str() {
        Some("rum_compact") => match body["table"].as_str().and_then(Dataset::rum) {
            Some(dataset) => dataset,
            None => return crate::http::Response::new(400, "Invalid table").private(),
        },
        _ => Dataset::Request,
    };
    if let Some(usage) = runtime.usage_agg() {
        usage.set_caller(dataset.caller());
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
    let turn = body["turn"].as_str().zip(body["pass"].as_u64());
    let turn = turn.map(|(id, pass)| Turn {
        id,
        pass: pass as u32,
        last: body["last"].as_bool().unwrap_or(true),
    });
    match compact_pass(&store, &cdn_key, target, 1, turn, dataset) {
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
    /// RUM: the hour a scan starting afresh lists from (all retained hours when unset).
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
    /// RUM: where the scan in progress resumes its listing.
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
    /// The maintenance turn pass the lease was handed on to, as `turn:pass`. That pass takes
    /// over the lease without writing it again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handoff: Option<String>,
    /// RUM: when the table next needs a pass (see [`rum_due`]).
    #[serde(default)]
    pub next_check: u64,
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
    dataset: Dataset,
    root: String,
    /// RUM: every key listed in the hour being merged, which a new compact's tags must not name
    /// unless it is one of its sources.
    hour_keys: Vec<String>,
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
    /// vCPU spent restoring a resumed job's checkpoint, exempt from [`VCPU_STOP_MS`].
    restore_vcpu: u64,
}

/// One bounded compaction pass: abort a stale upload, delete sources past the grace, then merge
/// closed hours from the cursor while calls, wall time and vCPU allow.
pub fn request_compact<R: Runtime>(
    store: &Store<'_, R>,
    cdn_key: &str,
    target: u64,
    tail: u32,
) -> Result<Report, String> {
    request_compact_pass(store, cdn_key, target, tail, None)
}

/// A pass's place in a maintenance turn's chain of passes.
#[derive(Debug, Clone, Copy)]
pub struct Turn<'a> {
    pub id: &'a str,
    /// Numbered from 1.
    pub pass: u32,
    /// Whether the turn chains no pass after this one.
    pub last: bool,
}

/// Reads `key` past the edge cache. The KV API now and then answers 200 with a body cut short;
/// one more read clears that before the pass gives up.
fn read_fresh<R: Runtime>(
    runtime: &R,
    key: &str,
    token: &str,
) -> Result<Option<crate::admin::Record>, String> {
    match runtime.admin_get_fresh(key, token) {
        Err(error)
            if matches!(
                error.as_str(),
                "backend_json_invalid" | "backend_body_unavailable"
            ) =>
        {
            runtime.admin_get_fresh(key, token)
        }
        result => result,
    }
}

fn read_state<R: Runtime>(
    runtime: &R,
    key: &str,
    token: &str,
) -> Result<(Option<crate::admin::Record>, State), String> {
    let parse = |record: &Option<crate::admin::Record>| {
        record
            .as_ref()
            .map(|r| serde_json::from_value::<State>(r.value.clone()))
            .transpose()
            .map(Option::unwrap_or_default)
    };
    let record = read_fresh(runtime, key, token)?;
    if let Ok(state) = parse(&record) {
        return Ok((record, state));
    }
    let record = read_fresh(runtime, key, token)?;
    let state = parse(&record).map_err(|_| "compact_state_invalid".to_owned())?;
    Ok((record, state))
}

/// [`request_compact`] as one of a maintenance turn's passes.
pub fn request_compact_pass<R: Runtime>(
    store: &Store<'_, R>,
    cdn_key: &str,
    target: u64,
    tail: u32,
    turn: Option<Turn<'_>>,
) -> Result<Report, String> {
    compact_pass(store, cdn_key, target, tail, turn, Dataset::Request)
}

/// One bounded pass over `dataset`. A pass that leaves work for the turn's next pass hands it
/// the lease in its final state write, so the next pass needn't write the state just to take the
/// lease (a paced KV write, ~1 s live).
pub fn compact_pass<R: Runtime>(
    store: &Store<'_, R>,
    cdn_key: &str,
    target: u64,
    tail: u32,
    turn: Option<Turn<'_>>,
    dataset: Dataset,
) -> Result<Report, String> {
    let runtime = store.runtime;
    let now = runtime.now();
    let rum = matches!(dataset, Dataset::Rum(_));
    if let Dataset::Rum(table) = dataset {
        if Dataset::rum(table).is_none() {
            return Err("compact_dataset_invalid".into());
        }
        // Delete Data removes RUM files written before its reset; a compact of them written
        // meanwhile would bring their rows back.
        if store.service.data_reset.is_some() {
            return Ok(Report {
                stopped: Some("data_reset".into()),
                ..Report::default()
            });
        }
    }
    let token = runtime
        .secret(&store.service.fastly_token_ref)?
        .filter(|token| !token.is_empty())
        .ok_or("fastly_token_missing")?;
    let (record, state) = read_state(runtime, dataset.state_key(), &token)?;
    let watermark = if rum {
        None
    } else {
        runtime.admin_get(WATERMARK_KEY)?
    };
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
        dataset,
        root: dataset.root(store.service),
        hour_keys: Vec::new(),
        target,
        tail,
        now,
        state,
        generation: record.map(|r| r.generation),
        saved: None,
        report: Report::default(),
        changed: false,
        bump: false,
        restore_vcpu: 0,
    };
    // Only the final state write sets a handoff, so a stale read can't show one meant for this
    // pass: an earlier pass's handoff names an earlier pass.
    let handed = run.state.lease_until > now
        && turn.is_some_and(|turn| {
            run.state.handoff.as_deref() == Some(&format!("{}:{}", turn.id, turn.pass))
        });
    if run.state.lease_until > now && !handed {
        run.report.more_pending = true;
        run.report.stopped = Some("lease_held".into());
        return Ok(run.report);
    }
    if let Some(stop) = run.limit_stop(4) {
        run.report.more_pending = true;
        run.report.stopped = Some(stop.into());
        return Ok(run.report);
    }
    run.state.handoff = None;
    if !handed {
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
    }
    if let Some(upload) = run.state.upload.take() {
        run.changed = true;
        if let Err(error) = store.multipart_abort(&upload.key, &upload.id) {
            run.report.errors.push(error);
            run.state.upload = Some(upload);
        }
    }
    run.drain();
    // An in-flight ingest plan does not block a new job: compaction reads only committed
    // catalog files and publishes by watermark CAS, and the plan's commit re-reads the watermark
    // (or retries on `source_index_base_mismatch`), so both publications land.
    let mut pending = Vec::new();
    if run.state.job.is_some() {
        run.resume_job(&mut pending)?;
    } else {
        pending = run.scan()?;
    }
    if run.bump && rum {
        // RUM readers list the folder itself, so the compact is published once it exists.
        run.state.queue.extend(pending);
        run.finish_job();
        run.changed = true;
    } else if run.bump {
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
            // Ingest publishes to the same watermark several times a second, so losing both
            // CAS attempts is contention: save the finished job and let the next pass commit it.
            Err(error) if error == "compact_watermark_conflict" => {
                run.state.lease_until = 0;
                run.state.handoff = None;
                if run.changed && run.remaining() > run.tail {
                    let value = serde_json::to_value(&run.state).map_err(|e| e.to_string())?;
                    if serde_json::to_vec(&value).map_err(|e| e.to_string())?.len()
                        > MAX_STATE_BYTES
                    {
                        return Err("compact_state_limit".into());
                    }
                    run.write(&value, false)?;
                }
                run.report.compacts = 0;
                run.report.sources = 0;
                run.report.rows = 0;
                run.report.more_pending = true;
                run.report.stopped = Some("watermark_conflict".into());
                return Ok(run.report);
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
        if committed {
            run.finish_job();
        }
        run.changed = true;
    }
    run.state.lease_until = 0;
    if !rum
        && !run.bump
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
    if rum {
        let busy = run.report.more_pending
            || run.state.job.is_some()
            || run.state.upload.is_some()
            || run.state.list_after.is_some();
        let grace_end = run
            .state
            .queue
            .iter()
            .map(|queued| queued.at.saturating_add(DELETE_GRACE_S))
            .min()
            .unwrap_or(u64::MAX);
        run.state.next_check = if busy {
            now
        } else {
            now.saturating_add(RUM_CHECK_S).min(grace_end)
        };
    }
    // The turn's next pass gets at most this pass's remaining wall time, so a handed-on lease
    // lapses soon after it, or soon after this pass if no next pass comes. Counted from this
    // pass's start instead, it could lapse mid-pass and let an overlapping turn take it.
    let left = super::storage::WALL_STOP
        .saturating_sub(runtime.admin_elapsed(store.started))
        .as_secs();
    if let Some(turn) = turn.filter(|turn| {
        !turn.last
            && run.report.more_pending
            && run.report.errors.is_empty()
            && left >= HANDOFF_MIN_WALL_S
    }) {
        run.state.lease_until = runtime.now().saturating_add(left + HANDOFF_MARGIN_S);
        run.state.handoff = Some(format!("{}:{}", turn.id, turn.pass + 1));
    }
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

/// The `{plan_id}s` run-token prefix of the RUM plan the watermark has not committed, if any.
fn uncommitted_rum_plan<R: Runtime>(runtime: &R) -> Result<Option<String>, String> {
    let stream = super::rum::STREAM;
    let Some(plan) = runtime.admin_get(&plan_key(stream))? else {
        return Ok(None);
    };
    let Some(id) = plan_id_of(&plan.value).map(str::to_owned) else {
        return Ok(None);
    };
    let committed = runtime
        .admin_get(&watermark_key(stream))?
        .and_then(|record| record.value["run_id"].as_str().map(str::to_owned));
    Ok((committed.as_deref() != Some(id.as_str())).then(|| format!("{id}s")))
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

/// The run token of a window file's name.
fn file_name_run(key: &str) -> Option<&str> {
    super::file_name(key)
        .strip_prefix("window=")?
        .split('-')
        .nth(2)
}

/// The hour a window or compact file's name places it in.
fn file_hour(key: &str) -> Option<i64> {
    let start = match parse_processed(key) {
        Some(name) => name.start,
        None => i64::try_from(compact::parse_compact_name(key)?.a).ok()?,
    };
    Some(start - start.rem_euclid(3600))
}

/// Appends merged window files as one sorted row group, or each as it is when they are a lone
/// single group or don't rewrite to the same schema, row count and at most their bytes. An
/// error carries how many of them were appended before it.
fn append_merged(
    writer: &mut checkpoint::Writer,
    merge: &[(Bytes, ParquetMetaData)],
) -> Result<(), (usize, String)> {
    let single = merge.len() == 1 && merge[0].1.num_row_groups() <= 1;
    if let Some((bytes, metadata)) = (!single).then(|| rewrite(merge)).flatten() {
        if writer.accepts(&metadata) {
            return writer.append(&bytes, &metadata).map_err(|error| (0, error));
        }
    }
    for (appended, (bytes, metadata)) in merge.iter().enumerate() {
        writer
            .append(bytes, metadata)
            .map_err(|error| (appended, error))?;
    }
    Ok(())
}

fn rewrite(merge: &[(Bytes, ParquetMetaData)]) -> Option<(Bytes, ParquetMetaData)> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let mut batches = Vec::new();
    for (bytes, _) in merge {
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
            .ok()?
            .build()
            .ok()?;
        for batch in reader {
            batches.push(batch.ok()?);
        }
    }
    // The window writer's sort handles only the types window files are written with.
    let supported = |field: &arrow_schema::FieldRef| {
        use arrow_schema::{DataType, TimeUnit};
        matches!(
            field.data_type(),
            DataType::Utf8
                | DataType::Int64
                | DataType::Int32
                | DataType::Float64
                | DataType::Boolean
                | DataType::Timestamp(TimeUnit::Millisecond, _)
        )
    };
    if !batches.first()?.schema().fields().iter().all(supported) {
        return None;
    }
    let metadata = merge[0]
        .1
        .file_metadata()
        .key_value_metadata()
        .map(|kv| {
            kv.iter()
                .filter(|kv| kv.key != parquet::arrow::ARROW_SCHEMA_META_KEY)
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    let bytes = Bytes::from(
        super::convert::write_sorted(&batches.iter().collect::<Vec<_>>(), metadata).ok()?,
    );
    let rows: u64 = merge
        .iter()
        .map(|(_, m)| m.file_metadata().num_rows() as u64)
        .sum();
    let input: usize = merge.iter().map(|(b, _)| b.len()).sum();
    let reader = SerializedFileReader::new(bytes.clone()).ok()?;
    let metadata = reader.metadata().clone();
    (metadata.file_metadata().num_rows() as u64 == rows
        && metadata.num_row_groups() == 1
        && bytes.len() <= input)
        .then_some((bytes, metadata))
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
        let key = self.dataset.state_key();
        if !runtime.admin_cas(key, self.generation.as_deref(), value)? {
            let recovered = match self.saved.clone() {
                Some(own) => {
                    self.read_back(&own)?
                        && runtime.admin_cas(key, self.generation.as_deref(), value)?
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
            let record = read_fresh(runtime, self.dataset.state_key(), &token)?;
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

    /// Clears a job whose compact is published, leaving its checkpoint to be cleaned up.
    fn finish_job(&mut self) {
        if self.state.job.as_ref().is_some_and(|job| job.complete) {
            if let Some(key) = self.state.job.take().and_then(|job| job.checkpoint) {
                self.state.work_cleanup.push(key);
            }
        }
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
        // A RUM compact merged from compacts hides them, not their sources: each of them goes
        // only once its own queued sources are gone.
        let compacts: BTreeSet<&String> = match self.dataset {
            Dataset::Rum(_) => self.state.queue.iter().map(|q| &q.compact).collect(),
            Dataset::Request => BTreeSet::new(),
        };
        let mut due = Vec::new();
        let mut keys = Vec::new();
        for (index, queued) in self.state.queue.iter().enumerate() {
            if queued.at + DELETE_GRACE_S <= self.now
                && keys.len() + queued.keys.len() <= MAX_DELETES
                && !queued.keys.iter().any(|key| compacts.contains(key))
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
        } else if self.store.vcpu_ms().saturating_sub(self.restore_vcpu) >= VCPU_STOP_MS {
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

    /// Only the pinned committed catalog may supply request compaction inputs.
    fn scan(&mut self) -> Result<Vec<Queued>, String> {
        if let Dataset::Rum(_) = self.dataset {
            return self.rum_scan();
        }
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

    /// Lists the RUM table folder from the scan's cursor and merges it hour by hour.
    fn rum_scan(&mut self) -> Result<Vec<Queued>, String> {
        let mut pending = Vec::new();
        if let Some(stop) = self.limit_stop(6) {
            self.report.more_pending = true;
            self.report.stopped.get_or_insert_with(|| stop.into());
            return Ok(pending);
        }
        let uncommitted = uncommitted_rum_plan(self.store.runtime)?;
        loop {
            if let Some(stop) = self.limit_stop(4) {
                self.report.more_pending = true;
                self.report.stopped.get_or_insert_with(|| stop.into());
                return Ok(pending);
            }
            let start_after = self.state.list_after.clone().or_else(|| {
                self.state
                    .next_hour
                    .map(|hour| format!("{}{}", self.root, hour_dir(hour)))
            });
            let page = self
                .store
                .list(&self.root, start_after.as_deref(), None, 1000, None)?;
            let last = page.objects.last().map(|object| object.key.clone());
            let mut objects = page.objects;
            // A listing resumed inside an hour has passed that hour's compacts, which sort first.
            if let Some((folder, _)) = start_after
                .as_deref()
                .filter(|after| !after.ends_with('/'))
                .and_then(|after| after.rsplit_once('/'))
            {
                let compacts =
                    self.store
                        .list(&format!("{folder}/compact="), None, None, 1000, None)?;
                objects.extend(compacts.objects);
            }
            let mut hours: std::collections::BTreeMap<i64, Vec<Object>> = Default::default();
            for object in objects {
                if let Some(hour) = file_hour(&object.key) {
                    hours.entry(hour).or_default().push(object);
                }
            }
            let next = match page.next_token {
                None => None,
                // The last hour may continue on the next page: list it again whole.
                Some(_) if hours.len() > 1 => hours
                    .pop_last()
                    .map(|(hour, _)| format!("{}{}", self.root, hour_dir(hour))),
                Some(_) => last,
            };
            let first = hours.keys().next().copied();
            for (hour, objects) in hours {
                if !self.rum_hour(hour, &objects, uncommitted.as_deref(), &mut pending)? {
                    if Some(hour) != first {
                        self.state.list_after = Some(format!("{}{}", self.root, hour_dir(hour)));
                    }
                    self.changed = true;
                    self.report.more_pending = true;
                    return Ok(pending);
                }
            }
            self.changed = true;
            if next.is_none() || next == start_after {
                let now = self.now as i64;
                let floor = now - now.rem_euclid(3600) - RUM_REVISIT_S;
                self.state.list_after = None;
                self.state.next_hour = Some(floor);
                self.state
                    .skip
                    .retain(|key| file_hour(key).is_some_and(|hour| hour >= floor));
                return Ok(pending);
            }
            self.state.list_after = next;
        }
    }

    /// Re-queues the sources of a RUM hour's compacts whose queue record was lost, then merges
    /// its windows that no compact covers, are old enough and belong to a committed plan, and
    /// its compacts no other compact replaced. False when it stopped before finishing.
    fn rum_hour(
        &mut self,
        hour: i64,
        objects: &[Object],
        uncommitted: Option<&str>,
        pending: &mut Vec<Queued>,
    ) -> Result<bool, String> {
        let (valid, uncovered) = compact::covering(objects);
        let queued: BTreeSet<String> = self
            .state
            .queue
            .iter()
            .chain(pending.iter())
            .flat_map(|q| q.keys.iter())
            .cloned()
            .collect();
        let mut superseded = BTreeSet::new();
        for (compact_key, covered) in &valid {
            superseded.extend(
                covered
                    .iter()
                    .filter(|key| compact::parse_compact_name(key).is_some())
                    .cloned(),
            );
            let lost: Vec<String> = covered
                .iter()
                .filter(|key| !queued.contains(*key))
                .cloned()
                .collect();
            if !lost.is_empty() {
                self.state.queue.push(Queued {
                    compact: compact_key.clone(),
                    keys: lost,
                    at: self.now,
                    ..Queued::default()
                });
                self.changed = true;
            }
        }
        let uncovered: BTreeSet<String> = uncovered.into_iter().collect();
        let candidates: Vec<Object> = objects
            .iter()
            .filter(|object| {
                if parse_processed(&object.key).is_some() {
                    let token = file_name_run(&object.key);
                    uncovered.contains(&object.key)
                        && mtime(object).is_some_and(|at| at.saturating_add(MIN_AGE_S) <= self.now)
                        && uncommitted
                            .is_none_or(|plan| !token.is_some_and(|t| t.starts_with(plan)))
                } else {
                    valid.iter().any(|(key, _)| *key == object.key)
                        && !superseded.contains(&object.key)
                }
            })
            .cloned()
            .collect();
        self.hour_keys = objects.iter().map(|object| object.key.clone()).collect();
        self.hour(hour, &candidates, pending)
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
            let candidate = Candidate {
                key: object.key.clone(),
                start,
                end,
                rows,
                size: object.size,
            };
            candidates.push((candidate, mtime(object).unwrap_or(self.now)));
        }
        // Window files merge first, so an hour still receiving them grows by new compacts rather
        // than rewriting its earlier ones each time. Its compacts merge together once no more
        // than one window file is left and either the hour is closed, however young the
        // compacts are, or [`COMPACT_TRIGGER`] of them have piled up in the open hour.
        let windows = candidates
            .iter()
            .filter(|(c, _)| compact::parse_compact_name(&c.key).is_none())
            .count();
        let compacts = candidates.len() - windows;
        let closed = (hour + 3600).saturating_add(MIN_AGE_S as i64) <= self.now as i64;
        let closing = windows < 2 && (closed || compacts >= COMPACT_TRIGGER);
        if !closing {
            candidates.retain(|(c, _)| compact::parse_compact_name(&c.key).is_none());
        }
        let oldest = candidates
            .iter()
            .map(|(_, at)| *at)
            .min()
            .unwrap_or(self.now);
        let candidates: Vec<Candidate> = candidates.into_iter().map(|(c, _)| c).collect();
        let bytes: u64 = candidates.iter().map(|c| c.size).sum();
        if candidates.len() < 2
            || (!closing
                && bytes < self.target
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
                limit => limit,
            };
            // Two sources each under the limit still probe a pair when their sum is over it:
            // the limit is a sum of source counts learned from other files, a rough model that
            // undercounts compacts, and without the probe a closed hour of such compacts would
            // stay in pieces for good. `reject_input` settles a failed probe; `completed_job`
            // forgets the limit after a successful one.
            while take < rest.len() && take < MAX_SOURCES {
                let next = source_weight(&rest[take].key);
                let fits = weight + next <= limit || (take == 1 && weight < limit && next < limit);
                if !fits || total + rest[take].size > self.target {
                    break;
                }
                total += rest[take].size;
                weight += next;
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
        let output_prefix = match self.dataset {
            Dataset::Request => format!("{prefix}catalog1/{digest}/"),
            Dataset::Rum(_) => prefix.to_owned(),
        };
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
        // A RUM compact hides whatever its tags name, so they must name its sources and nothing
        // else listed beside it.
        if matches!(self.dataset, Dataset::Rum(_)) {
            let collides = compact::parse_compact_name(&key).is_none()
                || self
                    .hour_keys
                    .iter()
                    .any(|other| !names.contains(other) && compact::names_source(&key, other));
            if collides {
                if group.len() > 2 {
                    return self.merge(prefix, &group[..group.len() - 1], pending);
                }
                return Ok(Outcome::Consumed(1));
            }
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

    /// Stores the job's writer and records it in the state, saving the state when `save` (the
    /// pass's final state write records it otherwise).
    fn checkpoint_job(
        &mut self,
        job: &mut Job,
        writer: &checkpoint::Writer,
        save: bool,
    ) -> Result<(), String> {
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
        self.changed = true;
        if save {
            self.save()?;
        }
        Ok(())
    }

    fn completed_job(&mut self, job: &Job, pending: &mut Vec<Queued>) {
        let weight = job
            .sources
            .iter()
            .map(|c| source_weight(&c.key))
            .sum::<usize>();
        if self.state.group_limit != 0 && weight > self.state.group_limit {
            // A probe past the learned limit fit, so the limit was wrong for these sources.
            // Group freely again; a real overflow relearns it.
            self.state.group_limit = 0;
        }
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
        // fits with no other (a full compact) as it is. A failed pair probe (two sources under
        // the limit whose sum is over it) makes the heavier one the limit, so like sources are
        // left alone from now on without being skipped.
        let weights = job
            .sources
            .iter()
            .map(|source| source_weight(&source.key))
            .collect::<Vec<_>>();
        let probe = weights.len() == 2
            && self.state.group_limit != 0
            && weights.iter().all(|w| *w < self.state.group_limit)
            && weights.iter().sum::<usize>() > self.state.group_limit;
        let stopped = if job.next > 1 {
            self.state.group_limit = weights[..job.next].iter().sum();
            error.trim_start_matches("compact_")
        } else if probe {
            self.state.group_limit = weights.iter().copied().max().unwrap_or(1);
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
            let stop = self.exceeded().unwrap_or("call_budget");
            self.report.stopped.get_or_insert_with(|| stop.into());
            return Ok(());
        }
        // Restoring is the fixed cost of resuming, like worker startup: a checkpoint of several
        // MB can spend the whole stop on its own, and counted against it the job never finishes.
        let restore_start = self.store.vcpu_ms();
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
        self.restore_vcpu += self.store.vcpu_ms().saturating_sub(restore_start);
        let (mut bytes_read, mut rows_read) = (0u64, 0u64);
        let mut touched_groups = BTreeSet::new();
        let initial_next = job.next;
        let initial_parts = job.etags.len();
        let initial_copy = job.copying.clone();
        let mut merge: Vec<(Bytes, ParquetMetaData)> = Vec::new();
        let mut merge_rows = 0u64;
        while job.next + merge.len() < job.sources.len() {
            if let (true, Some(writer)) = (merge.is_empty(), writer.as_mut()) {
                self.upload_parts(&mut job, writer)?;
                if writer.buffer.len() >= PART_BYTES {
                    break;
                }
            }
            let source = job.sources[job.next + merge.len()].clone();
            let large = source.size > MAX_SOURCE_BYTES || source.rows > MAX_EXECUTION_ROWS;
            let mergeable = source.rows <= MERGE_SOURCE_ROWS;
            if !merge.is_empty()
                && (large || !mergeable || merge_rows + source.rows > MERGE_GROUP_ROWS)
            {
                let w = writer.as_mut().ok_or("compact_writer_missing")?;
                if let Err((appended, error)) = append_merged(w, &merge) {
                    job.next += appended;
                    let key = job.sources[job.next].key.clone();
                    self.reject_input(job, &key, error);
                    return Ok(());
                }
                job.next += std::mem::take(&mut merge).len();
                merge_rows = 0;
                continue;
            }
            if large {
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
                            self.checkpoint_job(&mut job, writer, false)?;
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
            if mergeable {
                if !w.accepts(reader.metadata()) {
                    self.reject_input(job, &source.key, "compact_schema_mismatch".into());
                    return Ok(());
                }
                bytes_read += source.size;
                rows_read += source.rows;
                merge_rows += source.rows;
                merge.push((bytes, reader.metadata().clone()));
                continue;
            }
            if let Err(error) = w.append(&bytes, reader.metadata()) {
                self.reject_input(job, &source.key, error);
                return Ok(());
            }
            bytes_read += source.size;
            rows_read += source.rows;
            self.upload_parts(&mut job, w)?;
            job.next += 1;
        }
        if !merge.is_empty() {
            let w = writer.as_mut().ok_or("compact_writer_missing")?;
            if let Err((appended, error)) = append_merged(w, &merge) {
                job.next += appended;
                let key = job.sources[job.next].key.clone();
                self.reject_input(job, &key, error);
                return Ok(());
            }
            job.next += merge.len();
        }
        let Some(mut writer) = writer else {
            self.report.more_pending = true;
            return Ok(());
        };
        let progressed = job.next != initial_next
            || job.etags.len() != initial_parts
            || job.copying != initial_copy;
        // Completing needs the last part, the completion and possibly a HEAD, and first the
        // checkpoint's save, so a pass that completes the upload but fails to record it finds
        // the finished job. A pass that can't complete leaves its checkpoint to the final state
        // write. The commit follows when its calls are left; otherwise the next pass commits.
        let finishing = job.next == job.sources.len()
            && self.checkpointable(3 + if progressed { 2 } else { 0 })
            && self.within_limits();
        if progressed {
            self.checkpoint_job(&mut job, &writer, finishing)?;
        }
        if !finishing {
            self.report.more_pending = true;
            if job.next == job.sources.len() {
                let stop = self.exceeded().unwrap_or("call_budget");
                self.report.stopped.get_or_insert_with(|| stop.into());
            }
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
