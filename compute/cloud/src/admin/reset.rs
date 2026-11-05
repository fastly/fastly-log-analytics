use super::{
    check_generation, decode, load_service, model::*, save_service, serialize, Context, Error,
    StageRow,
};
use crate::{
    ingest::{
        bundle, catalog, compaction, inflight_prefix, minute_dir, plan_key, raw_key_minute,
        raw_prefix, retention, scope, settle_s,
        storage::{Object, Store},
        watermark_key, BACKFILL_KEY, DELETEQ_PREFIX, FLOOR_FIELD, QUARANTINE_PREFIX, WATERMARK_KEY,
    },
    shell::Runtime,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

pub const JOB_KEY: &str = "reset:delete_data";
const MAX_MESSAGES: usize = 50;
const TERMINAL: [&str; 3] = ["complete", "failed", "cancelled"];

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    pub at: u64,
    pub text: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ResetJob {
    pub job_id: String,
    pub status: String,
    pub reset_at: u64,
    pub started_at: u64,
    pub reset_cas_at: Option<u64>,
    /// Set when the CAS was found on the watermark rather than made by this job; otherwise the
    /// grace runs [`compaction::DELETE_GRACE_S`] from `reset_cas_at`.
    pub retirement_grace_until: Option<u64>,
    /// The reset manifest the new catalog head names; cleanup must keep it.
    pub reset_manifest: Option<String>,
    /// When the fence finished and the job entered "resetting".
    pub resetting_at: Option<u64>,
    /// The edge purge has not landed yet; cleanup retries it.
    pub purge_pending: bool,
    pub deleted: BTreeMap<String, u64>,
    pub bytes: u64,
    pub pending: u64,
    pub cursors: BTreeMap<String, Option<String>>,
    pub raw_recheck_at: Option<u64>,
    /// The reset parts and the final purge that are done; cleanup finishes any others.
    pub steps_done: BTreeSet<String>,
    /// Retryable edge purge failures since the last success or give-up.
    pub purge_failures: u32,
    pub error: Option<String>,
    pub messages: VecDeque<Message>,
    pub updated_at: u64,
}

pub fn push_message(job: &mut ResetJob, at: u64, text: impl Into<String>) {
    job.messages.push_back(Message {
        at,
        text: text.into(),
    });
    while job.messages.len() > MAX_MESSAGES {
        job.messages.pop_front();
    }
}

fn load_job(context: &Context<'_, impl Runtime>) -> Result<Option<(Record, ResetJob)>, Error> {
    context
        .get(JOB_KEY)?
        .map(|record| decode(record.value.clone()).map(|job| (record, job)))
        .transpose()
}

pub fn start(
    context: &Context<'_, impl Runtime>,
    actor: &str,
    service_id: &str,
    input: Value,
) -> Result<Value, Error> {
    if input["confirm_service_id"].as_str() != Some(service_id) {
        return Err(Error::new(400, "confirm_service_id_mismatch"));
    }
    let (record, mut service) = load_service(context, service_id)?;
    check_generation(&record, &input)?;
    let existing = load_job(context)?;
    let now = context.runtime.now();
    // With no marker naming it, a job past the fence bound lost its start and its rollback.
    let live = |job: &ResetJob| {
        !TERMINAL.contains(&job.status.as_str())
            && now <= job.started_at.saturating_add(FENCE_GIVE_UP_S)
    };
    if service.pending_job_id.is_some()
        || service.data_reset.is_some()
        || existing.as_ref().is_some_and(|(_, job)| live(job))
    {
        return Err(Error::new(409, "data_reset_in_progress"));
    }
    let job_id = crate::auth::encode(
        &context
            .runtime
            .random()
            .map_err(|_| Error::new(503, "random_unavailable"))?,
    );
    let reset_at = now - now % 60;
    let mut job = ResetJob {
        job_id: job_id.clone(),
        status: "fencing".into(),
        reset_at,
        started_at: now,
        updated_at: now,
        ..ResetJob::default()
    };
    push_message(&mut job, now, "Pausing ingest for this service…");
    let previous_generation = existing
        .as_ref()
        .map(|(record, _)| record.generation.clone());
    context.cas(JOB_KEY, previous_generation.as_deref(), &serialize(&job)?)?;
    service.data_reset = Some(DataReset {
        job_id,
        reset_at,
        phase: ResetPhase::Fencing,
        started_at: now,
        actor: actor.into(),
        rum_held: false,
    });
    service.updated_at = now;
    if let Err(error) = save_service(context, &record, &service, actor, "data_reset.started") {
        rollback_job(context, &mut job, now);
        return Err(error);
    }
    let current = context
        .get(&format!("svc:{service_id}"))?
        .ok_or_else(|| Error::new(503, "state_unavailable"))?;
    Ok(json!({
        "job": serialize(&job)?,
        "service": service_value(&service, &current.generation),
        "generation": current.generation,
    }))
}

fn rollback_job(context: &Context<'_, impl Runtime>, job: &mut ResetJob, now: u64) {
    job.status = "cancelled".into();
    job.updated_at = now;
    push_message(job, now, "Delete Data did not start.");
    if let (Ok(Some(record)), Ok(value)) = (context.get(JOB_KEY), serialize(job)) {
        let _ = context.cas(JOB_KEY, Some(&record.generation), &value);
    }
}

pub fn status(context: &Context<'_, impl Runtime>, service_id: &str) -> Result<Value, Error> {
    load_service(context, service_id)?;
    match load_job(context)? {
        Some((_, job)) => serialize(&job),
        None => Ok(json!({"status": "none"})),
    }
}

pub fn cancel(
    context: &Context<'_, impl Runtime>,
    actor: &str,
    service_id: &str,
) -> Result<Value, Error> {
    let (record, mut service) = load_service(context, service_id)?;
    let (job_record, mut job) =
        load_job(context)?.ok_or_else(|| Error::new(409, "no_data_reset"))?;
    if job.reset_cas_at.is_some() || matches!(job.status.as_str(), "cleanup" | "complete") {
        return Err(Error::new(409, "reset_committed"));
    }
    let owns_marker = service
        .data_reset
        .as_ref()
        .is_some_and(|marker| marker.job_id == job.job_id && marker.phase == ResetPhase::Fencing);
    if !owns_marker || !matches!(job.status.as_str(), "fencing" | "cancelled") {
        return Err(Error::new(409, "data_reset_not_cancellable"));
    }
    let now = context.runtime.now();
    if job.status == "fencing" {
        job.status = "cancelled".into();
        job.updated_at = now;
        push_message(&mut job, now, "Delete Data cancelled. Ingest resumes.");
        context.cas(JOB_KEY, Some(&job_record.generation), &serialize(&job)?)?;
    }
    service.data_reset = None;
    service.updated_at = now;
    save_service(context, &record, &service, actor, "data_reset.cancelled")?;
    Ok(json!({"job": serialize(&job)?}))
}

pub fn fencing(service: &Service) -> bool {
    service
        .data_reset
        .as_ref()
        .is_some_and(|marker| marker.phase == ResetPhase::Fencing)
}

/// Whether RUM ingest must not run: while fencing, and in cleanup until the RUM cursor reset
/// and the RUM plan clear have landed. Request ingest resumes on time regardless.
pub fn rum_held(service: &Service) -> bool {
    service
        .data_reset
        .as_ref()
        .is_some_and(|marker| marker.phase == ResetPhase::Fencing || marker.rum_held)
}

pub(super) fn reset_stage<R: Runtime>(store: &Store<'_, R>) -> Option<StageRow> {
    let started = store.runtime.now();
    let result = advance(store)?;
    let mut row = StageRow::new("delete_data", started);
    match result {
        Ok(()) => row.outcome = "done",
        Err(error) => {
            row.outcome = "error";
            row.error = Some(error);
        }
    }
    Some(row)
}

/// One bounded Delete Data step for the cron; `None` when there is nothing to do.
pub fn advance<R: Runtime>(store: &Store<'_, R>) -> Option<Result<(), String>> {
    let marker = store.service.data_reset.as_ref()?;
    let runtime = store.runtime;
    let record = runtime.admin_get(JOB_KEY).ok().flatten()?;
    let mut job: ResetJob = serde_json::from_value(record.value).ok()?;
    if TERMINAL.contains(&job.status.as_str()) {
        if marker.job_id != job.job_id {
            return None;
        }
        if job.status == "failed" {
            // A failed job whose reset CAS landed is committed: it resumes into cleanup.
            match committed(store, &job) {
                Err(error) => return Some(Err(error)),
                Ok(Some(watermark)) => {
                    adopt(&mut job, &watermark, runtime.now());
                    return Some(step(store, &mut job, &record.generation));
                }
                Ok(None) => {}
            }
        }
        // A terminal job whose marker clear did not land retries here, so ingest resumes: a
        // failed or complete job's own clear, or a cancel or rolled-back start whose save failed.
        let stranded = marker.phase == owned_phase(&job);
        return stranded.then(|| clear_marker(store, &job));
    }
    Some(step(store, &mut job, &record.generation))
}

fn step<R: Runtime>(
    store: &Store<'_, R>,
    job: &mut ResetJob,
    generation: &str,
) -> Result<(), String> {
    let runtime = store.runtime;
    let now = runtime.now();
    let status = job.status.clone();
    let result = if status == "fencing" {
        let result = fence(store, job).map(|_| ());
        // Any stall past the bound (a held lease, a recurring error, a budget pause) fails the
        // job so ingest resumes with nothing deleted.
        if job.status == "fencing" && now >= job.started_at.saturating_add(FENCE_GIVE_UP_S) {
            job.status = "failed".into();
            job.error = Some(match &result {
                Err(error) => format!("fence_timeout: {error}"),
                Ok(()) => "fence_timeout".into(),
            });
            push_message(
                job,
                now,
                "In-flight work did not finish in time. Delete Data stopped; ingest resumes with nothing deleted.",
            );
            Ok(())
        } else {
            result
        }
    } else {
        Ok(())
    };
    // A step that fenced leaves the reset to the next one, with a fresh budget.
    let result = match result {
        Ok(()) if status == "resetting" => {
            let result = reset_state(store, job).map(|_| ());
            if job.status == "resetting" {
                reset_bound(store, job, result)
            } else {
                result
            }
        }
        Ok(()) if status == "cleanup" => {
            if !marker_cleanup(store.service) {
                resume(store, job)
            } else {
                match cleanup_step(store, job) {
                    Ok(true) => finish(store, job),
                    other => other.map(|_| ()),
                }
            }
        }
        other => other,
    };
    if let Err(error) = &result {
        job.error = Some(error.clone());
    }
    job.updated_at = now;
    let value = serde_json::to_value(&*job).map_err(|_| String::from("state_encoding_failed"))?;
    if !runtime.admin_cas(JOB_KEY, Some(generation), &value)? {
        return Err("data_reset_conflict".into());
    }
    if job.status == "failed" || job.status == "complete" {
        clear_marker(store, job)?;
    }
    result
}

/// How long "resetting" may stall, before and after its watermark CAS, before it gives up.
pub const RESET_GIVE_UP_S: u64 = 3 * super::schedule::MAINT_INTERVAL_S;
const PURGE_FAILED: &str = "edge_purge_failed: ";

/// Bounds a stalled reset so ingest never stays paused. Before the watermark CAS nothing has
/// changed, so the job fails and the fencing marker clears. After it, the reset is committed:
/// the job enters cleanup with the purge pending, and the watermark is never written again.
fn reset_bound<R: Runtime>(
    store: &Store<'_, R>,
    job: &mut ResetJob,
    result: Result<(), String>,
) -> Result<(), String> {
    let now = store.runtime.now();
    let since = *job.resetting_at.get_or_insert(now);
    let error = result.as_ref().err().cloned();
    match job.reset_cas_at {
        None if now >= since.saturating_add(RESET_GIVE_UP_S) => {
            // A CAS whose job save was lost is committed; a failed read retries next tick.
            if let Some(watermark) = committed(store, job)? {
                adopt(job, &watermark, now);
                return resume(store, job);
            }
            job.status = "failed".into();
            job.error = Some(match &error {
                Some(error) => format!("reset_timeout: {error}"),
                None => "reset_timeout".into(),
            });
            push_message(
                job,
                now,
                "The catalog reset did not start in time. Delete Data stopped; ingest resumes with nothing deleted.",
            );
            Ok(())
        }
        Some(at) if now >= at.saturating_add(RESET_GIVE_UP_S) => {
            let error = error.unwrap_or_else(|| "reset_timeout".into());
            let text = match error.strip_prefix(PURGE_FAILED) {
                Some(purge) => format!("Edge purge failed: {purge}; continuing, will retry during cleanup"),
                None => format!("Reset did not finish ({error}); continuing, will retry the edge purge during cleanup"),
            };
            job.error = Some(error);
            job.purge_pending = true;
            push_message(job, now, text);
            job.status = "cleanup".into();
            resume(store, job)
        }
        _ => result,
    }
}

/// Whether the edge purge can run: a front service and the purge token. A transient read
/// failure is an error; a missing one is reported, since the purge would be skipped.
fn purge_unready<R: Runtime>(store: &Store<'_, R>) -> Result<Option<String>, String> {
    let runtime = store.runtime;
    if runtime
        .config("front_service_id")?
        .is_none_or(|id| id.is_empty())
    {
        return Ok(Some("front_service_id_missing".into()));
    }
    if runtime.secret(&store.service.fastly_token_ref)?.is_none() {
        return Ok(Some("fastly_token_missing".into()));
    }
    Ok(None)
}

/// A tick that read a pre-marker `svc:<id>` from a POP's read cache may still start a plan until
/// the cache expires; that plan's lease plus a margin then bounds its writes.
pub const FENCE_WAIT_S: u64 =
    crate::shell::KV_READ_CACHE_TTL_S + crate::ingest::coordinator::LEASE_S + 30;
/// The wait plus three of the schedule's maintenance intervals, the cadence that drives this stage.
pub const FENCE_GIVE_UP_S: u64 = FENCE_WAIT_S + 3 * super::schedule::MAINT_INTERVAL_S;
const STREAMS: [&str; 2] = ["request", "rum"];
const PREPARING_KEYS: [&str; 3] = [
    "ingest:catalog_preparing",
    "ingest:catalog_replacement_preparing",
    "ingest:source_index_preparing",
];
/// Calls left for the job save and a failed fence's marker clear (re-read + CAS).
const CALL_RESERVE: u32 = 4;

fn lease_until(value: &Value) -> u64 {
    value["lease_until"].as_u64().unwrap_or(0)
}

fn plan_id(value: &Value) -> Option<&str> {
    crate::ingest::plan_id_of(value)
}

/// Waits out the schedule, plan and compaction leases, then abandons uncommitted plans, open
/// compaction uploads and catalog preparations. Every abandon is idempotent, so a step that
/// stops on its budget resumes from KV on the next tick.
fn fence<R: Runtime>(store: &Store<'_, R>, job: &mut ResetJob) -> Result<bool, String> {
    let runtime = store.runtime;
    let now = runtime.now();
    let waiting = |job: &mut ResetJob, until: u64| {
        push_message(
            job,
            now,
            format!(
                "Waiting for in-flight ingest to finish ({}s left)…",
                until - now
            ),
        );
    };
    let fenced_at = job.started_at.saturating_add(FENCE_WAIT_S);
    if now < fenced_at {
        waiting(job, fenced_at);
        return Ok(false);
    }
    let affordable = |calls: u32| store.can_afford(calls, CALL_RESERVE, retention::VCPU_STOP_MS);
    let paused = |job: &mut ResetJob| {
        push_message(job, now, "Fencing continues on the next tick.");
        Ok(false)
    };
    if !affordable(STREAMS.len() as u32 + 1) {
        return paused(job);
    }
    let mut plans = Vec::new();
    for stream in STREAMS {
        if let Some(record) = runtime.admin_get(&plan_key(stream))? {
            plans.push((stream, record.value));
        }
    }
    let compact = runtime.admin_get(compaction::STATE_KEY)?;
    let held = plans
        .iter()
        .map(|(_, plan)| lease_until(plan))
        .chain(compact.as_ref().map(|record| lease_until(&record.value)))
        .filter(|until| *until > now)
        .max();
    if let Some(until) = held {
        waiting(job, until);
        return Ok(false);
    }
    for (stream, plan) in &plans {
        if !affordable(3) {
            return paused(job);
        }
        let watermark = runtime.admin_get(&watermark_key(stream))?;
        let committed = watermark
            .as_ref()
            .and_then(|record| record.value["run_id"].as_str());
        let id = plan_id(plan);
        if id == committed {
            continue;
        }
        if let Some(id) = id {
            let prefix = format!("{}{id}/", inflight_prefix(store.service));
            let mut after: Option<String> = None;
            loop {
                if !affordable(2) {
                    return paused(job);
                }
                let page = store.list(&prefix, after.as_deref(), None, 1000, None)?;
                if page.objects.is_empty() {
                    break;
                }
                for object in page.objects {
                    if !affordable(2) {
                        return paused(job);
                    }
                    store.delete(&object.key)?;
                    after = Some(object.key);
                }
            }
        }
        runtime.kv_delete(&plan_key(stream))?;
        push_message(
            job,
            now,
            format!(
                "Abandoned uncommitted {stream} ingest plan {}.",
                id.unwrap_or("(unnamed)")
            ),
        );
    }
    if let Some(record) = &compact {
        let uploads: BTreeSet<(&str, &str)> =
            [&record.value["upload"], &record.value["job"]["upload"]]
                .into_iter()
                .filter_map(|upload| Some((upload["key"].as_str()?, upload["id"].as_str()?)))
                .collect();
        if !affordable(uploads.len() as u32 + 1) {
            return paused(job);
        }
        for (key, id) in &uploads {
            store.multipart_abort(key, id)?;
        }
        runtime.kv_delete(compaction::STATE_KEY)?;
        push_message(
            job,
            now,
            if uploads.is_empty() {
                "Cleared compaction state."
            } else {
                "Aborted the in-progress compaction upload and cleared compaction state."
            },
        );
    }
    if !affordable(PREPARING_KEYS.len() as u32) {
        return paused(job);
    }
    for key in PREPARING_KEYS {
        runtime.kv_delete(key)?;
    }
    job.status = "resetting".into();
    job.resetting_at = Some(now);
    job.error = None;
    push_message(job, now, "In-flight ingest fenced; resetting the catalog…");
    Ok(true)
}

/// Progress state of the stages fencing pauses, all describing pre-reset data. The reset deletes
/// it before its watermark CAS, while nothing writes it: once ingest resumes, these stages write
/// new state, and a pre-reset bundle publication resumed after the CAS would put deleted rows in
/// the new head.
const STATE_KEYS: [&str; 6] = [
    compaction::STATE_KEY,
    bundle::STATE_KEY,
    "retention:request",
    "retention:rum",
    "retention:summary:request",
    "retention:summary_bundle:request",
];
const RESET_LIST: u32 = 100;

/// `prev` with its ingest cursor and floor at `floor`: passes, swept stragglers and the raw
/// source index start over, and the straggler sweep starts at the floor. `run_id` is kept.
fn reset_cursor(prev: Option<&Value>, floor: &str, now: u64) -> Value {
    let mut value = prev.cloned().unwrap_or_else(|| json!({}));
    if let Some(object) = value.as_object_mut() {
        for field in ["raw_sources", "raw_scan", "raw_scan_due"] {
            object.remove(field);
        }
    }
    value["key"] = floor.into();
    value[FLOOR_FIELD] = floor.into();
    value["passes"] = json!([]);
    value["swept"] = json!([]);
    value["sweep"] = json!({"after": floor, "at": now});
    value["at"] = now.into();
    crate::ingest::bump_manifest_gen(prev, value)
}

/// After the fence: one overwrite CAS moves each stream's cursor and floor to the reset minute
/// and the request catalog to an empty head; stored plans and pre-reset queues are removed, the
/// edge and status caches purged, and the marker moved to cleanup, which resumes ingest. Every
/// part is idempotent, and a watermark already at this job's floor is not written again.
fn reset_state<R: Runtime>(store: &Store<'_, R>, job: &mut ResetJob) -> Result<bool, String> {
    let runtime = store.runtime;
    let now = runtime.now();
    let affordable = |calls: u32| store.can_afford(calls, CALL_RESERVE, retention::VCPU_STOP_MS);
    let paused = |job: &mut ResetJob| {
        push_message(job, now, "Resetting continues on the next tick.");
        Ok(false)
    };
    let minute = minute_dir(job.reset_at as i64);
    if job.reset_cas_at.is_none() {
        // The token read, the watermark read, the state deletes, the manifest PUT and the CAS.
        if !affordable(4 + STATE_KEYS.len() as u32) {
            return paused(job);
        }
        if let Some(error) = purge_unready(store)? {
            push_message(
                job,
                now,
                format!("Edge cache purge will be skipped: {error}"),
            );
            job.error = Some(error);
        }
        let floor = format!("{}{minute}", raw_prefix(store.service));
        let record = runtime.admin_get(WATERMARK_KEY)?;
        let prev = record.as_ref().map(|record| &record.value);
        match prev.filter(|value| value[FLOOR_FIELD].as_str() == Some(floor.as_str())) {
            // An earlier step's CAS landed but its job save did not: no reset reaches the same
            // floor twice, since each job starts after the last one's fence wait.
            Some(value) => {
                recover_times(job, value, now);
                job.reset_manifest = value["catalog"]["manifest"].as_str().map(str::to_owned);
            }
            None => {
                for key in STATE_KEYS {
                    runtime.kv_delete(key)?;
                }
                let old = catalog::head(prev.unwrap_or(&Value::Null))?;
                let (mut head, manifest) =
                    catalog::reset_head(&old, now, &store.service.service_id);
                let key = catalog::put(store, &manifest)?;
                job.reset_manifest = Some(key.clone());
                head.manifest = Some(key.clone());
                for event in &mut head.recent {
                    event.manifest = Some(key.clone());
                }
                let mut value = reset_cursor(prev, &floor, now);
                value["catalog"] = serde_json::to_value(&head).map_err(|e| e.to_string())?;
                let generation = record.as_ref().map(|record| record.generation.as_str());
                if !runtime.admin_cas(WATERMARK_KEY, generation, &value)? {
                    return Err("watermark_conflict".into());
                }
                job.reset_cas_at = Some(now);
            }
        }
    }
    if !affordable(4) {
        return paused(job);
    }
    reset_rum(store, job)?;
    job.steps_done.insert(RUM_PART.into());
    if !affordable(4) {
        return paused(job);
    }
    // A re-run after a lost save may find ingest resumed: keep what started after the reset.
    clear_pre_reset(store, job, &plan_key("request"), "created_at")?;
    clear_pre_reset(store, job, BACKFILL_KEY, "started_at")?;
    job.steps_done.insert(KEYS_PART.into());
    // Until the marker moves to cleanup nothing has resumed, so every queue record is pre-reset.
    let resumed = marker_cleanup(store.service);
    for (part, prefix) in QUEUES {
        if !clear_queue(store, job, prefix, resumed, &affordable)? {
            return paused(job);
        }
        job.steps_done.insert(part.into());
    }
    // The front service's token read, the purge and the status-cache purge.
    if !affordable(3) {
        return paused(job);
    }
    match retention::purge_edge(store) {
        Ok(()) => push_message(job, now, "Edge cache purged"),
        Err((error, true)) => return Err(format!("{PURGE_FAILED}{error}")),
        // An install fault (no front service or token) would stall ingest forever if retried.
        Err((error, false)) => {
            push_message(job, now, format!("Edge cache purge skipped: {error}"));
            job.error = Some(error);
        }
    }
    // Best effort, like every other caller: the status entries also expire by TTL.
    let _ = super::purge_status_cache(runtime, &store.service.service_id);
    job.status = "cleanup".into();
    enter_cleanup(store, job)?;
    Ok(true)
}

const RUM_PART: &str = "rum_watermark";
const KEYS_PART: &str = "keys";
const QUEUES: [(&str, &str); 2] = [
    ("deleteq", DELETEQ_PREFIX),
    ("quarantine", QUARANTINE_PREFIX),
];
const RUM_RELEASED: &str = "rum_released";
const FINAL_PURGE: &str = "final_purge";
/// Retryable edge purge failures cleanup records before it stops retrying.
const PURGE_TRIES: u32 = 3;

/// Moves the RUM watermark's cursor and floor to the reset minute, unless it is already there,
/// then clears a RUM plan created before the reset CAS.
fn reset_rum<R: Runtime>(store: &Store<'_, R>, job: &ResetJob) -> Result<(), String> {
    let runtime = store.runtime;
    let rum_key = watermark_key("rum");
    let rum_floor = format!(
        "{}raw/rum/{}",
        scope(store.service),
        minute_dir(job.reset_at as i64)
    );
    let record = runtime.admin_get(&rum_key)?;
    let prev = record.as_ref().map(|record| &record.value);
    let at_floor = prev.is_some_and(|value| value[FLOOR_FIELD].as_str() == Some(&rum_floor));
    if (prev.is_some() || store.service.rum.enabled) && !at_floor {
        let generation = record.as_ref().map(|record| record.generation.as_str());
        let value = reset_cursor(prev, &rum_floor, runtime.now());
        if !runtime.admin_cas(&rum_key, generation, &value)? {
            return Err("rum_watermark_conflict".into());
        }
    }
    clear_pre_reset(store, job, &plan_key("rum"), "created_at")
}

/// Deletes `key` unless its record's `field` is after the reset CAS: a plan or backfill that
/// started once ingest resumed is new work, not pre-reset state.
fn clear_pre_reset<R: Runtime>(
    store: &Store<'_, R>,
    job: &ResetJob,
    key: &str,
    field: &str,
) -> Result<(), String> {
    let runtime = store.runtime;
    let cutoff = job.reset_cas_at.unwrap_or(u64::MAX);
    if let Some(record) = runtime.admin_get(key)? {
        if record.value[field].as_u64().is_none_or(|at| at <= cutoff) {
            runtime.kv_delete(key)?;
        }
    }
    Ok(())
}

/// Lifts this job's RUM hold once its RUM reset landed. A marker without the hold is left alone.
fn release_rum<R: Runtime>(store: &Store<'_, R>, job: &ResetJob) -> Result<(), String> {
    let runtime = store.runtime;
    let key = format!("svc:{}", store.service.service_id);
    let record = runtime.admin_get(&key)?.ok_or("service_missing")?;
    let marker = &record.value["data_reset"];
    if marker["job_id"].as_str() != Some(job.job_id.as_str()) || marker["rum_held"] != true {
        return Ok(());
    }
    let mut value = record.value.clone();
    if let Some(marker) = value["data_reset"].as_object_mut() {
        marker.remove("rum_held");
    }
    value["updated_at"] = runtime.now().into();
    if !runtime.admin_cas(&key, Some(&record.generation), &value)? {
        return Err("service_conflict".into());
    }
    Ok(())
}

/// Moves the marker to cleanup after a committed reset that may have left parts undone, first
/// finishing what this execution's budget allows. What is left runs in cleanup; RUM ingest stays
/// held until its own reset lands, and request ingest resumes now either way.
fn resume<R: Runtime>(store: &Store<'_, R>, job: &mut ResetJob) -> Result<(), String> {
    let runtime = store.runtime;
    // Two calls more for the marker move itself. vCPU counts from the execution's start, as
    // the reset step that bounded may have spent it already.
    let affordable =
        |calls: u32| store.can_afford(calls, CALL_RESERVE + 2, retention::VCPU_STOP_MS);
    if let Err(error) = finish_reset(store, job, &affordable) {
        let now = runtime.now();
        push_message(
            job,
            now,
            format!("Some reset steps did not finish ({error}); cleanup retries them."),
        );
        job.error = Some(error);
    }
    enter_cleanup(store, job)
}

/// Deletes every queue record under `prefix`; once ingest may have resumed (`resumed`), only those
/// written by the reset CAS, as one written after it is new work. `false` when the budget ran out
/// first. Progress is kept in `job.cursors`, as the LIST page and the last key handled on it, so
/// kept records are read once rather than on every step.
fn clear_queue<R: Runtime>(
    store: &Store<'_, R>,
    job: &mut ResetJob,
    prefix: &str,
    resumed: bool,
    affordable: &dyn Fn(u32) -> bool,
) -> Result<bool, String> {
    let runtime = store.runtime;
    let (page_name, after_name) = (format!("queue:{prefix}"), format!("queue_after:{prefix}"));
    loop {
        if !affordable(1) {
            return Ok(false);
        }
        let cursor = job.cursors.get(&page_name).cloned().flatten();
        let after = job.cursors.get(&after_name).cloned().flatten();
        let page = runtime.kv_list(prefix, cursor.as_deref(), RESET_LIST)?;
        for key in &page.keys {
            if after.as_ref().is_some_and(|after| key <= after) {
                continue;
            }
            if !affordable(2) {
                return Ok(false);
            }
            if resumed {
                clear_pre_reset(store, job, key, "at")?;
            } else {
                runtime.kv_delete(key)?;
            }
            job.cursors.insert(after_name.clone(), Some(key.clone()));
        }
        job.cursors.remove(&after_name);
        match page.next_cursor {
            Some(next) => {
                job.cursors.insert(page_name.clone(), Some(next));
            }
            None => {
                job.cursors.remove(&page_name);
                return Ok(true);
            }
        }
    }
}

/// The request watermark when it carries this job's floor, which only its reset CAS writes.
fn committed<R: Runtime>(store: &Store<'_, R>, job: &ResetJob) -> Result<Option<Value>, String> {
    let floor = format!(
        "{}{}",
        raw_prefix(store.service),
        minute_dir(job.reset_at as i64)
    );
    Ok(store
        .runtime
        .admin_get(WATERMARK_KEY)?
        .map(|record| record.value)
        .filter(|value| value[FLOOR_FIELD].as_str() == Some(floor.as_str())))
}

/// The cutoff and grace of a reset CAS found on the watermark. Its `at` moves on with every
/// commit once ingest resumes, so the cutoff is the fence's end, after which no pre-reset object
/// is written, and the grace runs from that `at`, which is never before the CAS.
fn recover_times(job: &mut ResetJob, watermark: &Value, now: u64) {
    let at = watermark["at"].as_u64().unwrap_or(now);
    job.reset_cas_at = Some(job.resetting_at.map_or(at, |since| since.min(at)));
    job.retirement_grace_until = Some(at.saturating_add(compaction::DELETE_GRACE_S));
}

/// Takes a landed reset CAS as this job's commit. The steps after it may not have run, so
/// cleanup finishes them and retries the edge purge.
fn adopt(job: &mut ResetJob, watermark: &Value, now: u64) {
    if job.reset_cas_at.is_none() {
        recover_times(job, watermark, now);
    }
    if job.reset_manifest.is_none() {
        job.reset_manifest = watermark["catalog"]["manifest"].as_str().map(str::to_owned);
    }
    job.purge_pending = true;
    job.status = "cleanup".into();
    push_message(
        job,
        now,
        "The catalog reset had landed; continuing with cleanup.",
    );
}

/// The folders cleanup empties, each under `scope(service)`. `processed/ops/` is never listed.
pub const CLEANUP_PREFIXES: [&str; 10] = [
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
/// [`CLEANUP_PREFIXES`] starts with the raw folders, whose keys are judged by their minute.
const RAW_PREFIXES: usize = 2;
const RUM_PROCESSED: &str = CLEANUP_PREFIXES[3];
/// Cursor-name prefix of the one raw recheck pass.
const RECHECK: &str = "recheck:";

enum Rule {
    /// Keys whose minute is before `before`; the listing ends at the floor key.
    Raw { floor: String, before: i64 },
    /// Objects written before `cutoff` that the new head does not reference.
    Processed { cutoff: u64 },
}

/// One bounded cleanup step. It first finishes any reset part left undone and retries a pending
/// edge purge, then deletes raw keys below the floor once they have settled, with one recheck
/// pass a settle interval later, and processed objects written before the reset CAS once the
/// retirement grace is over. `true` once everything is deleted and the edge purged again.
pub fn cleanup_step<R: Runtime>(store: &Store<'_, R>, job: &mut ResetJob) -> Result<bool, String> {
    let runtime = store.runtime;
    let now = runtime.now();
    let affordable = |calls: u32| store.can_afford(calls, CALL_RESERVE, retention::VCPU_STOP_MS);
    let resumed_with_rum = store.service.data_reset.as_ref().is_some_and(|marker| {
        marker.job_id == job.job_id && marker.phase == ResetPhase::Cleanup && !marker.rum_held
    });
    if resumed_with_rum {
        // Cleanup without the hold began after the RUM reset landed; RUM ingest may run again.
        job.steps_done.insert(RUM_PART.into());
        job.steps_done.insert(RUM_RELEASED.into());
    } else if store
        .service
        .data_reset
        .as_ref()
        .is_some_and(|m| m.rum_held)
    {
        // Still held: only `release_rum` may record the release.
        job.steps_done.remove(RUM_RELEASED);
    }
    if !finish_reset(store, job, &affordable)? {
        return Ok(false);
    }
    if job.purge_pending {
        purge(store, job, &affordable);
    }
    let cas_at = job.reset_cas_at.ok_or("reset_cas_at_missing")?;
    let rule = Rule::Processed { cutoff: cas_at };
    let mut references = None;
    // RUM queries LIST this folder directly, with no catalog snapshot that the grace protects,
    // so pre-reset RUM files go first.
    if !sweep(
        store,
        job,
        "",
        RUM_PROCESSED,
        &rule,
        &mut references,
        &affordable,
    )? {
        return Ok(false);
    }
    let settle = settle_s(store.service.log_period.max(store.service.rum.log_period));
    let mut done = true;
    if now >= job.reset_at.saturating_add(settle) {
        for pass in ["", RECHECK] {
            if pass == RECHECK {
                match job.raw_recheck_at {
                    None => {
                        job.raw_recheck_at = Some(now + settle);
                        push_message(
                            job,
                            now,
                            format!("Rechecking raw logs for late uploads in {settle}s."),
                        );
                        done = false;
                        break;
                    }
                    Some(at) if now < at => {
                        done = false;
                        break;
                    }
                    Some(_) => {}
                }
            }
            for prefix in &CLEANUP_PREFIXES[..RAW_PREFIXES] {
                let rule = Rule::Raw {
                    floor: format!(
                        "{}{prefix}{}",
                        scope(store.service),
                        minute_dir(job.reset_at as i64)
                    ),
                    before: job.reset_at as i64,
                };
                if !sweep(store, job, pass, prefix, &rule, &mut None, &affordable)? {
                    return Ok(false);
                }
            }
        }
    } else {
        done = false;
    }
    let grace = job
        .retirement_grace_until
        .unwrap_or(cas_at.saturating_add(compaction::DELETE_GRACE_S));
    if now >= grace {
        for prefix in &CLEANUP_PREFIXES[RAW_PREFIXES..] {
            if !sweep(store, job, "", prefix, &rule, &mut references, &affordable)? {
                return Ok(false);
            }
        }
    } else {
        done = false;
    }
    if !done {
        return Ok(false);
    }
    // Objects read during the grace may be edge-cached; purge once more after the deletes.
    if !job.steps_done.contains(FINAL_PURGE) {
        if !purge(store, job, &affordable) {
            return Ok(false);
        }
        job.steps_done.insert(FINAL_PURGE.into());
    }
    Ok(true)
}

/// Finishes the reset parts a bounded or adopted reset may have left undone, skipping those it
/// recorded as done. Ingest may already run again, so the request plan, which may be a new one,
/// is left to the floor.
fn finish_reset<R: Runtime>(
    store: &Store<'_, R>,
    job: &mut ResetJob,
    affordable: &dyn Fn(u32) -> bool,
) -> Result<bool, String> {
    let now = store.runtime.now();
    // A failing RUM reset keeps RUM held but must not hold back the other clears.
    let mut rum_error = None;
    if !job.steps_done.contains(RUM_PART) {
        if !affordable(4) {
            return Ok(false);
        }
        match reset_rum(store, job) {
            Ok(()) => {
                job.steps_done.insert(RUM_PART.into());
                push_message(job, now, "Reset the RUM ingest cursor.");
            }
            Err(error) => rum_error = Some(error),
        }
    }
    if rum_error.is_none() && !job.steps_done.contains(RUM_RELEASED) {
        if !affordable(2) {
            return Ok(false);
        }
        release_rum(store, job)?;
        job.steps_done.insert(RUM_RELEASED.into());
        push_message(job, now, "RUM ingest resumed");
    }
    if !job.steps_done.contains(KEYS_PART) {
        if !affordable(2) {
            return Ok(false);
        }
        clear_pre_reset(store, job, BACKFILL_KEY, "started_at")?;
        job.steps_done.insert(KEYS_PART.into());
        push_message(job, now, "Cleared pre-reset ingest state.");
    }
    for (part, prefix) in QUEUES {
        if job.steps_done.contains(part) {
            continue;
        }
        if !clear_queue(store, job, prefix, true, affordable)? {
            return Ok(false);
        }
        job.steps_done.insert(part.into());
        push_message(job, now, format!("Cleared the {part} queue."));
    }
    rum_error.map_or(Ok(true), Err)
}

/// One edge and status-cache purge attempt. `true` once settled: purged, skipped for an install
/// fault, or failed [`PURGE_TRIES`] times, which is recorded and not fatal.
fn purge<R: Runtime>(
    store: &Store<'_, R>,
    job: &mut ResetJob,
    affordable: &dyn Fn(u32) -> bool,
) -> bool {
    let now = store.runtime.now();
    // The front service's token read, the purge and the status-cache purge.
    if !affordable(3) {
        return false;
    }
    let settled = match retention::purge_edge(store) {
        Ok(()) => {
            push_message(job, now, "Edge cache purged");
            true
        }
        Err((error, false)) => {
            push_message(job, now, format!("Edge cache purge skipped: {error}"));
            job.error = Some(error);
            true
        }
        Err((error, true)) => {
            job.purge_failures += 1;
            let settled = job.purge_failures >= PURGE_TRIES;
            if settled {
                push_message(
                    job,
                    now,
                    format!(
                        "Edge purge gave up after {PURGE_TRIES} tries; cached objects expire within 24h"
                    ),
                );
                job.error = Some(format!(
                    "{PURGE_FAILED}{error}; gave up after {PURGE_TRIES} tries"
                ));
            } else {
                push_message(job, now, format!("Edge purge failed: {error}; will retry"));
                job.error = Some(format!("{PURGE_FAILED}{error}"));
            }
            settled
        }
    };
    if settled {
        job.purge_pending = false;
        job.purge_failures = 0;
        // Best effort, like every other caller: the status entries also expire by TTL.
        let _ = super::purge_status_cache(store.runtime, &store.service.service_id);
    }
    settled
}

/// One pass over `prefix`, a LIST page at a time from its cursor in `job.cursors`; `true` once
/// the pass is done. A page whose deletes partly fail keeps its cursor, so the next step lists
/// it again: deleted keys are gone from that LIST, so nothing is counted twice.
fn sweep<R: Runtime>(
    store: &Store<'_, R>,
    job: &mut ResetJob,
    pass: &str,
    prefix: &str,
    rule: &Rule,
    references: &mut Option<BTreeSet<String>>,
    affordable: &dyn Fn(u32) -> bool,
) -> Result<bool, String> {
    let now = store.runtime.now();
    let name = format!("{pass}{prefix}");
    let full = format!("{}{prefix}", scope(store.service));
    loop {
        let after = match job.cursors.get(&name) {
            Some(None) => return Ok(true),
            Some(Some(key)) => Some(key.clone()),
            None => None,
        };
        if matches!(rule, Rule::Processed { .. }) && references.is_none() {
            if !affordable(4) {
                return Ok(false);
            }
            *references = Some(head_references(store, job)?);
        }
        if !affordable(2) {
            return Ok(false);
        }
        let page = store.list(&full, after.as_deref(), None, 1000, None)?;
        let mut end = page.next_token.is_none() || page.objects.is_empty();
        let mut last = after;
        let mut doomed = Vec::new();
        for object in &page.objects {
            let delete = match rule {
                Rule::Raw { floor, before } => {
                    if object.key >= *floor {
                        end = true;
                        break;
                    }
                    raw_key_minute(&object.key, &full).is_some_and(|minute| minute < *before)
                }
                Rule::Processed { cutoff } => {
                    modified(object).is_some_and(|at| at < *cutoff)
                        && !references
                            .as_ref()
                            .is_some_and(|keys| keys.contains(&object.key))
                }
            };
            last = Some(object.key.clone());
            if delete {
                doomed.push(object);
            }
        }
        if !doomed.is_empty() {
            let keys: Vec<String> = doomed.iter().map(|object| object.key.clone()).collect();
            let outcome = store.delete_objects(&keys)?;
            let deleted: BTreeSet<&String> = outcome.deleted.iter().collect();
            *job.deleted.entry(prefix.to_owned()).or_default() += deleted.len() as u64;
            job.bytes += doomed
                .iter()
                .filter(|object| deleted.contains(&object.key))
                .map(|object| object.size)
                .sum::<u64>();
            job.pending = outcome.failed.len() as u64;
            if !outcome.failed.is_empty() {
                return Err(format!(
                    "delete_failed: {} objects under {prefix}",
                    outcome.failed.len()
                ));
            }
        }
        if end {
            job.cursors.insert(name, None);
            let count = job.deleted.get(prefix).copied().unwrap_or(0);
            push_message(job, now, format!("Deleted {count} objects under {prefix}"));
            return Ok(true);
        }
        job.cursors.insert(name.clone(), last);
    }
}

fn modified(object: &Object) -> Option<u64> {
    let at = time::OffsetDateTime::parse(
        object.last_modified.as_deref()?,
        &time::format_description::well_known::Rfc3339,
    )
    .ok()?;
    u64::try_from(at.unix_timestamp()).ok()
}

/// Every processed key the request and RUM watermarks name, plus the reset manifest. Objects
/// the new head reaches only through those were all written after the reset CAS.
fn head_references<R: Runtime>(
    store: &Store<'_, R>,
    job: &ResetJob,
) -> Result<BTreeSet<String>, String> {
    let processed = format!("{}processed/", scope(store.service));
    let mut keys: BTreeSet<String> = job.reset_manifest.iter().cloned().collect();
    for key in [WATERMARK_KEY.to_owned(), watermark_key("rum")] {
        if let Some(record) = store.runtime.admin_get(&key)? {
            collect_keys(&record.value, &processed, &mut keys);
        }
    }
    Ok(keys)
}

fn collect_keys(value: &Value, prefix: &str, keys: &mut BTreeSet<String>) {
    match value {
        Value::String(text) if text.starts_with(prefix) => {
            keys.insert(text.clone());
        }
        Value::Array(items) => {
            for item in items {
                collect_keys(item, prefix, keys);
            }
        }
        Value::Object(map) => {
            for (name, item) in map {
                if name.starts_with(prefix) {
                    keys.insert(name.clone());
                }
                collect_keys(item, prefix, keys);
            }
        }
        _ => {}
    }
}

/// Requests the storage reconcile so the Usage page's figures match, then completes the job.
/// The step clears the marker with the `delete_data_completed` audit entry once this is saved,
/// so a lost save retries the finish rather than leaving a job in cleanup with no marker.
pub fn finish<R: Runtime>(store: &Store<'_, R>, job: &mut ResetJob) -> Result<(), String> {
    let runtime = store.runtime;
    if runtime.admin_remaining_calls() < 2 + CALL_RESERVE {
        return Ok(());
    }
    retention::request_storage_reconcile(runtime, "system:delete_data")?;
    let now = runtime.now();
    job.status = "complete".into();
    job.pending = 0;
    let total: u64 = job.deleted.values().sum();
    let text = format!(
        "Delete Data complete: deleted {total} objects ({} bytes).",
        job.bytes
    );
    push_message(job, now, text);
    Ok(())
}

fn marker_cleanup(service: &Service) -> bool {
    service
        .data_reset
        .as_ref()
        .is_some_and(|marker| marker.phase == ResetPhase::Cleanup)
}

/// Moves this job's marker to the cleanup phase, which resumes ingest. Without the budget it
/// waits for the next step, which retries it.
fn enter_cleanup<R: Runtime>(store: &Store<'_, R>, job: &mut ResetJob) -> Result<(), String> {
    let runtime = store.runtime;
    let now = runtime.now();
    if runtime.admin_remaining_calls() < 2 + CALL_RESERVE {
        push_message(job, now, "Resuming ingest on the next tick.");
        return Ok(());
    }
    let key = format!("svc:{}", store.service.service_id);
    let record = runtime.admin_get(&key)?.ok_or("service_missing")?;
    if record.value["data_reset"]["job_id"].as_str() != Some(job.job_id.as_str()) {
        return Err("data_reset_marker_lost".into());
    }
    let mut held = !job.steps_done.contains(RUM_PART);
    if record.value["data_reset"]["phase"] != json!(ResetPhase::Cleanup) {
        let mut value = record.value.clone();
        value["data_reset"]["phase"] = json!(ResetPhase::Cleanup);
        if held {
            value["data_reset"]["rum_held"] = true.into();
        }
        value["updated_at"] = now.into();
        if !runtime.admin_cas(&key, Some(&record.generation), &value)? {
            return Err("service_conflict".into());
        }
    } else if record.value["data_reset"]["rum_held"] != true {
        // An earlier step resumed ingest after the RUM reset landed: nothing to hold or redo.
        job.steps_done.insert(RUM_PART.into());
        job.steps_done.insert(RUM_RELEASED.into());
        push_message(job, now, "Catalog reset; ingest resumed");
        return Ok(());
    } else {
        // An earlier step held RUM ingest; the stored hold lifts only through `release_rum`.
        job.steps_done.remove(RUM_RELEASED);
        held = true;
    }
    if held {
        push_message(
            job,
            now,
            "Catalog reset; request ingest resumed, RUM ingest waits for its cursor reset",
        );
    } else {
        job.steps_done.insert(RUM_RELEASED.into());
        push_message(job, now, "Catalog reset; ingest resumed");
    }
    Ok(())
}

/// The marker phase a terminal job leaves behind: a failed or cancelled job's fencing marker, or
/// a complete job's cleanup marker.
fn owned_phase(job: &ResetJob) -> ResetPhase {
    if job.status == "complete" {
        ResetPhase::Cleanup
    } else {
        ResetPhase::Fencing
    }
}

/// Clears this terminal job's marker so ingest resumes or the next Delete Data can start, with
/// its audit entry; leaves any other marker alone.
fn clear_marker<R: Runtime>(store: &Store<'_, R>, job: &ResetJob) -> Result<(), String> {
    let runtime = store.runtime;
    let key = format!("svc:{}", store.service.service_id);
    let record = runtime.admin_get(&key)?.ok_or("service_missing")?;
    let owned = record.value["data_reset"]["job_id"].as_str() == Some(job.job_id.as_str())
        && record.value["data_reset"]["phase"] == json!(owned_phase(job));
    if !owned {
        return Ok(());
    }
    let now = runtime.now();
    let mut value = record.value.clone();
    if let Some(object) = value.as_object_mut() {
        object.remove("data_reset");
    }
    value["updated_at"] = now.into();
    let audit = match job.status.as_str() {
        "complete" => json!({"actor": "system", "at": now, "event": "delete_data_completed",
            "job_id": job.job_id, "deleted": job.deleted, "bytes": job.bytes,
            "config_version": value["config_version"]}),
        "cancelled" => json!({"actor": "system", "at": now, "event": "data_reset.cancelled",
            "job_id": job.job_id, "config_version": value["config_version"]}),
        _ => json!({"actor": "system", "at": now, "event": "data_reset.failed",
            "config_version": value["config_version"]}),
    };
    value["_audit"] = audit.clone();
    // Capped like an admin save's outbox; a full one loses only this entry, never the clear.
    match value["_audit_outbox"].as_array_mut() {
        Some(outbox) if outbox.len() < 4096 => outbox.push(audit),
        Some(_) => {}
        None => value["_audit_outbox"] = json!([audit]),
    }
    if !runtime.admin_cas(&key, Some(&record.generation), &value)? {
        return Err("service_conflict".into());
    }
    Ok(())
}
