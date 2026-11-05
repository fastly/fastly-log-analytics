//! Deletion of raw objects after conversion and of expired Parquet datasets.
//!
//! `raw_delete` consumes only `ingest:deleteq:*` records written by the converter, so an object
//! is removed only after it was converted. A queue record is dropped once its keys are confirmed
//! deleted; keys that failed stay queued for the next tick. Failed and quarantined objects a
//! queue record carries as `held` move to an `ingest:quarantine:<plan id>` record instead; they
//! are deleted only by an admin purge.

use super::{
    hour_dir, scope,
    storage::{DeleteResult, Store},
    DELETEQ_PREFIX, QUARANTINE_PREFIX,
};
use crate::{
    admin::{transport, ApiRequest},
    http,
    shell::Runtime,
    telemetry::{write_log, LogBuffer, LogKind, OperationalLogSettings, WritableLogKind},
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{cell::Cell, collections::BTreeSet};

const QUEUES_PER_TICK: u32 = 20;
pub const WORKER_KEYS: usize = 28;
const DAY_S: i64 = 86_400;
pub const SWEEP_INTERVAL_S: u64 = 6 * 3600;
pub const VCPU_STOP_MS: u64 = 700;
/// Present while expired objects were deleted but the front's edge copies are not yet purged.
pub const PURGE_KEY: &str = "retention:purge";

/// Deletes with `DeleteObjects`; if the endpoint itself fails and the fallback is enabled, keys go
/// to up to `retention_fanout` worker calls of at most `WORKER_KEYS` each. Keys left over are
/// reported as failed so the caller requeues them.
pub fn delete_keys<R: Runtime>(
    store: &Store<'_, R>,
    keys: &[String],
    settings: &OperationalLogSettings,
    logs: &mut Vec<Value>,
) -> Result<DeleteResult, String> {
    match store.delete_objects(keys) {
        Ok(result) => Ok(result),
        Err(error) if settings.worker_delete_fallback => {
            logs.push(json!({"event": "retention.delete_objects_failed", "error": error}));
            fan_out(store, keys, settings, logs)
        }
        Err(error) => Err(error),
    }
}

fn fan_out<R: Runtime>(
    store: &Store<'_, R>,
    keys: &[String],
    settings: &OperationalLogSettings,
    logs: &mut Vec<Value>,
) -> Result<DeleteResult, String> {
    let mut result = DeleteResult::default();
    let calls = settings.retention_fanout.max(1) as usize;
    for (index, chunk) in keys.chunks(WORKER_KEYS).enumerate() {
        if index >= calls {
            result
                .failed
                .extend(keys[index * WORKER_KEYS..].iter().cloned());
            break;
        }
        let body = json!({
            "service_id": store.service.service_id, "region": store.service.fos_region,
            "bucket": store.service.fos_bucket, "prefix": scope(store.service), "keys": chunk,
        });
        let reply = match store.runtime.worker_delete(&body) {
            Ok(reply) => reply,
            Err(error) => {
                if index == 0 {
                    return Err(error);
                }
                result
                    .failed
                    .extend(keys[index * WORKER_KEYS..].iter().cloned());
                break;
            }
        };
        let names = |field: &str| -> BTreeSet<String> {
            reply[field]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|v| v.as_str().map(str::to_owned))
                .collect()
        };
        let deleted = names("deleted");
        for key in chunk {
            if deleted.contains(key) {
                result.deleted.push(key.clone());
            } else {
                result.failed.push(key.clone());
            }
        }
        if let Some(entries) = reply["logs"].as_array() {
            logs.extend(entries.iter().take(4).cloned());
        }
    }
    if let Some(usage) = store.runtime.usage_agg() {
        usage.record_deleted(&result.deleted);
    }
    Ok(result)
}

pub(crate) fn safe_scope(prefix: &str) -> bool {
    prefix.is_empty()
        || (prefix.ends_with('/')
            && !prefix.starts_with('/')
            && !prefix.contains("//")
            && prefix.split('/').all(|part| part != ".." && part != "."))
}

pub(crate) fn safe_key(key: &str, prefix: &str) -> bool {
    key.len() <= 1024
        && key.starts_with(prefix)
        && (key[prefix.len()..].starts_with("processed/")
            || key[prefix.len()..].starts_with("raw/"))
        && !key.contains("//")
        && !key.chars().any(char::is_control)
        && key.split('/').all(|part| part != ".." && part != ".")
}

/// Worker side of the fan-out: deletes at most `WORKER_KEYS` keys of one service prefix.
/// It never writes operational logs; what happened goes back in the response.
pub fn handle_worker_delete<R: Runtime>(runtime: &R, request: &http::Request) -> http::Response {
    let deny = |status: u16, text: &'static str| http::Response::new(status, text).private();
    if !super::worker::authorized(runtime, request) {
        return deny(403, "Forbidden");
    }
    let Ok(body) = serde_json::from_slice::<Value>(&request.body) else {
        return deny(400, "Invalid request");
    };
    let text = |field: &str| body[field].as_str().unwrap_or_default().to_owned();
    let (sid, region, bucket, prefix) = (
        text("service_id"),
        text("region"),
        text("bucket"),
        text("prefix"),
    );
    let keys: Vec<String> = body["keys"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|k| k.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default();
    let count = body["keys"].as_array().map_or(0, Vec::len);
    if keys.len() != count
        || count == 0
        || count > WORKER_KEYS
        || sid.is_empty()
        || !sid.chars().all(|c| c.is_ascii_alphanumeric())
        || !crate::admin::REGIONS.contains(&region.as_str())
        || !crate::admin::valid_bucket(&bucket)
        || !safe_scope(&prefix)
        || !keys.iter().all(|key| safe_key(key, &prefix))
    {
        return deny(400, "Invalid request");
    }
    let Some((access, secret_key)) = super::worker::worker_service(&sid, &region, &bucket, &prefix)
        .ok()
        .and_then(|service| crate::admin::fos_credentials(runtime, &service))
    else {
        return deny(503, "Credentials unavailable");
    };
    let (mut deleted, mut failed) = (Vec::new(), Vec::new());
    let started = std::time::Instant::now();
    for key in keys {
        if runtime.admin_elapsed(started) >= super::storage::WALL_STOP {
            failed.push(key);
            continue;
        }
        let response = runtime.admin_fos(crate::admin::FosRequest {
            method: "DELETE",
            region: &region,
            bucket: &bucket,
            key: &key,
            query: "",
            access_key: &access,
            secret_key: &secret_key,
            body: Vec::new(),
            timeout: crate::admin::transport::FOS_TIMEOUT,
            response_limit: crate::admin::transport::FOS_RESPONSE_LIMIT,
            content_md5: None,
        });
        match response {
            Ok(r) if (200..300).contains(&r.status) || r.status == 404 => deleted.push(key),
            _ => failed.push(key),
        }
    }
    let logs = json!([{"event": "retention.worker_delete", "deleted": deleted.len(), "failed": failed.len()}]);
    http::Response::new(
        200,
        json!({"deleted": deleted, "failed": failed, "logs": logs}).to_string(),
    )
    .header("content-type", "application/json")
    .private()
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DeleteReport {
    pub queues: u64,
    /// Failed and quarantined objects moved from queue records to quarantine records.
    pub quarantined: u64,
    pub deleted: u64,
    pub failed: u64,
    pub bytes: u64,
    pub more_pending: bool,
    pub backend_calls: u32,
}

struct Queue {
    key: String,
    generation: String,
    run_id: String,
    stream: Option<String>,
    keys: Vec<String>,
    sizes: Vec<u64>,
    /// Intent manifests of the committed plan; deleted with its raw objects.
    inflight: Vec<String>,
    /// Failed and quarantined objects, moved to a quarantine record and never deleted here.
    held: Vec<Value>,
    planned_at: u64,
}

fn decode_queue(key: String, generation: String, value: &Value) -> Result<Queue, String> {
    Ok(Queue {
        key,
        generation,
        run_id: value["run_id"].as_str().unwrap_or_default().to_owned(),
        stream: value["stream"].as_str().map(str::to_owned),
        keys: serde_json::from_value(value["keys"].clone()).map_err(|_| "deleteq_invalid")?,
        sizes: serde_json::from_value(value["sizes"].clone()).unwrap_or_default(),
        inflight: serde_json::from_value(value["inflight_keys"].clone()).unwrap_or_default(),
        held: serde_json::from_value(value["held"].clone()).unwrap_or_default(),
        planned_at: value["planned_at"]
            .as_u64()
            .unwrap_or_else(|| value["at"].as_u64().unwrap_or(0)),
    })
}

/// The plan of `stream` whose watermark commit has not landed yet, if any (2 calls).
fn uncommitted_plan<R: Runtime>(
    runtime: &R,
    stream: &str,
    committed: Option<&str>,
) -> Result<Option<String>, String> {
    let plan = runtime.admin_get(&super::plan_key(stream))?;
    Ok(plan
        .and_then(|r| super::plan_id_of(&r.value).map(str::to_owned))
        .filter(|run| Some(run.as_str()) != committed))
}

/// Deletes the converted raw objects named by queue records, keeping `reserve` backend calls free.
pub fn raw_delete<R: Runtime>(
    store: &Store<'_, R>,
    buffer: &LogBuffer,
    reserve: u32,
    settings: &OperationalLogSettings,
) -> Result<DeleteReport, String> {
    raw_delete_with_inline(store, buffer, reserve, settings, &[], true)
}

/// Deletes converted raw objects from in-memory `inline_queues` (committed earlier in the same
/// tick) and optionally sweeps KV `ingest:deleteq:` records when `check_kv` is true.
pub fn raw_delete_with_inline<R: Runtime>(
    store: &Store<'_, R>,
    buffer: &LogBuffer,
    reserve: u32,
    settings: &OperationalLogSettings,
    inline_queues: &[(String, Value)],
    check_kv: bool,
) -> Result<DeleteReport, String> {
    let mut report = DeleteReport::default();
    // With `delete_after` off the raw keys stay in FOS, but queue records and their inflight
    // manifests are still consumed so they do not pile up.
    let delete_raw = store.service.delete_after;
    let runtime = store.runtime;
    let started_with = runtime.admin_remaining_calls();
    let vcpu_start = runtime.vcpu_ms();
    let room = |need: u32| store.within_wall() && runtime.admin_remaining_calls() >= reserve + need;
    let mut queues = Vec::new();
    for (key, value) in inline_queues {
        queues.push(decode_queue(key.clone(), String::new(), value)?);
    }
    if check_kv {
        if !room(5) {
            report.more_pending = true;
            if queues.is_empty() {
                return Ok(report);
            }
        } else {
            let page = runtime.kv_list(DELETEQ_PREFIX, None, QUEUES_PER_TICK)?;
            report.more_pending = page.next_cursor.is_some();
            let mut committed = std::collections::BTreeMap::<&str, Option<String>>::new();
            let mut uncommitted = std::collections::BTreeMap::<&str, Option<String>>::new();
            for key in page.keys {
                if queues.iter().any(|q| q.key == key) {
                    continue;
                }
                if !room(3)
                    || (!queues.is_empty()
                        && runtime.vcpu_ms().saturating_sub(vcpu_start) >= VCPU_STOP_MS)
                {
                    report.more_pending = true;
                    break;
                }
                let Some(record) = runtime.admin_get(&key)? else {
                    continue;
                };
                let stream = if record.value["stream"] == super::rum::STREAM {
                    super::rum::STREAM
                } else {
                    "request"
                };
                if !committed.contains_key(stream) {
                    let watermark = runtime.admin_get(&super::watermark_key(stream))?;
                    committed.insert(
                        stream,
                        watermark
                            .and_then(|record| record.value["run_id"].as_str().map(str::to_owned)),
                    );
                }
                let run = key.strip_prefix(DELETEQ_PREFIX);
                if committed[stream].as_deref() != run {
                    if !uncommitted.contains_key(stream) {
                        if !room(2) {
                            report.more_pending = true;
                            break;
                        }
                        uncommitted.insert(
                            stream,
                            uncommitted_plan(runtime, stream, committed[stream].as_deref())?,
                        );
                    }
                    if uncommitted[stream].as_deref() == run {
                        continue;
                    }
                }
                queues.push(decode_queue(key, record.generation, &record.value)?);
            }
        }
    }
    let all: Vec<String> = queues
        .iter()
        .flat_map(|q| {
            q.keys
                .iter()
                .filter(|_| delete_raw)
                .chain(&q.inflight)
                .cloned()
        })
        .collect();
    if all.is_empty() && queues.is_empty() {
        report.backend_calls = started_with - runtime.admin_remaining_calls();
        return Ok(report);
    }
    if let Some(usage) = runtime.usage_agg() {
        usage.note_sizes(queues.iter().flat_map(|q| {
            q.keys
                .iter()
                .map(String::as_str)
                .zip(q.sizes.iter().copied())
        }));
    }
    let inline_retry_calls = queues
        .iter()
        .filter(|queue| queue.generation.is_empty())
        .count() as u32;
    let delete_calls = if inline_retry_calls == 0 {
        1
    } else {
        all.len().div_ceil(super::storage::DELETE_BATCH) as u32
    };
    let held_checkpoints = 2 * queues.iter().filter(|queue| !queue.held.is_empty()).count() as u32;
    let outcome = if all.is_empty() {
        Default::default()
    } else if !room(delete_calls + inline_retry_calls + held_checkpoints) {
        report.more_pending = true;
        Default::default()
    } else {
        delete_keys(store, &all, settings, &mut Vec::new())?
    };
    let deleted: std::collections::BTreeSet<&String> = outcome.deleted.iter().collect();
    let now_ms = runtime.now() as i64 * 1000;
    for queue in &queues {
        let mut remaining = (Vec::new(), Vec::new());
        for (index, key) in queue.keys.iter().enumerate().filter(|_| delete_raw) {
            let size = queue.sizes.get(index).copied().unwrap_or(0);
            if deleted.contains(key) {
                report.deleted += 1;
                report.bytes += size;
                write_log(
                    runtime,
                    buffer,
                    WritableLogKind::IngestFiles,
                    json!({
                        "run_id": queue.run_id, "dataset": "request", "raw_key": key,
                        "raw_size": size, "status": "converted", "state_version": 2,
                        "raw_deleted": true, "raw_deleted_at": now_ms,
                    }),
                );
            } else {
                remaining.0.push(key.clone());
                remaining.1.push(size);
            }
        }
        let inflight: Vec<&String> = queue
            .inflight
            .iter()
            .filter(|key| !deleted.contains(key))
            .collect();
        let emptied = remaining.0.is_empty() && inflight.is_empty();
        let is_inline = queue.generation.is_empty();
        let raw_keys = if delete_raw { queue.keys.len() } else { 0 };
        let failed = (remaining.0.len() + inflight.len()) as u64;
        let unchanged = remaining.0.len() == raw_keys && inflight.len() == queue.inflight.len();
        if !is_inline && !emptied && unchanged && queue.held.is_empty() {
            report.failed += failed;
            continue;
        }
        let kv_need = if is_inline && emptied && queue.held.is_empty() {
            0
        } else if queue.held.is_empty() {
            1
        } else {
            2
        };
        if kv_need > 0 && !room(kv_need) {
            report.more_pending = true;
            if is_inline {
                // Preserve the committed handoff even after the FOS work window closes.
                if runtime.admin_remaining_calls() <= reserve {
                    return Err("backend_budget_exhausted".into());
                }
                let value = inline_queues
                    .iter()
                    .find(|(key, _)| key == &queue.key)
                    .map(|(_, value)| value)
                    .ok_or("deleteq_invalid")?;
                runtime.admin_cas(&queue.key, None, value)?;
                report.failed += failed;
            }
            continue;
        }
        if !queue.held.is_empty() {
            // Create-only: after a crash between this and the queue update the record is there.
            runtime.admin_cas(
                &format!("{QUARANTINE_PREFIX}{}", queue.run_id),
                None,
                &json!({
                    "run_id": queue.run_id, "objects": queue.held,
                    "first_seen": queue.planned_at, "at": runtime.now(),
                }),
            )?;
            report.quarantined += queue.held.len() as u64;
        }
        if emptied {
            if !is_inline {
                runtime.kv_delete(&queue.key)?;
            }
            report.queues += 1;
        } else {
            report.failed += failed;
            let mut value: Value = json!({
                "run_id": queue.run_id, "keys": remaining.0, "sizes": remaining.1,
                "inflight_keys": inflight, "held": [], "planned_at": queue.planned_at,
                "at": runtime.now(),
            });
            if let Some(stream) = &queue.stream {
                value["stream"] = stream.clone().into();
            }
            if is_inline {
                report.more_pending = true;
                runtime.admin_cas(&queue.key, None, &value)?;
            } else {
                runtime.admin_cas(&queue.key, Some(&queue.generation), &value)?;
            }
        }
    }
    report.backend_calls = started_with - runtime.admin_remaining_calls();
    Ok(report)
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct SweepReport {
    pub datasets: Vec<Value>,
    pub deleted: u64,
    pub failed: u64,
    pub bytes: u64,
    pub more_pending: bool,
    pub backend_calls: u32,
    pub logs: Vec<Value>,
    /// Index of the first dataset left unfinished, so the next sweep can skip the finished ones.
    pub resume_from: usize,
}

#[derive(Serialize, Deserialize, Default, Clone)]
struct SweepState {
    generation: String,
    cutoff_ts: i64,
    cursor: Option<String>,
    pending: Vec<String>,
    done: bool,
    updated: u64,
}

/// The datasets a sweep covers: `(name, key prefix, retention days)`.
pub fn datasets(
    service: &crate::admin::Service,
    settings: &OperationalLogSettings,
) -> Vec<(String, String, u32)> {
    let scope = scope(service);
    let mut out = vec![
        (
            "request".to_owned(),
            format!("{scope}processed/request/"),
            service.log_retention_days,
        ),
        (
            "rum".to_owned(),
            format!("{scope}processed/rum/"),
            service.rum_retention_days,
        ),
    ];
    for kind in LogKind::ALL {
        let days = settings
            .retention_days_by_kind
            .get(kind.as_str())
            .copied()
            .unwrap_or_else(|| kind.default_retention_days());
        out.push((
            format!("ops:{}", kind.as_str()),
            format!("{scope}processed/ops/{}/", kind.as_str()),
            days,
        ));
    }
    // Last, so resume indexes persisted before summaries existed still name the same datasets.
    out.push((
        "summary:request".to_owned(),
        super::summary::summary_prefix(&scope, "request"),
        service.log_retention_days,
    ));
    out.push((
        "summary_bundle:request".to_owned(),
        super::bundle::bundle_prefix(&scope, "request"),
        service.log_retention_days,
    ));
    out
}

/// End of the folder a key lives in, or `None` if the key is not under a `year=/month=/day=`
/// (and optionally `hour=`) layout. Day-level `compact=`, `day_bundle=` and `partial=` files end
/// with their day; `hour_bundle=HH-` files end with their hour.
fn folder_end(key: &str, prefix: &str) -> Option<i64> {
    let relative = key.strip_prefix(prefix)?;
    let relative = if relative.starts_with("sha256=") {
        let (hash, rest) = relative.split_once('/')?;
        (hash.len() == 71 && hash[7..].bytes().all(|b| b.is_ascii_hexdigit())).then_some(rest)?
    } else {
        relative
    };
    let mut parts = relative.split('/');
    let mut field = |name: &str| -> Option<i64> {
        parts
            .next()?
            .strip_prefix(name)?
            .strip_prefix('=')?
            .parse()
            .ok()
    };
    let (year, month, day) = (field("year")?, field("month")?, field("day")?);
    let date = time::Date::from_calendar_date(
        i32::try_from(year).ok()?,
        time::Month::try_from(u8::try_from(month).ok()?).ok()?,
        u8::try_from(day).ok()?,
    )
    .ok()?;
    let start = date.with_hms(0, 0, 0).ok()?.assume_utc().unix_timestamp();
    let next = parts.next()?;
    if ["compact=", "day_bundle=", "partial="]
        .iter()
        .any(|p| next.starts_with(p))
    {
        return Some(start + DAY_S);
    }
    let hour: i64 = match next.strip_prefix("hour_bundle=") {
        Some(name) => name.get(..2)?.parse().ok()?,
        None => next.strip_prefix("hour=")?.parse().ok()?,
    };
    (0..24)
        .contains(&hour)
        .then_some(start + hour * 3600 + 3600)
}

fn day_name(ts: i64) -> String {
    hour_dir(ts)[..hour_dir(ts).len() - "hour=00/".len()].to_owned()
}

fn state_value(state: &SweepState) -> Value {
    serde_json::to_value(state).unwrap_or(Value::Null)
}

/// Sweeps expired objects from every dataset, or only `only` when given.
pub fn retention_sweep<R: Runtime>(
    store: &Store<'_, R>,
    settings: &OperationalLogSettings,
    reserve: u32,
    only: Option<&str>,
) -> Result<SweepReport, String> {
    retention_sweep_from(store, settings, reserve, only, 0)
}

/// Like `retention_sweep`, starting at dataset index `from` to resume an unfinished sweep.
pub fn retention_sweep_from<R: Runtime>(
    store: &Store<'_, R>,
    settings: &OperationalLogSettings,
    reserve: u32,
    only: Option<&str>,
    from: usize,
) -> Result<SweepReport, String> {
    let runtime = store.runtime;
    let started_with = runtime.admin_remaining_calls();
    let mut report = SweepReport::default();
    let room = |need: u32| store.within_wall() && runtime.admin_remaining_calls() >= reserve + need;
    let mut first_unfinished = None;
    let marked = Cell::new(false);
    let catalog_grace = if room(5) {
        super::catalog::retire_reserving(store, reserve)?
    } else {
        report.more_pending = true;
        runtime.now().saturating_add(1)
    };
    for (index, (name, prefix, days)) in datasets(store.service, settings).into_iter().enumerate() {
        if index < from || only.is_some_and(|o| o != name) || days == 0 {
            continue;
        }
        if catalog_grace > runtime.now()
            && (prefix == super::processed_prefix(store.service)
                || prefix
                    == super::summary::summary_prefix(&super::scope(store.service), "request")
                || prefix == super::bundle::bundle_prefix(&super::scope(store.service), "request"))
        {
            report.more_pending = true;
            first_unfinished.get_or_insert(index);
            continue;
        }
        // Measured for the whole execution: a maintenance step may follow a Delete Data step.
        if report.deleted > 0 && runtime.vcpu_ms() >= VCPU_STOP_MS {
            report.more_pending = true;
            first_unfinished.get_or_insert(index);
            break;
        }
        let before = (report.deleted, report.failed);
        let finished = sweep_dataset(
            store,
            &Target {
                name: &name,
                state_key: format!("retention:{name}"),
                prefix: &prefix,
                cutoff: None,
                days,
                purge: Some(&marked),
            },
            settings,
            &room,
            &mut report,
        )?;
        report.more_pending |= !finished;
        if !finished {
            first_unfinished.get_or_insert(index);
        }
        report.datasets.push(json!({
            "dataset": name, "retention_days": days, "complete": finished,
            "deleted": report.deleted - before.0, "failed": report.failed - before.1,
        }));
    }
    // Purge-pending state is read only when the sweep has room to act on it.
    let pending = marked.get() || (room(4) && runtime.admin_get(PURGE_KEY)?.is_some());
    if pending {
        if !room(3) {
            report.more_pending = true;
        } else {
            match purge_edge(store) {
                Ok(()) => runtime.kv_delete(PURGE_KEY)?,
                Err((error, retry)) => {
                    report
                        .logs
                        .push(json!({"event": "retention.purge_failed", "error": error}));
                    report.more_pending |= retry;
                }
            }
        }
    }
    report.resume_from = first_unfinished.unwrap_or(0);
    report.backend_calls = started_with.saturating_sub(runtime.admin_remaining_calls());
    Ok(report)
}

/// Hard-purges every edge copy of this service's FOS objects from the front service, so nothing
/// deleted by retention is served afterwards. A missing front or token is an install fault, not
/// worth a retry every tick; the pending marker still makes the next scheduled sweep try again.
pub(crate) fn purge_edge<R: Runtime>(store: &Store<'_, R>) -> Result<(), (String, bool)> {
    let runtime = store.runtime;
    let fault = |error: String| (error, false);
    let front = runtime
        .config("front_service_id")
        .map_err(fault)?
        .filter(|id| !id.is_empty())
        .ok_or_else(|| fault("front_service_id_missing".into()))?;
    let token = runtime
        .secret(&store.service.fastly_token_ref)
        .map_err(fault)?
        .ok_or_else(|| fault("fastly_token_missing".into()))?;
    let path = format!(
        "/service/{}/purge/flp-fos-{}",
        transport::encode(&front),
        transport::encode(&store.service.service_id)
    );
    let response = runtime
        .admin_api(ApiRequest {
            method: "POST",
            path: &path,
            query: "",
            token: &token,
            body: None,
            timeout: transport::request_timeout(
                transport::FASTLY_API_TIMEOUT,
                runtime.admin_elapsed(store.started),
            ),
        })
        .map_err(|error| (error, true))?;
    if (200..300).contains(&response.status) {
        Ok(())
    } else {
        Err((format!("purge_status_{}", response.status), true))
    }
}

struct Target<'a> {
    name: &'a str,
    state_key: String,
    prefix: &'a str,
    /// Fixed cutoff for delete-history; `None` derives it from `days` and the clock.
    cutoff: Option<i64>,
    days: u32,
    /// Set once this sweep recorded `PURGE_KEY` ahead of its first delete.
    purge: Option<&'a Cell<bool>>,
}

/// Deletes every hour folder of one operational kind that ended before `cutoff_ts`. The cutoff is
/// fixed by the caller so a resumed job keeps its generation.
pub fn history_sweep<R: Runtime>(
    store: &Store<'_, R>,
    kind: LogKind,
    cutoff_ts: i64,
    settings: &OperationalLogSettings,
    reserve: u32,
) -> Result<SweepReport, String> {
    let runtime = store.runtime;
    let started_with = runtime.admin_remaining_calls();
    let mut report = SweepReport::default();
    let room = |need: u32| store.within_wall() && runtime.admin_remaining_calls() >= reserve + need;
    let prefix = format!("{}processed/ops/{}/", scope(store.service), kind.as_str());
    let name = format!("history:{}", kind.as_str());
    let finished = sweep_dataset(
        store,
        &Target {
            name: &name,
            state_key: name.clone(),
            prefix: &prefix,
            cutoff: Some(cutoff_ts),
            days: 0,
            purge: None,
        },
        settings,
        &room,
        &mut report,
    )?;
    report.more_pending = !finished;
    report.backend_calls = started_with.saturating_sub(runtime.admin_remaining_calls());
    Ok(report)
}

fn sweep_dataset<R: Runtime>(
    store: &Store<'_, R>,
    target: &Target<'_>,
    settings: &OperationalLogSettings,
    room: &dyn Fn(u32) -> bool,
    report: &mut SweepReport,
) -> Result<bool, String> {
    let runtime = store.runtime;
    if !room(1) {
        return Ok(false);
    }
    let (name, prefix, key) = (target.name, target.prefix, target.state_key.as_str());
    let record = runtime.admin_get(key)?;
    let generation = record.as_ref().map(|r| r.generation.clone());
    let stored: Option<SweepState> = record
        .as_ref()
        .and_then(|r| serde_json::from_value(r.value.clone()).ok());
    let now = runtime.now() as i64;
    let cutoff_ts = target
        .cutoff
        .unwrap_or_else(|| (now - target.days as i64 * DAY_S).div_euclid(3600) * 3600);
    let wanted = match target.cutoff {
        Some(fixed) => format!("{name}@{fixed}"),
        None => format!("{name}@{}", day_name(cutoff_ts)),
    };
    let mut state = match stored {
        Some(state) if state.generation == wanted => state,
        previous => SweepState {
            generation: wanted,
            cutoff_ts,
            pending: previous.map(|p| p.pending).unwrap_or_default(),
            ..Default::default()
        },
    };
    // A KV read right after a write can still return the old generation, so a second CAS in
    // the same execution could lose to its own first write. Progress is kept in memory and
    // written once on the way out; a lost step only re-LISTs from the stored cursor, and
    // keys it already deleted are gone from that LIST.
    let mut save: Option<SweepState> = None;
    let mut persist = |state: &mut SweepState| -> Result<bool, String> {
        state.updated = runtime.now();
        if !room(1) {
            return Ok(false);
        }
        save = Some(state.clone());
        Ok(true)
    };
    let result = (|| loop {
        if !state.pending.is_empty() {
            let mark = target.purge.filter(|marked| !marked.get());
            if !room(2 + u32::from(mark.is_some())) {
                return Ok(false);
            }
            if let Some(marked) = mark {
                // Recorded before deleting so a crash between delete and purge still purges.
                runtime.admin_cas(PURGE_KEY, None, &json!({"since": runtime.now()}))?;
                marked.set(true);
            }
            let outcome = match delete_keys(store, &state.pending, settings, &mut report.logs) {
                Ok(outcome) => outcome,
                Err(error) => {
                    report
                        .logs
                        .push(json!({"event": "retention.delete_failed", "error": error}));
                    return Ok(false);
                }
            };
            report.deleted += outcome.deleted.len() as u64;
            report.failed += outcome.failed.len() as u64;
            state.pending = outcome.failed;
            if !persist(&mut state)? {
                return Ok(false);
            }
            if !state.pending.is_empty() {
                return Ok(false);
            }
        }
        if state.done {
            return Ok(true);
        }
        if !room(2) {
            return Ok(false);
        }
        let page = store.list(prefix, state.cursor.as_deref(), None, 1000, None)?;
        let mut expired = Vec::new();
        let mut reached_live = false;
        for object in &page.objects {
            match folder_end(&object.key, prefix) {
                Some(end) if end <= state.cutoff_ts => {
                    expired.push(object.key.clone());
                    report.bytes += object.size;
                }
                Some(_) => reached_live = true,
                None => {}
            }
        }
        state.cursor = page.objects.last().map(|o| o.key.clone());
        state.done =
            page.next_token.is_none() || (reached_live && !prefix.contains("/summary_bundle/"));
        if expired.is_empty() {
            if state.done {
                return Ok(true);
            }
            continue;
        }
        state.pending = expired;
        if !persist(&mut state)? {
            return Ok(false);
        }
    })();
    let Some(save) = save else {
        return result;
    };
    if !room(1) {
        return result.map(|_| false);
    }
    let written = runtime.admin_cas(key, generation.as_deref(), &state_value(&save));
    let finished = result?;
    if !written? {
        return Err("retention_state_conflict".into());
    }
    Ok(finished)
}

const RECONCILE_KEY: &str = "storage:reconcile";
/// Distinct `by_kind` keys kept in reconcile state; further folders fold into `other`.
pub const MAX_RECONCILE_KINDS: usize = 32;
const RECONCILE_LIST: &[(&str, &str)] = &[
    ("raw_request", "raw/request/"),
    ("raw_rum", "raw/rum/"),
    ("pq_request", "processed/request/"),
    ("pq_rum", "processed/rum/"),
    ("pq_ops", "processed/ops/"),
    // Window summaries count as request Parquet, as in `telemetry::usage`. Appended so an
    // in-flight reconcile's index keeps naming the same folders.
    ("pq_request", "processed/summary/"),
    ("pq_request", "processed/summary_bundle/"),
];

#[derive(Debug, Default, Clone, PartialEq)]
pub struct ReconcileReport {
    pub snapshot: Option<Value>,
    pub backend_calls: u32,
}

#[derive(Serialize, Deserialize, Default)]
struct ReconcileState {
    requested_at: u64,
    actor: String,
    index: usize,
    token: Option<String>,
    totals: Vec<(u64, u64)>,
    by_kind: std::collections::BTreeMap<String, (u64, u64)>,
}

/// Starts a storage reconcile unless one is already running. Returns whether it started.
pub fn request_storage_reconcile(runtime: &impl Runtime, actor: &str) -> Result<bool, String> {
    if runtime.admin_get(RECONCILE_KEY)?.is_some() {
        return Ok(false);
    }
    let state = ReconcileState {
        requested_at: runtime.now(),
        actor: actor.into(),
        totals: vec![(0, 0); RECONCILE_LIST.len()],
        ..Default::default()
    };
    let value = serde_json::to_value(&state).map_err(|_| "state_encoding_failed".to_string())?;
    runtime.admin_cas(RECONCILE_KEY, None, &value)
}

/// Advances a requested reconcile by as many LIST pages as the budget allows, then persists the
/// cursor once. Returns `None` when nothing was requested; the report carries the
/// snapshot once the last dataset is listed, and the state record is removed.
pub fn storage_reconcile_step<R: Runtime>(
    store: &Store<'_, R>,
    reserve: u32,
) -> Result<Option<ReconcileReport>, String> {
    let runtime = store.runtime;
    let started_with = runtime.admin_remaining_calls();
    let Some(record) = runtime.admin_get(RECONCILE_KEY)? else {
        return Ok(None);
    };
    let mut state: ReconcileState =
        serde_json::from_value(record.value).map_err(|_| "reconcile_state_invalid".to_string())?;
    state.totals.resize(RECONCILE_LIST.len(), (0, 0));
    let scope = scope(store.service);
    let room = |need: u32| store.within_wall() && runtime.admin_remaining_calls() >= reserve + need;
    let mut listed = false;
    // One LIST per page, and one call kept for the single cursor write below.
    while state.index < RECONCILE_LIST.len() {
        if !room(2) {
            break;
        }
        listed = true;
        let (_, folder) = RECONCILE_LIST[state.index];
        let prefix = format!("{scope}{folder}");
        let page = store.list(&prefix, None, state.token.as_deref(), 1000, None)?;
        for object in &page.objects {
            let total = &mut state.totals[state.index];
            total.0 += object.size;
            total.1 += 1;
            if RECONCILE_LIST[state.index].0 == "pq_ops" {
                if let Some(kind) = object
                    .key
                    .strip_prefix(&prefix)
                    .and_then(|k| k.split('/').next())
                {
                    let kind = if state.by_kind.contains_key(kind)
                        || state.by_kind.len() < MAX_RECONCILE_KINDS
                    {
                        kind
                    } else {
                        "other"
                    };
                    let entry = state.by_kind.entry(kind.to_owned()).or_default();
                    entry.0 += object.size;
                    entry.1 += 1;
                }
            }
        }
        match page.next_token {
            Some(token) => state.token = Some(token),
            None => {
                state.index += 1;
                state.token = None;
            }
        }
    }
    // The cursor is written once per step: a KV read right after a write may still return the
    // old generation, so a second CAS in the same execution can fail against our own write.
    if listed && state.index < RECONCILE_LIST.len() {
        let value =
            serde_json::to_value(&state).map_err(|_| "state_encoding_failed".to_string())?;
        if !runtime.admin_cas(RECONCILE_KEY, Some(&record.generation), &value)? {
            return Err("reconcile_state_conflict".into());
        }
    }
    let mut report = ReconcileReport::default();
    if state.index >= RECONCILE_LIST.len() {
        let mut datasets = serde_json::Map::new();
        for (index, (name, _)) in RECONCILE_LIST.iter().enumerate() {
            let (mut bytes, mut objects) = state.totals[index];
            if let Some(seen) = datasets.get(*name) {
                bytes += seen["bytes"].as_u64().unwrap_or(0);
                objects += seen["objects"].as_u64().unwrap_or(0);
            }
            let mut entry = json!({"bytes": bytes, "objects": objects});
            if *name == "pq_ops" {
                entry["by_kind"] = state
                    .by_kind
                    .iter()
                    .map(|(kind, (bytes, objects))| {
                        (kind.clone(), json!({"bytes": bytes, "objects": objects}))
                    })
                    .collect::<serde_json::Map<_, _>>()
                    .into();
            }
            datasets.insert((*name).into(), entry);
        }
        report.snapshot = Some(json!({
            "datasets": datasets,
            "requested_at": state.requested_at,
            "completed_at": runtime.now(),
        }));
        runtime.kv_delete(RECONCILE_KEY)?;
    }
    report.backend_calls = started_with.saturating_sub(runtime.admin_remaining_calls());
    Ok(Some(report))
}
