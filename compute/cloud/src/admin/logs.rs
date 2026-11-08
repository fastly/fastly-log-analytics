//! Operational-log admin endpoints and the maintenance stages a clock tick dispatches to the
//! worker service (`/_internal/stage`).

use super::{
    admit_stage, fos_credentials, load_installed_service, reset, transport, StageRow, VcpuSpan,
};
use crate::{
    admin::{ApiRequest, Service},
    http::{Request, Response},
    ingest::{
        bundle, compaction,
        retention::{self, SWEEP_INTERVAL_S},
        scope,
        storage::Store,
    },
    shell::Runtime,
    telemetry::{
        self, compact,
        flush::{self as log_flush, ClearError, FlushStatus, FlushTarget},
        write_log, LogBuffer, LogKind, OperationalLogSettings, Tier, WritableLogKind,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::{Duration, Instant};

const JOB_KEY: &str = "logs:delete_history";
const LAST_SWEEP_KEY: &str = "retention:last_sweep";
const MAX_HISTORY_DAYS: u64 = 3650;
/// CDN key read, state read and write, and the smallest useful chunk.
const MIN_COMPACT_CALLS: u32 = 6;
/// CDN key read, state and watermark reads, a LIST, a GET and the state write.
const MIN_BUNDLE_CALLS: u32 = 6;
/// Calls the storage reconcile step is admitted with.
const RECONCILE_CALLS: u32 = 4;

pub struct Outcome {
    pub response: Response,
    /// `(action, target)` the shell records in the auth audit after a successful action.
    pub audit: Option<(String, String)>,
}

fn reply(status: u16, body: Value) -> Response {
    Response::new(status, body.to_string())
        .header("content-type", "application/json")
        .private()
}

fn fail(status: u16, detail: &str) -> Outcome {
    Outcome {
        response: reply(status, json!({"detail": detail, "error": detail})),
        audit: None,
    }
}

fn done(body: Value, audit: Option<(&str, String)>) -> Outcome {
    Outcome {
        response: reply(200, body),
        audit: audit.map(|(action, target)| (action.to_owned(), target)),
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct HistoryJob {
    kind: String,
    older_than_days: Option<u64>,
    cutoff_ts: i64,
    /// `deleting`, `purging` or `complete`.
    status: String,
    actor: String,
    started_at: u64,
    updated_at: u64,
    deleted: u64,
    error: Option<String>,
}

fn body_of(request: &Request) -> Result<Value, Outcome> {
    if request.body.is_empty() {
        return Ok(Value::Null);
    }
    if request.body.len() > 65_536 {
        return Err(fail(413, "body_too_large"));
    }
    serde_json::from_slice(&request.body).map_err(|_| fail(400, "invalid_input"))
}

struct Installed {
    service: Service,
    access_key: String,
    secret_key: String,
}

impl Installed {
    fn load(runtime: &impl Runtime) -> Result<Self, Outcome> {
        let service = load_installed_service(runtime)
            .map_err(|e| fail(503, &e))?
            .1
            .ok_or_else(|| fail(409, "service_not_configured"))?;
        let (access_key, secret_key) = fos_credentials(runtime, &service)
            .ok_or_else(|| fail(503, "fos_credentials_unavailable"))?;
        Ok(Self {
            service,
            access_key,
            secret_key,
        })
    }

    fn store<'a, R: Runtime>(&'a self, runtime: &'a R, started: Instant) -> Store<'a, R> {
        Store {
            runtime,
            service: &self.service,
            access_key: self.access_key.clone(),
            secret_key: self.secret_key.clone(),
            started,
            vcpu_start: 0,
        }
    }
}

pub fn handle(runtime: &impl Runtime, actor: &str, request: &Request) -> Outcome {
    if let Some(("/api/admin/usage/compute-billing", query)) = request.path.split_once('?') {
        if request.method == "GET" {
            return compute_billing(runtime, query);
        }
    }
    if request.method == "GET" && request.path == "/api/admin/usage/logged-traffic" {
        return logged_traffic(runtime);
    }
    let route = (request.method.as_str(), request.path.as_str());
    match route {
        ("GET", "/api/admin/operational-logging") => done(
            json!({
                "settings": telemetry::api::operational_settings(runtime),
                "defaults": OperationalLogSettings::default(),
                "dev_telemetry_available": telemetry::dev_telemetry_available(runtime),
            }),
            None,
        ),
        ("PATCH", "/api/admin/operational-logging") => patch_settings(runtime, request),
        ("GET", "/api/admin/logs/status") => status(runtime),
        ("POST", "/api/admin/logs/flush") => flush(runtime),
        ("POST", "/api/admin/logs/clear-cache") => clear_cache(runtime, request),
        ("POST", "/api/admin/logs/delete-history") => delete_history(runtime, actor, request),
        ("POST", "/api/admin/logs/sweep") => sweep(runtime),
        ("POST", "/api/admin/logs/backfill") => backfill(runtime, actor, request),
        ("POST", "/api/admin/usage/reconcile-storage") => reconcile(runtime, actor),
        _ => fail(404, "not_found"),
    }
}

fn patch_settings(runtime: &impl Runtime, request: &Request) -> Outcome {
    let patch = match body_of(request) {
        Ok(patch) => patch,
        Err(outcome) => return outcome,
    };
    match telemetry::api::patch_operational_settings(runtime, &patch) {
        Ok((settings, changed)) => {
            let audit =
                (!changed.is_empty()).then(|| ("operational_logging_updated", changed.join(",")));
            done(
                json!({
                    "settings": settings,
                    "changed": changed,
                    "dev_telemetry_available": telemetry::dev_telemetry_available(runtime),
                }),
                audit,
            )
        }
        Err(error) if error == "settings_conflict" => fail(409, &error),
        Err(error) => fail(400, &error),
    }
}

fn status(runtime: &impl Runtime) -> Outcome {
    let get = |key: &str| runtime.admin_get(key).ok().flatten().map(|r| r.value);
    let reconcile = get("storage:reconcile")
        .map(|state| json!({"requested_at": state["requested_at"], "actor": state["actor"]}));
    done(
        json!({
            "delete_history": get(JOB_KEY),
            "storage_reconcile": reconcile,
            "last_sweep": get(LAST_SWEEP_KEY),
            "backfill": get(crate::ingest::BACKFILL_KEY),
        }),
        None,
    )
}

fn flush_status(status: FlushStatus) -> &'static str {
    match status {
        FlushStatus::Unprovisioned => "unprovisioned",
        FlushStatus::NotDue => "not_due",
        FlushStatus::Busy => "busy",
        FlushStatus::Deferred => "deferred",
        FlushStatus::Stopped => "stopped",
        FlushStatus::GcPending => "gc_pending",
        FlushStatus::Committed => "committed",
    }
}

fn flush(runtime: &impl Runtime) -> Outcome {
    let installed = match Installed::load(runtime) {
        Ok(installed) => installed,
        Err(outcome) => return outcome,
    };
    let service = &installed.service;
    let ops_prefix = format!("{}processed/ops/", scope(service));
    let target = FlushTarget {
        region: &service.fos_region,
        bucket: &service.fos_bucket,
        access_key: &installed.access_key,
        secret_key: &installed.secret_key,
        ops_prefix: &ops_prefix,
    };
    let settings = telemetry::api::operational_settings(runtime);
    match log_flush::log_flush_reserving(
        runtime,
        &service.service_id,
        Some(&target),
        &settings,
        true,
        Instant::now(),
        TAIL + 2,
    ) {
        Ok(report) => {
            let _ = crate::admin::purge_status_cache(runtime, &service.service_id);
            done(
                json!({
                    "status": flush_status(report.status), "rows": report.rows,
                    "files": report.files, "purged": report.purged,
                    "outbox_depth": report.outbox_depth,
                }),
                Some(("log_flush_requested", "flush".into())),
            )
        }
        Err(error) => fail(502, &error),
    }
}

fn clear_cache(runtime: &impl Runtime, request: &Request) -> Outcome {
    let body = match body_of(request) {
        Ok(body) => body,
        Err(outcome) => return outcome,
    };
    let kind = match body["kind"].as_str() {
        Some("all") => None,
        Some(name) => match LogKind::parse(name)
            .filter(|k| k.tier() == Tier::Cache && *k != LogKind::CronTicks)
        {
            Some(kind) => Some(kind),
            None => return fail(400, "kind_not_clearable"),
        },
        None => return fail(400, "invalid_input"),
    };
    let service = match load_installed_service(runtime) {
        Ok((_, Some(service), _)) => service,
        Ok(_) => return fail(409, "service_not_configured"),
        Err(error) => return fail(503, &error),
    };
    match log_flush::clear_cache(runtime, &service.service_id, kind) {
        Ok(()) => done(
            json!({"cleared": kind.map_or("all", LogKind::as_str)}),
            Some((
                "log_cache_cleared",
                kind.map_or("all", LogKind::as_str).into(),
            )),
        ),
        Err(ClearError::Busy) => fail(409, "flush_in_progress"),
        Err(ClearError::ScanBehind) => fail(409, "scan_behind"),
        Err(ClearError::Failed(error)) => fail(502, &error),
    }
}

fn floor_hour(ts: i64) -> i64 {
    ts - ts.rem_euclid(3600)
}

fn delete_history(runtime: &impl Runtime, actor: &str, request: &Request) -> Outcome {
    let body = match body_of(request) {
        Ok(body) => body,
        Err(outcome) => return outcome,
    };
    let Some(kind) = body["kind"].as_str().and_then(LogKind::parse) else {
        return fail(400, "invalid_input");
    };
    let days = match &body["older_than_days"] {
        Value::Null => None,
        value => match value
            .as_u64()
            .filter(|d| (1..=MAX_HISTORY_DAYS).contains(d))
        {
            Some(days) => Some(days),
            None => return fail(400, "invalid_input"),
        },
    };
    let installed = match Installed::load(runtime) {
        Ok(installed) => installed,
        Err(outcome) => return outcome,
    };
    let existing = match runtime.admin_get(JOB_KEY) {
        Ok(record) => record,
        Err(error) => return fail(503, &error),
    };
    let generation = existing.as_ref().map(|r| r.generation.clone());
    if existing
        .as_ref()
        .and_then(|r| serde_json::from_value::<HistoryJob>(r.value.clone()).ok())
        .is_some_and(|job| job.status != "complete")
    {
        return fail(409, "history_job_in_progress");
    }
    let now = runtime.now();
    let cutoff_ts = match days {
        Some(days) => floor_hour(now as i64 - days as i64 * 86_400),
        None => floor_hour(now as i64) + 2 * 3600,
    };
    let mut job = HistoryJob {
        kind: kind.as_str().into(),
        older_than_days: days,
        cutoff_ts,
        status: "deleting".into(),
        actor: actor.into(),
        started_at: now,
        updated_at: now,
        deleted: 0,
        error: None,
    };
    let value = serde_json::to_value(&job).unwrap_or(Value::Null);
    match runtime.admin_cas(JOB_KEY, generation.as_deref(), &value) {
        Ok(true) => {}
        Ok(false) => return fail(409, "history_job_in_progress"),
        Err(error) => return fail(503, &error),
    }
    let settings = telemetry::api::operational_settings(runtime);
    let store = installed.store(runtime, Instant::now());
    let generation = runtime
        .admin_get(JOB_KEY)
        .ok()
        .flatten()
        .map(|r| r.generation);
    if let Err(error) = step_history(runtime, &store, &settings, &mut job, generation, 0) {
        job.error = Some(error);
    }
    let target = match days {
        Some(days) => format!("{}:older_than_days={days}", kind.as_str()),
        None => format!("{}:all", kind.as_str()),
    };
    done(
        json!({"job": job}),
        Some(("log_history_delete_started", target)),
    )
}

/// Advances a delete-history job as far as the budget allows and persists it. A job whose
/// sweep finished goes on to its one front-service purge in the same step when calls remain.
fn step_history<R: Runtime>(
    runtime: &R,
    store: &Store<'_, R>,
    settings: &OperationalLogSettings,
    job: &mut HistoryJob,
    generation: Option<String>,
    reserve: u32,
) -> Result<(), String> {
    let room = |need: u32| runtime.admin_remaining_calls() >= reserve + need;
    let kind = LogKind::parse(&job.kind).ok_or("history_job_invalid")?;
    let before = (job.status.clone(), job.deleted, job.error.clone());
    if job.status == "deleting" && room(3) {
        let report = retention::history_sweep(store, kind, job.cutoff_ts, settings, reserve)?;
        job.deleted += report.deleted;
        if !report.more_pending {
            runtime.kv_delete(&format!("history:{}", job.kind))?;
            job.status = "purging".into();
        }
    }
    if job.status == "purging" && room(3) {
        let front = runtime
            .config("front_service_id")?
            .filter(|id| !id.is_empty())
            .ok_or("front_service_id_missing")?;
        let token = runtime
            .secret(&store.service.fastly_token_ref)?
            .ok_or("fastly_token_missing")?;
        let path = format!(
            "/service/{}/purge/flp-fos-ops-{}",
            transport::encode(&front),
            job.kind
        );
        let response = runtime.admin_api(ApiRequest {
            method: "POST",
            path: &path,
            query: "",
            token: &token,
            body: None,
            timeout: transport::request_timeout(
                transport::FASTLY_API_TIMEOUT,
                runtime.admin_elapsed(store.started),
            ),
        })?;
        if (200..300).contains(&response.status) {
            job.status = "complete".into();
            job.error = None;
        } else {
            job.error = Some(format!("purge_status_{}", response.status));
        }
    }
    if (job.status.clone(), job.deleted, job.error.clone()) != before {
        job.updated_at = runtime.now();
        let value = serde_json::to_value(&*job).map_err(|_| "state_encoding_failed")?;
        runtime.admin_cas(JOB_KEY, generation.as_deref(), &value)?;
    }
    Ok(())
}

fn sweep(runtime: &impl Runtime) -> Outcome {
    let installed = match Installed::load(runtime) {
        Ok(installed) => installed,
        Err(outcome) => return outcome,
    };
    if reset::fencing(&installed.service) {
        return fail(409, "data_reset_in_progress");
    }
    let settings = telemetry::api::operational_settings(runtime);
    let last = runtime.admin_get(LAST_SWEEP_KEY).ok().flatten();
    let resume_from = last
        .as_ref()
        .filter(|record| !record.value["complete"].as_bool().unwrap_or(true))
        .map_or(0, |record| {
            record.value["next"].as_u64().unwrap_or(0) as usize
        });
    let store = installed.store(runtime, Instant::now());
    let report =
        match retention::retention_sweep_from(&store, &settings, AUDIT_TAIL, None, resume_from) {
            Ok(report) => report,
            Err(error) => return fail(502, &error),
        };
    note_sweep(
        runtime,
        last.as_ref().map(|r| r.generation.as_str()),
        report.more_pending,
        report.resume_from,
    );
    done(
        json!({
            "deleted": report.deleted, "failed": report.failed, "bytes": report.bytes,
            "more_pending": report.more_pending, "datasets": report.datasets,
            "backend_calls": report.backend_calls,
        }),
        Some(("log_retention_sweep_requested", "retention".into())),
    )
}

fn note_sweep(
    runtime: &impl Runtime,
    generation: Option<&str>,
    more_pending: bool,
    resume_from: usize,
) {
    let value = json!({"at": runtime.now(), "complete": !more_pending, "next": resume_from});
    let _ = runtime.admin_cas(LAST_SWEEP_KEY, generation, &value);
}

/// Most hours one billing read may cover: 31 days of hourly buckets per field.
const BILLING_MAX_SPAN_S: u64 = 31 * 86_400;

/// Fastly's billed Compute usage for `from`..`to` (epoch ms), to reconcile the logged counts.
fn compute_billing(runtime: &impl Runtime, query: &str) -> Outcome {
    let param = |name: &str| {
        url::form_urlencoded::parse(query.as_bytes())
            .find(|(key, _)| key == name)
            .and_then(|(_, value)| value.parse::<u64>().ok())
    };
    let (Some(from), Some(to)) = (param("from"), param("to")) else {
        return fail(400, "invalid_range");
    };
    let (from, to) = (from / 1000, to.div_ceil(1000));
    if from >= to || to - from > BILLING_MAX_SPAN_S {
        return fail(400, "invalid_range");
    }
    match runtime.compute_billing(from, to) {
        Ok(billing) => done(billing, None),
        Err(error) => fail(502, &error),
    }
}

/// Complete UTC days of logged-service traffic the cost estimate averages, as in v3's prefill.
const TRAFFIC_DAYS: u64 = 3;

/// The logged service's daily Fastly-reported requests over the last complete UTC days, with the
/// logging settings that turn them into raw files and per-day ingested Parquet row accounting.
fn logged_traffic(runtime: &impl Runtime) -> Outcome {
    let service = match load_installed_service(runtime) {
        Ok((_, Some(service), _)) => service,
        Ok(_) => return fail(409, "service_not_configured"),
        Err(error) => return fail(503, &error),
    };
    let today = runtime.now() / 86_400 * 86_400;
    match runtime.logged_traffic(&service.service_id, today - TRAFFIC_DAYS * 86_400, today) {
        Ok(mut traffic) => {
            let store = fos_credentials(runtime, &service).map(|(access_key, secret_key)| Store {
                runtime,
                service: &service,
                access_key,
                secret_key,
                started: Instant::now(),
                vcpu_start: 0,
            });
            let sample = service.sample_rate.clamp(1, 100) as f64 / 100.0;
            if let Some(days) = traffic["days"].as_array_mut() {
                for day in days {
                    let base = if service.edge_only {
                        day["edge_requests"]
                            .as_f64()
                            .or_else(|| day["requests"].as_f64())
                            .unwrap_or(0.0)
                    } else {
                        day["requests"].as_f64().unwrap_or(0.0)
                    };
                    let expected = (base * sample).round() as u64;
                    day["expected_rows"] = json!(expected);
                    if let (Some(store), Some(day_ts)) = (store.as_ref(), day["day"].as_u64()) {
                        let min_dir = crate::ingest::minute_dir(day_ts as i64);
                        let day_dir = min_dir.split("/hour=").next().unwrap_or(&min_dir);
                        let prefix = format!("{}processed/request/{day_dir}/", scope(&service));
                        if runtime.admin_remaining_calls() > 2 {
                            if let Ok(page) = store.list(&prefix, None, None, 1000, None) {
                                let mut keys: Vec<String> =
                                    page.objects.into_iter().map(|o| o.key).collect();
                                if let Some(token) = page
                                    .next_token
                                    .filter(|_| runtime.admin_remaining_calls() > 2)
                                {
                                    if let Ok(page2) =
                                        store.list(&prefix, None, Some(&token), 1000, None)
                                    {
                                        keys.extend(page2.objects.into_iter().map(|o| o.key));
                                    }
                                }
                                let rows: u64 = crate::ingest::processed_spans(&keys)
                                    .iter()
                                    .map(|(_, _, rows)| rows)
                                    .sum();
                                day["ingested_rows"] = json!(rows);
                            }
                        }
                    }
                }
            }
            traffic["logging"] = json!({
                "log_period": service.log_period, "sample_rate": service.sample_rate,
                "edge_only": service.edge_only, "delete_after": service.delete_after,
                "log_retention_days": service.log_retention_days,
                "rum_enabled": service.rum.enabled, "rum_log_period": service.rum.log_period,
                "rum_sample_rate": service.rum.sample_rate,
                "rum_retention_days": service.rum_retention_days,
                "compaction": service.compaction,
                "ingest_relay": crate::ingest::coordinator::relay_shards(runtime),
            });
            done(traffic, None)
        }
        Err(error) => fail(502, &error),
    }
}

fn opt_ts(body: &Value, key: &str) -> Result<Option<u64>, Outcome> {
    match &body[key] {
        Value::Null => Ok(None),
        value => value
            .as_u64()
            .map(Some)
            .ok_or_else(|| fail(400, "invalid_input")),
    }
}

fn backfill(runtime: &impl Runtime, actor: &str, request: &Request) -> Outcome {
    let body = match body_of(request) {
        Ok(body) => body,
        Err(outcome) => return outcome,
    };
    let from_ts = match opt_ts(&body, "from_ts") {
        Ok(ts) => ts,
        Err(outcome) => return outcome,
    };
    let to_ts = match opt_ts(&body, "to_ts") {
        Ok(ts) => ts,
        Err(outcome) => return outcome,
    };
    if let (Some(from), Some(to)) = (from_ts, to_ts) {
        if from > to {
            return fail(400, "invalid_input");
        }
    }
    let service = match load_installed_service(runtime) {
        Ok((_, Some(service), _)) => service,
        Ok(_) => return fail(409, "service_not_configured"),
        Err(error) => return fail(503, &error),
    };
    if reset::fencing(&service) {
        return fail(409, "data_reset_in_progress");
    }
    let existing = match runtime.admin_get(crate::ingest::BACKFILL_KEY) {
        Ok(record) => record,
        Err(error) => return fail(503, &error),
    };
    let generation = existing.as_ref().map(|r| r.generation.clone());
    let restart = body["restart"].as_bool().unwrap_or(false);
    if !restart
        && existing
            .as_ref()
            .and_then(|r| {
                serde_json::from_value::<crate::ingest::BackfillJob>(r.value.clone()).ok()
            })
            .is_some_and(|job| job.status == "running")
    {
        return fail(409, "backfill_in_progress");
    }
    let raw_prefix = format!("{}raw/request/", scope(&service));
    let after_key = from_ts
        .map(|from| {
            let min = from - (from % 60);
            format!("{raw_prefix}{}/", crate::ingest::minute_dir(min as i64))
        })
        .unwrap_or_default();
    let to_key = to_ts
        .map(|to| {
            let next_min = (to - (to % 60)) + 60;
            format!(
                "{raw_prefix}{}/",
                crate::ingest::minute_dir(next_min as i64)
            )
        })
        .unwrap_or_default();
    let now = runtime.now();
    let job = crate::ingest::BackfillJob {
        status: "running".into(),
        actor: actor.into(),
        started_at: now,
        updated_at: now,
        from_ts,
        to_ts,
        after_key,
        to_key,
        scanned: 0,
        planned: 0,
    };
    let value = serde_json::to_value(&job).unwrap_or(Value::Null);
    match runtime.admin_cas(crate::ingest::BACKFILL_KEY, generation.as_deref(), &value) {
        Ok(true) => {}
        Ok(false) => return fail(409, "backfill_in_progress"),
        Err(error) => return fail(503, &error),
    }
    if let Ok(Some(record)) = runtime.admin_get("cron:state") {
        let mut state = record.value;
        state["jobs"]["ingest"]["more_pending"] = Value::Bool(true);
        state["jobs"]["ingest"]["retry_after"] = Value::from(0);
        state["jobs"]["ingest"]["idle_streak"] = Value::from(0);
        let _ = runtime.admin_cas("cron:state", Some(&record.generation), &state);
    }
    let target = match (from_ts, to_ts) {
        (None, None) => "full".to_owned(),
        (from, to) => format!("{}:{}", from.unwrap_or(0), to.unwrap_or(0)),
    };
    done(
        json!({"backfill": job}),
        Some(("log_backfill_requested", target)),
    )
}

fn reconcile(runtime: &impl Runtime, actor: &str) -> Outcome {
    match retention::request_storage_reconcile(runtime, actor) {
        Ok(started) => done(
            json!({"started": started}),
            started.then(|| ("storage_reconcile_requested", "storage".into())),
        ),
        Err(error) => fail(503, &error),
    }
}

pub struct TickContext<'a> {
    pub service: &'a Service,
    pub access_key: &'a str,
    pub secret_key: &'a str,
    pub run_id: &'a str,
    pub tick_id: &'a str,
    pub trigger: &'a str,
    pub actor: &'a str,
    pub bucket: u64,
    pub started: Instant,
}

/// Maintenance stages in `cron:state.maint_next` round-robin order. Each clock tick dispatches
/// one stage execution, which runs the first of these, from `maint_next` on, that has work.
/// `request_compact` is listed for its cron row and schedule entry but runs on its own job
/// ([`compact_execution`]), not in the rotation.
pub const MAINT_STAGES: [&str; 7] = [
    "retention_sweep",
    "log_compact",
    "delete_history",
    "storage_reconcile",
    "summary_bundle",
    "request_compact",
    "delete_data",
];

/// Calls kept back for the cron_runs outbox write and the sweep marker after a stage.
const TAIL: u32 = 3;
/// Calls kept back on synchronous HTTP admin routes for `note_sweep` (1), `ProjectedStore`
/// `record_admin_action` (6 KV calls: 2 snapshot reads + 1 root CAS + 2 audit projection calls
/// + 1 projection marker), and request buffer flush (1).
const AUDIT_TAIL: u32 = 8;

pub(super) struct MaintOutcome {
    /// Index into `MAINT_STAGES` of the stage that did work, if any.
    pub(super) ran: Option<usize>,
    /// Where the next tick's stage execution starts.
    pub(super) next: usize,
    pub(super) rows: Vec<StageRow>,
    pub(super) worker_executions_metrics: Vec<telemetry::execution::WorkerExecutionMetrics>,
}

pub(super) struct CompactOutcome {
    pub(super) row: Option<StageRow>,
    pub(super) worker_executions_metrics: Vec<telemetry::execution::WorkerExecutionMetrics>,
    /// Request compaction ran and has more to do.
    pub(super) compact_pending: bool,
}

/// A maintenance execution runs no rotating stage once the Delete Data step left it past this.
const ROTATING_VCPU_MS: u64 = 300;

/// Runs the first maintenance stage at or after `start` (cyclically) that has work, and writes
/// its cron_runs row (a sweep that ran but found nothing to delete writes none, as before).
/// Stages with nothing to do cost one or two reads and pass the turn on in the same execution.
/// `next` is the stage after the one that ran, or `start` when none did. The rotation skips
/// request compaction, which has its own job and execution.
pub(super) fn maintenance_stage(
    runtime: &impl Runtime,
    tick: &TickContext<'_>,
    settings: &OperationalLogSettings,
    start: usize,
) -> MaintOutcome {
    let buffer = LogBuffer::new(&tick.service.service_id, &format!("{}-maint", tick.run_id));
    let store = Store {
        runtime,
        service: tick.service,
        access_key: tick.access_key.to_owned(),
        secret_key: tick.secret_key.to_owned(),
        started: tick.started,
        vcpu_start: 0,
    };
    let start = start % MAINT_STAGES.len();
    let mut outcome = MaintOutcome {
        ran: None,
        next: start,
        rows: Vec::new(),
        worker_executions_metrics: Vec::new(),
    };
    let fencing = reset::fencing(tick.service);
    if store.within_wall() {
        outcome.rows.extend(reset::reset_stage(&store));
    }
    // A rotating stage checks the execution's vCPU only between units of work, so none follows
    // a reset step that already spent much of the allowance.
    let spent = runtime.vcpu_ms() > ROTATING_VCPU_MS;
    let reset_ran = !outcome.rows.is_empty();
    for offset in 0..MAINT_STAGES.len() {
        let index = (start + offset) % MAINT_STAGES.len();
        if spent || !store.within_wall() {
            break;
        }
        if index == 5 || (index == 6 && reset_ran) || (fencing && matches!(index, 0 | 3 | 4)) {
            continue;
        }
        let row = match index {
            0 => sweep_stage(&store, settings),
            1 => compact_stage(&store, settings),
            2 => history_stage(&store, settings),
            3 => reconcile_stage(&store),
            4 => bundle_stage(&store),
            6 => reset::reset_stage(&store),
            _ => unreachable!("MAINT_STAGES has seven entries; request_compact has its own job"),
        };
        if let Some(row) = row {
            outcome.rows.push(row);
            outcome.ran = Some(index);
            outcome.next = (index + 1) % MAINT_STAGES.len();
            break;
        }
    }
    for row in outcome.rows.iter().filter(|row| row.outcome != "idle") {
        write_log(
            runtime,
            &buffer,
            WritableLogKind::CronRuns,
            row.json(
                tick.run_id,
                tick.tick_id,
                tick.trigger,
                tick.actor,
                tick.bucket,
            ),
        );
    }
    if let Some(error) = telemetry::flush_request_buffer(runtime, &buffer).outbox_error {
        runtime.admin_event(json!({"event": "log.outbox_error", "error": error}));
    }
    outcome
}

/// The request compaction execution: its own job, dispatched beside the ingest fan-out on its
/// own cadence, with the whole stage wall time for chained worker passes. Writes its cron_runs
/// row when it did anything.
pub(super) fn compact_execution(runtime: &impl Runtime, tick: &TickContext<'_>) -> CompactOutcome {
    let buffer = LogBuffer::new(
        &tick.service.service_id,
        &format!("{}-compact", tick.run_id),
    );
    let store = Store {
        runtime,
        service: tick.service,
        access_key: tick.access_key.to_owned(),
        secret_key: tick.secret_key.to_owned(),
        started: tick.started,
        vcpu_start: 0,
    };
    let mut outcome = CompactOutcome {
        row: None,
        worker_executions_metrics: Vec::new(),
        compact_pending: false,
    };
    if reset::fencing(tick.service) || !store.within_wall() {
        return outcome;
    }
    let row = request_compact_stage(
        &store,
        &buffer,
        tick.run_id,
        &mut outcome.worker_executions_metrics,
        Duration::ZERO,
    );
    if let Some(row) = row {
        outcome.compact_pending = compact_backlog(&row);
        if row.outcome != "idle" {
            write_log(
                runtime,
                &buffer,
                WritableLogKind::CronRuns,
                row.json(
                    tick.run_id,
                    tick.tick_id,
                    tick.trigger,
                    tick.actor,
                    tick.bucket,
                ),
            );
        }
        outcome.row = Some(row);
    }
    if let Some(error) = telemetry::flush_request_buffer(runtime, &buffer).outbox_error {
        runtime.admin_event(json!({"event": "log.outbox_error", "error": error}));
    }
    outcome
}

/// A failed run retries on the regular cadence rather than the catch-up one.
fn compact_backlog(row: &StageRow) -> bool {
    row.more_pending && row.outcome != "error"
}

fn enough(runtime: &impl Runtime, needed: u32) -> bool {
    admit_stage(runtime.admin_remaining_calls(), TAIL + needed)
}

fn caller(runtime: &impl Runtime, name: &'static str) {
    if let Some(usage) = runtime.usage_agg() {
        usage.set_caller(name);
    }
}

/// The retention sweep, every `SWEEP_INTERVAL_S` or until a started sweep completes.
fn sweep_stage<R: Runtime>(
    store: &Store<'_, R>,
    settings: &OperationalLogSettings,
) -> Option<StageRow> {
    let runtime = store.runtime;
    caller(runtime, "cron:retention");
    if !enough(runtime, 5) {
        return None;
    }
    let last = runtime.admin_get(LAST_SWEEP_KEY).ok()?;
    let resume_from = last
        .as_ref()
        .filter(|record| !record.value["complete"].as_bool().unwrap_or(true))
        .map_or(0, |record| {
            record.value["next"].as_u64().unwrap_or(0) as usize
        });
    let due = last.as_ref().is_none_or(|record| {
        !record.value["complete"].as_bool().unwrap_or(false)
            || runtime
                .now()
                .saturating_sub(record.value["at"].as_u64().unwrap_or(0))
                >= SWEEP_INTERVAL_S
    });
    if !due {
        return None;
    }
    let mut row = StageRow::new("retention_sweep", runtime.now());
    let span = VcpuSpan::start(runtime);
    let began = Instant::now();
    let before = runtime.admin_remaining_calls();
    match retention::retention_sweep_from(store, settings, TAIL + 1, None, resume_from) {
        Ok(report) => {
            row.deleted = report.deleted;
            row.bytes = report.bytes;
            row.more_pending = report.more_pending;
            row.details = Some(json!({"datasets": report.datasets, "failed": report.failed}));
            // Logged even when nothing expired, so the six-hourly cadence shows in cron_runs.
            row.outcome = if report.more_pending {
                "partial"
            } else {
                "done"
            };
            note_sweep(
                runtime,
                last.as_ref().map(|r| r.generation.as_str()),
                report.more_pending,
                report.resume_from,
            );
        }
        Err(error) => {
            row.outcome = "error";
            row.error = Some(error);
        }
    }
    finish_row(runtime, &mut row, before, began, &span);
    Some(row)
}

fn compact_stage<R: Runtime>(
    store: &Store<'_, R>,
    settings: &OperationalLogSettings,
) -> Option<StageRow> {
    let runtime = store.runtime;
    if !settings.compaction_enabled || !enough(runtime, MIN_COMPACT_CALLS) {
        return None;
    }
    let cdn_key = runtime
        .secret("cdn_key")
        .ok()
        .flatten()
        .filter(|key| !key.is_empty())?;
    caller(runtime, "cron:log_compact");
    let mut row = StageRow::new("log_compact", runtime.now());
    let span = VcpuSpan::start(runtime);
    let began = Instant::now();
    let before = runtime.admin_remaining_calls();
    match compact::log_compact(store, &cdn_key, settings, TAIL + 1) {
        Ok(report) => {
            row.listed = report.sources;
            row.files = u64::from(report.chunks);
            row.rows = report.rows;
            row.bytes = report.bytes_written;
            row.deleted = report.deleted;
            row.more_pending = report.more_pending;
            row.error = report.errors.first().cloned();
            if report.chunks > 0 || report.deleted > 0 || report.failed > 0 {
                row.outcome = if report.more_pending {
                    "partial"
                } else {
                    "done"
                };
            } else if row.error.is_some() {
                row.outcome = "error";
            } else if report.more_pending {
                row.outcome = "partial";
            }
        }
        Err(error) => {
            row.outcome = "error";
            row.error = Some(error);
        }
    }
    if row.outcome == "idle" {
        return None;
    }
    finish_row(runtime, &mut row, before, began, &span);
    Some(row)
}

/// Compaction passes one maintenance turn may chain. Each is a fresh worker execution with its
/// own 32-call budget (`ingest::worker::WORKER_CALLS`). A 32-source compact uploads in about two
/// and commits in the next, which writes the state once.
const MAX_COMPACT_PASSES: u32 = 8;
/// A further pass starts only with this much of the stage's wall time left.
const MIN_COMPACT_PASS_WALL: Duration = Duration::from_secs(5);
/// How long past a pass's own wall budget the stage waits for its reply (the old 40 s/45 s gap).
const COMPACT_REPLY_MARGIN: Duration = Duration::from_secs(5);

/// Merges small request window files when the service's `compaction` setting asks for it,
/// chaining worker passes while the stage has calls and wall time and the last pass progressed.
/// Chains passes until `reserve` is all that is left of the stage's wall time. Each pass is
/// numbered within the turn `turn`, so a pass can hand its lease to the next.
fn request_compact_stage<R: Runtime>(
    store: &Store<'_, R>,
    buffer: &LogBuffer,
    turn: &str,
    worker_metrics: &mut Vec<telemetry::execution::WorkerExecutionMetrics>,
    reserve: Duration,
) -> Option<StageRow> {
    let runtime = store.runtime;
    store.service.compaction.target_bytes()?;
    if !enough(runtime, MIN_COMPACT_CALLS) {
        return None;
    }
    let watermark = runtime.admin_get(crate::ingest::WATERMARK_KEY).ok()?;
    let catalog = crate::ingest::catalog::head(
        watermark
            .as_ref()
            .map(|record| &record.value)
            .unwrap_or(&Value::Null),
    )
    .ok()?;
    if catalog.version != crate::ingest::catalog::VERSION
        || (!catalog.compaction_due && catalog.next_compaction_check > runtime.now())
    {
        return None;
    }
    caller(runtime, "cron:request_compact");
    let mut row = StageRow::new("request_compact", runtime.now());
    let span = VcpuSpan::start(runtime);
    let began = Instant::now();
    let before = runtime.admin_remaining_calls();
    let mut worker_vcpu = 0;
    let mut passes = 0u32;
    let mut stopped = None;
    let mut lease_generation = None;
    let mut errors = Vec::new();
    let mut failed = 0;
    loop {
        let left = crate::ingest::storage::WALL_STOP
            .saturating_sub(runtime.admin_elapsed(store.started))
            .saturating_sub(reserve);
        if passes > 0
            && (passes >= MAX_COMPACT_PASSES || left < MIN_COMPACT_PASS_WALL || !enough(runtime, 1))
        {
            break;
        }
        let wall = left.max(MIN_COMPACT_PASS_WALL);
        passes += 1;
        let requests = [(
            crate::ingest::worker::PATH.to_owned(),
            json!({
                "maintenance": "request_compact",
                "service": store.service,
                "wall_ms": wall.as_millis() as u64,
                "turn": turn,
                "pass": passes,
                "last": passes >= MAX_COMPACT_PASSES,
            }),
        )];
        let reply = runtime
            .worker_dispatch(&requests, wall + COMPACT_REPLY_MARGIN)
            .into_iter()
            .next()
            .unwrap_or_else(|| Err("compact_worker_reply_missing".into()));
        worker_metrics.push(telemetry::execution::WorkerExecutionMetrics::from_reply(
            "request_compact",
            None,
            reply.as_ref().ok(),
        ));
        let report = reply.and_then(|reply| {
            worker_vcpu += reply["vcpu_ms"].as_u64().unwrap_or(0);
            if let Some(rows) = reply["usage"].as_array() {
                for value in rows {
                    write_log(runtime, buffer, WritableLogKind::FosUsage, value.clone());
                }
            }
            if reply["status"] != "ok" {
                return Err(reply["error"]
                    .as_str()
                    .unwrap_or("compact_worker_failed")
                    .to_owned());
            }
            serde_json::from_value::<compaction::Report>(reply["report"].clone())
                .map_err(|e| e.to_string())
        });
        let report = match report {
            Ok(report) => report,
            Err(error) => {
                errors.push(error);
                break;
            }
        };
        row.listed += report.sources;
        row.files += u64::from(report.compacts);
        row.rows += report.rows;
        row.bytes += report.bytes_written;
        row.deleted += report.deleted;
        failed += report.failed;
        row.more_pending = report.more_pending;
        // Call, wall, vCPU and heap stops end only that execution; a fresh pass resets them.
        let blocked = matches!(
            report.stopped.as_deref(),
            Some("lease_held" | "lease_conflict")
        );
        stopped = report.stopped.or(stopped);
        lease_generation = report.lease_generation.or(lease_generation);
        let done = !report.more_pending || !report.errors.is_empty();
        errors.extend(report.errors);
        if done || blocked {
            break;
        }
    }
    row.error = errors.first().cloned();
    if passes > 1 || stopped.is_some() {
        row.details = Some(json!({
            "passes": passes,
            "stopped": stopped,
            "lease_generation": lease_generation,
        }));
    }
    if row.files > 0 || row.deleted > 0 || failed > 0 {
        row.outcome = if row.more_pending { "partial" } else { "done" };
    } else if row.error.is_some() {
        row.outcome = "error";
    } else if row.more_pending {
        row.outcome = "partial";
    }
    if row.outcome == "idle" {
        return None;
    }
    finish_row(runtime, &mut row, before, began, &span);
    row.vcpu_ms += worker_vcpu;
    Some(row)
}

/// Merges closed hours' window summaries into hour bundles and closed days into day bundles.
fn bundle_stage<R: Runtime>(store: &Store<'_, R>) -> Option<StageRow> {
    let runtime = store.runtime;
    if !enough(runtime, MIN_BUNDLE_CALLS) {
        return None;
    }
    let cdn_key = runtime
        .secret("cdn_key")
        .ok()
        .flatten()
        .filter(|key| !key.is_empty())?;
    caller(runtime, "cron:summary_bundle");
    let mut row = StageRow::new("summary_bundle", runtime.now());
    let span = VcpuSpan::start(runtime);
    let began = Instant::now();
    let before = runtime.admin_remaining_calls();
    match bundle::summary_bundle(store, &cdn_key, TAIL + 1) {
        Ok(report) => {
            row.listed = report.sources;
            row.files = u64::from(report.hours + report.days + report.partials);
            row.rows = report.rows;
            row.bytes = report.bytes_written;
            row.deleted = report.deleted;
            row.more_pending = report.more_pending;
            row.error = report.errors.first().cloned();
            row.details = Some(json!({
                "hours": report.hours,
                "days": report.days,
                "partials": report.partials,
                "stopped": report.stopped,
            }));
            if row.files > 0 || report.deleted > 0 {
                row.outcome = if report.more_pending {
                    "partial"
                } else {
                    "done"
                };
            } else if row.error.is_some() {
                row.outcome = "error";
            } else if report.more_pending {
                row.outcome = "partial";
            }
        }
        Err(error) => {
            row.outcome = "error";
            row.error = Some(error);
        }
    }
    if row.outcome == "idle" {
        return None;
    }
    finish_row(runtime, &mut row, before, began, &span);
    Some(row)
}

fn history_stage<R: Runtime>(
    store: &Store<'_, R>,
    settings: &OperationalLogSettings,
) -> Option<StageRow> {
    let runtime = store.runtime;
    caller(runtime, "cron:retention");
    if !enough(runtime, 4) {
        return None;
    }
    let record = runtime.admin_get(JOB_KEY).ok().flatten()?;
    let mut job = serde_json::from_value::<HistoryJob>(record.value).ok()?;
    if job.status == "complete" {
        return None;
    }
    let mut row = StageRow::new("delete_history", runtime.now());
    let span = VcpuSpan::start(runtime);
    let began = Instant::now();
    let before = runtime.admin_remaining_calls();
    let deleted_before = job.deleted;
    match step_history(
        runtime,
        store,
        settings,
        &mut job,
        Some(record.generation),
        TAIL,
    ) {
        Ok(()) => {
            row.deleted = job.deleted - deleted_before;
            row.more_pending = job.status != "complete";
            row.outcome = if row.more_pending { "partial" } else { "done" };
            row.details = Some(json!({"kind": job.kind, "status": job.status}));
            row.error = job.error.clone();
        }
        Err(error) => {
            row.outcome = "error";
            row.error = Some(error);
        }
    }
    finish_row(runtime, &mut row, before, began, &span);
    Some(row)
}

fn reconcile_stage<R: Runtime>(store: &Store<'_, R>) -> Option<StageRow> {
    let runtime = store.runtime;
    caller(runtime, "cron:storage_reconcile");
    if !enough(runtime, RECONCILE_CALLS) {
        return None;
    }
    let began = Instant::now();
    let span = VcpuSpan::start(runtime);
    let before = runtime.admin_remaining_calls();
    let mut row = StageRow::new("storage_reconcile", runtime.now());
    match retention::storage_reconcile_step(store, TAIL) {
        Ok(Some(report)) => {
            if let Some(snapshot) = report.snapshot {
                row.outcome = "done";
                row.details = Some(snapshot);
            } else {
                row.outcome = "partial";
                row.more_pending = true;
            }
        }
        Ok(None) => return None,
        Err(error) if error == "reconcile_state_conflict" => {
            row.outcome = "busy";
            row.more_pending = true;
        }
        Err(error) => {
            row.outcome = "error";
            row.error = Some(error);
        }
    }
    finish_row(runtime, &mut row, before, began, &span);
    Some(row)
}

fn finish_row(
    runtime: &impl Runtime,
    row: &mut StageRow,
    calls_before: u32,
    began: Instant,
    span: &VcpuSpan,
) {
    row.calls = calls_before.saturating_sub(runtime.admin_remaining_calls());
    row.duration_ms = began.elapsed().as_millis() as u64;
    row.apply_span(runtime, span);
}
