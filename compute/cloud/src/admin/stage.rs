//! The maintenance stage worker: `POST /_internal/stage` on flp-worker.
//!
//! Each clock tick sends one stage request in its ingest dispatch batch, so maintenance costs
//! the tick one backend call and runs with the worker execution's own call budget, wall time
//! and Core Cache transaction limit. The worker runs the first `logs::MAINT_STAGES` entry from
//! `start` on that has work and writes that stage's `cron_runs` row through the KV outbox
//! itself, so the row is recorded exactly once whether or not the tick hears the reply. Its FOS
//! usage and Fastly API rows go back in the reply for the tick to write; they are best-effort
//! and lost if the tick gives up on the worker before it answers.

use super::logs::{self, TickContext, MAINT_STAGES};
use super::{fos_credentials, load_installed_service};
use crate::http;
use crate::shell::Runtime;
use crate::telemetry;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::{Duration, Instant};

pub const PATH: &str = "/_internal/stage";

/// The stage stops starting FOS calls this long after it starts. A tick dispatches by
/// `DISPATCH_BY` (10 s) and waits until `DRAIN_AT` (32 s), so a stage that finishes by then
/// is always heard.
pub const STAGE_STOP: Duration = Duration::from_secs(20);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StageRequest {
    pub service_id: String,
    pub run_id: String,
    pub tick_id: String,
    pub trigger: String,
    pub actor: String,
    pub bucket: u64,
    pub start: usize,
}

fn valid(request: &StageRequest) -> bool {
    let word = |value: &str, max: usize| {
        !value.is_empty()
            && value.len() <= max
            && value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    };
    word(&request.service_id, 64)
        && word(&request.run_id, 96)
        && (request.tick_id.is_empty() || word(&request.tick_id, 96))
        && matches!(request.trigger.as_str(), "clock" | "manual")
        && !request.actor.is_empty()
        && request.actor.len() <= 128
        && request.start < MAINT_STAGES.len()
}

/// Parses a stage reply's `next` pointer; `None` if the reply is not a stage answer.
pub fn reply_next(reply: &Value) -> Option<usize> {
    reply["next"]
        .as_u64()
        .map(|next| next as usize)
        .filter(|next| *next < MAINT_STAGES.len())
}

/// Whether the reply's request compaction left a backlog, which brings the next turn forward.
pub fn reply_compact_pending(reply: &Value) -> bool {
    reply["compact_pending"].as_bool() == Some(true)
}

/// The FOS usage and API rows a stage reply carries back for the tick to write.
pub fn reply_rows(reply: &Value) -> Vec<(telemetry::WritableLogKind, Value)> {
    reply["rows"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|item| {
            let kind = telemetry::WritableLogKind::parse(item["kind"].as_str()?)?;
            item["row"].is_object().then(|| (kind, item["row"].clone()))
        })
        .collect()
}

pub fn handle_worker_stage<R: Runtime>(runtime: &R, request: &http::Request) -> http::Response {
    let execution_started = Instant::now();
    let calls_at_start = runtime.admin_remaining_calls();
    let deny = |status: u16, text: &'static str| http::Response::new(status, text).private();
    if !crate::ingest::worker::authorized(runtime, request) {
        return deny(403, "Forbidden");
    }
    let Ok(body) = serde_json::from_slice::<StageRequest>(&request.body) else {
        return deny(400, "Invalid request");
    };
    if !valid(&body) {
        return deny(400, "Invalid request");
    }
    let reply = |mut value: Value| {
        value["rows"] = telemetry::api::runtime_rows(runtime)
            .into_iter()
            .map(|(kind, row)| json!({"kind": telemetry::LogKind::from(kind).as_str(), "row": row}))
            .collect::<Vec<_>>()
            .into();
        value["calls"] = calls_at_start
            .saturating_sub(runtime.admin_remaining_calls())
            .into();
        value["cache_ops"] = runtime.cache_ops().into();
        value["duration_ms"] = (execution_started.elapsed().as_millis() as u64).into();
        value["heap_mib"] = runtime.heap_mib().into();
        value["worker_requests"] = runtime.worker_requests().into();
        let metrics = telemetry::execution::ExecutionMetrics::snapshot(runtime, execution_started);
        value["vcpu_ms"] = metrics.vcpu_ms.into();
        value["execution_metrics"] = json!(metrics);
        http::Response::new(200, value.to_string())
            .header("content-type", "application/json")
            .private()
    };
    let service = match load_installed_service(runtime) {
        Ok((_, Some(service), _)) if service.service_id == body.service_id => service,
        Ok(_) => return reply(json!({"status": "not_installed", "next": body.start})),
        Err(error) => return reply(json!({"status": "error", "error": error})),
    };
    let Some((access_key, secret_key)) = fos_credentials(runtime, &service) else {
        return reply(json!({"status": "error", "error": "credentials_unavailable"}));
    };
    // The storage helpers stop at `WALL_STOP` after `started`; backdating it makes them stop at
    // `STAGE_STOP` instead.
    let now = Instant::now();
    let started = now
        .checked_sub(crate::ingest::storage::WALL_STOP.saturating_sub(STAGE_STOP))
        .unwrap_or(now);
    let settings = telemetry::api::operational_settings(runtime);
    let outcome = logs::maintenance_stage(
        runtime,
        &TickContext {
            service: &service,
            access_key: &access_key,
            secret_key: &secret_key,
            run_id: &body.run_id,
            tick_id: &body.tick_id,
            trigger: &body.trigger,
            actor: &body.actor,
            bucket: body.bucket,
            started,
        },
        &settings,
        body.start,
    );
    reply(json!({
        "status": "ok",
        "ran": outcome.ran.map(|index| MAINT_STAGES[index]),
        "next": outcome.next,
        "compact_pending": outcome.compact_pending,
        "worker_executions_metrics": outcome.worker_executions_metrics,
    }))
}
