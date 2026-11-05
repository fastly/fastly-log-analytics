//! Worker side of ingest fan-out (`POST /_internal/ingest` on the peer worker service).
//!
//! One request converts one shard: at most [`SHARD_MAX_OBJECTS`] raw objects of one plan. The
//! worker reads raw objects through the CDN, converts them all in memory, and only then decides
//! whether its window PUTs fit the 32-call budget; if not, or once the batch's decompressed bytes
//! or the sandbox's heap near their limits, it answers `split` before any write. Otherwise it
//! PUTs an intent manifest naming every output key, then one Parquet file per grid window, each
//! sent with `Content-MD5`, verified by `ETag == MD5` and dropped before the next window is
//! encoded. The budget's spare call buys one retry of a GET or PUT that met a 5xx, 429 or
//! `SlowDown`. It writes no KV, emits no telemetry of its own (its FOS usage rows go back in the
//! response) and never dispatches further, so it uses no Core Cache operations.

use super::compaction::PART_BYTES;
use super::coordinator::WORKER_LEASE_MARGIN_S;
use super::{
    convert, load,
    storage::{Retry, Store, WALL_STOP},
    summary, Full, Loaded, Outcome, BATCH_FULL, MAX_BATCH_DECOMPRESSED_BYTES, SEGMENT_NEEDED,
};
use crate::{admin::Service, auth::settings::UsageLogging, http, shell::Runtime};
use arrow_array::RecordBatch;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::BTreeMap, time::Duration};

pub const PATH: &str = "/_internal/ingest";
/// Segment shards use their own path, so a worker release without segments answers 404 before
/// any write instead of converting the whole object.
pub const RANGE_PATH: &str = "/_internal/ingest-range";
/// Largest decompressed segment a shard may ask for.
pub const MAX_SEGMENT_BYTES: u64 = 24 * 1024 * 1024;
/// A relay execution on the worker service forwards shard requests to the main service, its
/// peer, so one coordinator call reaches several shard workers.
pub const RELAY_PATH: &str = "/_internal/relay";
pub const MAX_RELAY_SHARDS: usize = 16;
/// Largest relay request body, below the 64 KiB request body the services read.
pub const RELAY_BODY_LIMIT: usize = 60_000;
/// A relay answers this long before its caller's deadline; relayed shards stop this much earlier.
pub const RELAY_MARGIN: Duration = Duration::from_secs(2);
pub const SHARD_MAX_OBJECTS: usize = 22;
/// Calls one worker execution may make (the platform per-execution backend limit).
pub const WORKER_CALLS: usize = 32;
/// worker_secret, fos_sk.<sid> (envelope with access_key + secret_key), cdn_key: Secret Store reads charge the budget.
pub const WORKER_SECRET_READS: usize = 3;
/// The worker starts no new GET or PUT this long after receiving the request.
pub const WORKER_STOP: Duration = Duration::from_secs(28);
/// Stop decompressing or skipping a segment prefix once this much worker vCPU is spent.
pub const VCPU_STOP_MS: u64 = super::coordinator::VCPU_STOP_MS;
pub const SAMPLE_BYTES: usize = 512;
/// A shard of more than one object splits once the sandbox uses this much memory after a load:
/// encoding a window still needs room for its sorted copy and file under the 128 MiB limit.
pub const HEAP_SPLIT_MIB: u32 = 80;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardRequest {
    pub service_id: String,
    pub region: String,
    pub bucket: String,
    pub prefix: String,
    pub plan_id: String,
    pub shard: String,
    pub attempt: u32,
    pub stream: String,
    pub table: String,
    pub schema: String,
    /// The plan's custom columns; with the catalog they must hash to `schema`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub custom: Vec<convert::CustomColumn>,
    pub window_s: i64,
    pub keys: Vec<String>,
    /// Milliseconds after receipt when the worker must stop starting GETs and PUTs.
    pub stop_ms: u64,
    /// The plan lease this attempt runs under (unix seconds). No PUT starts within
    /// `WORKER_LEASE_MARGIN_S` of it, so an abandon that waits out the lease never races a write.
    #[serde(default)]
    pub lease_until: u64,
    /// The plan's frozen creation time (unix seconds). Rows with no usable timestamp in an
    /// object whose key carries no minute are routed by it, so every attempt writes the same keys.
    #[serde(default)]
    pub created_at: u64,
    /// The decompressed range of the shard's one object; absent for whole objects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub segment: Option<convert::Segment>,
    /// Segment size the coordinator cuts objects into; 0 when segmenting is off. A lone whole
    /// object or open segment decompressing past twice this answers `segment` with its length.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub segment_bytes: u64,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Output {
    pub key: String,
    pub rows: u64,
    pub bytes: u64,
    #[serde(default)]
    pub md5: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<super::file_metadata::Metadata>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectResult {
    pub key: String,
    pub gz_size: u64,
    /// `converted`, `failed` or `quarantined`.
    pub status: String,
    #[serde(default)]
    pub reason: Option<String>,
    pub rows: u64,
    pub lines_read: u64,
    pub bad_lines: u64,
    #[serde(default)]
    pub bad_sample: Option<String>,
    #[serde(default)]
    pub outputs: Vec<Output>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardResult {
    pub attempt: u32,
    pub manifest: String,
    pub outputs: Vec<Output>,
    pub objects: Vec<ObjectResult>,
    /// One `summary::summary_key` per window in `outputs`.
    #[serde(default)]
    pub summaries: Vec<Output>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub segment: Option<convert::Segment>,
}

/// Distinct windows a shard of `keys` objects may write: what is left of the worker's budget after
/// the secret reads, one GET per object, the manifest PUT and one spare call (for one retry), at
/// two PUTs (window file and summary) per window.
pub fn window_room(keys: usize) -> usize {
    WORKER_CALLS.saturating_sub(WORKER_SECRET_READS + keys + 2) / 2
}

pub fn run_token(plan_id: &str, shard: &str) -> String {
    format!("{plan_id}s{shard}")
}

pub fn manifest_key(scope: &str, plan_id: &str, shard: &str) -> String {
    format!("{scope}processed/_inflight/{plan_id}/{shard}.json")
}

pub(crate) fn authorized<R: Runtime>(runtime: &R, request: &http::Request) -> bool {
    let Some(secret) = runtime.secret("worker_secret").ok().flatten() else {
        return false;
    };
    let given = request.header("x-flp-worker-key").unwrap_or("");
    compute_common::crypto::constant_time_secret_eq(given, &secret)
}

fn lower_word(value: &str, max: usize) -> bool {
    !value.is_empty() && value.len() <= max && value.bytes().all(|b| b.is_ascii_lowercase())
}

fn valid(request: &ShardRequest) -> bool {
    let raw = format!("{}raw/{}/", request.prefix, request.stream);
    !request.service_id.is_empty()
        && request.service_id.len() <= 64
        && request
            .service_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric())
        && crate::admin::REGIONS.contains(&request.region.as_str())
        && crate::admin::valid_bucket(&request.bucket)
        && super::retention::safe_scope(&request.prefix)
        && lower_word(&request.stream, 16)
        && lower_word(&request.table, 16)
        && request.plan_id.len() == 12
        && request.plan_id.bytes().all(|b| b.is_ascii_hexdigit())
        && !request.shard.is_empty()
        && request.shard.len() <= 16
        && request
            .shard
            .bytes()
            .all(|b| b.is_ascii_digit() || b.is_ascii_lowercase())
        && (1..=3600).contains(&request.window_s)
        && (1..=SHARD_MAX_OBJECTS).contains(&request.keys.len())
        && request.segment_bytes <= MAX_SEGMENT_BYTES
        && request.segment.is_none_or(|segment| {
            request.keys.len() == 1
                && request.stream != super::rum::STREAM
                && segment.end.is_none_or(|end| end > segment.start)
        })
        && request.keys.iter().all(|key| {
            key.len() <= 1024
                && key.starts_with(&raw)
                && super::is_raw_key(key)
                && !key.contains("//")
                && !key.chars().any(char::is_control)
                && key.split('/').all(|part| part != ".." && part != ".")
        })
}

/// A minimal service record for worker-side FOS access, with the provisioned credential refs.
pub(crate) fn worker_service(
    service_id: &str,
    region: &str,
    bucket: &str,
    prefix: &str,
) -> Result<Service, String> {
    serde_json::from_value(json!({
        "service_id": service_id, "name": "worker", "status": "active",
        "created_at": 0, "updated_at": 0, "config_version": 0, "logging_enabled": true,
        "logging_endpoint_name": "", "log_period": 60, "sample_rate": 1, "edge_only": false,
        "custom_condition": "", "fos_bucket": bucket, "fos_prefix": prefix,
        "fos_region": region, "fos_endpoint": "", "log_fields": [], "field_limits": {},
        "fastly_token_ref": "", "fos_access_key_ref": format!("fos_ak.{service_id}"),
        "fos_secret_key_ref": format!("fos_sk.{service_id}"), "owner": "",
        "bucket_adopted": true, "bucket_checked": true, "last_activated_version": null,
        "last_reconcile": null, "pending_job_id": null
    }))
    .map_err(|_| "worker_service_invalid".into())
}

fn truncate(sample: &str) -> String {
    compute_common::str::truncate_utf8(sample, SAMPLE_BYTES).to_owned()
}

#[derive(Deserialize)]
struct RelayRequest {
    deadline_ms: u64,
    requests: Vec<(String, Value)>,
}

/// Forwards 2..=`MAX_RELAY_SHARDS` shard requests to the peer service and answers
/// `{status: "ok", replies: [{reply} | {error}]}` in request order once all answered or
/// `RELAY_MARGIN` before `deadline_ms`.
pub fn handle_worker_relay<R: Runtime>(runtime: &R, request: &http::Request) -> http::Response {
    let started = std::time::Instant::now();
    let calls_at_start = runtime.admin_remaining_calls();
    let deny = |status: u16, text: &'static str| http::Response::new(status, text).private();
    if !authorized(runtime, request) {
        return deny(403, "Forbidden");
    }
    let Ok(relay) = serde_json::from_slice::<RelayRequest>(&request.body) else {
        return deny(400, "Invalid request");
    };
    if request.body.len() > RELAY_BODY_LIMIT
        || !(2..=MAX_RELAY_SHARDS).contains(&relay.requests.len())
        || relay.deadline_ms > super::coordinator::DRAIN_AT.as_millis() as u64
        || relay
            .requests
            .iter()
            .any(|(path, body)| (path != PATH && path != RANGE_PATH) || !body.is_object())
    {
        return deny(400, "Invalid request");
    }
    let deadline = Duration::from_millis(relay.deadline_ms).saturating_sub(RELAY_MARGIN);
    let replies: Vec<Value> = runtime
        .worker_dispatch(&relay.requests, deadline)
        .into_iter()
        .map(|reply| match reply {
            Ok(reply) => json!({"reply": reply}),
            Err(error) => json!({"error": error}),
        })
        .collect();
    let metrics = crate::telemetry::execution::ExecutionMetrics::snapshot(runtime, started);
    let body = json!({
        "status": "ok",
        "replies": replies,
        "calls": calls_at_start.saturating_sub(runtime.admin_remaining_calls()),
        "vcpu_ms": metrics.vcpu_ms,
        "duration_ms": started.elapsed().as_millis() as u64,
        "heap_mib": runtime.heap_mib(),
        "worker_requests": runtime.worker_requests(),
        "execution_metrics": metrics,
    });
    http::Response::new(200, body.to_string())
        .header("content-type", "application/json")
        .private()
}

/// Converts one shard. Response `status` is `ok`, `split`, `schema_mismatch` or `error`.
pub fn handle_worker_ingest<R: Runtime>(runtime: &R, request: &http::Request) -> http::Response {
    let started = std::time::Instant::now();
    let calls_at_start = runtime.admin_remaining_calls();
    let deny = |status: u16, text: &'static str| http::Response::new(status, text).private();
    if !authorized(runtime, request) {
        return deny(403, "Forbidden");
    }
    if request.path == PATH
        && serde_json::from_slice::<Value>(&request.body)
            .is_ok_and(|value| value["maintenance"] == "source_index")
    {
        return super::source_index::handle_authorized_worker(runtime, request, started);
    }
    if request.path == PATH
        && serde_json::from_slice::<Value>(&request.body)
            .is_ok_and(|value| value["maintenance"] == "request_compact")
    {
        return super::compaction::handle_authorized_worker(runtime, request, started);
    }
    let Ok(shard) = serde_json::from_slice::<ShardRequest>(&request.body) else {
        return deny(400, "Invalid request");
    };
    if !valid(&shard) || shard.segment.is_some() != (request.path == RANGE_PATH) {
        return deny(400, "Invalid request");
    }
    let summary_vcpu = std::cell::Cell::new(0u64);
    let reply = |mut body: Value| {
        if let Some(usage) = runtime.usage_agg() {
            body["usage"] = usage.drain_rows(&UsageLogging::default()).into();
        }
        body["calls"] = calls_at_start
            .saturating_sub(runtime.admin_remaining_calls())
            .into();
        body["summary_vcpu_ms"] = summary_vcpu.get().into();
        body["duration_ms"] = (started.elapsed().as_millis() as u64).into();
        body["heap_mib"] = runtime.heap_mib().into();
        body["kv_ops"] = runtime.kv_ops().into();
        body["cache_lookups"] = runtime.cache_lookups().into();
        body["cache_replaces"] = runtime.cache_replaces().into();
        body["purges"] = runtime.purges().into();
        let mut metrics = crate::telemetry::execution::ExecutionMetrics::snapshot(runtime, started);
        metrics.summary_vcpu_ms = metrics.vcpu_ms.map(|_| summary_vcpu.get());
        body["vcpu_ms"] = metrics.vcpu_ms.into();
        body["execution_metrics"] = json!(metrics);
        http::Response::new(200, body.to_string())
            .header("content-type", "application/json")
            .private()
    };
    let rum = shard.stream == super::rum::STREAM;
    let schema = convert::RequestSchema::new(shard.custom.clone());
    let expected = if rum {
        super::rum::schema_hash()
    } else {
        schema.hash()
    };
    if shard.schema != expected || (rum && !shard.custom.is_empty()) {
        return reply(json!({"status": "schema_mismatch", "schema": expected}));
    }
    let dataset = if rum {
        super::Dataset::Rum {
            service_id: &shard.service_id,
        }
    } else {
        super::Dataset::Request(&schema)
    };
    let Ok(service) = worker_service(
        &shard.service_id,
        &shard.region,
        &shard.bucket,
        &shard.prefix,
    ) else {
        return deny(400, "Invalid request");
    };
    // `fos_sk.<sid>` holds the provisioning envelope; this unwraps it like every other reader.
    let (Some((access, secret)), Ok(Some(cdn_key))) = (
        crate::admin::fos_credentials(runtime, &service),
        runtime.secret("cdn_key"),
    ) else {
        return deny(503, "Credentials unavailable");
    };
    if let Some(usage) = runtime.usage_agg() {
        usage.set_caller("worker:ingest");
    }
    let store = Store {
        runtime,
        service: &service,
        access_key: access,
        secret_key: secret,
        started,
    };
    let stop = WORKER_STOP
        .min(Duration::from_millis(shard.stop_ms))
        .min(WALL_STOP);
    match convert_shard(&store, &cdn_key, &shard, &dataset, stop, &summary_vcpu) {
        Ok(Done::Written(result)) => reply(json!({"status": "ok", "result": result})),
        Ok(Done::Split) => reply(json!({"status": "split"})),
        Ok(Done::Segment(total)) => reply(json!({"status": "segment", "total": total})),
        Err(error) => reply(json!({"status": "error", "error": error})),
    }
}

enum Done {
    Written(ShardResult),
    Split,
    /// The lone object needs segments; its measured decompressed length.
    Segment(u64),
}

fn convert_shard<R: Runtime>(
    store: &Store<'_, R>,
    cdn_key: &str,
    shard: &ShardRequest,
    dataset: &super::Dataset<'_>,
    stop: Duration,
    summary_vcpu: &std::cell::Cell<u64>,
) -> Result<Done, String> {
    let runtime = store.runtime;
    let raw_prefix = format!("{}raw/{}/", shard.prefix, shard.stream);
    let processed = format!("{}processed/{}/", shard.prefix, shard.table);
    // Request windows write a file and its summary; RUM windows write up to one file per table.
    let rum = matches!(dataset, super::Dataset::Rum { .. });
    let tables = if rum {
        vec![
            format!("{processed}{}/", super::rum::VITALS),
            format!("{processed}{}/", super::rum::ERRORS),
        ]
    } else {
        vec![processed]
    };
    let summaries = summary::summary_prefix(&shard.prefix, &shard.table);
    let mut loaded: Vec<Loaded> = Vec::new();
    let mut decompressed = 0u64;
    let planned_at = match shard.created_at {
        0 => runtime.now(),
        at => at,
    };
    let retry = Retry::new(1, stop);
    let vcpu_start = runtime.vcpu_ms();
    let stop_decompress = || {
        runtime.admin_elapsed(store.started) >= stop
            || runtime.vcpu_ms().saturating_sub(vcpu_start) >= VCPU_STOP_MS
    };
    let segmenting = shard.segment_bytes > 0 && !rum;
    let segment = shard.segment.unwrap_or(convert::Segment::WHOLE);
    let full = match (segmenting, shard.keys.len()) {
        (true, 1) if segment.end.is_none() => Full::Measure,
        (true, 1) | (false, _) => Full::Hold,
        (true, _) => Full::Split,
    };
    for key in &shard.keys {
        if runtime.admin_elapsed(store.started) >= stop {
            return Err("worker_wall".into());
        }
        // Nothing held yet: a lone object may use the whole per-object cap.
        let room = match decompressed {
            0 if segmenting && full != Full::Hold => {
                convert::MAX_DECOMPRESSED_BYTES.min(shard.segment_bytes.saturating_mul(2))
            }
            0 => convert::MAX_DECOMPRESSED_BYTES,
            held => MAX_BATCH_DECOMPRESSED_BYTES.saturating_sub(held),
        };
        let item = load(
            store,
            cdn_key,
            &raw_prefix,
            key,
            shard.window_s,
            planned_at,
            room,
            Some(&retry),
            dataset,
            segment,
            full,
            &stop_decompress,
        )?;
        if matches!(item.outcome, Outcome::Failed(BATCH_FULL)) {
            return Ok(Done::Split);
        }
        if matches!(item.outcome, Outcome::Failed(SEGMENT_NEEDED)) {
            return Ok(Done::Segment(item.decompressed));
        }
        decompressed += item.decompressed;
        loaded.push(item);
        if shard.keys.len() > 1 && runtime.heap_mib().is_some_and(|mib| mib >= HEAP_SPLIT_MIB) {
            return Ok(Done::Split);
        }
    }
    // Each window owns its batches, so they are freed as soon as its files are written.
    // Keyed by (table, window start).
    let mut by_window: BTreeMap<(usize, i64), Vec<RecordBatch>> = BTreeMap::new();
    let mut object_windows: Vec<Vec<((usize, i64), u64)>> = Vec::with_capacity(loaded.len());
    for item in &mut loaded {
        let mut windows = Vec::new();
        if let Outcome::Rows(rows) = &mut item.outcome {
            let tabled = std::mem::take(&mut rows.windows)
                .into_iter()
                .map(|(start, batch)| ((0, start), batch))
                .chain(
                    std::mem::take(&mut rows.errors)
                        .into_iter()
                        .map(|(start, batch)| ((1, start), batch)),
                );
            for (at, batch) in tabled {
                windows.push((at, batch.num_rows() as u64));
                by_window.entry(at).or_default().push(batch);
            }
        }
        object_windows.push(windows);
    }
    // `window_room` counts two PUTs per window; a RUM file is one.
    let puts_per_file = if rum { 1 } else { 2 };
    if by_window.len() * puts_per_file > 2 * window_room(shard.keys.len() + retry.used() as usize) {
        return Ok(Done::Split);
    }
    let token = run_token(&shard.plan_id, &shard.shard);
    let mut keys = BTreeMap::new();
    for (&(table, start), batches) in &by_window {
        let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        let end = start + shard.window_s;
        let key = super::processed_key(&tables[table], start, end, &token, rows);
        let summary = (!rum).then(|| summary::summary_key(&summaries, start, end, &token, rows));
        keys.insert((table, start), (key, summary, rows));
    }
    let manifest = manifest_key(&shard.prefix, &shard.plan_id, &shard.shard);
    let intent = json!({
        "plan_id": shard.plan_id, "shard": shard.shard, "attempt": shard.attempt,
        "keys": keys
            .values()
            .flat_map(|(key, summary, _)| std::iter::once(key).chain(summary))
            .collect::<Vec<_>>(),
    });
    let may_put = || {
        if runtime.admin_elapsed(store.started) >= stop {
            return Err("worker_wall".to_string());
        }
        if shard.lease_until > 0 && runtime.now() + WORKER_LEASE_MARGIN_S >= shard.lease_until {
            return Err("worker_lease_expired".to_string());
        }
        Ok(())
    };
    may_put()?;
    store.put_verified_retrying(&manifest, intent.to_string().into_bytes(), Some(&retry))?;
    let mut outputs = BTreeMap::new();
    let mut summary_outputs = Vec::new();
    while let Some((start, batches)) = by_window.pop_first() {
        let batches: Vec<&RecordBatch> = batches.iter().collect();
        may_put()?;
        let (key, summary_key, rows) = &keys[&start];
        let bytes = convert::write_parquet(&batches)?;
        let metadata = super::file_metadata::from_parquet(&bytes)?;
        let size = bytes.len() as u64;
        let parts = bytes.len().div_ceil(PART_BYTES);
        let after = usize::from(summary_key.is_some()) + puts_per_file * by_window.len() + 1;
        let md5 = if parts > 1 && runtime.admin_remaining_calls() as usize >= parts + 2 + after {
            put_parts(store, key, bytes, &may_put)?
        } else {
            store.put_verified_retrying(key, bytes, Some(&retry))?
        };
        outputs.insert(
            start,
            Output {
                key: key.clone(),
                rows: *rows,
                bytes: size,
                md5,
                metadata,
            },
        );
        let Some(summary_key) = summary_key else {
            continue;
        };
        may_put()?;
        let vcpu = runtime.vcpu_ms();
        let start = start.1;
        let bytes = summary::write_summary(&batches, start);
        summary_vcpu.set(summary_vcpu.get() + runtime.vcpu_ms().saturating_sub(vcpu));
        let bytes = bytes?;
        let size = bytes.len() as u64;
        let md5 = store.put_verified_retrying(summary_key, bytes, Some(&retry))?;
        summary_outputs.push(Output {
            key: summary_key.clone(),
            rows: *rows,
            bytes: size,
            md5,
            metadata: None,
        });
    }
    let objects = loaded
        .iter()
        .zip(object_windows)
        .map(|(item, windows)| match &item.outcome {
            Outcome::Rows(rows) => ObjectResult {
                key: item.key.clone(),
                gz_size: item.gz_size,
                status: "converted".into(),
                reason: None,
                rows: rows.rows,
                lines_read: rows.lines_read,
                bad_lines: rows.bad_lines,
                bad_sample: rows.bad_sample.as_deref().map(truncate),
                outputs: windows
                    .iter()
                    .map(|(start, rows)| Output {
                        key: outputs[start].key.clone(),
                        rows: *rows,
                        bytes: row_share(outputs[start].bytes, *rows, outputs[start].rows),
                        md5: String::new(),
                        metadata: None,
                    })
                    .collect(),
            },
            Outcome::Failed(reason) => ObjectResult {
                key: item.key.clone(),
                gz_size: item.gz_size,
                status: if super::quarantined(reason) {
                    "quarantined"
                } else {
                    "failed"
                }
                .into(),
                reason: Some((*reason).into()),
                ..Default::default()
            },
        })
        .collect();
    Ok(Done::Written(ShardResult {
        attempt: shard.attempt,
        manifest,
        outputs: outputs.into_values().collect(),
        objects,
        summaries: summary_outputs,
        segment: shard.segment,
    }))
}

/// Uploads `bytes` as `PART_BYTES` multipart parts, each checked by its ETag, and returns the
/// whole body's MD5. The upload is aborted on any failure.
fn put_parts<R: Runtime>(
    store: &Store<'_, R>,
    key: &str,
    bytes: Vec<u8>,
    may_put: &dyn Fn() -> Result<(), String>,
) -> Result<String, String> {
    use md5::{Digest, Md5};
    let md5 = hex::encode(Md5::digest(&bytes));
    let id = store.multipart_create(key)?;
    let uploaded = (|| {
        let mut etags = Vec::new();
        for (index, chunk) in bytes.chunks(PART_BYTES).enumerate() {
            may_put()?;
            etags.push(store.multipart_part(key, &id, index as u32 + 1, chunk.to_vec())?);
        }
        store.multipart_complete(key, &id, &etags)
    })();
    match uploaded {
        Ok(()) => Ok(md5),
        Err(error) => {
            store.multipart_abort(key, &id).map_err(|_| error.clone())?;
            Err(error)
        }
    }
}

/// The part of a `bytes`-sized window file that `rows` of its `total` rows account for.
fn row_share(bytes: u64, rows: u64, total: u64) -> u64 {
    (u128::from(bytes) * u128::from(rows) / u128::from(total.max(1))) as u64
}
