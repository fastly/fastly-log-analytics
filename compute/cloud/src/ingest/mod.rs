//! Raw-log processing: gzip NDJSON in `raw/request/` becomes immutable Parquet window files in
//! `processed/request/`.
//!
//! Exactly-once design: a tick first persists a *plan* (`ingest:plan`) naming the raw objects it
//! will consume, split into shards that peer worker executions convert (see [`coordinator`] and
//! [`worker`]). Output keys are a pure function of (window, plan id, shard, row count), so a
//! retry after a crash re-PUTs identical keys instead of creating second copies. The plan is
//! complete once `ingest:watermark` names the same plan id; the watermark CAS is the single
//! commit point. Before the commit, one `ingest:deleteq:<plan id>` record names the raw objects
//! that were converted (never failed or quarantined ones); raw deletion consumes only those
//! records, because the watermark alone is not proof of conversion. The same record carries the
//! failed and quarantined objects as `held`; raw deletion moves them to one
//! `ingest:quarantine:<plan id>` record once the plan is committed, and only an admin purge
//! ([`api::quarantine_purge`]) deletes those raw objects. Each raw object also
//! produces one `ingest_files` telemetry row. Available immutable request objects are consumed
//! without waiting for their minute to close. Exact raw-key receipts and the query catalog are
//! published in the same watermark CAS; a bounded, cursor-backed recent-prefix rescan finds
//! arrivals below the lexical watermark without relying on upload timestamps. Receipts persist
//! when raw objects are kept, independently of compaction and the bounded straggler sweep.
//! [`settle_s`] remains the conservative closed-partition cache and RUM allowance.
//!
//! A plan without a `version` field was written by the single-execution (v1) converter; it is
//! finished once in-tick by [`finish`] before any v2 planning.

pub mod api;
pub mod bundle;
pub mod catalog;
pub mod compaction;
pub mod convert;
pub mod coordinator;
pub mod file_metadata;
pub mod listing;
pub mod retention;
pub mod rum;
mod source_index;
pub mod storage;
pub mod summary;
pub mod worker;

use crate::{
    admin::Service,
    shell::Runtime,
    telemetry::{write_log, LogBuffer, WritableLogKind},
};
use arrow_array::RecordBatch;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Instant,
};
use storage::{Store, CDN_READ_LIMIT};

pub const PLAN_KEY: &str = "ingest:plan";
pub const WATERMARK_KEY: &str = "ingest:watermark";
pub const BACKFILL_KEY: &str = "ingest:backfill";
pub const DELETEQ_PREFIX: &str = "ingest:deleteq:";

/// The plan record of a raw stream: `ingest:plan` for requests, `ingest:<stream>:plan` otherwise.
pub fn plan_key(stream: &str) -> String {
    if stream == "request" {
        PLAN_KEY.into()
    } else {
        format!("ingest:{stream}:plan")
    }
}

/// The watermark record of a raw stream, keyed like [`plan_key`].
pub fn watermark_key(stream: &str) -> String {
    if stream == "request" {
        WATERMARK_KEY.into()
    } else {
        format!("ingest:{stream}:watermark")
    }
}

/// The watermark field a Delete Data reset sets to the first raw key it keeps. It is permanent:
/// no raw key below it is ever planned again, whichever path lists it.
pub const FLOOR_FIELD: &str = "floor_key";

pub fn below_floor(key: &str, watermark: Option<&Value>) -> bool {
    watermark
        .and_then(|value| value[FLOOR_FIELD].as_str())
        .is_some_and(|floor| key < floor)
}
pub const QUARANTINE_PREFIX: &str = "ingest:quarantine:";

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackfillJob {
    pub status: String,
    pub actor: String,
    pub started_at: u64,
    pub updated_at: u64,
    pub from_ts: Option<u64>,
    pub to_ts: Option<u64>,
    pub after_key: String,
    pub to_key: String,
    pub scanned: u64,
    pub planned: u64,
}
pub const DEFAULT_WINDOW_S: i64 = 60;
/// Conservative allowance for closed-partition caches, row windows and RUM ingestion.
pub const SETTLE_S: u64 = 120;
/// How far below the watermark (by minute directory) the straggler sweep looks.
pub const SWEEP_S: u64 = 2 * 3600;
/// Seconds between straggler sweeps (the maintenance cadence).
pub const SWEEP_INTERVAL_S: u64 = 120;
/// LIST calls one sweep may make.
pub const SWEEP_LISTS: usize = 2;
/// `passes` and `swept` entries older than this can no longer cover a key the sweep looks at.
const PASS_KEEP_S: u64 = SWEEP_S + 2 * 3600;
/// Older passes are dropped first; a key whose pass is gone matches a later one, which only
/// makes the sweep skip it.
const MAX_PASSES: usize = 256;
/// Stragglers remembered at once; the sweep plans no more while this many are kept.
pub const MAX_SWEPT: usize = 1000;
/// Cumulative decompressed bytes one execution may hold in memory (keeps heap well under 128 MB).
/// A shard's first object may still use the full per-object [`convert::MAX_DECOMPRESSED_BYTES`].
pub const MAX_BATCH_DECOMPRESSED_BYTES: u64 = 24 * 1024 * 1024;
/// One object spanning more windows than this is quarantined: its PUTs would not fit the
/// 32-call budget, and a plan that can never finish would block every later object.
pub const MAX_WINDOWS_PER_OBJECT: usize = 12;
pub const LIST_PAGE: usize = 200;
/// Backend calls left for the tick's own bookkeeping (cron state, manual manifest).
pub const TICK_RESERVE: u32 = 4;

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize)]
pub struct Report {
    pub listed: u64,
    pub converted: u64,
    pub failed: u64,
    pub rows: u64,
    pub bytes: u64,
    pub lines_read: u64,
    pub bad_lines: u64,
    pub files: Vec<String>,
    pub resumed: bool,
    pub busy: bool,
    pub more_pending: bool,
    /// Raw objects known to be unconverted remain (a plan in flight, listed candidates not yet
    /// dispatched, or further listing pages). Unlike `more_pending`, scans and sweeps never set it.
    pub backlog: bool,
    /// This tick's raw listing had another page (feeds `Plan::backlog`).
    #[serde(skip)]
    pub backlog_listing: bool,
    pub backend_calls: u32,
    pub kv_ops: u32,
    pub cache_lookups: u32,
    pub cache_replaces: u32,
    pub purges: u32,
    /// vCPU the answering workers reported, and the part of it spent building summaries.
    pub worker_vcpu_ms: u64,
    pub summary_vcpu_ms: u64,
    pub worker_executions_metrics: Vec<crate::telemetry::execution::WorkerExecutionMetrics>,
    /// Shards dispatched to workers this tick and how many of them answered.
    pub shards_dispatched: u32,
    pub shards_done: u32,
    /// Peer executions relays started for this tick; a relay whose reply was lost counts its
    /// whole group.
    pub relayed_worker_requests: u32,
    /// Raw objects the straggler sweep found below the watermark and planned this tick.
    pub stragglers: u64,
    /// Responses to `coordinator::Options::extra` requests, in request order.
    pub extra_results: Vec<Result<Value, String>>,
    /// Deferred delete-queue record `(key, value)` when `coordinator::Options::defer_deleteq` is set.
    #[serde(skip)]
    pub pending_deleteq: Option<(String, Value)>,
}

#[derive(Serialize, Deserialize, Clone)]
struct Plan {
    run_id: String,
    window_s: i64,
    schema: String,
    raw_keys: Vec<String>,
    created_at: u64,
}

pub fn scope(service: &Service) -> String {
    if service.fos_prefix.is_empty() {
        String::new()
    } else {
        format!("{}/", service.fos_prefix.trim_end_matches('/'))
    }
}

pub fn raw_prefix(service: &Service) -> String {
    format!("{}raw/request/", scope(service))
}

pub fn processed_prefix(service: &Service) -> String {
    format!("{}processed/request/", scope(service))
}

/// Intent manifests of in-flight plans; a sibling of the table prefixes, so readers skip it.
pub fn inflight_prefix(service: &Service) -> String {
    format!("{}processed/_inflight/", scope(service))
}

/// Whether a raw object key has a supported zstd compressed log extension (`.zst` or `.zstd`).
pub fn is_raw_key(key: &str) -> bool {
    key.ends_with(".zst") || key.ends_with(".zstd")
}

pub fn window_seconds(runtime: &impl Runtime) -> i64 {
    runtime
        .config("window_s")
        .ok()
        .flatten()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .filter(|v| (1..=3600).contains(v))
        .unwrap_or(DEFAULT_WINDOW_S)
}

/// Scales the ingest window to the service's `log_period` when `window_s` is not explicitly
/// overridden in config and the period is an exact minute divisor of an hour (`60..=600` seconds:
/// `60`, `120`, `180`, `240`, `300`, `360`, `600`). That keeps each period's shard to one window
/// and one summary file instead of splitting every raw upload into multiple 60-second windows.
pub fn window_seconds_for_period(runtime: &impl Runtime, log_period: u32) -> i64 {
    if let Some(configured) = runtime
        .config("window_s")
        .ok()
        .flatten()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .filter(|v| (1..=3600).contains(v))
    {
        return configured;
    }
    let period = i64::from(log_period);
    if (DEFAULT_WINDOW_S..=600).contains(&period) && period % 60 == 0 && 3600 % period == 0 {
        period
    } else {
        DEFAULT_WINDOW_S
    }
}

fn utc(seconds: i64) -> time::OffsetDateTime {
    time::OffsetDateTime::from_unix_timestamp(seconds).unwrap_or(time::OffsetDateTime::UNIX_EPOCH)
}

pub fn day_start(seconds: i64) -> i64 {
    seconds - seconds.rem_euclid(86_400)
}

pub fn hour_floor(seconds: i64) -> i64 {
    seconds - seconds.rem_euclid(3600)
}

pub fn day_dir(seconds: i64) -> String {
    let t = utc(seconds);
    format!(
        "year={:04}/month={:02}/day={:02}/",
        t.year(),
        u8::from(t.month()),
        t.day()
    )
}

pub fn hour_dir(seconds: i64) -> String {
    let t = utc(seconds);
    format!(
        "year={:04}/month={:02}/day={:02}/hour={:02}/",
        t.year(),
        u8::from(t.month()),
        t.day(),
        t.hour()
    )
}

pub fn minute_dir(seconds: i64) -> String {
    let t = utc(seconds);
    format!(
        "year={:04}/month={:02}/day={:02}/hour={:02}/minute={:02}/",
        t.year(),
        u8::from(t.month()),
        t.day(),
        t.hour(),
        t.minute()
    )
}

pub fn file_name(key: &str) -> &str {
    key.rsplit('/').next().unwrap_or(key)
}

/// Parses `-n<rows>` from the `.parquet` filename of `key`.
pub fn parse_name_rows(key: &str) -> Option<u64> {
    file_name(key)
        .strip_suffix(".parquet")?
        .rsplit_once("-n")
        .and_then(|(_, rest)| rest.split('-').next()?.parse().ok())
}

pub fn processed_key(prefix: &str, start: i64, end: i64, run_id: &str, rows: u64) -> String {
    format!(
        "{prefix}{}window={start}-{end}-{run_id}-n{rows}.parquet",
        hour_dir(start)
    )
}

#[derive(Debug, PartialEq, Eq)]
pub struct ProcessedName {
    pub start: i64,
    pub end: i64,
    pub rows: Option<u64>,
}

/// Parses `window=<start>-<end>-<run>[-n<rows>].parquet` from the last key segment.
pub fn parse_processed(key: &str) -> Option<ProcessedName> {
    let name = file_name(key).strip_prefix("window=")?;
    let name = name.strip_suffix(".parquet")?;
    let mut parts = name.split('-');
    let start = parts.next()?.parse().ok()?;
    let end = parts.next()?.parse().ok()?;
    parts.next()?;
    let rows = parts
        .next()
        .and_then(|p| p.strip_prefix('n'))
        .and_then(|p| p.parse().ok());
    Some(ProcessedName { start, end, rows })
}

/// Start, end and rows of each request file a reader counts among the listed `keys`: valid
/// `compact=` files and the windows none of them covers.
pub fn processed_spans(keys: &[String]) -> Vec<(i64, i64, u64)> {
    crate::telemetry::compact::visible(keys, &[])
        .iter()
        .filter_map(|key| match parse_processed(key) {
            Some(name) => Some((name.start, name.end, name.rows.unwrap_or(0))),
            None => crate::telemetry::compact::parse_compact_name(key)
                .map(|c| (c.a as i64, c.b as i64, c.rows)),
        })
        .collect()
}

/// Seconds after a minute directory starts before its objects are consumed: the log period
/// (at least the minute itself) plus `SETTLE_S` for uploads in flight.
pub fn settle_s(log_period: u32) -> u64 {
    u64::from(log_period).max(60) + SETTLE_S
}

/// Most windows one raw object can touch: its log period (at least the minute in its key)
/// plus `SETTLE_S` of slack for late lines, and one more when windows don't align to minutes.
/// Above `MAX_WINDOWS_PER_OBJECT` every object would be quarantined, so config rejects it.
pub fn object_windows(window_s: i64, log_period: u32) -> u64 {
    let window = window_s.max(1) as u64;
    settle_s(log_period).div_ceil(window) + u64::from(60 % window != 0)
}

/// The watermark value once `run_id` consumed raw keys up to `last`, which a LIST finished by
/// `listed_at` returned. The key never moves backwards; a forward move appends `[last,
/// listed_at]` to `passes`. `swept` stragglers are added, and the sweep cursor is kept.
pub(crate) fn watermark_value(
    prev: Option<&Value>,
    last: &str,
    run_id: &str,
    listed_at: u64,
    swept: &[String],
    now: u64,
) -> Value {
    let prev_key = prev.and_then(|v| v["key"].as_str()).unwrap_or_default();
    let entries = |field: &str| -> Vec<(String, u64)> {
        prev.and_then(|v| serde_json::from_value(v[field].clone()).ok())
            .unwrap_or_default()
    };
    let (mut passes, mut done) = (entries("passes"), entries("swept"));
    if last > prev_key {
        passes.push((last.to_owned(), listed_at));
    }
    done.extend(swept.iter().map(|key| (key.clone(), now)));
    let floor = now.saturating_sub(PASS_KEEP_S);
    passes.retain(|pass| pass.1 >= floor);
    done.retain(|entry| entry.1 >= floor);
    if passes.len() > MAX_PASSES {
        passes.drain(..passes.len() - MAX_PASSES);
    }
    let mut value = json!({
        "key": last.max(prev_key), "run_id": run_id, "at": now, "passes": passes, "swept": done,
    });
    if let Some(sweep) = prev.map(|v| &v["sweep"]).filter(|v| !v.is_null()) {
        value["sweep"] = sweep.clone();
    }
    if let Some(generation) = prev.and_then(|v| v["manifest_gen"].as_u64()) {
        value["manifest_gen"] = generation.into();
    }
    if let Some(catalog) = prev.map(|v| &v["catalog"]).filter(|v| !v.is_null()) {
        value["catalog"] = catalog.clone();
    }
    for field in ["raw_sources", "raw_scan", "raw_scan_due", FLOOR_FIELD] {
        if let Some(saved) = prev.map(|v| &v[field]).filter(|v| !v.is_null()) {
            value[field] = saved.clone();
        }
    }
    value
}

/// Whether the hour starting at `hour` is closed to readers' manifest caches at `now`: a commit
/// adding a window to such an hour must bump the watermark's `manifest_gen`.
pub fn manifest_hour_closed(hour: i64, log_period: u32, now: u64) -> bool {
    let settle = i64::try_from(settle_s(log_period)).unwrap_or(i64::MAX);
    hour.saturating_add(3600).saturating_add(settle) <= i64::try_from(now).unwrap_or(i64::MAX)
}

/// `value` with `manifest_gen` advanced past `prev`'s.
pub(crate) fn bump_manifest_gen(prev: Option<&Value>, mut value: Value) -> Value {
    let generation = prev.and_then(|v| v["manifest_gen"].as_u64()).unwrap_or(0);
    value["manifest_gen"] = (generation + 1).into();
    value
}

/// Epoch seconds of the minute directory in a raw key, or `None` if the key doesn't have one.
pub fn raw_key_minute(key: &str, prefix: &str) -> Option<i64> {
    let mut parts = key.strip_prefix(prefix)?.split('/');
    let mut field = |name: &str| -> Option<i64> {
        parts
            .next()?
            .strip_prefix(name)?
            .strip_prefix('=')?
            .parse()
            .ok()
    };
    let (year, month, day, hour, minute) = (
        field("year")?,
        field("month")?,
        field("day")?,
        field("hour")?,
        field("minute")?,
    );
    let date = time::Date::from_calendar_date(
        i32::try_from(year).ok()?,
        time::Month::try_from(u8::try_from(month).ok()?).ok()?,
        u8::try_from(day).ok()?,
    )
    .ok()?;
    let at = date
        .with_hms(u8::try_from(hour).ok()?, u8::try_from(minute).ok()?, 0)
        .ok()?
        .assume_utc();
    Some(at.unix_timestamp())
}

enum Outcome {
    Rows(convert::ObjectRows),
    Failed(&'static str),
}

const BATCH_FULL: &str = "batch_decompressed_cap";
/// A lone object or open segment past its decompressed room: the worker reports the object's
/// measured length so the coordinator can cut it into segments.
const SEGMENT_NEEDED: &str = "segment_needed";
/// Largest raw object (compressed) a segment worker reads through the CDN.
pub const SEGMENT_READ_LIMIT: usize = 24 * 1024 * 1024;

/// What a load does when the object's decompressed bytes exceed its room.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Full {
    /// Below the per-object cap the shard splits; at it the object is quarantined.
    Hold,
    Split,
    /// Measure the whole object for segmenting (up to `convert::MAX_OBJECT_DECOMPRESSED_BYTES`).
    Measure,
}

/// What a raw object converts as: request rows under the plan's schema, or RUM beacons of one
/// service.
pub(crate) enum Dataset<'a> {
    Request(&'a convert::RequestSchema),
    Rum { service_id: &'a str },
}

struct Loaded {
    key: String,
    gz_size: u64,
    decompressed: u64,
    outcome: Outcome,
}

#[allow(clippy::too_many_arguments)]
fn load<R: Runtime>(
    store: &Store<'_, R>,
    cdn_key: &str,
    raw_prefix: &str,
    key: &str,
    window_s: i64,
    planned_at: u64,
    decompressed_room: u64,
    retry: Option<&storage::Retry>,
    dataset: &Dataset<'_>,
    segment: convert::Segment,
    full: Full,
    stop: &dyn Fn() -> bool,
) -> Result<Loaded, String> {
    let object_time = raw_key_minute(key, raw_prefix).unwrap_or(planned_at as i64);
    let limit = if segment == convert::Segment::WHOLE {
        CDN_READ_LIMIT
    } else {
        SEGMENT_READ_LIMIT
    };
    let Some(gz) = (match store.get_retrying(cdn_key, key, limit, retry) {
        Ok(found) => found,
        Err(code) if code == "cdn_body_too_large" => {
            return Ok(Loaded {
                key: key.into(),
                gz_size: 0,
                decompressed: 0,
                outcome: Outcome::Failed("raw_object_too_large"),
            })
        }
        Err(code) => return Err(code),
    }) else {
        return Ok(Loaded {
            key: key.into(),
            gz_size: 0,
            decompressed: 0,
            outcome: Outcome::Failed("raw_object_missing"),
        });
    };
    let converted = match dataset {
        Dataset::Request(schema) => convert::convert_object_range(
            &gz,
            window_s,
            object_time,
            segment,
            decompressed_room,
            schema,
            stop,
        ),
        Dataset::Rum { service_id } => {
            rum::convert_object(&gz, window_s, object_time, decompressed_room, service_id)
        }
    };
    let (outcome, decompressed) = match converted {
        Ok(rows)
            if rows
                .windows
                .keys()
                .chain(rows.errors.keys())
                .collect::<BTreeSet<_>>()
                .len()
                > MAX_WINDOWS_PER_OBJECT =>
        {
            (Outcome::Failed("too_many_windows"), 0)
        }
        Ok(rows) => {
            let decompressed = rows.decompressed;
            (Outcome::Rows(rows), decompressed)
        }
        Err(convert::ConvertError::Stopped) => {
            return Err(convert::ConvertError::Stopped.code().into())
        }
        Err(convert::ConvertError::TooLarge) if full == Full::Measure => {
            match convert::decompressed_len(&gz, convert::MAX_OBJECT_DECOMPRESSED_BYTES, stop) {
                Ok(total) => (Outcome::Failed(SEGMENT_NEEDED), total),
                Err(convert::ConvertError::Stopped) => {
                    return Err(convert::ConvertError::Stopped.code().into())
                }
                Err(error) => (Outcome::Failed(error.code()), 0),
            }
        }
        Err(convert::ConvertError::TooLarge)
            if full == Full::Split || decompressed_room < convert::MAX_DECOMPRESSED_BYTES =>
        {
            (Outcome::Failed(BATCH_FULL), 0)
        }
        Err(error) => (Outcome::Failed(error.code()), 0),
    };
    Ok(Loaded {
        key: key.into(),
        gz_size: gz.len() as u64,
        decompressed,
        outcome,
    })
}

pub fn run<R: Runtime>(
    runtime: &R,
    service: &Service,
    access_key: &str,
    secret_key: &str,
    started: Instant,
    buffer: &LogBuffer,
) -> Result<Report, String> {
    run_capped(runtime, service, access_key, secret_key, started, buffer, 0)
}

/// Like [`run`], leaving `reserve` backend calls untouched for later tick stages.
pub fn run_capped<R: Runtime>(
    runtime: &R,
    service: &Service,
    access_key: &str,
    secret_key: &str,
    started: Instant,
    buffer: &LogBuffer,
    reserve: u32,
) -> Result<Report, String> {
    let options = coordinator::Options {
        reserve,
        ..coordinator::Options::request(runtime)
    };
    coordinator::coordinate(
        runtime, service, access_key, secret_key, started, buffer, &options,
    )
}

/// One failed or quarantined object as a delete-queue record carries it in `held`.
pub(crate) fn held_object(
    key: &str,
    size: u64,
    status: &str,
    reason: &str,
    attempts: u32,
) -> Value {
    json!({"key": key, "size": size, "status": status, "reason": reason, "attempts": attempts})
}

pub(crate) fn quarantined(reason: &str) -> bool {
    matches!(
        reason,
        "corrupt_gzip" | "decompressed_too_large" | "too_many_windows"
    )
}

/// Finishes an uncommitted plan written by the single-execution (v1) converter, in this
/// execution: every raw object is read again and every window PUT here.
pub(crate) fn finish_legacy<R: Runtime>(
    store: &Store<'_, R>,
    plan: Value,
    watermark: Option<&crate::admin::Record>,
    buffer: &LogBuffer,
    report: &mut Report,
) -> Result<(), String> {
    let plan: Plan = serde_json::from_value(plan).map_err(|_| "ingest_plan_invalid")?;
    let cdn_key = store
        .runtime
        .secret("cdn_key")?
        .filter(|k| !k.is_empty())
        .ok_or("cdn_key_missing")?;
    report.resumed = true;
    let raw_prefix = raw_prefix(store.service);
    let schema = convert::RequestSchema::for_fields(&store.service.log_fields);
    let mut loaded = Vec::new();
    for key in &plan.raw_keys {
        loaded.push(load(
            store,
            &cdn_key,
            &raw_prefix,
            key,
            plan.window_s,
            plan.created_at,
            convert::MAX_DECOMPRESSED_BYTES,
            None,
            &Dataset::Request(&schema),
            convert::Segment::WHOLE,
            Full::Hold,
            &|| false,
        )?);
    }
    report.listed = loaded.len() as u64;
    finish(store, &plan, loaded, watermark, buffer, report)
}

/// The `run_id` of a v1 plan, if `value` is one.
pub(crate) fn legacy_run_id(value: &Value) -> Option<&str> {
    value["version"]
        .is_null()
        .then(|| value["run_id"].as_str())
        .flatten()
}

/// Extracts `plan_id` (v2/v3) or `run_id` (v1) from a serialized plan record.
pub fn plan_id_of(value: &Value) -> Option<&str> {
    value["plan_id"]
        .as_str()
        .or_else(|| value["run_id"].as_str())
}

/// Whether `stream` has an active plan record in KV whose `plan_id` / `run_id` does not match its
/// committed watermark `run_id`.
pub fn has_uncommitted_plan<R: Runtime>(runtime: &R, stream: &str) -> Result<bool, String> {
    let Some(plan) = runtime.admin_get(&plan_key(stream))? else {
        return Ok(false);
    };
    let Some(id) = plan_id_of(&plan.value) else {
        return Ok(false);
    };
    let committed = runtime
        .admin_get(&watermark_key(stream))?
        .and_then(|r| r.value["run_id"].as_str().map(str::to_owned));
    Ok(committed.as_deref() != Some(id))
}

fn finish<R: Runtime>(
    store: &Store<'_, R>,
    plan: &Plan,
    loaded: Vec<Loaded>,
    watermark: Option<&crate::admin::Record>,
    buffer: &LogBuffer,
    report: &mut Report,
) -> Result<(), String> {
    let runtime = store.runtime;
    let prefix = processed_prefix(store.service);
    let mut by_window: BTreeMap<i64, Vec<&RecordBatch>> = BTreeMap::new();
    for item in &loaded {
        if let Outcome::Rows(rows) = &item.outcome {
            for (start, batch) in &rows.windows {
                by_window.entry(*start).or_default().push(batch);
            }
        }
    }
    let mut outputs_by_window = BTreeMap::new();
    for (start, batches) in &by_window {
        let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        let end = start + plan.window_s;
        let bytes = convert::write_parquet(batches)?;
        let size = bytes.len() as u64;
        let key = processed_key(&prefix, *start, end, &plan.run_id, rows);
        store.put(&key, bytes)?;
        report.bytes += size;
        report.rows += rows;
        report.files.push(key.clone());
        outputs_by_window.insert(*start, (key, size));
    }
    let mut converted_keys = Vec::new();
    let mut converted_sizes = Vec::new();
    let mut held = Vec::new();
    for item in &loaded {
        let row = match &item.outcome {
            Outcome::Rows(rows) => {
                report.converted += 1;
                report.lines_read += rows.lines_read;
                report.bad_lines += rows.bad_lines;
                converted_keys.push(item.key.clone());
                converted_sizes.push(item.gz_size);
                if let Some(sample) = &rows.bad_sample {
                    write_log(
                        runtime,
                        buffer,
                        WritableLogKind::PlatformErrors,
                        json!({
                            "error_domain": "ingest_corrupt", "code": "bad_ndjson_line",
                            "severity": "warn", "caller": "cron:ingest",
                            "object_key": item.key,
                            "message": format!("{} of {} line(s) rejected", rows.bad_lines, rows.lines_read),
                            "sample_json": sample_json(sample),
                        }),
                    );
                }
                json!({
                    "status": "converted", "rows": rows.rows, "lines_read": rows.lines_read,
                    "bad_lines": rows.bad_lines,
                    "output_keys_json": json!(rows.windows.iter().map(|(start, batch)| {
                        let (key, bytes) = &outputs_by_window[start];
                        json!({"key": key, "rows": batch.num_rows(), "bytes": bytes})
                    }).collect::<Vec<_>>()).to_string(),
                })
            }
            Outcome::Failed(reason) => {
                report.failed += 1;
                let corrupt = quarantined(reason);
                held.push(held_object(
                    &item.key,
                    item.gz_size,
                    if corrupt { "quarantined" } else { "failed" },
                    reason,
                    1,
                ));
                if corrupt {
                    write_log(
                        runtime,
                        buffer,
                        WritableLogKind::PlatformErrors,
                        json!({
                            "error_domain": "ingest_corrupt", "code": reason, "severity": "warn",
                            "caller": "cron:ingest", "object_key": item.key,
                            "message": if *reason == "too_many_windows" {
                                "raw object spans too many windows and was quarantined"
                            } else {
                                "raw object could not be decoded and was quarantined"
                            },
                        }),
                    );
                }
                json!({
                    "status": if corrupt { "quarantined" } else { "failed" },
                    "severity": "warn", "reason": reason, "rows": 0, "lines_read": 0,
                    "bad_lines": 0, "output_keys_json": "[]",
                })
            }
        };
        let Value::Object(mut row) = row else {
            continue;
        };
        row.insert("run_id".into(), plan.run_id.clone().into());
        row.insert("dataset".into(), "request".into());
        row.insert("raw_key".into(), item.key.clone().into());
        row.insert("raw_size".into(), item.gz_size.into());
        row.insert("state_version".into(), 1.into());
        row.insert("raw_deleted".into(), false.into());
        write_log(
            runtime,
            buffer,
            WritableLogKind::IngestFiles,
            Value::Object(row),
        );
    }
    if !converted_keys.is_empty() || !held.is_empty() {
        runtime.admin_cas(
            &format!("{DELETEQ_PREFIX}{}", plan.run_id),
            None,
            &json!({
                "run_id": plan.run_id, "keys": converted_keys, "sizes": converted_sizes,
                "held": held, "planned_at": plan.created_at, "at": runtime.now(),
            }),
        )?;
    }
    let last = plan.raw_keys.last().cloned().unwrap_or_default();
    // Legacy plans don't record their output hours; any may be closed.
    runtime.admin_cas(
        WATERMARK_KEY,
        watermark.map(|r| r.generation.as_str()),
        &bump_manifest_gen(
            watermark.map(|r| &r.value),
            watermark_value(
                watermark.map(|r| &r.value),
                &last,
                &plan.run_id,
                plan.created_at,
                &[],
                runtime.now(),
            ),
        ),
    )?;
    Ok(())
}

/// Wraps the sample so the serialized, escaped value fits the log field cap as valid JSON.
fn sample_json(sample: &str) -> Value {
    let mut end = sample.len();
    loop {
        let prefix = compute_common::str::truncate_utf8(sample, end);
        let wrapped = json!({"line": prefix});
        if prefix.is_empty() || wrapped.to_string().len() <= crate::telemetry::MAX_JSON_FIELD_BYTES
        {
            return wrapped;
        }
        end = prefix.len().saturating_sub(1);
    }
}

pub fn rfc3339(seconds: i64) -> String {
    compute_common::runtime::rfc3339(seconds)
}
