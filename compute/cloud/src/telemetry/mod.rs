//! Operational logging core: `write_log`, durability tiers, the per-request buffer, live reads,
//! redaction and settings (design spec 2026-10-30, §2 and §3).
//!
//! `write_log` takes a [`WritableLogKind`], which has no `AdminAudit` variant, so audit rows
//! cannot be written directly:
//!
//! ```compile_fail
//! use fastly_log_analytics::shell::Runtime;
//! use fastly_log_analytics::telemetry::{write_log, LogBuffer, LogKind};
//! fn audit(runtime: &impl Runtime, buffer: &LogBuffer) {
//!     write_log(runtime, buffer, LogKind::AdminAudit, serde_json::json!({}));
//! }
//! ```
//!
//! ```compile_fail
//! use fastly_log_analytics::telemetry::WritableLogKind;
//! let _ = WritableLogKind::AdminAudit;
//! ```

pub mod api;
pub mod compact;
pub mod execution;
pub mod flush;
pub mod schema;
#[cfg(test)]
mod tests;
pub mod usage;

use crate::shell::{CoreCacheEntry, Runtime};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

pub const LOG_SHARDS: u32 = 4;
pub const MAX_ROWS_PER_KIND: usize = 256;
pub const MAX_BUCKET_BYTES: usize = 1 << 20;
pub const MAX_ROW_BYTES: usize = 4096;
pub const MAX_JSON_FIELD_BYTES: usize = 2048;
const MAX_CRON_DETAILS_BYTES: usize = 96 * 1024;
const MAX_CRON_ROW_BYTES: usize = 128 * 1024;
pub const BUCKET_TTL: Duration = Duration::from_secs(86_400);
pub const DEFAULT_PAGE: usize = 500;
pub const MAX_PAGE: usize = 2000;
/// Closed minutes `(scan_min, now_min − 2]` inspected at once; the guard and open minutes are
/// read separately.
pub const MAX_SCAN_MIN: u64 = 30;
const LIST_PAGE: u32 = 100;
const MAX_LIST_PAGES: usize = 50;
/// Seconds per outbox batch key, the clock tick interval.
pub const OUTBOX_BUCKET_S: u64 = 120;
/// Most NDJSON bytes appended to one outbox key before writers roll to the next, well under
/// the 2 MiB a KV read accepts.
pub const OUTBOX_VALUE_CAP: usize = 512 * 1024;
/// Rollover keys `<bucket>_<n>` a writer walks past full ones before writing an overflow key.
const OUTBOX_MAX_ROLLS: u32 = 3;
/// Re-read-and-merge retries after a lost generation CAS.
const OUTBOX_RETRIES: u32 = 2;
/// Calls the batched append may spend before it falls back to a one-call overflow key.
const OUTBOX_APPEND_CALLS: u32 = 6;
const REDACTED: &str = "[redacted]";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogKind {
    CronRuns,
    CronTicks,
    IngestFiles,
    FosUsage,
    AdminAudit,
    ApiTelemetry,
    PlatformErrors,
}

/// Kinds a caller may write. `AdminAudit` is absent on purpose: audit rows are archived from
/// the existing durable outboxes by the flush only. `CronTicks` is retired: the 1 s dispatcher
/// counts idle ticks on the `cron_runs` rows instead, and old files stay readable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WritableLogKind {
    CronRuns,
    IngestFiles,
    FosUsage,
    ApiTelemetry,
    PlatformErrors,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tier {
    /// Archived from the existing audit outboxes.
    Audit,
    /// KV outbox batches, one `logq_<sid>_<bucket>` key per 2-minute bucket (see
    /// [`append_outbox`]).
    Outbox,
    /// Sharded Core Cache minute buckets.
    Cache,
}

impl WritableLogKind {
    pub fn parse(value: &str) -> Option<Self> {
        Some(match LogKind::parse(value)? {
            LogKind::CronRuns => Self::CronRuns,
            LogKind::IngestFiles => Self::IngestFiles,
            LogKind::FosUsage => Self::FosUsage,
            LogKind::ApiTelemetry => Self::ApiTelemetry,
            LogKind::PlatformErrors => Self::PlatformErrors,
            LogKind::AdminAudit | LogKind::CronTicks => return None,
        })
    }
}

impl From<WritableLogKind> for LogKind {
    fn from(kind: WritableLogKind) -> Self {
        match kind {
            WritableLogKind::CronRuns => Self::CronRuns,
            WritableLogKind::IngestFiles => Self::IngestFiles,
            WritableLogKind::FosUsage => Self::FosUsage,
            WritableLogKind::ApiTelemetry => Self::ApiTelemetry,
            WritableLogKind::PlatformErrors => Self::PlatformErrors,
        }
    }
}

impl LogKind {
    pub const ALL: [LogKind; 7] = [
        Self::CronRuns,
        Self::CronTicks,
        Self::IngestFiles,
        Self::FosUsage,
        Self::AdminAudit,
        Self::ApiTelemetry,
        Self::PlatformErrors,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::CronRuns => "cron_runs",
            Self::CronTicks => "cron_ticks",
            Self::IngestFiles => "ingest_files",
            Self::FosUsage => "fos_usage",
            Self::AdminAudit => "admin_audit",
            Self::ApiTelemetry => "api_telemetry",
            Self::PlatformErrors => "platform_errors",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.as_str() == value)
    }

    pub fn tier(self) -> Tier {
        match self {
            Self::AdminAudit => Tier::Audit,
            Self::CronRuns | Self::IngestFiles => Tier::Outbox,
            _ => Tier::Cache,
        }
    }

    pub fn surrogate_key(self) -> String {
        format!("flp-log-{}", self.as_str())
    }

    pub fn default_retention_days(self) -> u32 {
        match self {
            Self::AdminAudit => 365,
            Self::CronRuns | Self::IngestFiles | Self::PlatformErrors | Self::FosUsage => 30,
            Self::ApiTelemetry | Self::CronTicks => 14,
        }
    }
}

pub fn fnv1a(value: &str) -> u32 {
    compute_common::crypto::fnv1a32(value.bytes())
}

pub fn shard_of(request_id: &str) -> u32 {
    fnv1a(request_id) % LOG_SHARDS
}

pub fn bucket_key(kind: LogKind, sid: &str, epoch_min: u64, shard: u32) -> String {
    format!("flp:log:v1:{}:{sid}:{epoch_min}:{shard}", kind.as_str())
}

/// Stable per logical source event, so re-archiving or retries deduplicate.
pub fn audit_event_id(source: &str, source_seq: u64) -> String {
    compute_common::crypto::sha256_hex(format!("{source}:{source_seq}").as_bytes())[..32].to_owned()
}

#[derive(Default)]
struct Inner {
    cache: BTreeMap<LogKind, Vec<String>>,
    outbox: Vec<String>,
    outbox_first_id: Option<String>,
    cap_dropped: u64,
    shield_dropped: u64,
    extra_dropped: u64,
    seq: u64,
}

/// Rows written during one request; appended to Core Cache and KV once by
/// [`flush_request_buffer`].
pub struct LogBuffer {
    sid: String,
    request_id: String,
    inner: RefCell<Inner>,
}

impl LogBuffer {
    pub fn new(sid: &str, request_id: &str) -> Self {
        Self {
            sid: sid.into(),
            request_id: request_id.into(),
            inner: RefCell::default(),
        }
    }

    /// Counts a row intentionally not recorded, such as a rate-limited beacon.
    pub fn note_dropped(&self) {
        self.inner.borrow_mut().extra_dropped += 1;
    }
}

fn event_id(runtime: &impl Runtime, buffer: &LogBuffer, inner: &mut Inner) -> String {
    inner.seq += 1;
    match runtime.random() {
        Ok(bytes) => hex::encode(&bytes[..16]),
        Err(_) => compute_common::crypto::sha256_hex(
            format!("{}:{}:{}", buffer.request_id, runtime.now(), inner.seq).as_bytes(),
        )[..32]
            .to_owned(),
    }
}

fn truncate_str(value: &mut String, max: usize) {
    compute_common::str::truncate_string_utf8(value, max);
}

fn build_line(
    runtime: &impl Runtime,
    buffer: &LogBuffer,
    inner: &mut Inner,
    kind: LogKind,
    row: Value,
    outbox: bool,
    preset_id: Option<&str>,
) -> (String, String) {
    let Value::Object(mut map) = redact(row) else {
        return build_line(runtime, buffer, inner, kind, json!({}), outbox, preset_id);
    };
    let id = preset_id.map_or_else(|| event_id(runtime, buffer, inner), str::to_owned);
    map.insert("event_id".into(), id.clone().into());
    map.entry("timestamp")
        .or_insert_with(|| (runtime.now() * 1000).into());
    map.entry("service_id")
        .or_insert_with(|| buffer.sid.clone().into());
    map.entry("severity").or_insert_with(|| "info".into());
    if matches!(kind, LogKind::CronTicks | LogKind::ApiTelemetry) {
        if let Some(pop) = runtime.pop() {
            map.entry("pop").or_insert_with(|| pop.into());
        }
    }
    for field in ["details_json", "sample_json"] {
        if let Some(value) = map.get_mut(field) {
            let mut text = match &*value {
                Value::String(text) => text.clone(),
                other => other.to_string(),
            };
            let limit = if kind == LogKind::CronRuns && field == "details_json" {
                MAX_CRON_DETAILS_BYTES
            } else {
                MAX_JSON_FIELD_BYTES
            };
            if kind == LogKind::CronRuns && field == "details_json" && text.len() > limit {
                text = json!({"telemetry_incomplete": "cron_details_capacity"}).to_string();
            } else {
                truncate_str(&mut text, limit);
            }
            *value = text.into();
        }
    }
    if outbox {
        map.insert("kind".into(), kind.as_str().into());
    }
    let limit = if kind == LogKind::CronRuns {
        MAX_CRON_ROW_BYTES
    } else {
        MAX_ROW_BYTES
    };
    (id, bounded_line(map, limit))
}

fn bounded_line(mut map: Map<String, Value>, limit: usize) -> String {
    loop {
        let line = Value::Object(map.clone()).to_string();
        if line.len() <= limit {
            return line;
        }
        let over = line.len() - limit;
        let longest = map
            .iter()
            .filter_map(|(key, v)| v.as_str().map(|s| (s.len(), key.clone())))
            .max();
        match longest {
            Some((len, key)) if len > 0 => {
                let mut text = map[&key].as_str().unwrap_or_default().to_string();
                truncate_str(&mut text, len.saturating_sub(over));
                map.insert(key, text.into());
            }
            _ => return line,
        }
    }
}

/// Queues a row on the request buffer and returns its server-generated `event_id`.
pub fn write_log(
    runtime: &impl Runtime,
    buffer: &LogBuffer,
    kind: WritableLogKind,
    row: Value,
) -> String {
    write_log_inner(runtime, buffer, kind, row, None)
}

/// A server-generated `event_id` for [`write_log_with_id`], so rows written elsewhere (by a
/// worker execution) can reference a row this request writes later.
pub fn new_event_id(runtime: &impl Runtime, buffer: &LogBuffer) -> String {
    event_id(runtime, buffer, &mut buffer.inner.borrow_mut())
}

/// Like [`write_log`] with an `event_id` from [`new_event_id`].
pub fn write_log_with_id(
    runtime: &impl Runtime,
    buffer: &LogBuffer,
    kind: WritableLogKind,
    row: Value,
    event_id: &str,
) -> String {
    write_log_inner(runtime, buffer, kind, row, Some(event_id))
}

/// Whether developer telemetry controls are unlocked in this deployment via `flp_config`
/// (`dev_telemetry = "1"` / `"true"`) or build-time `FLP_DEV_TELEMETRY=1`.
pub fn dev_telemetry_available(runtime: &impl Runtime) -> bool {
    if let Ok(Some(v)) = runtime.config("dev_telemetry") {
        let trimmed = v.trim();
        if matches!(trimmed, "1" | "true" | "TRUE") {
            return true;
        }
        if matches!(trimmed, "0" | "false" | "FALSE") {
            return false;
        }
    }
    if let Some(v) = option_env!("FLP_DEV_TELEMETRY") {
        return matches!(v.trim(), "1" | "true" | "TRUE");
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        true
    }
    #[cfg(target_arch = "wasm32")]
    {
        false
    }
}

thread_local! {
    static DEV_TELEMETRY_ENABLED: Cell<bool> = const { Cell::new(true) };
}

pub(crate) fn set_dev_telemetry_enabled(enabled: bool) {
    DEV_TELEMETRY_ENABLED.with(|cell| cell.set(enabled));
}

/// Whether developer-only telemetry datasets (`fos_usage`, `api_telemetry`) are currently
/// active.
pub fn effective_dev_telemetry(runtime: &impl Runtime) -> bool {
    dev_telemetry_available(runtime) && DEV_TELEMETRY_ENABLED.with(Cell::get)
}

fn write_log_inner(
    runtime: &impl Runtime,
    buffer: &LogBuffer,
    kind: WritableLogKind,
    row: Value,
    preset_id: Option<&str>,
) -> String {
    // `ingest_files` feeds the Logs page's Ingestion History on both deployments, so it is
    // always on; only the cost-auditing kinds are developer telemetry.
    if matches!(
        kind,
        WritableLogKind::FosUsage | WritableLogKind::ApiTelemetry
    ) && !effective_dev_telemetry(runtime)
    {
        return String::new();
    }
    let kind = LogKind::from(kind);
    let mut inner = buffer.inner.borrow_mut();
    match kind.tier() {
        Tier::Cache => {
            if !runtime.on_log_shield() {
                inner.shield_dropped += 1;
                return String::new();
            }
            if inner
                .cache
                .get(&kind)
                .is_some_and(|rows| rows.len() >= MAX_ROWS_PER_KIND)
            {
                inner.cap_dropped += 1;
                return String::new();
            }
            let (id, line) = build_line(runtime, buffer, &mut inner, kind, row, false, preset_id);
            inner.cache.entry(kind).or_default().push(line);
            id
        }
        _ => {
            let (id, line) = build_line(runtime, buffer, &mut inner, kind, row, true, preset_id);
            inner.outbox_first_id.get_or_insert_with(|| id.clone());
            inner.outbox.push(line);
            id
        }
    }
}

#[derive(Default, Serialize, Deserialize)]
struct BucketMeta {
    count: u64,
    bytes: u64,
    dropped: u64,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct FlushOutcome {
    pub dropped: u64,
    pub outbox_error: Option<String>,
}

impl FlushOutcome {
    pub fn header(&self) -> Option<(&'static str, String)> {
        (self.dropped > 0).then(|| ("x-flp-log-dropped", self.dropped.to_string()))
    }
}

/// `Ok(rejected_rows)`: rows refused because the bucket is full. `Err` means the replace failed.
fn append_bucket(
    runtime: &impl Runtime,
    kind: LogKind,
    sid: &str,
    epoch_min: u64,
    shard: u32,
    lines: &[String],
) -> Result<u64, ()> {
    let added: Vec<u8> = lines
        .iter()
        .flat_map(|line| line.bytes().chain(std::iter::once(b'\n')))
        .collect();
    let rows = lines.len() as u64;
    let rejected = Cell::new(false);
    let update = |existing: Option<CoreCacheEntry>| {
        let (mut body, meta) = existing.unwrap_or_default();
        let mut meta: BucketMeta = serde_json::from_slice(&meta).unwrap_or_default();
        if body.len() + added.len() > MAX_BUCKET_BYTES {
            rejected.set(true);
            meta.dropped += rows;
        } else {
            body.extend_from_slice(&added);
            meta.count += rows;
            meta.bytes = body.len() as u64;
        }
        let meta = serde_json::to_vec(&meta).unwrap_or_default();
        (body, meta)
    };
    let surrogate_kind = kind.surrogate_key();
    let surrogate_hour = format!("{surrogate_kind}-{sid}-h{}", epoch_min / 60);
    runtime
        .core_cache_replace(
            &bucket_key(kind, sid, epoch_min, shard),
            &["flp-logs-all", &surrogate_kind, &surrogate_hour],
            BUCKET_TTL,
            &update,
        )
        .map_err(|_| ())?;
    Ok(if rejected.get() { rows } else { 0 })
}

/// Stem of every outbox key of `sid`. Service ids have no `_`, so one sid's stem never
/// prefixes another's, and `_` is searchable in a KV list prefix where `:` is not.
pub fn outbox_stem(sid: &str) -> String {
    format!("logq_{sid}_")
}

/// `logq_<sid>_<bucket>` (zero-padded so keys sort by bucket), then `_<n>` for a rollover or
/// `_x<event id>` for an overflow key.
pub fn outbox_key(sid: &str, bucket: u64, suffix: Option<&str>) -> String {
    match suffix {
        Some(suffix) => format!("{}{bucket:010}_{suffix}", outbox_stem(sid)),
        None => format!("{}{bucket:010}", outbox_stem(sid)),
    }
}

/// List prefix covering every sid's batch keys and the per-request `logq:{sid}:{ms}-{id}` keys
/// written before batching, which still hold unflushed rows. `:` cannot appear in a KV list
/// prefix, so the sid is filtered by [`outbox_bucket`].
pub const OUTBOX_LIST_PREFIX: &str = "logq";

/// The 2-minute bucket an outbox key of `sid` belongs to, for batch and legacy keys alike.
pub fn outbox_bucket(key: &str, sid: &str) -> Option<u64> {
    if let Some(rest) = key.strip_prefix(&outbox_stem(sid)) {
        return rest.get(..10)?.parse().ok();
    }
    let ms: u64 = key
        .strip_prefix(&format!("logq:{sid}:"))?
        .get(..13)?
        .parse()
        .ok()?;
    Some(ms / 1000 / OUTBOX_BUCKET_S)
}

/// Appends NDJSON rows to the current bucket's outbox key with a generation CAS. A lost CAS
/// re-reads, merges and retries up to `OUTBOX_RETRIES` times; a full key rolls to `_<n>`. When
/// retries, rollovers or the call budget run out, the rows go to a create-only overflow key
/// named after their first event id, so nothing is lost and a short budget costs one call.
/// The bucket is recomputed on every attempt, so a write never lands in a bucket that closed
/// before it started (the flush relies on this; see `flush::closed_bucket`).
pub fn append_outbox(
    runtime: &impl Runtime,
    sid: &str,
    first_id: &str,
    ndjson: &str,
) -> Result<(), String> {
    append_outbox_reserving(runtime, sid, first_id, ndjson, 0)
}

/// Like [`append_outbox`], leaving `reserve` calls for the caller: the batched append runs only
/// while a GET, a CAS, the overflow write and the reserve still fit.
pub fn append_outbox_reserving(
    runtime: &impl Runtime,
    sid: &str,
    first_id: &str,
    ndjson: &str,
    reserve: u32,
) -> Result<(), String> {
    // Counted here: in a request's telemetry mode the remaining count may not move per call.
    let spent = Cell::new(0u32);
    let call = || spent.set(spent.get() + 1);
    let (mut roll, mut losses) = (0u32, 0u32);
    let mut bucket = runtime.now() / OUTBOX_BUCKET_S;
    // A GET and a CAS, leaving one call for the overflow key.
    while ndjson.len() < OUTBOX_VALUE_CAP
        && spent.get() + 2 <= OUTBOX_APPEND_CALLS
        && runtime.admin_remaining_calls() >= 3 + reserve
    {
        let now_bucket = runtime.now() / OUTBOX_BUCKET_S;
        if now_bucket != bucket {
            (bucket, roll) = (now_bucket, 0);
        }
        let suffix = (roll > 0).then(|| roll.to_string());
        let key = outbox_key(sid, bucket, suffix.as_deref());
        call();
        let Ok(current) = runtime.admin_get(&key) else {
            break;
        };
        let existing = current
            .as_ref()
            .and_then(|record| record.value["ndjson"].as_str())
            .unwrap_or_default();
        if existing.len() + 1 + ndjson.len() > OUTBOX_VALUE_CAP {
            if roll >= OUTBOX_MAX_ROLLS {
                break;
            }
            roll += 1;
            continue;
        }
        let merged = if existing.is_empty() {
            ndjson.to_owned()
        } else {
            format!("{existing}\n{ndjson}")
        };
        let generation = current.as_ref().map(|record| record.generation.as_str());
        call();
        match runtime.admin_cas(&key, generation, &json!({ "ndjson": merged })) {
            Ok(true) => return Ok(()),
            Ok(false) if losses < OUTBOX_RETRIES => losses += 1,
            _ => break,
        }
    }
    let bucket = runtime.now() / OUTBOX_BUCKET_S;
    let key = outbox_key(sid, bucket, Some(&format!("x{first_id}")));
    match runtime.admin_cas(&key, None, &json!({ "ndjson": ndjson })) {
        Ok(true) => Ok(()),
        Ok(false) => Err("outbox_key_exists".into()),
        Err(error) => Err(error),
    }
}

/// Appends the request's rows once: one Core Cache replace per touched kind, and one batched
/// KV outbox append for all durable rows.
pub fn flush_request_buffer(runtime: &impl Runtime, buffer: &LogBuffer) -> FlushOutcome {
    flush_request_buffer_reserving(runtime, buffer, 0)
}

/// Like [`flush_request_buffer`], keeping `reserve` backend calls for the caller's own writes.
pub fn flush_request_buffer_reserving(
    runtime: &impl Runtime,
    buffer: &LogBuffer,
    reserve: u32,
) -> FlushOutcome {
    let mut inner = buffer.inner.take();
    let now_s = runtime.now();
    let epoch_min = now_s / 60;
    let mut outcome = FlushOutcome::default();
    let mut dropped = inner.cap_dropped + inner.shield_dropped + inner.extra_dropped;

    if !inner.outbox.is_empty() {
        let id = inner.outbox_first_id.clone().unwrap_or_default();
        let ndjson = inner.outbox.join("\n");
        let error = append_outbox_reserving(runtime, &buffer.sid, &id, &ndjson, reserve).err();
        if error.is_some() {
            let mut kept = buffer.inner.borrow_mut();
            kept.outbox = std::mem::take(&mut inner.outbox);
            kept.outbox_first_id = Some(id);
        }
        outcome.outbox_error = error;
    }

    let shard = shard_of(&buffer.request_id);
    let mut failed_rows = 0u64;
    for kind in [LogKind::ApiTelemetry, LogKind::FosUsage] {
        let Some(lines) = inner.cache.remove(&kind) else {
            continue;
        };
        match append_bucket(runtime, kind, &buffer.sid, epoch_min, shard, &lines) {
            Ok(rejected) => dropped += rejected,
            Err(()) => failed_rows += lines.len() as u64,
        }
    }
    dropped += failed_rows;

    if runtime.on_log_shield() {
        let mut errors = inner
            .cache
            .remove(&LogKind::PlatformErrors)
            .unwrap_or_default();
        let synthetic = |code: &str, message: String| {
            json!({"error_domain": "core_cache", "code": code, "severity": "warn",
                   "caller": "telemetry", "message": message})
        };
        let mut extra = Vec::new();
        if failed_rows > 0 {
            extra.push(synthetic(
                "core_cache_replace_failed",
                format!("{failed_rows} row(s) not written"),
            ));
        }
        if inner.cap_dropped > 0 {
            extra.push(synthetic(
                "log_buffer_full",
                format!("{} row(s) over the per-request cap", inner.cap_dropped),
            ));
        }
        for row in extra {
            let (_, line) = build_line(
                runtime,
                buffer,
                &mut inner,
                LogKind::PlatformErrors,
                row,
                false,
                None,
            );
            errors.push(line);
        }
        if !errors.is_empty() {
            match append_bucket(
                runtime,
                LogKind::PlatformErrors,
                &buffer.sid,
                epoch_min,
                shard,
                &errors,
            ) {
                Ok(rejected) => dropped += rejected,
                Err(()) => dropped += errors.len() as u64,
            }
        }
    }
    outcome.dropped = dropped;
    outcome
}

#[derive(Debug, Default, PartialEq)]
pub struct LivePage {
    pub rows: Vec<Value>,
    pub next_cursor: Option<String>,
    /// Sources were omitted, rather than represented by a continuation, at request limits.
    pub truncated: bool,
}

/// Backend calls left for the caller when a live read stops fetching sources.
const LIVE_CALL_RESERVE: u32 = 4;
/// Core Cache operations a live read leaves for the request's own telemetry.
const LIVE_CACHE_RESERVE: u32 = 6;
const LIVE_MAX_BYTES: usize = 8 * 1024 * 1024;
/// Explicit opt-in to source-order pagination, not globally timestamp-sorted pagination.
pub const LIVE_SOURCE_CURSOR: &str = "source-v1";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LiveSourceCursor {
    sid: String,
    kind: String,
    since: Option<i64>,
    list: Option<String>,
    done: bool,
    /// Listed outbox keys not yet read, in store order; one list call feeds many reads.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pending: Vec<String>,
    key: Option<String>,
    offset: usize,
    hash: Option<String>,
}

/// Outbox keys fetched per list call while paging live sources. Listing one key per call spent
/// half of a page's KV budget on listing; the keys ride in the cursor until they are read.
const LIVE_LIST_PAGE: u32 = 50;

fn read_live_sources(
    runtime: &impl Runtime,
    sid: &str,
    kind: LogKind,
    since: Option<i64>,
    limit: usize,
    cursor: &str,
) -> Result<LivePage, String> {
    let invalid = || "invalid_live_source_cursor".to_string();
    if kind.tier() != Tier::Outbox || cursor.len() > 16 * 1024 {
        return Err(invalid());
    }
    let mut state = if cursor == LIVE_SOURCE_CURSOR {
        LiveSourceCursor {
            sid: sid.into(),
            kind: kind.as_str().into(),
            since,
            list: None,
            done: false,
            pending: Vec::new(),
            key: None,
            offset: 0,
            hash: None,
        }
    } else {
        let payload = cursor.strip_prefix("source-v1:").ok_or_else(invalid)?;
        serde_json::from_slice::<LiveSourceCursor>(&hex::decode(payload).map_err(|_| invalid())?)
            .map_err(|_| invalid())?
    };
    if state.sid != sid
        || state.kind != kind.as_str()
        || state.since != since
        || state
            .key
            .as_ref()
            .is_some_and(|key| outbox_bucket(key, sid).is_none())
        || state.pending.len() > LIVE_LIST_PAGE as usize
        || state
            .pending
            .iter()
            .any(|key| outbox_bucket(key, sid).is_none())
        || (state.key.is_none() && (state.offset != 0 || state.hash.is_some()))
        || (state.offset == 0) != state.hash.is_none()
    {
        return Err(invalid());
    }
    let mut rows = Vec::new();
    let mut bytes_read = 0;
    let mut scanned_rows = 0;
    let mut calls = 0;
    loop {
        if state.key.is_none() && !state.pending.is_empty() {
            state.key = Some(state.pending.remove(0));
        }
        if state.key.is_none() && state.done {
            return Ok(LivePage {
                rows,
                next_cursor: None,
                truncated: false,
            });
        }
        if rows.len() >= limit
            || scanned_rows >= MAX_PAGE
            || bytes_read >= LIVE_MAX_BYTES
            || calls >= 32 - LIVE_CALL_RESERVE
            || runtime.admin_remaining_calls() <= LIVE_CALL_RESERVE
        {
            if calls == 0 && scanned_rows == 0 {
                return Err("live_source_budget_exhausted".into());
            }
            let payload = serde_json::to_vec(&state).map_err(|_| invalid())?;
            return Ok(LivePage {
                rows,
                next_cursor: Some(format!("{LIVE_SOURCE_CURSOR}:{}", hex::encode(payload))),
                truncated: false,
            });
        }
        if state.key.is_none() {
            // Keep the backend's opaque cursor, not an assumed lexical start-after key.
            let page =
                runtime.kv_list(OUTBOX_LIST_PREFIX, state.list.as_deref(), LIVE_LIST_PAGE)?;
            calls += 1;
            if page.keys.len() > LIVE_LIST_PAGE as usize
                || (page.next_cursor.is_some() && page.next_cursor == state.list)
            {
                return Err("live_source_list_did_not_advance".into());
            }
            state.list = page.next_cursor;
            state.done = state.list.is_none();
            state.pending = page
                .keys
                .into_iter()
                .filter(|key| outbox_bucket(key, sid).is_some())
                .collect();
            continue;
        }
        let key = state.key.as_deref().ok_or_else(invalid)?;
        let record = runtime.admin_get(key)?;
        calls += 1;
        let record = record.ok_or("live_source_changed")?;
        let body = record.value["ndjson"]
            .as_str()
            .ok_or("invalid_live_source")?;
        bytes_read += body.len();
        let hash = compute_common::crypto::sha256_hex(body.as_bytes());
        if state
            .hash
            .as_ref()
            .is_some_and(|expected| expected != &hash)
        {
            return Err("live_source_changed".into());
        }
        if !body.is_char_boundary(state.offset) || state.offset > body.len() {
            return Err(invalid());
        }
        for line in body[state.offset..].split_inclusive('\n') {
            if rows.len() >= limit || scanned_rows >= MAX_PAGE {
                break;
            }
            scanned_rows += 1;
            state.offset += line.len();
            if line.trim().is_empty() {
                continue;
            }
            let Value::Object(mut row) =
                serde_json::from_str::<Value>(line).map_err(|_| "invalid_live_source")?
            else {
                return Err("invalid_live_source".into());
            };
            if row.get("kind").and_then(Value::as_str) == Some(kind.as_str()) {
                let timestamp = row
                    .get("timestamp")
                    .and_then(Value::as_i64)
                    .ok_or("invalid_live_source")?;
                if since.is_none_or(|since| timestamp >= since) {
                    row.remove("kind");
                    rows.push(Value::Object(row));
                }
            }
        }
        if state.offset == body.len() {
            state.key = None;
            state.offset = 0;
            state.hash = None;
        } else {
            state.hash = Some(hash);
        }
    }
}

pub fn page_limit(requested: Option<usize>) -> usize {
    requested.unwrap_or(DEFAULT_PAGE).clamp(1, MAX_PAGE)
}

fn sort_key(row: &Value) -> (i64, String) {
    (
        row["timestamp"].as_i64().unwrap_or(0),
        row["event_id"].as_str().unwrap_or_default().to_string(),
    )
}

fn push_lines(rows: &mut Vec<Value>, text: &str, kind: Option<LogKind>) {
    for line in text.lines() {
        let Ok(Value::Object(mut row)) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if let Some(kind) = kind {
            if row.get("kind").and_then(Value::as_str) != Some(kind.as_str()) {
                continue;
            }
        }
        row.remove("kind");
        rows.push(Value::Object(row));
    }
}

fn list_all(runtime: &impl Runtime, prefix: &str) -> Result<Vec<String>, String> {
    let mut keys = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..MAX_LIST_PAGES {
        let page = runtime.kv_list(prefix, cursor.as_deref(), LIST_PAGE)?;
        keys.extend(page.keys);
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    Ok(keys)
}

pub(crate) fn auth_audit_row(sid: &str, seq: u64, entry: &Value) -> Value {
    json!({
        "timestamp": entry["time"].as_u64().unwrap_or(0) * 1000,
        "event_id": audit_event_id("auth_archive", seq),
        "service_id": sid, "severity": "info", "domain": "auth",
        "actor": entry["actor"], "action": entry["action"], "target": entry["target"],
        "ip": entry["ip"], "outcome": "ok", "source": "auth_archive", "source_seq": seq,
    })
}

/// `index` is absolute: `_audit_outbox_base` plus the position in the stored array.
pub(crate) fn svc_audit_row(sid: &str, index: u64, entry: &Value) -> Value {
    json!({
        "timestamp": entry["at"].as_u64().unwrap_or(0) * 1000,
        "event_id": audit_event_id(&format!("svc_outbox:{sid}"), index),
        "service_id": sid, "severity": "info", "domain": "control_plane",
        "actor": entry["actor"], "action": entry["event"], "job_id": entry["job_id"],
        "outcome": "ok", "source": "svc_outbox", "source_seq": index,
        "details_json": json!({"config_version": entry["config_version"]}).to_string(),
    })
}

fn audit_rows(
    runtime: &impl Runtime,
    sid: &str,
    cursor: &Value,
    rows: &mut Vec<Value>,
) -> Result<bool, String> {
    let archived_seq = cursor["audit_seq"].as_u64();
    let mut truncated = false;
    let mut keys = match list_all(runtime, "audit:") {
        Ok(keys) => keys,
        Err(e) if e.contains("budget") => return Ok(true),
        Err(e) => return Err(e),
    };
    keys.reverse();
    for key in keys {
        let Some(seq) = key
            .strip_prefix("audit:")
            .and_then(|s| s.parse::<u64>().ok())
        else {
            continue;
        };
        if archived_seq.is_some_and(|archived| seq <= archived) {
            continue;
        }
        if runtime.admin_remaining_calls() <= LIVE_CALL_RESERVE {
            truncated = true;
            break;
        }
        let Some(record) = runtime.admin_get(&key)? else {
            continue;
        };
        rows.push(auth_audit_row(sid, seq, &record.value));
    }
    Ok(truncated)
}

/// Unflushed rows, newest first: the pending summary's buckets, the unscanned and open
/// minutes, the durable outbox and (for `admin_audit`) the unarchived user/admin audit keys.
/// Service outbox events are service history and never appear in `admin_audit` reads.
///
/// Outbox consumers needing exhaustive coverage may start with `cursor=source-v1`.
/// That opt-in returns source/line order, with opaque, query-bound continuations for
/// both source and row limits. Only a terminal page certifies the traversal finished;
/// `truncated=false` on intermediate pages means no sources were discarded. A source
/// modified while partially read fails closed. Legacy timestamp cursors are unchanged.
pub fn read_live(
    runtime: &impl Runtime,
    sid: &str,
    kind: LogKind,
    since: Option<i64>,
    limit: Option<usize>,
    cursor: Option<&str>,
) -> Result<LivePage, String> {
    if matches!(kind, LogKind::FosUsage | LogKind::ApiTelemetry)
        && !effective_dev_telemetry(runtime)
    {
        return Ok(LivePage::default());
    }
    // Row timestamps are milliseconds. A seconds value would admit every row, so take a time
    // after 2001 in seconds (a millisecond time in that range is from 1970-1973) as seconds.
    let since = since.map(|since| {
        if (1_000_000_000..100_000_000_000).contains(&since) {
            since.saturating_mul(1000)
        } else {
            since
        }
    });
    let limit = page_limit(limit);
    if let Some(cursor) = cursor.filter(|cursor| cursor.starts_with(LIVE_SOURCE_CURSOR)) {
        return read_live_sources(runtime, sid, kind, since, limit, cursor);
    }
    let mut rows = Vec::new();
    let mut truncated = false;
    let mut bytes_read = 0usize;
    match kind.tier() {
        Tier::Cache => {
            let state = runtime
                .admin_get(&format!("logs:cursor:{sid}"))?
                .map_or(Value::Null, |record| record.value);
            let now_min = runtime.now() / 60;
            let closed_end = now_min.saturating_sub(2);
            // Without a cursor, show what the flush's fresh cursor will scan first.
            let scan_min = state["scan_min"]
                .as_u64()
                .unwrap_or(closed_end.saturating_sub(MAX_SCAN_MIN));
            let closed_start = (scan_min + 1).max(closed_end.saturating_sub(MAX_SCAN_MIN - 1));
            let mut buckets = BTreeSet::new();
            for minute in (closed_start..=closed_end).chain(closed_end + 1..=now_min) {
                for shard in 0..LOG_SHARDS {
                    buckets.insert((minute, shard));
                }
            }
            for entry in state["pending"]["nonempty"]
                .as_array()
                .into_iter()
                .flatten()
            {
                if entry[0].as_str() == Some(kind.as_str()) {
                    if let (Some(minute), Some(shard)) = (entry[1].as_u64(), entry[2].as_u64()) {
                        buckets.insert((minute, shard as u32));
                    }
                }
            }
            for (minute, shard) in buckets.into_iter().rev() {
                if crate::shell::cache_ops_remaining(runtime) <= LIVE_CACHE_RESERVE {
                    truncated = true;
                    break;
                }
                let key = bucket_key(kind, sid, minute, shard);
                if let Some((body, _)) =
                    runtime.core_cache_lookup(&key).map_err(|_| "core_cache")?
                {
                    bytes_read += body.len();
                    push_lines(&mut rows, &String::from_utf8_lossy(&body), None);
                    if bytes_read > LIVE_MAX_BYTES {
                        truncated = true;
                        break;
                    }
                }
            }
        }
        Tier::Outbox => {
            let mut keys = match list_all(runtime, OUTBOX_LIST_PREFIX) {
                Ok(keys) => keys
                    .into_iter()
                    .filter(|key| outbox_bucket(key, sid).is_some())
                    .collect(),
                Err(e) if e.contains("budget") => {
                    truncated = true;
                    Vec::new()
                }
                Err(e) => return Err(e),
            };
            keys.reverse();
            for key in keys {
                if runtime.admin_remaining_calls() <= LIVE_CALL_RESERVE
                    || bytes_read > LIVE_MAX_BYTES
                {
                    truncated = true;
                    break;
                }
                if let Some(record) = runtime.admin_get(&key)? {
                    bytes_read += record.value["ndjson"].as_str().map_or(0, str::len);
                    push_lines(
                        &mut rows,
                        record.value["ndjson"].as_str().unwrap_or_default(),
                        Some(kind),
                    );
                }
            }
        }
        Tier::Audit => {
            let state = runtime
                .admin_get(&format!("logs:cursor:{sid}"))?
                .map_or(Value::Null, |record| record.value);
            truncated = audit_rows(runtime, sid, &state, &mut rows)?;
        }
    }
    let mut seen = BTreeSet::new();
    rows.retain(|row| {
        since.is_none_or(|since| row["timestamp"].as_i64().unwrap_or(0) >= since)
            && seen.insert(sort_key(row).1)
    });
    rows.sort_by_key(|row| std::cmp::Reverse(sort_key(row)));
    if let Some(after) = cursor.and_then(decode_cursor) {
        rows.retain(|row| sort_key(row) < after);
    }
    let more = rows.len() > limit;
    rows.truncate(limit);
    let next_cursor = more
        .then(|| rows.last().map(|row| encode_cursor(&sort_key(row))))
        .flatten();
    Ok(LivePage {
        rows,
        next_cursor,
        truncated,
    })
}

fn encode_cursor(key: &(i64, String)) -> String {
    hex::encode(json!([key.0, key.1]).to_string())
}

fn decode_cursor(cursor: &str) -> Option<(i64, String)> {
    let value: Value = serde_json::from_slice(&hex::decode(cursor).ok()?).ok()?;
    Some((value[0].as_i64()?, value[1].as_str()?.to_string()))
}

/// Latest row per `raw_key` by `(state_version, timestamp, event_id)`.
pub fn latest_ingest_state(rows: &[Value]) -> Vec<Value> {
    let mut latest: BTreeMap<String, &Value> = BTreeMap::new();
    let rank = |row: &Value| {
        (
            row["state_version"].as_i64().unwrap_or(0),
            row["timestamp"].as_i64().unwrap_or(0),
            row["event_id"].as_str().unwrap_or_default().to_string(),
        )
    };
    for row in rows {
        let key = row["raw_key"].as_str().unwrap_or_default().to_string();
        match latest.get(&key) {
            Some(current) if rank(current) >= rank(row) => {}
            _ => {
                latest.insert(key, row);
            }
        }
    }
    latest.into_values().cloned().collect()
}

fn sensitive_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    [
        "authorization",
        "cookie",
        "token",
        "secret",
        "password",
        "signature",
        "credential",
    ]
    .iter()
    .any(|part| key.contains(part))
}

const SENSITIVE_PARAMS: [&str; 5] = ["signature", "credential", "token", "secret", "password"];

fn redact_text(text: &str) -> String {
    let lower = text.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let mut out = String::with_capacity(text.len());
    let (mut last, mut i) = (0, 0);
    while i < bytes.len() {
        let name = SENSITIVE_PARAMS.iter().find(|name| {
            bytes[i..].starts_with(name.as_bytes())
                && bytes.get(i + name.len()) == Some(&b'=')
                && (i == 0 || !bytes[i - 1].is_ascii_alphanumeric())
        });
        if let Some(name) = name {
            let start = i + name.len() + 1;
            let end = text[start..]
                .find(|c: char| {
                    matches!(c, '&' | '"' | '\'' | '\\' | ' ' | '\n' | '\t' | ',' | ';')
                })
                .map_or(text.len(), |n| start + n);
            out.push_str(&text[last..start]);
            out.push_str(REDACTED);
            last = end;
            i = end;
        } else {
            i += 1;
        }
    }
    out.push_str(&text[last..]);
    out
}

/// Removes sensitive fields at any depth and masks signed-URL and secret query parameters in
/// strings. Runs before any truncation so a cut can never leave a partial secret behind.
pub fn redact(value: Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.into_iter()
                .filter(|(key, _)| !sensitive_key(key))
                .map(|(key, value)| (key, redact(value)))
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.into_iter().map(redact).collect()),
        Value::String(text) => {
            let trimmed = text.trim_start();
            if trimmed.starts_with(['{', '[']) {
                if let Ok(parsed) = serde_json::from_str::<Value>(&text) {
                    return Value::String(redact(parsed).to_string());
                }
            }
            Value::String(redact_text(&text))
        }
        other => other,
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct OperationalLogSettings {
    pub dev_telemetry_enabled: bool,
    pub flush_max_rows: u32,
    pub flush_max_age_s: u32,
    pub flush_min_interval_s: u32,
    pub browser_query_telemetry: bool,
    pub compaction_enabled: bool,
    pub retention_days_by_kind: BTreeMap<String, u32>,
    pub retention_fanout: u32,
    pub worker_delete_fallback: bool,
}

impl Default for OperationalLogSettings {
    fn default() -> Self {
        Self {
            dev_telemetry_enabled: true,
            flush_max_rows: 500,
            flush_max_age_s: 3600,
            flush_min_interval_s: 300,
            browser_query_telemetry: true,
            compaction_enabled: true,
            retention_fanout: 8,
            worker_delete_fallback: true,
            retention_days_by_kind: LogKind::ALL
                .into_iter()
                .filter(|kind| *kind != LogKind::FosUsage)
                .map(|kind| (kind.as_str().to_string(), kind.default_retention_days()))
                .collect(),
        }
    }
}

impl OperationalLogSettings {
    pub fn validate(&self) -> Result<(), String> {
        if !(50..=5_000).contains(&self.flush_max_rows) {
            return Err("flush_max_rows must be 50-5000".into());
        }
        if !(300..=21_600).contains(&self.flush_max_age_s) {
            return Err("flush_max_age_s must be 300-21600".into());
        }
        if !(120..=3_600).contains(&self.flush_min_interval_s) {
            return Err("flush_min_interval_s must be 120-3600".into());
        }
        if self.flush_min_interval_s > self.flush_max_age_s {
            return Err("flush_min_interval_s must not exceed flush_max_age_s".into());
        }
        if !(1..=16).contains(&self.retention_fanout) {
            return Err("retention_fanout must be 1-16".into());
        }
        for (kind, days) in &self.retention_days_by_kind {
            match LogKind::parse(kind) {
                Some(LogKind::FosUsage) | None => {
                    return Err(format!("retention is not configurable for {kind}"))
                }
                Some(_) if !(1..=3_650).contains(days) => {
                    return Err(format!("retention for {kind} must be 1-3650 days"))
                }
                Some(_) => {}
            }
        }
        Ok(())
    }
}
