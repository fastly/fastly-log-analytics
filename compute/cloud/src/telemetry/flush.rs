//! `log_flush`: archives unflushed operational rows to `processed/ops/{kind}/` Parquet files
//! (design spec 2026-10-30, §2.5 and §2.8).
//!
//! One global cursor (`logs:cursor:{sid}`) tracks the scan watermark and a summary of pending
//! buckets. A flush persists an immutable manifest (`logs:flush_txn:{sid}`) that carries the
//! exact cursor it will commit, so a retry rewrites identical files and only a CAS on the
//! cursor generation recorded in the manifest can commit.

use super::{
    audit_event_id, auth_audit_row, bucket_key, outbox_bucket, schema, svc_audit_row, LogKind,
    OperationalLogSettings, LOG_SHARDS, MAX_SCAN_MIN, OUTBOX_BUCKET_S, OUTBOX_LIST_PREFIX,
};
use crate::{
    admin::{FosRequest, Record},
    ingest::hour_dir,
    shell::{cache_ops_remaining, Runtime},
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

#[cfg(test)]
mod tests;

pub const WALL_STOP: Duration = Duration::from_secs(40);
pub const LEASE_SECONDS: u64 = 120;
/// Minutes a Core Cache bucket can live (24 h TTL).
pub const EXPIRY_MIN: u64 = 1440;
/// Closed outbox batch keys waiting before a flush notes `logq_depth_high`: two hours of
/// 2-minute buckets. A quiet service lets about an hour (the default `flush_max_age_s`) build
/// up before the flush is due, so only twice that means the flush is not keeping up.
pub const OUTBOX_WARN_DEPTH: usize = 60;
/// Newest `audit:<seq>` keys kept regardless of age (D10).
pub const AUDIT_KEEP: usize = 50;
pub const SVC_OUTBOX_KEEP: usize = 50;
const FIXED_CALLS: u32 = 16;
/// Cursor, manifest and outbox-list reads spent before a due flush is admitted.
const PRECHECK_CALLS: u32 = 3;
/// Calls a due flush needs once admitted.
pub const FLUSH_MIN_CALLS: u32 = FIXED_CALLS + 1;
/// Calls a due flush needs from its start, pre-check reads included.
pub const FLUSH_TICK_CALLS: u32 = PRECHECK_CALLS + FLUSH_MIN_CALLS;
const MAX_GROUPS: usize = 6;
/// Calls a committed transaction spends after `collect`, besides one PUT per file group, one
/// delete per outbox key and the conditional [`SVC_TRIM_CALLS`] and [`PURGE_CALLS`]: the
/// manifest write, the cursor CAS, the GC manifest read and write (which also records the
/// commit), and the lease release.
const POST_COLLECT_CALLS: u32 = 5;
/// The svc audit outbox trim's read and CAS, spent only when the transaction archived svc rows.
const SVC_TRIM_CALLS: u32 = 2;
/// The post-commit purge's cursor read and CAS, spent only when a purge is ready.
const PURGE_CALLS: u32 = 2;
/// Bucket bodies a scan keeps in memory for `collect`, so a bucket found and archived by the
/// same flush costs one lookup, not two.
const SCAN_KEEP_BYTES: usize = 16 << 20;
/// Core Cache operations a flush leaves for its caller: up to four telemetry bucket replaces,
/// the status-cache purge and the schedule release, plus two of headroom so an execution stays
/// within the 56-op soft budget the harness and Tick & Budget Health flag (hard limit 64).
pub const FLUSH_CACHE_RESERVE: u32 = 8;
/// Bucket reads a scan leaves for `collect` when nothing looks due yet.
const COLLECT_MIN_OPS: u32 = 8;
/// Bucket reads a scan leaves for `collect` when the pending rows already look due.
const COLLECT_DUE_OPS: u32 = 32;
/// Rows held in memory by one flush; later sources stay pending for the next one.
const MAX_COLLECT_ROWS: usize = 5_000;
const MAX_OUTBOX_BATCH: usize = 8;
pub const FLUSH_VCPU_SOFT_MS: u64 = 700;
/// Cursor entries held in KV; scanning pauses at the cap (cache TTL still bounds data loss and
/// expiry is counted) so a stalled flush cannot grow the cursor value without bound.
const MAX_PENDING_ENTRIES: usize = 1_000;
const KV_PAGE: u32 = 1000;
const MAX_SVC_ENTRIES: u64 = 200;
const FOS_PUT_TIMEOUT: Duration = Duration::from_secs(30);
const CURSOR_PREFIX: &str = "logs:cursor:";
const MANIFEST_PREFIX: &str = "logs:flush_txn:";
const LEASE_KEY: &str = "logs:flush_lease";
const CACHE_KINDS: [LogKind; 3] = [
    LogKind::FosUsage,
    LogKind::ApiTelemetry,
    LogKind::PlatformErrors,
];

/// `(kind, epoch_min, shard, rows, sid)`. The first three elements are what `read_live` reads.
pub type Entry = (String, u64, u32, u64, String);

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Pending {
    pub rows: u64,
    pub oldest_min: Option<u64>,
    pub nonempty: Vec<Entry>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Cursor {
    pub scan_min: u64,
    pub cache_min: u64,
    pub purged_hour: u64,
    pub pending: Pending,
    pub audit_seq: Option<u64>,
    pub svc_outbox_archived: u64,
    pub last_flush_s: u64,
    pub expired: u64,
    pub dropped: u64,
    pub global_end: u64,
    /// Last committed flush transaction; a manifest is committed only when this matches its id.
    pub txn_id: String,
    /// `(kind, epoch_hour, sid)` flushed hours whose Core Cache surrogate key is not yet purged.
    pub purge_due: Vec<(String, u64, String)>,
    /// The last flush left sources it could not read within its call, cache-op or size bounds;
    /// the next one is due without waiting for `flush_min_interval_s`.
    pub behind: bool,
}

impl Pending {
    fn recompute(&mut self) {
        self.rows = self.nonempty.iter().map(|entry| entry.3).sum();
        self.oldest_min = self.nonempty.iter().map(|entry| entry.1).min();
    }
}

impl Cursor {
    fn advance_cache_min(&mut self) {
        self.pending.recompute();
        let bound = self
            .pending
            .oldest_min
            .map_or(self.scan_min, |oldest| oldest.saturating_sub(1));
        self.cache_min = self.cache_min.max(bound);
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
struct Notes {
    expired: u64,
    dropped: u64,
    outbox_depth: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
struct Manifest {
    txn_id: String,
    cursor_generation: Option<String>,
    start_min: u64,
    buckets: Vec<Entry>,
    outbox_keys: Vec<String>,
    auth_keys: Vec<String>,
    svc_range: (u64, u64),
    notes: Notes,
    outputs: Vec<String>,
    next_cursor: Cursor,
    created_at: u64,
    committed: bool,
    gc_next: usize,
    gc_done: bool,
    /// Only manifests built before `cron_ticks` retired carry its rows; new ones leave this
    /// false, so resuming an old one rebuilds the same files.
    keep_idle: bool,
    note_ms: i64,
}

pub struct FlushTarget<'a> {
    pub region: &'a str,
    pub bucket: &'a str,
    pub access_key: &'a str,
    pub secret_key: &'a str,
    /// `{fos_prefix}processed/ops/`
    pub ops_prefix: &'a str,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FlushStatus {
    #[default]
    Unprovisioned,
    NotDue,
    /// Another executor holds the lease.
    Busy,
    /// Not enough backend calls left for a bounded transaction.
    Deferred,
    /// Wall-time limit reached; the manifest stays uncommitted for the next tick.
    Stopped,
    /// A committed transaction's cleanup could not finish.
    GcPending,
    Committed,
}

#[derive(Debug, Default, PartialEq)]
pub struct FlushReport {
    pub status: FlushStatus,
    pub lookups: u64,
    pub scanned_minutes: u64,
    pub rows: u64,
    pub files: u64,
    pub bytes_written: u64,
    pub purged: u32,
    pub outbox_depth: u64,
    pub oldest_pending_s: Option<u64>,
    /// A committed flush that left sources unread: the next one is due at once.
    pub behind: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ClearError {
    Busy,
    ScanBehind,
    Failed(String),
}

fn cursor_key(sid: &str) -> String {
    format!("{CURSOR_PREFIX}{sid}")
}

fn manifest_key(sid: &str) -> String {
    format!("{MANIFEST_PREFIX}{sid}")
}

fn parse<T: for<'de> Deserialize<'de> + Default>(record: &Option<Record>) -> T {
    record
        .as_ref()
        .and_then(|record| serde_json::from_value(record.value.clone()).ok())
        .unwrap_or_default()
}

fn to_value<T: Serialize>(value: &T) -> Value {
    serde_json::to_value(value).unwrap_or(Value::Null)
}

fn over_wall_time(runtime: &impl Runtime, started: Instant) -> bool {
    runtime.admin_elapsed(started) >= WALL_STOP
}

fn list_keys(
    runtime: &impl Runtime,
    prefix: &str,
    max_pages: usize,
) -> Result<(Vec<String>, bool), String> {
    let mut keys = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..max_pages {
        let page = runtime.kv_list(prefix, cursor.as_deref(), KV_PAGE)?;
        keys.extend(page.keys);
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => return Ok((keys, false)),
        }
    }
    Ok((keys, true))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScanStop {
    /// Reached the last closed minute or the `MAX_SCAN_MIN` bound.
    Done,
    /// The next whole minute would exceed the Core Cache operation budget.
    Budget,
    PendingCap,
    Error,
}

/// Bucket bodies read by this execution's scan, by bucket key.
#[derive(Default)]
struct Kept {
    bodies: BTreeMap<String, Vec<u8>>,
    bytes: usize,
}

struct Scanned {
    lookups: u64,
    minutes: u64,
    stop: ScanStop,
}

/// Scans whole closed minutes `(scan_min, now_min − 2]` for `kinds`, at most `MAX_SCAN_MIN` of
/// them and at most `budget` lookups. Minutes older than the cache TTL are counted as expired.
/// Every stop persists only fully scanned minutes, so the next run resumes after them.
fn scan(
    runtime: &impl Runtime,
    sid: &str,
    cursor: &mut Cursor,
    kinds: &[LogKind],
    budget: u32,
    kept: &mut Kept,
) -> Scanned {
    let now_min = runtime.now() / 60;
    let closed_end = now_min.saturating_sub(2);
    if cursor.scan_min >= closed_end {
        return Scanned {
            lookups: 0,
            minutes: 0,
            stop: ScanStop::Done,
        };
    }
    let mut first = cursor.scan_min + 1;
    let floor = now_min.saturating_sub(EXPIRY_MIN);
    if first < floor {
        cursor.expired += floor - first;
        first = floor;
    }
    let last = closed_end.min(first + MAX_SCAN_MIN - 1);
    let vcpu_start = runtime.vcpu_ms();
    let mut lookups = 0;
    let stopped = |cursor: &mut Cursor, minute: u64, lookups: u64, stop: ScanStop| {
        cursor.scan_min = minute - 1;
        cursor.advance_cache_min();
        Scanned {
            lookups,
            minutes: minute - first,
            stop,
        }
    };
    for minute in first..=last {
        if cursor.pending.nonempty.len() >= MAX_PENDING_ENTRIES {
            return stopped(cursor, minute, lookups, ScanStop::PendingCap);
        }
        let mut sids = vec![sid];
        if minute <= cursor.global_end && sid != "global" {
            sids.push("global");
        }
        let cost = (sids.len() * kinds.len()) as u64 * u64::from(LOG_SHARDS);
        if lookups + cost > u64::from(budget)
            || (!kept.bodies.is_empty()
                && runtime.vcpu_ms().saturating_sub(vcpu_start) >= FLUSH_VCPU_SOFT_MS)
        {
            return stopped(cursor, minute, lookups, ScanStop::Budget);
        }
        let before = cursor.pending.nonempty.len();
        for bucket_sid in sids {
            for kind in kinds {
                for shard in 0..LOG_SHARDS {
                    lookups += 1;
                    let key = bucket_key(*kind, bucket_sid, minute, shard);
                    match runtime.core_cache_lookup(&key) {
                        Ok(Some((body, meta))) if !body.is_empty() => {
                            let meta: Value = serde_json::from_slice(&meta).unwrap_or(Value::Null);
                            if kept.bytes + body.len() <= SCAN_KEEP_BYTES {
                                kept.bytes += body.len();
                                kept.bodies.insert(key.clone(), body.clone());
                            }
                            let count = meta["count"]
                                .as_u64()
                                .unwrap_or_else(|| body.split(|b| *b == b'\n').count() as u64);
                            cursor.dropped += meta["dropped"].as_u64().unwrap_or(0);
                            cursor.pending.nonempty.push((
                                kind.as_str().into(),
                                minute,
                                shard,
                                count,
                                bucket_sid.into(),
                            ));
                        }
                        Ok(_) => {}
                        Err(_) => {
                            // Entries found earlier in this partial minute are rescanned later.
                            cursor.pending.nonempty.truncate(before);
                            return stopped(cursor, minute, lookups, ScanStop::Error);
                        }
                    }
                }
            }
        }
    }
    cursor.scan_min = last;
    cursor.advance_cache_min();
    Scanned {
        lookups,
        minutes: last - first + 1,
        stop: ScanStop::Done,
    }
}

/// The bucket of an outbox key the flush may take: one closed at least a whole bucket ago.
/// KV has no conditional delete, so a writer that read a key before the flush did must have
/// finished its CAS before the flush reads it. Writers recompute the bucket per attempt and an
/// execution lasts at most 60 s, so a key untouched for a full 120 s bucket is final, which also
/// covers KV's cross-POP propagation delay.
fn closed_bucket(key: &str, sid: &str, now: u64) -> Option<u64> {
    outbox_bucket(key, sid).filter(|bucket| bucket + 1 < now / OUTBOX_BUCKET_S)
}

/// Closed outbox keys from a sorted listing, excluding `skip`.
fn closed_outbox(keys: Vec<String>, sid: &str, now: u64, skip: &BTreeSet<String>) -> Vec<String> {
    keys.into_iter()
        .filter(|key| closed_bucket(key, sid, now).is_some() && !skip.contains(key))
        .collect()
}

struct Due {
    due: bool,
    oldest_s: Option<u64>,
}

fn check_due(
    cursor: &Cursor,
    outbox: &[String],
    sid: &str,
    now: u64,
    settings: &OperationalLogSettings,
    manual: bool,
) -> Due {
    let rows = cursor.pending.rows + outbox.len() as u64;
    let oldest_s = cursor
        .pending
        .oldest_min
        .map(|minute| minute * 60)
        .into_iter()
        .chain(
            outbox
                .iter()
                .filter_map(|key| outbox_bucket(key, sid).map(|b| b * OUTBOX_BUCKET_S)),
        )
        .min();
    if rows == 0 && !cursor.behind {
        return Due {
            due: false,
            oldest_s,
        };
    }
    if manual {
        return Due {
            due: true,
            oldest_s,
        };
    }
    // A flush that could not read everything in one execution must not wait out the floor,
    // or a steady production rate outruns one transaction per interval.
    let floor_passed = cursor.behind
        || now.saturating_sub(cursor.last_flush_s) >= settings.flush_min_interval_s.into();
    let aged = oldest_s
        .is_some_and(|oldest| now.saturating_sub(oldest) >= u64::from(settings.flush_max_age_s));
    Due {
        due: floor_passed && (cursor.behind || rows >= settings.flush_max_rows.into() || aged),
        oldest_s,
    }
}

#[derive(Clone, Debug)]
enum SourceId {
    Bucket(Entry),
    Outbox(String),
    Auth(String),
    Svc(u64, u64),
}

type LogRow = (LogKind, Value);

fn read_source(runtime: &impl Runtime, sid: &str, id: &SourceId) -> Result<Vec<LogRow>, String> {
    read_source_with(runtime, sid, id, &Kept::default(), None)
}

/// Reads one source, taking bucket bodies this execution's scan kept and the `svc:` record
/// `collect` already read instead of fetching them again.
fn read_source_with(
    runtime: &impl Runtime,
    sid: &str,
    id: &SourceId,
    kept: &Kept,
    svc: Option<&Value>,
) -> Result<Vec<LogRow>, String> {
    let mut rows = Vec::new();
    match id {
        SourceId::Bucket((kind, minute, shard, _, bucket_sid)) => {
            let Some(kind) = LogKind::parse(kind) else {
                return Ok(rows);
            };
            let key = bucket_key(kind, bucket_sid, *minute, *shard);
            let entry = match kept.bodies.get(&key) {
                Some(body) => Some(body.clone()),
                None => runtime
                    .core_cache_lookup(&key)
                    .map_err(|_| "core_cache".to_string())?
                    .map(|(body, _)| body),
            };
            if let Some(body) = entry {
                for line in String::from_utf8_lossy(&body).lines() {
                    if let Ok(row @ Value::Object(_)) = serde_json::from_str::<Value>(line) {
                        rows.push((kind, row));
                    }
                }
            }
        }
        SourceId::Outbox(key) => {
            if let Some(record) = runtime.admin_get(key)? {
                for line in record.value["ndjson"].as_str().unwrap_or_default().lines() {
                    let Ok(Value::Object(mut row)) = serde_json::from_str::<Value>(line) else {
                        continue;
                    };
                    let kind = row
                        .remove("kind")
                        .and_then(|k| k.as_str().and_then(LogKind::parse));
                    if let Some(kind) = kind {
                        rows.push((kind, Value::Object(row)));
                    }
                }
            }
        }
        SourceId::Auth(key) => {
            let seq = auth_seq(key);
            if let (Some(seq), Some(record)) = (seq, runtime.admin_get(key)?) {
                rows.push((LogKind::AdminAudit, auth_audit_row(sid, seq, &record.value)));
            }
        }
        SourceId::Svc(from, to) => {
            let fetched = match svc {
                Some(_) => None,
                None => runtime
                    .admin_get(&format!("svc:{sid}"))?
                    .map(|record| record.value),
            };
            if let Some(value) = svc.or(fetched.as_ref()) {
                let base = value["_audit_outbox_base"].as_u64().unwrap_or(0);
                for index in *from..*to {
                    let entry = index
                        .checked_sub(base)
                        .and_then(|i| value["_audit_outbox"].get(i as usize));
                    if let Some(entry) = entry {
                        rows.push((LogKind::AdminAudit, svc_audit_row(sid, index, entry)));
                    }
                }
            }
        }
    }
    Ok(rows)
}

fn auth_seq(key: &str) -> Option<u64> {
    key.strip_prefix("audit:")?.parse().ok()
}

fn row_ts_ms(row: &Value) -> i64 {
    row["timestamp"].as_i64().unwrap_or(0)
}

fn group_of(kind: LogKind, row: &Value) -> (LogKind, u64) {
    (kind, (row_ts_ms(row).max(0) / 3_600_000) as u64)
}

/// Collapses runs of consecutive idle `cron_ticks` rows inside one hour into one row.
fn rollup_idle_ticks(rows: Vec<Value>) -> Vec<Value> {
    let idle = |row: &Value| {
        row["outcome"].as_str() == Some("idle") && row["trigger"].as_str() != Some("idle_rollup")
    };
    let mut out = Vec::new();
    let mut run: Vec<Value> = Vec::new();
    let flush_run = |run: &mut Vec<Value>, out: &mut Vec<Value>| {
        if run.len() < 2 {
            out.append(run);
            return;
        }
        let ids: Vec<&str> = run
            .iter()
            .map(|row| row["event_id"].as_str().unwrap_or_default())
            .collect();
        let digest = Sha256::digest(ids.join(",").as_bytes());
        let sum = |field: &str| {
            run.iter()
                .map(|r| r[field].as_i64().unwrap_or(0))
                .sum::<i64>()
        };
        let ticks: i64 = run
            .iter()
            .map(|row| row["ticks_count"].as_i64().unwrap_or(1))
            .sum();
        let mut merged = run[0].clone();
        merged["event_id"] = hex::encode(&digest[..16]).into();
        merged["trigger"] = "idle_rollup".into();
        merged["outcome"] = "idle".into();
        merged["ticks_count"] = ticks.into();
        for field in ["duration_ms", "vcpu_ms", "backend_requests_used"] {
            merged[field] = sum(field).into();
        }
        out.push(merged);
        run.clear();
    };
    for row in rows {
        let same_hour = run
            .first()
            .is_none_or(|first| row_ts_ms(first) / 3_600_000 == row_ts_ms(&row) / 3_600_000);
        if idle(&row) && same_hour {
            run.push(row);
        } else {
            flush_run(&mut run, &mut out);
            if idle(&row) {
                run.push(row);
            } else {
                out.push(row);
            }
        }
    }
    flush_run(&mut run, &mut out);
    out
}

fn note_rows(sid: &str, manifest: &Manifest) -> Vec<LogRow> {
    let notes = &manifest.notes;
    let now_ms = manifest.note_ms;
    let mut rows = Vec::new();
    for (code, count, message) in [
        (
            "log_scan_expired",
            notes.expired,
            format!(
                "{} minute(s) expired before they could be scanned",
                notes.expired
            ),
        ),
        (
            "log_buffer_full",
            notes.dropped,
            format!("{} row(s) dropped by full buckets", notes.dropped),
        ),
        (
            "logq_depth_high",
            if notes.outbox_depth as usize > OUTBOX_WARN_DEPTH {
                notes.outbox_depth
            } else {
                0
            },
            format!("{} durable outbox keys are waiting", notes.outbox_depth),
        ),
    ] {
        if count > 0 {
            rows.push((
                LogKind::PlatformErrors,
                json!({
                    "timestamp": now_ms,
                    "event_id": audit_event_id(&format!("flush_note:{code}:{sid}"), manifest.start_min),
                    "service_id": sid, "severity": "warn", "error_domain": "telemetry",
                    "code": code, "caller": "cron:log_flush", "message": message,
                }),
            ));
        }
    }
    rows
}

#[cfg(test)]
thread_local! {
    static BUILD_FILES_CALLS: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// Groups rows into one file per (kind, hour). Returns `(key, kind, rows)` in key order.
fn build_files(
    ops_prefix: &str,
    start_min: u64,
    txn_id: &str,
    keep_idle: bool,
    rows: Vec<LogRow>,
) -> Vec<(String, LogKind, Vec<Value>)> {
    #[cfg(test)]
    BUILD_FILES_CALLS.with(|calls| calls.set(calls.get() + 1));
    let mut by_kind: BTreeMap<LogKind, Vec<Value>> = BTreeMap::new();
    for (kind, row) in rows {
        by_kind.entry(kind).or_default().push(row);
    }
    let mut groups: BTreeMap<(LogKind, u64), Vec<Value>> = BTreeMap::new();
    for (kind, mut rows) in by_kind {
        rows.sort_by_key(|row| {
            (
                row_ts_ms(row),
                row["event_id"].as_str().unwrap_or_default().to_string(),
            )
        });
        let mut seen = BTreeSet::new();
        rows.retain(|row| seen.insert(row["event_id"].as_str().unwrap_or_default().to_string()));
        if kind == LogKind::CronTicks && !keep_idle {
            rows = rollup_idle_ticks(rows);
        }
        for row in rows {
            groups.entry(group_of(kind, &row)).or_default().push(row);
        }
    }
    let short = &txn_id[..txn_id.len().min(16)];
    groups
        .into_iter()
        .map(|((kind, hour), rows)| {
            let minutes = rows.iter().map(|row| row_ts_ms(row).max(0) as u64 / 60_000);
            let end = minutes.clone().max().unwrap_or(start_min);
            let start = minutes.min().unwrap_or(start_min);
            let key = format!(
                "{ops_prefix}{}/{}window={start}-{end}-{short}-n{}.parquet",
                kind.as_str(),
                hour_dir((hour * 3600) as i64),
                rows.len()
            );
            (key, kind, rows)
        })
        .collect()
}

fn group_count(rows: &[LogRow]) -> BTreeSet<(LogKind, u64)> {
    rows.iter()
        .map(|(kind, row)| group_of(*kind, row))
        .collect()
}

struct Lease<'a, R: Runtime> {
    runtime: &'a R,
}

impl<R: Runtime> Lease<'_, R> {
    fn release(self) {
        let _ = self.runtime.kv_delete(LEASE_KEY);
    }
}

fn acquire_lease<'a, R: Runtime>(
    runtime: &'a R,
    txn_id: Option<&str>,
) -> Result<Option<Lease<'a, R>>, String> {
    let now = runtime.now();
    let value = json!({"txn_id": txn_id, "expires_at": now + LEASE_SECONDS});
    let current = runtime.admin_get(LEASE_KEY)?;
    let generation = match &current {
        None => None,
        Some(record) if record.value["expires_at"].as_u64().unwrap_or(0) <= now => {
            Some(record.generation.as_str())
        }
        Some(_) => return Ok(None),
    };
    Ok(runtime
        .admin_cas(LEASE_KEY, generation, &value)?
        .then_some(Lease { runtime }))
}

fn put_file(
    runtime: &impl Runtime,
    target: &FlushTarget<'_>,
    key: &str,
    body: Vec<u8>,
) -> Result<(), String> {
    let response = runtime.admin_fos(FosRequest {
        method: "PUT",
        region: target.region,
        bucket: target.bucket,
        key,
        query: "",
        access_key: target.access_key,
        secret_key: target.secret_key,
        body,
        timeout: FOS_PUT_TIMEOUT,
        response_limit: crate::admin::transport::FOS_RESPONSE_LIMIT,
        content_md5: None,
    })?;
    if (200..300).contains(&response.status) {
        Ok(())
    } else {
        Err(format!("object_put_failed:{}", response.status))
    }
}

fn save_manifest(runtime: &impl Runtime, sid: &str, manifest: &Manifest) -> Result<(), String> {
    let key = manifest_key(sid);
    let generation = runtime.admin_get(&key)?.map(|record| record.generation);
    if runtime.admin_cas(&key, generation.as_deref(), &to_value(manifest))? {
        Ok(())
    } else {
        Err("manifest_conflict".into())
    }
}

/// Removes flushed entries and records the purge obligations for the flushed hours.
fn build_next_cursor(cursor: &Cursor, manifest: &Manifest, now: u64) -> Cursor {
    let mut next = cursor.clone();
    let flushed: BTreeSet<&Entry> = manifest.buckets.iter().collect();
    next.pending
        .nonempty
        .retain(|entry| !flushed.contains(entry));
    next.pending.recompute();
    next.cache_min = match next.pending.oldest_min {
        Some(oldest) => (oldest - 1).max(cursor.cache_min),
        None => next.scan_min.max(cursor.cache_min),
    };
    let mut due: BTreeSet<(String, u64, String)> = next.purge_due.iter().cloned().collect();
    for (kind, minute, _, _, bucket_sid) in &manifest.buckets {
        due.insert((kind.clone(), minute / 60, bucket_sid.clone()));
    }
    next.purge_due = due.into_iter().collect();
    if let Some(max_seq) = manifest.auth_keys.iter().filter_map(|k| auth_seq(k)).max() {
        next.audit_seq = Some(max_seq.max(next.audit_seq.unwrap_or(0)));
    }
    next.svc_outbox_archived = next.svc_outbox_archived.max(manifest.svc_range.1);
    next.txn_id = manifest.txn_id.clone();
    next.last_flush_s = now;
    next.expired = 0;
    next.dropped = 0;
    next
}

fn manifest_sources(manifest: &Manifest) -> Vec<SourceId> {
    let mut sources: Vec<SourceId> = manifest
        .buckets
        .iter()
        .cloned()
        .map(SourceId::Bucket)
        .collect();
    sources.extend(manifest.outbox_keys.iter().cloned().map(SourceId::Outbox));
    sources.extend(manifest.auth_keys.iter().cloned().map(SourceId::Auth));
    if manifest.svc_range.0 < manifest.svc_range.1 {
        sources.push(SourceId::Svc(manifest.svc_range.0, manifest.svc_range.1));
    }
    sources
}

fn read_manifest_rows(
    runtime: &impl Runtime,
    sid: &str,
    manifest: &Manifest,
) -> Result<Vec<LogRow>, String> {
    let mut rows = note_rows(sid, manifest);
    for source in manifest_sources(manifest) {
        rows.extend(read_source(runtime, sid, &source)?);
    }
    Ok(rows)
}

struct Collected {
    manifest: Manifest,
    rows: Vec<LogRow>,
    /// Some source was left for a later flush.
    left: bool,
}

/// Source classes `collect` takes turns between, so a large backlog in one cannot starve the
/// others of reads: Core Cache buckets, outbox batches, `audit:` keys, the svc audit outbox.
const CLASSES: usize = 4;

/// Reads sources round-robin by class until the call budget (keeping `reserve` plus what the
/// rest of the transaction needs), the Core Cache budget or the file-group and row bounds stop
/// it; unread sources stay for a later flush. Buckets and outbox keys are independent, so one
/// that does not fit is skipped; audit keys and the svc outbox are archived as contiguous
/// ranges, so the first that does not fit ends its class. `now` is the flush's own clock
/// reading, so the note rows land in the file groups budgeted here.
#[allow(clippy::too_many_arguments)]
fn collect(
    runtime: &impl Runtime,
    sid: &str,
    cursor: &Cursor,
    generation: Option<String>,
    skip: &BTreeSet<String>,
    reserve: u32,
    kept: &Kept,
    listed: Option<Vec<String>>,
    now: u64,
) -> Result<Collected, String> {
    let mut buckets = cursor.pending.nonempty.clone();
    buckets.sort_by_key(|e| (e.1, e.0.clone(), e.2));
    let (outbox, truncated) = match listed {
        Some(keys) => (keys, false),
        None => list_keys(runtime, OUTBOX_LIST_PREFIX, 11)?,
    };
    let outbox = closed_outbox(outbox, sid, now, skip);
    let outbox_depth = outbox.len() as u64 + u64::from(truncated);
    let (auth, _) = list_keys(runtime, "audit:", 11)?;
    let mut auth: Vec<(u64, String)> = auth
        .into_iter()
        .filter_map(|key| auth_seq(&key).map(|seq| (seq, key)))
        .filter(|(seq, _)| cursor.audit_seq.is_none_or(|archived| *seq > archived))
        .collect();
    auth.sort();
    let purge = if purge_pending(cursor) {
        PURGE_CALLS
    } else {
        0
    };
    // Calls the rest of the transaction needs, plus the caller's reserve.
    let after = |groups: usize, deletes: usize, svc: bool| {
        reserve
            + POST_COLLECT_CALLS
            + purge
            + if svc { SVC_TRIM_CALLS } else { 0 }
            + (groups + deletes) as u32
    };
    let spare = |groups: usize, deletes: usize, svc: bool| {
        runtime
            .admin_remaining_calls()
            .saturating_sub(after(groups, deletes, svc))
    };
    let mut svc = None;
    let mut queues: [std::collections::VecDeque<SourceId>; CLASSES] = Default::default();
    queues[0].extend(buckets.into_iter().map(SourceId::Bucket));
    queues[1].extend(outbox.into_iter().map(SourceId::Outbox));
    queues[2].extend(auth.into_iter().map(|(_, key)| SourceId::Auth(key)));
    if spare(1, 0, true) > 1 {
        if let Some(record) = runtime.admin_get(&format!("svc:{sid}"))? {
            let base = record.value["_audit_outbox_base"].as_u64().unwrap_or(0);
            let entries = record.value["_audit_outbox"].as_array();
            let len = entries.map_or(0, Vec::len) as u64;
            let end = (base + len).min(cursor.svc_outbox_archived.max(base) + MAX_SVC_ENTRIES);
            let mut seg_start = cursor.svc_outbox_archived.max(base);
            while seg_start < end {
                let hour_of = |idx: u64| -> u64 {
                    entries
                        .and_then(|arr| arr.get((idx - base) as usize))
                        .and_then(|e| e["at"].as_u64())
                        .unwrap_or(now)
                        / 3600
                };
                let seg_hour = hour_of(seg_start);
                let mut seg_end = seg_start + 1;
                while seg_end < end && hour_of(seg_end) == seg_hour {
                    seg_end += 1;
                }
                queues[3].push_back(SourceId::Svc(seg_start, seg_end));
                seg_start = seg_end;
            }
            svc = Some(record.value);
        }
    }

    let mut manifest = Manifest {
        cursor_generation: generation,
        start_min: now / 60,
        created_at: now,
        notes: Notes {
            expired: cursor.expired,
            dropped: cursor.dropped,
            outbox_depth,
        },
        note_ms: (now * 1000) as i64,
        ..Default::default()
    };
    let mut rows = Vec::new();
    // The note rows' file is written with the sources', so it is budgeted from the start.
    let mut groups = group_count(&note_rows(sid, &manifest));
    let mut left = false;
    let mut included = 0usize;
    let mut unkept_lookups = 0u32;
    let vcpu_start = runtime.vcpu_ms();
    let mut open = [true; CLASSES];
    while open.iter().any(|o| *o) {
        for class in 0..CLASSES {
            if !open[class] {
                continue;
            }
            if included > 0
                && included.is_multiple_of(8)
                && runtime.vcpu_ms().saturating_sub(vcpu_start) >= FLUSH_VCPU_SOFT_MS
            {
                open = [false; CLASSES];
                left = true;
                break;
            }
            let Some(source) = queues[class].pop_front() else {
                open[class] = false;
                continue;
            };
            let deletes = manifest.outbox_keys.len();
            let has_svc = manifest.svc_range.1 > 0 || matches!(source, SourceId::Svc(..));
            let in_kept = match &source {
                SourceId::Bucket(entry) => LogKind::parse(&entry.0)
                    .map(|kind| bucket_key(kind, &entry.4, entry.1, entry.2))
                    .is_some_and(|key| kept.bodies.contains_key(&key)),
                _ => false,
            };
            let fits = match &source {
                SourceId::Bucket(_) => {
                    in_kept
                        || (cache_ops_remaining(runtime) > FLUSH_CACHE_RESERVE
                            && unkept_lookups < COLLECT_DUE_OPS
                            && (cache_ops_remaining(runtime) > 16 || unkept_lookups == 0))
                }
                // The read and the key's later delete; a new group is checked once read.
                SourceId::Outbox(_) => {
                    deletes < MAX_OUTBOX_BATCH && spare(groups.len(), deletes, has_svc) >= 2
                }
                SourceId::Svc(..) => spare(groups.len(), deletes, has_svc) >= 1,
                SourceId::Auth(_) => spare(groups.len(), deletes, has_svc) >= 2,
            };
            let mut take = fits;
            let mut source_rows = Vec::new();
            if take {
                if matches!(source, SourceId::Bucket(_)) && !in_kept {
                    unkept_lookups += 1;
                }
                source_rows = read_source_with(runtime, sid, &source, kept, svc.as_ref())?;
                let mut next = groups.clone();
                next.extend(group_count(&source_rows));
                let extra = usize::from(matches!(source, SourceId::Outbox(_)));
                take = included == 0
                    || (next.len() <= MAX_GROUPS
                        && rows.len() + source_rows.len() <= MAX_COLLECT_ROWS
                        && runtime.admin_remaining_calls()
                            >= after(next.len(), deletes + extra, has_svc));
                if take {
                    groups = next;
                }
            }
            if !take {
                left = true;
                if matches!(source, SourceId::Auth(_) | SourceId::Svc(..))
                    || !matches!(source, SourceId::Bucket(_)) && !fits
                {
                    open[class] = false;
                }
                continue;
            }
            included += 1;
            rows.extend(source_rows);
            match source {
                SourceId::Bucket(entry) => manifest.buckets.push(entry),
                SourceId::Outbox(key) => manifest.outbox_keys.push(key),
                SourceId::Auth(key) => manifest.auth_keys.push(key),
                SourceId::Svc(from, to) => {
                    if manifest.svc_range.0 == manifest.svc_range.1 {
                        manifest.svc_range = (from, to);
                    } else {
                        manifest.svc_range.1 = to;
                    }
                }
            }
        }
    }
    let svc_end = svc.as_ref().map_or(0, |value| {
        value["_audit_outbox_base"].as_u64().unwrap_or(0)
            + value["_audit_outbox"].as_array().map_or(0, Vec::len) as u64
    });
    left |= truncated || manifest.svc_range.1.max(cursor.svc_outbox_archived) < svc_end;
    Ok(Collected {
        manifest,
        rows,
        left,
    })
}

fn txn_id(sid: &str, manifest: &Manifest) -> String {
    let inputs = json!([
        sid,
        manifest.start_min,
        manifest.buckets,
        manifest.outbox_keys,
        manifest.auth_keys,
        manifest.svc_range,
        manifest.notes,
    ]);
    hex::encode(Sha256::digest(inputs.to_string().as_bytes()))
}

struct PreparedFiles(Vec<(String, LogKind, Vec<Value>)>);

/// Core of a transaction. `prepared` is `None` on resume, when durable sources are re-read.
fn run_txn(
    runtime: &impl Runtime,
    sid: &str,
    target: &FlushTarget<'_>,
    manifest: &mut Manifest,
    prepared: Option<PreparedFiles>,
    started: Instant,
    report: &mut FlushReport,
) -> Result<bool, String> {
    // A fresh transaction (rows in hand) has a new id no cursor can carry yet.
    let already_committed = prepared.is_none()
        && runtime.admin_get(&cursor_key(sid))?.is_some_and(|record| {
            record.value["txn_id"].as_str() == Some(manifest.txn_id.as_str())
        });
    if !already_committed {
        let files = match prepared {
            Some(PreparedFiles(files)) => files,
            None => build_files(
                target.ops_prefix,
                manifest.start_min,
                &manifest.txn_id,
                manifest.keep_idle,
                read_manifest_rows(runtime, sid, manifest)?,
            ),
        };
        for (key, kind, file_rows) in &files {
            if over_wall_time(runtime, started) {
                report.status = FlushStatus::Stopped;
                return Ok(false);
            }
            let encoded = schema::to_parquet(*kind, file_rows)?;
            let encoded_len = encoded.len() as u64;
            put_file(runtime, target, key, encoded)?;
            report.files += 1;
            report.rows += file_rows.len() as u64;
            report.bytes_written += encoded_len;
        }
        if over_wall_time(runtime, started) {
            report.status = FlushStatus::Stopped;
            return Ok(false);
        }
        let committed = runtime.admin_cas(
            &cursor_key(sid),
            manifest.cursor_generation.as_deref(),
            &to_value(&manifest.next_cursor),
        )?;
        if !committed {
            return Err("flush_fenced".into());
        }
    }
    // The cursor carrying the txn id is the commit; the GC's manifest write records it, so a
    // second write here would only spend a call a source delete needs.
    manifest.committed = true;
    Ok(true)
}

fn delete_file(runtime: &impl Runtime, target: &FlushTarget<'_>, key: &str) -> Result<(), String> {
    let response = runtime.admin_fos(FosRequest {
        method: "DELETE",
        region: target.region,
        bucket: target.bucket,
        key,
        query: "",
        access_key: target.access_key,
        secret_key: target.secret_key,
        body: Vec::new(),
        timeout: FOS_PUT_TIMEOUT,
        response_limit: crate::admin::transport::FOS_RESPONSE_LIMIT,
        content_md5: None,
    })?;
    if (200..300).contains(&response.status) || response.status == 404 {
        Ok(())
    } else {
        Err(format!("object_delete_failed:{}", response.status))
    }
}

/// An uncommitted manifest whose cursor generation moved without carrying its txn id can never
/// commit (the CAS is fenced). Drops its orphan outputs and the manifest so the next tick re-plans
/// from the current cursor; sources are untouched, so nothing is lost.
fn abandon_fenced(
    runtime: &impl Runtime,
    sid: &str,
    target: &FlushTarget<'_>,
    manifest: &Manifest,
    cursor_record: &Option<Record>,
) -> Result<bool, String> {
    let moved = cursor_record.as_ref().map(|r| r.generation.as_str())
        != manifest.cursor_generation.as_deref();
    let carries_txn = cursor_record
        .as_ref()
        .is_some_and(|r| r.value["txn_id"].as_str() == Some(manifest.txn_id.as_str()));
    if !moved || carries_txn {
        return Ok(false);
    }
    if runtime.admin_remaining_calls() < manifest.outputs.len() as u32 + 2 {
        return Ok(false);
    }
    for key in &manifest.outputs {
        delete_file(runtime, target, key)?;
    }
    runtime.kv_delete(&manifest_key(sid))?;
    Ok(true)
}

/// Idempotent cleanup driven by a committed manifest. Returns whether everything finished.
/// Every step keeps the purge cursor write, the lease release and the caller's `reserve`.
fn gc(
    runtime: &impl Runtime,
    sid: &str,
    manifest: &mut Manifest,
    reserve: u32,
    newly_committed: bool,
) -> Result<bool, String> {
    let mut progressed = newly_committed;
    // With no svc rows archived (`svc_range.1 == 0`) the trim would remove nothing.
    let trim = if manifest.svc_range.1 > 0 {
        SVC_TRIM_CALLS
    } else {
        0
    };
    // The GC manifest read and write and the lease release stay available.
    let tail = 3 + reserve;
    while manifest.gc_next < manifest.outbox_keys.len() {
        if runtime.admin_remaining_calls() < 1 + trim + tail {
            break;
        }
        runtime.kv_delete(&manifest.outbox_keys[manifest.gc_next])?;
        manifest.gc_next += 1;
        progressed = true;
    }
    let mut done = manifest.gc_next >= manifest.outbox_keys.len();
    if done && trim > 0 {
        done = runtime.admin_remaining_calls() >= trim + tail
            && trim_svc_outbox(runtime, sid, manifest.svc_range.1)?;
    }
    if done {
        manifest.gc_done = true;
        runtime.kv_delete(&manifest_key(sid))?;
    } else if progressed {
        save_manifest(runtime, sid, manifest)?;
    }
    Ok(done)
}

/// Keeps the newest `SVC_OUTBOX_KEEP` entries, never removing one that is not archived.
/// A generation conflict leaves the outbox intact; the next tick retries.
fn trim_svc_outbox(runtime: &impl Runtime, sid: &str, archived: u64) -> Result<bool, String> {
    let key = format!("svc:{sid}");
    let Some(record) = runtime.admin_get(&key)? else {
        return Ok(true);
    };
    let base = record.value["_audit_outbox_base"].as_u64().unwrap_or(0);
    let outbox = record.value["_audit_outbox"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let removable = outbox
        .len()
        .saturating_sub(SVC_OUTBOX_KEEP)
        .min(archived.saturating_sub(base) as usize);
    if removable == 0 {
        return Ok(true);
    }
    let mut value = record.value.clone();
    value["_audit_outbox"] = outbox[removable..].to_vec().into();
    value["_audit_outbox_base"] = (base + removable as u64).into();
    runtime.admin_cas(&key, Some(&record.generation), &value)
}

/// D10: deletes `audit:<seq>` keys that are archived and older than the retention window,
/// always keeping the newest `AUDIT_KEEP`. Returns the number of keys deleted.
fn audit_key_gc(
    runtime: &impl Runtime,
    archived_seq: Option<u64>,
    retention_days: u32,
    reserve: u32,
) -> Result<u32, String> {
    let Some(archived) = archived_seq else {
        return Ok(0);
    };
    if runtime.admin_remaining_calls() < 9 + reserve {
        return Ok(0);
    }
    let (keys, _) = list_keys(runtime, "audit:", 11)?;
    let mut seqs: Vec<(u64, String)> = keys
        .into_iter()
        .filter_map(|key| auth_seq(&key).map(|seq| (seq, key)))
        .collect();
    seqs.sort();
    let candidates = seqs.len().saturating_sub(AUDIT_KEEP);
    let cutoff = runtime
        .now()
        .saturating_sub(u64::from(retention_days) * 86_400);
    let mut deleted = 0;
    for (seq, key) in seqs.into_iter().take(candidates) {
        if seq > archived || runtime.admin_remaining_calls() < 7 + reserve {
            break;
        }
        let Some(record) = runtime.admin_get(&key)? else {
            continue;
        };
        if record.value["time"].as_u64().unwrap_or(u64::MAX) >= cutoff {
            break;
        }
        runtime.kv_delete(&key)?;
        deleted += 1;
    }
    Ok(deleted)
}

/// Purges Core Cache surrogate keys of hours the cursor has fully archived. A failure or the
/// Core Cache budget leaves the entry and `purged_hour` untouched so the next flush retries.
/// Returns the purge count.
fn purge_pending(cursor: &Cursor) -> bool {
    cursor
        .purge_due
        .iter()
        .any(|(_, hour, _)| hour * 60 + 59 <= cursor.cache_min)
}

fn purge_ready(runtime: &impl Runtime, cursor: &mut Cursor) -> u32 {
    let mut purged = 0;
    let mut failed = false;
    let mut remaining = Vec::new();
    let mut max_hour = cursor.purged_hour;
    for entry in std::mem::take(&mut cursor.purge_due) {
        let (kind, hour, bucket_sid) = &entry;
        let ready = hour * 60 + 59 <= cursor.cache_min;
        if !ready || failed {
            remaining.push(entry);
            continue;
        }
        if cache_ops_remaining(runtime) <= FLUSH_CACHE_RESERVE {
            failed = true;
            remaining.push(entry);
            continue;
        }
        let key = format!("flp-log-{kind}-{bucket_sid}-h{hour}");
        match runtime.core_cache_purge(&key) {
            Ok(()) => {
                purged += 1;
                max_hour = max_hour.max(*hour);
            }
            Err(_) => {
                failed = true;
                remaining.push(entry);
            }
        }
    }
    cursor.purge_due = remaining;
    if !failed {
        cursor.purged_hour = max_hour;
    }
    purged
}

fn purge_after_commit(
    runtime: &impl Runtime,
    sid: &str,
    committed: &Cursor,
    reserve: u32,
) -> Result<u32, String> {
    // The committed cursor is what the lease holder just wrote, so it says whether a read is
    // worth spending; the read, the CAS and the lease release must fit.
    if !purge_pending(committed) || runtime.admin_remaining_calls() < PURGE_CALLS + 1 + reserve {
        return Ok(0);
    }
    let record = runtime.admin_get(&cursor_key(sid))?;
    let Some(record) = record else { return Ok(0) };
    let mut cursor: Cursor = serde_json::from_value(record.value.clone()).unwrap_or_default();
    let before = cursor.clone();
    let purged = purge_ready(runtime, &mut cursor);
    if cursor != before {
        runtime.admin_cas(
            &cursor_key(sid),
            Some(&record.generation),
            &to_value(&cursor),
        )?;
    }
    Ok(purged)
}

fn finish_committed(
    runtime: &impl Runtime,
    sid: &str,
    manifest: &mut Manifest,
    settings: &OperationalLogSettings,
    report: &mut FlushReport,
    reserve: u32,
    newly_committed: bool,
) -> Result<(), String> {
    if gc(runtime, sid, manifest, reserve, newly_committed)? {
        let retention = settings
            .retention_days_by_kind
            .get("admin_audit")
            .copied()
            .unwrap_or(365);
        audit_key_gc(runtime, manifest.next_cursor.audit_seq, retention, reserve)?;
        report.purged += purge_after_commit(runtime, sid, &manifest.next_cursor, reserve)?;
        report.status = FlushStatus::Committed;
        report.behind = manifest.next_cursor.behind;
    } else {
        report.status = FlushStatus::GcPending;
    }
    Ok(())
}

/// True when the oldest pending row is at least twice `flush_max_age_s` old (one KV read).
pub fn flush_overdue(runtime: &impl Runtime, sid: &str, settings: &OperationalLogSettings) -> bool {
    let Ok(Some(record)) = runtime.admin_get(&cursor_key(sid)) else {
        return false;
    };
    let cursor: Cursor = serde_json::from_value(record.value).unwrap_or_default();
    cursor.pending.oldest_min.is_some_and(|minute| {
        runtime.now().saturating_sub(minute * 60) >= 2 * u64::from(settings.flush_max_age_s)
    })
}

/// One tick of the flush. Does nothing before FOS is provisioned (`target` is `None`).
/// `manual` flushes every pending kind and ignores the minimum-interval floor (D11).
pub fn log_flush(
    runtime: &impl Runtime,
    sid: &str,
    target: Option<&FlushTarget<'_>>,
    settings: &OperationalLogSettings,
    manual: bool,
    started: Instant,
) -> Result<FlushReport, String> {
    log_flush_reserving(runtime, sid, target, settings, manual, started, 0)
}

/// Like [`log_flush`], leaving `reserve` backend calls for the rest of the tick.
pub fn log_flush_reserving(
    runtime: &impl Runtime,
    sid: &str,
    target: Option<&FlushTarget<'_>>,
    settings: &OperationalLogSettings,
    manual: bool,
    started: Instant,
    reserve: u32,
) -> Result<FlushReport, String> {
    let mut report = FlushReport::default();
    let Some(target) = target else {
        return Ok(report);
    };
    report.status = FlushStatus::NotDue;
    if runtime.admin_remaining_calls() < 4 {
        report.status = FlushStatus::Deferred;
        return Ok(report);
    }
    let now = runtime.now();
    let load_cursor = || -> Result<(Option<Record>, Cursor), String> {
        let record = runtime.admin_get(&cursor_key(sid))?;
        let mut cursor: Cursor = parse(&record);
        if record.is_none() {
            // A first flush looks back only as far as `read_live` shows unflushed rows without a
            // cursor, so nothing visible disappears and the budgeted scan can catch up.
            let closed_end = (now / 60).saturating_sub(2);
            cursor.scan_min = closed_end.saturating_sub(MAX_SCAN_MIN);
            cursor.cache_min = cursor.scan_min;
            cursor.global_end = closed_end;
        }
        Ok((record, cursor))
    };
    let (mut cursor_record, mut cursor) = load_cursor()?;
    let manifest_record = runtime.admin_get(&manifest_key(sid))?;
    let mut manifest: Manifest = parse(&manifest_record);
    // `run_txn` does not write the commit mark separately: a cursor carrying the txn id is it.
    if cursor_record
        .as_ref()
        .is_some_and(|r| r.value["txn_id"].as_str() == Some(manifest.txn_id.as_str()))
    {
        manifest.committed = true;
    }
    let mut skip = BTreeSet::new();

    if manifest_record.is_some() && !manifest.committed {
        let Some(lease) = acquire_lease(runtime, Some(&manifest.txn_id))? else {
            report.status = FlushStatus::Busy;
            return Ok(report);
        };
        match abandon_fenced(runtime, sid, target, &manifest, &cursor_record) {
            Ok(true) => {
                lease.release();
                return Ok(report);
            }
            Ok(false) => {}
            Err(error) => {
                lease.release();
                return Err(error);
            }
        }
        let result = run_txn(
            runtime,
            sid,
            target,
            &mut manifest,
            None,
            started,
            &mut report,
        );
        let finished = match result {
            Ok(true) => finish_committed(
                runtime,
                sid,
                &mut manifest,
                settings,
                &mut report,
                reserve,
                true,
            ),
            Ok(false) => Ok(()),
            Err(error) => Err(error),
        };
        lease.release();
        finished?;
        return Ok(report);
    }
    if manifest_record.is_some() && !manifest.gc_done {
        skip.extend(manifest.outbox_keys.iter().cloned());
        let Some(lease) = acquire_lease(runtime, Some(&manifest.txn_id))? else {
            report.status = FlushStatus::Busy;
            return Ok(report);
        };
        let finished = finish_committed(
            runtime,
            sid,
            &mut manifest,
            settings,
            &mut report,
            reserve,
            false,
        );
        lease.release();
        finished?;
        if report.status == FlushStatus::GcPending {
            return Ok(report);
        }
        if runtime.admin_remaining_calls().saturating_sub(reserve) < FIXED_CALLS + 4 {
            return Ok(report);
        }
        report.status = FlushStatus::NotDue;
        // The GC step may have advanced the cursor (purges), so its generation is stale.
        (cursor_record, cursor) = load_cursor()?;
    }
    let generation = cursor_record
        .as_ref()
        .map(|record| record.generation.clone());
    let fresh = cursor_record.is_none();

    // Buckets the scan finds are kept in memory, so only already-pending ones cost a second
    // lookup; a due flush reserves just those, the scan takes the rest.
    let collect_ops = if check_due(&cursor, &[], sid, now, settings, manual).due {
        (cursor.pending.nonempty.len() as u32).clamp(COLLECT_MIN_OPS, COLLECT_DUE_OPS)
    } else {
        COLLECT_MIN_OPS
    };
    let scan_budget = cache_ops_remaining(runtime)
        .saturating_sub(FLUSH_CACHE_RESERVE)
        .saturating_sub(collect_ops);
    let mut kept = Kept::default();
    let scan_kinds: &[LogKind] = if super::effective_dev_telemetry(runtime) {
        &CACHE_KINDS
    } else {
        &[LogKind::PlatformErrors]
    };
    let scanned = scan(
        runtime,
        sid,
        &mut cursor,
        scan_kinds,
        scan_budget,
        &mut kept,
    );
    report.lookups = scanned.lookups;
    report.scanned_minutes = scanned.minutes;
    let (listed, more) = list_keys(runtime, OUTBOX_LIST_PREFIX, 1)?;
    let outbox_page = closed_outbox(listed.clone(), sid, now, &skip);
    // A complete first page is the whole outbox; `collect` reuses it instead of listing again.
    let listed = (!more).then_some(listed);
    let due = check_due(&cursor, &outbox_page, sid, now, settings, manual);
    report.oldest_pending_s = due.oldest_s;
    report.outbox_depth = outbox_page.len() as u64;
    if !due.due {
        let purged = purge_ready(runtime, &mut cursor);
        report.purged = purged;
        let stored: Cursor = parse(&cursor_record);
        if fresh || cursor != stored {
            if let Some(lease) = acquire_lease(runtime, None)? {
                let open = runtime
                    .admin_get(&manifest_key(sid))?
                    .is_some_and(|record| !parse::<Manifest>(&Some(record)).committed);
                let written = if open {
                    Ok(false)
                } else {
                    runtime.admin_cas(&cursor_key(sid), generation.as_deref(), &to_value(&cursor))
                };
                lease.release();
                written?;
            }
        }
        return Ok(report);
    }

    let avail = runtime.admin_remaining_calls().saturating_sub(reserve);
    if avail < FIXED_CALLS + 1 {
        report.status = FlushStatus::Deferred;
        return Ok(report);
    }
    let Some(lease) = acquire_lease(runtime, None)? else {
        report.status = FlushStatus::Busy;
        return Ok(report);
    };
    let outcome = (|| {
        let current = runtime.admin_get(&cursor_key(sid))?;
        let unchanged = current.as_ref().map(|r| r.generation.as_str()) == generation.as_deref();
        let existing = runtime.admin_get(&manifest_key(sid))?;
        let open = existing
            .as_ref()
            .is_some_and(|record| !parse::<Manifest>(&Some(record.clone())).committed);
        if !unchanged || open {
            report.status = FlushStatus::Busy;
            return Ok(());
        }
        let Collected {
            mut manifest,
            rows,
            left,
        } = collect(
            runtime,
            sid,
            &cursor,
            generation.clone(),
            &skip,
            reserve,
            &kept,
            listed,
            now,
        )?;
        report.outbox_depth = manifest.notes.outbox_depth;
        manifest.txn_id = txn_id(sid, &manifest);
        let mut all_rows = note_rows(sid, &manifest);
        all_rows.extend(rows);
        let prepared = PreparedFiles(build_files(
            target.ops_prefix,
            manifest.start_min,
            &manifest.txn_id,
            manifest.keep_idle,
            all_rows,
        ));
        manifest.outputs = prepared.0.iter().map(|(key, _, _)| key.clone()).collect();
        manifest.next_cursor = build_next_cursor(&cursor, &manifest, now);
        manifest.next_cursor.behind = left;
        // Only the lease holder writes the manifest, so the generation read above still holds.
        let ok = runtime.admin_cas(
            &manifest_key(sid),
            existing.as_ref().map(|record| record.generation.as_str()),
            &to_value(&manifest),
        )?;
        if !ok {
            return Err("manifest_conflict".to_string());
        }
        if run_txn(
            runtime,
            sid,
            target,
            &mut manifest,
            Some(prepared),
            started,
            &mut report,
        )? {
            finish_committed(
                runtime,
                sid,
                &mut manifest,
                settings,
                &mut report,
                reserve,
                true,
            )?;
        }
        Ok(())
    })();
    lease.release();
    outcome?;
    Ok(report)
}

/// Clears unflushed best-effort rows on purpose (audited by the caller). Advances the scan
/// watermark, drops `pending` for the purged kinds, then purges the Core Cache surrogate key.
pub fn clear_cache(
    runtime: &impl Runtime,
    sid: &str,
    kind: Option<LogKind>,
) -> Result<(), ClearError> {
    let failed = |error: String| ClearError::Failed(error);
    let Some(lease) = acquire_lease(runtime, None).map_err(failed)? else {
        return Err(ClearError::Busy);
    };
    let result = clear_cache_leased(runtime, sid, kind);
    lease.release();
    result
}

fn clear_cache_leased(
    runtime: &impl Runtime,
    sid: &str,
    kind: Option<LogKind>,
) -> Result<(), ClearError> {
    let failed = |error: String| ClearError::Failed(error);
    let manifest_record = runtime.admin_get(&manifest_key(sid)).map_err(failed)?;
    if manifest_record.is_some() && !parse::<Manifest>(&manifest_record).committed {
        return Err(ClearError::Busy);
    }
    let record = runtime.admin_get(&cursor_key(sid)).map_err(failed)?;
    let mut cursor: Cursor = parse(&record);
    let closed_end = (runtime.now() / 60).saturating_sub(2);
    match kind {
        Some(kind) => {
            if cursor.scan_min + MAX_SCAN_MIN < closed_end {
                return Err(ClearError::ScanBehind);
            }
            let others: Vec<LogKind> = CACHE_KINDS.into_iter().filter(|k| *k != kind).collect();
            let budget = cache_ops_remaining(runtime).saturating_sub(FLUSH_CACHE_RESERVE);
            let scanned = scan(
                runtime,
                sid,
                &mut cursor,
                &others,
                budget,
                &mut Kept::default(),
            );
            if scanned.stop == ScanStop::Budget {
                return Err(ClearError::ScanBehind);
            }
            if cursor.scan_min < closed_end {
                return Err(ClearError::Failed("scan_incomplete".into()));
            }
            cursor
                .pending
                .nonempty
                .retain(|entry| entry.0 != kind.as_str());
            cursor.purge_due.retain(|entry| entry.0 != kind.as_str());
        }
        None => {
            cursor.pending = Pending::default();
            cursor.purge_due.clear();
        }
    }
    cursor.scan_min = closed_end;
    cursor.pending.recompute();
    cursor.cache_min = match cursor.pending.oldest_min {
        Some(oldest) => (oldest - 1).max(cursor.cache_min),
        None => closed_end.max(cursor.cache_min),
    };
    let committed = runtime
        .admin_cas(
            &cursor_key(sid),
            record.as_ref().map(|r| r.generation.as_str()),
            &to_value(&cursor),
        )
        .map_err(failed)?;
    if !committed {
        return Err(ClearError::Busy);
    }
    let surrogate = kind.map_or("flp-logs-all".to_string(), LogKind::surrogate_key);
    runtime
        .core_cache_purge(&surrogate)
        .map_err(|error| ClearError::Failed(format!("{error:?}")))
}
