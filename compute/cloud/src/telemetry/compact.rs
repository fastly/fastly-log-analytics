//! `log_compact`: merges a closed day's operational window files into `compact=` chunks
//! (design spec 2026-10-30, §4).
//!
//! `compact={a}-{b}-c{nsrc}-n{rows}-{tags}.parquet` sits in the `day=DD/` folder. `tags` is the
//! sorted 8-hex-digit FNV-1a hash of every source window's file name, so the key itself records
//! the exact source set: a compact is valid once it exists, skips exactly those windows whether or
//! not they still exist, and never hides a window outside the set. A chunk is committed with one
//! PUT; only then are its source keys queued in KV (`logs:compact_delete:{kind}:{day}`) and
//! deleted on this or a later tick. Resumability comes from the listing itself: a compact whose
//! listed sources are not all queued is verified and those sources queued again. Names without
//! `tags` (`…-n{rows}.parquet`) keep the older rule: they cover the day's windows whose start is
//! in `[a, b]` and are valid only while all or none of `nsrc` of them are listed.

use super::{schema, LogKind, OperationalLogSettings};
use crate::ingest::{
    retention::delete_keys,
    storage::{Object, Store, CDN_READ_LIMIT},
};
use crate::shell::Runtime;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

pub const CHUNK_SOURCES: usize = 24;
const STATE_KEY: &str = "logs:compact_state";
const QUEUE_PREFIX: &str = "logs:compact_delete:";
const DAY_S: i64 = 86_400;
const CLOSE_GRACE_S: i64 = 2 * 3600;
const LOOKBACK_DAYS: i64 = 30;
/// Compacted days stay under review this long, so a window flushed late is still picked up.
const REVISIT_DAYS: i64 = 2;
const REVISIT_INTERVAL_S: i64 = 6 * 3600;
/// Day LIST, queue read, two source GETs, PUT and queue insert.
const MIN_CHUNK_CALLS: u32 = 6;
const DRAIN_CALLS: u32 = 3;
/// Compressed source bytes one chunk may read. Decoded rows are `serde_json` values, so this and
/// `MAX_CHUNK_ROWS` keep peak heap well under 64 MiB of the 128 MiB Compute limit.
pub const MAX_CHUNK_BYTES: u64 = 2 * 1024 * 1024;
pub const MAX_CHUNK_ROWS: u64 = 10_000;
/// Stop starting work this far into the execution; the Compute limit is 60 s.
pub const WALL_STOP: std::time::Duration = std::time::Duration::from_secs(40);
/// Stop starting another chunk once this much vCPU is spent in the execution.
pub const VCPU_STOP_MS: u64 = 700;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CompactReport {
    pub chunks: u32,
    pub sources: u64,
    pub rows: u64,
    pub deleted: u64,
    pub failed: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub more_pending: bool,
    /// `wall_time` or `vcpu` when a time limit ended the run early.
    pub stopped: Option<&'static str>,
    pub backend_calls: u32,
    pub errors: Vec<String>,
}

/// Which window files a reader skips (`covered`) or must read (`read`) given a listing.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Coverage {
    pub valid: Vec<String>,
    pub ignored: Vec<String>,
    pub covered: Vec<String>,
    pub read: Vec<String>,
}

#[derive(Clone)]
struct Window {
    key: String,
    start: u64,
    size: u64,
    rows: u64,
}

struct CompactFile {
    key: String,
    a: u64,
    b: u64,
    nsrc: u64,
    rows: u64,
    tags: Option<BTreeSet<u32>>,
}

fn tag(key: &str) -> u32 {
    compute_common::crypto::fnv1a32(file_name(key).bytes())
}

fn parse_tags(text: &str, nsrc: u64) -> Option<BTreeSet<u32>> {
    if text.len() as u64 != nsrc * 8 || !text.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    (0..text.len())
        .step_by(8)
        .map(|i| u32::from_str_radix(&text[i..i + 8], 16).ok())
        .collect()
}

/// The key of the compact file covering `sources`, inside the `…/day=DD/` folder `day_prefix`.
pub fn compact_key(day_prefix: &str, a: u64, b: u64, sources: &[String], rows: u64) -> String {
    let tags: BTreeSet<u32> = sources.iter().map(|key| tag(key)).collect();
    let hex: String = tags.iter().map(|t| format!("{t:08x}")).collect();
    format!(
        "{day_prefix}compact={a}-{b}-c{}-n{rows}-{hex}.parquet",
        sources.len()
    )
}

fn file_name(key: &str) -> &str {
    crate::ingest::file_name(key)
}

fn parse_window(key: &str) -> Option<u64> {
    let rest = file_name(key)
        .strip_prefix("window=")?
        .strip_suffix(".parquet")?;
    rest.split('-').next()?.parse().ok()
}

/// Row count from a window name's `-n{rows}` suffix; 0 when absent.
fn parse_rows(key: &str) -> u64 {
    file_name(key)
        .strip_suffix(".parquet")
        .and_then(|name| name.rsplit_once("-n"))
        .and_then(|(_, rows)| rows.parse().ok())
        .unwrap_or(0)
}

fn parse_compact(key: &str) -> Option<CompactFile> {
    let rest = file_name(key)
        .strip_prefix("compact=")?
        .strip_suffix(".parquet")?;
    let mut parts = rest.split('-');
    let (a, b) = (parts.next()?.parse().ok()?, parts.next()?.parse().ok()?);
    let nsrc = parts.next()?.strip_prefix('c')?.parse().ok()?;
    let rows = parts.next()?.strip_prefix('n')?.parse().ok()?;
    let tags = match parts.next() {
        Some(text) => Some(parse_tags(text, nsrc)?),
        None => None,
    };
    parts.next().is_none().then(|| CompactFile {
        key: key.to_owned(),
        a,
        b,
        nsrc,
        rows,
        tags,
    })
}

/// The `…/day=DD/` folder a window or compact file belongs to.
fn day_folder(key: &str) -> &str {
    let folder = key.rsplit_once('/').map_or("", |(dir, _)| dir);
    match folder.rsplit_once('/') {
        Some((day, last)) if last.starts_with("hour=") => day,
        _ => folder,
    }
}

/// The `…/hour=HH` folder `key` sits in, if any.
fn hour_folder(key: &str) -> Option<&str> {
    let folder = key.rsplit_once('/')?.0;
    folder
        .rsplit('/')
        .next()
        .filter(|last| last.starts_with("hour="))
        .map(|_| folder)
}

struct Valid {
    file: CompactFile,
    /// Listed windows this compact covers.
    sources: Vec<Window>,
}

struct Analysis {
    valid: Vec<Valid>,
    ignored: Vec<String>,
    covered: Vec<Window>,
    uncovered: Vec<Window>,
}

fn analyze(objects: &[Object]) -> Analysis {
    let mut windows: BTreeMap<&str, Vec<Window>> = BTreeMap::new();
    let mut compacts = Vec::new();
    for object in objects {
        if let Some(start) = parse_window(&object.key) {
            windows
                .entry(day_folder(&object.key))
                .or_default()
                .push(Window {
                    key: object.key.clone(),
                    start,
                    size: object.size,
                    rows: parse_rows(&object.key),
                });
        } else if let Some(file) = parse_compact(&object.key) {
            compacts.push(file);
        }
    }
    let mut analysis = Analysis {
        valid: Vec::new(),
        ignored: Vec::new(),
        covered: Vec::new(),
        uncovered: Vec::new(),
    };
    let mut taken = BTreeSet::new();
    for file in compacts {
        // A compact filed under an `hour=` folder (request compaction) covers only that hour.
        let own = hour_folder(&file.key);
        let in_folder: Vec<&Window> = windows
            .get(day_folder(&file.key))
            .into_iter()
            .flatten()
            .filter(|w| own.is_none_or(|own| hour_folder(&w.key) == Some(own)))
            .collect();
        let sources: Vec<Window> = match &file.tags {
            Some(tags) => in_folder
                .into_iter()
                .filter(|w| tags.contains(&tag(&w.key)))
                .cloned()
                .collect(),
            None => {
                let in_range: Vec<Window> = in_folder
                    .into_iter()
                    .filter(|w| (file.a..=file.b).contains(&w.start))
                    .cloned()
                    .collect();
                if in_range.len() as u64 != file.nsrc && !in_range.is_empty() {
                    analysis.ignored.push(file.key);
                    continue;
                }
                in_range
            }
        };
        for window in &sources {
            if taken.insert(window.key.clone()) {
                analysis.covered.push(window.clone());
            }
        }
        analysis.valid.push(Valid { file, sources });
    }
    for window in windows.into_values().flatten() {
        if !taken.contains(&window.key) {
            analysis.uncovered.push(window);
        }
    }
    analysis.covered.sort_by(|x, y| x.key.cmp(&y.key));
    analysis.uncovered.sort_by(|x, y| x.key.cmp(&y.key));
    analysis
}

/// Applies the coverage rule to a listing of keys (the same rule readers apply).
pub fn classify(keys: &[String]) -> Coverage {
    let objects: Vec<Object> = keys
        .iter()
        .map(|key| Object {
            key: key.clone(),
            size: 0,
            last_modified: None,
        })
        .collect();
    let analysis = analyze(&objects);
    Coverage {
        valid: analysis.valid.into_iter().map(|v| v.file.key).collect(),
        ignored: analysis.ignored,
        covered: analysis.covered.into_iter().map(|w| w.key).collect(),
        read: analysis.uncovered.into_iter().map(|w| w.key).collect(),
    }
}

/// Each valid compact among `objects` with the listed windows it covers, and the listed windows
/// no valid compact covers.
pub fn covering(objects: &[Object]) -> (Vec<(String, Vec<String>)>, Vec<String>) {
    let analysis = analyze(objects);
    let valid = analysis
        .valid
        .into_iter()
        .map(|v| (v.file.key, v.sources.into_iter().map(|w| w.key).collect()))
        .collect();
    let uncovered = analysis.uncovered.into_iter().map(|w| w.key).collect();
    (valid, uncovered)
}

/// What a `compact=` file name records: the first and last source window start (`a`, `b`, in
/// the dataset's window name units), its source count and its row count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactName {
    pub a: u64,
    pub b: u64,
    pub sources: u64,
    pub rows: u64,
}

pub fn parse_compact_name(key: &str) -> Option<CompactName> {
    parse_compact(key).map(|file| CompactName {
        a: file.a,
        b: file.b,
        sources: file.nsrc,
        rows: file.rows,
    })
}

/// `…/day=DD/compact=`: the listing prefix of the compacts that may cover `key`'s day.
pub fn day_compact_prefix(key: &str) -> String {
    format!("{}/compact=", day_folder(key))
}

/// The `listed` keys a reader returns: valid compacts and the windows no valid compact covers.
/// `context` adds compacts listed elsewhere (a day's compacts from an earlier page); they hide
/// their sources but are not returned themselves.
pub fn visible(listed: &[String], context: &[String]) -> BTreeSet<String> {
    let all: BTreeSet<&String> = listed.iter().chain(context).collect();
    let keys: Vec<String> = all.into_iter().cloned().collect();
    let coverage = classify(&keys);
    let listed: BTreeSet<&String> = listed.iter().collect();
    coverage
        .valid
        .into_iter()
        .chain(coverage.read)
        .filter(|key| listed.contains(key))
        .collect()
}

#[derive(Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
struct State {
    /// Last unix day fully compacted, per kind.
    done: BTreeMap<String, i64>,
    /// `{kind}:{day}` ids with a delete queue record.
    pending: BTreeSet<String>,
    /// When each recent `{kind}:{day}` was last found fully compacted.
    checked: BTreeMap<String, i64>,
}

fn day_label(day: i64) -> String {
    let t = time::OffsetDateTime::from_unix_timestamp(day * DAY_S)
        .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
    format!("{:04}-{:02}-{:02}", t.year(), u8::from(t.month()), t.day())
}

fn day_folder_of(day: i64) -> String {
    let t = time::OffsetDateTime::from_unix_timestamp(day * DAY_S)
        .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
    format!(
        "year={:04}/month={:02}/day={:02}/",
        t.year(),
        u8::from(t.month()),
        t.day()
    )
}

enum DayOutcome {
    Done,
    Budget,
    Blocked(String),
}

struct Queue {
    generation: Option<String>,
    keys: Vec<String>,
    sizes: Vec<u64>,
}

fn read_queue(runtime: &impl Runtime, key: &str) -> Result<Queue, String> {
    let record = runtime.admin_get(key)?;
    let value = record.as_ref().map(|r| &r.value);
    let keys: Vec<String> = value
        .and_then(|v| serde_json::from_value(v["keys"].clone()).ok())
        .unwrap_or_default();
    let mut sizes: Vec<u64> = value
        .and_then(|v| serde_json::from_value(v["sizes"].clone()).ok())
        .unwrap_or_default();
    sizes.resize(keys.len(), 0);
    Ok(Queue {
        generation: record.map(|r| r.generation),
        keys,
        sizes,
    })
}

fn enqueue(
    runtime: &impl Runtime,
    key: &str,
    mut queue: Queue,
    sources: &[Window],
) -> Result<(), String> {
    for window in sources {
        if !queue.keys.contains(&window.key) {
            queue.keys.push(window.key.clone());
            queue.sizes.push(window.size);
        }
    }
    let value = json!({"keys": queue.keys, "sizes": queue.sizes, "at": runtime.now()});
    if runtime.admin_cas(key, queue.generation.as_deref(), &value)? {
        Ok(())
    } else {
        Err("compact_queue_conflict".into())
    }
}

/// Up to `cap` uncovered windows in start order whose compressed bytes and declared rows stay
/// within the chunk limits. A window over a limit by itself, or one that cannot be paired within
/// them, is left as a loose window, which readers still read.
fn next_chunk(uncovered: &[Window], cap: usize) -> Vec<Window> {
    let mut sorted: Vec<Window> = uncovered
        .iter()
        .filter(|w| w.size <= MAX_CHUNK_BYTES && w.rows <= MAX_CHUNK_ROWS)
        .cloned()
        .collect();
    sorted.sort_by(|x, y| (x.start, &x.key).cmp(&(y.start, &y.key)));
    for from in 0..sorted.len() {
        let (mut bytes, mut rows) = (0, 0);
        let chunk: Vec<Window> = sorted[from..]
            .iter()
            .take(cap.min(CHUNK_SOURCES))
            .take_while(|w| {
                bytes += w.size;
                rows += w.rows;
                bytes <= MAX_CHUNK_BYTES && rows <= MAX_CHUNK_ROWS
            })
            .cloned()
            .collect();
        if chunk.len() >= 2 {
            return chunk;
        }
    }
    Vec::new()
}

fn read_rows(
    store: &Store<'_, impl Runtime>,
    cdn_key: &str,
    kind: LogKind,
    key: &str,
) -> Result<(Vec<Value>, u64), String> {
    let bytes = store
        .get(cdn_key, key, CDN_READ_LIMIT)?
        .ok_or_else(|| format!("compact_source_missing:{key}"))?;
    let size = bytes.len() as u64;
    Ok((schema::from_parquet(kind, bytes)?, size))
}

fn ts_ms(row: &Value) -> i64 {
    row["timestamp"].as_i64().unwrap_or(0)
}

fn merge(rows: Vec<Value>) -> Vec<Value> {
    let mut rows = rows;
    rows.sort_by_key(|row| {
        (
            ts_ms(row),
            row["event_id"].as_str().unwrap_or_default().to_owned(),
        )
    });
    let mut seen = BTreeSet::new();
    rows.retain(|row| seen.insert(row["event_id"].as_str().unwrap_or_default().to_owned()));
    rows
}

#[allow(clippy::too_many_arguments)]
fn compact_day<R: Runtime>(
    store: &Store<'_, R>,
    cdn_key: &str,
    kind: LogKind,
    day: i64,
    state: &mut State,
    report: &mut CompactReport,
    room: &dyn Fn(u32) -> bool,
    stop: &dyn Fn() -> Option<&'static str>,
) -> DayOutcome {
    let runtime = store.runtime;
    let id = format!("{}:{}", kind.as_str(), day_label(day));
    let queue_key = format!("{QUEUE_PREFIX}{id}");
    let prefix = format!(
        "{}processed/ops/{}/{}",
        crate::ingest::scope(store.service),
        kind.as_str(),
        day_folder_of(day)
    );
    let mut unverified = BTreeSet::new();
    loop {
        if !room(MIN_CHUNK_CALLS) {
            return DayOutcome::Budget;
        }
        let mut step = || -> Result<Option<DayOutcome>, String> {
            let page = store.list(&prefix, None, None, 1000, None)?;
            if page.next_token.is_some() {
                return Ok(Some(DayOutcome::Blocked("compact_day_too_large".into())));
            }
            let analysis = analyze(&page.objects);
            if analysis.covered.is_empty() && analysis.uncovered.len() < 2 {
                return Ok(Some(DayOutcome::Done));
            }
            let queue = read_queue(runtime, &queue_key)?;
            if !queue.keys.is_empty() {
                state.pending.insert(id.clone());
            }
            for Valid { file, sources } in &analysis.valid {
                if sources.is_empty()
                    || unverified.contains(&file.key)
                    || sources.iter().all(|w| queue.keys.contains(&w.key))
                {
                    continue;
                }
                if !room(2) {
                    return Ok(Some(DayOutcome::Budget));
                }
                let (rows, _) = read_rows(store, cdn_key, kind, &file.key)?;
                if rows.len() as u64 != file.rows {
                    unverified.insert(file.key.clone());
                    report
                        .errors
                        .push(format!("compact_rows_mismatch:{}", file.key));
                    return Ok(None);
                }
                enqueue(runtime, &queue_key, queue, sources)?;
                state.pending.insert(id.clone());
                return Ok(None);
            }
            let uncovered: Vec<Window> = analysis
                .uncovered
                .into_iter()
                .filter(|w| !queue.keys.contains(&w.key))
                .collect();
            if uncovered.len() < 2 {
                return Ok(Some(DayOutcome::Done));
            }
            if let Some(reason) = stop() {
                report.stopped = Some(reason);
                return Ok(Some(DayOutcome::Budget));
            }
            let Some(cap) = (2..=uncovered.len().min(CHUNK_SOURCES))
                .rev()
                .find(|n| room(*n as u32 + 2))
            else {
                return Ok(Some(DayOutcome::Budget));
            };
            let chunk = next_chunk(&uncovered, cap);
            if chunk.len() < 2 {
                return Ok(Some(DayOutcome::Done));
            }
            let mut rows = Vec::new();
            for window in &chunk {
                if stop() == Some("wall_time") {
                    report.stopped = Some("wall_time");
                    return Ok(Some(DayOutcome::Budget));
                }
                let (decoded, size) = read_rows(store, cdn_key, kind, &window.key)?;
                report.bytes_read += size;
                rows.extend(decoded);
            }
            let rows = merge(rows);
            let (a, b) = (chunk[0].start, chunk[chunk.len() - 1].start);
            let names: Vec<String> = chunk.iter().map(|w| w.key.clone()).collect();
            let key = compact_key(&prefix, a, b, &names, rows.len() as u64);
            let body = schema::to_parquet(kind, &rows)?;
            let written = body.len() as u64;
            store.put(&key, body)?;
            report.chunks += 1;
            report.sources += chunk.len() as u64;
            report.rows += rows.len() as u64;
            report.bytes_written += written;
            enqueue(runtime, &queue_key, queue, &chunk)?;
            state.pending.insert(id.clone());
            Ok(None)
        };
        match step() {
            Ok(Some(outcome)) => return outcome,
            Ok(None) => {}
            Err(error) => return DayOutcome::Blocked(error),
        }
    }
}

/// `{scope}processed/ops/{kind}/year=…/month=…/day=…/` for a `{kind}:{YYYY-MM-DD}` queue id.
fn owned_prefix<R: Runtime>(store: &Store<'_, R>, id: &str) -> Option<String> {
    let (kind, date) = id.split_once(':')?;
    let kind = LogKind::parse(kind)?;
    let mut parts = date.split('-');
    let (year, month, day) = (parts.next()?, parts.next()?, parts.next()?);
    Some(format!(
        "{}processed/ops/{}/year={year}/month={month}/day={day}/",
        crate::ingest::scope(store.service),
        kind.as_str()
    ))
}

/// Only this service's window and compact files of the queue's own day are ever deleted.
fn is_compactable<R: Runtime>(store: &Store<'_, R>, prefix: &str, key: &str) -> bool {
    crate::ingest::api::valid_processed_key(store.service, key)
        && key.strip_prefix(prefix).is_some_and(|rest| {
            let name = rest.rsplit('/').next().unwrap_or(rest);
            name.starts_with("window=") || name.starts_with("compact=")
        })
}

fn drain<R: Runtime>(
    store: &Store<'_, R>,
    state: &mut State,
    settings: &OperationalLogSettings,
    report: &mut CompactReport,
    room: &dyn Fn(u32) -> bool,
) {
    let runtime = store.runtime;
    // Worker fan-out is not charged to the tick budget, so compaction deletes only use the endpoint.
    let settings = OperationalLogSettings {
        worker_delete_fallback: false,
        ..settings.clone()
    };
    for id in state.pending.clone() {
        if !room(DRAIN_CALLS) {
            report.more_pending = true;
            return;
        }
        let key = format!("{QUEUE_PREFIX}{id}");
        let owned = owned_prefix(store, &id);
        let mut step = || -> Result<(), String> {
            let mut queue = read_queue(runtime, &key)?;
            let sizes = std::mem::take(&mut queue.sizes);
            let (keys, sizes): (Vec<String>, Vec<u64>) = queue
                .keys
                .into_iter()
                .zip(sizes)
                .filter(|(k, _)| {
                    owned
                        .as_deref()
                        .is_some_and(|p| is_compactable(store, p, k))
                })
                .unzip();
            let queue = Queue {
                generation: queue.generation,
                keys,
                sizes,
            };
            if queue.keys.is_empty() {
                runtime.kv_delete(&key)?;
                state.pending.remove(&id);
                return Ok(());
            }
            if let Some(usage) = runtime.usage_agg() {
                usage.note_sizes(
                    queue
                        .keys
                        .iter()
                        .map(String::as_str)
                        .zip(queue.sizes.iter().copied()),
                );
            }
            let outcome = delete_keys(store, &queue.keys, &settings, &mut Vec::new())?;
            report.deleted += outcome.deleted.len() as u64;
            report.failed += outcome.failed.len() as u64;
            if outcome.failed.is_empty() {
                runtime.kv_delete(&key)?;
                state.pending.remove(&id);
                return Ok(());
            }
            report.more_pending = true;
            let sizes: Vec<u64> = queue
                .keys
                .iter()
                .zip(&queue.sizes)
                .filter(|(k, _)| outcome.failed.contains(k))
                .map(|(_, s)| *s)
                .collect();
            let value = json!({"keys": outcome.failed, "sizes": sizes, "at": runtime.now()});
            runtime.admin_cas(&key, queue.generation.as_deref(), &value)?;
            Ok(())
        };
        if let Err(error) = step() {
            report.errors.push(error);
            report.more_pending = true;
            return;
        }
    }
}

/// Compacts closed days per operational kind, oldest first, then deletes committed sources with
/// whatever budget is left. `reserve` calls stay free for the caller; it must be at least 1,
/// which the final state write uses.
pub fn log_compact<R: Runtime>(
    store: &Store<'_, R>,
    cdn_key: &str,
    settings: &OperationalLogSettings,
    reserve: u32,
) -> Result<CompactReport, String> {
    let mut report = CompactReport::default();
    if !settings.compaction_enabled {
        return Ok(report);
    }
    let runtime = store.runtime;
    let started_with = runtime.admin_remaining_calls();
    let room = |need: u32| runtime.admin_remaining_calls() >= reserve + need;
    // Measured for the whole execution: a maintenance step may follow a Delete Data step.
    let stop = || {
        if runtime.admin_elapsed(store.started) >= WALL_STOP {
            Some("wall_time")
        } else if runtime.vcpu_ms() >= VCPU_STOP_MS {
            Some("vcpu")
        } else {
            None
        }
    };
    if !room(1) {
        report.more_pending = true;
        return Ok(report);
    }
    let record = runtime.admin_get(STATE_KEY)?;
    let mut state: State = record
        .as_ref()
        .and_then(|r| serde_json::from_value(r.value.clone()).ok())
        .unwrap_or_default();
    let original = state.clone();
    let now = runtime.now() as i64;
    let today = now.div_euclid(DAY_S);
    if let Some(usage) = runtime.usage_agg() {
        usage.set_caller("cron:log_compact");
    }
    'kinds: for kind in LogKind::ALL {
        let days = settings
            .retention_days_by_kind
            .get(kind.as_str())
            .copied()
            .unwrap_or_else(|| kind.default_retention_days());
        let lookback = if days == 0 {
            LOOKBACK_DAYS
        } else {
            i64::from(days).min(LOOKBACK_DAYS)
        };
        let closed_last = (now - CLOSE_GRACE_S).div_euclid(DAY_S) - 1;
        let first = state
            .done
            .get(kind.as_str())
            .map_or(today - lookback, |done| done + 1)
            .max(today - lookback);
        let revisit_from = (today - REVISIT_DAYS).max(today - lookback);
        let new_days = first..=closed_last;
        let revisits = revisit_from..=closed_last.min(first - 1);
        for (day, is_new) in new_days
            .map(|d| (d, true))
            .chain(revisits.map(|d| (d, false)))
        {
            let id = format!("{}:{}", kind.as_str(), day_label(day));
            if !is_new
                && state
                    .checked
                    .get(&id)
                    .is_some_and(|at| now - at < REVISIT_INTERVAL_S)
            {
                continue;
            }
            match compact_day(
                store,
                cdn_key,
                kind,
                day,
                &mut state,
                &mut report,
                &room,
                &stop,
            ) {
                DayOutcome::Done => {
                    state.checked.insert(id, now);
                    let done = state.done.entry(kind.as_str().to_owned()).or_insert(day);
                    *done = (*done).max(day);
                }
                DayOutcome::Budget => {
                    report.more_pending = true;
                    break 'kinds;
                }
                DayOutcome::Blocked(error) => {
                    report.errors.push(error);
                    report.more_pending = true;
                    continue 'kinds;
                }
            }
        }
    }
    state
        .checked
        .retain(|_, at| now - *at < (REVISIT_DAYS + 2) * DAY_S);
    if report.stopped != Some("wall_time") {
        drain(store, &mut state, settings, &mut report, &room);
    }
    if state != original {
        let value = serde_json::to_value(&state).map_err(|_| "compact_state_encode")?;
        runtime.admin_cas(
            STATE_KEY,
            record.as_ref().map(|r| r.generation.as_str()),
            &value,
        )?;
    }
    report.backend_calls = started_with.saturating_sub(runtime.admin_remaining_calls());
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn window(start: u64, size: u64, rows: u64) -> Window {
        Window {
            key: format!("w{start}"),
            start,
            size,
            rows,
        }
    }

    #[test]
    fn chunks_stop_at_the_byte_and_row_caps() {
        let by_bytes: Vec<Window> = (0..5)
            .map(|i| window(i, MAX_CHUNK_BYTES / 3 + 1, 10))
            .collect();
        assert_eq!(next_chunk(&by_bytes, 24).len(), 2);
        let by_rows: Vec<Window> = (0..5)
            .map(|i| window(i, 10, MAX_CHUNK_ROWS / 3 + 1))
            .collect();
        assert_eq!(next_chunk(&by_rows, 24).len(), 2);
        let small: Vec<Window> = (0..30).map(|i| window(i, 10, 10)).collect();
        assert_eq!(next_chunk(&small, 24).len(), 24);
        assert_eq!(next_chunk(&small, 5).len(), 5);
    }

    #[test]
    fn an_oversized_window_is_left_loose_and_the_rest_still_pair_up() {
        let windows = vec![
            window(1, MAX_CHUNK_BYTES + 1, 1),
            window(2, 10, 10),
            window(3, 10, 10),
        ];
        let chunk = next_chunk(&windows, 24);
        assert_eq!(
            chunk.iter().map(|w| w.start).collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert!(next_chunk(&windows[..2], 24).is_empty());
        let wide = vec![
            window(1, 10, MAX_CHUNK_ROWS),
            window(2, 10, 1),
            window(3, 10, 1),
        ];
        assert_eq!(
            next_chunk(&wide, 24)
                .iter()
                .map(|w| w.start)
                .collect::<Vec<_>>(),
            vec![2, 3],
            "a window that cannot be paired is skipped, not retried forever"
        );
    }

    #[test]
    fn an_hour_folder_compact_covers_only_windows_of_its_own_hour() {
        let day = "r/year=2027/month=01/day=15/";
        let w8 = format!("{day}hour=08/window=100-160-x-n1.parquet");
        let w9 = format!("{day}hour=09/window=3700-3760-x-n1.parquet");
        let own = compact_key(
            &format!("{day}hour=08/"),
            100,
            160,
            std::slice::from_ref(&w8),
            1,
        );
        // A tag that matches a window of another hour (as a hash collision would) hides nothing.
        let stray = compact_key(
            &format!("{day}hour=08/"),
            3700,
            3760,
            std::slice::from_ref(&w9),
            1,
        );
        let coverage = classify(&[own.clone(), stray.clone(), w8.clone(), w9.clone()]);
        assert_eq!(coverage.covered, vec![w8]);
        assert_eq!(coverage.read, vec![w9]);
    }
}
