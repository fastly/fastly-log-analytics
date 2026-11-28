use super::{rfc3339, scope, storage::Store, worker::Output, WATERMARK_KEY};
use crate::{admin::Service, shell::Runtime};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

pub mod bundles;
pub mod delta;
pub mod discovery;

pub const VERSION: u32 = 2;
pub const REPLAY_COMMITS: usize = 32;
pub const MANIFEST_LIMIT: usize = 512 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Event {
    pub cursor: String,
    pub generation: u64,
    pub committed_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest: Option<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Head {
    pub version: u32,
    pub generation: u64,
    pub manifest: Option<String>,
    #[serde(default)]
    pub recent: Vec<Event>,
    #[serde(default)]
    pub days: BTreeMap<i64, String>,
    #[serde(default)]
    pub compaction_due: bool,
    #[serde(default)]
    pub next_compaction_check: u64,
    #[serde(default)]
    pub retirement_grace_until: u64,
    #[serde(default)]
    pub statistics: BTreeMap<i64, Statistics>,
    #[serde(default)]
    pub last_compaction: Option<CompactionCommit>,
    #[serde(default)]
    pub hour_bundles: BTreeMap<i64, Reference>,
    #[serde(default)]
    pub open_bundles: BTreeMap<i64, Reference>,
    #[serde(default)]
    pub day_bundles: BTreeMap<i64, Reference>,
    #[serde(default)]
    pub bundle_retired: BTreeMap<String, u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reset_generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reset_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionCommit {
    pub at: u64,
    pub files: Vec<Output>,
    pub replaces: BTreeSet<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Reference {
    pub key: String,
    pub generation: u64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Day {
    pub version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_id: Option<String>,
    pub hours: BTreeMap<i64, Reference>,
    #[serde(default)]
    pub statistics: Statistics,
    #[serde(default)]
    pub summary_counts: BTreeMap<i64, u64>,
    #[serde(default)]
    pub hour_statistics: BTreeMap<i64, Statistics>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Statistics {
    pub earliest: Option<i64>,
    pub latest: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub earliest_log: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_log: Option<i64>,
    pub rows: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Block {
    pub version: u32,
    pub service_id: String,
    pub generation: u64,
    pub hour: i64,
    pub previous: Option<Reference>,
    pub files: Vec<Output>,
    pub summaries: Vec<Output>,
    pub replaces: Vec<String>,
    pub committed_at: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u32,
    pub service_id: String,
    pub generation: u64,
    pub batch: String,
    pub schema: String,
    pub committed_at: String,
    pub previous: Option<String>,
    pub files: Vec<Output>,
    pub summaries: Vec<Output>,
    pub sources: Vec<String>,
    pub replaces: Vec<String>,
    pub bytes: u64,
    pub rows: u64,
    #[serde(default)]
    pub parents: BTreeMap<i64, Reference>,
}

pub fn head(watermark: &Value) -> Result<Head, String> {
    if watermark["catalog"].is_null() {
        return Ok(Head::default());
    }
    let head: Head = serde_json::from_value(watermark["catalog"].clone())
        .map_err(|_| "catalog_head_invalid".to_owned())?;
    if head.version != VERSION && head.version != 1 {
        return Err("catalog_version_unsupported".into());
    }
    if head.generation > 9_007_199_254_740_991
        || head.recent.len() > REPLAY_COMMITS
        || (head.generation > 0
            && head.recent.last().map(|event| event.generation) != Some(head.generation))
        || head
            .recent
            .windows(2)
            .any(|events| events[0].generation >= events[1].generation)
    {
        return Err("catalog_head_invalid".into());
    }
    Ok(head)
}

pub fn file_hour(key: &str) -> Option<i64> {
    let start = file_bounds(key)?.0;
    Some(start - start.rem_euclid(3600))
}

pub fn file_bounds(key: &str) -> Option<(i64, i64)> {
    if let Some(name) = super::bundle::parse_name(key) {
        let field = |prefix: &str| key.split('/').find_map(|part| part.strip_prefix(prefix));
        let date = time::Date::from_calendar_date(
            field("year=")?.parse().ok()?,
            time::Month::try_from(field("month=")?.parse::<u8>().ok()?).ok()?,
            field("day=")?.parse().ok()?,
        )
        .ok()?
        .midnight()
        .assume_utc()
        .unix_timestamp();
        return match name.kind {
            super::bundle::NameKind::Hour => {
                let start = date + i64::from(name.unit) * 3600;
                Some((start, start + 3600))
            }
            super::bundle::NameKind::Open => {
                let start = date + i64::from(name.unit) * 3600;
                let end = name
                    .end_ms
                    .map_or(start + 3600, |ms| (ms + 999).div_euclid(1000).max(start));
                Some((start, end))
            }
            super::bundle::NameKind::Day => Some((date, date + 86_400)),
            super::bundle::NameKind::Partial => None,
        };
    }
    if let Some(name) = super::parse_processed(key) {
        return Some((name.start, name.end));
    }
    if let Some(name) = crate::telemetry::compact::parse_compact_name(key) {
        return Some((i64::try_from(name.a).ok()?, i64::try_from(name.b).ok()?));
    }
    let name = key
        .rsplit('/')
        .next()?
        .strip_prefix("summary=")?
        .strip_suffix(".parquet")?;
    let mut parts = name.split('-');
    Some((parts.next()?.parse().ok()?, parts.next()?.parse().ok()?))
}

pub fn day_start(hour: i64) -> i64 {
    hour - hour.rem_euclid(86_400)
}

fn decode_i64_le_hex(hex_str: &str) -> Option<i64> {
    let bytes: [u8; 8] = hex::decode(hex_str).ok()?.try_into().ok()?;
    Some(i64::from_le_bytes(bytes))
}

fn file_log_bounds(file: &Output, start: i64, end: i64, now_s: i64) -> (i64, i64) {
    if let Some(meta) = &file.metadata {
        let mut min_ms: Option<i64> = None;
        let mut max_ms: Option<i64> = None;
        for group in &meta.row_groups {
            for col in &group.columns {
                if col.name == "ts_ms" {
                    if let Some(m) = col.min.as_deref().and_then(decode_i64_le_hex) {
                        min_ms = Some(min_ms.map_or(m, |old| old.min(m)));
                    }
                    if let Some(m) = col.max.as_deref().and_then(decode_i64_le_hex) {
                        max_ms = Some(max_ms.map_or(m, |old| old.max(m)));
                    }
                }
            }
        }
        if let (Some(a), Some(b)) = (min_ms, max_ms) {
            return (a.div_euclid(1000), b.div_euclid(1000));
        }
    }
    let clamped_end = if start <= now_s {
        end.min(now_s).max(start)
    } else {
        end
    };
    (start, clamped_end)
}

pub fn read<T: serde::de::DeserializeOwned, R: Runtime>(
    store: &Store<'_, R>,
    key: &str,
) -> Result<T, String> {
    let bytes = store
        .get_direct(key, MANIFEST_LIMIT)?
        .ok_or("catalog_object_not_visible")?;
    serde_json::from_slice(&bytes).map_err(|_| "catalog_object_invalid".into())
}

pub(crate) fn validate_day(day: &Day, service_id: &str) -> Result<(), &'static str> {
    if day.version != VERSION {
        return Err("catalog_version_unsupported");
    }
    if day
        .service_id
        .as_deref()
        .is_some_and(|sid| sid != service_id)
    {
        return Err("catalog_manifest_invalid");
    }
    Ok(())
}

pub(crate) fn read_day<R: Runtime>(store: &Store<'_, R>, key: &str) -> Result<Day, String> {
    let day: Day = read(store, key)?;
    validate_day(&day, &store.service.service_id)?;
    Ok(day)
}

/// Results of catalog reads sent ahead of need, by key.
pub type Prefetched = BTreeMap<String, Result<Option<Vec<u8>>, String>>;

/// Most bytes speculative reads may reserve at once: held answer buffers at allocated capacity
/// plus sent-but-unread requests at their full response limits. The reserve is also held below
/// the heap's headroom under the 96 MiB admission, so with one required read of at most 4 MiB
/// and the decode of one object at a time, the 128 MiB heap keeps a margin.
const PREFETCH_BYTES: usize = 32 * 1024 * 1024;

/// Whether reading `key` needs no new backend call: its answer is held, or it was sent and
/// charged already.
pub fn already_sent<R: Runtime>(store: &Store<'_, R>, prefetched: &Prefetched, key: &str) -> bool {
    prefetched.contains_key(key)
        || store
            .runtime
            .admin_unread(&store.service.service_id)
            .contains_key(key)
}

/// Reads `key` as [`Store::get_direct`] would, sending up to `max_batch - 1` of the not-yet-sent
/// `upcoming` objects alongside it so the walk's later reads share its round trip. Each
/// speculative answer, including a 404 or an error, is kept and returned when its key is read;
/// one left unread is resumed then, without another call. Callers check admission first.
pub fn fetch_batched<R: Runtime>(
    store: &Store<'_, R>,
    prefetched: &mut Prefetched,
    key: &str,
    limit: usize,
    max_batch: usize,
    upcoming: impl FnOnce() -> Vec<(String, usize)>,
) -> Result<Option<Vec<u8>>, String> {
    fetch_batched_reserving(store, prefetched, key, limit, max_batch, 3, upcoming)
}

/// Like [`fetch_batched`], keeping at least `reserve` backend calls free for the caller's tail.
pub fn fetch_batched_reserving<R: Runtime>(
    store: &Store<'_, R>,
    prefetched: &mut Prefetched,
    key: &str,
    limit: usize,
    max_batch: usize,
    reserve: u32,
    upcoming: impl FnOnce() -> Vec<(String, usize)>,
) -> Result<Option<Vec<u8>>, String> {
    if let Some(result) = prefetched.remove(key) {
        return result;
    }
    let unread = store.runtime.admin_unread(&store.service.service_id);
    let room = (store.runtime.admin_remaining_calls() as usize)
        .saturating_sub(reserve as usize + 1)
        .clamp(1, max_batch);
    let held: usize = prefetched
        .values()
        .map(|result| {
            result
                .as_ref()
                .map_or(0, |body| body.as_ref().map_or(0, Vec::capacity))
        })
        .sum();
    // This read consumes its own reservation, if it has one.
    let reserved: usize = unread
        .iter()
        .filter(|(item, _)| *item != key)
        .map(|(_, limit)| limit)
        .sum();
    let mut bytes = store
        .runtime
        .heap_mib()
        .map_or(0, |heap| {
            (96usize.saturating_sub(heap as usize) << 20).min(PREFETCH_BYTES)
        })
        .saturating_sub(held + reserved);
    let mut batch = vec![(key.to_owned(), limit)];
    for (item, limit) in upcoming() {
        if batch.len() >= room || limit > bytes {
            break;
        }
        if !prefetched.contains_key(&item)
            && !unread.contains_key(&item)
            && batch.iter().all(|(k, _)| *k != item)
        {
            bytes -= limit;
            batch.push((item, limit));
        }
    }
    let mut results = store.get_direct_many(&batch).into_iter();
    let mut needed = results
        .next()
        .unwrap_or_else(|| Err("catalog_object_not_visible".into()));
    for ((key, _), result) in batch.iter().skip(1).zip(results) {
        if !matches!(
            &result,
            Err(error)
                if error == crate::shell::PREFETCH_UNREAD
                    || super::storage::transient_error(error)
        ) {
            prefetched.insert(key.clone(), result);
        }
    }
    if let Err(ref error) = needed {
        if super::storage::transient_error(error)
            && store.within_wall()
            && store.runtime.admin_remaining_calls() > reserve
        {
            needed = store.get_direct(key, limit);
        }
    }
    needed
}

pub(crate) fn put<T: Serialize, R: Runtime>(
    store: &Store<'_, R>,
    value: &T,
) -> Result<String, String> {
    put_limit(store, value, MANIFEST_LIMIT)
}

fn encode_catalog_object<T: Serialize>(
    scope_prefix: &str,
    prefix_tag: &str,
    value: &T,
    limit: usize,
) -> Result<(String, Vec<u8>), String> {
    let body = serde_json::to_vec(value).map_err(|e| e.to_string())?;
    if body.len() > limit {
        return Err("catalog_manifest_too_large".into());
    }
    let digest = hex::encode(Sha256::digest(&body));
    let key = format!("{scope_prefix}processed/_catalog/{prefix_tag}{digest}.json");
    Ok((key, body))
}

type HeadDaySnapshot<'a> = (&'a BTreeMap<i64, String>, &'a BTreeMap<i64, Statistics>);

pub(crate) fn put_manifest<R: Runtime>(
    store: &Store<'_, R>,
    manifest: &Manifest,
    reset_generation: Option<u64>,
    reset_at: Option<i64>,
    head_snapshot: Option<HeadDaySnapshot<'_>>,
    limit: usize,
) -> Result<String, String> {
    let mut val = serde_json::to_value(manifest).map_err(|e| e.to_string())?;
    if let Some(obj) = val.as_object_mut() {
        if let Some(rg) = reset_generation {
            obj.insert("reset_generation".into(), Value::from(rg));
        }
        if let Some(ra) = reset_at {
            obj.insert("reset_at".into(), Value::from(ra));
        }
        if let Some((h_days, h_stats)) = head_snapshot {
            obj.insert(
                "head_days".into(),
                serde_json::to_value(h_days).map_err(|e| e.to_string())?,
            );
            obj.insert(
                "head_statistics".into(),
                serde_json::to_value(h_stats).map_err(|e| e.to_string())?,
            );
        }
    }
    let prefix_tag = format!(
        "manifest-s-{}-rev-{:020}-",
        manifest.service_id,
        u64::MAX - manifest.generation
    );
    let (key, body) = encode_catalog_object(&scope(store.service), &prefix_tag, &val, limit)?;
    store.put_verified(&key, body)?;
    Ok(key)
}

fn put_limit<T: Serialize, R: Runtime>(
    store: &Store<'_, R>,
    value: &T,
    limit: usize,
) -> Result<String, String> {
    let (key, body) = encode_catalog_object(&scope(store.service), "", value, limit)?;
    store.put_verified(&key, body)?;
    Ok(key)
}

/// vCPU the catalog may spend in one execution, counted from the ingest step's start. A tick
/// reaches the commit with 40-50 ms already spent on plan parsing and the source index; a gate
/// below that re-plans the same files every tick and never commits.
pub const VCPU_STOP_MS: u64 = 150;

const PREPARING: &str = "ingest:catalog_preparing";
const REPLACEMENT_PREPARING: &str = "ingest:catalog_replacement_preparing";

#[derive(Serialize, Deserialize)]
struct Preparing {
    generation: u64,
    identity: String,
    at: String,
    touched: u64,
    days: BTreeMap<i64, Day>,
    published: BTreeMap<i64, String>,
    #[serde(default)]
    published_statistics: BTreeMap<i64, Statistics>,
    manifest: Option<String>,
    #[serde(default)]
    proofs: BTreeMap<i64, Proof>,
    #[serde(default)]
    blocks: BTreeMap<i64, Reference>,
    #[serde(default)]
    offsets: BTreeMap<i64, usize>,
}

const MAX_PROOF_BLOCKS: usize = 16_384;

#[derive(Serialize, Deserialize)]
struct Proof {
    remaining: BTreeSet<String>,
    current: Option<Reference>,
    #[serde(default)]
    rows: u64,
    #[serde(default)]
    visited_blocks: BTreeSet<String>,
}

/// The returned watermark and ingestion watermark are committed by the same CAS.
#[allow(clippy::too_many_arguments)]
pub fn prepare<R: Runtime>(
    store: &Store<'_, R>,
    watermark: &Value,
    batch: &str,
    schema: &str,
    files: &[Output],
    summaries: &[Output],
    sources: &[String],
    replaces: &[String],
) -> Result<Value, String> {
    prepare_reserving(
        store, watermark, batch, schema, files, summaries, sources, replaces, 0,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn prepare_reserving<R: Runtime>(
    store: &Store<'_, R>,
    watermark: &Value,
    batch: &str,
    schema: &str,
    files: &[Output],
    summaries: &[Output],
    sources: &[String],
    replaces: &[String],
    reserve: u32,
) -> Result<Value, String> {
    if store.runtime.admin_remaining_calls() <= reserve + 2 {
        return Err("catalog_prepare_pending".into());
    }
    let mut head = head(watermark)?;
    // Version one was only a journal. The approved reset does not import its objects.
    if head.version != VERSION {
        head.days.clear();
        head.statistics.clear();
    }
    let generation = head
        .generation
        .checked_add(1)
        .filter(|generation| *generation <= 9_007_199_254_740_991)
        .ok_or("catalog_generation_exhausted")?;
    let hours: BTreeSet<i64> = files
        .iter()
        .chain(summaries)
        .map(|file| &file.key)
        .chain(replaces)
        .map(|key| file_hour(key).ok_or_else(|| "catalog_file_partition_invalid".to_owned()))
        .collect::<Result<_, _>>()?;
    let days: BTreeSet<i64> = hours.iter().copied().map(day_start).collect();
    let identity = hex::encode(Sha256::digest(
        serde_json::to_vec(&(batch, schema, files, summaries, sources, replaces))
            .map_err(|e| e.to_string())?,
    ));
    let runtime = store.runtime;
    // Independent checkpoints keep a paused maintenance publication from leasing out ingestion.
    // The watermark CAS still arbitrates every committed generation.
    let preparing_key = if replaces.is_empty() {
        PREPARING
    } else {
        REPLACEMENT_PREPARING
    };
    let record = runtime.admin_get(preparing_key)?;
    let saved: Option<Preparing> = record
        .as_ref()
        .map(|r| serde_json::from_value(r.value.clone()))
        .transpose()
        .map_err(|_| "catalog_preparation_invalid")?;
    if saved.as_ref().is_some_and(|p| {
        p.generation == generation
            && p.identity != identity
            && p.touched.saturating_add(70) > runtime.now()
    }) {
        return Err("catalog_prepare_pending".into());
    }
    let mut work = saved
        .filter(|p| p.generation == generation && p.identity == identity)
        .unwrap_or_else(|| Preparing {
            generation,
            identity,
            at: rfc3339(runtime.now() as i64),
            touched: runtime.now(),
            days: BTreeMap::new(),
            published: BTreeMap::new(),
            published_statistics: BTreeMap::new(),
            manifest: None,
            proofs: BTreeMap::new(),
            blocks: BTreeMap::new(),
            offsets: BTreeMap::new(),
        });
    let pending = |work: &mut Preparing| -> Result<Value, String> {
        work.touched = runtime.now();
        let value = serde_json::to_value(work).map_err(|e| e.to_string())?;
        if serde_json::to_vec(&value).map_err(|e| e.to_string())?.len() > MANIFEST_LIMIT {
            return Err("catalog_preparation_too_large".into());
        }
        runtime.admin_cas(
            preparing_key,
            record.as_ref().map(|r| r.generation.as_str()),
            &value,
        )?;
        Err("catalog_prepare_pending".into())
    };
    for day in &days {
        if work.days.contains_key(day) {
            continue;
        }
        if runtime.admin_remaining_calls() <= reserve + 1 || !admitted(store) {
            return pending(&mut work);
        }
        let index = head
            .days
            .get(day)
            .map(|key| read_day(store, key))
            .transpose()?
            .unwrap_or_else(|| Day {
                version: VERSION,
                service_id: Some(store.service.service_id.clone()),
                ..Default::default()
            });
        work.days.insert(*day, index);
    }
    let committed_at = work.at.clone();
    for hour in &hours {
        let wanted = replaces
            .iter()
            .filter(|key| file_hour(key) == Some(*hour))
            .cloned()
            .collect::<BTreeSet<_>>();
        if wanted.is_empty() {
            continue;
        }
        work.proofs.entry(*hour).or_insert_with(|| Proof {
            remaining: wanted,
            current: work.days[&day_start(*hour)].hours.get(hour).cloned(),
            rows: 0,
            visited_blocks: BTreeSet::new(),
        });
        while !work.proofs[hour].remaining.is_empty() {
            if runtime.admin_remaining_calls() <= reserve + 1 || !admitted(store) {
                return pending(&mut work);
            }
            let proof = work
                .proofs
                .get_mut(hour)
                .ok_or("catalog_preparation_invalid")?;
            let current = proof
                .current
                .as_ref()
                .ok_or("catalog_replacement_not_active")?
                .clone();
            if proof.visited_blocks.contains(&current.key) {
                return Err("catalog_parent_invalid".into());
            }
            let parent: Block = read(store, &current.key)?;
            if parent.version != VERSION
                || parent.service_id != store.service.service_id
                || parent.hour != *hour
                || parent.generation != current.generation
                || current.generation > head.generation
            {
                return Err("catalog_manifest_invalid".into());
            }
            proof.visited_blocks.insert(current.key.clone());
            if proof.visited_blocks.len() > MAX_PROOF_BLOCKS {
                return Err("catalog_identity_admission".into());
            }
            if parent.previous.as_ref().is_some_and(|prev| {
                prev.generation > current.generation
                    || prev.key == current.key
                    || proof.visited_blocks.contains(&prev.key)
            }) {
                return Err("catalog_parent_invalid".into());
            }
            if parent
                .replaces
                .iter()
                .any(|key| proof.remaining.contains(key))
            {
                return Err("catalog_replacement_not_active".into());
            }
            for file in &parent.files {
                if proof.remaining.remove(&file.key) {
                    proof.rows = proof
                        .rows
                        .checked_add(file.rows)
                        .ok_or("catalog_rows_overflow")?;
                }
            }
            proof.current = parent.previous;
        }
    }
    for hour in &hours {
        let entries = files
            .iter()
            .map(|file| (false, file))
            .chain(summaries.iter().map(|file| (true, file)))
            .filter(|(_, file)| file_hour(&file.key) == Some(*hour))
            .collect::<Vec<_>>();
        let mut offset = work.offsets.get(hour).copied().unwrap_or(0);
        while offset < entries.len().max(1) {
            if runtime.admin_remaining_calls() <= reserve + 3 || !admitted(store) {
                return pending(&mut work);
            }
            let end = (offset + 64).min(entries.len());
            let chunk = entries.get(offset..end).unwrap_or_default();
            let previous = work
                .blocks
                .get(hour)
                .cloned()
                .or_else(|| work.days[&day_start(*hour)].hours.get(hour).cloned());
            let mut block = Block {
                version: VERSION,
                service_id: store.service.service_id.clone(),
                generation,
                hour: *hour,
                previous: previous.clone(),
                files: chunk
                    .iter()
                    .filter(|(summary, _)| !summary)
                    .map(|(_, file)| (*file).clone())
                    .collect(),
                summaries: chunk
                    .iter()
                    .filter(|(summary, _)| *summary)
                    .map(|(_, file)| (*file).clone())
                    .collect(),
                replaces: if offset == 0 {
                    replaces
                        .iter()
                        .filter(|key| file_hour(key) == Some(*hour))
                        .cloned()
                        .collect()
                } else {
                    Vec::new()
                },
                committed_at: chunk
                    .iter()
                    .map(|(_, file)| (file.key.clone(), committed_at.clone()))
                    .collect(),
            };
            if let Some(reference) = previous {
                let max_parent_gen = if work.blocks.contains_key(hour) {
                    generation
                } else {
                    head.generation
                };
                let parent: Block = read(store, &reference.key)?;
                if parent.version != VERSION
                    || parent.service_id != store.service.service_id
                    || parent.hour != *hour
                    || parent.generation != reference.generation
                    || parent.generation > max_parent_gen
                {
                    return Err("catalog_manifest_invalid".into());
                }
                if parent.previous.as_ref().is_some_and(|prev| {
                    prev.generation > parent.generation || prev.key == reference.key
                }) {
                    return Err("catalog_parent_invalid".into());
                }
                let mut merged = block.clone();
                merged.previous = parent.previous;
                let old_keys = parent
                    .files
                    .iter()
                    .map(|file| file.key.as_str())
                    .collect::<BTreeSet<_>>();
                merged
                    .replaces
                    .retain(|key| !old_keys.contains(key.as_str()));
                merged.replaces.extend(parent.replaces);
                let new_keys = block
                    .files
                    .iter()
                    .chain(&block.summaries)
                    .map(|file| file.key.clone())
                    .chain(block.replaces.iter().cloned())
                    .collect::<BTreeSet<_>>();
                merged.files.extend(
                    parent
                        .files
                        .into_iter()
                        .filter(|file| !new_keys.contains(&file.key)),
                );
                merged.summaries.extend(
                    parent
                        .summaries
                        .into_iter()
                        .filter(|file| !new_keys.contains(&file.key)),
                );
                merged.committed_at.extend(
                    parent
                        .committed_at
                        .into_iter()
                        .filter(|(key, _)| !new_keys.contains(key)),
                );
                if merged.files.len() + merged.summaries.len() + merged.replaces.len() <= 128
                    && serde_json::to_vec(&merged)
                        .map_err(|e| e.to_string())?
                        .len()
                        <= MANIFEST_LIMIT
                {
                    block = merged;
                }
            }
            block.files.sort_by(|a, b| a.key.cmp(&b.key));
            block.summaries.sort_by(|a, b| a.key.cmp(&b.key));
            let key = put(store, &block)?;
            work.blocks.insert(*hour, Reference { key, generation });
            offset = end.max(1);
            work.offsets.insert(*hour, offset);
        }
    }
    let parents = hours
        .iter()
        .filter_map(|hour| {
            work.days
                .get(&day_start(*hour))?
                .hours
                .get(hour)
                .cloned()
                .map(|r| (*hour, r))
        })
        .collect();
    let manifest = Manifest {
        version: VERSION,
        service_id: store.service.service_id.clone(),
        generation,
        batch: batch.to_owned(),
        schema: schema.to_owned(),
        committed_at: committed_at.clone(),
        previous: head.manifest.clone(),
        files: files.to_vec(),
        summaries: summaries.to_vec(),
        sources: sources.to_vec(),
        replaces: replaces.to_vec(),
        bytes: files.iter().map(|f| f.bytes).sum(),
        rows: files.iter().map(|f| f.rows).sum(),
        parents,
    };
    let scope_prefix = scope(store.service);
    let now_s =
        time::OffsetDateTime::parse(&work.at, &time::format_description::well_known::Rfc3339)
            .ok()
            .map(|dt| dt.unix_timestamp())
            .unwrap_or_else(|| runtime.now() as i64);
    let mut computed_days: BTreeMap<i64, (Day, String, Vec<u8>)> = BTreeMap::new();
    let mut preview_days = head.days.clone();
    let mut preview_stats = head.statistics.clone();
    for day in &days {
        let mut index = work.days[day].clone();
        if !work.published.contains_key(day) {
            let removed = work
                .proofs
                .iter()
                .filter(|(hour, _)| day_start(**hour) == *day)
                .try_fold(0u64, |sum, (_, proof)| sum.checked_add(proof.rows))
                .ok_or("catalog_rows_overflow")?;
            index.statistics.rows = index
                .statistics
                .rows
                .checked_sub(removed)
                .ok_or("catalog_rows_invalid")?;
            for file in files
                .iter()
                .filter(|file| file_hour(&file.key).map(day_start) == Some(*day))
            {
                let (start, end) =
                    file_bounds(&file.key).ok_or("catalog_file_partition_invalid")?;
                let (log_start, log_end) = file_log_bounds(file, start, end, now_s);
                index.statistics.earliest = Some(
                    index
                        .statistics
                        .earliest
                        .map_or(start, |old| old.min(start)),
                );
                index.statistics.latest =
                    Some(index.statistics.latest.map_or(end, |old| old.max(end)));
                index.statistics.earliest_log = Some(
                    index
                        .statistics
                        .earliest_log
                        .or(index.statistics.earliest)
                        .map_or(log_start, |old| old.min(log_start)),
                );
                index.statistics.latest_log = Some(
                    index
                        .statistics
                        .latest_log
                        .map_or(log_end, |old| old.max(log_end)),
                );
                index.statistics.rows = index
                    .statistics
                    .rows
                    .checked_add(file.rows)
                    .ok_or("catalog_rows_overflow")?;
            }
            for hour in hours.iter().filter(|hour| day_start(**hour) == *day) {
                if index.hour_statistics.contains_key(hour) || !index.hours.contains_key(hour) {
                    let statistics = index.hour_statistics.entry(*hour).or_default();
                    statistics.rows = statistics
                        .rows
                        .checked_sub(work.proofs.get(hour).map_or(0, |p| p.rows))
                        .ok_or("catalog_rows_invalid")?;
                    for file in files
                        .iter()
                        .filter(|file| file_hour(&file.key) == Some(*hour))
                    {
                        let (a, b) =
                            file_bounds(&file.key).ok_or("catalog_file_partition_invalid")?;
                        let (log_a, log_b) = file_log_bounds(file, a, b, now_s);
                        statistics.earliest = Some(statistics.earliest.map_or(a, |old| old.min(a)));
                        statistics.latest = Some(statistics.latest.map_or(b, |old| old.max(b)));
                        statistics.earliest_log = Some(
                            statistics
                                .earliest_log
                                .or(statistics.earliest)
                                .map_or(log_a, |old| old.min(log_a)),
                        );
                        statistics.latest_log =
                            Some(statistics.latest_log.map_or(log_b, |old| old.max(log_b)));
                        statistics.rows = statistics
                            .rows
                            .checked_add(file.rows)
                            .ok_or("catalog_rows_overflow")?;
                    }
                }
                let added = summaries
                    .iter()
                    .filter(|file| file_hour(&file.key) == Some(*hour))
                    .count() as u64;
                if let Some(count) = index.summary_counts.get_mut(hour) {
                    *count = count.checked_add(added).ok_or("catalog_rows_overflow")?;
                } else if !index.hours.contains_key(hour) {
                    index.summary_counts.insert(*hour, added);
                }
                index.hours.insert(*hour, work.blocks[hour].clone());
            }
            index.service_id = Some(store.service.service_id.clone());
            let (day_key, day_body) =
                encode_catalog_object(&scope_prefix, "", &index, MANIFEST_LIMIT)?;
            preview_days.insert(*day, day_key.clone());
            preview_stats.insert(*day, index.statistics.clone());
            computed_days.insert(*day, (index, day_key, day_body));
        } else {
            preview_days.insert(*day, work.published[day].clone());
            let stats = work
                .published_statistics
                .get(day)
                .cloned()
                .unwrap_or_else(|| index.statistics.clone());
            preview_stats.insert(*day, stats);
        }
    }
    for day in &days {
        if work.published.contains_key(day) {
            continue;
        }
        if runtime.admin_remaining_calls() <= reserve + 1 || !admitted(store) {
            return pending(&mut work);
        }
        let (index, day_key, day_body) = computed_days
            .remove(day)
            .ok_or("catalog_preparation_invalid")?;
        store.put_verified(&day_key, day_body)?;
        work.published.insert(*day, day_key);
        work.published_statistics.insert(*day, index.statistics);
    }
    if work.manifest.is_none() {
        if runtime.admin_remaining_calls() <= reserve + 1 || !admitted(store) {
            return pending(&mut work);
        }
        work.manifest = Some(put_manifest(
            store,
            &manifest,
            head.reset_generation,
            head.reset_at,
            Some((&preview_days, &preview_stats)),
            4 * 1024 * 1024,
        )?);
    }
    let key = work.manifest.clone().ok_or("catalog_preparation_invalid")?;
    for day in &days {
        if let Some(stats) = preview_stats.get(day) {
            head.statistics.insert(*day, stats.clone());
        }
    }
    head.days.extend(work.published);
    head.version = VERSION;
    head.generation = generation;
    head.manifest = Some(key.clone());
    if batch != "compaction" && !files.is_empty() {
        head.compaction_due = true;
        head.next_compaction_check = runtime.now().saturating_add(super::compaction::MIN_AGE_S);
    }
    if batch == "compaction" {
        head.last_compaction = Some(CompactionCommit {
            at: runtime.now(),
            files: files
                .iter()
                .map(|file| Output {
                    metadata: None,
                    ..file.clone()
                })
                .collect(),
            replaces: replaces.iter().cloned().collect(),
        });
    }
    head.recent.push(Event {
        cursor: format!("{}:{generation}", store.service.service_id),
        generation,
        committed_at: Some(committed_at),
        manifest: Some(key),
    });
    if head.recent.len() > REPLAY_COMMITS {
        head.recent.drain(..head.recent.len() - REPLAY_COMMITS);
    }
    let mut value = watermark.clone();
    value["catalog"] = serde_json::to_value(head).map_err(|e| e.to_string())?;
    Ok(value)
}

/// Whether `key` appears in any committed block of its hour in `watermark`'s active catalog.
pub(super) fn is_file_published<R: Runtime>(
    store: &Store<'_, R>,
    watermark: &Value,
    key: &str,
    reserve: u32,
) -> Result<bool, String> {
    let head = head(watermark)?;
    if head.version != VERSION {
        return Ok(false);
    }
    let hour = file_hour(key).ok_or("catalog_file_partition_invalid")?;
    let Some(day_key) = head.days.get(&day_start(hour)) else {
        return Ok(false);
    };
    if store.runtime.admin_remaining_calls() <= reserve + 1 || !admitted(store) {
        return Err("catalog_prepare_pending".into());
    }
    let day = read_day(store, day_key)?;
    let mut current = day.hours.get(&hour).cloned();
    let mut visited = BTreeSet::new();
    while let Some(reference) = current {
        if !visited.insert(reference.key.clone()) || visited.len() > MAX_PROOF_BLOCKS {
            return Err("catalog_parent_invalid".into());
        }
        if store.runtime.admin_remaining_calls() <= reserve + 1 || !admitted(store) {
            return Err("catalog_prepare_pending".into());
        }
        let block: Block = read(store, &reference.key)?;
        if block.version != VERSION
            || block.service_id != store.service.service_id
            || block.hour != hour
            || block.generation != reference.generation
            || reference.generation > head.generation
        {
            return Err("catalog_manifest_invalid".into());
        }
        if block.files.iter().any(|file| file.key == key)
            || block.replaces.iter().any(|replaced| replaced == key)
        {
            return Ok(true);
        }
        current = block.previous;
    }
    Ok(false)
}

pub fn admitted<R: Runtime>(store: &Store<'_, R>) -> bool {
    store.within_wall()
        && store.vcpu_ms() < VCPU_STOP_MS
        && store.runtime.heap_mib().is_none_or(|heap| heap < 96)
}

pub fn retire<R: Runtime>(store: &Store<'_, R>) -> Result<u64, String> {
    retire_reserving(store, 0)
}

pub fn retire_reserving<R: Runtime>(store: &Store<'_, R>, reserve: u32) -> Result<u64, String> {
    let runtime = store.runtime;
    let Some(record) = runtime.admin_get(WATERMARK_KEY)? else {
        return Ok(0);
    };
    let mut catalog = head(&record.value)?;
    if catalog.version != VERSION || store.service.log_retention_days == 0 {
        return Ok(0);
    }
    let cutoff = (runtime.now() as i64 - i64::from(store.service.log_retention_days) * 86_400)
        .div_euclid(3600)
        * 3600;
    let cutoff_day = day_start(cutoff);
    let mut partial = None;
    if let Some(key) = catalog.days.get(&cutoff_day) {
        if runtime.admin_remaining_calls() <= reserve + 1 || !admitted(store) {
            return Err("catalog_prepare_pending".into());
        }
        let mut day = read_day(store, key)?;
        if day.hours.keys().any(|hour| *hour < cutoff) {
            day.hours.retain(|hour, _| *hour >= cutoff);
            day.summary_counts.retain(|hour, _| *hour >= cutoff);
            day.hour_statistics.retain(|hour, _| *hour >= cutoff);
            if day
                .hours
                .keys()
                .any(|hour| !day.hour_statistics.contains_key(hour))
            {
                return Err("catalog_retirement_statistics_unavailable".into());
            }
            day.statistics = Statistics::default();
            for statistics in day.hour_statistics.values() {
                day.statistics.rows = day
                    .statistics
                    .rows
                    .checked_add(statistics.rows)
                    .ok_or("catalog_rows_overflow")?;
                if let Some(a) = statistics.earliest {
                    day.statistics.earliest =
                        Some(day.statistics.earliest.map_or(a, |old| old.min(a)));
                }
                if let Some(b) = statistics.latest {
                    day.statistics.latest = Some(day.statistics.latest.map_or(b, |old| old.max(b)));
                }
                if let Some(a) = statistics.earliest_log.or(statistics.earliest) {
                    day.statistics.earliest_log =
                        Some(day.statistics.earliest_log.map_or(a, |old| old.min(a)));
                }
                if let Some(b) = statistics.latest_log.or(statistics.latest) {
                    day.statistics.latest_log =
                        Some(day.statistics.latest_log.map_or(b, |old| old.max(b)));
                }
            }
            day.service_id = Some(store.service.service_id.clone());
            partial = Some(day);
        }
    }
    if catalog.days.keys().all(|day| *day >= cutoff_day) && partial.is_none() {
        return Ok(catalog.retirement_grace_until);
    }
    if runtime.admin_remaining_calls()
        < reserve + 3 + u32::from(partial.as_ref().is_some_and(|day| !day.hours.is_empty()))
        || !admitted(store)
    {
        return Err("catalog_prepare_pending".into());
    }
    let generation = catalog
        .generation
        .checked_add(1)
        .filter(|value| *value <= 9_007_199_254_740_991)
        .ok_or("catalog_generation_exhausted")?;
    let committed_at = rfc3339(runtime.now() as i64);
    let manifest = Manifest {
        version: VERSION,
        service_id: store.service.service_id.clone(),
        generation,
        batch: "retention".into(),
        schema: String::new(),
        committed_at: committed_at.clone(),
        previous: catalog.manifest.clone(),
        files: Vec::new(),
        summaries: Vec::new(),
        sources: Vec::new(),
        replaces: Vec::new(),
        bytes: 0,
        rows: 0,
        parents: BTreeMap::new(),
    };
    let mut next_days = catalog.days.clone();
    let mut next_stats = catalog.statistics.clone();
    next_days.retain(|day, _| *day >= cutoff_day);
    next_stats.retain(|day, _| *day >= cutoff_day);
    let encoded_partial = match partial {
        Some(day) if day.hours.is_empty() => {
            next_days.remove(&cutoff_day);
            next_stats.remove(&cutoff_day);
            None
        }
        Some(day) => {
            let (day_key, day_body) =
                encode_catalog_object(&scope(store.service), "", &day, MANIFEST_LIMIT)?;
            next_stats.insert(cutoff_day, day.statistics.clone());
            next_days.insert(cutoff_day, day_key.clone());
            Some((day_key, day_body))
        }
        None => None,
    };
    if let Some((day_key, day_body)) = encoded_partial {
        store.put_verified(&day_key, day_body)?;
    }
    let key = put_manifest(
        store,
        &manifest,
        catalog.reset_generation,
        catalog.reset_at,
        Some((&next_days, &next_stats)),
        MANIFEST_LIMIT,
    )?;
    catalog.days = next_days;
    catalog.statistics = next_stats;
    catalog.hour_bundles.retain(|hour, _| *hour >= cutoff);
    catalog.open_bundles.retain(|hour, _| *hour >= cutoff);
    catalog.day_bundles.retain(|day, _| *day >= cutoff);
    catalog
        .bundle_retired
        .retain(|key, _| file_bounds(key).is_some_and(|(_, end)| end > cutoff));
    catalog.generation = generation;
    catalog.manifest = Some(key.clone());
    catalog.retirement_grace_until = runtime
        .now()
        .saturating_add(super::compaction::DELETE_GRACE_S);
    catalog.recent.push(Event {
        cursor: format!("{}:{generation}", store.service.service_id),
        generation,
        committed_at: Some(committed_at),
        manifest: Some(key),
    });
    if catalog.recent.len() > REPLAY_COMMITS {
        catalog
            .recent
            .drain(..catalog.recent.len() - REPLAY_COMMITS);
    }
    let grace = catalog.retirement_grace_until;
    let mut value = record.value;
    value["catalog"] = serde_json::to_value(catalog).map_err(|e| e.to_string())?;
    if !runtime.admin_cas(WATERMARK_KEY, Some(&record.generation), &value)? {
        return Err("catalog_retirement_conflict".into());
    }
    Ok(grace)
}

/// A Delete Data reset of `prev`: the next generation, with an empty catalog and a `"reset"`
/// manifest that names no previous commit, so delta walks and SSE cursors from before it resync.
/// The caller stores the manifest and sets its key on the head and its one event.
pub fn reset_head(prev: &Head, now: u64, service_id: &str) -> (Head, Manifest) {
    let generation = prev.generation + 1;
    let committed_at = rfc3339(now as i64);
    let manifest = Manifest {
        version: VERSION,
        service_id: service_id.to_owned(),
        generation,
        batch: "reset".into(),
        schema: String::new(),
        committed_at: committed_at.clone(),
        previous: None,
        files: Vec::new(),
        summaries: Vec::new(),
        sources: Vec::new(),
        replaces: Vec::new(),
        bytes: 0,
        rows: 0,
        parents: BTreeMap::new(),
    };
    let head = Head {
        version: VERSION,
        generation,
        manifest: None,
        recent: vec![Event {
            cursor: format!("{service_id}:{generation}"),
            generation,
            committed_at: Some(committed_at),
            manifest: None,
        }],
        retirement_grace_until: now.saturating_add(super::compaction::DELETE_GRACE_S),
        reset_generation: Some(generation),
        reset_at: Some(now as i64),
        ..Head::default()
    };
    (head, manifest)
}

pub fn retry_ms(period_s: u32) -> u64 {
    (u64::from(period_s) * 500).clamp(1000, 30_000)
}

pub fn events<R: Runtime>(runtime: &R, service: &Service, query: &str) -> Result<String, String> {
    if let Some(usage) = runtime.usage_agg() {
        usage.set_caller("api:ingest_events");
    }
    let record = runtime.admin_get(WATERMARK_KEY)?;
    let watermark = record.as_ref().map(|r| &r.value).unwrap_or(&Value::Null);
    let head = head(watermark)?;
    let current = head.recent.last().cloned().unwrap_or(Event {
        cursor: format!("{}:0", service.service_id),
        generation: 0,
        committed_at: None,
        manifest: None,
    });
    let cursor = url::form_urlencoded::parse(query.as_bytes())
        .find(|(k, _)| k == "cursor")
        .map(|(_, v)| v.into_owned());
    let kind = match cursor.as_deref() {
        None | Some("") => "ready",
        Some(cursor) if cursor == current.cursor => {
            return Ok(format!(
                "retry: {}\n\n: current\n\n",
                retry_ms(service.log_period)
            ));
        }
        Some(cursor) => {
            if cursor
                .strip_prefix(&format!("{}:", service.service_id))
                .and_then(|v| v.parse::<u64>().ok())
                .is_some_and(|generation| generation > head.generation)
            {
                return Ok(format!(
                    "retry: {}\n\n: catalog head behind cursor\n\n",
                    retry_ms(service.log_period)
                ));
            }
            let retained = head.recent.iter().any(|e| e.cursor == cursor)
                || (head.generation <= REPLAY_COMMITS as u64
                    && cursor == format!("{}:0", service.service_id));
            if retained {
                "commit"
            } else {
                "resync"
            }
        }
    };
    let data = serde_json::to_string(&current).map_err(|e| e.to_string())?;
    Ok(format!(
        "retry: {}\n\nid: {}\nevent: {kind}\ndata: {data}\n\n",
        retry_ms(service.log_period),
        current.cursor
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        admin::{FosRequest, FosResponse, Record, Service},
        setup::SetupStores,
    };
    use serde_json::json;
    use std::{
        cell::{Cell, RefCell},
        time::Instant,
    };

    struct MockRt {
        kv: RefCell<BTreeMap<String, Value>>,
        objects: RefCell<BTreeMap<String, Vec<u8>>>,
        remaining: Cell<u32>,
        gets: Cell<u32>,
    }
    impl SetupStores for MockRt {
        fn config_present(&self, _: &str) -> Result<bool, String> {
            Ok(true)
        }
        fn secret_present(&self, _: &str) -> Result<bool, String> {
            Ok(true)
        }
        fn state_linked(&self) -> Result<bool, String> {
            Ok(true)
        }
    }
    impl Runtime for MockRt {
        fn config(&self, _: &str) -> Result<Option<String>, String> {
            Ok(None)
        }
        fn secret(&self, _: &str) -> Result<Option<String>, String> {
            Ok(Some("tok".into()))
        }
        fn random(&self) -> Result<[u8; 32], String> {
            Ok([0; 32])
        }
        fn now(&self) -> u64 {
            1_800_003_600
        }
        fn checks(&self, _: bool) -> Result<Value, String> {
            Ok(json!({}))
        }
        fn versions(&self) -> Result<Value, String> {
            Ok(json!({}))
        }
        fn compute_service_id(&self) -> Option<String> {
            None
        }
        fn compute_service_version(&self) -> Option<String> {
            None
        }
        fn admin_get(&self, key: &str) -> Result<Option<Record>, String> {
            Ok(self.kv.borrow().get(key).map(|v| Record {
                generation: "1".into(),
                value: v.clone(),
            }))
        }
        fn admin_cas(&self, key: &str, _: Option<&str>, v: &Value) -> Result<bool, String> {
            self.kv.borrow_mut().insert(key.to_owned(), v.clone());
            Ok(true)
        }
        fn admin_fos(&self, request: FosRequest<'_>) -> Result<FosResponse, String> {
            let rem = self.remaining.get();
            if rem == 0 {
                return Err("backend_budget_exhausted".into());
            }
            self.remaining.set(rem - 1);
            let key = request.key.trim_start_matches('/');
            if request.method == "PUT" {
                self.objects
                    .borrow_mut()
                    .insert(key.to_owned(), request.body);
                return Ok(FosResponse {
                    status: 200,
                    body: Vec::new(),
                    cdn_cache: None,
                    retry_after: None,
                });
            }
            if request.method == "GET" {
                self.gets.set(self.gets.get() + 1);
            }
            match self.objects.borrow().get(key) {
                Some(body) => Ok(FosResponse {
                    status: 200,
                    body: body.clone(),
                    cdn_cache: None,
                    retry_after: None,
                }),
                None => Ok(FosResponse {
                    status: 404,
                    body: Vec::new(),
                    cdn_cache: None,
                    retry_after: None,
                }),
            }
        }
        fn admin_fos_etag(
            &self,
            request: FosRequest<'_>,
        ) -> Result<(FosResponse, Option<String>), String> {
            use md5::{Digest as _, Md5};
            let etag = if request.method == "PUT" {
                Some(hex::encode(Md5::digest(&request.body)))
            } else {
                None
            };
            self.admin_fos(request).map(|resp| (resp, etag))
        }
        fn admin_remaining_calls(&self) -> u32 {
            self.remaining.get()
        }
    }

    fn test_service() -> Service {
        serde_json::from_value(json!({
            "service_id": "svc", "name": "n", "status": "active", "created_at": 0, "updated_at": 0,
            "config_version": 1, "logging_enabled": true, "logging_endpoint_name": "e",
            "log_period": 120, "sample_rate": 1, "edge_only": false, "custom_condition": "",
            "fos_bucket": "fos-abcdefghijklmnopqrst12-logs", "fos_prefix": "", "fos_region": "us-east-1",
            "fos_endpoint": "x", "log_fields": [], "field_limits": {}, "fastly_token_ref": "t",
            "fos_access_key_ref": "ak", "fos_secret_key_ref": "sk", "owner": "o",
            "bucket_adopted": false, "bucket_checked": false, "last_activated_version": null,
            "last_reconcile": null, "pending_job_id": null
        }))
        .unwrap()
    }

    #[test]
    fn iter36_f7_catalog_prepare_proof_rejects_cycle_and_mismatched_block_metadata() {
        let svc = test_service();
        let hour = 1_800_000_000i64; // 2027-01-15T08:00:00Z
        let day = day_start(hour);
        let day_key = "processed/_catalog/day-proof.json".to_owned();
        let key_a = "processed/_catalog/proof-block-a.json".to_owned();
        let key_b = "processed/_catalog/proof-block-b.json".to_owned();
        let target_file =
            "processed/request/year=2027/month=01/day=15/hour=08/window=1800000000-1800000120-0.parquet"
                .to_owned();
        let day_index = Day {
            version: VERSION,
            service_id: Some(svc.service_id.clone()),
            hours: BTreeMap::from([(
                hour,
                Reference {
                    key: key_a.clone(),
                    generation: 5,
                },
            )]),
            ..Default::default()
        };
        let head = Head {
            version: VERSION,
            generation: 5,
            recent: vec![Event {
                cursor: "5".into(),
                generation: 5,
                committed_at: None,
                manifest: None,
            }],
            days: BTreeMap::from([(day, day_key.clone())]),
            ..Default::default()
        };
        let wm = json!({ "catalog": head });

        // 1. 2-node equal-gen cycle A (gen 5) -> B (gen 5) -> A (gen 5) during replacement proof
        let blk_a = Block {
            version: VERSION,
            service_id: svc.service_id.clone(),
            generation: 5,
            hour,
            previous: Some(Reference {
                key: key_b.clone(),
                generation: 5,
            }),
            files: vec![],
            summaries: vec![],
            replaces: vec![],
            committed_at: BTreeMap::new(),
        };
        let blk_b = Block {
            version: VERSION,
            service_id: svc.service_id.clone(),
            generation: 5,
            hour,
            previous: Some(Reference {
                key: key_a.clone(),
                generation: 5,
            }),
            files: vec![],
            summaries: vec![],
            replaces: vec![],
            committed_at: BTreeMap::new(),
        };
        let rt = MockRt {
            kv: RefCell::new(BTreeMap::new()),
            objects: RefCell::new(BTreeMap::from([
                (day_key.clone(), serde_json::to_vec(&day_index).unwrap()),
                (key_a.clone(), serde_json::to_vec(&blk_a).unwrap()),
                (key_b.clone(), serde_json::to_vec(&blk_b).unwrap()),
            ])),
            remaining: Cell::new(24),
            gets: Cell::new(0),
        };
        let store = Store {
            runtime: &rt,
            service: &svc,
            access_key: "ak".into(),
            secret_key: "sk".into(),
            started: Instant::now(),
            vcpu_start: 0,
        };
        let res = prepare_reserving(
            &store,
            &wm,
            "compaction",
            "v2",
            &[],
            &[],
            &[],
            std::slice::from_ref(&target_file),
            0,
        );
        assert_eq!(res.err().as_deref(), Some("catalog_parent_invalid"));
        assert!(
            rt.gets.get() <= 4,
            "cycle in proof must terminate immediately, gets={}",
            rt.gets.get()
        );

        // 2. Foreign service_id / wrong version / wrong hour / wrong generation on proof block -> catalog_manifest_invalid!
        for (label, bad_blk) in [
            (
                "foreign_sid",
                Block {
                    service_id: "foreignService".into(),
                    previous: None,
                    ..blk_a.clone()
                },
            ),
            (
                "wrong_version",
                Block {
                    version: VERSION + 1,
                    previous: None,
                    ..blk_a.clone()
                },
            ),
            (
                "wrong_hour",
                Block {
                    hour: hour + 3600,
                    previous: None,
                    ..blk_a.clone()
                },
            ),
            (
                "wrong_gen",
                Block {
                    generation: 4,
                    previous: None,
                    ..blk_a.clone()
                },
            ),
        ] {
            rt.kv.borrow_mut().clear();
            rt.objects
                .borrow_mut()
                .insert(key_a.clone(), serde_json::to_vec(&bad_blk).unwrap());
            rt.remaining.set(24);
            let err = prepare_reserving(
                &store,
                &wm,
                "compaction",
                "v2",
                &[],
                &[],
                &[],
                std::slice::from_ref(&target_file),
                0,
            )
            .err();
            assert_eq!(
                err.as_deref(),
                Some("catalog_manifest_invalid"),
                "iter36 F7 ({label}): proof block metadata mismatch must be rejected!"
            );
        }
    }

    #[test]
    fn iter37_prepare_reserving_validates_proof_head_gen_and_parent_block_merge() {
        let svc = test_service();
        let hour = 1_800_000_000i64; // 2027-01-15T08:00:00Z
        let day = day_start(hour);
        let day_key = "processed/_catalog/day-iter37.json".to_owned();
        let key_a = "processed/_catalog/block-iter37-a.json".to_owned();
        let key_b = "processed/_catalog/block-iter37-b.json".to_owned();
        let target_file =
            "processed/request/year=2027/month=01/day=15/hour=08/window=1800000000-1800000120-0.parquet"
                .to_owned();
        let new_file = Output {
            key: "processed/request/year=2027/month=01/day=15/hour=08/window=1800000120-1800000240-0.parquet"
                .to_owned(),
            bytes: 128,
            rows: 10,
            ..Default::default()
        };
        let head = Head {
            version: VERSION,
            generation: 5,
            recent: vec![Event {
                cursor: "5".into(),
                generation: 5,
                committed_at: None,
                manifest: None,
            }],
            days: BTreeMap::from([(day, day_key.clone())]),
            ..Default::default()
        };
        let wm = json!({ "catalog": serde_json::to_value(&head).unwrap() });

        let rt = MockRt {
            kv: RefCell::new(BTreeMap::new()),
            objects: RefCell::new(BTreeMap::new()),
            remaining: Cell::new(32),
            gets: Cell::new(0),
        };
        let store = Store {
            runtime: &rt,
            service: &svc,
            access_key: "ak".into(),
            secret_key: "sk".into(),
            started: Instant::now(),
            vcpu_start: 0,
        };

        // 1. Proof loop: current.generation (6) > head.generation (5) must be rejected.
        {
            let day_gt = Day {
                version: VERSION,
                service_id: Some(svc.service_id.clone()),
                hours: BTreeMap::from([(
                    hour,
                    Reference {
                        key: key_a.clone(),
                        generation: 6,
                    },
                )]),
                ..Default::default()
            };
            let blk_gt = Block {
                version: VERSION,
                service_id: svc.service_id.clone(),
                generation: 6,
                hour,
                previous: None,
                files: vec![Output {
                    key: target_file.clone(),
                    ..new_file.clone()
                }],
                summaries: vec![],
                replaces: vec![],
                committed_at: BTreeMap::new(),
            };
            rt.kv.borrow_mut().clear();
            rt.objects.borrow_mut().clear();
            rt.objects
                .borrow_mut()
                .insert(day_key.clone(), serde_json::to_vec(&day_gt).unwrap());
            rt.objects
                .borrow_mut()
                .insert(key_a.clone(), serde_json::to_vec(&blk_gt).unwrap());
            rt.remaining.set(32);
            let err = prepare_reserving(
                &store,
                &wm,
                "compaction",
                "v2",
                std::slice::from_ref(&new_file),
                &[],
                &[],
                std::slice::from_ref(&target_file),
                0,
            )
            .err();
            assert_eq!(
                err.as_deref(),
                Some("catalog_manifest_invalid"),
                "iter37: proof loop must reject current.generation > head.generation!"
            );
        }

        // 2. Parent block merge (no `replaces`, so proof loop is skipped):
        //    validate version, service_id, hour, generation mismatch, generation > head.generation,
        //    self-loop previous, and non-monotonic previous.generation > parent.generation.
        let base_blk = Block {
            version: VERSION,
            service_id: svc.service_id.clone(),
            generation: 5,
            hour,
            previous: None,
            files: vec![],
            summaries: vec![],
            replaces: vec![],
            committed_at: BTreeMap::new(),
        };
        let cases: Vec<(&str, u64, Block, &str)> = vec![
            (
                "foreign_sid",
                5,
                Block {
                    service_id: "foreignService".into(),
                    ..base_blk.clone()
                },
                "catalog_manifest_invalid",
            ),
            (
                "wrong_version",
                5,
                Block {
                    version: VERSION + 1,
                    ..base_blk.clone()
                },
                "catalog_manifest_invalid",
            ),
            (
                "wrong_hour",
                5,
                Block {
                    hour: hour + 3600,
                    ..base_blk.clone()
                },
                "catalog_manifest_invalid",
            ),
            (
                "gen_mismatch",
                5,
                Block {
                    generation: 4,
                    ..base_blk.clone()
                },
                "catalog_manifest_invalid",
            ),
            (
                "gen_exceeds_head",
                6,
                Block {
                    generation: 6,
                    ..base_blk.clone()
                },
                "catalog_manifest_invalid",
            ),
            (
                "self_loop_previous",
                5,
                Block {
                    previous: Some(Reference {
                        key: key_a.clone(),
                        generation: 5,
                    }),
                    ..base_blk.clone()
                },
                "catalog_parent_invalid",
            ),
            (
                "monotonic_violation_previous",
                5,
                Block {
                    previous: Some(Reference {
                        key: key_b.clone(),
                        generation: 6,
                    }),
                    ..base_blk.clone()
                },
                "catalog_parent_invalid",
            ),
        ];
        for (label, ref_gen, bad_parent, expected_err) in cases {
            let day_index = Day {
                version: VERSION,
                service_id: Some(svc.service_id.clone()),
                hours: BTreeMap::from([(
                    hour,
                    Reference {
                        key: key_a.clone(),
                        generation: ref_gen,
                    },
                )]),
                ..Default::default()
            };
            rt.kv.borrow_mut().clear();
            rt.objects.borrow_mut().clear();
            rt.objects
                .borrow_mut()
                .insert(day_key.clone(), serde_json::to_vec(&day_index).unwrap());
            rt.objects
                .borrow_mut()
                .insert(key_a.clone(), serde_json::to_vec(&bad_parent).unwrap());
            rt.remaining.set(32);
            let err = prepare_reserving(
                &store,
                &wm,
                "ingest",
                "v2",
                std::slice::from_ref(&new_file),
                &[],
                &[],
                &[],
                0,
            )
            .err();
            assert_eq!(
                err.as_deref(),
                Some(expected_err),
                "iter37 ({label}): parent block merge must reject invalid parent!"
            );
        }
    }

    #[test]
    fn iter39_f1_catalog_prepare_and_retire_validate_day_version_and_service_id() {
        let mut svc = test_service();
        svc.log_retention_days = 1;
        let hour = 1_800_000_000i64; // 2027-01-15T08:00:00Z
        let day = day_start(hour);
        let day_key = "processed/_catalog/day-iter39.json".to_owned();
        let new_file = Output {
            key: "processed/request/year=2027/month=01/day=15/hour=08/window=1800000120-1800000240-0.parquet"
                .into(),
            rows: 5,
            bytes: 128,
            md5: String::new(),
            metadata: None,
        };
        let wm = json!({
            "catalog": Head {
                version: VERSION,
                generation: 5,
                recent: vec![Event {
                    cursor: "svc:5".into(),
                    generation: 5,
                    committed_at: None,
                    manifest: None,
                }],
                days: BTreeMap::from([(day, day_key.clone())]),
                ..Default::default()
            }
        });
        let rt = MockRt {
            kv: RefCell::new(BTreeMap::new()),
            objects: RefCell::new(BTreeMap::new()),
            remaining: Cell::new(32),
            gets: Cell::new(0),
        };
        let store = Store {
            runtime: &rt,
            service: &svc,
            access_key: "ak".into(),
            secret_key: "sk".into(),
            started: Instant::now(),
            vcpu_start: 0,
        };

        // 1. prepare_reserving: reject foreign service_id and unsupported version; accept legacy None.
        let prepare_reject_cases = [
            (
                "foreign_service_id",
                Day {
                    version: VERSION,
                    service_id: Some("foreign-svc".into()),
                    ..Default::default()
                },
                "catalog_manifest_invalid",
            ),
            (
                "unsupported_version",
                Day {
                    version: VERSION + 1,
                    service_id: Some(svc.service_id.clone()),
                    ..Default::default()
                },
                "catalog_version_unsupported",
            ),
        ];
        for (label, bad_day, expected_err) in prepare_reject_cases {
            rt.kv.borrow_mut().clear();
            rt.objects.borrow_mut().clear();
            rt.objects
                .borrow_mut()
                .insert(day_key.clone(), serde_json::to_vec(&bad_day).unwrap());
            rt.remaining.set(32);
            let err = prepare_reserving(
                &store,
                &wm,
                "ingest",
                "v2",
                std::slice::from_ref(&new_file),
                &[],
                &[],
                &[],
                0,
            )
            .err();
            assert_eq!(
                err.as_deref(),
                Some(expected_err),
                "iter39 ({label}): prepare_reserving must reject invalid Day without laundering"
            );
            assert_eq!(
                rt.objects.borrow().len(),
                1,
                "iter39 ({label}): prepare_reserving must not write laundered Day/Block objects"
            );
        }

        // Legacy v2 Day with service_id: None must remain accepted by prepare_reserving.
        let legacy_day = Day {
            version: VERSION,
            service_id: None,
            ..Default::default()
        };
        rt.kv.borrow_mut().clear();
        rt.objects.borrow_mut().clear();
        rt.objects
            .borrow_mut()
            .insert(day_key.clone(), serde_json::to_vec(&legacy_day).unwrap());
        rt.remaining.set(32);
        let prepared = prepare_reserving(
            &store,
            &wm,
            "ingest",
            "v2",
            std::slice::from_ref(&new_file),
            &[],
            &[],
            &[],
            0,
        );
        assert!(
            prepared.is_ok(),
            "iter39: prepare_reserving must accept legacy v2 Day with service_id: None, got {:?}",
            prepared.err()
        );

        // 2. retire_reserving: reject foreign service_id and unsupported version; accept legacy None.
        let cutoff =
            (rt.now() as i64 - i64::from(svc.log_retention_days) * 86_400).div_euclid(3600) * 3600;
        let cutoff_day = day_start(cutoff);
        let retire_day_key = "processed/_catalog/day-retire-iter39.json".to_owned();
        let retire_wm = json!({
            "catalog": Head {
                version: VERSION,
                generation: 5,
                recent: vec![Event {
                    cursor: "svc:5".into(),
                    generation: 5,
                    committed_at: None,
                    manifest: None,
                }],
                days: BTreeMap::from([(cutoff_day, retire_day_key.clone())]),
                ..Default::default()
            }
        });
        let retire_reject_cases = [
            (
                "foreign_service_id",
                Day {
                    version: VERSION,
                    service_id: Some("foreign-svc".into()),
                    hours: BTreeMap::from([(
                        cutoff - 3600,
                        Reference {
                            key: "processed/_catalog/old-blk.json".into(),
                            generation: 1,
                        },
                    )]),
                    ..Default::default()
                },
                "catalog_manifest_invalid",
            ),
            (
                "unsupported_version",
                Day {
                    version: VERSION + 1,
                    service_id: Some(svc.service_id.clone()),
                    hours: BTreeMap::from([(
                        cutoff - 3600,
                        Reference {
                            key: "processed/_catalog/old-blk.json".into(),
                            generation: 1,
                        },
                    )]),
                    ..Default::default()
                },
                "catalog_version_unsupported",
            ),
        ];
        for (label, bad_day, expected_err) in retire_reject_cases {
            rt.kv.borrow_mut().clear();
            rt.kv
                .borrow_mut()
                .insert(WATERMARK_KEY.into(), retire_wm.clone());
            rt.objects.borrow_mut().clear();
            rt.objects.borrow_mut().insert(
                retire_day_key.clone(),
                serde_json::to_vec(&bad_day).unwrap(),
            );
            rt.remaining.set(32);
            let err = retire_reserving(&store, 0).err();
            assert_eq!(
                err.as_deref(),
                Some(expected_err),
                "iter39 ({label}): retire_reserving must reject invalid Day without laundering"
            );
            assert_eq!(
                rt.objects.borrow().len(),
                1,
                "iter39 ({label}): retire_reserving must not write laundered Day objects"
            );
        }

        // Legacy v2 Day with service_id: None must remain accepted by retire_reserving.
        let legacy_retire_day = Day {
            version: VERSION,
            service_id: None,
            hours: BTreeMap::from([(
                cutoff,
                Reference {
                    key: "processed/_catalog/kept-blk.json".into(),
                    generation: 2,
                },
            )]),
            ..Default::default()
        };
        rt.kv.borrow_mut().clear();
        rt.kv.borrow_mut().insert(WATERMARK_KEY.into(), retire_wm);
        rt.objects.borrow_mut().clear();
        rt.objects.borrow_mut().insert(
            retire_day_key,
            serde_json::to_vec(&legacy_retire_day).unwrap(),
        );
        rt.remaining.set(32);
        let retired = retire_reserving(&store, 0);
        assert!(
            retired.is_ok(),
            "iter39: retire_reserving must accept legacy v2 Day with service_id: None, got {:?}",
            retired.err()
        );
    }

    #[test]
    fn opt33_fetch_batched_reserving_skips_caching_transient_prefetch_and_retries_primary() {
        struct RetryPrefetchRt {
            inner: MockRt,
            attempts: RefCell<BTreeMap<String, u32>>,
        }
        impl SetupStores for RetryPrefetchRt {
            fn config_present(&self, n: &str) -> Result<bool, String> {
                self.inner.config_present(n)
            }
            fn secret_present(&self, n: &str) -> Result<bool, String> {
                self.inner.secret_present(n)
            }
            fn state_linked(&self) -> Result<bool, String> {
                self.inner.state_linked()
            }
        }
        impl Runtime for RetryPrefetchRt {
            fn config(&self, k: &str) -> Result<Option<String>, String> {
                self.inner.config(k)
            }
            fn secret(&self, k: &str) -> Result<Option<String>, String> {
                self.inner.secret(k)
            }
            fn random(&self) -> Result<[u8; 32], String> {
                self.inner.random()
            }
            fn now(&self) -> u64 {
                self.inner.now()
            }
            fn checks(&self, l: bool) -> Result<Value, String> {
                self.inner.checks(l)
            }
            fn versions(&self) -> Result<Value, String> {
                self.inner.versions()
            }
            fn compute_service_id(&self) -> Option<String> {
                self.inner.compute_service_id()
            }
            fn compute_service_version(&self) -> Option<String> {
                self.inner.compute_service_version()
            }
            fn admin_get(&self, key: &str) -> Result<Option<Record>, String> {
                self.inner.admin_get(key)
            }
            fn admin_cas(&self, k: &str, g: Option<&str>, v: &Value) -> Result<bool, String> {
                self.inner.admin_cas(k, g, v)
            }
            fn admin_wait(&self, _: u64) {}
            fn admin_remaining_calls(&self) -> u32 {
                self.inner.admin_remaining_calls()
            }
            fn admin_fos_concurrent(&self) -> bool {
                true
            }
            fn heap_mib(&self) -> Option<u32> {
                Some(16)
            }
            fn admin_fos(&self, request: FosRequest<'_>) -> Result<FosResponse, String> {
                let key = request.key.trim_start_matches('/').to_owned();
                let count = {
                    let mut map = self.attempts.borrow_mut();
                    let next = map.get(&key).copied().unwrap_or(0) + 1;
                    map.insert(key, next);
                    next
                };
                if count == 1 {
                    return Ok(FosResponse {
                        status: 503,
                        body: Vec::new(),
                        cdn_cache: None,
                        retry_after: None,
                    });
                }
                self.inner.admin_fos(request)
            }
        }

        let svc = test_service();
        let k1 = "processed/_catalog/day-1.json".to_owned();
        let k2 = "processed/_catalog/day-2.json".to_owned();
        let rt = RetryPrefetchRt {
            inner: MockRt {
                kv: RefCell::new(BTreeMap::new()),
                objects: RefCell::new(BTreeMap::from([
                    (k1.clone(), b"{\"day\":1}".to_vec()),
                    (k2.clone(), b"{\"day\":2}".to_vec()),
                ])),
                remaining: Cell::new(32),
                gets: Cell::new(0),
            },
            attempts: RefCell::new(BTreeMap::new()),
        };
        let store = Store {
            runtime: &rt,
            service: &svc,
            access_key: "ak".into(),
            secret_key: "sk".into(),
            started: Instant::now(),
            vcpu_start: 0,
        };
        let mut prefetched = Prefetched::new();
        let first =
            fetch_batched_reserving(&store, &mut prefetched, &k1, MANIFEST_LIMIT, 8, 3, || {
                vec![(k2.clone(), MANIFEST_LIMIT)]
            })
            .expect("primary transient 503 must recover on direct retry");
        assert_eq!(first.as_deref(), Some(b"{\"day\":1}".as_slice()));
        assert!(
            !prefetched.contains_key(&k2),
            "transient 503 from speculative prefetch must not be cached"
        );
        let second =
            fetch_batched_reserving(&store, &mut prefetched, &k2, MANIFEST_LIMIT, 8, 3, Vec::new)
                .expect("prefetched transient 503 must recover when requested");
        assert_eq!(second.as_deref(), Some(b"{\"day\":2}".as_slice()));
    }
}
