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
    pub day_bundles: BTreeMap<i64, Reference>,
    #[serde(default)]
    pub bundle_retired: BTreeMap<String, u64>,
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
    if let Some(result) = prefetched.remove(key) {
        return result;
    }
    let unread = store.runtime.admin_unread(&store.service.service_id);
    let room = (store.runtime.admin_remaining_calls() as usize)
        .saturating_sub(4)
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
    let needed = results
        .next()
        .unwrap_or_else(|| Err("catalog_object_not_visible".into()));
    for ((key, _), result) in batch.iter().skip(1).zip(results) {
        if !matches!(&result, Err(error) if error == crate::shell::PREFETCH_UNREAD) {
            prefetched.insert(key.clone(), result);
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

fn put_limit<T: Serialize, R: Runtime>(
    store: &Store<'_, R>,
    value: &T,
    limit: usize,
) -> Result<String, String> {
    let body = serde_json::to_vec(value).map_err(|e| e.to_string())?;
    if body.len() > limit {
        return Err("catalog_manifest_too_large".into());
    }
    let digest = hex::encode(Sha256::digest(&body));
    let key = format!("{}processed/_catalog/{digest}.json", scope(store.service));
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
    manifest: Option<String>,
    #[serde(default)]
    proofs: BTreeMap<i64, Proof>,
    #[serde(default)]
    blocks: BTreeMap<i64, Reference>,
    #[serde(default)]
    offsets: BTreeMap<i64, usize>,
}

#[derive(Serialize, Deserialize)]
struct Proof {
    remaining: BTreeSet<String>,
    current: Option<Reference>,
    #[serde(default)]
    rows: u64,
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
            .map(|key| read(store, key))
            .transpose()?
            .unwrap_or_else(|| Day {
                version: VERSION,
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
                .ok_or("catalog_replacement_not_active")?;
            let parent: Block = read(store, &current.key)?;
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
                let parent: Block = read(store, &reference.key)?;
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
    if work.manifest.is_none() {
        if runtime.admin_remaining_calls() <= reserve + 1 || !admitted(store) {
            return pending(&mut work);
        }
        work.manifest = Some(put_limit(store, &manifest, 4 * 1024 * 1024)?);
    }
    let key = work.manifest.clone().ok_or("catalog_preparation_invalid")?;
    for day in &days {
        if work.published.contains_key(day) {
            continue;
        }
        if runtime.admin_remaining_calls() <= reserve + 1 || !admitted(store) {
            return pending(&mut work);
        }
        let mut index = work.days[day].clone();
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
        let now_s = runtime.now() as i64;
        for file in files
            .iter()
            .filter(|file| file_hour(&file.key).map(day_start) == Some(*day))
        {
            let (start, end) = file_bounds(&file.key).ok_or("catalog_file_partition_invalid")?;
            let (log_start, log_end) = file_log_bounds(file, start, end, now_s);
            index.statistics.earliest = Some(
                index
                    .statistics
                    .earliest
                    .map_or(start, |old| old.min(start)),
            );
            index.statistics.latest = Some(index.statistics.latest.map_or(end, |old| old.max(end)));
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
                    let (a, b) = file_bounds(&file.key).ok_or("catalog_file_partition_invalid")?;
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
        work.published.insert(*day, put(store, &index)?);
        work.days.insert(*day, index);
    }
    for day in &days {
        head.statistics
            .insert(*day, work.days[day].statistics.clone());
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
        let mut day: Day = read(store, key)?;
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
    let key = put(store, &manifest)?;
    catalog.days.retain(|day, _| *day >= cutoff_day);
    catalog.statistics.retain(|day, _| *day >= cutoff_day);
    if let Some(day) = partial {
        if day.hours.is_empty() {
            catalog.days.remove(&cutoff_day);
            catalog.statistics.remove(&cutoff_day);
        } else {
            catalog
                .statistics
                .insert(cutoff_day, day.statistics.clone());
            catalog.days.insert(cutoff_day, put(store, &day)?);
        }
    }
    catalog.hour_bundles.retain(|hour, _| *hour >= cutoff);
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
        ..Head::default()
    };
    (head, manifest)
}

pub fn retry_ms(period_s: u32) -> u64 {
    (u64::from(period_s) * 500).clamp(1000, 5000)
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
