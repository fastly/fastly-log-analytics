use super::{raw_key_minute, raw_prefix, scope, storage::Store};
use crate::{admin::Record, http, shell::Runtime};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

const LIMIT: usize = 512 * 1024;
const PREPARING: &str = "ingest:source_index_preparing";
const GROUPS_PER_STEP: usize = 8;
const FIELD: &str = "raw_sources";
const WORK_VCPU_MS: u64 = 40;
const REQUEST_LIMIT: usize = 2 * LIMIT;

#[derive(Serialize, Deserialize)]
struct Request {
    maintenance: String,
    service_id: String,
    region: String,
    bucket: String,
    prefix: String,
    plan_id: String,
    attempt: u32,
    base_generation: Option<String>,
    base_identity: String,
    root: Value,
    keys: Vec<String>,
    stop_ms: u64,
}

fn root_identity(root: &Value) -> Result<String, String> {
    Ok(hex::encode(Sha256::digest(
        serde_json::to_vec(root).map_err(|e| e.to_string())?,
    )))
}

pub fn prepare_remote<R: Runtime>(
    store: &Store<'_, R>,
    value: &Value,
    watermark: Option<&Record>,
    plan: &super::coordinator::Plan,
    reserve: u32,
    observed: impl FnOnce(&Result<Value, String>),
) -> Result<Value, String> {
    let remaining =
        super::coordinator::DRAIN_AT.saturating_sub(store.runtime.admin_elapsed(store.started));
    if store.runtime.admin_remaining_calls() < reserve + 3 || remaining < Duration::from_secs(2) {
        return Err("source_index_pending".into());
    }
    let root = watermark
        .map(|record| record.value.clone())
        .unwrap_or(Value::Null);
    let request = Request {
        maintenance: "source_index".into(),
        service_id: store.service.service_id.clone(),
        region: store.service.fos_region.clone(),
        bucket: store.service.fos_bucket.clone(),
        prefix: scope(store.service),
        plan_id: plan.plan_id.clone(),
        attempt: plan.attempt,
        base_generation: watermark.map(|record| record.generation.clone()),
        base_identity: root_identity(&root)?,
        root,
        keys: plan.raw_keys.clone(),
        stop_ms: remaining.saturating_sub(Duration::from_secs(1)).as_millis() as u64,
    };
    let body = serde_json::to_value(&request).map_err(|e| e.to_string())?;
    if serde_json::to_vec(&body).map_err(|e| e.to_string())?.len() > REQUEST_LIMIT {
        return Err("source_index_capacity".into());
    }
    let replies = store
        .runtime
        .worker_dispatch(&[(super::worker::PATH.into(), body)], remaining);
    let reply = replies
        .into_iter()
        .next()
        .unwrap_or_else(|| Err("source_index_pending".into()));
    observed(&reply);
    let reply = reply.map_err(|_| "source_index_pending")?;
    if reply["plan_id"] != request.plan_id
        || reply["attempt"] != request.attempt
        || reply["service_id"] != request.service_id
        || reply["bucket"] != request.bucket
        || reply["prefix"] != request.prefix
        || reply["base_generation"]
            != serde_json::to_value(&request.base_generation).map_err(|e| e.to_string())?
        || reply["base_identity"] != request.base_identity
    {
        return Err("source_index_reply_invalid".into());
    }
    match reply["status"].as_str() {
        Some("pending" | "stale") => return Err("source_index_pending".into()),
        Some("ok") => {}
        Some("error") => return Err("source_index_worker_failed".into()),
        _ => return Err("source_index_reply_invalid".into()),
    }
    let mut indexed = value.clone();
    indexed[FIELD] = reply[FIELD].clone();
    roots(&indexed)?;
    Ok(indexed)
}

pub(crate) fn handle_authorized_worker<R: Runtime>(
    runtime: &R,
    request: &http::Request,
    started: Instant,
) -> http::Response {
    if let Some(usage) = runtime.usage_agg() {
        usage.set_caller("cron:source_index");
    }
    let deny = || http::Response::new(400, "Invalid request").private();
    if request.body.len() > REQUEST_LIMIT {
        return deny();
    }
    let Ok(input) = serde_json::from_slice::<Request>(&request.body) else {
        return deny();
    };
    let prefix = format!("{}raw/request/", input.prefix);
    if input.maintenance != "source_index"
        || input.service_id.is_empty()
        || input.service_id.len() > 64
        || !input.service_id.bytes().all(|b| b.is_ascii_alphanumeric())
        || !crate::admin::REGIONS.contains(&input.region.as_str())
        || !crate::admin::valid_bucket(&input.bucket)
        || !super::retention::safe_scope(&input.prefix)
        || input.plan_id.len() != 12
        || !input.plan_id.bytes().all(|b| b.is_ascii_hexdigit())
        || input.attempt == 0
        || input.keys.is_empty()
        || input.keys.len() > super::SWEEP_LISTS * super::LIST_PAGE
        || input.keys.iter().any(|key| {
            key.len() > 1024
                || !key.starts_with(&prefix)
                || !super::is_raw_key(key)
                || key.contains("//")
                || key.chars().any(char::is_control)
                || key.split('/').any(|part| part == "." || part == "..")
        })
        || input.keys.windows(2).any(|pair| pair[0] >= pair[1])
        || input.stop_ms == 0
        || input.stop_ms > super::coordinator::DRAIN_AT.as_millis() as u64
        || root_identity(&input.root).ok().as_deref() != Some(input.base_identity.as_str())
    {
        return deny();
    }
    let before = runtime.admin_remaining_calls();
    let result = (|| -> Result<Value, String> {
        let current = runtime.admin_get(super::WATERMARK_KEY)?;
        let generation = current.as_ref().map(|record| &record.generation);
        let root = current
            .as_ref()
            .map(|record| record.value.clone())
            .unwrap_or(Value::Null);
        if generation != input.base_generation.as_ref()
            || root_identity(&root)? != input.base_identity
        {
            return Err("source_index_base_mismatch".into());
        }
        let plan = runtime.admin_get(super::PLAN_KEY)?;
        if plan.as_ref().is_none_or(|record| {
            record.value["plan_id"] != input.plan_id
                || record.value["attempt"] != input.attempt
                || !record.value["abandoning"].is_null()
                || record.value["raw_keys"] != json!(input.keys)
        }) {
            return Err("source_index_stale".into());
        }
        let service = super::worker::worker_service(
            &input.service_id,
            &input.region,
            &input.bucket,
            &input.prefix,
        )?;
        let (access_key, secret_key) =
            crate::admin::fos_credentials(runtime, &service).ok_or("missing_fos_credentials")?;
        let store = Store {
            runtime,
            service: &service,
            access_key,
            secret_key,
            started,
            vcpu_start: 0,
        };
        prepare(
            &store,
            &input.root,
            &input.plan_id,
            &input.keys,
            2,
            Duration::from_millis(input.stop_ms),
        )
    })();
    let mut reply = json!({
        "plan_id": input.plan_id, "attempt": input.attempt,
        "service_id": input.service_id, "bucket": input.bucket, "prefix": input.prefix,
        "base_generation": input.base_generation, "base_identity": input.base_identity,
        "calls": before.saturating_sub(runtime.admin_remaining_calls()),
        "kv_ops": runtime.kv_ops(),
        "duration_ms": started.elapsed().as_millis() as u64,
        "heap_mib": runtime.heap_mib(),
    });
    match result {
        Ok(indexed) => {
            reply["status"] = "ok".into();
            reply[FIELD] = indexed[FIELD].clone();
        }
        Err(error) if error == "source_index_pending" => reply["status"] = "pending".into(),
        Err(error) if error == "source_index_base_mismatch" => {
            reply["status"] = "stale".into();
            reply["noop_reason"] = error.into();
        }
        Err(error) if error == "source_index_stale" => reply["status"] = "stale".into(),
        Err(_) => reply["status"] = "error".into(),
    }
    if let Some(usage) = runtime.usage_agg() {
        reply["usage"] = usage
            .drain_rows(&crate::auth::settings::UsageLogging::default())
            .into();
    }
    let metrics = crate::telemetry::execution::ExecutionMetrics::snapshot(runtime, started);
    reply["vcpu_ms"] = metrics.vcpu_ms.into();
    reply["execution_metrics"] = json!(metrics);
    http::Response::new(200, reply.to_string())
        .header("content-type", "application/json")
        .private()
}

type Directory = BTreeMap<i64, String>;
type Hour = BTreeMap<i64, Vec<Page>>;

#[derive(Clone, Serialize, Deserialize)]
struct Page {
    through: String,
    key: String,
}

#[derive(Serialize, Deserialize)]
struct Preparing {
    identity: String,
    days: Directory,
    done: usize,
}

fn roots(root: &Value) -> Result<Directory, String> {
    if root[FIELD].is_null() {
        return Ok(Directory::new());
    }
    serde_json::from_value(root[FIELD].clone()).map_err(|_| "source_index_invalid".into())
}

fn read<T: DeserializeOwned, R: Runtime>(store: &Store<'_, R>, key: &str) -> Result<T, String> {
    let prefix = format!("{}processed/_sources/", scope(store.service));
    if !key.starts_with(&prefix) {
        return Err("source_index_scope".into());
    }
    let bytes = store
        .get_direct(key, LIMIT)?
        .ok_or("source_index_not_visible")?;
    serde_json::from_slice(&bytes).map_err(|_| "source_index_invalid".into())
}

fn optional<T: DeserializeOwned + Default, R: Runtime>(
    store: &Store<'_, R>,
    key: Option<&String>,
) -> Result<T, String> {
    key.map_or_else(|| Ok(T::default()), |key| read(store, key))
}

fn put<T: Serialize, R: Runtime>(store: &Store<'_, R>, value: &T) -> Result<String, String> {
    let body = serde_json::to_vec(value).map_err(|e| e.to_string())?;
    if body.len() > LIMIT {
        return Err("source_index_capacity".into());
    }
    let digest = hex::encode(Sha256::digest(&body));
    let key = format!("{}processed/_sources/{digest}.json", scope(store.service));
    store.put_verified(&key, body)?;
    Ok(key)
}

fn partition(key: &str, prefix: &str) -> Option<(i64, i64, i64)> {
    let minute = raw_key_minute(key, prefix)?;
    Some((
        minute - minute.rem_euclid(86_400),
        minute - minute.rem_euclid(3600),
        minute,
    ))
}

/// Checks a bounded prefix of a LIST page. An unreadable receipt is never an absent receipt.
/// The caller must persist progress only through the returned checked count.
pub fn covered<R: Runtime>(
    store: &Store<'_, R>,
    root: &Value,
    keys: &[String],
    reserve: u32,
) -> Result<(BTreeSet<String>, usize), String> {
    let days = roots(root)?;
    let prefix = raw_prefix(store.service);
    let mut day_cache: BTreeMap<i64, Directory> = BTreeMap::new();
    let mut hour_cache: BTreeMap<i64, Hour> = BTreeMap::new();
    let mut leaves: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut known = BTreeSet::new();
    let mut checked = 0;
    for key in keys {
        if store.runtime.admin_remaining_calls() < reserve + 3 || !store.within_wall() {
            break;
        }
        let Some((day, hour, minute)) = partition(key, &prefix) else {
            checked += 1;
            continue;
        };
        if (!day_cache.contains_key(&day) && day_cache.len() == 4)
            || (!hour_cache.contains_key(&hour) && hour_cache.len() == 4)
        {
            break;
        }
        if let std::collections::btree_map::Entry::Vacant(entry) = day_cache.entry(day) {
            entry.insert(optional(store, days.get(&day))?);
        }
        if let std::collections::btree_map::Entry::Vacant(entry) = hour_cache.entry(hour) {
            entry.insert(optional(store, day_cache[&day].get(&hour))?);
        }
        if let Some(pages) = hour_cache[&hour].get(&minute) {
            if let Some(page) = pages.iter().find(|page| key <= &page.through) {
                if !leaves.contains_key(&page.key) {
                    // Bound retained decoded pages as well as network calls.
                    if leaves.len() == 4 {
                        break;
                    }
                    leaves.insert(page.key.clone(), read(store, &page.key)?);
                }
                if leaves[&page.key].contains(key) {
                    known.insert(key.clone());
                }
            }
        }
        checked += 1;
    }
    Ok((known, checked))
}

/// Immutable receipts are published by the ingestion watermark CAS, together with the catalog.
/// A separate checkpoint makes larger batches resumable without holding a maintenance lease.
pub fn prepare<R: Runtime>(
    store: &Store<'_, R>,
    root: &Value,
    batch: &str,
    keys: &[String],
    reserve: u32,
    stop: Duration,
) -> Result<Value, String> {
    if store.runtime.admin_remaining_calls() <= reserve + 2 {
        return Err("source_index_pending".into());
    }
    let initial = roots(root)?;
    let identity = hex::encode(Sha256::digest(
        serde_json::to_vec(&(
            &store.service.service_id,
            &store.service.fos_bucket,
            scope(store.service),
            batch,
            &initial,
            keys,
        ))
        .map_err(|e| e.to_string())?,
    ));
    let record = store.runtime.admin_get(PREPARING)?;
    let saved: Option<Preparing> = record
        .as_ref()
        .map(|r| serde_json::from_value(r.value.clone()))
        .transpose()
        .map_err(|_| "source_index_preparation_invalid")?;
    let mut work = saved
        .filter(|saved| saved.identity == identity)
        .unwrap_or(Preparing {
            identity,
            days: initial,
            done: 0,
        });
    let prefix = raw_prefix(store.service);
    let vcpu_start = store.runtime.vcpu_ms();
    let mut groups = 0;
    let mut day_cache: BTreeMap<i64, Directory> = BTreeMap::new();
    let mut hour_cache: BTreeMap<i64, Hour> = BTreeMap::new();
    while work.done < keys.len() {
        let key = &keys[work.done];
        let Some((day, hour, minute)) = partition(key, &prefix) else {
            work.done += 1;
            continue;
        };
        let new_day = !day_cache.contains_key(&day);
        let new_hour = !hour_cache.contains_key(&hour);
        let reads = if new_day {
            if work.days.contains_key(&day) {
                3
            } else {
                0
            }
        } else if new_hour {
            if day_cache[&day].contains_key(&hour) {
                2
            } else {
                0
            }
        } else {
            u32::from(hour_cache[&hour].contains_key(&minute))
        };
        let flush =
            day_cache.len() + hour_cache.len() + usize::from(new_day) + usize::from(new_hour);
        if groups == GROUPS_PER_STEP
            || (new_day && day_cache.len() == 2)
            || (new_hour && hour_cache.len() == 2)
            || store.runtime.admin_remaining_calls() < reserve + flush as u32 + reads + 3
            || !store.within_wall()
            || store.runtime.admin_elapsed(store.started) >= stop
            || store.runtime.vcpu_ms().saturating_sub(vcpu_start) >= WORK_VCPU_MS
        {
            break;
        }
        if new_day {
            day_cache.insert(day, optional(store, work.days.get(&day))?);
        }
        if new_hour {
            hour_cache.insert(hour, optional(store, day_cache[&day].get(&hour))?);
        }
        let pages = hour_cache
            .get_mut(&hour)
            .unwrap()
            .entry(minute)
            .or_default();
        let at = pages
            .iter()
            .position(|page| key <= &page.through)
            .unwrap_or_else(|| pages.len().saturating_sub(1));
        let mut entries: BTreeSet<String> = optional(store, pages.get(at).map(|page| &page.key))?;
        let end = keys[work.done..]
            .iter()
            .take_while(|candidate| {
                partition(candidate, &prefix) == Some((day, hour, minute))
                    && (pages.is_empty()
                        || at + 1 == pages.len()
                        || *candidate <= &pages[at].through)
            })
            .count()
            + work.done;
        entries.extend(keys[work.done..end].iter().cloned());
        let mut replacement = Vec::new();
        let bytes = serde_json::to_vec(&entries).map_err(|e| e.to_string())?;
        if bytes.len() > LIMIT {
            let mut first = entries;
            let middle = first
                .iter()
                .nth(first.len() / 2)
                .cloned()
                .ok_or("source_index_capacity")?;
            let second = first.split_off(&middle);
            for entries in [first, second] {
                replacement.push(Page {
                    through: entries.last().cloned().ok_or("source_index_capacity")?,
                    key: put(store, &entries)?,
                });
            }
        } else {
            replacement.push(Page {
                through: entries.last().cloned().ok_or("source_index_capacity")?,
                key: put(store, &entries)?,
            });
        }
        if pages.is_empty() {
            pages.extend(replacement);
        } else {
            pages.splice(at..=at, replacement);
        }
        work.done = end;
        groups += 1;
    }
    for (hour, index) in hour_cache {
        let day = hour - hour.rem_euclid(86_400);
        day_cache
            .get_mut(&day)
            .unwrap()
            .insert(hour, put(store, &index)?);
    }
    for (day, index) in day_cache {
        work.days.insert(day, put(store, &index)?);
    }
    let encoded = serde_json::to_value(&work).map_err(|e| e.to_string())?;
    if serde_json::to_vec(&encoded)
        .map_err(|e| e.to_string())?
        .len()
        > LIMIT
    {
        return Err("source_index_capacity".into());
    }
    if !store.runtime.admin_cas(
        PREPARING,
        record.as_ref().map(|record| record.generation.as_str()),
        &encoded,
    )? {
        return Err("source_index_pending".into());
    }
    if work.done < keys.len() {
        return Err("source_index_pending".into());
    }
    let mut value = root.clone();
    value[FIELD] = serde_json::to_value(work.days).map_err(|e| e.to_string())?;
    Ok(value)
}
