use super::{
    admitted, already_sent,
    bundles::{self, COVERAGE_LIMIT},
    day_start, fetch_batched, file_bounds, file_hour, head, Block, Day, Head, Prefetched,
    Reference, MANIFEST_LIMIT, VERSION,
};
use crate::{
    ingest::{api::FILES_PAGE, rfc3339, storage::Store, worker::Output, WATERMARK_KEY},
    shell::Runtime,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

const STATE_LIMIT: usize = 4 * 1024 * 1024;
const IDENTITY_LIMIT: usize = 16_384;
pub const SNAPSHOT_SECONDS: u64 = 600;

#[derive(Default, Clone, Serialize, Deserialize)]
struct State {
    version: u32,
    service: String,
    from: i64,
    to: i64,
    summary: bool,
    generation: u64,
    created: u64,
    days: Vec<(i64, String)>,
    day: usize,
    hours: Vec<(i64, Reference)>,
    hour: usize,
    current: Option<Reference>,
    offset: usize,
    summary_offset: usize,
    retired: BTreeSet<String>,
    seen: BTreeSet<String>,
    summaries_seen: BTreeSet<String>,
    #[serde(default)]
    hour_bundles: BTreeMap<i64, Reference>,
    #[serde(default)]
    open_bundles: BTreeMap<i64, Reference>,
    #[serde(default)]
    day_bundles: BTreeMap<i64, Reference>,
    #[serde(default)]
    day_hours: BTreeMap<i64, Reference>,
    #[serde(default)]
    covered_windows: BTreeSet<String>,
    #[serde(default)]
    summary_ready: bool,
    #[serde(default)]
    summary_counts: BTreeMap<i64, u64>,
    #[serde(default)]
    day_bundle: Option<Reference>,
    #[serde(default)]
    day_bundle_ready: bool,
    #[serde(default)]
    summary_bounds: Option<(i64, i64)>,
    /// A fork after the first of its day, whose day bundle an earlier fork lists.
    #[serde(default)]
    day_bundle_listed: bool,
}

pub struct Page {
    pub files: Vec<Value>,
    pub summaries: Vec<Value>,
    pub next: Option<String>,
    pub generation: u64,
    pub open_hour: Option<i64>,
}

fn bounds(file: &Output) -> Option<(i64, i64)> {
    file_bounds(&file.key)
}

pub fn entry(file: &Output, generation: u64) -> Value {
    let (start, end) = bounds(file).unwrap_or((i64::MIN / 1000, i64::MAX / 1000));
    let mut value = json!({
        "key": file.key, "start": rfc3339(start), "end": rfc3339(end),
        "size": file.bytes, "rows": file.rows,
        "identity": if file.md5.is_empty() {file.key.clone()} else {file.md5.clone()},
        "catalog_generation": generation,
    });
    if let Some(metadata) = &file.metadata {
        value["metadata"] = serde_json::to_value(metadata).unwrap_or(Value::Null);
    }
    if let Some(name) = crate::telemetry::compact::parse_compact_name(&file.key) {
        value["sources"] = name.sources.into();
        value["span"] = json!([rfc3339(name.a as i64), rfc3339(name.b as i64)]);
    }
    if file
        .key
        .rsplit('/')
        .next()
        .is_some_and(|name| name.starts_with("summary="))
    {
        value["kind"] = "window".into();
        value["sources"] = 1.into();
    }
    if let Some(name) = crate::ingest::bundle::parse_name(&file.key) {
        value["kind"] = match name.kind {
            crate::ingest::bundle::NameKind::Hour | crate::ingest::bundle::NameKind::Open => "hour",
            crate::ingest::bundle::NameKind::Day => "day",
            crate::ingest::bundle::NameKind::Partial => return value,
        }
        .into();
        value["sources"] = name.count.into();
        value["span"] = json!([rfc3339(start), rfc3339(end)]);
    }
    value
}

fn included(file: &Output, hour: i64, from: i64, to: i64) -> bool {
    file_hour(&file.key) == Some(hour) && bounds(file).is_none_or(|(a, b)| a < to && b > from)
}

pub fn page<R: Runtime>(
    store: &Store<'_, R>,
    from: i64,
    to: i64,
    summary: bool,
    include_summaries: bool,
    checkpoint: Option<&str>,
) -> Result<Page, String> {
    pinned_page(
        store,
        from,
        to,
        summary,
        include_summaries,
        checkpoint,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn pinned_page<R: Runtime>(
    store: &Store<'_, R>,
    from: i64,
    to: i64,
    summary: bool,
    include_summaries: bool,
    checkpoint: Option<&str>,
    root: Option<Head>,
) -> Result<Page, String> {
    pinned_page_precise(
        store,
        from,
        to,
        summary,
        include_summaries,
        checkpoint,
        root,
        (from, to),
    )
}

/// Most catalog objects one discovery round sends concurrently.
const PREFETCH_LIMIT: usize = 16;

/// Reads `key` with the walk's upcoming reads alongside it; see [`super::fetch_batched`].
fn fetch<R: Runtime>(
    store: &Store<'_, R>,
    prefetched: &mut Prefetched,
    key: &str,
    limit: usize,
    upcoming: impl FnOnce() -> Vec<(String, usize)>,
) -> Result<Vec<u8>, String> {
    fetch_batched(store, prefetched, key, limit, PREFETCH_LIMIT, upcoming)?
        .ok_or_else(|| "catalog_object_not_visible".into())
}

/// The bundle and head-block reads the walk makes from the current hour on, in walk order,
/// stopping once the page would plausibly be full, then the next day's index.
fn upcoming(
    state: &State,
    include_summaries: bool,
    summary_bounds: (i64, i64),
) -> Vec<(String, usize)> {
    let mut keys = Vec::new();
    if !state.day_bundle_ready {
        if let Some(reference) = &state.day_bundle {
            keys.push((reference.key.clone(), COVERAGE_LIMIT));
        }
        return keys;
    }
    let mut estimate = 0;
    for (index, (hour, head)) in state.hours.iter().enumerate().skip(state.hour) {
        let current = index == state.hour;
        let selected = if state.summary || include_summaries {
            state.day_hours.get(hour).or_else(|| {
                if summary_bounds.0 <= *hour && summary_bounds.1 >= hour + 3600 {
                    state.hour_bundles.get(hour)
                } else {
                    None
                }
                .or_else(|| {
                    (summary_bounds.0 <= *hour)
                        .then(|| state.open_bundles.get(hour))
                        .flatten()
                })
            })
        } else {
            None
        };
        if let Some(reference) = selected.filter(|_| !(current && state.summary_ready)) {
            keys.push((reference.key.clone(), COVERAGE_LIMIT));
        }
        if !state.summary || selected.is_none() {
            if let Some(block) = if current {
                state.current.as_ref()
            } else {
                Some(head)
            } {
                keys.push((block.key.clone(), MANIFEST_LIMIT));
            }
        }
        let windows = state.summary_counts.get(hour).copied().unwrap_or(0) as usize;
        estimate += if state.summary && selected.is_some() {
            1
        } else {
            windows.max(1)
        };
        if estimate >= FILES_PAGE {
            return keys;
        }
    }
    if let Some((_, key)) = state.days.get(state.day) {
        keys.push((key.clone(), MANIFEST_LIMIT));
    }
    keys
}

#[allow(clippy::too_many_arguments)]
pub fn pinned_page_precise<R: Runtime>(
    store: &Store<'_, R>,
    from: i64,
    to: i64,
    summary: bool,
    include_summaries: bool,
    checkpoint: Option<&str>,
    root: Option<Head>,
    summary_bounds: (i64, i64),
) -> Result<Page, String> {
    let state = if let Some(token) = checkpoint {
        load(store, token, from, to, summary, summary_bounds)?
    } else {
        fresh(store, root, from, to, summary, summary_bounds)?
    };
    walk(
        store,
        state,
        include_summaries,
        summary_bounds,
        Prefetched::new(),
    )
}

/// The walk state a cursor token names: a checkpoint key, or `{key}#{i}` for fork `i` of a
/// forked listing's setup.
fn load<R: Runtime>(
    store: &Store<'_, R>,
    token: &str,
    from: i64,
    to: i64,
    summary: bool,
    summary_bounds: (i64, i64),
) -> Result<State, String> {
    let (key, fork) = match token.rsplit_once('#') {
        Some((key, index)) => (
            key,
            Some(
                index
                    .parse::<usize>()
                    .map_err(|_| "catalog_cursor_invalid")?,
            ),
        ),
        None => (token, None),
    };
    if !key.starts_with(&format!(
        "{}processed/_catalog/",
        crate::ingest::scope(store.service)
    )) {
        return Err("catalog_cursor_invalid".into());
    }
    let body = store
        .get_direct(key, STATE_LIMIT)?
        .ok_or("catalog_cursor_expired")?;
    let state = match fork {
        None => serde_json::from_slice::<State>(&body).ok(),
        Some(index) => serde_json::from_slice::<Vec<State>>(&body)
            .ok()
            .and_then(|forks| forks.into_iter().nth(index)),
    }
    .ok_or("catalog_cursor_invalid")?;
    if state.version != VERSION
        || state.service != store.service.service_id
        || state.from != from
        || state.to != to
        || state.summary != summary
        || state.summary_bounds.unwrap_or((state.from, state.to)) != summary_bounds
    {
        return Err("catalog_cursor_invalid".into());
    }
    if state.created.saturating_add(SNAPSHOT_SECONDS) <= store.runtime.now() {
        return Err("catalog_cursor_expired".into());
    }
    Ok(state)
}

fn fresh<R: Runtime>(
    store: &Store<'_, R>,
    root: Option<Head>,
    from: i64,
    to: i64,
    summary: bool,
    summary_bounds: (i64, i64),
) -> Result<State, String> {
    let root = if let Some(root) = root {
        root
    } else {
        let record = store.runtime.admin_get(WATERMARK_KEY)?;
        head(record.as_ref().map(|r| &r.value).unwrap_or(&Value::Null))?
    };
    let mut state = initial(
        root,
        store.service.service_id.clone(),
        from,
        to,
        summary,
        store.runtime.now(),
    );
    state.summary_bounds = Some(summary_bounds);
    Ok(state)
}

/// Starts walking `state` at its next day: the hours of `index` inside the range.
fn enter_day(
    state: &mut State,
    day: i64,
    index: Day,
    include_summaries: bool,
    summary_bounds: (i64, i64),
) {
    let (from, to) = (state.from, state.to);
    state.day_hours.clear();
    state.day_bundle = if (state.summary || include_summaries)
        && summary_bounds.0 <= day
        && summary_bounds.1 >= day + 86_400
    {
        state.day_bundles.get(&day).cloned()
    } else {
        None
    };
    state.day_bundle_ready = false;
    state.day_bundle_listed = false;
    state.summary_counts = index.summary_counts;
    state.hours = index
        .hours
        .into_iter()
        .filter(|(hour, _)| *hour < to && hour.saturating_add(3600) > from.saturating_sub(3600))
        .collect();
    state.hour = 0;
    state.current = state.hours.first().map(|(_, r)| r.clone());
    state.offset = 0;
    state.summary_offset = 0;
    state.retired.clear();
    state.seen.clear();
    state.summaries_seen.clear();
    state.summary_ready = false;
    state.covered_windows.clear();
}

fn walk<R: Runtime>(
    store: &Store<'_, R>,
    mut state: State,
    include_summaries: bool,
    summary_bounds: (i64, i64),
    mut prefetched: Prefetched,
) -> Result<Page, String> {
    let (from, to, summary) = (state.from, state.to, state.summary);
    let mut files = Vec::new();
    let mut summaries = Vec::new();
    let affordable = |prefetched: &Prefetched, key: &str| {
        (already_sent(store, prefetched, key) || store.runtime.admin_remaining_calls() > 3)
            && admitted(store)
    };
    while files.len() < FILES_PAGE && summaries.len() < FILES_PAGE {
        if !admitted(store) {
            break;
        }
        if state.hour >= state.hours.len() {
            let Some((day, key)) = state.days.get(state.day).cloned() else {
                break;
            };
            if !affordable(&prefetched, &key) {
                break;
            }
            let bytes = fetch(store, &mut prefetched, &key, MANIFEST_LIMIT, Vec::new)?;
            let index: Day =
                serde_json::from_slice(&bytes).map_err(|_| "catalog_object_invalid")?;
            if index.version != VERSION {
                return Err("catalog_version_unsupported".into());
            }
            enter_day(&mut state, day, index, include_summaries, summary_bounds);
            state.day += 1;
            if state.hours.is_empty() {
                continue;
            }
        }
        if !state.day_bundle_ready {
            if let Some(reference) = &state.day_bundle {
                if !affordable(&prefetched, &reference.key) {
                    break;
                }
                let bytes = fetch(
                    store,
                    &mut prefetched,
                    &reference.key,
                    COVERAGE_LIMIT,
                    || upcoming(&state, include_summaries, summary_bounds),
                )?;
                let coverage = bundles::parse(&bytes)?;
                state.day_hours = coverage.hours;
                if !state.day_bundle_listed {
                    if summary {
                        files.push(entry(&coverage.file, state.generation));
                    } else {
                        summaries.push(entry(&coverage.file, state.generation));
                    }
                }
            }
            state.day_bundle_ready = true;
        }
        if files.len() >= FILES_PAGE || summaries.len() >= FILES_PAGE {
            break;
        }
        let hour = state.hours[state.hour].0;
        if (summary || include_summaries) && !state.summary_ready {
            let day_covered = state.day_hours.contains_key(&hour);
            let mut is_open = false;
            let selected = state.day_hours.get(&hour).or_else(|| {
                if summary_bounds.0 <= hour && summary_bounds.1 >= hour + 3600 {
                    state.hour_bundles.get(&hour)
                } else {
                    None
                }
                .or_else(|| {
                    if summary_bounds.0 <= hour {
                        is_open = true;
                        state.open_bundles.get(&hour)
                    } else {
                        None
                    }
                })
            });
            state.covered_windows.clear();
            if let Some(reference) = selected {
                if !affordable(&prefetched, &reference.key) {
                    break;
                }
                let bytes = fetch(
                    store,
                    &mut prefetched,
                    &reference.key,
                    COVERAGE_LIMIT,
                    || upcoming(&state, include_summaries, summary_bounds),
                )?;
                let coverage = bundles::parse(&bytes)?;
                let open_fits = !is_open
                    || if coverage.windows.is_empty() {
                        file_bounds(&coverage.file.key)
                            .is_some_and(|(_, end)| summary_bounds.1 >= end)
                    } else {
                        coverage
                            .windows
                            .iter()
                            .all(|k| file_bounds(k).is_some_and(|(start, _)| start < to))
                    };
                if open_fits {
                    state.covered_windows = coverage.windows;
                    if !day_covered {
                        if summary {
                            files.push(entry(&coverage.file, state.generation));
                        } else {
                            summaries.push(entry(&coverage.file, state.generation));
                        }
                        if files.len() >= FILES_PAGE || summaries.len() >= FILES_PAGE {
                            break;
                        }
                    }
                }
            }
            state.summary_ready = true;
            if summary
                && !state.covered_windows.is_empty()
                && state.summary_counts.get(&hour).copied()
                    == Some(state.covered_windows.len() as u64)
            {
                state.current = None;
            }
        }
        let Some(reference) = state.current.clone() else {
            state.hour += 1;
            state.current = state.hours.get(state.hour).map(|(_, r)| r.clone());
            state.offset = 0;
            state.summary_offset = 0;
            state.retired.clear();
            state.seen.clear();
            state.summaries_seen.clear();
            state.summary_ready = false;
            state.covered_windows.clear();
            continue;
        };
        if !affordable(&prefetched, &reference.key) {
            break;
        }
        let bytes = fetch(
            store,
            &mut prefetched,
            &reference.key,
            MANIFEST_LIMIT,
            || upcoming(&state, include_summaries, summary_bounds),
        )?;
        let manifest: Block =
            serde_json::from_slice(&bytes).map_err(|_| "catalog_object_invalid")?;
        if manifest.version != VERSION
            || manifest.service_id != state.service
            || manifest.generation != reference.generation
            || reference.generation > state.generation
            || manifest.hour != hour
        {
            return Err("catalog_manifest_invalid".into());
        }
        if !summary {
            state.retired.extend(manifest.replaces.iter().cloned());
        }
        let outputs = if summary {
            &manifest.summaries
        } else {
            &manifest.files
        };
        while state.offset < outputs.len() && files.len() < FILES_PAGE {
            let file = &outputs[state.offset];
            state.offset += 1;
            if included(file, hour, from, to)
                && (!summary || !state.covered_windows.contains(&file.key))
                && !state.retired.contains(&file.key)
                && state.seen.insert(file.key.clone())
            {
                let mut value = entry(file, state.generation);
                value["committed_at"] = manifest.committed_at.get(&file.key).cloned().into();
                files.push(value);
            }
        }
        if include_summaries && !summary {
            while state.summary_offset < manifest.summaries.len() && summaries.len() < FILES_PAGE {
                let file = &manifest.summaries[state.summary_offset];
                state.summary_offset += 1;
                if included(file, hour, from, to)
                    && !state.covered_windows.contains(&file.key)
                    && state.summaries_seen.insert(file.key.clone())
                {
                    let mut value = entry(file, state.generation);
                    value["committed_at"] = manifest.committed_at.get(&file.key).cloned().into();
                    summaries.push(value);
                }
            }
        } else {
            state.summary_offset = manifest.summaries.len();
        }
        if state.offset == outputs.len() && state.summary_offset == manifest.summaries.len() {
            state.current = manifest.previous;
            if state.current.as_ref().is_some_and(|parent| {
                parent.generation > reference.generation || parent.key == reference.key
            }) {
                return Err("catalog_parent_invalid".into());
            }
            state.offset = 0;
            state.summary_offset = 0;
        }
        if state.seen.len() + state.retired.len() + state.summaries_seen.len() > IDENTITY_LIMIT {
            return Err("catalog_identity_admission".into());
        }
    }
    let complete = state.day >= state.days.len()
        && (state.hour >= state.hours.len()
            || (state.current.is_none() && state.hour + 1 >= state.hours.len()));
    let next = if complete {
        None
    } else {
        let body = serde_json::to_vec(&state).map_err(|e| e.to_string())?;
        if body.len() > STATE_LIMIT {
            return Err("catalog_cursor_admission".into());
        }
        let digest = hex::encode(sha2::Sha256::digest(&body));
        let key = format!(
            "{}processed/_catalog/cursor-{digest}.json",
            crate::ingest::scope(store.service)
        );
        store.put_verified(&key, body)?;
        Some(key)
    };
    let open_hour = state
        .current
        .as_ref()
        .and_then(|_| state.hours.get(state.hour).map(|(hour, _)| *hour));
    Ok(Page {
        files,
        summaries,
        next,
        generation: state.generation,
        open_hour,
    })
}

/// Most concurrent chains one forked listing splits into.
pub const MAX_FORKS: usize = 16;
/// Most day indexes a fork setup reads to split the range by hour; a longer range splits by day.
const FORK_DAY_INDEXES: usize = 12;
/// Ranges this short list in one page, so forking would only add a round trip.
const FORK_MIN_SECONDS: i64 = 2 * 3600;

/// The first page of a listing that may fork.
pub enum Start {
    Page(Page),
    /// Cursor tokens of independent chains that together list the range exactly once.
    Forks {
        tokens: Vec<String>,
        generation: u64,
    },
}

/// Splits the walk of `[from, to)` into chains that never share an hour, so a client can page
/// them concurrently. Each chain is a whole-day or within-day run of hours of one pinned head;
/// every per-hour and per-day walk state starts fresh at an hour or day, so walking the chains
/// lists what one walk would. A range with one chain is walked here.
pub fn forked_page<R: Runtime>(
    store: &Store<'_, R>,
    from: i64,
    to: i64,
    summary: bool,
    include_summaries: bool,
    summary_bounds: (i64, i64),
) -> Result<Start, String> {
    let base = fresh(store, None, from, to, summary, summary_bounds)?;
    if to - from <= FORK_MIN_SECONDS {
        return walk(
            store,
            base,
            include_summaries,
            summary_bounds,
            Prefetched::new(),
        )
        .map(Start::Page);
    }
    let mut prefetched = Prefetched::new();
    let mut shards = Vec::new();
    if base.days.len() > FORK_DAY_INDEXES {
        let size = base.days.len().div_ceil(MAX_FORKS);
        for days in base.days.chunks(size) {
            let (first, last) = (days[0].0, days[days.len() - 1].0 + 86_400);
            let mut shard = base.clone();
            shard.days = days.to_vec();
            shard
                .hour_bundles
                .retain(|hour, _| (first..last).contains(hour));
            shard
                .open_bundles
                .retain(|hour, _| (first..last).contains(hour));
            shard
                .day_bundles
                .retain(|day, _| (first..last).contains(day));
            shards.push(shard);
        }
    } else {
        let keys: Vec<(String, usize)> = base
            .days
            .iter()
            .map(|(_, key)| (key.clone(), MANIFEST_LIMIT))
            .collect();
        let mut days = Vec::new();
        for (n, (day, key)) in base.days.iter().enumerate() {
            if !admitted(store)
                || !(already_sent(store, &prefetched, key)
                    || store.runtime.admin_remaining_calls() > 3)
            {
                return Err("backend_budget_exhausted".into());
            }
            let bytes = fetch(store, &mut prefetched, key, MANIFEST_LIMIT, || {
                keys[n + 1..].to_vec()
            })?;
            let index: Day =
                serde_json::from_slice(&bytes).map_err(|_| "catalog_object_invalid")?;
            if index.version != VERSION {
                return Err("catalog_version_unsupported".into());
            }
            let mut state = base.clone();
            state.days = Vec::new();
            state.day = 0;
            enter_day(&mut state, *day, index, include_summaries, summary_bounds);
            if !state.hours.is_empty() {
                days.push(state);
            }
        }
        let total: usize = days.iter().map(|day| day.hours.len()).sum();
        let mut size = total.div_ceil(MAX_FORKS).max(1);
        while days
            .iter()
            .map(|day| day.hours.len().div_ceil(size))
            .sum::<usize>()
            > MAX_FORKS
        {
            size += 1;
        }
        for day in days {
            for (n, hours) in day.hours.chunks(size).enumerate() {
                let mut shard = day.clone();
                shard.hours = hours.to_vec();
                shard.current = hours.first().map(|(_, r)| r.clone());
                shard.day_bundle_listed = n > 0;
                let held = |hour: &i64| hours.iter().any(|(h, _)| h == hour);
                shard.hour_bundles.retain(|hour, _| held(hour));
                shard.open_bundles.retain(|hour, _| held(hour));
                shard.day_bundles.clear();
                shards.push(shard);
            }
        }
    }
    if shards.len() <= 1 {
        let state = match shards.pop() {
            Some(state) => state,
            None => State {
                days: Vec::new(),
                ..base
            },
        };
        return walk(store, state, include_summaries, summary_bounds, prefetched).map(Start::Page);
    }
    let generation = base.generation;
    let body = serde_json::to_vec(&shards).map_err(|e| e.to_string())?;
    if body.len() > STATE_LIMIT {
        return Err("catalog_cursor_admission".into());
    }
    let digest = hex::encode(sha2::Sha256::digest(&body));
    let key = format!(
        "{}processed/_catalog/forks-{digest}.json",
        crate::ingest::scope(store.service)
    );
    store.put_verified(&key, body)?;
    Ok(Start::Forks {
        tokens: (0..shards.len()).map(|i| format!("{key}#{i}")).collect(),
        generation,
    })
}

fn initial(root: Head, service: String, from: i64, to: i64, summary: bool, now: u64) -> State {
    let days = if root.version == VERSION {
        root.days
            .into_iter()
            .filter(|(day, _)| {
                *day >= day_start(from.saturating_sub(3600)) && *day <= day_start(to - 1)
            })
            .collect()
    } else {
        Vec::new()
    };
    State {
        version: VERSION,
        service,
        from,
        to,
        summary,
        generation: root.generation,
        created: now,
        days,
        hour_bundles: root.hour_bundles,
        open_bundles: root.open_bundles,
        day_bundles: root.day_bundles,
        ..Default::default()
    }
}

use sha2::Digest;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        admin::{FosRequest, FosResponse, Record, Service},
        ingest::bundle::open_bundle_key,
        setup::SetupStores,
    };
    use std::{cell::RefCell, time::Instant};

    const BASE: i64 = 1_800_000_000; // 2027-01-15T08:00:00Z

    struct MockRt {
        now: u64,
        kv: RefCell<BTreeMap<String, Value>>,
        objects: RefCell<BTreeMap<String, Vec<u8>>>,
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
            self.now
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
        fn admin_get_fresh(&self, key: &str, _: &str) -> Result<Option<Record>, String> {
            self.admin_get(key)
        }
        fn admin_cas(&self, _: &str, _: Option<&str>, _: &Value) -> Result<bool, String> {
            Ok(true)
        }
        fn admin_fos(&self, request: FosRequest<'_>) -> Result<FosResponse, String> {
            let key = request.key.trim_start_matches('/');
            match request.method {
                "GET" => match self.objects.borrow().get(key) {
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
                },
                "PUT" => {
                    self.objects
                        .borrow_mut()
                        .insert(key.to_owned(), request.body.to_vec());
                    Ok(FosResponse {
                        status: 200,
                        body: Vec::new(),
                        cdn_cache: None,
                        retry_after: None,
                    })
                }
                other => Err(format!("unexpected fos method: {other}")),
            }
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

    /// Seeds one open hour (BASE..BASE+3600) with 28 two-minute summary windows (08:00..08:56)
    /// and a matching `open_bundle=` covering all 28 windows.
    fn seed_open_hour() -> (MockRt, Service, String, Vec<String>) {
        let rt = MockRt {
            now: (BASE + 55 * 60 + 6) as u64,
            kv: RefCell::new(BTreeMap::new()),
            objects: RefCell::new(BTreeMap::new()),
        };
        let svc = test_service();
        let mut windows = Vec::new();
        let mut summaries = Vec::new();
        for i in 0..28 {
            let start = BASE + i * 120;
            let end = start + 120;
            let key = format!(
                "processed/summary/request/year=2027/month=01/day=15/hour=08/summary={start}-{end}-0.parquet"
            );
            summaries.push(Output {
                key: key.clone(),
                bytes: 100,
                rows: 10,
                md5: format!("m{i}"),
                metadata: None,
            });
            windows.push(key);
        }
        let end_ms = (BASE + 28 * 120) * 1000;
        let open_key = open_bundle_key("processed/summary_bundle/request/", BASE, end_ms, &windows);
        let coverage_key = "processed/_catalog/open-coverage.json".to_owned();
        let coverage = bundles::Coverage {
            file: Output {
                key: open_key.clone(),
                bytes: 2800,
                rows: 280,
                md5: "openmd5".into(),
                metadata: None,
            },
            start: BASE,
            end: BASE + 28 * 120,
            windows: windows.iter().cloned().collect(),
            hours: BTreeMap::new(),
            built_generation: 1,
        };
        let block_key = "processed/_catalog/block-1.json".to_owned();
        let block = Block {
            version: VERSION,
            service_id: svc.service_id.clone(),
            generation: 1,
            hour: BASE,
            previous: None,
            files: Vec::new(),
            summaries,
            replaces: Vec::new(),
            committed_at: BTreeMap::new(),
        };
        let day = day_start(BASE);
        let day_key = "processed/_catalog/day-1.json".to_owned();
        let day_index = Day {
            version: VERSION,
            service_id: Some(svc.service_id.clone()),
            hours: BTreeMap::from([(
                BASE,
                Reference {
                    key: block_key.clone(),
                    generation: 1,
                },
            )]),
            summary_counts: BTreeMap::from([(BASE, windows.len() as u64)]),
            ..Default::default()
        };
        let head = Head {
            version: VERSION,
            generation: 1,
            recent: vec![super::super::Event {
                cursor: "1".into(),
                generation: 1,
                committed_at: None,
                manifest: None,
            }],
            days: BTreeMap::from([(day, day_key.clone())]),
            open_bundles: BTreeMap::from([(
                BASE,
                Reference {
                    key: coverage_key.clone(),
                    generation: 1,
                },
            )]),
            ..Default::default()
        };
        rt.objects
            .borrow_mut()
            .insert(coverage_key, serde_json::to_vec(&coverage).unwrap());
        rt.objects
            .borrow_mut()
            .insert(block_key, serde_json::to_vec(&block).unwrap());
        rt.objects
            .borrow_mut()
            .insert(day_key, serde_json::to_vec(&day_index).unwrap());
        rt.kv
            .borrow_mut()
            .insert(WATERMARK_KEY.into(), json!({ "catalog": head }));
        (rt, svc, open_key, windows)
    }

    #[test]
    fn open_bundle_fits_when_to_is_inside_latest_window() {
        let (rt, svc, open_key, _) = seed_open_hour();
        let store = Store {
            runtime: &rt,
            service: &svc,
            access_key: "ak".into(),
            secret_key: "sk".into(),
            started: Instant::now(),
            vcpu_start: 0,
        };
        // Live query at 08:55:06Z, inside the last 2-minute window [08:54:00, 08:56:00).
        let from = BASE;
        let to = BASE + 55 * 60 + 6;
        let page =
            pinned_page_precise(&store, from, to, true, false, None, None, (from, to)).unwrap();
        let keys: Vec<&str> = page
            .files
            .iter()
            .map(|f| f["key"].as_str().unwrap())
            .collect();
        assert_eq!(keys, vec![open_key.as_str()]);
        assert!(page.next.is_none());
    }

    #[test]
    fn open_bundle_matches_included_for_fractional_to() {
        let (rt, svc, open_key, windows) = seed_open_hour();
        let store = Store {
            runtime: &rt,
            service: &svc,
            access_key: "ak".into(),
            secret_key: "sk".into(),
            started: Instant::now(),
            vcpu_start: 0,
        };
        // Fractional `to` = 08:54:00.500Z: api.rs sets summary_bounds.1 = 08:54:00, to = 08:54:01.
        // `included()` includes [08:54:00, 08:56:00), so open_bundle fits.
        let last_start = BASE + 54 * 60;
        let page = pinned_page_precise(
            &store,
            BASE,
            last_start + 1,
            true,
            false,
            None,
            None,
            (BASE, last_start),
        )
        .unwrap();
        let keys: Vec<&str> = page
            .files
            .iter()
            .map(|f| f["key"].as_str().unwrap())
            .collect();
        assert_eq!(keys, vec![open_key.as_str()]);

        // Fractional `to` = 08:53:59.500Z: api.rs sets summary_bounds.1 = 08:53:59, to = 08:54:00.
        // `included()` excludes [08:54:00, 08:56:00), so open_bundle must be rejected.
        let page = pinned_page_precise(
            &store,
            BASE,
            last_start,
            true,
            false,
            None,
            None,
            (BASE, last_start - 1),
        )
        .unwrap();
        let keys: Vec<&str> = page
            .files
            .iter()
            .map(|f| f["key"].as_str().unwrap())
            .collect();
        assert_eq!(keys.len(), 27);
        assert!(keys.iter().all(|k| !k.contains("open_bundle=")));
        assert!(!keys.contains(&windows[27].as_str()));
    }

    #[test]
    fn open_bundle_rejected_for_historical_sub_hour_range() {
        let (rt, svc, _, windows) = seed_open_hour();
        let store = Store {
            runtime: &rt,
            service: &svc,
            access_key: "ak".into(),
            secret_key: "sk".into(),
            started: Instant::now(),
            vcpu_start: 0,
        };
        // Sub-hour historical query ending at 08:30:00Z (15 windows before 08:30).
        let from = BASE;
        let to = BASE + 30 * 60;
        let page =
            pinned_page_precise(&store, from, to, true, false, None, None, (from, to)).unwrap();
        let keys: Vec<&str> = page
            .files
            .iter()
            .map(|f| f["key"].as_str().unwrap())
            .collect();
        assert_eq!(keys.len(), 15);
        assert!(keys.iter().all(|k| !k.contains("open_bundle=")));
        assert!(keys
            .iter()
            .all(|k| file_bounds(k).is_some_and(|(s, _)| s < to)));

        // Exact boundary at last_window.start (08:54:00Z): excludes window 27.
        let to_boundary = BASE + 54 * 60;
        let page = pinned_page_precise(
            &store,
            from,
            to_boundary,
            true,
            false,
            None,
            None,
            (from, to_boundary),
        )
        .unwrap();
        let keys: Vec<&str> = page
            .files
            .iter()
            .map(|f| f["key"].as_str().unwrap())
            .collect();
        assert_eq!(keys.len(), 27);
        assert!(keys.iter().all(|k| !k.contains("open_bundle=")));
        assert!(!keys.contains(&windows[27].as_str()));
    }
}
