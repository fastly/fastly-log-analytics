use super::{
    admitted, already_sent,
    bundles::{self, COVERAGE_LIMIT},
    day_start, fetch_batched, file_bounds, file_hour, head, validate_day, Block, Day, Head,
    Prefetched, Reference, MANIFEST_LIMIT, VERSION,
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
    visited_blocks: BTreeSet<String>,
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
                if summary_bounds.0 <= *hour && summary_bounds.1 >= hour.saturating_add(3600) {
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
        && summary_bounds.1 >= day.saturating_add(86_400)
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
    state.visited_blocks.clear();
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
            validate_day(&index, &store.service.service_id)?;
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
                if summary_bounds.0 <= hour && summary_bounds.1 >= hour.saturating_add(3600) {
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
            if files.len() >= FILES_PAGE || summaries.len() >= FILES_PAGE {
                break;
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
            state.visited_blocks.clear();
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
            state.visited_blocks.insert(reference.key.clone());
            state.current = manifest.previous;
            if state.current.as_ref().is_some_and(|parent| {
                parent.generation > reference.generation
                    || parent.key == reference.key
                    || state.visited_blocks.contains(&parent.key)
            }) {
                return Err("catalog_parent_invalid".into());
            }
            state.offset = 0;
            state.summary_offset = 0;
        }
        if state.seen.len()
            + state.retired.len()
            + state.summaries_seen.len()
            + state.visited_blocks.len()
            > IDENTITY_LIMIT
        {
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

/// Most concurrent chains one forked listing splits into (day-level splitting).
pub const MAX_FORKS: usize = 16;
/// Most concurrent chains one hour-level forked listing splits into, keeping
/// typical 24h–7d cold listings within browser per-origin concurrency.
const MAX_HOUR_FORKS: usize = 8;
/// Fewest hours assigned to one hour-level fork. Each Compute invocation has a
/// 32-call backend budget and 16-way FOS prefetch, so 6–8 hours fit in one page
/// and one parallel FOS batch; ranges at or below this floor walk inline in one
/// request instead of paying fork setup + parallel round-trip overhead.
const MIN_HOURS_PER_FORK: usize = 8;
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
    if to.saturating_sub(from) <= FORK_MIN_SECONDS {
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
            let (first, last) = (days[0].0, days[days.len() - 1].0.saturating_add(86_400));
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
        let mut non_empty_days = Vec::new();
        let mut day_bytes = Vec::new();
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
            validate_day(&index, &store.service.service_id)?;
            let mut state = base.clone();
            state.days = Vec::new();
            state.day = 0;
            enter_day(&mut state, *day, index, include_summaries, summary_bounds);
            if !state.hours.is_empty() {
                non_empty_days.push((*day, key.clone()));
                day_bytes.push((key.clone(), bytes));
                days.push(state);
            }
        }
        let total: usize = days.iter().map(|day| day.hours.len()).sum();
        if total <= MIN_HOURS_PER_FORK {
            if days.len() <= 1 {
                let state = days.pop().unwrap_or(State {
                    days: Vec::new(),
                    ..base
                });
                return walk(store, state, include_summaries, summary_bounds, prefetched)
                    .map(Start::Page);
            }
            for (key, bytes) in day_bytes {
                prefetched.insert(key, Ok(Some(bytes)));
            }
            let mut state = base;
            state.days = non_empty_days;
            return walk(store, state, include_summaries, summary_bounds, prefetched)
                .map(Start::Page);
        }
        let max_forks = MAX_HOUR_FORKS.max(days.len());
        let mut size = total.div_ceil(max_forks).max(MIN_HOURS_PER_FORK);
        while days
            .iter()
            .map(|day| day.hours.len().div_ceil(size))
            .sum::<usize>()
            > max_forks
        {
            size += 1;
        }
        for day in days {
            let parts = day.hours.len().div_ceil(size);
            let chunk = day.hours.len().div_ceil(parts);
            for (n, hours) in day.hours.chunks(chunk).enumerate() {
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
        fn admin_fos_etag(
            &self,
            request: FosRequest<'_>,
        ) -> Result<(FosResponse, Option<String>), String> {
            use md5::{Digest, Md5};
            let etag = (request.method == "PUT").then(|| hex::encode(Md5::digest(&request.body)));
            let resp = self.admin_fos(request)?;
            Ok((resp, etag))
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

    #[test]
    fn rt_e1a_open_and_hour_bundle_not_duplicated_at_files_page_boundary() {
        use crate::ingest::bundle::hour_bundle_key;

        // Case 1 & 2: Hour 0 has FILES_PAGE - 1 (499) unbundled summary files; Hour 1 has 10
        // windows covered by an `open_bundle`. Test both with `summary_counts` matching 10 (fast
        // skip) and without `summary_counts` fast-skip (forces Page 2 to resume Hour 1!).
        for with_fast_skip in [false, true] {
            let hour0 = BASE;
            let hour1 = BASE + 3600;
            let rt = MockRt {
                now: (hour1 + 25 * 60) as u64,
                kv: RefCell::new(BTreeMap::new()),
                objects: RefCell::new(BTreeMap::new()),
            };
            let svc = test_service();

            let mut h0_summaries = Vec::with_capacity(FILES_PAGE - 1);
            for i in 0..(FILES_PAGE - 1) {
                let start = hour0 + (i as i64) * 2;
                let end = start + 2;
                let key = format!(
                    "processed/summary/request/year=2027/month=01/day=15/hour=08/summary={start}-{end}-{i}.parquet"
                );
                h0_summaries.push(Output {
                    key,
                    bytes: 100,
                    rows: 1,
                    md5: format!("h0m{i}"),
                    metadata: None,
                });
            }
            let h0_block_key = "processed/_catalog/block-h0.json".to_owned();
            let h0_block = Block {
                version: VERSION,
                service_id: svc.service_id.clone(),
                generation: 1,
                hour: hour0,
                previous: None,
                files: Vec::new(),
                summaries: h0_summaries,
                replaces: Vec::new(),
                committed_at: BTreeMap::new(),
            };

            let mut h1_windows = Vec::new();
            let mut h1_summaries = Vec::new();
            for i in 0..10 {
                let start = hour1 + i * 120;
                let end = start + 120;
                let key = format!(
                    "processed/summary/request/year=2027/month=01/day=15/hour=09/summary={start}-{end}-0.parquet"
                );
                h1_summaries.push(Output {
                    key: key.clone(),
                    bytes: 100,
                    rows: 10,
                    md5: format!("h1m{i}"),
                    metadata: None,
                });
                h1_windows.push(key);
            }
            let end_ms = (hour1 + 10 * 120) * 1000;
            let open_key = open_bundle_key(
                "processed/summary_bundle/request/",
                hour1,
                end_ms,
                &h1_windows,
            );
            let coverage_key = "processed/_catalog/open-coverage-h1.json".to_owned();
            let coverage = bundles::Coverage {
                file: Output {
                    key: open_key.clone(),
                    bytes: 1000,
                    rows: 100,
                    md5: "openmd5h1".into(),
                    metadata: None,
                },
                start: hour1,
                end: hour1 + 10 * 120,
                windows: h1_windows.iter().cloned().collect(),
                hours: BTreeMap::new(),
                built_generation: 1,
            };
            let h1_block_key = "processed/_catalog/block-h1.json".to_owned();
            let h1_block = Block {
                version: VERSION,
                service_id: svc.service_id.clone(),
                generation: 1,
                hour: hour1,
                previous: None,
                files: Vec::new(),
                summaries: h1_summaries,
                replaces: Vec::new(),
                committed_at: BTreeMap::new(),
            };

            let day = day_start(hour0);
            let day_key = "processed/_catalog/day-e1a.json".to_owned();
            let mut summary_counts = BTreeMap::from([(hour0, (FILES_PAGE - 1) as u64)]);
            if with_fast_skip {
                summary_counts.insert(hour1, 10);
            }
            let day_index = Day {
                version: VERSION,
                service_id: Some(svc.service_id.clone()),
                hours: BTreeMap::from([
                    (
                        hour0,
                        Reference {
                            key: h0_block_key.clone(),
                            generation: 1,
                        },
                    ),
                    (
                        hour1,
                        Reference {
                            key: h1_block_key.clone(),
                            generation: 1,
                        },
                    ),
                ]),
                summary_counts,
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
                    hour1,
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
                .insert(h0_block_key, serde_json::to_vec(&h0_block).unwrap());
            rt.objects
                .borrow_mut()
                .insert(h1_block_key, serde_json::to_vec(&h1_block).unwrap());
            rt.objects
                .borrow_mut()
                .insert(day_key, serde_json::to_vec(&day_index).unwrap());
            rt.kv
                .borrow_mut()
                .insert(WATERMARK_KEY.into(), json!({ "catalog": head }));

            let store = Store {
                runtime: &rt,
                service: &svc,
                access_key: "ak".into(),
                secret_key: "sk".into(),
                started: Instant::now(),
                vcpu_start: 0,
            };
            let from = hour0;
            let to = hour1 + 25 * 60;
            let mut checkpoint: Option<String> = None;
            let mut pages = 0usize;
            let mut all_keys = Vec::new();
            loop {
                let page = pinned_page_precise(
                    &store,
                    from,
                    to,
                    true,
                    false,
                    checkpoint.as_deref(),
                    None,
                    (from, to),
                )
                .unwrap();
                pages += 1;
                all_keys.extend(
                    page.files
                        .iter()
                        .map(|f| f["key"].as_str().unwrap().to_owned()),
                );
                checkpoint = page.next;
                if checkpoint.is_none() {
                    break;
                }
                assert!(pages < 10, "pagination loop did not terminate");
            }
            let open_occurrences = all_keys.iter().filter(|k| *k == &open_key).count();
            assert_eq!(
                open_occurrences, 1,
                "RT-E1a (with_fast_skip={with_fast_skip}): open_bundle emitted {open_occurrences} times (expected 1)"
            );
            assert_eq!(
                all_keys.len(),
                FILES_PAGE,
                "RT-E1a (with_fast_skip={with_fast_skip}): expected exactly {FILES_PAGE} files, got {}",
                all_keys.len()
            );
            if !with_fast_skip {
                assert_eq!(
                    pages, 2,
                    "RT-E1a (no fast-skip): expected 2 pages to exercise checkpoint resume"
                );
            } else {
                assert_eq!(
                    pages, 1,
                    "RT-E1a (fast-skip): covered last hour should complete on page 1 without redundant cursor"
                );
            }
        }

        // Case 3: Hour 0 = 499 unbundled summaries, Hour 1 = closed hour_bundle (10 windows), Hour 2 = 1 summary.
        {
            let hour0 = BASE;
            let hour1 = BASE + 3600;
            let hour2 = BASE + 7200;
            let rt = MockRt {
                now: (hour2 + 600) as u64,
                kv: RefCell::new(BTreeMap::new()),
                objects: RefCell::new(BTreeMap::new()),
            };
            let svc = test_service();
            let h0_summaries: Vec<Output> = (0..(FILES_PAGE - 1))
                .map(|i| {
                    let start = hour0 + (i as i64) * 2;
                    let end = start + 2;
                    Output {
                        key: format!(
                            "processed/summary/request/year=2027/month=01/day=15/hour=08/summary={start}-{end}-{i}.parquet"
                        ),
                        bytes: 100,
                        rows: 1,
                        md5: format!("hb0m{i}"),
                        metadata: None,
                    }
                })
                .collect();
            let h1_windows: Vec<String> = (0..10)
                .map(|i| {
                    let start = hour1 + i * 120;
                    let end = start + 120;
                    format!(
                        "processed/summary/request/year=2027/month=01/day=15/hour=09/summary={start}-{end}-0.parquet"
                    )
                })
                .collect();
            let h1_summaries: Vec<Output> = h1_windows
                .iter()
                .enumerate()
                .map(|(i, k)| Output {
                    key: k.clone(),
                    bytes: 100,
                    rows: 10,
                    md5: format!("hb1m{i}"),
                    metadata: None,
                })
                .collect();
            let h2_key = format!(
                "processed/summary/request/year=2027/month=01/day=15/hour=10/summary={hour2}-{}-0.parquet",
                hour2 + 120
            );
            let h2_block = Block {
                version: VERSION,
                service_id: svc.service_id.clone(),
                generation: 1,
                hour: hour2,
                previous: None,
                files: Vec::new(),
                summaries: vec![Output {
                    key: h2_key.clone(),
                    bytes: 100,
                    rows: 10,
                    md5: "hb2m0".into(),
                    metadata: None,
                }],
                replaces: Vec::new(),
                committed_at: BTreeMap::new(),
            };
            let hour_bundle =
                hour_bundle_key("processed/summary_bundle/request/", hour1, &h1_windows);
            let coverage_key = "processed/_catalog/hour-coverage-h1.json".to_owned();
            let coverage = bundles::Coverage {
                file: Output {
                    key: hour_bundle.clone(),
                    bytes: 1000,
                    rows: 100,
                    md5: "hourmd5h1".into(),
                    metadata: None,
                },
                start: hour1,
                end: hour1 + 3600,
                windows: h1_windows.iter().cloned().collect(),
                hours: BTreeMap::new(),
                built_generation: 1,
            };
            let day = day_start(hour0);
            let day_key = "processed/_catalog/day-e1a-hb.json".to_owned();
            let day_index = Day {
                version: VERSION,
                service_id: Some(svc.service_id.clone()),
                hours: BTreeMap::from([
                    (
                        hour0,
                        Reference {
                            key: "processed/_catalog/hb-block-h0.json".into(),
                            generation: 1,
                        },
                    ),
                    (
                        hour1,
                        Reference {
                            key: "processed/_catalog/hb-block-h1.json".into(),
                            generation: 1,
                        },
                    ),
                    (
                        hour2,
                        Reference {
                            key: "processed/_catalog/hb-block-h2.json".into(),
                            generation: 1,
                        },
                    ),
                ]),
                summary_counts: BTreeMap::from([
                    (hour0, (FILES_PAGE - 1) as u64),
                    (hour1, 10),
                    (hour2, 1),
                ]),
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
                hour_bundles: BTreeMap::from([(
                    hour1,
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
            rt.objects.borrow_mut().insert(
                "processed/_catalog/hb-block-h0.json".into(),
                serde_json::to_vec(&Block {
                    version: VERSION,
                    service_id: svc.service_id.clone(),
                    generation: 1,
                    hour: hour0,
                    previous: None,
                    files: Vec::new(),
                    summaries: h0_summaries,
                    replaces: Vec::new(),
                    committed_at: BTreeMap::new(),
                })
                .unwrap(),
            );
            rt.objects.borrow_mut().insert(
                "processed/_catalog/hb-block-h1.json".into(),
                serde_json::to_vec(&Block {
                    version: VERSION,
                    service_id: svc.service_id.clone(),
                    generation: 1,
                    hour: hour1,
                    previous: None,
                    files: Vec::new(),
                    summaries: h1_summaries,
                    replaces: Vec::new(),
                    committed_at: BTreeMap::new(),
                })
                .unwrap(),
            );
            rt.objects.borrow_mut().insert(
                "processed/_catalog/hb-block-h2.json".into(),
                serde_json::to_vec(&h2_block).unwrap(),
            );
            rt.objects
                .borrow_mut()
                .insert(day_key, serde_json::to_vec(&day_index).unwrap());
            rt.kv
                .borrow_mut()
                .insert(WATERMARK_KEY.into(), json!({ "catalog": head }));

            let store = Store {
                runtime: &rt,
                service: &svc,
                access_key: "ak".into(),
                secret_key: "sk".into(),
                started: Instant::now(),
                vcpu_start: 0,
            };
            let from = hour0;
            let to = hour2 + 600;
            let mut checkpoint: Option<String> = None;
            let mut pages = 0usize;
            let mut all_keys = Vec::new();
            loop {
                let page = pinned_page_precise(
                    &store,
                    from,
                    to,
                    true,
                    false,
                    checkpoint.as_deref(),
                    None,
                    (from, to),
                )
                .unwrap();
                pages += 1;
                all_keys.extend(
                    page.files
                        .iter()
                        .map(|f| f["key"].as_str().unwrap().to_owned()),
                );
                checkpoint = page.next;
                if checkpoint.is_none() {
                    break;
                }
                assert!(pages < 10);
            }
            let hb_occurrences = all_keys.iter().filter(|k| *k == &hour_bundle).count();
            assert_eq!(
                hb_occurrences, 1,
                "RT-E1a (hour_bundle): emitted {hb_occurrences} times (expected 1)"
            );
            assert_eq!(pages, 2);
            assert_eq!(all_keys.len(), FILES_PAGE + 1);
            assert!(all_keys.contains(&h2_key));
        }
    }

    #[test]
    fn iter35_m2_two_node_equal_gen_block_cycle_terminates_with_error() {
        use std::cell::Cell;

        struct BoundedRt {
            inner: MockRt,
            remaining: Cell<u32>,
            gets: Cell<u32>,
        }
        impl SetupStores for BoundedRt {
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
        impl Runtime for BoundedRt {
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
            fn admin_get_fresh(&self, key: &str, token: &str) -> Result<Option<Record>, String> {
                self.inner.admin_get_fresh(key, token)
            }
            fn admin_cas(&self, k: &str, g: Option<&str>, v: &Value) -> Result<bool, String> {
                self.inner.admin_cas(k, g, v)
            }
            fn admin_fos(&self, request: FosRequest<'_>) -> Result<FosResponse, String> {
                let rem = self.remaining.get();
                if rem == 0 {
                    return Err("backend_budget_exhausted".into());
                }
                self.remaining.set(rem - 1);
                if request.method == "GET" {
                    self.gets.set(self.gets.get() + 1);
                }
                self.inner.admin_fos(request)
            }
            fn admin_fos_etag(
                &self,
                request: FosRequest<'_>,
            ) -> Result<(FosResponse, Option<String>), String> {
                let rem = self.remaining.get();
                if rem == 0 {
                    return Err("backend_budget_exhausted".into());
                }
                self.remaining.set(rem - 1);
                self.inner.admin_fos_etag(request)
            }
            fn admin_remaining_calls(&self) -> u32 {
                self.remaining.get()
            }
        }

        let svc = test_service();
        let hour = BASE;
        let day = day_start(hour);
        let key_a = "processed/_catalog/block-cycle-a.json".to_owned();
        let key_b = "processed/_catalog/block-cycle-b.json".to_owned();
        let day_key = "processed/_catalog/day-cycle.json".to_owned();

        // 2-node equal-generation cycle: A (gen 5) -> B (gen 5) -> A (gen 5)
        let block_a = Block {
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
        let block_b = Block {
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
            recent: vec![super::super::Event {
                cursor: "5".into(),
                generation: 5,
                committed_at: None,
                manifest: None,
            }],
            days: BTreeMap::from([(day, day_key.clone())]),
            ..Default::default()
        };

        let inner = MockRt {
            now: (BASE + 3600) as u64,
            kv: RefCell::new(BTreeMap::from([(
                WATERMARK_KEY.into(),
                json!({ "catalog": head }),
            )])),
            objects: RefCell::new(BTreeMap::from([
                (day_key, serde_json::to_vec(&day_index).unwrap()),
                (key_a, serde_json::to_vec(&block_a).unwrap()),
                (key_b, serde_json::to_vec(&block_b).unwrap()),
            ])),
        };
        let rt = BoundedRt {
            inner,
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

        let res = page(&store, hour, hour + 3600, false, false, None);
        assert_eq!(
            res.err().as_deref(),
            Some("catalog_parent_invalid"),
            "m2: 2-node equal-generation Block cycle A <-> B must terminate cleanly with catalog_parent_invalid (gets={})",
            rt.gets.get()
        );
        assert!(
            rt.gets.get() <= 4,
            "m2: cycle must terminate immediately after visiting A and B, not burn budget (gets={})",
            rt.gets.get()
        );
    }

    #[test]
    fn iter36_f10_discovery_near_i64_max_hour_and_day_do_not_overflow() {
        let svc = test_service();
        let near_max_hour = i64::MAX - 100;
        let near_max_day = i64::MAX - 10_000;

        // 1. enter_day with day near i64::MAX must not panic on `day + 86_400`
        let mut st = State {
            from: 0,
            to: i64::MAX,
            summary: true,
            day_bundles: BTreeMap::from([(
                near_max_day,
                Reference {
                    key: "processed/_catalog/day-bundle.json".into(),
                    generation: 1,
                },
            )]),
            ..State::default()
        };
        let idx = Day {
            version: VERSION,
            service_id: Some(svc.service_id.clone()),
            hours: BTreeMap::from([(
                near_max_hour,
                Reference {
                    key: "processed/_catalog/blk-max.json".into(),
                    generation: 1,
                },
            )]),
            ..Default::default()
        };
        enter_day(&mut st, near_max_day, idx, true, (0, i64::MAX));
        assert!(st.day_bundle.is_some());

        // 2. upcoming with hour near i64::MAX must not panic on `hour + 3600`
        st.day_bundle_ready = true;
        st.hour_bundles.insert(
            near_max_hour,
            Reference {
                key: "processed/_catalog/hb-max.json".into(),
                generation: 1,
            },
        );
        let keys = upcoming(&st, true, (0, i64::MAX));
        assert!(keys
            .iter()
            .any(|(k, _)| k == "processed/_catalog/hb-max.json"));

        // 3. walk with hour near i64::MAX and forked_page with >FORK_DAY_INDEXES days ending near i64::MAX
        let aligned_max_day = i64::MAX - i64::MAX.rem_euclid(86_400);
        let mut days_map = BTreeMap::new();
        for i in 0..14i64 {
            let d = aligned_max_day - (13 - i) * 86_400;
            days_map.insert(d, format!("processed/_catalog/d-{i}.json"));
        }
        let head = Head {
            version: VERSION,
            generation: 1,
            recent: vec![super::super::Event {
                cursor: "1".into(),
                generation: 1,
                committed_at: None,
                manifest: None,
            }],
            days: days_map,
            ..Default::default()
        };
        let rt = MockRt {
            now: (BASE + 3600) as u64,
            kv: RefCell::new(BTreeMap::from([(
                WATERMARK_KEY.into(),
                json!({ "catalog": head }),
            )])),
            objects: RefCell::new(BTreeMap::new()),
        };
        let store = Store {
            runtime: &rt,
            service: &svc,
            access_key: "ak".into(),
            secret_key: "sk".into(),
            started: Instant::now(),
            vcpu_start: 0,
        };
        let start = forked_page(&store, 0, i64::MAX, true, false, (0, i64::MAX)).unwrap();
        assert!(matches!(start, Start::Forks { .. }));
    }

    #[test]
    fn iter39_f1_discovery_walk_and_forked_page_validate_day_version_and_service_id() {
        let svc = test_service();
        let hour = BASE;
        let day = day_start(hour);
        let day_key = "processed/_catalog/day-iter39-disc.json".to_owned();
        let head = Head {
            version: VERSION,
            generation: 5,
            recent: vec![super::super::Event {
                cursor: "5".into(),
                generation: 5,
                committed_at: None,
                manifest: None,
            }],
            days: BTreeMap::from([(day, day_key.clone())]),
            ..Default::default()
        };

        let cases: Vec<(&str, Day, Option<&str>)> = vec![
            (
                "foreign_service_id",
                Day {
                    version: VERSION,
                    service_id: Some("foreign-svc".into()),
                    ..Default::default()
                },
                Some("catalog_manifest_invalid"),
            ),
            (
                "unsupported_version",
                Day {
                    version: VERSION + 1,
                    service_id: Some(svc.service_id.clone()),
                    ..Default::default()
                },
                Some("catalog_version_unsupported"),
            ),
            (
                "legacy_service_id_none",
                Day {
                    version: VERSION,
                    service_id: None,
                    ..Default::default()
                },
                None,
            ),
        ];

        for (label, day_obj, expected_err) in cases {
            let rt = MockRt {
                now: (BASE + 3600) as u64,
                kv: RefCell::new(BTreeMap::from([(
                    WATERMARK_KEY.into(),
                    json!({ "catalog": head }),
                )])),
                objects: RefCell::new(BTreeMap::from([(
                    day_key.clone(),
                    serde_json::to_vec(&day_obj).unwrap(),
                )])),
            };
            let store = Store {
                runtime: &rt,
                service: &svc,
                access_key: "ak".into(),
                secret_key: "sk".into(),
                started: Instant::now(),
                vcpu_start: 0,
            };

            // 1. walk (via page)
            let walk_res = page(&store, hour, hour + 3600, false, false, None);
            assert_eq!(
                walk_res.err().as_deref(),
                expected_err,
                "iter39 ({label}): discovery::walk Day validation mismatch"
            );

            // 2. forked_page (> FORK_MIN_SECONDS and <= FORK_DAY_INDEXES so it reads Day indexes)
            let fork_res = forked_page(
                &store,
                hour,
                hour + 4 * 3600,
                false,
                false,
                (hour, hour + 4 * 3600),
            );
            assert_eq!(
                fork_res.err().as_deref(),
                expected_err,
                "iter39 ({label}): discovery::forked_page Day validation mismatch"
            );
        }
    }
}
