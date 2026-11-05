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

#[derive(Default, Serialize, Deserialize)]
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
            crate::ingest::bundle::NameKind::Hour => "hour",
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
                (summary_bounds.0 <= *hour && summary_bounds.1 >= hour + 3600)
                    .then(|| state.hour_bundles.get(hour))
                    .flatten()
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
    let mut state = if let Some(key) = checkpoint {
        if !key.starts_with(&format!(
            "{}processed/_catalog/",
            crate::ingest::scope(store.service)
        )) {
            return Err("catalog_cursor_invalid".into());
        }
        let body = store
            .get_direct(key, STATE_LIMIT)?
            .ok_or("catalog_cursor_expired")?;
        let state: State = serde_json::from_slice(&body).map_err(|_| "catalog_cursor_invalid")?;
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
        state
    } else {
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
        state
    };
    let mut files = Vec::new();
    let mut summaries = Vec::new();
    let mut prefetched = Prefetched::new();
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
            state.day_hours.clear();
            state.day_bundle = if (summary || include_summaries)
                && summary_bounds.0 <= day
                && summary_bounds.1 >= day + 86_400
            {
                state.day_bundles.get(&day).cloned()
            } else {
                None
            };
            state.day_bundle_ready = false;
            state.summary_counts = index.summary_counts;
            state.hours = index
                .hours
                .into_iter()
                .filter(|(hour, _)| {
                    *hour < to && hour.saturating_add(3600) > from.saturating_sub(3600)
                })
                .collect();
            state.day += 1;
            state.hour = 0;
            state.current = state.hours.first().map(|(_, r)| r.clone());
            state.offset = 0;
            state.summary_offset = 0;
            state.retired.clear();
            state.seen.clear();
            state.summaries_seen.clear();
            state.summary_ready = false;
            state.covered_windows.clear();
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
                if summary {
                    files.push(entry(&coverage.file, state.generation));
                } else {
                    summaries.push(entry(&coverage.file, state.generation));
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
            let selected = state.day_hours.get(&hour).or_else(|| {
                if summary_bounds.0 <= hour && summary_bounds.1 >= hour + 3600 {
                    state.hour_bundles.get(&hour)
                } else {
                    None
                }
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
        day_bundles: root.day_bundles,
        ..Default::default()
    }
}

use sha2::Digest;
