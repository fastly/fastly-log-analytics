use super::*;
use discovery::SNAPSHOT_SECONDS;
use serde_json::json;

const LIMIT: usize = 4 * 1024 * 1024;
/// Most journal manifests one delta page reads concurrently.
const PREFETCH_LIMIT: usize = 8;
const IDENTITIES: usize = 16_384;

#[derive(Serialize, Deserialize)]
struct State {
    service: String,
    from: i64,
    to: i64,
    previous_from: i64,
    previous_to: i64,
    summaries: bool,
    summary_bounds: (i64, i64),
    previous_summary_bounds: (i64, i64),
    base: u64,
    root: Head,
    created: u64,
    expected: u64,
    #[serde(default)]
    base_checked: bool,
    journal: Option<String>,
    changes: Vec<Value>,
    days: BTreeSet<i64>,
    markers_sent: bool,
    strips: Vec<(i64, i64)>,
    strip: usize,
    strip_cursor: Option<String>,
    summary_days: Option<Vec<i64>>,
    summary_day: usize,
    summary_cursor: Option<String>,
    bytes: usize,
    identities: BTreeSet<String>,
}

fn overlaps(file: &Output, from: i64, to: i64) -> bool {
    file_bounds(&file.key).is_some_and(|(a, b)| a < to && b > from)
}

fn add_days(days: &mut BTreeSet<i64>, from: i64, to: i64) {
    let mut at = day_start(from);
    while at < to {
        days.insert(at);
        at += 86_400;
    }
}

#[allow(clippy::too_many_arguments)]
pub fn page<R: Runtime>(
    store: &Store<'_, R>,
    from: i64,
    to: i64,
    previous_from: i64,
    previous_to: i64,
    base: u64,
    summaries: bool,
    summary_bounds: (i64, i64),
    previous_summary_bounds: (i64, i64),
    checkpoint: Option<&str>,
) -> Result<Value, String> {
    let runtime = store.runtime;
    let mut state = if let Some(key) = checkpoint {
        if !key.starts_with(&format!(
            "{}processed/_catalog/delta-",
            scope(store.service)
        )) {
            return Err("catalog_cursor_invalid".into());
        }
        let body = store
            .get_direct(key, LIMIT)?
            .ok_or("catalog_resync_required")?;
        let state: State = serde_json::from_slice(&body).map_err(|_| "catalog_cursor_invalid")?;
        if state.service != store.service.service_id
            || state.from != from
            || state.to != to
            || state.previous_from != previous_from
            || state.previous_to != previous_to
            || state.base != base
            || state.summaries != summaries
            || state.summary_bounds != summary_bounds
            || state.previous_summary_bounds != previous_summary_bounds
        {
            return Err("catalog_cursor_invalid".into());
        }
        if state.created.saturating_add(SNAPSHOT_SECONDS) <= runtime.now() {
            return Err("catalog_resync_required".into());
        }
        state
    } else {
        if runtime.admin_remaining_calls() < 3 {
            return Err("catalog_retry".into());
        }
        let record = runtime.admin_get(WATERMARK_KEY)?;
        let root = head(record.as_ref().map(|r| &r.value).unwrap_or(&Value::Null))?;
        if root.generation < base {
            return Err("catalog_head_behind".into());
        }
        if base > 0 && (root.version != VERSION || root.manifest.is_none()) {
            return Err("catalog_resync_required".into());
        }
        if root.generation - base > REPLAY_COMMITS as u64 {
            return Err("catalog_resync_required".into());
        }
        let mut strips = Vec::new();
        if from < previous_from {
            strips.push((from, to.min(previous_from)));
        }
        if to > previous_to {
            strips.push((from.max(previous_to), to));
        }
        let mut days = BTreeSet::new();
        if summaries {
            for &(a, b) in &strips {
                add_days(&mut days, a, b);
            }
            if from != previous_from
                || to != previous_to
                || summary_bounds != previous_summary_bounds
            {
                for at in [from, to - 1, previous_from, previous_to - 1] {
                    let day = day_start(at);
                    if day < to && day + 86_400 > from {
                        days.insert(day);
                    }
                }
            }
        }
        State {
            service: store.service.service_id.clone(),
            from,
            to,
            previous_from,
            previous_to,
            summaries,
            summary_bounds,
            previous_summary_bounds,
            base,
            expected: root.generation,
            base_checked: base == 0 || root.generation == base,
            journal: root.manifest.clone(),
            root,
            created: runtime.now(),
            changes: Vec::new(),
            days,
            markers_sent: false,
            strips,
            strip: 0,
            strip_cursor: None,
            summary_days: None,
            summary_day: 0,
            summary_cursor: None,
            bytes: 0,
            identities: BTreeSet::new(),
        }
    };
    // The head names each recent commit's manifest, so the journal walk's reads go out together
    // instead of one round trip per commit; each body is still checked against the walk's key.
    let mut prefetched = Prefetched::new();
    let upcoming = |state: &State| {
        state
            .root
            .recent
            .iter()
            .rev()
            .filter(|event| {
                event.generation < state.expected
                    && (event.generation > state.base
                        || (event.generation == state.base && !state.base_checked))
            })
            .filter_map(|event| Some((event.manifest.clone()?, LIMIT)))
            .collect::<Vec<_>>()
    };
    while state.expected > state.base {
        let journal = state.journal.clone();
        if (!journal
            .as_ref()
            .is_some_and(|key| already_sent(store, &prefetched, key))
            && runtime.admin_remaining_calls() <= 3)
            || !admitted(store)
        {
            break;
        }
        let key = journal.ok_or("catalog_resync_required")?;
        let body = fetch_batched(store, &mut prefetched, &key, LIMIT, PREFETCH_LIMIT, || {
            upcoming(&state)
        })?
        .ok_or("catalog_resync_required")?;
        let manifest: Manifest =
            serde_json::from_slice(&body).map_err(|_| "catalog_resync_required")?;
        if manifest.version != VERSION
            || manifest.service_id != state.service
            || manifest.generation != state.expected
            || manifest.batch == "retention"
            || manifest.batch == "reset"
        {
            return Err("catalog_resync_required".into());
        }
        let files = manifest
            .files
            .iter()
            .filter(|file| overlaps(file, from, to))
            .map(|file| discovery::entry(file, manifest.generation))
            .collect::<Vec<_>>();
        state.changes.push(json!({"generation":manifest.generation,
            "files":files, "replaces":manifest.replaces}));
        if summaries {
            for file in &manifest.summaries {
                if let Some((a, b)) = file_bounds(&file.key) {
                    if a < to && b > from {
                        add_days(&mut state.days, a.max(from), b.min(to));
                    }
                }
            }
        }
        state.journal = manifest.previous;
        state.expected -= 1;
        if serde_json::to_vec(&state.changes)
            .map_err(|e| e.to_string())?
            .len()
            > LIMIT
        {
            return Err("catalog_resync_required".into());
        }
    }
    let mut changes = Vec::new();
    let mut partitions = Vec::new();
    let mut files = Vec::new();
    let mut summary_files = Vec::new();
    let base_key = state.journal.clone();
    if state.expected == base
        && !state.base_checked
        && (base_key
            .as_ref()
            .is_some_and(|key| already_sent(store, &prefetched, key))
            || runtime.admin_remaining_calls() > 3)
        && admitted(store)
    {
        let key = base_key.as_ref().ok_or("catalog_resync_required")?;
        let body = fetch_batched(store, &mut prefetched, key, LIMIT, 1, Vec::new)?
            .ok_or("catalog_resync_required")?;
        let manifest: Manifest =
            serde_json::from_slice(&body).map_err(|_| "catalog_resync_required")?;
        if manifest.version != VERSION
            || manifest.service_id != state.service
            || manifest.generation != base
        {
            return Err("catalog_resync_required".into());
        }
        state.base_checked = true;
    }
    if state.expected == base && state.base_checked {
        changes = std::mem::take(&mut state.changes);
        changes.reverse();
        if !state.markers_sent {
            partitions = state
                .days
                .iter()
                .map(|day| json!({"from":rfc3339(*day),"to":rfc3339(*day+86_400)}))
                .collect();
            state.markers_sent = true;
            state.summary_days = Some(state.days.iter().copied().collect());
        }
        while state.strip < state.strips.len()
            && runtime.admin_remaining_calls() > 5
            && admitted(store)
        {
            let (a, b) = state.strips[state.strip];
            let page = discovery::pinned_page(
                store,
                a,
                b,
                false,
                false,
                state.strip_cursor.as_deref(),
                Some(state.root.clone()),
            )?;
            files.extend(page.files);
            state.strip_cursor = page.next;
            if state.strip_cursor.is_some() {
                break;
            }
            state.strip += 1;
            if files.len() >= super::super::api::FILES_PAGE {
                break;
            }
        }
        let days = state.summary_days.as_deref().unwrap_or_default();
        while state.strip == state.strips.len()
            && state.summary_day < days.len()
            && runtime.admin_remaining_calls() > 5
            && admitted(store)
        {
            let day = days[state.summary_day];
            let mut root = state.root.clone();
            root.days.retain(|at, _| *at == day);
            let page = discovery::pinned_page_precise(
                store,
                from,
                to,
                true,
                false,
                state.summary_cursor.as_deref(),
                Some(root),
                summary_bounds,
            )?;
            summary_files.extend(page.files);
            state.summary_cursor = page.next;
            if state.summary_cursor.is_some() {
                break;
            }
            state.summary_day += 1;
            if summary_files.len() >= super::super::api::FILES_PAGE {
                break;
            }
        }
    }
    let mut response = json!({"base_generation":base,"catalog_generation":state.root.generation,
        "files":files,"summary_files":summary_files,"changes":changes,
        "summary_partitions":partitions,"next_cursor":null,"complete":false});
    state.bytes += serde_json::to_vec(&response)
        .map_err(|e| e.to_string())?
        .len();
    for array in [
        response["files"].as_array(),
        response["summary_files"].as_array(),
    ] {
        for file in array.into_iter().flatten() {
            if let Some(key) = file["key"].as_str() {
                state.identities.insert(key.to_owned());
            }
        }
    }
    for change in response["changes"].as_array().into_iter().flatten() {
        for file in change["files"].as_array().into_iter().flatten() {
            if let Some(key) = file["key"].as_str() {
                state.identities.insert(key.to_owned());
            }
        }
        for key in change["replaces"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
        {
            state.identities.insert(key.to_owned());
        }
    }
    if state.bytes > LIMIT || state.identities.len() > IDENTITIES {
        return Err("catalog_resync_required".into());
    }
    let complete = state.expected == base
        && state.base_checked
        && state.strip == state.strips.len()
        && state.summary_day == state.summary_days.as_ref().map_or(0, Vec::len);
    if complete {
        response["complete"] = true.into();
    } else {
        let body = serde_json::to_vec(&state).map_err(|e| e.to_string())?;
        if body.len() > LIMIT {
            return Err("catalog_resync_required".into());
        }
        let key = format!(
            "{}processed/_catalog/delta-{}.json",
            scope(store.service),
            hex::encode(Sha256::digest(&body))
        );
        store.put_verified(&key, body)?;
        response["next_cursor"] = key.into();
    }
    Ok(response)
}
