use super::*;

pub const COVERAGE_LIMIT: usize = 2 * 1024 * 1024;
const MAX_SOURCES: usize = 16_384;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Coverage {
    pub file: Output,
    pub start: i64,
    pub end: i64,
    pub windows: BTreeSet<String>,
    pub hours: BTreeMap<i64, Reference>,
    pub built_generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Inventory {
    pub hour: i64,
    pub generation: u64,
    pub current: Option<Reference>,
    pub windows: BTreeSet<String>,
}

pub fn collect<R: Runtime>(
    store: &Store<'_, R>,
    inventory: &mut Inventory,
    reserve: u32,
) -> Result<bool, String> {
    while let Some(reference) = inventory.current.clone() {
        if store.runtime.admin_remaining_calls() <= reserve + 1 || !admitted(store) {
            return Ok(false);
        }
        let block: Block = read(store, &reference.key)?;
        if block.version != VERSION
            || block.service_id != store.service.service_id
            || block.hour != inventory.hour
            || block.generation != reference.generation
            || block.generation > inventory.generation
        {
            return Err("catalog_manifest_invalid".into());
        }
        inventory
            .windows
            .extend(block.summaries.into_iter().map(|file| file.key));
        if inventory.windows.len() > MAX_SOURCES
            || serde_json::to_vec(inventory)
                .map_err(|e| e.to_string())?
                .len()
                > COVERAGE_LIMIT
        {
            return Err("catalog_bundle_admission".into());
        }
        inventory.current = block.previous;
    }
    Ok(true)
}

pub fn load<R: Runtime>(store: &Store<'_, R>, reference: &Reference) -> Result<Coverage, String> {
    let bytes = store
        .get_direct(&reference.key, COVERAGE_LIMIT)?
        .ok_or("catalog_object_not_visible")?;
    parse(&bytes)
}

pub fn parse(bytes: &[u8]) -> Result<Coverage, String> {
    let coverage: Coverage = serde_json::from_slice(bytes).map_err(|_| "catalog_object_invalid")?;
    if coverage.windows.len() > MAX_SOURCES || coverage.hours.len() > 24 {
        return Err("catalog_bundle_admission".into());
    }
    Ok(coverage)
}

pub fn publish<R: Runtime>(
    store: &Store<'_, R>,
    coverage: &Coverage,
    reserve: u32,
) -> Result<bool, String> {
    let runtime = store.runtime;
    if runtime.admin_remaining_calls() < reserve + 6 || !admitted(store) {
        return Ok(false);
    }
    let record = runtime
        .admin_get(WATERMARK_KEY)?
        .ok_or("catalog_bundle_not_active")?;
    let mut root = head(&record.value)?;
    if root.version != VERSION || !root.days.contains_key(&day_start(coverage.start)) {
        return Err("catalog_bundle_not_active".into());
    }
    if store.service.log_retention_days > 0 {
        let cutoff = (runtime.now() as i64 - i64::from(store.service.log_retention_days) * 86_400)
            .div_euclid(3600)
            * 3600;
        if (coverage.hours.is_empty() && coverage.start < cutoff)
            || coverage.hours.keys().any(|hour| *hour < cutoff)
        {
            return Err("catalog_bundle_not_active".into());
        }
    }
    let body = serde_json::to_vec(coverage).map_err(|e| e.to_string())?;
    if body.len() > COVERAGE_LIMIT {
        return Err("catalog_bundle_admission".into());
    }
    let digest = hex::encode(Sha256::digest(&body));
    let key = format!("{}processed/_catalog/{digest}.json", scope(store.service));
    let is_day = coverage.end - coverage.start == 86_400;
    let previous = if is_day {
        &root.day_bundles
    } else {
        &root.hour_bundles
    };
    if previous.get(&coverage.start).is_some_and(|r| r.key == key) {
        return Ok(true);
    }
    if root.bundle_retired.contains_key(&coverage.file.key) {
        return Err("catalog_bundle_not_active".into());
    }
    if previous
        .get(&coverage.start)
        .is_some_and(|r| r.generation > coverage.built_generation)
    {
        return Err("catalog_bundle_not_active".into());
    }
    let replaced = previous
        .get(&coverage.start)
        .map(|reference| load(store, reference))
        .transpose()?;
    if replaced
        .as_ref()
        .is_some_and(|old| old.file.key == coverage.file.key)
    {
        return Ok(true);
    }
    let generation = root
        .generation
        .checked_add(1)
        .filter(|n| *n <= 9_007_199_254_740_991)
        .ok_or("catalog_generation_exhausted")?;
    store.put_verified(&key, body)?;
    let at = rfc3339(runtime.now() as i64);
    let manifest = Manifest {
        version: VERSION,
        service_id: store.service.service_id.clone(),
        generation,
        batch: "summary_bundle".into(),
        schema: String::new(),
        committed_at: at.clone(),
        previous: root.manifest.clone(),
        files: Vec::new(),
        summaries: vec![coverage.file.clone()],
        sources: Vec::new(),
        replaces: Vec::new(),
        bytes: 0,
        rows: 0,
        parents: BTreeMap::new(),
    };
    let manifest_key = put(store, &manifest)?;
    let reference = Reference { key, generation };
    if let Some(old) = replaced {
        root.bundle_retired.insert(
            old.file.key,
            runtime
                .now()
                .saturating_add(super::super::compaction::DELETE_GRACE_S),
        );
    }
    if is_day {
        root.day_bundles.insert(coverage.start, reference);
    } else {
        root.hour_bundles.insert(coverage.start, reference);
    }
    root.generation = generation;
    root.manifest = Some(manifest_key.clone());
    root.recent.push(Event {
        cursor: format!("{}:{generation}", store.service.service_id),
        generation,
        committed_at: Some(at),
        manifest: Some(manifest_key),
    });
    if root.recent.len() > REPLAY_COMMITS {
        root.recent.drain(..root.recent.len() - REPLAY_COMMITS);
    }
    let mut value = record.value;
    value["catalog"] = serde_json::to_value(root).map_err(|e| e.to_string())?;
    runtime.admin_cas(WATERMARK_KEY, Some(&record.generation), &value)
}

pub fn cleanup<R: Runtime>(
    store: &Store<'_, R>,
    record: Option<&crate::admin::Record>,
    reserve: u32,
) -> Result<u64, String> {
    let Some(record) = record else {
        return Ok(0);
    };
    let mut root = head(&record.value)?;
    let keys = root
        .bundle_retired
        .iter()
        .filter(|(_, at)| **at > 0 && **at <= store.runtime.now())
        .take(1000)
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    if keys.is_empty() || store.runtime.admin_remaining_calls() < reserve + 2 || !admitted(store) {
        return Ok(0);
    }
    let deleted = store.delete_objects(&keys)?;
    if !deleted.failed.is_empty() {
        return Err("catalog_bundle_delete_partial".into());
    }
    for key in &deleted.deleted {
        root.bundle_retired.insert(key.clone(), 0);
    }
    let mut value = record.value.clone();
    value["catalog"] = serde_json::to_value(root).map_err(|e| e.to_string())?;
    if !store
        .runtime
        .admin_cas(WATERMARK_KEY, Some(&record.generation), &value)?
    {
        store
            .runtime
            .admin_event(serde_json::json!({"event":"catalog.bundle_cleanup_conflict"}));
    }
    Ok(deleted.deleted.len() as u64)
}
