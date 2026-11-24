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
    /// Snapshotted after the hour closed, so it may build the hour's final bundle.
    #[serde(default)]
    pub closed: bool,
}

/// Catalog reads and commits made by the `summary_bundle` stage. They stop at the stage's own
/// fold stop ([`crate::ingest::bundle::VCPU_STOP_MS`]), which leaves room for an encode, rather
/// than the ingest commit's [`super::VCPU_STOP_MS`]: stage executions often reach bundling past
/// that, and a gate there left them doing nothing.
pub fn admitted<R: Runtime>(store: &Store<'_, R>) -> bool {
    store.within_wall()
        && store.vcpu_ms() < crate::ingest::bundle::VCPU_STOP_MS
        && store.runtime.heap_mib().is_none_or(|heap| heap < 96)
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

pub enum PublishOutcome {
    /// Deferred due to call/vCPU/heap budget or CAS conflict; retry next execution.
    Deferred,
    /// Committed (or already committed idempotently); carries the active catalog head.
    Published(Head),
    /// Pending publication was superseded by a newer generation, closed hour bundle,
    /// retirement, or retention cutoff. Carries the active catalog head for the rest of the tick.
    Superseded { head: Head, reason: &'static str },
}

pub fn publish<R: Runtime>(
    store: &Store<'_, R>,
    coverage: &Coverage,
    reserve: u32,
) -> Result<PublishOutcome, String> {
    let runtime = store.runtime;
    if runtime.admin_remaining_calls() < reserve + 6 || !admitted(store) {
        return Ok(PublishOutcome::Deferred);
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
            return Ok(PublishOutcome::Superseded {
                head: root,
                reason: "retention_cutoff",
            });
        }
    }
    let body = serde_json::to_vec(coverage).map_err(|e| e.to_string())?;
    if body.len() > COVERAGE_LIMIT {
        return Err("catalog_bundle_admission".into());
    }
    let digest = hex::encode(Sha256::digest(&body));
    let key = format!("{}processed/_catalog/{digest}.json", scope(store.service));
    let is_open = super::super::bundle::parse_name(&coverage.file.key)
        .is_some_and(|n| n.kind == super::super::bundle::NameKind::Open);
    let is_day = !is_open && coverage.end - coverage.start == 86_400;
    if is_open {
        if let Some(reference) = root.hour_bundles.get(&coverage.start) {
            let expected_end = coverage.start + 3600;
            if reference.generation <= coverage.built_generation
                || reference.generation > root.generation
            {
                return Err("catalog_bundle_not_active".into());
            }
            let closed =
                load(store, reference).map_err(|_| "catalog_bundle_not_active".to_owned())?;
            let name = super::super::bundle::parse_name(&closed.file.key);
            if closed.built_generation >= reference.generation
                || closed.start != coverage.start
                || closed.end != expected_end
                || !name.is_some_and(|name| name.kind == super::super::bundle::NameKind::Hour)
                || super::file_bounds(&closed.file.key) != Some((coverage.start, expected_end))
            {
                return Err("catalog_bundle_not_active".into());
            }
            return Ok(PublishOutcome::Superseded {
                head: root,
                reason: "hour_bundle_closed",
            });
        }
    }
    let previous = if is_day {
        &root.day_bundles
    } else if is_open {
        &root.open_bundles
    } else {
        &root.hour_bundles
    };
    if previous.get(&coverage.start).is_some_and(|r| r.key == key) {
        return Ok(PublishOutcome::Published(root));
    }
    if root.bundle_retired.contains_key(&coverage.file.key) {
        return Ok(PublishOutcome::Superseded {
            head: root,
            reason: "bundle_retired",
        });
    }
    if previous
        .get(&coverage.start)
        .is_some_and(|r| r.generation > coverage.built_generation)
    {
        return Ok(PublishOutcome::Superseded {
            head: root,
            reason: "newer_generation",
        });
    }
    let replaced = previous
        .get(&coverage.start)
        .map(|reference| load(store, reference))
        .transpose()?;
    if replaced
        .as_ref()
        .is_some_and(|old| old.file.key == coverage.file.key)
    {
        return Ok(PublishOutcome::Published(root));
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
    let manifest_key = put_manifest(
        store,
        &manifest,
        root.reset_generation,
        root.reset_at,
        Some((&root.days, &root.statistics)),
        MANIFEST_LIMIT,
    )?;
    let reference = Reference { key, generation };
    let grace = if is_open {
        super::super::bundle::RETIRE_GRACE_S as u64
    } else {
        super::super::compaction::DELETE_GRACE_S
    };
    if let Some(old) = replaced {
        root.bundle_retired
            .insert(old.file.key, runtime.now().saturating_add(grace));
    }
    if is_day {
        root.day_bundles.insert(coverage.start, reference);
    } else if is_open {
        root.open_bundles.insert(coverage.start, reference);
    } else {
        if let Some(open_ref) = root.open_bundles.remove(&coverage.start) {
            // An open bundle whose coverage is gone has nothing left to retire.
            match load(store, &open_ref) {
                Ok(open_cov) => {
                    root.bundle_retired.insert(
                        open_cov.file.key,
                        runtime
                            .now()
                            .saturating_add(super::super::bundle::RETIRE_GRACE_S as u64),
                    );
                }
                Err(error) if error == "catalog_object_not_visible" => {}
                Err(error) => return Err(error),
            }
        }
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
    value["catalog"] = serde_json::to_value(&root).map_err(|e| e.to_string())?;
    if runtime.admin_cas(WATERMARK_KEY, Some(&record.generation), &value)? {
        Ok(PublishOutcome::Published(root))
    } else {
        Ok(PublishOutcome::Deferred)
    }
}

/// Removes the listed open, hour or day bundle `references` whose objects are gone, in one
/// catalog commit naming their files so delta readers refresh those days. `Ok(None)` when out of
/// budget or the watermark moved; `Ok(Some(head))` is the committed (or already clean) head.
pub fn drop_missing<R: Runtime>(
    store: &Store<'_, R>,
    references: &[(Reference, String)],
    reserve: u32,
) -> Result<Option<Head>, String> {
    let runtime = store.runtime;
    if runtime.admin_remaining_calls() < reserve + 3 || !admitted(store) {
        return Ok(None);
    }
    let record = runtime
        .admin_get(WATERMARK_KEY)?
        .ok_or("catalog_bundle_not_active")?;
    let mut root = head(&record.value)?;
    let mut dropped = Vec::new();
    for map in [
        &mut root.open_bundles,
        &mut root.hour_bundles,
        &mut root.day_bundles,
    ] {
        map.retain(|_, reference| {
            let Some((_, file)) = references.iter().find(|(r, _)| r == reference) else {
                return true;
            };
            dropped.push(Output {
                key: file.clone(),
                ..Output::default()
            });
            false
        });
    }
    if dropped.is_empty() {
        return Ok(Some(root));
    }
    let generation = root
        .generation
        .checked_add(1)
        .filter(|n| *n <= 9_007_199_254_740_991)
        .ok_or("catalog_generation_exhausted")?;
    let at = rfc3339(runtime.now() as i64);
    let manifest = Manifest {
        version: VERSION,
        service_id: store.service.service_id.clone(),
        generation,
        batch: "summary_bundle_repair".into(),
        schema: String::new(),
        committed_at: at.clone(),
        previous: root.manifest.clone(),
        files: Vec::new(),
        summaries: dropped,
        sources: Vec::new(),
        replaces: Vec::new(),
        bytes: 0,
        rows: 0,
        parents: BTreeMap::new(),
    };
    let manifest_key = put_manifest(
        store,
        &manifest,
        root.reset_generation,
        root.reset_at,
        Some((&root.days, &root.statistics)),
        MANIFEST_LIMIT,
    )?;
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
    value["catalog"] = serde_json::to_value(&root).map_err(|e| e.to_string())?;
    if runtime.admin_cas(WATERMARK_KEY, Some(&record.generation), &value)? {
        Ok(Some(root))
    } else {
        Ok(None)
    }
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
    // Keys that failed stay due and are retried; a missing object counts as deleted.
    let deleted = store.delete_objects(&keys)?;
    if deleted.deleted.is_empty() {
        return if deleted.failed.is_empty() {
            Ok(0)
        } else {
            Err("catalog_bundle_delete_partial".into())
        };
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
