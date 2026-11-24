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
    #[serde(default)]
    pub visited_blocks: BTreeSet<String>,
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
        if inventory.visited_blocks.contains(&reference.key) {
            return Err("catalog_parent_invalid".into());
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
        inventory.visited_blocks.insert(reference.key.clone());
        if inventory.visited_blocks.len() > MAX_SOURCES {
            return Err("catalog_identity_admission".into());
        }
        if block.previous.as_ref().is_some_and(|parent| {
            parent.generation > reference.generation
                || parent.key == reference.key
                || inventory.visited_blocks.contains(&parent.key)
        }) {
            return Err("catalog_parent_invalid".into());
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
        fn admin_get(&self, _: &str) -> Result<Option<Record>, String> {
            Ok(None)
        }
        fn admin_cas(&self, _: &str, _: Option<&str>, _: &Value) -> Result<bool, String> {
            Ok(true)
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
            let key = request.key.trim_start_matches('/');
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
    fn iter36_f7_bundles_collect_rejects_equal_gen_cycle_self_loop_and_upward_parent() {
        let svc = test_service();
        let hour = 1_800_000_000i64;
        let key_a = "processed/_catalog/block-a.json".to_owned();
        let key_b = "processed/_catalog/block-b.json".to_owned();

        // 1. 2-node equal-gen cycle A (gen 5) -> B (gen 5) -> A (gen 5)
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
            objects: RefCell::new(BTreeMap::from([
                (key_a.clone(), serde_json::to_vec(&blk_a).unwrap()),
                (key_b.clone(), serde_json::to_vec(&blk_b).unwrap()),
            ])),
            remaining: Cell::new(20),
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
        let mut inv: Inventory = serde_json::from_value(json!({
            "hour": hour,
            "generation": 5,
            "current": { "key": key_a, "generation": 5 },
            "windows": [],
            "closed": false
        }))
        .unwrap();
        let res = collect(&store, &mut inv, 0);
        assert_eq!(res.err().as_deref(), Some("catalog_parent_invalid"));
        assert!(
            rt.gets.get() <= 2,
            "must terminate immediately after visiting A and B"
        );

        // 2. Cross-tick budget pause (visit A -> Ok(false), resume on B -> Err("catalog_parent_invalid"))
        rt.remaining.set(2);
        rt.gets.set(0);
        let mut inv_pause: Inventory = serde_json::from_value(json!({
            "hour": hour,
            "generation": 5,
            "current": { "key": key_a, "generation": 5 },
            "windows": [],
            "closed": false
        }))
        .unwrap();
        assert_eq!(collect(&store, &mut inv_pause, 0), Ok(false));
        let roundtripped: Inventory =
            serde_json::from_slice(&serde_json::to_vec(&inv_pause).unwrap()).unwrap();
        let mut inv_resumed = roundtripped;
        rt.remaining.set(10);
        assert_eq!(
            collect(&store, &mut inv_resumed, 0).err().as_deref(),
            Some("catalog_parent_invalid")
        );

        // 3. Upward generation parent (parent.generation > reference.generation)
        let blk_up = Block {
            version: VERSION,
            service_id: svc.service_id.clone(),
            generation: 5,
            hour,
            previous: Some(Reference {
                key: key_b.clone(),
                generation: 6,
            }),
            files: vec![],
            summaries: vec![],
            replaces: vec![],
            committed_at: BTreeMap::new(),
        };
        rt.objects
            .borrow_mut()
            .insert(key_a.clone(), serde_json::to_vec(&blk_up).unwrap());
        rt.remaining.set(10);
        let mut inv_up: Inventory = serde_json::from_value(json!({
            "hour": hour,
            "generation": 6,
            "current": { "key": key_a, "generation": 5 },
            "windows": [],
            "closed": false
        }))
        .unwrap();
        assert_eq!(
            collect(&store, &mut inv_up, 0).err().as_deref(),
            Some("catalog_parent_invalid")
        );
    }
}
