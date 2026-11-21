use super::support::*;
use fastly_log_analytics::{
    admin::{
        dr::{self, CloudDrState, SCHEMA_VERSION},
        FosRequest, FosResponse, Record, Service,
    },
    ingest::catalog::{self, Day, Event, Head, Manifest, Reference, Statistics, VERSION},
    setup::SetupStores,
    shell::Runtime,
    views::SavedView,
};
use serde_json::{json, Value};
use std::collections::BTreeMap;

#[test]
fn dr_mirror_written_on_service_patch_and_contains_no_secrets() {
    let platform = Platform::new();
    let provisioned = platform.provision();
    platform.drain(provisioned["job"].clone());

    let objects_after_provision = platform.adapter.bucket_objects("fos-fixture-logs");
    assert!(
        objects_after_provision.contains_key("meta/cloud_dr_state.json"),
        "expected finalize to mirror meta/cloud_dr_state.json"
    );

    FOS_LOG.with(|log| log.borrow_mut().clear());
    let patched = platform.json(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({
            "generation": platform.service()["generation"],
            "log_period": 120,
            "log_retention_days": 45
        }),
    );
    assert_eq!(patched["service"]["log_period"], 120);

    let fos_puts: Vec<_> = FOS_LOG.with(|log| {
        log.borrow()
            .iter()
            .filter(|(m, k, _)| m == "PUT" && k == "meta/cloud_dr_state.json")
            .cloned()
            .collect()
    });
    assert!(
        !fos_puts.is_empty(),
        "expected PATCH to mirror meta/cloud_dr_state.json"
    );

    let bytes = platform
        .adapter
        .bucket_objects("fos-fixture-logs")
        .remove("meta/cloud_dr_state.json")
        .expect("dr state object");
    let text = String::from_utf8(bytes).expect("utf8 dr state");
    assert!(!text.contains("fixture-token"));
    assert!(!text.contains("fastly_token_ref"));
    assert!(!text.contains("fos_access_key_ref"));
    assert!(!text.contains("fos_secret_key_ref"));
    assert!(!text.contains("pending_job_id"));

    let state: CloudDrState = serde_json::from_str(&text).expect("parse CloudDrState");
    assert_eq!(state.schema_version, SCHEMA_VERSION);
    assert_eq!(state.service_id, "fixtureService");
    assert_eq!(state.service["log_period"], 120);
    assert_eq!(state.service["log_retention_days"], 45);
}

#[test]
fn recover_from_dr_state_api_key_only_rehydrates_kv_and_front_dicts() {
    let platform = Platform::new();
    let head = Head {
        version: VERSION,
        generation: 7,
        manifest: Some("processed/_catalog/m7.json".into()),
        recent: vec![Event {
            cursor: "fixtureService:7".into(),
            generation: 7,
            committed_at: Some("2026-11-20T00:00:00Z".into()),
            manifest: Some("processed/_catalog/m7.json".into()),
        }],
        days: BTreeMap::from([(1_700_006_400, "processed/_catalog/d1.json".into())]),
        ..Head::default()
    };
    let dr_state = CloudDrState {
        schema_version: SCHEMA_VERSION,
        updated_at: 1_800_000_000,
        service_id: "fixtureService".into(),
        service: json!({
            "name": "Recovered Prod",
            "log_period": 180,
            "log_retention_days": 60,
            "sample_rate": 50
        }),
        views: vec![
            SavedView {
                id: "v1".into(),
                service_id: "fixtureService".into(),
                name: "5xx Errors".into(),
                filters_json: r#"{"status":"5xx"}"#.into(),
                time_range_type: Some("relative".into()),
                start_time: None,
                end_time: None,
                page: Some("overview".into()),
                created_at: Some("2026-11-20T00:00:00Z".into()),
            },
            SavedView {
                id: "v2".into(),
                service_id: "fixtureService".into(),
                name: "Slow Origins".into(),
                filters_json: r#"{"origin_latency_ms":500}"#.into(),
                time_range_type: Some("relative".into()),
                start_time: None,
                end_time: None,
                page: Some("origin".into()),
                created_at: Some("2026-11-20T00:01:00Z".into()),
            },
        ],
        catalog_head: Some(head),
        floor_key: Some("raw/2026/floor.zst".into()),
        rum_stats: Some(json!({"latest_log": 1_800_000_000, "rows": 99, "at": 1_800_000_010})),
    };
    platform.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "meta/cloud_dr_state.json",
        serde_json::to_vec(&dr_state).unwrap(),
    );

    let result = platform.json(
        "POST",
        "/api/admin/provision/recover",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1"
        }),
    );
    assert_eq!(result["ok"], true);
    assert_eq!(result["service_id"], "fixtureService");
    assert_eq!(result["source"], "dr_state");
    assert_eq!(result["views_restored"], 2);
    assert_eq!(result["catalog_generation"], 7);
    assert_eq!(result["front_synced"], true);

    let records = platform.adapter.records();
    assert_eq!(
        records["admin:install"].value["service_id"],
        "fixtureService"
    );
    assert_eq!(records["svc:fixtureService"].value["status"], "active");
    assert_eq!(records["svc:fixtureService"].value["log_period"], 180);
    assert_eq!(
        records["svc:fixtureService"].value["log_retention_days"],
        60
    );
    assert_eq!(
        records["views:fixtureService"].value["views"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let wm = &records["ingest:watermark"].value;
    let parsed_head = catalog::head(wm).expect("valid catalog head");
    assert_eq!(parsed_head.generation, 7);
    assert_eq!(wm["floor_key"], "raw/2026/floor.zst");
    assert_eq!(
        records["ingest:rum:watermark"].value["rum_stats"]["rows"],
        99
    );

    assert_eq!(
        platform
            .adapter
            .secret("fastly_token.fixtureService")
            .unwrap()
            .as_deref(),
        Some("fixture-token")
    );
    let ak = platform
        .adapter
        .secret("fos_ak.fixtureService")
        .unwrap()
        .expect("fos_ak");
    let sk_env: Value = serde_json::from_str(
        &platform
            .adapter
            .secret("fos_sk.fixtureService")
            .unwrap()
            .expect("fos_sk"),
    )
    .unwrap();
    assert_eq!(sk_env["access_key"], ak);
    let sk = sk_env["secret_key"].as_str().unwrap();

    let buckets_dict = platform.adapter.dictionary_items("fixtureBuckets");
    assert_eq!(
        buckets_dict.get("fixtureService").map(String::as_str),
        Some("fos-fixture-logs|us-east-1")
    );
    let creds_dict = platform.adapter.dictionary_items("fixtureCredentials");
    assert_eq!(
        creds_dict.get("fixtureService.ak").map(String::as_str),
        Some(ak.as_str())
    );
    assert_eq!(
        creds_dict.get("fixtureService.sk").map(String::as_str),
        Some(sk)
    );
}

#[test]
fn recover_via_provision_alias_with_explicit_fos_keys() {
    let platform = Platform::new();
    platform
        .adapter
        .seed_fos_key("explicit-ak", "explicit-sk", "fos-fixture-logs");
    let dr_state = CloudDrState {
        schema_version: SCHEMA_VERSION,
        updated_at: 1_800_000_000,
        service_id: "fixtureService".into(),
        service: json!({"log_period": 90}),
        views: vec![],
        catalog_head: None,
        floor_key: None,
        rum_stats: None,
    };
    platform.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "meta/cloud_dr_state.json",
        serde_json::to_vec(&dr_state).unwrap(),
    );
    let keys_before = platform.adapter.key_count();

    let result = platform.json(
        "POST",
        "/api/provision/recover",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1",
            "fos_access_key": "explicit-ak",
            "fos_secret_key": "explicit-sk"
        }),
    );
    assert_eq!(result["ok"], true);
    assert_eq!(result["source"], "dr_state");
    assert_eq!(result["front_synced"], true);
    assert_eq!(
        platform.adapter.key_count(),
        keys_before,
        "explicit FOS keys should not mint a new FOS key"
    );
    assert_eq!(
        platform
            .adapter
            .secret("fos_ak.fixtureService")
            .unwrap()
            .as_deref(),
        Some("explicit-ak")
    );
}

#[test]
fn recover_fallback_scans_catalog_when_dr_state_missing_and_respects_budget() {
    let platform = Platform::new();
    RICH_FOS.with(|r| r.set(true));
    MTIMES.with(|m| m.borrow_mut().clear());

    let day_key = "processed/_catalog/day1.json";
    let manifest_key = "processed/_catalog/manifest12.json";
    let day = Day {
        version: VERSION,
        hours: BTreeMap::from([(
            1_700_006_400,
            Reference {
                key: "processed/_catalog/block12.json".into(),
                generation: 12,
            },
        )]),
        statistics: Statistics {
            earliest: Some(1_700_006_400),
            latest: Some(1_700_009_999),
            earliest_log: None,
            latest_log: None,
            rows: 500,
        },
        ..Day::default()
    };
    let manifest = Manifest {
        version: VERSION,
        service_id: "fixtureService".into(),
        generation: 12,
        batch: "b12".into(),
        schema: "v2".into(),
        committed_at: "2026-11-20T00:00:00Z".into(),
        previous: None,
        files: vec![],
        summaries: vec![],
        sources: vec![],
        replaces: vec![],
        bytes: 1024,
        rows: 500,
        parents: BTreeMap::new(),
    };
    platform.adapter.seed_object_bytes(
        "fos-fixture-logs",
        day_key,
        serde_json::to_vec(&day).unwrap(),
    );
    platform.adapter.seed_object_bytes(
        "fos-fixture-logs",
        manifest_key,
        serde_json::to_vec(&manifest).unwrap(),
    );
    MTIMES.with(|m| {
        let mut m = m.borrow_mut();
        m.insert(day_key.into(), 1_800_000_010);
        m.insert(manifest_key.into(), 1_800_000_020);
    });

    let result = platform.json(
        "POST",
        "/api/admin/provision/recover",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1"
        }),
    );
    RICH_FOS.with(|r| r.set(false));

    assert_eq!(result["ok"], true);
    assert_eq!(result["source"], "catalog_scan");
    assert_eq!(result["catalog_generation"], 12);
    assert_eq!(result["views_restored"], 0);
    assert_eq!(result["front_synced"], true);

    let records = platform.adapter.records();
    let head = catalog::head(&records["ingest:watermark"].value).expect("valid scanned head");
    assert_eq!(head.generation, 12);
    assert_eq!(head.manifest.as_deref(), Some(manifest_key));
    assert_eq!(
        head.days.get(&1_700_006_400).map(String::as_str),
        Some(day_key)
    );
}

#[test]
fn recover_handles_corrupt_dr_state_and_empty_bucket_as_minimal() {
    let platform = Platform::new();
    platform.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "meta/cloud_dr_state.json",
        b"{not valid json".to_vec(),
    );

    let result = platform.json(
        "POST",
        "/api/admin/provision/recover",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1"
        }),
    );
    assert_eq!(result["ok"], true);
    assert_eq!(result["source"], "minimal");
    assert!(result["catalog_generation"].is_null());
    assert_eq!(result["views_restored"], 0);
    assert_eq!(result["front_synced"], true);
}

#[test]
fn recover_rejects_already_installed_service() {
    let platform = Platform::new();
    let provisioned = platform.provision();
    platform.drain(provisioned["job"].clone());

    let response = platform.call(
        "POST",
        "/api/admin/provision/recover",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureOther",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1"
        }),
    );
    assert_eq!(response.status, 409);
    let body: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(body["error"], "install_already_reserved");
}

#[test]
fn re_recovery_after_teardown_respects_budget_with_existing_kv_and_many_catalog_objects() {
    let platform = Platform::new();
    let provisioned = platform.provision();
    platform.drain(provisioned["job"].clone());

    let teardown = platform.json(
        "POST",
        "/api/admin/provision/teardown",
        json!({
            "service_id": "fixtureService",
            "confirm_service_id": "fixtureService",
            "token": "fixture-token",
            "generation": platform.service()["generation"],
            "delete_data": false
        }),
    );
    platform.drain(teardown["job"].clone());
    assert_eq!(platform.service()["service"]["status"], "torn_down");

    // Pre-populate KV targets so upsert_kv performs get -> cas (2 calls each) against existing generations.
    platform
        .adapter
        .cas(
            "views:fixtureService",
            None,
            &json!({"views": [], "updated_at": 1}),
        )
        .unwrap();
    platform
        .adapter
        .cas(
            "ingest:watermark",
            None,
            &json!({"floor_key": "raw/old.zst"}),
        )
        .unwrap();
    platform
        .adapter
        .cas(
            "ingest:rum:watermark",
            None,
            &json!({"rum_stats": {"rows": 1}}),
        )
        .unwrap();

    // Seed DR state with catalog_head: None (forcing scan_catalog_head) + views + floor_key + rum_stats.
    let dr_state = CloudDrState {
        schema_version: SCHEMA_VERSION,
        updated_at: 1_800_000_000,
        service_id: "fixtureService".into(),
        service: json!({"log_period": 120}),
        views: vec![SavedView {
            id: "v-re".into(),
            service_id: "fixtureService".into(),
            name: "Re-recovered View".into(),
            filters_json: r#"{"status":503}"#.into(),
            time_range_type: Some("relative".into()),
            start_time: None,
            end_time: None,
            page: Some("overview".into()),
            created_at: Some("2026-11-20T00:00:00Z".into()),
        }],
        catalog_head: None,
        floor_key: Some("raw/2026/re.zst".into()),
        rum_stats: Some(json!({"rows": 42})),
    };
    platform.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "meta/cloud_dr_state.json",
        serde_json::to_vec(&dr_state).unwrap(),
    );

    RICH_FOS.with(|r| r.set(true));
    MTIMES.with(|m| m.borrow_mut().clear());
    for i in 1u64..=16 {
        let key = format!("processed/_catalog/day_{i:02}.json");
        let day = Day {
            version: VERSION,
            hours: BTreeMap::from([(
                1_700_006_400 + (i as i64) * 86_400,
                Reference {
                    key: format!("processed/_catalog/block_{i:02}.json"),
                    generation: i,
                },
            )]),
            statistics: Statistics {
                earliest: Some(1_700_006_400 + (i as i64) * 86_400),
                latest: Some(1_700_009_999 + (i as i64) * 86_400),
                earliest_log: None,
                latest_log: None,
                rows: 10,
            },
            ..Day::default()
        };
        platform.adapter.seed_object_bytes(
            "fos-fixture-logs",
            &key,
            serde_json::to_vec(&day).unwrap(),
        );
        MTIMES.with(|m| {
            m.borrow_mut().insert(key, 1_800_000_000 + i);
        });
    }
    let manifest_key = "processed/_catalog/manifest_20.json";
    let manifest = Manifest {
        version: VERSION,
        service_id: "fixtureService".into(),
        generation: 20,
        batch: "b20".into(),
        schema: "v2".into(),
        committed_at: "2026-11-20T00:00:00Z".into(),
        previous: None,
        files: vec![],
        summaries: vec![],
        sources: vec![],
        replaces: vec![],
        bytes: 2048,
        rows: 160,
        parents: BTreeMap::new(),
    };
    platform.adapter.seed_object_bytes(
        "fos-fixture-logs",
        manifest_key,
        serde_json::to_vec(&manifest).unwrap(),
    );
    MTIMES.with(|m| {
        m.borrow_mut().insert(manifest_key.into(), 1_800_000_100);
    });

    let response = platform.call(
        "POST",
        "/api/admin/provision/recover",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1"
        }),
    );
    RICH_FOS.with(|r| r.set(false));

    assert_eq!(
        response.status,
        200,
        "re-recovery should not overflow budget: {}",
        String::from_utf8_lossy(&response.body)
    );
    let body: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(body["ok"], true);
    assert_eq!(body["source"], "dr_state");
    assert_eq!(body["views_restored"], 1);
    assert_eq!(body["front_synced"], true);

    let records = platform.adapter.records();
    assert_eq!(records["svc:fixtureService"].value["status"], "active");
    assert_eq!(
        records["admin:install"].value["service_id"],
        "fixtureService"
    );
    assert_eq!(
        records["ingest:watermark"].value["floor_key"],
        "raw/2026/re.zst"
    );
    assert_eq!(
        records["ingest:rum:watermark"].value["rum_stats"]["rows"],
        42
    );
    assert_eq!(
        records["views:fixtureService"].value["views"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn recover_does_not_adopt_foreign_service_catalog_day_objects() {
    let platform = Platform::new();
    RICH_FOS.with(|r| r.set(true));
    MTIMES.with(|m| m.borrow_mut().clear());

    let foreign_dr = CloudDrState {
        schema_version: SCHEMA_VERSION,
        updated_at: 1_800_000_000,
        service_id: "otherService".into(),
        service: json!({"name": "Other"}),
        views: vec![],
        catalog_head: None,
        floor_key: None,
        rum_stats: None,
    };
    platform.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "meta/cloud_dr_state.json",
        serde_json::to_vec(&foreign_dr).unwrap(),
    );
    let foreign_manifest = Manifest {
        version: VERSION,
        service_id: "otherService".into(),
        generation: 99,
        batch: "b99".into(),
        schema: "v2".into(),
        committed_at: "2026-11-20T00:00:00Z".into(),
        previous: None,
        files: vec![],
        summaries: vec![],
        sources: vec![],
        replaces: vec![],
        bytes: 1024,
        rows: 50,
        parents: BTreeMap::new(),
    };
    let foreign_day = Day {
        version: VERSION,
        hours: BTreeMap::from([(
            1_700_006_400,
            Reference {
                key: "processed/_catalog/block99.json".into(),
                generation: 99,
            },
        )]),
        statistics: Statistics {
            earliest: Some(1_700_006_400),
            latest: Some(1_700_009_999),
            earliest_log: None,
            latest_log: None,
            rows: 50,
        },
        ..Day::default()
    };
    platform.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "processed/_catalog/manifest_other.json",
        serde_json::to_vec(&foreign_manifest).unwrap(),
    );
    platform.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "processed/_catalog/day_other.json",
        serde_json::to_vec(&foreign_day).unwrap(),
    );
    MTIMES.with(|m| {
        let mut m = m.borrow_mut();
        m.insert(
            "processed/_catalog/manifest_other.json".into(),
            1_800_000_010,
        );
        m.insert("processed/_catalog/day_other.json".into(), 1_800_000_020);
    });

    let result = platform.json(
        "POST",
        "/api/admin/provision/recover",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1"
        }),
    );
    RICH_FOS.with(|r| r.set(false));

    assert_eq!(result["ok"], true);
    assert_eq!(result["source"], "minimal");
    assert!(result["catalog_generation"].is_null());
    let records = platform.adapter.records();
    assert!(
        !records.contains_key("ingest:watermark"),
        "foreign service Day object must not be written into ingest:watermark"
    );
}

#[test]
fn recover_falls_back_to_catalog_scan_when_dr_state_catalog_head_is_invalid() {
    let platform = Platform::new();
    RICH_FOS.with(|r| r.set(true));
    MTIMES.with(|m| m.borrow_mut().clear());

    let invalid_head = Head {
        version: 0,
        generation: 5,
        manifest: None,
        recent: vec![],
        ..Head::default()
    };
    let dr_state = CloudDrState {
        schema_version: SCHEMA_VERSION,
        updated_at: 1_800_000_000,
        service_id: "fixtureService".into(),
        service: json!({"log_period": 120}),
        views: vec![],
        catalog_head: Some(invalid_head),
        floor_key: None,
        rum_stats: None,
    };
    platform.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "meta/cloud_dr_state.json",
        serde_json::to_vec(&dr_state).unwrap(),
    );
    let manifest_key = "processed/_catalog/manifest15.json";
    let manifest = Manifest {
        version: VERSION,
        service_id: "fixtureService".into(),
        generation: 15,
        batch: "b15".into(),
        schema: "v2".into(),
        committed_at: "2026-11-20T00:00:00Z".into(),
        previous: None,
        files: vec![],
        summaries: vec![],
        sources: vec![],
        replaces: vec![],
        bytes: 1024,
        rows: 100,
        parents: BTreeMap::new(),
    };
    platform.adapter.seed_object_bytes(
        "fos-fixture-logs",
        manifest_key,
        serde_json::to_vec(&manifest).unwrap(),
    );
    MTIMES.with(|m| {
        m.borrow_mut().insert(manifest_key.into(), 1_800_000_050);
    });

    let result = platform.json(
        "POST",
        "/api/admin/provision/recover",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1"
        }),
    );
    RICH_FOS.with(|r| r.set(false));

    assert_eq!(result["ok"], true);
    assert_eq!(result["source"], "dr_state");
    assert_eq!(result["catalog_generation"], 15);
    let records = platform.adapter.records();
    let head = catalog::head(&records["ingest:watermark"].value).expect("valid scanned head");
    assert_eq!(head.generation, 15);
    assert_eq!(head.manifest.as_deref(), Some(manifest_key));
}

struct KvErrorRuntime<'a> {
    inner: &'a Platform,
    fail_key: &'static str,
}

impl SetupStores for KvErrorRuntime<'_> {
    fn config_present(&self, key: &str) -> Result<bool, String> {
        self.inner.config_present(key)
    }
    fn secret_present(&self, key: &str) -> Result<bool, String> {
        self.inner.secret_present(key)
    }
    fn state_linked(&self) -> Result<bool, String> {
        self.inner.state_linked()
    }
}

impl Runtime for KvErrorRuntime<'_> {
    fn config(&self, name: &str) -> Result<Option<String>, String> {
        self.inner.config(name)
    }
    fn secret(&self, name: &str) -> Result<Option<String>, String> {
        self.inner.secret(name)
    }
    fn random(&self) -> Result<[u8; 32], String> {
        self.inner.random()
    }
    fn now(&self) -> u64 {
        self.inner.now()
    }
    fn checks(&self, front_rl: bool) -> Result<Value, String> {
        self.inner.checks(front_rl)
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
        if key == self.fail_key {
            return Err("transient_kv_error".into());
        }
        self.inner.admin_get(key)
    }
    fn admin_fos(&self, request: FosRequest<'_>) -> Result<FosResponse, String> {
        self.inner.admin_fos(request)
    }
    fn admin_remaining_calls(&self) -> u32 {
        self.inner.admin_remaining_calls()
    }
}

#[test]
fn sync_best_effort_aborts_on_low_budget_or_kv_error_without_clobbering_dr_state() {
    let platform = Platform::new();
    let provisioned = platform.provision();
    platform.drain(provisioned["job"].clone());

    let service: Service = serde_json::from_value(
        platform.adapter.records()["svc:fixtureService"]
            .value
            .clone(),
    )
    .expect("service");
    let sentinel = CloudDrState {
        schema_version: SCHEMA_VERSION,
        updated_at: 1_777_777_777,
        service_id: "fixtureService".into(),
        service: json!({"log_period": 999}),
        views: vec![SavedView {
            id: "sentinel-view".into(),
            service_id: "fixtureService".into(),
            name: "Must Not Be Clobbered".into(),
            filters_json: r#"{}"#.into(),
            time_range_type: None,
            start_time: None,
            end_time: None,
            page: None,
            created_at: None,
        }],
        catalog_head: None,
        floor_key: Some("raw/sentinel-floor.zst".into()),
        rum_stats: Some(json!({"rows": 777})),
    };
    let sentinel_bytes = serde_json::to_vec(&sentinel).unwrap();
    platform.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "meta/cloud_dr_state.json",
        sentinel_bytes.clone(),
    );

    // 1. Low call budget (3 remaining calls < 4 required for 3 KV GETs + 1 FOS PUT).
    // Pre-warm secrets so fos_credentials does not consume extra calls before the budget check.
    CALLS.with(|c| c.set(0));
    SECRETS.with(|s| s.borrow_mut().clear());
    let _ = platform.secret(&service.fos_access_key_ref);
    let _ = platform.secret(&service.fos_secret_key_ref);
    CALLS.with(|c| c.set(29)); // 32 - 29 = 3 remaining calls
    dr::sync_best_effort(&platform, &service);
    let after_low_budget = platform
        .adapter
        .bucket_objects("fos-fixture-logs")
        .remove("meta/cloud_dr_state.json")
        .expect("sentinel preserved on low budget");
    assert_eq!(
        after_low_budget, sentinel_bytes,
        "sync_best_effort must not clobber DR state when remaining calls < 4"
    );

    // 2. Transient KV error on admin_get("ingest:watermark") with plenty of call budget.
    CALLS.with(|c| c.set(0));
    SECRETS.with(|s| s.borrow_mut().clear());
    let failing = KvErrorRuntime {
        inner: &platform,
        fail_key: "ingest:watermark",
    };
    dr::sync_best_effort(&failing, &service);
    let after_kv_err = platform
        .adapter
        .bucket_objects("fos-fixture-logs")
        .remove("meta/cloud_dr_state.json")
        .expect("sentinel preserved on kv error");
    assert_eq!(
        after_kv_err, sentinel_bytes,
        "sync_best_effort must not clobber DR state on transient KV read error"
    );
}

#[test]
fn recover_retry_after_fresh_kv_partial_failure_respects_budget() {
    let platform = Platform::new();

    // Simulate a prior partial-failure retry state on fresh KV: svc:fixtureService and
    // admin:install are absent, while secondary KV keys already exist from the earlier attempt.
    assert!(!platform
        .adapter
        .records()
        .contains_key("svc:fixtureService"));
    platform
        .adapter
        .cas(
            "views:fixtureService",
            None,
            &json!({"views": [], "updated_at": 1}),
        )
        .unwrap();
    platform
        .adapter
        .cas(
            "ingest:watermark",
            None,
            &json!({"floor_key": "raw/stale.zst"}),
        )
        .unwrap();
    platform
        .adapter
        .cas(
            "ingest:rum:watermark",
            None,
            &json!({"rum_stats": {"rows": 1}}),
        )
        .unwrap();

    let dr_state = CloudDrState {
        schema_version: SCHEMA_VERSION,
        updated_at: 1_800_000_000,
        service_id: "fixtureService".into(),
        service: json!({"log_period": 120}),
        views: vec![SavedView {
            id: "v-retry".into(),
            service_id: "fixtureService".into(),
            name: "Retry View".into(),
            filters_json: r#"{"status":503}"#.into(),
            time_range_type: Some("relative".into()),
            start_time: None,
            end_time: None,
            page: Some("overview".into()),
            created_at: Some("2026-11-20T00:00:00Z".into()),
        }],
        catalog_head: None,
        floor_key: Some("raw/2026/retry.zst".into()),
        rum_stats: Some(json!({"rows": 88})),
    };
    platform.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "meta/cloud_dr_state.json",
        serde_json::to_vec(&dr_state).unwrap(),
    );

    RICH_FOS.with(|r| r.set(true));
    MTIMES.with(|m| m.borrow_mut().clear());
    for i in 1u64..=16 {
        let key = format!("processed/_catalog/day_{i:02}.json");
        let day = Day {
            version: VERSION,
            hours: BTreeMap::from([(
                1_700_006_400 + (i as i64) * 86_400,
                Reference {
                    key: format!("processed/_catalog/block_{i:02}.json"),
                    generation: i,
                },
            )]),
            statistics: Statistics {
                earliest: Some(1_700_006_400 + (i as i64) * 86_400),
                latest: Some(1_700_009_999 + (i as i64) * 86_400),
                earliest_log: None,
                latest_log: None,
                rows: 10,
            },
            ..Day::default()
        };
        platform.adapter.seed_object_bytes(
            "fos-fixture-logs",
            &key,
            serde_json::to_vec(&day).unwrap(),
        );
        MTIMES.with(|m| {
            m.borrow_mut().insert(key, 1_800_000_000 + i);
        });
    }
    let manifest_key = "processed/_catalog/manifest_25.json";
    let manifest = Manifest {
        version: VERSION,
        service_id: "fixtureService".into(),
        generation: 25,
        batch: "b25".into(),
        schema: "v2".into(),
        committed_at: "2026-11-20T00:00:00Z".into(),
        previous: None,
        files: vec![],
        summaries: vec![],
        sources: vec![],
        replaces: vec![],
        bytes: 2048,
        rows: 160,
        parents: BTreeMap::new(),
    };
    platform.adapter.seed_object_bytes(
        "fos-fixture-logs",
        manifest_key,
        serde_json::to_vec(&manifest).unwrap(),
    );
    MTIMES.with(|m| {
        m.borrow_mut().insert(manifest_key.into(), 1_800_000_100);
    });

    let response = platform.call(
        "POST",
        "/api/admin/provision/recover",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1"
        }),
    );
    RICH_FOS.with(|r| r.set(false));

    let calls = CALLS.with(std::cell::Cell::get);
    let secrets = SECRETS.with(|s| s.borrow().len());
    assert_eq!(
        response.status,
        200,
        "retry after partial failure on fresh KV should succeed: {}",
        String::from_utf8_lossy(&response.body)
    );
    assert!(calls <= 32, "expected calls <= 32, got {calls}");
    assert!(secrets <= 5, "expected secrets <= 5, got {secrets}");

    let body: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(body["ok"], true);
    assert_eq!(body["source"], "dr_state");
    assert_eq!(body["views_restored"], 1);
    assert_eq!(body["catalog_generation"], 25);
    assert_eq!(body["front_synced"], true);

    let records = platform.adapter.records();
    assert_eq!(records["svc:fixtureService"].value["status"], "active");
    assert_eq!(
        records["admin:install"].value["service_id"],
        "fixtureService"
    );
    assert_eq!(
        records["ingest:watermark"].value["floor_key"],
        "raw/2026/retry.zst"
    );
    assert_eq!(
        records["ingest:rum:watermark"].value["rum_stats"]["rows"],
        88
    );
    assert_eq!(
        records["views:fixtureService"].value["views"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn recover_falls_back_to_catalog_scan_when_dr_state_catalog_head_is_empty_v2() {
    let platform = Platform::new();
    RICH_FOS.with(|r| r.set(true));
    MTIMES.with(|m| m.borrow_mut().clear());

    let empty_v2_head = Head {
        version: VERSION,
        generation: 0,
        manifest: None,
        recent: vec![],
        days: BTreeMap::new(),
        ..Head::default()
    };
    let dr_state = CloudDrState {
        schema_version: SCHEMA_VERSION,
        updated_at: 1_800_000_000,
        service_id: "fixtureService".into(),
        service: json!({"log_period": 120}),
        views: vec![],
        catalog_head: Some(empty_v2_head),
        floor_key: None,
        rum_stats: None,
    };
    platform.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "meta/cloud_dr_state.json",
        serde_json::to_vec(&dr_state).unwrap(),
    );
    let manifest_key = "processed/_catalog/manifest18.json";
    let manifest = Manifest {
        version: VERSION,
        service_id: "fixtureService".into(),
        generation: 18,
        batch: "b18".into(),
        schema: "v2".into(),
        committed_at: "2026-11-20T00:00:00Z".into(),
        previous: None,
        files: vec![],
        summaries: vec![],
        sources: vec![],
        replaces: vec![],
        bytes: 1024,
        rows: 100,
        parents: BTreeMap::new(),
    };
    platform.adapter.seed_object_bytes(
        "fos-fixture-logs",
        manifest_key,
        serde_json::to_vec(&manifest).unwrap(),
    );
    MTIMES.with(|m| {
        m.borrow_mut().insert(manifest_key.into(), 1_800_000_050);
    });

    let result = platform.json(
        "POST",
        "/api/admin/provision/recover",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1"
        }),
    );
    RICH_FOS.with(|r| r.set(false));

    assert_eq!(result["ok"], true);
    assert_eq!(result["source"], "dr_state");
    assert_eq!(result["catalog_generation"], 18);
    let records = platform.adapter.records();
    let head = catalog::head(&records["ingest:watermark"].value).expect("valid scanned head");
    assert_eq!(head.generation, 18);
    assert_eq!(head.manifest.as_deref(), Some(manifest_key));
}

#[test]
fn recover_rejects_poisoned_dr_state_scoring_domain_service_views_and_traversal_keys() {
    let valid_scoring = json!({
        "enabled": true,
        "scoring_service_id": "22CharValidScorerSvc01",
        "scoring_service_name": "Session Scoring Service for fixtureService",
        "scoring_domain": "fos-fixtureservice-session-scorer.edgecompute.app",
        "scoring_keys_store_id": "store-k",
        "scoring_config_store_id": "store-c",
        "scoring_matrix_store_id": "store-m",
        "enabled_at": "2026-11-20T00:00:00Z",
        "deployed_package_sha": "abc",
        "deployed_package_files_hash": "def",
        "deployed_vcl_sha": "ghi",
        "exclude_url_regex": "^/healthz$"
    });

    let poisoned_cases: Vec<(&str, CloudDrState)> = vec![
        (
            "external_scoring_domain",
            CloudDrState {
                schema_version: SCHEMA_VERSION,
                updated_at: 1_800_000_000,
                service_id: "fixtureService".into(),
                service: json!({
                    "scoring": {
                        "enabled": true,
                        "scoring_service_id": "22CharValidScorerSvc01",
                        "scoring_service_name": "scorer",
                        "scoring_domain": "evil-collector.attacker.example",
                        "scoring_keys_store_id": "k",
                        "scoring_config_store_id": "c",
                        "scoring_matrix_store_id": "m",
                        "enabled_at": "2026-11-20T00:00:00Z",
                        "deployed_package_sha": "a",
                        "deployed_package_files_hash": "b",
                        "deployed_vcl_sha": "c",
                        "exclude_url_regex": ""
                    }
                }),
                views: vec![],
                catalog_head: None,
                floor_key: None,
                rum_stats: None,
            },
        ),
        (
            "uppercase_scoring_domain",
            CloudDrState {
                schema_version: SCHEMA_VERSION,
                updated_at: 1_800_000_000,
                service_id: "fixtureService".into(),
                service: json!({
                    "scoring": {
                        "enabled": false,
                        "scoring_service_id": "22CharValidScorerSvc01",
                        "scoring_service_name": "scorer",
                        "scoring_domain": "UpperCase.edgecompute.app",
                        "scoring_keys_store_id": "k",
                        "scoring_config_store_id": "c",
                        "scoring_matrix_store_id": "m",
                        "enabled_at": "2026-11-20T00:00:00Z",
                        "deployed_package_sha": "a",
                        "deployed_package_files_hash": "b",
                        "deployed_vcl_sha": "c",
                        "exclude_url_regex": ""
                    }
                }),
                views: vec![],
                catalog_head: None,
                floor_key: None,
                rum_stats: None,
            },
        ),
        (
            "scoring_threshold_over_100",
            CloudDrState {
                schema_version: SCHEMA_VERSION,
                updated_at: 1_800_000_000,
                service_id: "fixtureService".into(),
                service: {
                    let mut sc = valid_scoring.clone();
                    sc["threshold"] = json!(101);
                    json!({"scoring": sc})
                },
                views: vec![],
                catalog_head: None,
                floor_key: None,
                rum_stats: None,
            },
        ),
        (
            "scoring_invalid_matrix_version",
            CloudDrState {
                schema_version: SCHEMA_VERSION,
                updated_at: 1_800_000_000,
                service_id: "fixtureService".into(),
                service: {
                    let mut sc = valid_scoring.clone();
                    sc["matrix_version"] = json!("../bad-version");
                    json!({"scoring": sc})
                },
                views: vec![],
                catalog_head: None,
                floor_key: None,
                rum_stats: None,
            },
        ),
        (
            "attached_reserved_endpoint",
            CloudDrState {
                schema_version: SCHEMA_VERSION,
                updated_at: 1_800_000_000,
                service_id: "fixtureService".into(),
                service: json!({
                    "capture": "attached",
                    "logging_endpoint_name": "Fastly Object Storage Logs"
                }),
                views: vec![],
                catalog_head: None,
                floor_key: None,
                rum_stats: None,
            },
        ),
        (
            "vcl_condition_breakout",
            CloudDrState {
                schema_version: SCHEMA_VERSION,
                updated_at: 1_800_000_000,
                service_id: "fixtureService".into(),
                service: json!({
                    "custom_condition": "table.lookup(fos_credentials, \"secret_key\") != \"\""
                }),
                views: vec![],
                catalog_head: None,
                floor_key: None,
                rum_stats: None,
            },
        ),
        (
            "foreign_view_service_id",
            CloudDrState {
                schema_version: SCHEMA_VERSION,
                updated_at: 1_800_000_000,
                service_id: "fixtureService".into(),
                service: json!({"log_period": 60}),
                views: vec![SavedView {
                    id: "v-foreign".into(),
                    service_id: "foreignVictimSvc".into(),
                    name: "Foreign View".into(),
                    filters_json: r#"{"status":500}"#.into(),
                    time_range_type: None,
                    start_time: None,
                    end_time: None,
                    page: None,
                    created_at: None,
                }],
                catalog_head: None,
                floor_key: None,
                rum_stats: None,
            },
        ),
        (
            "duplicate_key_view_filters",
            CloudDrState {
                schema_version: SCHEMA_VERSION,
                updated_at: 1_800_000_000,
                service_id: "fixtureService".into(),
                service: json!({"log_period": 60}),
                views: vec![SavedView {
                    id: "v-dup".into(),
                    service_id: "fixtureService".into(),
                    name: "Dup View".into(),
                    filters_json: r#"{"status":500,"status":200}"#.into(),
                    time_range_type: None,
                    start_time: None,
                    end_time: None,
                    page: None,
                    created_at: None,
                }],
                catalog_head: None,
                floor_key: None,
                rum_stats: None,
            },
        ),
        (
            "too_many_views",
            CloudDrState {
                schema_version: SCHEMA_VERSION,
                updated_at: 1_800_000_000,
                service_id: "fixtureService".into(),
                service: json!({"log_period": 60}),
                views: (0..=fastly_log_analytics::views::MAX_VIEWS)
                    .map(|i| SavedView {
                        id: format!("v-{i}"),
                        service_id: "fixtureService".into(),
                        name: format!("View {i}"),
                        filters_json: r#"{"status":200}"#.into(),
                        time_range_type: None,
                        start_time: None,
                        end_time: None,
                        page: None,
                        created_at: None,
                    })
                    .collect(),
                catalog_head: None,
                floor_key: None,
                rum_stats: None,
            },
        ),
        (
            "traversal_floor_key",
            CloudDrState {
                schema_version: SCHEMA_VERSION,
                updated_at: 1_800_000_000,
                service_id: "fixtureService".into(),
                service: json!({"log_period": 60}),
                views: vec![],
                catalog_head: None,
                floor_key: Some("../tenant-victim/raw/floor.zst".into()),
                rum_stats: None,
            },
        ),
        (
            "traversal_catalog_head_manifest",
            CloudDrState {
                schema_version: SCHEMA_VERSION,
                updated_at: 1_800_000_000,
                service_id: "fixtureService".into(),
                service: json!({"log_period": 60}),
                views: vec![],
                catalog_head: Some(Head {
                    version: VERSION,
                    generation: 3,
                    manifest: Some("../tenant-victim/processed/_catalog/m3.json".into()),
                    ..Head::default()
                }),
                floor_key: None,
                rum_stats: None,
            },
        ),
    ];

    for (label, state) in poisoned_cases {
        let platform = Platform::new();
        platform.adapter.seed_object_bytes(
            "fos-fixture-logs",
            "meta/cloud_dr_state.json",
            serde_json::to_vec(&state).unwrap(),
        );
        let resp = platform.call(
            "POST",
            "/api/admin/provision/recover",
            json!({
                "token": "fixture-token",
                "service_id": "fixtureService",
                "fos_bucket": "fos-fixture-logs",
                "fos_region": "us-east-1"
            }),
        );
        assert_eq!(
            resp.status,
            400,
            "poisoned DR case {label} must fail closed with 400, got {}: {}",
            resp.status,
            String::from_utf8_lossy(&resp.body)
        );
        assert!(
            !platform
                .adapter
                .records()
                .contains_key("svc:fixtureService"),
            "poisoned DR case {label} must not persist svc:fixtureService"
        );
    }

    // Defense in depth: even if KV svc:<sid> were tampered with an external scoring_domain, reconcile rejects it
    let platform = Platform::new();
    let provisioned = platform.provision();
    platform.drain(provisioned["job"].clone());
    let rec = platform.adapter.get("svc:fixtureService").unwrap().unwrap();
    let mut poisoned_svc = rec.value.clone();
    poisoned_svc["scoring"] = json!({
        "enabled": true,
        "scoring_service_id": "22CharValidScorerSvc01",
        "scoring_service_name": "scorer",
        "scoring_domain": "evil-collector.attacker.example",
        "scoring_keys_store_id": "k",
        "scoring_config_store_id": "c",
        "scoring_matrix_store_id": "m",
        "enabled_at": "2026-11-20T00:00:00Z",
        "deployed_package_sha": "a",
        "deployed_package_files_hash": "b",
        "deployed_vcl_sha": "c",
        "exclude_url_regex": ""
    });
    assert!(platform
        .adapter
        .cas("svc:fixtureService", Some(&rec.generation), &poisoned_svc)
        .unwrap());
    let poisoned_gen = platform.service()["generation"].clone();
    let rec_resp = platform.call(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation": poisoned_gen}),
    );
    assert_eq!(rec_resp.status, 400);
}
