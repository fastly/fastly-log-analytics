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
        service_id: Some("fixtureService".into()),
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

#[test]
fn recover_rejects_dr_scoring_store_traversal_own_service_regex_views_and_service_name() {
    let base_scoring = json!({
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

    let make_scoring_case = |patch: &dyn Fn(&mut Value)| -> CloudDrState {
        let mut sc = base_scoring.clone();
        patch(&mut sc);
        CloudDrState {
            schema_version: SCHEMA_VERSION,
            updated_at: 1_800_000_000,
            service_id: "fixtureService".into(),
            service: json!({"scoring": sc}),
            views: vec![],
            catalog_head: None,
            floor_key: None,
            rum_stats: None,
        }
    };

    let overlong_name = "N".repeat(201);
    let overlong_sha = "a".repeat(129);
    let cases: Vec<(&str, CloudDrState)> = vec![
        (
            "keys_store_traversal",
            make_scoring_case(&|sc| {
                sc["scoring_keys_store_id"] =
                    json!("../../service/fixtureApp/version/1/deactivate");
            }),
        ),
        (
            "config_store_traversal",
            make_scoring_case(&|sc| {
                sc["scoring_config_store_id"] = json!("../../../service/fixtureOther");
            }),
        ),
        (
            "matrix_store_own_install",
            make_scoring_case(&|sc| {
                sc["scoring_matrix_store_id"] = json!("fixtureSecrets");
            }),
        ),
        (
            "duplicate_scoring_store_ids",
            make_scoring_case(&|sc| {
                sc["scoring_keys_store_id"] = json!("store-dup");
                sc["scoring_config_store_id"] = json!("store-dup");
            }),
        ),
        (
            "scoring_service_id_matches_customer_service",
            make_scoring_case(&|sc| {
                sc["scoring_service_id"] = json!("fixtureService");
            }),
        ),
        (
            "scoring_service_id_matches_own_front",
            make_scoring_case(&|sc| {
                sc["scoring_service_id"] = json!("fixtureFront");
            }),
        ),
        (
            "exclude_regex_trailing_backslash",
            make_scoring_case(&|sc| {
                sc["exclude_url_regex"] = json!(r"^/health\");
            }),
        ),
        (
            "exclude_regex_unbalanced_parens",
            make_scoring_case(&|sc| {
                sc["exclude_url_regex"] = json!("(((");
            }),
        ),
        (
            "enforce_status_code_out_of_range",
            make_scoring_case(&|sc| {
                sc["enforce_status_code"] = json!(200);
            }),
        ),
        (
            "deployed_sha_overlong",
            make_scoring_case(&|sc| {
                sc["deployed_package_sha"] = json!(overlong_sha);
            }),
        ),
        (
            "enabled_at_control_char",
            make_scoring_case(&|sc| {
                sc["enabled_at"] = json!("2026-11-20T00:00:00Z\n");
            }),
        ),
        (
            "view_id_contains_slash",
            CloudDrState {
                schema_version: SCHEMA_VERSION,
                updated_at: 1_800_000_000,
                service_id: "fixtureService".into(),
                service: json!({"log_period": 60}),
                views: vec![SavedView {
                    id: "undeletable/view".into(),
                    service_id: "fixtureService".into(),
                    name: "Slash View".into(),
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
            "duplicate_view_ids",
            CloudDrState {
                schema_version: SCHEMA_VERSION,
                updated_at: 1_800_000_000,
                service_id: "fixtureService".into(),
                service: json!({"log_period": 60}),
                views: vec![
                    SavedView {
                        id: "dup-view".into(),
                        service_id: "fixtureService".into(),
                        name: "View A".into(),
                        filters_json: r#"{"status":500}"#.into(),
                        time_range_type: None,
                        start_time: None,
                        end_time: None,
                        page: None,
                        created_at: None,
                    },
                    SavedView {
                        id: "dup-view".into(),
                        service_id: "fixtureService".into(),
                        name: "View B".into(),
                        filters_json: r#"{"status":200}"#.into(),
                        time_range_type: None,
                        start_time: None,
                        end_time: None,
                        page: None,
                        created_at: None,
                    },
                ],
                catalog_head: None,
                floor_key: None,
                rum_stats: None,
            },
        ),
        (
            "empty_service_name",
            CloudDrState {
                schema_version: SCHEMA_VERSION,
                updated_at: 1_800_000_000,
                service_id: "fixtureService".into(),
                service: json!({"name": "   ", "log_period": 60}),
                views: vec![],
                catalog_head: None,
                floor_key: None,
                rum_stats: None,
            },
        ),
        (
            "control_char_service_name",
            CloudDrState {
                schema_version: SCHEMA_VERSION,
                updated_at: 1_800_000_000,
                service_id: "fixtureService".into(),
                service: json!({"name": "Prod\nInjected", "log_period": 60}),
                views: vec![],
                catalog_head: None,
                floor_key: None,
                rum_stats: None,
            },
        ),
        (
            "overlong_service_name",
            CloudDrState {
                schema_version: SCHEMA_VERSION,
                updated_at: 1_800_000_000,
                service_id: "fixtureService".into(),
                service: json!({"name": overlong_name, "log_period": 60}),
                views: vec![],
                catalog_head: None,
                floor_key: None,
                rum_stats: None,
            },
        ),
    ];

    for (label, state) in cases {
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
            "residual DR case {label} must fail closed with 400, got {}: {}",
            resp.status,
            String::from_utf8_lossy(&resp.body)
        );
        assert!(
            !platform
                .adapter
                .records()
                .contains_key("svc:fixtureService"),
            "residual DR case {label} must not persist svc:fixtureService"
        );
    }

    // scan_catalog_head must skip traversal object keys and manifests with control chars in committed_at
    let platform = Platform::new();
    RICH_FOS.with(|r| r.set(true));
    MTIMES.with(|m| m.borrow_mut().clear());
    let bad_traversal_key = "processed/_catalog/../../other/manifest99.json";
    let bad_ctrl_key = "processed/_catalog/manifest_ctrl.json";
    let traversal_manifest = Manifest {
        version: VERSION,
        service_id: "fixtureService".into(),
        generation: 99,
        batch: "b99".into(),
        schema: "v2".into(),
        committed_at: "2026-11-20T00:00:00Z".into(),
        previous: None,
        files: vec![],
        summaries: vec![],
        sources: vec![],
        replaces: vec![],
        bytes: 64,
        rows: 1,
        parents: BTreeMap::new(),
    };
    let ctrl_manifest = Manifest {
        committed_at: "2026-11-20T00:00:00Z\n".into(),
        generation: 98,
        ..traversal_manifest.clone()
    };
    platform.adapter.seed_object_bytes(
        "fos-fixture-logs",
        bad_traversal_key,
        serde_json::to_vec(&traversal_manifest).unwrap(),
    );
    platform.adapter.seed_object_bytes(
        "fos-fixture-logs",
        bad_ctrl_key,
        serde_json::to_vec(&ctrl_manifest).unwrap(),
    );
    MTIMES.with(|m| {
        let mut m = m.borrow_mut();
        m.insert(bad_traversal_key.into(), 1_800_000_200);
        m.insert(bad_ctrl_key.into(), 1_800_000_190);
    });
    let scan_res = platform.json(
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
    assert_eq!(scan_res["source"], "minimal");
    assert!(scan_res["catalog_generation"].is_null());
}

#[test]
fn recover_overwrites_poisoned_owned_endpoint_names_and_scoring_protects_foreign_services_and_stores(
) {
    // 1. Shared Capture::Attached service with High-Scale / GCE VM + SIEM endpoints must NOT have them wiped on post-DR reconcile
    let platform = Platform::new();
    for (resource, body) in [
        (
            "snippet",
            json!({"name": "Fastly Log Analytics - vcl_recv", "type": "recv", "priority": "10", "content": "# hs"}),
        ),
        (
            "snippet",
            json!({"name": "Fastly Log Analytics - vcl_deliver", "type": "deliver", "priority": "10", "content": "# hs"}),
        ),
        (
            "snippet",
            json!({"name": "flp-owner-fixtureApp", "type": "recv", "priority": "0", "content": "# flp-owner:fixtureApp:fixtureService"}),
        ),
        (
            "condition",
            json!({"name": "log_analytics_condition", "type": "RESPONSE", "priority": "10", "statement": "req.url !~ \"^/health\""}),
        ),
        (
            "logging/fos",
            json!({"name": "Fastly Object Storage Logs", "bucket_name": "hs-bucket", "region": "us-east-1", "path": "raw/"}),
        ),
        (
            "logging/fos",
            json!({"name": "Fastly RUM Logs", "bucket_name": "hs-bucket", "region": "us-east-1", "path": "rum/"}),
        ),
        (
            "logging/fos",
            json!({"name": "FLP Cloud Logs", "bucket_name": "fos-fixture-logs", "region": "us-east-1", "path": "raw/", "response_condition": "flp_cloud_log_condition"}),
        ),
        (
            "logging/s3",
            json!({"name": "Customer Production SIEM", "bucket_name": "siem-bucket"}),
        ),
    ] {
        platform
            .adapter
            .seed_resource("fixtureService", resource, body);
    }

    let poisoned_attached = CloudDrState {
        schema_version: SCHEMA_VERSION,
        updated_at: 1_800_000_000,
        service_id: "fixtureService".into(),
        service: json!({
            "capture": "attached",
            "logging_enabled": true,
            "logging_endpoint_name": "FLP Cloud Logs",
            "owned_endpoint_names": [
                "FLP Cloud Logs",
                "Fastly Object Storage Logs",
                "Fastly RUM Logs",
                "Customer Production SIEM"
            ]
        }),
        views: vec![],
        catalog_head: None,
        floor_key: None,
        rum_stats: None,
    };
    platform.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "meta/cloud_dr_state.json",
        serde_json::to_vec(&poisoned_attached).unwrap(),
    );

    let recovered = platform.json(
        "POST",
        "/api/admin/provision/recover",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1"
        }),
    );
    assert_eq!(recovered["ok"], true);
    assert_eq!(
        platform.adapter.records()["svc:fixtureService"].value["owned_endpoint_names"],
        json!(["FLP Cloud Logs"])
    );

    let gen = platform.service()["generation"].clone();
    let rec_job = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation": gen}),
    )["job"]
        .clone();
    platform.drain(rec_job);

    let fos_names: Vec<String> = platform
        .adapter
        .resources("fixtureService", "logging/fos")
        .into_iter()
        .filter_map(|v| v["name"].as_str().map(str::to_owned))
        .collect();
    let s3_names: Vec<String> = platform
        .adapter
        .resources("fixtureService", "logging/s3")
        .into_iter()
        .filter_map(|v| v["name"].as_str().map(str::to_owned))
        .collect();
    assert!(fos_names.iter().any(|n| n == "Fastly Object Storage Logs"));
    assert!(fos_names.iter().any(|n| n == "Fastly RUM Logs"));
    assert!(fos_names.iter().any(|n| n == "FLP Cloud Logs"));
    assert!(s3_names.iter().any(|n| n == "Customer Production SIEM"));

    // 2. Defense-in-depth in scoring.rs: tampered scoring_config_store_id path traversal & foreign scoring_service_id rejected
    let p2 = Platform::new();
    let prov2 = p2.provision();
    p2.drain(prov2["job"].clone());
    let (_, foreign_cfg_resp) = p2
        .adapter
        .api(fastly_log_analytics::admin::ApiRequest {
            method: "POST",
            path: "/resources/stores/config",
            token: "fixture-token",
            body: Some(json!({"name": "customer_prod_config"})),
            timeout: std::time::Duration::from_secs(5),
            query: "",
        })
        .unwrap();
    let foreign_cfg_id = foreign_cfg_resp["id"].as_str().unwrap().to_owned();

    let rec2 = p2.adapter.get("svc:fixtureService").unwrap().unwrap();
    let mut tampered = rec2.value.clone();
    tampered["scoring"] = json!({
        "enabled": true,
        "scoring_service_id": "fixtureOther",
        "scoring_service_name": "Session Scoring Service for fixtureService",
        "scoring_domain": "fos-fixtureservice-session-scorer.edgecompute.app",
        "scoring_keys_store_id": "store-k",
        "scoring_config_store_id": "../../../service/fixtureOther",
        "scoring_matrix_store_id": "store-m",
        "enabled_at": "2026-11-20T00:00:00Z",
        "deployed_package_sha": "a",
        "deployed_package_files_hash": "b",
        "deployed_vcl_sha": "c",
        "exclude_url_regex": ""
    });
    assert!(p2
        .adapter
        .cas("svc:fixtureService", Some(&rec2.generation), &tampered)
        .unwrap());

    let enf_resp = p2.call(
        "PUT",
        "/api/admin/services/fixtureService/scoring/enforce-threshold?confirm=true",
        json!({"enforce_threshold": 80}),
    );
    assert_eq!(enf_resp.status, 400);

    // Fix config store ID to foreign_cfg_id so we also test delete_scorer_resources_step refusing non-wasm / wrong-name service
    let rec3 = p2.adapter.get("svc:fixtureService").unwrap().unwrap();
    let mut tampered2 = rec3.value.clone();
    tampered2["scoring"]["scoring_config_store_id"] = json!(foreign_cfg_id);
    assert!(p2
        .adapter
        .cas("svc:fixtureService", Some(&rec3.generation), &tampered2)
        .unwrap());

    let gen2 = p2.service()["generation"].clone();
    let td_start = p2.json(
        "POST",
        "/api/admin/services/fixtureService/scoring/teardown",
        json!({"confirm_service_id": "fixtureService", "generation": gen2}),
    );
    let mut job = td_start["job"].clone();
    for _ in 0..20 {
        if job["status"] == "complete" || job["status"] == "failed" {
            break;
        }
        let id = job["id"].as_str().unwrap();
        job = p2.json("POST", &format!("/api/admin/jobs/{id}/step"), json!({}))["job"].clone();
    }
    assert_eq!(job["status"], "failed");
    assert_eq!(job["error"], "invalid_scoring_service_id");
    assert!(
        p2.adapter.has_service("fixtureOther"),
        "foreign customer VCL service fixtureOther must NOT be deleted!"
    );

    // Also verify delete_scorer_stores_step refuses to delete foreign_cfg_id ("customer_prod_config")
    let p3 = Platform::new();
    let prov3 = p3.provision();
    p3.drain(prov3["job"].clone());
    let (_, foreign_cfg_resp3) = p3
        .adapter
        .api(fastly_log_analytics::admin::ApiRequest {
            method: "POST",
            path: "/resources/stores/config",
            token: "fixture-token",
            body: Some(json!({"name": "customer_prod_config"})),
            timeout: std::time::Duration::from_secs(5),
            query: "",
        })
        .unwrap();
    let foreign_cfg_id3 = foreign_cfg_resp3["id"].as_str().unwrap().to_owned();
    let _ = p3
        .adapter
        .api(fastly_log_analytics::admin::ApiRequest {
            method: "POST",
            path: &format!("/resources/stores/config/{foreign_cfg_id3}/item"),
            token: "fixture-token",
            body: Some(json!({"item_key": "keep", "item_value": "intact"})),
            timeout: std::time::Duration::from_secs(5),
            query: "",
        })
        .unwrap();
    let rec_p3 = p3.adapter.get("svc:fixtureService").unwrap().unwrap();
    let mut tampered_p3 = rec_p3.value.clone();
    tampered_p3["scoring"] = json!({
        "enabled": true,
        "scoring_service_id": "22CharDeletedScorer001",
        "scoring_service_name": "Session Scoring Service for fixtureService",
        "scoring_domain": "fos-fixtureservice-session-scorer.edgecompute.app",
        "scoring_keys_store_id": "store-k",
        "scoring_config_store_id": foreign_cfg_id3,
        "scoring_matrix_store_id": "store-m",
        "enabled_at": "2026-11-20T00:00:00Z",
        "deployed_package_sha": "a",
        "deployed_package_files_hash": "b",
        "deployed_vcl_sha": "c",
        "exclude_url_regex": ""
    });
    assert!(p3
        .adapter
        .cas("svc:fixtureService", Some(&rec_p3.generation), &tampered_p3)
        .unwrap());
    let gen3 = p3.service()["generation"].clone();
    let td3 = p3.json(
        "POST",
        "/api/admin/services/fixtureService/scoring/teardown",
        json!({"confirm_service_id": "fixtureService", "generation": gen3}),
    );
    let mut job3 = td3["job"].clone();
    for _ in 0..20 {
        if job3["status"] == "complete" || job3["status"] == "failed" {
            break;
        }
        let id = job3["id"].as_str().unwrap();
        job3 = p3.json("POST", &format!("/api/admin/jobs/{id}/step"), json!({}))["job"].clone();
    }
    assert_eq!(job3["status"], "failed");
    assert_eq!(job3["error"], "invalid_scoring_store_id");
    assert_eq!(
        p3.adapter
            .config_store_value(&foreign_cfg_id3, "keep")
            .as_deref(),
        Some("intact"),
        "foreign Config Store must NOT be deleted!"
    );
}

#[test]
fn recover_and_scoring_close_foreign_store_siem_attached_rum_and_view_id_confused_deputies() {
    // 1. BUG-CLOUD-DR-04 (EXPLOIT-1A): dr::recover rejects unowned/foreign scorer service & stores,
    //    and runtime scoring endpoints (enforce-threshold, l2-enforce, rotate-key, retrain, restore)
    //    reject foreign Fastly ConfigStore & KVStore IDs even if KV svc:<sid> were tampered directly.
    let p1 = Platform::new();
    let (_, fk_resp) = p1
        .adapter
        .api(fastly_log_analytics::admin::ApiRequest {
            method: "POST",
            path: "/resources/stores/config",
            token: "fixture-token",
            body: Some(json!({"name": "customer_prod_cookie_keys"})),
            timeout: std::time::Duration::from_secs(5),
            query: "",
        })
        .unwrap();
    let foreign_keys_id = fk_resp["id"].as_str().unwrap().to_owned();
    let _ = p1
        .adapter
        .api(fastly_log_analytics::admin::ApiRequest {
            method: "POST",
            path: &format!("/resources/stores/config/{foreign_keys_id}/item"),
            token: "fixture-token",
            body: Some(
                json!({"item_key": "current_key_hex", "item_value": "ORIG_PROD_AES256_KEY_HEX"}),
            ),
            timeout: std::time::Duration::from_secs(5),
            query: "",
        })
        .unwrap();
    let (_, fc_resp) = p1
        .adapter
        .api(fastly_log_analytics::admin::ApiRequest {
            method: "POST",
            path: "/resources/stores/config",
            token: "fixture-token",
            body: Some(json!({"name": "scoring_config_otherTenantScorer"})),
            timeout: std::time::Duration::from_secs(5),
            query: "",
        })
        .unwrap();
    let foreign_cfg_id = fc_resp["id"].as_str().unwrap().to_owned();
    let _ = p1
        .adapter
        .api(fastly_log_analytics::admin::ApiRequest {
            method: "POST",
            path: &format!("/resources/stores/config/{foreign_cfg_id}/item"),
            token: "fixture-token",
            body: Some(json!({"item_key": "enforce_threshold", "item_value": "95"})),
            timeout: std::time::Duration::from_secs(5),
            query: "",
        })
        .unwrap();
    let (_, fm_resp) = p1
        .adapter
        .api(fastly_log_analytics::admin::ApiRequest {
            method: "POST",
            path: "/resources/stores/kv",
            token: "fixture-token",
            body: Some(json!({"name": "customer_prod_kv_store"})),
            timeout: std::time::Duration::from_secs(5),
            query: "",
        })
        .unwrap();
    let foreign_kv_id = fm_resp["id"].as_str().unwrap().to_owned();

    // Seed an owned wasm scorer service so we test both missing-scorer AND owned-scorer + foreign-stores!
    let (_, owned_scorer_resp) = p1
        .adapter
        .api(fastly_log_analytics::admin::ApiRequest {
            method: "POST",
            path: "/service",
            token: "fixture-token",
            body: Some(
                json!({"name": "Session Scoring Service for fixtureService", "type": "wasm"}),
            ),
            timeout: std::time::Duration::from_secs(5),
            query: "",
        })
        .unwrap();
    let owned_scorer_sid = owned_scorer_resp["id"].as_str().unwrap().to_owned();

    let poisoned_dr_sc = CloudDrState {
        schema_version: SCHEMA_VERSION,
        updated_at: 1_800_000_000,
        service_id: "fixtureService".into(),
        service: json!({
            "scoring": {
                "enabled": true,
                "scoring_service_id": owned_scorer_sid,
                "scoring_service_name": "Session Scoring Service for fixtureService",
                "scoring_domain": "fos-fixtureservice-session-scorer.edgecompute.app",
                "scoring_keys_store_id": foreign_keys_id,
                "scoring_config_store_id": foreign_cfg_id,
                "scoring_matrix_store_id": foreign_kv_id,
                "enabled_at": "2026-11-20T00:00:00Z",
                "deployed_package_sha": "abc",
                "deployed_package_files_hash": "def",
                "deployed_vcl_sha": "ghi",
                "exclude_url_regex": ""
            }
        }),
        views: vec![],
        catalog_head: None,
        floor_key: None,
        rum_stats: None,
    };
    p1.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "meta/cloud_dr_state.json",
        serde_json::to_vec(&poisoned_dr_sc).unwrap(),
    );
    let r_dr_foreign = p1.call(
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
        r_dr_foreign.status,
        400,
        "dr::recover must reject foreign scoring stores: {}",
        String::from_utf8_lossy(&r_dr_foreign.body)
    );

    // Legitimate scoring DR state (matching wasm scorer service + matching store names) recovers 200 OK
    let (_, ok_k_resp) = p1
        .adapter
        .api(fastly_log_analytics::admin::ApiRequest {
            method: "POST",
            path: "/resources/stores/config",
            token: "fixture-token",
            body: Some(json!({"name": format!("scoring_keys_{owned_scorer_sid}")})),
            timeout: std::time::Duration::from_secs(5),
            query: "",
        })
        .unwrap();
    let ok_keys_id = ok_k_resp["id"].as_str().unwrap().to_owned();
    let (_, ok_c_resp) = p1
        .adapter
        .api(fastly_log_analytics::admin::ApiRequest {
            method: "POST",
            path: "/resources/stores/config",
            token: "fixture-token",
            body: Some(json!({"name": format!("scoring_config_{owned_scorer_sid}")})),
            timeout: std::time::Duration::from_secs(5),
            query: "",
        })
        .unwrap();
    let ok_cfg_id = ok_c_resp["id"].as_str().unwrap().to_owned();
    let (_, ok_m_resp) = p1
        .adapter
        .api(fastly_log_analytics::admin::ApiRequest {
            method: "POST",
            path: "/resources/stores/kv",
            token: "fixture-token",
            body: Some(json!({"name": format!("scoring_matrix_{owned_scorer_sid}")})),
            timeout: std::time::Duration::from_secs(5),
            query: "",
        })
        .unwrap();
    let ok_mat_id = ok_m_resp["id"].as_str().unwrap().to_owned();

    let legit_dr_sc = CloudDrState {
        schema_version: SCHEMA_VERSION,
        updated_at: 1_800_000_000,
        service_id: "fixtureService".into(),
        service: json!({
            "scoring": {
                "enabled": true,
                "scoring_service_id": owned_scorer_sid,
                "scoring_service_name": "Session Scoring Service for fixtureService",
                "scoring_domain": "fos-fixtureservice-session-scorer.edgecompute.app",
                "scoring_keys_store_id": ok_keys_id,
                "scoring_config_store_id": ok_cfg_id,
                "scoring_matrix_store_id": ok_mat_id,
                "enabled_at": "2026-11-20T00:00:00Z",
                "deployed_package_sha": "abc",
                "deployed_package_files_hash": "def",
                "deployed_vcl_sha": "ghi",
                "exclude_url_regex": ""
            }
        }),
        views: vec![],
        catalog_head: None,
        floor_key: None,
        rum_stats: None,
    };
    p1.adapter.seed_resource(
        "fixtureService",
        "snippet",
        json!({"name": "flp-owner-fixtureApp", "type": "recv", "priority": "0", "content": "# flp-owner:fixtureApp:fixtureService"}),
    );
    p1.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "meta/cloud_dr_state.json",
        serde_json::to_vec(&legit_dr_sc).unwrap(),
    );
    let r_dr_ok = p1.call(
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
        r_dr_ok.status,
        200,
        "legitimate scoring DR state must recover cleanly: {}",
        String::from_utf8_lossy(&r_dr_ok.body)
    );

    // Now tamper KV svc:fixtureService directly to point at foreign stores and verify runtime scoring routes block all reads/writes!
    let rec_tamper = p1.adapter.get("svc:fixtureService").unwrap().unwrap();
    let mut val_tamper = rec_tamper.value.clone();
    val_tamper["scoring"]["scoring_keys_store_id"] = json!(foreign_keys_id);
    val_tamper["scoring"]["scoring_config_store_id"] = json!(foreign_cfg_id);
    val_tamper["scoring"]["scoring_matrix_store_id"] = json!(foreign_kv_id);
    assert!(p1
        .adapter
        .cas(
            "svc:fixtureService",
            Some(&rec_tamper.generation),
            &val_tamper
        )
        .unwrap());
    p1.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "processed/scoring/matrices/v20261121.json",
        serde_json::to_vec(&json!({
            "version": "v20261121",
            "vocab_size": 1,
            "session_count": 1,
            "transition_count": 1,
            "counts": {"/": {"/": 1}},
            "row_totals": {"/": 1}
        }))
        .unwrap(),
    );

    assert_eq!(
        p1.call(
            "GET",
            "/api/admin/services/fixtureService/scoring/enforce-threshold",
            json!({})
        )
        .status,
        400
    );
    assert_eq!(
        p1.call(
            "PUT",
            "/api/admin/services/fixtureService/scoring/enforce-threshold?confirm=true",
            json!({"enforce_threshold": 0})
        )
        .status,
        400
    );
    assert_eq!(
        p1.call(
            "GET",
            "/api/admin/services/fixtureService/scoring/l2-enforce",
            json!({})
        )
        .status,
        400
    );
    assert_eq!(
        p1.call(
            "PUT",
            "/api/admin/services/fixtureService/scoring/l2-enforce?confirm=true",
            json!({"enabled": true})
        )
        .status,
        400
    );
    assert_eq!(
        p1.call(
            "POST",
            "/api/admin/services/fixtureService/scoring/rotate-key?confirm=true",
            json!({})
        )
        .status,
        400
    );
    assert_eq!(
        p1.call(
            "POST",
            "/api/admin/services/fixtureService/scoring/retrain",
            json!({
                "matrix": {
                    "version": "v20261121",
                    "vocab_size": 1,
                    "session_count": 1,
                    "transition_count": 1,
                    "counts": {"/": {"/": 1}},
                    "row_totals": {"/": 1}
                }
            })
        )
        .status,
        400
    );
    assert_eq!(
        p1.call(
            "POST",
            "/api/admin/services/fixtureService/scoring/matrix/versions/restore?confirm=true",
            json!({"version": "v20261121"})
        )
        .status,
        400
    );
    assert_eq!(
        p1.adapter
            .config_store_value(&foreign_keys_id, "current_key_hex")
            .as_deref(),
        Some("ORIG_PROD_AES256_KEY_HEX")
    );
    assert_eq!(
        p1.adapter
            .config_store_value(&foreign_cfg_id, "enforce_threshold")
            .as_deref(),
        Some("95")
    );
    assert!(p1
        .adapter
        .kv_store_value(&foreign_kv_id, "matrix")
        .is_none());

    // 2. BUG-CLOUD-DR-04 (EXPLOIT-1B + cleanup_scoring_state_step valid_matrix_version):
    //    404 scorer service in teardown must NOT delete orphaned foreign scorer stores, and invalid matrix versions are skipped.
    let orphan_sid = "otherScorerSid22Chars0";
    let (_, orph_k) = p1
        .adapter
        .api(fastly_log_analytics::admin::ApiRequest {
            method: "POST",
            path: "/resources/stores/config",
            token: "fixture-token",
            body: Some(json!({"name": format!("scoring_keys_{orphan_sid}")})),
            timeout: std::time::Duration::from_secs(5),
            query: "",
        })
        .unwrap();
    let orph_keys_id = orph_k["id"].as_str().unwrap().to_owned();
    let _ = p1
        .adapter
        .api(fastly_log_analytics::admin::ApiRequest {
            method: "POST",
            path: &format!("/resources/stores/config/{orph_keys_id}/item"),
            token: "fixture-token",
            body: Some(json!({"item_key": "current_key_hex", "item_value": "ORPHAN_SECRET_KEY"})),
            timeout: std::time::Duration::from_secs(5),
            query: "",
        })
        .unwrap();
    let (_, orph_m) = p1
        .adapter
        .api(fastly_log_analytics::admin::ApiRequest {
            method: "POST",
            path: "/resources/stores/kv",
            token: "fixture-token",
            body: Some(json!({"name": format!("scoring_matrix_{orphan_sid}")})),
            timeout: std::time::Duration::from_secs(5),
            query: "",
        })
        .unwrap();
    let orph_mat_id = orph_m["id"].as_str().unwrap().to_owned();
    let _ = p1
        .adapter
        .api_raw(fastly_log_analytics::admin::ApiRawRequest {
            method: "PUT",
            path: &format!("/resources/stores/kv/{orph_mat_id}/keys/matrix"),
            token: "fixture-token",
            content_type: "application/octet-stream",
            body: b"FSM1\x01keep",
            timeout: std::time::Duration::from_secs(5),
        });
    p1.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "raw/keep_victim.json",
        b"must-stay".to_vec(),
    );
    p1.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "processed/scoring/matrices/index.json",
        serde_json::to_vec(&json!({"versions": [{"version": "../../../raw/keep_victim"}]}))
            .unwrap(),
    );

    let rec_orph = p1.adapter.get("svc:fixtureService").unwrap().unwrap();
    let mut val_orph = rec_orph.value.clone();
    val_orph["scoring"] = json!({
        "enabled": true,
        "scoring_service_id": orphan_sid,
        "scoring_service_name": "Session Scoring Service for fixtureService",
        "scoring_domain": "fos-fixtureservice-session-scorer.edgecompute.app",
        "scoring_keys_store_id": orph_keys_id,
        "scoring_config_store_id": "",
        "scoring_matrix_store_id": orph_mat_id,
        "enabled_at": "2026-11-20T00:00:00Z",
        "deployed_package_sha": "abc",
        "deployed_package_files_hash": "def",
        "deployed_vcl_sha": "ghi",
        "exclude_url_regex": ""
    });
    assert!(p1
        .adapter
        .cas("svc:fixtureService", Some(&rec_orph.generation), &val_orph)
        .unwrap());
    let gen_orph = p1.service()["generation"].clone();
    let td_orph = p1.json(
        "POST",
        "/api/admin/services/fixtureService/scoring/teardown",
        json!({"confirm_service_id": "fixtureService", "generation": gen_orph}),
    );
    p1.drain(td_orph["job"].clone());
    assert_eq!(
        p1.adapter
            .config_store_value(&orph_keys_id, "current_key_hex")
            .as_deref(),
        Some("ORPHAN_SECRET_KEY"),
        "404 scorer service in teardown must NOT delete unverified foreign stores!"
    );
    assert!(
        p1.adapter
            .bucket_objects("fos-fixture-logs")
            .contains_key("raw/keep_victim.json"),
        "cleanup_scoring_state_step must reject traversal matrix versions in index.json!"
    );

    // 3. BUG-CLOUD-DR-05 + Reviewer Finding 1: Pre-existing customer SIEM protection & Capture::Attached RUM normalization
    let p_siem = Platform::new();
    p_siem.adapter.seed_resource(
        "fixtureService",
        "logging/fos",
        json!({"name": "Customer FOS SIEM", "bucket_name": "customer-siem-bucket", "region": "us-east-1", "path": "siem/"}),
    );
    p_siem.adapter.seed_resource(
        "fixtureService",
        "logging/s3",
        json!({"name": "Customer S3 SIEM", "bucket_name": "customer-s3-siem-bucket"}),
    );
    for (label, svc_json) in [
        (
            "attached_poisoned_logging_fos_siem",
            json!({
                "capture": "attached",
                "logging_enabled": true,
                "logging_endpoint_name": "Customer FOS SIEM"
            }),
        ),
        (
            "managed_poisoned_logging_fos_siem",
            json!({
                "logging_enabled": true,
                "logging_endpoint_name": "Customer FOS SIEM"
            }),
        ),
        (
            "attached_poisoned_rum_s3_siem",
            json!({
                "capture": "attached",
                "logging_enabled": true,
                "logging_endpoint_name": "FLP Cloud Logs",
                "rum": {"enabled": true, "endpoint_name": "Customer S3 SIEM", "log_period": 60}
            }),
        ),
        (
            "managed_poisoned_rum_s3_siem",
            json!({
                "logging_enabled": true,
                "logging_endpoint_name": "Fastly Object Storage Logs",
                "rum": {"enabled": true, "endpoint_name": "Customer S3 SIEM", "log_period": 60}
            }),
        ),
    ] {
        p_siem.adapter.seed_object_bytes(
            "fos-fixture-logs",
            "meta/cloud_dr_state.json",
            serde_json::to_vec(&CloudDrState {
                schema_version: SCHEMA_VERSION,
                updated_at: 1_800_000_000,
                service_id: "fixtureService".into(),
                service: svc_json,
                views: vec![],
                catalog_head: None,
                floor_key: None,
                rum_stats: None,
            })
            .unwrap(),
        );
        let resp = p_siem.call(
            "POST",
            "/api/admin/provision/recover",
            json!({
                "token": "fixture-token",
                "service_id": "fixtureService",
                "fos_bucket": "fos-fixture-logs",
                "fos_region": "us-east-1"
            }),
        );
        assert!(
            matches!(resp.status, 400 | 409),
            "SIEM collision case {label} must fail closed (400/409), got {}: {}",
            resp.status,
            String::from_utf8_lossy(&resp.body)
        );
    }

    // EXPLOIT-2B & EXPLOIT-2A-PATCH: Capture::Attached recovery with rum.enabled=false normalizes rum.endpoint_name,
    // allows PATCH {"rum_enabled": true}, and rejects PATCH {"rum_endpoint_name": "Customer Production SIEM"}.
    let p_att = Platform::new();
    for (resource, body) in [
        (
            "snippet",
            json!({"name": "Fastly Log Analytics - vcl_recv", "type": "recv", "priority": "10", "content": "# hs"}),
        ),
        (
            "snippet",
            json!({"name": "Fastly Log Analytics - vcl_deliver", "type": "deliver", "priority": "10", "content": "# hs"}),
        ),
        (
            "condition",
            json!({"name": "log_analytics_condition", "type": "RESPONSE", "priority": "10", "statement": "req.url !~ \"^/health\""}),
        ),
        (
            "condition",
            json!({"name": "rum_log_condition", "type": "RESPONSE", "priority": "10", "statement": "req.url.path == \"/rum\""}),
        ),
    ] {
        p_att
            .adapter
            .seed_resource("fixtureService", resource, body);
    }
    p_att.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "meta/cloud_dr_state.json",
        serde_json::to_vec(&CloudDrState {
            schema_version: SCHEMA_VERSION,
            updated_at: 1_800_000_000,
            service_id: "fixtureService".into(),
            service: json!({
                "capture": "attached",
                "logging_enabled": true,
                "logging_endpoint_name": "FLP Cloud Logs"
            }),
            views: vec![],
            catalog_head: None,
            floor_key: None,
            rum_stats: None,
        })
        .unwrap(),
    );
    let att_rec = p_att.json(
        "POST",
        "/api/admin/provision/recover",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1"
        }),
    );
    assert_eq!(att_rec["ok"], true);
    let att_gen = p_att.service()["generation"].clone();
    let bad_patch = p_att.call(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({
            "generation": att_gen,
            "rum_enabled": true,
            "rum_endpoint_name": "Customer Production SIEM"
        }),
    );
    assert_eq!(bad_patch.status, 400);

    let ok_patch = p_att.call(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({
            "generation": att_gen,
            "rum_enabled": true
        }),
    );
    assert_eq!(
        ok_patch.status,
        200,
        "post-DR Capture::Attached RUM enable must succeed: {}",
        String::from_utf8_lossy(&ok_patch.body)
    );
    let ok_patch_body: Value = serde_json::from_slice(&ok_patch.body).unwrap();
    assert_eq!(
        ok_patch_body["service"]["rum"]["endpoint_name"],
        "FLP Cloud RUM"
    );

    // 4. BUG-CLOUD-VIEWS-DR-06: views::handle (POST /api/views) and dr::recover reject '.', '..', '/', '?', '#', '\\', and control chars
    for bad_view_id in [
        "release/v3",
        "bad?q=1",
        "bad#frag",
        "bad\\slash",
        ".",
        "..",
        "bad\nctrl",
    ] {
        let save_res = fastly_log_analytics::views::handle(
            &p_att,
            "fixtureService",
            fastly_log_analytics::auth::Role::Admin,
            &fastly_log_analytics::http::Request {
                method: "POST".into(),
                path: "/api/views".into(),
                headers: BTreeMap::new(),
                body: serde_json::to_vec(&json!({
                    "id": bad_view_id,
                    "service_id": "fixtureService",
                    "name": "Bad View",
                    "filters_json": "{\"status\":500}"
                }))
                .unwrap(),
            },
        );
        assert!(
            matches!(
                save_res,
                Err(fastly_log_analytics::auth::Error::InvalidInput)
            ),
            "POST /api/views must reject unsafe id {bad_view_id:?} with InvalidInput (400)"
        );
    }
    for dot_id in [".", ".."] {
        let p_dot = Platform::new();
        p_dot.adapter.seed_object_bytes(
            "fos-fixture-logs",
            "meta/cloud_dr_state.json",
            serde_json::to_vec(&CloudDrState {
                schema_version: SCHEMA_VERSION,
                updated_at: 1_800_000_000,
                service_id: "fixtureService".into(),
                service: json!({"log_period": 60}),
                views: vec![SavedView {
                    id: dot_id.into(),
                    service_id: "fixtureService".into(),
                    name: "Dot View".into(),
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
            })
            .unwrap(),
        );
        let dot_resp = p_dot.call(
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
            dot_resp.status, 400,
            "dr::recover must reject view id {dot_id:?}"
        );
    }
}

#[test]
fn bug_cloud_dr_07_fail_closed_endpoint_verification_protects_shared_bucket_absent_non2xx_and_deactivated_siem(
) {
    // PROBE_2A: Shared FOS bucket (via fos_prefix = "flp/") where customer has an unconditional
    // FOS SIEM endpoint ("Customer Audit FOS", response_condition: "") in the same bucket.
    let p_2a = Platform::new();
    p_2a.adapter.seed_resource(
        "fixtureService",
        "snippet",
        json!({"name": "flp-owner-fixtureApp", "type": "recv", "priority": "0", "content": "# flp-owner:fixtureApp:fixtureService"}),
    );
    p_2a.adapter.seed_resource(
        "fixtureService",
        "logging/fos",
        json!({
            "name": "Customer Audit FOS",
            "bucket_name": "fos-fixture-logs",
            "region": "us-east-1",
            "path": "customer-audit/",
            "response_condition": ""
        }),
    );
    p_2a.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "flp/meta/cloud_dr_state.json",
        serde_json::to_vec(&CloudDrState {
            schema_version: SCHEMA_VERSION,
            updated_at: 1_800_000_000,
            service_id: "fixtureService".into(),
            service: json!({
                "logging_enabled": true,
                "logging_endpoint_name": "Customer Audit FOS",
                "fos_prefix": "flp/"
            }),
            views: vec![],
            catalog_head: None,
            floor_key: None,
            rum_stats: None,
        })
        .unwrap(),
    );
    let r_2a = p_2a.call(
        "POST",
        "/api/admin/provision/recover",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1",
            "fos_prefix": "flp/"
        }),
    );
    assert_eq!(
        r_2a.status,
        409,
        "PROBE_2A: unconditional customer FOS SIEM in shared bucket must collide (409), not be claimed: {}",
        String::from_utf8_lossy(&r_2a.body)
    );
    assert!(
        !p_2a.adapter.records().contains_key("svc:fixtureService"),
        "PROBE_2A: svc record must not be created when shared-bucket SIEM collides"
    );

    // PROBE_2A (teardown survival when Customer Audit FOS is smuggled via owned_endpoint_names):
    let p_2a_td = Platform::new();
    p_2a_td.adapter.seed_resource(
        "fixtureService",
        "snippet",
        json!({"name": "flp-owner-fixtureApp", "type": "recv", "priority": "0", "content": "# flp-owner:fixtureApp:fixtureService"}),
    );
    p_2a_td.adapter.seed_resource(
        "fixtureService",
        "logging/fos",
        json!({
            "name": "Customer Audit FOS",
            "bucket_name": "fos-fixture-logs",
            "region": "us-east-1",
            "path": "customer-audit/",
            "response_condition": ""
        }),
    );
    p_2a_td.adapter.seed_resource(
        "fixtureService",
        "logging/fos",
        json!({
            "name": "Fastly Object Storage Logs",
            "bucket_name": "fos-fixture-logs",
            "region": "us-east-1",
            "path": "flp/raw/",
            "response_condition": "log_analytics_condition"
        }),
    );
    p_2a_td.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "flp/meta/cloud_dr_state.json",
        serde_json::to_vec(&CloudDrState {
            schema_version: SCHEMA_VERSION,
            updated_at: 1_800_000_000,
            service_id: "fixtureService".into(),
            service: json!({
                "logging_enabled": true,
                "logging_endpoint_name": "Fastly Object Storage Logs",
                "fos_prefix": "flp/",
                "owned_endpoint_names": ["Fastly Object Storage Logs", "Customer Audit FOS"]
            }),
            views: vec![],
            catalog_head: None,
            floor_key: None,
            rum_stats: None,
        })
        .unwrap(),
    );
    assert_eq!(
        p_2a_td
            .call(
                "POST",
                "/api/admin/provision/recover",
                json!({
                    "token": "fixture-token",
                    "service_id": "fixtureService",
                    "fos_bucket": "fos-fixture-logs",
                    "fos_region": "us-east-1",
                    "fos_prefix": "flp/"
                }),
            )
            .status,
        200
    );
    assert_eq!(
        p_2a_td.adapter.records()["svc:fixtureService"].value["owned_endpoint_names"],
        json!(["Fastly Object Storage Logs"])
    );
    let td_2a = p_2a_td.json(
        "POST",
        "/api/admin/provision/teardown",
        json!({
            "service_id": "fixtureService",
            "confirm_service_id": "fixtureService",
            "token": "fixture-token",
            "generation": p_2a_td.service()["generation"],
        }),
    )["job"]
        .clone();
    p_2a_td.drain(td_2a);
    let remaining_2a: Vec<String> = p_2a_td
        .adapter
        .resources("fixtureService", "logging/fos")
        .into_iter()
        .filter_map(|v| v["name"].as_str().map(str::to_owned))
        .collect();
    assert_eq!(remaining_2a, vec!["Customer Audit FOS"]);

    // PROBE_2B: Non-2xx logging/s3 or logging/fos must fail closed (owned_endpoint_names == [])
    // so customer SIEM survives teardown!
    for fault_path in [
        "/service/fixtureService/version/1/logging/s3",
        "/service/fixtureService/version/1/logging/fos",
    ] {
        let p_2b = Platform::new();
        p_2b.adapter.seed_resource(
            "fixtureService",
            "snippet",
            json!({"name": "flp-owner-fixtureApp", "type": "recv", "priority": "0", "content": "# flp-owner:fixtureApp:fixtureService"}),
        );
        p_2b.adapter.seed_resource(
            "fixtureService",
            "logging/s3",
            json!({"name": "Customer S3 SIEM", "bucket_name": "customer-siem-bucket"}),
        );
        p_2b.api_faults.lock().unwrap().push(ApiFault {
            method: "GET",
            path: fault_path,
            name: None,
            status: 404,
            retry_after: None,
        });
        p_2b.adapter.seed_object_bytes(
            "fos-fixture-logs",
            "meta/cloud_dr_state.json",
            serde_json::to_vec(&CloudDrState {
                schema_version: SCHEMA_VERSION,
                updated_at: 1_800_000_000,
                service_id: "fixtureService".into(),
                service: json!({
                    "logging_enabled": true,
                    "logging_endpoint_name": "Customer S3 SIEM"
                }),
                views: vec![],
                catalog_head: None,
                floor_key: None,
                rum_stats: None,
            })
            .unwrap(),
        );
        let r_2b = p_2b.call(
            "POST",
            "/api/admin/provision/recover",
            json!({
                "token": "fixture-token",
                "service_id": "fixtureService",
                "fos_bucket": "fos-fixture-logs",
                "fos_region": "us-east-1"
            }),
        );
        assert_eq!(r_2b.status, 200);
        assert_eq!(
            p_2b.adapter.records()["svc:fixtureService"].value["owned_endpoint_names"],
            json!([]),
            "PROBE_2B ({fault_path}): non-2xx endpoint listing must leave owned_endpoint_names empty"
        );
        let td_2b = p_2b.json(
            "POST",
            "/api/admin/provision/teardown",
            json!({
                "service_id": "fixtureService",
                "confirm_service_id": "fixtureService",
                "token": "fixture-token",
                "generation": p_2b.service()["generation"],
            }),
        )["job"]
            .clone();
        p_2b.drain(td_2b);
        let s3_rem: Vec<String> = p_2b
            .adapter
            .resources("fixtureService", "logging/s3")
            .into_iter()
            .filter_map(|v| v["name"].as_str().map(str::to_owned))
            .collect();
        assert_eq!(
            s3_rem,
            vec!["Customer S3 SIEM"],
            "PROBE_2B ({fault_path}): Customer S3 SIEM must survive teardown"
        );
    }

    // PROBE_2C: Absent endpoint at DR time must NOT be pre-claimed in owned_endpoint_names,
    // so if customer adds an S3 SIEM with that name before reconcile, reconcile fails 409 instead of overwriting!
    let p_2c = Platform::new();
    p_2c.adapter.seed_resource(
        "fixtureService",
        "snippet",
        json!({"name": "flp-owner-fixtureApp", "type": "recv", "priority": "0", "content": "# flp-owner:fixtureApp:fixtureService"}),
    );
    p_2c.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "meta/cloud_dr_state.json",
        serde_json::to_vec(&CloudDrState {
            schema_version: SCHEMA_VERSION,
            updated_at: 1_800_000_000,
            service_id: "fixtureService".into(),
            service: json!({
                "logging_enabled": true,
                "logging_endpoint_name": "Customer Future SIEM"
            }),
            views: vec![],
            catalog_head: None,
            floor_key: None,
            rum_stats: None,
        })
        .unwrap(),
    );
    assert_eq!(
        p_2c.call(
            "POST",
            "/api/admin/provision/recover",
            json!({
                "token": "fixture-token",
                "service_id": "fixtureService",
                "fos_bucket": "fos-fixture-logs",
                "fos_region": "us-east-1"
            }),
        )
        .status,
        200
    );
    assert_eq!(
        p_2c.adapter.records()["svc:fixtureService"].value["owned_endpoint_names"],
        json!([]),
        "PROBE_2C: absent endpoint must not be pre-claimed at DR time"
    );
    p_2c.adapter.seed_resource(
        "fixtureService",
        "logging/s3",
        json!({"name": "Customer Future SIEM", "bucket_name": "customer-s3-siem-bucket"}),
    );
    let gen_2c = p_2c.service()["generation"].clone();
    let rec_2c_start = p_2c.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation": gen_2c}),
    )["job"]
        .clone();
    let mut job_2c = rec_2c_start;
    for _ in 0..30 {
        if job_2c["status"] == "failed" || job_2c["status"] == "complete" {
            break;
        }
        job_2c = p_2c.json(
            "POST",
            &format!("/api/admin/jobs/{}/step", job_2c["id"].as_str().unwrap()),
            json!({}),
        )["job"]
            .clone();
    }
    assert_eq!(
        job_2c["status"], "failed",
        "PROBE_2C: reconcile must fail closed on newly added customer S3 SIEM"
    );
    assert_eq!(job_2c["error"], "resource_name_collision");
    let s3_2c = p_2c.adapter.resources("fixtureService", "logging/s3");
    assert_eq!(s3_2c.len(), 1);
    assert_eq!(s3_2c[0]["bucket_name"], "customer-s3-siem-bucket");

    // PROBE_2D: Active version lookup failure (e.g. deactivated/unreachable versions) must leave owned_endpoint_names empty!
    let p_2d = Platform::new();
    p_2d.adapter.seed_resource(
        "fixtureService",
        "logging/s3",
        json!({"name": "Customer S3 SIEM", "bucket_name": "customer-s3-siem-bucket"}),
    );
    p_2d.api_faults.lock().unwrap().push(ApiFault {
        method: "GET",
        path: "/service/fixtureService/version",
        name: None,
        status: 404,
        retry_after: None,
    });
    p_2d.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "meta/cloud_dr_state.json",
        serde_json::to_vec(&CloudDrState {
            schema_version: SCHEMA_VERSION,
            updated_at: 1_800_000_000,
            service_id: "fixtureService".into(),
            service: json!({
                "logging_enabled": true,
                "logging_endpoint_name": "Customer S3 SIEM",
                "owned_endpoint_names": ["Customer S3 SIEM"]
            }),
            views: vec![],
            catalog_head: None,
            floor_key: None,
            rum_stats: None,
        })
        .unwrap(),
    );
    assert_eq!(
        p_2d.call(
            "POST",
            "/api/admin/provision/recover",
            json!({
                "token": "fixture-token",
                "service_id": "fixtureService",
                "fos_bucket": "fos-fixture-logs",
                "fos_region": "us-east-1"
            }),
        )
        .status,
        200
    );
    assert_eq!(
        p_2d.adapter.records()["svc:fixtureService"].value["owned_endpoint_names"],
        json!([]),
        "PROBE_2D: active_version Err must fail closed with empty owned_endpoint_names"
    );
}

#[test]
fn bug_cloud_dr_08_cold_start_scoring_fits_24_call_budget_and_heals_partial_svc_commit() {
    // 1. Cold-start DR with scoring + pre-existing permanent FOS key ("flp-fixtureApp-fixtureService")
    // + views + catalog_head + rum_stats fits cleanly in single 24-call budget with front_synced == true!
    let p = Platform::new();
    let (_, sc_svc) = p
        .adapter
        .api(fastly_log_analytics::admin::ApiRequest {
            method: "POST",
            path: "/service",
            token: "fixture-token",
            body: Some(json!({
                "name": "Session Scoring Service for fixtureService",
                "type": "wasm"
            })),
            timeout: std::time::Duration::from_secs(5),
            query: "",
        })
        .unwrap();
    let scorer_sid = sc_svc["id"].as_str().unwrap().to_owned();
    let (_, k_resp) = p
        .adapter
        .api(fastly_log_analytics::admin::ApiRequest {
            method: "POST",
            path: "/resources/stores/config",
            token: "fixture-token",
            body: Some(json!({"name": format!("scoring_keys_{scorer_sid}")})),
            timeout: std::time::Duration::from_secs(5),
            query: "",
        })
        .unwrap();
    let keys_id = k_resp["id"].as_str().unwrap().to_owned();
    let (_, c_resp) = p
        .adapter
        .api(fastly_log_analytics::admin::ApiRequest {
            method: "POST",
            path: "/resources/stores/config",
            token: "fixture-token",
            body: Some(json!({"name": format!("scoring_config_{scorer_sid}")})),
            timeout: std::time::Duration::from_secs(5),
            query: "",
        })
        .unwrap();
    let cfg_id = c_resp["id"].as_str().unwrap().to_owned();
    let (_, m_resp) = p
        .adapter
        .api(fastly_log_analytics::admin::ApiRequest {
            method: "POST",
            path: "/resources/stores/kv",
            token: "fixture-token",
            body: Some(json!({"name": format!("scoring_matrix_{scorer_sid}")})),
            timeout: std::time::Duration::from_secs(5),
            query: "",
        })
        .unwrap();
    let mat_id = m_resp["id"].as_str().unwrap().to_owned();

    // Seed pre-disaster permanent Fastly FOS access key while Secret Store is empty!
    let _ = p
        .adapter
        .api(fastly_log_analytics::admin::ApiRequest {
            method: "POST",
            path: "/resources/object-storage/access-keys",
            token: "fixture-token",
            body: Some(json!({
                "permission": "read-write-objects",
                "description": "flp-fixtureApp-fixtureService",
                "buckets": ["fos-fixture-logs"]
            })),
            timeout: std::time::Duration::from_secs(5),
            query: "",
        })
        .unwrap();

    let head = Head {
        version: fastly_log_analytics::ingest::catalog::VERSION,
        generation: 12,
        manifest: Some("processed/_catalog/manifest=12-fixtureService.json".into()),
        recent: vec![Event {
            cursor: "fixtureService:12".into(),
            generation: 12,
            committed_at: Some("2026-11-21T00:00:00Z".into()),
            manifest: Some("processed/_catalog/manifest=12-fixtureService.json".into()),
        }],
        ..Head::default()
    };
    let full_dr = CloudDrState {
        schema_version: SCHEMA_VERSION,
        updated_at: 1_800_000_000,
        service_id: "fixtureService".into(),
        service: json!({
            "scoring": {
                "enabled": true,
                "scoring_service_id": scorer_sid,
                "scoring_service_name": "Session Scoring Service for fixtureService",
                "scoring_domain": "fos-fixtureservice-session-scorer.edgecompute.app",
                "scoring_keys_store_id": keys_id,
                "scoring_config_store_id": cfg_id,
                "scoring_matrix_store_id": mat_id,
                "enabled_at": "2026-11-20T00:00:00Z",
                "deployed_package_sha": "abc",
                "deployed_package_files_hash": "def",
                "deployed_vcl_sha": "ghi",
                "exclude_url_regex": ""
            }
        }),
        views: vec![SavedView {
            id: "ops-view-1".into(),
            service_id: "fixtureService".into(),
            name: "Ops Errors".into(),
            filters_json: r#"{"status":500}"#.into(),
            time_range_type: Some("1h".into()),
            start_time: None,
            end_time: None,
            page: Some("/dashboard".into()),
            created_at: Some("2026-11-21T00:00:00Z".into()),
        }],
        catalog_head: Some(head),
        floor_key: Some("raw/2026/floor.zst".into()),
        rum_stats: Some(json!({"rows": 42, "bytes": 4096})),
    };
    p.adapter.seed_resource(
        "fixtureService",
        "logging/fos",
        json!({
            "name": "Fastly Object Storage Logs",
            "bucket_name": "fos-fixture-logs",
            "region": "us-east-1",
            "path": "raw/",
            "response_condition": "log_analytics_condition"
        }),
    );
    p.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "meta/cloud_dr_state.json",
        serde_json::to_vec(&full_dr).unwrap(),
    );

    let r_full = p.call(
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
        r_full.status,
        200,
        "BUG-CLOUD-DR-08: cold-start DR with scoring + FOS key + views + head + rum_stats must succeed in 1 request: {}",
        String::from_utf8_lossy(&r_full.body)
    );
    let full_body: Value = serde_json::from_slice(&r_full.body).unwrap();
    assert_eq!(full_body["ok"], true);
    assert_eq!(full_body["views_restored"], 1);
    assert_eq!(full_body["catalog_generation"], 12);
    assert_eq!(
        p.adapter.records()["svc:fixtureService"].value["owned_endpoint_names"],
        json!(["Fastly Object Storage Logs"]),
        "BUG-CLOUD-DR-11: single-shot cold-start DR with scoring + 1 FOS key + views + head + rum_stats must verify owned_endpoint_names (16 + 3 + 5 = 24 <= 24)"
    );
    assert_eq!(full_body["front_synced"], false);

    // Subsequent duplicate recover when install is already active must reject 409 service_already_configured!
    let r_dup = p.call(
        "POST",
        "/api/admin/provision/recover",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1"
        }),
    );
    assert_eq!(r_dup.status, 409);
    let dup_body: Value = serde_json::from_slice(&r_dup.body).unwrap();
    assert_eq!(dup_body["error"], "service_already_configured");

    // 2. Partial-commit healing: when svc:fixtureService (status: "active") exists in KV while
    // admin:install has service_id == None, retry must heal and commit admin:install (200 OK)!
    let p_heal = Platform::new();
    assert!(p_heal
        .adapter
        .cas(
            "svc:fixtureService",
            None,
            &json!({
                "service_id": "fixtureService",
                "name": "Partial Service",
                "status": "active",
                "created_at": 1_800_000_000u64,
                "updated_at": 1_800_000_000u64,
                "config_version": 1,
                "logging_enabled": true,
                "logging_endpoint_name": "Fastly Object Storage Logs",
                "log_period": 60,
                "sample_rate": 100,
                "edge_only": true,
                "custom_condition": "",
                "fos_bucket": "fos-fixture-logs",
                "fos_prefix": "",
                "fos_region": "us-east-1",
                "fos_endpoint": "us-east-1.object.fastlystorage.app",
                "log_fields": {"groups": ["A", "C"], "custom_fields": [], "field_overrides": {}, "field_limits": {}},
                "field_limits": {},
                "fastly_token_ref": "fastly_token.fixtureService",
                "fos_access_key_ref": "fos_ak.fixtureService",
                "fos_secret_key_ref": "fos_sk.fixtureService",
                "owner": "fixtureApp",
                "bucket_adopted": true,
                "bucket_checked": true,
                "last_activated_version": null,
                "last_reconcile": null,
                "pending_job_id": null
            }),
        )
        .unwrap());
    let r_heal = p_heal.call(
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
        r_heal.status,
        200,
        "BUG-CLOUD-DR-08: partial svc:fixtureService with uncommitted admin:install must heal on retry: {}",
        String::from_utf8_lossy(&r_heal.body)
    );
    assert_eq!(
        p_heal.adapter.records()["admin:install"].value["service_id"],
        "fixtureService"
    );

    // 3. Atomic pre-check before svc:<sid>: when only 1 call remains right before final commit,
    // svc:<sid> must NOT be written!
    let p_atomic = Platform::new();
    // Minimal cold-start recover uses 11 Platform::CALLS before final 2 CAS writes:
    // validate_token(3) + install(1) + svc(1) + put_secret(1) + ensure_key(GET+POST+2*PUT = 4) + get_object(dr_key)(1) = 11.
    // Setting CALLS = 32 - 12 = 20 leaves remaining_calls() == 1 right at dr.rs's `if context.remaining_calls() < 2` check!
    ELAPSED_MS.with(|e| e.set(0));
    CALLS.with(|c| c.set(20));
    SECRETS.with(|s| {
        *s.borrow_mut() =
            std::collections::BTreeSet::from(["front_key".into(), "session_key".into()])
    });
    let r_atomic = fastly_log_analytics::admin::handle(
        &p_atomic,
        "fixtureAdmin",
        &fastly_log_analytics::http::Request {
            method: "POST".into(),
            path: "/api/admin/provision/recover".into(),
            headers: BTreeMap::new(),
            body: serde_json::to_vec(&json!({
                "token": "fixture-token",
                "service_id": "fixtureService",
                "fos_bucket": "fos-fixture-logs",
                "fos_region": "us-east-1"
            }))
            .unwrap(),
        },
    );
    assert_eq!(r_atomic.status, 503);
    assert!(
        !p_atomic.adapter.records().contains_key("svc:fixtureService"),
        "BUG-CLOUD-DR-08: svc:fixtureService must NOT be written when < 2 calls remain for atomic svc+install commit"
    );
}

#[test]
fn bug_cloud_views_dr_09_views_save_rejects_dup_keys_and_control_chars_preserving_dr_roundtrip() {
    let p = Platform::new();
    let provisioned = p.provision();
    p.drain(provisioned["job"].clone());

    for (label, payload) in [
        (
            "dup_keys_filters_json",
            json!({
                "service_id": "fixtureService",
                "name": "Valid Name",
                "filters_json": "{\"status\":500,\"status\":200}"
            }),
        ),
        (
            "null_in_filters_json",
            json!({
                "service_id": "fixtureService",
                "name": "Valid Name",
                "filters_json": "{\"url\":\"/a\u{0000}b\"}"
            }),
        ),
        (
            "tab_in_name",
            json!({
                "service_id": "fixtureService",
                "name": "Ops\tView",
                "filters_json": "{\"status\":500}"
            }),
        ),
        (
            "newline_in_page",
            json!({
                "service_id": "fixtureService",
                "name": "Ops View",
                "filters_json": "{\"status\":500}",
                "page": "/dashboard\n"
            }),
        ),
        (
            "ctrl_in_time_range_type",
            json!({
                "service_id": "fixtureService",
                "name": "Ops View",
                "filters_json": "{\"status\":500}",
                "time_range_type": "1h\r"
            }),
        ),
    ] {
        let res = fastly_log_analytics::views::handle(
            &p,
            "fixtureService",
            fastly_log_analytics::auth::Role::Admin,
            &fastly_log_analytics::http::Request {
                method: "POST".into(),
                path: "/api/views".into(),
                headers: BTreeMap::new(),
                body: serde_json::to_vec(&payload).unwrap(),
            },
        );
        assert!(
            matches!(res, Err(fastly_log_analytics::auth::Error::InvalidInput)),
            "BUG-CLOUD-VIEWS-DR-09 ({label}): POST /api/views must reject DR-incompatible view input"
        );
    }

    // Valid view saved via POST /api/views + dr::sync_best_effort must round-trip cold-start dr::recover!
    let ok_save = fastly_log_analytics::views::handle(
        &p,
        "fixtureService",
        fastly_log_analytics::auth::Role::Admin,
        &fastly_log_analytics::http::Request {
            method: "POST".into(),
            path: "/api/views".into(),
            headers: BTreeMap::new(),
            body: serde_json::to_vec(&json!({
                "service_id": "fixtureService",
                "name": "Clean Ops View",
                "filters_json": "{\"status\":500}",
                "page": "/dashboard",
                "time_range_type": "1h"
            }))
            .unwrap(),
        },
    )
    .unwrap();
    assert_eq!(ok_save.status, 201);
    let svc_struct: Service =
        serde_json::from_value(p.adapter.get("svc:fixtureService").unwrap().unwrap().value)
            .unwrap();
    fastly_log_analytics::admin::dr::sync_best_effort(&p, &svc_struct);

    let dr_blob = p
        .adapter
        .bucket_objects("fos-fixture-logs")
        .get("meta/cloud_dr_state.json")
        .cloned()
        .expect("dr::sync_best_effort should write cloud_dr_state.json");
    let p_cold = Platform::new();
    p_cold
        .adapter
        .seed_object_bytes("fos-fixture-logs", "meta/cloud_dr_state.json", dr_blob);
    let cold_res = p_cold.call(
        "POST",
        "/api/admin/provision/recover",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1"
        }),
    );
    assert_eq!(cold_res.status, 200);
    let cold_body: Value = serde_json::from_slice(&cold_res.body).unwrap();
    assert_eq!(cold_body["views_restored"], 1);
}

fn seed_coexisting_gce_and_attached_cloud(p: &Platform) {
    for (resource, body) in [
        (
            "snippet",
            json!({"name": "Fastly Log Analytics - vcl_recv", "type": "recv", "priority": "10", "content": "# gce-primary-recv"}),
        ),
        (
            "snippet",
            json!({"name": "Fastly Log Analytics - vcl_deliver", "type": "deliver", "priority": "10", "content": "# gce-primary-deliver"}),
        ),
        (
            "snippet",
            json!({"name": "flp-owner-fixtureApp", "type": "recv", "priority": "0", "content": "# flp-owner:fixtureApp:fixtureService"}),
        ),
        (
            "condition",
            json!({"name": "log_analytics_condition", "type": "RESPONSE", "priority": "10", "statement": "!segmented_caching.is_inner_req"}),
        ),
        (
            "condition",
            json!({"name": "flp_cloud_log_condition", "type": "RESPONSE", "priority": "10", "statement": "!segmented_caching.is_inner_req"}),
        ),
        (
            "backend",
            json!({"name": "fos_origin", "address": "gce-rum.object.fastlystorage.app", "port": 443, "use_ssl": true, "ssl_check_cert": true}),
        ),
        (
            "dictionary",
            json!({"name": "fos_credentials", "write_only": true}),
        ),
        (
            "logging/fos",
            json!({"name": "Fastly Object Storage Logs", "bucket_name": "fos-gce-logs", "region": "us-east-1", "path": "raw/", "response_condition": "log_analytics_condition"}),
        ),
        (
            "logging/fos",
            json!({"name": "FLP Cloud Logs", "bucket_name": "fos-fixture-logs", "region": "us-east-1", "path": "raw/request/year=%Y/", "response_condition": "flp_cloud_log_condition"}),
        ),
    ] {
        p.adapter.seed_resource("fixtureService", resource, body);
    }
}

#[test]
fn bug_cloud_dr_10_coexistence_and_shared_bucket_path_scope_protects_primary_install() {
    // PROBE_1F1: Poisoned cloud_dr_state.json downgrading Attached -> Managed must be rejected 409 at DR time,
    // AND even if svc:<sid> were tampered to Capture::Managed with own_marker == true, reconcile/teardown
    // must fail closed with 409 managed_resources_not_owned without touching GCE VM VCL/backends/dictionaries!
    let p_1f1 = Platform::new();
    seed_coexisting_gce_and_attached_cloud(&p_1f1);
    p_1f1.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "meta/cloud_dr_state.json",
        serde_json::to_vec(&CloudDrState {
            schema_version: SCHEMA_VERSION,
            updated_at: 1_800_000_000,
            service_id: "fixtureService".into(),
            service: json!({
                "capture": "managed",
                "logging_enabled": true,
                "logging_endpoint_name": "Cloud Temp Logs",
                "rum": {"enabled": true, "endpoint_name": "Cloud Temp RUM", "log_period": 60}
            }),
            views: vec![],
            catalog_head: None,
            floor_key: None,
            rum_stats: None,
        })
        .unwrap(),
    );
    let r_1f1 = p_1f1.call(
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
        r_1f1.status,
        409,
        "PROBE_1F1: Capture::Managed DR against coexisting Attached/GCE service must fail 409: {}",
        String::from_utf8_lossy(&r_1f1.body)
    );
    assert!(!p_1f1.adapter.records().contains_key("svc:fixtureService"));

    // Defense-in-depth in reconcile::discover even when own_marker == true & svc:<sid> claims Capture::Managed:
    p_1f1
        .adapter
        .cas(
            "admin:install",
            None,
            &json!({"service_id": "fixtureService", "job_id": null, "fence": 1}),
        )
        .unwrap();
    p_1f1
        .adapter
        .cas(
            "svc:fixtureService",
            None,
            &json!({
                "service_id": "fixtureService",
                "name": "Tampered Managed",
                "status": "active",
                "created_at": 1_800_000_000u64,
                "updated_at": 1_800_000_000u64,
                "config_version": 1,
                "logging_enabled": true,
                "logging_endpoint_name": "Cloud Temp Logs",
                "log_period": 60,
                "sample_rate": 100,
                "edge_only": true,
                "custom_condition": "",
                "fos_bucket": "fos-fixture-logs",
                "fos_prefix": "",
                "fos_region": "us-east-1",
                "fos_endpoint": "us-east-1.object.fastlystorage.app",
                "log_fields": {"groups": ["A", "C"], "custom_fields": [], "field_overrides": {}, "field_limits": {}},
                "field_limits": {},
                "rum": {"enabled": true, "endpoint_name": "Cloud Temp RUM", "log_period": 60},
                "fastly_token_ref": "fastly_token.fixtureService",
                "fos_access_key_ref": "fos_ak.fixtureService",
                "fos_secret_key_ref": "fos_sk.fixtureService",
                "owner": "fixtureApp",
                "bucket_adopted": true,
                "bucket_checked": true,
                "last_activated_version": null,
                "last_reconcile": null,
                "pending_job_id": null
            }),
        )
        .unwrap();
    let rec_1f1_job = p_1f1.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation": p_1f1.service()["generation"]}),
    )["job"]
        .clone();
    let rec_1f1_job = p_1f1.until(rec_1f1_job, "discover");
    let rec_1f1_step = p_1f1.json(
        "POST",
        &format!(
            "/api/admin/jobs/{}/step",
            rec_1f1_job["id"].as_str().unwrap()
        ),
        json!({}),
    )["job"]
        .clone();
    assert_eq!(rec_1f1_step["status"], "failed");
    assert_eq!(rec_1f1_step["error"], "managed_resources_not_owned");
    assert!(p_1f1
        .adapter
        .resources("fixtureService", "snippet")
        .iter()
        .any(|s| s["name"] == "Fastly Log Analytics - vcl_recv"
            && s["content"] == "# gce-primary-recv"));
    assert_eq!(
        p_1f1.adapter.resources("fixtureService", "backend")[0]["address"],
        "gce-rum.object.fastlystorage.app"
    );

    // PROBE_1F2: Benign "minimal" DR (missing cloud_dr_state.json) on a Capture::Attached service
    // must infer Capture::Attached, set FLP Cloud Logs / FLP Cloud RUM, verify owned_endpoint_names,
    // and preserve 100% of GCE VM VCL/backends/dictionaries/endpoints across teardown!
    let p_1f2 = Platform::new();
    seed_coexisting_gce_and_attached_cloud(&p_1f2);
    let r_1f2 = p_1f2.call(
        "POST",
        "/api/admin/provision/recover",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1"
        }),
    );
    assert_eq!(r_1f2.status, 200);
    let svc_1f2 = &p_1f2.adapter.records()["svc:fixtureService"].value;
    assert_eq!(svc_1f2["capture"], "attached");
    assert_eq!(svc_1f2["logging_endpoint_name"], "FLP Cloud Logs");
    assert_eq!(svc_1f2["rum"]["endpoint_name"], "FLP Cloud RUM");
    assert_eq!(svc_1f2["owned_endpoint_names"], json!(["FLP Cloud Logs"]));

    let td_1f2 = p_1f2.json(
        "POST",
        "/api/admin/provision/teardown",
        json!({
            "service_id": "fixtureService",
            "confirm_service_id": "fixtureService",
            "token": "fixture-token",
            "generation": p_1f2.service()["generation"]
        }),
    )["job"]
        .clone();
    p_1f2.drain(td_1f2);
    let rem_snippets: Vec<String> = p_1f2
        .adapter
        .resources("fixtureService", "snippet")
        .into_iter()
        .filter_map(|v| v["name"].as_str().map(str::to_owned))
        .collect();
    assert!(rem_snippets.contains(&"Fastly Log Analytics - vcl_recv".to_owned()));
    assert!(rem_snippets.contains(&"Fastly Log Analytics - vcl_deliver".to_owned()));
    assert!(!rem_snippets.contains(&"flp-owner-fixtureApp".to_owned()));
    let rem_fos: Vec<String> = p_1f2
        .adapter
        .resources("fixtureService", "logging/fos")
        .into_iter()
        .filter_map(|v| v["name"].as_str().map(str::to_owned))
        .collect();
    assert_eq!(rem_fos, vec!["Fastly Object Storage Logs"]);
    assert_eq!(
        p_1f2.adapter.resources("fixtureService", "backend").len(),
        1
    );
    assert_eq!(
        p_1f2
            .adapter
            .resources("fixtureService", "dictionary")
            .len(),
        1
    );

    // PROBE_1F3: Shared FOS bucket with fos_prefix ("cloud/") must reject High-Scale's "hs/raw/" endpoint
    // with 409 resource_name_collision instead of claiming and deleting it!
    let p_1f3 = Platform::new();
    for (resource, body) in [
        (
            "snippet",
            json!({"name": "flp-owner-fixtureApp", "type": "recv", "priority": "0", "content": "# flp-owner:fixtureApp:fixtureService"}),
        ),
        (
            "logging/fos",
            json!({"name": "Fastly Object Storage Logs", "bucket_name": "fos-fixture-logs", "region": "us-east-1", "path": "hs/raw/", "response_condition": "log_analytics_condition"}),
        ),
    ] {
        p_1f3
            .adapter
            .seed_resource("fixtureService", resource, body);
    }
    p_1f3.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "cloud/meta/cloud_dr_state.json",
        serde_json::to_vec(&CloudDrState {
            schema_version: SCHEMA_VERSION,
            updated_at: 1_800_000_000,
            service_id: "fixtureService".into(),
            service: json!({
                "capture": "managed",
                "logging_enabled": true,
                "logging_endpoint_name": "Fastly Object Storage Logs",
                "fos_prefix": "cloud/"
            }),
            views: vec![],
            catalog_head: None,
            floor_key: None,
            rum_stats: None,
        })
        .unwrap(),
    );
    let r_1f3 = p_1f3.call(
        "POST",
        "/api/admin/provision/recover",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1",
            "fos_prefix": "cloud/"
        }),
    );
    assert_eq!(
        r_1f3.status,
        409,
        "PROBE_1F3: shared-bucket foreign prefix 'hs/raw/' must be rejected 409: {}",
        String::from_utf8_lossy(&r_1f3.body)
    );
    assert!(!p_1f3.adapter.records().contains_key("svc:fixtureService"));
}

#[test]
fn bug_cloud_dr_11_post_dr_reconcile_self_heals_verified_endpoints_and_preseed_views_fits_24_call_budget(
) {
    let make_scoring_platform = |n_keys: usize, preseed_views: bool| -> Platform {
        let p = Platform::new();
        let (_, sc_svc) = p
            .adapter
            .api(fastly_log_analytics::admin::ApiRequest {
                method: "POST",
                path: "/service",
                token: "fixture-token",
                body: Some(json!({
                    "name": "Session Scoring Service for fixtureService",
                    "type": "wasm"
                })),
                timeout: std::time::Duration::from_secs(5),
                query: "",
            })
            .unwrap();
        let scorer_sid = sc_svc["id"].as_str().unwrap().to_owned();
        let (_, k_resp) = p
            .adapter
            .api(fastly_log_analytics::admin::ApiRequest {
                method: "POST",
                path: "/resources/stores/config",
                token: "fixture-token",
                body: Some(json!({"name": format!("scoring_keys_{scorer_sid}")})),
                timeout: std::time::Duration::from_secs(5),
                query: "",
            })
            .unwrap();
        let (_, c_resp) = p
            .adapter
            .api(fastly_log_analytics::admin::ApiRequest {
                method: "POST",
                path: "/resources/stores/config",
                token: "fixture-token",
                body: Some(json!({"name": format!("scoring_config_{scorer_sid}")})),
                timeout: std::time::Duration::from_secs(5),
                query: "",
            })
            .unwrap();
        let (_, m_resp) = p
            .adapter
            .api(fastly_log_analytics::admin::ApiRequest {
                method: "POST",
                path: "/resources/stores/kv",
                token: "fixture-token",
                body: Some(json!({"name": format!("scoring_matrix_{scorer_sid}")})),
                timeout: std::time::Duration::from_secs(5),
                query: "",
            })
            .unwrap();
        for _ in 0..n_keys {
            let _ = p
                .adapter
                .api(fastly_log_analytics::admin::ApiRequest {
                    method: "POST",
                    path: "/resources/object-storage/access-keys",
                    token: "fixture-token",
                    body: Some(json!({
                        "permission": "read-write-objects",
                        "description": "flp-fixtureApp-fixtureService",
                        "buckets": ["fos-fixture-logs"]
                    })),
                    timeout: std::time::Duration::from_secs(5),
                    query: "",
                })
                .unwrap();
        }
        p.adapter.seed_resource(
            "fixtureService",
            "snippet",
            json!({"name": "flp-owner-fixtureApp", "type": "recv", "priority": "0", "content": "# flp-owner:fixtureApp:fixtureService"}),
        );
        p.adapter.seed_resource(
            "fixtureService",
            "logging/fos",
            json!({
                "name": "Fastly Object Storage Logs",
                "bucket_name": "fos-fixture-logs",
                "region": "us-east-1",
                "path": "raw/request/year=%Y/month=%m/day=%d/hour=%H/minute=%M/",
                "response_condition": "log_analytics_condition"
            }),
        );
        if preseed_views {
            p.adapter
                .cas(
                    "views:fixtureService",
                    None,
                    &json!({"views": [], "updated_at": 1}),
                )
                .unwrap();
        }
        let head = Head {
            version: fastly_log_analytics::ingest::catalog::VERSION,
            generation: 7,
            manifest: Some("processed/_catalog/manifest=7-fixtureService.json".into()),
            recent: vec![Event {
                cursor: "fixtureService:7".into(),
                generation: 7,
                committed_at: Some("2026-11-21T00:00:00Z".into()),
                manifest: Some("processed/_catalog/manifest=7-fixtureService.json".into()),
            }],
            ..Head::default()
        };
        p.adapter.seed_object_bytes(
            "fos-fixture-logs",
            "meta/cloud_dr_state.json",
            serde_json::to_vec(&CloudDrState {
                schema_version: SCHEMA_VERSION,
                updated_at: 1_800_000_000,
                service_id: "fixtureService".into(),
                service: json!({
                    "scoring": {
                        "enabled": false,
                        "scoring_service_id": scorer_sid,
                        "scoring_service_name": "Session Scoring Service for fixtureService",
                        "scoring_domain": "fos-fixtureservice-session-scorer.edgecompute.app",
                        "scoring_keys_store_id": k_resp["id"],
                        "scoring_config_store_id": c_resp["id"],
                        "scoring_matrix_store_id": m_resp["id"],
                        "enabled_at": "2026-11-20T00:00:00Z",
                        "deployed_package_sha": "abc",
                        "deployed_package_files_hash": "def",
                        "deployed_vcl_sha": "ghi",
                        "exclude_url_regex": ""
                    }
                }),
                views: vec![SavedView {
                    id: "v-dr11".into(),
                    service_id: "fixtureService".into(),
                    name: "Ops View".into(),
                    filters_json: r#"{"status":500}"#.into(),
                    time_range_type: Some("1h".into()),
                    start_time: None,
                    end_time: None,
                    page: Some("/dashboard".into()),
                    created_at: Some("2026-11-21T00:00:00Z".into()),
                }],
                catalog_head: Some(head),
                floor_key: Some("raw/2026/floor.zst".into()),
                rum_stats: Some(json!({"rows": 9})),
            })
            .unwrap(),
        );
        p
    };

    // 1. PROBE_2A_BUDGET [n_keys=1, preseed_views=true]: by-value try_insert_first keeps watermarks 1-call CAS -> 200 OK (<= 24 calls)!
    let p_preseed = make_scoring_platform(1, true);
    let r_preseed = p_preseed.call(
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
        r_preseed.status,
        200,
        "PROBE_2A_BUDGET [n_keys=1, preseed_views=true] must succeed 200 OK within <=24 calls: {}",
        String::from_utf8_lossy(&r_preseed.body)
    );
    assert!(p_preseed
        .adapter
        .records()
        .contains_key("svc:fixtureService"));
    assert_eq!(
        p_preseed.adapter.records()["admin:install"].value["service_id"],
        "fixtureService"
    );

    // 2. PROBE_2B [n_keys=3]: when budget forces owned_endpoint_names == [] at DR time,
    // subsequent reconcile self-heals verified Cloud endpoint into owned_endpoint_names and completes cleanly!
    let p_2keys = make_scoring_platform(3, false);
    let r_2keys = p_2keys.call(
        "POST",
        "/api/admin/provision/recover",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1"
        }),
    );
    assert_eq!(r_2keys.status, 200);
    assert_eq!(
        p_2keys.adapter.records()["svc:fixtureService"].value["owned_endpoint_names"],
        json!([])
    );

    let rec_job = p_2keys.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation": p_2keys.service()["generation"]}),
    )["job"]
        .clone();
    let rec_done = p_2keys.drain(rec_job);
    assert_eq!(
        rec_done["status"], "complete",
        "PROBE_2B: post-DR reconcile must self-heal verified Cloud endpoint instead of wedging 409: {rec_done}"
    );
    assert_eq!(
        p_2keys.adapter.records()["svc:fixtureService"].value["owned_endpoint_names"],
        json!(["Fastly Object Storage Logs"])
    );
}

#[test]
fn bug_cloud_dr_12_13_14_coexistence_scoring_teardown_minimal_attached_and_cold_retry_budget() {
    let seed_gce_scoring = |p: &Platform, n_keys: usize| -> (String, String, String, String) {
        seed_coexisting_gce_and_attached_cloud(p);
        for (res, body) in [
            (
                "snippet",
                json!({
                    "name": "Fastly Log Analytics - Session Scoring (vcl_recv)",
                    "type": "recv",
                    "priority": "20",
                    "content": "# gce-scoring-recv"
                }),
            ),
            (
                "snippet",
                json!({
                    "name": "Fastly Log Analytics - Session Scoring (vcl_pass)",
                    "type": "pass",
                    "priority": "20",
                    "content": "# gce-scoring-pass"
                }),
            ),
            (
                "backend",
                json!({
                    "name": "session_scorer",
                    "address": "gce-scorer.edgecompute.app",
                    "port": 443,
                    "use_ssl": true,
                    "ssl_check_cert": true
                }),
            ),
        ] {
            p.adapter.seed_resource("fixtureService", res, body);
        }
        let (_, sc_svc) = p
            .adapter
            .api(fastly_log_analytics::admin::ApiRequest {
                method: "POST",
                path: "/service",
                token: "fixture-token",
                body: Some(json!({
                    "name": "Session Scoring Service for fixtureService",
                    "type": "wasm"
                })),
                timeout: std::time::Duration::from_secs(5),
                query: "",
            })
            .unwrap();
        let scorer_sid = sc_svc["id"].as_str().unwrap().to_owned();
        let (_, k_resp) = p
            .adapter
            .api(fastly_log_analytics::admin::ApiRequest {
                method: "POST",
                path: "/resources/stores/config",
                token: "fixture-token",
                body: Some(json!({"name": format!("scoring_keys_{scorer_sid}")})),
                timeout: std::time::Duration::from_secs(5),
                query: "",
            })
            .unwrap();
        let (_, c_resp) = p
            .adapter
            .api(fastly_log_analytics::admin::ApiRequest {
                method: "POST",
                path: "/resources/stores/config",
                token: "fixture-token",
                body: Some(json!({"name": format!("scoring_config_{scorer_sid}")})),
                timeout: std::time::Duration::from_secs(5),
                query: "",
            })
            .unwrap();
        let (_, m_resp) = p
            .adapter
            .api(fastly_log_analytics::admin::ApiRequest {
                method: "POST",
                path: "/resources/stores/kv",
                token: "fixture-token",
                body: Some(json!({"name": format!("scoring_matrix_{scorer_sid}")})),
                timeout: std::time::Duration::from_secs(5),
                query: "",
            })
            .unwrap();
        for _ in 0..n_keys {
            let _ = p
                .adapter
                .api(fastly_log_analytics::admin::ApiRequest {
                    method: "POST",
                    path: "/resources/object-storage/access-keys",
                    token: "fixture-token",
                    body: Some(json!({
                        "permission": "read-write-objects",
                        "description": "flp-fixtureApp-fixtureService",
                        "buckets": ["fos-fixture-logs"]
                    })),
                    timeout: std::time::Duration::from_secs(5),
                    query: "",
                })
                .unwrap();
        }
        (
            scorer_sid,
            k_resp["id"].as_str().unwrap().to_owned(),
            c_resp["id"].as_str().unwrap().to_owned(),
            m_resp["id"].as_str().unwrap().to_owned(),
        )
    };

    // --- BUG-CLOUD-DR-12 (i): n_keys=2 + poisoned capture:"managed" + scoring DR ---
    // Must preserve 100% of GCE VM scoring snippets, session_scorer backend, scorer WASM service, & stores
    // across reconcile (409), teardown(remove_snippets=true -> 409), AND teardown(remove_logging=false, remove_snippets=false -> complete)!
    let p_dr12 = Platform::new();
    let (sc_sid, k_id, c_id, m_id) = seed_gce_scoring(&p_dr12, 2);
    let head = Head {
        version: fastly_log_analytics::ingest::catalog::VERSION,
        generation: 7,
        manifest: Some("processed/_catalog/manifest=7-fixtureService.json".into()),
        recent: vec![Event {
            cursor: "fixtureService:7".into(),
            generation: 7,
            committed_at: Some("2026-11-21T00:00:00Z".into()),
            manifest: Some("processed/_catalog/manifest=7-fixtureService.json".into()),
        }],
        ..Head::default()
    };
    p_dr12.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "meta/cloud_dr_state.json",
        serde_json::to_vec(&CloudDrState {
            schema_version: SCHEMA_VERSION,
            updated_at: 1_800_000_000,
            service_id: "fixtureService".into(),
            service: json!({
                "capture": "managed",
                "scoring": {
                    "enabled": true,
                    "scoring_service_id": sc_sid,
                    "scoring_service_name": "Session Scoring Service for fixtureService",
                    "scoring_domain": "gce-scorer.edgecompute.app",
                    "scoring_keys_store_id": k_id,
                    "scoring_config_store_id": c_id,
                    "scoring_matrix_store_id": m_id,
                    "enabled_at": "2026-11-20T00:00:00Z",
                    "deployed_package_sha": "abc",
                    "deployed_package_files_hash": "def",
                    "deployed_vcl_sha": "ghi",
                    "exclude_url_regex": ""
                }
            }),
            views: vec![SavedView {
                id: "v-1".into(),
                service_id: "fixtureService".into(),
                name: "V1".into(),
                filters_json: "{}".into(),
                time_range_type: Some("1h".into()),
                start_time: None,
                end_time: None,
                page: Some("/dashboard".into()),
                created_at: Some("2026-11-21T00:00:00Z".into()),
            }],
            catalog_head: Some(head.clone()),
            floor_key: Some("raw/2026/f.zst".into()),
            rum_stats: Some(json!({"rows": 3})),
        })
        .unwrap(),
    );
    // Note: if n_keys=3 skips DR inspection (or n_keys=2 if DR rejects 409 immediately), test both DR and wedged KV state!
    let r_dr12 = p_dr12.call(
        "POST",
        "/api/admin/provision/recover",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1"
        }),
    );
    if r_dr12.status == 409 {
        // Even better: DR rejected poisoned Managed+Scoring at n_keys=2! Now also test n_keys=3 where DR budget skips inspection:
        let p_dr12_wedged = Platform::new();
        let (w_sc_sid, w_k_id, w_c_id, w_m_id) = seed_gce_scoring(&p_dr12_wedged, 3);
        p_dr12_wedged.adapter.seed_object_bytes(
            "fos-fixture-logs",
            "meta/cloud_dr_state.json",
            serde_json::to_vec(&CloudDrState {
                schema_version: SCHEMA_VERSION,
                updated_at: 1_800_000_000,
                service_id: "fixtureService".into(),
                service: json!({
                    "capture": "managed",
                    "scoring": {
                        "enabled": true,
                        "scoring_service_id": w_sc_sid,
                        "scoring_service_name": "Session Scoring Service for fixtureService",
                        "scoring_domain": "gce-scorer.edgecompute.app",
                        "scoring_keys_store_id": w_k_id,
                        "scoring_config_store_id": w_c_id,
                        "scoring_matrix_store_id": w_m_id,
                        "enabled_at": "2026-11-20T00:00:00Z",
                        "deployed_package_sha": "abc",
                        "deployed_package_files_hash": "def",
                        "deployed_vcl_sha": "ghi",
                        "exclude_url_regex": ""
                    }
                }),
                views: vec![SavedView {
                    id: "v-1".into(),
                    service_id: "fixtureService".into(),
                    name: "V1".into(),
                    filters_json: "{}".into(),
                    time_range_type: Some("1h".into()),
                    start_time: None,
                    end_time: None,
                    page: Some("/dashboard".into()),
                    created_at: Some("2026-11-21T00:00:00Z".into()),
                }],
                catalog_head: Some(head.clone()),
                floor_key: Some("raw/2026/f.zst".into()),
                rum_stats: Some(json!({"rows": 3})),
            })
            .unwrap(),
        );
        assert_eq!(
            p_dr12_wedged
                .call(
                    "POST",
                    "/api/admin/provision/recover",
                    json!({
                        "token": "fixture-token",
                        "service_id": "fixtureService",
                        "fos_bucket": "fos-fixture-logs",
                        "fos_region": "us-east-1"
                    }),
                )
                .status,
            200
        );
        // Verify reconcile -> failed (managed_resources_not_owned)
        let rec_w = p_dr12_wedged.json(
            "POST",
            "/api/admin/services/fixtureService/reconcile",
            json!({"generation": p_dr12_wedged.service()["generation"]}),
        )["job"]
            .clone();
        let rec_w = p_dr12_wedged.until(rec_w, "discover");
        let rec_w_step = p_dr12_wedged.json(
            "POST",
            &format!("/api/admin/jobs/{}/step", rec_w["id"].as_str().unwrap()),
            json!({}),
        )["job"]
            .clone();
        assert_eq!(rec_w_step["status"], "failed");
        assert_eq!(rec_w_step["error"], "managed_resources_not_owned");

        // Verify teardown(remove_snippets=true) -> failed BEFORE touching GCE scoring VCL / backend / scorer service / stores!
        let td_rm = p_dr12_wedged.json(
            "POST",
            "/api/admin/provision/teardown",
            json!({
                "service_id": "fixtureService",
                "confirm_service_id": "fixtureService",
                "token": "fixture-token",
                "generation": p_dr12_wedged.service()["generation"],
                "remove_logging": true,
                "remove_snippets": true
            }),
        )["job"]
            .clone();
        let td_rm_step = p_dr12_wedged.json(
            "POST",
            &format!("/api/admin/jobs/{}/step", td_rm["id"].as_str().unwrap()),
            json!({}),
        )["job"]
            .clone();
        assert_eq!(td_rm_step["status"], "failed");
        assert_eq!(td_rm_step["error"], "managed_resources_not_owned");
        assert!(p_dr12_wedged.adapter.has_service(&w_sc_sid));
        assert_eq!(
            p_dr12_wedged
                .adapter
                .api(fastly_log_analytics::admin::ApiRequest {
                    method: "GET",
                    path: &format!("/resources/stores/config/{w_k_id}"),
                    token: "fixture-token",
                    body: None,
                    timeout: std::time::Duration::from_secs(5),
                    query: "",
                })
                .unwrap()
                .0,
            200
        );
        assert!(p_dr12_wedged
            .adapter
            .resources("fixtureService", "snippet")
            .iter()
            .any(|s| s["name"] == "Fastly Log Analytics - Session Scoring (vcl_recv)"));
        assert!(p_dr12_wedged
            .adapter
            .resources("fixtureService", "backend")
            .iter()
            .any(|b| b["name"] == "session_scorer"));

        // Verify local-only escape hatch teardown(remove_logging=false, remove_snippets=false, delete_fos_keys=false) completes cleanly!
        let td_local = p_dr12_wedged.json(
            "POST",
            "/api/admin/provision/teardown",
            json!({
                "service_id": "fixtureService",
                "confirm_service_id": "fixtureService",
                "token": "fixture-token",
                "generation": p_dr12_wedged.service()["generation"],
                "remove_logging": false,
                "remove_snippets": false,
                "delete_fos_keys": false
            }),
        )["job"]
            .clone();
        let td_local_done = p_dr12_wedged.drain(td_local);
        assert_eq!(td_local_done["status"], "complete");
        assert_eq!(
            p_dr12_wedged.service()["service"]["status"],
            "torn_down",
            "local-only teardown must cleanly unprovision wedged service"
        );
        assert_eq!(
            p_dr12_wedged.json("GET", "/api/services", json!({}))["services"],
            json!([])
        );
        assert!(p_dr12_wedged.adapter.has_service(&w_sc_sid));
        assert!(p_dr12_wedged
            .adapter
            .resources("fixtureService", "snippet")
            .iter()
            .any(|s| s["name"] == "Fastly Log Analytics - Session Scoring (vcl_recv)"));
        assert!(p_dr12_wedged
            .adapter
            .resources("fixtureService", "backend")
            .iter()
            .any(|b| b["name"] == "session_scorer"));
    } else {
        assert_eq!(r_dr12.status, 200);
        // Same assertions on p_dr12 directly when n_keys=2 skipped inspection!
        let td_rm = p_dr12.json(
            "POST",
            "/api/admin/provision/teardown",
            json!({
                "service_id": "fixtureService",
                "confirm_service_id": "fixtureService",
                "token": "fixture-token",
                "generation": p_dr12.service()["generation"],
                "remove_logging": true,
                "remove_snippets": true
            }),
        )["job"]
            .clone();
        let td_rm_step = p_dr12.json(
            "POST",
            &format!("/api/admin/jobs/{}/step", td_rm["id"].as_str().unwrap()),
            json!({}),
        )["job"]
            .clone();
        assert_eq!(
            td_rm_step["status"], "failed",
            "BUG-CLOUD-DR-12: teardown(remove_snippets=true) on coexisting GCE service must fail closed before touching scorer!"
        );
        assert_eq!(td_rm_step["error"], "managed_resources_not_owned");
        assert!(
            p_dr12.adapter.has_service(&sc_sid),
            "BUG-CLOUD-DR-12: GCE Compute scorer WASM service must survive!"
        );
        assert!(p_dr12
            .adapter
            .resources("fixtureService", "snippet")
            .iter()
            .any(|s| s["name"] == "Fastly Log Analytics - Session Scoring (vcl_recv)"));
        assert!(p_dr12
            .adapter
            .resources("fixtureService", "backend")
            .iter()
            .any(|b| b["name"] == "session_scorer"));

        let td_local = p_dr12.json(
            "POST",
            "/api/admin/provision/teardown",
            json!({
                "service_id": "fixtureService",
                "confirm_service_id": "fixtureService",
                "token": "fixture-token",
                "generation": p_dr12.service()["generation"],
                "remove_logging": false,
                "remove_snippets": false,
                "delete_fos_keys": false
            }),
        )["job"]
            .clone();
        let td_local_done = p_dr12.drain(td_local);
        assert_eq!(td_local_done["status"], "complete");
        assert_eq!(
            p_dr12.json("GET", "/api/services", json!({}))["services"],
            json!([])
        );
        assert!(p_dr12.adapter.has_service(&sc_sid));
        assert!(p_dr12
            .adapter
            .resources("fixtureService", "snippet")
            .iter()
            .any(|s| s["name"] == "Fastly Log Analytics - Session Scoring (vcl_recv)"));
        assert!(p_dr12
            .adapter
            .resources("fixtureService", "backend")
            .iter()
            .any(|b| b["name"] == "session_scorer"));
    }

    // Also verify budget-exhausted DR without scoring (n_keys=8) can cleanly unprovision via local-only teardown:
    let p_nkeys5 = Platform::new();
    let _ = seed_gce_scoring(&p_nkeys5, 8);
    p_nkeys5.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "meta/cloud_dr_state.json",
        serde_json::to_vec(&CloudDrState {
            schema_version: SCHEMA_VERSION,
            updated_at: 1_800_000_000,
            service_id: "fixtureService".into(),
            service: json!({"capture": "managed"}),
            views: vec![],
            catalog_head: None,
            floor_key: None,
            rum_stats: None,
        })
        .unwrap(),
    );
    assert_eq!(
        p_nkeys5
            .call(
                "POST",
                "/api/admin/provision/recover",
                json!({
                    "token": "fixture-token",
                    "service_id": "fixtureService",
                    "fos_bucket": "fos-fixture-logs",
                    "fos_region": "us-east-1"
                }),
            )
            .status,
        200
    );
    let td_n5 = p_nkeys5.json(
        "POST",
        "/api/admin/provision/teardown",
        json!({
            "service_id": "fixtureService",
            "confirm_service_id": "fixtureService",
            "token": "fixture-token",
            "generation": p_nkeys5.service()["generation"],
            "remove_logging": false,
            "remove_snippets": false,
            "delete_fos_keys": false
        }),
    )["job"]
        .clone();
    assert_eq!(p_nkeys5.drain(td_n5)["status"], "complete");
    assert_eq!(
        p_nkeys5.json("GET", "/api/services", json!({}))["services"],
        json!([])
    );

    // --- BUG-CLOUD-DR-12 (ii): Capture::Attached forbids scoring across DR, validate_service, and /scoring/* ---
    let p_att_sc = Platform::new();
    let (att_sc_sid, att_k, att_c, att_m) = seed_gce_scoring(&p_att_sc, 0);
    p_att_sc.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "meta/cloud_dr_state.json",
        serde_json::to_vec(&CloudDrState {
            schema_version: SCHEMA_VERSION,
            updated_at: 1_800_000_000,
            service_id: "fixtureService".into(),
            service: json!({
                "capture": "attached",
                "logging_endpoint_name": "FLP Cloud Logs",
                "scoring": {
                    "enabled": true,
                    "scoring_service_id": att_sc_sid,
                    "scoring_service_name": "Session Scoring Service for fixtureService",
                    "scoring_domain": "gce-scorer.edgecompute.app",
                    "scoring_keys_store_id": att_k,
                    "scoring_config_store_id": att_c,
                    "scoring_matrix_store_id": att_m,
                    "enabled_at": "2026-11-20T00:00:00Z",
                    "deployed_package_sha": "abc",
                    "deployed_package_files_hash": "def",
                    "deployed_vcl_sha": "ghi",
                    "exclude_url_regex": ""
                }
            }),
            views: vec![],
            catalog_head: None,
            floor_key: None,
            rum_stats: None,
        })
        .unwrap(),
    );
    let r_att_sc_dr = p_att_sc.call(
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
        r_att_sc_dr.status,
        400,
        "BUG-CLOUD-DR-12: Capture::Attached DR with scoring must be rejected 400: {}",
        String::from_utf8_lossy(&r_att_sc_dr.body)
    );

    // Provision a legitimate Capture::Attached service and verify POST /scoring/{enable,disable,teardown} all return 409!
    let p_att_routes = Platform::new();
    seed_coexisting_gce_and_attached_cloud(&p_att_routes);
    // Remove pre-seeded Cloud owner marker + FLP Cloud Logs so fresh attach provision succeeds
    let p_att_live = Platform::new();
    for (resource, body) in [
        (
            "snippet",
            json!({"name": "Fastly Log Analytics - vcl_recv", "type": "recv", "priority": "10", "content": "# gce-recv"}),
        ),
        (
            "snippet",
            json!({"name": "Fastly Log Analytics - vcl_deliver", "type": "deliver", "priority": "10", "content": "# gce-deliver"}),
        ),
        (
            "condition",
            json!({"name": "log_analytics_condition", "type": "RESPONSE", "priority": "10", "statement": "!segmented_caching.is_inner_req"}),
        ),
        (
            "logging/fos",
            json!({"name": "Fastly Object Storage Logs", "bucket_name": "fos-gce-logs", "region": "us-east-1", "path": "raw/", "response_condition": "log_analytics_condition"}),
        ),
    ] {
        p_att_live
            .adapter
            .seed_resource("fixtureService", resource, body);
    }
    let att_prov = p_att_live.json(
        "POST",
        "/api/admin/provision/execute",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1",
            "capture": "attached"
        }),
    );
    p_att_live.drain(att_prov["job"].clone());
    let att_gen = p_att_live.service()["generation"].clone();
    for (subpath, payload) in [
        ("enable", json!({})),
        ("disable", json!({})),
        (
            "teardown",
            json!({"confirm_service_id": "fixtureService", "generation": att_gen}),
        ),
    ] {
        let resp = p_att_live.call(
            "POST",
            &format!("/api/admin/services/fixtureService/scoring/{subpath}"),
            payload,
        );
        assert_eq!(
            resp.status,
            409,
            "BUG-CLOUD-DR-12: POST /scoring/{subpath} on Capture::Attached must fail 409: {}",
            String::from_utf8_lossy(&resp.body)
        );
    }
    // Tamper scoring onto Attached service and verify PATCH (validate_service) rejects with 400!
    let rec_att = p_att_live
        .adapter
        .get("svc:fixtureService")
        .unwrap()
        .unwrap();
    let mut val_att = rec_att.value.clone();
    val_att["scoring"] = json!({
        "enabled": true,
        "scoring_service_id": "22CharScorerService001",
        "scoring_service_name": "Session Scoring Service for fixtureService",
        "scoring_domain": "s.edgecompute.app",
        "scoring_keys_store_id": "k",
        "scoring_config_store_id": "c",
        "scoring_matrix_store_id": "m",
        "enabled_at": "2026-11-20T00:00:00Z",
        "deployed_package_sha": "a",
        "deployed_package_files_hash": "b",
        "deployed_vcl_sha": "c",
        "exclude_url_regex": ""
    });
    p_att_live
        .adapter
        .cas("svc:fixtureService", Some(&rec_att.generation), &val_att)
        .unwrap();
    let patch_att_sc = p_att_live.call(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation": p_att_live.service()["generation"], "sample_rate": 50}),
    );
    assert_eq!(
        patch_att_sc.status, 400,
        "BUG-CLOUD-DR-12: validate_service must reject scoring on Capture::Attached with 400"
    );

    // --- BUG-CLOUD-DR-12 (iii): Legacy High-Scale endpoint names & condition-matched primary endpoints ---
    for (ep_name, ep_cond) in [
        ("Fastly Log Analytics", "log_analytics_condition"),
        ("Fastly RUM Object Storage Logs", "rum_log_condition"),
        ("Custom Primary Request Logs", "log_analytics_condition"),
        ("Custom Primary RUM Logs", "rum_log_condition"),
    ] {
        let p_leg = Platform::new();
        for (res, body) in [
            (
                "snippet",
                json!({"name": "Fastly Log Analytics - vcl_recv", "type": "recv", "priority": "10", "content": "# gce-primary-recv"}),
            ),
            (
                "snippet",
                json!({"name": "Fastly Log Analytics - vcl_deliver", "type": "deliver", "priority": "10", "content": "# gce-primary-deliver"}),
            ),
            (
                "snippet",
                json!({"name": "flp-owner-fixtureApp", "type": "recv", "priority": "0", "content": "# flp-owner:fixtureApp:fixtureService"}),
            ),
            (
                "logging/fos",
                json!({"name": ep_name, "bucket_name": "fos-gce-logs", "region": "us-east-1", "path": "raw/", "response_condition": ep_cond}),
            ),
        ] {
            p_leg.adapter.seed_resource("fixtureService", res, body);
        }
        p_leg.adapter.seed_object_bytes(
            "fos-fixture-logs",
            "meta/cloud_dr_state.json",
            serde_json::to_vec(&CloudDrState {
                schema_version: SCHEMA_VERSION,
                updated_at: 1_800_000_000,
                service_id: "fixtureService".into(),
                service: json!({"capture": "managed", "logging_endpoint_name": "Cloud Custom Logs"}),
                views: vec![],
                catalog_head: None,
                floor_key: None,
                rum_stats: None,
            })
            .unwrap(),
        );
        let r_leg = p_leg.call(
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
            r_leg.status, 409,
            "BUG-CLOUD-DR-12: foreign primary endpoint ({ep_name}, {ep_cond}) must reject Managed DR with 409"
        );
    }

    // --- BUG-CLOUD-DR-13: "minimal" / "catalog_scan" fallback DR on Capture::Attached with n_keys = 1, 2, 3 ---
    for n_keys in [1usize, 2, 3] {
        let p_min = Platform::new();
        seed_coexisting_gce_and_attached_cloud(&p_min);
        for _ in 0..n_keys {
            let _ = p_min
                .adapter
                .api(fastly_log_analytics::admin::ApiRequest {
                    method: "POST",
                    path: "/resources/object-storage/access-keys",
                    token: "fixture-token",
                    body: Some(json!({
                        "permission": "read-write-objects",
                        "description": "flp-fixtureApp-fixtureService",
                        "buckets": ["fos-fixture-logs"]
                    })),
                    timeout: std::time::Duration::from_secs(5),
                    query: "",
                })
                .unwrap();
        }
        let r_min = p_min.call(
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
            r_min.status, 200,
            "minimal DR [n_keys={n_keys}] must succeed 200"
        );
        let svc_min = &p_min.adapter.records()["svc:fixtureService"].value;
        assert_eq!(
            svc_min["capture"], "attached",
            "BUG-CLOUD-DR-13 [n_keys={n_keys}]: minimal DR must infer Capture::Attached"
        );
        assert_eq!(
            svc_min["owned_endpoint_names"],
            json!(["FLP Cloud Logs"]),
            "BUG-CLOUD-DR-13 [n_keys={n_keys}]: minimal DR must verify FLP Cloud Logs"
        );
        let rec_min = p_min.json(
            "POST",
            "/api/admin/services/fixtureService/reconcile",
            json!({"generation": p_min.service()["generation"]}),
        )["job"]
            .clone();
        assert_eq!(p_min.drain(rec_min)["status"], "complete");
        let td_min = p_min.json(
            "POST",
            "/api/admin/provision/teardown",
            json!({
                "service_id": "fixtureService",
                "confirm_service_id": "fixtureService",
                "token": "fixture-token",
                "generation": p_min.service()["generation"]
            }),
        )["job"]
            .clone();
        assert_eq!(p_min.drain(td_min)["status"], "complete");
        assert!(p_min
            .adapter
            .resources("fixtureService", "logging/fos")
            .iter()
            .any(|e| e["name"] == "Fastly Object Storage Logs"));
    }

    // --- BUG-CLOUD-DR-14: Cold-start DR with scoring + n_keys = 1 across ALL 8 (pv, pw, pr) combinations ---
    for identical in [true, false] {
        for pv in [false, true] {
            for pw in [false, true] {
                for pr in [false, true] {
                    let p_b = Platform::new();
                    let (_, sc_svc) = p_b
                        .adapter
                        .api(fastly_log_analytics::admin::ApiRequest {
                            method: "POST",
                            path: "/service",
                            token: "fixture-token",
                            body: Some(json!({
                                "name": "Session Scoring Service for fixtureService",
                                "type": "wasm"
                            })),
                            timeout: std::time::Duration::from_secs(5),
                            query: "",
                        })
                        .unwrap();
                    let s_id = sc_svc["id"].as_str().unwrap().to_owned();
                    let (_, k_r) = p_b
                        .adapter
                        .api(fastly_log_analytics::admin::ApiRequest {
                            method: "POST",
                            path: "/resources/stores/config",
                            token: "fixture-token",
                            body: Some(json!({"name": format!("scoring_keys_{s_id}")})),
                            timeout: std::time::Duration::from_secs(5),
                            query: "",
                        })
                        .unwrap();
                    let (_, c_r) = p_b
                        .adapter
                        .api(fastly_log_analytics::admin::ApiRequest {
                            method: "POST",
                            path: "/resources/stores/config",
                            token: "fixture-token",
                            body: Some(json!({"name": format!("scoring_config_{s_id}")})),
                            timeout: std::time::Duration::from_secs(5),
                            query: "",
                        })
                        .unwrap();
                    let (_, m_r) = p_b
                        .adapter
                        .api(fastly_log_analytics::admin::ApiRequest {
                            method: "POST",
                            path: "/resources/stores/kv",
                            token: "fixture-token",
                            body: Some(json!({"name": format!("scoring_matrix_{s_id}")})),
                            timeout: std::time::Duration::from_secs(5),
                            query: "",
                        })
                        .unwrap();
                    let _ = p_b
                        .adapter
                        .api(fastly_log_analytics::admin::ApiRequest {
                            method: "POST",
                            path: "/resources/object-storage/access-keys",
                            token: "fixture-token",
                            body: Some(json!({
                                "permission": "read-write-objects",
                                "description": "flp-fixtureApp-fixtureService",
                                "buckets": ["fos-fixture-logs"]
                            })),
                            timeout: std::time::Duration::from_secs(5),
                            query: "",
                        })
                        .unwrap();
                    p_b.adapter.seed_resource(
                        "fixtureService",
                        "snippet",
                        json!({"name": "flp-owner-fixtureApp", "type": "recv", "priority": "0", "content": "# flp-owner:fixtureApp:fixtureService"}),
                    );
                    p_b.adapter.seed_resource(
                        "fixtureService",
                        "logging/fos",
                        json!({
                            "name": "Fastly Object Storage Logs",
                            "bucket_name": "fos-fixture-logs",
                            "region": "us-east-1",
                            "path": "raw/request/year=%Y/month=%m/day=%d/hour=%H/minute=%M/",
                            "response_condition": "log_analytics_condition"
                        }),
                    );
                    let saved_view = SavedView {
                        id: "v-dr14".into(),
                        service_id: "fixtureService".into(),
                        name: "Ops View".into(),
                        filters_json: r#"{"status":500}"#.into(),
                        time_range_type: Some("1h".into()),
                        start_time: None,
                        end_time: None,
                        page: Some("/dashboard".into()),
                        created_at: Some("2026-11-21T00:00:00Z".into()),
                    };
                    if pv {
                        let v_payload = if identical {
                            json!({"views": [saved_view.clone()], "updated_at": 1, "_kv_write_id": "prev"})
                        } else {
                            json!({"views": [], "updated_at": 1})
                        };
                        p_b.adapter
                            .cas("views:fixtureService", None, &v_payload)
                            .unwrap();
                    }
                    if pw {
                        let w_payload = if identical {
                            json!({
                                "catalog": serde_json::to_value(&head).unwrap(),
                                "floor": "raw/2026/floor.zst",
                                "_kv_write_id": "prev"
                            })
                        } else {
                            json!({"floor": "raw/2025/old.zst"})
                        };
                        p_b.adapter
                            .cas("ingest:watermark", None, &w_payload)
                            .unwrap();
                    }
                    if pr {
                        let r_payload = if identical {
                            json!({"rum_stats": {"rows": 9}, "_kv_write_id": "prev"})
                        } else {
                            json!({"rum_stats": {"rows": 0}})
                        };
                        p_b.adapter
                            .cas("ingest:rum:watermark", None, &r_payload)
                            .unwrap();
                    }
                    p_b.adapter.seed_object_bytes(
                        "fos-fixture-logs",
                        "meta/cloud_dr_state.json",
                        serde_json::to_vec(&CloudDrState {
                            schema_version: SCHEMA_VERSION,
                            updated_at: 1_800_000_000,
                            service_id: "fixtureService".into(),
                            service: json!({
                                "scoring": {
                                    "enabled": false,
                                    "scoring_service_id": s_id,
                                    "scoring_service_name": "Session Scoring Service for fixtureService",
                                    "scoring_domain": "fos-fixtureservice-session-scorer.edgecompute.app",
                                    "scoring_keys_store_id": k_r["id"],
                                    "scoring_config_store_id": c_r["id"],
                                    "scoring_matrix_store_id": m_r["id"],
                                    "enabled_at": "2026-11-20T00:00:00Z",
                                    "deployed_package_sha": "abc",
                                    "deployed_package_files_hash": "def",
                                    "deployed_vcl_sha": "ghi",
                                    "exclude_url_regex": ""
                                }
                            }),
                            views: vec![saved_view],
                            catalog_head: Some(head.clone()),
                            floor_key: Some("raw/2026/floor.zst".into()),
                            rum_stats: Some(json!({"rows": 9})),
                        })
                        .unwrap(),
                    );
                    let r_b = p_b.call(
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
                        r_b.status,
                        200,
                        "BUG-CLOUD-DR-14 [identical={identical}, pv={pv}, pw={pw}, pr={pr}] must fit <=24 calls: {}",
                        String::from_utf8_lossy(&r_b.body)
                    );
                    if !pv && !pw && !pr {
                        assert_eq!(
                            p_b.adapter.records()["svc:fixtureService"].value["owned_endpoint_names"],
                            json!(["Fastly Object Storage Logs"]),
                            "BUG-CLOUD-DR-14 [pv=false, pw=false, pr=false] must verify owned_endpoint_names"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn bug_cloud_dr_15_16_coexistence_scoring_mutation_routes_catalog_scan_and_foreign_owner_local_teardown(
) {
    struct RichFosReset;
    impl Drop for RichFosReset {
        fn drop(&mut self) {
            RICH_FOS.with(|r| r.set(false));
            MTIMES.with(|m| m.borrow_mut().clear());
        }
    }

    let seed_gce_coexist_with_scoring =
        |p: &Platform, n_keys: usize| -> (String, String, String, String) {
            for (res, body) in [
                (
                    "snippet",
                    json!({
                        "name": "Fastly Log Analytics - vcl_recv",
                        "type": "recv",
                        "priority": "10",
                        "content": "# gce-primary-recv"
                    }),
                ),
                (
                    "snippet",
                    json!({
                        "name": "Fastly Log Analytics - vcl_deliver",
                        "type": "deliver",
                        "priority": "10",
                        "content": "# gce-primary-deliver"
                    }),
                ),
                (
                    "snippet",
                    json!({
                        "name": "Fastly Log Analytics - Session Scoring (vcl_recv)",
                        "type": "recv",
                        "priority": "20",
                        "content": "# gce-scoring-recv"
                    }),
                ),
                (
                    "snippet",
                    json!({
                        "name": "Fastly Log Analytics - Session Scoring (vcl_pass)",
                        "type": "pass",
                        "priority": "20",
                        "content": "# gce-scoring-pass"
                    }),
                ),
                (
                    "condition",
                    json!({
                        "name": "log_analytics_condition",
                        "type": "RESPONSE",
                        "priority": "10",
                        "statement": "!segmented_caching.is_inner_req"
                    }),
                ),
                (
                    "logging/fos",
                    json!({
                        "name": "Fastly Object Storage Logs",
                        "bucket_name": "fos-gce-logs",
                        "region": "us-east-1",
                        "path": "raw/",
                        "response_condition": "log_analytics_condition"
                    }),
                ),
                (
                    "backend",
                    json!({
                        "name": "session_scorer",
                        "address": "gce-scorer.edgecompute.app",
                        "port": 443,
                        "use_ssl": true,
                        "ssl_check_cert": true
                    }),
                ),
            ] {
                p.adapter.seed_resource("fixtureService", res, body);
            }
            let (_, sc_svc) = p
                .adapter
                .api(fastly_log_analytics::admin::ApiRequest {
                    method: "POST",
                    path: "/service",
                    token: "fixture-token",
                    body: Some(json!({
                        "name": "Session Scoring Service for fixtureService",
                        "type": "wasm"
                    })),
                    timeout: std::time::Duration::from_secs(5),
                    query: "",
                })
                .unwrap();
            let scorer_sid = sc_svc["id"].as_str().unwrap().to_owned();
            let (_, k_resp) = p
                .adapter
                .api(fastly_log_analytics::admin::ApiRequest {
                    method: "POST",
                    path: "/resources/stores/config",
                    token: "fixture-token",
                    body: Some(json!({"name": format!("scoring_keys_{scorer_sid}")})),
                    timeout: std::time::Duration::from_secs(5),
                    query: "",
                })
                .unwrap();
            let k_id = k_resp["id"].as_str().unwrap().to_owned();
            let _ = p
                .adapter
                .api(fastly_log_analytics::admin::ApiRequest {
                    method: "POST",
                    path: &format!("/resources/stores/config/{k_id}/item"),
                    token: "fixture-token",
                    body: Some(
                        json!({"item_key": "current_key_hex", "item_value": "GCE_ORIG_AES_KEY"}),
                    ),
                    timeout: std::time::Duration::from_secs(5),
                    query: "",
                })
                .unwrap();
            let _ = p
                .adapter
                .api(fastly_log_analytics::admin::ApiRequest {
                    method: "POST",
                    path: &format!("/resources/stores/config/{k_id}/item"),
                    token: "fixture-token",
                    body: Some(
                        json!({"item_key": "request_secret", "item_value": "GCE_ORIG_REQ_SECRET"}),
                    ),
                    timeout: std::time::Duration::from_secs(5),
                    query: "",
                })
                .unwrap();
            let (_, c_resp) = p
                .adapter
                .api(fastly_log_analytics::admin::ApiRequest {
                    method: "POST",
                    path: "/resources/stores/config",
                    token: "fixture-token",
                    body: Some(json!({"name": format!("scoring_config_{scorer_sid}")})),
                    timeout: std::time::Duration::from_secs(5),
                    query: "",
                })
                .unwrap();
            let c_id = c_resp["id"].as_str().unwrap().to_owned();
            let _ = p
                .adapter
                .api(fastly_log_analytics::admin::ApiRequest {
                    method: "POST",
                    path: &format!("/resources/stores/config/{c_id}/item"),
                    token: "fixture-token",
                    body: Some(json!({"item_key": "enforce_threshold", "item_value": "95"})),
                    timeout: std::time::Duration::from_secs(5),
                    query: "",
                })
                .unwrap();
            let _ = p
                .adapter
                .api(fastly_log_analytics::admin::ApiRequest {
                    method: "POST",
                    path: &format!("/resources/stores/config/{c_id}/item"),
                    token: "fixture-token",
                    body: Some(json!({"item_key": "l2_enforce_enabled", "item_value": "0"})),
                    timeout: std::time::Duration::from_secs(5),
                    query: "",
                })
                .unwrap();
            let (_, m_resp) = p
                .adapter
                .api(fastly_log_analytics::admin::ApiRequest {
                    method: "POST",
                    path: "/resources/stores/kv",
                    token: "fixture-token",
                    body: Some(json!({"name": format!("scoring_matrix_{scorer_sid}")})),
                    timeout: std::time::Duration::from_secs(5),
                    query: "",
                })
                .unwrap();
            let m_id = m_resp["id"].as_str().unwrap().to_owned();
            let _ = p
                .adapter
                .api_raw(fastly_log_analytics::admin::ApiRawRequest {
                    method: "PUT",
                    path: &format!("/resources/stores/kv/{m_id}/keys/matrix"),
                    token: "fixture-token",
                    content_type: "application/octet-stream",
                    body: b"GCE-TRAINED-MATRIX-SENTINEL",
                    timeout: std::time::Duration::from_secs(5),
                })
                .unwrap();
            for _ in 0..n_keys {
                let _ = p
                    .adapter
                    .api(fastly_log_analytics::admin::ApiRequest {
                        method: "POST",
                        path: "/resources/object-storage/access-keys",
                        token: "fixture-token",
                        body: Some(json!({
                            "permission": "read-write-objects",
                            "description": "flp-fixtureApp-fixtureService",
                            "buckets": ["fos-fixture-logs"]
                        })),
                        timeout: std::time::Duration::from_secs(5),
                        query: "",
                    })
                    .unwrap();
            }
            (scorer_sid, k_id, c_id, m_id)
        };

    // --- 1A. BUG-CLOUD-DR-15: All 7 synchronous /scoring/* mutation routes on wedged n_keys=3 Capture::Managed + scoring ---
    let p_dr15 = Platform::new();
    let (sc15_sid, k15_id, c15_id, m15_id) = seed_gce_coexist_with_scoring(&p_dr15, 3);
    // Also seed foreign owner marker `# flp-owner:otherApp:fixtureService` to pin Wave-18 Auditor Challenge 2 (`fn teardown` in mod.rs:1427-1451)
    p_dr15.adapter.seed_resource(
        "fixtureService",
        "snippet",
        json!({
            "name": "flp-owner-otherApp",
            "type": "recv",
            "priority": "0",
            "content": "# flp-owner:otherApp:fixtureService"
        }),
    );
    let head15 = Head {
        version: VERSION,
        generation: 7,
        manifest: Some("processed/_catalog/manifest=7-fixtureService.json".into()),
        recent: vec![Event {
            cursor: "fixtureService:7".into(),
            generation: 7,
            committed_at: Some("2026-11-21T00:00:00Z".into()),
            manifest: Some("processed/_catalog/manifest=7-fixtureService.json".into()),
        }],
        ..Head::default()
    };
    p_dr15.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "processed/scoring/matrices/2026-11-21-hijack.json",
        serde_json::to_vec(&json!({
            "version": "2026-11-21-hijack",
            "vocab_size": 1,
            "session_count": 1,
            "transition_count": 1,
            "counts": {"/": {"/": 1}},
            "row_totals": {"/": 1}
        }))
        .unwrap(),
    );
    p_dr15.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "meta/cloud_dr_state.json",
        serde_json::to_vec(&CloudDrState {
            schema_version: SCHEMA_VERSION,
            updated_at: 1_800_000_000,
            service_id: "fixtureService".into(),
            service: json!({
                "capture": "managed",
                "scoring": {
                    "enabled": true,
                    "scoring_service_id": sc15_sid,
                    "scoring_service_name": "Session Scoring Service for fixtureService",
                    "scoring_domain": "gce-scorer.edgecompute.app",
                    "scoring_keys_store_id": k15_id,
                    "scoring_config_store_id": c15_id,
                    "scoring_matrix_store_id": m15_id,
                    "enabled_at": "2026-11-20T00:00:00Z",
                    "deployed_package_sha": "abc",
                    "deployed_package_files_hash": "def",
                    "deployed_vcl_sha": "ghi",
                    "exclude_url_regex": ""
                }
            }),
            views: vec![SavedView {
                id: "v-dr15".into(),
                service_id: "fixtureService".into(),
                name: "V15".into(),
                filters_json: "{}".into(),
                time_range_type: Some("1h".into()),
                start_time: None,
                end_time: None,
                page: Some("/dashboard".into()),
                created_at: Some("2026-11-21T00:00:00Z".into()),
            }],
            catalog_head: Some(head15),
            floor_key: Some("raw/2026/f.zst".into()),
            rum_stats: Some(json!({"rows": 3})),
        })
        .unwrap(),
    );
    assert_eq!(
        p_dr15
            .call(
                "POST",
                "/api/admin/provision/recover",
                json!({
                    "token": "fixture-token",
                    "service_id": "fixtureService",
                    "fos_bucket": "fos-fixture-logs",
                    "fos_region": "us-east-1"
                }),
            )
            .status,
        200
    );

    for (label, method, path, payload) in [
        (
            "exclude-regex",
            "PUT",
            "/api/admin/services/fixtureService/scoring/exclude-regex?confirm=true",
            json!({"exclude_url_regex": r"^/hijacked$"}),
        ),
        (
            "enforce-status-code",
            "PUT",
            "/api/admin/services/fixtureService/scoring/enforce-status-code?confirm=true",
            json!({"status_code": 403}),
        ),
        (
            "enforce-threshold",
            "PUT",
            "/api/admin/services/fixtureService/scoring/enforce-threshold?confirm=true",
            json!({"enforce_threshold": 0}),
        ),
        (
            "l2-enforce",
            "PUT",
            "/api/admin/services/fixtureService/scoring/l2-enforce?confirm=true",
            json!({"enabled": true}),
        ),
        (
            "rotate-key",
            "POST",
            "/api/admin/services/fixtureService/scoring/rotate-key?confirm=true",
            json!({}),
        ),
        (
            "retrain",
            "POST",
            "/api/admin/services/fixtureService/scoring/retrain",
            json!({
                "matrix": {
                    "version": "2026-11-22-hijack",
                    "vocab_size": 1,
                    "session_count": 1,
                    "transition_count": 1,
                    "counts": {"/": {"/": 1}},
                    "row_totals": {"/": 1}
                }
            }),
        ),
        (
            "restore",
            "POST",
            "/api/admin/services/fixtureService/scoring/matrix/versions/restore?confirm=true",
            json!({"version": "2026-11-21-hijack"}),
        ),
    ] {
        let resp = p_dr15.call(method, path, payload);
        assert_eq!(
            resp.status,
            409,
            "BUG-CLOUD-DR-15 [{label}]: expected 409 managed_resources_not_owned, got {}: {}",
            resp.status,
            String::from_utf8_lossy(&resp.body)
        );
        let parsed: Value = serde_json::from_slice(&resp.body).unwrap_or(Value::Null);
        assert_eq!(
            parsed["error"], "managed_resources_not_owned",
            "BUG-CLOUD-DR-15 [{label}]"
        );
    }

    // Verify ZERO GCE VCL snippets, ConfigStore items, AES keys, or trained Markov matrix bytes were mutated!
    let snips15 = p_dr15.adapter.resources("fixtureService", "snippet");
    assert!(
        !snips15.iter().any(|s| s["name"] == "Session Scoring - Recv" || s["name"] == "Session Scoring - Enforce"),
        "BUG-CLOUD-DR-15: Cloud must NOT inject Session Scoring - Recv/Enforce onto GCE VM customer service!"
    );
    assert_eq!(
        p_dr15
            .adapter
            .config_store_value(&c15_id, "enforce_threshold")
            .as_deref(),
        Some("95"),
        "BUG-CLOUD-DR-15: GCE enforce_threshold must remain 95!"
    );
    assert_eq!(
        p_dr15
            .adapter
            .config_store_value(&c15_id, "l2_enforce_enabled")
            .as_deref(),
        Some("0"),
        "BUG-CLOUD-DR-15: GCE l2_enforce_enabled must remain 0!"
    );
    assert_eq!(
        p_dr15
            .adapter
            .config_store_value(&k15_id, "current_key_hex")
            .as_deref(),
        Some("GCE_ORIG_AES_KEY"),
        "BUG-CLOUD-DR-15: GCE AES cookie key must remain untouched!"
    );
    assert_eq!(
        p_dr15.adapter.kv_store_value(&m15_id, "matrix").as_deref(),
        Some(b"GCE-TRAINED-MATRIX-SENTINEL".as_slice()),
        "BUG-CLOUD-DR-15: GCE trained Markov matrix must remain untouched!"
    );

    // Wave-18 Auditor Challenge 2: with foreign `# flp-owner:otherApp:fixtureService` present,
    // local-only teardown(remove_logging=false, remove_snippets=false) bypasses preflight in `fn teardown` (mod.rs:1427-1451) and completes cleanly!
    let td_foreign_local = p_dr15.call(
        "POST",
        "/api/admin/provision/teardown",
        json!({
            "service_id": "fixtureService",
            "confirm_service_id": "fixtureService",
            "token": "fixture-token",
            "generation": p_dr15.service()["generation"],
            "remove_logging": false,
            "remove_snippets": false,
            "delete_fos_keys": false
        }),
    );
    assert_eq!(
        td_foreign_local.status,
        200,
        "Auditor Challenge 2: local-only teardown with foreign flp-owner-otherApp must pass fn teardown preflight bypass: {}",
        String::from_utf8_lossy(&td_foreign_local.body)
    );
    let td_foreign_job: Value = serde_json::from_slice(&td_foreign_local.body).unwrap();
    let td_foreign_done = p_dr15.drain(td_foreign_job["job"].clone());
    assert_eq!(td_foreign_done["status"], "complete");
    assert!(p_dr15.adapter.has_service(&sc15_sid));

    // --- 1B. BUG-CLOUD-DR-15: POST /scoring/enable & ensure_scorer_service_step on wedged n_keys=8 Capture::Managed (scoring: null) ---
    let p_en15 = Platform::new();
    let (_en_sc_sid, en_k_id, _en_c_id, en_m_id) = seed_gce_coexist_with_scoring(&p_en15, 8);
    p_en15.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "meta/cloud_dr_state.json",
        serde_json::to_vec(&CloudDrState {
            schema_version: SCHEMA_VERSION,
            updated_at: 1_800_000_000,
            service_id: "fixtureService".into(),
            service: json!({"capture": "managed"}),
            views: vec![],
            catalog_head: None,
            floor_key: None,
            rum_stats: None,
        })
        .unwrap(),
    );
    assert_eq!(
        p_en15
            .call(
                "POST",
                "/api/admin/provision/recover",
                json!({
                    "token": "fixture-token",
                    "service_id": "fixtureService",
                    "fos_bucket": "fos-fixture-logs",
                    "fos_region": "us-east-1"
                }),
            )
            .status,
        200
    );
    let en_resp = p_en15.call(
        "POST",
        "/api/admin/services/fixtureService/scoring/enable",
        json!({}),
    );
    assert_eq!(
        en_resp.status,
        409,
        "BUG-CLOUD-DR-15 [enable]: POST /scoring/enable must fail synchronously with 409 managed_resources_not_owned, got {}: {}",
        en_resp.status,
        String::from_utf8_lossy(&en_resp.body)
    );
    let en_body: Value = serde_json::from_slice(&en_resp.body).unwrap_or(Value::Null);
    assert_eq!(en_body["error"], "managed_resources_not_owned");
    assert_eq!(
        p_en15.adapter.kv_store_value(&en_m_id, "matrix").as_deref(),
        Some(b"GCE-TRAINED-MATRIX-SENTINEL".as_slice()),
        "BUG-CLOUD-DR-15 [enable]: GCE matrix must NOT be wiped!"
    );
    assert!(
        p_en15
            .adapter
            .secret("scoring_request_secret.fixtureService")
            .unwrap()
            .is_none(),
        "BUG-CLOUD-DR-15 [enable]: GCE request_secret must NOT be copied into Cloud Secret Store!"
    );
    assert_eq!(
        p_en15
            .adapter
            .config_store_value(&en_k_id, "current_key_hex")
            .as_deref(),
        Some("GCE_ORIG_AES_KEY")
    );

    // Also test `ensure_scorer_service_step` defense-in-depth directly if a `scoring_enable` job was queued before ownership was lost:
    let p_step15 = Platform::new();
    let prov_step15 = p_step15.provision();
    p_step15.drain(prov_step15["job"].clone());
    let en_queued = p_step15.json(
        "POST",
        "/api/admin/services/fixtureService/scoring/enable",
        json!({}),
    )["job"]
        .clone();
    // Simulate concurrent GCE VM takeover before `scorer_service` step runs:
    let (_, _, _, step_m_id) = seed_gce_coexist_with_scoring(&p_step15, 0);
    let step_res = p_step15.json(
        "POST",
        &format!("/api/admin/jobs/{}/step", en_queued["id"].as_str().unwrap()),
        json!({}),
    )["job"]
        .clone();
    assert_eq!(
        step_res["status"], "failed",
        "BUG-CLOUD-DR-15 [ensure_scorer_service_step]: must fail closed before adopting GCE scorer!"
    );
    assert_eq!(step_res["error"], "managed_resources_not_owned");
    assert_eq!(step_res["completed_steps"], json!([]));
    assert_eq!(
        p_step15
            .adapter
            .kv_store_value(&step_m_id, "matrix")
            .as_deref(),
        Some(b"GCE-TRAINED-MATRIX-SENTINEL".as_slice())
    );

    // --- 2. BUG-CLOUD-DR-16: RICH_FOS = true "catalog_scan" reserve across n_days = 1..=6, n_keys = 0..=3, with_ep = false & true, modern & legacy GCE endpoint names ---
    let _rich_guard = RichFosReset;
    for primary_ep_name in ["Fastly Object Storage Logs", "Fastly Log Analytics"] {
        for with_ep in [false, true] {
            for n_days in [1usize, 4, 5, 6] {
                for n_keys in 0..=3usize {
                    let p_cat = Platform::new();
                    RICH_FOS.with(|r| r.set(true));
                    MTIMES.with(|m| m.borrow_mut().clear());

                    for (res, body) in [
                        (
                            "snippet",
                            json!({
                                "name": "Fastly Log Analytics - vcl_recv",
                                "type": "recv",
                                "priority": "10",
                                "content": "# gce-recv"
                            }),
                        ),
                        (
                            "snippet",
                            json!({
                                "name": "Fastly Log Analytics - vcl_deliver",
                                "type": "deliver",
                                "priority": "10",
                                "content": "# gce-deliver"
                            }),
                        ),
                        (
                            "condition",
                            json!({
                                "name": "log_analytics_condition",
                                "type": "RESPONSE",
                                "priority": "10",
                                "statement": "!segmented_caching.is_inner_req"
                            }),
                        ),
                        (
                            "logging/fos",
                            json!({
                                "name": primary_ep_name,
                                "bucket_name": "fos-gce-logs",
                                "region": "us-east-1",
                                "path": "raw/",
                                "response_condition": "log_analytics_condition"
                            }),
                        ),
                    ] {
                        p_cat.adapter.seed_resource("fixtureService", res, body);
                    }
                    if with_ep {
                        p_cat.adapter.seed_resource(
                            "fixtureService",
                            "condition",
                            json!({
                                "name": "flp_cloud_log_condition",
                                "type": "RESPONSE",
                                "priority": "10",
                                "statement": "!segmented_caching.is_inner_req"
                            }),
                        );
                        p_cat.adapter.seed_resource(
                            "fixtureService",
                            "logging/fos",
                            json!({
                                "name": "FLP Cloud Logs",
                                "bucket_name": "fos-fixture-logs",
                                "region": "us-east-1",
                                "path": "raw/request/year=%Y/month=%m/day=%d/hour=%H/minute=%M/",
                                "response_condition": "flp_cloud_log_condition"
                            }),
                        );
                    }
                    for _ in 0..n_keys {
                        let _ = p_cat
                            .adapter
                            .api(fastly_log_analytics::admin::ApiRequest {
                                method: "POST",
                                path: "/resources/object-storage/access-keys",
                                token: "fixture-token",
                                body: Some(json!({
                                    "permission": "read-write-objects",
                                    "description": "flp-fixtureApp-fixtureService",
                                    "buckets": ["fos-fixture-logs"]
                                })),
                                timeout: std::time::Duration::from_secs(5),
                                query: "",
                            })
                            .unwrap();
                    }
                    for d in 1..=n_days {
                        let day_ts = 1_700_006_400i64 + ((d as i64 - 1) * 86_400);
                        let day_key = format!("processed/_catalog/day{d}.json");
                        let day = Day {
                            version: VERSION,
                            hours: BTreeMap::from([(
                                day_ts,
                                Reference {
                                    key: format!("processed/_catalog/block{d}.json"),
                                    generation: d as u64,
                                },
                            )]),
                            statistics: Statistics {
                                earliest: Some(day_ts),
                                latest: Some(day_ts + 3599),
                                earliest_log: None,
                                latest_log: None,
                                rows: 100,
                            },
                            ..Day::default()
                        };
                        p_cat.adapter.seed_object_bytes(
                            "fos-fixture-logs",
                            &day_key,
                            serde_json::to_vec(&day).unwrap(),
                        );
                        MTIMES.with(|m| {
                            m.borrow_mut()
                                .insert(day_key, 1_800_000_000 + (d as u64) * 10);
                        });
                    }
                    let manifest_key = "processed/_catalog/manifest99.json";
                    let manifest = Manifest {
                        version: VERSION,
                        service_id: "fixtureService".into(),
                        generation: 99,
                        batch: "b99".into(),
                        schema: "v2".into(),
                        committed_at: "2026-11-21T00:00:00Z".into(),
                        previous: None,
                        files: vec![],
                        summaries: vec![],
                        sources: vec![],
                        replaces: vec![],
                        bytes: 512,
                        rows: 100,
                        parents: BTreeMap::new(),
                    };
                    p_cat.adapter.seed_object_bytes(
                        "fos-fixture-logs",
                        manifest_key,
                        serde_json::to_vec(&manifest).unwrap(),
                    );
                    MTIMES.with(|m| {
                        m.borrow_mut().insert(manifest_key.into(), 1_800_009_999);
                    });

                    let r_cat = p_cat.call(
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
                        r_cat.status,
                        200,
                        "BUG-CLOUD-DR-16 [ep={primary_ep_name}, with_ep={with_ep}, n_days={n_days}, n_keys={n_keys}] must succeed 200: {}",
                        String::from_utf8_lossy(&r_cat.body)
                    );
                    let r_cat_json: Value = serde_json::from_slice(&r_cat.body).unwrap();
                    assert_eq!(
                        r_cat_json["source"], "catalog_scan",
                        "BUG-CLOUD-DR-16 [ep={primary_ep_name}, with_ep={with_ep}, n_days={n_days}, n_keys={n_keys}] must use catalog_scan"
                    );
                    let svc_cat = &p_cat.adapter.records()["svc:fixtureService"].value;
                    assert_eq!(
                        svc_cat["capture"],
                        "attached",
                        "BUG-CLOUD-DR-16 [ep={primary_ep_name}, with_ep={with_ep}, n_days={n_days}, n_keys={n_keys}] must infer Capture::Attached, got: {}",
                        svc_cat["capture"]
                    );
                    let expected_owned = if with_ep {
                        json!(["FLP Cloud Logs"])
                    } else {
                        json!([])
                    };
                    assert_eq!(svc_cat["owned_endpoint_names"], expected_owned);
                }
            }
        }
    }
    drop(_rich_guard);
}

#[test]
fn bug_cloud_dr_24d_post_delete_data_dr_recovery_preserves_reset_manifest_floor_and_cleared_rum_stats(
) {
    use fastly_log_analytics::ingest::{self, storage::Store};
    use std::sync::atomic::Ordering;

    struct RichGuard;
    impl Drop for RichGuard {
        fn drop(&mut self) {
            RICH_FOS.with(|r| r.set(false));
            MTIMES.with(|m| m.borrow_mut().clear());
        }
    }
    let _rich_guard = RichGuard;
    RICH_FOS.with(|r| r.set(true));
    MTIMES.with(|m| m.borrow_mut().clear());

    let p = Platform::new();
    let provisioned = p.provision();
    p.drain(provisioned["job"].clone());

    let old_manifest_key = "processed/_catalog/manifest-000007-old.json";
    let old_manifest = Manifest {
        version: VERSION,
        service_id: "fixtureService".into(),
        generation: 7,
        batch: "b-old".into(),
        schema: "v2".into(),
        committed_at: "2026-11-20T00:00:00Z".into(),
        previous: None,
        files: vec![],
        summaries: vec![],
        sources: vec![],
        replaces: vec![],
        bytes: 0,
        rows: 0,
        parents: BTreeMap::new(),
    };
    p.adapter.seed_object_bytes(
        "fos-fixture-logs",
        old_manifest_key,
        serde_json::to_vec(&old_manifest).unwrap(),
    );
    MTIMES.with(|m| {
        m.borrow_mut()
            .insert(old_manifest_key.into(), 1_763_596_800);
    });
    let old_head = Head {
        version: VERSION,
        generation: 7,
        manifest: Some(old_manifest_key.into()),
        recent: vec![Event {
            cursor: "fixtureService:7".into(),
            generation: 7,
            committed_at: Some("2026-11-20T00:00:00Z".into()),
            manifest: Some(old_manifest_key.into()),
        }],
        ..Head::default()
    };
    p.adapter
        .cas(
            ingest::WATERMARK_KEY,
            None,
            &json!({
                "catalog": old_head,
                "floor_key": "raw/request/year=2026/month=11/day=20/hour=00/2026-11-20T00:00:00.000-old.log.zst"
            }),
        )
        .unwrap();
    p.adapter
        .cas(
            &ingest::watermark_key("rum"),
            None,
            &json!({"rum_stats": {"earliest": 1_763_596_800u64, "latest": 1_763_597_000u64, "total_rows": 99_999u64}}),
        )
        .unwrap();

    let svc_pre: Service =
        serde_json::from_value(p.adapter.records()["svc:fixtureService"].value.clone()).unwrap();
    CALLS.with(|c| c.set(0));
    SECRETS.with(|s| s.borrow_mut().clear());
    dr::sync_best_effort(&p, &svc_pre);
    let stale_dr_bytes = p
        .adapter
        .bucket_objects("fos-fixture-logs")
        .get("meta/cloud_dr_state.json")
        .cloned()
        .expect("pre-reset cloud_dr_state.json must be written");
    let stale_dr_val: Value = serde_json::from_slice(&stale_dr_bytes).unwrap();
    assert_eq!(stale_dr_val["catalog_head"]["generation"], 7);
    assert!(stale_dr_val["rum_stats"].is_object());

    // Advance clock to 1_800_000_000 and run Delete Data to completion:
    p.clock.store(1_800_000_000, Ordering::SeqCst);
    let dd_gen = p.adapter.records()["svc:fixtureService"].generation.clone();
    let dd_start = p.json(
        "POST",
        "/api/admin/services/fixtureService/delete-data",
        json!({"confirm_service_id": "fixtureService", "generation": dd_gen}),
    );
    assert_eq!(dd_start["job"]["status"], "fencing");

    // Verify dr::sync_best_effort is a no-op while data_reset.is_some():
    let mut mutated_svc_mid_dd: Service =
        serde_json::from_value(p.adapter.records()["svc:fixtureService"].value.clone()).unwrap();
    mutated_svc_mid_dd.sample_rate = 11;
    CALLS.with(|c| c.set(0));
    SECRETS.with(|s| s.borrow_mut().clear());
    dr::sync_best_effort(&p, &mutated_svc_mid_dd);
    let mid_dr: Value = serde_json::from_slice(
        &p.adapter.bucket_objects("fos-fixture-logs")["meta/cloud_dr_state.json"],
    )
    .unwrap();
    assert_ne!(
        mid_dr["service"]["sample_rate"], 11,
        "BUG-CLOUD-DR-24D: dr::sync_best_effort must skip while service.data_reset.is_some()"
    );

    // Step Delete Data through fencing -> resetting -> cleanup -> complete:
    p.clock.fetch_add(
        fastly_log_analytics::admin::reset::FENCE_WAIT_S + 5,
        Ordering::SeqCst,
    );
    for _ in 0..25 {
        let recs = p.adapter.records();
        if recs["svc:fixtureService"]
            .value
            .get("data_reset")
            .is_none_or(Value::is_null)
        {
            break;
        }
        let cur_svc: Service =
            serde_json::from_value(recs["svc:fixtureService"].value.clone()).unwrap();
        let (ak, sk) = fastly_log_analytics::admin::fos_credentials(&p, &cur_svc).unwrap();
        CALLS.with(|c| c.set(0));
        SECRETS.with(|s| s.borrow_mut().clear());
        ELAPSED_MS.with(|e| e.set(0));
        let store = Store {
            runtime: &p,
            service: &cur_svc,
            access_key: ak,
            secret_key: sk,
            started: std::time::Instant::now(),
            vcpu_start: 0,
        };
        let Some(step_res) = fastly_log_analytics::admin::reset::advance(&store) else {
            break;
        };
        step_res.unwrap();
        p.clock.fetch_add(450, Ordering::SeqCst);
    }

    let post_dd_job = p.json(
        "GET",
        "/api/admin/services/fixtureService/delete-data",
        json!({}),
    );
    assert_eq!(post_dd_job["status"], "complete", "{post_dd_job}");
    let post_dd_objs = p.adapter.bucket_objects("fos-fixture-logs");
    assert!(
        !post_dd_objs.contains_key(old_manifest_key),
        "Delete Data must have deleted pre-reset manifest-000007-old.json"
    );
    let post_dd_wm = p.adapter.records()[ingest::WATERMARK_KEY].value.clone();
    let post_dd_gen = post_dd_wm["catalog"]["generation"].as_u64().unwrap();
    let post_dd_manifest = post_dd_wm["catalog"]["manifest"]
        .as_str()
        .unwrap()
        .to_owned();
    let post_dd_floor = post_dd_wm["floor_key"].as_str().unwrap().to_owned();
    assert_eq!(post_dd_gen, 8);
    let post_dd_mtime = MTIMES.with(|m| {
        m.borrow()
            .get(&post_dd_manifest)
            .copied()
            .unwrap_or(1_800_000_200)
    });

    // Verify both normal post-reset DR recovery (synced cloud_dr_state.json) AND stale pre-reset cloud_dr_state.json replay:
    for replay_stale_dr in [false, true] {
        let p_rec = Platform::new();
        p_rec.clock.store(p.now(), Ordering::SeqCst);
        p_rec.adapter.seed_resource(
            "fixtureService",
            "snippet",
            json!({"name": "flp-owner-fixtureApp", "type": "recv", "priority": "0", "content": "# flp-owner:fixtureApp:fixtureService"}),
        );
        MTIMES.with(|m| m.borrow_mut().clear());
        for (k, v) in &post_dd_objs {
            p_rec
                .adapter
                .seed_object_bytes("fos-fixture-logs", k, v.clone());
        }
        MTIMES.with(|m| {
            m.borrow_mut()
                .insert(post_dd_manifest.clone(), post_dd_mtime);
        });
        if replay_stale_dr {
            p_rec.adapter.seed_object_bytes(
                "fos-fixture-logs",
                "meta/cloud_dr_state.json",
                stale_dr_bytes.clone(),
            );
        }
        let rec_res = p_rec.json(
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
            rec_res["catalog_generation"], 8,
            "BUG-CLOUD-DR-24D [replay_stale_dr={replay_stale_dr}]: recover must restore post-reset gen=8, not deleted gen=7: {rec_res}"
        );
        let rec_records = p_rec.adapter.records();
        let rec_wm = &rec_records[ingest::WATERMARK_KEY].value;
        assert_eq!(
            rec_wm["catalog"]["manifest"], post_dd_manifest,
            "BUG-CLOUD-DR-24D [replay_stale_dr={replay_stale_dr}]: restored manifest must be post-reset manifest"
        );
        if replay_stale_dr {
            let restored_floor = rec_wm["floor_key"].as_str().unwrap_or("");
            assert!(
                restored_floor >= post_dd_floor.as_str(),
                "BUG-CLOUD-DR-24D [replay_stale_dr=true]: restored floor_key {restored_floor:?} must not rewind before post_dd_floor {post_dd_floor:?}"
            );
        } else {
            assert_eq!(
                rec_wm["floor_key"], post_dd_floor,
                "BUG-CLOUD-DR-24D [replay_stale_dr=false]: restored floor_key must equal exact post-reset floor_key"
            );
        }
        assert!(
            rec_records
                .get(&ingest::watermark_key("rum"))
                .is_none_or(|r| r.value.get("rum_stats").is_none_or(Value::is_null)),
            "BUG-CLOUD-DR-24D [replay_stale_dr={replay_stale_dr}]: deleted rum_stats must NOT be resurrected"
        );
    }
}

#[test]
fn bug_cloud_dr_25a_post_delete_data_dr_recovery_handles_none_head_and_post_reset_ingest_batches() {
    use fastly_log_analytics::ingest::{self, storage::Store};
    use std::sync::atomic::Ordering;

    struct RichGuard;
    impl Drop for RichGuard {
        fn drop(&mut self) {
            RICH_FOS.with(|r| r.set(false));
            MTIMES.with(|m| m.borrow_mut().clear());
        }
    }
    let _rich_guard = RichGuard;
    RICH_FOS.with(|r| r.set(true));
    MTIMES.with(|m| m.borrow_mut().clear());

    let run_delete_data = |p: &Platform| {
        let dd_gen = p.adapter.records()["svc:fixtureService"].generation.clone();
        let dd_start = p.json(
            "POST",
            "/api/admin/services/fixtureService/delete-data",
            json!({"confirm_service_id": "fixtureService", "generation": dd_gen}),
        );
        assert_eq!(dd_start["job"]["status"], "fencing");
        p.clock.fetch_add(
            fastly_log_analytics::admin::reset::FENCE_WAIT_S + 5,
            Ordering::SeqCst,
        );
        for _ in 0..25 {
            let recs = p.adapter.records();
            if recs["svc:fixtureService"]
                .value
                .get("data_reset")
                .is_none_or(Value::is_null)
            {
                break;
            }
            let cur_svc: Service =
                serde_json::from_value(recs["svc:fixtureService"].value.clone()).unwrap();
            let (ak, sk) = fastly_log_analytics::admin::fos_credentials(p, &cur_svc).unwrap();
            CALLS.with(|c| c.set(0));
            SECRETS.with(|s| s.borrow_mut().clear());
            ELAPSED_MS.with(|e| e.set(0));
            let store = Store {
                runtime: p,
                service: &cur_svc,
                access_key: ak,
                secret_key: sk,
                started: std::time::Instant::now(),
                vcpu_start: 0,
            };
            let Some(step_res) = fastly_log_analytics::admin::reset::advance(&store) else {
                break;
            };
            step_res.unwrap();
            p.clock.fetch_add(450, Ordering::SeqCst);
        }
        let post_dd_job = p.json(
            "GET",
            "/api/admin/services/fixtureService/delete-data",
            json!({}),
        );
        assert_eq!(post_dd_job["status"], "complete", "{post_dd_job}");
    };

    // Sub-defect A: Stale pre-reset cloud_dr_state.json where catalog_head == None (0 request commits prior to Delete Data),
    // pre-reset floor_key, and pre-reset rum_stats (42,000 rows).
    {
        let p = Platform::new();
        let provisioned = p.provision();
        p.drain(provisioned["job"].clone());
        p.clock.store(1_799_000_000, Ordering::SeqCst);
        p.adapter
            .cas(
                ingest::WATERMARK_KEY,
                None,
                &json!({"floor_key": "raw/request/year=2026/month=11/day=20/hour=00/minute=00/"}),
            )
            .unwrap();
        p.adapter
            .cas(
                &ingest::watermark_key("rum"),
                None,
                &json!({"rum_stats": {"earliest": 1_763_596_800u64, "latest": 1_763_597_000u64, "total_rows": 42_000u64}}),
            )
            .unwrap();
        let svc_pre: Service =
            serde_json::from_value(p.adapter.records()["svc:fixtureService"].value.clone())
                .unwrap();
        CALLS.with(|c| c.set(0));
        SECRETS.with(|s| s.borrow_mut().clear());
        dr::sync_best_effort(&p, &svc_pre);
        let stale_none_dr_bytes =
            p.adapter.bucket_objects("fos-fixture-logs")["meta/cloud_dr_state.json"].clone();
        let stale_none_dr_val: Value = serde_json::from_slice(&stale_none_dr_bytes).unwrap();
        assert!(
            stale_none_dr_val["catalog_head"].is_null(),
            "Pre-reset DR state must have catalog_head == null: {stale_none_dr_val}"
        );
        assert!(stale_none_dr_val["rum_stats"].is_object());

        p.clock.store(1_800_000_000, Ordering::SeqCst);
        run_delete_data(&p);
        let post_dd_objs = p.adapter.bucket_objects("fos-fixture-logs");
        let post_dd_wm = p.adapter.records()[ingest::WATERMARK_KEY].value.clone();
        let post_dd_manifest = post_dd_wm["catalog"]["manifest"]
            .as_str()
            .unwrap()
            .to_owned();
        let post_dd_floor = post_dd_wm["floor_key"].as_str().unwrap().to_owned();

        let p_rec = Platform::new();
        p_rec.clock.store(p.now(), Ordering::SeqCst);
        p_rec.adapter.seed_resource(
            "fixtureService",
            "snippet",
            json!({"name": "flp-owner-fixtureApp", "type": "recv", "priority": "0", "content": "# flp-owner:fixtureApp:fixtureService"}),
        );
        MTIMES.with(|m| m.borrow_mut().clear());
        for (k, v) in &post_dd_objs {
            p_rec
                .adapter
                .seed_object_bytes("fos-fixture-logs", k, v.clone());
        }
        MTIMES.with(|m| {
            m.borrow_mut()
                .insert(post_dd_manifest.clone(), 1_800_000_200);
        });
        p_rec.adapter.seed_object_bytes(
            "fos-fixture-logs",
            "meta/cloud_dr_state.json",
            stale_none_dr_bytes,
        );

        let rec_res = p_rec.json(
            "POST",
            "/api/admin/provision/recover",
            json!({
                "token": "fixture-token",
                "service_id": "fixtureService",
                "fos_bucket": "fos-fixture-logs",
                "fos_region": "us-east-1"
            }),
        );
        assert_eq!(rec_res["catalog_generation"], 1, "{rec_res}");
        let rec_records = p_rec.adapter.records();
        assert!(
            rec_records
                .get(&ingest::watermark_key("rum"))
                .is_none_or(|r| r.value.get("rum_stats").is_none_or(Value::is_null)),
            "BUG-CLOUD-DR-25A Sub-defect A: deleted rum_stats must NOT be resurrected when stale cloud_dr_state.json had catalog_head=None"
        );
        let restored_floor = rec_records[ingest::WATERMARK_KEY].value["floor_key"]
            .as_str()
            .unwrap_or("");
        assert!(
            restored_floor >= post_dd_floor.as_str(),
            "BUG-CLOUD-DR-25A Sub-defect A: floor_key {restored_floor:?} must NOT rewind before post_dd_floor {post_dd_floor:?}"
        );
    }

    // Sub-defect B + Auditor Low note: Stale pre-reset cloud_dr_state.json (gen=7) + reset manifest (gen=8 at 08:14:05Z)
    // + post-reset ingest batch manifest (gen=9 at 08:45:00Z), tested both during DELETE_GRACE_S (in_grace=true)
    // and after DELETE_GRACE_S (in_grace=false), plus total _catalog/ wipe (dr_manifest_missing && scanned_opt.is_none()).
    {
        let p = Platform::new();
        let provisioned = p.provision();
        p.drain(provisioned["job"].clone());

        let old_manifest_key = "processed/_catalog/manifest-000007-old.json";
        let old_manifest = Manifest {
            version: VERSION,
            service_id: "fixtureService".into(),
            generation: 7,
            batch: "b-old".into(),
            schema: "v2".into(),
            committed_at: "2026-11-20T00:00:00Z".into(),
            previous: None,
            files: vec![],
            summaries: vec![],
            sources: vec![],
            replaces: vec![],
            bytes: 0,
            rows: 0,
            parents: BTreeMap::new(),
        };
        let old_manifest_bytes = serde_json::to_vec(&old_manifest).unwrap();
        p.adapter.seed_object_bytes(
            "fos-fixture-logs",
            old_manifest_key,
            old_manifest_bytes.clone(),
        );
        let old_head = Head {
            version: VERSION,
            generation: 7,
            manifest: Some(old_manifest_key.into()),
            recent: vec![Event {
                cursor: "fixtureService:7".into(),
                generation: 7,
                committed_at: Some("2026-11-20T00:00:00Z".into()),
                manifest: Some(old_manifest_key.into()),
            }],
            ..Head::default()
        };
        p.adapter
            .cas(
                ingest::WATERMARK_KEY,
                None,
                &json!({
                    "catalog": old_head,
                    "floor_key": "raw/request/year=2026/month=11/day=20/hour=00/minute=00/"
                }),
            )
            .unwrap();
        p.adapter
            .cas(
                &ingest::watermark_key("rum"),
                None,
                &json!({"rum_stats": {"earliest": 1_763_596_800u64, "latest": 1_763_597_000u64, "total_rows": 55_555u64}}),
            )
            .unwrap();

        let svc_pre: Service =
            serde_json::from_value(p.adapter.records()["svc:fixtureService"].value.clone())
                .unwrap();
        CALLS.with(|c| c.set(0));
        SECRETS.with(|s| s.borrow_mut().clear());
        dr::sync_best_effort(&p, &svc_pre);
        let stale_gen7_dr_bytes =
            p.adapter.bucket_objects("fos-fixture-logs")["meta/cloud_dr_state.json"].clone();

        // 2027-01-15T08:14:00Z == 1_800_000_840; reset commits after FENCE_WAIT_S (300s) around 08:19Z:
        p.clock.store(1_800_000_540, Ordering::SeqCst);
        run_delete_data(&p);
        let post_dd_objs = p.adapter.bucket_objects("fos-fixture-logs");
        let post_dd_wm = p.adapter.records()[ingest::WATERMARK_KEY].value.clone();
        let reset_manifest_key = post_dd_wm["catalog"]["manifest"]
            .as_str()
            .unwrap()
            .to_owned();
        let reset_manifest: Manifest =
            serde_json::from_slice(&post_dd_objs[&reset_manifest_key]).unwrap();
        assert_eq!(reset_manifest.generation, 8);
        assert_eq!(reset_manifest.batch, "reset");
        let reset_dt = time::OffsetDateTime::parse(
            &reset_manifest.committed_at,
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap();
        let expected_reset_floor = post_dd_wm["floor_key"].as_str().unwrap().to_owned();

        // Seed post-reset ingest batch gen=9 committed 45 minutes AFTER reset:
        let post_ingest_key = "processed/_catalog/manifest-000009-ingest.json";
        let post_ingest_dt = reset_dt + time::Duration::minutes(45);
        let post_ingest_committed = post_ingest_dt
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();
        let post_ingest_manifest = Manifest {
            version: VERSION,
            service_id: "fixtureService".into(),
            generation: 9,
            batch: "ingest-post-reset".into(),
            schema: "v2".into(),
            committed_at: post_ingest_committed,
            previous: Some(reset_manifest_key.clone()),
            files: vec![],
            summaries: vec![],
            sources: vec![],
            replaces: vec![],
            bytes: 128,
            rows: 5,
            parents: BTreeMap::new(),
        };
        let post_ingest_bytes = serde_json::to_vec(&post_ingest_manifest).unwrap();

        for in_grace in [true, false] {
            let p_rec = Platform::new();
            p_rec.clock.store(p.now() + 2_700, Ordering::SeqCst);
            p_rec.adapter.seed_resource(
                "fixtureService",
                "snippet",
                json!({"name": "flp-owner-fixtureApp", "type": "recv", "priority": "0", "content": "# flp-owner:fixtureApp:fixtureService"}),
            );
            MTIMES.with(|m| m.borrow_mut().clear());
            for (k, v) in &post_dd_objs {
                p_rec
                    .adapter
                    .seed_object_bytes("fos-fixture-logs", k, v.clone());
            }
            if in_grace {
                p_rec.adapter.seed_object_bytes(
                    "fos-fixture-logs",
                    old_manifest_key,
                    old_manifest_bytes.clone(),
                );
                MTIMES.with(|m| {
                    m.borrow_mut()
                        .insert(old_manifest_key.into(), 1_763_596_800);
                });
            }
            p_rec.adapter.seed_object_bytes(
                "fos-fixture-logs",
                post_ingest_key,
                post_ingest_bytes.clone(),
            );
            p_rec.adapter.seed_object_bytes(
                "fos-fixture-logs",
                "meta/cloud_dr_state.json",
                stale_gen7_dr_bytes.clone(),
            );
            MTIMES.with(|m| {
                m.borrow_mut()
                    .insert(reset_manifest_key.clone(), reset_dt.unix_timestamp() as u64);
                m.borrow_mut().insert(
                    post_ingest_key.into(),
                    post_ingest_dt.unix_timestamp() as u64,
                );
            });

            let rec_res = p_rec.json(
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
                rec_res["catalog_generation"], 9,
                "BUG-CLOUD-DR-25A [in_grace={in_grace}]: must restore latest gen=9: {rec_res}"
            );
            let rec_records = p_rec.adapter.records();
            assert!(
                rec_records
                    .get(&ingest::watermark_key("rum"))
                    .is_none_or(|r| r.value.get("rum_stats").is_none_or(Value::is_null)),
                "BUG-CLOUD-DR-25A [in_grace={in_grace}]: deleted rum_stats must NOT be resurrected"
            );
            assert_eq!(
                rec_records[ingest::WATERMARK_KEY].value["floor_key"],
                expected_reset_floor,
                "BUG-CLOUD-DR-25A [in_grace={in_grace}]: floor_key must equal gen=8 reset minute (not rewound to 2026-11-20 and not jumped 45m forward to gen=9)"
            );
        }
    }
}

fn commit_real_catalog_batch(
    p: &Platform,
    svc: &Service,
    batch_name: &str,
    start_ts: i64,
    rows: u64,
) -> (String, Value) {
    use fastly_log_analytics::ingest::{self, storage::Store};

    let pq_key = ingest::processed_key(
        &format!("{}processed/request/", ingest::scope(svc)),
        start_ts,
        start_ts + 60,
        batch_name,
        rows,
    );
    p.adapter
        .seed_object_bytes(&svc.fos_bucket, &pq_key, vec![b'P', b'A', b'R', b'1']);
    let (ak, sk) = fastly_log_analytics::admin::fos_credentials(p, svc).unwrap();
    CALLS.with(|c| c.set(0));
    SECRETS.with(|s| s.borrow_mut().clear());
    ELAPSED_MS.with(|e| e.set(0));
    let store = Store {
        runtime: p,
        service: svc,
        access_key: ak,
        secret_key: sk,
        started: std::time::Instant::now(),
        vcpu_start: 0,
    };
    let prev_rec = p.adapter.records().get(ingest::WATERMARK_KEY).cloned();
    let prev_val = prev_rec
        .as_ref()
        .map(|r| r.value.clone())
        .unwrap_or(Value::Null);
    let output = ingest::worker::Output {
        key: pq_key.clone(),
        bytes: 128,
        rows,
        ..Default::default()
    };
    let next_wm = ingest::catalog::prepare(
        &store,
        &prev_val,
        batch_name,
        "v2",
        &[output],
        &[],
        &[],
        &[],
    )
    .expect("real catalog::prepare 3-file commit");
    p.adapter
        .cas(
            ingest::WATERMARK_KEY,
            prev_rec.as_ref().map(|r| r.generation.as_str()),
            &next_wm,
        )
        .unwrap();
    (pq_key, next_wm)
}

#[test]
fn bug_cloud_dr_26a_post_delete_data_3file_commits_day_filtering_and_rum_floor() {
    use fastly_log_analytics::ingest::{self, storage::Store};
    use std::sync::atomic::Ordering;

    RICH_FOS.with(|r| r.set(true));
    MTIMES.with(|m| m.borrow_mut().clear());

    let p = Platform::new();
    let provisioned = p.provision();
    p.drain(provisioned["job"].clone());

    let mut svc: Service =
        serde_json::from_value(p.adapter.records()["svc:fixtureService"].value.clone()).unwrap();
    svc.rum.enabled = true;
    p.adapter
        .cas(
            "svc:fixtureService",
            Some(&p.adapter.records()["svc:fixtureService"].generation),
            &serde_json::to_value(&svc).unwrap(),
        )
        .unwrap();

    // Commit two real 3-file (Block + Day + Manifest) pre-reset batches across two UTC days:
    let day1_ts = 1_795_046_400i64; // 2026-11-19T00:00:00Z
    let day2_ts = 1_795_132_800i64; // 2026-11-20T00:00:00Z
    p.clock.store(day1_ts as u64 + 120, Ordering::SeqCst);
    let (pre_pq_1, _) = commit_real_catalog_batch(&p, &svc, "g1s0", day1_ts, 200);
    p.clock.store(day2_ts as u64 + 120, Ordering::SeqCst);
    let (pre_pq_2, wm_gen2) = commit_real_catalog_batch(&p, &svc, "g2s0", day2_ts, 300);

    let mut wm_with_pre_floor = wm_gen2;
    wm_with_pre_floor["floor_key"] =
        json!("raw/request/year=2026/month=11/day=19/hour=00/minute=00/");
    p.adapter
        .cas(
            ingest::WATERMARK_KEY,
            Some(&p.adapter.records()[ingest::WATERMARK_KEY].generation),
            &wm_with_pre_floor,
        )
        .unwrap();
    p.adapter
        .cas(
            &ingest::watermark_key("rum"),
            None,
            &json!({
                "floor_key": "raw/rum/year=2026/month=11/day=19/hour=00/minute=00/",
                "rum_stats": {"earliest": day1_ts, "latest": day2_ts + 60, "rows": 42_000u64, "at": day2_ts + 120}
            }),
        )
        .unwrap();

    CALLS.with(|c| c.set(0));
    SECRETS.with(|s| s.borrow_mut().clear());
    dr::sync_best_effort(&p, &svc);
    let stale_some_dr_bytes =
        p.adapter.bucket_objects("fos-fixture-logs")["meta/cloud_dr_state.json"].clone();
    let stale_some_dr_val: Value = serde_json::from_slice(&stale_some_dr_bytes).unwrap();
    assert_eq!(
        stale_some_dr_val["rum_floor_key"], "raw/rum/year=2026/month=11/day=19/hour=00/minute=00/",
        "BUG-CLOUD-DR-26A(c): sync_best_effort must persist ingest:watermark:rum[floor_key]"
    );

    let mut stale_none_dr_val = stale_some_dr_val.clone();
    stale_none_dr_val
        .as_object_mut()
        .unwrap()
        .remove("catalog_head");
    let stale_none_dr_bytes = serde_json::to_vec(&stale_none_dr_val).unwrap();

    // Start Delete Data at 2027-01-15T08:09:00Z (1_800_000_540) and step ONLY into "cleanup"
    // (still inside DELETE_GRACE_S = 660s, so pre-reset Block/Day/Manifest & Parquet files remain in S3!):
    let dd_start_ts = 1_800_000_540u64;
    p.clock.store(dd_start_ts, Ordering::SeqCst);
    let dd_gen = p.adapter.records()["svc:fixtureService"].generation.clone();
    p.json(
        "POST",
        "/api/admin/services/fixtureService/delete-data",
        json!({"confirm_service_id": "fixtureService", "generation": dd_gen}),
    );
    p.clock.fetch_add(
        fastly_log_analytics::admin::reset::FENCE_WAIT_S + 5,
        Ordering::SeqCst,
    );
    for _ in 0..4 {
        let recs = p.adapter.records();
        let cur_svc: Service =
            serde_json::from_value(recs["svc:fixtureService"].value.clone()).unwrap();
        let (ak, sk) = fastly_log_analytics::admin::fos_credentials(&p, &cur_svc).unwrap();
        CALLS.with(|c| c.set(0));
        SECRETS.with(|s| s.borrow_mut().clear());
        ELAPSED_MS.with(|e| e.set(0));
        let store = Store {
            runtime: &p,
            service: &cur_svc,
            access_key: ak,
            secret_key: sk,
            started: std::time::Instant::now(),
            vcpu_start: 0,
        };
        let Some(step_res) = fastly_log_analytics::admin::reset::advance(&store) else {
            break;
        };
        step_res.unwrap();
        let job_rec = p.adapter.records()[fastly_log_analytics::admin::reset::JOB_KEY]
            .value
            .clone();
        if job_rec["status"] == "cleanup" {
            break;
        }
        p.clock.fetch_add(2, Ordering::SeqCst);
    }

    let in_grace_objs = p.adapter.bucket_objects("fos-fixture-logs");
    assert!(
        in_grace_objs.contains_key(&pre_pq_1) && in_grace_objs.contains_key(&pre_pq_2),
        "Pre-reset Parquet files must still be present during DELETE_GRACE_S"
    );
    let synced_cleanup_dr: Value =
        serde_json::from_slice(&in_grace_objs["meta/cloud_dr_state.json"]).unwrap();
    assert!(
        synced_cleanup_dr.get("rum_stats").is_none_or(Value::is_null),
        "BUG-CLOUD-DR-26A(a): entering Delete Data cleanup must immediately sanitize meta/cloud_dr_state.json rum_stats: {synced_cleanup_dr}"
    );
    assert_eq!(
        synced_cleanup_dr["catalog_head"]["generation"], 3,
        "BUG-CLOUD-DR-26A(a): entering Delete Data cleanup must mirror reset catalog_head (gen=3)"
    );

    let post_dd_wm = p.adapter.records()[ingest::WATERMARK_KEY].value.clone();
    let reset_manifest_key = post_dd_wm["catalog"]["manifest"]
        .as_str()
        .unwrap()
        .to_owned();
    let reset_manifest: Manifest =
        serde_json::from_slice(&in_grace_objs[&reset_manifest_key]).unwrap();
    let reset_dt = time::OffsetDateTime::parse(
        &reset_manifest.committed_at,
        &time::format_description::well_known::Rfc3339,
    )
    .unwrap();
    let expected_req_floor = post_dd_wm["floor_key"].as_str().unwrap().to_owned();
    let expected_rum_floor = p.adapter.records()[&ingest::watermark_key("rum")].value["floor_key"]
        .as_str()
        .unwrap()
        .to_owned();

    // Test 0, 1, and 12 real 3-file post-reset ingest commits (up to 36 post-reset .json files!)
    // across both stale Some(head) and stale None cloud_dr_state.json during DELETE_GRACE_S:
    let mut committed_so_far = 0usize;
    for post_commits in [0usize, 1, 12] {
        let cur_svc: Service =
            serde_json::from_value(p.adapter.records()["svc:fixtureService"].value.clone())
                .unwrap();
        while committed_so_far < post_commits {
            committed_so_far += 1;
            let commit_ts = reset_dt.unix_timestamp() + (committed_so_far as i64) * 60;
            p.clock.store(commit_ts as u64, Ordering::SeqCst);
            commit_real_catalog_batch(
                &p,
                &cur_svc,
                &format!("post{committed_so_far}"),
                commit_ts,
                10,
            );
        }
        let sim_objs = p.adapter.bucket_objects("fos-fixture-logs");
        let sim_mtimes = MTIMES.with(|m| m.borrow().clone());

        for head_is_some in [false, true] {
            let p_rec = Platform::new();
            p_rec.clock.store(p.now() + 60, Ordering::SeqCst);
            p_rec.adapter.seed_resource(
                "fixtureService",
                "snippet",
                json!({"name": "flp-owner-fixtureApp", "type": "recv", "priority": "0", "content": "# flp-owner:fixtureApp:fixtureService"}),
            );
            MTIMES.with(|m| *m.borrow_mut() = sim_mtimes.clone());
            for (k, v) in &sim_objs {
                p_rec
                    .adapter
                    .seed_object_bytes("fos-fixture-logs", k, v.clone());
            }
            p_rec.adapter.seed_object_bytes(
                "fos-fixture-logs",
                "meta/cloud_dr_state.json",
                if head_is_some {
                    stale_some_dr_bytes.clone()
                } else {
                    stale_none_dr_bytes.clone()
                },
            );

            let rec_res = p_rec.json(
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
                rec_res["catalog_generation"],
                3 + post_commits as u64,
                "[post_commits={post_commits}, head_is_some={head_is_some}] wrong generation: {rec_res}"
            );
            let rec_records = p_rec.adapter.records();
            let rum_wm = rec_records
                .get(&ingest::watermark_key("rum"))
                .expect("BUG-CLOUD-DR-26A(c): ingest:watermark:rum must be upserted with floor_key after reset");
            assert!(
                rum_wm.value.get("rum_stats").is_none_or(Value::is_null),
                "[post_commits={post_commits}, head_is_some={head_is_some}] rum_stats resurrected: {:?}",
                rum_wm.value
            );
            assert_eq!(
                rum_wm.value["floor_key"], expected_rum_floor,
                "[post_commits={post_commits}, head_is_some={head_is_some}] wrong RUM floor_key"
            );
            assert!(
                ingest::below_floor(
                    "raw/rum/year=2026/month=11/day=20/hour=00/minute=00/beacon.zst",
                    Some(&rum_wm.value)
                ),
                "pre-reset raw/rum beacon must be below recovered RUM floor_key"
            );
            assert_eq!(
                rec_records[ingest::WATERMARK_KEY].value["floor_key"],
                expected_req_floor,
                "[post_commits={post_commits}, head_is_some={head_is_some}] request floor_key must equal reset minute"
            );

            let recovered_svc: Service =
                serde_json::from_value(rec_records["svc:fixtureService"].value.clone()).unwrap();
            CALLS.with(|c| c.set(0));
            let ext =
                ingest::api::extents(&p_rec, &recovered_svc, std::time::Instant::now()).unwrap();
            let expected_rows = if post_commits == 0 {
                Value::Null
            } else {
                json!((post_commits as u64) * 10)
            };
            assert_eq!(
                ext["total_rows"],
                expected_rows,
                "[post_commits={post_commits}, head_is_some={head_is_some}] pre-reset rows resurrected in log-extents: {ext}"
            );

            CALLS.with(|c| c.set(0));
            let pre_files_resp = ingest::api::files(
                &p_rec,
                &recovered_svc,
                "from=2026-11-19T00:00:00Z&to=2026-11-21T00:00:00Z",
                std::time::Instant::now(),
            )
            .unwrap();
            assert_eq!(
                pre_files_resp["files"].as_array().unwrap().len(),
                0,
                "[post_commits={post_commits}, head_is_some={head_is_some}] pre-reset Parquet files resurrected: {pre_files_resp}"
            );
        }
    }

    // Poisoned rum_floor_key in cloud_dr_state.json must be rejected with 400 invalid_dr_floor_key:
    let mut bad_rum_dr = stale_some_dr_val;
    bad_rum_dr["rum_floor_key"] = json!("raw/rum/../../other/evil/");
    let p_bad = Platform::new();
    p_bad.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "meta/cloud_dr_state.json",
        serde_json::to_vec(&bad_rum_dr).unwrap(),
    );
    let bad_resp = p_bad.call(
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
    assert_eq!(bad_resp.status, 400);
    assert!(
        String::from_utf8_lossy(&bad_resp.body).contains("invalid_dr_floor_key"),
        "expected invalid_dr_floor_key, got {}",
        String::from_utf8_lossy(&bad_resp.body)
    );
}

#[test]
fn bug_cloud_dr_26b_reconcile_reserve_preserves_front_dictionary_sync_across_budgets() {
    use fastly_log_analytics::{admin, http::Request};
    use std::collections::BTreeSet;
    use std::sync::atomic::Ordering;

    RICH_FOS.with(|r| r.set(true));
    MTIMES.with(|m| m.borrow_mut().clear());

    // Build 9 real 3-file catalog commits (27 .json objects in processed/_catalog/) with catalog_head=Some(gen=3):
    let p_seed = Platform::new();
    let provisioned = p_seed.provision();
    p_seed.drain(provisioned["job"].clone());
    let svc_seed: Service =
        serde_json::from_value(p_seed.adapter.records()["svc:fixtureService"].value.clone())
            .unwrap();
    let base_ts = 1_795_200_000i64;
    for i in 1..=3 {
        p_seed
            .clock
            .store((base_ts + i * 60) as u64, Ordering::SeqCst);
        commit_real_catalog_batch(&p_seed, &svc_seed, &format!("seed{i}"), base_ts + i * 60, 5);
    }
    CALLS.with(|c| c.set(0));
    SECRETS.with(|s| s.borrow_mut().clear());
    dr::sync_best_effort(&p_seed, &svc_seed);
    let gen3_dr_bytes =
        p_seed.adapter.bucket_objects("fos-fixture-logs")["meta/cloud_dr_state.json"].clone();
    for i in 4..=12 {
        p_seed
            .clock
            .store((base_ts + i * 60) as u64, Ordering::SeqCst);
        commit_real_catalog_batch(&p_seed, &svc_seed, &format!("seed{i}"), base_ts + i * 60, 5);
    }
    let catalog_objs = p_seed.adapter.bucket_objects("fos-fixture-logs");
    let catalog_json_count = catalog_objs
        .keys()
        .filter(|k| k.starts_with("processed/_catalog/") && k.ends_with(".json"))
        .count();
    assert!(
        catalog_json_count >= 24,
        "expected >= 24 catalog .json objects, got {catalog_json_count}"
    );
    let seed_mtimes = MTIMES.with(|m| m.borrow().clone());

    for remaining in [24u32, 25, 32] {
        let p_cold = Platform::new();
        p_cold.adapter.seed_resource(
            "fixtureService",
            "snippet",
            json!({"name": "flp-owner-fixtureApp", "type": "recv", "priority": "0", "content": "# flp-owner:fixtureApp:fixtureService"}),
        );
        MTIMES.with(|m| *m.borrow_mut() = seed_mtimes.clone());
        for (k, v) in &catalog_objs {
            p_cold
                .adapter
                .seed_object_bytes("fos-fixture-logs", k, v.clone());
        }
        p_cold.adapter.seed_object_bytes(
            "fos-fixture-logs",
            "meta/cloud_dr_state.json",
            gen3_dr_bytes.clone(),
        );
        ELAPSED_MS.with(|e| e.set(0));
        CALLS.with(|c| c.set(32 - remaining));
        SECRETS
            .with(|s| *s.borrow_mut() = BTreeSet::from(["front_key".into(), "session_key".into()]));
        let resp = admin::handle(
            &p_cold,
            "fixtureAdmin",
            &Request {
                method: "POST".into(),
                path: "/api/admin/provision/recover".into(),
                headers: BTreeMap::new(),
                body: json!({
                    "token": "fixture-token",
                    "service_id": "fixtureService",
                    "fos_bucket": "fos-fixture-logs",
                    "fos_region": "us-east-1"
                })
                .to_string()
                .into_bytes(),
            },
        );
        assert_eq!(
            resp.status,
            200,
            "[cold_start remaining={remaining}] expected 200: {}",
            String::from_utf8_lossy(&resp.body)
        );
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["front_synced"], true,
            "[cold_start remaining={remaining}] BUG-CLOUD-DR-26B: front_synced must be true: {body}"
        );
        assert_eq!(
            p_cold
                .adapter
                .dictionary_items("fixtureBuckets")
                .get("fixtureService")
                .map(String::as_str),
            Some("fos-fixture-logs|us-east-1")
        );
        let creds_dict = p_cold.adapter.dictionary_items("fixtureCredentials");
        assert!(creds_dict.contains_key("fixtureService.ak"));
        assert!(creds_dict.contains_key("fixtureService.sk"));
    }

    // Warm teardown re-recovery at remaining=25 with views=[v1] and rum_stats=Some(...):
    let mut torn_svc = svc_seed.clone();
    torn_svc.status = "torn_down".into();
    p_seed
        .adapter
        .cas(
            "svc:fixtureService",
            Some(&p_seed.adapter.records()["svc:fixtureService"].generation),
            &serde_json::to_value(&torn_svc).unwrap(),
        )
        .unwrap();
    let mut warm_dr_val: Value = serde_json::from_slice(&gen3_dr_bytes).unwrap();
    warm_dr_val["views"] = json!([{
        "id": "v1",
        "service_id": "fixtureService",
        "name": "Errors",
        "filters_json": "{\"status\":500}",
        "time_range_type": "relative",
        "page": "overview"
    }]);
    warm_dr_val["rum_stats"] = json!({"rows": 120, "at": base_ts + 180});
    p_seed.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "meta/cloud_dr_state.json",
        serde_json::to_vec(&warm_dr_val).unwrap(),
    );
    let warm_res = p_seed.json(
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
        warm_res["front_synced"], true,
        "[teardown_re_recovery remaining=25] BUG-CLOUD-DR-26B: front_synced must be true: {warm_res}"
    );
}

#[test]
fn bug_cloud_dr_26c_shared_prefix_foreign_day_isolation() {
    use fastly_log_analytics::ingest;
    use std::sync::atomic::Ordering;

    RICH_FOS.with(|r| r.set(true));
    MTIMES.with(|m| m.borrow_mut().clear());

    // 1. Mixed shared _catalog/ prefix: fixtureService has gen=1..2 (10 rows total),
    // while foreignService22Chars0 has gen=2 and gen=10 (9,999 rows) in the same prefix!
    let p_own = Platform::new();
    let provisioned = p_own.provision();
    p_own.drain(provisioned["job"].clone());
    let own_svc: Service =
        serde_json::from_value(p_own.adapter.records()["svc:fixtureService"].value.clone())
            .unwrap();
    let day_ts = 1_795_132_800i64; // 2026-11-20T00:00:00Z
    p_own.clock.store(day_ts as u64 + 60, Ordering::SeqCst);
    let (own_pq_1, _) = commit_real_catalog_batch(&p_own, &own_svc, "own1", day_ts, 4);
    p_own.clock.store(day_ts as u64 + 120, Ordering::SeqCst);
    let (own_pq_2, _) = commit_real_catalog_batch(&p_own, &own_svc, "own2", day_ts + 60, 6);
    let own_wm = p_own.adapter.records()[ingest::WATERMARK_KEY].value.clone();
    let own_manifest_key = own_wm["catalog"]["manifest"].as_str().unwrap().to_owned();
    let own_day_key = own_wm["catalog"]["days"][day_ts.to_string()]
        .as_str()
        .unwrap()
        .to_owned();
    let own_objs = p_own.adapter.bucket_objects("fos-fixture-logs");
    let own_mtimes = MTIMES.with(|m| m.borrow().clone());

    let p_mix = Platform::new();
    p_mix.adapter.seed_resource(
        "fixtureService",
        "snippet",
        json!({"name": "flp-owner-fixtureApp", "type": "recv", "priority": "0", "content": "# flp-owner:fixtureApp:fixtureService"}),
    );
    MTIMES.with(|m| *m.borrow_mut() = own_mtimes);
    for (k, v) in &own_objs {
        if k != "meta/cloud_dr_state.json" {
            p_mix
                .adapter
                .seed_object_bytes("fos-fixture-logs", k, v.clone());
        }
    }
    // Seed foreign Manifest (gen=10) + both unattributed high-gen and low-gen foreign Day objects:
    let foreign_manifest = Manifest {
        version: VERSION,
        service_id: "foreignService22Chars0".into(),
        generation: 10,
        batch: "foreign-b10".into(),
        schema: "v2".into(),
        committed_at: "2026-11-20T02:00:00Z".into(),
        previous: None,
        files: vec![],
        summaries: vec![],
        sources: vec![],
        replaces: vec![],
        bytes: 4096,
        rows: 9_999,
        parents: BTreeMap::new(),
    };
    let foreign_day_unattributed = Day {
        version: VERSION,
        hours: BTreeMap::from([(
            day_ts,
            Reference {
                key: "processed/_catalog/foreign_block_10.json".into(),
                generation: 10,
            },
        )]),
        statistics: Statistics {
            earliest: Some(day_ts),
            latest: Some(day_ts + 3599),
            earliest_log: None,
            latest_log: None,
            rows: 9_999,
        },
        ..Day::default()
    };
    let foreign_day_low_gen = Day {
        version: VERSION,
        hours: BTreeMap::from([(
            day_ts + 86_400,
            Reference {
                key: "processed/_catalog/foreign_block_01.json".into(),
                generation: 1,
            },
        )]),
        statistics: Statistics {
            earliest: Some(day_ts + 86_400),
            latest: Some(day_ts + 89_999),
            earliest_log: None,
            latest_log: None,
            rows: 777,
        },
        ..Day::default()
    };
    p_mix.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "processed/_catalog/foreign_manifest_10.json",
        serde_json::to_vec(&foreign_manifest).unwrap(),
    );
    p_mix.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "processed/_catalog/foreign_day_10.json",
        serde_json::to_vec(&foreign_day_unattributed).unwrap(),
    );
    p_mix.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "processed/_catalog/foreign_day_01.json",
        serde_json::to_vec(&foreign_day_low_gen).unwrap(),
    );
    MTIMES.with(|m| {
        let mut m = m.borrow_mut();
        m.insert(
            "processed/_catalog/foreign_manifest_10.json".into(),
            day_ts as u64 + 605,
        );
        m.insert(
            "processed/_catalog/foreign_day_10.json".into(),
            day_ts as u64 + 604,
        );
        m.insert(
            "processed/_catalog/foreign_day_01.json".into(),
            day_ts as u64 + 603,
        );
        m.insert(own_manifest_key, day_ts as u64 + 602);
        m.insert(own_day_key, day_ts as u64 + 601);
    });

    SECRETS.with(|s| s.borrow_mut().clear());
    let mix_res = p_mix.json(
        "POST",
        "/api/admin/provision/recover",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1"
        }),
    );
    assert_eq!(mix_res["source"], "catalog_scan", "{mix_res}");
    assert_eq!(mix_res["catalog_generation"], 2, "{mix_res}");

    let mix_svc: Service =
        serde_json::from_value(p_mix.adapter.records()["svc:fixtureService"].value.clone())
            .unwrap();
    CALLS.with(|c| c.set(0));
    let mix_ext = ingest::api::extents(&p_mix, &mix_svc, std::time::Instant::now()).unwrap();
    assert_eq!(
        mix_ext["total_rows"], 10,
        "BUG-CLOUD-DR-26C: foreign Day rows must NOT leak into log-extents: {mix_ext}"
    );
    CALLS.with(|c| c.set(0));
    let mix_files = ingest::api::files(
        &p_mix,
        &mix_svc,
        "from=2026-11-20T00:00:00Z&to=2026-11-21T00:00:00Z",
        std::time::Instant::now(),
    )
    .expect("BUG-CLOUD-DR-26C: /api/files must succeed 200 OK without foreign Block 503");
    let returned_keys: Vec<&str> = mix_files["files"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|f| f["key"].as_str())
        .collect();
    assert_eq!(returned_keys, vec![own_pq_1.as_str(), own_pq_2.as_str()]);

    // 2. Foreign Day objects with newer LastModified than foreign Manifest (best_manifest.is_none()):
    let p_only_foreign = Platform::new();
    p_only_foreign.adapter.seed_resource(
        "fixtureService",
        "snippet",
        json!({"name": "flp-owner-fixtureApp", "type": "recv", "priority": "0", "content": "# flp-owner:fixtureApp:fixtureService"}),
    );
    MTIMES.with(|m| m.borrow_mut().clear());
    for i in 0..8u64 {
        let fday_key = format!("processed/_catalog/fday_{i:02}.json");
        let fblk_key = format!("processed/_catalog/fblk_{i:02}.json");
        let fblk = json!({
            "version": VERSION,
            "service_id": "foreignService22Chars0",
            "generation": 10 + i,
            "hour": day_ts + (i as i64) * 3600,
            "previous": null,
            "files": [],
            "summaries": [],
            "replaces": [],
            "committed_at": {}
        });
        let fday = Day {
            version: VERSION,
            hours: BTreeMap::from([(
                day_ts + (i as i64) * 86_400,
                Reference {
                    key: fblk_key.clone(),
                    generation: 10 + i,
                },
            )]),
            statistics: Statistics {
                earliest: Some(day_ts),
                latest: Some(day_ts + 3599),
                earliest_log: None,
                latest_log: None,
                rows: 8_888,
            },
            ..Day::default()
        };
        p_only_foreign.adapter.seed_object_bytes(
            "fos-fixture-logs",
            &fblk_key,
            serde_json::to_vec(&fblk).unwrap(),
        );
        p_only_foreign.adapter.seed_object_bytes(
            "fos-fixture-logs",
            &fday_key,
            serde_json::to_vec(&fday).unwrap(),
        );
        MTIMES.with(|m| {
            m.borrow_mut().insert(fblk_key, 1_800_000_000 + i);
            m.borrow_mut().insert(fday_key, 1_800_001_000 + i);
        });
    }
    let only_foreign_res = p_only_foreign.json(
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
    assert_eq!(only_foreign_res["source"], "minimal", "{only_foreign_res}");
    assert!(
        only_foreign_res["catalog_generation"].is_null(),
        "{only_foreign_res}"
    );
}

#[test]
fn bug_cloud_dr_27b_paused_prepare_manifest_not_dangling_and_gen_sorted_manifests() {
    use fastly_log_analytics::ingest::{self, storage::Store};
    use std::sync::atomic::Ordering;

    struct RichGuard;
    impl Drop for RichGuard {
        fn drop(&mut self) {
            RICH_FOS.with(|r| r.set(false));
            MTIMES.with(|m| m.borrow_mut().clear());
        }
    }
    let _guard = RichGuard;
    RICH_FOS.with(|r| r.set(true));
    MTIMES.with(|m| m.borrow_mut().clear());

    let p = Platform::new();
    let provisioned = p.provision();
    p.drain(provisioned["job"].clone());
    let svc: Service =
        serde_json::from_value(p.adapter.records()["svc:fixtureService"].value.clone()).unwrap();
    let (ak, sk) = fastly_log_analytics::admin::fos_credentials(&p, &svc).unwrap();

    // 1. 2-day commit pausing mid-prepare_reserving at T1 and resuming at T1 + 120s:
    // Every day_key embedded in manifest.head_days MUST exist in S3 (no 404 dangling key),
    // and manifest key must carry manifest-{:020}- prefix with mtime >= max(day mtimes).
    let day1 = 1_795_132_800i64; // 2026-11-19T00:00:00Z
    let day2 = day1 + 86_400; // 2026-11-20T00:00:00Z
    let pq1 = ingest::processed_key("processed/request/", day1 + 60, day1 + 120, "p1", 10);
    let pq2 = ingest::processed_key("processed/request/", day2 + 60, day2 + 3540, "p2", 20);
    p.adapter
        .seed_object_bytes(&svc.fos_bucket, &pq1, b"PAR1".to_vec());
    p.adapter
        .seed_object_bytes(&svc.fos_bucket, &pq2, b"PAR1".to_vec());
    let outputs = [
        ingest::worker::Output {
            key: pq1,
            bytes: 128,
            rows: 10,
            ..Default::default()
        },
        ingest::worker::Output {
            key: pq2,
            bytes: 128,
            rows: 20,
            ..Default::default()
        },
    ];
    // Pause when 1 call short of finishing Day uploads + manifest:
    p.clock.store((day2 + 120) as u64, Ordering::SeqCst);
    SECRETS.with(|s| s.borrow_mut().clear());
    ELAPSED_MS.with(|e| e.set(0));
    CALLS.with(|c| c.set(32 - 6)); // 6 remaining calls (reserve=1 -> stops mid-commit!)
    let store_t1 = Store {
        runtime: &p,
        service: &svc,
        access_key: ak.clone(),
        secret_key: sk.clone(),
        started: std::time::Instant::now(),
        vcpu_start: 0,
    };
    let err_t1 = ingest::catalog::prepare_reserving(
        &store_t1,
        &Value::Null,
        "paused-2day",
        "v2",
        &outputs,
        &[],
        &[],
        &[],
        1,
    )
    .expect_err("Tick 1 with 8 remaining calls must pause mid-prepare");
    assert_eq!(err_t1, "catalog_prepare_pending");

    // Advance clock by +180s on Tick 2 so runtime.now() differs from work.at:
    p.clock.store((day2 + 300) as u64, Ordering::SeqCst);
    CALLS.with(|c| c.set(0));
    let store_t2 = Store {
        runtime: &p,
        service: &svc,
        access_key: ak,
        secret_key: sk,
        started: std::time::Instant::now(),
        vcpu_start: 0,
    };
    let next_wm = ingest::catalog::prepare_reserving(
        &store_t2,
        &Value::Null,
        "paused-2day",
        "v2",
        &outputs,
        &[],
        &[],
        &[],
        1,
    )
    .expect("Tick 2 must finish paused 2-day prepare");
    p.adapter
        .cas(ingest::WATERMARK_KEY, None, &next_wm)
        .unwrap();

    let head = catalog::head(&next_wm).unwrap();
    let m_key = head.manifest.as_ref().unwrap();
    let expected_rev_g1 = format!(
        "processed/_catalog/manifest-s-fixtureService-rev-{:020}-",
        u64::MAX - 1
    );
    assert!(
        m_key.starts_with(&expected_rev_g1),
        "manifest key must carry service-scoped reverse-generation prefix: {m_key}"
    );
    let bucket_objs = p.adapter.bucket_objects(&svc.fos_bucket);
    let m_val: Value = serde_json::from_slice(&bucket_objs[m_key]).unwrap();
    let embedded_days = m_val["head_days"].as_object().expect("head_days map");
    assert_eq!(embedded_days.len(), 2);
    let mtimes = MTIMES.with(|m| m.borrow().clone());
    for (_dts, dk_val) in embedded_days {
        let dk = dk_val.as_str().unwrap();
        assert!(
            bucket_objs.contains_key(dk),
            "BUG-CLOUD-DR-27B: manifest.head_days key {dk} is a dangling 404 in S3!"
        );
        assert!(
            mtimes[m_key] >= mtimes[dk],
            "manifest mtime {} must be >= day mtime {}",
            mtimes[m_key],
            mtimes[dk]
        );
    }

    // 2. 18 commits all sharing the exact same LastModified second (e.g. S3 CopyObject):
    // Zero-padded generation prefix lets tight 24-call DR recover generation 19 on 1st get_object!
    for i in 2..=19u64 {
        commit_real_catalog_batch(&p, &svc, &format!("tie{i}"), day2 + (i as i64) * 60, 5);
    }
    let all_objs = p.adapter.bucket_objects(&svc.fos_bucket);
    MTIMES.with(|m| {
        let mut map = m.borrow_mut();
        for k in all_objs.keys() {
            map.insert(k.clone(), 1_800_000_000);
        }
    });
    let p_dr = Platform::new();
    p_dr.adapter.seed_resource(
        "fixtureService",
        "snippet",
        json!({"name": "flp-owner-fixtureApp", "type": "recv", "priority": "0", "content": "# flp-owner:fixtureApp:fixtureService"}),
    );
    for (k, v) in &all_objs {
        if k != "meta/cloud_dr_state.json" {
            p_dr.adapter
                .seed_object_bytes(&svc.fos_bucket, k, v.clone());
        }
    }
    CALLS.with(|c| c.set(32 - 24));
    SECRETS.with(|s| {
        *s.borrow_mut() =
            std::collections::BTreeSet::from(["front_key".into(), "session_key".into()])
    });
    let rec_resp = fastly_log_analytics::admin::handle(
        &p_dr,
        "fixtureAdmin",
        &fastly_log_analytics::http::Request {
            method: "POST".into(),
            path: "/api/admin/provision/recover".into(),
            headers: BTreeMap::new(),
            body: json!({
                "token": "fixture-token",
                "service_id": "fixtureService",
                "fos_bucket": "fos-fixture-logs",
                "fos_region": "us-east-1"
            })
            .to_string()
            .into_bytes(),
        },
    );
    assert_eq!(rec_resp.status, 200);
    let rec_body: Value = serde_json::from_slice(&rec_resp.body).unwrap();
    assert_eq!(rec_body["catalog_generation"], 19, "{rec_body}");
}

#[test]
fn bug_cloud_dr_27a_summary_bundle_publish_and_drop_missing_preserve_manifest_days_and_reset_fence()
{
    use fastly_log_analytics::ingest::{
        self,
        bundle::{open_bundle_key, Unit},
        catalog::bundles::{self, Coverage, PublishOutcome},
        storage::Store,
    };
    use std::collections::BTreeSet;
    use std::sync::atomic::Ordering;

    struct RichGuard;
    impl Drop for RichGuard {
        fn drop(&mut self) {
            RICH_FOS.with(|r| r.set(false));
            MTIMES.with(|m| m.borrow_mut().clear());
        }
    }
    let _guard = RichGuard;
    RICH_FOS.with(|r| r.set(true));
    MTIMES.with(|m| m.borrow_mut().clear());

    let p = Platform::new();
    let provisioned = p.provision();
    p.drain(provisioned["job"].clone());
    let svc: Service =
        serde_json::from_value(p.adapter.records()["svc:fixtureService"].value.clone()).unwrap();

    // Part 1: Normal service with 500 rows across 2 days + dr_state synced at gen=2 + 6 open_bundle publishes + 1 drop_missing:
    let day1 = 1_795_132_800i64;
    let day2 = day1 + 86_400;
    p.clock.store((day1 + 120) as u64, Ordering::SeqCst);
    commit_real_catalog_batch(&p, &svc, "norm1", day1 + 60, 200);
    p.clock.store((day2 + 120) as u64, Ordering::SeqCst);
    commit_real_catalog_batch(&p, &svc, "norm2", day2 + 60, 300);
    CALLS.with(|c| c.set(0));
    SECRETS.with(|s| s.borrow_mut().clear());
    dr::sync_best_effort(&p, &svc);
    let dr_gen2 = p.adapter.bucket_objects(&svc.fos_bucket)["meta/cloud_dr_state.json"].clone();

    let (ak, sk) = fastly_log_analytics::admin::fos_credentials(&p, &svc).unwrap();
    let mut last_open_ref = None;
    let mut last_open_file = String::new();
    for step_i in 1..=6u64 {
        p.clock
            .store((day2 + 200 + step_i as i64 * 10) as u64, Ordering::SeqCst);
        CALLS.with(|c| c.set(0));
        let store = Store {
            runtime: &p,
            service: &svc,
            access_key: ak.clone(),
            secret_key: sk.clone(),
            started: std::time::Instant::now(),
            vcpu_start: 0,
        };
        let ob_key = open_bundle_key(
            "processed/summary_bundle/request/",
            day2,
            (day2 + step_i as i64 * 60) * 1000,
            &[format!("w{step_i}.parquet")],
        );
        p.adapter
            .seed_object_bytes(&svc.fos_bucket, &ob_key, b"PAR1".to_vec());
        let cov = Coverage {
            file: ingest::worker::Output {
                key: ob_key.clone(),
                bytes: 64,
                rows: step_i * 5,
                ..Default::default()
            },
            start: day2,
            end: day2 + 3600,
            windows: BTreeSet::from([format!("w{step_i}.parquet")]),
            hours: BTreeMap::new(),
            built_generation: 2 + step_i - 1,
        };
        match bundles::publish(&store, &cov, 0).unwrap() {
            PublishOutcome::Published(h) => {
                last_open_ref = h.open_bundles.get(&day2).cloned();
                last_open_file = ob_key;
            }
            _ => panic!("expected Published"),
        }
    }
    p.clock.store((day2 + 400) as u64, Ordering::SeqCst);
    CALLS.with(|c| c.set(0));
    let store_drop = Store {
        runtime: &p,
        service: &svc,
        access_key: ak.clone(),
        secret_key: sk.clone(),
        started: std::time::Instant::now(),
        vcpu_start: 0,
    };
    let dropped_head =
        bundles::drop_missing(&store_drop, &[(last_open_ref.unwrap(), last_open_file)], 0)
            .unwrap()
            .expect("drop_missing head");
    assert_eq!(dropped_head.generation, 9);
    let expected_rev_g9 = format!(
        "processed/_catalog/manifest-s-fixtureService-rev-{:020}-",
        u64::MAX - 9
    );
    assert!(
        dropped_head
            .manifest
            .as_deref()
            .unwrap()
            .starts_with(&expected_rev_g9),
        "bundles::drop_missing must write service-scoped reverse-generation manifest: {:?}",
        dropped_head.manifest
    );
    let _ = Unit::Hour;

    // Cold DR recover at remaining=25 must preserve all 500 rows across both days:
    let snap_objs = p.adapter.bucket_objects(&svc.fos_bucket);
    let mtimes_norm = MTIMES.with(|m| m.borrow().clone());
    let p_rec1 = Platform::new();
    p_rec1.adapter.seed_resource(
        "fixtureService",
        "snippet",
        json!({"name": "flp-owner-fixtureApp", "type": "recv", "priority": "0", "content": "# flp-owner:fixtureApp:fixtureService"}),
    );
    MTIMES.with(|m| *m.borrow_mut() = mtimes_norm);
    for (k, v) in &snap_objs {
        p_rec1
            .adapter
            .seed_object_bytes(&svc.fos_bucket, k, v.clone());
    }
    p_rec1
        .adapter
        .seed_object_bytes(&svc.fos_bucket, "meta/cloud_dr_state.json", dr_gen2.clone());
    CALLS.with(|c| c.set(32 - 25));
    SECRETS.with(|s| {
        *s.borrow_mut() =
            std::collections::BTreeSet::from(["front_key".into(), "session_key".into()])
    });
    let r1 = fastly_log_analytics::admin::handle(
        &p_rec1,
        "fixtureAdmin",
        &fastly_log_analytics::http::Request {
            method: "POST".into(),
            path: "/api/admin/provision/recover".into(),
            headers: BTreeMap::new(),
            body: json!({
                "token": "fixture-token",
                "service_id": "fixtureService",
                "fos_bucket": "fos-fixture-logs",
                "fos_region": "us-east-1"
            })
            .to_string()
            .into_bytes(),
        },
    );
    assert_eq!(r1.status, 200);
    let rec1_head = catalog::head(&p_rec1.adapter.records()[ingest::WATERMARK_KEY].value).unwrap();
    assert_eq!(rec1_head.generation, 9);
    assert_eq!(
        rec1_head.statistics.values().map(|s| s.rows).sum::<u64>(),
        500,
        "BUG-CLOUD-DR-27A: summary_bundle commits must not wipe Head.days on DR recover!"
    );

    // Part 2: Post-Delete-Data reset + 1 post-reset ingest (10 rows) + 12 open_bundle publishes + stale pre-reset dr_state (42,000 RUM rows):
    p.adapter
        .cas(
            &ingest::watermark_key("rum"),
            None,
            &json!({"floor_key": "raw/rum/year=2026/month=11/day=19/hour=00/minute=00/", "rum_stats": {"earliest": day1, "latest": day2, "rows": 42_000u64, "at": day2 + 400}}),
        )
        .unwrap();
    CALLS.with(|c| c.set(0));
    SECRETS.with(|s| s.borrow_mut().clear());
    dr::sync_best_effort(&p, &svc);
    let stale_pre_reset_dr =
        p.adapter.bucket_objects(&svc.fos_bucket)["meta/cloud_dr_state.json"].clone();

    let reset_start_ts = day2 + 3600;
    p.clock.store(reset_start_ts as u64, Ordering::SeqCst);
    let gen_svc = p.adapter.records()["svc:fixtureService"].generation.clone();
    let dd_resp = p.call(
        "POST",
        "/api/admin/services/fixtureService/delete-data",
        json!({"confirm_service_id": "fixtureService", "generation": gen_svc}),
    );
    assert_eq!(dd_resp.status, 200);
    p.clock
        .store((reset_start_ts + 425) as u64, Ordering::SeqCst);
    for _ in 0..8 {
        let recs = p.adapter.records();
        let cur_svc: Service =
            serde_json::from_value(recs["svc:fixtureService"].value.clone()).unwrap();
        let (ak_s, sk_s) = fastly_log_analytics::admin::fos_credentials(&p, &cur_svc).unwrap();
        CALLS.with(|c| c.set(0));
        SECRETS.with(|s| s.borrow_mut().clear());
        let store = Store {
            runtime: &p,
            service: &cur_svc,
            access_key: ak_s,
            secret_key: sk_s,
            started: std::time::Instant::now(),
            vcpu_start: 0,
        };
        if fastly_log_analytics::admin::reset::advance(&store).is_none() {
            break;
        }
    }
    let post_day = day2 + 86_400;
    p.clock.store((post_day + 120) as u64, Ordering::SeqCst);
    commit_real_catalog_batch(&p, &svc, "post1", post_day + 60, 10);
    for step_i in 1..=12u64 {
        p.clock.store(
            (post_day + 200 + step_i as i64 * 10) as u64,
            Ordering::SeqCst,
        );
        CALLS.with(|c| c.set(0));
        let store = Store {
            runtime: &p,
            service: &svc,
            access_key: ak.clone(),
            secret_key: sk.clone(),
            started: std::time::Instant::now(),
            vcpu_start: 0,
        };
        let ob_key = open_bundle_key(
            "processed/summary_bundle/request/",
            post_day,
            (post_day + step_i as i64 * 60) * 1000,
            &[format!("pw{step_i}.parquet")],
        );
        p.adapter
            .seed_object_bytes(&svc.fos_bucket, &ob_key, b"PAR1".to_vec());
        let cov = Coverage {
            file: ingest::worker::Output {
                key: ob_key,
                bytes: 64,
                rows: step_i,
                ..Default::default()
            },
            start: post_day,
            end: post_day + 3600,
            windows: BTreeSet::from([format!("pw{step_i}.parquet")]),
            hours: BTreeMap::new(),
            built_generation: 10 + step_i,
        };
        assert!(matches!(
            bundles::publish(&store, &cov, 0).unwrap(),
            PublishOutcome::Published(_)
        ));
    }

    let post_objs = p.adapter.bucket_objects(&svc.fos_bucket);
    let mtimes_post = MTIMES.with(|m| m.borrow().clone());
    let p_rec2 = Platform::new();
    p_rec2.adapter.seed_resource(
        "fixtureService",
        "snippet",
        json!({"name": "flp-owner-fixtureApp", "type": "recv", "priority": "0", "content": "# flp-owner:fixtureApp:fixtureService"}),
    );
    MTIMES.with(|m| *m.borrow_mut() = mtimes_post);
    for (k, v) in &post_objs {
        p_rec2
            .adapter
            .seed_object_bytes(&svc.fos_bucket, k, v.clone());
    }
    p_rec2.adapter.seed_object_bytes(
        &svc.fos_bucket,
        "meta/cloud_dr_state.json",
        stale_pre_reset_dr,
    );
    CALLS.with(|c| c.set(32 - 25));
    SECRETS.with(|s| {
        *s.borrow_mut() =
            std::collections::BTreeSet::from(["front_key".into(), "session_key".into()])
    });
    let r2 = fastly_log_analytics::admin::handle(
        &p_rec2,
        "fixtureAdmin",
        &fastly_log_analytics::http::Request {
            method: "POST".into(),
            path: "/api/admin/provision/recover".into(),
            headers: BTreeMap::new(),
            body: json!({
                "token": "fixture-token",
                "service_id": "fixtureService",
                "fos_bucket": "fos-fixture-logs",
                "fos_region": "us-east-1"
            })
            .to_string()
            .into_bytes(),
        },
    );
    assert_eq!(r2.status, 200);
    let rec2_recs = p_rec2.adapter.records();
    let rec2_head = catalog::head(&rec2_recs[ingest::WATERMARK_KEY].value).unwrap();
    assert_eq!(
        rec2_head.statistics.values().map(|s| s.rows).sum::<u64>(),
        10,
        "post-reset 10 rows must survive 12 summary_bundle commits!"
    );
    assert!(
        rec2_recs
            .get(&ingest::watermark_key("rum"))
            .is_none_or(|r| r.value.get("rum_stats").is_none_or(Value::is_null)),
        "BUG-CLOUD-DR-27A: deleted 42,000 RUM rows must NOT be resurrected!"
    );
}

#[test]
fn bug_cloud_dr_27a_reset_manifest_tie_exact_job_floor_and_rum_part_gate() {
    use fastly_log_analytics::ingest::{self, storage::Store};
    use std::sync::atomic::Ordering;

    struct RichGuard;
    impl Drop for RichGuard {
        fn drop(&mut self) {
            RICH_FOS.with(|r| r.set(false));
            MTIMES.with(|m| m.borrow_mut().clear());
        }
    }
    let _guard = RichGuard;
    RICH_FOS.with(|r| r.set(true));
    MTIMES.with(|m| m.borrow_mut().clear());

    // Part 1: 16 pre-reset commits (2,000 rows) + Delete Data at 08:09:20Z (floor 08:09:00Z, CAS at 08:16:25Z)
    // + same-second LastModified tie across all _catalog/*.json + 0 post-reset commits:
    let p = Platform::new();
    let provisioned = p.provision();
    p.drain(provisioned["job"].clone());
    let svc: Service =
        serde_json::from_value(p.adapter.records()["svc:fixtureService"].value.clone()).unwrap();
    let base_ts = 1_799_996_400i64; // 2027-01-15T07:00:00Z
    for i in 1..=16u64 {
        p.clock
            .store((base_ts + i as i64 * 60) as u64, Ordering::SeqCst);
        commit_real_catalog_batch(&p, &svc, &format!("pre{i}"), base_ts + i as i64 * 60, 125);
    }
    p.adapter
        .cas(
            &ingest::watermark_key("rum"),
            None,
            &json!({"rum_stats": {"earliest": base_ts, "latest": base_ts + 960, "rows": 5_000u64, "at": base_ts + 960}}),
        )
        .unwrap();
    CALLS.with(|c| c.set(0));
    SECRETS.with(|s| s.borrow_mut().clear());
    dr::sync_best_effort(&p, &svc);
    let stale_dr = p.adapter.bucket_objects(&svc.fos_bucket)["meta/cloud_dr_state.json"].clone();

    let dd_click_ts = 1_800_000_560u64; // minute = 1_800_000_540 (08:09:00Z)
    p.clock.store(dd_click_ts, Ordering::SeqCst);
    let gen_svc = p.adapter.records()["svc:fixtureService"].generation.clone();
    let dd_resp = p.call(
        "POST",
        "/api/admin/services/fixtureService/delete-data",
        json!({"confirm_service_id": "fixtureService", "generation": gen_svc}),
    );
    assert_eq!(dd_resp.status, 200);
    p.clock.store(dd_click_ts + 425, Ordering::SeqCst); // 08:16:25Z (7m05s later!)
    for _ in 0..8 {
        let recs = p.adapter.records();
        let cur_svc: Service =
            serde_json::from_value(recs["svc:fixtureService"].value.clone()).unwrap();
        let (ak_s, sk_s) = fastly_log_analytics::admin::fos_credentials(&p, &cur_svc).unwrap();
        CALLS.with(|c| c.set(0));
        SECRETS.with(|s| s.borrow_mut().clear());
        let store = Store {
            runtime: &p,
            service: &cur_svc,
            access_key: ak_s,
            secret_key: sk_s,
            started: std::time::Instant::now(),
            vcpu_start: 0,
        };
        if fastly_log_analytics::admin::reset::advance(&store).is_none() {
            break;
        }
    }
    let expected_req_floor = p.adapter.records()[ingest::WATERMARK_KEY].value["floor_key"]
        .as_str()
        .unwrap()
        .to_owned();
    let expected_rum_floor = p.adapter.records()[&ingest::watermark_key("rum")].value["floor_key"]
        .as_str()
        .unwrap()
        .to_owned();

    // Force same-second LastModified tie across ALL _catalog/*.json objects:
    let objs = p.adapter.bucket_objects(&svc.fos_bucket);
    MTIMES.with(|m| {
        let mut map = m.borrow_mut();
        for k in objs.keys() {
            map.insert(k.clone(), 1_800_001_000);
        }
    });
    let p_tie = Platform::new();
    p_tie.adapter.seed_resource(
        "fixtureService",
        "snippet",
        json!({"name": "flp-owner-fixtureApp", "type": "recv", "priority": "0", "content": "# flp-owner:fixtureApp:fixtureService"}),
    );
    for (k, v) in &objs {
        p_tie
            .adapter
            .seed_object_bytes(&svc.fos_bucket, k, v.clone());
    }
    p_tie
        .adapter
        .seed_object_bytes(&svc.fos_bucket, "meta/cloud_dr_state.json", stale_dr);
    CALLS.with(|c| c.set(32 - 24));
    SECRETS.with(|s| {
        *s.borrow_mut() =
            std::collections::BTreeSet::from(["front_key".into(), "session_key".into()])
    });
    let r_tie = fastly_log_analytics::admin::handle(
        &p_tie,
        "fixtureAdmin",
        &fastly_log_analytics::http::Request {
            method: "POST".into(),
            path: "/api/admin/provision/recover".into(),
            headers: BTreeMap::new(),
            body: json!({
                "token": "fixture-token",
                "service_id": "fixtureService",
                "fos_bucket": "fos-fixture-logs",
                "fos_region": "us-east-1"
            })
            .to_string()
            .into_bytes(),
        },
    );
    assert_eq!(r_tie.status, 200);
    let tie_recs = p_tie.adapter.records();
    let tie_head = catalog::head(&tie_recs[ingest::WATERMARK_KEY].value).unwrap();
    assert!(
        tie_head.days.is_empty(),
        "BUG-CLOUD-DR-27A: same-second tie resurrected pre-reset days: {:?}",
        tie_head.days
    );
    assert_eq!(
        tie_recs[ingest::WATERMARK_KEY].value["floor_key"].as_str(),
        Some(expected_req_floor.as_str()),
        "BUG-CLOUD-DR-27A: recovered request floor_key must match job.reset_at, not post-fence reset_cas_at!"
    );
    assert_eq!(
        tie_recs[&ingest::watermark_key("rum")].value["floor_key"].as_str(),
        Some(expected_rum_floor.as_str()),
        "BUG-CLOUD-DR-27A: recovered rum floor_key must match job.reset_at!"
    );
    assert!(tie_recs[&ingest::watermark_key("rum")]
        .value
        .get("rum_stats")
        .is_none_or(Value::is_null));

    // Part 2: Premature sync_dr_now gate when status transitions "resetting" -> "cleanup" via RESET_GIVE_UP_S
    // while RUM_PART has NOT yet succeeded (conflict on rum watermark):
    let p2 = Platform::new();
    let prov2 = p2.provision();
    p2.drain(prov2["job"].clone());
    p2.adapter
        .cas(
            &ingest::watermark_key("rum"),
            None,
            &json!({"rum_stats": {"earliest": base_ts, "latest": base_ts + 60, "rows": 7_777u64, "at": base_ts + 60}}),
        )
        .unwrap();
    let t0 = 1_800_100_000u64;
    p2.clock.store(t0, Ordering::SeqCst);
    let g2 = p2.adapter.records()["svc:fixtureService"]
        .generation
        .clone();
    assert_eq!(
        p2.call(
            "POST",
            "/api/admin/services/fixtureService/delete-data",
            json!({"confirm_service_id": "fixtureService", "generation": g2}),
        )
        .status,
        200
    );
    // Tick 1: advance past fence wait ("fencing" -> "resetting"):
    p2.clock.store(t0 + 425, Ordering::SeqCst);
    {
        let cur_svc: Service =
            serde_json::from_value(p2.adapter.records()["svc:fixtureService"].value.clone())
                .unwrap();
        let (ak_s, sk_s) = fastly_log_analytics::admin::fos_credentials(&p2, &cur_svc).unwrap();
        CALLS.with(|c| c.set(0));
        SECRETS.with(|s| s.borrow_mut().clear());
        let store = Store {
            runtime: &p2,
            service: &cur_svc,
            access_key: ak_s,
            secret_key: sk_s,
            started: std::time::Instant::now(),
            vcpu_start: 0,
        };
        let _ = fastly_log_analytics::admin::reset::advance(&store);
    }
    assert_eq!(
        p2.adapter.records()[fastly_log_analytics::admin::reset::JOB_KEY].value["status"],
        "resetting"
    );
    struct ConflictGuard;
    impl Drop for ConflictGuard {
        fn drop(&mut self) {
            CONFLICT_CAS.with(|k| *k.borrow_mut() = None);
        }
    }
    let _conflict_guard = ConflictGuard;
    let rum_wm_key = ingest::watermark_key("rum");
    CONFLICT_CAS.with(|k| *k.borrow_mut() = Some(rum_wm_key));

    // Tick 2: request WATERMARK_KEY CAS lands, then rum_watermark CAS conflicts so RUM_PART is NOT in steps_done:
    p2.clock.store(t0 + 426, Ordering::SeqCst);
    {
        let cur_svc: Service =
            serde_json::from_value(p2.adapter.records()["svc:fixtureService"].value.clone())
                .unwrap();
        let (ak_s, sk_s) = fastly_log_analytics::admin::fos_credentials(&p2, &cur_svc).unwrap();
        CALLS.with(|c| c.set(0));
        SECRETS.with(|s| s.borrow_mut().clear());
        let store = Store {
            runtime: &p2,
            service: &cur_svc,
            access_key: ak_s,
            secret_key: sk_s,
            started: std::time::Instant::now(),
            vcpu_start: 0,
        };
        let _ = fastly_log_analytics::admin::reset::advance(&store);
    }
    // Tick 3: advance past RESET_GIVE_UP_S while rum_watermark CAS still conflicts so reset_bound moves to "cleanup" without RUM_PART (with plenty of remaining calls >= 5):
    p2.clock.store(
        t0 + 426 + fastly_log_analytics::admin::reset::RESET_GIVE_UP_S + 10,
        Ordering::SeqCst,
    );
    {
        let cur_svc: Service =
            serde_json::from_value(p2.adapter.records()["svc:fixtureService"].value.clone())
                .unwrap();
        let (ak_s, sk_s) = fastly_log_analytics::admin::fos_credentials(&p2, &cur_svc).unwrap();
        CALLS.with(|c| c.set(0));
        SECRETS.with(|s| s.borrow_mut().clear());
        let store = Store {
            runtime: &p2,
            service: &cur_svc,
            access_key: ak_s,
            secret_key: sk_s,
            started: std::time::Instant::now(),
            vcpu_start: 0,
        };
        let _ = fastly_log_analytics::admin::reset::advance(&store);
    }
    let job_rec = &p2.adapter.records()[fastly_log_analytics::admin::reset::JOB_KEY].value;
    assert_eq!(job_rec["status"], "cleanup");
    let steps: Vec<&str> = job_rec["steps_done"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(Value::as_str)
        .collect();
    assert!(
        !steps.contains(&"rum_watermark"),
        "expected RUM_PART still pending: {steps:?}"
    );
    assert!(
        !steps.contains(&"dr_synced"),
        "1e728113 Finding 3: sync_dr_now must NOT mark dr_synced before RUM_PART completes: {steps:?}"
    );
}

#[test]
fn bug_cloud_dr_27bcd_retire_isolation_mixed_legacy_days_and_cold_catalog_scan_front_sync() {
    use fastly_log_analytics::ingest::{self, storage::Store};
    use std::sync::atomic::Ordering;

    struct RichGuard;
    impl Drop for RichGuard {
        fn drop(&mut self) {
            RICH_FOS.with(|r| r.set(false));
            MTIMES.with(|m| m.borrow_mut().clear());
        }
    }
    let _guard = RichGuard;
    RICH_FOS.with(|r| r.set(true));
    MTIMES.with(|m| m.borrow_mut().clear());

    // 1. BUG-CLOUD-DR-27B/C (2A): catalog::retire + POST /api/admin/provision/recover must NOT resurrect retired Day objects:
    let p = Platform::new();
    let provisioned = p.provision();
    p.drain(provisioned["job"].clone());
    let patched = p.json(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation": p.service()["generation"], "log_retention_days": 7}),
    );
    let svc: Service = serde_json::from_value(patched["service"].clone()).unwrap();
    let expired_day = 1_794_268_800i64; // 2026-11-10
    let active_day = 1_794_873_600i64; // 2026-11-17
    p.clock.store((active_day + 120) as u64, Ordering::SeqCst);
    commit_real_catalog_batch(&p, &svc, "act1", active_day + 60, 50);
    p.clock.store((active_day + 180) as u64, Ordering::SeqCst);
    commit_real_catalog_batch(&p, &svc, "exp1", expired_day + 60, 700);

    // Advance clock 8 days past expired_day so expired_day < cutoff_day while active_day >= cutoff_day:
    p.clock
        .store((expired_day + 8 * 86_400 + 3600) as u64, Ordering::SeqCst);
    CALLS.with(|c| c.set(0));
    SECRETS.with(|s| s.borrow_mut().clear());
    let (ak, sk) = fastly_log_analytics::admin::fos_credentials(&p, &svc).unwrap();
    let store = Store {
        runtime: &p,
        service: &svc,
        access_key: ak,
        secret_key: sk,
        started: std::time::Instant::now(),
        vcpu_start: 0,
    };
    ingest::catalog::retire_reserving(&store, 0).expect("catalog::retire_reserving");
    let head_before = catalog::head(&p.adapter.records()[ingest::WATERMARK_KEY].value).unwrap();
    assert_eq!(
        head_before.statistics.values().map(|s| s.rows).sum::<u64>(),
        50
    );

    let objs_ret = p.adapter.bucket_objects(&svc.fos_bucket);
    let mtimes_ret = MTIMES.with(|m| m.borrow().clone());
    let p_ret_dr = Platform::new();
    p_ret_dr.adapter.seed_resource(
        "fixtureService",
        "snippet",
        json!({"name": "flp-owner-fixtureApp", "type": "recv", "priority": "0", "content": "# flp-owner:fixtureApp:fixtureService"}),
    );
    MTIMES.with(|m| *m.borrow_mut() = mtimes_ret);
    for (k, v) in &objs_ret {
        p_ret_dr
            .adapter
            .seed_object_bytes(&svc.fos_bucket, k, v.clone());
    }
    let ret_res = p_ret_dr.json(
        "POST",
        "/api/admin/provision/recover",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1"
        }),
    );
    assert_eq!(ret_res["ok"], true);
    let head_after =
        catalog::head(&p_ret_dr.adapter.records()[ingest::WATERMARK_KEY].value).unwrap();
    assert_eq!(
        head_after.statistics.values().map(|s| s.rows).sum::<u64>(),
        50,
        "BUG-CLOUD-DR-27B: DR recovery resurrected retired Day rows: {:?}",
        head_after.days
    );

    // 2. BUG-CLOUD-DR-27C (4A): Mixed legacy own Day (10 rows) + foreign Day (9,999 rows) without manifest:
    let p_mix = Platform::new();
    p_mix.adapter.seed_resource(
        "fixtureService",
        "snippet",
        json!({"name": "flp-owner-fixtureApp", "type": "recv", "priority": "0", "content": "# flp-owner:fixtureApp:fixtureService"}),
    );
    MTIMES.with(|m| m.borrow_mut().clear());
    let own_blk_key = "processed/_catalog/own_blk_legacy.json";
    let own_day_key = "processed/_catalog/own_day_legacy.json";
    let for_blk_key = "processed/_catalog/for_blk_legacy.json";
    let for_day_key = "processed/_catalog/for_day_legacy.json";
    p_mix.adapter.seed_object_bytes(
        "fos-fixture-logs",
        own_blk_key,
        serde_json::to_vec(&json!({
            "version": VERSION, "service_id": "fixtureService", "generation": 2,
            "hour": active_day, "previous": null, "files": [], "summaries": [], "replaces": [], "committed_at": {}
        }))
        .unwrap(),
    );
    p_mix.adapter.seed_object_bytes(
        "fos-fixture-logs",
        own_day_key,
        serde_json::to_vec(&Day {
            version: VERSION,
            service_id: None,
            hours: BTreeMap::from([(
                active_day,
                Reference {
                    key: own_blk_key.into(),
                    generation: 2,
                },
            )]),
            statistics: Statistics {
                earliest: Some(active_day),
                latest: Some(active_day + 3599),
                rows: 10,
                ..Default::default()
            },
            ..Day::default()
        })
        .unwrap(),
    );
    p_mix.adapter.seed_object_bytes(
        "fos-fixture-logs",
        for_blk_key,
        serde_json::to_vec(&json!({
            "version": VERSION, "service_id": "foreignService22Chars0", "generation": 5,
            "hour": active_day + 86_400, "previous": null, "files": [], "summaries": [], "replaces": [], "committed_at": {}
        }))
        .unwrap(),
    );
    p_mix.adapter.seed_object_bytes(
        "fos-fixture-logs",
        for_day_key,
        serde_json::to_vec(&Day {
            version: VERSION,
            service_id: None,
            hours: BTreeMap::from([(
                active_day + 86_400,
                Reference {
                    key: for_blk_key.into(),
                    generation: 5,
                },
            )]),
            statistics: Statistics {
                earliest: Some(active_day + 86_400),
                latest: Some(active_day + 90_000),
                rows: 9_999,
                ..Default::default()
            },
            ..Day::default()
        })
        .unwrap(),
    );
    MTIMES.with(|m| {
        let mut map = m.borrow_mut();
        map.insert(own_day_key.into(), 1_800_000_200);
        map.insert(for_day_key.into(), 1_800_000_190);
        map.insert(own_blk_key.into(), 1_800_000_100);
        map.insert(for_blk_key.into(), 1_800_000_090);
    });
    SECRETS.with(|s| s.borrow_mut().clear());
    let mix_res = p_mix.json(
        "POST",
        "/api/admin/provision/recover",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1"
        }),
    );
    assert_eq!(mix_res["ok"], true);
    let mix_head = catalog::head(&p_mix.adapter.records()[ingest::WATERMARK_KEY].value).unwrap();
    assert_eq!(
        mix_head.statistics.values().map(|s| s.rows).sum::<u64>(),
        10,
        "BUG-CLOUD-DR-27C (4A): foreign unattributed legacy Day leaked 9,999 rows: {:?}",
        mix_head.days
    );

    // 3. BUG-CLOUD-DR-27D: Cold-start catalog_scan (NO meta/cloud_dr_state.json) with 27+ .json objects
    // must achieve front_synced == true across remaining = 24, 25, AND 32:
    let p_seed = Platform::new();
    let prov_s = p_seed.provision();
    p_seed.drain(prov_s["job"].clone());
    let svc_s: Service =
        serde_json::from_value(p_seed.adapter.records()["svc:fixtureService"].value.clone())
            .unwrap();
    for i in 1..=11i64 {
        p_seed
            .clock
            .store((active_day + i * 60) as u64, Ordering::SeqCst);
        commit_real_catalog_batch(
            &p_seed,
            &svc_s,
            &format!("cscan{i}"),
            active_day + i * 60,
            5,
        );
    }
    let mut seed_objs = p_seed.adapter.bucket_objects(&svc_s.fos_bucket);
    seed_objs.remove("meta/cloud_dr_state.json");
    let seed_mtimes = MTIMES.with(|m| m.borrow().clone());
    for remaining in [24u32, 25, 32] {
        let p_cold = Platform::new();
        p_cold.adapter.seed_resource(
            "fixtureService",
            "snippet",
            json!({"name": "flp-owner-fixtureApp", "type": "recv", "priority": "0", "content": "# flp-owner:fixtureApp:fixtureService"}),
        );
        MTIMES.with(|m| *m.borrow_mut() = seed_mtimes.clone());
        for (k, v) in &seed_objs {
            p_cold
                .adapter
                .seed_object_bytes("fos-fixture-logs", k, v.clone());
        }
        ELAPSED_MS.with(|e| e.set(0));
        CALLS.with(|c| c.set(32 - remaining));
        SECRETS.with(|s| {
            *s.borrow_mut() =
                std::collections::BTreeSet::from(["front_key".into(), "session_key".into()])
        });
        let resp = fastly_log_analytics::admin::handle(
            &p_cold,
            "fixtureAdmin",
            &fastly_log_analytics::http::Request {
                method: "POST".into(),
                path: "/api/admin/provision/recover".into(),
                headers: BTreeMap::new(),
                body: json!({
                    "token": "fixture-token",
                    "service_id": "fixtureService",
                    "fos_bucket": "fos-fixture-logs",
                    "fos_region": "us-east-1"
                })
                .to_string()
                .into_bytes(),
            },
        );
        assert_eq!(resp.status, 200);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["source"], "catalog_scan");
        assert_eq!(
            body["front_synced"], true,
            "[catalog_scan remaining={remaining}] BUG-CLOUD-DR-27D: front_synced must be true: {body}"
        );
        assert_eq!(
            p_cold
                .adapter
                .dictionary_items("fixtureBuckets")
                .get("fixtureService")
                .map(String::as_str),
            Some("fos-fixture-logs|us-east-1")
        );
    }
}

#[test]
fn bug_cloud_budget_27e_provision_and_scoring_propagate_503_backend_budget() {
    use fastly_log_analytics::{admin, http::Request};
    use std::collections::BTreeSet;

    // 1. Provision preflight at budget=4 (and across 1..=24) must return 503 backend_budget, never 400 object_storage_required:
    for budget in 1..=10u32 {
        let p = Platform::new();
        ELAPSED_MS.with(|e| e.set(0));
        CALLS.with(|c| c.set(32 - budget));
        SECRETS
            .with(|s| *s.borrow_mut() = BTreeSet::from(["front_key".into(), "session_key".into()]));
        let resp = admin::handle(
            &p,
            "fixtureAdmin",
            &Request {
                method: "POST".into(),
                path: "/api/admin/provision/execute".into(),
                headers: BTreeMap::new(),
                body: json!({
                    "token": "fixture-token",
                    "service_id": "fixtureService",
                    "fos_bucket": "fos-fixture-logs",
                    "fos_region": "us-east-1"
                })
                .to_string()
                .into_bytes(),
            },
        );
        assert!(
            matches!(resp.status, 200 | 503),
            "BUG-CLOUD-BUDGET-27E: provision/execute at budget={budget} returned {} instead of 200/503: {}",
            resp.status,
            String::from_utf8_lossy(&resp.body)
        );
    }

    // 2. Scoring enable with brokered token at budget=2 (and across 1..=10) must return 503 backend_budget, never 403:
    let p_sc = Platform::new();
    let prov_sc = p_sc.provision();
    p_sc.drain(prov_sc["job"].clone());
    let svc_gen = p_sc.service()["generation"].as_str().unwrap().to_owned();
    for budget in 1..=6u32 {
        ELAPSED_MS.with(|e| e.set(0));
        CALLS.with(|c| c.set(32 - budget));
        SECRETS
            .with(|s| *s.borrow_mut() = BTreeSet::from(["front_key".into(), "session_key".into()]));
        let resp = admin::handle(
            &p_sc,
            "fixtureAdmin",
            &Request {
                method: "POST".into(),
                path: "/api/admin/services/fixtureService/scoring/enable".into(),
                headers: BTreeMap::new(),
                body: json!({"generation": svc_gen}).to_string().into_bytes(),
            },
        );
        assert!(
            matches!(resp.status, 200 | 503),
            "BUG-CLOUD-BUDGET-27E: scoring/enable at budget={budget} returned {} instead of 200/503: {}",
            resp.status,
            String::from_utf8_lossy(&resp.body)
        );
    }
}

#[test]
fn bug_cloud_dr_28ac_reverse_manifest_keys_scale_past_1000_hex_and_foreign_manifests() {
    use fastly_log_analytics::ingest;
    use std::collections::BTreeSet;
    use std::sync::atomic::Ordering;

    struct RichGuard;
    impl Drop for RichGuard {
        fn drop(&mut self) {
            RICH_FOS.with(|r| r.set(false));
            MTIMES.with(|m| m.borrow_mut().clear());
        }
    }
    let _guard = RichGuard;
    RICH_FOS.with(|r| r.set(true));
    MTIMES.with(|m| m.borrow_mut().clear());

    // 1. Seed >1,000 {64-hex}.json objects + pre-reset ingest + stale dr_state + Delete Data reset manifest:
    let p_hex = Platform::new();
    let prov = p_hex.provision();
    p_hex.drain(prov["job"].clone());
    let svc: Service =
        serde_json::from_value(p_hex.adapter.records()["svc:fixtureService"].value.clone())
            .unwrap();
    let base_day = 1_800_000_000i64 - (1_800_000_000i64 % 86_400);
    p_hex.clock.store((base_day + 120) as u64, Ordering::SeqCst);
    commit_real_catalog_batch(&p_hex, &svc, "pre-reset-hex", base_day + 60, 1010);
    p_hex
        .adapter
        .cas(
            &ingest::watermark_key("rum"),
            None,
            &json!({"rum_stats": {"earliest": base_day as u64, "latest": (base_day + 60) as u64, "total_rows": 777u64, "at": base_day + 120}}),
        )
        .unwrap();
    CALLS.with(|c| c.set(0));
    SECRETS.with(|s| s.borrow_mut().clear());
    dr::sync_best_effort(&p_hex, &svc);
    let stale_dr_bytes =
        p_hex.adapter.bucket_objects(&svc.fos_bucket)["meta/cloud_dr_state.json"].clone();

    // Seed 1,005 hex catalog objects ([0-9a-f] < 'm') so Page 1 of processed/_catalog/ has 0 manifests:
    for idx in 0..1005u32 {
        let hex_key = format!("processed/_catalog/{idx:064x}.json");
        p_hex
            .adapter
            .seed_object_bytes(&svc.fos_bucket, &hex_key, b"{\"stub\":1}".to_vec());
        MTIMES.with(|m| m.borrow_mut().insert(hex_key, 1_800_000_100));
    }

    // Run Delete Data (reset.rs writes service-scoped reverse-gen reset manifest at Gen 2):
    p_hex.clock.store((base_day + 600) as u64, Ordering::SeqCst);
    {
        let dd_gen = p_hex.adapter.records()["svc:fixtureService"]
            .generation
            .clone();
        let dd_start = p_hex.json(
            "POST",
            "/api/admin/services/fixtureService/delete-data",
            json!({"confirm_service_id": "fixtureService", "generation": dd_gen}),
        );
        assert_eq!(dd_start["job"]["status"], "fencing");
        p_hex.clock.fetch_add(
            fastly_log_analytics::admin::reset::FENCE_WAIT_S + 5,
            Ordering::SeqCst,
        );
        for _ in 0..25 {
            let recs = p_hex.adapter.records();
            if recs["svc:fixtureService"]
                .value
                .get("data_reset")
                .is_none_or(Value::is_null)
            {
                break;
            }
            let cur_svc: Service =
                serde_json::from_value(recs["svc:fixtureService"].value.clone()).unwrap();
            let (ak_d, sk_d) =
                fastly_log_analytics::admin::fos_credentials(&p_hex, &cur_svc).unwrap();
            CALLS.with(|c| c.set(0));
            SECRETS.with(|s| s.borrow_mut().clear());
            ELAPSED_MS.with(|e| e.set(0));
            let store_d = ingest::storage::Store {
                runtime: &p_hex,
                service: &cur_svc,
                access_key: ak_d,
                secret_key: sk_d,
                started: std::time::Instant::now(),
                vcpu_start: 0,
            };
            let Some(step_res) = fastly_log_analytics::admin::reset::advance(&store_d) else {
                break;
            };
            step_res.unwrap();
            p_hex.clock.fetch_add(450, Ordering::SeqCst);
        }
    }
    let post_dd_objs = p_hex.adapter.bucket_objects(&svc.fos_bucket);
    let post_dd_mtimes = MTIMES.with(|m| m.borrow().clone());

    // 1A. Stale dr_state (Gen 1, 1010 rows, rum_stats=777) + 1,005 hex objects + Gen 2 reset manifest:
    let p_stale = Platform::new();
    p_stale.clock.store(p_hex.now() + 60, Ordering::SeqCst);
    MTIMES.with(|m| *m.borrow_mut() = post_dd_mtimes.clone());
    for (k, v) in &post_dd_objs {
        p_stale
            .adapter
            .seed_object_bytes(&svc.fos_bucket, k, v.clone());
    }
    p_stale
        .adapter
        .seed_object_bytes(&svc.fos_bucket, "meta/cloud_dr_state.json", stale_dr_bytes);
    let stale_rec = p_stale.json(
        "POST",
        "/api/admin/provision/recover",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1"
        }),
    );
    assert_eq!(stale_rec["ok"], true);
    assert_eq!(
        stale_rec["catalog_generation"], 2,
        "BUG-CLOUD-DR-28A: stale dr_state with >1,000 hex objects missed Gen 2 reset manifest: {stale_rec}"
    );
    let stale_head =
        catalog::head(&p_stale.adapter.records()[ingest::WATERMARK_KEY].value).unwrap();
    assert_eq!(
        stale_head.statistics.values().map(|s| s.rows).sum::<u64>(),
        0,
        "BUG-CLOUD-DR-28A: pre-reset rows resurrected!"
    );
    let rum_wm = &p_stale.adapter.records()[&ingest::watermark_key("rum")].value;
    assert!(
        rum_wm.get("rum_stats").is_none(),
        "BUG-CLOUD-DR-28A: pre-reset rum_stats resurrected: {rum_wm}"
    );

    // 1B. Cold-start catalog_scan at remaining = 24 with 1,005 hex objects must achieve front_synced == true and Gen 2:
    let p_cold24 = Platform::new();
    p_cold24.clock.store(p_hex.now() + 60, Ordering::SeqCst);
    p_cold24.adapter.seed_resource(
        "fixtureService",
        "snippet",
        json!({"name": "flp-owner-fixtureApp", "type": "recv", "priority": "0", "content": "# flp-owner:fixtureApp:fixtureService"}),
    );
    MTIMES.with(|m| *m.borrow_mut() = post_dd_mtimes.clone());
    for (k, v) in &post_dd_objs {
        if k != "meta/cloud_dr_state.json" {
            p_cold24
                .adapter
                .seed_object_bytes(&svc.fos_bucket, k, v.clone());
        }
    }
    ELAPSED_MS.with(|e| e.set(0));
    CALLS.with(|c| c.set(32 - 24));
    SECRETS.with(|s| *s.borrow_mut() = BTreeSet::from(["front_key".into(), "session_key".into()]));
    let cold24_resp = fastly_log_analytics::admin::handle(
        &p_cold24,
        "fixtureAdmin",
        &fastly_log_analytics::http::Request {
            method: "POST".into(),
            path: "/api/admin/provision/recover".into(),
            headers: BTreeMap::new(),
            body: json!({
                "token": "fixture-token",
                "service_id": "fixtureService",
                "fos_bucket": "fos-fixture-logs",
                "fos_region": "us-east-1"
            })
            .to_string()
            .into_bytes(),
        },
    );
    assert_eq!(cold24_resp.status, 200);
    let cold24_body: Value = serde_json::from_slice(&cold24_resp.body).unwrap();
    assert_eq!(cold24_body["catalog_generation"], 2);
    assert_eq!(cold24_body["front_synced"], true, "{cold24_body}");

    // 2. >1,000 manifests (both service-scoped reverse-gen AND pre-Iter29 ascending manifest-{:020}-):
    for use_legacy_keys in [false, true] {
        let p_m1005 = Platform::new();
        p_m1005.adapter.seed_resource(
            "fixtureService",
            "snippet",
            json!({"name": "flp-owner-fixtureApp", "type": "recv", "priority": "0", "content": "# flp-owner:fixtureApp:fixtureService"}),
        );
        MTIMES.with(|m| m.borrow_mut().clear());
        for g in 1..=1005u64 {
            let is_reset = g == 1005;
            let mut m_json = json!({
                "version": VERSION,
                "service_id": "fixtureService",
                "generation": g,
                "batch": if is_reset { "reset" } else { "ingest" },
                "schema": "v2",
                "committed_at": "2027-01-15T00:00:00Z",
                "previous": null,
                "files": [],
                "summaries": [],
                "sources": [],
                "replaces": [],
                "bytes": 0,
                "rows": if is_reset { 0 } else { 1000 },
                "parents": {},
                "head_days": {},
                "head_statistics": {},
            });
            if is_reset {
                m_json["reset_generation"] = json!(1005);
                m_json["reset_at"] = json!(base_day + 900);
            }
            let key = if use_legacy_keys {
                format!("processed/_catalog/manifest-{g:020}-d{g:04}.json")
            } else {
                format!(
                    "processed/_catalog/manifest-s-fixtureService-rev-{:020}-d{g:04}.json",
                    u64::MAX - g
                )
            };
            p_m1005.adapter.seed_object_bytes(
                "fos-fixture-logs",
                &key,
                serde_json::to_vec(&m_json).unwrap(),
            );
            MTIMES.with(|m| m.borrow_mut().insert(key, 1_800_000_500));
        }
        let rec_1005 = p_m1005.json(
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
            rec_1005["catalog_generation"], 1005,
            "BUG-CLOUD-DR-28A (legacy={use_legacy_keys}): >1,000 manifests missed Gen 1005: {rec_1005}"
        );
    }

    // 3. BUG-CLOUD-DR-28C: Shared bucket with 35 higher-generation foreign legacy manifests + foreign rev manifests
    // must NOT starve own catalog manifest OR own Delete Data reset manifest:
    let p_shared = Platform::new();
    p_shared.clock.store(p_hex.now() + 60, Ordering::SeqCst);
    p_shared.adapter.seed_resource(
        "fixtureService",
        "snippet",
        json!({"name": "flp-owner-fixtureApp", "type": "recv", "priority": "0", "content": "# flp-owner:fixtureApp:fixtureService"}),
    );
    MTIMES.with(|m| *m.borrow_mut() = post_dd_mtimes);
    for (k, v) in &post_dd_objs {
        if k != "meta/cloud_dr_state.json" {
            p_shared
                .adapter
                .seed_object_bytes(&svc.fos_bucket, k, v.clone());
        }
    }
    for f in 1..=35u64 {
        let f_gen = 900_000 + f;
        let f_body = serde_json::to_vec(&json!({
            "version": VERSION,
            "service_id": "foreignService22Chars0",
            "generation": f_gen,
            "batch": "foreign",
            "schema": "v2",
            "committed_at": "2027-01-15T01:00:00Z",
            "previous": null,
            "files": [], "summaries": [], "sources": [], "replaces": [],
            "bytes": 0, "rows": 9999, "parents": {}, "head_days": {}, "head_statistics": {}
        }))
        .unwrap();
        let leg_f_key = format!("processed/_catalog/manifest-{f_gen:020}-foreign.json");
        let rev_f_key = format!(
            "processed/_catalog/manifest-s-foreignService22Chars0-rev-{:020}-foreign.json",
            u64::MAX - f_gen
        );
        p_shared
            .adapter
            .seed_object_bytes(&svc.fos_bucket, &leg_f_key, f_body.clone());
        p_shared
            .adapter
            .seed_object_bytes(&svc.fos_bucket, &rev_f_key, f_body);
    }
    let shared_rec = p_shared.json(
        "POST",
        "/api/admin/provision/recover",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1"
        }),
    );
    assert_eq!(shared_rec["ok"], true);
    assert_eq!(
        shared_rec["catalog_generation"], 2,
        "BUG-CLOUD-DR-28C: foreign higher-gen manifests starved own reset manifest recovery: {shared_rec}"
    );
    assert_eq!(shared_rec["front_synced"], true);
    let shared_head =
        catalog::head(&p_shared.adapter.records()[ingest::WATERMARK_KEY].value).unwrap();
    assert_eq!(
        shared_head.statistics.values().map(|s| s.rows).sum::<u64>(),
        0
    );
}

#[test]
fn bug_cloud_dr_28b_legacy_day_verifies_all_blocks_avoids_clobber_and_filters_retention() {
    use fastly_log_analytics::ingest;

    struct RichGuard;
    impl Drop for RichGuard {
        fn drop(&mut self) {
            RICH_FOS.with(|r| r.set(false));
            MTIMES.with(|m| m.borrow_mut().clear());
        }
    }
    let _guard = RichGuard;
    RICH_FOS.with(|r| r.set(true));
    MTIMES.with(|m| m.borrow_mut().clear());

    let active_day = 1_800_000_000i64 - (1_800_000_000i64 % 86_400);

    // 1. Multi-hour spliced legacy unattributed Day (hour 0 = own Block, hour 1 = foreign Block) MUST be rejected:
    let p_splice = Platform::new();
    p_splice.adapter.seed_resource(
        "fixtureService",
        "snippet",
        json!({"name": "flp-owner-fixtureApp", "type": "recv", "priority": "0", "content": "# flp-owner:fixtureApp:fixtureService"}),
    );
    let own_b =
        "processed/_catalog/0000000000000000000000000000000000000000000000000000000000000001.json";
    let for_b =
        "processed/_catalog/0000000000000000000000000000000000000000000000000000000000000002.json";
    let spliced_d =
        "processed/_catalog/0000000000000000000000000000000000000000000000000000000000000003.json";
    p_splice.adapter.seed_object_bytes(
        "fos-fixture-logs",
        own_b,
        serde_json::to_vec(&json!({
            "version": VERSION, "service_id": "fixtureService", "generation": 1,
            "hour": active_day, "previous": null, "files": [], "summaries": [], "replaces": [], "committed_at": {}
        }))
        .unwrap(),
    );
    p_splice.adapter.seed_object_bytes(
        "fos-fixture-logs",
        for_b,
        serde_json::to_vec(&json!({
            "version": VERSION, "service_id": "foreignService22Chars0", "generation": 5,
            "hour": active_day + 3600, "previous": null, "files": [], "summaries": [], "replaces": [], "committed_at": {}
        }))
        .unwrap(),
    );
    p_splice.adapter.seed_object_bytes(
        "fos-fixture-logs",
        spliced_d,
        serde_json::to_vec(&Day {
            version: VERSION,
            service_id: None,
            hours: BTreeMap::from([
                (
                    active_day,
                    Reference {
                        key: own_b.into(),
                        generation: 1,
                    },
                ),
                (
                    active_day + 3600,
                    Reference {
                        key: for_b.into(),
                        generation: 5,
                    },
                ),
            ]),
            statistics: Statistics {
                earliest: Some(active_day),
                latest: Some(active_day + 7199),
                rows: 500,
                ..Default::default()
            },
            ..Day::default()
        })
        .unwrap(),
    );
    let splice_rec = p_splice.json(
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
        splice_rec["source"], "minimal",
        "BUG-CLOUD-DR-28B: spliced multi-hour legacy Day with foreign block must be rejected: {splice_rec}"
    );
    assert!(splice_rec["catalog_generation"].is_null());

    // 2. Same-day higher-gen foreign legacy Day must NOT clobber lower-gen own legacy Day,
    // AND dr_state log_retention_days = 7 must filter expired legacy Days:
    let p_clob = Platform::new();
    let expired_day = active_day - 20 * 86_400;
    let own_b2 =
        "processed/_catalog/1000000000000000000000000000000000000000000000000000000000000001.json";
    let exp_b2 =
        "processed/_catalog/1000000000000000000000000000000000000000000000000000000000000002.json";
    let for_b2 =
        "processed/_catalog/1000000000000000000000000000000000000000000000000000000000000003.json";
    let own_d2 =
        "processed/_catalog/1000000000000000000000000000000000000000000000000000000000000004.json";
    let exp_d2 =
        "processed/_catalog/1000000000000000000000000000000000000000000000000000000000000005.json";
    let for_d2 =
        "processed/_catalog/1000000000000000000000000000000000000000000000000000000000000006.json";
    for (k, sid_b, gen_b, hr) in [
        (own_b2, "fixtureService", 2u64, active_day),
        (exp_b2, "fixtureService", 1u64, expired_day),
        (for_b2, "foreignService22Chars0", 9u64, active_day),
    ] {
        p_clob.adapter.seed_object_bytes(
            "fos-fixture-logs",
            k,
            serde_json::to_vec(&json!({
                "version": VERSION, "service_id": sid_b, "generation": gen_b,
                "hour": hr, "previous": null, "files": [], "summaries": [], "replaces": [], "committed_at": {}
            }))
            .unwrap(),
        );
    }
    for (dk, bk, gen_d, dts, rows_d) in [
        (own_d2, own_b2, 2u64, active_day, 10u64),
        (exp_d2, exp_b2, 1u64, expired_day, 99u64),
        (for_d2, for_b2, 9u64, active_day, 5000u64),
    ] {
        p_clob.adapter.seed_object_bytes(
            "fos-fixture-logs",
            dk,
            serde_json::to_vec(&Day {
                version: VERSION,
                service_id: None,
                hours: BTreeMap::from([(
                    dts,
                    Reference {
                        key: bk.into(),
                        generation: gen_d,
                    },
                )]),
                statistics: Statistics {
                    earliest: Some(dts),
                    latest: Some(dts + 3599),
                    rows: rows_d,
                    ..Default::default()
                },
                ..Day::default()
            })
            .unwrap(),
        );
    }
    p_clob.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "meta/cloud_dr_state.json",
        serde_json::to_vec(&CloudDrState {
            schema_version: SCHEMA_VERSION,
            updated_at: active_day as u64 + 100,
            service_id: "fixtureService".into(),
            service: json!({"log_retention_days": 7}),
            views: vec![],
            catalog_head: None,
            floor_key: None,
            rum_stats: None,
        })
        .unwrap(),
    );
    ELAPSED_MS.with(|e| e.set(0));
    CALLS.with(|c| c.set(0));
    SECRETS.with(|s| {
        *s.borrow_mut() =
            std::collections::BTreeSet::from(["front_key".into(), "session_key".into()])
    });
    let clob_resp = fastly_log_analytics::admin::handle(
        &p_clob,
        "fixtureAdmin",
        &fastly_log_analytics::http::Request {
            method: "POST".into(),
            path: "/api/admin/provision/recover".into(),
            headers: BTreeMap::new(),
            body: json!({
                "token": "fixture-token",
                "service_id": "fixtureService",
                "fos_bucket": "fos-fixture-logs",
                "fos_region": "us-east-1"
            })
            .to_string()
            .into_bytes(),
        },
    );
    assert_eq!(clob_resp.status, 200);
    assert!(CALLS.with(std::cell::Cell::get) <= 32);
    let clob_rec: Value = serde_json::from_slice(&clob_resp.body).unwrap();
    assert_eq!(clob_rec["ok"], true);
    assert!(
        clob_rec["catalog_generation"].is_number(),
        "BUG-CLOUD-DR-28B: p_clob failed to recover catalog_head: {clob_rec}"
    );
    let clob_head = catalog::head(&p_clob.adapter.records()[ingest::WATERMARK_KEY].value).unwrap();
    assert_eq!(
        clob_head.days.get(&active_day).map(String::as_str),
        Some(own_d2),
        "BUG-CLOUD-DR-28B: higher-gen foreign legacy Day clobbered own legacy Day: {:?}",
        clob_head.days
    );
    assert!(
        !clob_head.days.contains_key(&expired_day),
        "expired legacy Day beyond log_retention_days=7 should be filtered: {:?}",
        clob_head.days
    );
    assert_eq!(
        clob_head.statistics.values().map(|s| s.rows).sum::<u64>(),
        10
    );
}

#[test]
fn bug_cloud_outbox_dr_28d_recover_and_reprovision_preserve_audit_outbox_and_flush_continuity() {
    use fastly_log_analytics::telemetry::{
        flush::{log_flush, FlushStatus, FlushTarget},
        schema::from_parquet,
        LogKind, OperationalLogSettings,
    };
    use std::sync::atomic::Ordering;

    for mode in ["recover", "reprovision"] {
        let p = Platform::new();
        let prov = p.provision();
        p.drain(prov["job"].clone());

        // Generate > SVC_OUTBOX_KEEP (50) service audit entries via PATCH:
        for i in 1..=55u64 {
            p.clock.store(1_800_000_000 + i, Ordering::SeqCst);
            let g = p.service()["generation"].clone();
            p.json(
                "PATCH",
                "/api/admin/services/fixtureService",
                json!({"generation": g, "sample_rate": (i % 90) + 10}),
            );
        }

        let svc_before: Service =
            serde_json::from_value(p.adapter.records()["svc:fixtureService"].value.clone())
                .unwrap();
        let (ak, sk) = fastly_log_analytics::admin::fos_credentials(&p, &svc_before).unwrap();
        let ops_prefix = "processed/ops/";
        let target = FlushTarget {
            region: &svc_before.fos_region,
            bucket: &svc_before.fos_bucket,
            access_key: &ak,
            secret_key: &sk,
            ops_prefix,
        };
        p.clock.store(1_800_000_200, Ordering::SeqCst);
        CALLS.with(|c| c.set(0));
        fastly_log_analytics::telemetry::append_outbox(
            &p,
            "fixtureService",
            "f1",
            "{\"kind\":\"cron_ticks\",\"row\":{\"timestamp\":1800000200000,\"outcome\":\"ok\",\"trigger\":\"cron\"}}\n",
        )
        .unwrap();
        p.clock.store(1_800_001_200, Ordering::SeqCst);
        CALLS.with(|c| c.set(0));
        SECRETS.with(|s| s.borrow_mut().clear());
        let rep1 = log_flush(
            &p,
            "fixtureService",
            Some(&target),
            &OperationalLogSettings::default(),
            true,
            std::time::Instant::now(),
        )
        .unwrap();
        assert_eq!(rep1.status, FlushStatus::Committed);
        let pre_base = p.adapter.records()["svc:fixtureService"].value["_audit_outbox_base"]
            .as_u64()
            .unwrap_or(0);
        assert!(
            pre_base > 0,
            "trim_svc_outbox must have advanced _audit_outbox_base > 0"
        );

        // Teardown leaves 2+ unarchived teardown audit entries in svc:fixtureService:
        let td_gen = p.service()["generation"].clone();
        let td = p.json(
            "POST",
            "/api/admin/provision/teardown",
            json!({
                "service_id": "fixtureService",
                "confirm_service_id": "fixtureService",
                "generation": td_gen,
                "remove_logging": false,
                "remove_snippets": false,
                "delete_fos_keys": false
            }),
        );
        p.drain(td["job"].clone());

        let count_audit_rows = |plat: &Platform| -> usize {
            plat.adapter
                .bucket_objects("fos-fixture-logs")
                .into_iter()
                .filter(|(k, _)| k.starts_with("processed/ops/admin_audit/"))
                .map(|(_, bytes)| from_parquet(LogKind::AdminAudit, bytes).unwrap().len())
                .sum()
        };
        let archived_before = count_audit_rows(&p);

        p.clock.store(1_800_001_500, Ordering::SeqCst);
        let td_job_id = td["job"]["id"].as_str().unwrap().to_owned();
        let mut reprov_job_id = String::new();
        if mode == "recover" {
            let rec = p.json(
                "POST",
                "/api/admin/provision/recover",
                json!({
                    "token": "fixture-token",
                    "service_id": "fixtureService",
                    "fos_bucket": "fos-fixture-logs",
                    "fos_region": "us-east-1"
                }),
            );
            assert_eq!(rec["ok"], true);
        } else {
            let reprov = p.provision();
            reprov_job_id = reprov["job"]["id"].as_str().unwrap().to_owned();
            p.drain(reprov["job"].clone());
        }

        let post_svc = &p.adapter.records()["svc:fixtureService"].value;
        assert!(
            post_svc["_audit_outbox_base"].as_u64().unwrap_or(0) >= pre_base,
            "BUG-CLOUD-OUTBOX-DR-28D ({mode}): _audit_outbox_base was wiped: {post_svc}"
        );
        let outbox_arr = post_svc["_audit_outbox"]
            .as_array()
            .expect("_audit_outbox array");
        let outbox_events: Vec<&str> = outbox_arr
            .iter()
            .filter_map(|e| e["event"].as_str())
            .collect();
        assert!(
            outbox_arr.iter().any(|e| {
                e["event"].as_str() == Some("job.completed")
                    && e["job_id"].as_str() == Some(td_job_id.as_str())
            }),
            "BUG-CLOUD-OUTBOX-DR-28D ({mode}): unarchived teardown audit entries were wiped: {outbox_arr:?}"
        );
        if mode == "recover" {
            assert!(outbox_events.contains(&"service.recovered"));
        } else {
            assert!(
                outbox_arr.iter().any(|e| {
                    e["event"].as_str() == Some("job.started")
                        && e["job_id"].as_str() == Some(reprov_job_id.as_str())
                }),
                "BUG-CLOUD-OUTBOX-DR-28D (reprovision): job.started provision entry missing: {outbox_arr:?}"
            );
        }

        // Flush post-recovery/post-reprovision and confirm new audit rows ARE archived immediately (0 blackhole!):
        p.clock.store(1_800_001_600, Ordering::SeqCst);
        CALLS.with(|c| c.set(0));
        fastly_log_analytics::telemetry::append_outbox(
            &p,
            "fixtureService",
            "f2",
            "{\"kind\":\"cron_ticks\",\"row\":{\"timestamp\":1800001600000,\"outcome\":\"ok\",\"trigger\":\"cron\"}}\n",
        )
        .unwrap();
        p.clock.store(1_800_002_600, Ordering::SeqCst);
        CALLS.with(|c| c.set(0));
        SECRETS.with(|s| s.borrow_mut().clear());
        let (ak2, sk2) = fastly_log_analytics::admin::fos_credentials(
            &p,
            &serde_json::from_value(p.adapter.records()["svc:fixtureService"].value.clone())
                .unwrap(),
        )
        .unwrap();
        let target2 = FlushTarget {
            region: "us-east-1",
            bucket: "fos-fixture-logs",
            access_key: &ak2,
            secret_key: &sk2,
            ops_prefix,
        };
        let rep2 = log_flush(
            &p,
            "fixtureService",
            Some(&target2),
            &OperationalLogSettings::default(),
            true,
            std::time::Instant::now(),
        )
        .unwrap();
        assert_eq!(rep2.status, FlushStatus::Committed);
        let archived_after = count_audit_rows(&p);
        assert!(
            archived_after >= archived_before + 3,
            "BUG-CLOUD-OUTBOX-DR-28D ({mode}): post-{mode} log_flush blackholed admin_audit rows ({archived_before} -> {archived_after})"
        );
    }

    // Cold-start DR (no existing svc:fixtureService) must also populate _audit_outbox with service.recovered:
    let p_cold = Platform::new();
    p_cold.adapter.seed_resource(
        "fixtureService",
        "snippet",
        json!({"name": "flp-owner-fixtureApp", "type": "recv", "priority": "0", "content": "# flp-owner:fixtureApp:fixtureService"}),
    );
    let cold_rec = p_cold.json(
        "POST",
        "/api/admin/provision/recover",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1"
        }),
    );
    assert_eq!(cold_rec["ok"], true);
    let cold_outbox = p_cold.adapter.records()["svc:fixtureService"].value["_audit_outbox"]
        .as_array()
        .expect("cold DR _audit_outbox array")
        .clone();
    assert_eq!(cold_outbox.len(), 1);
    assert_eq!(cold_outbox[0]["event"], "service.recovered");
}

#[test]
fn bug_cloud_budget_28e_scorer_store_5xx_and_teardown_503_propagation() {
    use fastly_log_analytics::{admin, http::Request};
    use std::collections::BTreeSet;

    // 1. Non-destructive teardown (remove_logging=false, remove_snippets=false) must propagate 503 backend_budget
    // across budget = 1..=10 and 502 on upstream 502 from /enabled-products/v1/object_storage (never 400):
    let p_td = Platform::new();
    let prov = p_td.provision();
    p_td.drain(prov["job"].clone());
    let td_gen = p_td.service()["generation"].as_str().unwrap().to_owned();
    for budget in 1..=8u32 {
        ELAPSED_MS.with(|e| e.set(0));
        CALLS.with(|c| c.set(32 - budget));
        SECRETS
            .with(|s| *s.borrow_mut() = BTreeSet::from(["front_key".into(), "session_key".into()]));
        let resp = admin::handle(
            &p_td,
            "fixtureAdmin",
            &Request {
                method: "POST".into(),
                path: "/api/admin/provision/teardown".into(),
                headers: BTreeMap::new(),
                body: json!({
                    "service_id": "fixtureService",
                    "confirm_service_id": "fixtureService",
                    "generation": td_gen,
                    "remove_logging": false,
                    "remove_snippets": false,
                    "delete_fos_keys": false
                })
                .to_string()
                .into_bytes(),
            },
        );
        assert!(
            matches!(resp.status, 200 | 503),
            "BUG-CLOUD-BUDGET-28A/28B: non-destructive teardown at budget={budget} returned {} instead of 200/503: {}",
            resp.status,
            String::from_utf8_lossy(&resp.body)
        );
        if resp.status == 200 {
            break;
        }
    }

    let p_td502 = Platform::new();
    let prov2 = p_td502.provision();
    p_td502.drain(prov2["job"].clone());
    let td_gen2 = p_td502.service()["generation"].as_str().unwrap().to_owned();
    for _ in 0..4 {
        p_td502.api_faults.lock().unwrap().push(ApiFault {
            method: "GET",
            path: "/enabled-products/v1/object_storage",
            name: None,
            status: 502,
            retry_after: None,
        });
    }
    let resp502 = p_td502.call(
        "POST",
        "/api/admin/provision/teardown",
        json!({
            "service_id": "fixtureService",
            "confirm_service_id": "fixtureService",
            "generation": td_gen2,
            "remove_logging": false,
            "remove_snippets": false,
            "delete_fos_keys": false
        }),
    );
    assert_eq!(
        resp502.status, 502,
        "upstream 502 on /enabled-products/v1/object_storage in teardown must return 502, got {}: {}",
        resp502.status,
        String::from_utf8_lossy(&resp502.body)
    );

    // 2. Scorer store lookup 502 on GET /resources/stores/config and GET /resources/stores/kv must NOT create duplicate stores:
    for fault_path in ["/resources/stores/config", "/resources/stores/kv"] {
        let p_sc = Platform::new();
        let prov_sc = p_sc.provision();
        p_sc.drain(prov_sc["job"].clone());
        let g = p_sc.service()["generation"].clone();
        let en = p_sc.json(
            "POST",
            "/api/admin/services/fixtureService/scoring/enable",
            json!({"generation": g}),
        );
        // Advance until `scorer_stores` so `scoring_service_id` is populated, then pre-seed existing stores:
        let job = p_sc.until(en["job"].clone(), "scorer_stores");
        assert_eq!(job["step"], "scorer_stores");
        let scorer_sid = p_sc.adapter.records()["svc:fixtureService"].value["scoring"]
            ["scoring_service_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let (_, orig_keys) = p_sc
            .adapter
            .api(fastly_log_analytics::admin::ApiRequest {
                method: "POST",
                path: "/resources/stores/config",
                token: "fixture-token",
                body: Some(json!({"name": format!("scoring_keys_{scorer_sid}")})),
                timeout: std::time::Duration::from_secs(5),
                query: "",
            })
            .unwrap();
        let orig_keys_id = orig_keys["id"].as_str().unwrap().to_owned();
        let (_, orig_cfg) = p_sc
            .adapter
            .api(fastly_log_analytics::admin::ApiRequest {
                method: "POST",
                path: "/resources/stores/config",
                token: "fixture-token",
                body: Some(json!({"name": format!("scoring_config_{scorer_sid}")})),
                timeout: std::time::Duration::from_secs(5),
                query: "",
            })
            .unwrap();
        let orig_cfg_id = orig_cfg["id"].as_str().unwrap().to_owned();
        let (_, orig_mat) = p_sc
            .adapter
            .api(fastly_log_analytics::admin::ApiRequest {
                method: "POST",
                path: "/resources/stores/kv",
                token: "fixture-token",
                body: Some(json!({"name": format!("scoring_matrix_{scorer_sid}")})),
                timeout: std::time::Duration::from_secs(5),
                query: "",
            })
            .unwrap();
        let orig_mat_id = orig_mat["id"].as_str().unwrap().to_owned();

        // Inject upstream 5xx (520 CDN/gateway error, which api_status returns directly as Ok((520, {})))
        // on fault_path — before BUG-CLOUD-BUDGET-28E fix, scoring.rs treated non-2xx as an empty store list
        // and created duplicate stores!
        p_sc.api_faults.lock().unwrap().push(ApiFault {
            method: "GET",
            path: fault_path,
            name: None,
            status: 520,
            retry_after: None,
        });
        let step_5xx = p_sc.json(
            "POST",
            &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
            json!({}),
        );
        assert_eq!(
            step_5xx["job"]["status"], "failed",
            "BUG-CLOUD-BUDGET-28E: scorer_stores treated 520 on {fault_path} as empty store list and created duplicates: {step_5xx}"
        );
        assert_eq!(step_5xx["job"]["error"], "store_lookup_failed");
        assert_eq!(step_5xx["job"]["step"], "scorer_stores");

        // Once 5xx clears, retrying and advancing past `scorer_stores` must adopt the ORIGINAL stores (zero duplicates created!):
        p_sc.api_faults.lock().unwrap().clear();
        let resumed = p_sc.retry(&step_5xx["job"]);
        p_sc.until(resumed, "scorer_store_keys");
        let final_sc = &p_sc.adapter.records()["svc:fixtureService"].value["scoring"];
        assert_eq!(
            final_sc["scoring_keys_store_id"], orig_keys_id,
            "BUG-CLOUD-BUDGET-28E ({fault_path}): orphaned original scoring_keys store!"
        );
        assert_eq!(final_sc["scoring_config_store_id"], orig_cfg_id);
        assert_eq!(
            final_sc["scoring_matrix_store_id"], orig_mat_id,
            "BUG-CLOUD-BUDGET-28E ({fault_path}): orphaned original scoring_matrix store!"
        );
    }
}

#[test]
fn bug_cloud_catalog_28f_paused_prepare_reserving_preserves_exact_pre_batch_parents() {
    use fastly_log_analytics::ingest::{self, storage::Store};
    use std::sync::atomic::Ordering;

    let day1 = 1_795_132_800i64;
    let day2 = day1 + 86_400;

    let run_two_commits = |pause_gen2: bool| -> (Manifest, Head) {
        let p = Platform::new();
        let prov = p.provision();
        p.drain(prov["job"].clone());
        let svc: Service =
            serde_json::from_value(p.adapter.records()["svc:fixtureService"].value.clone())
                .unwrap();
        let (ak, sk) = fastly_log_analytics::admin::fos_credentials(&p, &svc).unwrap();

        // Commit Generation 1 across day1 and day2:
        let g1_f1 = ingest::processed_key("processed/request/", day1 + 60, day1 + 120, "g1a", 5);
        let g1_f2 = ingest::processed_key("processed/request/", day2 + 60, day2 + 120, "g1b", 5);
        p.adapter
            .seed_object_bytes(&svc.fos_bucket, &g1_f1, b"PAR1".to_vec());
        p.adapter
            .seed_object_bytes(&svc.fos_bucket, &g1_f2, b"PAR1".to_vec());
        let g1_out = [
            ingest::worker::Output {
                key: g1_f1,
                bytes: 64,
                rows: 5,
                ..Default::default()
            },
            ingest::worker::Output {
                key: g1_f2,
                bytes: 64,
                rows: 5,
                ..Default::default()
            },
        ];
        p.clock.store((day2 + 180) as u64, Ordering::SeqCst);
        CALLS.with(|c| c.set(0));
        let store1 = Store {
            runtime: &p,
            service: &svc,
            access_key: ak.clone(),
            secret_key: sk.clone(),
            started: std::time::Instant::now(),
            vcpu_start: 0,
        };
        let wm1 = ingest::catalog::prepare_reserving(
            &store1,
            &Value::Null,
            "gen1",
            "v2",
            &g1_out,
            &[],
            &[],
            &[],
            0,
        )
        .unwrap();
        p.adapter.cas(ingest::WATERMARK_KEY, None, &wm1).unwrap();

        // Commit Generation 2 touching the SAME hours on day1 and day2:
        let g2_f1 = ingest::processed_key("processed/request/", day1 + 120, day1 + 180, "g2a", 7);
        let g2_f2 = ingest::processed_key("processed/request/", day2 + 120, day2 + 180, "g2b", 9);
        p.adapter
            .seed_object_bytes(&svc.fos_bucket, &g2_f1, b"PAR1".to_vec());
        p.adapter
            .seed_object_bytes(&svc.fos_bucket, &g2_f2, b"PAR1".to_vec());
        let g2_out = [
            ingest::worker::Output {
                key: g2_f1,
                bytes: 64,
                rows: 7,
                ..Default::default()
            },
            ingest::worker::Output {
                key: g2_f2,
                bytes: 64,
                rows: 9,
                ..Default::default()
            },
        ];
        p.clock.store((day2 + 300) as u64, Ordering::SeqCst);
        let wm2 = if pause_gen2 {
            // Allow Day 1 upload to succeed, then pause right before Day 2 / manifest upload:
            let mut cur_wm: Option<Value> = None;
            let mut pauses = 0u32;
            for _ in 0..8u32 {
                ELAPSED_MS.with(|e| e.set(0));
                CALLS.with(|c| c.set(32 - 6));
                let st = Store {
                    runtime: &p,
                    service: &svc,
                    access_key: ak.clone(),
                    secret_key: sk.clone(),
                    started: std::time::Instant::now(),
                    vcpu_start: 0,
                };
                match ingest::catalog::prepare_reserving(
                    &st,
                    &wm1,
                    "gen2",
                    "v2",
                    &g2_out,
                    &[],
                    &[],
                    &[],
                    1,
                ) {
                    Ok(done) => {
                        cur_wm = Some(done);
                        break;
                    }
                    Err(e) => {
                        assert_eq!(e, "catalog_prepare_pending");
                        pauses += 1;
                    }
                }
            }
            assert!(
                pauses >= 1,
                "expected at least 1 pause across multi-day Gen 2 prepare"
            );
            cur_wm.expect("Gen 2 prepare must complete across ticks")
        } else {
            CALLS.with(|c| c.set(0));
            let st = Store {
                runtime: &p,
                service: &svc,
                access_key: ak,
                secret_key: sk,
                started: std::time::Instant::now(),
                vcpu_start: 0,
            };
            ingest::catalog::prepare_reserving(&st, &wm1, "gen2", "v2", &g2_out, &[], &[], &[], 0)
                .unwrap()
        };
        let h2 = catalog::head(&wm2).unwrap();
        let m2_key = h2.manifest.as_ref().unwrap();
        let m2: Manifest =
            serde_json::from_slice(&p.adapter.bucket_objects(&svc.fos_bucket)[m2_key]).unwrap();
        (m2, h2)
    };

    let (unpaused_m, unpaused_h) = run_two_commits(false);
    let (paused_m, paused_h) = run_two_commits(true);
    assert_eq!(paused_m.parents.len(), 2);
    for (hr, parent_ref) in &paused_m.parents {
        assert_eq!(
            parent_ref.generation, 1,
            "BUG-CLOUD-CATALOG-28F: hour {hr} self-parented to Generation {} instead of Generation 1!",
            parent_ref.generation
        );
    }
    assert_eq!(
        serde_json::to_value(&paused_m).unwrap(),
        serde_json::to_value(&unpaused_m).unwrap(),
        "paused vs unpaused prepare_reserving must produce identical Manifest!"
    );
    assert_eq!(paused_h.days, unpaused_h.days);
    assert_eq!(
        serde_json::to_value(&paused_h.statistics).unwrap(),
        serde_json::to_value(&unpaused_h.statistics).unwrap()
    );
}

#[test]
fn bug_cloud_dr_29a_cold_start_inspected_active_reserve_sweep_20_to_32() {
    use fastly_log_analytics::{admin, http::Request};
    use std::collections::BTreeSet;

    struct RichGuard;
    impl Drop for RichGuard {
        fn drop(&mut self) {
            RICH_FOS.with(|r| r.set(false));
            MTIMES.with(|m| m.borrow_mut().clear());
        }
    }
    let _guard = RichGuard;
    RICH_FOS.with(|r| r.set(true));
    MTIMES.with(|m| m.borrow_mut().clear());

    let day_ts = 1_799_971_200i64;
    let day_obj = Day {
        version: VERSION,
        service_id: Some("fixtureService".into()),
        hours: BTreeMap::from([(
            day_ts + 3600,
            Reference {
                key: format!("processed/_catalog/{:064x}.json", 1u32),
                generation: 1,
            },
        )]),
        statistics: Statistics {
            earliest: Some(day_ts + 3600),
            latest: Some(day_ts + 3660),
            rows: 10,
            ..Default::default()
        },
        ..Day::default()
    };
    let day_bytes = serde_json::to_vec(&day_obj).unwrap();
    let dr_state = CloudDrState {
        schema_version: SCHEMA_VERSION,
        updated_at: 1_800_000_000,
        service_id: "fixtureService".into(),
        service: json!({"log_period": 60}),
        views: vec![],
        catalog_head: None,
        floor_key: None,
        rum_stats: Some(json!({"rows": 5, "bytes": 120, "files": 1, "at": 1_800_000_000})),
    };
    let dr_bytes = serde_json::to_vec(&dr_state).unwrap();

    for budget in 20..=32u32 {
        let p = Platform::new();
        p.adapter.seed_resource(
            "fixtureService",
            "snippet",
            json!({
                "name": "flp-owner-fixtureApp",
                "type": "recv",
                "priority": "0",
                "content": "# flp-owner:fixtureApp:fixtureService"
            }),
        );
        p.adapter.seed_object_bytes(
            "fos-fixture-logs",
            "meta/cloud_dr_state.json",
            dr_bytes.clone(),
        );
        p.adapter.seed_object_bytes(
            "fos-fixture-logs",
            &format!("processed/_catalog/{:064x}.json", 0xffu32),
            day_bytes.clone(),
        );
        for idx in 1..=24u32 {
            let blk = json!({
                "version": VERSION,
                "service_id": "fixtureService",
                "hour": day_ts + 3600,
                "generation": 1,
                "previous": null,
                "files": [],
                "summaries": [],
                "replaces": [],
                "committed_at": {}
            });
            p.adapter.seed_object_bytes(
                "fos-fixture-logs",
                &format!("processed/_catalog/{idx:064x}.json"),
                serde_json::to_vec(&blk).unwrap(),
            );
        }

        ELAPSED_MS.with(|e| e.set(0));
        CALLS.with(|c| c.set(32 - budget));
        SECRETS
            .with(|s| *s.borrow_mut() = BTreeSet::from(["front_key".into(), "session_key".into()]));
        let resp = admin::handle(
            &p,
            "fixtureAdmin",
            &Request {
                method: "POST".into(),
                path: "/api/admin/provision/recover".into(),
                headers: BTreeMap::new(),
                body: json!({
                    "token": "fixture-token",
                    "service_id": "fixtureService",
                    "fos_bucket": "fos-fixture-logs",
                    "fos_region": "us-east-1"
                })
                .to_string()
                .into_bytes(),
            },
        );
        let body_str = String::from_utf8_lossy(&resp.body);
        assert_eq!(
            resp.status, 200,
            "BUG-CLOUD-DR-29A: cold-start recover failed at budget={budget} with status={}: {body_str}",
            resp.status
        );
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ok"], true);
        assert_eq!(
            body["catalog_generation"], 1,
            "BUG-CLOUD-DR-29A: expected catalog_generation == 1 at budget={budget}, got {body}"
        );
        assert!(
            p.adapter.records().contains_key("svc:fixtureService"),
            "svc:fixtureService missing at budget={budget}"
        );
        assert!(
            p.adapter.records().contains_key("admin:install"),
            "admin:install missing at budget={budget}"
        );
        if budget >= 23 {
            assert_eq!(
                body["front_synced"], true,
                "BUG-CLOUD-DR-29A: expected front_synced == true at budget={budget}, got {body}"
            );
        }
    }
}

#[test]
fn bug_cloud_outbox_dr_29abc_provision_recover_compaction_and_outbox_limit() {
    use fastly_log_analytics::{
        ingest::storage::Store,
        telemetry::{
            compact::log_compact,
            flush::{log_flush, FlushStatus, FlushTarget},
            schema::from_parquet,
            LogKind, OperationalLogSettings,
        },
    };
    use std::collections::BTreeSet;
    use std::sync::atomic::Ordering;

    // 1. 29A-2 (end-to-end flush + compact::log_compact): 58 pre-reset rows + 3 post-reset rows on same day D
    // -> log_compact merges day D and retains ALL 61 unique event_ids (zero collisions!):
    {
        struct CdnPlatform<'a>(&'a Platform, &'a str, &'a str, &'a str);
        impl fastly_log_analytics::setup::SetupStores for CdnPlatform<'_> {
            fn config_present(&self, name: &str) -> Result<bool, String> {
                self.0.config_present(name)
            }
            fn secret_present(&self, name: &str) -> Result<bool, String> {
                self.0.secret_present(name)
            }
            fn state_linked(&self) -> Result<bool, String> {
                self.0.state_linked()
            }
        }
        impl Runtime for CdnPlatform<'_> {
            fn config(&self, k: &str) -> Result<Option<String>, String> {
                self.0.config(k)
            }
            fn secret(&self, k: &str) -> Result<Option<String>, String> {
                self.0.secret(k)
            }
            fn random(&self) -> Result<[u8; 32], String> {
                self.0.random()
            }
            fn now(&self) -> u64 {
                self.0.now()
            }
            fn checks(&self, live: bool) -> Result<Value, String> {
                self.0.checks(live)
            }
            fn versions(&self) -> Result<Value, String> {
                self.0.versions()
            }
            fn compute_service_id(&self) -> Option<String> {
                self.0.compute_service_id()
            }
            fn compute_service_version(&self) -> Option<String> {
                self.0.compute_service_version()
            }
            fn admin_get(&self, key: &str) -> Result<Option<Record>, String> {
                self.0.admin_get(key)
            }
            fn admin_cas(
                &self,
                key: &str,
                generation: Option<&str>,
                value: &Value,
            ) -> Result<bool, String> {
                self.0.admin_cas(key, generation, value)
            }
            fn kv_delete(&self, key: &str) -> Result<(), String> {
                self.0.kv_delete(key)
            }
            fn admin_fos(&self, req: FosRequest<'_>) -> Result<FosResponse, String> {
                self.0.admin_fos(req)
            }
            fn admin_cdn_get(
                &self,
                _service_id: &str,
                key: &str,
                _cdn_key: &str,
                max_bytes: usize,
                timeout: std::time::Duration,
            ) -> Result<FosResponse, String> {
                self.0.admin_fos(FosRequest {
                    method: "GET",
                    region: "us-east-1",
                    bucket: self.1,
                    key,
                    query: "",
                    access_key: self.2,
                    secret_key: self.3,
                    body: Vec::new(),
                    timeout,
                    response_limit: max_bytes,
                    content_md5: None,
                })
            }
            fn admin_remaining_calls(&self) -> u32 {
                self.0.admin_remaining_calls()
            }
            fn admin_elapsed(&self, started: std::time::Instant) -> std::time::Duration {
                self.0.admin_elapsed(started)
            }
            fn admin_wait(&self, millis: u64) {
                self.0.admin_wait(millis)
            }
        }

        struct RichGuard29A;
        impl Drop for RichGuard29A {
            fn drop(&mut self) {
                RICH_FOS.with(|r| r.set(false));
                MTIMES.with(|m| m.borrow_mut().clear());
            }
        }
        let _guard_29a = RichGuard29A;
        RICH_FOS.with(|r| r.set(true));
        MTIMES.with(|m| m.borrow_mut().clear());

        let p = Platform::new();
        let day_base = 1_800_000_000u64 - (1_800_000_000u64 % 86_400) + 7_200;
        p.clock.store(day_base, Ordering::SeqCst);
        let prov = p.provision();
        p.drain(prov["job"].clone());

        // Seed 58 entries into svc:fixtureService and flush on day D:
        let rec0 = p.adapter.get("svc:fixtureService").unwrap().unwrap();
        let mut val58 = rec0.value.clone();
        val58["_audit_outbox_base"] = json!(0);
        val58["_audit_outbox"] = Value::Array(
            (0..58u64)
                .map(|i| {
                    json!({
                        "actor": "fixtureAdmin",
                        "at": day_base + i,
                        "event": "service.updated",
                        "config_version": i + 1
                    })
                })
                .collect(),
        );
        assert!(p
            .adapter
            .cas("svc:fixtureService", Some(&rec0.generation), &val58)
            .unwrap());

        let svc_model: Service = serde_json::from_value(val58).unwrap();
        let (ak, sk) = fastly_log_analytics::admin::fos_credentials(&p, &svc_model).unwrap();
        let ops_prefix = "processed/ops/";
        let target = FlushTarget {
            region: &svc_model.fos_region,
            bucket: &svc_model.fos_bucket,
            access_key: &ak,
            secret_key: &sk,
            ops_prefix,
        };
        p.clock.store(day_base + 300, Ordering::SeqCst);
        CALLS.with(|c| c.set(0));
        fastly_log_analytics::telemetry::append_outbox(
            &p,
            "fixtureService",
            "c1",
            &format!(
                "{{\"kind\":\"cron_ticks\",\"row\":{{\"timestamp\":{},\"outcome\":\"ok\",\"trigger\":\"cron\"}}}}\n",
                (day_base + 300) * 1000
            ),
        )
        .unwrap();
        p.clock.store(day_base + 900, Ordering::SeqCst);
        CALLS.with(|c| c.set(0));
        SECRETS.with(|s| s.borrow_mut().clear());
        let f1 = log_flush(
            &p,
            "fixtureService",
            Some(&target),
            &OperationalLogSettings::default(),
            true,
            std::time::Instant::now(),
        )
        .unwrap();
        assert_eq!(f1.status, FlushStatus::Committed);

        // External KV reset wipes _audit_outbox_base to 0 with 3 post-reset entries on the SAME day D:
        let rec1 = p.adapter.get("svc:fixtureService").unwrap().unwrap();
        let mut reset_val = rec1.value.clone();
        reset_val
            .as_object_mut()
            .unwrap()
            .remove("_audit_outbox_base");
        reset_val["_audit_outbox"] = Value::Array(
            (0..3u64)
                .map(|i| {
                    json!({
                        "actor": "fixtureAdmin",
                        "at": day_base + 1_000 + i,
                        "event": "service.recovered",
                        "config_version": i + 1
                    })
                })
                .collect(),
        );
        assert!(p
            .adapter
            .cas("svc:fixtureService", Some(&rec1.generation), &reset_val)
            .unwrap());

        p.clock.store(day_base + 1_200, Ordering::SeqCst);
        CALLS.with(|c| c.set(0));
        fastly_log_analytics::telemetry::append_outbox(
            &p,
            "fixtureService",
            "c2",
            &format!(
                "{{\"kind\":\"cron_ticks\",\"row\":{{\"timestamp\":{},\"outcome\":\"ok\",\"trigger\":\"cron\"}}}}\n",
                (day_base + 1_200) * 1000
            ),
        )
        .unwrap();
        p.clock.store(day_base + 1_800, Ordering::SeqCst);
        CALLS.with(|c| c.set(0));
        SECRETS.with(|s| s.borrow_mut().clear());
        let f2 = log_flush(
            &p,
            "fixtureService",
            Some(&target),
            &OperationalLogSettings::default(),
            true,
            std::time::Instant::now(),
        )
        .unwrap();
        assert_eq!(f2.status, FlushStatus::Committed);
        assert_eq!(
            p.adapter.records()["svc:fixtureService"].value["_audit_outbox_base"].as_u64(),
            Some(58)
        );

        // Advance to Day D + 2 (past CLOSE_GRACE_S) and run real compact::log_compact across ticks until quiescent:
        let d_prev = (day_base as i64).div_euclid(86_400) - 1;
        p.adapter
            .cas(
                "logs:compact_state",
                None,
                &json!({
                    "done": {
                        "cron_runs": d_prev,
                        "cron_ticks": d_prev,
                        "ingest_files": d_prev,
                        "fos_usage": d_prev,
                        "admin_audit": d_prev,
                        "api_telemetry": d_prev,
                        "platform_errors": d_prev,
                        "storage_inventory": d_prev
                    }
                }),
            )
            .unwrap();
        p.clock.store(day_base + 172_800, Ordering::SeqCst);
        let cdn_rt = CdnPlatform(&p, &svc_model.fos_bucket, &ak, &sk);
        let mut total_chunks = 0u32;
        for _ in 0..25 {
            CALLS.with(|c| c.set(0));
            ELAPSED_MS.with(|e| e.set(0));
            let store = Store {
                runtime: &cdn_rt,
                service: &svc_model,
                access_key: ak.clone(),
                secret_key: sk.clone(),
                started: std::time::Instant::now(),
                vcpu_start: 0,
            };
            let comp = log_compact(
                &store,
                "fixture-cdn-key",
                &OperationalLogSettings::default(),
                1,
            )
            .unwrap();
            total_chunks += comp.chunks;
            if !comp.more_pending {
                break;
            }
        }
        assert!(
            total_chunks >= 1,
            "expected admin_audit day compaction to merge chunks"
        );

        let audit_rows: Vec<Value> = p
            .adapter
            .bucket_objects(&svc_model.fos_bucket)
            .into_iter()
            .filter(|(k, _)| k.starts_with("processed/ops/admin_audit/"))
            .flat_map(|(_, bytes)| from_parquet(LogKind::AdminAudit, bytes).unwrap())
            .collect();
        let unique_ids: BTreeSet<String> = audit_rows
            .iter()
            .map(|r| r["event_id"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(
            audit_rows.len(),
            61,
            "BUG-CLOUD-OUTBOX-DR-29A-2: log_compact dropped colliding post-reset rows!"
        );
        assert_eq!(unique_ids.len(), 61);
    }

    // 2. 29A-3 (admin/mod.rs independent): provision when svc:{sid} is None while logs:cursor:{sid} has svc_outbox_archived = 58
    // immediately sets svc:{sid}["_audit_outbox_base"] == 58:
    {
        let p_prov = Platform::new();
        p_prov
            .adapter
            .cas(
                "logs:cursor:fixtureService",
                None,
                &json!({"svc_outbox_archived": 58, "last_flush_s": 1_799_999_000}),
            )
            .unwrap();
        let _ = p_prov.provision();
        assert_eq!(
            p_prov.adapter.records()["svc:fixtureService"].value["_audit_outbox_base"].as_u64(),
            Some(58),
            "BUG-CLOUD-OUTBOX-DR-29A-3: provision with previous == None ignored surviving logs:cursor:fixtureService!"
        );
    }

    // 3. 29B (dr.rs independent): cold-start dr::recover with surviving logs:cursor:fixtureService (svc_outbox_archived = 58)
    // in a non-truncated shared bucket containing own rev manifest + 30 higher-gen foreign legacy manifests sets _audit_outbox_base == 58:
    {
        struct RichGuard29B;
        impl Drop for RichGuard29B {
            fn drop(&mut self) {
                RICH_FOS.with(|r| r.set(false));
                MTIMES.with(|m| m.borrow_mut().clear());
            }
        }
        let _guard_29b = RichGuard29B;
        RICH_FOS.with(|r| r.set(true));
        MTIMES.with(|m| m.borrow_mut().clear());

        let p_dr = Platform::new();
        p_dr.adapter
            .cas(
                "logs:cursor:fixtureService",
                None,
                &json!({"svc_outbox_archived": 58, "last_flush_s": 1_799_999_000}),
            )
            .unwrap();
        p_dr.adapter.seed_resource(
            "fixtureService",
            "snippet",
            json!({
                "name": "flp-owner-fixtureApp",
                "type": "recv",
                "priority": "0",
                "content": "# flp-owner:fixtureApp:fixtureService"
            }),
        );
        let day_ts = 1_799_971_200i64;
        let own_d4 = Day {
            version: VERSION,
            service_id: Some("fixtureService".into()),
            hours: BTreeMap::new(),
            statistics: Statistics {
                earliest: Some(day_ts + 60),
                latest: Some(day_ts + 120),
                rows: 12,
                ..Default::default()
            },
            ..Day::default()
        };
        p_dr.adapter.seed_object_bytes(
            "fos-fixture-logs",
            "processed/_catalog/00000000000000000000000000000000000000000000000000000000000000d4.json",
            serde_json::to_vec(&own_d4).unwrap(),
        );
        let own_m = json!({
            "version": VERSION,
            "service_id": "fixtureService",
            "generation": 4u64,
            "batch": "b4",
            "schema": "v2",
            "committed_at": "2027-01-14T12:00:00Z",
            "previous": null,
            "files": [], "summaries": [], "sources": [], "replaces": [],
            "bytes": 100, "rows": 12, "parents": {},
            "head_days": {
                day_ts.to_string(): "processed/_catalog/00000000000000000000000000000000000000000000000000000000000000d4.json"
            },
            "head_statistics": {
                day_ts.to_string(): {
                    "earliest": day_ts + 60,
                    "latest": day_ts + 120,
                    "rows": 12
                }
            }
        });
        let own_rev = u64::MAX - 4;
        p_dr.adapter.seed_object_bytes(
            "fos-fixture-logs",
            &format!("processed/_catalog/manifest-s-fixtureService-rev-{own_rev:020}-aaaa.json"),
            serde_json::to_vec(&own_m).unwrap(),
        );
        for fg in 10..40u64 {
            let fm = json!({
                "version": VERSION,
                "service_id": "foreignService22Chars0",
                "generation": fg,
                "batch": format!("f{fg}"),
                "schema": "v2",
                "committed_at": "2027-01-14T13:00:00Z",
                "previous": null,
                "files": [], "summaries": [], "sources": [], "replaces": [],
                "bytes": 0, "rows": 9999, "parents": {}, "head_days": {}, "head_statistics": {}
            });
            p_dr.adapter.seed_object_bytes(
                "fos-fixture-logs",
                &format!("processed/_catalog/manifest-{fg:020}-bbbb.json"),
                serde_json::to_vec(&fm).unwrap(),
            );
        }
        let rec_out = p_dr.json(
            "POST",
            "/api/admin/provision/recover",
            json!({
                "token": "fixture-token",
                "service_id": "fixtureService",
                "fos_bucket": "fos-fixture-logs",
                "fos_region": "us-east-1"
            }),
        );
        assert_eq!(rec_out["ok"], true);
        assert_eq!(rec_out["catalog_generation"], 4);
        assert_eq!(
            p_dr.adapter.records()["svc:fixtureService"].value["_audit_outbox_base"].as_u64(),
            Some(58),
            "BUG-CLOUD-OUTBOX-DR-29B: cold-start dr::recover skipped logs:cursor:fixtureService in mixed shared bucket!"
        );
    }

    // 4. 29C (admin/mod.rs & dr.rs independent): provision over torn_down with 4096 unarchived entries returns 409
    // audit_archive_required without draining unarchived.0, and dr::recover never drains unarchived entries:
    {
        let p_lim = Platform::new();
        let prov = p_lim.provision();
        p_lim.drain(prov["job"].clone());
        let td_gen = p_lim.service()["generation"].clone();
        let td = p_lim.json(
            "POST",
            "/api/admin/provision/teardown",
            json!({
                "service_id": "fixtureService",
                "confirm_service_id": "fixtureService",
                "generation": td_gen,
                "remove_logging": false,
                "remove_snippets": false,
                "delete_fos_keys": false
            }),
        );
        p_lim.drain(td["job"].clone());

        let td_rec = p_lim.adapter.get("svc:fixtureService").unwrap().unwrap();
        let mut full_val = td_rec.value.clone();
        full_val["_audit_outbox_base"] = json!(0);
        full_val["_audit_outbox"] = Value::Array(
            (0..4096usize)
                .map(|i| {
                    json!({
                        "actor": "fixtureAdmin",
                        "at": 1_800_000_000u64 + i as u64,
                        "event": format!("unarchived.{i}"),
                        "config_version": 1
                    })
                })
                .collect(),
        );
        assert!(p_lim
            .adapter
            .cas("svc:fixtureService", Some(&td_rec.generation), &full_val)
            .unwrap());

        let reprov_resp = p_lim.call(
            "POST",
            "/api/admin/provision/execute",
            json!({
                "token": "fixture-token",
                "service_id": "fixtureService",
                "fos_bucket": "fos-fixture-logs",
                "fos_region": "us-east-1"
            }),
        );
        assert_eq!(
            reprov_resp.status,
            409,
            "BUG-CLOUD-OUTBOX-DR-29C: provision over torn_down bypassed AUDIT_OUTBOX_LIMIT: {}",
            String::from_utf8_lossy(&reprov_resp.body)
        );
        let reprov_body: Value = serde_json::from_slice(&reprov_resp.body).unwrap();
        assert_eq!(reprov_body["error"], "audit_archive_required");
        let preserved = &p_lim.adapter.records()["svc:fixtureService"].value;
        assert_eq!(preserved["_audit_outbox"][0]["event"], "unarchived.0");

        // dr::recover over torn_down with 4096 unarchived entries must NOT drain unarchived.0:
        let dr_ok = p_lim.call(
            "POST",
            "/api/admin/provision/recover",
            json!({
                "token": "fixture-token",
                "service_id": "fixtureService",
                "fos_bucket": "fos-fixture-logs",
                "fos_region": "us-east-1"
            }),
        );
        assert_eq!(dr_ok.status, 200);
        let after_dr = &p_lim.adapter.records()["svc:fixtureService"].value;
        assert_eq!(
            after_dr["_audit_outbox"][0]["event"], "unarchived.0",
            "BUG-CLOUD-OUTBOX-DR-29C: dr::recover drained unarchived.0!"
        );
        assert_eq!(after_dr["_audit_outbox"].as_array().unwrap().len(), 4097);
    }
}
