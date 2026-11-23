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
        let reset_minute_ts = reset_dt.unix_timestamp() - reset_dt.unix_timestamp().rem_euclid(60);
        let expected_reset_floor = format!("raw/request/{}", ingest::minute_dir(reset_minute_ts));

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
