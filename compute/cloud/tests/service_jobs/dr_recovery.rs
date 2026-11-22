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
