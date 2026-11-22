mod common;

use common::FixtureRuntime;
use fastly_log_analytics::{
    admin::{ApiRequest, FosRequest},
    shell::Runtime,
};
use serde_json::json;
use std::time::Duration;

#[test]
fn runtime_kv_matches_fixture_adapter_cas_contract() {
    let runtime = FixtureRuntime::new();

    assert!(runtime
        .adapter
        .cas("state:one", None, &json!({"n": 1}))
        .unwrap());
    let runtime_record = runtime.admin_get("state:one").unwrap().unwrap();
    let adapter_record = runtime.adapter.get("state:one").unwrap().unwrap();
    assert_eq!(runtime_record.generation, adapter_record.generation);
    assert_eq!(runtime_record.value, adapter_record.value);
    assert!(!runtime
        .admin_cas("state:one", Some("stale"), &json!({"n": 2}))
        .unwrap());
    let generation = runtime.admin_get("state:one").unwrap().unwrap().generation;
    assert!(runtime
        .admin_cas("state:one", Some(&generation), &json!({"n": 2}))
        .unwrap());
    assert_eq!(
        runtime.admin_get("state:one").unwrap().unwrap().value,
        json!({"n": 2})
    );
}

#[test]
fn runtime_fos_matches_fixture_adapter_for_reads_and_lists() {
    let runtime = FixtureRuntime::new();
    let created = runtime
        .admin_api(ApiRequest {
            method: "POST",
            path: "/resources/object-storage/access-keys",
            query: "",
            token: "fixture-token",
            body: Some(json!({
                "name": "fixture",
                "permission": "read-write-admin",
                "buckets": []
            })),
            timeout: Duration::from_secs(1),
        })
        .unwrap();
    let access_key = created.body["access_key"].as_str().unwrap();
    let secret_key = created.body["secret_key"].as_str().unwrap();
    let request = |method, key, body| FosRequest {
        method,
        region: "us-east-1",
        bucket: "fixture-bucket",
        key,
        query: "",
        access_key,
        secret_key,
        body,
        timeout: Duration::from_secs(1),
        response_limit: 4096,
        content_md5: None,
    };

    assert_eq!(
        runtime
            .admin_fos(request("PUT", "", vec![]))
            .unwrap()
            .status,
        200
    );
    assert_eq!(
        runtime
            .admin_fos(request("PUT", "logs/one", b"payload".to_vec()))
            .unwrap()
            .status,
        200
    );
    assert_eq!(
        runtime
            .admin_fos(request("GET", "logs/one", vec![]))
            .unwrap()
            .body,
        runtime.adapter.bucket_objects("fixture-bucket")["logs/one"]
    );
    assert_eq!(
        runtime.admin_fos(request("GET", "", vec![])).unwrap().body,
        runtime
            .adapter
            .fos(request("GET", "", vec![]))
            .unwrap()
            .body
    );
}
