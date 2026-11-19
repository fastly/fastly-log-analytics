use super::support::*;
use fastly_log_analytics::{
    http::Request,
    shell::{Runtime, Shell},
};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::atomic::Ordering};

#[test]
fn api_requests_log_normalized_path_actor_pop_and_vcpu_delta() {
    let harness = Harness::new();
    harness.send("GET", "/api/admin/users?token=abc", "admin");
    harness.send("GET", "/api/auth/session", "anonymous");
    let rows = harness.rows("api_telemetry");
    assert_eq!(rows.len(), 2, "{rows:?}");
    let users = rows
        .iter()
        .find(|r| r["path"] == "/api/admin/users")
        .unwrap();
    assert_eq!(users["source"], "compute_api");
    assert_eq!(users["method"], "GET");
    assert_eq!(users["status_code"], 200);
    assert_eq!(users["actor"], "u-admin");
    assert_eq!(users["pop"], "IAD");
    assert_eq!(users["service_id"], "fixtureService");
    let vcpu = users["vcpu_ms"].as_u64().unwrap();
    assert!(vcpu > 0 && vcpu < 1_000, "cumulative vcpu leaked: {vcpu}");
    assert!(!Value::Array(rows.clone()).to_string().contains("abc"));
    let session = rows
        .iter()
        .find(|r| r["path"] == "/api/auth/session")
        .unwrap();
    assert!(session["actor"].is_null() || session["actor"] == "");
}

#[test]
fn beacon_records_one_server_attributed_row_and_no_compute_api_row() {
    let harness = Harness::new();
    let response = harness.beacon(
        "analyst",
        json!({"files_listed": 4, "cdn_hits": 2, "actor": "evil", "event_id": "evil",
               "service_id": "evil", "source": "evil", "timestamp": 1, "unknown": true}),
    );
    assert_eq!(response.status, 204);
    let rows = harness.rows("api_telemetry");
    assert_eq!(rows.len(), 1, "{rows:?}");
    let row = &rows[0];
    assert_eq!(row["source"], "browser_query");
    assert_eq!(row["actor"], "u-analyst");
    assert_eq!(row["service_id"], "fixtureService");
    assert_eq!(row["files_listed"], 4);
    assert_ne!(row["event_id"], "evil");
    assert_ne!(row["timestamp"], 1);
    assert!(row.get("unknown").is_none());
}

#[test]
fn beacon_folds_dashboard_file_reads_into_one_compute_row() {
    let harness = Harness::new();
    let response = harness.beacon(
        "analyst",
        json!({"files_fetched": 9, "compute_requests": 7, "vcpu_ms": 35}),
    );
    assert_eq!(response.status, 204);
    let rows = harness.rows("api_telemetry");
    assert_eq!(rows.len(), 2, "{rows:?}");
    let folded = rows
        .iter()
        .find(|r| r["source"] == "compute_api")
        .expect("folded compute_api row");
    assert_eq!(folded["path"], "/api/file");
    assert_eq!(folded["files_fetched"], 7);
    assert_eq!(folded["vcpu_ms"], 35);
    assert_eq!(folded["actor"], "u-analyst");
    let browser = rows
        .iter()
        .find(|r| r["source"] == "browser_query")
        .expect("browser_query row");
    assert_eq!(browser["files_fetched"], 9);
    assert!(browser.get("vcpu_ms").is_none());
}

#[test]
fn folded_file_row_carries_the_beacon_harness_label() {
    let harness = Harness::new();
    let response = harness.send_with_headers(
        "POST",
        "/api/telemetry/query",
        "analyst",
        Some("https://logs.example.com"),
        json!({"compute_requests": 3, "vcpu_ms": 12})
            .to_string()
            .as_bytes(),
        &[("x-flp-harness", "camp-p10-s1")],
    );
    assert_eq!(response.status, 204);
    let rows = harness.rows("api_telemetry");
    let folded = rows
        .iter()
        .find(|r| r["source"] == "compute_api" && r["path"] == "/api/file")
        .expect("folded compute_api row");
    let details: Value = serde_json::from_str(folded["details_json"].as_str().unwrap()).unwrap();
    assert_eq!(details["harness"], "camp-p10-s1");
}

#[test]
fn folded_file_reads_are_recorded_when_browser_query_telemetry_is_off() {
    let harness = Harness::new();
    assert!(harness
        .platform
        .admin_cas(
            "settings:operational_logs",
            None,
            &json!({"browser_query_telemetry": false})
        )
        .unwrap());
    let response = harness.beacon("analyst", json!({"compute_requests": 3, "vcpu_ms": 12}));
    assert_eq!(response.status, 204);
    let rows = harness.rows("api_telemetry");
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["source"], "compute_api");
    assert_eq!(rows[0]["files_fetched"], 3);
}

#[test]
fn a_file_response_streams_from_the_cdn_with_the_clients_range() {
    let harness = Harness::new();
    let headers = BTreeMap::from([
        (
            "x-flp-front-key".to_string(),
            "fixture-front-key".to_string(),
        ),
        ("fastly-client-ip".into(), "192.0.2.1".into()),
        ("x-flp-front-rl".into(), "1".into()),
        ("range".into(), "bytes=0-9".into()),
        (
            "cookie".into(),
            harness.sessions["analyst"].clone().unwrap(),
        ),
    ]);
    let response = Shell::new(&harness.platform, &harness.store, &[]).handle(&Request {
        method: "GET".into(),
        path: "/api/file?key=processed/request/x.parquet".into(),
        headers,
        body: vec![],
    });
    assert_eq!(response.status, 200);
    assert!(response.body.is_empty());
    let upstream = response.upstream.expect("streamed from the CDN");
    assert_eq!(upstream.key, "processed/request/x.parquet");
    assert_eq!(upstream.cdn_key, "fixture-cdn-key");
    assert_eq!(upstream.range.as_deref(), Some("bytes=0-9"));
    assert_eq!(
        response.headers["content-type"],
        "application/vnd.apache.parquet"
    );
}

#[test]
fn duckdb_file_head_is_authenticated_and_forwards_its_open_range() {
    let harness = Harness::new();
    let response = Shell::new(&harness.platform, &harness.store, &[]).handle(&Request {
        method: "HEAD".into(),
        path: "/api/file?key=processed/request/x.parquet".into(),
        headers: BTreeMap::from([
            ("x-flp-front-key".into(), "fixture-front-key".into()),
            ("fastly-client-ip".into(), "192.0.2.1".into()),
            ("x-flp-front-rl".into(), "1".into()),
            ("range".into(), "bytes=0-".into()),
            (
                "cookie".into(),
                harness.sessions["analyst"].clone().unwrap(),
            ),
        ]),
        body: vec![],
    });
    assert_eq!(response.status, 200);
    let upstream = response
        .upstream
        .expect("HEAD must stream metadata, not read the file");
    assert!(upstream.head_only);
    assert_eq!(upstream.range.as_deref(), Some("bytes=0-"));
    assert!(response.body.is_empty());
}

#[test]
fn flagged_file_read_failures_still_record_their_own_row() {
    let harness = Harness::new();
    let mut headers = BTreeMap::from([
        (
            "x-flp-front-key".to_string(),
            "fixture-front-key".to_string(),
        ),
        ("fastly-client-ip".into(), "192.0.2.1".into()),
        ("x-flp-front-rl".into(), "1".into()),
        ("x-flp-fold-telemetry".into(), "1".into()),
    ]);
    headers.insert(
        "cookie".into(),
        harness.sessions["analyst"].clone().unwrap(),
    );
    let response = Shell::new(&harness.platform, &harness.store, &[]).handle(&Request {
        method: "GET".into(),
        path: "/api/file?key=elsewhere/x.parquet".into(),
        headers,
        body: vec![],
    });
    assert_eq!(response.status, 400);
    let rows = harness.rows("api_telemetry");
    assert!(
        rows.iter()
            .any(|r| r["source"] == "compute_api" && r["path"] == "/api/file"),
        "{rows:?}"
    );
}

#[test]
fn beacon_rejects_oversize_signed_out_and_cross_origin() {
    let harness = Harness::new();
    let big = json!({"files_listed": 1, "pad": "x".repeat(5000)});
    assert_eq!(harness.beacon("analyst", big).status, 413);
    assert_eq!(harness.beacon("anonymous", json!({})).status, 401);
    let cross = harness.send_with(
        "POST",
        "/api/telemetry/query",
        "analyst",
        Some("https://evil.example"),
        b"{}",
    );
    assert_eq!(cross.status, 403);
    assert!(harness
        .rows("api_telemetry")
        .iter()
        .all(|r| r["source"] != "browser_query"));
}

#[test]
fn beacons_over_sixty_per_minute_are_dropped_with_204_then_recover() {
    let harness = Harness::new();
    for _ in 0..60 {
        assert_eq!(
            harness.beacon("analyst", json!({"files_listed": 1})).status,
            204
        );
    }
    let over = harness.beacon("analyst", json!({"files_listed": 1}));
    assert_eq!(over.status, 204);
    assert_eq!(
        over.headers.get("x-flp-log-dropped").map(String::as_str),
        Some("1")
    );
    let browser = |h: &Harness| {
        h.rows("api_telemetry")
            .iter()
            .filter(|r| r["source"] == "browser_query")
            .count()
    };
    assert_eq!(browser(&harness), 60);
    assert_eq!(
        harness.beacon("admin", json!({"files_listed": 1})).status,
        204
    );
    assert_eq!(browser(&harness), 61);
    harness.platform.clock.fetch_add(60, Ordering::SeqCst);
    harness.beacon("analyst", json!({"files_listed": 1}));
    assert_eq!(browser(&harness), 62);
}

#[test]
fn beacons_are_ignored_when_browser_query_telemetry_is_off() {
    let harness = Harness::new();
    assert!(harness
        .platform
        .admin_cas(
            "settings:operational_logs",
            None,
            &json!({"browser_query_telemetry": false})
        )
        .unwrap());
    let response = harness.beacon("analyst", json!({"files_listed": 1}));
    assert_eq!(response.status, 204);
    assert!(harness.rows("api_telemetry").is_empty());
}

#[test]
fn unauthenticated_failures_do_no_telemetry_work() {
    let harness = Harness::new();
    let reads = harness.platform.kv_reads.get();
    for path in [
        "/api/admin/users",
        "/api/files",
        "/api/zz-fuzz-1",
        "/api/admin/zz-fuzz-2",
    ] {
        let status = harness.send("GET", path, "anonymous").status;
        assert!(matches!(status, 401 | 403 | 404), "{path}: {status}");
    }
    assert_eq!(harness.platform.kv_reads.get(), reads);
    assert!(harness.rows("api_telemetry").is_empty());
    assert!(harness.platform.cache.borrow().is_empty());
}

#[test]
fn route_table_paths_never_normalize_to_unknown_and_fuzzed_segments_do() {
    use fastly_log_analytics::telemetry::api::normalize_path;
    for (_, path, _) in ROUTES {
        assert!(!normalize_path(path).contains(":unknown"), "{path}");
    }
    assert_eq!(
        normalize_path("/api/admin/users/abc/zzz"),
        "/api/admin/users/:id/:unknown"
    );
}

#[test]
fn telemetry_survives_an_exhausted_backend_budget_without_falling_to_global() {
    let harness = Harness::new();
    harness.platform.metered.set(true);
    for _ in 0..32 {
        harness.platform.budget.charge().unwrap();
    }
    assert!(harness.platform.admin_get("admin:install").is_err());
    harness.send("GET", "/api/admin/users", "admin");
    let rows = harness.rows("api_telemetry");
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["service_id"], "fixtureService");
}
