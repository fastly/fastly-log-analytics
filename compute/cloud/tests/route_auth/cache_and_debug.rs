use super::support::*;
use fastly_log_analytics::{admin, http::Request, shell::Shell};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::atomic::Ordering};

#[test]
fn status_reads_are_cached_per_service_and_never_reach_unauthorized_callers() {
    let h = Harness::new();
    for path in ["/api/sync-status", "/api/cron-runs"] {
        let start = h.platform.kv_reads.get();
        let first = h.send("GET", path, "admin");
        assert_eq!(first.status, 200);
        let miss_reads = h.platform.kv_reads.get() - start;
        let start = h.platform.kv_reads.get();
        let second = h.send("GET", path, "admin");
        assert_eq!(second.body, first.body, "{path}");
        assert!(
            h.platform.kv_reads.get() - start < miss_reads,
            "{path} hit skips the reads"
        );
    }
    assert!(h
        .platform
        .cache
        .borrow()
        .keys()
        .all(|k| !k.starts_with("flp-status-") || k.contains("fixtureService")));
    assert_eq!(status_entries(&h), 2);
    for path in ["/api/sync-status", "/api/cron-runs"] {
        for session in ["anonymous", "expired", "tampered"] {
            assert_eq!(h.send("GET", path, session).status, 401, "{path} {session}");
        }
        for session in ["analyst", "no_pii"] {
            assert_eq!(h.send("GET", path, session).status, 403, "{path} {session}");
        }
    }
}

#[test]
fn every_signed_in_role_reads_the_header_badge_without_cron_or_audit_history() {
    let h = Harness::new();
    let admin: Value =
        serde_json::from_slice(&h.send("GET", "/api/sync-status", "admin").body).unwrap();
    assert_eq!(admin["configured"], true);
    assert!(admin["request"].is_object());
    for session in ["analyst", "no_pii", "admin"] {
        let response = h.send("GET", "/api/header-badge", session);
        assert_eq!(response.status, 200, "{session}");
        let badge: Value = serde_json::from_slice(&response.body).unwrap();
        let mut keys = badge
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        keys.sort();
        assert_eq!(keys, ["configured", "request", "rum"], "{session}");
        assert_eq!(badge["request"], admin["request"], "{session}");
        assert_eq!(badge["rum"], admin["rum"], "{session}");
    }
    assert_eq!(
        status_entries(&h),
        2,
        "the badge is cached beside sync-status"
    );
    for session in ["anonymous", "expired", "tampered"] {
        assert_eq!(
            h.send("GET", "/api/header-badge", session).status,
            401,
            "{session}"
        );
    }
}

#[test]
fn manual_tick_purges_the_status_cache_and_the_next_read_is_fresh() {
    let h = Harness::new();
    let before: Value =
        serde_json::from_slice(&h.send("GET", "/api/cron-runs", "admin").body).unwrap();
    assert_eq!(status_entries(&h), 1);
    h.platform.clock.fetch_add(300, Ordering::SeqCst);
    assert_eq!(
        h.send("POST", "/api/cron-runs/trigger", "admin").status,
        200
    );
    assert_eq!(*h.platform.purges.borrow(), ["flp-status-fixtureService"]);
    assert_eq!(status_entries(&h), 0);
    let after: Value =
        serde_json::from_slice(&h.send("GET", "/api/cron-runs", "admin").body).unwrap();
    assert_ne!(after["last_tick_at"], before["last_tick_at"]);
}

#[test]
fn status_purge_is_skipped_when_no_backend_calls_remain() {
    let h = Harness::new();
    h.send("GET", "/api/cron-runs", "admin");
    while h.platform.budget.charge().is_ok() {}
    assert!(admin::purge_status_cache(&h.platform, "fixtureService").is_err());
    assert_eq!(status_entries(&h), 1);
}

#[test]
fn admin_pages_accept_head_requests_and_enforce_session_gate() {
    use fastly_log_analytics::site::Asset;
    let h = Harness::new();
    let assets = [
        Asset {
            path: "/admin.html",
            content_type: "text/html; charset=utf-8",
            identity: Some(b"admin"),
            gzip: None,
            br: None,
        },
        Asset {
            path: "/logs.html",
            content_type: "text/html; charset=utf-8",
            identity: Some(b"logs"),
            gzip: None,
            br: None,
        },
    ];
    let shell = Shell::new(&h.platform, &h.store, &assets);
    for path in ["/admin", "/logs", "/logs?tab=cron"] {
        let admin_res = shell.handle(&Request {
            method: "HEAD".into(),
            path: path.into(),
            headers: BTreeMap::from([
                ("x-flp-front-key".into(), "fixture-front-key".into()),
                ("fastly-client-ip".into(), "192.0.2.1".into()),
                ("cookie".into(), h.sessions["admin"].clone().unwrap()),
            ]),
            body: vec![],
        });
        assert_eq!(admin_res.status, 200, "HEAD {path} for admin");
        let analyst_res = shell.handle(&Request {
            method: "HEAD".into(),
            path: path.into(),
            headers: BTreeMap::from([
                ("x-flp-front-key".into(), "fixture-front-key".into()),
                ("fastly-client-ip".into(), "192.0.2.1".into()),
                ("cookie".into(), h.sessions["analyst"].clone().unwrap()),
            ]),
            body: vec![],
        });
        assert_eq!(analyst_res.status, 403, "HEAD {path} for analyst");
    }
}

#[test]
fn page_load_id_is_recorded_in_telemetry_and_debug_headers_are_emitted() {
    let h = Harness::new();
    h.platform
        .api_log
        .record("GET", "/service/fixtureService", Some(200), 5, None, "");
    let shell = Shell::new(&h.platform, &h.store, &[]);
    let res = shell.handle(&Request {
        method: "GET".into(),
        path: "/api/admin/services/fixtureService".into(),
        headers: BTreeMap::from([
            ("x-flp-front-key".into(), "fixture-front-key".into()),
            ("fastly-client-ip".into(), "192.0.2.1".into()),
            ("cookie".into(), h.sessions["admin"].clone().unwrap()),
            ("x-page-load-id".into(), "pl_test_123".into()),
            ("x-debug-responses".into(), "1".into()),
        ]),
        body: vec![],
    });
    assert_eq!(res.status, 200);
    assert!(res.headers.contains_key("x-flp-vcpu-ms"));
    let debug_calls: Value = serde_json::from_str(
        res.headers
            .get("x-flp-debug-calls")
            .expect("x-flp-debug-calls header"),
    )
    .unwrap();
    let arr = debug_calls.as_array().expect("debug_calls array");
    assert!(!arr.is_empty());
    assert_eq!(arr[0]["status"], 200);
    assert_eq!(arr[0]["status_code"], 200);
    assert_eq!(arr[0]["path"], "/service/:id");
    assert_eq!(arr[0]["endpoint"], "/service/:id");

    let beacon_res = shell.handle(&Request {
        method: "POST".into(),
        path: "/api/telemetry/query".into(),
        headers: BTreeMap::from([
            ("x-flp-front-key".into(), "fixture-front-key".into()),
            ("fastly-client-ip".into(), "192.0.2.1".into()),
            ("x-flp-front-rl".into(), "1".into()),
            ("origin".into(), "https://logs.example.com".into()),
            ("cookie".into(), h.sessions["analyst"].clone().unwrap()),
            ("x-page-load-id".into(), "pl_beacon_456".into()),
            ("content-type".into(), "application/json".into()),
        ]),
        body: serde_json::to_vec(&json!({
            "files_listed": 2,
            "files_fetched": 1,
            "files_cached": 1,
            "bytes_scanned": 1024,
            "duration_ms": 45
        }))
        .unwrap(),
    });
    assert_eq!(beacon_res.status, 204);

    let rows = h.rows("api_telemetry");
    let compute_row = rows
        .iter()
        .find(|r| r["source"] == "compute_api" && r["path"] == "/api/admin/services/:id")
        .expect("compute_api row");
    assert_eq!(
        compute_row["details_json"].as_str(),
        Some(r#"{"page_load_id":"pl_test_123"}"#)
    );
    let fastly_row = rows
        .iter()
        .find(|r| r["source"] == "fastly_api")
        .expect("fastly_api row");
    assert_eq!(
        fastly_row["details_json"].as_str(),
        Some(r#"{"page_load_id":"pl_test_123"}"#)
    );
    let beacon_row = rows
        .iter()
        .find(|r| r["source"] == "browser_query")
        .expect("browser_query row");
    assert_eq!(
        beacon_row["details_json"].as_str(),
        Some(r#"{"outcome":"ok","page_load_id":"pl_beacon_456","surface":"dashboard"}"#)
    );
}


#[test]
fn harness_header_is_recorded_only_for_authenticated_requests() {
    let path = "/api/admin/services/fixtureService";
    let authed = harness_request(path, &[("x-flp-harness", "camp-p10-s1")], true);
    assert_eq!(
        compute_row_details(&authed, "/api/admin/services/:id"),
        Some(json!({"harness": "camp-p10-s1"}))
    );
    assert!(usage_callers(&authed)
        .iter()
        .all(|c| c.starts_with("harness:")));
    assert!(authed
        .rows("fos_usage")
        .iter()
        .all(|r| r["harness"] == "camp-p10-s1"));

    let anonymous = harness_request(
        "/api/auth/session",
        &[("x-flp-harness", "camp-p10-s1")],
        false,
    );
    assert_eq!(compute_row_details(&anonymous, "/api/auth/session"), None);
    assert!(usage_callers(&anonymous)
        .iter()
        .all(|c| !c.starts_with("harness:")));
    assert!(anonymous
        .rows("fos_usage")
        .iter()
        .all(|r| r.get("harness").is_none()));

    let bad = harness_request(path, &[("x-flp-harness", "bad label!")], true);
    assert_eq!(compute_row_details(&bad, "/api/admin/services/:id"), None);
    assert!(usage_callers(&bad)
        .iter()
        .all(|c| !c.starts_with("harness:")));
}

#[test]
fn harness_label_and_page_load_id_share_details_json() {
    let h = harness_request(
        "/api/admin/services/fixtureService",
        &[
            ("x-flp-harness", "camp-p10-s1"),
            ("x-page-load-id", "pl_test_123"),
        ],
        true,
    );
    assert_eq!(
        compute_row_details(&h, "/api/admin/services/:id"),
        Some(json!({"harness": "camp-p10-s1", "page_load_id": "pl_test_123"}))
    );
}

#[test]
fn debug_panel_visibility_settings_are_persisted_and_resolved_per_role() {
    let h = Harness::new();
    let patch = h.send_with(
        "PATCH",
        "/api/admin/usage-logging",
        "admin",
        Some("https://logs.example.com"),
        &serde_json::to_vec(&json!({
            "query_debug_visibility": "both",
            "api_call_debug_visibility": "admins"
        }))
        .unwrap(),
    );
    assert_eq!(patch.status, 200);
    let patch_body: Value = serde_json::from_slice(&patch.body).unwrap();
    assert_eq!(patch_body["query_debug_visibility"], "both");
    assert_eq!(patch_body["api_call_debug_visibility"], "admins");

    let admin_session: Value =
        serde_json::from_slice(&h.send("GET", "/api/auth/session", "admin").body).unwrap();
    assert_eq!(admin_session["debug_queries"], true);
    assert_eq!(admin_session["debug_api_calls"], true);

    let analyst_session: Value =
        serde_json::from_slice(&h.send("GET", "/api/auth/session", "analyst").body).unwrap();
    assert_eq!(analyst_session["debug_queries"], true);
    assert_eq!(analyst_session["debug_api_calls"], false);
}

#[test]
fn shield_clock_advertises_sleep_ttl_until_next_due_wake() {
    let h = Harness::new();
    h.platform.clock.fetch_add(120, Ordering::SeqCst);

    let tick0 = h.send("GET", "/_tick", "anonymous");
    assert_eq!(tick0.status, 200);
    assert_eq!(
        tick0.headers.get("x-flp-tick-ttl").map(String::as_str),
        Some("58")
    );
    assert_eq!(
        tick0.headers.get("cache-control").map(String::as_str),
        Some("public, max-age=58")
    );

    let flush0 = h.send("GET", "/_flush", "anonymous");
    assert_eq!(flush0.status, 200);
    assert_eq!(
        flush0.headers.get("x-flp-tick-ttl").map(String::as_str),
        Some("60")
    );
    assert_eq!(
        flush0.headers.get("cache-control").map(String::as_str),
        Some("public, max-age=60")
    );

    h.platform.clock.fetch_add(13, Ordering::SeqCst);
    let tick1 = h.send("GET", "/_tick", "anonymous");
    assert_eq!(
        tick1.headers.get("x-flp-tick-ttl").map(String::as_str),
        Some("45")
    );
    let flush1 = h.send("GET", "/_flush", "anonymous");
    assert_eq!(
        flush1.headers.get("x-flp-tick-ttl").map(String::as_str),
        Some("47")
    );
}
