use super::support::*;
use fastly_log_analytics::{admin, http::Request, shell::Runtime};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::atomic::Ordering};

#[test]
fn cron_schedule_projects_due_jobs_without_claiming_or_fabricating_stage_history() {
    let platform = Platform::new();
    let job = platform.provision();
    platform.drain(job["job"].clone());
    let now = platform.now();
    let old = platform.adapter.get("svc:fixtureService").unwrap().unwrap();
    let mut service = old.value;
    service["log_period"] = json!(60);
    service["rum"]["enabled"] = json!(true);
    service["rum"]["log_period"] = json!(300);
    assert!(platform
        .adapter
        .cas("svc:fixtureService", Some(&old.generation), &service)
        .unwrap());
    let period = admin::schedule::ready_rum_period(now, 300);
    let state = json!({
        "jobs": {
            "ingest": {"last_run": now - 5, "more_pending": true, "retry_after": now + 30},
            "raw_delete": {"last_run": now - 10, "more_pending": false, "last_status": "idle",
                "service_id": "fixtureService"},
            "maint": {"last_run": now - 20, "retry_after": now + 200},
            "rum": {"last_run": now - 300, "last_success_period": period,
                "last_success_period_s": 300, "retry_after": now + 10}
        }
    });
    assert!(platform.adapter.cas("cron:state", None, &state).unwrap());
    let svc_off = platform.adapter.get("svc:fixtureService").unwrap().unwrap();
    let mut svc_off_val = svc_off.value;
    svc_off_val["compaction"] = json!({"mode": "off", "size_target_mb": 128});
    assert!(platform
        .adapter
        .cas(
            "svc:fixtureService",
            Some(&svc_off.generation),
            &svc_off_val
        )
        .unwrap());
    let before = platform.adapter.get("cron:state").unwrap().unwrap();
    let snapshot = admin::cron_audit_snapshot(&platform);
    let find = |task: &str| {
        snapshot
            .schedules
            .iter()
            .find(|row| row["task"] == task)
            .unwrap()
    };
    let time = |at| {
        time::OffsetDateTime::from_unix_timestamp(at as i64)
            .unwrap()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap()
    };
    assert_eq!(find("request_ingest")["interval_s"], 60);
    // The clock probes at half the 60 s log period (30 s).
    assert_eq!(snapshot.clock["probe_interval_s"], 30.0);
    assert_eq!(find("cloud_ingest_clock")["interval_s"], 30);
    assert_eq!(find("request_ingest")["next_run_time"], time(now + 30));
    assert!(find("raw_delete")["next_run_time"].is_null());
    assert_eq!(
        find("raw_delete")["execution"],
        json!({
            "service_id": "fixtureService", "last_run": now - 10, "status": "idle",
            "more_pending": false, "retry_after": 0,
        })
    );
    assert_eq!(find("maintenance")["next_run_time"], time(now + 200));
    assert_eq!(
        find("rum_ingest")["next_run_time"],
        time((period + 1) * 300 + fastly_log_analytics::ingest::settle_s(300))
    );
    assert!(find("rum_ingest")["disabled_reason"].is_null());
    for task in admin::logs::MAINT_STAGES {
        assert!(find(task)["last_run_time"].is_null());
        assert!(find(task)["next_run_time"].is_null());
    }
    assert_eq!(
        find("request_compact")["disabled_reason"],
        "Request compaction is disabled"
    );
    assert!(snapshot.runs.is_empty());
    let after = platform.adapter.get("cron:state").unwrap().unwrap();
    assert_eq!(before.generation, after.generation);
    assert_eq!(before.value, after.value);

    // With compaction configured, request_compact is its own job with a real schedule.
    let old = platform.adapter.get("svc:fixtureService").unwrap().unwrap();
    let mut service = old.value;
    service["compaction"] = json!({"mode": "size_target", "size_target_mb": 32});
    assert!(platform
        .adapter
        .cas("svc:fixtureService", Some(&old.generation), &service)
        .unwrap());
    let old = platform.adapter.get("cron:state").unwrap().unwrap();
    let mut state = old.value;
    state["jobs"]["compact"] =
        json!({"last_run": now - 10, "more_pending": true, "retry_after": now + 7});
    assert!(platform
        .adapter
        .cas("cron:state", Some(&old.generation), &state)
        .unwrap());
    let snapshot = admin::cron_audit_snapshot(&platform);
    let compact = snapshot
        .schedules
        .iter()
        .find(|row| row["task"] == "request_compact")
        .unwrap();
    assert!(compact["disabled_reason"].is_null(), "{compact}");
    assert_eq!(compact["last_run_time"], time(now - 10));
    assert_eq!(
        compact["next_run_time"],
        time(now - 10 + admin::schedule::COMPACT_INTERVAL_S)
    );
    assert_eq!(compact["interval_s"], admin::schedule::COMPACT_INTERVAL_S);

    let old = platform.adapter.get("cron:state").unwrap().unwrap();
    let mut state = old.value;
    state["jobs"]["ingest"]["more_pending"] = json!(false);
    assert!(platform
        .adapter
        .cas("cron:state", Some(&old.generation), &state)
        .unwrap());
    let snapshot = admin::cron_audit_snapshot(&platform);
    let compact = snapshot
        .schedules
        .iter()
        .find(|row| row["task"] == "request_compact")
        .unwrap();
    assert_eq!(compact["next_run_time"], time(now + 7));
}

#[test]
fn cron_schedule_raw_delete_execution_rejects_unscoped_state_and_exports_only_known_statuses() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    for (service_id, status, expected_status) in [
        (None, "idle", None),
        (Some("otherService"), "idle", None),
        (
            Some("fixtureService"),
            "unexpected-private-detail",
            Some("unknown"),
        ),
        (Some("fixtureService"), "error", Some("error")),
    ] {
        let prior = platform.adapter.get("cron:state").unwrap();
        let value = json!({"jobs": {"raw_delete": {
            "service_id": service_id, "last_run": platform.now(), "last_status": status,
            "more_pending": true, "retry_after": platform.now() + 30,
        }}});
        assert!(platform
            .adapter
            .cas(
                "cron:state",
                prior.as_ref().map(|r| r.generation.as_str()),
                &value
            )
            .unwrap());
        let snapshot = admin::cron_audit_snapshot(&platform);
        let row = snapshot
            .schedules
            .iter()
            .find(|row| row["task"] == "raw_delete")
            .unwrap();
        if let Some(status) = expected_status {
            assert_eq!(
                row["execution"],
                json!({
                    "service_id": "fixtureService", "last_run": platform.now(), "status": status,
                    "more_pending": true, "retry_after": platform.now() + 30,
                })
            );
        } else {
            assert!(row["execution"].is_null());
        }
        assert!(!serde_json::to_string(&snapshot.schedules)
            .unwrap()
            .contains("unexpected-private-detail"));
    }
}

#[test]
fn cron_schedule_natural_idle_raw_delete_records_the_executing_service_without_inventing_a_run() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    platform.shield.store(true, Ordering::SeqCst);
    CALLS.with(|c| c.set(0));
    SECRETS.with(|s| s.borrow_mut().clear());
    admin::record_tick(&platform, "clock", "clock").unwrap();
    let state = platform.adapter.get("cron:state").unwrap().unwrap().value;
    assert_eq!(state["jobs"]["raw_delete"]["last_status"], "idle");
    assert_eq!(state["jobs"]["raw_delete"]["service_id"], "fixtureService");
    let snapshot = admin::cron_audit_snapshot(&platform);
    let row = snapshot
        .schedules
        .iter()
        .find(|row| row["task"] == "raw_delete")
        .unwrap();
    assert_eq!(row["execution"]["status"], "idle");
    assert_eq!(row["execution"]["last_run"], platform.now());
    assert_eq!(snapshot.runs.len(), 1);
}

#[test]
fn metadata_status_fresh_bypasses_cache_and_reads_one_post_extents_cron_snapshot() {
    use fastly_log_analytics::{
        auth::{self, webauthn::Credential, AuthState, Role, User},
        shell::Shell,
        state::{MemoryStore, StateStore},
    };
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;

    let platform = Platform::new();
    platform.enable_status_cache.store(true, Ordering::SeqCst);
    let job = platform.provision();
    platform.drain(job["job"].clone());
    let store = MemoryStore::default();
    let user = User {
        id: "metadata-admin".into(),
        display_name: "Admin".into(),
        role: Role::Admin,
        credentials: vec![Credential {
            id: "cred".into(),
            public_key: vec![1],
            sign_count: 0,
            revoked: false,
        }],
        revoked: false,
        created_by: "setup".into(),
        created_at: platform.now(),
        last_login_at: None,
    };
    let mut state = AuthState::default();
    state.users.insert(user.id.clone(), user.clone());
    assert!(store.compare_and_swap(None, &state).unwrap());
    let claims = auth::encode(
        json!({"sub":user.id,"role":user.role,"policy_version":0,"exp":platform.now()+3600})
            .to_string()
            .as_bytes(),
    );
    let mut mac =
        Hmac::<Sha256>::new_from_slice(b"fixture-session-signing-material-32-bytes").unwrap();
    mac.update(claims.as_bytes());
    let cookie = format!(
        "flp_session={claims}.{}",
        auth::encode(&mac.finalize().into_bytes())
    );
    let fetch = |path: &str, cookie: Option<&str>| {
        CALLS.with(|c| c.set(0));
        SECRETS.with(|s| s.borrow_mut().clear());
        platform.metadata_reads.lock().unwrap().clear();
        let mut headers = BTreeMap::from([
            ("x-flp-front-key".into(), "fixture-front-key".into()),
            ("fastly-client-ip".into(), "192.0.2.1".into()),
            ("x-flp-front-rl".into(), "1".into()),
        ]);
        if let Some(cookie) = cookie {
            headers.insert("cookie".into(), cookie.into());
        }
        Shell::new(&platform, &store, &[]).handle(&Request {
            method: "GET".into(),
            path: path.into(),
            headers,
            body: vec![],
        })
    };
    let before_fos = platform.fos_timeouts.lock().unwrap().len();
    let warm = fetch("/api/sync-status", Some(&cookie));
    let extents_fos = platform.fos_timeouts.lock().unwrap().len() - before_fos;
    assert_eq!(warm.status, 200, "{}", String::from_utf8_lossy(&warm.body));
    let stale: Value = serde_json::from_slice(&warm.body).unwrap();
    let old = platform.adapter.get("svc:fixtureService").unwrap().unwrap();
    let mut service = old.value;
    service["_audit_outbox"] = json!([
        {"at":1800000001u64,"event":"older"},
        {"at":1800000002u64,"event":"newest"}
    ]);
    assert!(platform
        .adapter
        .cas("svc:fixtureService", Some(&old.generation), &service)
        .unwrap());
    assert_eq!(fetch("/api/sync-status", Some(&cookie)).body, warm.body);
    assert!(!platform
        .metadata_reads
        .lock()
        .unwrap()
        .iter()
        .any(|key| key == "cron:state"));
    *platform.cron_during_extents.lock().unwrap() = Some(json!({
        "last_tick":1800000003u64, "last_ingest_at":1800000003u64, "last_ingest_service_id":"fixtureService",
        "runs":[{
            "id":"during-extents", "started_at":"2027-01-15T08:00:03Z", "finished_at":"2027-01-15T08:00:03Z",
            "duration_ms":0,"outcome":"done","trigger":"clock","actor":"clock","bucket":1,
            "service_id":"fixtureService","listed_raw_files":1,"converted_raw_files":1,
            "deleted_raw_files":0,"rows_written":1,"bytes_written":1,"backend_requests_used":1,
            "fos_manifest_key":null,"error":null
        }]
    }));
    let before_fos = platform.fos_timeouts.lock().unwrap().len();
    let fresh = fetch("/api/sync-status?fresh=true", Some(&cookie));
    assert_eq!(
        platform.fos_timeouts.lock().unwrap().len() - before_fos,
        extents_fos
    );
    assert_eq!(fresh.status, 200);
    assert_eq!(fresh.headers["cache-control"], "private, no-store");
    let value: Value = serde_json::from_slice(&fresh.body).unwrap();
    assert_eq!(value["cron_runs"][0]["id"], "during-extents");
    assert_eq!(value["latest_ingested_file_at"], "2027-01-15T08:00:03Z");
    assert_eq!(value["audit_logs"][0]["action"], "newest");
    assert_eq!(
        platform
            .metadata_reads
            .lock()
            .unwrap()
            .iter()
            .filter(|k| k.as_str() == "cron:state")
            .count(),
        1
    );
    assert!(!platform
        .metadata_reads
        .lock()
        .unwrap()
        .iter()
        .any(|k| k == &admin::status_cache_key("sync-status", "fixtureService")));
    assert_ne!(value, stale);
    for path in ["/api/sync-status", "/api/sync-status?fresh=false"] {
        assert_eq!(fetch(path, Some(&cookie)).body, fresh.body);
        assert!(!platform
            .metadata_reads
            .lock()
            .unwrap()
            .iter()
            .any(|k| k == "cron:state"));
    }
    platform.shield.store(true, Ordering::SeqCst);
    let audit = fetch("/api/admin/audit-logs", Some(&cookie));
    let audit: Value = serde_json::from_slice(&audit.body).unwrap();
    assert_eq!(audit["entries"], value["audit_logs"]);
    assert!(!platform
        .metadata_reads
        .lock()
        .unwrap()
        .iter()
        .any(|k| k == "cron:state" || k == admin::schedule::SCHED_KEY));
    platform.shield.store(false, Ordering::SeqCst);
    let cron = fetch("/api/cron-runs", Some(&cookie));
    let cron: Value = serde_json::from_slice(&cron.body).unwrap();
    assert_eq!(cron["runs"], value["cron_runs"]);
    assert_eq!(cron["last_ingest_at"], value["latest_ingested_file_at"]);
    let schedules = cron["schedules"].as_array().expect("native task schedules");
    let tasks: Vec<_> = schedules
        .iter()
        .map(|row| row["task"].as_str().unwrap())
        .collect();
    assert!(tasks.contains(&"request_ingest"));
    assert!(tasks.contains(&"log_flush"));
    assert!(tasks.contains(&"cloud_ingest_clock"));
    for task in admin::logs::MAINT_STAGES {
        assert!(tasks.contains(&task), "missing maintenance task {task}");
    }
    let rum = schedules
        .iter()
        .find(|row| row["task"] == "rum_ingest")
        .unwrap();
    assert_eq!(rum["disabled_reason"], "RUM is disabled");
    assert!(rum["next_run_time"].is_null());
    assert_eq!(
        platform
            .metadata_reads
            .lock()
            .unwrap()
            .iter()
            .filter(|k| k.as_str() == "cron:state")
            .count(),
        1
    );
    assert_eq!(
        fetch("/api/sync-status?fresh=invalid", Some(&cookie)).status,
        422
    );
    assert_eq!(fetch("/api/sync-status?fresh=true", None).status, 401);
    let install = platform.adapter.get("admin:install").unwrap().unwrap();
    let mut uninstalled = install.value;
    uninstalled["service_id"] = Value::Null;
    assert!(platform
        .adapter
        .cas("admin:install", Some(&install.generation), &uninstalled)
        .unwrap());
    let absent = fetch("/api/admin/audit-logs", Some(&cookie));
    assert_eq!(absent.status, 200);
    assert_eq!(
        serde_json::from_slice::<Value>(&absent.body).unwrap(),
        json!({"entries":[]})
    );
    assert!(!platform
        .metadata_reads
        .lock()
        .unwrap()
        .iter()
        .any(|key| key == "cron:state"));
    let mut state = store.load().unwrap();
    state.state.users.get_mut(&user.id).unwrap().role = Role::Analyst;
    assert!(store
        .compare_and_swap(state.generation.as_deref(), &state.state)
        .unwrap());
    assert_eq!(
        fetch("/api/sync-status?fresh=true", Some(&cookie)).status,
        403
    );
}

#[test]
fn shield_ticks_manual_trigger_fos_manifest_and_audit_logs_end_to_end() {
    use fastly_log_analytics::{
        auth::{self, webauthn::Credential, AuditEntry, AuthState, Role, User},
        shell::Shell,
        state::{MemoryStore, StateStore},
    };
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;

    let platform = Platform::new();

    let user = User {
        id: "admin-user".into(),
        display_name: "Admin".into(),
        role: Role::Admin,
        credentials: vec![Credential {
            id: "cred-1".into(),
            public_key: vec![1],
            sign_count: 0,
            revoked: false,
        }],
        revoked: false,
        created_by: "setup".into(),
        created_at: platform.now(),
        last_login_at: None,
    };
    let mut auth_state = AuthState::default();
    auth_state.users.insert(user.id.clone(), user.clone());
    auth_state.recent_audit.push(AuditEntry {
        time: platform.now().saturating_sub(5),
        actor: "setup".into(),
        action: "setup.complete".into(),
        target: user.id.clone(),
        ip: "192.0.2.1".into(),
    });
    let store = MemoryStore::default();
    assert!(store.compare_and_swap(None, &auth_state).unwrap());

    let claims = auth::encode(
        json!({"sub":user.id,"role":user.role,"policy_version":0,"exp":platform.now()+3600})
            .to_string()
            .as_bytes(),
    );
    let mut mac =
        Hmac::<Sha256>::new_from_slice(b"fixture-session-signing-material-32-bytes").unwrap();
    mac.update(claims.as_bytes());
    let cookie = format!(
        "flp_session={claims}.{}",
        auth::encode(&mac.finalize().into_bytes())
    );

    let tick_req = Request {
        method: "GET".into(),
        path: "/_tick".into(),
        headers: BTreeMap::from([("x-flp-front-key".into(), "fixture-front-key".into())]),
        body: vec![],
    };

    // 0. An off-shield probe returns 200 without work: no heartbeat, no CronRun row, and a
    // manual trigger before provisioning returns 409.
    CALLS.with(|c| c.set(0));
    SECRETS.with(|s| s.borrow_mut().clear());
    let resp0 = Shell::new(&platform, &store, &[]).handle(&tick_req);
    assert_eq!(resp0.status, 200);
    assert_eq!(
        resp0.headers.get("cache-control").map(String::as_str),
        Some("public, max-age=1")
    );
    let sync_before_req = Request {
        method: "GET".into(),
        path: "/api/sync-status".into(),
        headers: BTreeMap::from([
            ("x-flp-front-key".into(), "fixture-front-key".into()),
            ("fastly-client-ip".into(), "192.0.2.1".into()),
            ("x-flp-front-rl".into(), "1".into()),
            ("cookie".into(), cookie.clone()),
        ]),
        body: vec![],
    };
    CALLS.with(|c| c.set(0));
    SECRETS.with(|s| s.borrow_mut().clear());
    let sync_before_resp = Shell::new(&platform, &store, &[]).handle(&sync_before_req);
    assert_eq!(sync_before_resp.status, 200);
    let sync_before_body: Value = serde_json::from_slice(&sync_before_resp.body).unwrap();
    assert!(sync_before_body["last_tick_at"].is_null());
    assert_eq!(sync_before_body["cron_runs"].as_array().unwrap().len(), 0);

    let unconfigured_trigger_req = Request {
        method: "POST".into(),
        path: "/api/cron-runs/trigger".into(),
        headers: BTreeMap::from([
            ("x-flp-front-key".into(), "fixture-front-key".into()),
            ("fastly-client-ip".into(), "192.0.2.1".into()),
            ("x-flp-front-rl".into(), "1".into()),
            ("origin".into(), "https://logs.example.com".into()),
            ("content-type".into(), "application/json".into()),
            ("cookie".into(), cookie.clone()),
        ]),
        body: b"{}".to_vec(),
    };
    CALLS.with(|c| c.set(0));
    SECRETS.with(|s| s.borrow_mut().clear());
    let unconfigured_trigger_resp =
        Shell::new(&platform, &store, &[]).handle(&unconfigured_trigger_req);
    assert_eq!(unconfigured_trigger_resp.status, 409);

    // Provision a service and move the clock onto the shield.
    platform.drain(platform.provision()["job"].clone());
    platform.clock.fetch_add(120, Ordering::SeqCst);
    platform.shield.store(true, Ordering::SeqCst);

    // 1-3. Shield ticks a second apart, then a period later. With no raw files nothing runs,
    // so nothing is written to FOS or to the run history. When Core Cache is unavailable, the
    // response falls back to a 1s TTL so VCL keeps polling until the schedule record returns.
    for step in [0, 1, 120] {
        platform.clock.fetch_add(step, Ordering::SeqCst);
        CALLS.with(|c| c.set(0));
        SECRETS.with(|s| s.borrow_mut().clear());
        let resp = Shell::new(&platform, &store, &[]).handle(&tick_req);
        assert_eq!(resp.status, 200);
        assert_eq!(
            resp.headers.get("x-flp-tick-ttl").map(String::as_str),
            Some("1")
        );
    }
    assert!(platform
        .adapter
        .object_keys("fos-fixture-logs")
        .iter()
        .all(|k| !k.starts_with("meta/cron-runs/")));

    // 4. Manual trigger via POST /api/cron-runs/trigger -> writes meta/cron-runs/... JSON to FOS.
    let trigger_req = Request {
        method: "POST".into(),
        path: "/api/cron-runs/trigger".into(),
        headers: BTreeMap::from([
            ("x-flp-front-key".into(), "fixture-front-key".into()),
            ("fastly-client-ip".into(), "192.0.2.1".into()),
            ("x-flp-front-rl".into(), "1".into()),
            ("origin".into(), "https://logs.example.com".into()),
            ("content-type".into(), "application/json".into()),
            ("cookie".into(), cookie.clone()),
        ]),
        body: b"{}".to_vec(),
    };
    CALLS.with(|c| c.set(0));
    SECRETS.with(|s| s.borrow_mut().clear());
    let trigger_resp = Shell::new(&platform, &store, &[]).handle(&trigger_req);
    assert_eq!(
        trigger_resp.status,
        200,
        "{}",
        String::from_utf8_lossy(&trigger_resp.body)
    );
    let trigger_body: Value = serde_json::from_slice(&trigger_resp.body).unwrap();
    assert_eq!(trigger_body["run"]["trigger"], "manual");
    assert_eq!(trigger_body["run"]["outcome"], "done");
    assert_eq!(trigger_body["run"]["actor"], "admin-user");
    let manifest_key = trigger_body["run"]["fos_manifest_key"]
        .as_str()
        .expect("manual trigger should write FOS manifest")
        .to_owned();
    assert!(manifest_key.starts_with("meta/cron-runs/dt="));
    assert!(manifest_key.ends_with(".json"));

    let bucket_objects = platform.adapter.bucket_objects("fos-fixture-logs");
    let manifest_bytes = bucket_objects
        .get(&manifest_key)
        .expect("manifest object must exist in FOS bucket");
    let manifest_json: Value = serde_json::from_slice(manifest_bytes).unwrap();
    assert_eq!(manifest_json["id"], trigger_body["run"]["id"]);
    assert_eq!(manifest_json["service_id"], "fixtureService");

    // Newest first: the manual run, then the two due ingests; the tick between wrote nothing.
    let runs = trigger_body["runs"].as_array().unwrap();
    let outcomes: Vec<_> = runs.iter().map(|r| r["outcome"].clone()).collect();
    assert_eq!(outcomes, ["done", "idle", "idle"]);
    assert_eq!(runs[0]["trigger"], "manual");

    // 5. Verify GET /api/sync-status and GET /api/admin/audit-logs include both cron runs and merged audit logs.
    for path in [
        "/api/sync-status",
        "/api/admin/audit-logs",
        "/api/cron-runs",
    ] {
        let get_req = Request {
            method: "GET".into(),
            path: path.into(),
            headers: BTreeMap::from([
                ("x-flp-front-key".into(), "fixture-front-key".into()),
                ("fastly-client-ip".into(), "192.0.2.1".into()),
                ("x-flp-front-rl".into(), "1".into()),
                ("cookie".into(), cookie.clone()),
            ]),
            body: vec![],
        };
        CALLS.with(|c| c.set(0));
        SECRETS.with(|s| s.borrow_mut().clear());
        let get_resp = Shell::new(&platform, &store, &[]).handle(&get_req);
        assert_eq!(get_resp.status, 200, "{path}");
        let body: Value = serde_json::from_slice(&get_resp.body).unwrap();
        if path == "/api/sync-status" {
            assert_eq!(body["configured"], true);
            // Half the fixture's 60 s log period: the front clock health check interval.
            assert_eq!(body["clock_interval_s"], 30);
            assert!(body["last_tick_at"].is_string());
            assert_eq!(body["request"]["last_sync_at"], body["last_tick_at"]);
            assert_eq!(body["cron_runs"].as_array().unwrap().len(), 3);
            let audit = body["audit_logs"].as_array().unwrap();
            assert!(audit.iter().any(|e| e["action"] == "job.completed"));
            assert!(audit.iter().all(|e| e["category"] == "control_plane"));
            assert!(!audit.iter().any(|e| e["action"] == "setup.complete"));
        } else if path == "/api/admin/audit-logs" {
            let entries = body["entries"].as_array().unwrap();
            assert!(entries.iter().any(|e| e["action"] == "job.completed"));
            assert!(entries.iter().all(|e| e["category"] == "control_plane"));
            assert!(!entries.iter().any(|e| e["action"] == "setup.complete"));
        } else {
            assert_eq!(body["configured"], true);
            assert_eq!(body["service_id"], "fixtureService");
            assert_eq!(body["runs"].as_array().unwrap().len(), 3);
            assert_eq!(body["clock_interval_s"], 30);
            assert_eq!(
                body["clock"],
                json!({"probe_interval_s": 30.0, "log_period_s": 60, "rum_log_period_s": null})
            );
        }
    }
}

#[test]
fn cmcd_provision_reconcile_disable_and_teardown_preserve_owned_typed_fields() {
    let platform = Platform::new();
    let result = platform.json(
        "POST",
        "/api/admin/provision/execute",
        json!({
            "token":"fixture-token","service_id":"fixtureService","fos_bucket":"fos-fixture-logs",
            "fos_region":"us-east-1","cmcd":{"enabled":true,"mode":"headers","version":2}
        }),
    );
    platform.drain(result["job"].clone());
    let current = platform.service();
    assert_eq!(
        current["service"]["cmcd"],
        json!({"enabled":true,"mode":"headers","version":2})
    );
    let custom = current["service"]["log_fields"]["custom_fields"]
        .as_array()
        .unwrap();
    assert_eq!(custom.len(), 14);
    for field in custom {
        assert_eq!(field["system_managed"], true);
        assert_eq!(field["committed"], true);
        assert_eq!(field["enabled"], true);
    }
    let bs = custom.iter().find(|f| f["name"] == "cmcd_bs").unwrap();
    assert_eq!(bs["parquet_type"], "Int64");
    let sid = custom.iter().find(|f| f["name"] == "cmcd_sid").unwrap();
    assert_eq!(sid["byte_limit"], 288);
    let snippets = platform.adapter.resources("fixtureService", "snippet");
    let recv = snippets
        .iter()
        .find(|s| s["name"] == "Fastly Log Analytics - vcl_recv")
        .unwrap()["content"]
        .as_str()
        .unwrap();
    assert!(
        recv.find("unset req.http.x-cmcd;").unwrap() < recv.find("CMCD v2 header-mode").unwrap()
    );
    assert!(
        recv.find("CMCD v2 header-mode").unwrap()
            < recv
                .find("\n  set req.http.x-fos-edge-data:cmcd_sid")
                .unwrap()
    );
    assert_eq!(
        platform
            .adapter
            .resources("fixtureService", "logging/fos")
            .len(),
        1
    );
    let job = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({
            "generation":current["generation"],"cmcd":{"enabled":false,"mode":"headers","version":2}
        }),
    )["job"]
        .clone();
    let pending = platform.service();
    for (method, path) in [
        ("PATCH", "/api/admin/services/fixtureService"),
        ("POST", "/api/admin/services/fixtureService/reconcile"),
    ] {
        assert_eq!(platform.call(method,path,json!({
            "generation":pending["generation"],"cmcd":{"enabled":true,"mode":"query_string","version":1}
        })).status,409);
    }
    assert_eq!(
        platform.service()["service"]["cmcd"],
        pending["service"]["cmcd"]
    );
    platform.drain(job);
    let disabled = platform.service();
    assert_eq!(
        disabled["service"]["log_fields"]["custom_fields"]
            .as_array()
            .unwrap()
            .len(),
        14
    );
    assert!(disabled["service"]["log_fields"]["custom_fields"]
        .as_array()
        .unwrap()
        .iter()
        .all(|f| f["enabled"] == false && f["committed"] == true));
    let endpoints = platform.adapter.resources("fixtureService", "logging/fos");
    assert!(!endpoints[0]["format"]
        .as_str()
        .unwrap()
        .contains("\"cmcd_sid\""));
    let snippets = platform.adapter.resources("fixtureService", "snippet");
    assert!(snippets.iter().all(|s| !s["content"]
        .as_str()
        .unwrap_or("")
        .contains("CMCD v2 header-mode")));
    let job=platform.json("POST","/api/admin/services/fixtureService/reconcile",json!({
        "generation":disabled["generation"],"cmcd":{"enabled":true,"mode":"query_string","version":1}
    }))["job"].clone();
    let job = platform.until(job, "activate");
    let path = format!(
        "/service/fixtureService/version/{}/activate",
        job["draft_version"].as_u64().unwrap()
    );
    platform.adapter.fail_next("PUT", &path, true);
    let failed = platform.json(
        "POST",
        &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
        json!({}),
    );
    assert_eq!(failed["job"]["status"], "failed");
    platform.drain(platform.retry(&job));
    assert_eq!(
        platform
            .adapter
            .calls()
            .iter()
            .filter(|(m, p)| m == "PUT" && p == &path)
            .count(),
        1
    );
    let current = platform.service();
    let job=platform.json("POST","/api/admin/provision/teardown",json!({
        "token":"fixture-token","service_id":"fixtureService","confirm_service_id":"fixtureService",
        "generation":current["generation"],"remove_logging":true,"remove_snippets":true,
        "delete_fos_keys":true,"delete_data":false,"delete_fos_bucket":false
    }))["job"].clone();
    platform.drain(job);
    assert!(platform
        .adapter
        .resources("fixtureService", "snippet")
        .iter()
        .all(|s| !s["content"].as_str().unwrap_or("").contains("CMCD")));
    assert_eq!(
        platform.service()["service"]["log_fields"]["custom_fields"]
            .as_array()
            .unwrap()
            .len(),
        14
    );
    platform.drain(platform.provision()["job"].clone());
    let reinstalled = platform.service();
    assert_eq!(reinstalled["service"]["cmcd"]["enabled"], false);
    assert_eq!(
        reinstalled["service"]["log_fields"]["custom_fields"]
            .as_array()
            .unwrap()
            .len(),
        14
    );
    assert!(reinstalled["service"]["log_fields"]["custom_fields"]
        .as_array()
        .unwrap()
        .iter()
        .all(|field| field["committed"] == true && field["enabled"] == false));
}

#[test]
fn cmcd_settings_and_managed_fields_cannot_be_forged_or_mutated() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    let current = platform.service();
    for cmcd in [
        json!({"enabled":true,"mode":"cookie","version":1}),
        json!({"enabled":true,"mode":"headers","version":3}),
        json!({"enabled":"yes","mode":"headers","version":1}),
    ] {
        assert_eq!(
            platform
                .call(
                    "PATCH",
                    "/api/admin/services/fixtureService",
                    json!({
                        "generation":current["generation"],"cmcd":cmcd
                    })
                )
                .status,
            400
        );
    }
    let collision =
        json!({"name":"cmcd_sid","vcl":"req.http.Evil","parquet_type":"Utf8","enabled":true});
    let mut fields = current["service"]["log_fields"].clone();
    fields["custom_fields"] = json!([collision.clone()]);
    assert_eq!(platform.call("PATCH","/api/admin/services/fixtureService",json!({
        "generation":current["generation"],"cmcd":{"enabled":true,"mode":"headers","version":1},
        "log_fields":fields
    })).status,400);
    assert_eq!(
        platform.json(
            "POST",
            "/api/admin/log-fields/custom/validate",
            json!({"field":collision})
        )["valid"],
        false
    );
    let enabled=platform.json("PATCH","/api/admin/services/fixtureService",json!({
        "generation":current["generation"],"cmcd":{"enabled":true,"mode":"query_string","version":1}
    }));
    for (key, value) in [
        ("vcl", json!("req.http.Evil")),
        ("parquet_type", json!("Boolean")),
        ("byte_limit", json!(16384)),
        ("enabled", json!(false)),
        ("system_managed", json!(false)),
    ] {
        let mut fields = enabled["service"]["log_fields"].clone();
        fields["custom_fields"][0][key] = value;
        assert_eq!(
            platform
                .call(
                    "PATCH",
                    "/api/admin/services/fixtureService",
                    json!({
                        "generation":enabled["generation"],"log_fields":fields
                    })
                )
                .status,
            400,
            "{key}"
        );
    }
    let mut fields = enabled["service"]["log_fields"].clone();
    fields["custom_fields"] = json!([]);
    let restored = platform.json(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({
            "generation":enabled["generation"],"log_fields":fields
        }),
    );
    assert_eq!(
        restored["service"]["log_fields"]["custom_fields"]
            .as_array()
            .unwrap()
            .len(),
        14
    );
    let mut persisted = restored["service"]["log_fields"].clone();
    for field in persisted["custom_fields"].as_array_mut().unwrap() {
        field.as_object_mut().unwrap().remove("committed");
    }
    let preview = platform.json(
        "POST",
        "/api/admin/log-fields/preview",
        json!({
            "log_fields":persisted,"cmcd":restored["service"]["cmcd"]
        }),
    );
    assert!(preview["format"].as_str().unwrap().contains("\"cmcd_sid\""));
    let limits=platform.json("PATCH","/api/admin/log-fields/limits",json!({
        "service_id":"fixtureService","generation":restored["generation"],"field_limits":{"url":1000}
    }));
    assert_eq!(
        limits["log_fields"]["custom_fields"],
        restored["service"]["log_fields"]["custom_fields"]
    );
    let preview = platform.json(
        "POST",
        "/api/admin/log-fields/preview",
        json!({
            "log_fields":{"groups":["A"],"custom_fields":[],"field_overrides":{},"field_limits":{}},
            "cmcd":{"enabled":true,"mode":"headers","version":2}
        }),
    );
    assert!(preview["format"].as_str().unwrap().contains("\"cmcd_sid\""));
}

#[test]
fn cmcd_snapshot_recovery_does_not_replace_committed_capture_configuration() {
    let platform = Platform::new();
    platform
        .adapter
        .fail_next("KV_CAS", "svc:fixtureService", false);
    let input = json!({"token":"fixture-token","service_id":"fixtureService","fos_bucket":"fos-fixture-logs",
        "fos_region":"us-east-1","cmcd":{"enabled":true,"mode":"headers","version":2}});
    assert_eq!(
        platform
            .call("POST", "/api/admin/provision/execute", input)
            .status,
        503
    );
    platform.drain(platform.provision()["job"].clone());
    let current = platform.service();
    assert_eq!(
        current["service"]["cmcd"],
        json!({"enabled":true,"mode":"headers","version":2})
    );
    assert_eq!(
        current["service"]["log_fields"]["custom_fields"]
            .as_array()
            .unwrap()
            .len(),
        14
    );
}

#[test]
fn cmcd_draft_validation_failure_stays_inactive_and_retries_the_same_fields() {
    let platform = Platform::new();
    let response=platform.json("POST","/api/admin/provision/execute",json!({
        "token":"fixture-token","service_id":"fixtureService","fos_bucket":"fos-fixture-logs","fos_region":"us-east-1",
        "cmcd":{"enabled":true,"mode":"headers","version":2}
    }));
    let job = platform.until(response["job"].clone(), "validate");
    platform.adapter.invalid_draft(true);
    let failed = platform.json(
        "POST",
        &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
        json!({}),
    );
    assert_eq!(failed["job"]["error"], "draft_validation_failed");
    assert!(platform
        .adapter
        .calls()
        .iter()
        .all(|(_, path)| !path.starts_with("/service/fixtureService/")
            || !path.ends_with("/activate")));
    platform.adapter.invalid_draft(false);
    platform.drain(platform.retry(&job));
    assert_eq!(
        platform.service()["service"]["cmcd"],
        json!({"enabled":true,"mode":"headers","version":2})
    );
    assert_eq!(
        platform.service()["service"]["log_fields"]["custom_fields"]
            .as_array()
            .unwrap()
            .len(),
        14
    );
}

#[test]
fn cmcd_capture_never_adopts_an_existing_extraction_snippet_or_local_variable() {
    let platform = Platform::new();
    platform.adapter.seed_resource("fixtureService","snippet",json!({
        "name":"CMCD Extraction","type":"recv","priority":1,"content":"set req.http.Customer-CMCD = \"owned\";"
    }));
    let response=platform.call("POST","/api/admin/provision/execute",json!({
        "token":"fixture-token","service_id":"fixtureService","fos_bucket":"fos-fixture-logs","fos_region":"us-east-1",
        "cmcd":{"enabled":true,"mode":"headers","version":1}
    }));
    assert_eq!(response.status, 409);
    assert!(platform
        .adapter
        .records()
        .keys()
        .all(|key| !key.starts_with("job:")));
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    platform.adapter.seed_resource("fixtureService","snippet",json!({
        "name":"Customer recv","type":"recv","priority":1,"content":"declare local var.cmcd STRING;"
    }));
    let current = platform.service();
    let job=platform.json("POST","/api/admin/services/fixtureService/reconcile",json!({
        "generation":current["generation"],"cmcd":{"enabled":true,"mode":"query_string","version":1}
    }))["job"].clone();
    let job = platform.until(job, "discover");
    let failed = platform.json(
        "POST",
        &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
        json!({}),
    );
    assert_eq!(failed["job"]["status"], "failed");
    assert_eq!(failed["job"]["error"], "cmcd_vcl_variable_collision");
    assert!(failed["job"]["draft_version"].is_null());
}

#[test]
fn teardown_preserves_customer_cmcd_snippet_added_after_installation() {
    for enabled in [false, true] {
        let platform = Platform::new();
        let installed = platform.json("POST", "/api/admin/provision/execute", json!({
            "token":"fixture-token","service_id":"fixtureService","fos_bucket":"fos-fixture-logs",
            "fos_region":"us-east-1","cmcd":{"enabled":enabled,"mode":"headers","version":2}
        }));
        platform.drain(installed["job"].clone());
        let customer = json!({
            "name":"CMCD Extraction","type":"recv","priority":1,
            "content":"declare local var.cmcd STRING;\nset req.http.Customer-CMCD = \"customer-owned\";"
        });
        platform
            .adapter
            .seed_resource("fixtureService", "snippet", customer.clone());
        let current = platform.service();
        let teardown = platform.json("POST", "/api/admin/provision/teardown", json!({
            "token":"fixture-token","service_id":"fixtureService","confirm_service_id":"fixtureService",
            "generation":current["generation"],"remove_logging":true,"remove_snippets":true,
            "delete_fos_keys":true,"delete_data":false,"delete_fos_bucket":false
        }));
        platform.drain(teardown["job"].clone());
        assert_eq!(platform.service()["service"]["status"], "torn_down");
        assert_eq!(
            platform.adapter.resources("fixtureService", "snippet"),
            vec![customer]
        );
        assert!(platform
            .adapter
            .resources("fixtureService", "logging/fos")
            .is_empty());
    }
}

#[test]
fn sec_coex_vcl_31a_spoofed_continue_without_tick_pass_does_not_suppress_clock_continuation() {
    use fastly_log_analytics::{
        setup::SetupStores,
        shell::{CoreCacheEntry, CoreCacheError, Runtime, Shell},
        state::MemoryStore,
    };
    use std::cell::RefCell;

    struct ClockContinueRt {
        now_s: u64,
        continued: RefCell<Vec<(String, u32)>>,
    }
    impl SetupStores for ClockContinueRt {
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
    impl Runtime for ClockContinueRt {
        fn config(&self, _: &str) -> Result<Option<String>, String> {
            Ok(None)
        }
        fn secret(&self, name: &str) -> Result<Option<String>, String> {
            Ok((name == "front_key").then(|| "fixture-front-key".into()))
        }
        fn random(&self) -> Result<[u8; 32], String> {
            Ok([7; 32])
        }
        fn now(&self) -> u64 {
            self.now_s
        }
        fn checks(&self, _: bool) -> Result<Value, String> {
            Ok(json!({}))
        }
        fn versions(&self) -> Result<Value, String> {
            Ok(json!({}))
        }
        fn compute_service_id(&self) -> Option<String> {
            Some("fixtureApp".into())
        }
        fn compute_service_version(&self) -> Option<String> {
            Some("1".into())
        }
        fn on_log_shield(&self) -> bool {
            true
        }
        fn core_cache_lookup(&self, key: &str) -> Result<Option<CoreCacheEntry>, CoreCacheError> {
            if matches!(
                key,
                fastly_log_analytics::admin::schedule::SCHED_KEY
                    | fastly_log_analytics::admin::schedule::FLUSH_SCHED_KEY
            ) {
                let stale = fastly_log_analytics::admin::schedule::Sched::default();
                return Ok(Some((serde_json::to_vec(&stale).unwrap(), Vec::new())));
            }
            Ok(None)
        }
        fn core_cache_replace(
            &self,
            _: &str,
            _: &[&str],
            _: std::time::Duration,
            _: &dyn Fn(Option<CoreCacheEntry>) -> CoreCacheEntry,
        ) -> Result<(), CoreCacheError> {
            Ok(())
        }
        fn continue_clock(&self, path: &str, depth: u32) -> Result<(), String> {
            self.continued.borrow_mut().push((path.into(), depth));
            Ok(())
        }
    }

    let store = MemoryStore::default();
    let rt = ClockContinueRt {
        now_s: 1_800_000_000,
        continued: RefCell::new(Vec::new()),
    };

    // 1. Spoofed x-flp-continue: 20 WITHOUT x-flp-tick-pass must NOT suppress self-continuation on /_tick or /_flush!
    for path in ["/_tick", "/_flush"] {
        rt.continued.borrow_mut().clear();
        let spoofed = Request {
            method: "GET".into(),
            path: path.into(),
            headers: BTreeMap::from([
                ("x-flp-front-key".into(), "fixture-front-key".into()),
                ("x-flp-continue".into(), "20".into()),
            ]),
            body: vec![],
        };
        let resp = Shell::new(&rt, &store, &[]).handle(&spoofed);
        assert_eq!(resp.status, 200, "path={path}");
        assert_eq!(
            *rt.continued.borrow(),
            vec![(path.to_owned(), 1)],
            "SEC-COEX-VCL-31A: spoofed x-flp-continue: 20 without x-flp-tick-pass suppressed continuation on {path}"
        );

        // Wrong x-flp-tick-pass value ("0") must ALSO ignore spoofed x-flp-continue: 20:
        rt.continued.borrow_mut().clear();
        let bad_pass = Request {
            method: "GET".into(),
            path: path.into(),
            headers: BTreeMap::from([
                ("x-flp-front-key".into(), "fixture-front-key".into()),
                ("x-flp-continue".into(), "20".into()),
                ("x-flp-tick-pass".into(), "0".into()),
            ]),
            body: vec![],
        };
        let resp_bad = Shell::new(&rt, &store, &[]).handle(&bad_pass);
        assert_eq!(resp_bad.status, 200, "path={path}");
        assert_eq!(*rt.continued.borrow(), vec![(path.to_owned(), 1)]);

        // Authorized continuation (x-flp-tick-pass: 1 + x-flp-continue: 4) advances to depth 5:
        rt.continued.borrow_mut().clear();
        let auth_mid = Request {
            method: "GET".into(),
            path: path.into(),
            headers: BTreeMap::from([
                ("x-flp-front-key".into(), "fixture-front-key".into()),
                ("x-flp-continue".into(), "4".into()),
                ("x-flp-tick-pass".into(), "1".into()),
            ]),
            body: vec![],
        };
        let resp_mid = Shell::new(&rt, &store, &[]).handle(&auth_mid);
        assert_eq!(resp_mid.status, 200, "path={path}");
        assert_eq!(*rt.continued.borrow(), vec![(path.to_owned(), 5)]);

        // Authorized continuation at cap (x-flp-tick-pass: 1 + x-flp-continue: 20) stops chaining:
        rt.continued.borrow_mut().clear();
        let auth_cap = Request {
            method: "GET".into(),
            path: path.into(),
            headers: BTreeMap::from([
                ("x-flp-front-key".into(), "fixture-front-key".into()),
                ("x-flp-continue".into(), "20".into()),
                ("x-flp-tick-pass".into(), "1".into()),
            ]),
            body: vec![],
        };
        let resp_cap = Shell::new(&rt, &store, &[]).handle(&auth_cap);
        assert_eq!(resp_cap.status, 200, "path={path}");
        assert!(
            rt.continued.borrow().is_empty(),
            "authorized cap at depth 20 must stop chaining on {path}"
        );
    }
}
