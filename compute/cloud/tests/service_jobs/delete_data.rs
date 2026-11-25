use super::support::*;
use fastly_log_analytics::{
    admin::{self, Record},
    http::{Request, Response},
    shell::Runtime,
};
use serde_json::{json, Value};
use std::{
    cell::Cell,
    collections::{BTreeMap, BTreeSet},
    sync::atomic::Ordering,
};

fn shell_post(
    platform: &Platform,
    role: fastly_log_analytics::auth::Role,
    store: &fastly_log_analytics::state::MemoryStore,
    path: &str,
    body: Value,
) -> Response {
    use fastly_log_analytics::{
        auth::{self, webauthn::Credential, AuthState, User},
        shell::Shell,
        state::StateStore,
    };
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;
    if store.load().unwrap().generation.is_none() {
        let mut state = AuthState::default();
        state.users.insert(
            "signed-user".into(),
            User {
                id: "signed-user".into(),
                display_name: "Test".into(),
                role,
                credentials: vec![Credential {
                    id: "credential".into(),
                    public_key: vec![1],
                    sign_count: 0,
                    revoked: false,
                }],
                revoked: false,
                created_by: "test".into(),
                created_at: platform.now(),
                last_login_at: None,
            },
        );
        assert!(store.compare_and_swap(None, &state).unwrap());
    }
    let claims = auth::encode(
        json!({"sub":"signed-user","role":role,"policy_version":0,"exp":platform.now()+3600})
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
    CALLS.with(|calls| calls.set(7));
    SECRETS.with(|v| *v.borrow_mut() = BTreeSet::from(["front_key".into(), "session_key".into()]));
    Shell::new(platform, store, &[]).handle(&Request {
        method: "POST".into(),
        path: path.into(),
        headers: BTreeMap::from([
            ("x-flp-front-key".into(), "fixture-front-key".into()),
            ("fastly-client-ip".into(), "192.0.2.1".into()),
            ("x-flp-front-rl".into(), "1".into()),
            ("origin".into(), "https://logs.example.com".into()),
            ("content-type".into(), "application/json".into()),
            ("cookie".into(), cookie),
        ]),
        body: body.to_string().into_bytes(),
    })
}

fn provisioned() -> Platform {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    platform
}

const DELETE_DATA: &str = "/api/admin/services/fixtureService/delete-data";

fn delete_data_body(platform: &Platform) -> Value {
    json!({"confirm_service_id":"fixtureService","generation":platform.service()["generation"]})
}

#[test]
fn delete_data_requires_admin_and_matching_confirm_id() {
    use fastly_log_analytics::{auth::Role, state::MemoryStore};
    let platform = provisioned();
    let mut body = delete_data_body(&platform);
    body["confirm_service_id"] = "otherService".into();
    let response = platform.call("POST", DELETE_DATA, body);
    assert_eq!(response.status, 400);
    assert_eq!(
        serde_json::from_slice::<Value>(&response.body).unwrap()["error"],
        "confirm_service_id_mismatch"
    );
    for role in [Role::Analyst, Role::AnalystNoPii] {
        let store = MemoryStore::default();
        let response = shell_post(
            &platform,
            role,
            &store,
            DELETE_DATA,
            delete_data_body(&platform),
        );
        assert_eq!(response.status, 403, "{role:?}");
    }
    let records = platform.adapter.records();
    assert!(!records.contains_key(admin::reset::JOB_KEY));
    assert!(platform.service()["service"].get("data_reset").is_none());
}

#[test]
fn delete_data_start_sets_marker_job_and_audit() {
    use fastly_log_analytics::{
        auth::Role,
        state::{MemoryStore, StateStore},
    };
    let platform = provisioned();
    let store = MemoryStore::default();
    let now = platform.now();
    let response = shell_post(
        &platform,
        Role::Admin,
        &store,
        DELETE_DATA,
        delete_data_body(&platform),
    );
    assert_eq!(
        response.status,
        200,
        "{}",
        String::from_utf8_lossy(&response.body)
    );
    let started: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(started["job"]["status"], "fencing");
    let service = platform.service();
    let marker: admin::DataReset =
        serde_json::from_value(service["service"]["data_reset"].clone()).unwrap();
    assert_eq!(marker.phase, admin::ResetPhase::Fencing);
    assert_eq!(marker.reset_at, now - now % 60);
    assert_eq!(marker.started_at, now);
    assert_eq!(marker.actor, "signed-user");
    assert_eq!(marker.job_id, started["job"]["job_id"]);
    assert_eq!(
        started["service"]["data_reset"],
        service["service"]["data_reset"]
    );
    let records = platform.adapter.records();
    let job = &records[admin::reset::JOB_KEY].value;
    assert_eq!(job["status"], "fencing");
    assert_eq!(job["messages"].as_array().unwrap().len(), 1);
    assert_eq!(
        job["messages"][0]["text"],
        "Pausing ingest for this service…"
    );
    let state = store.load().unwrap().state;
    let entry = state
        .recent_audit
        .iter()
        .chain(&state.audit)
        .find(|entry| entry.action == "delete_data_started")
        .expect("audit entry");
    assert_eq!(entry.actor, "signed-user");
    assert_eq!(entry.target, "fixtureService");
}

#[test]
fn second_delete_data_start_is_rejected() {
    let platform = provisioned();
    let first = platform.json("POST", DELETE_DATA, delete_data_body(&platform));
    let response = platform.call("POST", DELETE_DATA, delete_data_body(&platform));
    assert_eq!(response.status, 409);
    assert_eq!(
        serde_json::from_slice::<Value>(&response.body).unwrap()["error"],
        "data_reset_in_progress"
    );
    let records = platform.adapter.records();
    assert_eq!(
        records[admin::reset::JOB_KEY].value["job_id"],
        first["job"]["job_id"]
    );
}

#[test]
fn data_reset_marker_survives_service_record_round_trip() {
    let platform = provisioned();
    platform.json("POST", DELETE_DATA, delete_data_body(&platform));
    let record = platform.adapter.records()["svc:fixtureService"].clone();
    let service: admin::Service = serde_json::from_value(record.value).unwrap();
    let marker = service.data_reset.clone().expect("marker");
    let again: admin::Service =
        serde_json::from_value(serde_json::to_value(&service).unwrap()).unwrap();
    assert_eq!(again.data_reset, Some(marker));
}

#[test]
fn delete_data_status_reports_phase_counts_and_last_50_messages() {
    let platform = provisioned();
    assert_eq!(
        platform.json("GET", DELETE_DATA, json!({}))["status"],
        "none"
    );
    let mut job = admin::reset::ResetJob {
        job_id: "job1".into(),
        status: "cleanup".into(),
        bytes: 42,
        pending: 7,
        error: Some("boom".into()),
        ..Default::default()
    };
    job.deleted.insert("request".into(), 3);
    for i in 0..60 {
        admin::reset::push_message(&mut job, 1_800_000_000 + i, format!("message {i}"));
    }
    assert!(platform
        .adapter
        .cas(
            admin::reset::JOB_KEY,
            None,
            &serde_json::to_value(&job).unwrap()
        )
        .unwrap());
    let status = platform.json("GET", DELETE_DATA, json!({}));
    assert_eq!(status["status"], "cleanup");
    assert_eq!(status["bytes"], 42);
    assert_eq!(status["pending"], 7);
    assert_eq!(status["error"], "boom");
    assert_eq!(status["deleted"]["request"], 3);
    let messages = status["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 50);
    assert_eq!(messages[0]["text"], "message 10");
    assert_eq!(messages[49]["text"], "message 59");
}

#[test]
fn delete_data_cancel_is_allowed_only_while_fencing() {
    let platform = provisioned();
    platform.json("POST", DELETE_DATA, delete_data_body(&platform));
    let cancel = format!("{DELETE_DATA}/cancel");
    let cancelled = platform.json("POST", &cancel, json!({}));
    assert_eq!(cancelled["job"]["status"], "cancelled");
    assert!(platform.service()["service"].get("data_reset").is_none());
    let response = platform.call("POST", &cancel, json!({}));
    assert_eq!(response.status, 409);
    platform.json("POST", DELETE_DATA, delete_data_body(&platform));
    assert_eq!(
        platform.json("GET", DELETE_DATA, json!({}))["status"],
        "fencing"
    );
}

#[test]
fn delete_data_cancel_retries_after_the_service_write_fails() {
    let platform = provisioned();
    let started = platform.json("POST", DELETE_DATA, delete_data_body(&platform));
    let cancel = format!("{DELETE_DATA}/cancel");
    platform
        .adapter
        .fail_next("KV_CAS", "svc:fixtureService", false);
    assert_eq!(platform.call("POST", &cancel, json!({})).status, 503);
    let records = platform.adapter.records();
    assert_eq!(records[admin::reset::JOB_KEY].value["status"], "cancelled");
    assert!(platform.service()["service"].get("data_reset").is_some());
    let retried = platform.json("POST", &cancel, json!({}));
    assert_eq!(retried["job"]["status"], "cancelled");
    assert!(platform.service()["service"].get("data_reset").is_none());
    let restarted = platform.json("POST", DELETE_DATA, delete_data_body(&platform));
    assert_ne!(restarted["job"]["job_id"], started["job"]["job_id"]);
    assert_eq!(restarted["job"]["status"], "fencing");
}

#[test]
fn sweep_backfill_and_quarantine_purge_refuse_while_fencing() {
    use fastly_log_analytics::{auth::Role, state::MemoryStore};
    let platform = provisioned();
    let store = MemoryStore::default();
    let started = platform.json("POST", DELETE_DATA, delete_data_body(&platform));
    assert_eq!(started["job"]["status"], "fencing");
    for (path, body) in [
        ("/api/admin/logs/sweep", json!({})),
        ("/api/admin/logs/backfill", json!({})),
        ("/api/admin/quarantine/purge", json!({"target": "all"})),
    ] {
        let response = shell_post(&platform, Role::Admin, &store, path, body);
        assert_eq!(
            response.status,
            409,
            "{path}: {}",
            String::from_utf8_lossy(&response.body)
        );
        let text = String::from_utf8_lossy(&response.body);
        assert!(text.contains("data_reset_in_progress"), "{path}: {text}");
    }
}

fn fence_tick(platform: &Platform) -> Value {
    if let Some(result) = fence_try(platform) {
        result.unwrap();
    }
    platform.adapter.records()[admin::reset::JOB_KEY]
        .value
        .clone()
}

fn fence_try(platform: &Platform) -> Option<Result<(), String>> {
    let service: admin::Service = serde_json::from_value(
        platform.adapter.records()["svc:fixtureService"]
            .value
            .clone(),
    )
    .unwrap();
    fence_try_as(platform, service)
}

/// [`fence_try`] on a tick that loaded `service`, which a POP's read cache can hold stale.
fn fence_try_as(platform: &Platform, service: admin::Service) -> Option<Result<(), String>> {
    use fastly_log_analytics::ingest::storage::Store;
    CALLS.with(|calls| calls.set(0));
    SECRETS.with(|secrets| secrets.borrow_mut().clear());
    ELAPSED_MS.with(|elapsed| elapsed.set(0));
    let (access_key, secret_key) = admin::fos_credentials(platform, &service).unwrap();
    let store = Store {
        runtime: platform,
        service: &service,
        access_key,
        secret_key,
        started: std::time::Instant::now(),
        vcpu_start: 0,
    };
    let result = admin::reset::advance(&store);
    assert!(CALLS.with(Cell::get) <= 32);
    result
}

fn fence_started() -> (Platform, admin::Service) {
    let platform = provisioned();
    platform.json("POST", DELETE_DATA, delete_data_body(&platform));
    let service = serde_json::from_value(
        platform.adapter.records()["svc:fixtureService"]
            .value
            .clone(),
    )
    .unwrap();
    (platform, service)
}

fn seed(platform: &Platform, key: &str, value: Value) {
    let old = platform
        .adapter
        .records()
        .get(key)
        .map(|r| r.generation.clone());
    assert!(platform.adapter.cas(key, old.as_deref(), &value).unwrap());
}

fn snapshot(platform: &Platform, key: &str) -> (String, Value) {
    let record = platform.adapter.records()[key].clone();
    (record.generation, record.value)
}

fn advance_clock(platform: &Platform, seconds: u64) {
    platform.clock.fetch_add(seconds, Ordering::SeqCst);
}

fn last_message(job: &Value) -> String {
    job["messages"].as_array().unwrap().last().unwrap()["text"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn messages(job: &Value) -> Vec<String> {
    job["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["text"].as_str().unwrap().to_owned())
        .collect()
}

#[test]
fn fence_waits_out_leases_before_resetting() {
    use admin::reset::FENCE_WAIT_S;
    let (platform, _) = fence_started();
    let now = platform.now();
    seed(
        &platform,
        "ingest:plan",
        json!({"version": 3, "plan_id": "p1", "lease_until": now + FENCE_WAIT_S + 30}),
    );
    advance_clock(&platform, 30);
    let job = fence_tick(&platform);
    assert_eq!(job["status"], "fencing");
    assert_eq!(
        last_message(&job),
        format!(
            "Waiting for in-flight ingest to finish ({}s left)…",
            FENCE_WAIT_S - 30
        )
    );
    advance_clock(&platform, FENCE_WAIT_S - 20);
    let job = fence_tick(&platform);
    assert_eq!(job["status"], "fencing");
    assert_eq!(
        last_message(&job),
        "Waiting for in-flight ingest to finish (20s left)…"
    );
    assert!(platform.adapter.records().contains_key("ingest:plan"));
    advance_clock(&platform, 20);
    let job = fence_tick(&platform);
    assert_eq!(job["status"], "resetting", "{job}");
    assert!(platform.service()["service"].get("data_reset").is_some());
}

#[test]
fn fence_wait_covers_the_svc_read_cache_and_a_plan_lease() {
    use fastly_log_analytics::{ingest::coordinator::LEASE_S, shell::KV_READ_CACHE_TTL_S};
    const { assert!(admin::reset::FENCE_WAIT_S >= KV_READ_CACHE_TTL_S + LEASE_S) };
    assert_eq!(admin::reset::FENCE_WAIT_S, 420);
    assert_eq!(admin::reset::FENCE_GIVE_UP_S, 780);
}

#[test]
fn fence_abandons_uncommitted_plans_and_their_inflight_outputs() {
    let (platform, service) = fence_started();
    let inflight = fastly_log_analytics::ingest::inflight_prefix(&service);
    seed(
        &platform,
        "ingest:watermark",
        json!({"run_id": "committed", "catalog": {"generation": 4}}),
    );
    seed(
        &platform,
        "ingest:plan",
        json!({"version": 3, "plan_id": "p1", "lease_until": 0}),
    );
    seed(
        &platform,
        "ingest:rum:plan",
        json!({"version": 3, "plan_id": "r1", "lease_until": 0}),
    );
    let bucket = "fos-fixture-logs";
    for key in ["p1/0.json", "p1/1a.json", "r1/0.json", "other/0.json"] {
        platform
            .adapter
            .seed_object(bucket, &format!("{inflight}{key}"));
    }
    let watermark = snapshot(&platform, "ingest:watermark");
    advance_clock(&platform, admin::reset::FENCE_WAIT_S);
    let job = fence_tick(&platform);
    assert_eq!(job["status"], "resetting", "{job}");
    let records = platform.adapter.records();
    assert!(!records.contains_key("ingest:plan"));
    assert!(!records.contains_key("ingest:rum:plan"));
    assert_eq!(snapshot(&platform, "ingest:watermark"), watermark);
    let left: Vec<String> = platform
        .adapter
        .object_keys(bucket)
        .into_iter()
        .filter(|key| key.starts_with(&inflight))
        .collect();
    assert_eq!(left, vec![format!("{inflight}other/0.json")]);
    let texts = messages(&job);
    assert!(texts.iter().any(|t| t.contains("p1")), "{texts:?}");
    assert!(texts.iter().any(|t| t.contains("r1")), "{texts:?}");
}

#[test]
fn fence_aborts_in_progress_compaction_upload_and_clears_preparing_records() {
    let (platform, _) = fence_started();
    seed(
        &platform,
        "ingest:compact_state",
        json!({
            "lease_until": 0,
            "upload": {"key": "processed/request/stale.parquet", "id": "u1"},
            "job": {"upload": {"key": "processed/request/open.parquet", "id": "u2"}},
            "queue": []
        }),
    );
    seed(
        &platform,
        "ingest:compact_state:rum:client_vitals",
        json!({
            "lease_until": 0,
            "job": {"upload": {"key": "processed/rum/client_vitals/open.parquet", "id": "u3"}},
            "queue": []
        }),
    );
    for key in [
        "ingest:catalog_preparing",
        "ingest:catalog_replacement_preparing",
        "ingest:source_index_preparing",
    ] {
        seed(&platform, key, json!({"identity": "x"}));
    }
    FOS_LOG.with(|log| log.borrow_mut().clear());
    advance_clock(&platform, admin::reset::FENCE_WAIT_S);
    let job = fence_tick(&platform);
    assert_eq!(job["status"], "resetting", "{job}");
    let log = FOS_LOG.with(|log| log.borrow().clone());
    for (key, id) in [
        ("request/stale", "u1"),
        ("request/open", "u2"),
        ("rum/client_vitals/open", "u3"),
    ] {
        let key = format!("processed/{key}.parquet");
        let query = format!("uploadId={id}");
        assert!(
            log.iter()
                .any(|(m, k, q)| m == "DELETE" && *k == key && *q == query),
            "{log:?}"
        );
    }
    let records = platform.adapter.records();
    for key in [
        "ingest:compact_state",
        "ingest:compact_state:rum:client_vitals",
        "ingest:catalog_preparing",
        "ingest:catalog_replacement_preparing",
        "ingest:source_index_preparing",
    ] {
        assert!(!records.contains_key(key), "{key}");
    }
    assert!(
        messages(&job).iter().any(|t| t.contains("compaction")),
        "{job}"
    );
}

#[test]
fn committed_plan_is_left_alone() {
    let (platform, service) = fence_started();
    let inflight = fastly_log_analytics::ingest::inflight_prefix(&service);
    seed(&platform, "ingest:watermark", json!({"run_id": "p1"}));
    seed(
        &platform,
        "ingest:plan",
        json!({"version": 3, "plan_id": "p1", "lease_until": 0}),
    );
    platform
        .adapter
        .seed_object("fos-fixture-logs", &format!("{inflight}p1/0.json"));
    let plan = snapshot(&platform, "ingest:plan");
    advance_clock(&platform, admin::reset::FENCE_WAIT_S);
    let job = fence_tick(&platform);
    assert_eq!(job["status"], "resetting", "{job}");
    assert_eq!(snapshot(&platform, "ingest:plan"), plan);
    assert!(platform
        .adapter
        .object_keys("fos-fixture-logs")
        .contains(&format!("{inflight}p1/0.json")));
}

#[test]
fn fence_gives_up_after_bound_and_resumes_ingest_unchanged() {
    let (platform, _) = fence_started();
    let now = platform.now();
    seed(
        &platform,
        "ingest:watermark",
        json!({"run_id": "committed", "catalog": {"generation": 4}}),
    );
    seed(
        &platform,
        "ingest:plan",
        json!({"version": 3, "plan_id": "p1", "lease_until": now + 100_000}),
    );
    let before: Vec<_> = ["ingest:watermark", "ingest:plan"]
        .map(|key| snapshot(&platform, key))
        .into();
    advance_clock(&platform, admin::reset::FENCE_WAIT_S);
    assert_eq!(fence_tick(&platform)["status"], "fencing");
    advance_clock(
        &platform,
        admin::reset::FENCE_GIVE_UP_S - admin::reset::FENCE_WAIT_S,
    );
    let job = fence_tick(&platform);
    assert_eq!(job["status"], "failed", "{job}");
    assert!(job["error"].is_string());
    assert!(platform.service()["service"].get("data_reset").is_none());
    let after: Vec<_> = ["ingest:watermark", "ingest:plan"]
        .map(|key| snapshot(&platform, key))
        .into();
    assert_eq!(after, before);
    assert!(last_message(&job).contains("ingest resumes"), "{job}");
    let service: admin::Service = serde_json::from_value(
        platform.adapter.records()["svc:fixtureService"]
            .value
            .clone(),
    )
    .unwrap();
    assert!(!admin::reset::fencing(&service));
    assert_eq!(fence_tick(&platform), job);
}

#[test]
fn fence_gives_up_after_bound_on_a_persistent_error() {
    let (platform, _) = fence_started();
    seed(
        &platform,
        "ingest:watermark",
        json!({"run_id": "committed", "catalog": {"generation": 4}}),
    );
    let watermark = snapshot(&platform, "ingest:watermark");
    FAIL_KV_DELETE.with(|fail| fail.set(true));
    advance_clock(&platform, admin::reset::FENCE_WAIT_S);
    let error = fence_try(&platform).unwrap().unwrap_err();
    assert_eq!(error, "kv_delete_failed");
    let job = platform.adapter.records()[admin::reset::JOB_KEY]
        .value
        .clone();
    assert_eq!(job["status"], "fencing", "{job}");
    assert_eq!(job["error"], "kv_delete_failed");
    assert!(platform.service()["service"].get("data_reset").is_some());
    advance_clock(
        &platform,
        admin::reset::FENCE_GIVE_UP_S - admin::reset::FENCE_WAIT_S,
    );
    fence_try(&platform).unwrap().unwrap();
    FAIL_KV_DELETE.with(|fail| fail.set(false));
    let job = platform.adapter.records()[admin::reset::JOB_KEY]
        .value
        .clone();
    assert_eq!(job["status"], "failed", "{job}");
    assert_eq!(job["error"], "fence_timeout: kv_delete_failed");
    assert!(last_message(&job).contains("ingest resumes"), "{job}");
    assert!(platform.service()["service"].get("data_reset").is_none());
    let service: admin::Service = serde_json::from_value(
        platform.adapter.records()["svc:fixtureService"]
            .value
            .clone(),
    )
    .unwrap();
    assert!(!admin::reset::fencing(&service));
    assert_eq!(snapshot(&platform, "ingest:watermark"), watermark);
}

/// A fencing job past its wait whose watermarks hold pre-reset state, with every record the
/// reset clears.
fn reset_seeded() -> (Platform, admin::Service) {
    let (platform, service) = fence_started();
    let raw = fastly_log_analytics::ingest::raw_prefix(&service);
    let old = |prefix: &str| format!("{prefix}year=2027/month=01/day=15/hour=07/minute=00/a.zst");
    seed(
        &platform,
        "ingest:watermark",
        json!({
            "run_id": "committed", "key": old(&raw), "at": 1,
            "passes": [[old(&raw), 1]], "swept": [[old(&raw), 1]],
            "sweep": {"after": old(&raw), "at": 1},
            "raw_sources": {"index": "x"}, "raw_scan": old(&raw), "raw_scan_due": true,
            "manifest_gen": 7, "custom": "kept",
            "catalog": {
                "version": 2, "generation": 4, "manifest": "processed/_catalog/old.json",
                "recent": [{"cursor": "fixtureService:4", "generation": 4,
                    "committed_at": null, "manifest": "processed/_catalog/old.json"}],
                "days": {"1800000000": "processed/_catalog/day.json"},
                "statistics": {"1800000000": {"earliest": 1, "latest": 2, "rows": 3}},
                "hour_bundles": {"1800000000": {"key": "processed/_catalog/h.json", "generation": 4}},
                "bundle_retired": {"processed/summary_bundle/x.parquet": 1},
                "retirement_grace_until": 0
            }
        }),
    );
    let rum = raw.replace("raw/request/", "raw/rum/");
    seed(
        &platform,
        "ingest:rum:watermark",
        json!({"run_id": "rum-committed", "key": old(&rum), "at": 1, "passes": [[old(&rum), 1]],
            "swept": [], "sweep": {"after": old(&rum), "at": 1}, "manifest_gen": 2,
            "rum_stats": {"latest_log": 1, "rows": 85491, "at": 1}}),
    );
    for key in [
        "ingest:plan",
        "ingest:rum:plan",
        "ingest:backfill",
        "ingest:deleteq:p1",
        "ingest:deleteq:p2",
        "ingest:quarantine:raw/a.zst",
        "summary:bundle_state",
        "summary:cmcd_bundle_state",
        "retention:request",
        "retention:rum",
        "retention:summary:request",
        "retention:summary_bundle:request",
    ] {
        seed(
            &platform,
            key,
            json!({"plan_id": "committed", "lease_until": 0}),
        );
    }
    for key in [
        "retention:last_sweep",
        "retention:purge",
        "history:x",
        "cron:state",
    ] {
        seed(&platform, key, json!({"kept": true}));
    }
    advance_clock(&platform, admin::reset::FENCE_WAIT_S);
    (platform, service)
}

/// [`reset_seeded`] after the fence step, so the next step resets.
fn reset_ready() -> (Platform, admin::Service) {
    let (platform, service) = reset_seeded();
    assert_eq!(fence_tick(&platform)["status"], "resetting");
    advance_clock(&platform, 2);
    (platform, service)
}

/// Steps the job until it leaves "resetting", as maintenance ticks would.
fn reset_tick(platform: &Platform) -> Value {
    let mut job = Value::Null;
    for _ in 0..4 {
        job = fence_tick(platform);
        if job["status"] != "resetting" {
            break;
        }
        advance_clock(platform, 2);
    }
    job
}

fn floor(service: &admin::Service, stream: &str, reset_at: u64) -> String {
    format!(
        "{}{}",
        fastly_log_analytics::ingest::raw_prefix(service)
            .replace("raw/request/", &format!("raw/{stream}/")),
        fastly_log_analytics::ingest::minute_dir(reset_at as i64)
    )
}

#[test]
fn reset_state_moves_cursor_and_floor_to_reset_minute() {
    let (platform, service) = reset_ready();
    let reset_at = service.data_reset.as_ref().unwrap().reset_at;
    let job = reset_tick(&platform);
    assert_eq!(job["status"], "cleanup", "{job}");
    let records = platform.adapter.records();
    let watermark = &records["ingest:watermark"].value;
    let request_floor = floor(&service, "request", reset_at);
    assert_eq!(watermark["key"], request_floor);
    assert_eq!(watermark["floor_key"], request_floor);
    assert_eq!(watermark["manifest_gen"], 8);
    assert_eq!(watermark["passes"], json!([]));
    assert_eq!(watermark["swept"], json!([]));
    assert_eq!(watermark["sweep"]["after"], request_floor);
    for field in ["raw_sources", "raw_scan", "raw_scan_due"] {
        assert!(watermark.get(field).is_none(), "{field}: {watermark}");
    }
    assert_eq!(watermark["run_id"], "committed");
    assert_eq!(watermark["custom"], "kept");
    let catalog = &watermark["catalog"];
    assert_eq!(catalog["generation"], 5);
    assert_eq!(catalog["recent"].as_array().unwrap().len(), 1);
    assert_eq!(catalog["days"], json!({}));
    assert_eq!(catalog["hour_bundles"], json!({}));
    assert_eq!(catalog["bundle_retired"], json!({}));
    let manifest = catalog["manifest"].as_str().unwrap();
    assert_eq!(catalog["recent"][0]["manifest"], manifest);
    assert_eq!(job["reset_manifest"], manifest);
    assert!(platform
        .adapter
        .object_keys("fos-fixture-logs")
        .contains(&manifest.to_owned()));
    assert_eq!(job["reset_cas_at"], watermark["at"]);
    let rum = &records["ingest:rum:watermark"].value;
    let rum_floor = floor(&service, "rum", reset_at);
    assert_eq!(rum["key"], rum_floor);
    assert_eq!(rum["floor_key"], rum_floor);
    assert_eq!(rum["manifest_gen"], 3);
    assert_eq!(rum["passes"], json!([]));
    assert_eq!(rum["sweep"]["after"], rum_floor);
    assert_eq!(rum["run_id"], "rum-committed");
    assert!(rum.get("rum_stats").is_none(), "{rum}");
    for key in [
        "ingest:plan",
        "ingest:rum:plan",
        "ingest:backfill",
        "ingest:deleteq:p1",
        "ingest:deleteq:p2",
        "ingest:quarantine:raw/a.zst",
        "summary:bundle_state",
        "summary:cmcd_bundle_state",
        "retention:request",
        "retention:rum",
        "retention:summary:request",
        "retention:summary_bundle:request",
    ] {
        assert!(!records.contains_key(key), "{key}");
    }
    for key in [
        "retention:last_sweep",
        "retention:purge",
        "history:x",
        "cron:state",
    ] {
        assert_eq!(records[key].value["kept"], true, "{key}");
    }
}

#[test]
fn reset_state_purges_edge_and_status_cache_then_resumes_ingest() {
    let (platform, _) = reset_ready();
    platform.adapter.clear_calls();
    CACHE_PURGES.with(|purges| purges.borrow_mut().clear());
    let job = reset_tick(&platform);
    assert_eq!(job["status"], "cleanup", "{job}");
    assert!(platform.adapter.calls().contains(&(
        "POST".into(),
        "/service/fixtureFront/purge/flp-fos-fixtureService".into()
    )));
    assert_eq!(
        CACHE_PURGES.with(|purges| purges.borrow().clone()),
        vec![admin::status_surrogate("fixtureService")]
    );
    let service: admin::Service = serde_json::from_value(
        platform.adapter.records()["svc:fixtureService"]
            .value
            .clone(),
    )
    .unwrap();
    let marker = service.data_reset.as_ref().unwrap();
    assert_eq!(marker.phase, admin::ResetPhase::Cleanup);
    assert!(!admin::reset::fencing(&service), "ingest dispatch resumes");
    let texts = messages(&job);
    for text in ["Edge cache purged", "Catalog reset; ingest resumed"] {
        assert!(texts.iter().any(|t| t == text), "{text}: {texts:?}");
    }
}

#[test]
fn reset_state_retries_the_cleanup_marker_without_repeating_the_watermark_cas() {
    let (platform, _) = reset_ready();
    platform
        .adapter
        .fail_next("KV_CAS", "svc:fixtureService", false);
    let error = fence_try(&platform).unwrap().unwrap_err();
    assert!(error.contains("injected_failure"), "{error}");
    let job = platform.adapter.records()[admin::reset::JOB_KEY]
        .value
        .clone();
    assert_eq!(job["status"], "cleanup", "{job}");
    assert!(job["reset_cas_at"].is_u64());
    assert_eq!(
        platform.service()["service"]["data_reset"]["phase"],
        "fencing"
    );
    let watermark = snapshot(&platform, "ingest:watermark");
    advance_clock(&platform, 2);
    let job = fence_tick(&platform);
    assert_eq!(snapshot(&platform, "ingest:watermark"), watermark);
    assert_eq!(
        platform.service()["service"]["data_reset"]["phase"],
        "cleanup"
    );
    assert!(messages(&job)
        .iter()
        .any(|t| t == "Catalog reset; ingest resumed"));
}

#[test]
fn reset_state_after_a_lost_job_save_does_not_repeat_the_watermark_cas() {
    let (platform, _) = reset_ready();
    platform
        .adapter
        .fail_next("KV_CAS", admin::reset::JOB_KEY, false);
    assert_eq!(
        fence_try(&platform).unwrap().unwrap_err(),
        "injected_failure"
    );
    let job = platform.adapter.records()[admin::reset::JOB_KEY]
        .value
        .clone();
    assert_eq!(job["status"], "resetting", "{job}");
    assert!(job["reset_cas_at"].is_null());
    let watermark = snapshot(&platform, "ingest:watermark");
    assert_eq!(watermark.1["catalog"]["generation"], 5);
    advance_clock(&platform, 2);
    let job = reset_tick(&platform);
    assert_eq!(job["status"], "cleanup", "{job}");
    assert_eq!(snapshot(&platform, "ingest:watermark"), watermark);
    // The watermark's `at` moves on with commits once ingest resumes: the fence's end bounds it.
    assert_eq!(job["reset_cas_at"], job["resetting_at"]);
    assert!(job_u64(&job, "reset_cas_at") <= watermark.1["at"].as_u64().unwrap());
    assert_eq!(
        job["retirement_grace_until"],
        watermark.1["at"].as_u64().unwrap()
            + fastly_log_analytics::ingest::compaction::DELETE_GRACE_S
    );
    assert_eq!(job["reset_manifest"], watermark.1["catalog"]["manifest"]);
}

#[test]
fn cancel_after_reset_cas_is_refused() {
    let (platform, _) = reset_ready();
    assert_eq!(reset_tick(&platform)["status"], "cleanup");
    let response = platform.call("POST", &format!("{DELETE_DATA}/cancel"), json!({}));
    assert_eq!(response.status, 409);
    assert_eq!(
        serde_json::from_slice::<Value>(&response.body).unwrap()["error"],
        "reset_committed"
    );
    assert!(platform.service()["service"]["data_reset"].is_object());
}

#[test]
fn cancel_while_fencing_clears_marker_and_changes_nothing() {
    use fastly_log_analytics::{
        auth::Role,
        state::{MemoryStore, StateStore},
    };
    let (platform, _) = reset_seeded();
    let before: Vec<_> = ["ingest:watermark", "ingest:rum:watermark", "ingest:plan"]
        .map(|key| snapshot(&platform, key))
        .into();
    let store = MemoryStore::default();
    let response = shell_post(
        &platform,
        Role::Admin,
        &store,
        &format!("{DELETE_DATA}/cancel"),
        json!({}),
    );
    assert_eq!(
        response.status,
        200,
        "{}",
        String::from_utf8_lossy(&response.body)
    );
    let cancelled: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(cancelled["job"]["status"], "cancelled");
    assert!(platform.service()["service"].get("data_reset").is_none());
    let after: Vec<_> = ["ingest:watermark", "ingest:rum:watermark", "ingest:plan"]
        .map(|key| snapshot(&platform, key))
        .into();
    assert_eq!(after, before);
    assert!(
        fence_try(&platform).is_none(),
        "no step runs without a marker"
    );
    let state = store.load().unwrap().state;
    assert!(state
        .recent_audit
        .iter()
        .chain(&state.audit)
        .any(|entry| entry.action == "delete_data_cancelled" && entry.target == "fixtureService"));
}

#[test]
fn a_persistent_purge_failure_after_the_reset_cas_enters_cleanup_at_the_bound() {
    let (platform, _) = reset_ready();
    for _ in 0..20 {
        platform.api_faults.lock().unwrap().push(ApiFault {
            method: "POST",
            path: "/service/fixtureFront/purge/flp-fos-fixtureService",
            name: None,
            status: 403,
            retry_after: None,
        });
    }
    let error = fence_try(&platform).unwrap().unwrap_err();
    assert_eq!(error, "edge_purge_failed: purge_status_403");
    let job = platform.adapter.records()[admin::reset::JOB_KEY]
        .value
        .clone();
    assert_eq!(job["status"], "resetting", "{job}");
    assert!(job["reset_cas_at"].is_u64());
    let watermark = snapshot(&platform, "ingest:watermark");
    advance_clock(&platform, admin::reset::RESET_GIVE_UP_S - 2);
    assert!(fence_try(&platform).unwrap().is_err());
    assert_eq!(
        platform.adapter.records()[admin::reset::JOB_KEY].value["status"],
        "resetting"
    );
    advance_clock(&platform, 2);
    let job = fence_tick(&platform);
    assert_eq!(job["status"], "cleanup", "{job}");
    assert_eq!(job["error"], "edge_purge_failed: purge_status_403");
    assert_eq!(job["purge_pending"], true);
    assert!(
        messages(&job)
            .iter()
            .any(|t| t
                == "Edge purge failed: purge_status_403; continuing, will retry during cleanup")
    );
    assert_eq!(snapshot(&platform, "ingest:watermark"), watermark);
    let service: admin::Service = serde_json::from_value(
        platform.adapter.records()["svc:fixtureService"]
            .value
            .clone(),
    )
    .unwrap();
    assert_eq!(
        service.data_reset.as_ref().unwrap().phase,
        admin::ResetPhase::Cleanup
    );
    assert!(!admin::reset::fencing(&service), "ingest dispatch resumes");
}

#[test]
fn a_corrupt_head_fails_the_reset_at_its_bound_and_resumes_ingest_unchanged() {
    let (platform, _) = reset_ready();
    let (_, mut corrupt) = snapshot(&platform, "ingest:watermark");
    corrupt["catalog"]["version"] = 99.into();
    seed(&platform, "ingest:watermark", corrupt);
    let watermark = snapshot(&platform, "ingest:watermark");
    let error = fence_try(&platform).unwrap().unwrap_err();
    assert_eq!(error, "catalog_version_unsupported");
    assert_eq!(
        platform.adapter.records()[admin::reset::JOB_KEY].value["status"],
        "resetting"
    );
    let cancel = platform.call("POST", &format!("{DELETE_DATA}/cancel"), json!({}));
    assert_eq!(
        cancel.status, 409,
        "no cancel while the reset may race its CAS"
    );
    advance_clock(&platform, admin::reset::RESET_GIVE_UP_S);
    let job = fence_tick(&platform);
    assert_eq!(job["status"], "failed", "{job}");
    assert_eq!(job["error"], "reset_timeout: catalog_version_unsupported");
    assert!(job["reset_cas_at"].is_null());
    assert!(last_message(&job).contains("ingest resumes"), "{job}");
    assert!(platform.service()["service"].get("data_reset").is_none());
    assert_eq!(snapshot(&platform, "ingest:watermark"), watermark);
}

#[test]
fn a_missing_purge_token_is_recorded_before_the_reset_cas() {
    let (platform, service) = reset_ready();
    platform
        .adapter
        .override_secret_reads(&service.fastly_token_ref, Ok(None));
    let job = reset_tick(&platform);
    platform.adapter.clear_secret_reads();
    assert_eq!(job["status"], "cleanup", "{job}");
    assert_eq!(job["error"], "fastly_token_missing");
    let texts = messages(&job);
    assert!(
        texts
            .iter()
            .any(|t| t == "Edge cache purge will be skipped: fastly_token_missing"),
        "{texts:?}"
    );
    assert_eq!(
        platform.service()["service"]["data_reset"]["phase"],
        "cleanup"
    );
}

const BUCKET: &str = "fos-fixture-logs";
const CLEANUP_PREFIXES: [&str; 10] = [
    "raw/request/",
    "raw/rum/",
    "processed/request/",
    "processed/rum/",
    "processed/summary/request/",
    "processed/summary_bundle/request/",
    "processed/_catalog/",
    "processed/_sources/",
    "processed/_inflight/",
    "processed/_compact_work/",
];

fn rich_fos() {
    RICH_FOS.with(|on| on.set(true));
    MTIMES.with(|m| m.borrow_mut().clear());
}

/// A job in cleanup right after its reset, with FOS objects listed by LastModified.
fn cleanup_ready() -> (Platform, admin::Service, Value) {
    rich_fos();
    let (platform, service) = reset_ready();
    let job = reset_tick(&platform);
    assert_eq!(job["status"], "cleanup", "{job}");
    (platform, service, job)
}

fn seed_at(platform: &Platform, key: &str, at: u64) {
    platform.adapter.seed_object(BUCKET, key);
    MTIMES.with(|m| m.borrow_mut().insert(key.into(), at));
}

fn bucket_keys(platform: &Platform) -> BTreeSet<String> {
    platform.adapter.object_keys(BUCKET).into_iter().collect()
}

fn raw_key(service: &admin::Service, stream: &str, minute: u64, name: &str) -> String {
    format!(
        "{}{}{name}.zst",
        fastly_log_analytics::ingest::raw_prefix(service)
            .replace("raw/request/", &format!("raw/{stream}/")),
        fastly_log_analytics::ingest::minute_dir(minute as i64)
    )
}

fn job_u64(job: &Value, field: &str) -> u64 {
    job[field]
        .as_u64()
        .unwrap_or_else(|| panic!("{field}: {job}"))
}

/// Steps cleanup a minute apart, as maintenance ticks would, until the job completes.
fn run_cleanup(platform: &Platform) -> Value {
    for _ in 0..60 {
        let job = fence_tick(platform);
        if job["status"] == "complete" {
            return job;
        }
        assert_eq!(job["status"], "cleanup", "{job}");
        advance_clock(platform, 60);
    }
    panic!("cleanup did not finish");
}

#[test]
fn cleanup_deletes_pre_reset_objects_and_keeps_new_ones() {
    let (platform, service, job) = cleanup_ready();
    let (cas_at, reset_at) = (job_u64(&job, "reset_cas_at"), job_u64(&job, "reset_at"));
    let scope = fastly_log_analytics::ingest::scope(&service);
    let (mut old, mut new) = (BTreeSet::new(), BTreeSet::new());
    for prefix in CLEANUP_PREFIXES
        .iter()
        .filter(|p| p.starts_with("processed/"))
    {
        let gone = format!("{scope}{prefix}x/old.parquet");
        seed_at(&platform, &gone, cas_at - 30);
        old.insert(gone);
        let kept = format!("{scope}{prefix}x/new.parquet");
        seed_at(&platform, &kept, cas_at + 30);
        new.insert(kept);
    }
    for stream in ["request", "rum"] {
        let gone = raw_key(&service, stream, reset_at - 120, "old");
        seed_at(&platform, &gone, cas_at + 30);
        old.insert(gone);
        let kept = raw_key(&service, stream, reset_at, "new");
        seed_at(&platform, &kept, cas_at - 30);
        new.insert(kept);
    }
    let job = run_cleanup(&platform);
    let left = bucket_keys(&platform);
    for key in &old {
        assert!(!left.contains(key), "{key} survived");
    }
    for key in &new {
        assert!(left.contains(key), "{key} was deleted");
    }
    let texts = messages(&job);
    for prefix in CLEANUP_PREFIXES {
        assert_eq!(job["deleted"][prefix], 1, "{prefix}: {job}");
        let text = format!("Deleted 1 objects under {prefix}");
        assert!(texts.contains(&text), "{text}: {texts:?}");
    }
    assert_eq!(job["bytes"], old.len());
    assert_eq!(job["pending"], 0);
}

#[test]
fn cleanup_keeps_object_modified_exactly_at_reset_cas_time() {
    let (platform, service, job) = cleanup_ready();
    let cas_at = job_u64(&job, "reset_cas_at");
    let scope = fastly_log_analytics::ingest::scope(&service);
    let equal = format!("{scope}processed/request/x/equal.parquet");
    let before = format!("{scope}processed/request/x/before.parquet");
    seed_at(&platform, &equal, cas_at);
    seed_at(&platform, &before, cas_at - 1);
    run_cleanup(&platform);
    let left = bucket_keys(&platform);
    assert!(left.contains(&equal));
    assert!(!left.contains(&before));
}

#[test]
fn cleanup_never_deletes_objects_the_new_head_references() {
    let (platform, service, job) = cleanup_ready();
    let cas_at = job_u64(&job, "reset_cas_at");
    let scope = fastly_log_analytics::ingest::scope(&service);
    let manifest = job["reset_manifest"].as_str().unwrap().to_owned();
    // Written before the CAS, in the reset's own execution, so its LastModified is earlier.
    MTIMES.with(|m| m.borrow_mut().insert(manifest.clone(), cas_at - 5));
    let day = format!("{scope}processed/_catalog/day-new.json");
    let bundle =
        format!("{scope}processed/summary_bundle/request/year=2027/hour_bundle=08.parquet");
    let rum_file = format!("{scope}processed/rum/x/rum-new.parquet");
    let (_, mut watermark) = snapshot(&platform, "ingest:watermark");
    watermark["catalog"]["days"] = json!({"1800000000": day});
    watermark["catalog"]["hour_bundles"] = json!({"1800000000": {"key": bundle, "generation": 6}});
    seed(&platform, "ingest:watermark", watermark);
    let (_, mut rum) = snapshot(&platform, "ingest:rum:watermark");
    rum["catalog"] = json!({"version": 2, "generation": 1, "manifest": rum_file,
        "recent": [{"cursor": "fixtureService:1", "generation": 1, "committed_at": null}]});
    seed(&platform, "ingest:rum:watermark", rum);
    // Clock skew between the CAS and FOS: referenced objects that look older still stay.
    for key in [&day, &bundle, &rum_file] {
        seed_at(&platform, key, cas_at - 5);
    }
    let fresh = format!("{scope}processed/request/x/new-ingest.parquet");
    seed_at(&platform, &fresh, cas_at + 200);
    let stale = format!("{scope}processed/_catalog/old-manifest.json");
    seed_at(&platform, &stale, cas_at - 5);
    run_cleanup(&platform);
    let left = bucket_keys(&platform);
    for key in [&manifest, &day, &bundle, &rum_file, &fresh] {
        assert!(left.contains(key), "{key} was deleted");
    }
    assert!(!left.contains(&stale));
}

#[test]
fn cleanup_never_lists_or_deletes_ops_logs() {
    let (platform, service, job) = cleanup_ready();
    let cas_at = job_u64(&job, "reset_cas_at");
    let ops = format!(
        "{}processed/ops/",
        fastly_log_analytics::ingest::scope(&service)
    );
    let log = format!("{ops}cron_runs/year=2027/month=01/day=15/hour=07/part.parquet");
    seed_at(&platform, &log, cas_at - 100);
    let parquet = format!(
        "{}processed/request/x/old.parquet",
        fastly_log_analytics::ingest::scope(&service)
    );
    seed_at(&platform, &parquet, cas_at - 100);
    FOS_LOG.with(|log| log.borrow_mut().clear());
    run_cleanup(&platform);
    let left = bucket_keys(&platform);
    assert!(left.contains(&log));
    assert!(!left.contains(&parquet));
    let requests = FOS_LOG.with(|log| log.borrow().clone());
    assert!(requests.iter().any(|(m, _, _)| m == "POST"), "{requests:?}");
    for (method, key, query) in requests {
        assert!(!key.starts_with(&ops), "{method} {key}");
        let prefix = url::form_urlencoded::parse(query.as_bytes())
            .find(|(k, _)| k == "prefix")
            .map(|(_, v)| v.into_owned());
        if let Some(prefix) = prefix {
            assert!(
                !prefix.starts_with(&ops) && !ops.starts_with(&prefix),
                "LIST {prefix} covers ops logs"
            );
        }
    }
}

#[test]
fn cleanup_waits_for_grace_and_raw_settle_and_rechecks_raw_once() {
    use fastly_log_analytics::ingest::compaction::DELETE_GRACE_S;
    rich_fos();
    let (platform, service) = reset_ready();
    let (_, mut svc) = snapshot(&platform, "svc:fixtureService");
    svc["log_period"] = 600.into();
    seed(&platform, "svc:fixtureService", svc);
    let settle = fastly_log_analytics::ingest::settle_s(600);
    let job = reset_tick(&platform);
    assert_eq!(job["status"], "cleanup", "{job}");
    let (cas_at, reset_at) = (job_u64(&job, "reset_cas_at"), job_u64(&job, "reset_at"));
    assert!(reset_at + settle > cas_at && reset_at + settle < cas_at + DELETE_GRACE_S);
    let scope = fastly_log_analytics::ingest::scope(&service);
    let processed = format!("{scope}processed/request/x/old.parquet");
    seed_at(&platform, &processed, cas_at - 30);
    let raw = raw_key(&service, "request", reset_at - 60, "old");
    seed_at(&platform, &raw, cas_at - 30);
    let at = |t: u64| platform.clock.store(t, Ordering::SeqCst);

    at(reset_at + settle - 1);
    fence_tick(&platform);
    let left = bucket_keys(&platform);
    assert!(left.contains(&raw), "raw waits for T + settle");
    assert!(left.contains(&processed));

    at(reset_at + settle);
    let job = fence_tick(&platform);
    let left = bucket_keys(&platform);
    assert!(!left.contains(&raw));
    assert!(left.contains(&processed), "processed waits for the grace");
    assert_eq!(job["raw_recheck_at"], reset_at + 2 * settle);
    let late = raw_key(&service, "request", reset_at - 60, "late");
    seed_at(&platform, &late, reset_at + settle + 1);

    at(cas_at + DELETE_GRACE_S - 1);
    fence_tick(&platform);
    assert!(bucket_keys(&platform).contains(&processed));

    at(cas_at + DELETE_GRACE_S);
    fence_tick(&platform);
    let left = bucket_keys(&platform);
    assert!(!left.contains(&processed));
    assert!(
        left.contains(&late),
        "the recheck waits another settle interval"
    );

    at(reset_at + 2 * settle);
    let job = fence_tick(&platform);
    assert!(!bucket_keys(&platform).contains(&late), "{job}");
    assert_eq!(job["deleted"]["raw/request/"], 2);
    let later = raw_key(&service, "request", reset_at - 60, "later");
    seed_at(&platform, &later, reset_at + 2 * settle + 1);
    let job = run_cleanup(&platform);
    assert!(
        bucket_keys(&platform).contains(&later),
        "exactly one recheck"
    );
    assert_eq!(job["deleted"]["raw/request/"], 2);
}

#[test]
fn finish_reconciles_storage_clears_marker_and_audits() {
    let (platform, service, job) = cleanup_ready();
    let cas_at = job_u64(&job, "reset_cas_at");
    let scope = fastly_log_analytics::ingest::scope(&service);
    seed_at(
        &platform,
        &format!("{scope}processed/request/x/old.parquet"),
        cas_at - 1,
    );
    seed(
        &platform,
        fastly_log_analytics::ingest::bundle::RUM_STATE_KEY,
        json!({"next_hour": 1}),
    );
    CACHE_PURGES.with(|purges| purges.borrow_mut().clear());
    platform.adapter.clear_calls();
    let job = run_cleanup(&platform);
    assert_eq!(job["status"], "complete");
    let records = platform.adapter.records();
    assert!(!records.contains_key(fastly_log_analytics::ingest::bundle::RUM_STATE_KEY));
    assert!(records.contains_key("storage:reconcile"));
    let svc = &records["svc:fixtureService"].value;
    assert!(svc.get("data_reset").is_none(), "{svc}");
    let audit = svc["_audit_outbox"].as_array().unwrap().last().unwrap();
    assert_eq!(audit["event"], "delete_data_completed");
    assert_eq!(audit["job_id"], job["job_id"]);
    assert_eq!(audit["deleted"]["processed/request/"], 1);
    assert_eq!(audit["bytes"], 1);
    assert!(
        last_message(&job).starts_with("Delete Data complete"),
        "{job}"
    );
    assert!(platform.adapter.calls().contains(&(
        "POST".into(),
        "/service/fixtureFront/purge/flp-fos-fixtureService".into()
    )));
    assert!(!CACHE_PURGES.with(|purges| purges.borrow().is_empty()));
    let service: admin::Service = serde_json::from_value(svc.clone()).unwrap();
    assert!(service.data_reset.is_none());
    assert!(fence_try(&platform).is_none(), "nothing runs once complete");
}

/// The reset's KV clears that cleanup finishes when the reset step may not have.
const LEFTOVER_KEYS: [&str; 5] = [
    "ingest:rum:plan",
    "ingest:backfill",
    "ingest:deleteq:p1",
    "ingest:deleteq:p2",
    "ingest:quarantine:raw/a.zst",
];

/// A reset whose watermark CAS landed but whose job save was lost.
fn reset_cas_with_lost_job_save() -> (Platform, admin::Service, Value) {
    rich_fos();
    let (platform, service) = reset_ready();
    platform
        .adapter
        .fail_next("KV_CAS", admin::reset::JOB_KEY, false);
    assert_eq!(
        fence_try(&platform).unwrap().unwrap_err(),
        "injected_failure"
    );
    let job = platform.adapter.records()[admin::reset::JOB_KEY]
        .value
        .clone();
    assert_eq!(job["status"], "resetting", "{job}");
    assert!(job["reset_cas_at"].is_null());
    let (_, watermark) = snapshot(&platform, "ingest:watermark");
    (platform, service, watermark)
}

#[test]
fn a_reset_whose_cas_landed_is_never_failed_at_its_bound_and_cleanup_finishes_it() {
    let (platform, service, watermark) = reset_cas_with_lost_job_save();
    let reset_at = service.data_reset.as_ref().unwrap().reset_at;
    // The lost save also lost the record of the reset steps after the CAS: put them back undone.
    seed(
        &platform,
        "ingest:rum:watermark",
        json!({"run_id": "rum-committed", "key": "raw/rum/old", "at": 1, "manifest_gen": 2}),
    );
    for key in LEFTOVER_KEYS.into_iter().chain(PRE_RESET_STATE) {
        seed(
            &platform,
            key,
            json!({"plan_id": "committed", "lease_until": 0}),
        );
    }
    platform.adapter.override_secret_reads(
        &service.fastly_token_ref,
        Err("secret_store_unavailable".into()),
    );
    advance_clock(&platform, admin::reset::RESET_GIVE_UP_S);
    let job = fence_tick(&platform);
    platform.adapter.clear_secret_reads();
    assert_eq!(job["status"], "cleanup", "{job}");
    assert_eq!(job["reset_cas_at"], job["resetting_at"]);
    assert_eq!(
        job["retirement_grace_until"],
        watermark["at"].as_u64().unwrap()
            + fastly_log_analytics::ingest::compaction::DELETE_GRACE_S
    );
    assert_eq!(job["reset_manifest"], watermark["catalog"]["manifest"]);
    assert_eq!(job["purge_pending"], true);
    assert_eq!(snapshot(&platform, "ingest:watermark").1, watermark);
    assert_eq!(
        platform.service()["service"]["data_reset"]["phase"],
        "cleanup"
    );
    let job = run_cleanup(&platform);
    assert_eq!(job["purge_pending"], false);
    assert!(messages(&job).iter().any(|t| t == "Edge cache purged"));
    let records = platform.adapter.records();
    assert_eq!(
        records["ingest:rum:watermark"].value["floor_key"],
        floor(&service, "rum", reset_at)
    );
    for key in LEFTOVER_KEYS {
        assert!(!records.contains_key(key), "{key}");
    }
    // Stage state is cleared before the CAS; what exists after it is new and stays.
    for key in PRE_RESET_STATE {
        assert!(records.contains_key(key), "{key}");
    }
    assert!(platform.service()["service"].get("data_reset").is_none());
}

#[test]
fn a_failed_job_whose_reset_cas_landed_resumes_cleanup_and_finishes() {
    let (platform, _, watermark) = reset_cas_with_lost_job_save();
    let (_, mut job) = snapshot(&platform, admin::reset::JOB_KEY);
    job["status"] = "failed".into();
    job["error"] = "reset_timeout".into();
    seed(&platform, admin::reset::JOB_KEY, job);
    advance_clock(&platform, 2);
    let job = fence_tick(&platform);
    assert_eq!(job["status"], "cleanup", "{job}");
    assert_eq!(job["reset_cas_at"], job["resetting_at"]);
    assert_eq!(
        job["retirement_grace_until"],
        watermark["at"].as_u64().unwrap()
            + fastly_log_analytics::ingest::compaction::DELETE_GRACE_S
    );
    // The lost save had already moved the marker to cleanup, so cleanup retried the purge.
    assert_eq!(job["purge_pending"], false);
    let texts = messages(&job);
    let adopted = texts
        .iter()
        .position(|t| t == "The catalog reset had landed; continuing with cleanup.")
        .expect("adopted");
    assert!(
        texts[adopted..].iter().any(|t| t == "Edge cache purged"),
        "{texts:?}"
    );
    assert_eq!(
        platform.service()["service"]["data_reset"]["phase"],
        "cleanup"
    );
    let job = run_cleanup(&platform);
    assert_eq!(job["status"], "complete");
    assert!(platform.service()["service"].get("data_reset").is_none());
    assert_eq!(snapshot(&platform, "ingest:watermark").1, watermark);
}

#[test]
fn a_post_cas_bound_holds_rum_ingest_until_its_cursor_reset_lands() {
    let (platform, _) = reset_ready();
    seed(
        &platform,
        "ingest:rum:plan",
        json!({"version": 3, "plan_id": "r-old", "lease_until": 0}),
    );
    // The RUM plan clear after the reset CAS keeps failing, so the RUM reset never lands.
    FAIL_KV_DELETE_KEY.with(|key| *key.borrow_mut() = Some("ingest:rum:plan".into()));
    assert_eq!(
        fence_try(&platform).unwrap().unwrap_err(),
        "kv_delete_failed"
    );
    let job = platform.adapter.records()[admin::reset::JOB_KEY]
        .value
        .clone();
    assert_eq!(job["status"], "resetting", "{job}");
    let cas_at = job_u64(&job, "reset_cas_at");
    advance_clock(&platform, admin::reset::RESET_GIVE_UP_S);
    let job = fence_tick(&platform);
    FAIL_KV_DELETE_KEY.with(|key| key.borrow_mut().take());
    assert_eq!(job["status"], "cleanup", "{job}");
    let load = || -> admin::Service {
        serde_json::from_value(
            platform.adapter.records()["svc:fixtureService"]
                .value
                .clone(),
        )
        .unwrap()
    };
    let service = load();
    assert!(!admin::reset::fencing(&service), "request ingest resumes");
    assert!(admin::reset::rum_held(&service), "RUM ingest stays held");
    assert!(platform.adapter.records().contains_key("ingest:rum:plan"));
    // A backfill a user starts once request ingest resumed is new work, not pre-reset state.
    seed(
        &platform,
        "ingest:backfill",
        json!({"status": "running", "actor": "a", "started_at": cas_at + 100, "updated_at": cas_at + 100}),
    );
    advance_clock(&platform, 2);
    let job = fence_tick(&platform);
    let service = load();
    assert!(!admin::reset::rum_held(&service), "{job}");
    assert!(service.data_reset.is_some());
    let records = platform.adapter.records();
    assert!(!records.contains_key("ingest:rum:plan"));
    assert!(!records.contains_key("ingest:compact_state"));
    assert!(records.contains_key("ingest:backfill"), "{job}");
    assert!(messages(&job).iter().any(|t| t == "RUM ingest resumed"));
}

#[test]
fn a_purge_that_keeps_failing_gives_up_and_is_recorded() {
    let (platform, _, _) = cleanup_ready();
    for _ in 0..3 {
        platform.api_faults.lock().unwrap().push(ApiFault {
            method: "POST",
            path: "/service/fixtureFront/purge/flp-fos-fixtureService",
            name: None,
            status: 503,
            retry_after: None,
        });
    }
    let job = run_cleanup(&platform);
    assert_eq!(
        job["error"],
        "edge_purge_failed: purge_status_503; gave up after 3 tries"
    );
    assert!(messages(&job)
        .iter()
        .any(|t| t == "Edge purge gave up after 3 tries; cached objects expire within 24h"));
}

/// The post-CAS bound resumes request ingest with RUM held, and that step's job save is lost.
fn held_resume_with_lost_job_save() -> (Platform, Value) {
    let (platform, _) = reset_ready();
    seed(
        &platform,
        "ingest:rum:plan",
        json!({"version": 3, "plan_id": "r-old", "lease_until": 0}),
    );
    FAIL_KV_DELETE_KEY.with(|key| *key.borrow_mut() = Some("ingest:rum:plan".into()));
    assert_eq!(
        fence_try(&platform).unwrap().unwrap_err(),
        "kv_delete_failed"
    );
    let job = snapshot(&platform, admin::reset::JOB_KEY).1;
    advance_clock(&platform, admin::reset::RESET_GIVE_UP_S);
    platform
        .adapter
        .fail_next("KV_CAS", admin::reset::JOB_KEY, false);
    assert_eq!(
        fence_try(&platform).unwrap().unwrap_err(),
        "injected_failure"
    );
    FAIL_KV_DELETE_KEY.with(|key| key.borrow_mut().take());
    let marker = &platform.service()["service"]["data_reset"];
    assert_eq!(marker["phase"], "cleanup");
    assert_eq!(marker["rum_held"], true);
    assert_eq!(
        snapshot(&platform, admin::reset::JOB_KEY).1["status"],
        "resetting"
    );
    (platform, job)
}

fn held(platform: &Platform) -> bool {
    let service: admin::Service = serde_json::from_value(
        platform.adapter.records()["svc:fixtureService"]
            .value
            .clone(),
    )
    .unwrap();
    admin::reset::rum_held(&service)
}

#[test]
fn a_lost_save_after_a_held_resume_still_lifts_the_rum_hold() {
    let (platform, _) = held_resume_with_lost_job_save();
    // The re-run lands the RUM reset, then finds the stored marker already in cleanup and held.
    for _ in 0..3 {
        advance_clock(&platform, 2);
        fence_tick(&platform);
    }
    assert!(!held(&platform), "RUM ingest resumes once its reset lands");
    assert!(!platform.adapter.records().contains_key("ingest:rum:plan"));
}

#[test]
fn a_reset_rerun_after_ingest_resumed_keeps_new_plans_and_backfills() {
    let (platform, job) = held_resume_with_lost_job_save();
    let cas_at = job_u64(&job, "reset_cas_at");
    // Started by resumed request ingest and by a user after the reset.
    let plan =
        json!({"version": 3, "plan_id": "p-new", "created_at": cas_at + 100, "lease_until": 0});
    seed(&platform, "ingest:plan", plan.clone());
    let backfill = json!({"status": "running", "actor": "a", "started_at": cas_at + 100,
        "updated_at": cas_at + 100});
    seed(&platform, "ingest:backfill", backfill.clone());
    for _ in 0..3 {
        advance_clock(&platform, 2);
        fence_tick(&platform);
    }
    let records = platform.adapter.records();
    assert_eq!(records["ingest:plan"].value, plan);
    assert_eq!(records["ingest:backfill"].value, backfill);
}

fn marker(platform: &Platform) -> Value {
    platform.adapter.records()["svc:fixtureService"].value["data_reset"].clone()
}

fn last_audit(platform: &Platform) -> Value {
    platform.adapter.records()["svc:fixtureService"].value["_audit_outbox"]
        .as_array()
        .unwrap()
        .last()
        .unwrap()
        .clone()
}

#[test]
fn a_start_whose_service_save_landed_but_failed_clears_its_stranded_marker() {
    let platform = provisioned();
    platform
        .adapter
        .fail_next("KV_CAS", "svc:fixtureService", true);
    let response = platform.call("POST", DELETE_DATA, delete_data_body(&platform));
    assert_eq!(response.status, 503);
    let job = snapshot(&platform, admin::reset::JOB_KEY).1;
    assert_eq!(job["status"], "cancelled", "{job}");
    assert_eq!(marker(&platform)["phase"], "fencing", "the save landed");
    assert_eq!(fence_try(&platform), Some(Ok(())));
    assert!(marker(&platform).is_null(), "ingest resumes");
    assert_eq!(last_audit(&platform)["event"], "data_reset.cancelled");
    assert!(fence_try(&platform).is_none());
    let restarted = platform.json("POST", DELETE_DATA, delete_data_body(&platform));
    assert_eq!(restarted["job"]["status"], "fencing");
}

#[test]
fn a_cancel_whose_service_save_failed_clears_its_stranded_marker_on_the_next_tick() {
    let platform = provisioned();
    platform.json("POST", DELETE_DATA, delete_data_body(&platform));
    platform
        .adapter
        .fail_next("KV_CAS", "svc:fixtureService", false);
    let cancel = format!("{DELETE_DATA}/cancel");
    assert_eq!(platform.call("POST", &cancel, json!({})).status, 503);
    assert_eq!(
        snapshot(&platform, admin::reset::JOB_KEY).1["status"],
        "cancelled"
    );
    assert_eq!(marker(&platform)["phase"], "fencing");
    assert_eq!(fence_try(&platform), Some(Ok(())));
    assert!(marker(&platform).is_null(), "ingest resumes");
    assert_eq!(last_audit(&platform)["event"], "data_reset.cancelled");
}

/// Steps the job as a POP whose read cache still holds `before`, the service record from
/// before this job's own last marker write.
fn stale_pop_tick(platform: &Platform, before: &Record) -> Option<Result<(), String>> {
    platform
        .adapter
        .cache_stale("svc:fixtureService", Some(before.clone()));
    fence_try_as(
        platform,
        serde_json::from_value(before.value.clone()).unwrap(),
    )
}

#[test]
fn a_stale_cached_fencing_marker_does_not_fail_the_step_after_the_move_to_cleanup() {
    let (platform, _) = reset_ready();
    let before = platform.adapter.records()["svc:fixtureService"].clone();
    assert_eq!(reset_tick(&platform)["status"], "cleanup");
    let moved = platform.adapter.records()["svc:fixtureService"].clone();
    assert_eq!(moved.value["data_reset"]["phase"], "cleanup");
    advance_clock(&platform, 2);
    assert_eq!(stale_pop_tick(&platform, &before), Some(Ok(())));
    let after = platform.adapter.records()["svc:fixtureService"].clone();
    assert_eq!(after.generation, moved.generation, "nothing to write again");
    let job = snapshot(&platform, admin::reset::JOB_KEY).1;
    assert_eq!(job["status"], "cleanup", "{job}");
    assert!(job["error"].is_null(), "{job}");
}

#[test]
fn a_stale_cached_cleanup_marker_does_not_fail_a_completed_jobs_clear() {
    let (platform, _, _) = cleanup_ready();
    let before = platform.adapter.records()["svc:fixtureService"].clone();
    assert_eq!(before.value["data_reset"]["phase"], "cleanup");
    assert_eq!(run_cleanup(&platform)["status"], "complete");
    let cleared = platform.adapter.records()["svc:fixtureService"].clone();
    assert!(cleared.value.get("data_reset").is_none());
    assert_eq!(stale_pop_tick(&platform, &before), Some(Ok(())));
    let after = platform.adapter.records()["svc:fixtureService"].clone();
    assert_eq!(
        after.generation, cleared.generation,
        "no second clear or audit"
    );
}

#[test]
fn a_non_terminal_job_no_marker_names_is_stale_after_the_fence_bound() {
    let platform = provisioned();
    // The job CAS landed, the service save failed and so did the rollback.
    let started_at = platform.now();
    seed(
        &platform,
        admin::reset::JOB_KEY,
        json!({"job_id": "lost", "status": "fencing", "reset_at": started_at - started_at % 60,
            "started_at": started_at}),
    );
    advance_clock(&platform, admin::reset::FENCE_GIVE_UP_S);
    let response = platform.call("POST", DELETE_DATA, delete_data_body(&platform));
    assert_eq!(response.status, 409, "still within the fence bound");
    advance_clock(&platform, 1);
    let started = platform.json("POST", DELETE_DATA, delete_data_body(&platform));
    assert_eq!(started["job"]["status"], "fencing");
    assert_ne!(started["job"]["job_id"], "lost");
}

#[test]
fn a_full_audit_outbox_does_not_block_the_marker_clear() {
    let (platform, _) = fence_started();
    let (_, mut svc) = snapshot(&platform, "svc:fixtureService");
    let entry = json!({"actor": "a", "at": 1, "event": "e", "config_version": 1});
    svc["_audit_outbox"] = Value::Array(vec![entry; 4096]);
    seed(&platform, "svc:fixtureService", svc);
    let held = platform.now() + 10 * admin::reset::FENCE_GIVE_UP_S;
    seed(
        &platform,
        "ingest:plan",
        json!({"version": 3, "plan_id": "p1", "lease_until": held}),
    );
    advance_clock(&platform, admin::reset::FENCE_GIVE_UP_S);
    let job = fence_tick(&platform);
    assert_eq!(job["status"], "failed", "{job}");
    let svc = snapshot(&platform, "svc:fixtureService").1;
    assert!(svc["data_reset"].is_null(), "ingest resumes");
    assert_eq!(svc["_audit_outbox"].as_array().unwrap().len(), 4096);
}

const PRE_RESET_STATE: [&str; 6] = [
    "ingest:compact_state",
    "summary:bundle_state",
    "retention:request",
    "retention:rum",
    "retention:summary:request",
    "retention:summary_bundle:request",
];

#[test]
fn a_persistent_rum_conflict_after_the_cas_never_keeps_pre_reset_bundle_state() {
    let (platform, _) = reset_ready();
    seed(
        &platform,
        "ingest:compact_state",
        json!({"plan_id": "committed", "lease_until": 0}),
    );
    CONFLICT_CAS.with(|k| *k.borrow_mut() = Some("ingest:rum:watermark".into()));
    let result = fence_try(&platform).unwrap();
    assert_eq!(result, Err("rum_watermark_conflict".into()));
    let job = snapshot(&platform, admin::reset::JOB_KEY).1;
    let cas_at = job_u64(&job, "reset_cas_at");
    let records = platform.adapter.records();
    for key in PRE_RESET_STATE {
        assert!(!records.contains_key(key), "{key} outlived the reset CAS");
    }
    advance_clock(&platform, admin::reset::RESET_GIVE_UP_S);
    fence_try(&platform).unwrap().unwrap();
    assert_eq!(
        snapshot(&platform, admin::reset::JOB_KEY).1["status"],
        "cleanup"
    );
    assert_eq!(marker(&platform)["rum_held"], true);
    // Written by the stages that resumed with request ingest.
    let new_bundle = json!({"built_after": cas_at + 100});
    seed(&platform, "summary:bundle_state", new_bundle.clone());
    let new_queue = json!({"run_id": "p-new", "keys": [], "at": cas_at + 100});
    seed(&platform, "ingest:deleteq:p-new", new_queue.clone());
    for _ in 0..3 {
        advance_clock(&platform, 60);
        assert_eq!(
            fence_try(&platform).unwrap(),
            Err("rum_watermark_conflict".into())
        );
    }
    CONFLICT_CAS.with(|k| k.borrow_mut().take());
    let records = platform.adapter.records();
    for key in [
        "ingest:deleteq:p1",
        "ingest:deleteq:p2",
        "ingest:quarantine:raw/a.zst",
    ] {
        assert!(!records.contains_key(key), "{key}: pre-reset queue kept");
    }
    assert_eq!(records["ingest:deleteq:p-new"].value, new_queue);
    assert_eq!(records["summary:bundle_state"].value, new_bundle);
    let head = &records["ingest:watermark"].value["catalog"];
    assert_eq!(head["hour_bundles"], json!({}));
    assert_eq!(head["days"], json!({}));
}

#[test]
fn the_first_cleanup_tick_removes_pre_reset_rum_files_without_the_catalog_grace() {
    let (platform, service, job) = cleanup_ready();
    let cas_at = job_u64(&job, "reset_cas_at");
    let scope = fastly_log_analytics::ingest::scope(&service);
    let old = format!("{scope}processed/rum/x/old.parquet");
    let new = format!("{scope}processed/rum/x/new.parquet");
    let request = format!("{scope}processed/request/x/old.parquet");
    seed_at(&platform, &old, cas_at - 30);
    seed_at(&platform, &new, cas_at + 1);
    seed_at(&platform, &request, cas_at - 30);
    advance_clock(&platform, 2);
    let job = fence_tick(&platform);
    let left = bucket_keys(&platform);
    // RUM queries LIST processed/rum/ directly; no catalog snapshot reader needs the grace.
    assert!(!left.contains(&old), "{job}");
    assert!(left.contains(&new));
    assert!(left.contains(&request), "catalog files wait for the grace");
    assert_eq!(job["deleted"]["processed/rum/"], 1);
}

#[test]
fn cleanup_finishes_past_more_kept_queue_records_than_one_step_can_read() {
    let (platform, service, job) = cleanup_ready();
    let (cas_at, reset_at) = (job_u64(&job, "reset_cas_at"), job_u64(&job, "reset_at"));
    // The post-CAS bound left the queues uncleared, and resumed ingest wrote new records.
    let (_, mut stored) = snapshot(&platform, admin::reset::JOB_KEY);
    let steps: Vec<Value> = stored["steps_done"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|step| !matches!(step.as_str(), Some("deleteq" | "quarantine")))
        .cloned()
        .collect();
    stored["steps_done"] = steps.into();
    seed(&platform, admin::reset::JOB_KEY, stored);
    let kept: Vec<String> = (0..40)
        .map(|i| format!("ingest:deleteq:new-{i:02}"))
        .chain((0..10).map(|i| format!("ingest:quarantine:new-{i:02}")))
        .collect();
    for key in &kept {
        seed(
            &platform,
            key,
            json!({"run_id": key, "keys": [], "at": cas_at + 100}),
        );
    }
    seed(
        &platform,
        "ingest:deleteq:old",
        json!({"run_id": "old", "keys": [], "at": cas_at - 100}),
    );
    let scope = fastly_log_analytics::ingest::scope(&service);
    let old = [
        format!("{scope}processed/rum/x/old.parquet"),
        format!("{scope}processed/request/x/old.parquet"),
        raw_key(&service, "request", reset_at - 120, "old"),
    ];
    for key in &old {
        seed_at(&platform, key, cas_at - 30);
    }
    let job = run_cleanup(&platform);
    assert_eq!(job["status"], "complete");
    let records = platform.adapter.records();
    for key in &kept {
        assert!(records.contains_key(key), "{key} was deleted");
    }
    assert!(!records.contains_key("ingest:deleteq:old"));
    let left = bucket_keys(&platform);
    for key in &old {
        assert!(!left.contains(key), "{key} survived");
    }
}
