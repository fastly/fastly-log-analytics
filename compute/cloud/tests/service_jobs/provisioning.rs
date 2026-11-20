use super::support::*;
use fastly_log_analytics::{
    admin::{
        self, fixture::FixtureAdapter, valid_bucket, valid_prefix, valid_service_id, FosRequest,
    },
    http::Request,
    shell::Runtime,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{atomic::Ordering, mpsc, Arc},
};

#[test]
fn provisioning_waits_for_the_persisted_service_generation() {
    let platform = Platform::new();
    platform.adapter.delay_kv_visibility(1);
    let result = platform.provision();
    let saved = platform.adapter.records();
    assert_eq!(result["generation"], saved["svc:fixtureService"].generation);
}

#[test]
fn job_claim_and_release_do_not_use_stale_post_write_records() {
    let platform = Platform::new();
    let result = platform.provision();
    platform.adapter.delay_kv_visibility(u32::MAX);
    let job = platform.json(
        "POST",
        &format!(
            "/api/admin/jobs/{}/step",
            result["job"]["id"].as_str().unwrap()
        ),
        json!({}),
    )["job"]
        .clone();
    assert_eq!(job["status"], "pending");
    let saved = platform.adapter.records();
    assert_eq!(
        saved["reconcile_lock:fixtureService"].value["expires_at"],
        0
    );
    assert_eq!(
        saved[&format!("job:{}", job["id"].as_str().unwrap())].value["step"],
        job["step"]
    );
}

#[test]
fn unpropagated_writes_fail_explicitly_without_guessing_a_generation() {
    let platform = Platform::new();
    platform.json("GET", "/api/admin/services", json!({}));
    platform.adapter.delay_confirmation_visibility(100);
    let context = admin::transport::Context::new(&platform);
    context.confirm_with_token("fixture-token");
    context.cas("probe", None, &json!({"state":"new"})).unwrap();
    let error = context.get("probe").err().expect("unconfirmed write");
    assert_eq!((error.status, error.code), (503, "state_propagating"));
    assert_eq!(platform.adapter.records()["probe"].value["state"], "new");
}

#[test]
fn post_write_confirmation_rejects_another_writers_record() {
    let platform = Platform::new();
    platform.json("GET", "/api/admin/services", json!({}));
    let context = admin::transport::Context::new(&platform);
    context.confirm_with_token("fixture-token");
    context
        .cas("probe", None, &json!({"state":"ours"}))
        .unwrap();
    let generation = platform.adapter.records()["probe"].generation.clone();
    assert!(platform
        .adapter
        .cas("probe", Some(&generation), &json!({"state":"theirs"}))
        .unwrap());
    let error = context.get("probe").err().expect("concurrent write");
    assert_eq!((error.status, error.code), (409, "generation_conflict"));
    assert_eq!(platform.adapter.records()["probe"].value["state"], "theirs");
}

#[test]
fn confirmation_distinguishes_an_older_identical_value_from_our_write() {
    let platform = Platform::new();
    platform.json("GET", "/api/admin/services", json!({}));
    let first = admin::transport::Context::new(&platform);
    first.cas("probe", None, &json!({"state":"ours"})).unwrap();
    let old = platform.adapter.records()["probe"].clone();
    let second = admin::transport::Context::new(&platform);
    second
        .cas("probe", Some(&old.generation), &json!({"state":"theirs"}))
        .unwrap();
    let previous = platform.adapter.records()["probe"].generation.clone();
    let context = admin::transport::Context::new(&platform);
    context.confirm_with_token("fixture-token");
    context
        .cas("probe", Some(&previous), &json!({"state":"ours"}))
        .unwrap();
    let actual = platform.adapter.records()["probe"].generation.clone();
    platform.adapter.stale_kv_read("probe", 1, Some(old));
    let observed = context.get("probe").unwrap().unwrap();
    assert_eq!(observed.generation, actual);
}

#[test]
fn service_identifiers_cannot_escape_api_paths() {
    assert!(valid_service_id("0123456789ABCDEFGHIJKL"));
    for value in ["", "../service", "a/b", "é", "a?token=x", "a%2fb"] {
        assert!(!valid_service_id(value), "{value}");
    }
}

#[test]
fn bucket_and_prefix_validation_rejects_ambiguous_names() {
    assert!(valid_bucket("fos-example-logs"));
    assert!(valid_prefix("customer/logs/"));
    for value in ["a", "Bucket", "a..b", "192.168.1.1", "../bucket"] {
        assert!(!valid_bucket(value), "{value}");
    }
    for value in ["/raw", "../raw", "a/../b", "x?secret", "a%2fb", "a\\b"] {
        assert!(!valid_prefix(value), "{value}");
    }
}

#[test]
fn provisioning_reconcile_and_teardown_are_durable_and_secret_free() {
    let platform = Platform::new();
    let result = platform.provision();
    let job = platform.drain(result["job"].clone());
    assert_eq!(job["status"], "complete");
    let service = platform.service();
    assert_eq!(service["service"]["status"], "active");
    let endpoints = platform.adapter.resources("fixtureService", "logging/fos");
    assert_eq!(endpoints.len(), 1);
    assert!(platform
        .adapter
        .resources("fixtureService", "logging/s3")
        .is_empty());
    assert_eq!(endpoints[0]["compression_codec"], "zstd");
    assert!(endpoints[0].get("gzip_level").is_none());
    assert_eq!(endpoints[0]["format_version"], "2");
    assert_eq!(endpoints[0]["message_type"], "blank");
    assert_eq!(
        endpoints[0]["path"],
        "raw/request/year=%Y/month=%m/day=%d/hour=%H/minute=%M/"
    );
    let records = serde_json::to_string(&platform.adapter.records()).unwrap();
    for secret in [
        "fixture-token",
        "fixture-ak-",
        "fixture-sk-",
        "fixture-cdn-key",
    ] {
        assert!(!records.contains(secret), "{secret} persisted");
    }
    let result=platform.json("POST","/api/admin/provision/teardown",json!({
            "service_id":"fixtureService","confirm_service_id":"fixtureService","token":"fixture-token",
            "generation":service["generation"],"delete_fos_bucket":true,"delete_data":true
        }));
    platform.drain(result["job"].clone());
    assert_eq!(platform.service()["service"]["status"], "torn_down");
    assert_eq!(platform.adapter.key_count(), 0);
    assert_eq!(
        platform.json("GET", "/api/services", json!({}))["services"],
        json!([])
    );
    platform.drain(platform.provision()["job"].clone());
    assert_eq!(platform.service()["service"]["status"], "active");
}

#[test]
fn preflight_rejects_bad_tokens_accounts_self_and_managed_resources_before_mutations() {
    for (token, sid, expected) in [
        ("bad-token", "fixtureService", 403),
        ("fixture-read-only", "fixtureService", 403),
        ("fixture-wrong-account", "fixtureService", 403),
        ("fixture-token", "fixtureApp", 400),
        ("fixture-token", "../escape", 400),
    ] {
        let platform = Platform::new();
        let response = platform.call(
            "POST",
            "/api/admin/provision/validate",
            json!({"token":token,"service_id":sid}),
        );
        assert_eq!(
            response.status,
            expected,
            "{}",
            String::from_utf8_lossy(&response.body)
        );
        assert!(platform
            .adapter
            .calls()
            .iter()
            .all(|(m, _)| m == "GET" || m == "KV_GET"));
    }
    let platform = Platform::new();
    platform.adapter.seed_resource(
        "fixtureService",
        "snippet",
        json!({"name":"Fastly Log Analytics - vcl_recv","content":"customer-owned","type":"recv"}),
    );
    assert_eq!(platform.call("POST","/api/admin/provision/execute",json!({"token":"fixture-token","service_id":"fixtureService","fos_bucket":"fos-test-logs","fos_region":"us-east-1"})).status,409);
    assert_eq!(platform.adapter.key_count(), 0);
}

#[test]
fn pending_install_blocks_a_second_service_and_stale_config_updates() {
    let platform = Platform::new();
    platform.provision();
    assert_eq!(platform.call("POST","/api/admin/provision/execute",json!({"token":"fixture-token","service_id":"fixtureOther","fos_bucket":"fos-other-logs","fos_region":"us-east-1"})).status,409);
    let response = platform.call(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation":"stale","log_period":1}),
    );
    assert_eq!(response.status, 409);
    assert_eq!(platform.service()["service"]["log_period"], 60);
}

#[test]
fn log_period_longer_than_the_object_window_cap_is_rejected() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    let service = platform.service();
    let response = platform.call(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation":service["generation"],"log_period":3600}),
    );
    assert_eq!(response.status, 400);
    let body: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(body["error"], "log_period_exceeds_window_capacity");
    assert_eq!(platform.service()["service"]["log_period"], 60);
    platform.json(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation":service["generation"],"log_period":300}),
    );
}

#[test]
fn rum_log_period_longer_than_the_object_window_cap_is_rejected() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    let service = platform.service();
    let rum = |log_period: u32| {
        json!({"enabled": true, "log_period": log_period, "custom_condition": "",
            "faro_version": "2.10.0", "endpoint_name": "Fastly RUM Logs",
            "capture_vitals": true, "capture_performance": true, "capture_errors": true,
            "capture_events": true})
    };
    let response = platform.call(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation":service["generation"],"rum":rum(3600)}),
    );
    assert_eq!(response.status, 400);
    let body: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(body["error"], "rum_log_period_exceeds_window_capacity");
    platform.json(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation":service["generation"],"rum":rum(300)}),
    );
}

#[test]
fn cloud_log_periods_below_one_minute_are_rejected() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    let service = platform.service();
    let response = platform.call(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation":service["generation"],"log_period":30}),
    );
    assert_eq!(response.status, 400);
    let body: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(body["error"], "log_period_below_minimum");
    let rum = json!({"enabled": true, "log_period": 10, "custom_condition": "",
        "faro_version": "2.10.0", "endpoint_name": "Fastly RUM Logs",
        "capture_vitals": true, "capture_performance": true, "capture_errors": true,
        "capture_events": true});
    let response = platform.call(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation":service["generation"],"rum":rum}),
    );
    assert_eq!(response.status, 400);
    let body: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(body["error"], "rum_log_period_below_minimum");
    let fresh = Platform::new();
    let response = fresh.call(
        "POST",
        "/api/admin/provision/execute",
        json!({"token":"fixture-token","service_id":"fixtureService","fos_bucket":"fos-test-logs",
            "fos_region":"us-east-1","log_period":10}),
    );
    assert_eq!(response.status, 400);
    let body: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(body["error"], "log_period_below_minimum");
}

#[test]
fn stored_sub_minute_periods_are_raised_to_one_minute_on_the_next_update() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    let mut record = platform.adapter.get("svc:fixtureService").unwrap().unwrap();
    record.value["log_period"] = 10.into();
    record.value["rum"]["log_period"] = 10.into();
    assert!(platform
        .adapter
        .cas(
            "svc:fixtureService",
            Some(&record.generation),
            &record.value
        )
        .unwrap());
    let service = platform.service();
    platform.json(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation":service["generation"],"sample_rate":50}),
    );
    let current = platform.service();
    assert_eq!(current["service"]["log_period"], 60);
    assert_eq!(current["service"]["rum"]["log_period"], 60);
    assert_eq!(current["service"]["sample_rate"], 50);
}

#[test]
fn dry_run_and_validate_only_never_activate_and_preserve_unmanaged_configuration() {
    let platform = Platform::new();
    platform.adapter.seed_resource("fixtureService","snippet",json!({"name":"customer-snippet","type":"recv","priority":20,"content":"set req.http.Customer = \"kept\";"}));
    platform.drain(platform.provision()["job"].clone());
    let service = platform.service();
    let result = platform.json(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation":service["generation"],"sample_rate":50}),
    );
    platform.adapter.clear_calls();
    let job = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation":result["generation"],"dry_run":true}),
    );
    platform.drain(job["job"].clone());
    assert!(!platform
        .adapter
        .calls()
        .iter()
        .any(|(m, p)| m == "PUT" && (p.ends_with("/clone") || p.ends_with("/activate"))));
    let service = platform.service();
    platform.adapter.clear_calls();
    let job = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation":service["generation"],"activate":false}),
    );
    platform.drain(job["job"].clone());
    assert!(!platform
        .adapter
        .calls()
        .iter()
        .any(|(_, p)| p.ends_with("/activate")));
    assert!(platform
        .adapter
        .resources("fixtureService", "snippet")
        .iter()
        .any(|v| v["name"] == "customer-snippet"
            && v["content"] == "set req.http.Customer = \"kept\";"));
}

#[test]
fn service_checkpoint_gap_resumes_without_rotating_credentials() {
    let platform = Platform::new();
    let job = platform.until(platform.provision()["job"].clone(), "bucket_probe");
    let response = platform.fail_checkpoint_before(job.clone(), "bucket_create");
    assert_eq!(response.status, 503);
    let resumed = platform.retry(&job);
    assert_ne!(resumed["status"], "failed", "{resumed}");
    platform.drain(resumed);
    assert_eq!(platform.adapter.key_count(), 1);
    let created = platform
        .adapter
        .calls()
        .iter()
        .filter(|(m, p)| m == "POST" && p == "/resources/object-storage/access-keys")
        .count();
    assert_eq!(
        created, 2,
        "temporary + permanent, without duplicate rotation"
    );
}

#[test]
fn partial_secret_pair_write_recovers_only_the_scoped_managed_key() {
    let platform = Platform::new();
    let job = platform.until(platform.provision()["job"].clone(), "key");
    platform.api_faults.lock().unwrap().push(ApiFault {
        method: "PUT",
        path: "/resources/stores/secret/fixtureSecrets/secrets",
        name: Some("fos_ak.fixtureService"),
        status: 400,
        retry_after: None,
    });
    let result = platform.json(
        "POST",
        &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
        json!({}),
    );
    assert_eq!(result["job"]["status"], "failed");
    let resumed = platform.retry(&job);
    platform.drain(resumed);
    assert_eq!(platform.adapter.key_count(), 1);
    let endpoint = &platform.adapter.resources("fixtureService", "logging/fos")[0];
    assert!(endpoint["access_key"]
        .as_str()
        .unwrap()
        .starts_with("fixture-fos-endpoint-key-"));
    assert!(endpoint.get("secret_key").is_none());
    let front = platform.adapter.dictionary_items("fixtureCredentials");
    assert!(
        front.values().any(|v| v.starts_with("fixture-sk-")),
        "{front:?}"
    );
}

#[test]
fn lost_activation_response_is_recovered_without_a_second_activation() {
    let platform = Platform::new();
    let job = platform.until(platform.provision()["job"].clone(), "activate");
    let path = format!(
        "/service/fixtureService/version/{}/activate",
        job["draft_version"].as_u64().unwrap()
    );
    platform.adapter.fail_next("PUT", &path, true);
    let result = platform.json(
        "POST",
        &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
        json!({}),
    );
    assert_eq!(result["job"]["status"], "failed");
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
}

fn reconcile_job_at_activate(platform: &Platform) -> Value {
    platform.drain(platform.provision()["job"].clone());
    let service = platform.service();
    let job = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation":service["generation"],"cmcd":{"enabled":true,"mode":"query_string","version":1}}),
    )["job"]
        .clone();
    platform.until(job, "activate")
}

#[test]
fn a_transient_state_read_failure_schedules_a_retry_instead_of_failing_the_job() {
    let platform = Platform::new();
    let job = reconcile_job_at_activate(&platform);
    *platform.failing_lease_reads.lock().unwrap() = 1;
    let step = format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap());
    let waiting = platform.json("POST", &step, json!({}))["job"].clone();
    assert_eq!(waiting["status"], "pending", "{waiting}");
    assert_eq!(waiting["retry_reason"], "state_unavailable");
    assert_eq!(platform.drain(waiting)["status"], "complete");
}

#[test]
fn a_state_read_that_keeps_failing_still_fails_the_job() {
    let platform = Platform::new();
    let job = reconcile_job_at_activate(&platform);
    *platform.failing_lease_reads.lock().unwrap() = u32::MAX;
    let step = format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap());
    let mut job = job;
    for _ in 0..50 {
        if job["status"] == "failed" {
            break;
        }
        if let Some(at) = job["retry_at"].as_u64() {
            platform.clock.fetch_max(at, Ordering::SeqCst);
        }
        job = platform.json("POST", &step, json!({}))["job"].clone();
    }
    assert_eq!(job["status"], "failed", "{job}");
    assert_eq!(job["error"], "state_unavailable");
}

#[test]
fn a_failed_reconcile_job_does_not_block_the_next_reconcile() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    let service = platform.service();
    let job = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation":service["generation"],"cmcd":{"enabled":true,"mode":"query_string","version":1}}),
    )["job"]
        .clone();
    let job = platform.until(job, "activate");
    let mut record = platform.adapter.get("svc:fixtureService").unwrap().unwrap();
    record.value["sample_rate"] = 7.into();
    assert!(platform
        .adapter
        .cas(
            "svc:fixtureService",
            Some(&record.generation),
            &record.value
        )
        .unwrap());
    let failed = platform.json(
        "POST",
        &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
        json!({}),
    );
    assert_eq!(failed["job"]["status"], "failed");
    assert_eq!(failed["job"]["error"], "service_changed_rerun");
    let service = platform.service();
    platform.json(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation":service["generation"],"sample_rate":50}),
    );
    let service = platform.service();
    assert_eq!(service["service"]["sample_rate"], 50);
    let next = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation":service["generation"]}),
    );
    assert_eq!(platform.drain(next["job"].clone())["status"], "complete");
}

#[test]
fn validation_failure_and_config_generation_change_leave_draft_inactive() {
    let platform = Platform::new();
    let job = platform.until(platform.provision()["job"].clone(), "validate");
    platform.adapter.invalid_draft(true);
    let result = platform.json(
        "POST",
        &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
        json!({}),
    );
    assert_eq!(result["job"]["error"], "draft_validation_failed");
    assert!(!platform
        .adapter
        .calls()
        .iter()
        .any(|(_, p)| p.starts_with("/service/fixtureService/") && p.ends_with("/activate")));
    platform.adapter.invalid_draft(false);
    let job = platform.retry(&job);
    assert_eq!(job["step"], "activate");
    let mut record = platform.adapter.get("svc:fixtureService").unwrap().unwrap();
    record.value["sample_rate"] = 7.into();
    assert!(platform
        .adapter
        .cas(
            "svc:fixtureService",
            Some(&record.generation),
            &record.value
        )
        .unwrap());
    let result = platform.json(
        "POST",
        &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
        json!({}),
    );
    assert_eq!(result["job"]["error"], "service_changed_rerun");
    assert!(!platform
        .adapter
        .calls()
        .iter()
        .any(|(_, p)| p.starts_with("/service/fixtureService/") && p.ends_with("/activate")));
}

#[test]
fn adopted_bucket_protects_unrelated_objects_and_whole_bucket_deletion() {
    let platform = Platform::new();
    platform
        .adapter
        .seed_object("fos-fixture-logs", "unrelated/customer-data");
    platform.drain(platform.provision()["job"].clone());
    let service = platform.service();
    assert_eq!(service["service"]["bucket_adopted"], true);
    let result=platform.call("POST","/api/admin/provision/teardown",json!({
        "service_id":"fixtureService","confirm_service_id":"fixtureService","generation":service["generation"],
        "token":"fixture-token","delete_fos_bucket":true,"delete_data":true
    }));
    assert_eq!(result.status, 409);
    assert_eq!(
        platform.adapter.object_keys("fos-fixture-logs"),
        ["unrelated/customer-data"]
    );
}

#[test]
fn concurrent_provision_reservations_cannot_install_two_services() {
    let platform = Arc::new(Platform::new());
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let mut workers = Vec::new();
    for (sid, bucket) in [
        ("fixtureService", "fos-fixture-logs"),
        ("fixtureOther", "fos-other-logs"),
    ] {
        let platform = platform.clone();
        let barrier = barrier.clone();
        workers.push(std::thread::spawn(move|| {
            barrier.wait();
            platform.call("POST","/api/admin/provision/execute",json!({"token":"fixture-token","service_id":sid,"fos_bucket":bucket,"fos_region":"us-east-1"})).status
        }));
    }
    let mut statuses: Vec<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();
    statuses.sort();
    assert_eq!(statuses, [200, 409]);
    assert_eq!(
        platform
            .adapter
            .records()
            .keys()
            .filter(|k| k.starts_with("svc:"))
            .count(),
        1
    );
}

#[test]
fn interrupted_job_creation_keeps_a_recoverable_single_install_reservation() {
    let platform = Platform::new();
    platform.adapter.fail_next("KV_CAS", "job:", false);
    let response=platform.call("POST","/api/admin/provision/execute",json!({"token":"fixture-token","service_id":"fixtureService","fos_bucket":"fos-fixture-logs","fos_region":"us-east-1"}));
    assert_eq!(response.status, 503);
    assert!(platform.adapter.records().contains_key("admin:install"));
    platform.drain(platform.provision()["job"].clone());
    assert_eq!(platform.service()["service"]["status"], "active");
}

#[test]
fn original_v3_execute_shape_is_supported_without_accepting_future_goal_settings() {
    let platform = Platform::new();
    let result=platform.json("POST","/api/provision/execute",json!({
        "token":"fixture-token","service_id":"fixtureService","service_name":"Spoofed name ignored",
        "fos_bucket_name":"fos-fixture-logs","fos_region":"us-east-1","endpoint_name":"Fastly Object Storage Logs",
        "log_period":"1 minute","sample_rate":"100","custom_condition":null,
        "log_fields":"{\"groups\":[\"A\"],\"custom_fields\":[],\"field_overrides\":{},\"field_limits\":{}}"
    }));
    assert_eq!(result["service"]["name"], "Fixture delivery service");
    platform.drain(result["job"].clone());
    assert_eq!(
        platform.adapter.resources("fixtureService", "logging/fos")[0]["name"],
        "Fastly Object Storage Logs"
    );
    assert_eq!(platform.call("POST","/api/provision/execute",json!({"token":"fixture-token","service_id":"fixtureOther","fos_bucket_name":"fos-other-logs","fos_region":"us-east-1","scoring_enabled":true})).status,400);
}

#[test]
fn shell_enforces_roles_front_auth_origin_and_json_for_all_provision_aliases() {
    use fastly_log_analytics::{
        auth::{self, webauthn::Credential, AuthState, Role, User},
        shell::Shell,
        state::{MemoryStore, StateStore},
    };
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;
    for role in [Role::Admin, Role::Analyst, Role::AnalystNoPii] {
        let platform = Platform::new();
        platform.drain(platform.provision()["job"].clone());
        let user = User {
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
        };
        let mut state = AuthState::default();
        state.users.insert(user.id.clone(), user.clone());
        let store = MemoryStore::default();
        assert!(store.compare_and_swap(None, &state).unwrap());
        let claims = auth::encode(
            json!({"sub":user.id,"role":role,"policy_version":0,"exp":platform.now()+3600})
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
        for path in ["/api/services", "/api/log-fields/catalog"] {
            let request = Request {
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
            CALLS.with(|calls| calls.set(0));
            SECRETS.with(|v| v.borrow_mut().clear());
            assert_eq!(
                Shell::new(&platform, &store, &[]).handle(&request).status,
                200,
                "{path} {role:?}"
            );
        }
        for path in ["/api/provision/validate", "/api/admin/provision/validate"] {
            let mut request = Request {
                method: "POST".into(),
                path: path.into(),
                headers: BTreeMap::from([
                    ("x-flp-front-key".into(), "fixture-front-key".into()),
                    ("fastly-client-ip".into(), "192.0.2.1".into()),
                    ("x-flp-front-rl".into(), "1".into()),
                    ("origin".into(), "https://logs.example.com".into()),
                    ("content-type".into(), "application/json".into()),
                    ("cookie".into(), cookie.clone()),
                ]),
                body: json!({"token":"fixture-token","service_id":"fixtureService"})
                    .to_string()
                    .into_bytes(),
            };
            CALLS.with(|calls| calls.set(0));
            SECRETS.with(|v| v.borrow_mut().clear());
            assert_eq!(
                Shell::new(&platform, &store, &[]).handle(&request).status,
                if role == Role::Admin { 200 } else { 403 }
            );
            request.headers.remove("origin");
            assert_eq!(
                Shell::new(&platform, &store, &[]).handle(&request).status,
                403
            );
            request.headers.remove("cookie");
            request
                .headers
                .insert("origin".into(), "https://logs.example.com".into());
            assert_eq!(
                Shell::new(&platform, &store, &[]).handle(&request).status,
                401
            );
            request.headers.remove("x-flp-front-key");
            assert_eq!(
                Shell::new(&platform, &store, &[]).handle(&request).status,
                403
            );
        }
        for (method, path) in [
            ("PATCH", "/api/admin/services/fixtureService"),
            ("POST", "/api/admin/services/fixtureService/reconcile"),
            ("POST", "/api/admin/log-fields/preview"),
        ] {
            let current = platform.service();
            let mut request = Request {
                method: method.into(),
                path: path.into(),
                headers: BTreeMap::from([
                    ("x-flp-front-key".into(), "fixture-front-key".into()),
                    ("fastly-client-ip".into(), "192.0.2.1".into()),
                    ("x-flp-front-rl".into(), "1".into()),
                    ("origin".into(), "https://logs.example.com".into()),
                    ("content-type".into(), "application/json".into()),
                    ("cookie".into(), cookie.clone()),
                ]),
                body: json!({"generation":current["generation"],
                    "log_fields":current["service"]["log_fields"],
                    "cmcd":{"enabled":true,"mode":"headers","version":2}
                })
                .to_string()
                .into_bytes(),
            };
            assert_eq!(
                Shell::new(&platform, &store, &[]).handle(&request).status,
                if role == Role::Admin { 200 } else { 403 },
                "{method} {path}"
            );
            request.headers.remove("origin");
            assert_eq!(
                Shell::new(&platform, &store, &[]).handle(&request).status,
                403
            );
        }
    }
}

#[test]
fn service_prefix_is_applied_and_teardown_deletes_only_that_prefix() {
    let platform = Platform::new();
    platform
        .adapter
        .seed_object("fos-fixture-logs", "tenant/own/raw/request/file&one.gz");
    platform
        .adapter
        .seed_object("fos-fixture-logs", "tenant/own/raw/request/file-two.gz");
    platform
        .adapter
        .seed_object("fos-fixture-logs", "tenant/other/keep.gz");
    let result=platform.json("POST","/api/provision/execute",json!({
                "token":"fixture-token","service_id":"fixtureService","fos_bucket_name":"fos-fixture-logs",
                "fos_region":"us-east-1","fos_prefix":"tenant/own/"
            }));
    platform.drain(result["job"].clone());
    assert_eq!(
        platform.adapter.resources("fixtureService", "logging/fos")[0]["path"],
        "tenant/own/raw/request/year=%Y/month=%m/day=%d/hour=%H/minute=%M/"
    );
    let service = platform.service();
    let job=platform.json("POST","/api/provision/teardown",json!({
                "service_id":"fixtureService","confirm_service_id":"fixtureService","token":"fixture-token",
                "generation":service["generation"],"delete_data":true,"delete_fos_bucket":false
            }));
    platform.drain(job["job"].clone());
    assert_eq!(
        platform.adapter.object_keys("fos-fixture-logs"),
        ["tenant/other/keep.gz"]
    );
}

#[test]
fn empty_reconcile_does_not_clone_and_ip_reset_precedes_capture() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    let snippets = platform.adapter.resources("fixtureService", "snippet");
    assert!(snippets
        .iter()
        .any(|v| v["name"] == "Fastly Log Analysis Reset Client IP" && v["priority"] == (-100)));
    let service = platform.service();
    platform.adapter.clear_calls();
    let job = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation":service["generation"]}),
    );
    let result = platform.drain(job["job"].clone());
    assert_eq!(result["changes"], json!([]));
    assert!(!platform
        .adapter
        .calls()
        .iter()
        .any(|(_, p)| p.ends_with("/clone")));
}

#[test]
fn reconcile_step_confirms_a_stale_cached_service_record_instead_of_failing() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    let service = platform.service();
    let before = platform.adapter.records()["svc:fixtureService"].clone();
    let job = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation":service["generation"],"cmcd":{"enabled":true,"mode":"query_string","version":1}}),
    );
    platform
        .adapter
        .cache_stale("svc:fixtureService", Some(before));
    let result = platform.drain(job["job"].clone());
    assert_eq!(result["status"], "complete");
}

#[test]
fn reconcile_forces_blank_message_type_on_the_request_endpoint() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    let mut endpoint = platform.adapter.resources("fixtureService", "logging/fos")[0].clone();
    endpoint["message_type"] = json!("classic");
    platform
        .adapter
        .seed_resource("fixtureService", "logging/fos", endpoint);
    let service = platform.service();
    let job = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation":service["generation"]}),
    );
    let result = platform.drain(job["job"].clone());
    assert!(result["changes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|c| c["resource"] == "logging/fos" && c["action"] == "upsert"));
    let endpoints = platform.adapter.resources("fixtureService", "logging/fos");
    assert_eq!(endpoints[0]["message_type"], "blank");
}

fn reconcile_changes(platform: &Platform) -> Vec<Value> {
    let service = platform.service();
    let job = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation":service["generation"]}),
    );
    platform.drain(job["job"].clone())["changes"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

#[test]
fn new_install_uses_the_native_fos_endpoint_and_sends_no_credentials() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    assert!(platform
        .adapter
        .resources("fixtureService", "logging/s3")
        .is_empty());
    let endpoints = platform.adapter.resources("fixtureService", "logging/fos");
    assert_eq!(endpoints.len(), 1);
    let endpoint = endpoints[0].as_object().unwrap();
    let mut keys: Vec<_> = endpoint.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        [
            "access_key",
            "bucket_name",
            "compression_codec",
            "format",
            "format_version",
            "message_type",
            "name",
            "path",
            "period",
            "region",
            "response_condition",
            "timestamp_format"
        ]
    );
    assert_eq!(endpoint["name"], "Fastly Object Storage Logs");
    assert_eq!(endpoint["bucket_name"], "fos-fixture-logs");
    assert_eq!(endpoint["region"], "us-east-1");
    assert_eq!(endpoint["response_condition"], "log_analytics_condition");
    assert!(endpoint["access_key"]
        .as_str()
        .unwrap()
        .starts_with("fixture-fos-endpoint-key-"));
    // Fastly echoes numbers as strings and adds its own fields; neither re-plans the endpoint.
    assert!(reconcile_changes(&platform).is_empty());
}

#[test]
fn legacy_s3_install_keeps_its_endpoint_and_never_gains_a_fos_duplicate() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    let ours = platform
        .adapter
        .remove_resource(
            "fixtureService",
            "logging/fos",
            "Fastly Object Storage Logs",
        )
        .unwrap();
    let legacy = json!({
        "name":ours["name"],"path":ours["path"],"period":60,"format":ours["format"],
        "format_version":2,"compression_codec":"zstd","message_type":"classic",
        "response_condition":ours["response_condition"],"placement":null,"response_object_name":"",
        "bucket_name":"fos-fixture-logs","domain":"us-east-1.object.fastlystorage.app",
        "access_key":"legacy-ak","secret_key":"legacy-sk"
    });
    platform
        .adapter
        .seed_resource("fixtureService", "logging/s3", legacy);
    let changes = reconcile_changes(&platform);
    assert!(changes
        .iter()
        .any(|c| c["resource"] == "logging/s3" && c["action"] == "upsert"));
    assert!(changes.iter().all(|c| c["resource"] != "logging/fos"));
    assert!(platform
        .adapter
        .resources("fixtureService", "logging/fos")
        .is_empty());
    let endpoints = platform.adapter.resources("fixtureService", "logging/s3");
    assert_eq!(endpoints.len(), 1);
    assert_eq!(endpoints[0]["message_type"], "blank");
    assert_eq!(endpoints[0]["domain"], "us-east-1.object.fastlystorage.app");
    assert!(endpoints[0]["access_key"]
        .as_str()
        .unwrap()
        .starts_with("fixture-ak-"));
    assert!(reconcile_changes(&platform).is_empty());

    let customer_s3 =
        json!({"name":"customer-s3","path":"customer/","bucket_name":"customer-bucket"});
    let customer_fos = json!({"name":"customer-fos","path":"customer/","bucket_name":"customer-bucket","region":"us-east-1"});
    platform
        .adapter
        .seed_resource("fixtureService", "logging/s3", customer_s3.clone());
    platform
        .adapter
        .seed_resource("fixtureService", "logging/fos", customer_fos.clone());
    let current = platform.service();
    let teardown=platform.json("POST","/api/provision/teardown",json!({"service_id":"fixtureService","confirm_service_id":"fixtureService","token":"fixture-token","generation":current["generation"]}));
    platform.drain(teardown["job"].clone());
    assert_eq!(platform.service()["service"]["status"], "torn_down");
    assert_eq!(
        platform.adapter.resources("fixtureService", "logging/s3"),
        vec![customer_s3]
    );
    assert_eq!(
        platform.adapter.resources("fixtureService", "logging/fos"),
        vec![customer_fos]
    );
}

#[test]
fn preflight_rejects_a_managed_fos_endpoint_from_another_install() {
    let platform = Platform::new();
    platform.adapter.seed_resource(
        "fixtureService",
        "logging/fos",
        json!({"name":"Fastly Object Storage Logs","bucket_name":"other-bucket","region":"us-east-1"}),
    );
    let response = platform.call("POST","/api/admin/provision/execute",json!({
        "token":"fixture-token","service_id":"fixtureService","fos_bucket":"fos-fixture-logs","fos_region":"us-east-1"
    }));
    assert_eq!(response.status, 409);
    assert!(String::from_utf8_lossy(&response.body).contains("service_managed_by_other_install"));
}

#[test]
fn expired_lease_can_be_reclaimed_but_live_lease_cannot_be_stolen() {
    let platform = Platform::new();
    let job = platform.provision()["job"].clone();
    platform
        .adapter
        .cas(
            "reconcile_lock:fixtureService",
            None,
            &json!({"job_id":"another-job","fence":41,"expires_at":platform.now()+30}),
        )
        .unwrap();
    assert_eq!(
        platform
            .call(
                "POST",
                &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
                json!({})
            )
            .status,
        409
    );
    platform.clock.fetch_add(31, Ordering::SeqCst);
    let result = platform.json(
        "POST",
        &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
        json!({}),
    );
    assert_eq!(result["job"]["status"], "pending");
    assert_eq!(
        platform
            .adapter
            .get("reconcile_lock:fixtureService")
            .unwrap()
            .unwrap()
            .value["fence"],
        42
    );
}

#[test]
fn committed_custom_field_name_and_semantic_type_cannot_change() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    let mut record = platform.adapter.get("svc:fixtureService").unwrap().unwrap();
    record.value["log_fields"]["custom_fields"] = json!([{"name":"locked_value","vcl":"req.http.X-Locked","parquet_type":"Utf8","committed":true,"enabled":true}]);
    platform
        .adapter
        .cas(
            "svc:fixtureService",
            Some(&record.generation),
            &record.value,
        )
        .unwrap();
    let current = platform.service();
    for fields in [
        json!([]),
        json!([{"name":"locked_value","vcl":"req.http.X-Locked","parquet_type":"Int64","committed":true,"enabled":true}]),
        json!([{"name":"renamed_value","vcl":"req.http.X-Locked","parquet_type":"Utf8","committed":true,"enabled":true}]),
    ] {
        let response=platform.call("PATCH","/api/admin/services/fixtureService",json!({"generation":current["generation"],"log_fields":{"groups":["A"],"custom_fields":fields,"field_overrides":{},"field_limits":{}}}));
        assert_eq!(response.status, 409);
    }
    let updated=platform.json("PATCH","/api/admin/services/fixtureService",json!({"generation":current["generation"],"log_fields":{"groups":["A"],"custom_fields":[{"name":"locked_value","vcl":"req.http.X-Locked","parquet_type":"VARCHAR","committed":true,"enabled":false}],"field_overrides":{},"field_limits":{}}}));
    assert_eq!(
        updated["service"]["log_fields"]["custom_fields"][0]["enabled"],
        false
    );
}

#[test]
fn custom_field_lifecycle_rejects_bad_types_and_allows_change_and_removal_before_commit() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    let initial = platform.service();
    let fields = json!({"groups":["A"],"custom_fields":[{
        "name":"tenant","label":"Tenant","vcl":"req.http.X-Tenant",
        "parquet_type":"Utf8","value_type":"string","bytes_estimate":40,"enabled":true
    }],"field_overrides":{},"field_limits":{}});
    let created = platform.json(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation":initial["generation"],"log_fields":fields}),
    );
    assert_eq!(
        created["service"]["log_fields"]["custom_fields"][0]["value_type"],
        "string"
    );

    let mut invalid = created["service"]["log_fields"].clone();
    invalid["custom_fields"][0]["value_type"] = json!("numeric");
    assert_eq!(
        platform
            .call(
                "PATCH",
                "/api/admin/services/fixtureService",
                json!({"generation":created["generation"],"log_fields":invalid})
            )
            .status,
        400
    );
    assert_eq!(
        platform.service()["service"]["log_fields"]["custom_fields"][0]["value_type"],
        "string"
    );

    let mut changed_fields = created["service"]["log_fields"].clone();
    changed_fields["custom_fields"][0]["label"] = json!("Account");
    changed_fields["custom_fields"][0]["parquet_type"] = json!("Int64");
    changed_fields["custom_fields"][0]["value_type"] = json!("numeric");
    let changed = platform.json(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation":created["generation"],"log_fields":changed_fields}),
    );
    assert_eq!(
        changed["service"]["log_fields"]["custom_fields"][0]["parquet_type"],
        "Int64"
    );

    let mut removed_fields = changed["service"]["log_fields"].clone();
    removed_fields["custom_fields"] = json!([]);
    let removed = platform.json(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation":changed["generation"],"log_fields":removed_fields}),
    );
    assert!(removed["service"]["log_fields"]["custom_fields"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[test]
fn s3_paths_use_sigv4_encoding_not_html_form_encoding() {
    assert_eq!(
        admin::fos_path("fos-test-logs", "raw/~a* b+é.gz"),
        "/fos-test-logs/raw/~a%2A%20b%2B%C3%A9.gz"
    );
    let headers = admin::fos_headers(
        &FosRequest {
            method: "HEAD",
            region: "us-east-1",
            bucket: "fos-test-logs",
            key: "",
            query: "",
            access_key: "synthetic-ak",
            secret_key: "synthetic-sk",
            body: vec![],
            timeout: std::time::Duration::from_secs(60),
            response_limit: admin::transport::FOS_RESPONSE_LIMIT,
            content_md5: None,
        },
        1_577_836_800,
    )
    .unwrap();
    assert!(headers
        .iter()
        .any(|(k, v)| k == "x-amz-date" && v == "20200101T000000Z"));
    assert!(headers.iter().any(|(k, v)| k == "x-amz-content-sha256"
        && v == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"));
    assert!(headers.iter().any(|(k, v)| k == "authorization"
        && v.starts_with(
            "AWS4-HMAC-SHA256 Credential=synthetic-ak/20200101/us-east-1/s3/aws4_request,"
        )));
}

#[test]
fn endpoint_rename_removes_old_owned_name_and_never_adopts_customer_endpoint() {
    for resource in ["logging/s3", "logging/fos"] {
        let platform = Platform::new();
        let start=platform.json("POST","/api/provision/execute",json!({"token":"fixture-token","service_id":"fixtureService","fos_bucket":"fos-fixture-logs","fos_region":"us-east-1","logging_endpoint_name":"our-old-logs"}));
        platform.drain(start["job"].clone());
        let current = platform.service();
        let saved = platform.json(
            "PATCH",
            "/api/admin/services/fixtureService",
            json!({"generation":current["generation"],"logging_endpoint_name":"our-new-logs"}),
        );
        let job = platform.json(
            "POST",
            "/api/admin/services/fixtureService/reconcile",
            json!({"generation":saved["generation"]}),
        );
        platform.drain(job["job"].clone());
        let endpoints = platform.adapter.resources("fixtureService", "logging/fos");
        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0]["name"], "our-new-logs");
        let customer = json!({"name":"customer-audit","path":"customer/private/","bucket_name":"customer-bucket"});
        platform
            .adapter
            .seed_resource("fixtureService", resource, customer.clone());
        let current = platform.service();
        let saved = platform.json(
            "PATCH",
            "/api/admin/services/fixtureService",
            json!({"generation":current["generation"],"logging_endpoint_name":"customer-audit"}),
        );
        let job = platform.json(
            "POST",
            "/api/admin/services/fixtureService/reconcile",
            json!({"generation":saved["generation"]}),
        );
        let failed = platform.json(
            "POST",
            &format!(
                "/api/admin/jobs/{}/step",
                job["job"]["id"].as_str().unwrap()
            ),
            json!({}),
        );
        assert_eq!(failed["job"]["status"], "failed");
        assert!(failed.to_string().contains("resource_name_collision"));
        let current = platform.service();
        let teardown=platform.json("POST","/api/provision/teardown",json!({"service_id":"fixtureService","confirm_service_id":"fixtureService","token":"fixture-token","generation":current["generation"]}));
        platform.drain(teardown["job"].clone());
        assert_eq!(
            platform.adapter.resources("fixtureService", resource),
            vec![customer]
        );
        let other = if resource == "logging/s3" {
            "logging/fos"
        } else {
            "logging/s3"
        };
        assert!(platform
            .adapter
            .resources("fixtureService", other)
            .is_empty());
    }
}

#[test]
fn delayed_stale_or_failed_secret_reads_never_rotate_a_checkpointed_key() {
    for mode in ["missing", "stale", "error", "metadata_error"] {
        let platform = Platform::new();
        let job = platform.until(platform.provision()["job"].clone(), "key");
        assert_eq!(
            platform
                .fail_checkpoint_before(job.clone(), "cleanup_temp")
                .status,
            503
        );
        let value = match mode {
            "stale" => Ok(Some(
                json!({"access_key":"previous-key","secret_key":"previous-secret"}).to_string(),
            )),
            "error" => Err("synthetic-secret-read-failure".into()),
            _ => Ok(None),
        };
        platform.adapter.override_secret_reads("fos_sk.", value);
        platform
            .adapter
            .override_secret_reads("fos_recovery.", Ok(None));
        if mode == "metadata_error" {
            platform.adapter.fail_next(
                "GET",
                "/resources/stores/secret/fixtureSecrets/secrets/fos_recovery.",
                false,
            );
        }
        let mut failed = platform.retry(&job);
        if matches!(mode, "missing" | "stale") {
            assert_eq!(failed["status"], "pending", "{mode}: {failed}");
            assert_eq!(failed["retry_reason"], "credentials_pending", "{mode}");
            failed = platform.exhaust(failed);
        }
        assert_eq!(failed["status"], "failed", "{mode}: {failed}");
        assert_eq!(platform.adapter.key_count(), 2, "{mode}");
        assert!(
            !platform
                .adapter
                .calls()
                .iter()
                .any(|(m, p)| m == "DELETE"
                    && p.starts_with("/resources/object-storage/access-keys/")),
            "{mode}"
        );
        platform.adapter.clear_secret_reads();
        platform.drain(platform.retry(&failed));
        assert_eq!(platform.adapter.key_count(), 1);
        assert_eq!(
            platform
                .adapter
                .calls()
                .iter()
                .filter(|(m, p)| m == "POST" && p == "/resources/object-storage/access-keys")
                .count(),
            2,
            "{mode}"
        );
    }
}

#[test]
fn lost_token_delete_ack_and_final_checkpoint_resume_without_service_credentials() {
    for checkpoint in [false, true] {
        let platform = Platform::new();
        platform.drain(platform.provision()["job"].clone());
        let service = platform.service();
        let result=platform.json("POST","/api/provision/teardown",json!({"service_id":"fixtureService","confirm_service_id":"fixtureService","token":"fixture-token","generation":service["generation"]}));
        let job = platform.until(result["job"].clone(), "finalize");
        if checkpoint {
            *platform.fail_checkpoint_step.lock().unwrap() = Some("complete");
        } else {
            platform.adapter.fail_next(
                "DELETE",
                "/resources/stores/secret/fixtureSecrets/secrets/fastly_token.fixtureService",
                true,
            );
        }
        let response = platform.call(
            "POST",
            &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
            json!({}),
        );
        assert_eq!(response.status, if checkpoint { 503 } else { 200 });
        assert_eq!(
            platform
                .adapter
                .secret("fastly_token.fixtureService")
                .unwrap(),
            None
        );
        platform.drain(platform.retry(&job));
        assert_eq!(platform.service()["service"]["status"], "torn_down");
    }
}

#[test]
fn shell_setup_grant_remains_usable_for_registration_and_system_checks() {
    use fastly_log_analytics::{auth, shell::Shell, state::MemoryStore};
    let platform = Platform::new();
    platform.adapter.override_secret_reads(
        "admin_setup_hash",
        Ok(Some(
            json!({"sha256":auth::hash("fixture-setup"),"expires_at":platform.now()+900})
                .to_string(),
        )),
    );
    let state = MemoryStore::default();
    let shell = Shell::new(&platform, &state, &[]);
    let mut request = Request {
        method: "POST".into(),
        path: "/api/setup/accept".into(),
        headers: BTreeMap::from([
            ("x-flp-front-key".into(), "fixture-front-key".into()),
            ("fastly-client-ip".into(), "192.0.2.11".into()),
            ("origin".into(), "https://logs.example.com".into()),
            ("content-type".into(), "application/json".into()),
            ("x-flp-front-rl".into(), "1".into()),
        ]),
        body: json!({"code":"fixture-setup"}).to_string().into_bytes(),
    };
    CALLS.with(|v| v.set(0));
    SECRETS.with(|v| v.borrow_mut().clear());
    let accepted = shell.handle(&request);
    assert_eq!(
        accepted.status,
        200,
        "{}",
        String::from_utf8_lossy(&accepted.body)
    );
    let cookie = accepted.headers["set-cookie"]
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    request.headers.insert("cookie".into(), cookie);
    request.path = "/api/auth/register/options".into();
    request.body = json!({"display_name":"Fixture admin"})
        .to_string()
        .into_bytes();
    CALLS.with(|v| v.set(0));
    SECRETS.with(|v| v.borrow_mut().clear());
    let options = shell.handle(&request);
    assert_eq!(
        options.status,
        200,
        "{}",
        String::from_utf8_lossy(&options.body)
    );
    assert!(
        serde_json::from_slice::<Value>(&options.body).unwrap()["public_key"]["challenge"]
            .is_string()
    );
    request.method = "GET".into();
    request.path = "/api/admin/system-checks".into();
    request.body.clear();
    CALLS.with(|v| v.set(0));
    SECRETS.with(|v| v.borrow_mut().clear());
    assert_eq!(shell.handle(&request).status, 200);
}

#[test]
fn shared_write_pacing_covers_claim_checkpoint_release_and_rapid_polls() {
    let adapter = FixtureAdapter::new();
    adapter.enforce_kv_write_rate();
    assert!(adapter.cas("rate-check", None, &json!({})).unwrap());
    let record = adapter.get("rate-check").unwrap().unwrap();
    adapter.advance_kv_clock(100);
    assert_eq!(
        adapter
            .cas("rate-check", Some(&record.generation), &json!({}))
            .unwrap_err(),
        "kv_write_rate_exceeded"
    );
    assert!(adapter
        .paced_cas("rate-check", Some(&record.generation), &json!({}))
        .unwrap());
    assert_eq!(adapter.kv_waits(), 1);
    assert_eq!(admin::kv_write_delay_ms(1100, 1100).unwrap(), 1100);
    assert_eq!(admin::kv_write_delay_ms(1100, 1099).unwrap(), 1100);
    let platform = Platform::new();
    platform.adapter.enforce_kv_write_rate();
    let mut job = platform.provision()["job"].clone();
    for _ in 0..150 {
        if job["status"] == "complete" {
            break;
        }
        platform.adapter.advance_kv_clock(100);
        if let Some(at) = job["retry_at"].as_u64() {
            platform.clock.fetch_max(at, Ordering::SeqCst);
        }
        job = platform.json(
            "POST",
            &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
            json!({}),
        )["job"]
            .clone();
        assert_ne!(job["status"], "failed", "{job}");
    }
    assert_eq!(job["status"], "complete");
    assert!(
        platform.adapter.kv_waits() > 10,
        "claim/checkpoint/release must be paced"
    );
    let service = platform.service();
    let job=platform.json("POST","/api/provision/teardown",json!({"service_id":"fixtureService","confirm_service_id":"fixtureService","token":"fixture-token","generation":service["generation"]}));
    platform.drain(job["job"].clone());
    assert_eq!(platform.service()["service"]["status"], "torn_down");
}

#[test]
fn omitted_provision_fields_match_catalog_all_groups_without_cookie_session() {
    let platform = Platform::new();
    let catalog = platform.json("GET", "/api/log-fields/catalog", json!({}));
    let insights = catalog["insights"]
        .as_array()
        .expect("catalog should include insights array");
    assert_eq!(insights.len(), 51);
    assert!(insights.iter().any(|insight| {
        insight["id"] == "error_spikes"
            && insight["title"] == "Error Spikes"
            && insight["required_groups"] == json!(["A"])
    }));
    assert!(insights.iter().any(|insight| {
        insight["id"] == "botnet_grouping"
            && insight["title"] == "Botnet Grouping"
            && insight["required_groups"] == json!(["H"])
    }));
    let result=platform.json("POST","/api/provision/execute",json!({"token":"fixture-token","service_id":"fixtureService","fos_bucket":"fos-fixture-logs","fos_region":"us-east-1"}));
    assert_eq!(result["service"]["log_fields"], catalog["default_config"]);
    assert_eq!(
        result["service"]["log_fields"]["groups"],
        json!(["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M"])
    );
    platform.drain(result["job"].clone());
    let endpoint = &platform.adapter.resources("fixtureService", "logging/fos")[0];
    assert!(!endpoint["format"]
        .as_str()
        .unwrap()
        .contains("\"cookie_session\""));
}

#[test]
fn teardown_does_not_claim_customer_endpoint_created_after_reservation() {
    for resource in ["logging/s3", "logging/fos"] {
        let platform = Platform::new();
        platform.provision();
        let customer = json!({"name":"Fastly Object Storage Logs","path":"customer/private/","bucket_name":"customer-bucket"});
        platform
            .adapter
            .seed_resource("fixtureService", resource, customer.clone());
        let service = platform.service();
        let teardown=platform.json("POST","/api/provision/teardown",json!({"service_id":"fixtureService","confirm_service_id":"fixtureService","token":"fixture-token","generation":service["generation"]}));
        let result = platform.json(
            "POST",
            &format!(
                "/api/admin/jobs/{}/step",
                teardown["job"]["id"].as_str().unwrap()
            ),
            json!({}),
        );
        assert_eq!(result["job"]["status"], "failed", "{resource}");
        assert_eq!(result["job"]["error"], "managed_resources_not_owned");
        assert_eq!(
            platform.adapter.resources("fixtureService", resource),
            vec![customer]
        );
    }
}

#[test]
fn superseded_teardown_cannot_delete_data_after_loading_service_before_lease() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    let object = "raw/request/preserve.gz";
    platform.adapter.seed_object("fos-fixture-logs", object);
    let service = platform.service();
    let teardown = platform.json(
        "POST",
        "/api/provision/teardown",
        json!({
            "service_id":"fixtureService","confirm_service_id":"fixtureService",
            "token":"fixture-token","generation":service["generation"],"delete_data":true
        }),
    );
    let deleting = platform.until(teardown["job"].clone(), "delete_page");
    let (loaded, ready) = mpsc::channel();
    let (resume, waiting) = mpsc::channel();
    *platform.before_lease.lock().unwrap() = Some(LeasePause {
        loaded,
        resume: waiting,
    });
    std::thread::scope(|scope| {
        let paused = scope.spawn(|| {
            platform.json(
                "POST",
                &format!("/api/admin/jobs/{}/step", deleting["id"].as_str().unwrap()),
                json!({}),
            )
        });
        ready
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let current = platform.service();
        let preserving = platform.json(
            "POST",
            "/api/provision/teardown",
            json!({
                "service_id":"fixtureService","confirm_service_id":"fixtureService",
                "token":"fixture-token","generation":current["generation"],"delete_data":false
            }),
        );
        resume.send(()).unwrap();
        let rejected = paused.join().unwrap();
        assert_eq!(
            platform.adapter.object_keys("fos-fixture-logs"),
            vec![object]
        );
        assert_eq!(rejected["job"]["status"], "failed");
        assert_eq!(rejected["job"]["error"], "service_changed_rerun");
        platform.drain(preserving["job"].clone());
    });
    assert_eq!(
        platform.adapter.object_keys("fos-fixture-logs"),
        vec![object]
    );
    assert_eq!(platform.service()["service"]["status"], "torn_down");
}

#[test]
fn sec20a_service_patch_rejects_poisoned_custom_and_disabled_rum_conditions() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    let generation = platform.service()["generation"].clone();

    for bad_cond in [
        ") || true || (",
        "req.http.x-fastly-key != \"\"",
        "header.get(req, \"Auth\" + \"orization\") != \"\"",
        "req.http.Cookie:flp_session != \"\"",
    ] {
        let bad_custom = platform.call(
            "PATCH",
            "/api/admin/services/fixtureService",
            json!({
                "generation": generation,
                "custom_condition": bad_cond
            }),
        );
        assert_eq!(bad_custom.status, 400, "PATCH must reject {bad_cond}");
    }

    for bad_rum in [
        ") || table.lookup(fos_credentials, \"secret_key\") != \"\" || (",
        "req.http.x-flp-front-key != \"\"",
        "REQ.URL.path ~ \"^/secret\"",
    ] {
        let bad_disabled_rum = platform.call(
            "PATCH",
            "/api/admin/services/fixtureService",
            json!({
                "generation": generation,
                "rum_enabled": false,
                "rum_custom_condition": bad_rum
            }),
        );
        assert_eq!(
            bad_disabled_rum.status, 400,
            "PATCH rum must reject {bad_rum}"
        );
        let body: Value = serde_json::from_slice(&bad_disabled_rum.body).unwrap();
        assert_eq!(body["error"], "invalid_rum_settings");

        let bad_reconcile = platform.call(
            "POST",
            "/api/admin/services/fixtureService/reconcile",
            json!({
                "generation": generation,
                "rum": {
                    "enabled": true,
                    "custom_condition": bad_rum
                }
            }),
        );
        assert_eq!(
            bad_reconcile.status, 400,
            "reconcile rum must reject {bad_rum}"
        );
    }

    let valid_patch = platform.call(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({
            "generation": generation,
            "custom_condition": "req.method == \"GET\"",
            "rum_enabled": false,
            "rum_custom_condition": "req.http.User-Agent !~ \"bot\""
        }),
    );
    assert_eq!(valid_patch.status, 200);
}

#[test]
fn cleanup_temp_secrets_waits_for_permanent_credentials_before_deleting_temp_secrets() {
    let platform = Platform::new();
    let job = platform.until(platform.provision()["job"].clone(), "cleanup_temp_secrets");
    platform.adapter.override_secret_reads("fos_sk.", Ok(None));
    let next = platform.retry(&job);
    assert_eq!(next["status"], "pending");
    assert_eq!(next["step"], "cleanup_temp_secrets");
    assert_eq!(next["retry_reason"], "credentials_pending");
    assert!(
        platform
            .adapter
            .secret("fos_tmp_ak.fixtureService")
            .unwrap()
            .is_some(),
        "must not delete temp secret before permanent credentials propagate"
    );
}
