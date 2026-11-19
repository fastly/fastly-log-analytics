use super::support::*;
use fastly_log_analytics::{
    admin::{self, ApiRequest, FosRequest},
    http::Request,
    shell::Runtime,
};
use serde_json::{json, Value};
use std::{cell::Cell, collections::BTreeMap, sync::atomic::Ordering};

#[test]
fn slow_control_plane_inventory_completes_inside_the_step_wall_budget() {
    for millis in [7500, 8500] {
        let platform = Platform::new();
        let job = platform.provision()["job"].clone();
        platform.api_delays.lock().unwrap().push(ApiDelay {
            method: "GET",
            path: "/resources/object-storage/access-keys",
            millis,
            after_effect: false,
        });
        let result = platform.json(
            "POST",
            &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
            json!({}),
        );
        assert_eq!(result["job"]["status"], "pending", "{result}");
        assert_eq!(platform.adapter.key_count(), 1, "{millis}ms: {result}");
        let granted = platform.api_timeouts.lock().unwrap()[0];
        assert!(granted > std::time::Duration::from_millis(millis));
        assert!(granted <= std::time::Duration::from_secs(30));
        assert_eq!(ELAPSED_MS.with(Cell::get), millis);
        platform.drain(result["job"].clone());
        assert_eq!(platform.service()["service"]["status"], "active");
    }
}

#[test]
fn slow_key_creation_keeps_checkpoint_reserve_and_recovers_unknown_side_effects() {
    for (millis, expected) in [
        (35000, "fastly_backend_deadline"),
        (28000, "wall_time_budget"),
    ] {
        let platform = Platform::new();
        let job = platform.provision()["job"].clone();
        platform.api_delays.lock().unwrap().extend([
            ApiDelay {
                method: "GET",
                path: "/resources/object-storage/access-keys",
                millis: 15000,
                after_effect: false,
            },
            ApiDelay {
                method: "POST",
                path: "/resources/object-storage/access-keys",
                millis,
                after_effect: true,
            },
        ]);
        let failed = platform.json(
            "POST",
            &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
            json!({}),
        )["job"]
            .clone();
        assert_eq!(failed["status"], "pending", "{failed}");
        assert_eq!(failed["retry_reason"], expected);
        assert_eq!(
            platform.adapter.key_count(),
            1,
            "a timeout cannot disprove key creation"
        );
        let persisted = platform.adapter.records()[&format!("job:{}", job["id"].as_str().unwrap())]
            .value
            .clone();
        assert_eq!(persisted["status"], "pending");
        assert_eq!(persisted["lease_until"], 0);
        assert_eq!(persisted["step"], "bucket");
        assert_eq!(ELAPSED_MS.with(Cell::get), 43000);
        assert_eq!(
            *platform.api_timeouts.lock().unwrap(),
            vec![
                std::time::Duration::from_millis(30000),
                std::time::Duration::from_millis(28000)
            ]
        );
        platform.drain(failed);
        assert_eq!(platform.service()["service"]["status"], "active");
        assert_eq!(platform.adapter.key_count(), 1);
        assert_eq!(
            platform
                .adapter
                .calls()
                .iter()
                .filter(|(method, path)| method == "POST"
                    && path == "/resources/object-storage/access-keys")
                .count(),
            3,
            "orphan temporary key is replaced before permanent scoped key creation"
        );
    }
}

#[test]
fn transient_inventory_http_error_retries_without_manual_resume() {
    let platform = Platform::new();
    let job = platform.provision()["job"].clone();
    platform.api_faults.lock().unwrap().push(ApiFault {
        method: "GET",
        path: "/resources/object-storage/access-keys",
        name: None,
        status: 503,
        retry_after: None,
    });
    let result = platform.json(
        "POST",
        &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
        json!({}),
    );
    assert_eq!(result["job"]["status"], "pending", "{result}");
    assert_eq!(result["job"]["step"], "bucket");
    assert_eq!(platform.adapter.key_count(), 1);
    platform.drain(result["job"].clone());
    assert_eq!(platform.service()["service"]["status"], "active");
}

#[test]
fn permanent_api_rejection_does_not_retry() {
    for status in [400, 401, 403, 422] {
        let platform = Platform::new();
        let job = platform.provision()["job"].clone();
        platform.api_faults.lock().unwrap().push(ApiFault {
            method: "GET",
            path: "/resources/object-storage/access-keys",
            name: None,
            status,
            retry_after: None,
        });
        let before = platform.api_attempts.load(Ordering::SeqCst);
        let result = platform.json(
            "POST",
            &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
            json!({}),
        );
        assert_eq!(result["job"]["status"], "failed");
        assert_eq!(result["job"]["error"], "fastly_api_rejected");
        assert_eq!(platform.api_attempts.load(Ordering::SeqCst) - before, 1);
        assert_eq!(platform.adapter.key_count(), 0);
    }
}

#[test]
fn retry_after_is_durable_and_early_poll_does_not_repeat_the_request() {
    let platform = Platform::new();
    let job = platform.provision()["job"].clone();
    platform.api_faults.lock().unwrap().push(ApiFault {
        method: "GET",
        path: "/resources/object-storage/access-keys",
        name: None,
        status: 429,
        retry_after: Some(std::time::Duration::from_secs(30)),
    });
    let path = format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap());
    let pending = platform.json("POST", &path, json!({}))["job"].clone();
    assert_eq!(pending["status"], "pending");
    assert_eq!(pending["retry_at"], platform.now() + 30);
    assert_eq!(pending["retry_attempt"], 1);
    assert_eq!(pending["retry_reason"], "fastly_api_rate_limited");
    let before = platform.api_attempts.load(Ordering::SeqCst);
    assert_eq!(platform.json("POST", &path, json!({}))["job"], pending);
    assert_eq!(platform.api_attempts.load(Ordering::SeqCst), before);
    platform.drain(pending);
    assert_eq!(platform.service()["service"]["status"], "active");
}

#[test]
fn automatic_retries_stop_after_four_attempts_across_step_requests() {
    let platform = Platform::new();
    let mut job = platform.provision()["job"].clone();
    platform
        .api_faults
        .lock()
        .unwrap()
        .extend((0..4).map(|_| ApiFault {
            method: "GET",
            path: "/resources/object-storage/access-keys",
            name: None,
            status: 503,
            retry_after: None,
        }));
    let before = platform.api_attempts.load(Ordering::SeqCst);
    let path = format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap());
    for _ in 0..5 {
        if job["status"] == "failed" {
            break;
        }
        if let Some(at) = job["retry_at"].as_u64() {
            platform.clock.fetch_max(at, Ordering::SeqCst);
        }
        job = platform.json("POST", &path, json!({}))["job"].clone();
    }
    assert_eq!(job["status"], "failed");
    assert_eq!(job["retry_attempt"], 4);
    assert_eq!(job["error"], "fastly_api_unavailable");
    assert_eq!(platform.api_attempts.load(Ordering::SeqCst) - before, 4);
    assert_eq!(platform.json("POST", &path, json!({}))["job"], job);
    assert_eq!(platform.adapter.key_count(), 0);
}

#[test]
fn ambiguous_secret_write_recovers_automatically_without_duplicate_keys() {
    let platform = Platform::new();
    let job = platform.provision()["job"].clone();
    platform.api_delays.lock().unwrap().push(ApiDelay {
        method: "PUT",
        path: "/resources/stores/secret/fixtureSecrets/secrets",
        millis: 31000,
        after_effect: true,
    });
    let result = platform.json(
        "POST",
        &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
        json!({}),
    );
    assert_eq!(result["job"]["status"], "pending");
    assert_eq!(result["job"]["retry_reason"], "fastly_backend_deadline");
    assert_eq!(platform.adapter.key_count(), 1);
    platform.drain(result["job"].clone());
    assert_eq!(platform.adapter.key_count(), 1);
    assert_eq!(
        platform
            .adapter
            .calls()
            .iter()
            .filter(|(method, path)| method == "POST"
                && path == "/resources/object-storage/access-keys")
            .count(),
        2
    );
    assert_eq!(platform.service()["service"]["status"], "active");
}

#[test]
fn ambiguous_activation_readback_finishes_without_reactivating() {
    let platform = Platform::new();
    let job = platform.until(platform.provision()["job"].clone(), "activate");
    let path = "/service/fixtureService/version/2/activate";
    platform.api_delays.lock().unwrap().push(ApiDelay {
        method: "PUT",
        path,
        millis: 31000,
        after_effect: true,
    });
    let result = platform.json(
        "POST",
        &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
        json!({}),
    );
    assert_eq!(result["job"]["status"], "pending");
    platform.drain(result["job"].clone());
    assert_eq!(
        platform
            .adapter
            .calls()
            .iter()
            .filter(|(method, value)| method == "PUT" && value == path)
            .count(),
        1
    );
    assert_eq!(platform.service()["service"]["status"], "active");
}

#[test]
fn inventory_network_timeout_resumes_automatically_in_the_next_request() {
    let platform = Platform::new();
    let job = platform.provision()["job"].clone();
    platform
        .api_delays
        .lock()
        .unwrap()
        .extend((0..2).map(|_| ApiDelay {
            method: "GET",
            path: "/resources/object-storage/access-keys",
            millis: 31000,
            after_effect: false,
        }));
    let result = platform.json(
        "POST",
        &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
        json!({}),
    );
    assert_eq!(result["job"]["status"], "pending");
    assert_eq!(result["job"]["retry_reason"], "fastly_backend_deadline");
    assert_eq!(result["job"]["retry_attempt"], 2);
    assert_eq!(ELAPSED_MS.with(Cell::get), 43000);
    assert_eq!(platform.adapter.key_count(), 0);
    platform.drain(result["job"].clone());
    assert_eq!(platform.service()["service"]["status"], "active");
}

#[test]
fn ambiguous_unmarked_clone_fails_closed_instead_of_cloning_again() {
    let platform = Platform::new();
    let job = platform.until(platform.provision()["job"].clone(), "clone");
    let step = format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap());
    platform.json("POST", &step, json!({}));
    let path = "/service/fixtureService/version/1/clone";
    platform.api_delays.lock().unwrap().push(ApiDelay {
        method: "PUT",
        path,
        millis: 31000,
        after_effect: true,
    });
    let pending = platform.json("POST", &step, json!({}))["job"].clone();
    assert_eq!(pending["status"], "pending");
    platform
        .clock
        .fetch_max(pending["retry_at"].as_u64().unwrap(), Ordering::SeqCst);
    let rejected = platform.json("POST", &step, json!({}))["job"].clone();
    assert_eq!(rejected["status"], "failed");
    assert_eq!(rejected["error"], "uncheckpointed_clone_requires_review");
    assert_eq!(
        platform
            .adapter
            .calls()
            .iter()
            .filter(|(method, value)| method == "PUT" && value == path)
            .count(),
        1
    );
}

#[test]
fn delayed_mutation_completion_cannot_be_replayed_even_after_manual_retry() {
    for clone in [false, true] {
        let platform = Platform::new();
        let job = platform.provision()["job"].clone();
        let job = if clone {
            platform.until(job, "clone")
        } else {
            job
        };
        let step = format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap());
        if clone {
            platform.json("POST", &step, json!({}));
        }
        let (method, path) = if clone {
            ("PUT", "/service/fixtureService/version/1/clone")
        } else {
            ("POST", "/resources/object-storage/access-keys")
        };
        platform.api_delays.lock().unwrap().push(ApiDelay {
            method,
            path,
            millis: 31000,
            after_effect: false,
        });
        let pending = platform.json("POST", &step, json!({}))["job"].clone();
        assert_eq!(pending["status"], "pending");
        platform
            .clock
            .fetch_max(pending["retry_at"].as_u64().unwrap(), Ordering::SeqCst);
        let rejected = platform.json("POST", &step, json!({}))["job"].clone();
        assert_eq!(rejected["status"], "failed", "{rejected}");
        assert_eq!(rejected["error"], "mutation_outcome_unknown");
        let rejected = platform.retry(&rejected);
        assert_eq!(rejected["error"], "mutation_outcome_unknown");
        assert_eq!(
            platform
                .adapter
                .calls()
                .iter()
                .filter(|(verb, value)| verb == method && value == path)
                .count(),
            0,
            "the original request has not completed yet"
        );
        let late = platform.late_requests.lock().unwrap().pop().unwrap();
        platform
            .adapter
            .api(ApiRequest {
                method: &late.method,
                path: &late.path,
                query: "",
                token: "fixture-token",
                body: late.body,
                timeout: std::time::Duration::from_secs(6),
            })
            .unwrap();
        let resumed = platform.retry(&rejected);
        if clone {
            assert_eq!(resumed["error"], "uncheckpointed_clone_requires_review");
            assert_eq!(
                platform
                    .adapter
                    .calls()
                    .iter()
                    .filter(|(verb, value)| verb == method && value == path)
                    .count(),
                1
            );
        } else {
            platform.drain(resumed);
            assert_eq!(platform.adapter.key_count(), 1);
            assert_eq!(platform.service()["service"]["status"], "active");
        }
    }
}

#[test]
fn response_condition_is_created_before_the_endpoint_that_references_it() {
    let platform = Platform::new();
    let job = platform.until(platform.provision()["job"].clone(), "apply");
    let position = |resource: &str| {
        job["changes"]
            .as_array()
            .unwrap()
            .iter()
            .position(|change| change["resource"] == resource)
            .unwrap()
    };
    assert!(position("condition") < position("logging/fos"));
}

#[test]
fn draft_upsert_readback_replaces_unrecorded_mutation_uncertainty() {
    let platform = Platform::new();
    let job = platform.until(platform.provision()["job"].clone(), "apply");
    let step = format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap());
    platform.api_delays.lock().unwrap().push(ApiDelay {
        method: "POST",
        path: "/service/fixtureService/version/2/snippet",
        millis: 31000,
        after_effect: false,
    });
    platform
        .fail_timeout_checkpoint
        .store(true, Ordering::SeqCst);
    assert_eq!(platform.call("POST", &step, json!({})).status, 503);
    platform.clock.fetch_add(61, Ordering::SeqCst);
    let resumed = platform.json("POST", &step, json!({}))["job"].clone();
    assert_ne!(resumed["error"], "mutation_outcome_unknown", "{resumed}");
    assert_eq!(
        platform
            .adapter
            .calls()
            .iter()
            .filter(|(verb, path)| verb == "POST"
                && path == "/service/fixtureService/version/2/snippet")
            .count(),
        1
    );
}

#[test]
fn unrecorded_delete_outcome_is_replayed_because_deletes_are_idempotent() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    let service = platform.service();
    let teardown = platform.json(
        "POST",
        "/api/provision/teardown",
        json!({
            "service_id":"fixtureService","confirm_service_id":"fixtureService",
            "token":"fixture-token","generation":service["generation"]
        }),
    );
    let job = platform.until(teardown["job"].clone(), "revoke_secret_ak");
    let step = format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap());
    let path = "/resources/stores/secret/fixtureSecrets/secrets/fos_ak.fixtureService";
    platform.api_delays.lock().unwrap().push(ApiDelay {
        method: "DELETE",
        path,
        millis: 31000,
        after_effect: false,
    });
    platform
        .fail_timeout_checkpoint
        .store(true, Ordering::SeqCst);
    assert_eq!(platform.call("POST", &step, json!({})).status, 503);
    platform.clock.fetch_add(61, Ordering::SeqCst);
    let resumed = platform.json("POST", &step, json!({}))["job"].clone();
    assert_ne!(resumed["error"], "mutation_outcome_unknown", "{resumed}");
    assert_eq!(platform.drain(resumed)["status"], "complete");
}

#[test]
fn provision_accepts_the_rum_capture_field_names_the_wizard_sends() {
    let platform = Platform::new();
    let response = platform.call(
        "POST",
        "/api/provision/execute",
        json!({
            "token":"fixture-token","service_id":"fixtureService","service_name":"Fixture",
            "fos_bucket_name":"fos-fixture-logs","fos_region":"us-east-1",
            "rum_enabled":false,"rum_capture_vitals":true,"rum_capture_performance":true,
            "rum_capture_errors":true,"rum_capture_events":true
        }),
    );
    assert_eq!(
        response.status,
        200,
        "{}",
        String::from_utf8_lossy(&response.body)
    );
}

#[test]
fn provision_persists_non_default_auto_delete_and_retention_settings() {
    let platform = Platform::new();
    platform.json(
        "POST",
        "/api/admin/provision/execute",
        json!({
            "token":"fixture-token","service_id":"fixtureService","fos_bucket":"fos-fixture-logs",
            "fos_region":"us-east-1","delete_after":false,"log_retention_days":7,
            "rum_retention_days":90
        }),
    );
    let service = platform.adapter.records()["svc:fixtureService"]
        .value
        .clone();
    assert_eq!(service["delete_after"], false, "{service}");
    assert_eq!(service["log_retention_days"], 7);
    assert_eq!(service["rum_retention_days"], 90);
}

#[test]
fn compaction_setting_defaults_off_and_patch_validates_it() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    let current = platform.service();
    assert_eq!(
        current["service"]["compaction"],
        json!({"mode": "off", "size_target_mb": 128})
    );
    for bad in [
        json!({"mode": "always", "size_target_mb": 64}),
        json!({"mode": "size_target", "size_target_mb": 0}),
        json!({"mode": "size_target", "size_target_mb": 257}),
    ] {
        let response = platform.call(
            "PATCH",
            "/api/admin/services/fixtureService",
            json!({"generation": current["generation"], "compaction": bad}),
        );
        assert_eq!(response.status, 400, "{bad}");
    }
    let updated = platform.json(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation": current["generation"],
               "compaction": {"mode": "size_target", "size_target_mb": 32}}),
    );
    assert_eq!(
        updated["service"]["compaction"],
        json!({"mode": "size_target", "size_target_mb": 32})
    );
}

#[test]
fn provision_persists_a_chosen_compaction_setting() {
    let platform = Platform::new();
    platform.json(
        "POST",
        "/api/admin/provision/execute",
        json!({
            "token":"fixture-token","service_id":"fixtureService","fos_bucket":"fos-fixture-logs",
            "fos_region":"us-east-1","compaction":{"mode":"size_target","size_target_mb":128}
        }),
    );
    let service = platform.adapter.records()["svc:fixtureService"]
        .value
        .clone();
    assert_eq!(
        service["compaction"],
        json!({"mode": "size_target", "size_target_mb": 128})
    );
}

#[test]
fn rejected_job_claim_releases_the_service_lease() {
    let platform = Platform::new();
    let job = platform.provision()["job"].clone();
    let step = format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap());
    platform.reject_job_claim.store(true, Ordering::SeqCst);
    let rejected = platform.call("POST", &step, json!({}));
    assert_eq!(rejected.status, 409);
    assert_eq!(
        platform.adapter.records()["reconcile_lock:fixtureService"].value["expires_at"],
        0
    );
    let retried = platform.call("POST", &step, json!({}));
    assert_eq!(retried.status, 200);
}

#[test]
fn mutation_attempt_limit_survives_intervening_readback_failures() {
    let platform = Platform::new();
    let mut job = platform.until(platform.provision()["job"].clone(), "apply");
    assert_eq!(job["changes"][0]["name"], "Fastly Log Analytics - vcl_recv");
    let step = format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap());
    for attempt in 1..=4 {
        platform.api_faults.lock().unwrap().push(ApiFault {
            method: "POST",
            path: "/service/fixtureService/version/2/snippet",
            name: None,
            status: 429,
            retry_after: None,
        });
        if let Some(at) = job["retry_at"].as_u64() {
            platform.clock.fetch_max(at, Ordering::SeqCst);
        }
        job = platform.json("POST", &step, json!({}))["job"].clone();
        assert_eq!(job["retry_attempt"], attempt, "{job}");
        if attempt == 4 {
            break;
        }
        platform.api_faults.lock().unwrap().push(ApiFault {
            method: "GET",
            path:
                "/service/fixtureService/version/2/snippet/Fastly%20Log%20Analytics%20-%20vcl_recv",
            name: None,
            status: 503,
            retry_after: Some(std::time::Duration::from_secs(30)),
        });
        platform
            .clock
            .fetch_max(job["retry_at"].as_u64().unwrap(), Ordering::SeqCst);
        job = platform.json("POST", &step, json!({}))["job"].clone();
        assert_eq!(job["retry_reason"], "fastly_api_unavailable", "{job}");
    }
    assert_eq!(job["status"], "failed", "{job}");
    assert_eq!(job["error"], "fastly_api_rate_limited");
}

#[test]
fn uncertain_canonical_secret_write_stops_without_exceeding_secret_budget() {
    let platform = Platform::new();
    let job = platform.until(platform.provision()["job"].clone(), "key");
    platform.api_faults.lock().unwrap().push(ApiFault {
        method: "PUT",
        path: "/resources/stores/secret/fixtureSecrets/secrets",
        name: Some("fos_ak.fixtureService"),
        status: 503,
        retry_after: None,
    });
    let step = format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap());
    let pending = platform.json("POST", &step, json!({}))["job"].clone();
    assert_eq!(pending["status"], "pending");
    platform
        .clock
        .fetch_max(pending["retry_at"].as_u64().unwrap(), Ordering::SeqCst);
    let rejected = platform.exhaust(platform.json("POST", &step, json!({}))["job"].clone());
    assert_eq!(rejected["status"], "failed", "{rejected}");
    assert_eq!(rejected["error"], "credentials_pending");
    assert_eq!(platform.adapter.key_count(), 2);
    assert_eq!(
        platform.adapter.records()[&format!("job:{}", job["id"].as_str().unwrap())].value
            ["uncertain_operations"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn delayed_secret_visibility_retries_on_schedule_and_resumes_without_rotating_keys() {
    let platform = Platform::new();
    let job = platform.until(platform.provision()["job"].clone(), "bucket_probe");
    platform.adapter.override_secret_reads("fos_tmp_", Ok(None));
    let step = format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap());
    let first = platform.json("POST", &step, json!({}))["job"].clone();
    assert_eq!(first["status"], "pending", "{first}");
    assert_eq!(first["step"], "bucket_probe");
    assert_eq!(first["retry_attempt"], 1);
    assert_eq!(first["retry_reason"], "credentials_pending");
    assert_eq!(first["retry_at"], platform.now() + 10);

    let before = platform.adapter.calls().len();
    let waiting = platform.json("POST", &step, json!({}))["job"].clone();
    assert_eq!(waiting["retry_attempt"], 1);
    assert_eq!(
        platform.adapter.calls()[before..],
        [(
            "KV_GET".to_string(),
            format!("job:{}", job["id"].as_str().unwrap())
        )]
    );

    platform
        .clock
        .fetch_max(first["retry_at"].as_u64().unwrap(), Ordering::SeqCst);
    let second = platform.json("POST", &step, json!({}))["job"].clone();
    assert_eq!(second["retry_attempt"], 2, "{second}");

    platform.adapter.clear_secret_reads();
    platform.drain(second);
    assert_eq!(platform.service()["service"]["status"], "active");
    assert_eq!(platform.adapter.key_count(), 1);
    assert_eq!(
        platform
            .adapter
            .calls()
            .iter()
            .filter(|(m, p)| m == "POST" && p == "/resources/object-storage/access-keys")
            .count(),
        2
    );
}

#[test]
fn secret_propagation_retries_stop_at_the_budget_and_a_manual_retry_resets_it() {
    let platform = Platform::new();
    let job = platform.until(platform.provision()["job"].clone(), "bucket_probe");
    platform.adapter.override_secret_reads("fos_tmp_", Ok(None));
    let step = format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap());
    let first = platform.json("POST", &step, json!({}))["job"].clone();
    let failed = platform.exhaust(first);
    assert_eq!(failed["status"], "failed", "{failed}");
    assert_eq!(failed["error"], "credentials_pending");
    assert_eq!(failed["retry_attempt"], 24);
    assert!(failed["retry_at"].is_null(), "{failed}");

    platform.adapter.clear_secret_reads();
    let resumed = platform.retry(&failed);
    assert_eq!(resumed["retry_attempt"], 0, "{resumed}");
    platform.drain(resumed);
    assert_eq!(platform.service()["service"]["status"], "active");
}

#[test]
fn mutation_dispatch_intent_survives_a_failed_timeout_checkpoint() {
    for clone in [false, true] {
        let platform = Platform::new();
        let job = platform.provision()["job"].clone();
        let job = if clone {
            platform.until(job, "clone")
        } else {
            job
        };
        let step = format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap());
        if clone {
            platform.json("POST", &step, json!({}));
        }

        let (method, path) = if clone {
            ("PUT", "/service/fixtureService/version/1/clone")
        } else {
            ("POST", "/resources/object-storage/access-keys")
        };
        platform.api_delays.lock().unwrap().push(ApiDelay {
            method,
            path,
            millis: 31000,
            after_effect: false,
        });
        platform
            .fail_timeout_checkpoint
            .store(true, Ordering::SeqCst);
        let response = platform.call("POST", &step, json!({}));
        assert_eq!(response.status, 503);
        assert_eq!(
            serde_json::from_slice::<Value>(&response.body).unwrap()["error"],
            "state_write_failed"
        );
        let persisted =
            &platform.adapter.records()[&format!("job:{}", job["id"].as_str().unwrap())].value;
        assert_eq!(
            persisted["uncertain_operations"].as_array().unwrap().len(),
            1,
            "dispatch uncertainty must predate the external mutation"
        );
        platform.clock.fetch_add(45, Ordering::SeqCst);
        assert_eq!(
            platform.call("POST", &step, json!({})).status,
            409,
            "a claimed job must remain leased throughout its step wall budget"
        );
        platform.clock.fetch_add(16, Ordering::SeqCst);
        let rejected = platform.json("POST", &step, json!({}))["job"].clone();
        assert_eq!(rejected["error"], "mutation_outcome_unknown", "{rejected}");
        assert_eq!(
            platform
                .adapter
                .calls()
                .iter()
                .filter(|(verb, value)| verb == method && value == path)
                .count(),
            0
        );
        assert_eq!(platform.late_requests.lock().unwrap().len(), 1);
    }
}

#[test]
fn expired_grant_after_journaling_does_not_poison_an_undispatched_mutation() {
    let platform = Platform::new();
    let job = platform.provision()["job"].clone();
    platform.journal_delay_ms.store(43500, Ordering::SeqCst);
    let step = format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap());
    let pending = platform.json("POST", &step, json!({}))["job"].clone();
    assert_eq!(pending["status"], "pending");
    assert_eq!(pending["retry_reason"], "wall_time_budget");
    assert_eq!(pending["retry_attempt"], 0);
    let saved = platform.adapter.records();
    let record = &saved[&format!("job:{}", job["id"].as_str().unwrap())].value;
    assert_eq!(record["uncertain_operations"], json!([]));
    assert_eq!(record["retry_attempts"], json!({}));
    assert_eq!(platform.adapter.key_count(), 0);
    assert_eq!(
        platform
            .adapter
            .calls()
            .iter()
            .filter(|(method, path)| method == "POST"
                && path == "/resources/object-storage/access-keys")
            .count(),
        0
    );
    platform.drain(pending);
    assert_eq!(platform.service()["service"]["status"], "active");
}

fn slow_fos_step_completes(method: &'static str, step: &str) {
    for millis in [7500, 8500] {
        let platform = Platform::new();
        let provision = platform.provision()["job"].clone();
        let job = if method == "GET" {
            platform.drain(provision);
            platform
                .adapter
                .seed_object("fos-fixture-logs", "raw/request/remove.gz");
            let service = platform.service();
            let teardown = platform.json(
                "POST",
                "/api/provision/teardown",
                json!({
                    "service_id":"fixtureService","confirm_service_id":"fixtureService",
                    "token":"fixture-token","generation":service["generation"],"delete_data":true
                }),
            );
            platform.until(teardown["job"].clone(), step)
        } else {
            let job = platform.until(provision, step);
            if method == "HEAD" {
                platform
                    .adapter
                    .seed_object("fos-fixture-logs", "customer/preserve.gz");
            }
            job
        };
        platform.fos_timeouts.lock().unwrap().clear();
        platform
            .fos_delays
            .lock()
            .unwrap()
            .push(FosDelay { method, millis });
        let result = platform.json(
            "POST",
            &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
            json!({}),
        );
        assert_eq!(
            result["job"]["status"], "pending",
            "{method} {millis}ms: {result}"
        );
        assert!(result["job"]["error"].is_null(), "{result}");
        assert!(result["job"]["retry_reason"].is_null(), "{result}");
        assert!(platform.fos_delays.lock().unwrap().is_empty());
        assert_eq!(ELAPSED_MS.with(Cell::get), millis);
        assert_eq!(
            platform.fos_timeouts.lock().unwrap()[0],
            std::time::Duration::from_secs(43)
        );
        platform.drain(result["job"].clone());
        assert_eq!(
            platform.service()["service"]["status"],
            if method == "GET" {
                "torn_down"
            } else {
                "active"
            }
        );
        if method == "HEAD" {
            assert_eq!(
                platform.adapter.object_keys("fos-fixture-logs"),
                vec!["customer/preserve.gz"]
            );
        } else if method == "GET" {
            assert!(platform.adapter.object_keys("fos-fixture-logs").is_empty());
        }
    }
}

#[test]
fn slow_fos_head_completes_inside_the_step_wall_budget() {
    slow_fos_step_completes("HEAD", "bucket_probe");
}

#[test]
fn slow_fos_put_completes_inside_the_step_wall_budget() {
    slow_fos_step_completes("PUT", "bucket_create");
}

#[test]
fn slow_fos_list_completes_inside_the_step_wall_budget() {
    slow_fos_step_completes("GET", "delete_page");
}

#[test]
fn fos_does_not_dispatch_after_the_business_grant_is_exhausted() {
    for elapsed in [43000, 44999, 45000] {
        let platform = Platform::new();
        CALLS.with(|calls| calls.set(7));
        ELAPSED_MS.with(|clock| clock.set(0));
        let context = admin::transport::Context::new(&platform);
        ELAPSED_MS.with(|clock| clock.set(elapsed));
        let error = context
            .fos(FosRequest {
                method: "PUT",
                region: "us-east-1",
                bucket: "fos-fixture-logs",
                key: "",
                query: "",
                access_key: "never-dispatched-ak",
                secret_key: "never-dispatched-sk",
                body: vec![],
                timeout: std::time::Duration::from_secs(60),
                response_limit: admin::transport::FOS_RESPONSE_LIMIT,
                content_md5: None,
            })
            .err()
            .expect("an exhausted business grant must reject before dispatch");
        assert_eq!(error.code, "wall_time_budget");
        assert_eq!(error.status, 503);
        let retry = error
            .retry
            .expect("an undispatched request must checkpoint for the next execution");
        assert_eq!(retry.reason, "wall_time_budget");
        assert_eq!(retry.at, platform.now() + 1);
        assert!(platform.fos_timeouts.lock().unwrap().is_empty());
        assert!(platform.adapter.calls().is_empty());
        assert_eq!(CALLS.with(Cell::get), 7);
    }
}

#[test]
fn fos_deadline_is_clipped_to_remaining_time_and_reports_expiration() {
    for millis in [8500, 28001] {
        let platform = Platform::new();
        platform.until(platform.provision()["job"].clone(), "bucket_probe");
        platform
            .adapter
            .seed_object("fos-fixture-logs", "customer/preserve.gz");
        let ak = platform
            .adapter
            .secret("fos_tmp_ak.fixtureService")
            .unwrap()
            .unwrap();
        let pair: Value = serde_json::from_str(
            &platform
                .adapter
                .secret("fos_tmp_sk.fixtureService")
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        platform.fos_delays.lock().unwrap().push(FosDelay {
            method: "HEAD",
            millis,
        });
        CALLS.with(|calls| calls.set(7));
        ELAPSED_MS.with(|clock| clock.set(0));
        let context = admin::transport::Context::new(&platform);
        ELAPSED_MS.with(|clock| clock.set(15000));
        let result = context.fos(FosRequest {
            method: "HEAD",
            region: "us-east-1",
            bucket: "fos-fixture-logs",
            key: "",
            query: "",
            access_key: &ak,
            secret_key: pair["secret_key"].as_str().unwrap(),
            body: vec![],
            timeout: std::time::Duration::from_secs(60),
            response_limit: admin::transport::FOS_RESPONSE_LIMIT,
            content_md5: None,
        });
        if millis == 8500 {
            assert_eq!(result.unwrap().status, 200);
            assert_eq!(ELAPSED_MS.with(Cell::get), 23500);
        } else {
            let error = result
                .err()
                .expect("the remaining grant must bound the request");
            assert_eq!(error.code, "object_storage_backend_deadline");
            assert_eq!(error.status, 502);
            assert_eq!(ELAPSED_MS.with(Cell::get), 43000);
        }
        assert_eq!(
            *platform.fos_timeouts.lock().unwrap(),
            vec![std::time::Duration::from_secs(28)]
        );
    }
}

#[test]
fn rum_provision_reconcile_and_teardown_manage_backend_dictionary_fos_assets_and_endpoints() {
    let platform = Platform::new();
    let versions = platform.json("GET", "/api/admin/rum/versions", json!({}));
    assert_eq!(versions["default"], "2.13.0");
    assert_eq!(versions["versions"], json!(["2.13.0", "2.10.0"]));

    let result = platform.json(
        "POST",
        "/api/admin/provision/execute",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1",
            "logging_enabled": true,
            "rum": {
                "enabled": true,
                "log_period": 120,
                "custom_condition": "req.http.Host == \"example.com\"",
                "faro_version": "2.10.0",
                "endpoint_name": "Fastly RUM Logs",
                "capture_vitals": true,
                "capture_performance": true,
                "capture_errors": true,
                "capture_events": true
            }
        }),
    );
    platform.drain(result["job"].clone());
    let current = platform.service();
    assert_eq!(current["service"]["status"], "active");
    assert_eq!(current["service"]["rum_enabled"], true);
    assert_eq!(current["service"]["rum"]["enabled"], true);
    assert_eq!(current["service"]["rum"]["log_period"], 120);
    assert_eq!(
        current["service"]["owned_endpoint_names"],
        json!(["Fastly Object Storage Logs", "Fastly RUM Logs"])
    );

    let endpoints = platform.adapter.resources("fixtureService", "logging/fos");
    assert_eq!(endpoints.len(), 2);
    assert!(endpoints
        .iter()
        .any(|e| e["name"] == "Fastly Object Storage Logs"));
    assert!(endpoints
        .iter()
        .any(|e| e["name"] == "Fastly RUM Logs" && e["period"] == "120"));

    let backends = platform.adapter.resources("fixtureService", "backend");
    assert_eq!(backends.len(), 1);
    assert_eq!(backends[0]["name"], "fos_origin");
    assert_eq!(
        backends[0]["request_condition"],
        "fastly_log_analytics_false"
    );

    let dicts = platform.adapter.resources("fixtureService", "dictionary");
    assert_eq!(dicts.len(), 1);
    assert_eq!(dicts[0]["name"], "fos_credentials");
    let items = platform
        .adapter
        .dictionary_items("dict-fixtureService-fos_credentials");
    assert_eq!(
        items.get("bucket").map(String::as_str),
        Some("fos-fixture-logs")
    );
    assert_eq!(items.get("region").map(String::as_str), Some("us-east-1"));
    assert!(items.get("access_key").unwrap().starts_with("fixture-ak-"));

    let objects = platform.adapter.bucket_objects("fos-fixture-logs");
    assert_eq!(
        objects
            .get("rum/faro-web-sdk-v2.10.0.iife.js")
            .map(Vec::len),
        Some(103553)
    );
    let tracker = String::from_utf8(objects.get("rum/rum-tracker.js").unwrap().clone()).unwrap();
    assert!(tracker.contains("var CAPTURE_VITALS = true;"));

    // Toggle capture_vitals via PATCH + reconcile (no VCL diff, updates rum-tracker.js in FOS).
    let patched = platform.json(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({
            "generation": current["generation"],
            "capture_vitals": false
        }),
    );
    assert_eq!(patched["service"]["capture_vitals"], false);
    let job = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({ "generation": patched["generation"] }),
    )["job"]
        .clone();
    platform.drain(job);
    let objects_after = platform.adapter.bucket_objects("fos-fixture-logs");
    let tracker_after =
        String::from_utf8(objects_after.get("rum/rum-tracker.js").unwrap().clone()).unwrap();
    assert!(tracker_after.contains("var CAPTURE_VITALS = false;"));
    assert_eq!(platform.service()["service"]["faro_version"], "2.10.0");

    // The pinned Faro version only changes on an explicit, supported upgrade.
    let current = platform.service();
    let unsupported = platform.call(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation": current["generation"], "faro_version": "2.11.0"}),
    );
    assert_eq!(unsupported.status, 400);
    let upgraded = platform.json(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation": current["generation"], "faro_version": "2.13.0"}),
    );
    assert_eq!(upgraded["service"]["faro_version"], "2.13.0");
    assert_eq!(upgraded["service"]["rum"]["faro_version"], "2.13.0");
    let job = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({ "generation": upgraded["generation"] }),
    )["job"]
        .clone();
    platform.drain(job);
    let objects_upgraded = platform.adapter.bucket_objects("fos-fixture-logs");
    assert_eq!(
        objects_upgraded
            .get("rum/faro-web-sdk-v2.13.0.iife.js")
            .map(Vec::len),
        Some(126190)
    );
    assert!(!objects_upgraded.contains_key("rum/faro-web-sdk-v2.10.0.iife.js"));
    let snippets = platform
        .adapter
        .resources("fixtureService", "snippet")
        .iter()
        .map(|snippet| snippet["content"].as_str().unwrap_or_default().to_owned())
        .collect::<String>();
    assert!(snippets.contains("/rum/faro-web-sdk-v2.13.0.iife.js"));
    assert!(!snippets.contains("faro-web-sdk-v2.10.0"));

    // Services always use the smallest compact wire format (zstd + positional array) and reject gzip/json.
    let current = platform.service();
    assert_eq!(current["service"]["compression_codec"], "zstd");
    assert_eq!(current["service"]["wire_format"], "array");
    for ep in platform.adapter.resources("fixtureService", "logging/fos") {
        assert_eq!(ep["compression_codec"], "zstd", "{ep}");
        assert!(ep.get("gzip_level").is_none(), "{ep}");
        assert!(ep["format"].as_str().unwrap().starts_with('['), "{ep}");
    }
    assert_eq!(
        platform
            .call(
                "PATCH",
                "/api/admin/services/fixtureService",
                json!({"generation": current["generation"], "compression_codec": "gzip"}),
            )
            .status,
        400
    );
    assert_eq!(
        platform
            .call(
                "PATCH",
                "/api/admin/services/fixtureService",
                json!({"generation": current["generation"], "compression_codec": "lz4"}),
            )
            .status,
        400
    );
    assert_eq!(
        platform
            .call(
                "PATCH",
                "/api/admin/services/fixtureService",
                json!({"generation": current["generation"], "wire_format": "json"}),
            )
            .status,
        400
    );
    assert_eq!(
        platform
            .call(
                "PATCH",
                "/api/admin/services/fixtureService",
                json!({"generation": current["generation"], "wire_format": "csv"}),
            )
            .status,
        400
    );
    let restored = platform.json(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({
            "generation": current["generation"],
            "compression_codec": "",
            "wire_format": ""
        }),
    );
    assert_eq!(restored["service"]["compression_codec"], "zstd");
    assert_eq!(restored["service"]["wire_format"], "array");
    let job = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({ "generation": restored["generation"] }),
    )["job"]
        .clone();
    platform.drain(job);
    for ep in platform.adapter.resources("fixtureService", "logging/fos") {
        assert_eq!(ep["compression_codec"], "zstd", "{ep}");
        assert!(ep.get("gzip_level").is_none(), "{ep}");
        assert!(ep["format"].as_str().unwrap().starts_with('['), "{ep}");
    }

    // Teardown removes backend, dictionary, endpoints, snippets, conditions, and FOS RUM assets.
    let current = platform.service();
    let teardown = platform.json(
        "POST",
        "/api/admin/provision/teardown",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "confirm_service_id": "fixtureService",
            "generation": current["generation"],
            "remove_logging": true,
            "remove_snippets": true,
            "delete_fos_keys": true,
            "delete_data": false,
            "delete_fos_bucket": false
        }),
    );
    platform.drain(teardown["job"].clone());
    assert_eq!(platform.service()["service"]["status"], "torn_down");
    assert!(platform
        .adapter
        .resources("fixtureService", "backend")
        .is_empty());
    assert!(platform
        .adapter
        .resources("fixtureService", "dictionary")
        .is_empty());
    assert!(platform
        .adapter
        .resources("fixtureService", "logging/fos")
        .is_empty());
    assert!(platform.adapter.object_keys("fos-fixture-logs").is_empty());
}

#[test]
fn retention_settings_patch_validates_and_records_a_service_audit_event() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    let current = platform.service();
    assert_eq!(current["service"]["delete_after"], true);
    assert_eq!(current["service"]["log_retention_days"], 30);
    let bad = platform.call(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation":current["generation"],"log_retention_days":4000}),
    );
    assert_eq!(bad.status, 400);
    let updated = platform.json(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation":current["generation"],"delete_after":false,"log_retention_days":0,"rum_retention_days":90}),
    );
    assert_eq!(updated["service"]["delete_after"], false);
    assert_eq!(updated["service"]["log_retention_days"], 0);
    assert_eq!(updated["service"]["rum_retention_days"], 90);
    let record = platform.adapter.get("svc:fixtureService").unwrap().unwrap();
    assert!(record.value["_audit"]
        .to_string()
        .contains("config.updated"));
}

/// Pins the KV billing shape of a steady-state idle tick: the cost analysis in
/// `docs/investigations/2026-11-02-compute-cost-optimization.md` rests on reads dominating
/// writes, and on the reported `kv_ops` exceeding the ops actually performed.
#[test]
fn an_idle_tick_is_read_dominated_and_over_reports_its_kv_ops() {
    use fastly_log_analytics::{shell::Shell, state::MemoryStore};

    let platform = Platform::new();
    let store = MemoryStore::default();
    platform.drain(platform.provision()["job"].clone());
    platform.clock.fetch_add(120, Ordering::SeqCst);
    platform.shield.store(true, Ordering::SeqCst);

    let tick_req = Request {
        method: "GET".into(),
        path: "/_tick".into(),
        headers: BTreeMap::from([("x-flp-front-key".into(), "fixture-front-key".into())]),
        body: vec![],
    };

    // Warm past the one-off first-tick work so the measured tick is steady state.
    for _ in 0..3 {
        platform.clock.fetch_add(120, Ordering::SeqCst);
        CALLS.with(|c| c.set(0));
        SECRETS.with(|s| s.borrow_mut().clear());
        assert_eq!(
            Shell::new(&platform, &store, &[]).handle(&tick_req).status,
            200
        );
    }

    platform.clock.fetch_add(120, Ordering::SeqCst);
    CALLS.with(|c| c.set(0));
    SECRETS.with(|s| s.borrow_mut().clear());
    KV_CLASS.with(|ops| ops.borrow_mut().clear());
    assert_eq!(
        Shell::new(&platform, &store, &[]).handle(&tick_req).status,
        200
    );
    let ops = KV_CLASS.with(|ops| ops.borrow().clone());
    let reads = ops.iter().filter(|(c, _)| c.starts_with("read")).count();
    let writes = ops.iter().filter(|(c, _)| *c == "write").count();
    eprintln!("idle tick KV ops ({} total): {ops:#?}", ops.len());
    eprintln!("reads={reads} writes={writes}");

    assert_eq!(
        (reads, writes),
        (5, 2),
        "steady-state idle tick should perform 5 KV reads and 2 KV writes: {ops:#?}"
    );
    assert!(
        reads > writes,
        "idle tick should be read-dominated, got {reads} reads vs {writes} writes: {ops:#?}"
    );
    assert!(
        !ops.iter().any(|(c, _)| *c == "read_api"),
        "an idle tick should not reach the control-plane API: {ops:#?}"
    );
    // Mutable schedule and ingest-coordination keys remain uncached KV reads every tick.
    for key in [
        "cron:state",
        "ingest:plan",
        "ingest:watermark",
        "ingest:backfill",
    ] {
        assert!(
            ops.iter().any(|(c, k)| *c == "read" && k == key),
            "expected an uncached read of {key}: {ops:#?}"
        );
    }
    // Stable config keys (`admin:install`, `svc:{sid}`, `settings:operational_logs`) are served
    // from Core Cache after warm-up rather than billing Class B reads on every tick.
    for key in [
        "admin:install",
        "svc:fixtureService",
        "settings:operational_logs",
    ] {
        assert!(
            !ops.iter().any(|(_, k)| k == key),
            "expected {key} to be served from KV cache on steady-state tick: {ops:#?}"
        );
    }
}
