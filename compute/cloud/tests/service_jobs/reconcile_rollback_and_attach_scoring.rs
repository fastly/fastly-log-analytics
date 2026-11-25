use super::support::*;
use fastly_log_analytics::{
    admin::{self, ApiRequest, FosRequest},
    ingest,
    telemetry::LogBuffer,
};
use serde_json::{json, Value};

fn seed_other_install_for_attach(platform: &Platform, with_scoring: bool) {
    let other = Platform::new();
    let started = other.json(
        "POST",
        "/api/admin/provision/execute",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-other-logs",
            "fos_region": "us-east-1",
            "log_fields": {"groups": ["A", "C"], "custom_fields": [], "field_overrides": {}, "field_limits": {}}
        }),
    );
    other.drain(started["job"].clone());
    for resource in ["snippet", "logging/fos", "condition"] {
        for value in other.adapter.resources("fixtureService", resource) {
            if value["name"]
                .as_str()
                .is_some_and(|n| n.starts_with("flp-owner-"))
            {
                continue;
            }
            platform
                .adapter
                .seed_resource("fixtureService", resource, value);
        }
    }
    if with_scoring {
        platform.adapter.seed_resource(
            "fixtureService",
            "snippet",
            json!({
                "name": "Session Scoring - Recv",
                "type": "recv",
                "priority": 45,
                "content": "set req.http.x-edge-score:score = \"42\";\nset req.http.x-edge-score:l1 = \"10\";\nset req.http.x-edge-score:l2 = \"32\";\nset req.http.x-edge-score:compliance = \"ok\";\nset req.http.x-edge-score:reason = \"none\";\nset req.http.x-edge-score:sid = \"abc123def456\";\nset req.http.x-edge-score:rtt = \"100\";\nset req.http.x-edge-score:exec = \"50\";\nset req.http.x-edge-score:matrix = \"2026-11-20-a\";"
            }),
        );
    }
}

fn enable_scoring_on(platform: &Platform) -> Value {
    let en = platform.json(
        "POST",
        "/api/services/fixtureService/scoring/enable",
        json!({}),
    );
    let done = platform.drain(en["job"].clone());
    assert_eq!(done["status"], "complete", "{done}");
    done
}

#[test]
fn reconcile_accepts_all_patchable_settings_and_dry_run_leaves_service_untouched() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    let initial = platform.service();
    assert!(initial["service"].get("deployed_snapshot").is_none());
    assert!(initial["service"]
        .get("previous_deployed_snapshot")
        .is_none());
    assert!(initial["service"]
        .get("previous_activated_version")
        .is_none());
    assert!(initial["service"]["active_log_field_ids"].is_array());

    for hc_name in ["flp_clock", "flp_flush"] {
        platform.adapter.seed_resource(
            "fixtureFront",
            "healthcheck",
            json!({"name": hc_name, "check_interval": 1000}),
        );
    }

    platform.adapter.clear_calls();
    let dry = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({
            "generation": initial["generation"],
            "dry_run": true,
            "sample_rate": 42,
            "log_period": 120,
            "rum": {"enabled": true, "capture_vitals": false},
            "cmcd": {"enabled": true, "mode": "headers", "version": 1},
        }),
    );
    let done = platform.drain(dry["job"].clone());
    assert_eq!(done["status"], "complete", "{done}");
    assert!(
        done["changes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["resource"] == "fos_object" && c["action"] == "upsert"),
        "expected dry_run changes to include fos_object: {done}"
    );
    assert!(
        !done["front_changes"].as_array().unwrap().is_empty(),
        "expected dry_run front_changes for log_period=120: {done}"
    );
    assert!(
        platform
            .adapter
            .resources("fixtureFront", "healthcheck")
            .iter()
            .all(|h| h["check_interval"] == 1000),
        "dry_run must not mutate front healthchecks"
    );
    assert!(!platform
        .adapter
        .calls()
        .iter()
        .any(|(m, p)| m == "PUT" && (p.ends_with("/clone") || p.ends_with("/activate"))));
    assert!(!platform
        .adapter
        .bucket_objects("fos-fixture-logs")
        .keys()
        .any(|k| k.starts_with("rum/")));

    let after = platform.service();
    assert_eq!(after["service"]["sample_rate"], 100);
    assert_eq!(after["service"]["log_period"], 60);
    assert_eq!(after["service"]["rum"]["enabled"], false);
    assert_eq!(after["service"]["cmcd"]["enabled"], false);
    assert!(after["service"]["pending_job_id"].is_null());
}

#[test]
fn reconcile_validate_failure_rolls_back_svc_and_defers_fos_object_sync() {
    let platform = Platform::new();
    let prov = platform.json(
        "POST",
        "/api/admin/provision/execute",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1",
            "rum": {"enabled": true, "faro_version": "2.10.0", "capture_vitals": true},
            "log_fields": {"groups": ["A", "C"], "custom_fields": [], "field_overrides": {}, "field_limits": {}}
        }),
    );
    platform.drain(prov["job"].clone());

    let before_tracker = String::from_utf8(
        platform
            .adapter
            .bucket_objects("fos-fixture-logs")
            .get("rum/rum-tracker.js")
            .unwrap()
            .clone(),
    )
    .unwrap();
    assert!(before_tracker.contains("var CAPTURE_VITALS = true;"));

    let cur = platform.service();
    let before_cfg_ver = cur["service"]["config_version"].as_u64().unwrap();
    let rec = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({
            "generation": cur["generation"],
            "sample_rate": 25,
            "faro_version": "2.13.0",
            "capture_vitals": false
        }),
    );
    let at_validate = platform.until(rec["job"].clone(), "validate");
    // Before validate passes, FOS objects must NOT have been overwritten!
    let mid_objects = platform.adapter.bucket_objects("fos-fixture-logs");
    assert!(mid_objects.contains_key("rum/faro-web-sdk-v2.10.0.iife.js"));
    assert!(!mid_objects.contains_key("rum/faro-web-sdk-v2.13.0.iife.js"));
    let mid_tracker =
        String::from_utf8(mid_objects.get("rum/rum-tracker.js").unwrap().clone()).unwrap();
    assert!(mid_tracker.contains("var CAPTURE_VITALS = true;"));

    platform.adapter.invalid_draft(true);
    let failed = platform.json(
        "POST",
        &format!(
            "/api/admin/jobs/{}/step",
            at_validate["id"].as_str().unwrap()
        ),
        json!({}),
    )["job"]
        .clone();
    assert_eq!(failed["status"], "failed");
    assert_eq!(failed["error"], "draft_validation_failed");

    let after = platform.service();
    assert_eq!(after["service"]["sample_rate"], 100);
    assert_eq!(after["service"]["faro_version"], "2.10.0");
    assert_eq!(after["service"]["capture_vitals"], true);
    assert!(after["service"]["pending_job_id"].is_null());
    assert!(after["service"]["config_version"].as_u64().unwrap() > before_cfg_ver);

    let after_objects = platform.adapter.bucket_objects("fos-fixture-logs");
    assert!(after_objects.contains_key("rum/faro-web-sdk-v2.10.0.iife.js"));
    assert!(!after_objects.contains_key("rum/faro-web-sdk-v2.13.0.iife.js"));
}

#[test]
fn reconcile_post_activate_failure_reactivates_previous_version_and_rolls_back_svc() {
    let platform = Platform::new();
    let prov = platform.json(
        "POST",
        "/api/admin/provision/execute",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1",
            "rum": {"enabled": true, "faro_version": "2.10.0", "capture_vitals": true},
            "log_fields": {"groups": ["A", "C"], "custom_fields": [], "field_overrides": {}, "field_limits": {}}
        }),
    );
    platform.drain(prov["job"].clone());

    let before = platform.service();
    let orig_ver = before["service"]["last_activated_version"]
        .as_u64()
        .unwrap();

    let rec = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({
            "generation": before["generation"],
            "sample_rate": 33,
            "faro_version": "2.13.0"
        }),
    );
    let at_sync_rum = platform.until(rec["job"].clone(), "sync_rum");
    // Draft is now active on Fastly; inject a non-retryable FOS PUT failure to fail sync_rum.
    platform
        .adapter
        .fail_next("PUT", "/fos-fixture-logs/rum/", false);
    let failed = platform.json(
        "POST",
        &format!(
            "/api/admin/jobs/{}/step",
            at_sync_rum["id"].as_str().unwrap()
        ),
        json!({}),
    )["job"]
        .clone();
    assert_eq!(failed["status"], "failed");

    let (_, versions) = platform
        .adapter
        .api(ApiRequest {
            method: "GET",
            path: "/service/fixtureService/version",
            query: "",
            token: "fixture-token",
            body: None,
            timeout: std::time::Duration::from_secs(5),
        })
        .unwrap();
    let active_ver = versions
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["active"] == true)
        .and_then(|v| v["number"].as_u64())
        .unwrap();
    assert_eq!(
        active_ver, orig_ver,
        "post-activate reconcile failure must re-activate source_version"
    );

    let after = platform.service();
    assert_eq!(after["service"]["sample_rate"], 100);
    assert_eq!(after["service"]["faro_version"], "2.10.0");
    assert_eq!(after["service"]["last_activated_version"], orig_ver);
    assert!(after["service"]["pending_job_id"].is_null());

    let snap = platform
        .adapter
        .record_snapshot("svc:fixtureService")
        .unwrap();
    assert!(
        snap.value["deployed_snapshot"].is_object(),
        "deployed_snapshot must remain intact after rollback"
    );
}

#[test]
fn reconcile_rollback_preserves_audit_outbox_and_supports_deferred_step() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());

    let snap = platform
        .adapter
        .record_snapshot("svc:fixtureService")
        .unwrap();
    let mut val = snap.value.clone();
    val["_audit_outbox"] = Value::Array(
        (0..4095)
            .map(|i| json!({"actor":"fixtureAdmin","at":1_800_000_000u64 + i as u64,"event":"seed","config_version":1}))
            .collect(),
    );
    val["_audit_outbox_base"] = json!(100);
    platform.adapter.cache_invalidate("svc:fixtureService");
    platform
        .adapter
        .cas("svc:fixtureService", Some(&snap.generation), &val)
        .unwrap();

    let cur = platform.service();
    let rec = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation": cur["generation"], "sample_rate": 20}),
    );
    let at_validate = platform.until(rec["job"].clone(), "validate");
    platform.adapter.invalid_draft(true);
    let failed = platform.json(
        "POST",
        &format!(
            "/api/admin/jobs/{}/step",
            at_validate["id"].as_str().unwrap()
        ),
        json!({}),
    )["job"]
        .clone();
    assert_eq!(failed["status"], "failed");
    assert_eq!(failed["error"], "draft_validation_failed");

    let after = platform
        .adapter
        .record_snapshot("svc:fixtureService")
        .unwrap();
    assert_eq!(after.value["sample_rate"], 100);
    assert_eq!(after.value["_audit_outbox_base"], 100);
    assert_eq!(after.value["_audit_outbox"].as_array().unwrap().len(), 4097);

    // Also verify deferred `reconcile_rollback` step (e.g., if in-tick rollback lacked budget)
    // re-activates front_draft_version back to front_source_version and completes rollback cleanly.
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    let cur2 = platform.service();
    let rec2 = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation": cur2["generation"], "sample_rate": 45}),
    );
    let job2_id = rec2["job"]["id"].as_str().unwrap().to_owned();
    let _at_val2 = platform.until(rec2["job"].clone(), "validate");
    // Simulate front_clock having activated a new front version (v2 from v1) and job deferred to reconcile_rollback.
    let (_, front_clone) = platform
        .adapter
        .api(ApiRequest {
            method: "PUT",
            path: "/service/fixtureFront/version/1/clone",
            query: "",
            token: "fixture-token",
            body: None,
            timeout: std::time::Duration::from_secs(5),
        })
        .unwrap();
    let front_v2 = front_clone["number"].as_u64().unwrap();
    platform
        .adapter
        .api(ApiRequest {
            method: "PUT",
            path: &format!("/service/fixtureFront/version/{front_v2}/activate"),
            query: "",
            token: "fixture-token",
            body: None,
            timeout: std::time::Duration::from_secs(5),
        })
        .unwrap();

    let job2_key = format!("job:{job2_id}");
    let job2_rec = platform.adapter.record_snapshot(&job2_key).unwrap();
    let mut job2_val = job2_rec.value.clone();
    job2_val["step"] = json!("reconcile_rollback");
    job2_val["pending_failure"] = json!("simulated_post_front_failure");
    job2_val["front_source_version"] = json!(1);
    job2_val["front_draft_version"] = json!(front_v2);
    platform
        .adapter
        .cas(&job2_key, Some(&job2_rec.generation), &job2_val)
        .unwrap();

    let rb_res = platform.json(
        "POST",
        &format!("/api/admin/jobs/{job2_id}/step"),
        json!({}),
    );
    assert_eq!(rb_res["job"]["status"], "failed");
    assert_eq!(rb_res["job"]["error"], "simulated_post_front_failure");

    let (_, front_versions) = platform
        .adapter
        .api(ApiRequest {
            method: "GET",
            path: "/service/fixtureFront/version",
            query: "",
            token: "fixture-token",
            body: None,
            timeout: std::time::Duration::from_secs(5),
        })
        .unwrap();
    let front_active = front_versions
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["active"] == true)
        .and_then(|v| v["number"].as_u64())
        .unwrap();
    assert_eq!(
        front_active, 1,
        "deferred reconcile_rollback must re-activate front_source_version"
    );
    assert_eq!(platform.service()["service"]["sample_rate"], 100);
}

#[test]
fn explicit_rollback_endpoint_reactivates_previous_version_and_restores_snapshot() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());

    let v1_svc = platform.service();
    let v1_num = v1_svc["service"]["last_activated_version"]
        .as_u64()
        .unwrap();
    let unavail = platform.call(
        "POST",
        "/api/admin/services/fixtureService/rollback",
        json!({"generation": v1_svc["generation"]}),
    );
    assert_eq!(unavail.status, 409);
    assert_eq!(
        serde_json::from_slice::<Value>(&unavail.body).unwrap()["error"],
        "rollback_unavailable"
    );

    let rec = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation": v1_svc["generation"], "sample_rate": 40}),
    );
    let rec_done = platform.drain(rec["job"].clone());
    assert_eq!(rec_done["status"], "complete");

    let v2_svc = platform.service();
    assert_eq!(v2_svc["service"]["sample_rate"], 40);
    let v2_num = v2_svc["service"]["last_activated_version"]
        .as_u64()
        .unwrap();
    assert_ne!(v2_num, v1_num);

    let rolled = platform.json(
        "POST",
        "/api/admin/services/fixtureService/rollback",
        json!({"generation": v2_svc["generation"]}),
    );
    assert_eq!(rolled["activated_version"], v1_num);
    assert_eq!(rolled["service"]["sample_rate"], 100);
    assert_eq!(rolled["service"]["last_activated_version"], v1_num);

    let (_, versions) = platform
        .adapter
        .api(ApiRequest {
            method: "GET",
            path: "/service/fixtureService/version",
            query: "",
            token: "fixture-token",
            body: None,
            timeout: std::time::Duration::from_secs(5),
        })
        .unwrap();
    let active_ver = versions
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["active"] == true)
        .and_then(|v| v["number"].as_u64())
        .unwrap();
    assert_eq!(active_ver, v1_num);

    // Second rollback must return 409 rollback_unavailable.
    let again = platform.call(
        "POST",
        "/api/admin/services/fixtureService/rollback",
        json!({"generation": rolled["generation"]}),
    );
    assert_eq!(again.status, 409);
    assert_eq!(
        serde_json::from_slice::<Value>(&again.body).unwrap()["error"],
        "rollback_unavailable"
    );
}

#[test]
fn tick_skips_commit_fields_and_uses_deployed_snapshot_schema_mid_reconcile() {
    let platform = Platform::new();
    let prov = platform.provision();
    // While status == "provisioning", active_service (load_installed_service) must return None.
    assert!(admin::active_service(&platform).unwrap().is_none());
    platform.drain(prov["job"].clone());
    assert!(admin::active_service(&platform).unwrap().is_some());

    let cur = platform.service();
    let mut lf = cur["service"]["log_fields"].clone();
    lf["custom_fields"] = json!([{
        "name": "cf_unactivated",
        "label": "Unactivated Field",
        "vcl_log_expression": "req.http.x-cf-unactivated",
        "collection_stage": "edge",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 32,
        "enabled": true
    }]);
    let rec = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation": cur["generation"], "log_fields": lf}),
    );
    let _at_validate = platform.until(rec["job"].clone(), "validate");

    let snap = platform
        .adapter
        .record_snapshot("svc:fixtureService")
        .unwrap();
    let svc: admin::Service = serde_json::from_value(snap.value.clone()).unwrap();
    assert!(svc.pending_job_id.is_some());

    let (ak, sk) = admin::fos_credentials(&platform, &svc).unwrap();
    let put_resp = platform
        .adapter
        .fos(FosRequest {
            method: "PUT",
            region: &svc.fos_region,
            bucket: &svc.fos_bucket,
            key: "raw/request/2026/11/25/10/00/part-0.zst",
            query: "",
            access_key: &ak,
            secret_key: &sk,
            body: b"placeholder".to_vec(),
            timeout: std::time::Duration::from_secs(5),
            response_limit: 1024,
            content_md5: None,
        })
        .unwrap();
    assert_eq!(put_resp.status, 200);

    // Simulate a tick/ingest run while pending_job_id is set (and candidates exist so commit_fields is reached).
    let buffer = LogBuffer::new("fixtureService", "tick-fence");
    let options = ingest::coordinator::Options::request(&platform);
    CALLS.with(|c| c.set(0));
    let (report, res) = ingest::coordinator::coordinate_reporting(
        &platform,
        &svc,
        &ak,
        &sk,
        std::time::Instant::now(),
        &buffer,
        &options,
    );
    assert!(res.is_ok(), "{res:?}");
    assert_eq!(report.listed, 1);

    // Un-activated custom field must NOT be marked committed: true mid-reconcile.
    let after_tick = platform
        .adapter
        .record_snapshot("svc:fixtureService")
        .unwrap();
    let cf = after_tick.value["log_fields"]["custom_fields"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["name"] == "cf_unactivated")
        .unwrap();
    assert_ne!(
        cf["committed"], true,
        "commit_fields must not commit un-activated fields while pending_job_id is Some"
    );
}

#[test]
fn scoring_ssot_persists_enforce_config_and_rolls_back_failed_snippet_update() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    enable_scoring_on(&platform);

    platform.json(
        "PUT",
        "/api/services/fixtureService/scoring/enforce-threshold?confirm=true",
        json!({"enforce_threshold": 80}),
    );
    platform.json(
        "PUT",
        "/api/services/fixtureService/scoring/l2-enforce?confirm=true",
        json!({"enabled": true}),
    );
    let cfg = platform.json(
        "GET",
        "/api/services/fixtureService/scoring/config",
        json!({}),
    );
    assert_eq!(cfg["enforce_threshold"]["enforce_threshold"], 80);
    assert_eq!(cfg["l2_enforce"]["enabled"], true);
    assert!(cfg["l2_enforce"]["l2_enabled_at"].is_u64());

    platform.json(
        "PUT",
        "/api/services/fixtureService/scoring/exclude-regex?confirm=true",
        json!({"exclude_url_regex": r"^/health$"}),
    );
    platform.json(
        "PUT",
        "/api/services/fixtureService/scoring/enforce-status-code?confirm=true",
        json!({"status_code": 403}),
    );
    let status_ok = platform.json(
        "GET",
        "/api/services/fixtureService/scoring/status",
        json!({}),
    );
    assert_eq!(
        status_ok["scorer_drift"], false,
        "fingerprint must include exclude_url_regex & enforce_status_code: {status_ok}"
    );
    assert_eq!(status_ok["scoring"]["enforce_threshold"], 80);
    assert_eq!(status_ok["scoring"]["l2_enforce_enabled"], true);

    // Failed draft validation during exclude-regex must roll back svc:<id> to ^/health$.
    platform.adapter.invalid_draft(true);
    let fail_regex = platform.call(
        "PUT",
        "/api/services/fixtureService/scoring/exclude-regex?confirm=true",
        json!({"exclude_url_regex": r"^/broken$"}),
    );
    assert_eq!(fail_regex.status, 400);
    let fail_code = platform.call(
        "PUT",
        "/api/services/fixtureService/scoring/enforce-status-code?confirm=true",
        json!({"status_code": 429}),
    );
    assert_eq!(fail_code.status, 400);
    platform.adapter.invalid_draft(false);

    let after = platform.json(
        "GET",
        "/api/services/fixtureService/scoring/status",
        json!({}),
    );
    assert_eq!(after["scoring"]["exclude_url_regex"], r"^/health$");
    assert_eq!(after["scoring"]["enforce_status_code"], 403);
    assert_eq!(after["scorer_drift"], false);
}

#[test]
fn attach_mode_scoring_present_missing_fields_edge_fields_and_scoring_status() {
    // 1. Other install without scoring: scoring_present=false, edge_score reported missing.
    let p_no_sc = Platform::new();
    seed_other_install_for_attach(&p_no_sc, false);
    let mut fields_with_edge = json!({
        "groups": ["A", "C"],
        "custom_fields": [],
        "field_overrides": {},
        "field_limits": {}
    });
    fields_with_edge["custom_fields"] = json!([{
        "name": "edge_score",
        "label": "Edge Score",
        "description": "Score",
        "vcl_log_expression": "req.http.x-edge-score:score",
        "collection_stage": "deliver",
        "sql_type": "INTEGER",
        "duckdb_type": "INTEGER",
        "value_type": "numeric",
        "bytes_estimate": 4,
        "enabled": true
    }]);
    let val_no_sc = p_no_sc.json(
        "POST",
        "/api/admin/provision/validate",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "capture": "attached",
            "log_fields": fields_with_edge
        }),
    );
    assert_eq!(val_no_sc["capture"]["scoring_present"], false);
    assert_eq!(
        val_no_sc["capture"]["missing_fields"],
        json!(["edge_score"])
    );

    // 2. Non-owned endpoint containing "edge_score" also sets scoring_present=true.
    let mut ep = p_no_sc
        .adapter
        .resources("fixtureService", "logging/fos")
        .into_iter()
        .find(|v| v["name"] == "Fastly Object Storage Logs")
        .unwrap();
    ep["format"] = json!(format!(
        "{}{}",
        ep["format"].as_str().unwrap(),
        r#","edge_score":%{req.http.x-edge-score:score}V"#
    ));
    p_no_sc
        .adapter
        .seed_resource("fixtureService", "logging/fos", ep);
    let val_ep_sc = p_no_sc.json(
        "POST",
        "/api/admin/provision/validate",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "capture": "attached",
            "log_fields": {"groups": ["A", "C"], "custom_fields": [], "field_overrides": {}, "field_limits": {}}
        }),
    );
    assert_eq!(val_ep_sc["capture"]["scoring_present"], true);

    // 3. Other install WITH scoring snippets: scoring_present=true, edge_score NOT in missing_fields.
    let platform = Platform::new();
    seed_other_install_for_attach(&platform, true);
    let val_sc = platform.json(
        "POST",
        "/api/admin/provision/validate",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "capture": "attached",
            "log_fields": fields_with_edge
        }),
    );
    assert_eq!(val_sc["capture"]["scoring_present"], true);
    assert_eq!(val_sc["capture"]["missing_fields"], json!([]));

    // 4. Preview accepts edge_score when capture=attached and enabled: false omits it from format.
    let mut preview_fields = fields_with_edge.clone();
    preview_fields["custom_fields"][0]["enabled"] = json!(false);
    let preview = platform.json(
        "POST",
        "/api/admin/log-fields/preview",
        json!({
            "capture": "attached",
            "log_fields": preview_fields
        }),
    );
    assert!(!preview["format"]
        .as_str()
        .unwrap()
        .contains("\"edge_score\""));

    // 5. Provision Capture::Attached with edge_score and check scoring/status skips pragma check.
    let prov = platform.json(
        "POST",
        "/api/admin/provision/execute",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-attach-logs",
            "fos_region": "us-east-1",
            "capture": "attached",
            "log_fields": fields_with_edge
        }),
    );
    platform.drain(prov["job"].clone());

    // Even with edge_score in our own owned endpoint, validate must NOT count our own endpoint!
    // (When other install has no scoring snippets/endpoints, re-validating won't see our owned ep).
    platform.adapter.clear_calls();
    let sc_status = platform.json(
        "GET",
        "/api/services/fixtureService/scoring/status",
        json!({}),
    );
    assert_eq!(sc_status["capture"], "attached");
    assert_eq!(sc_status["scoring_fields_enabled"], true);
    assert!(sc_status["preflight_pragmas_ready"].is_null());
    assert!(
        !platform
            .adapter
            .calls()
            .iter()
            .any(|(_, p)| p.contains("/snippet")),
        "Capture::Attached scoring/status must skip pragma snippet check"
    );

    // 6. Simulate ingest setting committed: true on edge_score, then toggle enabled: false via PATCH.
    let snap = platform
        .adapter
        .record_snapshot("svc:fixtureService")
        .unwrap();
    let mut val = snap.value.clone();
    val["log_fields"]["custom_fields"][0]["committed"] = json!(true);
    platform.adapter.cache_invalidate("svc:fixtureService");
    platform
        .adapter
        .cas("svc:fixtureService", Some(&snap.generation), &val)
        .unwrap();

    let cur = platform.service();
    let mut toggled_lf = cur["service"]["log_fields"].clone();
    toggled_lf["custom_fields"][0]["enabled"] = json!(false);
    let patched = platform.json(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({
            "generation": cur["generation"],
            "log_fields": toggled_lf
        }),
    );
    assert!(!patched["service"]["active_log_field_ids"]
        .as_array()
        .unwrap()
        .iter()
        .any(|id| id == "edge_score"));
    let sc_status_after = platform.json(
        "GET",
        "/api/services/fixtureService/scoring/status",
        json!({}),
    );
    assert_eq!(sc_status_after["scoring_fields_enabled"], false);
}

#[test]
fn rum_enable_claims_rid_and_populates_timing_allow_origin_then_disable_releases_rid() {
    let platform = Platform::new();
    platform.drain(platform.provision()["job"].clone());
    platform.adapter.seed_resource(
        "fixtureService",
        "domain",
        json!({"name": "WWW.Example.com"}),
    );
    platform.adapter.seed_resource(
        "fixtureService",
        "domain",
        json!({"name": "*.wild.example.com"}),
    );

    let initial = platform.service();
    assert!(!initial["service"]["active_log_field_ids"]
        .as_array()
        .unwrap()
        .iter()
        .any(|id| id == "rid"));

    // Enabling RUM on a managed service claims `rid` and populates timing_allow_origins from service domains.
    let enable = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({
            "generation": initial["generation"],
            "dry_run": false,
            "rum": {"enabled": true, "capture_vitals": true},
        }),
    );
    let enabled_job = platform.drain(enable["job"].clone());
    assert_eq!(enabled_job["status"], "complete", "{enabled_job}");

    let after_enable = platform.service();
    assert_eq!(
        after_enable["service"]["feature_enabled_fields"]["rum"],
        json!(["rid"])
    );
    assert!(after_enable["service"]["active_log_field_ids"]
        .as_array()
        .unwrap()
        .iter()
        .any(|id| id == "rid"));
    assert_eq!(
        after_enable["service"]["rum"]["timing_allow_origins"],
        json!(["https://www.example.com"])
    );

    let deliver = platform
        .adapter
        .resources("fixtureService", "snippet")
        .into_iter()
        .find(|s| s["name"] == "Fastly Log Analytics - vcl_deliver")
        .expect("vcl_deliver snippet present");
    let deliver_vcl = deliver["content"].as_str().unwrap();
    assert!(deliver_vcl.contains("Timing-Allow-Origin = \"https://www.example.com\";"));
    assert!(deliver_vcl.contains("Server-Timing = \"rid;desc=\" + req.http.x-req-id;"));

    // Disabling RUM releases `rid` when RUM claimed it and no other feature still needs it.
    let disable = platform.json(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({
            "generation": after_enable["generation"],
            "dry_run": false,
            "rum": {"enabled": false},
        }),
    );
    let disabled_job = platform.drain(disable["job"].clone());
    assert_eq!(disabled_job["status"], "complete", "{disabled_job}");

    let after_disable = platform.service();
    assert!(after_disable["service"]
        .get("feature_enabled_fields")
        .and_then(|v| v.get("rum"))
        .is_none());
    assert!(!after_disable["service"]["active_log_field_ids"]
        .as_array()
        .unwrap()
        .iter()
        .any(|id| id == "rid"));
}
