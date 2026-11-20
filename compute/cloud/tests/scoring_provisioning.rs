use fastly_log_analytics::{
    admin::{
        self, fixture::FixtureAdapter, ApiRawRequest, ApiRequest, FosRequest, FosResponse, Record,
    },
    http::Request,
    setup::SetupStores,
    shell::Runtime,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
};

struct Platform {
    adapter: FixtureAdapter,
    clock: AtomicU64,
    random_counter: AtomicU64,
}

impl Platform {
    fn new() -> Self {
        Self {
            adapter: FixtureAdapter::new(),
            clock: AtomicU64::new(1_800_000_000),
            random_counter: AtomicU64::new(1),
        }
    }

    fn call(&self, method: &str, path: &str, body: Value) -> (u16, Value) {
        let resp = admin::handle(
            self,
            "fixtureAdmin",
            &Request {
                method: method.into(),
                path: path.into(),
                headers: BTreeMap::new(),
                body: if method == "GET" {
                    vec![]
                } else {
                    body.to_string().into_bytes()
                },
            },
        );
        let parsed = serde_json::from_slice(&resp.body).unwrap_or(Value::Null);
        (resp.status, parsed)
    }

    fn ok(&self, method: &str, path: &str, body: Value) -> Value {
        let (status, val) = self.call(method, path, body);
        assert_eq!(status, 200, "{method} {path} -> {status}: {val}");
        val
    }

    fn run_job(&self, mut job: Value) -> Value {
        let mut same_step = 0u32;
        for _ in 0..200 {
            if job["status"] == "complete" || job["status"] == "failed" {
                return job;
            }
            if let Some(at) = job["retry_at"].as_u64() {
                self.clock.fetch_max(at, Ordering::SeqCst);
            }
            let prev_step = job["step"].as_str().unwrap_or("").to_owned();
            let id = job["id"].as_str().unwrap();
            let (status, res) = self.call("POST", &format!("/api/admin/jobs/{id}/step"), json!({}));
            if status == 503 {
                self.clock.fetch_add(65, Ordering::SeqCst);
                job = self.ok("GET", &format!("/api/admin/jobs/{id}"), json!({}))["job"].clone();
                continue;
            }
            assert_eq!(
                status, 200,
                "POST /api/admin/jobs/{id}/step -> {status}: {res}"
            );
            job = res["job"].clone();
            if job["step"].as_str() == Some(prev_step.as_str()) {
                same_step += 1;
                if same_step > 40 {
                    panic!("job stalled at step {prev_step}: {job}");
                }
            } else {
                same_step = 0;
            }
        }
        panic!("job did not finish: {job}");
    }

    fn provision_logging_service(&self) {
        let job = self.ok(
            "POST",
            "/api/admin/provision/execute",
            json!({
                "token": "fixture-token",
                "service_id": "fixtureService",
                "fos_bucket": "fos-fixture-logs",
                "fos_region": "us-east-1",
                "log_fields": {"groups":["A","C"],"custom_fields":[],"field_overrides":{},"field_limits":{}}
            }),
        )["job"]
            .clone();
        let done = self.run_job(job);
        assert_eq!(done["status"], "complete", "{done}");
    }
}

impl SetupStores for Platform {
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

impl Runtime for Platform {
    fn config(&self, key: &str) -> Result<Option<String>, String> {
        Ok(Some(
            match key {
                "flp_service_id" => "fixtureApp",
                "worker_service_id" => "fixtureWorker",
                "front_service_id" => "fixtureFront",
                "install_name" => "fixture",
                "secret_store_id" => "fixtureSecrets",
                "buckets_dictionary_id" => "fixtureBuckets",
                "credentials_dictionary_id" => "fixtureCredentials",
                "front_auth_dictionary_id" => "fixtureAuth",
                "public_base_url" => "https://logs.example.com",
                _ => "fixtureValue",
            }
            .into(),
        ))
    }
    fn secret(&self, name: &str) -> Result<Option<String>, String> {
        match name {
            "cdn_key" => Ok(Some("fixture-cdn-key".into())),
            "front_key" => Ok(Some("fixture-front-key".into())),
            "fastly_token" => Ok(Some("fixture-token".into())),
            _ => self.adapter.secret(name),
        }
    }
    fn random(&self) -> Result<[u8; 32], String> {
        let n = self.random_counter.fetch_add(1, Ordering::SeqCst);
        let mut out = [0u8; 32];
        out[..8].copy_from_slice(&n.to_le_bytes());
        out[8..16].copy_from_slice(&(n.wrapping_mul(7)).to_le_bytes());
        Ok(out)
    }
    fn now(&self) -> u64 {
        self.clock.load(Ordering::SeqCst)
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
    fn admin_get(&self, key: &str) -> Result<Option<Record>, String> {
        self.adapter.get(key)
    }
    fn admin_get_fresh(&self, key: &str, _: &str) -> Result<Option<Record>, String> {
        self.adapter.get(key)
    }
    fn admin_cas(
        &self,
        key: &str,
        generation: Option<&str>,
        value: &Value,
    ) -> Result<bool, String> {
        self.adapter.cas(key, generation, value)
    }
    fn admin_api(&self, request: ApiRequest<'_>) -> Result<admin::ApiResponse, String> {
        self.adapter.api(request).map(Into::into)
    }
    fn admin_api_raw(&self, request: ApiRawRequest<'_>) -> Result<admin::ApiResponse, String> {
        self.adapter.api_raw(request).map(Into::into)
    }
    fn admin_fos(&self, request: FosRequest<'_>) -> Result<FosResponse, String> {
        self.adapter.fos(request)
    }
    fn scorer_package(&self) -> Option<Vec<u8>> {
        Some(b"fixture-session-scorer-wasm-tar-gz".to_vec())
    }
}

#[test]
fn scoring_enable_checks_pragmas_and_entitlements() {
    let p = Platform::new();
    p.provision_logging_service();

    p.adapter.set_pragmas_missing("fixtureService", true);
    let (status, err) = p.call(
        "POST",
        "/api/services/fixtureService/scoring/enable",
        json!({}),
    );
    assert_eq!(status, 409);
    assert_eq!(err["error"], "missing_scoring_pragmas");

    p.adapter.set_pragmas_missing("fixtureService", false);
    p.adapter.disable_product("kv_store");
    let res = p.ok(
        "POST",
        "/api/services/fixtureService/scoring/enable",
        json!({}),
    );
    let job = p.run_job(res["job"].clone());
    assert_eq!(job["status"], "failed");
    assert_eq!(job["error"], "kv_store_not_enabled");
}

#[test]
fn scoring_enable_isolates_secrets_and_completes_control_plane_ops() {
    let p = Platform::new();
    p.provision_logging_service();

    // Enable without passing token in body (uses brokered fastly_token_ref).
    let res = p.ok(
        "POST",
        "/api/services/fixtureService/scoring/enable",
        json!({}),
    );
    let job_id = res["job"]["id"].as_str().unwrap().to_owned();
    let done = p.run_job(res["job"].clone());
    assert_eq!(done["status"], "complete", "{done}");

    let status = p.ok(
        "GET",
        "/api/services/fixtureService/scoring/status",
        json!({}),
    );
    assert_eq!(status["enabled"], true);
    assert_eq!(status["supported"], true);
    let sc = &status["scoring"];
    let scorer_sid = sc["scoring_service_id"].as_str().unwrap();
    let keys_store_id = sc["scoring_keys_store_id"].as_str().unwrap();
    let cfg_store_id = sc["scoring_config_store_id"].as_str().unwrap();
    let matrix_store_id = sc["scoring_matrix_store_id"].as_str().unwrap();

    // Verify secret isolation: svc:<sid> and job:<id> must not contain token, request_secret, or aes_key_hex.
    let svc_raw = p
        .adapter
        .record_snapshot("svc:fixtureService")
        .unwrap()
        .value
        .to_string();
    let job_raw = p
        .adapter
        .record_snapshot(&format!("job:{job_id}"))
        .unwrap()
        .value
        .to_string();
    let req_secret = p
        .adapter
        .secret("scoring_request_secret.fixtureService")
        .unwrap()
        .unwrap();
    let aes_key = p
        .adapter
        .config_store_value(keys_store_id, "current_key_hex")
        .unwrap();
    assert!(!aes_key.is_empty());
    assert!(!req_secret.is_empty());
    for secret_val in ["fixture-token", req_secret.as_str(), aes_key.as_str()] {
        assert!(!svc_raw.contains(secret_val), "svc record leaked secret");
        assert!(!job_raw.contains(secret_val), "job record leaked secret");
    }
    // aes_key_hex must never be in Cloud's Secret Store.
    assert!(p
        .adapter
        .secret("aes_key_hex.fixtureService")
        .unwrap()
        .is_none());

    // Verify config store defaults and initial matrix in KV.
    assert_eq!(
        p.adapter
            .config_store_value(cfg_store_id, "debug_logging_enabled")
            .as_deref(),
        Some("0")
    );
    assert!(p
        .adapter
        .config_store_value(cfg_store_id, "scoring_enabled_at")
        .is_some());
    let kv_bytes = p.adapter.kv_store_value(matrix_store_id, "matrix").unwrap();
    assert!(kv_bytes.starts_with(b"FSM1\x01"));

    // Rotate key shifts current -> previous.
    p.ok(
        "POST",
        "/api/services/fixtureService/scoring/rotate-key?confirm=true",
        json!({}),
    );
    let new_aes = p
        .adapter
        .config_store_value(keys_store_id, "current_key_hex")
        .unwrap();
    let prev_aes = p
        .adapter
        .config_store_value(keys_store_id, "previous_key_hex")
        .unwrap();
    assert_ne!(new_aes, aes_key);
    assert_eq!(prev_aes, aes_key);

    // Threshold + enforce-threshold + l2-enforce + exclude-regex + enforce-status-code.
    p.ok(
        "PUT",
        "/api/services/fixtureService/scoring/threshold",
        json!({"threshold": 65}),
    );
    p.ok(
        "PUT",
        "/api/services/fixtureService/scoring/enforce-threshold?confirm=true",
        json!({"enforce_threshold": 80}),
    );
    let l2 = p.ok(
        "PUT",
        "/api/services/fixtureService/scoring/l2-enforce?confirm=true",
        json!({"enabled": true}),
    );
    assert_eq!(l2["enabled"], true);

    let val = p.ok(
        "POST",
        "/api/services/fixtureService/scoring/exclude-regex/validate",
        json!({"exclude_url_regex": r"\.(png|jpg)$"}),
    );
    assert_eq!(val["valid"], true);
    p.ok(
        "PUT",
        "/api/services/fixtureService/scoring/exclude-regex?confirm=true",
        json!({"exclude_url_regex": r"\.(png|jpg)$"}),
    );
    p.ok(
        "PUT",
        "/api/services/fixtureService/scoring/enforce-status-code?confirm=true",
        json!({"status_code": 403}),
    );

    // Retrain writes FOS + KV FSM1 binary.
    let retrain = p.ok(
        "POST",
        "/api/services/fixtureService/scoring/retrain",
        json!({
            "matrix": {
                "version": "2026-11-18-000001",
                "vocab_size": 2,
                "session_count": 3,
                "transition_count": 5,
                "counts": {"/": {"/products/*": 5}},
                "row_totals": {"/": 5},
                "categories": {"/": "home", "/products/*": "product"},
                "anchors": []
            },
            "stats": {"sessions_kept": 3, "transitions": 5},
            "auc_against_labels": null
        }),
    );
    assert_eq!(retrain["ok"], true);
    assert_eq!(retrain["version"], "2026-11-18-000001");
    let updated_kv = p.adapter.kv_store_value(matrix_store_id, "matrix").unwrap();
    assert!(updated_kv.starts_with(b"FSM1\x01"));
    assert!(updated_kv.windows(17).any(|w| w == b"2026-11-18-000001"));

    // Labels CRUD + matrix versions + audit.
    let lbl = p.ok(
        "POST",
        "/api/services/fixtureService/scoring/labels",
        json!({"edge_sid": "abcdef123456", "label": "bad", "notes": "scraper"}),
    );
    let lbl_id = lbl["label"]["id"].as_str().unwrap();
    let list = p.ok(
        "GET",
        "/api/services/fixtureService/scoring/labels",
        json!({}),
    );
    assert_eq!(list["counts"]["bad"], 1);
    p.ok(
        "DELETE",
        &format!("/api/services/fixtureService/scoring/labels/{lbl_id}"),
        json!({}),
    );
    let versions = p.ok(
        "GET",
        "/api/services/fixtureService/scoring/matrix/versions",
        json!({}),
    );
    assert_eq!(versions["versions"].as_array().unwrap().len(), 1);
    let audit = p.ok(
        "GET",
        "/api/services/fixtureService/scoring/audit",
        json!({}),
    );
    assert!(!audit["entries"].as_array().unwrap().is_empty());

    assert!(p.adapter.has_service(scorer_sid));
}

#[test]
fn scoring_disable_aborts_before_deleting_scorer_when_vcl_strip_fails() {
    let p = Platform::new();
    p.provision_logging_service();
    let en = p.ok(
        "POST",
        "/api/services/fixtureService/scoring/enable",
        json!({}),
    );
    let en_done = p.run_job(en["job"].clone());
    assert_eq!(en_done["status"], "complete");
    let scorer_sid = p.ok(
        "GET",
        "/api/services/fixtureService/scoring/status",
        json!({}),
    )["scoring"]["scoring_service_id"]
        .as_str()
        .unwrap()
        .to_owned();

    // Inject logging draft validation failure during strip_logging_vcl.
    p.adapter.invalid_draft(true);
    let dis = p.ok(
        "POST",
        "/api/services/fixtureService/scoring/disable",
        json!({}),
    );
    let dis_done = p.run_job(dis["job"].clone());
    assert_eq!(dis_done["status"], "failed");

    // Zero DELETE /service/{scorer_id} or store delete calls must have been issued.
    let calls = p.adapter.calls();
    assert!(
        !calls
            .iter()
            .any(|(m, path)| m == "DELETE" && path == &format!("/service/{scorer_sid}")),
        "scorer service was deleted despite VCL strip failure"
    );
    assert!(p.adapter.has_service(&scorer_sid));
}

#[test]
fn full_teardown_strips_scoring_vcl_and_deletes_scorer_before_logging_teardown() {
    let p = Platform::new();
    p.provision_logging_service();
    let en = p.ok(
        "POST",
        "/api/services/fixtureService/scoring/enable",
        json!({}),
    );
    assert_eq!(p.run_job(en["job"].clone())["status"], "complete");

    let status = p.ok(
        "GET",
        "/api/services/fixtureService/scoring/status",
        json!({}),
    );
    let scorer_sid = status["scoring"]["scoring_service_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let gen = status["generation"].as_str().unwrap().to_owned();

    // Call /api/provision/teardown WITHOUT passing token (uses brokered fastly_token_ref).
    let before_teardown = p.adapter.calls().len();
    let td = p.ok(
        "POST",
        "/api/provision/teardown",
        json!({
            "service_id": "fixtureService",
            "confirm_service_id": "fixtureService",
            "generation": gen,
        }),
    );
    let td_done = p.run_job(td["job"].clone());
    assert_eq!(td_done["status"], "complete", "{td_done}");
    assert!(!p.adapter.has_service(&scorer_sid));

    // Verify DELETE /service/{scorer_sid} happened before DELETE /resources/object-storage/access-keys.
    let all_calls = p.adapter.calls();
    let calls = &all_calls[before_teardown..];
    let scorer_del_idx = calls
        .iter()
        .position(|(m, path)| m == "DELETE" && path == &format!("/service/{scorer_sid}"))
        .expect("scorer delete call");
    let key_del_idx = calls
        .iter()
        .position(|(m, path)| {
            m == "DELETE" && path.starts_with("/resources/object-storage/access-keys/")
        })
        .expect("fos key delete call");
    assert!(
        scorer_del_idx < key_del_idx,
        "scorer must be deleted before FOS access keys"
    );
}

#[test]
fn sec21_scoring_validators_backend_tls_and_cookie_scrub_parity() {
    for bad_regex in [
        "^/api/%00admin",
        "^/api/%u0000admin",
        "^/api/%U0000admin",
        "^/api/%U{000}admin",
        "^/api/\u{2028}admin",
        "^/api/\u{2029}admin",
        "^/api/admin\n",
    ] {
        assert!(
            admin::scoring::validate_exclude_url_regex(bad_regex).is_err(),
            "expected validate_exclude_url_regex to reject {bad_regex:?}"
        );
        assert!(
            admin::scoring::validate_vcl_literal(bad_regex, true).is_err(),
            "expected validate_vcl_literal to reject {bad_regex:?}"
        );
    }
    assert_eq!(
        admin::scoring::validate_exclude_url_regex("^/quote%22path$").unwrap(),
        "^/quote%22path$"
    );

    let backend = admin::scoring::desired_scoring_backend("scorer.edgecompute.app");
    assert_eq!(backend["ssl_check_cert"], true);
    assert_eq!(backend["use_ssl"], true);
    assert_eq!(backend["ssl_cert_hostname"], "scorer.edgecompute.app");
    assert_eq!(backend["ssl_sni_hostname"], "scorer.edgecompute.app");

    assert!(admin::scoring::pass_snippet("bad\"; unset req.url; //", "secret").is_err());
    let pass_vcl = admin::scoring::pass_snippet("fixtureService", "secret-token").unwrap();
    assert!(
        pass_vcl.contains("unset bereq.http.Cookie;"),
        "pass_snippet must scrub customer cookies: {pass_vcl}"
    );
    assert!(
        pass_vcl.contains("subfield(req.http.Cookie, \"X-Session-State\", \";\")")
            && pass_vcl.contains("subfield(req.http.Cookie, \"_ess\", \";\")"),
        "pass_snippet must preserve allowlisted scorer cookies: {pass_vcl}"
    );
}

#[test]
fn sec21_active_scoring_service_patch_round_trips_canonical_fields_and_blocks_spoof() {
    let p = Platform::new();
    p.provision_logging_service();
    let en = p.ok(
        "POST",
        "/api/services/fixtureService/scoring/enable",
        json!({}),
    );
    assert_eq!(p.run_job(en["job"].clone())["status"], "complete");

    let svc_get = p.ok("GET", "/api/admin/services/fixtureService", json!({}));
    let initial_schema = fastly_log_analytics::ingest::convert::RequestSchema::for_fields(
        &svc_get["service"]["log_fields"],
    );
    let initial_arrow = initial_schema.arrow();
    for edge_col in admin::scoring::SCORING_FIELD_NAMES {
        assert!(
            initial_arrow.field_with_name(edge_col).is_ok(),
            "expected {edge_col} in RequestSchema::arrow() immediately after scoring/enable"
        );
        let custom_col = initial_schema
            .custom()
            .iter()
            .find(|c| c.name == *edge_col)
            .unwrap_or_else(|| panic!("missing CustomColumn for {edge_col}"));
        assert!(
            !custom_col.slot_only,
            "expected {edge_col} to have slot_only=false after scoring/enable"
        );
    }

    let gen = svc_get["generation"].as_str().unwrap().to_owned();
    let mut lf = svc_get["service"]["log_fields"].clone();
    lf["custom_fields"].as_array_mut().unwrap().push(json!({
        "name": "lt_run_id",
        "label": "LT Run ID",
        "vcl_log_expression": "req.http.X-LT-Run-Id",
        "collection_stage": "edge",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 36,
        "enabled": true
    }));

    let patched = p.ok(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({
            "generation": gen,
            "custom_condition": "req.url.ext != \"ico\"",
            "log_fields": lf
        }),
    );
    let next_gen = patched["generation"].as_str().unwrap().to_owned();

    let mut spoofed_lf = patched["service"]["log_fields"].clone();
    for cf in spoofed_lf["custom_fields"].as_array_mut().unwrap() {
        if cf["name"] == "edge_score" {
            cf["vcl_log_expression"] = json!("req.http.X-Spoofed-Score");
        }
    }
    let (bad_status, bad_body) = p.call(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({
            "generation": next_gen,
            "log_fields": spoofed_lf
        }),
    );
    assert_eq!(bad_status, 400);
    assert_eq!(bad_body["error"], "invalid_custom_field");

    // Shadow-key ("vcl" / "parquet_type" / "system_managed") & type/byte_limit/optional-field spoofing
    let base_lf = patched["service"]["log_fields"].clone();
    for (label, expected_err, patch_fn) in [
        (
            "shadow_vcl",
            "invalid_custom_field",
            &(|cf: &mut Value| {
                cf["vcl"] = json!("req.http.X-Spoofed-Score");
            }) as &dyn Fn(&mut Value),
        ),
        (
            "shadow_parquet_type",
            "invalid_custom_field",
            &|cf: &mut Value| {
                cf["parquet_type"] = json!("VARCHAR");
            },
        ),
        (
            "mismatched_duckdb_type",
            "invalid_custom_field",
            &|cf: &mut Value| {
                cf["duckdb_type"] = json!("BOOLEAN");
            },
        ),
        (
            "negative_byte_limit",
            "invalid_custom_field",
            &|cf: &mut Value| {
                cf["byte_limit"] = json!(-5);
            },
        ),
        (
            "boolean_byte_limit",
            "invalid_custom_field",
            &|cf: &mut Value| {
                cf["byte_limit"] = json!(true);
            },
        ),
        (
            "string_byte_limit",
            "invalid_custom_field",
            &|cf: &mut Value| {
                cf["byte_limit"] = json!("64");
            },
        ),
        (
            "system_managed_false",
            "invalid_custom_field",
            &|cf: &mut Value| {
                cf["system_managed"] = json!(false);
            },
        ),
        (
            "system_managed_true",
            "system_field_locked",
            &|cf: &mut Value| {
                cf["system_managed"] = json!(true);
            },
        ),
        (
            "control_char_label",
            "invalid_custom_field",
            &|cf: &mut Value| {
                cf["label"] = json!("Edge\nScore");
            },
        ),
    ] {
        let mut candidate_lf = base_lf.clone();
        for cf in candidate_lf["custom_fields"].as_array_mut().unwrap() {
            if cf["name"] == "edge_score" {
                patch_fn(cf);
            }
        }
        let (st_patch, body_patch) = p.call(
            "PATCH",
            "/api/admin/services/fixtureService",
            json!({
                "generation": next_gen,
                "log_fields": candidate_lf
            }),
        );
        assert_eq!(
            st_patch, 400,
            "expected PATCH 400 for {label}, got {st_patch}: {body_patch}"
        );
        assert_eq!(body_patch["error"], expected_err, "{label}");

        let (st_prev, body_prev) = p.call(
            "POST",
            "/api/admin/log-fields/preview",
            json!({
                "scoring": {"enabled": true},
                "log_fields": candidate_lf
            }),
        );
        assert_eq!(
            st_prev, 400,
            "expected preview 400 for {label}, got {st_prev}: {body_prev}"
        );
        assert_eq!(body_prev["error"], expected_err, "{label} preview");
    }

    // Duplicate edge_score entries must return 400 invalid_custom_field
    let mut dup_lf = base_lf.clone();
    let dup_edge = dup_lf["custom_fields"]
        .as_array()
        .unwrap()
        .iter()
        .find(|cf| cf["name"] == "edge_score")
        .unwrap()
        .clone();
    dup_lf["custom_fields"]
        .as_array_mut()
        .unwrap()
        .push(dup_edge);
    let (dup_status, dup_body) = p.call(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({
            "generation": next_gen,
            "log_fields": dup_lf
        }),
    );
    assert_eq!(dup_status, 400);
    assert_eq!(dup_body["error"], "invalid_custom_field");

    // Forged committed=true before ingest commits must return 400 committed_flag_reserved
    let mut forged_committed_lf = base_lf.clone();
    for cf in forged_committed_lf["custom_fields"].as_array_mut().unwrap() {
        if cf["name"] == "edge_score" {
            cf["committed"] = json!(true);
        }
    }
    let (forged_patch_st, forged_patch_body) = p.call(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({
            "generation": next_gen,
            "log_fields": forged_committed_lf
        }),
    );
    assert_eq!(forged_patch_st, 400);
    assert_eq!(forged_patch_body["error"], "committed_flag_reserved");

    let (forged_prev_st, forged_prev_body) = p.call(
        "POST",
        "/api/admin/log-fields/preview",
        json!({
            "scoring": {"enabled": true},
            "log_fields": forged_committed_lf
        }),
    );
    assert_eq!(forged_prev_st, 400);
    assert_eq!(forged_prev_body["error"], "committed_flag_reserved");

    // Once KV svc:fixtureService marks edge_score committed=true, canonical PATCH with committed=true
    // succeeds (200 OK) while omitting edge_score returns 409 committed_field_locked.
    let rec = p.admin_get("svc:fixtureService").unwrap().unwrap();
    let mut stored_svc = rec.value;
    for cf in stored_svc["log_fields"]["custom_fields"]
        .as_array_mut()
        .unwrap()
    {
        if cf["name"] == "edge_score" {
            cf["committed"] = json!(true);
        }
    }
    assert!(p
        .admin_cas("svc:fixtureService", Some(&rec.generation), &stored_svc)
        .unwrap());

    let committed_get = p.ok("GET", "/api/admin/services/fixtureService", json!({}));
    let pre_committed_schema = fastly_log_analytics::ingest::convert::RequestSchema::for_fields(
        &committed_get["service"]["log_fields"],
    );
    assert!(pre_committed_schema
        .arrow()
        .field_with_name("edge_score")
        .is_ok());
    let expected_schema_hash = pre_committed_schema.hash().to_owned();

    let committed_gen = committed_get["generation"].as_str().unwrap().to_owned();
    let mut committed_lf = committed_get["service"]["log_fields"].clone();
    for cf in committed_lf["custom_fields"].as_array_mut().unwrap() {
        if cf["name"] == "edge_score" {
            cf["duckdb_type"] = json!("INTEGER");
            cf["byte_limit"] = json!(64);
        }
    }
    let committed_ok = p.ok(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({
            "generation": committed_gen,
            "log_fields": committed_lf
        }),
    );
    let with_duckdb_schema = fastly_log_analytics::ingest::convert::RequestSchema::for_fields(
        &committed_ok["service"]["log_fields"],
    );
    assert!(with_duckdb_schema
        .arrow()
        .field_with_name("edge_score")
        .is_ok());
    assert_eq!(with_duckdb_schema.hash(), expected_schema_hash);
    let with_duckdb_gen = committed_ok["generation"].as_str().unwrap().to_owned();

    // Round-trip omitting duckdb_type (sending canonical sql_type + committed=true) must preserve
    // edge_score in RequestSchema::arrow() and keep RequestSchema::hash() invariant (8B-R).
    let mut omit_duckdb_lf = committed_ok["service"]["log_fields"].clone();
    for cf in omit_duckdb_lf["custom_fields"].as_array_mut().unwrap() {
        if cf["name"] == "edge_score" {
            cf.as_object_mut().unwrap().remove("duckdb_type");
            cf["sql_type"] = json!("INTEGER");
            cf["committed"] = json!(true);
        }
    }
    let omit_duckdb_ok = p.ok(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({
            "generation": with_duckdb_gen,
            "log_fields": omit_duckdb_lf
        }),
    );
    let omit_duckdb_schema = fastly_log_analytics::ingest::convert::RequestSchema::for_fields(
        &omit_duckdb_ok["service"]["log_fields"],
    );
    assert!(
        omit_duckdb_schema
            .arrow()
            .field_with_name("edge_score")
            .is_ok(),
        "committed PATCH omitting duckdb_type must not drop edge_score Arrow column"
    );
    assert_eq!(
        omit_duckdb_schema.hash(),
        expected_schema_hash,
        "committed PATCH omitting duckdb_type must preserve RequestSchema::hash() (8B-R)"
    );
    let post_committed_gen = omit_duckdb_ok["generation"].as_str().unwrap().to_owned();

    let mut dropped_lf = omit_duckdb_ok["service"]["log_fields"].clone();
    dropped_lf["custom_fields"]
        .as_array_mut()
        .unwrap()
        .retain(|cf| cf["name"] != "edge_score");
    let (drop_st, drop_body) = p.call(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({
            "generation": post_committed_gen,
            "log_fields": dropped_lf
        }),
    );
    assert_eq!(drop_st, 409);
    assert_eq!(drop_body["error"], "committed_field_locked");
}
