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
