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
    cell::{Cell, RefCell},
    collections::{BTreeMap, BTreeSet},
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
};

thread_local! {
    static REQ_CALLS: Cell<u32> = const { Cell::new(0) };
    static REQ_SECRETS: RefCell<BTreeSet<String>> = const { RefCell::new(BTreeSet::new()) };
}

fn charge_backend() -> Result<(), String> {
    REQ_CALLS.with(|calls| {
        if calls.get() >= 32 {
            return Err("backend_budget_exhausted".into());
        }
        calls.set(calls.get() + 1);
        Ok(())
    })
}

struct Platform {
    adapter: FixtureAdapter,
    clock: AtomicU64,
    random_counter: AtomicU64,
    refuse_scorer_teardown: AtomicBool,
    fail_svc_cas: AtomicBool,
}

impl Platform {
    fn new() -> Self {
        Self {
            adapter: FixtureAdapter::new(),
            clock: AtomicU64::new(1_800_000_000),
            random_counter: AtomicU64::new(1),
            refuse_scorer_teardown: AtomicBool::new(false),
            fail_svc_cas: AtomicBool::new(false),
        }
    }

    fn call(&self, method: &str, path: &str, body: Value) -> (u16, Value) {
        self.call_with_initial_used(0, method, path, body)
    }

    fn call_with_initial_used(
        &self,
        initial_used: u32,
        method: &str,
        path: &str,
        body: Value,
    ) -> (u16, Value) {
        REQ_CALLS.with(|c| c.set(initial_used));
        REQ_SECRETS.with(|s| s.borrow_mut().clear());
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
            assert_eq!(
                status, 200,
                "POST /api/admin/jobs/{id}/step must not emit 503 or error -> {status}: {res}"
            );
            let lease_rec = self
                .adapter
                .record_snapshot("reconcile_lock:fixtureService");
            assert_eq!(
                lease_rec
                    .as_ref()
                    .and_then(|r| r.value["expires_at"].as_u64())
                    .unwrap_or(0),
                0,
                "reconcile_lock:fixtureService must be released after every step: {:?}",
                lease_rec.as_ref().map(|r| &r.value)
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
        let fresh = REQ_SECRETS.with(|s| s.borrow_mut().insert(name.into()));
        if fresh {
            charge_backend()?;
        }
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
        if let Some(cached) = self.adapter.cached_get(key) {
            return Ok(cached);
        }
        charge_backend()?;
        self.adapter.get(key)
    }
    fn admin_get_fresh(&self, key: &str, _: &str) -> Result<Option<Record>, String> {
        charge_backend()?;
        self.adapter.confirm(key)
    }
    fn admin_cas(
        &self,
        key: &str,
        generation: Option<&str>,
        value: &Value,
    ) -> Result<bool, String> {
        self.adapter.cache_invalidate(key);
        charge_backend()?;
        if key == "svc:fixtureService" && self.fail_svc_cas.load(Ordering::SeqCst) {
            return Err("kv_write_injected_failure".into());
        }
        self.adapter.cas(key, generation, value)
    }
    fn admin_api(&self, request: ApiRequest<'_>) -> Result<admin::ApiResponse, String> {
        charge_backend()?;
        if self.refuse_scorer_teardown.load(Ordering::SeqCst)
            && request.path.starts_with("/service/fixtureScorer")
            && (request.method == "DELETE" || request.path.ends_with("/deactivate"))
        {
            return Ok(admin::ApiResponse {
                status: 409,
                body: json!({"msg": "scorer service retained"}),
                retry_after: None,
            });
        }
        self.adapter.api(request).map(Into::into)
    }
    fn admin_api_raw(&self, request: ApiRawRequest<'_>) -> Result<admin::ApiResponse, String> {
        charge_backend()?;
        self.adapter.api_raw(request).map(Into::into)
    }
    fn admin_fos(&self, request: FosRequest<'_>) -> Result<FosResponse, String> {
        charge_backend()?;
        self.adapter.fos(request)
    }
    fn admin_fos_etag(
        &self,
        request: FosRequest<'_>,
    ) -> Result<(FosResponse, Option<String>), String> {
        use md5::{Digest, Md5};
        let etag = (request.method == "PUT")
            .then(|| format!("\"{}\"", hex::encode(Md5::digest(&request.body))));
        let response = self.admin_fos(request)?;
        let etag = etag.filter(|_| (200..300).contains(&response.status));
        Ok((response, etag))
    }
    fn admin_remaining_calls(&self) -> u32 {
        REQ_CALLS.with(|c| 32u32.saturating_sub(c.get()))
    }
    fn kv_list(
        &self,
        prefix: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<fastly_log_analytics::shell::KvPage, String> {
        charge_backend()?;
        Ok(fastly_log_analytics::shell::KvPage::from_sorted_keys(
            self.adapter.records().into_keys(),
            prefix,
            cursor,
            limit,
        ))
    }
    fn kv_delete(&self, key: &str) -> Result<(), String> {
        self.adapter.cache_invalidate(key);
        charge_backend()?;
        self.adapter.delete(key)
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
    let scorer_domain = sc["scoring_domain"].as_str().unwrap();
    let keys_store_id = sc["scoring_keys_store_id"].as_str().unwrap();
    let cfg_store_id = sc["scoring_config_store_id"].as_str().unwrap();
    let matrix_store_id = sc["scoring_matrix_store_id"].as_str().unwrap();

    // BUG-CLOUD-DR-17: fresh enable must populate both domain and placeholder_origin backend + 3 resource links.
    let scorer_domains = p.adapter.resources(scorer_sid, "domain");
    assert_eq!(
        scorer_domains.len(),
        1,
        "scorer domains: {scorer_domains:?}"
    );
    assert_eq!(scorer_domains[0]["name"], scorer_domain);
    let scorer_backends = p.adapter.resources(scorer_sid, "backend");
    assert_eq!(
        scorer_backends.len(),
        1,
        "scorer backends must include placeholder_origin: {scorer_backends:?}"
    );
    assert_eq!(scorer_backends[0]["name"], "placeholder_origin");
    let scorer_links = p.adapter.resources(scorer_sid, "resource");
    assert_eq!(
        scorer_links.len(),
        3,
        "scorer resource links: {scorer_links:?}"
    );

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

#[test]
fn scoring_retrain_and_restore_validate_matrix_versions_and_protect_index_json() {
    let p = Platform::new();
    p.provision_logging_service();
    let en = p.ok(
        "POST",
        "/api/services/fixtureService/scoring/enable",
        json!({}),
    );
    assert_eq!(p.run_job(en["job"].clone())["status"], "complete");

    let valid_retrain = p.ok(
        "POST",
        "/api/services/fixtureService/scoring/retrain",
        json!({
            "matrix": {
                "version": "2027-01-15-v1",
                "vocab_size": 2,
                "session_count": 4,
                "transition_count": 6,
                "counts": {"/": {"/checkout": 6}},
                "row_totals": {"/": 6},
                "categories": {"/": "home", "/checkout": "checkout"},
                "anchors": []
            },
            "stats": {"sessions_kept": 4, "transitions": 6},
            "auc_against_labels": null
        }),
    );
    assert_eq!(valid_retrain["ok"], true);
    assert_eq!(valid_retrain["version"], "2027-01-15-v1");

    let long_ver = "a".repeat(65);
    for bad_ver in ["index", "..", ".", "", long_ver.as_str(), "v1\0", "a..b"] {
        let (retrain_st, retrain_body) = p.call(
            "POST",
            "/api/services/fixtureService/scoring/retrain",
            json!({
                "matrix": {
                    "version": bad_ver,
                    "vocab_size": 2,
                    "session_count": 1,
                    "transition_count": 1,
                    "counts": {"/": {"/checkout": 1}},
                    "row_totals": {"/": 1}
                }
            }),
        );
        assert_eq!(
            retrain_st, 400,
            "expected retrain 400 for {bad_ver:?}, got {retrain_st}: {retrain_body}"
        );
        assert_eq!(
            retrain_body["error"], "invalid_matrix_version",
            "retrain error mismatch for {bad_ver:?}: {retrain_body}"
        );

        let (restore_st, restore_body) = p.call(
            "POST",
            "/api/services/fixtureService/scoring/matrix/versions/restore?confirm=true",
            json!({"version": bad_ver}),
        );
        assert_eq!(
            restore_st, 400,
            "expected restore 400 for {bad_ver:?}, got {restore_st}: {restore_body}"
        );
        assert_eq!(
            restore_body["error"], "invalid_matrix_version",
            "restore error mismatch for {bad_ver:?}: {restore_body}"
        );
    }

    let versions = p.ok(
        "GET",
        "/api/services/fixtureService/scoring/matrix/versions",
        json!({}),
    );
    let version_list = versions["versions"].as_array().unwrap();
    assert_eq!(
        version_list.len(),
        1,
        "matrices/index.json must stay intact: {versions}"
    );
    assert_eq!(version_list[0]["version"], "2027-01-15-v1");
    assert_eq!(versions["active_version"], "2027-01-15-v1");

    let restored = p.ok(
        "POST",
        "/api/services/fixtureService/scoring/matrix/versions/restore?confirm=true",
        json!({"version": "2027-01-15-v1"}),
    );
    assert_eq!(restored["ok"], true);
    assert_eq!(restored["version"], "2027-01-15-v1");

    // Seed 52 entries (49 valid + "index" + "../escape" + "old-valid-50") in matrices/index.json
    // and verify MAX_MATRIX_HISTORY eviction deletes "old-valid-50" while skipping poisoned names.
    let svc_rec = p.admin_get("svc:fixtureService").unwrap().unwrap().value;
    let ak_ref = svc_rec["fos_access_key_ref"].as_str().unwrap();
    let sk_ref = svc_rec["fos_secret_key_ref"].as_str().unwrap();
    let ak = p.adapter.secret(ak_ref).unwrap().unwrap();
    let sk_env: Value = serde_json::from_str(&p.adapter.secret(sk_ref).unwrap().unwrap()).unwrap();
    let sk = sk_env["secret_key"].as_str().unwrap().to_owned();
    let index_key = p
        .adapter
        .calls()
        .into_iter()
        .find_map(|(m, path)| {
            (m == "PUT" && path.ends_with("/matrices/index.json"))
                .then(|| path.strip_prefix("/fos-fixture-logs/").unwrap().to_owned())
        })
        .expect("retrain must have written matrices/index.json");
    let mut seeded_versions: Vec<Value> = (1..=49)
        .map(|n| json!({"version": format!("2027-01-15-keep-{n:02}"), "vocab_size": 2}))
        .collect();
    seeded_versions.push(json!({"version": "index", "vocab_size": 2}));
    seeded_versions.push(json!({"version": "../escape", "vocab_size": 2}));
    seeded_versions.push(json!({"version": "2027-01-15-evict-ok", "vocab_size": 2}));
    let put_resp = p
        .adapter
        .fos(FosRequest {
            method: "PUT",
            region: "us-east-1",
            bucket: "fos-fixture-logs",
            key: &index_key,
            query: "",
            access_key: &ak,
            secret_key: &sk,
            body: serde_json::to_vec(&json!({"versions": seeded_versions})).unwrap(),
            timeout: std::time::Duration::from_secs(5),
            response_limit: 65_536,
            content_md5: None,
        })
        .unwrap();
    assert_eq!(put_resp.status, 200);

    let calls_before = p.adapter.calls().len();
    p.ok(
        "POST",
        "/api/services/fixtureService/scoring/retrain",
        json!({
            "matrix": {
                "version": "2027-01-15-v2",
                "vocab_size": 2,
                "session_count": 4,
                "transition_count": 6,
                "counts": {"/": {"/checkout": 6}},
                "row_totals": {"/": 6}
            }
        }),
    );
    let all_calls = p.adapter.calls();
    let new_deletes: Vec<&str> = all_calls[calls_before..]
        .iter()
        .filter(|(m, _)| m == "DELETE")
        .map(|(_, path)| path.as_str())
        .collect();
    assert!(
        new_deletes
            .iter()
            .any(|p| p.ends_with("/matrices/2027-01-15-evict-ok.json")),
        "expected valid evicted version to be deleted from FOS, got {new_deletes:?}"
    );
    assert!(
        !new_deletes
            .iter()
            .any(|p| p.ends_with("/matrices/index.json") || p.contains("..")),
        "MAX_MATRIX_HISTORY eviction must never delete index.json or traversal paths: {new_deletes:?}"
    );
}

#[test]
fn bug_cloud_dr_17_stale_kv_locked_reuse_post_activate_retry_and_failed_job_supersede() {
    // 1. Cross-POP stale svc:fixtureService KV cache on Tick 0 (scorer_service) AND on scorer_stores:
    // must complete with 0 HTTP 503s, domains == 1, backends == 1 ("placeholder_origin"), and _job_checkpoint.
    let p = Platform::new();
    p.provision_logging_service();
    let pre_enable_svc = p.adapter.record_snapshot("svc:fixtureService");
    let en = p.ok(
        "POST",
        "/api/services/fixtureService/scoring/enable",
        json!({}),
    );
    let mut job = en["job"].clone();
    let job_id = job["id"].as_str().unwrap().to_owned();
    // Simulate cross-POP stale KV read on Tick 0 (scorer_service):
    p.adapter.cache_stale("svc:fixtureService", pre_enable_svc);

    let mut saw_scorer_stores_stale = false;
    let mut prev_svc = p.adapter.record_snapshot("svc:fixtureService");
    for _ in 0..80 {
        if job["status"] == "complete" || job["status"] == "failed" {
            break;
        }
        if let Some(at) = job["retry_at"].as_u64() {
            p.clock.fetch_max(at, Ordering::SeqCst);
        }
        if job["step"] == "scorer_stores" && !saw_scorer_stores_stale {
            saw_scorer_stores_stale = true;
            p.adapter
                .cache_stale("svc:fixtureService", prev_svc.clone());
        }
        prev_svc = p.adapter.record_snapshot("svc:fixtureService");
        let (st, res) = p.call("POST", &format!("/api/admin/jobs/{job_id}/step"), json!({}));
        assert_eq!(st, 200, "scoring_enable step must not hit 503: {res}");
        job = res["job"].clone();
    }
    assert!(saw_scorer_stores_stale, "must have exercised scorer_stores");
    assert_eq!(
        job["status"], "complete",
        "stale-KV scoring_enable failed: {job}"
    );
    let svc_after = p.adapter.record_snapshot("svc:fixtureService").unwrap();
    assert_eq!(svc_after.value["_job_checkpoint"]["job_id"], job_id);
    let scorer_sid = svc_after.value["scoring"]["scoring_service_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(p.adapter.resources(&scorer_sid, "domain").len(), 1);
    let backends = p.adapter.resources(&scorer_sid, "backend");
    assert_eq!(backends.len(), 1);
    assert_eq!(backends[0]["name"], "placeholder_origin");
    assert_eq!(p.adapter.resources(&scorer_sid, "resource").len(), 3);

    // 2. Bulk MAX_MATRIX_HISTORY (65 seeded entries -> 16 evictions) stays within 24-call Context budget:
    let ak_ref = svc_after.value["fos_access_key_ref"].as_str().unwrap();
    let sk_ref = svc_after.value["fos_secret_key_ref"].as_str().unwrap();
    let ak = p.adapter.secret(ak_ref).unwrap().unwrap();
    let sk_env: Value = serde_json::from_str(&p.adapter.secret(sk_ref).unwrap().unwrap()).unwrap();
    let sk = sk_env["secret_key"].as_str().unwrap().to_owned();
    let bulk_versions: Vec<Value> = (1..=65)
        .map(|n| json!({"version": format!("2027-02-01-bulk-{n:03}"), "vocab_size": 2}))
        .collect();
    assert_eq!(
        p.adapter
            .fos(FosRequest {
                method: "PUT",
                region: "us-east-1",
                bucket: "fos-fixture-logs",
                key: "processed/scoring/matrices/index.json",
                query: "",
                access_key: &ak,
                secret_key: &sk,
                body: serde_json::to_vec(&json!({"versions": bulk_versions})).unwrap(),
                timeout: std::time::Duration::from_secs(5),
                response_limit: 65_536,
                content_md5: None,
            })
            .unwrap()
            .status,
        200
    );
    let bulk_retrain = p.ok(
        "POST",
        "/api/services/fixtureService/scoring/retrain",
        json!({
            "matrix": {
                "version": "2027-02-01-v66",
                "vocab_size": 2,
                "session_count": 3,
                "transition_count": 4,
                "counts": {"/": {"/checkout": 4}},
                "row_totals": {"/": 4}
            }
        }),
    );
    assert_eq!(bulk_retrain["ok"], true);
    let capped_versions = p.ok(
        "GET",
        "/api/services/fixtureService/scoring/matrix/versions",
        json!({}),
    );
    assert_eq!(capped_versions["versions"].as_array().unwrap().len(), 50);

    // 3. Disable scoring while DELETE /service/{scorer_sid} returns 409 (preserving active/locked v1 scorer service),
    // then re-enable scoring: must reuse locked-v1 scorer service cleanly without scorer_resource_link_failed,
    // and link all 3 resources onto cloned v2!
    p.refuse_scorer_teardown.store(true, Ordering::SeqCst);
    let dis = p.ok(
        "POST",
        "/api/services/fixtureService/scoring/disable",
        json!({}),
    );
    let dis_done = p.run_job(dis["job"].clone());
    p.refuse_scorer_teardown.store(false, Ordering::SeqCst);
    assert_eq!(dis_done["status"], "complete", "{dis_done}");
    assert!(
        p.adapter.has_service(&scorer_sid),
        "scorer service must still exist with locked v1"
    );
    // Remove stores so reuse must create new stores and link all 3 onto cloned v2!
    let prev_keys = svc_after.value["scoring"]["scoring_keys_store_id"]
        .as_str()
        .unwrap();
    let prev_cfg = svc_after.value["scoring"]["scoring_config_store_id"]
        .as_str()
        .unwrap();
    let prev_mat = svc_after.value["scoring"]["scoring_matrix_store_id"]
        .as_str()
        .unwrap();
    assert!(p
        .adapter
        .config_store_value(prev_keys, "current_key_hex")
        .is_none());
    assert!(p
        .adapter
        .config_store_value(prev_cfg, "debug_logging_enabled")
        .is_none());
    assert!(p.adapter.kv_store_value(prev_mat, "matrix").is_none());

    let re_en = p.ok(
        "POST",
        "/api/services/fixtureService/scoring/enable",
        json!({}),
    );
    let re_done = p.run_job(re_en["job"].clone());
    assert_eq!(
        re_done["status"], "complete",
        "reusing locked-v1 scorer service must succeed: {re_done}"
    );
    let re_status = p.ok(
        "GET",
        "/api/services/fixtureService/scoring/status",
        json!({}),
    );
    assert_eq!(re_status["scoring"]["scoring_service_id"], scorer_sid);
    let re_svc = p.adapter.record_snapshot("svc:fixtureService").unwrap();
    let new_keys = re_svc.value["scoring"]["scoring_keys_store_id"]
        .as_str()
        .unwrap();
    let new_cfg = re_svc.value["scoring"]["scoring_config_store_id"]
        .as_str()
        .unwrap();
    let new_mat = re_svc.value["scoring"]["scoring_matrix_store_id"]
        .as_str()
        .unwrap();
    assert_ne!(new_keys, prev_keys);
    assert_ne!(new_cfg, prev_cfg);
    assert_ne!(new_mat, prev_mat);
    let v2_links = p.adapter.resources(&scorer_sid, "resource");
    let link_pairs: BTreeSet<(&str, &str)> = v2_links
        .iter()
        .filter_map(|r| Some((r["name"].as_str()?, r["resource_id"].as_str()?)))
        .collect();
    assert_eq!(
        link_pairs,
        BTreeSet::from([
            ("scoring_keys", new_keys),
            ("scoring_config", new_cfg),
            ("scoring_matrix", new_mat),
        ]),
        "cloned scorer version must link all 3 newly recreated store IDs: {v2_links:?}"
    );

    // 4. Post-activate transient FOS failure on scorer_deploy + manual retry:
    // must NOT leak extra cloned versions or wedge with service_changed_rerun!
    let p2 = Platform::new();
    p2.provision_logging_service();
    let en2 = p2.ok(
        "POST",
        "/api/services/fixtureService/scoring/enable",
        json!({}),
    );
    let mut job2 = en2["job"].clone();
    let job2_id = job2["id"].as_str().unwrap().to_owned();
    let mut injected_fos_fail = false;
    for _ in 0..80 {
        if job2["status"] == "complete" || job2["status"] == "failed" {
            break;
        }
        if let Some(at) = job2["retry_at"].as_u64() {
            p2.clock.fetch_max(at, Ordering::SeqCst);
        }
        if job2["step"] == "scorer_deploy" && !injected_fos_fail {
            // Inject transient FOS failure on matrix_latest.json (runs only after activate)
            p2.adapter.fail_next(
                "GET",
                "/fos-fixture-logs/processed/scoring/matrix_latest.json",
                false,
            );
            injected_fos_fail = true;
        }
        let (st, res) = p2.call(
            "POST",
            &format!("/api/admin/jobs/{job2_id}/step"),
            json!({}),
        );
        assert_eq!(st, 200, "step returned {st}: {res}");
        job2 = res["job"].clone();
    }
    assert_eq!(job2["status"], "failed");
    assert_eq!(job2["error"], "object_storage_transport_failed");
    let scorer2_sid = p2
        .adapter
        .record_snapshot("svc:fixtureService")
        .unwrap()
        .value["scoring"]["scoring_service_id"]
        .as_str()
        .unwrap()
        .to_owned();

    // Retry failed job2 and drive to completion:
    p2.clock.fetch_add(65, Ordering::SeqCst);
    let retried = p2.ok(
        "POST",
        &format!("/api/admin/jobs/{job2_id}/retry"),
        json!({}),
    )["job"]
        .clone();
    let retried_done = p2.run_job(retried);
    assert_eq!(
        retried_done["status"], "complete",
        "post-activate retry must complete without service_changed_rerun: {retried_done}"
    );
    let (_, vers_val) = p2
        .adapter
        .api(ApiRequest {
            method: "GET",
            path: &format!("/service/{scorer2_sid}/version"),
            query: "",
            token: "fixture-token",
            body: None,
            timeout: std::time::Duration::from_secs(5),
        })
        .unwrap();
    assert_eq!(
        vers_val.as_array().unwrap().len(),
        1,
        "post-activate FOS retry must not leak extra cloned scorer versions: {vers_val}"
    );

    // 5. Mid-job GCE takeover right before scorer_deploy (even on locked-v1 reuse) still fails closed with
    // managed_resources_not_owned, AND failed scoring jobs can be superseded by PATCH / reconcile / enable!
    p2.refuse_scorer_teardown.store(true, Ordering::SeqCst);
    let dis2 = p2.ok(
        "POST",
        "/api/services/fixtureService/scoring/disable",
        json!({}),
    );
    let dis2_done = p2.run_job(dis2["job"].clone());
    p2.refuse_scorer_teardown.store(false, Ordering::SeqCst);
    assert_eq!(dis2_done["status"], "complete");

    let en3 = p2.ok(
        "POST",
        "/api/services/fixtureService/scoring/enable",
        json!({}),
    );
    let mut job3 = en3["job"].clone();
    let job3_id = job3["id"].as_str().unwrap().to_owned();
    let mut injected_takeover = false;
    for _ in 0..80 {
        if job3["status"] == "complete" || job3["status"] == "failed" {
            break;
        }
        if job3["step"] == "scorer_deploy" && !injected_takeover {
            p2.adapter.seed_resource(
                "fixtureService",
                "logging/fos",
                json!({
                    "name": "Fastly Object Storage Logs",
                    "bucket_name": "fos-gce-logs",
                    "region": "us-east-1",
                    "path": "raw/"
                }),
            );
            injected_takeover = true;
        }
        let (st, res) = p2.call(
            "POST",
            &format!("/api/admin/jobs/{job3_id}/step"),
            json!({}),
        );
        assert_eq!(st, 200);
        job3 = res["job"].clone();
    }
    assert_eq!(job3["status"], "failed");
    assert_eq!(job3["error"], "managed_resources_not_owned");

    // Clean up foreign endpoint and verify failed scoring_enable does NOT permanently brick PATCH / enable:
    p2.adapter.remove_resource(
        "fixtureService",
        "logging/fos",
        "Fastly Object Storage Logs",
    );
    let cur_svc = p2.ok("GET", "/api/admin/services/fixtureService", json!({}));
    let patched = p2.ok(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({
            "generation": cur_svc["generation"],
            "sample_rate": 50
        }),
    );
    assert_eq!(patched["service"]["sample_rate"], 50);
    let en4 = p2.ok(
        "POST",
        "/api/services/fixtureService/scoring/enable",
        json!({}),
    );
    assert_eq!(p2.run_job(en4["job"].clone())["status"], "complete");
}

#[test]
fn scoring_enable_rollback_retry_rewinds_to_logging_reconcile() {
    let p = Platform::new();
    p.provision_logging_service();
    let en = p.ok(
        "POST",
        "/api/services/fixtureService/scoring/enable",
        json!({}),
    );
    let mut job = en["job"].clone();
    let job_id = job["id"].as_str().unwrap().to_owned();
    let mut injected_validate_fail = false;
    for _ in 0..80 {
        if job["status"] == "complete" || job["status"] == "failed" {
            break;
        }
        if let Some(at) = job["retry_at"].as_u64() {
            p.clock.fetch_max(at, Ordering::SeqCst);
        }
        if job["step"] == "validate" && !injected_validate_fail {
            p.adapter.invalid_draft(true);
            injected_validate_fail = true;
        }
        let (st, res) = p.call("POST", &format!("/api/admin/jobs/{job_id}/step"), json!({}));
        assert_eq!(st, 200, "step returned {st}: {res}");
        job = res["job"].clone();
    }
    p.adapter.invalid_draft(false);
    assert!(injected_validate_fail, "must reach customer VCL validate");
    assert_eq!(job["status"], "failed", "{job}");
    assert_eq!(job["error"], "draft_validation_failed", "{job}");

    // Rollback must have reverted enabled=false and stripped SCORING_FIELD_NAMES while failed:
    let rolled_back_svc = p.adapter.record_snapshot("svc:fixtureService").unwrap();
    assert_eq!(rolled_back_svc.value["scoring"]["enabled"], false);
    assert_eq!(
        rolled_back_svc.value["log_fields"]["custom_fields"]
            .as_array()
            .unwrap()
            .len(),
        0
    );

    // Retry must rewind to logging_reconcile, re-enable scoring + all 9 custom fields,
    // activate customer scoring VCL, and allow clean scoring/disable!
    p.clock.fetch_add(65, Ordering::SeqCst);
    let retried = p.ok(
        "POST",
        &format!("/api/admin/jobs/{job_id}/retry"),
        json!({}),
    )["job"]
        .clone();
    let done = p.run_job(retried);
    assert_eq!(done["status"], "complete", "{done}");

    let status_after = p.ok(
        "GET",
        "/api/services/fixtureService/scoring/status",
        json!({}),
    );
    assert_eq!(status_after["scoring"]["enabled"], true, "{status_after}");
    let svc_after = p.adapter.record_snapshot("svc:fixtureService").unwrap();
    let custom_names: BTreeSet<&str> = svc_after.value["log_fields"]["custom_fields"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|f| f["name"].as_str())
        .collect();
    assert_eq!(
        custom_names,
        BTreeSet::from([
            "edge_cookie_compliance",
            "edge_matrix_version",
            "edge_score",
            "edge_score_exec_us",
            "edge_score_l1",
            "edge_score_l2",
            "edge_score_reason",
            "edge_score_rtt_us",
            "edge_sid",
        ]),
        "all 9 scoring custom fields must be restored on retry: {custom_names:?}"
    );
    let active_snippets = p.adapter.resources("fixtureService", "snippet");
    assert!(
        active_snippets
            .iter()
            .any(|s| s["name"] == "Session Scoring - Recv"),
        "live customer VCL must include Session Scoring snippets"
    );

    let dis = p.ok(
        "POST",
        "/api/services/fixtureService/scoring/disable",
        json!({}),
    );
    let dis_done = p.run_job(dis["job"].clone());
    assert_eq!(dis_done["status"], "complete", "{dis_done}");
}

#[test]
fn scoring_cleanup_state_deletes_multi_batch_matrices_without_livelock() {
    let seed_matrices = |p: &Platform, count: usize, with_labels: bool| {
        let svc = p.adapter.record_snapshot("svc:fixtureService").unwrap();
        let ak_ref = svc.value["fos_access_key_ref"].as_str().unwrap();
        let sk_ref = svc.value["fos_secret_key_ref"].as_str().unwrap();
        let ak = p.adapter.secret(ak_ref).unwrap().unwrap();
        let sk_env: Value =
            serde_json::from_str(&p.adapter.secret(sk_ref).unwrap().unwrap()).unwrap();
        let sk = sk_env["secret_key"].as_str().unwrap().to_owned();
        let mut versions = Vec::new();
        for n in 1..=count {
            let ver = format!("2027-03-01-mat-{n:03}");
            let mat_key = format!("processed/scoring/matrices/{ver}.json");
            assert_eq!(
                p.adapter
                    .fos(FosRequest {
                        method: "PUT",
                        region: "us-east-1",
                        bucket: "fos-fixture-logs",
                        key: &mat_key,
                        query: "",
                        access_key: &ak,
                        secret_key: &sk,
                        body: serde_json::to_vec(&json!({"version": ver, "vocab_size": 1}))
                            .unwrap(),
                        timeout: std::time::Duration::from_secs(5),
                        response_limit: 65_536,
                        content_md5: None,
                    })
                    .unwrap()
                    .status,
                200
            );
            versions.push(json!({"version": ver, "vocab_size": 1}));
        }
        for (key, payload) in [
            (
                "processed/scoring/matrices/index.json",
                json!({"versions": versions}),
            ),
            (
                "processed/scoring/matrix_latest.json",
                json!({"version": "latest", "vocab_size": 1}),
            ),
        ] {
            assert_eq!(
                p.adapter
                    .fos(FosRequest {
                        method: "PUT",
                        region: "us-east-1",
                        bucket: "fos-fixture-logs",
                        key,
                        query: "",
                        access_key: &ak,
                        secret_key: &sk,
                        body: serde_json::to_vec(&payload).unwrap(),
                        timeout: std::time::Duration::from_secs(5),
                        response_limit: 65_536,
                        content_md5: None,
                    })
                    .unwrap()
                    .status,
                200
            );
        }
        if with_labels {
            assert_eq!(
                p.adapter
                    .fos(FosRequest {
                        method: "PUT",
                        region: "us-east-1",
                        bucket: "fos-fixture-logs",
                        key: "processed/scoring/labels.json",
                        query: "",
                        access_key: &ak,
                        secret_key: &sk,
                        body: serde_json::to_vec(&json!({"labels": {"sid-1": "bot"}})).unwrap(),
                        timeout: std::time::Duration::from_secs(5),
                        response_limit: 65_536,
                        content_md5: None,
                    })
                    .unwrap()
                    .status,
                200
            );
        }
    };

    // Case 1 & 2: 4-retrain warm-KV and 5-retrain warm-KV on scoring/disable({"delete_matrices": true})
    // (must delete matrices + index.json + matrix_latest.json, while preserving labels.json!)
    let p = Platform::new();
    p.provision_logging_service();
    for count in [4usize, 5usize] {
        let en = p.ok(
            "POST",
            "/api/services/fixtureService/scoring/enable",
            json!({}),
        );
        assert_eq!(p.run_job(en["job"].clone())["status"], "complete");
        seed_matrices(&p, count, true);
        let dis = p.ok(
            "POST",
            "/api/services/fixtureService/scoring/disable",
            json!({"delete_matrices": true}),
        );
        let dis_done = p.run_job(dis["job"].clone());
        assert_eq!(
            dis_done["status"], "complete",
            "warm-KV disable(delete_matrices=true) with {count} matrices must complete: {dis_done}"
        );
        let keys = p.adapter.object_keys("fos-fixture-logs");
        assert!(
            !keys
                .iter()
                .any(|k| k.starts_with("processed/scoring/matrices/")
                    || k == "processed/scoring/matrix_latest.json"),
            "matrices and matrix_latest.json must be deleted for count={count}: {keys:?}"
        );
        assert!(
            keys.iter().any(|k| k == "processed/scoring/labels.json"),
            "scoring_disable(delete_matrices=true) must preserve labels.json: {keys:?}"
        );
    }

    // Case 3: 50-matrix stale-KV on POST /scoring/teardown (deletes labels.json too!)
    let en50 = p.ok(
        "POST",
        "/api/services/fixtureService/scoring/enable",
        json!({}),
    );
    assert_eq!(p.run_job(en50["job"].clone())["status"], "complete");
    seed_matrices(&p, 50, true);
    let td_gen = p
        .adapter
        .record_snapshot("svc:fixtureService")
        .unwrap()
        .generation;
    let td = p.ok(
        "POST",
        "/api/services/fixtureService/scoring/teardown",
        json!({"confirm_service_id": "fixtureService", "generation": td_gen}),
    );
    let mut td_job = td["job"].clone();
    let td_id = td_job["id"].as_str().unwrap().to_owned();
    let mut prev_svc = p.adapter.record_snapshot("svc:fixtureService");
    for _ in 0..80 {
        if td_job["status"] == "complete" || td_job["status"] == "failed" {
            break;
        }
        if let Some(stale) = prev_svc.clone() {
            p.adapter.cache_stale("svc:fixtureService", Some(stale));
        }
        prev_svc = p.adapter.record_snapshot("svc:fixtureService");
        let (st, res) = p.call("POST", &format!("/api/admin/jobs/{td_id}/step"), json!({}));
        assert_eq!(st, 200, "stale-KV scoring_teardown step failed: {res}");
        td_job = res["job"].clone();
    }
    assert_eq!(td_job["status"], "complete", "{td_job}");
    let keys_after_td = p.adapter.object_keys("fos-fixture-logs");
    assert!(
        !keys_after_td
            .iter()
            .any(|k| k.starts_with("processed/scoring/")),
        "scoring_teardown must delete all processed/scoring/* including labels.json: {keys_after_td:?}"
    );

    // Case 4: 65-matrix stale-KV on full POST /api/admin/provision/teardown
    let en65 = p.ok(
        "POST",
        "/api/services/fixtureService/scoring/enable",
        json!({}),
    );
    assert_eq!(p.run_job(en65["job"].clone())["status"], "complete");
    seed_matrices(&p, 65, true);
    let full_td_gen = p
        .adapter
        .record_snapshot("svc:fixtureService")
        .unwrap()
        .generation;
    let full_td = p.ok(
        "POST",
        "/api/admin/provision/teardown",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "confirm_service_id": "fixtureService",
            "generation": full_td_gen,
            "remove_logging": true,
            "remove_snippets": true
        }),
    );
    let mut ftd_job = full_td["job"].clone();
    let ftd_id = ftd_job["id"].as_str().unwrap().to_owned();
    let mut prev_svc2 = p.adapter.record_snapshot("svc:fixtureService");
    for _ in 0..100 {
        if ftd_job["status"] == "complete" || ftd_job["status"] == "failed" {
            break;
        }
        if let Some(at) = ftd_job["retry_at"].as_u64() {
            p.clock.fetch_max(at, Ordering::SeqCst);
        }
        if let Some(stale) = prev_svc2.clone() {
            p.adapter.cache_stale("svc:fixtureService", Some(stale));
        }
        prev_svc2 = p.adapter.record_snapshot("svc:fixtureService");
        let (st, res) = p.call("POST", &format!("/api/admin/jobs/{ftd_id}/step"), json!({}));
        assert_eq!(st, 200, "stale-KV provision/teardown step failed: {res}");
        ftd_job = res["job"].clone();
    }
    assert_eq!(ftd_job["status"], "complete", "{ftd_job}");
    let keys_after_full = p.adapter.object_keys("fos-fixture-logs");
    assert!(
        !keys_after_full
            .iter()
            .any(|k| k.starts_with("processed/scoring/")),
        "full provision/teardown must delete all processed/scoring/*: {keys_after_full:?}"
    );
}

#[test]
fn bug_cloud_dr_19a_scoring_enable_without_rum_creates_backend_and_reconcile_cleans_orphans() {
    let p = Platform::new();
    // Provision with default rum.enabled == false:
    p.provision_logging_service();

    let en = p.ok(
        "POST",
        "/api/services/fixtureService/scoring/enable",
        json!({}),
    );
    let done = p.run_job(en["job"].clone());
    assert_eq!(done["status"], "complete", "{done}");

    let backends = p.adapter.resources("fixtureService", "backend");
    assert_eq!(
        backends.len(),
        1,
        "scoring/enable without RUM must create session_scorer backend: {backends:?}"
    );
    assert_eq!(backends[0]["name"], "session_scorer");
    assert_eq!(
        backends[0]["request_condition"],
        "fastly_log_analytics_false"
    );

    // Subsequent reconcile on non-RUM scoring service must keep session_scorer and emit zero rum_assets S3 deletes:
    let calls_before = p.adapter.calls().len();
    let cur = p.ok("GET", "/api/admin/services/fixtureService", json!({}));
    let patched = p.ok(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation": cur["generation"], "sample_rate": 50}),
    );
    let rec = p.ok(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation": patched["generation"]}),
    );
    let rec_done = p.run_job(rec["job"].clone());
    assert_eq!(rec_done["status"], "complete", "{rec_done}");
    assert!(
        !rec_done["changes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["resource"] == "fos_object"),
        "non-RUM reconcile must not emit spurious rum_assets fos_object changes: {rec_done}"
    );
    let new_calls = &p.adapter.calls()[calls_before..];
    assert!(
        !new_calls.iter().any(
            |(m, path)| m == "DELETE" && (path.contains("rum.js") || path.contains("faro-sdk"))
        ),
        "non-RUM reconcile must not issue S3 DELETE on rum_assets: {new_calls:?}"
    );
    assert_eq!(
        p.adapter
            .resources("fixtureService", "backend")
            .iter()
            .filter(|b| b["name"] == "session_scorer")
            .count(),
        1
    );

    // Orphan cleanup: seed orphaned scoring snippet + session_scorer backend + fastly_log_analytics_false
    // on a non-scoring, non-RUM service and verify reconcile deletes them in dependency-safe order.
    let p_orphan = Platform::new();
    p_orphan.provision_logging_service();
    p_orphan.adapter.seed_resource(
        "fixtureService",
        "condition",
        json!({
            "name": "fastly_log_analytics_false",
            "type": "REQUEST",
            "priority": "10",
            "statement": "false"
        }),
    );
    p_orphan.adapter.seed_resource(
        "fixtureService",
        "backend",
        json!({
            "name": "session_scorer",
            "address": "orphan-scorer.edgecompute.app",
            "port": 443,
            "use_ssl": true,
            "ssl_check_cert": true,
            "request_condition": "fastly_log_analytics_false"
        }),
    );
    p_orphan.adapter.seed_resource(
        "fixtureService",
        "snippet",
        json!({
            "name": "Session Scoring - Recv",
            "type": "recv",
            "priority": 5,
            "content": "set req.backend = F_session_scorer;"
        }),
    );
    let orphan_gen =
        p_orphan.ok("GET", "/api/admin/services/fixtureService", json!({}))["generation"].clone();
    let orphan_rec = p_orphan.ok(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation": orphan_gen}),
    );
    let orphan_done = p_orphan.run_job(orphan_rec["job"].clone());
    assert_eq!(orphan_done["status"], "complete", "{orphan_done}");
    assert!(
        p_orphan
            .adapter
            .resources("fixtureService", "backend")
            .is_empty(),
        "orphan session_scorer backend must be deleted by reconcile"
    );
    assert!(
        !p_orphan
            .adapter
            .resources("fixtureService", "condition")
            .iter()
            .any(|c| c["name"] == "fastly_log_analytics_false"),
        "orphan fastly_log_analytics_false condition must be deleted after backend"
    );
    assert!(
        !p_orphan
            .adapter
            .resources("fixtureService", "snippet")
            .iter()
            .any(|s| s["name"] == "Session Scoring - Recv"),
        "orphan scoring snippet must be deleted"
    );

    // Bare orphan session_scorer backend (without snippets or condition) is also cleaned by reconcile:
    p_orphan.adapter.seed_resource(
        "fixtureService",
        "backend",
        json!({
            "name": "session_scorer",
            "address": "bare-orphan-scorer.edgecompute.app",
            "port": 443,
            "use_ssl": true,
            "ssl_check_cert": true
        }),
    );
    let bare_gen =
        p_orphan.ok("GET", "/api/admin/services/fixtureService", json!({}))["generation"].clone();
    let bare_rec = p_orphan.ok(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation": bare_gen}),
    );
    assert_eq!(
        p_orphan.run_job(bare_rec["job"].clone())["status"],
        "complete"
    );
    assert!(
        p_orphan
            .adapter
            .resources("fixtureService", "backend")
            .is_empty(),
        "bare orphan session_scorer backend must be deleted by reconcile"
    );
}

#[test]
fn bug_cloud_dr_19b_retry_ignores_stale_job_tagged_draft_after_customer_activates_newer_version() {
    let p = Platform::new();
    p.provision_logging_service();
    let en = p.ok(
        "POST",
        "/api/services/fixtureService/scoring/enable",
        json!({}),
    );
    let mut job = en["job"].clone();
    let job_id = job["id"].as_str().unwrap().to_owned();
    let mut injected = false;
    for _ in 0..80 {
        if job["status"] == "complete" || job["status"] == "failed" {
            break;
        }
        if job["step"] == "validate" && !injected {
            p.adapter.invalid_draft(true);
            injected = true;
        }
        let (st, res) = p.call("POST", &format!("/api/admin/jobs/{job_id}/step"), json!({}));
        assert_eq!(st, 200, "{res}");
        job = res["job"].clone();
    }
    p.adapter.invalid_draft(false);
    assert_eq!(job["status"], "failed", "{job}");
    assert_eq!(job["error"], "draft_validation_failed", "{job}");

    // Customer clones active v2 -> v4, adds a critical snippet, and activates v4 while v3 stays unlocked with flp-job:{job_id}:
    let (_, cloned_v4) = p
        .adapter
        .api(ApiRequest {
            method: "PUT",
            path: "/service/fixtureService/version/2/clone",
            query: "",
            token: "fixture-token",
            body: None,
            timeout: std::time::Duration::from_secs(5),
        })
        .unwrap();
    let v4_num = cloned_v4["number"].as_u64().unwrap();
    assert_eq!(v4_num, 4);
    let (snip_st, _) = p
        .adapter
        .api(ApiRequest {
            method: "POST",
            path: "/service/fixtureService/version/4/snippet",
            query: "",
            token: "fixture-token",
            body: Some(json!({
                "name": "Customer Critical Auth v4",
                "type": "recv",
                "priority": 1,
                "content": "set req.http.X-Customer-Auth = \"v4-live\";"
            })),
            timeout: std::time::Duration::from_secs(5),
        })
        .unwrap();
    assert_eq!(snip_st, 200);
    let (act_st, _) = p
        .adapter
        .api(ApiRequest {
            method: "PUT",
            path: "/service/fixtureService/version/4/activate",
            query: "",
            token: "fixture-token",
            body: None,
            timeout: std::time::Duration::from_secs(5),
        })
        .unwrap();
    assert_eq!(act_st, 200);

    // Retry must ignore stale unlocked v3 (3 < 4), clone v5 from v4, and activate v5 preserving Customer Critical Auth v4!
    p.clock.fetch_add(65, Ordering::SeqCst);
    let retried = p.ok(
        "POST",
        &format!("/api/admin/jobs/{job_id}/retry"),
        json!({}),
    )["job"]
        .clone();
    let done = p.run_job(retried);
    assert_eq!(done["status"], "complete", "{done}");

    let (_, versions) = p
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
    let active_num = versions
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["active"] == true)
        .and_then(|v| v["number"].as_u64())
        .unwrap();
    assert_eq!(
        active_num, 5,
        "retry must clone and activate v5 (> v4), not stale v3: {versions}"
    );
    let active_snippets = p.adapter.resources("fixtureService", "snippet");
    assert!(
        active_snippets
            .iter()
            .any(|s| s["name"] == "Customer Critical Auth v4"),
        "Customer Critical Auth v4 snippet must be preserved on live v5: {active_snippets:?}"
    );
    assert!(
        active_snippets
            .iter()
            .any(|s| s["name"] == "Session Scoring - Recv"),
        "Session Scoring snippets must also be active on v5"
    );
}

#[test]
fn bug_cloud_dr_19c_low_budget_post_logging_reconcile_failure_defers_rollback_and_retries_cleanly()
{
    let p = Platform::new();
    let prov = p.ok(
        "POST",
        "/api/admin/provision/execute",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1",
            "rum_enabled": true,
            "log_fields": {"groups":["A","C"],"custom_fields":[],"field_overrides":{},"field_limits":{}}
        }),
    )["job"]
        .clone();
    assert_eq!(p.run_job(prov)["status"], "complete");

    let pre_enable_svc = p.adapter.record_snapshot("svc:fixtureService");
    let en = p.ok(
        "POST",
        "/api/services/fixtureService/scoring/enable",
        json!({}),
    );
    let mut job = en["job"].clone();
    let job_id = job["id"].as_str().unwrap().to_owned();

    // Advance until job reaches customer VCL "discover" (immediately after logging_reconcile):
    for _ in 0..40 {
        if job["step"] == "discover" {
            break;
        }
        let (st, res) = p.call("POST", &format!("/api/admin/jobs/{job_id}/step"), json!({}));
        assert_eq!(st, 200, "{res}");
        job = res["job"].clone();
    }
    assert_eq!(job["step"], "discover", "{job}");

    // At "discover", svc:fixtureService currently has scoring.enabled == true + 9 scoring fields.
    // Inject foreign primary endpoint so "discover" fails terminally (409 managed_resources_not_owned),
    // AND simulate stale POP KV + Shell preamble + transient GET retries leaving < 6 remaining calls (5 left):
    p.adapter.seed_resource(
        "fixtureService",
        "logging/fos",
        json!({
            "name": "Fastly Object Storage Logs",
            "bucket_name": "fos-other-gce-bucket",
            "region": "us-east-1",
            "path": "raw/",
            "response_condition": "log_analytics_condition"
        }),
    );
    p.adapter
        .cache_stale("svc:fixtureService", pre_enable_svc.clone());
    // With initial_used = 13: step preamble + stale svc confirm + secret + fence = 8 calls (used = 21),
    // discover GET version + 4 base + 2 rum = 7 calls (used = 28 -> remaining_calls() == 4 < 6),
    // leaving 4 calls (>= 3 needed for job CAS + lock GET + lock CAS) so step returns 200 without 503!
    let (low_st, low_res) = p.call_with_initial_used(
        13,
        "POST",
        &format!("/api/admin/jobs/{job_id}/step"),
        json!({}),
    );
    assert_eq!(
        low_st, 200,
        "low-budget discover failure must checkpoint cleanly without 503: {low_res}"
    );
    job = low_res["job"].clone();
    assert_eq!(
        job["step"], "scoring_rollback",
        "when remaining_calls() < 6 prevents in-tick svc rollback, job must defer to scoring_rollback: {job}"
    );
    assert_ne!(
        job["status"], "failed",
        "job must not enter terminal failed while svc.scoring.enabled is still true: {job}"
    );

    // Next tick (even with stale KV!) executes scoring_rollback, reverts svc.scoring.enabled = false + strips custom_fields,
    // rewinds step = "logging_reconcile", and marks terminal status = "failed" with original error code:
    p.adapter.cache_stale("svc:fixtureService", pre_enable_svc);
    let (rb_st, rb_res) = p.call("POST", &format!("/api/admin/jobs/{job_id}/step"), json!({}));
    assert_eq!(rb_st, 200, "{rb_res}");
    job = rb_res["job"].clone();
    assert_eq!(job["status"], "failed", "{job}");
    assert_eq!(job["error"], "managed_resources_not_owned", "{job}");
    assert_eq!(job["step"], "logging_reconcile", "{job}");

    let svc_failed = p.adapter.record_snapshot("svc:fixtureService").unwrap();
    assert_eq!(
        svc_failed.value["scoring"]["enabled"], false,
        "svc.scoring.enabled must be rolled back to false before terminal failed: {}",
        svc_failed.value
    );
    assert_eq!(
        svc_failed.value["log_fields"]["custom_fields"]
            .as_array()
            .unwrap()
            .len(),
        0,
        "all 9 scoring custom fields must be stripped on rollback"
    );

    // Remove the foreign endpoint collision and verify POST /retry succeeds cleanly to complete!
    // Restore Cloud's owned Fastly Object Storage Logs endpoint first so Managed service has its endpoint:
    p.adapter.seed_resource(
        "fixtureService",
        "logging/fos",
        json!({
            "name": "Fastly Object Storage Logs",
            "bucket_name": "fos-fixture-logs",
            "region": "us-east-1",
            "path": "raw/request/year=%Y/month=%m/day=%d/hour=%H/minute=%M/",
            "response_condition": "log_analytics_condition"
        }),
    );
    p.clock.fetch_add(65, Ordering::SeqCst);
    let retried = p.ok(
        "POST",
        &format!("/api/admin/jobs/{job_id}/retry"),
        json!({}),
    )["job"]
        .clone();
    let done = p.run_job(retried);
    assert_eq!(done["status"], "complete", "{done}");
    let svc_done = p.adapter.record_snapshot("svc:fixtureService").unwrap();
    assert_eq!(svc_done.value["scoring"]["enabled"], true);
    assert_eq!(
        svc_done.value["log_fields"]["custom_fields"]
            .as_array()
            .unwrap()
            .len(),
        9
    );
}

#[test]
fn bug_cloud_dr_20a_post_dr_recover_reconcile_backfills_missing_scoring_request_secret() {
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
    let keys_store_id = status["scoring"]["scoring_keys_store_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let expected_secret = p
        .adapter
        .config_store_value(&keys_store_id, "request_secret")
        .expect("scorer ConfigStore must retain request_secret");
    assert!(!expected_secret.is_empty());

    // Simulate cold-start Cloud DR: wipe Cloud KV records and Cloud Secret Store secrets
    // while customer Fastly service, FOS bucket (meta/cloud_dr_state.json), and scorer stores survive.
    for key in p.adapter.records().into_keys() {
        p.adapter.cache_invalidate(&key);
        p.adapter.delete(&key).unwrap();
    }
    for sec in [
        "fastly_token.fixtureService",
        "fos_ak.fixtureService",
        "fos_sk.fixtureService",
        "scoring_request_secret.fixtureService",
    ] {
        let (st, _) = p
            .adapter
            .api(ApiRequest {
                method: "DELETE",
                path: &format!("/resources/stores/secret/fixtureSecrets/secrets/{sec}"),
                query: "",
                token: "fixture-token",
                body: None,
                timeout: std::time::Duration::from_secs(5),
            })
            .unwrap();
        assert_eq!(st, 204);
    }
    assert!(p
        .adapter
        .secret("scoring_request_secret.fixtureService")
        .unwrap()
        .is_none());

    let recovered = p.ok(
        "POST",
        "/api/admin/provision/recover",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1"
        }),
    );
    assert_eq!(recovered["source"], "dr_state", "{recovered}");
    assert!(
        p.adapter
            .secret("scoring_request_secret.fixtureService")
            .unwrap()
            .is_none(),
        "dr::recover itself must add zero extra secret/store calls"
    );

    // Post-DR reconcile must recover request_secret from verified scoring_keys store, backfill Secret Store, and complete!
    let rec_gen = p
        .adapter
        .record_snapshot("svc:fixtureService")
        .unwrap()
        .generation;
    let rec_job = p.ok(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation": rec_gen}),
    )["job"]
        .clone();
    let done = p.run_job(rec_job);
    assert_eq!(
        done["status"], "complete",
        "post-DR reconcile on scoring-enabled service must recover request_secret and complete: {done}"
    );
    assert_eq!(
        p.adapter
            .secret("scoring_request_secret.fixtureService")
            .unwrap()
            .as_deref(),
        Some(expected_secret.as_str()),
        "reconcile must backfill scoring_request_secret.fixtureService in Cloud Secret Store"
    );
    let pass_snip = p
        .adapter
        .resources("fixtureService", "snippet")
        .into_iter()
        .find(|s| s["name"] == "Session Scoring - Pass")
        .expect("Session Scoring - Pass snippet must exist on active customer VCL");
    let content = pass_snip["content"].as_str().unwrap_or("");
    assert!(
        content.contains(&expected_secret) && !content.contains("__FLP_SCORING_REQUEST_SECRET__"),
        "Session Scoring - Pass must embed recovered request_secret without placeholder: {content}"
    );

    // Fail-closed check: if BOTH Secret Store AND customer ConfigStore lack request_secret, reconcile fails closed with credentials_pending!
    p.adapter
        .api(ApiRequest {
            method: "DELETE",
            path: "/resources/stores/secret/fixtureSecrets/secrets/scoring_request_secret.fixtureService",
            query: "",
            token: "fixture-token",
            body: None,
            timeout: std::time::Duration::from_secs(5),
        })
        .unwrap();
    p.adapter
        .api(ApiRequest {
            method: "PUT",
            path: &format!("/resources/stores/config/{keys_store_id}/item/request_secret"),
            query: "",
            token: "fixture-token",
            body: Some(json!({"item_value": ""})),
            timeout: std::time::Duration::from_secs(5),
        })
        .unwrap();
    let rec2_gen = p
        .adapter
        .record_snapshot("svc:fixtureService")
        .unwrap()
        .generation;
    let rec2_job = p.ok(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation": rec2_gen}),
    )["job"]
        .clone();
    let failed_closed = p.run_job(rec2_job);
    assert_eq!(failed_closed["status"], "failed", "{failed_closed}");
    assert_eq!(
        failed_closed["error"], "credentials_pending",
        "{failed_closed}"
    );
}

#[test]
fn bug_cloud_dr_20b_mutating_scoring_and_label_routes_reject_active_jobs_leases_and_data_reset() {
    let p = Platform::new();
    p.provision_logging_service();
    let en = p.ok(
        "POST",
        "/api/services/fixtureService/scoring/enable",
        json!({}),
    );
    assert_eq!(p.run_job(en["job"].clone())["status"], "complete");

    let valid_matrix = json!({
        "matrix": {
            "version": "2026-11-22-v1",
            "vocab_size": 2,
            "session_count": 10,
            "transition_count": 20,
            "counts": {"0": {"1": 20}},
            "row_totals": {"0": 20}
        }
    });
    p.ok(
        "POST",
        "/api/services/fixtureService/scoring/retrain",
        valid_matrix.clone(),
    );
    let lbl = p.ok(
        "POST",
        "/api/services/fixtureService/scoring/labels",
        json!({"edge_sid": "sid-initial-1", "label": "bad", "notes": "seed"}),
    );
    let lbl_id = lbl["label"]["id"].as_str().unwrap().to_owned();

    let assert_all_mutations_409 = |p: &Platform, phase: &str| {
        for (method, path, payload) in [
            (
                "PUT",
                "/api/services/fixtureService/scoring/threshold".to_owned(),
                json!({"threshold": 55}),
            ),
            (
                "PUT",
                "/api/services/fixtureService/scoring/enforce-threshold?confirm=1".to_owned(),
                json!({"enforce_threshold": 80}),
            ),
            (
                "PUT",
                "/api/services/fixtureService/scoring/l2-enforce?confirm=1".to_owned(),
                json!({"enabled": true}),
            ),
            (
                "PUT",
                "/api/services/fixtureService/scoring/exclude-regex?confirm=1".to_owned(),
                json!({"exclude_url_regex": "^/healthz$"}),
            ),
            (
                "PUT",
                "/api/services/fixtureService/scoring/enforce-status-code?confirm=1".to_owned(),
                json!({"status_code": 403}),
            ),
            (
                "POST",
                "/api/services/fixtureService/scoring/rotate-key?confirm=1".to_owned(),
                json!({}),
            ),
            (
                "POST",
                "/api/services/fixtureService/scoring/retrain".to_owned(),
                valid_matrix.clone(),
            ),
            (
                "POST",
                "/api/services/fixtureService/scoring/matrix/versions/restore?confirm=1".to_owned(),
                json!({"version": "2026-11-22-v1"}),
            ),
            (
                "POST",
                "/api/services/fixtureService/scoring/labels".to_owned(),
                json!({"edge_sid": "sid-blocked", "label": "good"}),
            ),
            (
                "PATCH",
                format!("/api/services/fixtureService/scoring/labels/{lbl_id}"),
                json!({"label": "neutral"}),
            ),
            (
                "DELETE",
                format!("/api/services/fixtureService/scoring/labels/{lbl_id}"),
                json!({}),
            ),
        ] {
            let (st, res) = p.call(method, &path, payload);
            assert_eq!(
                st, 409,
                "[{phase}] {method} {path} must reject concurrent mutation with 409, got {st}: {res}"
            );
        }
    };

    // 1. Active reconcile job + active service lease:
    let rec_gen = p
        .adapter
        .record_snapshot("svc:fixtureService")
        .unwrap()
        .generation;
    let rec = p.ok(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation": rec_gen}),
    )["job"]
        .clone();
    assert_all_mutations_409(&p, "pending_reconcile_job");
    let now = p.clock.load(Ordering::SeqCst);
    let lock_gen = p
        .adapter
        .record_snapshot("reconcile_lock:fixtureService")
        .map(|r| r.generation);
    p.adapter.cache_invalidate("reconcile_lock:fixtureService");
    p.adapter
        .cas(
            "reconcile_lock:fixtureService",
            lock_gen.as_deref(),
            &json!({"job_id": rec["id"], "fence": 99, "expires_at": now + 30}),
        )
        .unwrap();
    assert_all_mutations_409(&p, "active_reconcile_lease");
    p.clock.fetch_add(35, Ordering::SeqCst);
    assert_eq!(p.run_job(rec)["status"], "complete");

    // 1b. Standalone active reconcile_lock lease when pending_job_id is None (Auditor Challenge 1):
    let post_rec_svc = p.adapter.record_snapshot("svc:fixtureService").unwrap();
    assert!(
        post_rec_svc.value["pending_job_id"].is_null(),
        "pending_job_id must be null after reconcile completes: {}",
        post_rec_svc.value
    );
    let standalone_now = p.clock.load(Ordering::SeqCst);
    let standalone_lock_gen = p
        .adapter
        .record_snapshot("reconcile_lock:fixtureService")
        .map(|r| r.generation);
    p.adapter.cache_invalidate("reconcile_lock:fixtureService");
    p.adapter
        .cas(
            "reconcile_lock:fixtureService",
            standalone_lock_gen.as_deref(),
            &json!({"job_id": "external-lease-only", "fence": 105, "expires_at": standalone_now + 30}),
        )
        .unwrap();
    assert_all_mutations_409(&p, "standalone_active_reconcile_lease");
    let clear_lock_gen = p
        .adapter
        .record_snapshot("reconcile_lock:fixtureService")
        .map(|r| r.generation);
    p.adapter.cache_invalidate("reconcile_lock:fixtureService");
    p.adapter
        .cas(
            "reconcile_lock:fixtureService",
            clear_lock_gen.as_deref(),
            &json!({"job_id": "external-lease-only", "fence": 105, "expires_at": 0}),
        )
        .unwrap();

    // 2. Active data_reset marker on svc:fixtureService:
    let svc_snap = p.adapter.record_snapshot("svc:fixtureService").unwrap();
    let mut with_reset = svc_snap.value.clone();
    with_reset["data_reset"] = json!({
        "job_id": "resetJob1",
        "reset_at": now,
        "phase": "fencing",
        "started_at": now,
        "actor": "fixtureAdmin",
        "rum_held": false
    });
    p.adapter.cache_invalidate("svc:fixtureService");
    p.adapter
        .cas(
            "svc:fixtureService",
            Some(&svc_snap.generation),
            &with_reset,
        )
        .unwrap();
    assert_all_mutations_409(&p, "active_data_reset");
    let reset_snap = p.adapter.record_snapshot("svc:fixtureService").unwrap();
    let mut cleared_reset = reset_snap.value.clone();
    cleared_reset.as_object_mut().unwrap().remove("data_reset");
    p.adapter.cache_invalidate("svc:fixtureService");
    p.adapter
        .cas(
            "svc:fixtureService",
            Some(&reset_snap.generation),
            &cleared_reset,
        )
        .unwrap();

    // 3. During provision/teardown at scoring_cleanup_state: PUT /enforce-status-code must 409,
    // never leak Session Scoring - Enforce onto stripped live VCL, and never wedge teardown!
    let td_gen = p
        .adapter
        .record_snapshot("svc:fixtureService")
        .unwrap()
        .generation;
    let mut td_job = p.ok(
        "POST",
        "/api/admin/provision/teardown",
        json!({
            "service_id": "fixtureService",
            "confirm_service_id": "fixtureService",
            "generation": td_gen,
            "remove_logging": true,
            "remove_snippets": true,
            "delete_fos_keys": true,
            "delete_data": true,
            "delete_fos_bucket": true
        }),
    )["job"]
        .clone();
    let td_id = td_job["id"].as_str().unwrap().to_owned();
    for _ in 0..30 {
        if td_job["step"] == "scoring_cleanup_state" {
            break;
        }
        let (st, res) = p.call("POST", &format!("/api/admin/jobs/{td_id}/step"), json!({}));
        assert_eq!(st, 200, "{res}");
        td_job = res["job"].clone();
    }
    assert_eq!(td_job["step"], "scoring_cleanup_state", "{td_job}");
    assert_all_mutations_409(&p, "teardown_scoring_cleanup_state");
    let td_done = p.run_job(td_job);
    assert_eq!(td_done["status"], "complete", "{td_done}");
    assert!(
        !p.adapter
            .resources("fixtureService", "snippet")
            .iter()
            .any(|s| s["name"] == "Session Scoring - Enforce"),
        "Session Scoring - Enforce must never leak onto torn-down customer VCL"
    );
}

#[test]
fn bug_cloud_dr_20c_scoring_rollback_never_livelocks_preserves_pending_failure_and_skips_superseded(
) {
    // Part A: _audit_outbox reaches 4095 right before logging_reconcile (4091 before scoring/enable),
    // logging_reconcile pushes #4096, and validate fails with draft_validation_failed ->
    // must roll back cleanly to status="failed", error="draft_validation_failed", scoring.enabled=false in <= 2 ticks!
    let p = Platform::new();
    p.provision_logging_service();
    let svc_rec = p.adapter.record_snapshot("svc:fixtureService").unwrap();
    let mut seeded = svc_rec.value.clone();
    let entries: Vec<Value> = (0..4089)
        .map(|i| json!({"actor": "fixtureAdmin", "at": 1_800_000_000u64 + i, "event": "seed", "config_version": 1}))
        .collect();
    seeded["_audit_outbox"] = Value::Array(entries);
    p.adapter.cache_invalidate("svc:fixtureService");
    p.adapter
        .cas("svc:fixtureService", Some(&svc_rec.generation), &seeded)
        .unwrap();

    let mut job = p.ok(
        "POST",
        "/api/services/fixtureService/scoring/enable",
        json!({}),
    )["job"]
        .clone();
    let job_id = job["id"].as_str().unwrap().to_owned();
    let mut injected = false;
    let mut post_validate_ticks = 0u32;
    for _ in 0..40 {
        if job["status"] == "complete" || job["status"] == "failed" {
            break;
        }
        if job["step"] == "validate" && !injected {
            let cur = p.adapter.record_snapshot("svc:fixtureService").unwrap();
            assert_eq!(
                cur.value["_audit_outbox"].as_array().unwrap().len(),
                4096,
                "logging_reconcile must have filled _audit_outbox to 4096 at validate"
            );
            p.adapter.invalid_draft(true);
            injected = true;
        }
        if injected {
            post_validate_ticks += 1;
            assert!(
                post_validate_ticks <= 3,
                "scoring_rollback livelocked past 3 ticks: {job}"
            );
        }
        let (st, res) = p.call("POST", &format!("/api/admin/jobs/{job_id}/step"), json!({}));
        assert_eq!(st, 200, "{res}");
        job = res["job"].clone();
    }
    p.adapter.invalid_draft(false);
    assert!(injected, "job must have reached validate: {job}");
    assert_eq!(job["status"], "failed", "{job}");
    assert_eq!(job["error"], "draft_validation_failed", "{job}");
    let after_a = p.adapter.record_snapshot("svc:fixtureService").unwrap();
    assert_eq!(after_a.value["scoring"]["enabled"], false);
    assert_eq!(
        after_a.value["log_fields"]["custom_fields"]
            .as_array()
            .unwrap()
            .len(),
        0
    );

    // Part B: Low-budget deferred scoring_rollback taking a second budget tick + non-retryable error inside
    // scoring_rollback both preserve original pending_failure without livelocking!
    let p2 = Platform::new();
    p2.provision_logging_service();
    let pre_svc = p2.adapter.record_snapshot("svc:fixtureService");
    let mut j2 = p2.ok(
        "POST",
        "/api/services/fixtureService/scoring/enable",
        json!({}),
    )["job"]
        .clone();
    let j2_id = j2["id"].as_str().unwrap().to_owned();
    for _ in 0..30 {
        if j2["step"] == "discover" {
            break;
        }
        let (_, res) = p2.call("POST", &format!("/api/admin/jobs/{j2_id}/step"), json!({}));
        j2 = res["job"].clone();
    }
    assert_eq!(j2["step"], "discover");
    p2.adapter.seed_resource(
        "fixtureService",
        "logging/fos",
        json!({
            "name": "Fastly Object Storage Logs",
            "bucket_name": "fos-other-gce-bucket",
            "region": "us-east-1",
            "path": "raw/",
            "response_condition": "log_analytics_condition"
        }),
    );
    p2.adapter.cache_stale("svc:fixtureService", pre_svc);
    let (st_def, res_def) = p2.call_with_initial_used(
        14,
        "POST",
        &format!("/api/admin/jobs/{j2_id}/step"),
        json!({}),
    );
    assert_eq!(st_def, 200, "{res_def}");
    assert_eq!(res_def["job"]["step"], "scoring_rollback");
    // While paused at scoring_rollback, mutating scoring routes (e.g. enforce-status-code, threshold) MUST return 409!
    for (m, path, payload) in [
        (
            "PUT",
            "/api/services/fixtureService/scoring/enforce-status-code?confirm=1",
            json!({"status_code": 403}),
        ),
        (
            "PUT",
            "/api/services/fixtureService/scoring/threshold",
            json!({"threshold": 70}),
        ),
    ] {
        let (bst, bres) = p2.call(m, path, payload);
        assert_eq!(
            bst, 409,
            "{m} {path} must return 409 during scoring_rollback: {bres}"
        );
    }
    // Transient state_unavailable during scoring_rollback (2nd tick required) preserves original pending_failure!
    let _ = p2.adapter.get("svc:fixtureService").unwrap();
    p2.adapter.stale_kv_read("svc:fixtureService", 4, None);
    let (st_low_rb, res_low_rb) =
        p2.call("POST", &format!("/api/admin/jobs/{j2_id}/step"), json!({}));
    assert_eq!(st_low_rb, 200, "{res_low_rb}");
    assert_eq!(
        res_low_rb["job"]["step"], "scoring_rollback",
        "{res_low_rb}"
    );
    p2.adapter.stale_kv_read("svc:fixtureService", 0, None);
    if let Some(at) = res_low_rb["job"]["retry_at"].as_u64() {
        p2.clock.fetch_max(at + 1, Ordering::SeqCst);
    }
    // Full-budget tick completes scoring_rollback and preserves original error ("managed_resources_not_owned"):
    let (st_fin_rb, res_fin_rb) =
        p2.call("POST", &format!("/api/admin/jobs/{j2_id}/step"), json!({}));
    assert_eq!(st_fin_rb, 200, "{res_fin_rb}");
    assert_eq!(res_fin_rb["job"]["status"], "failed", "{res_fin_rb}");
    assert_eq!(
        res_fin_rb["job"]["error"], "managed_resources_not_owned",
        "{res_fin_rb}"
    );
    assert!(
        !p2.adapter
            .resources("fixtureService", "snippet")
            .iter()
            .any(|s| s["name"] == "Session Scoring - Enforce"),
        "Session Scoring - Enforce must never leak onto live VCL during scoring_rollback"
    );

    // Also verify non-retryable error inside scoring_rollback (>= 4120 _audit_outbox) terminates in 1 tick with orig_error!
    let p2b = Platform::new();
    p2b.provision_logging_service();
    let pre_svc_b = p2b.adapter.record_snapshot("svc:fixtureService");
    let mut j2b = p2b.ok(
        "POST",
        "/api/services/fixtureService/scoring/enable",
        json!({}),
    )["job"]
        .clone();
    let j2b_id = j2b["id"].as_str().unwrap().to_owned();
    for _ in 0..30 {
        if j2b["step"] == "discover" {
            break;
        }
        let (_, res) = p2b.call("POST", &format!("/api/admin/jobs/{j2b_id}/step"), json!({}));
        j2b = res["job"].clone();
    }
    p2b.adapter.seed_resource(
        "fixtureService",
        "logging/fos",
        json!({
            "name": "Fastly Object Storage Logs",
            "bucket_name": "fos-other-gce-bucket",
            "region": "us-east-1",
            "path": "raw/",
            "response_condition": "log_analytics_condition"
        }),
    );
    p2b.adapter.cache_stale("svc:fixtureService", pre_svc_b);
    let (_, def_b) = p2b.call_with_initial_used(
        14,
        "POST",
        &format!("/api/admin/jobs/{j2b_id}/step"),
        json!({}),
    );
    assert_eq!(def_b["job"]["step"], "scoring_rollback");
    // Corrupt _audit_outbox to 4121 entries while keeping expected_generation synced so bind_service passes and rollback save hits 409:
    let cur_b = p2b.adapter.record_snapshot("svc:fixtureService").unwrap();
    let mut over_b = cur_b.value.clone();
    over_b["_audit_outbox"] = Value::Array(
        (0..4121)
            .map(|i| json!({"actor": "fixtureAdmin", "at": 1_800_000_000u64 + i, "event": "seed", "config_version": 1}))
            .collect(),
    );
    over_b["_job_checkpoint"] =
        json!({"job_id": j2b_id, "from_generation": cur_b.generation, "next": "scoring_rollback"});
    p2b.adapter.cache_invalidate("svc:fixtureService");
    p2b.adapter
        .cas("svc:fixtureService", Some(&cur_b.generation), &over_b)
        .unwrap();
    let (st_term, res_term) =
        p2b.call("POST", &format!("/api/admin/jobs/{j2b_id}/step"), json!({}));
    assert_eq!(st_term, 200, "{res_term}");
    assert_eq!(
        res_term["job"]["status"], "failed",
        "non-retryable error inside scoring_rollback must terminate immediately without livelock: {res_term}"
    );
    assert_eq!(
        res_term["job"]["error"], "managed_resources_not_owned",
        "non-retryable scoring_rollback termination must preserve original pending_failure: {res_term}"
    );

    // Part C (Reviewer Minor Finding 1): Superseded scoring_enable (after provision/teardown starts)
    // must NOT touch svc:fixtureService or break teardown!
    let p3 = Platform::new();
    p3.provision_logging_service();
    let mut j_sup = p3.ok(
        "POST",
        "/api/services/fixtureService/scoring/enable",
        json!({}),
    )["job"]
        .clone();
    let j_sup_id = j_sup["id"].as_str().unwrap().to_owned();
    for _ in 0..30 {
        if j_sup["step"] == "discover" {
            break;
        }
        let (_, res) = p3.call(
            "POST",
            &format!("/api/admin/jobs/{j_sup_id}/step"),
            json!({}),
        );
        j_sup = res["job"].clone();
    }
    assert_eq!(j_sup["step"], "discover");
    let td_gen = p3
        .adapter
        .record_snapshot("svc:fixtureService")
        .unwrap()
        .generation;
    let td_job = p3.ok(
        "POST",
        "/api/admin/provision/teardown",
        json!({
            "service_id": "fixtureService",
            "confirm_service_id": "fixtureService",
            "generation": td_gen,
            "remove_logging": true,
            "remove_snippets": true,
            "delete_fos_keys": true,
            "delete_data": true,
            "delete_fos_bucket": true
        }),
    )["job"]
        .clone();
    let svc_after_td_start = p3
        .adapter
        .record_snapshot("svc:fixtureService")
        .unwrap()
        .generation;
    // Abandoned tab steps superseded scoring_enable J1:
    let (sup_st, sup_res) = p3.call(
        "POST",
        &format!("/api/admin/jobs/{j_sup_id}/step"),
        json!({}),
    );
    assert_eq!(sup_st, 200, "{sup_res}");
    assert_eq!(sup_res["job"]["status"], "failed", "{sup_res}");
    assert_eq!(
        sup_res["job"]["error"], "service_changed_rerun",
        "{sup_res}"
    );
    assert_eq!(
        p3.adapter
            .record_snapshot("svc:fixtureService")
            .unwrap()
            .generation,
        svc_after_td_start,
        "superseded scoring_enable must never mutate svc:fixtureService"
    );
    assert_eq!(p3.run_job(td_job)["status"], "complete");
}

#[test]
fn bug_cloud_dr_20d_strip_logging_activate_guards_source_version_and_retries_preserving_customer_vcl(
) {
    let p = Platform::new();
    p.provision_logging_service();
    let en = p.ok(
        "POST",
        "/api/services/fixtureService/scoring/enable",
        json!({}),
    );
    assert_eq!(p.run_job(en["job"].clone())["status"], "complete");

    let mut dis_job = p.ok(
        "POST",
        "/api/services/fixtureService/scoring/disable",
        json!({}),
    )["job"]
        .clone();
    let dis_id = dis_job["id"].as_str().unwrap().to_owned();
    for _ in 0..20 {
        if dis_job["step"] == "scoring_strip_activate" {
            break;
        }
        let (st, res) = p.call("POST", &format!("/api/admin/jobs/{dis_id}/step"), json!({}));
        assert_eq!(st, 200, "{res}");
        dis_job = res["job"].clone();
    }
    assert_eq!(dis_job["step"], "scoring_strip_activate", "{dis_job}");

    // Customer clones active v3 -> v5, adds "Customer Critical Auth v5", and activates v5 before scoring_strip_activate:
    let (_, cloned_v5) = p
        .adapter
        .api(ApiRequest {
            method: "PUT",
            path: "/service/fixtureService/version/3/clone",
            query: "",
            token: "fixture-token",
            body: None,
            timeout: std::time::Duration::from_secs(5),
        })
        .unwrap();
    let v5_num = cloned_v5["number"].as_u64().unwrap();
    assert_eq!(v5_num, 5);
    p.adapter
        .api(ApiRequest {
            method: "POST",
            path: "/service/fixtureService/version/5/snippet",
            query: "",
            token: "fixture-token",
            body: Some(json!({
                "name": "Customer Critical Auth v5",
                "type": "recv",
                "priority": 1,
                "content": "set req.http.X-Customer-Auth = \"v5-live\";"
            })),
            timeout: std::time::Duration::from_secs(5),
        })
        .unwrap();
    p.adapter
        .api(ApiRequest {
            method: "PUT",
            path: "/service/fixtureService/version/5/activate",
            query: "",
            token: "fixture-token",
            body: None,
            timeout: std::time::Duration::from_secs(5),
        })
        .unwrap();

    // scoring_strip_activate MUST fail closed (active_version_changed) and rewind to scoring_strip_logging_vcl!
    let (act_st, act_res) = p.call("POST", &format!("/api/admin/jobs/{dis_id}/step"), json!({}));
    assert_eq!(act_st, 200, "{act_res}");
    assert_eq!(
        act_res["job"]["status"], "failed",
        "scoring_strip_activate must fail closed when customer activated v5 mid-job: {act_res}"
    );
    assert_eq!(
        act_res["job"]["error"], "active_version_changed",
        "{act_res}"
    );
    assert_eq!(
        act_res["job"]["step"], "scoring_strip_logging_vcl",
        "{act_res}"
    );

    let (_, versions_mid) = p
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
    let active_mid = versions_mid
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["active"] == true)
        .and_then(|v| v["number"].as_u64())
        .unwrap();
    assert_eq!(
        active_mid, 5,
        "customer v5 must remain active after strip_activate refusal: {versions_mid}"
    );
    assert!(
        p.adapter
            .resources("fixtureService", "snippet")
            .iter()
            .any(|s| s["name"] == "Customer Critical Auth v5"),
        "Customer Critical Auth v5 must remain intact on live v5"
    );

    // Retry must ignore stale draft v4 (4 < 5), clone v6 from v5, strip scoring, and preserve Customer Critical Auth v5!
    p.clock.fetch_add(65, Ordering::SeqCst);
    let retried = p.ok(
        "POST",
        &format!("/api/admin/jobs/{dis_id}/retry"),
        json!({}),
    )["job"]
        .clone();
    let done = p.run_job(retried);
    assert_eq!(done["status"], "complete", "{done}");

    let (_, versions_end) = p
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
    let active_end = versions_end
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["active"] == true)
        .and_then(|v| v["number"].as_u64())
        .unwrap();
    assert_eq!(
        active_end, 6,
        "retry must clone and activate v6 from v5: {versions_end}"
    );
    let end_snippets = p.adapter.resources("fixtureService", "snippet");
    assert!(
        end_snippets
            .iter()
            .any(|s| s["name"] == "Customer Critical Auth v5"),
        "Customer Critical Auth v5 must be preserved on v6: {end_snippets:?}"
    );
    assert!(
        !end_snippets.iter().any(|s| s["name"]
            .as_str()
            .unwrap_or("")
            .starts_with("Session Scoring - ")),
        "all Session Scoring snippets must be stripped on v6: {end_snippets:?}"
    );
}

#[test]
fn bug_cloud_dr_21a_post_dr_reconcile_recovers_secret_under_stale_kv_and_tight_budgets_without_livelock_or_clone_poison(
) {
    let make_post_dr_platform = || -> (Platform, Option<Record>, String) {
        let p = Platform::new();
        p.provision_logging_service();
        let en = p.ok(
            "POST",
            "/api/services/fixtureService/scoring/enable",
            json!({}),
        );
        assert_eq!(p.run_job(en["job"].clone())["status"], "complete");
        for k in p.adapter.records().into_keys() {
            p.adapter.cache_invalidate(&k);
            p.adapter.delete(&k).unwrap();
        }
        for sec in [
            "fastly_token.fixtureService",
            "fos_ak.fixtureService",
            "fos_sk.fixtureService",
            "scoring_request_secret.fixtureService",
        ] {
            let _ = p
                .adapter
                .api(ApiRequest {
                    method: "DELETE",
                    path: &format!("/resources/stores/secret/fixtureSecrets/secrets/{sec}"),
                    query: "",
                    token: "fixture-token",
                    body: None,
                    timeout: std::time::Duration::from_secs(5),
                })
                .unwrap();
        }
        let (recov_st, recov_body) = p.call_with_initial_used(
            3,
            "POST",
            "/api/admin/provision/recover",
            json!({
                "token": "fixture-token",
                "service_id": "fixtureService",
                "fos_bucket": "fos-fixture-logs",
                "fos_region": "us-east-1"
            }),
        );
        assert_eq!(recov_st, 200, "{recov_body}");
        assert_eq!(recov_body["source"], "dr_state");
        assert!(
            p.adapter
                .secret("scoring_request_secret.fixtureService")
                .unwrap()
                .is_none(),
            "recover should leave scoring_request_secret lazy for reconcile"
        );
        let pre_rec_snap = p.adapter.record_snapshot("svc:fixtureService");
        let rec_gen = pre_rec_snap.as_ref().unwrap().generation.clone();
        let rec_job = p.ok(
            "POST",
            "/api/admin/services/fixtureService/reconcile",
            json!({"generation": rec_gen}),
        )["job"]
            .clone();
        (p, pre_rec_snap, rec_job["id"].as_str().unwrap().to_owned())
    };

    // 1. Stale svc:fixtureService KV cache on EVERY tick under 29/32 Shell budget (initial_used = 3):
    // must recover scoring_request_secret and complete reconcile without stalling!
    let (p_stale, stale_snap, stale_job_id) = make_post_dr_platform();
    let mut final_stale_job = Value::Null;
    for _ in 0..20 {
        p_stale
            .adapter
            .cache_stale("svc:fixtureService", stale_snap.clone());
        let (st, res) = p_stale.call_with_initial_used(
            3,
            "POST",
            &format!("/api/admin/jobs/{stale_job_id}/step"),
            json!({}),
        );
        assert_eq!(st, 200, "{res}");
        final_stale_job = res["job"].clone();
        if let Some(at) = final_stale_job["retry_at"].as_u64() {
            p_stale.clock.fetch_max(at + 1, Ordering::SeqCst);
        }
        if final_stale_job["status"] == "complete" || final_stale_job["status"] == "failed" {
            break;
        }
    }
    assert_eq!(
        final_stale_job["status"], "complete",
        "post-DR reconcile under stale KV + 29/32 budget must complete cleanly: {final_stale_job}"
    );
    assert!(
        p_stale
            .adapter
            .secret("scoring_request_secret.fixtureService")
            .unwrap()
            .is_some(),
        "scoring_request_secret must be backfilled into Secret Store"
    );

    // 2. Sweep initial_used across 0..=18 (admin_remaining_calls == 32..=14), under both warm KV and stale KV:
    // - NEVER fails with uncheckpointed_clone_requires_review!
    // - NEVER spins infinitely with status=="pending" && retry_at==null!
    // - Completes cleanly once budget allows (or directly at initial_used <= 12)!
    for stale_mode in [false, true] {
        for init_used in 0..=18u32 {
            let (p_sw, sw_snap, sw_job_id) = make_post_dr_platform();
            let mut jb = Value::Null;
            let mut prev_marker = String::new();
            let mut null_retry_same_state_streak = 0u32;
            for tick in 0..16u32 {
                if stale_mode {
                    p_sw.adapter
                        .cache_stale("svc:fixtureService", sw_snap.clone());
                }
                // Give constrained budgets (14..19 remaining) 6 constrained ticks to verify zero clone-poison
                // and proper retry_at backoff, then allow normal 29/32 ticks to finish cleanly:
                let used_this_tick = if tick < 6 {
                    init_used
                } else {
                    init_used.min(3)
                };
                let (st, res) = p_sw.call_with_initial_used(
                    used_this_tick,
                    "POST",
                    &format!("/api/admin/jobs/{sw_job_id}/step"),
                    json!({}),
                );
                if init_used == 18 && tick < 6 {
                    // 32 - 18 = 14 remaining BEFORE load_job (13 < 14 at admission gate): returns retryable 503 backend_budget!
                    assert_eq!(st, 503, "stale={stale_mode} init={init_used}: {res}");
                    assert_eq!(res["error"], "backend_budget");
                    continue;
                } else {
                    assert_eq!(st, 200, "stale={stale_mode} init={init_used}: {res}");
                }
                jb = res["job"].clone();
                assert_ne!(
                    jb["error"],
                    json!("uncheckpointed_clone_requires_review"),
                    "stale={stale_mode} init={init_used} self-poisoned clone: {jb}"
                );
                let secret_now = p_sw
                    .adapter
                    .secret("scoring_request_secret.fixtureService")
                    .unwrap()
                    .is_some();
                let marker = format!(
                    "{}:{}:{}:{secret_now}",
                    jb["step"].as_str().unwrap_or(""),
                    jb["draft_version"],
                    jb["completed_steps"]
                );
                if jb["status"] == "pending" && jb["retry_at"].is_null() && marker == prev_marker {
                    null_retry_same_state_streak += 1;
                    assert!(
                        null_retry_same_state_streak <= 1,
                        "stale={stale_mode} init={init_used} livelocked with retry_at=null at {marker}: {jb}"
                    );
                } else {
                    null_retry_same_state_streak = 0;
                }
                prev_marker = marker;
                if let Some(at) = jb["retry_at"].as_u64() {
                    p_sw.clock.fetch_max(at + 1, Ordering::SeqCst);
                }
                if jb["status"] == "complete" || jb["status"] == "failed" {
                    break;
                }
            }
            if jb["status"] != "complete" && jb["status"] != "failed" {
                jb = p_sw.run_job(jb);
            }
            assert_eq!(
                jb["status"], "complete",
                "stale={stale_mode} init={init_used} must complete cleanly: {jb}"
            );
        }
    }
}

#[test]
fn bug_cloud_dr_21b_audit_outbox_boundary_headroom_and_post_activate_strip_kv_idempotency() {
    let seed_outbox = |p: &Platform, count: usize| -> String {
        let snap = p.adapter.record_snapshot("svc:fixtureService").unwrap();
        let mut val = snap.value.clone();
        val["_audit_outbox"] = Value::Array(
            (0..count)
                .map(|i| {
                    json!({
                        "actor": "fixtureAdmin",
                        "at": 1_800_000_000u64 + i as u64,
                        "event": "seed",
                        "config_version": 1
                    })
                })
                .collect(),
        );
        p.adapter.cache_invalidate("svc:fixtureService");
        p.adapter
            .cas("svc:fixtureService", Some(&snap.generation), &val)
            .unwrap();
        p.adapter
            .record_snapshot("svc:fixtureService")
            .unwrap()
            .generation
    };

    // 1. Sweep _audit_outbox == 4090..=4097 across scoring_enable, scoring_disable, scoring_teardown,
    // provision/teardown, and reconcile:
    // - At 4096..=4097: rejected upfront with 409 audit_archive_required and ZERO Fastly VCL mutations!
    // - At 4090..=4095: admitted job MUST complete all checkpoints to "complete" without crashing mid-job!
    for count in 4090..=4097usize {
        // 1a. scoring_enable + reconcile:
        let p_en = Platform::new();
        p_en.provision_logging_service();
        let _ = seed_outbox(&p_en, count);
        let (st_en, res_en) = p_en.call(
            "POST",
            "/api/services/fixtureService/scoring/enable",
            json!({}),
        );
        if count >= 4096 {
            assert_eq!(st_en, 409, "count={count}: {res_en}");
            assert_eq!(res_en["error"], "audit_archive_required");
        } else {
            assert_eq!(st_en, 200, "count={count}: {res_en}");
            let done_en = p_en.run_job(res_en["job"].clone());
            assert_eq!(
                done_en["status"], "complete",
                "scoring_enable admitted at outbox={count} must complete without mid-job audit_archive_required: {done_en}"
            );
            let svc_en = p_en.adapter.record_snapshot("svc:fixtureService").unwrap();
            assert_eq!(svc_en.value["scoring"]["enabled"], true);
        }

        // 1b. scoring_disable at count:
        let p_dis = Platform::new();
        p_dis.provision_logging_service();
        let en_job = p_dis.ok(
            "POST",
            "/api/services/fixtureService/scoring/enable",
            json!({}),
        )["job"]
            .clone();
        assert_eq!(p_dis.run_job(en_job)["status"], "complete");
        let _ = seed_outbox(&p_dis, count);
        let (st_dis, res_dis) = p_dis.call(
            "POST",
            "/api/services/fixtureService/scoring/disable",
            json!({}),
        );
        if count >= 4096 {
            assert_eq!(st_dis, 409, "count={count}: {res_dis}");
            assert_eq!(res_dis["error"], "audit_archive_required");
            assert!(
                p_dis
                    .adapter
                    .resources("fixtureService", "snippet")
                    .iter()
                    .any(|s| s["name"] == "Session Scoring - Pass"),
                "rejected scoring_disable at {count} must leave live scoring VCL untouched"
            );
        } else {
            assert_eq!(st_dis, 200, "count={count}: {res_dis}");
            let done_dis = p_dis.run_job(res_dis["job"].clone());
            assert_eq!(
                done_dis["status"], "complete",
                "scoring_disable admitted at outbox={count} must complete cleanly: {done_dis}"
            );
            let svc_dis = p_dis.adapter.record_snapshot("svc:fixtureService").unwrap();
            assert!(
                svc_dis.value["scoring"].is_null()
                    || svc_dis.value["scoring"]["enabled"] == json!(false),
                "scoring must be disabled/cleared after scoring_disable completes: {}",
                svc_dis.value
            );
        }

        // 1c. scoring_teardown, provision/teardown, and reconcile at critical boundary 4095 & 4096:
        if matches!(count, 4095 | 4096) {
            let p_std = Platform::new();
            p_std.provision_logging_service();
            let en_std = p_std.ok(
                "POST",
                "/api/services/fixtureService/scoring/enable",
                json!({}),
            )["job"]
                .clone();
            assert_eq!(p_std.run_job(en_std)["status"], "complete");
            let gen_std = seed_outbox(&p_std, count);
            let (st_std, res_std) = p_std.call(
                "POST",
                "/api/services/fixtureService/scoring/teardown",
                json!({"confirm_service_id": "fixtureService", "generation": gen_std}),
            );
            if count >= 4096 {
                assert_eq!(st_std, 409);
                assert_eq!(res_std["error"], "audit_archive_required");
            } else {
                assert_eq!(st_std, 200);
                assert_eq!(p_std.run_job(res_std["job"].clone())["status"], "complete");
            }

            let p_ptd = Platform::new();
            p_ptd.provision_logging_service();
            let en_ptd = p_ptd.ok(
                "POST",
                "/api/services/fixtureService/scoring/enable",
                json!({}),
            )["job"]
                .clone();
            assert_eq!(p_ptd.run_job(en_ptd)["status"], "complete");
            let gen_ptd = seed_outbox(&p_ptd, count);
            let (st_ptd, res_ptd) = p_ptd.call(
                "POST",
                "/api/admin/provision/teardown",
                json!({
                    "service_id": "fixtureService",
                    "confirm_service_id": "fixtureService",
                    "generation": gen_ptd,
                    "remove_logging": true,
                    "remove_snippets": true,
                    "delete_fos_keys": true,
                    "delete_data": true,
                    "delete_fos_bucket": true
                }),
            );
            if count >= 4096 {
                assert_eq!(st_ptd, 409);
                assert_eq!(res_ptd["error"], "audit_archive_required");
            } else {
                assert_eq!(st_ptd, 200);
                assert_eq!(p_ptd.run_job(res_ptd["job"].clone())["status"], "complete");
            }

            let p_rec = Platform::new();
            p_rec.provision_logging_service();
            let gen_rec = seed_outbox(&p_rec, count);
            let (st_rec, res_rec) = p_rec.call(
                "POST",
                "/api/admin/services/fixtureService/reconcile",
                json!({"generation": gen_rec}),
            );
            if count >= 4096 {
                assert_eq!(st_rec, 409);
                assert_eq!(res_rec["error"], "audit_archive_required");
            } else {
                assert_eq!(st_rec, 200);
                assert_eq!(p_rec.run_job(res_rec["job"].clone())["status"], "complete");
            }
        }
    }

    // 2. Post-activate KV failure in strip_logging_activate_step:
    // Once PUT .../version/4/activate succeeds on Fastly, transient state_write_failed on svc:fixtureService
    // must NOT clear draft_version or rewind to scoring_strip_logging_vcl, and /retry must complete at active == 4
    // without cloning a duplicate v5!
    let p_kv = Platform::new();
    p_kv.provision_logging_service();
    let en_kv = p_kv.ok(
        "POST",
        "/api/services/fixtureService/scoring/enable",
        json!({}),
    )["job"]
        .clone();
    assert_eq!(p_kv.run_job(en_kv)["status"], "complete");
    let mut dis_kv = p_kv.ok(
        "POST",
        "/api/services/fixtureService/scoring/disable",
        json!({}),
    )["job"]
        .clone();
    let dis_kv_id = dis_kv["id"].as_str().unwrap().to_owned();
    for _ in 0..20 {
        if dis_kv["step"] == "scoring_strip_activate" {
            break;
        }
        let (st, res) = p_kv.call(
            "POST",
            &format!("/api/admin/jobs/{dis_kv_id}/step"),
            json!({}),
        );
        assert_eq!(st, 200, "{res}");
        dis_kv = res["job"].clone();
    }
    assert_eq!(dis_kv["step"], "scoring_strip_activate");
    assert_eq!(dis_kv["draft_version"], 4);

    p_kv.fail_svc_cas.store(true, Ordering::SeqCst);
    let (fail_st, fail_res) = p_kv.call(
        "POST",
        &format!("/api/admin/jobs/{dis_kv_id}/step"),
        json!({}),
    );
    p_kv.fail_svc_cas.store(false, Ordering::SeqCst);
    assert_eq!(fail_st, 200, "{fail_res}");
    assert_eq!(fail_res["job"]["status"], "failed", "{fail_res}");
    assert_eq!(fail_res["job"]["error"], "state_write_failed", "{fail_res}");
    assert_eq!(
        fail_res["job"]["step"], "scoring_strip_activate",
        "post-activate KV failure must NOT rewind step to scoring_strip_logging_vcl: {fail_res}"
    );
    assert_eq!(
        fail_res["job"]["draft_version"], 4,
        "post-activate KV failure must preserve draft_version=4: {fail_res}"
    );

    let retried_kv = p_kv.ok(
        "POST",
        &format!("/api/admin/jobs/{dis_kv_id}/retry"),
        json!({}),
    )["job"]
        .clone();
    let done_kv = p_kv.run_job(retried_kv);
    assert_eq!(done_kv["status"], "complete", "{done_kv}");
    let (_, vers_after) = p_kv
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
    let active_after = vers_after
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["active"] == true)
        .and_then(|v| v["number"].as_u64())
        .unwrap();
    let max_ver_after = vers_after
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|v| v["number"].as_u64())
        .max()
        .unwrap();
    assert_eq!(
        active_after, 4,
        "retry after post-activate KV failure must keep v4 active without cloning duplicate v5: {vers_after}"
    );
    assert_eq!(
        max_ver_after, 4,
        "no duplicate v5 draft may be cloned after v4 was already activated: {vers_after}"
    );
}

#[test]
fn bug_cloud_dr_21c_rejects_patch_reconcile_and_teardown_during_active_delete_data() {
    let p = Platform::new();
    p.provision_logging_service();

    let pre_dd_gen = p
        .adapter
        .record_snapshot("svc:fixtureService")
        .unwrap()
        .generation;
    let (dd_st, dd_res) = p.call(
        "POST",
        "/api/admin/services/fixtureService/delete-data",
        json!({"confirm_service_id": "fixtureService", "generation": pre_dd_gen}),
    );
    assert_eq!(dd_st, 200, "delete-data start must succeed: {dd_res}");
    let snap = p.adapter.record_snapshot("svc:fixtureService").unwrap();
    assert!(
        !snap.value["data_reset"].is_null(),
        "data_reset marker must be active: {}",
        snap.value
    );
    let gen = snap.generation;

    // 1. PATCH /api/admin/services/fixtureService attempting fos_prefix redirection -> 409 data_reset_in_progress
    let (patch_st, patch_res) = p.call(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation": gen, "fos_prefix": "team_b/"}),
    );
    assert_eq!(
        patch_st, 409,
        "PATCH /services during delete-data must return 409, got {patch_st}: {patch_res}"
    );
    assert_eq!(patch_res["error"], "data_reset_in_progress");
    assert_eq!(
        p.adapter
            .record_snapshot("svc:fixtureService")
            .unwrap()
            .value["fos_prefix"],
        "",
        "fos_prefix must never be mutated to team_b/ during delete-data"
    );

    // 2. PATCH /api/admin/log-fields/config -> 409 data_reset_in_progress
    let (lf_st, lf_res) = p.call(
        "PATCH",
        "/api/admin/log-fields/config",
        json!({
            "service_id": "fixtureService",
            "generation": gen,
            "log_fields": {"groups": ["A"], "custom_fields": [], "field_overrides": {}, "field_limits": {}}
        }),
    );
    assert_eq!(lf_st, 409, "{lf_res}");
    assert_eq!(lf_res["error"], "data_reset_in_progress");

    // 3. POST /api/admin/services/fixtureService/reconcile -> 409 data_reset_in_progress
    let (rec_st, rec_res) = p.call(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation": gen}),
    );
    assert_eq!(
        rec_st, 409,
        "POST /reconcile during delete-data must return 409, got {rec_st}: {rec_res}"
    );
    assert_eq!(rec_res["error"], "data_reset_in_progress");

    // 4. POST /api/admin/provision/teardown -> 409 data_reset_in_progress
    let (td_st, td_res) = p.call(
        "POST",
        "/api/admin/provision/teardown",
        json!({
            "service_id": "fixtureService",
            "confirm_service_id": "fixtureService",
            "generation": gen,
            "remove_logging": true,
            "remove_snippets": true,
            "delete_fos_keys": false,
            "delete_data": false,
            "delete_fos_bucket": false
        }),
    );
    assert_eq!(
        td_st, 409,
        "POST /provision/teardown during delete-data must return 409, got {td_st}: {td_res}"
    );
    assert_eq!(td_res["error"], "data_reset_in_progress");

    // 4b. Auditor Challenge 2: POST /api/admin/provision/execute and POST /api/admin/provision/recover
    // during active delete-data must ALSO fail closed with 409 data_reset_in_progress!
    let (prov_st, prov_res) = p.call(
        "POST",
        "/api/admin/provision/execute",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1",
            "log_fields": {"groups":["A","C"],"custom_fields":[],"field_overrides":{},"field_limits":{}}
        }),
    );
    assert_eq!(
        prov_st, 409,
        "POST /provision/execute during delete-data must return 409, got {prov_st}: {prov_res}"
    );
    assert_eq!(prov_res["error"], "data_reset_in_progress");

    let (dr_st, dr_res) = p.call(
        "POST",
        "/api/admin/provision/recover",
        json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_bucket": "fos-fixture-logs",
            "fos_region": "us-east-1"
        }),
    );
    assert_eq!(
        dr_st, 409,
        "POST /provision/recover during delete-data must return 409, got {dr_st}: {dr_res}"
    );
    assert_eq!(dr_res["error"], "data_reset_in_progress");

    // 5. GET & cancel delete-data still work cleanly, and once cancelled, PATCH succeeds again:
    let (get_dd_st, _) = p.call(
        "GET",
        "/api/admin/services/fixtureService/delete-data",
        json!({}),
    );
    assert_eq!(get_dd_st, 200);
    let (cancel_st, _) = p.call(
        "POST",
        "/api/admin/services/fixtureService/delete-data/cancel",
        json!({}),
    );
    assert_eq!(cancel_st, 200);
    let post_cancel_gen = p
        .adapter
        .record_snapshot("svc:fixtureService")
        .unwrap()
        .generation;
    let (post_patch_st, _) = p.call(
        "PATCH",
        "/api/admin/services/fixtureService",
        json!({"generation": post_cancel_gen, "sample_rate": 50}),
    );
    assert_eq!(post_patch_st, 200);
}

#[test]
fn bug_cloud_dr_24a_front_clone_tight_budget_never_self_poisons_provision() {
    for init_used in 15..=17u32 {
        let p = Platform::new();
        let start = p.ok(
            "POST",
            "/api/admin/provision/execute",
            json!({
                "token": "fixture-token",
                "service_id": "fixtureService",
                "fos_bucket": "fos-fixture-logs",
                "fos_region": "eu-central-1",
                "log_fields": {"groups":["A","C"],"custom_fields":[],"field_overrides":{},"field_limits":{}}
            }),
        );
        let mut job = start["job"].clone();
        let job_id = job["id"].as_str().unwrap().to_owned();
        while job["step"] != "front" {
            let (st, res) = p.call("POST", &format!("/api/admin/jobs/{job_id}/step"), json!({}));
            assert_eq!(st, 200, "init_used={init_used}: {res}");
            job = res["job"].clone();
        }
        // Advance front cursor 0 -> 1 and record front_clone_intent, then run constrained ticks at cursor 1:
        for tick in 0..5u32 {
            let (st, res) = p.call_with_initial_used(
                init_used,
                "POST",
                &format!("/api/admin/jobs/{job_id}/step"),
                json!({}),
            );
            assert_eq!(st, 200, "init_used={init_used} tick={tick}: {res}");
            job = res["job"].clone();
            assert_ne!(
                job["error"],
                json!("uncheckpointed_front_clone_requires_review"),
                "BUG-CLOUD-DR-24A: provision self-poisoned at init_used={init_used}: {job}"
            );
            assert_ne!(
                job["status"], "failed",
                "BUG-CLOUD-DR-24A: provision must not fail under tight front clone budget init_used={init_used}: {job}"
            );
            if let Some(at) = job["retry_at"].as_u64() {
                p.clock.fetch_max(at + 1, Ordering::SeqCst);
            }
            if job["step"] != "front" || job["status"] == "complete" {
                break;
            }
        }
        let done = p.run_job(job);
        assert_eq!(
            done["status"], "complete",
            "BUG-CLOUD-DR-24A: provision must complete cleanly after tight front budget init_used={init_used}: {done}"
        );
    }
}

#[test]
fn bug_cloud_dr_24b_scoring_tight_budget_backoff_and_zero_redundant_outbox_checkpoints() {
    let seed_outbox = |p: &Platform, count: usize| {
        let snap = p.adapter.record_snapshot("svc:fixtureService").unwrap();
        let mut val = snap.value.clone();
        val["_audit_outbox"] = Value::Array(
            (0..count)
                .map(|i| {
                    json!({
                        "actor": "fixtureAdmin",
                        "at": 1_800_000_000u64 + i as u64,
                        "event": "seed",
                        "config_version": 1
                    })
                })
                .collect(),
        );
        p.adapter.cache_invalidate("svc:fixtureService");
        p.adapter
            .cas("svc:fixtureService", Some(&snap.generation), &val)
            .unwrap();
    };

    // 1. scorer_stores at outbox == 4094 and init_used == 16 (rem == 16):
    // Once keys_store exists, constrained ticks must NOT repeatedly checkpoint svc:fixtureService or spin with retry_at == null!
    let p_st = Platform::new();
    p_st.provision_logging_service();
    seed_outbox(&p_st, 4094);
    let mut job_st = p_st.ok(
        "POST",
        "/api/services/fixtureService/scoring/enable",
        json!({}),
    )["job"]
        .clone();
    let job_st_id = job_st["id"].as_str().unwrap().to_owned();
    while job_st["step"] != "scorer_stores" {
        job_st = p_st.ok(
            "POST",
            &format!("/api/admin/jobs/{job_st_id}/step"),
            json!({}),
        )["job"]
            .clone();
    }
    // Pre-create only keys_store so ensure_scorer_stores_step hits the cfg_store `< 9` guard:
    let sc_sid = p_st
        .adapter
        .record_snapshot("svc:fixtureService")
        .unwrap()
        .value["scoring"]["scoring_service_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let _ = p_st
        .adapter
        .api(ApiRequest {
            method: "POST",
            path: "/resources/stores/config",
            query: "",
            token: "fixture-token",
            body: Some(json!({"name": format!("scoring_keys_{sc_sid}")})),
            timeout: std::time::Duration::from_secs(5),
        })
        .unwrap();
    let mut null_retry_ticks = 0u32;
    for _ in 0..8u32 {
        let (st, res) = p_st.call_with_initial_used(
            16,
            "POST",
            &format!("/api/admin/jobs/{job_st_id}/step"),
            json!({}),
        );
        assert_eq!(st, 200, "{res}");
        job_st = res["job"].clone();
        assert_ne!(
            job_st["status"], "failed",
            "BUG-CLOUD-DR-24B: scorer_stores must never burn outbox to 4112 and crash: {job_st}"
        );
        if job_st["retry_at"].is_null() {
            null_retry_ticks += 1;
        } else if let Some(at) = job_st["retry_at"].as_u64() {
            p_st.clock.fetch_max(at + 1, Ordering::SeqCst);
        }
    }
    assert!(
        null_retry_ticks <= 1,
        "BUG-CLOUD-DR-24B: scorer_stores may checkpoint discovered keys_store at most once, then must set retry_at backoff (null_retry_ticks={null_retry_ticks}): {job_st}"
    );
    let outbox_after_constrained = p_st
        .adapter
        .record_snapshot("svc:fixtureService")
        .unwrap()
        .value["_audit_outbox"]
        .as_array()
        .unwrap()
        .len();
    assert!(
        outbox_after_constrained <= 4098,
        "BUG-CLOUD-DR-24B: scorer_stores must not burn _audit_outbox on no-op ticks (got {outbox_after_constrained})"
    );
    assert_eq!(p_st.run_job(job_st)["status"], "complete");

    // 2. scorer_deploy at init_used == 13 (rem == 19) must back off via retry_at instead of spinning at retry_at == null:
    let p_dep = Platform::new();
    p_dep.provision_logging_service();
    let mut job_dep = p_dep.ok(
        "POST",
        "/api/services/fixtureService/scoring/enable",
        json!({}),
    )["job"]
        .clone();
    let job_dep_id = job_dep["id"].as_str().unwrap().to_owned();
    while job_dep["step"] != "scorer_deploy" {
        job_dep = p_dep.ok(
            "POST",
            &format!("/api/admin/jobs/{job_dep_id}/step"),
            json!({}),
        )["job"]
            .clone();
    }
    let mut dep_null_streak = 0u32;
    for _ in 0..5u32 {
        let (st, res) = p_dep.call_with_initial_used(
            13,
            "POST",
            &format!("/api/admin/jobs/{job_dep_id}/step"),
            json!({}),
        );
        assert_eq!(st, 200, "{res}");
        job_dep = res["job"].clone();
        if job_dep["step"] == "scorer_deploy" && job_dep["retry_at"].is_null() {
            dep_null_streak += 1;
        } else if let Some(at) = job_dep["retry_at"].as_u64() {
            p_dep.clock.fetch_max(at + 1, Ordering::SeqCst);
        }
    }
    assert!(
        dep_null_streak <= 1,
        "BUG-CLOUD-DR-24B: scorer_deploy spun with retry_at=null (streak={dep_null_streak}): {job_dep}"
    );
    assert_eq!(p_dep.run_job(job_dep)["status"], "complete");

    // 3. scoring_cleanup_state at init_used == 14 (rem == 18) must back off via retry_at instead of spinning:
    let p_cl = Platform::new();
    p_cl.provision_logging_service();
    let en_cl = p_cl.ok(
        "POST",
        "/api/services/fixtureService/scoring/enable",
        json!({}),
    )["job"]
        .clone();
    assert_eq!(p_cl.run_job(en_cl)["status"], "complete");
    let gen_cl = p_cl
        .adapter
        .record_snapshot("svc:fixtureService")
        .unwrap()
        .generation;
    let mut job_cl = p_cl.ok(
        "POST",
        "/api/services/fixtureService/scoring/teardown",
        json!({"confirm_service_id": "fixtureService", "generation": gen_cl}),
    )["job"]
        .clone();
    let job_cl_id = job_cl["id"].as_str().unwrap().to_owned();
    while job_cl["step"] != "scoring_cleanup_state" {
        job_cl = p_cl.ok(
            "POST",
            &format!("/api/admin/jobs/{job_cl_id}/step"),
            json!({}),
        )["job"]
            .clone();
    }
    let mut cl_null_streak = 0u32;
    for _ in 0..5u32 {
        let (st, res) = p_cl.call_with_initial_used(
            14,
            "POST",
            &format!("/api/admin/jobs/{job_cl_id}/step"),
            json!({}),
        );
        assert_eq!(st, 200, "{res}");
        job_cl = res["job"].clone();
        if job_cl["step"] == "scoring_cleanup_state" && job_cl["retry_at"].is_null() {
            cl_null_streak += 1;
        } else if let Some(at) = job_cl["retry_at"].as_u64() {
            p_cl.clock.fetch_max(at + 1, Ordering::SeqCst);
        }
    }
    assert!(
        cl_null_streak <= 1,
        "BUG-CLOUD-DR-24B: scoring_cleanup_state spun with retry_at=null (streak={cl_null_streak}): {job_cl}"
    );
    assert_eq!(p_cl.run_job(job_cl)["status"], "complete");
}

#[test]
fn bug_cloud_dr_24e_synchronous_scoring_routes_reject_full_outbox_before_external_side_effects() {
    let p = Platform::new();
    p.provision_logging_service();

    // Seed outbox == 4095 and run scoring_enable to completion so post-job outbox >= 4096 (normal post-job state!):
    let snap = p.adapter.record_snapshot("svc:fixtureService").unwrap();
    let mut val = snap.value.clone();
    val["_audit_outbox"] = Value::Array(
        (0..4095)
            .map(|i| json!({"actor":"fixtureAdmin","at":1_800_000_000u64 + i as u64,"event":"seed","config_version":1}))
            .collect(),
    );
    p.adapter.cache_invalidate("svc:fixtureService");
    p.adapter
        .cas("svc:fixtureService", Some(&snap.generation), &val)
        .unwrap();

    let en_job = p.ok(
        "POST",
        "/api/services/fixtureService/scoring/enable",
        json!({}),
    )["job"]
        .clone();
    assert_eq!(p.run_job(en_job)["status"], "complete");

    // Seed a stored historical matrix v-prev in S3 so /restore would find it if it reached S3:
    p.adapter.seed_object_bytes(
        "fos-fixture-logs",
        "processed/scoring/matrices/v-prev.json",
        serde_json::to_vec(&json!({
            "version": "v-prev",
            "vocab_size": 1,
            "session_count": 1,
            "transition_count": 1,
            "counts": {"/": {"/": 1}},
            "row_totals": {"/": 1}
        }))
        .unwrap(),
    );

    let post_en_snap = p.adapter.record_snapshot("svc:fixtureService").unwrap();
    let outbox_len = post_en_snap.value["_audit_outbox"]
        .as_array()
        .unwrap()
        .len();
    assert!(
        outbox_len >= 4096,
        "expected outbox >= 4096, got {outbox_len}"
    );
    let matrix_store_id = post_en_snap.value["scoring"]["scoring_matrix_store_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let matrix_kv_before = p.adapter.kv_store_value(&matrix_store_id, "matrix");

    let (_, vers_before) = p
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
    let max_ver_before = vers_before
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|v| v["number"].as_u64())
        .max()
        .unwrap();

    for (label, method, path, body) in [
        (
            "threshold",
            "PUT",
            "/api/services/fixtureService/scoring/threshold",
            json!({"threshold": 77}),
        ),
        (
            "exclude-regex",
            "PUT",
            "/api/services/fixtureService/scoring/exclude-regex?confirm=true",
            json!({"exclude_url_regex": r"\.(pdf|zip)$"}),
        ),
        (
            "enforce-status-code",
            "PUT",
            "/api/services/fixtureService/scoring/enforce-status-code?confirm=true",
            json!({"status_code": 429}),
        ),
        (
            "retrain",
            "POST",
            "/api/services/fixtureService/scoring/retrain",
            json!({
                "matrix": {
                    "version": "v-desync-1",
                    "vocab_size": 1,
                    "session_count": 1,
                    "transition_count": 1,
                    "counts": {"/": {"/": 1}},
                    "row_totals": {"/": 1}
                }
            }),
        ),
        (
            "restore",
            "POST",
            "/api/services/fixtureService/scoring/matrix/versions/restore?confirm=true",
            json!({"version": "v-prev"}),
        ),
    ] {
        let (st, res) = p.call(method, path, body);
        assert_eq!(st, 409, "BUG-CLOUD-DR-24E [{label}]: {res}");
        assert_eq!(
            res["error"], "audit_archive_required",
            "BUG-CLOUD-DR-24E [{label}]: {res}"
        );
    }

    let (_, vers_after) = p
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
    let max_ver_after = vers_after
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|v| v["number"].as_u64())
        .max()
        .unwrap();
    assert_eq!(
        max_ver_after, max_ver_before,
        "BUG-CLOUD-DR-24E: customer Fastly VCL must NOT be cloned/activated when outbox >= 4096"
    );
    assert_eq!(
        p.adapter.kv_store_value(&matrix_store_id, "matrix"),
        matrix_kv_before,
        "BUG-CLOUD-DR-24E: scoring_matrix KV must NOT be overwritten before 409 audit_archive_required"
    );
    assert!(
        !p.adapter
            .object_keys("fos-fixture-logs")
            .iter()
            .any(|k| k == "processed/scoring/matrices/v-desync-1.json"),
        "BUG-CLOUD-DR-24E: S3 matrices must NOT be written before 409 audit_archive_required"
    );

    for (label, method, path, body) in [
        (
            "disable-with-token",
            "POST",
            "/api/services/fixtureService/scoring/disable",
            json!({"token": "rotated-should-not-save-token"}),
        ),
        (
            "teardown-with-token",
            "POST",
            "/api/services/fixtureService/scoring/teardown",
            json!({
                "confirm_service_id": "fixtureService",
                "generation": post_en_snap.generation,
                "token": "rotated-should-not-save-token"
            }),
        ),
    ] {
        let (st, res) = p.call(method, path, body);
        assert_eq!(st, 409, "BUG-CLOUD-DR-24E [{label}]: {res}");
        assert_eq!(
            res["error"], "audit_archive_required",
            "BUG-CLOUD-DR-24E [{label}]: {res}"
        );
    }
    assert_eq!(
        p.adapter
            .secret("fastly_token.fixtureService")
            .unwrap()
            .as_deref(),
        Some("fixture-token"),
        "BUG-CLOUD-DR-24E: fastly_token.fixtureService must NOT be overwritten before 409 audit_archive_required"
    );
}

#[test]
fn bug_cloud_dr_24c_delete_data_torn_down_reconcile_lock_superseded_job_and_cancel_at_4095() {
    // 1. Torn-down service (both delete_fos_keys = true and false) rejects POST /delete-data with 409 service_torn_down
    // WITHOUT writing svc:fixtureService.data_reset, so re-provisioning succeeds!
    for delete_fos_keys in [true, false] {
        let p = Platform::new();
        p.provision_logging_service();
        let gen = p
            .adapter
            .record_snapshot("svc:fixtureService")
            .unwrap()
            .generation;
        let td_job = p.ok(
            "POST",
            "/api/admin/provision/teardown",
            json!({
                "service_id": "fixtureService",
                "confirm_service_id": "fixtureService",
                "generation": gen,
                "remove_logging": true,
                "remove_snippets": true,
                "delete_fos_keys": delete_fos_keys,
                "delete_data": false,
                "delete_fos_bucket": false
            }),
        )["job"]
            .clone();
        assert_eq!(p.run_job(td_job)["status"], "complete");
        let td_snap = p.adapter.record_snapshot("svc:fixtureService").unwrap();
        assert_eq!(td_snap.value["status"], "torn_down");
        let (dd_st, dd_res) = p.call(
            "POST",
            "/api/admin/services/fixtureService/delete-data",
            json!({"confirm_service_id": "fixtureService", "generation": td_snap.generation}),
        );
        assert_eq!(
            dd_st, 409,
            "delete_fos_keys={delete_fos_keys}: POST /delete-data on torn_down must return 409: {dd_res}"
        );
        assert_eq!(
            dd_res["error"], "service_torn_down",
            "delete_fos_keys={delete_fos_keys}: {dd_res}"
        );
        assert!(
            p.adapter
                .record_snapshot("svc:fixtureService")
                .unwrap()
                .value
                .get("data_reset")
                .is_none_or(Value::is_null),
            "delete_fos_keys={delete_fos_keys}: torn_down delete-data must NOT poison svc.data_reset"
        );
        let (reprov_st, reprov_res) = p.call(
            "POST",
            "/api/admin/provision/execute",
            json!({
                "token": "fixture-token",
                "service_id": "fixtureService",
                "fos_bucket": "fos-fixture-logs",
                "fos_region": "us-east-1",
                "log_fields": {"groups":["A","C"],"custom_fields":[],"field_overrides":{},"field_limits":{}}
            }),
        );
        assert_eq!(
            reprov_st, 200,
            "delete_fos_keys={delete_fos_keys}: re-provision after rejected delete-data must succeed: {reprov_res}"
        );
    }

    // 2. Active reconcile_lock blocks POST /delete-data with 409 service_locked, while terminal failed reconcile is superseded:
    let p_lock = Platform::new();
    p_lock.provision_logging_service();
    let lock_old = p_lock
        .adapter
        .record_snapshot("reconcile_lock:fixtureService")
        .map(|r| r.generation);
    p_lock
        .adapter
        .cas(
            "reconcile_lock:fixtureService",
            lock_old.as_deref(),
            &json!({"job_id": "active-step", "fence": 99, "expires_at": 1_800_000_060u64}),
        )
        .unwrap();
    let gen_lock = p_lock
        .adapter
        .record_snapshot("svc:fixtureService")
        .unwrap()
        .generation;
    let (lk_st, lk_res) = p_lock.call(
        "POST",
        "/api/admin/services/fixtureService/delete-data",
        json!({"confirm_service_id": "fixtureService", "generation": gen_lock}),
    );
    assert_eq!(
        lk_st, 409,
        "active reconcile_lock must block delete-data: {lk_res}"
    );
    assert_eq!(lk_res["error"], "service_locked");

    // Expire lock and seed a terminal failed reconcile job on svc.pending_job_id -> delete-data supersedes it (200 OK)!
    let lock_cur = p_lock
        .adapter
        .record_snapshot("reconcile_lock:fixtureService")
        .unwrap();
    p_lock
        .adapter
        .cas(
            "reconcile_lock:fixtureService",
            Some(&lock_cur.generation),
            &json!({"job_id": "active-step", "fence": 99, "expires_at": 0}),
        )
        .unwrap();
    let started_rec = p_lock.ok(
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        json!({"generation": gen_lock}),
    );
    let rec_job_id = started_rec["job"]["id"].as_str().unwrap().to_owned();
    let rec_job_key = format!("job:{rec_job_id}");
    let rec_job_snap = p_lock.adapter.record_snapshot(&rec_job_key).unwrap();
    let mut failed_job_val = rec_job_snap.value.clone();
    failed_job_val["status"] = json!("failed");
    failed_job_val["error"] = json!("active_version_changed");
    p_lock.adapter.cache_invalidate(&rec_job_key);
    p_lock
        .adapter
        .cas(
            &rec_job_key,
            Some(&rec_job_snap.generation),
            &failed_job_val,
        )
        .unwrap();
    let gen_sup = p_lock
        .adapter
        .record_snapshot("svc:fixtureService")
        .unwrap()
        .generation;
    let (sup_st, sup_res) = p_lock.call(
        "POST",
        "/api/admin/services/fixtureService/delete-data",
        json!({"confirm_service_id": "fixtureService", "generation": gen_sup}),
    );
    assert_eq!(
        sup_st, 200,
        "terminal failed reconcile job must not wedge POST /delete-data: {sup_res}"
    );

    // 3. Outbox == 4095 start (200 OK -> outbox 4096) followed by immediate POST /delete-data/cancel MUST succeed 200 OK and clear svc.data_reset!
    let p_ob = Platform::new();
    p_ob.provision_logging_service();
    let ob_snap = p_ob.adapter.record_snapshot("svc:fixtureService").unwrap();
    let mut ob_val = ob_snap.value.clone();
    ob_val["_audit_outbox"] = Value::Array(
        (0..4095)
            .map(|i| json!({"actor":"fixtureAdmin","at":1_800_000_000u64 + i as u64,"event":"seed","config_version":1}))
            .collect(),
    );
    p_ob.adapter.cache_invalidate("svc:fixtureService");
    p_ob.adapter
        .cas("svc:fixtureService", Some(&ob_snap.generation), &ob_val)
        .unwrap();
    let gen_4095 = p_ob
        .adapter
        .record_snapshot("svc:fixtureService")
        .unwrap()
        .generation;
    let (st_4095, res_4095) = p_ob.call(
        "POST",
        "/api/admin/services/fixtureService/delete-data",
        json!({"confirm_service_id": "fixtureService", "generation": gen_4095}),
    );
    assert_eq!(st_4095, 200, "{res_4095}");
    let (cancel_4095_st, cancel_4095_res) = p_ob.call(
        "POST",
        "/api/admin/services/fixtureService/delete-data/cancel",
        json!({}),
    );
    assert_eq!(
        cancel_4095_st, 200,
        "BUG-CLOUD-DR-24C: delete-data/cancel after start at outbox=4095 must succeed: {cancel_4095_res}"
    );
    assert!(
        p_ob.adapter
            .record_snapshot("svc:fixtureService")
            .unwrap()
            .value
            .get("data_reset")
            .is_none_or(Value::is_null),
        "BUG-CLOUD-DR-24C: svc.data_reset must be cleared by synchronous cancel at outbox=4096"
    );
}
