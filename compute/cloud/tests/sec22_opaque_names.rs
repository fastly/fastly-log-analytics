use fastly_log_analytics::{
    admin::{
        self,
        fixture::FixtureAdapter,
        scoring::{opaque_scoring_domain, valid_scoring_domain},
        ApiRawRequest, ApiRequest, FosRequest, FosResponse, Record,
    },
    auth::{self, AuthState, Credential, Role, User},
    http::Request,
    setup::SetupStores,
    shell::{Runtime, Shell},
    state::{MemoryStore, StateStore},
};
use hmac::{Hmac, KeyInit, Mac};
use serde_json::{json, Value};
use sha2::Sha256;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Mutex,
    },
};

struct Sec22Platform {
    adapter: FixtureAdapter,
    clock: AtomicU64,
    random_counter: AtomicU64,
    bucket_put_409_remaining: AtomicUsize,
    scorer_domain_409_remaining: AtomicUsize,
    fos_buckets_seen_on_put: Mutex<Vec<String>>,
    kv_bucket_before_fos_put: Mutex<Vec<String>>,
    scorer_domains_seen_on_post: Mutex<Vec<String>>,
    kv_scorer_domain_before_post: Mutex<Vec<String>>,
}

impl Sec22Platform {
    fn new() -> Self {
        Self {
            adapter: FixtureAdapter::new(),
            clock: AtomicU64::new(1_800_000_000),
            random_counter: AtomicU64::new(1),
            bucket_put_409_remaining: AtomicUsize::new(0),
            scorer_domain_409_remaining: AtomicUsize::new(0),
            fos_buckets_seen_on_put: Mutex::new(Vec::new()),
            kv_bucket_before_fos_put: Mutex::new(Vec::new()),
            scorer_domains_seen_on_post: Mutex::new(Vec::new()),
            kv_scorer_domain_before_post: Mutex::new(Vec::new()),
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

    fn drain_job(&self, mut job: Value) -> Value {
        let id = job["id"].as_str().unwrap().to_owned();
        for _ in 0..80 {
            if matches!(
                job["status"].as_str(),
                Some("complete" | "failed" | "cancelled")
            ) {
                return job;
            }
            if let Some(at) = job["retry_at"].as_u64() {
                self.clock.fetch_max(at, Ordering::SeqCst);
            }
            let (status, resp) =
                self.call("POST", &format!("/api/admin/jobs/{id}/step"), json!({}));
            assert_eq!(status, 200, "advance failed: {status} {resp}");
            job = resp["job"].clone();
        }
        panic!("job {id} did not finish: {job}");
    }

    fn provision_body(extra: Value) -> Value {
        let mut body = json!({
            "token": "fixture-token",
            "service_id": "fixtureService",
            "fos_region": "us-east-1",
            "fos_prefix": "",
            "logging_endpoint_name": "Fastly Object Storage Logs",
            "log_period": 60,
            "sample_rate": 100,
            "edge_only": true,
            "log_fields": {"groups":["A","C"],"custom_fields":[],"field_overrides":{},"field_limits":{}},
        });
        if let Some(obj) = extra.as_object() {
            for (k, v) in obj {
                body[k] = v.clone();
            }
        }
        body
    }
}

impl SetupStores for Sec22Platform {
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

impl Runtime for Sec22Platform {
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
                "compute_host" => "fla-cccccccccccccccccccccccc.edgecompute.app",
                "worker_host" => "fla-dddddddddddddddddddddddd.edgecompute.app",
                _ => "fixtureValue",
            }
            .into(),
        ))
    }

    fn secret(&self, name: &str) -> Result<Option<String>, String> {
        match name {
            "cdn_key" => Ok(Some("fixture-cdn-key".into())),
            "front_key" => Ok(Some("fixture-front-key".into())),
            "session_key" => Ok(Some("0123456789abcdef0123456789abcdef".into())),
            "fastly_token" => Ok(Some("fixture-token".into())),
            _ => self.adapter.secret(name),
        }
    }

    fn random(&self) -> Result<[u8; 32], String> {
        let n = self.random_counter.fetch_add(1, Ordering::SeqCst);
        let mut out = [0u8; 32];
        out[..8].copy_from_slice(&n.to_le_bytes());
        out[8..16].copy_from_slice(&(n.wrapping_mul(17) ^ 0xa5a5_5a5a).to_le_bytes());
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
        self.adapter.confirm(key)
    }

    fn admin_cas(
        &self,
        key: &str,
        generation: Option<&str>,
        value: &Value,
    ) -> Result<bool, String> {
        self.adapter.cache_invalidate(key);
        self.adapter.cas(key, generation, value)
    }

    fn admin_api(&self, request: ApiRequest<'_>) -> Result<admin::ApiResponse, String> {
        if request.method == "POST" && request.path.ends_with("/version/1/domain") {
            if let Some(domain) = request
                .body
                .as_ref()
                .and_then(|b| b["name"].as_str())
                .map(str::to_owned)
            {
                self.scorer_domains_seen_on_post
                    .lock()
                    .unwrap()
                    .push(domain);
                let persisted = self
                    .adapter
                    .get("svc:fixtureService")
                    .ok()
                    .flatten()
                    .and_then(|r| {
                        r.value["scoring"]["scoring_domain"]
                            .as_str()
                            .map(str::to_owned)
                    })
                    .unwrap_or_default();
                self.kv_scorer_domain_before_post
                    .lock()
                    .unwrap()
                    .push(persisted);
            }
            let rem = self.scorer_domain_409_remaining.load(Ordering::SeqCst);
            if rem > 0 {
                self.scorer_domain_409_remaining
                    .fetch_sub(1, Ordering::SeqCst);
                return Ok(admin::ApiResponse {
                    status: 409,
                    body: json!({"msg": "Domain already taken"}),
                    retry_after: None,
                });
            }
        }
        self.adapter.api(request).map(Into::into)
    }

    fn admin_api_raw(&self, request: ApiRawRequest<'_>) -> Result<admin::ApiResponse, String> {
        self.adapter.api_raw(request).map(Into::into)
    }

    fn admin_fos(&self, request: FosRequest<'_>) -> Result<FosResponse, String> {
        if request.method == "PUT" && request.key.is_empty() {
            self.fos_buckets_seen_on_put
                .lock()
                .unwrap()
                .push(request.bucket.to_owned());
            let persisted = self
                .adapter
                .get("svc:fixtureService")
                .ok()
                .flatten()
                .and_then(|r| r.value["fos_bucket"].as_str().map(str::to_owned))
                .unwrap_or_default();
            self.kv_bucket_before_fos_put
                .lock()
                .unwrap()
                .push(persisted);
            let rem = self.bucket_put_409_remaining.load(Ordering::SeqCst);
            if rem > 0 {
                self.bucket_put_409_remaining.fetch_sub(1, Ordering::SeqCst);
                return Ok(FosResponse {
                    status: 409,
                    body: Vec::new(),
                    cdn_cache: None,
                    retry_after: None,
                });
            }
        }
        self.adapter.fos(request)
    }

    fn kv_list(
        &self,
        prefix: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<fastly_log_analytics::shell::KvPage, String> {
        Ok(fastly_log_analytics::shell::KvPage::from_sorted_keys(
            self.adapter.records().into_keys(),
            prefix,
            cursor,
            limit,
        ))
    }

    fn kv_delete(&self, key: &str) -> Result<(), String> {
        self.adapter.cache_invalidate(key);
        self.adapter.delete(key)
    }

    fn scorer_package(&self) -> Option<Vec<u8>> {
        Some(b"fixture-session-scorer-wasm-tar-gz".to_vec())
    }
}

fn is_opaque_bucket(name: &str) -> bool {
    name.starts_with("fla-")
        && name.len() == 28
        && name.len() <= 63
        && name[4..]
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

fn is_opaque_scorer_domain(domain: &str) -> bool {
    valid_scoring_domain(domain)
        && domain.starts_with("fla-")
        && domain.ends_with(".edgecompute.app")
        && domain.len() == "fla-".len() + 24 + ".edgecompute.app".len()
}

#[test]
fn sec22_opaque_bucket_and_scorer_domain_generators_have_no_correlation() {
    let platform = Sec22Platform::new();
    let b1 = admin::opaque_bucket_name(&platform).unwrap();
    let b2 = admin::opaque_bucket_name(&platform).unwrap();
    assert!(is_opaque_bucket(&b1), "unexpected bucket: {b1}");
    assert!(is_opaque_bucket(&b2), "unexpected bucket: {b2}");
    assert_ne!(b1, b2);
    assert!(!b1.contains("fixture"));

    let d1 = opaque_scoring_domain(&platform).unwrap();
    let d2 = opaque_scoring_domain(&platform).unwrap();
    assert!(
        is_opaque_scorer_domain(&d1),
        "unexpected scorer domain: {d1}"
    );
    assert!(
        is_opaque_scorer_domain(&d2),
        "unexpected scorer domain: {d2}"
    );
    assert_ne!(d1, d2);
    assert!(!d1.contains("fixture"));
}

#[test]
fn sec22_provision_generates_persists_before_fos_and_reuses_opaque_bucket() {
    let platform = Sec22Platform::new();
    let validated = platform.ok(
        "POST",
        "/api/admin/provision/validate",
        json!({"token": "fixture-token", "service_id": "fixtureService"}),
    );
    let suggested = validated["defaults"]["fos_bucket_name"].as_str().unwrap();
    assert!(is_opaque_bucket(suggested));
    assert!(!suggested.contains("fixtureservice"));

    // Provision with omitted fos_bucket -> generates opaque bucket & persists to KV before FOS PUT.
    let job = platform.ok(
        "POST",
        "/api/admin/provision/execute",
        Sec22Platform::provision_body(json!({})),
    )["job"]
        .clone();
    let done = platform.drain_job(job);
    assert_eq!(done["status"], "complete", "{done}");

    let svc =
        platform.ok("GET", "/api/admin/services/fixtureService", json!({}))["service"].clone();
    let persisted_bucket = svc["fos_bucket"].as_str().unwrap();
    assert!(is_opaque_bucket(persisted_bucket));
    assert!(!persisted_bucket.contains("fixtureservice"));

    // Confirm KV already held the generated bucket BEFORE the FOS PUT fired.
    let seen = platform.fos_buckets_seen_on_put.lock().unwrap().clone();
    let kv_before = platform.kv_bucket_before_fos_put.lock().unwrap().clone();
    assert_eq!(seen, vec![persisted_bucket.to_owned()]);
    assert_eq!(kv_before, vec![persisted_bucket.to_owned()]);

    // Subsequent validate reuses the persisted bucket name idempotently.
    let revalidated = platform.ok(
        "POST",
        "/api/admin/provision/validate",
        json!({"token": "fixture-token", "service_id": "fixtureService"}),
    );
    assert_eq!(
        revalidated["defaults"]["fos_bucket_name"].as_str().unwrap(),
        persisted_bucket
    );
}

#[test]
fn sec22_legacy_bucket_and_explicit_override_are_preserved() {
    let platform = Sec22Platform::new();
    let job = platform.ok(
        "POST",
        "/api/admin/provision/execute",
        Sec22Platform::provision_body(json!({"fos_bucket": "fos-fixtureservice-logs"})),
    )["job"]
        .clone();
    let done = platform.drain_job(job);
    assert_eq!(done["status"], "complete", "{done}");

    let svc =
        platform.ok("GET", "/api/admin/services/fixtureService", json!({}))["service"].clone();
    assert_eq!(svc["fos_bucket"], "fos-fixtureservice-logs");

    // Re-validating preserves the legacy fos-<sid>-logs bucket name.
    let validated = platform.ok(
        "POST",
        "/api/admin/provision/validate",
        json!({"token": "fixture-token", "service_id": "fixtureService"}),
    );
    assert_eq!(
        validated["defaults"]["fos_bucket_name"],
        "fos-fixtureservice-logs"
    );
}

#[test]
fn sec22_generated_bucket_retries_409_collision_and_persists_winner() {
    let platform = Sec22Platform::new();
    platform.bucket_put_409_remaining.store(2, Ordering::SeqCst);

    let job = platform.ok(
        "POST",
        "/api/admin/provision/execute",
        Sec22Platform::provision_body(json!({})),
    )["job"]
        .clone();
    let done = platform.drain_job(job);
    assert_eq!(done["status"], "complete", "{done}");

    let seen = platform.fos_buckets_seen_on_put.lock().unwrap().clone();
    let kv_before = platform.kv_bucket_before_fos_put.lock().unwrap().clone();
    assert_eq!(seen.len(), 3, "expected 2 collisions + 1 success: {seen:?}");
    assert_eq!(
        kv_before, seen,
        "each candidate bucket must be persisted in KV before remote PUT"
    );
    assert_ne!(seen[0], seen[1]);
    assert_ne!(seen[1], seen[2]);
    let svc =
        platform.ok("GET", "/api/admin/services/fixtureService", json!({}))["service"].clone();
    assert_eq!(svc["fos_bucket"].as_str().unwrap(), seen[2]);
}

#[test]
fn sec22_scoring_domain_opaque_persisted_and_retries_409_collision() {
    let platform = Sec22Platform::new();
    let prov = platform.ok(
        "POST",
        "/api/admin/provision/execute",
        Sec22Platform::provision_body(json!({})),
    )["job"]
        .clone();
    assert_eq!(platform.drain_job(prov)["status"], "complete");

    platform
        .scorer_domain_409_remaining
        .store(2, Ordering::SeqCst);
    let enable = platform.ok(
        "POST",
        "/api/admin/services/fixtureService/scoring/enable",
        json!({"token": "fixture-token"}),
    )["job"]
        .clone();

    // Immediately after /scoring/enable (before any advance/Fastly calls), KV already has an opaque domain!
    let pre_kv = platform
        .adapter
        .get("svc:fixtureService")
        .unwrap()
        .unwrap()
        .value;
    let initial_domain = pre_kv["scoring"]["scoring_domain"].as_str().unwrap();
    assert!(
        is_opaque_scorer_domain(initial_domain),
        "expected persisted opaque scorer domain before Fastly calls, got {initial_domain}"
    );
    assert!(!initial_domain.contains("fixtureservice"));

    let done = platform.drain_job(enable);
    assert_eq!(done["status"], "complete", "{done}");

    let seen = platform.scorer_domains_seen_on_post.lock().unwrap().clone();
    let kv_before = platform
        .kv_scorer_domain_before_post
        .lock()
        .unwrap()
        .clone();
    assert_eq!(seen.len(), 3, "expected 2 collisions + 1 success: {seen:?}");
    assert_eq!(
        kv_before, seen,
        "each candidate scorer domain must be persisted to KV before Fastly POST"
    );
    assert_ne!(seen[0], seen[1]);
    assert_ne!(seen[1], seen[2]);

    let svc =
        platform.ok("GET", "/api/admin/services/fixtureService", json!({}))["service"].clone();
    assert_eq!(svc["scoring"]["scoring_domain"].as_str().unwrap(), seen[2]);
}

#[test]
fn sec22_scoring_reuses_persisted_legacy_domain() {
    let platform = Sec22Platform::new();
    let prov = platform.ok(
        "POST",
        "/api/admin/provision/execute",
        Sec22Platform::provision_body(json!({})),
    )["job"]
        .clone();
    assert_eq!(platform.drain_job(prov)["status"], "complete");

    // Seed legacy persisted scoring_domain in KV before enabling.
    let mut rec = platform.adapter.get("svc:fixtureService").unwrap().unwrap();
    rec.value["scoring"] = json!({
        "enabled": false,
        "scoring_service_id": "",
        "scoring_service_name": "",
        "scoring_domain": "fos-fixtureservice-session-scorer.edgecompute.app",
        "scoring_keys_store_id": "",
        "scoring_config_store_id": "",
        "scoring_matrix_store_id": "",
        "enabled_at": "",
        "deployed_package_sha": "",
        "deployed_package_files_hash": "",
        "deployed_vcl_sha": "",
        "exclude_url_regex": ""
    });
    assert!(platform
        .adapter
        .cas("svc:fixtureService", Some(&rec.generation), &rec.value)
        .unwrap());

    let enable = platform.ok(
        "POST",
        "/api/admin/services/fixtureService/scoring/enable",
        json!({"token": "fixture-token"}),
    )["job"]
        .clone();
    let done = platform.drain_job(enable);
    assert_eq!(done["status"], "complete", "{done}");

    let svc =
        platform.ok("GET", "/api/admin/services/fixtureService", json!({}))["service"].clone();
    assert_eq!(
        svc["scoring"]["scoring_domain"],
        "fos-fixtureservice-session-scorer.edgecompute.app"
    );
}

#[test]
fn sec22_public_and_non_admin_endpoints_never_leak_bucket_or_internal_domains() {
    let platform = Sec22Platform::new();
    let secret_bucket = "fla-aaaaaaaaaaaaaaaaaaaaaaaa";
    let secret_prefix = "private-tenant-prefix/";
    let secret_scorer = "fla-bbbbbbbbbbbbbbbbbbbbbbbb.edgecompute.app";

    // Provision a service and seed scoring metadata.
    let prov = platform.ok(
        "POST",
        "/api/admin/provision/execute",
        Sec22Platform::provision_body(json!({
            "fos_bucket": secret_bucket,
            "fos_prefix": secret_prefix,
        })),
    )["job"]
        .clone();
    assert_eq!(platform.drain_job(prov)["status"], "complete");

    let mut rec = platform.adapter.get("svc:fixtureService").unwrap().unwrap();
    rec.value["scoring"] = json!({
        "enabled": true,
        "scoring_service_id": "scorerSvc999",
        "scoring_service_name": "Session Scoring Service for fixtureService",
        "scoring_domain": secret_scorer,
        "scoring_keys_store_id": "ks1",
        "scoring_config_store_id": "cs1",
        "scoring_matrix_store_id": "ms1",
        "enabled_at": "2026-01-01T00:00:00Z",
        "deployed_package_sha": "abc",
        "deployed_package_files_hash": "def",
        "deployed_vcl_sha": "ghi",
        "exclude_url_regex": ""
    });
    assert!(platform
        .adapter
        .cas("svc:fixtureService", Some(&rec.generation), &rec.value)
        .unwrap());

    // Seed an analyst user in auth state so we can exercise real Shell::handle for both
    // unauthenticated and non-admin callers.
    let now = platform.now();
    let analyst = User {
        id: "u-analyst".into(),
        display_name: "u-analyst".into(),
        role: Role::Analyst,
        credentials: vec![Credential {
            id: "cred-1".into(),
            public_key: vec![1],
            sign_count: 0,
            revoked: false,
        }],
        revoked: false,
        created_by: "test".into(),
        created_at: now,
        last_login_at: None,
    };
    let admin = User {
        id: "u-admin".into(),
        display_name: "u-admin".into(),
        role: Role::Admin,
        credentials: vec![Credential {
            id: "cred-admin".into(),
            public_key: vec![1],
            sign_count: 0,
            revoked: false,
        }],
        revoked: false,
        created_by: "test".into(),
        created_at: now,
        last_login_at: None,
    };
    let mut auth_state = AuthState::default();
    auth_state.users.insert(admin.id.clone(), admin);
    auth_state.users.insert(analyst.id.clone(), analyst.clone());
    let store = MemoryStore::default();
    assert!(store.compare_and_swap(None, &auth_state).unwrap());

    let claims = auth::encode(
        json!({"sub": analyst.id, "role": analyst.role, "policy_version": 0, "exp": now + 3600})
            .to_string()
            .as_bytes(),
    );
    let mut mac = Hmac::<Sha256>::new_from_slice(b"0123456789abcdef0123456789abcdef").unwrap();
    mac.update(claims.as_bytes());
    let analyst_cookie = format!(
        "flp_session={claims}.{}",
        auth::encode(&mac.finalize().into_bytes())
    );

    let shell = Shell::new(&platform, &store, &[]);

    // 1. Unauthenticated / public endpoints via Shell::handle
    for (method, path) in [
        ("GET", "/api/health"),
        ("GET", "/share-login"),
        ("GET", "/js/rum.js"),
        ("POST", "/_rum"),
        ("GET", "/api/nonexistent"),
    ] {
        let mut headers = BTreeMap::new();
        headers.insert("x-flp-front-key".into(), "fixture-front-key".into());
        let resp = shell.handle(&Request {
            method: method.into(),
            path: path.into(),
            headers,
            body: Vec::new(),
        });
        let body = String::from_utf8_lossy(&resp.body);
        let all_headers: String = resp
            .headers
            .iter()
            .map(|(k, v)| format!("{k}:{v}"))
            .collect::<Vec<_>>()
            .join("\n");
        for forbidden in [
            secret_bucket,
            secret_prefix,
            secret_scorer,
            "edgecompute.app",
            "object.fastlystorage.app",
        ] {
            assert!(
                !body.contains(forbidden) && !all_headers.contains(forbidden),
                "public {method} {path} leaked {forbidden} in body/headers"
            );
        }
    }

    // 2. Non-admin (analyst) /api/services and /api/bootstrap responses via Shell::handle
    for path in ["/api/services", "/api/bootstrap"] {
        let mut headers = BTreeMap::new();
        headers.insert("x-flp-front-key".into(), "fixture-front-key".into());
        headers.insert("fastly-client-ip".into(), "192.0.2.1".into());
        headers.insert("x-flp-front-rl".into(), "1".into());
        headers.insert("cookie".into(), analyst_cookie.clone());
        let resp = shell.handle(&Request {
            method: "GET".into(),
            path: path.into(),
            headers,
            body: Vec::new(),
        });
        let body = String::from_utf8_lossy(&resp.body);
        assert_eq!(
            resp.status, 200,
            "analyst GET {path} -> {}: {body}",
            resp.status
        );
        let all_headers: String = resp
            .headers
            .iter()
            .map(|(k, v)| format!("{k}:{v}"))
            .collect::<Vec<_>>()
            .join("\n");
        for forbidden in [
            secret_bucket,
            secret_prefix,
            secret_scorer,
            "scorerSvc999",
            "edgecompute.app",
            "object.fastlystorage.app",
        ] {
            assert!(
                !body.contains(forbidden) && !all_headers.contains(forbidden),
                "analyst {path} leaked {forbidden} in body/headers"
            );
        }
    }
}
