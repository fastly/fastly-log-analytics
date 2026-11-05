use fastly_log_analytics::{
    admin::{self, fixture::FixtureAdapter, ApiRequest, FosRequest, FosResponse, Record},
    auth::{self, webauthn::Credential, AuthState, Role, User},
    http::{Request, Response},
    setup::SetupStores,
    shell::{CoreCacheEntry, CoreCacheError, Runtime, Shell, ANALYTICS_SITE_ROUTES},
    state::{MemoryStore, StateStore},
    telemetry::{api::ApiLog, usage::UsageAgg},
};
use hmac::{Hmac, KeyInit, Mac};
use serde_json::{json, Value};
use sha2::Sha256;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::atomic::{AtomicU64, Ordering},
};

const SESSION_KEY: &[u8] = b"fixture-session-signing-material-32-bytes";

struct Platform {
    adapter: FixtureAdapter,
    clock: AtomicU64,
    cache: std::cell::RefCell<BTreeMap<String, (CoreCacheEntry, Vec<String>)>>,
    purges: std::cell::RefCell<Vec<String>>,
    vcpu: AtomicU64,
    usage: UsageAgg,
    api_log: ApiLog,
    budget: admin::BackendBudget,
    metered: std::cell::Cell<bool>,
    kv_reads: std::cell::Cell<u32>,
}

impl Platform {
    fn new() -> Self {
        Self {
            adapter: FixtureAdapter::new(),
            clock: AtomicU64::new(1_800_000_000),
            cache: Default::default(),
            purges: Default::default(),
            vcpu: AtomicU64::new(1_000),
            usage: UsageAgg::default(),
            api_log: ApiLog::default(),
            budget: Default::default(),
            metered: Default::default(),
            kv_reads: Default::default(),
        }
    }

    fn admin_call(&self, method: &str, path: &str, body: Value) -> Value {
        let response = admin::handle(
            self,
            "fixtureAdmin",
            &Request {
                method: method.into(),
                path: path.into(),
                headers: BTreeMap::new(),
                body: body.to_string().into_bytes(),
            },
        );
        assert_eq!(
            response.status,
            200,
            "{}",
            String::from_utf8_lossy(&response.body)
        );
        serde_json::from_slice(&response.body).unwrap()
    }

    fn provision(&self) {
        let mut job = self.admin_call(
            "POST",
            "/api/admin/provision/execute",
            json!({
                "token":"fixture-token","service_id":"fixtureService","fos_bucket":"fos-fixture-logs",
                "fos_region":"us-east-1",
                "log_fields":{"groups":["A","C"],"custom_fields":[],"field_overrides":{},"field_limits":{}}
            }),
        )["job"]
            .clone();
        for _ in 0..150 {
            if job["status"] == "complete" {
                return;
            }
            assert_ne!(job["status"], "failed", "{job}");
            if let Some(at) = job["retry_at"].as_u64() {
                self.clock.fetch_max(at, Ordering::SeqCst);
            }
            job = self.admin_call(
                "POST",
                &format!("/api/admin/jobs/{}/step", job["id"].as_str().unwrap()),
                json!({}),
            )["job"]
                .clone();
        }
        panic!("provision did not finish");
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
    fn secret(&self, key: &str) -> Result<Option<String>, String> {
        match key {
            "cdn_key" => Ok(Some("fixture-cdn-key".into())),
            "front_key" => Ok(Some("fixture-front-key".into())),
            "session_key" => Ok(Some(String::from_utf8(SESSION_KEY.to_vec()).unwrap())),
            "fastly_token" => Ok(Some("fixture-token".into())),
            _ => self.adapter.secret(key),
        }
    }
    fn random(&self) -> Result<[u8; 32], String> {
        Ok([9; 32])
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
        self.kv_reads.set(self.kv_reads.get() + 1);
        if self.metered.get() {
            self.budget.charge()?;
        }
        self.adapter.get(key)
    }
    fn set_telemetry_budget(&self, on: bool) {
        self.budget.set_telemetry(on);
    }
    fn kv_list(
        &self,
        prefix: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<fastly_log_analytics::shell::KvPage, String> {
        let keys: Vec<String> = self
            .adapter
            .records()
            .into_keys()
            .filter(|k| k.starts_with(prefix) && cursor.is_none_or(|c| k.as_str() > c))
            .take(limit as usize)
            .collect();
        Ok(fastly_log_analytics::shell::KvPage {
            keys,
            next_cursor: None,
        })
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
        self.adapter.paced_cas(key, generation, value)
    }
    fn admin_api(&self, request: ApiRequest<'_>) -> Result<admin::ApiResponse, String> {
        self.adapter.api(request).map(Into::into)
    }
    fn admin_fos(&self, request: FosRequest<'_>) -> Result<FosResponse, String> {
        self.adapter.fos(request)
    }
    fn admin_wait(&self, millis: u64) {
        self.adapter.advance_kv_clock(millis);
    }
    fn usage_agg(&self) -> Option<&UsageAgg> {
        Some(&self.usage)
    }
    fn api_log(&self) -> Option<&ApiLog> {
        Some(&self.api_log)
    }
    fn core_cache_replace(
        &self,
        key: &str,
        surrogates: &[&str],
        _: std::time::Duration,
        update: &dyn Fn(Option<CoreCacheEntry>) -> CoreCacheEntry,
    ) -> Result<(), CoreCacheError> {
        let mut cache = self.cache.borrow_mut();
        let next = update(cache.get(key).map(|(entry, _)| entry.clone()));
        let keys = surrogates.iter().map(|k| k.to_string()).collect();
        cache.insert(key.into(), (next, keys));
        Ok(())
    }
    fn core_cache_lookup(&self, key: &str) -> Result<Option<CoreCacheEntry>, CoreCacheError> {
        Ok(self.cache.borrow().get(key).map(|(entry, _)| entry.clone()))
    }
    fn core_cache_purge(&self, surrogate: &str) -> Result<(), CoreCacheError> {
        self.purges.borrow_mut().push(surrogate.into());
        self.cache
            .borrow_mut()
            .retain(|_, (_, keys)| !keys.iter().any(|k| k == surrogate));
        Ok(())
    }
    fn admin_charge_call(&self) -> Result<(), String> {
        self.budget.charge()
    }
    fn vcpu_ms(&self) -> u64 {
        self.vcpu.fetch_add(7, Ordering::SeqCst)
    }
    fn pop(&self) -> Option<String> {
        Some("IAD".into())
    }
    fn on_log_shield(&self) -> bool {
        true
    }
}

fn user(id: &str, role: Role, now: u64) -> User {
    User {
        id: id.into(),
        display_name: id.into(),
        role,
        credentials: vec![Credential {
            id: format!("{id}-credential"),
            public_key: vec![1],
            sign_count: 0,
            revoked: false,
        }],
        revoked: false,
        created_by: "test".into(),
        created_at: now,
    }
}

fn cookie(user: &User, exp: u64) -> String {
    let claims = auth::encode(
        json!({"sub":user.id,"role":user.role,"policy_version":0,"exp":exp})
            .to_string()
            .as_bytes(),
    );
    let mut mac = Hmac::<Sha256>::new_from_slice(SESSION_KEY).unwrap();
    mac.update(claims.as_bytes());
    format!(
        "flp_session={claims}.{}",
        auth::encode(&mac.finalize().into_bytes())
    )
}

struct Harness {
    platform: Platform,
    store: MemoryStore,
    sessions: BTreeMap<&'static str, Option<String>>,
}

impl Harness {
    fn new() -> Self {
        let platform = Platform::new();
        platform.provision();
        let now = platform.now();
        let users = [
            ("admin", user("u-admin", Role::Admin, now)),
            ("analyst", user("u-analyst", Role::Analyst, now)),
            ("no_pii", user("u-nopii", Role::AnalystNoPii, now)),
        ];
        let victim = user("u-victim", Role::Analyst, now);
        let mut state = AuthState::default();
        for (_, user) in &users {
            state.users.insert(user.id.clone(), user.clone());
        }
        state.users.insert(victim.id.clone(), victim);
        let store = MemoryStore::default();
        assert!(store.compare_and_swap(None, &state).unwrap());
        let mut sessions = BTreeMap::new();
        sessions.insert("anonymous", None);
        for (name, user) in &users {
            sessions.insert(*name, Some(cookie(user, now + 3600)));
        }
        let admin = &users[0].1;
        sessions.insert("expired", Some(cookie(admin, now - 1)));
        let mut tampered = cookie(admin, now + 3600);
        let last = tampered.pop().unwrap();
        tampered.push(if last == 'A' { 'B' } else { 'A' });
        sessions.insert("tampered", Some(tampered));
        Self {
            platform,
            store,
            sessions,
        }
    }

    fn send(&self, method: &str, path: &str, session: &str) -> Response {
        let mut headers = BTreeMap::from([
            (
                "x-flp-front-key".to_string(),
                "fixture-front-key".to_string(),
            ),
            ("fastly-client-ip".into(), "192.0.2.1".into()),
            ("x-flp-front-rl".into(), "1".into()),
        ]);
        if let Some(cookie) = &self.sessions[session] {
            headers.insert("cookie".into(), cookie.clone());
        }
        let mut body = vec![];
        if method != "GET" {
            headers.insert("origin".into(), "https://logs.example.com".into());
            headers.insert("content-type".into(), "application/json".into());
            body = b"{}".to_vec();
        }
        Shell::new(&self.platform, &self.store, &[]).handle(&Request {
            method: method.into(),
            path: path.into(),
            headers,
            body,
        })
    }
}

impl Harness {
    fn send_with(
        &self,
        method: &str,
        path: &str,
        session: &str,
        origin: Option<&str>,
        body: &[u8],
    ) -> Response {
        self.send_with_headers(method, path, session, origin, body, &[])
    }

    fn send_with_headers(
        &self,
        method: &str,
        path: &str,
        session: &str,
        origin: Option<&str>,
        body: &[u8],
        extra: &[(&str, &str)],
    ) -> Response {
        let mut headers = BTreeMap::from([
            (
                "x-flp-front-key".to_string(),
                "fixture-front-key".to_string(),
            ),
            ("fastly-client-ip".into(), "192.0.2.1".into()),
            ("x-flp-front-rl".into(), "1".into()),
            ("content-type".into(), "application/json".into()),
        ]);
        if let Some(cookie) = &self.sessions[session] {
            headers.insert("cookie".into(), cookie.clone());
        }
        if let Some(origin) = origin {
            headers.insert("origin".into(), origin.into());
        }
        for (name, value) in extra {
            headers.insert((*name).into(), (*value).into());
        }
        Shell::new(&self.platform, &self.store, &[]).handle(&Request {
            method: method.into(),
            path: path.into(),
            headers,
            body: body.to_vec(),
        })
    }

    fn beacon(&self, session: &str, body: Value) -> Response {
        self.send_with(
            "POST",
            "/api/telemetry/query",
            session,
            Some("https://logs.example.com"),
            body.to_string().as_bytes(),
        )
    }

    fn rows(&self, kind: &str) -> Vec<Value> {
        let prefix = format!("flp:log:v1:{kind}:");
        self.platform
            .cache
            .borrow()
            .iter()
            .filter(|(key, _)| key.starts_with(&prefix))
            .flat_map(|(_, ((body, _), _))| {
                String::from_utf8_lossy(body)
                    .lines()
                    .map(|line| serde_json::from_str::<Value>(line).unwrap())
                    .collect::<Vec<_>>()
            })
            .collect()
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Class {
    Public,
    Session,
    NoPiiDenied,
    Admin,
}

const ROUTES: &[(&str, &str, Class)] = &[
    ("GET", "/api/services", Class::Session),
    ("GET", "/api/log-fields/catalog", Class::Session),
    ("GET", "/api/rum/versions", Class::Session),
    ("GET", "/api/log-extents", Class::Session),
    ("GET", "/api/files", Class::Session),
    ("GET", "/api/ingest/events", Class::Session),
    ("GET", "/api/file", Class::NoPiiDenied),
    ("HEAD", "/api/file", Class::NoPiiDenied),
    ("POST", "/api/telemetry/query", Class::Session),
    ("GET", "/api/sync-status", Class::Admin),
    ("GET", "/api/cron-runs", Class::Admin),
    ("POST", "/api/cron-runs/trigger", Class::Admin),
    ("GET", "/api/admin/users", Class::Admin),
    ("PATCH", "/api/admin/users/u-victim", Class::Admin),
    ("POST", "/api/admin/users/u-victim/revoke", Class::Admin),
    (
        "POST",
        "/api/admin/users/u-victim/passkeys/c/revoke",
        Class::Admin,
    ),
    ("GET", "/api/admin/invites", Class::Admin),
    ("POST", "/api/admin/invites", Class::Admin),
    ("POST", "/api/admin/invites/i/revoke", Class::Admin),
    ("GET", "/api/admin/versions", Class::Admin),
    ("GET", "/api/admin/usage-logging", Class::Admin),
    ("PATCH", "/api/admin/usage-logging", Class::Admin),
    ("GET", "/api/admin/audit-logs", Class::Admin),
    ("GET", "/api/admin/ingested-files", Class::Admin),
    ("GET", "/api/admin/raw-tree", Class::Admin),
    ("GET", "/api/admin/storage-tree", Class::Admin),
    ("GET", "/api/admin/quarantine", Class::Admin),
    ("POST", "/api/admin/quarantine/purge", Class::Admin),
    ("GET", "/api/admin/services", Class::Admin),
    ("GET", "/api/admin/services/fixtureService", Class::Admin),
    ("PATCH", "/api/admin/services/fixtureService", Class::Admin),
    (
        "POST",
        "/api/admin/services/fixtureService/reconcile",
        Class::Admin,
    ),
    ("GET", "/api/admin/system-checks", Class::Admin),
    ("POST", "/api/admin/system-checks", Class::Admin),
    ("POST", "/api/admin/provision/validate", Class::Admin),
    ("POST", "/api/admin/provision/execute", Class::Admin),
    ("POST", "/api/provision/validate", Class::Admin),
    ("POST", "/api/provision/execute", Class::Admin),
    ("POST", "/api/admin/jobs/j/step", Class::Admin),
    ("POST", "/api/admin/log-fields/preview", Class::Admin),
    ("GET", "/api/admin/rum/versions", Class::Admin),
    ("GET", "/api/admin/operational-logging", Class::Admin),
    ("PATCH", "/api/admin/operational-logging", Class::Admin),
    ("GET", "/api/admin/logs/status", Class::Admin),
    ("POST", "/api/admin/logs/flush", Class::Admin),
    ("POST", "/api/admin/logs/clear-cache", Class::Admin),
    ("POST", "/api/admin/logs/delete-history", Class::Admin),
    ("POST", "/api/admin/logs/sweep", Class::Admin),
    ("POST", "/api/admin/logs/backfill", Class::Admin),
    ("POST", "/api/admin/usage/reconcile-storage", Class::Admin),
    (
        "GET",
        "/api/admin/usage/compute-billing?from=0&to=1000",
        Class::Admin,
    ),
    ("GET", "/api/admin/usage/logged-traffic", Class::Admin),
    ("GET", "/api/logs/cron_ticks/live", Class::Admin),
];

// Every `/api/...` string literal in src/shell.rs must be declared here.
// Admin and session entries must also prefix a path exercised in ROUTES.
const DECLARED: &[(&str, Class)] = &[
    ("/api/", Class::Public),
    ("/api/auth/", Class::Public),
    ("/api/auth/session", Class::Public),
    ("/api/auth/login/options", Class::Public),
    ("/api/auth/login/verify", Class::Public),
    ("/api/auth/register/options", Class::Public),
    ("/api/auth/register/verify", Class::Public),
    ("/api/auth/logout", Class::Public),
    ("/api/setup", Class::Public),
    ("/api/setup/accept", Class::Public),
    ("/api/services", Class::Session),
    ("/api/log-fields/catalog", Class::Session),
    ("/api/rum/versions", Class::Session),
    ("/api/log-extents", Class::Session),
    ("/api/files", Class::Session),
    ("/api/ingest/events", Class::Session),
    ("/api/file", Class::NoPiiDenied),
    ("/api/telemetry/query", Class::Session),
    ("/api/sync-status", Class::Admin),
    ("/api/cron-runs", Class::Admin),
    ("/api/cron-runs/trigger", Class::Admin),
    ("/api/provision/", Class::Admin),
    ("/api/admin/", Class::Admin),
    ("/api/admin/provision/", Class::Admin),
    ("/api/admin/services", Class::Admin),
    ("/api/admin/jobs/", Class::Admin),
    ("/api/admin/log-fields/", Class::Admin),
    ("/api/admin/rum/", Class::Admin),
    ("/api/admin/users", Class::Admin),
    ("/api/admin/invites", Class::Admin),
    ("/api/admin/versions", Class::Admin),
    ("/api/admin/usage-logging", Class::Admin),
    ("/api/admin/ingested-files", Class::Admin),
    ("/api/admin/raw-tree", Class::Admin),
    ("/api/admin/storage-tree", Class::Admin),
    ("/api/admin/quarantine", Class::Admin),
    ("/api/admin/quarantine/purge", Class::Admin),
    ("/api/admin/audit-logs", Class::Admin),
    ("/api/admin/system-checks", Class::Admin),
    ("/api/admin/operational-logging", Class::Admin),
    ("/api/admin/logs/", Class::Admin),
    ("/api/admin/usage/", Class::Admin),
    ("/api/logs/", Class::Admin),
];

fn status_ok_for_admin(status: u16) -> bool {
    status != 401 && status != 403
}

#[test]
fn anonymous_expired_and_tampered_sessions_get_401_everywhere() {
    let harness = Harness::new();
    for (method, path, class) in ROUTES {
        for session in ["anonymous", "expired", "tampered"] {
            let status = harness.send(method, path, session).status;
            assert_eq!(status, 401, "{method} {path} ({class:?}) as {session}");
        }
    }
}

#[test]
fn admin_routes_reject_analysts_and_admin_is_not_blocked() {
    let harness = Harness::new();
    for (method, path, class) in ROUTES {
        let admin = harness.send(method, path, "admin").status;
        assert!(
            status_ok_for_admin(admin),
            "admin blocked on {method} {path}: {admin}"
        );
        for session in ["analyst", "no_pii"] {
            let status = harness.send(method, path, session).status;
            match class {
                Class::Admin => assert_eq!(status, 403, "{method} {path} as {session}"),
                Class::NoPiiDenied if session == "no_pii" => {
                    assert_eq!(status, 403, "{method} {path} as {session}")
                }
                _ => assert!(
                    status_ok_for_admin(status),
                    "{method} {path} as {session}: {status}"
                ),
            }
        }
    }
}

#[test]
fn admin_pages_redirect_anonymous_and_reject_analysts() {
    let harness = Harness::new();
    for page in [
        "/admin",
        "/admin/share",
        "/admin/usage-log",
        "/admin/rum",
        "/admin/session-scoring",
        "/admin/tick",
        "/logs",
    ] {
        for path in [
            page.to_string(),
            format!("{page}.html"),
            format!("{page}.txt"),
        ] {
            let anonymous = harness.send("GET", &path, "anonymous");
            assert_eq!(anonymous.status, 303, "{path} as anonymous");
            for session in ["analyst", "no_pii"] {
                let status = harness.send("GET", &path, session).status;
                assert_eq!(status, 403, "{path} as {session}");
            }
            let admin = harness.send("GET", &path, "admin").status;
            assert!(
                status_ok_for_admin(admin) && admin != 303,
                "{path} as admin: {admin}"
            );
        }
    }
}

#[test]
fn analytics_site_routes_allowlist_covers_all_exported_pages() {
    for route in [
        "/dashboard",
        "/origin",
        "/performance",
        "/security",
        "/network",
        "/fastly-value",
        "/assets-shield",
        "/rum",
        "/streaming",
        "/sessions",
        "/sessions/stream",
        "/query",
        "/insights",
        "/usage",
        "/how-it-works",
        "/share-login",
        "/share-login/acknowledge",
        "/logs",
        "/admin",
    ] {
        assert!(
            ANALYTICS_SITE_ROUTES.contains(&route),
            "expected {route} in ANALYTICS_SITE_ROUTES"
        );
    }
}

#[test]
fn operational_datasets_and_files_are_admin_only() {
    let harness = Harness::new();
    for path in [
        "/api/files?dataset=ops:cron_ticks",
        "/api/files?dataset=ops",
        "/api/file?key=processed/ops/cron_ticks/x.parquet",
    ] {
        for session in ["analyst", "no_pii"] {
            let status = harness.send("GET", path, session).status;
            assert_eq!(status, 403, "{path} as {session}");
        }
        assert_eq!(harness.send("GET", path, "anonymous").status, 401, "{path}");
        let admin = harness.send("GET", path, "admin").status;
        assert!(status_ok_for_admin(admin), "{path} as admin: {admin}");
    }
    for session in ["analyst", "no_pii"] {
        let status = harness
            .send("GET", "/api/files?dataset=request", session)
            .status;
        assert!(status_ok_for_admin(status), "request dataset as {session}");
    }
}

#[test]
fn operational_settings_changes_are_audited() {
    let harness = Harness::new();
    let patch = harness.send_with(
        "PATCH",
        "/api/admin/operational-logging",
        "admin",
        Some("https://logs.example.com"),
        br#"{"flush_max_rows": 750}"#,
    );
    assert_eq!(
        patch.status,
        200,
        "{}",
        String::from_utf8_lossy(&patch.body)
    );
    let rejected = harness.send_with(
        "PATCH",
        "/api/admin/operational-logging",
        "admin",
        Some("https://logs.example.com"),
        br#"{"flush_max_rows": 1}"#,
    );
    assert_eq!(rejected.status, 400);
    let audit = harness.send("GET", "/api/admin/audit-logs", "admin");
    let body: Value = serde_json::from_slice(&audit.body).unwrap();
    assert!(
        !body.to_string().contains("operational_logging_updated"),
        "settings changes belong to the audit log, not service history"
    );
    let state = harness.store.load().unwrap().state;
    let text = serde_json::to_string(&[&state.audit, &state.recent_audit]).unwrap();
    assert_eq!(
        text.matches("operational_logging_updated").count(),
        1,
        "only the applied patch is audited: {text}"
    );
    assert!(text.contains("flush_max_rows"));
}

#[test]
fn quarantine_purge_needs_a_confirm_token_and_audits_only_confirmed_steps() {
    let harness = Harness::new();
    let post = |body: Value| {
        let response = harness.send_with(
            "POST",
            "/api/admin/quarantine/purge",
            "admin",
            Some("https://logs.example.com"),
            body.to_string().as_bytes(),
        );
        let status = response.status;
        (
            status,
            serde_json::from_slice::<Value>(&response.body).unwrap(),
        )
    };
    let audited = || {
        let state = harness.store.load().unwrap().state;
        serde_json::to_string(&[&state.audit, &state.recent_audit])
            .unwrap()
            .matches("quarantine_purged")
            .count()
    };
    let (status, issued) = post(json!({"target": "all"}));
    assert_eq!(status, 200, "{issued}");
    assert_eq!(issued["status"], "confirm_required");
    assert_eq!(audited(), 0, "asking for a token is not a purge");
    let (status, _) = post(json!({"target": "all", "confirm": "forged.token"}));
    assert_eq!(status, 400);
    assert_eq!(audited(), 0);
    let (status, done) = post(json!({"target": "all", "confirm": issued["confirm_token"]}));
    assert_eq!(status, 200, "{done}");
    assert_eq!(
        (done["purged"].as_u64(), done["complete"].as_bool()),
        (Some(0), Some(true))
    );
    assert_eq!(audited(), 1);
}

#[test]
fn services_listing_is_redacted_for_analysts_only() {
    let harness = Harness::new();
    let read = |session| -> Vec<Value> {
        let response = harness.send("GET", "/api/services", session);
        assert_eq!(response.status, 200);
        serde_json::from_slice::<Value>(&response.body).unwrap()["services"]
            .as_array()
            .unwrap()
            .clone()
    };
    let allowed: BTreeSet<&str> = [
        "service_id",
        "name",
        "status",
        "logging_enabled",
        "created_at",
        "updated_at",
    ]
    .into();
    let full = read("admin");
    assert!(!full.is_empty());
    let full_keys: BTreeSet<String> = full[0].as_object().unwrap().keys().cloned().collect();
    assert!(
        full_keys.iter().any(|key| key.ends_with("_ref")
            || key == "bucket"
            || key == "prefix"
            || key == "owner"
            || key == "log_fields"),
        "admin record unexpectedly lacks sensitive fields: {full_keys:?}"
    );
    for session in ["analyst", "no_pii"] {
        let services = read(session);
        assert_eq!(services.len(), full.len());
        for service in services {
            for key in service.as_object().unwrap().keys() {
                assert!(
                    allowed.contains(key.as_str()),
                    "{session} sees {key} in /api/services"
                );
            }
        }
    }
}

#[test]
fn tick_requires_exact_path() {
    let harness = Harness::new();
    assert_eq!(harness.send("GET", "/_tick/", "anonymous").status, 403);
    assert_eq!(harness.send("GET", "/_tick/?x=1", "anonymous").status, 403);
    assert_eq!(harness.send("GET", "/_tick", "anonymous").status, 200);
}

#[test]
fn every_api_literal_in_the_shell_declares_an_auth_class() {
    let source = include_str!("../src/shell.rs");
    let declared: BTreeMap<&str, Class> = DECLARED.iter().copied().collect();
    let mut found = BTreeSet::new();
    let mut rest = source;
    while let Some(start) = rest.find("\"/api/") {
        let after = &rest[start + 1..];
        let end = after.find('"').unwrap();
        found.insert(&after[..end]);
        rest = &after[end + 1..];
    }
    assert!(found.len() > 20, "literal scan found too little: {found:?}");
    let undeclared: Vec<_> = found
        .iter()
        .filter(|literal| !declared.contains_key(**literal))
        .collect();
    assert!(
        undeclared.is_empty(),
        "declare an auth class for new shell routes in DECLARED and ROUTES: {undeclared:?}"
    );
    for (literal, class) in DECLARED {
        if *class != Class::Public {
            assert!(
                ROUTES.iter().any(|(_, path, _)| path.starts_with(literal)),
                "{literal} is declared but no ROUTES entry exercises it"
            );
        }
    }
    for (_, path, class) in ROUTES {
        let covering = DECLARED
            .iter()
            .filter(|(literal, _)| path.starts_with(literal) && *literal != "/api/")
            .map(|(_, class)| *class)
            .collect::<Vec<_>>();
        assert!(
            covering.contains(class),
            "{path} has no matching DECLARED class {class:?}"
        );
    }
}

#[test]
fn array_style_routes_are_not_missed_by_the_literal_scan() {
    let source = include_str!("../src/shell.rs");
    let mut rest = source;
    while let Some(start) = rest.find("[\"api\"") {
        let after = &rest[start..];
        let end = after.find(']').unwrap();
        let segments: Vec<&str> = after[1..end]
            .split(',')
            .map(str::trim)
            .map(|part| {
                if part.starts_with('"') {
                    part.trim_matches('"')
                } else {
                    "*"
                }
            })
            .collect();
        let path = format!("/{}", segments.join("/"));
        assert!(
            ROUTES.iter().any(|(_, route, _)| {
                let route_parts: Vec<_> = route.split('/').skip(1).collect();
                route_parts.len() >= segments.len()
                    && route_parts
                        .iter()
                        .zip(&segments)
                        .all(|(a, b)| *b == "*" || a == b)
            }),
            "segment route {path} is not exercised in ROUTES"
        );
        rest = &after[end..];
    }
}

#[test]
fn api_requests_log_normalized_path_actor_pop_and_vcpu_delta() {
    let harness = Harness::new();
    harness.send("GET", "/api/admin/users?token=abc", "admin");
    harness.send("GET", "/api/auth/session", "anonymous");
    let rows = harness.rows("api_telemetry");
    assert_eq!(rows.len(), 2, "{rows:?}");
    let users = rows
        .iter()
        .find(|r| r["path"] == "/api/admin/users")
        .unwrap();
    assert_eq!(users["source"], "compute_api");
    assert_eq!(users["method"], "GET");
    assert_eq!(users["status_code"], 200);
    assert_eq!(users["actor"], "u-admin");
    assert_eq!(users["pop"], "IAD");
    assert_eq!(users["service_id"], "fixtureService");
    let vcpu = users["vcpu_ms"].as_u64().unwrap();
    assert!(vcpu > 0 && vcpu < 1_000, "cumulative vcpu leaked: {vcpu}");
    assert!(!Value::Array(rows.clone()).to_string().contains("abc"));
    let session = rows
        .iter()
        .find(|r| r["path"] == "/api/auth/session")
        .unwrap();
    assert!(session["actor"].is_null() || session["actor"] == "");
}

#[test]
fn beacon_records_one_server_attributed_row_and_no_compute_api_row() {
    let harness = Harness::new();
    let response = harness.beacon(
        "analyst",
        json!({"files_listed": 4, "cdn_hits": 2, "actor": "evil", "event_id": "evil",
               "service_id": "evil", "source": "evil", "timestamp": 1, "unknown": true}),
    );
    assert_eq!(response.status, 204);
    let rows = harness.rows("api_telemetry");
    assert_eq!(rows.len(), 1, "{rows:?}");
    let row = &rows[0];
    assert_eq!(row["source"], "browser_query");
    assert_eq!(row["actor"], "u-analyst");
    assert_eq!(row["service_id"], "fixtureService");
    assert_eq!(row["files_listed"], 4);
    assert_ne!(row["event_id"], "evil");
    assert_ne!(row["timestamp"], 1);
    assert!(row.get("unknown").is_none());
}

#[test]
fn beacon_folds_dashboard_file_reads_into_one_compute_row() {
    let harness = Harness::new();
    let response = harness.beacon(
        "analyst",
        json!({"files_fetched": 9, "compute_requests": 7, "vcpu_ms": 35}),
    );
    assert_eq!(response.status, 204);
    let rows = harness.rows("api_telemetry");
    assert_eq!(rows.len(), 2, "{rows:?}");
    let folded = rows
        .iter()
        .find(|r| r["source"] == "compute_api")
        .expect("folded compute_api row");
    assert_eq!(folded["path"], "/api/file");
    assert_eq!(folded["files_fetched"], 7);
    assert_eq!(folded["vcpu_ms"], 35);
    assert_eq!(folded["actor"], "u-analyst");
    let browser = rows
        .iter()
        .find(|r| r["source"] == "browser_query")
        .expect("browser_query row");
    assert_eq!(browser["files_fetched"], 9);
    assert!(browser.get("vcpu_ms").is_none());
}

#[test]
fn folded_file_row_carries_the_beacon_harness_label() {
    let harness = Harness::new();
    let response = harness.send_with_headers(
        "POST",
        "/api/telemetry/query",
        "analyst",
        Some("https://logs.example.com"),
        json!({"compute_requests": 3, "vcpu_ms": 12})
            .to_string()
            .as_bytes(),
        &[("x-flp-harness", "camp-p10-s1")],
    );
    assert_eq!(response.status, 204);
    let rows = harness.rows("api_telemetry");
    let folded = rows
        .iter()
        .find(|r| r["source"] == "compute_api" && r["path"] == "/api/file")
        .expect("folded compute_api row");
    let details: Value = serde_json::from_str(folded["details_json"].as_str().unwrap()).unwrap();
    assert_eq!(details["harness"], "camp-p10-s1");
}

#[test]
fn folded_file_reads_are_recorded_when_browser_query_telemetry_is_off() {
    let harness = Harness::new();
    assert!(harness
        .platform
        .admin_cas(
            "settings:operational_logs",
            None,
            &json!({"browser_query_telemetry": false})
        )
        .unwrap());
    let response = harness.beacon("analyst", json!({"compute_requests": 3, "vcpu_ms": 12}));
    assert_eq!(response.status, 204);
    let rows = harness.rows("api_telemetry");
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["source"], "compute_api");
    assert_eq!(rows[0]["files_fetched"], 3);
}

#[test]
fn a_file_response_streams_from_the_cdn_with_the_clients_range() {
    let harness = Harness::new();
    let headers = BTreeMap::from([
        (
            "x-flp-front-key".to_string(),
            "fixture-front-key".to_string(),
        ),
        ("fastly-client-ip".into(), "192.0.2.1".into()),
        ("x-flp-front-rl".into(), "1".into()),
        ("range".into(), "bytes=0-9".into()),
        (
            "cookie".into(),
            harness.sessions["analyst"].clone().unwrap(),
        ),
    ]);
    let response = Shell::new(&harness.platform, &harness.store, &[]).handle(&Request {
        method: "GET".into(),
        path: "/api/file?key=processed/request/x.parquet".into(),
        headers,
        body: vec![],
    });
    assert_eq!(response.status, 200);
    assert!(response.body.is_empty());
    let upstream = response.upstream.expect("streamed from the CDN");
    assert_eq!(upstream.key, "processed/request/x.parquet");
    assert_eq!(upstream.cdn_key, "fixture-cdn-key");
    assert_eq!(upstream.range.as_deref(), Some("bytes=0-9"));
    assert_eq!(
        response.headers["content-type"],
        "application/vnd.apache.parquet"
    );
}

#[test]
fn duckdb_file_head_is_authenticated_and_forwards_its_open_range() {
    let harness = Harness::new();
    let response = Shell::new(&harness.platform, &harness.store, &[]).handle(&Request {
        method: "HEAD".into(),
        path: "/api/file?key=processed/request/x.parquet".into(),
        headers: BTreeMap::from([
            ("x-flp-front-key".into(), "fixture-front-key".into()),
            ("fastly-client-ip".into(), "192.0.2.1".into()),
            ("x-flp-front-rl".into(), "1".into()),
            ("range".into(), "bytes=0-".into()),
            (
                "cookie".into(),
                harness.sessions["analyst"].clone().unwrap(),
            ),
        ]),
        body: vec![],
    });
    assert_eq!(response.status, 200);
    let upstream = response
        .upstream
        .expect("HEAD must stream metadata, not read the file");
    assert!(upstream.head_only);
    assert_eq!(upstream.range.as_deref(), Some("bytes=0-"));
    assert!(response.body.is_empty());
}

#[test]
fn flagged_file_read_failures_still_record_their_own_row() {
    let harness = Harness::new();
    let mut headers = BTreeMap::from([
        (
            "x-flp-front-key".to_string(),
            "fixture-front-key".to_string(),
        ),
        ("fastly-client-ip".into(), "192.0.2.1".into()),
        ("x-flp-front-rl".into(), "1".into()),
        ("x-flp-fold-telemetry".into(), "1".into()),
    ]);
    headers.insert(
        "cookie".into(),
        harness.sessions["analyst"].clone().unwrap(),
    );
    let response = Shell::new(&harness.platform, &harness.store, &[]).handle(&Request {
        method: "GET".into(),
        path: "/api/file?key=elsewhere/x.parquet".into(),
        headers,
        body: vec![],
    });
    assert_eq!(response.status, 400);
    let rows = harness.rows("api_telemetry");
    assert!(
        rows.iter()
            .any(|r| r["source"] == "compute_api" && r["path"] == "/api/file"),
        "{rows:?}"
    );
}

#[test]
fn beacon_rejects_oversize_signed_out_and_cross_origin() {
    let harness = Harness::new();
    let big = json!({"files_listed": 1, "pad": "x".repeat(5000)});
    assert_eq!(harness.beacon("analyst", big).status, 413);
    assert_eq!(harness.beacon("anonymous", json!({})).status, 401);
    let cross = harness.send_with(
        "POST",
        "/api/telemetry/query",
        "analyst",
        Some("https://evil.example"),
        b"{}",
    );
    assert_eq!(cross.status, 403);
    assert!(harness
        .rows("api_telemetry")
        .iter()
        .all(|r| r["source"] != "browser_query"));
}

#[test]
fn beacons_over_sixty_per_minute_are_dropped_with_204_then_recover() {
    let harness = Harness::new();
    for _ in 0..60 {
        assert_eq!(
            harness.beacon("analyst", json!({"files_listed": 1})).status,
            204
        );
    }
    let over = harness.beacon("analyst", json!({"files_listed": 1}));
    assert_eq!(over.status, 204);
    assert_eq!(
        over.headers.get("x-flp-log-dropped").map(String::as_str),
        Some("1")
    );
    let browser = |h: &Harness| {
        h.rows("api_telemetry")
            .iter()
            .filter(|r| r["source"] == "browser_query")
            .count()
    };
    assert_eq!(browser(&harness), 60);
    assert_eq!(
        harness.beacon("admin", json!({"files_listed": 1})).status,
        204
    );
    assert_eq!(browser(&harness), 61);
    harness.platform.clock.fetch_add(60, Ordering::SeqCst);
    harness.beacon("analyst", json!({"files_listed": 1}));
    assert_eq!(browser(&harness), 62);
}

#[test]
fn beacons_are_ignored_when_browser_query_telemetry_is_off() {
    let harness = Harness::new();
    assert!(harness
        .platform
        .admin_cas(
            "settings:operational_logs",
            None,
            &json!({"browser_query_telemetry": false})
        )
        .unwrap());
    let response = harness.beacon("analyst", json!({"files_listed": 1}));
    assert_eq!(response.status, 204);
    assert!(harness.rows("api_telemetry").is_empty());
}

#[test]
fn unauthenticated_failures_do_no_telemetry_work() {
    let harness = Harness::new();
    let reads = harness.platform.kv_reads.get();
    for path in [
        "/api/admin/users",
        "/api/files",
        "/api/zz-fuzz-1",
        "/api/admin/zz-fuzz-2",
    ] {
        let status = harness.send("GET", path, "anonymous").status;
        assert!(matches!(status, 401 | 403 | 404), "{path}: {status}");
    }
    assert_eq!(harness.platform.kv_reads.get(), reads);
    assert!(harness.rows("api_telemetry").is_empty());
    assert!(harness.platform.cache.borrow().is_empty());
}

#[test]
fn route_table_paths_never_normalize_to_unknown_and_fuzzed_segments_do() {
    use fastly_log_analytics::telemetry::api::normalize_path;
    for (_, path, _) in ROUTES {
        assert!(!normalize_path(path).contains(":unknown"), "{path}");
    }
    assert_eq!(
        normalize_path("/api/admin/users/abc/zzz"),
        "/api/admin/users/:id/:unknown"
    );
}

#[test]
fn telemetry_survives_an_exhausted_backend_budget_without_falling_to_global() {
    let harness = Harness::new();
    harness.platform.metered.set(true);
    for _ in 0..32 {
        harness.platform.budget.charge().unwrap();
    }
    assert!(harness.platform.admin_get("admin:install").is_err());
    harness.send("GET", "/api/admin/users", "admin");
    let rows = harness.rows("api_telemetry");
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["service_id"], "fixtureService");
}

fn status_entries(h: &Harness) -> usize {
    h.platform
        .cache
        .borrow()
        .keys()
        .filter(|k| k.starts_with("flp-status-"))
        .count()
}

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
            path: "/admin/tick.html",
            content_type: "text/html; charset=utf-8",
            identity: Some(b"tick"),
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
    for path in ["/admin", "/admin/tick", "/logs", "/logs?tab=cron"] {
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

fn harness_request(path: &str, headers: &[(&str, &str)], with_cookie: bool) -> Harness {
    let h = Harness::new();
    h.platform.usage.record_fos(
        "GET",
        "processed/request/a.parquet",
        "",
        Some(200),
        1,
        0,
        10,
    );
    let shell = Shell::new(&h.platform, &h.store, &[]);
    let mut map = BTreeMap::from([
        (
            "x-flp-front-key".to_string(),
            "fixture-front-key".to_string(),
        ),
        ("fastly-client-ip".to_string(), "192.0.2.1".to_string()),
    ]);
    if with_cookie {
        map.insert("cookie".into(), h.sessions["admin"].clone().unwrap());
    }
    for (k, v) in headers {
        map.insert((*k).into(), (*v).into());
    }
    let res = shell.handle(&Request {
        method: "GET".into(),
        path: path.into(),
        headers: map,
        body: vec![],
    });
    assert_eq!(res.status, 200);
    h
}

fn compute_row_details(h: &Harness, path: &str) -> Option<Value> {
    let rows = h.rows("api_telemetry");
    let row = rows
        .iter()
        .find(|r| r["source"] == "compute_api" && r["path"] == path)
        .expect("compute_api row");
    row["details_json"]
        .as_str()
        .map(|d| serde_json::from_str(d).unwrap())
}

fn usage_callers(h: &Harness) -> Vec<String> {
    let callers: Vec<String> = h
        .rows("fos_usage")
        .iter()
        .map(|r| r["caller"].as_str().unwrap().to_owned())
        .collect();
    assert!(!callers.is_empty());
    callers
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
