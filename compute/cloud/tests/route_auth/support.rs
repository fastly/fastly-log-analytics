#![allow(dead_code)]
use fastly_log_analytics::{
    admin::{self, fixture::FixtureAdapter, ApiRequest, FosRequest, FosResponse, Record},
    auth::{self, webauthn::Credential, AuthState, Role, User},
    http::{Request, Response},
    setup::SetupStores,
    shell::{CoreCacheEntry, CoreCacheError, Runtime, Shell},
    state::{MemoryStore, StateStore},
    telemetry::{api::ApiLog, usage::UsageAgg},
};
use hmac::{Hmac, KeyInit, Mac};
use serde_json::{json, Value};
use sha2::Sha256;
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
};

pub(super) const SESSION_KEY: &[u8] = b"fixture-session-signing-material-32-bytes";

pub(super) struct Platform {
    pub(super) adapter: FixtureAdapter,
    pub(super) clock: AtomicU64,
    pub(super) cache: std::cell::RefCell<BTreeMap<String, (CoreCacheEntry, Vec<String>)>>,
    pub(super) purges: std::cell::RefCell<Vec<String>>,
    pub(super) vcpu: AtomicU64,
    pub(super) usage: UsageAgg,
    pub(super) api_log: ApiLog,
    pub(super) budget: admin::BackendBudget,
    pub(super) metered: std::cell::Cell<bool>,
    pub(super) kv_reads: std::cell::Cell<u32>,
}

impl Platform {
    pub(super) fn new() -> Self {
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

    pub(super) fn admin_call(&self, method: &str, path: &str, body: Value) -> Value {
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

    pub(super) fn provision(&self) {
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

pub(super) fn user(id: &str, role: Role, now: u64) -> User {
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
        last_login_at: None,
    }
}

pub(super) fn cookie(user: &User, exp: u64) -> String {
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

pub(super) struct Harness {
    pub(super) platform: Platform,
    pub(super) store: MemoryStore,
    pub(super) sessions: BTreeMap<&'static str, Option<String>>,
}

impl Harness {
    pub(super) fn new() -> Self {
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

    pub(super) fn send(&self, method: &str, path: &str, session: &str) -> Response {
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
    pub(super) fn send_with(
        &self,
        method: &str,
        path: &str,
        session: &str,
        origin: Option<&str>,
        body: &[u8],
    ) -> Response {
        self.send_with_headers(method, path, session, origin, body, &[])
    }

    pub(super) fn send_with_headers(
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

    pub(super) fn beacon(&self, session: &str, body: Value) -> Response {
        self.send_with(
            "POST",
            "/api/telemetry/query",
            session,
            Some("https://logs.example.com"),
            body.to_string().as_bytes(),
        )
    }

    pub(super) fn rows(&self, kind: &str) -> Vec<Value> {
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
pub(super) enum Class {
    Public,
    Session,
    NoPiiDenied,
    Admin,
}

pub(super) const ROUTES: &[(&str, &str, Class)] = &[
    ("GET", "/api/services", Class::Session),
    ("GET", "/api/log-fields/catalog", Class::Session),
    ("GET", "/api/rum/versions", Class::Session),
    ("GET", "/api/log-extents", Class::Session),
    ("GET", "/api/files", Class::Session),
    ("GET", "/api/views/svc", Class::Session),
    ("POST", "/api/views", Class::Admin),
    ("POST", "/api/views/", Class::Admin),
    ("DELETE", "/api/views/view-1", Class::Admin),
    ("GET", "/api/ingest/events", Class::Session),
    ("GET", "/api/security/ngwaf-bots", Class::Session),
    ("GET", "/api/file", Class::NoPiiDenied),
    ("HEAD", "/api/file", Class::NoPiiDenied),
    ("POST", "/api/telemetry/query", Class::Session),
    ("GET", "/api/header-badge", Class::Session),
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
    (
        "POST",
        "/api/admin/services/fixtureService/rollback",
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
    (
        "GET",
        "/api/admin/usage/service-stats?from=0&to=3600000",
        Class::Admin,
    ),
    ("GET", "/api/logs/cron_ticks/live", Class::Admin),
    (
        "GET",
        "/api/services/fixtureService/scoring/status",
        Class::Admin,
    ),
    (
        "GET",
        "/api/services/fixtureService/scoring/config",
        Class::Admin,
    ),
    (
        "POST",
        "/api/services/fixtureService/scoring/enable",
        Class::Admin,
    ),
    (
        "POST",
        "/api/services/fixtureService/scoring/disable",
        Class::Admin,
    ),
    (
        "POST",
        "/api/services/fixtureService/scoring/teardown",
        Class::Admin,
    ),
    (
        "POST",
        "/api/services/fixtureService/scoring/retrain",
        Class::Admin,
    ),
    (
        "GET",
        "/api/services/fixtureService/scoring/threshold",
        Class::Admin,
    ),
    (
        "PUT",
        "/api/services/fixtureService/scoring/threshold",
        Class::Admin,
    ),
    (
        "GET",
        "/api/services/fixtureService/scoring/enforce-threshold",
        Class::Admin,
    ),
    (
        "PUT",
        "/api/services/fixtureService/scoring/enforce-threshold",
        Class::Admin,
    ),
    (
        "GET",
        "/api/services/fixtureService/scoring/l2-enforce",
        Class::Admin,
    ),
    (
        "PUT",
        "/api/services/fixtureService/scoring/l2-enforce",
        Class::Admin,
    ),
    (
        "GET",
        "/api/services/fixtureService/scoring/exclude-regex",
        Class::Admin,
    ),
    (
        "PUT",
        "/api/services/fixtureService/scoring/exclude-regex",
        Class::Admin,
    ),
    (
        "POST",
        "/api/services/fixtureService/scoring/exclude-regex/validate",
        Class::Admin,
    ),
    (
        "GET",
        "/api/services/fixtureService/scoring/enforce-status-code",
        Class::Admin,
    ),
    (
        "PUT",
        "/api/services/fixtureService/scoring/enforce-status-code",
        Class::Admin,
    ),
    (
        "POST",
        "/api/services/fixtureService/scoring/rotate-key",
        Class::Admin,
    ),
    (
        "GET",
        "/api/services/fixtureService/scoring/matrix/versions",
        Class::Admin,
    ),
    (
        "POST",
        "/api/services/fixtureService/scoring/matrix/versions/restore",
        Class::Admin,
    ),
    (
        "GET",
        "/api/services/fixtureService/scoring/matrix/active",
        Class::Admin,
    ),
    (
        "GET",
        "/api/services/fixtureService/scoring/labels",
        Class::Admin,
    ),
    (
        "POST",
        "/api/services/fixtureService/scoring/labels",
        Class::Admin,
    ),
    (
        "PATCH",
        "/api/services/fixtureService/scoring/labels/l1",
        Class::Admin,
    ),
    (
        "DELETE",
        "/api/services/fixtureService/scoring/labels/l1",
        Class::Admin,
    ),
    (
        "GET",
        "/api/services/fixtureService/scoring/audit",
        Class::Admin,
    ),
];

// Every `/api/...` string literal in src/shell.rs must be declared here.
// Admin and session entries must also prefix a path exercised in ROUTES.
pub(super) const DECLARED: &[(&str, Class)] = &[
    ("/api/", Class::Public),
    ("/api/auth/", Class::Public),
    ("/api/auth/session", Class::Public),
    ("/api/auth/login/options", Class::Public),
    ("/api/auth/login/verify", Class::Public),
    ("/api/auth/register/options", Class::Public),
    ("/api/auth/register/verify", Class::Public),
    ("/api/auth/logout", Class::Public),
    ("/api/bootstrap", Class::Public),
    ("/api/setup", Class::Public),
    ("/api/setup/accept", Class::Public),
    ("/api/services", Class::Session),
    ("/api/services/", Class::Admin),
    ("/api/log-fields/catalog", Class::Session),
    ("/api/rum/versions", Class::Session),
    ("/api/log-extents", Class::Session),
    ("/api/files", Class::Session),
    ("/api/views", Class::Admin),
    ("/api/views/", Class::Session),
    ("/api/ingest/events", Class::Session),
    ("/api/security/ngwaf-bots", Class::Session),
    ("/api/file", Class::NoPiiDenied),
    ("/api/telemetry/query", Class::Session),
    ("/api/header-badge", Class::Session),
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
    ("/api/admin/users/", Class::Admin),
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

pub(super) fn status_ok_for_admin(status: u16) -> bool {
    status != 401 && status != 403
}

pub(super) fn status_entries(h: &Harness) -> usize {
    h.platform
        .cache
        .borrow()
        .keys()
        .filter(|k| k.starts_with("flp-status-"))
        .count()
}

pub(super) fn harness_request(path: &str, headers: &[(&str, &str)], with_cookie: bool) -> Harness {
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

pub(super) fn compute_row_details(h: &Harness, path: &str) -> Option<Value> {
    let rows = h.rows("api_telemetry");
    let row = rows
        .iter()
        .find(|r| r["source"] == "compute_api" && r["path"] == path)
        .expect("compute_api row");
    row["details_json"]
        .as_str()
        .map(|d| serde_json::from_str(d).unwrap())
}

pub(super) fn usage_callers(h: &Harness) -> Vec<String> {
    let callers: Vec<String> = h
        .rows("fos_usage")
        .iter()
        .map(|r| r["caller"].as_str().unwrap().to_owned())
        .collect();
    assert!(!callers.is_empty());
    callers
}
