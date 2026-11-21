"""Lock-safe compute/cloud shell-test-server, Rust VCL helper, WebAuthn, and Front VCL harness."""

from __future__ import annotations

import atexit
import base64
import fcntl
import hashlib
import hmac
import json
import os
import re
import secrets
import socket
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

REPO_ROOT = Path(__file__).resolve().parents[2]
CLOUD_DIR = REPO_ROOT / "compute" / "cloud"
CLOUD_MANIFEST = CLOUD_DIR / "Cargo.toml"
FRONT_MAIN_VCL = CLOUD_DIR / "vcl" / "front" / "main.vcl"
SCORER_MATRIX_RS = REPO_ROOT / "compute" / "scorer" / "src" / "matrix.rs"

_TMP_ROOT = Path(tempfile.gettempdir())
_UID = os.getuid()
LOCK_FILE = _TMP_ROOT / f"fla-e2e-sec-build-{_UID}.lock"
WEB_OUT_DIR = _TMP_ROOT / f"fla-e2e-sec-web-out-{_UID}"
CARGO_TARGET_DIR = _TMP_ROOT / f"fla-e2e-sec-cargo-target-{_UID}"
DRIVER_DIR = _TMP_ROOT / f"fla-e2e-sec-driver-{_UID}"
STAMP_FILE_NAME = "e2e-cloud-vcl-helper.stamp"

DEFAULT_CLOUD_ORIGIN = "https://logs.example.test"
DEFAULT_SETUP_CODE = "test-sentinel-setup-code-0001"
DEFAULT_FRONT_KEY = "test-sentinel-front-key-0001"  # gitleaks:allow
DEFAULT_CDN_KEY = "test-sentinel-cdn-key-0001"
FIXTURE_22_SID = "AAAAAAAAAAAAAAAAAAAAAA"

_BUILT_PATHS: tuple[Path, Path] | None = None

_HELPER_RS = r"""
use fastly_log_analytics::{
    admin::{
        self, fixture::FixtureAdapter, scoring, stage, ApiRawRequest, ApiRequest, ApiResponse,
        FosRequest, FosResponse, Record, Service,
    },
    auth::{
        self, AuditEntry, AuthConfig, AuthService, AuthState, BrowserCredential, Challenge,
        Credential, RegistrationAuthority, Role, SetupGrant, SetupState, User,
    },
    http::{Request, Response},
    ingest::{self, bundle, convert, storage::Store, summary, worker},
    logging,
    setup::{SetupStores, CONFIG_KEYS, SECRET_KEYS},
    shell::{Runtime, Shell},
    state::{MemoryStore, StateStore},
    telemetry,
};
use serde_json::{json, Value};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::{Cell, RefCell},
    collections::{BTreeMap, VecDeque},
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

thread_local! {
    static ALLOC_CAP: Cell<usize> = const { Cell::new(usize::MAX) };
}

struct CappedAlloc;

unsafe impl GlobalAlloc for CappedAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if ALLOC_CAP.with(|cap| layout.size() > cap.get()) {
            return std::ptr::null_mut();
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if ALLOC_CAP.with(|cap| layout.size() > cap.get()) {
            return std::ptr::null_mut();
        }
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if ALLOC_CAP.with(|cap| new_size > cap.get()) {
            return std::ptr::null_mut();
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static HELPER_ALLOC: CappedAlloc = CappedAlloc;

struct AllocCapGuard;
impl Drop for AllocCapGuard {
    fn drop(&mut self) {
        ALLOC_CAP.with(|cap| cap.set(usize::MAX));
    }
}

#[allow(dead_code)]
#[path = "scorer_matrix_inner.rs"]
mod scorer_matrix;

struct EpochTestRuntime {
    config: BTreeMap<String, String>,
    secrets: BTreeMap<String, String>,
    admin: FixtureAdapter,
    now_secs: AtomicU64,
    rng_ctr: AtomicU64,
    api_status_queue: RefCell<VecDeque<u16>>,
    dict_put_calls: RefCell<Vec<String>>,
    admin_events: RefCell<Vec<Value>>,
}

impl EpochTestRuntime {
    fn new(origin: &str, setup_code: &str, front_key: &str, cdn_key: &str) -> Self {
        let mut config: BTreeMap<String, String> = CONFIG_KEYS
            .iter()
            .map(|name| ((*name).into(), "native-test".into()))
            .collect();
        config.insert("public_base_url".into(), origin.into());
        for (k, v) in [
            ("flp_service_id", "fixtureApp"),
            ("worker_service_id", "fixtureWorker"),
            ("front_service_id", "fixtureFront"),
            ("secret_store_id", "fixtureSecrets"),
            ("config_store_id", "fixtureConfig"),
            ("kv_store_id", "fixtureState"),
            ("buckets_dictionary_id", "fixtureBuckets"),
            ("credentials_dictionary_id", "fixtureCredentials"),
            ("front_auth_dictionary_id", "fixtureAuth"),
        ] {
            config.insert(k.into(), v.into());
        }
        let mut secrets: BTreeMap<String, String> = SECRET_KEYS
            .iter()
            .map(|name| ((*name).into(), "native-test-material-not-a-real-secret".into()))
            .collect();
        secrets.insert("front_key".into(), front_key.into());
        secrets.insert("cdn_key".into(), cdn_key.into());
        secrets.insert("fastly_token".into(), "fixture-token".into());
        secrets.insert("worker_secret".into(), "test-sentinel-worker-key-0001".into());
        secrets.insert("session_key".into(), "s".repeat(48));
        secrets.insert(
            "admin_setup_hash".into(),
            json!({"sha256": auth::hash(setup_code), "expires_at": 1_800_000_900u64}).to_string(),
        );
        Self {
            config,
            secrets,
            admin: FixtureAdapter::new(),
            now_secs: AtomicU64::new(1_800_000_000),
            rng_ctr: AtomicU64::new(1),
            api_status_queue: RefCell::new(VecDeque::new()),
            dict_put_calls: RefCell::new(Vec::new()),
            admin_events: RefCell::new(Vec::new()),
        }
    }

    fn admin_call(&self, method: &str, path: &str, body: Value) -> (u16, Value) {
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

    fn run_admin_job(&self, mut job: Value) -> Value {
        for _ in 0..200 {
            if job["status"] == "complete" || job["status"] == "failed" {
                return job;
            }
            if let Some(at) = job["retry_at"].as_u64() {
                self.now_secs.fetch_max(at, Ordering::SeqCst);
            }
            let id = job["id"].as_str().expect("job id");
            let (status, res) = self.admin_call("POST", &format!("/api/admin/jobs/{id}/step"), json!({}));
            if status == 503 {
                self.now_secs.fetch_add(65, Ordering::SeqCst);
                let (gs, gres) = self.admin_call("GET", &format!("/api/admin/jobs/{id}"), json!({}));
                assert_eq!(gs, 200);
                job = gres["job"].clone();
                continue;
            }
            assert_eq!(status, 200, "job step {id} -> {status}: {res}");
            job = res["job"].clone();
        }
        panic!("job did not finish: {job}");
    }
}

impl SetupStores for EpochTestRuntime {
    fn config_present(&self, name: &str) -> Result<bool, String> {
        Ok(self.config.contains_key(name))
    }
    fn secret_present(&self, name: &str) -> Result<bool, String> {
        Ok(self.secrets.contains_key(name))
    }
    fn state_linked(&self) -> Result<bool, String> {
        Ok(true)
    }
}

impl Runtime for EpochTestRuntime {
    fn config(&self, name: &str) -> Result<Option<String>, String> {
        Ok(self.config.get(name).cloned())
    }
    fn secret(&self, name: &str) -> Result<Option<String>, String> {
        Ok(self.admin.secret(name)?.or_else(|| self.secrets.get(name).cloned()))
    }
    fn random(&self) -> Result<[u8; 32], String> {
        let n = self.rng_ctr.fetch_add(1, Ordering::SeqCst);
        let mut out = [11u8; 32];
        out[..8].copy_from_slice(&n.to_le_bytes());
        Ok(out)
    }
    fn now(&self) -> u64 {
        self.now_secs.load(Ordering::SeqCst)
    }
    fn checks(&self, _: bool) -> Result<Value, String> {
        Err("unused".into())
    }
    fn versions(&self) -> Result<Value, String> {
        Ok(json!({}))
    }
    fn compute_service_id(&self) -> Option<String> {
        None
    }
    fn compute_service_version(&self) -> Option<String> {
        None
    }
    fn admin_get(&self, key: &str) -> Result<Option<Record>, String> {
        self.admin.get(key)
    }
    fn admin_cas(&self, key: &str, generation: Option<&str>, value: &Value) -> Result<bool, String> {
        self.admin.cas(key, generation, value)
    }
    fn admin_api(&self, request: ApiRequest<'_>) -> Result<ApiResponse, String> {
        if request.method == "PUT" && request.path.ends_with("/item/auth_epoch") {
            self.dict_put_calls
                .borrow_mut()
                .push(format!("{} {}", request.method, request.path));
            if let Some(forced_status) = self.api_status_queue.borrow_mut().pop_front() {
                if (200..300).contains(&forced_status) {
                    return self.admin.api(request).map(Into::into);
                }
                return Ok(ApiResponse {
                    status: forced_status,
                    body: json!({"error": "forced"}),
                    retry_after: None,
                });
            }
        }
        self.admin.api(request).map(Into::into)
    }
    fn admin_api_raw(&self, request: ApiRawRequest<'_>) -> Result<ApiResponse, String> {
        self.admin.api_raw(request).map(Into::into)
    }
    fn scorer_package(&self) -> Option<Vec<u8>> {
        Some(b"\0asm\x01\0\0\0synthetic-scorer".to_vec())
    }
    fn admin_fos(&self, request: FosRequest<'_>) -> Result<FosResponse, String> {
        self.admin.fos(request)
    }
    fn admin_event(&self, event: Value) {
        self.admin_events.borrow_mut().push(event);
    }
}

struct MiniBundleRuntime {
    kv: RefCell<BTreeMap<String, (u64, Value)>>,
    bucket: RefCell<BTreeMap<String, Vec<u8>>>,
    calls: Cell<u32>,
    now_s: u64,
}

impl MiniBundleRuntime {
    fn new(now_s: u64) -> Self {
        Self {
            kv: RefCell::new(BTreeMap::new()),
            bucket: RefCell::new(BTreeMap::new()),
            calls: Cell::new(0),
            now_s,
        }
    }
    fn list_xml(&self, query: &str) -> String {
        let q: BTreeMap<String, String> = url::form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .collect();
        let prefix = q.get("prefix").cloned().unwrap_or_default();
        let delim = q.get("delimiter").cloned();
        let bucket = self.bucket.borrow();
        let mut contents = String::new();
        let mut prefixes = std::collections::BTreeSet::new();
        for (k, v) in bucket.iter() {
            if !k.starts_with(&prefix) {
                continue;
            }
            let rest = &k[prefix.len()..];
            if let Some(ref d) = delim {
                if let Some(idx) = rest.find(d.as_str()) {
                    prefixes.insert(format!("{prefix}{}", &rest[..idx + d.len()]));
                    continue;
                }
            }
            contents.push_str(&format!(
                "<Contents><Key>{k}</Key><Size>{}</Size></Contents>",
                v.len()
            ));
        }
        let mut pxml = String::new();
        for p in prefixes {
            pxml.push_str(&format!("<CommonPrefixes><Prefix>{p}</Prefix></CommonPrefixes>"));
        }
        format!("<ListBucketResult><IsTruncated>false</IsTruncated>{contents}{pxml}</ListBucketResult>")
    }
}

impl SetupStores for MiniBundleRuntime {
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

impl Runtime for MiniBundleRuntime {
    fn config(&self, _: &str) -> Result<Option<String>, String> {
        Ok(Some("v".into()))
    }
    fn secret(&self, name: &str) -> Result<Option<String>, String> {
        Ok(Some(format!("sec-{name}")))
    }
    fn random(&self) -> Result<[u8; 32], String> {
        Ok([7u8; 32])
    }
    fn now(&self) -> u64 {
        self.now_s
    }
    fn checks(&self, _: bool) -> Result<Value, String> {
        Err("unused".into())
    }
    fn versions(&self) -> Result<Value, String> {
        Ok(json!({}))
    }
    fn compute_service_id(&self) -> Option<String> {
        None
    }
    fn compute_service_version(&self) -> Option<String> {
        None
    }
    fn admin_remaining_calls(&self) -> u32 {
        64u32.saturating_sub(self.calls.get())
    }
    fn admin_get(&self, key: &str) -> Result<Option<Record>, String> {
        self.calls.set(self.calls.get() + 1);
        Ok(self.kv.borrow().get(key).map(|(g, v)| Record {
            generation: g.to_string(),
            value: v.clone(),
        }))
    }
    fn admin_cas(&self, key: &str, generation: Option<&str>, value: &Value) -> Result<bool, String> {
        self.calls.set(self.calls.get() + 1);
        let mut kv = self.kv.borrow_mut();
        let cur = kv.get(key).map(|(g, _)| g.to_string());
        if cur.as_deref() != generation {
            return Ok(false);
        }
        let next = kv.get(key).map_or(1, |(g, _)| g + 1);
        kv.insert(key.into(), (next, value.clone()));
        Ok(true)
    }
    fn admin_fos(&self, request: FosRequest<'_>) -> Result<FosResponse, String> {
        self.calls.set(self.calls.get() + 1);
        match request.method {
            "GET" if request.query.contains("list-type") => Ok(FosResponse {
                cdn_cache: None,
                status: 200,
                body: self.list_xml(request.query).into_bytes(),
                retry_after: None,
            }),
            "GET" => {
                let b = self.bucket.borrow().get(request.key).cloned();
                Ok(FosResponse {
                    cdn_cache: None,
                    status: if b.is_some() { 200 } else { 404 },
                    body: b.unwrap_or_default(),
                    retry_after: None,
                })
            }
            "PUT" => {
                self.bucket.borrow_mut().insert(request.key.into(), request.body);
                Ok(FosResponse {
                    cdn_cache: None,
                    status: 200,
                    body: vec![],
                    retry_after: None,
                })
            }
            "DELETE" => {
                self.bucket.borrow_mut().remove(request.key);
                Ok(FosResponse {
                    cdn_cache: None,
                    status: 204,
                    body: vec![],
                    retry_after: None,
                })
            }
            "POST" if request.query == "delete" => {
                let xml = String::from_utf8_lossy(&request.body);
                let mut out = String::from("<DeleteResult>");
                for part in xml.split("<Key>").skip(1) {
                    if let Some(k) = part.split("</Key>").next() {
                        self.bucket.borrow_mut().remove(k);
                        out.push_str(&format!("<Deleted><Key>{k}</Key></Deleted>"));
                    }
                }
                out.push_str("</DeleteResult>");
                Ok(FosResponse {
                    cdn_cache: None,
                    status: 200,
                    body: out.into_bytes(),
                    retry_after: None,
                })
            }
            _ => Ok(FosResponse {
                cdn_cache: None,
                status: 405,
                body: vec![],
                retry_after: None,
            }),
        }
    }
    fn admin_cdn_get(
        &self,
        _service_id: &str,
        key: &str,
        _cdn_key: &str,
        max_bytes: usize,
        _timeout: std::time::Duration,
    ) -> Result<FosResponse, String> {
        self.calls.set(self.calls.get() + 1);
        let b = self.bucket.borrow().get(key).cloned();
        if b.as_ref().is_some_and(|bytes| bytes.len() > max_bytes) {
            return Err("cdn_body_too_large".into());
        }
        Ok(FosResponse {
            cdn_cache: None,
            status: if b.is_some() { 200 } else { 404 },
            body: b.unwrap_or_default(),
            retry_after: None,
        })
    }
}

fn seal_test_session(sub: &str, role: &str, policy_version: u64, iat: u64, exp: u64, jti: &str) -> String {
    compute_common::crypto::seal_json(
        b"ssssssssssssssssssssssssssssssssssssssssssssssss",
        &json!({
            "sub": sub,
            "role": role,
            "policy_version": policy_version,
            "exp": exp,
            "iat": iat,
            "jti": jti,
        }),
    )
    .expect("seal session")
}

fn shell_req(
    method: &str,
    path: &str,
    front_key: &str,
    origin: &str,
    cookie: Option<&str>,
    body: Value,
) -> Request {
    let mut headers = BTreeMap::from([
        ("x-flp-front-key".into(), front_key.into()),
        ("x-flp-front-rl".into(), "1".into()),
        ("fastly-client-ip".into(), "198.51.100.10".into()),
    ]);
    if method != "GET" && method != "HEAD" {
        headers.insert("origin".into(), origin.into());
        headers.insert("content-type".into(), "application/json".into());
    }
    if let Some(c) = cookie {
        headers.insert("cookie".into(), c.into());
    }
    Request {
        method: method.into(),
        path: path.into(),
        headers,
        body: if method == "GET" || method == "HEAD" {
            vec![]
        } else {
            serde_json::to_vec(&body).unwrap()
        },
    }
}

fn resp_json(r: &Response) -> Value {
    serde_json::from_slice(&r.body).unwrap_or(Value::Null)
}

fn main() {
    let mut args = std::env::args().skip(1);
    let cmd = args.next().expect("command required");
    let payload_str = args.next().unwrap_or_else(|| "{}".to_owned());
    let payload: Value = serde_json::from_str(&payload_str).expect("valid JSON arg");
    match cmd.as_str() {
        "desired_snippets" => {
            match logging::desired_snippets(&payload) {
                Ok(v) => println!("{}", serde_json::to_string(&v).unwrap()),
                Err(e) => {
                    eprintln!("{e}");
                    std::process::exit(2);
                }
            }
        }
        "rum_tracker_js" => {
            let sid = payload["service_id"].as_str().unwrap_or("svc");
            let cfg: logging::rum::Config = match payload.get("config") {
                Some(c) if !c.is_null() => serde_json::from_value(c.clone()).unwrap_or_default(),
                _ => logging::rum::Config::default(),
            };
            let js = logging::rum::generate_rum_tracker_js(sid, &cfg);
            println!("{}", serde_json::to_string(&js).unwrap());
        }
        "issue_cdn_cookie" => {
            let sid = payload["service_id"].as_str().unwrap_or("AAAAAAAAAAAAAAAAAAAAAA");
            let scope = payload["scope"].as_str().unwrap_or("logs");
            let role_str = payload["role"].as_str().unwrap_or("admin");
            let role = match role_str {
                "admin" => Role::Admin,
                "analyst" => Role::Analyst,
                _ => Role::AnalystNoPii,
            };
            let session_cookie = payload["session_cookie"].as_str().unwrap_or("sess");
            let auth_epoch = payload["auth_epoch"].as_u64().unwrap_or(1);
            let now = payload["now"].as_u64().unwrap_or(1_718_000_000);
            let cdn_key = payload["cdn_key"].as_str().unwrap_or("test-sentinel-cdn-key-0001");
            let cookie = auth::issue_cdn_cookie(
                sid,
                scope,
                role,
                session_cookie,
                auth_epoch,
                now,
                cdn_key,
            );
            println!(
                "{}",
                serde_json::to_string(&json!({
                    "set_cookie": cookie,
                    "ttl": auth::CDN_COOKIE_TTL,
                }))
                .unwrap()
            );
        }
        "audit_entries_check" => {
            let store = MemoryStore::default();
            let mut state = AuthState::default();
            let mk_cred = |id: &str| -> Credential {
                serde_json::from_value(json!({
                    "id": id,
                    "public_key": vec![3u8; 32],
                    "algorithm": -8,
                    "sign_count": 0,
                    "created_at": 100,
                    "revoked": false,
                }))
                .expect("valid test credential")
            };
            state.users.insert(
                "admin-1".into(),
                User {
                    id: "admin-1".into(),
                    display_name: "Admin".into(),
                    role: Role::Admin,
                    credentials: vec![mk_cred("cred-admin")],
                    revoked: false,
                    created_by: "setup".into(),
                    created_at: 100,
                    last_login_at: Some(100),
                },
            );
            state.users.insert(
                "analyst-1".into(),
                User {
                    id: "analyst-1".into(),
                    display_name: "Analyst".into(),
                    role: Role::Analyst,
                    credentials: vec![mk_cred("cred-analyst")],
                    revoked: false,
                    created_by: "admin-1".into(),
                    created_at: 101,
                    last_login_at: Some(101),
                },
            );
            state.users.insert(
                "nopii-1".into(),
                User {
                    id: "nopii-1".into(),
                    display_name: "NoPii".into(),
                    role: Role::AnalystNoPii,
                    credentials: vec![mk_cred("cred-nopii")],
                    revoked: false,
                    created_by: "admin-1".into(),
                    created_at: 102,
                    last_login_at: Some(102),
                },
            );
            state.audit.push(AuditEntry {
                actor: "admin-1".into(),
                action: "user_registered".into(),
                target: "admin-1".into(),
                time: 100,
                ip: "198.51.100.10".into(),
            });
            assert!(store.compare_and_swap(None, &state).unwrap());
            let cfg = AuthConfig::new("https://logs.example.test", vec![7u8; 48], None).unwrap();
            let svc = AuthService::new(&store, &cfg);
            let analyst_res = svc.audit_entries("analyst-1");
            let nopii_res = svc.audit_entries("nopii-1");
            let anon_res = svc.audit_entries("anonymous");
            let admin_res = svc.audit_entries("admin-1");
            println!(
                "{}",
                serde_json::to_string(&json!({
                    "analyst_forbidden": matches!(analyst_res, Err(auth::Error::Forbidden)),
                    "nopii_forbidden": matches!(nopii_res, Err(auth::Error::Forbidden)),
                    "anon_forbidden": matches!(anon_res, Err(auth::Error::Forbidden)),
                    "admin_count": admin_res.map(|v| v.len()).unwrap_or(0),
                }))
                .unwrap()
            );
        }
        "revoke_epoch_check" => {
            let origin = payload["origin"].as_str().unwrap_or("https://logs.example.test");
            let setup_code = payload["setup_code"].as_str().unwrap_or("test-sentinel-setup-code-0001");
            let front_key = payload["front_key"].as_str().unwrap_or("test-sentinel-front-key-0001");
            let cdn_key = payload["cdn_key"].as_str().unwrap_or("test-sentinel-cdn-key-0001");
            let challenge = payload["challenge"].as_str().expect("challenge required");
            let cred: BrowserCredential = serde_json::from_value(payload["credential"].clone()).expect("valid credential");

            let now = 1_800_000_000u64;
            let runtime = EpochTestRuntime::new(origin, setup_code, front_key, cdn_key);
            let store = MemoryStore::default();
            let generation = auth::hash(setup_code);
            let grant_hash = auth::hash("grant-token-1");
            let mut initial = AuthState {
                setup: SetupState {
                    hash_generation: generation.clone(),
                    failures: 0,
                    spent: false,
                },
                ..Default::default()
            };
            initial.setup_grants.insert(
                grant_hash.clone(),
                SetupGrant {
                    generation: generation.clone(),
                    expires_at: now + 600,
                },
            );
            initial.challenges.insert(
                "chal-admin".into(),
                Challenge {
                    challenge: challenge.into(),
                    expires_at: now + 300,
                    registration: Some(RegistrationAuthority::Setup {
                        grant_hash,
                        generation: generation.clone(),
                    }),
                    display_name: "Admin".into(),
                    user_id: "admin-1".into(),
                },
            );
            assert!(store.compare_and_swap(None, &initial).unwrap());

            let cfg = AuthConfig::from_hash(
                origin,
                b"s".repeat(48),
                runtime.secret("admin_setup_hash").unwrap(),
            )
            .unwrap();
            let svc = AuthService::new(&store, &cfg);
            let signed = svc.finish_registration("chal-admin", &cred, "198.51.100.10", now).unwrap();

            // Seed two active analysts to exercise role downgrade + user revocation through Shell::handle
            let loaded = store.load().unwrap();
            let mut st = loaded.state.clone();
            for (uid, name) in [("analyst-1", "Analyst One"), ("analyst-2", "Analyst Two")] {
                st.users.insert(
                    uid.into(),
                    User {
                        id: uid.into(),
                        display_name: name.into(),
                        role: Role::Analyst,
                        credentials: signed.user.credentials.clone(),
                        revoked: false,
                        created_by: "admin-1".into(),
                        created_at: now,
                        last_login_at: Some(now),
                    },
                );
            }
            assert!(store.compare_and_swap(loaded.generation.as_deref(), &st).unwrap());

            let epoch_initial = svc.auth_epoch().unwrap();
            let shell = Shell::new(&runtime, &store, &[]);

            let r_role = shell.handle(&Request {
                method: "PATCH".into(),
                path: "/api/admin/users/analyst-1".into(),
                headers: BTreeMap::from([
                    ("x-flp-front-key".into(), front_key.into()),
                    ("x-flp-front-rl".into(), "1".into()),
                    ("fastly-client-ip".into(), "198.51.100.10".into()),
                    ("origin".into(), origin.into()),
                    ("content-type".into(), "application/json".into()),
                    ("cookie".into(), format!("flp_session={}", signed.cookie)),
                ]),
                body: serde_json::to_vec(&json!({"role": "analyst-no-pii"})).unwrap(),
            });
            let epoch_after_role = svc.auth_epoch().unwrap();
            let dict_after_role = runtime
                .admin
                .dictionary_items("fixtureAuth")
                .get("auth_epoch")
                .cloned()
                .unwrap_or_default();

            let r_revoke = shell.handle(&Request {
                method: "POST".into(),
                path: "/api/admin/users/analyst-2/revoke".into(),
                headers: BTreeMap::from([
                    ("x-flp-front-key".into(), front_key.into()),
                    ("x-flp-front-rl".into(), "1".into()),
                    ("fastly-client-ip".into(), "198.51.100.10".into()),
                    ("origin".into(), origin.into()),
                    ("content-type".into(), "application/json".into()),
                    ("cookie".into(), format!("flp_session={}", signed.cookie)),
                ]),
                body: b"{}".to_vec(),
            });
            let epoch_after_revoke = svc.auth_epoch().unwrap();
            let dict_after_revoke = runtime
                .admin
                .dictionary_items("fixtureAuth")
                .get("auth_epoch")
                .cloned()
                .unwrap_or_default();

            println!(
                "{}",
                serde_json::to_string(&json!({
                    "role_status": r_role.status,
                    "revoke_status": r_revoke.status,
                    "epoch_initial": epoch_initial,
                    "epoch_after_role": epoch_after_role,
                    "dict_after_role": dict_after_role,
                    "epoch_after_revoke": epoch_after_revoke,
                    "dict_after_revoke": dict_after_revoke,
                }))
                .unwrap()
            );
        }
        "scoring_schema_check" => {
            let origin = "https://logs.example.test";
            let setup_code = "test-sentinel-setup-code-0001";
            let front_val = "f".repeat(28);
            let front_key = front_val.as_str();
            let cdn_key = "test-sentinel-cdn-key-0001";
            let runtime = EpochTestRuntime::new(origin, setup_code, front_key, cdn_key);
            let store = MemoryStore::default();
            let now = runtime.now();
            let mut initial = AuthState {
                setup: SetupState {
                    hash_generation: auth::hash(setup_code),
                    failures: 0,
                    spent: true,
                },
                ..Default::default()
            };
            let cred = Credential {
                id: "cred-admin".into(),
                public_key: vec![3u8; 32],
                sign_count: 1,
                revoked: false,
            };
            initial.users.insert(
                "admin-1".into(),
                User {
                    id: "admin-1".into(),
                    display_name: "Admin".into(),
                    role: Role::Admin,
                    credentials: vec![cred.clone()],
                    revoked: false,
                    created_by: "setup".into(),
                    created_at: now,
                    last_login_at: Some(now),
                },
            );
            initial.users.insert(
                "analyst-1".into(),
                User {
                    id: "analyst-1".into(),
                    display_name: "Analyst".into(),
                    role: Role::Analyst,
                    credentials: vec![cred.clone()],
                    revoked: false,
                    created_by: "admin-1".into(),
                    created_at: now,
                    last_login_at: Some(now),
                },
            );
            initial.users.insert(
                "nopii-1".into(),
                User {
                    id: "nopii-1".into(),
                    display_name: "NoPii".into(),
                    role: Role::AnalystNoPii,
                    credentials: vec![cred],
                    revoked: false,
                    created_by: "admin-1".into(),
                    created_at: now,
                    last_login_at: Some(now),
                },
            );
            assert!(store.compare_and_swap(None, &initial).unwrap());
            let admin_cookie = format!(
                "flp_session={}",
                seal_test_session("admin-1", "admin", 0, now, now + 3600, "jti-admin-sc")
            );
            let analyst_cookie = format!(
                "flp_session={}",
                seal_test_session("analyst-1", "analyst", 0, now, now + 3600, "jti-analyst-sc")
            );
            let nopii_cookie = format!(
                "flp_session={}",
                seal_test_session("nopii-1", "analyst-no-pii", 0, now, now + 3600, "jti-nopii-sc")
            );

            // 1. Provision fixtureService
            let (p_st, p_body) = runtime.admin_call(
                "POST",
                "/api/admin/provision/execute",
                json!({
                    "token": "fixture-token",
                    "service_id": "fixtureService",
                    "fos_bucket": "fos-fixture-logs",
                    "fos_region": "us-east-1",
                    "log_fields": {
                        "groups": ["A", "C"],
                        "custom_fields": [],
                        "field_overrides": {},
                        "field_limits": {}
                    }
                }),
            );
            assert_eq!(p_st, 200, "provision execute: {p_body}");
            let done_prov = runtime.run_admin_job(p_body["job"].clone());
            assert_eq!(done_prov["status"], "complete", "provision job: {done_prov}");

            // 2. With scoring.enabled == false, reserved names are rejected on PATCH, preview, and custom/validate
            let shell = Shell::new(&runtime, &store, &[]);
            let mut disabled_reserved = BTreeMap::new();
            for reserved_name in [
                "rum_lcp",
                "rum_cid",
                "service_id",
                "fastly_req_id",
                "edge_score",
                "edge_custom",
            ] {
                let (_, sget) = runtime.admin_call("GET", "/api/admin/services/fixtureService", json!({}));
                let gen = sget["generation"].as_str().unwrap().to_owned();
                let cf = json!({
                    "name": reserved_name,
                    "label": "Reserved",
                    "description": "attempt",
                    "vcl_log_expression": "req.http.X-Test",
                    "collection_stage": "edge",
                    "sql_type": "VARCHAR",
                    "duckdb_type": "VARCHAR",
                    "value_type": "string",
                    "bytes_estimate": 32,
                    "enabled": true
                });
                let r_patch = shell.handle(&shell_req(
                    "PATCH",
                    "/api/admin/services/fixtureService",
                    front_key,
                    origin,
                    Some(&admin_cookie),
                    json!({
                        "generation": gen,
                        "log_fields": {
                            "groups": ["A", "C"],
                            "custom_fields": [cf.clone()],
                            "field_overrides": {},
                            "field_limits": {}
                        }
                    }),
                ));
                let r_prev = shell.handle(&shell_req(
                    "POST",
                    "/api/admin/log-fields/preview",
                    front_key,
                    origin,
                    Some(&admin_cookie),
                    json!({
                        "groups": ["A", "C"],
                        "custom_fields": [cf.clone()],
                        "field_overrides": {},
                        "field_limits": {}
                    }),
                ));
                let r_val = shell.handle(&shell_req(
                    "POST",
                    "/api/admin/log-fields/custom/validate",
                    front_key,
                    origin,
                    Some(&admin_cookie),
                    cf,
                ));
                let val_json = resp_json(&r_val);
                disabled_reserved.insert(
                    reserved_name.to_owned(),
                    json!({
                        "patch_status": r_patch.status,
                        "patch_error": resp_json(&r_patch)["error"],
                        "preview_status": r_prev.status,
                        "validate_valid": val_json["valid"],
                    }),
                );
            }

            // Empty custom/validate {} must fail with Missing required field: 'duckdb_type'
            let r_empty_val = shell.handle(&shell_req(
                "POST",
                "/api/admin/log-fields/custom/validate",
                front_key,
                origin,
                Some(&admin_cookie),
                json!({}),
            ));
            let empty_val_json = resp_json(&r_empty_val);

            // 3. Enable session scoring on fixtureService
            let (en_st, en_body) = runtime.admin_call(
                "POST",
                "/api/services/fixtureService/scoring/enable",
                json!({}),
            );
            assert_eq!(en_st, 200, "scoring enable: {en_body}");
            let done_en = runtime.run_admin_job(en_body["job"].clone());
            assert_eq!(done_en["status"], "complete", "scoring enable job: {done_en}");

            // Inspect RequestSchema::for_fields(&svc["log_fields"])
            let svc_rec = runtime.admin_get("svc:fixtureService").unwrap().unwrap().value;
            let schema0 = convert::RequestSchema::for_fields(&svc_rec["log_fields"]);
            let hash0 = schema0.hash().to_owned();
            let arrow0 = schema0.arrow();
            let arrow_fields: Vec<Value> = scoring::SCORING_FIELD_NAMES
                .iter()
                .map(|fname| {
                    let f = arrow0.field_with_name(fname).ok();
                    json!({
                        "name": fname,
                        "present": f.is_some(),
                        "dtype": f.map(|x| format!("{:?}", x.data_type())),
                    })
                })
                .collect();
            let custom_slots: Vec<Value> = schema0
                .custom()
                .iter()
                .map(|c| json!({"name": c.name, "slot_only": c.slot_only}))
                .collect();

            // 4. Canonical scoring_custom_fields() + user lt_run_id succeeds on PATCH
            let canon_scoring = scoring::scoring_custom_fields();
            let user_cf = json!({
                "name": "lt_run_id",
                "label": "LT Run ID",
                "description": "Load test correlation",
                "vcl_log_expression": "req.http.X-LT-Run",
                "collection_stage": "edge",
                "sql_type": "VARCHAR",
                "duckdb_type": "VARCHAR",
                "value_type": "string",
                "bytes_estimate": 32,
                "enabled": true
            });
            let mut valid_with_user = canon_scoring.clone();
            valid_with_user.push(user_cf);
            let (_, sget_valid) = runtime.admin_call("GET", "/api/admin/services/fixtureService", json!({}));
            let gen_valid = sget_valid["generation"].as_str().unwrap().to_owned();
            let r_valid_patch = shell.handle(&shell_req(
                "PATCH",
                "/api/admin/services/fixtureService",
                front_key,
                origin,
                Some(&admin_cookie),
                json!({
                    "generation": gen_valid,
                    "log_fields": {
                        "groups": ["A", "C"],
                        "custom_fields": valid_with_user,
                        "field_overrides": {},
                        "field_limits": {}
                    }
                }),
            ));
            assert_eq!(r_valid_patch.status, 200, "valid scoring+user patch failed: {}", resp_json(&r_valid_patch));

            // 5. Tamper matrix on active-scoring edge_* fields (before committed=true)
            let mk_tampered = |mutator: &dyn Fn(&mut Vec<Value>)| -> (u16, String, u16) {
                let mut fields = scoring::scoring_custom_fields();
                mutator(&mut fields);
                let (_, sg) = runtime.admin_call("GET", "/api/admin/services/fixtureService", json!({}));
                let g = sg["generation"].as_str().unwrap().to_owned();
                let rp = shell.handle(&shell_req(
                    "PATCH",
                    "/api/admin/services/fixtureService",
                    front_key,
                    origin,
                    Some(&admin_cookie),
                    json!({
                        "generation": g,
                        "log_fields": {
                            "groups": ["A", "C"],
                            "custom_fields": fields.clone(),
                            "field_overrides": {},
                            "field_limits": {}
                        }
                    }),
                ));
                let rprev = shell.handle(&shell_req(
                    "POST",
                    "/api/admin/log-fields/preview",
                    front_key,
                    origin,
                    Some(&admin_cookie),
                    json!({
                        "groups": ["A", "C"],
                        "custom_fields": fields,
                        "field_overrides": {},
                        "field_limits": {}
                    }),
                ));
                let err_code = resp_json(&rp)["error"].as_str().unwrap_or("").to_owned();
                (rp.status, err_code, rprev.status)
            };

            let mut tamper_results = BTreeMap::new();
            for (case_name, res) in [
                ("shadow_vcl", mk_tampered(&|f| { f[0]["vcl"] = json!("req.http.Secret"); })),
                ("shadow_parquet_type", mk_tampered(&|f| { f[0]["parquet_type"] = json!("UTF8"); })),
                ("system_managed_false", mk_tampered(&|f| { f[0]["system_managed"] = json!(false); })),
                ("system_managed_true", mk_tampered(&|f| { f[0]["system_managed"] = json!(true); })),
                ("forged_committed", mk_tampered(&|f| { f[0]["committed"] = json!(true); })),
                ("mutated_vcl_expr", mk_tampered(&|f| { f[0]["vcl_log_expression"] = json!("req.http.X-Evil"); })),
                ("swapped_sql_type", mk_tampered(&|f| { f[0]["sql_type"] = json!("VARCHAR"); f[0]["duckdb_type"] = json!("VARCHAR"); })),
                ("byte_limit_neg", mk_tampered(&|f| { f[0]["byte_limit"] = json!(-5); })),
                ("byte_limit_zero", mk_tampered(&|f| { f[0]["byte_limit"] = json!(0); })),
                ("byte_limit_over", mk_tampered(&|f| { f[0]["byte_limit"] = json!(16385); })),
                ("byte_limit_bool", mk_tampered(&|f| { f[0]["byte_limit"] = json!(true); })),
                ("byte_limit_str", mk_tampered(&|f| { f[0]["byte_limit"] = json!("64"); })),
                ("altered_bytes_estimate", mk_tampered(&|f| { f[0]["bytes_estimate"] = json!(999); })),
                ("control_char_label", mk_tampered(&|f| { f[0]["label"] = json!("Edge\x07Score"); })),
                ("overlen_label", mk_tampered(&|f| { f[0]["label"] = json!("L".repeat(81)); })),
                ("overlen_desc", mk_tampered(&|f| { f[0]["description"] = json!("D".repeat(501)); })),
                ("duplicate_edge_score", mk_tampered(&|f| { let dup = f[0].clone(); f.push(dup); })),
            ] {
                tamper_results.insert(
                    case_name.to_owned(),
                    json!({"patch_status": res.0, "patch_error": res.1, "preview_status": res.2}),
                );
            }

            // 6. Mark all 9 edge_* fields committed=true in KV and test sql_type-only PATCH invariance + 409 committed_field_locked
            let cur_rec = runtime.admin_get("svc:fixtureService").unwrap().unwrap();
            let mut committed_val = cur_rec.value.clone();
            if let Some(arr) = committed_val["log_fields"]["custom_fields"].as_array_mut() {
                arr.retain(|item| {
                    item["name"]
                        .as_str()
                        .is_some_and(|n| scoring::SCORING_FIELD_NAMES.contains(&n))
                });
                for item in arr.iter_mut() {
                    item["committed"] = json!(true);
                }
            }
            assert!(runtime
                .admin_cas("svc:fixtureService", Some(&cur_rec.generation), &committed_val)
                .unwrap());

            let (_, sg_c1) = runtime.admin_call("GET", "/api/admin/services/fixtureService", json!({}));
            let gen_c1 = sg_c1["generation"].as_str().unwrap().to_owned();
            let mut with_duckdb = scoring::scoring_custom_fields();
            for item in &mut with_duckdb {
                item["committed"] = json!(true);
            }
            let r_c_both = shell.handle(&shell_req(
                "PATCH",
                "/api/admin/services/fixtureService",
                front_key,
                origin,
                Some(&admin_cookie),
                json!({
                    "generation": gen_c1,
                    "log_fields": {
                        "groups": ["A", "C"],
                        "custom_fields": with_duckdb,
                        "field_overrides": {},
                        "field_limits": {}
                    }
                }),
            ));
            let hash_with_duckdb = convert::RequestSchema::for_fields(
                &runtime.admin_get("svc:fixtureService").unwrap().unwrap().value["log_fields"],
            )
            .hash()
            .to_owned();

            let (_, sg_c2) = runtime.admin_call("GET", "/api/admin/services/fixtureService", json!({}));
            let gen_c2 = sg_c2["generation"].as_str().unwrap().to_owned();
            let mut sql_only = scoring::scoring_custom_fields();
            for item in &mut sql_only {
                item["committed"] = json!(true);
                if let Some(obj) = item.as_object_mut() {
                    obj.remove("duckdb_type");
                }
            }
            let r_c_sql_only = shell.handle(&shell_req(
                "PATCH",
                "/api/admin/services/fixtureService",
                front_key,
                origin,
                Some(&admin_cookie),
                json!({
                    "generation": gen_c2,
                    "log_fields": {
                        "groups": ["A", "C"],
                        "custom_fields": sql_only.clone(),
                        "field_overrides": {},
                        "field_limits": {}
                    }
                }),
            ));
            let hash_sql_only = convert::RequestSchema::for_fields(
                &runtime.admin_get("svc:fixtureService").unwrap().unwrap().value["log_fields"],
            )
            .hash()
            .to_owned();

            // Dropping committed edge_score -> 409 committed_field_locked
            let (_, sg_c3) = runtime.admin_call("GET", "/api/admin/services/fixtureService", json!({}));
            let gen_c3 = sg_c3["generation"].as_str().unwrap().to_owned();
            let mut dropped_committed = sql_only;
            dropped_committed.remove(0);
            let r_drop_committed = shell.handle(&shell_req(
                "PATCH",
                "/api/admin/services/fixtureService",
                front_key,
                origin,
                Some(&admin_cookie),
                json!({
                    "generation": gen_c3,
                    "log_fields": {
                        "groups": ["A", "C"],
                        "custom_fields": dropped_committed,
                        "field_overrides": {},
                        "field_limits": {}
                    }
                }),
            ));

            // 7. Scoring retrain/restore matrix version validation (BUG-CLOUD-RUST-04)
            let (rt_ok_st, _) = runtime.admin_call(
                "POST",
                "/api/services/fixtureService/scoring/retrain",
                json!({
                    "matrix": {
                        "version": "2027-01-15-v1",
                        "vocab_size": 2,
                        "session_count": 4,
                        "transition_count": 6,
                        "counts": {"/": {"/checkout": 6}},
                        "row_totals": {"/": 6}
                    }
                }),
            );
            let long_v = "a".repeat(65);
            let mut bad_matrix_versions = Vec::new();
            for bad_v in ["index", "..", ".", "", long_v.as_str(), "v1\0", "a..b"] {
                let (rs1, rb1) = runtime.admin_call(
                    "POST",
                    "/api/services/fixtureService/scoring/retrain",
                    json!({
                        "matrix": {
                            "version": bad_v,
                            "vocab_size": 2,
                            "session_count": 1,
                            "transition_count": 1,
                            "counts": {"/": {"/checkout": 1}},
                            "row_totals": {"/": 1}
                        }
                    }),
                );
                let (rs2, rb2) = runtime.admin_call(
                    "POST",
                    "/api/services/fixtureService/scoring/matrix/versions/restore?confirm=true",
                    json!({"version": bad_v}),
                );
                bad_matrix_versions.push(json!({
                    "retrain_status": rs1,
                    "retrain_error": rb1["error"],
                    "restore_status": rs2,
                    "restore_error": rb2["error"],
                }));
            }
            let (mv_st, mv_body) = runtime.admin_call(
                "GET",
                "/api/services/fixtureService/scoring/matrix/versions",
                json!({}),
            );

            // 8. Non-admin service redaction + Admin scoring secret redaction (GAP-C09 & GAP-CLOUD-B08)
            let r_svc_analyst = shell.handle(&shell_req(
                "GET",
                "/api/services",
                front_key,
                origin,
                Some(&analyst_cookie),
                json!({}),
            ));
            let r_svc_nopii = shell.handle(&shell_req(
                "GET",
                "/api/services",
                front_key,
                origin,
                Some(&nopii_cookie),
                json!({}),
            ));
            let r_svc_admin = shell.handle(&shell_req(
                "GET",
                "/api/admin/services",
                front_key,
                origin,
                Some(&admin_cookie),
                json!({}),
            ));
            let (_, sc_settings) = runtime.admin_call(
                "GET",
                "/api/services/fixtureService/scoring/settings",
                json!({}),
            );

            // 9. Rust scoring backend TLS + desired_scoring_snippets (GAP-C11)
            let scoring_backend = scoring::desired_scoring_backend("scorer.edgecompute.app");
            let scoring_snippets = scoring::desired_scoring_snippets(
                "fixtureService",
                "test-scorer-req-secret-0001",
                Some("^/healthz$"),
                Some(429),
            )
            .expect("desired_scoring_snippets");

            println!(
                "{}",
                serde_json::to_string(&json!({
                    "disabled_reserved": disabled_reserved,
                    "empty_validate": empty_val_json,
                    "hash0": hash0,
                    "arrow_fields": arrow_fields,
                    "custom_slots": custom_slots,
                    "tamper_results": tamper_results,
                    "patch_both_status": r_c_both.status,
                    "hash_with_duckdb": hash_with_duckdb,
                    "patch_sql_only_status": r_c_sql_only.status,
                    "hash_sql_only": hash_sql_only,
                    "drop_committed_status": r_drop_committed.status,
                    "drop_committed_error": resp_json(&r_drop_committed)["error"],
                    "retrain_v1_status": rt_ok_st,
                    "bad_matrix_versions": bad_matrix_versions,
                    "matrix_versions_status": mv_st,
                    "matrix_versions": mv_body,
                    "analyst_services": resp_json(&r_svc_analyst),
                    "nopii_services": resp_json(&r_svc_nopii),
                    "admin_services_text": String::from_utf8_lossy(&r_svc_admin.body).into_owned(),
                    "scoring_settings_text": sc_settings.to_string(),
                    "scoring_backend": scoring_backend,
                    "scoring_snippets": scoring_snippets,
                }))
                .unwrap()
            );
        }
        "logout_ring_floor_check" => {
            let origin = "https://logs.example.test";
            let setup_code = "test-sentinel-setup-code-0001";
            let front_val = "f".repeat(28);
            let front_key = front_val.as_str();
            let cdn_key = "test-sentinel-cdn-key-0001";
            let runtime = EpochTestRuntime::new(origin, setup_code, front_key, cdn_key);
            let store = MemoryStore::default();
            let base_t = 1_800_000_000u64;
            let mut initial = AuthState {
                setup: SetupState {
                    hash_generation: auth::hash(setup_code),
                    failures: 0,
                    spent: true,
                },
                ..Default::default()
            };
            let cred = Credential {
                id: "cred-1".into(),
                public_key: vec![5u8; 32],
                sign_count: 1,
                revoked: false,
            };
            initial.users.insert(
                "admin-1".into(),
                User {
                    id: "admin-1".into(),
                    display_name: "Admin".into(),
                    role: Role::Admin,
                    credentials: vec![cred.clone()],
                    revoked: false,
                    created_by: "setup".into(),
                    created_at: base_t,
                    last_login_at: Some(base_t),
                },
            );
            initial.users.insert(
                "analyst-1".into(),
                User {
                    id: "analyst-1".into(),
                    display_name: "Analyst".into(),
                    role: Role::Analyst,
                    credentials: vec![cred],
                    revoked: false,
                    created_by: "admin-1".into(),
                    created_at: base_t,
                    last_login_at: Some(base_t),
                },
            );
            assert!(store.compare_and_swap(None, &initial).unwrap());
            let shell = Shell::new(&runtime, &store, &[]);

            let early_s = seal_test_session(
                "admin-1",
                "admin",
                0,
                base_t,
                base_t + auth::SESSION_TTL,
                "jti-early-0",
            );
            let mut admin_tokens = Vec::new();
            for idx in 1..=68u64 {
                let t = base_t + idx;
                let tok = seal_test_session(
                    "admin-1",
                    "admin",
                    0,
                    t,
                    t + auth::SESSION_TTL,
                    &format!("jti-adm-{idx}"),
                );
                runtime.now_secs.store(t, Ordering::SeqCst);
                let r_lo = shell.handle(&shell_req(
                    "POST",
                    "/api/auth/logout",
                    front_key,
                    origin,
                    Some(&format!("flp_session={tok}")),
                    json!({}),
                ));
                assert_eq!(r_lo.status, 200);
                admin_tokens.push(tok);
            }
            // Out-of-order logout of early_s at t0+69 (69th logout)
            runtime.now_secs.store(base_t + 69, Ordering::SeqCst);
            let r_early = shell.handle(&shell_req(
                "POST",
                "/api/auth/logout",
                front_key,
                origin,
                Some(&format!("flp_session={early_s}")),
                json!({}),
            ));
            assert_eq!(r_early.status, 200);
            admin_tokens.push(early_s);

            let st_after = store.load().unwrap().state;
            let ring_len = st_after
                .user_revoked_sessions
                .get("admin-1")
                .map(|v| v.len())
                .unwrap_or(0);
            let floor_val = st_after
                .user_min_session_exp
                .get("admin-1")
                .copied()
                .unwrap_or(0);

            // Verify all 69 logged-out tokens return 401 + x-flp-auth-fail: 1
            runtime.now_secs.store(base_t + 70, Ordering::SeqCst);
            let mut all_401 = true;
            let mut all_auth_fail = true;
            for tok in &admin_tokens {
                let r = shell.handle(&shell_req(
                    "GET",
                    "/api/admin/users",
                    front_key,
                    origin,
                    Some(&format!("flp_session={tok}")),
                    json!({}),
                ));
                if r.status != 401 {
                    all_401 = false;
                }
                if r.headers.get("x-flp-auth-fail").map(String::as_str) != Some("1") {
                    all_auth_fail = false;
                }
            }

            // Fresh session at base_t + 70 succeeds
            let fresh_tok = seal_test_session(
                "admin-1",
                "admin",
                0,
                base_t + 70,
                base_t + 70 + auth::SESSION_TTL,
                "jti-fresh-70",
            );
            let r_fresh_before = shell.handle(&shell_req(
                "GET",
                "/api/admin/users",
                front_key,
                origin,
                Some(&format!("flp_session={fresh_tok}")),
                json!({}),
            ));

            // Flood 100 analyst-1 logouts + 50 forged/replayed logouts
            for idx in 1..=100u64 {
                let t = base_t + 71 + idx;
                let atok = seal_test_session(
                    "analyst-1",
                    "analyst",
                    0,
                    t,
                    t + auth::SESSION_TTL,
                    &format!("jti-an-{idx}"),
                );
                let _ = shell.handle(&shell_req(
                    "POST",
                    "/api/auth/logout",
                    front_key,
                    origin,
                    Some(&format!("flp_session={atok}")),
                    json!({}),
                ));
            }
            for idx in 1..=50u64 {
                let _ = shell.handle(&shell_req(
                    "POST",
                    "/api/auth/logout",
                    front_key,
                    origin,
                    Some(&format!("flp_session=forged-garbage-{idx}")),
                    json!({}),
                ));
            }

            let r_fresh_after = shell.handle(&shell_req(
                "GET",
                "/api/admin/users",
                front_key,
                origin,
                Some(&format!("flp_session={fresh_tok}")),
                json!({}),
            ));
            let st_final = store.load().unwrap().state;
            let ring_len_after_flood = st_final
                .user_revoked_sessions
                .get("admin-1")
                .map(|v| v.len())
                .unwrap_or(0);
            let floor_after_flood = st_final
                .user_min_session_exp
                .get("admin-1")
                .copied()
                .unwrap_or(0);

            println!(
                "{}",
                serde_json::to_string(&json!({
                    "total_logged_out": admin_tokens.len(),
                    "ring_len": ring_len,
                    "expected_floor": base_t + 5 + auth::SESSION_TTL,
                    "actual_floor": floor_val,
                    "all_69_status_401": all_401,
                    "all_69_auth_fail_header": all_auth_fail,
                    "fresh_before_flood": r_fresh_before.status,
                    "fresh_after_flood": r_fresh_after.status,
                    "ring_len_after_flood": ring_len_after_flood,
                    "floor_after_flood": floor_after_flood,
                }))
                .unwrap()
            );
        }
        "passkey_epoch_and_policy_check" => {
            let origin = "https://logs.example.test";
            let setup_code = "test-sentinel-setup-code-0001";
            let front_val = "f".repeat(28);
            let front_key = front_val.as_str();
            let cdn_key = "test-sentinel-cdn-key-0001";
            let runtime = EpochTestRuntime::new(origin, setup_code, front_key, cdn_key);
            let store = MemoryStore::default();
            let now = runtime.now();

            // Provision fixtureService so /api/log-extents issues flp_cdn + __Host-flp_cdn
            let (p_st, p_body) = runtime.admin_call(
                "POST",
                "/api/admin/provision/execute",
                json!({
                    "token": "fixture-token",
                    "service_id": "fixtureService",
                    "fos_bucket": "fos-fixture-logs",
                    "fos_region": "us-east-1",
                    "log_fields": {
                        "groups": ["A", "C"],
                        "custom_fields": [],
                        "field_overrides": {},
                        "field_limits": {}
                    }
                }),
            );
            assert_eq!(p_st, 200);
            assert_eq!(runtime.run_admin_job(p_body["job"].clone())["status"], "complete");
            // Alias provisioned service under 22-char Fastly service ID AAAAAAAAAAAAAAAAAAAAAA for flp_cdn cookie signing
            let sid22 = "AAAAAAAAAAAAAAAAAAAAAA";
            let mut svc22 = runtime.admin_get("svc:fixtureService").unwrap().unwrap().value;
            svc22["service_id"] = json!(sid22);
            let svc22_typed: Service = serde_json::from_value(svc22.clone()).unwrap();
            let svc22_scope = ingest::scope(&svc22_typed);
            assert!(runtime.admin_cas(&format!("svc:{sid22}"), None, &svc22).unwrap());
            let inst_rec = runtime.admin_get("admin:install").unwrap().unwrap();
            let inst_22: Value = serde_json::from_str(
                &inst_rec.value.to_string().replace("fixtureService", sid22),
            )
            .unwrap();
            assert!(runtime
                .admin_cas("admin:install", Some(&inst_rec.generation), &inst_22)
                .unwrap());

            let mut initial = AuthState {
                setup: SetupState {
                    hash_generation: auth::hash(setup_code),
                    failures: 0,
                    spent: true,
                },
                auth_epoch: 1,
                ..Default::default()
            };
            let cred1 = Credential {
                id: "cred-pk-1".into(),
                public_key: vec![1u8; 32],
                sign_count: 1,
                revoked: false,
            };
            let cred2 = Credential {
                id: "cred-pk-2".into(),
                public_key: vec![2u8; 32],
                sign_count: 1,
                revoked: false,
            };
            initial.users.insert(
                "admin-1".into(),
                User {
                    id: "admin-1".into(),
                    display_name: "Admin".into(),
                    role: Role::Admin,
                    credentials: vec![cred1.clone()],
                    revoked: false,
                    created_by: "setup".into(),
                    created_at: now,
                    last_login_at: Some(now),
                },
            );
            initial.users.insert(
                "analyst-1".into(),
                User {
                    id: "analyst-1".into(),
                    display_name: "Analyst".into(),
                    role: Role::Analyst,
                    credentials: vec![cred1, cred2],
                    revoked: false,
                    created_by: "admin-1".into(),
                    created_at: now,
                    last_login_at: Some(now),
                },
            );
            assert!(store.compare_and_swap(None, &initial).unwrap());

            let admin_sess = seal_test_session("admin-1", "admin", 0, now, now + 3600, "jti-pk-adm");
            let analyst_sess = seal_test_session("analyst-1", "analyst", 0, now, now + 3600, "jti-pk-an");
            let shell = Shell::new(&runtime, &store, &[]);

            // Issue epoch=1 flp_cdn for analyst-1
            let stale_cdn_hdr = auth::issue_cdn_cookie(
                sid22,
                &svc22_scope,
                Role::Analyst,
                &analyst_sess,
                1,
                now,
                cdn_key,
            )
            .expect("epoch=1 cookie");
            let stale_cdn_val = stale_cdn_hdr
                .strip_prefix("flp_cdn=")
                .unwrap()
                .split(';')
                .next()
                .unwrap()
                .to_owned();
            let short_sid_none = auth::issue_cdn_cookie(
                "fixtureService",
                &svc22_scope,
                Role::Analyst,
                &analyst_sess,
                1,
                now,
                cdn_key,
            )
            .is_none();
            let v1_epoch0_hdr = auth::issue_cdn_cookie(
                sid22,
                &svc22_scope,
                Role::Analyst,
                &analyst_sess,
                0,
                now,
                cdn_key,
            )
            .expect("v1 epoch=0 fallback");

            // Queue HTTP 429 then 200 on PUT .../item/auth_epoch during passkey revoke (BUG-CLOUD-RUST-03 + GAP-C06)
            *runtime.api_status_queue.borrow_mut() = VecDeque::from([429, 200]);
            let r_pk_rev = shell.handle(&shell_req(
                "POST",
                "/api/admin/users/analyst-1/passkeys/cred-pk-1/revoke",
                front_key,
                origin,
                Some(&format!("flp_session={admin_sess}")),
                json!({}),
            ));
            let dict_put_attempts_429_200 = runtime.dict_put_calls.borrow().len();
            let dict_epoch_after_pk_rev = runtime
                .admin
                .dictionary_items("fixtureAuth")
                .get("auth_epoch")
                .cloned()
                .unwrap_or_default();
            let kv_epoch_after_pk_rev = store.load().unwrap().state.auth_epoch;

            // Calling GET /api/log-extents with valid flp_session + stale epoch=1 flp_cdn re-issues BOTH flp_cdn and __Host-flp_cdn at epoch=2
            let r_reissue = shell.handle(&shell_req(
                "GET",
                "/api/log-extents",
                front_key,
                origin,
                Some(&format!("flp_session={analyst_sess}; flp_cdn={stale_cdn_val}")),
                json!({}),
            ));
            let reissued_cookies = r_reissue.cookies().to_vec();

            // Calling GET /api/log-extents with tossed garbage flp_cdn + valid epoch=2 __Host-flp_cdn does NOT reissue
            let valid_e2_hdr = auth::issue_cdn_cookie(
                sid22,
                &svc22_scope,
                Role::Analyst,
                &analyst_sess,
                2,
                now,
                cdn_key,
            )
            .unwrap();
            let valid_e2_val = valid_e2_hdr
                .strip_prefix("flp_cdn=")
                .unwrap()
                .split(';')
                .next()
                .unwrap()
                .to_owned();
            let r_host_prec = shell.handle(&shell_req(
                "GET",
                "/api/log-extents",
                front_key,
                origin,
                Some(&format!(
                    "flp_session={analyst_sess}; flp_cdn=tossed_garbage_invalid; __Host-flp_cdn={valid_e2_val}"
                )),
                json!({}),
            ));
            let host_prec_reissued_count = r_host_prec.cookies().len();

            // Exhaustion test: 429 -> 503 emits auth.epoch_sync_failed event without failing HTTP 200
            *runtime.api_status_queue.borrow_mut() = VecDeque::from([429, 503]);
            let r_role = shell.handle(&shell_req(
                "PATCH",
                "/api/admin/users/analyst-1",
                front_key,
                origin,
                Some(&format!("flp_session={admin_sess}")),
                json!({"role": "analyst-no-pii"}),
            ));
            let admin_events = runtime.admin_events.borrow().clone();

            // Bump policy_version in AuthState -> existing flp_session immediately invalidated (401)
            let loaded = store.load().unwrap();
            let mut bumped = loaded.state.clone();
            bumped.policy_version += 1;
            assert!(store.compare_and_swap(loaded.generation.as_deref(), &bumped).unwrap());
            let r_after_policy = shell.handle(&shell_req(
                "GET",
                "/api/admin/users",
                front_key,
                origin,
                Some(&format!("flp_session={admin_sess}")),
                json!({}),
            ));

            println!(
                "{}",
                serde_json::to_string(&json!({
                    "short_sid_none": short_sid_none,
                    "v1_epoch0_hdr": v1_epoch0_hdr,
                    "pk_revoke_status": r_pk_rev.status,
                    "dict_put_attempts_429_200": dict_put_attempts_429_200,
                    "kv_epoch_after_pk_rev": kv_epoch_after_pk_rev,
                    "dict_epoch_after_pk_rev": dict_epoch_after_pk_rev,
                    "stale_cdn_val": stale_cdn_val,
                    "valid_e2_val": valid_e2_val,
                    "reissue_status": r_reissue.status,
                    "reissued_cookies": reissued_cookies,
                    "host_prec_status": r_host_prec.status,
                    "host_prec_reissued_count": host_prec_reissued_count,
                    "exhaustion_role_status": r_role.status,
                    "admin_events": admin_events,
                    "after_policy_bump_status": r_after_policy.status,
                }))
                .unwrap()
            );
        }
        "fsm1_and_bundle_oom_check" => {
            ALLOC_CAP.with(|cap| cap.set(64 * 1024 * 1024));
            let _guard = AllocCapGuard;

            // 1. Scorer FSM1 crafted header checks (BUG-CLOUD-RUST-01)
            let mut fsm1_u32_max = b"FSM1\x01".to_vec();
            fsm1_u32_max.extend_from_slice(&1u32.to_le_bytes()); // vocab_size
            fsm1_u32_max.extend_from_slice(&u32::MAX.to_le_bytes()); // n_routes
            fsm1_u32_max.extend_from_slice(&0u16.to_le_bytes()); // ver_len

            let mut fsm1_2000 = b"FSM1\x01".to_vec();
            fsm1_2000.extend_from_slice(&1u32.to_le_bytes());
            fsm1_2000.extend_from_slice(&0x2000_0000u32.to_le_bytes());
            fsm1_2000.extend_from_slice(&0u16.to_le_bytes());

            // n_routes = 1, str_off = [0, 0], row_total = [1], row_off = [0, u32::MAX] (uvarint)
            let mut fsm1_pairs_max = b"FSM1\x01".to_vec();
            fsm1_pairs_max.extend_from_slice(&1u32.to_le_bytes()); // vocab_size
            fsm1_pairs_max.extend_from_slice(&1u32.to_le_bytes()); // n_routes = 1
            fsm1_pairs_max.extend_from_slice(&0u16.to_le_bytes()); // ver_len = 0
            fsm1_pairs_max.extend_from_slice(&0u32.to_le_bytes()); // str_off[0] = 0
            fsm1_pairs_max.extend_from_slice(&0u32.to_le_bytes()); // str_off[1] = 0
            fsm1_pairs_max.push(1); // row_total[0] = 1
            fsm1_pairs_max.push(0); // row_off[0] = 0
            fsm1_pairs_max.extend_from_slice(&[0xff, 0xff, 0xff, 0xff, 0x0f]); // row_off[1] = u32::MAX

            // Valid 1-route FSM1 buffer
            let mut fsm1_valid = b"FSM1\x01".to_vec();
            fsm1_valid.extend_from_slice(&1u32.to_le_bytes()); // vocab_size = 1
            fsm1_valid.extend_from_slice(&1u32.to_le_bytes()); // n_routes = 1
            fsm1_valid.extend_from_slice(&2u16.to_le_bytes()); // ver_len = 2
            fsm1_valid.extend_from_slice(b"v1");
            fsm1_valid.extend_from_slice(&0u32.to_le_bytes()); // str_off[0] = 0
            fsm1_valid.extend_from_slice(&1u32.to_le_bytes()); // str_off[1] = 1
            fsm1_valid.extend_from_slice(b"/"); // str_blob
            fsm1_valid.push(3); // row_total[0] = 3
            fsm1_valid.push(0); // row_off[0] = 0
            fsm1_valid.push(1); // row_off[1] = 1
            fsm1_valid.push(0); // col_id = 0
            fsm1_valid.push(3); // count = 3

            let fsm1_results = json!({
                "u32_max_some": scorer_matrix::check_parse_fsm1(&fsm1_u32_max),
                "n_0x20000000_some": scorer_matrix::check_parse_fsm1(&fsm1_2000),
                "pairs_u32_max_some": scorer_matrix::check_parse_fsm1(&fsm1_pairs_max),
                "valid_some": scorer_matrix::check_parse_fsm1(&fsm1_valid),
            });

            // 2. Cloud summary_bundle crafted partial= decode check (BUG-CLOUD-RUST-02)
            let day_start: i64 = 1_799_971_200; // 2027-01-15T00:00:00Z (exact UTC day multiple of 86400)
            let mini = MiniBundleRuntime::new((day_start + 2 * 86_400) as u64);
            mini.kv.borrow_mut().insert(
                ingest::WATERMARK_KEY.into(),
                (
                    1,
                    json!({"key": "raw/request/year=2027/month=01/day=17/hour=00/2027-01-17T00:00:00.000-zzz.log.gz"}),
                ),
            );
            // Build 2 real hour_bundle Parquet files for 2027-01-15 hours 00 and 01
            let mut hour_names = Vec::new();
            let mut hours_map = serde_json::Map::new();
            for h in 0..2i64 {
                let h_start = day_start + h * 3600;
                let line_json = json!({
                    "timestamp": if h == 0 { "2027-01-15T00:05:00Z" } else { "2027-01-15T01:05:00Z" },
                    "status": 200,
                    "cache": "HIT",
                    "resp_bytes": 512,
                    "elapsed": 1200,
                    "url": "/ok",
                    "ip": "198.51.100.10",
                    "pop": "IAD",
                    "country": "US",
                    "asn": 15169
                })
                .to_string()
                    + "\n";
                let zst_log = zstd::stream::encode_all(line_json.as_bytes(), 1).unwrap();
                let conv = convert::convert_object(&zst_log, 300, h_start).unwrap();
                let batch = conv.windows.into_values().next().unwrap();
                let win_pq = summary::write_summary(&[&batch], h_start).unwrap();
                let hb_bytes = bundle::merge(&[win_pq.as_slice()], h_start, bundle::Unit::Hour).unwrap();
                let hb_key = bundle::hour_bundle_key(
                    "processed/summary_bundle/request/year=2027/month=01/day=15/",
                    h_start,
                    &[format!("w{h}.parquet")],
                );
                hour_names.push(hb_key.rsplit('/').next().unwrap().to_owned());
                hours_map.insert(h_start.to_string(), json!(hb_key.clone()));
                mini.bucket.borrow_mut().insert(hb_key, hb_bytes);
            }
            hour_names.sort();
            let fp1 = bundle::fingerprint([hour_names[0].as_str()]);

            // Craft raw FLPB\x09 partial with ses_len = u32::MAX (29 bytes) AND packed FLPB\x04 partial with cells = u32::MAX
            let mut raw_ses_max = b"FLPB\x09".to_vec();
            raw_ses_max.extend_from_slice(&1u64.to_le_bytes()); // sources
            raw_ses_max.extend_from_slice(&0u32.to_le_bytes()); // ov_len
            raw_ses_max.extend_from_slice(&0u32.to_le_bytes()); // min_len
            raw_ses_max.extend_from_slice(&0u32.to_le_bytes()); // fld_len
            raw_ses_max.extend_from_slice(&u32::MAX.to_le_bytes()); // ses_len = u32::MAX

            let mut raw_cells_max = b"FLPB\x09".to_vec();
            raw_cells_max.extend_from_slice(&1u64.to_le_bytes());
            raw_cells_max.extend_from_slice(&1u32.to_le_bytes()); // ov_len = 1
            raw_cells_max.extend_from_slice(&0i64.to_le_bytes()); // hour = 0
            raw_cells_max.extend_from_slice(&u32::MAX.to_le_bytes()); // cells = u32::MAX
            let zst_cells = zstd::bulk::compress(&raw_cells_max, 1).unwrap();
            let mut packed_cells_max = b"FLPB\x04".to_vec();
            packed_cells_max.extend_from_slice(&1u32.to_le_bytes()); // parts = 1
            packed_cells_max.extend_from_slice(&(raw_cells_max.len() as u64).to_le_bytes());
            packed_cells_max.extend_from_slice(&zst_cells);

            let corrupt_key_1 = format!(
                "processed/summary_bundle/request/year=2027/month=01/day=15/partial=c1-n1-h{fp1:016x}.parquet"
            );
            mini.bucket.borrow_mut().insert(corrupt_key_1.clone(), raw_ses_max);

            let svc: Service = serde_json::from_value(json!({
                "service_id": "fixtureService",
                "name": "Fixture",
                "status": "active",
                "created_at": 0,
                "updated_at": 0,
                "config_version": 1,
                "logging_enabled": true,
                "logging_endpoint_name": "ep",
                "log_period": 60,
                "sample_rate": 1,
                "edge_only": false,
                "custom_condition": "",
                "fos_bucket": "fos-fixture-logs",
                "fos_prefix": "",
                "fos_region": "us-east-1",
                "fos_endpoint": "",
                "log_fields": [],
                "field_limits": {},
                "fastly_token_ref": "",
                "fos_access_key_ref": "ak",
                "fos_secret_key_ref": "sk",
                "owner": "",
                "bucket_adopted": true,
                "bucket_checked": true,
                "last_activated_version": null,
                "last_reconcile": null,
                "pending_job_id": null
            }))
            .unwrap();

            let mut bundle_errors = Vec::new();
            mini.kv.borrow_mut().insert(
                bundle::STATE_KEY.into(),
                (
                    1,
                    json!({
                        "next_hour": day_start + 86_400,
                        "hours": hours_map.clone(),
                        "days": {},
                        "dirty": [day_start],
                        "revisit": null,
                        "revisited_at": day_start + 2 * 86_400,
                        "retire": {}
                    }),
                ),
            );
            for _ in 0..8 {
                mini.calls.set(0);
                let store_ref = Store {
                    runtime: &mini,
                    service: &svc,
                    access_key: "ak".into(),
                    secret_key: "sk".into(),
                    started: Instant::now(),
                    vcpu_start: 0,
                };
                let rep = bundle::summary_bundle(&store_ref, "ck", 1).unwrap();
                bundle_errors.extend(rep.errors);
                if !rep.more_pending {
                    break;
                }
            }
            // Now test the packed FLPB\x04 variant too
            mini.kv.borrow_mut().clear();
            mini.kv.borrow_mut().insert(
                ingest::WATERMARK_KEY.into(),
                (
                    1,
                    json!({"key": "raw/request/year=2027/month=01/day=17/hour=00/2027-01-17T00:00:00.000-zzz.log.gz"}),
                ),
            );
            mini.kv.borrow_mut().insert(
                bundle::STATE_KEY.into(),
                (
                    1,
                    json!({
                        "next_hour": day_start + 86_400,
                        "hours": hours_map.clone(),
                        "days": {},
                        "dirty": [day_start],
                        "revisit": null,
                        "revisited_at": day_start + 2 * 86_400,
                        "retire": {}
                    }),
                ),
            );
            mini.bucket.borrow_mut().retain(|k, _| !k.contains("day_bundle="));
            mini.bucket.borrow_mut().insert(corrupt_key_1.clone(), packed_cells_max);
            let mut packed_errors = Vec::new();
            for _ in 0..8 {
                mini.calls.set(0);
                let store_ref = Store {
                    runtime: &mini,
                    service: &svc,
                    access_key: "ak".into(),
                    secret_key: "sk".into(),
                    started: Instant::now(),
                    vcpu_start: 0,
                };
                let rep = bundle::summary_bundle(&store_ref, "ck", 1).unwrap();
                packed_errors.extend(rep.errors);
                if !rep.more_pending {
                    break;
                }
            }
            let day_bundle_written = mini.bucket.borrow().keys().any(|k| k.contains("day_bundle="));

            // 3. Worker x-flp-worker-key gates + query-bearing /api/file & /api/files RBAC/traversal/cursor + telemetry::redact
            let origin = "https://logs.example.test";
            let setup_code = "test-sentinel-setup-code-0001";
            let front_val = "f".repeat(28);
            let front_key = front_val.as_str();
            let cdn_key = "test-sentinel-cdn-key-0001";
            let runtime = EpochTestRuntime::new(origin, setup_code, front_key, cdn_key);
            let (p_st, p_body) = runtime.admin_call(
                "POST",
                "/api/admin/provision/execute",
                json!({
                    "token": "fixture-token",
                    "service_id": "fixtureService",
                    "fos_bucket": "fos-fixture-logs",
                    "fos_region": "us-east-1",
                    "log_fields": {"groups": ["A", "C"], "custom_fields": [], "field_overrides": {}, "field_limits": {}}
                }),
            );
            assert_eq!(p_st, 200);
            assert_eq!(runtime.run_admin_job(p_body["job"].clone())["status"], "complete");

            let w_missing = worker::handle_worker_ingest(
                &runtime,
                &Request {
                    method: "POST".into(),
                    path: "/_internal/ingest/run".into(),
                    headers: BTreeMap::new(),
                    body: b"{}".to_vec(),
                },
            )
            .status;
            let w_wrong = worker::handle_worker_ingest(
                &runtime,
                &Request {
                    method: "POST".into(),
                    path: "/_internal/ingest/run".into(),
                    headers: BTreeMap::from([("x-flp-worker-key".into(), "wrong-secret".into())]),
                    body: b"{}".to_vec(),
                },
            )
            .status;
            let w_valid_past_auth = worker::handle_worker_ingest(
                &runtime,
                &Request {
                    method: "POST".into(),
                    path: "/_internal/ingest/run".into(),
                    headers: BTreeMap::from([(
                        "x-flp-worker-key".into(),
                        "test-sentinel-worker-key-0001".into(),
                    )]),
                    body: b"{}".to_vec(),
                },
            )
            .status;
            let stage_wrong = stage::handle_worker_stage(
                &runtime,
                &Request {
                    method: "POST".into(),
                    path: "/_internal/admin/step".into(),
                    headers: BTreeMap::from([("x-flp-worker-key".into(), "wrong-secret".into())]),
                    body: b"{}".to_vec(),
                },
            )
            .status;
            let relay_missing = worker::handle_worker_relay(
                &runtime,
                &Request {
                    method: "POST".into(),
                    path: "/_internal/ingest/relay".into(),
                    headers: BTreeMap::new(),
                    body: b"{}".to_vec(),
                },
            )
            .status;

            // Query-bearing /api/file & /api/files RBAC/traversal/cursor via Shell::handle (GAP-CLOUD-B01)
            let auth_store = MemoryStore::default();
            let now = runtime.now();
            let mut st = AuthState {
                setup: SetupState {
                    hash_generation: auth::hash(setup_code),
                    failures: 0,
                    spent: true,
                },
                ..Default::default()
            };
            let cred = Credential {
                id: "c1".into(),
                public_key: vec![9u8; 32],
                sign_count: 1,
                revoked: false,
            };
            for (uid, role) in [
                ("adm", Role::Admin),
                ("an", Role::Analyst),
                ("np", Role::AnalystNoPii),
            ] {
                st.users.insert(
                    uid.into(),
                    User {
                        id: uid.into(),
                        display_name: uid.into(),
                        role,
                        credentials: vec![cred.clone()],
                        revoked: false,
                        created_by: "setup".into(),
                        created_at: now,
                        last_login_at: Some(now),
                    },
                );
            }
            assert!(auth_store.compare_and_swap(None, &st).unwrap());
            let c_adm = format!(
                "flp_session={}",
                seal_test_session("adm", "admin", 0, now, now + 3600, "j-adm")
            );
            let c_an = format!(
                "flp_session={}",
                seal_test_session("an", "analyst", 0, now, now + 3600, "j-an")
            );
            let c_np = format!(
                "flp_session={}",
                seal_test_session("np", "analyst-no-pii", 0, now, now + 3600, "j-np")
            );
            let shell = Shell::new(&runtime, &auth_store, &[]);
            let svc_val = runtime.admin_get("svc:fixtureService").unwrap().unwrap().value;
            let prefix = svc_val["fos_prefix"].as_str().unwrap_or("");
            let valid_req_key = format!("{prefix}processed/request/year=2027/month=01/day=15/a.parquet");
            let valid_ops_key = format!("{prefix}processed/ops/api_telemetry/year=2027/a.parquet");

            let np_file_get = shell
                .handle(&shell_req(
                    "GET",
                    &format!("/api/file?key={valid_req_key}"),
                    front_key,
                    origin,
                    Some(&c_np),
                    json!({}),
                ))
                .status;
            let np_file_head = shell
                .handle(&shell_req(
                    "HEAD",
                    &format!("/api/file?key={valid_req_key}"),
                    front_key,
                    origin,
                    Some(&c_np),
                    json!({}),
                ))
                .status;
            let an_ops_files = shell
                .handle(&shell_req(
                    "GET",
                    "/api/files?dataset=ops_api",
                    front_key,
                    origin,
                    Some(&c_an),
                    json!({}),
                ))
                .status;
            let an_ops_file = shell
                .handle(&shell_req(
                    "GET",
                    &format!("/api/file?key={valid_ops_key}"),
                    front_key,
                    origin,
                    Some(&c_an),
                    json!({}),
                ))
                .status;
            let mut adm_bad_keys = BTreeMap::new();
            for (lbl, bad_k) in [
                ("dotdot", format!("{prefix}processed/../raw/request/a.parquet")),
                ("pct_dotdot", format!("{prefix}processed/%2e%2e/raw/a.parquet")),
                ("double_slash", format!("{prefix}processed//request/a.parquet")),
                ("raw_zst", format!("{prefix}raw/request/a.zst")),
                ("non_parquet", format!("{prefix}processed/request/a.json")),
                ("wrong_prefix", "other-tenant/processed/request/a.parquet".to_owned()),
            ] {
                let s = shell
                    .handle(&shell_req(
                        "GET",
                        &format!("/api/file?key={bad_k}"),
                        front_key,
                        origin,
                        Some(&c_adm),
                        json!({}),
                    ))
                    .status;
                adm_bad_keys.insert(lbl.to_owned(), s);
            }
            let adm_bad_cursor = shell
                .handle(&shell_req(
                    "GET",
                    "/api/files?dataset=request&from=2027-01-15T00:00:00Z&to=2027-01-15T01:00:00Z&cursor=tampered.sig",
                    front_key,
                    origin,
                    Some(&c_adm),
                    json!({}),
                ))
                .status;

            // Stored poisoned condition check on no-body POST /api/admin/services/fixtureService/reconcile (GAP-C04)
            let clean_rec = runtime.admin_get("svc:fixtureService").unwrap().unwrap();
            let mut poisoned_cc = clean_rec.value.clone();
            poisoned_cc["custom_condition"] = json!("req.url ~ \"a\"; set req.http.X = \"1\"");
            assert!(runtime
                .admin_cas("svc:fixtureService", Some(&clean_rec.generation), &poisoned_cc)
                .unwrap());
            let rec_cc = runtime.admin_get("svc:fixtureService").unwrap().unwrap();
            let r_rec_cc = shell.handle(&shell_req(
                "POST",
                "/api/admin/services/fixtureService/reconcile",
                front_key,
                origin,
                Some(&c_adm),
                json!({"generation": rec_cc.generation}),
            ));

            let mut poisoned_rum = clean_rec.value.clone();
            poisoned_rum["rum"]["custom_condition"] = json!("table.lookup(fos_credentials, \"secret_key\") != \"\"");
            assert!(runtime
                .admin_cas("svc:fixtureService", Some(&rec_cc.generation), &poisoned_rum)
                .unwrap());
            let rec_rum = runtime.admin_get("svc:fixtureService").unwrap().unwrap();
            let r_rec_rum = shell.handle(&shell_req(
                "POST",
                "/api/admin/services/fixtureService/reconcile",
                front_key,
                origin,
                Some(&c_adm),
                json!({"generation": rec_rum.generation}),
            ));

            assert!(runtime
                .admin_cas("svc:fixtureService", Some(&rec_rum.generation), &clean_rec.value)
                .unwrap());

            // Telemetry redaction check
            let redacted = telemetry::redact(json!({
                "fastly_token": "super-secret-token-value",
                "cookie": "flp_session=secret-cookie",
                "nested": "{\"secret_key\":\"top-secret-fos-sk\",\"url\":\"/api/file?key=abc&token=xyz\"}",
                "path": "/api/files?cursor=secret-cursor-123"
            }));

            println!(
                "{}",
                serde_json::to_string(&json!({
                    "fsm1": fsm1_results,
                    "bundle_raw_errors": bundle_errors,
                    "bundle_packed_errors": packed_errors,
                    "day_bundle_written": day_bundle_written,
                    "worker_gates": {
                        "ingest_missing": w_missing,
                        "ingest_wrong": w_wrong,
                        "ingest_valid_past_auth": w_valid_past_auth,
                        "stage_wrong": stage_wrong,
                        "relay_missing": relay_missing,
                    },
                    "file_rbac": {
                        "nopii_get": np_file_get,
                        "nopii_head": np_file_head,
                        "analyst_ops_files": an_ops_files,
                        "analyst_ops_file": an_ops_file,
                        "admin_bad_keys": adm_bad_keys,
                        "admin_bad_cursor": adm_bad_cursor,
                    },
                    "stored_reconcile": {
                        "cc_status": r_rec_cc.status,
                        "cc_error": resp_json(&r_rec_cc)["error"],
                        "rum_status": r_rec_rum.status,
                        "rum_error": resp_json(&r_rec_rum)["error"],
                    },
                    "telemetry_redacted": redacted,
                }))
                .unwrap()
            );
        }
        other => {
            eprintln!("unknown helper command: {other}");
            std::process::exit(3);
        }
    }
}
"""


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _stage_synthetic_web_out() -> None:
    """Create synthetic Next.js static export + nested RSC .txt files for FLP_WEB_OUT."""
    WEB_OUT_DIR.mkdir(parents=True, exist_ok=True)
    html_pages = {
        "index.html": "<!doctype html><html><body>FLP Cloud Root</body></html>",
        "dashboard.html": "<!doctype html><html><body>FLP Dashboard</body></html>",
        "sign-in.html": "<!doctype html><html><body>Sign In</body></html>",
        "share-login.html": "<!doctype html><html><body>Share Login</body></html>",
        "setup.html": "<!doctype html><html><body>Admin Setup</body></html>",
        "admin.html": "<!doctype html><html><body>Admin Console</body></html>",
        "logs.html": "<!doctype html><html><body>Operational Logs</body></html>",
        "404.html": "<!doctype html><html><body>Not Found</body></html>",
    }
    rsc_files = {
        "admin/__next.admin.txt": '0:["$","div",null,{"children":"ADMIN_RSC_SECRET_PAYLOAD"}]\n',
        "admin/share/__next.admin.share.txt": '0:["$","div",null,{"children":"ADMIN_SHARE_RSC_PAYLOAD"}]\n',
        "admin/users/__next.page.txt": '0:["$","div",null,{"children":"ADMIN_USERS_RSC_PAYLOAD"}]\n',
        "logs/__next.logs.txt": '0:["$","div",null,{"children":"LOGS_RSC_SECRET_PAYLOAD"}]\n',
        "dashboard/__next.dashboard.txt": '0:["$","div",null,{"children":"DASHBOARD_RSC_PAYLOAD"}]\n',
        "_next/static/chunks/app.js": "console.log('synthetic-flp-chunk');\n",
    }
    for rel, content in {**html_pages, **rsc_files}.items():
        target = WEB_OUT_DIR / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists() or target.read_text(encoding="utf-8") != content:
            target.write_text(content, encoding="utf-8")


def _compute_source_sig(web_out_dir: Path) -> str:
    """Fast mtime+size digest across compute/ sources & staged web_out so warm xdist workers skip cargo rebuilds."""
    h = hashlib.sha256()
    skip_dirs = {"target", "pkg", ".git", "node_modules", "__pycache__"}
    for root_dir in (REPO_ROOT / "compute", web_out_dir):
        if not root_dir.exists():
            continue
        for cur_root, dirs, files in os.walk(root_dir):
            dirs[:] = sorted(d for d in dirs if d not in skip_dirs)
            for fname in sorted(files):
                fpath = Path(cur_root) / fname
                try:
                    st = fpath.stat()
                except OSError:
                    continue
                h.update(f"{fpath}:{st.st_mtime_ns}:{st.st_size}\n".encode())
    return h.hexdigest()


def ensure_cloud_binaries() -> tuple[Path, Path]:
    """Build shell-test-server and e2e-cloud-vcl-helper under an exclusive file lock."""
    global _BUILT_PATHS
    driver_dir = DRIVER_DIR
    lock_file = LOCK_FILE
    cargo_target_dir = CARGO_TARGET_DIR
    web_out_dir = WEB_OUT_DIR
    helper_src = driver_dir / "main.rs"
    helper_bin = driver_dir / "e2e-cloud-vcl-helper"
    stamp_file = driver_dir / STAMP_FILE_NAME
    if (
        _BUILT_PATHS is not None
        and _BUILT_PATHS[1] == helper_bin
        and all(p.exists() for p in _BUILT_PATHS)
        and stamp_file.exists()
        and helper_src.exists()
        and helper_src.read_text(encoding="utf-8") == _HELPER_RS
    ):
        return _BUILT_PATHS

    lock_file.parent.mkdir(parents=True, exist_ok=True)
    with lock_file.open("w", encoding="utf-8") as lock_fh:
        fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
        _stage_synthetic_web_out()
        cargo_target_dir.mkdir(parents=True, exist_ok=True)
        driver_dir.mkdir(parents=True, exist_ok=True)

        env = os.environ.copy()
        env["CARGO_TARGET_DIR"] = str(cargo_target_dir)
        env["FLP_WEB_OUT"] = str(web_out_dir)
        env["FLP_SCORER_PACKAGE"] = "/nonexistent"
        env["FLP_SCORER_MATRIX_RS"] = str(SCORER_MATRIX_RS)

        wanted_libs = {"fastly_log_analytics", "serde_json", "compute_common", "fastly", "zstd", "url"}
        cargo_stamp_file = cargo_target_dir / "e2e-cloud-cargo.stamp.json"
        src_sig = _compute_source_sig(web_out_dir)

        rlibs: dict[str, str] = {}
        server_bin = cargo_target_dir / "debug" / "shell-test-server"
        cargo_cached = False
        if cargo_stamp_file.is_file():
            try:
                cached_meta = json.loads(cargo_stamp_file.read_text(encoding="utf-8"))
                cached_rlibs = cached_meta.get("rlibs") or {}
                cached_srv = Path(str(cached_meta.get("server_bin") or server_bin))
                if (
                    cached_meta.get("src_sig") == src_sig
                    and cached_srv.is_file()
                    and not (wanted_libs - set(cached_rlibs))
                    and all(Path(cached_rlibs[k]).is_file() for k in wanted_libs)
                ):
                    rlibs = {k: str(cached_rlibs[k]) for k in wanted_libs}
                    server_bin = cached_srv
                    cargo_cached = True
            except (OSError, ValueError, TypeError):
                cargo_cached = False

        if not cargo_cached:
            proc = subprocess.run(
                [
                    "cargo",
                    "build",
                    "--manifest-path",
                    str(CLOUD_MANIFEST),
                    "--lib",
                    "--bin",
                    "shell-test-server",
                    "--offline",
                    "--locked",
                    "--message-format=json",
                ],
                cwd=str(CLOUD_DIR),
                env=env,
                capture_output=True,
                text=True,
                timeout=300,
                check=False,
            )
            if proc.returncode != 0:
                raise RuntimeError(f"cargo build shell-test-server failed ({proc.returncode}):\n{proc.stderr}")

            for line in proc.stdout.splitlines():
                if not line.startswith("{"):
                    continue
                msg = json.loads(line)
                if msg.get("reason") != "compiler-artifact":
                    continue
                target = msg.get("target") or {}
                tname = str(target.get("name") or "").replace("-", "_")
                tkinds = target.get("kind") or []
                filenames = msg.get("filenames") or []
                if tname in wanted_libs and "lib" in tkinds:
                    for fn in filenames:
                        if fn.endswith(".rlib"):
                            rlibs[tname] = fn
                elif target.get("name") == "shell-test-server" and "bin" in tkinds and filenames:
                    server_bin = Path(filenames[0])

            missing = sorted(wanted_libs - set(rlibs))
            if missing:
                raise RuntimeError(f"Failed to locate rlib artifacts (missing={missing!r}, found={sorted(rlibs)!r})")

            tmp_cstamp = cargo_target_dir / f"e2e-cloud-cargo.stamp.{os.getpid()}.{secrets.token_hex(4)}.tmp"
            try:
                tmp_cstamp.write_text(
                    json.dumps({"src_sig": src_sig, "server_bin": str(server_bin), "rlibs": rlibs}),
                    encoding="utf-8",
                )
                os.replace(tmp_cstamp, cargo_stamp_file)
            finally:
                tmp_cstamp.unlink(missing_ok=True)

        helper_sha = hashlib.sha256(_HELPER_RS.encode("utf-8")).hexdigest()

        def _stat_sig(p: str | Path) -> str:
            st = os.stat(p)
            return f"{p}:{st.st_mtime_ns}:{st.st_size}"

        expected_stamp = (
            "|".join(
                [
                    *(_stat_sig(rlibs[k]) for k in sorted(wanted_libs)),
                    _stat_sig(SCORER_MATRIX_RS),
                    helper_sha,
                ]
            )
            + "\n"
        )

        scorer_shim = driver_dir / "scorer_matrix_inner.rs"
        scorer_shim_src = (
            SCORER_MATRIX_RS.read_text(encoding="utf-8")
            + "\npub fn check_parse_fsm1(buf: &[u8]) -> bool { parse_fsm1(buf).is_some() }\n"
        )
        if not scorer_shim.exists() or scorer_shim.read_text(encoding="utf-8") != scorer_shim_src:
            tmp_shim = driver_dir / f"scorer_matrix_inner.{os.getpid()}.{secrets.token_hex(4)}.rs.tmp"
            try:
                tmp_shim.write_text(scorer_shim_src, encoding="utf-8")
                os.replace(tmp_shim, scorer_shim)
            finally:
                tmp_shim.unlink(missing_ok=True)

        if not helper_src.exists() or helper_src.read_text(encoding="utf-8") != _HELPER_RS:
            tmp_src = driver_dir / f"main.{os.getpid()}.{secrets.token_hex(4)}.rs.tmp"
            try:
                tmp_src.write_text(_HELPER_RS, encoding="utf-8")
                os.replace(tmp_src, helper_src)
            finally:
                tmp_src.unlink(missing_ok=True)

        if helper_bin.exists() and stamp_file.exists() and stamp_file.read_text(encoding="utf-8") == expected_stamp:
            _BUILT_PATHS = (server_bin, helper_bin)
            return _BUILT_PATHS

        deps_dir = cargo_target_dir / "debug" / "deps"
        tmp_bin = driver_dir / f"e2e-cloud-vcl-helper.{os.getpid()}.{secrets.token_hex(4)}.tmp"
        tmp_stamp = driver_dir / f"{STAMP_FILE_NAME}.{os.getpid()}.{secrets.token_hex(4)}.tmp"
        try:
            rustc_cmd = [
                "rustc",
                "--edition",
                "2021",
                str(helper_src),
                "-L",
                f"dependency={deps_dir}",
            ]
            for crate_name in sorted(wanted_libs):
                rustc_cmd.extend(["--extern", f"{crate_name}={rlibs[crate_name]}"])
            rustc_cmd.extend(["-o", str(tmp_bin)])

            rustc_proc = subprocess.run(
                rustc_cmd,
                cwd=str(CLOUD_DIR),
                env=env,
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
            if rustc_proc.returncode != 0:
                raise RuntimeError(f"rustc helper compile failed:\n{rustc_proc.stderr}")
            os.replace(tmp_bin, helper_bin)
            tmp_stamp.write_text(expected_stamp, encoding="utf-8")
            os.replace(tmp_stamp, stamp_file)
        finally:
            tmp_bin.unlink(missing_ok=True)
            tmp_stamp.unlink(missing_ok=True)

        _BUILT_PATHS = (server_bin, helper_bin)
        return _BUILT_PATHS


def cloud_run_helper(cmd: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """Invoke an arbitrary JSON subcommand on e2e-cloud-vcl-helper."""
    _, helper_bin = ensure_cloud_binaries()
    proc = subprocess.run(
        [str(helper_bin), cmd, json.dumps(payload or {})],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or f"helper {cmd} exited {proc.returncode}")
    return json.loads(proc.stdout)


def cloud_desired_snippets(state: dict[str, Any]) -> list[dict[str, Any]]:
    """Invoke fastly_log_analytics::logging::desired_snippets via the compiled helper."""
    _, helper_bin = ensure_cloud_binaries()
    proc = subprocess.run(
        [str(helper_bin), "desired_snippets", json.dumps(state)],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    if proc.returncode != 0:
        raise ValueError(proc.stderr.strip() or f"desired_snippets exited {proc.returncode}")
    return json.loads(proc.stdout)


def cloud_rum_tracker_js(service_id: str, config: dict[str, Any] | None = None) -> str:
    """Invoke fastly_log_analytics::logging::rum::generate_rum_tracker_js via helper."""
    _, helper_bin = ensure_cloud_binaries()
    proc = subprocess.run(
        [str(helper_bin), "rum_tracker_js", json.dumps({"service_id": service_id, "config": config})],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or f"rum_tracker_js exited {proc.returncode}")
    return json.loads(proc.stdout)


def cloud_issue_cdn_cookie(
    *,
    session_cookie: str,
    role: str = "admin",
    service_id: str = FIXTURE_22_SID,
    scope: str = "logs",
    auth_epoch: int = 1,
    now: int | None = None,
    cdn_key: str = DEFAULT_CDN_KEY,
) -> dict[str, Any]:
    """Invoke fastly_log_analytics::auth::issue_cdn_cookie + CDN_COOKIE_TTL via helper."""
    _, helper_bin = ensure_cloud_binaries()
    proc = subprocess.run(
        [
            str(helper_bin),
            "issue_cdn_cookie",
            json.dumps(
                {
                    "service_id": service_id,
                    "scope": scope,
                    "role": role,
                    "session_cookie": session_cookie,
                    "auth_epoch": auth_epoch,
                    "now": int(time.time()) if now is None else now,
                    "cdn_key": cdn_key,
                }
            ),
        ],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or f"issue_cdn_cookie exited {proc.returncode}")
    return json.loads(proc.stdout)


def cloud_check_audit_entries_rbac() -> dict[str, Any]:
    """Invoke AuthService::audit_entries behaviorally across Analyst, AnalystNoPii, anonymous, and Admin."""
    _, helper_bin = ensure_cloud_binaries()
    proc = subprocess.run(
        [str(helper_bin), "audit_entries_check", "{}"],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or f"audit_entries_check exited {proc.returncode}")
    return json.loads(proc.stdout)


def cloud_check_revoke_epoch_bump() -> dict[str, Any]:
    """Invoke Shell::handle on PATCH /api/admin/users/:id and POST /api/admin/users/:id/revoke and return auth_epoch + dictionary sync."""
    _, helper_bin = ensure_cloud_binaries()
    challenge = _b64url(b"\x17" * 32)
    _, _, cred_payload = _build_ed25519_attestation(
        rp_id="logs.example.test",
        origin=DEFAULT_CLOUD_ORIGIN,
        challenge=challenge,
    )
    proc = subprocess.run(
        [
            str(helper_bin),
            "revoke_epoch_check",
            json.dumps(
                {
                    "origin": DEFAULT_CLOUD_ORIGIN,
                    "setup_code": DEFAULT_SETUP_CODE,
                    "front_key": DEFAULT_FRONT_KEY,
                    "cdn_key": DEFAULT_CDN_KEY,
                    "challenge": challenge,
                    "credential": cred_payload,
                }
            ),
        ],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or f"revoke_epoch_check exited {proc.returncode}")
    return json.loads(proc.stdout)


@dataclass
class PasskeyIdentity:
    """Synthetic Ed25519 WebAuthn passkey identity + active session cookie."""

    display_name: str
    role: str
    cred_id: bytes
    private_key: Ed25519PrivateKey  # gitleaks:allow
    session_cookie: str
    sign_count: int = 0


def _build_ed25519_attestation(
    *,
    rp_id: str,
    origin: str,
    challenge: str,
) -> tuple[bytes, Ed25519PrivateKey, dict[str, Any]]:
    priv = Ed25519PrivateKey.generate()
    pub_bytes = priv.public_key().public_bytes_raw()
    cred_id = secrets.token_bytes(16)

    rp_id_hash = hashlib.sha256(rp_id.encode("utf-8")).digest()
    flags = b"\x45"  # UP (0x01) | UV (0x04) | AT (0x40)
    sign_count = b"\x00\x00\x00\x00"
    aaguid = b"\x00" * 16
    cred_len = len(cred_id).to_bytes(2, "big")
    # COSE_Key: {1: 1 (OKP), 3: -8 (EdDSA), -1: 6 (Ed25519), -2: pub_bytes}
    cose_key = b"\xa4\x01\x01\x03\x27\x20\x06\x21\x58\x20" + pub_bytes
    auth_data = rp_id_hash + flags + sign_count + aaguid + cred_len + cred_id + cose_key
    att_obj = b"\xa3\x63fmt\x64none\x67attStmt\xa0\x68authData\x58" + bytes([len(auth_data)]) + auth_data

    client_data = json.dumps(
        {
            "type": "webauthn.create",
            "challenge": challenge,
            "origin": origin,
            "crossOrigin": False,
        },
        separators=(",", ":"),
    ).encode("utf-8")

    cred_b64 = _b64url(cred_id)
    credential = {
        "id": cred_b64,
        "rawId": cred_b64,
        "type": "public-key",
        "response": {
            "clientDataJSON": _b64url(client_data),
            "attestationObject": _b64url(att_obj),
        },
    }
    return cred_id, priv, credential


def _build_ed25519_assertion(
    identity: PasskeyIdentity,
    *,
    rp_id: str,
    origin: str,
    challenge: str,
) -> dict[str, Any]:
    identity.sign_count += 1
    rp_id_hash = hashlib.sha256(rp_id.encode("utf-8")).digest()
    flags = b"\x05"  # UP (0x01) | UV (0x04)
    sign_count = identity.sign_count.to_bytes(4, "big")
    auth_data = rp_id_hash + flags + sign_count

    client_data = json.dumps(
        {
            "type": "webauthn.get",
            "challenge": challenge,
            "origin": origin,
            "crossOrigin": False,
        },
        separators=(",", ":"),
    ).encode("utf-8")
    sig = identity.private_key.sign(auth_data + hashlib.sha256(client_data).digest())
    cred_b64 = _b64url(identity.cred_id)
    return {
        "id": cred_b64,
        "rawId": cred_b64,
        "type": "public-key",
        "response": {
            "clientDataJSON": _b64url(client_data),
            "authenticatorData": _b64url(auth_data),
            "signature": _b64url(sig),
        },
    }


def extract_cookie_value(resp: httpx.Response, cookie_name: str) -> str | None:
    """Extract a specific Set-Cookie value from an httpx.Response."""
    prefix = f"{cookie_name}="
    for hdr in resp.headers.get_list("set-cookie"):
        first = hdr.split(";", 1)[0].strip()
        if first.startswith(prefix):
            return first[len(prefix) :]
    return None


class CloudTestServer:
    """Managed local loopback shell-test-server instance."""

    def __init__(
        self,
        *,
        origin: str = DEFAULT_CLOUD_ORIGIN,
        setup_code: str = DEFAULT_SETUP_CODE,
        front_key: str = DEFAULT_FRONT_KEY,
    ) -> None:
        self.origin = origin
        self.rp_id = urlparse(origin).hostname or "logs.example.test"
        self.setup_code = setup_code
        self.front_key = front_key
        self.port = self._pick_free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        self._proc: subprocess.Popen[bytes] | None = None
        self._client: httpx.Client | None = None
        self._atexit_registered: bool = False

    @staticmethod
    def _pick_free_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            return int(s.getsockname()[1])

    def start(self) -> None:
        server_bin, _ = ensure_cloud_binaries()
        env = os.environ.copy()
        env["FLP_TEST_LISTEN"] = f"127.0.0.1:{self.port}"
        env["FLP_TEST_ORIGIN"] = self.origin
        env["FLP_TEST_SETUP_CODE"] = self.setup_code
        env["FLP_TEST_FRONT_KEY"] = self.front_key

        self._proc = subprocess.Popen(
            [str(server_bin)],
            cwd=str(CLOUD_DIR),
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        atexit.register(self.stop)
        self._atexit_registered = True
        try:
            self._client = httpx.Client(base_url=self.base_url, timeout=10.0, follow_redirects=False)

            deadline = time.monotonic() + 10.0
            while time.monotonic() < deadline:
                if self._proc.poll() is not None:
                    raise RuntimeError(f"shell-test-server exited early with rc={self._proc.returncode}")
                try:
                    r = self.request("GET", "/version")
                    if r.status_code == 200:
                        return
                except httpx.HTTPError:
                    time.sleep(0.03)
            raise RuntimeError("Timed out waiting for shell-test-server on loopback")
        except BaseException:
            self.stop()
            raise

    def stop(self) -> None:
        if self._atexit_registered:
            atexit.unregister(self.stop)
            self._atexit_registered = False
        if self._client is not None:
            self._client.close()
            self._client = None
        if self._proc is not None:
            proc = self._proc
            self._proc = None
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)
            else:
                proc.wait(timeout=5)

    def reset(self) -> None:
        resp = self.request("POST", "/__test/reset", json_body={})
        assert resp.status_code == 200, f"reset failed: {resp.status_code} {resp.text}"

    def request(
        self,
        method: str,
        path: str,
        *,
        session_cookie: str | None = None,
        setup_cookie: str | None = None,
        raw_cookie: str | None = None,
        client_ip: str = "198.51.100.10",
        front_key: str | None = DEFAULT_FRONT_KEY,
        front_rl: str | None = "1",
        origin: str | None = DEFAULT_CLOUD_ORIGIN,
        harness_header: bool = False,
        extra_headers: dict[str, str] | None = None,
        json_body: Any = None,
    ) -> httpx.Response:
        assert self._client is not None, "CloudTestServer not started"
        headers: dict[str, str] = {"fastly-client-ip": client_ip}
        if front_key is not None:
            headers["x-flp-front-key"] = front_key
        if front_rl is not None:
            headers["x-flp-front-rl"] = front_rl
        if method.upper() not in {"GET", "HEAD"}:
            if origin is not None:
                headers["origin"] = origin
            headers.setdefault("content-type", "application/json")
        if harness_header:
            headers["x-flp-harness"] = "fdt-e2e-security"

        cookies_parts: list[str] = []
        if raw_cookie is not None:
            cookies_parts.append(raw_cookie)
        else:
            if session_cookie is not None:
                cookies_parts.append(f"flp_session={session_cookie}")
            if setup_cookie is not None:
                cookies_parts.append(f"flp_setup={setup_cookie}")
        if cookies_parts:
            headers["cookie"] = "; ".join(cookies_parts)

        if extra_headers:
            headers.update(extra_headers)

        content: bytes | None = None
        if json_body is not None:
            headers.setdefault("content-type", "application/json")
            content = json.dumps(json_body).encode("utf-8")

        return self._client.request(method.upper(), path, headers=headers, content=content)

    def register_admin(
        self,
        display_name: str = "Enterprise Admin",
        *,
        client_ip: str = "198.51.100.10",
    ) -> PasskeyIdentity:
        r_setup = self.request(
            "POST",
            "/api/setup/accept",
            client_ip=client_ip,
            json_body={"code": self.setup_code},
        )
        assert r_setup.status_code == 200, f"setup accept failed: {r_setup.status_code} {r_setup.text}"
        setup_cookie = extract_cookie_value(r_setup, "flp_setup")
        assert setup_cookie, "missing flp_setup cookie"

        r_opts = self.request(
            "POST",
            "/api/auth/register/options",
            setup_cookie=setup_cookie,
            client_ip=client_ip,
            json_body={"display_name": display_name, "invite_token": None},
        )
        assert r_opts.status_code == 200, f"register options failed: {r_opts.status_code} {r_opts.text}"
        opts = r_opts.json()
        challenge = opts["public_key"]["challenge"] if "public_key" in opts else opts["publicKey"]["challenge"]
        challenge_id = opts["challenge_id"]

        cred_id, priv, cred_payload = _build_ed25519_attestation(
            rp_id=self.rp_id,
            origin=self.origin,
            challenge=challenge,
        )
        r_ver = self.request(
            "POST",
            "/api/auth/register/verify",
            setup_cookie=setup_cookie,
            client_ip=client_ip,
            json_body={"challenge_id": challenge_id, "credential": cred_payload},
        )
        assert r_ver.status_code == 200, f"register verify failed: {r_ver.status_code} {r_ver.text}"
        sess_cookie = extract_cookie_value(r_ver, "flp_session")
        assert sess_cookie, "missing flp_session cookie"
        return PasskeyIdentity(
            display_name=display_name,
            role="admin",
            cred_id=cred_id,
            private_key=priv,
            session_cookie=sess_cookie,
        )

    def invite_and_register(
        self,
        admin: PasskeyIdentity,
        *,
        role: str,
        display_name: str,
        client_ip: str = "198.51.100.20",
    ) -> PasskeyIdentity:
        """Create an invite ('analyst' or 'analyst-no-pii') as Admin and register passkey."""
        r_inv = self.request(
            "POST",
            "/api/admin/invites",
            session_cookie=admin.session_cookie,
            json_body={"display_name": display_name, "role": role},
        )
        assert r_inv.status_code in {200, 201}, f"invite failed: {r_inv.status_code} {r_inv.text}"
        inv_body = r_inv.json()
        invite_token = inv_body.get("invite_token") or inv_body.get("token")
        assert invite_token, f"missing invite token in {inv_body}"

        r_opts = self.request(
            "POST",
            "/api/auth/register/options",
            client_ip=client_ip,
            json_body={"display_name": display_name, "invite_token": invite_token},
        )
        assert r_opts.status_code == 200, f"invite register options failed: {r_opts.status_code} {r_opts.text}"
        opts = r_opts.json()
        challenge = opts["public_key"]["challenge"] if "public_key" in opts else opts["publicKey"]["challenge"]
        challenge_id = opts["challenge_id"]

        cred_id, priv, cred_payload = _build_ed25519_attestation(
            rp_id=self.rp_id,
            origin=self.origin,
            challenge=challenge,
        )
        r_ver = self.request(
            "POST",
            "/api/auth/register/verify",
            client_ip=client_ip,
            json_body={"challenge_id": challenge_id, "credential": cred_payload},
        )
        assert r_ver.status_code == 200, f"invite register verify failed: {r_ver.status_code} {r_ver.text}"
        sess_cookie = extract_cookie_value(r_ver, "flp_session")
        assert sess_cookie, "missing flp_session cookie"
        return PasskeyIdentity(
            display_name=display_name,
            role=role,
            cred_id=cred_id,
            private_key=priv,
            session_cookie=sess_cookie,
        )

    def login_passkey(
        self,
        identity: PasskeyIdentity,
        *,
        client_ip: str = "198.51.100.10",
    ) -> str:
        r_opts = self.request("POST", "/api/auth/login/options", client_ip=client_ip, json_body={})
        assert r_opts.status_code == 200, f"login options failed: {r_opts.status_code} {r_opts.text}"
        opts = r_opts.json()
        challenge = opts["public_key"]["challenge"] if "public_key" in opts else opts["publicKey"]["challenge"]
        challenge_id = opts["challenge_id"]
        assertion = _build_ed25519_assertion(
            identity,
            rp_id=self.rp_id,
            origin=self.origin,
            challenge=challenge,
        )
        r_ver = self.request(
            "POST",
            "/api/auth/login/verify",
            client_ip=client_ip,
            json_body={"challenge_id": challenge_id, "credential": assertion},
        )
        assert r_ver.status_code == 200, f"login verify failed: {r_ver.status_code} {r_ver.text}"
        sess_cookie = extract_cookie_value(r_ver, "flp_session")
        assert sess_cookie, "missing flp_session cookie"
        identity.session_cookie = sess_cookie
        return sess_cookie

    def provision_fixture_service(self, admin: PasskeyIdentity) -> str:
        """Provision 'fixtureService' to completion via /api/admin/provision/execute + /jobs/<id>/step."""
        r_exec = self.request(
            "POST",
            "/api/admin/provision/execute",
            session_cookie=admin.session_cookie,
            harness_header=True,
            json_body={
                "token": "fixture-token",
                "service_id": "fixtureService",
                "fos_bucket": "fos-fixture-logs",
                "fos_region": "us-east-1",
                "log_fields": {
                    "groups": ["A", "C"],
                    "custom_fields": [],
                    "field_overrides": {},
                    "field_limits": {},
                },
            },
        )
        assert r_exec.status_code in {200, 201, 202}, f"provision execute failed: {r_exec.status_code} {r_exec.text}"
        payload = r_exec.json()
        job = payload.get("job") if isinstance(payload.get("job"), dict) else payload
        job_id = job.get("id") or job.get("job_id")
        assert job_id, f"missing job id in provision response: {payload}"
        for _ in range(100):
            if job.get("status") in {"complete", "completed", "done"}:
                break
            retry_at = job.get("retry_at")
            if isinstance(retry_at, int | float):
                wait_s = retry_at - time.time() + 0.05
                if wait_s > 0:
                    time.sleep(min(1.5, wait_s))
            r_step = self.request(
                "POST",
                f"/api/admin/jobs/{job_id}/step",
                session_cookie=admin.session_cookie,
                json_body={},
            )
            assert r_step.status_code == 200, f"job step failed: {r_step.status_code} {r_step.text}"
            step_payload = r_step.json()
            job = step_payload.get("job") if isinstance(step_payload.get("job"), dict) else step_payload
        assert job.get("status") in {"complete", "completed", "done"}, f"job did not complete: {job}"
        self.request(
            "GET",
            "/_tick",
            front_key=self.front_key,
            extra_headers={"x-flp-tick": "1"},
        )
        return "fixtureService"

    def service_generation(self, admin: PasskeyIdentity, sid: str = "fixtureService") -> str:
        """Fetch current KV CAS generation token for a provisioned Cloud service."""
        r = self.request("GET", f"/api/admin/services/{sid}", session_cookie=admin.session_cookie)
        assert r.status_code == 200, f"GET /api/admin/services/{sid} failed: {r.status_code} {r.text}"
        return str(r.json()["generation"])


def build_front_vcl_wrapper(
    *,
    front_key: str = DEFAULT_FRONT_KEY,
    cdn_key: str = DEFAULT_CDN_KEY,
    sid: str = FIXTURE_22_SID,
    bucket_entry: str = "fos-test-logs|us-east-1",
    extra_front_auth: dict[str, str] | None = None,
) -> tuple[str, dict[str, str]]:
    """Build a falco-testable main.vcl wrapper around compute/cloud/vcl/front/main.vcl."""
    front_vcl_text = FRONT_MAIN_VCL.read_text(encoding="utf-8")
    # Provide stub backends if main.vcl doesn't declare them directly
    auth_items = {"front_key": front_key, "cdn_key": cdn_key}
    if extra_front_auth:
        auth_items.update(extra_front_auth)
    auth_lines = "\n".join(f'  "{k}": "{v}",' for k, v in auth_items.items())

    wrapper = f"""backend F_compute {{ .host = "127.0.0.1"; .port = "443"; }}
backend F_fos_us_east_1 {{ .host = "us-east-1.object.fastlystorage.app"; .port = "443"; }}

table flp_front_auth {{
{auth_lines}
}}

table flp_fos_buckets {{
  "{sid}": "{bucket_entry}",
}}

table flp_fos_credentials {{
  "{sid}.ak": "test-sentinel-fos-ak-0001",
  "{sid}.sk": "test-sentinel-fos-sk-0001",
}}

sub vcl_recv {{
  set req.backend = F_compute;
}}

include "front_main.vcl";
"""
    return wrapper, {
        "front_main.vcl": front_vcl_text,
        ".falco.yaml": "testing:\n  overrides:\n    req.is_ssl: true\n",
    }


def make_flp_cdn_cookie(
    *,
    session_cookie: str,
    cdn_key: str = DEFAULT_CDN_KEY,
    sid: str = FIXTURE_22_SID,
    role: str = "admin",
    exp: int | None = None,
    scope: str = "",
    epoch: str = "0",
    tamper_sig: bool = False,
) -> str:
    """Mint a valid or tampered flp_cdn cookie matching front/main.vcl's HMAC format."""
    if exp is None:
        exp = int(time.time()) + 240
    shash = hashlib.sha256(session_cookie.encode("utf-8")).hexdigest()[:16]
    vcl_src = FRONT_MAIN_VCL.read_text(encoding="utf-8")

    # Detect if M3 bound auth_epoch/policy_version into flp_cdn payload/version prefix
    if "flp-cdn-v2~" in vcl_src and epoch != "":
        prefix = "flp-cdn-v2"
        payload = f"{sid}~{role}~{exp:010d}~{shash}~{epoch}~{scope}"
    else:
        prefix = "flp-cdn-v1"
        payload = f"{sid}~{role}~{exp:010d}~{shash}~{scope}"

    sig = hmac.new(
        cdn_key.encode(),
        f"{prefix}~{payload}".encode(),
        hashlib.sha256,
    ).hexdigest()
    if tamper_sig:
        sig = ("0" if sig[0] != "0" else "f") + sig[1:]
    return f"{payload}~{sig}"


def build_cloud_capture_vcl_wrapper(snippets: list[dict[str, Any]]) -> str:
    """Wrap compute/cloud logging::desired_snippets output into a falco-runnable VCL file."""
    by_sub: dict[str, list[str]] = {}
    for snip in sorted(snippets, key=lambda s: int(s.get("priority", 10))):
        sub = str(snip.get("subroutine") or "")
        body = str(snip.get("body") or "").strip()
        if sub and body:
            by_sub.setdefault(sub, []).append(body)

    blocks = [
        """backend F_origin { .host = "127.0.0.1"; .port = "8080"; }
backend F_fos_origin { .host = "us-east-1.object.fastlystorage.app"; .port = "443"; }
backend ssl_shield_iad_va_us { .host = "127.0.0.2"; .port = "443"; }

table fos_credentials {
  "access_key": "test-sentinel-fos-ak-0001",
  "secret_key": "test-sentinel-fos-sk-0001",
  "bucket": "fos-test-logs",
  "region": "us-east-1",
}

sub vcl_recv {
  set req.backend = F_origin;
}"""
    ]
    for sub_name in ("vcl_recv", "vcl_miss", "vcl_pass", "vcl_fetch", "vcl_deliver", "vcl_error"):
        bodies = "\n\n".join(by_sub.get(sub_name, []))
        bodies = re.sub(
            r"fastly_try_select_shield\(\s*[A-Za-z0-9_]+\s*,\s*([A-Za-z0-9_]+)\s*\)",
            r"\1",
            bodies,
        )
        blocks.append(f"sub {sub_name} {{\n{bodies}\n}}")
    return "\n\n".join(blocks) + "\n"
