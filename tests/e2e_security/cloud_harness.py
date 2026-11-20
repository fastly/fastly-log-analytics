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

_TMP_ROOT = Path(tempfile.gettempdir())
_UID = os.getuid()
LOCK_FILE = _TMP_ROOT / f"fla-e2e-sec-build-{_UID}.lock"
WEB_OUT_DIR = _TMP_ROOT / f"fla-e2e-sec-web-out-{_UID}"
CARGO_TARGET_DIR = _TMP_ROOT / f"fla-e2e-sec-cargo-target-{_UID}"
DRIVER_DIR = _TMP_ROOT / f"fla-e2e-sec-driver-{_UID}"

DEFAULT_CLOUD_ORIGIN = "https://logs.example.test"
DEFAULT_SETUP_CODE = "test-sentinel-setup-code-0001"
DEFAULT_FRONT_KEY = "test-sentinel-front-key-0001"  # gitleaks:allow
DEFAULT_CDN_KEY = "test-sentinel-cdn-key-0001"
FIXTURE_22_SID = "AAAAAAAAAAAAAAAAAAAAAA"

_BUILT_PATHS: tuple[Path, Path] | None = None

_HELPER_RS = r"""
use fastly_log_analytics::{
    admin::{fixture::FixtureAdapter, ApiRequest, ApiResponse, FosRequest, FosResponse, Record},
    auth::{
        self, AuditEntry, AuthConfig, AuthService, AuthState, BrowserCredential, Challenge,
        Credential, RegistrationAuthority, Role, SetupGrant, SetupState, User,
    },
    http::Request,
    logging,
    setup::{SetupStores, CONFIG_KEYS, SECRET_KEYS},
    shell::{Runtime, Shell},
    state::{MemoryStore, StateStore},
};
use serde_json::{json, Value};
use std::collections::BTreeMap;

struct EpochTestRuntime {
    config: BTreeMap<String, String>,
    secrets: BTreeMap<String, String>,
    admin: FixtureAdapter,
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
        secrets.insert("session_key".into(), "s".repeat(48));
        secrets.insert(
            "admin_setup_hash".into(),
            json!({"sha256": auth::hash(setup_code), "expires_at": 1_800_000_900u64}).to_string(),
        );
        Self {
            config,
            secrets,
            admin: FixtureAdapter::new(),
        }
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
        Ok([11u8; 32])
    }
    fn now(&self) -> u64 {
        1_800_000_000
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
        self.admin.api(request).map(Into::into)
    }
    fn admin_fos(&self, request: FosRequest<'_>) -> Result<FosResponse, String> {
        self.admin.fos(request)
    }
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


def ensure_cloud_binaries() -> tuple[Path, Path]:
    """Build shell-test-server and e2e-cloud-vcl-helper under an exclusive file lock."""
    global _BUILT_PATHS
    helper_src = DRIVER_DIR / "main.rs"
    if (
        _BUILT_PATHS is not None
        and all(p.exists() for p in _BUILT_PATHS)
        and helper_src.exists()
        and helper_src.read_text(encoding="utf-8") == _HELPER_RS
    ):
        return _BUILT_PATHS

    LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    with LOCK_FILE.open("w", encoding="utf-8") as lock_fh:
        fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
        _stage_synthetic_web_out()
        CARGO_TARGET_DIR.mkdir(parents=True, exist_ok=True)
        DRIVER_DIR.mkdir(parents=True, exist_ok=True)

        env = os.environ.copy()
        env["CARGO_TARGET_DIR"] = str(CARGO_TARGET_DIR)
        env["FLP_WEB_OUT"] = str(WEB_OUT_DIR)
        env["FLP_SCORER_PACKAGE"] = "/nonexistent"

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

        lib_rlib: str | None = None
        serde_json_rlib: str | None = None
        server_bin = CARGO_TARGET_DIR / "debug" / "shell-test-server"

        for line in proc.stdout.splitlines():
            if not line.startswith("{"):
                continue
            msg = json.loads(line)
            if msg.get("reason") != "compiler-artifact":
                continue
            target = msg.get("target") or {}
            tname = target.get("name")
            tkinds = target.get("kind") or []
            filenames = msg.get("filenames") or []
            if tname == "fastly_log_analytics" and "lib" in tkinds:
                for fn in filenames:
                    if fn.endswith(".rlib"):
                        lib_rlib = fn
            elif tname == "serde_json" and "lib" in tkinds:
                for fn in filenames:
                    if fn.endswith(".rlib"):
                        serde_json_rlib = fn
            elif tname == "shell-test-server" and "bin" in tkinds and filenames:
                server_bin = Path(filenames[0])

        if not lib_rlib or not serde_json_rlib:
            raise RuntimeError(f"Failed to locate rlib artifacts (lib={lib_rlib!r}, serde_json={serde_json_rlib!r})")

        helper_src = DRIVER_DIR / "main.rs"
        helper_bin = DRIVER_DIR / "e2e-cloud-vcl-helper"
        helper_src.write_text(_HELPER_RS, encoding="utf-8")
        deps_dir = CARGO_TARGET_DIR / "debug" / "deps"

        rustc_proc = subprocess.run(
            [
                "rustc",
                "--edition",
                "2021",
                str(helper_src),
                "-L",
                f"dependency={deps_dir}",
                "--extern",
                f"fastly_log_analytics={lib_rlib}",
                "--extern",
                f"serde_json={serde_json_rlib}",
                "-o",
                str(helper_bin),
            ],
            cwd=str(CLOUD_DIR),
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        if rustc_proc.returncode != 0:
            raise RuntimeError(f"rustc helper compile failed:\n{rustc_proc.stderr}")

        _BUILT_PATHS = (server_bin, helper_bin)
        return _BUILT_PATHS


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
    return wrapper, {"front_main.vcl": front_vcl_text}


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
