"""Milestone M1 security regression tests for SEC-01, SEC-02, SEC-03, and SEC-04."""

from __future__ import annotations

import pathlib
import shutil
import sqlite3
import subprocess

import pytest
import yaml
from fastapi import Depends, FastAPI, Request
from fastapi.testclient import TestClient
from starlette.requests import Request as StarletteRequest
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

import backend.config as svcconfig
import backend.core.oauth as oauth_registry
import backend.routers.share_admin as share_admin
from backend.core import share_db
from backend.core.request_context import require_admin
from backend.utils import tunnel
from backend.utils.remote_access import (
    ADMIN_GATEWAY_HEADER,
    RemoteAccessMiddleware,
    _analyst_api_limiter,
    _static_limiter,
    is_request_remote,
)

pytestmark = pytest.mark.no_postgres

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
SECRET = "m1-admin-gateway-secret-at-least-32-chars!!"


def _asgi_req(
    peer: tuple[str, int] | None,
    headers: dict[str, str] | list[tuple[str, str]] | None = None,
    *,
    path: str = "/api/admin/probe",
) -> StarletteRequest:
    raw_items = headers.items() if isinstance(headers, dict) else (headers or [])
    hdrs = [(k.lower().encode("latin-1"), v.encode("latin-1")) for k, v in raw_items]
    scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "headers": hdrs,
        "client": peer,
        "server": ("127.0.0.1", 8000),
        "scheme": "http",
        "query_string": b"",
    }
    return StarletteRequest(scope)


@pytest.fixture
def isolated_share_env(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch):
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("REMOTE_SHARE_DB_DIR", str(tmp_path / "system"))
    monkeypatch.setattr(svcconfig, "DATA_DIR", data_dir)
    monkeypatch.setattr(svcconfig, "load_config", lambda sid: {"service_id": sid} if sid == "svcA" else None)
    con = sqlite3.connect(str(tmp_path / "share.sqlite"), check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON")
    for ddl in share_db._SCHEMA:
        con.execute(ddl)
    con.commit()
    monkeypatch.setattr(share_db, "get_global_share_con", lambda: con)
    share_db.publish_tos_version("v1", "Test Terms of Service", con=con)
    tunnel.reset_for_tests()
    _static_limiter.clear()
    _analyst_api_limiter.clear()
    yield tmp_path
    _static_limiter.clear()
    _analyst_api_limiter.clear()
    tunnel.reset_for_tests()
    con.close()


# ── SEC-01: Shared-edge Fastly-Client-IP spoofing & Admin-Gateway preservation ─


@pytest.mark.parametrize(
    "spoof_headers",
    [
        {"X-Proxied-By-Caddy": "true"},
        {"X-FLA-Public-Gateway": "true"},
        {"X-FLA-Frontend-Proxy": "1"},
        {"Fastly-Client-IP": "127.0.0.1"},
        {"True-Client-IP": "127.0.0.1"},
        {"Forwarded": "for=127.0.0.1;proto=https"},
        {"X-Real-IP": "127.0.0.1"},
        {"X-Proxied-By-Caddy": "true", "X-Forwarded-For": "127.0.0.1"},
    ],
)
def test_sec01_loopback_peer_with_public_or_client_ip_headers_is_remote(
    monkeypatch: pytest.MonkeyPatch, spoof_headers: dict[str, str]
) -> None:
    monkeypatch.delenv("ADMIN_GATEWAY_REQUIRED", raising=False)
    monkeypatch.delenv("ADMIN_GATEWAY_SECRET", raising=False)
    req = _asgi_req(("127.0.0.1", 54321), spoof_headers)
    assert is_request_remote(req) is True


def test_sec01_uvicorn_proxy_headers_spoofed_loopback_cannot_reach_admin(
    isolated_share_env: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("ADMIN_GATEWAY_REQUIRED", raising=False)
    app = FastAPI()

    @app.get("/api/admin/secret-probe", dependencies=[Depends(require_admin)])
    def secret_probe(request: Request) -> dict[str, object]:
        return {"ok": True, "remote": getattr(request.state, "is_remote", None)}

    app.add_middleware(RemoteAccessMiddleware)
    app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="127.0.0.1")

    tunnel.get_tunnel_manager().start_sharing(public_endpoint="https://share.example.com")
    with TestClient(app, client=("127.0.0.1", 50000)) as c:
        # Simulate Caddy/shared-edge forwarding spoofed X-Forwarded-For: 127.0.0.1 + X-Proxied-By-Caddy: true
        resp = c.get(
            "/api/admin/secret-probe",
            headers={
                "Host": "share.example.com",
                "X-Proxied-By-Caddy": "true",
                "X-Forwarded-For": "127.0.0.1",
                "Fastly-Client-IP": "127.0.0.1",
            },
        )
        assert resp.status_code in (401, 403)
        assert resp.json().get("ok") is not True


@pytest.mark.parametrize("hdr_name", [ADMIN_GATEWAY_HEADER, "X-FLA-Admin-Gateway"])
def test_sec01_mtls_admin_gateway_stays_local_with_xff_but_rejects_public_markers(
    isolated_share_env: pathlib.Path, monkeypatch: pytest.MonkeyPatch, hdr_name: str
) -> None:
    monkeypatch.setenv("ADMIN_GATEWAY_REQUIRED", "1")
    monkeypatch.setenv("ADMIN_GATEWAY_SECRET", SECRET)

    # mTLS admin-gateway forwards client IP in X-Forwarded-For without public-gateway markers -> local Admin!
    mtls_req = _asgi_req(
        ("10.244.1.9", 41000),
        {hdr_name: SECRET, "X-Forwarded-For": "10.253.0.12", "Host": "10.253.3.86:8443"},
    )
    assert is_request_remote(mtls_req) is False

    # Same valid token combined with ANY public proxy marker MUST be rejected!
    for marker in ("X-Proxied-By-Caddy", "X-FLA-Public-Gateway", "X-FLA-Frontend-Proxy"):
        poisoned = _asgi_req(("10.244.1.9", 41000), {hdr_name: SECRET, marker: "true"})
        assert is_request_remote(poisoned) is True

    app = FastAPI()

    @app.get("/api/admin/check", dependencies=[Depends(require_admin)])
    def admin_check(request: Request) -> dict[str, bool]:
        return {
            "is_remote": request.state.is_remote,
            "leaked_hdr": hdr_name.lower() in {k.lower() for k in request.headers.keys()},
        }

    app.add_middleware(RemoteAccessMiddleware)
    with TestClient(app, client=("10.244.1.9", 41000)) as c:
        ok = c.get(
            "/api/admin/check", headers={hdr_name: SECRET, "X-Forwarded-For": "10.253.0.12", "Host": "localhost"}
        )
        assert ok.status_code == 200
        assert ok.json() == {"is_remote": False, "leaked_hdr": False}

        bad = c.get(
            "/api/admin/check",
            headers={hdr_name: SECRET, "X-FLA-Public-Gateway": "true", "Host": "localhost"},
        )
        assert bad.status_code == 401
        assert bad.json()["error"] == "admin_gateway_invalid"


def test_sec01_caddyfile_fastly_v6_fcip_sanitization_and_rate_limit_key(tmp_path: pathlib.Path) -> None:
    import re

    caddyfile = (REPO_ROOT / "Caddyfile").read_text(encoding="utf-8")
    assert "@from_fastly_v4" in caddyfile
    assert "@from_fastly_v6" in caddyfile
    assert "2a04:4e40::/32" in caddyfile and "2a04:4e42::/32" in caddyfile
    assert "@bad_fcip" in caddyfile
    assert "request_header -X-Admin-Gateway-Token" in caddyfile
    assert "request_header -X-FLA-Admin-Gateway" in caddyfile
    assert "request_header -X-FLA-Frontend-Proxy" in caddyfile
    assert "key {http.request.header.X-Forwarded-For}" in caddyfile
    assert "key {http.request.header.Fastly-Client-IP}" not in caddyfile

    caddy_bin = shutil.which("caddy")
    if caddy_bin:
        mods = subprocess.run([caddy_bin, "list-modules"], capture_output=True, text=True, timeout=10, check=False)
        target_cfg = REPO_ROOT / "Caddyfile"
        if "http.handlers.rate_limit" not in mods.stdout:
            sanitized = re.sub(r"^\s*order\s+rate_limit\s+before\s+reverse_proxy\s*$", "", caddyfile, flags=re.M)
            sanitized = re.sub(r"^\s*@share_login\s+path\s+/api/share/login\s*$", "", sanitized, flags=re.M)
            sanitized = re.sub(
                r"rate_limit\s+@share_login\s*\{\s*zone\s+share_login\s*\{[^)]*?events\s+\d+\s*\}\s*\}",
                "",
                sanitized,
                flags=re.S,
            )
            target_cfg = tmp_path / "Caddyfile.stdlib"
            target_cfg.write_text(sanitized, encoding="utf-8")
        proc = subprocess.run(
            [caddy_bin, "adapt", "--config", str(target_cfg), "--adapter", "caddyfile"],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        assert proc.returncode == 0, proc.stderr


# ── SEC-02: K8s X-Forwarded-For: 127.0.0.1 spoofing & public-gateway ConfigMap ─


def test_sec02_k8s_pod_xff_loopback_spoof_is_classified_remote(
    isolated_share_env: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("ADMIN_GATEWAY_REQUIRED", raising=False)
    monkeypatch.setenv("TRUSTED_PROXY_IPS", "10.0.0.0/8,172.16.0.0/12,192.168.0.0/16")

    app = FastAPI()

    @app.get("/api/admin/k8s-probe", dependencies=[Depends(require_admin)])
    def k8s_probe() -> dict[str, bool]:
        return {"admin": True}

    app.add_middleware(RemoteAccessMiddleware)
    app.add_middleware(
        ProxyHeadersMiddleware,
        trusted_hosts=["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"],
    )
    tunnel.get_tunnel_manager().start_sharing(public_endpoint="https://share.example.com")
    with TestClient(app, client=("10.244.2.15", 45000)) as c:
        resp = c.get(
            "/api/admin/k8s-probe",
            headers={"Host": "share.example.com", "X-Forwarded-For": "127.0.0.1"},
        )
        assert resp.status_code == 401
        assert resp.json()["error"] == "unauthenticated"


def test_sec02_helm_public_gateway_trusted_proxies_and_caddy_adapt(tmp_path: pathlib.Path) -> None:
    helm_bin = shutil.which("helm")
    if not helm_bin:
        pytest.skip("helm binary not installed")
    chart_dir = REPO_ROOT / "deploy" / "chart" / "fastly-log-analytics"
    proc = subprocess.run(
        [
            helm_bin,
            "template",
            "fla-test",
            str(chart_dir),
            "--set",
            "adminGateway.enabled=true",
            "--set",
            "adminGateway.existingSecret=operator-gateway",
            "--set",
            "adminGateway.host=admin.example.com",
            "--set",
            "adminGateway.image=registry.example.com/fla-admin-gateway:test",
            "--set",
            "secrets.existingSecret=test-sec",
            "--set",
            "secrets.cursorSecret=test-cursor-secret-32-chars-minimum",
        ],
        capture_output=True,
        text=True,
        timeout=20,
        check=True,
    )
    docs = [d for d in yaml.safe_load_all(proc.stdout) if d]
    cm = next(
        d
        for d in docs
        if d.get("kind") == "ConfigMap" and d.get("metadata", {}).get("name", "").endswith("-public-gateway")
    )
    caddy_cfg = cm["data"]["Caddyfile"]
    assert "trusted_proxies static 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16" in caddy_cfg
    assert "max_size 25MB" in caddy_cfg
    assert "request_header -X-Admin-Gateway-Token" in caddy_cfg
    assert "request_header -X-FLA-Admin-Gateway" in caddy_cfg
    assert "@bad_fcip" in caddy_cfg
    assert "2a04:4e40::/32" in caddy_cfg
    assert 'X-Content-Type-Options "nosniff"' in caddy_cfg
    assert 'X-Frame-Options "DENY"' in caddy_cfg
    assert 'Referrer-Policy "strict-origin-when-cross-origin"' in caddy_cfg
    assert "-Server" in caddy_cfg

    caddy_bin = shutil.which("caddy")
    if caddy_bin:
        cfg_file = tmp_path / "public-gateway.Caddyfile"
        cfg_file.write_text(caddy_cfg, encoding="utf-8")
        adapt = subprocess.run(
            [caddy_bin, "adapt", "--config", str(cfg_file), "--adapter", "caddyfile"],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        assert adapt.returncode == 0, adapt.stderr


# ── SEC-03: X-Remote-Analyst: 1 stays remote even when sharing is inactive ────


def test_sec03_x_remote_analyst_is_remote_when_sharing_stopped(
    isolated_share_env: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("ADMIN_GATEWAY_REQUIRED", raising=False)
    mgr = tunnel.get_tunnel_manager()
    assert mgr.is_sharing_active() is False

    req = _asgi_req(("127.0.0.1", 12345), {"Host": "testserver", "X-Remote-Analyst": "1"})
    assert is_request_remote(req) is True

    app = FastAPI()

    @app.get("/api/admin/ssr-probe", dependencies=[Depends(require_admin)])
    def ssr_probe() -> dict[str, bool]:
        return {"admin": True}

    app.add_middleware(RemoteAccessMiddleware)
    with TestClient(app, client=("127.0.0.1", 12345)) as c:
        resp = c.get("/api/admin/ssr-probe", headers={"Host": "testserver", "X-Remote-Analyst": "1"})
        assert resp.status_code in (400, 401, 403)
        assert resp.json().get("admin") is not True


# ── SEC-04: Authoritative sharing_enabled & Host: localhost spoofing block ────


@pytest.mark.parametrize("spoof_host", ["localhost", "127.0.0.1", "127.0.0.1:8000", "[::1]", "0.0.0.0", "backend"])
def test_sec04_external_host_localhost_spoofing_rejected(isolated_share_env: pathlib.Path, spoof_host: str) -> None:
    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.add_api_route("/api/health", lambda: {"ok": True}, methods=["GET"])

    # Rejected both before sharing starts and while sharing is active
    with TestClient(app, client=("203.0.113.44", 51000)) as c:
        r1 = c.get("/api/health", headers={"Host": spoof_host, "X-Proxied-By-Caddy": "true"})
        assert r1.status_code == 400
        assert r1.json()["error"] == "host_not_allowed"

        tunnel.get_tunnel_manager().start_sharing(public_endpoint="https://share.example.com")
        r2 = c.get("/api/health", headers={"Host": spoof_host, "X-Proxied-By-Caddy": "true"})
        assert r2.status_code == 400
        assert r2.json()["error"] == "host_not_allowed"


@pytest.mark.parametrize("action", ["stop", "panic"])
def test_sec04_stop_and_panic_persist_sharing_disabled_across_pods_and_block_relogin(
    isolated_share_env: pathlib.Path, monkeypatch: pytest.MonkeyPatch, action: str
) -> None:
    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.include_router(share_admin.router)
    app.add_api_route("/api/dashboard", lambda: {"ok": True}, methods=["GET"])
    app.add_api_route("/api/bootstrap", lambda: {"needs_login": False}, methods=["GET"])

    mgr = tunnel.get_tunnel_manager()
    mgr.start_sharing(public_endpoint="https://share.example.com")
    assert share_db.get_sharing_enabled(bypass_cache=True) is True

    invite = share_db.create_remote_invite(
        name="Analyst",
        email="analyst@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
    )
    if tos := share_db.get_latest_tos():
        share_db.mark_tos_accepted(invite["id"], tos["version"])
    claim_tok = share_db.create_claim_token(invite["id"], ttl_hours=1)

    remote_hdrs = {
        "Host": "share.example.com",
        "Origin": "https://share.example.com",
        "X-Proxied-By-Caddy": "true",
    }

    with TestClient(app, follow_redirects=False) as c:
        login_ok = c.post(
            "/api/share/login",
            json={"email": "analyst@example.com", "passcode": "ocean-breeze-cabin-42"},
            headers=remote_hdrs,
        )
        assert login_ok.status_code == 200
        sid = login_ok.cookies.get("analyst_session_id")
        assert sid
        c.cookies.set("analyst_session_id", sid)
        assert c.get("/api/dashboard?service=svcA", headers=remote_hdrs).status_code == 200

        # Simulate a sibling K8s pod holding its own in-memory TunnelManager before Stop/Panic
        sibling_pod_mgr = tunnel.TunnelManager()
        assert sibling_pod_mgr.is_sharing_active() is True
        assert sibling_pod_mgr.validate_session(sid) is not None

        # Admin stops sharing or triggers panic
        if action == "stop":
            mgr.stop_sharing()
        else:
            mgr.panic()

        assert share_db.get_sharing_enabled(bypass_cache=True) is False
        assert sibling_pod_mgr.is_sharing_active() is False
        assert sibling_pod_mgr.validate_session(sid) is None

        # Restarted pod with stale tunnel_state.json must NOT restore sharing when share_db says disabled
        state_file = pathlib.Path(tunnel._state_file_path())
        state_file.parent.mkdir(parents=True, exist_ok=True)
        state_file.write_text('{"public_endpoint": "https://share.example.com"}', encoding="utf-8")
        restarted_mgr = tunnel.TunnelManager()
        assert restarted_mgr.is_sharing_active() is False

        # Active analyst session cookie is immediately rejected with 403 sharing_disabled
        for blocked_path in ("/api/dashboard?service=svcA", "/api/share/heartbeat", "/api/share/tos", "/api/bootstrap"):
            r_sess = c.get(blocked_path, headers=remote_hdrs)
            assert r_sess.status_code == 403, f"{blocked_path}: {r_sess.status_code} {r_sess.text}"
            assert r_sess.json()["error"] == "sharing_disabled"

        # Re-login and remote claim are rejected with 403 sharing_disabled
        c.cookies.clear()
        r_relogin = c.post(
            "/api/share/login",
            json={"email": "analyst@example.com", "passcode": "ocean-breeze-cabin-42"},
            headers=remote_hdrs,
        )
        assert r_relogin.status_code == 403
        assert r_relogin.json()["detail"]["error"] == "sharing_disabled"

        r_claim = c.post(f"/api/share/claim/{claim_tok}", headers=remote_hdrs)
        assert r_claim.status_code == 403
        assert r_claim.json()["detail"]["error"] == "sharing_disabled"

        # OAuth authorize & callback redirect to /share-login?oauth_error=sharing_disabled
        r_oauth = c.get("/api/share/oauth/authorize?provider=google", headers=remote_hdrs)
        assert r_oauth.status_code == 302
        assert r_oauth.headers["location"] == "/share-login?oauth_error=sharing_disabled"

        r_cb = c.get("/api/share/oauth/callback?code=x&state=y", headers=remote_hdrs)
        assert r_cb.status_code == 302
        assert r_cb.headers["location"] == "/share-login?oauth_error=sharing_disabled"
        _ = oauth_registry  # keep import referenced


# ── Iteration 2 Hardening Regressions (V1, V2, C4, C5, C6, C7, C9) ───────────


@pytest.mark.security_regression
def test_raw_peer_ip_captured_before_proxy_headers_middleware_without_env(
    isolated_share_env: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from backend.routers import root_system as root_system_mod

    for env_key in (
        "TRUSTED_PROXY_IPS",
        "UVICORN_FORWARDED_ALLOW_IPS",
        "FORWARDED_ALLOW_IPS",
        "ADMIN_GATEWAY_REQUIRED",
        "ADMIN_GATEWAY_SECRET",
    ):
        monkeypatch.delenv(env_key, raising=False)

    app = FastAPI()

    @app.get("/api/admin/raw-peer-probe", dependencies=[Depends(require_admin)])
    def raw_peer_probe() -> dict[str, bool]:
        return {"admin": True}

    app.include_router(root_system_mod.router)
    app.add_middleware(RemoteAccessMiddleware)
    app.add_middleware(
        ProxyHeadersMiddleware,
        trusted_hosts=["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "127.0.0.1"],
    )

    for peer_ip in ("10.244.1.9", "172.18.0.5"):
        with TestClient(app, client=(peer_ip, 54321)) as c:
            for spoof_xff in ("127.0.0.1", "::1", "::ffff:127.0.0.1", "0:0:0:0:0:0:0:1"):
                r_adm = c.get(
                    "/api/admin/raw-peer-probe",
                    headers={"Host": "localhost", "X-Forwarded-For": spoof_xff},
                )
                assert r_adm.status_code in (400, 401, 403), f"{peer_ip} + {spoof_xff} -> {r_adm.status_code}"
                assert r_adm.json().get("admin") is not True

                r_met = c.get("/metrics", headers={"Host": "localhost", "X-Forwarded-For": spoof_xff})
                assert r_met.status_code in (400, 403)

    # Genuine loopback peer (Caddyfile.local without public-gateway markers) stays local admin
    with TestClient(app, client=("127.0.0.1", 54321)) as local_c:
        r_local = local_c.get(
            "/api/admin/raw-peer-probe",
            headers={"Host": "localhost", "X-Forwarded-For": "127.0.0.1"},
        )
        assert r_local.status_code == 200
        assert r_local.json() == {"admin": True}

    # Authenticated mTLS admin-gateway from pod peer succeeds
    monkeypatch.setenv("ADMIN_GATEWAY_REQUIRED", "1")
    monkeypatch.setenv("ADMIN_GATEWAY_SECRET", SECRET)
    with TestClient(app, client=("10.244.2.5", 54321)) as gw_c:
        r_gw = gw_c.get(
            "/api/admin/raw-peer-probe",
            headers={ADMIN_GATEWAY_HEADER: SECRET, "Host": "localhost", "X-Forwarded-For": "198.51.100.7"},
        )
        assert r_gw.status_code == 200
        assert r_gw.json() == {"admin": True}


@pytest.mark.security_regression
def test_zero_m1_testclient_or_pytest_backdoors_in_backend_diff() -> None:
    proc = subprocess.run(
        ["git", "diff", "HEAD", "--", "backend/"],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=15,
        check=True,
    )
    added_lines = [line for line in proc.stdout.splitlines() if line.startswith("+") and not line.startswith("+++")]
    offenders = [
        line for line in added_lines if any(tok in line for tok in ("PYTEST_CURRENT_TEST", "testclient", "testserver"))
    ]
    assert offenders == [], f"Forbidden test-only branches found in backend diff: {offenders}"


@pytest.mark.security_regression
def test_frontend_proxy_host_localhost_and_empty_content_length_rejected(
    isolated_share_env: pathlib.Path,
) -> None:
    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.add_api_route("/api/health", lambda: {"ok": True}, methods=["GET"])
    app.add_api_route("/api/share/login", lambda: {"ok": True}, methods=["POST"])
    tunnel.get_tunnel_manager().start_sharing(public_endpoint="https://share.example.com")

    with TestClient(app, client=("127.0.0.1", 43000)) as c:
        r_host = c.get(
            "/api/health",
            headers={"Host": "localhost", "X-FLA-Frontend-Proxy": "1", "X-Remote-Analyst": "1"},
        )
        assert r_host.status_code == 400
        assert r_host.json()["error"] == "host_not_allowed"

        for bad_cl in ("", "   "):
            r_cl = c.post(
                "/api/share/login",
                content=b"{}",
                headers={
                    "Host": "share.example.com",
                    "Origin": "https://share.example.com",
                    "X-Proxied-By-Caddy": "true",
                    "Content-Type": "application/json",
                    "Content-Length": bad_cl,
                },
            )
            assert r_cl.status_code == 400
            assert r_cl.json()["error"] == "invalid_content_length"


@pytest.mark.security_regression
def test_multipod_stop_panic_zero_ms_and_no_boot_session_resurrection_without_pytest_env(
    isolated_share_env: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)

    pod_a = tunnel.TunnelManager()
    pod_b = tunnel.TunnelManager()
    assert pod_a.is_sharing_active() is False
    assert pod_b.is_sharing_active() is False

    # Start sharing on pod_a; pre-booted pod_b must immediately discover public_endpoint & sharing=True at 0ms
    pod_a.start_sharing(public_endpoint="https://share-pod.example.com")
    assert pod_b.is_sharing_active() is True
    assert pod_b.public_url(sync_db=True) == "https://share-pod.example.com"

    invite = share_db.create_remote_invite(
        name="MultiPod Analyst",
        email="multipod@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
        allow_concurrent_sessions=True,
    )
    if tos := share_db.get_latest_tos():
        share_db.mark_tos_accepted(invite["id"], tos["version"])
    inv_row = share_db.get_remote_invite(invite["id"])
    assert inv_row is not None

    ua = {"user-agent": "Mozilla/5.0 Chrome/125.0"}
    sess1 = pod_a.create_session(invite=inv_row, ip_address="198.51.100.11", user_agent=ua["user-agent"], headers=ua)
    sess2 = pod_a.create_session(invite=inv_row, ip_address="198.51.100.12", user_agent=ua["user-agent"], headers=ua)

    # pod_b rehydrates sess1 on demand; sess2 is NEVER loaded into pod_b._sessions before boot!
    assert pod_b.validate_session(sess1.session_id) is not None
    assert sess2.session_id not in pod_b._sessions

    # Boot both sessions from pod_b -> deleted in share_db, evicted on pod_a at 0ms, touch_session cannot resurrect!
    assert pod_b.boot_session(sess1.session_id, reason="cross-pod boot 1") is True
    assert pod_b.boot_session(sess2.session_id, reason="cross-pod boot 2") is True
    assert share_db.get_session(sess1.session_id) is None
    assert share_db.get_session(sess2.session_id) is None
    assert pod_a.touch_session(sess1.session_id, action="heartbeat") is False
    assert share_db.get_session(sess1.session_id) is None
    assert pod_a.validate_session(sess1.session_id) is None
    assert pod_a.validate_session(sess2.session_id) is None

    # Rejected stale analyst_session_id cookie emits Max-Age=0 Set-Cookie while preserving tos_pending cookie
    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.include_router(share_admin.router)
    app.add_api_route("/api/dashboard", lambda: {"ok": True}, methods=["GET"])

    remote_hdrs = {
        "Host": "share-pod.example.com",
        "Origin": "https://share-pod.example.com",
        "X-Proxied-By-Caddy": "true",
    }
    with TestClient(app, client=("198.51.100.11", 50000), follow_redirects=False) as c:
        r_booted = c.get(
            "/api/dashboard?service=svcA",
            headers={**remote_hdrs, "Cookie": f"analyst_session_id={sess1.session_id}; tos_pending=1"},
        )
        assert r_booted.status_code == 401
        set_cookies = " | ".join(r_booted.headers.get_list("set-cookie"))
        assert 'analyst_session_id=""' in set_cookies or "analyst_session_id=;" in set_cookies
        assert "Max-Age=0" in set_cookies and "HttpOnly" in set_cookies and "SameSite=strict" in set_cookies
        assert "tos_pending=" not in set_cookies

        # Panic on pod_a propagates to pod_b in 0ms without PYTEST_CURRENT_TEST!
        pod_a.panic()
        assert pod_b.is_sharing_active() is False
        r_login = c.post(
            "/api/share/login",
            json={"email": "multipod@example.com", "passcode": "ocean-breeze-cabin-42"},
            headers=remote_hdrs,
        )
        assert r_login.status_code == 403


@pytest.mark.security_regression
def test_sliding_window_limiter_pins_active_windows_strikes_lockouts_and_ipv6_64(
    isolated_share_env: pathlib.Path,
) -> None:
    from backend.utils.rate_limit import SlidingWindowLimiter, normalize_rate_limit_ip

    now = [1000.0]
    lim = SlidingWindowLimiter(window_s=60.0, max_keys=10_000, clock=lambda: now[0])

    # 1. Active multi-hit window survives 10,001 single-hit probe IPs
    for _ in range(599):
        assert lim.try_acquire("attacker-win", max_events=600) is True
    for i in range(10_001):
        assert lim.try_acquire(f"probe-win-{i}", max_events=600) is True
    assert lim.try_acquire("attacker-win", max_events=600) is True
    assert lim.try_acquire("attacker-win", max_events=600) is False

    # 2. Pre-lockout strikes (4 of 5) and active lockouts survive 10,001 single-hit probe IPs
    strike_lim = SlidingWindowLimiter(window_s=60.0, max_keys=10_000, clock=lambda: now[0])
    for _ in range(4):
        assert strike_lim.strike("attacker-login", threshold=5, lockout_s=300.0) is False
    for i in range(10_001):
        strike_lim.strike(f"probe-strike-{i}", threshold=5, lockout_s=300.0)
    assert strike_lim.strike("attacker-login", threshold=5, lockout_s=300.0) is True
    assert strike_lim.is_locked("attacker-login")[0] is True
    for i in range(10_001):
        strike_lim.strike(f"probe-postlock-{i}", threshold=5, lockout_s=300.0)
    assert strike_lim.is_locked("attacker-login")[0] is True

    # 3. IPv6 /64 normalization collapses /128 address rotation in TunnelManager login rate limiter
    assert normalize_rate_limit_ip("2001:db8:abcd:1234::1") == "2001:db8:abcd:1234::/64"
    assert normalize_rate_limit_ip("::ffff:198.51.100.9") == "198.51.100.9"
    mgr = tunnel.get_tunnel_manager()
    for host_idx in range(1, 6):
        mgr.record_login_failure(f"2001:db8:abcd:1234::{host_idx:x}", f"user{host_idx}@example.com")
    locked, retry = mgr.check_rate_limit("2001:db8:abcd:1234::ffff")
    assert locked is True and retry > 0


@pytest.mark.security_regression
def test_caddyfile_route_order_nonpublic_fcip_and_refresh_fastly_cidrs_compat() -> None:
    import scripts.refresh_fastly_cidrs as refresh_fastly_cidrs

    caddy_text = (REPO_ROOT / "Caddyfile").read_text(encoding="utf-8")
    assert refresh_fastly_cidrs.MATCHER_BLOCK_RE.search(caddy_text) is not None
    assert "@nonpublic_fcip" in caddy_text
    assert "route {" in caddy_text
    assert "header_up X-Forwarded-For {http.request.header.X-Forwarded-For}" in caddy_text

    pos_route = caddy_text.index("route {")
    pos_uncond_xff = caddy_text.index("request_header X-Forwarded-For {http.request.remote.host}", pos_route)
    pos_fastly_xff = caddy_text.index("request_header @from_fastly_v4 X-Forwarded-For", pos_route)
    assert pos_route < pos_uncond_xff < pos_fastly_xff

    pub_gw_txt = (REPO_ROOT / "deploy/chart/fastly-log-analytics/templates/public-gateway.yaml").read_text(
        encoding="utf-8"
    )
    assert "@nonpublic_fcip" in pub_gw_txt

    for rel in (
        "deploy/chart/fastly-log-analytics/templates/public-gateway.yaml",
        "caddy/local-public-https.caddy",
        "caddy/Caddyfile.multipod",
    ):
        txt = (REPO_ROOT / rel).read_text(encoding="utf-8")
        assert "route {" in txt, rel
        assert "?Content-Security-Policy" in txt, rel
        assert "header_up X-Forwarded-For {http.request.header.X-Forwarded-For}" in txt, rel
    local_caddy = (REPO_ROOT / "caddy/Caddyfile.local").read_text(encoding="utf-8")
    assert "request_header -X-FLA-Frontend-Proxy" in local_caddy
    assert "X-Proxied-By-Caddy" not in local_caddy.splitlines()[35:]


@pytest.mark.security_regression
def test_campaign_b3_edge_csp_parity_and_object_src_none(tmp_path: pathlib.Path) -> None:
    from backend.utils import remote_access

    # Every edge Caddyfile and K8s public-gateway template must emit fallback CSP with
    # object-src 'none', base-uri 'self', frame-ancestors 'none', and form-action 'self'.
    for rel in (
        "Caddyfile",
        "caddy/Caddyfile.local",
        "caddy/Caddyfile.multipod",
        "caddy/local-public-https.caddy",
        "deploy/chart/fastly-log-analytics/templates/public-gateway.yaml",
    ):
        txt = (REPO_ROOT / rel).read_text(encoding="utf-8")
        assert "?Content-Security-Policy" in txt, rel
        csp_lines = [line for line in txt.splitlines() if "?Content-Security-Policy" in line]
        assert csp_lines, rel
        for line in csp_lines:
            for directive in (
                "object-src 'none'",
                "base-uri 'self'",
                "frame-ancestors 'none'",
                "form-action 'self'",
            ):
                assert directive in line, f"{rel} missing {directive}: {line}"
        assert 'X-Content-Type-Options "nosniff"' in txt, rel
        assert 'X-Frame-Options "DENY"' in txt, rel
        assert 'Cross-Origin-Opener-Policy "same-origin"' in txt, rel
        assert "-Server" in txt, rel

    # Local plain-HTTP Caddyfile.local must NOT emit Strict-Transport-Security on :80
    local_caddy = (REPO_ROOT / "caddy/Caddyfile.local").read_text(encoding="utf-8")
    assert "Strict-Transport-Security" not in local_caddy

    # Backend HTML CSP fallback must also include object-src 'none'
    for directive in (
        "object-src 'none'",
        "base-uri 'self'",
        "frame-ancestors 'none'",
        "form-action 'self'",
    ):
        assert directive in remote_access._HTML_CSP_DEFAULT, directive

    # Validate Caddy syntax if caddy binary is installed
    caddy_bin = shutil.which("caddy")
    if caddy_bin:
        for rel in ("caddy/Caddyfile.local", "caddy/Caddyfile.multipod"):
            proc = subprocess.run(
                [caddy_bin, "adapt", "--config", str(REPO_ROOT / rel), "--adapter", "caddyfile"],
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
            assert proc.returncode == 0, f"{rel}: {proc.stderr}"


def test_sec22_caddy_gateways_strip_internal_headers_and_dynamic_security_txt_canonical() -> None:
    stripped_headers = (
        "-X-Service-Id",
        "-X-FOS-Bucket",
        "-X-FOS-Bucket-Name",
        "-X-Upstream-Host",
        "-X-Backend-Host",
        "-X-Served-By",
        "-X-Edgecompute-Host",
    )
    for rel in (
        "Caddyfile",
        "deploy/chart/fastly-log-analytics/templates/public-gateway.yaml",
        "caddy/Caddyfile.multipod",
        "caddy/Caddyfile.local",
    ):
        txt = (REPO_ROOT / rel).read_text(encoding="utf-8")
        for directive in stripped_headers:
            assert directive in txt, f"{rel} missing {directive}"

    caddyfile = (REPO_ROOT / "Caddyfile").read_text(encoding="utf-8")
    assert "Canonical: https://{http.request.host}/.well-known/security.txt" in caddyfile
    assert "Canonical: https://fastly-log-analytics.global.ssl.fastly.net/.well-known/security.txt" not in caddyfile
