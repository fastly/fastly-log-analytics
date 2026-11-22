"""M1 Security Regression Suite — SEC-09 and SEC-10."""

from __future__ import annotations

import hashlib
import logging
import shutil
import sqlite3
import subprocess
import urllib.error
import urllib.request
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

import backend.config as svcconfig
from backend.core import request_context, share_db
from backend.routers import dashboard as dashboard_mod
from backend.routers import web_vitals as web_vitals_mod
from backend.routers.admin import _helpers as admin_helpers
from backend.routers.admin import _router as admin_router_mod
from backend.routers.admin import admin_queries as admin_queries_mod
from backend.routers.admin import sync_status as sync_status_mod
from backend.routers.share_admin import invites as invites_mod
from backend.routers.share_admin import sessions_and_tos as sessions_mod
from backend.utils import remote_access, tunnel

pytestmark = pytest.mark.no_postgres

REPO_ROOT = Path(__file__).resolve().parents[2]
CHART_DIR = REPO_ROOT / "deploy" / "chart" / "fastly-log-analytics"


@pytest.fixture(autouse=True)
def _isolated_sec09_10_state(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("REMOTE_SHARE_DB_DIR", str(tmp_path / "system"))
    monkeypatch.setattr(svcconfig, "DATA_DIR", data_dir)
    monkeypatch.delenv("ADMIN_GATEWAY_SECRET", raising=False)
    monkeypatch.delenv("ADMIN_GATEWAY_REQUIRED", raising=False)
    monkeypatch.delenv("SESSION_TOKEN_SECRET", raising=False)
    monkeypatch.delenv("HIGH_SCALE_CURSOR_SECRET", raising=False)
    monkeypatch.delenv("CURSOR_SECRET", raising=False)
    con = sqlite3.connect(str(tmp_path / "share_sec09_10.db"), check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON")
    for ddl in share_db._SCHEMA:
        con.execute(ddl)
    con.commit()
    monkeypatch.setattr(share_db, "get_global_share_con", lambda: con)
    share_db.publish_tos_version("v1", "Test Terms of Service", con=con)
    tunnel.reset_for_tests()
    if hasattr(web_vitals_mod, "_reset_web_vitals_limiter_for_tests"):
        web_vitals_mod._reset_web_vitals_limiter_for_tests()
    request_context._key = None
    yield
    request_context._key = None
    tunnel.reset_for_tests()
    con.close()


# ── SEC-09.1: CSV formula neutralization & filename sanitization ────────────


def test_sec09_dashboard_and_usage_csv_neutralize_formulas_while_preserving_signed_numbers(monkeypatch):
    df = pd.DataFrame(
        [
            {
                "timestamp": "2026-11-19T00:00:00Z",
                "url": "=1+1",
                "ua": "  +cmd|' /C calc'!A0",
                "referer": "-1+1",
                "city": "@SUM(A1:A2)",
                "tab_col": "\t=evil()",
                "cr_col": "\rcmd",
                "signed_str": "-12.5",
                "pos_str": "+5",
                "exp_str": "-1e309",
                "latency_delta": -42.5,
                "=injected_col": "ok",
            }
        ]
    )
    monkeypatch.setattr(dashboard_mod.hs_dashboard, "raw_df", lambda *a, **kw: df)
    fake_ctx = SimpleNamespace(
        clamp=lambda s, e: (s, e),
        analyst_session=None,
        service_id='svc"evil\r\nname',
        high_scale_service=SimpleNamespace(service_id='svc"evil\r\nname'),
    )
    monkeypatch.setattr(dashboard_mod, "require_high_scale", lambda ctx: ctx.high_scale_service)

    app = FastAPI()
    app.dependency_overrides[request_context.build_request_context] = lambda: fake_ctx
    app.include_router(dashboard_mod.router)

    with TestClient(app) as client:
        resp = client.post("/api/dashboard/raw/csv", json={})
        assert resp.status_code == 200
        disp = resp.headers.get("content-disposition", "")
        assert "\r" not in disp and "\n" not in disp
        assert disp.startswith('attachment; filename="logs_')
        csv_text = resp.text
        assert "'=injected_col" in csv_text
        assert "'=1+1" in csv_text
        assert "'  +cmd|" in csv_text
        assert "'-1+1" in csv_text
        assert "'@SUM(A1:A2)" in csv_text
        assert "'\t=evil()" in csv_text
        assert "'\rcmd" in csv_text
        assert "'-1e309" in csv_text
        # Clean signed decimals and numeric dtypes stay unprefixed!
        assert ",-12.5," in csv_text
        assert ",+5," in csv_text
        assert ",-42.5," in csv_text

    # Verify admin usage-log CSV uses identical sanitizer (including trailing-newline rejection)
    assert admin_queries_mod._sanitize_csv_cell("\tcmd") == "'\tcmd"
    assert admin_queries_mod._sanitize_csv_cell("-12.5") == "-12.5"
    assert admin_queries_mod._sanitize_csv_cell("-1+1") == "'-1+1"
    assert admin_queries_mod._sanitize_csv_cell("-12.5\n=cmd") == "'-12.5\n=cmd"
    assert dashboard_mod._sanitize_csv_cell("-12.5\n=cmd") == "'-12.5\n=cmd"


# ── SEC-09.2: Content-Length validation & 4 MiB body cap + SEC-10.5 CSP ─────


def test_sec09_and_sec10_middleware_content_length_stream_cap_and_api_csp():
    mgr = tunnel.get_tunnel_manager()
    mgr.start_sharing(public_endpoint="https://share.example.com")

    app = FastAPI()
    app.add_middleware(remote_access.RemoteAccessMiddleware)

    @app.get("/api/health")
    def api_health():
        return {"ok": True}

    @app.get("/js/rum.js")
    def rum_js():
        return PlainTextResponse("window.__RUM=1;", media_type="application/javascript")

    @app.post("/api/share/login")
    async def echo_login(request: Request):
        body = await request.body()
        return {"size": len(body)}

    with TestClient(app, base_url="https://share.example.com") as client:
        # SEC-10.5: /api/* gets default-src 'none', non-/api path gets HTML/script-src fallback CSP
        r_api = client.get("/api/health", headers={"X-Proxied-By-Caddy": "true"})
        assert r_api.status_code == 200
        assert r_api.headers["Content-Security-Policy"] == "default-src 'none'; frame-ancestors 'none'"

        r_non_api = client.get("/js/rum.js", headers={"X-Proxied-By-Caddy": "true"})
        assert r_non_api.status_code == 200
        assert "script-src" in r_non_api.headers["Content-Security-Policy"]

        # Direct call without path preserves backwards-compatible HTML CSP (test_oauth.py:1736)
        bare_resp = PlainTextResponse("ok")
        remote_access.apply_response_hardening(bare_resp)
        assert "script-src" in bare_resp.headers["Content-Security-Policy"]

        # SEC-09.2: malformed & negative Content-Length -> 400 with API CSP
        for bad_cl in ("abc", "-5"):
            r_cl = client.post(
                "/api/share/login",
                content=b"{}",
                headers={
                    "X-Proxied-By-Caddy": "true",
                    "Origin": "https://share.example.com",
                    "Content-Type": "application/json",
                    "Content-Length": bad_cl,
                },
            )
            assert r_cl.status_code == 400
            assert r_cl.json()["error"] == "invalid_content_length"
            assert r_cl.headers["Content-Security-Policy"] == "default-src 'none'; frame-ancestors 'none'"

        # SEC-09.2: > 4 MiB payload -> 413 payload_too_large on both remote AND local callers
        oversized = b"x" * (remote_access.BODY_INSPECT_MAX_BYTES + 1024)
        r_big_remote = client.post(
            "/api/share/login",
            content=oversized,
            headers={
                "X-Proxied-By-Caddy": "true",
                "Origin": "https://share.example.com",
                "Content-Type": "application/octet-stream",
            },
        )
        assert r_big_remote.status_code == 413
        assert r_big_remote.json()["error"] == "payload_too_large"

    with TestClient(app) as local_client:
        r_big_local = local_client.post(
            "/api/share/login",
            content=b"y" * (remote_access.BODY_INSPECT_MAX_BYTES + 512),
            headers={"Content-Type": "application/octet-stream"},
        )
        assert r_big_local.status_code == 413

        # Chunked streaming > 4 MiB without Content-Length -> 413
        def _big_chunks():
            for _ in range(5):
                yield b"z" * (1024 * 1024)

        r_chunked = local_client.post(
            "/api/share/login",
            content=_big_chunks(),
            headers={"Content-Type": "application/json"},
        )
        assert r_chunked.status_code == 413


# ── SEC-09.3: /api/download* auth, root/prefix/key traversal & Safe CDN ─────


def test_sec09_downloads_enforce_admin_auth_traversal_root_allowlist_and_safe_cdn(tmp_path):
    cache_root = tmp_path / "cache_svc"
    (cache_root / "raw").mkdir(parents=True)
    (cache_root / "raw" / "ok.log").write_bytes(b"ok-log-bytes")
    (cache_root / "secret.json").write_bytes(b"top-level-secret")

    source_cfg: dict = {
        "name": 'svc"bad\r\n',
        "service_id": "svc-dl",
        "bucket": "bkt",
        "prefix": "",
        "cdn_url": "",
    }

    app = FastAPI()
    app.dependency_overrides[request_context.get_source] = lambda: source_cfg
    app.include_router(admin_router_mod.router)

    with (
        patch("backend.core.fos_client._cache_dir", return_value=str(cache_root)),
        TestClient(app) as client,
    ):
        # 1. Remote / analyst caller rejected on all download endpoints -> 403 admin_only
        for ep in (
            "/api/download?key=raw/ok.log",
            "/api/download-folder?root=raw",
            "/api/download-all",
            "/api/admin/raw-tree",
        ):
            r_rem = client.get(ep, headers={"X-Proxied-By-Caddy": "true"})
            assert r_rem.status_code == 403, ep
            assert r_rem.json()["detail"]["error"] == "admin_only"

        # 2. download_folder root allowlist & prefix traversal rejection
        assert client.get("/api/download-folder?root=evil").status_code == 400
        assert client.get("/api/download-folder?root=raw&prefix=../etc").status_code == 400
        assert client.get("/api/download-folder?root=raw&prefix=a\\b").status_code == 400

        # 3. download_file requires raw/ or processed/ when source prefix is empty, blocks ..
        assert client.get("/api/download?key=secret.json").status_code == 400
        assert client.get("/api/download?key=raw/../secret.json").status_code == 400
        r_ok = client.get("/api/download?key=raw/ok.log")
        assert r_ok.status_code == 200
        assert r_ok.content == b"ok-log-bytes"

        # 4. SSRF & non-Fastly CDN URL rejected with 400 invalid_cdn_url
        for bad_cdn in ("http://169.254.169.254/latest", "https://evil.example"):
            source_cfg["cdn_url"] = bad_cdn
            r_ssrf = client.get("/api/download?key=raw/missing.log")
            assert r_ssrf.status_code == 400
            assert r_ssrf.json()["detail"]["error"] == "invalid_cdn_url"

        # 5. SafeRedirectHandler blocks 302 open-redirect SSRF
        handler = admin_helpers.SafeRedirectHandler()
        dummy_req = urllib.request.Request("https://mycdn.global.ssl.fastly.net/raw/missing.log")
        with pytest.raises(urllib.error.URLError, match="redirects are not allowed"):
            handler.redirect_request(dummy_req, MagicMock(), 302, "Found", {}, "http://127.0.0.1:8000/admin")


# ── SEC-10.1: Admin router Depends(require_admin) + /api/sync-status order ──


def test_sec10_all_admin_router_endpoints_require_admin_while_preserving_sync_status_scope_error():
    api_routes = [r for r in admin_router_mod.router.routes if isinstance(r, APIRoute)]
    assert len(api_routes) >= 35
    for route in api_routes:
        dep_calls = [d.call for d in route.dependant.dependencies]
        assert request_context.require_admin in dep_calls, f"{route.path} missing require_admin"

    # Verify /api/sync-status returns service_not_authorized for out-of-scope analyst (test_metrics_and_telemetry contract)
    # AND returns admin_only for in-scope analyst!
    app = FastAPI()
    app.include_router(sync_status_mod.router)

    @app.middleware("http")
    async def _inject_analyst(request: Request, call_next):
        if request.headers.get("x-test-analyst") == "1":
            request.state.analyst_session = SimpleNamespace(service_ids=["svc-a"])
        return await call_next(request)

    with TestClient(app) as client:
        r_wrong_svc = client.get("/api/sync-status?service_id=svc-b", headers={"x-test-analyst": "1"})
        assert r_wrong_svc.status_code == 403
        assert r_wrong_svc.json()["detail"]["error"] == "service_not_authorized"

        r_in_scope = client.get("/api/sync-status?service_id=svc-a", headers={"x-test-analyst": "1"})
        assert r_in_scope.status_code == 403
        assert r_in_scope.json()["detail"]["error"] == "admin_only"


# ── SEC-10.2: Share claim & OAuth per-IP rate limits ────────────────────────


def test_sec10_share_claim_and_oauth_endpoints_rate_limit_per_ip():
    mgr = tunnel.get_tunnel_manager()
    mgr.start_sharing(public_endpoint="https://share.example.com")

    app = FastAPI()
    app.include_router(sessions_mod.share_auth_router)
    app.include_router(invites_mod.share_oauth_router)

    with TestClient(app, client=("198.51.100.88", 50000)) as client:
        # 10 claim attempts return 404/400; 11th returns 429 with Retry-After
        statuses = [client.post(f"/api/share/claim/nonexistent-token-{i}").status_code for i in range(11)]
        assert statuses[:10] == [404] * 10
        r_claim_429 = client.post("/api/share/claim/nonexistent-token-11")
        assert statuses[10] == 429 and r_claim_429.status_code == 429
        assert "Retry-After" in r_claim_429.headers

        # 20 OAuth authorize attempts return 302 redirect; 21st returns 429 with Retry-After (shared with callback)
        oauth_codes = [
            client.get("/api/share/oauth/authorize?provider=google", follow_redirects=False).status_code
            for _ in range(20)
        ]
        assert oauth_codes == [302] * 20
        r_oauth_429 = client.get("/api/share/oauth/authorize?provider=google", follow_redirects=False)
        assert r_oauth_429.status_code == 429
        assert "Retry-After" in r_oauth_429.headers
        r_cb_429 = client.get("/api/share/oauth/callback?code=x&state=y", follow_redirects=False)
        assert r_cb_429.status_code == 429
        assert "Retry-After" in r_cb_429.headers


# ── SEC-10.4: SESSION_TOKEN_SECRET domain-separated fallback & Helm wiring ──


def test_sec10_session_token_secret_fallback_hierarchy_and_helm_template(monkeypatch, caplog):
    # 1. Explicit SESSION_TOKEN_SECRET wins
    request_context._key = None
    monkeypatch.setenv("SESSION_TOKEN_SECRET", "explicit-session-secret")
    monkeypatch.setenv("HIGH_SCALE_CURSOR_SECRET", "cursor-secret-abc")
    k_explicit = request_context._resolve_key()
    assert k_explicit == hashlib.sha256(b"explicit-session-secret").digest()

    # 2. Fallback to domain-separated HIGH_SCALE_CURSOR_SECRET (distinct from raw SHA256!)
    request_context._key = None
    monkeypatch.delenv("SESSION_TOKEN_SECRET", raising=False)
    k_cursor = request_context._resolve_key()
    assert k_cursor == hashlib.sha256(b"fla-session-token-v1:cursor-secret-abc").digest()
    assert k_cursor != hashlib.sha256(b"cursor-secret-abc").digest()

    # 3. Missing all secrets logs warning and generates 32 random bytes
    request_context._key = None
    monkeypatch.delenv("HIGH_SCALE_CURSOR_SECRET", raising=False)
    with caplog.at_level(logging.WARNING):
        k_rand = request_context._resolve_key()
    assert len(k_rand) == 32
    assert any("SESSION_TOKEN_SECRET" in rec.message for rec in caplog.records)

    # 4. Helm chart renders SESSION_TOKEN_SECRET with optional: true
    if shutil.which("helm"):
        proc = subprocess.run(
            [
                "helm",
                "template",
                "fla-test",
                str(CHART_DIR),
                "--set",
                "secrets.cursorSecret=cursor-secret-32-characters-long!!",
                "--set",
                "secrets.sessionTokenSecret=dedicated-session-secret-32-chars!",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        assert 'SESSION_TOKEN_SECRET: "dedicated-session-secret-32-chars!"' in proc.stdout
        assert "key: SESSION_TOKEN_SECRET" in proc.stdout
        assert "optional: true" in proc.stdout


# ── SEC-10.6: POST /api/web-vitals session/same-origin guard & rate limit ───


def test_sec10_web_vitals_rejects_cross_origin_remote_and_rate_limits(monkeypatch):
    mgr = tunnel.get_tunnel_manager()
    mgr.start_sharing(public_endpoint="https://share.example.com")

    app = FastAPI()
    app.include_router(web_vitals_mod.router)

    sample = {"name": "LCP", "value": 120.0, "rating": "good", "id": "v1", "pathname": "/", "delta": 10.0}

    with TestClient(app, client=("203.0.113.50", 50000), base_url="https://share.example.com") as remote_client:
        # Missing Origin/Referer/Sec-Fetch-Site on unauth remote -> 403
        r_no_origin = remote_client.post("/api/web-vitals", json=sample, headers={"X-Proxied-By-Caddy": "true"})
        assert r_no_origin.status_code == 403

        # Cross-origin Origin -> 403
        r_evil = remote_client.post(
            "/api/web-vitals",
            json=sample,
            headers={"X-Proxied-By-Caddy": "true", "Origin": "https://evil.example"},
        )
        assert r_evil.status_code == 403

        # Sec-Fetch-Site: cross-site -> 403 even with Origin
        r_sfs = remote_client.post(
            "/api/web-vitals",
            json=sample,
            headers={
                "X-Proxied-By-Caddy": "true",
                "Origin": "https://share.example.com",
                "Sec-Fetch-Site": "cross-site",
            },
        )
        assert r_sfs.status_code == 403

        # Valid same-origin Origin -> 200 up to WEB_VITALS_RATE_LIMIT, then 429!
        for _ in range(web_vitals_mod.WEB_VITALS_RATE_LIMIT):
            r_ok = remote_client.post(
                "/api/web-vitals",
                json=sample,
                headers={"X-Proxied-By-Caddy": "true", "Origin": "https://share.example.com"},
            )
            assert r_ok.status_code == 200
        r_429 = remote_client.post(
            "/api/web-vitals",
            json=sample,
            headers={"X-Proxied-By-Caddy": "true", "Origin": "https://share.example.com"},
        )
        assert r_429.status_code == 429
        assert "Retry-After" in r_429.headers

    # Local admin TestClient without Origin still works -> 200
    web_vitals_mod._reset_web_vitals_limiter_for_tests()
    with TestClient(app) as local_client:
        assert local_client.post("/api/web-vitals", json=sample).status_code == 200


# ── SEC-09.2 regression: POST EventSourceResponse must not abort after body drain ──


from pydantic import BaseModel as _PydanticBaseModel


class _SsePostPayload(_PydanticBaseModel):
    service_id: str
    steps: int = 3


def test_sec09_post_sse_event_source_streams_all_events_without_premature_disconnect():
    import asyncio

    from sse_starlette.sse import EventSourceResponse

    app = FastAPI()
    app.add_middleware(remote_access.RemoteAccessMiddleware)

    @app.post("/api/provision/reconcile")
    async def post_sse_endpoint(payload: _SsePostPayload):
        async def _gen():
            for idx in range(payload.steps):
                await asyncio.sleep(0.005)
                yield {"event": "step", "data": f"{payload.service_id}:{idx}"}
            yield {"event": "done", "data": "complete"}

        return EventSourceResponse(_gen())

    with TestClient(app) as client:
        resp = client.post("/api/provision/reconcile", json={"service_id": "svc-sse", "steps": 3})
        assert resp.status_code == 200
        text = resp.text
        assert "svc-sse:0" in text
        assert "svc-sse:1" in text
        assert "svc-sse:2" in text
        assert "event: done" in text
        assert "data: complete" in text


# ── Iteration 2 Hardening Regressions (V3, V4, C1, C8, F1.2, M8) ─────────────


@pytest.mark.security_regression
def test_sec09_csv_unicode_control_zero_width_and_percent_pipe_neutralization() -> None:
    dangerous_payloads = [
        "\u00a0=1",
        "\x0b=1",
        "\x0c=1",
        "\u2000=1",
        "\u3000+1",
        "\ufeff=1",
        "\u200b@1",
        "\x00=1",
        "\x1f-1",
        "%1",
        "|cmd",
        "-\uff11\uff12",
        "+\uff15",
        "+1\t=cmd()",
    ]
    for p in dangerous_payloads:
        assert dashboard_mod._sanitize_csv_cell(p) == f"'{p}", repr(p)
        assert admin_queries_mod._sanitize_csv_cell(p) == f"'{p}", repr(p)

    clean_numbers = ["-12.5", "-0.004", "-42", "+12.5", ".5", "-.5"]
    for num in clean_numbers:
        assert dashboard_mod._sanitize_csv_cell(num) == num, repr(num)
        assert admin_queries_mod._sanitize_csv_cell(num) == num, repr(num)


@pytest.mark.security_regression
def test_sec09_real_tcp_wire_302_redirect_blocked_and_download_all_scoped(tmp_path: Path) -> None:
    import http.server
    import io
    import threading
    import zipfile

    hits = {"redir": 0, "target": 0, "leaked_secret": False}

    class _TargetHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            hits["target"] += 1
            if self.headers.get("x-fastly-key"):
                hits["leaked_secret"] = True
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"SSRF_SECRET_TARGET")

        def log_message(self, *args: object) -> None:
            return

    t_srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _TargetHandler)
    t_port = t_srv.server_address[1]

    class _RedirectHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            hits["redir"] += 1
            self.send_response(302)
            self.send_header("Location", f"http://127.0.0.1:{t_port}/admin")
            self.end_headers()

        def log_message(self, *args: object) -> None:
            return

    r_srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _RedirectHandler)
    r_port = r_srv.server_address[1]

    t_thread = threading.Thread(target=t_srv.serve_forever, daemon=True)
    r_thread = threading.Thread(target=r_srv.serve_forever, daemon=True)
    t_thread.start()
    r_thread.start()

    try:
        assert urllib.request.urlopen is admin_helpers._STDLIB_URLOPEN

        # 1. Direct _open_safe_cdn over real TCP wire rejects 302 without touching target
        wire_req = urllib.request.Request(f"http://127.0.0.1:{r_port}/raw/log.gz")
        with pytest.raises(urllib.error.URLError, match="redirects are not allowed"):
            admin_helpers._open_safe_cdn(wire_req, timeout=5.0)
        assert hits == {"redir": 1, "target": 0, "leaked_secret": False}

        # 2. _validated_admin_cdn_url rejects userinfo, non-443 ports, query, fragment, and .. segments
        for bad_cdn in (
            "https://user:pass@legit.global.ssl.fastly.net",
            "https://legit.global.ssl.fastly.net:9443/x",
            "https://legit.global.ssl.fastly.net?tok=1",
            "https://legit.global.ssl.fastly.net#frag",
            "https://legit.global.ssl.fastly.net/../admin",
        ):
            assert admin_helpers._validated_admin_cdn_url(bad_cdn) is None, bad_cdn
        assert (
            admin_helpers._validated_admin_cdn_url("https://legit.global.ssl.fastly.net")
            == "https://legit.global.ssl.fastly.net"
        )

        # 3. Full route /api/download & /api/download-folder over real 302 wire (keeping _STDLIB_URLOPEN intact!)
        cache_dir = tmp_path / "dl_cache"
        (cache_dir / "raw").mkdir(parents=True)
        (cache_dir / "raw" / "good.log").write_bytes(b"GOOD_RAW_LOG")
        (cache_dir / "top_secret.json").write_bytes(b"TOP_LEVEL_SECRET")
        outside_secret = tmp_path / "outside_secret.txt"
        outside_secret.write_bytes(b"OUTSIDE_SECRET")
        (cache_dir / "raw" / "evil_link.log").symlink_to(outside_secret)

        src_cfg = {
            "name": "svc-wire",
            "service_id": "svc-wire",
            "bucket": "wire-bkt",
            "prefix": "",
            "cdn_url": "https://mycdn.global.ssl.fastly.net",
            "cdn_secret": "SUPER_SECRET_FASTLY_KEY",
        }

        def _forward_https_to_local_http(self_handler: urllib.request.HTTPSHandler, req: urllib.request.Request):
            proxy_req = urllib.request.Request(
                f"http://127.0.0.1:{r_port}{urllib.parse.urlsplit(req.full_url).path}",
                headers=dict(req.header_items()),
                method=req.get_method(),
            )
            proxy_req.timeout = getattr(req, "timeout", 5.0)  # type: ignore[attr-defined]
            return urllib.request.HTTPHandler().http_open(proxy_req)

        seen_prefixes: list[str] = []

        class _FakePaginator:
            def paginate(self, *, Bucket: str, Prefix: str):  # noqa: N803
                seen_prefixes.append(Prefix)
                if Prefix in ("raw/", "raw/sub/"):
                    return [
                        {
                            "Contents": [
                                {"Key": "../../etc/evil_zipslip.txt"},
                                {"Key": "other/secret.json"},
                                {"Key": f"{Prefix}good.log"},
                            ]
                        }
                    ]
                return []

        fake_fos = MagicMock()
        fake_fos.get_paginator.return_value = _FakePaginator()
        fake_fos.get_object.side_effect = lambda Bucket, Key: {  # noqa: N803
            "Body": io.BytesIO(f"FOS:{Key}".encode())
        }

        app = FastAPI()
        app.dependency_overrides[request_context.get_source] = lambda: src_cfg
        app.include_router(admin_router_mod.router)

        with (
            patch("backend.core.fos_client._cache_dir", return_value=str(cache_dir)),
            patch("backend.core.fos_client._get_fos_client", return_value=fake_fos),
            patch.object(urllib.request.HTTPSHandler, "https_open", _forward_https_to_local_http),
            TestClient(app) as client,
        ):
            assert urllib.request.urlopen is admin_helpers._STDLIB_URLOPEN

            # Symlink inside cache_dir/raw -> 400 invalid_key
            r_sym = client.get("/api/download", params={"key": "raw/evil_link.log"})
            assert r_sym.status_code == 400
            assert r_sym.json()["detail"]["error"] == "invalid_key"

            # CDN 302 on /api/download -> 502 cdn_fetch_failed, target never hit, secret never leaked
            r_dl_302 = client.get("/api/download", params={"key": "raw/missing.log"})
            assert r_dl_302.status_code == 502
            assert r_dl_302.json()["detail"]["error"] == "cdn_fetch_failed"
            assert hits["target"] == 0 and hits["leaked_secret"] is False

            # /api/download-all?include=local -> scopes strictly to raw/ & processed/, ignores top_secret.json & symlink
            r_local_all = client.get("/api/download-all", params={"include": "local"})
            assert r_local_all.status_code == 200
            with zipfile.ZipFile(io.BytesIO(r_local_all.content)) as zf_loc:
                assert zf_loc.namelist() == ["raw/good.log"]

            # /api/download-all?include=all -> lists ONLY ("raw/", "processed/"), skips Zip-Slip, falls back to FOS on 302
            seen_prefixes.clear()
            r_cloud_all = client.get("/api/download-all", params={"include": "all"})
            assert r_cloud_all.status_code == 200
            assert seen_prefixes == ["raw/", "processed/"]
            with zipfile.ZipFile(io.BytesIO(r_cloud_all.content)) as zf_cld:
                assert zf_cld.namelist() == ["raw/good.log"]
                assert zf_cld.read("raw/good.log") == b"FOS:raw/good.log"
            assert hits["target"] == 0 and hits["leaked_secret"] is False
    finally:
        r_srv.shutdown()
        t_srv.shutdown()
        r_srv.server_close()
        t_srv.server_close()


@pytest.mark.security_regression
def test_sec10_active_service_cookie_and_get_service_id_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    from starlette.requests import Request as StarletteRequest

    monkeypatch.setattr(
        svcconfig,
        "load_config",
        lambda sid: {"name": sid, "service_id": sid} if sid in ("valid-svc_01", "fallback-svc") else None,
    )
    monkeypatch.setattr(svcconfig, "get_active_service_id", lambda: "fallback-svc")
    monkeypatch.setattr(svcconfig, "get_cdn_service_id_map", lambda: {})

    def _mk_req(*, query: str = "", cookie: str = "") -> StarletteRequest:
        hdrs = [(b"cookie", cookie.encode("latin-1"))] if cookie else []
        return StarletteRequest(
            {
                "type": "http",
                "method": "GET",
                "path": "/api/dashboard",
                "query_string": query.encode("latin-1"),
                "headers": hdrs,
                "client": ("127.0.0.1", 12345),
                "server": ("127.0.0.1", 8000),
                "scheme": "http",
            }
        )

    # Valid ACTIVE_SERVICE_COOKIE resolves valid-svc_01 over fallback-svc
    assert (
        request_context.get_service_id(_mk_req(cookie=f"{request_context.ACTIVE_SERVICE_COOKIE}=valid-svc_01"))
        == "valid-svc_01"
    )

    # Malformed cookies (traversal, newline, >64 chars) are ignored and fall back safely
    for bad_cookie in ("../../etc/passwd", "svc%0ainjected", "a" * 65):
        assert (
            request_context.get_service_id(_mk_req(cookie=f"{request_context.ACTIVE_SERVICE_COOKIE}={bad_cookie}"))
            == "fallback-svc"
        )

    # Explicit invalid query ?service_id=../../etc/passwd returns None (never leaks fallback-svc!)
    assert request_context.get_service_id(_mk_req(query="service_id=../../etc/passwd")) is None

    app = FastAPI()

    @app.get("/api/ctx-probe")
    def ctx_probe(
        ctx: request_context.RequestContext = pytest.importorskip("fastapi").Depends(
            request_context.build_request_context
        ),
    ):
        return {"service_id": ctx.service_id}

    monkeypatch.setattr(
        request_context.db,
        "get_source_for_service",
        lambda sid: {"name": sid, "service_id": sid} if sid in ("valid-svc_01", "fallback-svc") else None,
    )
    with TestClient(app) as c:
        r_bad_q = c.get("/api/ctx-probe?service_id=../../etc/passwd")
        assert r_bad_q.status_code == 400
        assert r_bad_q.json()["detail"]["no_service"] is True

        r_good_cookie = c.get(
            "/api/ctx-probe",
            headers={"Cookie": f"{request_context.ACTIVE_SERVICE_COOKIE}=valid-svc_01"},
        )
        assert r_good_cookie.status_code == 200
        assert r_good_cookie.json() == {"service_id": "valid-svc_01"}


@pytest.mark.security_regression
def test_sec10_web_vitals_strict_origin_cohort_attribution_and_ipv6_churn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import json

    from backend.core import request_telemetry as web_vitals_store

    sink = tmp_path / "web_vitals.jsonl"
    monkeypatch.setattr(web_vitals_store, "LOG_PATH", sink)
    monkeypatch.setattr(web_vitals_store, "collection_enabled", lambda: True)
    web_vitals_mod._reset_web_vitals_limiter_for_tests()

    mgr = tunnel.get_tunnel_manager()
    mgr.start_sharing(public_endpoint="https://share.example.com")
    invite = share_db.create_remote_invite(
        name="Vitals Analyst",
        email="vitals@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svc-a"],
    )
    if tos := share_db.get_latest_tos():
        share_db.mark_tos_accepted(invite["id"], tos["version"])
    inv_row = share_db.get_remote_invite(invite["id"])
    assert inv_row is not None
    ua_str = "Mozilla/5.0 Chrome/125.0"
    sess = mgr.create_session(
        invite=inv_row, ip_address="198.51.100.77", user_agent=ua_str, headers={"user-agent": ua_str}
    )

    app = FastAPI()
    app.include_router(web_vitals_mod.router)
    sample = {"name": "LCP", "value": 120.0, "rating": "good", "id": "v-iter2", "pathname": "/", "delta": 5.0}

    # 1. Local caller with spoofed prefix/userinfo/path Origin -> 403
    with TestClient(app, client=("127.0.0.1", 50000)) as local_c:
        for spoof_origin in (
            "https://localhost.evil.com",
            "http://127.0.0.1.evil.com",
            "http://user@localhost:3000",
            "http://localhost:3000/extra",
        ):
            r_spoof = local_c.post("/api/web-vitals", json=sample, headers={"Origin": spoof_origin})
            assert r_spoof.status_code == 403, spoof_origin

        # Genuine local admin beacon records cohort == "admin"
        r_admin = local_c.post("/api/web-vitals", json=sample, headers={"Origin": "http://localhost:3000"})
        assert r_admin.status_code == 200

    # 2. Remote unauthenticated Sec-Fetch-Site: same-origin WITHOUT Origin/Referer -> 403!
    with TestClient(app, client=("198.51.100.77", 50000), base_url="https://share.example.com") as rem_c:
        r_sfs_only = rem_c.post(
            "/api/web-vitals",
            json=sample,
            headers={"X-Proxied-By-Caddy": "true", "Sec-Fetch-Site": "same-origin"},
        )
        assert r_sfs_only.status_code == 403

        # Remote unauthenticated WITH valid Origin -> 200 and cohort == "analyst" (never pollutes "admin"!)
        r_rem_unauth = rem_c.post(
            "/api/web-vitals",
            json={**sample, "id": "v-rem-unauth"},
            headers={"X-Proxied-By-Caddy": "true", "Origin": "https://share.example.com"},
        )
        assert r_rem_unauth.status_code == 200

        # Remote with valid analyst_session_id cookie -> 200 and cohort == "analyst" without bumping idle timer
        before_active = sess.last_active_time
        r_rem_auth = rem_c.post(
            "/api/web-vitals",
            json={**sample, "id": "v-rem-auth"},
            headers={
                "X-Proxied-By-Caddy": "true",
                "Origin": "https://share.example.com",
                "User-Agent": ua_str,
                "Cookie": f"analyst_session_id={sess.session_id}",
            },
        )
        assert r_rem_auth.status_code == 200
        assert sess.last_active_time == before_active

    records = [json.loads(line) for line in sink.read_text(encoding="utf-8").splitlines() if line.strip()]
    by_id = {r["id"]: r["cohort"] for r in records}
    assert by_id == {"v-iter2": "admin", "v-rem-unauth": "analyst", "v-rem-auth": "analyst"}

    # 3. IPv6 /64 rotation + >4,096 single-hit IP churn cannot evict saturated /64 subnet!
    web_vitals_mod._reset_web_vitals_limiter_for_tests()
    for i in range(web_vitals_mod.WEB_VITALS_RATE_LIMIT):
        with TestClient(app, client=(f"2001:db8:99::{i + 1:x}", 50000), base_url="https://share.example.com") as v6_c:
            assert (
                v6_c.post(
                    "/api/web-vitals",
                    json=sample,
                    headers={"X-Proxied-By-Caddy": "true", "Origin": "https://share.example.com"},
                ).status_code
                == 200
            )
    for j in range(4105):
        probe_ip = f"198.18.{j // 256}.{j % 256}"
        web_vitals_mod._WEB_VITALS_LIMITER.try_acquire_with_retry(
            f"wv:{probe_ip}", max_events=web_vitals_mod.WEB_VITALS_RATE_LIMIT
        )
    with TestClient(app, client=("2001:db8:99::ffff", 50000), base_url="https://share.example.com") as v6_end:
        r_v6_429 = v6_end.post(
            "/api/web-vitals",
            json=sample,
            headers={"X-Proxied-By-Caddy": "true", "Origin": "https://share.example.com"},
        )
        assert r_v6_429.status_code == 429
