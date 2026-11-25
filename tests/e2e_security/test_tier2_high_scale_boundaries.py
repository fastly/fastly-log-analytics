"""Tier 2 (Part A) — High-Scale Boundary, Spoofing & Bypass Rejection (28 tests across F1-F3)."""

from __future__ import annotations

import csv
import http.client
import io
import json
import os
import re
import subprocess
import urllib.error
import urllib.request
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

from backend import config as svcconfig
from backend.core import fos_client, share_db
from backend.core import request_context as req_ctx_mod
from backend.routers.admin import _helpers as admin_helpers
from tests.e2e_security.conftest import (
    REPO_ROOT,
    RecordingClickHouseClient,
    make_client,
    probe_sse_status,
    provision_analyst_session,
)

pytestmark = pytest.mark.no_postgres


# ============================================================================
# Area F1 — Edge/Proxy Client-IP Spoofing, SSR Proxy & Gateway Hardening (9)
# ============================================================================


def test_f1_rejects_fastly_client_ip_loopback_spoofing_via_caddy_marker() -> None:
    """SEC-01: Loopback TCP peer carrying X-Proxied-By-Caddy + spoofed Fastly-Client-IP: 127.0.0.1 must NOT get Admin."""
    # Without active tunnel, untrusted remote Host is rejected 400 (never 200 Admin)
    resp_untrusted_host = make_client(
        "127.0.0.1",
        headers={
            "X-Proxied-By-Caddy": "true",
            "Fastly-Client-IP": "127.0.0.1",
        },
    ).get("/api/admin/queries")
    assert resp_untrusted_host.status_code in {400, 401, 403}

    # With active tunnel Host, spoofed Fastly-Client-IP: 127.0.0.1 is rejected 401/403
    provision_analyst_session(service_ids=["svc-a"])
    resp = make_client(
        "127.0.0.1",
        headers={
            "Host": "share.example.test",
            "X-Proxied-By-Caddy": "true",
            "Fastly-Client-IP": "127.0.0.1",
        },
    ).get("/api/admin/queries")
    assert resp.status_code in {401, 403}, f"Spoofed Fastly-Client-IP via Caddy got {resp.status_code}"


def test_f1_rejects_xff_loopback_spoofing_from_pod_and_public_gateway() -> None:
    """SEC-01/SEC-02: Pod peer or public-gateway marker with X-Forwarded-For: 127.0.0.1 must NOT get Admin."""
    provision_analyst_session(service_ids=["svc-a"])
    pod_client = make_client(
        "10.244.1.9",
        headers={"Host": "share.example.test", "X-Forwarded-For": "127.0.0.1"},
    )
    r_pod = pod_client.get("/api/admin/queries")
    assert r_pod.status_code in {401, 403}, f"Pod XFF loopback spoof got {r_pod.status_code}"

    pub_gw_client = make_client(
        "127.0.0.1",
        headers={
            "Host": "share.example.test",
            "X-FLA-Public-Gateway": "1",
            "X-Forwarded-For": "127.0.0.1",
            "Fastly-Client-IP": "127.0.0.1",
        },
    )
    r_pub = pub_gw_client.get("/api/admin/queries")
    assert r_pub.status_code in {401, 403}, f"Public gateway XFF spoof got {r_pub.status_code}"


def test_f1_rejects_non_loopback_tcp_peer_with_spoofed_loopback_headers() -> None:
    """SEC-01/SEC-02: External TCP peer spoofing loopback IP headers is unconditionally remote."""
    provision_analyst_session(service_ids=["svc-a"])
    client = make_client(
        "93.184.216.34",
        headers={
            "Host": "share.example.test",
            "Fastly-Client-IP": "127.0.0.1",
            "X-Forwarded-For": "127.0.0.1",
            "X-Real-IP": "127.0.0.1",
        },
    )
    resp = client.get("/api/admin/queries")
    assert resp.status_code in {401, 403}


def test_f1_ssr_api_proxy_does_not_forward_untrusted_client_xff() -> None:
    """SEC-02: frontend/lib/apiSsrProxy.ts strips raw client x-forwarded-for and drops loopback literals."""
    ssr_proxy = (REPO_ROOT / "frontend" / "lib" / "apiSsrProxy.ts").read_text(encoding="utf-8")
    assert "STRIPPED_FORWARDED_HEADERS" in ssr_proxy, "Missing STRIPPED_FORWARDED_HEADERS in apiSsrProxy.ts"
    assert '"x-forwarded-for"' in ssr_proxy, "x-forwarded-for must be handled explicitly in apiSsrProxy.ts"
    assert "sanitizeForwardedFor" in ssr_proxy and "isLoopbackIpLiteral" in ssr_proxy, (
        "apiSsrProxy.ts must strip loopback literals via sanitizeForwardedFor/isLoopbackIpLiteral"
    )


def _strip_unquoted_inline_comment(line: str) -> str:
    in_single = False
    in_double = False
    escaped = False
    for idx, ch in enumerate(line):
        if escaped:
            escaped = False
            continue
        if ch == "\\" and (in_single or in_double):
            escaped = True
            continue
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif ch == "#" and not in_single and not in_double:
            return line[:idx].rstrip()
    return line


def _non_comment_lines(text: str) -> str:
    cleaned = [_strip_unquoted_inline_comment(line) for line in text.splitlines()]
    return "\n".join(line for line in cleaned if line.strip())


def _extract_caddy_block(text: str, directive: str) -> str:
    match = re.search(rf"(?m)^\s*{re.escape(directive)}\s*\{{", text)
    assert match is not None, f"{directive} {{ ... }} block missing in non-comment config"
    depth = 1
    in_double = False
    escaped = False
    pos = match.end()
    while pos < len(text) and depth > 0:
        ch = text[pos]
        if escaped:
            escaped = False
        elif ch == "\\" and in_double:
            escaped = True
        elif ch == '"':
            in_double = not in_double
        elif not in_double:
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return text[match.end() : pos]
        pos += 1
    raise AssertionError(f"Unclosed {directive} {{ ... }} block in config")


def test_f1_caddyfile_enforces_fastly_ipv6_ranges_and_safe_rate_limit_key() -> None:
    """SEC-01: Caddyfile must include Fastly IPv6 ranges (2a04:4e40::/32, 2a04:4e42::/32) and X-Forwarded-For rate_limit key."""
    caddy_texts = [
        _non_comment_lines(p.read_text(encoding="utf-8"))
        for p in [REPO_ROOT / "Caddyfile", *sorted((REPO_ROOT / "caddy").glob("*"))]
        if p.is_file()
    ]
    combined = "\n".join(caddy_texts)
    assert "2a04:4e40::/32" in combined, "Fastly IPv6 range 2a04:4e40::/32 missing from Caddyfile"
    assert "2a04:4e42::/32" in combined, "Fastly IPv6 range 2a04:4e42::/32 missing from Caddyfile"
    assert "rate_limit" in combined, "rate_limit directive missing from active Caddyfile lines"
    assert "{http.request.header.X-Forwarded-For}" in combined, (
        "Caddyfile rate_limit must key on {http.request.header.X-Forwarded-For}"
    )


def test_f1_helm_public_gateway_and_values_enforce_trusted_proxies_and_limits() -> None:
    """SEC-02: Non-comment Helm public-gateway.yaml and values.yaml enforce trusted_proxies, header strips, 172.16.0.0/12, 25MB, and security headers."""
    pub_gw_nc = _non_comment_lines(
        (REPO_ROOT / "deploy" / "chart" / "fastly-log-analytics" / "templates" / "public-gateway.yaml").read_text(
            encoding="utf-8"
        )
    )
    values_nc = _non_comment_lines(
        (REPO_ROOT / "deploy" / "chart" / "fastly-log-analytics" / "values.yaml").read_text(encoding="utf-8")
    )
    assert "trusted_proxies" in pub_gw_nc, "trusted_proxies missing in non-comment public-gateway.yaml"
    assert "@from_fastly_edge" in pub_gw_nc, "@from_fastly_edge matcher missing in non-comment public-gateway.yaml"
    for stripped_hdr in ("-X-Admin-Gateway-Token", "-X-FLA-Admin-Gateway", "-X-FLA-Frontend-Proxy"):
        assert stripped_hdr.lower() in pub_gw_nc.lower(), (
            f"{stripped_hdr} strip missing in non-comment public-gateway.yaml"
        )
    assert "25mb" in pub_gw_nc.lower(), "25MB request_body max_size missing in non-comment public-gateway.yaml"
    header_block = _extract_caddy_block(pub_gw_nc, "header")
    for sec_hdr in (
        "Strict-Transport-Security",
        "X-Frame-Options",
        "X-Content-Type-Options",
        "Referrer-Policy",
        "Permissions-Policy",
        "Cross-Origin-Opener-Policy",
    ):
        assert re.search(rf'(?m)^\s*{re.escape(sec_hdr)}\s+"[^"]+"$', header_block), (
            f"Active security header directive {sec_hdr} missing in header {{ ... }} block of public-gateway.yaml"
        )
    assert "172.16.0.0/12" in values_nc or "172.16.0.0/12" in pub_gw_nc, (
        "RFC1918 172.16.0.0/12 CIDR missing from non-comment Helm config"
    )


def test_f1_api_fallback_csp_blocks_scripts_and_framing() -> None:
    """SEC-10: /api/* responses carry strict fallback CSP (default-src 'none'; frame-ancestors 'none')."""
    resp = make_client("127.0.0.1").get("/api/bootstrap")
    assert resp.status_code == 200
    csp = resp.headers.get("content-security-policy", "")
    assert "default-src 'none'" in csp, f"Expected default-src 'none' in API CSP, got {csp!r}"
    assert "frame-ancestors 'none'" in csp, f"Expected frame-ancestors 'none' in API CSP, got {csp!r}"


def test_f1_active_service_cookie_regex_and_proxy_ts_cookie_hardening(
    isolated_e2e_security_state: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SEC-10b: Execute real frontend/proxy.ts via offline Node harness + FastAPI x-service-id/X-Fastly-Service-Id validation."""
    proxy_ts_path = REPO_ROOT / "frontend" / "proxy.ts"
    ts_mod_dir = REPO_ROOT / "frontend" / "node_modules" / "typescript"
    node_runner = r"""
const fs = require('fs');
const ts = require(process.env.FLA_TS_PATH);
const src = fs.readFileSync(process.env.FLA_PROXY_TS, 'utf8');
const transpiled = ts.transpileModule(src, {
  compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 }
}).outputText;

class FakeNextResponse {
  constructor(body = null, init = {}) {
    this.body = body;
    this.status = init.status || 200;
    this.headers = new Headers(init.headers || {});
    this.forwardedHeaders = init.request && init.request.headers ? new Headers(init.request.headers) : null;
    this._cookies = [];
    this.cookies = {
      set: (name, value, opts = {}) => {
        this._cookies.push({ name, value, opts });
      }
    };
  }
  static next(init = {}) { return new FakeNextResponse(null, init); }
  static redirect(url, status = 307) {
    return new FakeNextResponse(null, { status, headers: { location: String(url) } });
  }
}

const modExports = {};
const modObj = { exports: modExports };
const customRequire = (id) => {
  if (id === 'next/server') return { NextResponse: FakeNextResponse, NextRequest: class {} };
  if (id === './lib/utils') return { ACTIVE_SERVICE_COOKIE: 'fla_active_service' };
  return require(id);
};
const fn = new Function('exports', 'require', 'module', '__filename', '__dirname', transpiled);
fn(modExports, customRequire, modObj, process.env.FLA_PROXY_TS, process.cwd());
const proxy = modObj.exports.proxy;

function makeReq(urlStr, cookieHeader) {
  const u = new URL(urlStr);
  u.clone = () => new URL(u.toString());
  const hdrs = new Headers();
  if (cookieHeader) hdrs.set('cookie', cookieHeader);
  return {
    method: 'GET',
    url: u.toString(),
    nextUrl: u,
    headers: hdrs,
    cookies: {
      get: (name) => {
        if (!cookieHeader) return undefined;
        for (const p of cookieHeader.split(';')) {
          const eq = p.indexOf('=');
          if (eq !== -1 && p.slice(0, eq).trim() === name) {
            return { name, value: p.slice(eq + 1).trim() };
          }
        }
        return undefined;
      }
    }
  };
}

const rValid = proxy(makeReq('https://logs.example.test/dashboard?service=svc-b', ''));
const rMalicious = proxy(
  makeReq(
    'https://logs.example.test/dashboard?service=../../etc/passwd;evil',
    'fla_active_service=../../etc/passwd;evil'
  )
);
const rBadSess = proxy(
  makeReq('https://logs.example.test/dashboard', 'fla_active_service=svc-a; analyst_session_id=bad<script>')
);
const rGoodSess = proxy(
  makeReq(
    'https://logs.example.test/dashboard',
    'fla_active_service=svc-b; analyst_session_id=AbCdEfGhIjKlMnOpQrStUvWxYz012345'
  )
);

console.log(JSON.stringify({
  valid: {
    xServiceId: rValid.forwardedHeaders && rValid.forwardedHeaders.get('x-service-id'),
    cookies: rValid._cookies,
  },
  malicious: {
    xServiceId: rMalicious.forwardedHeaders && rMalicious.forwardedHeaders.get('x-service-id'),
    cookies: rMalicious._cookies,
  },
  badSess: {
    xServiceId: rBadSess.forwardedHeaders && rBadSess.forwardedHeaders.get('x-service-id'),
    cookies: rBadSess._cookies,
  },
  goodSess: {
    xServiceId: rGoodSess.forwardedHeaders && rGoodSess.forwardedHeaders.get('x-service-id'),
    cookies: rGoodSess._cookies,
  },
}));
"""
    env = os.environ.copy()
    env["NODE_ENV"] = "production"
    env["FLA_PROXY_TS"] = str(proxy_ts_path)
    env["FLA_TS_PATH"] = str(ts_mod_dir)
    proc = subprocess.run(
        ["node", "-e", node_runner],
        env=env,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert proc.returncode == 0, f"Offline proxy.ts Node execution failed:\n{proc.stderr}"
    out = json.loads(proc.stdout)

    # 1. Valid service param sets x-service-id and secure fla_active_service cookie in production
    assert out["valid"]["xServiceId"] == "svc-b"
    svc_cookies = [c for c in out["valid"]["cookies"] if c["name"] == "fla_active_service"]
    assert len(svc_cookies) == 1
    assert svc_cookies[0]["value"] == "svc-b"
    assert svc_cookies[0]["opts"].get("secure") is True
    assert svc_cookies[0]["opts"].get("sameSite") == "lax"

    # 2. Malicious service param & cookie failing SERVICE_ID_RE MUST NOT forward x-service-id or set cookie
    assert out["malicious"]["xServiceId"] is None, (
        f"Malicious service ID bypassed SERVICE_ID_RE into x-service-id: {out['malicious']}"
    )
    assert not [c for c in out["malicious"]["cookies"] if c["name"] == "fla_active_service"], (
        f"Malicious service ID set fla_active_service cookie: {out['malicious']}"
    )

    # 3. Malformed analyst_session_id failing PLAUSIBLE_SESSION_RE is actively cleared with secure+strict+httpOnly
    cleared = [c for c in out["badSess"]["cookies"] if c["name"] == "analyst_session_id"]
    assert len(cleared) == 1, f"Malformed analyst_session_id was not cleared by proxy.ts: {out['badSess']}"
    assert cleared[0]["value"] == ""
    assert cleared[0]["opts"].get("maxAge") == 0
    assert cleared[0]["opts"].get("httpOnly") is True
    assert cleared[0]["opts"].get("sameSite") == "strict"
    assert cleared[0]["opts"].get("secure") is True

    # 4. Valid analyst_session_id + valid fla_active_service cookie forwards svc-b without clearing session
    assert out["goodSess"]["xServiceId"] == "svc-b"
    assert not [c for c in out["goodSess"]["cookies"] if c["name"] == "analyst_session_id"]

    # 5. FastAPI backend rejects malicious/traversal x-service-id and X-Fastly-Service-Id headers with 400
    monkeypatch.setattr(
        "backend.config.get_active_service_id",
        isolated_e2e_security_state.orig_get_active_service_id,
    )
    assert Path(svcconfig.CONFIGS_DIR) != REPO_ROOT / "configs", (
        "svcconfig.CONFIGS_DIR must be sandboxed to tmp_path before staging escaped_outside.json"
    )
    outside_cfg = dict(isolated_e2e_security_state.configs["svc-a"])
    outside_cfg["service_id"] = "escaped_outside"
    outside_cfg["name"] = "Escaped Outside Config"
    outside_cfg["rum"] = {
        "enabled": True,
        "cid_salt": "outside-sentinel-salt",
        "faro_version": "1.19.0",
    }
    outside_cfg_path = Path(svcconfig.CONFIGS_DIR).parent / "escaped_outside.json"
    outside_cfg_path.write_text(json.dumps(outside_cfg), encoding="utf-8")
    with svcconfig._config_cache_lock:
        svcconfig._config_cache.clear()

    for hdr_name in ("x-service-id", "X-Fastly-Service-Id"):
        r_bad_hdr = make_client("127.0.0.1").post(
            "/api/query",
            headers={hdr_name: "../../etc/passwd;evil"},
            json={"sql": "SELECT count() AS n FROM logs", "dataset": "logs"},
        )
        assert r_bad_hdr.status_code in {400, 403, 422}, (
            f"FastAPI accepted malicious {hdr_name}: status={r_bad_hdr.status_code} body={r_bad_hdr.text}"
        )
        r_escape_audit = make_client("127.0.0.1").get(
            "/api/audit-logs",
            headers={hdr_name: "../escaped_outside"},
        )
        assert r_escape_audit.status_code == 400, (
            f"FastAPI loaded ../escaped_outside.json on GET /api/audit-logs via {hdr_name}: "
            f"status={r_escape_audit.status_code} body={r_escape_audit.text}"
        )
        r_escape_query = make_client("127.0.0.1").post(
            "/api/query",
            headers={hdr_name: "../escaped_outside"},
            json={"sql": "SELECT 1"},
        )
        assert r_escape_query.status_code == 400, (
            f"FastAPI loaded ../escaped_outside.json on POST /api/query via {hdr_name}: "
            f"status={r_escape_query.status_code} body={r_escape_query.text}"
        )

    assert "../escaped_outside" not in svcconfig._config_cache, (
        "Traversal probe populated ../escaped_outside in svcconfig._config_cache"
    )

    for probe_label, r_rum in (
        ("query param", make_client("127.0.0.1").get("/js/rum.js?service_id=../escaped_outside")),
        (
            "x-service-id header",
            make_client("127.0.0.1").get("/js/rum.js", headers={"x-service-id": "../escaped_outside"}),
        ),
        (
            "active_service_id cookie",
            make_client("127.0.0.1", cookies={"active_service_id": "../escaped_outside"}).get("/js/rum.js"),
        ),
    ):
        assert r_rum.status_code == 200, f"Unexpected /js/rum.js status via {probe_label}: {r_rum.status_code}"
        assert "escaped_outside" not in r_rum.text, (
            f"/js/rum.js traversed outside CONFIGS_DIR via {probe_label}: {r_rum.text[:200]}"
        )
        assert "outside-sentinel-salt" not in r_rum.text, f"/js/rum.js leaked outside-sentinel-salt via {probe_label}"

    r_teardown = make_client("127.0.0.1").post(
        "/api/provision/teardown",
        json={"service_id": "../escaped_outside", "token": ""},
    )
    assert r_teardown.status_code == 404, (
        f"POST /api/provision/teardown resolved ../escaped_outside outside CONFIGS_DIR: "
        f"status={r_teardown.status_code} body={r_teardown.text}"
    )
    assert "../escaped_outside" not in svcconfig._config_cache, (
        "Public HTTP traversal probes populated ../escaped_outside in svcconfig._config_cache"
    )


def test_f1_admin_router_require_admin_and_cross_origin_web_vitals_rejected() -> None:
    """SEC-10: admin/_router.py blocks analyst sessions (403) and unauthenticated cross-origin /api/web-vitals is rejected."""
    bundle = provision_analyst_session(service_ids=["svc-a"], client_ip="203.0.113.19")
    analyst_client = make_client("203.0.113.19", cookies=bundle.cookies())
    r_admin = analyst_client.get("/api/admin/raw-tree", headers=bundle.headers())
    assert r_admin.status_code in {401, 403}

    ext_client = make_client(
        "203.0.113.99",
        headers={
            "Host": "share.example.test",
            "Origin": "https://evil.attacker.example",
            "X-Proxied-By-Caddy": "true",
            "Fastly-Client-IP": "203.0.113.99",
        },
    )
    r_vitals = ext_client.post(
        "/api/web-vitals",
        json={
            "name": "LCP",
            "value": 100.0,
            "rating": "good",
            "id": "v4-attack-1",
            "page": "/dashboard",
        },
    )
    assert r_vitals.status_code in {401, 403, 429}, (
        f"Unauthenticated cross-origin /api/web-vitals returned {r_vitals.status_code}"
    )


# ============================================================================
# Area F2 — Share Inactive Leak, Stop Sharing/Panic, Rate-Limit & SSE Clamp (9)
# ============================================================================


def test_f2_rejects_x_remote_analyst_header_when_sharing_inactive() -> None:
    """SEC-03: SSR X-Remote-Analyst: 1 when sharing is off/inactive MUST NOT fall through to loopback Admin."""
    share_db.set_setting("sharing_enabled", "false")
    client = make_client(
        "127.0.0.1",
        headers={"X-Remote-Analyst": "1"},
    )
    r_admin = client.get("/api/admin/queries")
    assert r_admin.status_code in {401, 403}, (
        f"Inactive sharing X-Remote-Analyst: 1 leaked Admin ({r_admin.status_code})"
    )

    r_views = client.get("/api/views/svc-a")
    assert r_views.status_code in {401, 403}, (
        f"Inactive sharing X-Remote-Analyst: 1 leaked views ({r_views.status_code})"
    )


def test_f2_stop_sharing_immediately_blocks_active_session_and_relogin() -> None:
    """SEC-04: POST /api/admin/share/stop immediately blocks active analyst sessions AND passcode re-login."""
    bundle = provision_analyst_session(service_ids=["svc-a"], client_ip="203.0.113.31")
    admin_client = make_client("127.0.0.1")
    r_stop = admin_client.post("/api/admin/share/stop", json={})
    assert r_stop.status_code == 200

    analyst_client = make_client("203.0.113.31", cookies=bundle.cookies())
    r_api = analyst_client.get("/api/views/svc-a", headers=bundle.headers())
    assert r_api.status_code in {401, 403}, f"Active session survived Stop Sharing ({r_api.status_code})"

    r_relogin = make_client("203.0.113.31").post(
        "/api/share/login",
        headers=bundle.headers(),
        json={"email": bundle.email, "passcode": bundle.passcode},
    )
    assert r_relogin.status_code in {401, 403}, f"Passcode login succeeded after Stop Sharing ({r_relogin.status_code})"


def test_f2_panic_lockdown_revokes_sessions_invites_and_blocks_claim() -> None:
    """SEC-04: POST /api/admin/share/panic revokes sessions + invites and blocks claim tokens."""
    bundle = provision_analyst_session(service_ids=["svc-a"], client_ip="203.0.113.32")
    claim_tok = share_db.create_claim_token(bundle.invite_id)

    r_panic = make_client("127.0.0.1").post("/api/admin/share/panic", json={})
    assert r_panic.status_code == 200

    r_sess = make_client("203.0.113.32", cookies=bundle.cookies()).get(
        "/api/views/svc-a",
        headers=bundle.headers(),
    )
    assert r_sess.status_code in {401, 403}

    r_claim = make_client("203.0.113.32").post(
        f"/api/share/claim/{claim_tok}",
        headers=bundle.headers(),
        json={},
    )
    assert r_claim.status_code in {401, 403, 404, 410}


def test_f2_rejects_external_host_localhost_spoofing_on_remote_requests() -> None:
    """SEC-04: External caller sending Host: localhost must be rejected when no localhost public share endpoint exists."""
    bundle = provision_analyst_session(
        service_ids=["svc-a"],
        public_host="share.example.test",
        client_ip="203.0.113.33",
    )
    spoof_headers = bundle.headers({"Host": "localhost", "Origin": "http://localhost"})
    resp = make_client("203.0.113.33", cookies=bundle.cookies()).get(
        "/api/views/svc-a",
        headers=spoof_headers,
    )
    assert resp.status_code in {400, 401, 403}, (
        f"Remote request with spoofed Host: localhost returned {resp.status_code}"
    )


def test_f2_enforces_per_ip_rate_limiting_on_share_claim_and_oauth(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SEC-10c: /api/share/claim/* AND /api/share/oauth/* enforce per-IP 429 rate-limiting, and SESSION_TOKEN_SECRET is wired."""
    bundle = provision_analyst_session(service_ids=["svc-a"], client_ip="203.0.113.34")
    client_claim = make_client("203.0.113.34")
    claim_statuses = [
        client_claim.post(
            f"/api/share/claim/invalid-claim-token-{i:04d}",
            headers=bundle.headers(),
            json={},
        ).status_code
        for i in range(25)
    ]
    assert 429 in claim_statuses, (
        f"Expected 429 rate-limit on /api/share/claim/* brute force, got {set(claim_statuses)}"
    )

    # Exercise /api/share/oauth/callback and /api/share/oauth/authorize from distinct IPs to prove "oauth" bucket rate-limits
    client_oauth_cb = make_client("203.0.113.35")
    oauth_cb_statuses = [
        client_oauth_cb.get(
            f"/api/share/oauth/callback?code=bad-{i:04d}&state=bad-{i:04d}",
            headers=bundle.headers(),
            follow_redirects=False,
        ).status_code
        for i in range(25)
    ]
    assert 429 in oauth_cb_statuses, (
        f"Expected 429 rate-limit on /api/share/oauth/callback flood, got {set(oauth_cb_statuses)}"
    )

    client_oauth_authz = make_client("203.0.113.45")
    oauth_authz_statuses = [
        client_oauth_authz.get(
            "/api/share/oauth/authorize?provider=google",
            headers=bundle.headers(),
            follow_redirects=False,
        ).status_code
        for i in range(25)
    ]
    assert 429 in oauth_authz_statuses, (
        f"Expected 429 rate-limit on /api/share/oauth/authorize flood, got {set(oauth_authz_statuses)}"
    )

    # Verify SESSION_TOKEN_SECRET + CURSOR_SECRET fallback key derivation & Helm secret.yaml provisioning
    secret_yaml = _non_comment_lines(
        (REPO_ROOT / "deploy" / "chart" / "fastly-log-analytics" / "templates" / "secret.yaml").read_text(
            encoding="utf-8"
        )
    )
    assert "SESSION_TOKEN_SECRET" in secret_yaml and "sessionTokenSecret" in secret_yaml, (
        "Helm secret.yaml must provision SESSION_TOKEN_SECRET from .Values.secrets.sessionTokenSecret"
    )

    req_ctx_mod._key = None
    monkeypatch.setenv("SESSION_TOKEN_SECRET", "test-sentinel-session-token-secret-0001")
    monkeypatch.delenv("HIGH_SCALE_CURSOR_SECRET", raising=False)
    monkeypatch.delenv("CURSOR_SECRET", raising=False)
    sealed_a = req_ctx_mod.seal_session_token(
        "203.0.113.44",
        "t13d1516h2_8daaf6152771_b0da82dd1658",
        "2026-11-19T10:00:00Z",
        "2026-11-19T10:15:00Z",
        service_id="svc-a",
    )
    assert req_ctx_mod.open_session_token(sealed_a, service_id="svc-a")[0] == "203.0.113.44"

    # Rotating SESSION_TOKEN_SECRET must invalidate tokens sealed under the previous secret
    req_ctx_mod._key = None
    monkeypatch.setenv("SESSION_TOKEN_SECRET", "test-sentinel-session-token-secret-0002")
    with pytest.raises(req_ctx_mod.SessionTokenError):
        req_ctx_mod.open_session_token(sealed_a, service_id="svc-a")
    assert req_ctx_mod.verify_session_token(sealed_a, service_id="svc-a") is None

    # Fallback to HIGH_SCALE_CURSOR_SECRET when SESSION_TOKEN_SECRET is unset
    req_ctx_mod._key = None
    monkeypatch.delenv("SESSION_TOKEN_SECRET", raising=False)
    monkeypatch.setenv("HIGH_SCALE_CURSOR_SECRET", "test-sentinel-cursor-fallback-0001")
    sealed_fb = req_ctx_mod.seal_session_token(
        "203.0.113.45",
        None,
        "2026-11-19T10:00:00Z",
        "2026-11-19T10:15:00Z",
        service_id="svc-a",
    )
    req_ctx_mod._key = None
    assert req_ctx_mod.open_session_token(sealed_fb, service_id="svc-a")[0] == "203.0.113.45"
    req_ctx_mod._key = None


def test_f2_blocks_remote_caddy_and_analyst_access_to_metrics() -> None:
    """SEC-05: Remote, Caddy-proxied, and Analyst callers must all receive 401/403 on GET /metrics."""
    r_ext = make_client("93.184.216.34").get("/metrics")
    assert r_ext.status_code in {401, 403}

    r_caddy = make_client("127.0.0.1", headers={"X-Proxied-By-Caddy": "true"}).get("/metrics")
    assert r_caddy.status_code in {401, 403}

    bundle = provision_analyst_session(service_ids=["svc-a"], client_ip="203.0.113.36")
    r_analyst = make_client("203.0.113.36", cookies=bundle.cookies()).get(
        "/metrics",
        headers=bundle.headers(),
    )
    assert r_analyst.status_code in {401, 403}


def test_f2_blocks_unauthenticated_and_analyst_debug_and_sql_query_endpoints() -> None:
    """SEC-05: Debug SQLite/Postgres/telemetry and admin SQL query endpoints reject unauth & analyst callers."""
    bundle = provision_analyst_session(service_ids=["svc-a"], client_ip="203.0.113.37")
    unauth = make_client("203.0.113.37", headers={"Host": "share.example.test"})
    analyst = make_client("203.0.113.37", cookies=bundle.cookies())

    protected_paths = (
        "/api/debug/recent-sqlite",
        "/api/debug/recent-postgres",
        "/api/debug/page-telemetry",
        "/api/admin/queries",
        "/api/admin/slow-queries",
    )
    for path in protected_paths:
        ru = unauth.get(path)
        assert ru.status_code in {401, 403}, f"Unauth GET {path} returned {ru.status_code}"
        ra = analyst.get(path, headers=bundle.headers())
        assert ra.status_code in {401, 403}, f"Analyst GET {path} returned {ra.status_code}"


def test_f2_historical_analyst_window_clamped_off_realtime_seed() -> None:
    """SEC-06: Analyst with bounded historical query_end_time < now receives 403 on realtime-seed."""
    past_start = (datetime.now(UTC) - timedelta(hours=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
    past_end = (datetime.now(UTC) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    bundle = provision_analyst_session(
        service_ids=["svc-a"],
        query_start_time=past_start,
        query_end_time=past_end,
        client_ip="203.0.113.38",
    )
    resp = make_client("203.0.113.38", cookies=bundle.cookies()).get(
        "/api/services/svc-a/realtime-seed",
        headers=bundle.headers(),
    )
    assert resp.status_code == 403, f"Historical analyst allowed on realtime-seed ({resp.status_code})"


def test_f2_historical_and_future_analyst_windows_clamped_off_live_sse_streams(
    ch_recorder: RecordingClickHouseClient,
) -> None:
    """SEC-06: Historical (query_end_time < now) and future (query_start_time > now) analysts get 403 on live SSE."""
    past_bundle = provision_analyst_session(
        service_ids=["svc-a"],
        query_start_time="2026-01-01T00:00:00Z",
        query_end_time="2026-01-01T02:00:00Z",
        client_ip="203.0.113.39",
        email="past@tenant.example.test",
    )
    future_start = (datetime.now(UTC) + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    future_end = (datetime.now(UTC) + timedelta(hours=4)).strftime("%Y-%m-%dT%H:%M:%SZ")
    future_bundle = provision_analyst_session(
        service_ids=["svc-a"],
        query_start_time=future_start,
        query_end_time=future_end,
        client_ip="203.0.113.40",
        email="future@tenant.example.test",
    )

    for b, ip in ((past_bundle, "203.0.113.39"), (future_bundle, "203.0.113.40")):
        for sse_path in (
            "/api/services/svc-a/realtime-stream",
            "/api/services/svc-a/control-room/error-stream",
        ):
            status, _ = probe_sse_status(
                sse_path,
                peer_ip=ip,
                headers=b.headers(),
                cookies=b.cookies(),
            )
            assert status == 403, f"Bounded analyst got {status} on live SSE {sse_path}"


# ============================================================================
# Area F3 — Body Limits, CSV Formula, Download Traversal/SSRF & SQL Safety (10)
# ============================================================================


def test_f3_rejects_oversized_body_with_413_without_silent_truncation() -> None:
    """SEC-09: RemoteAccessMiddleware rejects >4MiB bodies with 413 instead of silently truncating."""
    bundle = provision_analyst_session(service_ids=["svc-a"], client_ip="203.0.113.70")
    huge_payload = b'{"email":"a@b.co","pad":"' + (b"A" * (4 * 1024 * 1024 + 4096)) + b'"}'
    resp = make_client("203.0.113.70").post(
        "/api/share/login",
        headers=bundle.headers({"Content-Type": "application/json"}),
        content=huge_payload,
    )
    assert resp.status_code == 413, f"Expected 413 on >4MiB body, got {resp.status_code}"


def test_f3_rejects_malformed_and_negative_content_length_with_400() -> None:
    """SEC-09: Malformed or negative Content-Length headers on remote requests return 400."""
    bundle = provision_analyst_session(service_ids=["svc-a"], client_ip="203.0.113.71")
    for bad_cl in ("-10", "not-a-number"):
        resp = make_client("203.0.113.71").post(
            "/api/share/login",
            headers=bundle.headers({"Content-Length": bad_cl, "Content-Type": "application/json"}),
            content=b"{}",
        )
        assert resp.status_code == 400, f"Content-Length={bad_cl!r} returned {resp.status_code}"


def test_f3_csv_export_neutralizes_all_spreadsheet_formula_prefixes(
    ch_recorder: RecordingClickHouseClient,
) -> None:
    """SEC-09: POST /api/dashboard/raw/csv escapes leading =, +, -, @, \\t, \\r formula injection cells."""
    malicious_formulas = [
        "=CMD|' /C calc'!A0",
        "+SUM(1,1)",
        "-2+3+cmd",
        "@SUM(A1:A2)",
        "\t=1+1",
        "\r=1+1",
    ]
    rows = [
        {
            "event_id": f"e-{idx}",
            "event_timestamp": datetime(2026, 6, 12, 0, idx + 1, tzinfo=UTC),
            "ingested_at": datetime(2026, 6, 12, 0, idx + 2, tzinfo=UTC),
            "owner_epoch": 1,
            "client_ip": "198.51.100.5",
            "method": "GET",
            "url": formula,
            "host": "shop.example.com",
            "status": 200,
            "pop": "IAD",
            "country": "US",
            "asn": 54113,
            "cache_status": "MISS",
            "resp_body_size": 128,
            "time_elapsed_usec": 900,
            "is_error": 0,
            "is_bot": 0,
            "ua": "Mozilla/5.0",
            "protocol": "HTTP/2",
            "tls_version": "TLSv1.3",
            "custom_fields": {},
        }
        for idx, formula in enumerate(malicious_formulas)
    ]
    ch_recorder.set_handler(lambda sql, params, settings: rows)

    resp = make_client("127.0.0.1", headers={"X-Fastly-Service-Id": "svc-a"}).post(
        "/api/dashboard/raw/csv",
        json={
            "start_time": "2026-06-12T00:00:00Z",
            "end_time": "2026-06-12T01:00:00Z",
            "filters": {},
            "columns": [],
        },
    )
    assert resp.status_code == 200
    parsed = list(csv.DictReader(io.StringIO(resp.text)))
    assert len(parsed) == len(malicious_formulas)
    for row_dict in parsed:
        cell = row_dict["url"]
        assert not cell.startswith(("=", "+", "-", "@", "\t", "\r")), (
            f"Unescaped spreadsheet formula cell in CSV export: {cell!r}"
        )


def test_f3_downloads_reject_unauthenticated_and_analyst_callers() -> None:
    """SEC-09: /api/download, /api/download-folder, and /api/download-all enforce require_admin."""
    bundle = provision_analyst_session(service_ids=["svc-a"], client_ip="203.0.113.72")
    unauth = make_client("203.0.113.72", headers={"Host": "share.example.test"})
    analyst = make_client("203.0.113.72", cookies=bundle.cookies())

    for dl_path in (
        "/api/download?key=raw/request/sample.log.gz",
        "/api/download-folder?root=raw&prefix=request",
        "/api/download-all?include=raw",
    ):
        assert unauth.get(dl_path).status_code in {401, 403}
        assert analyst.get(dl_path, headers=bundle.headers()).status_code in {401, 403}


def test_f3_download_folder_rejects_invalid_root_and_dotdot_traversal() -> None:
    """SEC-09: /api/download-folder restricts root to allowlist and rejects '..' traversal in prefix/key with 400."""
    admin_client = make_client("127.0.0.1")
    for bad_query in (
        "/api/download-folder?root=../../etc&prefix=passwd",
        "/api/download-folder?root=configs&prefix=svc-a",
        "/api/download-folder?root=raw&prefix=../../configs",
        "/api/download?key=raw/../../configs/svc-a.json",
    ):
        resp = admin_client.get(bad_query)
        assert resp.status_code == 400, f"Traversal/invalid root {bad_query} returned {resp.status_code} (expected 400)"


def test_f3_downloads_sanitize_crlf_and_quote_injection_in_content_disposition(
    isolated_e2e_security_state: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SEC-09b: /api/download-folder and /api/download reject CRLF with 400 and sanitize quotes/semicolons on 200 OK."""
    admin_client = make_client("127.0.0.1", headers={"X-Fastly-Service-Id": "svc-a"})

    # Phase A: CRLF injection in prefix and key is unconditionally rejected 400 without header splitting
    for crlf_url in (
        '/api/download-folder?root=raw&prefix=test"%0d%0aX-Injected:%20pwn',
        '/api/download?key=raw/request/test"%0d%0aX-Injected:%20pwn.log',
    ):
        r_crlf = admin_client.get(crlf_url)
        assert r_crlf.status_code == 400, f"Expected 400 on CRLF download input {crlf_url}, got {r_crlf.status_code}"
        assert "x-injected" not in {k.lower() for k in r_crlf.headers.keys()}

    # Phase B1: Quote & semicolon parameter injection on 200 OK /api/download-folder
    class _FakeFosBody:
        def __init__(self, payload: bytes) -> None:
            self._payload = payload

        def iter_chunks(self, chunk_size: int = 65536) -> Iterator[bytes]:
            for idx in range(0, len(self._payload), chunk_size):
                yield self._payload[idx : idx + chunk_size]

        def read(self, *_a: Any, **_kw: Any) -> bytes:
            return self._payload

        def close(self) -> None:
            pass

    class _FakePaginator:
        def paginate(self, **kwargs: Any) -> list[dict[str, Any]]:
            return [{"Contents": [{"Key": 'raw/test"; evil="pwned/sample.log'}]}]

    class _FakeFosClient:
        def get_paginator(self, *_a: Any, **_kw: Any) -> _FakePaginator:
            return _FakePaginator()

        def get_object(self, **_kw: Any) -> dict[str, Any]:
            return {
                "Body": _FakeFosBody(b"synthetic-fos-stream\n"),
                "ContentType": "application/gzip",
                "ContentLength": 21,
            }

    monkeypatch.setattr("backend.core.fos_client._get_fos_client", lambda source: _FakeFosClient())
    monkeypatch.setattr(
        "backend.routers.admin.downloads._fetch_file_to_zip",
        lambda source, fos, cdn, key, arcname, zf, hint: zf.writestr("sample.log", b"synthetic-log-line\n"),
    )
    r_folder = admin_client.get('/api/download-folder?root=raw&prefix=test"%3B%20evil%3D"pwned')
    assert r_folder.status_code == 200, (
        f"Expected 200 OK on /api/download-folder, got {r_folder.status_code}: {r_folder.text}"
    )
    cd_folder = r_folder.headers.get("content-disposition", "")
    assert cd_folder.startswith('attachment; filename="'), f"Unexpected Content-Disposition: {cd_folder!r}"
    assert cd_folder.count('"') == 2, (
        f"Unescaped quote injection in /api/download-folder Content-Disposition: {cd_folder!r}"
    )
    assert "\r" not in cd_folder and "\n" not in cd_folder
    assert "evil=" not in cd_folder, (
        f"Injected parameter survived in /api/download-folder Content-Disposition: {cd_folder!r}"
    )

    # Phase B2: Quote & semicolon parameter injection on 200 OK /api/download (local cache FileResponse)
    raw_dir = isolated_e2e_security_state.cache_a / "raw" / "request"
    raw_dir.mkdir(parents=True, exist_ok=True)
    local_quote_file = raw_dir / 'quote"; evil="pwned.log'
    local_quote_file.write_bytes(b"synthetic-log-entry\n")

    r_dl_local = admin_client.get('/api/download?key=raw/request/quote"%3B%20evil%3D"pwned.log')
    assert r_dl_local.status_code == 200, f"Expected 200 OK on local /api/download, got {r_dl_local.status_code}"
    cd_local = r_dl_local.headers.get("content-disposition", "")
    assert cd_local.startswith('attachment; filename="'), f"Unexpected Content-Disposition: {cd_local!r}"
    assert cd_local.count('"') == 2, (
        f"Unescaped quote injection in local /api/download Content-Disposition: {cd_local!r}"
    )
    assert "\r" not in cd_local and "\n" not in cd_local
    assert "evil=" not in cd_local, (
        f"Injected parameter survived in local /api/download Content-Disposition: {cd_local!r}"
    )

    # Phase B3: Quote & semicolon parameter injection on uncached FOS StreamingResponse branch (cdn_url="")
    def _clear_cdn_url(c: dict[str, Any]) -> None:
        c["cdn_url"] = ""
        c.setdefault("provisioning", {})["cdn_url"] = ""

    svcconfig.mutate_config("svc-a", _clear_cdn_url)
    fos_client.reload_default_source()

    fos_quote_rel = 'raw/2026-11-01/fosquote"; evil="pwned.json.gz'
    assert not (isolated_e2e_security_state.cache_a / fos_quote_rel).exists()
    r_dl_fos = admin_client.get('/api/download?key=raw/2026-11-01/fosquote"%3B%20evil%3D"pwned.json.gz')
    assert r_dl_fos.status_code == 200, (
        f"Expected 200 OK on uncached FOS /api/download, got {r_dl_fos.status_code}: {r_dl_fos.text}"
    )
    assert r_dl_fos.content == b"synthetic-fos-stream\n"
    cd_fos = r_dl_fos.headers.get("content-disposition", "")
    assert cd_fos.startswith('attachment; filename="'), f"Unexpected FOS Content-Disposition: {cd_fos!r}"
    assert cd_fos.count('"') == 2, f"Unescaped quote injection in FOS StreamingResponse Content-Disposition: {cd_fos!r}"
    assert "\r" not in cd_fos and "\n" not in cd_fos
    assert "evil=" not in cd_fos, (
        f"Injected parameter survived in FOS StreamingResponse Content-Disposition: {cd_fos!r}"
    )

    # Phase B4: Quote & semicolon parameter injection on uncached Fastly HTTPS CDN StreamingResponse branch
    def _set_fastly_cdn_url(c: dict[str, Any]) -> None:
        c["cdn_url"] = "https://cdn-svc-a.global.ssl.fastly.net"
        c.setdefault("provisioning", {})["cdn_url"] = "https://cdn-svc-a.global.ssl.fastly.net"

    svcconfig.mutate_config("svc-a", _set_fastly_cdn_url)
    fos_client.reload_default_source()

    class _FakeCdnSocket:
        def __init__(self, raw_bytes: bytes) -> None:
            self._buf = BytesIO(raw_bytes)

        def makefile(self, *_a: Any, **_kw: Any) -> BytesIO:
            return self._buf

        def close(self) -> None:
            pass

    def _cdn_200_getresponse(self: Any, *_a: Any, **_kw: Any) -> http.client.HTTPResponse:
        r = http.client.HTTPResponse(
            _FakeCdnSocket(  # type: ignore[arg-type]
                b"HTTP/1.1 200 OK\r\nContent-Type: application/gzip\r\nContent-Length: 21\r\n\r\nsynthetic-cdn-stream\n"
            )
        )
        r.begin()
        return r

    cdn_quote_rel = 'raw/2026-11-01/cdnquote"; evil="pwned.json.gz'
    assert not (isolated_e2e_security_state.cache_a / cdn_quote_rel).exists()
    assert urllib.request.urlopen is admin_helpers._STDLIB_URLOPEN
    with (
        patch.object(http.client.HTTPSConnection, "connect", lambda self: None),
        patch.object(http.client.HTTPSConnection, "request", lambda self, *_a, **_kw: None),
        patch.object(http.client.HTTPSConnection, "getresponse", _cdn_200_getresponse),
    ):
        r_dl_cdn = admin_client.get('/api/download?key=raw/2026-11-01/cdnquote"%3B%20evil%3D"pwned.json.gz')
    assert r_dl_cdn.status_code == 200, (
        f"Expected 200 OK on uncached CDN /api/download, got {r_dl_cdn.status_code}: {r_dl_cdn.text}"
    )
    assert r_dl_cdn.content == b"synthetic-cdn-stream\n"
    cd_cdn = r_dl_cdn.headers.get("content-disposition", "")
    assert cd_cdn.startswith('attachment; filename="'), f"Unexpected CDN Content-Disposition: {cd_cdn!r}"
    assert cd_cdn.count('"') == 2, f"Unescaped quote injection in CDN StreamingResponse Content-Disposition: {cd_cdn!r}"
    assert "\r" not in cd_cdn and "\n" not in cd_cdn
    assert "evil=" not in cd_cdn, (
        f"Injected parameter survived in CDN StreamingResponse Content-Disposition: {cd_cdn!r}"
    )


def test_f3_downloads_reject_unsafe_ssrf_and_redirecting_cdn_urls(
    isolated_e2e_security_state: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SEC-09c: /api/download blocks metadata cdn_url SSRF with 400 and SafeRedirectHandler blocks 302 redirects on Fastly CDN URLs."""

    # Phase A: Private/metadata cdn_url is rejected 400 BEFORE any network attempt
    def _set_metadata_cdn_url(c: dict[str, Any]) -> None:
        c["cdn_url"] = "http://169.254.169.254/latest/meta-data"
        c.setdefault("provisioning", {})["cdn_url"] = "http://169.254.169.254/latest/meta-data"

    svcconfig.mutate_config("svc-a", _set_metadata_cdn_url)
    fos_client.reload_default_source()

    called_urls: list[str] = []

    def _spy_urlopen(req: Any, *args: Any, **kwargs: Any) -> Any:
        url = req.full_url if hasattr(req, "full_url") else str(req)
        called_urls.append(url)
        raise urllib.error.URLError("blocked")

    with patch("urllib.request.urlopen", side_effect=_spy_urlopen):
        resp = make_client("127.0.0.1", headers={"X-Fastly-Service-Id": "svc-a"}).get(
            "/api/download?key=raw/request/file.log.gz"
        )
    assert resp.status_code == 400, f"Expected 400 on metadata cdn_url SSRF, got {resp.status_code}: {resp.text}"
    assert not called_urls, f"Metadata cdn_url reached urlopen: {called_urls}"

    # Phase B: Allowlisted Fastly HTTPS cdn_url returning 302 -> http://169.254.169.254/latest/meta-data/
    # Leave urllib.request.urlopen UNPATCHED (_STDLIB_URLOPEN) so _open_safe_cdn exercises SafeRedirectHandler!
    def _set_valid_cdn_url(c: dict[str, Any]) -> None:
        c["cdn_url"] = "https://cdn-svc-a.global.ssl.fastly.net"
        c.setdefault("provisioning", {})["cdn_url"] = "https://cdn-svc-a.global.ssl.fastly.net"

    svcconfig.mutate_config("svc-a", _set_valid_cdn_url)
    fos_client.reload_default_source()

    attempted_connections: list[tuple[str, str, dict[str, str]]] = []

    class _FakeRawSocket:
        def __init__(self, raw_bytes: bytes) -> None:
            self._buf = BytesIO(raw_bytes)

        def makefile(self, *_a: Any, **_kw: Any) -> BytesIO:
            return self._buf

        def close(self) -> None:
            pass

    def _https_request(
        self: Any,
        method: str,
        url: str,
        body: Any = None,
        headers: dict[str, str] | None = None,
        **_kw: Any,
    ) -> None:
        hdrs = {k.lower(): v for k, v in (headers or {}).items()}
        attempted_connections.append((self.host, url, hdrs))

    def _https_getresponse(self: Any, *_a: Any, **_kw: Any) -> http.client.HTTPResponse:
        r = http.client.HTTPResponse(
            _FakeRawSocket(  # type: ignore[arg-type]
                b"HTTP/1.1 302 Found\r\nLocation: http://169.254.169.254/latest/meta-data/\r\nContent-Length: 0\r\n\r\n"
            )
        )
        r.begin()
        return r

    def _http_request(
        self: Any,
        method: str,
        url: str,
        body: Any = None,
        headers: dict[str, str] | None = None,
        **_kw: Any,
    ) -> None:
        hdrs = {k.lower(): v for k, v in (headers or {}).items()}
        attempted_connections.append((self.host, url, hdrs))

    def _http_getresponse(self: Any, *_a: Any, **_kw: Any) -> http.client.HTTPResponse:
        r = http.client.HTTPResponse(
            _FakeRawSocket(  # type: ignore[arg-type]
                b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 6\r\n\r\nstolen"
            )
        )
        r.begin()
        return r

    assert urllib.request.urlopen is admin_helpers._STDLIB_URLOPEN, (
        "urllib.request.urlopen must remain _STDLIB_URLOPEN to exercise SafeRedirectHandler"
    )
    with (
        patch.object(http.client.HTTPSConnection, "connect", lambda self: None),
        patch.object(http.client.HTTPSConnection, "request", _https_request),
        patch.object(http.client.HTTPSConnection, "getresponse", _https_getresponse),
        patch.object(http.client.HTTPConnection, "connect", lambda self: None),
        patch.object(http.client.HTTPConnection, "request", _http_request),
        patch.object(http.client.HTTPConnection, "getresponse", _http_getresponse),
    ):
        resp_redir = make_client("127.0.0.1", headers={"X-Fastly-Service-Id": "svc-a"}).get(
            "/api/download?key=raw/request/redirect-probe.log.gz"
        )
    assert resp_redir.status_code == 502, (
        f"Expected 502 when SafeRedirectHandler blocks 302 CDN redirect, got {resp_redir.status_code}: {resp_redir.text}"
    )
    assert [h for h, _, _ in attempted_connections] == ["cdn-svc-a.global.ssl.fastly.net"], (
        f"Unexpected outbound connections during 302 redirect probe: {attempted_connections}"
    )
    assert not any("169.254.169.254" in h for h, _, _ in attempted_connections), (
        f"Redirect followed to metadata IP: {attempted_connections}"
    )


def test_f3_wellknown_bots_parameterizes_matching_uas_instead_of_sql_repr(
    ch_recorder: RecordingClickHouseClient,
) -> None:
    """SEC-07: _wellknown_bots binds matching UAs via ClickHouse query params instead of repr(u) SQL interpolation."""
    injected_bot_ua = "Mozilla/5.0 (compatible; Googlebot/2.1) ' OR 1=1 --"
    ch_recorder._http = SimpleNamespace(_transport=object())  # type: ignore[attr-defined]

    def _bot_handler(
        sql: str,
        params: dict[str, Any] | None,
        settings: dict[str, Any] | None,
    ) -> list[dict[str, Any]]:
        if "dimension='ua'" in sql:
            return [{"ua": injected_bot_ua}]
        if "FROM request_facts" in sql:
            return [{"ua": injected_bot_ua, "ip": "203.0.113.99", "cnt": 10}]
        return []

    ch_recorder.set_handler(_bot_handler)
    resp = make_client("127.0.0.1", headers={"X-Fastly-Service-Id": "svc-a"}).post(
        "/api/security/aggregates",
        json={
            "start_time": "2026-06-12T00:05:00Z",
            "end_time": "2026-06-12T00:55:00Z",
            "sections": ["wellknown_bots"],
        },
    )
    assert resp.status_code == 200, resp.text
    followup_calls = [(sql, params) for sql, params, _ in ch_recorder.calls if "FROM request_facts" in sql]
    assert followup_calls, "Expected wellknown_bots follow-up query"
    for sql, _params in followup_calls:
        assert "' OR 1=1 --" not in sql, f"Raw bot UA interpolated into SQL: {sql}"
    assert any(params and injected_bot_ua in params.values() for _, params in followup_calls), (
        f"Expected parameterized bot UA in ClickHouse query params, got {followup_calls}"
    )


def test_f3_sql_console_rewriter_preserves_string_literals_and_blocks_ddl(
    ch_recorder: RecordingClickHouseClient,
) -> None:
    """SEC-07: SQL Console query rewriter must not mutate inside '...' literals while blocking multi-statement/DDL/system escapes."""
    ch_recorder.set_handler(lambda sql, params, settings: [{"lit": "ok"}])
    client = make_client("127.0.0.1", headers={"X-Fastly-Service-Id": "svc-a"})

    literal_sql = "SELECT 'custom_fields[''a''] = 1' AS lit, 'FROM logs; DROP TABLE x' AS safe_text FROM logs LIMIT 1"
    r_ok = client.post("/api/query", json={"sql": literal_sql, "dataset": "logs"})
    assert r_ok.status_code == 200, r_ok.text
    executed_sqls = [sql for sql, _, _ in ch_recorder.calls if "FROM system.one" not in sql]
    assert executed_sqls
    # Inside the single-quoted literal, `= 1` and `FROM logs; DROP TABLE x` must NOT be mutated
    assert "custom_fields[''a''] = ''1''" not in executed_sqls[-1]
    assert "custom_fields[''a''] = '1'" not in executed_sqls[-1]
    assert "'FROM logs; DROP TABLE x'" in executed_sqls[-1]

    for forbidden_sql in (
        "SELECT 1 FROM logs; DROP TABLE request_facts",
        "DROP TABLE logs",
        "SELECT * FROM system.tables",
    ):
        r_bad = client.post("/api/query", json={"sql": forbidden_sql, "dataset": "logs"})
        assert r_bad.status_code in {400, 403, 422}, (
            f"Unsafe SQL {forbidden_sql!r} was not blocked ({r_bad.status_code})"
        )


def test_f3_cross_service_isolation_blocks_svc_a_analyst_from_svc_b_routes(
    ch_recorder: RecordingClickHouseClient,
) -> None:
    """SEC-07/SEC-08: Analyst scoped to svc-a is denied (403) across every svc-b query, CSV, view, and scoring route."""
    bundle = provision_analyst_session(
        service_ids=["svc-a"],
        mask_ips=True,
        client_ip="203.0.113.75",
    )
    client = make_client("203.0.113.75", cookies=bundle.cookies())
    hdrs_b = bundle.headers({"X-Fastly-Service-Id": "svc-b"})

    assert (
        client.post(
            "/api/query",
            headers=hdrs_b,
            json={"sql": "SELECT count() AS n FROM logs", "dataset": "logs"},
        ).status_code
        == 403
    )
    assert (
        client.post(
            "/api/high-scale/services/svc-b/queries",
            headers=hdrs_b,
            json={
                "domain": "request",
                "start_time": "2026-06-12T00:00:00Z",
                "end_time": "2026-06-12T01:00:00Z",
                "limit": 10,
            },
        ).status_code
        == 403
    )
    assert (
        client.post(
            "/api/dashboard/raw/csv",
            headers=hdrs_b,
            json={
                "start_time": "2026-06-12T00:00:00Z",
                "end_time": "2026-06-12T01:00:00Z",
                "filters": {},
                "columns": [],
            },
        ).status_code
        == 403
    )
    assert client.get("/api/views/svc-b", headers=hdrs_b).status_code == 403
    assert client.get("/api/services/svc-b/scoring/sessions/sess-1/events", headers=hdrs_b).status_code == 403
