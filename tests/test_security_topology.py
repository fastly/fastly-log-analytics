"""Consolidated tests for test_security_topology."""

from __future__ import annotations

import pytest

# Trust topology invariant — every test in this file pins one of the two
# protections that keep request.client.host on the real client IP.
pytestmark = pytest.mark.security_regression

import asyncio

# ── 1. Startup assertion behavior ──────────────────────────────────────────


def test_startup_assertion_warns_when_unset(monkeypatch):
    """No env vars set → warns but does not raise (local dev mode)."""
    from unittest.mock import patch

    from backend import main

    monkeypatch.delenv("TRUSTED_PROXY_IPS", raising=False)
    monkeypatch.delenv("REQUIRE_PROXY_HEADERS", raising=False)
    monkeypatch.delenv("STRICT_DATA_DIR_CHECK", raising=False)

    with patch.object(main.logging, "warning") as mock_warning:
        main._enforce_proxy_headers_configured()

    assert mock_warning.called, "expected warning when TRUSTED_PROXY_IPS unset"
    call_msg = " ".join(str(a) for a in mock_warning.call_args.args)
    assert "TRUSTED_PROXY_IPS" in call_msg


def test_startup_assertion_passes_when_set(monkeypatch):
    """With TRUSTED_PROXY_IPS set, function returns cleanly."""
    from unittest.mock import patch

    from backend import main

    monkeypatch.setenv("TRUSTED_PROXY_IPS", "127.0.0.1")
    with patch.object(main.logging, "info") as mock_info:
        # Should not raise.
        main._enforce_proxy_headers_configured()
    assert mock_info.called, "expected info log when TRUSTED_PROXY_IPS is set"
    call_msg = " ".join(str(a) for a in mock_info.call_args.args)
    assert "proxy-headers trust set" in call_msg


def test_startup_assertion_fatals_when_strict(monkeypatch):
    """In strict mode (REQUIRE_PROXY_HEADERS=1) the missing env var aborts boot."""
    from backend import main

    monkeypatch.delenv("TRUSTED_PROXY_IPS", raising=False)
    monkeypatch.setenv("REQUIRE_PROXY_HEADERS", "1")

    with pytest.raises(RuntimeError, match="TRUSTED_PROXY_IPS"):
        main._enforce_proxy_headers_configured()


def test_startup_assertion_fatals_when_strict_data_dir_check_set(monkeypatch):
    """Production sets STRICT_DATA_DIR_CHECK=1 — that should also force strict
    proxy-headers enforcement so prod can't accidentally boot without it."""
    from backend import main

    monkeypatch.delenv("TRUSTED_PROXY_IPS", raising=False)
    monkeypatch.delenv("REQUIRE_PROXY_HEADERS", raising=False)
    monkeypatch.setenv("STRICT_DATA_DIR_CHECK", "1")

    with pytest.raises(RuntimeError, match="TRUSTED_PROXY_IPS"):
        main._enforce_proxy_headers_configured()


@pytest.mark.parametrize("var", ["TRUSTED_PROXY_IPS", "UVICORN_FORWARDED_ALLOW_IPS"])
@pytest.mark.parametrize("value", ["*", "127.0.0.1,*", "*,10.0.0.0/8"])
def test_startup_assertion_rejects_wildcard_trust(monkeypatch, var, value):
    """`*` in either trust variable means uvicorn honors XFF from ANY peer —
    any direct client can stamp X-Forwarded-For: 127.0.0.1 and be classified
    loopback-admin. The guard must refuse to boot, strict mode or not."""
    from backend import main

    monkeypatch.delenv("TRUSTED_PROXY_IPS", raising=False)
    monkeypatch.delenv("UVICORN_FORWARDED_ALLOW_IPS", raising=False)
    monkeypatch.delenv("REQUIRE_PROXY_HEADERS", raising=False)
    monkeypatch.delenv("STRICT_DATA_DIR_CHECK", raising=False)
    monkeypatch.setenv(var, value)

    with pytest.raises(RuntimeError, match="wildcard"):
        main._enforce_proxy_headers_configured()


def test_startup_assertion_accepts_pinned_subnet(monkeypatch):
    """A CIDR-pinned trust set (the multipod compose topology) boots cleanly."""
    from unittest.mock import patch

    from backend import main

    monkeypatch.setenv("TRUSTED_PROXY_IPS", "172.28.0.0/16")
    monkeypatch.setenv("UVICORN_FORWARDED_ALLOW_IPS", "172.28.0.0/16")
    with patch.object(main.logging, "info") as mock_info:
        main._enforce_proxy_headers_configured()
    assert mock_info.called


# ── 2. uvicorn ProxyHeadersMiddleware end-to-end ───────────────────────────


def _run_through_proxy_middleware(*, peer_ip: str, xff: str | None, trusted_hosts: str | None):
    """Drive uvicorn's ProxyHeadersMiddleware and return the rewritten client."""
    from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

    captured: dict = {}

    async def fake_app(scope, receive, send):
        captured["client"] = scope.get("client")
        captured["scheme"] = scope.get("scheme")
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"", "more_body": False})

    headers: list[tuple[bytes, bytes]] = []
    if xff:
        headers.append((b"x-forwarded-for", xff.encode()))

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/",
        "raw_path": b"/",
        "query_string": b"",
        "headers": headers,
        "client": (peer_ip, 12345),
        "server": ("127.0.0.1", 8000),
    }

    if trusted_hosts is None:
        middleware = ProxyHeadersMiddleware(fake_app)
    else:
        middleware = ProxyHeadersMiddleware(fake_app, trusted_hosts=trusted_hosts)

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    sent: list = []

    async def send(message):
        sent.append(message)

    # asyncio.run creates a fresh event loop per call — needed under
    # python 3.13+/pytest-asyncio mode where get_event_loop() in a sync
    # test no longer auto-creates a loop and reusing one across tests
    # is order-dependent.
    asyncio.run(middleware(scope, receive, send))
    return captured


def test_proxy_headers_trusts_xff_only_from_loopback():
    """The production config (trusted_hosts="127.0.0.1") must rewrite XFF when
    the peer is 127.0.0.1 (= Caddy on this host) and IGNORE XFF when the peer
    is anything else."""
    # Loopback peer + XFF set → middleware rewrites to the XFF value.
    res = _run_through_proxy_middleware(peer_ip="127.0.0.1", xff="203.0.113.7", trusted_hosts="127.0.0.1")
    assert res["client"][0] == "203.0.113.7", (
        f"loopback peer + XFF=203.0.113.7 should rewrite client to 203.0.113.7, got {res['client']}"
    )

    # Non-loopback peer + XFF set → must NOT rewrite (untrusted XFF source).
    res = _run_through_proxy_middleware(peer_ip="8.8.8.8", xff="203.0.113.7", trusted_hosts="127.0.0.1")
    assert res["client"][0] == "8.8.8.8", (
        f"non-loopback peer should keep its peer IP regardless of XFF, got {res['client']}"
    )


def test_without_proxy_headers_xff_is_ignored_entirely():
    """Mirror of the pre-patch behavior: with no trusted_hosts configured,
    uvicorn defaults to trusting no proxy. The XFF header has no effect and
    request.client.host is the raw peer.

    This is what a deployment that DROPPED --proxy-headers would look like —
    Caddy-proxied requests would all appear as 127.0.0.1 and the IP-based
    gates would all collapse. The test proves the flag is load-bearing.
    """
    # uvicorn defaults to trusted_hosts="127.0.0.1" when constructed with no
    # args, so to simulate "dropped --proxy-headers" we explicitly pass
    # trusted_hosts="" which disables all proxy trust.
    res = _run_through_proxy_middleware(peer_ip="127.0.0.1", xff="203.0.113.7", trusted_hosts="")
    assert res["client"][0] == "127.0.0.1", f"with no proxy trust, peer IP should win, got {res['client']}"


# ── 3. Middleware-layer Host-spoof regression ──────────────────────────────


def test_admin_host_spoof_rejected_after_phase0_fix():
    """Security regression test: a remote peer (uvicorn rewrote
    request.client.host to a public IP via --proxy-headers) sending
    `Host: localhost` must NOT be treated as local-admin."""
    from fastapi import FastAPI
    from starlette.requests import Request

    from backend.utils import tunnel
    from backend.utils.remote_access import is_request_remote

    # Reset tunnel manager to ensure we don't accidentally honor the x-remote-analyst
    # fallback during this test.
    tunnel.reset_for_tests()

    async def _fake_send(_):
        pass

    async def _fake_receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    # Synthesise a request whose TCP peer is a public IP and Host header is
    # spoofed to 'localhost'. Pre-fix this would have been classified local;
    # post-fix it's remote because the peer != loopback.
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/api/dashboard",
        "headers": [(b"host", b"localhost")],
        "query_string": b"",
        "client": ("8.8.8.8", 31337),
        "app": FastAPI(),
    }
    request = Request(scope, receive=_fake_receive)
    assert is_request_remote(request) is True, "remote peer with Host:localhost must classify as remote"

    # And the inverse: a loopback peer with NO marker → admin (the only path
    # legitimate admin connections take).
    scope_admin = dict(scope)
    scope_admin["client"] = ("127.0.0.1", 31337)
    scope_admin["headers"] = [(b"host", b"localhost")]
    req_admin = Request(scope_admin, receive=_fake_receive)
    assert is_request_remote(req_admin) is False, "loopback peer + no marker = admin"


# ── Merged from test_trust_topology.py ──────────────────────────────────────────────

from pathlib import Path

import pytest

pytestmark = pytest.mark.security_regression


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(rel: str) -> str:
    return (REPO_ROOT / rel).read_text(encoding="utf-8")


# ── 1. Caddyfile trust topology ──────────────────────────────────────────────


def test_caddyfile_has_from_fastly_remote_ip_matcher():
    """The ``@from_fastly_v4`` matcher gates XFF rewrite on the TCP peer
    being in Fastly's published edge ranges. Without it, a direct
    port-80 attacker can spoof X-Forwarded-For and bypass IP-based
    gates."""
    caddyfile = _read("Caddyfile")
    assert "@from_fastly_v4" in caddyfile, "missing @from_fastly_v4 matcher"
    assert "remote_ip " in caddyfile, "missing remote_ip directive"


def test_caddyfile_rewrites_xff_only_when_peer_is_fastly():
    """The ``request_header @from_fastly_v4 X-Forwarded-For
    {http.request.header.Fastly-Client-IP}`` line is the trust-handoff
    moment — must stay scoped to the matcher."""
    caddyfile = _read("Caddyfile")
    assert "request_header @from_fastly_v4 X-Forwarded-For {http.request.header.Fastly-Client-IP}" in caddyfile, (
        "XFF rewrite missing or unscoped from @from_fastly_v4"
    )


def test_caddyfile_share_login_rate_limit_present():
    """5 share-login attempts per minute, keyed by Fastly-Client-IP.
    Bounds brute force on the share passcode."""
    caddyfile = _read("Caddyfile")
    assert "/api/share/login" in caddyfile
    assert "rate_limit @share_login" in caddyfile
    assert "events 5" in caddyfile, "share-login rate limit no longer 5 events"


def test_caddyfile_injects_proxied_by_caddy_header():
    """``X-Proxied-By-Caddy`` is the marker the frontend middleware reads
    to block /admin from anything that isn't reaching us through Caddy.
    Direct SSH-tunnel admin access has no such header → reaches /admin.
    Spoofing prevented because Caddy sets it unconditionally
    (overwriting any upstream value)."""
    caddyfile = _read("Caddyfile")
    assert 'request_header X-Proxied-By-Caddy "true"' in caddyfile


# ── 2. docker-compose.prod.yml backend hardening ─────────────────────────────


def test_compose_prod_backend_binds_loopback_only():
    """``--host 127.0.0.1`` keeps the backend off the public interface.
    Combined with the GCP/AWS/Azure firewall this is defense in depth."""
    compose = _read("docker-compose.prod.yml")
    assert '"--host",\n        "127.0.0.1",' in compose, "backend --host not loopback"


def test_compose_prod_backend_passes_proxy_headers_flag():
    """uvicorn ``--proxy-headers`` populates request.client.host from
    X-Forwarded-For (only when the TCP peer is in
    ``--forwarded-allow-ips``)."""
    compose = _read("docker-compose.prod.yml")
    assert '"--proxy-headers"' in compose


def test_compose_prod_backend_pins_forwarded_allow_ips_to_loopback():
    """``--forwarded-allow-ips=127.0.0.1`` means uvicorn only trusts XFF
    from loopback — i.e. only Caddy on the same host. Removing this
    re-opens the leftmost-XFF spoof + the admin Host-spoof bypass."""
    compose = _read("docker-compose.prod.yml")
    assert '"--forwarded-allow-ips=127.0.0.1"' in compose


def test_compose_prod_backend_has_memory_cap():
    """Container memory cap so an OOM-killer event doesn't take out the
    whole VM (sshd, caddy). Pre-2026-06-04 absence of this took down
    the host multiple times."""
    compose = _read("docker-compose.prod.yml")
    assert "mem_limit:" in compose, "backend mem_limit missing"
    assert "memswap_limit:" in compose, "backend memswap_limit missing"


def test_compose_prod_bounds_duckdb_connections_below_container_limit():
    """The single-host deployment must survive dashboard and cron overlap."""
    compose = _read("docker-compose.prod.yml")
    assert "DUCKDB_POOL_WARM_AT_BOOT=true" in compose
    assert "DUCKDB_POOL_MAX_SIZE=4" in compose
    assert "DUCKDB_POOL_CONN_MEMORY_LIMIT=${DUCKDB_POOL_CONN_MEMORY_LIMIT:-6GB}" in compose
    assert "DUCKDB_MEMORY_LIMIT=6GB" in compose
    assert "PARTIAL_HOUR_MERGE_ENABLED=${PARTIAL_HOUR_MERGE_ENABLED:-true}" in compose


# ── 3. backend/main.py middleware order (ADR-04) ─────────────────────────────


def test_middleware_order_declaration_matches_runtime():
    """``MIDDLEWARE_ORDER`` tuple in main.py matches the actual
    ``app.user_middleware`` tuple at boot. A reorder that compiles is
    not enough to ship — the boot assertion in main.py crashes start-up
    on divergence; this test catches the same drift at PR time."""
    from backend.main import MIDDLEWARE_ORDER, app

    actual = tuple(m.cls.__name__ for m in app.user_middleware)
    assert actual == MIDDLEWARE_ORDER


def test_middleware_order_is_compress_outermost_cors_innermost():
    """Spelled-out order assertion (independent of MIDDLEWARE_ORDER) so
    a refactor of the tuple constant has to face this test, not just
    rewrite the assertion target."""
    from backend.main import app

    names = tuple(m.cls.__name__ for m in app.user_middleware)
    assert names[0] == "CompressMiddleware", "Compress not outermost"
    assert names[-1] == "CORSMiddleware", "CORS not innermost"
    # The two telemetry layers sit between Compress and RemoteAccess
    assert "BaseHTTPMiddleware" in names, "telemetry decorator missing"
    assert "TelemetryResponseBodyMiddleware" in names, "telemetry body backstop missing"
    assert "RemoteAccessMiddleware" in names, "remote-access firewall missing"


def test_assert_middleware_order_crashes_on_violation():
    """``assert_middleware_order()`` must FAIL LOUDLY (RuntimeError) when
    the declared order doesn't match the actual order — the boot guard
    is only useful if it actually fires."""
    from fastapi import FastAPI
    from starlette.middleware.cors import CORSMiddleware

    from backend.main import assert_middleware_order

    bad_app = FastAPI()
    # Add CORS without anything else — won't match MIDDLEWARE_ORDER
    bad_app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    with pytest.raises(RuntimeError, match="Middleware order violation"):
        assert_middleware_order(bad_app)


# ── Merged from test_no_trace_leakage_sweep.py ──────────────────────────────────────────────

import json

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

# Trace-leakage sweep is a known-easy-to-silently-regress guard against
# returning traceback strings in JSON error responses.
pytestmark = pytest.mark.security_regression


def _make_leaky_app() -> FastAPI:
    """Build a tiny FastAPI app with two route shapes:

    - ``/leaky``: handler raises a generic exception → wrapped by
      ``query_errors`` → must NOT leak ``trace``.
    - ``/explicit-http``: raises HTTPException directly → must NOT leak
      ``trace`` either (defensive — the decorator passes these through but
      an upstream FastAPI bug could double-encode).
    """
    from backend.utils.router_utils import query_errors

    app = FastAPI()

    @app.get("/leaky")
    @query_errors(status_code=500)
    def leaky():
        raise RuntimeError("downstream said no")

    @app.get("/explicit-http")
    def explicit_http():
        raise HTTPException(status_code=500, detail={"error": "explicit"})

    return app


def test_query_errors_decorator_does_not_leak_trace():
    app = _make_leaky_app()
    with TestClient(app) as client:
        r = client.get("/leaky")
    assert r.status_code == 500, r.text
    body = r.json()
    detail = body.get("detail", body)
    assert "trace" not in json.dumps(detail).lower() or "traceback" not in r.text.lower(), (
        f"stack-trace leakage regression — response leaked traceback. body={r.text[:500]}"
    )
    # Also assert the structured detail does not include a 'trace' key
    if isinstance(detail, dict):
        assert "trace" not in detail, f"detail leaked 'trace' key: {detail}"


def test_explicit_httpexception_does_not_leak_trace():
    app = _make_leaky_app()
    with TestClient(app) as client:
        r = client.get("/explicit-http")
    assert r.status_code == 500
    body = r.json()
    detail = body.get("detail", body)
    if isinstance(detail, dict):
        assert "trace" not in detail


@pytest.mark.parametrize(
    "url",
    [
        # Routes known to be reachable without auth/state, that should
        # consistently 500 when fed garbage. The sweep doesn't need to be
        # exhaustive — it just needs broad enough coverage that any
        # regression-shaped change to query_errors is likely to be
        # exercised by at least one route in the parametrize list.
        "/api/log-fields/catalog?service_id=does-not-exist-svc-id",
    ],
)
def test_real_routes_do_not_leak_trace_on_forced_500(url):
    """Hit a handful of real routes and assert their response (regardless
    of status) does not contain a ``trace`` key or a 'Traceback' substring.

    These should respond cleanly (200 / 400 / 404) under normal conditions
    — they don't need to be 500-ing to satisfy the test. We're guarding
    against the regression in which they DO 500 + leak.
    """
    from backend.main import app

    with TestClient(app) as client:
        r = client.get(url)

    # No 'Traceback' substring anywhere in the response body — even if the
    # status code is 200/404/400, no leakage is allowed.
    assert "Traceback (most recent call last)" not in r.text, (
        f"response leaked a Python traceback for {url}; body[:500]={r.text[:500]}"
    )

    # If the response is JSON, additionally assert no detail.trace key.
    try:
        body = r.json()
    except json.JSONDecodeError:
        return
    detail = body.get("detail") if isinstance(body, dict) else None
    if isinstance(detail, dict):
        assert "trace" not in detail, f"{url} response leaked detail.trace key: {detail}"
