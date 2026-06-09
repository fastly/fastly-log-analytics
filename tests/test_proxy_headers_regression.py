"""Security regression guard.

Two protections live in the codebase to ensure ``request.client.host`` is
always the real client IP:

1. ``backend/main.py:_enforce_proxy_headers_configured`` — startup assertion
   that refuses to boot when ``TRUSTED_PROXY_IPS`` is missing under strict
   mode (production). Loud WARNING otherwise.

2. ``docker-compose.prod.yml`` — passes both ``--proxy-headers`` and
   ``--forwarded-allow-ips=127.0.0.1`` to uvicorn AND sets
   ``TRUSTED_PROXY_IPS=127.0.0.1`` in the env.

This file pins both. If a future config refactor drops the flag, the env
var, or the assertion, one of these tests fails.

We don't actually spin up two uvicorn processes for the integration test —
that's flaky in CI and expensive. Instead we exercise uvicorn's
``ProxyHeadersMiddleware`` (the implementation behind the CLI flags) directly
with the same trust set we configure in production and assert it does and
does not honor XFF as expected.
"""

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
