"""Phase 3 trust-topology + middleware-order snapshot tests.

Pins the three layers that together form the request trust chain:

1. **Caddyfile** — the ``@from_fastly_v4`` remote-IP matcher gates the
   ``X-Forwarded-For = {Fastly-Client-IP}`` rewrite on the TCP peer
   being inside Fastly's published edge ranges. The rate_limit on
   ``/api/share/login`` exists to bound brute-force on the share login.

2. **docker-compose.prod.yml** — backend uvicorn must run with
   ``--host 127.0.0.1``, ``--proxy-headers``, and
   ``--forwarded-allow-ips=127.0.0.1`` so it ONLY trusts XFF from
   loopback (i.e. only Caddy on the same host). A memory cap is set
   so an OOM-killer event doesn't take out sshd + caddy with the
   backend.

3. **backend/main.py middleware order** — declared in ``MIDDLEWARE_ORDER``
   and asserted at boot by ``assert_middleware_order()``. This file
   snapshot-tests the declaration; a reorder that compiles is no longer
   enough to ship.

A change to any of these three should be deliberate — the tests below
catch silent drift.

Tagged ``security_regression`` because every assertion below pins a
verified-fix surface (XFF spoofing, admin Host-spoof bypass, OOM
cascade, middleware-order regressions).
"""

from __future__ import annotations

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
    assert (
        "request_header @from_fastly_v4 X-Forwarded-For "
        "{http.request.header.Fastly-Client-IP}" in caddyfile
    ), "XFF rewrite missing or unscoped from @from_fastly_v4"


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
