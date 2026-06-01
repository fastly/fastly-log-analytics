"""Integration tests for RemoteAccessMiddleware.

Uses TestClient against the real ``backend.main.app`` with the share DB
isolated per-test. The middleware classifies requests via socket peer +
``X-Remote-Analyst`` header coordination, so tests just toggle that header
to flip between local-admin and analyst branches.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.core import share_db
from backend.utils import tunnel
from backend.utils.remote_access import RemoteAccessMiddleware

# ── Test app with a few stub routes wired through the middleware ────────────


def _build_test_app() -> FastAPI:
    """Tiny FastAPI app with the middleware + the share auth/admin routers.

    We don't load backend.main here because it brings up the scheduler/duckdb
    initialization paths. The middleware logic doesn't need them.
    """
    from backend.routers import share_admin, share_auth

    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.include_router(share_auth.router)
    app.include_router(share_admin.router)

    @app.get("/api/dashboard")
    def _dash():
        return {"ok": True}

    @app.post("/api/views")
    def _create_view():
        return {"ok": True}

    @app.get("/api/cron/stream")
    def _sse():
        return {"ok": True}

    return app


@pytest.fixture
def app():
    return _build_test_app()


@pytest.fixture
def client(app):
    with TestClient(app) as c:
        yield c


def _seed_invite(service_ids=None, ip_whitelist=None) -> dict:
    return share_db.create_remote_invite(
        name="Drew",
        email="drew@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=ip_whitelist,
        service_ids=service_ids or ["svcA", "svcB"],
    )


def _start_share():
    """Mark the tunnel manager as sharing so X-Remote-Analyst is honored."""
    mgr = tunnel.get_tunnel_manager()
    mgr.start_sharing(use_tunnel=False, public_endpoint="https://testserver")
    return mgr


def _login_analyst(client, invite, *, host="testserver") -> str:
    """Log in and return the session_id (also installs it on the TestClient
    cookie jar so subsequent calls carry it over plain HTTP)."""
    r = client.post(
        "/api/share/login",
        json={"email": invite["email"], "passcode": "ocean-breeze-cabin-42"},
        headers={
            "X-Remote-Analyst": "1",
            "Host": host,
            "Origin": f"https://{host}",
        },
    )
    assert r.status_code == 200, r.text
    sid = r.json()["session_id"]
    # TestClient cookie jar refuses `secure` cookies over http://; set it
    # manually so analyst follow-up calls carry it.
    client.cookies.set("analyst_session_id", sid)
    return sid


# ── DNS-rebinding gate ─────────────────────────────────────────────────────


def test_local_request_with_non_local_host_header_rejected(client):
    """A 127.0.0.1 connection that sends ``Host: evil.com`` is the
    DNS-rebinding shape we refuse."""
    r = client.get("/api/dashboard", headers={"Host": "evil.com"})
    assert r.status_code == 400
    assert r.json()["error"] == "host_not_allowed"


def test_local_request_with_local_host_passes(client):
    r = client.get("/api/dashboard")
    assert r.status_code == 200


def test_remote_request_unknown_host_rejected(client):
    """When sharing is active but Host doesn't match the registered endpoint."""
    _start_share()
    r = client.get(
        "/api/dashboard",
        headers={"X-Remote-Analyst": "1", "Host": "random.lhr.life"},
    )
    assert r.status_code == 400
    assert r.json()["error"] == "host_not_allowed"


# ── Local pass-through ─────────────────────────────────────────────────────


def test_local_admin_can_hit_admin_paths(client):
    """No X-Remote-Analyst header → local-admin → admin endpoints work."""
    r = client.get("/api/admin/share/status")
    assert r.status_code == 200, r.text


def test_local_admin_writes_pass_through(client):
    r = client.post("/api/views")
    assert r.status_code == 200


# ── Analyst path ───────────────────────────────────────────────────────────


def test_analyst_without_session_blocked(client):
    """No cookie → 401, never 500."""
    _start_share()
    r = client.get(
        "/api/dashboard",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r.status_code == 401
    assert r.json()["error"] == "unauthenticated"


def test_analyst_blocked_from_admin_path(client):
    _start_share()
    invite = _seed_invite()
    _login_analyst(client, invite)
    r2 = client.get(
        "/api/admin/share/status",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r2.status_code == 403
    assert r2.json()["error"] == "admin_only"


def test_analyst_blocked_from_sse_route(client):
    _start_share()
    invite = _seed_invite()
    _login_analyst(client, invite)
    r2 = client.get(
        "/api/cron/stream",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r2.status_code == 403
    assert r2.json()["error"] == "sse_blocked"


def test_analyst_read_only_blocks_writes(client):
    _start_share()
    invite = _seed_invite()
    _login_analyst(client, invite)
    r2 = client.post(
        "/api/views",
        headers={
            "X-Remote-Analyst": "1",
            "Host": "testserver",
            "Origin": "https://testserver",
        },
    )
    assert r2.status_code == 403
    assert r2.json()["error"] == "read_only"


def test_analyst_service_scope_blocks_unauthorized(client):
    _start_share()
    invite = _seed_invite(service_ids=["svcA"])
    _login_analyst(client, invite)
    r2 = client.get(
        "/api/dashboard?service=svcZ",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r2.status_code == 403
    assert r2.json()["error"] == "service_not_authorized"


def test_analyst_service_scope_allows_authorized(client):
    _start_share()
    invite = _seed_invite(service_ids=["svcA"])
    _login_analyst(client, invite)
    r2 = client.get(
        "/api/dashboard?service=svcA",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r2.status_code == 200


# ── Origin gate ────────────────────────────────────────────────────────────


def test_analyst_write_origin_must_match(client):
    """Writes from a foreign Origin (CSRF shape) are rejected."""
    _start_share()
    invite = _seed_invite()
    # Use direct share-login POST, which is allowed but goes through origin check.
    r = client.post(
        "/api/share/login",
        json={"email": invite["email"], "passcode": "ocean-breeze-cabin-42"},
        headers={
            "X-Remote-Analyst": "1",
            "Host": "testserver",
            "Origin": "https://attacker.example.com",
        },
    )
    assert r.status_code == 403
    assert r.json()["error"] == "origin_not_allowed"


# ── Login rate limiter ─────────────────────────────────────────────────────


def test_login_rate_limit_triggers_after_threshold(client):
    _start_share()
    _seed_invite()
    for _ in range(tunnel.LOGIN_FAILURE_THRESHOLD):
        client.post(
            "/api/share/login",
            json={"email": "drew@example.com", "passcode": "wrong-wrong-wrong-wrong"},
            headers={
                "X-Remote-Analyst": "1",
                "Host": "testserver",
                "Origin": "https://testserver",
            },
        )
    r = client.post(
        "/api/share/login",
        json={"email": "drew@example.com", "passcode": "ocean-breeze-cabin-42"},
        headers={
            "X-Remote-Analyst": "1",
            "Host": "testserver",
            "Origin": "https://testserver",
        },
    )
    assert r.status_code == 429
    assert r.json()["detail"]["error"] == "rate_limited"


# ── Hardening headers ──────────────────────────────────────────────────────


def test_remote_response_carries_hardening_headers(client):
    _start_share()
    invite = _seed_invite()
    r = client.post(
        "/api/share/login",
        json={"email": invite["email"], "passcode": "ocean-breeze-cabin-42"},
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    assert r.headers.get("cache-control") == "private, no-store"
    assert r.headers.get("x-frame-options") == "DENY"
    assert r.headers.get("referrer-policy") == "no-referrer"
