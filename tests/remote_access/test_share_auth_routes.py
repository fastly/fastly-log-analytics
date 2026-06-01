"""Route-level tests for ``/api/share/*`` (analyst auth surface).

Verifies the wiring between the router and ``share_db`` / tunnel manager.
The middleware logic is tested in [test_middleware.py]; here we focus on
the handlers themselves — payload shapes, error envelopes, and the
session-cookie contract.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.core import share_db
from backend.utils import tunnel
from backend.utils.remote_access import RemoteAccessMiddleware


def _app() -> FastAPI:
    from backend.routers import share_auth

    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.include_router(share_auth.router)
    return app


@pytest.fixture
def client():
    with TestClient(_app()) as c:
        yield c


def _seed_invite(**overrides) -> dict:
    return share_db.create_remote_invite(
        name=overrides.get("name", "Drew"),
        email=overrides.get("email", "drew@example.com"),
        passcode=overrides.get("passcode", "ocean-breeze-cabin-42"),
        expires_at_utc=overrides.get("expires_at_utc"),
        ip_whitelist=overrides.get("ip_whitelist"),
        service_ids=overrides.get("service_ids", ["svcA"]),
    )


def _activate_share():
    tunnel.get_tunnel_manager().start_sharing(use_tunnel=False, public_endpoint="https://testserver")


# ── /api/share/login ───────────────────────────────────────────────────────


def test_login_success_sets_cookie_and_returns_session(client):
    _activate_share()
    invite = _seed_invite()
    r = client.post(
        "/api/share/login",
        json={"email": invite["email"], "passcode": "ocean-breeze-cabin-42"},
        headers={
            "X-Remote-Analyst": "1",
            "Host": "testserver",
            "Origin": "https://testserver",
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert body["email"] == invite["email"]
    assert body["session_id"]
    assert body["service_ids"] == ["svcA"]
    # cookie set
    assert "analyst_session_id" in r.headers.get("set-cookie", "")


def test_login_wrong_passcode_returns_401(client):
    _activate_share()
    _seed_invite()
    r = client.post(
        "/api/share/login",
        json={"email": "drew@example.com", "passcode": "wrong-wrong-wrong-wrong"},
        headers={
            "X-Remote-Analyst": "1",
            "Host": "testserver",
            "Origin": "https://testserver",
        },
    )
    assert r.status_code == 401
    assert r.json()["detail"]["error"] == "invalid_credentials"


def test_login_unknown_email_returns_401(client):
    _activate_share()
    r = client.post(
        "/api/share/login",
        json={"email": "ghost@nowhere.tld", "passcode": "ocean-breeze-cabin-42"},
        headers={
            "X-Remote-Analyst": "1",
            "Host": "testserver",
            "Origin": "https://testserver",
        },
    )
    assert r.status_code == 401


def test_login_ip_whitelist_blocks_offlist_ip(client):
    _activate_share()
    _seed_invite(ip_whitelist="10.0.0.0/8")
    r = client.post(
        "/api/share/login",
        json={"email": "drew@example.com", "passcode": "ocean-breeze-cabin-42"},
        headers={
            "X-Remote-Analyst": "1",
            "Host": "testserver",
            "Origin": "https://testserver",
            "X-Forwarded-For": "203.0.113.5",
        },
    )
    assert r.status_code == 403
    assert r.json()["detail"]["error"] == "ip_not_whitelisted"


def test_login_capacity_cap_blocks_when_full(client):
    _activate_share()
    a = _seed_invite(name="A", email="a@example.com", passcode="ocean-breeze-cabin-42")
    b = _seed_invite(name="B", email="b@example.com", passcode="seagull-mountain-river-77")
    # Cap floors at 1 inside get_max_concurrent_sessions, so set to 1 and
    # consume it with A first.
    share_db.set_setting("max_concurrent_analyst_sessions", "1")
    r1 = client.post(
        "/api/share/login",
        json={"email": a["email"], "passcode": "ocean-breeze-cabin-42"},
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    assert r1.status_code == 200, r1.text
    r2 = client.post(
        "/api/share/login",
        json={"email": b["email"], "passcode": "seagull-mountain-river-77"},
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    assert r2.status_code == 503
    assert r2.json()["detail"]["error"] == "capacity_exceeded"


# ── /api/share/logout ──────────────────────────────────────────────────────


def test_logout_clears_cookie_and_boots_session(client):
    _activate_share()
    invite = _seed_invite()
    r = client.post(
        "/api/share/login",
        json={"email": invite["email"], "passcode": "ocean-breeze-cabin-42"},
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    sid = r.json()["session_id"]
    client.cookies.set("analyst_session_id", sid)
    r2 = client.post(
        "/api/share/logout",
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    assert r2.status_code == 200
    assert tunnel.get_tunnel_manager().validate_session(sid) is None


# ── /api/share/acknowledge ─────────────────────────────────────────────────


def test_acknowledge_marks_tos(client):
    _activate_share()
    # Migration 002 seeds v1 — use whatever the DB currently advertises.
    tos = share_db.get_latest_tos()
    assert tos and tos["version"]
    invite = _seed_invite()
    r = client.post(
        "/api/share/login",
        json={"email": invite["email"], "passcode": "ocean-breeze-cabin-42"},
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    assert r.json()["tos_pending"] is True
    sid = r.json()["session_id"]
    client.cookies.set("analyst_session_id", sid)
    r2 = client.post(
        "/api/share/acknowledge",
        json={"version": tos["version"]},
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    assert r2.status_code == 200
    refreshed = share_db.get_remote_invite(invite["id"])
    assert refreshed["tos_version"] == tos["version"]
    assert refreshed["tos_accepted_at"] is not None


def test_acknowledge_without_session_401(client):
    _activate_share()
    r = client.post(
        "/api/share/acknowledge",
        json={"version": "2026.05"},
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    assert r.status_code == 401


# ── /api/share/heartbeat ───────────────────────────────────────────────────


def test_heartbeat_valid_session_returns_ok(client):
    _activate_share()
    invite = _seed_invite()
    r = client.post(
        "/api/share/login",
        json={"email": invite["email"], "passcode": "ocean-breeze-cabin-42"},
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    sid = r.json()["session_id"]
    client.cookies.set("analyst_session_id", sid)
    r2 = client.get(
        "/api/share/heartbeat",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r2.status_code == 200
    assert r2.json()["session_id"] == sid


def test_heartbeat_missing_session_returns_401(client):
    _activate_share()
    r = client.get(
        "/api/share/heartbeat",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r.status_code == 401


# ── /api/share/claim/{token} ───────────────────────────────────────────────


def test_claim_token_one_shot_reveal(client):
    invite = _seed_invite()
    token = share_db.create_claim_token(invite["id"], ttl_hours=1)
    # No share required — claim happens before login.
    r = client.get(f"/api/share/claim/{token}")
    assert r.status_code == 200
    body = r.json()
    assert body["email"] == invite["email"]
    # second view: token is consumed
    r2 = client.get(f"/api/share/claim/{token}")
    assert r2.status_code == 404
    assert r2.json()["detail"]["error"] == "invalid_or_used"


def test_claim_invalid_token_returns_404(client):
    r = client.get("/api/share/claim/not-a-real-token")
    assert r.status_code == 404
