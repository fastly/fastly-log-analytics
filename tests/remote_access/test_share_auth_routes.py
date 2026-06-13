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


def test_acknowledge_rejects_mismatched_tos_version(client):
    """Regression for audit finding 021: previously the endpoint stored
    whatever string the client sent as ``version``, letting an analyst
    acknowledge a non-existent / outdated TOS and gain access without
    seeing the current text. The handler now validates the supplied
    version against the latest published TOS and rejects mismatches."""
    _activate_share()
    tos = share_db.get_latest_tos()
    assert tos and tos["version"]
    invite = _seed_invite()
    r = client.post(
        "/api/share/login",
        json={"email": invite["email"], "passcode": "ocean-breeze-cabin-42"},
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    sid = r.json()["session_id"]
    client.cookies.set("analyst_session_id", sid)
    r2 = client.post(
        "/api/share/acknowledge",
        json={"version": "0000.00-fabricated"},  # bogus, must not match real version
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    assert r2.status_code == 400
    assert r2.json()["detail"]["error"] == "invalid_tos_version"
    # No TOS acceptance should have been recorded.
    refreshed = share_db.get_remote_invite(invite["id"])
    assert refreshed["tos_version"] != "0000.00-fabricated"


# ── /api/share/tos ─────────────────────────────────────────────────────────


def test_get_tos_returns_current_version_with_pending_cookie(client):
    """The acknowledge page hits GET /tos with only the pending cookie set
    (login response set ``analyst_pending_session_id``, not the full one).
    The returned version must round-trip through POST /acknowledge."""
    _activate_share()
    tos = share_db.get_latest_tos()
    assert tos and tos["version"]
    invite = _seed_invite()
    r = client.post(
        "/api/share/login",
        json={"email": invite["email"], "passcode": "ocean-breeze-cabin-42"},
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    assert r.json()["tos_pending"] is True
    # Simulate the real cookie state after login: only the pending cookie is set.
    client.cookies.clear()
    client.cookies.set("analyst_pending_session_id", r.json()["session_id"])

    r2 = client.get(
        "/api/share/tos",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r2.status_code == 200, r2.text
    body = r2.json()
    assert body["version"] == tos["version"]
    assert body["text"] == tos["text"]

    # The version we just fetched must satisfy /acknowledge.
    r3 = client.post(
        "/api/share/acknowledge",
        json={"version": body["version"]},
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    assert r3.status_code == 200, r3.text
    refreshed = share_db.get_remote_invite(invite["id"])
    assert refreshed["tos_version"] == tos["version"]


def test_get_tos_without_session_returns_401(client):
    _activate_share()
    r = client.get(
        "/api/share/tos",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
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
    r = client.post(f"/api/share/claim/{token}")
    assert r.status_code == 200
    body = r.json()
    assert body["email"] == invite["email"]
    # second view: token is consumed
    r2 = client.post(f"/api/share/claim/{token}")
    assert r2.status_code == 404
    assert r2.json()["detail"]["error"] == "invalid_or_used"


def test_claim_invalid_token_returns_404(client):
    r = client.post("/api/share/claim/not-a-real-token")
    assert r.status_code == 404


# ── Terms of Service Cookie Isolation and Upgrade ──────────────────────────


def test_tos_pending_flow_isolation_and_upgrade(client):
    """Verify the entire TOS pending security lifecycle:
    1. Login with pending TOS sets analyst_pending_session_id.
    2. Standard protected endpoints (e.g. /api/sources) return 401 unauthenticated.
    3. /api/share/heartbeat is accessible with analyst_pending_session_id.
    4. /api/share/acknowledge works with analyst_pending_session_id and upgrades the cookie.
    5. After upgrade, standard protected endpoints are accessible.
    """
    _activate_share()
    tos = share_db.get_latest_tos()
    assert tos and tos["version"]
    invite = _seed_invite()

    # 1. Login with pending TOS
    r_login = client.post(
        "/api/share/login",
        json={"email": invite["email"], "passcode": "ocean-breeze-cabin-42"},
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    assert r_login.status_code == 200
    assert r_login.json()["tos_pending"] is True
    sid = r_login.json()["session_id"]

    # Inspect set-cookie header for pending cookie and absence of full cookie
    cookies_header = r_login.headers.get("set-cookie", "")
    assert "analyst_pending_session_id=" in cookies_header
    assert "analyst_session_id=" not in cookies_header or "Max-Age=0" in cookies_header or "expires=" in cookies_header

    # Let's set the pending cookie on the client and clear any other
    client.cookies.clear()
    client.cookies.set("analyst_pending_session_id", sid)

    # 2. Try to access a protected analyst endpoint (e.g. /api/sources)
    # The middleware should reject this because we don't have the full analyst_session_id cookie
    r_sources = client.get(
        "/api/sources",
        params={"service": "svcA"},
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r_sources.status_code == 401
    assert r_sources.json()["error"] == "unauthenticated"

    # 3. Heartbeat should still work
    r_hb = client.get(
        "/api/share/heartbeat",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r_hb.status_code == 200
    assert r_hb.json()["session_id"] == sid

    # 4. Acknowledge TOS using analyst_pending_session_id
    r_ack = client.post(
        "/api/share/acknowledge",
        json={"version": tos["version"]},
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    assert r_ack.status_code == 200

    # Acknowledge response must set the full cookie and delete the pending cookie
    ack_cookies = r_ack.headers.get("set-cookie", "")
    assert "analyst_session_id=" in ack_cookies
    assert "analyst_pending_session_id=" in ack_cookies  # Contains both because it deletes pending (Max-Age=0)

    # Apply the upgraded cookie to the client and remove the pending one
    client.cookies.clear()
    client.cookies.set("analyst_session_id", sid)

    # 5. After upgrade, standard endpoints should let us through (returns 404 instead of 401 because /api/sources is not in our test-only app router)
    r_sources_after = client.get(
        "/api/sources",
        params={"service": "svcA"},
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r_sources_after.status_code == 404  # Passes middleware authentication successfully!


def test_on_demand_session_rehydration(client):
    """Verify that if a session exists in share_db but is missing from
    the TunnelManager in-memory _sessions dictionary (simulating a request
    landing on a different backend worker process), it is successfully
    rehydrated on-demand during validation.
    """
    from backend.utils.tunnel import get_tunnel_manager

    _activate_share()
    invite = _seed_invite()

    # 1. Login to create the session
    r_login = client.post(
        "/api/share/login",
        json={"email": invite["email"], "passcode": "ocean-breeze-cabin-42"},
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    assert r_login.status_code == 200
    sid = r_login.json()["session_id"]

    # 2. Simulate worker boundary by clearing the session from TunnelManager memory
    mgr = get_tunnel_manager()
    with mgr._lock:
        assert sid in mgr._sessions
        del mgr._sessions[sid]  # Simulate an empty/different worker process cache

    # 3. Requesting heartbeat should trigger on-demand rehydration from SQLite
    client.cookies.clear()
    client.cookies.set("analyst_pending_session_id", sid)
    r_hb = client.get(
        "/api/share/heartbeat",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r_hb.status_code == 200
    assert r_hb.json()["session_id"] == sid

    # Confirm it was restored to memory
    with mgr._lock:
        assert sid in mgr._sessions
