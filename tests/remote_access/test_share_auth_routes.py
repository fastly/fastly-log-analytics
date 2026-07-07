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
    tunnel.get_tunnel_manager().start_sharing(public_endpoint="https://testserver")


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
    assert "session_id" not in body  # L1: sid is the bearer token, cookie-only
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
    sid = _sid_from_login(r)
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
    sid = _sid_from_login(r)
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
    sid = _sid_from_login(r)
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
    client.cookies.set("analyst_pending_session_id", _sid_from_login(r))

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
    sid = _sid_from_login(r)
    client.cookies.set("analyst_session_id", sid)
    r2 = client.get(
        "/api/share/heartbeat",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r2.status_code == 200
    assert r2.json()["ok"] is True  # L1: heartbeat no longer echoes session_id


def test_heartbeat_missing_session_returns_401(client):
    _activate_share()
    r = client.get(
        "/api/share/heartbeat",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r.status_code == 401


def _login_and_get_session(client):
    invite = _seed_invite()
    r = client.post(
        "/api/share/login",
        json={"email": invite["email"], "passcode": "ocean-breeze-cabin-42"},
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    sid = _sid_from_login(r)
    client.cookies.set("analyst_session_id", sid)
    return tunnel.get_tunnel_manager(), sid


def test_heartbeat_active_user_resets_idle_clock(client, monkeypatch):
    """The heartbeat is the activity channel for a quiet tab: when it reports
    genuine recent interaction (X-User-Active: 1) it bumps last_active_time so
    an active user on a dashboard with no data traffic isn't idle-logged-out."""
    _activate_share()
    mgr, _sid = _login_and_get_session(client)
    calls: list[dict] = []
    real = mgr.touch_session
    monkeypatch.setattr(mgr, "touch_session", lambda s, **kw: (calls.append(kw), real(s, **kw))[1])
    r = client.get(
        "/api/share/heartbeat",
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "X-User-Active": "1"},
    )
    assert r.status_code == 200
    assert calls, "active heartbeat (X-User-Active: 1) must touch the session"


def test_heartbeat_idle_user_does_not_reset_idle_clock(client, monkeypatch):
    """The complement: an idle heartbeat (X-User-Active: 0, or header absent)
    must NOT touch the session — otherwise a backgrounded/abandoned tab's
    heartbeats would keep it alive forever (the original bug)."""
    _activate_share()
    mgr, _sid = _login_and_get_session(client)
    calls: list[dict] = []
    real = mgr.touch_session
    monkeypatch.setattr(mgr, "touch_session", lambda s, **kw: (calls.append(kw), real(s, **kw))[1])
    for headers in (
        {"X-Remote-Analyst": "1", "Host": "testserver", "X-User-Active": "0"},
        {"X-Remote-Analyst": "1", "Host": "testserver"},  # absent
    ):
        calls.clear()
        r = client.get("/api/share/heartbeat", headers=headers)
        assert r.status_code == 200
        assert not calls, f"idle heartbeat must NOT touch the session (headers={headers})"


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


def _parse_set_cookies(response) -> dict[str, dict[str, str]]:
    """Parse a response's Set-Cookie headers into ``{name: {key: value, ...}}``.

    Uses httpx's ``headers.get_list("set-cookie")`` so the ``, `` inside
    ``expires=Tue, 17 Jun ...`` doesn't get mis-split (the prior single-string
    splitter false-flagged a date comma as a cookie boundary). Returns the most
    recent value per name; ``"_value"`` carries the raw cookie value.
    """
    out: dict[str, dict[str, str]] = {}
    headers = (
        response.headers.get_list("set-cookie")
        if hasattr(response.headers, "get_list")
        else ([response.headers.get("set-cookie", "")] if response.headers.get("set-cookie") else [])
    )
    for raw in headers:
        parts = [p.strip() for p in raw.split(";") if p.strip()]
        if not parts:
            continue
        nv = parts[0]
        if "=" not in nv:
            continue
        name, value = nv.split("=", 1)
        attrs = {"_value": value}
        for attr in parts[1:]:
            if "=" in attr:
                k, v = attr.split("=", 1)
                attrs[k.strip().lower()] = v.strip()
            else:
                attrs[attr.strip().lower()] = ""
        out[name.strip()] = attrs
    return out


def _sid_from_login(response) -> str:
    """Return the live session id from a login/acknowledge response's
    Set-Cookie headers.

    L1: the session id is no longer mirrored into the JSON body — it's the
    bearer token and lives ONLY in the httponly cookie (``analyst_pending_
    session_id`` before TOS, ``analyst_session_id`` after). Skips deletion
    cookies (Max-Age=0) so it returns the freshly-issued value.
    """
    cookies = _parse_set_cookies(response)
    for name in ("analyst_pending_session_id", "analyst_session_id"):
        c = cookies.get(name)
        if c and c.get("_value") and c.get("max-age") != "0":
            return c["_value"]
    return ""


def test_tos_pending_flow_isolation_and_upgrade(client):
    """Verify the entire TOS pending security lifecycle:
    1. Login with pending TOS sets analyst_pending_session_id ONLY — the full
       cookie must NOT be emitted (or, if a prior session was set, it must be
       explicitly deleted via Max-Age=0).
    2. Standard protected endpoints (e.g. /api/sources) return 401 unauthenticated.
    3. /api/share/heartbeat is accessible with analyst_pending_session_id.
    4. /api/share/acknowledge upgrades the cookie AND rotates the session id
       (the post-acknowledge cookie value must differ from the pre-acknowledge
       value — defense against session fixation across the TOS boundary).
    5. After upgrade, standard protected endpoints are accessible with the
       NEW session id.
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
    sid = _sid_from_login(r_login)

    # Per-cookie strict parse — the prior assertion used a permissive OR-chain
    # that matched the pending-cookie deletion's Max-Age=0 even when the full
    # cookie was simultaneously set with a 24h Max-Age (the original audit bug).
    login_cookies = _parse_set_cookies(r_login)
    assert "analyst_pending_session_id" in login_cookies, "pending cookie must be set"
    assert login_cookies["analyst_pending_session_id"]["_value"] == sid
    assert login_cookies["analyst_pending_session_id"].get("max-age") == "86400"
    # If analyst_session_id appears at all on the login response with tos_pending,
    # it MUST be a deletion (Max-Age=0) — never a live 24h cookie.
    if "analyst_session_id" in login_cookies:
        assert login_cookies["analyst_session_id"].get("max-age") == "0", (
            "pending login must NOT emit a live analyst_session_id cookie"
        )

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
    assert r_hb.json()["ok"] is True  # L1: heartbeat no longer echoes session_id

    # 4. Acknowledge TOS using analyst_pending_session_id
    r_ack = client.post(
        "/api/share/acknowledge",
        json={"version": tos["version"]},
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    assert r_ack.status_code == 200

    # Acknowledge must (a) set the full cookie, (b) delete the pending cookie,
    # AND (c) ROTATE the session id — defense against fixation across the TOS
    # boundary.
    ack_cookies = _parse_set_cookies(r_ack)
    assert "analyst_session_id" in ack_cookies
    full_attrs = ack_cookies["analyst_session_id"]
    assert full_attrs.get("max-age") == "86400"
    new_sid = full_attrs["_value"]
    assert new_sid and new_sid != sid, "session id MUST rotate at TOS acceptance"
    # Pending cookie must be explicitly deleted.
    assert "analyst_pending_session_id" in ack_cookies
    assert ack_cookies["analyst_pending_session_id"].get("max-age") == "0"

    # Apply the upgraded (rotated) cookie. Using the OLD sid here would 401 —
    # that's the whole point of rotation.
    client.cookies.clear()
    client.cookies.set("analyst_session_id", new_sid)

    # 5. After upgrade, standard endpoints should let us through (returns 404 instead of 401 because /api/sources is not in our test-only app router)
    r_sources_after = client.get(
        "/api/sources",
        params={"service": "svcA"},
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r_sources_after.status_code == 404  # Passes middleware authentication successfully!

    # 6. The OLD session id must no longer be valid against the heartbeat
    # endpoint either — both stores (in-memory + share_db) had the row deleted
    # in rotate_session_id().
    client.cookies.clear()
    client.cookies.set("analyst_session_id", sid)
    r_hb_old = client.get(
        "/api/share/heartbeat",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r_hb_old.status_code == 401, "pre-rotation session id must not be replayable"


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
    sid = _sid_from_login(r_login)

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
    assert r_hb.json()["ok"] is True  # L1: heartbeat no longer echoes session_id

    # Confirm it was restored to memory
    with mgr._lock:
        assert sid in mgr._sessions


# ── L2 / L3: login-failure counter reset + audit-email sanitisation ──────────


def test_login_success_does_not_clear_login_failures(client, monkeypatch):
    """A successful login must NOT reset the per-IP failure counter —
    otherwise an attacker with one valid credential can interleave valid
    logins to keep the counter below the lockout threshold while brute-
    forcing a victim's passcode indefinitely (finding 016)."""
    _activate_share()
    _seed_invite(email="l2@example.com")
    mgr = tunnel.get_tunnel_manager()
    rl = mgr._rate_limiter
    ip = "127.0.0.1"
    rl.record_failure(ip)
    r = client.post(
        "/api/share/login",
        json={"email": "l2@example.com", "passcode": "ocean-breeze-cabin-42"},
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    assert r.status_code == 200, r.text
    assert rl._failures.get(ip), "failure counter must persist after a successful login"


def test_safe_audit_email_sanitizes_and_bounds():
    """L3: the unauth-reachable failure paths audit-log payload.email; strip
    control chars (log forging) + cap the length before it lands in the log."""
    from backend.routers.share_auth import _safe_audit_email

    assert _safe_audit_email("a@b.com") == "a@b.com"
    assert _safe_audit_email(None) == "-"
    assert _safe_audit_email("") == "-"
    out = _safe_audit_email("a@b.com\nLOGIN_SUCCESS forged-line")
    assert "\n" not in out and "\r" not in out
    assert len(_safe_audit_email("x" * 9999)) <= 254
