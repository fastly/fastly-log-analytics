from __future__ import annotations

"""Route-level tests for ``/api/admin/share/*`` and ``/api/share/*``."""

import pytest
from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.testclient import TestClient

from backend.core import share_db
from backend.routers import share_admin, share_auth
from backend.routers.share_admin import build_share_live_payload, build_share_status
from backend.utils import tunnel
from backend.utils.tunnel.manager import AnalystSession, get_tunnel_manager
from tests.remote_access import conftest as H

_ANALYST_HEADERS = {"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"}
_ANALYST_GET_HEADERS = {"X-Remote-Analyst": "1", "Host": "testserver"}
_DEFAULT_PASSCODE = "ocean-breeze-cabin-42"


def _app() -> FastAPI:
    return H.make_remote_access_app(share_admin.router, share_auth.router)


@pytest.fixture
def client():
    with TestClient(_app()) as c:
        yield c


def _seed_invite(**overrides) -> dict:
    return share_db.create_remote_invite(
        name=overrides.get("name", "Drew"),
        email=overrides.get("email", "drew@example.com"),
        passcode=overrides.get("passcode", _DEFAULT_PASSCODE),
        expires_at_utc=overrides.get("expires_at_utc"),
        ip_whitelist=overrides.get("ip_whitelist"),
        service_ids=overrides.get("service_ids", ["svcA"]),
    )


def _create_invite_via_api(client, **extra) -> dict:
    payload = {
        "name": "Drew",
        "email": "drew@example.com",
        "passcode": _DEFAULT_PASSCODE,
        "service_ids": ["svcA"],
        **extra,
    }
    r = client.post("/api/admin/share/invites", json=payload)
    assert r.status_code == 200, r.text
    return r.json()


def _activate_share():
    tunnel.get_tunnel_manager().start_sharing(public_endpoint="https://testserver")


def _parse_set_cookies(response) -> dict[str, dict[str, str]]:
    """Parse Set-Cookie headers into ``{name: {key: value, ...}}``."""
    out: dict[str, dict[str, str]] = {}
    headers = (
        response.headers.get_list("set-cookie")
        if hasattr(response.headers, "get_list")
        else ([response.headers.get("set-cookie", "")] if response.headers.get("set-cookie") else [])
    )
    for raw in headers:
        parts = [p.strip() for p in raw.split(";") if p.strip()]
        if not parts or "=" not in parts[0]:
            continue
        name, value = parts[0].split("=", 1)
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
    """Return the live session id from a login/acknowledge response's Set-Cookie headers."""
    cookies = _parse_set_cookies(response)
    for name in ("analyst_pending_session_id", "analyst_session_id"):
        c = cookies.get(name)
        if c and c.get("_value") and c.get("max-age") != "0":
            return c["_value"]
    return ""


def _login_and_get_session(client):
    invite = _seed_invite()
    r = client.post(
        "/api/share/login", json={"email": invite["email"], "passcode": _DEFAULT_PASSCODE}, headers=_ANALYST_HEADERS
    )
    sid = _sid_from_login(r)
    client.cookies.set("analyst_session_id", sid)
    return tunnel.get_tunnel_manager(), sid


# ── /status & /audit-logs ──────────────────────────────────────────────────


def test_status_returns_expected_keys(client):
    r = client.get("/api/admin/share/status")
    assert r.status_code == 200, r.text
    body = r.json()
    expected_keys = (
        "sharing_active",
        "public_endpoint",
        "public_url",
        "active_session_count",
        "max_concurrent_sessions",
        "invites",
        "sessions",
        "audit_logs",
        "services",
        "rate_limits",
        "telemetry",
    )
    for key in expected_keys:
        assert key in body, f"missing {key}"
    assert body["sharing_active"] is False
    assert body["active_session_count"] == 0
    assert "failures" in body["rate_limits"] and "lockouts" in body["rate_limits"]
    assert "heartbeat_unauth_count" in body["telemetry"]


def test_audit_logs_endpoint_returns_filtered_rows_and_rejects_bad_limit(client):
    share_db.log_share_audit_event(
        event_type="LOGIN_FAIL", email="alice@example.com", ip_address="1.1.1.1", details="x"
    )
    share_db.log_share_audit_event(
        event_type="LOGIN_SUCCESS", email="alice@example.com", ip_address="1.1.1.1", details="x"
    )
    r = client.get("/api/admin/share/audit-logs", params={"event_type": "LOGIN_FAIL"})
    assert r.status_code == 200
    body = r.json()
    assert "audit_logs" in body
    assert all(row["event_type"] == "LOGIN_FAIL" for row in body["audit_logs"])
    assert client.get("/api/admin/share/audit-logs", params={"limit": 0}).status_code == 422


@pytest.mark.parametrize(
    ("name", "email", "log_events", "expect_non_null"),
    [
        ("Alice", "alice@example.com", [("LOGIN_SUCCESS", "1.1.1.1"), ("LOGIN_FAIL", "1.1.1.1")], True),
        ("Bob", "bob@example.com", [], False),
    ],
)
def test_status_invites_last_login_derivation(client, name, email, log_events, expect_non_null):
    inv = _seed_invite(name=name, email=email, service_ids=[])
    for ev_type, ip in log_events:
        share_db.log_share_audit_event(event_type=ev_type, email=email, ip_address=ip, details="x")
    body = client.get("/api/admin/share/status").json()
    row = next(i for i in body["invites"] if i["id"] == inv["id"])
    assert (row["last_login_at"] is not None) is expect_non_null


# ── /start /stop /panic ────────────────────────────────────────────────────


def test_start_stop_and_panic(client):
    r_bad = client.post("/api/admin/share/start", json={"public_endpoint": "http://example.com"})
    assert r_bad.status_code == 400
    assert r_bad.json()["detail"]["error"] == "invalid_request"

    r = client.post("/api/admin/share/start", json={"public_endpoint": "https://share.example.com"})
    assert r.status_code == 200, r.text
    assert r.json()["public_url"] == "https://share.example.com"
    assert client.post("/api/admin/share/stop").status_code == 200

    r_panic = client.post("/api/admin/share/panic")
    assert r_panic.status_code == 200
    assert r_panic.json().get("sessions_booted") == 0


# ── /invites ───────────────────────────────────────────────────────────────


def test_create_invite_happy_path_and_concurrent_session_flags(client):
    body = _create_invite_via_api(client, duration_hours=24)
    assert body["email"] == "drew@example.com"
    assert "id" in body
    assert body["allow_concurrent_sessions"] is False

    body_concurrent = _create_invite_via_api(client, email="drew2@example.com", allow_concurrent_sessions=True)
    assert body_concurrent["allow_concurrent_sessions"] is True


@pytest.mark.parametrize(
    ("overrides", "expected_error"),
    [
        ({"passcode": "shrt"}, "weak_passcode"),
        ({"name": "<script>"}, "invalid_input"),
        ({"service_ids": []}, "invalid_request"),
    ],
)
def test_create_invite_validation_errors(client, overrides, expected_error):
    payload = {
        "name": "Drew",
        "email": "drew@example.com",
        "passcode": _DEFAULT_PASSCODE,
        "service_ids": ["svcA"],
        **overrides,
    }
    r = client.post("/api/admin/share/invites", json=payload)
    assert r.status_code == 400
    assert r.json()["detail"]["error"] == expected_error


def test_update_invite_services_and_empty_rejected(client):
    invite_id = _create_invite_via_api(client)["id"]
    r2 = client.patch(f"/api/admin/share/invites/{invite_id}/services", json={"service_ids": ["svcA", "svcB"]})
    assert r2.status_code == 200
    assert sorted(r2.json()["service_ids"]) == ["svcA", "svcB"]

    r_empty = client.patch(f"/api/admin/share/invites/{invite_id}/services", json={"service_ids": []})
    assert r_empty.status_code == 400
    assert r_empty.json()["detail"]["error"] == "invalid_request"


def test_update_invite_pii_and_sharing_toggles(client):
    inv = _create_invite_via_api(client, pii_policy={"mask_ips": False})
    invite_id = inv["id"]
    assert inv["pii_policy"]["mask_ips"] is False
    assert inv["allow_concurrent_sessions"] is False

    r_pii = client.patch(f"/api/admin/share/invites/{invite_id}/pii", json={"mask_ips": True})
    assert r_pii.status_code == 200, r_pii.text
    assert r_pii.json()["pii_policy"]["mask_ips"] is True

    r_sh1 = client.patch(f"/api/admin/share/invites/{invite_id}/sharing", json={"allow_concurrent_sessions": True})
    assert r_sh1.status_code == 200 and r_sh1.json()["allow_concurrent_sessions"] is True
    r_sh2 = client.patch(f"/api/admin/share/invites/{invite_id}/sharing", json={"allow_concurrent_sessions": False})
    assert r_sh2.status_code == 200 and r_sh2.json()["allow_concurrent_sessions"] is False


def test_update_invite_passcode_happy_and_weak_rejected(client):
    invite = _seed_invite(email="drew-passcode@example.com")
    r_weak = client.patch(f"/api/admin/share/invites/{invite['id']}/passcode", json={"passcode": "weak"})
    assert r_weak.status_code == 400

    r = client.patch(
        f"/api/admin/share/invites/{invite['id']}/passcode",
        json={"passcode": "river-stone-mountain-99"},
    )
    assert r.status_code == 200, r.text
    assert r.json()["ok"] is True

    refreshed = share_db.get_remote_invite(invite["id"])
    assert not share_db.verify_passcode(_DEFAULT_PASSCODE, refreshed["passcode"])
    assert share_db.verify_passcode("river-stone-mountain-99", refreshed["passcode"])


def test_revoke_delete_and_issue_claim_token_happy_paths(client):
    inv1 = _seed_invite(email="drew@example.com")
    r_claim = client.post(f"/api/admin/share/invites/{inv1['id']}/claim-token")
    assert r_claim.status_code == 200 and len(r_claim.json()["token"]) >= 16

    r_rev = client.post(f"/api/admin/share/invites/{inv1['id']}/revoke")
    assert r_rev.status_code == 200 and r_rev.json()["ok"] is True

    inv2 = _seed_invite(email="drew-delete@example.com")
    r_del = client.delete(f"/api/admin/share/invites/{inv2['id']}")
    assert r_del.status_code == 200 and r_del.json()["ok"] is True
    assert share_db.get_remote_invite(inv2["id"]) is None


@pytest.mark.parametrize(
    ("method", "path", "payload"),
    [
        ("PATCH", "/api/admin/share/invites/no-such-invite/services", {"service_ids": ["svcA"]}),
        ("PATCH", "/api/admin/share/invites/no-such-invite/pii", {"mask_ips": True}),
        ("PATCH", "/api/admin/share/invites/no-such-invite/sharing", {"allow_concurrent_sessions": True}),
        ("PATCH", "/api/admin/share/invites/no-such-invite/passcode", {"passcode": _DEFAULT_PASSCODE}),
        ("POST", "/api/admin/share/invites/missing/revoke", None),
        ("DELETE", "/api/admin/share/invites/missing", None),
        ("POST", "/api/admin/share/invites/missing/claim-token", None),
    ],
)
def test_unknown_invite_routes_return_404(client, method, path, payload):
    r = client.request(method, path, json=payload) if payload is not None else client.request(method, path)
    assert r.status_code == 404


# ── Backup, GDPR, Settings, Wordphrase ─────────────────────────────────────


def test_backup_export_import_round_trip_and_errors(client):
    r_weak = client.post("/api/admin/share/backup/export", json={"passphrase": "short"})
    assert r_weak.status_code == 400 and r_weak.json()["detail"]["error"] == "weak_passphrase"

    _seed_invite()
    r = client.post("/api/admin/share/backup/export", json={"passphrase": "correct-horse-battery-staple"})
    assert r.status_code == 200 and len(r.content) > 100
    blob = r.content

    r_bad_mode = client.post(
        "/api/admin/share/backup/import",
        data={"passphrase": "correct-horse-battery-staple", "mode": "merge-all-the-things"},
        files={"file": ("backup.enc", b"junk", "application/octet-stream")},
    )
    assert r_bad_mode.status_code == 400 and r_bad_mode.json()["detail"]["error"] == "invalid_mode"

    r_bad_pass = client.post(
        "/api/admin/share/backup/import",
        data={"passphrase": "wrong-horse-battery-staple", "mode": "skip-collisions"},
        files={"file": ("backup.enc", blob, "application/octet-stream")},
    )
    assert r_bad_pass.status_code == 400 and r_bad_pass.json()["detail"]["error"] == "import_failed"

    r2 = client.post(
        "/api/admin/share/backup/import",
        data={"passphrase": "correct-horse-battery-staple", "mode": "skip-collisions"},
        files={"file": ("backup.enc", blob, "application/octet-stream")},
    )
    assert r2.status_code == 200, r2.text
    assert isinstance(r2.json(), dict)
    assert share_db.get_remote_invite_by_email_passcode("drew@example.com", _DEFAULT_PASSCODE) is not None


def test_gdpr_erase_settings_and_wordphrase(client):
    _seed_invite()
    r_erase = client.post("/api/admin/share/gdpr/erase", json={"email": "drew@example.com", "reason": "user request"})
    assert r_erase.status_code == 200, r_erase.text
    assert share_db.get_remote_invite_by_email_passcode("drew@example.com", _DEFAULT_PASSCODE) is None

    r_blank = client.post("/api/admin/share/gdpr/erase", json={"email": "", "reason": "user request"})
    assert r_blank.status_code == 400 and r_blank.json()["detail"]["error"] == "invalid_request"

    r_cap = client.patch("/api/admin/share/settings", json={"max_concurrent_analyst_sessions": 25})
    assert r_cap.status_code == 200 and r_cap.json()["max_concurrent_analyst_sessions"] == 25
    assert client.patch("/api/admin/share/settings", json={"max_concurrent_analyst_sessions": 0}).status_code == 400

    r_wp = client.get("/api/admin/share/wordphrase")
    assert r_wp.status_code == 200
    passcode = r_wp.json()["passcode"]
    assert isinstance(passcode, str) and len(passcode) >= 16 and "-" in passcode


# ── share-live freshness & wire contracts ──────────────────────────────────


def test_dedicated_stream_route_removed_and_live_payload_shape(client):
    app = FastAPI()
    app.include_router(share_admin.router)
    paths = app.openapi()["paths"]
    assert "/api/admin/share/stream" not in paths
    assert "/api/admin/share/live" in paths

    payload = build_share_live_payload()
    for key in ("sharing_active", "public_url", "active_session_count", "rate_limits", "telemetry"):
        assert key in payload, f"missing {key}"


def _seed_status_fixtures():
    _seed_invite(name="Carol", email="carol@example.com", service_ids=["svc-a"])
    share_db.log_share_audit_event(event_type="LOGIN_FAIL", email=None, ip_address="2.2.2.2", details="d")
    mgr = get_tunnel_manager()
    mgr._sessions["sess-1"] = AnalystSession(
        session_id="sess-1",
        invite_id="inv-1",
        name="Carol",
        email="carol@example.com",
        ip_address="2.2.2.2",
        user_agent="ua",
        fingerprint_signature="fp",
        pii_policy={"mask_ips": True},
        query_window_hours=None,
        query_start_time=None,
        query_end_time=None,
        login_time="2026-01-01T00:00:00Z",
        last_active_time="2026-01-01T00:01:00Z",
        service_ids=["svc-a"],
        auth_method="oauth",
        oauth_provider="google",
    )
    mgr._tunnel_uptime_history.append(
        {"started_at": "2026-01-01T00:00:00Z", "ended_at": "2026-01-01T00:05:00Z", "duration_s": 300, "reason": "stop"}
    )
    mgr.record_login_failure("9.9.9.9", None)
    mgr.record_heartbeat_unauth()


def test_status_live_and_audit_logs_match_producer(client):
    _seed_status_fixtures()
    expected = jsonable_encoder(build_share_status())
    body = client.get("/api/admin/share/status").json()

    assert body == expected
    assert [list(s) for s in body["sessions"]] == [list(s) for s in expected["sessions"]]
    assert body["sessions"][0]["auth_method"] == "oauth"
    assert body["rate_limits"]["failures"][0]["ip"] == "9.9.9.9"
    assert body["telemetry"]["heartbeat_unauth_count"] == 1
    assert body["telemetry"]["tunnel_uptime_history"][0]["duration_s"] == 300
    assert body["audit_logs"] and set(body["audit_logs"][0]) >= {"id", "timestamp", "event_type", "email"}

    assert client.get("/api/admin/share/live").json() == jsonable_encoder(build_share_live_payload())
    assert client.get("/api/admin/share/audit-logs").json()["audit_logs"] == jsonable_encoder(
        share_db.get_share_audit_logs(limit=200)
    )


def test_invite_revoked_is_integer_flag_on_the_wire(client):
    inv = _seed_invite(name="Dan", email="dan@example.com", service_ids=[])
    row = next(i for i in client.get("/api/admin/share/status").json()["invites"] if i["id"] == inv["id"])
    assert row["revoked"] == 0 and type(row["revoked"]) is int
    assert share_db.revoke_remote_invite(inv["id"])
    row = next(i for i in client.get("/api/admin/share/status").json()["invites"] if i["id"] == inv["id"])
    assert row["revoked"] == 1 and type(row["revoked"]) is int


# ── /api/share/login & /logout ─────────────────────────────────────────────


def test_login_success_sets_cookie_and_logout_clears_session(client):
    _activate_share()
    invite = _seed_invite()
    r = client.post(
        "/api/share/login",
        json={"email": invite["email"], "passcode": _DEFAULT_PASSCODE},
        headers=_ANALYST_HEADERS,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert body["email"] == invite["email"]
    assert "session_id" not in body
    assert body["service_ids"] == ["svcA"]
    cookie = r.headers.get("set-cookie", "")
    assert "analyst_session_id" in cookie and "secure" in cookie.lower()

    sid = _sid_from_login(r)
    client.cookies.set("analyst_session_id", sid)
    r2 = client.post("/api/share/logout", headers=_ANALYST_HEADERS)
    assert r2.status_code == 200
    assert tunnel.get_tunnel_manager().validate_session(sid) is None


@pytest.mark.parametrize(
    ("seed_kw", "login_body", "extra_headers", "expected_status", "expected_error"),
    [
        ({}, {"email": "drew@example.com", "passcode": "wrong-wrong-wrong-wrong"}, {}, 401, "invalid_credentials"),
        (None, {"email": "ghost@nowhere.tld", "passcode": _DEFAULT_PASSCODE}, {}, 401, None),
        (
            {"ip_whitelist": "10.0.0.0/8"},
            {"email": "drew@example.com", "passcode": _DEFAULT_PASSCODE},
            {"X-Forwarded-For": "203.0.113.5"},
            403,
            "ip_not_whitelisted",
        ),
    ],
)
def test_login_rejection_cases(client, seed_kw, login_body, extra_headers, expected_status, expected_error):
    _activate_share()
    if seed_kw is not None:
        _seed_invite(**seed_kw)
    r = client.post("/api/share/login", json=login_body, headers={**_ANALYST_HEADERS, **extra_headers})
    assert r.status_code == expected_status
    if expected_error:
        assert r.json()["detail"]["error"] == expected_error


def test_login_capacity_cap_blocks_when_full(client):
    _activate_share()
    a = _seed_invite(name="A", email="a@example.com", passcode=_DEFAULT_PASSCODE)
    b = _seed_invite(name="B", email="b@example.com", passcode="seagull-mountain-river-77")
    share_db.set_setting("max_concurrent_analyst_sessions", "1")
    r1 = client.post(
        "/api/share/login", json={"email": a["email"], "passcode": _DEFAULT_PASSCODE}, headers=_ANALYST_HEADERS
    )
    assert r1.status_code == 200, r1.text
    r2 = client.post(
        "/api/share/login",
        json={"email": b["email"], "passcode": "seagull-mountain-river-77"},
        headers=_ANALYST_HEADERS,
    )
    assert r2.status_code == 503 and r2.json()["detail"]["error"] == "capacity_exceeded"


# ── /api/share/acknowledge & /tos ──────────────────────────────────────────


def test_acknowledge_marks_tos_and_validates_version_and_session(client):
    _activate_share()
    assert (
        client.post("/api/share/acknowledge", json={"version": "2026.05"}, headers=_ANALYST_HEADERS).status_code == 401
    )
    assert client.get("/api/share/tos", headers=_ANALYST_GET_HEADERS).status_code == 401

    tos = share_db.get_latest_tos()
    assert tos and tos["version"]
    invite = _seed_invite()
    r = client.post(
        "/api/share/login", json={"email": invite["email"], "passcode": _DEFAULT_PASSCODE}, headers=_ANALYST_HEADERS
    )
    assert r.json()["tos_pending"] is True
    sid = _sid_from_login(r)
    client.cookies.set("analyst_session_id", sid)

    # Mismatched version must fail with 400 invalid_tos_version (finding 021).
    r_bad = client.post("/api/share/acknowledge", json={"version": "0000.00-fabricated"}, headers=_ANALYST_HEADERS)
    assert r_bad.status_code == 400 and r_bad.json()["detail"]["error"] == "invalid_tos_version"
    assert share_db.get_remote_invite(invite["id"])["tos_version"] != "0000.00-fabricated"

    # Matching version succeeds.
    r2 = client.post("/api/share/acknowledge", json={"version": tos["version"]}, headers=_ANALYST_HEADERS)
    assert r2.status_code == 200
    refreshed = share_db.get_remote_invite(invite["id"])
    assert refreshed["tos_version"] == tos["version"] and refreshed["tos_accepted_at"] is not None


def test_get_tos_returns_current_version_with_pending_cookie(client):
    _activate_share()
    tos = share_db.get_latest_tos()
    assert tos and tos["version"]
    invite = _seed_invite()
    r = client.post(
        "/api/share/login", json={"email": invite["email"], "passcode": _DEFAULT_PASSCODE}, headers=_ANALYST_HEADERS
    )
    assert r.json()["tos_pending"] is True
    client.cookies.clear()
    client.cookies.set("analyst_pending_session_id", _sid_from_login(r))

    r2 = client.get("/api/share/tos", headers=_ANALYST_GET_HEADERS)
    assert r2.status_code == 200, r2.text
    body = r2.json()
    assert body["version"] == tos["version"] and body["text"] == tos["text"]

    r3 = client.post("/api/share/acknowledge", json={"version": body["version"]}, headers=_ANALYST_HEADERS)
    assert r3.status_code == 200, r3.text
    assert share_db.get_remote_invite(invite["id"])["tos_version"] == tos["version"]


# ── /api/share/heartbeat & /claim/{token} ──────────────────────────────────


def test_heartbeat_session_and_activity_tracking(client, monkeypatch):
    _activate_share()
    assert client.get("/api/share/heartbeat", headers=_ANALYST_GET_HEADERS).status_code == 401

    mgr, _sid = _login_and_get_session(client)
    r_ok = client.get("/api/share/heartbeat", headers=_ANALYST_GET_HEADERS)
    assert r_ok.status_code == 200 and r_ok.json()["ok"] is True

    calls: list[dict] = []
    real = mgr.touch_session
    monkeypatch.setattr(mgr, "touch_session", lambda s, **kw: (calls.append(kw), real(s, **kw))[1])

    # Active user resets idle clock.
    r_act = client.get("/api/share/heartbeat", headers={**_ANALYST_GET_HEADERS, "X-User-Active": "1"})
    assert r_act.status_code == 200 and calls

    # Idle user (X-User-Active: 0 or absent) does NOT touch session.
    for hdrs in ({**_ANALYST_GET_HEADERS, "X-User-Active": "0"}, _ANALYST_GET_HEADERS):
        calls.clear()
        r_idle = client.get("/api/share/heartbeat", headers=hdrs)
        assert r_idle.status_code == 200 and not calls


def test_claim_token_one_shot_invalid_and_revoked(client):
    assert client.post("/api/share/claim/not-a-real-token").status_code == 404

    invite = _seed_invite()
    token = share_db.create_claim_token(invite["id"], ttl_hours=1)
    r = client.post(f"/api/share/claim/{token}")
    assert r.status_code == 200 and r.json()["email"] == invite["email"]
    r2 = client.post(f"/api/share/claim/{token}")
    assert r2.status_code == 404 and r2.json()["detail"]["error"] == "invalid_or_used"

    rev_invite = _seed_invite(email="revoked-claim@example.com")
    rev_token = share_db.create_claim_token(rev_invite["id"], ttl_hours=1)
    share_db.revoke_remote_invite(rev_invite["id"])
    r_rev = client.post(f"/api/share/claim/{rev_token}", headers={"X-Page-Load-ID": "pl-claim-410"})
    assert r_rev.status_code == 410 and r_rev.json()["detail"]["error"] == "invite_gone"


# ── TOS Cookie Isolation, Upgrade, Rehydration & Brute-Force Protection ────


def test_tos_pending_flow_isolation_and_upgrade(client):
    _activate_share()
    tos = share_db.get_latest_tos()
    assert tos and tos["version"]
    invite = _seed_invite()

    # 1. Login with pending TOS sets analyst_pending_session_id ONLY.
    r_login = client.post(
        "/api/share/login", json={"email": invite["email"], "passcode": _DEFAULT_PASSCODE}, headers=_ANALYST_HEADERS
    )
    assert r_login.status_code == 200 and r_login.json()["tos_pending"] is True
    sid = _sid_from_login(r_login)

    login_cookies = _parse_set_cookies(r_login)
    assert "analyst_pending_session_id" in login_cookies
    assert login_cookies["analyst_pending_session_id"]["_value"] == sid
    assert login_cookies["analyst_pending_session_id"].get("max-age") == "86400"
    if "analyst_session_id" in login_cookies:
        assert login_cookies["analyst_session_id"].get("max-age") == "0"

    client.cookies.clear()
    client.cookies.set("analyst_pending_session_id", sid)

    # 2. Protected endpoints reject pending cookie with 401.
    r_sources = client.get("/api/sources", params={"service": "svcA"}, headers=_ANALYST_GET_HEADERS)
    assert r_sources.status_code == 401 and r_sources.json()["error"] == "unauthenticated"

    # 3. Heartbeat works with pending cookie.
    r_hb = client.get("/api/share/heartbeat", headers=_ANALYST_GET_HEADERS)
    assert r_hb.status_code == 200 and r_hb.json()["ok"] is True

    # 4. Acknowledge TOS upgrades cookie and rotates session id.
    r_ack = client.post("/api/share/acknowledge", json={"version": tos["version"]}, headers=_ANALYST_HEADERS)
    assert r_ack.status_code == 200
    ack_cookies = _parse_set_cookies(r_ack)
    assert "analyst_session_id" in ack_cookies
    full_attrs = ack_cookies["analyst_session_id"]
    assert full_attrs.get("max-age") == "86400"
    new_sid = full_attrs["_value"]
    assert new_sid and new_sid != sid
    assert ack_cookies["analyst_pending_session_id"].get("max-age") == "0"

    client.cookies.clear()
    client.cookies.set("analyst_session_id", new_sid)

    # 5. Protected endpoints pass middleware (404 on test-only router).
    assert client.get("/api/sources", params={"service": "svcA"}, headers=_ANALYST_GET_HEADERS).status_code == 404

    # 6. Pre-rotation session id is no longer valid.
    client.cookies.clear()
    client.cookies.set("analyst_session_id", sid)
    assert client.get("/api/share/heartbeat", headers=_ANALYST_GET_HEADERS).status_code == 401


def test_on_demand_session_rehydration(client):
    _activate_share()
    invite = _seed_invite()
    r_login = client.post(
        "/api/share/login", json={"email": invite["email"], "passcode": _DEFAULT_PASSCODE}, headers=_ANALYST_HEADERS
    )
    assert r_login.status_code == 200
    sid = _sid_from_login(r_login)

    mgr = get_tunnel_manager()
    with mgr._lock:
        assert sid in mgr._sessions
        del mgr._sessions[sid]

    client.cookies.clear()
    client.cookies.set("analyst_pending_session_id", sid)
    r_hb = client.get("/api/share/heartbeat", headers=_ANALYST_GET_HEADERS)
    assert r_hb.status_code == 200 and r_hb.json()["ok"] is True
    with mgr._lock:
        assert sid in mgr._sessions


def test_login_rate_limiting_and_audit_email_sanitization(client):
    from backend.routers.share_auth import _safe_audit_email

    # L3: audit email sanitization
    assert _safe_audit_email("a@b.com") == "a@b.com"
    assert _safe_audit_email(None) == "-" and _safe_audit_email("") == "-"
    out = _safe_audit_email("a@b.com\nLOGIN_SUCCESS forged-line")
    assert "\n" not in out and "\r" not in out
    assert len(_safe_audit_email("x" * 9999)) <= 254

    # L2: successful login does not clear failure counter (finding 016)
    _activate_share()
    _seed_invite(email="l2@example.com")
    rl = tunnel.get_tunnel_manager()._rate_limiter
    rl.record_failure("127.0.0.1")
    r_ok = client.post(
        "/api/share/login", json={"email": "l2@example.com", "passcode": _DEFAULT_PASSCODE}, headers=_ANALYST_HEADERS
    )
    assert r_ok.status_code == 200 and rl._events.get("127.0.0.1")

    # Lockout after 5 failures returns 429 with retry_after_s and Retry-After header
    _seed_invite(email="lockout-target@example.com")
    headers = {**_ANALYST_HEADERS, "X-Forwarded-For": "198.51.100.77", "X-Page-Load-ID": "pl-lockout-test"}
    for _ in range(5):
        r_fail = client.post(
            "/api/share/login",
            json={"email": "lockout-target@example.com", "passcode": "wrong-passcode"},
            headers=headers,
        )
        assert r_fail.status_code == 401

    r_locked = client.post(
        "/api/share/login", json={"email": "lockout-target@example.com", "passcode": _DEFAULT_PASSCODE}, headers=headers
    )
    assert r_locked.status_code == 429
    detail = r_locked.json()["detail"]
    assert detail["error"] == "rate_limited" and detail["retry_after_s"] > 0
    assert int(r_locked.headers.get("retry-after", "0")) > 0


def test_auth_config_returns_typed_schema_with_page_load_id(client):
    r = client.get("/api/share/auth-config", headers={"X-Page-Load-ID": "pl-auth-cfg-1"})
    assert r.status_code == 200
    body = r.json()
    assert set(body.keys()) == {"passcode_enabled", "providers"}
    assert isinstance(body["passcode_enabled"], bool) and isinstance(body["providers"], list)
