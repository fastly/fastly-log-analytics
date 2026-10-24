from __future__ import annotations

"""Route-level tests for ``/api/admin/share/*`` (admin-only surface).

The middleware blocks analyst sessions from this prefix; that's covered in
[test_middleware.py]. Here we exercise the happy path + the error envelopes
of each handler. Tunnel ``start`` is exercised in direct-expose mode only —
we don't spawn ssh.
"""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.core import share_db
from backend.routers import share_admin, share_auth
from tests.remote_access import oauth_helpers as H


def _app() -> FastAPI:
    return H.make_remote_access_app(share_admin.router, share_auth.router)


@pytest.fixture
def client():
    with TestClient(_app()) as c:
        yield c


# ── /status ────────────────────────────────────────────────────────────────


def test_status_returns_expected_keys(client):
    r = client.get("/api/admin/share/status")
    assert r.status_code == 200, r.text
    body = r.json()
    for key in (
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
    ):
        assert key in body, f"missing {key}"
    assert body["sharing_active"] is False
    assert body["active_session_count"] == 0
    assert "failures" in body["rate_limits"] and "lockouts" in body["rate_limits"]
    assert "heartbeat_unauth_count" in body["telemetry"]


def test_audit_logs_endpoint_returns_filtered_rows(client):
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


def test_status_invites_carry_last_login(client):
    """Each invite in /status carries its derived last_login_at."""
    inv = share_db.create_remote_invite(
        name="Alice",
        email="alice@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=[],
    )
    # A successful login for this invite's email; a FAIL must not set last-login.
    share_db.log_share_audit_event(
        event_type="LOGIN_SUCCESS", email="alice@example.com", ip_address="1.1.1.1", details="x"
    )
    share_db.log_share_audit_event(
        event_type="LOGIN_FAIL", email="alice@example.com", ip_address="1.1.1.1", details="x"
    )

    body = client.get("/api/admin/share/status").json()
    row = next(i for i in body["invites"] if i["id"] == inv["id"])
    assert row["last_login_at"] is not None


def test_status_invite_never_logged_in_has_null_last_login(client):
    """An invite with no successful-login events reports last_login_at null."""
    inv = share_db.create_remote_invite(
        name="Bob",
        email="bob@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=[],
    )
    body = client.get("/api/admin/share/status").json()
    row = next(i for i in body["invites"] if i["id"] == inv["id"])
    assert row["last_login_at"] is None


def test_audit_logs_endpoint_rejects_bad_limit(client):
    """``limit`` out of [1, 2000] now rejected by FastAPI's Query(ge/le)
    validator → 422 with the structured ``loc`` path that every other
    422 in the codebase uses (consistency with body/query validation
    conventions, instead of the prior hand-rolled 400)."""
    r = client.get("/api/admin/share/audit-logs", params={"limit": 0})
    assert r.status_code == 422


# ── /start /stop /panic ────────────────────────────────────────────────────


def test_start_direct_mode_validates_https(client):
    r = client.post(
        "/api/admin/share/start",
        json={"public_endpoint": "http://example.com"},
    )
    assert r.status_code == 400
    assert r.json()["detail"]["error"] == "invalid_request"


def test_start_direct_mode_happy_path(client):
    r = client.post(
        "/api/admin/share/start",
        json={"public_endpoint": "https://share.example.com"},
    )
    assert r.status_code == 200, r.text
    assert r.json()["public_url"] == "https://share.example.com"
    # Stop is idempotent.
    r2 = client.post("/api/admin/share/stop")
    assert r2.status_code == 200


def test_panic_returns_summary(client):
    r = client.post("/api/admin/share/panic")
    assert r.status_code == 200
    body = r.json()
    assert isinstance(body, dict)
    # No active sessions in the test → 0 booted.
    assert body.get("sessions_booted") == 0


# ── /invites ───────────────────────────────────────────────────────────────


def test_create_invite_happy_path(client):
    r = client.post(
        "/api/admin/share/invites",
        json={
            "name": "Drew",
            "email": "drew@example.com",
            "passcode": "ocean-breeze-cabin-42",
            "duration_hours": 24,
            "service_ids": ["svcA"],
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["email"] == "drew@example.com"
    assert "id" in body


def test_create_invite_weak_passcode_rejected(client):
    r = client.post(
        "/api/admin/share/invites",
        json={
            "name": "Drew",
            "email": "drew@example.com",
            "passcode": "shrt",
            "service_ids": ["svcA"],
        },
    )
    assert r.status_code == 400
    assert r.json()["detail"]["error"] == "weak_passcode"


def test_create_invite_invalid_name_rejected(client):
    r = client.post(
        "/api/admin/share/invites",
        json={
            "name": "<script>",
            "email": "drew@example.com",
            "passcode": "ocean-breeze-cabin-42",
            "service_ids": ["svcA"],
        },
    )
    assert r.status_code == 400
    assert r.json()["detail"]["error"] == "invalid_input"


def test_update_invite_services(client):
    r = client.post(
        "/api/admin/share/invites",
        json={
            "name": "Drew",
            "email": "drew@example.com",
            "passcode": "ocean-breeze-cabin-42",
            "service_ids": ["svcA"],
        },
    )
    invite_id = r.json()["id"]
    r2 = client.patch(
        f"/api/admin/share/invites/{invite_id}/services",
        json={"service_ids": ["svcA", "svcB"]},
    )
    assert r2.status_code == 200
    assert sorted(r2.json()["service_ids"]) == ["svcA", "svcB"]


def test_update_invite_services_unknown_404(client):
    r = client.patch(
        "/api/admin/share/invites/no-such-invite/services",
        json={"service_ids": ["svcA"]},
    )
    assert r.status_code == 404


def test_create_invite_empty_services_400(client):
    """An invite with no services strands the analyst on 'No service found'."""
    r = client.post(
        "/api/admin/share/invites",
        json={
            "name": "Drew",
            "email": "drew@example.com",
            "passcode": "ocean-breeze-cabin-42",
            "service_ids": [],
        },
    )
    assert r.status_code == 400
    assert r.json()["detail"]["error"] == "invalid_request"


def test_update_invite_services_empty_400(client):
    r = client.post(
        "/api/admin/share/invites",
        json={
            "name": "Drew",
            "email": "drew@example.com",
            "passcode": "ocean-breeze-cabin-42",
            "service_ids": ["svcA"],
        },
    )
    invite_id = r.json()["id"]
    r2 = client.patch(
        f"/api/admin/share/invites/{invite_id}/services",
        json={"service_ids": []},
    )
    assert r2.status_code == 400
    assert r2.json()["detail"]["error"] == "invalid_request"


def test_update_invite_pii_toggles_mask_ips(client):
    r = client.post(
        "/api/admin/share/invites",
        json={
            "name": "Drew",
            "email": "drew@example.com",
            "passcode": "ocean-breeze-cabin-42",
            "service_ids": ["svcA"],
            "pii_policy": {"mask_ips": False},
        },
    )
    invite_id = r.json()["id"]
    assert r.json()["pii_policy"]["mask_ips"] is False
    r2 = client.patch(
        f"/api/admin/share/invites/{invite_id}/pii",
        json={"mask_ips": True},
    )
    assert r2.status_code == 200, r2.text
    assert r2.json()["pii_policy"]["mask_ips"] is True


def test_update_invite_pii_unknown_404(client):
    r = client.patch(
        "/api/admin/share/invites/no-such-invite/pii",
        json={"mask_ips": True},
    )
    assert r.status_code == 404


def test_create_invite_allow_concurrent_sessions(client):
    r = client.post(
        "/api/admin/share/invites",
        json={
            "name": "Drew",
            "email": "drew@example.com",
            "passcode": "ocean-breeze-cabin-42",
            "service_ids": ["svcA"],
            "allow_concurrent_sessions": True,
        },
    )
    assert r.status_code == 200, r.text
    assert r.json()["allow_concurrent_sessions"] is True


def test_create_invite_defaults_allow_concurrent_sessions_false(client):
    r = client.post(
        "/api/admin/share/invites",
        json={
            "name": "Drew",
            "email": "drew@example.com",
            "passcode": "ocean-breeze-cabin-42",
            "service_ids": ["svcA"],
        },
    )
    assert r.status_code == 200, r.text
    assert r.json()["allow_concurrent_sessions"] is False


def test_update_invite_sharing_toggles(client):
    r = client.post(
        "/api/admin/share/invites",
        json={
            "name": "Drew",
            "email": "drew@example.com",
            "passcode": "ocean-breeze-cabin-42",
            "service_ids": ["svcA"],
        },
    )
    invite_id = r.json()["id"]
    assert r.json()["allow_concurrent_sessions"] is False
    r2 = client.patch(
        f"/api/admin/share/invites/{invite_id}/sharing",
        json={"allow_concurrent_sessions": True},
    )
    assert r2.status_code == 200, r2.text
    assert r2.json()["allow_concurrent_sessions"] is True
    # And back off again.
    r3 = client.patch(
        f"/api/admin/share/invites/{invite_id}/sharing",
        json={"allow_concurrent_sessions": False},
    )
    assert r3.status_code == 200
    assert r3.json()["allow_concurrent_sessions"] is False


def test_update_invite_sharing_unknown_404(client):
    r = client.patch(
        "/api/admin/share/invites/no-such-invite/sharing",
        json={"allow_concurrent_sessions": True},
    )
    assert r.status_code == 404


def test_update_invite_passcode_happy_path(client):
    """Rotate passcode without recreating the invite. Verifies the new passcode
    works at the share-login endpoint and the old one no longer does."""
    invite = share_db.create_remote_invite(
        name="Drew",
        email="drew-passcode@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
    )
    r = client.patch(
        f"/api/admin/share/invites/{invite['id']}/passcode",
        json={"passcode": "river-stone-mountain-99"},
    )
    assert r.status_code == 200, r.text
    assert r.json()["ok"] is True

    # Old passcode must no longer verify; new one must.
    refreshed = share_db.get_remote_invite(invite["id"])
    assert not share_db.verify_passcode("ocean-breeze-cabin-42", refreshed["passcode"])
    assert share_db.verify_passcode("river-stone-mountain-99", refreshed["passcode"])


def test_update_invite_passcode_unknown_404(client):
    r = client.patch(
        "/api/admin/share/invites/no-such-invite/passcode",
        json={"passcode": "ocean-breeze-cabin-42"},
    )
    assert r.status_code == 404


def test_update_invite_passcode_weak_400(client):
    invite = share_db.create_remote_invite(
        name="Drew",
        email="drew-weak@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
    )
    r = client.patch(
        f"/api/admin/share/invites/{invite['id']}/passcode",
        json={"passcode": "weak"},
    )
    assert r.status_code == 400


def test_revoke_invite_happy_path(client):
    invite = share_db.create_remote_invite(
        name="Drew",
        email="drew@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
    )
    r = client.post(f"/api/admin/share/invites/{invite['id']}/revoke")
    assert r.status_code == 200
    assert r.json()["ok"] is True


def test_revoke_unknown_invite_404(client):
    r = client.post("/api/admin/share/invites/missing/revoke")
    assert r.status_code == 404


def test_delete_invite_happy_path(client):
    invite = share_db.create_remote_invite(
        name="Drew",
        email="drew-delete@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
    )
    r = client.delete(f"/api/admin/share/invites/{invite['id']}")
    assert r.status_code == 200
    assert r.json()["ok"] is True
    assert share_db.get_remote_invite(invite["id"]) is None


def test_delete_unknown_invite_404(client):
    r = client.delete("/api/admin/share/invites/missing")
    assert r.status_code == 404


def test_issue_claim_token_unknown_invite_404(client):
    r = client.post("/api/admin/share/invites/missing/claim-token")
    assert r.status_code == 404


def test_issue_claim_token_happy(client):
    invite = share_db.create_remote_invite(
        name="Drew",
        email="drew@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
    )
    r = client.post(f"/api/admin/share/invites/{invite['id']}/claim-token")
    assert r.status_code == 200
    assert len(r.json()["token"]) >= 16


# ── Backup ─────────────────────────────────────────────────────────────────


def test_backup_export_weak_passphrase_400(client):
    r = client.post(
        "/api/admin/share/backup/export",
        json={"passphrase": "short"},
    )
    assert r.status_code == 400
    assert r.json()["detail"]["error"] == "weak_passphrase"


def test_backup_export_then_import_round_trip(client):
    share_db.create_remote_invite(
        name="Drew",
        email="drew@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
    )
    r = client.post(
        "/api/admin/share/backup/export",
        json={"passphrase": "correct-horse-battery-staple"},
    )
    assert r.status_code == 200
    blob = r.content
    assert len(blob) > 100

    # Re-import on top of itself with "skip-collisions" → no-op succeeds.
    files = {"file": ("backup.enc", blob, "application/octet-stream")}
    r2 = client.post(
        "/api/admin/share/backup/import",
        data={"passphrase": "correct-horse-battery-staple", "mode": "skip-collisions"},
        files=files,
    )
    assert r2.status_code == 200, r2.text
    body = r2.json()
    # Existing invite preserved.
    restored = share_db.get_remote_invite_by_email_passcode("drew@example.com", "ocean-breeze-cabin-42")
    assert restored is not None
    assert isinstance(body, dict)


def test_backup_import_bad_mode_400(client):
    r = client.post(
        "/api/admin/share/backup/import",
        data={"passphrase": "correct-horse-battery-staple", "mode": "merge-all-the-things"},
        files={"file": ("backup.enc", b"junk", "application/octet-stream")},
    )
    assert r.status_code == 400
    assert r.json()["detail"]["error"] == "invalid_mode"


def test_backup_import_wrong_passphrase_400(client):
    invite = share_db.create_remote_invite(
        name="Drew",
        email="drew@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
    )
    r = client.post(
        "/api/admin/share/backup/export",
        json={"passphrase": "correct-horse-battery-staple"},
    )
    blob = r.content
    r2 = client.post(
        "/api/admin/share/backup/import",
        data={"passphrase": "wrong-horse-battery-staple", "mode": "skip-collisions"},
        files={"file": ("backup.enc", blob, "application/octet-stream")},
    )
    assert r2.status_code == 400
    assert r2.json()["detail"]["error"] == "import_failed"


# ── GDPR ───────────────────────────────────────────────────────────────────


def test_gdpr_erase_removes_records(client):
    share_db.create_remote_invite(
        name="Drew",
        email="drew@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
    )
    r = client.post(
        "/api/admin/share/gdpr/erase",
        json={"email": "drew@example.com", "reason": "user request"},
    )
    assert r.status_code == 200, r.text
    assert share_db.get_remote_invite_by_email_passcode("drew@example.com", "ocean-breeze-cabin-42") is None


def test_gdpr_erase_blank_email_400(client):
    r = client.post(
        "/api/admin/share/gdpr/erase",
        json={"email": "", "reason": "user request"},
    )
    assert r.status_code == 400
    assert r.json()["detail"]["error"] == "invalid_request"


# ── Settings ───────────────────────────────────────────────────────────────


def test_update_settings_cap_value(client):
    r = client.patch(
        "/api/admin/share/settings",
        json={"max_concurrent_analyst_sessions": 25},
    )
    assert r.status_code == 200
    assert r.json()["max_concurrent_analyst_sessions"] == 25


def test_update_settings_zero_400(client):
    r = client.patch(
        "/api/admin/share/settings",
        json={"max_concurrent_analyst_sessions": 0},
    )
    assert r.status_code == 400


# ── Wordphrase ─────────────────────────────────────────────────────────────


def test_wordphrase_returns_dashed_string(client):
    r = client.get("/api/admin/share/wordphrase")
    assert r.status_code == 200
    passcode = r.json()["passcode"]
    assert isinstance(passcode, str)
    assert len(passcode) >= 16
    assert "-" in passcode


# ── share-live freshness (now on the multiplexed admin event stream) ───────
#
# The dedicated ``/api/admin/share/stream`` SSE endpoint was REMOVED: its
# live freshness now rides the multiplexed ``/api/admin/events/stream``
# (``share`` channel) so /admin/share holds ONE SSE connection over the
# HTTP/1.1 admin tunnel instead of two. The streaming wiring (envelope
# framing, serviceless start, cleanup) is covered in
# tests/routers/test_admin_events_stream.py. Here we pin that the dedicated
# stream route is gone, the polling /live endpoint stays, and the shared
# payload helper still has the wire shape the frontend cache expects.


def test_dedicated_stream_route_is_removed():
    """The standalone /api/admin/share/stream endpoint is gone — its job
    moved to the admin event stream's `share` channel. Guard against an
    accidental re-add (which would put /admin/share back to two streams)."""
    from fastapi import FastAPI

    from backend.routers import share_admin

    app = FastAPI()
    app.include_router(share_admin.router)
    paths = app.openapi()["paths"]
    assert "/api/admin/share/stream" not in paths
    # The lean polling snapshot endpoint stays (mount + mutation refetch).
    assert "/api/admin/share/live" in paths


def test_live_payload_shape_matches_share_channel(client):
    """The `share` channel and the /live polling endpoint reuse the same
    ``build_share_live_payload`` helper, so the wire-shape contract the
    frontend SHARE_LIVE_QUERY_KEY cache expects holds for both."""
    from backend.routers.share_admin import build_share_live_payload

    payload = build_share_live_payload()
    for key in ("sharing_active", "public_url", "active_session_count", "rate_limits", "telemetry"):
        assert key in payload, f"missing {key}"


# ── typed nested models keep the producer's wire payload ───────────────────


def _seed_status_fixtures():
    from backend.utils.tunnel.manager import get_tunnel_manager
    from backend.utils.tunnel.session import AnalystSession

    share_db.create_remote_invite(
        name="Carol",
        email="carol@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svc-a"],
    )
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


def test_status_nested_collections_match_producer(client):
    from fastapi.encoders import jsonable_encoder

    from backend.routers.share_admin import build_share_status

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


def test_live_and_audit_logs_match_producer(client):
    from fastapi.encoders import jsonable_encoder

    from backend.utils.tunnel.manager import build_share_live_payload

    _seed_status_fixtures()
    live = client.get("/api/admin/share/live").json()
    assert live == jsonable_encoder(build_share_live_payload())

    audit = client.get("/api/admin/share/audit-logs").json()
    assert audit["audit_logs"] == jsonable_encoder(share_db.get_share_audit_logs(limit=200))


def test_invite_revoked_is_integer_flag_on_the_wire(client):
    inv = share_db.create_remote_invite(
        name="Dan",
        email="dan@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=[],
    )
    body = client.get("/api/admin/share/status").json()
    row = next(i for i in body["invites"] if i["id"] == inv["id"])
    assert row["revoked"] == 0
    assert type(row["revoked"]) is int
    assert share_db.revoke_remote_invite(inv["id"])
    row = next(i for i in client.get("/api/admin/share/status").json()["invites"] if i["id"] == inv["id"])
    assert row["revoked"] == 1
    assert type(row["revoked"]) is int


# ── Merged from test_share_auth_routes.py ──────────────────────────────────────────────────────

"""Route-level tests for ``/api/share/*`` (analyst auth surface).

Verifies the wiring between the router and ``share_db`` / tunnel manager.
The middleware logic is tested in [test_middleware.py]; here we focus on
the handlers themselves — payload shapes, error envelopes, and the
session-cookie contract.
"""

import pytest

from backend.utils import tunnel


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
    cookie = r.headers.get("set-cookie", "")
    assert "analyst_session_id" in cookie
    assert "secure" in cookie.lower()


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
    assert rl._events.get(ip), "failure counter must persist after a successful login"


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


def test_claim_revoked_invite_returns_410(client):
    """When an invite is revoked before its one-shot claim token is redeemed,
    POST /api/share/claim/{token} returns 410 invite_gone."""
    invite = _seed_invite(email="revoked-claim@example.com")
    token = share_db.create_claim_token(invite["id"], ttl_hours=1)
    share_db.revoke_remote_invite(invite["id"])
    r = client.post(
        f"/api/share/claim/{token}",
        headers={"X-Page-Load-ID": "pl-claim-410"},
    )
    assert r.status_code == 410
    assert r.json()["detail"]["error"] == "invite_gone"


def test_login_lockout_returns_429_with_retry_after(client):
    """Brute-force rate limiting locks the client IP after 5 failures and
    returns HTTP 429 with retry_after_s and Retry-After header."""
    _activate_share()
    _seed_invite(email="lockout-target@example.com")
    headers = {
        "X-Remote-Analyst": "1",
        "Host": "testserver",
        "Origin": "https://testserver",
        "X-Forwarded-For": "198.51.100.77",
        "X-Page-Load-ID": "pl-lockout-test",
    }
    for _ in range(5):
        r_fail = client.post(
            "/api/share/login",
            json={"email": "lockout-target@example.com", "passcode": "wrong-passcode"},
            headers=headers,
        )
        assert r_fail.status_code == 401

    r_locked = client.post(
        "/api/share/login",
        json={"email": "lockout-target@example.com", "passcode": "ocean-breeze-cabin-42"},
        headers=headers,
    )
    assert r_locked.status_code == 429
    detail = r_locked.json()["detail"]
    assert detail["error"] == "rate_limited"
    assert detail["retry_after_s"] > 0
    assert int(r_locked.headers.get("retry-after", "0")) > 0


def test_auth_config_returns_typed_schema_with_page_load_id(client):
    """GET /api/share/auth-config returns typed AuthConfigResponse and accepts X-Page-Load-ID."""
    r = client.get(
        "/api/share/auth-config",
        headers={"X-Page-Load-ID": "pl-auth-cfg-1"},
    )
    assert r.status_code == 200
    body = r.json()
    assert set(body.keys()) == {"passcode_enabled", "providers"}
    assert isinstance(body["passcode_enabled"], bool)
    assert isinstance(body["providers"], list)
