"""Route-level tests for ``/api/admin/share/*`` (admin-only surface).

The middleware blocks analyst sessions from this prefix; that's covered in
[test_middleware.py]. Here we exercise the happy path + the error envelopes
of each handler. Tunnel ``start`` is exercised in direct-expose mode only —
we don't spawn ssh.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.core import share_db
from backend.utils.remote_access import RemoteAccessMiddleware


def _app() -> FastAPI:
    from backend.routers import share_admin

    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.include_router(share_admin.router)
    return app


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
        "use_tunnel",
        "tunnel_url",
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


def test_audit_logs_endpoint_rejects_bad_limit(client):
    r = client.get("/api/admin/share/audit-logs", params={"limit": 0})
    assert r.status_code == 400


# ── /start /stop /panic ────────────────────────────────────────────────────


def test_start_direct_mode_validates_https(client):
    r = client.post(
        "/api/admin/share/start",
        json={"use_tunnel": False, "public_endpoint": "http://example.com"},
    )
    assert r.status_code == 400
    assert r.json()["detail"]["error"] == "invalid_request"


def test_start_direct_mode_happy_path(client):
    r = client.post(
        "/api/admin/share/start",
        json={"use_tunnel": False, "public_endpoint": "https://share.example.com"},
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
