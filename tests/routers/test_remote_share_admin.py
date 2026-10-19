"""Router unit tests for ``/api/admin/share/*`` (Admin Live Share Management).

Covers:
- ``GET /api/admin/share/status`` and ``GET /api/admin/share/live``
- ``POST /api/admin/share/start``, ``POST /api/admin/share/stop``, ``POST /api/admin/share/panic``
- Invite lifecycle: ``GET /api/admin/share/invites``, ``POST /api/admin/share/invites``,
  ``POST /api/admin/share/invites/{invite_id}/revoke``,
  ``POST /api/admin/share/invites/{invite_id}/reactivate``,
  ``POST /api/admin/share/invites/{invite_id}/regenerate-passcode``
- Session revocation: ``POST /api/admin/share/sessions/{session_id}/revoke`` and ``/boot``
- Audit log filtering: ``GET /api/admin/share/audit-logs``
- Settings & TOS management: ``GET / PATCH /api/admin/share/settings``,
  ``GET / POST / PUT /api/admin/share/tos``
- Strict Admin-only RBAC (`403 Forbidden` with ``error: "admin_only"``) for Analyst Path B
  sessions across all ``/api/admin/share/*`` routes (both middleware and route dependency).
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.middleware.base import BaseHTTPMiddleware

from backend.core import share_db
from backend.routers import share_admin
from backend.utils.remote_access import RemoteAccessMiddleware
from backend.utils.tunnel.manager import get_tunnel_manager
from backend.utils.tunnel.session import AnalystSession


def _make_admin_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.include_router(share_admin.router)
    return app


def _make_analyst_dependency_app() -> FastAPI:
    """App that stamps ``request.state.analyst_session`` so ``Depends(require_admin)``
    is exercised directly on the router even if middleware is bypassed."""

    class _StampAnalystMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request, call_next):
            request.state.analyst_session = AnalystSession(
                session_id="sess-analyst-1",
                invite_id="inv-analyst-1",
                name="Analyst B",
                email="analyst@example.com",
                ip_address="203.0.113.10",
                user_agent="pytest-analyst",
                fingerprint_signature="fp-analyst",
                pii_policy={"mask_ips": True},
                query_window_hours=24,
                query_start_time=None,
                query_end_time=None,
                login_time="2026-01-01T00:00:00Z",
                last_active_time="2026-01-01T00:05:00Z",
                service_ids=["svc-1"],
            )
            return await call_next(request)

    app = FastAPI()
    app.add_middleware(_StampAnalystMiddleware)
    app.include_router(share_admin.router)
    return app


@pytest.fixture
def client():
    with TestClient(_make_admin_app()) as c:
        yield c


@pytest.fixture
def analyst_dep_client():
    with TestClient(_make_analyst_dependency_app()) as c:
        yield c


def test_status_and_live_contracts(client: TestClient):
    """GET /api/admin/share/status and /api/admin/share/live return typed snapshots."""
    r_status = client.get("/api/admin/share/status")
    assert r_status.status_code == 200, r_status.text
    status_body = r_status.json()
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
        assert key in status_body

    r_live = client.get("/api/admin/share/live")
    assert r_live.status_code == 200, r_live.text
    live_body = r_live.json()
    for key in (
        "sharing_active",
        "public_url",
        "active_session_count",
        "rate_limits",
        "telemetry",
    ):
        assert key in live_body


def test_start_and_stop_direct_sharing(client: TestClient):
    """POST /api/admin/share/start and /stop toggle direct HTTPS live sharing."""
    r_bad = client.post(
        "/api/admin/share/start",
        json={"public_endpoint": "http://insecure.example.com"},
    )
    assert r_bad.status_code == 400
    assert r_bad.json()["detail"]["error"] == "invalid_request"

    r_start = client.post(
        "/api/admin/share/start",
        json={"public_endpoint": "https://share.example.com"},
    )
    assert r_start.status_code == 200, r_start.text
    assert r_start.json()["public_url"] == "https://share.example.com"

    r_live = client.get("/api/admin/share/live")
    assert r_live.status_code == 200
    assert r_live.json()["sharing_active"] is True
    assert r_live.json()["public_url"] == "https://share.example.com"

    r_stop = client.post("/api/admin/share/stop")
    assert r_stop.status_code == 200
    assert r_stop.json()["ok"] is True


def test_invite_lifecycle_create_list_revoke_reactivate_regenerate_passcode(client: TestClient):
    """Full invite lifecycle: create -> list -> revoke -> reactivate -> regenerate-passcode."""
    r_create = client.post(
        "/api/admin/share/invites",
        json={
            "name": "Alice Analyst",
            "email": "alice@example.com",
            "passcode": "ocean-breeze-cabin-42",
            "duration_hours": 48,
            "service_ids": ["svc-alpha"],
            "pii_policy": {"mask_ips": True},
        },
    )
    assert r_create.status_code == 200, r_create.text
    invite = r_create.json()
    invite_id = invite["id"]
    assert invite["email"] == "alice@example.com"
    assert invite["revoked"] == 0
    assert invite["service_ids"] == ["svc-alpha"]

    # List invites via GET /api/admin/share/invites
    r_list = client.get("/api/admin/share/invites")
    assert r_list.status_code == 200, r_list.text
    invites_list = r_list.json()["invites"]
    assert any(row["id"] == invite_id for row in invites_list)

    # Revoke invite
    r_revoke = client.post(f"/api/admin/share/invites/{invite_id}/revoke")
    assert r_revoke.status_code == 200, r_revoke.text
    assert r_revoke.json()["ok"] is True
    assert share_db.get_remote_invite(invite_id)["revoked"] == 1

    # Reactivate invite
    r_reactivate = client.post(f"/api/admin/share/invites/{invite_id}/reactivate")
    assert r_reactivate.status_code == 200, r_reactivate.text
    reactivated_body = r_reactivate.json()
    assert reactivated_body["id"] == invite_id
    assert reactivated_body["revoked"] == 0
    assert share_db.get_remote_invite(invite_id)["revoked"] == 0

    # Unknown invite reactivate returns 404
    r_reactivate_404 = client.post("/api/admin/share/invites/nonexistent-id/reactivate")
    assert r_reactivate_404.status_code == 404

    # Regenerate passcode
    r_regen = client.post(f"/api/admin/share/invites/{invite_id}/regenerate-passcode")
    assert r_regen.status_code == 200, r_regen.text
    regen_body = r_regen.json()
    assert regen_body["ok"] is True
    new_passcode = regen_body["passcode"]
    assert isinstance(new_passcode, str) and len(new_passcode) >= 16 and "-" in new_passcode

    # Verify the regenerated passcode replaced the old one in share_db
    stored = share_db.get_remote_invite(invite_id)
    assert not share_db.verify_passcode("ocean-breeze-cabin-42", stored["passcode"])
    assert share_db.verify_passcode(new_passcode, stored["passcode"])

    # Unknown invite regenerate-passcode returns 404
    r_regen_404 = client.post("/api/admin/share/invites/nonexistent-id/regenerate-passcode")
    assert r_regen_404.status_code == 404


def test_session_revocation_boot_and_revoke_endpoints(client: TestClient):
    """Both /sessions/{session_id}/revoke and /sessions/{session_id}/boot terminate an active session."""
    mgr = get_tunnel_manager()
    for sid in ("sess-revoke-1", "sess-boot-2"):
        mgr._sessions[sid] = AnalystSession(
            session_id=sid,
            invite_id="inv-1",
            name="Bob Analyst",
            email="bob@example.com",
            ip_address="198.51.100.7",
            user_agent="Mozilla/5.0",
            fingerprint_signature="fp-bob",
            pii_policy={"mask_ips": True},
            query_window_hours=24,
            query_start_time=None,
            query_end_time=None,
            login_time="2026-01-01T00:00:00Z",
            last_active_time="2026-01-01T00:01:00Z",
            service_ids=["svc-alpha"],
        )

    r_revoke = client.post("/api/admin/share/sessions/sess-revoke-1/revoke")
    assert r_revoke.status_code == 200, r_revoke.text
    assert r_revoke.json()["ok"] is True
    assert "sess-revoke-1" not in mgr._sessions

    r_boot = client.post("/api/admin/share/sessions/sess-boot-2/boot")
    assert r_boot.status_code == 200, r_boot.text
    assert r_boot.json()["ok"] is True
    assert "sess-boot-2" not in mgr._sessions

    # Unknown session returns 404 on both routes
    assert client.post("/api/admin/share/sessions/missing-sess/revoke").status_code == 404
    assert client.post("/api/admin/share/sessions/missing-sess/boot").status_code == 404


def test_audit_log_filtering_by_event_type_and_email(client: TestClient):
    """GET /api/admin/share/audit-logs filters by event_type, email, and limit."""
    share_db.log_share_audit_event(
        event_type="LOGIN_SUCCESS",
        email="filter-target@example.com",
        ip_address="192.0.2.1",
        details="Successful login",
    )
    share_db.log_share_audit_event(
        event_type="LOGIN_FAIL",
        email="other-user@example.com",
        ip_address="192.0.2.2",
        details="Bad passcode",
    )

    r_type = client.get(
        "/api/admin/share/audit-logs",
        params={"event_type": "LOGIN_SUCCESS", "email": "filter-target@example.com", "limit": 50},
    )
    assert r_type.status_code == 200, r_type.text
    rows = r_type.json()["audit_logs"]
    assert len(rows) >= 1
    assert all(r["event_type"] == "LOGIN_SUCCESS" for r in rows)
    assert all("filter-target@example.com" in (r.get("email") or "") for r in rows)


def test_settings_and_tos_crud(client: TestClient):
    """GET/PATCH /api/admin/share/settings and GET/POST/PUT /api/admin/share/tos."""
    r_patch = client.patch(
        "/api/admin/share/settings",
        json={"max_concurrent_analyst_sessions": 15},
    )
    assert r_patch.status_code == 200, r_patch.text
    assert r_patch.json()["max_concurrent_analyst_sessions"] == 15

    r_get_settings = client.get("/api/admin/share/settings")
    assert r_get_settings.status_code == 200, r_get_settings.text
    assert r_get_settings.json()["max_concurrent_analyst_sessions"] == 15

    # Read current TOS
    r_tos_get = client.get("/api/admin/share/tos")
    assert r_tos_get.status_code == 200, r_tos_get.text
    assert "version" in r_tos_get.json()
    assert "text" in r_tos_get.json()

    # Publish new TOS version via POST
    new_md = "# Custom Terms of Use\n\nConfidential analyst terms v2 for testing."
    r_tos_post = client.post("/api/admin/share/tos", json={"version": "v2.0-test", "text": new_md})
    assert r_tos_post.status_code == 200, r_tos_post.text
    assert r_tos_post.json()["version"] == "v2.0-test"
    assert r_tos_post.json()["text"] == new_md

    # Publish another TOS version via PUT
    newer_md = "# Custom Terms of Use\n\nConfidential analyst terms v3 for testing."
    r_tos_put = client.put("/api/admin/share/tos", json={"version": "v3.0-test", "text": newer_md})
    assert r_tos_put.status_code == 200, r_tos_put.text
    assert r_tos_put.json()["version"] == "v3.0-test"
    assert r_tos_put.json()["text"] == newer_md

    # Blank TOS version/text rejected with 400
    r_tos_blank = client.post("/api/admin/share/tos", json={"version": "   ", "text": ""})
    assert r_tos_blank.status_code == 400


@pytest.mark.parametrize(
    "method,path,json_body",
    [
        ("GET", "/api/admin/share/status", None),
        ("GET", "/api/admin/share/live", None),
        ("GET", "/api/admin/share/invites", None),
        (
            "POST",
            "/api/admin/share/invites",
            {"name": "X", "email": "x@y.com", "passcode": "ocean-breeze-cabin-42", "service_ids": ["s1"]},
        ),
        ("POST", "/api/admin/share/invites/inv-1/revoke", None),
        ("POST", "/api/admin/share/invites/inv-1/reactivate", None),
        ("POST", "/api/admin/share/invites/inv-1/regenerate-passcode", None),
        ("POST", "/api/admin/share/sessions/sess-1/revoke", None),
        ("POST", "/api/admin/share/sessions/sess-1/boot", None),
        ("GET", "/api/admin/share/audit-logs", None),
        ("GET", "/api/admin/share/settings", None),
        ("PATCH", "/api/admin/share/settings", {"max_concurrent_analyst_sessions": 5}),
        ("GET", "/api/admin/share/tos", None),
        ("POST", "/api/admin/share/tos", {"version": "v1", "text": "A" * 30}),
        ("PUT", "/api/admin/share/tos", {"version": "v2", "text": "B" * 30}),
        ("POST", "/api/admin/share/start", {"public_endpoint": "https://share.example.com"}),
        ("POST", "/api/admin/share/stop", None),
        ("POST", "/api/admin/share/panic", None),
    ],
)
def test_analyst_path_b_rejected_403_on_all_admin_share_routes(
    analyst_dep_client: TestClient,
    method: str,
    path: str,
    json_body: dict | None,
):
    """Analyst Path B sessions receive 403 Forbidden with error='admin_only' on every /api/admin/share/* route."""
    resp = analyst_dep_client.request(method, path, json=json_body)
    assert resp.status_code == 403, f"Expected 403 on {method} {path}, got {resp.status_code}: {resp.text}"
    payload = resp.json()
    detail = payload.get("detail", payload)
    assert detail.get("error") == "admin_only"
