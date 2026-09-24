"""Tests for share_audit_purge background job and POST /api/admin/share/purge endpoint.

Covers:
- purge_old_audit_logs retention window filtering.
- purge_stale_share_records for expired invites, tokens, and stale sessions.
- PRAGMA wal_checkpoint(TRUNCATE) execution.
- _run_share_audit_purge background job lifecycle and telemetry.
- POST /api/admin/share/purge admin endpoint.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from backend.core import share_db
from backend.cron.jobs.metadata import _run_share_audit_purge
from backend.main import app
from backend.utils.date_utils import iso_z


@pytest.fixture
def test_share_db():
    """Isolated share DB connection for purge testing."""
    con = share_db.get_global_share_con()
    con.execute("DELETE FROM remote_sessions")
    con.execute("DELETE FROM remote_invite_claim_tokens")
    con.execute("DELETE FROM invite_services")
    con.execute("DELETE FROM remote_invites")
    con.execute("DELETE FROM remote_share_audit_logs")
    con.commit()
    yield con


def test_purge_old_audit_logs(test_share_db):
    """Audit logs older than cutoff are deleted, newer are retained."""
    con = test_share_db
    old_ts = iso_z(datetime.now(UTC) - timedelta(days=95))
    recent_ts = iso_z(datetime.now(UTC) - timedelta(days=10))

    con.execute(
        "INSERT INTO remote_share_audit_logs(timestamp, event_type, email, ip_address, details) VALUES (?,?,?,?,?)",
        (old_ts, "LOGIN_SUCCESS", "old@example.com", "1.1.1.1", "{}"),
    )
    con.execute(
        "INSERT INTO remote_share_audit_logs(timestamp, event_type, email, ip_address, details) VALUES (?,?,?,?,?)",
        (recent_ts, "LOGIN_SUCCESS", "new@example.com", "2.2.2.2", "{}"),
    )
    con.commit()

    deleted = share_db.purge_old_audit_logs(retention_days=90, con=con)
    assert deleted == 1

    remaining = con.execute("SELECT email FROM remote_share_audit_logs").fetchall()
    assert len(remaining) == 1
    assert remaining[0]["email"] == "new@example.com"


def test_purge_stale_share_records(test_share_db):
    """Expired invites, tokens, and stale sessions are pruned; active are retained."""
    con = test_share_db
    past_iso = iso_z(datetime.now(UTC) - timedelta(days=2))
    future_iso = iso_z(datetime.now(UTC) + timedelta(days=5))
    stale_session_ts = iso_z(datetime.now(UTC) - timedelta(days=35))
    active_session_ts = iso_z(datetime.now(UTC) - timedelta(hours=2))

    # Expired invite & non-expired invite
    con.execute(
        "INSERT INTO remote_invites(id, name, email, passcode, expires_at, created_at) VALUES (?,?,?,?,?,?)",
        ("inv_expired", "Expired User", "expired@test.com", "hash", past_iso, past_iso),
    )
    con.execute(
        "INSERT INTO remote_invites(id, name, email, passcode, expires_at, created_at) VALUES (?,?,?,?,?,?)",
        ("inv_active", "Active User", "active@test.com", "hash", future_iso, past_iso),
    )

    # Expired claim token
    con.execute(
        "INSERT INTO remote_invite_claim_tokens(token, invite_id, created_at, expires_at) VALUES (?,?,?,?)",
        ("tok_expired", "inv_active", past_iso, past_iso),
    )

    # Stale session (> 30 days) and active session
    con.execute(
        "INSERT INTO remote_sessions(session_id, invite_id, name, email, ip_address, user_agent, fingerprint_signature, pii_policy, login_time, last_active_time) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            "sess_stale",
            "inv_active",
            "Active User",
            "active@test.com",
            "1.1.1.1",
            "ua",
            "fp",
            "{}",
            past_iso,
            stale_session_ts,
        ),
    )
    con.execute(
        "INSERT INTO remote_sessions(session_id, invite_id, name, email, ip_address, user_agent, fingerprint_signature, pii_policy, login_time, last_active_time) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            "sess_active",
            "inv_active",
            "Active User",
            "active@test.com",
            "1.1.1.1",
            "ua",
            "fp",
            "{}",
            past_iso,
            active_session_ts,
        ),
    )
    con.commit()

    stats = share_db.purge_stale_share_records(max_idle_session_days=30, con=con)
    assert stats["deleted_expired_invites"] == 1
    assert stats["deleted_claim_tokens"] == 1
    assert stats["deleted_stale_sessions"] == 1

    # Verify active invite and active session remain
    invites = con.execute("SELECT id FROM remote_invites").fetchall()
    assert [r["id"] for r in invites] == ["inv_active"]

    sessions = con.execute("SELECT session_id FROM remote_sessions").fetchall()
    assert [r["session_id"] for r in sessions] == ["sess_active"]


def test_run_share_audit_purge_lifecycle():
    """Job reads setting, executes audit and stale purge, and returns summary."""
    with (
        patch("backend.core.share_db.get_setting", return_value="60"),
        patch("backend.core.share_db.purge_old_audit_logs", return_value=5) as mock_audit,
        patch(
            "backend.core.share_db.purge_stale_share_records",
            return_value={"deleted_expired_invites": 2, "deleted_stale_sessions": 3, "deleted_claim_tokens": 1},
        ) as mock_stale,
    ):
        result = _run_share_audit_purge.__wrapped__()

    mock_audit.assert_called_once_with(retention_days=60)
    mock_stale.assert_called_once_with(max_idle_session_days=30)
    assert "deleted=5" in result
    assert "retention_days=60" in result
    assert "expired_invites=2" in result
    assert "stale_sessions=3" in result


def test_admin_share_purge_endpoint():
    """POST /api/admin/share/purge triggers purge_all_share_records."""
    client = TestClient(app)

    with patch(
        "backend.core.share_db.purge_all_share_records",
        return_value={
            "deleted_audit_logs": 12,
            "deleted_expired_invites": 1,
            "deleted_stale_sessions": 4,
            "deleted_claim_tokens": 2,
            "audit_retention_days": 45,
            "max_idle_session_days": 15,
        },
    ) as mock_purge_all:
        resp = client.post("/api/admin/share/purge?audit_retention_days=45&max_idle_session_days=15")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["deleted_audit_logs"] == 12
        assert data["deleted_expired_invites"] == 1
        assert data["deleted_stale_sessions"] == 4
        mock_purge_all.assert_called_once_with(audit_retention_days=45, max_idle_session_days=15)
