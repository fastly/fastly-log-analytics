"""Backend unit & RBAC contract tests for Admin Overview (/admin) endpoints.

Covers:
- GET /api/admin/sync-status and GET /api/sync-status (SyncStatusResponse)
- GET /api/admin/system-metrics, GET /api/admin/health, GET /api/admin/health-snapshot (HealthSnapshotResponse)
- GET /api/admin/compaction-status and GET /api/admin/compaction-status/{service_id} (CompactionStatusResponse)
- GET /api/admin/log-accounting/{service_id} and GET /api/admin/log-accounting (LogAccountingResponse)
- Manual cron triggers:
  - POST /api/admin/sync/{service_id}
  - POST /api/admin/commit/{service_id}
  - POST /api/admin/compact/{service_id}
  - POST /api/admin/optimize/{service_id}
- Strict Admin-only RBAC (403 Forbidden for Analyst Path B sessions across /api/admin/* and /api/services/* write routes).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from backend.deps import get_source, require_admin
from backend.main import app
from backend.utils.remote_access import _is_blocked_path


def _make_source(
    service_id: str = "svc-admin-test",
    access_level: str = "read_write",
) -> dict:
    return {
        "name": service_id,
        "service_id": service_id,
        "display_name": "Admin Overview Test Service",
        "bucket": "test-bucket",
        "prefix": "fastly-logs",
        "endpoint": "https://us-east.object.fastlystorage.app",
        "access_key": "AKIAIOSFODNN7EXAMPLE",
        "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "access_level": access_level,
    }


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


def test_admin_sync_status_alias_and_primary(client: TestClient):
    """Both /api/admin/sync-status and /api/sync-status return typed SyncStatusResponse."""
    src = _make_source()
    fake_payload = {
        "configured": True,
        "busy": False,
        "storage_mode": "ducklake",
        "access_level": "read_write",
        "local_rows": 1250,
        "earliest_log_at": "2026-10-15T20:00:00Z",
        "latest_log_at": "2026-10-16T20:00:00Z",
    }
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch(
            "backend.routers.admin.sync_status.compute_sync_status_cached",
            return_value=fake_payload,
        ),
    ):
        r1 = client.get("/api/admin/sync-status?service_id=svc-admin-test&skip_fos=true")
        assert r1.status_code == 200
        body1 = r1.json()
        assert body1["configured"] is True
        assert body1["access_level"] == "read_write"
        assert body1["local_rows"] == 1250

        r2 = client.get("/api/sync-status?service_id=svc-admin-test&skip_fos=true")
        assert r2.status_code == 200
        assert r2.json()["local_rows"] == 1250


def test_admin_system_metrics_and_health_aliases(client: TestClient):
    """/api/admin/system-metrics, /api/admin/health, and /api/admin/health-snapshot return HealthSnapshotResponse."""
    for path in (
        "/api/admin/system-metrics",
        "/api/admin/health",
        "/api/admin/health-snapshot",
    ):
        resp = client.get(path)
        assert resp.status_code == 200, f"{path} returned {resp.status_code}"
        data = resp.json()
        assert "load" in data
        assert "vcpus" in data
        assert "in_flight_runs" in data
        assert "compaction" in data
        assert "pool_wait" in data
        assert "recent_cron_failures" in data


def test_admin_compaction_status_endpoints(client: TestClient):
    """/api/admin/compaction-status and /api/admin/compaction-status/{service_id} return CompactionStatsResponse."""
    src = _make_source()
    app.dependency_overrides[get_source] = lambda: src
    try:
        r1 = client.get(f"/api/admin/compaction-status/{src['service_id']}")
        assert r1.status_code == 200
        body1 = r1.json()
        assert "partitions" in body1
        assert "total_files" in body1
        assert "partitions_above_3" in body1

        r2 = client.get(f"/api/admin/compaction-status?service_id={src['service_id']}")
        assert r2.status_code == 200
        assert r2.json()["total_files"] == body1["total_files"]
    finally:
        app.dependency_overrides.pop(get_source, None)


def test_admin_log_accounting_path_and_query(client: TestClient):
    """/api/admin/log-accounting/{service_id} and /api/admin/log-accounting return LogAccountingResponse."""
    src = _make_source()
    app.dependency_overrides[get_source] = lambda: src
    fake_accounting = {
        "by": "hour",
        "from_ts": "2026-10-15T20:00:00Z",
        "to_ts": "2026-10-16T20:00:00Z",
        "buckets": [],
        "totals": {
            "fastly_requests": 1000,
            "our_rows": 1000,
            "gap": 0,
            "gap_pct": 0.0,
            "worst_bucket_ts": None,
            "worst_bucket_gap_pct": None,
        },
        "sustained_loss": None,
        "catchup": {
            "latest_ingest_ts": "2026-10-16T19:59:50Z",
            "lag_seconds": 10,
            "status": "caught_up",
        },
        "section_timings": [],
    }
    try:
        with patch(
            "backend.routers.admin.log_accounting.compute_log_accounting",
            return_value=fake_accounting,
        ):
            r1 = client.get(f"/api/admin/log-accounting/{src['service_id']}?hours=24")
            assert r1.status_code == 200
            assert r1.json()["totals"]["gap_pct"] == 0.0
            assert r1.json()["totals"]["fastly_requests"] == 1000

            r2 = client.get(f"/api/admin/log-accounting?service_id={src['service_id']}&hours=24")
            assert r2.status_code == 200
            assert r2.json()["totals"]["our_rows"] == 1000
    finally:
        app.dependency_overrides.pop(get_source, None)


def test_admin_manual_cron_trigger_routes(client: TestClient):
    """Manual trigger routes (/sync/{id}, /commit/{id}, /compact/{id}, /optimize/{id}) return typed responses."""
    src = _make_source()
    app.dependency_overrides[get_source] = lambda: src
    try:
        with (
            patch(
                "backend.utils.router_utils.start_or_resume_cron",
                return_value={"ok": True, "message": "Started.", "run_id": 42},
            ) as mock_start,
            patch(
                "backend.core.local_compaction.compact_local_partitions",
                return_value={
                    "partitions_checked": 2,
                    "partitions_compacted": 1,
                    "files_removed": 3,
                    "rows_preserved": 500,
                    "dups_removed": 0,
                },
            ),
            patch(
                "backend.cron.jobs.optimize._run_optimize",
                return_value={"files_rewritten": 4, "files_added": 1},
            ),
        ):
            r_sync = client.post(f"/api/admin/sync/{src['service_id']}")
            assert r_sync.status_code == 200
            assert r_sync.json()["ok"] is True
            assert mock_start.called

            r_commit = client.post(f"/api/admin/commit/{src['service_id']}")
            assert r_commit.status_code == 202
            assert r_commit.json()["ok"] is True

            r_compact = client.post(f"/api/admin/compact/{src['service_id']}")
            assert r_compact.status_code == 200
            assert r_compact.json()["partitions_compacted"] == 1

            r_optimize = client.post(f"/api/admin/optimize/{src['service_id']}")
            assert r_optimize.status_code == 200
            assert r_optimize.json()["files_rewritten"] == 4
    finally:
        app.dependency_overrides.pop(get_source, None)


def test_admin_manual_triggers_reject_read_only_service(client: TestClient):
    """Analyst Path A (read_only service config) receives 403 Forbidden on manual write/ingest triggers."""
    ro_src = _make_source(access_level="read_only")
    app.dependency_overrides[get_source] = lambda: ro_src
    try:
        for path in (
            f"/api/admin/sync/{ro_src['service_id']}",
            f"/api/admin/commit/{ro_src['service_id']}",
        ):
            resp = client.post(path)
            assert resp.status_code == 403, f"Expected 403 on {path} for read_only service, got {resp.status_code}"
    finally:
        app.dependency_overrides.pop(get_source, None)


def test_analyst_path_b_rejected_with_403_on_admin_overview_routes():
    """Analyst Path B sessions are blocked by _is_blocked_path and require_admin on /api/admin/* routes."""
    for path in (
        "/api/admin/sync-status",
        "/api/sync-status",
        "/api/admin/system-metrics",
        "/api/admin/health",
        "/api/admin/health-snapshot",
        "/api/admin/compaction-status",
        "/api/admin/compaction-status/svc-admin-test",
        "/api/admin/log-accounting/svc-admin-test",
        "/api/admin/sync/svc-admin-test",
        "/api/admin/commit/svc-admin-test",
        "/api/admin/compact/svc-admin-test",
        "/api/admin/optimize/svc-admin-test",
    ):
        assert _is_blocked_path(path) is True, f"Expected {path} to be blocked for Analyst Path B"

    fake_req = SimpleNamespace(
        state=SimpleNamespace(analyst_session=SimpleNamespace(session_id="sess_analyst")),
        headers={},
        client=SimpleNamespace(host="203.0.113.55"),
    )
    with pytest.raises(HTTPException) as exc:
        require_admin(fake_req)  # type: ignore[arg-type]
    assert exc.value.status_code == 403
