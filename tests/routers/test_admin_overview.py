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

from functools import partial
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from backend.deps import get_source, require_admin
from backend.main import app
from backend.utils.remote_access import _is_blocked_path
from tests._helpers.sources import make_source

_make_source = partial(make_source, "svc-admin-test", kind="cloud")


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


# ── Merged from test_admin_iceberg.py ──

import json

from backend.core.metadata.cron_log import CronLeaseHeldError


def test_iceberg_info_endpoint(client, override_source):

    fake_info = {
        "table_name": "logs",
        "table_location": "s3://bucket/prefix/iceberg",
        "snapshots": 5,
        "data_files": 12,
        "size_bytes": 1024,
        "latest_snapshot_at": "2026-08-19T00:00:00Z",
        "buffer_files": 1,
        "buffer_size_bytes": 512,
    }
    with patch("backend.core.iceberg.get_table_info", return_value=fake_info) as mock_get:
        response = client.get("/api/admin/iceberg-info?table_name=logs")
        assert response.status_code == 200
        data = response.json()
        assert data["size_bytes"] == 1024
        assert data["table_name"] == "logs"
        mock_get.assert_called_once_with(override_source, table_name="logs")


def test_iceberg_calendar_endpoint(client, override_source):
    fake_calendar = {
        "2026-08-19": {"data_files": 5, "size_bytes": 1024},
    }
    with patch("backend.core.iceberg.get_snapshot_calendar", return_value=fake_calendar) as mock_get:
        response = client.get("/api/admin/iceberg-calendar?table_name=logs")
        assert response.status_code == 200
        data = response.json()
        assert data["2026-08-19"] == {"data_files": 5, "size_bytes": 1024}
        mock_get.assert_called_once_with(override_source, table_name="logs")


def test_iceberg_commit_endpoint(client, override_source):
    with patch("backend.utils.router_utils.start_or_resume_cron", return_value={"ok": True}) as mock_start:
        response = client.post("/api/admin/commit-iceberg")
        assert response.status_code == 202
        mock_start.assert_called_once()


def test_rebuild_local_view_endpoint_success(client, override_source):
    with (
        patch("backend.core.iceberg.clear_source_caches") as mock_clear,
        patch("backend.core.duckdb.start_cron_run", return_value="run-123") as mock_start_cron,
        patch("backend.cron_progress.start_progress") as mock_prog,
        patch("threading.Thread") as mock_thread,
    ):
        response = client.post("/api/admin/rebuild-local-view")
        assert response.status_code == 202
        assert response.json()["run_id"] == "run-123"
        mock_clear.assert_called_once_with("service123")
        mock_start_cron.assert_called_once_with(override_source, "metadata_sync")
        mock_prog.assert_called_once_with("run-123", service_id="service123", task="metadata_sync")
        mock_thread.assert_called_once()


def test_rebuild_local_view_endpoint_busy(client, override_source):
    with (
        patch("backend.core.iceberg.clear_source_caches"),
        patch("backend.core.duckdb._cache_dir", return_value="/tmp"),
        patch("os.path.exists", return_value=False),
        patch("backend.core.duckdb.start_cron_run", side_effect=CronLeaseHeldError("busy")),
    ):
        response = client.post("/api/admin/rebuild-local-view")
        assert response.status_code == 503


def test_reset_logs_endpoint_read_only(client, override_source):
    override_source["access_level"] = "read_only"
    response = client.post(
        "/api/admin/reset-logs",
        json={"confirm": "service123", "delete_raw_logs": False, "preserve_usage_history": True},
    )
    assert response.status_code == 403


def test_reset_logs_endpoint_confirm_mismatch(client, override_source):
    response = client.post(
        "/api/admin/reset-logs",
        json={"confirm": "wrong", "delete_raw_logs": False, "preserve_usage_history": True},
    )
    assert response.status_code == 400


def test_reset_logs_endpoint_success(client, override_source):
    def fake_reset(*args, **kwargs):
        yield {"type": "status", "message": "starting"}
        yield {"type": "done", "message": "done"}

    with patch("backend.core.reset.reset_service_logs", side_effect=fake_reset):
        response = client.post(
            "/api/admin/reset-logs",
            json={"confirm": "service123", "delete_raw_logs": False, "preserve_usage_history": True},
        )
        assert response.status_code == 200
        assert "text/event-stream" in response.headers["content-type"]
        events = []
        for line in response.iter_lines():
            if line.startswith("data:"):
                events.append(json.loads(line[5:]))
        assert len(events) == 2
        assert events[0]["type"] == "status"
        assert events[1]["type"] == "done"


def test_reset_rum_endpoint_read_only(client, override_source):
    override_source["access_level"] = "read_only"
    response = client.post(
        "/api/admin/reset-rum",
        json={"confirm": "service123", "delete_raw_logs": False},
    )
    assert response.status_code == 403


def test_reset_rum_endpoint_confirm_mismatch(client, override_source):
    response = client.post(
        "/api/admin/reset-rum",
        json={"confirm": "wrong", "delete_raw_logs": False},
    )
    assert response.status_code == 400


def test_reset_rum_endpoint_success(client, override_source):
    def fake_reset(*args, **kwargs):
        yield {"type": "status", "message": "starting"}

    with patch("backend.core.reset.reset_service_rum", side_effect=fake_reset):
        response = client.post(
            "/api/admin/reset-rum",
            json={"confirm": "service123", "delete_raw_logs": False},
        )
        assert response.status_code == 200
        assert "text/event-stream" in response.headers["content-type"]
        events = []
        for line in response.iter_lines():
            if line.startswith("data:"):
                events.append(json.loads(line[5:]))
        assert len(events) == 1
        assert events[0]["type"] == "status"


# ── Merged from test_admin_metric_history.py ──

from datetime import UTC, datetime, timedelta

import pytest
from fastapi import FastAPI, Request

from backend.core import metric_snapshots
from backend.routers.admin import router as admin_router
from backend.utils.date_utils import iso_z


def test_metric_history_batch_groups_by_scope_key():
    t1 = iso_z(datetime.now(UTC) - timedelta(minutes=10))
    t2 = iso_z(datetime.now(UTC) - timedelta(minutes=5))
    metric_snapshots.record_snapshot("cpu_load_1m", 0.7, ts=t1)
    metric_snapshots.record_snapshot("cpu_load_1m", 0.9, ts=t2)
    metric_snapshots.record_snapshot("pool_wait_p95_ms", 5.0, service_id="svc-a", ts=t2)
    metric_snapshots.record_snapshot("cron_duration_ms", 800.0, service_id="svc-a", task="sync", ts=t2)

    with TestClient(app) as client:
        r = client.get("/api/admin/metric-history/batch?since=1h")

    assert r.status_code == 200
    series = r.json()["series"]
    assert "cpu_load_1m" in series
    assert "pool_wait_p95_ms|svc-a" in series
    assert "cron_duration_ms|svc-a|sync" in series
    # Points within a series are ordered chronologically ascending
    cpu_points = series["cpu_load_1m"]
    assert len(cpu_points) >= 2
    assert cpu_points[-2]["value"] == pytest.approx(0.7)
    assert cpu_points[-1]["value"] == pytest.approx(0.9)
    assert cpu_points[-2]["ts"] < cpu_points[-1]["ts"]


@pytest.mark.parametrize(
    ("window", "expected_points"),
    [
        ("30m", 1),
        ("1h", 2),
        ("6h", 3),
        ("24h", 4),
        ("7d", 5),
    ],
)
def test_metric_history_batch_windows(window: str, expected_points: int):
    metric_name = f"test_window_metric_{window}"
    now = datetime.now(UTC)
    metric_snapshots.record_snapshot(metric_name, 10.0, ts=iso_z(now - timedelta(minutes=10)))
    metric_snapshots.record_snapshot(metric_name, 20.0, ts=iso_z(now - timedelta(minutes=45)))
    metric_snapshots.record_snapshot(metric_name, 30.0, ts=iso_z(now - timedelta(hours=3)))
    metric_snapshots.record_snapshot(metric_name, 40.0, ts=iso_z(now - timedelta(hours=12)))
    metric_snapshots.record_snapshot(metric_name, 50.0, ts=iso_z(now - timedelta(days=3)))

    with TestClient(app) as client:
        r = client.get(f"/api/admin/metric-history/batch?since={window}")

    assert r.status_code == 200
    body = r.json()
    assert "series" in body
    pts = body["series"].get(metric_name, [])
    assert len(pts) == expected_points


@pytest.mark.parametrize("bad_window", ["invalid", "0h", "-5h", "1w", "abc", ""])
def test_metric_history_batch_rejects_invalid_window(bad_window: str):
    with TestClient(app) as client:
        r = client.get(f"/api/admin/metric-history/batch?since={bad_window}")

    assert r.status_code == 400
    assert "Invalid lookback window" in r.json()["detail"]["error"]


def test_metric_history_batch_rejects_analyst_path_b():
    # 1. Middleware path gate blocks /api/admin/metric-history/batch
    assert _is_blocked_path("/api/admin/metric-history/batch") is True

    # 2. Route-level Depends(require_admin) also rejects Analyst Path B sessions with 403 admin_only
    analyst_app = FastAPI()

    @analyst_app.middleware("http")
    async def _stamp_analyst(request: Request, call_next):
        request.state.analyst_session = SimpleNamespace(
            session_id="sess-analyst-1",
            service_ids=["svc-a"],
            email="analyst@example.com",
        )
        return await call_next(request)

    analyst_app.include_router(admin_router)

    with TestClient(analyst_app) as client:
        r = client.get("/api/admin/metric-history/batch?since=1h")

    assert r.status_code == 403
    detail = r.json().get("detail", r.json())
    assert detail["error"] == "admin_only"
