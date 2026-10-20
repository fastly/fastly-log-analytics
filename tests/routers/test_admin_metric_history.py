"""Tests for the /api/admin/metric-history/batch endpoint.

The single-series ``GET /api/admin/metric-history`` route (and its
``MetricHistoryResponse`` model) was intentionally dropped in c14121b
("drop unused routes" — no UI caller; the /admin/trends page + System
Health sparklines read the batch endpoint). Only the batch endpoint
remains, so only its test does.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from backend.core import metric_snapshots
from backend.main import app
from backend.routers.admin import router as admin_router
from backend.utils.date_utils import iso_z
from backend.utils.remote_access import _is_blocked_path


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
