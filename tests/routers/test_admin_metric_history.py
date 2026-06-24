"""Tests for the /api/admin/metric-history/batch endpoint.

The single-series ``GET /api/admin/metric-history`` route (and its
``MetricHistoryResponse`` model) was intentionally dropped in c14121b
("drop unused routes" — no UI caller; the /admin/trends page + System
Health sparklines read the batch endpoint). Only the batch endpoint
remains, so only its test does.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from backend.core import metric_snapshots
from backend.main import app


def test_metric_history_batch_groups_by_scope_key():
    metric_snapshots.record_snapshot("cpu_load_1m", 0.7)
    metric_snapshots.record_snapshot("pool_wait_p95_ms", 5.0, service_id="svc-a")
    metric_snapshots.record_snapshot("cron_duration_ms", 800.0, service_id="svc-a", task="sync")

    with TestClient(app) as client:
        r = client.get("/api/admin/metric-history/batch?since=1h")

    assert r.status_code == 200
    series = r.json()["series"]
    assert "cpu_load_1m" in series
    assert "pool_wait_p95_ms|svc-a" in series
    assert "cron_duration_ms|svc-a|sync" in series
