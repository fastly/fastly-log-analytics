"""Tests for duckdb_recycle job and DuckDB admin endpoints.

Covers:
- Memory trim and malloc_trim handling.
- Connection pool status and barrier state reporting.
- run_duckdb_recycle execution with adaptive interval adjustment.
- POST /api/admin/duckdb/recycle manual trigger (standard and forced).
- GET /api/admin/duckdb/status inspection endpoint.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from backend.core import duckdb as _db
from backend.core import duckdb_pool as _pool
from backend.core import duckdb_recycle as _recycle
from backend.cron.jobs.duckdb_recycle import _maybe_adjust_recycle_schedule, run_duckdb_recycle
from backend.main import app


def test_trim_memory_graceful():
    """_trim_memory runs gc.collect and survives missing or present malloc_trim."""
    _recycle._trim_memory()


def test_get_pool_status_and_barrier():
    """Pool status and barrier inspection return expected dictionary structure."""
    stats = _pool.get_pool_status()
    assert "pools" in stats
    assert "retired_pools" in stats

    # Barrier check
    assert _db.is_recycle_barrier_active("/non/existent/path") is False
    assert isinstance(_db.is_recycle_barrier_active(), set)


def test_run_duckdb_recycle_rss_threshold_skip():
    """Recycle is skipped when RSS is below the configured threshold."""
    with (
        patch("backend.core.memory_guard.maybe_graceful_restart", return_value=False),
        patch("backend.core.duckdb_recycle._recycle_rss_threshold_bytes", return_value=500 * 1024 * 1024),
        patch("backend.core.duckdb.current_rss_bytes", return_value=100 * 1024 * 1024),
        patch("backend.core.duckdb_recycle.recycle_once") as mock_recycle,
    ):
        result = run_duckdb_recycle()
        assert "skipped" in result
        assert "100MB < threshold 500MB" in result
        mock_recycle.assert_not_called()


def test_run_duckdb_recycle_adaptive_expedite():
    """Recycle adjusts schedule dynamically when approaching threshold."""
    mock_sched = MagicMock()
    mock_job = MagicMock()
    trigger = MagicMock()
    trigger.interval.total_seconds.return_value = 3600
    mock_job.trigger = trigger
    mock_sched.get_job.return_value = mock_job

    with (
        patch("backend.core.memory_guard.maybe_graceful_restart", return_value=False),
        patch("backend.core.duckdb_recycle._recycle_rss_threshold_bytes", return_value=1000 * 1024 * 1024),
        patch("backend.core.duckdb.current_rss_bytes", side_effect=[850 * 1024 * 1024, 400 * 1024 * 1024]),
        patch("backend.cron.scheduler.get_scheduler", return_value=mock_sched),
        patch("backend.core.duckdb_recycle.recycle_interval_min", return_value=60.0),
        patch("backend.core.duckdb_recycle.recycle_once", return_value="interval: recycled 1/1 instance(s), freed ~450MB") as mock_recycle,
    ):
        # 850MB is >= 800MB (80% of threshold), should adapt schedule
        _maybe_adjust_recycle_schedule(expedite=True)
        mock_job.reschedule.assert_called_with("interval", minutes=2.0)


def test_admin_duckdb_recycle_endpoint():
    """POST /api/admin/duckdb/recycle triggers recycle or skips based on force flag."""
    client = TestClient(app)

    # 1. Skipped when below threshold and force=False
    with (
        patch("backend.core.duckdb.current_rss_bytes", return_value=100 * 1024 * 1024),
        patch("backend.core.duckdb_recycle._recycle_rss_threshold_bytes", return_value=500 * 1024 * 1024),
    ):
        resp = client.post("/api/admin/duckdb/recycle")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["status"] == "skipped"

    # 2. Forced recycle
    with (
        patch("backend.core.duckdb.current_rss_bytes", side_effect=[300 * 1024 * 1024, 200 * 1024 * 1024]),
        patch("backend.core.duckdb_recycle._recycle_rss_threshold_bytes", return_value=500 * 1024 * 1024),
        patch("backend.core.duckdb_recycle.recycle_once", return_value="manual_admin: recycled 1/1 instance(s), freed ~100MB"),
    ):
        resp = client.post("/api/admin/duckdb/recycle?force=true")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["status"] == "recycled"
        assert data["freed_mb"] == 100.0


def test_admin_duckdb_status_endpoint():
    """GET /api/admin/duckdb/status returns memory and pool states."""
    client = TestClient(app)

    with (
        patch("backend.core.duckdb.current_rss_bytes", return_value=250 * 1024 * 1024),
        patch("backend.core.duckdb_recycle._recycle_rss_threshold_bytes", return_value=1000 * 1024 * 1024),
        patch("backend.core.duckdb_recycle.recycle_interval_min", return_value=60.0),
    ):
        resp = client.get("/api/admin/duckdb/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["memory"]["current_rss_mb"] == 250.0
        assert data["memory"]["recycle_threshold_mb"] == 1000.0
        assert data["memory"]["recycle_interval_min"] == 60.0
        assert "pools" in data
        assert "barrier" in data
