"""Tests for the iceberg admin router."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from backend.deps import get_source
from backend.main import app


@pytest.fixture
def override_source():
    src = {
        "service_id": "service123",
        "name": "service123",
        "bucket": "test-bucket",
        "access_level": "read_write",
    }
    app.dependency_overrides[get_source] = lambda: src
    yield src
    app.dependency_overrides.clear()


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
        "calendar": {"2026-08-19": 5},
    }
    with patch("backend.core.iceberg.get_snapshot_calendar", return_value=fake_calendar) as mock_get:
        response = client.get("/api/admin/iceberg-calendar?table_name=logs")
        assert response.status_code == 200
        data = response.json()
        assert data["calendar"]["2026-08-19"] == 5
        mock_get.assert_called_once_with(override_source, table_name="logs")


def test_iceberg_commit_endpoint(client, override_source):
    with patch("backend.utils.router_utils.start_or_resume_cron", return_value={"ok": True}) as mock_start:
        response = client.post("/api/admin/commit-iceberg")
        assert response.status_code == 202
        mock_start.assert_called_once()


def test_rebuild_local_view_endpoint_success(client, override_source):
    with (
        patch("backend.core.iceberg.clear_source_caches") as mock_clear,
        patch("backend.core.duckdb._cache_dir", return_value="/tmp"),
        patch("os.path.exists", return_value=True),
        patch("os.remove") as mock_remove,
        patch("backend.core.duckdb.start_cron_run", return_value="run-123") as mock_start_cron,
        patch("backend.cron_progress.start_progress") as mock_prog,
        patch("threading.Thread") as mock_thread,
    ):
        response = client.post("/api/admin/rebuild-local-view")
        assert response.status_code == 202
        assert response.json()["run_id"] == "run-123"
        mock_clear.assert_called_once_with("service123")
        mock_remove.assert_called_once_with("/tmp/snapshot_files_cache.json")
        mock_start_cron.assert_called_once_with(override_source, "metadata_sync")
        mock_prog.assert_called_once_with("run-123", service_id="service123", task="metadata_sync")
        mock_thread.assert_called_once()


def test_rebuild_local_view_endpoint_os_remove_fails(client, override_source):
    with (
        patch("backend.core.iceberg.clear_source_caches"),
        patch("backend.core.duckdb._cache_dir", return_value="/tmp"),
        patch("os.path.exists", return_value=True),
        patch("os.remove", side_effect=OSError("denied")),
    ):
        response = client.post("/api/admin/rebuild-local-view")
        assert response.status_code == 500


def test_rebuild_local_view_endpoint_busy(client, override_source):
    with (
        patch("backend.core.iceberg.clear_source_caches"),
        patch("backend.core.duckdb._cache_dir", return_value="/tmp"),
        patch("os.path.exists", return_value=False),
        patch("backend.core.duckdb.start_cron_run", side_effect=RuntimeError("busy")),
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
