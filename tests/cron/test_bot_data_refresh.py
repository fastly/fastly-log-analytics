"""Tests for bot_data_refresh cron job and bot-sources refresh endpoints.

Covers:
- _run_bot_data_refresh success, warning on partial failure, error on total failure, and dev_mode_no_crons skip.
- POST /api/admin/bot-sources/refresh and POST /api/admin/bots/refresh admin endpoints.
"""

from __future__ import annotations

from unittest.mock import patch

from fastapi.testclient import TestClient

from backend.cron.jobs.metadata import _run_bot_data_refresh
from backend.main import app


def test_run_bot_data_refresh_records_success_with_total_entries():
    """Successful refresh records job result with total entry count and status='success'."""
    fake_results = [
        {"id": "well-known-bots", "entry_count": 100},
        {"id": "tor-exit-nodes", "entry_count": 50},
    ]

    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.utils.bot_sources.refresh_all_sources", return_value=fake_results),
        patch("backend.utils.system_jobs.record_job_run") as mock_record,
    ):
        _run_bot_data_refresh()

    mock_record.assert_called_once()
    args = mock_record.call_args[0]
    assert args[0] == "bot_data_refresh"
    assert args[1] == "success"
    summary = args[3]
    assert "2" in summary
    assert "150" in summary


def test_run_bot_data_refresh_records_warning_on_partial_failure():
    """When some bot sources fail, status is recorded as 'warning'."""
    fake_results = [
        {"id": "well-known-bots", "entry_count": 100},
        {"id": "tor-exit-nodes", "error": "HTTP 503 Service Unavailable", "entry_count": 0, "failed": True},
    ]

    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.utils.bot_sources.refresh_all_sources", return_value=fake_results),
        patch("backend.utils.system_jobs.record_job_run") as mock_record,
    ):
        _run_bot_data_refresh()

    mock_record.assert_called_once()
    args = mock_record.call_args[0]
    assert args[0] == "bot_data_refresh"
    assert args[1] == "warning"
    summary = args[3]
    assert "1 source(s) failed: tor-exit-nodes" in summary


def test_run_bot_data_refresh_records_error_on_total_failure():
    """When all bot sources fail, status is recorded as 'error'."""
    fake_results = [
        {"id": "well-known-bots", "error": "Connection refused", "entry_count": 0, "failed": True},
        {"id": "tor-exit-nodes", "error": "DNS resolution failed", "entry_count": 0, "failed": True},
    ]

    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.utils.bot_sources.refresh_all_sources", return_value=fake_results),
        patch("backend.utils.system_jobs.record_job_run") as mock_record,
    ):
        _run_bot_data_refresh()

    mock_record.assert_called_once()
    args = mock_record.call_args[0]
    assert args[0] == "bot_data_refresh"
    assert args[1] == "error"
    summary = args[3]
    assert "All 2 bot sources failed" in summary


def test_run_bot_data_refresh_skips_when_dev_mode_active():
    """When FLA_DEV_NO_CRONS=1 is set, refresh skips outbound HTTP requests."""
    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=True),
        patch("backend.utils.bot_sources.refresh_all_sources") as mock_refresh,
        patch("backend.utils.system_jobs.record_job_run") as mock_record,
    ):
        _run_bot_data_refresh()

    mock_refresh.assert_not_called()
    mock_record.assert_called_once()
    args = mock_record.call_args[0]
    assert args[0] == "bot_data_refresh"
    assert args[1] == "skipped"
    assert "dev_mode_no_crons" in args[3]


def test_admin_refresh_all_bot_sources_endpoints():
    """POST /api/admin/bot-sources/refresh and /api/admin/bots/refresh trigger refresh_all_sources."""
    client = TestClient(app)
    fake_results = [
        {"id": "well-known-bots", "entry_count": 100},
        {"id": "tor-exit-nodes", "entry_count": 50},
    ]

    with patch("backend.utils.bot_sources.refresh_all_sources", return_value=fake_results) as mock_refresh:
        # Test primary endpoint
        resp = client.post("/api/admin/bot-sources/refresh")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["updated_count"] == 2
        assert data["failed_count"] == 0

        # Test alias endpoint
        resp_alias = client.post("/api/admin/bots/refresh")
        assert resp_alias.status_code == 200
        data_alias = resp_alias.json()
        assert data_alias["ok"] is True
        assert data_alias["updated_count"] == 2

        assert mock_refresh.call_count == 2
