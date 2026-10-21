"""Tests for ngwaf_sync background job and NGWAF admin endpoints.

Covers:
- _run_ngwaf_bot_sync dev_mode_no_crons skip.
- First-time sync watermark initialization.
- Paged bot fetch, enrichment, upsert, and cleanup.
- Adaptive rescheduling to 2m under high volume/budget exceeded.
- Error classification for auth errors (401/403) vs transient errors.
- POST /api/admin/ngwaf/sync/{service_id} and GET /api/admin/ngwaf/status endpoints.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from backend.core.metadata.cron_log import CronLeaseHeldError
from backend.cron.jobs.metadata import _run_ngwaf_bot_sync
from backend.main import app


@pytest.fixture
def fake_cfg():
    return {
        "service_id": "test_service",
        "name": "Test Service",
        "fastly_api_key": "test_api_key",
        "ngwaf": {
            "workspace_id": "ws_123",
        },
        "provisioning": {
            "cron_ngwaf": {
                "interval_mins": 5,
                "log_retention_days": 30,
            }
        },
    }


def test_ngwaf_sync_skips_when_dev_mode_active():
    """dev_mode_no_crons skips NGWAF sync without loading config or hitting APIs."""
    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=True),
        patch("backend.config.load_config") as mock_load,
    ):
        _run_ngwaf_bot_sync("test_service")

    mock_load.assert_not_called()


def test_ngwaf_sync_seeds_watermark_on_first_run(fake_cfg):
    """When no watermark exists, seeds current timestamp and skips fetching."""
    mock_src = {"name": "test_service", "id": "test_service"}

    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws_123"),
        patch("backend.core.duckdb.get_source_for_service", return_value=mock_src),
        patch("backend.core.duckdb.start_cron_run", return_value=1),
        patch("backend.core.duckdb.log_cron_run") as mock_log,
        patch("backend.utils.ngwaf_bot_cache.ensure_schema"),
        patch("backend.utils.ngwaf_bot_cache.get_last_timestamp", return_value=None),
        patch("backend.utils.ngwaf_bot_cache.update_sync_watermark") as mock_update_wm,
        patch("backend.utils.ngwaf.fetch_verified_bots_paged") as mock_fetch,
    ):
        _run_ngwaf_bot_sync("test_service")

    mock_update_wm.assert_called_once()
    mock_fetch.assert_not_called()
    mock_log.assert_called_once()
    assert mock_log.call_args[0][3] == "success"
    assert "First sync" in mock_log.call_args[1]["summary"]


def test_ngwaf_sync_pages_records_and_restores_baseline_interval(fake_cfg):
    """Normal sync pages records, enriches, upserts, and maintains baseline interval."""
    mock_src = {"name": "test_service", "id": "test_service"}
    fake_records = [
        {"waf_req_id": "req_1", "bot_name": "Googlebot", "user_agent": "Googlebot/2.1"},
        {"waf_req_id": "req_2", "bot_name": "Bingbot", "user_agent": "bingbot/2.0"},
    ]

    mock_sched = MagicMock()
    mock_job = MagicMock()
    mock_sched.get_job.return_value = mock_job

    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws_123"),
        patch("backend.core.duckdb.get_source_for_service", return_value=mock_src),
        patch("backend.core.duckdb.start_cron_run", return_value=1),
        patch("backend.core.duckdb.log_cron_run") as mock_log,
        patch("backend.utils.ngwaf_bot_cache.ensure_schema"),
        patch("backend.utils.ngwaf_bot_cache.get_last_timestamp", return_value="2026-09-01T00:00:00Z"),
        patch(
            "backend.utils.ngwaf.fetch_verified_bots_paged",
            return_value=[(fake_records, "2026-09-01T01:00:00Z", 2)],
        ),
        patch("backend.utils.ngwaf_bot_cache.upsert_bots") as mock_upsert,
        patch("backend.utils.ngwaf_bot_cache.cleanup_old_bots", return_value=0),
        patch("backend.cron.scheduler.get_scheduler", return_value=mock_sched),
    ):
        _run_ngwaf_bot_sync("test_service")

    mock_upsert.assert_called_once()
    mock_log.assert_called_once()
    assert mock_log.call_args[0][3] == "success"
    assert "Synced 2 bot record(s)" in mock_log.call_args[1]["summary"]

    # Low volume (< 500) -> restores baseline interval of 5 mins
    mock_job.reschedule.assert_called_with("interval", minutes=5)


def test_ngwaf_sync_adaptive_rescheduling_under_heavy_load(fake_cfg):
    """When synced records >= 500, adaptive rescheduling tightens interval to 2 minutes."""
    mock_src = {"name": "test_service", "id": "test_service"}
    fake_records = [{"waf_req_id": f"req_{i}", "bot_name": "Googlebot"} for i in range(500)]

    mock_sched = MagicMock()
    mock_job = MagicMock()
    mock_sched.get_job.return_value = mock_job

    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws_123"),
        patch("backend.core.duckdb.get_source_for_service", return_value=mock_src),
        patch("backend.core.duckdb.start_cron_run", return_value=1),
        patch("backend.core.duckdb.log_cron_run") as mock_log,
        patch("backend.utils.ngwaf_bot_cache.ensure_schema"),
        patch("backend.utils.ngwaf_bot_cache.get_last_timestamp", return_value="2026-09-01T00:00:00Z"),
        patch(
            "backend.utils.ngwaf.fetch_verified_bots_paged",
            return_value=[(fake_records, "2026-09-01T01:00:00Z", 500)],
        ),
        patch("backend.utils.ngwaf_bot_cache.upsert_bots"),
        patch("backend.utils.ngwaf_bot_cache.cleanup_old_bots", return_value=0),
        patch("backend.cron.scheduler.get_scheduler", return_value=mock_sched),
    ):
        _run_ngwaf_bot_sync("test_service")

    mock_log.assert_called_once()
    assert mock_log.call_args[0][3] == "success"
    # Adaptive reschedule triggered:
    mock_job.reschedule.assert_called_with("interval", minutes=2)


def test_ngwaf_sync_classifies_auth_errors(fake_cfg):
    """Auth errors (401/403) are clearly identified in cron run summary."""
    mock_src = {"name": "test_service", "id": "test_service"}

    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws_123"),
        patch("backend.core.duckdb.get_source_for_service", return_value=mock_src),
        patch("backend.core.duckdb.start_cron_run", return_value=1),
        patch("backend.core.duckdb.log_cron_run") as mock_log,
        patch("backend.utils.ngwaf_bot_cache.ensure_schema"),
        patch("backend.utils.ngwaf_bot_cache.get_last_timestamp", return_value="2026-09-01T00:00:00Z"),
        patch(
            "backend.utils.ngwaf.fetch_verified_bots_paged",
            side_effect=RuntimeError("Fastly API 401 Unauthorized: invalid_api_key"),
        ),
    ):
        _run_ngwaf_bot_sync("test_service")

    mock_log.assert_called_once()
    assert mock_log.call_args[0][3] == "error"
    summary = mock_log.call_args[1]["summary"]
    assert "authentication error (check Fastly API key / workspace permissions)" in summary


def test_admin_ngwaf_endpoints(fake_cfg):
    """Test POST /api/admin/ngwaf/sync/{service_id} and GET /api/admin/ngwaf/status."""
    client = TestClient(app)

    with (
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws_123"),
        patch("backend.cron.jobs.metadata._run_ngwaf_bot_sync") as mock_run,
        patch(
            "backend.utils.ngwaf_bot_cache.get_cache_stats",
            return_value={"total_cached_bots": 42, "workspaces": {"ws_123": "2026-09-01T00:00:00Z"}, "top_bots": {}},
        ),
    ):
        # Trigger sync
        sync_resp = client.post("/api/admin/ngwaf/sync/test_service")
        assert sync_resp.status_code == 200
        sync_data = sync_resp.json()
        assert sync_data["ok"] is True
        assert sync_data["service_id"] == "test_service"
        assert sync_data["stats"]["total_cached_bots"] == 42
        mock_run.assert_called_once_with("test_service")

        # Get status
        status_resp = client.get("/api/admin/ngwaf/status?service_id=test_service")
        assert status_resp.status_code == 200
        status_data = status_resp.json()
        assert status_data["ok"] is True
        assert status_data["total_cached_bots"] == 42
        assert status_data["service_configured"] is True


def test_ngwaf_sync_adaptive_rescheduling_on_budget_exceeded(fake_cfg):
    """When execution runtime exceeds 240s budget, job halts and tightens interval to 2m."""
    mock_src = {"name": "test_service", "id": "test_service"}
    fake_records = [{"waf_req_id": f"req_{i}", "bot_name": "Googlebot"} for i in range(5)]

    mock_sched = MagicMock()
    mock_job = MagicMock()
    mock_sched.get_job.return_value = mock_job

    # Advance the simulated clock when the paged fetch yields so background
    # threads (e.g. psycopg_pool maintenance) or logging calls cannot consume
    # the start timestamp before cron_run.__enter__ records _start_time.
    clock = [1000.0]

    def mock_time():
        return clock[0]

    def fake_fetch_paged(*_args, **_kwargs):
        clock[0] = 1245.0
        yield (fake_records, "2026-09-01T01:00:00Z", 5)

    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws_123"),
        patch("backend.core.duckdb.get_source_for_service", return_value=mock_src),
        patch("backend.core.duckdb.start_cron_run", return_value=1),
        patch("backend.core.duckdb.log_cron_run") as mock_log,
        patch("backend.utils.ngwaf_bot_cache.ensure_schema"),
        patch("backend.utils.ngwaf_bot_cache.get_last_timestamp", return_value="2026-09-01T00:00:00Z"),
        patch(
            "backend.utils.ngwaf.fetch_verified_bots_paged",
            side_effect=fake_fetch_paged,
        ),
        patch("backend.utils.ngwaf_bot_cache.upsert_bots"),
        patch("backend.utils.ngwaf_bot_cache.cleanup_old_bots", return_value=0),
        patch("backend.cron.scheduler.get_scheduler", return_value=mock_sched),
        patch("time.time", side_effect=mock_time),
    ):
        _run_ngwaf_bot_sync("test_service")

    mock_log.assert_called_once()
    assert mock_log.call_args[0][3] == "success"
    summary = mock_log.call_args[1]["summary"]
    assert "budget reached — will continue next run" in summary
    assert "Synced 5 bot record(s)" in summary
    mock_job.reschedule.assert_called_with("interval", minutes=2)


def test_ngwaf_sync_progress_lifecycle(fake_cfg):
    """Verifies start_progress, end_progress, and finalize_cron_duration are called in full lifecycle."""
    mock_src = {"name": "test_service", "id": "test_service"}
    fake_records = [{"waf_req_id": "req_1", "bot_name": "Googlebot"}]

    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws_123"),
        patch("backend.core.duckdb.get_source_for_service", return_value=mock_src),
        patch("backend.core.duckdb.start_cron_run", return_value=42),
        patch("backend.core.duckdb.log_cron_run"),
        patch("backend.utils.ngwaf_bot_cache.ensure_schema"),
        patch("backend.utils.ngwaf_bot_cache.get_last_timestamp", return_value="2026-09-01T00:00:00Z"),
        patch(
            "backend.utils.ngwaf.fetch_verified_bots_paged",
            return_value=[(fake_records, "2026-09-01T01:00:00Z", 1)],
        ),
        patch("backend.utils.ngwaf_bot_cache.upsert_bots"),
        patch("backend.utils.ngwaf_bot_cache.cleanup_old_bots", return_value=0),
        patch("backend.cron_progress.start_progress") as mock_start_prog,
        patch("backend.cron_progress.end_progress") as mock_end_prog,
        patch("backend.cron.jobs._common.finalize_cron_duration") as mock_finalize,
    ):
        _run_ngwaf_bot_sync("test_service")

    mock_start_prog.assert_called_once_with(42, service_id="test_service", task="ngwaf_sync")
    mock_end_prog.assert_called_once_with(42)
    mock_finalize.assert_called_once()
    assert mock_finalize.call_args[0][0] == mock_src
    assert mock_finalize.call_args[0][1] == 42


def test_ngwaf_sync_guaranteed_cleanup_on_error(fake_cfg):
    """Verifies end_progress and finalize_cron_duration run in finally even if an error occurs."""
    mock_src = {"name": "test_service", "id": "test_service"}

    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws_123"),
        patch("backend.core.duckdb.get_source_for_service", return_value=mock_src),
        patch("backend.core.duckdb.start_cron_run", return_value=99),
        patch("backend.core.duckdb.log_cron_run") as mock_log,
        patch("backend.utils.ngwaf_bot_cache.ensure_schema"),
        patch("backend.utils.ngwaf_bot_cache.get_last_timestamp", return_value="2026-09-01T00:00:00Z"),
        patch("backend.utils.ngwaf.fetch_verified_bots_paged", side_effect=ValueError("Unexpected fetch failure")),
        patch("backend.cron_progress.end_progress") as mock_end_prog,
        patch("backend.cron.jobs._common.finalize_cron_duration") as mock_finalize,
    ):
        _run_ngwaf_bot_sync("test_service")

    mock_log.assert_called_once()
    assert mock_log.call_args[0][3] == "error"
    mock_end_prog.assert_called_once_with(99)
    mock_finalize.assert_called_once()
    assert mock_finalize.call_args[0][0] == mock_src
    assert mock_finalize.call_args[0][1] == 99


def test_ngwaf_sync_first_run_watermark_guaranteed_cleanup(fake_cfg):
    """Verifies end_progress and finalize_cron_duration execute even on first-run watermark seed early exit."""
    mock_src = {"name": "test_service", "id": "test_service"}

    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws_123"),
        patch("backend.core.duckdb.get_source_for_service", return_value=mock_src),
        patch("backend.core.duckdb.start_cron_run", return_value=101),
        patch("backend.core.duckdb.log_cron_run") as mock_log,
        patch("backend.utils.ngwaf_bot_cache.ensure_schema"),
        patch("backend.utils.ngwaf_bot_cache.get_last_timestamp", return_value=None),
        patch("backend.utils.ngwaf_bot_cache.update_sync_watermark"),
        patch("backend.cron_progress.end_progress") as mock_end_prog,
        patch("backend.cron.jobs._common.finalize_cron_duration") as mock_finalize,
    ):
        _run_ngwaf_bot_sync("test_service")

    mock_log.assert_called_once()
    assert mock_log.call_args[0][3] == "success"
    mock_end_prog.assert_called_once_with(101)
    mock_finalize.assert_called_once()
    assert mock_finalize.call_args[0][0] == mock_src
    assert mock_finalize.call_args[0][1] == 101


def test_ngwaf_sync_lease_held_is_clean_skip(fake_cfg):
    mock_src = {"name": "test_service", "id": "test_service"}

    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws_123"),
        patch("backend.core.duckdb.get_source_for_service", return_value=mock_src),
        patch("backend.core.duckdb.start_cron_run", side_effect=CronLeaseHeldError("held")),
        patch("backend.core.duckdb.log_cron_run") as mock_log,
        patch("backend.utils.ngwaf_bot_cache.ensure_schema"),
        patch("backend.utils.ngwaf.fetch_verified_bots_paged") as mock_fetch,
        patch("backend.cron_progress.end_progress") as mock_end,
    ):
        _run_ngwaf_bot_sync("test_service")

    mock_log.assert_not_called()
    mock_fetch.assert_not_called()
    mock_end.assert_not_called()


def test_ngwaf_sync_error_row_matches_legacy_shape(fake_cfg):
    mock_src = {"name": "test_service", "id": "test_service"}

    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws_123"),
        patch("backend.core.duckdb.get_source_for_service", return_value=mock_src),
        patch("backend.core.duckdb.start_cron_run", return_value=5),
        patch("backend.core.duckdb.log_cron_run") as mock_log,
        patch("backend.utils.ngwaf_bot_cache.ensure_schema"),
        patch("backend.utils.ngwaf_bot_cache.get_last_timestamp", return_value="2026-09-01T00:00:00Z"),
        patch("backend.utils.ngwaf.fetch_verified_bots_paged", side_effect=RuntimeError("socket reset")),
    ):
        _run_ngwaf_bot_sync("test_service")

    mock_log.assert_called_once()
    args, kwargs = mock_log.call_args
    assert args[1] == "ngwaf_sync"
    assert args[3] == "error"
    assert kwargs["summary"] == "NGWAF sync failed: socket reset"
    assert kwargs["error_message"] == "socket reset"
    assert kwargs["files_downloaded"] == 0
    assert kwargs["run_id"] == 5
    assert kwargs["log_output"] is None
