"""Tests for backend.cron.jobs.rum_commit.

``_run_rum_commit`` is currently a Phase 3 placeholder (no real compaction
work yet) but it must still honor the cron-run bookkeeping contract every
real cron job follows: start a run, log success/error with the right
status, and always finalize the run in the ``finally`` block — including
when the (future) body raises. Uses ``.__wrapped__`` to call the
undecorated function directly, same pattern as tests/cron/test_commit.py.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from backend.cron.jobs import rum_commit


def test_success_path_starts_and_finalizes_run(monkeypatch):
    start = MagicMock(return_value=42)
    log = MagicMock()
    finalize = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log)
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", finalize)
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"provisioning": {"cron_sync": {"enabled": True}}})
    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service",
        lambda sid: {"name": sid, "service_id": sid, "access_level": "read_write"},
    )
    monkeypatch.setattr(
        "backend.core.iceberg.commit_buffer", lambda *args, **kwargs: {"files_committed": 0, "rows_committed": 0}
    )
    monkeypatch.setattr("backend.core.iceberg.sync_data", lambda *args, **kwargs: None)

    rum_commit._run_rum_commit.__wrapped__("svc-1")

    start.assert_called_once_with({"name": "svc-1", "service_id": "svc-1", "access_level": "read_write"}, "rum_commit")
    log_args, log_kwargs = log.call_args
    assert log_args[0] == {"name": "svc-1", "service_id": "svc-1", "access_level": "read_write"}
    assert log_args[1] == "rum_commit"
    assert log_args[3] == "success"
    assert log_kwargs["rows_ingested"] == 0
    assert log_kwargs["run_id"] == 42
    finalize.assert_called_once_with(
        {"name": "svc-1", "service_id": "svc-1", "access_level": "read_write"}, "rum_commit", 42
    )


def test_exception_logs_error_status_finalizes_and_reraises(monkeypatch):
    start = MagicMock(return_value=99)
    log = MagicMock()
    finalize = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log)
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", finalize)
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"provisioning": {"cron_sync": {"enabled": True}}})
    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service",
        lambda sid: {"name": sid, "service_id": sid, "access_level": "read_write"},
    )
    monkeypatch.setattr(
        "backend.core.iceberg.commit_buffer", lambda *args, **kwargs: {"files_committed": 0, "rows_committed": 0}
    )
    monkeypatch.setattr("backend.core.iceberg.sync_data", lambda *args, **kwargs: None)

    def _boom(*args, **kwargs):
        raise RuntimeError("compaction backend unavailable")

    # Force the try body to raise by making log_cron_run's FIRST call (the
    # 'done' path) blow up, which the except/finally must still handle.
    log.side_effect = [RuntimeError("compaction backend unavailable"), None]

    try:
        rum_commit._run_rum_commit.__wrapped__("svc-1")
        raised = None
    except RuntimeError as e:
        raised = e

    assert raised is not None
    assert "compaction backend unavailable" in str(raised)

    # First call attempted 'done', second call (from the except block) logs 'error'.
    assert log.call_count == 2
    error_args, error_kwargs = log.call_args_list[1]
    assert error_args[3] == "error"
    assert error_kwargs["error_message"] == "compaction backend unavailable"
    assert error_kwargs["run_id"] == 99

    # finally always finalizes, even though the function re-raised.
    finalize.assert_called_once_with(
        {"name": "svc-1", "service_id": "svc-1", "access_level": "read_write"}, "rum_commit", 99
    )
