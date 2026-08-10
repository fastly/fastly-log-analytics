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
    monkeypatch.setattr(rum_commit, "start_cron_run", start)
    monkeypatch.setattr(rum_commit, "log_cron_run", log)
    monkeypatch.setattr(rum_commit, "finalize_cron_run_if_running", finalize)

    rum_commit._run_rum_commit.__wrapped__("svc-1")

    start.assert_called_once_with("svc-1", "rum_commit")
    log_args, log_kwargs = log.call_args
    assert log_args[0] == "svc-1"
    assert log_args[1] == "rum_commit"
    assert log_args[3] == "done"
    assert log_kwargs["rows_ingested"] == 0
    assert log_kwargs["run_id"] == 42
    finalize.assert_called_once_with("svc-1", "rum_commit", 42)


def test_exception_logs_error_status_finalizes_and_reraises(monkeypatch):
    start = MagicMock(return_value=99)
    log = MagicMock()
    finalize = MagicMock()
    monkeypatch.setattr(rum_commit, "start_cron_run", start)
    monkeypatch.setattr(rum_commit, "log_cron_run", log)
    monkeypatch.setattr(rum_commit, "finalize_cron_run_if_running", finalize)

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
    finalize.assert_called_once_with("svc-1", "rum_commit", 99)
