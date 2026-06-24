"""Tests for :mod:`backend.cron.jobs.optimize`.

``_run_optimize`` is the daily Iceberg small-file compaction cron. It
wraps ``iceberg.optimize_table`` with the standard cron envelope:
defer-gate, start_cron_run, progress lifecycle, log_cron_run with the
right status and counts. Tests pin the wrapper's behaviour without
exercising the heavy compaction itself.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from backend.cron.jobs import optimize


@pytest.fixture
def stub_source(monkeypatch) -> dict:
    src = {"name": "fos-test-svc", "service_id": "svc-1", "bucket": "fos-test-bkt"}
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src)
    return src


@pytest.fixture
def stub_cron_envelope(monkeypatch) -> dict[str, MagicMock]:
    """Stub the standard cron envelope helpers so the body's
    progress + cron_runs side effects can be inspected without real DB
    writes."""
    start = MagicMock(return_value=11)
    log = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log)

    start_progress = MagicMock()
    end_progress = MagicMock()
    cleanup = MagicMock()
    log_event = MagicMock()
    monkeypatch.setattr("backend.cron_progress.start_progress", start_progress)
    monkeypatch.setattr("backend.cron_progress.end_progress", end_progress)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", cleanup)
    monkeypatch.setattr("backend.cron.scheduler._log_and_add_progress", log_event)
    monkeypatch.setattr("backend.cron.scheduler._display_name", lambda src, sid: src.get("name", sid))
    monkeypatch.setattr("backend.cron.scheduler._extract_log_text", lambda rid: "")
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda *a, **kw: False)
    monkeypatch.setattr("backend.cron.jobs._common.finalize_cron_duration", MagicMock())
    return {
        "start": start,
        "log": log,
        "start_progress": start_progress,
        "end_progress": end_progress,
        "log_event": log_event,
    }


def test_returns_when_should_defer_cron_true(monkeypatch):
    """Active-request gate fires → bail out before source lookup."""
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda *a, **kw: True)
    get_src = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", get_src)
    optimize._run_optimize.__wrapped__("svc-1")
    get_src.assert_not_called()


def test_returns_when_source_missing(monkeypatch):
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda *a, **kw: False)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)
    start = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start)
    optimize._run_optimize.__wrapped__("ghost-svc")
    start.assert_not_called()


def test_skips_when_start_cron_run_raises(monkeypatch, stub_source, stub_cron_envelope):
    """``start_cron_run`` raises (another instance in-flight) → log a
    skip message and return; iceberg.optimize_table never runs."""
    stub_cron_envelope["start"].side_effect = RuntimeError("already running")
    opt = MagicMock()
    monkeypatch.setattr("backend.core.iceberg.optimize_table", opt)

    optimize._run_optimize.__wrapped__("svc-1")

    opt.assert_not_called()
    stub_cron_envelope["log"].assert_not_called()
    stub_cron_envelope["start_progress"].assert_not_called()


def test_success_records_rewritten_and_added_counts(monkeypatch, stub_source, stub_cron_envelope):
    monkeypatch.setattr(
        "backend.core.iceberg.optimize_table",
        MagicMock(return_value={"files_rewritten": 12, "files_added": 3, "eligible_partitions": 2}),
    )

    optimize._run_optimize.__wrapped__("svc-1")

    args, kwargs = stub_cron_envelope["log"].call_args
    assert args[3] == "success"
    assert kwargs["parquet_files_optimized"] == 12
    assert kwargs["parquet_files_created"] == 3
    assert "Rewrote 12" in kwargs["summary"]
    assert "into 3" in kwargs["summary"]
    stub_cron_envelope["end_progress"].assert_called_once()


def test_partition_errors_with_files_added_logs_warning(monkeypatch, stub_source, stub_cron_envelope):
    """Some partitions failed but files_added > 0 → warning, not error."""
    monkeypatch.setattr(
        "backend.core.iceberg.optimize_table",
        MagicMock(
            return_value={
                "files_rewritten": 8,
                "files_added": 2,
                "eligible_partitions": 5,
                "partition_errors": ["a failed", "b failed"],
            }
        ),
    )

    optimize._run_optimize.__wrapped__("svc-1")

    args, kwargs = stub_cron_envelope["log"].call_args
    assert args[3] == "warning"
    assert "2/5 partitions failed" in kwargs["summary"]
    assert "a failed" in kwargs["error_message"]


def test_partition_errors_with_zero_files_added_logs_error(monkeypatch, stub_source, stub_cron_envelope):
    """All partitions failed (files_added == 0 alongside partition_errors)
    → status='error'. Distinguishing this from 'warning' is the contract
    the dashboard alert routing depends on."""
    monkeypatch.setattr(
        "backend.core.iceberg.optimize_table",
        MagicMock(
            return_value={
                "files_rewritten": 0,
                "files_added": 0,
                "eligible_partitions": 4,
                "partition_errors": ["x", "y", "z", "w"],
            }
        ),
    )

    optimize._run_optimize.__wrapped__("svc-1")

    args, kwargs = stub_cron_envelope["log"].call_args
    assert args[3] == "error"
    assert "4/4 partitions failed" in kwargs["summary"]
    # First 3 errors surface; remaining are truncated for log-line budget.
    assert "(1 more)" in kwargs["error_message"]


def test_top_level_error_key_logs_error(monkeypatch, stub_source, stub_cron_envelope):
    """``result['error']`` (catalog load failure) → status='error' and
    both an 'error' and a 'warning' progress event get emitted (per
    optimize.py's deliberate double-emit pattern)."""
    monkeypatch.setattr(
        "backend.core.iceberg.optimize_table",
        MagicMock(return_value={"error": "schema mismatch"}),
    )

    optimize._run_optimize.__wrapped__("svc-1")

    args, kwargs = stub_cron_envelope["log"].call_args
    assert args[3] == "error"
    assert "schema mismatch" in kwargs["error_message"]


def test_unexpected_exception_logged_and_end_progress_fires(monkeypatch, stub_source, stub_cron_envelope):
    """An uncaught exception in optimize_table → status='error' AND
    end_progress runs in the finally block."""
    monkeypatch.setattr(
        "backend.core.iceberg.optimize_table",
        MagicMock(side_effect=RuntimeError("manifest read failed")),
    )

    optimize._run_optimize.__wrapped__("svc-1")

    args, kwargs = stub_cron_envelope["log"].call_args
    assert args[3] == "error"
    assert "manifest read failed" in kwargs["error_message"]
    stub_cron_envelope["end_progress"].assert_called_once()
