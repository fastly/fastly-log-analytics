"""Tests for :mod:`backend.cron.jobs.expire`.

``_run_expire_snapshots`` is the weekly cloud-maintenance / snapshot-
expiry job. It's a thin wrapper around ``iceberg.run_cloud_maintenance``;
these tests pin the wrapper's contract: skip on missing source / already-
running, log success when the maintenance call succeeds, log warning
when individual sub-steps return ``_error`` keys, log error on the
catalog-load top-level ``error``, and log error+exception on uncaught
exceptions.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from backend.cron.jobs import expire


@pytest.fixture
def stub_source(monkeypatch) -> dict:
    src = {"name": "fos-test-svc", "service_id": "svc-1", "bucket": "fos-test-bkt"}
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src)
    return src


@pytest.fixture
def stub_cron_log(monkeypatch) -> dict[str, MagicMock]:
    """``start_cron_run`` returns a stable run_id; ``log_cron_run`` is
    mocked so each test can inspect the (status, summary, error) tuple."""
    start = MagicMock(return_value=7)
    log = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log)
    monkeypatch.setattr("backend.cron.scheduler._display_name", lambda src, sid: src.get("name", sid))
    return {"start": start, "log": log}


def test_returns_when_source_missing(monkeypatch):
    """Missing source → early return; nothing touches cron_runs."""
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)
    start = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start)
    expire._run_expire_snapshots.__wrapped__("ghost-svc")
    start.assert_not_called()


def test_skips_when_start_cron_run_raises(monkeypatch, stub_source, stub_cron_log):
    """``start_cron_run`` raises (e.g. another instance already running)
    → log and return without invoking iceberg.run_cloud_maintenance."""
    stub_cron_log["start"].side_effect = RuntimeError("already running")
    maint = MagicMock()
    monkeypatch.setattr("backend.core.iceberg.run_cloud_maintenance", maint)

    expire._run_expire_snapshots.__wrapped__("svc-1")

    maint.assert_not_called()
    stub_cron_log["log"].assert_not_called()


def test_success_logs_summary_with_each_metric(monkeypatch, stub_source, stub_cron_log):
    """Happy path: maintenance returns a metrics dict (no errors) →
    log_cron_run records status='success' with key=value pairs in
    summary."""
    monkeypatch.setattr(
        "backend.core.iceberg.run_cloud_maintenance",
        MagicMock(return_value={"snapshots_expired": 5, "orphan_files_deleted": 12, "data_files_kept": 100}),
    )

    expire._run_expire_snapshots.__wrapped__("svc-1")

    stub_cron_log["log"].assert_called_once()
    args, kwargs = stub_cron_log["log"].call_args
    assert args[3] == "success"
    assert kwargs["error_message"] is None
    for fragment in ("snapshots_expired=5", "orphan_files_deleted=12", "data_files_kept=100"):
        assert fragment in kwargs["summary"]


def test_success_with_subtask_errors_logs_warning(monkeypatch, stub_source, stub_cron_log):
    """Per-subtask ``_error`` keys → status='warning' + error_message
    aggregates the failures so the next dashboard render shows the
    triage info without falsely flagging the whole run as failed."""
    monkeypatch.setattr(
        "backend.core.iceberg.run_cloud_maintenance",
        MagicMock(
            return_value={
                "snapshots_expired": 3,
                "orphan_files_deleted_error": "permission denied",
                "data_files_kept": 50,
            }
        ),
    )

    expire._run_expire_snapshots.__wrapped__("svc-1")

    args, kwargs = stub_cron_log["log"].call_args
    assert args[3] == "warning"
    assert "orphan_files_deleted_error" in kwargs["error_message"]
    assert "permission denied" in kwargs["error_message"]
    assert "snapshots_expired=3" in kwargs["summary"]


def test_catalog_load_error_logs_error(monkeypatch, stub_source, stub_cron_log):
    """Top-level ``error`` key (catalog load failure) → status='error'
    and a 'Maintenance failed at catalog load' summary."""
    monkeypatch.setattr(
        "backend.core.iceberg.run_cloud_maintenance",
        MagicMock(return_value={"error": "catalog unreachable"}),
    )

    expire._run_expire_snapshots.__wrapped__("svc-1")

    args, kwargs = stub_cron_log["log"].call_args
    assert args[3] == "error"
    assert kwargs["error_message"] == "catalog unreachable"
    assert "catalog load" in kwargs["summary"]


def test_unexpected_exception_logged_as_error(monkeypatch, stub_source, stub_cron_log):
    """An uncaught exception in run_cloud_maintenance → status='error',
    summary references the uncaught exception, error_message carries
    the str(exc). The finally-block logger.info(...) must not raise."""
    monkeypatch.setattr(
        "backend.core.iceberg.run_cloud_maintenance",
        MagicMock(side_effect=ConnectionError("S3 timeout")),
    )

    expire._run_expire_snapshots.__wrapped__("svc-1")

    args, kwargs = stub_cron_log["log"].call_args
    assert args[3] == "error"
    assert "S3 timeout" in kwargs["error_message"]
    assert "uncaught" in kwargs["summary"]
