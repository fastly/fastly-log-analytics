"""Tests for :mod:`backend.cron.jobs.compaction`.

Both cron entry points (``_run_local_compact`` and
``_run_rollup_compact_daily``) are wrapper-shaped: look up source,
open a cron run, call the underlying compaction function, log
success/error, close the run. Tests stub the heavy lifting and pin
the orchestration shape.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from backend.cron.jobs import compaction


@pytest.fixture
def stub_source(monkeypatch) -> dict:
    """Make ``get_source_for_service`` return a stable dict for any service id."""
    src = {"name": "fos-test-svc", "service_id": "svc-1", "bucket": "fos-test-bkt"}
    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service",
        lambda sid: src,
    )
    return src


@pytest.fixture
def stub_progress(monkeypatch) -> dict[str, MagicMock]:
    """Replace cron_progress + run-id helpers with mocks the test can inspect."""
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=42))
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
    # cron_progress helpers — start, end, cleanup. The decorators import
    # them lazily inside the function bodies so we have to patch the
    # source modules, not local references.
    start_progress = MagicMock()
    end_progress = MagicMock()
    cleanup = MagicMock()
    monkeypatch.setattr("backend.cron_progress.start_progress", start_progress)
    monkeypatch.setattr("backend.cron_progress.end_progress", end_progress)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", cleanup)
    log_event = MagicMock()
    monkeypatch.setattr("backend.cron.scheduler._log_and_add_progress", log_event)
    monkeypatch.setattr("backend.cron.scheduler._display_name", lambda src, sid: src.get("name", sid))
    monkeypatch.setattr("backend.cron.scheduler._extract_log_text", lambda rid: "")
    return {
        "start_progress": start_progress,
        "end_progress": end_progress,
        "cleanup": cleanup,
        "log_event": log_event,
    }


# ── _run_local_compact ────────────────────────────────────────────────────────


def test_local_compact_returns_when_source_missing(monkeypatch):
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)
    # Must not raise.
    compaction._run_local_compact.__wrapped__("missing-svc")


def test_local_compact_skips_when_start_cron_run_raises(monkeypatch, stub_source, stub_progress):
    """``start_cron_run`` raises RuntimeError when another instance of this
    cron is already in-flight. The job should log and return without
    touching the underlying compaction logic."""
    monkeypatch.setattr(
        "backend.core.duckdb.start_cron_run",
        MagicMock(side_effect=RuntimeError("already running")),
    )
    compact_mock = MagicMock()
    monkeypatch.setattr("backend.core.local_compaction.compact_local_partitions", compact_mock)

    compaction._run_local_compact.__wrapped__("svc-1")

    compact_mock.assert_not_called()
    stub_progress["start_progress"].assert_not_called()


def test_local_compact_success_logs_and_records(monkeypatch, stub_source, stub_progress):
    monkeypatch.setattr(
        "backend.core.local_compaction.compact_local_partitions",
        MagicMock(
            return_value={
                "partitions_compacted": 3,
                "files_merged": 12,
                "files_removed": 9,
                "errors": [],
            }
        ),
    )

    compaction._run_local_compact.__wrapped__("svc-1")

    # log_cron_run was called with status='success' and a non-error summary.
    from backend.core import duckdb as _db

    _db.log_cron_run.assert_called_once()
    args, kwargs = _db.log_cron_run.call_args
    # Positional: (src, task, duration, status). Keyword: summary, error_message, run_id, log_output.
    assert args[3] == "success"
    assert "Compacted 3 partition" in kwargs["summary"]
    assert kwargs["error_message"] is None
    # Progress lifecycle closed.
    stub_progress["start_progress"].assert_called_once()
    stub_progress["end_progress"].assert_called_once()


def test_local_compact_records_warning_when_errors_present(monkeypatch, stub_source, stub_progress):
    monkeypatch.setattr(
        "backend.core.local_compaction.compact_local_partitions",
        MagicMock(
            return_value={
                "partitions_compacted": 2,
                "files_merged": 4,
                "files_removed": 4,
                "errors": ["partition a failed", "partition b failed", "partition c failed", "partition d"],
            }
        ),
    )

    compaction._run_local_compact.__wrapped__("svc-1")

    from backend.core import duckdb as _db

    args, kwargs = _db.log_cron_run.call_args
    assert args[3] == "warning"
    assert "4 partition error" in kwargs["summary"]
    # First 3 errors surface; the rest are truncated.
    assert "partition a failed" in kwargs["error_message"]
    assert "1 more" in kwargs["error_message"]


def test_local_compact_records_error_on_exception(monkeypatch, stub_source, stub_progress):
    monkeypatch.setattr(
        "backend.core.local_compaction.compact_local_partitions",
        MagicMock(side_effect=RuntimeError("disk full")),
    )

    compaction._run_local_compact.__wrapped__("svc-1")

    from backend.core import duckdb as _db

    args, kwargs = _db.log_cron_run.call_args
    assert args[3] == "error"
    assert "disk full" in kwargs["error_message"]
    # end_progress STILL fires (in the finally block).
    stub_progress["end_progress"].assert_called_once()


# ── _run_rollup_compact_daily ────────────────────────────────────────────────


def test_rollup_compact_returns_when_source_missing(monkeypatch):
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)
    compaction._run_rollup_compact_daily.__wrapped__("missing-svc")


def test_rollup_compact_skips_when_start_cron_run_raises(monkeypatch, stub_source, stub_progress):
    monkeypatch.setattr(
        "backend.core.duckdb.start_cron_run",
        MagicMock(side_effect=RuntimeError("already running")),
    )
    compact_mock = MagicMock()
    monkeypatch.setattr("backend.core.rollups.compact_closed_days_to_daily", compact_mock)

    compaction._run_rollup_compact_daily.__wrapped__("svc-1")

    compact_mock.assert_not_called()


def test_rollup_compact_success_records_rebuilt_and_bundled(monkeypatch, stub_source, stub_progress):
    monkeypatch.setattr("backend.core.rollups.compact_closed_days_to_daily", MagicMock(return_value=14))
    monkeypatch.setattr("backend.core.rollups.backfill_day_bundles", MagicMock(return_value=7))

    compaction._run_rollup_compact_daily.__wrapped__("svc-1")

    from backend.core import duckdb as _db

    args, kwargs = _db.log_cron_run.call_args
    assert args[3] == "success"
    assert "Rebuilt 14" in kwargs["summary"]
    assert "bundled 7 day" in kwargs["summary"]


def test_rollup_compact_logs_warning_when_bundle_step_fails(monkeypatch, stub_source, stub_progress, caplog):
    monkeypatch.setattr("backend.core.rollups.compact_closed_days_to_daily", MagicMock(return_value=5))
    monkeypatch.setattr(
        "backend.core.rollups.backfill_day_bundles",
        MagicMock(side_effect=RuntimeError("disk full")),
    )

    import logging as _logging

    with caplog.at_level(_logging.WARNING, logger="backend.scheduler"):
        compaction._run_rollup_compact_daily.__wrapped__("svc-1")

    from backend.core import duckdb as _db

    args, kwargs = _db.log_cron_run.call_args
    # Still success — bundling is best-effort.
    assert args[3] == "success"
    # Summary shows 0 bundled.
    assert "bundled 0 day" in kwargs["summary"]


def test_rollup_compact_records_error_on_exception(monkeypatch, stub_source, stub_progress):
    monkeypatch.setattr(
        "backend.core.rollups.compact_closed_days_to_daily",
        MagicMock(side_effect=RuntimeError("manifest read failed")),
    )

    compaction._run_rollup_compact_daily.__wrapped__("svc-1")

    from backend.core import duckdb as _db

    args, kwargs = _db.log_cron_run.call_args
    assert args[3] == "error"
    assert "manifest read failed" in kwargs["error_message"]


# ── _run_rollup_hour_heal ─────────────────────────────────────────────────────


def test_rollup_heal_returns_when_source_missing(monkeypatch):
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)
    compaction._run_rollup_hour_heal.__wrapped__("missing-svc")


def test_rollup_heal_defers_under_api_load(monkeypatch, stub_source, stub_progress):
    """The heal's view scan is CPU-bound — an in-flight API request wins."""
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda *a, **k: True)
    heal_mock = MagicMock()
    monkeypatch.setattr("backend.core.rollups.backfill_missing_hour_bundles", heal_mock)

    compaction._run_rollup_hour_heal.__wrapped__("svc-1")

    heal_mock.assert_not_called()


def test_rollup_heal_skips_when_start_cron_run_raises(monkeypatch, stub_source, stub_progress):
    monkeypatch.setattr(
        "backend.core.duckdb.start_cron_run",
        MagicMock(side_effect=RuntimeError("already running")),
    )
    heal_mock = MagicMock()
    monkeypatch.setattr("backend.core.rollups.backfill_missing_hour_bundles", heal_mock)

    compaction._run_rollup_hour_heal.__wrapped__("svc-1")

    heal_mock.assert_not_called()


def test_rollup_heal_success_uses_one_day_lookback(monkeypatch, stub_source, stub_progress):
    """The hourly tick must stay CHEAP: 1-day lookback, not the daily
    deep pass's 30 — the whole point of the split cadence."""
    heal_mock = MagicMock(return_value={"missing": 2, "rebuilt_fields": 8, "bundled": 2})
    monkeypatch.setattr("backend.core.rollups.backfill_missing_hour_bundles", heal_mock)

    compaction._run_rollup_hour_heal.__wrapped__("svc-1")

    assert heal_mock.call_args.kwargs.get("lookback_days") == 1

    from backend.core import duckdb as _db

    args, kwargs = _db.log_cron_run.call_args
    assert args[1] == "rollup_hour_heal"
    assert args[3] == "success"
    assert "Healed 2 missing hour(s)" in kwargs["summary"]
    assert "2 hour(s) bundled" in kwargs["summary"]


def test_rollup_heal_records_error_on_exception(monkeypatch, stub_source, stub_progress):
    monkeypatch.setattr(
        "backend.core.rollups.backfill_missing_hour_bundles",
        MagicMock(side_effect=RuntimeError("view refresh failed")),
    )

    compaction._run_rollup_hour_heal.__wrapped__("svc-1")

    from backend.core import duckdb as _db

    args, kwargs = _db.log_cron_run.call_args
    assert args[3] == "error"
    assert "view refresh failed" in kwargs["error_message"]
