"""Tests for :mod:`backend.cron.jobs.commit`.

``_run_commit`` drains the local buffer to the shared Iceberg table.
It's a wrapper around ``iceberg.commit_buffer`` with config / disk /
cron-run plumbing around it. Tests stub the heavy commit and pin
the orchestration shape — same scaffold as test_sync_job /
test_compaction_jobs / test_expire / test_optimize.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from backend.cron.jobs import commit


@pytest.fixture
def stub_load_config(monkeypatch):
    cfg = {
        "service_id": "svc-1",
        "name": "svc-1",
        "provisioning": {"cron_sync": {"enabled": True, "commit_interval_mins": 5}},
    }
    load = MagicMock(return_value=cfg)
    monkeypatch.setattr("backend.config.load_config", load)
    return {"load": load, "cfg": cfg}


@pytest.fixture
def stub_source(monkeypatch) -> dict:
    src = {"name": "fos-test-svc", "service_id": "svc-1", "bucket": "fos-test-bkt", "access_level": "read_write"}
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src)
    return src


@pytest.fixture
def stub_cron_envelope(monkeypatch) -> dict[str, MagicMock]:
    start = MagicMock(return_value=33)
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
    monkeypatch.setattr("backend.cron.scheduler._check_disk_space", lambda *a, **kw: (True, ""))
    monkeypatch.setattr("backend.cron.scheduler._check_buffer_backlog", lambda *a, **kw: "")
    monkeypatch.setattr("backend.cron.scheduler._extract_log_text", lambda rid: "")
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda src: "/tmp/cache")
    monkeypatch.setattr("backend.cron.jobs._common.finalize_cron_duration", MagicMock())
    monkeypatch.setattr("backend.cron.jobs._common.refresh_view_and_warm_pool", MagicMock())
    # Post-commit metadata-sync resolves off backend.cron.jobs.metadata.
    monkeypatch.setattr("backend.cron.jobs.metadata._run_metadata_sync", MagicMock())
    return {
        "start": start,
        "log": log,
        "start_progress": start_progress,
        "end_progress": end_progress,
        "log_event": log_event,
    }


def test_returns_when_config_missing(monkeypatch):
    monkeypatch.setattr("backend.config.load_config", MagicMock(return_value=None))
    get_src = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", get_src)
    commit._run_commit.__wrapped__("ghost-svc")
    get_src.assert_not_called()


def test_returns_when_source_missing(monkeypatch, stub_load_config):
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)
    start = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start)
    commit._run_commit.__wrapped__("svc-1")
    start.assert_not_called()


def test_read_only_source_skipped_without_force(monkeypatch, stub_load_config):
    """A read_only source must NOT run commit unless force=True is
    passed (manual admin trigger). This is the contract that prevents
    a read-only mirror from clobbering the writer's snapshots."""
    ro_src = {"name": "ro", "service_id": "svc-1", "access_level": "read_only"}
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: ro_src)
    start = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start)

    commit._run_commit.__wrapped__("svc-1")  # force defaults False

    start.assert_not_called()


def test_disabled_sync_skipped_without_force(monkeypatch, stub_load_config, stub_source):
    """cron_sync.enabled=False without force → no-op. The shared
    ``cron_sync`` config gates both ingest and commit so they can be
    suspended together for maintenance windows."""
    stub_load_config["cfg"]["provisioning"]["cron_sync"]["enabled"] = False
    start = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start)
    commit._run_commit.__wrapped__("svc-1")
    start.assert_not_called()


def test_disk_check_failure_logs_error_and_returns(monkeypatch, stub_load_config, stub_source, stub_cron_envelope):
    """Disk space below floor → log_cron_run with 'error' + 'Commit
    aborted: ...', commit_buffer never runs. The whole point of the
    pre-check is that a midway disk failure during commit would corrupt
    the iceberg state — refusing to start is the safe move."""
    # ``_check_disk_space`` was bound on ``backend.cron.jobs.commit`` at
    # import time via ``from backend.cron.scheduler import _check_disk_space``.
    # Patch where the name lives so the body resolves to our mock.
    monkeypatch.setattr("backend.cron.jobs.commit._check_disk_space", lambda *a, **kw: (False, "disk 95% full"))
    cb = MagicMock()
    monkeypatch.setattr("backend.core.iceberg.commit_buffer", cb)

    commit._run_commit.__wrapped__("svc-1")

    cb.assert_not_called()
    args, kwargs = stub_cron_envelope["log"].call_args
    assert args[3] == "error"
    assert "disk 95% full" in kwargs["error_message"]
    assert "Commit aborted" in kwargs["summary"]


def test_success_with_committed_files_logs_summary_and_refreshes_view(
    monkeypatch, stub_load_config, stub_source, stub_cron_envelope
):
    """Happy path: commit_buffer returns rows_committed > 0 →
    log_cron_run records 'success' with the row count + snapshot id,
    refresh_view_and_warm_pool fires, and metadata_sync gets kicked."""
    monkeypatch.setattr(
        "backend.core.iceberg.commit_buffer",
        MagicMock(return_value={"files_committed": 2, "rows_committed": 1234, "snapshot_id": 8675309}),
    )

    commit._run_commit.__wrapped__("svc-1")

    args, kwargs = stub_cron_envelope["log"].call_args
    assert args[3] == "success"
    assert kwargs["rows_ingested"] == 1234
    assert "1234 rows" in kwargs["summary"]
    assert "8675309" in kwargs["summary"]

    from backend.cron.jobs import _common
    from backend.cron.jobs.metadata import _run_metadata_sync as _sync_shim

    _common.refresh_view_and_warm_pool.assert_called_once()
    _sync_shim.assert_called_once_with("svc-1")


def test_success_no_files_committed_logs_no_new_data(monkeypatch, stub_load_config, stub_source, stub_cron_envelope):
    """Empty buffer → status='success' (NOT a failure mode) with the
    'No new data to commit' summary. metadata_sync should NOT be
    triggered (no new data to sync)."""
    monkeypatch.setattr(
        "backend.core.iceberg.commit_buffer",
        MagicMock(return_value={"files_committed": 0, "rows_committed": 0, "snapshot_id": None}),
    )

    commit._run_commit.__wrapped__("svc-1")

    args, kwargs = stub_cron_envelope["log"].call_args
    assert args[3] == "success"
    assert "No new data" in kwargs["summary"]

    from backend.cron.jobs.metadata import _run_metadata_sync as _sync_shim

    _sync_shim.assert_not_called()


def test_quarantined_files_appear_in_summary(monkeypatch, stub_load_config, stub_source, stub_cron_envelope):
    """commit_buffer reports unreadable buffer files via
    ``quarantined_files``; the summary must surface the count so the
    user can act on the corrupt-input signal."""
    monkeypatch.setattr(
        "backend.core.iceberg.commit_buffer",
        MagicMock(
            return_value={
                "files_committed": 5,
                "rows_committed": 100,
                "snapshot_id": 1,
                "quarantined_files": 2,
            }
        ),
    )

    commit._run_commit.__wrapped__("svc-1")

    args, kwargs = stub_cron_envelope["log"].call_args
    assert args[3] == "success"
    assert "quarantined 2" in kwargs["summary"]


def test_unexpected_exception_logged_as_error(monkeypatch, stub_load_config, stub_source, stub_cron_envelope):
    """Uncaught exception in commit_buffer → status='error' AND
    end_progress STILL fires in the finally block. Pinned because a
    skipped end_progress would leak a 'running' progress row that
    the dashboard's live indicator never clears."""
    monkeypatch.setattr(
        "backend.core.iceberg.commit_buffer",
        MagicMock(side_effect=RuntimeError("snapshot conflict")),
    )

    commit._run_commit.__wrapped__("svc-1")

    args, kwargs = stub_cron_envelope["log"].call_args
    assert args[3] == "error"
    assert "snapshot conflict" in kwargs["error_message"]
    stub_cron_envelope["end_progress"].assert_called_once()
