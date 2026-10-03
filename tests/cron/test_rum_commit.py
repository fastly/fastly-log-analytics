"""Tests for ``backend.cron.jobs.rum_commit``."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from backend.cron.jobs import rum_commit as rum_commit_mod

SERVICE_ID = "svc_test_rum_commit"

FAKE_CFG = {
    "service_id": SERVICE_ID,
    "name": "Test Service",
    "provisioning": {"cron_sync": {"enabled": True}},
    "rum": {"enabled": True},
}

FAKE_SRC = {
    "service_id": SERVICE_ID,
    "name": "Test Service",
    "access_level": "read_write",
}


from contextlib import contextmanager


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)

    @contextmanager
    def mock_get_connection(*args, **kwargs):
        mock_con = MagicMock()
        mock_con.execute.return_value.fetchall.return_value = []
        yield mock_con

    import backend.core.iceberg._ducklake as _dl

    monkeypatch.setattr("backend.core.duckdb.get_connection", mock_get_connection)
    monkeypatch.setattr("backend.core.rollups.rum.recompute_rum_aggregates", MagicMock())
    monkeypatch.setattr(_dl, "_ducklake_attach", MagicMock())


def test_rum_commit_success(monkeypatch):
    """When both vitals and errors commit buffer successfully:
    - Status is 'success'
    - Total rows and summary reflect both tables
    - _mark_ledger_published(rum=True) is called
    - Progress is cleanly started and ended
    """
    log_calls = []
    ledger_calls = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 999)
    monkeypatch.setattr("backend.cron.scheduler._check_disk_space", lambda dir, sid, task: (True, ""))
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda src: "/tmp/cache")
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run",
        lambda *args, **kwargs: log_calls.append((args, kwargs)),
    )
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", lambda *a, **k: None)

    def mock_commit_buffer(src, table_name):
        if table_name == "client_vitals":
            return {"files_committed": 2, "rows_committed": 50}
        elif table_name == "client_errors":
            return {"files_committed": 1, "rows_committed": 10}
        return {}

    monkeypatch.setattr("backend.core.iceberg.commit_buffer", mock_commit_buffer)
    monkeypatch.setattr("backend.core.iceberg.sync_data", lambda src, table_name: None)
    monkeypatch.setattr(
        "backend.core.ingest._mark_ledger_published", lambda sid, rum=False: ledger_calls.append((sid, rum))
    )

    start_progress = MagicMock()
    end_progress = MagicMock()
    monkeypatch.setattr("backend.cron_progress.start_progress", start_progress)
    monkeypatch.setattr("backend.cron_progress.end_progress", end_progress)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())

    rum_commit_mod._run_rum_commit.__wrapped__(SERVICE_ID)

    start_progress.assert_called_once_with(999, service_id=SERVICE_ID, task="rum_commit")
    end_progress.assert_called_once_with(999)
    assert len(ledger_calls) == 1
    assert ledger_calls[0] == (SERVICE_ID, True)

    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert kwargs.get("run_id") == 999
    assert args[3] == "success"  # status
    assert kwargs.get("rows_ingested") == 60
    assert "Committed 2 vitals files (50 rows) and 1 errors files (10 rows)" in kwargs.get("summary", "")


def test_rum_commit_partial_failure_warning(monkeypatch):
    """When vitals commit succeeds but errors commit raises:
    - Status is 'warning'
    - Partial accounting is logged in summary
    - _mark_ledger_published is NOT called (raw files must not be deleted if errors failed)
    - Exception is not re-raised out of the cron task
    """
    log_calls = []
    ledger_calls = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 1001)
    monkeypatch.setattr("backend.cron.scheduler._check_disk_space", lambda dir, sid, task: (True, ""))
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda src: "/tmp/cache")
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run",
        lambda *args, **kwargs: log_calls.append((args, kwargs)),
    )
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", lambda *a, **k: None)

    def mock_commit_buffer(src, table_name):
        if table_name == "client_vitals":
            return {"files_committed": 3, "rows_committed": 75}
        raise RuntimeError("FOS connection timeout on client_errors")

    monkeypatch.setattr("backend.core.iceberg.commit_buffer", mock_commit_buffer)
    monkeypatch.setattr("backend.core.iceberg.sync_data", lambda src, table_name: None)
    monkeypatch.setattr(
        "backend.core.ingest._mark_ledger_published", lambda sid, rum=False: ledger_calls.append((sid, rum))
    )
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())

    rum_commit_mod._run_rum_commit.__wrapped__(SERVICE_ID)

    assert len(ledger_calls) == 0  # Not called because errors failed
    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert args[3] == "warning"
    assert kwargs.get("rows_ingested") == 75
    assert "Partial RUM commit" in kwargs.get("summary", "")
    assert "errors failed" in kwargs.get("summary", "")
    assert "FOS connection timeout" in kwargs.get("error_message", "")


def test_rum_commit_active_request_politeness(monkeypatch):
    """When active queries are present:
    - Background automated ticks defer
    - Manual ticks (is_manual=True) or forced ticks (force=True) bypass deferral
    """
    start_calls = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: start_calls.append(task) or 1002)
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda job, sid: True)

    # Automated background run -> defers
    rum_commit_mod._run_rum_commit.__wrapped__(SERVICE_ID)
    assert len(start_calls) == 0

    # Force run -> proceeds
    with patch("backend.cron.scheduler._check_disk_space", return_value=(False, "disk full")):
        with patch("backend.core.duckdb.log_cron_run"):
            rum_commit_mod._run_rum_commit.__wrapped__(SERVICE_ID, force=True)
            assert len(start_calls) == 1

    # Manual run -> proceeds
    with patch("backend.cron.scheduler._check_disk_space", return_value=(False, "disk full")):
        with patch("backend.core.duckdb.log_cron_run"):
            rum_commit_mod._run_rum_commit.__wrapped__(SERVICE_ID, is_manual=True)
            assert len(start_calls) == 2


def test_rum_commit_aborts_on_low_disk(monkeypatch):
    """When disk space pre-check fails:
    - Status is logged as 'error'
    - Commit and table sync are not attempted
    """
    log_calls = []
    commit_calls = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 1003)
    monkeypatch.setattr(
        "backend.cron.scheduler._check_disk_space", lambda dir, sid, task: (False, "Disk free space is below 500MB")
    )
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda src: "/tmp/cache")
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run",
        lambda *args, **kwargs: log_calls.append((args, kwargs)),
    )
    monkeypatch.setattr("backend.core.iceberg.commit_buffer", lambda src, table_name: commit_calls.append(table_name))

    rum_commit_mod._run_rum_commit.__wrapped__(SERVICE_ID)

    assert len(commit_calls) == 0
    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert args[3] == "error"
    assert "Disk free space is below 500MB" in kwargs.get("error_message", "")
    assert "RUM commit aborted" in kwargs.get("summary", "")


def test_rum_commit_aggregates_self_heal_on_stale_lake(monkeypatch):
    """RUM aggregate recompute self-heals the cold-start lake-detach race (Trap #35).

    A concurrent DETACH can rip ``lake`` out from under the recompute mid-tick,
    raising ``schema "lake" does not exist`` — the same race the requests rollup
    COPY path self-heals. The recompute must route through
    ``execute_with_stale_view_retry`` so it re-attaches and retries once instead
    of silently dropping that tick's aggregates.
    """
    import duckdb

    recompute_calls = {"n": 0}

    def flaky_recompute(con, service_id, hours=None):
        recompute_calls["n"] += 1
        if recompute_calls["n"] == 1:
            raise duckdb.CatalogException(
                'Catalog Error: Table with name "lake.logs_svc__client_vitals" '
                'does not exist because schema "lake" does not exist.'
            )
        return None

    def _retry_double(con, source, fn, *args, table_name="logs", **kwargs):
        try:
            return fn(con, *args, **kwargs)
        except Exception:
            return fn(con, *args, **kwargs)

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 1004)
    monkeypatch.setattr("backend.cron.scheduler._check_disk_space", lambda dir, sid, task: (True, ""))
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda src: "/tmp/cache")
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **k: None)
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", lambda *a, **k: None)
    monkeypatch.setattr(
        "backend.core.iceberg.commit_buffer", lambda src, table_name: {"files_committed": 1, "rows_committed": 5}
    )
    monkeypatch.setattr("backend.core.iceberg.sync_data", lambda src, table_name: None)
    monkeypatch.setattr("backend.core.ingest._mark_ledger_published", lambda sid, rum=False: None)
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())
    monkeypatch.setattr("backend.core.rollups.rum.recompute_rum_aggregates", flaky_recompute)
    monkeypatch.setattr("backend.core.iceberg.execute_with_stale_view_retry", _retry_double)

    rum_commit_mod._run_rum_commit.__wrapped__(SERVICE_ID)

    assert recompute_calls["n"] == 2, "recompute must retry once after the stale-lake self-heal"
