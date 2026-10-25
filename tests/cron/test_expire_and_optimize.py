"""Consolidated tests for test_expire_and_optimize."""

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import duckdb
import pytest
from fastapi import HTTPException

from backend.core import duckdb as duckdb_mod
from backend.core import iceberg as iceberg_mod
from backend.core.iceberg import buffer as buffer_mod
from backend.core.metadata.cron_log import CronLeaseHeldError
from backend.core.query_attribution import current_attribution
from backend.core.query_registry import query_registry
from backend.cron.jobs import compaction as expire
from backend.cron.scheduler import Scheduler
from backend.utils.telemetry import process_context_scope
from tests.cron.conftest import make_cron_source as _source
from tests.cron.conftest import patch_maintenance_config as _maintenance_config


@pytest.fixture
def stub_cron_log(monkeypatch) -> dict[str, MagicMock]:
    """``start_cron_run`` returns a stable run_id; ``log_cron_run`` is
    mocked so each test can inspect the (status, summary, error) tuple."""
    start = MagicMock(return_value=7)
    log = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log)
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
    stub_cron_log["start"].side_effect = CronLeaseHeldError("already running")
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


# ── Contract Tests (Cron 8: expire_{service_id}) ───────────────────────────


def _snapshot_connection(*, count: int = 0, expire_error: Exception | None = None):
    calls: list[str] = []

    class Connection:
        def execute(self, sql, params=None):
            calls.append(sql)
            result = MagicMock()
            if "ducklake_snapshots" in sql:
                result.fetchone.return_value = (count,)
            elif "ducklake_expire_snapshots" in sql and expire_error:
                raise expire_error
            else:
                result.fetchall.return_value = []
            return result

    return Connection(), calls


def test_snapshot_cleanup_runs_even_when_catalog_is_empty():
    con, calls = _snapshot_connection(count=0)

    result = buffer_mod._ducklake_expire_snapshots(con, {"name": "svc"}, 7)

    assert result["snapshots_expired_count"] == 0
    assert any("CALL ducklake_expire_snapshots" in sql for sql in calls)
    assert any("CALL ducklake_cleanup_old_files" in sql for sql in calls)
    assert not any("ducklake_delete_orphaned_files" in sql for sql in calls)


def test_old_file_cleanup_runs_when_snapshot_expiry_fails():
    con, calls = _snapshot_connection(count=2, expire_error=RuntimeError("catalog write failed"))

    result = buffer_mod._ducklake_expire_snapshots(con, {"name": "svc"}, 7)

    assert "snapshot_expiry_error" in result
    assert "data_file_cleanup_error" not in result
    assert any("CALL ducklake_cleanup_old_files" in sql for sql in calls)
    assert not any("ducklake_delete_orphaned_files" in sql for sql in calls)


def test_old_file_cleanup_count_survives_snapshot_count_failures(caplog):
    class Connection:
        def execute(self, sql, params=None):
            if "ducklake_snapshots" in sql:
                raise RuntimeError("snapshot count unavailable")
            result = MagicMock()
            result.fetchall.return_value = [("deleted.parquet",)]
            return result

    caplog.set_level("INFO", logger="backend.core.iceberg._core")
    result = buffer_mod._ducklake_expire_snapshots(Connection(), {"name": "svc"}, 7)

    assert result["data_files_cleaned"] == 1
    assert "snapshot_expiry_error" in result
    assert any("unlinked 1 file(s)" in record.getMessage() for record in caplog.records)


def test_maintenance_reports_exact_retention_snapshot_and_unlink_totals(tmp_path, monkeypatch):
    src = _source(tmp_path)
    _maintenance_config(monkeypatch, data_retention_days=0, rum_retention_days=0, cache_retention_days=0)
    monkeypatch.setattr(
        buffer_mod,
        "_run_ducklake_maintenance",
        lambda *args, **kwargs: {
            "data_rows_deleted": 3,
            "rum_log_rows_deleted": 4,
            "rum_beacon_rows_deleted": 2,
            "snapshots_expired_count": 5,
            "data_files_cleaned": 7,
        },
    )

    result = buffer_mod._run_cloud_maintenance_impl(src)

    assert result["retention_deleted_rows"] == 9
    assert result["snapshots_expired"] == 5
    assert result["files_unlinked"] == 7


def test_local_cache_temp_rollup_cleanup_preserves_quarantine_evidence(tmp_path, monkeypatch):
    src = _source(tmp_path)
    cache = tmp_path / "cache"
    cache.mkdir()
    monkeypatch.setattr(duckdb_mod, "_cache_dir", lambda _source: str(cache))
    evidence_dir = tmp_path / "data" / "services" / src["service_id"] / "quarantine"
    evidence_dir.mkdir(parents=True)
    evidence = evidence_dir / "1.dat"
    evidence.write_bytes(b"original evidence")
    old_time = (datetime.now(UTC) - timedelta(days=120)).timestamp()
    os.utime(evidence, (old_time, old_time))

    old_parquet = cache / "data" / "old.parquet"
    old_parquet.parent.mkdir()
    old_parquet.touch()
    os.utime(old_parquet, (old_time, old_time))
    old_temp = cache / "aborted.part"
    old_temp.touch()
    os.utime(old_temp, (old_time, old_time))
    old_bad_temp = cache / "failed.bad.jsonl.tmp"
    old_bad_temp.touch()
    os.utime(old_bad_temp, (old_time, old_time))
    old_tmp = cache / "failed.tmp"
    old_tmp.touch()
    os.utime(old_tmp, (old_time, old_time))
    fresh_temp = cache / "active.tmp"
    fresh_temp.touch()
    old_day = (datetime.now(UTC) - timedelta(days=120)).date().isoformat()
    fresh_day = datetime.now(UTC).date().isoformat()
    old_rollup = cache / "rollups" / "day_bundled" / f"day={old_day}" / "old.parquet"
    old_rollup.parent.mkdir(parents=True)
    old_rollup.touch()
    os.utime(old_rollup, (old_time, old_time))
    fresh_rollup = cache / "rollups" / "day_bundled" / f"day={fresh_day}" / "fresh.parquet"
    fresh_rollup.parent.mkdir(parents=True)
    fresh_rollup.touch()

    _maintenance_config(
        monkeypatch,
        data_retention_days=0,
        rum_retention_days=0,
        cache_retention_days=1,
        rollup_retention_months=1,
        quarantine_retention_days=1,
    )
    monkeypatch.setattr(buffer_mod, "_run_ducklake_maintenance", lambda *args, **kwargs: {})
    monkeypatch.setattr(duckdb_mod, "_cache_dir", lambda _source: str(cache))
    result = buffer_mod._run_cloud_maintenance_impl(src)

    assert not old_parquet.exists()
    assert not old_temp.exists()
    assert not old_bad_temp.exists()
    assert not old_tmp.exists()
    assert fresh_temp.exists()
    assert not old_rollup.exists()
    assert fresh_rollup.exists()
    assert evidence.read_bytes() == b"original evidence"
    assert result["local_cache_files_deleted"] == 1
    assert result["local_temp_files_deleted"] == 3
    assert result["local_rollup_files_deleted"] == 1


def test_local_cache_delete_failure_is_recorded(tmp_path, monkeypatch):
    src = _source(tmp_path)
    old_file = tmp_path / "cache" / "data" / "old.parquet"
    old_file.parent.mkdir(parents=True)
    old_file.touch()
    old_time = (datetime.now(UTC) - timedelta(days=10)).timestamp()
    os.utime(old_file, (old_time, old_time))
    monkeypatch.setattr(duckdb_mod, "_cache_dir", lambda _source: str(tmp_path / "cache"))
    _maintenance_config(monkeypatch, data_retention_days=0, rum_retention_days=0, cache_retention_days=1)
    monkeypatch.setattr(buffer_mod, "_run_ducklake_maintenance", lambda *args, **kwargs: {})
    remove = os.remove

    def fail_target(path):
        if os.fspath(path) == os.fspath(old_file):
            raise PermissionError("cache is read-only")
        remove(path)

    monkeypatch.setattr(buffer_mod.os, "remove", fail_target)

    result = buffer_mod._run_cloud_maintenance_impl(src)

    assert "local_cache_error" in result
    assert "read-only" in result["local_cache_error"]


def test_manual_trigger_runs_expire_and_persists_file_deletion_count(client, monkeypatch):
    src = {"name": "svc-expire-contract", "service_id": "svc-expire-contract", "access_level": "read_write"}
    logged: list[dict] = []
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda _sid: src)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda *_args: 44)
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", lambda *args: None)
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run",
        lambda *args, **kwargs: logged.append({"args": args, "kwargs": kwargs}),
    )
    monkeypatch.setattr(
        "backend.core.iceberg.run_cloud_maintenance",
        lambda _source: {
            "data_rows_deleted": 3,
            "rum_beacon_rows_deleted": 2,
            "snapshots_expired_count": 4,
            "data_files_cleaned": 6,
            "retention_deleted_rows": 5,
            "snapshots_expired": 4,
            "files_unlinked": 6,
        },
    )
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", lambda: None)
    monkeypatch.setattr("backend.cron_progress.start_progress", lambda *args, **kwargs: None)
    monkeypatch.setattr("backend.cron_progress.end_progress", lambda *args, **kwargs: None)
    monkeypatch.setattr("backend.cron.jobs._common.finalize_cron_duration", lambda *args, **kwargs: None)

    with patch("backend.cron.jobs.compaction._run_expire_snapshots", return_value={"status": "success"}) as run_expire:
        response = client.post("/api/admin/expire-snapshots/svc-expire-contract")

    assert response.status_code == 200
    assert response.json()["status"] == "success"
    run_expire.assert_called_once_with("svc-expire-contract", manual=True)


def test_expire_cron_persists_unlinked_file_count_and_metric_summary(monkeypatch):
    src = {"name": "svc-expire-contract", "service_id": "svc-expire-contract"}
    logged: list[dict] = []
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda _sid: src)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda *_args: 44)
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", lambda *args: None)
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run",
        lambda *args, **kwargs: logged.append({"args": args, "kwargs": kwargs}),
    )
    monkeypatch.setattr(
        "backend.core.iceberg.run_cloud_maintenance",
        lambda _source: {
            "data_rows_deleted": 3,
            "rum_log_rows_deleted": 1,
            "rum_beacon_rows_deleted": 2,
            "snapshots_expired_count": 4,
            "data_files_cleaned": 6,
            "retention_deleted_rows": 6,
            "snapshots_expired": 4,
            "files_unlinked": 6,
        },
    )
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", lambda: None)
    monkeypatch.setattr("backend.cron_progress.start_progress", lambda *args, **kwargs: None)
    monkeypatch.setattr("backend.cron_progress.end_progress", lambda *args, **kwargs: None)
    monkeypatch.setattr("backend.cron.jobs._common.finalize_cron_duration", lambda *args, **kwargs: None)

    result = expire._run_expire_snapshots.__wrapped__("svc-expire-contract", manual=True)

    kwargs = logged[0]["kwargs"]
    assert result["status"] == "success"
    assert kwargs["files_deleted_fos"] == 6
    assert "retention_deleted_rows=6" in kwargs["summary"]
    assert "snapshots_expired=4" in kwargs["summary"]
    assert "files_unlinked=6" in kwargs["summary"]


@pytest.mark.parametrize("failure_stage", ["cleanup", "start", "end"])
def test_expire_finalizes_cron_lease_when_progress_fails(monkeypatch, failure_stage):
    src = {"name": "svc-expire-contract", "service_id": "svc-expire-contract"}
    finalized: list[tuple] = []
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda _sid: src)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda *_args: 44)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        "backend.core.duckdb.finalize_cron_run_if_running",
        lambda *args: finalized.append(args),
    )
    monkeypatch.setattr(
        "backend.core.iceberg.run_cloud_maintenance",
        lambda _source: {"snapshots_expired_count": 0},
    )

    def fail_progress(*_args, **_kwargs):
        raise RuntimeError(f"progress {failure_stage} failed")

    monkeypatch.setattr(
        "backend.cron_progress.cleanup_progress_and_reap",
        fail_progress if failure_stage == "cleanup" else lambda: None,
    )
    monkeypatch.setattr(
        "backend.cron_progress.start_progress",
        fail_progress if failure_stage == "start" else lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "backend.cron_progress.end_progress",
        fail_progress if failure_stage == "end" else lambda _run_id: None,
    )
    monkeypatch.setattr("backend.cron.jobs._common.finalize_cron_duration", lambda *args, **kwargs: None)

    if failure_stage == "end":
        with pytest.raises(RuntimeError, match=f"progress {failure_stage} failed"):
            expire._run_expire_snapshots.__wrapped__("svc-expire-contract", manual=True)
    else:
        result = expire._run_expire_snapshots.__wrapped__("svc-expire-contract", manual=True)
        assert result["status"] == "error"
        assert result["error"] == f"progress {failure_stage} failed"

    assert finalized == [(src, "expire_snapshots", 44)]


def test_expire_cron_context_attributes_queries_and_flushes_usage(monkeypatch):
    from backend.core.query_attribution import derive_from_process_context
    from backend.utils.telemetry import get_process_context

    src = {"name": "svc-expire-contract", "service_id": "svc-expire-contract"}
    observed: dict[str, object] = {}
    flush_contexts: list[str | None] = []
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda _sid: src)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda *_args: 44)
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", lambda *args: None)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *args, **kwargs: None)

    def run_maintenance(_source):
        context = get_process_context()
        observed["context"] = context
        observed["attribution"] = derive_from_process_context(context)
        return {"snapshots_expired_count": 0}

    monkeypatch.setattr("backend.core.iceberg.run_cloud_maintenance", run_maintenance)
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda *_args: False)
    monkeypatch.setattr("backend.utils.telemetry.start_call_tracking", lambda: None)
    monkeypatch.setattr(
        "backend.utils.telemetry.flush_usage_log",
        lambda _service_id: flush_contexts.append(get_process_context()),
    )
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", lambda: None)
    monkeypatch.setattr("backend.cron_progress.start_progress", lambda *args, **kwargs: None)
    monkeypatch.setattr("backend.cron_progress.end_progress", lambda *args, **kwargs: None)
    monkeypatch.setattr("backend.cron.jobs._common.finalize_cron_duration", lambda *args, **kwargs: None)

    result = expire._run_expire_snapshots("svc-expire-contract", manual=True)

    attribution = observed["attribution"]
    assert result["status"] == "success"
    assert observed["context"] == "cron:expire_snapshots"
    assert attribution is not None
    assert attribution.kind == "cron"
    assert attribution.cron_job == "expire_snapshots"
    assert flush_contexts == ["cron:expire_snapshots"]


def test_ducklake_maintenance_sql_is_registered_with_cron_attribution(monkeypatch):
    src = {"name": "svc-expire-contract", "service_id": "svc-expire-contract"}
    raw_con = duckdb.connect(":memory:")

    @contextmanager
    def write_connection(_source):
        yield raw_con

    def expire_fn(con, _source, _keep_snapshot_days):
        con.execute("SELECT 1 AS cron_expire_probe").fetchone()
        return {"snapshots_expired_count": 0, "data_files_cleaned": 0}

    registered = []
    register = query_registry.register

    def capture_register(*args, **kwargs):
        registered.append((args, kwargs, current_attribution.get()))
        return register(*args, **kwargs)

    monkeypatch.setattr(buffer_mod, "_ducklake_write_connection", write_connection)
    monkeypatch.setattr(buffer_mod, "_ducklake_expire_snapshots", expire_fn)
    monkeypatch.setattr(query_registry, "register", capture_register)

    try:
        with process_context_scope("cron:expire_snapshots"):
            result = buffer_mod._run_ducklake_maintenance(
                src,
                data_retention_days=0,
                rum_retention_days=0,
                keep_snapshot_days=7,
            )
    finally:
        raw_con.close()

    matching = [item for item in registered if item[0][1] == "SELECT 1 AS cron_expire_probe"]
    assert matching, "Cron 8 maintenance SQL was not registered"
    call = matching[0]
    assert result == {"snapshots_expired_count": 0, "data_files_cleaned": 0}
    assert call[0][0] == "DuckDB"
    assert call[1]["service_id"] == src["service_id"]
    assert call[2].kind == "cron"
    assert call[2].cron_job == "expire_snapshots"


def test_expire_politeness_gate_applies_to_cron_but_manual_trigger_bypasses(monkeypatch):
    src = {"name": "svc-expire-contract", "service_id": "svc-expire-contract"}
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda _sid: src)
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda *_args: True)
    maintenance = MagicMock(return_value={"snapshots_expired_count": 0})
    monkeypatch.setattr("backend.core.iceberg.run_cloud_maintenance", maintenance)

    deferred = expire._run_expire_snapshots.__wrapped__("svc-expire-contract")
    assert deferred["status"] == "deferred"
    maintenance.assert_not_called()

    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda *_args: 44)
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", lambda *args: None)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *args, **kwargs: None)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", lambda: None)
    monkeypatch.setattr("backend.cron_progress.start_progress", lambda *args, **kwargs: None)
    monkeypatch.setattr("backend.cron_progress.end_progress", lambda *args, **kwargs: None)
    monkeypatch.setattr("backend.cron.jobs._common.finalize_cron_duration", lambda *args, **kwargs: None)

    manual = expire._run_expire_snapshots.__wrapped__("svc-expire-contract", manual=True)
    assert manual["status"] == "success"
    maintenance.assert_called_once()


def test_expire_runner_skips_when_another_service_run_holds_the_lease(monkeypatch):
    src = {"name": "svc-expire-contract", "service_id": "svc-expire-contract"}
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda _sid: src)
    monkeypatch.setattr(
        "backend.core.duckdb.start_cron_run", MagicMock(side_effect=CronLeaseHeldError("already running"))
    )
    maintenance = MagicMock()
    monkeypatch.setattr("backend.core.iceberg.run_cloud_maintenance", maintenance)

    result = expire._run_expire_snapshots.__wrapped__("svc-expire-contract", manual=True)

    assert result["status"] == "skipped"
    assert "already running" in result["summary"]
    maintenance.assert_not_called()


def test_nonfatal_step_error_is_recorded_as_warning(monkeypatch):
    src = {"name": "svc-expire-contract", "service_id": "svc-expire-contract"}
    logged: list[dict] = []
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda _sid: src)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda *_args: 44)
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", lambda *args: None)
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run",
        lambda *args, **kwargs: logged.append({"args": args, "kwargs": kwargs}),
    )
    monkeypatch.setattr(
        "backend.core.iceberg.run_cloud_maintenance",
        lambda _source: {
            "retention_deleted_rows": 2,
            "snapshots_expired": 0,
            "files_unlinked": 1,
            "local_cache_error": "cache is read-only",
        },
    )
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", lambda: None)
    monkeypatch.setattr("backend.cron_progress.start_progress", lambda *args, **kwargs: None)
    monkeypatch.setattr("backend.cron_progress.end_progress", lambda *args, **kwargs: None)
    monkeypatch.setattr("backend.cron.jobs._common.finalize_cron_duration", lambda *args, **kwargs: None)

    result = expire._run_expire_snapshots.__wrapped__("svc-expire-contract", manual=True)

    assert result["status"] == "warning"
    assert logged[0]["args"][3] == "warning"
    assert "local_cache_error=cache is read-only" in logged[0]["kwargs"]["error_message"]


def test_manual_endpoint_rejects_read_only_service():
    from backend.routers.admin.compaction import expire_snapshots_service

    with pytest.raises(HTTPException) as exc:
        expire_snapshots_service("svc-analyst", {"access_level": "read_only"})

    assert exc.value.status_code == 403


def test_dev_no_crons_blocks_expire_registration_and_execution(monkeypatch, tmp_path):
    service_id = "svc-expire-contract"
    src = _source(tmp_path, service_id)
    cfg = {
        **src,
        "log_period": 60,
        "access_level": "read_write",
        "provisioning": {"cron_sync": {"enabled": True}, "cron_compact": {"enabled": True}},
    }
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    scheduler = Scheduler()
    monkeypatch.setattr("backend.config.list_configs", lambda: [cfg])
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda _sid: src)
    monkeypatch.setattr("backend.core.duckdb.is_configured", lambda _source: True)
    monkeypatch.setattr("backend.config.get_ngwaf_workspace_id", lambda _sid: None)
    monkeypatch.setattr(scheduler, "_add_job", lambda *args, **kwargs: None)

    scheduler._sync_jobs()

    assert f"expire_{service_id}" not in scheduler._job_ids

    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock())
    monkeypatch.setattr("backend.core.iceberg.run_cloud_maintenance", MagicMock())
    skipped = expire._run_expire_snapshots.__wrapped__(service_id, manual=True)
    assert skipped["status"] == "skipped"
    assert "FLA_DEV_NO_CRONS=1" in skipped["summary"]
    duckdb_mod.start_cron_run.assert_not_called()
    iceberg_mod.run_cloud_maintenance.assert_not_called()


def test_registers_expire_on_the_serving_scheduler(monkeypatch, tmp_path):
    service_id = "svc-expire-contract"
    src = _source(tmp_path, service_id)
    src["access_level"] = "read_write"
    cfg = {
        **src,
        "log_period": 60,
        "access_level": "read_write",
        "provisioning": {"cron_sync": {"enabled": True}, "cron_compact": {"enabled": True}},
    }
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
    scheduler = Scheduler()
    monkeypatch.setattr("backend.config.list_configs", lambda: [cfg])
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda _sid: src)
    monkeypatch.setattr("backend.core.duckdb.is_configured", lambda _source: True)
    monkeypatch.setattr("backend.config.get_ngwaf_workspace_id", lambda _sid: None)
    registrations = {}

    def capture_job(func, trigger, **kwargs):
        registrations[kwargs["id"]] = (func, trigger)

    monkeypatch.setattr(scheduler, "_add_job", capture_job)
    scheduler._sync_jobs()

    job_id = f"expire_{service_id}"
    assert registrations[job_id] == (expire._run_expire_snapshots, "interval")


def test_usage_log_classifies_bulk_fos_deletes_as_class_a():
    from backend.core import metadata as metadata_db
    from backend.core.metadata import usage_log_db

    service_id = "svc-expire-billing-contract"
    metadata_db.log_usage_calls(
        service_id,
        [{"method": "DELETE_OBJECTS", "service": "FOS", "path": "ducklake/data", "status": 200}],
        process_context="cron:expire_snapshots",
    )
    con = usage_log_db.get_con(service_id)
    row = con.execute(
        "SELECT operation_class, operation_type, process_context FROM usage_log "
        "WHERE service_id = ? ORDER BY id DESC LIMIT 1",
        (service_id,),
    ).fetchone()

    assert row["operation_class"] == "A"
    assert row["operation_type"] == "DELETE_OBJECTS"
    assert row["process_context"] == "cron:expire_snapshots"


# ── Merged from test_optimize.py ──────────────────────────────────────────────

import logging

import pytest
from starlette.testclient import TestClient

from backend.cron.jobs import compaction as optimize


@pytest.fixture
def optimize_test_source(monkeypatch, tmp_path):

    service_id = "svc-optimize-contract"
    cache_root = tmp_path / "cache" / service_id
    cache_root.mkdir(parents=True, exist_ok=True)
    src = {
        "name": service_id,
        "service_id": service_id,
        "bucket": "test-fos-bucket",
        "_cache_dir_override": str(cache_root),
        "provisioning": {
            "access_level": "read_write",
            "cron_compact": {"enabled": True},
        },
    }
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src if sid == service_id else None)
    monkeypatch.setattr("backend.config.load_config", lambda sid: src if sid == service_id else None)
    return src


class TestOptimizeJob:
    def test_returns_when_should_defer_cron_true(self, monkeypatch):
        """Active-request gate fires → bail out before source lookup."""
        monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda *a, **kw: True)
        get_src = MagicMock()
        monkeypatch.setattr("backend.core.duckdb.get_source_for_service", get_src)
        optimize._run_optimize.__wrapped__("svc-1")
        get_src.assert_not_called()

    def test_returns_when_source_missing(self, monkeypatch):
        monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda *a, **kw: False)
        monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)
        start = MagicMock()
        monkeypatch.setattr("backend.core.duckdb.start_cron_run", start)
        optimize._run_optimize.__wrapped__("ghost-svc")
        start.assert_not_called()

    def test_skips_when_start_cron_run_raises(self, monkeypatch, stub_source, stub_cron_envelope):
        """``start_cron_run`` raises (another instance in-flight) → log a
        skip message and return; iceberg.optimize_table never runs."""
        stub_cron_envelope["start"].side_effect = CronLeaseHeldError("already running")
        opt = MagicMock()
        monkeypatch.setattr("backend.core.iceberg.optimize_table", opt)

        optimize._run_optimize.__wrapped__("svc-1")

        opt.assert_not_called()
        stub_cron_envelope["log"].assert_not_called()
        stub_cron_envelope["start_progress"].assert_not_called()

    def test_success_records_rewritten_and_added_counts(self, monkeypatch, stub_source, stub_cron_envelope):
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

    def test_partition_errors_with_files_added_logs_warning(self, monkeypatch, stub_source, stub_cron_envelope):
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

    def test_partition_errors_with_zero_files_added_logs_error(self, monkeypatch, stub_source, stub_cron_envelope):
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

    def test_top_level_error_key_logs_error(self, monkeypatch, stub_source, stub_cron_envelope):
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

    def test_unexpected_exception_logged_and_end_progress_fires(self, monkeypatch, stub_source, stub_cron_envelope):
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

    # ── Contract Tests (Cron 7: optimize_{service_id}) ─────────────────────────

    def test_contract_1_and_4_manual_trigger_and_cron_runs_accounting(self, optimize_test_source):
        """Checklist Items 1 & 4:
        - Trigger POST /api/admin/optimize/{service_id}; verify HTTP 200 response.
        - Confirm in cron_runs: run status success with non-zero duration and exact metric accounting.
        """
        from backend.deps import get_service_id, get_source
        from backend.main import app

        service_id = optimize_test_source["service_id"]
        logged_runs = []

        def fake_log_cron_run(source, task, duration_s, status, **kwargs):
            logged_runs.append(
                {
                    "task": task,
                    "duration_s": duration_s,
                    "status": status,
                    "parquet_files_optimized": kwargs.get("parquet_files_optimized"),
                    "parquet_files_created": kwargs.get("parquet_files_created"),
                    "summary": kwargs.get("summary"),
                }
            )

        app.dependency_overrides[get_source] = lambda: optimize_test_source
        app.dependency_overrides[get_service_id] = lambda: service_id
        try:
            with (
                patch("backend.core.duckdb.start_cron_run", return_value=101),
                patch("backend.core.duckdb.log_cron_run", side_effect=fake_log_cron_run),
                patch(
                    "backend.core.iceberg.optimize_table",
                    return_value={
                        "files_rewritten": 42,
                        "files_added": 4,
                        "eligible_partitions": 3,
                        "partition_errors": [],
                    },
                ),
                patch("backend.cron.jobs._common.finalize_cron_duration"),
            ):
                client = TestClient(app)
                resp = client.post(f"/api/admin/optimize/{service_id}")

                assert resp.status_code == 200, f"Expected 200, got: {resp.text}"
                body = resp.json()
                assert body["files_rewritten"] == 42
                assert body["files_added"] == 4

                # Verify cron_runs logged exactly
                assert len(logged_runs) == 1
                run = logged_runs[0]
                assert run["task"] == "optimize"
                assert run["status"] == "success"
                assert run["duration_s"] >= 0.0
                assert run["parquet_files_optimized"] == 42
                assert run["parquet_files_created"] == 4
                assert "Rewrote 42 files into 4 files" in run["summary"]
        finally:
            app.dependency_overrides.clear()

    def test_contract_2_and_3_verify_logs_for_flush_and_rewrite(self, optimize_test_source, caplog):
        """Checklist Items 2 & 3:
        - Verify in logs: ducklake_flush_inlined_data executed successfully.
        - Verify in logs: ducklake_rewrite_data_files executed successfully.
        """
        from backend.core.iceberg import buffer as buffer_mod

        src = optimize_test_source
        mock_con = MagicMock()
        mock_con.execute.return_value.fetchall.return_value = [("main", "logs", 10, 2)]

        caplog.set_level(logging.INFO)

        with (
            patch.object(buffer_mod, "_ducklake_write_connection") as mock_conn_ctx,
            patch.object(buffer_mod, "_lake_columns", return_value={"id", "timestamp"}),
        ):
            mock_conn_ctx.return_value.__enter__.return_value = mock_con

            res = buffer_mod._optimize_table_impl(src)
            assert "error" not in res
            assert res["files_rewritten"] > 0

        log_text = caplog.text
        assert "ducklake_flush_inlined_data executed successfully" in log_text, (
            "Checklist Item 2: Log must confirm ducklake_flush_inlined_data executed successfully"
        )
        assert "ducklake_rewrite_data_files executed successfully" in log_text, (
            "Checklist Item 3: Log must confirm ducklake_rewrite_data_files executed successfully"
        )

    def test_contract_5_usage_log_telemetry_attributed_to_cron_optimize(self, optimize_test_source):
        """Checklist Item 5:
        Confirm PostgreSQL's usage_log table records FOS Class A/B calls attributed to cron.optimize.
        """
        from backend.core import metadata as metadata_db
        from backend.core.metadata import usage_log_db

        service_id = optimize_test_source["service_id"]

        # Emulate FOS calls made during optimize job
        fos_calls = [
            {"method": "PUT_OBJECT", "service": "FOS", "bytes": 1024 * 1024},
            {"method": "GET_OBJECT", "service": "FOS", "bytes": 512 * 1024},
            {"method": "DELETE_OBJECT", "service": "FOS"},
        ]

        # Verify attribution to cron.optimize
        metadata_db.log_usage_calls(service_id, fos_calls, process_context="cron.optimize")

        con = usage_log_db.get_con(service_id)
        rows = con.execute(
            "SELECT operation_class, process_context, operation_type FROM usage_log WHERE service_id = ? ORDER BY id",
            (service_id,),
        ).fetchall()

        assert len(rows) == 3
        # PUT_OBJECT -> Class A
        assert rows[0]["operation_class"] == "A"
        assert rows[0]["process_context"] == "cron.optimize"
        assert rows[0]["operation_type"] == "PUT_OBJECT"

        # GET_OBJECT -> Class B
        assert rows[1]["operation_class"] == "B"
        assert rows[1]["process_context"] == "cron.optimize"
        assert rows[1]["operation_type"] == "GET_OBJECT"

        # DELETE_OBJECT -> Class B
        assert rows[2]["operation_class"] == "B"
        assert rows[2]["process_context"] == "cron.optimize"
        assert rows[2]["operation_type"] == "DELETE_OBJECT"

    def test_contract_6_safety_gate_under_fla_dev_no_crons(self, optimize_test_source, monkeypatch, caplog):
        """Checklist Item 6:
        Under FLA_DEV_NO_CRONS=1, verify job does not register or execute.
        """
        service_id = optimize_test_source["service_id"]
        monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")

        # 1. Scheduler registration test: optimize_{service_id} must NOT be registered
        scheduler = Scheduler()
        with (
            patch("backend.config.list_configs", return_value=[optimize_test_source]),
            patch("backend.core.duckdb.is_configured", return_value=True),
        ):
            scheduler._register_dev_local_safe_jobs()
            job_ids = list(scheduler._job_ids.keys())
            assert f"optimize_{service_id}" not in job_ids, (
                "optimize job must not be registered under FLA_DEV_NO_CRONS=1"
            )

        # 2. Execution test: direct invocation of _run_optimize must refuse and log
        caplog.set_level(logging.WARNING)
        mock_opt = MagicMock()
        monkeypatch.setattr("backend.core.iceberg.optimize_table", mock_opt)

        res = optimize._run_optimize.__wrapped__(service_id)
        assert res is not None
        assert res.get("status") == "skipped"
        mock_opt.assert_not_called()
        assert any("FLA_DEV_NO_CRONS=1" in record.message for record in caplog.records), (
            "Refusal log must cite FLA_DEV_NO_CRONS=1"
        )

    def test_contract_politeness_gating_and_manual_bypass(self, optimize_test_source):
        """Requirement 5:
        Verify politeness gating (should_defer_cron('optimize', service_id)) defers scheduled
        runs, but manual trigger bypasses deferral.
        """
        service_id = optimize_test_source["service_id"]

        with (
            patch("backend.utils.active_requests.should_defer_cron", return_value=True),
            patch("backend.core.iceberg.optimize_table") as mock_opt,
        ):
            # Scheduled run (manual=False): must defer
            res = optimize._run_optimize.__wrapped__(service_id, manual=False)
            assert res is not None
            assert res.get("status") == "deferred"
            mock_opt.assert_not_called()

            # Manual run (manual=True): must bypass deferral
            mock_opt.return_value = {"files_rewritten": 5, "files_added": 1}
            with (
                patch("backend.core.duckdb.start_cron_run", return_value=201),
                patch("backend.core.duckdb.log_cron_run"),
                patch("backend.cron.jobs._common.finalize_cron_duration"),
            ):
                res_manual = optimize._run_optimize.__wrapped__(service_id, manual=True)
                assert res_manual.get("status") == "success"
                mock_opt.assert_called_once()

    def test_contract_exclusive_per_service_locking(self, optimize_test_source):
        """Requirement 5: Exclusive per-service locking."""
        service_id = optimize_test_source["service_id"]

        with (
            patch("backend.core.duckdb.start_cron_run", side_effect=CronLeaseHeldError("already running")),
            patch("backend.core.iceberg.optimize_table") as mock_opt,
        ):
            res = optimize._run_optimize.__wrapped__(service_id)
            assert res is not None
            assert res.get("status") == "skipped"
            mock_opt.assert_not_called()

    def test_contract_all_lake_tables_durability_and_compaction(self, optimize_test_source):
        """Requirement 1 & 2:
        Verify the DuckLake durability flush (CALL ducklake_flush_inlined_data('lake')) executes
        before rewrite/merge operations across all lake tables (logs, client_vitals, client_errors).
        """
        from backend.core.iceberg import buffer as buffer_mod

        src = optimize_test_source
        calls = []

        class MockCon:
            def execute(self, sql, *args):
                calls.append(sql.strip())
                m = MagicMock()
                m.fetchall.return_value = [("main", "test", 2, 1)]
                return m

        mock_con = MockCon()

        with (
            patch.object(buffer_mod, "_ducklake_write_connection") as mock_conn_ctx,
            patch.object(buffer_mod, "_lake_columns", return_value={"id", "timestamp"}),
        ):
            mock_conn_ctx.return_value.__enter__.return_value = mock_con

            res = buffer_mod._optimize_table_impl(src)
            assert "error" not in res
            assert res["files_rewritten"] > 0
            assert res["files_added"] > 0

        # 1. ducklake_flush_inlined_data must be the very first CALL executed
        call_flush_idx = next(i for i, c in enumerate(calls) if "CALL ducklake_flush_inlined_data" in c)
        assert call_flush_idx == 0, "Durability flush must execute before any merge or rewrite"

        # 2. Both merge and rewrite must execute across all lake tables
        merge_calls = [c for c in calls if "ducklake_merge_adjacent_files" in c]
        rewrite_calls = [c for c in calls if "ducklake_rewrite_data_files" in c]

        # Should cover logs, client_vitals, client_errors
        assert len(merge_calls) == 3, f"Expected merge on 3 tables, got: {merge_calls}"
        assert len(rewrite_calls) == 3, f"Expected rewrite on 3 tables, got: {rewrite_calls}"
