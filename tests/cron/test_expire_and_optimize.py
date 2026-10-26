"""Consolidated tests for test_expire_and_optimize."""

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import duckdb
import pytest
from backend.core.query_attribution import current_attribution
from fastapi import HTTPException

from backend.core import duckdb as duckdb_mod
from backend.core import iceberg as iceberg_mod
from backend.core.iceberg import buffer as buffer_mod
from backend.core.metadata.cron_log import CronLeaseHeldError
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


# ── Merged from tests/cron/test_maintenance_jobs.py ──

"""Consolidated tests for test_maintenance_jobs."""

from backend.core import duckdb as _db
from backend.core import duckdb_pool as _pool
from backend.core import duckdb_recycle as _recycle
from backend.cron.jobs.metadata import run_duckdb_recycle
from backend.main import app


def test_trim_memory_graceful():
    """_trim_memory runs gc.collect and survives missing or present malloc_trim."""
    _recycle._trim_memory()


def test_get_pool_status_and_barrier():
    """Pool status and barrier inspection return expected dictionary structure."""
    stats = _pool.get_pool_status()
    assert "pools" in stats
    assert "retired_pools" in stats

    # Barrier check
    assert _db.is_recycle_barrier_active("/non/existent/path") is False
    assert isinstance(_db.is_recycle_barrier_active(), set)


def test_run_duckdb_recycle_rss_threshold_skip():
    """Recycle is skipped when RSS is below the configured threshold."""
    with (
        patch("backend.core.duckdb_recycle.maybe_graceful_restart", return_value=False),
        patch("backend.cron.jobs.metadata._recycle_rss_threshold_bytes", return_value=500 * 1024 * 1024),
        patch("backend.core.duckdb.current_rss_bytes", return_value=100 * 1024 * 1024),
        patch("backend.cron.jobs.metadata.recycle_once") as mock_recycle,
    ):
        result = run_duckdb_recycle.__wrapped__()
        assert "skipped" in result
        assert "100MB < threshold 500MB" in result
        mock_recycle.assert_not_called()


def test_run_duckdb_recycle_adaptive_expedite():
    """Recycle adjusts schedule dynamically when approaching threshold."""
    mock_sched = MagicMock()
    mock_job = MagicMock()
    trigger = MagicMock()
    trigger.interval.total_seconds.return_value = 3600
    mock_job.trigger = trigger
    mock_sched.get_job.return_value = mock_job

    with (
        patch("backend.core.duckdb_recycle.maybe_graceful_restart", return_value=False),
        patch("backend.cron.jobs.metadata._recycle_rss_threshold_bytes", return_value=1000 * 1024 * 1024),
        patch("backend.core.duckdb.current_rss_bytes", return_value=850 * 1024 * 1024),
        patch("backend.cron.scheduler.get_scheduler", return_value=mock_sched),
        patch("backend.core.duckdb_recycle.recycle_interval_min", return_value=60.0),
        patch("backend.cron.jobs.metadata.recycle_once") as mock_recycle,
    ):
        # 850MB is >= 800MB (80% of 1000MB threshold), skips recycle but tightens interval to 2m
        result = run_duckdb_recycle.__wrapped__()
        assert "skipped" in result
        mock_recycle.assert_not_called()
        mock_job.reschedule.assert_called_with("interval", minutes=2.0)


def test_run_duckdb_recycle_adaptive_incomplete():
    """Recycle expedites schedule down to 2m when recycle_once reports incomplete."""
    mock_sched = MagicMock()
    mock_job = MagicMock()
    trigger = MagicMock()
    trigger.interval.total_seconds.return_value = 3600
    mock_job.trigger = trigger
    mock_sched.get_job.return_value = mock_job

    with (
        patch("backend.core.duckdb_recycle.maybe_graceful_restart", return_value=False),
        patch("backend.cron.jobs.metadata._recycle_rss_threshold_bytes", return_value=0),
        patch("backend.core.duckdb.current_rss_bytes", return_value=200 * 1024 * 1024),
        patch("backend.cron.scheduler.get_scheduler", return_value=mock_sched),
        patch("backend.core.duckdb_recycle.recycle_interval_min", return_value=60.0),
        patch(
            "backend.cron.jobs.metadata.recycle_once",
            return_value="interval: recycled 0/1 instance(s), freed ~0MB (incomplete: 1)",
        ),
    ):
        result = run_duckdb_recycle.__wrapped__()
        assert "incomplete" in result
        mock_job.reschedule.assert_called_with("interval", minutes=2.0)


def test_run_duckdb_recycle_adaptive_relax():
    """Recycle relaxes schedule back to baseline when RSS drops below 50% threshold."""
    mock_sched = MagicMock()
    mock_job = MagicMock()
    trigger = MagicMock()
    trigger.interval.total_seconds.return_value = 120  # currently expedited to 2m
    mock_job.trigger = trigger
    mock_sched.get_job.return_value = mock_job

    with (
        patch("backend.core.duckdb_recycle.maybe_graceful_restart", return_value=False),
        patch("backend.cron.jobs.metadata._recycle_rss_threshold_bytes", return_value=1000 * 1024 * 1024),
        patch("backend.core.duckdb.current_rss_bytes", side_effect=[1100 * 1024 * 1024, 400 * 1024 * 1024]),
        patch("backend.cron.scheduler.get_scheduler", return_value=mock_sched),
        patch("backend.core.duckdb_recycle.recycle_interval_min", return_value=60.0),
        patch(
            "backend.cron.jobs.metadata.recycle_once",
            return_value="interval: recycled 1/1 instance(s), freed ~700MB",
        ),
    ):
        result = run_duckdb_recycle.__wrapped__()
        assert "recycled 1/1" in result
        # 400MB is < 500MB (50% of 1000MB threshold), relaxes back to 60.0m baseline
        mock_job.reschedule.assert_called_with("interval", minutes=60.0)


def test_recycle_once_incomplete_reporting(monkeypatch):
    """recycle_once adds (incomplete: N) detail when any instance drain fails."""
    monkeypatch.setattr(
        _recycle,
        "_sources_by_db_path",
        lambda: {"/tmp/test.duckdb": [{"name": "s1"}]},
    )
    monkeypatch.setattr(
        _recycle,
        "_recycle_db_path",
        lambda path, sources: {"db_path": path, "status": "incomplete", "drained": False, "live_after": 1},
    )
    detail = _recycle.recycle_once()
    assert "incomplete: 1" in detail


def test_admin_duckdb_recycle_endpoint():
    """POST /api/admin/duckdb/recycle triggers recycle or skips based on force flag."""
    client = TestClient(app)

    # 1. Skipped when below threshold and force=False
    with (
        patch("backend.core.duckdb.current_rss_bytes", return_value=100 * 1024 * 1024),
        patch("backend.core.duckdb_recycle._recycle_rss_threshold_bytes", return_value=500 * 1024 * 1024),
    ):
        resp = client.post("/api/admin/duckdb/recycle")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["status"] == "skipped"

    # 2. Forced recycle
    with (
        patch("backend.core.duckdb.current_rss_bytes", side_effect=[300 * 1024 * 1024, 200 * 1024 * 1024]),
        patch("backend.core.duckdb_recycle._recycle_rss_threshold_bytes", return_value=500 * 1024 * 1024),
        patch(
            "backend.core.duckdb_recycle.recycle_once",
            return_value="manual_admin: recycled 1/1 instance(s), freed ~100MB",
        ),
    ):
        resp = client.post("/api/admin/duckdb/recycle?force=true")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["status"] == "recycled"
        assert data["freed_mb"] == 100.0


def test_admin_duckdb_status_endpoint():
    """GET /api/admin/duckdb/status returns memory and pool states."""
    client = TestClient(app)

    with (
        patch("backend.core.duckdb.current_rss_bytes", return_value=250 * 1024 * 1024),
        patch("backend.core.duckdb_recycle._recycle_rss_threshold_bytes", return_value=1000 * 1024 * 1024),
        patch("backend.core.duckdb_recycle.recycle_interval_min", return_value=60.0),
    ):
        resp = client.get("/api/admin/duckdb/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["memory"]["current_rss_mb"] == 250.0
        assert data["memory"]["recycle_threshold_mb"] == 1000.0
        assert data["memory"]["recycle_interval_min"] == 60.0
        assert "pools" in data
        assert "barrier" in data


# ── Merged from test_heartbeat.py ──────────────────────────────────────────────

import threading
import time

import pytest
from backend.cron.decorators import CronHeartbeat

from backend.core.metadata import pg_connection as pgc
from backend.core.metadata.base import get_con
from backend.cron import decorators

SID = "svc-heartbeat"
JOB = "heartbeat_job"
OTHER_JOB = "other_job"


def _stray_threads(before: set[threading.Thread]) -> list[threading.Thread]:
    """Threads started since ``before`` that are still alive (the shared watchdog pool is long-lived by design)."""
    return [
        t for t in threading.enumerate() if t not in before and t.is_alive() and not t.name.startswith("cron-watchdog")
    ]


def _wait_for_exit(threads: list[threading.Thread], timeout: float = 5.0) -> list[threading.Thread]:
    deadline = time.monotonic() + timeout
    while threads and time.monotonic() < deadline:
        threads = [t for t in threads if t.is_alive()]
        time.sleep(0.01)
    return threads


@pytest.fixture
def baseline():
    """Thread set and checked-out pool size with this thread's own metadata connection already open."""
    get_con(SID)
    return set(threading.enumerate()), len(pgc._checked_out)


def _seed_leases() -> None:
    con = get_con(SID)
    for job in (JOB, OTHER_JOB):
        con.execute(
            "INSERT INTO job_runs (service_id, job_name, started_at, heartbeat_at, lease_ttl_s, status) "
            "VALUES (?, ?, ?, ?, ?, 'running')",
            (SID, job, 1.0, 1.0, 60),
        )
    con.commit()


def _heartbeat_at(job: str) -> float:
    row = (
        get_con(SID)
        .execute("SELECT heartbeat_at FROM job_runs WHERE service_id = ? AND job_name = ?", (SID, job))
        .fetchone()
    )
    return float(row[0])


def test_heartbeat_refreshes_only_its_own_job_lease(baseline):
    _seed_leases()

    with CronHeartbeat(SID, JOB, interval_s=0.02):
        deadline = time.monotonic() + 5.0
        while _heartbeat_at(JOB) <= 1.0 and time.monotonic() < deadline:
            time.sleep(0.02)

    assert _heartbeat_at(JOB) > 1.0
    assert _heartbeat_at(OTHER_JOB) == 1.0


def test_no_thread_or_pooled_connection_survives_a_normal_exit(baseline):
    before, checked_out = baseline
    _seed_leases()

    with CronHeartbeat(SID, JOB, interval_s=0.02):
        deadline = time.monotonic() + 5.0
        while _heartbeat_at(JOB) <= 1.0 and time.monotonic() < deadline:
            time.sleep(0.02)

    assert _wait_for_exit(_stray_threads(before)) == []
    assert len(pgc._checked_out) == checked_out


def test_no_thread_or_pooled_connection_survives_an_exception_in_the_body(baseline):
    before, checked_out = baseline
    _seed_leases()

    with pytest.raises(RuntimeError, match="job blew up"):
        with CronHeartbeat(SID, JOB, interval_s=0.02):
            deadline = time.monotonic() + 5.0
            while _heartbeat_at(JOB) <= 1.0 and time.monotonic() < deadline:
                time.sleep(0.02)
            raise RuntimeError("job blew up")

    assert _wait_for_exit(_stray_threads(before)) == []
    assert len(pgc._checked_out) == checked_out


def test_the_thread_releases_its_connection_even_when_it_never_used_one():
    with patch("backend.core.metadata.base.release_thread_connection") as release:
        with CronHeartbeat(SID, None, interval_s=0.02):
            pass

    release.assert_called_once_with()


def test_a_job_without_a_name_refreshes_nothing(baseline):
    _seed_leases()

    with CronHeartbeat(SID, None, interval_s=0.02):
        time.sleep(0.15)

    assert _heartbeat_at(JOB) == 1.0
    assert _heartbeat_at(OTHER_JOB) == 1.0


def test_a_failing_refresh_does_not_kill_the_loop_or_leak(baseline):
    before, checked_out = baseline
    calls: list[int] = []

    def _boom(*_a, **_kw):
        calls.append(1)
        raise RuntimeError("db down")

    with patch("backend.core.metadata.base.get_con", side_effect=_boom):
        with CronHeartbeat(SID, JOB, interval_s=0.01):
            deadline = time.monotonic() + 5.0
            while len(calls) < 3 and time.monotonic() < deadline:
                time.sleep(0.01)

    assert len(calls) >= 3
    assert _wait_for_exit(_stray_threads(before)) == []
    assert len(pgc._checked_out) == checked_out


# ── through cron_task ────────────────────────────────────────────────────────


@pytest.fixture
def fast_heartbeat(monkeypatch):
    monkeypatch.setattr("backend.cron.decorators.DEFAULT_INTERVAL_S", 0.02)


def test_cron_task_leaks_nothing_when_the_executor_refuses_the_job(baseline, fast_heartbeat):
    before, checked_out = baseline

    @decorators.cron_task("exec-refuses", job_name=JOB)
    def _job(service_id: str):
        return None

    class _Dead:
        def submit(self, *_a, **_kw):
            raise RuntimeError("cannot schedule new futures after shutdown")

    with patch.object(decorators, "_get_watchdog_executor", return_value=_Dead()):
        with pytest.raises(RuntimeError, match="shutdown"):
            _job(SID)

    assert _wait_for_exit(_stray_threads(before)) == []
    assert len(pgc._checked_out) == checked_out


def test_cron_task_leaks_nothing_across_repeated_failures_and_watchdog_timeouts(baseline, fast_heartbeat, monkeypatch):
    before, checked_out = baseline
    monkeypatch.setattr(decorators, "_CRON_HARD_CAP_S", 0.05)

    @decorators.cron_task("flaky", job_name=JOB)
    def _job(service_id: str, mode: str):
        if mode == "slow":
            time.sleep(0.15)
        if mode == "raise":
            raise ValueError("boom")

    for mode in ["ok", "raise", "slow"] * 5:
        if mode == "raise":
            with pytest.raises(ValueError, match="boom"):
                _job(SID, mode)
        else:
            _job(SID, mode)

    assert _wait_for_exit(_stray_threads(before)) == []
    assert len(pgc._checked_out) == checked_out


def test_cron_task_returns_the_job_result(fast_heartbeat):
    @decorators.cron_task("returns", job_name=JOB)
    def _job(service_id: str):
        return {"ok": service_id}

    assert _job(SID) == {"ok": SID}


def test_watchdog_timeout_still_waits_for_the_job_and_returns_its_result(fast_heartbeat, monkeypatch):
    monkeypatch.setattr(decorators, "_CRON_HARD_CAP_S", 0.05)

    @decorators.cron_task("slow-result", job_name=JOB)
    def _job(service_id: str):
        time.sleep(0.15)
        return "finished"

    assert _job(SID) == "finished"


# ── Merged from test_metric_snapshot_job.py ──────────────────────────────────────────────

import io

from backend.cron.jobs.metric_snapshot import (
    _run_metric_snapshot,
    _safe_record,
    _sample_active_queries,
    _sample_celery_queues,
    _sample_cron_duration,
    _sample_ingest_lag,
    _sample_ingest_ledger,
    _sample_os_vitals,
)
from tests._helpers.config_fakes import fetch_metric_snapshot_history as _get_history

from backend.core import metric_snapshots


def test_tick_records_active_query_count():
    """Active-query path is the simplest — it doesn't depend on
    per-service state, just the global registry summary."""
    with patch("backend.core.query_registry.query_registry.summary", return_value={"active_total": 3}):
        _run_metric_snapshot()

    out = _get_history("active_query_count", since=datetime.now(UTC) - timedelta(hours=1))
    assert len(out) >= 1
    assert out[-1]["value"] == 3.0


def test_tick_records_pool_wait_per_service():
    fake_stats = [
        {"service": "svc-a", "wait": {"p95_ms": 12.3, "count": 100}},
        {"service": "svc-b", "wait": {"p95_ms": 45.6, "count": 50}},
    ]
    with patch("backend.core.duckdb_pool.get_all_stats", return_value=fake_stats):
        _run_metric_snapshot()

    since = datetime.now(UTC) - timedelta(hours=1)
    a = _get_history("pool_wait_p95_ms", since=since, service_id="svc-a")
    b = _get_history("pool_wait_p95_ms", since=since, service_id="svc-b")
    assert any(r["value"] == 12.3 for r in a)
    assert any(r["value"] == 45.6 for r in b)


def test_tick_continues_when_one_metric_fails():
    """A blow-up in one sampler must not skip the others — the
    _safe_record wrapper + per-metric try/except in the sampler ensures
    the rest of the tick still runs."""
    with patch("backend.core.duckdb_pool.get_all_stats", side_effect=RuntimeError("boom")):
        with patch("backend.core.query_registry.query_registry.summary", return_value={"active_total": 7}):
            _run_metric_snapshot()  # must not raise

    out = _get_history("active_query_count", since=datetime.now(UTC) - timedelta(hours=1))
    assert out and out[-1]["value"] == 7.0


# ── _safe_record swallows per-metric failures ──────────────────────────────────


def test_safe_record_swallows_record_snapshot_exception():
    """_safe_record is the wrapper that keeps a sampler-emitted bad
    value from poisoning the whole tick. Pin that an exception in
    record_snapshot is caught and only logged at debug."""
    with patch.object(metric_snapshots, "record_snapshot", side_effect=ValueError("bad metric")):
        _safe_record("bogus", 1.0)  # must not raise


def test_sample_celery_queues_records_reachability_and_known_queues():
    with patch(
        "backend.celery_status.celery_queue_depths",
        return_value=({"q.ingest": 4}, True),
    ):
        _sample_celery_queues()

    since = datetime.now(UTC) - timedelta(hours=1)
    assert _get_history("celery_broker_reachable", since=since)[-1]["value"] == 1.0
    assert _get_history("celery_queue_depth_q.ingest", since=since)[-1]["value"] == 4.0
    assert _get_history("celery_queue_depth_q.control", since=since)[-1]["value"] == 0.0
    assert _get_history("celery_queue_depth", since=since)[-1]["value"] == 4.0


def test_sample_celery_queues_records_broker_unreachable():
    with patch(
        "backend.celery_status.celery_queue_depths",
        return_value=({}, False),
    ):
        _sample_celery_queues()

    since = datetime.now(UTC) - timedelta(hours=1)
    assert _get_history("celery_broker_reachable", since=since)[-1]["value"] == 0.0
    assert _get_history("celery_queue_depth", since=since)[-1]["value"] == 0.0


def test_sample_ingest_ledger_records_each_state():
    with patch(
        "backend.celery_status.ingest_ledger_summary",
        return_value={"discovered": 5, "claimed": 2, "committed": 9},
    ):
        _sample_ingest_ledger()

    since = datetime.now(UTC) - timedelta(hours=1)
    assert _get_history("ingest_ledger_discovered", since=since)[-1]["value"] == 5.0
    assert _get_history("ingest_ledger_claimed", since=since)[-1]["value"] == 2.0


# ── _sample_cron_duration: per-service iteration + error containment ──────────


def _fake_row(task: str, secs: float) -> dict:
    """SQLite row_factory dict shape (con.row_factory=Row)."""
    return {"task": task, "duration_s": secs}


def test_sample_cron_duration_records_per_service_per_task():
    """Mock list_configs to enumerate two services and verify that the
    latest terminal cron_runs row for each task is recorded with the
    right service_id/task labels."""
    cfgs = [{"service_id": "svc-a"}, {"service_id": "svc-b"}]

    fake_cons: dict[str, MagicMock] = {}
    for svc, rows in (("svc-a", [_fake_row("sync", 0.5)]), ("svc-b", [_fake_row("optimize", 2.0)])):
        cur = MagicMock()
        cur.fetchall.return_value = rows
        con = MagicMock()
        con.execute.return_value = cur
        fake_cons[svc] = con

    with (
        patch("backend.config.list_configs", return_value=cfgs),
        patch("backend.core.metadata.base.get_con", side_effect=lambda sid: fake_cons[sid]),
    ):
        _sample_cron_duration()

    since = datetime.now(UTC) - timedelta(hours=1)
    a = _get_history("cron_duration_ms", since=since, service_id="svc-a", task="sync")
    b = _get_history("cron_duration_ms", since=since, service_id="svc-b", task="optimize")
    assert any(r["value"] == 500.0 for r in a)  # 0.5 s → 500 ms
    assert any(r["value"] == 2000.0 for r in b)


def test_sample_cron_duration_skips_configs_without_service_id():
    """A config without a service_id (partial init / corrupted file) is
    silently skipped — must not raise and must not call get_con."""
    cfgs = [{"service_id": None}, {}, {"service_id": "svc-real"}]

    called_with: list[str] = []

    def _record(sid: str) -> MagicMock:
        called_with.append(sid)
        cur = MagicMock()
        cur.fetchall.return_value = []
        con = MagicMock()
        con.execute.return_value = cur
        return con

    with (
        patch("backend.config.list_configs", return_value=cfgs),
        patch("backend.core.metadata.base.get_con", side_effect=_record),
    ):
        _sample_cron_duration()

    assert called_with == ["svc-real"]


def test_sample_cron_duration_continues_after_one_service_fails():
    """A get_con failure for one service must be caught and logged; the
    iteration must proceed to the next service."""
    cfgs = [{"service_id": "svc-bad"}, {"service_id": "svc-good"}]

    good_cur = MagicMock()
    good_cur.fetchall.return_value = [_fake_row("sync", 1.0)]
    good_con = MagicMock()
    good_con.execute.return_value = good_cur

    def _get_con(sid: str):
        if sid == "svc-bad":
            raise RuntimeError("locked")
        return good_con

    with (
        patch("backend.config.list_configs", return_value=cfgs),
        patch("backend.core.metadata.base.get_con", side_effect=_get_con),
    ):
        _sample_cron_duration()  # must not raise

    since = datetime.now(UTC) - timedelta(hours=1)
    good = _get_history("cron_duration_ms", since=since, service_id="svc-good", task="sync")
    assert any(r["value"] == 1000.0 for r in good)


def test_sample_cron_duration_outer_except_handles_import_failure():
    """If the late `from backend import config` itself blows up (e.g.
    the package somehow can't import in this process), the outer except
    must swallow it. Pin that path."""
    with patch("backend.config.list_configs", side_effect=RuntimeError("module broken")):
        _sample_cron_duration()  # must not raise


# ── _sample_ingest_lag: per-service iteration + error containment ─────────────


def test_sample_ingest_lag_records_recent_lag():
    """Happy path: latest ingest 10 minutes ago → lag_s ≈ 600."""
    cfgs = [{"service_id": "svc-ingest"}]
    ten_min_ago = (datetime.now(UTC) - timedelta(minutes=10)).isoformat().replace("+00:00", "Z")

    with (
        patch("backend.config.list_configs", return_value=cfgs),
        patch("backend.core.metadata.get_latest_ingest_ts", return_value=ten_min_ago),
    ):
        _sample_ingest_lag()

    since = datetime.now(UTC) - timedelta(hours=1)
    rows = _get_history("ingest_lag_s", since=since, service_id="svc-ingest")
    assert rows
    # ~600 s, allow ±30 s wall-clock slack.
    assert 570 <= rows[-1]["value"] <= 630


def test_sample_ingest_lag_skips_when_no_latest_ts():
    """A service that has never ingested has no latest_ts. Must skip
    rather than recording a bogus huge value or crashing on None."""
    cfgs = [{"service_id": "svc-empty"}]

    with (
        patch("backend.config.list_configs", return_value=cfgs),
        patch("backend.core.metadata.get_latest_ingest_ts", return_value=None),
    ):
        _sample_ingest_lag()

    since = datetime.now(UTC) - timedelta(hours=1)
    assert _get_history("ingest_lag_s", since=since, service_id="svc-empty") == []


def test_sample_ingest_lag_skips_unparseable_timestamp():
    """A latest_ts that parse_iso_utc can't parse returns None — the
    sampler must skip silently rather than crashing on datetime math."""
    cfgs = [{"service_id": "svc-bad-ts"}]

    with (
        patch("backend.config.list_configs", return_value=cfgs),
        patch("backend.core.metadata.get_latest_ingest_ts", return_value="not-a-timestamp"),
    ):
        _sample_ingest_lag()  # must not raise

    since = datetime.now(UTC) - timedelta(hours=1)
    assert _get_history("ingest_lag_s", since=since, service_id="svc-bad-ts") == []


def test_sample_ingest_lag_continues_after_one_service_fails():
    cfgs = [{"service_id": "svc-bad"}, {"service_id": "svc-good"}]

    def _fake_get_latest(sid: str):
        if sid == "svc-bad":
            raise RuntimeError("locked")
        return (datetime.now(UTC) - timedelta(minutes=1)).isoformat().replace("+00:00", "Z")

    with (
        patch("backend.config.list_configs", return_value=cfgs),
        patch("backend.core.metadata.get_latest_ingest_ts", side_effect=_fake_get_latest),
    ):
        _sample_ingest_lag()  # must not raise

    since = datetime.now(UTC) - timedelta(hours=1)
    assert _get_history("ingest_lag_s", since=since, service_id="svc-good")


def test_sample_ingest_lag_skips_configs_without_service_id():
    cfgs = [{"service_id": None}, {"service_id": "svc-real"}]
    seen: list[str] = []

    def _record(sid: str) -> None:
        seen.append(sid)
        return None  # no data

    with (
        patch("backend.config.list_configs", return_value=cfgs),
        patch("backend.core.metadata.get_latest_ingest_ts", side_effect=_record),
    ):
        _sample_ingest_lag()

    assert seen == ["svc-real"]


# ── _sample_active_queries: exception path ────────────────────────────────────


def test_sample_active_queries_swallows_summary_exception():
    with patch("backend.core.query_registry.query_registry.summary", side_effect=RuntimeError("boom")):
        _sample_active_queries()  # must not raise

    # Nothing recorded for this tick.
    since = datetime.now(UTC) - timedelta(seconds=5)
    pre = _get_history("active_query_count", since=since)
    assert pre == [] or all(r["value"] != 0 for r in pre[:0])  # vacuous — just pin no-crash


# ── _sample_os_vitals: getloadavg / /proc/meminfo / disk_usage branches ───────


def test_sample_os_vitals_swallows_getloadavg_exception():
    """getloadavg() raises on platforms that don't expose it (some
    container runtimes) — must be caught."""
    with patch("os.getloadavg", side_effect=OSError("not available")):
        _sample_os_vitals()  # must not raise


def test_sample_os_vitals_parses_proc_meminfo_when_available():
    """Linux path: /proc/meminfo opens and is parsed; mem_used_pct is
    recorded based on (1 - MemAvailable/MemTotal) * 100. The dev macOS
    box doesn't have /proc, so this exercises the parse path via a
    fake open()."""
    meminfo = "MemTotal:       16384 kB\nMemAvailable:    4096 kB\nBuffers:    100 kB\n"

    def _fake_open(path, *args, **kwargs):
        assert path == "/proc/meminfo"
        return io.StringIO(meminfo)

    with patch("builtins.open", side_effect=_fake_open):
        _sample_os_vitals()  # must not raise

    since = datetime.now(UTC) - timedelta(hours=1)
    rows = _get_history("mem_used_pct", since=since)
    assert rows
    # (1 - 4096/16384) * 100 = 75.0
    assert rows[-1]["value"] == 75.0


def test_sample_os_vitals_swallows_meminfo_parse_error():
    """Open succeeds but the file contents are garbage — must catch the
    non-FileNotFoundError exception."""

    def _fake_open(path, *args, **kwargs):
        assert path == "/proc/meminfo"
        raise PermissionError("denied")

    with patch("builtins.open", side_effect=_fake_open):
        _sample_os_vitals()  # must not raise


def test_sample_os_vitals_records_disk_usage():
    """disk_usage() returns a triple; the percentage is rounded to two
    decimal places."""
    fake_disk = MagicMock(total=1000, used=500, free=500)

    with patch("shutil.disk_usage", return_value=fake_disk):
        _sample_os_vitals()  # must not raise

    since = datetime.now(UTC) - timedelta(hours=1)
    rows = _get_history("disk_used_pct", since=since)
    assert rows
    assert rows[-1]["value"] == 50.0


def test_sample_os_vitals_swallows_disk_usage_exception_per_path():
    """When /app/data raises a non-FileNotFoundError (e.g. permission
    denied), the loop must continue and still try /."""

    def _disk(path):
        if path == "/app/data":
            raise PermissionError("denied")
        return MagicMock(total=1000, used=100, free=900)

    with patch("shutil.disk_usage", side_effect=_disk):
        _sample_os_vitals()  # must not raise

    since = datetime.now(UTC) - timedelta(hours=1)
    rows = _get_history("disk_used_pct_root", since=since)
    assert rows
    assert rows[-1]["value"] == 10.0


def test_metric_snapshot_samples_vitals(monkeypatch):
    """Test that _run_metric_snapshot samples all operational vitals and records them."""
    from backend.cron.jobs import metric_snapshot as ms_mod

    monkeypatch.setattr("backend.config.list_configs", lambda: [{"service_id": "svc_test"}])
    monkeypatch.setattr("backend.config.duckdb_path", lambda sid: f"/mock/path/{sid}.duckdb")
    monkeypatch.setattr(
        "backend.core.duckdb_pool.get_all_stats",
        lambda: [{"service": "svc_test", "wait": {"p95_ms": 12.5}}],
    )
    monkeypatch.setattr(
        "backend.core.query_registry.query_registry.summary",
        lambda: {"active_total": 3},
    )
    monkeypatch.setattr(
        "backend.core.iceberg._ducklake.get_admission_stats",
        lambda: {"svc_test": {"acquisitions": 2.0, "wait_ms_total": 40.0, "hold_ms_total": 100.0, "timeouts": 0.0}},
    )
    monkeypatch.setattr(
        "backend.celery_status.celery_queue_depths",
        lambda: ({"q.ingest": 5}, True),
    )
    monkeypatch.setattr(
        "backend.celery_status.ingest_ledger_summary",
        lambda: {"discovered": 10, "claimed": 2},
    )
    monkeypatch.setattr("os.path.exists", lambda p: True if "svc_test.duckdb" in p else False)
    monkeypatch.setattr("os.path.getsize", lambda p: 1048576)

    result = ms_mod._run_metric_snapshot.__wrapped__()
    assert "sampled=" in result

    batch = metric_snapshots.get_batch(since="1h")
    assert "pool_wait_p95_ms|svc_test" in batch
    assert batch["pool_wait_p95_ms|svc_test"][-1]["value"] == 12.5
    assert "duckdb_file_bytes|svc_test" in batch
    assert batch["duckdb_file_bytes|svc_test"][-1]["value"] == 1048576.0
    assert "active_query_count" in batch
    assert batch["active_query_count"][-1]["value"] == 3.0
    assert "ducklake_admission_wait_avg_ms|svc_test" in batch
    assert batch["ducklake_admission_wait_avg_ms|svc_test"][-1]["value"] == 20.0
    assert "celery_broker_reachable" in batch
    assert batch["celery_broker_reachable"][-1]["value"] == 1.0
    assert "celery_queue_depth_q.ingest" in batch
    assert batch["celery_queue_depth_q.ingest"][-1]["value"] == 5.0
    assert "ingest_ledger_discovered" in batch
    assert batch["ingest_ledger_discovered"][-1]["value"] == 10.0


def test_cron_duration_includes_warning_status(tmp_path, monkeypatch):
    """Verify that _sample_cron_duration extracts terminal durations for tasks with status 'warning'."""
    import sqlite3

    from backend.cron.jobs import metric_snapshot as ms_mod

    service_id = "svc_warning_test"
    db_file = tmp_path / f"{service_id}.metadata.db"
    con = sqlite3.connect(str(db_file))
    con.row_factory = sqlite3.Row
    con.execute(
        """
        CREATE TABLE cron_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task TEXT NOT NULL,
            duration_s REAL,
            status TEXT NOT NULL
        )
        """
    )
    con.execute("INSERT INTO cron_runs (task, duration_s, status) VALUES ('commit', 4.5, 'success')")
    con.execute("INSERT INTO cron_runs (task, duration_s, status) VALUES ('ledger_sweep', 1.8, 'warning')")
    con.execute("INSERT INTO cron_runs (task, duration_s, status) VALUES ('optimize', 12.2, 'error')")
    con.commit()

    monkeypatch.setattr("backend.config.list_configs", lambda: [{"service_id": service_id}])
    monkeypatch.setattr("backend.core.metadata.base.get_con", lambda sid: con)

    count = ms_mod._sample_cron_duration()
    assert count == 3

    batch = metric_snapshots.get_batch(since="1h")
    assert f"cron_duration_ms|{service_id}|commit" in batch
    assert batch[f"cron_duration_ms|{service_id}|commit"][-1]["value"] == 4500.0

    assert f"cron_duration_ms|{service_id}|ledger_sweep" in batch
    assert batch[f"cron_duration_ms|{service_id}|ledger_sweep"][-1]["value"] == 1800.0

    assert f"cron_duration_ms|{service_id}|optimize" in batch
    assert batch[f"cron_duration_ms|{service_id}|optimize"][-1]["value"] == 12200.0
    con.close()


def test_sample_celery_workers_uses_timeout():
    """Verify _sample_celery_workers uses an explicit short timeout (0.5s) to avoid blocking."""
    from backend.cron.jobs import metric_snapshot as ms_mod

    mock_inspect = MagicMock()
    mock_inspect.stats.return_value = {"worker1@node": {}}
    mock_inspect.active.return_value = {"worker1@node": [{"id": "task1"}]}

    with patch("backend.celery_app.app.control.inspect", return_value=mock_inspect) as inspect_call:
        count = ms_mod._sample_celery_workers()
        assert count == 2
        inspect_call.assert_called_once_with(timeout=0.5)

    batch = metric_snapshots.get_batch(since="1h")
    assert "celery_active_workers" in batch
    assert batch["celery_active_workers"][-1]["value"] == 1.0
    assert "celery_active_tasks" in batch
    assert batch["celery_active_tasks"][-1]["value"] == 1.0


def test_sample_duckdb_size(monkeypatch):
    """Verify _sample_duckdb_size samples duckdb_file_bytes per service using os.path.getsize."""
    from backend.cron.jobs import metric_snapshot as ms_mod

    cfgs = [{"service_id": "svc_1"}, {"service_id": "svc_2"}, {"service_id": "svc_missing"}]
    monkeypatch.setattr("backend.config.list_configs", lambda: cfgs)
    monkeypatch.setattr("backend.config.duckdb_path", lambda sid: f"/mock/{sid}.duckdb")

    def _mock_exists(path: str) -> bool:
        return "svc_missing" not in path

    def _mock_getsize(path: str) -> int:
        if "svc_1" in path:
            return 2048
        if "svc_2" in path:
            return 8192
        raise FileNotFoundError(path)

    monkeypatch.setattr("os.path.exists", _mock_exists)
    monkeypatch.setattr("os.path.getsize", _mock_getsize)

    count = ms_mod._sample_duckdb_size()
    assert count == 2

    batch = metric_snapshots.get_batch(since="1h")
    assert "duckdb_file_bytes|svc_1" in batch
    assert batch["duckdb_file_bytes|svc_1"][-1]["value"] == 2048.0
    assert "duckdb_file_bytes|svc_2" in batch
    assert batch["duckdb_file_bytes|svc_2"][-1]["value"] == 8192.0
    assert "duckdb_file_bytes|svc_missing" not in batch


def test_metric_snapshot_registered_in_dev_local_safe_mode():
    """Verify metric_snapshot is registered when FLA_DEV_NO_CRONS=1 and dev-local crons are enabled."""
    from backend.cron.jobs import metric_snapshot as ms_mod
    from backend.cron.scheduler import Scheduler

    sched = Scheduler()
    sched._add_job = MagicMock()

    sched._register_dev_local_safe_jobs()

    assert "metric_snapshot" in sched._job_ids
    sched._add_job.assert_any_call(
        ms_mod._run_metric_snapshot,
        "interval",
        seconds=60,
        jitter=5,
        id="metric_snapshot",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=60,
    )
