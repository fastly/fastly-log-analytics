"""Contract tests for Cron 8: expire_{service_id}.

These pin the retention, snapshot cleanup, cache, quarantine, accounting,
politeness, manual-trigger, and dev-safety requirements in
``docs/cron/jobs/expire.md``.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from backend.core import duckdb as duckdb_mod
from backend.core import iceberg as iceberg_mod
from backend.core.iceberg import buffer as buffer_mod
from backend.cron.jobs import expire
from backend.cron.scheduler import Scheduler


def _source(tmp_path, service_id: str = "svc-expire-contract") -> dict:
    cache_root = tmp_path / service_id
    cache_root.mkdir(parents=True, exist_ok=True)
    return {
        "name": service_id,
        "service_id": service_id,
        "bucket": "test-fos-bucket",
    }


def _maintenance_config(monkeypatch, **cron_sync) -> None:
    monkeypatch.setattr(
        "backend.config.load_config",
        lambda _service_id: {"provisioning": {"cron_sync": cron_sync}},
    )


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

    with patch("backend.cron.jobs.expire._run_expire_snapshots", return_value={"status": "success"}) as run_expire:
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
        "backend.utils.usage_logger.flush_usage_log",
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
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(side_effect=RuntimeError("already running")))
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


@pytest.mark.parametrize("mode", ["inprocess", "external"])
def test_dev_no_crons_blocks_expire_registration_and_execution(mode, monkeypatch, tmp_path):
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
    scheduler.mode = mode
    monkeypatch.setattr("backend.config.list_configs", lambda: [cfg])
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda _sid: src)
    monkeypatch.setattr("backend.core.duckdb.is_configured", lambda _source: True)
    monkeypatch.setattr("backend.config.get_ngwaf_workspace_id", lambda _sid: None)
    monkeypatch.setattr(scheduler, "_add_job", lambda *args, **kwargs: None)

    scheduler._sync_jobs()

    assert f"expire_{service_id}" not in scheduler._job_ids
    assert scheduler._routes_to_redbeat(f"expire_{service_id}") is False

    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock())
    monkeypatch.setattr("backend.core.iceberg.run_cloud_maintenance", MagicMock())
    skipped = expire._run_expire_snapshots.__wrapped__(service_id, manual=True)
    assert skipped["status"] == "skipped"
    assert "FLA_DEV_NO_CRONS=1" in skipped["summary"]
    duckdb_mod.start_cron_run.assert_not_called()
    iceberg_mod.run_cloud_maintenance.assert_not_called()


def test_external_mode_registers_expire_on_the_serving_scheduler(monkeypatch, tmp_path):
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
    scheduler.mode = "external"
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
    assert scheduler._routes_to_redbeat(job_id) is False


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
