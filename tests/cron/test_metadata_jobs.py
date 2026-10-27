"""Consolidated tests for test_metadata_jobs."""

from __future__ import annotations

import collections
import contextlib
import json
import logging as _logging
import os
import shutil
import threading
import time
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from backend.core.metadata.cron_log import CronLeaseHeldError
from backend.cron.jobs.compaction import (
    _call_with_lookback,
    _run_partial_hour_merge,
    _run_rollup_compact_daily,
    _run_rollup_hour_heal,
)
from backend.deps import get_service_id, get_source, require_admin
from backend.utils.date_utils import iso_z
from starlette.testclient import TestClient
from tests._helpers.config_fakes import fake_mutate_config

from backend.core import duckdb as _db
from backend.core import local_compaction as lc
from backend.core import metadata as metadata_db
from backend.core import share_db
from backend.core.metadata import reconciliation, usage_log_db
from backend.core.rollups import partial_hour as ph
from backend.cron.jobs import compaction
from backend.cron.jobs import metadata as metadata_job
from backend.cron.jobs.metadata import (
    _run_bot_data_refresh,
    _run_metadata_cleanup,
    _run_ngwaf_bot_sync,
    _run_rdns_enrichment,
    _run_share_audit_purge,
)
from backend.cron.scheduler import TASK_MAP, Scheduler, partial_hour_merge_interval_sec
from backend.main import app
from backend.repositories._base import _LIVE_TOPN_SKIP_FIELDS, QueryRunner
from backend.utils import rdns_cache
from tests.cron.conftest import _StubSource

metadata = metadata_job


def _collect_sse_events(body: bytes) -> list[dict]:
    events: list[dict] = []
    for line in body.decode("utf-8", errors="replace").splitlines():
        if line.startswith("data: "):
            try:
                events.append(json.loads(line[6:]))
            except json.JSONDecodeError:
                continue
    return events


def _seed_usage_log(service_id: str, rows: int, days_ago: int = 0) -> None:
    con = usage_log_db.get_con(service_id)
    now_ts = iso_z(datetime.now(UTC) - timedelta(days=days_ago))
    con.executemany(
        "INSERT INTO usage_log (timestamp, service_id, operation_class, operation_type, bytes, count) VALUES (?, ?, 'A', 'PUT_OBJECT', 0, 1)",
        [(now_ts, service_id) for _ in range(rows)],
    )
    con.execute(
        """INSERT INTO usage_log_hourly_summary (service_id, hour, operation_class, operation_type, count, bytes, last_updated)
           VALUES (?, ?, 'A', 'PUT_OBJECT', ?, 0, ?)
           ON CONFLICT (service_id, hour, operation_class, operation_type)
           DO UPDATE SET count = usage_log_hourly_summary.count + EXCLUDED.count, last_updated = EXCLUDED.last_updated""",
        (service_id, now_ts[:13], rows, now_ts),
    )
    con.commit()


def _seed_ingested_files(
    service_id: str, rows: int, days_ago: int = 0, table_name: str = "logs", prefix: str = "raw/requests"
) -> None:
    con = metadata_db.get_con(service_id)
    con.executemany(
        f"INSERT INTO ingested_files (file_name, source_name, table_name, ingested_at, row_count, file_size_bytes) VALUES (?, 'fos', ?, datetime('now', '-{days_ago} days'), 1, 100)",
        [(f"{prefix}/{days_ago}d-{table_name}-{i}.gz", table_name) for i in range(rows)],
    )
    con.commit()


def _seed_cron_runs(service_id: str, rows: int, days_ago: int = 0) -> None:
    con = metadata_db.get_con(service_id)
    con.executemany(
        f"INSERT INTO cron_runs (task, started_at, duration_s, status, parquet_keys) VALUES ('sync', datetime('now', '-{days_ago} days'), 1.0, 'success', '[]')",
        [() for _ in range(rows)],
    )
    con.commit()


def _seed_slow_queries(service_id: str, rows: int, days_ago: int = 0) -> list[int]:
    con = metadata_db.get_con(service_id)
    cutoff = time.time() - (days_ago * 86400)
    sql = """INSERT INTO slow_queries (
        query_id, db_type, service_id, started_at_utc, ended_at_utc, duration_ms,
        outcome, sql_preview, sql_len, attr_kind, attr_label, attr_caller_qualname, attr_caller_file
    ) VALUES (1, 'duckdb', ?, ?, ?, 100.0, 'success', 'SELECT 1', 8, 'api', 'test', 'test_caller', 'test.py') RETURNING id"""
    ids = [con.execute(sql, (service_id, cutoff, cutoff + 0.1)).fetchone()[0] for _ in range(rows)]
    con.commit()
    return ids


def _sync_scheduler_with(s: Scheduler, cfg: dict, src: dict) -> None:
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()


@pytest.fixture
def mc_contract_source(monkeypatch, tmp_path):
    service_id = "svc-mc-contract"
    cache_root = tmp_path / "cache" / service_id
    cache_root.mkdir(parents=True, exist_ok=True)
    src = {
        "name": service_id,
        "service_id": service_id,
        "service_name": service_id,
        "logging_service_id": "log-svc-mc-contract",
        "bucket": "test-mc-contract-bucket",
        "_cache_dir_override": str(cache_root),
        "access_level": "read_write",
        "log_retention_days": 90,
        "metadata_retention": {
            "usage_log_days": 1,
            "ingested_files_days": 7,
            "cron_runs_days": 7,
            "slow_queries_days": 30,
        },
        "provisioning": {
            "access_level": "read_write",
            "cron_sync": {"enabled": True, "delete_after": True},
            "cron_metadata_cleanup": {"enabled": True, "cron_hour": 3, "cron_minute": 0},
        },
    }
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src if sid == service_id else None)
    monkeypatch.setattr("backend.config.load_config", lambda sid: src if sid == service_id else None)
    return src


def _patch_cleanup_progress_mocks(monkeypatch, run_id: int = 999):
    start_prog, end_prog, finalize_dur, log_cron, purge_mock = (
        MagicMock(),
        MagicMock(),
        MagicMock(),
        MagicMock(),
        MagicMock(),
    )
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=run_id))
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_cron)
    monkeypatch.setattr("backend.core.metric_snapshots.purge_old", purge_mock)
    monkeypatch.setattr("backend.cron_progress.start_progress", start_prog)
    monkeypatch.setattr("backend.cron_progress.end_progress", end_prog)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())
    monkeypatch.setattr("backend.cron.jobs._common.finalize_cron_duration", finalize_dur)
    return start_prog, end_prog, finalize_dur, log_cron, purge_mock


def test_metadata_cleanup_skip_gates_and_zero_fos_calls(monkeypatch, mc_contract_source):
    sid = mc_contract_source["service_id"]
    start_cron, cleanup_mock = MagicMock(), MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start_cron)
    monkeypatch.setattr("backend.core.metadata.cleanup_metadata", cleanup_mock)

    # 1. FLA_DEV_NO_CRONS skips execution and scheduler registration
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    metadata_job._run_metadata_cleanup(sid)
    start_cron.assert_not_called()
    cleanup_mock.assert_not_called()

    s = Scheduler()
    s._sched = MagicMock()
    _sync_scheduler_with(s, mc_contract_source, mc_contract_source)
    assert all(not kw.get("id", "").startswith("metadata_cleanup_") for _, kw in s._sched.add_job.call_args_list)

    # 2. Politeness deferral skips execution
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", MagicMock(return_value=True))
    metadata_job._run_metadata_cleanup(sid)
    start_cron.assert_not_called()

    # 3. Lease held skips without logging row
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", MagicMock(return_value=False))
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(side_effect=CronLeaseHeldError("held")))
    log_cron, end_prog = MagicMock(), MagicMock()
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_cron)
    monkeypatch.setattr("backend.cron_progress.end_progress", end_prog)
    _run_metadata_cleanup(sid)
    log_cron.assert_not_called()
    cleanup_mock.assert_not_called()
    end_prog.assert_not_called()

    # 4. Zero FOS calls during normal cleanup
    boto3_mock = MagicMock()
    monkeypatch.setattr("boto3.client", boto3_mock)
    monkeypatch.setattr("boto3.resource", boto3_mock)
    monkeypatch.undo()
    boto3_mock2 = MagicMock()
    monkeypatch.setattr("boto3.client", boto3_mock2)
    monkeypatch.setattr("boto3.resource", boto3_mock2)
    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service", lambda s: mc_contract_source if s == sid else None
    )
    monkeypatch.setattr("backend.config.load_config", lambda s: mc_contract_source if s == sid else None)
    _patch_cleanup_progress_mocks(monkeypatch, run_id=999)
    metadata_job._run_metadata_cleanup(sid)
    boto3_mock2.assert_not_called()


def test_metadata_cleanup_retention_windows_and_dedup_suppression(monkeypatch, mc_contract_source):
    sid = mc_contract_source["service_id"]

    # Dedup suppression when delete_after=False
    mc_contract_source["provisioning"]["cron_sync"]["delete_after"] = False
    _seed_ingested_files(sid, 5, days_ago=20)
    events: list[dict] = []
    res = reconciliation.cleanup_metadata(sid, retention={"ingested_files_days": 7}, on_event=events.append)
    assert res["deleted"]["ingested_files"] == 0 and res["after"]["ingested_files"] == 5
    override_msgs = [e["message"] for e in events if "cron_sync.delete_after=false" in e.get("message", "")]
    assert len(override_msgs) == 1 and "ingested_files retention (7d) ignored" in override_msgs[0]

    # RUM window calculation when delete_after=True
    mc_contract_source["provisioning"]["cron_sync"]["delete_after"] = True
    mc_contract_source["log_retention_days"] = 90
    _seed_ingested_files(sid, 3, days_ago=10, table_name="logs", prefix="raw/request")
    _seed_ingested_files(sid, 2, days_ago=2, table_name="logs", prefix="raw/request")
    for tbl in ("client_vitals", "client_errors"):
        _seed_ingested_files(sid, 2, days_ago=400, table_name=tbl, prefix="raw/rum")
        _seed_ingested_files(sid, 1, days_ago=20, table_name=tbl, prefix="raw/rum")

    res_rum = reconciliation.cleanup_metadata(sid, retention={"ingested_files_days": 7})
    assert res_rum["deleted"]["ingested_files"] == 12 and res_rum["after"]["ingested_files"] == 4
    summary_row = (
        metadata_db.get_con(sid)
        .execute("SELECT file_count FROM ingested_files_summary WHERE source_name = 'fos'")
        .fetchone()
    )
    if summary_row:
        assert summary_row[0] == 4

    # Usage log chunked deletion preserves hourly summary
    _seed_usage_log(sid, 20, days_ago=5)
    _seed_usage_log(sid, 10, days_ago=0)
    u_con = usage_log_db.get_con(sid)
    assert (
        u_con.execute("SELECT sum(count) FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()[0]
        == 30
    )
    res_usage = reconciliation.cleanup_metadata(sid, retention={"usage_log_days": 1})
    assert res_usage["deleted"]["usage_log"] == 20 and res_usage["after"]["usage_log"] == 10
    assert (
        u_con.execute("SELECT sum(count) FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()[0]
        == 30
    )

    # Cron runs, slow queries, and metric snapshots retention
    _seed_cron_runs(sid, 4, days_ago=15)
    _seed_cron_runs(sid, 2, days_ago=2)
    expired_ids = _seed_slow_queries(sid, 5, days_ago=45)
    recent_ids = _seed_slow_queries(sid, 3, days_ago=5)
    _, _, _, _, purge_mock = _patch_cleanup_progress_mocks(monkeypatch, run_id=101)
    metadata_job._run_metadata_cleanup(sid)
    con = metadata_db.get_con(sid)
    assert con.execute("SELECT count(*) FROM cron_runs").fetchone()[0] == 2
    seeded_ids = expired_ids + recent_ids
    placeholders = ", ".join("?" for _ in seeded_ids)
    remaining_sq = con.execute(
        f"SELECT id FROM slow_queries WHERE service_id = ? AND id IN ({placeholders})", (sid, *seeded_ids)
    ).fetchall()
    assert {r[0] for r in remaining_sq} == set(recent_ids)
    purge_mock.assert_called_once_with(retention_days=30)


def test_metadata_cleanup_telemetry_manual_trigger_and_scheduler(monkeypatch, mc_contract_source):
    sid = mc_contract_source["service_id"]

    # Success telemetry & progress
    start_prog, end_prog, finalize_dur, log_cron, _ = _patch_cleanup_progress_mocks(monkeypatch, run_id=777)
    fake_result = {
        "deleted": {"usage_log": 5, "cron_runs": 10},
        "before": {"usage_log": 10, "cron_runs": 15},
        "after": {"usage_log": 5, "cron_runs": 5},
        "vacuumed": True,
        "duration_s": 0.25,
    }
    monkeypatch.setattr("backend.core.metadata.cleanup_metadata", MagicMock(return_value=fake_result))
    metadata_job._run_metadata_cleanup(sid)
    start_prog.assert_called_once_with(777, service_id=sid, task="metadata_cleanup")
    end_prog.assert_called_once_with(777)
    finalize_dur.assert_called_once()
    args, kwargs = log_cron.call_args
    assert (args[1], args[3], kwargs["rows_ingested"], kwargs["run_id"]) == ("metadata_cleanup", "success", 15, 777)
    assert "Trimmed 15 rows" in kwargs["summary"]

    # Error finalization
    _, end_prog, finalize_dur, log_cron, _ = _patch_cleanup_progress_mocks(monkeypatch, run_id=888)
    monkeypatch.setattr(
        "backend.core.metadata.cleanup_metadata", MagicMock(side_effect=RuntimeError("simulated metadata fail"))
    )
    metadata_job._run_metadata_cleanup(sid)
    end_prog.assert_called_once_with(888)
    finalize_dur.assert_called_once()
    args, kwargs = log_cron.call_args
    assert (args[1], args[3], kwargs["run_id"]) == (
        "metadata_cleanup",
        "error",
        888,
    ) and "simulated metadata fail" in kwargs["summary"]

    # Manual trigger SSE endpoint parity & RBAC
    app.dependency_overrides[get_source] = lambda: mc_contract_source
    app.dependency_overrides[get_service_id] = lambda: sid
    client = TestClient(app)
    try:
        with (
            patch("backend.core.duckdb.start_cron_run", return_value=501),
            patch("backend.core.duckdb.log_cron_run"),
            patch(
                "backend.core.metadata.cleanup_metadata",
                return_value={"deleted": {"usage_log": 12, "cron_runs": 3}, "vacuumed": True},
            ),
        ):
            for path in (f"/api/admin/metadata-cleanup/{sid}", "/api/admin/metadata-cleanup"):
                resp = client.post(path, headers={"x-fastly-service-id": sid})
                assert resp.status_code == 200 and resp.headers["content-type"].startswith("text/event-stream")
                assert any(
                    e.get("type") == "done" and "Trimmed 15 rows" in e.get("message", "")
                    for e in _collect_sse_events(resp.content)
                )

            from fastapi import HTTPException

            app.dependency_overrides[require_admin] = lambda: (_ for _ in ()).throw(
                HTTPException(status_code=403, detail={"error": "admin_only"})
            )
            assert (
                client.post(f"/api/admin/metadata-cleanup/{sid}", headers={"x-fastly-service-id": sid}).status_code
                == 403
            )
    finally:
        app.dependency_overrides.clear()

    # Dynamic scheduler rescheduling and disable
    s = Scheduler()
    mock_job = MagicMock()
    s._sched = MagicMock()
    s._sched.get_job = MagicMock(return_value=mock_job)
    s._job_ids[f"metadata_cleanup_{sid}"] = f"metadata_cleanup_{sid}"
    mc_contract_source["provisioning"]["cron_metadata_cleanup"] = {"cron_hour": 4, "cron_minute": 45}
    _sync_scheduler_with(s, mc_contract_source, mc_contract_source)
    mock_job.reschedule.assert_called_once_with("cron", hour=4, minute=45)

    mc_contract_source["provisioning"]["cron_metadata_cleanup"] = {"enabled": False}
    s2 = Scheduler()
    s2._sched = MagicMock()
    _sync_scheduler_with(s2, mc_contract_source, mc_contract_source)
    assert all(not kw.get("id", "").startswith("metadata_cleanup_") for _, kw in s2._sched.add_job.call_args_list)


# ── Metadata Sync Contract ───────────────────────────────────────────────────


def _fake_src(service_id: str = "svc-meta-contract", access_level: str = "read_only") -> dict:
    return _StubSource(service_id=service_id, access_level=access_level)


def _fake_cfg(
    service_id: str = "svc-meta-contract",
    access_level: str = "read_only",
    log_period: int = 60,
    sync_enabled: bool = True,
    sync_interval_seconds: int | None = None,
    meta_enabled: bool = True,
    meta_interval_seconds: int | None = None,
    time_range: dict | None = None,
) -> dict:
    prov: dict = {
        "access_level": access_level,
        "cron_sync": {"enabled": sync_enabled},
        "cron_metadata_sync": {"enabled": meta_enabled},
    }
    if sync_interval_seconds is not None:
        prov["cron_sync"]["interval_seconds"] = sync_interval_seconds
    if meta_interval_seconds is not None:
        prov["cron_metadata_sync"]["interval_seconds"] = meta_interval_seconds
    if time_range is not None:
        prov["time_range"] = time_range
    return {
        "service_id": service_id,
        "name": service_id,
        "log_period": log_period,
        "access_level": access_level,
        "provisioning": prov,
    }


def _patch_iceberg_sync(
    monkeypatch,
    *,
    init_ret: object = True,
    table_exists: bool = True,
    files: int = 0,
    rows: int = 0,
    admin_err: Exception | None = None,
):
    init_mock = MagicMock(side_effect=init_ret) if isinstance(init_ret, Exception) else MagicMock(return_value=init_ret)
    monkeypatch.setattr("backend.core.iceberg.init_iceberg_table", init_mock)
    monkeypatch.setattr("backend.core.iceberg.ducklake_table_exists", MagicMock(return_value=table_exists))
    sync_mock = MagicMock(return_value={"files_downloaded": files, "rows_downloaded": rows})
    monkeypatch.setattr("backend.core.iceberg.sync_data", sync_mock)
    update_view = MagicMock()
    monkeypatch.setattr("backend.core.iceberg.update_iceberg_view", update_view)
    import_admin = MagicMock(side_effect=admin_err) if admin_err else MagicMock()
    monkeypatch.setattr("backend.state_sync.import_admin_state", import_admin)
    return sync_mock, update_view, import_admin


@pytest.fixture
def stub_source(monkeypatch) -> dict:
    src = _fake_src("svc-meta-contract", access_level="read_only")
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src)
    return src


@pytest.fixture
def base_context(monkeypatch, stub_source):
    cfg = _fake_cfg("svc-meta-contract", access_level="read_only")
    load_cfg = MagicMock(return_value=cfg)
    saved_cfgs: list[dict] = []
    save_cfg = MagicMock(side_effect=fake_mutate_config(lambda: cfg, saves=saved_cfgs))
    monkeypatch.setattr("backend.config.load_config", load_cfg)
    monkeypatch.setattr("backend.config.mutate_config", save_cfg)

    fake_con, start_run = MagicMock(), MagicMock(return_value=88)
    get_conn = MagicMock(return_value=fake_con)
    log_run, refresh_status, finalize_dur = MagicMock(), MagicMock(), MagicMock()
    start_prog, end_prog, log_prog, reap_prog = MagicMock(), MagicMock(), MagicMock(), MagicMock()

    monkeypatch.setattr("backend.core.duckdb.get_connection", get_conn)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start_run)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_run)
    monkeypatch.setattr("backend.core.duckdb.refresh_config_status", refresh_status)
    monkeypatch.setattr("backend.cron.jobs._common.finalize_cron_duration", finalize_dur)
    monkeypatch.setattr("backend.cron_progress.start_progress", start_prog)
    monkeypatch.setattr("backend.cron_progress.end_progress", end_prog)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", reap_prog)
    monkeypatch.setattr("backend.cron.jobs.metadata._log_and_add_progress", log_prog)
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda task, sid: False)

    return {
        "cfg": cfg,
        "load_cfg": load_cfg,
        "save_cfg": save_cfg,
        "saved_cfgs": saved_cfgs,
        "src": stub_source,
        "fake_con": fake_con,
        "get_conn": get_conn,
        "start_run": start_run,
        "log_run": log_run,
        "refresh_status": refresh_status,
        "finalize_dur": finalize_dur,
        "start_prog": start_prog,
        "end_prog": end_prog,
        "log_prog": log_prog,
        "reap_prog": reap_prog,
    }


def test_metadata_sync_role_gates_cadence_and_scheduler_registration():
    from backend.utils.remote_access import _ANALYST_BLOCKED_PREFIXES, _is_blocked_path

    assert any("/api/admin/" == p for p in _ANALYST_BLOCKED_PREFIXES)
    assert _is_blocked_path("/api/admin/rebuild-local-view") and _is_blocked_path(
        "/api/admin/rebuild-local-view?service_id=svc-1"
    )

    s = Scheduler()
    _sync_scheduler_with(
        s, _fake_cfg("svc-analyst-1", access_level="read_only"), _fake_src("svc-analyst-1", "read_only")
    )
    assert "sync_metadata_svc-analyst-1" in s._job_ids
    assert all(f"{p}_svc-analyst-1" not in s._job_ids for p in ("log_discovery", "commit", "optimize", "expire"))

    s._sched.remove_all_jobs()
    s._job_ids.clear()
    _sync_scheduler_with(s, _fake_cfg("svc-admin-1", access_level="read_write"), _fake_src("svc-admin-1", "read_write"))
    assert "sync_metadata_svc-admin-1" not in s._job_ids

    for sid, log_period, custom_sec, expected_sec in (
        ("svc-c60", 60, None, 30),
        ("svc-c15", 15, None, 15),
        ("svc-cc", 60, 45, 45),
    ):
        s._sched.remove_all_jobs()
        s._job_ids.clear()
        _sync_scheduler_with(
            s,
            _fake_cfg(sid, access_level="read_only", log_period=log_period, meta_interval_seconds=custom_sec),
            _fake_src(sid, "read_only"),
        )
        job = s._sched.get_job(f"sync_metadata_{sid}")
        assert job is not None and int(job.trigger.interval.total_seconds()) == expected_sec
        assert job.coalesce is True and job.misfire_grace_time == 60

    # Disabled vs rescheduled
    s._sched.remove_all_jobs()
    s._job_ids.clear()
    _sync_scheduler_with(
        s,
        _fake_cfg("svc-meta-dis", access_level="read_only", meta_enabled=False),
        _fake_src("svc-meta-dis", "read_only"),
    )
    assert "sync_metadata_svc-meta-dis" not in s._job_ids

    mock_job = MagicMock()
    s._sched = MagicMock()
    s._sched.get_job = MagicMock(return_value=mock_job)
    s._job_ids["sync_metadata_svc-resched"] = "sync_metadata_svc-resched"
    _sync_scheduler_with(
        s,
        _fake_cfg("svc-resched", access_level="read_only", log_period=60, meta_interval_seconds=45),
        _fake_src("svc-resched", "read_only"),
    )
    mock_job.reschedule.assert_called_once_with("interval", seconds=45)

    # Startup initial_sync trigger
    s_boot = Scheduler()
    added_jobs: list[dict] = []
    s_boot._add_job = lambda fn, *a, **kw: added_jobs.append({"fn": fn, "args": a, "kwargs": kw})
    s_boot._sched = MagicMock()
    s_boot._sync_jobs = MagicMock()
    cfgs = [
        _fake_cfg("svc-init-analyst", access_level="read_only", sync_enabled=True),
        _fake_cfg("svc-init-admin", access_level="read_write", sync_enabled=True),
        _fake_cfg("svc-init-disabled", access_level="read_only", sync_enabled=False),
    ]
    with (
        patch("backend.cron.scheduler.dev_mode_no_crons", return_value=False),
        patch("backend.core.duckdb_pool._pool_warm_at_boot_enabled", return_value=False),
        patch("backend.config.list_configs", return_value=cfgs),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src()),
        patch("backend.core.duckdb.is_configured", return_value=True),
    ):
        s_boot.start()
    job_ids = [j["kwargs"].get("id") for j in added_jobs]
    assert "initial_sync_svc-init-analyst" in job_ids
    assert "initial_sync_svc-init-admin" not in job_ids and "initial_sync_svc-init-disabled" not in job_ids


def test_post_commit_metadata_sync_dispatch_coalesce_and_error_recovery(monkeypatch):
    calls, dispatched_threads = [], []

    class DeferredThread:
        def __init__(self, *, target, args, **kwargs):
            self.target, self.args = target, args
            dispatched_threads.append(self)

        def start(self):
            return None

    def fake_sync(service_id: str) -> None:
        calls.append(service_id)
        if len(calls) == 1:
            metadata.dispatch_post_commit_metadata_sync(service_id)

    with (
        patch.object(metadata.threading, "Thread", DeferredThread),
        patch.object(metadata, "_run_post_commit_metadata_sync", fake_sync),
    ):
        metadata.dispatch_post_commit_metadata_sync("svc-admin-coalesce")
        metadata.dispatch_post_commit_metadata_sync("svc-admin-coalesce")
        assert len(dispatched_threads) == 1 and calls == []
        dispatched_threads[0].target(*dispatched_threads[0].args)
        assert calls == ["svc-admin-coalesce", "svc-admin-coalesce"]
        assert metadata._post_commit_metadata_sync_rerun == {}

    # Worker failure clears rerun state
    monkeypatch.setattr(metadata.threading, "Thread", DeferredThread)
    monkeypatch.setattr(
        metadata, "_run_post_commit_metadata_sync", MagicMock(side_effect=RuntimeError("unexpected failure"))
    )
    metadata.dispatch_post_commit_metadata_sync("svc-1")
    metadata._post_commit_metadata_sync_worker("svc-1")
    assert metadata._post_commit_metadata_sync_rerun == {}

    # Thread start failure clears rerun state
    class FailedThread:
        def __init__(self, *, target, args, **kwargs):
            pass

        def start(self):
            raise RuntimeError("thread unavailable")

    monkeypatch.setattr(metadata.threading, "Thread", FailedThread)
    with pytest.raises(RuntimeError, match="thread unavailable"):
        metadata.dispatch_post_commit_metadata_sync("svc-1")
    assert metadata._post_commit_metadata_sync_rerun == {}


def test_metadata_sync_politeness_early_exits_and_catalog_states(base_context, monkeypatch):
    # Politeness deferral on scheduled vs manual trigger
    with patch("backend.utils.active_requests.should_defer_cron", return_value=True) as mock_defer:
        metadata._run_metadata_sync("svc-meta-contract")
        mock_defer.assert_called_once_with("metadata_sync", "svc-meta-contract")
        base_context["start_run"].assert_not_called()

    _patch_iceberg_sync(monkeypatch)
    with patch("backend.utils.active_requests.should_defer_cron", return_value=True) as mock_defer:
        metadata._run_metadata_sync("svc-meta-contract", run_id=777)
        mock_defer.assert_not_called()
        assert (
            base_context["log_run"].call_args[0][3] == "success"
            and base_context["log_run"].call_args[1]["run_id"] == 777
        )

    # Uncommitted table / table not found -> graceful success skip; catalog attach None -> error
    for init_ret, tbl_exists, exp_status, exp_text in (
        (True, False, "success", "not found"),
        (ValueError("Table logs not found in catalog"), True, "success", "not found"),
        (None, True, "error", "Metadata sync failed"),
    ):
        base_context["log_run"].reset_mock()
        base_context["end_prog"].reset_mock()
        sync_data, _, _ = _patch_iceberg_sync(monkeypatch, init_ret=init_ret, table_exists=tbl_exists)
        metadata._run_metadata_sync("svc-meta-contract")
        sync_data.assert_not_called()
        args, kwargs = base_context["log_run"].call_args
        assert args[3] == exp_status and exp_text.lower() in kwargs["summary"].lower()
        assert kwargs["log_output"] is None and base_context["end_prog"].called

    # Missing config, missing source, and lease held skips
    base_context["start_run"].reset_mock()
    base_context["log_run"].reset_mock()
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)
    metadata._run_metadata_sync("svc-meta-contract")
    base_context["start_run"].assert_not_called()

    monkeypatch.setattr("backend.config.load_config", MagicMock(return_value=None))
    get_src = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", get_src)
    metadata._run_metadata_sync("ghost")
    get_src.assert_not_called()


def test_metadata_sync_lease_held_skips(base_context, monkeypatch):
    base_context["start_run"].side_effect = CronLeaseHeldError("already running")
    init = MagicMock()
    monkeypatch.setattr("backend.core.iceberg.init_iceberg_table", init)
    metadata._run_metadata_sync("svc-meta-contract")
    init.assert_not_called()
    base_context["log_run"].assert_not_called()


def test_metadata_sync_time_ranges_admin_import_and_telemetry(base_context, monkeypatch):
    # Explicit time range persisted to config
    sync_mock, _, _ = _patch_iceberg_sync(monkeypatch)
    metadata._run_metadata_sync(
        "svc-meta-contract", run_id=12, start_time="2026-06-01T00:00:00Z", end_time="2026-06-15T00:00:00Z"
    )
    tr = base_context["saved_cfgs"][-1]["provisioning"]["time_range"]
    assert tr == {"start": "2026-06-01T00:00:00Z", "end": "2026-06-15T00:00:00Z"} == base_context["src"]["time_range"]
    assert (sync_mock.call_args[1]["start_time"], sync_mock.call_args[1]["end_time"]) == (
        "2026-06-01T00:00:00Z",
        "2026-06-15T00:00:00Z",
    )

    # Manual sync all clears pinned time range
    base_context["cfg"]["provisioning"]["time_range"] = {"start": "2026-01-01T00:00:00Z"}
    _patch_iceberg_sync(monkeypatch, files=1, rows=10)
    metadata._run_metadata_sync("svc-meta-contract", run_id=999)
    assert (
        "time_range" not in base_context["saved_cfgs"][-1]["provisioning"] and base_context["src"]["time_range"] is None
    )

    # Scheduled run respects existing config time range + zero new files succeeds
    base_context["cfg"]["provisioning"]["time_range"] = {"start": "2026-05-01T00:00:00Z"}
    sync_mock, _, _ = _patch_iceberg_sync(monkeypatch, files=0, rows=0)
    metadata._run_metadata_sync("svc-meta-contract")
    assert sync_mock.call_args[1]["start_time"] == "2026-05-01T00:00:00Z"
    assert (
        base_context["log_run"].call_args[0][3] == "success"
        and "new Iceberg data file" not in base_context["log_run"].call_args[1]["summary"]
    )

    # Admin state import failure yields warning status
    base_context["refresh_status"].reset_mock()
    _, update_view, _ = _patch_iceberg_sync(
        monkeypatch, files=2, rows=500, admin_err=RuntimeError("connection refused to admin state bucket")
    )
    metadata._run_metadata_sync("svc-meta-contract")
    update_view.assert_called_once()
    base_context["refresh_status"].assert_called_once_with("svc-meta-contract")
    args, kwargs = base_context["log_run"].call_args
    assert args[3] == "warning" and "admin state import warning" in kwargs["summary"]
    assert (kwargs["files_downloaded"], kwargs["rows_ingested"]) == (2, 500)

    # Telemetry attribution, dashboard invalidation, zero Class-A cloud mutations, and error cleanup
    mock_s3 = MagicMock()
    monkeypatch.setattr("backend.core.duckdb._get_fos_client", lambda src: mock_s3)
    _, update_view, _ = _patch_iceberg_sync(monkeypatch, files=3, rows=2_500)
    invalidate_mock = MagicMock()
    monkeypatch.setattr("backend.repositories.dashboard.invalidate_service", invalidate_mock)
    metadata._run_metadata_sync("svc-meta-contract", run_id=101)
    invalidate_mock.assert_called_once_with(base_context["src"]["name"])
    assert "downloaded 3 new Iceberg data file(s)" in base_context["log_run"].call_args[1]["summary"]
    base_context["end_prog"].assert_called_with(101)
    for m in (mock_s3.put_object, mock_s3.delete_object, mock_s3.delete_objects, mock_s3.create_multipart_upload):
        m.assert_not_called()

    base_context["end_prog"].reset_mock()
    base_context["finalize_dur"].reset_mock()
    monkeypatch.setattr("backend.core.iceberg.sync_data", MagicMock(side_effect=RuntimeError("disk write error")))
    metadata._run_metadata_sync("svc-meta-contract", run_id=102)
    base_context["end_prog"].assert_called_with(102)
    base_context["finalize_dur"].assert_called_once()
    assert (
        base_context["log_run"].call_args[0][3] == "error"
        and "disk write error" in base_context["log_run"].call_args[1]["error_message"]
    )


def test_rebuild_local_view_endpoint_lifecycle_and_busy():
    test_src = _fake_src("svc-rebuild-test")
    app.dependency_overrides[get_source] = lambda: test_src
    client = TestClient(app)
    try:
        with (
            patch("backend.core.iceberg.clear_source_caches") as mock_clear,
            patch("backend.core.duckdb._cache_dir", return_value="/tmp/test_cache"),
            patch("os.path.exists", return_value=True),
            patch("os.remove"),
            patch("backend.core.duckdb.start_cron_run", return_value="run-789") as mock_start,
            patch("backend.cron_progress.start_progress") as mock_prog,
            patch("threading.Thread") as mock_thread,
        ):
            resp = client.post("/api/admin/rebuild-local-view")
            body = resp.json()
            assert resp.status_code == 202 and (body["ok"], body["run_id"], body["message"]) == (
                True,
                "run-789",
                "Local view rebuild started.",
            )
            mock_clear.assert_called_once_with("svc-rebuild-test")
            mock_start.assert_called_once_with(test_src, "metadata_sync")
            mock_prog.assert_called_once_with("run-789", service_id="svc-rebuild-test", task="metadata_sync")
            mock_thread.assert_called_once()

        with (
            patch("backend.core.iceberg.clear_source_caches"),
            patch("backend.core.duckdb._cache_dir", return_value="/tmp/test_cache"),
            patch("os.path.exists", return_value=False),
            patch("backend.core.duckdb.start_cron_run", side_effect=CronLeaseHeldError("cron job busy")),
        ):
            resp_busy = client.post("/api/admin/rebuild-local-view")
            assert resp_busy.status_code == 503 and "cron_busy" in str(resp_busy.json())
    finally:
        app.dependency_overrides.pop(get_source, None)


# ── Bot Data Refresh, NGWAF Bot Sync, rDNS Enrichment, Share Audit Purge ─────


@pytest.mark.parametrize(
    ("dev_mode", "fake_results", "expected_status", "expected_substr"),
    [
        (
            False,
            [{"id": "well-known-bots", "entry_count": 100}, {"id": "tor-exit-nodes", "entry_count": 50}],
            "success",
            "150",
        ),
        (
            False,
            [
                {"id": "well-known-bots", "entry_count": 100},
                {"id": "tor-exit-nodes", "error": "HTTP 503", "entry_count": 0, "failed": True},
            ],
            "warning",
            "1 source(s) failed: tor-exit-nodes",
        ),
        (
            False,
            [
                {"id": "well-known-bots", "error": "Conn refused", "entry_count": 0, "failed": True},
                {"id": "tor-exit-nodes", "error": "DNS fail", "entry_count": 0, "failed": True},
            ],
            "error",
            "All 2 bot sources failed",
        ),
        (True, [], "skipped", "dev_mode_no_crons"),
    ],
)
def test_run_bot_data_refresh_outcomes(dev_mode, fake_results, expected_status, expected_substr):
    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=dev_mode),
        patch("backend.utils.bot_sources.refresh_all_sources", return_value=fake_results) as mock_refresh,
        patch("backend.cron.decorators.record_job_run") as mock_record,
    ):
        _run_bot_data_refresh()
    if dev_mode:
        mock_refresh.assert_not_called()
    args = mock_record.call_args[0]
    assert (args[0], args[1]) == ("bot_data_refresh", expected_status) and expected_substr in args[3]


def test_admin_refresh_all_bot_sources_endpoints():
    client = TestClient(app)
    fake_results = [{"id": "well-known-bots", "entry_count": 100}, {"id": "tor-exit-nodes", "entry_count": 50}]
    with patch("backend.utils.bot_sources.refresh_all_sources", return_value=fake_results) as mock_refresh:
        for path in ("/api/admin/bot-sources/refresh", "/api/admin/bots/refresh"):
            resp = client.post(path)
            assert resp.status_code == 200 and resp.json()["ok"] is True and resp.json()["updated_count"] == 2
        assert mock_refresh.call_count == 2


@pytest.fixture
def fake_cfg():
    return {
        "service_id": "test_service",
        "name": "Test Service",
        "fastly_api_key": "test_api_key",
        "ngwaf": {"workspace_id": "ws_123"},
        "provisioning": {"cron_ngwaf": {"interval_mins": 5, "log_retention_days": 30}},
    }


@contextmanager
def _ngwaf_sync_env(
    fake_cfg: dict,
    *,
    run_id: int = 1,
    start_side_effect: Exception | None = None,
    last_ts: str | None = "2026-09-01T00:00:00Z",
    fetch_ret: list | None = None,
    fetch_side_effect: object = None,
):
    mock_src = {"name": "test_service", "id": "test_service"}
    mock_sched, mock_job = MagicMock(), MagicMock()
    mock_sched.get_job.return_value = mock_job
    start_kwargs = {"side_effect": start_side_effect} if start_side_effect else {"return_value": run_id}
    fetch_kwargs = {"side_effect": fetch_side_effect} if fetch_side_effect else {"return_value": fetch_ret or []}

    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws_123"),
        patch("backend.core.duckdb.get_source_for_service", return_value=mock_src),
        patch("backend.core.duckdb.start_cron_run", **start_kwargs),
        patch("backend.core.duckdb.log_cron_run") as mock_log,
        patch("backend.utils.ngwaf.get_last_timestamp", return_value=last_ts),
        patch("backend.utils.ngwaf.update_sync_watermark") as mock_update_wm,
        patch("backend.utils.ngwaf.fetch_verified_bots_paged", **fetch_kwargs) as mock_fetch,
        patch("backend.utils.ngwaf.upsert_bots") as mock_upsert,
        patch("backend.utils.ngwaf.cleanup_old_bots", return_value=0),
        patch("backend.cron.scheduler.get_scheduler", return_value=mock_sched),
        patch("backend.cron_progress.start_progress") as mock_start_prog,
        patch("backend.cron_progress.end_progress") as mock_end_prog,
        patch("backend.cron.jobs._common.finalize_cron_duration") as mock_finalize,
    ):
        yield {
            "src": mock_src,
            "log": mock_log,
            "update_wm": mock_update_wm,
            "fetch": mock_fetch,
            "upsert": mock_upsert,
            "job": mock_job,
            "start_prog": mock_start_prog,
            "end_prog": mock_end_prog,
            "finalize": mock_finalize,
        }


def test_ngwaf_sync_lifecycle_adaptive_rescheduling_and_errors(fake_cfg):
    # Dev mode skip
    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=True),
        patch("backend.config.load_config") as mock_load,
    ):
        _run_ngwaf_bot_sync("test_service")
    mock_load.assert_not_called()

    # First run watermark seed + guaranteed cleanup
    with _ngwaf_sync_env(fake_cfg, run_id=101, last_ts=None) as env:
        _run_ngwaf_bot_sync("test_service")
    env["update_wm"].assert_called_once()
    env["fetch"].assert_not_called()
    assert env["log"].call_args[0][3] == "success" and "First sync" in env["log"].call_args[1]["summary"]
    env["end_prog"].assert_called_once_with(101)
    assert env["finalize"].call_args[0][:2] == (env["src"], 101)

    # Normal paging & baseline interval (5m) vs heavy load adaptive interval (2m)
    recs_2 = [{"waf_req_id": f"req_{i}", "bot_name": "Googlebot"} for i in range(2)]
    with _ngwaf_sync_env(fake_cfg, run_id=42, fetch_ret=[(recs_2, "2026-09-01T01:00:00Z", 2)]) as env:
        _run_ngwaf_bot_sync("test_service")
    env["upsert"].assert_called_once()
    env["start_prog"].assert_called_once_with(42, service_id="test_service", task="ngwaf_sync")
    env["end_prog"].assert_called_once_with(42)
    env["job"].reschedule.assert_called_with("interval", minutes=5)

    recs_500 = [{"waf_req_id": f"req_{i}", "bot_name": "Googlebot"} for i in range(500)]
    with _ngwaf_sync_env(fake_cfg, fetch_ret=[(recs_500, "2026-09-01T01:00:00Z", 500)]) as env:
        _run_ngwaf_bot_sync("test_service")
    env["job"].reschedule.assert_called_with("interval", minutes=2)

    # Budget exceeded adaptive reschedule
    clock = [1000.0]

    def fake_fetch_paged(*_a, **_kw):
        clock[0] = 1245.0
        yield (recs_2, "2026-09-01T01:00:00Z", 2)

    with (
        _ngwaf_sync_env(fake_cfg, fetch_side_effect=fake_fetch_paged) as env,
        patch("time.time", side_effect=lambda: clock[0]),
    ):
        _run_ngwaf_bot_sync("test_service")
    assert "budget reached — will continue next run" in env["log"].call_args[1]["summary"]
    env["job"].reschedule.assert_called_with("interval", minutes=2)

    # Auth error classification, generic error shape, and lease held skip
    with _ngwaf_sync_env(
        fake_cfg, fetch_side_effect=RuntimeError("Fastly API 401 Unauthorized: invalid_api_key")
    ) as env:
        _run_ngwaf_bot_sync("test_service")
    assert env["log"].call_args[0][3] == "error" and "authentication error" in env["log"].call_args[1]["summary"]

    with _ngwaf_sync_env(fake_cfg, run_id=5, fetch_side_effect=RuntimeError("socket reset")) as env:
        _run_ngwaf_bot_sync("test_service")
    args, kwargs = env["log"].call_args
    assert (
        args[1],
        args[3],
        kwargs["summary"],
        kwargs["error_message"],
        kwargs["files_downloaded"],
        kwargs["run_id"],
        kwargs["log_output"],
    ) == (
        "ngwaf_sync",
        "error",
        "NGWAF sync failed: socket reset",
        "socket reset",
        0,
        5,
        None,
    )
    env["end_prog"].assert_called_once_with(5)

    with _ngwaf_sync_env(fake_cfg, start_side_effect=CronLeaseHeldError("held")) as env:
        _run_ngwaf_bot_sync("test_service")
    env["log"].assert_not_called()
    env["fetch"].assert_not_called()


def test_admin_ngwaf_endpoints(fake_cfg):
    client = TestClient(app)
    with (
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws_123"),
        patch("backend.cron.jobs.metadata._run_ngwaf_bot_sync") as mock_run,
        patch(
            "backend.utils.ngwaf.get_cache_stats",
            return_value={"total_cached_bots": 42, "workspaces": {"ws_123": "2026-09-01T00:00:00Z"}, "top_bots": {}},
        ),
    ):
        sync_resp = client.post("/api/admin/ngwaf/sync/test_service")
        assert sync_resp.status_code == 200 and sync_resp.json()["stats"]["total_cached_bots"] == 42
        mock_run.assert_called_once_with("test_service")

        status_resp = client.get("/api/admin/ngwaf/status?service_id=test_service")
        assert status_resp.status_code == 200 and status_resp.json()["service_configured"] is True


@pytest.mark.parametrize(
    ("dev_mode", "fake_summary", "expected_status", "expected_substr"),
    [
        (False, {"resolved": 20, "errors": 0, "discovered": 10, "batch_limit": 200}, "success", "resolved=20"),
        (
            False,
            {"resolved": 0, "errors": 8, "discovered": 0, "batch_limit": 200},
            "warning",
            "DNS lookup failures detected",
        ),
        (True, {}, "skipped", "dev_mode_no_crons"),
    ],
)
def test_run_rdns_enrichment_outcomes(dev_mode, fake_summary, expected_status, expected_substr):
    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=dev_mode),
        patch("backend.utils.rdns_cache.enrich_batch", return_value=fake_summary) as mock_enrich,
        patch("backend.cron.decorators.record_job_run") as mock_record,
    ):
        _run_rdns_enrichment()
    if dev_mode:
        mock_enrich.assert_not_called()
    args = mock_record.call_args[0]
    assert (args[0], args[1]) == ("rdns_enrichment", expected_status) and expected_substr in args[3]


@pytest.mark.parametrize(
    ("pending_count", "expected_batch_limit"), [(0, 200), (50, 100), (150, 150), (500, 500), (2500, 1000)]
)
def test_enrich_batch_dynamic_and_explicit_sizing(pending_count, expected_batch_limit):
    with (
        patch("backend.utils.rdns_cache._get_pending_count", return_value=pending_count) as mock_count,
        patch("backend.utils.rdns_cache._select_ips_with_status", return_value=[]) as mock_select,
        patch("backend.utils.rdns_cache._select_stale_ips", return_value=[]),
        patch("backend.utils.rdns_cache._discover_new_ips", return_value=0),
        patch("backend.utils.rdns_cache._maybe_reap_stale_rows"),
    ):
        assert rdns_cache.enrich_batch()["batch_limit"] == expected_batch_limit
        if pending_count == 0:
            mock_count.reset_mock()
            res = rdns_cache.enrich_batch(limit=42)
            mock_count.assert_not_called()
            assert res["batch_limit"] == 42 and mock_select.call_args[1]["limit"] == 42


def test_admin_rdns_endpoints_and_maintenance_executor_routing():
    client = TestClient(app)
    fake_summary = {"resolved": 5, "errors": 0, "discovered": 2, "batch_limit": 200}
    with patch("backend.utils.rdns_cache.enrich_batch", return_value=fake_summary) as mock_enrich:
        resp = client.post("/api/admin/rdns/enrich?limit=50")
        assert resp.status_code == 200 and resp.json()["summary"] == fake_summary
        mock_enrich.assert_called_once_with(limit=50)

    fake_stats = {"total": 1200, "pending": 45, "last_enrichment_at": "2026-09-21T00:00:00Z"}
    with patch("backend.utils.rdns_cache.get_stats", return_value=fake_stats):
        resp_stats = client.get("/api/admin/rdns/stats")
        assert resp_stats.status_code == 200 and resp_stats.json()["stats"] == fake_stats

    s = Scheduler()
    for job_id in ("rdns_enrichment", "share_audit_purge"):
        with patch.object(s._sched, "add_job") as mock_add_job:
            s._add_job(lambda: None, "interval", minutes=5, id=job_id)
            assert mock_add_job.call_args[1]["executor"] == "maintenance"


# ── Share Audit Purge ────────────────────────────────────────────────────────


@pytest.fixture
def test_share_db():
    con = share_db.get_global_share_con()
    for tbl in (
        "remote_sessions",
        "remote_invite_claim_tokens",
        "invite_services",
        "remote_invites",
        "remote_share_audit_logs",
    ):
        con.execute(f"DELETE FROM {tbl}")
    con.commit()
    yield con


def test_purge_old_audit_logs_and_stale_share_records(test_share_db):
    con = test_share_db
    old_ts = iso_z(datetime.now(UTC) - timedelta(days=95))
    recent_ts = iso_z(datetime.now(UTC) - timedelta(days=10))
    for ts, email, ip in ((old_ts, "old@example.com", "1.1.1.1"), (recent_ts, "new@example.com", "2.2.2.2")):
        con.execute(
            "INSERT INTO remote_share_audit_logs(timestamp, event_type, email, ip_address, details) VALUES (?,?,?,?,?)",
            (ts, "LOGIN_SUCCESS", email, ip, "{}"),
        )
    con.commit()
    assert share_db.purge_old_audit_logs(retention_days=90, con=con) == 1
    assert [r["email"] for r in con.execute("SELECT email FROM remote_share_audit_logs").fetchall()] == [
        "new@example.com"
    ]

    past_iso = iso_z(datetime.now(UTC) - timedelta(days=2))
    future_iso = iso_z(datetime.now(UTC) + timedelta(days=5))
    stale_sess = iso_z(datetime.now(UTC) - timedelta(days=35))
    active_sess = iso_z(datetime.now(UTC) - timedelta(hours=2))
    for inv_id, name, email, exp in (
        ("inv_expired", "Expired User", "expired@test.com", past_iso),
        ("inv_active", "Active User", "active@test.com", future_iso),
    ):
        con.execute(
            "INSERT INTO remote_invites(id, name, email, passcode, expires_at, created_at) VALUES (?,?,?,?,?,?)",
            (inv_id, name, email, "hash", exp, past_iso),
        )
    con.execute(
        "INSERT INTO remote_invite_claim_tokens(token, invite_id, created_at, expires_at) VALUES (?,?,?,?)",
        ("tok_expired", "inv_active", past_iso, past_iso),
    )
    for sid, last_ts in (("sess_stale", stale_sess), ("sess_active", active_sess)):
        con.execute(
            "INSERT INTO remote_sessions(session_id, invite_id, name, email, ip_address, user_agent, fingerprint_signature, pii_policy, login_time, last_active_time) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (sid, "inv_active", "Active User", "active@test.com", "1.1.1.1", "ua", "fp", "{}", past_iso, last_ts),
        )
    con.commit()

    stats = share_db.purge_stale_share_records(max_idle_session_days=30, con=con)
    assert (stats["deleted_expired_invites"], stats["deleted_claim_tokens"], stats["deleted_stale_sessions"]) == (
        1,
        1,
        1,
    )
    assert [r["id"] for r in con.execute("SELECT id FROM remote_invites").fetchall()] == ["inv_active"]
    assert [r["session_id"] for r in con.execute("SELECT session_id FROM remote_sessions").fetchall()] == [
        "sess_active"
    ]


def test_share_audit_purge_job_and_admin_endpoints(test_share_db):
    with (
        patch("backend.core.share_db.get_setting", return_value="60"),
        patch("backend.core.share_db.purge_old_audit_logs", return_value=5) as mock_audit,
        patch(
            "backend.core.share_db.purge_stale_share_records",
            return_value={"deleted_expired_invites": 2, "deleted_stale_sessions": 3, "deleted_claim_tokens": 1},
        ) as mock_stale,
    ):
        result = _run_share_audit_purge.__wrapped__()
    mock_audit.assert_called_once_with(retention_days=60)
    mock_stale.assert_called_once_with(max_idle_session_days=30)
    status, detail = result if isinstance(result, tuple) else ("success", result)
    assert status == "success" and all(
        t in detail for t in ("deleted=5", "retention_days=60", "expired_invites=2", "stale_sessions=3")
    )

    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=True),
        patch("backend.core.share_db.purge_old_audit_logs") as mock_audit2,
    ):
        res_skip = _run_share_audit_purge.__wrapped__()
        assert res_skip[0] == "skipped" and "dev_mode_no_crons" in res_skip[1]
        mock_audit2.assert_not_called()

    client = TestClient(app)
    with patch(
        "backend.core.share_db.purge_all_share_records",
        return_value={
            "deleted_audit_logs": 12,
            "deleted_expired_invites": 1,
            "deleted_stale_sessions": 4,
            "deleted_claim_tokens": 2,
            "audit_retention_days": 45,
            "max_idle_session_days": 15,
        },
    ) as mock_purge_all:
        resp = client.post("/api/admin/share/purge?audit_retention_days=45&max_idle_session_days=15")
        assert resp.status_code == 200 and resp.json()["deleted_audit_logs"] == 12
        mock_purge_all.assert_called_once_with(audit_retention_days=45, max_idle_session_days=15)

    con = test_share_db
    con.execute(
        "INSERT INTO remote_share_audit_logs(timestamp, event_type, email, ip_address, details) VALUES (?,?,?,?,?)",
        (iso_z(datetime.now(UTC) - timedelta(hours=1)), "LOGIN_SUCCESS", "analyst@example.com", "127.0.0.1", "{}"),
    )
    con.commit()
    resp_logs = client.get("/api/admin/share/audit-logs")
    assert resp_logs.status_code == 200 and any(
        e["email"] == "analyst@example.com" for e in resp_logs.json()["audit_logs"]
    )


# ── Merged from tests/cron/test_compaction_jobs.py ──

Usage = collections.namedtuple("Usage", ["total", "used", "free"])

CRON_DAY_COMPACTOR_ORDER = [
    "origin_summary",
    "network_rtt",
    "network_speed",
    "network_quality",
    "verified_bots_ts",
    "perf_latency",
    "origin_dims",
    "origin_latency_ts",
    "security_dims",
    "perf_dims",
    "ngwaf_bots",
    "overview",
    "pop_health",
]


@pytest.fixture
def stub_progress(monkeypatch) -> dict[str, MagicMock]:
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=42))
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
    mocks = {k: MagicMock() for k in ("start_progress", "end_progress", "cleanup", "log_event")}
    monkeypatch.setattr("backend.cron_progress.start_progress", mocks["start_progress"])
    monkeypatch.setattr("backend.cron_progress.end_progress", mocks["end_progress"])
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", mocks["cleanup"])
    monkeypatch.setattr("backend.cron.scheduler._log_and_add_progress", mocks["log_event"])
    monkeypatch.setattr("backend.cron.scheduler._extract_log_text", lambda rid: "")
    return mocks


@pytest.mark.parametrize(
    "job_fn,target_attr",
    [
        (compaction._run_local_compact, "backend.core.local_compaction.compact_local_partitions"),
        (compaction._run_rollup_compact_daily, "backend.core.rollups.compact_closed_days_to_daily"),
        (_run_rollup_hour_heal, "backend.core.rollups.backfill_missing_hour_bundles"),
    ],
)
def test_compaction_jobs_missing_source_and_lease_skip(monkeypatch, stub_source, stub_progress, job_fn, target_attr):
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)
    job_fn.__wrapped__("missing-svc")

    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: stub_source)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(side_effect=CronLeaseHeldError("running")))
    work_mock = MagicMock()
    monkeypatch.setattr(target_attr, work_mock)
    job_fn.__wrapped__("svc-1")
    work_mock.assert_not_called()
    stub_progress["start_progress"].assert_not_called()


# ── _run_local_compact & _run_rollup_compact_daily ────────────────────────────


def test_local_compact_outcomes(monkeypatch, stub_source, stub_progress):
    monkeypatch.setattr(
        "backend.core.local_compaction.compact_local_partitions",
        MagicMock(return_value={"partitions_compacted": 3, "files_merged": 12, "files_removed": 9, "errors": []}),
    )
    compaction._run_local_compact.__wrapped__("svc-1")
    args, kwargs = _db.log_cron_run.call_args
    assert args[3] == "success" and "Compacted 3 partition" in kwargs["summary"] and kwargs["error_message"] is None
    stub_progress["start_progress"].assert_called_once()
    stub_progress["end_progress"].assert_called_once()

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
    args, kwargs = _db.log_cron_run.call_args
    assert args[3] == "warning" and "4 partition error" in kwargs["summary"]
    assert "partition a failed" in kwargs["error_message"] and "1 more" in kwargs["error_message"]

    monkeypatch.setattr(
        "backend.core.local_compaction.compact_local_partitions", MagicMock(side_effect=RuntimeError("disk full"))
    )
    compaction._run_local_compact.__wrapped__("svc-1")
    args, kwargs = _db.log_cron_run.call_args
    assert args[3] == "error" and "disk full" in kwargs["error_message"]


def _mock_rollups(monkeypatch):
    monkeypatch.setattr("backend.core.rollups.compact_closed_days_to_daily", lambda *a, **kw: 0)
    monkeypatch.setattr("backend.core.rollups.backfill_day_bundles", lambda *a, **kw: 0)
    monkeypatch.setattr("backend.core.rollups.retire_compacted_hour_bundles", lambda *a, **kw: (0, 0))
    monkeypatch.setattr(
        "backend.core.rollups.backfill_missing_hour_bundles", lambda *a, **kw: {"missing": 0, "bundled": 0}
    )
    monkeypatch.setattr(
        "backend.core.rollups.backfill_missing_hour_ip_spread", lambda *a, **kw: {"missing": 0, "rebuilt": 0}
    )
    for label in CRON_DAY_COMPACTOR_ORDER:
        if hasattr(__import__("backend.core.rollups", fromlist=["*"]), f"compact_{label}_closed_days_to_daily"):
            monkeypatch.setattr(f"backend.core.rollups.compact_{label}_closed_days_to_daily", lambda *a, **kw: 0)


def test_rollup_compact_success_warning_error_and_provided_run_id(monkeypatch, stub_source, stub_progress, caplog):
    _mock_rollups(monkeypatch)
    monkeypatch.setattr("backend.core.rollups.compact_closed_days_to_daily", MagicMock(return_value=14))
    monkeypatch.setattr("backend.core.rollups.backfill_day_bundles", MagicMock(return_value=7))
    compaction._run_rollup_compact_daily.__wrapped__("svc-1")
    args, kwargs = _db.log_cron_run.call_args
    assert args[3] == "success" and "Rebuilt 14" in kwargs["summary"] and "bundled 7 day" in kwargs["summary"]

    monkeypatch.setattr("backend.core.rollups.compact_closed_days_to_daily", MagicMock(return_value=5))
    monkeypatch.setattr("backend.core.rollups.backfill_day_bundles", MagicMock(side_effect=RuntimeError("disk full")))
    with caplog.at_level(_logging.WARNING, logger="backend.scheduler"):
        compaction._run_rollup_compact_daily.__wrapped__("svc-1")
    args, kwargs = _db.log_cron_run.call_args
    assert (
        args[3] == "warning"
        and "day_bundles: disk full" in kwargs["error_message"]
        and "bundled 0 day" in kwargs["summary"]
    )

    start_mock = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start_mock)
    _mock_rollups(monkeypatch)
    compaction._run_rollup_compact_daily.__wrapped__("svc-1", run_id=321)
    start_mock.assert_not_called()

    stub_progress["start_progress"].side_effect = RuntimeError("manifest read failed")
    compaction._run_rollup_compact_daily.__wrapped__("svc-1")
    args, kwargs = _db.log_cron_run.call_args
    assert args[3] == "error" and "manifest read failed" in kwargs["error_message"]


@pytest.mark.parametrize(
    "fail_label,expected_status,expected_err",
    [(None, "success", None), ("ngwaf_bots", "warning", "ngwaf_bots: boom")],
)
def test_rollup_compact_day_compactor_order_and_isolation(
    monkeypatch, stub_source, stub_progress, fail_label, expected_status, expected_err
):
    _mock_rollups(monkeypatch)
    calls: list = []
    for label in CRON_DAY_COMPACTOR_ORDER:

        def compactor(*args, _label=label, **kwargs):
            calls.append((_label, args, kwargs))
            if _label == fail_label:
                raise RuntimeError("boom")
            return 0

        monkeypatch.setattr(f"backend.core.rollups.compact_{label}_closed_days_to_daily", compactor)

    compaction._run_rollup_compact_daily.__wrapped__("svc-1")
    assert [c[0] for c in calls] == CRON_DAY_COMPACTOR_ORDER
    assert all(args == ("svc-1", stub_source) and kwargs == {"lookback_days": 30} for _, args, kwargs in calls)
    args, kwargs = _db.log_cron_run.call_args
    assert args[3] == expected_status and kwargs["error_message"] == expected_err


# ── Local compact contract ───────────────────────────────────────────────────


def _write_sample_parquet(path: str, rows: int, ts_start: int = 0, rid_start: int | None = None) -> None:
    cols = {
        "timestamp": pa.array(range(ts_start, ts_start + rows), type=pa.int64()),
        "ip": pa.array([f"192.168.1.{i % 254 + 1}" for i in range(rows)]),
        "status": pa.array([200 if i % 10 != 0 else 500 for i in range(rows)], type=pa.int32()),
    }
    if rid_start is not None:
        cols["rid"] = pa.array([f"rid_{rid_start + i}" for i in range(rows)])
    pq.write_table(pa.table(cols), path, compression="zstd")


@pytest.fixture
def mock_service_env(tmp_path, monkeypatch):
    service_id = "test-compaction-svc"
    cache_root = tmp_path / "cache" / service_id
    data_dir = cache_root / "data"
    data_dir.mkdir(parents=True)
    src = {
        "name": service_id,
        "service_id": service_id,
        "bucket": service_id,
        "_test_cache_root": str(cache_root),
        "provisioning": {"access_level": "read_write", "cron_compact": {"enabled": True}},
    }
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda s: s.get("_test_cache_root", str(cache_root)))
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src if sid == service_id else None)
    monkeypatch.setattr("backend.deps.get_service_id", lambda request=None: service_id)
    monkeypatch.setattr("backend.deps.get_source", lambda service_id=None: src)
    monkeypatch.setattr("backend.core.local_compaction._DAILY_TIER_AGE_DAYS", 365)
    monkeypatch.setattr("backend.core.local_compaction._WEEKLY_TIER_AGE_DAYS", 365)
    return {"service_id": service_id, "source": src, "data_dir": str(data_dir), "cache_root": str(cache_root)}


class TestLocalCompactContract:
    def test_contract_consolidate_10_small_files_via_admin_endpoint(self, mock_service_env):
        service_id = mock_service_env["service_id"]
        part_dir = os.path.join(mock_service_env["data_dir"], "timestamp_hour=2026-05-30-00")
        os.makedirs(part_dir, exist_ok=True)
        for i in range(10):
            _write_sample_parquet(os.path.join(part_dir, f"batch_{i:02d}.parquet"), 15, i * 100, i * 100)

        resp = TestClient(app).post(f"/api/admin/compact/{service_id}?min_files=3")
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert (data["files_merged"], data["files_removed"], data["partitions_compacted"], data["errors"]) == (
            10,
            10,
            1,
            [],
        )

        remaining = [f for f in os.listdir(part_dir) if f.endswith(".parquet")]
        assert len(remaining) == 1 and remaining[0].startswith("compacted_")
        assert [f for f in os.listdir(part_dir) if f.endswith(".tmp")] == []

        with duckdb.connect() as con:
            compacted_path = os.path.join(part_dir, remaining[0])
            assert con.execute(f"SELECT COUNT(*) FROM read_parquet('{compacted_path}')").fetchone()[0] == 150

    def test_contract_zero_outbound_fos_calls(self, mock_service_env, monkeypatch):
        src, service_id = mock_service_env["source"], mock_service_env["service_id"]
        part_dir = os.path.join(mock_service_env["data_dir"], "timestamp_hour=2026-05-30-00")
        os.makedirs(part_dir, exist_ok=True)
        for i in range(4):
            _write_sample_parquet(os.path.join(part_dir, f"b_{i}.parquet"), rows=10, ts_start=i * 10)

        monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
        monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value="test_run_123"))
        with patch("urllib.request.urlopen", side_effect=AssertionError("HTTP request made")):
            res = lc.compact_local_partitions(src)
            assert res["partitions_compacted"] == 1 and res["files_merged"] == 4
            compaction._run_local_compact.__wrapped__(service_id)

    def test_contract_atomic_replacement_and_crash_safety(self, mock_service_env, monkeypatch):
        src = mock_service_env["source"]
        part_dir = os.path.join(mock_service_env["data_dir"], "timestamp_hour=2026-05-30-00")
        os.makedirs(part_dir, exist_ok=True)
        for i in range(5):
            _write_sample_parquet(os.path.join(part_dir, f"file_{i}.parquet"), rows=10, ts_start=i * 10)

        call_order: list[str] = []
        real_rename, real_remove = os.rename, os.remove
        monkeypatch.setattr(
            os,
            "rename",
            lambda s, d: call_order.append(f"rename:{os.path.basename(s)}->{os.path.basename(d)}") or real_rename(s, d),
        )
        monkeypatch.setattr(
            os, "remove", lambda p: call_order.append(f"remove:{os.path.basename(p)}") or real_remove(p)
        )

        lc.compact_local_partitions(src)
        assert len(call_order) == 6 and call_order[0].startswith("rename:compacted_")
        assert all(a.startswith("remove:file_") for a in call_order[1:])

        part_dir_crash = os.path.join(mock_service_env["data_dir"], "timestamp_hour=2026-05-30-01")
        os.makedirs(part_dir_crash, exist_ok=True)
        crash_paths = [os.path.join(part_dir_crash, f"crash_{i}.parquet") for i in range(4)]
        for i, fp in enumerate(crash_paths):
            _write_sample_parquet(fp, rows=10, ts_start=i * 10)

        monkeypatch.setattr(os, "rename", MagicMock(side_effect=OSError("Simulated I/O error")))
        res = lc.compact_local_partitions(src)
        assert res["errors"] and all(os.path.exists(fp) for fp in crash_paths)

    def test_contract_concurrent_queries_during_compaction_no_filenotfound(self, mock_service_env):
        src = mock_service_env["source"]
        part_dir = os.path.join(mock_service_env["data_dir"], "timestamp_hour=2026-05-30-00")
        os.makedirs(part_dir, exist_ok=True)
        for i in range(8):
            _write_sample_parquet(os.path.join(part_dir, f"cq_{i}.parquet"), rows=50, ts_start=i * 50)

        stop_event = threading.Event()
        query_errors: list[Exception] = []
        query_counts: list[int] = []

        def reader_loop():
            with duckdb.connect() as con:
                runner = QueryRunner(con, src)
                glob_pattern = os.path.join(mock_service_env["data_dir"], "**", "*.parquet")
                while not stop_event.is_set():
                    try:
                        query_counts.append(
                            runner.execute(f"SELECT COUNT(*) FROM read_parquet('{glob_pattern}')").fetchone()[0]
                        )
                    except Exception as e:
                        query_errors.append(e)
                    time.sleep(0.005)

        with (
            patch("backend.core.iceberg.view.update_iceberg_view", lambda *a, **k: None),
            patch("backend.core.iceberg.update_iceberg_view", lambda *a, **k: None),
        ):
            thread = threading.Thread(target=reader_loop, daemon=True)
            thread.start()
            try:
                lc.compact_local_partitions(src)
            finally:
                stop_event.set()
                thread.join(timeout=3.0)

        assert not [e for e in query_errors if isinstance(e, FileNotFoundError) or "No such file" in str(e)]
        assert len(query_counts) > 0

    def test_contract_enospc_precheck_skips_safely(self, mock_service_env, monkeypatch):
        src = mock_service_env["source"]
        part_dir = os.path.join(mock_service_env["data_dir"], "timestamp_hour=2026-05-30-00")
        os.makedirs(part_dir, exist_ok=True)
        for i in range(5):
            _write_sample_parquet(os.path.join(part_dir, f"enospc_{i}.parquet"), rows=20)

        monkeypatch.setattr(shutil, "disk_usage", lambda p: Usage(total=10**9, used=5 * 10**8, free=100))
        res = lc.compact_local_partitions(src)
        assert res["partitions_compacted"] == 0 and any("ENOSPC" in err for err in res["errors"])
        assert len([f for f in os.listdir(part_dir) if f.endswith(".parquet")]) == 5

    def test_contract_scheduler_and_high_scale_ledger_recompute(self, monkeypatch):
        monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
        monkeypatch.setenv("LOCAL_COMPACT_INTERVAL_MIN", "5")
        cfg = {
            "service_id": "svc-contract-test",
            "provisioning": {"access_level": "read_write", "cron_compact": {"enabled": True}},
        }
        sched = Scheduler()
        with (
            patch("backend.config.list_configs", return_value=[cfg]),
            patch("backend.core.duckdb.get_source_for_service", return_value={"name": "svc-contract-test"}),
            patch("backend.core.duckdb.is_configured", return_value=True),
            patch.object(sched._sched, "add_job") as add_job,
        ):
            sched._register_dev_local_safe_jobs()

        assert "local_compact_svc-contract-test" in sched._job_ids
        lc_call = next(c for c in add_job.call_args_list if c.kwargs.get("id") == "local_compact_svc-contract-test")
        assert lc_call.kwargs.get("minutes") == 5

        service_id = "svc-high-scale"
        src = {
            "name": service_id,
            "service_id": service_id,
            "deployment_mode": "high_scale",
            "provisioning": {"access_level": "read_write"},
        }
        monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src)
        monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value="run_ht_123"))
        monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
        monkeypatch.setattr(
            "backend.core.local_compaction.compact_local_partitions",
            MagicMock(return_value={"partitions_compacted": 0, "files_merged": 0, "files_removed": 0, "errors": []}),
        )
        touched_hours = {"2026-10-01-12", "2026-10-01-13"}
        monkeypatch.setattr("backend.cron.jobs.compaction._ledger_touched_hours", lambda sid, lookback_s: touched_hours)
        recompute_mock = MagicMock()
        monkeypatch.setattr("backend.core.rollups.recompute.recompute_touched_hours", recompute_mock)

        compaction._run_local_compact.__wrapped__(service_id)
        recompute_mock.assert_called_once_with(service_id, src, touched_hours)


# ── Rollup compact contract ──────────────────────────────────────────────────


def _write_synthetic_hour_bundle(cache_root: str, hour: str, rows: list[dict]) -> tuple[str, str]:
    d = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    fields = pa.array([r["field"] for r in rows])
    vals = pa.array([r["value"] for r in rows])
    cnts = pa.array([r["count"] for r in rows], type=pa.int64())
    p, ip_p = os.path.join(d, "all_fields.parquet"), os.path.join(d, "all_fields_ip.parquet")
    pq.write_table(pa.table({"field": fields, "value": vals, "count": cnts}), p)
    pq.write_table(pa.table({"field": fields, "value": vals, "unique_ips": cnts}), ip_p)
    return p, ip_p


@pytest.fixture
def contract_service_source(tmp_path):
    cache_root = tmp_path / "cache-contract"
    cache_root.mkdir(parents=True, exist_ok=True)
    return {
        "name": "svc_rollup_contract",
        "service_id": "svc-rollup-contract-1",
        "storage_mode": "local",
        "bucket": "contract_test_bucket",
        "_cache_dir_override": str(cache_root),
        "cache_dir": str(cache_root),
    }


def test_rollup_compact_contract_full_lifecycle(contract_service_source, client, monkeypatch):
    src, sid, cache_root = (
        contract_service_source,
        contract_service_source["service_id"],
        contract_service_source["cache_dir"],
    )
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda s: cache_root)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s_id: src if s_id == sid else None)
    monkeypatch.setattr("backend.deps.get_source", lambda: src)
    monkeypatch.setattr(
        "backend.core.rollups.backfill_missing_hour_bundles", lambda *a, **k: {"missing": 0, "bundled": 0}
    )
    monkeypatch.setattr(
        "backend.core.rollups.backfill_missing_hour_ip_spread", lambda *a, **k: {"missing": 0, "rebuilt": 0}
    )

    target_day = (datetime.now(UTC) - timedelta(days=2)).strftime("%Y-%m-%d")
    hour_bundled_root = os.path.join(cache_root, "rollups", "hour_bundled")
    hourly_rows = [
        {"field": "ua", "value": "Mozilla/5.0", "count": 10},
        {"field": "status", "value": "200", "count": 50},
    ]
    for h in range(24):
        _write_synthetic_hour_bundle(cache_root, f"{target_day}-{h:02d}", hourly_rows)

    resp = client.post(f"/api/admin/rollups/compact/{sid}")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert (body["status"], body["service_id"], body["retired_files"]) == ("success", sid, 24)
    assert body["rebuilt"] >= 1 and body["bundled"] >= 1

    day_bundled_root = os.path.join(cache_root, "rollups", "day_bundled")
    flat_alias = os.path.join(day_bundled_root, f"day_bundle_{target_day}.parquet")
    for p in (
        os.path.join(day_bundled_root, f"day={target_day}", "all_fields.parquet"),
        os.path.join(day_bundled_root, f"day={target_day}", f"day_bundle_{target_day}.parquet"),
        flat_alias,
    ):
        assert os.path.isfile(p)

    with duckdb.connect() as con:
        assert (
            con.execute(
                f"SELECT sum(count) FROM read_parquet('{flat_alias}') WHERE field = 'ua' AND value = 'Mozilla/5.0'"
            ).fetchone()[0]
            == 240
        )
        assert (
            con.execute(
                f"SELECT sum(count) FROM read_parquet('{flat_alias}') WHERE field = 'status' AND value = '200'"
            ).fetchone()[0]
            == 1200
        )

    for h in range(24):
        hour_dir = os.path.join(hour_bundled_root, f"hour={target_day}-{h:02d}")
        assert not os.path.exists(os.path.join(hour_dir, "all_fields.parquet")) and os.path.isfile(
            os.path.join(hour_dir, "all_fields_ip.parquet")
        )

    runner = QueryRunner(duckdb.connect(":memory:"), src)
    t0 = time.perf_counter()
    phase_log: list[dict] = []
    next_day = (datetime.strptime(target_day, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    res_tuples, fields_ret = runner.execute_top_n_rollups(
        ["ua", "status"], start_time=f"{target_day}T00:00:00Z", end_time=f"{next_day}T00:00:00Z", _phase_log=phase_log
    )
    duration_s = time.perf_counter() - t0
    timings = {p["section"].replace("top_n_rollups:", ""): p["time_ms"] for p in phase_log}
    assert timings.get("dir_enum:n_bundled_day_files", 0) >= 1
    assert timings.get("dir_enum:n_bundled_hour_files", 0) == 0 and timings.get("dir_enum:n_hour_files", 0) == 0

    res = {f: [] for f in fields_ret}
    for field, val, count in res_tuples:
        res[field].append({"value": val, "count": count})
    assert (
        res["ua"][0] == {"value": "Mozilla/5.0", "count": 240}
        and res["status"][0] == {"value": "200", "count": 1200}
        and duration_s < 0.150
    )


def test_rollup_compact_gates_and_subsystems(contract_service_source, client, monkeypatch):
    src, sid, cache_root = (
        contract_service_source,
        contract_service_source["service_id"],
        contract_service_source["cache_dir"],
    )
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s_id: src if s_id == sid else None)
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda s: cache_root)
    monkeypatch.setattr("backend.deps.get_source", lambda: src)

    # 1. Politeness gate
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda task, s_id: True)
    res = _run_rollup_compact_daily.__wrapped__(sid, manual=False)
    assert res["status"] == "deferred" and "active" in res["summary"].lower()
    _mock_rollups(monkeypatch)
    assert _run_rollup_compact_daily.__wrapped__(sid, manual=True)["status"] == "success"

    # 2. Zero FOS egress
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda task, s_id: False)
    mock_fos = MagicMock(side_effect=AssertionError("FOS client invoked"))
    monkeypatch.setattr("backend.core.duckdb._get_fos_client", mock_fos)
    assert _run_rollup_compact_daily.__wrapped__(sid, manual=True)["status"] == "success"
    mock_fos.assert_not_called()

    # 3. Subsystem warnings
    monkeypatch.setattr("backend.core.rollups.compact_closed_days_to_daily", lambda *a, **k: 1)
    monkeypatch.setattr("backend.core.rollups.backfill_day_bundles", lambda *a, **k: 1)
    monkeypatch.setattr("backend.core.rollups.retire_compacted_hour_bundles", lambda *a, **k: (24, 1024))
    monkeypatch.setattr(
        "backend.core.rollups.compact_origin_summary_closed_days_to_daily",
        MagicMock(side_effect=RuntimeError("origin summary disk timeout")),
    )
    monkeypatch.setattr(
        "backend.core.rollups.compact_security_dims_closed_days_to_daily",
        MagicMock(side_effect=RuntimeError("security dims binder error")),
    )
    logged_runs: list = []
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **k: logged_runs.append((a, k)))
    res_warn = _run_rollup_compact_daily.__wrapped__(sid, manual=True)
    assert res_warn["status"] == "warning" and all(
        any(k in e for e in res_warn["errors"]) for k in ("origin_summary", "security_dims")
    )
    assert logged_runs[-1][0][3] == "warning" and "origin_summary" in logged_runs[-1][1].get("error_message", "")

    # 4. ENOSPC disk safety check
    monkeypatch.setattr(
        shutil, "disk_usage", lambda path: Usage(total=10**10, used=10**10 - 20 * 1024 * 1024, free=20 * 1024 * 1024)
    )
    res_enospc = _run_rollup_compact_daily.__wrapped__(sid, manual=True)
    assert res_enospc["status"] == "warning" and "critically low" in res_enospc["summary"].lower()

    # 5. Admin status endpoint
    day_dir = os.path.join(cache_root, "rollups", "day_bundled", "day=2026-06-01")
    os.makedirs(day_dir, exist_ok=True)
    open(os.path.join(day_dir, "all_fields.parquet"), "wb").close()
    for h in ("2026-06-02-10", "2026-06-02-11"):
        os.makedirs(os.path.join(cache_root, "rollups", "hour_bundled", f"hour={h}"), exist_ok=True)
    data = client.get(f"/api/admin/rollups/status/{sid}").json()
    assert (data["service_id"], data["days_bundled"], data["hour_bundles"], data["latest_day_bundle"]) == (
        sid,
        1,
        2,
        "2026-06-01",
    )


def test_call_with_lookback_does_not_rerun_unbounded_on_inner_typeerror():
    calls: list[dict] = []

    def _fn(*args, lookback_days=None):
        calls.append({"lookback_days": lookback_days})
        raise TypeError("bad value for lookback_days")

    with pytest.raises(TypeError):
        _call_with_lookback(_fn, "svc", {}, lookback_days=30)
    assert calls == [{"lookback_days": 30}]


# ── Rollup heal contract ─────────────────────────────────────────────────────


class TestRollupHealContract:
    @pytest.fixture
    def contract_service_source(self, tmp_path):
        cache_root = tmp_path / "cache-rollup-heal-contract"
        cache_root.mkdir(parents=True, exist_ok=True)
        return {
            "name": "svc_rollup_heal_contract",
            "service_id": "svc-rollup-heal-contract-1",
            "storage_mode": "local",
            "bucket": "contract_rollup_heal_bucket",
            "_cache_dir_override": str(cache_root),
            "cache_dir": str(cache_root),
        }

    @staticmethod
    def _setup_heal(monkeypatch, src, *, run_id=101, heal_return=None, heal_side_effect=None):
        sid = src["service_id"]
        monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s_id: src if s_id == sid else None)
        monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=run_id))
        logged_runs = []
        monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **kw: logged_runs.append((a, kw)))
        default_ret = {"missing": 1, "rebuilt_fields": 8, "bundled": 1, "stamped_empty": 0, "coverage_verified": True}
        heal_mock = (
            MagicMock(side_effect=heal_side_effect)
            if heal_side_effect is not None
            else MagicMock(return_value=heal_return or default_ret)
        )
        monkeypatch.setattr("backend.core.rollups.backfill_missing_hour_bundles", heal_mock)
        return sid, heal_mock, logged_runs

    def test_scheduler_registration_and_schedule_mapping(self, frozen_clock):
        assert TASK_MAP.get("rollup_heal") == "rollup_hour_heal"
        sched = Scheduler()
        mock_add = MagicMock()
        sched._add_job = mock_add
        cfg = {
            "service_id": "svc-dev-test",
            "name": "Dev Test Svc",
            "fastly": {"service_id": "svc-dev-test"},
            "provisioning": {"access_level": "read_write"},
        }
        expected_startup = datetime.now(UTC) + timedelta(seconds=5)

        for method_name, patch_target, patch_val in (
            ("_register_dev_local_safe_jobs", "backend.cron.scheduler.dev_local_crons_enabled", True),
            ("_sync_jobs", "backend.cron.scheduler.dev_mode_no_crons", False),
        ):
            mock_add.reset_mock()
            sched._job_ids.clear()
            with (
                patch("backend.config.list_configs", return_value=[cfg]),
                patch("backend.core.duckdb.get_source_for_service", return_value=dict(cfg)),
                patch("backend.core.duckdb.is_configured", return_value=True),
                patch(patch_target, return_value=patch_val),
            ):
                getattr(sched, method_name)()

            calls = [c for c in mock_add.call_args_list if c.kwargs.get("id") == "rollup_heal_svc-dev-test"]
            assert len(calls) == 1
            call = calls[0]
            assert call.args == (_run_rollup_hour_heal, "cron") and call.kwargs.get("minute") == 5
            assert call.kwargs.get("args") == ["svc-dev-test"] and call.kwargs.get("next_run_time") == expected_startup

    def test_rollup_heal_lookback_sentinels_politeness_enospc_egress_and_errors(
        self, contract_service_source, monkeypatch
    ):
        # 1. 48h lookback & sentinels
        sid, heal_mock, logged_runs = self._setup_heal(
            monkeypatch,
            contract_service_source,
            heal_return={
                "missing": 3,
                "rebuilt_fields": 12,
                "bundled": 3,
                "stamped_empty": 1,
                "coverage_verified": True,
            },
        )
        res = _run_rollup_hour_heal.__wrapped__(sid)
        assert heal_mock.call_args.kwargs.get("lookback_days") == 2 and res["status"] == "success"
        assert (res["missing"], res["rebuilt_fields"], res["bundled"], res["stamped_empty"]) == (3, 12, 3, 1)
        assert (
            "Healed 3 missing hour(s)" in logged_runs[0][1]["summary"]
            and "1 empty hour(s) stamped" in logged_runs[0][1]["summary"]
        )

        # 2. Empty hour sentinels only
        sid, _, logged2 = self._setup_heal(
            monkeypatch,
            contract_service_source,
            heal_return={
                "missing": 0,
                "rebuilt_fields": 0,
                "bundled": 0,
                "stamped_empty": 4,
                "coverage_verified": True,
            },
        )
        res_empty = _run_rollup_hour_heal.__wrapped__(sid)
        assert res_empty["stamped_empty"] == 4 and "4 empty hour(s) stamped" in logged2[0][1]["summary"]

        # 3. Politeness gate & manual bypass
        sid, heal_mock, _ = self._setup_heal(
            monkeypatch, contract_service_source, heal_return={"missing": 0, "rebuilt_fields": 0, "bundled": 0}
        )
        monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda task, s_id: True)
        assert _run_rollup_hour_heal.__wrapped__(sid, manual=False)["status"] == "deferred"
        heal_mock.assert_not_called()
        assert _run_rollup_hour_heal.__wrapped__(sid, manual=True)["status"] == "success"
        monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda task, s_id: False)

        # 4. Zero FOS egress
        boto_mock = MagicMock(side_effect=AssertionError("Outbound FOS network call forbidden"))
        monkeypatch.setattr("boto3.client", boto_mock)
        assert _run_rollup_hour_heal.__wrapped__(sid)["status"] == "success"
        boto_mock.assert_not_called()

        # 5. Error handling
        sid, _, logged_err = self._setup_heal(
            monkeypatch, contract_service_source, heal_side_effect=RuntimeError("DuckDB connection pool exhausted")
        )
        res_err = _run_rollup_hour_heal.__wrapped__(sid)
        assert res_err["status"] == "error" and "DuckDB connection pool exhausted" in logged_err[0][1]["error_message"]

        # 6. ENOSPC safety
        sid, heal_mock, logged_disk = self._setup_heal(monkeypatch, contract_service_source)
        monkeypatch.setattr("shutil.disk_usage", lambda path: MagicMock(free=10 * 1024 * 1024))
        res_disk = _run_rollup_hour_heal.__wrapped__(sid)
        assert res_disk["status"] == "warning" and "Skipped due to low disk space" in logged_disk[0][1]["summary"]
        heal_mock.assert_not_called()

    def test_rollup_heal_durable_mode_throttle_and_readiness_guards(self, contract_service_source, monkeypatch):
        sid, heal_partial, _ = self._setup_heal(
            monkeypatch,
            contract_service_source,
            heal_return={"missing": 1, "rebuilt_fields": 5, "bundled": 1, "coverage_verified": False},
        )
        monkeypatch.setattr("backend.config.is_durable_serving_mode", lambda s: True)
        monkeypatch.setattr("backend.core.rollups._common.rollup_coverage_ready", lambda s_id: False)
        mark_ready_mock = MagicMock()
        monkeypatch.setattr("backend.core.rollups._common.mark_rollup_coverage_ready", mark_ready_mock)

        assert _run_rollup_hour_heal.__wrapped__(sid)["status"] == "success"
        assert heal_partial.call_args.kwargs.get("max_missing_hours") == 1
        mark_ready_mock.assert_not_called()

        # Non-durable mode does not mark ready
        monkeypatch.setattr("backend.config.is_durable_serving_mode", lambda s: False)
        _run_rollup_hour_heal.__wrapped__(sid)
        mark_ready_mock.assert_not_called()

        # Failure in durable mode does not mark ready
        monkeypatch.setattr("backend.config.is_durable_serving_mode", lambda s: True)
        self._setup_heal(monkeypatch, contract_service_source, heal_side_effect=RuntimeError("view refresh failed"))
        _run_rollup_hour_heal.__wrapped__(sid)
        mark_ready_mock.assert_not_called()

        # Verified coverage in durable mode marks ready
        monkeypatch.setattr(
            "backend.core.rollups.backfill_missing_hour_bundles",
            MagicMock(return_value={"missing": 0, "rebuilt_fields": 0, "bundled": 0, "coverage_verified": True}),
        )
        assert _run_rollup_hour_heal.__wrapped__(sid)["status"] == "success"
        mark_ready_mock.assert_called_once_with(sid)

    def test_admin_trigger_and_inspection_parity(self, contract_service_source, monkeypatch):
        from backend.deps import get_source

        src = contract_service_source
        sid, _, _ = self._setup_heal(
            monkeypatch,
            src,
            heal_return={
                "missing": 2,
                "rebuilt_fields": 10,
                "bundled": 2,
                "stamped_empty": 0,
                "coverage_verified": True,
            },
        )
        app.dependency_overrides[get_source] = lambda: src
        client = TestClient(app)
        try:
            data1 = client.post(f"/api/admin/rollup-heal/{sid}").json()
            assert (data1["status"], data1["service_id"], data1["missing"], data1["bundled"]) == ("success", sid, 2, 2)
            assert client.post("/api/admin/rollup-heal", headers={"x-service-id": sid}).json()["status"] == "success"
            assert client.post(f"/api/admin/rollups/heal/{sid}").json()["status"] == "success"

            import backend.core.rollups as rollups_pkg

            for fn_name in dir(rollups_pkg):
                if fn_name.startswith(("backfill_", "compact_")) and callable(getattr(rollups_pkg, fn_name)):
                    monkeypatch.setattr(f"backend.core.rollups.{fn_name}", lambda *a, **k: 0)
            monkeypatch.setattr("backend.core.rollups._common.backfill_missing_bundles", lambda *a, **k: 0)
            monkeypatch.setattr("backend.core.duckdb.get_connection", lambda *a, **k: MagicMock())
            monkeypatch.setattr("backend.core.rollups.rum.recompute_rum_aggregates", lambda *a, **k: None)
            monkeypatch.setattr("backend.core.rollups.rum.table_exists", lambda *a, **k: False)

            data4 = client.post("/api/admin/backfill-bundle-rollups", headers={"x-service-id": sid}).json()
            assert "slow_urls" in data4 and "origin_summary" in data4
        finally:
            app.dependency_overrides.pop(get_source, None)


# ── Partial-hour merge cron & contract tests ─────────────────────────────────


@contextlib.contextmanager
def _partial_hour_merge_ctx(monkeypatch, fields: list[str], *, new_files: int = 1, duration_ms: float = 1.0):
    fake_src = {"service_id": "svc-a", "name": "svc-a"}
    monkeypatch.setattr("backend.repositories.dashboard.FIELDS", fields)
    with (
        patch("backend.utils.active_requests.should_defer_cron", return_value=False),
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.core.duckdb.start_cron_run", return_value="run-1"),
        patch("backend.core.duckdb.log_cron_run") as mock_log,
        patch("backend.cron_progress.cleanup_progress_and_reap"),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron_progress.end_progress"),
        patch(
            "backend.core.rollups.partial_hour.merge_partial_hour",
            return_value={"hour": "2026-09-09-14", "new_files": new_files, "duration_ms": duration_ms},
        ) as mock_merge,
        patch("backend.core.rollups.partial_hour.gc_stale_partial_hours", return_value=0),
        patch("backend.core.rollups._common.mark_rollup_coverage_ready") as mock_mark,
        patch("backend.core.metadata.delete_cron_run") as mock_delete,
    ):
        yield {"merge": mock_merge, "log": mock_log, "mark": mock_mark, "delete": mock_delete}


@pytest.mark.parametrize(
    "extra_excluded",
    [
        [next(iter(_LIVE_TOPN_SKIP_FIELDS))],
        ["requests", "hit_rate"],
        ["_bot_name", "waf_sig_ind"],
    ],
)
def test_run_partial_hour_merge_filters_excluded_fields(monkeypatch, extra_excluded):
    with _partial_hour_merge_ctx(monkeypatch, ["country", *extra_excluded], new_files=3, duration_ms=12.5) as mocks:
        _run_partial_hour_merge("svc-a")
    merged_fields = mocks["merge"].call_args[0][2]
    assert "country" in merged_fields and all(f not in merged_fields for f in extra_excluded)
    mocks["mark"].assert_not_called()
    mocks["log"].assert_called_once()


@pytest.mark.parametrize("new_files", [0, 2])
def test_run_partial_hour_merge_keeps_cron_runs_row_and_defers(monkeypatch, new_files):
    with _partial_hour_merge_ctx(monkeypatch, ["country"], new_files=new_files) as mocks:
        _run_partial_hour_merge("svc-a")
    mocks["log"].assert_called_once()
    mocks["delete"].assert_not_called()

    with (
        patch("backend.utils.active_requests.should_defer_cron", return_value=True),
        patch("backend.core.duckdb.start_cron_run") as mock_start,
    ):
        _run_partial_hour_merge("svc-a")
    mock_start.assert_not_called()


def _write_active_hour_parquet(path: str, rows: int, dt_start: datetime) -> None:
    timestamps = [dt_start + timedelta(seconds=i) for i in range(rows)]
    table = pa.table(
        {
            "timestamp": pa.array(timestamps, type=pa.timestamp("us", tz="UTC")),
            "status": pa.array([200 if i % 5 != 0 else 500 for i in range(rows)], type=pa.int32()),
            "country": pa.array(["US" if i % 2 == 0 else "CA" for i in range(rows)], type=pa.string()),
            "service_id": pa.array(["test-ph-svc"] * rows, type=pa.string()),
        }
    )
    pq.write_table(table, path, compression="zstd")


def _active_hour_bounds() -> tuple[datetime, str, datetime]:
    now = datetime.now(UTC)
    return now, now.strftime("%Y-%m-%d-%H"), now.replace(minute=0, second=0, microsecond=0)


@pytest.fixture
def mock_ph_env(tmp_path, monkeypatch):
    service_id = "test-ph-svc"
    cache_root = tmp_path / "cache" / service_id
    buffer_dir = cache_root / "buffer"
    buffer_dir.mkdir(parents=True)
    rollups_dir = cache_root / "rollups" / "partial_hour"
    rollups_dir.mkdir(parents=True)
    src = {
        "name": service_id,
        "service_id": service_id,
        "bucket": service_id,
        "_test_cache_root": str(cache_root),
        "provisioning": {"access_level": "read_write"},
    }
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda s: s.get("_test_cache_root", str(cache_root)))
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src if sid == service_id else None)
    monkeypatch.setattr("backend.deps.get_service_id", lambda request=None: service_id)
    monkeypatch.setattr("backend.deps.get_source", lambda service_id=None: src)
    return {
        "service_id": service_id,
        "source": src,
        "cache_root": str(cache_root),
        "buffer_dir": str(buffer_dir),
        "rollups_dir": str(rollups_dir),
    }


def test_contract_incremental_folding_and_consecutive_ticks(mock_ph_env):
    src, service_id, buffer_dir = mock_ph_env["source"], mock_ph_env["service_id"], mock_ph_env["buffer_dir"]
    _, active_hour, hour_start = _active_hour_bounds()

    f1 = os.path.join(buffer_dir, "batch_1.parquet")
    _write_active_hour_parquet(f1, rows=40, dt_start=hour_start + timedelta(minutes=1))
    stats1 = ph.merge_partial_hour(service_id, src, ["status", "country"])
    assert (stats1["new_files"], stats1["hour"], ph.read_partial_hour_total(src, active_hour)) == (1, active_hour, 40)
    assert os.path.isfile(ph._all_fields_path(src, active_hour))
    fields = {r[0] for r in ph.read_partial_hour_all_fields(src, active_hour)}
    assert "status" in fields and "country" in fields and ph.TOTAL_FIELD not in fields
    assert ph.read_partial_hour_watermark(src, active_hour) > 0.0

    stats2 = ph.merge_partial_hour(service_id, src, ["status", "country"])
    assert stats2["new_files"] == 0 and ph.read_partial_hour_total(src, active_hour) == 40

    time.sleep(0.05)
    f2 = os.path.join(buffer_dir, "batch_2.parquet")
    _write_active_hour_parquet(f2, rows=60, dt_start=hour_start + timedelta(minutes=2))
    os.utime(f2, (time.time() + 10, time.time() + 10))
    stats3 = ph.merge_partial_hour(service_id, src, ["status", "country"])
    assert stats3["new_files"] == 1 and ph.read_partial_hour_total(src, active_hour) == 100


def test_contract_admin_endpoints_and_query_runner_narrowing(mock_ph_env):
    src, service_id, buffer_dir = mock_ph_env["source"], mock_ph_env["service_id"], mock_ph_env["buffer_dir"]
    now, active_hour, hour_start = _active_hour_bounds()
    _write_active_hour_parquet(
        os.path.join(buffer_dir, "buf_admin.parquet"), rows=25, dt_start=hour_start + timedelta(minutes=2)
    )

    client = TestClient(app)
    data = client.post(f"/api/admin/partial-hour-merge/{service_id}").json()
    assert (data["service_id"], data["new_files"], data["status"]) == (service_id, 1, "success")

    status_data = client.get(f"/api/admin/partial-hour-status/{service_id}").json()
    assert (status_data["service_id"], status_data["file_exists"], status_data["total_rows"]) == (service_id, True, 25)
    assert status_data["watermark"] > 0

    with duckdb.connect() as con:
        runner = QueryRunner(con, src)
        adjusted_start, partial_rows, partial_total = runner._partial_hour_adjusted_live_start(
            hour_start, active_hour, now + timedelta(minutes=5)
        )
        assert partial_total == 25 and len(partial_rows) > 0 and adjusted_start > hour_start


def test_contract_fos_atomic_corrupt_and_high_scale(mock_ph_env, monkeypatch):
    src, service_id, buffer_dir = mock_ph_env["source"], mock_ph_env["service_id"], mock_ph_env["buffer_dir"]
    _, active_hour, hour_start = _active_hour_bounds()
    f1 = os.path.join(buffer_dir, "b1.parquet")
    _write_active_hour_parquet(f1, rows=20, dt_start=hour_start + timedelta(minutes=1))

    # Zero FOS calls
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value="run_ph_fos"))
    with patch("urllib.request.urlopen", side_effect=AssertionError("Outbound network egress")):
        assert ph.merge_partial_hour(service_id, src, ["status"])["new_files"] == 1
        compaction._run_partial_hour_merge.__wrapped__(service_id)

    # Atomic crash safety
    ph_dir = ph.partial_hour_dir(src, active_hour)
    assert [f for f in os.listdir(ph_dir) if f.startswith(".tmp_")] == []
    f2 = os.path.join(buffer_dir, "b2.parquet")
    _write_active_hour_parquet(f2, rows=30, dt_start=hour_start + timedelta(minutes=2))
    os.utime(f2, (time.time() + 10, time.time() + 10))
    with patch("os.replace", side_effect=OSError("disk error")), pytest.raises(OSError):
        ph.merge_partial_hour(service_id, src, ["status"])
    assert [f for f in os.listdir(ph_dir) if f.startswith(".tmp_")] == []
    assert ph.read_partial_hour_total(src, active_hour) == 20

    # Corrupt file self-healing
    with open(ph._all_fields_path(src, active_hour), "wb") as f:
        f.write(b"NOT_A_VALID_PARQUET_FILE_GARBAGE")
    stats = ph.merge_partial_hour(service_id, src, ["status"])
    assert stats["new_files"] == 2 and ph.read_partial_hour_total(src, active_hour) == 50

    # High-scale mode noop
    src["deployment_mode"] = "high_scale"
    hs_stats = ph.merge_partial_hour(service_id, src, ["status"])
    assert hs_stats["new_files"] == 0 and hs_stats["duration_ms"] < 200.0


def test_contract_concurrent_reader_resilience_and_scheduler(mock_ph_env, monkeypatch):
    src, service_id, buffer_dir = mock_ph_env["source"], mock_ph_env["service_id"], mock_ph_env["buffer_dir"]
    _, active_hour, hour_start = _active_hour_bounds()
    for i in range(5):
        _write_active_hour_parquet(
            os.path.join(buffer_dir, f"conc_{i}.parquet"), rows=15, dt_start=hour_start + timedelta(minutes=i)
        )

    stop_event = threading.Event()
    reader_errors: list[Exception] = []
    read_counts: list[int] = []

    def reader_loop():
        with duckdb.connect() as con:
            while not stop_event.is_set():
                try:
                    read_counts.append(ph.read_partial_hour_total(src, active_hour, con=con))
                    ph.read_partial_hour_all_fields(src, active_hour, con=con)
                except Exception as e:
                    reader_errors.append(e)
                time.sleep(0.005)

    thread = threading.Thread(target=reader_loop, daemon=True)
    thread.start()
    try:
        for _ in range(3):
            ph.merge_partial_hour(service_id, src, ["status", "country"])
            time.sleep(0.01)
    finally:
        stop_event.set()
        thread.join(timeout=3.0)
    assert not reader_errors and len(read_counts) > 0

    for env_val, expected in [("45", 45), ("5", 10), ("200", 120)]:
        monkeypatch.setenv("PARTIAL_HOUR_MERGE_INTERVAL_SEC", env_val)
        assert partial_hour_merge_interval_sec() == expected

    monkeypatch.setenv("PARTIAL_HOUR_MERGE_INTERVAL_SEC", "40")
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    cfg = {"service_id": "svc-ph-sched", "provisioning": {"access_level": "read_write"}}
    sched = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "svc-ph-sched"}),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch.object(sched._sched, "add_job") as add_job,
    ):
        sched._register_dev_local_safe_jobs()
    ph_call = next(c for c in add_job.call_args_list if c.kwargs.get("id") == "partial_hour_merge_svc-ph-sched")
    assert ph_call.kwargs.get("seconds") == 40
