"""Consolidated tests for test_metadata_jobs."""

from __future__ import annotations

import json
import time
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from starlette.testclient import TestClient
from tests._helpers.config_fakes import fake_mutate_config

from backend.core import metadata as metadata_db
from backend.core import share_db
from backend.core.metadata import reconciliation, usage_log_db
from backend.core.metadata.cron_log import CronLeaseHeldError
from backend.cron.jobs import metadata as metadata_job
from backend.cron.jobs.metadata import (
    _run_bot_data_refresh,
    _run_metadata_cleanup,
    _run_ngwaf_bot_sync,
    _run_rdns_enrichment,
    _run_share_audit_purge,
)
from backend.cron.scheduler import Scheduler
from backend.deps import get_service_id, get_source, require_admin
from backend.main import app
from backend.utils import rdns_cache
from backend.utils.date_utils import iso_z
from tests.cron.conftest import _StubSource

metadata = metadata_job


def _collect_sse_events(body: bytes) -> list[dict]:
    events: list[dict] = []
    for line in body.decode("utf-8", errors="replace").splitlines():
        if line.startswith("data: "):
            try:
                events.append(json.loads(line[len("data: ") :]))
            except json.JSONDecodeError:
                continue
    return events


def _seed_usage_log(service_id: str, rows: int, days_ago: int = 0) -> None:
    con = usage_log_db.get_con(service_id)
    now_ts = iso_z(datetime.now(UTC) - timedelta(days=days_ago))
    con.executemany(
        "INSERT INTO usage_log (timestamp, service_id, operation_class, operation_type, bytes, count) "
        "VALUES (?, ?, 'A', 'PUT_OBJECT', 0, 1)",
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
        "INSERT INTO ingested_files (file_name, source_name, table_name, ingested_at, row_count, file_size_bytes) "
        f"VALUES (?, 'fos', ?, datetime('now', '-{days_ago} days'), 1, 100)",
        [(f"{prefix}/{days_ago}d-{table_name}-{i}.gz", table_name) for i in range(rows)],
    )
    con.commit()


def _seed_cron_runs(service_id: str, rows: int, days_ago: int = 0) -> None:
    con = metadata_db.get_con(service_id)
    con.executemany(
        "INSERT INTO cron_runs (task, started_at, duration_s, status, parquet_keys) "
        f"VALUES ('sync', datetime('now', '-{days_ago} days'), 1.0, 'success', '[]')",
        [() for _ in range(rows)],
    )
    con.commit()


def _seed_slow_queries(service_id: str, rows: int, days_ago: int = 0) -> list[int]:
    con = metadata_db.get_con(service_id)
    cutoff = time.time() - (days_ago * 86400)
    ids = [
        con.execute(
            """INSERT INTO slow_queries (
                query_id, db_type, service_id, started_at_utc, ended_at_utc, duration_ms,
                outcome, sql_preview, sql_len, attr_kind, attr_label, attr_caller_qualname, attr_caller_file
            ) VALUES (1, 'duckdb', ?, ?, ?, 100.0, 'success', 'SELECT 1', 8, 'api', 'test', 'test_caller', 'test.py')
            RETURNING id""",
            (service_id, cutoff, cutoff + 0.1),
        ).fetchone()[0]
        for _ in range(rows)
    ]
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


def test_contract_1_dev_mode_no_crons_skips_execution(monkeypatch, mc_contract_source):
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    start_cron, cleanup_mock = MagicMock(), MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start_cron)
    monkeypatch.setattr("backend.core.metadata.cleanup_metadata", cleanup_mock)

    metadata_job._run_metadata_cleanup(mc_contract_source["service_id"])
    start_cron.assert_not_called()
    cleanup_mock.assert_not_called()


def test_contract_1_dev_mode_no_crons_skips_scheduler_registration(monkeypatch, mc_contract_source):
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    s = Scheduler()
    s._sched = MagicMock()
    _sync_scheduler_with(s, mc_contract_source, mc_contract_source)
    for _, kwargs in s._sched.add_job.call_args_list:
        assert not kwargs.get("id", "").startswith("metadata_cleanup_")


def test_contract_2_politeness_gating_defers_execution(monkeypatch, mc_contract_source):
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", MagicMock(return_value=True))
    start_cron, cleanup_mock = MagicMock(), MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start_cron)
    monkeypatch.setattr("backend.core.metadata.cleanup_metadata", cleanup_mock)

    metadata_job._run_metadata_cleanup(mc_contract_source["service_id"])
    start_cron.assert_not_called()
    cleanup_mock.assert_not_called()


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


def test_contract_3_zero_fos_calls_contract(monkeypatch, mc_contract_source):
    boto3_mock = MagicMock()
    monkeypatch.setattr("boto3.client", boto3_mock)
    monkeypatch.setattr("boto3.resource", boto3_mock)
    _patch_cleanup_progress_mocks(monkeypatch, run_id=999)

    metadata_job._run_metadata_cleanup(mc_contract_source["service_id"])
    boto3_mock.assert_not_called()


def test_contract_4_dedup_suppression_when_delete_after_false(mc_contract_source):
    sid = mc_contract_source["service_id"]
    mc_contract_source["provisioning"]["cron_sync"]["delete_after"] = False
    _seed_ingested_files(sid, 5, days_ago=20)
    events: list[dict] = []

    res = reconciliation.cleanup_metadata(sid, retention={"ingested_files_days": 7}, on_event=events.append)
    assert res["deleted"]["ingested_files"] == 0 and res["after"]["ingested_files"] == 5
    override_messages = [e["message"] for e in events if "cron_sync.delete_after=false" in e.get("message", "")]
    assert len(override_messages) == 1 and "ingested_files retention (7d) ignored" in override_messages[0]


def test_contract_5_rum_window_calculation(mc_contract_source):
    sid = mc_contract_source["service_id"]
    mc_contract_source["provisioning"]["cron_sync"]["delete_after"] = True
    mc_contract_source["log_retention_days"] = 90

    _seed_ingested_files(sid, 3, days_ago=10, table_name="logs", prefix="raw/request")
    _seed_ingested_files(sid, 2, days_ago=2, table_name="logs", prefix="raw/request")
    for tbl in ("client_vitals", "client_errors"):
        _seed_ingested_files(sid, 2, days_ago=400, table_name=tbl, prefix="raw/rum")
        _seed_ingested_files(sid, 1, days_ago=20, table_name=tbl, prefix="raw/rum")

    res = reconciliation.cleanup_metadata(sid, retention={"ingested_files_days": 7})
    assert res["deleted"]["ingested_files"] == 7 and res["after"]["ingested_files"] == 4
    summary_row = (
        metadata_db.get_con(sid)
        .execute("SELECT file_count FROM ingested_files_summary WHERE source_name = 'fos'")
        .fetchone()
    )
    if summary_row:
        assert summary_row[0] == 4


def test_contract_6_usage_log_chunked_deletion_preserves_hourly_summary(mc_contract_source):
    sid = mc_contract_source["service_id"]
    _seed_usage_log(sid, 20, days_ago=5)
    _seed_usage_log(sid, 10, days_ago=0)

    con = usage_log_db.get_con(sid)
    summary_before = con.execute(
        "SELECT sum(count) FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)
    ).fetchone()[0]
    assert summary_before == 30

    res = reconciliation.cleanup_metadata(sid, retention={"usage_log_days": 1})
    assert res["deleted"]["usage_log"] == 20 and res["after"]["usage_log"] == 10
    summary_after = con.execute(
        "SELECT sum(count) FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)
    ).fetchone()[0]
    assert summary_after == summary_before


def test_contract_7_cron_runs_and_slow_queries_retention(monkeypatch, mc_contract_source):
    sid = mc_contract_source["service_id"]
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
    remaining_slow_queries = con.execute(
        f"SELECT id FROM slow_queries WHERE service_id = ? AND id IN ({placeholders})", (sid, *seeded_ids)
    ).fetchall()
    assert {row[0] for row in remaining_slow_queries} == set(recent_ids)
    purge_mock.assert_called_once_with(retention_days=30)


def test_contract_8_telemetry_progress_and_audit_logging_success(monkeypatch, mc_contract_source):
    sid = mc_contract_source["service_id"]
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


def test_contract_8_duration_finalized_on_error(monkeypatch, mc_contract_source):
    sid = mc_contract_source["service_id"]
    _, end_prog, finalize_dur, log_cron, _ = _patch_cleanup_progress_mocks(monkeypatch, run_id=888)
    monkeypatch.setattr(
        "backend.core.metadata.cleanup_metadata", MagicMock(side_effect=RuntimeError("simulated metadata fail"))
    )

    metadata_job._run_metadata_cleanup(sid)
    end_prog.assert_called_once_with(888)
    finalize_dur.assert_called_once()
    args, kwargs = log_cron.call_args
    assert (args[1], args[3], kwargs["run_id"]) == ("metadata_cleanup", "error", 888)
    assert "simulated metadata fail" in kwargs["summary"]


def test_contract_9_manual_trigger_path_parity(mc_contract_source):
    sid = mc_contract_source["service_id"]
    fake_result = {"deleted": {"usage_log": 12, "cron_runs": 3}, "vacuumed": True}
    app.dependency_overrides[get_source] = lambda: mc_contract_source
    app.dependency_overrides[get_service_id] = lambda: sid
    client = TestClient(app)

    try:
        with (
            patch("backend.core.duckdb.start_cron_run", return_value=501),
            patch("backend.core.duckdb.log_cron_run"),
            patch("backend.core.metadata.cleanup_metadata", return_value=fake_result),
        ):
            for path in (f"/api/admin/metadata-cleanup/{sid}", "/api/admin/metadata-cleanup"):
                resp = client.post(path, headers={"x-fastly-service-id": sid})
                assert resp.status_code == 200 and resp.headers["content-type"].startswith("text/event-stream")
                events = _collect_sse_events(resp.content)
                assert any(e.get("type") == "done" and "Trimmed 15 rows" in e.get("message", "") for e in events)

            def reject_analyst():
                from fastapi import HTTPException

                raise HTTPException(status_code=403, detail={"error": "admin_only"})

            app.dependency_overrides[require_admin] = reject_analyst
            resp_denied = client.post(f"/api/admin/metadata-cleanup/{sid}", headers={"x-fastly-service-id": sid})
            assert resp_denied.status_code == 403
    finally:
        app.dependency_overrides.clear()


def test_contract_10_dynamic_scheduler_rescheduling_and_disable(mc_contract_source):
    s = Scheduler()
    mock_job = MagicMock()
    s._sched = MagicMock()
    s._sched.get_job = MagicMock(return_value=mock_job)
    job_id = f"metadata_cleanup_{mc_contract_source['service_id']}"
    s._job_ids[job_id] = job_id

    mc_contract_source["provisioning"]["cron_metadata_cleanup"] = {"cron_hour": 4, "cron_minute": 45}
    _sync_scheduler_with(s, mc_contract_source, mc_contract_source)
    mock_job.reschedule.assert_called_once_with("cron", hour=4, minute=45)

    mc_contract_source["provisioning"]["cron_metadata_cleanup"] = {"enabled": False}
    s2 = Scheduler()
    s2._sched = MagicMock()
    _sync_scheduler_with(s2, mc_contract_source, mc_contract_source)
    for _, kwargs in s2._sched.add_job.call_args_list:
        assert not kwargs.get("id", "").startswith("metadata_cleanup_")


def test_metadata_cleanup_lease_held_skips_without_row(monkeypatch, mc_contract_source):
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", MagicMock(return_value=False))
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(side_effect=CronLeaseHeldError("held")))
    log_cron, cleanup, end_prog = MagicMock(), MagicMock(), MagicMock()
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_cron)
    monkeypatch.setattr("backend.core.metadata.cleanup_metadata", cleanup)
    monkeypatch.setattr("backend.cron_progress.end_progress", end_prog)

    _run_metadata_cleanup(mc_contract_source["service_id"])
    log_cron.assert_not_called()
    cleanup.assert_not_called()
    end_prog.assert_not_called()


# ── Merged from test_sync_metadata_contract.py ──────────────────────────────────────────────


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
    prov: dict = {"access_level": access_level, "cron_sync": {"enabled": sync_enabled}}
    if sync_interval_seconds is not None:
        prov["cron_sync"]["interval_seconds"] = sync_interval_seconds
    cron_meta: dict = {"enabled": meta_enabled}
    if meta_interval_seconds is not None:
        cron_meta["interval_seconds"] = meta_interval_seconds
    prov["cron_metadata_sync"] = cron_meta
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
    if isinstance(init_ret, Exception):
        monkeypatch.setattr("backend.core.iceberg.init_iceberg_table", MagicMock(side_effect=init_ret))
    else:
        monkeypatch.setattr("backend.core.iceberg.init_iceberg_table", MagicMock(return_value=init_ret))
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

    fake_con = MagicMock()
    get_conn = MagicMock(return_value=fake_con)
    start_run = MagicMock(return_value=88)
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


def test_contract_role_gate_analyst_path_a_only():
    s = Scheduler()
    _sync_scheduler_with(
        s, _fake_cfg("svc-analyst-1", access_level="read_only"), _fake_src("svc-analyst-1", "read_only")
    )
    assert "sync_metadata_svc-analyst-1" in s._job_ids
    for prefix in ("log_discovery", "commit", "optimize", "expire"):
        assert f"{prefix}_svc-analyst-1" not in s._job_ids

    s._sched.remove_all_jobs()
    s._job_ids.clear()
    _sync_scheduler_with(s, _fake_cfg("svc-admin-1", access_level="read_write"), _fake_src("svc-admin-1", "read_write"))
    assert "sync_metadata_svc-admin-1" not in s._job_ids


def test_contract_role_gate_analyst_path_b_blocked():
    from backend.utils.remote_access import _ANALYST_BLOCKED_PREFIXES, _is_blocked_path

    assert any("/api/admin/" == p for p in _ANALYST_BLOCKED_PREFIXES)
    assert _is_blocked_path("/api/admin/rebuild-local-view") is True
    assert _is_blocked_path("/api/admin/rebuild-local-view?service_id=svc-1") is True


def test_contract_role_gate_admin_post_commit_dispatch():
    calls, dispatched_threads = [], []

    class DeferredThread:
        def __init__(self, *, target, args, **kwargs):
            self.target = target
            self.args = args
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


def test_contract_cadence_derivation_and_execution_flags():
    s = Scheduler()
    cases = [
        ("svc-cadence-60", 60, None, 30),
        ("svc-cadence-15", 15, None, 15),
        ("svc-cadence-custom", 60, 45, 45),
    ]
    for sid, log_period, custom_sec, expected_sec in cases:
        s._sched.remove_all_jobs()
        s._job_ids.clear()
        cfg = _fake_cfg(sid, access_level="read_only", log_period=log_period, meta_interval_seconds=custom_sec)
        _sync_scheduler_with(s, cfg, _fake_src(sid, "read_only"))
        job = s._sched.get_job(f"sync_metadata_{sid}")
        assert job is not None
        assert int(job.trigger.interval.total_seconds()) == expected_sec
        assert job.coalesce is True and job.misfire_grace_time == 60


def test_contract_startup_trigger_initial_sync():
    s = Scheduler()
    added_jobs = []
    s._add_job = lambda fn, *args, **kwargs: added_jobs.append({"fn": fn, "args": args, "kwargs": kwargs})
    s._sched = MagicMock()
    s._sync_jobs = MagicMock()

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
        s.start()

    job_ids = [j["kwargs"].get("id") for j in added_jobs]
    assert "initial_sync_svc-init-analyst" in job_ids
    assert "initial_sync_svc-init-admin" not in job_ids
    assert "initial_sync_svc-init-disabled" not in job_ids


def test_contract_dynamic_registration_gating_and_rescheduling():
    s = Scheduler()
    cfg_disabled = _fake_cfg("svc-meta-dis", access_level="read_only", meta_enabled=False)
    _sync_scheduler_with(s, cfg_disabled, _fake_src("svc-meta-dis", "read_only"))
    assert "sync_metadata_svc-meta-dis" not in s._job_ids

    mock_job = MagicMock()
    s._sched = MagicMock()
    s._sched.get_job = MagicMock(return_value=mock_job)
    s._job_ids["sync_metadata_svc-meta-resched"] = "sync_metadata_svc-meta-resched"

    cfg_resched = _fake_cfg("svc-meta-resched", access_level="read_only", log_period=60, meta_interval_seconds=45)
    _sync_scheduler_with(s, cfg_resched, _fake_src("svc-meta-resched", "read_only"))
    mock_job.reschedule.assert_called_once_with("interval", seconds=45)


def test_contract_politeness_deferral_on_scheduled_runs(base_context):
    with patch("backend.utils.active_requests.should_defer_cron", return_value=True) as mock_defer:
        metadata._run_metadata_sync("svc-meta-contract")
        mock_defer.assert_called_once_with("metadata_sync", "svc-meta-contract")
        base_context["start_run"].assert_not_called()
        base_context["log_run"].assert_not_called()


def test_contract_politeness_deferral_bypassed_on_manual_trigger(base_context, monkeypatch):
    _patch_iceberg_sync(monkeypatch)
    with patch("backend.utils.active_requests.should_defer_cron", return_value=True) as mock_defer:
        metadata._run_metadata_sync("svc-meta-contract", run_id=777)
        mock_defer.assert_not_called()
        args, kwargs = base_context["log_run"].call_args
        assert args[3] == "success" and kwargs["run_id"] == 777


def test_contract_uncommitted_table_logs_success_and_skips(base_context, monkeypatch):
    sync_data, _, _ = _patch_iceberg_sync(monkeypatch, table_exists=False)
    metadata._run_metadata_sync("svc-meta-contract")
    sync_data.assert_not_called()
    args, kwargs = base_context["log_run"].call_args
    assert args[3] == "success" and "not found" in kwargs["summary"].lower() and base_context["end_prog"].called


def test_contract_table_not_found_exception_treated_as_graceful_skip(base_context, monkeypatch):
    sync_data, _, _ = _patch_iceberg_sync(monkeypatch, init_ret=ValueError("Table logs not found in catalog"))
    metadata._run_metadata_sync("svc-meta-contract")
    sync_data.assert_not_called()
    args, kwargs = base_context["log_run"].call_args
    assert args[3] == "success" and "not found" in kwargs["summary"].lower()


def test_contract_catalog_attach_failure_raises_and_logs_error(base_context, monkeypatch):
    _patch_iceberg_sync(monkeypatch, init_ret=None)
    metadata._run_metadata_sync("svc-meta-contract")
    args, kwargs = base_context["log_run"].call_args
    assert args[3] == "error"
    assert "could not attach the DuckLake catalog" in kwargs["error_message"]
    assert "Metadata sync failed" in kwargs["summary"]


def test_contract_explicit_time_range_persisted_to_config(base_context, monkeypatch):
    sync_mock, _, _ = _patch_iceberg_sync(monkeypatch)
    metadata._run_metadata_sync(
        "svc-meta-contract", run_id=12, start_time="2026-06-01T00:00:00Z", end_time="2026-06-15T00:00:00Z"
    )
    assert base_context["save_cfg"].called
    tr = base_context["saved_cfgs"][-1]["provisioning"]["time_range"]
    assert tr == {"start": "2026-06-01T00:00:00Z", "end": "2026-06-15T00:00:00Z"}
    assert base_context["src"]["time_range"] == tr
    assert sync_mock.call_args[1]["start_time"] == "2026-06-01T00:00:00Z"
    assert sync_mock.call_args[1]["end_time"] == "2026-06-15T00:00:00Z"


def test_contract_manual_sync_all_clears_pinned_time_range(base_context, monkeypatch):
    base_context["cfg"]["provisioning"]["time_range"] = {"start": "2026-01-01T00:00:00Z"}
    _patch_iceberg_sync(monkeypatch, files=1, rows=10)
    metadata._run_metadata_sync("svc-meta-contract", run_id=999)
    assert base_context["save_cfg"].called
    assert "time_range" not in base_context["saved_cfgs"][-1]["provisioning"]
    assert base_context["src"]["time_range"] is None


def test_contract_scheduled_run_respects_existing_config_time_range(base_context, monkeypatch):
    base_context["cfg"]["provisioning"]["time_range"] = {"start": "2026-05-01T00:00:00Z"}
    sync_mock, _, _ = _patch_iceberg_sync(monkeypatch)
    metadata._run_metadata_sync("svc-meta-contract")
    assert sync_mock.call_args[1]["start_time"] == "2026-05-01T00:00:00Z"


def test_contract_admin_state_import_failure_yields_warning_status(base_context, monkeypatch):
    _, update_view, _ = _patch_iceberg_sync(
        monkeypatch, files=2, rows=500, admin_err=RuntimeError("connection refused to admin state bucket")
    )
    metadata._run_metadata_sync("svc-meta-contract")
    update_view.assert_called_once()
    base_context["refresh_status"].assert_called_once_with("svc-meta-contract")
    args, kwargs = base_context["log_run"].call_args
    assert args[3] == "warning"
    assert "admin state import warning" in kwargs["summary"] and "connection refused" in kwargs["summary"]
    assert (kwargs["files_downloaded"], kwargs["rows_ingested"]) == (2, 500)


def test_contract_zero_class_a_cloud_mutations(base_context, monkeypatch):
    mock_s3 = MagicMock()
    monkeypatch.setattr("backend.core.duckdb._get_fos_client", lambda src: mock_s3)
    _patch_iceberg_sync(monkeypatch)
    from backend.core import iceberg as db_iceberg

    assert db_iceberg.sync_data(base_context["src"]) == {"files_downloaded": 0, "rows_downloaded": 0}
    for m in (mock_s3.put_object, mock_s3.delete_object, mock_s3.delete_objects, mock_s3.create_multipart_upload):
        m.assert_not_called()


def test_contract_telemetry_attribution_and_dashboard_cache_invalidation(base_context, monkeypatch):
    _, update_view, _ = _patch_iceberg_sync(monkeypatch, files=3, rows=2_500)
    invalidate_mock = MagicMock()
    monkeypatch.setattr("backend.repositories.dashboard.invalidate_service", invalidate_mock)

    metadata._run_metadata_sync("svc-meta-contract")
    update_view.assert_called_once()
    invalidate_mock.assert_called_once_with(base_context["src"]["name"])
    args, kwargs = base_context["log_run"].call_args
    assert (args[1], args[3], kwargs["files_downloaded"], kwargs["rows_ingested"]) == (
        "metadata_sync",
        "success",
        3,
        2_500,
    )
    assert "downloaded 3 new Iceberg data file(s)" in kwargs["summary"]


def test_contract_guaranteed_cleanup_on_success_and_error(base_context, monkeypatch):
    _patch_iceberg_sync(monkeypatch)
    metadata._run_metadata_sync("svc-meta-contract", run_id=101)
    base_context["end_prog"].assert_called_with(101)
    base_context["finalize_dur"].assert_called_once()

    base_context["end_prog"].reset_mock()
    base_context["finalize_dur"].reset_mock()
    monkeypatch.setattr("backend.core.iceberg.sync_data", MagicMock(side_effect=RuntimeError("disk write error")))

    metadata._run_metadata_sync("svc-meta-contract", run_id=102)
    base_context["end_prog"].assert_called_with(102)
    base_context["finalize_dur"].assert_called_once()
    args, kwargs = base_context["log_run"].call_args
    assert args[3] == "error" and "disk write error" in kwargs["error_message"]


def test_contract_rebuild_local_view_endpoint_lifecycle():
    test_src = _fake_src("svc-rebuild-test")
    with (
        patch("backend.core.iceberg.clear_source_caches") as mock_clear,
        patch("backend.core.duckdb._cache_dir", return_value="/tmp/test_cache"),
        patch("os.path.exists", return_value=True),
        patch("os.remove"),
        patch("backend.core.duckdb.start_cron_run", return_value="run-789") as mock_start_cron,
        patch("backend.cron_progress.start_progress") as mock_prog,
        patch("threading.Thread") as mock_thread,
    ):
        app.dependency_overrides[get_source] = lambda: test_src
        client = TestClient(app)
        try:
            resp = client.post("/api/admin/rebuild-local-view")
            assert resp.status_code == 202
            data = resp.json()
            assert (data["ok"], data["run_id"], data["message"]) == (
                True,
                "run-789",
                "Local view rebuild started.",
            )
            mock_clear.assert_called_once_with("svc-rebuild-test")
            mock_start_cron.assert_called_once_with(test_src, "metadata_sync")
            mock_prog.assert_called_once_with("run-789", service_id="svc-rebuild-test", task="metadata_sync")
            mock_thread.assert_called_once()
        finally:
            app.dependency_overrides.pop(get_source, None)


def test_contract_rebuild_local_view_endpoint_busy_handling():
    test_src = _fake_src("svc-busy-test")
    with (
        patch("backend.core.iceberg.clear_source_caches"),
        patch("backend.core.duckdb._cache_dir", return_value="/tmp/test_cache"),
        patch("os.path.exists", return_value=False),
        patch("backend.core.duckdb.start_cron_run", side_effect=CronLeaseHeldError("cron job busy")),
    ):
        app.dependency_overrides[get_source] = lambda: test_src
        client = TestClient(app)
        try:
            resp = client.post("/api/admin/rebuild-local-view")
            assert resp.status_code == 503 and "cron_busy" in str(resp.json())
        finally:
            app.dependency_overrides.pop(get_source, None)


def test_post_commit_metadata_sync_worker_clears_state_after_failure(monkeypatch):
    class DeferredThread:
        def __init__(self, *, target, args, **kwargs):
            self.target, self.args = target, args

        def start(self):
            return None

    monkeypatch.setattr(metadata.threading, "Thread", DeferredThread)
    monkeypatch.setattr(
        metadata, "_run_post_commit_metadata_sync", MagicMock(side_effect=RuntimeError("unexpected failure"))
    )
    metadata.dispatch_post_commit_metadata_sync("svc-1")
    metadata._post_commit_metadata_sync_worker("svc-1")
    assert metadata._post_commit_metadata_sync_rerun == {}


def test_post_commit_metadata_sync_dispatch_clears_state_if_thread_start_fails(monkeypatch):
    class FailedThread:
        def __init__(self, *, target, args, **kwargs):
            pass

        def start(self):
            raise RuntimeError("thread unavailable")

    monkeypatch.setattr(metadata.threading, "Thread", FailedThread)
    with pytest.raises(RuntimeError, match="thread unavailable"):
        metadata.dispatch_post_commit_metadata_sync("svc-1")
    assert metadata._post_commit_metadata_sync_rerun == {}


def test_returns_when_config_missing(monkeypatch):
    monkeypatch.setattr("backend.config.load_config", MagicMock(return_value=None))
    get_src = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", get_src)
    metadata._run_metadata_sync("ghost")
    get_src.assert_not_called()


def test_returns_when_source_missing(monkeypatch, base_context):
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)
    metadata._run_metadata_sync("svc-meta-contract")
    base_context["start_run"].assert_not_called()


def test_skips_when_start_cron_run_raises(monkeypatch, base_context):
    base_context["start_run"].side_effect = CronLeaseHeldError("already running")
    init = MagicMock()
    monkeypatch.setattr("backend.core.iceberg.init_iceberg_table", init)
    metadata._run_metadata_sync("svc-meta-contract")
    init.assert_not_called()
    base_context["log_run"].assert_not_called()


def test_happy_path_with_zero_new_files_still_succeeds(monkeypatch, base_context):
    _patch_iceberg_sync(monkeypatch, files=0, rows=0)
    metadata._run_metadata_sync("svc-meta-contract")
    args, kwargs = base_context["log_run"].call_args
    assert (args[3], kwargs["files_downloaded"]) == ("success", 0)
    assert "new Iceberg data file" not in kwargs["summary"]


def test_lifecycle_rows_have_no_log_output_and_one_terminal_row(monkeypatch, base_context):
    _patch_iceberg_sync(monkeypatch, init_ret=ValueError("Table foo.bar not found in catalog"))
    metadata._run_metadata_sync("svc-meta-contract")
    base_context["log_run"].assert_called_once()
    args, kwargs = base_context["log_run"].call_args
    assert (args[1], kwargs["run_id"], kwargs["log_output"]) == ("metadata_sync", 88, None)
    base_context["end_prog"].assert_called_once_with(88)


# ── Merged from test_bot_data_refresh.py ──────────────────────────────────────────────


def test_run_bot_data_refresh_records_success_with_total_entries():
    fake_results = [{"id": "well-known-bots", "entry_count": 100}, {"id": "tor-exit-nodes", "entry_count": 50}]
    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.utils.bot_sources.refresh_all_sources", return_value=fake_results),
        patch("backend.cron.decorators.record_job_run") as mock_record,
    ):
        _run_bot_data_refresh()
    args = mock_record.call_args[0]
    assert (args[0], args[1]) == ("bot_data_refresh", "success") and "2" in args[3] and "150" in args[3]


def test_run_bot_data_refresh_records_warning_on_partial_failure():
    fake_results = [
        {"id": "well-known-bots", "entry_count": 100},
        {"id": "tor-exit-nodes", "error": "HTTP 503 Service Unavailable", "entry_count": 0, "failed": True},
    ]
    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.utils.bot_sources.refresh_all_sources", return_value=fake_results),
        patch("backend.cron.decorators.record_job_run") as mock_record,
    ):
        _run_bot_data_refresh()
    args = mock_record.call_args[0]
    assert (args[0], args[1]) == ("bot_data_refresh", "warning")
    assert "1 source(s) failed: tor-exit-nodes" in args[3]


def test_run_bot_data_refresh_records_error_on_total_failure():
    fake_results = [
        {"id": "well-known-bots", "error": "Connection refused", "entry_count": 0, "failed": True},
        {"id": "tor-exit-nodes", "error": "DNS resolution failed", "entry_count": 0, "failed": True},
    ]
    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.utils.bot_sources.refresh_all_sources", return_value=fake_results),
        patch("backend.cron.decorators.record_job_run") as mock_record,
    ):
        _run_bot_data_refresh()
    args = mock_record.call_args[0]
    assert (args[0], args[1]) == ("bot_data_refresh", "error") and "All 2 bot sources failed" in args[3]


def test_run_bot_data_refresh_skips_when_dev_mode_active():
    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=True),
        patch("backend.utils.bot_sources.refresh_all_sources") as mock_refresh,
        patch("backend.cron.decorators.record_job_run") as mock_record,
    ):
        _run_bot_data_refresh()
    mock_refresh.assert_not_called()
    args = mock_record.call_args[0]
    assert (args[0], args[1]) == ("bot_data_refresh", "skipped") and "dev_mode_no_crons" in args[3]


def test_admin_refresh_all_bot_sources_endpoints():
    client = TestClient(app)
    fake_results = [{"id": "well-known-bots", "entry_count": 100}, {"id": "tor-exit-nodes", "entry_count": 50}]
    with patch("backend.utils.bot_sources.refresh_all_sources", return_value=fake_results) as mock_refresh:
        for path in ("/api/admin/bot-sources/refresh", "/api/admin/bots/refresh"):
            resp = client.post(path)
            assert resp.status_code == 200
            data = resp.json()
            assert data["ok"] is True and data["updated_count"] == 2 and data["failed_count"] == 0
        assert mock_refresh.call_count == 2


# ── Merged from test_ngwaf_sync.py ──────────────────────────────────────────────


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


def test_ngwaf_sync_skips_when_dev_mode_active():
    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=True),
        patch("backend.config.load_config") as mock_load,
    ):
        _run_ngwaf_bot_sync("test_service")
    mock_load.assert_not_called()


def test_ngwaf_sync_seeds_watermark_on_first_run(fake_cfg):
    with _ngwaf_sync_env(fake_cfg, last_ts=None) as env:
        _run_ngwaf_bot_sync("test_service")
    env["update_wm"].assert_called_once()
    env["fetch"].assert_not_called()
    assert env["log"].call_args[0][3] == "success" and "First sync" in env["log"].call_args[1]["summary"]


def test_ngwaf_sync_pages_records_and_restores_baseline_interval(fake_cfg):
    fake_records = [
        {"waf_req_id": "req_1", "bot_name": "Googlebot", "user_agent": "Googlebot/2.1"},
        {"waf_req_id": "req_2", "bot_name": "Bingbot", "user_agent": "bingbot/2.0"},
    ]
    with _ngwaf_sync_env(fake_cfg, fetch_ret=[(fake_records, "2026-09-01T01:00:00Z", 2)]) as env:
        _run_ngwaf_bot_sync("test_service")
    env["upsert"].assert_called_once()
    assert env["log"].call_args[0][3] == "success"
    assert "Synced 2 bot record(s)" in env["log"].call_args[1]["summary"]
    env["job"].reschedule.assert_called_with("interval", minutes=5)


def test_ngwaf_sync_adaptive_rescheduling_under_heavy_load(fake_cfg):
    fake_records = [{"waf_req_id": f"req_{i}", "bot_name": "Googlebot"} for i in range(500)]
    with _ngwaf_sync_env(fake_cfg, fetch_ret=[(fake_records, "2026-09-01T01:00:00Z", 500)]) as env:
        _run_ngwaf_bot_sync("test_service")
    assert env["log"].call_args[0][3] == "success"
    env["job"].reschedule.assert_called_with("interval", minutes=2)


def test_ngwaf_sync_classifies_auth_errors(fake_cfg):
    err = RuntimeError("Fastly API 401 Unauthorized: invalid_api_key")
    with _ngwaf_sync_env(fake_cfg, fetch_side_effect=err) as env:
        _run_ngwaf_bot_sync("test_service")
    assert env["log"].call_args[0][3] == "error"
    assert "authentication error (check Fastly API key / workspace permissions)" in env["log"].call_args[1]["summary"]


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
        assert sync_resp.status_code == 200
        sync_data = sync_resp.json()
        assert (sync_data["ok"], sync_data["service_id"], sync_data["stats"]["total_cached_bots"]) == (
            True,
            "test_service",
            42,
        )
        mock_run.assert_called_once_with("test_service")

        status_resp = client.get("/api/admin/ngwaf/status?service_id=test_service")
        assert status_resp.status_code == 200
        status_data = status_resp.json()
        assert (status_data["ok"], status_data["total_cached_bots"], status_data["service_configured"]) == (
            True,
            42,
            True,
        )


def test_ngwaf_sync_adaptive_rescheduling_on_budget_exceeded(fake_cfg):
    fake_records = [{"waf_req_id": f"req_{i}", "bot_name": "Googlebot"} for i in range(5)]
    clock = [1000.0]

    def fake_fetch_paged(*_args, **_kwargs):
        clock[0] = 1245.0
        yield (fake_records, "2026-09-01T01:00:00Z", 5)

    with (
        _ngwaf_sync_env(fake_cfg, fetch_side_effect=fake_fetch_paged) as env,
        patch("time.time", side_effect=lambda: clock[0]),
    ):
        _run_ngwaf_bot_sync("test_service")

    assert env["log"].call_args[0][3] == "success"
    summary = env["log"].call_args[1]["summary"]
    assert "budget reached — will continue next run" in summary and "Synced 5 bot record(s)" in summary
    env["job"].reschedule.assert_called_with("interval", minutes=2)


def test_ngwaf_sync_progress_lifecycle(fake_cfg):
    fake_records = [{"waf_req_id": "req_1", "bot_name": "Googlebot"}]
    with _ngwaf_sync_env(fake_cfg, run_id=42, fetch_ret=[(fake_records, "2026-09-01T01:00:00Z", 1)]) as env:
        _run_ngwaf_bot_sync("test_service")
    env["start_prog"].assert_called_once_with(42, service_id="test_service", task="ngwaf_sync")
    env["end_prog"].assert_called_once_with(42)
    assert env["finalize"].call_args[0][:2] == (env["src"], 42)


def test_ngwaf_sync_guaranteed_cleanup_on_error(fake_cfg):
    with _ngwaf_sync_env(fake_cfg, run_id=99, fetch_side_effect=ValueError("Unexpected fetch failure")) as env:
        _run_ngwaf_bot_sync("test_service")
    assert env["log"].call_args[0][3] == "error"
    env["end_prog"].assert_called_once_with(99)
    assert env["finalize"].call_args[0][:2] == (env["src"], 99)


def test_ngwaf_sync_first_run_watermark_guaranteed_cleanup(fake_cfg):
    with _ngwaf_sync_env(fake_cfg, run_id=101, last_ts=None) as env:
        _run_ngwaf_bot_sync("test_service")
    assert env["log"].call_args[0][3] == "success"
    env["end_prog"].assert_called_once_with(101)
    assert env["finalize"].call_args[0][:2] == (env["src"], 101)


def test_ngwaf_sync_lease_held_is_clean_skip(fake_cfg):
    with _ngwaf_sync_env(fake_cfg, start_side_effect=CronLeaseHeldError("held")) as env:
        _run_ngwaf_bot_sync("test_service")
    env["log"].assert_not_called()
    env["fetch"].assert_not_called()
    env["end_prog"].assert_not_called()


def test_ngwaf_sync_error_row_matches_legacy_shape(fake_cfg):
    with _ngwaf_sync_env(fake_cfg, run_id=5, fetch_side_effect=RuntimeError("socket reset")) as env:
        _run_ngwaf_bot_sync("test_service")
    args, kwargs = env["log"].call_args
    assert (args[1], args[3]) == ("ngwaf_sync", "error")
    assert (
        kwargs["summary"],
        kwargs["error_message"],
        kwargs["files_downloaded"],
        kwargs["run_id"],
        kwargs["log_output"],
    ) == ("NGWAF sync failed: socket reset", "socket reset", 0, 5, None)


# ── Merged from test_rdns_enrichment.py ──────────────────────────────────────────────


def test_run_rdns_enrichment_records_success_with_counts():
    fake_summary = {"resolved": 20, "errors": 0, "discovered": 10, "batch_limit": 200}
    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.utils.rdns_cache.enrich_batch", return_value=fake_summary),
        patch("backend.cron.decorators.record_job_run") as mock_record,
    ):
        _run_rdns_enrichment()
    args = mock_record.call_args[0]
    assert (args[0], args[1]) == ("rdns_enrichment", "success")
    for tok in ("resolved=20", "errors=0", "discovered=10"):
        assert tok in args[3]


def test_run_rdns_enrichment_records_warning_on_dns_failures():
    fake_summary = {"resolved": 0, "errors": 8, "discovered": 0, "batch_limit": 200}
    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.utils.rdns_cache.enrich_batch", return_value=fake_summary),
        patch("backend.cron.decorators.record_job_run") as mock_record,
    ):
        _run_rdns_enrichment()
    args = mock_record.call_args[0]
    assert (args[0], args[1]) == ("rdns_enrichment", "warning") and "DNS lookup failures detected" in args[3]


def test_run_rdns_enrichment_skips_when_dev_mode_active():
    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=True),
        patch("backend.utils.rdns_cache.enrich_batch") as mock_enrich,
        patch("backend.cron.decorators.record_job_run") as mock_record,
    ):
        _run_rdns_enrichment()
    mock_enrich.assert_not_called()
    args = mock_record.call_args[0]
    assert (args[0], args[1]) == ("rdns_enrichment", "skipped") and "dev_mode_no_crons" in args[3]


@pytest.mark.parametrize(
    ("pending_count", "expected_batch_limit"),
    [(0, 200), (50, 100), (150, 150), (500, 500), (2500, 1000)],
)
def test_enrich_batch_dynamic_batch_sizing(pending_count, expected_batch_limit):
    with (
        patch("backend.utils.rdns_cache._get_pending_count", return_value=pending_count),
        patch("backend.utils.rdns_cache._select_ips_with_status", return_value=[]),
        patch("backend.utils.rdns_cache._select_stale_ips", return_value=[]),
        patch("backend.utils.rdns_cache._discover_new_ips", return_value=0),
        patch("backend.utils.rdns_cache._maybe_reap_stale_rows"),
    ):
        assert rdns_cache.enrich_batch()["batch_limit"] == expected_batch_limit


def test_enrich_batch_honors_explicit_limit_override():
    with (
        patch("backend.utils.rdns_cache._get_pending_count") as mock_count,
        patch("backend.utils.rdns_cache._select_ips_with_status", return_value=[]) as mock_select,
        patch("backend.utils.rdns_cache._select_stale_ips", return_value=[]),
        patch("backend.utils.rdns_cache._discover_new_ips", return_value=0),
        patch("backend.utils.rdns_cache._maybe_reap_stale_rows"),
    ):
        res = rdns_cache.enrich_batch(limit=42)
        mock_count.assert_not_called()
        assert res["batch_limit"] == 42 and mock_select.call_args[1]["limit"] == 42


def test_admin_rdns_enrich_endpoint():
    client = TestClient(app)
    fake_summary = {"resolved": 5, "errors": 0, "discovered": 2, "batch_limit": 200}
    with patch("backend.utils.rdns_cache.enrich_batch", return_value=fake_summary) as mock_enrich:
        resp = client.post("/api/admin/rdns/enrich?limit=50")
        assert resp.status_code == 200 and resp.json()["ok"] is True and resp.json()["summary"] == fake_summary
        mock_enrich.assert_called_once_with(limit=50)


def test_admin_rdns_stats_endpoint():
    client = TestClient(app)
    fake_stats = {"total": 1200, "pending": 45, "last_enrichment_at": "2026-09-21T00:00:00Z"}
    with patch("backend.utils.rdns_cache.get_stats", return_value=fake_stats):
        resp = client.get("/api/admin/rdns/stats")
        assert resp.status_code == 200 and resp.json()["ok"] is True and resp.json()["stats"] == fake_stats


def test_rdns_enrichment_routes_to_maintenance_executor():
    s = Scheduler()
    with patch.object(s._sched, "add_job") as mock_add_job:
        s._add_job(lambda: None, "interval", minutes=5, id="rdns_enrichment")
        assert mock_add_job.call_args[1]["executor"] == "maintenance"


# ── Merged from test_share_audit_purge.py ──────────────────────────────────────────────


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


def test_purge_old_audit_logs(test_share_db):
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
    remaining = con.execute("SELECT email FROM remote_share_audit_logs").fetchall()
    assert len(remaining) == 1 and remaining[0]["email"] == "new@example.com"


def test_purge_stale_share_records(test_share_db):
    con = test_share_db
    past_iso = iso_z(datetime.now(UTC) - timedelta(days=2))
    future_iso = iso_z(datetime.now(UTC) + timedelta(days=5))
    stale_session_ts = iso_z(datetime.now(UTC) - timedelta(days=35))
    active_session_ts = iso_z(datetime.now(UTC) - timedelta(hours=2))

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
    for sid, last_ts in (("sess_stale", stale_session_ts), ("sess_active", active_session_ts)):
        con.execute(
            "INSERT INTO remote_sessions(session_id, invite_id, name, email, ip_address, user_agent, fingerprint_signature, pii_policy, login_time, last_active_time) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (sid, "inv_active", "Active User", "active@test.com", "1.1.1.1", "ua", "fp", "{}", past_iso, last_ts),
        )
    con.commit()

    stats = share_db.purge_stale_share_records(max_idle_session_days=30, con=con)
    assert (
        stats["deleted_expired_invites"],
        stats["deleted_claim_tokens"],
        stats["deleted_stale_sessions"],
    ) == (1, 1, 1)
    assert [r["id"] for r in con.execute("SELECT id FROM remote_invites").fetchall()] == ["inv_active"]
    assert [r["session_id"] for r in con.execute("SELECT session_id FROM remote_sessions").fetchall()] == [
        "sess_active"
    ]


def test_run_share_audit_purge_lifecycle():
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
    assert status == "success"
    for tok in ("deleted=5", "retention_days=60", "expired_invites=2", "stale_sessions=3"):
        assert tok in detail


def test_share_audit_purge_routes_to_maintenance_executor():
    s = Scheduler()
    with patch.object(s._sched, "add_job") as mock_add_job:
        s._add_job(lambda: None, "cron", hour=3, minute=45, id="share_audit_purge")
        assert mock_add_job.call_args[1]["executor"] == "maintenance"


def test_run_share_audit_purge_skips_when_dev_mode_active():
    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=True),
        patch("backend.core.share_db.purge_old_audit_logs") as mock_audit,
    ):
        result = _run_share_audit_purge.__wrapped__()
        status, detail = result if isinstance(result, tuple) else ("success", result)
        assert status == "skipped" and "dev_mode_no_crons" in detail
        mock_audit.assert_not_called()


def test_admin_share_purge_endpoint():
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
        assert resp.status_code == 200
        data = resp.json()
        assert (
            data["ok"],
            data["deleted_audit_logs"],
            data["deleted_expired_invites"],
            data["deleted_stale_sessions"],
        ) == (True, 12, 1, 4)
        mock_purge_all.assert_called_once_with(audit_retention_days=45, max_idle_session_days=15)


def test_admin_share_audit_logs_endpoint(test_share_db):
    con = test_share_db
    recent_ts = iso_z(datetime.now(UTC) - timedelta(hours=1))
    con.execute(
        "INSERT INTO remote_share_audit_logs(timestamp, event_type, email, ip_address, details) VALUES (?,?,?,?,?)",
        (recent_ts, "LOGIN_SUCCESS", "analyst@example.com", "127.0.0.1", "{}"),
    )
    con.commit()

    client = TestClient(app)
    resp = client.get("/api/admin/share/audit-logs")
    assert resp.status_code == 200
    data = resp.json()
    assert "audit_logs" in data and any(entry["email"] == "analyst@example.com" for entry in data["audit_logs"])
