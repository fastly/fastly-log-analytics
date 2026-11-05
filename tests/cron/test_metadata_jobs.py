"""Consolidated tests for test_metadata_jobs."""

from __future__ import annotations

import json
import time
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from starlette.testclient import TestClient

import backend.core.metadata as metadata_db
import backend.core.metadata.base as reconciliation
import backend.core.metadata.usage_log as usage_log_db
import backend.utils.bot_sources as rdns_cache
from backend.core import share_db
from backend.core.metadata.usage_log import CronLeaseHeldError
from backend.core.request_context import get_service_id, get_source, require_admin
from backend.cron.jobs import metadata as metadata_job
from backend.cron.jobs.metadata import (
    _run_bot_data_refresh,
    _run_metadata_cleanup,
    _run_ngwaf_bot_sync,
    _run_rdns_enrichment,
    _run_share_audit_purge,
)
from backend.cron.scheduler import Scheduler
from backend.main import app
from backend.utils.date_utils import iso_z

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
    ) VALUES (1, 'clickhouse', ?, ?, ?, 100.0, 'success', 'SELECT 1', 8, 'api', 'test', 'test_caller', 'test.py') RETURNING id"""
    ids = [con.execute(sql, (service_id, cutoff, cutoff + 0.1)).fetchone()[0] for _ in range(rows)]
    con.commit()
    return ids


def _sync_scheduler_with(s: Scheduler, cfg: dict, src: dict) -> None:
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.fos_client.get_source_for_service", return_value=src),
        patch("backend.core.fos_client.is_configured", return_value=True),
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
        "log_retention_days": 90,
        "metadata_retention": {
            "usage_log_days": 1,
            "ingested_files_days": 7,
            "cron_runs_days": 7,
            "slow_queries_days": 30,
        },
        "provisioning": {
            "cron_sync": {"enabled": True, "delete_after": True},
            "cron_metadata_cleanup": {"enabled": True, "cron_hour": 3, "cron_minute": 0},
        },
    }
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
    monkeypatch.setattr(
        "backend.core.fos_client.get_source_for_service", lambda sid: src if sid == service_id else None
    )
    monkeypatch.setattr("backend.config.load_config", lambda sid: src if sid == service_id else None)
    return src


def _patch_cleanup_progress_mocks(monkeypatch, run_id: int = 999):
    start_prog, end_prog, finalize_dur, log_cron = (
        MagicMock(),
        MagicMock(),
        MagicMock(),
        MagicMock(),
    )
    monkeypatch.setattr("backend.core.fos_client.start_cron_run", MagicMock(return_value=run_id))
    monkeypatch.setattr("backend.core.fos_client.log_cron_run", log_cron)
    monkeypatch.setattr("backend.cron.scheduler.start_progress", start_prog)
    monkeypatch.setattr("backend.cron.scheduler.end_progress", end_prog)
    monkeypatch.setattr("backend.cron.scheduler.cleanup_progress_and_reap", MagicMock())
    monkeypatch.setattr("backend.cron.jobs._common.finalize_cron_duration", finalize_dur)
    return start_prog, end_prog, finalize_dur, log_cron


def test_metadata_cleanup_skip_gates_and_zero_fos_calls(monkeypatch, mc_contract_source):
    sid = mc_contract_source["service_id"]
    start_cron, cleanup_mock = MagicMock(), MagicMock()
    monkeypatch.setattr("backend.core.fos_client.start_cron_run", start_cron)
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
    monkeypatch.setattr("backend.utils.telemetry.should_defer_cron", MagicMock(return_value=True))
    metadata_job._run_metadata_cleanup(sid)
    start_cron.assert_not_called()

    # 3. Lease held skips without logging row
    monkeypatch.setattr("backend.utils.telemetry.should_defer_cron", MagicMock(return_value=False))
    monkeypatch.setattr("backend.core.fos_client.start_cron_run", MagicMock(side_effect=CronLeaseHeldError("held")))
    log_cron, end_prog = MagicMock(), MagicMock()
    monkeypatch.setattr("backend.core.fos_client.log_cron_run", log_cron)
    monkeypatch.setattr("backend.cron.scheduler.end_progress", end_prog)
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
        "backend.core.fos_client.get_source_for_service", lambda s: mc_contract_source if s == sid else None
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

    # Cron runs and slow queries retention
    _seed_cron_runs(sid, 4, days_ago=15)
    _seed_cron_runs(sid, 2, days_ago=2)
    expired_ids = _seed_slow_queries(sid, 5, days_ago=45)
    recent_ids = _seed_slow_queries(sid, 3, days_ago=5)
    _patch_cleanup_progress_mocks(monkeypatch, run_id=101)
    metadata_job._run_metadata_cleanup(sid)
    con = metadata_db.get_con(sid)
    assert con.execute("SELECT count(*) FROM cron_runs").fetchone()[0] == 2
    seeded_ids = expired_ids + recent_ids
    placeholders = ", ".join("?" for _ in seeded_ids)
    remaining_sq = con.execute(
        f"SELECT id FROM slow_queries WHERE service_id = ? AND id IN ({placeholders})", (sid, *seeded_ids)
    ).fetchall()
    assert {r[0] for r in remaining_sq} == set(recent_ids)


def test_metadata_cleanup_telemetry_manual_trigger_and_scheduler(monkeypatch, mc_contract_source):
    sid = mc_contract_source["service_id"]

    # Success telemetry & progress
    start_prog, end_prog, finalize_dur, log_cron = _patch_cleanup_progress_mocks(monkeypatch, run_id=777)
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
    _, end_prog, finalize_dur, log_cron = _patch_cleanup_progress_mocks(monkeypatch, run_id=888)
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
            patch("backend.core.fos_client.start_cron_run", return_value=501),
            patch("backend.core.fos_client.log_cron_run"),
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
        patch("backend.cron.scheduler.dev_mode_no_crons", return_value=dev_mode),
        patch("backend.utils.bot_sources.refresh_all_sources", return_value=fake_results) as mock_refresh,
        patch("backend.cron.scheduler.record_job_run") as mock_record,
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
        patch("backend.cron.scheduler.dev_mode_no_crons", return_value=False),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws_123"),
        patch("backend.cron.jobs.metadata.get_source_for_service", return_value=mock_src),
        patch("backend.core.fos_client.start_cron_run", **start_kwargs),
        patch("backend.core.fos_client.log_cron_run") as mock_log,
        patch("backend.cron.jobs.metadata.get_last_timestamp", return_value=last_ts),
        patch("backend.cron.jobs.metadata.update_sync_watermark") as mock_update_wm,
        patch("backend.cron.jobs.metadata.fetch_verified_bots_paged", **fetch_kwargs) as mock_fetch,
        patch("backend.cron.jobs.metadata.upsert_bots") as mock_upsert,
        patch("backend.cron.jobs.metadata.cleanup_old_bots", return_value=0),
        patch("backend.cron.scheduler.get_scheduler", return_value=mock_sched),
        patch("backend.cron.scheduler.start_progress") as mock_start_prog,
        patch("backend.cron.scheduler.end_progress") as mock_end_prog,
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
        patch("backend.cron.scheduler.dev_mode_no_crons", return_value=True),
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
            "backend.cron.jobs.metadata.get_cache_stats",
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
        patch("backend.cron.scheduler.dev_mode_no_crons", return_value=dev_mode),
        patch("backend.utils.bot_sources.enrich_batch", return_value=fake_summary) as mock_enrich,
        patch("backend.cron.scheduler.record_job_run") as mock_record,
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
        patch("backend.utils.bot_sources._get_pending_count", return_value=pending_count) as mock_count,
        patch("backend.utils.bot_sources._select_ips_with_status", return_value=[]) as mock_select,
        patch("backend.utils.bot_sources._select_stale_ips", return_value=[]),
        patch("backend.utils.bot_sources._discover_new_ips", return_value=0),
        patch("backend.utils.bot_sources._maybe_reap_stale_rows"),
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
    with patch("backend.utils.bot_sources.enrich_batch", return_value=fake_summary) as mock_enrich:
        resp = client.post("/api/admin/rdns/enrich?limit=50")
        assert resp.status_code == 200 and resp.json()["summary"] == fake_summary
        mock_enrich.assert_called_once_with(limit=50)

    fake_stats = {"total": 1200, "pending": 45, "last_enrichment_at": "2026-09-21T00:00:00Z"}
    with patch("backend.utils.bot_sources.get_stats", return_value=fake_stats):
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
        patch("backend.cron.scheduler.dev_mode_no_crons", return_value=True),
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
