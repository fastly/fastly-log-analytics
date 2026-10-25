"""Consolidated tests for test_metadata_jobs."""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from starlette.testclient import TestClient

from backend.core import metadata as metadata_db
from backend.core.metadata import reconciliation, usage_log_db
from backend.cron.jobs import metadata as metadata_job
from backend.cron.scheduler import Scheduler
from backend.deps import get_service_id, get_source, require_admin
from backend.main import app
from backend.utils.date_utils import iso_z


def _collect_sse_events(body: bytes) -> list[dict]:
    events: list[dict] = []
    for line in body.decode("utf-8", errors="replace").splitlines():
        if not line.startswith("data: "):
            continue
        try:
            events.append(json.loads(line[len("data: ") :]))
        except json.JSONDecodeError:
            continue
    return events


def _seed_usage_log(service_id: str, rows: int, days_ago: int = 0) -> None:
    con = usage_log_db.get_con(service_id)
    now_dt = datetime.now(UTC) - timedelta(days=days_ago)
    now_ts = iso_z(now_dt)
    hour = now_ts[:13]
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
        (service_id, hour, rows, now_ts),
    )
    con.commit()


def _seed_ingested_files(
    service_id: str,
    rows: int,
    days_ago: int = 0,
    table_name: str = "logs",
    prefix: str = "raw/requests",
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
    ids = []
    for _ in range(rows):
        row = con.execute(
            """INSERT INTO slow_queries (
                query_id, db_type, service_id, started_at_utc, ended_at_utc, duration_ms,
                outcome, sql_preview, sql_len, attr_kind, attr_label, attr_caller_qualname, attr_caller_file
            ) VALUES (1, 'duckdb', ?, ?, ?, 100.0, 'success', 'SELECT 1', 8, 'api', 'test', 'test_caller', 'test.py')
            RETURNING id""",
            (service_id, cutoff, cutoff + 0.1),
        ).fetchone()
        ids.append(row[0])
    con.commit()
    return ids


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


# ── 1. Kill-Switch Protection (FLA_DEV_NO_CRONS=1) ───────────────────────────


def test_contract_1_dev_mode_no_crons_skips_execution(monkeypatch, mc_contract_source):
    """Checklist Item 1: Under FLA_DEV_NO_CRONS=1, _run_metadata_cleanup skips immediately."""
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    start_cron = MagicMock()
    cleanup_mock = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start_cron)
    monkeypatch.setattr("backend.core.metadata.cleanup_metadata", cleanup_mock)

    metadata_job._run_metadata_cleanup(mc_contract_source["service_id"])

    start_cron.assert_not_called()
    cleanup_mock.assert_not_called()


def test_contract_1_dev_mode_no_crons_skips_scheduler_registration(monkeypatch, mc_contract_source):
    """Checklist Item 1: Under FLA_DEV_NO_CRONS=1, Scheduler._sync_jobs does not register the job."""
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    s = Scheduler()
    s._sched = MagicMock()

    with (
        patch("backend.config.list_configs", return_value=[mc_contract_source]),
        patch("backend.core.duckdb.get_source_for_service", return_value=mc_contract_source),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()

    # Verify no metadata_cleanup job was registered with the underlying scheduler
    for call in s._sched.add_job.call_args_list:
        args, kwargs = call
        assert not kwargs.get("id", "").startswith("metadata_cleanup_")


# ── 2. Politeness Gating (should_defer_cron) ──────────────────────────────────


def test_contract_2_politeness_gating_defers_execution(monkeypatch, mc_contract_source):
    """Checklist Item 2: Defers execution when active queries are running on the dashboard."""
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", MagicMock(return_value=True))
    start_cron = MagicMock()
    cleanup_mock = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start_cron)
    monkeypatch.setattr("backend.core.metadata.cleanup_metadata", cleanup_mock)

    metadata_job._run_metadata_cleanup(mc_contract_source["service_id"])

    start_cron.assert_not_called()
    cleanup_mock.assert_not_called()


# ── 3. Zero FOS Calls Contract ────────────────────────────────────────────────


def test_contract_3_zero_fos_calls_contract(monkeypatch, mc_contract_source):
    """Checklist Item 3: Purged legacy FOS quarantine calls; zero FOS operations during cleanup."""
    boto3_mock = MagicMock()
    monkeypatch.setattr("boto3.client", boto3_mock)
    monkeypatch.setattr("boto3.resource", boto3_mock)

    start_cron = MagicMock(return_value=999)
    log_cron = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start_cron)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_cron)
    monkeypatch.setattr("backend.core.metric_snapshots.purge_old", MagicMock())
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())
    monkeypatch.setattr("backend.cron.jobs._common.finalize_cron_duration", MagicMock())

    metadata_job._run_metadata_cleanup(mc_contract_source["service_id"])

    # Ensure zero boto3 / FOS clients were created or called
    boto3_mock.assert_not_called()


# ── 4. Dedup Suppression on Ingested Files ────────────────────────────────────


def test_contract_4_dedup_suppression_when_delete_after_false(mc_contract_source):
    """Checklist Item 4: When delete_after=False, ingested_files retention is forced to 0."""
    sid = mc_contract_source["service_id"]
    # Set delete_after=False
    mc_contract_source["provisioning"]["cron_sync"]["delete_after"] = False

    _seed_ingested_files(sid, 5, days_ago=20)
    events: list[dict] = []

    res = reconciliation.cleanup_metadata(
        sid,
        retention={"ingested_files_days": 7},
        on_event=events.append,
    )

    # All 5 files must survive because dedup is inactive
    assert res["deleted"]["ingested_files"] == 0
    assert res["after"]["ingested_files"] == 5

    # Verify status message explaining override was emitted
    override_messages = [e["message"] for e in events if "cron_sync.delete_after=false" in e.get("message", "")]
    assert len(override_messages) == 1
    assert "ingested_files retention (7d) ignored" in override_messages[0]


# ── 5. RUM Cutoff Calculation ─────────────────────────────────────────────────


def test_contract_5_rum_window_calculation(mc_contract_source):
    """Checklist Item 5: Request rows pruned at ingested_files_days, RUM rows pruned at max(ingested_files_days, log_retention_days + 1)."""
    sid = mc_contract_source["service_id"]
    mc_contract_source["provisioning"]["cron_sync"]["delete_after"] = True
    mc_contract_source["log_retention_days"] = 90  # RUM cutoff = max(7, 90 + 1) = 91 days

    # 1. Seed request files: 3 files 10 days ago (older than 7d -> pruned), 2 files 2 days ago (kept)
    _seed_ingested_files(sid, 3, days_ago=10, table_name="logs", prefix="raw/request")
    _seed_ingested_files(sid, 2, days_ago=2, table_name="logs", prefix="raw/request")

    # 2. Seed RUM files:
    # 4 files 400 days ago (older than 91d -> pruned)
    _seed_ingested_files(sid, 2, days_ago=400, table_name="client_vitals", prefix="raw/rum")
    _seed_ingested_files(sid, 2, days_ago=400, table_name="client_errors", prefix="raw/rum")
    # 2 files 20 days ago (within 91d window -> kept)
    _seed_ingested_files(sid, 1, days_ago=20, table_name="client_vitals", prefix="raw/rum")
    _seed_ingested_files(sid, 1, days_ago=20, table_name="client_errors", prefix="raw/rum")

    res = reconciliation.cleanup_metadata(sid, retention={"ingested_files_days": 7})

    # Pruned: 3 request files + 4 RUM files = 7
    assert res["deleted"]["ingested_files"] == 7
    # Kept: 2 recent request files + 2 recent RUM files = 4
    assert res["after"]["ingested_files"] == 4

    # Verify summary rollup was updated
    con = metadata_db.get_con(sid)
    summary_row = con.execute("SELECT file_count FROM ingested_files_summary WHERE source_name = 'fos'").fetchone()
    if summary_row:
        assert summary_row[0] == 4


# ── 6. Usage Log Chunked Deletion & Summary Preservation ─────────────────────


def test_contract_6_usage_log_chunked_deletion_preserves_hourly_summary(mc_contract_source):
    """Checklist Item 6: Trims raw usage_log in chunks while preserving usage_log_hourly_summary."""
    sid = mc_contract_source["service_id"]
    _seed_usage_log(sid, 20, days_ago=5)
    _seed_usage_log(sid, 10, days_ago=0)

    con = usage_log_db.get_con(sid)
    summary_before = con.execute(
        "SELECT sum(count) FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)
    ).fetchone()[0]
    assert summary_before == 30

    res = reconciliation.cleanup_metadata(sid, retention={"usage_log_days": 1})

    # Raw usage_log older than 1 day deleted (20 deleted, 10 remain)
    assert res["deleted"]["usage_log"] == 20
    assert res["after"]["usage_log"] == 10

    # Hourly summary strictly preserved
    summary_after = con.execute(
        "SELECT sum(count) FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)
    ).fetchone()[0]
    assert summary_after == summary_before


# ── 7. Cron Runs, Slow Queries, & Global Metric Snapshots Retention ───────────


def test_contract_7_cron_runs_and_slow_queries_retention(monkeypatch, mc_contract_source):
    """Checklist Item 7: cron_runs (7d), slow_queries (30d epoch), and metric_snapshots (30d)."""
    sid = mc_contract_source["service_id"]
    _seed_cron_runs(sid, 4, days_ago=15)
    _seed_cron_runs(sid, 2, days_ago=2)

    expired_ids = _seed_slow_queries(sid, 5, days_ago=45)
    recent_ids = _seed_slow_queries(sid, 3, days_ago=5)

    purge_mock = MagicMock()
    monkeypatch.setattr("backend.core.metric_snapshots.purge_old", purge_mock)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=101))
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())
    monkeypatch.setattr("backend.cron.jobs._common.finalize_cron_duration", MagicMock())

    metadata_job._run_metadata_cleanup(sid)

    # Check cron_runs and slow_queries trimmed
    con = metadata_db.get_con(sid)
    remaining_cron_runs = con.execute("SELECT count(*) FROM cron_runs").fetchone()[0]
    # 2 recent seeded (4 older than 7d trimmed, and log_cron_run was mocked) = 2
    assert remaining_cron_runs == 2

    seeded_ids = expired_ids + recent_ids
    placeholders = ", ".join("?" for _ in seeded_ids)
    remaining_slow_queries = con.execute(
        f"SELECT id FROM slow_queries WHERE service_id = ? AND id IN ({placeholders})",
        (sid, *seeded_ids),
    ).fetchall()
    assert {row[0] for row in remaining_slow_queries} == set(recent_ids)

    # Check global metric_snapshots purge was invoked with retention_days=30
    purge_mock.assert_called_once_with(retention_days=30)


# ── 8. Telemetry, Progress & Duration Finalization ───────────────────────────


def test_contract_8_telemetry_progress_and_audit_logging_success(monkeypatch, mc_contract_source):
    """Checklist Item 8: Telemetry attribution and cron_runs row recording on success."""
    sid = mc_contract_source["service_id"]
    start_prog = MagicMock()
    end_prog = MagicMock()
    finalize_dur = MagicMock()
    log_cron = MagicMock()

    monkeypatch.setattr("backend.cron_progress.start_progress", start_prog)
    monkeypatch.setattr("backend.cron_progress.end_progress", end_prog)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())
    monkeypatch.setattr("backend.cron.jobs._common.finalize_cron_duration", finalize_dur)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=777))
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_cron)
    monkeypatch.setattr("backend.core.metric_snapshots.purge_old", MagicMock())

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

    # Verify audit row
    log_cron.assert_called_once()
    args, kwargs = log_cron.call_args
    assert args[1] == "metadata_cleanup"
    assert args[3] == "success"
    assert kwargs["rows_ingested"] == 15
    assert "Trimmed 15 rows" in kwargs["summary"]
    assert kwargs["run_id"] == 777


def test_contract_8_duration_finalized_on_error(monkeypatch, mc_contract_source):
    """Checklist Item 8: Duration and progress are guaranteed finalized in finally: on exception."""
    sid = mc_contract_source["service_id"]
    end_prog = MagicMock()
    finalize_dur = MagicMock()
    log_cron = MagicMock()

    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", end_prog)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())
    monkeypatch.setattr("backend.cron.jobs._common.finalize_cron_duration", finalize_dur)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=888))
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_cron)
    monkeypatch.setattr(
        "backend.core.metadata.cleanup_metadata", MagicMock(side_effect=RuntimeError("simulated metadata fail"))
    )

    metadata_job._run_metadata_cleanup(sid)

    end_prog.assert_called_once_with(888)
    finalize_dur.assert_called_once()

    log_cron.assert_called_once()
    args, kwargs = log_cron.call_args
    assert args[1] == "metadata_cleanup"
    assert args[3] == "error"
    assert "simulated metadata fail" in kwargs["summary"]
    assert kwargs["run_id"] == 888


# ── 9. Manual Trigger Endpoints & Route Parity ────────────────────────────────


def test_contract_9_manual_trigger_path_parity(mc_contract_source):
    """Checklist Item 9: POST /api/admin/metadata-cleanup/{service_id} and POST /api/admin/metadata-cleanup parity."""
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
            # Test 1: Path with explicit {service_id}
            resp_with_id = client.post(
                f"/api/admin/metadata-cleanup/{sid}",
                headers={"x-fastly-service-id": sid},
            )
            assert resp_with_id.status_code == 200
            assert resp_with_id.headers["content-type"].startswith("text/event-stream")
            events_id = _collect_sse_events(resp_with_id.content)
            assert any(e.get("type") == "done" and "Trimmed 15 rows" in e.get("message", "") for e in events_id)

            # Test 2: Generic path without {service_id}
            resp_generic = client.post(
                "/api/admin/metadata-cleanup",
                headers={"x-fastly-service-id": sid},
            )
            assert resp_generic.status_code == 200
            assert resp_generic.headers["content-type"].startswith("text/event-stream")
            events_generic = _collect_sse_events(resp_generic.content)
            assert any(e.get("type") == "done" and "Trimmed 15 rows" in e.get("message", "") for e in events_generic)

            # Test 3: Analyst denial
            def reject_analyst():
                from fastapi import HTTPException

                raise HTTPException(status_code=403, detail={"error": "admin_only"})

            app.dependency_overrides[require_admin] = reject_analyst
            resp_denied = client.post(
                f"/api/admin/metadata-cleanup/{sid}",
                headers={"x-fastly-service-id": sid},
            )
            assert resp_denied.status_code == 403
    finally:
        app.dependency_overrides.clear()


# ── 10. Dynamic Scheduler Rescheduling & Disable ─────────────────────────────


def test_contract_10_dynamic_scheduler_rescheduling_and_disable(mc_contract_source):
    """Checklist Item 10: Dynamic rescheduling when cron_hour/cron_minute change and disable."""
    s = Scheduler()
    mock_job = MagicMock()
    s._sched = MagicMock()
    s._sched.get_job = MagicMock(return_value=mock_job)
    job_id = f"metadata_cleanup_{mc_contract_source['service_id']}"
    s._job_ids[job_id] = job_id

    # 1. Reschedule test: hour=4, minute=45
    mc_contract_source["provisioning"]["cron_metadata_cleanup"] = {"cron_hour": 4, "cron_minute": 45}

    with (
        patch("backend.config.list_configs", return_value=[mc_contract_source]),
        patch("backend.core.duckdb.get_source_for_service", return_value=mc_contract_source),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()

    mock_job.reschedule.assert_called_once_with("cron", hour=4, minute=45)

    # 2. Disable test: enabled=False
    mc_contract_source["provisioning"]["cron_metadata_cleanup"] = {"enabled": False}
    s2 = Scheduler()
    s2._sched = MagicMock()

    with (
        patch("backend.config.list_configs", return_value=[mc_contract_source]),
        patch("backend.core.duckdb.get_source_for_service", return_value=mc_contract_source),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s2._sync_jobs()

    # Ensure add_job was never called for metadata_cleanup
    for call in s2._sched.add_job.call_args_list:
        args, kwargs = call
        assert not kwargs.get("id", "").startswith("metadata_cleanup_")


def test_metadata_cleanup_lease_held_skips_without_row(monkeypatch, mc_contract_source):
    from backend.core.metadata.cron_log import CronLeaseHeldError
    from backend.cron.jobs.metadata import _run_metadata_cleanup

    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", MagicMock(return_value=False))
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(side_effect=CronLeaseHeldError("held")))
    log_cron = MagicMock()
    cleanup = MagicMock()
    end_prog = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_cron)
    monkeypatch.setattr("backend.core.metadata.cleanup_metadata", cleanup)
    monkeypatch.setattr("backend.cron_progress.end_progress", end_prog)

    _run_metadata_cleanup(mc_contract_source["service_id"])

    log_cron.assert_not_called()
    cleanup.assert_not_called()
    end_prog.assert_not_called()


# ── Merged from test_sync_metadata_contract.py ──────────────────────────────────────────────


import pytest

from backend.core.metadata.cron_log import CronLeaseHeldError
from backend.cron.jobs import metadata
from tests._helpers.config_fakes import fake_mutate_config
from tests.cron.conftest import _StubSource

# ── Fixtures & Helpers ────────────────────────────────────────────────────────


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
    }
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


@pytest.fixture
def stub_source(monkeypatch) -> dict:
    src = _fake_src("svc-meta-contract", access_level="read_only")
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src)
    return src


@pytest.fixture
def base_context(monkeypatch, stub_source):
    """Mocks common lifecycle helpers for direct _run_metadata_sync execution."""
    cfg = _fake_cfg("svc-meta-contract", access_level="read_only")
    load_cfg = MagicMock(return_value=cfg)
    saved_cfgs: list[dict] = []
    save_cfg = MagicMock(side_effect=fake_mutate_config(lambda: cfg, saves=saved_cfgs))
    monkeypatch.setattr("backend.config.load_config", load_cfg)
    monkeypatch.setattr("backend.config.mutate_config", save_cfg)

    fake_con = MagicMock()
    get_conn = MagicMock(return_value=fake_con)
    start_run = MagicMock(return_value=88)
    log_run = MagicMock()
    refresh_status = MagicMock()
    finalize_dur = MagicMock()

    start_prog = MagicMock()
    end_prog = MagicMock()
    log_prog = MagicMock()
    reap_prog = MagicMock()

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


# ── Requirement 1: Role Gate & Tenant Isolation ─────────────────────────────


def test_contract_role_gate_analyst_path_a_only():
    """Requirement 1: Registered strictly for Analyst Path A standalone instances (access_level: "read_only"),
    never for Admin (read_write) in APScheduler.
    """
    s = Scheduler()

    # Case A: Analyst Path A (read_only) -> Registered
    cfg_analyst = _fake_cfg("svc-analyst-1", access_level="read_only")
    with (
        patch("backend.config.list_configs", return_value=[cfg_analyst]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-analyst-1", "read_only")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()

    assert "sync_metadata_svc-analyst-1" in s._job_ids
    # Verify write-side jobs are omitted for Analyst Path A
    assert "log_discovery_svc-analyst-1" not in s._job_ids
    assert "commit_svc-analyst-1" not in s._job_ids
    assert "optimize_svc-analyst-1" not in s._job_ids
    assert "expire_svc-analyst-1" not in s._job_ids

    # Case B: Admin (read_write) -> NOT registered in APScheduler
    s._sched.remove_all_jobs()
    s._job_ids.clear()
    cfg_admin = _fake_cfg("svc-admin-1", access_level="read_write")
    with (
        patch("backend.config.list_configs", return_value=[cfg_admin]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-admin-1", "read_write")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()

    assert "sync_metadata_svc-admin-1" not in s._job_ids


def test_contract_role_gate_analyst_path_b_blocked():
    """Requirement 1: Analyst Path B (remote share session) connects to host admin server and
    is blocked from accessing /api/admin/rebuild-local-view with HTTP 403 (admin_only).
    """
    from backend.utils.remote_access import _ANALYST_BLOCKED_PREFIXES, _is_blocked_path

    # Verify /api/admin/ is part of blocked prefixes
    assert any("/api/admin/" == p for p in _ANALYST_BLOCKED_PREFIXES)

    # Verify _is_blocked_path correctly flags /api/admin/rebuild-local-view
    assert _is_blocked_path("/api/admin/rebuild-local-view") is True
    assert _is_blocked_path("/api/admin/rebuild-local-view?service_id=svc-1") is True


def test_contract_role_gate_admin_post_commit_dispatch():
    """Requirement 1: Admins do not need a scheduled cron because they trigger view refresh
    on-demand via dispatch_post_commit_metadata_sync, which coalesces concurrent requests.
    """
    calls = []
    dispatched_threads = []

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

        assert len(dispatched_threads) == 1
        assert calls == []
        dispatched_threads[0].target(*dispatched_threads[0].args)

        assert calls == ["svc-admin-coalesce", "svc-admin-coalesce"]
        assert metadata._post_commit_metadata_sync_rerun == {}


# ── Requirement 2: Cadence, Trigger & Application Startup Execution ─────────


def test_contract_cadence_derivation_and_execution_flags():
    """Requirement 2: Cadence is derived from log_period (log_period // 2 when >= 60, or log_period when < 60),
    or overridden by cron_metadata_sync.interval_seconds.
    Trigger execution flags: max_instances=1, coalesce=True, misfire_grace_time=60.
    """
    s = Scheduler()

    # Case A: Derived from log_period >= 60 -> log_period // 2
    cfg_derived = _fake_cfg("svc-cadence-60", access_level="read_only", log_period=60)
    with (
        patch("backend.config.list_configs", return_value=[cfg_derived]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-cadence-60", "read_only")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()

    job_derived = s._sched.get_job("sync_metadata_svc-cadence-60")
    assert job_derived is not None
    assert int(job_derived.trigger.interval.total_seconds()) == 30
    assert job_derived.coalesce is True
    assert job_derived.misfire_grace_time == 60

    # Case B: Derived from log_period < 60 -> log_period
    s._sched.remove_all_jobs()
    s._job_ids.clear()
    cfg_short = _fake_cfg("svc-cadence-15", access_level="read_only", log_period=15)
    with (
        patch("backend.config.list_configs", return_value=[cfg_short]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-cadence-15", "read_only")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()

    job_short = s._sched.get_job("sync_metadata_svc-cadence-15")
    assert job_short is not None
    assert int(job_short.trigger.interval.total_seconds()) == 15

    # Case C: Overridden by cron_metadata_sync.interval_seconds
    s._sched.remove_all_jobs()
    s._job_ids.clear()
    cfg_custom = _fake_cfg("svc-cadence-custom", access_level="read_only", log_period=60, meta_interval_seconds=45)
    with (
        patch("backend.config.list_configs", return_value=[cfg_custom]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-cadence-custom", "read_only")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()

    job_custom = s._sched.get_job("sync_metadata_svc-cadence-custom")
    assert job_custom is not None
    assert int(job_custom.trigger.interval.total_seconds()) == 45


def test_contract_startup_trigger_initial_sync():
    """Requirement 2: Scheduler.start() registers one-shot initial_sync_{service_id} immediately at boot
    for Analyst Path A services with cron_sync.enabled = True, populating the dashboard without blocking boot.
    Admin services and disabled services do NOT register initial_sync_{service_id}.
    """
    s = Scheduler()
    added_jobs = []

    def fake_add_job(fn, *args, **kwargs):
        added_jobs.append({"fn": fn, "args": args, "kwargs": kwargs})

    s._add_job = fake_add_job
    s._sched = MagicMock()
    s._sync_jobs = MagicMock()

    cfg_analyst = _fake_cfg("svc-init-analyst", access_level="read_only", sync_enabled=True)
    cfg_admin = _fake_cfg("svc-init-admin", access_level="read_write", sync_enabled=True)
    cfg_disabled = _fake_cfg("svc-init-disabled", access_level="read_only", sync_enabled=False)

    with (
        patch("backend.cron.scheduler.dev_mode_no_crons", return_value=False),
        patch("backend.core.duckdb_pool._pool_warm_at_boot_enabled", return_value=False),
        patch("backend.config.list_configs", return_value=[cfg_analyst, cfg_admin, cfg_disabled]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src()),
        patch("backend.core.duckdb.is_configured", return_value=True),
    ):
        s.start()

    job_ids = [j["kwargs"].get("id") for j in added_jobs]
    assert "initial_sync_svc-init-analyst" in job_ids
    assert "initial_sync_svc-init-admin" not in job_ids
    assert "initial_sync_svc-init-disabled" not in job_ids


# ── Requirement 3: Dynamic Registration Gating & Rescheduling ───────────────


def test_contract_dynamic_registration_gating_and_rescheduling():
    """Requirement 3: Gated on cron_metadata_sync.enabled (default true). Dynamically reschedules
    when interval_seconds is updated, and unregisters when disabled.
    """
    s = Scheduler()

    # Case A: Disabled via cron_metadata_sync.enabled = False
    cfg_disabled = _fake_cfg("svc-meta-dis", access_level="read_only", meta_enabled=False)
    with (
        patch("backend.config.list_configs", return_value=[cfg_disabled]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-meta-dis", "read_only")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()

    assert "sync_metadata_svc-meta-dis" not in s._job_ids

    # Case B: Reschedule existing job when interval changes
    mock_job = MagicMock()
    s._sched = MagicMock()
    s._sched.get_job = MagicMock(return_value=mock_job)
    s._job_ids["sync_metadata_svc-meta-resched"] = "sync_metadata_svc-meta-resched"

    cfg_resched = _fake_cfg("svc-meta-resched", access_level="read_only", log_period=60, meta_interval_seconds=45)
    with (
        patch("backend.config.list_configs", return_value=[cfg_resched]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-meta-resched", "read_only")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()

    mock_job.reschedule.assert_called_once_with("interval", seconds=45)


# ── Requirement 4: Politeness Deferral Contract ─────────────────────────────


def test_contract_politeness_deferral_on_scheduled_runs(base_context):
    """Requirement 4: Scheduled runs (run_id is None) defer via should_defer_cron("metadata_sync", service_id)
    when active dashboard queries are in flight, avoiding view lock contention.
    """
    with patch("backend.utils.active_requests.should_defer_cron", return_value=True) as mock_defer:
        metadata._run_metadata_sync("svc-meta-contract")

        mock_defer.assert_called_once_with("metadata_sync", "svc-meta-contract")
        base_context["start_run"].assert_not_called()
        base_context["log_run"].assert_not_called()


def test_contract_politeness_deferral_bypassed_on_manual_trigger(base_context, monkeypatch):
    """Requirement 4: Manual trigger (run_id is not None) completely bypasses politeness deferral
    and executes to completion.
    """
    monkeypatch.setattr("backend.core.iceberg.init_iceberg_table", MagicMock(return_value=True))
    monkeypatch.setattr("backend.core.iceberg.ducklake_table_exists", MagicMock(return_value=True))
    monkeypatch.setattr(
        "backend.core.iceberg.sync_data",
        MagicMock(return_value={"files_downloaded": 0, "rows_downloaded": 0}),
    )
    monkeypatch.setattr("backend.core.iceberg.update_iceberg_view", MagicMock())
    monkeypatch.setattr("backend.state_sync.import_admin_state", MagicMock())

    with patch("backend.utils.active_requests.should_defer_cron", return_value=True) as mock_defer:
        # Manual run passing explicit run_id
        metadata._run_metadata_sync("svc-meta-contract", run_id=777)

        # Politeness deferral was bypassed (never called)
        mock_defer.assert_not_called()
        base_context["log_run"].assert_called_once()
        args, kwargs = base_context["log_run"].call_args
        assert args[3] == "success"
        assert kwargs["run_id"] == 777


# ── Requirement 5: Graceful Uncommitted Table Handling ──────────────────────


def test_contract_uncommitted_table_logs_success_and_skips(base_context, monkeypatch):
    """Requirement 5: When the DuckLake/Iceberg table has not yet been committed by the admin
    (ducklake_table_exists == False), logs status "success" with skip summary, avoiding false alert
    errors on fresh services.
    """
    monkeypatch.setattr("backend.core.iceberg.init_iceberg_table", MagicMock(return_value=True))
    monkeypatch.setattr("backend.core.iceberg.ducklake_table_exists", MagicMock(return_value=False))
    sync_data = MagicMock()
    monkeypatch.setattr("backend.core.iceberg.sync_data", sync_data)

    metadata._run_metadata_sync("svc-meta-contract")

    sync_data.assert_not_called()
    base_context["log_run"].assert_called_once()
    args, kwargs = base_context["log_run"].call_args
    assert args[3] == "success"
    assert "not found" in kwargs["summary"].lower()
    assert base_context["end_prog"].called


def test_contract_table_not_found_exception_treated_as_graceful_skip(base_context, monkeypatch):
    """Requirement 5: If table init raises an exception matching 'not found', 'does not exist',
    or 'nosuchtable', it is treated gracefully as an uncommitted table with status 'success'.
    """
    monkeypatch.setattr(
        "backend.core.iceberg.init_iceberg_table",
        MagicMock(side_effect=ValueError("Table logs not found in catalog")),
    )
    sync_data = MagicMock()
    monkeypatch.setattr("backend.core.iceberg.sync_data", sync_data)

    metadata._run_metadata_sync("svc-meta-contract")

    sync_data.assert_not_called()
    base_context["log_run"].assert_called_once()
    args, kwargs = base_context["log_run"].call_args
    assert args[3] == "success"
    assert "not found" in kwargs["summary"].lower()


def test_contract_catalog_attach_failure_raises_and_logs_error(base_context, monkeypatch):
    """Requirement 5: If init_iceberg_table returns None (catalog attach failure without table-not-found error),
    it raises RuntimeError and records status 'error'.
    """
    monkeypatch.setattr("backend.core.iceberg.init_iceberg_table", MagicMock(return_value=None))

    metadata._run_metadata_sync("svc-meta-contract")

    base_context["log_run"].assert_called_once()
    args, kwargs = base_context["log_run"].call_args
    assert args[3] == "error"
    assert "could not attach the DuckLake catalog" in kwargs["error_message"]
    assert "Metadata sync failed" in kwargs["summary"]


# ── Requirement 6: Time Range Bounds Management ─────────────────────────────


def test_contract_explicit_time_range_persisted_to_config(base_context, monkeypatch):
    """Requirement 6: Passing explicit start_time and/or end_time persists the bounds into
    cfg['provisioning']['time_range'] via mutate_config and updates src['time_range'].
    """
    monkeypatch.setattr("backend.core.iceberg.init_iceberg_table", MagicMock(return_value=True))
    monkeypatch.setattr("backend.core.iceberg.ducklake_table_exists", MagicMock(return_value=True))
    sync_mock = MagicMock(return_value={"files_downloaded": 0, "rows_downloaded": 0})
    monkeypatch.setattr("backend.core.iceberg.sync_data", sync_mock)
    monkeypatch.setattr("backend.core.iceberg.update_iceberg_view", MagicMock())
    monkeypatch.setattr("backend.state_sync.import_admin_state", MagicMock())

    metadata._run_metadata_sync(
        "svc-meta-contract",
        run_id=12,
        start_time="2026-06-01T00:00:00Z",
        end_time="2026-06-15T00:00:00Z",
    )

    save_cfg = base_context["save_cfg"]
    assert save_cfg.called
    saved_cfg = base_context["saved_cfgs"][-1]
    tr = saved_cfg["provisioning"]["time_range"]
    assert tr["start"] == "2026-06-01T00:00:00Z"
    assert tr["end"] == "2026-06-15T00:00:00Z"
    assert base_context["src"]["time_range"] == tr
    assert sync_mock.call_args[1]["start_time"] == "2026-06-01T00:00:00Z"
    assert sync_mock.call_args[1]["end_time"] == "2026-06-15T00:00:00Z"


def test_contract_manual_sync_all_clears_pinned_time_range(base_context, monkeypatch):
    """Requirement 6: Manual trigger (run_id is not None) without start_time represents 'Sync All',
    clearing any previously pinned time_range limit from config and setting src['time_range'] = None.
    """
    base_context["cfg"]["provisioning"]["time_range"] = {"start": "2026-01-01T00:00:00Z"}
    monkeypatch.setattr("backend.core.iceberg.init_iceberg_table", MagicMock(return_value=True))
    monkeypatch.setattr("backend.core.iceberg.ducklake_table_exists", MagicMock(return_value=True))
    sync_mock = MagicMock(return_value={"files_downloaded": 1, "rows_downloaded": 10})
    monkeypatch.setattr("backend.core.iceberg.sync_data", sync_mock)
    monkeypatch.setattr("backend.core.iceberg.update_iceberg_view", MagicMock())
    monkeypatch.setattr("backend.state_sync.import_admin_state", MagicMock())

    metadata._run_metadata_sync("svc-meta-contract", run_id=999)

    save_cfg = base_context["save_cfg"]
    assert save_cfg.called
    saved_cfg = base_context["saved_cfgs"][-1]
    assert "time_range" not in saved_cfg["provisioning"]
    assert base_context["src"]["time_range"] is None


def test_contract_scheduled_run_respects_existing_config_time_range(base_context, monkeypatch):
    """Requirement 6: Scheduled run (run_id is None) reads and passes configured provisioning.time_range.start
    to sync_data.
    """
    base_context["cfg"]["provisioning"]["time_range"] = {"start": "2026-05-01T00:00:00Z"}
    monkeypatch.setattr("backend.core.iceberg.init_iceberg_table", MagicMock(return_value=True))
    monkeypatch.setattr("backend.core.iceberg.ducklake_table_exists", MagicMock(return_value=True))
    sync_mock = MagicMock(return_value={"files_downloaded": 0, "rows_downloaded": 0})
    monkeypatch.setattr("backend.core.iceberg.sync_data", sync_mock)
    monkeypatch.setattr("backend.core.iceberg.update_iceberg_view", MagicMock())
    monkeypatch.setattr("backend.state_sync.import_admin_state", MagicMock())

    metadata._run_metadata_sync("svc-meta-contract")

    assert sync_mock.call_args[1]["start_time"] == "2026-05-01T00:00:00Z"


# ── Requirement 7: Admin State Sync Resilience ──────────────────────────────


def test_contract_admin_state_import_failure_yields_warning_status(base_context, monkeypatch):
    """Requirement 7: import_admin_state failure does not crash metadata sync; it logs a warning event,
    allows view rebuild to complete, and records run_status = 'warning' in cron_runs.
    """
    monkeypatch.setattr("backend.core.iceberg.init_iceberg_table", MagicMock(return_value=True))
    monkeypatch.setattr("backend.core.iceberg.ducklake_table_exists", MagicMock(return_value=True))
    monkeypatch.setattr(
        "backend.core.iceberg.sync_data",
        MagicMock(return_value={"files_downloaded": 2, "rows_downloaded": 500}),
    )
    update_view = MagicMock()
    monkeypatch.setattr("backend.core.iceberg.update_iceberg_view", update_view)
    monkeypatch.setattr(
        "backend.state_sync.import_admin_state",
        MagicMock(side_effect=RuntimeError("connection refused to admin state bucket")),
    )

    metadata._run_metadata_sync("svc-meta-contract")

    update_view.assert_called_once()
    base_context["refresh_status"].assert_called_once_with("svc-meta-contract")
    base_context["log_run"].assert_called_once()
    args, kwargs = base_context["log_run"].call_args
    assert args[3] == "warning"
    assert "admin state import warning" in kwargs["summary"]
    assert "connection refused" in kwargs["summary"]
    assert kwargs["files_downloaded"] == 2
    assert kwargs["rows_ingested"] == 500


# ── Requirement 8: Zero Class A Cloud Mutations & Attribution Contract ──────


def test_contract_zero_class_a_cloud_mutations(base_context, monkeypatch):
    """Requirement 8: Analyst standalone instance executes zero FOS Class A PUT or DELETE operations
    during sync_data.
    """
    mock_s3 = MagicMock()
    monkeypatch.setattr("backend.core.duckdb._get_fos_client", lambda src: mock_s3)
    monkeypatch.setattr("backend.core.iceberg.init_iceberg_table", MagicMock(return_value=True))
    monkeypatch.setattr("backend.core.iceberg.ducklake_table_exists", MagicMock(return_value=True))
    monkeypatch.setattr("backend.core.iceberg.update_iceberg_view", MagicMock())
    monkeypatch.setattr("backend.state_sync.import_admin_state", MagicMock())

    from backend.core import iceberg as db_iceberg

    data_res = db_iceberg.sync_data(base_context["src"])
    assert data_res == {"files_downloaded": 0, "rows_downloaded": 0}

    # Assert zero Class A mutations
    mock_s3.put_object.assert_not_called()
    mock_s3.delete_object.assert_not_called()
    mock_s3.delete_objects.assert_not_called()
    mock_s3.create_multipart_upload.assert_not_called()


def test_contract_telemetry_attribution_and_dashboard_cache_invalidation(base_context, monkeypatch):
    """Requirement 8: View refresh executes update_iceberg_view, logs cron_runs entry with files/rows,
    and invalidates dashboard cache via invalidate_service.
    """
    monkeypatch.setattr("backend.core.iceberg.init_iceberg_table", MagicMock(return_value=True))
    monkeypatch.setattr("backend.core.iceberg.ducklake_table_exists", MagicMock(return_value=True))
    monkeypatch.setattr(
        "backend.core.iceberg.sync_data",
        MagicMock(return_value={"files_downloaded": 3, "rows_downloaded": 2_500}),
    )
    update_view = MagicMock()
    monkeypatch.setattr("backend.core.iceberg.update_iceberg_view", update_view)
    monkeypatch.setattr("backend.state_sync.import_admin_state", MagicMock())
    invalidate_mock = MagicMock()
    monkeypatch.setattr("backend.repositories.dashboard.invalidate_service", invalidate_mock)

    metadata._run_metadata_sync("svc-meta-contract")

    update_view.assert_called_once()
    invalidate_mock.assert_called_once_with(base_context["src"]["name"])
    base_context["log_run"].assert_called_once()
    args, kwargs = base_context["log_run"].call_args
    assert args[1] == "metadata_sync"
    assert args[3] == "success"
    assert kwargs["files_downloaded"] == 3
    assert kwargs["rows_ingested"] == 2_500
    assert "downloaded 3 new Iceberg data file(s)" in kwargs["summary"]


# ── Requirement 9: Guaranteed Cleanup Contract ──────────────────────────────


def test_contract_guaranteed_cleanup_on_success_and_error(base_context, monkeypatch):
    """Requirement 9: end_progress(run_id) and finalize_cron_duration(src, run_id, start_time_exec)
    are guaranteed to execute in a finally block across both successful and error executions.
    """
    # Case A: Success execution
    monkeypatch.setattr("backend.core.iceberg.init_iceberg_table", MagicMock(return_value=True))
    monkeypatch.setattr("backend.core.iceberg.ducklake_table_exists", MagicMock(return_value=True))
    monkeypatch.setattr(
        "backend.core.iceberg.sync_data",
        MagicMock(return_value={"files_downloaded": 0, "rows_downloaded": 0}),
    )
    monkeypatch.setattr("backend.core.iceberg.update_iceberg_view", MagicMock())
    monkeypatch.setattr("backend.state_sync.import_admin_state", MagicMock())

    metadata._run_metadata_sync("svc-meta-contract", run_id=101)

    base_context["end_prog"].assert_called_with(101)
    base_context["finalize_dur"].assert_called_once()

    # Case B: Error execution
    base_context["end_prog"].reset_mock()
    base_context["finalize_dur"].reset_mock()
    monkeypatch.setattr(
        "backend.core.iceberg.sync_data",
        MagicMock(side_effect=RuntimeError("disk write error")),
    )

    metadata._run_metadata_sync("svc-meta-contract", run_id=102)

    base_context["end_prog"].assert_called_with(102)
    base_context["finalize_dur"].assert_called_once()
    args, kwargs = base_context["log_run"].call_args
    assert args[3] == "error"
    assert "disk write error" in kwargs["error_message"]


# ── Requirement 10: Manual Trigger Endpoints & Admin Controls ────────────────


def test_contract_rebuild_local_view_endpoint_lifecycle():
    """Requirement 10: POST /api/admin/rebuild-local-view returns 202 Accepted, clears source caches,
    removes snapshot_files_cache.json, starts cron run, starts progress, and launches background worker.
    """
    test_src = _fake_src("svc-rebuild-test")

    with (
        patch("backend.core.iceberg.clear_source_caches") as mock_clear,
        patch("backend.core.duckdb._cache_dir", return_value="/tmp/test_cache"),
        patch("os.path.exists", return_value=True),
        patch("os.remove") as mock_remove,
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
            assert data["ok"] is True
            assert data["run_id"] == "run-789"
            assert data["message"] == "Local view rebuild started."

            mock_clear.assert_called_once_with("svc-rebuild-test")
            mock_start_cron.assert_called_once_with(test_src, "metadata_sync")
            mock_prog.assert_called_once_with("run-789", service_id="svc-rebuild-test", task="metadata_sync")
            mock_thread.assert_called_once()
        finally:
            app.dependency_overrides.pop(get_source, None)


def test_contract_rebuild_local_view_endpoint_busy_handling():
    """Requirement 10: POST /api/admin/rebuild-local-view returns HTTP 503 when start_cron_run raises
    RuntimeError (cron busy).
    """
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
            assert resp.status_code == 503
            err_data = resp.json()
            assert "cron_busy" in str(err_data)
        finally:
            app.dependency_overrides.pop(get_source, None)


# ── Additional unit tests folded from test_metadata.py ──────────────────────


def test_post_commit_metadata_sync_worker_clears_state_after_failure(monkeypatch):
    class DeferredThread:
        def __init__(self, *, target, args, **kwargs):
            self.target = target
            self.args = args

        def start(self):
            return None

    monkeypatch.setattr(metadata.threading, "Thread", DeferredThread)
    monkeypatch.setattr(
        metadata,
        "_run_post_commit_metadata_sync",
        MagicMock(side_effect=RuntimeError("unexpected failure")),
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
    monkeypatch.setattr("backend.core.iceberg.init_iceberg_table", MagicMock())
    monkeypatch.setattr(
        "backend.core.iceberg.sync_data",
        MagicMock(return_value={"files_downloaded": 0, "rows_downloaded": 0}),
    )
    monkeypatch.setattr("backend.core.iceberg.update_iceberg_view", MagicMock())
    monkeypatch.setattr("backend.state_sync.import_admin_state", MagicMock())

    metadata._run_metadata_sync("svc-meta-contract")

    args, kwargs = base_context["log_run"].call_args
    assert args[3] == "success"
    assert kwargs["files_downloaded"] == 0
    assert "new Iceberg data file" not in kwargs["summary"]


def test_lifecycle_rows_have_no_log_output_and_one_terminal_row(monkeypatch, base_context):
    monkeypatch.setattr(
        "backend.core.iceberg.init_iceberg_table",
        MagicMock(side_effect=ValueError("Table foo.bar not found in catalog")),
    )

    metadata._run_metadata_sync("svc-meta-contract")

    base_context["log_run"].assert_called_once()
    args, kwargs = base_context["log_run"].call_args
    assert args[1] == "metadata_sync"
    assert kwargs["run_id"] == 88
    assert kwargs["log_output"] is None
    base_context["end_prog"].assert_called_once_with(88)


# ── Merged from test_bot_data_refresh.py ──────────────────────────────────────────────


from backend.cron.jobs.metadata import _run_bot_data_refresh


def test_run_bot_data_refresh_records_success_with_total_entries():
    """Successful refresh records job result with total entry count and status='success'."""
    fake_results = [
        {"id": "well-known-bots", "entry_count": 100},
        {"id": "tor-exit-nodes", "entry_count": 50},
    ]

    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.utils.bot_sources.refresh_all_sources", return_value=fake_results),
        patch("backend.cron.decorators.record_job_run") as mock_record,
    ):
        _run_bot_data_refresh()

    mock_record.assert_called_once()
    args = mock_record.call_args[0]
    assert args[0] == "bot_data_refresh"
    assert args[1] == "success"
    summary = args[3]
    assert "2" in summary
    assert "150" in summary


def test_run_bot_data_refresh_records_warning_on_partial_failure():
    """When some bot sources fail, status is recorded as 'warning'."""
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

    mock_record.assert_called_once()
    args = mock_record.call_args[0]
    assert args[0] == "bot_data_refresh"
    assert args[1] == "warning"
    summary = args[3]
    assert "1 source(s) failed: tor-exit-nodes" in summary


def test_run_bot_data_refresh_records_error_on_total_failure():
    """When all bot sources fail, status is recorded as 'error'."""
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

    mock_record.assert_called_once()
    args = mock_record.call_args[0]
    assert args[0] == "bot_data_refresh"
    assert args[1] == "error"
    summary = args[3]
    assert "All 2 bot sources failed" in summary


def test_run_bot_data_refresh_skips_when_dev_mode_active():
    """When FLA_DEV_NO_CRONS=1 is set, refresh skips outbound HTTP requests."""
    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=True),
        patch("backend.utils.bot_sources.refresh_all_sources") as mock_refresh,
        patch("backend.cron.decorators.record_job_run") as mock_record,
    ):
        _run_bot_data_refresh()

    mock_refresh.assert_not_called()
    mock_record.assert_called_once()
    args = mock_record.call_args[0]
    assert args[0] == "bot_data_refresh"
    assert args[1] == "skipped"
    assert "dev_mode_no_crons" in args[3]


def test_admin_refresh_all_bot_sources_endpoints():
    """POST /api/admin/bot-sources/refresh and /api/admin/bots/refresh trigger refresh_all_sources."""
    client = TestClient(app)
    fake_results = [
        {"id": "well-known-bots", "entry_count": 100},
        {"id": "tor-exit-nodes", "entry_count": 50},
    ]

    with patch("backend.utils.bot_sources.refresh_all_sources", return_value=fake_results) as mock_refresh:
        # Test primary endpoint
        resp = client.post("/api/admin/bot-sources/refresh")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["updated_count"] == 2
        assert data["failed_count"] == 0

        # Test alias endpoint
        resp_alias = client.post("/api/admin/bots/refresh")
        assert resp_alias.status_code == 200
        data_alias = resp_alias.json()
        assert data_alias["ok"] is True
        assert data_alias["updated_count"] == 2

        assert mock_refresh.call_count == 2


# ── Merged from test_ngwaf_sync.py ──────────────────────────────────────────────


import pytest

from backend.cron.jobs.metadata import _run_ngwaf_bot_sync


@pytest.fixture
def fake_cfg():
    return {
        "service_id": "test_service",
        "name": "Test Service",
        "fastly_api_key": "test_api_key",
        "ngwaf": {
            "workspace_id": "ws_123",
        },
        "provisioning": {
            "cron_ngwaf": {
                "interval_mins": 5,
                "log_retention_days": 30,
            }
        },
    }


def test_ngwaf_sync_skips_when_dev_mode_active():
    """dev_mode_no_crons skips NGWAF sync without loading config or hitting APIs."""
    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=True),
        patch("backend.config.load_config") as mock_load,
    ):
        _run_ngwaf_bot_sync("test_service")

    mock_load.assert_not_called()


def test_ngwaf_sync_seeds_watermark_on_first_run(fake_cfg):
    """When no watermark exists, seeds current timestamp and skips fetching."""
    mock_src = {"name": "test_service", "id": "test_service"}

    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws_123"),
        patch("backend.core.duckdb.get_source_for_service", return_value=mock_src),
        patch("backend.core.duckdb.start_cron_run", return_value=1),
        patch("backend.core.duckdb.log_cron_run") as mock_log,
        patch("backend.utils.ngwaf_bot_cache.get_last_timestamp", return_value=None),
        patch("backend.utils.ngwaf_bot_cache.update_sync_watermark") as mock_update_wm,
        patch("backend.utils.ngwaf.fetch_verified_bots_paged") as mock_fetch,
    ):
        _run_ngwaf_bot_sync("test_service")

    mock_update_wm.assert_called_once()
    mock_fetch.assert_not_called()
    mock_log.assert_called_once()
    assert mock_log.call_args[0][3] == "success"
    assert "First sync" in mock_log.call_args[1]["summary"]


def test_ngwaf_sync_pages_records_and_restores_baseline_interval(fake_cfg):
    """Normal sync pages records, enriches, upserts, and maintains baseline interval."""
    mock_src = {"name": "test_service", "id": "test_service"}
    fake_records = [
        {"waf_req_id": "req_1", "bot_name": "Googlebot", "user_agent": "Googlebot/2.1"},
        {"waf_req_id": "req_2", "bot_name": "Bingbot", "user_agent": "bingbot/2.0"},
    ]

    mock_sched = MagicMock()
    mock_job = MagicMock()
    mock_sched.get_job.return_value = mock_job

    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws_123"),
        patch("backend.core.duckdb.get_source_for_service", return_value=mock_src),
        patch("backend.core.duckdb.start_cron_run", return_value=1),
        patch("backend.core.duckdb.log_cron_run") as mock_log,
        patch("backend.utils.ngwaf_bot_cache.get_last_timestamp", return_value="2026-09-01T00:00:00Z"),
        patch(
            "backend.utils.ngwaf.fetch_verified_bots_paged",
            return_value=[(fake_records, "2026-09-01T01:00:00Z", 2)],
        ),
        patch("backend.utils.ngwaf_bot_cache.upsert_bots") as mock_upsert,
        patch("backend.utils.ngwaf_bot_cache.cleanup_old_bots", return_value=0),
        patch("backend.cron.scheduler.get_scheduler", return_value=mock_sched),
    ):
        _run_ngwaf_bot_sync("test_service")

    mock_upsert.assert_called_once()
    mock_log.assert_called_once()
    assert mock_log.call_args[0][3] == "success"
    assert "Synced 2 bot record(s)" in mock_log.call_args[1]["summary"]

    # Low volume (< 500) -> restores baseline interval of 5 mins
    mock_job.reschedule.assert_called_with("interval", minutes=5)


def test_ngwaf_sync_adaptive_rescheduling_under_heavy_load(fake_cfg):
    """When synced records >= 500, adaptive rescheduling tightens interval to 2 minutes."""
    mock_src = {"name": "test_service", "id": "test_service"}
    fake_records = [{"waf_req_id": f"req_{i}", "bot_name": "Googlebot"} for i in range(500)]

    mock_sched = MagicMock()
    mock_job = MagicMock()
    mock_sched.get_job.return_value = mock_job

    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws_123"),
        patch("backend.core.duckdb.get_source_for_service", return_value=mock_src),
        patch("backend.core.duckdb.start_cron_run", return_value=1),
        patch("backend.core.duckdb.log_cron_run") as mock_log,
        patch("backend.utils.ngwaf_bot_cache.get_last_timestamp", return_value="2026-09-01T00:00:00Z"),
        patch(
            "backend.utils.ngwaf.fetch_verified_bots_paged",
            return_value=[(fake_records, "2026-09-01T01:00:00Z", 500)],
        ),
        patch("backend.utils.ngwaf_bot_cache.upsert_bots"),
        patch("backend.utils.ngwaf_bot_cache.cleanup_old_bots", return_value=0),
        patch("backend.cron.scheduler.get_scheduler", return_value=mock_sched),
    ):
        _run_ngwaf_bot_sync("test_service")

    mock_log.assert_called_once()
    assert mock_log.call_args[0][3] == "success"
    # Adaptive reschedule triggered:
    mock_job.reschedule.assert_called_with("interval", minutes=2)


def test_ngwaf_sync_classifies_auth_errors(fake_cfg):
    """Auth errors (401/403) are clearly identified in cron run summary."""
    mock_src = {"name": "test_service", "id": "test_service"}

    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws_123"),
        patch("backend.core.duckdb.get_source_for_service", return_value=mock_src),
        patch("backend.core.duckdb.start_cron_run", return_value=1),
        patch("backend.core.duckdb.log_cron_run") as mock_log,
        patch("backend.utils.ngwaf_bot_cache.get_last_timestamp", return_value="2026-09-01T00:00:00Z"),
        patch(
            "backend.utils.ngwaf.fetch_verified_bots_paged",
            side_effect=RuntimeError("Fastly API 401 Unauthorized: invalid_api_key"),
        ),
    ):
        _run_ngwaf_bot_sync("test_service")

    mock_log.assert_called_once()
    assert mock_log.call_args[0][3] == "error"
    summary = mock_log.call_args[1]["summary"]
    assert "authentication error (check Fastly API key / workspace permissions)" in summary


def test_admin_ngwaf_endpoints(fake_cfg):
    """Test POST /api/admin/ngwaf/sync/{service_id} and GET /api/admin/ngwaf/status."""
    client = TestClient(app)

    with (
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws_123"),
        patch("backend.cron.jobs.metadata._run_ngwaf_bot_sync") as mock_run,
        patch(
            "backend.utils.ngwaf_bot_cache.get_cache_stats",
            return_value={"total_cached_bots": 42, "workspaces": {"ws_123": "2026-09-01T00:00:00Z"}, "top_bots": {}},
        ),
    ):
        # Trigger sync
        sync_resp = client.post("/api/admin/ngwaf/sync/test_service")
        assert sync_resp.status_code == 200
        sync_data = sync_resp.json()
        assert sync_data["ok"] is True
        assert sync_data["service_id"] == "test_service"
        assert sync_data["stats"]["total_cached_bots"] == 42
        mock_run.assert_called_once_with("test_service")

        # Get status
        status_resp = client.get("/api/admin/ngwaf/status?service_id=test_service")
        assert status_resp.status_code == 200
        status_data = status_resp.json()
        assert status_data["ok"] is True
        assert status_data["total_cached_bots"] == 42
        assert status_data["service_configured"] is True


def test_ngwaf_sync_adaptive_rescheduling_on_budget_exceeded(fake_cfg):
    """When execution runtime exceeds 240s budget, job halts and tightens interval to 2m."""
    mock_src = {"name": "test_service", "id": "test_service"}
    fake_records = [{"waf_req_id": f"req_{i}", "bot_name": "Googlebot"} for i in range(5)]

    mock_sched = MagicMock()
    mock_job = MagicMock()
    mock_sched.get_job.return_value = mock_job

    # Advance the simulated clock when the paged fetch yields so background
    # threads (e.g. psycopg_pool maintenance) or logging calls cannot consume
    # the start timestamp before cron_run.__enter__ records _start_time.
    clock = [1000.0]

    def mock_time():
        return clock[0]

    def fake_fetch_paged(*_args, **_kwargs):
        clock[0] = 1245.0
        yield (fake_records, "2026-09-01T01:00:00Z", 5)

    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws_123"),
        patch("backend.core.duckdb.get_source_for_service", return_value=mock_src),
        patch("backend.core.duckdb.start_cron_run", return_value=1),
        patch("backend.core.duckdb.log_cron_run") as mock_log,
        patch("backend.utils.ngwaf_bot_cache.get_last_timestamp", return_value="2026-09-01T00:00:00Z"),
        patch(
            "backend.utils.ngwaf.fetch_verified_bots_paged",
            side_effect=fake_fetch_paged,
        ),
        patch("backend.utils.ngwaf_bot_cache.upsert_bots"),
        patch("backend.utils.ngwaf_bot_cache.cleanup_old_bots", return_value=0),
        patch("backend.cron.scheduler.get_scheduler", return_value=mock_sched),
        patch("time.time", side_effect=mock_time),
    ):
        _run_ngwaf_bot_sync("test_service")

    mock_log.assert_called_once()
    assert mock_log.call_args[0][3] == "success"
    summary = mock_log.call_args[1]["summary"]
    assert "budget reached — will continue next run" in summary
    assert "Synced 5 bot record(s)" in summary
    mock_job.reschedule.assert_called_with("interval", minutes=2)


def test_ngwaf_sync_progress_lifecycle(fake_cfg):
    """Verifies start_progress, end_progress, and finalize_cron_duration are called in full lifecycle."""
    mock_src = {"name": "test_service", "id": "test_service"}
    fake_records = [{"waf_req_id": "req_1", "bot_name": "Googlebot"}]

    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws_123"),
        patch("backend.core.duckdb.get_source_for_service", return_value=mock_src),
        patch("backend.core.duckdb.start_cron_run", return_value=42),
        patch("backend.core.duckdb.log_cron_run"),
        patch("backend.utils.ngwaf_bot_cache.get_last_timestamp", return_value="2026-09-01T00:00:00Z"),
        patch(
            "backend.utils.ngwaf.fetch_verified_bots_paged",
            return_value=[(fake_records, "2026-09-01T01:00:00Z", 1)],
        ),
        patch("backend.utils.ngwaf_bot_cache.upsert_bots"),
        patch("backend.utils.ngwaf_bot_cache.cleanup_old_bots", return_value=0),
        patch("backend.cron_progress.start_progress") as mock_start_prog,
        patch("backend.cron_progress.end_progress") as mock_end_prog,
        patch("backend.cron.jobs._common.finalize_cron_duration") as mock_finalize,
    ):
        _run_ngwaf_bot_sync("test_service")

    mock_start_prog.assert_called_once_with(42, service_id="test_service", task="ngwaf_sync")
    mock_end_prog.assert_called_once_with(42)
    mock_finalize.assert_called_once()
    assert mock_finalize.call_args[0][0] == mock_src
    assert mock_finalize.call_args[0][1] == 42


def test_ngwaf_sync_guaranteed_cleanup_on_error(fake_cfg):
    """Verifies end_progress and finalize_cron_duration run in finally even if an error occurs."""
    mock_src = {"name": "test_service", "id": "test_service"}

    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws_123"),
        patch("backend.core.duckdb.get_source_for_service", return_value=mock_src),
        patch("backend.core.duckdb.start_cron_run", return_value=99),
        patch("backend.core.duckdb.log_cron_run") as mock_log,
        patch("backend.utils.ngwaf_bot_cache.get_last_timestamp", return_value="2026-09-01T00:00:00Z"),
        patch("backend.utils.ngwaf.fetch_verified_bots_paged", side_effect=ValueError("Unexpected fetch failure")),
        patch("backend.cron_progress.end_progress") as mock_end_prog,
        patch("backend.cron.jobs._common.finalize_cron_duration") as mock_finalize,
    ):
        _run_ngwaf_bot_sync("test_service")

    mock_log.assert_called_once()
    assert mock_log.call_args[0][3] == "error"
    mock_end_prog.assert_called_once_with(99)
    mock_finalize.assert_called_once()
    assert mock_finalize.call_args[0][0] == mock_src
    assert mock_finalize.call_args[0][1] == 99


def test_ngwaf_sync_first_run_watermark_guaranteed_cleanup(fake_cfg):
    """Verifies end_progress and finalize_cron_duration execute even on first-run watermark seed early exit."""
    mock_src = {"name": "test_service", "id": "test_service"}

    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws_123"),
        patch("backend.core.duckdb.get_source_for_service", return_value=mock_src),
        patch("backend.core.duckdb.start_cron_run", return_value=101),
        patch("backend.core.duckdb.log_cron_run") as mock_log,
        patch("backend.utils.ngwaf_bot_cache.get_last_timestamp", return_value=None),
        patch("backend.utils.ngwaf_bot_cache.update_sync_watermark"),
        patch("backend.cron_progress.end_progress") as mock_end_prog,
        patch("backend.cron.jobs._common.finalize_cron_duration") as mock_finalize,
    ):
        _run_ngwaf_bot_sync("test_service")

    mock_log.assert_called_once()
    assert mock_log.call_args[0][3] == "success"
    mock_end_prog.assert_called_once_with(101)
    mock_finalize.assert_called_once()
    assert mock_finalize.call_args[0][0] == mock_src
    assert mock_finalize.call_args[0][1] == 101


def test_ngwaf_sync_lease_held_is_clean_skip(fake_cfg):
    mock_src = {"name": "test_service", "id": "test_service"}

    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws_123"),
        patch("backend.core.duckdb.get_source_for_service", return_value=mock_src),
        patch("backend.core.duckdb.start_cron_run", side_effect=CronLeaseHeldError("held")),
        patch("backend.core.duckdb.log_cron_run") as mock_log,
        patch("backend.utils.ngwaf.fetch_verified_bots_paged") as mock_fetch,
        patch("backend.cron_progress.end_progress") as mock_end,
    ):
        _run_ngwaf_bot_sync("test_service")

    mock_log.assert_not_called()
    mock_fetch.assert_not_called()
    mock_end.assert_not_called()


def test_ngwaf_sync_error_row_matches_legacy_shape(fake_cfg):
    mock_src = {"name": "test_service", "id": "test_service"}

    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws_123"),
        patch("backend.core.duckdb.get_source_for_service", return_value=mock_src),
        patch("backend.core.duckdb.start_cron_run", return_value=5),
        patch("backend.core.duckdb.log_cron_run") as mock_log,
        patch("backend.utils.ngwaf_bot_cache.get_last_timestamp", return_value="2026-09-01T00:00:00Z"),
        patch("backend.utils.ngwaf.fetch_verified_bots_paged", side_effect=RuntimeError("socket reset")),
    ):
        _run_ngwaf_bot_sync("test_service")

    mock_log.assert_called_once()
    args, kwargs = mock_log.call_args
    assert args[1] == "ngwaf_sync"
    assert args[3] == "error"
    assert kwargs["summary"] == "NGWAF sync failed: socket reset"
    assert kwargs["error_message"] == "socket reset"
    assert kwargs["files_downloaded"] == 0
    assert kwargs["run_id"] == 5
    assert kwargs["log_output"] is None


# ── Merged from test_rdns_enrichment.py ──────────────────────────────────────────────


import pytest

from backend.cron.jobs.metadata import _run_rdns_enrichment
from backend.utils import rdns_cache


def test_run_rdns_enrichment_records_success_with_counts():
    """Successful enrichment records resolved, errors, and discovered counts with status='success'."""
    fake_summary = {"resolved": 20, "errors": 0, "discovered": 10, "batch_limit": 200}

    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.utils.rdns_cache.enrich_batch", return_value=fake_summary),
        patch("backend.cron.decorators.record_job_run") as mock_record,
    ):
        _run_rdns_enrichment()

    mock_record.assert_called_once()
    args = mock_record.call_args[0]
    assert args[0] == "rdns_enrichment"
    assert args[1] == "success"
    summary = args[3]
    assert "resolved=20" in summary
    assert "errors=0" in summary
    assert "discovered=10" in summary


def test_run_rdns_enrichment_records_warning_on_dns_failures():
    """When resolution has errors and zero successful resolves, status is recorded as 'warning'."""
    fake_summary = {"resolved": 0, "errors": 8, "discovered": 0, "batch_limit": 200}

    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=False),
        patch("backend.utils.rdns_cache.enrich_batch", return_value=fake_summary),
        patch("backend.cron.decorators.record_job_run") as mock_record,
    ):
        _run_rdns_enrichment()

    mock_record.assert_called_once()
    args = mock_record.call_args[0]
    assert args[0] == "rdns_enrichment"
    assert args[1] == "warning"
    summary = args[3]
    assert "DNS lookup failures detected" in summary


def test_run_rdns_enrichment_skips_when_dev_mode_active():
    """When FLA_DEV_NO_CRONS=1 is set, enrichment skips DuckDB view queries and DNS resolution."""
    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=True),
        patch("backend.utils.rdns_cache.enrich_batch") as mock_enrich,
        patch("backend.cron.decorators.record_job_run") as mock_record,
    ):
        _run_rdns_enrichment()

    mock_enrich.assert_not_called()
    mock_record.assert_called_once()
    args = mock_record.call_args[0]
    assert args[0] == "rdns_enrichment"
    assert args[1] == "skipped"
    assert "dev_mode_no_crons" in args[3]


@pytest.mark.parametrize(
    ("pending_count", "expected_batch_limit"),
    [
        (0, 200),  # empty backlog defaults to standard 200 limit
        (50, 100),  # small backlog uses baseline minimum 100
        (150, 150),  # moderate small backlog scales to exact count
        (500, 500),  # medium backlog scales up to 500
        (2500, 1000),  # large backlog caps at safety ceiling 1000
    ],
)
def test_enrich_batch_dynamic_batch_sizing(pending_count, expected_batch_limit):
    """Dynamic queue depth scaling selects appropriate batch limits."""
    with (
        patch("backend.utils.rdns_cache._get_pending_count", return_value=pending_count),
        patch("backend.utils.rdns_cache._select_ips_with_status", return_value=[]),
        patch("backend.utils.rdns_cache._select_stale_ips", return_value=[]),
        patch("backend.utils.rdns_cache._discover_new_ips", return_value=0),
        patch("backend.utils.rdns_cache._maybe_reap_stale_rows"),
    ):
        res = rdns_cache.enrich_batch()
        assert res["batch_limit"] == expected_batch_limit


def test_enrich_batch_honors_explicit_limit_override():
    """When explicit limit is supplied, dynamic queue scaling is bypassed."""
    with (
        patch("backend.utils.rdns_cache._get_pending_count") as mock_count,
        patch("backend.utils.rdns_cache._select_ips_with_status", return_value=[]) as mock_select,
        patch("backend.utils.rdns_cache._select_stale_ips", return_value=[]),
        patch("backend.utils.rdns_cache._discover_new_ips", return_value=0),
        patch("backend.utils.rdns_cache._maybe_reap_stale_rows"),
    ):
        res = rdns_cache.enrich_batch(limit=42)
        mock_count.assert_not_called()
        assert res["batch_limit"] == 42
        assert mock_select.call_args[1]["limit"] == 42


def test_admin_rdns_enrich_endpoint():
    """POST /api/admin/rdns/enrich runs manual enrichment batch and returns summary."""
    client = TestClient(app)
    fake_summary = {"resolved": 5, "errors": 0, "discovered": 2, "batch_limit": 200}

    with patch("backend.utils.rdns_cache.enrich_batch", return_value=fake_summary) as mock_enrich:
        resp = client.post("/api/admin/rdns/enrich?limit=50")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["summary"] == fake_summary
        mock_enrich.assert_called_once_with(limit=50)


def test_admin_rdns_stats_endpoint():
    """GET /api/admin/rdns/stats returns cache statistics."""
    client = TestClient(app)
    fake_stats = {"total": 1200, "pending": 45, "last_enrichment_at": "2026-09-21T00:00:00Z"}

    with patch("backend.utils.rdns_cache.get_stats", return_value=fake_stats):
        resp = client.get("/api/admin/rdns/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["stats"] == fake_stats


def test_rdns_enrichment_routes_to_maintenance_executor():
    """rdns_enrichment job ID routes to the maintenance executor pool."""
    from backend.cron.scheduler import Scheduler

    s = Scheduler()
    with patch.object(s._sched, "add_job") as mock_add_job:
        s._add_job(lambda: None, "interval", minutes=5, id="rdns_enrichment")
        mock_add_job.assert_called_once()
        assert mock_add_job.call_args[1]["executor"] == "maintenance"


# ── Merged from test_share_audit_purge.py ──────────────────────────────────────────────


import pytest

from backend.core import share_db
from backend.cron.jobs.metadata import _run_share_audit_purge


@pytest.fixture
def test_share_db():
    """Isolated share DB connection for purge testing."""
    con = share_db.get_global_share_con()
    con.execute("DELETE FROM remote_sessions")
    con.execute("DELETE FROM remote_invite_claim_tokens")
    con.execute("DELETE FROM invite_services")
    con.execute("DELETE FROM remote_invites")
    con.execute("DELETE FROM remote_share_audit_logs")
    con.commit()
    yield con


def test_purge_old_audit_logs(test_share_db):
    """Audit logs older than cutoff are deleted, newer are retained."""
    con = test_share_db
    old_ts = iso_z(datetime.now(UTC) - timedelta(days=95))
    recent_ts = iso_z(datetime.now(UTC) - timedelta(days=10))

    con.execute(
        "INSERT INTO remote_share_audit_logs(timestamp, event_type, email, ip_address, details) VALUES (?,?,?,?,?)",
        (old_ts, "LOGIN_SUCCESS", "old@example.com", "1.1.1.1", "{}"),
    )
    con.execute(
        "INSERT INTO remote_share_audit_logs(timestamp, event_type, email, ip_address, details) VALUES (?,?,?,?,?)",
        (recent_ts, "LOGIN_SUCCESS", "new@example.com", "2.2.2.2", "{}"),
    )
    con.commit()

    deleted = share_db.purge_old_audit_logs(retention_days=90, con=con)
    assert deleted == 1

    remaining = con.execute("SELECT email FROM remote_share_audit_logs").fetchall()
    assert len(remaining) == 1
    assert remaining[0]["email"] == "new@example.com"


def test_purge_stale_share_records(test_share_db):
    """Expired invites, tokens, and stale sessions are pruned; active are retained."""
    con = test_share_db
    past_iso = iso_z(datetime.now(UTC) - timedelta(days=2))
    future_iso = iso_z(datetime.now(UTC) + timedelta(days=5))
    stale_session_ts = iso_z(datetime.now(UTC) - timedelta(days=35))
    active_session_ts = iso_z(datetime.now(UTC) - timedelta(hours=2))

    # Expired invite & non-expired invite
    con.execute(
        "INSERT INTO remote_invites(id, name, email, passcode, expires_at, created_at) VALUES (?,?,?,?,?,?)",
        ("inv_expired", "Expired User", "expired@test.com", "hash", past_iso, past_iso),
    )
    con.execute(
        "INSERT INTO remote_invites(id, name, email, passcode, expires_at, created_at) VALUES (?,?,?,?,?,?)",
        ("inv_active", "Active User", "active@test.com", "hash", future_iso, past_iso),
    )

    # Expired claim token
    con.execute(
        "INSERT INTO remote_invite_claim_tokens(token, invite_id, created_at, expires_at) VALUES (?,?,?,?)",
        ("tok_expired", "inv_active", past_iso, past_iso),
    )

    # Stale session (> 30 days) and active session
    con.execute(
        "INSERT INTO remote_sessions(session_id, invite_id, name, email, ip_address, user_agent, fingerprint_signature, pii_policy, login_time, last_active_time) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            "sess_stale",
            "inv_active",
            "Active User",
            "active@test.com",
            "1.1.1.1",
            "ua",
            "fp",
            "{}",
            past_iso,
            stale_session_ts,
        ),
    )
    con.execute(
        "INSERT INTO remote_sessions(session_id, invite_id, name, email, ip_address, user_agent, fingerprint_signature, pii_policy, login_time, last_active_time) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            "sess_active",
            "inv_active",
            "Active User",
            "active@test.com",
            "1.1.1.1",
            "ua",
            "fp",
            "{}",
            past_iso,
            active_session_ts,
        ),
    )
    con.commit()

    stats = share_db.purge_stale_share_records(max_idle_session_days=30, con=con)
    assert stats["deleted_expired_invites"] == 1
    assert stats["deleted_claim_tokens"] == 1
    assert stats["deleted_stale_sessions"] == 1

    # Verify active invite and active session remain
    invites = con.execute("SELECT id FROM remote_invites").fetchall()
    assert [r["id"] for r in invites] == ["inv_active"]

    sessions = con.execute("SELECT session_id FROM remote_sessions").fetchall()
    assert [r["session_id"] for r in sessions] == ["sess_active"]


def test_run_share_audit_purge_lifecycle():
    """Job reads setting, executes audit and stale purge, and returns summary."""
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
    assert "deleted=5" in detail
    assert "retention_days=60" in detail
    assert "expired_invites=2" in detail
    assert "stale_sessions=3" in detail


def test_share_audit_purge_routes_to_maintenance_executor():
    """Job ID share_audit_purge routes to maintenance executor threadpool."""
    from backend.cron.scheduler import Scheduler

    s = Scheduler()
    with patch.object(s._sched, "add_job") as mock_add_job:
        s._add_job(lambda: None, "cron", hour=3, minute=45, id="share_audit_purge")
        mock_add_job.assert_called_once()
        assert mock_add_job.call_args[1]["executor"] == "maintenance"


def test_run_share_audit_purge_skips_when_dev_mode_active():
    """FLA_DEV_NO_CRONS=1 causes job to skip execution safely."""
    with (
        patch("backend.cron.decorators.dev_mode_no_crons", return_value=True),
        patch("backend.core.share_db.purge_old_audit_logs") as mock_audit,
    ):
        result = _run_share_audit_purge.__wrapped__()
        status, detail = result if isinstance(result, tuple) else ("success", result)
        assert status == "skipped"
        assert "dev_mode_no_crons" in detail
        mock_audit.assert_not_called()


def test_admin_share_purge_endpoint():
    """POST /api/admin/share/purge triggers purge_all_share_records."""
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
        assert data["ok"] is True
        assert data["deleted_audit_logs"] == 12
        assert data["deleted_expired_invites"] == 1
        assert data["deleted_stale_sessions"] == 4
        mock_purge_all.assert_called_once_with(audit_retention_days=45, max_idle_session_days=15)


def test_admin_share_audit_logs_endpoint(test_share_db):
    """GET /api/admin/share/audit-logs returns audit log entries."""
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
    assert "audit_logs" in data
    assert any(entry["email"] == "analyst@example.com" for entry in data["audit_logs"])
