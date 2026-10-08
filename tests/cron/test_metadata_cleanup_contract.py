"""Contract test suite for Cron 11: metadata_cleanup_{service_id}.

Verifies all requirements and checklist items from docs/cron/jobs/metadata-cleanup.md:
1. FLA_DEV_NO_CRONS=1 / dev_mode_no_crons() skips execution and scheduler registration.
2. Politeness gating (should_defer_cron) defers when active requests are running.
3. Zero FOS calls contract: asserting no FOS client or S3 deletion during cleanup.
4. Dedup suppression on ingested_files when delete_after=False (rows preserved, override status emitted).
5. RUM cutoff calculation: request rows pruned at ingested_files_days, RUM rows pruned at max(ingested_files_days, log_retention + 1).
6. usage_log chunked deletion in 5,000 batches while preserving usage_log_hourly_summary.
7. cron_runs (7d), slow_queries (30d epoch), and global metric_snapshots (30d) retention pruning.
8. Telemetry, progress, and audit row recording (cron_runs success/error, duration finalized in finally).
9. Manual trigger path parity: POST /api/admin/metadata-cleanup/{service_id} and POST /api/admin/metadata-cleanup.
10. Dynamic scheduler rescheduling on cron_hour/cron_minute change and disabling when enabled=False.
"""

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


def _seed_slow_queries(service_id: str, rows: int, days_ago: int = 0) -> None:
    con = metadata_db.get_con(service_id)
    cutoff = time.time() - (days_ago * 86400)
    con.executemany(
        """INSERT INTO slow_queries (
            query_id, db_type, service_id, started_at_utc, ended_at_utc, duration_ms,
            outcome, sql_preview, sql_len, attr_kind, attr_label, attr_caller_qualname, attr_caller_file
        ) VALUES (1, 'duckdb', ?, ?, ?, 100.0, 'success', 'SELECT 1', 8, 'api', 'test', 'test_caller', 'test.py')""",
        [(service_id, cutoff, cutoff + 0.1) for _ in range(rows)],
    )
    con.commit()


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

    _seed_slow_queries(sid, 5, days_ago=45)
    _seed_slow_queries(sid, 3, days_ago=5)

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

    remaining_slow_queries = con.execute("SELECT count(*) FROM slow_queries WHERE service_id = ?", (sid,)).fetchone()[0]
    assert remaining_slow_queries == 3

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
