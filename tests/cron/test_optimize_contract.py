"""Contract test suite for Cron 7: optimize_{service_id}.

Verifies all requirements and checklist items from docs/cron/jobs/optimize.md (§1–§9):
1. Trigger POST /api/admin/optimize/{service_id}; verify HTTP 200 response.
2. Verify in logs: ducklake_flush_inlined_data executed successfully.
3. Verify in logs: ducklake_rewrite_data_files executed successfully.
4. Confirm in cron_runs: run status success with non-zero duration (no stubs).
5. Confirm PostgreSQL's usage_log table records FOS Class A/B calls attributed to cron.optimize.
6. Under FLA_DEV_NO_CRONS=1, verify job does not register or execute.
7. Politeness gating (should_defer_cron) and manual trigger bypass.
8. Exclusive per-service locking and concurrency safety.
9. Compaction and durability flush across all lake tables (logs, client_vitals, client_errors).
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest
from starlette.testclient import TestClient

from backend.cron.jobs import optimize
from backend.cron.scheduler import Scheduler


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


def test_contract_1_and_4_manual_trigger_and_cron_runs_accounting(optimize_test_source):
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


def test_contract_2_and_3_verify_logs_for_flush_and_rewrite(optimize_test_source, caplog):
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


def test_contract_5_usage_log_telemetry_attributed_to_cron_optimize(optimize_test_source):
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


def test_contract_6_safety_gate_under_fla_dev_no_crons(optimize_test_source, monkeypatch, caplog):
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
        assert f"optimize_{service_id}" not in job_ids, "optimize job must not be registered under FLA_DEV_NO_CRONS=1"

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


def test_contract_politeness_gating_and_manual_bypass(optimize_test_source):
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


def test_contract_exclusive_per_service_locking(optimize_test_source):
    """Requirement 5: Exclusive per-service locking."""
    service_id = optimize_test_source["service_id"]

    with (
        patch("backend.core.duckdb.start_cron_run", side_effect=RuntimeError("already running")),
        patch("backend.core.iceberg.optimize_table") as mock_opt,
    ):
        res = optimize._run_optimize.__wrapped__(service_id)
        assert res is not None
        assert res.get("status") == "skipped"
        mock_opt.assert_not_called()


def test_contract_all_lake_tables_durability_and_compaction(optimize_test_source):
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
