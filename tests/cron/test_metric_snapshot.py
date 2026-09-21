"""Tests for ``backend.cron.jobs.metric_snapshot._run_metric_snapshot``."""

from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from backend.core import metric_snapshots
from backend.cron.jobs import metric_snapshot as ms_mod


@pytest.fixture(autouse=True)
def clean_db(tmp_path, monkeypatch):
    """Use a temporary sqlite database for system metrics."""
    test_db = tmp_path / "system_metrics.db"
    monkeypatch.setattr(metric_snapshots, "_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(metric_snapshots, "_use_postgres", lambda: False)
    metric_snapshots.close_all_connections()
    yield
    metric_snapshots.close_all_connections()


def test_metric_snapshot_samples_vitals(monkeypatch):
    """Test that _run_metric_snapshot samples all operational vitals and records them."""
    monkeypatch.setattr(
        "backend.core.duckdb_pool.get_all_stats",
        lambda: [{"service": "svc_test", "wait": {"p95_ms": 12.5}}],
    )
    monkeypatch.setattr(
        "backend.core.query_registry.query_registry.summary",
        lambda: {"active_total": 3},
    )
    monkeypatch.setattr(
        "backend.core.ducklake_admission.get_admission_stats",
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

    result = ms_mod._run_metric_snapshot.__wrapped__()
    assert "sampled=" in result

    batch = metric_snapshots.get_batch(since="1h")
    assert "pool_wait_p95_ms|svc_test" in batch
    assert batch["pool_wait_p95_ms|svc_test"][-1]["value"] == 12.5
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
    # Insert success, error, and warning runs
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


def test_safe_record_handles_exceptions_gracefully(monkeypatch):
    """Verify that a failing metric recording does not raise or interrupt the tick."""
    with patch("backend.core.metric_snapshots.record_snapshot", side_effect=RuntimeError("db locked")):
        assert ms_mod._safe_record("test_metric", 10.0) is False
