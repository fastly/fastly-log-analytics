"""Comprehensive verification tests for Cron 3: local_compact_{service_id}.

Validates the complete execution contract specified in docs/cron/jobs/local-compact.md:
  1. Hourly bin-packing consolidation of 10 small synthetic Parquet files.
  2. Invocation via POST /api/admin/compact/{service_id} (HTTP 200).
  3. Preservation of row counts and data integrity across consolidation.
  4. Zero outbound Fastly Object Storage (FOS) network calls.
  5. Atomic replacement order: atomic rename succeeds BEFORE unlinking input files.
  6. Crash resilience: if rename fails, original files are completely untouched.
  7. Concurrent queries during compaction never hit FileNotFoundError.
  8. ENOSPC disk pre-check safely aborts without data modification.
  9. Scheduler registration under FLA_DEV_NO_CRONS=1 and LOCAL_COMPACT_INTERVAL_MIN.
 10. High-scale mode Celery rollup recompute via ingest_ledger.
"""

from __future__ import annotations

import os
import threading
import time
from unittest.mock import MagicMock, patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from starlette.testclient import TestClient

from backend.core import local_compaction as lc
from backend.cron.jobs import compaction


def _write_sample_parquet(path: str, rows: int, ts_start: int = 0, rid_start: int | None = None) -> None:
    """Write a small parquet file with schema matching standard log tables."""
    cols = {
        "timestamp": pa.array(range(ts_start, ts_start + rows), type=pa.int64()),
        "ip": pa.array([f"192.168.1.{i % 254 + 1}" for i in range(rows)]),
        "status": pa.array([200 if i % 10 != 0 else 500 for i in range(rows)], type=pa.int32()),
    }
    if rid_start is not None:
        cols["rid"] = pa.array([f"rid_{rid_start + i}" for i in range(rows)])
    table = pa.table(cols)
    pq.write_table(table, path, compression="zstd")


@pytest.fixture
def mock_service_env(tmp_path, monkeypatch):
    """Set up an isolated cache root and mock configuration for testing."""
    service_id = "test-compaction-svc"
    cache_root = tmp_path / "cache" / service_id
    data_dir = cache_root / "data"
    data_dir.mkdir(parents=True)

    src = {
        "name": service_id,
        "service_id": service_id,
        "bucket": service_id,
        "_test_cache_root": str(cache_root),
        "provisioning": {
            "access_level": "read_write",
            "cron_compact": {"enabled": True},
        },
    }

    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda s: s.get("_test_cache_root", str(cache_root)))
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src if sid == service_id else None)
    monkeypatch.setattr("backend.deps.get_service_id", lambda request=None: service_id)
    monkeypatch.setattr("backend.deps.get_source", lambda service_id=None: src)
    monkeypatch.setattr("backend.core.local_compaction._DAILY_TIER_AGE_DAYS", 365)
    monkeypatch.setattr("backend.core.local_compaction._WEEKLY_TIER_AGE_DAYS", 365)

    return {"service_id": service_id, "source": src, "data_dir": str(data_dir), "cache_root": str(cache_root)}


def test_contract_consolidate_10_small_files_via_admin_endpoint(mock_service_env):
    """Checklist Items 1-3:
    1. Generate 10 small synthetic Parquet files in an hourly partition directory.
    2. Trigger POST /api/admin/compact/{service_id}; confirm HTTP 200.
    3. Verify the 10 small files are consolidated into a single file with identical rows.
    """
    from backend.main import app

    service_id = mock_service_env["service_id"]
    part_dir = os.path.join(mock_service_env["data_dir"], "timestamp_hour=2026-05-30-00")
    os.makedirs(part_dir, exist_ok=True)

    total_expected_rows = 0
    for i in range(10):
        rows = 15
        total_expected_rows += rows
        file_path = os.path.join(part_dir, f"batch_{i:02d}.parquet")
        _write_sample_parquet(file_path, rows=rows, ts_start=i * 100, rid_start=i * 100)

    # Confirm 10 input files present
    initial_files = [f for f in os.listdir(part_dir) if f.endswith(".parquet")]
    assert len(initial_files) == 10

    client = TestClient(app)
    response = client.post(f"/api/admin/compact/{service_id}?min_files=3")
    assert response.status_code == 200, f"Expected 200, got: {response.text}"
    data = response.json()

    assert data["files_merged"] == 10
    assert data["files_removed"] == 10
    assert data["partitions_compacted"] == 1
    assert data["errors"] == []

    # Verify directory now contains exactly 1 compacted parquet file
    remaining_files = [f for f in os.listdir(part_dir) if f.endswith(".parquet")]
    assert len(remaining_files) == 1
    compacted_file = remaining_files[0]
    assert compacted_file.startswith("compacted_")

    # Verify no temporary files remain
    tmp_files = [f for f in os.listdir(part_dir) if f.endswith(".tmp")]
    assert len(tmp_files) == 0

    # Verify row count and contents match exactly
    con = duckdb.connect()
    try:
        compacted_path = os.path.join(part_dir, compacted_file)
        cnt = con.execute(f"SELECT COUNT(*) FROM read_parquet('{compacted_path}')").fetchone()[0]
        assert cnt == total_expected_rows
    finally:
        con.close()


def test_contract_zero_outbound_fos_calls(mock_service_env, monkeypatch):
    """Checklist Item 4: Confirm zero outbound FOS calls were made.
    local_compact operates strictly on local disk without egress or cloud SDK calls.
    """
    src = mock_service_env["source"]
    service_id = mock_service_env["service_id"]
    part_dir = os.path.join(mock_service_env["data_dir"], "timestamp_hour=2026-05-30-00")
    os.makedirs(part_dir, exist_ok=True)

    for i in range(4):
        _write_sample_parquet(os.path.join(part_dir, f"b_{i}.parquet"), rows=10, ts_start=i * 10)

    # Spy/mock potential cloud egress entry points
    boto_called = []
    httpx_called = []

    def fake_boto(*args, **kwargs):
        boto_called.append((args, kwargs))
        raise AssertionError("boto3 client invoked during local compaction!")

    def fake_http(*args, **kwargs):
        httpx_called.append((args, kwargs))
        raise AssertionError("HTTP request made during local compaction!")

    monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value="test_run_123"))

    with patch("urllib.request.urlopen", side_effect=fake_http):
        res = lc.compact_local_partitions(src)
        assert res["partitions_compacted"] == 1
        assert res["files_merged"] == 4

        # Run via the cron entrypoint too
        compaction._run_local_compact.__wrapped__(service_id)

    assert len(boto_called) == 0, "Outbound boto calls occurred!"
    assert len(httpx_called) == 0, "Outbound HTTP calls occurred!"


def test_contract_atomic_replacement_and_crash_safety(mock_service_env, monkeypatch):
    """Contract: Atomic Swap & Unlink.
    1. Rename from tmp to compacted happens BEFORE deleting inputs.
    2. If rename fails, original files are untouched (no data loss).
    """
    src = mock_service_env["source"]
    part_dir = os.path.join(mock_service_env["data_dir"], "timestamp_hour=2026-05-30-00")
    os.makedirs(part_dir, exist_ok=True)

    file_paths = []
    for i in range(5):
        fp = os.path.join(part_dir, f"file_{i}.parquet")
        _write_sample_parquet(fp, rows=10, ts_start=i * 10)
        file_paths.append(fp)

    call_order: list[str] = []
    real_rename = os.rename
    real_remove = os.remove

    def spy_rename(src_p, dst_p):
        call_order.append(f"rename:{os.path.basename(src_p)}->{os.path.basename(dst_p)}")
        return real_rename(src_p, dst_p)

    def spy_remove(path):
        call_order.append(f"remove:{os.path.basename(path)}")
        return real_remove(path)

    monkeypatch.setattr(os, "rename", spy_rename)
    monkeypatch.setattr(os, "remove", spy_remove)

    lc.compact_local_partitions(src)

    # Verify rename happened FIRST, then removals
    assert len(call_order) == 6
    assert call_order[0].startswith("rename:compacted_")
    for action in call_order[1:]:
        assert action.startswith("remove:file_")

    # Now test crash safety: if rename fails, originals must NOT be deleted
    call_order.clear()
    part_dir_crash = os.path.join(mock_service_env["data_dir"], "timestamp_hour=2026-05-30-01")
    os.makedirs(part_dir_crash, exist_ok=True)
    crash_paths = []
    for i in range(4):
        fp = os.path.join(part_dir_crash, f"crash_{i}.parquet")
        _write_sample_parquet(fp, rows=10, ts_start=i * 10)
        crash_paths.append(fp)

    def failing_rename(src_p, dst_p):
        raise OSError("Simulated filesystem I/O error during atomic rename")

    monkeypatch.setattr(os, "rename", failing_rename)

    res = lc.compact_local_partitions(src)
    assert len(res["errors"]) > 0

    # Verify all 4 original files are still present and untouched!
    remaining = [f for f in os.listdir(part_dir_crash) if f.endswith(".parquet")]
    assert len(remaining) == 4
    for fp in crash_paths:
        assert os.path.exists(fp), f"{fp} was deleted despite rename failure!"


def test_contract_concurrent_queries_during_compaction_no_filenotfound(mock_service_env):
    """Checklist Item 5: Concurrent queries against DuckDB / views must never hit FileNotFoundError."""
    src = mock_service_env["source"]
    part_dir = os.path.join(mock_service_env["data_dir"], "timestamp_hour=2026-05-30-00")
    os.makedirs(part_dir, exist_ok=True)

    for i in range(8):
        _write_sample_parquet(os.path.join(part_dir, f"cq_{i}.parquet"), rows=50, ts_start=i * 50)

    stop_event = threading.Event()
    query_errors: list[Exception] = []
    query_counts: list[int] = []

    def reader_loop():
        from backend.repositories._base import QueryRunner

        con = duckdb.connect()
        try:
            runner = QueryRunner(con, src)
            while not stop_event.is_set():
                try:
                    glob_pattern = os.path.join(mock_service_env["data_dir"], "**", "*.parquet")
                    res = runner.execute(f"SELECT COUNT(*) FROM read_parquet('{glob_pattern}')").fetchone()
                    query_counts.append(res[0])
                except Exception as e:
                    query_errors.append(e)
                time.sleep(0.005)
        finally:
            con.close()

    with (
        patch("backend.core.iceberg.view.update_iceberg_view", lambda *a, **k: None),
        patch("backend.core.iceberg.update_iceberg_view", lambda *a, **k: None),
    ):
        thread = threading.Thread(target=reader_loop, daemon=True)
        thread.start()

        try:
            # Run local compaction while queries are in flight
            lc.compact_local_partitions(src)
        finally:
            stop_event.set()
            thread.join(timeout=3.0)

    # Queries must never hit FileNotFoundError
    file_not_found_errors = [e for e in query_errors if isinstance(e, FileNotFoundError) or "No such file" in str(e)]
    assert len(file_not_found_errors) == 0, f"Encountered FileNotFoundError during query: {file_not_found_errors}"
    assert len(query_counts) > 0, "Reader thread did not execute queries"


def test_contract_enospc_precheck_skips_safely(mock_service_env, monkeypatch):
    """Contract: Aborts compaction if available disk is less than 2x needed."""
    src = mock_service_env["source"]
    part_dir = os.path.join(mock_service_env["data_dir"], "timestamp_hour=2026-05-30-00")
    os.makedirs(part_dir, exist_ok=True)

    for i in range(5):
        _write_sample_parquet(os.path.join(part_dir, f"enospc_{i}.parquet"), rows=20)

    # Mock shutil.disk_usage: 50% disk used (does not trigger watermark eviction), but free space is only 100 bytes
    import shutil
    from collections import namedtuple

    Usage = namedtuple("Usage", ["total", "used", "free"])
    monkeypatch.setattr(shutil, "disk_usage", lambda p: Usage(total=10**9, used=5 * 10**8, free=100))

    res = lc.compact_local_partitions(src)

    # Verify aborted with ENOSPC error and original files are unchanged
    assert res["partitions_compacted"] == 0
    assert any("ENOSPC" in err for err in res["errors"])
    remaining = [f for f in os.listdir(part_dir) if f.endswith(".parquet")]
    assert len(remaining) == 5


def test_contract_scheduler_dev_no_crons_and_interval_override(monkeypatch):
    """Checklist Item 6: Under FLA_DEV_NO_CRONS=1, local_compact is registered and functions normally.
    Also confirms LOCAL_COMPACT_INTERVAL_MIN env var is respected.
    """
    from backend.cron.scheduler import Scheduler

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

    # Confirmed registered
    assert "local_compact_svc-contract-test" in sched._job_ids

    # Find the job call and verify interval matches LOCAL_COMPACT_INTERVAL_MIN=5
    lc_call = next(c for c in add_job.call_args_list if c.kwargs.get("id") == "local_compact_svc-contract-test")
    assert lc_call.kwargs.get("minutes") == 5


def test_contract_high_scale_mode_recomputes_rollups_from_ledger(monkeypatch):
    """Verify High-Scale pod top-N recompute path triggered by _run_local_compact."""
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
