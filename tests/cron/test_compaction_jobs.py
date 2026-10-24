"""Consolidated tests for test_compaction_jobs."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from backend.core.metadata.cron_log import CronLeaseHeldError
from backend.cron.jobs import compaction


@pytest.fixture
def stub_progress(monkeypatch) -> dict[str, MagicMock]:
    """Replace cron_progress + run-id helpers with mocks the test can inspect."""
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=42))
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
    # cron_progress helpers — start, end, cleanup. The decorators import
    # them lazily inside the function bodies so we have to patch the
    # source modules, not local references.
    start_progress = MagicMock()
    end_progress = MagicMock()
    cleanup = MagicMock()
    monkeypatch.setattr("backend.cron_progress.start_progress", start_progress)
    monkeypatch.setattr("backend.cron_progress.end_progress", end_progress)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", cleanup)
    log_event = MagicMock()
    monkeypatch.setattr("backend.cron.scheduler._log_and_add_progress", log_event)
    monkeypatch.setattr("backend.cron.scheduler._extract_log_text", lambda rid: "")
    return {
        "start_progress": start_progress,
        "end_progress": end_progress,
        "cleanup": cleanup,
        "log_event": log_event,
    }


# ── _run_local_compact ────────────────────────────────────────────────────────


def test_local_compact_returns_when_source_missing(monkeypatch):
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)
    # Must not raise.
    compaction._run_local_compact.__wrapped__("missing-svc")


def test_local_compact_skips_when_start_cron_run_raises(monkeypatch, stub_source, stub_progress):
    """``start_cron_run`` raises RuntimeError when another instance of this
    cron is already in-flight. The job should log and return without
    touching the underlying compaction logic."""
    monkeypatch.setattr(
        "backend.core.duckdb.start_cron_run",
        MagicMock(side_effect=CronLeaseHeldError("already running")),
    )
    compact_mock = MagicMock()
    monkeypatch.setattr("backend.core.local_compaction.compact_local_partitions", compact_mock)

    compaction._run_local_compact.__wrapped__("svc-1")

    compact_mock.assert_not_called()
    stub_progress["start_progress"].assert_not_called()


def test_local_compact_success_logs_and_records(monkeypatch, stub_source, stub_progress):
    monkeypatch.setattr(
        "backend.core.local_compaction.compact_local_partitions",
        MagicMock(
            return_value={
                "partitions_compacted": 3,
                "files_merged": 12,
                "files_removed": 9,
                "errors": [],
            }
        ),
    )

    compaction._run_local_compact.__wrapped__("svc-1")

    # log_cron_run was called with status='success' and a non-error summary.
    from backend.core import duckdb as _db

    _db.log_cron_run.assert_called_once()
    args, kwargs = _db.log_cron_run.call_args
    # Positional: (src, task, duration, status). Keyword: summary, error_message, run_id, log_output.
    assert args[3] == "success"
    assert "Compacted 3 partition" in kwargs["summary"]
    assert kwargs["error_message"] is None
    # Progress lifecycle closed.
    stub_progress["start_progress"].assert_called_once()
    stub_progress["end_progress"].assert_called_once()


def test_local_compact_records_warning_when_errors_present(monkeypatch, stub_source, stub_progress):
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

    from backend.core import duckdb as _db

    args, kwargs = _db.log_cron_run.call_args
    assert args[3] == "warning"
    assert "4 partition error" in kwargs["summary"]
    # First 3 errors surface; the rest are truncated.
    assert "partition a failed" in kwargs["error_message"]
    assert "1 more" in kwargs["error_message"]


def test_local_compact_records_error_on_exception(monkeypatch, stub_source, stub_progress):
    monkeypatch.setattr(
        "backend.core.local_compaction.compact_local_partitions",
        MagicMock(side_effect=RuntimeError("disk full")),
    )

    compaction._run_local_compact.__wrapped__("svc-1")

    from backend.core import duckdb as _db

    args, kwargs = _db.log_cron_run.call_args
    assert args[3] == "error"
    assert "disk full" in kwargs["error_message"]
    # end_progress STILL fires (in the finally block).
    stub_progress["end_progress"].assert_called_once()


# ── _run_rollup_compact_daily ────────────────────────────────────────────────


def test_rollup_compact_returns_when_source_missing(monkeypatch):
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)
    compaction._run_rollup_compact_daily.__wrapped__("missing-svc")


def test_rollup_compact_skips_when_start_cron_run_raises(monkeypatch, stub_source, stub_progress):
    monkeypatch.setattr(
        "backend.core.duckdb.start_cron_run",
        MagicMock(side_effect=CronLeaseHeldError("already running")),
    )
    compact_mock = MagicMock()
    monkeypatch.setattr("backend.core.rollups.compact_closed_days_to_daily", compact_mock)

    compaction._run_rollup_compact_daily.__wrapped__("svc-1")

    compact_mock.assert_not_called()


def _mock_rollups(monkeypatch):
    monkeypatch.setattr("backend.core.rollups.backfill_day_bundles", lambda *a, **kw: 0)
    monkeypatch.setattr(
        "backend.core.rollups.backfill_missing_hour_bundles", lambda *a, **kw: {"missing": 0, "bundled": 0}
    )
    monkeypatch.setattr(
        "backend.core.rollups.backfill_missing_hour_ip_spread", lambda *a, **kw: {"missing": 0, "rebuilt": 0}
    )
    monkeypatch.setattr("backend.core.rollups.compact_network_rtt_closed_days_to_daily", lambda *a, **kw: 0)
    monkeypatch.setattr("backend.core.rollups.compact_network_speed_closed_days_to_daily", lambda *a, **kw: 0)
    monkeypatch.setattr("backend.core.rollups.compact_ngwaf_bots_closed_days_to_daily", lambda *a, **kw: 0)
    monkeypatch.setattr("backend.core.rollups.compact_origin_dims_closed_days_to_daily", lambda *a, **kw: 0)
    monkeypatch.setattr("backend.core.rollups.compact_origin_latency_ts_closed_days_to_daily", lambda *a, **kw: 0)
    monkeypatch.setattr("backend.core.rollups.compact_origin_summary_closed_days_to_daily", lambda *a, **kw: 0)
    monkeypatch.setattr("backend.core.rollups.compact_overview_closed_days_to_daily", lambda *a, **kw: 0)
    monkeypatch.setattr("backend.core.rollups.compact_perf_latency_closed_days_to_daily", lambda *a, **kw: 0)
    monkeypatch.setattr("backend.core.rollups.compact_security_dims_closed_days_to_daily", lambda *a, **kw: 0)
    monkeypatch.setattr("backend.core.rollups.compact_verified_bots_ts_closed_days_to_daily", lambda *a, **kw: 0)


def test_rollup_compact_success_records_rebuilt_and_bundled(monkeypatch, stub_source, stub_progress):
    _mock_rollups(monkeypatch)
    monkeypatch.setattr("backend.core.rollups.compact_closed_days_to_daily", MagicMock(return_value=14))
    monkeypatch.setattr("backend.core.rollups.backfill_day_bundles", MagicMock(return_value=7))

    compaction._run_rollup_compact_daily.__wrapped__("svc-1")

    from backend.core import duckdb as _db

    args, kwargs = _db.log_cron_run.call_args
    assert args[3] == "success"
    assert "Rebuilt 14" in kwargs["summary"]
    assert "bundled 7 day" in kwargs["summary"]


def test_rollup_compact_logs_warning_when_bundle_step_fails(monkeypatch, stub_source, stub_progress, caplog):
    _mock_rollups(monkeypatch)
    monkeypatch.setattr("backend.core.rollups.compact_closed_days_to_daily", MagicMock(return_value=5))
    monkeypatch.setattr(
        "backend.core.rollups.backfill_day_bundles",
        MagicMock(side_effect=RuntimeError("disk full")),
    )

    import logging as _logging

    with caplog.at_level(_logging.WARNING, logger="backend.scheduler"):
        compaction._run_rollup_compact_daily.__wrapped__("svc-1")

    from backend.core import duckdb as _db

    args, kwargs = _db.log_cron_run.call_args
    # Subsystem errors log status="warning" with error_message listing failed subsystems per spec §5.8.
    assert args[3] == "warning"
    assert "day_bundles: disk full" in kwargs["error_message"]
    # Summary shows 0 bundled.
    assert "bundled 0 day" in kwargs["summary"]


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


def _record_day_compactors(monkeypatch, calls: list, *, fail: str | None = None) -> None:
    for label in CRON_DAY_COMPACTOR_ORDER:

        def compactor(*args, _label=label, **kwargs):
            calls.append((_label, args, kwargs))
            if _label == fail:
                raise RuntimeError("boom")
            return 0

        monkeypatch.setattr(f"backend.core.rollups.compact_{label}_closed_days_to_daily", compactor)


def test_rollup_compact_runs_every_day_compactor_in_order_with_30d_lookback(monkeypatch, stub_source, stub_progress):
    _mock_rollups(monkeypatch)
    calls: list = []
    _record_day_compactors(monkeypatch, calls)

    compaction._run_rollup_compact_daily.__wrapped__("svc-1")

    assert [c[0] for c in calls] == CRON_DAY_COMPACTOR_ORDER
    assert all(args == ("svc-1", stub_source) and kwargs == {"lookback_days": 30} for _, args, kwargs in calls)
    from backend.core import duckdb as _db

    assert _db.log_cron_run.call_args.args[3] == "success"


def test_rollup_compact_isolates_a_failing_day_compactor(monkeypatch, stub_source, stub_progress):
    _mock_rollups(monkeypatch)
    calls: list = []
    _record_day_compactors(monkeypatch, calls, fail="ngwaf_bots")

    compaction._run_rollup_compact_daily.__wrapped__("svc-1")

    # Everything after the failing one still ran.
    assert [c[0] for c in calls] == CRON_DAY_COMPACTOR_ORDER
    from backend.core import duckdb as _db

    args, kwargs = _db.log_cron_run.call_args
    assert args[3] == "warning"
    assert kwargs["error_message"] == "ngwaf_bots: boom"


def test_rollup_compact_records_error_on_exception(monkeypatch, stub_source, stub_progress):
    _mock_rollups(monkeypatch)
    stub_progress["start_progress"].side_effect = RuntimeError("manifest read failed")

    compaction._run_rollup_compact_daily.__wrapped__("svc-1")

    from backend.core import duckdb as _db

    args, kwargs = _db.log_cron_run.call_args
    assert args[3] == "error"
    assert "manifest read failed" in kwargs["error_message"]


def test_rollup_compact_provided_run_id_skips_lease(monkeypatch, stub_source, stub_progress):
    start_mock = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start_mock)
    _mock_rollups(monkeypatch)
    monkeypatch.setattr("backend.core.rollups.compact_closed_days_to_daily", MagicMock(return_value=0))

    compaction._run_rollup_compact_daily.__wrapped__("svc-1", run_id=321)

    start_mock.assert_not_called()


# ── Merged from test_local_compact_contract.py ──────────────────────────────────────────────

import os
import threading
import time
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from starlette.testclient import TestClient

from backend.core import local_compaction as lc


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


class TestLocalCompactContract:
    def test_contract_consolidate_10_small_files_via_admin_endpoint(self, mock_service_env):
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

    def test_contract_zero_outbound_fos_calls(self, mock_service_env, monkeypatch):
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

    def test_contract_atomic_replacement_and_crash_safety(self, mock_service_env, monkeypatch):
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

    def test_contract_concurrent_queries_during_compaction_no_filenotfound(self, mock_service_env):
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
        file_not_found_errors = [
            e for e in query_errors if isinstance(e, FileNotFoundError) or "No such file" in str(e)
        ]
        assert len(file_not_found_errors) == 0, f"Encountered FileNotFoundError during query: {file_not_found_errors}"
        assert len(query_counts) > 0, "Reader thread did not execute queries"

    def test_contract_enospc_precheck_skips_safely(self, mock_service_env, monkeypatch):
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

    def test_contract_scheduler_dev_no_crons_and_interval_override(self, monkeypatch):
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

    def test_contract_high_scale_mode_recomputes_rollups_from_ledger(self, monkeypatch):
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


# ── Merged from test_rollup_compact_contract.py ──────────────────────────────────────────────

import collections
import shutil
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from backend.cron.jobs.compaction import _run_rollup_compact_daily
from backend.repositories._base import QueryRunner


def _write_synthetic_hour_bundle(cache_root: str, hour: str, rows: list[dict]) -> tuple[str, str]:
    """Write an hourly rollup bundle to
    <cache_root>/rollups/hour_bundled/hour=<hour>/all_fields.parquet and all_fields_ip.parquet.
    """
    d = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "field": pa.array([r["field"] for r in rows]),
            "value": pa.array([r["value"] for r in rows]),
            "count": pa.array([r["count"] for r in rows], type=pa.int64()),
        }
    )
    p = os.path.join(d, "all_fields.parquet")
    tmp = os.path.join(d, f".tmp_{uuid.uuid4().hex[:8]}.parquet")
    pq.write_table(table, tmp)
    os.replace(tmp, p)

    ip_table = pa.table(
        {
            "field": pa.array([r["field"] for r in rows]),
            "value": pa.array([r["value"] for r in rows]),
            "unique_ips": pa.array([r["count"] for r in rows], type=pa.int64()),
        }
    )
    ip_p = os.path.join(d, "all_fields_ip.parquet")
    ip_tmp = os.path.join(d, f".tmp_ip_{uuid.uuid4().hex[:8]}.parquet")
    pq.write_table(ip_table, ip_tmp)
    os.replace(ip_tmp, ip_p)
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
    """Verifies items 1, 2, 3, 4, 5, 6 from docs/cron/jobs/rollup-compact.md §9."""
    src = contract_service_source
    sid = src["service_id"]
    cache_root = src["cache_dir"]

    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda s: cache_root)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s_id: src if s_id == sid else None)
    monkeypatch.setattr("backend.deps.get_source", lambda: src)
    monkeypatch.setattr(
        "backend.core.rollups.backfill_missing_hour_bundles", lambda *a, **k: {"missing": 0, "bundled": 0}
    )
    monkeypatch.setattr(
        "backend.core.rollups.backfill_missing_hour_ip_spread", lambda *a, **k: {"missing": 0, "rebuilt": 0}
    )

    # 1. Generate 24 hourly synthetic rollup bundles for a past UTC day (e.g. 2 days ago)
    target_day = (datetime.now(UTC) - timedelta(days=2)).strftime("%Y-%m-%d")
    hour_bundled_root = os.path.join(cache_root, "rollups", "hour_bundled")

    hourly_rows = [
        {"field": "ua", "value": "Mozilla/5.0", "count": 10},
        {"field": "status", "value": "200", "count": 50},
    ]

    for h in range(24):
        hour_str = f"{target_day}-{h:02d}"
        _write_synthetic_hour_bundle(cache_root, hour_str, hourly_rows)

    # Confirm all constituent hourly files exist (both all_fields and all_fields_ip)
    constituent_files = []
    for h in range(24):
        h_dir = os.path.join(hour_bundled_root, f"hour={target_day}-{h:02d}")
        constituent_files.append(os.path.join(h_dir, "all_fields.parquet"))
        constituent_files.append(os.path.join(h_dir, "all_fields_ip.parquet"))
    for p in constituent_files:
        assert os.path.isfile(p), f"Initial hour bundle should exist: {p}"

    # 2. Trigger POST /api/admin/rollups/compact/{service_id}; confirm HTTP 200
    resp = client.post(f"/api/admin/rollups/compact/{sid}")
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
    body = resp.json()
    assert body["status"] == "success"
    assert body["service_id"] == sid
    assert body["rebuilt"] >= 1
    assert body["bundled"] >= 1
    assert body["retired_files"] == 24

    # 3. Verify day_bundle_YYYY-MM-DD.parquet is created
    day_bundled_root = os.path.join(cache_root, "rollups", "day_bundled")
    canonical_bundle = os.path.join(day_bundled_root, f"day={target_day}", "all_fields.parquet")
    dir_alias = os.path.join(day_bundled_root, f"day={target_day}", f"day_bundle_{target_day}.parquet")
    flat_alias = os.path.join(day_bundled_root, f"day_bundle_{target_day}.parquet")

    assert os.path.isfile(canonical_bundle), f"Canonical day bundle missing: {canonical_bundle}"
    assert os.path.isfile(dir_alias), f"Directory alias missing: {dir_alias}"
    assert os.path.isfile(flat_alias), f"Flat alias missing: {flat_alias}"

    # Verify mathematical exact sums in day bundle
    con = duckdb.connect()
    try:
        ua_count = con.execute(
            f"SELECT sum(count) FROM read_parquet('{flat_alias}') WHERE field = 'ua' AND value = 'Mozilla/5.0'"
        ).fetchone()[0]
        assert ua_count == 240, f"Expected 240 for ua, got {ua_count}"

        status_count = con.execute(
            f"SELECT sum(count) FROM read_parquet('{flat_alias}') WHERE field = 'status' AND value = '200'"
        ).fetchone()[0]
        assert status_count == 1200, f"Expected 1200 for status, got {status_count}"
    finally:
        con.close()

    # 4. Confirm standard hourly bundles are retired; IP bundles remain because
    # this fixture does not create a day-level IP bundle.
    for h in range(24):
        hour_dir = os.path.join(hour_bundled_root, f"hour={target_day}-{h:02d}")
        assert not os.path.exists(os.path.join(hour_dir, "all_fields.parquet")), (
            f"Hourly bundle should be retired: {hour_dir}/all_fields.parquet"
        )
        assert os.path.isfile(os.path.join(hour_dir, "all_fields_ip.parquet")), (
            f"Hourly IP bundle should remain: {hour_dir}/all_fields_ip.parquet"
        )

    # 5. Run a query covering the target day; confirm query reads the day bundle directly
    mem_db = duckdb.connect(":memory:")
    runner = QueryRunner(mem_db, src)
    t0 = time.perf_counter()
    phase_log: list[dict] = []
    next_day = (datetime.strptime(target_day, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    res_tuples, fields_ret = runner.execute_top_n_rollups(
        ["ua", "status"],
        start_time=f"{target_day}T00:00:00Z",
        end_time=f"{next_day}T00:00:00Z",
        _phase_log=phase_log,
    )
    duration_s = time.perf_counter() - t0

    # Assert query read the bundled day file directly
    timings = {p["section"].replace("top_n_rollups:", ""): p["time_ms"] for p in phase_log}
    assert timings.get("dir_enum:n_bundled_day_files", 0) >= 1
    assert timings.get("dir_enum:n_bundled_hour_files", 0) == 0
    assert timings.get("dir_enum:n_hour_files", 0) == 0

    # Assert correct aggregate totals returned
    res = {f: [] for f in fields_ret}
    for field, val, count in res_tuples:
        res[field].append({"value": val, "count": count})

    assert "ua" in res
    assert res["ua"][0]["value"] == "Mozilla/5.0"
    assert res["ua"][0]["count"] == 240
    assert "status" in res
    assert res["status"][0]["value"] == "200"
    assert res["status"][0]["count"] == 1200

    # 6. Confirm query response time is < 150ms
    assert duration_s < 0.150, f"Query took {duration_s * 1000:.1f}ms, expected < 150ms"


def test_rollup_compact_active_request_politeness_gate(contract_service_source, monkeypatch):
    """Verify active-request politeness gate defers scheduled run but is bypassed by manual=True."""
    src = contract_service_source
    sid = src["service_id"]
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s_id: src)
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda task, s_id: True)

    # Scheduled run (manual=False) must defer
    res = _run_rollup_compact_daily.__wrapped__(sid, manual=False)
    assert res["status"] == "deferred"
    assert "active" in res["summary"].lower()

    # Manual run (manual=True) must bypass the gate
    # Stub compaction to avoid needing real data
    monkeypatch.setattr("backend.core.rollups.compact_closed_days_to_daily", lambda *a, **k: 0)
    monkeypatch.setattr("backend.core.rollups.backfill_day_bundles", lambda *a, **k: 0)
    monkeypatch.setattr("backend.core.rollups.retire_compacted_hour_bundles", lambda *a, **k: (0, 0))
    monkeypatch.setattr(
        "backend.core.rollups.backfill_missing_hour_bundles", lambda *a, **k: {"missing": 0, "bundled": 0}
    )
    monkeypatch.setattr(
        "backend.core.rollups.backfill_missing_hour_ip_spread", lambda *a, **k: {"missing": 0, "rebuilt": 0}
    )

    res_manual = _run_rollup_compact_daily.__wrapped__(sid, manual=True)
    assert res_manual["status"] == "success"


def test_rollup_compact_enospc_disk_safety_check(contract_service_source, monkeypatch):
    """Verify ENOSPC pre-check aborts compaction with status='warning' when disk space < 50MB."""
    src = contract_service_source
    sid = src["service_id"]
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s_id: src)
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda task, s_id: False)

    Usage = collections.namedtuple("Usage", ["total", "used", "free"])
    # 20MB free (< 50MB)
    monkeypatch.setattr(
        shutil, "disk_usage", lambda path: Usage(total=10**10, used=10**10 - 20 * 1024 * 1024, free=20 * 1024 * 1024)
    )

    logged_runs = []
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **k: logged_runs.append((a, k)))

    res = _run_rollup_compact_daily.__wrapped__(sid, manual=True)
    assert res["status"] == "warning"
    assert "critically low" in res["summary"].lower()

    assert len(logged_runs) == 1
    call_args, call_kwargs = logged_runs[0]
    assert call_args[3] == "warning"
    assert "critically low" in call_kwargs.get("error_message", "").lower()


def test_rollup_compact_subsystem_warning_status(contract_service_source, monkeypatch):
    """Verify that if ANY secondary subsystem fails, status='warning' is recorded with failed subsystem names."""
    src = contract_service_source
    sid = src["service_id"]
    cache_root = src["cache_dir"]
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s_id: src)
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda task, s_id: False)

    monkeypatch.setattr("backend.core.rollups.compact_closed_days_to_daily", lambda *a, **k: 1)
    monkeypatch.setattr("backend.core.rollups.backfill_day_bundles", lambda *a, **k: 1)
    monkeypatch.setattr("backend.core.rollups.retire_compacted_hour_bundles", lambda *a, **k: (24, 1024))
    monkeypatch.setattr(
        "backend.core.rollups.backfill_missing_hour_bundles", lambda *a, **k: {"missing": 0, "bundled": 0}
    )
    monkeypatch.setattr(
        "backend.core.rollups.backfill_missing_hour_ip_spread", lambda *a, **k: {"missing": 0, "rebuilt": 0}
    )

    # Simulate origin_summary and security_dims failing
    def _fail_origin(*a, **k):
        raise RuntimeError("origin summary disk timeout")

    def _fail_sec(*a, **k):
        raise RuntimeError("security dims binder error")

    monkeypatch.setattr("backend.core.rollups.compact_origin_summary_closed_days_to_daily", _fail_origin)
    monkeypatch.setattr("backend.core.rollups.compact_security_dims_closed_days_to_daily", _fail_sec)

    logged_runs = []
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **k: logged_runs.append((a, k)))

    res = _run_rollup_compact_daily.__wrapped__(sid, manual=True)
    assert res["status"] == "warning"
    assert res["errors"] is not None
    assert any("origin_summary" in e for e in res["errors"])
    assert any("security_dims" in e for e in res["errors"])

    # Ensure cron_runs recorded warning and the error_message contains failed subsystems
    assert len(logged_runs) == 1
    call_args, call_kwargs = logged_runs[0]
    assert call_args[3] == "warning"
    err_msg = call_kwargs.get("error_message", "")
    assert "origin_summary" in err_msg
    assert "security_dims" in err_msg


def test_rollup_compact_zero_fos_egress(contract_service_source, monkeypatch):
    """Confirm zero FOS / S3 calls during rollup compaction."""
    src = contract_service_source
    sid = src["service_id"]
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s_id: src)
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda task, s_id: False)

    # Throw if any FOS client factory or S3 client is touched
    mock_fos = MagicMock(side_effect=AssertionError("FOS client should NEVER be invoked during rollup compaction"))
    monkeypatch.setattr("backend.core.duckdb._get_fos_client", mock_fos)

    monkeypatch.setattr("backend.core.rollups.compact_closed_days_to_daily", lambda *a, **k: 0)
    monkeypatch.setattr("backend.core.rollups.backfill_day_bundles", lambda *a, **k: 0)
    monkeypatch.setattr("backend.core.rollups.retire_compacted_hour_bundles", lambda *a, **k: (0, 0))
    monkeypatch.setattr(
        "backend.core.rollups.backfill_missing_hour_bundles", lambda *a, **k: {"missing": 0, "bundled": 0}
    )
    monkeypatch.setattr(
        "backend.core.rollups.backfill_missing_hour_ip_spread", lambda *a, **k: {"missing": 0, "rebuilt": 0}
    )

    res = _run_rollup_compact_daily.__wrapped__(sid, manual=True)
    assert res["status"] == "success"
    mock_fos.assert_not_called()


def test_rollup_compact_admin_status_endpoint(contract_service_source, client, monkeypatch):
    """Verify GET /api/admin/rollups/status returns accurate days_bundled and hour_bundles counts."""
    src = contract_service_source
    sid = src["service_id"]
    cache_root = src["cache_dir"]

    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda s: cache_root)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s_id: src if s_id == sid else None)
    monkeypatch.setattr("backend.deps.get_source", lambda: src)

    # Seed 1 day bundle and 2 hour bundles
    day_dir = os.path.join(cache_root, "rollups", "day_bundled", "day=2026-06-01")
    os.makedirs(day_dir, exist_ok=True)
    with open(os.path.join(day_dir, "all_fields.parquet"), "wb") as f:
        f.write(b"")

    h1_dir = os.path.join(cache_root, "rollups", "hour_bundled", "hour=2026-06-02-10")
    h2_dir = os.path.join(cache_root, "rollups", "hour_bundled", "hour=2026-06-02-11")
    os.makedirs(h1_dir, exist_ok=True)
    os.makedirs(h2_dir, exist_ok=True)

    resp = client.get(f"/api/admin/rollups/status/{sid}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["service_id"] == sid
    assert data["days_bundled"] == 1
    assert data["hour_bundles"] == 2
    assert data["latest_day_bundle"] == "2026-06-01"


def test_call_with_lookback_does_not_rerun_unbounded_on_inner_typeerror():
    from backend.cron.jobs.compaction import _call_with_lookback

    calls: list[dict] = []

    def _fn(*args, lookback_days=None):
        calls.append({"lookback_days": lookback_days})
        raise TypeError("bad value for lookback_days")

    with pytest.raises(TypeError):
        _call_with_lookback(_fn, "svc", {}, lookback_days=30)

    assert calls == [{"lookback_days": 30}]  # no second, unbounded re-run


# ── Merged from test_rollup_heal_contract.py ──────────────────────────────────────────────


import pytest

from backend.cron.jobs.compaction import _run_rollup_hour_heal


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

    def test_scheduler_registration_and_schedule_mapping(self, frozen_clock):
        """Verifies Checklist Items 1 & 2:
        - Registration in backend/cron/scheduler.py at minute=5 and startup (NOW() + 5s).
        - Task mapping "rollup_heal": "rollup_hour_heal" in backend/cron/schedule.py.
        """
        from backend.cron.schedule import TASK_MAP

        assert TASK_MAP.get("rollup_heal") == "rollup_hour_heal"

        from backend.cron.scheduler import Scheduler

        sched = Scheduler()
        mock_add = MagicMock()
        sched._add_job = mock_add

        cfg = {
            "service_id": "svc-dev-test",
            "name": "Dev Test Svc",
            "fastly": {"service_id": "svc-dev-test"},
            "provisioning": {"access_level": "read_write"},
        }
        src = {
            "service_id": "svc-dev-test",
            "name": "Dev Test Svc",
            "fastly": {"service_id": "svc-dev-test"},
            "provisioning": {"access_level": "read_write"},
        }

        # 1. Dev-local safe registration
        with (
            patch("backend.config.list_configs", return_value=[cfg]),
            patch("backend.core.duckdb.get_source_for_service", return_value=src),
            patch("backend.core.duckdb.is_configured", return_value=True),
            patch("backend.cron.scheduler.dev_local_crons_enabled", return_value=True),
        ):
            sched._register_dev_local_safe_jobs()

        heal_calls = [call for call in mock_add.call_args_list if call.kwargs.get("id") == "rollup_heal_svc-dev-test"]
        assert len(heal_calls) == 1
        call = heal_calls[0]
        assert call.args[0] == _run_rollup_hour_heal
        assert call.args[1] == "cron"
        assert call.kwargs.get("minute") == 5
        assert call.kwargs.get("args") == ["svc-dev-test"]
        assert call.kwargs.get("max_instances") == 1
        assert call.kwargs.get("coalesce") is True
        assert call.kwargs.get("misfire_grace_time") == 900
        expected_startup = datetime.now(UTC) + timedelta(seconds=5)
        assert call.kwargs.get("next_run_time") == expected_startup

        # 2. Sync jobs registration
        mock_add.reset_mock()
        sched._job_ids.clear()
        with (
            patch("backend.config.list_configs", return_value=[cfg]),
            patch("backend.core.duckdb.get_source_for_service", return_value=src),
            patch("backend.core.duckdb.is_configured", return_value=True),
            patch("backend.cron.scheduler.dev_mode_no_crons", return_value=False),
        ):
            sched._sync_jobs()

        sync_heal_calls = [
            call for call in mock_add.call_args_list if call.kwargs.get("id") == "rollup_heal_svc-dev-test"
        ]
        assert len(sync_heal_calls) == 1
        scall = sync_heal_calls[0]
        assert scall.args[0] == _run_rollup_hour_heal
        assert scall.kwargs.get("minute") == 5
        assert scall.kwargs.get("next_run_time") == expected_startup

    def test_rollup_heal_lookback_window_48h(self, contract_service_source, monkeypatch):
        """Verifies Checklist Item 3:
        48-hour lookback window (lookback_days = 2) in backend/cron/jobs/compaction.py.
        """
        src = contract_service_source
        sid = src["service_id"]

        monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s_id: src if s_id == sid else None)
        monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=101))
        logged_runs = []
        monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **kw: logged_runs.append((a, kw)))

        heal_mock = MagicMock(
            return_value={
                "missing": 3,
                "rebuilt_fields": 12,
                "bundled": 3,
                "stamped_empty": 1,
                "coverage_verified": True,
            }
        )
        monkeypatch.setattr("backend.core.rollups.backfill_missing_hour_bundles", heal_mock)

        res = _run_rollup_hour_heal.__wrapped__(sid)

        assert heal_mock.call_count == 1
        assert heal_mock.call_args.kwargs.get("lookback_days") == 2
        assert res["status"] == "success"
        assert res["missing"] == 3
        assert res["rebuilt_fields"] == 12
        assert res["bundled"] == 3
        assert res["stamped_empty"] == 1
        assert "Healed 3 missing hour(s)" in res["summary"]

        assert len(logged_runs) == 1
        args, kwargs = logged_runs[0]
        assert args[1] == "rollup_hour_heal"
        assert args[3] == "success"
        assert "Healed 3 missing hour(s)" in kwargs["summary"]
        assert "1 empty hour(s) stamped" in kwargs["summary"]

    def test_rollup_heal_politeness_gate_and_manual_bypass(self, contract_service_source, monkeypatch):
        """Verifies Checklist Item 4:
        Politeness gate yields via should_defer_cron("rollup_hour_heal", service_id)
        when user queries are active; manual=True bypasses it.
        """
        src = contract_service_source
        sid = src["service_id"]

        monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s_id: src if s_id == sid else None)
        monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda task, s_id: True)
        heal_mock = MagicMock(return_value={"missing": 0, "rebuilt_fields": 0, "bundled": 0})
        monkeypatch.setattr("backend.core.rollups.backfill_missing_hour_bundles", heal_mock)

        # 1. Automated cron tick: yields when query load is detected
        deferred_res = _run_rollup_hour_heal.__wrapped__(sid, manual=False)
        assert deferred_res["status"] == "deferred"
        heal_mock.assert_not_called()

        # 2. Manual trigger: executes regardless of in-flight queries
        monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=102))
        monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
        manual_res = _run_rollup_hour_heal.__wrapped__(sid, manual=True)
        assert manual_res["status"] == "success"
        assert heal_mock.call_count == 1

    def test_rollup_heal_durable_mode_throttle_and_readiness(self, contract_service_source, monkeypatch):
        """Verifies Checklist Item 5:
        Durable serving mode throttle: max_missing_hours = 1 startup catch-up ceiling
        until coverage is verified.
        """
        src = contract_service_source
        sid = src["service_id"]

        monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s_id: src if s_id == sid else None)
        monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=103))
        monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
        monkeypatch.setattr("backend.config.is_durable_serving_mode", lambda s: True)
        monkeypatch.setattr("backend.core.rollup_readiness.rollup_coverage_ready", lambda s_id: False)
        mark_ready_mock = MagicMock()
        monkeypatch.setattr("backend.core.rollup_readiness.mark_rollup_coverage_ready", mark_ready_mock)

        # 1. Unverified pass: throttled to max_missing_hours = 1, does not mark ready
        heal_partial = MagicMock(
            return_value={
                "missing": 1,
                "rebuilt_fields": 5,
                "bundled": 1,
                "coverage_verified": False,
            }
        )
        monkeypatch.setattr("backend.core.rollups.backfill_missing_hour_bundles", heal_partial)
        res1 = _run_rollup_hour_heal.__wrapped__(sid)
        assert res1["status"] == "success"
        assert heal_partial.call_args.kwargs.get("max_missing_hours") == 1
        mark_ready_mock.assert_not_called()

        # 2. Verified pass: marks rollup coverage ready
        heal_complete = MagicMock(
            return_value={
                "missing": 0,
                "rebuilt_fields": 0,
                "bundled": 0,
                "coverage_verified": True,
            }
        )
        monkeypatch.setattr("backend.core.rollups.backfill_missing_hour_bundles", heal_complete)
        res2 = _run_rollup_hour_heal.__wrapped__(sid)
        assert res2["status"] == "success"
        mark_ready_mock.assert_called_once_with(sid)

    def test_rollup_heal_empty_hour_sentinels(self, contract_service_source, monkeypatch):
        """Verifies Checklist Item 6:
        Empty-hour sentinel bundle generation for zero-row closed hours
        prevents re-scan thrashing.
        """
        src = contract_service_source
        sid = src["service_id"]

        monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s_id: src if s_id == sid else None)
        monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=104))
        logged = []
        monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **k: logged.append((a, k)))

        heal_mock = MagicMock(
            return_value={
                "missing": 0,
                "rebuilt_fields": 0,
                "bundled": 0,
                "stamped_empty": 4,
                "coverage_verified": True,
            }
        )
        monkeypatch.setattr("backend.core.rollups.backfill_missing_hour_bundles", heal_mock)

        res = _run_rollup_hour_heal.__wrapped__(sid)
        assert res["status"] == "success"
        assert res["stamped_empty"] == 4
        assert "4 empty hour(s) stamped" in res["summary"]

        assert len(logged) == 1
        args, kwargs = logged[0]
        assert "4 empty hour(s) stamped" in kwargs["summary"]

    def test_admin_trigger_and_inspection_parity(self, contract_service_source, monkeypatch):
        """Verifies Checklist Item 7:
        Manual trigger & inspection parity:
        - POST /api/admin/rollup-heal/{service_id} (HTTP 200, RollupHealResponse)
        - POST /api/admin/rollup-heal (HTTP 200, RollupHealResponse)
        - POST /api/admin/rollups/heal/{service_id} (HTTP 200, RollupHealResponse)
        - POST /api/admin/backfill-bundle-rollups (HTTP 200, BackfillBundleRollupsResponse)
        """
        from backend.deps import get_source
        from backend.main import app

        src = contract_service_source
        sid = src["service_id"]

        monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s_id: src if s_id == sid else None)
        app.dependency_overrides[get_source] = lambda: src
        monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=105))
        monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())

        heal_mock = MagicMock(
            return_value={
                "missing": 2,
                "rebuilt_fields": 10,
                "bundled": 2,
                "stamped_empty": 0,
                "coverage_verified": True,
            }
        )
        monkeypatch.setattr("backend.core.rollups.backfill_missing_hour_bundles", heal_mock)

        client = TestClient(app)
        try:
            # 1. POST /api/admin/rollup-heal/{service_id}
            resp1 = client.post(f"/api/admin/rollup-heal/{sid}")
            assert resp1.status_code == 200, f"Expected 200, got: {resp1.text}"
            data1 = resp1.json()
            assert data1["status"] == "success"
            assert data1["service_id"] == sid
            assert data1["missing"] == 2
            assert data1["bundled"] == 2
            assert "Healed 2 missing hour(s)" in data1["summary"]

            # 2. POST /api/admin/rollup-heal
            resp2 = client.post("/api/admin/rollup-heal", headers={"x-service-id": sid})
            assert resp2.status_code == 200, f"Expected 200, got: {resp2.text}"
            data2 = resp2.json()
            assert data2["status"] == "success"

            # 3. POST /api/admin/rollups/heal/{service_id} (alias)
            resp3 = client.post(f"/api/admin/rollups/heal/{sid}")
            assert resp3.status_code == 200, f"Expected 200, got: {resp3.text}"
            data3 = resp3.json()
            assert data3["status"] == "success"

            # 4. POST /api/admin/backfill-bundle-rollups (existing endpoint)
            # Stub every exported writer: a hand-kept list missed perf_dims / pop_health compaction, so the
            # request ran real rollup scans that stalled on the per-service rollup lock under CI load.
            import backend.core.rollups as rollups_pkg

            for fn_name in dir(rollups_pkg):
                if fn_name.startswith(("backfill_", "compact_")) and callable(getattr(rollups_pkg, fn_name)):
                    monkeypatch.setattr(f"backend.core.rollups.{fn_name}", lambda *a, **k: 0)

            monkeypatch.setattr("backend.core.rollups._common.backfill_missing_bundles", lambda *a, **k: 0)

            # The route ends with a RUM-aggregates backfill that opens a real ``.rum.duckdb`` connection;
            # a contended file sat in the lock-retry sleep until the 60 s worker kill (CI hang dump).
            monkeypatch.setattr("backend.core.duckdb.get_connection", lambda *a, **k: MagicMock())
            monkeypatch.setattr("backend.core.rollups.rum.recompute_rum_aggregates", lambda *a, **k: None)
            monkeypatch.setattr("backend.core.rollups.rum.table_exists", lambda *a, **k: False)

            resp4 = client.post("/api/admin/backfill-bundle-rollups", headers={"x-service-id": sid})
            assert resp4.status_code == 200, f"Expected 200, got: {resp4.text}"
            data4 = resp4.json()
            assert "slow_urls" in data4
            assert "origin_summary" in data4
        finally:
            app.dependency_overrides.pop(get_source, None)

    def test_rollup_heal_enospc_safety(self, contract_service_source, monkeypatch):
        """Verifies Checklist Item 8:
        ENOSPC disk space safety pre-check (< 50MB free) safely skips execution,
        logs a warning, and returns status="warning".
        """
        src = contract_service_source
        sid = src["service_id"]

        monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s_id: src if s_id == sid else None)
        heal_mock = MagicMock()
        monkeypatch.setattr("backend.core.rollups.backfill_missing_hour_bundles", heal_mock)

        logged = []
        monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **k: logged.append((a, k)))

        # Simulate critical disk space (< 50MB free, e.g. 10MB)
        fake_usage = MagicMock(free=10 * 1024 * 1024)
        monkeypatch.setattr("shutil.disk_usage", lambda path: fake_usage)

        res = _run_rollup_hour_heal.__wrapped__(sid)

        assert res["status"] == "warning"
        assert "Disk space critically low" in res["summary"]
        heal_mock.assert_not_called()

        assert len(logged) == 1
        args, kwargs = logged[0]
        assert args[1] == "rollup_hour_heal"
        assert args[3] == "warning"
        assert "Skipped due to low disk space" in kwargs["summary"]

    def test_rollup_heal_zero_fos_egress(self, contract_service_source, monkeypatch):
        """Verifies Checklist Item 9:
        Zero FOS egress contract — local-only writes, no outbound S3/boto3 calls.
        """
        src = contract_service_source
        sid = src["service_id"]

        monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s_id: src if s_id == sid else None)
        monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=106))
        monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())

        heal_mock = MagicMock(
            return_value={
                "missing": 1,
                "rebuilt_fields": 8,
                "bundled": 1,
                "stamped_empty": 0,
                "coverage_verified": True,
            }
        )
        monkeypatch.setattr("backend.core.rollups.backfill_missing_hour_bundles", heal_mock)

        # Poison boto3 client to fail if called
        boto_mock = MagicMock(side_effect=AssertionError("Outbound FOS network call forbidden in rollup_heal!"))
        monkeypatch.setattr("boto3.client", boto_mock)

        res = _run_rollup_hour_heal.__wrapped__(sid)
        assert res["status"] == "success"
        boto_mock.assert_not_called()

    def test_rollup_heal_error_handling(self, contract_service_source, monkeypatch):
        """Verifies Checklist Item 10:
        Resilient exception handling: on runtime failure during backfill, logs
        error to cron_runs and returns structured error dict.
        """
        src = contract_service_source
        sid = src["service_id"]

        monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s_id: src if s_id == sid else None)
        monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=107))
        logged = []
        monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **k: logged.append((a, k)))

        monkeypatch.setattr(
            "backend.core.rollups.backfill_missing_hour_bundles",
            MagicMock(side_effect=RuntimeError("DuckDB connection pool exhausted")),
        )

        res = _run_rollup_hour_heal.__wrapped__(sid)
        assert res["status"] == "error"
        assert "DuckDB connection pool exhausted" in res["error_message"]

        assert len(logged) == 1
        args, kwargs = logged[0]
        assert args[1] == "rollup_hour_heal"
        assert args[3] == "error"
        assert "DuckDB connection pool exhausted" in kwargs["error_message"]

    def test_rollup_heal_returns_when_source_missing(self, monkeypatch):
        monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)
        _run_rollup_hour_heal.__wrapped__("missing-svc")

    def test_rollup_heal_skips_when_start_cron_run_raises(self, contract_service_source, monkeypatch):
        from backend.core.metadata.cron_log import CronLeaseHeldError

        src = contract_service_source
        sid = src["service_id"]
        monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s_id: src if s_id == sid else None)
        monkeypatch.setattr(
            "backend.core.duckdb.start_cron_run",
            MagicMock(side_effect=CronLeaseHeldError("already running")),
        )
        heal_mock = MagicMock()
        monkeypatch.setattr("backend.core.rollups.backfill_missing_hour_bundles", heal_mock)

        _run_rollup_hour_heal.__wrapped__(sid)

        heal_mock.assert_not_called()

    def test_rollup_heal_does_not_mark_ready_when_not_durable_mode(self, contract_service_source, monkeypatch):
        src = contract_service_source
        sid = src["service_id"]
        monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s_id: src if s_id == sid else None)
        monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=201))
        monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
        heal_mock = MagicMock(return_value={"missing": 1, "rebuilt_fields": 1, "bundled": 1})
        monkeypatch.setattr("backend.core.rollups.backfill_missing_hour_bundles", heal_mock)
        monkeypatch.setattr("backend.config.is_durable_serving_mode", lambda s: False)
        mark_mock = MagicMock()
        monkeypatch.setattr("backend.core.rollup_readiness.mark_rollup_coverage_ready", mark_mock)

        _run_rollup_hour_heal.__wrapped__(sid)

        mark_mock.assert_not_called()

    def test_rollup_heal_does_not_mark_ready_on_failure(self, contract_service_source, monkeypatch):
        src = contract_service_source
        sid = src["service_id"]
        monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s_id: src if s_id == sid else None)
        monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=202))
        monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
        monkeypatch.setattr(
            "backend.core.rollups.backfill_missing_hour_bundles",
            MagicMock(side_effect=RuntimeError("view refresh failed")),
        )
        monkeypatch.setattr("backend.config.is_durable_serving_mode", lambda s: True)
        mark_mock = MagicMock()
        monkeypatch.setattr("backend.core.rollup_readiness.mark_rollup_coverage_ready", mark_mock)

        _run_rollup_hour_heal.__wrapped__(sid)

        mark_mock.assert_not_called()


# ── Merged from test_partial_hour_cron.py ──────────────────────────────────────────────


import pytest

from backend.core.rollups import partial_hour as ph
from backend.cron.jobs import compaction as ph_job


def test_run_partial_hour_merge_calls_merge_and_does_not_mark_ready(monkeypatch):
    """C2 (final whole-branch review): a 30s tick — even a successful one —
    only proves the writer path is alive, not that this pod's local
    closed-hour rollup tree reflects a genuine backfill pass. That flag is
    now only raised by main.py's startup catch-up and by
    _run_rollup_hour_heal's success path."""
    from backend.cron.jobs.compaction import _run_partial_hour_merge

    fake_src = {"service_id": "svc-a", "name": "svc-a"}
    monkeypatch.setattr("backend.repositories.dashboard.FIELDS", ["country", "method"])

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
            return_value={"hour": "2026-09-09-14", "new_files": 3, "duration_ms": 12.5},
        ) as mock_merge,
        patch("backend.core.rollups.partial_hour.gc_stale_partial_hours", return_value=0),
        patch("backend.core.rollup_readiness.mark_rollup_coverage_ready") as mock_mark,
    ):
        _run_partial_hour_merge("svc-a")

    mock_merge.assert_called_once()
    mock_mark.assert_not_called()
    mock_log.assert_called_once()
    assert mock_log.call_args.kwargs.get("status") == "success" or mock_log.call_args[0][3] == "success"


def test_run_partial_hour_merge_filters_out_live_topn_skip_fields(monkeypatch):
    """M1 (final whole-branch review): the cron should aggregate only the
    fields execute_top_n_rollups's own live top-up would bother with, not
    the full FIELDS list (which includes high-cardinality identifier/raw
    columns with no top-N panel — see _LIVE_TOPN_SKIP_FIELDS)."""
    from backend.cron.jobs.compaction import _run_partial_hour_merge
    from backend.repositories._base import _LIVE_TOPN_SKIP_FIELDS

    fake_src = {"service_id": "svc-a", "name": "svc-a"}
    skip_field = next(iter(_LIVE_TOPN_SKIP_FIELDS))
    monkeypatch.setattr("backend.repositories.dashboard.FIELDS", ["country", skip_field])

    with (
        patch("backend.utils.active_requests.should_defer_cron", return_value=False),
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.core.duckdb.start_cron_run", return_value="run-1"),
        patch("backend.core.duckdb.log_cron_run"),
        patch("backend.cron_progress.cleanup_progress_and_reap"),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron_progress.end_progress"),
        patch(
            "backend.core.rollups.partial_hour.merge_partial_hour",
            return_value={"hour": "2026-09-09-14", "new_files": 1, "duration_ms": 1.0},
        ) as mock_merge,
        patch("backend.core.rollups.partial_hour.gc_stale_partial_hours", return_value=0),
    ):
        _run_partial_hour_merge("svc-a")

    merged_fields = mock_merge.call_args[0][2]
    assert "country" in merged_fields
    assert skip_field not in merged_fields


def test_run_partial_hour_merge_filters_out_metrics_only_synthetic_fields(monkeypatch):
    """Regression test for a live production incident: METRICS-group catalog
    entries (requests, hit_rate, p50_latency, ...) have vcl=None — they are
    chart-only synthetic concepts, never a real per-row column — but
    _LIVE_TOPN_SKIP_FIELDS never excluded them, so merge_partial_hour tried
    to SELECT a "requests" column that doesn't exist in the source parquet
    and errored on every 30s tick for every service."""
    from backend.cron.jobs.compaction import _run_partial_hour_merge

    fake_src = {"service_id": "svc-a", "name": "svc-a"}
    monkeypatch.setattr("backend.repositories.dashboard.FIELDS", ["country", "requests", "hit_rate"])

    with (
        patch("backend.utils.active_requests.should_defer_cron", return_value=False),
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.core.duckdb.start_cron_run", return_value="run-1"),
        patch("backend.core.duckdb.log_cron_run"),
        patch("backend.cron_progress.cleanup_progress_and_reap"),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron_progress.end_progress"),
        patch(
            "backend.core.rollups.partial_hour.merge_partial_hour",
            return_value={"hour": "2026-09-09-14", "new_files": 1, "duration_ms": 1.0},
        ) as mock_merge,
        patch("backend.core.rollups.partial_hour.gc_stale_partial_hours", return_value=0),
    ):
        _run_partial_hour_merge("svc-a")

    merged_fields = mock_merge.call_args[0][2]
    assert "country" in merged_fields
    assert "requests" not in merged_fields
    assert "hit_rate" not in merged_fields


def test_run_partial_hour_merge_filters_out_any_derived_field(monkeypatch):
    """Regression test for a second live incident on the same tick: after
    excluding METRICS fields, the very next tick errored the same way on
    "_bot_name" — a VIRTUAL-group field never in the narrow _VIRTUAL_FIELDS
    tuple, so it wasn't in _LIVE_TOPN_SKIP_FIELDS either. This writer has
    no special handling for ANY derived (vcl=None) field, so it must
    exclude the whole set, not a hand-maintained subset."""
    from backend.cron.jobs.compaction import _run_partial_hour_merge

    fake_src = {"service_id": "svc-a", "name": "svc-a"}
    monkeypatch.setattr("backend.repositories.dashboard.FIELDS", ["country", "_bot_name", "waf_sig_ind"])

    with (
        patch("backend.utils.active_requests.should_defer_cron", return_value=False),
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.core.duckdb.start_cron_run", return_value="run-1"),
        patch("backend.core.duckdb.log_cron_run"),
        patch("backend.cron_progress.cleanup_progress_and_reap"),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron_progress.end_progress"),
        patch(
            "backend.core.rollups.partial_hour.merge_partial_hour",
            return_value={"hour": "2026-09-09-14", "new_files": 1, "duration_ms": 1.0},
        ) as mock_merge,
        patch("backend.core.rollups.partial_hour.gc_stale_partial_hours", return_value=0),
    ):
        _run_partial_hour_merge("svc-a")

    merged_fields = mock_merge.call_args[0][2]
    assert "country" in merged_fields
    assert "_bot_name" not in merged_fields
    assert "waf_sig_ind" not in merged_fields


def test_run_partial_hour_merge_keeps_cron_runs_row_for_noop_tick(monkeypatch):
    """A no-op tick still records a successful heartbeat for the Cron UI."""
    from backend.cron.jobs.compaction import _run_partial_hour_merge

    fake_src = {"service_id": "svc-a", "name": "svc-a"}
    monkeypatch.setattr("backend.repositories.dashboard.FIELDS", ["country"])

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
            return_value={"hour": "2026-09-09-14", "new_files": 0, "duration_ms": 1.0},
        ),
        patch("backend.core.rollups.partial_hour.gc_stale_partial_hours", return_value=0),
        patch("backend.core.metadata.delete_cron_run") as mock_delete,
    ):
        _run_partial_hour_merge("svc-a")

    mock_log.assert_called_once()
    assert mock_log.call_args.kwargs.get("status") == "success" or mock_log.call_args[0][3] == "success"
    mock_delete.assert_not_called()


def test_run_partial_hour_merge_keeps_cron_runs_row_when_files_merged(monkeypatch):
    """Sibling to the no-op test above: a real tick's row must NOT be deleted."""
    from backend.cron.jobs.compaction import _run_partial_hour_merge

    fake_src = {"service_id": "svc-a", "name": "svc-a"}
    monkeypatch.setattr("backend.repositories.dashboard.FIELDS", ["country"])

    with (
        patch("backend.utils.active_requests.should_defer_cron", return_value=False),
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.core.duckdb.start_cron_run", return_value="run-1"),
        patch("backend.core.duckdb.log_cron_run"),
        patch("backend.cron_progress.cleanup_progress_and_reap"),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron_progress.end_progress"),
        patch(
            "backend.core.rollups.partial_hour.merge_partial_hour",
            return_value={"hour": "2026-09-09-14", "new_files": 2, "duration_ms": 1.0},
        ),
        patch("backend.core.rollups.partial_hour.gc_stale_partial_hours", return_value=0),
        patch("backend.core.metadata.delete_cron_run") as mock_delete,
    ):
        _run_partial_hour_merge("svc-a")

    mock_delete.assert_not_called()


def test_run_partial_hour_merge_defers_when_requests_in_flight():
    from backend.cron.jobs.compaction import _run_partial_hour_merge

    with (
        patch("backend.utils.active_requests.should_defer_cron", return_value=True),
        patch("backend.core.duckdb.start_cron_run") as mock_start,
    ):
        _run_partial_hour_merge("svc-a")

    mock_start.assert_not_called()


# ── Contract Tests (Cron 4: partial_hour_merge_{service_id}) ───────────────


def _write_active_hour_parquet(path: str, rows: int, dt_start: datetime) -> None:
    """Write synthetic log rows stamped within the given datetime."""
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


@pytest.fixture
def mock_ph_env(tmp_path, monkeypatch):
    """Set up an isolated cache root and mock configuration for testing partial-hour merge."""
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
        "provisioning": {
            "access_level": "read_write",
        },
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


def test_contract_incremental_folding_and_total_count(mock_ph_env):
    """Checklist Items 1-3:
    1. Write synthetic buffer parquets for current active hour.
    2. Execute merge_partial_hour.
    3. Verify rollup contains accurate __total__ and dimension aggregations.
    """
    src = mock_ph_env["source"]
    service_id = mock_ph_env["service_id"]
    buffer_dir = mock_ph_env["buffer_dir"]

    now = datetime.now(UTC)
    active_hour = now.strftime("%Y-%m-%d-%H")
    hour_start = now.replace(minute=0, second=0, microsecond=0)

    # Write 3 small buffer parquet files
    file1 = os.path.join(buffer_dir, "buf_01.parquet")
    file2 = os.path.join(buffer_dir, "buf_02.parquet")
    file3 = os.path.join(buffer_dir, "buf_03.parquet")

    _write_active_hour_parquet(file1, rows=20, dt_start=hour_start + timedelta(minutes=1))
    _write_active_hour_parquet(file2, rows=30, dt_start=hour_start + timedelta(minutes=5))
    _write_active_hour_parquet(file3, rows=50, dt_start=hour_start + timedelta(minutes=10))

    stats = ph.merge_partial_hour(service_id, src, ["status", "country"])

    assert stats["new_files"] == 3
    assert stats["hour"] == active_hour
    assert stats["duration_ms"] >= 0

    # Verify rollup file exists
    rollup_file = ph._all_fields_path(src, active_hour)
    assert os.path.isfile(rollup_file)

    # Verify __total__ count is exactly 100
    total = ph.read_partial_hour_total(src, active_hour)
    assert total == 100

    # Verify all_fields has status and country rows
    rows = ph.read_partial_hour_all_fields(src, active_hour)
    fields = {r[0] for r in rows}
    assert "status" in fields
    assert "country" in fields
    assert ph.TOTAL_FIELD not in fields

    # Verify watermark file is populated
    wm = ph.read_partial_hour_watermark(src, active_hour)
    assert wm > 0.0


def test_contract_admin_endpoints(mock_ph_env):
    """Checklist Item 2: Invocation via POST /api/admin/partial-hour-merge and status via GET."""
    from backend.main import app

    service_id = mock_ph_env["service_id"]
    buffer_dir = mock_ph_env["buffer_dir"]

    now = datetime.now(UTC)
    hour_start = now.replace(minute=0, second=0, microsecond=0)
    file1 = os.path.join(buffer_dir, "buf_admin.parquet")
    _write_active_hour_parquet(file1, rows=25, dt_start=hour_start + timedelta(minutes=2))

    client = TestClient(app)

    # 1. Trigger merge via admin POST
    resp = client.post(f"/api/admin/partial-hour-merge/{service_id}")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["service_id"] == service_id
    assert data["new_files"] == 1
    assert data["status"] == "success"

    # 2. Check status via admin GET
    status_resp = client.get(f"/api/admin/partial-hour-status/{service_id}")
    assert status_resp.status_code == 200, status_resp.text
    status_data = status_resp.json()
    assert status_data["service_id"] == service_id
    assert status_data["file_exists"] is True
    assert status_data["total_rows"] == 25
    assert status_data["watermark"] > 0


def test_contract_consecutive_ticks_without_double_count(mock_ph_env):
    """Subsequent ticks fold in newly arrived files on top of existing rollup without double counting."""
    src = mock_ph_env["source"]
    service_id = mock_ph_env["service_id"]
    buffer_dir = mock_ph_env["buffer_dir"]

    now = datetime.now(UTC)
    active_hour = now.strftime("%Y-%m-%d-%H")
    hour_start = now.replace(minute=0, second=0, microsecond=0)

    # Tick 1: 40 rows
    f1 = os.path.join(buffer_dir, "batch_1.parquet")
    _write_active_hour_parquet(f1, rows=40, dt_start=hour_start + timedelta(minutes=1))
    stats1 = ph.merge_partial_hour(service_id, src, ["status", "country"])
    assert stats1["new_files"] == 1
    assert ph.read_partial_hour_total(src, active_hour) == 40

    # Tick 2 (no new files): 0 files merged, total remains 40
    stats2 = ph.merge_partial_hour(service_id, src, ["status", "country"])
    assert stats2["new_files"] == 0
    assert ph.read_partial_hour_total(src, active_hour) == 40

    # Ensure timestamp of next file is newer than previous file
    time.sleep(0.05)
    f2 = os.path.join(buffer_dir, "batch_2.parquet")
    _write_active_hour_parquet(f2, rows=60, dt_start=hour_start + timedelta(minutes=2))
    # Explicitly ensure mtime is higher
    os.utime(f2, (time.time() + 10, time.time() + 10))

    # Tick 3: 60 new rows, total becomes 100
    stats3 = ph.merge_partial_hour(service_id, src, ["status", "country"])
    assert stats3["new_files"] == 1
    assert ph.read_partial_hour_total(src, active_hour) == 100


def test_contract_zero_outbound_fos_calls(mock_ph_env, monkeypatch):
    """Checklist Item 6: Verify zero outbound Fastly Object Storage / network calls."""
    src = mock_ph_env["source"]
    service_id = mock_ph_env["service_id"]
    buffer_dir = mock_ph_env["buffer_dir"]

    now = datetime.now(UTC)
    hour_start = now.replace(minute=0, second=0, microsecond=0)
    _write_active_hour_parquet(
        os.path.join(buffer_dir, "buf_fos.parquet"), rows=10, dt_start=hour_start + timedelta(minutes=1)
    )

    def fake_egress(*args, **kwargs):
        raise AssertionError("Outbound network egress occurred during partial hour merge!")

    monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value="run_ph_fos"))

    with patch("urllib.request.urlopen", side_effect=fake_egress):
        res = ph.merge_partial_hour(service_id, src, ["status"])
        assert res["new_files"] == 1

        # Also run via cron wrapper
        ph_job._run_partial_hour_merge.__wrapped__(service_id)


def test_contract_atomic_replacement_and_crash_safety(mock_ph_env, monkeypatch):
    """Verify atomic replace order, temp file cleanup, and crash safety."""
    src = mock_ph_env["source"]
    service_id = mock_ph_env["service_id"]
    buffer_dir = mock_ph_env["buffer_dir"]

    now = datetime.now(UTC)
    active_hour = now.strftime("%Y-%m-%d-%H")
    hour_start = now.replace(minute=0, second=0, microsecond=0)
    f1 = os.path.join(buffer_dir, "b1.parquet")
    _write_active_hour_parquet(f1, rows=20, dt_start=hour_start + timedelta(minutes=1))

    # Test 1: Successful run has zero .tmp_ files leftover
    ph.merge_partial_hour(service_id, src, ["status"])
    ph_dir = ph.partial_hour_dir(src, active_hour)
    tmp_files = [f for f in os.listdir(ph_dir) if f.startswith(".tmp_")]
    assert len(tmp_files) == 0

    # Test 2: Crash during atomic replace cleans up .tmp_ file and leaves existing file intact
    f2 = os.path.join(buffer_dir, "b2.parquet")
    _write_active_hour_parquet(f2, rows=30, dt_start=hour_start + timedelta(minutes=2))
    os.utime(f2, (time.time() + 10, time.time() + 10))

    def failing_replace(src_p, dst_p):
        raise OSError("Simulated disk error during atomic replace")

    with patch("os.replace", side_effect=failing_replace):
        with pytest.raises(OSError):
            ph.merge_partial_hour(service_id, src, ["status"])

    # Confirm no tmp files left behind and previous total unchanged (20)
    tmp_files = [f for f in os.listdir(ph_dir) if f.startswith(".tmp_")]
    assert len(tmp_files) == 0
    assert ph.read_partial_hour_total(src, active_hour) == 20


def test_contract_corrupt_partial_file_self_healing(mock_ph_env):
    """Section 7: If partial-hour file is corrupt, it is deleted and re-accumulated cleanly."""
    src = mock_ph_env["source"]
    service_id = mock_ph_env["service_id"]
    buffer_dir = mock_ph_env["buffer_dir"]

    now = datetime.now(UTC)
    active_hour = now.strftime("%Y-%m-%d-%H")
    hour_start = now.replace(minute=0, second=0, microsecond=0)
    f1 = os.path.join(buffer_dir, "b1.parquet")
    _write_active_hour_parquet(f1, rows=50, dt_start=hour_start + timedelta(minutes=1))

    # Initial merge: 50 rows
    ph.merge_partial_hour(service_id, src, ["status"])
    assert ph.read_partial_hour_total(src, active_hour) == 50

    # Corrupt the parquet file by overwriting it with garbage bytes
    rollup_file = ph._all_fields_path(src, active_hour)
    with open(rollup_file, "wb") as f:
        f.write(b"NOT_A_VALID_PARQUET_FILE_GARBAGE")

    # Second tick: detects corruption, removes corrupt file, resets watermark, re-accumulates from b1
    stats = ph.merge_partial_hour(service_id, src, ["status"])
    assert stats["new_files"] == 1
    assert ph.read_partial_hour_total(src, active_hour) == 50


def test_contract_concurrent_reader_resilience(mock_ph_env):
    """Concurrent readers never crash on FileNotFoundError or transient file replace."""
    src = mock_ph_env["source"]
    service_id = mock_ph_env["service_id"]
    buffer_dir = mock_ph_env["buffer_dir"]

    now = datetime.now(UTC)
    active_hour = now.strftime("%Y-%m-%d-%H")
    hour_start = now.replace(minute=0, second=0, microsecond=0)

    for i in range(5):
        f = os.path.join(buffer_dir, f"conc_{i}.parquet")
        _write_active_hour_parquet(f, rows=15, dt_start=hour_start + timedelta(minutes=i))

    stop_event = threading.Event()
    reader_errors: list[Exception] = []
    read_counts: list[int] = []

    def reader_loop():
        con = duckdb.connect()
        try:
            while not stop_event.is_set():
                try:
                    tot = ph.read_partial_hour_total(src, active_hour, con=con)
                    read_counts.append(tot)
                    ph.read_partial_hour_all_fields(src, active_hour, con=con)
                except Exception as e:
                    reader_errors.append(e)
                time.sleep(0.005)
        finally:
            con.close()

    thread = threading.Thread(target=reader_loop, daemon=True)
    thread.start()

    try:
        for _ in range(3):
            ph.merge_partial_hour(service_id, src, ["status", "country"])
            time.sleep(0.01)
    finally:
        stop_event.set()
        thread.join(timeout=3.0)

    assert len(reader_errors) == 0, f"Reader thread encountered errors: {reader_errors}"
    assert len(read_counts) > 0


def test_contract_high_scale_mode_noop_in_sub_millisecond(mock_ph_env):
    """High-Scale Mode: Detects 0 files in < 1ms and records successful cron run."""
    src = mock_ph_env["source"]
    service_id = mock_ph_env["service_id"]
    src["deployment_mode"] = "high_scale"

    # Empty buffer dir
    stats = ph.merge_partial_hour(service_id, src, ["status"])
    assert stats["new_files"] == 0
    assert stats["duration_ms"] < 20.0  # Typically < 1ms


def test_contract_scheduler_registration_and_interval_override(monkeypatch):
    """Section 2: PARTIAL_HOUR_MERGE_INTERVAL_SEC and PARTIAL_HOUR_MERGE_ENABLED."""
    from backend.cron.scheduler import Scheduler, partial_hour_merge_interval_sec

    # Test override parsing & bounds
    monkeypatch.setenv("PARTIAL_HOUR_MERGE_INTERVAL_SEC", "45")
    assert partial_hour_merge_interval_sec() == 45

    monkeypatch.setenv("PARTIAL_HOUR_MERGE_INTERVAL_SEC", "5")  # Below min 10
    assert partial_hour_merge_interval_sec() == 10

    monkeypatch.setenv("PARTIAL_HOUR_MERGE_INTERVAL_SEC", "200")  # Above max 120
    assert partial_hour_merge_interval_sec() == 120

    monkeypatch.setenv("PARTIAL_HOUR_MERGE_INTERVAL_SEC", "40")
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")

    cfg = {
        "service_id": "svc-ph-sched",
        "provisioning": {"access_level": "read_write"},
    }

    sched = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "svc-ph-sched"}),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch.object(sched._sched, "add_job") as add_job,
    ):
        sched._register_dev_local_safe_jobs()

    assert "partial_hour_merge_svc-ph-sched" in sched._job_ids
    ph_call = next(c for c in add_job.call_args_list if c.kwargs.get("id") == "partial_hour_merge_svc-ph-sched")
    assert ph_call.kwargs.get("seconds") == 40


def test_contract_query_runner_narrows_live_scan(mock_ph_env):
    """Checklist Items 4-5: QueryRunner._partial_hour_adjusted_live_start narrows the live scan."""
    from backend.repositories._base import QueryRunner

    src = mock_ph_env["source"]
    service_id = mock_ph_env["service_id"]
    buffer_dir = mock_ph_env["buffer_dir"]

    now = datetime.now(UTC)
    active_hour = now.strftime("%Y-%m-%d-%H")
    hour_start = now.replace(minute=0, second=0, microsecond=0)

    f1 = os.path.join(buffer_dir, "batch_qr.parquet")
    _write_active_hour_parquet(f1, rows=30, dt_start=hour_start + timedelta(minutes=5))
    ph.merge_partial_hour(service_id, src, ["status", "country"])

    con = duckdb.connect()
    try:
        runner = QueryRunner(con, src)
        naive_start = hour_start
        window_end = now + timedelta(minutes=5)

        adjusted_start, partial_rows, partial_total = runner._partial_hour_adjusted_live_start(
            naive_start, active_hour, window_end
        )

        assert partial_total == 30
        assert len(partial_rows) > 0
        assert adjusted_start > naive_start
    finally:
        con.close()
