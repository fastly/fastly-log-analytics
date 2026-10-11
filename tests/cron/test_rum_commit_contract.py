"""Contract test suite for Cron 17: rum_commit_{service_id}.

Verifies all requirements specified in docs/cron/jobs/rum-commit.md:
1. Dual-table buffer drain into DuckLake (client_vitals and client_errors)
2. Ledger publication safety: only published when both tables succeed
3. Warning status and partial accounting on single-table failure
4. Error status when both tables fail or disk space pre-check fails
5. Automated tick bypass of active-request deferral gate
6. SSE sync status broadcast and freshness persistence
7. Natural key deduplication in DuckLake buffer commit and local compaction
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from backend.core.iceberg._ducklake import _ducklake_attach, ducklake_table_name
from backend.core.iceberg.buffer import _commit_buffer_impl
from backend.core.iceberg.rum_schema import CLIENT_VITALS_ARROW_SCHEMA
from backend.core.local_compaction import compact_local_partitions
from backend.cron.jobs import rum_commit as rum_commit_mod

SERVICE_ID = "svc_rum_contract_test"

FAKE_CFG = {
    "service_id": SERVICE_ID,
    "name": "RUM Contract Test Service",
    "provisioning": {"cron_sync": {"enabled": True}},
    "rum": {"enabled": True},
}

FAKE_SRC = {
    "service_id": SERVICE_ID,
    "name": "RUM Contract Test Service",
    "access_level": "read_write",
}


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)


@pytest.fixture
def mock_orchestration_env(monkeypatch):
    @contextmanager
    def mock_get_connection(*args, **kwargs):
        mock_con = MagicMock()
        mock_con.execute.return_value.fetchall.return_value = []
        yield mock_con

    import backend.core.iceberg._ducklake as _dl

    monkeypatch.setattr("backend.core.duckdb.get_connection", mock_get_connection)
    monkeypatch.setattr("backend.core.rollups.rum.recompute_rum_aggregates", MagicMock())
    monkeypatch.setattr(_dl, "_ducklake_attach", MagicMock())


def test_contract_full_commit_success_lifecycle(monkeypatch, mock_orchestration_env):
    """Full commit success contract:
    - Status is 'success'
    - Rows and files committed across both tables recorded
    - Ledger published (rum=True)
    - SSE sync-status notification published
    - Progress cleanly starts and ends
    """
    log_calls = []
    ledger_calls = []
    status_calls = []
    sse_published = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 1101)
    monkeypatch.setattr("backend.cron.scheduler._check_disk_space", lambda dir, sid, task: (True, ""))
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda src: "/tmp/cache")
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", lambda *a, **k: None)
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run",
        lambda *args, **kwargs: log_calls.append((args, kwargs)),
    )
    monkeypatch.setattr(
        "backend.core.ingest._mark_ledger_published",
        lambda sid, rum=False: ledger_calls.append((sid, rum)),
    )
    monkeypatch.setattr(
        "backend.config.update_status",
        lambda sid, status: status_calls.append((sid, status)),
    )

    def mock_commit(src, table_name):
        if table_name == "client_vitals":
            return {"files_committed": 3, "rows_committed": 45}
        if table_name == "client_errors":
            return {"files_committed": 2, "rows_committed": 15}
        return {}

    monkeypatch.setattr("backend.core.iceberg.commit_buffer", mock_commit)
    monkeypatch.setattr("backend.core.iceberg.sync_data", lambda src, table_name: None)

    start_progress = MagicMock()
    end_progress = MagicMock()
    monkeypatch.setattr("backend.cron_progress.start_progress", start_progress)
    monkeypatch.setattr("backend.cron_progress.end_progress", end_progress)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())

    latest = datetime(2026, 10, 11, 14, 30, tzinfo=UTC)
    monkeypatch.setattr("backend.core.rollups.rum.rum_count_and_latest", lambda con: (60, latest))

    mock_pub = MagicMock()
    mock_pub.publish.side_effect = lambda sid, snap: sse_published.append((sid, snap))
    monkeypatch.setattr("backend.sync_status_publisher.publisher", mock_pub)
    monkeypatch.setattr(
        "backend.sync_status_snapshot.compute_sync_status_cached",
        lambda sid: {"service_id": sid, "sync_stage": "idle"},
    )

    rum_commit_mod._run_rum_commit.__wrapped__(SERVICE_ID)

    start_progress.assert_called_once_with(1101, service_id=SERVICE_ID, task="rum_commit")
    end_progress.assert_called_once_with(1101)

    # Ledger publication verified
    assert ledger_calls == [(SERVICE_ID, True)]

    # Cron run log verified
    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert args[3] == "success"
    assert kwargs.get("run_id") == 1101
    assert kwargs.get("rows_ingested") == 60
    assert "Committed 3 vitals files (45 rows) and 2 errors files (15 rows)" in kwargs.get("summary", "")

    # SSE notification verified
    assert len(sse_published) == 1
    assert sse_published[0][0] == SERVICE_ID

    # Freshness update verified
    rum_updates = [s for _, s in status_calls if "rum" in s]
    assert rum_updates
    assert rum_updates[-1]["rum"]["total_rows"] == 60
    assert rum_updates[-1]["rum"]["latest_log_at"] == latest.isoformat()


def test_contract_partial_failure_warning_and_ledger_protection(monkeypatch, mock_orchestration_env):
    """Partial failure contract:
    - If client_errors fails, status is 'warning'
    - _mark_ledger_published is NOT called (preserves raw files)
    - Successful table (client_vitals) syncs metadata
    - Exception is safely handled without raising
    """
    log_calls = []
    ledger_calls = []
    sync_calls = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 1102)
    monkeypatch.setattr("backend.cron.scheduler._check_disk_space", lambda dir, sid, task: (True, ""))
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda src: "/tmp/cache")
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", lambda *a, **k: None)
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run",
        lambda *args, **kwargs: log_calls.append((args, kwargs)),
    )
    monkeypatch.setattr(
        "backend.core.ingest._mark_ledger_published",
        lambda sid, rum=False: ledger_calls.append((sid, rum)),
    )

    def mock_commit(src, table_name):
        if table_name == "client_vitals":
            return {"files_committed": 2, "rows_committed": 30}
        raise OSError("Transient FOS S3 error on errors commit")

    monkeypatch.setattr("backend.core.iceberg.commit_buffer", mock_commit)
    monkeypatch.setattr(
        "backend.core.iceberg.sync_data",
        lambda src, table_name: sync_calls.append(table_name),
    )
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())

    rum_commit_mod._run_rum_commit.__wrapped__(SERVICE_ID)

    # Client vitals metadata sync occurred
    assert "client_vitals" in sync_calls
    # Ledger publication was BLOCKED
    assert ledger_calls == []

    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert args[3] == "warning"
    assert kwargs.get("rows_ingested") == 30
    assert "Partial RUM commit" in kwargs.get("summary", "")
    assert "errors failed" in kwargs.get("summary", "")
    assert "Transient FOS S3 error" in kwargs.get("error_message", "")


def test_contract_both_tables_failure_error_status(monkeypatch, mock_orchestration_env):
    """Both tables failure contract:
    - Status is 'error'
    - Summary and error message describe both table failures
    - Ledger publication is blocked
    """
    log_calls = []
    ledger_calls = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 1103)
    monkeypatch.setattr("backend.cron.scheduler._check_disk_space", lambda dir, sid, task: (True, ""))
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda src: "/tmp/cache")
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", lambda *a, **k: None)
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run",
        lambda *args, **kwargs: log_calls.append((args, kwargs)),
    )
    monkeypatch.setattr(
        "backend.core.ingest._mark_ledger_published",
        lambda sid, rum=False: ledger_calls.append((sid, rum)),
    )

    def mock_commit(src, table_name):
        if table_name == "client_vitals":
            raise RuntimeError("Vitals commit network timeout")
        raise RuntimeError("Errors commit network timeout")

    monkeypatch.setattr("backend.core.iceberg.commit_buffer", mock_commit)
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())

    rum_commit_mod._run_rum_commit.__wrapped__(SERVICE_ID)

    assert ledger_calls == []
    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert args[3] == "error"
    assert "RUM commit failed for both tables" in kwargs.get("summary", "")
    assert "Vitals commit network timeout" in kwargs.get("error_message", "")
    assert "Errors commit network timeout" in kwargs.get("error_message", "")


def test_contract_low_disk_precheck_abort(monkeypatch, mock_orchestration_env):
    """Disk space safety contract:
    - When disk space is below safety threshold (<500MB), abort with status 'error'
    - Commit and sync are never attempted
    """
    commit_calls = []
    log_calls = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 1104)
    monkeypatch.setattr(
        "backend.cron.scheduler._check_disk_space",
        lambda dir, sid, task: (False, "Free disk space is 250MB (< 500MB minimum)"),
    )
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda src: "/tmp/cache")
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run",
        lambda *args, **kwargs: log_calls.append((args, kwargs)),
    )
    monkeypatch.setattr(
        "backend.core.iceberg.commit_buffer",
        lambda src, table_name: commit_calls.append(table_name),
    )

    rum_commit_mod._run_rum_commit.__wrapped__(SERVICE_ID)

    assert commit_calls == []
    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert args[3] == "error"
    assert "RUM commit aborted" in kwargs.get("summary", "")
    assert "Free disk space is 250MB" in kwargs.get("error_message", "")


def test_contract_automated_tick_no_deferral(monkeypatch, mock_orchestration_env):
    """Active-requests politeness gate removal contract (commit 5a7541fd):
    - Automated ticks proceed immediately and are NOT deferred by in-flight queries.
    """
    start_calls = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC)
    monkeypatch.setattr(
        "backend.core.duckdb.start_cron_run",
        lambda src, task: start_calls.append(task) or 1105,
    )
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda job, sid: True)

    with patch("backend.cron.scheduler._check_disk_space", return_value=(False, "disk check")):
        with patch("backend.core.duckdb.log_cron_run"):
            rum_commit_mod._run_rum_commit.__wrapped__(SERVICE_ID)
            # The run proceeded to start_cron_run without being deferred
            assert len(start_calls) == 1
            assert start_calls[0] == "rum_commit"


def test_contract_rum_buffer_natural_key_deduplication(tmp_path):
    """DuckLake buffer commit natural key deduplication contract:
    - client_vitals commits deduplicate rows by natural keys:
      ("timestamp", "metric_name", "pathname", "cid", "req_id")
    - Repeated or overlapping buffer files do NOT generate duplicate rows in DuckLake.
    """
    import duckdb

    cache_dir = tmp_path / "cache"
    buf_dir = cache_dir / "buffer" / "client_vitals"
    buf_dir.mkdir(parents=True)

    src = {
        "name": "rum_dedup_svc",
        "service_id": "rum_dedup_svc",
        "fos_local_warehouse": True,
        "_cache_dir_override": str(cache_dir),
        "duckdb_path": str(tmp_path / "rum_dedup.duckdb"),
        "ducklake_dir": str(tmp_path / "lake"),
    }

    ts1 = datetime(2026, 10, 11, 10, 0, tzinfo=UTC)
    ts2 = datetime(2026, 10, 11, 10, 5, tzinfo=UTC)

    # Chunk 1: Beacon A (LCP) and Beacon B (CLS)
    chunk1_table = pa.Table.from_pylist(
        [
            {
                "timestamp": ts1,
                "metric_name": "LCP",
                "metric_value": 2.5,
                "metric_rating": "good",
                "pathname": "/home",
                "browser": "Chrome",
                "os": "macOS",
                "device": "Desktop",
                "cid": "cid_user1",
                "req_id": "req_100",
                "city": "Denver",
                "region": "CO",
                "country": "US",
                "pop": "DEN",
                "tls": "1.3",
                "ttfb": 0.12,
            },
            {
                "timestamp": ts2,
                "metric_name": "CLS",
                "metric_value": 0.05,
                "metric_rating": "good",
                "pathname": "/about",
                "browser": "Chrome",
                "os": "macOS",
                "device": "Desktop",
                "cid": "cid_user2",
                "req_id": "req_101",
                "city": "Denver",
                "region": "CO",
                "country": "US",
                "pop": "DEN",
                "tls": "1.3",
                "ttfb": 0.08,
            },
        ],
        schema=CLIENT_VITALS_ARROW_SCHEMA,
    )
    pq.write_table(chunk1_table, str(buf_dir / "buf1.parquet"))

    res1 = _commit_buffer_impl(src, table_name="client_vitals")
    assert res1["files_committed"] == 1
    assert res1["rows_committed"] == 2

    # Chunk 2: Duplicate of Beacon A + New Beacon C (FID)
    chunk2_table = pa.Table.from_pylist(
        [
            {
                "timestamp": ts1,
                "metric_name": "LCP",
                "metric_value": 2.5,
                "metric_rating": "good",
                "pathname": "/home",
                "browser": "Chrome",
                "os": "macOS",
                "device": "Desktop",
                "cid": "cid_user1",
                "req_id": "req_100",
                "city": "Denver",
                "region": "CO",
                "country": "US",
                "pop": "DEN",
                "tls": "1.3",
                "ttfb": 0.12,
            },
            {
                "timestamp": ts2,
                "metric_name": "FID",
                "metric_value": 15.0,
                "metric_rating": "good",
                "pathname": "/contact",
                "browser": "Chrome",
                "os": "macOS",
                "device": "Desktop",
                "cid": "cid_user3",
                "req_id": "req_102",
                "city": "Denver",
                "region": "CO",
                "country": "US",
                "pop": "DEN",
                "tls": "1.3",
                "ttfb": 0.09,
            },
        ],
        schema=CLIENT_VITALS_ARROW_SCHEMA,
    )
    pq.write_table(chunk2_table, str(buf_dir / "buf2.parquet"))

    res2 = _commit_buffer_impl(src, table_name="client_vitals")
    assert res2["files_committed"] == 1

    # Verify table in DuckLake contains exactly 3 distinct rows (Beacon A was not duplicated)
    con = duckdb.connect()
    try:
        _ducklake_attach(con, src, read_only=True)
        tbl_name = ducklake_table_name(src, table_name="client_vitals")
        count = con.execute(f'SELECT count(*) FROM lake."{tbl_name}"').fetchone()[0]
        assert count == 3, f"Expected 3 unique rows after deduplicating commit, got {count}"
    finally:
        con.close()


def test_contract_rum_local_compaction_natural_key_deduplication(tmp_path):
    """Local compaction natural key deduplication contract:
    - compact_local_partitions on client_vitals merges files and deduplicates
      by composite natural key.
    """
    import duckdb

    cache_dir = tmp_path / "cache"
    part_dir = cache_dir / "data_client_vitals" / "timestamp_hour=2026-10-11-10"
    part_dir.mkdir(parents=True)

    src = {
        "name": "rum_compact_svc",
        "service_id": "rum_compact_svc",
        "_cache_dir_override": str(cache_dir),
    }

    ts = datetime(2026, 10, 11, 10, 15, tzinfo=UTC)

    # File 1 has Beacon A
    t1 = pa.Table.from_pylist(
        [
            {
                "timestamp": ts,
                "metric_name": "LCP",
                "metric_value": 3.1,
                "metric_rating": "needs-improvement",
                "pathname": "/product/1",
                "browser": "Firefox",
                "os": "Linux",
                "device": "Desktop",
                "cid": "cid_42",
                "req_id": "req_555",
                "city": "Austin",
                "region": "TX",
                "country": "US",
                "pop": "DFW",
                "tls": "1.3",
                "ttfb": 0.25,
            }
        ],
        schema=CLIENT_VITALS_ARROW_SCHEMA,
    )
    pq.write_table(t1, str(part_dir / "part1.parquet"))

    # File 2 has exact duplicate of Beacon A + new Beacon B
    t2 = pa.Table.from_pylist(
        [
            {
                "timestamp": ts,
                "metric_name": "LCP",
                "metric_value": 3.1,
                "metric_rating": "needs-improvement",
                "pathname": "/product/1",
                "browser": "Firefox",
                "os": "Linux",
                "device": "Desktop",
                "cid": "cid_42",
                "req_id": "req_555",
                "city": "Austin",
                "region": "TX",
                "country": "US",
                "pop": "DFW",
                "tls": "1.3",
                "ttfb": 0.25,
            },
            {
                "timestamp": ts,
                "metric_name": "CLS",
                "metric_value": 0.02,
                "metric_rating": "good",
                "pathname": "/product/1",
                "browser": "Firefox",
                "os": "Linux",
                "device": "Desktop",
                "cid": "cid_42",
                "req_id": "req_556",
                "city": "Austin",
                "region": "TX",
                "country": "US",
                "pop": "DFW",
                "tls": "1.3",
                "ttfb": 0.25,
            },
        ],
        schema=CLIENT_VITALS_ARROW_SCHEMA,
    )
    pq.write_table(t2, str(part_dir / "part2.parquet"))

    res = compact_local_partitions(
        src,
        min_files_per_partition=1,
        table_name="client_vitals",
        compact_active_hour=True,
    )
    assert res["partitions_compacted"] == 1
    assert res["files_merged"] == 2

    # Check the remaining parquet file in the partition
    remaining_files = [f for f in os.listdir(part_dir) if f.endswith(".parquet")]
    assert len(remaining_files) == 1

    con = duckdb.connect()
    try:
        merged_path = str(part_dir / remaining_files[0])
        rows = con.execute(f"SELECT * FROM read_parquet('{merged_path}')").fetchall()
        # Exactly 2 rows (duplicate Beacon A was removed)
        assert len(rows) == 2, f"Expected 2 rows in compacted parquet, found {len(rows)}"
    finally:
        con.close()
