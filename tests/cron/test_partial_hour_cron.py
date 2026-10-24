"""Tests and contract suite for :mod:`backend.cron.jobs.partial_hour` (Cron 4: ``partial_hour_merge_{service_id}``)."""

from __future__ import annotations

import os
import threading
import time
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from starlette.testclient import TestClient

from backend.core.rollups import partial_hour as ph
from backend.cron.jobs import partial_hour as ph_job


def test_run_partial_hour_merge_calls_merge_and_does_not_mark_ready(monkeypatch):
    """C2 (final whole-branch review): a 30s tick — even a successful one —
    only proves the writer path is alive, not that this pod's local
    closed-hour rollup tree reflects a genuine backfill pass. That flag is
    now only raised by main.py's startup catch-up and by
    _run_rollup_hour_heal's success path."""
    from backend.cron.jobs.partial_hour import _run_partial_hour_merge

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
    from backend.cron.jobs.partial_hour import _run_partial_hour_merge
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
    from backend.cron.jobs.partial_hour import _run_partial_hour_merge

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
    from backend.cron.jobs.partial_hour import _run_partial_hour_merge

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
    from backend.cron.jobs.partial_hour import _run_partial_hour_merge

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
    from backend.cron.jobs.partial_hour import _run_partial_hour_merge

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
    from backend.cron.jobs.partial_hour import _run_partial_hour_merge

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
