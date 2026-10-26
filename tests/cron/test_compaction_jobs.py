"""Consolidated tests for compaction cron jobs (local_compact, rollup_compact, rollup_heal, partial_hour_merge)."""

from __future__ import annotations

import collections
import contextlib
import logging as _logging
import os
import shutil
import threading
import time
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from starlette.testclient import TestClient

from backend.core import duckdb as _db
from backend.core import local_compaction as lc
from backend.core.metadata.cron_log import CronLeaseHeldError
from backend.core.rollups import partial_hour as ph
from backend.cron.jobs import compaction
from backend.cron.jobs.compaction import (
    _call_with_lookback,
    _run_partial_hour_merge,
    _run_rollup_compact_daily,
    _run_rollup_hour_heal,
)
from backend.cron.scheduler import TASK_MAP, Scheduler, partial_hour_merge_interval_sec
from backend.main import app
from backend.repositories._base import _LIVE_TOPN_SKIP_FIELDS, QueryRunner

Usage = collections.namedtuple("Usage", ["total", "used", "free"])

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


@pytest.fixture
def stub_progress(monkeypatch) -> dict[str, MagicMock]:
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=42))
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
    mocks = {k: MagicMock() for k in ("start_progress", "end_progress", "cleanup", "log_event")}
    monkeypatch.setattr("backend.cron_progress.start_progress", mocks["start_progress"])
    monkeypatch.setattr("backend.cron_progress.end_progress", mocks["end_progress"])
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", mocks["cleanup"])
    monkeypatch.setattr("backend.cron.scheduler._log_and_add_progress", mocks["log_event"])
    monkeypatch.setattr("backend.cron.scheduler._extract_log_text", lambda rid: "")
    return mocks


@pytest.mark.parametrize(
    "job_fn,target_attr",
    [
        (compaction._run_local_compact, "backend.core.local_compaction.compact_local_partitions"),
        (compaction._run_rollup_compact_daily, "backend.core.rollups.compact_closed_days_to_daily"),
        (_run_rollup_hour_heal, "backend.core.rollups.backfill_missing_hour_bundles"),
    ],
)
def test_compaction_jobs_missing_source_and_lease_skip(monkeypatch, stub_source, stub_progress, job_fn, target_attr):
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)
    job_fn.__wrapped__("missing-svc")

    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: stub_source)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(side_effect=CronLeaseHeldError("running")))
    work_mock = MagicMock()
    monkeypatch.setattr(target_attr, work_mock)
    job_fn.__wrapped__("svc-1")
    work_mock.assert_not_called()
    stub_progress["start_progress"].assert_not_called()


# ── _run_local_compact & _run_rollup_compact_daily ────────────────────────────


def test_local_compact_outcomes(monkeypatch, stub_source, stub_progress):
    monkeypatch.setattr(
        "backend.core.local_compaction.compact_local_partitions",
        MagicMock(return_value={"partitions_compacted": 3, "files_merged": 12, "files_removed": 9, "errors": []}),
    )
    compaction._run_local_compact.__wrapped__("svc-1")
    args, kwargs = _db.log_cron_run.call_args
    assert args[3] == "success" and "Compacted 3 partition" in kwargs["summary"] and kwargs["error_message"] is None
    stub_progress["start_progress"].assert_called_once()
    stub_progress["end_progress"].assert_called_once()

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
    args, kwargs = _db.log_cron_run.call_args
    assert args[3] == "warning" and "4 partition error" in kwargs["summary"]
    assert "partition a failed" in kwargs["error_message"] and "1 more" in kwargs["error_message"]

    monkeypatch.setattr(
        "backend.core.local_compaction.compact_local_partitions", MagicMock(side_effect=RuntimeError("disk full"))
    )
    compaction._run_local_compact.__wrapped__("svc-1")
    args, kwargs = _db.log_cron_run.call_args
    assert args[3] == "error" and "disk full" in kwargs["error_message"]


def _mock_rollups(monkeypatch):
    monkeypatch.setattr("backend.core.rollups.compact_closed_days_to_daily", lambda *a, **kw: 0)
    monkeypatch.setattr("backend.core.rollups.backfill_day_bundles", lambda *a, **kw: 0)
    monkeypatch.setattr("backend.core.rollups.retire_compacted_hour_bundles", lambda *a, **kw: (0, 0))
    monkeypatch.setattr(
        "backend.core.rollups.backfill_missing_hour_bundles", lambda *a, **kw: {"missing": 0, "bundled": 0}
    )
    monkeypatch.setattr(
        "backend.core.rollups.backfill_missing_hour_ip_spread", lambda *a, **kw: {"missing": 0, "rebuilt": 0}
    )
    for label in CRON_DAY_COMPACTOR_ORDER:
        if hasattr(__import__("backend.core.rollups", fromlist=["*"]), f"compact_{label}_closed_days_to_daily"):
            monkeypatch.setattr(f"backend.core.rollups.compact_{label}_closed_days_to_daily", lambda *a, **kw: 0)


def test_rollup_compact_success_warning_error_and_provided_run_id(monkeypatch, stub_source, stub_progress, caplog):
    _mock_rollups(monkeypatch)
    monkeypatch.setattr("backend.core.rollups.compact_closed_days_to_daily", MagicMock(return_value=14))
    monkeypatch.setattr("backend.core.rollups.backfill_day_bundles", MagicMock(return_value=7))
    compaction._run_rollup_compact_daily.__wrapped__("svc-1")
    args, kwargs = _db.log_cron_run.call_args
    assert args[3] == "success" and "Rebuilt 14" in kwargs["summary"] and "bundled 7 day" in kwargs["summary"]

    monkeypatch.setattr("backend.core.rollups.compact_closed_days_to_daily", MagicMock(return_value=5))
    monkeypatch.setattr("backend.core.rollups.backfill_day_bundles", MagicMock(side_effect=RuntimeError("disk full")))
    with caplog.at_level(_logging.WARNING, logger="backend.scheduler"):
        compaction._run_rollup_compact_daily.__wrapped__("svc-1")
    args, kwargs = _db.log_cron_run.call_args
    assert (
        args[3] == "warning"
        and "day_bundles: disk full" in kwargs["error_message"]
        and "bundled 0 day" in kwargs["summary"]
    )

    start_mock = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start_mock)
    _mock_rollups(monkeypatch)
    compaction._run_rollup_compact_daily.__wrapped__("svc-1", run_id=321)
    start_mock.assert_not_called()

    stub_progress["start_progress"].side_effect = RuntimeError("manifest read failed")
    compaction._run_rollup_compact_daily.__wrapped__("svc-1")
    args, kwargs = _db.log_cron_run.call_args
    assert args[3] == "error" and "manifest read failed" in kwargs["error_message"]


@pytest.mark.parametrize(
    "fail_label,expected_status,expected_err",
    [(None, "success", None), ("ngwaf_bots", "warning", "ngwaf_bots: boom")],
)
def test_rollup_compact_day_compactor_order_and_isolation(
    monkeypatch, stub_source, stub_progress, fail_label, expected_status, expected_err
):
    _mock_rollups(monkeypatch)
    calls: list = []
    for label in CRON_DAY_COMPACTOR_ORDER:

        def compactor(*args, _label=label, **kwargs):
            calls.append((_label, args, kwargs))
            if _label == fail_label:
                raise RuntimeError("boom")
            return 0

        monkeypatch.setattr(f"backend.core.rollups.compact_{label}_closed_days_to_daily", compactor)

    compaction._run_rollup_compact_daily.__wrapped__("svc-1")
    assert [c[0] for c in calls] == CRON_DAY_COMPACTOR_ORDER
    assert all(args == ("svc-1", stub_source) and kwargs == {"lookback_days": 30} for _, args, kwargs in calls)
    args, kwargs = _db.log_cron_run.call_args
    assert args[3] == expected_status and kwargs["error_message"] == expected_err


# ── Local compact contract ───────────────────────────────────────────────────


def _write_sample_parquet(path: str, rows: int, ts_start: int = 0, rid_start: int | None = None) -> None:
    cols = {
        "timestamp": pa.array(range(ts_start, ts_start + rows), type=pa.int64()),
        "ip": pa.array([f"192.168.1.{i % 254 + 1}" for i in range(rows)]),
        "status": pa.array([200 if i % 10 != 0 else 500 for i in range(rows)], type=pa.int32()),
    }
    if rid_start is not None:
        cols["rid"] = pa.array([f"rid_{rid_start + i}" for i in range(rows)])
    pq.write_table(pa.table(cols), path, compression="zstd")


@pytest.fixture
def mock_service_env(tmp_path, monkeypatch):
    service_id = "test-compaction-svc"
    cache_root = tmp_path / "cache" / service_id
    data_dir = cache_root / "data"
    data_dir.mkdir(parents=True)
    src = {
        "name": service_id,
        "service_id": service_id,
        "bucket": service_id,
        "_test_cache_root": str(cache_root),
        "provisioning": {"access_level": "read_write", "cron_compact": {"enabled": True}},
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
        service_id = mock_service_env["service_id"]
        part_dir = os.path.join(mock_service_env["data_dir"], "timestamp_hour=2026-05-30-00")
        os.makedirs(part_dir, exist_ok=True)
        for i in range(10):
            _write_sample_parquet(os.path.join(part_dir, f"batch_{i:02d}.parquet"), 15, i * 100, i * 100)

        resp = TestClient(app).post(f"/api/admin/compact/{service_id}?min_files=3")
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert (data["files_merged"], data["files_removed"], data["partitions_compacted"], data["errors"]) == (
            10,
            10,
            1,
            [],
        )

        remaining = [f for f in os.listdir(part_dir) if f.endswith(".parquet")]
        assert len(remaining) == 1 and remaining[0].startswith("compacted_")
        assert [f for f in os.listdir(part_dir) if f.endswith(".tmp")] == []

        with duckdb.connect() as con:
            compacted_path = os.path.join(part_dir, remaining[0])
            assert con.execute(f"SELECT COUNT(*) FROM read_parquet('{compacted_path}')").fetchone()[0] == 150

    def test_contract_zero_outbound_fos_calls(self, mock_service_env, monkeypatch):
        src, service_id = mock_service_env["source"], mock_service_env["service_id"]
        part_dir = os.path.join(mock_service_env["data_dir"], "timestamp_hour=2026-05-30-00")
        os.makedirs(part_dir, exist_ok=True)
        for i in range(4):
            _write_sample_parquet(os.path.join(part_dir, f"b_{i}.parquet"), rows=10, ts_start=i * 10)

        monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
        monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value="test_run_123"))
        with patch("urllib.request.urlopen", side_effect=AssertionError("HTTP request made")):
            res = lc.compact_local_partitions(src)
            assert res["partitions_compacted"] == 1 and res["files_merged"] == 4
            compaction._run_local_compact.__wrapped__(service_id)

    def test_contract_atomic_replacement_and_crash_safety(self, mock_service_env, monkeypatch):
        src = mock_service_env["source"]
        part_dir = os.path.join(mock_service_env["data_dir"], "timestamp_hour=2026-05-30-00")
        os.makedirs(part_dir, exist_ok=True)
        for i in range(5):
            _write_sample_parquet(os.path.join(part_dir, f"file_{i}.parquet"), rows=10, ts_start=i * 10)

        call_order: list[str] = []
        real_rename, real_remove = os.rename, os.remove
        monkeypatch.setattr(
            os,
            "rename",
            lambda s, d: call_order.append(f"rename:{os.path.basename(s)}->{os.path.basename(d)}") or real_rename(s, d),
        )
        monkeypatch.setattr(
            os, "remove", lambda p: call_order.append(f"remove:{os.path.basename(p)}") or real_remove(p)
        )

        lc.compact_local_partitions(src)
        assert len(call_order) == 6 and call_order[0].startswith("rename:compacted_")
        assert all(a.startswith("remove:file_") for a in call_order[1:])

        part_dir_crash = os.path.join(mock_service_env["data_dir"], "timestamp_hour=2026-05-30-01")
        os.makedirs(part_dir_crash, exist_ok=True)
        crash_paths = [os.path.join(part_dir_crash, f"crash_{i}.parquet") for i in range(4)]
        for i, fp in enumerate(crash_paths):
            _write_sample_parquet(fp, rows=10, ts_start=i * 10)

        monkeypatch.setattr(os, "rename", MagicMock(side_effect=OSError("Simulated I/O error")))
        res = lc.compact_local_partitions(src)
        assert res["errors"] and all(os.path.exists(fp) for fp in crash_paths)

    def test_contract_concurrent_queries_during_compaction_no_filenotfound(self, mock_service_env):
        src = mock_service_env["source"]
        part_dir = os.path.join(mock_service_env["data_dir"], "timestamp_hour=2026-05-30-00")
        os.makedirs(part_dir, exist_ok=True)
        for i in range(8):
            _write_sample_parquet(os.path.join(part_dir, f"cq_{i}.parquet"), rows=50, ts_start=i * 50)

        stop_event = threading.Event()
        query_errors: list[Exception] = []
        query_counts: list[int] = []

        def reader_loop():
            with duckdb.connect() as con:
                runner = QueryRunner(con, src)
                glob_pattern = os.path.join(mock_service_env["data_dir"], "**", "*.parquet")
                while not stop_event.is_set():
                    try:
                        query_counts.append(
                            runner.execute(f"SELECT COUNT(*) FROM read_parquet('{glob_pattern}')").fetchone()[0]
                        )
                    except Exception as e:
                        query_errors.append(e)
                    time.sleep(0.005)

        with (
            patch("backend.core.iceberg.view.update_iceberg_view", lambda *a, **k: None),
            patch("backend.core.iceberg.update_iceberg_view", lambda *a, **k: None),
        ):
            thread = threading.Thread(target=reader_loop, daemon=True)
            thread.start()
            try:
                lc.compact_local_partitions(src)
            finally:
                stop_event.set()
                thread.join(timeout=3.0)

        assert not [e for e in query_errors if isinstance(e, FileNotFoundError) or "No such file" in str(e)]
        assert len(query_counts) > 0

    def test_contract_enospc_precheck_skips_safely(self, mock_service_env, monkeypatch):
        src = mock_service_env["source"]
        part_dir = os.path.join(mock_service_env["data_dir"], "timestamp_hour=2026-05-30-00")
        os.makedirs(part_dir, exist_ok=True)
        for i in range(5):
            _write_sample_parquet(os.path.join(part_dir, f"enospc_{i}.parquet"), rows=20)

        monkeypatch.setattr(shutil, "disk_usage", lambda p: Usage(total=10**9, used=5 * 10**8, free=100))
        res = lc.compact_local_partitions(src)
        assert res["partitions_compacted"] == 0 and any("ENOSPC" in err for err in res["errors"])
        assert len([f for f in os.listdir(part_dir) if f.endswith(".parquet")]) == 5

    def test_contract_scheduler_and_high_scale_ledger_recompute(self, monkeypatch):
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

        assert "local_compact_svc-contract-test" in sched._job_ids
        lc_call = next(c for c in add_job.call_args_list if c.kwargs.get("id") == "local_compact_svc-contract-test")
        assert lc_call.kwargs.get("minutes") == 5

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


# ── Rollup compact contract ──────────────────────────────────────────────────


def _write_synthetic_hour_bundle(cache_root: str, hour: str, rows: list[dict]) -> tuple[str, str]:
    d = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    fields = pa.array([r["field"] for r in rows])
    vals = pa.array([r["value"] for r in rows])
    cnts = pa.array([r["count"] for r in rows], type=pa.int64())
    p, ip_p = os.path.join(d, "all_fields.parquet"), os.path.join(d, "all_fields_ip.parquet")
    pq.write_table(pa.table({"field": fields, "value": vals, "count": cnts}), p)
    pq.write_table(pa.table({"field": fields, "value": vals, "unique_ips": cnts}), ip_p)
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
    src, sid, cache_root = (
        contract_service_source,
        contract_service_source["service_id"],
        contract_service_source["cache_dir"],
    )
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda s: cache_root)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s_id: src if s_id == sid else None)
    monkeypatch.setattr("backend.deps.get_source", lambda: src)
    monkeypatch.setattr(
        "backend.core.rollups.backfill_missing_hour_bundles", lambda *a, **k: {"missing": 0, "bundled": 0}
    )
    monkeypatch.setattr(
        "backend.core.rollups.backfill_missing_hour_ip_spread", lambda *a, **k: {"missing": 0, "rebuilt": 0}
    )

    target_day = (datetime.now(UTC) - timedelta(days=2)).strftime("%Y-%m-%d")
    hour_bundled_root = os.path.join(cache_root, "rollups", "hour_bundled")
    hourly_rows = [
        {"field": "ua", "value": "Mozilla/5.0", "count": 10},
        {"field": "status", "value": "200", "count": 50},
    ]
    for h in range(24):
        _write_synthetic_hour_bundle(cache_root, f"{target_day}-{h:02d}", hourly_rows)

    resp = client.post(f"/api/admin/rollups/compact/{sid}")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert (body["status"], body["service_id"], body["retired_files"]) == ("success", sid, 24)
    assert body["rebuilt"] >= 1 and body["bundled"] >= 1

    day_bundled_root = os.path.join(cache_root, "rollups", "day_bundled")
    flat_alias = os.path.join(day_bundled_root, f"day_bundle_{target_day}.parquet")
    for p in (
        os.path.join(day_bundled_root, f"day={target_day}", "all_fields.parquet"),
        os.path.join(day_bundled_root, f"day={target_day}", f"day_bundle_{target_day}.parquet"),
        flat_alias,
    ):
        assert os.path.isfile(p)

    with duckdb.connect() as con:
        assert (
            con.execute(
                f"SELECT sum(count) FROM read_parquet('{flat_alias}') WHERE field = 'ua' AND value = 'Mozilla/5.0'"
            ).fetchone()[0]
            == 240
        )
        assert (
            con.execute(
                f"SELECT sum(count) FROM read_parquet('{flat_alias}') WHERE field = 'status' AND value = '200'"
            ).fetchone()[0]
            == 1200
        )

    for h in range(24):
        hour_dir = os.path.join(hour_bundled_root, f"hour={target_day}-{h:02d}")
        assert not os.path.exists(os.path.join(hour_dir, "all_fields.parquet")) and os.path.isfile(
            os.path.join(hour_dir, "all_fields_ip.parquet")
        )

    runner = QueryRunner(duckdb.connect(":memory:"), src)
    t0 = time.perf_counter()
    phase_log: list[dict] = []
    next_day = (datetime.strptime(target_day, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    res_tuples, fields_ret = runner.execute_top_n_rollups(
        ["ua", "status"], start_time=f"{target_day}T00:00:00Z", end_time=f"{next_day}T00:00:00Z", _phase_log=phase_log
    )
    duration_s = time.perf_counter() - t0
    timings = {p["section"].replace("top_n_rollups:", ""): p["time_ms"] for p in phase_log}
    assert timings.get("dir_enum:n_bundled_day_files", 0) >= 1
    assert timings.get("dir_enum:n_bundled_hour_files", 0) == 0 and timings.get("dir_enum:n_hour_files", 0) == 0

    res = {f: [] for f in fields_ret}
    for field, val, count in res_tuples:
        res[field].append({"value": val, "count": count})
    assert (
        res["ua"][0] == {"value": "Mozilla/5.0", "count": 240}
        and res["status"][0] == {"value": "200", "count": 1200}
        and duration_s < 0.150
    )


def test_rollup_compact_gates_and_subsystems(contract_service_source, client, monkeypatch):
    src, sid, cache_root = (
        contract_service_source,
        contract_service_source["service_id"],
        contract_service_source["cache_dir"],
    )
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s_id: src if s_id == sid else None)
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda s: cache_root)
    monkeypatch.setattr("backend.deps.get_source", lambda: src)

    # 1. Politeness gate
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda task, s_id: True)
    res = _run_rollup_compact_daily.__wrapped__(sid, manual=False)
    assert res["status"] == "deferred" and "active" in res["summary"].lower()
    _mock_rollups(monkeypatch)
    assert _run_rollup_compact_daily.__wrapped__(sid, manual=True)["status"] == "success"

    # 2. Zero FOS egress
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda task, s_id: False)
    mock_fos = MagicMock(side_effect=AssertionError("FOS client invoked"))
    monkeypatch.setattr("backend.core.duckdb._get_fos_client", mock_fos)
    assert _run_rollup_compact_daily.__wrapped__(sid, manual=True)["status"] == "success"
    mock_fos.assert_not_called()

    # 3. Subsystem warnings
    monkeypatch.setattr("backend.core.rollups.compact_closed_days_to_daily", lambda *a, **k: 1)
    monkeypatch.setattr("backend.core.rollups.backfill_day_bundles", lambda *a, **k: 1)
    monkeypatch.setattr("backend.core.rollups.retire_compacted_hour_bundles", lambda *a, **k: (24, 1024))
    monkeypatch.setattr(
        "backend.core.rollups.compact_origin_summary_closed_days_to_daily",
        MagicMock(side_effect=RuntimeError("origin summary disk timeout")),
    )
    monkeypatch.setattr(
        "backend.core.rollups.compact_security_dims_closed_days_to_daily",
        MagicMock(side_effect=RuntimeError("security dims binder error")),
    )
    logged_runs: list = []
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **k: logged_runs.append((a, k)))
    res_warn = _run_rollup_compact_daily.__wrapped__(sid, manual=True)
    assert res_warn["status"] == "warning" and all(
        any(k in e for e in res_warn["errors"]) for k in ("origin_summary", "security_dims")
    )
    assert logged_runs[-1][0][3] == "warning" and "origin_summary" in logged_runs[-1][1].get("error_message", "")

    # 4. ENOSPC disk safety check
    monkeypatch.setattr(
        shutil, "disk_usage", lambda path: Usage(total=10**10, used=10**10 - 20 * 1024 * 1024, free=20 * 1024 * 1024)
    )
    res_enospc = _run_rollup_compact_daily.__wrapped__(sid, manual=True)
    assert res_enospc["status"] == "warning" and "critically low" in res_enospc["summary"].lower()

    # 5. Admin status endpoint
    day_dir = os.path.join(cache_root, "rollups", "day_bundled", "day=2026-06-01")
    os.makedirs(day_dir, exist_ok=True)
    open(os.path.join(day_dir, "all_fields.parquet"), "wb").close()
    for h in ("2026-06-02-10", "2026-06-02-11"):
        os.makedirs(os.path.join(cache_root, "rollups", "hour_bundled", f"hour={h}"), exist_ok=True)
    data = client.get(f"/api/admin/rollups/status/{sid}").json()
    assert (data["service_id"], data["days_bundled"], data["hour_bundles"], data["latest_day_bundle"]) == (
        sid,
        1,
        2,
        "2026-06-01",
    )


def test_call_with_lookback_does_not_rerun_unbounded_on_inner_typeerror():
    calls: list[dict] = []

    def _fn(*args, lookback_days=None):
        calls.append({"lookback_days": lookback_days})
        raise TypeError("bad value for lookback_days")

    with pytest.raises(TypeError):
        _call_with_lookback(_fn, "svc", {}, lookback_days=30)
    assert calls == [{"lookback_days": 30}]


# ── Rollup heal contract ─────────────────────────────────────────────────────


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

    @staticmethod
    def _setup_heal(monkeypatch, src, *, run_id=101, heal_return=None, heal_side_effect=None):
        sid = src["service_id"]
        monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s_id: src if s_id == sid else None)
        monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=run_id))
        logged_runs = []
        monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **kw: logged_runs.append((a, kw)))
        default_ret = {"missing": 1, "rebuilt_fields": 8, "bundled": 1, "stamped_empty": 0, "coverage_verified": True}
        heal_mock = (
            MagicMock(side_effect=heal_side_effect)
            if heal_side_effect is not None
            else MagicMock(return_value=heal_return or default_ret)
        )
        monkeypatch.setattr("backend.core.rollups.backfill_missing_hour_bundles", heal_mock)
        return sid, heal_mock, logged_runs

    def test_scheduler_registration_and_schedule_mapping(self, frozen_clock):
        assert TASK_MAP.get("rollup_heal") == "rollup_hour_heal"
        sched = Scheduler()
        mock_add = MagicMock()
        sched._add_job = mock_add
        cfg = {
            "service_id": "svc-dev-test",
            "name": "Dev Test Svc",
            "fastly": {"service_id": "svc-dev-test"},
            "provisioning": {"access_level": "read_write"},
        }
        expected_startup = datetime.now(UTC) + timedelta(seconds=5)

        for method_name, patch_target, patch_val in (
            ("_register_dev_local_safe_jobs", "backend.cron.scheduler.dev_local_crons_enabled", True),
            ("_sync_jobs", "backend.cron.scheduler.dev_mode_no_crons", False),
        ):
            mock_add.reset_mock()
            sched._job_ids.clear()
            with (
                patch("backend.config.list_configs", return_value=[cfg]),
                patch("backend.core.duckdb.get_source_for_service", return_value=dict(cfg)),
                patch("backend.core.duckdb.is_configured", return_value=True),
                patch(patch_target, return_value=patch_val),
            ):
                getattr(sched, method_name)()

            calls = [c for c in mock_add.call_args_list if c.kwargs.get("id") == "rollup_heal_svc-dev-test"]
            assert len(calls) == 1
            call = calls[0]
            assert call.args == (_run_rollup_hour_heal, "cron") and call.kwargs.get("minute") == 5
            assert call.kwargs.get("args") == ["svc-dev-test"] and call.kwargs.get("next_run_time") == expected_startup

    def test_rollup_heal_lookback_sentinels_politeness_enospc_egress_and_errors(
        self, contract_service_source, monkeypatch
    ):
        # 1. 48h lookback & sentinels
        sid, heal_mock, logged_runs = self._setup_heal(
            monkeypatch,
            contract_service_source,
            heal_return={
                "missing": 3,
                "rebuilt_fields": 12,
                "bundled": 3,
                "stamped_empty": 1,
                "coverage_verified": True,
            },
        )
        res = _run_rollup_hour_heal.__wrapped__(sid)
        assert heal_mock.call_args.kwargs.get("lookback_days") == 2 and res["status"] == "success"
        assert (res["missing"], res["rebuilt_fields"], res["bundled"], res["stamped_empty"]) == (3, 12, 3, 1)
        assert (
            "Healed 3 missing hour(s)" in logged_runs[0][1]["summary"]
            and "1 empty hour(s) stamped" in logged_runs[0][1]["summary"]
        )

        # 2. Empty hour sentinels only
        sid, _, logged2 = self._setup_heal(
            monkeypatch,
            contract_service_source,
            heal_return={
                "missing": 0,
                "rebuilt_fields": 0,
                "bundled": 0,
                "stamped_empty": 4,
                "coverage_verified": True,
            },
        )
        res_empty = _run_rollup_hour_heal.__wrapped__(sid)
        assert res_empty["stamped_empty"] == 4 and "4 empty hour(s) stamped" in logged2[0][1]["summary"]

        # 3. Politeness gate & manual bypass
        sid, heal_mock, _ = self._setup_heal(
            monkeypatch, contract_service_source, heal_return={"missing": 0, "rebuilt_fields": 0, "bundled": 0}
        )
        monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda task, s_id: True)
        assert _run_rollup_hour_heal.__wrapped__(sid, manual=False)["status"] == "deferred"
        heal_mock.assert_not_called()
        assert _run_rollup_hour_heal.__wrapped__(sid, manual=True)["status"] == "success"
        monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda task, s_id: False)

        # 4. Zero FOS egress
        boto_mock = MagicMock(side_effect=AssertionError("Outbound FOS network call forbidden"))
        monkeypatch.setattr("boto3.client", boto_mock)
        assert _run_rollup_hour_heal.__wrapped__(sid)["status"] == "success"
        boto_mock.assert_not_called()

        # 5. Error handling
        sid, _, logged_err = self._setup_heal(
            monkeypatch, contract_service_source, heal_side_effect=RuntimeError("DuckDB connection pool exhausted")
        )
        res_err = _run_rollup_hour_heal.__wrapped__(sid)
        assert res_err["status"] == "error" and "DuckDB connection pool exhausted" in logged_err[0][1]["error_message"]

        # 6. ENOSPC safety
        sid, heal_mock, logged_disk = self._setup_heal(monkeypatch, contract_service_source)
        monkeypatch.setattr("shutil.disk_usage", lambda path: MagicMock(free=10 * 1024 * 1024))
        res_disk = _run_rollup_hour_heal.__wrapped__(sid)
        assert res_disk["status"] == "warning" and "Skipped due to low disk space" in logged_disk[0][1]["summary"]
        heal_mock.assert_not_called()

    def test_rollup_heal_durable_mode_throttle_and_readiness_guards(self, contract_service_source, monkeypatch):
        sid, heal_partial, _ = self._setup_heal(
            monkeypatch,
            contract_service_source,
            heal_return={"missing": 1, "rebuilt_fields": 5, "bundled": 1, "coverage_verified": False},
        )
        monkeypatch.setattr("backend.config.is_durable_serving_mode", lambda s: True)
        monkeypatch.setattr("backend.core.rollups._common.rollup_coverage_ready", lambda s_id: False)
        mark_ready_mock = MagicMock()
        monkeypatch.setattr("backend.core.rollups._common.mark_rollup_coverage_ready", mark_ready_mock)

        assert _run_rollup_hour_heal.__wrapped__(sid)["status"] == "success"
        assert heal_partial.call_args.kwargs.get("max_missing_hours") == 1
        mark_ready_mock.assert_not_called()

        # Non-durable mode does not mark ready
        monkeypatch.setattr("backend.config.is_durable_serving_mode", lambda s: False)
        _run_rollup_hour_heal.__wrapped__(sid)
        mark_ready_mock.assert_not_called()

        # Failure in durable mode does not mark ready
        monkeypatch.setattr("backend.config.is_durable_serving_mode", lambda s: True)
        self._setup_heal(monkeypatch, contract_service_source, heal_side_effect=RuntimeError("view refresh failed"))
        _run_rollup_hour_heal.__wrapped__(sid)
        mark_ready_mock.assert_not_called()

        # Verified coverage in durable mode marks ready
        monkeypatch.setattr(
            "backend.core.rollups.backfill_missing_hour_bundles",
            MagicMock(return_value={"missing": 0, "rebuilt_fields": 0, "bundled": 0, "coverage_verified": True}),
        )
        assert _run_rollup_hour_heal.__wrapped__(sid)["status"] == "success"
        mark_ready_mock.assert_called_once_with(sid)

    def test_admin_trigger_and_inspection_parity(self, contract_service_source, monkeypatch):
        from backend.deps import get_source

        src = contract_service_source
        sid, _, _ = self._setup_heal(
            monkeypatch,
            src,
            heal_return={
                "missing": 2,
                "rebuilt_fields": 10,
                "bundled": 2,
                "stamped_empty": 0,
                "coverage_verified": True,
            },
        )
        app.dependency_overrides[get_source] = lambda: src
        client = TestClient(app)
        try:
            data1 = client.post(f"/api/admin/rollup-heal/{sid}").json()
            assert (data1["status"], data1["service_id"], data1["missing"], data1["bundled"]) == ("success", sid, 2, 2)
            assert client.post("/api/admin/rollup-heal", headers={"x-service-id": sid}).json()["status"] == "success"
            assert client.post(f"/api/admin/rollups/heal/{sid}").json()["status"] == "success"

            import backend.core.rollups as rollups_pkg

            for fn_name in dir(rollups_pkg):
                if fn_name.startswith(("backfill_", "compact_")) and callable(getattr(rollups_pkg, fn_name)):
                    monkeypatch.setattr(f"backend.core.rollups.{fn_name}", lambda *a, **k: 0)
            monkeypatch.setattr("backend.core.rollups._common.backfill_missing_bundles", lambda *a, **k: 0)
            monkeypatch.setattr("backend.core.duckdb.get_connection", lambda *a, **k: MagicMock())
            monkeypatch.setattr("backend.core.rollups.rum.recompute_rum_aggregates", lambda *a, **k: None)
            monkeypatch.setattr("backend.core.rollups.rum.table_exists", lambda *a, **k: False)

            data4 = client.post("/api/admin/backfill-bundle-rollups", headers={"x-service-id": sid}).json()
            assert "slow_urls" in data4 and "origin_summary" in data4
        finally:
            app.dependency_overrides.pop(get_source, None)


# ── Partial-hour merge cron & contract tests ─────────────────────────────────


@contextlib.contextmanager
def _partial_hour_merge_ctx(monkeypatch, fields: list[str], *, new_files: int = 1, duration_ms: float = 1.0):
    fake_src = {"service_id": "svc-a", "name": "svc-a"}
    monkeypatch.setattr("backend.repositories.dashboard.FIELDS", fields)
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
            return_value={"hour": "2026-09-09-14", "new_files": new_files, "duration_ms": duration_ms},
        ) as mock_merge,
        patch("backend.core.rollups.partial_hour.gc_stale_partial_hours", return_value=0),
        patch("backend.core.rollups._common.mark_rollup_coverage_ready") as mock_mark,
        patch("backend.core.metadata.delete_cron_run") as mock_delete,
    ):
        yield {"merge": mock_merge, "log": mock_log, "mark": mock_mark, "delete": mock_delete}


@pytest.mark.parametrize(
    "extra_excluded",
    [
        [next(iter(_LIVE_TOPN_SKIP_FIELDS))],
        ["requests", "hit_rate"],
        ["_bot_name", "waf_sig_ind"],
    ],
)
def test_run_partial_hour_merge_filters_excluded_fields(monkeypatch, extra_excluded):
    with _partial_hour_merge_ctx(monkeypatch, ["country", *extra_excluded], new_files=3, duration_ms=12.5) as mocks:
        _run_partial_hour_merge("svc-a")
    merged_fields = mocks["merge"].call_args[0][2]
    assert "country" in merged_fields and all(f not in merged_fields for f in extra_excluded)
    mocks["mark"].assert_not_called()
    mocks["log"].assert_called_once()


@pytest.mark.parametrize("new_files", [0, 2])
def test_run_partial_hour_merge_keeps_cron_runs_row_and_defers(monkeypatch, new_files):
    with _partial_hour_merge_ctx(monkeypatch, ["country"], new_files=new_files) as mocks:
        _run_partial_hour_merge("svc-a")
    mocks["log"].assert_called_once()
    mocks["delete"].assert_not_called()

    with (
        patch("backend.utils.active_requests.should_defer_cron", return_value=True),
        patch("backend.core.duckdb.start_cron_run") as mock_start,
    ):
        _run_partial_hour_merge("svc-a")
    mock_start.assert_not_called()


def _write_active_hour_parquet(path: str, rows: int, dt_start: datetime) -> None:
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


def _active_hour_bounds() -> tuple[datetime, str, datetime]:
    now = datetime.now(UTC)
    return now, now.strftime("%Y-%m-%d-%H"), now.replace(minute=0, second=0, microsecond=0)


@pytest.fixture
def mock_ph_env(tmp_path, monkeypatch):
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
        "provisioning": {"access_level": "read_write"},
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


def test_contract_incremental_folding_and_consecutive_ticks(mock_ph_env):
    src, service_id, buffer_dir = mock_ph_env["source"], mock_ph_env["service_id"], mock_ph_env["buffer_dir"]
    _, active_hour, hour_start = _active_hour_bounds()

    f1 = os.path.join(buffer_dir, "batch_1.parquet")
    _write_active_hour_parquet(f1, rows=40, dt_start=hour_start + timedelta(minutes=1))
    stats1 = ph.merge_partial_hour(service_id, src, ["status", "country"])
    assert (stats1["new_files"], stats1["hour"], ph.read_partial_hour_total(src, active_hour)) == (1, active_hour, 40)
    assert os.path.isfile(ph._all_fields_path(src, active_hour))
    fields = {r[0] for r in ph.read_partial_hour_all_fields(src, active_hour)}
    assert "status" in fields and "country" in fields and ph.TOTAL_FIELD not in fields
    assert ph.read_partial_hour_watermark(src, active_hour) > 0.0

    stats2 = ph.merge_partial_hour(service_id, src, ["status", "country"])
    assert stats2["new_files"] == 0 and ph.read_partial_hour_total(src, active_hour) == 40

    time.sleep(0.05)
    f2 = os.path.join(buffer_dir, "batch_2.parquet")
    _write_active_hour_parquet(f2, rows=60, dt_start=hour_start + timedelta(minutes=2))
    os.utime(f2, (time.time() + 10, time.time() + 10))
    stats3 = ph.merge_partial_hour(service_id, src, ["status", "country"])
    assert stats3["new_files"] == 1 and ph.read_partial_hour_total(src, active_hour) == 100


def test_contract_admin_endpoints_and_query_runner_narrowing(mock_ph_env):
    src, service_id, buffer_dir = mock_ph_env["source"], mock_ph_env["service_id"], mock_ph_env["buffer_dir"]
    now, active_hour, hour_start = _active_hour_bounds()
    _write_active_hour_parquet(
        os.path.join(buffer_dir, "buf_admin.parquet"), rows=25, dt_start=hour_start + timedelta(minutes=2)
    )

    client = TestClient(app)
    data = client.post(f"/api/admin/partial-hour-merge/{service_id}").json()
    assert (data["service_id"], data["new_files"], data["status"]) == (service_id, 1, "success")

    status_data = client.get(f"/api/admin/partial-hour-status/{service_id}").json()
    assert (status_data["service_id"], status_data["file_exists"], status_data["total_rows"]) == (service_id, True, 25)
    assert status_data["watermark"] > 0

    with duckdb.connect() as con:
        runner = QueryRunner(con, src)
        adjusted_start, partial_rows, partial_total = runner._partial_hour_adjusted_live_start(
            hour_start, active_hour, now + timedelta(minutes=5)
        )
        assert partial_total == 25 and len(partial_rows) > 0 and adjusted_start > hour_start


def test_contract_fos_atomic_corrupt_and_high_scale(mock_ph_env, monkeypatch):
    src, service_id, buffer_dir = mock_ph_env["source"], mock_ph_env["service_id"], mock_ph_env["buffer_dir"]
    _, active_hour, hour_start = _active_hour_bounds()
    f1 = os.path.join(buffer_dir, "b1.parquet")
    _write_active_hour_parquet(f1, rows=20, dt_start=hour_start + timedelta(minutes=1))

    # Zero FOS calls
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value="run_ph_fos"))
    with patch("urllib.request.urlopen", side_effect=AssertionError("Outbound network egress")):
        assert ph.merge_partial_hour(service_id, src, ["status"])["new_files"] == 1
        compaction._run_partial_hour_merge.__wrapped__(service_id)

    # Atomic crash safety
    ph_dir = ph.partial_hour_dir(src, active_hour)
    assert [f for f in os.listdir(ph_dir) if f.startswith(".tmp_")] == []
    f2 = os.path.join(buffer_dir, "b2.parquet")
    _write_active_hour_parquet(f2, rows=30, dt_start=hour_start + timedelta(minutes=2))
    os.utime(f2, (time.time() + 10, time.time() + 10))
    with patch("os.replace", side_effect=OSError("disk error")), pytest.raises(OSError):
        ph.merge_partial_hour(service_id, src, ["status"])
    assert [f for f in os.listdir(ph_dir) if f.startswith(".tmp_")] == []
    assert ph.read_partial_hour_total(src, active_hour) == 20

    # Corrupt file self-healing
    with open(ph._all_fields_path(src, active_hour), "wb") as f:
        f.write(b"NOT_A_VALID_PARQUET_FILE_GARBAGE")
    stats = ph.merge_partial_hour(service_id, src, ["status"])
    assert stats["new_files"] == 2 and ph.read_partial_hour_total(src, active_hour) == 50

    # High-scale mode noop
    src["deployment_mode"] = "high_scale"
    hs_stats = ph.merge_partial_hour(service_id, src, ["status"])
    assert hs_stats["new_files"] == 0 and hs_stats["duration_ms"] < 20.0


def test_contract_concurrent_reader_resilience_and_scheduler(mock_ph_env, monkeypatch):
    src, service_id, buffer_dir = mock_ph_env["source"], mock_ph_env["service_id"], mock_ph_env["buffer_dir"]
    _, active_hour, hour_start = _active_hour_bounds()
    for i in range(5):
        _write_active_hour_parquet(
            os.path.join(buffer_dir, f"conc_{i}.parquet"), rows=15, dt_start=hour_start + timedelta(minutes=i)
        )

    stop_event = threading.Event()
    reader_errors: list[Exception] = []
    read_counts: list[int] = []

    def reader_loop():
        with duckdb.connect() as con:
            while not stop_event.is_set():
                try:
                    read_counts.append(ph.read_partial_hour_total(src, active_hour, con=con))
                    ph.read_partial_hour_all_fields(src, active_hour, con=con)
                except Exception as e:
                    reader_errors.append(e)
                time.sleep(0.005)

    thread = threading.Thread(target=reader_loop, daemon=True)
    thread.start()
    try:
        for _ in range(3):
            ph.merge_partial_hour(service_id, src, ["status", "country"])
            time.sleep(0.01)
    finally:
        stop_event.set()
        thread.join(timeout=3.0)
    assert not reader_errors and len(read_counts) > 0

    for env_val, expected in [("45", 45), ("5", 10), ("200", 120)]:
        monkeypatch.setenv("PARTIAL_HOUR_MERGE_INTERVAL_SEC", env_val)
        assert partial_hour_merge_interval_sec() == expected

    monkeypatch.setenv("PARTIAL_HOUR_MERGE_INTERVAL_SEC", "40")
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    cfg = {"service_id": "svc-ph-sched", "provisioning": {"access_level": "read_write"}}
    sched = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "svc-ph-sched"}),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch.object(sched._sched, "add_job") as add_job,
    ):
        sched._register_dev_local_safe_jobs()
    ph_call = next(c for c in add_job.call_args_list if c.kwargs.get("id") == "partial_hour_merge_svc-ph-sched")
    assert ph_call.kwargs.get("seconds") == 40
