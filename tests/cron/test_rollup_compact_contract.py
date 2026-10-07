"""Contract test suite for Cron 6: rollup_compact_{service_id}.

Verifies all requirements and checklist items from docs/cron/jobs/rollup-compact.md (§1–§9):
1. Generate 24 hourly synthetic rollup bundles for a past UTC day.
2. Trigger POST /api/admin/rollups/compact/{service_id}; confirm HTTP 200 and success.
3. Verify day_bundle_YYYY-MM-DD.parquet is created (flat and partition alias).
4. Confirm the 24 individual hourly bundle files are retired/unlinked.
5. Run query over the date range; confirm query reads the day bundle directly.
6. Confirm query response time is < 150ms.
7. Active-request politeness gate (should_defer_cron) and manual bypass.
8. ENOSPC disk space safety pre-check (< 50MB free).
9. Subsystem error handling, warning status, and error_message attribution.
10. Zero FOS egress (local disk only).
11. Admin status inspection endpoint (GET /api/admin/rollups/status).
"""

from __future__ import annotations

import collections
import os
import shutil
import time
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from backend.cron.jobs.rollup_compact import _run_rollup_compact_daily
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
    assert body["retired_files"] == 48

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

    # 4. Confirm all individual hourly bundle files are retired/unlinked
    for p in constituent_files:
        assert not os.path.exists(p), f"Hourly bundle should be retired: {p}"
    for h in range(24):
        hour_dir = os.path.join(hour_bundled_root, f"hour={target_day}-{h:02d}")
        assert not os.path.exists(hour_dir), f"Hour dir should be removed: {hour_dir}"

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
