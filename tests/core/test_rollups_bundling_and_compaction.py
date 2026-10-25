"""Tests for rollup day-compaction, day/hour bundling, and partial-hour rollups."""

from __future__ import annotations

import glob
import logging
import os
import shutil
import time
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from backend.utils.hll import HyperLogLog
from tests._helpers.rollups import noop_lock as _noop_lock
from tests._helpers.sources import make_source

from backend.core import rollups
from backend.core.metadata.cron_log import CronLeaseHeldError
from backend.core.rollups import day_bundles
from backend.core.rollups import partial_hour as ph
from backend.core.rollups._common import DAY_BUNDLE_TOP_K
from backend.core.rollups.day_bundles import _link_day_bundle_alias
from backend.core.rollups.hour_bundles import bundle_hours_ip_spread
from backend.core.rollups.recompute import _bundle_metadata
from backend.cron.jobs import compaction as sch
from backend.repositories._base import QueryRunner


class _FrozenNow(datetime):
    @classmethod
    def now(cls, tz=None):
        return datetime(2026, 6, 5, 0, 0, 0, tzinfo=tz or UTC)


def _query_mem(sql: str) -> list:
    con = duckdb.connect(":memory:")
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def _write_hour_rollup(buf: str, field: str, hour: str, rows: list[dict]) -> str:
    d = os.path.join(buf, "rollups", "hour", f"field={field}", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "field": pa.array([r["field"] for r in rows]),
            "value": pa.array([r["value"] for r in rows]),
            "count": pa.array([r["count"] for r in rows], type=pa.int64()),
        }
    )
    p = os.path.join(d, f"compacted_{uuid.uuid4().hex[:12]}.parquet")
    pq.write_table(table, p)
    return p


def _write_hour_bundle(buf: str, hour: str, rows: list[dict]) -> str:
    d = os.path.join(buf, "rollups", "hour_bundled", f"hour={hour}")
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
    return p


def _setup_backfill_env(monkeypatch, view_sqls: str | list[str], recompute_fn=None):
    sql_list = [view_sqls] if isinstance(view_sqls, str) else view_sqls

    def _fake_update_iceberg_view(con, _src):
        for s in sql_list:
            con.execute(s)

    monkeypatch.setattr("backend.core.iceberg.update_iceberg_view", _fake_update_iceberg_view)
    monkeypatch.setattr("backend.core.rollups.recompute.datetime", _FrozenNow)
    captured_hours: list[set[str]] = []
    monkeypatch.setattr(
        "backend.core.rollups.recompute.recompute_touched_hours",
        recompute_fn or (lambda _sid, _src, hours: captured_hours.append(set(hours))),
    )
    return captured_hours


# ── Merged from tests/core/test_rollups_compaction.py ──


def test_compact_writes_per_day_file_summing_hour_counts(tmp_path):
    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    src = {"name": "svc-compact-1"}
    today = datetime.now(UTC).strftime("%Y-%m-%d")

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_hour_rollup(
            str(cache_root), "ua", "2026-06-04-10", [{"field": "ua", "value": "Mozilla/5.0", "count": 100}]
        )
        _write_hour_rollup(
            str(cache_root), "ua", "2026-06-04-11", [{"field": "ua", "value": "Mozilla/5.0", "count": 250}]
        )
        _write_hour_rollup(
            str(cache_root), "ua", f"{today}-12", [{"field": "ua", "value": "Mozilla/5.0", "count": 999}]
        )
        rebuilt = rollups.compact_closed_days_to_daily("svc-compact-1", src)

    assert rebuilt == 1
    day_file = cache_root / "rollups" / "day" / "field=ua" / "day=2026-06-04" / "compacted.parquet"
    assert day_file.exists()
    rows = _query_mem(f"SELECT field, value, count FROM read_parquet('{day_file}') ORDER BY field, value")
    assert rows == [("ua", "Mozilla/5.0", 350)]
    assert not (cache_root / "rollups" / "day" / "field=ua" / f"day={today}" / "compacted.parquet").exists()


def test_compact_uses_in_memory_duckdb_not_per_service_file(tmp_path):
    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    _write_hour_rollup(str(cache_root), "ua", "2026-06-04-10", [{"field": "ua", "value": "x", "count": 1}])

    connect_calls: list = []
    real_connect = duckdb.connect
    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch.object(
            duckdb, "connect", side_effect=lambda *a, **kw: (connect_calls.append((a, kw)), real_connect(*a, **kw))[1]
        ),
    ):
        rollups.compact_closed_days_to_daily("svc-compact-mem", {"name": "svc-compact-mem"})

    assert any(c[0] and c[0][0] == ":memory:" for c in connect_calls)
    assert not any(c[0] and isinstance(c[0][0], str) and c[0][0].endswith(".duckdb") for c in connect_calls)


def test_compact_skips_when_per_day_file_is_already_up_to_date(tmp_path):
    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    src = {"name": "svc-compact-idem"}

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_hour_rollup(str(cache_root), "ua", "2026-06-04-10", [{"field": "ua", "value": "y", "count": 5}])
        assert rollups.compact_closed_days_to_daily("svc-compact-idem", src) == 1
        day_file = cache_root / "rollups" / "day" / "field=ua" / "day=2026-06-04" / "compacted.parquet"
        os.utime(str(day_file), (time.time() + 60, time.time() + 60))
        assert rollups.compact_closed_days_to_daily("svc-compact-idem", src) == 0


def test_compact_falls_back_to_hour_bundled_when_per_field_hours_cleaned_up(tmp_path):
    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    _write_hour_bundle(
        str(cache_root),
        "2026-06-04-10",
        [
            {"field": "method", "value": "POST", "count": 1000},
            {"field": "method", "value": "GET", "count": 2000},
            {"field": "country", "value": "US", "count": 3000},
        ],
    )
    _write_hour_bundle(
        str(cache_root),
        "2026-06-04-11",
        [{"field": "method", "value": "POST", "count": 1500}, {"field": "method", "value": "GET", "count": 2500}],
    )

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        rebuilt = rollups.compact_closed_days_to_daily("svc-compact-fallback", {"name": "svc-compact-fallback"})

    assert rebuilt >= 2
    method_day = cache_root / "rollups" / "day" / "field=method" / "day=2026-06-04" / "compacted.parquet"
    country_day = cache_root / "rollups" / "day" / "field=country" / "day=2026-06-04" / "compacted.parquet"
    assert method_day.exists() and country_day.exists()
    assert sorted(_query_mem(f"SELECT value, count FROM read_parquet('{method_day}')")) == [
        ("GET", 4500),
        ("POST", 2500),
    ]
    assert _query_mem(f"SELECT value, count FROM read_parquet('{country_day}')") == [("US", 3000)]


def test_compact_unions_per_field_hour_and_bundled_hour_sources(tmp_path):
    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    _write_hour_rollup(str(cache_root), "method", "2026-06-04-12", [{"field": "method", "value": "POST", "count": 500}])
    _write_hour_bundle(str(cache_root), "2026-06-04-10", [{"field": "method", "value": "POST", "count": 200}])
    _write_hour_bundle(str(cache_root), "2026-06-04-11", [{"field": "method", "value": "POST", "count": 300}])

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        rollups.compact_closed_days_to_daily("svc-compact-mixed", {"name": "svc-compact-mixed"})

    method_day = cache_root / "rollups" / "day" / "field=method" / "day=2026-06-04" / "compacted.parquet"
    assert method_day.exists()
    assert _query_mem(f"SELECT value, count FROM read_parquet('{method_day}')") == [("POST", 1000)]


def test_backfill_missing_hour_bundles_detects_gaps_via_view(tmp_path, monkeypatch):
    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    src = {"name": "svc-heal", "service_id": "svc-heal"}
    _write_hour_bundle(str(cache_root), "2026-06-04-10", [{"field": "method", "value": "GET", "count": 100}])
    captured_hours = _setup_backfill_env(
        monkeypatch,
        "CREATE OR REPLACE VIEW logs_svc_heal AS SELECT * FROM (VALUES "
        "(TIMESTAMP '2026-06-04 09:30:00+00'), (TIMESTAMP '2026-06-04 09:45:00+00'), "
        "(TIMESTAMP '2026-06-04 10:15:00+00')) AS t(timestamp)",
    )
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        result = rollups.backfill_missing_hour_bundles("svc-heal", src, lookback_days=2)

    assert captured_hours == [{"2026-06-04-09"}] and result["missing"] == 1


def test_backfill_missing_hour_bundles_heals_partial_bundle_missing_core_fields(tmp_path, monkeypatch):
    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    src = {"name": "svc-partial-heal", "service_id": "svc-partial-heal"}
    _write_hour_bundle(
        str(cache_root),
        "2026-06-04-10",
        [
            {"field": "ip", "value": "107.170.164.220", "count": 100},
            {"field": "status", "value": "200", "count": 100},
            {"field": "method", "value": "GET", "count": 100},
            {"field": "pop", "value": "IAD", "count": 100},
        ],
    )
    _write_hour_bundle(str(cache_root), "2026-06-04-09", [{"field": "url", "value": "/only-url", "count": 120}])
    captured_hours = _setup_backfill_env(
        monkeypatch,
        "CREATE OR REPLACE VIEW logs_svc_partial_heal AS SELECT * FROM (VALUES "
        "(TIMESTAMP '2026-06-04 09:30:00+00', '107.170.164.220', 200, 'GET', 'IAD'), "
        "(TIMESTAMP '2026-06-04 10:15:00+00', '107.170.164.220', 200, 'GET', 'IAD')"
        ") AS t(timestamp, ip, status, method, pop)",
    )
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        result = rollups.backfill_missing_hour_bundles("svc-partial-heal", src, lookback_days=2)

    assert captured_hours == [{"2026-06-04-09"}] and result["missing"] == 1
    assert (
        os.path.getmtime(cache_root / "rollups" / "hour_bundled" / "hour=2026-06-04-09" / "all_fields.parquet") == 0.0
    )


def test_backfill_ignores_rum_child_views_when_finding_request_view(tmp_path, monkeypatch):
    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    src = {"name": "svc-rum", "service_id": "svc-rum"}
    captured_hours = _setup_backfill_env(
        monkeypatch,
        [
            "CREATE OR REPLACE VIEW logs_svc_rum__client_errors AS SELECT * FROM (VALUES (TIMESTAMP '2026-06-04 09:30:00+00')) AS t(timestamp)",
            "CREATE OR REPLACE VIEW logs_svc_rum AS SELECT * FROM (VALUES (TIMESTAMP '2026-06-04 09:45:00+00')) AS t(timestamp)",
        ],
    )
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        result = rollups.backfill_missing_hour_bundles("svc-rum", src, lookback_days=2)

    assert captured_hours == [{"2026-06-04-09"}] and result["missing"] == 1


def test_backfill_missing_hour_bundles_noop_when_complete(tmp_path, monkeypatch):
    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    src = {"name": "svc-noop", "service_id": "svc-noop"}
    _write_hour_bundle(str(cache_root), "2026-06-04-09", [{"field": "method", "value": "GET", "count": 50}])
    dispatched = _setup_backfill_env(
        monkeypatch,
        "CREATE OR REPLACE VIEW logs_svc_noop AS SELECT * FROM (VALUES (TIMESTAMP '2026-06-04 09:30:00+00')) AS t(timestamp)",
    )
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        result = rollups.backfill_missing_hour_bundles("svc-noop", src, lookback_days=2)
        result2 = rollups.backfill_missing_hour_bundles("svc-noop", src, lookback_days=2)

    assert dispatched == []
    assert (
        result["missing"] == 0
        and result["rebuilt_fields"] == 0
        and result["bundled"] == 0
        and result["stamped_empty"] == 47
    )
    assert result2 == {"missing": 0, "rebuilt_fields": 0, "bundled": 0, "stamped_empty": 0, "coverage_verified": True}


def test_backfill_rebuilds_empty_sentinel_when_late_rows_arrive(tmp_path, monkeypatch):
    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    src = {"name": "svc-late", "service_id": "svc-late"}
    _write_hour_bundle(str(cache_root), "2026-06-04-09", [])
    dispatched: list[set[str]] = []

    def _fake_recompute(_sid, _src, hours):
        dispatched.append(set(hours))
        field_dir = cache_root / "rollups" / "hour" / "field=method" / "hour=2026-06-04-09"
        field_dir.mkdir(parents=True, exist_ok=True)
        source_path = field_dir / "late.parquet"
        pq.write_table(
            pa.table({"field": ["method"], "value": ["GET"], "count": pa.array([7], type=pa.int64())}), source_path
        )
        os.utime(source_path, (source_path.stat().st_mtime + 2, source_path.stat().st_mtime + 2))
        assert rollups.bundle_hours("svc-late", src, list(hours)) == 1

    _setup_backfill_env(
        monkeypatch,
        "CREATE OR REPLACE VIEW logs_svc_late AS SELECT * FROM (VALUES (TIMESTAMP '2026-06-04 09:30:00+00')) AS t(timestamp)",
        recompute_fn=_fake_recompute,
    )
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        result = rollups.backfill_missing_hour_bundles("svc-late", src, lookback_days=2)

    assert dispatched == [{"2026-06-04-09"}] and result["missing"] == 1
    assert (
        pq.read_metadata(cache_root / "rollups" / "hour_bundled" / "hour=2026-06-04-09" / "all_fields.parquet").num_rows
        == 1
    )


def test_corrupt_hour_bundle_is_not_treated_as_coverage(tmp_path):
    corrupt = tmp_path / "all_fields.parquet"
    corrupt.write_bytes(b"not parquet")
    assert _bundle_metadata(str(corrupt)) is None


def test_compact_returns_zero_when_rollups_dir_missing(tmp_path):
    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        assert rollups.compact_closed_days_to_daily("svc-compact-empty", {"name": "svc-compact-empty"}) == 0


def _mock_rollups(monkeypatch):
    for fn in (
        "backfill_day_bundles",
        "compact_network_rtt_closed_days_to_daily",
        "compact_network_speed_closed_days_to_daily",
        "compact_ngwaf_bots_closed_days_to_daily",
        "compact_origin_dims_closed_days_to_daily",
        "compact_origin_latency_ts_closed_days_to_daily",
        "compact_origin_summary_closed_days_to_daily",
        "compact_overview_closed_days_to_daily",
        "compact_perf_latency_closed_days_to_daily",
        "compact_security_dims_closed_days_to_daily",
        "compact_security_proxies_closed_days_to_daily",
        "compact_verified_bots_ts_closed_days_to_daily",
    ):
        monkeypatch.setattr(f"backend.core.rollups.{fn}", lambda *a, **kw: 0)
    monkeypatch.setattr(
        "backend.core.rollups.backfill_missing_hour_bundles", lambda *a, **kw: {"missing": 0, "bundled": 0}
    )
    monkeypatch.setattr(
        "backend.core.rollups.backfill_missing_hour_ip_spread", lambda *a, **kw: {"missing": 0, "rebuilt": 0}
    )


def test_run_rollup_compact_daily_passes_run_id_on_success(monkeypatch):
    _mock_rollups(monkeypatch)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: {"name": sid})
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 4242)
    monkeypatch.setattr("backend.core.rollups.compact_closed_days_to_daily", lambda sid, src, lookback_days=30: 7)
    log_calls: list = []
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run", lambda *a, **kw: log_calls.append({"args": a, "kwargs": kw})
    )

    sch._run_rollup_compact_daily.__wrapped__("svc-run-ok")
    assert len(log_calls) == 1
    assert log_calls[0]["kwargs"].get("run_id") == 4242 and log_calls[0]["args"][3] == "success"
    assert "Rebuilt 7" in (log_calls[0]["kwargs"].get("summary") or "")


def test_run_rollup_compact_daily_passes_run_id_on_error(monkeypatch):
    _mock_rollups(monkeypatch)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: {"name": sid})
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 9999)
    monkeypatch.setattr(
        "backend.core.rollups.compact_closed_days_to_daily",
        lambda sid, src, lookback_days=30: (_ for _ in ()).throw(RuntimeError("simulated DB lock")),
    )
    log_calls: list = []
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run", lambda *a, **kw: log_calls.append({"args": a, "kwargs": kw})
    )

    sch._run_rollup_compact_daily.__wrapped__("svc-run-err")
    assert len(log_calls) == 1
    assert log_calls[0]["kwargs"].get("run_id") == 9999 and log_calls[0]["args"][3] == "error"
    assert "simulated DB lock" in (log_calls[0]["kwargs"].get("error_message") or "")


def test_compacted_day_file_has_bigint_count_column(tmp_path):
    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_hour_rollup(str(cache_root), "ua", "2026-06-04-10", [{"field": "ua", "value": "Mozilla", "count": 1}])
        rollups.compact_closed_days_to_daily("svc-compact-bigint", {"name": "svc-compact-bigint"})

    day_file = cache_root / "rollups" / "day" / "field=ua" / "day=2026-06-04" / "compacted.parquet"
    schema = _query_mem(f"DESCRIBE SELECT * FROM read_parquet('{day_file}')")
    assert next((row[1] for row in schema if row[0] == "count"), None) == "BIGINT"


def test_mixed_day_and_hour_read_via_union_all_does_not_hit_hive_partition_mismatch(tmp_path):
    cache_root = tmp_path / "cache-root"
    cache_root.mkdir()
    src = {"name": "svc-mixed-read"}
    today = datetime.now(UTC).strftime("%Y-%m-%d")

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_hour_rollup(str(cache_root), "ua", "2026-06-04-10", [{"field": "ua", "value": "Mozilla", "count": 100}])
        rollups.compact_closed_days_to_daily("svc-mixed-read", src)
        _write_hour_rollup(str(cache_root), "ua", f"{today}-12", [{"field": "ua", "value": "Mozilla", "count": 50}])

    day_files = glob.glob(str(cache_root / "rollups" / "day" / "field=ua" / "day=*" / "*.parquet"))
    hour_files = glob.glob(str(cache_root / "rollups" / "hour" / "field=ua" / f"hour={today}-*" / "*.parquet"))
    assert day_files and hour_files

    paths_sql = ", ".join(f"'{p}'" for p in day_files + hour_files)
    with pytest.raises(duckdb.BinderException, match=r"Hive partition mismatch"):
        _query_mem(
            f"SELECT field, value, SUM(count) AS c FROM read_parquet([{paths_sql}], hive_partitioning=1) GROUP BY field, value"
        )

    day_sql = ", ".join(f"'{p}'" for p in day_files)
    hour_sql = ", ".join(f"'{p}'" for p in hour_files)
    rows = _query_mem(
        f"SELECT field, value, SUM(count) AS c FROM ("
        f"  SELECT field, value, CAST(count AS BIGINT) AS count FROM read_parquet([{day_sql}], hive_partitioning=1)"
        f"  UNION ALL "
        f"  SELECT field, value, CAST(count AS BIGINT) AS count FROM read_parquet([{hour_sql}], hive_partitioning=1)"
        f") GROUP BY field, value"
    )
    assert rows == [("ua", "Mozilla", 150)]


def test_run_rollup_compact_daily_returns_silently_when_start_cron_run_skips(monkeypatch):
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: {"name": sid})
    monkeypatch.setattr(
        "backend.core.duckdb.start_cron_run", lambda src, task: (_ for _ in ()).throw(CronLeaseHeldError("busy"))
    )
    monkeypatch.setattr(
        "backend.core.rollups.compact_closed_days_to_daily", lambda sid, src: pytest.fail("must not run")
    )
    log_calls: list = []
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **kw: log_calls.append((a, kw)))

    sch._run_rollup_compact_daily("svc-run-busy")
    assert log_calls == []


# ── Merged from tests/core/test_rollups_day_bundles.py ──


def _write_per_field_day(cache_root: str, field: str, day: str, rows: list[tuple[str, int]]) -> str:
    d = os.path.join(cache_root, "rollups", "day", f"field={field}", f"day={day}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "field": pa.array([field] * len(rows)),
            "value": pa.array([r[0] for r in rows]),
            "count": pa.array([r[1] for r in rows], type=pa.int64()),
        }
    )
    p = os.path.join(d, f"compacted_{uuid.uuid4().hex[:8]}.parquet")
    pq.write_table(table, p)
    return p


def _past_day(days_ago: int) -> str:
    return (datetime.now(UTC) - timedelta(days=days_ago)).strftime("%Y-%m-%d")


def test_bundle_days_writes_combined_parquet(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    day = _past_day(2)
    _write_per_field_day(str(cache_root), "url", day, [("/a", 100), ("/b", 50)])
    _write_per_field_day(str(cache_root), "country", day, [("US", 80), ("JP", 20)])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
    ):
        assert day_bundles.bundle_days("svc-day", {"name": "svc-day"}, [day]) == 1

    bundle = cache_root / "rollups" / "day_bundled" / f"day={day}" / "all_fields.parquet"
    assert bundle.exists()
    t = pq.read_table(str(bundle))
    rows = set(zip(t["field"].to_pylist(), t["value"].to_pylist(), t["count"].to_pylist()))
    assert {("url", "/a", 100), ("url", "/b", 50), ("country", "US", 80), ("country", "JP", 20)} <= rows


def test_bundle_days_top_k_truncation_with_other_synthetic_row(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    day = _past_day(2)
    _write_per_field_day(str(cache_root), "url", day, [(f"v{i}", i + 1) for i in range(DAY_BUNDLE_TOP_K + 50)])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
    ):
        assert day_bundles.bundle_days("svc-topk", {"name": "svc-topk"}, [day]) == 1

    records = pq.read_table(
        str(cache_root / "rollups" / "day_bundled" / f"day={day}" / "all_fields.parquet")
    ).to_pylist()
    other = [r for r in records if r["value"] == "__other__"]
    assert len(other) == 1 and other[0]["count"] == sum(range(1, 51))
    assert len([r for r in records if r["value"] != "__other__"]) == DAY_BUNDLE_TOP_K


def test_bundle_days_preserves_unmodified_fields_from_existing_bundle(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-day-preserve"}
    day = _past_day(2)
    _write_per_field_day(str(cache_root), "ip", day, [("107.170.164.220", 500)])
    _write_per_field_day(str(cache_root), "url", day, [("/old", 100)])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
    ):
        assert day_bundles.bundle_days("svc-day-preserve", src, [day]) == 1
        new_p = _write_per_field_day(str(cache_root), "url", day, [("/new", 250)])
        os.utime(new_p, (time.time() + 60, time.time() + 60))
        assert day_bundles.bundle_days("svc-day-preserve", src, [day]) == 1

    t = pq.read_table(str(cache_root / "rollups" / "day_bundled" / f"day={day}" / "all_fields.parquet"))
    rows = set(zip(t["field"].to_pylist(), t["value"].to_pylist(), t["count"].to_pylist()))
    assert ("ip", "107.170.164.220", 500) in rows and ("url", "/new", 250) in rows and ("url", "/old", 100) not in rows


def test_bundle_days_skips_active_day(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    active = datetime.now(UTC).strftime("%Y-%m-%d")
    _write_per_field_day(str(cache_root), "url", active, [("/x", 1)])
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        assert day_bundles.bundle_days("svc", {"name": "svc"}, [active]) == 0
    assert not (cache_root / "rollups" / "day_bundled" / f"day={active}" / "all_fields.parquet").exists()


def test_bundle_days_skips_when_bundle_up_to_date(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    day = _past_day(2)
    _write_per_field_day(str(cache_root), "url", day, [("/a", 1)])
    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
    ):
        assert day_bundles.bundle_days("svc", {"name": "svc"}, [day]) == 1
        assert day_bundles.bundle_days("svc", {"name": "svc"}, [day]) == 0


def test_bundle_days_rebuilds_when_source_newer_than_bundle(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    day = _past_day(2)
    _write_per_field_day(str(cache_root), "url", day, [("/a", 1)])
    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
    ):
        assert day_bundles.bundle_days("svc", {"name": "svc"}, [day]) == 1
        new_p = _write_per_field_day(str(cache_root), "url", day, [("/b", 5)])
        os.utime(new_p, (time.time() + 60, time.time() + 60))
        assert day_bundles.bundle_days("svc", {"name": "svc"}, [day]) == 1


def test_bundle_days_no_per_field_files_skipped(tmp_path):
    cache_root = tmp_path / "cache"
    (cache_root / "rollups" / "day").mkdir(parents=True)
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        assert day_bundles.bundle_days("svc", {"name": "svc"}, [_past_day(2)]) == 0


def test_bundle_days_no_day_root_returns_zero(tmp_path):
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path / "cache_missing")):
        assert day_bundles.bundle_days("svc", {"name": "svc"}, [_past_day(2)]) == 0


def test_bundle_days_empty_input_returns_zero():
    assert day_bundles.bundle_days("svc", {"name": "svc"}, []) == 0


def test_bundle_days_malformed_day_token_skipped(tmp_path):
    cache_root = tmp_path / "cache"
    (cache_root / "rollups" / "day").mkdir(parents=True)
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        assert day_bundles.bundle_days("svc", {"name": "svc"}, ["not-a-day"]) == 0


def test_backfill_day_bundles_discovers_and_caps(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    days = [_past_day(i) for i in range(2, 6)]
    for d in days:
        _write_per_field_day(str(cache_root), "url", d, [("/x", 1)])
    seeded_dir = cache_root / "rollups" / "day_bundled" / f"day={days[0]}"
    seeded_dir.mkdir(parents=True)
    (seeded_dir / "all_fields.parquet").write_bytes(b"present")

    seen: list[list[str]] = []
    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch(
            "backend.core.rollups.day_bundles.bundle_days",
            lambda _sid, _src, days_in: (seen.append(list(days_in)), len(days_in))[1],
        ),
    ):
        rebuilt = day_bundles.backfill_day_bundles("svc-day-bf", {"name": "svc-day-bf"}, max_days=2)

    assert rebuilt == 2 and len(seen[0]) == 2 and days[0] not in seen[0]


def test_backfill_day_bundles_no_root_returns_zero(tmp_path):
    with patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path / "nope")):
        assert day_bundles.backfill_day_bundles("svc", {"name": "svc"}) == 0


def test_backfill_day_bundles_all_present_returns_zero(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    for i in range(2, 4):
        d = _past_day(i)
        _write_per_field_day(str(cache_root), "url", d, [("/x", 1)])
        bd = cache_root / "rollups" / "day_bundled" / f"day={d}"
        bd.mkdir(parents=True)
        (bd / "all_fields.parquet").write_bytes(b"x")

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups.day_bundles.bundle_days", side_effect=AssertionError("must not be invoked")),
    ):
        assert day_bundles.backfill_day_bundles("svc", {"name": "svc"}) == 0


def test_link_day_bundle_alias_hardlinks_and_leaves_no_tmp(tmp_path) -> None:
    d = tmp_path / "alias"
    d.mkdir()
    src, dst = d / "src.parquet", d / "dst.parquet"
    src.write_bytes(b"new")
    dst.write_bytes(b"old")
    _link_day_bundle_alias(str(src), str(dst))
    assert os.path.samefile(src, dst) and dst.read_bytes() == b"new"
    assert sorted(p.name for p in d.iterdir()) == ["dst.parquet", "src.parquet"]


def test_link_day_bundle_alias_never_removes_existing_dst_on_failure(tmp_path) -> None:
    d = tmp_path / "alias"
    d.mkdir()
    src, dst = d / "src.parquet", d / "dst.parquet"
    src.write_bytes(b"new")
    dst.write_bytes(b"old")
    with (
        patch("backend.core.rollups.day_bundles.os.link", side_effect=OSError("no link")),
        patch("backend.core.rollups.day_bundles.shutil.copyfile", side_effect=OSError("no copy")),
    ):
        _link_day_bundle_alias(str(src), str(dst))
    assert dst.read_bytes() == b"old" and sorted(p.name for p in d.iterdir()) == ["dst.parquet", "src.parquet"]


def test_link_day_bundle_alias_copy_fallback_is_atomic_replace(tmp_path) -> None:
    d = tmp_path / "alias"
    d.mkdir()
    src, dst = d / "src.parquet", d / "dst.parquet"
    src.write_bytes(b"new")
    dst.write_bytes(b"old")
    seen_during_copy: list[bool] = []
    real_copy = shutil.copyfile
    with (
        patch("backend.core.rollups.day_bundles.os.link", side_effect=OSError("no link")),
        patch(
            "backend.core.rollups.day_bundles.shutil.copyfile",
            side_effect=lambda s, dt: (seen_during_copy.append(dst.exists()), real_copy(s, dt))[1],
        ),
    ):
        _link_day_bundle_alias(str(src), str(dst))
    assert seen_during_copy == [True] and dst.read_bytes() == b"new"
    assert sorted(p.name for p in d.iterdir()) == ["dst.parquet", "src.parquet"]


# ── Merged from tests/core/test_rollups_hour_bundling.py ──


def _write_per_field_hour(cache_root: str, field: str, hour: str, rows: list[dict]) -> str:
    d = os.path.join(cache_root, "rollups", "hour", f"field={field}", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "value": pa.array([r["value"] for r in rows]),
            "count": pa.array([r["count"] for r in rows], type=pa.int64()),
        }
    )
    p = os.path.join(d, f"compacted_{uuid.uuid4().hex[:12]}.parquet")
    pq.write_table(table, p)
    return p


def _write_per_field_hour_ip_spread(cache_root: str, field: str, hour: str, rows: list[dict]) -> str:
    d = os.path.join(cache_root, "rollups", "hour_ip_spread", f"field={field}", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "value": pa.array([r["value"] for r in rows]),
            "ip_sketch": pa.array([r["ip_sketch"] for r in rows], type=pa.binary()),
            "ip_count_observed": pa.array([r["ip_count_observed"] for r in rows], type=pa.int64()),
            "sample_capped": pa.array([r["sample_capped"] for r in rows], type=pa.bool_()),
        }
    )
    p = os.path.join(d, f"compacted_{uuid.uuid4().hex[:12]}.parquet")
    pq.write_table(table, p)
    return p


def test_bundle_hours_writes_one_parquet_per_hour(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour(
            str(cache_root), "url", "2026-05-15-10", [{"value": "/login", "count": 100}, {"value": "/api", "count": 75}]
        )
        _write_per_field_hour(
            str(cache_root), "country", "2026-05-15-10", [{"value": "US", "count": 80}, {"value": "JP", "count": 20}]
        )
        assert rollups.bundle_hours("svc-bundle-1", {"name": "svc-bundle-1"}, ["2026-05-15-10"]) == 1

    bundle = cache_root / "rollups" / "hour_bundled" / "hour=2026-05-15-10" / "all_fields.parquet"
    assert bundle.exists()
    t = pq.read_table(str(bundle))
    assert {"field", "value", "count"}.issubset(set(t.column_names))
    rows = set(zip(t["field"].to_pylist(), t["value"].to_pylist(), t["count"].to_pylist()))
    assert {("url", "/login", 100), ("url", "/api", 75), ("country", "US", 80), ("country", "JP", 20)} <= rows


def test_bundle_hours_skips_active_hour(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    active = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour(str(cache_root), "url", active, [{"value": "/x", "count": 1}])
        assert rollups.bundle_hours("svc-bundle-active", {"name": "svc-bundle-active"}, [active]) == 0
    assert not (cache_root / "rollups" / "hour_bundled" / f"hour={active}" / "all_fields.parquet").exists()


def test_bundle_hours_skips_when_bundle_is_up_to_date(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bundle-skip"}
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/x", "count": 1}])
        assert rollups.bundle_hours("svc-bundle-skip", src, ["2026-05-15-10"]) == 1
        bundle = cache_root / "rollups" / "hour_bundled" / "hour=2026-05-15-10" / "all_fields.parquet"
        mtime_first = os.path.getmtime(bundle)
        assert rollups.bundle_hours("svc-bundle-skip", src, ["2026-05-15-10"]) == 0
        assert os.path.getmtime(bundle) == mtime_first


def test_bundle_hours_rebuilds_when_source_files_newer(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bundle-stale"}
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/x", "count": 1}])
        rollups.bundle_hours("svc-bundle-stale", src, ["2026-05-15-10"])
        new_p = _write_per_field_hour(
            str(cache_root), "url", "2026-05-15-10", [{"value": "/x", "count": 1}, {"value": "/y", "count": 2}]
        )
        os.utime(new_p, (time.time() + 10, time.time() + 10))
        assert rollups.bundle_hours("svc-bundle-stale", src, ["2026-05-15-10"]) == 1

    t = pq.read_table(str(cache_root / "rollups" / "hour_bundled" / "hour=2026-05-15-10" / "all_fields.parquet"))
    assert {"/x", "/y"} <= set(t["value"].to_pylist())


def test_bundle_hours_preserves_unmodified_fields_from_existing_bundle(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bundle-preserve"}
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour(str(cache_root), "ip", "2026-05-15-10", [{"value": "107.170.164.220", "count": 120}])
        _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/a", "count": 10}])
        assert rollups.bundle_hours("svc-bundle-preserve", src, ["2026-05-15-10"]) == 1
        new_p = _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/b", "count": 25}])
        os.utime(new_p, (time.time() + 10, time.time() + 10))
        assert rollups.bundle_hours("svc-bundle-preserve", src, ["2026-05-15-10"]) == 1

    t = pq.read_table(str(cache_root / "rollups" / "hour_bundled" / "hour=2026-05-15-10" / "all_fields.parquet"))
    rows = set(zip(t["field"].to_pylist(), t["value"].to_pylist(), t["count"].to_pylist()))
    assert ("ip", "107.170.164.220", 120) in rows and ("url", "/b", 25) in rows and ("url", "/a", 10) not in rows


def _exec_top_n(cache_root, src, fields, st, et):
    con = duckdb.connect(":memory:")
    try:
        with (
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
            patch("backend.core.rollups._safe_table_for", return_value="dummy"),
            patch("backend.core.rollups._is_safe_ident", return_value=True),
        ):
            rows, _ = QueryRunner(con, src).execute_top_n_rollups(fields, st, et, limit=10)
            return rows
    finally:
        con.close()


def test_reader_uses_bundle_when_available_skipping_per_field_files(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-read-prefer-bundle", "bucket": "b", "prefix": "p"}
    _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/login", "count": 100}])
    _write_per_field_hour(str(cache_root), "country", "2026-05-15-10", [{"value": "US", "count": 50}])
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        rollups.bundle_hours("svc-read-prefer-bundle", src, ["2026-05-15-10"])
    rows = _exec_top_n(cache_root, src, ["url", "country"], "2026-05-15T10:00:00", "2026-05-15T11:00:00")
    by_field: dict[str, list[tuple]] = {}
    for f, v, c in rows:
        by_field.setdefault(f, []).append((v, c))
    assert by_field.get("url") == [("/login", 100)] and by_field.get("country") == [("US", 50)]


def test_reader_falls_back_to_per_field_when_bundle_missing(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-read-fallback", "bucket": "b", "prefix": "p"}
    _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/login", "count": 100}])
    assert _exec_top_n(cache_root, src, ["url"], "2026-05-15T10:00:00", "2026-05-15T11:00:00") == [
        ("url", "/login", 100)
    ]


def test_reader_mixed_bundled_and_per_field_hours(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-read-mixed", "bucket": "b", "prefix": "p"}
    _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/login", "count": 100}])
    _write_per_field_hour(str(cache_root), "url", "2026-05-15-11", [{"value": "/login", "count": 50}])
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        rollups.bundle_hours("svc-read-mixed", src, ["2026-05-15-10"])
    assert _exec_top_n(cache_root, src, ["url"], "2026-05-15T10:00:00", "2026-05-15T12:00:00") == [
        ("url", "/login", 150)
    ]


def test_bundle_hours_cleans_up_per_field_files_after_publish(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/login", "count": 100}])
        _write_per_field_hour(str(cache_root), "country", "2026-05-15-10", [{"value": "US", "count": 80}])
        assert rollups.bundle_hours("svc-cleanup", {"name": "svc-cleanup"}, ["2026-05-15-10"]) == 1

    assert (cache_root / "rollups" / "hour_bundled" / "hour=2026-05-15-10" / "all_fields.parquet").exists()
    for f in ("url", "country"):
        assert not (cache_root / "rollups" / "hour" / f"field={f}" / "hour=2026-05-15-10").exists()


def test_bundle_hours_cleanup_dry_run_logs_but_does_not_unlink(tmp_path, caplog):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch.dict(os.environ, {"ROLLUP_CLEANUP_DRY_RUN": "1"}),
        caplog.at_level(logging.INFO, logger="backend.core.rollups"),
    ):
        _write_per_field_hour(str(cache_root), "url", "2026-05-15-10", [{"value": "/x", "count": 1}])
        rollups.bundle_hours("svc-dry-run", {"name": "svc-dry-run"}, ["2026-05-15-10"])

    assert (cache_root / "rollups" / "hour" / "field=url" / "hour=2026-05-15-10").exists()
    assert any("ROLLUP_CLEANUP_DRY_RUN" in r.message for r in caplog.records)


def test_bundle_hours_ip_spread_writes_one_parquet_per_hour(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    hll_us, hll_jp, hll_ja3 = HyperLogLog(), HyperLogLog(), HyperLogLog()
    hll_us.update([f"1.1.1.{i}" for i in range(50)])
    hll_jp.update([f"2.2.2.{i}" for i in range(30)])
    hll_ja3.update([f"3.3.3.{i}" for i in range(10)])
    sketch_us, sketch_jp, sketch_ja3 = hll_us.to_bytes(), hll_jp.to_bytes(), hll_ja3.to_bytes()

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour_ip_spread(
            str(cache_root),
            "country",
            "2026-05-15-10",
            [
                {"value": "US", "ip_sketch": sketch_us, "ip_count_observed": 50, "sample_capped": False},
                {"value": "JP", "ip_sketch": sketch_jp, "ip_count_observed": 30, "sample_capped": False},
            ],
        )
        _write_per_field_hour_ip_spread(
            str(cache_root),
            "ja3",
            "2026-05-15-10",
            [{"value": "ja3-abc", "ip_sketch": sketch_ja3, "ip_count_observed": 10, "sample_capped": False}],
        )
        assert bundle_hours_ip_spread("svc-ipsp-bundle", {"name": "svc-ipsp-bundle"}, ["2026-05-15-10"]) == 1

    bundle = cache_root / "rollups" / "hour_bundled" / "hour=2026-05-15-10" / "all_fields_ip.parquet"
    assert bundle.exists()
    t = pq.read_table(str(bundle))
    assert {"field", "value", "ip_sketch", "ip_count_observed", "sample_capped"}.issubset(set(t.column_names))
    by_key = {(r["field"], r["value"]): r for r in t.to_pylist()}
    assert set(by_key) == {("country", "US"), ("country", "JP"), ("ja3", "ja3-abc")}
    assert by_key[("country", "US")]["ip_sketch"] == sketch_us and by_key[("country", "JP")]["ip_sketch"] == sketch_jp
    assert by_key[("ja3", "ja3-abc")]["ip_sketch"] == sketch_ja3
    assert by_key[("country", "US")]["ip_count_observed"] == 50 and by_key[("country", "US")]["sample_capped"] is False
    assert abs(HyperLogLog.from_bytes(by_key[("country", "US")]["ip_sketch"]).count() - 50) <= 10


def test_bundle_hours_ip_spread_skips_active_hour(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour_ip_spread(
            str(cache_root),
            "country",
            active_hour,
            [{"value": "US", "ip_sketch": HyperLogLog().to_bytes(), "ip_count_observed": 0, "sample_capped": False}],
        )
        assert bundle_hours_ip_spread("svc-ipsp-active", {"name": "svc-ipsp-active"}, [active_hour]) == 0
    assert not (cache_root / "rollups" / "hour_bundled" / f"hour={active_hour}" / "all_fields_ip.parquet").exists()


def test_bundle_hours_ip_spread_skips_when_bundle_is_up_to_date(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    hour = "2026-05-15-10"
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        _write_per_field_hour_ip_spread(
            str(cache_root),
            "country",
            hour,
            [{"value": "US", "ip_sketch": HyperLogLog().to_bytes(), "ip_count_observed": 0, "sample_capped": False}],
        )
        assert bundle_hours_ip_spread("svc-ipsp-fresh", {"name": "svc-ipsp-fresh"}, [hour]) == 1
        bundle = cache_root / "rollups" / "hour_bundled" / f"hour={hour}" / "all_fields_ip.parquet"
        time.sleep(0.05)
        os.utime(bundle, None)
        assert bundle_hours_ip_spread("svc-ipsp-fresh", {"name": "svc-ipsp-fresh"}, [hour]) == 0


def test_bundle_hours_ip_spread_returns_zero_when_no_per_field_files(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        assert bundle_hours_ip_spread("svc-ipsp-empty", {"name": "svc-ipsp-empty"}, ["2026-05-15-10"]) == 0
    assert not (cache_root / "rollups" / "hour_bundled" / "hour=2026-05-15-10").exists()


def test_bundle_hours_ip_spread_returns_zero_when_root_missing(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        assert bundle_hours_ip_spread("svc-ipsp-cold", {"name": "svc-ipsp-cold"}, ["2026-05-15-10"]) == 0


# ── Merged from tests/core/test_rollups_partial_hour.py ──


def _write_hourly_parquet(path, rows, schema="timestamp TIMESTAMP, country VARCHAR"):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con = duckdb.connect(":memory:")
    try:
        con.execute("SET TimeZone='UTC';")
        con.execute(f"CREATE TABLE t ({schema})")
        placeholders = ", ".join(["?"] * len(rows[0]))
        con.executemany(f"INSERT INTO t VALUES ({placeholders})", rows)
        con.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")
    finally:
        con.close()


class TestRollupsPartialHour:
    @pytest.fixture(autouse=True)
    def _patch_cache_dir(self, tmp_path, monkeypatch):
        monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda src: str(tmp_path / src["name"]))
        yield

    def _setup_active_hour(self, tmp_path):
        src = make_source(tmp_path, "svc-a", cache_key="cache_dir_override")
        active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
        hourly_dir = os.path.join(str(tmp_path / "svc-a"), "data", f"timestamp_hour={active_hour}")
        return src, active_hour, hourly_dir, datetime.now(UTC)

    def test_watermark_defaults_to_zero(self, tmp_path):
        src = make_source(tmp_path, "svc-a", cache_key="cache_dir_override")
        assert ph.read_partial_hour_watermark(src, "2026-09-09-14") == 0.0

    def test_all_fields_defaults_to_empty(self, tmp_path):
        src = make_source(tmp_path, "svc-a", cache_key="cache_dir_override")
        assert ph.read_partial_hour_all_fields(src, "2026-09-09-14") == []

    def test_total_defaults_to_zero(self, tmp_path):
        src = make_source(tmp_path, "svc-a", cache_key="cache_dir_override")
        assert ph.read_partial_hour_total(src, "2026-09-09-14") == 0

    def test_merge_partial_hour_writes_a_total_row_not_per_field_summable(self, tmp_path):
        src, active_hour, hourly_dir, now = self._setup_active_hour(tmp_path)
        _write_hourly_parquet(
            os.path.join(hourly_dir, "batch1.parquet"),
            [(now, "US", "GET"), (now, "US", "POST"), (now, "CA", "GET")],
            schema="timestamp TIMESTAMP, country VARCHAR, method VARCHAR",
        )
        assert ph.merge_partial_hour("svc-a", src, ["country", "method"])["new_files"] == 1
        per_field_rows = ph.read_partial_hour_all_fields(src, active_hour)
        assert sum(c for _, _, c in per_field_rows) == 6
        assert ph.read_partial_hour_total(src, active_hour) == 3
        assert all(field != ph.TOTAL_FIELD for field, _, _ in per_field_rows)

    def test_merge_partial_hour_excludes_a_custom_field_named_total_field(self, tmp_path):
        src, active_hour, hourly_dir, now = self._setup_active_hour(tmp_path)
        _write_hourly_parquet(
            os.path.join(hourly_dir, "batch1.parquet"),
            [(now, "US", "x"), (now, "CA", "y")],
            schema='timestamp TIMESTAMP, country VARCHAR, "__total__" VARCHAR',
        )
        assert ph.merge_partial_hour("svc-a", src, ["country", ph.TOTAL_FIELD])["new_files"] == 1
        assert ph.read_partial_hour_total(src, active_hour) == 2
        assert dict((v, c) for _, v, c in ph.read_partial_hour_all_fields(src, active_hour)) == {"US": 1, "CA": 1}

    def test_read_partial_hour_functions_reuse_caller_connection(self, tmp_path, monkeypatch):
        src, active_hour, hourly_dir, now = self._setup_active_hour(tmp_path)
        _write_hourly_parquet(os.path.join(hourly_dir, "batch1.parquet"), [(now, "US"), (now, "CA")])
        ph.merge_partial_hour("svc-a", src, ["country"])

        real_connect = duckdb.connect
        connect_calls = []
        caller_con = duckdb.connect(":memory:")
        monkeypatch.setattr(
            duckdb, "connect", lambda *a, **kw: (connect_calls.append((a, kw)), real_connect(*a, **kw))[1]
        )
        try:
            rows = ph.read_partial_hour_all_fields(src, active_hour, con=caller_con)
            total = ph.read_partial_hour_total(src, active_hour, con=caller_con)
        finally:
            caller_con.close()

        assert dict((v, c) for _, v, c in rows) == {"US": 1, "CA": 1} and total == 2 and connect_calls == []

    def test_merge_partial_hour_total_accumulates_across_ticks(self, tmp_path):
        src, active_hour, hourly_dir, now = self._setup_active_hour(tmp_path)
        _write_hourly_parquet(os.path.join(hourly_dir, "batch1.parquet"), [(now, "US"), (now, "CA")])
        ph.merge_partial_hour("svc-a", src, ["country"])
        assert ph.read_partial_hour_total(src, active_hour) == 2

        time.sleep(0.05)
        _write_hourly_parquet(os.path.join(hourly_dir, "batch2.parquet"), [(now, "US")])
        ph.merge_partial_hour("svc-a", src, ["country"])
        assert ph.read_partial_hour_total(src, active_hour) == 3

        ph.merge_partial_hour("svc-a", src, ["country"])
        assert ph.read_partial_hour_total(src, active_hour) == 3

    def test_merge_partial_hour_is_incremental_and_idempotent(self, tmp_path):
        src, active_hour, hourly_dir, now = self._setup_active_hour(tmp_path)
        _write_hourly_parquet(os.path.join(hourly_dir, "batch1.parquet"), [(now, "US"), (now, "US"), (now, "CA")])

        assert ph.merge_partial_hour("svc-a", src, ["country"])["new_files"] == 1
        assert dict((v, c) for _, v, c in ph.read_partial_hour_all_fields(src, active_hour)) == {"US": 2, "CA": 1}

        assert ph.merge_partial_hour("svc-a", src, ["country"])["new_files"] == 0
        assert dict((v, c) for _, v, c in ph.read_partial_hour_all_fields(src, active_hour)) == {"US": 2, "CA": 1}

        time.sleep(0.05)
        _write_hourly_parquet(os.path.join(hourly_dir, "batch2.parquet"), [(now, "US"), (now, "DE")])
        assert ph.merge_partial_hour("svc-a", src, ["country"])["new_files"] == 1
        assert dict((v, c) for _, v, c in ph.read_partial_hour_all_fields(src, active_hour)) == {
            "US": 3,
            "CA": 1,
            "DE": 1,
        }

    def test_merge_partial_hour_excludes_rows_before_hour_start(self, tmp_path):
        src, active_hour, hourly_dir, inside_hour = self._setup_active_hour(tmp_path)
        just_before_hour = datetime.strptime(active_hour, "%Y-%m-%d-%H").replace(tzinfo=UTC) - timedelta(seconds=5)
        _write_hourly_parquet(
            os.path.join(hourly_dir, "straddling.parquet"), [(just_before_hour, "PREV_HOUR_ONLY"), (inside_hour, "US")]
        )

        assert ph.merge_partial_hour("svc-a", src, ["country"])["new_files"] == 1
        assert dict((v, c) for _, v, c in ph.read_partial_hour_all_fields(src, active_hour)) == {"US": 1}

    def test_gc_stale_partial_hours_removes_old_but_not_active(self, tmp_path):
        src = make_source(tmp_path, "svc-a", cache_key="cache_dir_override")
        active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
        stale_hour = (datetime.now(UTC) - timedelta(hours=5)).strftime("%Y-%m-%d-%H")
        os.makedirs(ph.partial_hour_dir(src, active_hour))
        os.makedirs(ph.partial_hour_dir(src, stale_hour))

        assert ph.gc_stale_partial_hours(src, max_age_hours=2) == 1
        assert os.path.isdir(ph.partial_hour_dir(src, active_hour))
        assert not os.path.isdir(ph.partial_hour_dir(src, stale_hour))

    def test_gc_stale_partial_hours_ignores_malformed_hour_token(self, tmp_path):
        src = make_source(tmp_path, "svc-a", cache_key="cache_dir_override")
        active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
        stale_hour = (datetime.now(UTC) - timedelta(hours=5)).strftime("%Y-%m-%d-%H")
        malformed_dir = os.path.join(ph.partial_hour_root(src), "hour=garbage")
        for d in (ph.partial_hour_dir(src, active_hour), ph.partial_hour_dir(src, stale_hour), malformed_dir):
            os.makedirs(d)

        assert ph.gc_stale_partial_hours(src, max_age_hours=2) == 1
        assert os.path.isdir(ph.partial_hour_dir(src, active_hour)) and os.path.isdir(malformed_dir)
        assert not os.path.isdir(ph.partial_hour_dir(src, stale_hour))
