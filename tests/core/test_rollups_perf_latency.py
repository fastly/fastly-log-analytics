"""Tests for the perf_latency rollup (top_urls + top_asns elapsed percentiles)
writer/backfill + hybrid-free reader + weighted-avg day compactor.

Same per-dimension-percentiles-over-window shape as slow_urls (per-URL) and
network_rtt (per-ASN): one module, two parquet files (perf_top_urls /
perf_top_asns) per hour. Percentiles are request-weight-averaged across hours
(biased → _approx); avg is exact (elapsed_sum / elapsed_count). The reader
re-ranks by the caller's sort_by.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

_COLS = ("value", "requests", "elapsed_count", "elapsed_sum", "p50_us", "p95_us", "p99_us")


def _write_hour_perf(cache_root: str, hour: str, rows: list[dict], filename: str = "perf_top_urls.parquet") -> str:
    d = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "value": pa.array([r["value"] for r in rows]),
            "requests": pa.array([r["requests"] for r in rows], type=pa.int64()),
            "elapsed_count": pa.array([r["elapsed_count"] for r in rows], type=pa.int64()),
            "elapsed_sum": pa.array([r["elapsed_sum"] for r in rows], type=pa.float64()),
            "p50_us": pa.array([r["p50_us"] for r in rows], type=pa.float64()),
            "p95_us": pa.array([r["p95_us"] for r in rows], type=pa.float64()),
            "p99_us": pa.array([r["p99_us"] for r in rows], type=pa.float64()),
        }
    )
    p = os.path.join(d, filename)
    pq.write_table(table, p)
    return p


def _write_hour_all_fields(cache_root: str, hour: str) -> None:
    d = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    pq.write_table(
        pa.table({"field": pa.array(["x"]), "value": pa.array(["y"]), "count": pa.array([1], type=pa.int64())}),
        os.path.join(d, "all_fields.parquet"),
    )


@contextmanager
def _noop_lock(_key):
    yield


def _yesterday_iso() -> str:
    return (datetime.now(UTC).date() - timedelta(days=1)).strftime("%Y-%m-%d")


def _three_days_ago_iso() -> str:
    return (datetime.now(UTC).date() - timedelta(days=3)).strftime("%Y-%m-%d")


def _two_days_ago_iso() -> str:
    return (datetime.now(UTC).date() - timedelta(days=2)).strftime("%Y-%m-%d")


def _row(value: str, requests: int, p50: float, p95: float, p99: float, avg_us: float = 1000.0) -> dict:
    return {
        "value": value,
        "requests": requests,
        "elapsed_count": requests,
        "elapsed_sum": avg_us * requests,
        "p50_us": p50,
        "p95_us": p95,
        "p99_us": p99,
    }


def test_backfill_skips_built_hours(tmp_path):
    """Backfill walks hour_bundled and queues hours WITH all_fields.parquet
    but WITHOUT perf_top_urls.parquet (the sentinel)."""
    from backend.core.rollups import perf_latency

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-pl-bf", "service_id": "svc-pl-bf"}
    yday = _yesterday_iso()

    _write_hour_all_fields(str(cache_root), f"{yday}-10")
    _write_hour_all_fields(str(cache_root), f"{yday}-11")
    _write_hour_perf(str(cache_root), f"{yday}-11", [_row("/a", 10, 1.0, 2.0, 3.0)])

    captured: list[list[str]] = []

    def _stub_build(_sid, _src, hours):
        captured.append(list(hours))
        return len(hours)

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        with patch.object(perf_latency, "build_perf_latency_bundles", side_effect=_stub_build):
            n = perf_latency.backfill_perf_latency_bundles("svc-pl-bf", src)

    assert n == 1
    assert captured == [[f"{yday}-10"]]


def _stub_runner(src: dict, captured_sql: list[str], stub_rows: list[tuple]):
    from backend.repositories._base import QueryRunner

    class _Result:
        def fetchall(self):
            return stub_rows

    class _Conn:
        def execute(self, sql, params=None):
            captured_sql.append(sql)
            return _Result()

    runner = QueryRunner.__new__(QueryRunner)
    runner.src = src
    runner._table = "logs_svc_pl_reader"
    runner.execute = _Conn().execute  # type: ignore[method-assign]
    return runner


def test_reader_returns_none_when_filtered(tmp_path):
    src = {"name": "svc-pl-r", "service_id": "svc-pl-r"}
    runner = _stub_runner(src, [], [])
    result = runner.try_perf_latency_from_rollup(
        "2026-05-01T00:00:00+00:00",
        "2026-05-30T00:00:00+00:00",
        dimension="url",
        sort_by="p99",
        has_filters=True,
        min_requests=5,
        limit=20,
    )
    assert result is None


def test_reader_returns_none_for_short_window(tmp_path):
    src = {"name": "svc-pl-r2", "service_id": "svc-pl-r2"}
    runner = _stub_runner(src, [], [])
    result = runner.try_perf_latency_from_rollup(
        "2026-05-01T00:00:00+00:00",
        "2026-05-01T12:00:00+00:00",
        dimension="url",
        sort_by="p99",
        has_filters=False,
        min_requests=5,
        limit=20,
    )
    assert result is None


def _serve_window(cache_root, filename: str):
    day_a = _three_days_ago_iso()
    day_b = _two_days_ago_iso()
    for d_iso in (day_a, day_b):
        for h in range(24):
            _write_hour_perf(
                str(cache_root),
                f"{d_iso}-{h:02d}",
                [_row("/a", 50, 1000.0, 2000.0, 3000.0), _row("/b", 30, 500.0, 900.0, 1500.0)],
                filename=filename,
            )
    st_iso = f"{day_a}T00:00:00+00:00"
    end_iso = (datetime.fromisoformat(st_iso) + timedelta(days=2)).isoformat()
    return st_iso, end_iso


def test_reader_serves_url_rows_weighted_avg(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-pl-r3", "service_id": "svc-pl-r3"}
    st_iso, end_iso = _serve_window(cache_root, "perf_top_urls.parquet")

    captured: list[str] = []
    # (value, requests, avg_us, p50_us_w, p95_us_w, p99_us_w)
    stub_rows = [("/a", 2400, 1000.0, 1000.0, 2000.0, 3000.0)]
    runner = _stub_runner(src, captured, stub_rows)

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        result = runner.try_perf_latency_from_rollup(
            st_iso,
            end_iso,
            dimension="url",
            sort_by="p99",
            has_filters=False,
            min_requests=5,
            limit=20,
        )

    assert result == {
        "rows": [{"value": "/a", "requests": 2400, "avg_ms": 1.0, "p50_ms": 1.0, "p95_ms": 2.0, "p99_ms": 3.0}],
        "_approx": True,
    }
    assert len(captured) == 1
    sql = captured[0]
    assert "perf_top_urls.parquet" in sql
    assert "read_parquet" in sql
    assert "SUM(p99_us * requests)" in sql
    assert "GROUP BY value" in sql
    assert "ORDER BY p99_us_w DESC" in sql  # default sort_by=p99


def test_reader_asn_dim_and_sort_by_p95(tmp_path):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-pl-r4", "service_id": "svc-pl-r4"}
    st_iso, end_iso = _serve_window(cache_root, "perf_top_asns.parquet")

    captured: list[str] = []
    runner = _stub_runner(src, captured, [("7922", 100, 1000.0, 1.0, 2.0, 3.0)])
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        result = runner.try_perf_latency_from_rollup(
            st_iso,
            end_iso,
            dimension="asn",
            sort_by="p95",
            has_filters=False,
            min_requests=10,
            limit=20,
        )
    assert result is not None
    sql = captured[0]
    assert "perf_top_asns.parquet" in sql
    assert "ORDER BY p95_us_w DESC" in sql  # sort_by=p95 honored


def test_compact_weighted_avg_per_day(tmp_path):
    """24 hour files → 1 day file: requests summed, percentiles request-
    weight-averaged. Same value+p95 each hour → day p95 unchanged, requests
    summed."""
    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-pl-cd", "service_id": "svc-pl-cd"}
    day = _yesterday_iso()

    for h in range(24):
        _write_hour_perf(
            str(cache_root),
            f"{day}-{h:02d}",
            [_row("/a", 10, 100.0, 200.0, 300.0), _row("/b", 5, 50.0, 90.0, 150.0)],
            filename="perf_top_urls.parquet",
        )

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
            rebuilt = rollups.compact_perf_latency_closed_days_to_daily("svc-pl-cd", src)

    assert rebuilt >= 1
    day_file = cache_root / "rollups" / "day_bundled" / f"day={day}" / "perf_top_urls.parquet"
    assert day_file.exists()

    con = duckdb.connect(":memory:")
    try:
        rows = con.execute(f"SELECT value, requests, p95_us FROM read_parquet('{day_file}') ORDER BY value").fetchall()
    finally:
        con.close()
    # /a: 24×10 reqs = 240, p95 weighted-avg of identical 200 = 200
    # /b: 24×5 reqs = 120, p95 = 90
    assert rows == [("/a", 240, 200.0), ("/b", 120, 90.0)]


def test_compact_skips_active_day(tmp_path):
    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-pl-active", "service_id": "svc-pl-active"}
    today = datetime.now(UTC).strftime("%Y-%m-%d")

    for h in range(24):
        _write_hour_perf(str(cache_root), f"{today}-{h:02d}", [_row("/a", 10, 100.0, 200.0, 300.0)])

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
            rebuilt = rollups.compact_perf_latency_closed_days_to_daily("svc-pl-active", src)

    assert rebuilt == 0
    assert not (cache_root / "rollups" / "day_bundled" / f"day={today}").exists()
