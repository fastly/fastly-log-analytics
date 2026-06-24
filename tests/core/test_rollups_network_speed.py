"""Tests for the per-hour network_speed rollup writer + reader + day
compactor (Task A4).

network_speed differs from network_rtt in that the math is EXACT
across hours — pure SUM of integer counts, no weighted-average. The
reader returns rows the live SQL produces; the caller dict-keys by
ASN as before. No ``_approx`` flag.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


def _write_hour_speed(cache_root: str, hour: str, rows: list[dict]) -> str:
    d = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    table = pa.table(
        {
            "asn": pa.array([r["asn"] for r in rows], type=pa.int64()),
            "c_speed": pa.array([r["c_speed"] for r in rows]),
            "count": pa.array([r["count"] for r in rows], type=pa.int64()),
        }
    )
    p = os.path.join(d, "network_speed.parquet")
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


def test_backfill_skips_built_hours(tmp_path):
    """Backfill walks rollups/hour_bundled and only queues hours WITH
    all_fields.parquet AND WITHOUT network_speed.parquet."""
    from backend.core.rollups import network_speed

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-ns-bf", "service_id": "svc-ns-bf"}
    yday = _yesterday_iso()

    _write_hour_all_fields(str(cache_root), f"{yday}-10")
    _write_hour_all_fields(str(cache_root), f"{yday}-11")
    _write_hour_speed(str(cache_root), f"{yday}-11", [{"asn": 1, "c_speed": "fast", "count": 10}])

    captured: list[list[str]] = []

    def _stub_build(_sid, _src, hours):
        captured.append(list(hours))
        return len(hours)

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        with patch.object(network_speed, "build_network_speed_bundles", side_effect=_stub_build):
            n = network_speed.backfill_network_speed_bundles("svc-ns-bf", src)

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
    runner._table = "logs_svc_ns_reader"
    runner.execute = _Conn().execute  # type: ignore[method-assign]
    return runner


def test_reader_returns_none_when_filtered(tmp_path):
    src = {"name": "svc-ns-r", "service_id": "svc-ns-r"}
    runner = _stub_runner(src, [], [])
    result = runner.try_network_speed_from_rollup(
        "2026-05-01T00:00:00+00:00",
        "2026-05-30T00:00:00+00:00",
        top_asns=[1],
        has_filters=True,
    )
    assert result is None


def test_reader_returns_none_for_short_window(tmp_path):
    src = {"name": "svc-ns-r2", "service_id": "svc-ns-r2"}
    runner = _stub_runner(src, [], [])
    result = runner.try_network_speed_from_rollup(
        "2026-05-01T00:00:00+00:00",
        "2026-05-01T12:00:00+00:00",
        top_asns=[1],
        has_filters=False,
    )
    assert result is None


def test_reader_serves_rows_with_prune_to_top_asns(tmp_path):
    """Eligible window → reader builds a read_parquet SQL with the ASN
    list bound, returns (asn, c_speed, count) tuples."""
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-ns-r3", "service_id": "svc-ns-r3"}
    day_a = _three_days_ago_iso()
    day_b = _two_days_ago_iso()

    for d_iso in (day_a, day_b):
        for h in range(24):
            _write_hour_speed(
                str(cache_root),
                f"{d_iso}-{h:02d}",
                [
                    {"asn": 7922, "c_speed": "fast", "count": 50},
                    {"asn": 15169, "c_speed": "slow", "count": 30},
                ],
            )

    captured: list[str] = []
    stub_rows = [(7922, "fast", 2400), (15169, "slow", 1440)]
    runner = _stub_runner(src, captured, stub_rows)

    st_iso = f"{day_a}T00:00:00+00:00"
    end_iso = (datetime.fromisoformat(st_iso) + timedelta(days=2)).isoformat()
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        result = runner.try_network_speed_from_rollup(
            st_iso,
            end_iso,
            top_asns=[7922, 15169],
            has_filters=False,
        )

    assert result == [(7922, "fast", 2400), (15169, "slow", 1440)]
    assert len(captured) == 1
    sql = captured[0]
    assert "network_speed.parquet" in sql
    assert "asn IN (?, ?)" in sql
    assert "GROUP BY asn, c_speed" in sql


def test_compact_writes_per_day_file_with_correct_sums(tmp_path):
    """24 hour files for a closed day → 1 day file. Exact integer SUMs
    per (asn, c_speed); no weighted-average since counts are exact."""
    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-ns-cd", "service_id": "svc-ns-cd"}
    day = _yesterday_iso()

    for h in range(24):
        _write_hour_speed(
            str(cache_root),
            f"{day}-{h:02d}",
            [
                {"asn": 7922, "c_speed": "fast", "count": 100},
                {"asn": 7922, "c_speed": "slow", "count": 25},
                {"asn": 15169, "c_speed": "fast", "count": 50},
            ],
        )

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
            rebuilt = rollups.compact_network_speed_closed_days_to_daily("svc-ns-cd", src)

    assert rebuilt == 1
    day_file = cache_root / "rollups" / "day_bundled" / f"day={day}" / "network_speed.parquet"
    assert day_file.exists()

    con = duckdb.connect(":memory:")
    try:
        rows = con.execute(
            f"SELECT asn, c_speed, count FROM read_parquet('{day_file}') ORDER BY asn, c_speed"
        ).fetchall()
    finally:
        con.close()
    # 24 hours × the per-hour counts: 7922/fast=2400, 7922/slow=600, 15169/fast=1200
    assert rows == [
        (7922, "fast", 2400),
        (7922, "slow", 600),
        (15169, "fast", 1200),
    ]


def test_compact_skips_active_day(tmp_path):
    from backend.core import rollups

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-ns-active", "service_id": "svc-ns-active"}
    today = datetime.now(UTC).strftime("%Y-%m-%d")

    for h in range(24):
        _write_hour_speed(
            str(cache_root),
            f"{today}-{h:02d}",
            [{"asn": 1, "c_speed": "fast", "count": 5}],
        )

    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        with patch("backend.core.iceberg.view._get_service_lock", _noop_lock):
            rebuilt = rollups.compact_network_speed_closed_days_to_daily("svc-ns-active", src)

    assert rebuilt == 0
    assert not (cache_root / "rollups" / "day_bundled" / f"day={today}").exists()
