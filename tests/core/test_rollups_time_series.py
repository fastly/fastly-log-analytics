"""Tests for the per-hour 1-minute time_series bundle writer + its
backfill driver. Mirrors the sessions tests in structure — the two
writers share an architecture.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import duckdb
import pyarrow.parquet as pq


def _seed_logs(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict]) -> None:
    """Create ``table`` with the column set the time_series writer reads
    and INSERT rows."""
    con.execute(
        f"CREATE TABLE {table} (timestamp TIMESTAMPTZ, status INTEGER, cache VARCHAR, resp_bytes BIGINT, ttfb DOUBLE)"
    )
    for r in rows:
        con.execute(
            f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?)",
            [
                r["timestamp"],
                r.get("status"),
                r.get("cache"),
                r.get("resp_bytes"),
                r.get("ttfb"),
            ],
        )


@contextmanager
def _noop_lock(_key):
    yield


def _past_hour(hours_ago: int) -> tuple[str, datetime]:
    dt = (datetime.now(UTC) - timedelta(hours=hours_ago)).replace(minute=0, second=0, microsecond=0)
    return dt.strftime("%Y-%m-%d-%H"), dt


def test_build_time_series_writes_per_minute_buckets(tmp_path):
    """Happy path: closed hour with rows in 3 distinct minutes produces
    3 per-minute rows with the documented metric set."""
    from backend.core.rollups import time_series

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-ts"}
    hour_token, hour_dt = _past_hour(2)

    con = duckdb.connect(":memory:")
    _seed_logs(
        con,
        "logs_ts",
        [
            {
                "timestamp": hour_dt + timedelta(minutes=0, seconds=10),
                "status": 200,
                "cache": "HIT",
                "resp_bytes": 100,
                "ttfb": 0.05,
            },
            {
                "timestamp": hour_dt + timedelta(minutes=0, seconds=20),
                "status": 404,
                "cache": "HIT-STALE",
                "resp_bytes": 80,
                "ttfb": 0.1,
            },
            {
                "timestamp": hour_dt + timedelta(minutes=1),
                "status": 500,
                "cache": "MISS",
                "resp_bytes": 200,
                "ttfb": 0.2,
            },
            {
                "timestamp": hour_dt + timedelta(minutes=2),
                "status": 200,
                "cache": "HIT",
                "resp_bytes": 300,
                "ttfb": 0.15,
            },
        ],
    )

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value="logs_ts"),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
        patch(
            "backend.core.iceberg.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        n = time_series.build_time_series_bundles("svc-ts", src, [hour_token])

    assert n == 1
    bundle = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}" / "time_series.parquet"
    assert bundle.exists()

    t = pq.read_table(str(bundle))
    cols = set(t.column_names)
    assert {
        "bucket",
        "requests",
        "status_4xx",
        "status_5xx",
        "hits",
        "cache_total",
        "resp_bytes_sum",
        "ttfb_sum",
        "ttfb_count",
    }.issubset(cols)

    rows = sorted(t.to_pylist(), key=lambda r: r["bucket"])
    assert len(rows) == 3, f"expected 3 minute buckets; got {len(rows)}"
    # Minute 0: 2 requests, 1 in 4xx, both are HIT/HIT-STALE so hits=2
    assert rows[0]["requests"] == 2
    assert rows[0]["status_4xx"] == 1
    assert rows[0]["status_5xx"] == 0
    assert rows[0]["hits"] == 2
    assert rows[0]["cache_total"] == 2
    # Minute 1: 5xx
    assert rows[1]["status_5xx"] == 1
    # Minute 2: 200, HIT, ttfb=0.15
    assert rows[2]["ttfb_count"] == 1
    assert abs(rows[2]["ttfb_sum"] - 0.15) < 1e-9


def test_build_time_series_skips_active_hour(tmp_path):
    from backend.core.rollups import time_series

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    active = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    con = duckdb.connect(":memory:")
    _seed_logs(con, "logs_ts", [])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value="logs_ts"),
        patch("backend.core.duckdb.get_connection", return_value=con),
    ):
        n = time_series.build_time_series_bundles("svc", {"name": "svc"}, [active])

    assert n == 0


def test_build_time_series_empty_input_returns_zero():
    from backend.core.rollups import time_series

    assert time_series.build_time_series_bundles("svc", {"name": "svc"}, []) == 0


def test_build_time_series_malformed_hour_skipped(tmp_path):
    from backend.core.rollups import time_series

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    con = duckdb.connect(":memory:")
    _seed_logs(con, "logs_ts", [])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value="logs_ts"),
        patch("backend.core.duckdb.get_connection", return_value=con),
    ):
        assert time_series.build_time_series_bundles("svc", {"name": "svc"}, ["bad"]) == 0


def test_build_time_series_no_safe_table_returns_zero(tmp_path):
    from backend.core.rollups import time_series

    hour_token, _ = _past_hour(2)
    with patch("backend.core.rollups._common._safe_table_for", return_value=None):
        assert time_series.build_time_series_bundles("svc", {"name": "svc"}, [hour_token]) == 0


def test_build_time_series_no_timestamp_column_returns_zero(tmp_path):
    """Schema without timestamp column — the writer can't bucket; skip."""
    from backend.core.rollups import time_series

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    hour_token, _ = _past_hour(2)
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE logs_no_ts (status INTEGER)")

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value="logs_no_ts"),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch(
            "backend.core.iceberg.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        assert time_series.build_time_series_bundles("svc", {"name": "svc"}, [hour_token]) == 0


def test_build_time_series_describe_failure_returns_zero(tmp_path):
    from backend.core.rollups import time_series

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    hour_token, _ = _past_hour(2)
    con = duckdb.connect(":memory:")

    def _boom(_c, _src, _fn):
        raise duckdb.Error("synthetic")

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value="logs_x"),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch("backend.core.iceberg.execute_with_stale_view_retry", side_effect=_boom),
    ):
        assert time_series.build_time_series_bundles("svc", {"name": "svc"}, [hour_token]) == 0


def test_build_time_series_columns_absent_use_constant_zero(tmp_path):
    """Schema missing status/cache/resp_bytes/ttfb → those columns
    are written as constant 0 so the parquet shape stays uniform."""
    from backend.core.rollups import time_series

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-bare"}
    hour_token, hour_dt = _past_hour(2)

    con = duckdb.connect(":memory:")
    # Only timestamp.
    con.execute("CREATE TABLE logs_bare (timestamp TIMESTAMPTZ)")
    con.execute("INSERT INTO logs_bare VALUES (?)", [hour_dt + timedelta(minutes=1)])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value="logs_bare"),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
        patch(
            "backend.core.iceberg.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        n = time_series.build_time_series_bundles("svc-bare", src, [hour_token])

    assert n == 1
    bundle = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}" / "time_series.parquet"
    rows = pq.read_table(str(bundle)).to_pylist()
    assert len(rows) == 1
    assert rows[0]["requests"] == 1
    assert rows[0]["status_4xx"] == 0
    assert rows[0]["status_5xx"] == 0
    assert rows[0]["hits"] == 0
    assert rows[0]["cache_total"] == 0
    assert rows[0]["resp_bytes_sum"] == 0
    assert rows[0]["ttfb_count"] == 0
