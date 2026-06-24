"""Tests for the per-hour sessions bundle writer + its backfill driver.

The writer COPIES (ip, ja4)-grouped aggregates from the service's live
DuckDB view into ``rollups/hour_bundled/hour=H/sessions.parquet``.
``/api/sessions`` reads this instead of re-scanning raw logs.

Strategy: seed an in-memory DuckDB with the columns the COPY references,
patch ``get_connection`` to return it, exercise the function, then
inspect the written parquet.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import duckdb
import pyarrow.parquet as pq


def _seed_logs(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict]) -> None:
    """Create ``table`` and INSERT ``rows`` (dicts with keys matching
    the column set the sessions writer reads)."""
    cols_sql = (
        "timestamp TIMESTAMPTZ, ip VARCHAR, ja4 VARCHAR, country VARCHAR, "
        "asn INTEGER, status INTEGER, resp_bytes BIGINT, tcp_rtt DOUBLE, "
        "edge INTEGER, ua VARCHAR, edge_sid VARCHAR"
    )
    con.execute(f"CREATE TABLE {table} ({cols_sql})")
    for r in rows:
        con.execute(
            f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                r["timestamp"],
                r.get("ip"),
                r.get("ja4"),
                r.get("country"),
                r.get("asn"),
                r.get("status"),
                r.get("resp_bytes"),
                r.get("tcp_rtt"),
                r.get("edge"),
                r.get("ua"),
                r.get("edge_sid"),
            ],
        )


@contextmanager
def _noop_lock(_key):
    yield


def _past_hour(hours_ago: int) -> tuple[str, datetime]:
    dt = (datetime.now(UTC) - timedelta(hours=hours_ago)).replace(minute=0, second=0, microsecond=0)
    return dt.strftime("%Y-%m-%d-%H"), dt


def test_build_sessions_writes_aggregates_for_closed_hour(tmp_path):
    """Happy path: a closed hour with real (ip, ja4) traffic produces a
    sessions.parquet with one row per group + the expected aggregates."""
    from backend.core.rollups import sessions

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-sess", "service_id": "svc-sess"}

    hour_token, hour_dt = _past_hour(2)
    con = duckdb.connect(":memory:")
    _seed_logs(
        con,
        "logs_svc_sess",
        [
            {
                "timestamp": hour_dt + timedelta(minutes=1),
                "ip": "1.1.1.1",
                "ja4": "ja4-A",
                "country": "US",
                "asn": 100,
                "status": 200,
                "resp_bytes": 500,
                "tcp_rtt": 12.5,
                "edge": 1,
                "ua": "Mozilla/5.0",
                "edge_sid": "sid-1",
            },
            {
                "timestamp": hour_dt + timedelta(minutes=2),
                "ip": "1.1.1.1",
                "ja4": "ja4-A",
                "country": "US",
                "asn": 100,
                "status": 404,
                "resp_bytes": 100,
                "tcp_rtt": 18.0,
                "edge": 1,
                "ua": "Mozilla/5.0",
                "edge_sid": "sid-2",
            },
            {
                "timestamp": hour_dt + timedelta(minutes=5),
                "ip": "2.2.2.2",
                "ja4": "ja4-B",
                "country": "JP",
                "asn": 200,
                "status": 500,
                "resp_bytes": 250,
                "tcp_rtt": 30.0,
                "edge": 0,
                "ua": "curl/8.0",
                "edge_sid": "sid-3",
            },
        ],
    )

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value="logs_svc_sess"),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
        patch(
            "backend.core.iceberg.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        n = sessions.build_session_bundles("svc-sess", src, [hour_token])

    assert n == 1, f"expected 1 bundle written; got {n}"

    bundle = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}" / "sessions.parquet"
    assert bundle.exists(), f"sessions.parquet missing at {bundle}"

    t = pq.read_table(str(bundle))
    cols = set(t.column_names)
    assert {
        "bucket",
        "ip",
        "ja4",
        "first_ts",
        "last_ts",
        "req_count",
        "country",
        "asn",
        "reqs_4xx",
        "reqs_5xx",
        "total_bytes",
        "rtt_sum",
        "rtt_count",
        "edge_count",
        "shield_count",
        "ua_min",
        "edge_sid_max",
    }.issubset(cols), f"missing columns: {cols}"

    rows_by_ip = {r["ip"]: r for r in t.to_pylist()}
    a = rows_by_ip["1.1.1.1"]
    assert a["req_count"] == 2
    assert a["reqs_4xx"] == 1  # status=404
    assert a["reqs_5xx"] == 0
    assert a["total_bytes"] == 600
    assert a["edge_count"] == 2
    assert a["shield_count"] == 0

    b = rows_by_ip["2.2.2.2"]
    assert b["req_count"] == 1
    assert b["reqs_5xx"] == 1
    assert b["edge_count"] == 0
    assert b["shield_count"] == 1


def test_build_sessions_skips_active_hour(tmp_path):
    """Active UTC hour must be skipped — its data is still in flight."""
    from backend.core.rollups import sessions

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-sess-active"}
    active = datetime.now(UTC).strftime("%Y-%m-%d-%H")

    con = duckdb.connect(":memory:")
    _seed_logs(con, "logs_x", [])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value="logs_x"),
        patch("backend.core.duckdb.get_connection", return_value=con),
    ):
        n = sessions.build_session_bundles("svc-sess-active", src, [active])

    assert n == 0
    assert not (cache_root / "rollups" / "hour_bundled" / f"hour={active}" / "sessions.parquet").exists()


def test_build_sessions_no_hours_returns_zero(tmp_path):
    from backend.core.rollups import sessions

    assert sessions.build_session_bundles("svc", {"name": "svc"}, []) == 0


def test_build_sessions_malformed_hour_token_skipped(tmp_path):
    """Bad hour token (not YYYY-MM-DD-HH) → logged + skipped, no crash."""
    from backend.core.rollups import sessions

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    con = duckdb.connect(":memory:")
    _seed_logs(con, "logs_x", [])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value="logs_x"),
        patch("backend.core.duckdb.get_connection", return_value=con),
    ):
        n = sessions.build_session_bundles("svc", {"name": "svc"}, ["not-an-hour"])

    assert n == 0


def test_build_sessions_no_safe_table_returns_zero(tmp_path):
    from backend.core.rollups import sessions

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    hour_token, _ = _past_hour(2)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value=None),
    ):
        assert sessions.build_session_bundles("svc", {"name": "svc"}, [hour_token]) == 0


def test_build_sessions_missing_ip_column_returns_zero(tmp_path):
    """Service whose schema has timestamp but no ip column → can't roll
    up sessions, skip cleanly."""
    from backend.core.rollups import sessions

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    hour_token, hour_dt = _past_hour(2)
    con = duckdb.connect(":memory:")
    # Note: no ip column.
    con.execute("CREATE TABLE logs_no_ip (timestamp TIMESTAMPTZ, status INTEGER)")

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value="logs_no_ip"),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch(
            "backend.core.iceberg.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        n = sessions.build_session_bundles("svc", {"name": "svc"}, [hour_token])

    assert n == 0


def test_build_sessions_describe_failure_returns_zero(tmp_path):
    """If DESCRIBE blows up (stale view, corrupt table), the writer
    logs + returns 0 instead of crashing the cron."""
    from backend.core.rollups import sessions

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    hour_token, _ = _past_hour(2)
    con = duckdb.connect(":memory:")

    def _boom(_c, _src, _fn):
        raise duckdb.Error("synthetic describe failure")

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value="logs_x"),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch("backend.core.iceberg.execute_with_stale_view_retry", side_effect=_boom),
    ):
        n = sessions.build_session_bundles("svc", {"name": "svc"}, [hour_token])

    assert n == 0


def test_build_sessions_ja4_absent_writes_null(tmp_path):
    """Schema without ja4 column → ja4 is written as NULL VARCHAR (the
    parquet shape must stay uniform across services)."""
    from backend.core.rollups import sessions

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-no-ja4"}
    hour_token, hour_dt = _past_hour(2)

    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE logs_x (timestamp TIMESTAMPTZ, ip VARCHAR)")
    con.execute("INSERT INTO logs_x VALUES (?, ?)", [hour_dt + timedelta(minutes=3), "9.9.9.9"])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value="logs_x"),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
        patch(
            "backend.core.iceberg.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        n = sessions.build_session_bundles("svc-no-ja4", src, [hour_token])

    assert n == 1
    bundle = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}" / "sessions.parquet"
    t = pq.read_table(str(bundle))
    rows = t.to_pylist()
    assert len(rows) == 1
    assert rows[0]["ja4"] is None
    assert rows[0]["country"] is None  # no country column either
    assert rows[0]["edge_count"] == 0  # no edge column → constant 0
