"""Tests for the per-hour sessions bundle writer + its backfill driver.

The writer COPIES (ip, ja4)-grouped aggregates from the service's live
DuckDB view into ``rollups/hour_bundled/hour=H/sessions.parquet``.
``/api/sessions`` reads this instead of re-scanning raw logs.

Strategy: seed an in-memory DuckDB with the columns the COPY references,
patch ``get_connection`` to return it, exercise the function, then
inspect the written parquet.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

import duckdb
import pyarrow.parquet as pq
import pytest

from backend.core.rollups.rum import backfill_missing_rum_aggregates, recompute_rum_aggregates, table_exists
from backend.repositories import rum as rum_repo
from tests._helpers.rollups import noop_lock as _noop_lock
from tests._helpers.rollups import past_hour as _past_hour
from tests._helpers.rollups import writer_patches


# ── Merged from tests/core/test_rollups_sessions.py ──
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


@pytest.mark.usefixtures("frozen_clock")
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

    with writer_patches(cache_root, "logs_svc_sess", con):
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


@pytest.mark.usefixtures("frozen_clock")
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


@pytest.mark.usefixtures("frozen_clock")
def test_build_sessions_no_hours_returns_zero(tmp_path):
    from backend.core.rollups import sessions

    assert sessions.build_session_bundles("svc", {"name": "svc"}, []) == 0


@pytest.mark.usefixtures("frozen_clock")
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


@pytest.mark.usefixtures("frozen_clock")
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


@pytest.mark.usefixtures("frozen_clock")
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


@pytest.mark.usefixtures("frozen_clock")
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


@pytest.mark.usefixtures("frozen_clock")
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

    with writer_patches(cache_root, "logs_x", con):
        n = sessions.build_session_bundles("svc-no-ja4", src, [hour_token])

    assert n == 1
    bundle = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}" / "sessions.parquet"
    t = pq.read_table(str(bundle))
    rows = t.to_pylist()
    assert len(rows) == 1
    assert rows[0]["ja4"] is None
    assert rows[0]["country"] is None  # no country column either
    assert rows[0]["edge_count"] == 0  # no edge column → constant 0


# ── Merged from tests/core/test_rollups_time_series.py ──
def _seed_logs_rollups_time_series(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict]) -> None:
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


@pytest.mark.usefixtures("frozen_clock")
def test_build_time_series_writes_per_minute_buckets(tmp_path):
    """Happy path: closed hour with rows in 3 distinct minutes produces
    3 per-minute rows with the documented metric set."""
    from backend.core.rollups import time_series

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-ts"}
    hour_token, hour_dt = _past_hour(2)

    con = duckdb.connect(":memory:")
    _seed_logs_rollups_time_series(
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

    with writer_patches(cache_root, "logs_ts", con):
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


@pytest.mark.usefixtures("frozen_clock")
def test_build_time_series_replaces_an_existing_hour_bundle(tmp_path):
    from backend.core.rollups import time_series

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-ts-rebuild"}
    hour_token, hour_dt = _past_hour(2)
    db_path = tmp_path / "source.duckdb"
    con = duckdb.connect(str(db_path))
    _seed_logs_rollups_time_series(
        con,
        "logs_ts",
        [
            {
                "timestamp": hour_dt + timedelta(minutes=59),
                "status": 200,
                "cache": "MISS",
                "resp_bytes": 100,
                "ttfb": 0.05,
            }
        ],
    )

    bundle = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}" / "time_series.parquet"
    with writer_patches(cache_root, "logs_ts", con):
        assert time_series.build_time_series_bundles("svc-ts-rebuild", src, [hour_token]) == 1

    assert pq.read_table(str(bundle))["requests"].to_pylist() == [1]

    con = duckdb.connect(str(db_path))
    con.execute(
        "INSERT INTO logs_ts VALUES (?, ?, ?, ?, ?)",
        [hour_dt + timedelta(minutes=59, seconds=30), 200, "MISS", 100, 0.05],
    )
    with writer_patches(cache_root, "logs_ts", con):
        assert time_series.build_time_series_bundles("svc-ts-rebuild", src, [hour_token]) == 1

    assert pq.read_table(str(bundle))["requests"].to_pylist() == [2]


@pytest.mark.usefixtures("frozen_clock")
def test_build_time_series_skips_active_hour(tmp_path):
    from backend.core.rollups import time_series

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    active = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    con = duckdb.connect(":memory:")
    _seed_logs_rollups_time_series(con, "logs_ts", [])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value="logs_ts"),
        patch("backend.core.duckdb.get_connection", return_value=con),
    ):
        n = time_series.build_time_series_bundles("svc", {"name": "svc"}, [active])

    assert n == 0


@pytest.mark.usefixtures("frozen_clock")
def test_build_time_series_empty_input_returns_zero():
    from backend.core.rollups import time_series

    assert time_series.build_time_series_bundles("svc", {"name": "svc"}, []) == 0


@pytest.mark.usefixtures("frozen_clock")
def test_build_time_series_malformed_hour_skipped(tmp_path):
    from backend.core.rollups import time_series

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    con = duckdb.connect(":memory:")
    _seed_logs_rollups_time_series(con, "logs_ts", [])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value="logs_ts"),
        patch("backend.core.duckdb.get_connection", return_value=con),
    ):
        assert time_series.build_time_series_bundles("svc", {"name": "svc"}, ["bad"]) == 0


@pytest.mark.usefixtures("frozen_clock")
def test_build_time_series_no_safe_table_returns_zero(tmp_path):
    from backend.core.rollups import time_series

    hour_token, _ = _past_hour(2)
    with patch("backend.core.rollups._common._safe_table_for", return_value=None):
        assert time_series.build_time_series_bundles("svc", {"name": "svc"}, [hour_token]) == 0


@pytest.mark.usefixtures("frozen_clock")
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


@pytest.mark.usefixtures("frozen_clock")
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


@pytest.mark.usefixtures("frozen_clock")
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

    with writer_patches(cache_root, "logs_bare", con):
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


# ── Merged from tests/core/test_rollups_rum.py ──
def test_recompute_rum_aggregates_idempotency_and_correctness():
    # 1. Setup in-memory DuckDB connection
    con = duckdb.connect(":memory:")

    # 2. Create raw client_vitals and client_errors tables
    con.execute("""
        CREATE TABLE client_vitals (
            timestamp TIMESTAMPTZ,
            metric_name VARCHAR,
            metric_value DOUBLE,
            metric_rating VARCHAR,
            pathname VARCHAR,
            browser VARCHAR,
            os VARCHAR,
            device VARCHAR,
            cid VARCHAR,
            req_id VARCHAR,
            city VARCHAR,
            region VARCHAR,
            country VARCHAR,
            pop VARCHAR,
            tls VARCHAR,
            ttfb DOUBLE
        )
    """)
    con.execute("""
        CREATE TABLE client_errors (
            timestamp TIMESTAMPTZ,
            error_message VARCHAR,
            error_file VARCHAR,
            error_line INT,
            error_col INT,
            pathname VARCHAR,
            browser VARCHAR,
            os VARCHAR,
            device VARCHAR,
            cid VARCHAR,
            req_id VARCHAR,
            city VARCHAR,
            region VARCHAR,
            country VARCHAR,
            pop VARCHAR,
            tls VARCHAR,
            ttfb DOUBLE
        )
    """)

    # 3. Seed some RUM beacons
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    hour1 = now - timedelta(hours=2)
    hour2 = now - timedelta(hours=1)

    # Beacons for hour1
    con.execute(
        "INSERT INTO client_vitals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            hour1,
            "LCP",
            2.5,
            "good",
            "/home",
            "Chrome",
            "macOS",
            "desktop",
            "user1",
            "req1",
            "SFO",
            "CA",
            "US",
            "SFO",
            "TLS1.3",
            0.1,
        ],
    )
    con.execute(
        "INSERT INTO client_vitals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            hour1,
            "LCP",
            4.5,
            "poor",
            "/home",
            "Chrome",
            "macOS",
            "desktop",
            "user2",
            "req2",
            "SFO",
            "CA",
            "US",
            "SFO",
            "TLS1.3",
            0.1,
        ],
    )
    con.execute(
        "INSERT INTO client_errors VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            hour1,
            "ReferenceError",
            "app.js",
            10,
            20,
            "/home",
            "Chrome",
            "macOS",
            "desktop",
            "user1",
            "req1",
            "SFO",
            "CA",
            "US",
            "SFO",
            "TLS1.3",
            0.1,
        ],
    )

    # Beacons for hour2
    con.execute(
        "INSERT INTO client_vitals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            hour2,
            "CLS",
            0.05,
            "good",
            "/about",
            "Safari",
            "iOS",
            "mobile",
            "user3",
            "req3",
            "NYC",
            "NY",
            "US",
            "JFK",
            "TLS1.3",
            0.1,
        ],
    )

    # 4. Trigger recomputation
    recompute_rum_aggregates(con, "svc-rum-test")

    # 5. Assertions: check table existence
    assert table_exists(con, "rum_vitals_aggregates")
    assert table_exists(con, "rum_error_aggregates")

    # Check vitals aggregates
    v_rows = con.execute(
        "SELECT dimension, value, event_count, good_count, poor_count, p75_value, bucket_start FROM rum_vitals_aggregates ORDER BY dimension, value, bucket_start"
    ).fetchall()
    # Expect:
    # - 'browser' -> Chrome (2), Safari (1)
    # - 'device' -> desktop (2), mobile (1)
    # - 'metric_name' -> LCP (2, good=1, poor=1), CLS (1, good=1)
    # - 'os' -> macOS (2), iOS (1)
    # - 'pathname' -> /home (2), /about (1)
    # - 'total' -> pageviews (2 + 1 = 3)
    dimensions = [r[0] for r in v_rows]
    assert "browser" in dimensions
    assert "metric_name" in dimensions
    assert "total" in dimensions

    # Check metric_name p75_value calculation
    lcp_row = [r for r in v_rows if r[0] == "metric_name" and r[1] == "LCP"][0]
    assert lcp_row[2] == 2  # event_count
    assert lcp_row[3] == 1  # good_count
    assert lcp_row[4] == 1  # poor_count
    assert lcp_row[5] == 4.0  # p75 of [2.5, 4.5] is exact 4.0 under PERCENTILE_CONT

    # Check errors aggregates
    e_rows = con.execute(
        "SELECT dimension, value, error_count, bucket_start FROM rum_error_aggregates ORDER BY dimension, value, bucket_start"
    ).fetchall()
    # Expect:
    # - 'total' -> errors (1)
    # - 'pathname' -> /home (1)
    # - 'exception' -> ReferenceError|app.js|10|20 (1)
    assert len(e_rows) == 3
    assert e_rows[2][0] == "total"
    assert e_rows[2][2] == 1

    # 6. Idempotency check: recompute again and verify no duplication
    recompute_rum_aggregates(con, "svc-rum-test")
    v_rows_after = con.execute(
        "SELECT dimension, value, event_count, good_count, poor_count, p75_value, bucket_start FROM rum_vitals_aggregates ORDER BY dimension, value, bucket_start"
    ).fetchall()
    assert v_rows_after == v_rows


def test_backfill_missing_rum_aggregates_heals_legacy_hour_without_total_beacons():
    """Hours aggregated before 3fcbcd27 have LCP/pageviews rows in rum_vitals_aggregates
    but lack a 'total_beacons' row. backfill_missing_rum_aggregates() must detect and
    recompute those hours across the 30-day lookback window."""
    from backend.core.rum_ingest import backfill_missing_rum_aggregates

    con = duckdb.connect(":memory:")
    con.execute("""
        CREATE TABLE client_vitals (
            timestamp TIMESTAMPTZ,
            metric_name VARCHAR,
            metric_value DOUBLE,
            metric_rating VARCHAR,
            pathname VARCHAR,
            browser VARCHAR,
            os VARCHAR,
            device VARCHAR,
            cid VARCHAR,
            req_id VARCHAR,
            city VARCHAR,
            region VARCHAR,
            country VARCHAR,
            pop VARCHAR,
            tls VARCHAR,
            ttfb DOUBLE
        )
    """)
    con.execute("""
        CREATE TABLE client_errors (
            timestamp TIMESTAMPTZ,
            error_message VARCHAR,
            error_file VARCHAR,
            error_line INT,
            error_col INT,
            pathname VARCHAR,
            browser VARCHAR,
            os VARCHAR,
            device VARCHAR,
            cid VARCHAR,
            req_id VARCHAR,
            city VARCHAR,
            region VARCHAR,
            country VARCHAR,
            pop VARCHAR,
            tls VARCHAR,
            ttfb DOUBLE
        )
    """)

    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    legacy_hour = now - timedelta(days=12)
    recent_hour = now - timedelta(hours=2)
    beyond_lookback_hour = now - timedelta(days=40)

    for hr, req_id in [
        (legacy_hour, "req-legacy-1"),
        (legacy_hour, "req-legacy-2"),
        (recent_hour, "req-recent-1"),
        (beyond_lookback_hour, "req-old-1"),
    ]:
        con.execute(
            "INSERT INTO client_vitals VALUES (?, 'LCP', 2.1, 'good', '/home', 'Chrome', 'macOS', 'desktop', 'u1', ?, 'SFO', 'CA', 'US', 'SFO', 'TLS1.3', 0.1)",
            [hr, req_id],
        )

    # Initial recompute populates all rows including total_beacons
    recompute_rum_aggregates(con, "svc-rum-test")

    # Simulate pre-3fcbcd27 state on legacy_hour and beyond_lookback_hour:
    # LCP and pageviews rows exist in rum_vitals_aggregates, but total_beacons is missing.
    con.execute(
        "DELETE FROM rum_vitals_aggregates WHERE service_id = 'svc-rum-test' AND bucket_start IN (?, ?) AND value = 'total_beacons'",
        [legacy_hour, beyond_lookback_hour],
    )

    # Confirm LCP aggregate row still exists for legacy_hour, while total_beacons does not
    lcp_cnt = con.execute(
        "SELECT event_count FROM rum_vitals_aggregates WHERE service_id = 'svc-rum-test' AND bucket_start = ? AND dimension = 'metric_name' AND value = 'LCP'",
        [legacy_hour],
    ).fetchone()
    assert lcp_cnt is not None and lcp_cnt[0] == 2
    tb_before = con.execute(
        "SELECT event_count FROM rum_vitals_aggregates WHERE service_id = 'svc-rum-test' AND bucket_start = ? AND dimension = 'total' AND value = 'total_beacons'",
        [legacy_hour],
    ).fetchone()
    assert tb_before is None

    # Run backfill with 30-day lookback: should detect and heal legacy_hour (12d ago),
    # skip recent_hour (already has total_beacons), and ignore beyond_lookback_hour (40d ago).
    healed = backfill_missing_rum_aggregates(con, "svc-rum-test", lookback_days=30)
    assert len(healed) == 1

    tb_after = con.execute(
        "SELECT event_count FROM rum_vitals_aggregates WHERE service_id = 'svc-rum-test' AND bucket_start = ? AND dimension = 'total' AND value = 'total_beacons'",
        [legacy_hour],
    ).fetchone()
    assert tb_after is not None and tb_after[0] == 2

    # Second run is a clean no-op
    healed_again = backfill_missing_rum_aggregates(con, "svc-rum-test", lookback_days=30)
    assert healed_again == []


def test_rollup_hour_heal_invokes_rum_total_beacons_backfill(monkeypatch, tmp_path):
    """_run_rollup_hour_heal triggers 30-day backfill_missing_rum_aggregates when RUM is enabled."""
    from unittest.mock import MagicMock

    import backend.core.iceberg._ducklake as _dl
    from backend.cron.jobs import compaction

    sid = "svc-rum-heal-cron"
    src = {
        "name": "rum-heal-svc",
        "service_id": sid,
        "access_level": "read_write",
        "cache_dir": str(tmp_path),
        "_cache_dir_override": str(tmp_path),
    }
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s: src)
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda *a, **k: False)
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda s: str(tmp_path))
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda *a, **k: 42)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **k: None)
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", lambda *a, **k: None)
    monkeypatch.setattr("backend.cron_progress.start_progress", lambda *a, **k: None)
    monkeypatch.setattr("backend.cron_progress.end_progress", lambda *a, **k: None)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", lambda *a, **k: None)
    monkeypatch.setattr("backend.cron.scheduler._log_and_add_progress", lambda *a, **k: None)
    monkeypatch.setattr("backend.cron.scheduler._check_disk_space", lambda *a, **k: (True, ""))
    monkeypatch.setattr(
        "backend.config.load_config",
        lambda s: {"provisioning": {"cron_sync": {}}, "rum": {"enabled": True}},
    )
    monkeypatch.setattr("backend.config.is_durable_serving_mode", lambda s: False)
    monkeypatch.setattr(
        "backend.core.rollups.backfill_missing_hour_bundles",
        lambda *a, **k: {"missing": 0, "rebuilt_fields": 0, "bundled": 0, "stamped_empty": 0},
    )
    monkeypatch.setattr("backend.core.rollups._common._hour_bundled_root", lambda s: str(tmp_path / "hours"))

    fake_con = MagicMock()

    monkeypatch.setattr("backend.core.duckdb.rum_source_for", lambda s: dict(s))
    monkeypatch.setattr("backend.core.duckdb.get_connection", lambda *a, **k: fake_con)
    monkeypatch.setattr(_dl, "_ducklake_attach", lambda *a, **k: None)
    monkeypatch.setattr(
        "backend.core.iceberg.execute_with_stale_view_retry",
        lambda con, s, fn, **kw: fn(con),
    )

    backfill_calls: list[tuple[object, str, int]] = []
    monkeypatch.setattr(
        "backend.core.rollups.rum.backfill_missing_rum_aggregates",
        lambda con, service_id, lookback_days=30: backfill_calls.append((con, service_id, lookback_days)) or [],
    )

    res = compaction._run_rollup_hour_heal.__wrapped__(sid, manual=True)
    assert res["status"] == "success"
    assert backfill_calls == [(fake_con, sid, 30)]


def _seeded_file_db(path: Path, hours: int = 4) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(path))
    con.execute(
        "CREATE TABLE client_vitals (timestamp TIMESTAMPTZ, metric_name VARCHAR, metric_value DOUBLE, "
        "metric_rating VARCHAR, pathname VARCHAR, browser VARCHAR, os VARCHAR, device VARCHAR, cid VARCHAR, "
        "req_id VARCHAR)"
    )
    con.execute(
        "CREATE TABLE client_errors (timestamp TIMESTAMPTZ, error_message VARCHAR, error_file VARCHAR, "
        "error_line INT, error_col INT, pathname VARCHAR, browser VARCHAR, os VARCHAR, device VARCHAR, "
        "cid VARCHAR, req_id VARCHAR)"
    )
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    for h in range(hours):
        for i in range(10):
            con.execute(
                "INSERT INTO client_vitals VALUES (?, 'LCP', 1000, 'good', '/p', 'c', 'o', 'd', ?, ?)",
                [now - timedelta(hours=h) + timedelta(seconds=i), f"c{i}", f"r{h}_{i}"],
            )
    return con


def _duplicate_keys(con: duckdb.DuckDBPyConnection) -> int:
    return con.execute(
        "SELECT COUNT(*) FROM (SELECT 1 FROM rum_vitals_aggregates GROUP BY service_id, bucket_start, dimension, value "
        "HAVING COUNT(*) > 1)"
    ).fetchone()[0]


def test_concurrent_recomputes_never_duplicate_aggregate_rows(tmp_path: Path):
    con = _seeded_file_db(tmp_path / "rum.duckdb")
    recompute_rum_aggregates(con, "svc")

    errors: list[BaseException] = []

    def run() -> None:
        try:
            recompute_rum_aggregates(con.cursor(), "svc")
        except BaseException as exc:  # noqa: BLE001 - surfaced by the assertion below
            errors.append(exc)

    for _ in range(10):
        threads = [threading.Thread(target=run) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    assert errors == []
    assert _duplicate_keys(con) == 0


def test_heal_rebuilds_hours_whose_total_beacons_row_is_duplicated(tmp_path: Path):
    con = _seeded_file_db(tmp_path / "rum.duckdb")
    recompute_rum_aggregates(con, "svc")
    con.execute("INSERT INTO rum_vitals_aggregates SELECT * FROM rum_vitals_aggregates WHERE value = 'total_beacons'")
    assert _duplicate_keys(con) > 0

    healed = backfill_missing_rum_aggregates(con, "svc", lookback_days=30)

    assert healed
    assert _duplicate_keys(con) == 0
    assert (
        con.execute("SELECT SUM(event_count) FROM rum_vitals_aggregates WHERE value = 'total_beacons'").fetchone()[0]
        == 40
    )


# ── Merged from tests/core/test_rum_rollups.py ──
def test_get_beacon_health_and_header_stats(in_memory_duckdb):
    con = in_memory_duckdb
    con.execute(
        "CREATE TABLE client_vitals ("
        "timestamp TIMESTAMPTZ, metric_name VARCHAR, metric_value DOUBLE, "
        "metric_rating VARCHAR, pathname VARCHAR, cid VARCHAR, req_id VARCHAR)"
    )
    con.execute(
        "CREATE TABLE client_errors ("
        "timestamp TIMESTAMPTZ, error_message VARCHAR, pathname VARCHAR, cid VARCHAR, req_id VARCHAR)"
    )
    now = datetime.now(UTC)
    con.execute(
        "INSERT INTO client_vitals (timestamp, metric_name, metric_value, cid, req_id) VALUES (?, 'LCP', 2.0, 'c1', 'r1')",
        [now],
    )
    recent_count, last_dt = rum_repo.get_beacon_health(con)
    assert recent_count == 1
    assert last_dt is not None

    distinct_count, newest_dt = rum_repo.get_header_stats(con)
    assert distinct_count == 1
    assert newest_dt is not None

    assert rum_repo.get_client_vitals_columns(con) == [
        "timestamp",
        "metric_name",
        "metric_value",
        "metric_rating",
        "pathname",
        "cid",
        "req_id",
    ]


# ── Merged from tests/core/test_rollup_writers_golden.py ──
GOLDEN_PATH = Path(__file__).parent / "golden" / "rollup_writers.json"

SERVICE = "svc_golden"

TABLE = "logs_svc_golden"

_LOG_TABLE_SQL = f"""
CREATE TABLE {TABLE} AS
SELECT
  TIMESTAMPTZ '2026-06-13 00:00:00+00' + to_minutes(i::INTEGER) AS timestamp,
  '10.0.' || (i % 5) || '.' || (i % 7) AS ip,
  (['Mozilla/5.0', 'curl/7.68', 'Googlebot/2.1', 'python-requests'])[(i % 4) + 1] AS ua,
  '/p' || (i % 6) AS url,
  (['US', 'JP', 'DE'])[(i % 3) + 1] AS country,
  (['NYC', 'TYO', 'BER'])[(i % 3) + 1] AS city,
  (['NY', 'TK', 'BE'])[(i % 3) + 1] AS region,
  (['501', '392', '999'])[(i % 3) + 1] AS metro,
  40.0 + (i % 3) AS lat,
  -70.0 + (i % 3) AS lon,
  CAST(100 + (i % 3) AS BIGINT) AS asn,
  (['DEN', 'IAD', 'LAX'])[((i // 3) % 3) + 1] AS pop,
  (CASE i % 5 WHEN 0 THEN 200 WHEN 1 THEN 200 WHEN 2 THEN 404 WHEN 3 THEN 500 ELSE 304 END)::INTEGER AS status,
  (['HIT', 'MISS', 'PASS', 'HIT-STALE', 'MISS'])[(i % 5) + 1] AS cache,
  (['GET', 'POST'])[(i % 2) + 1] AS method,
  0.01 + (i % 10) * 0.01 AS ttfb,
  CAST(50000 + (i % 20) * 1000 AS DOUBLE) AS ottfb,
  CAST(80000 + (i % 25) * 1000 AS DOUBLE) AS ottlb,
  CAST(1000 + (i % 40) AS BIGINT) AS obytes,
  CAST(100 + (i % 30) AS DOUBLE) AS elapsed,
  CAST(10000 + (i % 50) * 100 AS BIGINT) AS tcp_rtt,
  CAST(5000 + (i % 7) * 100 AS BIGINT) AS rtt_min,
  CAST(100 + (i % 9) AS BIGINT) AS rtt_var,
  (i % 10) / 100.0 AS ploss,
  CAST(500 + (i % 100) AS BIGINT) AS req_bytes,
  CAST(1000 + (i % 100) AS BIGINT) AS resp_bytes,
  CAST(200 + (i % 30) AS BIGINT) AS req_header_bytes,
  CAST(1 + (i % 25) AS BIGINT) AS conn_requests,
  (['slow', 'med', 'fast'])[(i % 3) + 1] AS c_speed,
  (i % 4) <> 0 AS edge,
  '203.0.113.' || (i % 4) AS oip,
  (CASE i % 3 WHEN 0 THEN 200 WHEN 1 THEN 200 ELSE 500 END)::INTEGER AS ost,
  'ja4_' || (i % 5) AS ja4,
  CASE WHEN i % 7 = 0 THEN NULL ELSE 'sha' || (i % 3) END AS tls_ciphers_sha,
  CAST((i % 5) * 60 AS BIGINT) AS ttl,
  CASE WHEN i % 9 = 0 THEN 'VERIFIED-BOT.google,WAF.x' ELSE NULL END AS waf_sig,
  (i % 11 = 0) AS waf,
  (i % 13 = 0) AS waf_resp,
  (i % 2 = 0) AS is_shield,
  (i % 3 <> 0) AS is_ssl,
  (i % 8 = 0) AS ipv6,
  'HTTP/2' AS protocol,
  'sid' || (i % 12) AS edge_sid,
  'cmcd' || (i % 6) AS cmcd_sid,
  'req' || i AS waf_req_id
FROM range(0, 2880) AS t(i)
"""

_TMP_RE = re.compile(r"(\.tmp_[a-z0-9_]*?)[0-9a-f]{12}(\.parquet)")

_RANDOM_NAME_RE = re.compile(r"(compacted_|part-|data_)[0-9a-f]{12,32}")

_TMP_DIR_RE = re.compile(r"(rollups/tmp/[A-Za-z0-9_]+?)_[0-9a-f]{8}\b")

_PATHS_RE = re.compile(r"read_parquet\(\[([^\]]*)\]")


class _Recorder:
    """Forwards to a DuckDB connection while recording every statement."""

    def __init__(self, con: duckdb.DuckDBPyConnection, log: list[str], close: bool) -> None:
        self._con = con
        self._log = log
        self._close = close

    def execute(self, query: str, *args: Any, **kwargs: Any) -> Any:
        self._log.append(query)
        return self._con.execute(query, *args, **kwargs)

    def close(self) -> None:
        if self._close:
            self._con.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._con, name)


def _normalise_sql(q: str, root: str) -> str:
    q = q.replace(root, "<R>")
    q = _TMP_RE.sub(r"\1<id>\2", q)
    q = _RANDOM_NAME_RE.sub(r"\1<id>", q)
    q = _TMP_DIR_RE.sub(r"\1_<id>", q)

    def _sorted_paths(m: re.Match) -> str:
        parts = sorted(p.strip() for p in m.group(1).split(","))
        return "read_parquet([" + ", ".join(parts) + "]"

    q = re.sub(r"\s+", " ", _PATHS_RE.sub(_sorted_paths, q)).strip()
    # Whitespace next to parentheses is cosmetic too (a multi-line COPY vs the shared one-line helper).
    return re.sub(r" ?([()]) ?", r"\1", q)


_SHAPE_RE = re.compile(r"\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[+-]\d{2}:\d{2})?|-\d{2})?")


def _digest(text: str) -> str:
    return hashlib.sha1(text.encode()).hexdigest()[:12]


def _shape(q: str) -> str:
    """The statement with date/hour literals blanked, for readable diagnostics."""
    return _SHAPE_RE.sub("<date>", q)


SCHEMAS: dict[str, list[str]] = {}


def _canon(v: Any) -> Any:
    if isinstance(v, float):
        return round(v, 4)
    if isinstance(v, bytes):
        return v.hex()
    if isinstance(v, list):
        return [_canon(x) for x in v]
    return v


def _snapshot_tree(
    root: Path,
    file_cache: dict[Path, tuple[int, int, dict[str, Any], list[str]]] | None = None,
) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for p in sorted(root.rglob("*")):
        if not p.is_file():
            continue
        rel = _RANDOM_NAME_RE.sub(r"\1<id>", p.relative_to(root).as_posix())
        if rel.startswith("_scratch"):
            continue
        st = p.stat()
        if p.suffix != ".parquet":
            out[rel] = {"bytes": st.st_size}
            continue
        cached = file_cache.get(p) if file_cache is not None else None
        if cached is not None and cached[0] == st.st_mtime_ns and cached[1] == st.st_size:
            entry, schema = cached[2], cached[3]
            SCHEMAS[entry["schema"]] = schema
            out[rel] = entry
            continue
        table = pq.ParquetFile(p).read()
        rows = sorted(json.dumps(_canon(list(r.values())), default=str) for r in table.to_pylist())
        schema = [f"{f.name}:{f.type}" for f in table.schema]
        schema_digest = _digest(json.dumps(schema))
        SCHEMAS[schema_digest] = schema
        entry = {
            "schema": schema_digest,
            "rows": len(rows),
            "sha": _digest("\n".join(rows)),
        }
        if file_cache is not None:
            file_cache[p] = (st.st_mtime_ns, st.st_size, entry, schema)
        out[rel] = entry
    return out


def _writers() -> list[tuple[str, Callable[[str, dict, list[str]], int]]]:
    from backend.core.rollups import (
        network_health_geo,
        network_health_heatmap,
        network_quality,
        network_rtt,
        network_speed,
        network_summary,
        ngwaf_bots,
        origin_dims,
        origin_latency_ts,
        origin_summary,
        overview,
        perf_dims,
        perf_latency,
        pop_health,
        security_dims,
        sessions,
        slow_urls,
        time_series,
        verified_bots_ts,
    )

    return [
        ("time_series", time_series.build_time_series_bundles),
        ("sessions", sessions.build_session_bundles),
        ("slow_urls", slow_urls.build_slow_urls_bundles),
        ("origin_summary", origin_summary.build_origin_summary_bundles),
        ("origin_dims", origin_dims.build_origin_dims_bundles),
        ("origin_latency_ts", origin_latency_ts.build_origin_latency_ts_bundles),
        ("network_rtt", network_rtt.build_network_rtt_bundles),
        ("network_speed", network_speed.build_network_speed_bundles),
        ("network_quality", network_quality.build_network_quality_bundles),
        ("network_summary", network_summary.build_network_summary_bundles),
        ("network_heatmap", network_health_heatmap.build_network_heatmap_bundles),
        ("network_geo", network_health_geo.build_network_geo_bundles),
        ("perf_dims", perf_dims.build_perf_dims_bundles),
        ("perf_latency", perf_latency.build_perf_latency_bundles),
        ("security_dims", security_dims.build_security_dims_bundles),
        ("overview", overview.build_overview_bundles),
        ("pop_health", pop_health.build_pop_health_bundles),
        ("verified_bots_ts", verified_bots_ts.build_verified_bots_ts_bundles),
        ("ngwaf_bots", ngwaf_bots.build_ngwaf_bots_bundles),
        ("ngwaf_bots_ts", ngwaf_bots.build_ngwaf_bots_ts_bundles),
    ]


def _compactors() -> list[tuple[str, Callable[..., int]]]:
    from backend.core.rollups import day_bundles as d

    return [
        ("origin_summary", d.compact_origin_summary_closed_days_to_daily),
        ("network_rtt", d.compact_network_rtt_closed_days_to_daily),
        ("network_speed", d.compact_network_speed_closed_days_to_daily),
        ("ngwaf_bots", d.compact_ngwaf_bots_closed_days_to_daily),
        ("ngwaf_bots_ts", d.compact_ngwaf_bots_ts_closed_days_to_daily),
        ("verified_bots_ts", d.compact_verified_bots_ts_closed_days_to_daily),
        ("origin_latency_ts", d.compact_origin_latency_ts_closed_days_to_daily),
        ("perf_latency", d.compact_perf_latency_closed_days_to_daily),
        ("origin_dims", d.compact_origin_dims_closed_days_to_daily),
        ("security_dims", d.compact_security_dims_closed_days_to_daily),
        ("perf_dims", d.compact_perf_dims_closed_days_to_daily),
        ("overview", d.compact_overview_closed_days_to_daily),
        ("pop_health", d.compact_pop_health_closed_days_to_daily),
        ("network_quality", d.compact_network_quality_closed_days_to_daily),
    ]


_FIELDS = ["country", "url", "status", "pop", "ua", "cache"]

_BOT_PATTERN = "Googlebot|curl"

_BOT_VERSION = "v-golden"


def _closed_hours() -> list[str]:
    return [f"2026-06-{d}-{h:02d}" for d in (13, 14) for h in range(24)]


def _write_ngwaf_cache(path: Path) -> None:
    """The SQLite ``ngwaf_bots`` cache the writer joins ``waf_req_id`` against."""
    import sqlite3

    con = sqlite3.connect(path)
    con.execute("CREATE TABLE ngwaf_bots (waf_req_id TEXT, bot_name TEXT, category TEXT)")
    rows = [(f"req{i}", f"bot{i % 3}", None if i % 5 == 0 else f"cat{i % 2}") for i in range(0, 2880, 7)]
    rows.append(("req1", None, "untagged"))
    con.executemany("INSERT INTO ngwaf_bots VALUES (?, ?, ?)", rows)
    con.commit()
    con.close()


def _write_data_partitions(shared: duckdb.DuckDBPyConnection, cache_root: Path, hours: list[str]) -> None:
    """Committed raw partitions (``data/timestamp_hour=H``) the wellknown-bots writer reads."""
    for h in hours:
        d = cache_root / "data" / f"timestamp_hour={h}"
        d.mkdir(parents=True)
        start = datetime.strptime(h, "%Y-%m-%d-%H").replace(tzinfo=UTC)
        shared.execute(
            f"COPY (SELECT * FROM {TABLE} WHERE timestamp >= TIMESTAMPTZ '{start.isoformat()}' "
            f"AND timestamp < TIMESTAMPTZ '{(start + timedelta(hours=1)).isoformat()}') "
            f"TO '{d / 'part-0.parquet'}' (FORMAT PARQUET)"
        )


def _plant_stale_wellknown(shared: duckdb.DuckDBPyConnection, cache_root: Path) -> None:
    """One data-backed hour stamped with an old version, one orphaned stale hour with no raw data."""
    root = cache_root / "rollups" / "wellknown_bots"
    for hour, wipe in (("2026-06-13-05", True), ("2026-06-01-05", False)):
        d = root / f"hour={hour}"
        d.mkdir(parents=True, exist_ok=True)
        if wipe:
            for f in d.glob("*.parquet"):
                f.unlink()
        shared.execute(
            "COPY (SELECT 'curl/7.68' AS ua, '10.0.0.1' AS ip, 3::BIGINT AS request_count, "
            f"'old-version' AS pattern_set_version) TO '{d / 'compacted_aaaaaaaaaaaa.parquet'}' (FORMAT PARQUET)"
        )


def _run_pipeline(tmp_path: Path) -> tuple[dict[str, Any], dict[str, str]]:
    from backend.core import rollups
    from backend.core.rollups import day_bundles, hour_bundles, wellknown_bots

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    root = str(cache_root)
    log: list[str] = []

    shared = duckdb.connect(":memory:")
    shared.execute("PRAGMA threads=1")
    shared.execute("SET TimeZone='UTC'")
    shared.execute(_LOG_TABLE_SQL)
    mem_shared = duckdb.connect(":memory:")
    mem_shared.execute("PRAGMA threads=1")
    mem_shared.execute("SET TimeZone='UTC'")
    ngwaf_db = tmp_path / "ngwaf_bot_cache.db"
    _write_ngwaf_cache(ngwaf_db)

    def _writer_con(*_a: Any, **_k: Any) -> _Recorder:
        cur = shared.cursor()
        cur.execute("SET TimeZone='UTC'")
        return _Recorder(cur, log, close=True)

    def _memory_con(*_a: Any, **_k: Any) -> _Recorder:
        cur = mem_shared.cursor()
        cur.execute("SET TimeZone='UTC'")
        return _Recorder(cur, log, close=True)

    source = {"name": SERVICE, "service_id": SERVICE}
    hours = _closed_hours()
    phases: dict[str, Any] = {}
    texts: dict[str, str] = {}
    seen: dict[str, dict] = {}
    file_cache: dict[Path, tuple[int, int, dict[str, Any], list[str]]] = {}

    def phase(name: str, fn: Callable[[], Any]) -> None:
        log.clear()
        returned = fn()
        tree = _snapshot_tree(cache_root, file_cache)
        delta = {k: v for k, v in tree.items() if seen.get(k) != v}
        removed = sorted(k for k in seen if k not in tree)
        seen.clear()
        seen.update(tree)
        normalised = [_normalise_sql(q, root) for q in log]
        texts.update({_digest(q): _shape(q) for q in normalised})
        phases[name] = {
            "returned": returned,
            "changed": delta,
            "removed": removed,
            "sql": sorted(_digest(q) for q in normalised),
            "sql_count": len(log),
        }

    with (
        patch("backend.core.duckdb._cache_dir", return_value=root),
        patch("backend.core.duckdb.get_connection", side_effect=_writer_con),
        patch("backend.core.duckdb.get_memory_connection", side_effect=_memory_con),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
        patch("backend.core.iceberg.execute_with_stale_view_retry", side_effect=lambda c, _s, fn: fn(c)),
        patch("backend.config.ngwaf_db_path", return_value=str(ngwaf_db)),
        patch("backend.utils.bot_sources.get_pattern_set_version", return_value=_BOT_VERSION),
        patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value=_BOT_PATTERN),
    ):
        phase(
            "per_field_copy",
            lambda: (
                rollups._run_per_field_copy(SERVICE, source, rollups._safe_table_for(source), "1=1", _FIELDS),
                rollups.recompute._run_ip_spread_per_field(
                    SERVICE, source, rollups._safe_table_for(source), "1=1", _FIELDS
                ),
            )[1],
        )
        phase("bundle_hours", lambda: hour_bundles.bundle_hours(SERVICE, source, hours))
        phase("compact_closed_days", lambda: rollups.compact_closed_days_to_daily(SERVICE, source))
        phase("bundle_days", lambda: day_bundles.bundle_days(SERVICE, source, ["2026-06-13", "2026-06-14"]))
        for name, writer in _writers():
            phase(f"hour_writer:{name}", lambda w=writer: w(SERVICE, source, hours))
        for name, compactor in _compactors():
            phase(f"day_compactor:{name}", lambda c=compactor: c(SERVICE, source))
        _write_data_partitions(shared, cache_root, hours)
        seen.update(_snapshot_tree(cache_root, file_cache))
        phase(
            "wellknown_bots:recompute", lambda: wellknown_bots.recompute_wellknown_bots_rollup(SERVICE, source, hours)
        )
        _plant_stale_wellknown(shared, cache_root)
        seen.update(_snapshot_tree(cache_root, file_cache))
        phase("wellknown_bots:backfill", lambda: wellknown_bots.backfill_wellknown_bots_rollup(SERVICE, source))
    mem_shared.close()
    shared.close()
    return phases, texts


@pytest.mark.usefixtures("frozen_clock")
@pytest.mark.timeout(600)
def test_writers_match_golden(tmp_path: Path) -> None:
    phases, texts = _run_pipeline(tmp_path)
    got = json.loads(json.dumps(phases, sort_keys=True))

    if os.environ.get("UPDATE_ROLLUP_GOLDEN"):
        GOLDEN_PATH.parent.mkdir(parents=True, exist_ok=True)
        GOLDEN_PATH.write_text(
            json.dumps(
                {"phases": got, "schemas": SCHEMAS, "sql_shapes": sorted(set(texts.values()))}, indent=1, sort_keys=True
            )
            + "\n"
        )
        return

    golden = json.loads(GOLDEN_PATH.read_text())
    want = golden["phases"]
    assert got.keys() == want.keys()
    for name in want:
        if got[name] != want[name]:
            import difflib

            new = [h for h in got[name]["sql"] if h not in want[name]["sql"]]
            detail = "\n".join(f"new statement: {texts.get(h)}" for h in new[:3])
            lines = difflib.unified_diff(
                json.dumps(want[name], indent=1, sort_keys=True).splitlines(),
                json.dumps(got[name], indent=1, sort_keys=True).splitlines(),
                "golden",
                "got",
                lineterm="",
                n=2,
            )
            diff = "\n".join(list(lines)[:60])
            raise AssertionError(f"phase {name!r} drifted from golden\n{detail}\n{diff}")
