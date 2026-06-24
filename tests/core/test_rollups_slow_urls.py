"""Tests for the per-hour slow_urls bundle writer + its backfill driver.

Mirrors test_rollups_time_series.py in structure — same patch pattern,
same _noop_lock + _past_hour helpers — so the two writers stay legible
side-by-side.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import duckdb
import pyarrow.parquet as pq


def _seed_logs(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict]) -> None:
    """Create ``table`` with the column set the slow_urls writer reads
    and INSERT rows."""
    con.execute(f"CREATE TABLE {table} (timestamp TIMESTAMPTZ, url VARCHAR, ottfb DOUBLE, ttfb DOUBLE, cache VARCHAR)")
    for r in rows:
        con.execute(
            f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?)",
            [r["timestamp"], r.get("url"), r.get("ottfb"), r.get("ttfb"), r.get("cache")],
        )


@contextmanager
def _noop_lock(_key):
    yield


def _past_hour(hours_ago: int) -> tuple[str, datetime]:
    dt = (datetime.now(UTC) - timedelta(hours=hours_ago)).replace(minute=0, second=0, microsecond=0)
    return dt.strftime("%Y-%m-%d-%H"), dt


def test_build_slow_urls_writes_top_k_per_hour(tmp_path):
    """Happy path: closed hour with rows for several URLs produces one
    row per URL with the documented schema, ranked by p95 DESC."""
    from backend.core.rollups import slow_urls

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-su"}
    hour_token, hour_dt = _past_hour(2)

    # Six rows for URL "/slow" (above min_requests=5) with high latency
    # and six rows for URL "/fast" with low latency. Below min_requests
    # the rollup drops the row, so add five for "/edge".
    rows = []
    for i in range(6):
        rows.append(
            {
                "timestamp": hour_dt + timedelta(minutes=i),
                "url": "/slow",
                "ottfb": 500_000.0 + i * 1000,  # ~500ms in us
                "ttfb": 0.5,
                "cache": "MISS",
            }
        )
    for i in range(6):
        rows.append(
            {
                "timestamp": hour_dt + timedelta(minutes=i),
                "url": "/fast",
                "ottfb": 20_000.0 + i * 100,  # ~20ms
                "ttfb": 0.02,
                "cache": "MISS",
            }
        )
    # /edge has only 4 rows — should be filtered out by HAVING
    for i in range(4):
        rows.append(
            {
                "timestamp": hour_dt + timedelta(minutes=i),
                "url": "/edge",
                "ottfb": 100_000.0,
                "ttfb": 0.1,
                "cache": "MISS",
            }
        )

    con = duckdb.connect(":memory:")
    _seed_logs(con, "logs_su", rows)

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value="logs_su"),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
        patch(
            "backend.core.iceberg.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        n = slow_urls.build_slow_urls_bundles("svc-su", src, [hour_token])

    assert n == 1
    bundle = cache_root / "rollups" / "hour_bundled" / f"hour={hour_token}" / "slow_urls.parquet"
    assert bundle.exists()

    t = pq.read_table(str(bundle))
    cols = set(t.column_names)
    # Required columns — match time_series test's issubset posture so a
    # future column add doesn't break the test.
    assert {"url", "requests", "lat_us_count", "lat_us_sum", "p50_us", "p95_us", "p99_us"}.issubset(cols), (
        f"missing required columns; got: {cols}"
    )

    urls_present = set(t.column("url").to_pylist())
    # /edge had 4 rows — below min_requests=5 — and should be dropped
    assert "/slow" in urls_present
    assert "/fast" in urls_present
    assert "/edge" not in urls_present
    assert len(urls_present) == 2

    # /slow should rank higher by p95
    by_url = {r["url"]: r for r in t.to_pylist()}
    assert by_url["/slow"]["requests"] == 6
    assert by_url["/slow"]["p95_us"] > by_url["/fast"]["p95_us"]


def test_build_slow_urls_skips_active_hour(tmp_path):
    """The active UTC hour is still being written; the writer must skip it
    (live SQL serves the active hour). Same convention as time_series."""
    from backend.core.rollups import slow_urls

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-su"}
    active_token = datetime.now(UTC).strftime("%Y-%m-%d-%H")

    con = duckdb.connect(":memory:")
    _seed_logs(con, "logs_su", [])

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value="logs_su"),
        patch("backend.core.duckdb.get_connection", return_value=con),
    ):
        n = slow_urls.build_slow_urls_bundles("svc-su", src, [active_token])

    assert n == 0
    assert not (cache_root / "rollups" / "hour_bundled" / f"hour={active_token}" / "slow_urls.parquet").exists()


def test_build_slow_urls_no_url_column_skips(tmp_path):
    """Services whose schema lacks `url` have no panel input — writer
    must return 0 rather than fail or write an empty file."""
    from backend.core.rollups import slow_urls

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-su"}
    hour_token, _ = _past_hour(2)

    # Table with no url column
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE logs_nourl (timestamp TIMESTAMPTZ, ottfb DOUBLE)")

    with (
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)),
        patch("backend.core.rollups._common._safe_table_for", return_value="logs_nourl"),
        patch("backend.core.duckdb.get_connection", return_value=con),
        patch(
            "backend.core.iceberg.execute_with_stale_view_retry",
            side_effect=lambda c, _src, fn: fn(c),
        ),
    ):
        n = slow_urls.build_slow_urls_bundles("svc-su", src, [hour_token])

    assert n == 0


def test_backfill_slow_urls_walks_existing_bundle_hours(tmp_path):
    """The self-heal driver should pick up closed hours that already have
    all_fields.parquet but no slow_urls.parquet, and build the missing
    bundle for each."""
    from backend.core.rollups import slow_urls

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-su"}

    # Seed two closed hours with all_fields.parquet — only one of them
    # already has slow_urls.parquet, so backfill should hit the other.
    h1_token, h1_dt = _past_hour(3)
    h2_token, h2_dt = _past_hour(4)
    bundled_root = cache_root / "rollups" / "hour_bundled"
    (bundled_root / f"hour={h1_token}").mkdir(parents=True)
    (bundled_root / f"hour={h1_token}" / "all_fields.parquet").write_bytes(b"x")  # marker only
    (bundled_root / f"hour={h2_token}").mkdir(parents=True)
    (bundled_root / f"hour={h2_token}" / "all_fields.parquet").write_bytes(b"x")
    (bundled_root / f"hour={h2_token}" / "slow_urls.parquet").write_bytes(b"x")  # already-built

    # Logs covering h1 only — slow_urls should be writeable for h1.
    rows = [
        {"timestamp": h1_dt + timedelta(minutes=i), "url": "/u", "ottfb": 100_000.0, "ttfb": 0.1, "cache": "MISS"}
        for i in range(6)
    ]
    con = duckdb.connect(":memory:")
    _seed_logs(con, "logs_su", rows)

    written_hours: list[list[str]] = []

    def _spy_build(_sid, _src, hours):
        written_hours.append(list(hours))
        # Don't actually write — only verify backfill driver picked
        # the right hour.
        return len(hours)

    with patch("backend.core.rollups.slow_urls.build_slow_urls_bundles", side_effect=_spy_build):
        with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
            n = slow_urls.backfill_slow_urls_bundles("svc-su", src)

    assert n == 1
    assert written_hours == [[h1_token]]


def test_try_slow_urls_from_rollup_eligibility_gates(tmp_path):
    """Reader returns None for: filters present, window too short, missing
    closed-hour rollup file. Confirms the explicit gates documented in
    the docstring."""
    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-su"}

    con = duckdb.connect(":memory:")
    runner = QueryRunner(con, src)

    # has_filters → None
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        assert (
            runner.try_slow_urls_from_rollup(
                "2026-06-01T00:00:00Z",
                "2026-06-15T00:00:00Z",
                has_filters=True,
                min_requests=10,
                limit=50,
            )
            is None
        )

        # Window < 48h → None
        assert (
            runner.try_slow_urls_from_rollup(
                "2026-06-01T00:00:00Z",
                "2026-06-02T12:00:00Z",
                has_filters=False,
                min_requests=10,
                limit=50,
            )
            is None
        )

        # Empty bundle dir → None (the walk finds the first missing
        # closed hour and falls back).
        assert (
            runner.try_slow_urls_from_rollup(
                "2026-06-01T00:00:00Z",
                "2026-06-15T00:00:00Z",
                has_filters=False,
                min_requests=10,
                limit=50,
            )
            is None
        )

        # Malformed timestamps → None
        assert (
            runner.try_slow_urls_from_rollup(
                "not-a-date",
                "2026-06-15T00:00:00Z",
                has_filters=False,
                min_requests=10,
                limit=50,
            )
            is None
        )


def test_try_slow_urls_from_rollup_returns_request_weighted_p95(tmp_path):
    """End-to-end: build a 50-hour window of slow_urls files (above the
    48-hour MIN_HOURS gate), then verify the reader returns request-
    weighted p95 values and the _approx flag."""
    import os

    from backend.repositories._base import QueryRunner

    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    src = {"name": "svc-su"}

    bundled_root = str(cache_root / "rollups" / "hour_bundled")

    # Seed 50 consecutive closed hours starting 100h ago (well above the
    # 48h MIN_HOURS gate and clear of the active hour).
    base_dt = (datetime.now(UTC) - timedelta(hours=100)).replace(minute=0, second=0, microsecond=0)
    hours = [base_dt + timedelta(hours=i) for i in range(50)]
    for dt in hours:
        hour_dir = f"{bundled_root}/hour={dt.strftime('%Y-%m-%d-%H')}"
        os.makedirs(hour_dir, exist_ok=True)
        # Write a minimal slow_urls parquet:
        # /a: requests=100, p95=1000us
        # /b: requests=10,  p95=10000us
        sql_path = f"{hour_dir}/slow_urls.parquet"
        wcon = duckdb.connect()
        try:
            wcon.execute(
                f"COPY (SELECT * FROM (VALUES "
                f"('/a', CAST(100 AS BIGINT), CAST(100 AS BIGINT), 100000.0, 500.0, 1000.0, 1500.0), "
                f"('/b', CAST(10 AS BIGINT), CAST(10 AS BIGINT), 100000.0, 5000.0, 10000.0, 15000.0)"
                f") AS t(url, requests, lat_us_count, lat_us_sum, p50_us, p95_us, p99_us)) "
                f"TO '{sql_path}' (FORMAT PARQUET)"
            )
        finally:
            wcon.close()

    con = duckdb.connect(":memory:")
    runner = QueryRunner(con, src)

    # Use start = first hour, end = last hour + 1h
    st_iso = hours[0].isoformat().replace("+00:00", "Z")
    et_iso = (hours[-1] + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
    with patch("backend.core.duckdb._cache_dir", return_value=str(cache_root)):
        result = runner.try_slow_urls_from_rollup(
            st_iso,
            et_iso,
            has_filters=False,
            min_requests=1,
            limit=10,
        )

    assert result is not None, "reader returned None — eligibility gate fired unexpectedly"
    assert result["_approx"] is True
    assert result["has_data"] is True
    by_url = {r["url"]: r for r in result["rows"]}
    # Each hour contributes identical per-row p95 values, so the
    # request-weighted average across 50 hours == per-hour p95.
    # /a p95: 1000us = 1.0ms; /b p95: 10000us = 10.0ms
    assert abs(by_url["/b"]["p95_ms"] - 10.0) < 1e-6
    assert abs(by_url["/a"]["p95_ms"] - 1.0) < 1e-6
    # /b ranks first by p95
    assert result["rows"][0]["url"] == "/b"
    # Request totals are summed across the 50 hours
    assert by_url["/a"]["requests"] == 100 * 50
    assert by_url["/b"]["requests"] == 10 * 50
