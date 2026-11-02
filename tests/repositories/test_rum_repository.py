"""Tests for Standard-mode ``backend.repositories.rum`` against DuckDB."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from backend.repositories import rum as rum_repo


def _create_raw_rum_tables(con) -> None:
    con.execute(
        """
        CREATE TABLE client_vitals (
            timestamp TIMESTAMPTZ,
            req_id VARCHAR,
            cid VARCHAR,
            metric_name VARCHAR,
            metric_value DOUBLE,
            metric_rating VARCHAR,
            browser VARCHAR,
            os VARCHAR,
            device VARCHAR,
            pathname VARCHAR,
            city VARCHAR,
            region VARCHAR,
            country VARCHAR,
            pop VARCHAR,
            tls VARCHAR,
            ttfb DOUBLE
        )
        """
    )
    con.execute(
        """
        CREATE TABLE client_errors (
            timestamp TIMESTAMPTZ,
            req_id VARCHAR,
            cid VARCHAR,
            pathname VARCHAR,
            error_message VARCHAR,
            error_file VARCHAR,
            error_line BIGINT,
            error_col BIGINT,
            browser VARCHAR,
            os VARCHAR,
            device VARCHAR,
            city VARCHAR,
            region VARCHAR,
            country VARCHAR,
            pop VARCHAR,
            tls VARCHAR,
            ttfb DOUBLE
        )
        """
    )


def _create_rollup_rum_tables(con) -> None:
    con.execute(
        """
        CREATE TABLE rum_vitals_aggregates (
            service_id VARCHAR,
            bucket_start TIMESTAMPTZ,
            dimension VARCHAR,
            value VARCHAR,
            event_count BIGINT,
            value_sum DOUBLE,
            good_count BIGINT,
            ni_count BIGINT,
            poor_count BIGINT,
            p50_value DOUBLE,
            p75_value DOUBLE,
            p99_value DOUBLE
        )
        """
    )
    con.execute(
        """
        CREATE TABLE rum_error_aggregates (
            service_id VARCHAR,
            bucket_start TIMESTAMPTZ,
            dimension VARCHAR,
            value VARCHAR,
            error_count BIGINT
        )
        """
    )


def test_get_beacon_health_and_header_stats_and_schema(in_memory_duckdb) -> None:
    con = in_memory_duckdb
    assert rum_repo.get_client_vitals_columns(con) is None

    _create_raw_rum_tables(con)
    cols = rum_repo.get_client_vitals_columns(con)
    assert cols is not None
    assert "metric_name" in cols
    assert "pathname" in cols

    now = datetime.now(UTC).replace(microsecond=0)
    old_ts = now - timedelta(hours=3)
    recent_vital_ts = now - timedelta(minutes=20)
    recent_err_ts = now - timedelta(minutes=5)

    con.execute(
        "INSERT INTO client_vitals (timestamp, req_id, cid, metric_name, metric_value, metric_rating, pathname) "
        "VALUES (?, 'r-old', 'c1', 'LCP', 1200.0, 'good', '/home'), "
        "(?, 'r-recent', 'c2', 'LCP', 1800.0, 'good', '/home')",
        [old_ts, recent_vital_ts],
    )
    con.execute(
        "INSERT INTO client_errors (timestamp, req_id, cid, pathname, error_message, error_file, error_line, error_col) "
        "VALUES (?, 'r-err', 'c3', '/checkout', 'ReferenceError: x', 'app.js', 10, 2)",
        [recent_err_ts],
    )

    recent_count, newest_dt = rum_repo.get_beacon_health(con)
    assert recent_count == 2
    assert newest_dt == recent_err_ts

    distinct_beacons, header_newest = rum_repo.get_header_stats(con)
    assert distinct_beacons == 3
    assert header_newest == recent_err_ts


def test_fetch_rum_analytics_raw_scan_and_empty_state(in_memory_duckdb) -> None:
    con = in_memory_duckdb
    _create_raw_rum_tables(con)

    base = datetime(2026, 8, 5, 10, 15, 0, tzinfo=UTC)
    start_iso = (base - timedelta(hours=1)).isoformat()
    end_iso = (base + timedelta(hours=1)).isoformat()

    empty = rum_repo.fetch_rum_analytics(
        con,
        service_id="svc-std",
        start_time=start_iso,
        end_time=end_iso,
        parsed_filters={},
        where_sql="timestamp >= ? AND timestamp <= ?",
        params=[start_iso, end_iso],
        raw_window_max_s=86400,
    )
    assert empty == {"no_data": True}

    con.execute(
        """
        INSERT INTO client_vitals (
            timestamp, req_id, cid, metric_name, metric_value, metric_rating,
            browser, os, device, pathname
        ) VALUES
            (?, 'req-1', 'c1', 'LCP', 1500.0, 'good', 'Chrome', 'macOS', 'Desktop', '/'),
            (?, 'req-2', 'c2', 'LCP', 4200.0, 'poor', 'Safari', 'iOS', 'Mobile', '/checkout'),
            (?, 'req-3', 'c3', 'CLS', 0.05, 'good', 'Chrome', 'macOS', 'Desktop', '/'),
            (?, 'req-4', 'c4', 'event_click', 12.0, 'good', 'Chrome', 'macOS', 'Desktop', '/checkout')
        """,
        [base, base, base, base],
    )
    con.execute(
        """
        INSERT INTO client_errors (
            timestamp, req_id, cid, pathname, error_message, error_file, error_line, error_col
        ) VALUES
            (?, 'req-err-1', 'c2', '/checkout', 'TypeError: boom', 'checkout.js', 42, 7)
        """,
        [base],
    )

    result = rum_repo.fetch_rum_analytics(
        con,
        service_id="svc-std",
        start_time=start_iso,
        end_time=end_iso,
        parsed_filters={},
        where_sql="timestamp >= ? AND timestamp <= ?",
        params=[start_iso, end_iso],
        raw_window_max_s=86400,
    )

    assert result["no_data"] is False
    assert result["pageviews"] == 3
    assert result["interactions"] == 1
    assert result["errors_count"] == 1
    assert result["total_beacons"] == 5
    assert result["browsers"] == {"Chrome": 3, "Safari": 1}
    assert result["os"] == {"macOS": 3, "iOS": 1}
    assert result["devices"] == {"Desktop": 3, "Mobile": 1}

    vitals_by_name = {row[0]: row for row in result["vitals_rows"]}
    assert vitals_by_name["LCP"][2] == 2  # total_count
    assert vitals_by_name["LCP"][3] == 1  # good_count
    assert vitals_by_name["LCP"][5] == 1  # poor_count

    assert result[" worst_pages_rows" if " worst_pages_rows" in result else "worst_pages_rows"][0][0] == "/checkout"
    assert result["errors_rows"] == [("TypeError: boom", "checkout.js", 42, 7, 1)]
    assert len(result["trends_rows"]) == 1


def test_fetch_rum_analytics_rollup_and_tail_merge(in_memory_duckdb) -> None:
    con = in_memory_duckdb
    _create_raw_rum_tables(con)
    _create_rollup_rum_tables(con)

    bucket_closed = datetime(2026, 8, 1, 10, 0, 0, tzinfo=UTC)
    bucket_tail = datetime(2026, 8, 5, 11, 0, 0, tzinfo=UTC)
    tail_ts = bucket_tail + timedelta(minutes=15)

    con.execute(
        """
        INSERT INTO rum_vitals_aggregates VALUES
            ('svc-std', ?, 'total', 'pageviews', 10, 0.0, 0, 0, 0, 0.0, 0.0, 0.0),
            ('svc-std', ?, 'total', 'interactions', 2, 0.0, 0, 0, 0, 0.0, 0.0, 0.0),
            ('svc-std', ?, 'total', 'total_beacons', 12, 0.0, 0, 0, 0, 0.0, 0.0, 0.0),
            ('svc-std', ?, 'metric_name', 'LCP', 10, 18000.0, 8, 1, 1, 1600.0, 2100.0, 3200.0),
            ('svc-std', ?, 'browser', 'Chrome', 10, 0.0, 0, 0, 0, 0.0, 0.0, 0.0),
            ('svc-std', ?, 'os', 'macOS', 10, 0.0, 0, 0, 0, 0.0, 0.0, 0.0),
            ('svc-std', ?, 'device', 'Desktop', 10, 0.0, 0, 0, 0, 0.0, 0.0, 0.0),
            ('svc-std', ?, 'pathname', '/home', 10, 0.0, 0, 0, 0, 0.0, 0.0, 0.0),
            ('svc-std', ?, 'path_metric_load', '/home', 10, 18000.0, 0, 0, 0, 0.0, 0.0, 0.0),
            ('svc-std', ?, 'path_metric_lcp', '/home', 10, 0.0, 0, 0, 0, 0.0, 2100.0, 0.0)
        """,
        [bucket_closed] * 10,
    )
    con.execute(
        """
        INSERT INTO rum_error_aggregates VALUES
            ('svc-std', ?, 'total', 'errors', 1),
            ('svc-std', ?, 'pathname', '/home', 1),
            ('svc-std', ?, 'exception', 'SyntaxError: bad|bundle.js|12|4', 1)
        """,
        [bucket_closed] * 3,
    )

    assert rum_repo.count_aggregate_buckets(con, "svc-std") == 1

    # Add un-aggregated tail events in a newer hour so RUM_EFF_*_AGG_CREATE merges them.
    con.execute(
        """
        INSERT INTO client_vitals (
            timestamp, req_id, cid, metric_name, metric_value, metric_rating,
            browser, os, device, pathname
        ) VALUES
            (?, 'tail-req-1', 'c9', 'LCP', 1900.0, 'good', 'Firefox', 'Linux', 'Desktop', '/tail')
        """,
        [tail_ts],
    )
    con.execute(
        """
        INSERT INTO client_errors (
            timestamp, req_id, cid, pathname, error_message, error_file, error_line, error_col
        ) VALUES
            (?, 'tail-err-1', 'c9', '/tail', 'RangeError: oob', 'tail.js', 9, 1)
        """,
        [tail_ts],
    )

    start_iso = (bucket_closed - timedelta(hours=1)).isoformat()
    end_iso = (bucket_tail + timedelta(hours=1)).isoformat()

    result = rum_repo.fetch_rum_analytics(
        con,
        service_id="svc-std",
        start_time=start_iso,
        end_time=end_iso,
        parsed_filters={},
        where_sql="timestamp >= ? AND timestamp <= ?",
        params=[start_iso, end_iso],
        raw_window_max_s=3600,  # multi-day window > 1h uses rollup + tail merge
    )

    assert result["no_data"] is False
    assert result["_approx"] is True
    assert result["pageviews"] == 11
    assert result["interactions"] == 2
    assert result["errors_count"] == 2
    assert result["total_beacons"] == 14
    assert result["browsers"] == {"Chrome": 10, "Firefox": 1}
    assert ("SyntaxError: bad", "bundle.js", 12, 4, 1) in result["errors_rows"]
    assert ("RangeError: oob", "tail.js", 9, 1, 1) in result["errors_rows"]


def test_fetch_rum_live_events_and_prune_missing_view_files(in_memory_duckdb, tmp_path: Path) -> None:
    con = in_memory_duckdb
    _create_raw_rum_tables(con)

    t1 = datetime(2026, 8, 5, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2026, 8, 5, 12, 1, 0, tzinfo=UTC)
    # Insert duplicate vital rows to verify QUALIFY deduplication and ordering.
    con.execute(
        """
        INSERT INTO client_vitals (
            timestamp, pathname, metric_name, metric_value, metric_rating,
            browser, os, device, cid, req_id, city, region, country, pop, tls, ttfb
        ) VALUES
            (?, '/p1', 'LCP', 1100.0, 'good', 'Chrome', 'macOS', 'Desktop', 'c1', 'r1', 'Denver', 'CO', 'US', 'DEN', 'TLSv1.3', 45.0),
            (?, '/p1', 'LCP', 1100.0, 'good', 'Chrome', 'macOS', 'Desktop', 'c1', 'r1', 'Denver', 'CO', 'US', 'DEN', 'TLSv1.3', 45.0)
        """,
        [t1, t1],
    )
    con.execute(
        """
        INSERT INTO client_errors (
            timestamp, pathname, browser, os, device, cid, req_id,
            error_message, error_file, error_line, city, region, country, pop, tls, ttfb
        ) VALUES
            (?, '/p2', 'Safari', 'iOS', 'Mobile', 'c2', 'r2', 'Error: fail', 'app.js', 5, 'Austin', 'TX', 'US', 'DFW', 'TLSv1.3', 60.0)
        """,
        [t2],
    )

    events = rum_repo.fetch_rum_live_events(
        con,
        where_sql="timestamp >= ?",
        params=[t1 - timedelta(minutes=5)],
    )
    assert len(events) == 2
    assert events[0][0] == "error"
    assert events[1][0] == "pageview"

    # Verify _prune_missing_files_from_rum_views rewrites views referencing deleted parquet files.
    valid_parquet = tmp_path / "batch_001.parquet"
    missing_parquet = tmp_path / "batch_002.parquet"
    con.execute(f"COPY (SELECT * FROM client_vitals) TO '{valid_parquet}' (FORMAT PARQUET)")
    con.execute(f"COPY (SELECT * FROM client_vitals) TO '{missing_parquet}' (FORMAT PARQUET)")
    con.execute("DROP TABLE client_vitals")
    con.execute(
        f"CREATE TEMP VIEW client_vitals AS SELECT * FROM read_parquet(['{valid_parquet}', '{missing_parquet}'])"
    )
    missing_parquet.unlink()

    rum_repo._prune_missing_files_from_rum_views(con)
    rows = con.execute("SELECT COUNT(*) FROM client_vitals").fetchone()
    assert rows[0] == 2
