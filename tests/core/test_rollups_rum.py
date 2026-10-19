"""Tests for precomputed RUM aggregates writer."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import duckdb

from backend.core.rollups.rum import recompute_rum_aggregates, table_exists


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
