"""RUM status freshness must mirror the request side (AGENTS.md Trap #39).

The sync-status header sources RUM freshness through
``rum_count_and_latest``. The precomputed aggregate tables are a fast
count path, but an existing-but-EMPTY aggregate (rollup lag, a failed
recompute tick, or a service whose first rollup hasn't run yet) must NOT
suppress the raw-derived ``latest_log_at`` — otherwise the header shows
"Never" while live beacons are already queryable on the /rum page, which
is exactly the asymmetry that never affects the request side (it reads
the raw view directly).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import duckdb


def _make_rum_con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    # Aggregate tables EXIST (mirrors a service that has rolled up before)
    # but are left EMPTY to simulate rollup lag behind freshly-landed raw.
    con.execute("""
        CREATE TABLE rum_vitals_aggregates (
            service_id VARCHAR, bucket_start TIMESTAMPTZ, dimension VARCHAR,
            value VARCHAR, event_count BIGINT, value_sum DOUBLE,
            good_count BIGINT, ni_count BIGINT, poor_count BIGINT,
            p50_value DOUBLE, p75_value DOUBLE, p99_value DOUBLE
        )
    """)
    con.execute("""
        CREATE TABLE rum_error_aggregates (
            service_id VARCHAR, bucket_start TIMESTAMPTZ, dimension VARCHAR,
            value VARCHAR, error_count BIGINT
        )
    """)
    con.execute("CREATE TABLE client_vitals (req_id VARCHAR, cid VARCHAR, timestamp TIMESTAMPTZ)")
    con.execute("CREATE TABLE client_errors (req_id VARCHAR, cid VARCHAR, timestamp TIMESTAMPTZ)")
    return con


def test_empty_aggregates_still_report_raw_freshness():
    """Aggregates exist but are empty; raw client_vitals has fresh rows.

    Expect a non-zero count and the raw MAX(timestamp), NOT ``None``.
    """
    from backend.core.rollups.rum import rum_count_and_latest

    con = _make_rum_con()
    recent = datetime.now(UTC) - timedelta(seconds=30)
    con.execute(
        "INSERT INTO client_vitals VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)",
        ["r1", "c1", recent, "r2", "c2", recent, "r3", "c3", recent],
    )

    count, latest = rum_count_and_latest(con)

    assert count == 3, f"expected 3 raw beacons, got {count}"
    assert latest is not None, "latest_log_at must come from raw MAX(timestamp), not be gated on empty aggregates"
    # Normalize to compare the instant regardless of tz representation.
    assert abs((latest.replace(tzinfo=UTC) if latest.tzinfo is None else latest) - recent) < timedelta(seconds=1)


def test_populated_aggregates_fast_path_returns_count_and_raw_latest():
    """When aggregates have rows, use them for the count but keep raw freshness."""
    from backend.core.rollups.rum import rum_count_and_latest

    con = _make_rum_con()
    bucket = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    con.execute(
        "INSERT INTO rum_vitals_aggregates (service_id, bucket_start, dimension, value, event_count) VALUES (?, ?, 'total', 'pageviews', 10)",
        ["svc", bucket],
    )
    recent = datetime.now(UTC) - timedelta(seconds=15)
    con.execute("INSERT INTO client_vitals VALUES (?, ?, ?)", ["r1", "c1", recent])

    count, latest = rum_count_and_latest(con)

    assert count == 10, f"aggregate fast-path count expected 10, got {count}"
    assert latest is not None
    assert abs((latest.replace(tzinfo=UTC) if latest.tzinfo is None else latest) - recent) < timedelta(seconds=1)
