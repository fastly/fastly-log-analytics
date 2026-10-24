"""Tests for backend.repositories.rum — client_vitals / client_errors query layer.

This repository queries the modern RUM dataset stored in DuckDB/Iceberg.
"""

from __future__ import annotations

from datetime import UTC, datetime

from backend.repositories import rum as rum_repo


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
