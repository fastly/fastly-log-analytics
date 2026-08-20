"""Tests for backend.repositories.rum — client_vitals / client_errors query layer.

This repository queries the modern RUM dataset stored in DuckDB/Iceberg.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from backend.repositories import rum as rum_repo
from backend.repositories._base import QueryRunner


def _runner(con, source) -> QueryRunner:
    return QueryRunner(con, source)


def test_get_web_vitals_summary_computes_percentiles_per_metric(in_memory_duckdb, test_service_source):
    con = in_memory_duckdb
    con.execute(
        "CREATE TABLE client_vitals ("
        "timestamp TIMESTAMPTZ, metric_name VARCHAR, metric_value DOUBLE, "
        "metric_rating VARCHAR, pathname VARCHAR, cid VARCHAR, req_id VARCHAR)"
    )
    base = datetime(2026, 8, 5, 10, 0, 0, tzinfo=UTC)
    # LCP values 1,2,3,4 in the same hour bucket -> p50 should land at 2.5 (linear interpolation).
    for v in (1.0, 2.0, 3.0, 4.0):
        con.execute(
            "INSERT INTO client_vitals (timestamp, metric_name, metric_value) VALUES (?, 'LCP', ?)",
            [base, v],
        )
    runner = _runner(con, test_service_source)

    rows = rum_repo.get_web_vitals_summary(runner, "svc1", base - timedelta(hours=1), base + timedelta(hours=1))

    assert len(rows) == 1
    hour, metric_name, p50, p75, p95, count = rows[0]
    assert metric_name == "LCP"
    assert count == 4
    assert p50 == 2.5


def test_get_web_vitals_summary_excludes_metrics_outside_the_known_set(in_memory_duckdb, test_service_source):
    con = in_memory_duckdb
    con.execute(
        "CREATE TABLE client_vitals ("
        "timestamp TIMESTAMPTZ, metric_name VARCHAR, metric_value DOUBLE, "
        "metric_rating VARCHAR, pathname VARCHAR, cid VARCHAR, req_id VARCHAR)"
    )
    base = datetime(2026, 8, 5, 10, 0, 0, tzinfo=UTC)
    con.execute("INSERT INTO client_vitals (timestamp, metric_name, metric_value) VALUES (?, 'LCP', 2.0)", [base])
    con.execute(
        "INSERT INTO client_vitals (timestamp, metric_name, metric_value) VALUES (?, 'NOT_A_REAL_METRIC', 99.0)", [base]
    )
    runner = _runner(con, test_service_source)

    rows = rum_repo.get_web_vitals_summary(runner, "svc1", base - timedelta(hours=1), base + timedelta(hours=1))

    metric_names = {r[1] for r in rows}
    assert metric_names == {"LCP"}


def test_get_web_vitals_summary_respects_time_window(in_memory_duckdb, test_service_source):
    con = in_memory_duckdb
    con.execute(
        "CREATE TABLE client_vitals ("
        "timestamp TIMESTAMPTZ, metric_name VARCHAR, metric_value DOUBLE, "
        "metric_rating VARCHAR, pathname VARCHAR, cid VARCHAR, req_id VARCHAR)"
    )
    base = datetime(2026, 8, 5, 10, 0, 0, tzinfo=UTC)
    con.execute("INSERT INTO client_vitals (timestamp, metric_name, metric_value) VALUES (?, 'LCP', 2.0)", [base])
    con.execute(
        "INSERT INTO client_vitals (timestamp, metric_name, metric_value) VALUES (?, 'LCP', 9.0)",
        [base - timedelta(days=5)],
    )
    runner = _runner(con, test_service_source)

    rows = rum_repo.get_web_vitals_summary(runner, "svc1", base - timedelta(hours=1), base + timedelta(hours=1))

    assert len(rows) == 1
    assert rows[0][2] == 2.0  # p50 — only the in-window row counted


def test_get_error_rate_trend_computes_percentage_across_vitals_and_errors(in_memory_duckdb, test_service_source):
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
    base = datetime(2026, 8, 5, 10, 0, 0, tzinfo=UTC)
    # 3 vitals beacons (no error) + 1 error beacon => 4 total, 1 error => 25%.
    for i in range(3):
        con.execute(
            "INSERT INTO client_vitals (timestamp, metric_name, metric_value, req_id) VALUES (?, 'LCP', 2.0, ?)",
            [base, f"req_v_{i}"],
        )
    con.execute(
        "INSERT INTO client_errors (timestamp, error_message, req_id) VALUES (?, 'TypeError: boom', 'req_err_1')",
        [base],
    )
    runner = _runner(con, test_service_source)

    rows = rum_repo.get_error_rate_trend(runner, "svc1", base - timedelta(hours=1), base + timedelta(hours=1))

    assert len(rows) == 1
    _hour, error_count, total_beacons, error_rate_pct = rows[0]
    assert error_count == 1
    assert total_beacons == 4
    assert error_rate_pct == 25.0


def test_get_error_rate_trend_zero_beacons_does_not_divide_by_zero(in_memory_duckdb, test_service_source):
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
    base = datetime(2026, 8, 5, 10, 0, 0, tzinfo=UTC)
    runner = _runner(con, test_service_source)

    rows = rum_repo.get_error_rate_trend(runner, "svc1", base - timedelta(hours=1), base + timedelta(hours=1))

    assert rows == []


def test_get_worst_pages_orders_by_combined_poor_percentage(in_memory_duckdb, test_service_source):
    con = in_memory_duckdb
    con.execute(
        "CREATE TABLE client_vitals ("
        "timestamp TIMESTAMPTZ, metric_name VARCHAR, metric_value DOUBLE, "
        "metric_rating VARCHAR, pathname VARCHAR, cid VARCHAR, req_id VARCHAR)"
    )
    base = datetime(2026, 8, 5, 10, 0, 0, tzinfo=UTC)
    # /bad: LCP+INP+CLS all poor for both sessions (100% poor on every metric).
    # /good: same three metrics present but rated good (0% poor on every
    # metric) — every metric type must be present on both pages so neither
    # page's *_poor_pct is NULL (NULLIF divide-by-zero), keeping the DESC
    # ordering by the summed percentage well-defined.
    for cid in ("s1", "s2"):
        for metric in ("LCP", "INP", "CLS"):
            con.execute(
                "INSERT INTO client_vitals (timestamp, metric_name, metric_rating, pathname, cid) VALUES (?, ?, 'poor', '/bad', ?)",
                [base, metric, cid],
            )
            con.execute(
                "INSERT INTO client_vitals (timestamp, metric_name, metric_rating, pathname, cid) VALUES (?, ?, 'good', '/good', ?)",
                [base, metric, cid],
            )
    runner = _runner(con, test_service_source)

    rows = rum_repo.get_worst_pages(runner, "svc1", base - timedelta(hours=1), base + timedelta(hours=1))

    assert [r[0] for r in rows] == ["/bad", "/good"]
    assert rows[0][1] == 100.0  # lcp_poor_pct for /bad
    assert rows[0][4] == 2  # session_count
    assert rows[1][1] == 0.0  # lcp_poor_pct for /good


def test_get_worst_pages_respects_limit(in_memory_duckdb, test_service_source):
    con = in_memory_duckdb
    con.execute(
        "CREATE TABLE client_vitals ("
        "timestamp TIMESTAMPTZ, metric_name VARCHAR, metric_value DOUBLE, "
        "metric_rating VARCHAR, pathname VARCHAR, cid VARCHAR, req_id VARCHAR)"
    )
    base = datetime(2026, 8, 5, 10, 0, 0, tzinfo=UTC)
    for i in range(5):
        con.execute(
            "INSERT INTO client_vitals (timestamp, metric_name, metric_rating, pathname, cid) VALUES (?, 'LCP', 'poor', ?, 's1')",
            [base, f"/page{i}"],
        )
    runner = _runner(con, test_service_source)

    rows = rum_repo.get_worst_pages(runner, "svc1", base - timedelta(hours=1), base + timedelta(hours=1), limit=2)

    assert len(rows) == 2


def test_get_worst_sessions_requires_more_than_five_metrics(in_memory_duckdb, test_service_source):
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
    base = datetime(2026, 8, 5, 10, 0, 0, tzinfo=UTC)
    # session "small" has only 3 metrics -> excluded by HAVING COUNT(*) > 5.
    for i in range(3):
        con.execute(
            "INSERT INTO client_vitals (timestamp, metric_rating, pathname, cid) VALUES (?, 'good', ?, 'small')",
            [base, f"/p{i}"],
        )
    # session "big" has 6 metrics, 2 poor -> included.
    for i in range(6):
        rating = "poor" if i < 2 else "good"
        con.execute(
            "INSERT INTO client_vitals (timestamp, metric_rating, pathname, cid) VALUES (?, ?, ?, 'big')",
            [base, rating, f"/p{i}"],
        )
    con.execute(
        "INSERT INTO client_errors (timestamp, cid) VALUES (?, 'big')",
        [base],
    )
    runner = _runner(con, test_service_source)

    rows = rum_repo.get_worst_sessions(runner, "svc1", base - timedelta(hours=1), base + timedelta(hours=1))

    session_ids = [r[0] for r in rows]
    assert session_ids == ["big"]
    _cid, page_count, poor_metrics, error_count, _last_seen = rows[0]
    assert page_count == 6
    assert poor_metrics == 2
    assert error_count == 1


def test_get_worst_sessions_respects_limit(in_memory_duckdb, test_service_source):
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
    base = datetime(2026, 8, 5, 10, 0, 0, tzinfo=UTC)
    for sid in ("a", "b", "c"):
        for i in range(6):
            con.execute(
                "INSERT INTO client_vitals (timestamp, metric_rating, pathname, cid) VALUES (?, 'good', ?, ?)",
                [base, f"/p{i}", sid],
            )
    runner = _runner(con, test_service_source)

    rows = rum_repo.get_worst_sessions(runner, "svc1", base - timedelta(hours=1), base + timedelta(hours=1), limit=1)

    assert len(rows) == 1
