import time
from unittest.mock import patch

import pytest

from backend.repositories._base import _safe_table
from backend.repositories.dashboard import get_aggregates
from backend.repositories.insights import get_insights


@pytest.fixture
def one_million_row_service(in_memory_duckdb, test_service_source):
    """Seed 1,000,000 rows into DuckDB using native generation for speed."""
    table = _safe_table(test_service_source["name"])

    # 1. Create table schema
    in_memory_duckdb.execute(f"""
        CREATE TABLE {table} (
            timestamp TIMESTAMPTZ,
            status INTEGER,
            country VARCHAR,
            pop VARCHAR,
            url VARCHAR,
            method VARCHAR,
            ip VARCHAR,
            elapsed INTEGER,
            cache VARCHAR,
            ttfb DOUBLE,
            edge BOOLEAN,
            resp_bytes INTEGER,
            backend VARCHAR
        )
    """)

    # 2. Seed 1M rows using range() and random values
    # This takes ~1-2 seconds on a modern laptop, much faster than executemany()
    in_memory_duckdb.execute(f"""
        INSERT INTO {table}
        SELECT
            now() - interval (random() * 24) hour as timestamp,
            (case when random() > 0.95 then 500 when random() > 0.90 then 404 else 200 end) as status,
            (case when random() > 0.5 then 'US' when random() > 0.3 then 'GB' else 'DE' end) as country,
            (case when random() > 0.7 then 'LAX' when random() > 0.4 then 'JFK' else 'LHR' end) as pop,
            '/path/' || (random() * 100)::int as url,
            'GET' as method,
            '10.0.0.1' as ip,
            (random() * 1000)::int as elapsed,
            (case when random() > 0.3 then 'HIT' else 'MISS' end) as cache,
            random() as ttfb,
            true as edge,
            (random() * 10000)::int as resp_bytes,
            'origin-1' as backend
        FROM range(1000000)
    """)
    return test_service_source


def test_performance_dashboard_aggregates_1m_rows(in_memory_duckdb, one_million_row_service):
    """Benchmark dashboard aggregates over 1M rows.

    Target: < 2.0s on medium CI runners.
    """
    start = time.time()

    res = get_aggregates(
        con=in_memory_duckdb,
        src=one_million_row_service,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 hour",
        chart_metric="requests",
    )

    duration = time.time() - start
    print(f"\nDashboard aggregates (1M rows) took {duration:.3f}s")

    assert res["total_rows"] == 1000000
    # CI runners vary, but 5s is a safe "something is very wrong" threshold
    assert duration < 5.0


def test_performance_insights_1m_rows(in_memory_duckdb, one_million_row_service):
    """Benchmark automated insights over 1M rows.

    This exercises ~20+ distinct analytical queries against the dataset.
    Target: < 5.0s on medium CI runners.
    """
    start = time.time()

    # insights registry needs some lat/lon data for the map card
    with patch(
        "backend.utils.pop_utils.get_pop_lat_lon_map",
        return_value={"LAX": (33.9, -118.4), "JFK": (40.6, -73.7), "LHR": (51.4, -0.4)},
    ):
        res = get_insights(con=in_memory_duckdb, src=one_million_row_service, window_hours=1, baseline_hours=24)

    duration = time.time() - start
    print(f"\nInsights generation (1M rows) took {duration:.3f}s")

    assert len(res["insights"]) > 0
    # Insights are heavier, 10s is the "safety" threshold
    assert duration < 10.0
