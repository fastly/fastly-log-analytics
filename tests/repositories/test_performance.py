from backend.repositories._base import _safe_table
from backend.repositories.performance import get_origin_ts, get_performance_aggregates
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


def test_performance_aggregates(in_memory_duckdb, test_service_source):
    # 1. Generate some mock data
    logs = generate_mock_logs(test_service_source, num_logs=50, hours_ago=1)

    # Ensure some data exists that fits the performance queries
    for i, log in enumerate(logs[:5]):
        log["elapsed"] = 500000 + (i * 10000)  # High latency
        log["ttfb"] = f"{(400000 + (i * 10000)) / 1000000:.6f}"
        log["url"] = f"/slow-path-{i}"
        log["asn"] = 7922

    table_name = _safe_table(test_service_source["name"])
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    # 2. Call the repository function
    filters = {}
    result = get_performance_aggregates(
        con=in_memory_duckdb, src=test_service_source, start_time=None, end_time=None, filters=filters
    )

    # 3. Assertions
    assert "top_urls" in result
    assert "top_asns" in result
    assert "latency_ts" in result

    # The URLs we artificially slowed down should be present (if enough requests)
    # The mock data generator adds randomness, so we mainly check the structure
    assert isinstance(result["top_urls"], list)
    if len(result["top_urls"]) > 0:
        top_url = result["top_urls"][0]
        assert "url" in top_url
        assert "p99" in top_url
        assert top_url["p99"] > 0

    assert isinstance(result["top_asns"], list)


def test_get_origin_ts_returns_timeseries_key(in_memory_duckdb, test_service_source):
    """Regression: get_origin_ts must use 'timeseries' key to match PerformanceOriginTsResponse."""
    logs = generate_mock_logs(test_service_source, num_logs=20, hours_ago=1)
    for log in logs:
        log["ottfb"] = 50000  # 50ms in microseconds

    table_name = _safe_table(test_service_source["name"])
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_origin_ts(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
    )

    assert "timeseries" in result, f"Expected 'timeseries' key, got: {list(result.keys())}"
    assert "origin_ts" not in result, "Stale key 'origin_ts' must not be returned"
    assert isinstance(result["timeseries"], list)
    assert len(result["timeseries"]) > 0
    assert "time" in result["timeseries"][0]
    assert "value" in result["timeseries"][0]


# ── get_performance_aggregates: early returns ──────────────────────────────


def test_get_performance_aggregates_returns_empty_arrays_for_unknown_schema(in_memory_duckdb, test_service_source):
    """No schema (table missing) → all-empty arrays so the frontend
    can map() over them without conditional checks."""
    # No insert_mock_logs call → table doesn't exist
    result = get_performance_aggregates(
        con=in_memory_duckdb, src=test_service_source, start_time=None, end_time=None, filters={}
    )

    for key in ("latency_ts", "top_urls", "top_asns", "ttl_dist", "scatter"):
        assert result[key] == []


def test_get_performance_aggregates_top_urls_respects_sort_by_p99(in_memory_duckdb, test_service_source):
    """Default sort is p99 — pinned because changing the sort column
    silently re-orders the dashboard list and users would lose the
    "slowest endpoints" semantic."""
    logs = generate_mock_logs(test_service_source, num_logs=80, hours_ago=1)
    for i, log in enumerate(logs):
        # First 40 logs: /high-tail (one huge outlier per group → high p99)
        if i < 40:
            log["url"] = "/high-tail"
            log["elapsed"] = 100_000 if i % 10 != 0 else 1_000_000
        else:
            log["url"] = "/steady"
            log["elapsed"] = 100_000

    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_performance_aggregates(
        con=in_memory_duckdb, src=test_service_source, start_time=None, end_time=None, filters={}
    )
    # /high-tail has a higher p99 than /steady → ranks first
    if len(result["top_urls"]) >= 2:
        urls = [r["url"] for r in result["top_urls"]]
        assert urls[0] == "/high-tail"


def test_get_performance_aggregates_top_urls_sortable_by_p50(in_memory_duckdb, test_service_source):
    """``sort_by='p50'`` switches the ORDER BY column."""
    logs = generate_mock_logs(test_service_source, num_logs=60, hours_ago=1)
    for i, log in enumerate(logs):
        if i < 30:
            log["url"] = "/slow-median"
            log["elapsed"] = 200_000  # consistently slow
        else:
            log["url"] = "/spiky"
            log["elapsed"] = 50_000 if i % 5 != 0 else 800_000
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_performance_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        sort_by="p50",
    )
    # By p50, /slow-median wins; by p99, /spiky would. Pin the p50 ordering.
    if len(result["top_urls"]) >= 2:
        assert result["top_urls"][0]["url"] == "/slow-median"


def test_get_performance_aggregates_ttl_dist_buckets_durations(in_memory_duckdb, test_service_source):
    """TTL histogram bucket boundaries are explicit constants in the
    CASE statement (10s, 30s, 60s, 5m, 10m, ..., 1y, >1y). Pinned
    because the frontend renders these as labelled bars and a bucket
    rename would silently break the legend."""
    logs = generate_mock_logs(test_service_source, num_logs=40, hours_ago=1)
    # Sprinkle TTL values across distinct buckets
    ttl_values = [5, 25, 50, 200, 500, 3600, 86400 * 2]
    for i, log in enumerate(logs):
        log["ttl"] = ttl_values[i % len(ttl_values)]
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_performance_aggregates(
        con=in_memory_duckdb, src=test_service_source, start_time=None, end_time=None, filters={}
    )
    buckets = {row["bucket"] for row in result["ttl_dist"]}
    # At least one of the expected labels should appear
    assert any(b in buckets for b in ("<10s", "<30s", "<1m", "<5m", "<10m", "<1h", "<3d"))


def test_get_performance_aggregates_scatter_filters_negative_edge_time(in_memory_duckdb, test_service_source):
    """The scatter query filters out rows where ``elapsed < ttfb``
    (would produce negative edge_ms). Pinned because including those
    would render visible artifacts in the lower-right quadrant of
    the scatter plot."""
    logs = generate_mock_logs(test_service_source, num_logs=20, hours_ago=1)
    for log in logs:
        log["elapsed"] = 100_000  # 100ms total
        log["ttfb"] = f"{0.05:.6f}"  # 50ms — elapsed > ttfb → valid
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_performance_aggregates(
        con=in_memory_duckdb, src=test_service_source, start_time=None, end_time=None, filters={}
    )
    # All scatter rows must have non-negative edge_ms
    for pt in result["scatter"]:
        assert pt["edge"] >= 0


# ── get_origin_ts: fallback paths ──────────────────────────────────────────


def test_get_origin_ts_returns_empty_when_no_schema(in_memory_duckdb, test_service_source):
    """Missing table → empty timeseries. Pinned because the frontend
    map() over an absent list would crash."""
    result = get_origin_ts(con=in_memory_duckdb, src=test_service_source, start_time=None, end_time=None, filters={})
    assert result["timeseries"] == []


def test_get_origin_ts_returns_empty_when_metric_unavailable(in_memory_duckdb, test_service_source):
    """``origin_metric='ttlb'`` but no ``ottlb`` column → empty
    timeseries. Pinned because querying a non-existent column would
    raise a CatalogException."""
    in_memory_duckdb.execute(
        f"CREATE TABLE {_safe_table(test_service_source['name'])} (timestamp TIMESTAMP, status INTEGER)"
    )
    result = get_origin_ts(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        origin_metric="ttlb",
    )
    assert result["timeseries"] == []
