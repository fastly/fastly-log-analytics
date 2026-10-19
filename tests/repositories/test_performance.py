from unittest.mock import patch

from backend.models.common import FilterSpec
from backend.repositories._base import _safe_table
from backend.repositories.performance import get_performance_aggregates
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


def _perf_latency_hit(*_a, **kw):
    """Canned try_perf_latency_from_rollup hit for url/asn."""
    dim = kw.get("dimension")
    value = "/rolled" if dim == "url" else 7922
    return {"rows": [{"value": value, "requests": 100, "avg_ms": 1.0, "p50_ms": 1.0, "p95_ms": 2.0, "p99_ms": 3.0}]}


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

    # The URLs we artificially slowed down should be present (if enough requests)
    # The mock data generator adds randomness, so we mainly check the structure
    assert isinstance(result["top_urls"], list)
    if len(result["top_urls"]) > 0:
        top_url = result["top_urls"][0]
        assert "url" in top_url
        assert "p99" in top_url
        assert top_url["p99"] > 0

    assert isinstance(result["top_asns"], list)


# ── get_performance_aggregates: early returns ──────────────────────────────


def test_get_performance_aggregates_returns_empty_arrays_for_unknown_schema(in_memory_duckdb, test_service_source):
    """No schema (table missing) → all-empty arrays so the frontend
    can map() over them without conditional checks."""
    # No insert_mock_logs call → table doesn't exist
    result = get_performance_aggregates(
        con=in_memory_duckdb, src=test_service_source, start_time=None, end_time=None, filters={}
    )

    for key in ("top_urls", "top_asns", "ttl_dist", "scatter"):
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
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={"status": FilterSpec(mode="include", values=["200", "404", "500"])},
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


# ── Section selector (P-4 slice 5) ───────────────────────────────────────────


_ALL_PERF_SECTIONS = {"waterfall", "top_urls", "top_asns", "ttl_dist", "scatter"}


def test_performance_aggregates_sections_none_preserves_full_response(in_memory_duckdb, test_service_source):
    """sections=None must return every section the full-response path
    produces today — the zero-risk default for callers that haven't
    opted into the selector."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=20)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_performance_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        sections=None,
    )

    assert _ALL_PERF_SECTIONS.issubset(result.keys()), (
        f"sections=None should keep the full response shape; missing: {_ALL_PERF_SECTIONS - result.keys()}"
    )


def test_performance_aggregates_single_ttl_dist_emits_only_requested(in_memory_duckdb, test_service_source):
    """sections={'ttl_dist'} returns ONLY ttl_dist among the 5 selectable
    sections — proves the gates suppress the unrequested SQL (a
    distributions-only request should not pay for the top_urls/top_asns
    CTE work)."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=10)
    for log in logs:
        log["ttl"] = 60
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_performance_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={"status": FilterSpec(mode="include", values=["200", "404", "500"])},
        sections={"ttl_dist"},
    )
    present = _ALL_PERF_SECTIONS & result.keys()
    assert present == {"ttl_dist"}, f"single-section request leaked extra section keys; got {present}"
    # Timer entries are the load-bearing FE signal — suppressed sections
    # must not append to it.
    timer_names = {t["section"] for t in result.get("section_timings", [])}
    assert "ttl_dist_query" in timer_names
    for blocked in ("top_urls_query", "top_asns_query", "scatter_waterfall_query"):
        assert blocked not in timer_names, f"selector did not suppress {blocked}; got {timer_names}"


def test_performance_aggregates_top_urls_keeps_top_asns_cte_shared(in_memory_duckdb, test_service_source):
    """top_urls + top_asns travel together as the expanded set the
    router produces — emulate that expansion here to prove the repo
    gate keeps both 2-pass CTE branches firing off the SAME per-request
    temp (commit 8fc53e1's optimization survives the selector)."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=30)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_performance_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={"status": FilterSpec(mode="include", values=["200", "404", "500"])},
        sections={"top_urls", "top_asns"},
    )
    present = _ALL_PERF_SECTIONS & result.keys()
    assert present == {"top_urls", "top_asns"}, f"top_urls+top_asns pair leaked or dropped keys; got {present}"
    timer_names = {t["section"] for t in result.get("section_timings", [])}
    # Both CTE branches fire, AND temp_table_create fires exactly once —
    # proves they share the temp.
    assert "top_urls_query" in timer_names
    assert "top_asns_query" in timer_names
    temp_marks = [t for t in result.get("section_timings", []) if t["section"] == "temp_table_create"]
    assert len(temp_marks) == 1, f"temp_table materialized {len(temp_marks)} times; expected 1"


def test_performance_aggregates_multi_section_runs_only_requested(in_memory_duckdb, test_service_source):
    """sections={'top_urls','top_asns','ttl_dist'} runs three branches
    and skips scatter+waterfall — covers the typical multi-card
    selector request shape."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=25)
    for log in logs:
        log["ttl"] = 300
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_performance_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={},
        sections={"top_urls", "top_asns", "ttl_dist"},
    )
    present = _ALL_PERF_SECTIONS & result.keys()
    assert present == {"top_urls", "top_asns", "ttl_dist"}, f"multi-section request got {present}"
    timer_names = {t["section"] for t in result.get("section_timings", [])}
    assert "scatter_waterfall_query" not in timer_names, (
        f"scatter/waterfall fired without being requested; got {timer_names}"
    )


def test_performance_aggregates_waterfall_emits_when_requested_alone(in_memory_duckdb, test_service_source):
    """sections={'waterfall','scatter'} — the pair shares the
    MATERIALIZED components CTE so requesting one auto-includes the
    other at the router boundary. The repo emits both keys without
    firing the top_urls/top_asns CTE."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=20)
    for log in logs:
        log["elapsed"] = 200_000
        log["ttfb"] = f"{0.1:.6f}"
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    result = get_performance_aggregates(
        con=in_memory_duckdb,
        src=test_service_source,
        start_time=None,
        end_time=None,
        filters={"status": FilterSpec(mode="include", values=["200", "404", "500"])},
        sections={"waterfall", "scatter"},
    )
    present = _ALL_PERF_SECTIONS & result.keys()
    assert present == {"waterfall", "scatter"}, f"waterfall+scatter pair got {present}"
    timer_names = {t["section"] for t in result.get("section_timings", [])}
    assert "scatter_waterfall_query" in timer_names
    for blocked in ("top_urls_query", "top_asns_query", "ttl_dist_query"):
        assert blocked not in timer_names, f"selector did not suppress {blocked}; got {timer_names}"


# ── Two-pass hoist: rollup-served → temp skipped / narrowed ──────────────────


def test_performance_aggregates_temp_skipped_when_all_rolled_no_scatter(in_memory_duckdb, test_service_source):
    """When top_urls + top_asns + ttl_dist all serve from rollups AND
    scatter/waterfall aren't requested, NO temp is built — the per-column
    materialize collapses to nothing (`perf:temp_skipped`)."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=20)
    for log in logs:
        log["ttl"] = 60
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    with (
        patch("backend.repositories._base.QueryRunner.try_perf_latency_from_rollup", side_effect=_perf_latency_hit),
        patch(
            "backend.repositories._base.QueryRunner.try_perf_ttl_dist_from_rollup",
            return_value=[{"bucket": "<1m", "count": 5}],
        ),
    ):
        result = get_performance_aggregates(
            con=in_memory_duckdb,
            src=test_service_source,
            start_time="2026-01-01T00:00:00Z",
            end_time="2026-01-08T00:00:00Z",
            filters={},
            sections={"top_urls", "top_asns", "ttl_dist"},
        )

    names = {t["section"] for t in result["section_timings"]}
    assert "perf:temp_skipped" in names
    assert "temp_table_create" not in names, "temp must not materialize when every section is rollup-served"
    assert result["ttl_dist"] == [{"bucket": "<1m", "count": 5}]
    assert result["top_urls"][0]["url"] == "/rolled"
    assert result.get("approx") is True


def test_performance_aggregates_temp_narrowed_when_scatter_forces_temp(in_memory_duckdb, test_service_source):
    """All-sections request: top_urls/top_asns/ttl_dist serve from rollups, but
    scatter+waterfall keep a temp — narrowed to their latency columns
    (`perf:temp_narrowed`), and ttl is served from the rollup not the live
    histogram."""
    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=20)
    for log in logs:
        log["elapsed"] = 200_000
        log["ttfb"] = f"{0.1:.6f}"
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    with (
        patch("backend.repositories._base.QueryRunner.try_perf_latency_from_rollup", side_effect=_perf_latency_hit),
        patch(
            "backend.repositories._base.QueryRunner.try_perf_ttl_dist_from_rollup",
            return_value=[{"bucket": "<1m", "count": 5}],
        ),
    ):
        result = get_performance_aggregates(
            con=in_memory_duckdb,
            src=test_service_source,
            start_time="2026-01-01T00:00:00Z",
            end_time="2026-01-08T00:00:00Z",
            filters={"status": FilterSpec(mode="include", values=["200", "404", "500"])},
            sections=None,
        )

    names = {t["section"] for t in result["section_timings"]}
    assert "perf:temp_narrowed" in names
    assert "temp_table_create" in names, "scatter still needs a (narrowed) temp"
    # ttl served from rollup, NOT the live histogram.
    assert "ttl_dist_rollup" in names
    assert "ttl_dist_query" not in names
    assert result["ttl_dist"] == [{"bucket": "<1m", "count": 5}]
    # scatter/waterfall still produced from the narrowed temp.
    assert "scatter" in result and "waterfall" in result
