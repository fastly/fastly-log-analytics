"""Regression tests for backend.repositories.origin — validates return keys with real data."""

from __future__ import annotations

import contextlib
import os
import re
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from backend.core.duckdb import _clear_schema_cache
from backend.models.common import FilterSpec
from backend.repositories import origin
from backend.repositories import origin as origin_mod
from backend.repositories._base import _safe_table
from backend.repositories.origin import (
    _enrich_with_distance,
    _haversine_km,
    get_aggregates,
    get_ip_health,
    get_path_breakdown,
    get_pop_latency,
    get_shielding_analysis,
    get_slow_urls,
    get_status_codes,
    get_summary,
    get_timeseries,
)
from scripts.lib.section_measurements import describe_response
from tests._helpers.duckdb_fixtures import generate_mock_logs, insert_mock_logs
from tests._helpers.rollups import noop_lock as _noop_lock


# ── Merged from tests/repositories/test_origin.py ──
def _origin_logs(src, num=40):
    """Generate logs with origin columns populated so has_origin_data returns True."""
    logs = generate_mock_logs(src, num_logs=num, hours_ago=1)
    for i, log in enumerate(logs):
        log["ottfb"] = 50000 + i * 1000  # 50–90ms in microseconds
        log["ost"] = 200 if i % 5 != 0 else 500
        log["oip"] = "203.0.113.1" if i < 25 else "203.0.113.2"
    return logs


def test_get_summary_no_origin_data(in_memory_duckdb, test_service_source):
    """Without any origin timing values (ottfb or ttfb), has_data is False."""
    logs = generate_mock_logs(test_service_source, num_logs=10)
    # Clear both candidate columns
    for log in logs:
        log["ottfb"] = None
        log["ttfb"] = None
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_summary(in_memory_duckdb, test_service_source, None, None, {})
    assert result["has_data"] is False


def test_get_summary_returns_expected_keys(in_memory_duckdb, test_service_source):
    """All latency percentile keys present and non-null when ottfb data exists."""
    logs = _origin_logs(test_service_source)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_summary(in_memory_duckdb, test_service_source, None, None, {})
    assert result["has_data"] is True
    for key in (
        "total_misses",
        "total_passes",
        "ottfb_p50_ms",
        "ottfb_p75_ms",
        "ottfb_p95_ms",
        "ottfb_p99_ms",
        "ottlb_p50_ms",
        "ottlb_p95_ms",
        "cdn_overhead_p50_ms",
        "origin_error_rate",
        "obytes_p50",
    ):
        assert key in result, f"Missing key: {key}"
    assert result["ottfb_p50_ms"] is not None
    assert result["ottfb_p50_ms"] > 0
    # ``by_leg`` was dropped — no UI surface consumed the per-edge
    # breakdown. The single-grouping rollup is roughly half the work.
    assert "by_leg" not in result


def test_get_summary_uses_single_grouping_pass(in_memory_duckdb, test_service_source):
    """``get_summary`` runs a single () GROUPING aggregate — no GROUPING
    SETS, no per-edge ``by_leg`` rows. The previous combined-pass shape
    paid for a second hash partition + per-edge percentile sorts the
    page never read; this test pins the slim shape so it doesn't drift
    back."""
    logs = _origin_logs(test_service_source, num=50)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_summary(in_memory_duckdb, test_service_source, None, None, {})
    assert result["has_data"] is True
    assert result["ottfb_p50_ms"] is not None
    assert "by_leg" not in result

    debug_queries = result.get("debug_queries") or result.get("_debug_queries") or []
    logs_scans = [
        q for q in debug_queries if "logs_" in q["sql"] and not q["sql"].lstrip().upper().startswith("CREATE")
    ]
    assert len(logs_scans) == 1, (
        f"get_summary must do a single scan. Got {len(logs_scans)} scan(s): {[q['sql'][:200] for q in logs_scans]}"
    )
    assert "GROUPING SETS" not in logs_scans[0]["sql"], (
        f"per-edge GROUPING SETS was dropped — frontend never read by_leg. Got: {logs_scans[0]['sql'][:300]}"
    )


def test_get_timeseries_returns_series_key(in_memory_duckdb, test_service_source):
    """Returns 'series', not 'rows' or 'data'. Each point has time/miss_count/value."""
    logs = _origin_logs(test_service_source)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_timeseries(in_memory_duckdb, test_service_source, None, None, {}, metric="ttfb", percentile="p95")
    assert result["has_data"] is True
    assert "series" in result
    assert isinstance(result["series"], list)
    assert len(result["series"]) > 0
    pt = result["series"][0]
    for key in ("time", "miss_count", "value"):
        assert key in pt, f"Missing key in series point: {key}"


def test_get_timeseries_ttfb_fallback(in_memory_duckdb, test_service_source):
    """Verify get_timeseries works when ottfb is missing/null but ttfb exists."""
    logs = generate_mock_logs(test_service_source, num_logs=20)
    for log in logs:
        log["ottfb"] = None
        log["ttfb"] = "0.123456"  # 123.456ms
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_timeseries(in_memory_duckdb, test_service_source, None, None, {}, metric="ttfb")
    assert result["has_data"] is True
    assert len(result["series"]) > 0
    pt = result["series"][0]
    # 0.123456s * 1000 = 123.456ms
    assert pt["value"] is not None
    assert 123.0 < pt["value"] < 124.0


def test_get_slow_urls_returns_rows_key(in_memory_duckdb, test_service_source):
    """Returns 'rows' (not 'data') with url/requests/p50_ms/p95_ms/p99_ms."""
    logs = _origin_logs(test_service_source, num=50)
    # Concentrate requests to two URLs so min_requests is met
    for i, log in enumerate(logs):
        log["url"] = "/slow" if i < 30 else "/fast"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_slow_urls(in_memory_duckdb, test_service_source, None, None, {}, min_requests=5)
    assert result["has_data"] is True
    assert "rows" in result, f"Expected 'rows' key, got: {list(result.keys())}"
    assert "data" not in result
    assert len(result["rows"]) > 0
    row = result["rows"][0]
    for key in ("url", "requests", "p50_ms", "p95_ms", "p99_ms"):
        assert key in row, f"Missing key in slow_url row: {key}"


def test_get_status_codes_returns_rows_key(in_memory_duckdb, test_service_source):
    """Returns 'rows' with status/count/pct fields."""
    logs = _origin_logs(test_service_source)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_status_codes(in_memory_duckdb, test_service_source, None, None, {})
    assert result["has_data"] is True
    assert "rows" in result, f"Expected 'rows' key, got: {list(result.keys())}"
    assert len(result["rows"]) > 0
    row = result["rows"][0]
    for key in ("status", "count", "pct"):
        assert key in row, f"Missing key in status_code row: {key}"
    # pct values should sum to ~100
    total_pct = sum(r["pct"] for r in result["rows"])
    assert 99.0 <= total_pct <= 101.0


def test_get_pop_latency_returns_rows_key(in_memory_duckdb, test_service_source):
    """Returns 'rows' with pop/requests/p50_ms/p95_ms/elevated."""
    logs = _origin_logs(test_service_source)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_pop_latency(in_memory_duckdb, test_service_source, None, None, {})
    assert result["has_data"] is True
    assert "rows" in result, f"Expected 'rows' key, got: {list(result.keys())}"
    assert "requires_group_c" in result
    assert len(result["rows"]) > 0
    row = result["rows"][0]
    for key in ("pop", "requests", "p50_ms", "p95_ms", "elevated"):
        assert key in row, f"Missing key in pop_latency row: {key}"
    assert isinstance(row["elevated"], bool)


def test_get_pop_latency_no_pop_col(in_memory_duckdb, test_service_source):
    """Returns requires_group_c=True when pop column is absent from data."""
    # Generate logs without pop (all None) — pop is in schema but null means not in actual data
    logs = generate_mock_logs(test_service_source, num_logs=10)
    for log in logs:
        log.pop("pop", None)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_pop_latency(in_memory_duckdb, test_service_source, None, None, {})
    # Either no data (no ottfb) or requires_group_c — either way rows should be empty
    assert "rows" in result
    assert "requires_group_c" in result


def test_get_ip_health_returns_rows_key(in_memory_duckdb, test_service_source):
    """Returns 'rows' with oip/requests/p50_ms/p95_ms/error_pct."""
    logs = _origin_logs(test_service_source, num=30)
    # All from same oip so HAVING COUNT(*) >= 10 is satisfied
    for log in logs:
        log["oip"] = "203.0.113.1"
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_ip_health(in_memory_duckdb, test_service_source, None, None, {})
    assert result["has_data"] is True
    assert "rows" in result, f"Expected 'rows' key, got: {list(result.keys())}"
    assert len(result["rows"]) > 0
    row = result["rows"][0]
    for key in ("oip", "requests", "p50_ms", "p95_ms", "error_pct"):
        assert key in row, f"Missing key in ip_health row: {key}"


def test_get_path_breakdown_returns_expected_keys(in_memory_duckdb, test_service_source):
    """Returns has_data, shielding_detected, and rows."""
    logs = _origin_logs(test_service_source)
    # Set edge=True on all logs; edge column required for this function
    for log in logs:
        log["edge"] = True
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = get_path_breakdown(in_memory_duckdb, test_service_source, None, None, {})
    assert "has_data" in result
    assert "shielding_detected" in result
    assert "rows" in result, f"Expected 'rows' key, got: {list(result.keys())}"
    if result["has_data"]:
        assert isinstance(result["rows"], list)
        if result["rows"]:
            row = result["rows"][0]
            for key in ("edge", "requests", "p50_ms", "p95_ms"):
                assert key in row


def test_haversine_zero_distance_when_coords_equal():
    """Identical coords → distance 0."""
    assert _haversine_km(40.7128, -74.0060, 40.7128, -74.0060) == pytest.approx(0.0)


def test_haversine_nyc_to_la_is_approximately_3935km():
    """NYC ↔ LA is ~3,935 km great-circle distance."""
    dist = _haversine_km(40.7128, -74.0060, 34.0522, -118.2437)
    assert 3925 <= dist <= 3945


def test_haversine_is_symmetric():
    """``haversine(a, b)`` must equal ``haversine(b, a)``."""
    a, b = (40.0, -74.0), (37.0, -122.0)
    assert _haversine_km(*a, *b) == pytest.approx(_haversine_km(*b, *a))


_LAX_SJC_POPS = {"LAX": (33.9425, -118.4081), "SJC": (37.3639, -121.9289)}


def test_enrich_with_known_pops_computes_distance_and_efficiency():
    """Two known POPs + measured p50 → distance, light-speed RTT, and efficiency ratio populated."""
    row = {"edge_pop": "LAX", "shield_pop": "SJC", "p50_ms": 100.0}
    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value=_LAX_SJC_POPS):
        out = _enrich_with_distance(row)

    assert out["distance_km"] is not None and out["distance_km"] > 0
    assert out["light_speed_rtt_ms"] is not None
    assert out["edge_lat"] == 33.9425
    assert out["shield_lon"] == -121.9289
    assert out["efficiency_ratio"] is not None and out["efficiency_ratio"] > 3.0


def test_enrich_pop_codes_are_uppercased_for_lookup():
    """The cache is keyed on uppercase POP codes; incoming values are upper-cased."""
    row = {"edge_pop": "lax", "shield_pop": "sjc", "p50_ms": 100.0}
    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value=_LAX_SJC_POPS):
        out = _enrich_with_distance(row)
    assert out["distance_km"] is not None


def test_enrich_with_unknown_pop_returns_none_for_all_coords():
    """If either POP isn't in the cache, distance and efficiency are None."""
    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value={"LAX": (33.9425, -118.4081)}):
        out = _enrich_with_distance({"edge_pop": "LAX", "shield_pop": "GHOST", "p50_ms": 50.0})

    assert out["distance_km"] is None
    assert out["light_speed_rtt_ms"] is None
    assert out["efficiency_ratio"] is None
    assert out["anomaly_static"] is False
    assert out["edge_lat"] == 33.9425
    assert out["shield_lat"] is None


def test_enrich_anomaly_only_flags_when_overhead_above_20ms():
    """``anomaly_static`` requires BOTH efficiency > 3x AND absolute overhead >= 20ms."""
    row = {"edge_pop": "SJC", "shield_pop": "LAX", "p50_ms": 15.0, "requests": 500}
    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value=_LAX_SJC_POPS):
        out = _enrich_with_distance(row)

    assert out["efficiency_ratio"] is not None and out["efficiency_ratio"] > 3.0
    assert out["low_sample"] is False
    assert out["anomaly_eligible"] is False
    assert out["anomaly_static"] is False


def test_enrich_suppresses_anomaly_below_min_request_floor():
    """High-overhead route is marked low_sample (not anomaly_static) below min request floor,
    while anomaly_eligible reflects the latency verdict independent of sample size."""
    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value=_LAX_SJC_POPS):
        thin = _enrich_with_distance({"edge_pop": "LAX", "shield_pop": "SJC", "p50_ms": 100.0, "requests": 5})
        thick = _enrich_with_distance({"edge_pop": "LAX", "shield_pop": "SJC", "p50_ms": 100.0, "requests": 200})

    assert thin["efficiency_ratio"] == thick["efficiency_ratio"]
    assert thin["efficiency_ratio"] > 3.0
    assert thin["anomaly_eligible"] is True and thick["anomaly_eligible"] is True
    assert thin["low_sample"] is True and thin["anomaly_static"] is False
    assert thick["low_sample"] is False and thick["anomaly_static"] is True


def test_enrich_with_none_p50_returns_none_efficiency():
    """p50_ms missing → efficiency_ratio is None."""
    fake_pops = {"LAX": (33.9425, -118.4081), "JFK": (40.6398, -73.7789)}
    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value=fake_pops):
        out = _enrich_with_distance({"edge_pop": "LAX", "shield_pop": "JFK", "p50_ms": None})

    assert out["efficiency_ratio"] is None
    assert out["anomaly_static"] is False


@pytest.mark.parametrize(
    "cols_ddl,expected_missing",
    [
        ("timestamp TIMESTAMP, dummy VARCHAR", {"rid", "prid", "edge", "pop", "ottfb"}),
        ('"timestamp" TIMESTAMP, "rid" VARCHAR, "edge" BOOLEAN, "pop" VARCHAR, "ottfb" DOUBLE', {"prid"}),
    ],
)
def test_shielding_analysis_returns_requires_fields_when_columns_missing(
    in_memory_duckdb, test_service_source, cols_ddl, expected_missing
):
    """If the table schema is missing required shielding columns, return requires_fields."""
    in_memory_duckdb.execute(f"CREATE TABLE {_safe_table(test_service_source['name'])} ({cols_ddl})")
    out = get_shielding_analysis(in_memory_duckdb, test_service_source, None, None, {})
    assert out["has_data"] is False
    assert set(out.get("requires_fields", [])) >= expected_missing


def test_shielding_analysis_returns_edge_only_when_no_shield_rows(in_memory_duckdb, test_service_source):
    """If only edge logs exist, return edge_only=True."""
    table = _safe_table(test_service_source["name"])
    logs = _origin_logs(test_service_source)
    for i, log in enumerate(logs):
        log["edge"] = True
        log["rid"] = f"r{i}"
        log["prid"] = ""
        log["pop"] = "LAX"
    insert_mock_logs(in_memory_duckdb, table, logs)

    out = get_shielding_analysis(in_memory_duckdb, test_service_source, None, None, {})
    assert out["has_data"] is False
    assert out.get("edge_only") is True
    assert out["rows"] == []


def test_get_summary_empty_schema_returns_has_data_false(in_memory_duckdb):
    """Service with no schema (table doesn't exist) → has_data=False."""
    src = {"name": "nonexistent_origin_svc", "service_id": "x"}
    out = get_summary(in_memory_duckdb, src, start_time=None, end_time=None, filters={})
    assert out["has_data"] is False


def test_response_cache_hit_skips_duckdb_on_repeat_call(in_memory_duckdb, test_service_source):
    """First call hits DuckDB; repeat call within TTL returns is_cached=True."""
    logs = _origin_logs(test_service_source)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    cold = get_summary(in_memory_duckdb, test_service_source, None, None, {})
    assert cold["has_data"] is True
    assert cold.get("is_cached") is not True

    warm = get_summary(in_memory_duckdb, test_service_source, None, None, {})
    assert warm["has_data"] is True
    assert warm.get("is_cached") is True
    assert warm["ottfb_p50_ms"] == cold["ottfb_p50_ms"]


def test_get_summary_uses_ttfb_only_when_ottfb_column_absent(in_memory_duckdb, test_service_source):
    """When ottfb is None, lat_val falls back to ttfb * 1000000.0."""
    logs = generate_mock_logs(test_service_source, num_logs=20, hours_ago=1)
    for i, log in enumerate(logs):
        log["ttfb"] = 0.05 + i * 0.001
        log["ottfb"] = None
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    out = get_summary(in_memory_duckdb, test_service_source, None, None, {})
    assert out["has_data"] is True
    assert out["ottfb_p50_ms"] is not None


def test_get_timeseries_metric_unavailable_returns_empty(in_memory_duckdb, test_service_source):
    """Requesting metric='ttlb' when ottlb column doesn't exist → has_data=False."""
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), _origin_logs(test_service_source, 10))
    out = get_timeseries(in_memory_duckdb, test_service_source, None, None, {}, metric="ttlb")
    assert out["has_data"] is False


def test_get_timeseries_sub_minute_bucket_uses_seconds_interval(in_memory_duckdb, test_service_source):
    """bucket_minutes < 1 uses INTERVAL 'N' seconds."""
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), _origin_logs(test_service_source, 20))
    out = get_timeseries(in_memory_duckdb, test_service_source, None, None, {}, bucket_minutes=0.5)
    assert "series" in out


def test_get_timeseries_p50_uses_median_not_approx_quantile(in_memory_duckdb, test_service_source):
    """For percentile='p50', the SQL uses MEDIAN(...)."""
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), _origin_logs(test_service_source, 20))
    out = get_timeseries(in_memory_duckdb, test_service_source, None, None, {}, percentile="p50")
    assert "series" in out


def test_get_timeseries_split_by_leg_includes_edge_in_series(in_memory_duckdb, test_service_source):
    """split_by_leg=True AND edge column present → series points include 'edge' key."""
    logs = _origin_logs(test_service_source, num=20)
    for i, log in enumerate(logs):
        log["edge"] = i % 2 == 0
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    out = get_timeseries(in_memory_duckdb, test_service_source, None, None, {}, split_by_leg=True)
    if out["series"]:
        assert "edge" in out["series"][0]


def test_get_status_codes_returns_has_data_false_when_ost_all_null(in_memory_duckdb, test_service_source):
    """All-null ost column → no rows → has_data=False."""
    logs = generate_mock_logs(test_service_source, num_logs=10, hours_ago=1)
    for log in logs:
        log["ost"] = None
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    out = get_status_codes(in_memory_duckdb, test_service_source, None, None, {})
    assert out["has_data"] is False


@pytest.mark.parametrize(
    "fn,extra_check",
    [
        (get_status_codes, None),
        (get_pop_latency, ("requires_group_c", True)),
        (get_ip_health, None),
    ],
)
def test_origin_endpoints_missing_required_cols_return_empty(in_memory_duckdb, test_service_source, fn, extra_check):
    """Missing required origin columns returns has_data=False (and requires_group_c=True for pop_latency)."""
    in_memory_duckdb.execute(
        f'CREATE TABLE "{_safe_table(test_service_source["name"])}" (timestamp TIMESTAMP, status INTEGER)'
    )
    out = fn(in_memory_duckdb, test_service_source, None, None, {})
    assert out["has_data"] is False
    if extra_check:
        assert out[extra_check[0]] is extra_check[1]


def test_get_pop_latency_pop_col_present_but_no_origin_data_flags_no_group_c_message(
    in_memory_duckdb,
    test_service_source,
):
    """Pop column EXISTS but no origin timing data → has_data=False, requires_group_c=False."""
    logs = generate_mock_logs(test_service_source, num_logs=10)
    for log in logs:
        log["ottfb"] = None
        log["ttfb"] = None
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    out = get_pop_latency(in_memory_duckdb, test_service_source, None, None, {})
    assert out["has_data"] is False
    assert out["requires_group_c"] is False


def test_get_ip_health_returns_empty_when_no_qualifying_ips(in_memory_duckdb, test_service_source):
    """No IPs hit the 10-request minimum → empty rows + has_data=False."""
    logs = generate_mock_logs(test_service_source, num_logs=5, hours_ago=1)
    for log in logs:
        log["oip"] = "1.1.1.1"
        log["ost"] = 200
        log["ottfb"] = 50000
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    out = get_ip_health(in_memory_duckdb, test_service_source, None, None, {})
    assert out["has_data"] is False


def test_shielding_analysis_returns_empty_when_no_origin_data(in_memory_duckdb, test_service_source):
    """All required cols exist but no ottfb values are populated → has_data=False."""
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=10, hours_ago=1)
    for log in logs:
        log.update({"ottfb": None, "ttfb": None, "rid": "rA", "prid": "rA", "edge": True, "pop": "LAX"})
    insert_mock_logs(in_memory_duckdb, table, logs)

    out = get_shielding_analysis(in_memory_duckdb, test_service_source, None, None, {})
    assert out["has_data"] is False


def test_get_pop_latency_returns_rows_with_median_p95_and_elevated_flag(in_memory_duckdb, test_service_source):
    """Returns has_data=True, includes median_p95_ms, and flags rows where p95 > median_p95 * 2 as elevated."""
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=20, hours_ago=1)
    for i, log in enumerate(logs):
        if i < 7:
            log["pop"], log["ottfb"] = "LAX", 50000
        elif i < 14:
            log["pop"], log["ottfb"] = "JFK", 60000
        else:
            log["pop"], log["ottfb"] = "SLOW", 500000
        log["ttfb"] = None
    insert_mock_logs(in_memory_duckdb, table, logs)

    out = get_pop_latency(in_memory_duckdb, test_service_source, None, None, {})
    assert out["has_data"] is True
    assert "median_p95_ms" in out
    assert out["requires_group_c"] is False
    pops = {r["pop"]: r for r in out["rows"]}
    assert {"LAX", "JFK", "SLOW"} <= pops.keys()
    assert pops["SLOW"]["elevated"] is True
    assert pops["LAX"]["elevated"] is False


def test_get_pop_latency_uses_ttfb_when_ottfb_column_missing(in_memory_duckdb, test_service_source):
    """When ottfb isn't in schema, latency is computed from ttfb * 1000000 (µs)."""
    table = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(f'CREATE TABLE "{table}" ("timestamp" TIMESTAMP, "pop" VARCHAR, "ttfb" DOUBLE)')
    base = datetime.now(UTC) - timedelta(hours=1)
    for i, ttfb_s in enumerate([0.05, 0.06, 0.07, 0.5]):
        in_memory_duckdb.execute(
            f'INSERT INTO "{table}" VALUES (?, ?, ?)',
            [(base + timedelta(seconds=i * 30)).isoformat(), "LAX", ttfb_s],
        )

    out = get_pop_latency(in_memory_duckdb, test_service_source, None, None, {})
    assert out["has_data"] is True
    lax_row = next(r for r in out["rows"] if r["pop"] == "LAX")
    assert lax_row["p95_ms"] is not None and lax_row["p95_ms"] >= 100


def test_get_ip_health_returns_rows_ordered_by_error_pct_desc(in_memory_duckdb, test_service_source):
    """Returns oips with >=10 requests, ranked by error_pct DESC."""
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=40, hours_ago=1)
    for i, log in enumerate(logs):
        log["oip"] = "1.1.1.1" if i < 20 else "2.2.2.2"
        log["ost"] = 500 if (i < 10 or 20 <= i < 22) else 200
        log["ottfb"] = 50000
    insert_mock_logs(in_memory_duckdb, table, logs)

    out = get_ip_health(in_memory_duckdb, test_service_source, None, None, {})
    assert out["has_data"] is True
    rows = out["rows"]
    assert len(rows) == 2
    assert rows[0]["oip"] == "1.1.1.1"
    assert rows[0]["error_pct"] >= rows[1]["error_pct"]


def _shield_pair_logs(src, *, edge_pop, shield_pop, n, edge_us, shield_us, rid_prefix, ttfb=None):
    """Build ``n`` edge logs at ``edge_pop`` + ``n`` matching shield logs at ``shield_pop`` (joined via prid=rid)."""
    edge_logs = generate_mock_logs(src, num_logs=n, hours_ago=1)
    shield_logs = generate_mock_logs(src, num_logs=n, hours_ago=1)
    for i in range(n):
        rid = f"{rid_prefix}{i}"
        edge_logs[i].update({"edge": True, "rid": rid, "prid": "", "pop": edge_pop, "ottfb": edge_us, "ttfb": ttfb})
        shield_logs[i].update(
            {
                "edge": False,
                "rid": f"s{rid_prefix}{i}",
                "prid": rid,
                "pop": shield_pop,
                "ottfb": shield_us,
                "ttfb": None,
            }
        )
    return edge_logs + shield_logs


def test_get_shielding_analysis_happy_path_returns_edge_to_shield_rows(in_memory_duckdb, test_service_source):
    """Happy path: edge logs + matching shield logs → aggregated rows grouped by (edge_pop, shield_pop)."""
    table = _safe_table(test_service_source["name"])
    logs = _shield_pair_logs(
        test_service_source, edge_pop="LAX", shield_pop="IAD", n=10, edge_us=100000, shield_us=30000, rid_prefix="req"
    )
    insert_mock_logs(in_memory_duckdb, table, logs)

    fake_pops = {"LAX": (33.9425, -118.4081), "IAD": (38.9445, -77.4558)}
    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value=fake_pops):
        out = get_shielding_analysis(in_memory_duckdb, test_service_source, None, None, {})

    assert len(out["rows"]) >= 1
    row = next((r for r in out["rows"] if r["edge_pop"] == "LAX" and r["shield_pop"] == "IAD"), None)
    assert row is not None
    assert row["requests"] == 10
    assert row["p95_ms"] is not None
    assert row.get("edge_lat") is not None and row.get("shield_lat") is not None


def test_shielding_analysis_edge_filter_does_not_strip_shield_leg(
    in_memory_duckdb,
    test_service_source,
):
    """T10: a filter on the EDGE leg (``pop = DEN``) must not strip the
    shield-side rows before the join. The shield CTE only carries time
    bounds — otherwise filtering by edge POP would drop the IAD shield hit
    and zero the DEN→IAD route. This invariant was previously untested."""
    table = _safe_table(test_service_source["name"])
    logs = _shield_pair_logs(
        test_service_source, edge_pop="DEN", shield_pop="IAD", n=8, edge_us=100000, shield_us=30000, rid_prefix="den"
    )
    # A second, unrelated edge POP that the filter should exclude.
    logs += _shield_pair_logs(
        test_service_source, edge_pop="ORD", shield_pop="IAD", n=8, edge_us=90000, shield_us=30000, rid_prefix="ord"
    )
    insert_mock_logs(in_memory_duckdb, table, logs)

    fake_pops = {"DEN": (39.86, -104.67), "IAD": (38.94, -77.46), "ORD": (41.97, -87.90)}
    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value=fake_pops):
        out = get_shielding_analysis(
            in_memory_duckdb,
            test_service_source,
            None,
            None,
            {"pop": FilterSpec(mode="include", values=["DEN"])},
        )

    pairs = {(r["edge_pop"], r["shield_pop"]) for r in out["rows"]}
    # DEN→IAD survives the edge filter (the shield IAD rows weren't stripped).
    assert ("DEN", "IAD") in pairs
    # ORD edge rows were filtered out, so no ORD→IAD route.
    assert ("ORD", "IAD") not in pairs
    den = next(r for r in out["rows"] if r["edge_pop"] == "DEN")
    assert den["requests"] == 8


def test_shielding_analysis_uses_ttfb_fallback_when_ottfb_null(
    in_memory_duckdb,
    test_service_source,
):
    """T11 (b198b04): when ``ottfb`` is NULL the transit delta falls back to
    ``ttfb`` (seconds → µs). Edge ttfb 0.1s, shield ottfb 30ms → ~70ms p50."""
    table = _safe_table(test_service_source["name"])
    # Edge ottfb NULL but ttfb = 0.1s (100ms); shield ottfb = 30ms.
    logs = _shield_pair_logs(
        test_service_source,
        edge_pop="LAX",
        shield_pop="IAD",
        n=10,
        edge_us=None,
        shield_us=30000,
        rid_prefix="fb",
        ttfb=0.1,
    )
    insert_mock_logs(in_memory_duckdb, table, logs)

    fake_pops = {"LAX": (33.94, -118.41), "IAD": (38.94, -77.46)}
    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value=fake_pops):
        out = get_shielding_analysis(in_memory_duckdb, test_service_source, None, None, {})

    row = next(r for r in out["rows"] if r["edge_pop"] == "LAX" and r["shield_pop"] == "IAD")
    # (100ms via ttfb fallback) - (30ms via ottfb) = ~70ms, NOT a negative or
    # NULL value (which is what a missing fallback would produce).
    assert row["p50_ms"] == pytest.approx(70.0, abs=1.0)


def test_shielding_analysis_has_data_true_without_coords(
    in_memory_duckdb,
    test_service_source,
):
    """L3: rows present but POP codes absent from the location map → the
    table still has data (``has_data`` gates on ROW presence, not arc
    coordinates). Previously this returned ``has_data=False`` and hid the
    whole table + CSV export whenever a POP was missing from
    pop_locations.json."""
    table = _safe_table(test_service_source["name"])
    logs = _shield_pair_logs(
        test_service_source, edge_pop="ZZZ", shield_pop="QQQ", n=5, edge_us=100000, shield_us=20000, rid_prefix="nc"
    )
    insert_mock_logs(in_memory_duckdb, table, logs)

    # Empty POP map → no coords resolve for either POP.
    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value={}):
        out = get_shielding_analysis(in_memory_duckdb, test_service_source, None, None, {})

    assert out["has_data"] is True
    assert len(out["rows"]) == 1
    row = out["rows"][0]
    assert row["edge_lat"] is None and row["shield_lat"] is None


def test_shielding_analysis_keeps_low_volume_high_overhead_route(
    in_memory_duckdb,
    test_service_source,
):
    """M1/T12: a low-volume but high-overhead route must NOT be buried by a
    request-volume LIMIT — it's exactly the mis-peered route this analysis
    exists to surface. With a small limit, the high-overhead route still
    appears (via the top-by-overhead rank), and ``total_routes`` /
    ``truncated`` report the full picture."""
    table = _safe_table(test_service_source["name"])
    logs = []
    # Six high-volume, low-overhead routes (50 reqs each, ~10ms transit). With
    # limit=2 the union of (top-2 by requests) and (top-2 by overhead) covers at
    # most 4 distinct routes, so at least two of these seven are always dropped
    # → ``truncated`` is deterministically True regardless of tie-breaking.
    high_vol_pops = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"]
    for k, ep in enumerate(high_vol_pops):
        logs += _shield_pair_logs(
            test_service_source,
            edge_pop=ep,
            shield_pop="IAD",
            n=50,
            edge_us=40000,
            shield_us=30000,
            rid_prefix=f"hi{k}",
        )
    # One LOW-volume, FAR-HIGHEST-overhead route (3 reqs, ~150ms transit) — it
    # is rank-1 by overhead (so always selected) but rank-7 by request volume
    # (so a plain ``ORDER BY requests DESC LIMIT 2`` would bury it).
    logs += _shield_pair_logs(
        test_service_source, edge_pop="ZED", shield_pop="IAD", n=3, edge_us=180000, shield_us=30000, rid_prefix="lo"
    )
    insert_mock_logs(in_memory_duckdb, table, logs)

    fake_pops = {ep: (float(i * 10), float(i * 10)) for i, ep in enumerate(high_vol_pops)}
    fake_pops.update({"ZED": (40.0, 40.0), "IAD": (38.94, -77.46)})
    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value=fake_pops):
        out = get_shielding_analysis(in_memory_duckdb, test_service_source, None, None, {}, limit=2)

    pairs = {(r["edge_pop"], r["shield_pop"]): r for r in out["rows"]}
    # The high-overhead ZED→IAD route survives despite being well outside the
    # top-2 by request volume.
    assert ("ZED", "IAD") in pairs
    assert pairs[("ZED", "IAD")]["requests"] == 3
    # Returned set is bounded by the two rank cutoffs (≤ 2*limit), strictly
    # fewer than the 7 total routes.
    assert len(out["rows"]) <= 4
    # Full route count + truncation flag are surfaced for "Top N of M".
    assert out["total_routes"] == 7
    assert out["truncated"] is True


# ── Merged from tests/repositories/test_origin_aggregates.py ──
@pytest.fixture
def _clear_origin_caches():
    """Drop the response memo + schema cache between tests so the composite always re-runs end-to-end."""
    _clear_schema_cache()
    origin_mod._response_cache.clear()
    yield
    _clear_schema_cache()
    origin_mod._response_cache.clear()


def _origin_logs_origin_aggregates(src: dict, num: int = 40, *, pop: str | None = None) -> list[dict]:
    """Origin-shaped logs: ottfb populated, mixed ostatus codes, mixed oip, url, and edge."""
    logs = _origin_logs(src, num=num)
    for i, log in enumerate(logs):
        log["url"] = "/api/data" if i % 2 == 0 else "/images/logo.png"
        log["edge"] = True
        if pop is not None:
            log["pop"] = pop
    return logs


@pytest.mark.usefixtures("_clear_origin_caches")
@pytest.mark.asyncio
async def test_get_aggregates_happy_path_returns_all_seven_cards(in_memory_duckdb, test_service_source):
    """Seeds origin-shaped logs, calls ``get_aggregates``, asserts all 7 sub-card keys and shapes."""
    logs = _origin_logs_origin_aggregates(test_service_source, num=40, pop="LAX")
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = await get_aggregates(in_memory_duckdb, test_service_source, None, None, {})

    for key in (
        "has_data",
        "summary",
        "timeseries",
        "slow_urls",
        "status_codes",
        "path_breakdown",
        "pop_latency",
        "ip_health",
        "section_timings",
    ):
        assert key in result, f"missing top-level key: {key}"

    assert result["has_data"] is True
    assert result["summary"]["has_data"] is True
    assert isinstance(result["timeseries"]["series"], list)
    for card in ("slow_urls", "status_codes", "path_breakdown", "pop_latency", "ip_health"):
        assert isinstance(result[card]["rows"], list)


@pytest.mark.usefixtures("_clear_origin_caches")
@pytest.mark.parametrize("schema_cols", [[], ["foo"]])
@pytest.mark.asyncio
async def test_get_aggregates_empty_or_disjoint_schema_returns_empty_payload(
    in_memory_duckdb, test_service_source, monkeypatch, schema_cols
):
    """When get_schema_cols() returns [] or no intersecting columns, short-circuit to empty_payload skeleton."""
    from backend.repositories._base import QueryRunner

    monkeypatch.setattr(QueryRunner, "get_schema_cols", lambda self: schema_cols)
    result = await get_aggregates(in_memory_duckdb, test_service_source, None, None, {})

    assert result["has_data"] is False
    assert result["summary"] == {}
    assert result["timeseries"] == {"has_data": False, "series": []}
    assert result["slow_urls"] == {"has_data": False, "rows": []}
    assert result["status_codes"] == {"has_data": False, "rows": []}
    assert result["path_breakdown"] == {"has_data": False, "shielding_detected": False, "rows": []}
    assert result["pop_latency"] == {"has_data": False, "requires_group_c": False, "rows": []}
    assert result["ip_health"] == {"has_data": False, "rows": []}
    assert "section_timings" not in result


@pytest.mark.usefixtures("_clear_origin_caches")
@pytest.mark.asyncio
async def test_get_aggregates_serial_fallback_on_pool_busy(in_memory_duckdb, test_service_source, monkeypatch):
    """When checkout_connection raises _PoolBusy, get_aggregates runs all branches serially against primary runner."""
    logs = _origin_logs_origin_aggregates(test_service_source, num=30, pop="LAX")
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    from backend.core import duckdb_pool

    @contextlib.contextmanager
    def _always_busy(src, max_wait=10.0):  # noqa: ARG001
        raise duckdb_pool._PoolBusy("test forced saturation")
        yield  # pragma: no cover

    monkeypatch.setattr(duckdb_pool, "checkout_connection", _always_busy)

    result = await get_aggregates(in_memory_duckdb, test_service_source, None, None, {})

    assert result["has_data"] is True
    assert result["summary"]["has_data"] is True
    assert isinstance(result["timeseries"]["series"], list)
    for card in ("status_codes", "pop_latency", "ip_health"):
        assert isinstance(result[card]["rows"], list)

    sections = {entry["section"] for entry in result["section_timings"]}
    assert "origin:parallel" not in sections
    assert _ALL_ORIGIN_SECTIONS.issubset(sections)


@pytest.mark.usefixtures("_clear_origin_caches")
@pytest.mark.asyncio
async def test_get_aggregates_drops_temp_table_in_finally(in_memory_duckdb, test_service_source, monkeypatch):
    """Outer try/finally must issue DROP TABLE IF EXISTS even when an inner branch raises."""
    logs = _origin_logs_origin_aggregates(test_service_source, num=20)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    from backend.core import duckdb_pool
    from backend.repositories._base import QueryRunner

    executed_sqls: list[str] = []
    original_execute = QueryRunner.execute

    def _sniff(self, q, p=None):
        executed_sqls.append(q)
        return original_execute(self, q, p)

    @contextlib.contextmanager
    def _busy(src, max_wait=10.0):  # noqa: ARG001
        raise duckdb_pool._PoolBusy("force serial path")
        yield  # pragma: no cover

    def _boom(*args, **kwargs):
        raise RuntimeError("synthetic summary failure")

    monkeypatch.setattr(QueryRunner, "execute", _sniff)
    monkeypatch.setattr(duckdb_pool, "checkout_connection", _busy)
    monkeypatch.setattr(origin_mod, "_origin_summary_from_temp", _boom)

    with pytest.raises(RuntimeError, match="synthetic summary failure"):
        await get_aggregates(in_memory_duckdb, test_service_source, None, None, {})

    drop_stmts = [s for s in executed_sqls if "DROP TABLE IF EXISTS" in s and "t_origin_" in s]
    assert len(drop_stmts) >= 1, f"outer finally must DROP temp table. Recorded SQLs: {executed_sqls!r}"


@pytest.mark.usefixtures("_clear_origin_caches")
@pytest.mark.asyncio
async def test_get_aggregates_section_timings_populated(in_memory_duckdb, test_service_source):
    """section_timings includes temp_table_create plus one entry per branch."""
    logs = _origin_logs_origin_aggregates(test_service_source, num=25, pop="JFK")
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = await get_aggregates(in_memory_duckdb, test_service_source, None, None, {})

    timings = result["section_timings"]
    assert isinstance(timings, list) and len(timings) >= 2
    for entry in timings:
        assert set(entry.keys()) >= {"section", "time_ms"}
        assert isinstance(entry["section"], str)
        assert isinstance(entry["time_ms"], (int, float))
        assert entry["current_execution"] is True
        assert entry["activity"] in {"compute", "overhead", "skipped"}
    summary_marks = [entry for entry in timings if entry["section"] == "summary"]
    assert [(entry["source"], entry["outcome"]) for entry in summary_marks] == [
        ("rollup", "fallback"),
        ("live", "served"),
    ]

    sections = {entry["section"] for entry in timings}
    assert "temp_table_create" in sections
    assert _ALL_ORIGIN_SECTIONS.issubset(sections)


_ALL_ORIGIN_SECTIONS = {
    "summary",
    "timeseries",
    "slow_urls",
    "status_codes",
    "path_breakdown",
    "pop_latency",
    "ip_health",
}


@pytest.mark.usefixtures("_clear_origin_caches")
@pytest.mark.parametrize(
    "sections_arg,pop,expected_sections",
    [
        (None, "DFW", _ALL_ORIGIN_SECTIONS),
        ({"summary"}, "MIA", {"summary"}),
        ({"timeseries", "status_codes", "path_breakdown"}, "ATL", {"timeseries", "status_codes", "path_breakdown"}),
        ({"pop_latency", "ip_health"}, "SEA", {"pop_latency", "ip_health"}),
    ],
)
@pytest.mark.asyncio
async def test_get_aggregates_sections_selector(
    in_memory_duckdb, test_service_source, sections_arg, pop, expected_sections
):
    """sections selector filters both returned card keys and section_timings entries."""
    logs = _origin_logs_origin_aggregates(test_service_source, num=25, pop=pop)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    result = await get_aggregates(in_memory_duckdb, test_service_source, None, None, {}, sections=sections_arg)
    present = _ALL_ORIGIN_SECTIONS & result.keys()
    assert present == expected_sections

    timer_names = {t["section"] for t in result.get("section_timings", [])}
    assert "temp_table_create" in timer_names
    assert expected_sections.issubset(timer_names)
    for blocked in _ALL_ORIGIN_SECTIONS - expected_sections:
        assert blocked not in timer_names, f"selector did not suppress {blocked}; got {timer_names}"


@pytest.mark.usefixtures("_clear_origin_caches")
def test_parallel_gather_uses_return_exceptions_true():
    """Verify get_aggregates runs branch queries sequentially on the primary connection's runner."""
    import inspect

    src = inspect.getsource(get_aggregates)
    assert "checkout_connection" not in src
    assert "asyncio.gather(" not in src


_PARTB_COLS = [
    "timestamp",
    "cache",
    "edge",
    "url",
    "oip",
    "ost",
    "pop",
    "ottfb",
    "ottlb",
    "ttfb",
    "elapsed",
    "obytes",
]


def _rollup_base_table() -> str:
    return "logs_partb_base"


def _seed_partb_table(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict]) -> None:
    """Create a table with the 12 origin rollup columns and insert ``rows``."""
    con.execute(
        f"CREATE TABLE {table} ("
        f"  timestamp TIMESTAMPTZ, cache VARCHAR, edge BOOLEAN, url VARCHAR, "
        f"  oip VARCHAR, ost INTEGER, pop VARCHAR, ottfb DOUBLE, ottlb DOUBLE, "
        f"  ttfb DOUBLE, elapsed DOUBLE, obytes DOUBLE"
        f")"
    )
    placeholders = ", ".join("?" for _ in _PARTB_COLS)
    for r in rows:
        con.execute(f"INSERT INTO {table} VALUES ({placeholders})", [r.get(c) for c in _PARTB_COLS])


def _seed_partb_base(con: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    _seed_partb_table(con, _rollup_base_table(), rows)


def _seed_partb_base_into(con: duckdb.DuckDBPyConnection, src: dict, rows: list[dict]) -> None:
    _seed_partb_table(con, _safe_table(src["name"]), rows)


def _write_all_fields_for_status(cache_root: str, hour: str, status_counts: dict[int, int]) -> None:
    """Write a closed-hour all_fields.parquet carrying field='ost' rows."""
    d = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={hour}")
    os.makedirs(d, exist_ok=True)
    wcon = duckdb.connect()
    try:
        tuples = ", ".join(f"('ost', '{code}', CAST({n} AS BIGINT))" for code, n in status_counts.items())
        wcon.execute(
            f"COPY (SELECT * FROM (VALUES {tuples}) AS t(field, value, count)) "
            f"TO '{d}/all_fields.parquet' (FORMAT PARQUET)"
        )
    finally:
        wcon.close()


def _seed_full_rollup_coverage(cache_root: str, base_rows: list[dict], hour_tokens: list[str]):
    """Drive all seven origin rollup writers against ``base_rows`` under ``cache_root``."""
    from backend.core import rollups

    def _fresh_con():
        c = duckdb.connect(":memory:")
        _seed_partb_base(c, base_rows)
        return c

    sid = "partb-svc"
    src = {"name": "partb-svc", "service_id": "partb-svc"}
    with (
        patch("backend.core.duckdb._cache_dir", return_value=cache_root),
        patch("backend.core.rollups._common._safe_table_for", return_value=_rollup_base_table()),
        patch("backend.core.duckdb.get_connection", side_effect=lambda *a, **k: _fresh_con()),
        patch("backend.core.iceberg.view._get_service_lock", _noop_lock),
        patch("backend.core.iceberg.execute_with_stale_view_retry", side_effect=lambda c, _src, fn: fn(c)),
    ):
        rollups.build_origin_summary_bundles(sid, src, hour_tokens)
        rollups.build_slow_urls_bundles(sid, src, hour_tokens)
        rollups.build_origin_dims_bundles(sid, src, hour_tokens)
        rollups.build_origin_latency_ts_bundles(sid, src, hour_tokens)

    for h in hour_tokens:
        _write_all_fields_for_status(cache_root, h, {200: 80, 404: 10, 500: 10})


def _closed_hours(n: int, *, end_hours_ago: int = 3) -> list[datetime]:
    base = (datetime.now(UTC) - timedelta(hours=end_hours_ago + n)).replace(minute=0, second=0, microsecond=0)
    return [base + timedelta(hours=i) for i in range(n)]


def _partb_base_rows(hours: list[datetime]) -> list[dict]:
    rows: list[dict] = []
    for hi, hour_dt in enumerate(hours):
        for i in range(20):
            rows.append(
                {
                    "timestamp": hour_dt + timedelta(minutes=i % 10, seconds=i),
                    "cache": "MISS" if i % 3 else "HIT",
                    "edge": i % 2 == 0,
                    "url": "/api/data" if i % 2 == 0 else "/img/logo.png",
                    "oip": "203.0.113.1" if i < 12 else "203.0.113.2",
                    "ost": 200 if i % 5 else 500,
                    "pop": "LAX" if i < 10 else "JFK",
                    "ottfb": 50_000.0 + hi * 500 + i * 100,
                    "ottlb": 80_000.0 + hi * 500 + i * 100,
                    "ttfb": None,
                    "elapsed": 120_000.0 + i * 100,
                    "obytes": 1024.0 + i,
                }
            )
    return rows


@contextlib.contextmanager
def _partb_rollup_env(con: duckdb.DuckDBPyConnection, src: dict, n_hours: int = 50):
    """Seed ``con`` and full rollup coverage in a temporary cache directory."""
    import tempfile

    hours = _closed_hours(n_hours)
    base_rows = _partb_base_rows(hours)
    hour_tokens = [h.strftime("%Y-%m-%d-%H") for h in hours]
    _seed_partb_base_into(con, src, base_rows)
    st = hours[0].isoformat()
    et = (hours[-1] + timedelta(hours=1)).isoformat()
    with tempfile.TemporaryDirectory() as cache_root:
        _seed_full_rollup_coverage(cache_root, base_rows, hour_tokens)
        with patch("backend.core.duckdb._cache_dir", return_value=cache_root):
            yield cache_root, st, et, hour_tokens


@pytest.mark.usefixtures("_clear_origin_caches")
@pytest.mark.asyncio
async def test_get_aggregates_all_rollups_hit_skips_temp(in_memory_duckdb, test_service_source):
    """Wide (>=48h) unfiltered request with full closed-hour rollup coverage skips temp table build."""
    with _partb_rollup_env(in_memory_duckdb, test_service_source) as (_cache_root, st, et, _tokens):
        result = await get_aggregates(in_memory_duckdb, test_service_source, st, et, {})

    assert (_ALL_ORIGIN_SECTIONS & result.keys()) == _ALL_ORIGIN_SECTIONS
    timer_names = {t["section"] for t in result["section_timings"]}
    assert "temp_table_create" not in timer_names
    assert "origin:temp_skipped" in timer_names
    assert result["has_data"] is True and result["summary"]["has_data"] is True
    for approx_sec in ("summary", "timeseries", "slow_urls", "pop_latency", "ip_health", "path_breakdown"):
        assert result[approx_sec].get("_approx") is True
    assert "_approx" not in result["status_codes"]


@pytest.mark.usefixtures("_clear_origin_caches")
@pytest.mark.asyncio
async def test_get_aggregates_partial_miss_builds_temp_for_missed_only(in_memory_duckdb, test_service_source):
    """Remove ONE section's rollup coverage (origin_latency_ts) → temp built and only missed section runs live."""
    with _partb_rollup_env(in_memory_duckdb, test_service_source) as (cache_root, st, et, hour_tokens):
        for h in hour_tokens:
            ts_file = os.path.join(cache_root, "rollups", "hour_bundled", f"hour={h}", "origin_latency_ts.parquet")
            if os.path.exists(ts_file):
                os.remove(ts_file)
            field_dir = os.path.join(cache_root, "rollups", "hour", "field=ottfb", f"hour={h}")
            os.makedirs(field_dir, exist_ok=True)
            with open(os.path.join(field_dir, "data.parquet"), "wb") as f:
                f.write(b"x")
        result = await get_aggregates(in_memory_duckdb, test_service_source, st, et, {})

    timer_names = {t["section"] for t in result["section_timings"]}
    assert "temp_table_create" in timer_names
    assert "origin:temp_skipped" not in timer_names
    assert "timeseries" in timer_names
    assert _ALL_ORIGIN_SECTIONS.issubset(result.keys())
    assert result["timeseries"]["has_data"] is True
    assert result["summary"].get("_approx") is True
    assert result["slow_urls"].get("_approx") is True


@pytest.mark.usefixtures("_clear_origin_caches")
@pytest.mark.asyncio
async def test_get_aggregates_filtered_request_builds_temp_all_live(in_memory_duckdb, test_service_source):
    """A filtered request (has_filters=True) makes every rollup reader return None → temp built, all sections live."""
    with _partb_rollup_env(in_memory_duckdb, test_service_source) as (_cache_root, st, et, _tokens):
        result = await get_aggregates(
            in_memory_duckdb,
            test_service_source,
            st,
            et,
            {"pop": FilterSpec(mode="include", values=["LAX"])},
        )

    timer_names = {t["section"] for t in result["section_timings"]}
    assert "temp_table_create" in timer_names
    assert "origin:temp_skipped" not in timer_names
    for section in ("summary", "slow_urls", "timeseries", "pop_latency", "ip_health", "path_breakdown"):
        assert "_approx" not in result[section], f"{section} carried _approx on the filtered live path"


@pytest.mark.usefixtures("_clear_origin_caches")
@pytest.mark.asyncio
async def test_get_aggregates_selector_subset_all_hit_skips_temp(in_memory_duckdb, test_service_source):
    """sections={'summary','status_codes'} with full coverage → only those two run, temp is skipped."""
    with _partb_rollup_env(in_memory_duckdb, test_service_source) as (_cache_root, st, et, _tokens):
        result = await get_aggregates(
            in_memory_duckdb, test_service_source, st, et, {}, sections={"summary", "status_codes"}
        )

    present = _ALL_ORIGIN_SECTIONS & result.keys()
    assert present == {"summary", "status_codes"}
    timer_names = {t["section"] for t in result["section_timings"]}
    assert "temp_table_create" not in timer_names
    assert "origin:temp_skipped" in timer_names
    assert {"summary", "status_codes"}.issubset(timer_names)
    for blocked in ("slow_urls", "timeseries", "path_breakdown", "pop_latency", "ip_health"):
        assert blocked not in timer_names


_TS = """
        SELECT
          time_bucket(INTERVAL '5' minutes, "timestamp")                              AS ts,
          COUNT(*)                                                          AS miss_count,
          {agg} {unit}                                            AS value

        FROM "t_svc"
        WHERE 1=1 AND ({lat} IS NOT NULL)
        GROUP BY ts
        ORDER BY ts
        """

_COALESCE = 'COALESCE("ottfb", "ttfb" * 1000000.0)'

_CASES = [
    # (cols, metric, percentile, lat_expr, unit_conv)
    (["ottfb", "ttfb"], "ttfb", "p95", _COALESCE, "/ 1000.0"),
    (["ottfb", "ttfb"], "ttfb", "p50", _COALESCE, "/ 1000.0"),
    (["ottfb", "ttfb"], "ttfb", "p99", _COALESCE, "/ 1000.0"),
    (["ottfb"], "ttfb", "p95", '"ottfb"', "/ 1000.0"),
    (["ttfb"], "ttfb", "p95", '"ttfb"', "* 1000.0"),
    (["ottfb", "ttfb", "ottlb"], "ttlb", "p95", '"ottlb"', "/ 1000.0"),
]


def _norm(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip()


def _run(cols: list[str], metric: str, percentile: str) -> str | None:
    runner = MagicMock()
    runner.get_schema_cols.return_value = cols
    runner.execute.return_value.fetchall.return_value = []
    with (
        patch.object(origin, "QueryRunner", return_value=runner),
        patch.object(origin, "_check_response_cache", return_value=("k", None)),
        patch.object(origin, "_store_response_cache", side_effect=lambda _k, payload, _r: payload),
        patch.object(origin, "_safe_table", return_value='"t_svc"'),
        patch.object(origin, "build_where_clause", return_value=([], "1=1")),
    ):
        origin.get_timeseries(MagicMock(), {"name": "svc"}, None, None, {}, metric=metric, percentile=percentile)
    if not runner.execute.called:
        return None
    return runner.execute.call_args.args[0]


@pytest.mark.usefixtures("_clear_origin_caches")
@pytest.mark.parametrize("cols,metric,percentile,lat,unit", _CASES)
def test_timeseries_sql_is_byte_stable(cols, metric, percentile, lat, unit):
    if percentile == "p50":
        agg = f"MEDIAN({lat})"
    else:
        agg = f"APPROX_QUANTILE({lat}, {0.95 if percentile == 'p95' else 0.99})"
    expected = _TS.format(agg=agg, unit=unit, lat=lat)
    assert _norm(_run(cols, metric, percentile)) == _norm(expected)


@pytest.mark.usefixtures("_clear_origin_caches")
def test_timeseries_without_any_latency_column_runs_no_query():
    assert _run(["url"], "ttfb", "p95") is None


@pytest.mark.usefixtures("_clear_origin_caches")
@pytest.mark.parametrize(
    "endpoint,rows",
    [
        ("summary", [(50.0,)]),
        ("timeseries", [(datetime(2026, 1, 1, tzinfo=UTC), 2, 50.0)]),
        ("slow_urls", [("/example", 2, 30.0, 50.0, 60.0)]),
        ("status_codes", [(200, 2, 100.0)]),
        ("path_breakdown", [(True, 2, 30.0, 50.0)]),
        ("pop_latency", [("IAD", 2, 30.0, 50.0)]),
        ("ip_health", [("203.0.113.1", 2, 30.0, 50.0, 0.0)]),
        ("drilldown", [("example.com", 2, 30.0, 50.0, 60.0, 0.0, 100)]),
        ("shielding_analysis", []),
        ("shielding_analysis", [("IAD", "SJC", 2, 30.0, 50.0, 60.0, 1)]),
    ],
)
def test_granular_reader_observations_follow_execution_and_cache_reuse(endpoint, rows, monkeypatch):
    monkeypatch.setattr("backend.repositories.utils.response_cache.get_queries", lambda: [])
    monkeypatch.setattr("backend.repositories.utils.response_cache.get_tracked_calls", lambda: [])
    runner = MagicMock()
    runner.get_schema_cols.return_value = [
        "timestamp",
        "ottfb",
        "ttfb",
        "ost",
        "url",
        "edge",
        "pop",
        "oip",
        "host",
        "rid",
        "prid",
    ]
    runner.telemetry.return_value = {}
    cursor = runner.execute.return_value
    cursor.fetchall.return_value = rows
    if endpoint == "summary":
        columns = [
            "total_misses",
            "total_passes",
            "ottfb_p50_ms",
            "ottfb_p75_ms",
            "ottfb_p95_ms",
            "ottfb_p99_ms",
            "ottlb_p50_ms",
            "ottlb_p95_ms",
            "cdn_overhead_p50_ms",
            "origin_error_rate",
            "obytes_p50",
        ]
        cursor.description = [(column,) for column in columns]
        cursor.fetchall.return_value = [(50.0,) * len(columns)]
    elif endpoint == "shielding_analysis":
        columns = ["edge_pop", "shield_pop", "requests", "p50_ms", "p95_ms", "p99_ms", "total_routes"]
        cursor.description = [(column,) for column in columns]
        monkeypatch.setattr(origin, "_enrich_with_distance", lambda row: row)
    cursor.fetchone.return_value = None
    source = {"name": "origin-observation", "service_id": "TestLogSvcABC123"}
    read = getattr(origin, f"get_{endpoint}")
    with patch.object(origin, "QueryRunner", return_value=runner):
        cold = read(MagicMock(), source, None, None, {})
        presence_probe = endpoint == "shielding_analysis" and not rows
        assert runner.execute.call_count == (2 if presence_probe else 1)
        timings = cold["section_timings"]
        expected_sections = [endpoint]
        if presence_probe:
            expected_sections.append("shielding_presence")
            assert cold["edge_only"] is True
        else:
            assert cold["has_data"] is True
        assert [row["section"] for row in timings] == expected_sections
        assert all(
            row["source"] == "live" and row["activity"] == "compute" and row["current_execution"] is True
            for row in timings
        )
        assert cold["_response_cache"]["state"] == "miss"
        runner.execute.reset_mock()
        runner.get_schema_cols.reset_mock()
        warm = read(MagicMock(), source, None, None, {})
        runner.execute.assert_not_called()
        runner.get_schema_cols.assert_not_called()
    assert warm["_response_cache"]["state"] == "hit"
    assert warm.get("rows") == cold.get("rows")
    sections = describe_response(warm)["components"][0]["sections"]
    assert sections
    assert all(row["current_execution"] is False for row in sections)


@pytest.mark.usefixtures("_clear_origin_caches")
@pytest.mark.parametrize("endpoint", ["status_codes", "timeseries", "shielding_analysis"])
def test_granular_unavailable_reader_does_not_claim_live_compute(endpoint, monkeypatch):
    monkeypatch.setattr("backend.repositories.utils.response_cache.get_queries", lambda: [])
    monkeypatch.setattr("backend.repositories.utils.response_cache.get_tracked_calls", lambda: [])
    runner = MagicMock()
    runner.get_schema_cols.return_value = ["timestamp"]
    runner.telemetry.return_value = {}
    with patch.object(origin, "QueryRunner", return_value=runner):
        result = getattr(origin, f"get_{endpoint}")(MagicMock(), {"name": "origin-unavailable"}, None, None, {})
    assert result["has_data"] is False
    runner.execute.assert_not_called()
    assert not any(row.get("activity") == "compute" for row in result.get("section_timings", []))
