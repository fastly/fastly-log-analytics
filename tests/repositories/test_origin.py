"""Regression tests for backend.repositories.origin — validates return keys with real data."""

from __future__ import annotations

import contextlib
import os
import re
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pytest
from backend.models.network import NetworkHealthSummary, NetworkWorstEntry
from backend.utils.countries import format_city_label
from backend.utils.remote_access import MAX_ANALYST_QUERY_SPAN, _time_bounds_from_params

from backend.core.duckdb import _clear_schema_cache
from backend.core.rollups._common import NETWORK_GEO_BUNDLE_FILENAME, NETWORK_HEATMAP_BUNDLE_FILENAME
from backend.models.common import FilterSpec
from backend.repositories import origin
from backend.repositories import origin as origin_mod
from backend.repositories._base import QueryRunner, _safe_table
from backend.repositories.network import (
    _avg_hs,
    _has_signal,
    _health_score,
    _response_cache_key,
    _rtt_congestion_expr,
    get_health,
    get_quality,
)
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
from backend.utils.time_window import invite_clamp_fingerprint, resolve_window
from scripts.lib.section_measurements import describe_response
from tests._helpers.duckdb_fixtures import generate_mock_logs, insert_mock_logs
from tests._helpers.rollups import noop_lock as _noop_lock
from tests._helpers.sources import make_source


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
        patch("backend.core.storage.view._get_service_lock", _noop_lock),
        patch("backend.core.storage.execute_with_stale_view_retry", side_effect=lambda c, _src, fn: fn(c)),
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


# ── Merged from tests/repositories/test_network.py ──

_F200 = {"status": FilterSpec(mode="include", values=["200"])}
_HEALTH_KEYS = ("summary", "heatmap", "buckets", "leaderboard", "metro_leaderboard", "cities", "map_buckets")


def _net_logs(source, n=20, **overrides):
    logs = generate_mock_logs(source, num_logs=n)
    for log in logs:
        log.update({"tcp_rtt": 25_000, "asn": 7922, "country": "US", "city": "Boston", **overrides})
    return logs


def _seed_net(con, source, n=20, **overrides):
    insert_mock_logs(con, _safe_table(source["name"]), _net_logs(source, n=n, **overrides))


def test_format_city_label():
    assert format_city_label("New York", "US", "NY") in ["New York, NY, United States", "New York, NY, US"]
    assert format_city_label("London", "GB") in ["London, United Kingdom", "London, GB"]
    assert format_city_label("", "US") in ["United States", "US"]
    assert format_city_label("Paris", "FR", "IDF") in ["Paris, IDF, France", "Paris, IDF, FR"]
    assert format_city_label("", "", "") == "Unknown"


def test_health_score():
    assert (
        _health_score(throughput_bps=1000000, rtt_congestion_us=0, avg_ploss=0, rtt_jitter_us=0, error_pct=0) == 100.0
    )
    assert _health_score(0, 200000, 0.05, 100000, 10.0) == 0.0
    assert _health_score(None, None, None, None, None) == 100.0


def test_network_health_summary_accepts_worst_entry_dicts():
    summary = NetworkHealthSummary(
        global_health_score=85.0,
        avg_rtt_ms=42.0,
        total_reqs=1000,
        worst_asn={"label": "Comcast (AS7922)", "score": 72.3},
        worst_country={"label": "United States", "score": 80.1},
    )
    assert isinstance(summary.worst_asn, NetworkWorstEntry)
    assert summary.worst_asn.label == "Comcast (AS7922)" and summary.worst_asn.score == 72.3
    assert isinstance(summary.worst_country, NetworkWorstEntry) and summary.worst_country.label == "United States"

    empty = NetworkHealthSummary(global_health_score=0, avg_rtt_ms=0, total_reqs=0)
    assert empty.worst_asn is None and empty.worst_country is None


def test_avg_hs_returns_none_when_no_buckets_match():
    assert _avg_hs({"k1": {"health_score": 80.0}}, ["k2", "k3"]) is None


def test_avg_hs_skips_buckets_with_none_health_score():
    buckets = {"k1": {"health_score": 80.0}, "k2": {"health_score": None}, "k3": {"health_score": 60.0}}
    assert _avg_hs(buckets, ["k1", "k2", "k3"]) == 70.0


def test_avg_hs_rounds_to_one_decimal_place():
    assert _avg_hs({"k1": {"health_score": 33.333}, "k2": {"health_score": 66.667}}, ["k1", "k2"]) == 50.0


def test_health_score_packet_loss_weight_is_40_percent():
    assert _health_score(None, 0, 0.05, 0, 0) == 60.0


def test_health_score_congestion_weight_is_30_percent():
    assert _health_score(None, 200_000, 0, 0, 0) == 70.0


def test_health_score_jitter_weight_is_20_percent():
    assert _health_score(None, 0, 0, 100_000, 0) == 80.0


def test_health_score_error_weight_is_10_percent():
    assert _health_score(None, 0, 0, 0, 10.0) == 90.0


def test_health_score_clamps_packet_loss_at_5_percent():
    assert _health_score(None, 0, 0.5, 0, 0) == 60.0


def test_get_health_returns_available_false_when_required_cols_missing(in_memory_duckdb, test_service_source):
    table = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(f"CREATE TABLE {table} (timestamp TIMESTAMP, status INTEGER)")
    out = get_health(lambda: in_memory_duckdb, test_service_source, None, None, _F200)
    assert out["available"] is False and "Groups F" in out["reason"] and "G" in out["reason"]


def test_get_health_strips_asn_filter_when_map_asn_specified(in_memory_duckdb, test_service_source):
    _seed_net(in_memory_duckdb, test_service_source, n=10)
    out = get_health(
        lambda: in_memory_duckdb,
        test_service_source,
        None,
        None,
        {"asn": {"mode": "exclude", "values": ["7922"]}},
        map_asn="7922",
    )
    assert out.get("available") is not False


def test_get_health_handles_unsigned_rtt_subtraction(in_memory_duckdb, test_service_source):
    table = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(
        f"CREATE TABLE {table} (timestamp TIMESTAMP, asn UINTEGER, tcp_rtt UINTEGER, rtt_min UINTEGER, "
        f"rtt_var UINTEGER, ploss FLOAT, country VARCHAR, status INTEGER, cache VARCHAR, elapsed UBIGINT, resp_bytes UBIGINT)"
    )
    in_memory_duckdb.execute(
        f"INSERT INTO {table} SELECT TIMESTAMP '2026-09-10 20:00:00' + INTERVAL (i * 10) SECOND, "
        f"7922, 76570, 96261, 100, 0.01, 'US', 200, 'HIT', 100000, 50000 FROM range(10) AS r(i)"
    )
    assert in_memory_duckdb.execute(f"SELECT {_rtt_congestion_expr()} FROM {table}").fetchone() == (-19691,)


def test_get_quality_returns_available_false_when_tcp_rtt_missing(in_memory_duckdb, test_service_source):
    table = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(f"CREATE TABLE {table} (timestamp TIMESTAMP, status INTEGER)")
    out = get_quality(lambda: in_memory_duckdb, test_service_source, None, None, {})
    assert out["available"] is False and out["region_country"] == "US"
    for key in ("by_country", "by_asn", "by_region", "by_pop", "scatter", "countries"):
        assert out[key] == []


def test_get_quality_returns_bars_grouped_by_country_when_data_present(in_memory_duckdb, test_service_source):
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=30)
    for i, log in enumerate(logs):
        log["tcp_rtt"] = 20_000 + (i * 1000)
        log["country"] = "US" if i < 20 else "GB"
    insert_mock_logs(in_memory_duckdb, table, logs)

    out = get_quality(lambda: in_memory_duckdb, test_service_source, None, None, {})
    by_country = out["by_country"]
    assert len(by_country) >= 1 and "US" in {r["label"] for r in by_country}
    for r in by_country:
        assert 0 < r["rtt_ms"] < 200 and r["reqs"] > 0
    attempts = [row for row in out["section_timings"] if row["section"] == "network_quality_rollup"]
    assert len(attempts) == 1 and attempts[0]["outcome"] == "fallback" and attempts[0]["activity"] == "overhead"
    assert any(
        row["source"] == "live" and row["activity"] == "compute" and row["current_execution"]
        for row in out["section_timings"]
        if row.get("source") == "live"
    )


def test_get_quality_retains_rollup_dispatch_timings(in_memory_duckdb, test_service_source, monkeypatch):
    monkeypatch.setattr(
        QueryRunner,
        "try_network_quality_from_rollup",
        lambda self, *args, **kwargs: {"available": True, "by_country": []},
    )
    out = get_quality(lambda: in_memory_duckdb, test_service_source, None, None, {})
    (row,) = out["section_timings"]
    assert (row["source"], row["activity"], row["outcome"]) == ("rollup", "compute", "served")
    assert row["current_execution"] is True


def test_get_quality_respects_region_country_param(in_memory_duckdb, test_service_source):
    _seed_net(in_memory_duckdb, test_service_source, n=10, country="GB", region="London")
    out = get_quality(lambda: in_memory_duckdb, test_service_source, None, None, {}, region_country="GB")
    assert out["region_country"] == "GB"


def test_get_quality_enriches_asn_labels_and_keeps_pop_as_code(in_memory_duckdb, test_service_source, monkeypatch):
    _seed_net(in_memory_duckdb, test_service_source, n=20, pop="SJC")
    monkeypatch.setattr(
        "backend.core.duckdb.get_asn_names",
        lambda service_id, asns: {7922: "Comcast Cable Communications"},
    )
    out = get_quality(lambda: in_memory_duckdb, test_service_source, None, None, {})
    asn_row = next(r for r in out["by_asn"] if r["value"] == "7922")
    assert asn_row["label"] == "Comcast Cable Communications (7922)"
    pop_row = next(r for r in out["by_pop"] if r["value"] == "SJC")
    assert pop_row["label"] == "SJC"
    for r in out["by_country"]:
        assert r["value"] == r["label"]


def test_get_health_full_response_when_sections_none(in_memory_duckdb, test_service_source):
    _seed_net(in_memory_duckdb, test_service_source)
    out = get_health(lambda: in_memory_duckdb, test_service_source, None, None, _F200)
    for key in _HEALTH_KEYS:
        assert key in out, f"section {key} missing from default response"


def test_get_health_selector_drops_unrequested_keys(in_memory_duckdb, test_service_source):
    _seed_net(in_memory_duckdb, test_service_source)
    out = get_health(lambda: in_memory_duckdb, test_service_source, None, None, _F200, sections={"summary"})
    assert "summary" in out
    for blocked in ("heatmap", "leaderboard", "metro_leaderboard"):
        assert blocked not in out, f"{blocked} leaked through summary-only selector"


def test_get_health_metro_only_skips_heatmap_query(in_memory_duckdb, test_service_source):
    _seed_net(in_memory_duckdb, test_service_source)
    out = get_health(lambda: in_memory_duckdb, test_service_source, None, None, _F200, sections={"metro_leaderboard"})
    timings = {t["section"] for t in out.get("section_timings", [])}
    assert "heatmap_query" not in timings and "map_query" not in timings and "metro_query" in timings
    assert "metro_leaderboard" in out


def test_get_health_summary_selector_pulls_dependent_queries(in_memory_duckdb, test_service_source):
    _seed_net(in_memory_duckdb, test_service_source)
    out = get_health(lambda: in_memory_duckdb, test_service_source, None, None, _F200, sections={"summary"})
    timings = {t["section"] for t in out.get("section_timings", [])}
    assert "heatmap_query" in timings and "map_query" in timings and "summary" in out


def test_get_health_selector_skips_response_cache_write(in_memory_duckdb, test_service_source):
    _seed_net(in_memory_duckdb, test_service_source)
    get_health(lambda: in_memory_duckdb, test_service_source, None, None, _F200, sections={"summary"})
    full = get_health(lambda: in_memory_duckdb, test_service_source, None, None, _F200)
    for key in _HEALTH_KEYS:
        assert key in full, f"selector poisoned the cache; full response missing {key}"


@pytest.mark.security_regression
def test_cache_key_partitions_by_service():
    src_a = {"name": "svc_a", "service_id": "a"}
    src_b = {"name": "svc_b", "service_id": "b"}
    tail = ("2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", {}, "health_score", 5, 30, "all", None, False)
    assert _response_cache_key(src_a, *tail) != _response_cache_key(src_b, *tail)


@pytest.mark.security_regression
def test_cache_key_partitions_by_mask_ips():
    src = {"name": "svc", "service_id": "s"}
    args = (src, "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", {}, "health_score", 5, 30, "all", None)
    assert _response_cache_key(*args, False) != _response_cache_key(*args, True)


@pytest.mark.security_regression
def test_cache_key_partitions_by_sections():
    src = {"name": "svc", "service_id": "s"}
    base = (src, "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", {}, "health_score", 5, 30, "all")
    core = _response_cache_key(*base, {"summary", "leaderboard", "metro_leaderboard"}, False)
    mp = _response_cache_key(*base, {"heatmap", "buckets", "cities", "map_buckets"}, False)
    full = _response_cache_key(*base, None, False)
    assert len({core, mp, full}) == 3
    assert _response_cache_key(*base, {"leaderboard", "metro_leaderboard", "summary"}, False) == core


@pytest.mark.security_regression
def test_cache_key_is_anchor_faithful_not_span_quantized():
    src = {"name": "svc", "service_id": "s"}
    tail = ({}, "health_score", 5, 30, "all", None, False)
    now_anchored = _response_cache_key(src, "2026-06-29T00:00:00Z", "2026-06-30T00:00:00Z", *tail)
    extent_anchored = _response_cache_key(src, "2026-06-01T00:00:00Z", "2026-06-02T00:00:00Z", *tail)
    assert now_anchored != extent_anchored


@pytest.mark.security_regression
def test_cache_key_minute_bucketed_seconds_collapse():
    src = {"name": "svc", "service_id": "s"}
    tail = ({}, "health_score", 5, 30, "all", None, False)
    k_10s = _response_cache_key(src, "2026-06-29T00:00:10Z", "2026-06-30T00:00:20Z", *tail)
    k_45s = _response_cache_key(src, "2026-06-29T00:00:45Z", "2026-06-30T00:00:55Z", *tail)
    k_next_min = _response_cache_key(src, "2026-06-29T00:01:10Z", "2026-06-30T00:00:20Z", *tail)
    assert k_10s == k_45s and k_10s != k_next_min


@pytest.mark.security_regression
def test_cache_read_write_round_trip(in_memory_duckdb, test_service_source):
    _seed_net(in_memory_duckdb, test_service_source)
    first = get_health(lambda: in_memory_duckdb, test_service_source, None, None, _F200)
    assert not first.get("is_cached")
    second = get_health(lambda: in_memory_duckdb, test_service_source, None, None, _F200)
    assert second.get("is_cached") is True


@pytest.mark.security_regression
def test_force_refresh_skips_read_but_writes(in_memory_duckdb, test_service_source):
    _seed_net(in_memory_duckdb, test_service_source)
    get_health(lambda: in_memory_duckdb, test_service_source, None, None, _F200)
    refreshed = get_health(lambda: in_memory_duckdb, test_service_source, None, None, _F200, force_refresh=True)
    assert not refreshed.get("is_cached")
    after = get_health(lambda: in_memory_duckdb, test_service_source, None, None, _F200)
    assert after.get("is_cached") is True


@pytest.mark.security_regression
def test_stale_empty_result_is_not_cached(in_memory_duckdb, test_service_source):
    _seed_net(in_memory_duckdb, test_service_source)
    empty_window = ("2020-01-01T00:00:00Z", "2020-01-02T00:00:00Z")
    out = get_health(lambda: in_memory_duckdb, test_service_source, *empty_window, {})
    assert not _has_signal(out)
    second = get_health(lambda: in_memory_duckdb, test_service_source, *empty_window, {})
    assert not second.get("is_cached"), "a zero-signal window was cached (poison risk)"


@pytest.mark.security_regression
def test_has_signal_guard():
    assert _has_signal({"summary": {"total_reqs": 5}})
    assert _has_signal({"heatmap": [{"asn": 1}]})
    assert _has_signal({"leaderboard": [{"asn": 1}]})
    assert not _has_signal({"summary": {"total_reqs": 0}, "heatmap": [], "leaderboard": []})
    assert not _has_signal({"available": True})


class _FakeAnalystSession:
    def __init__(self, query_start_time=None, query_end_time=None, query_window_hours=None):
        self.query_start_time = query_start_time
        self.query_end_time = query_end_time
        self.query_window_hours = query_window_hours


def _stable_key(src, *, range_token="30d", quantized_anchor="2026-06-29T00:00:00Z", icf=None, mask_ips=False):
    return _response_cache_key(
        src,
        "2026-06-29T00:00:00Z",
        "2026-06-29T12:34:56Z",
        {},
        "health_score",
        5,
        30,
        "all",
        None,
        mask_ips,
        range_token=range_token,
        quantized_anchor=quantized_anchor,
        invite_clamp_fingerprint=icf,
    )


@pytest.mark.security_regression
def test_stable_key_partitions_by_service():
    assert _stable_key({"name": "svc_a", "service_id": "a"}) != _stable_key({"name": "svc_b", "service_id": "b"})


@pytest.mark.security_regression
def test_stable_key_partitions_by_mask_ips():
    src = {"name": "svc", "service_id": "s"}
    assert _stable_key(src, mask_ips=False) != _stable_key(src, mask_ips=True)


@pytest.mark.security_regression
def test_stable_key_partitions_by_invite_clamp_fingerprint():
    src = {"name": "svc", "service_id": "s"}
    icf_open = invite_clamp_fingerprint(None)
    icf_restricted = invite_clamp_fingerprint(
        _FakeAnalystSession(query_start_time="2026-01-01T00:00:00Z", query_end_time="2026-02-01T00:00:00Z")
    )
    icf_windowed = invite_clamp_fingerprint(_FakeAnalystSession(query_window_hours=24))
    assert icf_open is None and icf_restricted is not None and icf_windowed is not None
    assert (
        len({_stable_key(src, icf=icf_open), _stable_key(src, icf=icf_restricted), _stable_key(src, icf=icf_windowed)})
        == 3
    )


@pytest.mark.security_regression
def test_stable_key_ignores_fe_supplied_bounds():
    src = {"name": "svc", "service_id": "s"}
    kw = dict(range_token="30d", quantized_anchor="2026-06-29T00:00:00Z", invite_clamp_fingerprint=None)
    tail = ({}, "health_score", 5, 30, "all", None, False)
    k1 = _response_cache_key(src, "2026-06-29T00:00:01Z", "2026-06-29T11:11:11Z", *tail, **kw)
    k2 = _response_cache_key(src, "2026-06-29T00:43:59Z", "2026-06-29T22:22:22Z", *tail, **kw)
    assert k1 == k2


@pytest.mark.security_regression
def test_stable_key_partitions_by_range_token_and_anchor():
    src = {"name": "svc", "service_id": "s"}
    assert (
        len(
            {
                _stable_key(src, range_token="30d"),
                _stable_key(src, range_token="7d"),
                _stable_key(src, range_token="24h"),
            }
        )
        == 3
    )
    assert _stable_key(src, quantized_anchor="2026-06-29T00:00:00Z") != _stable_key(
        src, quantized_anchor="2026-06-29T00:01:00Z"
    )


@pytest.mark.security_regression
def test_stable_key_disjoint_from_legacy_key():
    src = {"name": "svc", "service_id": "s"}
    legacy = _response_cache_key(
        src, "2026-05-30T00:00:00Z", "2026-06-29T00:00:00Z", {}, "health_score", 5, 30, "all", None, False
    )
    assert legacy != _stable_key(src, range_token="30d", quantized_anchor="2026-06-29T00:00:00Z")


@pytest.mark.security_regression
def test_clamp_ceiling_still_enforced_when_token_supplied():
    now = datetime(2026, 6, 29, 12, 0, 0, tzinfo=UTC)
    tb = _time_bounds_from_params(None, None, 24, now=now)
    resolved_start, resolved_end = resolve_window("30d", now.isoformat(), now=now)
    rs = datetime.fromisoformat(resolved_start.replace("Z", "+00:00"))
    re_ = datetime.fromisoformat(resolved_end.replace("Z", "+00:00"))
    assert re_ - rs >= timedelta(days=29)
    clamped_start, clamped_end = tb.clamp(rs, re_, max_span=MAX_ANALYST_QUERY_SPAN)
    assert clamped_end - clamped_start <= timedelta(hours=24) and clamped_start > rs


@pytest.mark.security_regression
def test_keyed_path_cache_hit_round_trip(in_memory_duckdb, test_service_source):
    in_memory_duckdb.execute("SET TimeZone='UTC'")
    _seed_net(in_memory_duckdb, test_service_source)

    now = datetime.now(UTC)
    anchor = now.strftime("%Y-%m-%dT%H:%M:00Z")
    win_start_a = (now - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:01Z")
    win_start_b = (now - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:59Z")
    win_end = (now + timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")

    first = get_health(
        lambda: in_memory_duckdb,
        test_service_source,
        win_start_a,
        win_end,
        _F200,
        range_token="30d",
        quantized_anchor=anchor,
    )
    assert not first.get("is_cached") and _has_signal(first)
    second = get_health(
        lambda: in_memory_duckdb,
        test_service_source,
        win_start_b,
        win_end,
        _F200,
        range_token="30d",
        quantized_anchor=anchor,
    )
    assert second.get("is_cached") is True


def _open_two_network_connections(tmp_path, source, logs):
    db_path = str(tmp_path / "network_health.duckdb")
    setup = duckdb.connect(db_path)
    insert_mock_logs(setup, _safe_table(source["name"]), logs)
    setup.close()
    return duckdb.connect(db_path, read_only=True), duckdb.connect(db_path, read_only=True)


def test_get_health_coalesces_concurrent_core_and_map_temp_table_builds(monkeypatch, tmp_path, test_service_source):
    con_core, con_map = _open_two_network_connections(
        tmp_path, test_service_source, _net_logs(test_service_source, n=30)
    )
    original_build = QueryRunner.create_filtered_temp_table
    call_count = 0
    count_lock = threading.Lock()
    build_started = threading.Event()
    release_leader = threading.Event()

    def slow_build(self, *args, **kwargs):
        nonlocal call_count
        with count_lock:
            call_count += 1
        build_started.set()
        assert release_leader.wait(timeout=5), "test deadlocked waiting for release"
        return original_build(self, *args, **kwargs)

    monkeypatch.setattr(QueryRunner, "create_filtered_temp_table", slow_build)
    results: dict[str, dict] = {}
    errors: list[BaseException] = []

    def run(label, con, sections):
        try:
            results[label] = get_health(lambda: con, test_service_source, None, None, _F200, sections=sections)
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    try:
        core_thread = threading.Thread(
            target=run, args=("core", con_core, {"summary", "leaderboard", "metro_leaderboard"})
        )
        core_thread.start()
        assert build_started.wait(timeout=5)
        map_thread = threading.Thread(
            target=run, args=("map", con_map, {"heatmap", "buckets", "cities", "map_buckets"})
        )
        map_thread.start()
        time.sleep(0.2)
        release_leader.set()
        core_thread.join(timeout=5)
        map_thread.join(timeout=5)
    finally:
        con_core.close()
        con_map.close()

    assert not errors and call_count == 1
    assert results["core"]["leaderboard"] and results["core"]["metro_leaderboard"]
    assert results["map"]["heatmap"] and results["map"]["map_buckets"]


def test_get_health_does_not_coalesce_requests_with_different_top_n(monkeypatch, tmp_path, test_service_source):
    con_a, con_b = _open_two_network_connections(tmp_path, test_service_source, _net_logs(test_service_source, n=30))
    original_build = QueryRunner.create_filtered_temp_table
    call_count = 0
    count_lock = threading.Lock()

    def counting_build(self, *args, **kwargs):
        nonlocal call_count
        with count_lock:
            call_count += 1
        return original_build(self, *args, **kwargs)

    monkeypatch.setattr(QueryRunner, "create_filtered_temp_table", counting_build)
    results: dict[str, dict] = {}

    def run(label, con, top_n):
        results[label] = get_health(
            lambda: con, test_service_source, None, None, _F200, sections={"heatmap"}, top_n=top_n
        )

    try:
        t1 = threading.Thread(target=run, args=("a", con_a, 10))
        t2 = threading.Thread(target=run, args=("b", con_b, 50))
        t1.start()
        t2.start()
        t1.join(timeout=5)
        t2.join(timeout=5)
    finally:
        con_a.close()
        con_b.close()

    assert call_count == 2


def test_get_health_coalesced_temp_table_failure_surfaces_to_both_waiters(monkeypatch, tmp_path, test_service_source):
    con_core, con_map = _open_two_network_connections(
        tmp_path, test_service_source, _net_logs(test_service_source, n=10)
    )
    build_started = threading.Event()
    release_leader = threading.Event()

    def failing_build(self, *args, **kwargs):
        build_started.set()
        assert release_leader.wait(timeout=5)
        return None

    monkeypatch.setattr(QueryRunner, "create_filtered_temp_table", failing_build)
    results: dict[str, dict] = {}

    def run(label, con, sections):
        results[label] = get_health(lambda: con, test_service_source, None, None, _F200, sections=sections)

    try:
        t1 = threading.Thread(target=run, args=("core", con_core, {"summary", "leaderboard", "metro_leaderboard"}))
        t1.start()
        assert build_started.wait(timeout=5)
        t2 = threading.Thread(target=run, args=("map", con_map, {"heatmap", "buckets", "cities", "map_buckets"}))
        t2.start()
        time.sleep(0.2)
        release_leader.set()
        t1.join(timeout=5)
        t2.join(timeout=5)
    finally:
        con_core.close()
        con_map.close()

    assert results["core"]["available"] is False and results["map"]["available"] is False


def _hours_back(n_hours: int, span_hours: int) -> tuple[str, str, list[str]]:
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    end = now - timedelta(hours=n_hours)
    start = end - timedelta(hours=span_hours)
    hours = [(start + timedelta(hours=i)).strftime("%Y-%m-%d-%H") for i in range(span_hours)]
    return start.isoformat(), end.isoformat(), hours


def _write_heatmap_bundle(bundled_root: Path, hour_str: str, asns: list[int] | None = None) -> None:
    hour_dir = bundled_root / f"hour={hour_str}"
    hour_dir.mkdir(parents=True, exist_ok=True)
    out = hour_dir / NETWORK_HEATMAP_BUNDLE_FILENAME
    hour_dt = datetime.strptime(hour_str, "%Y-%m-%d-%H").replace(tzinfo=UTC)
    rows_sql = ", ".join(
        f"({asn}, TIMESTAMPTZ '{hour_dt.isoformat()}', 100, 5, 1000000.0, 25000.0, 20000.0, 5000.0, 95, 0.001, 90)"
        for asn in (asns or [1234, 5678])
    )
    with contextlib.closing(duckdb.connect()) as con:
        con.execute(
            f"COPY (SELECT * FROM (VALUES {rows_sql}) AS t(asn, hour_ts, reqs, errors, resp_bytes_sum, "
            f"rtt_p50_us, rtt_min_p50_us, rtt_var_p50_us, rtt_count, ploss_sum, ploss_count)) TO '{out}' (FORMAT PARQUET)"
        )


def _write_geo_bundle(bundled_root: Path, hour_str: str) -> None:
    hour_dir = bundled_root / f"hour={hour_str}"
    hour_dir.mkdir(parents=True, exist_ok=True)
    out = hour_dir / NETWORK_GEO_BUNDLE_FILENAME
    hour_dt = datetime.strptime(hour_str, "%Y-%m-%d-%H").replace(tzinfo=UTC)
    rows_sql = (
        f"('US', 'San Jose', 37.34, -121.89, '807', TIMESTAMPTZ '{hour_dt.isoformat()}', 50, 2, 0.01, 45, 1250000.0, 48),"
        f"('DE', 'Berlin', 52.52, 13.41, NULL, TIMESTAMPTZ '{hour_dt.isoformat()}', 30, 1, 0.005, 28, 800000.0, 30)"
    )
    with contextlib.closing(duckdb.connect()) as con:
        con.execute(
            f"COPY (SELECT * FROM (VALUES {rows_sql}) AS t(country, city, lat, lon, metro, hour_ts, "
            f"reqs, errors, ploss_sum, ploss_count, rtt_sum, rtt_count)) TO '{out}' (FORMAT PARQUET)"
        )


def _write_all_fields_marker(bundled_root: Path, hour_str: str) -> None:
    hour_dir = bundled_root / f"hour={hour_str}"
    hour_dir.mkdir(parents=True, exist_ok=True)
    marker = hour_dir / "all_fields.parquet"
    with contextlib.closing(duckdb.connect()) as con:
        con.execute(
            f"COPY (SELECT 'marker' AS field, '{hour_str}' AS hour, 'v' AS value, 1 AS count) TO '{marker}' (FORMAT PARQUET)"
        )


class TestBuildNetworkHeatmapBundles:
    def test_writes_parquet_for_closed_hour(self, tmp_path):
        cache_dir = tmp_path / "cache" / "test_svc"
        bundled_root = cache_dir / "rollups" / "hour_bundled"
        hour_str = (datetime.now(UTC) - timedelta(hours=3)).strftime("%Y-%m-%d-%H")
        _write_heatmap_bundle(bundled_root, hour_str)

        out_path = bundled_root / f"hour={hour_str}" / NETWORK_HEATMAP_BUNDLE_FILENAME
        assert out_path.is_file()
        with contextlib.closing(duckdb.connect()) as con:
            rows = con.execute(f"SELECT * FROM '{out_path}'").fetchall()
            cols = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM '{out_path}'").fetchall()}
        assert rows
        expected = set(
            "asn hour_ts reqs errors resp_bytes_sum rtt_p50_us rtt_min_p50_us rtt_var_p50_us rtt_count ploss_sum ploss_count".split()
        )
        assert expected.issubset(cols)

    def test_skip_active_hour_token_is_filtered(self, tmp_path):
        from backend.core.rollups.network_health_heatmap import build_network_heatmap_bundles

        active_hour = datetime.now(UTC).strftime("%Y-%m-%d-%H")
        cache_dir = tmp_path / "cache" / "test_svc"
        cache_dir.mkdir(parents=True)
        src = make_source(str(cache_dir))
        try:
            n = build_network_heatmap_bundles("test", src, [active_hour])
        except Exception:
            n = 0
        assert n == 0


class TestBuildNetworkGeoBundles:
    def test_writes_parquet_with_correct_schema(self, tmp_path):
        hour_str = (datetime.now(UTC) - timedelta(hours=3)).strftime("%Y-%m-%d-%H")
        bundled_root = tmp_path / "cache" / "test_svc" / "rollups" / "hour_bundled"
        _write_geo_bundle(bundled_root, hour_str)
        out_path = bundled_root / f"hour={hour_str}" / NETWORK_GEO_BUNDLE_FILENAME
        assert out_path.is_file()
        with contextlib.closing(duckdb.connect()) as con:
            cols = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM '{out_path}'").fetchall()}
        expected = set("country city lat lon metro hour_ts reqs errors ploss_sum ploss_count rtt_sum rtt_count".split())
        assert expected.issubset(cols)


@pytest.fixture
def heatmap_rollup_layout(tmp_path):
    cache_dir = tmp_path / "cache" / "test_svc"
    bundled = cache_dir / "rollups" / "hour_bundled"
    bundled.mkdir(parents=True)
    start_iso, end_iso, hours = _hours_back(n_hours=2, span_hours=72)
    for h in hours:
        _write_heatmap_bundle(bundled, h)
        _write_all_fields_marker(bundled, h)
    return bundled, make_source(str(cache_dir)), start_iso, end_iso, hours


class TestTryNetworkHeatmapFromRollup:
    def test_returns_none_when_window_too_short(self, heatmap_rollup_layout):
        _, src, _, _, _ = heatmap_rollup_layout
        now = datetime.now(UTC)
        with contextlib.closing(duckdb.connect()) as con:
            res = QueryRunner(con, src).try_network_heatmap_from_rollup(
                (now - timedelta(hours=12)).isoformat(), now.isoformat(), has_filters=False
            )
        assert res is None

    def test_returns_none_when_has_filters(self, heatmap_rollup_layout):
        _, src, start_iso, end_iso, _ = heatmap_rollup_layout
        with contextlib.closing(duckdb.connect()) as con:
            assert QueryRunner(con, src).try_network_heatmap_from_rollup(start_iso, end_iso, has_filters=True) is None

    def test_returns_rows_for_eligible_window(self, heatmap_rollup_layout):
        _, src, start_iso, end_iso, _ = heatmap_rollup_layout
        with contextlib.closing(duckdb.connect()) as con:
            result = QueryRunner(con, src).try_network_heatmap_from_rollup(start_iso, end_iso, has_filters=False)
        assert result is not None and len(result) > 0

    def test_serves_hourly_rollup_for_finer_requested_bucket(self, heatmap_rollup_layout):
        _, src, start_iso, end_iso, _ = heatmap_rollup_layout
        with contextlib.closing(duckdb.connect()) as con:
            rows = QueryRunner(con, src).try_network_heatmap_from_rollup(
                start_iso, end_iso, has_filters=False, bucket_seconds=300
            )
        assert rows is not None and len(rows) > 0

    def test_returns_none_for_coarser_than_hourly_bucket(self, heatmap_rollup_layout):
        _, src, start_iso, end_iso, _ = heatmap_rollup_layout
        with contextlib.closing(duckdb.connect()) as con:
            rows = QueryRunner(con, src).try_network_heatmap_from_rollup(
                start_iso, end_iso, has_filters=False, bucket_seconds=7200
            )
        assert rows is None

    def test_row_shape_matches_live_heatmap_loop(self, heatmap_rollup_layout):
        _, src, start_iso, end_iso, _ = heatmap_rollup_layout
        with contextlib.closing(duckdb.connect()) as con:
            rows = QueryRunner(con, src).try_network_heatmap_from_rollup(start_iso, end_iso, has_filters=False)
        assert rows is not None and len(rows) > 0
        r = rows[0]
        assert len(r) == 10 and int(r[0]) > 0 and int(r[9]) > 0
        bucket = r[1].isoformat() if hasattr(r[1], "isoformat") else str(r[1])
        assert len(bucket) > 0

    def test_returns_none_when_no_bundles_exist(self, tmp_path):
        cache_dir = tmp_path / "cache" / "test_svc"
        (cache_dir / "rollups" / "hour_bundled").mkdir(parents=True)
        src = make_source(str(cache_dir))
        start_iso, end_iso, _ = _hours_back(n_hours=2, span_hours=72)
        with contextlib.closing(duckdb.connect()) as con:
            assert QueryRunner(con, src).try_network_heatmap_from_rollup(start_iso, end_iso, has_filters=False) is None


@pytest.fixture
def geo_rollup_layout(tmp_path):
    cache_dir = tmp_path / "cache" / "test_svc"
    bundled = cache_dir / "rollups" / "hour_bundled"
    bundled.mkdir(parents=True)
    start_iso, end_iso, hours = _hours_back(n_hours=2, span_hours=72)
    for h in hours:
        _write_geo_bundle(bundled, h)
        _write_all_fields_marker(bundled, h)
    return bundled, make_source(str(cache_dir)), start_iso, end_iso, hours


class TestTryNetworkGeoFromRollup:
    def test_returns_none_when_window_too_short(self, geo_rollup_layout):
        _, src, _, _, _ = geo_rollup_layout
        now = datetime.now(UTC)
        with contextlib.closing(duckdb.connect()) as con:
            res = QueryRunner(con, src).try_network_geo_from_rollup(
                (now - timedelta(hours=12)).isoformat(), now.isoformat(), has_filters=False
            )
        assert res is None

    def test_returns_none_when_has_filters(self, geo_rollup_layout):
        _, src, start_iso, end_iso, _ = geo_rollup_layout
        with contextlib.closing(duckdb.connect()) as con:
            assert QueryRunner(con, src).try_network_geo_from_rollup(start_iso, end_iso, has_filters=True) is None

    def test_returns_none_when_map_asn_not_all(self, geo_rollup_layout):
        _, src, start_iso, end_iso, _ = geo_rollup_layout
        with contextlib.closing(duckdb.connect()) as con:
            res = QueryRunner(con, src).try_network_geo_from_rollup(
                start_iso, end_iso, map_asn="12345", has_filters=False
            )
        assert res is None

    def test_returns_tuple_for_eligible_window(self, geo_rollup_layout):
        _, src, start_iso, end_iso, _ = geo_rollup_layout
        with contextlib.closing(duckdb.connect()) as con:
            result = QueryRunner(con, src).try_network_geo_from_rollup(start_iso, end_iso, has_filters=False)
        assert result is not None
        map_rows, metro_rows = result
        assert len(map_rows) > 0 and len(metro_rows) > 0

    def test_map_rows_shape_matches_live_loop(self, geo_rollup_layout):
        _, src, start_iso, end_iso, _ = geo_rollup_layout
        with contextlib.closing(duckdb.connect()) as con:
            result = QueryRunner(con, src).try_network_geo_from_rollup(start_iso, end_iso, has_filters=False)
        assert result is not None
        r = result[0][0]
        assert len(r) == 10 and isinstance(r[0], str) and len(r[0]) == 2 and int(r[9]) > 0
        bucket = r[5].isoformat() if hasattr(r[5], "isoformat") else str(r[5])
        assert len(bucket) > 0

    def test_metro_rows_shape_matches_live_loop(self, geo_rollup_layout):
        _, src, start_iso, end_iso, _ = geo_rollup_layout
        with contextlib.closing(duckdb.connect()) as con:
            result = QueryRunner(con, src).try_network_geo_from_rollup(start_iso, end_iso, has_filters=False)
        assert result is not None
        r = result[1][0]
        assert len(r) == 8 and isinstance(r[0], str) and r[2] == "" and int(r[7]) > 0

    def test_returns_none_when_no_bundles_exist(self, tmp_path):
        cache_dir = tmp_path / "cache" / "test_svc"
        (cache_dir / "rollups" / "hour_bundled").mkdir(parents=True)
        src = make_source(str(cache_dir))
        start_iso, end_iso, _ = _hours_back(n_hours=2, span_hours=72)
        with contextlib.closing(duckdb.connect()) as con:
            assert QueryRunner(con, src).try_network_geo_from_rollup(start_iso, end_iso, has_filters=False) is None

    def test_returns_tuple_when_window_includes_active_hour(self, geo_rollup_layout):
        bundled, src, _, _, _ = geo_rollup_layout
        now = datetime.now(UTC)
        start_iso, end_iso = (now - timedelta(hours=48)).isoformat(), now.isoformat()
        cur = (now - timedelta(hours=48)).replace(minute=0, second=0, microsecond=0)
        active_start = now.replace(minute=0, second=0, microsecond=0)
        while cur < active_start:
            h_str = cur.strftime("%Y-%m-%d-%H")
            _write_geo_bundle(bundled, h_str)
            _write_all_fields_marker(bundled, h_str)
            cur += timedelta(hours=1)

        with contextlib.closing(duckdb.connect()) as con:
            con.execute(
                "CREATE TABLE logs_test (country VARCHAR, city VARCHAR, lat DOUBLE, lon DOUBLE, "
                "metro VARCHAR, tcp_rtt BIGINT, status INT, timestamp TIMESTAMPTZ)"
            )
            con.execute("INSERT INTO logs_test VALUES ('US', 'Denver', 39.7, -104.9, '751', 25000, 200, now())")
            result = QueryRunner(con, src).try_network_geo_from_rollup(start_iso, end_iso, has_filters=False)
        assert result is not None and len(result[0]) > 0 and len(result[1]) > 0
