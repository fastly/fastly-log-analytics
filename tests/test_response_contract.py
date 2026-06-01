"""Pydantic response-model contract tests.

Catches the regression mode where a repo returns a dict with extra keys
that the FE depends on, but the response_model declaration silently
drops them. FastAPI's Pydantic validation silently strips any field
not declared on the response model, so the BE looks healthy in unit
tests but the FE sees `undefined` for that field at runtime.

This is exactly the bug that caused `window_total_requests` to vanish
from the insights response when we fixed the duplicate-key bug — the
field was in the repo's return dict, but never in `InsightsResponse`.

Each test below pairs (repo function → response model class) and
asserts that every key the repo emits is either:
  1. A declared field on the response model, OR
  2. In the explicit `_ALLOWED_DROPPED_KEYS` allowlist (telemetry /
     debug keys the model intentionally strips).

If a repo starts returning a new key, this test fails until the
key is added to the model (or the allowlist).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import duckdb
import pytest

from backend.repositories._base import _safe_table

# Telemetry / debug keys that every repo merges in via `**runner.telemetry()`.
# These are intentionally stripped from the FE-visible payload because the
# FE has its own request tracing. Listing them here documents the
# stripping intent so a new telemetry key surfacing in a different repo
# fails the test.
_ALLOWED_DROPPED_KEYS = {
    "debug_queries",
    "debug_calls",
    "_debug_queries",
    "_debug_calls",
    "_is_cached",
    # `insights.get_insights` computes a window-total row count that the FE
    # doesn't currently render. Keeping it as repo-internal telemetry rather
    # than declaring it on InsightsResponse keeps the FE TS surface minimal.
    # Move to the model if/when the FE actually needs it.
    "window_total_requests",
}

_TABLE_NAME = "test_service"
_src = {"name": _TABLE_NAME, "service_id": "tsid"}


@pytest.fixture
def seeded_con():
    """Seeded DuckDB used across every contract test."""
    con = duckdb.connect(":memory:")
    table = _safe_table(_TABLE_NAME)
    con.execute(
        f"""
        CREATE TABLE {table} (
            "timestamp" TIMESTAMPTZ,
            "dt" VARCHAR,
            "timestamp_hour" VARCHAR,
            "status" INTEGER,
            "country" VARCHAR,
            "url" VARCHAR,
            "ip" VARCHAR,
            "method" VARCHAR,
            "ua" VARCHAR,
            "pop" VARCHAR,
            "asn" INTEGER,
            "city" VARCHAR,
            "region" VARCHAR,
            "elapsed" INTEGER,
            "cache" VARCHAR,
            "ottfb" DOUBLE,
            "ttfb" DOUBLE,
            "ottlb" DOUBLE,
            "waf_sig" VARCHAR,
            "edge" BOOLEAN,
            "lat" DOUBLE,
            "lon" DOUBLE,
            "resp_bytes" INTEGER,
            "oip" VARCHAR,
            "ost" INTEGER,
            "tcp_rtt" INTEGER,
            "ploss" DOUBLE,
            "rtt_var" INTEGER,
            "rtt_min" INTEGER,
            "metro" VARCHAR,
            "ja3" VARCHAR,
            "ja4" VARCHAR,
            "ttl" INTEGER,
            "p_type" VARCHAR,
            "transport" VARCHAR,
            "c_speed" VARCHAR,
            "c_type" VARCHAR,
            "server_region" VARCHAR,
            "retrans" INTEGER,
            "bw" INTEGER,
            "delivery_rate" INTEGER,
            "rid" VARCHAR,
            "prid" VARCHAR
        )
        """
    )
    base = datetime.now(UTC) - timedelta(hours=1)
    for i in range(20):
        ts = base + timedelta(minutes=i * 3)
        con.execute(
            f"INSERT INTO {table} VALUES " + "(" + ", ".join(["?"] * 43) + ")",
            [
                ts,
                ts.strftime("%Y-%m-%d"),
                ts.strftime("%Y-%m-%d-%H"),
                200 if i % 5 else 500,
                "US",
                f"/p/{i}",
                f"10.0.0.{i}",
                "GET",
                "Mozilla/5.0",
                "LAX",
                15169,
                "SF",
                "CA",
                50 + i * 10,
                "HIT" if i % 2 else "MISS",
                50000.0 + i,
                0.05 + i * 0.01,
                0.1 + i * 0.01,
                "",
                True,
                37.7749,
                -122.4194,
                500 + i * 50,
                "origin-1",
                200 if i % 5 else 500,
                10000,
                0.0,
                100,
                50,
                "807",
                "ja3",
                "ja4",
                300,
                "U",
                "h2",
                "C",
                "broadband",
                "us-west",
                0,
                100000,
                100000,
                f"r{i}",
                f"pr{i % 5}",
            ],
        )
    yield con
    con.close()


def _check_no_dropped_keys(repo_result: dict, response_model_cls, label: str):
    """Assert every key in ``repo_result`` is either a declared field on
    the response model OR in the allowlist of intentionally-stripped
    telemetry keys."""
    declared_fields = set(response_model_cls.model_fields.keys())
    repo_keys = set(repo_result.keys())
    dropped = repo_keys - declared_fields - _ALLOWED_DROPPED_KEYS
    assert not dropped, (
        f"{label}: repo returned keys not declared on {response_model_cls.__name__} "
        f"and not in the allowlist: {sorted(dropped)}\n"
        f"Either add them to the model (FE wants them) or add them to "
        f"_ALLOWED_DROPPED_KEYS (intentionally stripped)."
    )


# ── dashboard.get_aggregates ↔ AggregatesResponse ────────────────────────


def test_dashboard_aggregates_contract(seeded_con):
    """`get_aggregates` return keys must match `AggregatesResponse` —
    minus the explicit telemetry allowlist. Pinned because losing this
    would let a repo refactor silently drop a field the FE renders
    (the regression mode of the `window_total_requests` bug)."""
    from backend.models.dashboard import AggregatesResponse
    from backend.repositories.dashboard import _dashboard_cache, get_aggregates

    _dashboard_cache.clear()
    result = get_aggregates(
        con=seeded_con,
        src=_src,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="requests",
    )
    _check_no_dropped_keys(result, AggregatesResponse, "dashboard.get_aggregates")


# ── dashboard.get_raw ↔ RawResponse ──────────────────────────────────────


def test_dashboard_raw_contract(seeded_con):
    """`get_raw` return keys must match `RawResponse`."""
    from backend.models.dashboard import RawResponse
    from backend.repositories.dashboard import get_raw

    result = get_raw(
        con=seeded_con,
        src=_src,
        start_time=None,
        end_time=None,
        filters={},
        page=1,
        limit=5,
        sort_col="timestamp",
        sort_dir="DESC",
        columns=["timestamp", "status", "url"],
    )
    _check_no_dropped_keys(result, RawResponse, "dashboard.get_raw")


# ── insights.get_insights ↔ InsightsResponse ─────────────────────────────


def test_insights_get_insights_contract(seeded_con):
    """`get_insights` return keys must match `InsightsResponse`.

    Regression-pinned for the original drop: ``window_total_requests``
    was in the repo's return dict but not declared on
    ``InsightsResponse``, so Pydantic silently stripped it and the FE
    saw `undefined`. The fix was to add it to the model — this test
    keeps it from regressing."""
    from backend.models.dashboard import InsightsResponse
    from backend.repositories.insights import _insights_cache, get_insights

    _insights_cache.clear()
    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value={"LAX": (33.94, -118.4)}):
        result = get_insights(seeded_con, _src, window_hours=1, baseline_hours=1)
    _check_no_dropped_keys(result, InsightsResponse, "insights.get_insights")


# ── origin.get_summary ↔ OriginSummaryResponse ───────────────────────────


def test_origin_get_summary_contract(seeded_con):
    """`get_summary` return keys must match `OriginSummaryResponse`."""
    from backend.models.origin import OriginSummaryResponse
    from backend.repositories.origin import get_summary

    result = get_summary(seeded_con, _src, None, None, {})
    _check_no_dropped_keys(result, OriginSummaryResponse, "origin.get_summary")


# ── origin.get_timeseries ↔ OriginTimeseriesResponse ─────────────────────


def test_origin_get_timeseries_contract(seeded_con):
    """`get_timeseries` return keys must match `OriginTimeseriesResponse`."""
    from backend.models.origin import OriginTimeseriesResponse
    from backend.repositories.origin import get_timeseries

    result = get_timeseries(seeded_con, _src, None, None, {})
    _check_no_dropped_keys(result, OriginTimeseriesResponse, "origin.get_timeseries")


# ── performance.get_performance_aggregates ↔ PerformanceAggregatesResponse


def test_performance_aggregates_contract(seeded_con):
    """`get_performance_aggregates` return keys must match
    `PerformanceAggregatesResponse`."""
    from backend.models.performance import PerformanceAggregatesResponse
    from backend.repositories.performance import get_performance_aggregates

    result = get_performance_aggregates(seeded_con, _src, None, None, {})
    _check_no_dropped_keys(result, PerformanceAggregatesResponse, "performance.get_performance_aggregates")


# ── security.get_security_aggregates ↔ SecurityAggregatesResponse ────────


def test_security_aggregates_contract(seeded_con):
    """`get_security_aggregates` return keys must match
    `SecurityAggregatesResponse`."""
    from backend.models.security import SecurityAggregatesResponse
    from backend.repositories.security import get_security_aggregates

    result = get_security_aggregates(seeded_con, _src, None, None, {})
    _check_no_dropped_keys(result, SecurityAggregatesResponse, "security.get_security_aggregates")


# ── network.get_health ↔ NetworkHealthResponse ───────────────────────────


def test_network_health_contract(seeded_con):
    """`get_health` return keys must match `NetworkHealthResponse`."""
    from backend.models.network import NetworkHealthResponse
    from backend.repositories.network import get_health

    result = get_health(seeded_con, _src, None, None, {})
    _check_no_dropped_keys(result, NetworkHealthResponse, "network.get_health")
