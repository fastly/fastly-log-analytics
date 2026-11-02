"""Unit tests for cross-engine response shaping helpers in ``backend.repositories.utils.response_shaping``."""

from __future__ import annotations

import importlib
import math
from datetime import UTC, datetime
from typing import Any

import pytest

from backend.repositories.utils import response_shaping as rs

# ── 1. ASN Label Enrichment ──────────────────────────────────────────────────


def test_parse_asn_int_handles_valid_and_invalid_inputs() -> None:
    assert rs.parse_asn_int(15169) == 15169
    assert rs.parse_asn_int("15169") == 15169
    assert rs.parse_asn_int("  13335 ") == 13335
    assert rs.parse_asn_int(None) is None
    assert rs.parse_asn_int(True) is None
    assert rs.parse_asn_int(False) is None
    assert rs.parse_asn_int("") is None
    assert rs.parse_asn_int("not-an-asn") is None


def test_resolve_and_format_asn_labels(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.core import duckdb as db_mod

    monkeypatch.setattr(
        db_mod,
        "get_asn_names",
        lambda svc, asns: {15169: "Google LLC", 13335: "Cloudflare, Inc."},
    )

    names = rs.resolve_asn_names_map("svc-1", ["15169", 13335, None, "invalid"])
    assert names == {15169: "Google LLC", 13335: "Cloudflare, Inc."}
    assert rs.resolve_asn_names_map("svc-1", [None, "bad"]) == {}

    assert rs.format_asn_display_label(15169, names) == "Google LLC (15169)"
    assert rs.format_asn_display_label(99999, names) == "AS99999"
    assert rs.format_asn_display_label("unknown", names, non_int_fallback="N/A") == "N/A"
    assert rs.format_asn_display_label(None, names) == ""

    label_map = rs.build_asn_label_map("svc-1", [15169, "13335", 15169, None])
    assert label_map == {
        15169: "Google LLC (15169)",
        13335: "Cloudflare, Inc. (13335)",
    }


def test_enrich_asn_rows_and_performance_asn_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.core import duckdb as db_mod

    monkeypatch.setattr(
        db_mod,
        "get_asn_names",
        lambda svc, asns: {15169: "Google LLC"},
    )

    rows = [
        {"value": "15169", "count": 100},
        {"value": "64512", "count": 20},
        {"value": "unknown", "count": 5},
    ]
    rs.enrich_asn_rows(rows, "svc-1", asn_key="value")
    assert rows[0]["label"] == "Google LLC (15169)"
    assert rows[1]["label"] == "AS64512"
    assert rows[2]["label"] == "unknown"

    tuple_rows = [("15169", 50, 12.3, 10.0, 25.0, 40.0)]
    dict_rows = [
        {
            "value": "64512",
            "req_count": 30,
            "avg_ms": 15.0,
            "p50_ms": 11.0,
            "p95_ms": 28.0,
            "p99_ms": 45.0,
        }
    ]
    shaped_tuples = rs.shape_performance_asn_rows("svc-1", tuple_rows)
    shaped_dicts = rs.shape_performance_asn_rows("svc-1", dict_rows)
    assert shaped_tuples == [
        {
            "asn": 15169,
            "label": "Google LLC (15169)",
            "requests": 50,
            "avg": 12.3,
            "p50": 10.0,
            "p95": 25.0,
            "p99": 40.0,
        }
    ]
    assert shaped_dicts == [
        {
            "asn": 64512,
            "label": "AS64512",
            "requests": 30,
            "avg": 15.0,
            "p50": 11.0,
            "p95": 28.0,
            "p99": 45.0,
        }
    ]


def test_finish_security_proxies_and_insight_asn_context(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.core import duckdb as db_mod

    monkeypatch.setattr(db_mod, "get_asn_names", lambda svc, asns: {15169: "Google LLC"})
    monkeypatch.setattr(db_mod, "_get_dma_map", lambda: {"807": "San Francisco-Oakland-San Jose"})

    payload = rs.finish_security_proxies(
        "svc-1",
        {"active_proxies_count": 2, "total_requests_count": 10, "distance_mismatches_count": 1},
        [("Direct Connection", 8), {"type": "Active Tunnel / Proxy", "count": 2}],
        [{"asn": "15169", "count": 4}],
        [{"ip": "203.0.113.42", "asn": 15169, "risk_level": "High"}],
        mask_ips=True,
    )
    assert payload["active_proxies_count"] == 2
    assert payload["tunnel_requests_count"] == 10
    assert payload["distance_mismatches_count"] == 1
    assert payload["traffic_quality"] == [
        {"label": "Direct Connection", "value": 80.0},
        {"label": "Active Tunnel / Proxy", "value": 20.0},
    ]
    assert payload["suspicious_isps"] == [{"isp": "Google LLC", "asn": 15169, "count": 4}]
    assert payload["active_clients"][0]["asn_name"] == "Google LLC"
    assert "asn" not in payload["active_clients"][0]
    assert payload["active_clients"][0]["ip"] != "203.0.113.42"

    ctx = rs.build_insight_asn_context("svc-1", [(15169, "807")], "asn_metro_anomaly", include_dma=True)
    assert ctx["asn_names"] == {15169: "Google LLC"}
    assert ctx["dma_map"] == {"807": "San Francisco-Oakland-San Jose"}
    assert rs.build_insight_asn_context("svc-1", [(1,)], "error_spikes") == {}


# ── 2. Time-Bucket Alignment & Zero-Filling ──────────────────────────────────


def test_chart_interval_normalization_and_bucket_alignment() -> None:
    assert rs.normalize_chart_interval("5m") == "5 minutes"
    assert rs.normalize_chart_interval("1 HOUR") == "1 hour"
    assert rs.normalize_chart_interval("invalid", default="1 minute") == "1 minute"
    assert rs.chart_interval_seconds("5m") == 300
    assert rs.chart_interval_seconds(None, default=60) == 60

    start = datetime(2026, 7, 1, 12, 0, 10, tzinfo=UTC)
    end = datetime(2026, 7, 1, 12, 2, 5, tzinfo=UTC)
    buckets = rs.generate_epoch_aligned_buckets(start, end, 60)
    assert buckets == [
        "2026-07-01T12:00:00",
        "2026-07-01T12:01:00",
        "2026-07-01T12:02:00",
    ]

    padded = rs.pad_timeseries_buckets(
        start,
        end,
        60,
        {"2026-07-01 12:01:00": {"requests": 42}},
        {"requests": 0},
    )
    assert padded == [
        {"bucket": "2026-07-01 12:00:00", "requests": 0},
        {"bucket": "2026-07-01 12:01:00", "requests": 42},
        {"bucket": "2026-07-01 12:02:00", "requests": 0},
    ]

    assert rs.select_insight_bucket_interval(1.5) == "1 minute"
    assert rs.select_insight_bucket_interval(1.5, clickhouse=True) == "INTERVAL 1 MINUTE"
    assert rs.select_insight_bucket_interval(24.0) == "15 minutes"
    assert rs.select_insight_bucket_interval(24.0, clickhouse=True) == "INTERVAL 15 MINUTE"
    assert rs.select_insight_bucket_interval(900.0) == "1 day"
    assert rs.select_insight_bucket_interval(900.0, clickhouse=True) == "INTERVAL 1 DAY"


# ── 3. Latency & Percentile Normalization ────────────────────────────────────


def test_latency_and_percentile_normalization() -> None:
    assert rs.opt_float("12.3456", 2) == 12.35
    assert rs.opt_float(float("nan"), reject_non_finite=True) is None
    assert rs.float_or_zero(None) == 0.0
    assert rs.float_or_zero(float("inf")) == 0.0
    assert rs.us_to_ms(12500, 2) == 12.5
    assert rs.us_to_ms(None) is None

    pct = rs.shape_latency_percentiles(10.123, 8.0, 20.0, 35.0, p75=14.0, include_p75=True, ndigits=1)
    assert pct == {"avg": 10.1, "p50": 8.0, "p75": 14.0, "p95": 20.0, "p99": 35.0}

    pop_res = rs.shape_pop_latency_rows(
        [("SJC", 100, 5.0, 10.0), ("IAD", 80, 6.0, 12.0), ("NRT", 20, 30.0, 50.0)],
        include_median_when_empty=True,
    )
    assert pop_res["has_data"] is True
    assert pop_res["median_p95_ms"] == 12.0
    elevated_by_pop = {r["pop"]: r["elevated"] for r in pop_res["rows"]}
    assert elevated_by_pop == {"SJC": False, "IAD": False, "NRT": True}

    wf = rs.shape_waterfall_avg(1.5, None, "3.25", 0)
    assert wf == {
        "avg": {
            "edge_processing": 1.5,
            "origin_wait": 0.0,
            "origin_download": 3.25,
            "client_download": 0.0,
        }
    }

    score = rs.compute_network_health_score(100_000_000, 10_000, 0.0, 5_000, 0.0)
    assert 90.0 <= score <= 100.0
    assert math.isclose(rs.haversine_km(37.7749, -122.4194, 37.7749, -122.4194), 0.0, abs_tol=1e-6)


# ── 4. Empty-Response Scaffolding ────────────────────────────────────────────


def test_empty_response_scaffolding_builders() -> None:
    origin_empty = rs.empty_origin_aggregates(section_timings=[])
    assert origin_empty["has_data"] is False
    assert origin_empty["slow_urls"] == {"has_data": False, "rows": []}

    perf_empty = rs.empty_performance_aggregates(sections={"top_urls", "waterfall"})
    assert perf_empty == {"top_urls": [], "waterfall": {}}

    sec_empty = rs.empty_security_aggregates()
    for k in rs.SECURITY_EMPTY_CORE_SECTIONS:
        assert sec_empty[k] == []

    proxies_empty = rs.empty_security_proxies()
    assert proxies_empty["active_proxies_count"] == 0
    assert proxies_empty["active_clients"] == []

    sess_empty = rs.empty_sessions_response(page=2, limit=25, has_ja4=True, min_reqs_flag=500)
    assert sess_empty["sessions"] == []
    assert sess_empty["page"] == 2
    assert sess_empty["has_ja4"] is True
    assert sess_empty["min_reqs_flag"] == 500

    dash_empty = rs.empty_dashboard_aggregates(["url", "status"], interval="5 minutes")
    assert dash_empty["data"] == {"url": {"top": [], "total": 0}, "status": {"top": [], "total": 0}}
    assert dash_empty["interval"] == "5 minutes"

    insights_env = rs.build_insights_envelope(
        [{"id": "error_spikes"}],
        window_start="2026-07-01T00:00:00Z",
        window_end="2026-07-01T01:00:00Z",
        window_total_requests=123,
    )
    assert insights_env["insights"] == [{"id": "error_spikes"}]
    assert insights_env["window_total_requests"] == 123

    query_env = rs.build_query_response_envelope(
        columns=["url"],
        data=[{"url": "/a"}],
        total_rows=1,
        truncated=False,
        elapsed_ms=14.7,
        debug_queries=[],
        debug_calls=[],
        section_timings=[],
        explain_plan="SCAN",
    )
    assert query_env["row_count"] == 1
    assert query_env["elapsed_ms"] == 14
    assert query_env["explain_plan"] == "SCAN"


# ── 5. Top-N Row Formatting & Domain Breakdown Shaping ───────────────────────


def test_top_n_and_breakdown_shaping() -> None:
    keyed = rs.shape_keyed_rows([("/index.html", 10)], ("url", "requests"))
    assert keyed == {"has_data": True, "rows": [{"url": "/index.html", "requests": 10}]}

    status_from_dicts = rs.shape_status_code_rows(
        [{"value": 200, "requests": 75}, {"value": 500, "requests": 25}],
        round_pct=True,
    )
    assert status_from_dicts == {
        "has_data": True,
        "rows": [
            {"status": 200, "count": 75, "pct": 75.0},
            {"status": 500, "count": 25, "pct": 25.0},
        ],
    }

    proto = rs.shape_protocol_distribution([("h3", 60), ("h2", 40)])
    assert proto == [
        {"protocol": "h3", "requests": 60, "share_pct": 60.0},
        {"protocol": "h2", "requests": 40, "share_pct": 40.0},
    ]

    assert rs.parse_metro_code("807.0") == 807
    assert rs.parse_metro_code("bad") is None

    speed_mix = rs.build_asn_speed_mix([(15169, "broadband", 80), (15169, "mobile", 20)])
    assert speed_mix == {15169: {"broadband": 0.8, "mobile": 0.2}}

    deduped = rs.deduplicate_metro_leaderboard(
        [
            {"city": "San Francisco, US", "total_reqs": 100, "health_score": 90.0},
            {"city": "San Francisco, US", "total_reqs": 100, "health_score": 80.0},
        ]
    )
    assert deduped == [{"city": "San Francisco, US", "total_reqs": 200, "health_score": 85.0}]

    breakdown = rs.build_cache_collapse_breakdown(70, 20, 5, 100)
    assert breakdown == {"hits": 70, "misses": 20, "passes": 5, "other": 5}

    point = rs.shape_cache_collapse_timeline_point("2026-07-01T00:00:00Z", 100, 60, 20, 0.9)
    assert point == {
        "bucket": "2026-07-01T00:00:00Z",
        "expected_hits": 72.0,
        "real_hits": 60,
        "misses": 20,
        "total_requests": 100,
        "hit_rate": 0.75,
    }


@pytest.mark.parametrize(
    ("repo_mod_name", "hs_mod_name"),
    [
        ("backend.repositories.origin", "backend.high_scale.origin"),
        ("backend.repositories.performance", "backend.high_scale.performance"),
        ("backend.repositories.cmcd", "backend.high_scale.cmcd"),
        ("backend.repositories.network", "backend.high_scale.network"),
        ("backend.repositories.security", "backend.high_scale.security"),
        ("backend.repositories.sessions", "backend.high_scale.sessions"),
        ("backend.repositories.dashboard", "backend.high_scale.dashboard"),
        ("backend.repositories.insights.repository", "backend.high_scale.insights"),
        ("backend.repositories.query", "backend.high_scale.query"),
    ],
)
def test_cross_engine_modules_import_cleanly(repo_mod_name: str, hs_mod_name: str) -> None:
    repo_mod: Any = importlib.import_module(repo_mod_name)
    hs_mod: Any = importlib.import_module(hs_mod_name)
    assert repo_mod is not None
    assert hs_mod is not None
