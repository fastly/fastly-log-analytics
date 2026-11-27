"""Parity tests for High-Scale /origin, /assets-shield, and /fastly-value."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from backend.high_scale._common import build_published_projection_where

pytestmark = pytest.mark.no_postgres
from backend.high_scale.ingest_controller.service import assets_aggregates
from backend.high_scale.performance import value_summary
from backend.models.analytics import AssetsRequest, ValueSummaryRequest


def _empty_service(client: Any, service_id: str = "svc-parity-test") -> SimpleNamespace:
    empty_wm = SimpleNamespace(coverage_start=None, coverage_end=None, last_visible_event_id=None)
    return SimpleNamespace(
        service_id=service_id,
        client=client,
        watermark=lambda: empty_wm,
        watermark_for=lambda domain="request": empty_wm,
    )


def test_projection_where_uses_half_open_end_bound() -> None:
    sql, params = build_published_projection_where(
        "svc-parity-test",
        "origin_summary",
        "2026-11-24T16:00:00Z",
        "2026-11-24T18:00:00Z",
    )
    assert "bucket_start >= {start:DateTime64(3)}" in sql
    assert "bucket_start < {end:DateTime64(3)}" in sql
    assert "bucket_start <=" not in sql
    assert params["end"] == datetime(2026, 11, 24, 18, 0, tzinfo=UTC)


def test_assets_aggregates_uses_half_open_end_bound() -> None:
    sqls: list[str] = []

    class _Client:
        def execute(self, sql: str, params: dict | None = None) -> list[dict[str, Any]]:
            sqls.append(sql)
            return []

    svc = _empty_service(_Client())
    req = AssetsRequest(filters={}, start_time="2026-11-24T16:00:00Z", end_time="2026-11-24T18:00:00Z")
    assets_aggregates(svc, req, "2026-11-24T16:00:00Z", "2026-11-24T18:00:00Z")  # type: ignore[arg-type]
    assert sqls and all("event_timestamp < {end_time:DateTime64(3)}" in s for s in sqls)
    assert all("event_timestamp <=" not in s for s in sqls)


def test_value_summary_performance_only_prefix_cache_waf_null_and_half_open() -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    class _Client:
        def execute(self, sql: str, params: dict | None = None) -> list[dict[str, Any]]:
            calls.append((sql, dict(params or {})))
            if "value:combined_buckets" in sql:
                return [
                    {
                        "bucket": "2026-11-24T16:00:00Z",
                        "total": 100,
                        "hit_cnt": 80,
                        "miss_cnt": 10,
                        "pass_cnt": 10,
                        "synth_cnt": 0,
                        "bandwidth_saved": 8000,
                        "total_bytes": 10000,
                        "origin_cnt": 20,
                        "shield_hit": 0,
                        "shield_total": 0,
                        "threats_cnt": 0,
                        "sum_hit_elapsed": 80000.0,
                        "cnt_hit_elapsed": 80,
                        "sum_miss_elapsed": 100000.0,
                        "cnt_miss_elapsed": 10,
                        "waf_sample_cnt": 0,
                        "waf_inspected": 0,
                        "waf_blocked": 0,
                        "waf_logged": 0,
                        "waf_passed": 0,
                        "waf_sig_cnt": 0,
                        "http3_cnt": 10,
                        "h2_cnt": 80,
                        "h11_cnt": 10,
                        "other_proto_cnt": 0,
                        "tls_cnt": 100,
                        "ipv6_cnt": 0,
                    }
                ]
            if "value:p99" in sql:
                return [{"p99_ms": 42.5}]
            return []

    svc = _empty_service(_Client(), service_id="svc-perf-only")
    req_perf = ValueSummaryRequest(
        filters={},
        sections=["performance"],
        start_time="2026-11-24T16:00:00Z",
        end_time="2026-11-24T18:00:00Z",
    )
    res_perf = value_summary(svc, req_perf, "2026-11-24T16:00:00Z", "2026-11-24T18:00:00Z", sections={"performance"})
    assert res_perf.performance is not None
    assert res_perf.performance.cache_accel_factor == 10.0
    assert res_perf.performance.avg_hit_latency_ms == 1.0
    assert res_perf.performance.avg_miss_latency_ms == 10.0
    assert res_perf.performance.p99_latency_ms == 42.5

    bucket_sql = next(sql for sql, _ in calls if "value:combined_buckets" in sql)
    assert "event_timestamp < {end:DateTime64(3)}" in bucket_sql
    assert "startsWith(upper(custom_fields['cache']), 'HIT')" in bucket_sql
    assert "startsWith(upper(custom_fields['cache']), 'MISS')" in bucket_sql

    req_sec = ValueSummaryRequest(
        filters={},
        sections=["overview", "security"],
        start_time="2026-11-24T16:00:00Z",
        end_time="2026-11-24T18:00:00Z",
    )
    res_sec = value_summary(
        svc, req_sec, "2026-11-24T16:00:00Z", "2026-11-24T18:00:00Z", sections={"overview", "security"}
    )
    assert res_sec.overview is not None and res_sec.overview.threats_blocked is None
    assert res_sec.security is not None
    assert res_sec.security.waf_blocked is None
    assert res_sec.security.waf_logged is None
    assert res_sec.security.waf_passed is None


def test_value_summary_populates_io_section() -> None:
    class _Client:
        def execute(self, sql: str, params: dict | None = None) -> list[dict[str, Any]]:
            if "value:combined_buckets" in sql:
                return [{"bucket": "2026-11-24T16:00:00Z", "total": 50}]
            if "value:io_summary" in sql:
                return [
                    {
                        "image_requests": 20,
                        "image_bytes": 400000,
                        "io_count": 15,
                        "io_bytes": 150000,
                        "io_shield_bytes": 50000,
                        "input_bytes": 300000,
                        "output_bytes": 150000,
                        "actual_saved": 150000,
                    }
                ]
            if "value:io_formats" in sql:
                return [{"format": "webp", "cnt": 15}, {"format": "png", "cnt": 5}]
            if "value:io_opportunities" in sql:
                return [{"url": "/img/hero.png", "request_count": 5, "total_bytes": 250000, "avg_kb": 48.8}]
            if "value:io_conversions" in sql:
                return [{"input_format": "png", "output_format": "webp", "cnt": 15, "avg_ratio": 2.0}]
            if "value:io_series" in sql:
                return [
                    {
                        "bucket": "2026-11-24T16:00:00Z",
                        "io_count": 15,
                        "io_bytes": 150000,
                        "actual_saved": 150000,
                    }
                ]
            return []

    svc = _empty_service(_Client(), service_id="svc-io")
    req = ValueSummaryRequest(
        filters={},
        sections=["io"],
        start_time="2026-11-24T16:00:00Z",
        end_time="2026-11-24T18:00:00Z",
    )
    res = value_summary(svc, req, "2026-11-24T16:00:00Z", "2026-11-24T18:00:00Z", sections={"io"})
    assert res.io is not None
    assert res.io.total_requests == 50
    assert res.io.image_request_count == 20
    assert res.io.image_bandwidth_bytes == 400000
    assert res.io.io_transforms == 15
    assert res.io.io_actual_bandwidth_saved_bytes == 150000
    assert res.io.io_actual_compression_ratio == 2.0
    assert res.io.modern_format_pct == 75.0
    assert len(res.io.format_distribution) == 2
    assert len(res.io.optimization_opportunities) == 1
    assert len(res.io.io_format_conversion_pairs) == 1


def test_value_summary_waf_case_insensitive_and_ua_bot_fallback() -> None:
    calls: list[str] = []

    class _Client:
        def execute(self, sql: str, params: dict | None = None) -> list[dict[str, Any]]:
            calls.append(sql)
            if "value:combined_buckets" in sql:
                return [
                    {
                        "bucket": "2026-11-24T16:00:00Z",
                        "total": 10,
                        "hit_cnt": 5,
                        "miss_cnt": 5,
                        "pass_cnt": 0,
                        "synth_cnt": 0,
                        "bandwidth_saved": 500,
                        "total_bytes": 1000,
                        "origin_cnt": 5,
                        "shield_hit": 0,
                        "shield_total": 0,
                        "threats_cnt": 2,
                        "sum_hit_elapsed": 5000.0,
                        "cnt_hit_elapsed": 5,
                        "sum_miss_elapsed": 25000.0,
                        "cnt_miss_elapsed": 5,
                        "waf_sample_cnt": 10,
                        "waf_inspected": 9,
                        "waf_blocked": 2,
                        "waf_logged": 1,
                        "waf_passed": 6,
                        "waf_sig_cnt": 3,
                        "http3_cnt": 2,
                        "h2_cnt": 8,
                        "h11_cnt": 0,
                        "other_proto_cnt": 0,
                        "tls_cnt": 10,
                        "ipv6_cnt": 0,
                    }
                ]
            if "value:bots" in sql:
                return [{"bot_requests": 0, "verified_bots": 1, "ua_cnt": 10, "waf_sig_cnt": 3}]
            if "value:bot_uas" in sql:
                return [
                    {"ua": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)", "cnt": 4},
                    {"ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)", "cnt": 6},
                ]
            return []

    svc = _empty_service(_Client(), service_id="svc-waf-bots")
    req = ValueSummaryRequest(
        filters={},
        sections=["overview", "security", "bots"],
        start_time="2026-11-24T16:00:00Z",
        end_time="2026-11-24T18:00:00Z",
    )
    res = value_summary(
        svc, req, "2026-11-24T16:00:00Z", "2026-11-24T18:00:00Z", sections={"overview", "security", "bots"}
    )
    bucket_sql = next(sql for sql in calls if "value:combined_buckets" in sql)
    assert "lower(custom_fields['waf']) IN ('1', 'true')" in bucket_sql
    assert res.overview is not None and res.overview.threats_blocked == 2
    assert res.security is not None
    assert (res.security.waf_blocked, res.security.waf_logged, res.security.waf_passed) == (2, 1, 6)
    assert res.bots is not None
    assert res.bots.bot_requests == 4
    assert res.bots.verified_bots == 1
    assert [b.name for b in res.bots.top_bots] == ["Google Crawler"]
    assert res.bots.top_bots[0].count == 4


def test_value_summary_prunes_unused_bucket_columns_and_reuses_matching_reported_window(monkeypatch) -> None:
    calls: list[str] = []

    class _Client:
        def execute(self, sql: str, params: dict | None = None) -> list[dict[str, Any]]:
            calls.append(sql)
            if "value:combined_buckets" in sql:
                return [
                    {
                        "bucket": "2026-11-24T16:00:00Z",
                        "total": 40,
                        "hit_cnt": 30,
                        "miss_cnt": 10,
                        "pass_cnt": 0,
                        "synth_cnt": 0,
                        "bandwidth_saved": 3000,
                        "total_bytes": 4000,
                        "origin_cnt": 10,
                        "shield_hit": 5,
                        "shield_total": 10,
                        "threats_cnt": 0,
                        "sum_hit_elapsed": 1000.0,
                        "cnt_hit_elapsed": 30,
                        "sum_miss_elapsed": 5000.0,
                        "cnt_miss_elapsed": 10,
                        "waf_sample_cnt": 0,
                        "waf_inspected": 0,
                        "http3_cnt": 10,
                        "h2_cnt": 30,
                        "h11_cnt": 0,
                        "other_proto_cnt": 0,
                        "tls_cnt": 40,
                        "ipv6_cnt": 20,
                    }
                ]
            if "value:fastly_reported_window" in sql:
                return [{"requests": 25, "bandwidth_bytes": 2500}]
            return []

    from backend.repositories import value as value_repo

    monkeypatch.setattr(
        value_repo,
        "fetch_fastly_reported",
        lambda *_a, **_kw: {
            "window_start": "2026-11-24T16:00:00Z",
            "window_end": "2026-11-24T17:00:00Z",
            "requests": 42,
            "bandwidth_bytes": 4200,
            "hit_requests": 30,
        },
    )
    svc = _empty_service(_Client(), service_id="svc-prune-match")
    req = ValueSummaryRequest(
        filters={},
        sections=["overview", "caching", "network"],
        start_time="2026-11-24T16:00:00Z",
        end_time="2026-11-24T17:00:00Z",
    )
    res = value_summary(
        svc,
        req,
        "2026-11-24T16:00:00Z",
        "2026-11-24T17:00:00Z",
        sections={"overview", "caching", "network"},
    )
    bucket_sql = next(sql for sql in calls if "value:combined_buckets" in sql)
    assert "waf_logged" not in bucket_sql
    assert "waf_passed" not in bucket_sql
    assert "waf_sig" not in bucket_sql
    assert "custom_fields['ua']" not in bucket_sql
    assert "custom_fields['_bot_name']" not in bucket_sql
    assert not any("value:fastly_reported_window" in sql for sql in calls)
    assert res.fastly_reported is not None
    assert res.fastly_reported.logged_requests == 40
    assert res.fastly_reported.logged_bandwidth_bytes == 4000

    # When the Fastly-reported window is narrower than the request range, the window query still runs.
    calls.clear()
    monkeypatch.setattr(
        value_repo,
        "fetch_fastly_reported",
        lambda *_a, **_kw: {
            "window_start": "2026-11-24T16:00:00Z",
            "window_end": "2026-11-24T16:45:00Z",
            "requests": 26,
            "bandwidth_bytes": 2600,
            "hit_requests": 18,
        },
    )
    svc2 = _empty_service(_Client(), service_id="svc-prune-narrow")
    res2 = value_summary(
        svc2,
        req,
        "2026-11-24T16:00:00Z",
        "2026-11-24T17:00:00Z",
        sections={"overview", "caching", "network"},
    )
    assert any("value:fastly_reported_window" in sql for sql in calls)
    assert res2.fastly_reported is not None
    assert res2.fastly_reported.logged_requests == 25
    assert res2.fastly_reported.logged_bandwidth_bytes == 2500


def test_value_summary_io_url_extension_fallback_and_ctype_precedence() -> None:
    calls: list[str] = []

    class _Client:
        def execute(self, sql: str, params: dict | None = None) -> list[dict[str, Any]]:
            calls.append(sql)
            if "value:combined_buckets" in sql:
                return [{"bucket": "2026-11-24T16:00:00Z", "total": 10}]
            if "value:io_summary" in sql:
                return [{"image_requests": 4, "image_bytes": 120000, "io_count": 0}]
            return []

    svc = _empty_service(_Client(), service_id="svc-io-fallback")
    req = ValueSummaryRequest(
        filters={},
        sections=["io"],
        start_time="2026-11-24T16:00:00Z",
        end_time="2026-11-24T18:00:00Z",
    )
    value_summary(svc, req, "2026-11-24T16:00:00Z", "2026-11-24T18:00:00Z", sections={"io"})

    io_summary_sql = next(sql for sql in calls if "value:io_summary" in sql)
    io_formats_sql = next(sql for sql in calls if "value:io_formats" in sql)
    io_opps_sql = next(sql for sql in calls if "value:io_opportunities" in sql)

    for ext in (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico"):
        assert f"endsWith(lower(url), '{ext}')" in io_summary_sql
        assert f"endsWith(lower(url), '{ext}')" in io_formats_sql
    assert "= '' AND" in io_summary_sql
    assert "THEN 'jpeg'" in io_formats_sql
    for ext in (".jpg", ".jpeg", ".png", ".gif"):
        assert f"endsWith(lower(url), '{ext}')" in io_opps_sql
    assert "endsWith(lower(url), '.svg')" not in io_opps_sql
    assert "= '' AND" in io_opps_sql


def test_origin_drilldown_aligns_dimensions_shield_pop_and_avg_bytes() -> None:
    from backend.high_scale.origin import origin_drilldown
    from backend.models.analytics import OriginDrilldownRequest

    for dimension, expected_col, expect_shield_filter in [
        ("datacenter", "custom_fields['server_region']", False),
        ("protocol", "custom_fields['proto']", False),
        ("shield_pop", "custom_fields['pop']", True),
    ]:
        sqls: list[str] = []

        class _Client:
            def execute(self, sql: str, params: dict | None = None) -> list[dict[str, Any]]:
                sqls.append(sql)
                return []

        svc = _empty_service(_Client(), service_id=f"svc-dd-{dimension}")
        req = OriginDrilldownRequest(
            dimension=dimension,
            start_time="2026-11-24T16:00:00Z",
            end_time="2026-11-24T17:00:00Z",
        )
        origin_drilldown(svc, req, "2026-11-24T16:00:00Z", "2026-11-24T17:00:00Z")
        dd_sql = next(s for s in sqls if "origin:drilldown" in s)
        assert expected_col in dd_sql
        assert "avg(toInt64OrNull(custom_fields['obytes']))" in dd_sql
        if expect_shield_filter:
            assert "custom_fields['edge']" in dd_sql and "'false'" in dd_sql


def test_assets_aggregates_strips_query_includes_zstd_and_reports_missing_fields(monkeypatch) -> None:
    from backend import config as svcconfig

    sqls: list[str] = []

    class _Client:
        def execute(self, sql: str, params: dict | None = None) -> list[dict[str, Any]]:
            sqls.append(sql)
            return []

    monkeypatch.setattr(svcconfig, "load_config", lambda _sid: {"service_id": "svc-assets-parity"})
    monkeypatch.setattr(svcconfig, "active_log_field_ids", lambda _cfg: ["url", "cache"])
    svc = _empty_service(_Client(), service_id="svc-assets-parity")
    req = AssetsRequest(
        filters={},
        sections=["summary"],
        start_time="2026-11-24T16:00:00Z",
        end_time="2026-11-24T18:00:00Z",
    )
    res = assets_aggregates(svc, req, "2026-11-24T16:00:00Z", "2026-11-24T18:00:00Z")
    dims_sql = next(s for s in sqls if "assets:summary_dims" in s)
    assert "path_lower LIKE '%.js'" in dims_sql
    assert "url_lower LIKE '%.js'" not in dims_sql
    assert "'zstd'" in dims_sql
    assert res.missing_fields == ["ttl", "resp_header_content_encoding"]
