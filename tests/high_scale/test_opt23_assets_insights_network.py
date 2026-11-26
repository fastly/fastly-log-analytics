"""Unit tests for Round 23 High-Scale ClickHouse assets, insights, and network optimizations."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from backend.core.catalog import LOG_FIELD_CATALOG
from backend.core.clickhouse.client import ClickHouseClient
from backend.high_scale import network as hs_network
from backend.high_scale.ingest_controller import HighScaleService
from backend.high_scale.ingest_controller import service as hs_service
from backend.high_scale.insights import evaluator
from backend.high_scale.insights.batches import (
    _IP_ABUSE_CANDIDATE_LIMIT,
    DETECTION_BATCHES,
    _build_ip_abuse,
    _build_ip_abuse_candidates,
    _build_latency_dim,
    _build_network_asn,
    _build_origin_perf,
    _build_url_perf,
)
from backend.high_scale.insights.sql_queries import (
    DETECTIONS,
    _base_sql,
    window_sample_stride,
)
from backend.models.analytics import AssetsRequest, InsightsRequest, NetworkQualityRequest
from backend.utils.bounded_cache import CacheRegistry

pytestmark = pytest.mark.no_postgres

ALL_FIELDS = [f["id"] for f in LOG_FIELD_CATALOG]


class _FakeRealClickHouseClient(ClickHouseClient):
    """Fake ClickHouseClient that triggers real-CH SQL paths and runs rewrite_publication_fence."""

    def __init__(self, handler: Any = None, *, materialized_cf: bool = False) -> None:
        self.calls: list[tuple[str, dict[str, Any], dict[str, str] | None]] = []
        self._handler = handler
        self._http = SimpleNamespace(_transport=object())
        self._materialized_cf = materialized_cf

    def _has_materialized_cf_columns(self) -> bool:
        return self._materialized_cf

    def _get_pending_batches(self, _service_id: str) -> dict[str, tuple[str, ...]] | None:
        return None

    def execute(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        *,
        settings: Mapping[str, str] | None = None,
        rewrite_cf: bool = True,
    ) -> list[dict[str, Any]]:
        rewritten = self.rewrite_publication_fence(sql, params, rewrite_cf=rewrite_cf)
        self.calls.append((rewritten, dict(params or {}), dict(settings) if settings else None))
        if callable(self._handler):
            return self._handler(rewritten, params or {})
        return list(self._handler or [])


def _service(client: Any, sid: str = "svc-opt23") -> HighScaleService:
    now = datetime.now(UTC)
    return HighScaleService(
        service_id=sid,
        client=client,
        cursor_secret=b"opt23-test-secret-32-bytes-long!",
        request_watermark=lambda: now,
    )


@pytest.fixture(autouse=True)
def _stub_config_and_clear_caches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("backend.config.load_config", lambda _sid: {})
    monkeypatch.setattr("backend.config.active_log_field_ids", lambda _cfg: ALL_FIELDS)
    hs_service._memo.cache.clear()
    hs_service._memo._telemetry_cache.clear()
    hs_network._network_memo.cache.clear()
    hs_network._network_memo._telemetry_cache.clear()
    hs_network._SHIELD_ACTIVE_CACHE.clear()
    evaluator._INSIGHTS_CACHE.clear()
    evaluator._INSIGHTS_TELEMETRY_CACHE.clear()
    evaluator._INSIGHTS_PARTIAL_CACHE.clear()
    evaluator._INSIGHTS_HISTORY_CACHE.clear()
    evaluator._INSIGHTS_LAST_RESULT.clear()
    evaluator._INSIGHTS_REFRESH_FAILURES.clear()


def test_assets_aggregates_uses_prewhere_explicit_columns_and_rewrites_cf() -> None:
    def _handler(sql: str, _params: dict[str, Any]) -> list[dict[str, Any]]:
        if "assets:summary_dims" in sql:
            return [
                {
                    "asset_type": "JavaScript/CSS",
                    "cache_status": "MISS",
                    "content_encoding_norm": "uncompressed",
                    "is_compressed": 0,
                    "is_compressible": 1,
                    "requests": 100,
                    "bytes": 2_000_000,
                }
            ]
        return []

    real_client = _FakeRealClickHouseClient(_handler, materialized_cf=True)
    svc_real = _service(real_client, "svc-opt23-assets")
    req_unfiltered = AssetsRequest(filters={}, start_time="2026-11-25T10:00:00Z", end_time="2026-11-25T11:00:00Z")
    hs_service.assets_aggregates(svc_real, req_unfiltered, "2026-11-25T10:00:00Z", "2026-11-25T11:00:00Z")

    assert len(real_client.calls) == 2
    by_marker = {
        ("assets:summary_dims" if "assets:summary_dims" in sql else "assets:hotspots"): sql
        for sql, _, _ in real_client.calls
    }
    assert set(by_marker) == {"assets:summary_dims", "assets:hotspots"}
    for sql in by_marker.values():
        assert "SELECT *" not in sql
        assert "FROM request_facts PREWHERE " in sql
        assert "url != ''" in sql
        assert "custom_fields[" not in sql
        assert "_cf_cache" in sql
        assert "_cf_resp_bytes" in sql
        assert "_cf_ttl" in sql
        assert "path_lower" in sql
        assert (
            "GROUP BY url, cache, content_encoding, status\n" in sql
            or "GROUP BY url, cache, content_encoding, status" in sql
        )

    hotspots_sql = by_marker["assets:hotspots"]
    assert "SELECT kind, url, requests, metric_float, total_bytes, status, asset_type FROM (" in hotspots_sql
    assert "path_lower LIKE '%.js'" in hotspots_sql
    assert "lower(splitByChar('#', splitByChar('?', url)[1])[1]) LIKE" not in hotspots_sql

    # With filters on real CH, base clauses stay in PREWHERE and filter clauses go into WHERE.
    real_client.calls.clear()
    req_filtered = AssetsRequest(
        filters={"country": {"mode": "include", "values": ["US"]}},
        start_time="2026-11-25T10:05:00Z",
        end_time="2026-11-25T11:05:00Z",
    )
    hs_service.assets_aggregates(svc_real, req_filtered, "2026-11-25T10:05:00Z", "2026-11-25T11:05:00Z")
    filtered_dims = next(sql for sql, _, _ in real_client.calls if "assets:summary_dims" in sql)
    grouped_raw_sql = filtered_dims.split("FROM request_facts PREWHERE ", 1)[1].split(" GROUP BY", 1)[0]
    prewhere_part, where_part = grouped_raw_sql.rsplit(" WHERE ", 1)
    assert "url != ''" in prewhere_part
    assert "country" in where_part

    # Non-real client keeps WHERE without PREWHERE.
    fake_calls: list[str] = []
    mock_client = SimpleNamespace(
        execute=lambda sql, params=None: (fake_calls.append(sql), [])[1],
    )
    svc_mock = SimpleNamespace(service_id="svc-opt23-mock", client=mock_client)
    hs_service.assets_aggregates(svc_mock, req_unfiltered, "2026-11-25T10:00:00Z", "2026-11-25T11:00:00Z")  # type: ignore[arg-type]
    assert len(fake_calls) == 2
    for sql in fake_calls:
        assert "FROM request_facts WHERE " in sql
        assert "PREWHERE" not in sql


def test_crawler_disparity_window_sampling_and_cutwww_domain_referer(monkeypatch: pytest.MonkeyPatch) -> None:
    assert window_sample_stride(0.0) == 1
    assert window_sample_stride(12.0) == 1
    assert window_sample_stride(24.0) == 4
    assert window_sample_stride(48.0) == 6
    assert DETECTIONS["crawler_disparity"].window_sampled is True

    sql_24h_168h = _base_sql(
        DETECTIONS["crawler_disparity"], "crawler_disparity", baseline_hours=168.0, window_hours=24.0
    )
    assert (
        "((event_timestamp >= {window_start:DateTime64(3)} AND intDiv(toUnixTimestamp(event_timestamp), 300) % 4 = 0)"
        " OR (event_timestamp < {window_start:DateTime64(3)} AND intDiv(toUnixTimestamp(event_timestamp), 300) % 8 = 0))"
        in sql_24h_168h
    )
    assert "countIf(is_w) * 4 AS w_cnt" in sql_24h_168h
    assert "sumIf(coalesce(resp_bytes, 0), is_w) * 4 AS w_b" in sql_24h_168h
    assert "countIf(is_b) * 8 AS b_cnt" in sql_24h_168h
    assert "sumIf(coalesce(resp_bytes, 0), is_b) * 8 AS b_b" in sql_24h_168h
    assert "splitByChar('.', lower(cutWWW(domain(referer))))[1]" in sql_24h_168h
    assert "extract(referer," not in sql_24h_168h
    assert "__W_SCALE__" not in sql_24h_168h
    assert "__B_SCALE__" not in sql_24h_168h

    # Short window (12h) with 168h baseline preserves exact baseline-only sampling text and unscaled window counts.
    sql_12h_168h = _base_sql(
        DETECTIONS["crawler_disparity"], "crawler_disparity", baseline_hours=168.0, window_hours=12.0
    )
    assert (
        "AND (event_timestamp >= {window_start:DateTime64(3)} OR intDiv(toUnixTimestamp(event_timestamp), 300) % 8 = 0)"
        in sql_12h_168h
    )
    assert "countIf(is_w) AS w_cnt" in sql_12h_168h
    assert "sumIf(coalesce(resp_bytes, 0), is_w) AS w_b" in sql_12h_168h

    # 24h window with short baseline (12h) samples only window rows.
    sql_24h_12h = _base_sql(
        DETECTIONS["crawler_disparity"], "crawler_disparity", baseline_hours=12.0, window_hours=24.0
    )
    assert (
        "AND (event_timestamp < {window_start:DateTime64(3)} OR intDiv(toUnixTimestamp(event_timestamp), 300) % 4 = 0)"
        in sql_24h_12h
    )
    assert "countIf(is_w) * 4 AS w_cnt" in sql_24h_12h
    assert "countIf(is_b) AS b_cnt" in sql_24h_12h

    # Evaluator automatically passes window_hours to standalone detections.
    def _handler(q: str, _params: dict[str, Any]) -> list[dict[str, Any]]:
        if "/* insight:history */" in q:
            return [{"earliest": datetime.now(UTC) - timedelta(days=10), "window_total": 1000, "hours": []}]
        if "/* insight:fields */" in q:
            return [{"fields": ALL_FIELDS}]
        return []

    client = _FakeRealClickHouseClient(_handler)
    evaluator.insights_endpoint(
        _service(client, "svc-opt23-eval"),
        InsightsRequest(window_size_hrs=24, baseline_hours=168),
        None,
        None,
        force_refresh=True,
    )
    crawler_q = next(q for q, _, _ in client.calls if "/* insight:crawler_disparity */" in q)
    assert "countIf(is_w) * 4 AS w_cnt" in crawler_q


def test_batch_window_and_baseline_sampling_across_four_heavy_batches() -> None:
    present = set(ALL_FIELDS)
    combined_pred = (
        "((event_timestamp >= {window_start:DateTime64(3)} AND intDiv(toUnixTimestamp(event_timestamp), 300) % 4 = 0)"
        " OR (event_timestamp < {window_start:DateTime64(3)} AND intDiv(toUnixTimestamp(event_timestamp), 300) % 8 = 0))"
    )

    # 1. origin_perf
    orig_default, _ = _build_origin_perf(
        {"origin_latency_spike", "timeout_split"}, present=present, rollup_span=None, filter_clause=""
    )
    assert "intDiv(toUnixTimestamp(event_timestamp)" not in orig_default
    assert "countIf(is_w) AS w_reqs" in orig_default
    assert "countIf(is_w AND oconnect_ms IS NOT NULL) AS ts_w_total" in orig_default

    orig_sampled, _ = _build_origin_perf(
        {"origin_latency_spike", "timeout_split"},
        present=present,
        rollup_span=None,
        filter_clause="",
        baseline_hours=168.0,
        window_hours=24.0,
    )
    assert combined_pred in orig_sampled
    assert "countIf(is_w) * 4 AS w_reqs" in orig_sampled
    assert "countIf(is_w AND oconnect_ms IS NOT NULL) * 4 AS ts_w_total" in orig_sampled
    assert "countIf(is_b AND oconnect_ms IS NOT NULL) * 8 AS ts_b_total" in orig_sampled

    # 2. network_asn
    nasn_default, _ = _build_network_asn(
        {"network_asn_health", "asn_metro_performance", "metro_delivery_degradation"},
        present=present,
        rollup_span=None,
        filter_clause="",
    )
    assert "intDiv(toUnixTimestamp(event_timestamp)" not in nasn_default
    assert "countIf(is_w) AS nah_w_total" in nasn_default

    nasn_sampled, _ = _build_network_asn(
        {"network_asn_health", "asn_metro_performance", "metro_delivery_degradation"},
        present=present,
        rollup_span=None,
        filter_clause="",
        baseline_hours=168.0,
        window_hours=24.0,
    )
    assert combined_pred in nasn_sampled
    assert "countIf(is_w) * 4 AS nah_w_total" in nasn_sampled
    assert "countIf(is_b) * 8 AS nah_b_total" in nasn_sampled
    assert "countIf(is_w AND tcp_rtt > 0 AND country = 'US') * 4 AS amp_w_total" in nasn_sampled
    assert "countIf(is_b AND tcp_rtt > 0 AND country = 'US') * 8 AS amp_b_total" in nasn_sampled
    assert "countIf(is_w AND delivery_rate > 0) * 4 AS mdd_w_total" in nasn_sampled
    assert "countIf(is_b AND delivery_rate > 0) * 8 AS mdd_b_total" in nasn_sampled

    # 3. url_perf
    url_default, _ = _build_url_perf(
        {"latency_regression", "payload_compression_regression"}, present=present, rollup_span=None, filter_clause=""
    )
    assert "intDiv(toUnixTimestamp(event_timestamp)" not in url_default
    assert "countIf(is_w AND elapsed IS NOT NULL) AS l_w_total" in url_default

    url_sampled, _ = _build_url_perf(
        {"latency_regression", "payload_compression_regression"},
        present=present,
        rollup_span=None,
        filter_clause="",
        baseline_hours=168.0,
        window_hours=24.0,
    )
    assert combined_pred in url_sampled
    assert "countIf(is_w AND elapsed IS NOT NULL) * 4 AS l_w_total" in url_sampled
    assert "countIf(is_b AND elapsed IS NOT NULL) * 8 AS l_b_total" in url_sampled
    assert " * 4 AS c_w_total" in url_sampled
    assert " * 8 AS c_b_total" in url_sampled

    # 4. latency_dim
    lat_default, _ = _build_latency_dim(
        {"pop_latency_regression", "region_latency", "city_latency_regressions"},
        present=present,
        rollup_span=None,
        filter_clause="",
    )
    assert "intDiv(toUnixTimestamp(event_timestamp)" not in lat_default
    assert "countIf(is_w) AS w_total" in lat_default

    lat_sampled, _ = _build_latency_dim(
        {"pop_latency_regression", "region_latency", "city_latency_regressions"},
        present=present,
        rollup_span=None,
        filter_clause="",
        baseline_hours=168.0,
        window_hours=24.0,
    )
    assert combined_pred in lat_sampled
    assert "countIf(is_w) * 4 AS w_total" in lat_sampled
    assert "countIf(is_b) * 8 AS b_total" in lat_sampled


def test_ip_abuse_candidate_limit_2000_and_baseline_sampling() -> None:
    assert _IP_ABUSE_CANDIDATE_LIMIT == 2000
    present = {"ip", "url", "status", "cookie_session", "referer"}
    active = {"content_discovery", "credential_enumeration", "session_harvesting", "orphaned_deep_crawl"}

    cand_sql = _build_ip_abuse_candidates(active, present=present, rollup_span=None, filter_clause="")
    assert cand_sql.endswith("LIMIT 2001")

    batch = next(b for b in DETECTION_BATCHES if b.name == "ip_abuse")
    assert batch.parse_candidates is not None
    at_limit = [{"k": f"198.51.100.{i}"} for i in range(2000)]
    above_limit = [{"k": f"198.51.100.{i}"} for i in range(2001)]
    assert batch.parse_candidates(active, at_limit) == {"ip": [f"198.51.100.{i}" for i in range(2000)]}
    assert batch.parse_candidates(active, above_limit) == {"ip": None}

    default_sql, _ = _build_ip_abuse(active, present=present, rollup_span=None, filter_clause="")
    assert "intDiv(toUnixTimestamp(event_timestamp)" not in default_sql
    assert "countIf(is_b AND url != '' AND status = 404) AS cd_b_404" in default_sql

    sampled_sql, _ = _build_ip_abuse(
        active,
        present=present,
        rollup_span=None,
        filter_clause="",
        baseline_hours=168.0,
        candidates={"ip": ["198.51.100.1"]},
        candidate_params={},
    )
    assert (
        "event_timestamp >= {window_start:DateTime64(3)} OR intDiv(toUnixTimestamp(event_timestamp), 300) % 8 = 0"
        in sampled_sql
    )
    assert "countIf(is_b AND url != '' AND status = 404) * 8 AS cd_b_404" in sampled_sql
    assert "status IN (401, 403)) * 8 AS ce_b_denied" in sampled_sql
    assert " * 8 AS od_b_reqs" in sampled_sql
    assert "uniqCombinedIf(cookie_session, is_b AND cookie_session != '') AS sh_b_sessions" in sampled_sql


def test_shield_active_cache_and_network_quality_scatter_stride(monkeypatch: pytest.MonkeyPatch) -> None:
    assert "high_scale.network._SHIELD_ACTIVE_CACHE" in CacheRegistry.names()
    assert hs_network._SHIELD_ACTIVE_CACHE.maxsize == 128
    assert hs_network._SHIELD_ACTIVE_CACHE.ttl_seconds == 600.0

    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    end = datetime(2026, 11, 26, 0, 0, tzinfo=UTC)
    start_24h = end - timedelta(hours=24)
    start_7d = end - timedelta(days=7)

    # Shield cache skips the existence probe after a positive hit.
    def _shield_handler(sql: str, _p: dict[str, Any]) -> list[dict[str, Any]]:
        if "SELECT count() AS n" in sql:
            return [{"n": 1}]
        return [
            {"edge_pop": "SJC", "shield_pop": "IAD", "requests": 100, "p50_ms": 10.0, "p95_ms": 20.0, "p99_ms": 30.0}
        ]

    sh_client = _FakeRealClickHouseClient(_shield_handler)
    sh_svc = _service(sh_client, "svc-opt23-shield")
    hs_network._shielding_analysis(sh_svc, start_24h, end, None)
    assert sum(1 for sql, _, _ in sh_client.calls if "SELECT count() AS n" in sql) == 1
    assert len(sh_client.calls) == 2

    hs_network._shielding_analysis(sh_svc, start_24h, end, None)
    assert sum(1 for sql, _, _ in sh_client.calls if "SELECT count() AS n" in sql) == 1
    assert len(sh_client.calls) == 3

    # Edge-only (n=0) does NOT cache True, so subsequent calls still probe.
    edge_only_client = _FakeRealClickHouseClient(lambda sql, _p: [{"n": 0}] if "SELECT count() AS n" in sql else [])
    edge_svc = _service(edge_only_client, "svc-opt23-edge")
    res_edge = hs_network._shielding_analysis(edge_svc, start_24h, end, None)
    assert res_edge.edge_only is True
    hs_network._shielding_analysis(edge_svc, start_24h, end, None)
    assert sum(1 for sql, _, _ in edge_only_client.calls if "SELECT count() AS n" in sql) == 2

    # Scatter uses stride 16 on <= 24h and accepts >= 50 rows without fallback.
    rollup_rows = [
        {"dimension": "country", "label": "US", "rtt_ms": 20.0, "reqs": 100},
        {"dimension": "asn", "label": "13335", "rtt_ms": 20.0, "reqs": 100},
    ]
    q_client_50 = _FakeRealClickHouseClient(
        lambda sql, _p: (
            rollup_rows
            if "FROM network_minute_dimensions" in sql
            else [{"rtt_ms": 20.0, "ttfb_ms": 40.0, "cache_state": "HIT"}] * 50
        )
    )
    q_svc_50 = _service(q_client_50, "svc-opt23-q50")
    res_q50 = hs_network.network_quality(q_svc_50, NetworkQualityRequest(), start_24h.isoformat(), end.isoformat())
    sc_calls_50 = [sql for sql, _, _ in q_client_50.calls if "AS ttfb_ms" in sql]
    assert len(sc_calls_50) == 1
    assert "line_ordinal % 16 = 0" in sc_calls_50[0]
    assert len(res_q50.scatter) == 50

    # Scatter falls back when < 50 rows on <= 24h.
    q_client_49 = _FakeRealClickHouseClient(
        lambda sql, _p: (
            rollup_rows
            if "FROM network_minute_dimensions" in sql
            else [{"rtt_ms": 20.0, "ttfb_ms": 40.0, "cache_state": "HIT"}] * 49
        )
    )
    q_svc_49 = _service(q_client_49, "svc-opt23-q49")
    hs_network.network_quality(q_svc_49, NetworkQualityRequest(), start_24h.isoformat(), end.isoformat())
    sc_calls_49 = [sql for sql, _, _ in q_client_49.calls if "AS ttfb_ms" in sql]
    assert len(sc_calls_49) == 2
    assert "line_ordinal % 16 = 0" in sc_calls_49[0]
    assert "line_ordinal" not in sc_calls_49[1]

    # Scatter uses stride 64 on > 24h (7d).
    q_client_7d = _FakeRealClickHouseClient(
        lambda sql, _p: (
            rollup_rows
            if "FROM network_minute_dimensions" in sql
            else [{"rtt_ms": 20.0, "ttfb_ms": 40.0, "cache_state": "HIT"}] * 50
        )
    )
    q_svc_7d = _service(q_client_7d, "svc-opt23-q7d")
    hs_network.network_quality(q_svc_7d, NetworkQualityRequest(), start_7d.isoformat(), end.isoformat())
    sc_calls_7d = [sql for sql, _, _ in q_client_7d.calls if "AS ttfb_ms" in sql]
    assert len(sc_calls_7d) == 1
    assert "line_ordinal % 64 = 0" in sc_calls_7d[0]
