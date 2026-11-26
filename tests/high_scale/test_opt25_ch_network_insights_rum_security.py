"""Unit tests for Round 25 High-Scale ClickHouse network, insights, RUM, and security optimizations."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

from backend.core.catalog import LOG_FIELD_CATALOG
from backend.core.clickhouse.client import ClickHouseClient
from backend.high_scale import network as hs_network
from backend.high_scale import rum as hs_rum
from backend.high_scale import security as hs_security
from backend.high_scale.ingest_controller import HighScaleService
from backend.high_scale.insights import evaluator
from backend.high_scale.insights.batches import (
    _build_latency_dim,
    _build_network_asn,
    _build_network_asn_candidates,
    _build_url_perf_candidates,
    _window_from,
)
from backend.high_scale.insights.rollup import ROLLUP_SHAPES, coverage_sql
from backend.models.analytics import InsightsRequest, NetworkBundleRequest, OriginShieldingAnalysisRequest

pytestmark = pytest.mark.no_postgres

ALL_FIELDS = [f["id"] for f in LOG_FIELD_CATALOG]


class _FakeRealCHClient(ClickHouseClient):
    """Fake ClickHouseClient that triggers real-CH paths and records executed SQL."""

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


def _service(client: Any, sid: str = "svc-opt25") -> HighScaleService:
    now = datetime.now(UTC)
    return HighScaleService(
        service_id=sid,
        client=client,
        cursor_secret=b"opt25-test-secret-32-bytes-long!",
        request_watermark=lambda: now,
    )


@pytest.fixture(autouse=True)
def _stub_and_clear_caches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("backend.config.load_config", lambda _sid: {})
    monkeypatch.setattr("backend.config.active_log_field_ids", lambda _cfg: ALL_FIELDS)
    hs_network._network_memo.cache.clear()
    hs_network._network_memo._telemetry_cache.clear()
    hs_network._network_model_cache.clear()
    hs_network._network_json_cache.clear()
    hs_network._pop_rollup_cache.clear()
    hs_network._SHIELD_ACTIVE_CACHE.clear()
    evaluator._INSIGHTS_CACHE.clear()
    evaluator._INSIGHTS_TELEMETRY_CACHE.clear()
    evaluator._INSIGHTS_PARTIAL_CACHE.clear()
    evaluator._INSIGHTS_HISTORY_CACHE.clear()
    evaluator._INSIGHTS_COVERAGE_CACHE.clear()
    evaluator._INSIGHTS_LAST_RESULT.clear()
    evaluator._INSIGHTS_REFRESH_FAILURES.clear()
    hs_rum._rum_memo.cache.clear()
    hs_security._security_memo.cache.clear()
    hs_security._security_memo._telemetry_cache.clear()


def test_network_sample_stride_14d_tier() -> None:
    assert hs_network._network_sample_stride(604800) == 100
    assert hs_network._network_sample_stride(1209599) == 100
    assert hs_network._network_sample_stride(1209600) == 500
    assert hs_network._network_sample_stride(2592000) == 500


def test_shield_active_cache_caches_false_and_samples_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    end = datetime(2026, 11, 26, 0, 0, tzinfo=UTC)
    start_30d = end - timedelta(days=30)

    edge_client = _FakeRealCHClient(lambda sql, _p: [{"n": 0}] if "SELECT count() AS n" in sql else [])
    edge_svc = _service(edge_client, "svc-opt25-edge")

    res1 = hs_network._shielding_analysis(edge_svc, start_30d, end, None)
    assert res1.has_data is False
    assert res1.edge_only is True
    assert len(edge_client.calls) == 1
    probe_sql = edge_client.calls[0][0]
    assert "SELECT count() AS n" in probe_sql
    assert "event_timestamp >= toDateTime(" in probe_sql

    # Second call within TTL hits _SHIELD_ACTIVE_CACHE == False with 0 ClickHouse queries.
    res2 = hs_network._shielding_analysis(edge_svc, start_30d, end, None)
    assert res2.has_data is False
    assert res2.edge_only is True
    assert len(edge_client.calls) == 1

    # origin_shielding_analysis also bypasses CH when _SHIELD_ACTIVE_CACHE is False.
    origin_res = hs_network.origin_shielding_analysis(
        edge_svc, OriginShieldingAnalysisRequest(), start_30d.isoformat(), end.isoformat()
    )
    assert origin_res.edge_only is True
    assert origin_res.has_data is False
    assert len(edge_client.calls) == 1


def test_network_bundle_shares_rollup_and_fact_and_stays_under_8_queries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    end = datetime(2026, 11, 26, 0, 0, tzinfo=UTC)
    start_30d = end - timedelta(days=30)

    def _handler(sql: str, _p: dict[str, Any]) -> list[dict[str, Any]]:
        if "SELECT count() AS n" in sql:
            return [{"n": 0}]
        if "FROM request_aggregates" in sql:
            return [{"asn": 13335, "cnt": 1000}]
        if "AS rtt_quants" in sql and "GROUP BY asn, bucket_ts" in sql:
            return [
                {
                    "asn": 13335,
                    "bucket_ts": int(start_30d.timestamp()),
                    "reqs": 50,
                    "throughput_bps": 1000.0,
                    "rtt_avg_us": 20000.0,
                    "rtt_quants": [10000.0, 20000.0, 40000.0, 60000.0],
                    "rtt_baseline_us": 15000.0,
                    "rtt_congestion_us": 5000.0,
                    "avg_ploss": 0.0,
                    "rtt_jitter_us": 1000.0,
                    "error_pct": 0.0,
                    "speed_map": {},
                }
            ]
        if "FROM network_minute_dimensions" in sql:
            return [
                {
                    "dimension": "country",
                    "label": "US",
                    "rtt_ms": 20.0,
                    "reqs": 500,
                    "pop_reqs": 0,
                    "pop_errors": 0,
                    "p50_rtt_us": None,
                    "p95_ttfb_ms": None,
                },
                {
                    "dimension": "asn",
                    "label": "13335",
                    "rtt_ms": 20.0,
                    "reqs": 500,
                    "pop_reqs": 0,
                    "pop_errors": 0,
                    "p50_rtt_us": None,
                    "p95_ttfb_ms": None,
                },
                {
                    "dimension": "pop",
                    "label": "SJC",
                    "rtt_ms": 15.0,
                    "reqs": 1000,
                    "pop_reqs": 1200,
                    "pop_errors": 12,
                    "p50_rtt_us": 15000.0,
                    "p95_ttfb_ms": 42.0,
                },
                {
                    "dimension": "region",
                    "label": "California",
                    "rtt_ms": 18.0,
                    "reqs": 250,
                    "pop_reqs": 0,
                    "pop_errors": 0,
                    "p50_rtt_us": None,
                    "p95_ttfb_ms": None,
                },
            ]
        if "AS cache_state" in sql:
            return [{"rtt_ms": 20.0, "ttfb_ms": 40.0, "cache_state": "HIT"}] * 60
        if "AS proto_map" in sql:
            return [
                {
                    "total_reqs": 10,
                    "total_bytes": 5000,
                    "rtt_quants": [10000.0, 20000.0, 30000.0, 40000.0],
                    "avg_retrans": 0.1,
                    "avg_ploss_pct": 0.05,
                    "avg_del_rate_mbps": 12.5,
                    "proto_map": {"HTTP/2": 10},
                    "pop_reqs_map": {"SJC": 10},
                    "pop_hits_map": {"SJC": 6},
                    "pop_bytes_map": {"SJC": 4000},
                }
            ]
        return []

    client = _FakeRealCHClient(_handler)
    svc = _service(client, "svc-opt25-bundle")
    with patch("backend.core.fos_client.get_asn_names", return_value={13335: "Cloudflare"}):
        resp = hs_network.network_bundle(svc, NetworkBundleRequest(), start_30d.isoformat(), end.isoformat())

    # Cold edge-only /network bundle stays strictly under 8 ClickHouse queries.
    assert len(client.calls) < 8, (
        f"Expected < 8 queries, got {len(client.calls)}: {[s[:80] for s, _, _ in client.calls]}"
    )
    assert sum(1 for sql, _, _ in client.calls if "FROM network_minute_dimensions" in sql) == 1
    assert resp.pop_health is not None and len(resp.pop_health) == 1
    sjc = resp.pop_health[0]
    assert sjc.pop == "SJC"
    assert sjc.requests == 1200
    assert sjc.errors == 12
    assert sjc.p50_rtt_us == 15000.0
    assert sjc.p95_ttfb_ms == 42.0
    assert sjc.cache_hit_rate == 60.0
    assert sjc.bandwidth_bytes == 480000
    assert resp.quality is not None and resp.quality.by_pop[0].value == "SJC"
    assert resp.transport is not None and resp.transport.total_requests == 5000

    # When sections=["quality", "pop_health"] (without transport), pop_health skips network_minute_dimensions.
    hs_network._network_memo.cache.clear()
    hs_network._network_model_cache.clear()
    hs_network._network_json_cache.clear()
    client2 = _FakeRealCHClient(
        lambda sql, p: (
            [{"pop": "SJC", "requests": 10, "cache_hits": 5, "bandwidth_bytes": 1000}]
            if ("FROM request_facts" in sql and "AS cache_hits" in sql and "UNION ALL" not in sql)
            else _handler(sql, p)
        )
    )
    svc2 = _service(client2, "svc-opt25-qp")
    with patch("backend.core.fos_client.get_asn_names", return_value={13335: "Cloudflare"}):
        qp_resp = hs_network.network_bundle(
            svc2, NetworkBundleRequest(sections=["quality", "pop_health"]), start_30d.isoformat(), end.isoformat()
        )
    assert sum(1 for sql, _, _ in client2.calls if "FROM network_minute_dimensions" in sql) == 1
    assert qp_resp.pop_health is not None and qp_resp.pop_health[0].requests == 1200


def test_insights_coverage_cache_ttl_900_ignores_live_hour_and_coverage_uses_prewhere() -> None:
    assert evaluator._INSIGHTS_COVERAGE_CACHE.ttl_seconds == 900.0
    cov_sql = coverage_sql()
    assert "FROM request_insight_hourly PREWHERE " in cov_sql
    assert (
        "AND bucket_start >= {coverage_start:DateTime64(3)} AND bucket_start < {coverage_end:DateTime64(3)} GROUP BY shape, hour"
        in cov_sql
    )

    def _hours(start: datetime, end: datetime) -> list[int]:
        hour = int(start.timestamp()) // 3600 * 3600
        return list(range(hour, int(end.timestamp()) + 1, 3600))

    live_checksum = 100
    baseline_mult = 7

    def _handler(sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        if "/* insight:history */" in sql:
            hours = _hours(params["baseline_start"], params["window_end"])
            win_start_ts = int(params["window_start"].timestamp()) // 3600 * 3600
            checksums = [str(live_checksum) if h >= win_start_ts else str(h * baseline_mult) for h in hours]
            return [
                {
                    "earliest": datetime.now(UTC) - timedelta(days=10),
                    "window_total": 5000,
                    "hours": hours,
                    "hour_rows": [10] * len(hours),
                    "hour_checksums": checksums,
                }
            ]
        if "/* insight:fields */" in sql:
            return [{"fields": ALL_FIELDS}]
        if "/* insight:rollup_coverage */" in sql:
            hours = _hours(params["coverage_start"], params["coverage_end"] - timedelta(seconds=1))
            return [
                {
                    "shape": shape,
                    "hour": str(h),
                    "row_count": "10",
                    "batch_checksum": str(h * baseline_mult),
                }
                for shape in ROLLUP_SHAPES
                for h in hours
            ]
        return []

    client = _FakeRealCHClient(_handler)
    svc = _service(client, "svc-opt25-cov")

    evaluator._evaluate(svc, 24.0, 168.0, None, "2026-11-26T12:00:00Z", False, "", {}, frozenset(ALL_FIELDS))
    assert sum(1 for q, _, _ in client.calls if "/* insight:rollup_coverage */" in q) == 1

    # Writes in live/window hours change live_checksum, which must NOT bust _INSIGHTS_COVERAGE_CACHE.
    live_checksum = 999999
    evaluator._INSIGHTS_HISTORY_CACHE.clear()
    evaluator._evaluate(svc, 24.0, 168.0, None, "2026-11-26T12:00:00Z", False, "", {}, frozenset(ALL_FIELDS))
    assert sum(1 for q, _, _ in client.calls if "/* insight:rollup_coverage */" in q) == 1

    # Changes inside [coverage_start, coverage_end) DO bust _INSIGHTS_COVERAGE_CACHE.
    baseline_mult = 11
    evaluator._INSIGHTS_HISTORY_CACHE.clear()
    evaluator._evaluate(svc, 24.0, 168.0, None, "2026-11-26T12:00:00Z", False, "", {}, frozenset(ALL_FIELDS))
    assert sum(1 for q, _, _ in client.calls if "/* insight:rollup_coverage */" in q) == 2


def test_insights_batches_window_sampling_and_candidate_prewhere_optimizations() -> None:
    win_from_sampled = _window_from(row_filter="url != ''", sample_stride=4)
    assert "intDiv(toUnixTimestamp(event_timestamp), 300) % 4 = 0" in win_from_sampled

    present = set(ALL_FIELDS)
    lat_24h, _ = _build_latency_dim(
        {"pop_latency_regression", "region_latency", "city_latency_regressions"},
        present=present,
        rollup_span=None,
        filter_clause="",
        baseline_hours=168.0,
        window_hours=24.0,
    )
    assert "intDiv(toUnixTimestamp(event_timestamp), 300) % 4 = 0" in lat_24h.split("win_cities")[0] or (
        "GROUP BY custom_fields['city'] HAVING count() * 4 >= 10" in lat_24h
    )
    assert "GROUP BY custom_fields['city'] HAVING count() * 4 >= 10" in lat_24h

    # url_perf candidates sample 24h window and tighten latency/compression HAVING predicates.
    url_cand_24h = _build_url_perf_candidates(
        {"latency_regression", "payload_compression_regression"},
        present=present,
        rollup_span=None,
        filter_clause="",
        window_hours=24.0,
    )
    assert "intDiv(toUnixTimestamp(event_timestamp), 300) % 4 = 0" in url_cand_24h
    assert ">= 200000" in url_cand_24h
    assert " * 4 >= 5" in url_cand_24h

    # network_asn candidates sample 24h window; when asn falls back to None, win_asns uses w_stride and prewhere is kept.
    nasn_cand_24h = _build_network_asn_candidates(
        {"network_asn_health", "asn_metro_performance", "metro_delivery_degradation"},
        present=present,
        rollup_span=None,
        filter_clause="",
        window_hours=24.0,
    )
    assert "intDiv(toUnixTimestamp(event_timestamp), 300) % 4 = 0" in nasn_cand_24h
    assert "HAVING count() * 4 >= 20" in nasn_cand_24h

    nasn_partial_cands, _ = _build_network_asn(
        {"network_asn_health", "asn_metro_performance", "metro_delivery_degradation"},
        present=present,
        rollup_span=None,
        filter_clause="",
        baseline_hours=168.0,
        window_hours=24.0,
        candidates={"asn": None, "metro": ["501"]},
        candidate_params={},
    )
    pre_part = nasn_partial_cands.split("FROM request_facts PREWHERE ", 1)[1].split(" SELECT ", 1)[0]
    assert "custom_fields['metro'] IN (" in pre_part or "custom_fields['asn'] != ''" in pre_part
    assert "HAVING count() * 4 >= 20" in nasn_partial_cands

    # Evaluator passes window_hours to candidate builders.
    def _eval_handler(q: str, _params: dict[str, Any]) -> list[dict[str, Any]]:
        if "/* insight:history */" in q:
            return [{"earliest": datetime.now(UTC) - timedelta(days=10), "window_total": 1000, "hours": []}]
        if "/* insight:fields */" in q:
            return [{"fields": ALL_FIELDS}]
        return []

    eval_client = _FakeRealCHClient(_eval_handler)
    evaluator.insights_endpoint(
        _service(eval_client, "svc-opt25-cand-wh"),
        InsightsRequest(window_size_hrs=24, baseline_hours=168),
        None,
        None,
        force_refresh=True,
    )
    url_cand_call = next(q for q, _, _ in eval_client.calls if "/* insight:batch_url_perf_candidates */" in q)
    assert "intDiv(toUnixTimestamp(event_timestamp), 300) % 4 = 0" in url_cand_call


def test_rum_slowest_sql_uses_array_join_without_union_all(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _FakeRealCHClient(
        lambda sql, _p: (
            [{"beacons": 5, "pageviews": 5, "interactions": 0, "errors": 0, "sampled": 0}]
            if "rum_analytics_counts" in sql
            else (
                [
                    {
                        "kind": "url",
                        "k": "/slow",
                        "requests": 10,
                        "avg_ms": 400.0,
                        "p50_ms": 380.0,
                        "p95_ms": 700.0,
                        "p99_ms": 900.0,
                        "matched_requests": 5,
                        "edge_avg_ms": 100.0,
                        "edge_p50_ms": 90.0,
                        "edge_p95_ms": 180.0,
                        "edge_p99_ms": 220.0,
                    },
                    {
                        "kind": "asn",
                        "k": "13335",
                        "requests": 10,
                        "avg_ms": 400.0,
                        "p50_ms": 380.0,
                        "p95_ms": 700.0,
                        "p99_ms": 900.0,
                        "matched_requests": 5,
                        "edge_avg_ms": 100.0,
                        "edge_p50_ms": 90.0,
                        "edge_p95_ms": 180.0,
                        "edge_p99_ms": 220.0,
                    },
                ]
                if "rum_analytics_slowest" in sql
                else []
            )
        )
    )
    svc = _service(client, "svc-opt25-rum")
    monkeypatch.setattr(
        "backend.repositories.utils.response_shaping.asn.resolve_asn_names_map",
        lambda _sid, _asns: {13335: "CLOUDFLARENET"},
    )
    res = hs_rum.rum_analytics(svc, "2026-11-25T00:00:00Z", "2026-11-26T00:00:00Z")
    slowest_sql = next(q for q, _, _ in client.calls if "/* rum_analytics_slowest */" in q)
    assert "UNION ALL" not in slowest_sql
    assert (
        "ARRAY JOIN [('url', if(resource_url != '', resource_url, '')), ('asn', if(asn_key != '' AND asn_key != '0', asn_key, ''))] AS kind_k"
        in slowest_sql
    )
    assert "WHERE kind_k.2 != ''" in slowest_sql
    assert "LIMIT 20 BY kind" in slowest_sql
    assert res["slowest_urls"][0]["url"] == "/slow"
    assert res["slowest_networks"][0]["asn"] == "AS13335"


def test_security_folds_verified_bots_ts_into_overview_5in1_and_reduces_queries() -> None:
    t0 = 1_790_002_800  # exact hour boundary 2026-09-21T15:00:00Z
    t1 = t0 + 300  # same hour, 5-minute bucket 15:05:00Z

    def _handler(sql: str, _params: dict[str, Any]) -> list[dict[str, Any]]:
        if "/* hs_security:overview_5in1 */" in sql:
            return [
                {
                    "bucket_ts": t0,
                    "total_rows": 100,
                    "tls_populated": 80,
                    "ipv6_rows": 20,
                    "h_map": {"0-256B": 100},
                    "c_map": {"1 (New)": 100},
                    "p_map": {"hosting": 10},
                    "vb_map": {"search-engine": 4, "ai": 2},
                },
                {
                    "bucket_ts": t1,
                    "total_rows": 100,
                    "tls_populated": 80,
                    "ipv6_rows": 40,
                    "h_map": {"0-256B": 100},
                    "c_map": {"1 (New)": 100},
                    "p_map": {},
                    "vb_map": {"search-engine": 6},
                },
            ]
        return []

    client = _FakeRealCHClient(_handler)
    req = SimpleNamespace(filters={}, bucket_seconds=300)
    end = datetime.now(UTC)
    start = end - timedelta(hours=24)

    with (
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws-opt25"),
        patch("backend.utils.bot_sources.build_well_known_bots_matcher", return_value=lambda _ua: False),
        patch("backend.core.fos_client.get_asn_names", return_value={}),
        patch("backend.high_scale.aggregate_query.can_use_hourly_rollup", return_value=None),
    ):
        res = hs_security.security_aggregates(
            _service(client, "svc-opt25-sec"),
            req,
            start.isoformat(),
            end.isoformat(),
            sections={
                "fingerprint_coverage",
                "req_size_dist",
                "top_ips_header",
                "ipv6_adoption",
                "proxy_dist",
                "conn_reuse_dist",
                "verified_bots_ts",
                "ngwaf_verified_bots",
                "ngwaf_verified_bots_ts",
                "top_bots",
                "proxies",
            },
        )

    assert not any("/* hs_security:verified_bots_ts */" in sql for sql, _, _ in client.calls)
    overview_sql = next(sql for sql, _, _ in client.calls if "/* hs_security:overview_5in1 */" in sql)
    assert "vb_map" in overview_sql
    assert "300" in overview_sql

    dumped = res.model_dump()
    # Both 5-minute buckets within the same hour aggregate into ONE hourly ipv6_adoption point: (20+40)/(100+100)*100 = 30.0%.
    assert dumped["ipv6_adoption"] == [{"time": "2026-09-21T15:00:00Z", "pct": 30.0}]
    # verified_bots_ts keeps 5-minute bucket_seconds granularity.
    assert dumped["verified_bots_ts"] == [
        {"time": "2026-09-21T15:00:00Z", "bot_type": "ai", "count": 2},
        {"time": "2026-09-21T15:00:00Z", "bot_type": "search-engine", "count": 4},
        {"time": "2026-09-21T15:05:00Z", "bot_type": "search-engine", "count": 6},
    ]

    # When bucket_seconds > 3600 or 3600 % bucket_seconds != 0, verified_bots_ts runs standalone.
    for bad_bs in (7200, 500):
        hs_security._security_memo.cache.clear()
        hs_security._security_memo._telemetry_cache.clear()
        client.calls.clear()
        hs_security.security_aggregates(
            _service(client, f"svc-opt25-sec-{bad_bs}"),
            SimpleNamespace(filters={}, bucket_seconds=bad_bs),
            start.isoformat(),
            end.isoformat(),
            sections={"ipv6_adoption", "proxy_dist", "verified_bots_ts"},
        )
        assert any("/* hs_security:verified_bots_ts */" in sql for sql, _, _ in client.calls)
