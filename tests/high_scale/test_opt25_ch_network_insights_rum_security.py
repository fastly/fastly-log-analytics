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
    _base_cte,
    _build_latency_dim,
    _build_misc,
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
    hs_network._pop_fact_cache.clear()
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


def test_shield_active_cache_caches_false_without_probe_sampling_and_scopes_short_windows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    end = datetime(2026, 11, 26, 0, 0, tzinfo=UTC)
    start_30d = end - timedelta(days=30)
    start_1h = end - timedelta(hours=1)

    # Short window (< 24h) with n=0 caches False only under window-scoped key, NOT service-wide.
    short_client = _FakeRealCHClient(lambda sql, _p: [{"n": 0}] if "SELECT count() AS n" in sql else [])
    short_svc = _service(short_client, "svc-opt25-short")
    res_1h_1 = hs_network._shielding_analysis(short_svc, start_1h, end, None)
    assert res_1h_1.has_data is False
    assert res_1h_1.edge_only is True
    assert len(short_client.calls) == 1
    probe_sql = short_client.calls[0][0]
    assert "SELECT count() AS n" in probe_sql
    assert "LIMIT 1" in probe_sql
    assert "event_timestamp >= toDateTime(" not in probe_sql

    # Repeat same 1h window hits window-scoped cache (0 extra CH calls).
    res_1h_2 = hs_network._shielding_analysis(short_svc, start_1h, end, None)
    assert res_1h_2.edge_only is True
    assert len(short_client.calls) == 1

    # Different/longer window (30d) still probes because short window did not poison service-wide key.
    res_30d = hs_network._shielding_analysis(short_svc, start_30d, end, None)
    assert res_30d.edge_only is True
    assert len(short_client.calls) == 2

    # 30d window (>= 86400s) with n=0 caches False under service-wide key.
    res_30d_2 = hs_network._shielding_analysis(short_svc, start_30d, end, None)
    assert res_30d_2.edge_only is True
    assert len(short_client.calls) == 2

    # origin_shielding_analysis also bypasses CH when service-wide _SHIELD_ACTIVE_CACHE is False.
    origin_res = hs_network.origin_shielding_analysis(
        short_svc, OriginShieldingAnalysisRequest(), start_30d.isoformat(), end.isoformat()
    )
    assert origin_res.edge_only is True
    assert origin_res.has_data is False
    assert len(short_client.calls) == 2

    # _invalidate_network_caches clears _SHIELD_ACTIVE_CACHE so the next call re-probes.
    hs_network._invalidate_network_caches(short_svc.service_id)
    assert len(hs_network._SHIELD_ACTIVE_CACHE) == 0
    hs_network._shielding_analysis(short_svc, start_30d, end, None)
    assert len(short_client.calls) == 3


def test_standalone_network_quality_and_transport_share_pop_caches_with_pop_health(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from backend.models.analytics import NetworkQualityRequest

    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    end = datetime(2026, 11, 26, 0, 0, tzinfo=UTC)
    start_30d = end - timedelta(days=30)

    def _handler(sql: str, _p: dict[str, Any]) -> list[dict[str, Any]]:
        if "FROM network_minute_dimensions" in sql and "UNION ALL" in sql:
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
            ]
        if "FROM network_minute_dimensions" in sql:
            return [
                {
                    "pop": "SJC",
                    "req_count": 1200,
                    "errors": 12,
                    "p50_rtt_us": 15000.0,
                    "p95_ttfb_ms": 42.0,
                }
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
    svc = _service(client, "svc-opt25-standalone-share")
    assert hs_network._pop_fact_cache.ttl_seconds == 60.0
    assert hs_network._pop_fact_cache.maxsize == 128

    # 1. Standalone network_quality populates _pop_rollup_cache even with pop_rollup_sink=None.
    with patch("backend.core.fos_client.get_asn_names", return_value={13335: "Cloudflare"}):
        hs_network.network_quality(svc, NetworkQualityRequest(), start_30d.isoformat(), end.isoformat())
    assert len(hs_network._pop_rollup_cache) == 1

    # 2. Standalone get_transport_metrics populates _pop_fact_cache.
    hs_network.get_transport_metrics(svc, start_30d.isoformat(), end.isoformat(), {})
    assert len(hs_network._pop_fact_cache) == 1

    # 3. Subsequent standalone get_pop_health hits BOTH caches with 0 ClickHouse queries!
    calls_before = len(client.calls)
    pop_res = hs_network.get_pop_health(svc, start_30d, end)
    assert len(client.calls) == calls_before
    assert len(pop_res.data) == 1
    assert pop_res.data[0].pop == "SJC"
    assert pop_res.data[0].requests == 1200
    assert pop_res.data[0].errors == 12
    assert pop_res.data[0].cache_hit_rate == 60.0
    assert pop_res.data[0].bandwidth_bytes == 480000

    # 4. _invalidate_network_caches clears _pop_fact_cache too.
    hs_network._invalidate_network_caches(svc.service_id)
    assert len(hs_network._pop_fact_cache) == 0
    assert len(hs_network._pop_rollup_cache) == 0

    # 5. Standalone transport then pop-health (with cold rollup cache) only queries network_minute_dimensions, not request_facts.
    hs_network._network_memo.cache.clear()
    client2 = _FakeRealCHClient(_handler)
    svc2 = _service(client2, "svc-opt25-fact-only")
    hs_network.get_transport_metrics(svc2, start_30d.isoformat(), end.isoformat(), {})
    calls_after_tr = len(client2.calls)
    pop_res2 = hs_network.get_pop_health(svc2, start_30d, end)
    new_sqls = [sql for sql, _, _ in client2.calls[calls_after_tr:]]
    assert len(new_sqls) == 1
    assert "FROM network_minute_dimensions" in new_sqls[0]
    assert "request_facts" not in new_sqls[0]
    assert pop_res2.data[0].requests == 1200


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

    # Evaluator passes window_start/window_end so candidate & batch queries use monotonic PK range sampling.
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
    assert "event_timestamp >= toDateTime(" in url_cand_call
    assert "intDiv(toUnixTimestamp(event_timestamp), 300) %" not in url_cand_call

    # End-to-end 24h/168h evaluation: every batch and standalone sql_queries query uses monotonic toDateTime(..., 'UTC') slices.
    all_batch_and_standalone = [
        q
        for q, _, _ in eval_client.calls
        if (
            "/* insight:batch_" in q
            or "/* insight:crawler_disparity */" in q
            or "/* insight:stale_browser_version */" in q
            or "/* insight:shield_path_degradation */" in q
        )
    ]
    assert all_batch_and_standalone
    for q in all_batch_and_standalone:
        assert "intDiv(toUnixTimestamp(event_timestamp), 300) %" not in q, q[:120]
    sampled_markers = (
        "/* insight:batch_misc */",
        "/* insight:batch_url_cache */",
        "/* insight:batch_latency_dim */",
        "/* insight:batch_url_perf_candidates */",
        "/* insight:batch_network_asn_candidates */",
        "/* insight:crawler_disparity */",
    )
    for marker in sampled_markers:
        matched = [q for q in eval_client.calls if marker in q[0]]
        assert matched, f"Missing {marker}"
        assert "toDateTime(" in matched[0][0] and "'UTC')" in matched[0][0], marker

    # Direct _base_cte (fact & mixed) and _window_from emit monotonic PK ranges when timestamps are supplied.
    w_end = datetime(2026, 11, 26, 12, 30, tzinfo=UTC)
    w_start = w_end - timedelta(hours=24)
    b_start = w_start - timedelta(hours=144)
    r_span = (datetime(2026, 11, 20, 13, 0, tzinfo=UTC), datetime(2026, 11, 25, 12, 0, tzinfo=UTC))

    mixed_sql, mixed_src = _build_misc(
        {"cache_hit_cliff", "method_drift"},
        present=present,
        rollup_span=r_span,
        filter_clause="",
        baseline_hours=168.0,
        baseline_start=b_start,
        window_start=w_start,
        window_end=w_end,
    )
    assert mixed_src == "mixed"
    mixed_raw, mixed_rollup = mixed_sql.split("UNION ALL", 1)
    assert "intDiv(toUnixTimestamp(event_timestamp), 300) %" not in mixed_raw
    assert "event_timestamp >= toDateTime(" in mixed_raw
    assert "toUInt64(8)" in mixed_raw
    assert "request_count AS n" in mixed_rollup

    fact_cte, fact_src = _base_cte(
        "test_pk",
        [("url", "url")],
        weighted=True,
        baseline_sample_stride=8,
        window_sample_stride=4,
        baseline_start=b_start,
        window_start=w_start,
        window_end=w_end,
    )
    assert fact_src == "fact"
    assert "intDiv(toUnixTimestamp(event_timestamp), 300) %" not in fact_cte
    assert "event_timestamp >= toDateTime(" in fact_cte
    assert "if(event_timestamp < {window_start:DateTime64(3)}, toUInt64(8), toUInt64(4)) AS n" in fact_cte

    win_pk = _window_from(row_filter="url != ''", sample_stride=4, window_start=w_start, window_end=w_end)
    assert "intDiv(toUnixTimestamp(event_timestamp), 300) %" not in win_pk
    assert "event_timestamp >= toDateTime(" in win_pk


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


def test_security_bundle_folds_tls_fingerprints_and_top_ips_header_to_at_most_7_queries() -> None:
    t0 = 1_790_002_800

    def _handler(sql: str, _params: dict[str, Any]) -> list[dict[str, Any]]:
        if "/* hs_security:tls_and_ips */" in sql:
            return [
                {"kind": "fp", "k": "sha-1", "v1": 4, "v2": 100},
                {"kind": "ip", "k": "203.0.113.5", "v1": 8192, "v2": 0},
            ]
        if "/* hs_security:overview_5in1 */" in sql:
            return [
                {
                    "bucket_ts": t0,
                    "total_rows": 100,
                    "tls_populated": 80,
                    "ipv6_rows": 20,
                    "h_map": {"0-256B": 100},
                    "c_map": {"1 (New)": 100},
                    "p_map": {},
                    "vb_map": {},
                }
            ]
        return []

    client = _FakeRealCHClient(_handler)
    req = SimpleNamespace(filters={}, bucket_seconds=300)
    end = datetime.now(UTC)
    start = end - timedelta(hours=24)
    bundle_sections = {
        "ngwaf_verified_bots",
        "ngwaf_verified_bots_ts",
        "wellknown_bots",
        "bot_efficiency",
        "tls_fingerprints",
        "fingerprint_coverage",
        "req_size_dist",
        "top_ips_header",
        "ipv6_adoption",
        "proxy_dist",
        "conn_reuse_dist",
        "proxies",
    }

    with (
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws-opt25-tls"),
        patch("backend.utils.bot_sources.build_well_known_bots_matcher", return_value=lambda _ua: False),
        patch("backend.core.fos_client.get_asn_names", return_value={}),
        patch("backend.high_scale.aggregate_query.can_use_hourly_rollup", return_value=None),
    ):
        res = hs_security.security_aggregates(
            _service(client, "svc-opt25-sec-bundle"),
            req,
            start.isoformat(),
            end.isoformat(),
            sections=bundle_sections,
            mask_ips=True,
        )

    assert len(client.calls) <= 7, (
        f"Expected <= 7 queries for security_bundle, got {len(client.calls)}: {[s[:60] for s, _, _ in client.calls]}"
    )
    assert any("/* hs_security:tls_and_ips */" in sql for sql, _, _ in client.calls)
    assert not any("/* hs_security:tls_fingerprints */" in sql for sql, _, _ in client.calls)
    assert not any("/* hs_security:top_ips_header */" in sql for sql, _, _ in client.calls)
    dumped = res.model_dump()
    assert dumped["tls_fingerprints"] == [{"fingerprint": "sha-1", "ip_count": 4, "request_count": 100}]
    assert dumped["top_ips_header"] == [{"ip": "203.0.113.xxx", "max_header": 8192}]

    # Follow-up request for tls_fingerprints alone hits per-section memo with 0 new queries.
    calls_before = len(client.calls)
    with patch("backend.config.get_ngwaf_workspace_id", return_value="ws-opt25-tls"):
        res_single = hs_security.security_aggregates(
            _service(client, "svc-opt25-sec-bundle"),
            req,
            start.isoformat(),
            end.isoformat(),
            sections={"tls_fingerprints"},
            mask_ips=True,
        )
    assert len(client.calls) == calls_before
    assert [r.model_dump() for r in res_single.tls_fingerprints] == dumped["tls_fingerprints"]
