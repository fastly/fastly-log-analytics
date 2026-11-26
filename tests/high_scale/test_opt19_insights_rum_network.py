"""Regression tests for Round 19 insights, prewarmer, RUM, and network ClickHouse optimizations."""

from __future__ import annotations

import inspect
import re
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from backend.high_scale import rum as hs_rum
from backend.high_scale.ingest_controller import HighScaleService
from backend.high_scale.insights import evaluator
from backend.high_scale.insights.batches import (
    DETECTION_BATCHES,
    _build_latency_dim,
    _build_origin_perf,
)
from backend.high_scale.insights.sql_queries import DETECTIONS, _base_sql
from backend.high_scale.network import (
    _network_json_cache,
    _network_memo,
    _network_model_cache,
    get_pop_health,
    network_bundle,
    network_quality,
)
from backend.models.analytics import InsightsRequest, NetworkQualityRequest
from backend.utils.bounded_cache import CacheRegistry

pytestmark = pytest.mark.no_postgres


class _RecordingCHClient:
    def __init__(self, handler: Any = None) -> None:
        self.calls: list[tuple[str, dict[str, Any], dict[str, str] | None]] = []
        self._handler = handler
        self._http = SimpleNamespace(_transport=SimpleNamespace())

    def execute(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        *,
        settings: Mapping[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        self.calls.append((sql, dict(params or {}), dict(settings) if settings else None))
        if callable(self._handler):
            return self._handler(sql, params or {})
        return list(self._handler or [])


def test_raw_standalone_detections_are_not_fused_into_group_raw_standalone(monkeypatch: pytest.MonkeyPatch) -> None:
    fields = [
        "timestamp",
        "url",
        "country",
        "status",
        "ua",
        "referer",
        "ip",
        "resp_bytes",
        "ottfb",
        "pop",
        "rid",
        "prid",
        "edge",
    ]
    monkeypatch.setattr("backend.config.load_config", lambda _sid: {})
    monkeypatch.setattr("backend.config.active_log_field_ids", lambda _cfg: fields)
    monkeypatch.setattr("backend.utils.pop_utils.get_pop_lat_lon_map", lambda: {"SJC": (37.36, -121.93)})

    def _handler(sql: str, _params: dict[str, Any]) -> list[dict[str, Any]]:
        if "/* insight:history */" in sql:
            return [{"earliest": datetime.now(UTC) - timedelta(days=10), "window_total": 1000, "hours": []}]
        if "/* insight:fields */" in sql:
            return [{"fields": fields}]
        return []

    client = _RecordingCHClient(_handler)
    service = SimpleNamespace(service_id="svc-unfuse", client=client)
    req = InsightsRequest(window_size_hrs=24, baseline_hours=168)
    evaluator.insights_endpoint(service, req, None, None)  # type: ignore[arg-type]

    sqls = [sql for sql, _, _ in client.calls]
    assert not any("/* insight:group_raw_standalone */" in s for s in sqls)
    assert any("/* insight:crawler_disparity */" in s for s in sqls)
    assert any("/* insight:stale_browser_version */" in s for s in sqls)
    assert any("/* insight:shield_path_degradation */" in s for s in sqls)


def test_batch_network_asn_candidates_latency_dim_and_origin_perf_prewhere() -> None:
    nasn_batch = next(b for b in DETECTION_BATCHES if b.name == "network_asn")
    assert nasn_batch.build_candidates_sql is not None
    assert nasn_batch.parse_candidates is not None

    present = {
        "asn",
        "metro",
        "ploss",
        "rtt_var",
        "retrans",
        "tcp_rtt",
        "country",
        "delivery_rate",
        "elapsed",
        "ottfb",
        "url",
        "oconnect_ms",
    }
    active_nasn = {"network_asn_health", "asn_metro_performance", "metro_delivery_degradation"}
    cand_sql = nasn_batch.build_candidates_sql(active_nasn, present=present, rollup_span=None, filter_clause="")
    assert "/* insight:batch_network_asn_candidates */" in cand_sql
    assert "HAVING count() >= 20 LIMIT 501" in cand_sql
    assert nasn_batch.parse_candidates(active_nasn, []) is None

    parsed = nasn_batch.parse_candidates(active_nasn, [{"kind": "asn", "k": "7922"}, {"kind": "metro", "k": "501"}])
    assert parsed == {"asn": ["7922"], "metro": ["501"]}
    c_params: dict[str, Any] = {}
    built_sql, _ = nasn_batch.build_sql(
        active_nasn,
        present=present,
        rollup_span=None,
        filter_clause="",
        baseline_hours=168.0,
        candidates=parsed,
        candidate_params=c_params,
    )
    prewhere = re.search(r"FROM request_facts PREWHERE (.*?) SELECT ", built_sql)
    assert prewhere is not None
    assert "custom_fields['asn'] IN (" in prewhere.group(1)
    assert "custom_fields['metro'] IN (" in prewhere.group(1)
    assert "7922" in c_params.values() and "501" in c_params.values()

    lat_sql, _ = _build_latency_dim(
        {"pop_latency_regression"}, present=present, rollup_span=None, filter_clause="", baseline_hours=168.0
    )
    lat_pre = re.search(r"FROM request_facts PREWHERE (.*?) WHERE \(elapsed", lat_sql)
    assert lat_pre is not None and "custom_fields['elapsed'] != ''" in lat_pre.group(1)

    orig_batch = next(b for b in DETECTION_BATCHES if b.name == "origin_perf")
    assert orig_batch.build_candidates_sql is not None
    assert orig_batch.parse_candidates is not None
    assert orig_batch.parse_candidates({"origin_latency_spike", "timeout_split"}, []) is None
    orig_sql, _ = _build_origin_perf(
        {"origin_latency_spike", "timeout_split"},
        present=present,
        rollup_span=None,
        filter_clause="",
        baseline_hours=168.0,
    )
    orig_pre = re.search(r"FROM request_facts PREWHERE (.*?) WHERE \(ottfb", orig_sql)
    assert orig_pre is not None and "custom_fields['ottfb'] != ''" in orig_pre.group(1)


def test_standalone_crawler_stale_shield_use_baseline_sampling_and_window_guards() -> None:
    sample_clause = (
        "event_timestamp >= {window_start:DateTime64(3)} OR intDiv(toUnixTimestamp(event_timestamp), 300) % 8 = 0"
    )

    crawler_sql = _base_sql(DETECTIONS["crawler_disparity"], "crawler_disparity", baseline_hours=168.0)
    assert sample_clause in crawler_sql
    assert "intDiv(toUnixTimestamp(event_timestamp), 300) % 4 = 0" in crawler_sql
    assert crawler_sql.count("FROM base") == 1
    assert "GROUP BY GROUPING SETS" in crawler_sql
    assert crawler_sql.index("HAVING sum(cnt) >= 50") < crawler_sql.index("GROUPING SETS")

    stale_sql = _base_sql(DETECTIONS["stale_browser_version"], "stale_browser_version", baseline_hours=168.0)
    assert sample_clause in stale_sql
    assert "countIf(is_b) * 8 AS b_reqs" in stale_sql
    assert "IN (SELECT extract(custom_fields['ua']" in stale_sql
    assert "HAVING count() >= 500" in stale_sql

    shield_sql = _base_sql(DETECTIONS["shield_path_degradation"], "shield_path_degradation", baseline_hours=168.0)
    assert sample_clause in shield_sql
    assert "HAVING count() >= 5" in shield_sql


def test_history_cache_is_registered_and_reused_within_minute(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("backend.config.load_config", lambda _sid: {})
    monkeypatch.setattr("backend.config.active_log_field_ids", lambda _cfg: ["status", "url"])
    cache = evaluator._INSIGHTS_HISTORY_CACHE
    assert "high_scale.insights._INSIGHTS_HISTORY_CACHE" in CacheRegistry.names()
    assert cache.maxsize == 128
    assert cache.ttl_seconds == 60.0
    cache.clear()

    def _handler(sql: str, _params: dict[str, Any]) -> list[dict[str, Any]]:
        if "/* insight:history */" in sql:
            return [{"earliest": datetime.now(UTC) - timedelta(days=2), "window_total": 100, "hours": []}]
        if "/* insight:fields */" in sql:
            return [{"fields": ["status", "url"]}]
        return []

    client = _RecordingCHClient(_handler)
    service = SimpleNamespace(service_id="svc-hist-cache", client=client)
    req = InsightsRequest(window_size_hrs=24, baseline_hours=24)

    evaluator.insights_endpoint(service, req, None, None)  # type: ignore[arg-type]
    evaluator.insights_endpoint(service, req, None, None)  # type: ignore[arg-type]
    history_calls = [s for s, _, _ in client.calls if "/* insight:history */" in s]
    assert len(history_calls) == 1


def test_force_refresh_non_hs_service_also_applies_background_max_threads(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("backend.config.load_config", lambda _sid: {})
    monkeypatch.setattr("backend.config.active_log_field_ids", lambda _cfg: ["status", "url"])

    def _handler(sql: str, _params: dict[str, Any]) -> list[dict[str, Any]]:
        if "/* insight:history */" in sql:
            return [{"earliest": datetime.now(UTC) - timedelta(days=2), "window_total": 100, "hours": []}]
        if "/* insight:fields */" in sql:
            return [{"fields": ["status", "url"]}]
        return []

    client = _RecordingCHClient(_handler)
    service = SimpleNamespace(service_id="svc-bg-nonhs", client=client)
    req = InsightsRequest(window_size_hrs=24, baseline_hours=24)
    evaluator.insights_endpoint(service, req, None, None, force_refresh=True)  # type: ignore[arg-type]
    assert client.calls
    for _, _, settings in client.calls:
        assert settings is not None
        assert settings.get("max_threads") == evaluator._BACKGROUND_QUERY_MAX_THREADS


def test_rum_analytics_constrains_req_edge_with_rum_vitals_prewhere() -> None:
    class _RumClient:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
            self.calls.append(sql)
            if "rum_analytics_counts" in sql:
                return [{"beacons": 1, "pageviews": 1, "interactions": 0, "errors": 0, "sampled": 0}]
            return []

    client = _RumClient()
    svc = HighScaleService("svc-rum-opt19", client, b"secret", None)
    hs_rum.rum_analytics(svc, "2026-11-25T00:00:00Z", "2026-11-26T00:00:00Z")
    sql = next(s for s in client.calls if "rum_analytics_slowest" in s)
    assert "FROM request_facts PREWHERE " in sql
    assert "_cf_rid IN (SELECT req_id FROM rum_vitals_facts PREWHERE " in sql


def test_network_prewhere_filtered_pop_sampling_and_bundle_concurrency(monkeypatch: pytest.MonkeyPatch) -> None:
    _network_memo.cache.clear()
    _network_model_cache.clear()
    _network_json_cache.clear()
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    end = datetime(2026, 11, 26, 0, 0, tzinfo=UTC)
    start = end - timedelta(hours=24)

    client = _RecordingCHClient(
        lambda sql, _p: (
            [
                {
                    "pop": "SJC",
                    "requests": 100,
                    "errors": 1,
                    "p50_rtt_us": 10000.0,
                    "p95_ttfb_ms": 20.0,
                    "cache_hits": 80,
                    "bandwidth_bytes": 1000,
                }
            ]
            if "FROM request_facts" in sql
            else []
        )
    )
    svc = HighScaleService("svc-pop-flt", client, b"s", None)
    get_pop_health(svc, start, end, filters={"country": {"mode": "include", "values": ["US"]}})
    pop_flt_sql = client.calls[0][0]
    assert "event_timestamp >= toDateTime(" in pop_flt_sql

    _network_memo.cache.clear()
    client.calls.clear()
    client._handler = lambda sql, _p: (
        [
            {"dimension": "country", "label": "US", "rtt_ms": 20.0, "reqs": 100},
            {"dimension": "asn", "label": "13335", "rtt_ms": 20.0, "reqs": 100},
        ]
        if "FROM network_minute_dimensions" in sql
        else [{"rtt_ms": 20.0, "ttfb_ms": 40.0, "cache_state": "HIT"}] * 200
    )
    network_quality(svc, NetworkQualityRequest(), start.isoformat(), end.isoformat())
    dims_sql = next(s for s, _, _ in client.calls if "FROM network_minute_dimensions" in s)
    assert "FROM network_minute_dimensions PREWHERE " in dims_sql
    assert "custom_fields['tcp_rtt'] != '0'" in dims_sql
    scatter_sql = next(s for s, _, _ in client.calls if "AS ttfb_ms" in s)
    assert "custom_fields['tcp_rtt'] != '0'" in scatter_sql
    assert "custom_fields['ttfb'] != '0'" in scatter_sql

    assert "max_workers=4" in inspect.getsource(network_bundle)
