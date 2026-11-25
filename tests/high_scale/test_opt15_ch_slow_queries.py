"""Regression tests for Opt 15 ClickHouse slow-query and CPU optimizations."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from backend.high_scale.ingest_controller import HighScaleService
from backend.high_scale.insights.batches import (
    _build_beaconing,
    _build_fingerprint_spread,
    _build_ip_abuse,
    _build_network_asn,
    _build_url_perf,
    _build_window_url,
)
from backend.high_scale.insights.evaluator import _MULTIPLEX_GROUPS
from backend.high_scale.network import (
    _network_json_cache,
    _network_memo,
    _network_model_cache,
    _network_sample_stride,
    get_pop_health,
)


class _RecordingCHClient:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self._rows = rows or []
        self._http = SimpleNamespace(_transport=SimpleNamespace())

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        self.calls.append((sql, dict(params or {})))
        return list(self._rows)


def test_network_sample_stride_tiers() -> None:
    assert _network_sample_stride(3600) == 1
    assert _network_sample_stride(21599) == 1
    assert _network_sample_stride(21600) == 5
    assert _network_sample_stride(43200) == 20
    assert _network_sample_stride(86400) == 20
    assert _network_sample_stride(172800) == 50
    assert _network_sample_stride(604800) == 100


def test_pop_health_uses_rollup_even_for_short_spans(monkeypatch: pytest.MonkeyPatch) -> None:
    _network_memo.cache.clear()
    _network_model_cache.clear()
    _network_json_cache.clear()
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    end = datetime(2026, 11, 25, 13, 0, tzinfo=UTC)
    start = end - timedelta(hours=1)
    client = _RecordingCHClient(
        rows=[
            {
                "kind": "rollup",
                "pop": "SJC",
                "req_count": 500,
                "errors": 5,
                "p50_rtt_us": 12000.0,
                "p95_ttfb_ms": 45.0,
                "cache_hits": 0,
                "bandwidth_bytes": 0,
            },
            {
                "kind": "fact",
                "pop": "SJC",
                "req_count": 500,
                "errors": None,
                "p50_rtt_us": None,
                "p95_ttfb_ms": None,
                "cache_hits": 400,
                "bandwidth_bytes": 100000,
            },
        ]
    )
    service = HighScaleService(service_id="svc-1", client=client, cursor_secret=b"s", request_watermark=None)
    result = get_pop_health(service, start, end, filters=None)

    assert len(client.calls) == 1
    sql = client.calls[0][0]
    assert "FROM network_minute_dimensions" in sql
    assert "quantileTDigest" not in sql
    assert len(result.data) == 1
    assert result.data[0].pop == "SJC"
    assert result.data[0].requests == 500
    assert result.data[0].errors == 5
    assert result.data[0].cache_hit_rate == 80.0


def test_insights_multiplex_groups_bound_raw_fact_branches() -> None:
    raw_groups = {name: members for name, members in _MULTIPLEX_GROUPS if not name.startswith("group_rollup_")}
    assert raw_groups, "expected raw-fact multiplex groups"
    for name, members in raw_groups.items():
        assert len(members) <= 3, f"{name} has too many raw-fact branches: {len(members)}"


def test_insights_batches_use_prewhere_uniq_combined_and_single_pass_quantiles() -> None:
    present = {
        "url",
        "elapsed",
        "ttl",
        "hits",
        "age",
        "status",
        "resp_bytes",
        "ua",
        "oretries",
        "ip",
        "cookie_session",
        "ja4",
        "tls_ciphers_sha",
        "p_type",
        "asn",
        "metro",
        "ploss",
        "rtt_var",
        "retrans",
        "tcp_rtt",
        "country",
        "delivery_rate",
        "sec",
        "resp_header_content_encoding",
    }

    win_sql, _ = _build_window_url({"tail_latency"}, present=present, rollup_span=None, filter_clause="")
    assert "FROM request_facts PREWHERE " in win_sql
    assert "quantilesTDigestIf(0.50, 0.99)" in win_sql
    assert "quantileTDigestIf(" not in win_sql

    url_sql, _ = _build_url_perf(
        {"latency_regression", "payload_compression_regression"}, present=present, rollup_span=None, filter_clause=""
    )
    assert "FROM request_facts PREWHERE " in url_sql
    assert "WHERE url != '' AND url IN (SELECT url FROM base WHERE is_w" in url_sql

    abuse_sql, _ = _build_ip_abuse(
        {"content_discovery", "credential_enumeration", "session_harvesting", "orphaned_deep_crawl"},
        present=present,
        rollup_span=None,
        filter_clause="",
    )
    assert "uniqCombinedIf(" in abuse_sql
    assert "uniqExact" not in abuse_sql

    fp_sql, _ = _build_fingerprint_spread(
        {"botnet_grouping", "cipher_spread", "residential_fingerprint_dispersion"},
        present=present,
        rollup_span=None,
        filter_clause="",
    )
    assert "uniqCombinedIf(" in fp_sql
    assert "uniqExact" not in fp_sql

    beacon_sql, _ = _build_beaconing({"beaconing_detection"}, present=present, rollup_span=None, filter_clause="")
    assert "uniqCombined(" in beacon_sql
    assert "uniqExact" not in beacon_sql

    asn_sql, _ = _build_network_asn(
        {"network_asn_health", "asn_metro_performance", "metro_delivery_degradation"},
        present=present,
        rollup_span=None,
        filter_clause="",
    )
    assert "FROM request_facts PREWHERE " in asn_sql
    assert "custom_fields['asn'] != '' OR custom_fields['metro'] != ''" in asn_sql
