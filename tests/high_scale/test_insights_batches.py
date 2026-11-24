"""Unit tests for High-Scale Insights query batching and consolidation."""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from backend.core.catalog import LOG_FIELD_CATALOG
from backend.high_scale.insights import evaluator
from backend.high_scale.insights.rollup import ROLLUP_SHAPES
from backend.models.analytics import InsightsRequest
from backend.repositories.insights import registry

ALL_FIELDS = [f["id"] for f in LOG_FIELD_CATALOG]
EVERY_CARD = {definition.id for definition in registry.get_all()}


@pytest.fixture(autouse=True)
def _enable_all_log_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("backend.config.active_log_field_ids", lambda _cfg: ALL_FIELDS)
    monkeypatch.setattr("backend.utils.pop_utils.get_pop_lat_lon_map", lambda: {"SJC": (37.36, -121.93)})


class _BatchFakeClient:
    """Fake ClickHouse client that records every SQL query and returns canned rows per marker."""

    def __init__(
        self,
        rows: dict[str, list[dict[str, Any]]] | None = None,
        *,
        with_rollup: bool = False,
        fail: set[str] | None = None,
    ) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self._rows = rows or {}
        self._with_rollup = with_rollup
        self._fail = fail or set()

    @staticmethod
    def _hours(start: datetime, end: datetime) -> list[int]:
        hour = int(start.timestamp()) // 3600 * 3600
        return list(range(hour, int(end.timestamp()) + 1, 3600))

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        params = params or {}
        self.calls.append((sql, params))
        marker = re.search(r"/\* insight:(\w+) \*/", sql)
        assert marker, f"Missing insight marker in SQL: {sql[:120]}"
        name = marker.group(1)
        if name in self._fail:
            raise RuntimeError(f"boom:{name}")
        if name == "history":
            row: dict[str, Any] = {
                "earliest": params["window_end"] - timedelta(hours=400),
                "window_total": 5000,
            }
            if self._with_rollup and "groupArray(hour)" in sql:
                hours = self._hours(params["baseline_start"], params["window_end"])
                row |= {"hours": hours, "hour_rows": [10] * len(hours), "hour_checksums": [str(h * 7) for h in hours]}
            return [row]
        if name == "fields":
            return [{"fields": ALL_FIELDS}]
        if name == "rollup_coverage" and self._with_rollup:
            hours = self._hours(params["coverage_start"], params["coverage_end"] - timedelta(seconds=1))
            return [
                {"shape": shape, "hour": str(h), "row_count": "10", "batch_checksum": str(h * 7)}
                for shape in ROLLUP_SHAPES
                for h in hours
            ]
        return self._rows.get(name, [])

    def queries(self) -> dict[str, tuple[str, dict[str, Any]]]:
        return {re.search(r"/\* insight:(\w+) \*/", sql).group(1): (sql, params) for sql, params in self.calls}  # type: ignore[union-attr]


def _run(client: _BatchFakeClient, *, mask_ips: bool = False, window: float = 24.0, baseline: float = 168.0):
    service = SimpleNamespace(service_id="svc-opt", client=client)
    req = InsightsRequest(window_size_hrs=window, baseline_hours=baseline)
    return evaluator.insights_endpoint(service, req, None, None, mask_ips)  # type: ignore[arg-type]


def _cards(response: Any) -> dict[str, dict[str, Any]]:
    return {card["id"]: card for card in json.loads(response.model_dump_json(by_alias=True))["insights"]}


def test_full_evaluation_consolidates_queries_by_at_least_half() -> None:
    """A full 51-detector High-Scale evaluation must execute <= 27 ClickHouse queries (down from 53-54)."""
    client = _BatchFakeClient(with_rollup=True)
    cards = _cards(_run(client))
    assert set(cards) == EVERY_CARD
    assert all(card["severity"] == "clean" for card in cards.values())
    # 3 metadata (history, fields, rollup_coverage) + <= 24 detector/batch queries = <= 27 total
    assert len(client.calls) <= 27, (
        f"Expected <= 27 ClickHouse queries, got {len(client.calls)}: {list(client.queries())}"
    )
    assert evaluator._MAX_WORKERS >= 8


def test_batched_rollup_queries_use_rollup_table_and_preserve_visibility_fence() -> None:
    client = _BatchFakeClient(with_rollup=True)
    _run(client)
    queries = client.queries()
    expected_rollup_batches = {
        "batch_misc": "misc",
        "batch_geo": "geo",
        "batch_asn": "asn",
        "batch_url_cache": "url_cache",
    }
    for batch_name, shape in expected_rollup_batches.items():
        assert batch_name in queries, f"Expected rollup batch {batch_name} to run"
        sql, params = queries[batch_name]
        assert "FROM request_insight_hourly" in sql and f"shape = '{shape}'" in sql
        assert "rollup_start" in params and "rollup_end" in params
        assert "svc-opt" not in sql
        assert all(name in params for name in re.findall(r"\{(\w+):", sql))


def test_batch_misc_extracts_all_six_detectors_with_exact_semantics() -> None:
    rows = {
        "batch_misc": [
            # Grand total row () -> cache_hit_cliff + http3_fallback
            {
                "g_ost": 1,
                "g_p_type": 1,
                "g_method": 1,
                "g_c_type": 1,
                "ost_str": "",
                "p_type": "",
                "method": "",
                "c_type": "",
                "c_speed": "",
                "w_cnt": 1000,
                "b_cnt": 5000,
                "w_hits": 200,
                "w_cacheable": 1000,
                "b_hits": 4000,
                "b_cacheable": 5000,
                "w_quic": 100,
                "w_transport_total": 1000,
                "b_quic": 3500,
                "b_transport_total": 5000,
            },
            # (ost_str) rows -> origin_error_rate
            {
                "g_ost": 0,
                "g_p_type": 1,
                "g_method": 1,
                "g_c_type": 1,
                "ost_str": "200",
                "p_type": "",
                "method": "",
                "c_type": "",
                "c_speed": "",
                "w_cnt": 900,
                "b_cnt": 4990,
                "w_hits": 0,
                "w_cacheable": 0,
                "b_hits": 0,
                "b_cacheable": 0,
                "w_quic": 0,
                "w_transport_total": 0,
                "b_quic": 0,
                "b_transport_total": 0,
            },
            {
                "g_ost": 0,
                "g_p_type": 1,
                "g_method": 1,
                "g_c_type": 1,
                "ost_str": "503",
                "p_type": "",
                "method": "",
                "c_type": "",
                "c_speed": "",
                "w_cnt": 100,
                "b_cnt": 10,
                "w_hits": 0,
                "w_cacheable": 0,
                "b_hits": 0,
                "b_cacheable": 0,
                "w_quic": 0,
                "w_transport_total": 0,
                "b_quic": 0,
                "b_transport_total": 0,
            },
            # (p_type) rows -> proxy_surge
            {
                "g_ost": 1,
                "g_p_type": 0,
                "g_method": 1,
                "g_c_type": 1,
                "ost_str": "",
                "p_type": "",
                "method": "",
                "c_type": "",
                "c_speed": "",
                "w_cnt": 700,
                "b_cnt": 4900,
                "w_hits": 0,
                "w_cacheable": 0,
                "b_hits": 0,
                "b_cacheable": 0,
                "w_quic": 0,
                "w_transport_total": 0,
                "b_quic": 0,
                "b_transport_total": 0,
            },
            {
                "g_ost": 1,
                "g_p_type": 0,
                "g_method": 1,
                "g_c_type": 1,
                "ost_str": "",
                "p_type": "vpn",
                "method": "",
                "c_type": "",
                "c_speed": "",
                "w_cnt": 300,
                "b_cnt": 100,
                "w_hits": 0,
                "w_cacheable": 0,
                "b_hits": 0,
                "b_cacheable": 0,
                "w_quic": 0,
                "w_transport_total": 0,
                "b_quic": 0,
                "b_transport_total": 0,
            },
            # (method) rows -> method_drift
            {
                "g_ost": 1,
                "g_p_type": 1,
                "g_method": 0,
                "g_c_type": 1,
                "ost_str": "",
                "p_type": "",
                "method": "GET",
                "c_type": "",
                "c_speed": "",
                "w_cnt": 700,
                "b_cnt": 4950,
                "w_hits": 0,
                "w_cacheable": 0,
                "b_hits": 0,
                "b_cacheable": 0,
                "w_quic": 0,
                "w_transport_total": 0,
                "b_quic": 0,
                "b_transport_total": 0,
            },
            {
                "g_ost": 1,
                "g_p_type": 1,
                "g_method": 0,
                "g_c_type": 1,
                "ost_str": "",
                "p_type": "",
                "method": "POST",
                "c_type": "",
                "c_speed": "",
                "w_cnt": 300,
                "b_cnt": 50,
                "w_hits": 0,
                "w_cacheable": 0,
                "b_hits": 0,
                "b_cacheable": 0,
                "w_quic": 0,
                "w_transport_total": 0,
                "b_quic": 0,
                "b_transport_total": 0,
            },
            # (c_type, c_speed) rows -> connection_type_mix
            {
                "g_ost": 1,
                "g_p_type": 1,
                "g_method": 1,
                "g_c_type": 0,
                "ost_str": "",
                "p_type": "",
                "method": "",
                "c_type": "mobile",
                "c_speed": "4g",
                "w_cnt": 600,
                "b_cnt": 200,
                "w_hits": 0,
                "w_cacheable": 0,
                "b_hits": 0,
                "b_cacheable": 0,
                "w_quic": 0,
                "w_transport_total": 0,
                "b_quic": 0,
                "b_transport_total": 0,
            },
            {
                "g_ost": 1,
                "g_p_type": 1,
                "g_method": 1,
                "g_c_type": 0,
                "ost_str": "",
                "p_type": "",
                "method": "",
                "c_type": "cable",
                "c_speed": "broadband",
                "w_cnt": 400,
                "b_cnt": 4800,
                "w_hits": 0,
                "w_cacheable": 0,
                "b_hits": 0,
                "b_cacheable": 0,
                "w_quic": 0,
                "w_transport_total": 0,
                "b_quic": 0,
                "b_transport_total": 0,
            },
        ]
    }
    cards = _cards(_run(_BatchFakeClient(rows)))
    for insight_id in (
        "cache_hit_cliff",
        "http3_fallback",
        "origin_error_rate",
        "proxy_surge",
        "method_drift",
        "connection_type_mix",
    ):
        assert cards[insight_id]["severity"] != "clean", insight_id
        assert len(cards[insight_id]["items"]) >= 1, insight_id


def test_batch_geo_and_asn_extract_members_accurately(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("backend.core.fos_client.get_asn_names", lambda _sid, _asns: {7922: "Comcast"})
    rows: dict[str, list[dict[str, Any]]] = {
        "batch_geo": [
            {"g_city": 1, "country": "ZZ", "city": "", "region": "", "w_cnt": 15, "b_cnt": 0, "w_err": 0, "b_err": 0},
            {
                "g_city": 0,
                "country": "US",
                "city": "Denver",
                "region": "CO",
                "w_cnt": 100,
                "b_cnt": 0,
                "w_err": 40,
                "b_err": 0,
            },
        ],
        "batch_asn": [
            {
                "asn": 7922,
                "w_cnt": 90,
                "b_cnt": 0,
                "w_total_all": 120,
                "b_total_all": 1000,
                "w_hosting": 60,
                "w_p_total": 90,
                "b_hosting": 0,
                "b_p_total": 0,
            },
        ],
    }
    cards = _cards(_run(_BatchFakeClient(rows)))
    assert cards["new_country_traffic"]["items"][0]["label"] == "ZZ"
    assert cards["new_city_traffic"]["items"][0]["label"] == "Denver, CO, United States"
    assert cards["city_surges"]["items"][0]["label"] == "Denver, CO, United States"
    assert cards["city_error_spikes"]["items"][0]["label"] == "Denver, CO, United States"
    assert cards["asn_concentration"]["items"][0]["label"] == "AS7922 (Comcast)"
    assert cards["new_asn_traffic"]["items"][0]["label"] == "AS7922 (Comcast)"
    assert cards["asn_hosting_shift"]["items"][0]["label"] == "AS7922 (Comcast)"


def test_batch_failure_marks_only_batch_members_as_error() -> None:
    client = _BatchFakeClient(fail={"batch_url_cache"})
    cards = _cards(_run(client))
    assert cards["cache_collapse"]["severity"] == "error"
    assert cards["cacheability_regression"]["severity"] == "error"
    assert cards["error_spikes"]["severity"] == "clean"
    assert cards["asn_concentration"]["severity"] == "clean"


def test_single_enabled_member_falls_back_to_standalone_query(monkeypatch: pytest.MonkeyPatch) -> None:
    # Enable only country (not city/region/status) -> only new_country_traffic from batch_geo is active.
    monkeypatch.setattr("backend.config.active_log_field_ids", lambda _cfg: ["timestamp", "url", "country", "ip"])
    client = _BatchFakeClient()
    cards = _cards(_run(client))
    queries = client.queries()
    assert "new_country_traffic" in cards
    assert "new_country_traffic" in queries
    assert "batch_geo" not in queries


def test_remaining_batches_extract_and_shape_cards(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("backend.core.fos_client.get_asn_names", lambda _sid, _asns: {7922: "Comcast"})
    rows: dict[str, list[dict[str, Any]]] = {
        "batch_url_cache": [
            {
                "url": "/static/app.js",
                "c_w_rate": 0.1,
                "c_b_rate": 0.9,
                "c_w_total": 100,
                "c_b_total": 500,
                "p_w_rate": 0.8,
                "p_b_rate": 0.05,
                "p_w_total": 100,
                "p_b_total": 500,
            }
        ],
        "batch_window_url": [
            {
                "url": "/hero.png",
                "tl_p99": 900.0,
                "tl_p50": 50.0,
                "tl_ratio": 18.0,
                "tl_total": 40,
                "ttl_avg": 600.0,
                "ttl_hits": 0.5,
                "ttl_age": 12.0,
                "ttl_cnt": 25,
                "img_cnt": 20,
                "img_bytes": 2 * 1024 * 1024,
                "img_avg_kb": 102.4,
                "img_mob": 0.6,
                "ret_cnt": 8,
                "ret_avg": 1.5,
                "ret_max": 3,
            }
        ],
        "batch_latency_dim": [
            {
                "g_pop": 0,
                "g_reg": 1,
                "g_city": 1,
                "pop": "SJC",
                "w_p95": 400.0,
                "b_p95": 100.0,
                "w_total": 30,
                "b_total": 100,
            },
            {
                "g_pop": 1,
                "g_reg": 0,
                "g_city": 1,
                "server_region": "US-West",
                "w_p95": 400.0,
                "b_p95": 100.0,
                "w_total": 30,
                "b_total": 100,
                "ottfb_p95": 250.0,
            },
            {
                "g_pop": 1,
                "g_reg": 1,
                "g_city": 0,
                "city": "Denver",
                "region": "CO",
                "country": "US",
                "w_p95": 900.0,
                "b_p95": 100.0,
                "w_total": 20,
                "b_total": 100,
            },
        ],
        "batch_url_perf": [
            {
                "url": "/api/feed.json",
                "l_w_p95": 800.0,
                "l_b_p95": 100.0,
                "l_w_total": 20,
                "l_b_total": 100,
                "c_w_rate": 0.9,
                "c_b_rate": 0.05,
                "c_w_total": 30,
                "c_b_total": 100,
            }
        ],
        "batch_ip_outlier": [
            {
                "ip": "198.51.100.9",
                "max_bytes": 9000,
                "avg_bytes": 4000.0,
                "rs_w_total": 10,
                "rs_b_p95": 1000.0,
                "rs_b_total": 50,
                "max_reqs": 300,
                "avg_reqs": 120.0,
                "ca_w_total": 10,
                "ca_b_p95": 20.0,
                "ca_b_total": 50,
            }
        ],
        "batch_ip_abuse": [
            {
                "ip": "198.51.100.10",
                "cd_w_404": 40,
                "cd_w_total": 45,
                "cd_w_distinct": 30,
                "cd_b_404": 0,
                "ce_w_denied": 30,
                "ce_w_attempts": 35,
                "ce_w_paths": 3,
                "ce_b_denied": 0,
                "sh_w_sessions": 30,
                "sh_w_reqs": 60,
                "sh_b_sessions": 0,
                "od_w_reqs": 80,
                "od_w_distinct": 20,
                "od_b_reqs": 5,
            }
        ],
        "batch_origin_perf": [
            {
                "g_url": 1,
                "w_url": "",
                "w_p95": 600.0,
                "b_p95": 100.0,
                "w_reqs": 100,
                "url_p95_ms": 600.0,
                "w_conn": 200.0,
                "b_conn": 30.0,
                "w_read": 400.0,
                "b_read": 70.0,
                "ts_w_total": 100,
                "ts_b_total": 200,
            },
            {
                "g_url": 0,
                "w_url": "/slow",
                "w_p95": 650.0,
                "b_p95": 100.0,
                "w_reqs": 50,
                "url_p95_ms": 650.0,
                "w_conn": 200.0,
                "b_conn": 30.0,
                "w_read": 450.0,
                "b_read": 70.0,
                "ts_w_total": 50,
                "ts_b_total": 200,
            },
        ],
        "batch_network_asn": [
            {
                "g_asn": 0,
                "g_metro": 1,
                "asn_str": "7922",
                "metro": "",
                "w_ploss": 0.05,
                "b_ploss": 0.001,
                "w_jitter": 50000.0,
                "b_jitter": 5000.0,
                "w_retrans": 2.0,
                "b_retrans": 0.1,
                "nah_w_total": 100,
                "nah_b_total": 200,
            },
            {
                "g_asn": 0,
                "g_metro": 0,
                "asn_str": "7922",
                "metro": "501",
                "amp_w_med": 120.0,
                "amp_b_med": 30.0,
                "amp_w_total": 50,
                "amp_b_total": 100,
            },
            {
                "g_asn": 1,
                "g_metro": 0,
                "asn_str": "",
                "metro": "501",
                "mdd_w_med": 100000.0,
                "mdd_b_med": 500000.0,
                "mdd_w_total": 50,
                "mdd_b_total": 100,
            },
        ],
        "batch_fingerprint_spread": [
            {
                "g_fp": 0,
                "g_cipher": 1,
                "fp_val": "t13d1516h2_abc_def",
                "tls_ciphers_sha": "",
                "w_ips": 40,
                "w_reqs": 200,
                "b_ips": 0,
                "res_w_ips": 35,
                "res_w_asns": 6,
                "res_w_reqs": 180,
                "res_b_ips": 0,
                "res_b_asns": 1,
            },
            {
                "g_fp": 1,
                "g_cipher": 0,
                "fp_val": "",
                "tls_ciphers_sha": "deadbeef1234",
                "w_ips": 20,
                "w_reqs": 150,
                "b_ips": 0,
            },
        ],
        "batch_beaconing": [
            {
                "g_ip": 0,
                "entity": "198.51.100.11",
                "n_gaps": 29,
                "n_events": 30,
                "avg_interval": 10.0,
                "stddev_interval": 0.2,
                "cv_corr": 0.02,
                "modal_frac": 0.9,
                "diversity": 1,
                "span_s": 300,
                "mode_gap": 10,
            },
            {
                "g_ip": 1,
                "entity": "t13d1516h2_abc_def",
                "n_gaps": 29,
                "n_events": 30,
                "avg_interval": 10.0,
                "stddev_interval": 0.2,
                "cv_corr": 0.02,
                "modal_frac": 0.9,
                "diversity": 4,
                "span_s": 300,
                "mode_gap": 10,
            },
        ],
    }
    cards = _cards(_run(_BatchFakeClient(rows)))
    for insight_id in (
        "cache_collapse",
        "cacheability_regression",
        "tail_latency",
        "cache_ttl_mismatch",
        "image_optimization_opportunities",
        "origin_retries",
        "pop_latency_regression",
        "region_latency",
        "city_latency_regressions",
        "latency_regression",
        "payload_compression_regression",
        "request_size_anomaly",
        "connection_abuse",
        "content_discovery",
        "credential_enumeration",
        "session_harvesting",
        "orphaned_deep_crawl",
        "origin_latency_spike",
        "timeout_split",
        "network_asn_health",
        "asn_metro_performance",
        "metro_delivery_degradation",
        "botnet_grouping",
        "cipher_spread",
        "residential_fingerprint_dispersion",
        "repeated_patterns",
        "repeated_patterns_fp",
    ):
        assert cards[insight_id]["items"], f"{insight_id} expected non-empty items"
        assert cards[insight_id]["severity"] != "error", insight_id
