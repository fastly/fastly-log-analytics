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
        if name.endswith("_candidates"):
            base = name.removesuffix("_candidates")
            if base in self._fail:
                raise RuntimeError(f"boom:{name}")
            base_rows = self._rows.get(base, [])
            if base == "batch_fingerprint_spread":
                out: list[dict[str, Any]] = []
                for row in base_rows:
                    if row.get("fp_val"):
                        out.append({"kind": "fp", "k": row["fp_val"]})
                    if row.get("tls_ciphers_sha"):
                        out.append({"kind": "cipher", "k": row["tls_ciphers_sha"]})
                return out
            if base == "batch_network_asn":
                out_nasn: list[dict[str, Any]] = []
                for row in base_rows:
                    if row.get("asn_str"):
                        out_nasn.append({"kind": "asn", "k": str(row["asn_str"])})
                    if row.get("metro"):
                        out_nasn.append({"kind": "metro", "k": str(row["metro"])})
                return out_nasn
            if base == "batch_origin_perf":
                return [{"ok": 1}] if base_rows else []
            return [{"k": row.get("ip") or row.get("url") or "cand"} for row in base_rows]
        if name.startswith("group_"):
            tags = re.findall(r"SELECT '(\w+)' AS _q, formatRow\('JSONEachRow', \*\) AS _r", sql)
            if any(tag in self._fail for tag in tags):
                raise RuntimeError(f"boom:{name}")
            return [{"_q": tag, "_r": json.dumps(row)} for tag in tags for row in self._rows.get(tag, [])]
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
    """A full 51-detector High-Scale evaluation un-fuses heavy baseline scans and stays <= 20 ClickHouse queries."""
    client = _BatchFakeClient(with_rollup=True)
    cards = _cards(_run(client))
    assert set(cards) == EVERY_CARD
    assert all(card["severity"] == "clean" for card in cards.values())
    queries = client.queries()
    assert "group_raw_standalone" not in queries
    assert len(client.calls) <= 20, f"Expected <= 20 ClickHouse queries, got {len(client.calls)}: {list(queries)}"
    group_map = dict(evaluator._MULTIPLEX_GROUPS)
    assert "group_baseline_perf" not in group_map
    assert "group_ip_abuse" not in group_map
    assert "group_network_fp" not in group_map
    assert group_map["group_window_perf"] == frozenset({"batch_window_url", "batch_beaconing"})
    assert evaluator._MAX_WORKERS == 3
    assert evaluator._BACKGROUND_MAX_WORKERS == 2
    assert evaluator._COLD_POOL._max_workers == 1


def test_rollup_groups_are_not_fused_when_falling_back_to_raw_facts() -> None:
    client = _BatchFakeClient(with_rollup=False)
    _run(client)
    queries = client.queries()
    assert "group_rollup_batches" not in queries
    assert "group_rollup_standalone" not in queries
    assert {"batch_misc", "batch_geo", "batch_asn", "batch_url_cache"} <= set(queries)
    assert {"error_spikes", "new_probe_urls", "ua_monoculture", "referer_monoculture", "waf_signal_spikes"} <= set(
        queries
    )


def test_candidate_precheck_skips_empty_pushes_prewhere_and_falls_back_above_500() -> None:
    # 1. 0 candidates -> skips baseline scans for ip_outlier, ip_abuse, fingerprint_spread, url_perf
    empty_client = _BatchFakeClient(with_rollup=True)
    _run(empty_client)
    empty_queries = empty_client.queries()
    for bname in ("batch_ip_outlier", "batch_ip_abuse", "batch_fingerprint_spread", "batch_url_perf"):
        assert f"{bname}_candidates" in empty_queries
        assert bname not in empty_queries

    # 2. <= 500 candidates -> pushes concrete IN (...) into PREWHERE
    class _FewCandClient(_BatchFakeClient):
        def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
            marker = re.search(r"/\* insight:(\w+) \*/", sql)
            name = marker.group(1) if marker else ""
            if name == "batch_url_perf_candidates":
                self.calls.append((sql, params or {}))
                return [{"k": "/api/a"}, {"k": "/api/b"}]
            if name in ("batch_ip_outlier_candidates", "batch_ip_abuse_candidates"):
                self.calls.append((sql, params or {}))
                return [{"k": "198.51.100.1"}]
            if name == "batch_fingerprint_spread_candidates":
                self.calls.append((sql, params or {}))
                return [{"kind": "fp", "k": "t13_fp_1"}]
            return super().execute(sql, params)

    few_client = _FewCandClient(with_rollup=True)
    _run(few_client)
    few_q = few_client.queries()
    url_sql, url_params = few_q["batch_url_perf"]
    url_pre = re.search(r"\bPREWHERE\b(.*?)WHERE \(url != ''\)", url_sql)
    assert url_pre and "url IN (" in url_pre.group(1)
    assert "IN (SELECT url FROM" not in url_sql
    assert "/api/a" in url_params.values() and "/api/b" in url_params.values()

    ip_out_sql, ip_out_params = few_q["batch_ip_outlier"]
    ip_out_pre = re.search(r"\bPREWHERE\b(.*?)WHERE \(\(custom_fields\['req_header_bytes'\]", ip_out_sql)
    assert ip_out_pre and "client_ip IN (" in ip_out_pre.group(1)
    assert "IN (SELECT client_ip" not in ip_out_sql
    assert "198.51.100.1" in ip_out_params.values()

    ip_abuse_sql, _ = few_q["batch_ip_abuse"]
    ip_abuse_pre = re.search(r"\bPREWHERE\b(.*?)WHERE \(client_ip != ''\)", ip_abuse_sql)
    assert ip_abuse_pre and "client_ip IN (" in ip_abuse_pre.group(1)
    assert "IN (SELECT client_ip" not in ip_abuse_sql

    fp_sql, fp_params = few_q["batch_fingerprint_spread"]
    fp_pre = re.search(r"\bPREWHERE\b(.*?)\)\s*SELECT\b", fp_sql)
    assert fp_pre and "IN (" in fp_pre.group(1)
    assert "IN (SELECT custom_fields" not in fp_sql
    assert "t13_fp_1" in fp_params.values()

    # 3. > 500 candidates -> falls back to existing subquery
    class _ManyCandClient(_BatchFakeClient):
        def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
            marker = re.search(r"/\* insight:(\w+) \*/", sql)
            name = marker.group(1) if marker else ""
            if name == "batch_url_perf_candidates":
                self.calls.append((sql, params or {}))
                return [{"k": f"/u/{i}"} for i in range(501)]
            return super().execute(sql, params)

    many_client = _ManyCandClient(with_rollup=True)
    _run(many_client)
    many_sql, _ = many_client.queries()["batch_url_perf"]
    assert "IN (SELECT url FROM" in many_sql


def test_batched_rollup_queries_use_rollup_table_and_preserve_visibility_fence() -> None:
    client = _BatchFakeClient(with_rollup=True)
    _run(client)
    queries = client.queries()
    assert "group_rollup_batches" in queries
    sql, params = queries["group_rollup_batches"]
    for batch_name, shape in {
        "batch_misc": "misc",
        "batch_geo": "geo",
        "batch_asn": "asn",
        "batch_url_cache": "url_cache",
    }.items():
        assert f"SELECT '{batch_name}' AS _q" in sql
        assert f"shape = '{shape}'" in sql
    assert "FROM request_insight_hourly" in sql
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
    all_sql = "\n".join(sql for sql, _ in queries.values())
    assert "new_country_traffic" in cards
    assert "new_country_traffic" in queries or "SELECT 'new_country_traffic' AS _q" in all_sql
    assert "batch_geo" not in queries and "SELECT 'batch_geo' AS _q" not in all_sql


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


def test_non_rollup_detector_groups_overlap_rollup_coverage_read() -> None:
    import threading

    barrier = threading.Barrier(2, timeout=2.0)
    non_rollup_started = threading.Event()

    class _OverlapClient(_BatchFakeClient):
        def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
            marker = re.search(r"/\* insight:(\w+) \*/", sql)
            name = marker.group(1) if marker else ""
            if name == "rollup_coverage":
                barrier.wait()
            elif name == "group_window_perf" and not non_rollup_started.is_set():
                non_rollup_started.set()
                barrier.wait()
            return super().execute(sql, params)

    client = _OverlapClient(with_rollup=True)
    cards = _cards(_run(client))
    assert set(cards) == EVERY_CARD
    assert non_rollup_started.is_set()
    assert not barrier.broken


def test_cache_collapse_detail_runs_three_queries_concurrently() -> None:
    import threading

    from backend.models.analytics import CacheCollapseDetailRequest

    barrier = threading.Barrier(3, timeout=2.0)

    class _DetailClient:
        def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
            barrier.wait()
            if "insight:cache_collapse_rates" in sql:
                return [
                    {
                        "w_hits": 10,
                        "w_miss": 90,
                        "w_pass": 0,
                        "w_total": 100,
                        "b_hits": 90,
                        "b_miss": 10,
                        "b_pass": 0,
                        "b_total": 100,
                    }
                ]
            return []

    service = SimpleNamespace(service_id="svc-opt", client=_DetailClient())
    req = CacheCollapseDetailRequest(url="/api/slow", window_size_hrs=24, baseline_hours=168)
    resp = evaluator.cache_collapse_detail_endpoint(service, req, None, None)  # type: ignore[arg-type]
    assert resp.url == "/api/slow"


def test_cold_wait_seconds_is_bounded_below_p95_budget() -> None:
    assert 0 < evaluator._COLD_WAIT_SECONDS <= 2.5


def test_background_insights_refresh_caps_concurrency_and_uses_tdigest(monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.high_scale.insights import batches as hs_batches

    # batch_ip_outlier uses quantileTDigestIf instead of exact quantile
    outlier_batch = next(b for b in hs_batches.DETECTION_BATCHES if b.name == "ip_outlier")
    sql, _ = outlier_batch.build_sql(
        {"request_smuggling_headers", "connection_abuse"},
        present={"ip", "req_header_bytes", "conn_requests"},
        rollup_span=None,
        filter_clause="",
    )
    assert "quantileTDigestIf(0.95)" in sql
    assert "quantileExactInclusiveIf" not in sql

    # force_refresh (prewarmer) caps max_workers below interactive _MAX_WORKERS
    assert 0 < evaluator._BACKGROUND_MAX_WORKERS < evaluator._MAX_WORKERS
    seen_workers: list[int] = []

    def fake_evaluate(*args: Any, max_workers: int = evaluator._MAX_WORKERS, **kwargs: Any):
        seen_workers.append(max_workers)
        return (
            {
                "insights": [],
                "window_start": "2026-09-14T00:00:00+00:00",
                "window_end": "2026-09-15T00:00:00+00:00",
                "baseline_start": "2026-09-07T00:00:00+00:00",
                "baseline_end": "2026-09-14T00:00:00+00:00",
                "computed_at": "2026-09-15T00:00:00+00:00",
                "window_hours": 24.0,
                "baseline_hours": 168.0,
                "window_total_requests": 0,
            },
            False,
        )

    from backend.high_scale.ingest_controller import HighScaleService

    monkeypatch.setattr(evaluator, "_evaluate", fake_evaluate)
    svc = HighScaleService(
        service_id="svc-bg-workers",
        client=SimpleNamespace(),  # type: ignore[arg-type]
        cursor_secret=b"secret",
        request_watermark=None,
    )
    req = SimpleNamespace(window_size_hrs=24.0, baseline_hours=168.0, filters=None)
    evaluator.insights_endpoint(svc, req, None, None, force_refresh=True)
    assert seen_workers == [evaluator._BACKGROUND_MAX_WORKERS]


def test_impossible_distance_uses_selective_prewhere_and_prunes_impossible_rtt() -> None:
    from backend.high_scale.insights.sql_queries import _base_sql, impossible_distance

    pops = {"SJC": (37.36, -121.93)}
    det_edge = impossible_distance(pops, "ja4", edge_only=True)
    assert det_edge.prewhere
    assert "{window_start" not in det_edge.prewhere and "{window_end" not in det_edge.prewhere
    assert "lower(" not in det_edge.prewhere
    assert "lower(" not in det_edge.row_filter

    sql_plain = _base_sql(det_edge, "impossible_distance")
    pre_plain = re.search(r"FROM request_facts PREWHERE (.*?) WHERE ", sql_plain)
    assert pre_plain, sql_plain
    pre_clause = pre_plain.group(1)
    assert "custom_fields['edge'] IN ('1', 'true', 'True', 'TRUE')" in pre_clause
    assert "custom_fields['ja4'] != ''" in pre_clause
    assert "custom_fields['pop'] != ''" in pre_clause
    assert "custom_fields['tcp_rtt'] != ''" in pre_clause
    assert "custom_fields['lat'] != ''" in pre_clause
    assert "custom_fields['lon'] != ''" in pre_clause
    assert "toFloat64OrNull(custom_fields['tcp_rtt']) > 0" in pre_clause
    assert "toFloat64OrNull(custom_fields['tcp_rtt']) <= 100100" in pre_clause
    assert "`_cf_" not in sql_plain

    sql_cf = _base_sql(det_edge, "impossible_distance", use_cf=True)
    assert "custom_fields[" not in sql_cf
    pre_cf = re.search(r"FROM request_facts PREWHERE (.*?) WHERE ", sql_cf)
    assert pre_cf, sql_cf
    for col in ("`_cf_edge`", "`_cf_ja4`", "`_cf_pop`", "`_cf_tcp_rtt`", "`_cf_lat`", "`_cf_lon`"):
        assert col in pre_cf.group(1)
    for col in ("`_cf_pop`", "`_cf_city`", "`_cf_ja4`", "`_cf_edge`"):
        assert col in sql_cf

    det_no_edge = impossible_distance(pops, "ja3", edge_only=False)
    assert "edge" not in det_no_edge.prewhere
    assert "custom_fields['ja3'] != ''" in det_no_edge.prewhere


def test_materialized_cf_columns_used_across_insights_and_cache_collapse_when_enabled() -> None:
    from backend.models.analytics import CacheCollapseDetailRequest

    # 1. Default client without _cf_* support keeps custom_fields[...] for chDB / legacy table parity.
    plain_client = _BatchFakeClient(with_rollup=True)
    _run(plain_client)
    for name, (sql, _params) in plain_client.queries().items():
        if name not in ("history", "fields", "rollup_coverage"):
            assert "`_cf_" not in sql, f"Unexpected _cf_ in {name} when disabled"

    # 2. Client with materialized _cf_* columns enabled rewrites all raw-fact insight queries + filters.
    cf_client = _BatchFakeClient(with_rollup=True)
    cf_client._enable_cf_rewrite_in_tests = True  # type: ignore[attr-defined]
    service = SimpleNamespace(service_id="svc-opt", client=cf_client)
    req = InsightsRequest(window_size_hrs=24.0, baseline_hours=168.0, filters={"cache": ["MISS"]})
    evaluator.insights_endpoint(service, req, None, None)  # type: ignore[arg-type]

    cf_queries = cf_client.queries()
    assert "fields" in cf_queries
    assert "mapKeys(custom_fields)" in cf_queries["fields"][0]
    assert f"LIMIT {evaluator._FIELD_SAMPLE_ROWS}" in cf_queries["fields"][0]

    for name, (sql, _params) in cf_queries.items():
        if name in ("history", "fields", "rollup_coverage"):
            continue
        assert "custom_fields[" not in sql, f"Unrewritten custom_fields in {name}: {sql}"

    assert "group_raw_standalone" not in cf_queries
    all_cf_sql = "\n".join(
        sql for name, (sql, _params) in cf_queries.items() if name not in ("history", "fields", "rollup_coverage")
    )
    for expected in ("`_cf_ja4`", "`_cf_lat`", "`_cf_lon`", "`_cf_tcp_rtt`", "`_cf_pop`", "`_cf_ua`", "`_cf_cache`"):
        assert expected in all_cf_sql

    # 3. cache_collapse_detail_endpoint also uses _cf_* and PREWHERE when enabled.
    detail_calls: list[str] = []

    class _DetailCFClient:
        _enable_cf_rewrite_in_tests = True

        def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
            detail_calls.append(sql)
            if "insight:cache_collapse_rates" in sql:
                return [
                    {
                        "w_hits": 10,
                        "w_miss": 90,
                        "w_pass": 0,
                        "w_total": 100,
                        "b_hits": 90,
                        "b_miss": 10,
                        "b_pass": 0,
                        "b_total": 100,
                    }
                ]
            return []

    detail_svc = SimpleNamespace(service_id="svc-opt", client=_DetailCFClient())
    evaluator.cache_collapse_detail_endpoint(
        detail_svc,  # type: ignore[arg-type]
        CacheCollapseDetailRequest(url="/api/slow", window_size_hrs=24, baseline_hours=168),
        None,
        None,
    )
    assert len(detail_calls) == 3
    for sql in detail_calls:
        assert "custom_fields[" not in sql
        assert "`_cf_cache`" in sql
        assert "FROM request_facts PREWHERE " in sql
    misses_sql = next(s for s in detail_calls if "insight:cache_collapse_misses" in s)
    assert "`_cf_pop`" in misses_sql and "`_cf_status`" in misses_sql
