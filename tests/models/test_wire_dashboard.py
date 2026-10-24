"""Performance row models keep every producer's wire bytes (repository, rollup hit, high-scale)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from backend.high_scale import performance as hs_performance
from backend.models.performance import PerformanceAggregatesResponse
from backend.repositories._base import _safe_table
from backend.repositories.performance import get_performance_aggregates
from tests._helpers.wire import assert_sections_wire_identical
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


def _seed(con, src, n=120):
    logs = generate_mock_logs(src, num_logs=n, hours_ago=1)
    for i, log in enumerate(logs):
        log["elapsed"] = 100_000 + (i * 7919) % 900_000
        log["ttfb"] = f"{(50_000 + (i * 1013) % 400_000) / 1_000_000:.6f}"
        log["ottfb"] = 20_000 + (i * 331) % 30_000
        log["ottlb"] = 60_000 + (i * 331) % 30_000
        log["url"] = f"/path-{i % 4}"
        log["asn"] = [7922, 15169][i % 2]
        log["ttl"] = [0, 5, 45, 3600, 90000][i % 5]
        log["cache"] = "HIT" if i % 3 else "MISS"
    insert_mock_logs(con, _safe_table(src["name"]), logs)


def test_repository_live_wire_identical(in_memory_duckdb, test_service_source, monkeypatch):
    _seed(in_memory_duckdb, test_service_source)
    monkeypatch.setattr("backend.core.duckdb.get_asn_names", lambda sid, asns: {7922: "Comcast"})
    res = get_performance_aggregates(
        con=in_memory_duckdb, src=test_service_source, start_time=None, end_time=None, filters={}
    )
    assert_sections_wire_identical(
        PerformanceAggregatesResponse, res, non_empty=("top_urls", "top_asns", "ttl_dist", "scatter", "waterfall")
    )


def test_repository_rollup_hit_wire_identical(in_memory_duckdb, test_service_source, monkeypatch):
    _seed(in_memory_duckdb, test_service_source)
    monkeypatch.setattr("backend.core.duckdb.get_asn_names", lambda sid, asns: {7922: "Comcast"})

    def _hit(self, *_a, **kw):
        value = "/rolled" if kw.get("dimension") == "url" else 7922
        return {"rows": [{"value": value, "requests": 100, "avg_ms": 1.0, "p50_ms": 1.0, "p95_ms": 2.0, "p99_ms": 3.0}]}

    with patch("backend.repositories._base.QueryRunner.try_perf_latency_from_rollup", _hit):
        res = get_performance_aggregates(
            con=in_memory_duckdb, src=test_service_source, start_time=None, end_time=None, filters={}
        )
    assert res["approx"] is True
    assert_sections_wire_identical(PerformanceAggregatesResponse, res, non_empty=("top_urls", "top_asns"))


def test_repository_empty_schema_wire_identical(in_memory_duckdb):
    src = {"name": "perf_wire_missing", "service_id": "perf_wire_missing"}
    res = get_performance_aggregates(con=in_memory_duckdb, src=src, start_time=None, end_time=None, filters={})
    assert_sections_wire_identical(PerformanceAggregatesResponse, res)


class _HsClient:
    def execute(self, sql, params=None):
        row = {"value": "/slow", "req_count": 9, "avg_ms": 12.5, "p50_ms": 10.0, "p95_ms": 20.0, "p99_ms": 30.0}
        if "perf:top_urls" in sql:
            return [row]
        if "perf:top_asns" in sql:
            return [{**row, "value": "7922"}]
        if "perf:ttl_dist" in sql:
            return [{"bucket": "0s", "count": 3, "min_ttl": 0.0}, {"bucket": "<1h", "count": 4, "min_ttl": 45.0}]
        if "perf:waterfall" in sql:
            return [
                {
                    "component_count": 2,
                    "edge_processing": 1.0,
                    "origin_wait": 2.0,
                    "origin_download": 3.0,
                    "client_download": 4.0,
                }
            ]
        if "perf:scatter" in sql:
            return [{"origin_ms": 10.0, "edge_ms": 5.0, "cache": "HIT"}]
        return []


def test_high_scale_wire_identical(monkeypatch):
    captured: dict = {}

    def _capture(cls, **kwargs):
        captured.update(kwargs)
        return cls(**kwargs)

    monkeypatch.setattr(PerformanceAggregatesResponse, "with_telemetry", classmethod(_capture))
    monkeypatch.setattr("backend.core.duckdb.get_asn_names", lambda sid, asns: {7922: "Comcast"})
    service = SimpleNamespace(service_id="svc", client=_HsClient())
    hs_performance.performance_aggregates(service, SimpleNamespace(), None, None)
    assert captured["top_asns"][0]["asn"] == 7922
    assert captured["top_asns"][0]["label"] == "Comcast (7922)"
    assert_sections_wire_identical(
        PerformanceAggregatesResponse,
        captured,
        non_empty=("top_urls", "top_asns", "ttl_dist", "scatter", "waterfall"),
    )


# ── Merged from test_wire_sessions.py ──

from datetime import UTC, datetime, timedelta

import pytest

from backend.core.log_fields import LOG_FIELD_CATALOG
from backend.high_scale.sessions import sessions_detail as hs_sessions_detail
from backend.models.dashboard import SessionDetailRequest, SessionDetailResponse
from backend.repositories.sessions import get_session_detail

_START = "2020-01-01T00:00:00+00:00"
_END = "2100-01-01T00:00:00+00:00"


def test_repository_detail_wire_identical_across_every_log_column(in_memory_duckdb, test_service_source):
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=40, hours_ago=1)
    for log in logs:
        log["ip"] = "203.0.113.7"
    insert_mock_logs(in_memory_duckdb, table, logs)

    res = get_session_detail(in_memory_duckdb, test_service_source, "203.0.113.7", _START, _END)

    raw_ids = {f["id"] for f in LOG_FIELD_CATALOG if f.get("vcl") is not None}
    assert raw_ids <= set(res["columns"]), "fixture must exercise every raw log column"
    kinds = {type(v) for row in res["data"] for v in row.values()}
    assert {str, int, float} <= kinds, "fixture must exercise string, integer and float cells"
    assert_sections_wire_identical(SessionDetailResponse, res, non_empty=("data", "columns"))


def test_repository_detail_keeps_booleans_and_nulls(in_memory_duckdb):
    src = {"name": "sess_wire_bool", "service_id": "sess_wire_bool"}
    table = _safe_table(src["name"])
    now = datetime.now(UTC)
    in_memory_duckdb.execute(
        f'CREATE TABLE {table} ("timestamp" TIMESTAMPTZ, "ip" VARCHAR, "flag" BOOLEAN, "n" BIGINT, "x" DOUBLE, "s" VARCHAR)'
    )
    in_memory_duckdb.execute(
        f"INSERT INTO {table} VALUES (?, '203.0.113.7', TRUE, 5, 2.0, NULL), (?, '203.0.113.7', FALSE, NULL, 0.5, 'a')",
        [now.isoformat(), (now + timedelta(seconds=1)).isoformat()],
    )
    res = get_session_detail(in_memory_duckdb, src, "203.0.113.7", _START, _END)
    assert [r["flag"] for r in res["data"]] == [True, False]
    assert_sections_wire_identical(SessionDetailResponse, res, non_empty=("data",))


def test_high_scale_detail_wire_identical():
    ts = datetime(2026, 9, 15, 0, 0, tzinfo=UTC)

    def _row(offset: int, url: str, status: int, asn: int | None) -> dict:
        return {
            "ip": "203.0.113.7",
            "ja4": "t13d1516h2_x",
            "timestamp": ts + timedelta(seconds=offset),
            "country": "US",
            "url": url,
            "status": status,
            "method": "GET",
            "protocol": "h2",
            "resp_bytes": 512,
            "tcp_rtt": 12.5,
            "edge": "1",
            "edge_sid": "sid-1",
            "ua": "curl/8",
            "asn": asn,
        }

    class _Client:
        def execute(self, query, params):
            return [_row(0, "/a", 200, 64500), _row(2, "/b", 404, None)]

    service = type("Svc", (), {"client": _Client(), "service_id": "svc"})()
    req = SessionDetailRequest(ip="203.0.113.7")
    res = hs_sessions_detail(service, req, _START, _END, "203.0.113.7", None)  # type: ignore[arg-type]
    assert res["data"][0]["timestamp"].endswith("Z")
    assert_sections_wire_identical(SessionDetailResponse, res, non_empty=("data", "columns"))


def test_non_scalar_cells_are_rejected():
    with pytest.raises(ValueError, match="data"):
        SessionDetailResponse.model_validate({"columns": ["a"], "data": [{"a": [1, 2]}]})


# ── Merged from test_wire_value.py ──

from backend.models.value import IOFormatConversionPair, IOOpportunityRow, TopBotRow, WafSignalRow
from backend.repositories.value import get_summary
from tests._helpers.wire import assert_rows_wire_identical


def _summary(con, src):
    logs = generate_mock_logs(src, num_logs=120, hours_ago=1)
    for i, log in enumerate(logs):
        log["waf_sig"] = ["SQLI,XSS", "XSS", "VERIFIED-BOT.search-engine"][i % 3]
        log["_bot_name"] = ["googlebot", "bingbot", ""][i % 3]
        log["url"] = f"/img/{i % 4}.jpg"
        log["io_input_bytes"] = 100_000 + i
        log["io_output_bytes"] = 40_000 + i
        log["io_input_format"] = ["jpeg", "png"][i % 2]
        log["io_output_format"] = ["webp", "avif"][i % 2]
        log["resp_bytes"] = 50_000 + i
    insert_mock_logs(con, _safe_table(src["name"]), logs)
    return get_summary(
        con,
        src,
        None,
        None,
        {},
        sections={"security", "bots", "io"},
        service_id=None,
    )


def test_value_list_rows_wire_identical(in_memory_duckdb, test_service_source):
    res = _summary(in_memory_duckdb, test_service_source)
    assert_rows_wire_identical(WafSignalRow, res["security"]["top_waf_signals"])
    assert_rows_wire_identical(TopBotRow, res["bots"]["top_bots"])
    assert_rows_wire_identical(IOOpportunityRow, res["io"]["optimization_opportunities"])
    assert_rows_wire_identical(IOFormatConversionPair, res["io"]["io_format_conversion_pairs"])
