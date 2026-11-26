"""Unit tests for Round 22 High-Scale ClickHouse insights and security optimizations."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

from backend.core.catalog import LOG_FIELD_CATALOG
from backend.core.clickhouse.client import ClickHouseClient
from backend.high_scale import security
from backend.high_scale.ingest_controller import HighScaleService
from backend.high_scale.insights import evaluator
from backend.high_scale.insights.batches import (
    _build_ip_outlier,
    _build_origin_perf,
)
from backend.high_scale.insights.sql_queries import (
    DETECTIONS,
    _base_sql,
    baseline_sample_stride,
)
from backend.models.analytics import InsightsRequest

pytestmark = pytest.mark.no_postgres

ALL_FIELDS = [f["id"] for f in LOG_FIELD_CATALOG]


class _FakeRealClickHouseClient(ClickHouseClient):
    """Fake ClickHouseClient that triggers real-CH SQL paths."""

    def __init__(self, handler: Any = None) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self._handler = handler
        self._http = SimpleNamespace(_transport=object())

    def execute(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        self.calls.append((query, dict(params or {})))
        if callable(self._handler):
            return self._handler(query, params or {})
        return list(self._handler or [])


def _service(client: _FakeRealClickHouseClient, sid: str = "svc-opt22") -> HighScaleService:
    now = datetime.now(UTC)
    return HighScaleService(
        service_id=sid,
        client=client,
        cursor_secret=b"opt22-test-secret-32-bytes-long!",
        request_watermark=lambda: now,
    )


def _sec_req(filters: dict[str, Any] | None = None, bucket_seconds: int = 3600) -> tuple[Any, str, str]:
    now = datetime.now(UTC)
    req = SimpleNamespace(filters=filters or {}, bucket_seconds=bucket_seconds)
    return req, (now - timedelta(hours=24)).isoformat(), now.isoformat()


@pytest.fixture(autouse=True)
def _clear_caches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("backend.config.load_config", lambda _sid: {})
    monkeypatch.setattr("backend.config.active_log_field_ids", lambda _cfg: ALL_FIELDS)
    evaluator._INSIGHTS_CACHE.clear()
    evaluator._INSIGHTS_TELEMETRY_CACHE.clear()
    evaluator._INSIGHTS_PARTIAL_CACHE.clear()
    evaluator._INSIGHTS_HISTORY_CACHE.clear()
    evaluator._INSIGHTS_LAST_RESULT.clear()
    evaluator._INSIGHTS_REFRESH_FAILURES.clear()
    security._security_memo.cache.clear()
    security._security_memo._telemetry_cache.clear()


def test_baseline_sample_stride_raises_24h_and_48h_strides() -> None:
    assert baseline_sample_stride(12.0) == 1
    assert baseline_sample_stride(24.0) == 4
    assert baseline_sample_stride(48.0) == 6
    assert baseline_sample_stride(168.0) == 8

    sql_24h = _base_sql(DETECTIONS["stale_browser_version"], "stale_browser_version", baseline_hours=24.0)
    assert "intDiv(toUnixTimestamp(event_timestamp), 300) % 4 = 0" in sql_24h
    assert "countIf(is_b) * 4 AS b_reqs" in sql_24h

    sql_48h = _base_sql(DETECTIONS["stale_browser_version"], "stale_browser_version", baseline_hours=48.0)
    assert "intDiv(toUnixTimestamp(event_timestamp), 300) % 6 = 0" in sql_48h
    assert "countIf(is_b) * 6 AS b_reqs" in sql_48h


def test_crawler_disparity_drops_empty_grouping_set_scales_baseline_and_uses_subquery_total(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sql_sampled = _base_sql(DETECTIONS["crawler_disparity"], "crawler_disparity", baseline_hours=168.0)
    assert sql_sampled.count("FROM base") == 1
    assert "GROUP BY GROUPING SETS ((bot_stem), (ref_stem, is_w, is_b))" in sql_sampled
    assert "GROUP BY GROUPING SETS ((), " not in sql_sampled
    assert "HAVING (g_bot = 0 AND bot_stem != '' AND w_cnt >= 50) OR (g_ref = 0 AND ref_stem != '')" in sql_sampled
    assert "g_bot = 1 AND g_ref = 1" not in sql_sampled
    assert "countIf(is_b) * 8 AS b_cnt" in sql_sampled
    assert "sumIf(coalesce(resp_bytes, 0), is_b) * 8 AS b_b" in sql_sampled
    assert (
        "greatest((SELECT sum(toInt64OrZero(custom_fields['resp_bytes'])) FROM request_facts PREWHERE " in sql_sampled
    )

    # Row filter drops the unconditional window-row pass-through before referer/bot predicates.
    base_section = sql_sampled.split("pre AS (")[0]
    assert "OR (referer != '' AND referer != '-')" not in base_section
    assert "AND ((referer != '' AND referer != '-') OR multiSearchAnyCaseInsensitive(ua," in base_section

    sql_unsampled = _base_sql(DETECTIONS["crawler_disparity"], "crawler_disparity", baseline_hours=0.0)
    assert "countIf(is_b) AS b_cnt" in sql_unsampled
    assert "sumIf(coalesce(resp_bytes, 0), is_b) AS b_b" in sql_unsampled
    assert "__B_SCALE__" not in sql_unsampled

    # Evaluator injects filter clauses and materializes custom_fields['resp_bytes'] in site_resp_total too.
    def _handler(q: str, _params: dict[str, Any]) -> list[dict[str, Any]]:
        if "/* insight:history */" in q:
            return [{"earliest": datetime.now(UTC) - timedelta(days=10), "window_total": 1000, "hours": []}]
        if "/* insight:fields */" in q:
            return [{"fields": ALL_FIELDS}]
        return []

    monkeypatch.setattr(evaluator, "_use_materialized_cf", lambda _client: True)
    client = _FakeRealClickHouseClient(_handler)
    req = InsightsRequest(
        window_size_hrs=24, baseline_hours=168, filters={"country": {"mode": "include", "values": ["US"]}}
    )
    evaluator.insights_endpoint(_service(client, "svc-opt22-cf"), req, None, None, force_refresh=True)
    crawler_q = next(q for q, _ in client.calls if "/* insight:crawler_disparity */" in q)
    site_total_frag = crawler_q.split("AS site_resp_total")[0].rsplit("greatest(", 1)[1]
    assert "_cf_resp_bytes" in site_total_frag
    assert "country IN (" in site_total_frag


def test_origin_perf_orders_and_limits_to_11_rows() -> None:
    sql, _ = _build_origin_perf(
        {"origin_latency_spike", "timeout_split"},
        present={"ottfb", "url", "oconnect_ms"},
        rollup_span=None,
        filter_clause="",
        baseline_hours=168.0,
    )
    assert sql.endswith("HAVING g_url = 1 OR w_reqs >= 10 ORDER BY g_url DESC, url_p95_ms DESC LIMIT 11")


def test_ip_outlier_supports_baseline_sampling_and_preserves_default() -> None:
    present = {"ip", "req_header_bytes", "conn_requests"}
    default_sql, _ = _build_ip_outlier(
        {"request_size_anomaly", "connection_abuse"}, present=present, rollup_span=None, filter_clause=""
    )
    assert "intDiv(toUnixTimestamp(event_timestamp)" not in default_sql
    assert "countIf(is_b AND req_header_bytes > 0) AS rs_b_total" in default_sql
    assert "countIf(is_b AND conn_requests > 0) AS ca_b_total" in default_sql

    sampled_sql, _ = _build_ip_outlier(
        {"request_size_anomaly", "connection_abuse"},
        present=present,
        rollup_span=None,
        filter_clause="",
        baseline_hours=168.0,
    )
    assert (
        "event_timestamp >= {window_start:DateTime64(3)} OR intDiv(toUnixTimestamp(event_timestamp), 300) % 8 = 0"
        in sampled_sql
    )
    assert "countIf(is_b AND req_header_bytes > 0) * 8 AS rs_b_total" in sampled_sql
    assert "countIf(is_b AND conn_requests > 0) * 8 AS ca_b_total" in sampled_sql

    # Evaluator passes baseline_hours automatically via inspect.signature.
    def _handler(q: str, _params: dict[str, Any]) -> list[dict[str, Any]]:
        if "/* insight:history */" in q:
            return [{"earliest": datetime.now(UTC) - timedelta(days=10), "window_total": 1000, "hours": []}]
        if "/* insight:fields */" in q:
            return [{"fields": ALL_FIELDS}]
        if "/* insight:batch_ip_outlier_candidates */" in q:
            return [{"k": "203.0.113.1"}]
        return []

    client = _FakeRealClickHouseClient(_handler)
    evaluator.insights_endpoint(
        _service(client, "svc-opt22-ip"),
        InsightsRequest(window_size_hrs=24, baseline_hours=168),
        None,
        None,
        force_refresh=True,
    )
    ip_out_q = next(q for q, _ in client.calls if "/* insight:batch_ip_outlier */" in q)
    assert "intDiv(toUnixTimestamp(event_timestamp), 300) % 8 = 0" not in ip_out_q
    assert "toDateTime(" in ip_out_q
    assert "countIf(is_b AND req_header_bytes > 0) * 8 AS rs_b_total" in ip_out_q


def test_security_combined_ngwaf_unfiltered_uses_single_projection_query_and_memo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from backend.core.request_telemetry import current_clickhouse_scope

    ch_sources: list[str | None] = []
    observations: list[dict[str, Any]] = []
    monkeypatch.setattr(
        "backend.core.request_telemetry._logger.info",
        lambda _event, **entry: observations.append(entry),
    )

    def _handler(sql: str, _params: dict[str, Any]) -> list[dict[str, Any]]:
        if "/* hs_security:ngwaf_combined */" in sql:
            ch_sources.append(current_clickhouse_scope().get("source"))
            return [
                {
                    "bucket_ts": 1_790_000_000,
                    "bot_name": "alpha_bot",
                    "wk_name": "",
                    "bot_cat": "crawler",
                    "count": 3,
                },
                {
                    "bucket_ts": 1_790_000_000,
                    "bot_name": "bingbot",
                    "wk_name": "bing",
                    "bot_cat": "search",
                    "count": 5,
                },
                {
                    "bucket_ts": 1_790_003_600,
                    "bot_name": "alpha_bot",
                    "wk_name": "alpha",
                    "bot_cat": "crawler",
                    "count": 10,
                },
            ]
        return []

    client = _FakeRealClickHouseClient(_handler)
    req, start_s, end_s = _sec_req()
    with patch("backend.config.get_ngwaf_workspace_id", return_value="ws-opt22"):
        res = security.security_aggregates(
            _service(client, "svc-opt22-ngwaf"),
            req,
            start_s,
            end_s,
            sections={"ngwaf_verified_bots", "ngwaf_verified_bots_ts"},
        )

    combined_calls = [sql for sql, _ in client.calls if "/* hs_security:ngwaf_combined */" in sql]
    assert len(combined_calls) == 1
    assert not any("/* hs_security:ngwaf_bots */" in sql for sql, _ in client.calls)
    assert not any("/* hs_security:ngwaf_verified_bots_ts */" in sql for sql, _ in client.calls)
    assert "FROM security_minute_dimensions" in combined_calls[0]
    assert "request_facts" not in combined_calls[0]
    assert "GROUP BY bucket_ts, bot_name ORDER BY bucket_ts, bot_name" in combined_calls[0]
    assert ch_sources == ["projection"]
    assert any(o.get("section") == "security._combined_ngwaf" and o.get("source") == "projection" for o in observations)

    dumped = res.model_dump()
    assert dumped["ngwaf_configured"] is True
    assert dumped["ngwaf_verified_bots"] == [
        {"bot_name": "alpha_bot", "wellknown_bot_name": "alpha", "category": "crawler", "request_count": 13},
        {"bot_name": "bingbot", "wellknown_bot_name": "bing", "category": "search", "request_count": 5},
    ]
    assert dumped["ngwaf_verified_bots_ts"] == [
        {"time": "2026-09-21T14:13:20Z", "bot_name": "alpha_bot", "count": 3},
        {"time": "2026-09-21T14:13:20Z", "bot_name": "bingbot", "count": 5},
        {"time": "2026-09-21T15:13:20Z", "bot_name": "alpha_bot", "count": 10},
    ]

    # Follow-up request for a single section reuses per-section memo without re-querying ClickHouse.
    with patch("backend.config.get_ngwaf_workspace_id", return_value="ws-opt22"):
        res_single = security.security_aggregates(
            _service(client, "svc-opt22-ngwaf"),
            req,
            start_s,
            end_s,
            sections={"ngwaf_verified_bots"},
        )
    assert len(client.calls) == 1
    assert [r.model_dump() for r in res_single.ngwaf_verified_bots] == dumped["ngwaf_verified_bots"]


def test_security_combined_ngwaf_filtered_uses_request_facts_prewhere(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from backend.core.request_telemetry import current_clickhouse_scope

    ch_sources: list[str | None] = []
    observations: list[dict[str, Any]] = []
    monkeypatch.setattr(
        "backend.core.request_telemetry._logger.info",
        lambda _event, **entry: observations.append(entry),
    )

    def _handler(sql: str, _params: dict[str, Any]) -> list[dict[str, Any]]:
        if "/* hs_security:ngwaf_combined */" in sql:
            ch_sources.append(current_clickhouse_scope().get("source"))
            return [
                {
                    "bucket_ts": 1_790_000_000,
                    "bot_name": "claudebot",
                    "wk_name": "claude",
                    "bot_cat": "ai",
                    "count": 7,
                }
            ]
        return []

    client = _FakeRealClickHouseClient(_handler)
    req, start_s, end_s = _sec_req(filters={"country": {"mode": "include", "values": ["US"]}})
    with patch("backend.config.get_ngwaf_workspace_id", return_value="ws-opt22"):
        res = security.security_aggregates(
            _service(client, "svc-opt22-ngwaf-flt"),
            req,
            start_s,
            end_s,
            sections={"ngwaf_verified_bots", "ngwaf_verified_bots_ts"},
        )

    combined_calls = [(sql, params) for sql, params in client.calls if "/* hs_security:ngwaf_combined */" in sql]
    assert len(combined_calls) == 1
    sql, params = combined_calls[0]
    assert "FROM request_facts PREWHERE " in sql
    assert "security_minute_dimensions" not in sql
    assert "custom_fields['ngwaf_bot'] != ''" in sql
    assert params["filter_0_0"] == "US"
    assert ch_sources == ["fact"]
    assert any(o.get("section") == "security._combined_ngwaf" and o.get("source") == "fact" for o in observations)

    dumped = res.model_dump()
    assert dumped["ngwaf_verified_bots"] == [
        {"bot_name": "claudebot", "wellknown_bot_name": "claude", "category": "ai", "request_count": 7}
    ]
    assert dumped["ngwaf_verified_bots_ts"] == [{"time": "2026-09-21T14:13:20Z", "bot_name": "claudebot", "count": 7}]
