"""Unit tests for Round 26 High-Scale ClickHouse insights and network optimizations."""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from backend.core.catalog import LOG_FIELD_CATALOG
from backend.core.clickhouse.client import ClickHouseClient
from backend.high_scale import network as hs_network
from backend.high_scale.ingest_controller import HighScaleService
from backend.high_scale.insights import evaluator
from backend.high_scale.insights.batches import _build_url_cache
from backend.high_scale.insights.rollup import rollup_base_sql
from backend.high_scale.insights.sql_queries import _PROBE_PREWHERE, DETECTIONS, _base_sql
from backend.models.analytics import InsightsRequest, NetworkQualityRequest
from backend.repositories.insights.shaping import NEW_PROBES
from backend.repositories.utils.response_cache import client_cache_token

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


def _service(client: Any, sid: str = "svc-opt26") -> HighScaleService:
    now = datetime.now(UTC)
    return HighScaleService(
        service_id=sid,
        client=client,
        cursor_secret=b"opt26-test-secret-32-bytes-long!",
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


def test_cold_wait_seconds_reduced_for_sub_2s_cold_insights() -> None:
    assert 0 < evaluator._COLD_WAIT_SECONDS <= 1.5
    assert evaluator._COLD_WAIT_SECONDS == 1.3


def test_shield_path_degradation_skips_ch_when_shield_cache_false(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)

    def _handler(sql: str, _params: dict[str, Any]) -> list[dict[str, Any]]:
        if "/* insight:history */" in sql:
            return [{"earliest": datetime.now(UTC) - timedelta(days=10), "window_total": 1000, "hours": []}]
        if "/* insight:fields */" in sql:
            return [{"fields": ALL_FIELDS}]
        return []

    client = _FakeRealCHClient(_handler)
    svc = _service(client, "svc-opt26-edge-only")
    shield_key = (svc.service_id, client_cache_token(client))

    # When _SHIELD_ACTIVE_CACHE is False, shield_path_degradation skips CH and returns an empty card.
    hs_network._SHIELD_ACTIVE_CACHE[shield_key] = False
    resp = evaluator.insights_endpoint(
        svc,
        InsightsRequest(window_size_hrs=24, baseline_hours=168),
        None,
        None,
        force_refresh=True,
    )
    assert not any("/* insight:shield_path_degradation */" in q for q, _, _ in client.calls)
    cards = {c["id"]: c for c in resp.model_dump()["insights"]}
    assert "shield_path_degradation" in cards
    assert cards["shield_path_degradation"]["items"] == []
    assert cards["shield_path_degradation"]["severity"] != "error"

    # When _SHIELD_ACTIVE_CACHE is True (or None), shield_path_degradation executes normally.
    client.calls.clear()
    hs_network._SHIELD_ACTIVE_CACHE[shield_key] = True
    evaluator.insights_endpoint(
        svc,
        InsightsRequest(window_size_hrs=24, baseline_hours=168),
        None,
        None,
        force_refresh=True,
    )
    assert any("/* insight:shield_path_degradation */" in q for q, _, _ in client.calls)


def test_probe_prewhere_excludes_pcre_and_keeps_match_in_row_filter() -> None:
    assert _PROBE_PREWHERE.startswith("multiSearchAnyCaseInsensitive(url, [")
    assert _PROBE_PREWHERE.endswith("])")
    assert "match(" not in _PROBE_PREWHERE
    assert "{probe_regex:String}" not in _PROBE_PREWHERE
    for needle in NEW_PROBES:
        assert f"'{needle}'" in _PROBE_PREWHERE

    for insight_id in ("new_probe_urls", "low_and_slow"):
        det = DETECTIONS[insight_id]
        assert det.prewhere == _PROBE_PREWHERE
        assert "match(url, {probe_regex:String})" in det.row_filter
        sql = _base_sql(det, insight_id)
        pre = re.search(r"FROM request_facts PREWHERE (.*?) WHERE ", sql)
        assert pre is not None
        assert pre.group(1) == _PROBE_PREWHERE
        assert "match(" not in pre.group(1)
        assert sql.count("match(url, {probe_regex:String})") == 1
        assert sql.index("WHERE ") < sql.index("match(url, {probe_regex:String})")

    rollup_sql = rollup_base_sql(
        DETECTIONS["new_probe_urls"], "new_probe_urls", "url_status", baseline_hours=168.0, window_hours=24.0
    )
    raw_part, rollup_part = rollup_sql.split("UNION ALL", 1)
    assert f"PREWHERE {_PROBE_PREWHERE}" in raw_part
    assert "match(url, {probe_regex:String})" in raw_part.split(" WHERE ", 1)[1]
    assert "match(url, {probe_regex:String})" in rollup_part.split(" WHERE ", 1)[1]


def test_shield_path_degradation_samples_window_and_scales_count() -> None:
    det = DETECTIONS["shield_path_degradation"]
    assert det.window_sampled is True
    assert det.baseline_sampled is True

    sql_24h = _base_sql(det, "shield_path_degradation", baseline_hours=168.0, window_hours=24.0)
    assert (
        "((event_timestamp >= {window_start:DateTime64(3)} AND intDiv(toUnixTimestamp(event_timestamp), 300) % 4 = 0)"
        " OR (event_timestamp < {window_start:DateTime64(3)} AND intDiv(toUnixTimestamp(event_timestamp), 300) % 8 = 0))"
        in sql_24h
    )
    assert "countIf(is_w) * 4 AS w_cnt" in sql_24h

    sql_1h = _base_sql(det, "shield_path_degradation", baseline_hours=168.0, window_hours=1.0)
    assert "intDiv(toUnixTimestamp(event_timestamp), 300) % 8 = 0" in sql_1h
    assert "countIf(is_w) AS w_cnt" in sql_1h


def test_build_url_cache_samples_24h_window_and_evaluates_with_window_hours() -> None:
    present = {"url", "cache"}
    active = {"cache_collapse", "cacheability_regression"}
    span = (datetime(2026, 11, 20, 0, tzinfo=UTC), datetime(2026, 11, 25, 0, tzinfo=UTC))

    sql_24h, src = _build_url_cache(
        active,
        present=present,
        rollup_span=span,
        filter_clause="",
        baseline_hours=168.0,
        window_hours=24.0,
    )
    assert src == "mixed"
    raw_part, rollup_part = sql_24h.split("UNION ALL", 1)
    assert (
        "((event_timestamp >= {window_start:DateTime64(3)} AND intDiv(toUnixTimestamp(event_timestamp), 300) % 4 = 0)"
        " OR (event_timestamp < {window_start:DateTime64(3)} AND intDiv(toUnixTimestamp(event_timestamp), 300) % 8 = 0))"
        in raw_part
    )
    assert "if(event_timestamp < {window_start:DateTime64(3)}, toUInt64(8), toUInt64(4)) AS n" in raw_part
    assert "intDiv(toUnixTimestamp(event_timestamp), 300)" not in rollup_part

    def _handler(q: str, _params: dict[str, Any]) -> list[dict[str, Any]]:
        if "/* insight:history */" in q:
            return [{"earliest": datetime.now(UTC) - timedelta(days=10), "window_total": 1000, "hours": []}]
        if "/* insight:fields */" in q:
            return [{"fields": ALL_FIELDS}]
        return []

    client = _FakeRealCHClient(_handler)
    evaluator.insights_endpoint(
        _service(client, "svc-opt26-url-cache"),
        InsightsRequest(window_size_hrs=24, baseline_hours=168),
        None,
        None,
        force_refresh=True,
    )
    url_cache_q = next(q for q, _, _ in client.calls if "/* insight:batch_url_cache */" in q)
    assert "intDiv(toUnixTimestamp(event_timestamp), 300) % 4 = 0" not in url_cache_q
    assert "toDateTime(" in url_cache_q
    assert "toUInt64(4)" in url_cache_q


def test_network_sample_stride_14d_is_500_and_standalone_quality_shares_pop_rollup_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    assert hs_network._network_sample_stride(1209600) == 500
    assert hs_network._network_sample_stride(2592000) == 1000

    end = datetime(2026, 11, 26, 12, 0, tzinfo=UTC)
    start_30d = end - timedelta(days=30)
    start_24h = end - timedelta(hours=24)

    def _handler(sql: str, _params: dict[str, Any]) -> list[dict[str, Any]]:
        if "'rollup' AS kind" in sql:
            return [
                {
                    "kind": "rollup",
                    "pop": "SJC",
                    "req_count": 200,
                    "errors": 2,
                    "p50_rtt_us": 15000.0,
                    "p95_ttfb_ms": 42.0,
                    "cache_hits": 0,
                    "bandwidth_bytes": 0,
                },
                {
                    "kind": "fact",
                    "pop": "SJC",
                    "req_count": 10,
                    "errors": None,
                    "p50_rtt_us": None,
                    "p95_ttfb_ms": None,
                    "cache_hits": 6,
                    "bandwidth_bytes": 4000,
                },
            ]
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
        if "AS cache_state" in sql:
            return [{"rtt_ms": 20.0, "ttfb_ms": 40.0, "cache_state": "HIT"}] * 60
        if "AS bandwidth_bytes" in sql:
            return [{"pop": "SJC", "requests": 10, "cache_hits": 6, "bandwidth_bytes": 4000}]
        return []

    client = _FakeRealCHClient(_handler)
    svc = _service(client, "svc-opt26-pop-cache")

    # 1. Standalone network_quality populates _pop_rollup_cache for the 30d window.
    hs_network.network_quality(svc, NetworkQualityRequest(), start_30d.isoformat(), end.isoformat())
    assert sum(1 for sql, _, _ in client.calls if "FROM network_minute_dimensions" in sql) == 1

    # 2. Standalone get_pop_health for the same 30d window hits _pop_rollup_cache and skips network_minute_dimensions.
    client.calls.clear()
    pop_30d = hs_network.get_pop_health(svc, start_30d, end)
    assert sum(1 for sql, _, _ in client.calls if "FROM network_minute_dimensions" in sql) == 0
    assert len(client.calls) == 1
    assert pop_30d.data[0].pop == "SJC"
    assert pop_30d.data[0].requests == 1200
    assert pop_30d.data[0].errors == 12
    assert pop_30d.data[0].p50_rtt_us == 15000.0
    assert pop_30d.data[0].p95_ttfb_ms == 42.0
    assert pop_30d.data[0].cache_hit_rate == 60.0

    # 3. A different window (24h vs 30d) does NOT collide with the 30d _pop_rollup_cache entry.
    client.calls.clear()
    hs_network.get_pop_health(svc, start_24h, end)
    assert sum(1 for sql, _, _ in client.calls if "FROM network_minute_dimensions" in sql) == 1
