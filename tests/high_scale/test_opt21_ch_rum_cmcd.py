"""Unit tests for Round 21 High-Scale ClickHouse slow-query, CMCD, and RUM optimizations."""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from backend.core.catalog import LOG_FIELD_CATALOG
from backend.high_scale import cmcd as hs_cmcd
from backend.high_scale import rum as hs_rum
from backend.high_scale.ingest_controller import HighScaleService
from backend.high_scale.insights import evaluator
from backend.high_scale.insights.batches import (
    _URL_PERF_CANDIDATE_LIMIT,
    DETECTION_BATCHES,
    _build_url_perf,
    _build_url_perf_candidates,
)
from backend.high_scale.insights.sql_queries import DETECTIONS, _base_sql
from backend.models.analytics import CmcdRequest, InsightsRequest

pytestmark = pytest.mark.no_postgres

ALL_FIELDS = [f["id"] for f in LOG_FIELD_CATALOG]


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


@pytest.fixture(autouse=True)
def _stub_config_and_clear_caches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("backend.config.load_config", lambda _sid: {})
    monkeypatch.setattr("backend.config.active_log_field_ids", lambda _cfg: ALL_FIELDS)
    monkeypatch.setattr(hs_cmcd, "_asn_names", lambda _sid, _asns: {7922: "Comcast"})
    evaluator._INSIGHTS_CACHE.clear()
    evaluator._INSIGHTS_TELEMETRY_CACHE.clear()
    evaluator._INSIGHTS_PARTIAL_CACHE.clear()
    evaluator._INSIGHTS_HISTORY_CACHE.clear()
    evaluator._INSIGHTS_LAST_RESULT.clear()
    evaluator._INSIGHTS_REFRESH_FAILURES.clear()
    hs_cmcd._memo.cache.clear()
    hs_rum._rum_memo.cache.clear()


def test_crawler_disparity_tightens_row_filter_and_computes_bot_needle_once() -> None:
    sql = _base_sql(DETECTIONS["crawler_disparity"], "crawler_disparity", baseline_hours=168.0)

    # Base WHERE skips baseline rows that are neither bots nor referrals while keeping all window rows.
    base_section = sql.split("pre AS (")[0]
    assert "event_timestamp >= {window_start:DateTime64(3)}" in base_section
    assert "(referer != '' AND referer != '-')" in base_section
    assert "positionCaseInsensitive(ua, '-user') > 0" in base_section
    assert "positionCaseInsensitive(ua, '-externalfetcher') > 0" in base_section

    # Inside the CTE chain, multiSearchAnyCaseInsensitive(ua, ...) is evaluated once in `pre`, not 3x in `tagged`.
    cte_body = "pre AS (" + sql.split("pre AS (", 1)[1]
    pre_part, after_pre = cte_body.split("tagged AS (", 1)
    tagged_part, classified_and_rest = after_pre.split("classified AS (", 1)
    classified_part = classified_and_rest.split("gs AS (", 1)[0]

    assert pre_part.count("multiSearchAnyCaseInsensitive(ua,") == 1
    assert "AS is_bot_ua" in pre_part
    assert "AS is_ref_ua" in pre_part
    assert "multiSearchAnyCaseInsensitive" not in tagged_part
    assert "if(is_bot_ua, extract(lower(ua)," in tagged_part
    assert "if(NOT is_bot_ua AND ip != '' AND is_ref_ua," in tagged_part
    assert "if(is_bot_ua AND raw_tok != ''," in classified_part
    assert "if(ua_ref_tok != '', ua_ref_tok," in classified_part
    assert "if(NOT is_bot_ua AND ip != '' AND referer != '' AND referer != '-'," in classified_part

    def _handler(q: str, _params: dict[str, Any]) -> list[dict[str, Any]]:
        if "/* insight:history */" in q:
            return [{"earliest": datetime.now(UTC) - timedelta(days=10), "window_total": 1000, "hours": []}]
        if "/* insight:fields */" in q:
            return [{"fields": ALL_FIELDS}]
        if "/* insight:crawler_disparity */" in q:
            return [
                {
                    "label": "claudebot",
                    "w_crawls": 1200,
                    "w_referrals": 1,
                    "w_bytes": 120_000_000,
                    "crawls_per_referral": 1200.0,
                    "baseline_crawls_per_referral": 80.0,
                    "bytes_per_referral": 120_000_000.0,
                    "baseline_bytes_per_referral": 5_000_000.0,
                    "bandwidth_share_pct": 8.0,
                }
            ]
        return []

    client = _RecordingCHClient(_handler)
    service = SimpleNamespace(service_id="svc-opt21-crawler", client=client)
    resp = evaluator.insights_endpoint(service, InsightsRequest(window_size_hrs=24, baseline_hours=168), None, None)  # type: ignore[arg-type]
    cards = {c.id: c for c in resp.insights}
    assert cards["crawler_disparity"].severity == "warning"
    assert cards["crawler_disparity"].items[0].meta["crawls"] == 1200


def test_url_perf_raises_candidate_limit_and_drops_redundant_fallback_subquery() -> None:
    assert _URL_PERF_CANDIDATE_LIMIT == 2000
    present = {"url", "elapsed", "status", "resp_bytes", "resp_header_content_encoding"}
    active = {"latency_regression", "payload_compression_regression"}

    cand_sql = _build_url_perf_candidates(active, present=present, rollup_span=None, filter_clause="")
    assert "LIMIT 2001" in cand_sql

    batch = next(b for b in DETECTION_BATCHES if b.name == "url_perf")
    assert batch.parse_candidates is not None
    at_limit = [{"k": f"/p/{i}"} for i in range(2000)]
    above_limit = [{"k": f"/p/{i}"} for i in range(2001)]
    assert batch.parse_candidates(active, at_limit) == {"url": [f"/p/{i}" for i in range(2000)]}
    assert batch.parse_candidates(active, above_limit) == {"url": None}

    fallback_sql, _ = _build_url_perf(
        active, present=present, rollup_span=None, filter_clause="", candidates={"url": None}
    )
    assert "WHERE url != '' " in fallback_sql
    assert "IN (SELECT url FROM" not in fallback_sql

    c_params: dict[str, Any] = {}
    prewhere_sql, _ = _build_url_perf(
        active,
        present=present,
        rollup_span=None,
        filter_clause="",
        candidates={"url": ["/api/v1"]},
        candidate_params=c_params,
    )
    prewhere_match = re.search(r"PREWHERE (.*?) WHERE \(url != ''\)", prewhere_sql)
    assert prewhere_match is not None and "url IN ({cand_url_0:String})" in prewhere_match.group(1)
    assert "WHERE url != '' " not in prewhere_sql.split("FROM base", 1)[1]
    assert c_params == {"cand_url_0": "/api/v1"}


def test_cmcd_projects_minimal_columns_per_task_and_hashes_sid() -> None:
    def _handler(sql: str, _params: dict[str, Any]) -> list[dict[str, Any]]:
        if "cmcd:probe" in sql:
            return [
                {
                    "events": 20,
                    "sessions": 4,
                    **{
                        f"has_{k}": 1
                        for k in ("bl", "br", "bs", "mtp", "ot", "sf", "st", "su", "tb", "cid", "country", "asn")
                    },
                    "rebuffer_sessions": 1,
                    "avg_br_v": 2400.0,
                    "avg_br_all": 2200.0,
                    "avg_bl_v": 10.0,
                    "avg_bl_all": 9.0,
                    "med_mtp_v": 7000.0,
                    "med_mtp_all": 6500.0,
                }
            ]
        return []

    client = _RecordingCHClient(_handler)
    svc = SimpleNamespace(service_id="svc-opt21-cmcd", client=client)
    req = CmcdRequest(bucket_seconds=300, top_n=10)

    hs_cmcd.cmcd_aggregates(
        svc,
        req,
        "2026-11-25T10:00:00Z",
        "2026-11-25T11:00:00Z",
        {
            "overview",
            "sessions_ts",
            "buffer_health_ts",
            "bitrate_ts",
            "throughput_ts",
            "startup_ts",
            "session_duration_dist",
            "top_content",
            "rebuffer_by_country",
            "rebuffer_by_asn",
            "object_type_dist",
            "streaming_format_dist",
        },
        False,
    )  # type: ignore[arg-type]

    by_marker = {
        re.search(r"/\* (cmcd:\w+) \*/", sql).group(1): sql  # type: ignore[union-attr]
        for sql, _, _ in client.calls
    }

    ts_sql = by_marker["cmcd:timeseries"]
    ts_events = ts_sql.split("FROM (", 1)[1].split("GROUP BY bucket", 1)[0]
    for col in ("cmcd['cid']", "cmcd['sf']", "cmcd['st']", "country", "asn"):
        assert col not in ts_events
    assert "uniqExact(cityHash64(sid)) AS concurrent_sessions" in ts_sql
    assert "uniqExactIf(cityHash64(sid), bs = 1)" in ts_sql
    assert "quantilesTDigestIf(0.5, 0.95)" in ts_sql
    assert "quantilesTDigestIf(0.5, 0.95, 0.99)" in ts_sql

    sess_sql = by_marker["cmcd:sessions"]
    sess_events = sess_sql.split("FROM (", 1)[1].split("GROUP BY sid)", 1)[0]
    assert "event_timestamp AS ts" in sess_events
    assert "cmcd['sid'] AS sid" in sess_events
    for col in ("cmcd['cid']", "cmcd['ot']", "cmcd['bl']", "cmcd['br']", "cmcd['mtp']", "cmcd['bs']"):
        assert col not in sess_events

    sb_sql = by_marker["cmcd:session_breakdowns"]
    sb_events = sb_sql.split("WITH events AS (", 1)[1].split("), sess_agg AS", 1)[0]
    assert "cityHash64(cmcd['sid']) AS sid_h" in sb_events
    assert "cmcd['sid'] AS sid," not in sb_events
    for col in ("event_timestamp AS ts", "cmcd['sf']", "cmcd['st']", "cmcd['tb']", "cmcd['su']"):
        assert col not in sb_events
    assert "GROUP BY GROUPING SETS ((cid, sid_h), (country, sid_h), (asn, sid_h))" in sb_sql

    bd_sql = by_marker["cmcd:breakdowns"]
    bd_events = bd_sql.split("WITH events AS (", 1)[1].split(") SELECT", 1)[0]
    assert "cmcd['ot'] AS ot" in bd_events
    assert "cmcd['sf'] AS sf" in bd_events
    for col in ("event_timestamp AS ts", "cmcd['cid']", "cmcd['br']", "cmcd['bl']", "cmcd['mtp']", "country"):
        assert col not in bd_events

    # Country-only session_breakdowns omits br/bl/cid/asn; ot-only breakdowns omits sid/sf/st/br.
    hs_cmcd._memo.cache.clear()
    client.calls.clear()
    hs_cmcd.cmcd_aggregates(
        svc,
        req,
        "2026-11-25T10:00:00Z",
        "2026-11-25T11:00:00Z",
        {"rebuffer_by_country", "object_type_dist"},
        False,
    )  # type: ignore[arg-type]
    narrow = {
        re.search(r"/\* (cmcd:\w+) \*/", sql).group(1): sql  # type: ignore[union-attr]
        for sql, _, _ in client.calls
    }
    country_events = narrow["cmcd:session_breakdowns"].split("WITH events AS (", 1)[1].split(") SELECT", 1)[0]
    assert "country" in country_events and "cmcd['mtp']" in country_events
    for col in ("cmcd['cid']", "cmcd['br']", "cmcd['bl']", "asn"):
        assert col not in country_events
    assert "GROUP BY country, sid_h)" in narrow["cmcd:session_breakdowns"]

    ot_events = narrow["cmcd:breakdowns"].split("WITH events AS (", 1)[1].split(") SELECT", 1)[0]
    ot_select = ot_events.split("FROM ", 1)[0]
    assert "cmcd['ot'] AS ot" in ot_select
    assert "cmcd['sid']" not in ot_select and "cmcd['sf']" not in ot_select


def test_rum_analytics_applies_prewhere_and_fuses_slowest_queries(monkeypatch: pytest.MonkeyPatch) -> None:
    class _RumClient:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
            self.calls.append(sql)
            if "rum_analytics_counts" in sql:
                return [{"beacons": 5, "pageviews": 5, "interactions": 0, "errors": 0, "sampled": 0}]
            if "rum_analytics_slowest" in sql:
                return [
                    {
                        "kind": "url",
                        "k": "/assets/app%20bundle.js",
                        "requests": 12,
                        "avg_ms": 420.0,
                        "p50_ms": 400.0,
                        "p95_ms": 750.0,
                        "p99_ms": 900.0,
                        "matched_requests": 8,
                        "edge_avg_ms": 110.0,
                        "edge_p50_ms": 100.0,
                        "edge_p95_ms": 200.0,
                        "edge_p99_ms": 250.0,
                    },
                    {
                        "kind": "asn",
                        "k": "13335",
                        "requests": 12,
                        "avg_ms": 420.0,
                        "p50_ms": 400.0,
                        "p95_ms": 750.0,
                        "p99_ms": 900.0,
                        "matched_requests": 8,
                        "edge_avg_ms": 110.0,
                        "edge_p50_ms": 100.0,
                        "edge_p95_ms": 200.0,
                        "edge_p99_ms": 250.0,
                    },
                ]
            return []

    client = _RumClient()
    svc = HighScaleService("svc-opt21-rum", client, b"secret", None)
    monkeypatch.setattr(
        "backend.repositories.utils.response_shaping.asn.resolve_asn_names_map",
        lambda _sid, _asns: {13335: "CLOUDFLARENET"},
    )
    res = hs_rum.rum_analytics(
        svc,
        "2026-11-25T00:00:00Z",
        "2026-11-26T00:00:00Z",
        filters={"country": {"mode": "include", "values": ["US"]}},
    )

    # Fused slowest query brings rum_analytics to 7 queries instead of 8.
    assert len(client.calls) == 7
    assert not any("rum_analytics_slowest_urls" in s or "rum_analytics_slowest_networks" in s for s in client.calls)
    slowest_sql = next(s for s in client.calls if "/* rum_analytics_slowest */" in s)
    assert slowest_sql.count("FROM request_facts PREWHERE ") == 1
    assert "ttlb_joined AS (" in slowest_sql
    assert "UNION ALL" not in slowest_sql
    assert "ARRAY JOIN [('url'," in slowest_sql and "LIMIT 20 BY kind" in slowest_sql

    # Every rum_vitals_facts and rum_error_facts scan uses PREWHERE for visibility + time window.
    for sql in client.calls:
        for table in ("rum_vitals_facts", "rum_error_facts"):
            for match in re.finditer(rf"FROM {table}\s+(\w+)", sql):
                assert match.group(1) == "PREWHERE", f"Expected PREWHERE after {table} in: {sql}"
        assert "WHERE " in sql and "lower(country) IN ({flt_0_0:String})" in sql

    assert res["slowest_urls"] == [
        {
            "url": "/assets/app bundle.js",
            "requests": 12,
            "avg": 420.0,
            "p50": 400.0,
            "p95": 750.0,
            "p99": 900.0,
            "matched_requests": 8,
            "edge_avg": 110.0,
            "edge_p50": 100.0,
            "edge_p95": 200.0,
            "edge_p99": 250.0,
        }
    ]
    assert res["slowest_networks"] == [
        {
            "asn": "AS13335",
            "label": "CLOUDFLARENET (13335)",
            "requests": 12,
            "avg": 420.0,
            "p50": 400.0,
            "p95": 750.0,
            "p99": 900.0,
            "matched_requests": 8,
            "edge_avg": 110.0,
            "edge_p50": 100.0,
            "edge_p95": 200.0,
            "edge_p99": 250.0,
        }
    ]
