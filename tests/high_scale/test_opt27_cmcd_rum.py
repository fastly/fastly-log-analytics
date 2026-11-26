"""Unit and real-ClickHouse parity tests for Round 27 CMCD and RUM optimizations."""

from __future__ import annotations

import json
import os
import re
import subprocess
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from email.parser import BytesParser
from email.policy import default
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch
from uuid import uuid4

import httpx
import pytest

from backend.config import ClickHouseConfig
from backend.core.catalog import LOG_FIELD_CATALOG
from backend.core.clickhouse.client import ClickHouseClient
from backend.high_scale import cmcd as hs_cmcd
from backend.high_scale import rum as hs_rum
from backend.high_scale.ingest_controller import HighScaleService
from backend.models.analytics import CmcdAggregatesResponse, CmcdRequest, RumAnalyticsResponse

pytestmark = pytest.mark.no_postgres

ALL_FIELDS = [f["id"] for f in LOG_FIELD_CATALOG]
_CH_IMG = os.getenv("RUM_CLICKHOUSE_TEST_IMAGE", "clickhouse/clickhouse-server:25.8.4.13")
_skip_unless_real_ch = pytest.mark.skipif(
    os.getenv("RUN_RUM_CLICKHOUSE_INTEGRATION") != "1",
    reason="explicit real ClickHouse integration gate",
)


class _FakeRealCHClient(ClickHouseClient):
    """Fake ClickHouseClient that triggers real-CH paths and records executed SQL."""

    def __init__(self, handler: Any = None) -> None:
        self.calls: list[tuple[str, dict[str, Any], dict[str, str] | None]] = []
        self._handler = handler
        self._http = SimpleNamespace(_transport=object())

    def _has_materialized_cf_columns(self) -> bool:
        return False

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


def _service(client: Any, sid: str = "svc-opt27") -> HighScaleService:
    now = datetime.now(UTC)
    return HighScaleService(
        service_id=sid,
        client=client,
        cursor_secret=b"opt27-test-secret-32-bytes-long!",
        request_watermark=lambda: now,
    )


@pytest.fixture(autouse=True)
def _stub_and_clear_caches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("backend.config.load_config", lambda _sid: {})
    monkeypatch.setattr("backend.config.active_log_field_ids", lambda _cfg: ALL_FIELDS)
    monkeypatch.setattr("backend.config.is_cmcd_enabled", lambda _cfg: True)
    monkeypatch.setattr(hs_cmcd, "_asn_names", lambda _sid, _asns: {7922: "Comcast"})
    hs_cmcd._memo.cache.clear()
    hs_rum._rum_memo.cache.clear()


def test_cmcd_sample_stride_tiers() -> None:
    assert hs_cmcd._cmcd_sample_stride(86400) == 1
    assert hs_cmcd._cmcd_sample_stride(604799) == 1
    assert hs_cmcd._cmcd_sample_stride(604800) == 4
    assert hs_cmcd._cmcd_sample_stride(1209599) == 4
    assert hs_cmcd._cmcd_sample_stride(1209600) == 10
    assert hs_cmcd._cmcd_sample_stride(2592000) == 10


def test_cmcd_cold_streaming_folds_probe_into_timeseries_and_reduces_queries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    bucket_dt = datetime(2026, 11, 25, 10, 5, tzinfo=UTC)
    bucket_epoch = int(bucket_dt.timestamp())

    def _handler(sql: str, _p: dict[str, Any]) -> list[dict[str, Any]]:
        if "/* cmcd:timeseries */" in sql:
            return [
                {
                    "is_probe": 0,
                    "bucket_ts": bucket_epoch,
                    "events": 20,
                    "has_bl": 1,
                    "has_br": 1,
                    "has_bs": 1,
                    "has_mtp": 1,
                    "has_ot": 1,
                    "has_su": 1,
                    "has_cid": 1,
                    "has_country": 1,
                    "has_asn": 1,
                    "has_sf": 1,
                    "has_st": 1,
                    "concurrent_sessions": 4,
                    "_reb_sess": 1,
                    "rebuffer_session_pct": 25.0,
                    "p50_buffer": 10.0,
                    "p95_buffer": 18.0,
                    "p50_buffer_all": 9.5,
                    "p95_buffer_all": 17.5,
                    "starvation_rate": 5.0,
                    "avg_bl_v": 10.5,
                    "avg_bl_all": 10.0,
                    "avg_bitrate": 3200.0,
                    "avg_bitrate_all": 3000.0,
                    "utilization_ratio": 0.65,
                    "utilization_ratio_all": 0.60,
                    "p50_throughput": 7200.0,
                    "p95_throughput": 9000.0,
                    "p99_throughput": 9800.0,
                    "p50_throughput_all": 7000.0,
                    "p95_throughput_all": 8800.0,
                    "p99_throughput_all": 9500.0,
                    "startup_ratio": 2.5,
                },
                {
                    "is_probe": 1,
                    "bucket_ts": 0,
                    "events": 20,
                    "has_bl": 1,
                    "has_br": 1,
                    "has_bs": 1,
                    "has_mtp": 1,
                    "has_ot": 1,
                    "has_su": 1,
                    "has_cid": 1,
                    "has_country": 1,
                    "has_asn": 1,
                    "has_sf": 1,
                    "has_st": 1,
                    "concurrent_sessions": 5,
                    "_reb_sess": 1,
                    "rebuffer_session_pct": 20.0,
                    "p50_buffer": 10.0,
                    "p95_buffer": 18.0,
                    "p50_buffer_all": 9.5,
                    "p95_buffer_all": 17.5,
                    "starvation_rate": 5.0,
                    "avg_bl_v": 11.0,
                    "avg_bl_all": 10.0,
                    "avg_bitrate": 3100.0,
                    "avg_bitrate_all": 2900.0,
                    "utilization_ratio": 0.65,
                    "utilization_ratio_all": 0.60,
                    "p50_throughput": 7400.0,
                    "p95_throughput": 9000.0,
                    "p99_throughput": 9800.0,
                    "p50_throughput_all": 7000.0,
                    "p95_throughput_all": 8800.0,
                    "p99_throughput_all": 9500.0,
                    "startup_ratio": 2.5,
                },
            ]
        if "/* cmcd:sessions */" in sql:
            return [
                {
                    "kind": "new_sessions",
                    "k": str(bucket_epoch),
                    "session_count": 3,
                    "positive_duration_sum": 0.0,
                    "positive_duration_count": 0,
                },
                {
                    "kind": "duration",
                    "k": "2",
                    "session_count": 5,
                    "positive_duration_sum": 450.0,
                    "positive_duration_count": 5,
                },
            ]
        if "/* cmcd:session_breakdowns */" in sql:
            return [
                {
                    "kind": "content",
                    "k": "c1",
                    "session_count": 4,
                    "request_count": 0,
                    "rebuffer_rate": 25.0,
                    "avg_br": 3200.0,
                    "avg_br_all": 3000.0,
                    "avg_bl": 10.0,
                    "avg_bl_all": 9.5,
                    "median_mtp": None,
                    "median_mtp_all": None,
                },
                {
                    "kind": "country",
                    "k": "US",
                    "session_count": 5,
                    "request_count": 0,
                    "rebuffer_rate": 20.0,
                    "avg_br": None,
                    "avg_br_all": None,
                    "avg_bl": None,
                    "avg_bl_all": None,
                    "median_mtp": 7400.0,
                    "median_mtp_all": 7000.0,
                },
                {
                    "kind": "asn",
                    "k": "7922",
                    "session_count": 3,
                    "request_count": 0,
                    "rebuffer_rate": 33.33,
                    "avg_br": 3100.0,
                    "avg_br_all": 2900.0,
                    "avg_bl": 9.0,
                    "avg_bl_all": 8.5,
                    "median_mtp": None,
                    "median_mtp_all": None,
                },
            ]
        if "/* cmcd:breakdowns */" in sql:
            return [
                {"kind": "ot", "k": "v", "session_count": 15, "request_count": 15},
                {"kind": "sf", "k": "h", "session_count": 5, "request_count": 20},
                {
                    "kind": "ladder",
                    "k": "3",
                    "session_count": 3,
                    "request_count": 10,
                    "avg_br": 3200.0,
                    "rebuffer_rate": 10.0,
                },
                {"kind": "st", "k": "v", "session_count": 5, "request_count": 20},
            ]
        return []

    client = _FakeRealCHClient(_handler)
    svc = _service(client, "svc-opt27-cmcd")
    req = CmcdRequest(
        bucket_seconds=300,
        top_n=10,
        filters={"country": {"mode": "include", "values": ["US"]}},
    )
    res = hs_cmcd.cmcd_aggregates(svc, req, "2026-11-25T10:00:00Z", "2026-11-25T10:20:00Z", None, False)

    # Cold /streaming drops from 5 queries to 4 by folding probe into timeseries.
    assert len(client.calls) == 4
    markers = {re.search(r"/\* (cmcd:\w+) \*/", sql).group(1) for sql, _, _ in client.calls}  # type: ignore[union-attr]
    assert markers == {"cmcd:timeseries", "cmcd:sessions", "cmcd:session_breakdowns", "cmcd:breakdowns"}
    assert "cmcd:probe" not in markers

    # Every query puts service_id, publication_state, timestamps, country filter, and cmcd['sid'] != '' in PREWHERE.
    for sql, _, _ in client.calls:
        pre = re.search(r"PREWHERE (.*?) WHERE ", sql, re.DOTALL)
        assert pre is not None
        pre_txt = pre.group(1)
        assert "country IN ({flt_0_0:String})" in pre_txt
        assert "cmcd['sid'] != ''" in pre_txt
        assert "formatDateTime(" not in sql

    ts_sql = next(sql for sql, _, _ in client.calls if "/* cmcd:timeseries */" in sql)
    assert "GROUP BY GROUPING SETS ((bucket_ts), ())" in ts_sql
    assert "cityHash64(cmcd['sid']) AS sid_h" in ts_sql

    sess_sql = next(sql for sql, _, _ in client.calls if "/* cmcd:sessions */" in sql)
    assert "GROUP BY sid_h" in sess_sql

    sb_sql = next(sql for sql, _, _ in client.calls if "/* cmcd:session_breakdowns */" in sql)
    assert "sumIf(br," in sb_sql and "avgIf(br," not in sb_sql

    bd_sql = next(sql for sql, _, _ in client.calls if "/* cmcd:breakdowns */" in sql)
    assert "uniqExact(sid_h)" in bd_sql and "concat('all:'" not in bd_sql

    model = CmcdAggregatesResponse.model_validate(res)
    assert model.overview is not None
    assert model.overview.active_sessions == 5
    assert model.overview.rebuffer_rate == 20.0
    assert model.overview.avg_bitrate == 3100.0
    assert model.overview.avg_buffer_length == 11.0
    assert model.overview.median_throughput == 7400.0
    assert model.overview.peak_viewers == 4
    assert model.overview.avg_session_duration == 90
    assert res["sessions_ts"][1]["concurrent_sessions"] == 4
    assert res["sessions_ts"][1]["new_sessions"] == 3
    assert res["bitrate_ladder"][0]["tier"] == "2.5–5 Mbps (1080p)"


def test_cmcd_30d_samples_sessions_scales_counts_and_falls_back_when_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    end = datetime(2026, 11, 26, 0, 0, tzinfo=UTC)
    start_30d = end - timedelta(days=30)
    bucket_epoch = int((end - timedelta(hours=1)).timestamp())

    def _handler_populated(sql: str, _p: dict[str, Any]) -> list[dict[str, Any]]:
        if "/* cmcd:timeseries */" in sql:
            return [
                {
                    "is_probe": 0,
                    "bucket_ts": bucket_epoch,
                    "events": 100,
                    "has_bl": 1,
                    "has_br": 1,
                    "has_bs": 1,
                    "has_mtp": 1,
                    "has_ot": 1,
                    "has_su": 1,
                    "has_cid": 1,
                    "has_country": 1,
                    "has_asn": 1,
                    "has_sf": 1,
                    "has_st": 1,
                    "concurrent_sessions": 7,
                    "_reb_sess": 1,
                    "rebuffer_session_pct": 14.286,
                    "p50_buffer": 10.0,
                    "p95_buffer": 18.0,
                    "p50_buffer_all": 10.0,
                    "p95_buffer_all": 18.0,
                    "starvation_rate": 2.0,
                    "avg_bl_v": 10.0,
                    "avg_bl_all": 10.0,
                    "avg_bitrate": 3000.0,
                    "avg_bitrate_all": 3000.0,
                    "utilization_ratio": 0.5,
                    "utilization_ratio_all": 0.5,
                    "p50_throughput": 8000.0,
                    "p95_throughput": 9000.0,
                    "p99_throughput": 9500.0,
                    "p50_throughput_all": 8000.0,
                    "p95_throughput_all": 9000.0,
                    "p99_throughput_all": 9500.0,
                    "startup_ratio": 1.0,
                },
                {
                    "is_probe": 1,
                    "bucket_ts": 0,
                    "events": 100,
                    "has_bl": 1,
                    "has_br": 1,
                    "has_bs": 1,
                    "has_mtp": 1,
                    "has_ot": 1,
                    "has_su": 1,
                    "has_cid": 1,
                    "has_country": 1,
                    "has_asn": 1,
                    "has_sf": 1,
                    "has_st": 1,
                    "concurrent_sessions": 12,
                    "_reb_sess": 3,
                    "avg_bl_v": 10.0,
                    "avg_bl_all": 10.0,
                    "avg_bitrate": 3000.0,
                    "avg_bitrate_all": 3000.0,
                    "p50_throughput": 8000.0,
                    "p50_throughput_all": 8000.0,
                },
            ]
        if "/* cmcd:sessions */" in sql:
            return [
                {"kind": "new_sessions", "k": str(bucket_epoch), "session_count": 4},
                {
                    "kind": "duration",
                    "k": "3",
                    "session_count": 12,
                    "positive_duration_sum": 1800.0,
                    "positive_duration_count": 12,
                },
            ]
        if "/* cmcd:session_breakdowns */" in sql:
            return [
                {
                    "kind": "content",
                    "k": "c1",
                    "session_count": 6,
                    "rebuffer_rate": 25.0,
                    "avg_br": 3000.0,
                    "avg_bl": 10.0,
                },
                {"kind": "country", "k": "US", "session_count": 8, "rebuffer_rate": 25.0, "median_mtp": 8000.0},
                {
                    "kind": "asn",
                    "k": "7922",
                    "session_count": 5,
                    "rebuffer_rate": 20.0,
                    "avg_br": 3000.0,
                    "avg_bl": 10.0,
                },
            ]
        if "/* cmcd:breakdowns */" in sql:
            return [
                {"kind": "ot", "k": "v", "session_count": 40, "request_count": 40},
                {"kind": "sf", "k": "h", "session_count": 12, "request_count": 50},
                {
                    "kind": "ladder",
                    "k": "3",
                    "session_count": 6,
                    "request_count": 25,
                    "avg_br": 3000.0,
                    "rebuffer_rate": 10.0,
                },
                {"kind": "st", "k": "v", "session_count": 12, "request_count": 50},
            ]
        return []

    client = _FakeRealCHClient(_handler_populated)
    svc = _service(client, "svc-opt27-30d")
    res = hs_cmcd.cmcd_aggregates(
        svc, CmcdRequest(bucket_seconds=3600, top_n=10), start_30d.isoformat(), end.isoformat(), None, False
    )

    for sql, _, _ in client.calls:
        assert "cityHash64(cmcd['sid']) % 10 = 0" in sql

    assert res["overview"]["active_sessions"] == 120
    assert res["overview"]["peak_viewers"] == 70
    assert res["overview"]["rebuffer_rate"] == 25.0
    assert res["overview"]["avg_session_duration"] == 150
    last_ts = next(p for p in res["sessions_ts"] if p["concurrent_sessions"] > 0)
    assert last_ts["concurrent_sessions"] == 70
    assert last_ts["new_sessions"] == 40
    assert res["session_duration_dist"][0]["session_count"] == 120
    assert res["top_content"][0]["session_count"] == 60
    assert res["rebuffer_by_country"][0]["session_count"] == 80
    assert res["rebuffer_by_asn"][0]["session_count"] == 50
    assert res["object_type_dist"][0]["request_count"] == 400
    assert res["streaming_format_dist"][0]["session_count"] == 120
    assert res["bitrate_ladder"][0]["request_count"] == 250
    assert res["bitrate_ladder"][0]["session_count"] == 60
    assert res["stream_type_dist"][0]["session_count"] == 120
    assert res["stream_type_dist"][0]["request_count"] == 500

    # Sparse-window fallback: if sampled run finds 0 events, falls back once to unsampled (stride=1).
    hs_cmcd._memo.cache.clear()
    seen_sampled: list[bool] = []

    def _handler_sparse(sql: str, p: dict[str, Any]) -> list[dict[str, Any]]:
        is_sampled = "% 10 = 0" in sql
        seen_sampled.append(is_sampled)
        if is_sampled:
            return []
        return _handler_populated(sql, p)

    sparse_client = _FakeRealCHClient(_handler_sparse)
    sparse_res = hs_cmcd.cmcd_aggregates(
        _service(sparse_client, "svc-opt27-sparse"),
        CmcdRequest(bucket_seconds=3600, top_n=10),
        start_30d.isoformat(),
        end.isoformat(),
        None,
        False,
    )
    assert True in seen_sampled and False in seen_sampled
    assert sparse_res["has_data"] is True
    assert sparse_res["overview"]["active_sessions"] == 12


def test_cmcd_request_facts_filter_path_also_uses_prewhere(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    client = _FakeRealCHClient(lambda _s, _p: [{"events": 0}])
    svc = _service(client, "svc-opt27-reqfacts")
    hs_cmcd.cmcd_aggregates(
        svc,
        CmcdRequest(bucket_seconds=300, filters={"url": {"mode": "include", "values": ["/seg/1.m4s"]}}),
        "2026-11-25T10:00:00Z",
        "2026-11-25T11:00:00Z",
        {"overview"},
        False,
    )
    probe_sql = next(sql for sql, _, _ in client.calls if "/* cmcd:probe */" in sql)
    assert "FROM request_facts\n    PREWHERE service_id={service_id:String}" in probe_sql


def test_rum_analytics_folds_counts_trends_pages_to_5_queries_with_tight_prewhere(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)

    def _handler(sql: str, _p: dict[str, Any]) -> list[dict[str, Any]]:
        if "/* rum_analytics_counts_trends_pages */" in sql:
            return [
                {
                    "kind": "count",
                    "bucket": "1970-01-01 00:00:00",
                    "path": "",
                    "beacons": 12,
                    "views": 10,
                    "pageviews": 8,
                    "interactions": 2,
                    "errors": 2,
                    "sampled": 0,
                    "avg_load": None,
                    "lcp": None,
                    "cls": None,
                    "fcp": None,
                    "ttfb": None,
                    "error_rate": None,
                    "page_rn": 1,
                },
                {
                    "kind": "trend",
                    "bucket": "2026-11-25 10:00:00",
                    "path": "",
                    "beacons": 12,
                    "views": 10,
                    "pageviews": 8,
                    "interactions": 2,
                    "errors": 2,
                    "sampled": 0,
                    "avg_load": None,
                    "lcp": 2200.0,
                    "cls": 0.08,
                    "fcp": 1100.0,
                    "ttfb": 400.0,
                    "error_rate": 16.67,
                    "page_rn": 1,
                },
                {
                    "kind": "page",
                    "bucket": "1970-01-01 00:00:00",
                    "path": "/checkout%20page",
                    "beacons": 12,
                    "views": 10,
                    "pageviews": 8,
                    "interactions": 2,
                    "errors": 2,
                    "sampled": 0,
                    "avg_load": 1500.0,
                    "lcp": 2200.0,
                    "cls": 0.08,
                    "fcp": None,
                    "ttfb": None,
                    "error_rate": 16.666,
                    "page_rn": 1,
                },
            ]
        if "/* rum_analytics_vitals */" in sql:
            return [{"metric_name": "lcp", "p75": 2200.0, "total": 10, "good": 8, "ni": 2, "poor": 0}]
        if "/* rum_analytics_environments */" in sql:
            return [
                {
                    "env_browser": "Chrome",
                    "env_os": "Unknown",
                    "env_device": "Unknown",
                    "beacons": 10,
                    "g_browser": 0,
                    "g_os": 1,
                    "g_device": 1,
                },
                {
                    "env_browser": "Unknown",
                    "env_os": "macOS",
                    "env_device": "Unknown",
                    "beacons": 10,
                    "g_browser": 1,
                    "g_os": 0,
                    "g_device": 1,
                },
                {
                    "env_browser": "Unknown",
                    "env_os": "Unknown",
                    "env_device": "Desktop",
                    "beacons": 10,
                    "g_browser": 1,
                    "g_os": 1,
                    "g_device": 0,
                },
            ]
        if "/* rum_analytics_exceptions */" in sql:
            return [{"error_message": "boom", "error_file": "app.js", "error_line": 10, "error_col": 2, "count": 2}]
        if "/* rum_analytics_slowest */" in sql:
            return [
                {
                    "kind": "url",
                    "k": "/api/data",
                    "requests": 5,
                    "avg_ms": 300.0,
                    "p50_ms": 280.0,
                    "p95_ms": 500.0,
                    "p99_ms": 600.0,
                    "matched_requests": 4,
                    "edge_avg_ms": 50.0,
                    "edge_p50_ms": 40.0,
                    "edge_p95_ms": 90.0,
                    "edge_p99_ms": 120.0,
                }
            ]
        return []

    client = _FakeRealCHClient(_handler)
    svc = _service(client, "svc-opt27-rum")
    with patch("backend.repositories.utils.response_shaping.asn.resolve_asn_names_map", return_value={}):
        res = hs_rum.rum_analytics(
            svc,
            "2026-11-25T10:00:00Z",
            "2026-11-25T12:00:00Z",
            filters={
                "country": {"mode": "include", "values": ["US"]},
                "pathname": {"mode": "include", "values": ["/checkout"]},
            },
        )

    # Query count reduced from 7 -> 5!
    assert len(client.calls) == 5
    markers = {re.search(r"/\* (\w+) \*/", sql).group(1) for sql, _, _ in client.calls}  # type: ignore[union-attr]
    assert markers == {
        "rum_analytics_counts_trends_pages",
        "rum_analytics_vitals",
        "rum_analytics_environments",
        "rum_analytics_exceptions",
        "rum_analytics_slowest",
    }

    # LowCardinality filters (country) & metric_name gates are in PREWHERE; pathname stays in WHERE.
    for sql, _, _ in client.calls:
        assert "lower(country) IN ({flt_0_0:String})" in sql
        for pre_match in re.finditer(
            r"FROM (rum_vitals_facts|rum_error_facts) PREWHERE (.*?) WHERE (pathname IN|req_id != '')",
            sql,
        ):
            pre_txt = pre_match.group(2)
            assert "lower(country) IN ({flt_0_0:String})" in pre_txt
            assert "pathname IN" not in pre_txt
        if "rum_analytics_slowest" in sql:
            assert "metric_name = 'ttlb'" in sql
            assert "ARRAY JOIN [('url'," in sql
        elif "rum_analytics_exceptions" not in sql:
            assert "metric_name != 'ttlb'" in sql

    ctp_sql = next(sql for sql, _, _ in client.calls if "rum_analytics_counts_trends_pages" in sql)
    assert "GROUP BY GROUPING SETS ((bucket_col), (pathname), ())" in ctp_sql
    assert "cityHash64(" in ctp_sql
    assert "WHERE kind != 'page' OR page_rn <= 5" in ctp_sql

    model = RumAnalyticsResponse.model_validate(res)
    assert model.beacon_count == 12
    assert model.pageview_count == 8
    assert model.interaction_count == 2
    assert model.error_count == 2
    assert model.worst_pages is not None and model.worst_pages[0].path == "/checkout page"
    assert model.worst_pages[0].error_rate == 16.67
    assert model.trends is not None and model.trends.lcp == [2.2, None]
    assert model.environments is not None and model.environments.browsers == {"Chrome": 10}


@_skip_unless_real_ch
def test_real_clickhouse_opt27_cmcd_and_rum_parity(monkeypatch: pytest.MonkeyPatch) -> None:
    pub_schema = re.sub(
        r"ReplicatedReplacingMergeTree\(([^)]+)\)\s*\('[^']+',\s*'[^']+'\)",
        r"ReplacingMergeTree(\1)",
        Path("backend/high_scale/sql/publication_schema.sql").read_text(),
    )
    cmcd_schema = re.sub(
        r"\nTTL[\s\S]*?;",
        ";",
        re.sub(
            r"ReplicatedMergeTree\([^;]*?\)", "MergeTree()", Path("backend/high_scale/sql/cmcd_schema.sql").read_text()
        ),
    )
    rum_schema = re.sub(
        r"\nTTL [^;]*;",
        ";",
        re.sub(
            r"ReplicatedMergeTree\([^;]*?\)", "MergeTree()", Path("backend/high_scale/sql/rum_schema.sql").read_text()
        ),
    )
    req_raw = (
        Path("backend/high_scale/sql/request_schema.sql")
        .read_text()
        .split("CREATE TABLE IF NOT EXISTS request_aggregates", 1)[0]
    )
    req_schema = re.sub(
        r"\nTTL[\s\S]*?;",
        ";",
        re.sub(r"ReplicatedMergeTree\([^;]*?\)", "MergeTree()", req_raw),
    )

    b_cmcd = str(uuid4())
    b_vitals = str(uuid4())
    b_errors = str(uuid4())
    b_req = str(uuid4())
    pub_rows = [
        {
            "batch_id": b_cmcd,
            "service_id": "svc-ch",
            "domain": "cmcd",
            "generation": "1",
            "batch_digest": "d",
            "expected_rows": 4,
            "visible_rows": 4,
            "quorum_acked": 1,
            "publication_state": "visible",
            "manifest_version": 2,
            "updated_at": "2026-11-25 10:00:00.000",
        },
        {
            "batch_id": b_vitals,
            "service_id": "svc-ch",
            "domain": "rum_vitals",
            "generation": "1",
            "batch_digest": "d",
            "expected_rows": 5,
            "visible_rows": 5,
            "quorum_acked": 1,
            "publication_state": "visible",
            "manifest_version": 2,
            "updated_at": "2026-11-25 10:00:00.000",
        },
        {
            "batch_id": b_errors,
            "service_id": "svc-ch",
            "domain": "rum_errors",
            "generation": "1",
            "batch_digest": "d",
            "expected_rows": 2,
            "visible_rows": 2,
            "quorum_acked": 1,
            "publication_state": "visible",
            "manifest_version": 2,
            "updated_at": "2026-11-25 10:00:00.000",
        },
        {
            "batch_id": b_req,
            "service_id": "svc-ch",
            "domain": "request",
            "generation": "1",
            "batch_digest": "d",
            "expected_rows": 1,
            "visible_rows": 1,
            "quorum_acked": 1,
            "publication_state": "visible",
            "manifest_version": 2,
            "updated_at": "2026-11-25 10:00:00.000",
        },
    ]
    cmcd_rows = [
        {
            "service_id": "svc-ch",
            "projection_key": str(uuid4()),
            "request_event_id": str(uuid4()),
            "event_timestamp": "2026-11-25 10:00:10.000",
            "batch_id": b_cmcd,
            "publication_state": "visible",
            "cmcd": {
                "sid": "s1",
                "cid": "c1",
                "ot": "v",
                "sf": "h",
                "st": "v",
                "bl": "10.0",
                "br": "3000",
                "mtp": "8000",
                "tb": "5000",
                "su": "1",
            },
            "country": "US",
            "asn": 7922,
        },
        {
            "service_id": "svc-ch",
            "projection_key": str(uuid4()),
            "request_event_id": str(uuid4()),
            "event_timestamp": "2026-11-25 10:01:40.000",
            "batch_id": b_cmcd,
            "publication_state": "visible",
            "cmcd": {
                "sid": "s1",
                "cid": "c1",
                "ot": "v",
                "sf": "h",
                "st": "v",
                "bl": "12.0",
                "br": "3500",
                "mtp": "9000",
                "tb": "5000",
                "bs": "true",
            },
            "country": "US",
            "asn": 7922,
        },
        {
            "service_id": "svc-ch",
            "projection_key": str(uuid4()),
            "request_event_id": str(uuid4()),
            "event_timestamp": "2026-11-25 10:05:10.000",
            "batch_id": b_cmcd,
            "publication_state": "visible",
            "cmcd": {
                "sid": "s2",
                "cid": "c2",
                "ot": "a",
                "sf": "d",
                "st": "l",
                "bl": "8.0",
                "br": "2000",
                "mtp": "6000",
                "tb": "4000",
            },
            "country": "DE",
            "asn": 7922,
        },
        {
            "service_id": "svc-ch",
            "projection_key": str(uuid4()),
            "request_event_id": str(uuid4()),
            "event_timestamp": "2026-11-25 10:06:10.000",
            "batch_id": b_cmcd,
            "publication_state": "visible",
            "cmcd": {
                "sid": "s2",
                "cid": "c2",
                "ot": "v",
                "sf": "d",
                "st": "l",
                "bl": "9.0",
                "br": "4000",
                "mtp": "7000",
                "tb": "6000",
            },
            "country": "DE",
            "asn": 7922,
        },
    ]
    shared_req = str(uuid4())
    vitals_rows = [
        {
            "service_id": "svc-ch",
            "event_id": str(uuid4()),
            "event_timestamp": "2026-11-25 10:05:00.000",
            "ingest_timestamp": "2026-11-25 10:05:01.000",
            "source_object_key": "k",
            "source_object_version": "v",
            "line_ordinal": 0,
            "transform_version": "1",
            "batch_id": b_vitals,
            "publication_state": "visible",
            "client_id": "c1",
            "request_event_id": shared_req,
            "req_id": "r1",
            "metric_name": "LCP",
            "metric_value": 2400.0,
            "metric_rating": "good",
            "pathname": "/home",
            "country": "US",
            "browser": "Chrome",
            "os": "macOS",
            "device": "Desktop",
            "city": "SJC",
            "region": "CA",
            "pop": "SJC",
            "tls": "1.3",
            "ttfb": 20.0,
            "resource_url": "",
            "asn": "7922",
            "sample_rate": 0.5,
        },
        {
            "service_id": "svc-ch",
            "event_id": str(uuid4()),
            "event_timestamp": "2026-11-25 10:05:00.000",
            "ingest_timestamp": "2026-11-25 10:05:01.000",
            "source_object_key": "k",
            "source_object_version": "v",
            "line_ordinal": 1,
            "transform_version": "1",
            "batch_id": b_vitals,
            "publication_state": "visible",
            "client_id": "c1",
            "request_event_id": shared_req,
            "req_id": "r1",
            "metric_name": "CLS",
            "metric_value": 0.05,
            "metric_rating": "good",
            "pathname": "/home",
            "country": "US",
            "browser": "Chrome",
            "os": "macOS",
            "device": "Desktop",
            "city": "SJC",
            "region": "CA",
            "pop": "SJC",
            "tls": "1.3",
            "ttfb": 20.0,
            "resource_url": "",
            "asn": "7922",
            "sample_rate": 0.5,
        },
        {
            "service_id": "svc-ch",
            "event_id": str(uuid4()),
            "event_timestamp": "2026-11-25 10:06:00.000",
            "ingest_timestamp": "2026-11-25 10:06:01.000",
            "source_object_key": "k",
            "source_object_version": "v",
            "line_ordinal": 2,
            "transform_version": "1",
            "batch_id": b_vitals,
            "publication_state": "visible",
            "client_id": "c2",
            "request_event_id": None,
            "req_id": "r2",
            "metric_name": "page_load",
            "metric_value": 1600.0,
            "metric_rating": "",
            "pathname": "/cart",
            "country": "US",
            "browser": "Firefox",
            "os": "Linux",
            "device": "Desktop",
            "city": "NYC",
            "region": "NY",
            "pop": "LGA",
            "tls": "1.3",
            "ttfb": 30.0,
            "resource_url": "",
            "asn": "7922",
            "sample_rate": 1.0,
        },
        {
            "service_id": "svc-ch",
            "event_id": str(uuid4()),
            "event_timestamp": "2026-11-25 10:06:00.000",
            "ingest_timestamp": "2026-11-25 10:06:01.000",
            "source_object_key": "k",
            "source_object_version": "v",
            "line_ordinal": 3,
            "transform_version": "1",
            "batch_id": b_vitals,
            "publication_state": "visible",
            "client_id": "c2",
            "request_event_id": None,
            "req_id": "r2",
            "metric_name": "event_click",
            "metric_value": 1.0,
            "metric_rating": "",
            "pathname": "/cart",
            "country": "US",
            "browser": "Firefox",
            "os": "Linux",
            "device": "Desktop",
            "city": "NYC",
            "region": "NY",
            "pop": "LGA",
            "tls": "1.3",
            "ttfb": 30.0,
            "resource_url": "",
            "asn": "7922",
            "sample_rate": 1.0,
        },
        {
            "service_id": "svc-ch",
            "event_id": str(uuid4()),
            "event_timestamp": "2026-11-25 10:07:00.000",
            "ingest_timestamp": "2026-11-25 10:07:01.000",
            "source_object_key": "k",
            "source_object_version": "v",
            "line_ordinal": 4,
            "transform_version": "1",
            "batch_id": b_vitals,
            "publication_state": "visible",
            "client_id": "c1",
            "request_event_id": None,
            "req_id": "r-edge-1",
            "metric_name": "ttlb",
            "metric_value": 420.0,
            "metric_rating": "poor",
            "pathname": "/home",
            "country": "US",
            "browser": "Chrome",
            "os": "macOS",
            "device": "Desktop",
            "city": "SJC",
            "region": "CA",
            "pop": "SJC",
            "tls": "1.3",
            "ttfb": 20.0,
            "resource_url": "/api/slow",
            "asn": "",
            "sample_rate": 1.0,
        },
    ]
    error_rows = [
        {
            "service_id": "svc-ch",
            "event_id": str(uuid4()),
            "event_timestamp": "2026-11-25 10:05:00.000",
            "ingest_timestamp": "2026-11-25 10:05:01.000",
            "source_object_key": "k",
            "source_object_version": "v",
            "line_ordinal": 0,
            "transform_version": "1",
            "batch_id": b_errors,
            "publication_state": "visible",
            "client_id": "c1",
            "request_event_id": shared_req,
            "req_id": "r1",
            "error_message": "Boom",
            "error_file": "a.js",
            "pathname": "/home",
            "country": "US",
            "error_line": 12,
            "error_col": 4,
            "browser": "Chrome",
            "os": "macOS",
            "device": "Desktop",
            "city": "SJC",
            "region": "CA",
            "pop": "SJC",
            "tls": "1.3",
            "ttfb": 20.0,
            "resource_url": "",
            "asn": "7922",
            "sample_rate": 0.25,
        },
        {
            "service_id": "svc-ch",
            "event_id": str(uuid4()),
            "event_timestamp": "2026-11-25 10:08:00.000",
            "ingest_timestamp": "2026-11-25 10:08:01.000",
            "source_object_key": "k",
            "source_object_version": "v",
            "line_ordinal": 1,
            "transform_version": "1",
            "batch_id": b_errors,
            "publication_state": "visible",
            "client_id": "c3",
            "request_event_id": None,
            "req_id": "r3",
            "error_message": "Oops",
            "error_file": "b.js",
            "pathname": "/cart",
            "country": "US",
            "error_line": 3,
            "error_col": 1,
            "browser": "Safari",
            "os": "iOS",
            "device": "Mobile",
            "city": "SJC",
            "region": "CA",
            "pop": "SJC",
            "tls": "1.3",
            "ttfb": 20.0,
            "resource_url": "",
            "asn": "7922",
            "sample_rate": 1.0,
        },
    ]
    req_rows = [
        {
            "service_id": "svc-ch",
            "event_id": str(uuid4()),
            "event_timestamp": "2026-11-25 10:07:00.000",
            "ingest_timestamp": "2026-11-25 10:07:01.000",
            "source_object_key": "k",
            "source_object_version": "v",
            "line_ordinal": 0,
            "transform_version": "1",
            "batch_id": b_req,
            "publication_state": "visible",
            "country": "US",
            "client_ip": "198.51.100.1",
            "url": "/api/slow",
            "custom_fields": {"rid": "r-edge-1", "elapsed": "120000", "asn": "7922"},
            "cmcd": {},
        },
    ]

    seed = (
        pub_schema
        + "\n"
        + cmcd_schema
        + "\n"
        + rum_schema
        + "\n"
        + req_schema
        + "\n;\nINSERT INTO high_scale_batch_publications FORMAT JSONEachRow\n"
        + "\n".join(json.dumps(r) for r in pub_rows)
        + "\n\nINSERT INTO cmcd_projection_facts FORMAT JSONEachRow\n"
        + "\n".join(json.dumps(r) for r in cmcd_rows)
        + "\n\nINSERT INTO rum_vitals_facts FORMAT JSONEachRow\n"
        + "\n".join(json.dumps(r) for r in vitals_rows)
        + "\n\nINSERT INTO rum_error_facts FORMAT JSONEachRow\n"
        + "\n".join(json.dumps(r) for r in error_rows)
        + "\n\nINSERT INTO request_facts FORMAT JSONEachRow\n"
        + "\n".join(json.dumps(r) for r in req_rows)
        + "\n\n"
    )

    def _transport(request: httpx.Request) -> httpx.Response:
        form = BytesParser(policy=default).parsebytes(
            f"Content-Type: {request.headers['content-type']}\r\n\r\n".encode() + request.read()
        )
        fields = {
            part.get_param("name", header="content-disposition"): part.get_payload(decode=True).decode()
            for part in form.iter_parts()
        }
        cmd = [
            "docker",
            "run",
            "--rm",
            "-i",
            "--entrypoint",
            "clickhouse",
            _CH_IMG,
            "local",
            "--multiquery",
            "--format=JSON",
            "--date_time_input_format=best_effort",
            "--output_format_json_quote_64bit_integers=0",
            *[f"--{k}={v}" for k, v in fields.items() if isinstance(k, str) and k.startswith("param_")],
        ]
        res = subprocess.run(cmd, input=f"{seed}\n{fields['query']};", text=True, capture_output=True, timeout=60)
        if res.returncode != 0:
            raise AssertionError(f"ClickHouse error ({res.returncode}): {res.stderr}\nQuery:\n{fields['query']}")
        return httpx.Response(200, content=res.stdout.encode())

    cfg = ClickHouseConfig(host="localhost", port=8123, database="default", user="default", password="")
    ch_client = ClickHouseClient(cfg, transport=httpx.MockTransport(_transport))
    svc = _service(ch_client, "svc-ch")

    # Baseline (opt27=False) vs Optimized (opt27=True) for both CMCD and RUM.
    with patch("backend.high_scale.cmcd._is_opt27_ch", return_value=False):
        cmcd_base = hs_cmcd._cmcd_aggregates_compute(
            svc, CmcdRequest(bucket_seconds=300, top_n=10), "2026-11-25T10:00:00Z", "2026-11-25T10:15:00Z", None, False
        )
    with patch("backend.high_scale.cmcd._is_opt27_ch", return_value=True):
        cmcd_opt = hs_cmcd._cmcd_aggregates_compute(
            svc, CmcdRequest(bucket_seconds=300, top_n=10), "2026-11-25T10:00:00Z", "2026-11-25T10:15:00Z", None, False
        )
    cmcd_base.pop("section_timings", None)
    cmcd_opt.pop("section_timings", None)
    assert cmcd_opt == cmcd_base

    # Empty CMCD window under opt27 returns empty_cmcd_aggregates cleanly.
    with patch("backend.high_scale.cmcd._is_opt27_ch", return_value=True):
        cmcd_empty = hs_cmcd._cmcd_aggregates_compute(
            svc, CmcdRequest(bucket_seconds=300, top_n=10), "2026-11-20T10:00:00Z", "2026-11-20T10:15:00Z", None, False
        )
    assert cmcd_empty["has_data"] is False

    # RUM parity (both unfiltered and filtered).
    for flts in (None, {"country": {"mode": "include", "values": ["US"]}}):
        with patch("backend.high_scale.rum._is_opt27_ch", return_value=False):
            rum_base = hs_rum.rum_analytics(svc, "2026-11-25T10:00:00Z", "2026-11-25T12:00:00Z", filters=flts)
        with patch("backend.high_scale.rum._is_opt27_ch", return_value=True):
            rum_opt = hs_rum.rum_analytics(svc, "2026-11-25T10:00:00Z", "2026-11-25T12:00:00Z", filters=flts)
        assert rum_opt == rum_base
