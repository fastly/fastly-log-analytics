"""Unit tests for Round 20 High-Scale ClickHouse Insights & CMCD query optimizations."""

from __future__ import annotations

import re
import threading
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from backend.core.catalog import LOG_FIELD_CATALOG
from backend.high_scale import cmcd as hs_cmcd
from backend.high_scale.insights import evaluator
from backend.high_scale.insights.batches import (
    _build_asn,
    _build_geo,
    _build_misc,
    _build_url_cache,
)
from backend.high_scale.insights.rollup import ROLLUP_SHAPES, SAMPLED_ROLLUP_DETECTIONS, rollup_base_sql
from backend.high_scale.insights.sql_queries import DETECTIONS, _base_sql, impossible_distance
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
    monkeypatch.setattr(
        "backend.utils.pop_utils.get_pop_lat_lon_map", lambda: {"SJC": (37.36, -121.93), "IAD": (38.94, -77.46)}
    )
    monkeypatch.setattr(hs_cmcd, "_asn_names", lambda _sid, _asns: {7922: "Comcast"})
    evaluator._INSIGHTS_CACHE.clear()
    evaluator._INSIGHTS_TELEMETRY_CACHE.clear()
    evaluator._INSIGHTS_PARTIAL_CACHE.clear()
    evaluator._INSIGHTS_HISTORY_CACHE.clear()
    evaluator._INSIGHTS_LAST_RESULT.clear()
    evaluator._INSIGHTS_REFRESH_FAILURES.clear()
    hs_cmcd._memo.cache.clear()


def test_multiplex_groups_disabled_and_queries_run_individually() -> None:
    assert evaluator._MULTIPLEX_GROUPS == ()

    def _hours(start: datetime, end: datetime) -> list[int]:
        hour = int(start.timestamp()) // 3600 * 3600
        return list(range(hour, int(end.timestamp()) + 1, 3600))

    def _handler(sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        if "/* insight:history */" in sql:
            hours = _hours(params["baseline_start"], params["window_end"])
            return [
                {
                    "earliest": datetime.now(UTC) - timedelta(days=10),
                    "window_total": 5000,
                    "hours": hours,
                    "hour_rows": [10] * len(hours),
                    "hour_checksums": [str(h * 7) for h in hours],
                }
            ]
        if "/* insight:fields */" in sql:
            return [{"fields": ALL_FIELDS}]
        if "/* insight:rollup_coverage */" in sql:
            hours = _hours(params["coverage_start"], params["coverage_end"] - timedelta(seconds=1))
            return [
                {"shape": shape, "hour": str(h), "row_count": "10", "batch_checksum": str(h * 7)}
                for shape in ROLLUP_SHAPES
                for h in hours
            ]
        return []

    client = _RecordingCHClient(_handler)
    service = SimpleNamespace(service_id="svc-opt20-nomux", client=client)
    req = InsightsRequest(window_size_hrs=24, baseline_hours=168)
    resp = evaluator.insights_endpoint(service, req, None, None)  # type: ignore[arg-type]

    sqls = [sql for sql, _, _ in client.calls]
    assert not any("/* insight:group_" in s for s in sqls)
    markers = {re.search(r"/\* insight:(\w+) \*/", s).group(1) for s in sqls}  # type: ignore[union-attr]
    assert {
        "batch_misc",
        "batch_geo",
        "batch_asn",
        "batch_url_cache",
        "batch_window_url",
        "batch_beaconing",
        "error_spikes",
        "new_probe_urls",
        "ua_monoculture",
        "referer_monoculture",
        "waf_signal_spikes",
    } <= markers
    assert len(resp.insights) > 0


def test_uncovered_baseline_sampling_for_misc_and_url_cache_only() -> None:
    present = {
        "url",
        "cache",
        "transport",
        "ost",
        "p_type",
        "method",
        "c_type",
        "c_speed",
        "country",
        "city",
        "region",
        "status",
        "asn",
    }
    span = (datetime(2026, 11, 20, 0, tzinfo=UTC), datetime(2026, 11, 25, 0, tzinfo=UTC))
    sample_pred = "intDiv(toUnixTimestamp(event_timestamp), 300) % 8 = 0"
    scaled_n = "if(event_timestamp < {window_start:DateTime64(3)}, toUInt64(8), toUInt64(1)) AS n"

    for builder, active in (
        (
            _build_misc,
            {
                "cache_hit_cliff",
                "http3_fallback",
                "origin_error_rate",
                "proxy_surge",
                "method_drift",
                "connection_type_mix",
            },
        ),
        (_build_url_cache, {"cache_collapse", "cacheability_regression"}),
    ):
        mixed_sql, mixed_src = builder(
            active, present=present, rollup_span=span, filter_clause="", baseline_hours=168.0
        )
        assert mixed_src == "mixed"
        raw_part, rollup_part = mixed_sql.split("UNION ALL", 1)
        assert sample_pred in raw_part
        assert scaled_n in raw_part
        assert sample_pred not in rollup_part
        assert "request_count AS n" in rollup_part

        raw_sql, raw_src = builder(active, present=present, rollup_span=None, filter_clause="", baseline_hours=168.0)
        assert raw_src == "fact"
        assert sample_pred in raw_sql
        assert scaled_n in raw_sql

        short_sql, _ = builder(active, present=present, rollup_span=span, filter_clause="", baseline_hours=12.0)
        assert "intDiv(toUnixTimestamp(event_timestamp), 300)" not in short_sql
        assert "toUInt64(1) AS n" in short_sql

    # geo and asn must NOT sample baseline (they include zero-baseline new_* detectors)
    geo_sql, _ = _build_geo({"new_country_traffic", "city_surges"}, present=present, rollup_span=span, filter_clause="")
    asn_sql, _ = _build_asn(
        {"asn_concentration", "new_asn_traffic"}, present=present, rollup_span=span, filter_clause=""
    )
    for untouched_sql in (geo_sql, asn_sql):
        assert "intDiv(toUnixTimestamp(event_timestamp), 300)" not in untouched_sql
        assert "toUInt64(1) AS n" in untouched_sql


def test_rollup_uncovered_baseline_sampling_and_new_probe_urls_unchanged() -> None:
    assert SAMPLED_ROLLUP_DETECTIONS == frozenset(
        {"error_spikes", "ua_monoculture", "referer_monoculture", "waf_signal_spikes"}
    )
    sample_pred = "intDiv(toUnixTimestamp(event_timestamp), 300) % 8 = 0"
    scaled_n = "if(event_timestamp < {window_start:DateTime64(3)}, toUInt64(8), toUInt64(1)) AS n"

    for insight_id, shape in (
        ("error_spikes", "url_status"),
        ("ua_monoculture", "ua"),
        ("referer_monoculture", "referer"),
        ("waf_signal_spikes", "waf"),
    ):
        sql = rollup_base_sql(DETECTIONS[insight_id], insight_id, shape, baseline_hours=168.0)
        raw_part, rollup_part = sql.split("UNION ALL", 1)
        assert sample_pred in raw_part
        assert scaled_n in raw_part
        assert sample_pred not in rollup_part
        assert "request_count AS n" in rollup_part
        assert not re.search(r"\bcount(If)?\(", sql)

        short_sql = rollup_base_sql(DETECTIONS[insight_id], insight_id, shape, baseline_hours=12.0)
        assert "intDiv(toUnixTimestamp(event_timestamp), 300)" not in short_sql
        assert "toUInt64(1) AS n" in short_sql

    probe_sql = rollup_base_sql(DETECTIONS["new_probe_urls"], "new_probe_urls", "url_status", baseline_hours=168.0)
    assert "intDiv(toUnixTimestamp(event_timestamp), 300)" not in probe_sql
    assert "toUInt64(1) AS n" in probe_sql


def test_crawler_disparity_single_scan_grouping_sets_and_sampled_guard() -> None:
    sql = _base_sql(DETECTIONS["crawler_disparity"], "crawler_disparity", baseline_hours=168.0)
    assert sql.count("FROM base") == 1
    assert "GROUP BY GROUPING SETS ((), (bot_stem), (ref_stem, is_w, is_b))" in sql
    assert "ARRAY JOIN bots AS b" in sql
    assert "intDiv(toUnixTimestamp(event_timestamp), 300) % 4 = 0" in sql
    assert "cnt * 4 AS cnt" in sql
    assert "HAVING sum(cnt) >= 50" in sql
    assert "intDiv(toUnixTimestamp(event_timestamp), 300) % 8 = 0" in sql

    def _handler(q: str, _params: dict[str, Any]) -> list[dict[str, Any]]:
        if "/* insight:history */" in q:
            return [{"earliest": datetime.now(UTC) - timedelta(days=10), "window_total": 1000, "hours": []}]
        if "/* insight:fields */" in q:
            return [{"fields": ALL_FIELDS}]
        if "/* insight:crawler_disparity */" in q:
            return [
                {
                    "label": "gptbot",
                    "w_crawls": 5000,
                    "w_referrals": 2,
                    "w_bytes": 600_000_000,
                    "crawls_per_referral": 2500.0,
                    "baseline_crawls_per_referral": 100.0,
                    "bytes_per_referral": 300_000_000.0,
                    "baseline_bytes_per_referral": 10_000_000.0,
                    "bandwidth_share_pct": 12.5,
                }
            ]
        return []

    client = _RecordingCHClient(_handler)
    service = SimpleNamespace(service_id="svc-crawler", client=client)
    resp = evaluator.insights_endpoint(service, InsightsRequest(window_size_hrs=24, baseline_hours=168), None, None)  # type: ignore[arg-type]
    cards = {c.id: c for c in resp.insights}
    assert cards["crawler_disparity"].severity == "critical"
    assert cards["crawler_disparity"].items[0].meta["crawls"] == 5000


def test_impossible_distance_uses_join_free_transform_and_early_filter() -> None:
    det = impossible_distance({"SJC": (37.36, -121.93), "IAD": (38.94, -77.46)}, "ja4", edge_only=True)
    sql = _base_sql(det, "impossible_distance", use_cf=True)
    assert "arrayJoin" not in sql
    assert "INNER JOIN" not in sql
    assert "pop_coords" not in sql
    assert "transform(pop, ['IAD', 'SJC'], [38.94, 37.36], 999.0) AS pop_lat" in sql
    assert "transform(pop, ['IAD', 'SJC'], [-77.46, -121.93], 999.0) AS pop_lon" in sql
    assert "WHERE transform(pop, ['IAD', 'SJC'], [38.94, 37.36], 999.0) <= 90.0" in sql
    assert "WHERE distance_km > max_km GROUP BY fp HAVING hits >= 2" in sql
    assert "custom_fields[" not in sql

    def _handler(q: str, _params: dict[str, Any]) -> list[dict[str, Any]]:
        if "/* insight:history */" in q:
            return [{"earliest": datetime.now(UTC) - timedelta(days=10), "window_total": 1000, "hours": []}]
        if "/* insight:fields */" in q:
            return [{"fields": ALL_FIELDS}]
        if "/* insight:impossible_distance */" in q:
            return [
                {
                    "fp": "t13d_spoof",
                    "hits": 4,
                    "worst_excess_km": 6000.0,
                    "max_dist_km": 7500.0,
                    "min_allowed_km": 1500.0,
                    "pop": "SJC",
                    "sample_ip": "198.51.100.42",
                    "client_lat": 51.5,
                    "client_lon": -0.1,
                    "pop_lat": 37.36,
                    "pop_lon": -121.93,
                    "tcp_rtt": 7500.0,
                    "country": "GB",
                    "city": "London",
                }
            ]
        return []

    client = _RecordingCHClient(_handler)
    service = SimpleNamespace(service_id="svc-imp", client=client)
    resp = evaluator.insights_endpoint(
        service,
        InsightsRequest(window_size_hrs=24, baseline_hours=168),
        None,
        None,
        mask_ips=True,  # type: ignore[arg-type]
    )
    cards = {c.id: c for c in resp.insights}
    assert cards["impossible_distance"].severity == "critical"
    assert cards["impossible_distance"].items[0].meta["excess_km"] == 6000.0
    assert cards["impossible_distance"].items[0].meta["sample_ip"] != "198.51.100.42"


def test_cmcd_splits_session_breakdowns_and_flat_breakdowns_in_parallel() -> None:
    barrier = threading.Barrier(2, timeout=2.0)

    def _handler(sql: str, _params: dict[str, Any]) -> list[dict[str, Any]]:
        if "cmcd:probe" in sql:
            return [
                {
                    "events": 50,
                    "sessions": 5,
                    "has_bl": 1,
                    "has_br": 1,
                    "has_bs": 1,
                    "has_mtp": 1,
                    "has_ot": 1,
                    "has_sf": 1,
                    "has_st": 1,
                    "has_su": 1,
                    "has_tb": 1,
                    "has_cid": 1,
                    "has_country": 1,
                    "has_asn": 1,
                    "rebuffer_sessions": 1,
                    "avg_br_v": 3000.0,
                    "avg_br_all": 2800.0,
                    "avg_bl_v": 10.0,
                    "avg_bl_all": 9.5,
                    "med_mtp_v": 8000.0,
                    "med_mtp_all": 7500.0,
                }
            ]
        if "cmcd:session_breakdowns" in sql:
            barrier.wait()
            return [
                {
                    "kind": "content",
                    "k": "c1",
                    "session_count": 4,
                    "rebuffer_rate": 25.0,
                    "avg_br": 3200.0,
                    "avg_br_all": 3000.0,
                    "avg_bl": 11.0,
                    "avg_bl_all": 10.0,
                    "median_mtp": None,
                    "median_mtp_all": None,
                },
                {
                    "kind": "country",
                    "k": "US",
                    "session_count": 4,
                    "rebuffer_rate": 25.0,
                    "avg_br": None,
                    "avg_br_all": None,
                    "avg_bl": None,
                    "avg_bl_all": None,
                    "median_mtp": 8100.0,
                    "median_mtp_all": 7900.0,
                },
                {
                    "kind": "asn",
                    "k": "7922",
                    "session_count": 3,
                    "rebuffer_rate": 33.33,
                    "avg_br": 2900.0,
                    "avg_br_all": 2700.0,
                    "avg_bl": 9.0,
                    "avg_bl_all": 8.5,
                    "median_mtp": None,
                    "median_mtp_all": None,
                },
            ]
        if "cmcd:breakdowns" in sql:
            barrier.wait()
            return [
                {
                    "kind": "ot",
                    "k": "v",
                    "session_count": 40,
                    "request_count": 40,
                    "rebuffer_rate": None,
                    "avg_br": None,
                },
                {
                    "kind": "sf",
                    "k": "h",
                    "session_count": 5,
                    "request_count": 50,
                    "rebuffer_rate": None,
                    "avg_br": None,
                },
                {
                    "kind": "ladder",
                    "k": "3",
                    "session_count": 3,
                    "request_count": 20,
                    "rebuffer_rate": 10.0,
                    "avg_br": 3100.0,
                },
                {
                    "kind": "st",
                    "k": "v",
                    "session_count": 5,
                    "request_count": 50,
                    "rebuffer_rate": None,
                    "avg_br": None,
                },
            ]
        return []

    client = _RecordingCHClient(_handler)
    svc = SimpleNamespace(service_id="svc-cmcd-split", client=client)
    req = CmcdRequest(bucket_seconds=300, top_n=10)
    sections = {
        "top_content",
        "rebuffer_by_country",
        "rebuffer_by_asn",
        "object_type_dist",
        "streaming_format_dist",
        "bitrate_ladder",
        "stream_type_dist",
    }
    res = hs_cmcd.cmcd_aggregates(svc, req, "2026-11-25T10:00:00Z", "2026-11-25T11:00:00Z", sections, False)  # type: ignore[arg-type]
    assert not barrier.broken

    sess_sql = next(s for s, _, _ in client.calls if "/* cmcd:session_breakdowns */" in s)
    flat_sql = next(s for s, _, _ in client.calls if "/* cmcd:breakdowns */" in s)
    assert sess_sql.count("FROM cmcd_projection_facts") == 1
    assert "GROUP BY GROUPING SETS ((cid, sid), (country, sid), (asn, sid))" in sess_sql
    assert "row_number() OVER (PARTITION BY kind ORDER BY session_count DESC, k ASC)" in sess_sql
    assert "WHERE rn <= {top_n:UInt32}" in sess_sql
    assert flat_sql.count("FROM cmcd_projection_facts") == 1
    assert "GROUP BY GROUPING SETS ((ot), (sf), (ladder_k), (st))" in flat_sql
    assert "SELECT 'content'" not in flat_sql

    assert res["top_content"] == [
        {
            "content_id": "c1",
            "session_count": 4,
            "rebuffer_rate": 25.0,
            "avg_bitrate": 3200.0,
            "avg_buffer_length": 11.0,
        }
    ]
    assert res["rebuffer_by_country"] == [
        {"country": "US", "rebuffer_rate": 25.0, "session_count": 4, "median_throughput": 8100.0}
    ]
    assert res["rebuffer_by_asn"] == [
        {
            "asn": 7922,
            "label": "Comcast (7922)",
            "rebuffer_rate": 33.33,
            "session_count": 3,
            "avg_bitrate": 2900.0,
            "avg_buffer_length": 9.0,
        }
    ]
    assert res["object_type_dist"] == [{"object_type": "v", "request_count": 40}]
    assert res["streaming_format_dist"] == [{"streaming_format": "h", "session_count": 5}]
    assert res["bitrate_ladder"] == [
        {
            "tier": "2.5–5 Mbps (1080p)",
            "request_count": 20,
            "session_count": 3,
            "avg_bitrate": 3100.0,
            "rebuffer_rate": 10.0,
        }
    ]
    assert res["stream_type_dist"] == [{"stream_type": "v", "session_count": 5, "request_count": 50}]
    timing_sections = {t["section"] for t in res["section_timings"]}
    assert {"cmcd.probe", "cmcd.session_breakdowns", "cmcd.breakdowns", "cmcd.asn_labels"} == timing_sections


def test_cmcd_single_session_or_partial_session_breakdowns() -> None:
    client = _RecordingCHClient(
        lambda sql, _p: (
            [
                {
                    "events": 10,
                    "sessions": 2,
                    "has_bl": 1,
                    "has_br": 1,
                    "has_bs": 1,
                    "has_mtp": 1,
                    "has_ot": 1,
                    "has_sf": 1,
                    "has_st": 1,
                    "has_su": 1,
                    "has_tb": 1,
                    "has_cid": 1,
                    "has_country": 1,
                    "has_asn": 1,
                }
            ]
            if "cmcd:probe" in sql
            else []
        )
    )
    svc = SimpleNamespace(service_id="svc-cmcd-partial", client=client)
    req = CmcdRequest(bucket_seconds=300, top_n=10)

    hs_cmcd.cmcd_aggregates(svc, req, "2026-11-25T10:00:00Z", "2026-11-25T11:00:00Z", {"top_content"}, False)  # type: ignore[arg-type]
    markers = {s.split("*/")[0].split("/*")[1].strip() for s, _, _ in client.calls}
    assert markers == {"cmcd:probe", "cmcd:session_breakdowns"}
    single_sql = next(s for s, _, _ in client.calls if "cmcd:session_breakdowns" in s)
    assert "GROUP BY cid ORDER BY session_count DESC" in single_sql
    assert "GROUPING SETS" not in single_sql

    hs_cmcd._memo.cache.clear()
    client.calls.clear()
    hs_cmcd.cmcd_aggregates(
        svc, req, "2026-11-25T10:00:00Z", "2026-11-25T11:00:00Z", {"top_content", "rebuffer_by_country"}, False
    )  # type: ignore[arg-type]
    two_sql = next(s for s, _, _ in client.calls if "cmcd:session_breakdowns" in s)
    assert "GROUP BY GROUPING SETS ((cid, sid), (country, sid))" in two_sql
    assert "(asn, sid)" not in two_sql
    assert "asn IS NOT NULL" not in two_sql
