"""Warm-cache, minute-bucketing, invalidation, and query-consolidation tests for High-Scale CMCD."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from backend.high_scale import cmcd as hs_cmcd
from backend.high_scale.cmcd import cmcd_aggregates
from backend.models.analytics import CmcdRequest
from backend.repositories.utils.response_cache import invalidate_service_cache

pytestmark = pytest.mark.no_postgres

ALL_SECTIONS = {
    "overview",
    "sessions_ts",
    "buffer_health_ts",
    "bitrate_ts",
    "throughput_ts",
    "top_content",
    "rebuffer_by_country",
    "rebuffer_by_asn",
    "object_type_dist",
    "streaming_format_dist",
    "startup_ts",
    "session_duration_dist",
}

_PROBE_ROW: dict[str, Any] = {
    "events": 40,
    "sessions": 4,
    "has_bl": 1,
    "has_br": 1,
    "has_bs": 1,
    "has_mtp": 1,
    "has_ot": 1,
    "has_sf": 1,
    "has_su": 1,
    "has_tb": 1,
    "has_cid": 1,
    "has_country": 1,
    "has_asn": 1,
    "rebuffer_sessions": 1,
    "avg_br_v": 2500.126,
    "avg_br_all": 2400.0,
    "avg_bl_v": 12.0,
    "avg_bl_all": 11.0,
    "med_mtp_v": 8000.0,
    "med_mtp_all": 7500.0,
}

_CANNED: dict[str, list[dict[str, Any]]] = {
    "cmcd:overview_peak": [{"peak_viewers": 4}],
    "cmcd:overview_duration": [{"avg_duration": 125.4}],
    "cmcd:timeseries": [
        {
            "bucket": "2026-09-14 10:05:00",
            "concurrent_sessions": 4,
            "rebuffer_session_pct": 33.333,
            "p50_buffer": 10.0,
            "p95_buffer": 20.0,
            "starvation_rate": 5.0,
            "avg_bitrate": 2400.0,
            "utilization_ratio": 0.51234,
            "p50_throughput": 7000.0,
            "p95_throughput": 9000.0,
            "p99_throughput": 9500.0,
            "startup_ratio": 2.5,
        }
    ],
    "cmcd:sessions": [
        {
            "kind": "new_sessions",
            "k": "2026-09-14 10:05:00",
            "session_count": 2,
            "positive_duration_sum": 0.0,
            "positive_duration_count": 0,
        },
        {
            "kind": "duration",
            "k": "2",
            "session_count": 1,
            "positive_duration_sum": 61.6,
            "positive_duration_count": 1,
        },
        {
            "kind": "duration",
            "k": "5",
            "session_count": 3,
            "positive_duration_sum": 440.0,
            "positive_duration_count": 3,
        },
    ],
    "cmcd:session_breakdowns": [
        {
            "kind": "content",
            "k": "movie-1",
            "session_count": 3,
            "rebuffer_rate": 33.33,
            "avg_br": 3000.0,
            "avg_bl": 9.5,
            "median_mtp": None,
        },
        {
            "kind": "country",
            "k": "US",
            "session_count": 3,
            "rebuffer_rate": 10.0,
            "avg_br": None,
            "avg_bl": None,
            "median_mtp": 7500.0,
        },
        {
            "kind": "asn",
            "k": "7922",
            "session_count": 2,
            "rebuffer_rate": 50.0,
            "avg_br": 2000.0,
            "avg_bl": 8.0,
            "median_mtp": None,
        },
    ],
    "cmcd:breakdowns": [
        {
            "kind": "ot",
            "k": "v",
            "session_count": 30,
            "rebuffer_rate": None,
            "avg_br": None,
            "avg_bl": None,
            "median_mtp": None,
        },
        {
            "kind": "ot",
            "k": "a",
            "session_count": 10,
            "rebuffer_rate": None,
            "avg_br": None,
            "avg_bl": None,
            "median_mtp": None,
        },
        {
            "kind": "sf",
            "k": "h",
            "session_count": 4,
            "rebuffer_rate": None,
            "avg_br": None,
            "avg_bl": None,
            "median_mtp": None,
        },
    ],
    "cmcd:new_sessions": [{"bucket": "2026-09-14 10:05:00", "new_sessions": 2}],
    "cmcd:session_duration_dist": [
        {"sort_order": 2, "session_count": 1, "positive_duration_sum": 61.6, "positive_duration_count": 1},
        {"sort_order": 5, "session_count": 3, "positive_duration_sum": 440.0, "positive_duration_count": 3},
    ],
}


class _FakeCmcdClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        self.calls.append((sql, params or {}))
        if "cmcd:probe" in sql:
            return [_PROBE_ROW]
        for marker, rows in _CANNED.items():
            if marker in sql:
                return rows
        raise AssertionError(f"Unexpected SQL: {sql}")


@pytest.fixture(autouse=True)
def _stub_asn_names(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(hs_cmcd, "_asn_names", lambda _sid, _asns: {7922: "Comcast"})


def test_cmcd_warm_reload_hits_cache_when_window_shifts_within_same_minute() -> None:
    client = _FakeCmcdClient()
    svc_a = SimpleNamespace(service_id="svc-a", client=client)
    svc_b = SimpleNamespace(service_id="svc-b", client=client)
    req = CmcdRequest(bucket_seconds=300, top_n=10)

    first = cmcd_aggregates(svc_a, req, "2026-09-14T10:00:05Z", "2026-09-14T10:20:05Z", ALL_SECTIONS, False)
    first_calls = len(client.calls)
    assert first["_response_cache"]["state"] == "miss"
    assert first["_response_cache"]["ttl_seconds"] == 60.0

    # Warm reload 37s later within the same minute bucket must hit cache with 0 ClickHouse queries
    warm = cmcd_aggregates(svc_a, req, "2026-09-14T10:00:42Z", "2026-09-14T10:20:42Z", ALL_SECTIONS, False)
    assert warm["_response_cache"]["state"] == "hit"
    assert warm["is_cached"] is True
    assert len(client.calls) == first_calls

    # Different minute window must miss
    next_min = cmcd_aggregates(svc_a, req, "2026-09-14T10:01:05Z", "2026-09-14T10:21:05Z", ALL_SECTIONS, False)
    assert next_min["_response_cache"]["state"] == "miss"
    assert len(client.calls) == first_calls * 2

    # Different service must miss (service isolation)
    other_svc = cmcd_aggregates(svc_b, req, "2026-09-14T10:00:05Z", "2026-09-14T10:20:05Z", ALL_SECTIONS, False)
    assert other_svc["_response_cache"]["state"] == "miss"
    assert len(client.calls) == first_calls * 3

    # Invalidating svc-a must force recomputation for svc-a while preserving svc-b
    invalidate_service_cache("svc-a")
    after_inv = cmcd_aggregates(svc_a, req, "2026-09-14T10:00:42Z", "2026-09-14T10:20:42Z", ALL_SECTIONS, False)
    assert after_inv["_response_cache"]["state"] == "miss"
    assert len(client.calls) == first_calls * 4
    still_cached = cmcd_aggregates(svc_b, req, "2026-09-14T10:00:42Z", "2026-09-14T10:20:42Z", ALL_SECTIONS, False)
    assert still_cached["_response_cache"]["state"] == "hit"
    assert len(client.calls) == first_calls * 4


def test_cmcd_consolidates_overview_into_probe_timeseries_and_session_duration() -> None:
    client = _FakeCmcdClient()
    svc = SimpleNamespace(service_id="svc-consolidate", client=client)
    req = CmcdRequest(bucket_seconds=300, top_n=10)

    res = cmcd_aggregates(svc, req, "2026-09-14T10:00:00Z", "2026-09-14T10:20:00Z", ALL_SECTIONS, False)
    markers = [sql.split("*/")[0].split("/*")[1].strip() for sql, _ in client.calls]
    assert len(markers) == 5
    assert set(markers) == {
        "cmcd:probe",
        "cmcd:timeseries",
        "cmcd:sessions",
        "cmcd:session_breakdowns",
        "cmcd:breakdowns",
    }
    assert res["overview"] == {
        "active_sessions": 4,
        "rebuffer_rate": 25.0,
        "avg_bitrate": 2500.13,
        "avg_buffer_length": 12.0,
        "median_throughput": 8000.0,
        "peak_viewers": 4,
        "avg_session_duration": 125,
    }
    bucket_1005 = next(p for p in res["sessions_ts"] if p["bucket"].startswith("2026-09-14 10:05:00"))
    assert bucket_1005["new_sessions"] == 2
    assert len(res["session_duration_dist"]) == 2
    assert res["top_content"][0]["content_id"] == "movie-1"
    assert res["rebuffer_by_country"][0]["country"] == "US"
    assert res["rebuffer_by_asn"][0]["asn"] == 7922
    assert res["rebuffer_by_asn"][0]["label"] == "Comcast (7922)"
    assert len(res["object_type_dist"]) == 2
    assert res["streaming_format_dist"][0]["streaming_format"] == "h"


def test_cmcd_overview_only_skips_redundant_overview_scan() -> None:
    client = _FakeCmcdClient()
    svc = SimpleNamespace(service_id="svc-overview-only", client=client)
    req = CmcdRequest(bucket_seconds=300, top_n=10)

    res = cmcd_aggregates(svc, req, "2026-09-14T10:00:00Z", "2026-09-14T10:20:00Z", {"overview"}, False)
    markers = [sql.split("*/")[0].split("/*")[1].strip() for sql, _ in client.calls]
    assert len(markers) == 3
    assert "cmcd:probe" in markers
    assert "cmcd:overview_peak" in markers
    assert "cmcd:overview_duration" in markers
    assert "cmcd:overview" not in markers
    assert res["overview"]["active_sessions"] == 4
    assert res["overview"]["rebuffer_rate"] == 25.0
    assert res["overview"]["peak_viewers"] == 4
    assert res["overview"]["avg_session_duration"] == 125


def test_cmcd_breakdowns_consolidate_flat_dimensions_with_grouping_sets_and_no_star() -> None:
    client = _FakeCmcdClient()
    svc = SimpleNamespace(service_id="svc-flat-grouping", client=client)
    req = CmcdRequest(bucket_seconds=300, top_n=10)

    cmcd_aggregates(svc, req, "2026-09-14T10:00:00Z", "2026-09-14T10:20:00Z", ALL_SECTIONS, False)
    sess_breakdown_sql = next(sql for sql, _ in client.calls if "/* cmcd:session_breakdowns */" in sql)
    assert "GROUP BY GROUPING SETS ((cid, sid_h), (country, sid_h), (asn, sid_h))" in sess_breakdown_sql
    assert "WITH events AS (" in sess_breakdown_sql
    assert sess_breakdown_sql.count("FROM cmcd_projection_facts") == 1
    breakdown_sql = next(sql for sql, _ in client.calls if "/* cmcd:breakdowns */" in sql)
    assert "SELECT * FROM (" not in breakdown_sql
    assert "GROUP BY GROUPING SETS ((ot), (sf))" in breakdown_sql
    assert "WITH events AS (" in breakdown_sql
    assert breakdown_sql.count("FROM cmcd_projection_facts") == 1
    probe_sql = next(sql for sql, _ in client.calls if "/* cmcd:probe */" in sql)
    assert (
        "PREWHERE service_id={service_id:String} AND publication_state='visible' "
        "AND event_timestamp >= {start:DateTime64(3)} AND event_timestamp < {end:DateTime64(3)}"
    ) in probe_sql
    assert "WHERE batch_id IN (" in probe_sql and "cmcd['sid'] != ''" in probe_sql


def test_cmcd_probe_runs_in_parallel_wave_with_queries_and_preserves_ot_and_bs_gating() -> None:
    import threading

    barrier = threading.Barrier(5, timeout=2.0)

    class _ParallelProbeClient(_FakeCmcdClient):
        def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
            if any(
                m in sql
                for m in (
                    "cmcd:probe",
                    "cmcd:timeseries",
                    "cmcd:sessions",
                    "cmcd:session_breakdowns",
                    "cmcd:breakdowns",
                )
            ):
                barrier.wait()
            if "cmcd:probe" in sql:
                return [{**_PROBE_ROW, "has_ot": 0, "has_bs": 0, "has_bl": 0}]
            if "cmcd:timeseries" in sql:
                return [
                    {
                        "bucket": "2026-09-14 10:05:00",
                        "concurrent_sessions": 4,
                        "rebuffer_session_pct": 33.333,
                        "p50_buffer": 10.0,
                        "p50_buffer_all": 99.0,
                        "p95_buffer": 20.0,
                        "starvation_rate": 5.0,
                        "avg_bitrate": 2400.0,
                        "avg_bitrate_all": 1800.0,
                        "utilization_ratio": 0.5,
                        "utilization_ratio_all": 0.75,
                        "p50_throughput": 7000.0,
                        "p50_throughput_all": 6000.0,
                        "p95_throughput": 9000.0,
                        "p95_throughput_all": 8500.0,
                        "p99_throughput": 9500.0,
                        "p99_throughput_all": 9100.0,
                        "startup_ratio": 2.5,
                    }
                ]
            return super().execute(sql, params)

    client = _ParallelProbeClient()
    svc = SimpleNamespace(service_id="svc-par-probe", client=client)
    req = CmcdRequest(bucket_seconds=300, top_n=10)
    res = cmcd_aggregates(svc, req, "2026-09-14T10:00:00Z", "2026-09-14T10:20:00Z", ALL_SECTIONS, False)
    assert not barrier.broken
    assert res["overview"]["avg_bitrate"] == 2400.0
    assert res["overview"]["rebuffer_rate"] is None
    assert "rebuffer_by_country" not in res
    assert "rebuffer_by_asn" not in res
    assert res["sessions_ts"][1]["rebuffer_session_pct"] is None
    assert res["bitrate_ts"][1]["avg_bitrate"] == 1800.0
    assert res["throughput_ts"][1]["p50"] == 6000.0
