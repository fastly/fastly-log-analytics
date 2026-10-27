from __future__ import annotations

import json
import re
import sqlite3
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from threading import Event
from time import monotonic, sleep
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from backend.core.high_scale_sections import current_clickhouse_scope
from backend.core.query_attribution import Attribution, current_attribution
from backend.high_scale._common import KeysetCursor, request_filter_clauses
from backend.high_scale.aggregate_query import (
    AggregateRequest,
    query_clickhouse_aggregate,
    query_clickhouse_aggregates_batch,
)
from backend.high_scale.archive_models import ServingWatermark
from backend.high_scale.dashboard import aggregates, bundle, header_metrics
from backend.high_scale.historical_jobs import HistoricalJob, HistoricalJobManager, HistoricalJobState
from backend.high_scale.insight_queries import COLUMNS, DETECTIONS, botnet_grouping
from backend.high_scale.insights import cache_collapse_detail_endpoint, insights_endpoint
from backend.high_scale.network import (
    get_pop_health,
    get_transport_metrics,
    network_bundle,
    network_health,
    network_quality,
)
from backend.high_scale.origin import aggregates as origin_aggregates
from backend.high_scale.origin_projection import build_origin_projection_rows
from backend.high_scale.performance_projection import SCATTER_SAMPLE_SIZE, build_performance_projection_rows
from backend.high_scale.query_router import (
    QueryIntent,
    QueryPlan,
    QueryRequest,
    QueryRouter,
    QueryRouterConfig,
    QueryTier,
)
from backend.high_scale.recovery import RecoveryReport
from backend.high_scale.registry_bootstrap import HighScaleService
from backend.high_scale.sessions import sessions_detail, sessions_endpoint
from backend.models.dashboard import AggregatesRequest, SessionDetailRequest, SessionsRequest, SessionsResponse
from backend.models.insights import CacheCollapseDetailRequest, InsightsRequest
from backend.models.network import NetworkBundleRequest, NetworkHealthRequest, NetworkQualityRequest
from backend.models.performance import PerformanceAggregatesResponse
from backend.models.security import SecurityTopBotsResponse

from backend.high_scale import dashboard, network, origin, security
from backend.high_scale import insights as hs_insights
from backend.high_scale import performance as hs_performance
from backend.high_scale import security as hs_security
from backend.high_scale import sessions as hs_sessions
from backend.high_scale.query_service import query_cmcd_facts, query_request_facts, query_rum_facts
from backend.models.common import FilterSpec
from backend.repositories.insights import registry
from backend.utils.telemetry import get_page_load_id, set_page_load_id
from tests.high_scale import test_cmcd as cmcd_cases
from tests.high_scale import test_security as security_cases


class _Client:
    def __init__(self, rows):
        self.rows, self.sql, self.params = rows, "", None

    def execute(self, sql, params=None):
        self.sql, self.params = sql, params
        return self.rows


def _watermark(domain: str = "request") -> ServingWatermark:
    return ServingWatermark(
        "svc", domain, 1, datetime(2026, 9, 11, 20, 0, tzinfo=UTC), datetime(2026, 9, 11, 20, 5, tzinfo=UTC),
        "cursor", "event-2", "event-2", True,
    )  # fmt: skip


def _row(second: int, event_id: str) -> dict:
    return {"event_id": event_id, "timestamp": datetime(2026, 9, 11, 20, 0, second, tzinfo=UTC)}


def test_request_rum_and_cmcd_facts_queries() -> None:
    t0, t1 = datetime(2026, 9, 11, 20, 0, tzinfo=UTC), datetime(2026, 9, 11, 21, 0, tzinfo=UTC)
    client = _Client([_row(1, "event-1"), _row(2, "event-2")])
    page = query_request_facts(
        client, service_id="svc", start=t0, end=t1, cursor_secret=b"secret", watermark=_watermark(),
        limit=1, now=datetime(2026, 9, 11, 20, 6, tzinfo=UTC),
    )  # fmt: skip
    assert "publication_state = 'visible'" in client.sql and "high_scale_batch_publications FINAL" in client.sql
    assert page.rows == (client.rows[0],)
    assert page.next_cursor and KeysetCursor.decode(page.next_cursor, b"secret").event_id == "event-1"
    assert page.metadata.exact is True and page.metadata.freshness_lag_seconds == 60

    bad_cursor = KeysetCursor(t0, "event-1", "svc", "rum_vitals").encode(b"secret")
    with pytest.raises(ValueError, match="service and domain"):
        query_request_facts(
            _Client([]), service_id="svc", start=t0, end=t1, cursor_secret=b"secret",
            watermark=_watermark(), cursor=bad_cursor,
        )  # fmt: skip

    for end_hour, limit, match in ((20, 100, "query end"), (21, 501, "between")):
        with pytest.raises(ValueError, match=match):
            query_request_facts(
                _Client([]), service_id="svc", start=t0, end=datetime(2026, 9, 11, end_hour, 0, tzinfo=UTC),
                cursor_secret=b"secret", watermark=_watermark(), limit=limit,
            )  # fmt: skip

    rum_client = _Client([_row(1, "event-1")])
    query_rum_facts(
        rum_client, service_id="svc", domain="rum_vitals", start=t0, end=t1,
        cursor_secret=b"secret", watermark=_watermark("rum_vitals"),
    )  # fmt: skip
    assert "FROM rum_vitals_facts" in rum_client.sql

    cmcd_client = _Client([_row(1, "projection-1")])
    query_cmcd_facts(
        cmcd_client, service_id="svc", start=t0, end=t1, cursor_secret=b"secret", watermark=_watermark("cmcd")
    )
    assert "FROM cmcd_projection_facts" in cmcd_client.sql and "projection_key AS event_id" in cmcd_client.sql


NOW = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)


def request(*, hours_old: int, hours: int = 1, intent: QueryIntent = QueryIntent.RAW) -> QueryRequest:
    end = NOW - timedelta(hours=hours_old)
    return QueryRequest("svc", "request", end - timedelta(hours=hours), end, intent)


@pytest.mark.parametrize(
    ("hours_old", "hours", "intent", "tier", "sources", "queued", "bounded", "max_rows"),
    [
        (2, 1, QueryIntent.TRIAGE, QueryTier.TRIAGE_AGGREGATE, ("aggregates",), False, False, None),
        (2, 1, QueryIntent.RAW, QueryTier.RECENT_FACTS, ("clickhouse_facts",), False, False, None),
        (24 * 10, 1, QueryIntent.RAW, QueryTier.WARM_FACTS_ARCHIVE, ("clickhouse_facts", "fos_archive"), False, False, None),
        (24 * 45, 2, QueryIntent.RAW, QueryTier.COLD_JOB, ("queued_query_job",), True, True, 500),
    ],
)  # fmt: skip
def test_query_router_tier_selection(hours_old, hours, intent, tier, sources, queued, bounded, max_rows) -> None:
    plan = QueryRouter().plan(request(hours_old=hours_old, hours=hours, intent=intent), now=NOW)
    assert plan.tier is tier and plan.sources == sources and plan.queued is queued and plan.bounded is bounded
    if max_rows is not None:
        assert plan.max_rows == max_rows


@pytest.mark.parametrize(
    ("request_value", "message"),
    [
        (request(hours_old=24 * 45, hours=24 * 8), "cold query range"),
        (QueryRequest("", "request", NOW - timedelta(hours=1), NOW), "service id"),
        (QueryRequest("svc", "unknown", NOW - timedelta(hours=1), NOW), "domain"),
        (QueryRequest("svc", "request", NOW, NOW - timedelta(hours=1)), "query end"),
        (QueryRequest("svc", "request", datetime(2026, 9, 11, 11), NOW), "timezone"),
        (QueryRequest("svc", "request", NOW - timedelta(minutes=1), NOW + timedelta(minutes=1)), "future"),
    ],
)
def test_query_range_and_identity_validation(request_value: QueryRequest, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        QueryRouter().plan(request_value, now=NOW)
    with pytest.raises(ValueError, match="warm retention"):
        QueryRouter(QueryRouterConfig(warm_retention=timedelta(hours=1)))


def test_request_filter_clauses_translation_and_safety() -> None:
    clauses, params = request_filter_clauses(
        {"country": FilterSpec(mode="include", values=["US", "CA"]), "status": {"mode": "exclude", "values": [404, "500"]}}
    )  # fmt: skip
    assert clauses == [
        "(country IN ({flt_0_0:String}, {flt_0_1:String}))",
        "(custom_fields[{flt_k1:String}] NOT IN ({flt_1_0:String}, {flt_1_1:String}))",
    ]
    assert params == {"flt_0_0": "US", "flt_0_1": "CA", "flt_k1": "status", "flt_1_0": "404", "flt_1_1": "500"}
    for key in ("ip", "client_ip", "IP", "xfilter_ip_2"):
        assert request_filter_clauses({key: {"mode": "include", "values": ["203.0.113.7"]}})[0] == [
            "(client_ip IN ({flt_0_0:String}))"
        ]
    h_clauses, h_params = request_filter_clauses({"status'] OR 1=1 --": {"mode": "include", "values": ["x"]}})
    h_sql = " ".join(h_clauses)
    assert "OR 1=1" not in h_sql and "'" not in h_sql and h_params["flt_k0"] == "statusor11"
    for empty_flt in (None, {}, {"country": {"mode": "include", "values": ["", "  "]}, "pop": {"mode": "include", "values": []}}):  # fmt: skip
        assert request_filter_clauses(empty_flt) == ([], {})
    assert request_filter_clauses({"pop": {"mode": "include", "values": [None, "SJC"]}})[0] == [
        "(custom_fields[{flt_k0:String}] IN ({flt_0_0:String}) OR custom_fields[{flt_k0:String}] = '')"
    ]
    assert request_filter_clauses({"pop": {"mode": "exclude", "values": [None]}})[0] == ["(custom_fields[{flt_k0:String}] != '')"]  # fmt: skip
    f_clauses, f_params = request_filter_clauses({"ttl": {"mode": "include", "values": [3600]}})
    assert f_clauses == ["(toString(toInt32(round(toFloat64OrZero(custom_fields[{flt_k0:String}])))) IN ({flt_0_0:String}))"]  # fmt: skip
    assert f_params["flt_k0"] == "ttl"


@pytest.mark.parametrize("key", ["_bot_name", "_ngwaf_bot_name", "_tunnel_requests", "waf_sig_ind"])
def test_filters_the_fact_table_cannot_express_raise(key: str) -> None:
    with pytest.raises(ValueError, match="do not support filtering"):
        request_filter_clauses({key: {"mode": "include", "values": ["x"]}})


def test_high_scale_cache_observations_do_not_execute_the_repeat() -> None:
    perf_client = _PerfClient()
    sec_client = security_cases._Client()
    sec_svc, sec_req = security_cases._service(sec_client), security_cases._req()
    cmcd_client = cmcd_cases._Client()
    dash_svc, dash_client = _recording_service()
    dash_req = AggregatesRequest(fields=["url"], include_time_series=False)
    for calls, run, is_model in (
        (perf_client.calls, lambda: _run_perf(perf_client, sections={"waterfall"}), True),
        (sec_client.calls, lambda: security.security_aggregates(sec_svc, sec_req, security_cases.START, security_cases.END, sections={"ipv6_adoption"}), True),
        (cmcd_client.calls, lambda: cmcd_cases._run(cmcd_client, sections={"overview"}), False),
        (dash_client.calls, lambda: dashboard.aggregates(dash_svc, dash_req, "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"), True),
    ):  # fmt: skip
        first = run()
        count = len(calls)
        hit = run()
        assert count > 0 and len(calls) == count
        if is_model:
            assert first.response_cache.state == "miss" and hit.response_cache.state == "hit" and hit.is_cached is True
        else:
            assert first["_response_cache"]["state"] == "miss" and hit["_response_cache"]["state"] == "hit"


@pytest.mark.parametrize("filtered", [False, True])
@pytest.mark.parametrize(
    ("start", "end"),
    [
        ("2026-01-01T00:00:01Z", "2026-01-02T00:00:00Z"),
        ("2026-01-01T00:00:00Z", "2026-01-02T00:00:01Z"),
        ("2026-01-01T00:00:00Z", "2026-01-02T00:00:00.001Z"),
    ],
)
def test_dashboard_cache_does_not_alias_different_event_windows(filtered: bool, start: str, end: str) -> None:
    service, client = _recording_service()
    req = AggregatesRequest(
        fields=["url"], include_time_series=False, include_map_data=False,
        filters={"country": {"mode": "include", "values": ["US"]}} if filtered else {},
    )  # fmt: skip
    dashboard.aggregates(service, req, "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z")
    previous_calls = len(client.calls)
    changed = dashboard.aggregates(service, req, start, end)
    assert changed.response_cache.state == "miss" and len(client.calls) > previous_calls
    changed_calls = len(client.calls)
    repeat = dashboard.aggregates(service, req, start, end)
    assert repeat.response_cache.state == "hit" and len(client.calls) == changed_calls


class _ScopeClient:
    def __init__(self) -> None:
        self.scopes: list[dict] = []

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        self.scopes.append(current_clickhouse_scope())
        return []


@pytest.mark.parametrize("reader", ["origin", "performance", "security"])
def test_selected_reader_sections_attribute_real_dispatch_without_extra_queries(reader: str) -> None:
    client = _ScopeClient()
    service = SimpleNamespace(service_id="service-test", client=client)
    req = SimpleNamespace(filters={}, bucket_seconds=3600, sort_by="p99")
    start, end = "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"
    if reader == "origin":
        timings, source = origin.aggregates(service, req, start, end, sections={"status_codes"})["section_timings"], "projection"  # fmt: skip
    elif reader == "performance":
        timings, source = hs_performance.performance_aggregates(service, req, start, end, sections={"ttl_dist"}).section_timings, "projection"  # fmt: skip
    else:
        timings, source = security.security_aggregates(service, req, start, end, sections={"req_size_dist"}).section_timings, "fact"  # fmt: skip
    assert len(client.scopes) == 1 and len(timings) == 1
    assert client.scopes[0]["service_id"] == "service-test" and client.scopes[0]["source"] == source
    assert timings[0]["section_id"] == client.scopes[0]["section_id"]
    assert timings[0]["server_cpu_ms"] is None and timings[0]["timing_kind"] == "client_section_wall"


@pytest.mark.parametrize("reader,expected_queries", [("transport", 2), ("quality", 5)])
def test_network_queries_have_named_fact_sections(reader: str, expected_queries: int) -> None:
    client = _ScopeClient()
    service = SimpleNamespace(service_id="service-test", client=client)
    start, end = "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"
    result = (
        network.get_transport_metrics(service, start, end, {})
        if reader == "transport"
        else network.network_quality(service, NetworkQualityRequest(), start, end)
    )
    assert len(client.scopes) == expected_queries and len(result.section_timings) == expected_queries
    assert all(scope["service_id"] == "service-test" and scope["source"] == "fact" for scope in client.scopes)
    assert len({scope["section_id"] for scope in client.scopes}) == expected_queries


def test_bot_component_and_network_bundle_section_attribution() -> None:
    client, net_client = _ScopeClient(), _ScopeClient()
    bot_resp = security.top_bots(SimpleNamespace(service_id="service-test", client=client), SimpleNamespace(filters={}), "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z")  # fmt: skip
    assert bot_resp.section_timings[0]["source"] == "mixed" and [s["source"] for s in client.scopes] == ["fact", "projection"]  # fmt: skip
    assert len({s["section_id"] for s in client.scopes}) == 1
    net_resp = network.network_bundle(SimpleNamespace(service_id="service-test", client=net_client), NetworkBundleRequest(sections=["pop_health"]), "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z")  # fmt: skip
    assert net_resp.pop_health == [] and len(net_client.scopes) == 1 and len(net_resp.section_timings) == 1
    assert net_resp.section_timings[0]["section_id"] == net_client.scopes[0]["section_id"] and net_resp.section_timings[0]["section"] == "network.pop_health"  # fmt: skip


def cold_plan(*, max_rows: int = 3, tier: QueryTier = QueryTier.COLD_JOB, queued: bool = True) -> QueryPlan:
    return QueryPlan(
        service_id="svc", domain="request", intent=QueryIntent.RAW, tier=tier,
        start=NOW - timedelta(days=2), end=NOW - timedelta(days=1),
        sources=("queued_query_job",) if queued else ("clickhouse_facts",),
        queued=queued, bounded=queued, max_rows=max_rows, reason="test",
    )  # fmt: skip


def wait_for(manager: HistoricalJobManager, job_id: str, state: HistoricalJobState) -> HistoricalJob:
    deadline = monotonic() + 2
    while monotonic() < deadline:
        job = manager.get(job_id)
        if job is not None and job.state is state:
            return job
        sleep(0.005)
    raise AssertionError(f"job did not reach {state}")


def test_job_completes_and_stops_at_max_rows() -> None:
    manager = HistoricalJobManager(max_output_bytes=100, terminal_grace_ttl=timedelta(seconds=60))
    job = wait_for(manager, manager.submit(cold_plan(), lambda: [{"id": 1}, {"id": 2}]), HistoricalJobState.COMPLETED)
    assert job.rows == ({"id": 1}, {"id": 2}) and job.row_count == 2 and job.output_bytes <= 100
    manager.shutdown()

    manager2 = HistoricalJobManager(max_output_bytes=10_000)
    consumed: list[int] = []

    def bounded_rows():
        for idx in range(10):
            consumed.append(idx)
            yield {"id": idx}

    job2 = wait_for(manager2, manager2.submit(cold_plan(max_rows=2), bounded_rows), HistoricalJobState.COMPLETED)
    assert job2.rows == ({"id": 0}, {"id": 1}) and job2.row_count == 2 and len(consumed) <= 3
    manager2.shutdown()


@pytest.mark.parametrize("mode", ["cancel", "expire"])
def test_cancellation_and_expiry_are_observed_during_iteration(mode: str) -> None:
    clock = [NOW]
    manager = HistoricalJobManager(ttl=timedelta(seconds=1), max_output_bytes=10_000, clock=lambda: clock[0])
    started, release = Event(), Event()

    def rows():
        started.set()
        yield {"id": 1}
        release.wait(2)
        yield {"id": 2}

    job_id = manager.submit(cold_plan(max_rows=10), rows, now=clock[0])
    assert started.wait(1)
    if mode == "cancel":
        assert manager.cancel(job_id) is True
        expected_state = HistoricalJobState.CANCELLED
    else:
        clock[0] += timedelta(seconds=2)
        expected_state = HistoricalJobState.EXPIRED
    release.set()
    assert wait_for(manager, job_id, expected_state).rows == ({"id": 1},)
    manager.shutdown()


def test_output_byte_limit_purging_and_plan_validation() -> None:
    manager = HistoricalJobManager(max_output_bytes=20)
    job = wait_for(manager, manager.submit(cold_plan(max_rows=10), lambda: [{"value": "x" * 100}]), HistoricalJobState.FAILED)  # fmt: skip
    assert job.rows == () and job.error == "historical job output exceeds byte limit"
    manager.shutdown()

    manager2 = HistoricalJobManager(terminal_grace_ttl=timedelta(seconds=1))
    job_id = manager2.submit(cold_plan(), lambda: [])
    completed = wait_for(manager2, job_id, HistoricalJobState.COMPLETED)
    assert completed.terminal_at is not None and manager2.get(job_id, now=completed.terminal_at + timedelta(seconds=2)) is None  # fmt: skip
    manager2.shutdown()

    manager3 = HistoricalJobManager()
    with pytest.raises(ValueError, match="cold"):
        manager3.submit(cold_plan(max_rows=1, tier=QueryTier.RECENT_FACTS, queued=False), lambda: [])
    manager3.shutdown()


# ── Merged from tests/high_scale/test_sessions.py ──

_SESS_START = "2026-09-15T00:00:00Z"
_SESS_END = "2026-09-16T00:00:00Z"


def _session_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "ip": "203.0.113.7", "ja4": "t13d1516h2_abc",
        "session_start": datetime(2026, 9, 15, 10, 0, 0, tzinfo=UTC), "session_end": "2026-09-15 10:20:00.000",
        "req_count": 1500, "asn": 7922, "country": "US", "unique_urls": 40, "ua": "curl/8",
        "reqs_4xx": 10, "reqs_5xx": 1, "total_bytes": 123456, "median_rtt_ms": 12.5,
        "edge_count": 1000, "shield_count": 500, "edge_sid": "es-1", "streaming_reqs": 3,
        "flagged": 1, "is_streaming": 1, "sid": 0, "total_sessions": 321,
        "any_rtt": 1, "any_ja4": 1, "any_edge": 1, "any_edge_sid": 1, "any_cmcd": 1,
    }  # fmt: skip
    row.update(overrides)
    return row


class _SessClient:
    def __init__(self, rows: list[dict[str, Any]] | None = None, *, total: int = 0, fail: bool = False) -> None:
        self.calls: list[tuple[str, dict]] = []
        self._rows = [_session_row()] if rows is None else rows
        self._total = total
        self._fail = fail

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        self.calls.append((sql, params or {}))
        if self._fail:
            raise RuntimeError("clickhouse unavailable")
        if "sessions:count" in sql:
            return [{"total_sessions": self._total}]
        if "sessions:page" in sql:
            return self._rows
        raise AssertionError(sql)

    def sql(self, marker: str) -> tuple[str, dict]:
        return next((sql, params) for sql, params in self.calls if marker in sql)


def _run_sess(client: _SessClient, **overrides: Any) -> dict[str, Any]:
    req = SessionsRequest(**overrides)
    return sessions_endpoint(SimpleNamespace(service_id="svc", client=client), req, _SESS_START, _SESS_END)  # type: ignore[arg-type]


@pytest.fixture(autouse=True)
def _sess_asn_names(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(hs_sessions, "_asn_label_map", lambda service_id, asns: {asn: f"AS{asn} Name" for asn in asns})


def test_session_filters_fencing_sorting_paging_and_flags() -> None:
    client = _SessClient()
    _run_sess(client, filters={"country": {"mode": "include", "values": ["US"]}, "pop": {"mode": "exclude", "values": ["SJC"]}})  # fmt: skip
    sql, params = client.sql("sessions:page")
    assert "country IN ({flt_0_0:String})" in sql and "custom_fields[{flt_k1:String}] NOT IN ({flt_1_0:String})" in sql
    assert params["flt_0_0"] == "US" and params["flt_k1"] == "pop" and params["flt_1_0"] == "SJC" and params["service_id"] == "svc"  # fmt: skip
    assert "publication_state='visible'" in sql and "high_scale_batch_publications FINAL" in sql
    assert "domain='request'" in sql and "event_timestamp >= {start_time:DateTime64(3)}" in sql
    assert "lower(edge) IN ('1', 'true')" in sql and "lower(edge) IN ('0', 'false')" in sql

    c_sort, c_bad_sort, c_page, c_flags, c_stream = _SessClient(), _SessClient(), _SessClient(), _SessClient(), _SessClient()  # fmt: skip
    _run_sess(c_sort, sort_by="req_count", sort_dir="asc")
    assert "ORDER BY req_count ASC" in c_sort.sql("sessions:page")[0]
    _run_sess(c_bad_sort, sort_by="1; DROP TABLE x")
    bad_sql, _ = c_bad_sort.sql("sessions:page")
    assert "ORDER BY session_start DESC" in bad_sql and "DROP TABLE" not in bad_sql
    _run_sess(c_page, page=3, limit=25)
    page_sql, page_params = c_page.sql("sessions:page")
    assert "LIMIT {limit:UInt32} OFFSET {offset:UInt32}" in page_sql and page_params["limit"] == 25 and page_params["offset"] == 50  # fmt: skip

    result = _run_sess(c_flags)
    _, flag_params = c_flags.sql("sessions:page")
    assert (flag_params["min_reqs_flag"], flag_params["min_4xx_pct_flag"], result["min_reqs_flag"], result["min_4xx_pct_flag"]) == (1000, 20.0, 1000, 20.0)  # fmt: skip
    custom = _run_sess(_SessClient(), min_reqs_flag=50, min_4xx_pct_flag=5.5)
    assert custom["min_reqs_flag"] == 50 and custom["min_4xx_pct_flag"] == 5.5

    _run_sess(c_stream, streaming_only=True)
    stream_sql, stream_params = c_stream.sql("sessions:page")
    assert "cmcd['sid']" in stream_sql and "custom_fields['cmcd_sid']" not in stream_sql and stream_params.get("streaming_only") == 1  # fmt: skip


def test_session_totals_empty_pages_availability_and_cleaning() -> None:
    res_limit = _run_sess(_SessClient(), limit=1)
    assert (res_limit["total"], res_limit["page"], res_limit["limit"]) == (321, 1, 1)

    c_past = _SessClient(rows=[], total=40)
    res_past = _run_sess(c_past, page=9, limit=5)
    assert res_past["sessions"] == [] and res_past["total"] == 40 and "sessions:count" in " ".join(sql for sql, _ in c_past.calls)  # fmt: skip

    c_empty = _SessClient(rows=[])
    assert _run_sess(c_empty)["total"] == 0 and all("sessions:count" not in sql for sql, _ in c_empty.calls)

    present = _run_sess(_SessClient())
    absent = _run_sess(_SessClient(rows=[_session_row(any_rtt=0, any_ja4=0, any_edge=0, any_edge_sid=0, any_cmcd=0, median_rtt_ms=None)]))  # fmt: skip
    flags = ("has_rtt", "has_ja4", "has_edge", "has_edge_sid", "has_cmcd")
    assert tuple(present[k] for k in flags) == (True,) * 5 and tuple(absent[k] for k in flags) == (False,) * 5

    (row,) = present["sessions"]
    assert row["session_start"] == "2026-09-15T10:00:00Z" and row["session_end"] == "2026-09-15T10:20:00.000Z"
    assert row["flagged"] is True and row["is_streaming"] is True and present["asn_names"] == {7922: "AS7922 Name"}
    assert not ({"sid", "total_sessions", "any_rtt", "any_ja4", "any_edge", "any_edge_sid", "any_cmcd", "streaming_reqs"} & set(row))  # fmt: skip

    model = SessionsResponse.model_validate(present)
    assert model.total == 321 and model.sessions[0].asn == 7922 and model.asn_names[7922] == "AS7922 Name"
    with pytest.raises(RuntimeError, match="unavailable"):
        _run_sess(_SessClient(fail=True))


def test_detail_and_two_phase_session_enrichment() -> None:
    client = _SessClient()
    client.execute = lambda sql, params=None: client.calls.append((sql, params or {})) or []  # type: ignore[method-assign]
    sessions_detail(SimpleNamespace(service_id="svc", client=client), SessionDetailRequest(), _SESS_START, _SESS_END, "203.0.113.7", "ja4-x")  # type: ignore[arg-type]  # fmt: skip
    sql, params = client.calls[0]
    assert "publication_state='visible'" in sql and "high_scale_batch_publications FINAL" in sql
    assert "client_ip = {ip:String}" in sql and "custom_fields['ja4'] = {ja4:String}" in sql
    assert params["ip"] == "203.0.113.7" and params["ja4"] == "ja4-x"

    fast_row = {k: v for k, v in _session_row().items() if k not in {"asn", "country", "unique_urls", "ua", "median_rtt_ms", "edge_sid"}}  # fmt: skip
    calls: list[tuple[str, dict]] = []

    class _TwoPhaseClient:
        def execute(self, sql: str, params: dict | None = None) -> list[dict]:
            calls.append((sql, params or {}))
            if "sessions:page" in sql:
                return [fast_row]
            if "sessions:enrich" in sql:
                return [{"row_idx": 0, "asn": 15169, "country": "DE", "unique_urls": 77, "ua": "Mozilla/5.0", "median_rtt_ms": 8.25, "edge_sid": "es-fast"}]  # fmt: skip
            raise AssertionError(sql)

    res = sessions_endpoint(SimpleNamespace(service_id="svc", client=_TwoPhaseClient()), SessionsRequest(sort_by="req_count", sort_dir="desc"), _SESS_START, _SESS_END)  # type: ignore[arg-type]  # fmt: skip
    assert len(calls) == 2
    page_sql, _ = calls[0]
    enrich_sql, enrich_params = calls[1]
    assert "uniqState" not in page_sql and "medianState" not in page_sql
    assert "sessions:enrich" in enrich_sql and enrich_params["en_ip_0"] == "203.0.113.7"
    s0 = res["sessions"][0]
    assert (s0["asn"], s0["country"], s0["unique_urls"], s0["ua"], s0["median_rtt_ms"], s0["edge_sid"]) == (15169, "DE", 77, "Mozilla/5.0", 8.25, "es-fast")  # fmt: skip


class PredicateClient:
    def __init__(self, timestamps: list[datetime]) -> None:
        self.calls: list[str] = []
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.create_function("toStartOfMinute", 1, lambda value: value[:16] + ":00.000000")
        self.db.executescript(
            "CREATE TABLE request_facts "
            "(event_timestamp TEXT, url TEXT, country TEXT, service_id TEXT, batch_id TEXT, publication_state TEXT);"
            "CREATE TABLE high_scale_batch_publications "
            "(service_id TEXT, batch_id TEXT, domain TEXT, publication_state TEXT);"
            "INSERT INTO high_scale_batch_publications VALUES "
            "('svc', 'facts', 'request', 'visible'), ('svc', 'projection', 'request_aggregate', 'visible');"
        )
        self.db.executemany(
            "INSERT INTO request_facts VALUES (?, '/a', 'US', 'svc', 'facts', 'visible')",
            [(self.stamp(value),) for value in timestamps],
        )
        self.db.execute(
            "CREATE TABLE request_aggregates AS "
            "SELECT toStartOfMinute(event_timestamp) AS bucket_start, 'url' AS dimension, url AS value, "
            "count(*) AS request_count, service_id, 'projection' AS batch_id, publication_state FROM request_facts "
            "GROUP BY bucket_start, url, service_id, publication_state UNION ALL "
            "SELECT toStartOfMinute(event_timestamp), 'country', country, count(*), service_id, 'projection', "
            "publication_state FROM request_facts GROUP BY toStartOfMinute(event_timestamp), country, service_id, publication_state"
        )

    @staticmethod
    def stamp(value: datetime) -> str:
        return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        self.calls.append(sql)
        sql = re.sub(r"\{(\w+):[^}]+\}", r":\1", sql).replace(" FINAL", "")
        sql = sql.replace(
            "toStartOfInterval(bucket_start, INTERVAL :bucket_seconds SECOND)", "toStartOfMinute(bucket_start)"
        ).replace("LIMIT 10 BY dimension", "")
        bindings = {k: self.stamp(v) if isinstance(v, datetime) else v for k, v in (params or {}).items()}
        return [dict(row) for row in self.db.execute(sql, bindings).fetchall()]


_WINDOWS = [
    ("2026-10-23T00:20:17Z", "2026-10-24T00:20:17Z"),
    ("2026-10-23T00:20:17.125Z", "2026-10-24T00:20:17.125Z"),
    ("2026-10-23T00:20:17.125Z", "2026-10-24T00:20:00Z"),
    ("2026-10-23T00:20:00Z", "2026-10-24T00:20:17.125Z"),
    ("2026-10-23T00:20:00Z", "2026-10-24T00:20:00Z"),
    ("2026-10-23T00:20:00Z", "2026-10-23T00:20:00Z"),
    ("2026-10-23T00:20:17.125Z", "2026-10-23T00:20:17.125Z"),
]


@pytest.fixture
def event_window(request):
    start, end = (datetime.fromisoformat(value).astimezone(UTC) for value in request.param)
    tick = timedelta(milliseconds=1)
    client = PredicateClient(
        [start - tick, start, start + tick, end - tick, end, end + tick, end + timedelta(seconds=30)]
    )
    service = HighScaleService(
        service_id="svc",
        client=client,
        cursor_secret=b"secret",
        request_watermark=ServingWatermark("svc", "request", 1, start, end, None, None, None, True),
    )
    try:
        yield service, start, end
    finally:
        client.db.close()


@pytest.mark.parametrize("event_window", _WINDOWS, indirect=True)
@pytest.mark.parametrize("entrypoint", [aggregates, bundle])
def test_dashboard_matches_exact_half_open_facts(event_window, entrypoint) -> None:
    service, start, end = event_window
    client = service.client
    expected = client.db.execute(
        "SELECT count(*) FROM request_facts WHERE service_id = 'svc' AND publication_state = 'visible' "
        "AND batch_id = 'facts' AND event_timestamp >= ? AND event_timestamp < ?",
        (client.stamp(start), client.stamp(end)),
    ).fetchone()[0]

    response = entrypoint(
        service,
        AggregatesRequest(
            fields=["url", "country"], sections=["core", "topten"], chart_metric="requests", chart_interval="1 minute"
        ),
        start.isoformat(),
        end.isoformat(),
    )
    if entrypoint is bundle:
        response = response["aggregates"]

    assert response.total_rows == response.total_rows_total == expected
    for field in ("url", "country"):
        assert response.data[field].total == expected and sum(row.count for row in response.data[field].top) == expected
    assert sum(row.count for row in response.map_data) == expected
    assert sum(row.value for row in response.time_series) == expected

    if any(value.second or value.microsecond for value in (start, end)):
        assert client.calls and all("FROM request_facts" in sql for sql in client.calls)
    else:
        assert any("FROM request_aggregates" in sql for sql in client.calls)


@pytest.mark.parametrize("event_window", [_WINDOWS[4]], indirect=True)
@pytest.mark.parametrize("batched", [False, True])
def test_request_projection_excludes_the_end_bucket(event_window, batched) -> None:
    service, start, end = event_window
    watermark = service.watermark_for("request")
    responses = (
        query_clickhouse_aggregates_batch(
            service.client,
            service_id="svc",
            domain="request",
            dimensions=["url", "country"],
            start=start,
            end=end,
            watermark=watermark,
        )  # fmt: skip
        if batched
        else {
            "url": query_clickhouse_aggregate(
                service.client, AggregateRequest("svc", "request", start, end, "url"), watermark=watermark
            )
        }  # fmt: skip
    )
    for response in responses.values():
        assert response.request_count == 3 and sum(count for _, count in response.top_values) == 3


_PERF_START = "2026-09-16T14:00:00Z"
_PERF_END = "2026-09-16T15:00:00Z"
_PERCENTILE_ROW = {"value": "/slow", "req_count": 9, "avg_ms": 12.5, "p50_ms": 10.0, "p95_ms": 20.0, "p99_ms": 30.0}


class _PerfClient:
    def __init__(self, answers: dict[str, list[dict]] | None = None) -> None:
        self.answers = answers or {}
        self.calls: list[tuple[str, dict]] = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params or {}))
        return self.answers.get(sql.split("/* perf:", 1)[1].split(" */", 1)[0], [])

    def sql_for(self, marker: str) -> tuple[str, dict]:
        return next(call for call in self.calls if f"/* perf:{marker} */" in call[0])

    @property
    def markers(self) -> set[str]:
        return {sql.split("/* perf:", 1)[1].split(" */", 1)[0] for sql, _ in self.calls}


def _perf_service(client: _PerfClient) -> SimpleNamespace:
    return SimpleNamespace(service_id="svc", client=client)


def _perf_req(**kwargs) -> SimpleNamespace:
    return SimpleNamespace(**{"filters": {}, "sort_by": "p99", **kwargs})


def _run_perf(client: _PerfClient, req=None, sections=None, start=_PERF_START, end=_PERF_END):
    return hs_performance.performance_aggregates(_perf_service(client), req or _perf_req(), start, end, sections=sections)  # fmt: skip


def test_waterfall_scatter_and_top_asn_labels(monkeypatch) -> None:
    wf_row = {"component_count": 4, "edge_processing": 12.5, "origin_wait": 40.0, "origin_download": 5.0, "client_download": 9.25}  # fmt: skip
    client = _PerfClient({"waterfall": [wf_row]})
    res = _run_perf(client, sections={"waterfall", "scatter"})
    assert res.waterfall.avg is not None and res.waterfall.avg.model_dump() == {"edge_processing": 12.5, "origin_wait": 40.0, "origin_download": 5.0, "client_download": 9.25}  # fmt: skip
    sql, params = client.sql_for("waterfall")
    assert "FROM performance_minute_summary" in sql and "AS component_count" not in sql
    assert "sum(edge_processing_sum_ms) / nullIf(sum(component_count), 0)" in sql and "publication_state='visible'" in sql and params["service_id"] == "svc"  # fmt: skip

    zero_row = {"component_count": 0, "edge_processing": None, "origin_wait": None, "origin_download": None, "client_download": None}  # fmt: skip
    res_zero = _run_perf(_PerfClient({"waterfall": [zero_row]}), sections={"waterfall"})
    assert res_zero.waterfall.avg is not None and res_zero.waterfall.avg.model_dump() == dict.fromkeys(("edge_processing", "origin_wait", "origin_download", "client_download"), 0.0)  # fmt: skip

    sc_client = _PerfClient({"scatter": [{"origin_ms": 100.0, "edge_ms": 150.0, "cache": "MISS"}, {"origin_ms": 20.0, "edge_ms": 100.0, "cache": "HIT"}, {"origin_ms": 5.0, "edge_ms": 1.0, "cache": ""}]})  # fmt: skip
    sc_res = _run_perf(sc_client, sections={"scatter", "waterfall"})
    assert [(p.origin, p.edge, p.cache) for p in sc_res.scatter] == [(100.0, 150.0, "MISS"), (20.0, 100.0, "HIT"), (5.0, 1.0, None)]  # fmt: skip
    sc_sql, sc_params = sc_client.sql_for("scatter")
    assert "FROM performance_scatter_samples" in sc_sql and "ORDER BY sample_key" in sc_sql and sc_params["scatter_limit"] == 300  # fmt: skip

    monkeypatch.setattr("backend.core.duckdb.get_asn_names", lambda service_id, asns: {7922: "Comcast Cable"} if asns else {})  # fmt: skip
    row = {**_PERCENTILE_ROW, "value": "7922", "req_count": 40}
    res_asn = _run_perf(_PerfClient({"top_asns": [row, {**row, "value": "64512"}]}), sections={"top_asns", "top_urls"})
    assert [a.asn for a in res_asn.top_asns] == [7922, 64512]
    assert res_asn.top_asns[0].label == "Comcast Cable (7922)" and res_asn.top_asns[1].label == "AS64512"


@pytest.mark.parametrize("filtered", [False, True])
def test_asn_enrichment_is_disjoint_overhead_and_cache_reuse_is_historical(monkeypatch: pytest.MonkeyPatch, filtered: bool) -> None:  # fmt: skip
    clock, scopes = [0.0], {}
    monkeypatch.setattr("backend.core.high_scale_sections.time.perf_counter", lambda: clock[0])

    class Client(_PerfClient):
        def execute(self, sql: str, params: dict | None = None) -> list[dict]:
            scopes["query"] = current_clickhouse_scope()
            clock[0] += 0.01
            return super().execute(sql, params)

    def labels(service_id: str, asns: list[int]) -> dict[int, str]:
        assert service_id == "svc" and asns == [7922]
        scopes["labels"] = current_clickhouse_scope()
        clock[0] += 0.04
        return {7922: "Comcast Cable"}

    monkeypatch.setattr("backend.core.duckdb.get_asn_names", labels)
    client = Client({"top_asns": [{**_PERCENTILE_ROW, "value": "7922", "req_count": 40}]})
    req = _perf_req(filters={"country": FilterSpec(mode="include", values=["US"])} if filtered else {})
    token = current_attribution.set(Attribution(kind="admin", request_id="request-perf"))
    previous_page = get_page_load_id()
    set_page_load_id("page-perf")
    try:
        response = _run_perf(client, req, sections={"top_asns"})
        hit = _run_perf(client, req, sections={"top_asns"})
    finally:
        set_page_load_id(previous_page)
        current_attribution.reset(token)

    timings = {row["section"]: row for row in response.section_timings}
    query, enrichment = timings["performance.top_asns"], timings["performance.asn_labels"]
    assert query["source"] == ("fact" if filtered else "projection") and query["activity"] == "compute" and query["time_ms"] == pytest.approx(10)  # fmt: skip
    assert enrichment["source"] == "unknown" and enrichment["activity"] == "overhead" and enrichment["time_ms"] == pytest.approx(40)  # fmt: skip
    assert enrichment["executor_dispatch"] == "inline" and enrichment["executor_wait_ms"] is None
    assert scopes["query"]["section_id"] == query["section_id"] and scopes["labels"]["section_id"] == enrichment["section_id"] and query["section_id"] != enrichment["section_id"]  # fmt: skip
    for row in timings.values():
        assert (row["service_id"], row["request_id"], row["page_load_id"], row["server_cpu_ms"]) == ("svc", "request-perf", "page-perf", None)  # fmt: skip
    assert response.top_asns[0].label == "Comcast Cable (7922)" and hit.response_cache.state == "hit" and hit.section_timings == response.section_timings  # fmt: skip
    assert len(client.calls) == 1 and clock[0] == pytest.approx(0.05) and current_clickhouse_scope()["section_id"] is None  # fmt: skip


@pytest.mark.parametrize(("sort_by", "column"), [("avg", "avg_ms"), ("p50", "p50_ms"), ("p95", "p95_ms"), ("p99", "p99_ms")])  # fmt: skip
def test_sort_by_orders_the_top_tables(sort_by, column):
    client = _PerfClient()
    _run_perf(client, _perf_req(sort_by=sort_by), sections={"top_urls", "top_asns"})
    for marker in ("top_urls", "top_asns"):
        assert f"ORDER BY {column} DESC NULLS LAST" in client.sql_for(marker)[0]


def test_perf_latencies_thresholds_sections_ttl_and_unfiltered_projections() -> None:
    client = _PerfClient({"top_urls": [_PERCENTILE_ROW]})
    res = _run_perf(client, sections={"top_urls", "top_asns"})
    url = res.top_urls[0]
    assert (url.avg, url.p50, url.p95, url.p99) == (12.5, 10.0, 20.0, 30.0) and client.sql_for("top_urls")[0].count("/ 1000.0") == 4  # fmt: skip
    assert "HAVING req_count > 5" in client.sql_for("top_urls")[0] and "HAVING req_count > 10" in client.sql_for("top_asns")[0]  # fmt: skip

    c_ttl = _PerfClient({"ttl_dist": [{"bucket": "0s", "count": 3, "min_ttl": 0}, {"bucket": "<30m", "count": 7, "min_ttl": 1201}]})  # fmt: skip
    res_ttl = _run_perf(c_ttl, sections={"ttl_dist"})
    assert c_ttl.markers == {"ttl_dist"} and res_ttl.top_urls == [] and res_ttl.top_asns == [] and res_ttl.scatter == [] and res_ttl.waterfall.avg is None  # fmt: skip
    assert [(b.bucket, b.count) for b in res_ttl.ttl_dist] == [("0s", 3), ("<30m", 7)]
    ttl_sql, _ = c_ttl.sql_for("ttl_dist")
    assert "FROM request_aggregates" in ttl_sql and "LIMIT" not in ttl_sql and all(edge in ttl_sql for edge in ("'<30m'", "'<3h'", "'<1w'", "'<1y'", "'>1y'"))  # fmt: skip

    c_all = _PerfClient({"top_urls": [_PERCENTILE_ROW]})
    res_all = _run_perf(c_all, sections=None)
    assert isinstance(res_all, PerformanceAggregatesResponse) and res_all.approx is True
    assert c_all.markers == {"top_urls", "top_asns", "ttl_dist", "waterfall", "scatter"} and not any("request_facts" in sql for sql, _ in c_all.calls)  # fmt: skip
    for sql, params in c_all.calls:
        assert "high_scale_batch_publications" in sql and params["start"].isoformat().startswith("2026-09-16T14:00:00")


def test_perf_filters_against_request_facts_and_validation() -> None:
    wf = {"component_count": 1, "edge_processing": 1.0, "origin_wait": 2.0, "origin_download": 3.0, "client_download": 4.0}  # fmt: skip
    client = _PerfClient({"top_urls": [_PERCENTILE_ROW], "waterfall": [wf], "scatter": [{"origin_ms": 1.0, "edge_ms": 2.0, "cache": "HIT"}]})  # fmt: skip
    res = _run_perf(client, _perf_req(filters={"country": FilterSpec(mode="include", values=["US"])}))
    assert res.top_urls[0].p99 == 30.0 and res.waterfall.avg is not None and res.waterfall.avg.client_download == 4.0
    assert res.scatter[0].cache == "HIT" and res.approx is False and client.markers == {"top_urls", "top_asns", "ttl_dist", "waterfall", "scatter"}  # fmt: skip
    for sql, params in client.calls:
        assert "FROM request_facts" in sql and "performance_minute" not in sql and "performance_scatter" not in sql
        assert "country IN ({filter_0_0:String})" in sql and params["filter_0_0"] == "US"
        assert "publication_state='visible'" in sql and "event_timestamp >=" in sql
    assert "ORDER BY cityHash64(event_id)" in client.sql_for("scatter")[0]

    c_sort = _PerfClient()
    _run_perf(c_sort, _perf_req(filters={"asn": FilterSpec(mode="exclude", values=["64512"])}, sort_by="avg"), sections={"top_urls", "top_asns"})  # fmt: skip
    assert "ORDER BY avg_ms DESC NULLS LAST" in c_sort.sql_for("top_urls")[0] and "NOT IN" in c_sort.sql_for("top_asns")[0]  # fmt: skip

    with pytest.raises(ValueError, match="unsupported"):
        _run_perf(_PerfClient(), _perf_req(filters={"not_a_field": FilterSpec(mode="include", values=["x"])}))

    c_empty = _PerfClient()
    _run_perf(c_empty, _perf_req(filters={"country": FilterSpec(mode="include", values=[])}), sections={"waterfall"})
    assert "FROM performance_minute_summary" in c_empty.sql_for("waterfall")[0]


def _perf_event(**overrides):
    event = {
        "event_id": "evt-1", "timestamp": "2026-09-16T14:00:01Z", "url": "/a", "asn": "7922",
        "cache": "MISS", "elapsed": 250_000, "ttfb": 0.1, "ottfb": 40_000, "ottlb": 90_000,
    }  # fmt: skip
    event.update(overrides)
    return event


def test_build_performance_projection_rows() -> None:
    events = (
        _perf_event(),
        _perf_event(event_id="evt-2", timestamp="2026-09-16T14:00:30Z", elapsed=150_000, ttfb=0.05, ottfb=None, ottlb=None),
        _perf_event(event_id="evt-3", timestamp="2026-09-16T14:01:00Z"),
    )  # fmt: skip
    original = deepcopy(events)
    projection = build_performance_projection_rows(events)
    assert events == original
    assert projection.summary_rows == (
        {"bucket_start": "2026-09-16T14:00:00+00:00", "component_count": 2, "edge_processing_sum_ms": 110.0, "origin_wait_sum_ms": 40.0, "origin_download_sum_ms": 50.0, "client_download_sum_ms": 250.0},
        {"bucket_start": "2026-09-16T14:01:00+00:00", "component_count": 1, "edge_processing_sum_ms": 60.0, "origin_wait_sum_ms": 40.0, "origin_download_sum_ms": 50.0, "client_download_sum_ms": 150.0},
    )  # fmt: skip

    clamped = build_performance_projection_rows(
        (_perf_event(elapsed=10_000, ttfb=0.001, ottfb=40_000, ottlb=20_000),)
    ).summary_rows[0]
    assert (
        clamped["edge_processing_sum_ms"], clamped["origin_download_sum_ms"],
        clamped["client_download_sum_ms"], clamped["origin_wait_sum_ms"],
    ) == (0.0, 0.0, 0.0, 40.0)  # fmt: skip

    empty_proj = build_performance_projection_rows(
        (_perf_event(ttfb=None), _perf_event(event_id="e2", elapsed=""), _perf_event(event_id="e3", ttfb="nan"))
    )
    assert empty_proj.summary_rows == () and empty_proj.scatter_rows == ()

    sc_proj = build_performance_projection_rows(
        (
            _perf_event(),
            _perf_event(event_id="evt-hit", cache="HIT", elapsed=120_000, ttfb=0.02),
            _perf_event(event_id="evt-neg", elapsed=1_000, ttfb=0.5),
            _perf_event(event_id="evt-nocache", cache=None),
        )
    )
    assert {(r["cache"], r["origin_ms"], r["edge_ms"]) for r in sc_proj.scatter_rows} == {
        ("MISS", 100.0, 150.0), ("HIT", 20.0, 100.0), ("", 100.0, 150.0),
    }  # fmt: skip
    assert all(
        0 <= r["sample_key"] < 2**64 and r["bucket_start"] == "2026-09-16T14:00:00+00:00" for r in sc_proj.scatter_rows
    )

    sample_events = tuple(_perf_event(event_id=f"evt-{i}") for i in range(SCATTER_SAMPLE_SIZE * 2))
    sample_proj = build_performance_projection_rows(sample_events)
    keys = [r["sample_key"] for r in sample_proj.scatter_rows]
    assert len(keys) == SCATTER_SAMPLE_SIZE and keys == sorted(keys)
    assert [
        r["sample_key"] for r in build_performance_projection_rows(tuple(reversed(sample_events))).scatter_rows
    ] == keys

    dim_proj = build_performance_projection_rows(
        (_perf_event(asn=7922), _perf_event(event_id="e2", url="/b", asn=15169))
    )
    assert {(r["dimension"], r["value"]) for r in dim_proj.dimension_rows} == {
        ("url", "/a"), ("url", "/b"), ("asn", "7922"), ("asn", "15169"),
    }  # fmt: skip


@pytest.fixture
def script(monkeypatch: pytest.MonkeyPatch):
    from scripts import backfill_high_scale as module

    service = SimpleNamespace(manifest_catalog=object(), archive=object(), client=MagicMock())
    monkeypatch.setattr(module.app_config, "load_clickhouse_config", lambda: object())
    monkeypatch.setattr(module, "ClickHouseClient", lambda settings: MagicMock())
    monkeypatch.setattr(
        module, "get_high_scale_service_registry", lambda: MagicMock(resolve=MagicMock(return_value=service))
    )
    monkeypatch.setattr(module, "register_high_scale_services_from_environment", lambda *a, **k: None)
    for attr in (
        "PostgresControlPlane",
        "PostgresBatchManifestStore",
        "AggregateBatchAdapter",
        "ClickHousePublication",
    ):
        monkeypatch.setattr(module, attr, lambda *a, **k: MagicMock())
    return module


def _run_backfill_script(script, monkeypatch, argv: list[str], *, complete: bool = True):
    seen: dict = {}
    monkeypatch.setattr(
        script,
        "rebuild_performance_projections_from_fos",
        lambda *a, **kw: (seen.update(kw), RecoveryReport(True, complete, True, complete, complete, 2, 5, (), "rb-1"))[
            1
        ],
    )
    monkeypatch.setattr("sys.argv", ["backfill", *argv])
    return script.main_performance(), seen


def test_backfill_script_cli_options_and_exit_codes(script, monkeypatch, capsys) -> None:
    code, seen = _run_backfill_script(
        script, monkeypatch, ["--service-id", "svc", "--until", "2026-10-20T12:00:00Z", "--days", "7"]
    )
    io = capsys.readouterr()
    assert (code, seen["now"], seen["window"], seen["service_id"]) == (
        0, datetime(2026, 10, 20, 12, 0, tzinfo=UTC), timedelta(days=7), "svc",
    )  # fmt: skip
    assert '"rebuild_id": "rb-1"' in io.out and seen["rebuild_id"] and f"rebuild_id={seen['rebuild_id']}" in io.err
    assert (
        _run_backfill_script(
            script, monkeypatch, ["--service-id", "svc", "--until", "2026-10-20T12:00:00Z"], complete=False
        )[0]
        == 1
    )

    for bad_argv in (["--service-id", "svc"], ["--service-id", "svc", "--until", "2026-10-20T12:00:00"]):
        with pytest.raises(SystemExit) as exc:
            _run_backfill_script(script, monkeypatch, bad_argv)
        assert exc.value.code == 2

    _, explicit = _run_backfill_script(
        script, monkeypatch, ["--service-id", "svc", "--until", "2026-10-20T12:00:00Z", "--rebuild-id", "abc123"]
    )
    assert explicit["rebuild_id"] == "abc123"


# ── Merged from tests/high_scale/test_network_parity.py ──

_NET_START = datetime(2026, 9, 15, 0, 0, tzinfo=UTC)
_NET_END = datetime(2026, 9, 15, 1, 0, tzinfo=UTC)
_NET_START_S = "2026-09-15T00:00:00Z"
_NET_END_S = "2026-09-15T01:00:00Z"
_B0 = int(_NET_START.timestamp())
_B1 = int(datetime(2026, 9, 15, 0, 15, tzinfo=UTC).timestamp())
_ASN_NAMES = {7922: "Comcast Cable Communications"}


class RecordingClient:
    def __init__(self, responder):
        self.calls: list[tuple[str, dict]] = []
        self._responder = responder

    def execute(self, sql, params=None):
        self.calls.append((sql, dict(params or {})))
        return self._responder(sql, params or {})

    def matching(self, needle: str) -> list[tuple[str, dict]]:
        return [c for c in self.calls if needle in c[0]]


def _net_service(responder=lambda sql, params: []) -> tuple[HighScaleService, RecordingClient]:
    client = RecordingClient(responder)
    return HighScaleService(service_id="svc", client=client, cursor_secret=b"s", request_watermark=None), client


@pytest.fixture(autouse=True)
def _no_asn_network_lookup():
    with patch(
        "backend.core.duckdb.get_asn_names", side_effect=lambda _s, asns: {a: _ASN_NAMES.get(a, f"AS{a}") for a in asns}
    ):
        yield


def test_quality_scatter_filters_and_error_propagation():
    def responder(sql, params):
        if "AS cache_state" in sql:
            return [{"rtt_ms": 20.123, "ttfb_ms": 55.557, "cache_state": "HIT"}, {"rtt_ms": 31.0, "ttfb_ms": 140.0, "cache_state": "UNKNOWN"}]  # fmt: skip
        return []

    service, client = _net_service(responder)
    resp = network_quality(
        service, NetworkQualityRequest(start_time=_NET_START_S, end_time=_NET_END_S), _NET_START_S, _NET_END_S
    )
    assert [(p.rtt_ms, p.ttfb_ms, p.cache) for p in resp.scatter] == [(20.12, 55.56, "HIT"), (31.0, 140.0, "UNKNOWN")]
    ((sql, params),) = client.matching("AS cache_state")
    assert "LIMIT 2000" in sql and "toFloat64OrNull(custom_fields['ttfb']) > 0" in sql
    assert "toFloat64OrNull(custom_fields['tcp_rtt']) > 0" in sql and params["service_id"] == "svc"

    service2, client2 = _net_service(
        lambda sql, p: (
            [{"label": 7922, "rtt_ms": 20.0, "reqs": 10}]
            if "toInt32OrNull(custom_fields['asn']) AS label" in sql
            else []
        )
    )
    req2 = NetworkQualityRequest(
        start_time=_NET_START_S,
        end_time=_NET_END_S,
        filters={"country": FilterSpec(mode="include", values=["US", "CA"])},
    )
    resp2 = network_quality(service2, req2, _NET_START_S, _NET_END_S)
    assert len(client2.calls) == 5
    for q_sql, q_params in client2.calls:
        assert "country IN ({filter_0_0:String}, {filter_0_1:String})" in q_sql
        assert q_params["filter_0_0"] == "US" and q_params["filter_0_1"] == "CA"
    assert resp2.by_asn[0].value == "7922" and resp2.by_asn[0].label == "Comcast Cable Communications (7922)"

    def fail_scatter(sql, p):
        if "AS cache_state" in sql:
            raise RuntimeError("clickhouse down")
        return []

    with pytest.raises(RuntimeError, match="clickhouse down"):
        network_quality(_net_service(fail_scatter)[0], NetworkQualityRequest(), _NET_START_S, _NET_END_S)


def test_pop_health_aggregates_and_default_bounds():
    rows = [
        {"pop": "SJC", "requests": 200, "errors": 10, "p50_rtt_us": 18000.0, "p95_ttfb_ms": 320.5, "cache_hits": 150, "bandwidth_bytes": 4096},
        {"pop": "LHR", "requests": 50, "errors": 0, "p50_rtt_us": None, "p95_ttfb_ms": None, "cache_hits": 0, "bandwidth_bytes": None},
    ]  # fmt: skip
    service, client = _net_service(lambda sql, params: rows)
    sjc, lhr = get_pop_health(service, _NET_START, _NET_END).data
    assert (sjc.pop, sjc.requests, sjc.errors, sjc.error_rate) == ("SJC", 200, 10, 5.0)
    assert (sjc.p50_rtt_us, sjc.p95_ttfb_ms, sjc.cache_hit_rate, sjc.bandwidth_bytes) == (18000.0, 320.5, 75.0, 4096)
    assert (lhr.p50_rtt_us, lhr.p95_ttfb_ms, lhr.cache_hit_rate, lhr.bandwidth_bytes) == (None, None, 0.0, 0)
    ((sql, params),) = client.calls
    assert "countIf(toInt32OrZero(custom_fields['status']) >= 400 OR toInt32OrZero(custom_fields['status']) = 0)" in sql
    assert "'HIT', 'HIT-STALE'" in sql and params["start"] == _NET_START and params["end"] == _NET_END

    s_def, c_def = _net_service()
    get_pop_health(s_def, None, None)
    assert (c_def.calls[0][1]["end"] - c_def.calls[0][1]["start"]).total_seconds() == 86400

    s_naive, c_naive = _net_service()
    get_pop_health(s_naive, datetime(2026, 9, 15, 0, 0), datetime(2026, 9, 15, 1, 0))
    assert c_naive.calls[0][1]["start"].tzinfo is UTC and c_naive.calls[0][1]["end"].tzinfo is UTC


def test_health_threads_filters_map_asn_and_reports_throughput():
    def responder(sql, params):
        if "rtt_jitter_us" in sql and "GROUP BY asn" in sql:
            return [{"asn": 7922, "bucket_ts": _B0, "reqs": 100, "throughput_bps": 1_250_000.4, "rtt_med_us": 20000.0, "rtt_baseline_us": 18000.0, "rtt_congestion_us": 2000.0, "avg_ploss": 0.0, "rtt_jitter_us": 1000.0, "error_pct": 0.0}]  # fmt: skip
        return []

    service, client = _net_service(responder)
    req = NetworkHealthRequest(
        start_time=_NET_START_S,
        end_time=_NET_END_S,
        sections=["summary", "heatmap", "map_buckets"],
        map_asn="7922",
        filters={
            "country": FilterSpec(mode="exclude", values=["CN"]),
            "asn": FilterSpec(mode="include", values=["15169"]),
        },
    )
    resp = network_health(service, req, _NET_START_S, _NET_END_S)
    assert resp.heatmap[0].buckets[0].throughput_bps == 1_250_000.0
    assert resp.heatmap[0].label == resp.leaderboard[0].label == "Comcast Cable Communications (7922)"
    assert len(client.calls) == 5
    for sql, params in client.calls:
        assert "country NOT IN ({filter_0_0:String})" in sql and "15169" not in params.values()
    ((map_sql, map_params),) = client.matching("AS lat")
    assert "toInt32OrNull(custom_fields['asn']) = {map_asn:Int32}" in map_sql and map_params["map_asn"] == 7922
    for needle in ("AS region", "GROUP BY asn, c_speed", "quantile(0.95)"):
        assert "map_asn" not in client.matching(needle)[0][0]


def test_health_shielding_analysis_and_section_pruning():
    def responder(sql, params):
        if "INNER JOIN" in sql:
            return [
                {"edge_pop": "DEN", "shield_pop": "IAD", "requests": 400, "p50_ms": 40.0, "p95_ms": 90.0, "p99_ms": 120.0},
                {"edge_pop": "LHR", "shield_pop": "IAD", "requests": 10, "p50_ms": 300.0, "p95_ms": 500.0, "p99_ms": None},
            ]  # fmt: skip
        return []

    service, client = _net_service(responder)
    req = NetworkHealthRequest(
        start_time=_NET_START_S,
        end_time=_NET_END_S,
        sections=["shielding_analysis"],
        filters={"pop": FilterSpec(mode="include", values=["DEN"])},
    )
    pop_coords = {"DEN": (39.86, -104.67), "IAD": (38.94, -77.46), "LHR": (51.47, -0.45)}
    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value=pop_coords):
        resp = network_health(service, req, _NET_START_S, _NET_END_S)

    shield = resp.shielding_analysis
    assert shield is not None and shield.has_data is True
    assert [(r.edge_pop, r.shield_pop, r.requests) for r in shield.rows] == [("DEN", "IAD", 400), ("LHR", "IAD", 10)]
    assert shield.rows[0].p95_ms == 90.0 and shield.rows[1].p99_ms is None and shield.rows[0].distance_km is not None
    assert (shield.total_routes, shield.truncated) == (2, False) and len(client.calls) == 1
    edge_cte, shield_cte = client.calls[0][0].split("INNER JOIN")
    assert "custom_fields[{filter_k0:String}] IN ({filter_0_0:String})" in edge_cte
    assert client.calls[0][1]["filter_k0"] == "pop" and "filter_0_0" not in shield_cte

    for n_val, expected_edge_only in ((0, True), (1, None)):
        svc_n, _ = _net_service(lambda sql, p, n=n_val: [{"n": n}] if "SELECT count() AS n" in sql else [])
        r_n = network_health(svc_n, NetworkHealthRequest(sections=["shielding_analysis"]), _NET_START_S, _NET_END_S)
        assert r_n.shielding_analysis.has_data is False and r_n.shielding_analysis.edge_only is expected_edge_only

    s_core, c_core = _net_service()
    network_health(s_core, NetworkHealthRequest(sections=["map_buckets", "cities"]), _NET_START_S, _NET_END_S)
    assert c_core.calls and not c_core.matching("INNER JOIN")


def test_transport_and_bundle_queries():
    t_row = {
        "total_reqs": 100, "total_bytes": 10, "rtt_p50": 10.0, "rtt_p90": 20.0, "rtt_p95": 30.0, "rtt_p99": 40.0,
        "avg_retrans": None, "avg_ploss_pct": 0.5, "avg_del_rate_mbps": None,
        "seen_tcp_rtt": 100, "seen_retrans": 0, "seen_ploss": 80, "seen_delivery_rate": 0,
    }  # fmt: skip
    service, client = _net_service(lambda sql, p: [t_row] if "seen_tcp_rtt" in sql else [])
    resp = get_transport_metrics(service, _NET_START_S, _NET_END_S, {"pop": FilterSpec(mode="include", values=["SJC"])})
    assert (
        resp.missing_fields == ["retrans", "delivery_rate"]
        and resp.has_transport_data is True
        and len(client.calls) == 2
    )
    for sql, params in client.calls:
        assert "custom_fields[{filter_k0:String}] IN ({filter_0_0:String})" in sql
        assert params["filter_k0"] == "pop" and params["filter_0_0"] == "SJC"

    empty_t = {"total_reqs": 5, "seen_tcp_rtt": 0, "seen_retrans": 0, "seen_ploss": 0, "seen_delivery_rate": 0}
    resp_empty = get_transport_metrics(
        _net_service(lambda sql, p: [empty_t] if "seen_tcp_rtt" in sql else [])[0], _NET_START_S, _NET_END_S
    )
    assert resp_empty.has_transport_data is False and resp_empty.missing_fields == [
        "tcp_rtt",
        "retrans",
        "ploss",
        "delivery_rate",
    ]

    def bundle_responder(sql, params):
        if "AS cache_state" in sql:
            return [{"rtt_ms": 10.0, "ttfb_ms": 50.0, "cache_state": "MISS"}]
        if "AS bandwidth_bytes" in sql:
            return [{"pop": "SJC", "requests": 10, "errors": 1, "p50_rtt_us": 1.0, "p95_ttfb_ms": 2.0, "cache_hits": 5, "bandwidth_bytes": 7}]  # fmt: skip
        return []

    b_svc, b_client = _net_service(bundle_responder)
    b_resp = network_bundle(
        b_svc,
        NetworkBundleRequest(
            start_time=_NET_START_S, end_time=_NET_END_S, filters={"country": FilterSpec(mode="include", values=["US"])}
        ),
        _NET_START_S,
        _NET_END_S,
    )
    assert b_resp.pop_health and b_resp.pop_health[0].pop == "SJC" and b_resp.quality.scatter[0].cache == "MISS"
    assert b_resp.transport is not None and b_resp.health is not None
    for _, params in b_client.matching("AS cache_state") + b_client.matching("seen_tcp_rtt"):
        assert params["filter_0_0"] == "US"
    assert "filter_0_0" not in b_client.matching("AS bandwidth_bytes")[0][1]

    s_sel, c_sel = _net_service()
    sel_resp = network_bundle(
        s_sel,
        NetworkBundleRequest(start_time=_NET_START_S, end_time=_NET_END_S, sections=["pop_health"]),
        _NET_START_S,
        _NET_END_S,
    )
    assert sel_resp.health is None and sel_resp.quality is None and sel_resp.transport is None
    assert sel_resp.pop_health == [] and len(c_sel.calls) == 1
    obs = sel_resp.component_observations["pop_health"]
    assert (
        obs.response_cache.state == "bypass"
        and obs.section_timings
        and obs.section_timings[0].model_extra["source"] == "fact"
    )


class _FailingClient:
    def __init__(self, marker: str = "") -> None:
        self._marker = marker

    def execute(self, sql, params=None):
        if self._marker in sql:
            raise RuntimeError("clickhouse down")
        return []


def _failing_service(marker: str = "") -> HighScaleService:
    return HighScaleService(service_id="svc", client=_FailingClient(marker), cursor_secret=b"s", request_watermark=None)


@pytest.mark.parametrize(
    ("marker", "call"),
    [
        (
            "",
            lambda s: network_health(
                s,
                NetworkHealthRequest(start_time=_NET_START_S, end_time=_NET_END_S, sections=["heatmap"]),
                _NET_START_S,
                _NET_END_S,
            ),
        ),
        (
            "",
            lambda s: network_quality(
                s, NetworkQualityRequest(start_time=_NET_START_S, end_time=_NET_END_S), _NET_START_S, _NET_END_S
            ),
        ),
        ("", lambda s: get_transport_metrics(s, _NET_START_S, _NET_END_S)),
        (
            "GROUP BY label",
            lambda s: network_quality(
                s, NetworkQualityRequest(start_time=_NET_START_S, end_time=_NET_END_S), _NET_START_S, _NET_END_S
            ),
        ),
        ("proto_clean", lambda s: get_transport_metrics(s, _NET_START_S, _NET_END_S)),
    ],
)
def test_clickhouse_failures_propagate(marker, call) -> None:
    with pytest.raises(RuntimeError, match="clickhouse down"):
        call(_failing_service(marker))


class _Req:
    start_time = _NET_START.isoformat().replace("+00:00", "Z")
    end_time = _NET_END.isoformat().replace("+00:00", "Z")
    bucket_seconds = 300
    top_n = 30
    region_country = "US"


class _NetHealthFakeClient:
    def execute(self, sql, params=None):
        if "INNER JOIN" in sql:
            return []
        if "rtt_jitter_us" in sql and "GROUP BY asn" in sql:
            return [
                {"asn": 7922, "bucket_ts": _B0, "reqs": 100, "rtt_med_us": 20000.0, "rtt_baseline_us": 18000.0, "rtt_congestion_us": 2000.0, "avg_ploss": 0.0, "rtt_jitter_us": 1000.0, "error_pct": 0.0},
                {"asn": 7922, "bucket_ts": _B1, "reqs": 80, "rtt_med_us": 21000.0, "rtt_baseline_us": 18500.0, "rtt_congestion_us": 2500.0, "avg_ploss": 0.0, "rtt_jitter_us": 1200.0, "error_pct": 0.0},
                {"asn": 15169, "bucket_ts": _B0, "reqs": 60, "rtt_med_us": 90000.0, "rtt_baseline_us": 30000.0, "rtt_congestion_us": 150000.0, "avg_ploss": 0.03, "rtt_jitter_us": 80000.0, "error_pct": 8.0},
            ]  # fmt: skip
        if "AS lat" in sql:
            return [
                {"country": "US", "city": "san jose", "lat": 37.33, "lon": -121.89, "metro": "807", "bucket_ts": _B0, "rtt_med_us": 20000.0, "avg_ploss": 0.0, "error_pct": 0.0, "reqs": 100},
                {"country": "US", "city": "boston", "lat": 42.36, "lon": -71.06, "metro": "506", "bucket_ts": _B1, "rtt_med_us": 90000.0, "avg_ploss": 0.03, "error_pct": 8.0, "reqs": 60},
            ]  # fmt: skip
        if "AS region" in sql:
            return [
                {"country": "US", "city": "san jose", "region": "California", "metro": "807", "avg_ploss": 0.0, "error_pct": 0.0, "total_reqs": 100},
                {"country": "US", "city": "boston", "region": "Massachusetts", "metro": "506", "avg_ploss": 0.03, "error_pct": 8.0, "total_reqs": 60},
            ]  # fmt: skip
        if "GROUP BY asn, c_speed" in sql:
            return [
                {"asn": 7922, "c_speed": "fast", "cnt": 150},
                {"asn": 7922, "c_speed": "medium", "cnt": 30},
                {"asn": 15169, "c_speed": "slow", "cnt": 60},
            ]
        if "quantile(0.95)" in sql:
            return [{"asn": 7922, "p95": 40000.0, "p99": 60000.0}, {"asn": 15169, "p95": 180000.0, "p99": 260000.0}]
        if "AS label" in sql:
            return [{"value": "US", "label": "US", "rtt_ms": 21.0, "reqs": 160}]
        return []


def _health_service() -> HighScaleService:
    return HighScaleService(
        service_id="svc", client=_NetHealthFakeClient(), cursor_secret=b"secret", request_watermark=None
    )


def test_network_health_and_quality_full_clickhouse_payload():
    svc = _health_service()
    resp = network_health(svc, _Req(), _Req.start_time, _Req.end_time)
    assert resp.buckets and len(resp.buckets) == 13 and resp.heatmap
    assert resp.heatmap[0].buckets and resp.heatmap[0].asn == 7922 and resp.leaderboard
    lead = {row.asn: row for row in resp.leaderboard}
    assert lead[7922].health_score_now is not None and lead[15169].health_score_now < lead[7922].health_score_now
    assert lead[7922].p95_rtt_us == 40000.0 and lead[7922].c_speed_mix
    assert resp.map_buckets and resp.cities and any(c.name for c in resp.cities) and resp.metro_leaderboard
    assert resp.summary is not None and resp.summary.global_health_score != 100.0
    assert resp.summary.total_reqs == 240 and resp.summary.worst_asn is not None

    q_resp = network_quality(svc, _Req(), _Req.start_time, _Req.end_time)
    assert q_resp.by_region and q_resp.by_country and q_resp.by_region[0].rtt_ms > 0


# ── Merged from tests/high_scale/test_dashboard.py ──

_WINDOW = ("2026-09-15T00:00:00Z", "2026-09-16T00:00:00Z")


class FakeClient:
    def execute(self, sql, params=None):
        if "AS chart_bucket" in sql:
            return [{"chart_bucket": "2026-09-15 19:00:00", "value": 12}]
        if "AS bucket_start" in sql or "SELECT bucket_start" in sql:
            return [{"bucket_start": "2026-09-15 19:00:00", "value": 12}]
        if "AS v, count() AS c" in sql:
            return [{"v": "/synthetic", "c": 12}]
        if "count() AS c FROM request_facts" in sql:
            return [{"c": 12}]
        if "max(event_timestamp)" in sql:
            domain = (params or {}).get("domain")
            rows_map = {"request": (12, 0), "rum_vitals": (8, 1), "rum_errors": (2, 2)}
            total, min_off = rows_map[domain]
            return [
                {
                    "total_rows": total,
                    "earliest_log_at": datetime(2026, 9, 15, 18, min_off, tzinfo=UTC),
                    "latest_log_at": datetime(2026, 9, 15, 19, min_off, tzinfo=UTC),
                }
            ]
        return [{"value": "/synthetic", "aggregate_count": 12, "total_count": 12}]


class _RecordingClient(FakeClient):
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params or {}))
        return super().execute(sql, params)


def _default_watermark() -> ServingWatermark:
    return ServingWatermark(
        service_id="svc",
        domain="request",
        owner_epoch=1,
        coverage_start=datetime(2026, 9, 15, tzinfo=UTC),
        coverage_end=datetime(2026, 9, 16, tzinfo=UTC),
        last_accepted_cursor=None,
        last_archived_event_id=None,
        last_visible_event_id=None,
        exact=True,
    )


def _service_with_watermark(client: Any = None, watermark: Any = None) -> HighScaleService:
    return HighScaleService(
        service_id="svc",
        client=client or FakeClient(),
        cursor_secret=b"secret",
        request_watermark=_default_watermark() if watermark is None else watermark,
    )


def _recording_service() -> tuple[HighScaleService, _RecordingClient]:
    client = _RecordingClient()
    return _service_with_watermark(client=client), client


# ── Dashboard Aggregates, Header Metrics & Bundle ────────────────────────────


def test_high_scale_dashboard_aggregates_and_extents():
    service = _service_with_watermark()
    response = aggregates(service, AggregatesRequest(fields=["url", "country"]), *_WINDOW)
    assert response.total_rows == 12 and response.data["url"].total == 12 and response.map_data
    assert response.time_series[0].value == 12 and response.time_series[0].time == "2026-09-15T19:00:00Z"
    assert response.earliest_log_at == "2026-09-15T00:00:00+00:00"
    assert response.latest_log_at == "2026-09-16T00:00:00+00:00"

    flt_resp = aggregates(
        service,
        AggregatesRequest(
            filters={"host": {"mode": "include", "values": ["example.com"]}},
            chart_interval="1 minute",
            chart_metric="requests",
        ),
        *_WINDOW,
    )
    assert flt_resp.time_series[0].value == 12 and flt_resp.time_series[0].time == "2026-09-15T19:00:00Z"
    assert flt_resp.earliest_log_at == "2026-09-15T00:00:00+00:00"
    assert flt_resp.latest_log_at == "2026-09-16T00:00:00+00:00"

    dims_resp = aggregates(service, AggregatesRequest(fields=["host", "method", "status", "cache", "ua"]), *_WINDOW)
    assert set(dims_resp.data) == {"host", "method", "status", "cache", "ua"}
    assert all(field.total == 12 for field in dims_resp.data.values())


def test_high_scale_header_metrics_and_clickhouse_failure_propagation():
    metrics = header_metrics(_service_with_watermark())
    assert metrics["request"] == {
        "earliest_log_at": "2026-09-15T18:00:00+00:00",
        "latest_log_at": "2026-09-15T19:00:00+00:00",
        "total_rows": 12,
        "last_sync_at": None,
    }
    assert metrics["rum"] == {
        "earliest_log_at": "2026-09-15T18:01:00+00:00",
        "latest_log_at": "2026-09-15T19:02:00+00:00",
        "total_rows": 10,
        "last_sync_at": None,
    }
    assert (
        metrics["earliest_log_at"] == "2026-09-15T18:00:00+00:00"
        and metrics["latest_log_at"] == "2026-09-15T19:00:00+00:00"
    )
    assert metrics["local_rows"] == 22

    class _Failing:
        def execute(self, sql, params=None):
            if "rum_error_aggregates" in sql:
                raise RuntimeError("clickhouse down")
            return []

    with pytest.raises(RuntimeError, match="clickhouse down"):
        header_metrics(
            HighScaleService(service_id="svc", client=_Failing(), cursor_secret=b"s", request_watermark=None)
        )


def test_high_scale_dashboard_reuses_request_watermark_once_per_bundle():
    calls = {"count": 0}

    def request_watermark():
        calls["count"] += 1
        return _default_watermark()

    service = _service_with_watermark(watermark=request_watermark)
    response = aggregates(service, AggregatesRequest(fields=["url", "country", "host"]), *_WINDOW)
    assert response.total_rows == 12 and calls["count"] == 1


def test_high_scale_time_series_intervals_and_end_minute_fence():
    service, client = _recording_service()
    aggregates(service, AggregatesRequest(fields=["url"]), "2026-09-15T00:00:00Z", "2026-09-15T19:00:00Z")
    ts_queries = [s for s, _ in client.calls if "FROM request_aggregates" in s]
    assert ts_queries and any("bucket_start < {end:DateTime64(3)}" in q for q in ts_queries)

    client.calls.clear()
    resp_1s = aggregates(service, AggregatesRequest(fields=["url"], chart_interval="1 second"), *_WINDOW)
    assert any("toStartOfSecond(event_timestamp)" in s for s, _ in client.calls)
    assert not any("FROM request_aggregates" in s for s, _ in client.calls)
    assert resp_1s.interval == "1 second"


@pytest.mark.parametrize(
    ("interval", "seconds"), [("1 minute", 60), ("5 minutes", 300), ("1 hour", 3600), ("1 day", 86400)]
)
def test_high_scale_time_series_groups_by_returned_interval(interval: str, seconds: int) -> None:
    service, client = _recording_service()
    response = aggregates(service, AggregatesRequest(fields=["url"], chart_interval=interval), *_WINDOW)
    query, params = next((sql, p) for sql, p in client.calls if "FROM request_aggregates" in sql and "AS value" in sql)
    assert "toStartOfInterval(bucket_start, INTERVAL {bucket_seconds:UInt32} SECOND)" in query
    assert "GROUP BY chart_bucket ORDER BY chart_bucket" in query
    assert params["bucket_seconds"] == seconds and response.interval == interval and response.time_series[0].value == 12


@pytest.mark.parametrize("filtered", [False, True])
def test_dashboard_attributes_projection_and_fact_dispatch_and_reuses_historical_cache(monkeypatch, filtered):
    from backend.core.high_scale_sections import current_clickhouse_scope

    service, client = _recording_service()
    scopes = []
    execute = client.execute
    monkeypatch.setattr(
        client, "execute", lambda sql, params=None: (scopes.append(current_clickhouse_scope()), execute(sql, params))[1]
    )
    req = AggregatesRequest(
        fields=["url"], filters={"host": {"mode": "include", "values": ["example.com"]}} if filtered else {}
    )
    response = aggregates(service, req, *_WINDOW)
    assert scopes and len(scopes) == len(client.calls)
    assert all(s["service_id"] == "svc" and s["source"] == ("fact" if filtered else "projection") for s in scopes)
    assert all(s["section_id"] in {row["section_id"] for row in response.section_timings} for s in scopes)

    calls_before = len(client.calls)
    hit = aggregates(service, req, *_WINDOW)
    assert hit.response_cache.state == "hit" and len(client.calls) == calls_before
    assert hit.section_timings == response.section_timings


@pytest.mark.parametrize(
    ("start_time", "end_time"),
    [
        ("2026-09-15T00:00:17Z", "2026-09-16T00:00:00Z"),
        ("2026-09-15T00:00:00Z", "2026-09-16T00:00:17Z"),
        ("2026-09-15T00:00:00.001Z", "2026-09-16T00:00:00Z"),
        ("2026-09-15T00:00:00Z", "2026-09-16T00:00:00.001Z"),
    ],
)
def test_unfiltered_subminute_windows_use_scoped_facts(start_time: str, end_time: str) -> None:
    service, client = _recording_service()
    response = aggregates(service, AggregatesRequest(fields=["url", "country"]), start_time, end_time)
    start, end = datetime.fromisoformat(start_time), datetime.fromisoformat(end_time)
    assert client.calls
    for sql, params in client.calls:
        assert "FROM request_facts" in sql and "high_scale_batch_publications FINAL" in sql
        assert (
            "event_timestamp >= {start:DateTime64(3)}" in sql and "event_timestamp < {end_upper:DateTime64(3)}" in sql
        )
        assert params["service_id"] == "svc" and params["start"] == start and params["end_upper"] == end
    assert all(row["source"] == "fact" for row in response.section_timings if row["activity"] != "overhead")


def test_filtered_aggregates_translation_fence_and_unsupported_fields():
    service, client = _recording_service()
    aggregates(
        service,
        AggregatesRequest(
            fields=["url"],
            filters={"country": {"mode": "include", "values": ["US"]}, "pop": {"mode": "exclude", "values": ["SJC"]}},
        ),
        *_WINDOW,
    )
    facts = [(sql, params) for sql, params in client.calls if "FROM request_facts" in sql]
    assert facts
    for sql, params in facts:
        assert (
            "country IN ({flt_0_0:String})" in sql and "custom_fields[{flt_k1:String}] NOT IN ({flt_1_0:String})" in sql
        )
        assert "high_scale_batch_publications FINAL" in sql and "domain='request'" in sql
        assert params["flt_0_0"] == "US" and params["flt_1_0"] == "SJC"

    client.calls.clear()
    aggregates(
        service, AggregatesRequest(fields=["url"], filters={"rid": {"mode": "include", "values": ["abc"]}}), *_WINDOW
    )
    assert any(params.get("flt_k0") == "rid" for _, params in client.calls)

    with pytest.raises(ValueError, match="do not support filtering"):
        aggregates(
            service, AggregatesRequest(filters={"_bot_name": {"mode": "include", "values": ["googlebot"]}}), *_WINDOW
        )


@pytest.mark.parametrize("failing", ["AS v, count() AS c", "count() AS c FROM request_facts", "AS bucket_start"])
def test_filtered_aggregate_failures_propagate_instead_of_rendering_empty_cards(failing):
    class Failing(FakeClient):
        def execute(self, sql, params=None):
            if failing in sql:
                raise RuntimeError("clickhouse unavailable")
            return super().execute(sql, params)

    service = _service_with_watermark(client=Failing())
    with pytest.raises(RuntimeError, match="unavailable"):
        aggregates(
            service,
            AggregatesRequest(fields=["url"], filters={"host": {"mode": "include", "values": ["example.com"]}}),
            *_WINDOW,
        )


def test_panel_flags_and_sections_selector():
    service, client = _recording_service()
    r_no_ts = aggregates(service, AggregatesRequest(fields=["url"], include_time_series=False), *_WINDOW)
    assert r_no_ts.time_series == [] and all("AS chart_bucket" not in sql for sql, _ in client.calls)

    client.calls.clear()
    r_no_map = aggregates(
        service, AggregatesRequest(fields=["url"], include_map_data=False, include_time_series=False), *_WINDOW
    )
    assert r_no_map.map_data == [] and not any(p.get("dimension") == "country" for _, p in client.calls)

    r_map_default = aggregates(service, AggregatesRequest(fields=["url"]), *_WINDOW)
    assert r_map_default.map_data and "country" not in r_map_default.data

    topten = aggregates(service, AggregatesRequest(fields=["url"], sections=["topten"]), *_WINDOW)
    assert topten.data and topten.time_series == [] and topten.map_data == []

    core = aggregates(service, AggregatesRequest(fields=["url"], sections=["core"]), *_WINDOW)
    assert core.data == {} and core.time_series and core.map_data


def test_bundle_top_bots_and_section_gating(monkeypatch):
    seen: list[tuple] = []

    def fake_top_bots(service, req, start_time, end_time):
        seen.append((service.service_id, start_time, end_time))
        return SecurityTopBotsResponse.with_telemetry(
            bots=[],
            ngwaf_bots=[
                {
                    "id": "googlebot",
                    "name": "Googlebot",
                    "category": "search",
                    "request_count": 5,
                    "verified_count": 5,
                    "impersonator_count": 0,
                    "unverified_count": 0,
                    "verification_coverage": 1.0,
                    "pending_count": 0,
                }
            ],
        )

    monkeypatch.setattr(hs_security, "top_bots", fake_top_bots)
    service, client = _recording_service()
    result = bundle(service, AggregatesRequest(fields=["url"]), *_WINDOW)
    assert (
        seen == [("svc", *_WINDOW)] and result["top_bots"].ngwaf_bots[0].id == "googlebot" and result["aggregates"].data
    )

    monkeypatch.setattr(hs_security, "top_bots", lambda *a, **k: pytest.fail("bots were not requested"))
    core_only = bundle(service, AggregatesRequest(fields=["url"], sections=["core"]), *_WINDOW)
    assert core_only["top_bots"] is None and core_only["aggregates"] is not None

    monkeypatch.setattr(
        hs_security, "top_bots", lambda *a, **k: SecurityTopBotsResponse.with_telemetry(bots=[], ngwaf_bots=[])
    )
    client.calls.clear()
    bots_only = bundle(service, AggregatesRequest(sections=["bots"]), *_WINDOW)
    assert bots_only["aggregates"] is None and bots_only["top_bots"] is not None and client.calls == []


# ── Origin Aggregates & Projections ──────────────────────────────────────────


class _OriginClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        self.calls.append((sql, params or {}))
        if "origin:summary" in sql:
            return [
                {
                    "requests": 10,
                    "misses": 6,
                    "passes": 2,
                    "origin_5xx": 1,
                    "status_count": 10,
                    "latency_q": [1000.0, 1500.0, 2500.0, 3000.0],
                    "latency_n": 10,
                    "ttlb_q": [2000.0, 4000.0],
                    "ttlb_n": 10,
                    "overhead_q": [500.0],
                    "overhead_n": 10,
                    "bytes_q": [128.0],
                    "bytes_n": 10,
                }
            ]
        if "origin:timeseries" in sql:
            return [{"time": "2026-09-14 10:00:00", "miss_count": 6, "value_q": [2500.0], "value_n": 6}]
        if "origin:slow_urls" in sql:
            return [{"value": "/slow", "requests": 5, "latency_q": [1000.0, 2500.0, 3000.0], "latency_n": 5}]
        if "origin:status_codes" in sql:
            return [{"value": "503", "requests": 2, "total_requests": 10}]
        if "origin:path_breakdown" in sql:
            return [{"value": "false", "requests": 4, "latency_q": [1200.0, 2400.0], "latency_n": 4}]
        if "origin:pop_latency" in sql:
            return [
                {"value": "SJC", "requests": 4, "latency_q": [1000.0, 1000.0], "latency_n": 4},
                {"value": "IAD", "requests": 3, "latency_q": [1200.0, 2000.0], "latency_n": 3},
                {"value": "LAX", "requests": 3, "latency_q": [1500.0, 5000.0], "latency_n": 3},
            ]
        if "origin:ip_health" in sql:
            return [
                {"value": "203.0.113.1", "requests": 7, "origin_5xx": 1, "latency_q": [1000.0, 3000.0], "latency_n": 7}
            ]
        raise AssertionError(sql)


class _OriginFactsClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        self.calls.append((sql, params or {}))
        if "origin:facts_summary" in sql:
            return [
                {
                    "requests": 10,
                    "misses": 6,
                    "passes": 2,
                    "origin_5xx": 1,
                    "status_count": 10,
                    "latency_q": [1000.0, 1500.0, 2500.0, 3000.0],
                    "ttlb_n": 10,
                    "ttlb_q": [2000.0, 4000.0],
                    "overhead_n": 10,
                    "overhead_q": 500.0,
                    "bytes_n": 10,
                    "bytes_q": 128.0,
                }
            ]
        if "origin:facts_slow_urls" in sql:
            return [{"value": "/slow", "requests": 5, "latency_q": [1000.0, 2500.0, 3000.0]}]
        if "origin:facts_status_codes" in sql:
            return [{"value": 503, "requests": 2}, {"value": 200, "requests": 8}]
        if "origin:facts_pop_latency" in sql:
            return [{"value": "SJC", "requests": 4, "origin_5xx": 0, "latency_n": 4, "latency_q": [1000.0, 2000.0]}]
        raise AssertionError(sql)


def _origin_request(**overrides):
    return SimpleNamespace(
        **{
            "filters": {},
            "bucket_minutes": 5,
            "split_by_leg": False,
            "timeseries_metric": "ttfb",
            "timeseries_percentile": "p95",
            "slow_urls_limit": 20,
            "slow_urls_min_requests": 1,
            "ip_health_limit": 30,
            "pop_latency_limit": 30,
            **overrides,
        }
    )


_ALL_ORIGIN_SECTIONS = {
    "summary",
    "timeseries",
    "slow_urls",
    "status_codes",
    "path_breakdown",
    "pop_latency",
    "ip_health",
}


def test_shapes_all_origin_sections_from_bounded_projection_queries() -> None:
    client = _OriginClient()
    service = SimpleNamespace(service_id="svc", client=client)
    result = origin_aggregates(
        service, _origin_request(), "2026-09-14T10:00:00Z", "2026-09-14T11:00:00Z", sections=_ALL_ORIGIN_SECTIONS
    )

    assert result["has_data"] is True
    assert result["summary"]["ottfb_p95_ms"] == 2.5 and result["summary"]["origin_error_rate"] == 0.1
    assert result["timeseries"]["series"][0]["value"] == 2.5 and result["slow_urls"]["rows"][0]["url"] == "/slow"
    assert result["status_codes"]["rows"][0] == {"status": 503, "count": 2, "pct": 20.0}
    assert (
        result["path_breakdown"]["shielding_detected"] is True and result["pop_latency"]["rows"][2]["elevated"] is True
    )
    assert result["ip_health"]["rows"][0]["error_pct"] == pytest.approx(14.285714)
    assert all(
        "high_scale_batch_publications FINAL" in sql and params["service_id"] == "svc" for sql, params in client.calls
    )
    assert next(p for s, p in client.calls if "origin:slow_urls" in s)["limit"] == 20
    dim_queries = [sql for sql, _ in client.calls if "FROM origin_minute_dimensions" in sql]
    assert dim_queries and all(
        "sum(origin_minute_dimensions.requests)" in s and "sum(requests)" not in s for s in dim_queries
    )
    assert "HAVING sum(origin_minute_dimensions.requests) >= 10" in next(
        s for s, _ in client.calls if "origin:ip_health" in s
    )

    for section in ("summary", "timeseries", "slow_urls", "path_breakdown", "pop_latency", "ip_health"):
        assert result[section]["_approx"] is True
        assert "histogram sketch" in result[section]["_approx_note"] and "1%" in result[section]["_approx_note"]
    assert "_approx" not in result["status_codes"]

    percentile_queries = [sql for sql, _ in client.calls if "_q" in sql or "quantiles" in sql]
    assert percentile_queries
    for sql in percentile_queries:
        assert "quantilesExactWeightedArray" in sql and "_sk_v" in sql and "_sk_w" in sql
        assert "latency_p50_us" not in sql and "latency_p95_us" not in sql and "* latency_count" not in sql


def test_filtered_requests_read_request_facts_exactly_instead_of_failing() -> None:
    client = _OriginFactsClient()
    service = SimpleNamespace(service_id="svc", client=client)
    req = _origin_request(filters={"country": FilterSpec(mode="include", values=["US"])})
    result = origin_aggregates(
        service, req, None, None, sections={"summary", "slow_urls", "status_codes", "pop_latency"}
    )

    sqls = [sql for sql, _ in client.calls]
    assert sqls and all(
        "FROM (SELECT" in sql and "request_facts" in sql and "origin_minute_" not in sql for sql in sqls
    )
    assert all(params["filter_0_0"] == "US" for _, params in client.calls)
    assert result["summary"]["_approx"] is False and result["slow_urls"]["_approx"] is False
    assert result["summary"]["ottfb_p95_ms"] == 2.5 and result["summary"]["origin_error_rate"] == 0.1
    assert [row["status"] for row in result["status_codes"]["rows"]] == [503, 200]
    assert result["status_codes"]["rows"][0]["pct"] == 20.0


def test_origin_filter_validation_section_gating_and_bucket_guards() -> None:
    client = _OriginClient()
    service = SimpleNamespace(service_id="svc", client=client)
    origin_aggregates(
        service,
        _origin_request(filters={"country": FilterSpec(mode="include", values=[])}),
        None,
        None,
        sections={"summary"},
    )
    assert "origin_minute_summary" in client.calls[0][0]

    client.calls.clear()
    res = origin_aggregates(service, _origin_request(), None, None, sections={"summary"})
    assert len(client.calls) == 1 and "summary" in res and "timeseries" not in res

    with pytest.raises(ValueError, match="_bot_name"):
        origin_aggregates(
            service,
            _origin_request(filters={"_bot_name": FilterSpec(mode="include", values=["x"])}),
            None,
            None,
            sections={"summary"},
        )

    with pytest.raises(ValueError, match="bucket_minutes"):
        origin_aggregates(service, _origin_request(bucket_minutes=0.5), None, None, sections={"timeseries"})


def test_origin_empty_sketch_observations_handling() -> None:
    class _NoSketch(_OriginClient):
        def execute(self, sql: str, params: dict | None = None) -> list[dict]:
            if "origin:slow_urls" in sql:
                return [{"value": "/legacy", "requests": 9, "latency_q": [0.0, 0.0, 0.0], "latency_n": 0}]
            if "origin:summary" in sql:
                return [
                    {
                        "requests": 4,
                        "misses": 0,
                        "passes": 0,
                        "origin_5xx": 0,
                        "status_count": 4,
                        "latency_q": [0.0] * 4,
                        "latency_n": 0,
                        "ttlb_q": [0.0, 0.0],
                        "ttlb_n": 0,
                        "overhead_q": [0.0],
                        "overhead_n": 0,
                        "bytes_q": [0.0],
                        "bytes_n": 0,
                    }
                ]
            return super().execute(sql, params)

    service = SimpleNamespace(service_id="svc", client=_NoSketch())
    res_slow = origin_aggregates(service, _origin_request(), None, None, sections={"slow_urls"})
    row = res_slow["slow_urls"]["rows"][0]
    assert row["requests"] == 9 and row["p50_ms"] is None and row["p95_ms"] is None and row["p99_ms"] is None

    res_sum = origin_aggregates(service, _origin_request(), None, None, sections={"summary"})
    assert res_sum["has_data"] is False and res_sum["summary"]["has_data"] is False


def test_builds_minute_summary_dimension_rows_and_mergeable_sketches():
    events = (
        {
            "timestamp": "2026-09-16T14:00:01Z",
            "cache": "MISS",
            "ost": 200,
            "ottfb": 1000,
            "ottlb": 1800,
            "elapsed": 2300,
            "obytes": 100,
            "url": "/a",
            "pop": "SJC",
            "oip": "192.0.2.10",
            "edge": True,
        },
        {
            "timestamp": "2026-09-16T14:00:40Z",
            "cache": "PASS",
            "ost": 503,
            "ttfb": 0.002,
            "ottlb": 2800,
            "elapsed": 3400,
            "obytes": 300,
            "url": "/a",
            "pop": "SJC",
            "oip": "192.0.2.10",
            "edge": False,
        },
        {
            "timestamp": "2026-09-16T14:01:00Z",
            "cache": "HIT",
            "ost": 829,
            "ottfb": "",
            "url": "/b",
            "pop": "IAD",
            "oip": "192.0.2.20",
            "edge": True,
        },
    )
    original = deepcopy(events)
    projection = build_origin_projection_rows(events)
    assert events == original
    (summary_row,) = projection.summary_rows
    for metric in ("latency", "ttlb", "overhead", "origin_bytes"):
        assert sum(summary_row[f"{metric}_sk_w"]) == 2
        assert len(summary_row[f"{metric}_sk_v"]) == len(summary_row[f"{metric}_sk_w"])
    for key in [k for k in summary_row if "_sk_" in k]:
        del summary_row[key]
    assert projection.summary_rows == (
        {
            "bucket_start": "2026-09-16T14:00:00+00:00",
            "requests": 2,
            "misses": 1,
            "passes": 1,
            "origin_5xx": 1,
            "status_count": 2,
            "origin_bytes": 400,
            "latency_count": 2,
            "ttlb_count": 2,
            "overhead_count": 2,
            "origin_bytes_count": 2,
            "latency_p50_us": 1500.0,
            "latency_p75_us": 1750.0,
            "latency_p95_us": 1950.0,
            "latency_p99_us": 1990.0,
            "ttlb_p50_us": 2300.0,
            "ttlb_p95_us": 2750.0,
            "cdn_overhead_p50_us": 550.0,
            "origin_bytes_p50": 200.0,
        },
    )
    url = next(row for row in projection.dimension_rows if row["dimension"] == "url" and row["value"] == "/a")
    assert url["requests"] == 2 and url["origin_5xx"] == 1 and url["latency_p50_us"] == 1500.0
    status = next(row for row in projection.dimension_rows if row["dimension"] == "status" and row["value"] == "-1")
    assert (
        status["requests"] == 1
        and status["latency_count"] == 0
        and status["latency_sk_v"] == []
        and status["latency_sk_w"] == []
    )

    # Identical observations merge into a single sketch bin of weight 2
    same_proj = build_origin_projection_rows((events[0], {**events[0], "timestamp": "2026-09-16T14:00:40Z"}))
    (same_sum,) = same_proj.summary_rows
    assert same_sum["latency_sk_w"] == [2] and same_sum["latency_sk_v"][0] == pytest.approx(1000.0, rel=0.01)

    # Invalid timestamps and NaN values are skipped
    bad_proj = build_origin_projection_rows(
        (
            {"timestamp": "invalid", "ottfb": 10},
            {"timestamp": "2026-09-16T14:00:00Z", "ottfb": float("nan"), "ost": "bad"},
        )
    )
    assert bad_proj.summary_rows == () and bad_proj.dimension_rows == ()


# ── Insights & Cache Collapse Detail ─────────────────────────────────────────

FINGERPRINT_DETECTIONS = {"botnet_grouping", "repeated_patterns_fp", "impossible_distance"}
EVERY_CARD = set(DETECTIONS) | FINGERPRINT_DETECTIONS
ALL_FIELDS = sorted({name for detection in DETECTIONS.values() for name in detection.columns} - {"signal"}) + [
    "waf_sig",
    "ja4",
    "oconnect_ms",
    "edge",
    "lat",
    "lon",
]


@pytest.fixture(autouse=True)
def _pop_coordinates(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "backend.utils.pop_utils.get_pop_lat_lon_map", lambda: {"SJC": (37.36, -121.93), "IAD": (38.94, -77.46)}
    )


class _InsightsClient:
    def __init__(
        self,
        rows: dict[str, list[dict[str, Any]]] | None = None,
        *,
        history_hours: float = 400.0,
        fields: list[str] | None = None,
        fail: set[str] | None = None,
    ) -> None:
        self.calls: list[tuple[str, dict]] = []
        self._rows = rows or {}
        self._history_hours = history_hours
        self._fields = ALL_FIELDS if fields is None else fields
        self._fail = fail or set()

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        params = params or {}
        self.calls.append((sql, params))
        marker = re.search(r"/\* insight:(\w+) \*/", sql)
        assert marker, sql
        name = marker.group(1)
        if name in self._fail:
            raise RuntimeError("clickhouse unavailable")
        if name == "history":
            return [{"earliest": params["window_end"] - timedelta(hours=self._history_hours), "window_total": 1234}]
        if name == "fields":
            return [{"fields": self._fields}]
        return self._rows.get(name, [])

    def queries(self) -> dict[str, tuple[str, dict]]:
        return {re.search(r"/\* insight:(\w+) \*/", sql).group(1): (sql, params) for sql, params in self.calls}  # type: ignore[union-attr]


def _run_insights(
    client: _InsightsClient, *, mask_ips: bool = False, window: float = 24.0, baseline: float = 168.0, **clamp: Any
):
    service = SimpleNamespace(service_id="svc", client=client)
    req = InsightsRequest(window_size_hrs=window, baseline_hours=baseline)
    return insights_endpoint(service, req, clamp.get("clamp_start"), clamp.get("clamp_end"), mask_ips)  # type: ignore[arg-type]


def _insight_cards(response) -> dict[str, dict]:
    return {card["id"]: card for card in json.loads(response.model_dump_json(by_alias=True))["insights"]}


def test_detections_registry_and_investigate_cards() -> None:
    assert callable(hs_insights.insights_endpoint) and callable(hs_insights.cache_collapse_detail_endpoint)
    registered = {definition.id: definition for definition in registry.get_all()}
    for insight_id, detection in DETECTIONS.items():
        assert insight_id in registered and registered[insight_id].row_processor is not None
        assert set(detection.columns) <= set(COLUMNS), insight_id
    assert botnet_grouping("ja3").columns == ("ip", "ja3")
    with pytest.raises(ValueError, match="ja4 or ja3"):
        botnet_grouping("x; DROP TABLE y")

    client = _InsightsClient(
        {
            "error_spikes": [
                {"url": "/api", "w_rate": 0.62, "b_rate": 0.01, "w_errors": 31, "w_total": 50, "b_total": 400},
                {"url": "/b", "w_rate": 0.10, "b_rate": 0.0, "w_errors": 5, "w_total": 50, "b_total": 400},
            ]
        }
    )
    cards = _insight_cards(_run_insights(client))
    card = cards["error_spikes"]
    assert (
        card["severity"] == "critical"
        and card["summary"] == "2 URLs with elevated server error rates"
        and card["category"] == "origin"
    )
    first = card["items"][0]
    assert first["label"] == "/api" and first["unit"] == "% 5xx"
    assert first["current_val"] == pytest.approx(62.0) and first["baseline_val"] == pytest.approx(1.0)
    assert first["meta"]["errors"] == 31 and first["meta"]["filters"] == {"url": "/api"}
    assert (
        first["investigate_url"].startswith("/dashboard?service=svc&start=")
        and "filter_url=%2Fapi" in first["investigate_url"]
    )
    assert (
        cards["latency_regression"]["severity"] == "clean"
        and cards["latency_regression"]["summary"] == "No latency regression detected"
    )


def test_every_detection_runs_and_query_is_visible_scoped_and_parameterized() -> None:
    client = _InsightsClient()
    response = _run_insights(client)
    cards = _insight_cards(response)
    assert set(cards) == EVERY_CARD == {definition.id for definition in registry.get_all()}
    assert all(card["severity"] == "clean" for card in cards.values())
    assert json.loads(response.model_dump_json(by_alias=True))["window_hours"] == 24.0

    queries = client.queries()
    for insight_id in EVERY_CARD:
        sql, params = queries[insight_id]
        assert (
            "FROM request_facts" in sql
            and "service_id={service_id:String}" in sql
            and "publication_state='visible'" in sql
        )
        assert "high_scale_batch_publications FINAL" in sql and "domain='request'" in sql
        assert "event_timestamp <= {window_end:DateTime64(3)}" in sql
        window_only = (insight_id in DETECTIONS and DETECTIONS[insight_id].window_only) or insight_id in {
            "repeated_patterns_fp",
            "impossible_distance",
        }
        lower = "{window_start:DateTime64(3)}" if window_only else "{baseline_start:DateTime64(3)}"
        if insight_id != "botnet_grouping":
            assert f"event_timestamp >= {lower}" in sql, insight_id
        assert (
            params["service_id"] == "svc" and params["baseline_start"] < params["window_start"] < params["window_end"]
        )

    for sql, params in client.calls:
        assert "svc" not in sql
        assert re.findall(r"\{(\w+):", sql) and all(name in params for name in re.findall(r"\{(\w+):", sql))


def test_insufficient_history_and_missing_fields_or_pop_coords(monkeypatch: pytest.MonkeyPatch) -> None:
    client_short = _InsightsClient(history_hours=5.0)
    cards_short = _insight_cards(_run_insights(client_short))
    assert set(cards_short) == EVERY_CARD
    for insight_id, card in cards_short.items():
        if insight_id == "impossible_distance":
            assert card["severity"] == "clean"
        else:
            assert (
                card["severity"] == "info"
                and card["summary"] == "Requires 168h of historical data (only 5.0h available)"
                and card["items"] == []
            )
    assert {re.search(r"insight:(\w+)", sql).group(1) for sql, _ in client_short.calls} == {
        "history",
        "fields",
        "impossible_distance",
    }  # type: ignore[union-attr]

    # Omitted latency fields
    client_no_lat = _InsightsClient(fields=[n for n in ALL_FIELDS if n not in {"elapsed", "ottfb"}])
    cards_no_lat = _insight_cards(_run_insights(client_no_lat))
    for omitted in ("latency_regression", "tail_latency", "pop_latency_regression", "origin_latency_spike"):
        assert omitted not in cards_no_lat and omitted not in client_no_lat.queries()
    assert "error_spikes" in cards_no_lat

    # Omitted TLS fingerprint vs JA3 fallback
    client_no_fp = _InsightsClient(fields=[n for n in ALL_FIELDS if n != "ja4"])
    assert not (FINGERPRINT_DETECTIONS & set(_insight_cards(_run_insights(client_no_fp))))
    assert "ja4 != ''" in client_no_lat.queries()["botnet_grouping"][0]
    client_ja3 = _InsightsClient(fields=[n for n in ALL_FIELDS if n != "ja4"] + ["ja3"])
    _run_insights(client_ja3)
    assert "ja3 != ''" in client_ja3.queries()["botnet_grouping"][0]

    # Impossible distance POP code sanitization and empty POP map
    monkeypatch.setattr(
        "backend.utils.pop_utils.get_pop_lat_lon_map",
        lambda: {"SJC": (37.36, -121.93), "SJC'; DROP TABLE x": (1.0, 2.0)},
    )
    client_pop = _InsightsClient()
    _run_insights(client_pop)
    pop_sql = client_pop.queries()["impossible_distance"][0]
    assert "'SJC'" in pop_sql and "DROP TABLE" not in pop_sql

    monkeypatch.setattr("backend.utils.pop_utils.get_pop_lat_lon_map", dict)
    assert "impossible_distance" not in _insight_cards(_run_insights(_InsightsClient()))


def test_insights_failures_masking_asn_and_window_clamping(monkeypatch: pytest.MonkeyPatch) -> None:
    client_fail = _InsightsClient(
        {"error_spikes": [{"url": "/x", "w_rate": 0.9, "b_rate": 0.0, "w_errors": 9, "w_total": 10, "b_total": 99}]},
        fail={"latency_regression"},
    )
    cards = _insight_cards(_run_insights(client_fail))
    assert (
        cards["latency_regression"]["severity"] == "error"
        and cards["latency_regression"]["summary"] == "Query failed: RuntimeError"
    )
    assert cards["latency_regression"]["items"] == [] and cards["error_spikes"]["severity"] == "critical"

    with pytest.raises(RuntimeError, match="unavailable"):
        _run_insights(_InsightsClient(fail={"history"}))

    # Analyst IP masking
    rows = {
        "request_size_anomaly": [
            {"ip": "203.0.113.77", "max_bytes": 9000, "avg_bytes": 4000.0, "w_total": 8, "b_p95": 1000.0}
        ]
    }
    plain = _insight_cards(_run_insights(_InsightsClient(rows)))["request_size_anomaly"]["items"][0]
    masked = _insight_cards(_run_insights(_InsightsClient(rows), mask_ips=True))["request_size_anomaly"]["items"][0]
    assert "203.0.113.77" in json.dumps(plain) and "203.0.113.77" not in json.dumps(masked)

    # ASN ISP resolution
    seen: list[tuple[str, list]] = []
    monkeypatch.setattr(
        "backend.core.duckdb.get_asn_names", lambda sid, asns: (seen.append((sid, list(asns))), {7922: "Comcast"})[1]
    )
    asn_rows = [{"asn": 7922, "w_cnt": 90, "b_cnt": 5, "b_total": 1000, "w_total": 120}]
    asn_card = _insight_cards(_run_insights(_InsightsClient({"asn_concentration": asn_rows})))["asn_concentration"]
    assert (
        seen == [("svc", [7922])]
        and asn_card["items"][0]["label"] == "AS7922 (Comcast)"
        and asn_card["severity"] == "critical"
    )

    # Clamp bounds & window parameters
    now = datetime.now(UTC)
    client_clamp = _InsightsClient()
    _run_insights(
        client_clamp,
        clamp_start=(now - timedelta(hours=30)).isoformat(),
        clamp_end=(now - timedelta(hours=2)).isoformat(),
    )
    _, h_params = client_clamp.queries()["history"]
    assert h_params["window_end"] <= now - timedelta(hours=2) + timedelta(seconds=1)
    assert h_params["baseline_start"] >= now - timedelta(hours=30) - timedelta(seconds=1)
    assert h_params["window_start"] >= h_params["baseline_start"]

    client_win = _InsightsClient()
    _run_insights(client_win, window=6.0, baseline=48.0)
    _, w_params = client_win.queries()["error_spikes"]
    assert w_params["window_hours"] == 6.0 and w_params["baseline_hours"] == 48.0
    assert w_params["window_end"] - w_params["window_start"] == pytest.approx(
        timedelta(hours=6), abs=timedelta(seconds=1)
    )
    assert w_params["window_start"] - w_params["baseline_start"] == pytest.approx(
        timedelta(hours=48), abs=timedelta(seconds=1)
    )


class _InsightsDetailClient:
    def __init__(self, *, empty: bool = False) -> None:
        self.calls: list[tuple[str, dict]] = []
        self._empty = empty

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        self.calls.append((sql, params or {}))
        if "cache_collapse_rates" in sql:
            zeros = dict.fromkeys(("w_hits", "w_miss", "w_pass", "w_total", "b_hits", "b_miss", "b_pass", "b_total"), 0)
            return (
                [zeros]
                if self._empty
                else [
                    {
                        **zeros,
                        "w_hits": 10,
                        "w_miss": 30,
                        "w_pass": 10,
                        "w_total": 55,
                        "b_hits": 90,
                        "b_miss": 10,
                        "b_pass": 5,
                        "b_total": 120,
                    }
                ]
            )
        if "cache_collapse_timeline" in sql:
            return (
                []
                if self._empty
                else [
                    {"bucket": "2026-09-14 10:00:00", "total_requests": 40, "real_hits": 10, "misses": 30},
                    {"bucket": datetime(2026, 9, 14, 11, tzinfo=UTC), "total_requests": 4, "real_hits": 0, "misses": 0},
                ]
            )
        if "cache_collapse_misses" in sql:
            return (
                []
                if self._empty
                else [
                    {
                        "timestamp": "2026-09-14 10:59:00.000",
                        "cache": "MISS",
                        "pop": "SJC",
                        "ip": "203.0.113.9",
                        "status": 200,
                    },
                    {
                        "timestamp": datetime(2026, 9, 14, 10, 58, tzinfo=UTC),
                        "cache": "",
                        "pop": "",
                        "ip": "",
                        "status": None,
                    },
                ]
            )
        raise AssertionError(sql)


def _collapse_detail(client: Any, *, mask_ips: bool = False, window: float = 24.0, baseline: float = 168.0):
    req = CacheCollapseDetailRequest(url="/hot", window_size_hrs=window, baseline_hours=baseline)
    service = SimpleNamespace(service_id="svc", client=client)
    return json.loads(cache_collapse_detail_endpoint(service, req, None, None, mask_ips).model_dump_json(by_alias=True))  # type: ignore[arg-type]


def test_cache_collapse_detail_timeline_misses_scoping_and_empty() -> None:
    client = _InsightsDetailClient()
    result = _collapse_detail(client)
    assert result["url"] == "/hot" and result["has_data"] is True
    assert result["breakdown"] == {"hits": 10, "misses": 30, "passes": 10, "other": 5}
    assert result["window_hit_rate"] == pytest.approx(25.0) and result["baseline_hit_rate"] == pytest.approx(90.0)
    assert result["window_pass_rate"] == pytest.approx(10 / 55 * 100) and result["baseline_pass_rate"] == pytest.approx(
        5 / 120 * 100
    )
    first = result["timeline"][0]
    assert (
        first["bucket"] == "2026-09-14T10:00:00Z"
        and first["expected_hits"] == pytest.approx(40 * 0.9)
        and first["hit_rate"] == pytest.approx(0.25)
    )
    assert (
        result["timeline"][1]["hit_rate"] == 0.0
        and result["timeline"][1]["bucket"] == "2026-09-14T11:00:00Z"
        and result["clusters"] == []
    )

    plain = result["recent_misses"]
    masked = _collapse_detail(_InsightsDetailClient(), mask_ips=True)["recent_misses"]
    assert plain[0] == {
        "timestamp": "2026-09-14T10:59:00.000Z",
        "cache": "MISS",
        "pop": "SJC",
        "ip": "203.0.113.9",
        "status": 200,
    }
    assert plain[1]["cache"] == "UNKNOWN" and plain[1]["status"] is None and plain[1]["pop"] is None
    assert masked[0]["ip"] != "203.0.113.9"

    assert len(client.calls) == 3
    for sql, params in client.calls:
        assert "url = {url:String}" in sql and params["url"] == "/hot"
        assert "publication_state='visible'" in sql and "high_scale_batch_publications FINAL" in sql
        assert "event_timestamp >= {baseline_start:DateTime64(3)}" in sql and params["service_id"] == "svc"

    empty_res = _collapse_detail(_InsightsDetailClient(empty=True))
    assert empty_res["has_data"] is False and empty_res["timeline"] == [] and empty_res["recent_misses"] == []
    assert empty_res["breakdown"] == {"hits": 0, "misses": 0, "passes": 0, "other": 0}


@pytest.mark.parametrize(
    ("window", "baseline", "interval"),
    [
        (1.0, 1.0, "INTERVAL 1 MINUTE"),
        (6.0, 6.0, "INTERVAL 5 MINUTE"),
        (24.0, 96.0, "INTERVAL 1 HOUR"),
        (24.0, 168.0, "INTERVAL 4 HOUR"),
    ],
)
def test_cache_collapse_detail_bucket_size_follows_the_lookback(window: float, baseline: float, interval: str) -> None:
    client = _InsightsDetailClient()
    _collapse_detail(client, window=window, baseline=baseline)
    assert interval in next(sql for sql, _ in client.calls if "cache_collapse_timeline" in sql)
