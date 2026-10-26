from __future__ import annotations

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
from backend.high_scale._common import KeysetCursor, request_filter_clauses
from backend.high_scale.aggregate_query import (
    AggregateRequest,
    query_clickhouse_aggregate,
    query_clickhouse_aggregates_batch,
)
from backend.high_scale.archive_models import ServingWatermark
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
from backend.models.network import NetworkBundleRequest, NetworkHealthRequest, NetworkQualityRequest
from backend.models.performance import PerformanceAggregatesResponse

from backend.high_scale import dashboard, network, origin, security
from backend.high_scale import performance as hs_performance
from backend.high_scale import sessions as hs_sessions
from backend.high_scale.dashboard import aggregates, bundle
from backend.high_scale.historical_jobs import HistoricalJob, HistoricalJobManager, HistoricalJobState
from backend.high_scale.network import (
    get_pop_health,
    get_transport_metrics,
    network_bundle,
    network_health,
    network_quality,
)
from backend.high_scale.query_service import query_cmcd_facts, query_request_facts, query_rum_facts
from backend.models.common import FilterSpec
from backend.utils.telemetry import get_page_load_id, set_page_load_id
from tests.high_scale import test_cmcd as cmcd_cases
from tests.high_scale import test_dashboard as dashboard_cases
from tests.high_scale import test_security as security_cases

from backend.core.high_scale_sections import current_clickhouse_scope  # isort: skip
from backend.core.query_attribution import Attribution, current_attribution  # isort: skip


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
            _Client([]),
            service_id="svc",
            start=t0,
            end=t1,
            cursor_secret=b"secret",
            watermark=_watermark(),
            cursor=bad_cursor,
        )

    for end_hour, limit, match in ((20, 100, "query end"), (21, 501, "between")):
        with pytest.raises(ValueError, match=match):
            query_request_facts(
                _Client([]),
                service_id="svc",
                start=t0,
                end=datetime(2026, 9, 11, end_hour, 0, tzinfo=UTC),
                cursor_secret=b"secret",
                watermark=_watermark(),
                limit=limit,
            )

    rum_client = _Client([_row(1, "event-1")])
    query_rum_facts(
        rum_client,
        service_id="svc",
        domain="rum_vitals",
        start=t0,
        end=t1,
        cursor_secret=b"secret",
        watermark=_watermark("rum_vitals"),
    )
    assert "FROM rum_vitals_facts" in rum_client.sql

    cmcd_client = _Client([_row(1, "projection-1")])
    query_cmcd_facts(
        cmcd_client, service_id="svc", start=t0, end=t1, cursor_secret=b"secret", watermark=_watermark("cmcd")
    )
    assert "FROM cmcd_projection_facts" in cmcd_client.sql and "projection_key AS event_id" in cmcd_client.sql


# ── Merged from test_query_router.py ──

NOW = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)


def request(*, hours_old: int, hours: int = 1, intent: QueryIntent = QueryIntent.RAW) -> QueryRequest:
    end = NOW - timedelta(hours=hours_old)
    return QueryRequest("svc", "request", end - timedelta(hours=hours), end, intent)


@pytest.mark.parametrize(
    ("hours_old", "hours", "intent", "tier", "sources", "queued", "bounded", "max_rows"),
    [
        (2, 1, QueryIntent.TRIAGE, QueryTier.TRIAGE_AGGREGATE, ("aggregates",), False, False, None),
        (2, 1, QueryIntent.RAW, QueryTier.RECENT_FACTS, ("clickhouse_facts",), False, False, None),
        (
            24 * 10,
            1,
            QueryIntent.RAW,
            QueryTier.WARM_FACTS_ARCHIVE,
            ("clickhouse_facts", "fos_archive"),
            False,
            False,
            None,
        ),
        (24 * 45, 2, QueryIntent.RAW, QueryTier.COLD_JOB, ("queued_query_job",), True, True, 500),
    ],
)
def test_query_router_tier_selection(hours_old, hours, intent, tier, sources, queued, bounded, max_rows) -> None:
    plan = QueryRouter().plan(request(hours_old=hours_old, hours=hours, intent=intent), now=NOW)
    assert plan.tier is tier and plan.sources == sources
    assert plan.queued is queued and plan.bounded is bounded
    if max_rows is not None:
        assert plan.max_rows == max_rows


def test_cold_range_has_a_hard_bound() -> None:
    with pytest.raises(ValueError, match="cold query range"):
        QueryRouter().plan(request(hours_old=24 * 45, hours=24 * 8), now=NOW)


@pytest.mark.parametrize(
    ("request_value", "message"),
    [
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


def test_invalid_router_bounds_are_rejected() -> None:
    with pytest.raises(ValueError, match="warm retention"):
        QueryRouter(QueryRouterConfig(warm_retention=timedelta(hours=1)))


# ── Merged from test_request_filters.py ──


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

    assert request_filter_clauses(
        {"country": {"mode": "include", "values": ["", "  "]}, "pop": {"mode": "include", "values": []}}
    ) == ([], {})
    assert request_filter_clauses(None) == ([], {}) and request_filter_clauses({}) == ([], {})

    include, _ = request_filter_clauses({"pop": {"mode": "include", "values": [None, "SJC"]}})
    exclude, _ = request_filter_clauses({"pop": {"mode": "exclude", "values": [None]}})
    assert include == ["(custom_fields[{flt_k0:String}] IN ({flt_0_0:String}) OR custom_fields[{flt_k0:String}] = '')"]
    assert exclude == ["(custom_fields[{flt_k0:String}] != '')"]

    f_clauses, f_params = request_filter_clauses({"ttl": {"mode": "include", "values": [3600]}})
    assert f_clauses == [
        "(toString(toInt32(round(toFloat64OrZero(custom_fields[{flt_k0:String}])))) IN ({flt_0_0:String}))"
    ]
    assert f_params["flt_k0"] == "ttl"


@pytest.mark.parametrize("key", ["_bot_name", "_ngwaf_bot_name", "_tunnel_requests", "waf_sig_ind"])
def test_filters_the_fact_table_cannot_express_raise(key: str) -> None:
    with pytest.raises(ValueError, match="do not support filtering"):
        request_filter_clauses({key: {"mode": "include", "values": ["x"]}})


# ── Merged from test_high_scale_response_cache.py ──


def test_high_scale_cache_observations_do_not_execute_the_repeat() -> None:
    perf_client = _PerfClient()
    first_perf = _run_perf(perf_client, sections={"waterfall"})
    perf_count = len(perf_client.calls)
    hit_perf = _run_perf(perf_client, sections={"waterfall"})
    assert perf_count > 0 and first_perf.response_cache.state == "miss"
    assert (
        hit_perf.response_cache.state == "hit" and hit_perf.is_cached is True and len(perf_client.calls) == perf_count
    )

    sec_client = security_cases._Client()
    sec_svc, sec_req = security_cases._service(sec_client), security_cases._req()
    first_sec = security.security_aggregates(
        sec_svc, sec_req, security_cases.START, security_cases.END, sections={"ipv6_adoption"}
    )
    sec_count = len(sec_client.calls)
    hit_sec = security.security_aggregates(
        sec_svc, sec_req, security_cases.START, security_cases.END, sections={"ipv6_adoption"}
    )
    assert sec_count > 0 and first_sec.response_cache.state == "miss"
    assert hit_sec.response_cache.state == "hit" and hit_sec.is_cached is True and len(sec_client.calls) == sec_count

    cmcd_client = cmcd_cases._Client()
    first_cmcd = cmcd_cases._run(cmcd_client, sections={"overview"})
    cmcd_count = len(cmcd_client.calls)
    hit_cmcd = cmcd_cases._run(cmcd_client, sections={"overview"})
    assert cmcd_count > 0 and first_cmcd["_response_cache"]["state"] == "miss"
    assert hit_cmcd["_response_cache"]["state"] == "hit" and len(cmcd_client.calls) == cmcd_count

    dash_svc, dash_client = dashboard_cases._recording_service()
    dash_req = AggregatesRequest(fields=["url"], include_time_series=False)
    first_dash = dashboard.aggregates(dash_svc, dash_req, "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z")
    dash_count = len(dash_client.calls)
    hit_dash = dashboard.aggregates(dash_svc, dash_req, "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z")
    assert dash_count > 0 and first_dash.model_dump(by_alias=True)["_response_cache"]["state"] == "miss"
    assert hit_dash.model_dump(by_alias=True)["_response_cache"]["state"] == "hit" and hit_dash.is_cached is True
    assert len(dash_client.calls) == dash_count


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
    service, client = dashboard_cases._recording_service()
    req = AggregatesRequest(
        fields=["url"],
        include_time_series=False,
        include_map_data=False,
        filters={"country": {"mode": "include", "values": ["US"]}} if filtered else {},
    )
    dashboard.aggregates(service, req, "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z")
    previous_calls = len(client.calls)

    changed = dashboard.aggregates(service, req, start, end)
    assert changed.response_cache.state == "miss" and len(client.calls) > previous_calls
    changed_calls = len(client.calls)
    repeat = dashboard.aggregates(service, req, start, end)
    assert repeat.response_cache.state == "hit" and len(client.calls) == changed_calls


# ── Merged from test_section_attribution.py ──


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
        timings = origin.aggregates(service, req, start, end, sections={"status_codes"})["section_timings"]
        source = "projection"
    elif reader == "performance":
        timings = hs_performance.performance_aggregates(service, req, start, end, sections={"ttl_dist"}).section_timings
        source = "projection"
    else:
        timings = security.security_aggregates(service, req, start, end, sections={"req_size_dist"}).section_timings
        source = "fact"
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
    client = _ScopeClient()
    service = SimpleNamespace(service_id="service-test", client=client)
    bot_resp = security.top_bots(service, SimpleNamespace(filters={}), "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z")
    assert bot_resp.section_timings[0]["source"] == "mixed"
    assert [scope["source"] for scope in client.scopes] == ["fact", "projection"]
    assert len({scope["section_id"] for scope in client.scopes}) == 1

    net_client = _ScopeClient()
    net_service = SimpleNamespace(service_id="service-test", client=net_client)
    net_resp = network.network_bundle(
        net_service, NetworkBundleRequest(sections=["pop_health"]), "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"
    )
    assert net_resp.pop_health == [] and len(net_client.scopes) == 1 and len(net_resp.section_timings) == 1
    assert net_resp.section_timings[0]["section_id"] == net_client.scopes[0]["section_id"]
    assert net_resp.section_timings[0]["section"] == "network.pop_health"


# ── Merged from test_historical_jobs.py ─────────────────────────────────────


def cold_plan(*, max_rows: int = 3) -> QueryPlan:
    return QueryPlan(
        service_id="svc",
        domain="request",
        intent=QueryIntent.RAW,
        tier=QueryTier.COLD_JOB,
        start=NOW - timedelta(days=2),
        end=NOW - timedelta(days=1),
        sources=("queued_query_job",),
        queued=True,
        bounded=True,
        max_rows=max_rows,
        reason="test",
    )


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


def test_cancellation_and_expiry_are_observed_during_iteration() -> None:
    manager = HistoricalJobManager(max_output_bytes=10_000)
    started, release = Event(), Event()

    def rows():
        started.set()
        yield {"id": 1}
        release.wait(2)
        yield {"id": 2}

    job_id = manager.submit(cold_plan(max_rows=10), rows)
    assert started.wait(1) and manager.cancel(job_id) is True
    release.set()
    assert wait_for(manager, job_id, HistoricalJobState.CANCELLED).rows == ({"id": 1},)
    manager.shutdown()

    clock = [NOW]
    manager2 = HistoricalJobManager(ttl=timedelta(seconds=1), max_output_bytes=10_000, clock=lambda: clock[0])
    started2, release2 = Event(), Event()

    def rows2():
        started2.set()
        yield {"id": 1}
        release2.wait(2)
        yield {"id": 2}

    job_id2 = manager2.submit(cold_plan(max_rows=10), rows2, now=clock[0])
    assert started2.wait(1)
    clock[0] += timedelta(seconds=2)
    release2.set()
    assert wait_for(manager2, job_id2, HistoricalJobState.EXPIRED).rows == ({"id": 1},)
    manager2.shutdown()


def test_output_byte_limit_purging_and_plan_validation() -> None:
    manager = HistoricalJobManager(max_output_bytes=20)
    job = wait_for(
        manager, manager.submit(cold_plan(max_rows=10), lambda: [{"value": "x" * 100}]), HistoricalJobState.FAILED
    )
    assert job.rows == () and job.error == "historical job output exceeds byte limit"
    manager.shutdown()

    manager2 = HistoricalJobManager(terminal_grace_ttl=timedelta(seconds=1))
    job_id = manager2.submit(cold_plan(), lambda: [])
    completed = wait_for(manager2, job_id, HistoricalJobState.COMPLETED)
    assert completed.terminal_at is not None
    assert manager2.get(job_id, now=completed.terminal_at + timedelta(seconds=2)) is None
    manager2.shutdown()

    manager3 = HistoricalJobManager()
    not_cold = QueryPlan(
        service_id="svc",
        domain="request",
        intent=QueryIntent.RAW,
        tier=QueryTier.RECENT_FACTS,
        start=NOW - timedelta(days=2),
        end=NOW - timedelta(days=1),
        sources=("clickhouse_facts",),
        queued=False,
        bounded=False,
        max_rows=1,
        reason="not cold",
    )
    with pytest.raises(ValueError, match="cold"):
        manager3.submit(not_cold, lambda: [])
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
    _run_sess(
        client,
        filters={"country": {"mode": "include", "values": ["US"]}, "pop": {"mode": "exclude", "values": ["SJC"]}},
    )
    sql, params = client.sql("sessions:page")
    assert "country IN ({flt_0_0:String})" in sql and "custom_fields[{flt_k1:String}] NOT IN ({flt_1_0:String})" in sql
    assert params["flt_0_0"] == "US" and params["flt_k1"] == "pop" and params["flt_1_0"] == "SJC"
    assert "publication_state='visible'" in sql and "high_scale_batch_publications FINAL" in sql
    assert "domain='request'" in sql and "event_timestamp >= {start_time:DateTime64(3)}" in sql
    assert params["service_id"] == "svc"
    assert "lower(edge) IN ('1', 'true')" in sql and "lower(edge) IN ('0', 'false')" in sql

    c_sort = _SessClient()
    _run_sess(c_sort, sort_by="req_count", sort_dir="asc")
    assert "ORDER BY req_count ASC" in c_sort.sql("sessions:page")[0]

    c_bad_sort = _SessClient()
    _run_sess(c_bad_sort, sort_by="1; DROP TABLE x")
    bad_sql, _ = c_bad_sort.sql("sessions:page")
    assert "ORDER BY session_start DESC" in bad_sql and "DROP TABLE" not in bad_sql

    c_page = _SessClient()
    _run_sess(c_page, page=3, limit=25)
    page_sql, page_params = c_page.sql("sessions:page")
    assert "LIMIT {limit:UInt32} OFFSET {offset:UInt32}" in page_sql
    assert page_params["limit"] == 25 and page_params["offset"] == 50

    c_flags = _SessClient()
    result = _run_sess(c_flags)
    _, flag_params = c_flags.sql("sessions:page")
    assert flag_params["min_reqs_flag"] == 1000 and flag_params["min_4xx_pct_flag"] == 20.0
    assert result["min_reqs_flag"] == 1000 and result["min_4xx_pct_flag"] == 20.0
    custom = _run_sess(_SessClient(), min_reqs_flag=50, min_4xx_pct_flag=5.5)
    assert custom["min_reqs_flag"] == 50 and custom["min_4xx_pct_flag"] == 5.5

    c_stream = _SessClient()
    _run_sess(c_stream, streaming_only=True)
    stream_sql, stream_params = c_stream.sql("sessions:page")
    assert "cmcd['sid']" in stream_sql and "custom_fields['cmcd_sid']" not in stream_sql
    assert stream_params.get("streaming_only") == 1


def test_session_totals_empty_pages_availability_and_cleaning() -> None:
    res_limit = _run_sess(_SessClient(), limit=1)
    assert res_limit["total"] == 321 and res_limit["page"] == 1 and res_limit["limit"] == 1

    c_past = _SessClient(rows=[], total=40)
    res_past = _run_sess(c_past, page=9, limit=5)
    assert res_past["sessions"] == [] and res_past["total"] == 40
    assert "sessions:count" in " ".join(sql for sql, _ in c_past.calls)

    c_empty = _SessClient(rows=[])
    res_empty = _run_sess(c_empty)
    assert res_empty["total"] == 0 and all("sessions:count" not in sql for sql, _ in c_empty.calls)

    present = _run_sess(_SessClient())
    absent = _run_sess(
        _SessClient(
            rows=[_session_row(any_rtt=0, any_ja4=0, any_edge=0, any_edge_sid=0, any_cmcd=0, median_rtt_ms=None)]
        )
    )
    flags = ("has_rtt", "has_ja4", "has_edge", "has_edge_sid", "has_cmcd")
    assert tuple(present[k] for k in flags) == (True,) * 5
    assert tuple(absent[k] for k in flags) == (False,) * 5

    (row,) = present["sessions"]
    assert row["session_start"] == "2026-09-15T10:00:00Z" and row["session_end"] == "2026-09-15T10:20:00.000Z"
    assert row["flagged"] is True and row["is_streaming"] is True
    for helper in (
        "sid",
        "total_sessions",
        "any_rtt",
        "any_ja4",
        "any_edge",
        "any_edge_sid",
        "any_cmcd",
        "streaming_reqs",
    ):
        assert helper not in row
    assert present["asn_names"] == {7922: "AS7922 Name"}

    model = SessionsResponse.model_validate(present)
    assert model.total == 321 and model.sessions[0].asn == 7922 and model.asn_names[7922] == "AS7922 Name"

    with pytest.raises(RuntimeError, match="unavailable"):
        _run_sess(_SessClient(fail=True))


def test_detail_and_two_phase_session_enrichment() -> None:
    client = _SessClient()
    client.execute = lambda sql, params=None: client.calls.append((sql, params or {})) or []  # type: ignore[method-assign]
    sessions_detail(
        SimpleNamespace(service_id="svc", client=client),  # type: ignore[arg-type]
        SessionDetailRequest(),
        _SESS_START,
        _SESS_END,
        "203.0.113.7",
        "ja4-x",
    )
    sql, params = client.calls[0]
    assert "publication_state='visible'" in sql and "high_scale_batch_publications FINAL" in sql
    assert "client_ip = {ip:String}" in sql and "custom_fields['ja4'] = {ja4:String}" in sql
    assert params["ip"] == "203.0.113.7" and params["ja4"] == "ja4-x"

    fast_row = _session_row()
    for k in ("asn", "country", "unique_urls", "ua", "median_rtt_ms", "edge_sid"):
        fast_row.pop(k, None)
    calls: list[tuple[str, dict]] = []

    class _TwoPhaseClient:
        def execute(self, sql: str, params: dict | None = None) -> list[dict]:
            calls.append((sql, params or {}))
            if "sessions:page" in sql:
                return [fast_row]
            if "sessions:enrich" in sql:
                return [{"row_idx": 0, "asn": 15169, "country": "DE", "unique_urls": 77, "ua": "Mozilla/5.0", "median_rtt_ms": 8.25, "edge_sid": "es-fast"}]  # fmt: skip
            raise AssertionError(sql)

    res = sessions_endpoint(
        SimpleNamespace(service_id="svc", client=_TwoPhaseClient()),  # type: ignore[arg-type]
        SessionsRequest(sort_by="req_count", sort_dir="desc"),
        _SESS_START,
        _SESS_END,
    )
    assert len(calls) == 2
    page_sql, _ = calls[0]
    enrich_sql, enrich_params = calls[1]
    assert "uniqState" not in page_sql and "medianState" not in page_sql
    assert "sessions:enrich" in enrich_sql and enrich_params["en_ip_0"] == "203.0.113.7"
    s0 = res["sessions"][0]
    assert (s0["asn"], s0["country"], s0["unique_urls"], s0["ua"], s0["median_rtt_ms"], s0["edge_sid"]) == (
        15169, "DE", 77, "Mozilla/5.0", 8.25, "es-fast"
    )  # fmt: skip


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
        )
        if batched
        else {
            "url": query_clickhouse_aggregate(
                service.client, AggregateRequest("svc", "request", start, end, "url"), watermark=watermark
            )
        }
    )
    for response in responses.values():
        assert response.request_count == 3 and sum(count for _, count in response.top_values) == 3


# ── Merged from tests/high_scale/test_performance.py ──

_PERF_START = "2026-09-16T14:00:00Z"
_PERF_END = "2026-09-16T15:00:00Z"
_PERCENTILE_ROW = {"value": "/slow", "req_count": 9, "avg_ms": 12.5, "p50_ms": 10.0, "p95_ms": 20.0, "p99_ms": 30.0}


class _PerfClient:
    def __init__(self, answers: dict[str, list[dict]] | None = None) -> None:
        self.answers = answers or {}
        self.calls: list[tuple[str, dict]] = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params or {}))
        marker = sql.split("/* perf:", 1)[1].split(" */", 1)[0]
        return self.answers.get(marker, [])

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
    return hs_performance.performance_aggregates(
        _perf_service(client), req or _perf_req(), start, end, sections=sections
    )


def test_waterfall_and_scatter_queries() -> None:
    wf_row = {"component_count": 4, "edge_processing": 12.5, "origin_wait": 40.0, "origin_download": 5.0, "client_download": 9.25}  # fmt: skip
    client = _PerfClient({"waterfall": [wf_row]})
    res = _run_perf(client, sections={"waterfall", "scatter"})
    assert res.waterfall.avg is not None
    assert res.waterfall.avg.model_dump() == {"edge_processing": 12.5, "origin_wait": 40.0, "origin_download": 5.0, "client_download": 9.25}  # fmt: skip
    sql, params = client.sql_for("waterfall")
    assert "FROM performance_minute_summary" in sql and "AS component_count" not in sql
    assert "sum(edge_processing_sum_ms) / nullIf(sum(component_count), 0)" in sql
    assert "publication_state='visible'" in sql and params["service_id"] == "svc"

    zero_row = {"component_count": 0, "edge_processing": None, "origin_wait": None, "origin_download": None, "client_download": None}  # fmt: skip
    res_zero = _run_perf(_PerfClient({"waterfall": [zero_row]}), sections={"waterfall"})
    assert res_zero.waterfall.avg is not None
    assert res_zero.waterfall.avg.model_dump() == dict.fromkeys(
        ("edge_processing", "origin_wait", "origin_download", "client_download"), 0.0
    )

    sc_client = _PerfClient(
        {
            "scatter": [
                {"origin_ms": 100.0, "edge_ms": 150.0, "cache": "MISS"},
                {"origin_ms": 20.0, "edge_ms": 100.0, "cache": "HIT"},
                {"origin_ms": 5.0, "edge_ms": 1.0, "cache": ""},
            ]
        }
    )
    sc_res = _run_perf(sc_client, sections={"scatter", "waterfall"})
    assert [(p.origin, p.edge, p.cache) for p in sc_res.scatter] == [
        (100.0, 150.0, "MISS"),
        (20.0, 100.0, "HIT"),
        (5.0, 1.0, None),
    ]
    sc_sql, sc_params = sc_client.sql_for("scatter")
    assert (
        "FROM performance_scatter_samples" in sc_sql
        and "ORDER BY sample_key" in sc_sql
        and sc_params["scatter_limit"] == 300
    )


def test_top_asns_carry_resolved_labels_and_ints(monkeypatch):
    monkeypatch.setattr(
        "backend.core.duckdb.get_asn_names", lambda service_id, asns: {7922: "Comcast Cable"} if asns else {}
    )
    row = {**_PERCENTILE_ROW, "value": "7922", "req_count": 40}
    res = _run_perf(_PerfClient({"top_asns": [row, {**row, "value": "64512"}]}), sections={"top_asns", "top_urls"})
    assert [a.asn for a in res.top_asns] == [7922, 64512]
    assert res.top_asns[0].label == "Comcast Cable (7922)" and res.top_asns[1].label == "AS64512"


@pytest.mark.parametrize("filtered", [False, True])
def test_asn_enrichment_is_disjoint_overhead_and_cache_reuse_is_historical(
    monkeypatch: pytest.MonkeyPatch, filtered: bool
) -> None:
    clock = [0.0]
    scopes: dict[str, dict] = {}
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
    assert query["source"] == ("fact" if filtered else "projection") and query["activity"] == "compute"
    assert query["time_ms"] == pytest.approx(10)
    assert enrichment["source"] == "unknown" and enrichment["activity"] == "overhead"
    assert enrichment["time_ms"] == pytest.approx(40) and enrichment["executor_dispatch"] == "inline"
    assert enrichment["executor_wait_ms"] is None
    assert (
        scopes["query"]["section_id"] == query["section_id"]
        and scopes["labels"]["section_id"] == enrichment["section_id"]
    )
    assert query["section_id"] != enrichment["section_id"]
    for row in timings.values():
        assert (row["service_id"], row["request_id"], row["page_load_id"], row["server_cpu_ms"]) == (
            "svc",
            "request-perf",
            "page-perf",
            None,
        )
    assert response.top_asns[0].label == "Comcast Cable (7922)"
    assert hit.response_cache.state == "hit" and hit.section_timings == response.section_timings
    assert (
        len(client.calls) == 1 and clock[0] == pytest.approx(0.05) and current_clickhouse_scope()["section_id"] is None
    )


@pytest.mark.parametrize(
    ("sort_by", "column"), [("avg", "avg_ms"), ("p50", "p50_ms"), ("p95", "p95_ms"), ("p99", "p99_ms")]
)
def test_sort_by_orders_the_top_tables(sort_by, column):
    client = _PerfClient()
    _run_perf(client, _perf_req(sort_by=sort_by), sections={"top_urls", "top_asns"})
    for marker in ("top_urls", "top_asns"):
        assert f"ORDER BY {column} DESC NULLS LAST" in client.sql_for(marker)[0]


def test_perf_latencies_thresholds_sections_ttl_and_unfiltered_projections() -> None:
    client = _PerfClient({"top_urls": [_PERCENTILE_ROW]})
    res = _run_perf(client, sections={"top_urls", "top_asns"})
    url = res.top_urls[0]
    assert (url.avg, url.p50, url.p95, url.p99) == (12.5, 10.0, 20.0, 30.0)
    assert client.sql_for("top_urls")[0].count("/ 1000.0") == 4
    assert "HAVING req_count > 5" in client.sql_for("top_urls")[0]
    assert "HAVING req_count > 10" in client.sql_for("top_asns")[0]

    c_ttl = _PerfClient(
        {"ttl_dist": [{"bucket": "0s", "count": 3, "min_ttl": 0}, {"bucket": "<30m", "count": 7, "min_ttl": 1201}]}
    )
    res_ttl = _run_perf(c_ttl, sections={"ttl_dist"})
    assert c_ttl.markers == {"ttl_dist"}
    assert res_ttl.top_urls == [] and res_ttl.top_asns == [] and res_ttl.scatter == [] and res_ttl.waterfall.avg is None
    assert [(b.bucket, b.count) for b in res_ttl.ttl_dist] == [("0s", 3), ("<30m", 7)]
    ttl_sql, _ = c_ttl.sql_for("ttl_dist")
    assert "FROM request_aggregates" in ttl_sql and "LIMIT" not in ttl_sql
    assert all(edge in ttl_sql for edge in ("'<30m'", "'<3h'", "'<1w'", "'<1y'", "'>1y'"))

    c_all = _PerfClient({"top_urls": [_PERCENTILE_ROW]})
    res_all = _run_perf(c_all, sections=None)
    assert isinstance(res_all, PerformanceAggregatesResponse) and res_all.approx is True
    assert c_all.markers == {"top_urls", "top_asns", "ttl_dist", "waterfall", "scatter"}
    assert not any("request_facts" in sql for sql, _ in c_all.calls)
    for sql, params in c_all.calls:
        assert "high_scale_batch_publications" in sql and params["start"].isoformat().startswith("2026-09-16T14:00:00")


def test_perf_filters_against_request_facts_and_validation() -> None:
    client = _PerfClient(
        {
            "top_urls": [_PERCENTILE_ROW],
            "waterfall": [
                {
                    "component_count": 1,
                    "edge_processing": 1.0,
                    "origin_wait": 2.0,
                    "origin_download": 3.0,
                    "client_download": 4.0,
                }
            ],  # fmt: skip
            "scatter": [{"origin_ms": 1.0, "edge_ms": 2.0, "cache": "HIT"}],
        }
    )
    req = _perf_req(filters={"country": FilterSpec(mode="include", values=["US"])})
    res = _run_perf(client, req)
    assert res.top_urls[0].p99 == 30.0 and res.waterfall.avg is not None and res.waterfall.avg.client_download == 4.0
    assert res.scatter[0].cache == "HIT" and res.approx is False
    assert client.markers == {"top_urls", "top_asns", "ttl_dist", "waterfall", "scatter"}
    for sql, params in client.calls:
        assert "FROM request_facts" in sql and "performance_minute" not in sql and "performance_scatter" not in sql
        assert "country IN ({filter_0_0:String})" in sql and params["filter_0_0"] == "US"
        assert "publication_state='visible'" in sql and "event_timestamp >=" in sql
    assert "ORDER BY cityHash64(event_id)" in client.sql_for("scatter")[0]

    c_sort = _PerfClient()
    _run_perf(
        c_sort,
        _perf_req(filters={"asn": FilterSpec(mode="exclude", values=["64512"])}, sort_by="avg"),
        sections={"top_urls", "top_asns"},
    )
    assert "ORDER BY avg_ms DESC NULLS LAST" in c_sort.sql_for("top_urls")[0]
    assert "NOT IN" in c_sort.sql_for("top_asns")[0]

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
        _perf_event(
            event_id="evt-2", timestamp="2026-09-16T14:00:30Z", elapsed=150_000, ttfb=0.05, ottfb=None, ottlb=None
        ),
        _perf_event(event_id="evt-3", timestamp="2026-09-16T14:01:00Z"),
    )
    original = deepcopy(events)
    projection = build_performance_projection_rows(events)
    assert events == original
    assert projection.summary_rows == (
        {
            "bucket_start": "2026-09-16T14:00:00+00:00",
            "component_count": 2,
            "edge_processing_sum_ms": 110.0,
            "origin_wait_sum_ms": 40.0,
            "origin_download_sum_ms": 50.0,
            "client_download_sum_ms": 250.0,
        },  # fmt: skip
        {
            "bucket_start": "2026-09-16T14:01:00+00:00",
            "component_count": 1,
            "edge_processing_sum_ms": 60.0,
            "origin_wait_sum_ms": 40.0,
            "origin_download_sum_ms": 50.0,
            "client_download_sum_ms": 150.0,
        },  # fmt: skip
    )

    clamped = build_performance_projection_rows(
        (_perf_event(elapsed=10_000, ttfb=0.001, ottfb=40_000, ottlb=20_000),)
    ).summary_rows[0]
    assert (
        clamped["edge_processing_sum_ms"],
        clamped["origin_download_sum_ms"],
        clamped["client_download_sum_ms"],
        clamped["origin_wait_sum_ms"],
    ) == (0.0, 0.0, 0.0, 40.0)

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
        ("MISS", 100.0, 150.0),
        ("HIT", 20.0, 100.0),
        ("", 100.0, 150.0),
    }
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
        ("url", "/a"),
        ("url", "/b"),
        ("asn", "7922"),
        ("asn", "15169"),
    }


@pytest.fixture
def script(monkeypatch: pytest.MonkeyPatch):
    from scripts import backfill_high_scale as module

    service = SimpleNamespace(manifest_catalog=object(), archive=object(), client=MagicMock())
    registry = MagicMock()
    registry.resolve.return_value = service
    monkeypatch.setattr(module.app_config, "load_clickhouse_config", lambda: object())
    monkeypatch.setattr(module, "ClickHouseClient", lambda settings: MagicMock())
    monkeypatch.setattr(module, "get_high_scale_service_registry", lambda: registry)
    monkeypatch.setattr(module, "register_high_scale_services_from_environment", lambda *a, **k: None)
    monkeypatch.setattr(module, "PostgresControlPlane", MagicMock)
    monkeypatch.setattr(module, "PostgresBatchManifestStore", lambda control: MagicMock())
    monkeypatch.setattr(module, "AggregateBatchAdapter", lambda client: MagicMock())
    monkeypatch.setattr(module, "ClickHousePublication", lambda manifests, adapter: MagicMock())
    return module


def _run_backfill_script(script, monkeypatch, argv: list[str], *, complete: bool = True):
    seen: dict = {}

    def fake_rebuild(*args, **kwargs):
        seen.update(kwargs)
        return RecoveryReport(True, complete, True, complete, complete, 2, 5, (), "rb-1")

    monkeypatch.setattr(script, "rebuild_performance_projections_from_fos", fake_rebuild)
    monkeypatch.setattr("sys.argv", ["backfill", *argv])
    return script.main_performance(), seen


def test_backfill_script_cli_options_and_exit_codes(script, monkeypatch, capsys) -> None:
    code, seen = _run_backfill_script(
        script, monkeypatch, ["--service-id", "svc", "--until", "2026-10-20T12:00:00Z", "--days", "7"]
    )
    io = capsys.readouterr()
    assert code == 0 and seen["now"] == datetime(2026, 10, 20, 12, 0, tzinfo=UTC)
    assert seen["window"] == timedelta(days=7) and seen["service_id"] == "svc"
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
            return [
                {"rtt_ms": 20.123, "ttfb_ms": 55.557, "cache_state": "HIT"},
                {"rtt_ms": 31.0, "ttfb_ms": 140.0, "cache_state": "UNKNOWN"},
            ]
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
        {
            "pop": "SJC",
            "requests": 200,
            "errors": 10,
            "p50_rtt_us": 18000.0,
            "p95_ttfb_ms": 320.5,
            "cache_hits": 150,
            "bandwidth_bytes": 4096,
        },  # fmt: skip
        {
            "pop": "LHR",
            "requests": 50,
            "errors": 0,
            "p50_rtt_us": None,
            "p95_ttfb_ms": None,
            "cache_hits": 0,
            "bandwidth_bytes": None,
        },  # fmt: skip
    ]
    service, client = _net_service(lambda sql, params: rows)
    resp = get_pop_health(service, _NET_START, _NET_END)
    sjc, lhr = resp.data
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
    assert resp.heatmap[0].label == "Comcast Cable Communications (7922)"
    assert resp.leaderboard[0].label == "Comcast Cable Communications (7922)"
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
                {
                    "edge_pop": "DEN",
                    "shield_pop": "IAD",
                    "requests": 400,
                    "p50_ms": 40.0,
                    "p95_ms": 90.0,
                    "p99_ms": 120.0,
                },  # fmt: skip
                {
                    "edge_pop": "LHR",
                    "shield_pop": "IAD",
                    "requests": 10,
                    "p50_ms": 300.0,
                    "p95_ms": 500.0,
                    "p99_ms": None,
                },  # fmt: skip
            ]
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


class FakeClient:
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
    return HighScaleService(service_id="svc", client=FakeClient(), cursor_secret=b"secret", request_watermark=None)


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
